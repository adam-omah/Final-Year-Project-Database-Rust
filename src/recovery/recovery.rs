// recovery.rs
use std::fs;
use std::path::{Path};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use actix_web::{get, post, web, HttpRequest, HttpResponse, Responder};
use serde_json::{json, Value};
use anyhow::{Result, Context};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use tracing::log::{ error, info};
use crate::AppState;
use crate::auth::auth::authenticate_request;
use crate::config::database_config::DatabaseConfig;
use crate::tables::table::{get_table_data, recalculate_table, recalculate_table_global, refresh_all_tables};
use crate::schema::schema::{global_drop_table_from_cache, load_schema, refresh_schema, save_schema};

#[derive(Clone)]
pub struct LogRecoveryManager {
    pub(crate) config: DatabaseConfig,
}


impl LogRecoveryManager {
    pub fn new(config: DatabaseConfig) -> Self {
        LogRecoveryManager { config }
    }
    /// Main recovery process
    pub async fn recover_database_state(&self) -> Result<()> {
        // Read all log entries
        let log_entries = self.read_log_entries()?;

        // Group logs by table name
        let mut table_logs: std::collections::HashMap<String, Vec<Value>> =
            std::collections::HashMap::new();

        for entry in log_entries {
            let table_name = entry["table_name"].as_str()
                .context("Invalid table name in log entry")?
                .to_string();

            table_logs.entry(table_name)
                .or_default()
                .push(entry);
        }

        // Process logs for each table
        for (table_name, logs) in table_logs {
            self.process_table_recovery(&table_name, logs).await?;
        }

        Ok(())
    }

    /// Read log entries from the file
    fn read_log_entries(&self) -> Result<Vec<Value>> {
        let file_location = self.config.log_dir.join(&self.config.log_file);
        info!("Reading log file: {}", file_location.display());
        let file = File::open(self.config.log_dir.join(&self.config.log_file))
            .context("Failed to open log file")?;

        let reader = BufReader::new(file);

        let mut log_entries = Vec::new();
        for line in reader.lines() {
            let line = line.context("Failed to read log line")?;
            let entry: Value = serde_json::from_str(&line)
                .context("Failed to parse log entry")?;
            log_entries.push(entry);
        }

        // Sort entries by timestamp if possible
        log_entries.sort_by_key(|entry| {
            entry.get("data")
                .and_then(|data| data.get("timestamp"))
                .and_then(|ts| ts.as_str())
                .unwrap_or_default()
                .to_string()
        });

        Ok(log_entries)
    }

    /// Process recovery for a specific table
    async fn process_table_recovery(&self, table_name: &str, logs: Vec<Value>) -> Result<()> {
        // Determine the correct files based on table name
        let initial_table_path = format!(
            "{}/{}/{}_initial",
            self.config.db_dir.display(),
            self.config.table_dir.display(),
            table_name
        );
        info!("Initial table path: {}", initial_table_path);

        let updates_table_path = format!(
            "{}/{}/{}_updates",
            self.config.db_dir.display(),
            self.config.table_dir.display(),
            table_name
        );

        for log in logs {
            let change_type = log["change_type"].as_str()
                .context("Invalid change type")?;

            match change_type {
                "Create" => {
                    // Handle table creation (might involve creating initial table file)
                    self.handle_table_creation(&initial_table_path, &updates_table_path, &log)?;
                },
                "Insert" => {
                    // Append to initial table and updates table
                    self.handle_row_insertion(&initial_table_path, &log).await?;
                },
                "Update" => {
                    // Append to updates table
                    self.handle_row_update(&updates_table_path, &log).await?;
                },
                "Delete" => {
                    // Append to updates table
                    self.handle_row_deletion(&updates_table_path, &log).await?;
                },
                "Drop" => {
                    self.handle_drop_table(&initial_table_path, &updates_table_path, &log)?;
                }
                _ => {
                    println!("Unhandled change type: {}", change_type);
                }
            }
        }
        Ok(())
    }

    /// Handle table creation
    pub(crate) fn handle_table_creation(&self, initial_table_path: &str, updates_table_path: &str, log: &Value) -> Result<()> {
        // Construct schema file path
        let schema_file_path = Path::new(&self.config.db_dir).join(&self.config.schema_file);
        info!("Log given {}", log);
        // Read existing schema or create a new one if not exists
        let mut schema: Value = if schema_file_path.exists() {
            serde_json::from_str(&fs::read_to_string(&schema_file_path)?)?
        } else {
            json!({ "tables": {} })
        };

        // Ensure tables object exists
        if !schema.get("tables").map_or(false, |t| t.is_object()) {
            schema["tables"] = json!({});
        }

        // Extract just the table name (removing path prefix)
        let initial_table_name = initial_table_path.split('/').last().unwrap_or(initial_table_path);
        let updates_table_name = updates_table_path.split('/').last().unwrap_or(updates_table_path);

        // Prepare columns with default UUID and timestamp
        let mut columns = vec![
                json!({
                "name": "UUID",
                "data_type": "UUID",
                "rules": [
                    {
                        "constraint_type": "NotNull",
                        "action": "Reject"
                    }
                ]
            })
        ];

        // Add columns from log
        if let Some(log_columns) = log["data"]["table_definition"]["columns"].as_array() {
            info!("Log columns {:?}", log_columns);
            columns.extend(log_columns.iter().cloned());
        }

        // Add timestamp column
        columns.push(json!({
            "name": "timestamp",
            "data_type": "DateTime",
            "rules": [
                {
                    "constraint_type": "NotNull",
                    "action": "Reject"
                }
            ]
        }));

        // Check if table exists, and if it does, compare the schema
        if schema["tables"].get(initial_table_name).is_none() {
            schema["tables"][initial_table_name] = json!({
                "name": initial_table_name,
                "columns": columns
            });
        } else {
            // Compare existing schema with new schema
            let existing_schema = schema["tables"][initial_table_name]["columns"].clone();
            if existing_schema != json!(columns) {
                tracing::info!("Updating schema for table {}: existing schema differs from recovery schema",initial_table_name);
                // Update the existing schema to match the new columns
                schema["tables"][initial_table_name]["columns"] = json!(columns);
            }
        }

        // Do the same for updates table
        if schema["tables"].get(updates_table_name).is_none() {
            schema["tables"][updates_table_name] = json!({
                "name": updates_table_name,
                "columns": columns
            });
        } else {
            // Compare existing schema with new schema
            let existing_schema = schema["tables"][updates_table_name]["columns"].clone();
            if existing_schema != json!(columns) {
                info!("Updating schema for table {}: existing schema differs from recovery schema",updates_table_name);
                // Update the existing schema to match the new columns
                schema["tables"][updates_table_name]["columns"] = json!(columns);
            }
        }

        // Write updated schema back to file
        let schema_json = serde_json::to_string_pretty(&schema)?;
        fs::write(&schema_file_path, schema_json)?;
        for table_relative_path in &[initial_table_path, updates_table_path] {
            let table_path = Path::new(table_relative_path);
            // Ensure directories exist, then create file if missing
            if let Some(parent_dir) = table_path.parent() {
                fs::create_dir_all(parent_dir)?;
            }
            // Open existing table file or create a new one if it doesn't exist
            OpenOptions::new().create(true).write(true).open(table_path)?;
            info!("Verified existence of table file at {:?}", &table_path);
        }
        Ok(())
    }

    /// Handle row insertion
    pub(crate) async fn handle_row_insertion(&self,
                                             initial_table_path: &str,
                                             log: &Value
    ) -> Result<()> {
        info!("Log given to insert: {}", log);
        info!("row data: {}", log["data"]["row_data"]);
        // Get row data
        let row_data = log["data"]["row_data"].as_array()
            .context("Invalid row data")?;
        // Extract the UUID (assuming it's the first element)
        let row_uuid = row_data.first()
            .and_then(|v| v.as_str())
            .context("No UUID found in row data")?;

        // Convert row data to csv
        let row_csv = row_data.iter()
            .map(|val| val.as_str().unwrap_or_default().to_string())
            .collect::<Vec<String>>()
            .join(",");

        // Check if the file exists and read its contents
        if Path::new(initial_table_path).exists() {
            let file_contents = fs::read_to_string(initial_table_path)
                .context("Failed to read initial table file")?;

            // Skip if UUID already exists
            if file_contents.lines()
                .any(|line| line.split(',').next().unwrap_or("").trim() == row_uuid) {
                return Ok(()); // UUID already exists, do nothing
            }
        }
        // Append to initial table if UUID is not found
        self.append_to_file(initial_table_path, &row_csv)?;

        // Extract table name from path
        let table_name = Path::new(initial_table_path)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.replace("_initial", ""))
            .context("Failed to extract table name from path")?;

        // Recalculate Table
        match self.recalculate_after_change(&table_name).await {
            Ok(_) => {},
            Err(e) => error!("Failed to recalculate table after insert: {}", e)
        }

        Ok(())
    }

    /// Handle row update
    pub(crate) async fn handle_row_update(&self, updates_table_path: &str, log: &Value) -> Result<()> {
        // Get row data
        let row_data = log["data"]["row_data"].as_array()
            .context("Invalid row data")?;

        // Extract UUID and timestamp (assuming UUID is first, timestamp is last)
        let row_uuid = row_data.first()
            .and_then(|v| v.as_str())
            .context("No UUID found in row data")?;

        let row_timestamp = row_data.last()
            .and_then(|v| v.as_str())
            .context("No timestamp found in row data")?;

        // Convert row data to CSV format
        let row_csv = row_data.iter()
            .map(|val| val.as_str().unwrap_or_default().to_string())
            .collect::<Vec<String>>()
            .join(",");

        // Check if the updates file exists and read its contents
        if Path::new(updates_table_path).exists() {
            let file_contents = fs::read_to_string(updates_table_path)
                .context("Failed to read updates table file")?;

            // Check if an update with the same UUID and timestamp already exists
            if file_contents.lines()
                .any(|line| {
                    let columns: Vec<&str> = line.split(',').collect();

                    // Check if the line has enough columns to perform the comparison
                    if columns.len() >= 2 {  // Minimum required columns for meaningful comparison
                        columns[0].trim() == row_uuid &&
                            columns[columns.len() - 1].trim() == row_timestamp
                    } else {
                        false
                    }
                }) {
                return Ok(()); // Update with same UUID and timestamp already exists, do nothing
            }
        }
        // Append to updates table if no matching update is found
        self.append_to_file(updates_table_path, &row_csv)?;

        // Extract table name from path
        let table_name = Path::new(updates_table_path)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.replace("_updates", ""))
            .context("Failed to extract table name from path")?;

        // Recalculate Table
        match self.recalculate_after_change(&table_name).await {
            Ok(_) => {},
            Err(e) => error!("Failed to recalculate table after update: {}", e)
        }


        Ok(())
    }

    pub(crate) fn handle_drop_table(
        &self,
        initial_table_path: &str,
        updates_table_path: &str,
        log: &Value
    ) -> Result<()> {
        let table_name = log["table_name"].as_str()
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid table name in drop log"
            ))?;

        // Load current schema
        let mut schema = load_schema(&self.config)?;

        // Remove both _initial and _updates entries from schema
        schema.tables.remove(&format!("{}_initial", table_name));
        schema.tables.remove(&format!("{}_updates", table_name));

        // Save the updated schema
        save_schema(&schema, &self.config)?;

        // Remove initial and updates files
        if Path::new(initial_table_path).exists() {
            fs::remove_file(initial_table_path)?;
        }
        if Path::new(updates_table_path).exists() {
            fs::remove_file(updates_table_path)?;
        }

        global_drop_table_from_cache(table_name.parse()?).expect("Unable to remove table from cache!");
        Ok(())
    }



    /// Handle row deletion
    pub(crate) async fn handle_row_deletion(&self, updates_table_path: &str, log: &Value) -> Result<()> {
        // Get row data
        let row_data = log["data"]["row_data"].as_array()
            .context("Invalid row data")?;

        // Extract UUID and timestamp (assuming UUID is first, timestamp is last)
        let row_uuid = row_data.first()
            .and_then(|v| v.as_str())
            .context("No UUID found in row data")?;

        let row_timestamp = row_data.last()
            .and_then(|v| v.as_str())
            .context("No timestamp found in row data")?;

        // Prepare row CSV (ensuring "ROW_REMOVED" or "0" is used)
        let row_csv = row_data.iter()
            .map(|val| {
                let val_str = val.as_str().unwrap_or_default();
                // Special handling for deletion marker
                if val_str.is_empty() || val_str == "0" {
                    "0".to_string()
                } else if val_str.to_uppercase() == "ROW_REMOVED" {
                    "ROW_REMOVED".to_string()
                } else {
                    val_str.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join(",");

        // Check if the updates file exists and read its contents
        if Path::new(updates_table_path).exists() {
            let file_contents = fs::read_to_string(updates_table_path)
                .context("Failed to read updates table file")?;

            // Check if a delete with the same UUID and timestamp already exists
            if file_contents.lines()
                .any(|line| {
                    let columns: Vec<&str> = line.split(',').collect();
                    if columns.len() >= 4 {
                        columns[0].trim() == row_uuid &&
                            columns[3].trim() == row_timestamp
                    } else {
                        false
                    }
                }) {
                return Ok(()); // Delete with same UUID and timestamp already exists, do nothing
            }
        }

        // Append to updates table if no matching delete is found
        self.append_to_file(updates_table_path, &row_csv)?;

        // Extract table name from path
        let table_name = Path::new(updates_table_path)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.replace("_updates", ""))
            .context("Failed to extract table name from path")?;

        // Recalculate Table
        match self.recalculate_after_change(&table_name).await {
            Ok(_) => {},
            Err(e) => error!("Failed to recalculate table after deletion: {}", e)
        }

        Ok(())
    }

    /// Append a line to a file
    fn append_to_file(&self, file_path: &str, content: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .context("Failed to open file for appending")?;

        writeln!(file, "{}", content)?;

        Ok(())
    }

    fn get_log_timestamp(&self, log: &Value) -> Option<DateTime<Utc>> {
        // Timestamp candidates in order of priority
        let timestamp_candidates = vec![
            // 1. Direct timestamp in data
            log.get("data").and_then(|data| data.get("timestamp")),
            // 2. Timestamp as last element in row_data
            log.get("data")
                .and_then(|data| data.get("row_data"))
                .and_then(|row_data| {
                    if let Value::Array(arr) = row_data {
                        // Get the last element of the array
                        arr.last()
                    } else {
                        None
                    }
                }),
            // 3. Timestamp at root level
            log.get("timestamp"),
        ];

        // Attempt parsing with multiple formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",   // Standard format
            "%Y-%m-%dT%H:%M:%S",   // ISO-like format
            "%Y-%m-%d %H:%M:%S%.f" // With optional microseconds
        ];

        for candidate in timestamp_candidates {
            if let Some(ts) = candidate.and_then(|t| t.as_str()) {
                // Remove surrounding quotes and whitespace
                let cleaned_ts = ts.trim_matches('"').trim();
                for format in &formats {
                    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(cleaned_ts, format) {
                        let timestamp = naive_dt.and_local_timezone(Utc).unwrap();
                        return Some(timestamp);
                    }
                }
            }
        }

        // Log detailed information if no timestamp found
        error!(
            "No valid timestamp found in log entry. Log details: {}",
            serde_json::to_string_pretty(log).unwrap_or_default()
        );
        None
    }

    pub async fn recalculate_after_change(&self, table_name: &str) -> Result<()> {
        info!("Recalculating table {} after applying changes", table_name);

        // Use actix-web runtime to handle the async operation
        let table_name_owned = table_name.to_string();

        actix_web::rt::spawn(async move {
            match recalculate_table_global(&table_name_owned).await {
                Ok(_) => {
                    info!("Successfully recalculated table {}", table_name_owned);
                },
                Err(e) => {
                    error!("Failed to recalculate table {}: {}", table_name_owned, e);
                }
            }
        });

        Ok(())
    }


    async fn recover_database_state_since(&self, since_timestamp: DateTime<Utc>) -> Result<()> {
        info!("Recovering database state since: {}", since_timestamp);

        // Read all log entries
        let log_entries = self.read_log_entries()?;
        info!("Total log entries: {}", log_entries.len());

        // Detailed logging for filtering
        let filtered_logs: Vec<Value> = log_entries
            .into_iter()
            .filter(|log| {
                if let Some(log_time) = self.get_log_timestamp(log) {
                    
                    log_time > since_timestamp
                } else {
                    false
                }
            })
            .collect();

        info!("Filtered log entries: {}", filtered_logs.len());

        // Rest of the recovery logic remains similar to previous implementation
        if filtered_logs.is_empty() {
            info!("No logs found after the specified timestamp");
            return Ok(());
        }

        // Group logs by table name
        let mut table_logs: std::collections::HashMap<String, Vec<Value>> =
            std::collections::HashMap::new();

        for log in filtered_logs {
            if let Some(table_name) = log.get("table_name")
                .and_then(|tn| tn.as_str())
                .map(|s| s.to_string()) {
                table_logs.entry(table_name)
                    .or_default()
                    .push(log);
            }
        }

        // Process recovery for each table
        for (table_name, logs) in table_logs {
            info!("Processing recovery for table: {}", table_name);
            // Sort logs by timestamp
            let mut sorted_logs = logs;
            sorted_logs.sort_by(|a, b| {
                let a_time = self.get_log_timestamp(a)
                    .unwrap_or_else(Utc::now);
                let b_time = self.get_log_timestamp(b)
                    .unwrap_or_else(Utc::now);
                a_time.cmp(&b_time)
            });
            // Process table recovery
            self.process_table_recovery(&table_name, sorted_logs).await?;
        }
        Ok(())
    }

    // An overloaded version for convenience (Duration)
    async fn recover_database_state_since_duration(&self, duration: Duration) -> Result<()> {
        let since_timestamp = Utc::now() - duration;
        self.recover_database_state_since(since_timestamp).await
    }

}

/// Public function to run recovery
pub async fn run_log_recovery(config: &DatabaseConfig) -> Result<()> {
    let recovery_manager = LogRecoveryManager::new(config.clone());
    recovery_manager.recover_database_state().await
}


async fn recover_specific_table(config: &DatabaseConfig ,table_name: &str) -> Result<()> {
    let recovery_manager = LogRecoveryManager::new(config.clone());

    // Read log entries
    let log_entries = recovery_manager.read_log_entries()?;

    // Filter logs for specific table
    let table_specific_logs: Vec<Value> = log_entries
        .into_iter()
        .filter(|entry| {
            entry.get("table_name")
                .and_then(|name| name.as_str())
                .map_or(false, |name| name == table_name)
        })
        .collect();

    // Process recovery for specific table
    recovery_manager.process_table_recovery(table_name, table_specific_logs).await?;

    Ok(())
}

// Configuration function to add routes to the service
pub fn configure_recovery_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_log_recovery)
        .service(trigger_specific_table_recovery)
        .service(trigger_time_based_recovery);
}



#[get("/api/recovery/trigger")]
pub async fn trigger_log_recovery(
    req: HttpRequest,
    app_state: web::Data<AppState>  // Change from Mutex<AppState> to direct AppState
) -> impl Responder {
    match authenticate_request(&req, &app_state).await {
        Ok(_) => {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
            // Perform log recovery
            match run_log_recovery(&app_state.config).await {
                Ok(_) => {
                    refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
                    if let Err(e) = refresh_all_tables(&app_state).await {
                        return HttpResponse::InternalServerError().json(json!({
                            "status": "error",
                            "message": format!("Log recovery succeeded but table refresh failed: {}", e)
                        }));
                    }
                    info!("Log recovery completed successfully");
                    HttpResponse::Ok().json(json!({
                        "status": "success",
                        "message": "Log recovery completed"
                    }))
                },
                Err(e) => {
                    refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
                    if let Err(e) = refresh_all_tables(&app_state).await {
                        return HttpResponse::InternalServerError().json(json!({
                            "status": "error",
                            "message": format!("Log recovery failed and table refresh failed: {}", e)
                        }));
                    }
                    error!("Log recovery failed: {}", e);
                    HttpResponse::InternalServerError().json(json!({
                        "status": "error",
                        "message": format!("Log recovery failed: {}", e)
                    }))
                }
            }
        }
        Err(auth_error) => auth_error.into()
    }
}

#[get("/api/recovery/trigger/{table_name}")]
pub async fn trigger_specific_table_recovery(
    app_state: web::Data<AppState>,  // Change from Mutex<AppState> to direct AppState
    path: web::Path<String>
) -> impl Responder {
    refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
    let table_name = path.into_inner();
    // Perform table-specific recovery
    match recover_specific_table(&app_state.config, &table_name).await {
        Ok(_) => {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
            // refresh Specific Table Only
            match get_table_data(app_state.clone(), &table_name).await {
                Ok(initial_table_data) => {
                    match recalculate_table(&app_state, &table_name, initial_table_data).await {
                        Ok(_) => HttpResponse::Ok().json(json!({
                            "status": "success",
                            "message": format!("Table {} recovery completed", table_name)
                        })),
                        Err(e) => HttpResponse::InternalServerError().json(json!({
                            "status": "error",
                            "message": format!("Table {} recalculation failed: {}", table_name, e)
                        }))
                    }
                },
                Err(e) => HttpResponse::InternalServerError().json(json!({
                    "status": "error",
                    "message": format!("Failed to get table data for {}: {}", table_name, e)
                }))
            }
        },
        Err(e) => {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
            error!("Log recovery failed for table {}: {}", table_name, e);
            HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": format!("Log recovery failed for table {}: {}", table_name, e)
            }))
        }
    }
}

#[post("/api/recovery/trigger/time-based")]
pub async fn trigger_time_based_recovery(
    app_state: web::Data<AppState>,
    request: web::Json<Value>
) -> impl Responder {
    refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
    let recovery_manager = &app_state.log_recovery_manager;

    let result = match (
        request.get("timestamp").and_then(|v| v.as_str()),
        request.get("duration_hours").and_then(|v| v.as_f64())
    ) {
        (Some(timestamp_str), None) => {
            let timestamp = match DateTime::parse_from_rfc3339(timestamp_str) {
                Ok(dt) => dt.with_timezone(&Utc),
                Err(_) => return HttpResponse::BadRequest().json(json!({
                    "status": "error",
                    "message": "Invalid timestamp format"
                }))
            };
            recovery_manager.recover_database_state_since(timestamp).await
        },
        (None, Some(hours)) => {
            recovery_manager.recover_database_state_since_duration(Duration::hours(hours as i64)).await
        },
        _ => {
            return HttpResponse::BadRequest().json(json!({
                "status": "error",
                "message": "Provide either timestamp or duration, not both"
            }));
        }
    };

    // Refresh all tables
    match result {
        Ok(_) => {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
            if let Err(e) = refresh_all_tables(&app_state).await {
                return HttpResponse::InternalServerError().json(json!({
                    "status": "error",
                    "message": format!("Log recovery succeeded but table refresh failed: {}", e)
                }));
            }
            HttpResponse::Ok().json(json!({
                "status": "success",
                "message": "Log recovery completed"
            }))
        },
        Err(e) => {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
            if let Err(e) = refresh_all_tables(&app_state).await {
                return HttpResponse::InternalServerError().json(json!({
                    "status": "error",
                    "message": format!("Log recovery failed and table refresh failed: {}", e)
                }));
            }
            HttpResponse::InternalServerError().json(json!({
                "status": "error",
                "message": format!("Recovery failed: {}", e)
            }))
        }
    }
}


