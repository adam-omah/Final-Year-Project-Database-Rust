use std::fs;
// recovery.rs
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::Mutex;
use actix_web::{get, post, web, HttpResponse, Responder};
use actix_web::cookie::time::format_description::well_known::iso8601::Config;
use serde_json::{json, Value};
use anyhow::{Result, Context, ensure};
use tracing::log::{error, info};
use crate::AppState;
use crate::config::database_config::DatabaseConfig;

#[derive(Clone)]
pub struct LogRecoveryManager {
    config: DatabaseConfig,
}


impl LogRecoveryManager {
    pub fn new(config: DatabaseConfig) -> Self {
        LogRecoveryManager { config }
    }


    /// Main recovery process
    pub fn recover_database_state(&self) -> Result<()> {
        // Read all log entries
        let log_entries = self.read_log_entries()?;

        // Group logs by table name
        let mut table_logs: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();

        for entry in log_entries {
            let table_name = entry["table_name"].as_str()
                .context("Invalid table name in log entry")?
                .to_string();

            table_logs.entry(table_name)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        // Process logs for each table
        for (table_name, logs) in table_logs {
            self.process_table_recovery(&table_name, logs)?;
        }

        // Clear the log file after successful recovery
        // self.clear_log_file()?;

        Ok(())
    }

    /// Read log entries from the file
    fn read_log_entries(&self) -> Result<Vec<serde_json::Value>> {
        let file_location = self.config.log_dir.join(&self.config.log_file);
        info!("Reading log file: {}", file_location.display());
        let file = File::open(&self.config.log_dir.join(&self.config.log_file))
            .context("Failed to open log file")?;

        let reader = BufReader::new(file);

        let mut log_entries = Vec::new();
        for line in reader.lines() {
            let line = line.context("Failed to read log line")?;
            let entry: serde_json::Value = serde_json::from_str(&line)
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
    fn process_table_recovery(&self, table_name: &str, logs: Vec<serde_json::Value>) -> Result<()> {
        // Determine the correct files based on table name
        let initial_table_path = format!(
            "{}/{}/{}_initial",
            self.config.db_dir.display(),
            self.config.table_dir.display(),
            table_name
        );

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
                    self.handle_table_creation(&initial_table_path, &updates_table_path,&log)?;
                },
                "Insert" => {
                    // Append to initial table and updates table
                    self.handle_row_insertion(&initial_table_path, &log)?;
                },
                "Update" => {
                    // Append to updates table
                    self.handle_row_update(&updates_table_path, &log)?;
                },
                "Delete" => {
                    // Append to updates table
                    self.handle_row_deletion(&updates_table_path, &log)?;
                },
                _ => {
                    println!("Unhandled change type: {}", change_type);
                }
            }
        }

        Ok(())
    }

    /// Handle table creation
    fn handle_table_creation(&self, initial_table_path: &str, updates_table_path: &str, log: &serde_json::Value) -> Result<()> {
        // Construct schema file path
        let schema_file_path = Path::new(&self.config.db_dir).join(&self.config.schema_file);

        // Read existing schema or create a new one if not exists
        let mut schema: serde_json::Value = if schema_file_path.exists() {
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
        if let Some(log_columns) = log["data"]["columns"].as_array() {
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

        // Only add tables if they don't already exist
        if !schema["tables"].get(initial_table_name).is_some() {
            schema["tables"][initial_table_name] = json!({
            "name": initial_table_name,
            "columns": columns
        });
        }

        if !schema["tables"].get(updates_table_name).is_some() {
                schema["tables"][updates_table_name] = json!({
                "name": updates_table_name,
                "columns": columns
            });
        }

        // Write updated schema back to file
        let schema_json = serde_json::to_string_pretty(&schema)?;
        fs::write(&schema_file_path, schema_json)?;

        Ok(())
    }

    /// Handle row insertion
    fn handle_row_insertion(&self,
                            initial_table_path: &str,
                            log: &serde_json::Value
    ) -> Result<()> {
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

        Ok(())
    }

    /// Handle row update
    fn handle_row_update(&self, updates_table_path: &str, log: &serde_json::Value) -> Result<()> {
        // Get row data
        let row_data = log["data"]["row_data"].as_array()
            .context("Invalid row data")?;

        // Extract UUID and timestamp (assuming UUID is first, timestamp is last)
        let row_uuid = row_data.first()
            .and_then(|v| v.as_str())
            .context("No UUID found in row data")?;

        info!("UUID: {}", row_uuid);

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
                    if columns.len() >= 4 {
                        columns[0].trim() == row_uuid &&
                            columns[3].trim() == row_timestamp
                    } else {
                        false
                    }
                }) {
                return Ok(()); // Update with same UUID and timestamp already exists, do nothing
            }
        }
        // Append to updates table if no matching update is found
        self.append_to_file(updates_table_path, &row_csv)?;
        Ok(())
    }

    /// Handle row deletion
    fn handle_row_deletion(&self, updates_table_path: &str, log: &serde_json::Value) -> Result<()> {
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

    // Clear the log file after successful recovery
    // fn clear_log_file(&self) -> Result<()> {
    //     // Truncate the file
    //     let file = File::create(&self.log_directory.join(&self.log_file))
    //         .context("Failed to clear log file")?;
    //
    //     Ok(())
    // }
}

/// Public function to run recovery
pub fn run_log_recovery(config: &DatabaseConfig) -> Result<()> {
    let recovery_manager = LogRecoveryManager::new(config.clone());
    recovery_manager.recover_database_state()
}



#[get("/api/recovery/trigger")]
pub async fn trigger_log_recovery(
    app_state: web::Data<AppState>  // Change from Mutex<AppState> to direct AppState
) -> impl Responder {

    // Perform log recovery
    match run_log_recovery(&app_state.config) {
        Ok(_) => {
            info!("Log recovery completed successfully");
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "message": "Log recovery completed"
            }))
        },
        Err(e) => {
            error!("Log recovery failed: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": format!("Log recovery failed: {}", e)
            }))
        }
    }
}

#[get("/api/recovery/trigger/{table_name}")]
pub async fn trigger_specific_table_recovery(
    app_state: web::Data<AppState>,  // Change from Mutex<AppState> to direct AppState
    path: web::Path<String>
) -> impl Responder {
    let table_name = path.into_inner();
    // Perform table-specific recovery
    match recover_specific_table(&app_state.config, &table_name) {
        Ok(_) => {
            info!("Log recovery completed for table: {}", table_name);
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "message": format!("Log recovery completed for table: {}", table_name)
            }))
        },
        Err(e) => {
            error!("Log recovery failed for table {}: {}", table_name, e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": format!("Log recovery failed for table {}: {}", table_name, e)
            }))
        }
    }
}

fn recover_specific_table(config: &DatabaseConfig ,table_name: &str) -> Result<()> {
    let recovery_manager = LogRecoveryManager::new(config.clone());

    // Read log entries
    let log_entries = recovery_manager.read_log_entries()?;

    // Filter logs for specific table
    let table_specific_logs: Vec<serde_json::Value> = log_entries
        .into_iter()
        .filter(|entry| {
            entry.get("table_name")
                .and_then(|name| name.as_str())
                .map_or(false, |name| name == table_name)
        })
        .collect();

    // Process recovery for specific table
    recovery_manager.process_table_recovery(table_name, table_specific_logs)?;

    Ok(())
}

// Configuration function to add routes to the service
pub fn configure_recovery_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_log_recovery)
        .service(trigger_specific_table_recovery);
}
