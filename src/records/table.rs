// Table.rs
use crate::schema::{schema::create_table as schema_create_table, schema::Table};
use crate::AppState;
use actix_web::{get, web, HttpResponse, Responder};
use std::fs::{OpenOptions};
use std::io::{Error, ErrorKind, Read, Result, Seek, Write};
use std::path::Path;
use crate::query::parser::Identifier;
use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use tracing::log::{debug, info};
use crate::schema::schema::{check_column_rules, is_valid_data_type, DataType};
use chrono::{Local, NaiveDateTime, Utc};
use uuid::Uuid;
use crate::change_logging::change_logging::{ChangeLogEntry, ChangeType};

pub fn create_table(table: &Table, state: &web::Data<AppState>) -> Result<()> {
    let mut schema = state.schema.lock().unwrap();
    schema_create_table(&mut schema, table.clone(), &state.config, &state.change_logger)?;
    drop(schema);
    Ok(())
}

pub async fn insert_row(
    table_name: &str,
    row_data: Vec<String>,
    state: &web::Data<AppState>,
    column_names: Option<Vec<String>>, // Add column names here
) -> Result<()> {
    // Acquire schema lock and clone it
    let schema_snapshot = {
        let schema_guard = state.schema.lock().unwrap();
        schema_guard.clone()
    };

    // Build the table name used for schema lookup
    let initial_table_name = format!("{}_initial", table_name);
    let table_schema = schema_snapshot
        .tables
        .get(&initial_table_name)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Table '{}' does not exist in schema", initial_table_name),
            )
        })?;

    // Validate column names if provided
    if let Some(ref provided_column_names) = column_names {
        // Create a set of valid column names from the schema for efficient lookup
        let valid_column_names: HashSet<String> = table_schema.columns
            .iter()
            .map(|col| col.name.clone())
            .collect();

        // Check if all provided column names exist in the schema
        for column_name in provided_column_names {
            if !valid_column_names.contains(column_name) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid column name: '{}'", column_name)
                ));
            }
        }
    }

    // Step 1: Build a `HashMap` to pair column names and their respective data values
    let mut row_data_map: HashMap<String, String> = HashMap::new();

    if let Some(provided_column_names) = column_names {
        // Case when column names are provided
        for (index, column_name) in provided_column_names.iter().enumerate() {
            if index < row_data.len() {
                row_data_map.insert(column_name.clone(), row_data[index].clone());
            }
        }
    } else {
        // Case when column names are NOT provided
        // Ensure UUID is the first column, and remaining values align with schema
        let mut data_index = 0;

        for column in &table_schema.columns {
            if column.name == "UUID" {
                // Insert UUID (generate one if not provided in the data)
                row_data_map.insert(
                    column.name.clone(),
                    if data_index < row_data.len() && Uuid::parse_str(&row_data[data_index]).is_ok() {
                        // Use provided UUID if valid
                        row_data[data_index].clone()
                    } else {
                        // Otherwise generate a new UUID
                        Uuid::new_v4().to_string()
                    },
                );
                // Increment index only if a UUID was provided
                if data_index < row_data.len()
                    && Uuid::parse_str(&row_data[data_index]).is_ok()
                {
                    data_index += 1;
                }
            } else{
                // For all other columns assign values in schema order
                row_data_map.insert(
                    column.name.clone(),
                    row_data.get(data_index).cloned().unwrap_or_default(), // Default to empty if missing
                );
                data_index += 1;
            }
        }
    }

    // Ensure all schema-defined columns are included
    for column in &table_schema.columns {
        row_data_map.entry(column.name.clone()).or_insert_with(|| {
            if column.name == "UUID" {
                // Generate a UUID if it's not provided
                Uuid::new_v4().to_string()
            } else {
                String::new() // Default to an empty string for other columns
            }
        });
    }

    // Step 2: Pass the complete `row_data_map` into the `validate_and_process_row` function
    let validated_row = validate_and_process_row(table_name, row_data_map.clone(), state).await?;

    // Prepare the `_initial` table file path
    let initial_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&initial_table_name);

    // Serialize validated row using `quote_if_needed`
    let serialized_row = validated_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>()
        .join(",");

    // Open the `_initial` table file in append-only mode and write the data
    let mut writer = open_table_file_append_only(&initial_table_path)?;
    write_newline_if_needed(&initial_table_path)?;
    writeln!(writer, "{}", serialized_row)
        .map_err(|e| std::io::Error::new(e.kind(), format!("Failed to write initial values  to file: {}", e)))?;

    let cache_row = validated_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>();
    // Add the validated row directly to the cache
    add_row_to_cache(table_name, cache_row.clone(), state).await?;

    state.change_logger.log_change(
        None,
        ChangeType::Insert,
        table_name.to_string(),
        serde_json::json!({
            "row_data": cache_row.clone()
        }),
        None,
        Option::from(state.config.database_name.clone())
    )?;
    Ok(())
}


// Append updates to the `_updates` table.
pub async fn update_row(
    table_name: &str,
    uuid: &String,
    mut updated_values: HashMap<String, String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&updates_table_name);

    // Acquire the schema and table layout information
    let schema = state.schema.lock().unwrap().clone();
    let initial_table_name = format!("{}_initial", table_name);
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Table '{}' does not exist in schema", initial_table_name),
        )
    })?;

    // Check if UUID exists in the current table state
    let current_data = get_table_data(state.clone(), table_name).await?;
    info!("Current data: {:#?}", current_data);

    let row_index = current_data
        .iter()
        .position(|row| row.get(0) == Some(uuid)) // Assume UUID is always in the first column
        .ok_or_else(|| {
            Error::new(ErrorKind::NotFound, format!("Row with UUID '{}' not found", uuid))
        })?;

    // Merge existing row data with updated values
    let current_row = &current_data[row_index].iter()
        .map(|value| {
            if value.starts_with('"') && value.ends_with('"') {
                value.trim_start_matches('"').trim_end_matches('"').to_string()
            } else {
                value.clone()
            }
        })
        .collect::<Vec<String>>();
    let mut merged_row = HashMap::new();

    for (index, column) in table.columns.iter().enumerate() {
        let column_name = &column.name.to_lowercase();
        if column_name == "timestamp" || column_name == "uuid" {
            continue;
        }
        // Use the updated value if provided; otherwise, use the existing value from the current row
        let value = updated_values
            .remove(column_name)
            .unwrap_or_else(|| current_row[index].clone());
        merged_row.insert(column_name.clone(), value);
    }

    // Add UUID to merged_row for validation
    merged_row.insert("UUID".to_string(), uuid.trim_matches('"').to_string());

    // Validate the merged row
    let validated_row = validate_and_process_row(table_name, merged_row, state).await?;

    // Serialize the validated row
    let serialized_row = validated_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>()
        .join(",");

    // Write the updated row to the `_updates` table
    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&updates_table_path)?;
    write_newline_if_needed(&updates_table_path)?;
    writeln!(writer, "{}", serialized_row)?;

    // Recalculate the cache to reflect the updated state
    recalculate_row(state, table_name, uuid).await?;

    let log_row = validated_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>();

    state.change_logger.log_change(
        None,
        ChangeType::Update,
        table_name.to_string(),
        serde_json::json!({
            "row_data": log_row
        }),
        None,
        Option::from(state.config.database_name.clone())
    )?;


    Ok(())
}


pub async fn delete_row(table_name: &String, uuid: &String, state: &web::Data<AppState>) -> Result<()> {
    // Construct the `_updates` table name and path
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&updates_table_name);

    // Load the schema for the table
    let schema = state.schema.lock().unwrap().clone();
    let initial_table_name = format!("{}_initial", table_name);
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        Error::new(
            std::io::ErrorKind::NotFound,
            format!("Table '{}' does not exist in schema", initial_table_name),
        )
    })?;

    // Construct a "deleted" row for the `_updates` file
    let mut deleted_row = vec![];
    let trimmed_uuid = uuid.trim_matches('"');
    for column in &table.columns {
        match column.data_type {
            DataType::UUID => {
                // Add the trimmed UUID
                deleted_row.push(trimmed_uuid.to_string());
            }
            DataType::DateTime => {
                // Add the current timestamp
                deleted_row.push(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string());
            }
            DataType::Int => {
                // For numeric (Int) columns, set value to `0`
                deleted_row.push("0".to_string());
            }
            _ => {
                // For all other columns (String, etc.), use "Deleted"
                deleted_row.push("ROW_REMOVED".to_string());
            }
        }
    }

    // Serialize the row for writing
    let serialized_row = deleted_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>()
        .join(",");

    // Append the "deleted" row to the `_updates` table
    let mut writer = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&updates_table_path)?;
    write_newline_if_needed(&updates_table_path)?;
    writeln!(writer, "{}", serialized_row)?;

    recalculate_row(state, table_name, uuid).await?;

    let log_row = deleted_row
        .iter()
        .map(|value| quote_if_needed(value))
        .collect::<Vec<String>>();

    state.change_logger.log_change(
        None,
        ChangeType::Delete,
        table_name.to_string(),
        serde_json::json!({
            "row_data": log_row
        }),
        None,
        Option::from(state.config.database_name.clone())
    )?;
    Ok(())
}


// Utility function for extracting data from `_initial` and `_updates` tables, applying updates, and returning combined data.
pub fn get_table_data(
    state: web::Data<AppState>,
    table_name: &str,
) -> BoxFuture<Result<Vec<Vec<String>>>> {
    Box::pin(async move {
        // Retrieve column names
        let column_names = get_column_names(table_name, &state).await?;

        // Attempt to get from cache (using a shorter lock scope)
        {
            let cache = state.cache.lock().unwrap();
            if let Some(cached_data) = cache.get(table_name) {
                // If data is found in cache, prepend column names
                let mut result = cached_data.clone();
                result.insert(0, column_names);
                return Ok(result);
            }
        }

        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&state.config.db_dir)
            .join(&state.config.table_dir)
            .join(&initial_table_name);

        let initial_data = load_table_data_from_file(&initial_table_path)?;
        recalculate_table(&state, table_name, initial_data.clone()).await?;  // Pass initial_data

        // Now, the cache *should* have the updated data
        let cache = state.cache.lock().unwrap();
        let cached_data = cache
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Data not found in cache after recalculation"))?;

        // Prepend column names to the cached data
        let mut result = cached_data.clone();
        result.insert(0, column_names);

        Ok(result)
    })
}




// Helper function for loading table data from a filepath.
pub fn load_table_data_from_file(table_path: &Path) -> Result<Vec<Vec<String>>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    if !table_path.exists() {
        return Ok(Vec::new()); // Return empty data if file doesn’t exist
    }

    let file = File::open(table_path)?;
    let reader = BufReader::new(file);

    reader
        .lines()
        .map(|line| {
            line.map(|l| {
                l.split(',')
                    .map(|s| s.trim().to_string())
                    .collect::<Vec<String>>()
            })
        })
        .collect()
}

async fn validate_and_process_row(
    table_name: &str,
    mut row_data: HashMap<String, String>,
    state: &web::Data<AppState>,
) -> Result<Vec<String>> {
    // Lock the schema just long enough to get a clone of it
    let schema = {
        let schema_guard = state.schema.lock().unwrap();
        schema_guard.clone() // Clone the schema to work on a local copy
    };

    // Validate that the table exists in the cloned schema
    let initial_table_name = format!("{}_initial", table_name);
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Table '{}' does not exist in schema", initial_table_name),
        )
    })?;

    // Extract the UUID from the row_data, which is assumed to be the first column
    let row_uuid = row_data
        .get("UUID")
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing UUID column in row data",
            )
        })?
        .to_owned();

    // Validate row data based on the schema's column constraints
    let mut validated_row = Vec::new();
    for column in &table.columns {
        let column_name = &column.name;
        let value = row_data.remove(column_name).unwrap_or_else(|| {
            if column_name == "timestamp" {
                // Add current timestamp if "timestamp" is not provided or is empty
                Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                String::new() // Default to an empty string for other columns
            }
        });

        // catch for if timestamp is provided but is empty.
        let value = if column_name == "timestamp" && value.trim().is_empty() {
            Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
        } else {
            value
        };

        // Validate the data type
        if !is_valid_data_type(&column.data_type, &value) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid data type for column '{}'. Expected: {:?}, Found: {}",
                    column_name, column.data_type, value
                ),
            ));
        }

        // Validate and transform the value based on column rules
        let validated_value = check_column_rules(column, &value, table_name, &state, Some(&row_uuid))
            .await?
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Constraint violation for column '{}'", column_name),
                )
            })?;

        validated_row.push(validated_value);
    }

    Ok(validated_row)
}

fn open_table_file_append_only(table_path: &Path) -> Result<std::fs::File> {
    OpenOptions::new()
        .append(true)
        .open(table_path)
        .map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to open table file '{}' in append-only mode: {}",
                    table_path.display(),
                    e
                ),
            )
        })
}

pub(crate) fn extract_literal_value(identifier: &Identifier) -> String {
    match identifier {
        Identifier::Literal(value, _) => value.clone(),
        Identifier::Name(name) => name.clone(),
        Identifier::Star => "*".to_string(), // Or handle this differently
    }
}

fn quote_if_needed(value: &str) -> String {
    // Check if the value is a valid number
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        return value.to_string(); // Return as-is for numbers
    }else {
        return format!("\"{}\"", value.replace('\"', "\\\"")); // Escape quotes within the value
    }
}

fn write_newline_if_needed(table_path: &Path) -> Result<()> {
    // Open the file in read+write mode
    let mut file = OpenOptions::new().read(true).write(true).open(table_path)?;
    // Check if the file is empty
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        // The file is empty, no need to check further
        return Ok(());
    }
    // Seek to the last byte
    file.seek(std::io::SeekFrom::End(-1))?;
    let mut last_byte = [0; 1];
    file.read_exact(&mut last_byte)?;

    // Check if the last byte is a newline (`\n`)
    if last_byte[0] != b'\n' {
        // If not, move to the end and append a newline
        file.seek(std::io::SeekFrom::End(0))?;
        writeln!(file)?; // Append the newline
    }
    Ok(())
}

async fn add_row_to_cache(
    table_name: &str,
    new_row: Vec<String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    let mut cache = state.cache.lock().unwrap();

    // Check if the table is already in the cache
    if let Some(cached_data) = cache.get_mut(table_name) {
        // Add the new row directly into the cached data
        cached_data.push(new_row);
    } else {
        drop(cache); // Unlock before calling async
        // If the table is not in the cache, trigger a full recalculation
        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&state.config.db_dir)
            .join(&state.config.table_dir)
            .join(&initial_table_name);

        let initial_data = load_table_data_from_file(&initial_table_path)?;
        recalculate_table(state, table_name, initial_data).await?;
    }
    Ok(())
}

pub async fn recalculate_row(
    state: &web::Data<AppState>,
    table_name: &str,
    uuid: &str,
) -> Result<()> {
    let cache = state.cache.lock().unwrap();

    // Check if the table is in the cache
    if !cache.contains_key(table_name) {
        drop(cache); // Unlock before performing expensive calculations
        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&state.config.db_dir)
            .join(&state.config.table_dir)
            .join(&initial_table_name);

        let initial_data = load_table_data_from_file(&initial_table_path)?;
        recalculate_table(state, table_name, initial_data).await?;
        return Ok(());
    }
    drop(cache); // Release the lock after confirming the cache's presence.
    // Load `_initial` data for the specific UUID
    let initial_table_name = format!("{}_initial", table_name);
    let initial_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&initial_table_name);

    let initial_data = load_table_data_from_file(&initial_table_path)?;

    // Find the row by matching the UUID after full cleaning
    let cleaned_uuid = uuid.trim_matches('"').to_string();
    let initial_row = initial_data
        .into_iter()
        .find(|row| {
            if let Some(id) = row.get(0) {
                let cleaned_id = id.trim_matches('"');
                return cleaned_id == cleaned_uuid;
            }
            false
        })
        .ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotFound,
                format!("Row with UUID '{}' not found in the initial table", uuid),
            )
        })?;

    // Load `_updates` data for the specific UUID
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&updates_table_name);

    let updates_data = load_table_data_from_file(&updates_table_path)?;
    let relevant_updates: Vec<Vec<String>> = updates_data
        .into_iter()
        .filter(|row| {
            if let Some(id) = row.get(0) {
                let cleaned_id = id.trim_matches('"');
                return cleaned_id == cleaned_uuid;
            }
            false
        })
        .collect();

    // Sort updates in descending order of timestamp
    let mut sorted_updates = relevant_updates.clone();
    sorted_updates.sort_by(|a, b| {
        let a_timestamp = parse_timestamp_from_row(a);
        let b_timestamp = parse_timestamp_from_row(b);
        b_timestamp.cmp(&a_timestamp) // Descending order
    });

    // Combine the initial row with updates into the final row
    let final_row = merge_row_with_updates(initial_row, sorted_updates);

    // Acquire the lock again to update the specific row in the cache
    let mut cache = state.cache.lock().unwrap();
    if let Some(cached_table) = cache.get_mut(table_name) {
        for row in cached_table.iter_mut() {
            if let Some(id) = row.get(0) {
                let cleaned_id = id.trim_matches('"');
                if cleaned_id == cleaned_uuid {
                    *row = final_row;
                    return Ok(()); // Update only the necessary row and exit
                }
            }
        }
        // If the row is not in the cached table, add it
        cached_table.push(final_row);
    }
    Ok(())
}

pub async fn recalculate_table(
    state: &web::Data<AppState>,
    table_name: &str,
    initial_table_data: Vec<Vec<String>>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&updates_table_name);

    // Load updates data
    let updates_data = load_table_data_from_file(&updates_table_path)?;

    // Sort updates in descending order of timestamp to ensure the most recent updates are applied
    let mut sorted_updates_data = updates_data.clone();
    sorted_updates_data.sort_by(|a, b| {
        let a_timestamp = a.iter().find(|col| col.contains("-") && col.contains(":"))
            .and_then(|timestamp| NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").ok());
        let b_timestamp = b.iter().find(|col| col.contains("-") && col.contains(":"))
            .and_then(|timestamp| NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").ok());
        b_timestamp.cmp(&a_timestamp)
    });
    // Merge the initial data with sorted updates
    let merged_data = merge_table_and_update(initial_table_data, sorted_updates_data);
    // Update the cache
    let mut cache = state.cache.lock().unwrap();
    cache.insert(table_name.to_string(), merged_data);
    Ok(())
}

fn merge_table_and_update(
    initial_table_data: Vec<Vec<String>>,
    updates_data: Vec<Vec<String>>,
) -> Vec<Vec<String>> {
    // Create a lookup map for updates using the unique ID (first column) as the key
    let mut updates_by_id: HashMap<String, Vec<String>> = HashMap::new();

    for update_row in updates_data {
        if let Some(id) = update_row.first() {
            updates_by_id.insert(id.clone(), update_row);
        }
    }

    // Merge updates into the initial table while preserving the order
    let merged_data = initial_table_data
        .into_iter()
        .map(|row| {
            if let Some(id) = row.first() {
                if let Some(updated_row) = updates_by_id.get(id) {
                    // Replace the row with the updated row
                    return updated_row.clone();
                }
            }
            // Return the original row if no update is found
            row
        })
        .collect();

    merged_data
}

fn merge_row_with_updates(initial_row: Vec<String>, updates: Vec<Vec<String>>) -> Vec<String> {
    if updates.is_empty() {
        return initial_row;
    }
    let mut current_row = initial_row.clone();
    for update in updates {
        for (index, value) in update.iter().enumerate() {
            if !value.is_empty() && value != "ROW_REMOVED" {
                current_row[index] = value.clone();
            }
        }
    }
    current_row
}


fn parse_timestamp_from_row(row: &Vec<String>) -> Option<NaiveDateTime> {
    row.iter()
        .find(|col| col.contains("-") && col.contains(":"))
        .and_then(|timestamp| NaiveDateTime::parse_from_str(timestamp.trim_matches('"'), "%Y-%m-%d %H:%M:%S").ok())
}


pub async fn get_table_at_timestamp(
    state: web::Data<AppState>,
    table_name: &String,
    timestamp: String,
) -> Result<Vec<Vec<String>>> {
    let column_names = get_column_names(table_name, &state).await?;

    let initial_table_name = format!("{}_initial", table_name);
    let updates_table_name = format!("{}_updates", table_name);

    // Parse the provided timestamp to a `NaiveDateTime`.
    let target_timestamp = NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%d %H:%M:%S")
        .map_err(|_| std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid timestamp format: {}", timestamp),
        ))?;

    // Load initial data and filter rows by the timestamp
    let initial_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&initial_table_name);

    let initial_data_result = load_table_data_from_file(&initial_table_path);
    let initial_data = match initial_data_result {
        Ok(data) => {
            data.into_iter()
                .filter(|row| row_timestamp_is_before(row, &target_timestamp))
                .collect::<Vec<_>>()
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                Vec::new() // Treat missing initial table as empty
            } else {
                return Err(e); // Propagate other errors
            }
        }
    };

    // Load updates data and filter rows by the timestamp
    let updates_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&updates_table_name);

    let updates_data_result = load_table_data_from_file(&updates_table_path);
    let updates_data = match updates_data_result {
        Ok(data) => {
            data.into_iter()
                .filter(|row| row_timestamp_is_before(row, &target_timestamp))
                .collect::<Vec<_>>()
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                Vec::new() // Treat missing updates table as empty
            } else {
                return Err(e); // Propagate other errors
            }
        }
    };

    // Merge the filtered initial and updates data
    let mut merged_data = merge_table_and_update(initial_data, updates_data);

    // Prepend column names if the merged data is not empty
    if !merged_data.is_empty() {
        merged_data.insert(0, column_names);
    }

    Ok(merged_data)
}

fn row_timestamp_is_before(row: &Vec<String>, target_timestamp: &NaiveDateTime) -> bool {
    if let Some(timestamp_str) = row.iter().find(|col| col.contains("-") && col.contains(":")) {
        let timestamp_str = timestamp_str.trim_matches('"'); // Strip quotes if present
        match NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
            Ok(row_timestamp) => row_timestamp <= *target_timestamp, // Compare timestamps
            Err(..) => {
                false // If timestamp parsing fails, exclude the row
            }
        }
    } else {
        false // Exclude rows without a valid timestamp
    }
}

pub async fn get_column_values(
    table_name: &str,
    column_name: &str,
    state: &web::Data<AppState>,
    row_uuid: Option<&str>,
) -> Result<HashSet<String>> {
    let table_data = get_table_data(state.clone(), table_name).await?;
    let schema = state.schema.lock().unwrap();
    let initial_table_name = format!("{}_initial", table_name);
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Table '{}' not found in schema", initial_table_name),
        )
    })?;

    // Find the index of the specified column, handling case where the column is not present
    let column_index = table.columns.iter().position(|col| col.name == column_name);
    let uuid_index = table.columns.iter().position(|col| col.name.to_lowercase() == "uuid");


    let mut values = HashSet::new();
    // Iterate and extract values for the specified column if it exists, or handle missing column case
    for row in table_data {
        // Skip rows with the specified UUID if `row_uuid` is provided
        if let (Some(uuid_index), Some(row_uuid)) = (uuid_index, row_uuid) {
            // Get and clean the UUID from the row
            if let Some(uuid_raw) = row.get(uuid_index) {
                let cleaned_uuid = uuid_raw.trim_matches('"');
                let cleaned_row_uuid = row_uuid.trim_matches('"');
                // Compare the cleaned versions
                if cleaned_uuid == cleaned_row_uuid {
                    continue;
                }
            } else {
                debug!("No UUID found at index {} in row: {:?}",uuid_index, row);
            }
            if let Some(index) = column_index {
                if let Some(value) = row.get(index) {
                    values.insert(value.trim_matches('"').to_string());
                } else {
                    debug!("Row is missing value at index {:?}", index);
                }
            } else {
                debug!("Column named '{}' not found for table '{}'",column_name, initial_table_name);
            }
        }
    }
    Ok(values)
}

pub async fn get_column_names(
    table_name: &str,
    state: &web::Data<AppState>,
) -> Result<Vec<String>> {
    // Acquire a lock on the schema to read table definitions
    let schema = state.schema.lock().unwrap();

    // Construct the initial table name used in the schema
    let initial_table_name = format!("{}_initial", table_name);

    // Look up the table in the schema
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        std::io::Error::new(
            ErrorKind::NotFound,
            format!("Table '{}' does not exist in the schema", initial_table_name),
        )
    })?;

    // Extract the column names from the `columns` field of the table
    let column_names: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();

    Ok(column_names)
}



#[get("/api/tables/{table_name}")]
async fn get_table_api(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();

    match get_table_data(data.clone(), &table_name).await {
        Ok(table_data) => {
            if table_data.is_empty() {
                return HttpResponse::NotFound().json(serde_json::json!({"error": "Table not found or table data is empty"})); // Return structured JSON for 404
            }

            // Format table data for JSON
            let formatted_data: Vec<Vec<serde_json::Value>> = table_data
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| {
                            if let Ok(num) = value.parse::<serde_json::Number>() {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::String(value.trim_matches('"').to_string())
                            }
                        })
                        .collect()
                })
                .collect();

            match serde_json::to_value(&formatted_data) {
                Ok(json_value) => HttpResponse::Ok().json(json_value),
                Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Serialization error: {}", e)})),
            }
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                HttpResponse::NotFound().json(serde_json::json!({"error": "Table data file not found"}))
            } else {
                HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error retrieving table data: {}", e)}))
            }
        }
    }
}


#[get("/api/tables/{table_name}/at/{timestamp}")]
async fn get_table_at_timestamp_api(
    path: web::Path<(String, String)>,
    data: web::Data<AppState>,
) -> impl Responder {
    let (table_name, timestamp) = path.into_inner(); // Extract table name and timestamp from the path
    match get_table_at_timestamp(data, &table_name, timestamp).await {
        Ok(filtered_data) => {
            // Serialize each value based on its type (number or string)
            let formatted_data: Vec<Vec<serde_json::Value>> = filtered_data
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| {
                            if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                                serde_json::Value::Number(
                                    value
                                        .parse::<serde_json::Number>()
                                        .expect("Invalid number format"),
                                )
                            } else {
                                serde_json::Value::String(value.trim_matches('"').to_string())
                            }
                        })
                        .collect()
                })
                .collect();

            // Convert the formatted table data to JSON and send it in the response
            match serde_json::to_value(&formatted_data) {
                Ok(json_value) => HttpResponse::Ok().json(json_value),
                Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Serialization error: {}", e)})),
            }
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                HttpResponse::NotFound().json(serde_json::json!({"error": "Table data file not found"}))
            } else {
                HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error retrieving table data: {}", e)}))
            }
        }
    }
}

#[get("/api/tables")]
async fn list_tables_api(state: web::Data<AppState>) -> impl Responder {
    // Acquire a read lock to access the schema
    let schema = state.schema.lock().unwrap();

    // Create a `HashSet` to store unique base table names by removing `_initial` and `_updates` suffixes
    let mut base_table_names = HashSet::new();

    for table_name in schema.tables.keys() {
        if let Some(base_name) = table_name.strip_suffix("_initial") {
            base_table_names.insert(base_name.to_string());
        } else if let Some(base_name) = table_name.strip_suffix("_updates") {
            base_table_names.insert(base_name.to_string());
        } else {
            // If the table doesn't have any special suffixes, add it as-is
            base_table_names.insert(table_name.clone());
        }
    }

    // Convert the `HashSet` to a sorted `Vec` for consistent output
    let mut table_list: Vec<String> = base_table_names.into_iter().collect();
    table_list.sort();

    // Return JSON containing the list of base table names
    HttpResponse::Ok().json(table_list)
}

#[get("/api/tables/{table_name}/columns")]
async fn get_column_names_api(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();

    // Call the `get_column_names` function
    match get_column_names(&table_name, &data).await {
        Ok(column_names) => HttpResponse::Ok().json(column_names),
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                HttpResponse::NotFound().json(serde_json::json!({"error": e.to_string()}))
            } else {
                HttpResponse::InternalServerError().json(serde_json::json!({"error": e.to_string()}))
            }
        }
    }
}


#[cfg(test)]
mod table_tests {

}