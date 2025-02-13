// Table.rs

use crate::schema::{schema::create_table as schema_create_table, schema::Table};
use crate::AppState;
use actix_web::{get, web, HttpResponse, Responder};
use std::fs::{OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind, Read, Result, Seek, Write};
use std::path::Path;
use crate::query::parser::Identifier;
use futures::future::BoxFuture;
use std::collections::HashMap;
use tracing::log::debug;
use crate::schema::schema::{check_column_rules, is_valid_data_type};
use chrono::{NaiveDateTime, Utc};


pub fn create_table(table: &Table, state: &web::Data<AppState>) -> Result<()> {
    let mut schema = state.schema.lock().unwrap();
    schema_create_table(&mut schema, table.clone(), &state.config)?;
    drop(schema);
    Ok(())
}

pub async fn insert_row(
    table_name: &str,
    row_data: Vec<String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    // Acquire schema lock and clone it for validation purposes
    let schema = {
        let schema_guard = state.schema.lock().unwrap();
        schema_guard.clone()
    };

    let initial_table_name = format!("{}_initial", table_name);
    let table = schema.tables.get(&initial_table_name).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Table '{}' does not exist in schema", initial_table_name),
        )
    })?;

    // Prepare and validate row data
    let mut row_data_map = HashMap::new();
    for (index, column) in table.columns.iter().enumerate() {
        row_data_map.insert(column.name.clone(), row_data.get(index).cloned().unwrap_or_default());
    }
    let validated_row = validate_and_process_row(table_name, row_data_map, state)?;

    // Prepare the `_initial` table file path
    let initial_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&initial_table_name);

    // Serialize validated row using `quote_if_needed`
    let serialized_row = validated_row
        .iter()
        .map(|value| quote_if_needed(value)) // Apply `quote_if_needed` before writing to storage
        .collect::<Vec<String>>()
        .join(",");


    // Open the `_initial` table file in append-only mode and write the data
    let mut writer = open_table_file_append_only(&initial_table_path)?;
    write_newline_if_needed(&initial_table_path)?;
    writeln!(writer, "{}", serialized_row)
        .map_err(|e| std::io::Error::new(e.kind(), format!("Failed to write initial values  to file: {}", e)))?;

    // Add the validated row directly to the cache
    add_row_to_cache(table_name, validated_row, state).await?;
    Ok(())
}


// Append updates to the `_updates` table.
pub async fn update_row(
    table_name: &str,
    uuid: &str,
    mut updated_values: HashMap<String, String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&updates_table_name);

    // Add UUID to the row data
    updated_values.insert("UUID".to_string(), uuid.to_string());
    // Validate and process the updated row
    let validated_row = validate_and_process_row(table_name, updated_values, state)?;

    // Serialize the row for writing using `quote_if_needed`
    let serialized_row = validated_row
        .iter()
        .map(|value| {
            if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                value.to_string() // Keep numbers as-is
            } else {
                quote_if_needed(value) // Use helper function for quoting
            }
        })
        .collect::<Vec<String>>()
        .join(",");

    // Open the `_updates` table file in append-only mode and write the data
    let mut writer = open_table_file_append_only(&updates_table_path)?;
    write_newline_if_needed(&updates_table_path)?;
    writeln!(writer, "{}", serialized_row)
        .map_err(|e| std::io::Error::new(e.kind(), format!("Failed to write update to file: {}", e)))?;

    // Recalculate the current state after an update
    let initial_table_name = format!("{}_initial", table_name);
    let initial_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&initial_table_name);

    let initial_data = load_table_data_from_file(&initial_table_path)?;
    recalculate_current(state, table_name, initial_data).await?;

    Ok(())
}




// Utility function for extracting data from `_initial` and `_updates` tables, applying updates, and returning combined data.
pub fn get_table_data(
    state: web::Data<AppState>,
    table_name: &str,
) -> BoxFuture<Result<Vec<Vec<String>>>> {
    Box::pin(async move {
        // Attempt to get from cache (using a shorter lock scope)
        {
            let cache = state.cache.lock().unwrap();
            if let Some(cached_data) = cache.get(table_name) {
                return Ok(cached_data.clone());
            }
        }


        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&state.config.db_dir)
            .join(&state.config.table_dir)
            .join(&initial_table_name);

        let initial_data = load_table_data_from_file(&initial_table_path)?;
        recalculate_current(&state, table_name, initial_data.clone()).await?;  //Pass initial_data
        // Now, the cache *should* have the updated data
        let cache = state.cache.lock().unwrap();
        cache
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Data not found in cache after recalculation"))
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

fn validate_and_process_row(
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
        let validated_value = check_column_rules(column, &value).and_then(|validated| {
            validated.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Constraint violation for column '{}'", column_name),
                )
            })
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
    if value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].to_string() // Remove existing quotes for consistent storage
    } else {
        value.to_string() // Store as-is if there are no quotes
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
        recalculate_current(state, table_name, initial_data).await?;
    }
    Ok(())
}



pub async fn recalculate_current(
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

pub async fn get_table_at_timestamp(
    state: web::Data<AppState>,
    table_name: &String,
    timestamp: String,
) -> Result<Vec<Vec<String>>> {
    let initial_table_name = format!("{}_initial", table_name);
    let updates_table_name = format!("{}_updates", table_name);

    debug!("timestamp: {}", timestamp);

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
    let merged_data = merge_table_and_update(initial_data, updates_data);

    Ok(merged_data)
}

fn row_timestamp_is_before(row: &Vec<String>, target_timestamp: &NaiveDateTime) -> bool {
    if let Some(timestamp_str) = row.iter().find(|col| col.contains("-") && col.contains(":")) {
        let timestamp_str = timestamp_str.trim_matches('"'); // Strip quotes if present
        match NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
            Ok(row_timestamp) => row_timestamp <= *target_timestamp, // Compare timestamps
            Err(e) => {
                debug!(
                    "Failed to parse timestamp '{}' in row. Error: {}",
                    timestamp_str, e
                );
                false // If timestamp parsing fails, exclude the row
            }
        }
    } else {
        false // Exclude rows without a valid timestamp
    }
}

#[get("/tables/{table_name}")]
async fn get_table_api(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();

    match get_table_data(data.clone(), &table_name).await {
        Ok(table_data) => {
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

            match serde_json::to_string(&formatted_data) {
                Ok(json) => HttpResponse::Ok().body(json),
                Err(e) => HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
            }
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                HttpResponse::NotFound().body("Table data file not found")
            } else {
                HttpResponse::InternalServerError().body(format!("Error retrieving table data: {}", e))
            }
        }
    }
}


#[get("/tables/{table_name}/at/{timestamp}")]
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
            match serde_json::to_string(&formatted_data) {
                Ok(json) => HttpResponse::Ok().body(json),
                Err(e) => HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
            }
        }
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                HttpResponse::NotFound().body("Table data file not found")
            } else {
                HttpResponse::InternalServerError().body(format!("Error retrieving table data: {}", e))
            }
        }
    }
}

