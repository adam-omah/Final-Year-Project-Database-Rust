// Table.rs

use crate::schema::{schema::create_table as schema_create_table, schema::Table};
use crate::AppState;
use actix_web::{get, web, HttpResponse, Responder};
use std::fs::{OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind, Result, Write};
use std::path::Path;
use crate::query::parser::Identifier;
use futures::future::BoxFuture;
use std::collections::HashMap;
use tracing::log::debug;
use crate::schema::schema::{check_column_rules, is_valid_data_type};

pub fn create_table(table: &Table, state: &web::Data<AppState>) -> Result<()> {
    let mut schema = state.schema.lock().unwrap();
    schema_create_table(&mut schema, table.clone(), &state.config)?;
    drop(schema);
    Ok(())
}

pub fn insert_row(
    table_name: &str,
    row_data: Vec<String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    // Convert the `row_data` into a HashMap of column names to values
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

    let mut row_data_map = HashMap::new();
    for (index, column) in table.columns.iter().enumerate() {
        row_data_map.insert(column.name.clone(), row_data.get(index).cloned().unwrap_or_default());
    }

    // Validate and process the row
    let validated_row = validate_and_process_row(table_name, row_data_map, state)?;

    let initial_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&initial_table_name);

    // Check if the `_initial` table file exists
    if !initial_table_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("The '_initial' table file for '{}' does not exist", table_name),
        ));
    }

    // Check if the row already exists in the `_initial` table
    let file = OpenOptions::new().read(true).open(&initial_table_path)?;
    let reader = BufReader::new(file);

    let serialized_row = validated_row
        .iter()
        .map(|value| {
            if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                value.to_string()
            } else {
                format!("\"{}\"", value)
            }
        })
        .collect::<Vec<String>>()
        .join(",");

    for line in reader.lines() {
        let existing_row = line?;
        if existing_row == serialized_row {
            // Row already exists
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "The row already exists in the initial table.",
            ));
        }
    }

    // Append the validated row to the `_initial` table
    let mut file = OpenOptions::new()
        .append(true)
        .open(&initial_table_path)?;

    writeln!(file, "{}", serialized_row)?;
    Ok(())
}


// Append updates to the `_updates` table.
pub async fn update_row(
    table_name: &str,
    uuid: &str,
    updated_values: HashMap<String, String>, // Represents column updates { column_name -> updated_value }
    state: &web::Data<AppState>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&updates_table_name);

    // Add UUID to the row data
    let mut row_data = updated_values;
    row_data.insert("UUID".to_string(), uuid.to_string());

    // Validate and process the row
    let validated_row = validate_and_process_row(table_name, row_data, state)?;

    // Serialize the row for writing
    let serialized_row = validated_row
        .iter()
        .map(|value| {
            if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                value.to_string()
            } else {
                format!("\"{}\"", value)
            }
        })
        .collect::<Vec<String>>()
        .join(",");

    debug!("Writing to _updates table: {:?}", serialized_row);

    // Append the validated row to the `_updates` table
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&updates_table_path)?;

    writeln!(file, "{}", serialized_row)?;
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
    row_data: HashMap<String, String>,
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
        // Get the value for the column
        let value = row_data.get(&column.name).unwrap_or(&String::new()).to_string();

        // Validate the data type
        if !is_valid_data_type(&column.data_type, &value) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid data type for column '{}'. Expected: {:?}, Found: {}",
                    column.name, column.data_type, value
                ),
            ));
        }

        // Validate and transform the value based on column rules
        let validated_value = check_column_rules(column, &value).and_then(|validated| {
            validated.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Constraint violation for column '{}'", column.name),
                )
            })
        })?;
        validated_row.push(validated_value);
    }

    Ok(validated_row)
}

pub(crate) fn extract_literal_value(identifier: &Identifier) -> String {
    match identifier {
        Identifier::Literal(value, _) => value.clone(),
        Identifier::Name(name) => name.clone(),
        Identifier::Star => "*".to_string(), // Or handle this differently
    }
}

pub async fn recalculate_current(
    state: &web::Data<AppState>,
    table_name: &str,
    initial_data: Vec<Vec<String>>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(&state.config.db_dir)
        .join(&state.config.table_dir)
        .join(&updates_table_name);
    let updates_data = load_table_data_from_file(&updates_table_path)?;

    let merged_data = merge_table_and_updates(initial_data, updates_data);

    // Update the cache:  This is now the ONLY place the "current" data is stored.
    let mut cache = state.cache.lock().unwrap();
    cache.insert(table_name.to_string(), merged_data);

    Ok(()) // No file writing is needed now!
}






fn merge_table_and_updates(
    table_data: Vec<Vec<String>>,
    updates_data: Vec<Vec<String>>,
) -> Vec<Vec<String>> {
    let mut data_by_id = HashMap::new();

    // Index the current data by ID
    for row in table_data.iter() {
        if let Some(id) = row.first() {
            data_by_id.insert(id.clone(), row.clone());
        }
    }

    for update_row in updates_data.iter() {
        if let Some(id) = update_row.first() {
            if let Some(existing_row) = data_by_id.get_mut(id) {
                for (i, value) in update_row.iter().enumerate().skip(1) {
                    if i < existing_row.len() {
                        existing_row[i] = if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                            value.to_string() // Numeric value stays as is
                        } else {
                            format!("\"{}\"", value.trim_matches('"')) // Ensure strings are quoted
                        };
                    }
                }
            } else {
                let new_row = update_row
                    .iter()
                    .map(|value| {
                        if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                            value.to_string() // Numeric value stays as is
                        } else {
                            format!("\"{}\"", value.trim_matches('"')) // Ensure strings are quoted
                        }
                    })
                    .collect::<Vec<String>>();
                data_by_id.insert(id.clone(), new_row);
            }
        }
    }

    data_by_id.into_values().collect()
}

#[get("/tables/{table_name}")]
async fn get_table(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();
    match get_table_data(data, &table_name).await {
        Ok(table_data) => {
            // Serialize each value based on its type
            let formatted_data: Vec<Vec<serde_json::Value>> = table_data
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

