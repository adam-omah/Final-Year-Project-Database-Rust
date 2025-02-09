// Table.rs

use crate::schema::{schema::create_table as schema_create_table, schema::Table};
use crate::AppState;
use actix_web::{get, web, HttpResponse, Responder};
use std::fs::{metadata, File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind, Result, Write};
use std::path::Path;
use crate::query::parser::Identifier;
use futures::future::BoxFuture;
use std::collections::HashMap;
use tracing::log::debug;
use crate::schema::schema::get_column_names_from_schema;

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
    let initial_table_name = format!("{}_initial", table_name);
    let initial_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&initial_table_name);

    // If the `_initial` table does not exist, create and insert the new row
    if metadata(&initial_table_path).is_err() {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&initial_table_path)?;

        let serialized_row = row_data
            .iter()
            .map(|value| {
                if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
                    value.to_string() // Numeric values stay as is
                } else {
                    format!("\"{}\"", value) // String values are wrapped in quotes
                }
            })
            .collect::<Vec<String>>()
            .join(",");

        writeln!(file, "{}", serialized_row)?;
        return Ok(()); // Row inserted into a new `_initial` table
    }

    // If `_initial` exists, check whether the row already exists
    let file = OpenOptions::new().read(true).open(&initial_table_path)?;
    let reader = BufReader::new(file);

    let serialized_row = row_data
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

    // If the row does not exist, append it to the `_initial` table
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

    let column_names = get_column_names_from_schema(state, &table_name.to_string())
        .map_err(|_| Error::new(ErrorKind::NotFound, "Schema not found for table"))?;

    let mut serialized_row = Vec::new();

    // Include the UUID as the first column, ensuring it's correctly quoted
    serialized_row.push(format!("\"{}\"", uuid.trim_matches('"')));

    for column_name in &column_names {
        if column_name.eq_ignore_ascii_case("UUID") {
            continue; // Skip UUID column—it’s already included
        }

        if let Some(updated_value) = updated_values.get(column_name) {
            if updated_value.parse::<i64>().is_ok() || updated_value.parse::<f64>().is_ok() {
                serialized_row.push(updated_value.clone()); // Numeric values stay as is
            } else {
                serialized_row.push(format!("\"{}\"", updated_value)); // Strings are quoted
            }
        } else {
            serialized_row.push(String::new()); // Empty placeholder for missing values
        }
    }

    debug!("Writing to _updates table: {:?}", serialized_row.join(","));

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&updates_table_path)?;

    writeln!(file, "{}", serialized_row.join(","))?;
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

