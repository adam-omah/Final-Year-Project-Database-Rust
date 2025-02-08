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

        let serialized_row = row_data.join(","); // Convert the row into a CSV-like string
        writeln!(file, "{}", serialized_row)?;
        return Ok(()); // Row inserted into a new `_initial` table
    }

    // If `_initial` exists, check whether the row already exists
    let file = OpenOptions::new().read(true).open(&initial_table_path)?;
    let reader = std::io::BufReader::new(file);

    // Convert `row_data` into a string to compare against each line in `_initial`
    let serialized_row = row_data.join(",");

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

    writeln!(file, "{}", serialized_row)?; // Add the new row
    Ok(())
}


// Append updates to the `_updates` table.
pub async fn update_row(
    table_name: &str,
    uuid: &str, // Now takes uuid directly
    updated_values: HashMap<String, String>,
    state: &web::Data<AppState>,
) -> Result<()> {
    let updates_table_name = format!("{}_updates", table_name);
    let updates_table_path = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path())
        .join(&updates_table_name);

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&updates_table_path)?;

    let mut serialized_row = format!("{},", uuid);  // Use provided uuid
    for value in updated_values.values() {
        serialized_row.push_str(&format!("{},", value));
    }
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
                        existing_row[i] = value.to_string();
                    }
                }
            } else {
                let mut new_row = Vec::new();
                for value in update_row.iter() {
                    new_row.push(value.to_string());
                }
                data_by_id.insert(id.clone(), new_row);
            }
        }
    }
    data_by_id.into_values().collect()
}

// API end point for get tables.

#[get("/tables/{table_name}")]
async fn get_table(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();
    let state = data.into_inner();
    let mut cache = state.cache.lock().unwrap();

    if let Some(table_data) = cache.get(&table_name) {
        match serde_json::to_string(&table_data) {
            Ok(json) => return HttpResponse::Ok().body(json),
            Err(e) => return HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
        }
    }

    // Case for where Table is not found in the cache.
    let schema = state.schema.lock().unwrap();
    if let Some(_table) = schema.tables.get(&table_name) {
        let table_path = state.config.db_dir.join(state.config.table_dir.as_path()).join(&table_name);
        if let Ok(file) = File::open(table_path) {
            let reader = BufReader::new(file);
            let mut table_data = Vec::new();

            for line_result in reader.lines() {
                if let Ok(line) = line_result {
                    let row_values: Vec<String> = line
                        .split(',')
                        .map(|s| s.trim_matches('"').to_string())
                        .collect();
                    table_data.push(row_values)
                } else {
                    return HttpResponse::InternalServerError().body("Error reading a line");
                }
            }

            cache.insert(table_name.clone(), table_data.clone());

            match serde_json::to_string(&table_data) {
                Ok(json) => HttpResponse::Ok().body(json),
                Err(e) => HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
            }
        } else {
            HttpResponse::NotFound().body("Table data file not found")
        }
    } else {
        HttpResponse::NotFound().body("Table not found in schema")
    }
}
