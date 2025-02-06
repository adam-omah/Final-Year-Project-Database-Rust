// Import necessary items
use crate::schema::{schema::create_table as schema_create_table,
                    schema::DataType,
                    schema::Table,
                    schema::check_column_rules,
                    schema::is_valid_data_type};
use crate::AppState;
use actix_web::{get, web, HttpResponse, Responder};
use std::fs::{metadata, File, OpenOptions};
use std::io::{BufRead, BufReader, Result, Write};
use std::path::Path;

pub fn create_table(table: &Table, state: &web::Data<AppState>) -> Result<()> {
    let mut schema = state.schema.lock().unwrap();
    schema_create_table(&mut schema, table.clone(), &state.config)?;
    drop(schema); // Release the lock on schema early

    let table_dir = state.config.db_dir.join(state.config.table_dir.as_path());
    std::fs::create_dir_all(&table_dir)?;

    let table_path = table_dir.join(&table.name);
    File::create(table_path)?;

    let mut cache = state.cache.lock().unwrap();
    cache.insert(table.name.clone(), Vec::new()); // Initialize cache for new table
    Ok(())
}

pub fn insert_row(
    table_name: &str,
    row_data: Vec<String>,
    columns: Option<Vec<String>>,
    state: &web::Data<AppState>,
) -> Result<()> {
    let mut cache = state.cache.lock().unwrap();

    // Load table data into cache if not present
    if !cache.contains_key(table_name) {
        let table_dir = Path::new(state.config.db_dir.as_path())
            .join(state.config.table_dir.as_path());
        let table_path = table_dir.join(table_name);

        if metadata(&table_path).is_ok() {
            // Load existing data from file
            let file = File::open(&table_path)?;
            let reader = BufReader::new(file);
            let mut table_data = Vec::new();

            for line in reader.lines() {
                let line = line?;
                let row_values: Vec<String> = line
                    .split(',')
                    .map(|s| s.trim_matches('"').to_string())
                    .collect();
                table_data.push(row_values);
            }

            cache.insert(table_name.to_string(), table_data);
        } else {
            // New table, initialize empty cache
            cache.insert(table_name.to_string(), Vec::new());
        }
    }

    let schema = state.schema.lock().unwrap();
    let table = schema.tables.get(table_name)
        .ok_or_else(|| std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Table not found in schema"
        ))?;

    // Validate columns and data lengths
    if let Some(cols) = &columns {
        if cols.len() != row_data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "The number of columns specified does not equal the number of values given"
            ));
        }

        for col_name in cols.iter() {
            if !table.columns.iter().any(|c| c.name == *col_name) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Column {} does not exist in table {}", col_name, table_name)
                ));
            }
        }

    } else if row_data.len() != table.columns.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Data length mismatch"
        ));
    }




    let mut processed_row_data: Vec<String> = Vec::new();

    if let Some(cols) = &columns {
        for (i, col_name) in cols.iter().enumerate() {
            let col = table.columns.iter().find(|c| c.name == *col_name).unwrap();
            let value = &row_data[i];

            if !is_valid_data_type(&col.data_type, value) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid data type for column: {}", col_name)
                ));
            }
            match check_column_rules(col, value) {
                Ok(Some(val)) => processed_row_data.push(val),
                Ok(None) => processed_row_data.push(String::new()),
                Err(e) => return Err(e),
            }
        }
    } else {
        for (_i, (value, col)) in row_data.iter().zip(table.columns.iter()).enumerate() {
            if !is_valid_data_type(&col.data_type, value) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid data type for column: {}", col.name)
                ));
            }
            match check_column_rules(col, value) {
                Ok(Some(val)) => processed_row_data.push(val),
                Ok(None) => processed_row_data.push(String::new()),
                Err(e) => return Err(e),
            }
        }
    }



    // Update cache
    if let Some(cached_table) = cache.get_mut(table_name) {
        cached_table.push(processed_row_data.clone());
    }

    // Write to file
    let table_dir = Path::new(state.config.db_dir.as_path())
        .join(state.config.table_dir.as_path());
    let table_path = table_dir.join(table_name);
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(table_path)?;

    let mut serialized_row = String::new();
    // If columns are provided, use their order for serialization
    if let Some(cols) = columns {
        for (i, col_name) in cols.iter().enumerate() {
            let col_index = table.columns.iter().position(|c| c.name == *col_name).unwrap();

            if i > 0 {
                serialized_row.push(',');
            }
            if let DataType::String = table.columns[col_index].data_type {
                serialized_row.push('"');
                serialized_row.push_str(&processed_row_data[i]);
                serialized_row.push('"');
            } else {
                serialized_row.push_str(&processed_row_data[i]);
            }
        }
    } else {
        // Otherwise, serialize data according to table schema order
        for (i, value) in processed_row_data.iter().enumerate() {
            if i > 0 {
                serialized_row.push(',');
            }
            if let DataType::String = table.columns[i].data_type {
                serialized_row.push('"');
                serialized_row.push_str(value);
                serialized_row.push('"');
            } else {
                serialized_row.push_str(value);
            }
        }
    }
    writeln!(file, "{}", serialized_row)?;
    Ok(())
}



pub(crate) async fn get_table_data(state: web::Data<AppState>, table_name: &str) -> std::result::Result<Vec<Vec<String>>, std::io::Error> {
    let mut cache = state.cache.lock().unwrap();
    if let Some(table_data) = cache.get(table_name) {
        Ok(table_data.clone())
    } else {
        let schema = state.schema.lock().unwrap();

        if schema.tables.contains_key(table_name) {
            let table_path = state.config.db_dir.join(state.config.table_dir.as_path()).join(table_name);
            let file = File::open(table_path)?;
            let reader = BufReader::new(file);
            let mut table_data = Vec::new();
            for line_result in reader.lines() {
                if let Ok(line) = line_result {
                    let row_values: Vec<String> = line.split(',').map(|s| s.trim_matches('"').to_string()).collect();
                    table_data.push(row_values);

                } else {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "Error reading a line"));
                }
            }
            cache.insert(table_name.to_string(), table_data.clone());
            Ok(table_data)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Table not found"))
        }
    }
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
