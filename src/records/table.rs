// Import necessary items
use crate::schema::schema::check_column_rules;
use crate::schema::{schema::create_table as schema_create_table
                    ,
                    schema::DataType,
                    schema::Table};
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

pub fn insert_row(table_name: &str, row_data: Vec<String>, state: &web::Data<AppState>) -> Result<()> {
    let mut cache = state.cache.lock().unwrap();
    let column_types: Vec<DataType>;

    // 1. Check Cache First
    if let Some(cached_table) = cache.get_mut(table_name) {
        // Lock schema to get column types
        {
            let schema = state.schema.lock().unwrap();
            let table = schema.tables.get(table_name).expect("Table not found in schema, even though it's in the cache. This is a serious internal error.");
            if row_data.len() != table.columns.len() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Data length mismatch"));
            }
            column_types = table.columns.iter().map(|c| c.data_type.clone()).collect();
        } // schema lock dropped here

        // Process cached table using pre-loaded column_types
        let mut serialized_row = String::new();
        for (i, value) in row_data.iter().enumerate() {
            let checked_value = check_column_rules(&state.schema.lock().unwrap().tables[table_name].columns[i], value)?;

            match checked_value {
                Some(final_value) => {
                    if column_types[i] == DataType::Int { // Access column_types
                        match final_value.parse::<i64>() {
                            Ok(int_val) => serialized_row.push_str(&format!("{}", int_val)),
                            Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid integer")),
                        }
                    } else if column_types[i] == DataType::String { // Access column_types
                        serialized_row.push_str(&format!("\"{}\"", final_value));
                    }
                },
                None => serialized_row.push_str("NULL"),
            }
            if i < row_data.len() - 1 {
                serialized_row.push_str(",");
            }
        }
        cached_table.push(row_data.clone());
    } else {
        // 2. Check if File Exists on Filesystem (New Step)
        let table_dir = Path::new(state.config.db_dir.as_path()).join(state.config.table_dir.as_path());
        let table_path = table_dir.join(table_name);

        if metadata(&table_path).is_ok() {
            // File exists, but not in cache. Load from file system later and insert into cache.
            cache.insert(table_name.to_string(), Vec::new()); // Initialize the entry with an empty vector
        } else {
            // 3. If File Doesn't Exist, Check Schema
            let schema = state.schema.lock().unwrap();

            let table = schema.tables.get(table_name).ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Table not found in schema or filesystem", // Updated error message
            ))?;

            if row_data.len() != table.columns.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Data length mismatch",
                ));
            }

            drop(schema);
            cache.insert(table_name.to_string(), vec![row_data.clone()]); // Add to cache
        }
    }

    // Write to File (All cases write to the file, but using the correct information source)
    let table_dir = Path::new(state.config.db_dir.as_path()).join(state.config.table_dir.as_path());
    let table_path = table_dir.join(table_name);
    let mut file = OpenOptions::new().append(true).create(true).open(table_path)?;

    // Serialization is the same regardless of the source
    let row_to_serialize = &cache[table_name].last().unwrap();
    let mut serialized_row = String::new();
    for (i, value) in row_to_serialize.iter().enumerate() {
        if i > 0 {
            serialized_row.push(',');
        }
        if let DataType::String = state.schema.lock().unwrap().tables[table_name].columns[i].data_type {
            serialized_row.push('"');
            serialized_row.push_str(value);
            serialized_row.push('"');
        } else {
            serialized_row.push_str(value);
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
