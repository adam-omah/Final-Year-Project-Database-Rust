use std::fs::{File, OpenOptions};
use std::io::{Result, Write};
use std::path::Path;

use crate::schema::{schema::create_table as schema_create_table
                    , schema::load_schema,
                    schema::DataType,
                    schema::Table};
// Import necessary items
use crate::schema::schema::check_column_rules;
use crate::config::database_config::DatabaseConfig;

pub fn create_table(table: &Table, config: &DatabaseConfig) -> Result<()> { // Add config parameter
    let mut schema = load_schema(config)?;
    schema_create_table(&mut schema, table.clone(), config)?;

    let table_dir = config.db_dir.join(config.table_dir.as_path()); // Use config for path
    std::fs::create_dir_all(&table_dir)?;

    let table_path = table_dir.join(&table.name);
    File::create(table_path)?;
    Ok(())
}


pub fn insert_row(table_name: &str, row_data: Vec<String>, config: &DatabaseConfig) -> Result<()> { // Add config parameter
    let schema = load_schema(config)?;
    let table = schema.tables.get(table_name).ok_or(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "Table not found",
    ))?;

    if row_data.len() != table.columns.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Data length mismatch",
        ));
    }

    let table_dir = Path::new(config.db_dir.as_path()).join(config.table_dir.as_path());
    let table_path = table_dir.join(table_name);

    let mut file = OpenOptions::new().append(true).create(true).open(table_path)?;
    let mut serialized_row = String::new();

    for (i, value) in row_data.iter().enumerate() {
        let column = &table.columns[i];
        let checked_value = check_column_rules(column, value)?; // Call check_column_rules HERE

        // Handle the Option<String> returned by check_column_rules
        match checked_value {
            Some(final_value) => {
                // Use final_value for serialization if it isn't Null
                match column.data_type {
                    DataType::Int => {
                        match final_value.parse::<i64>() { // Parse the string into a valid integer
                            Ok(int_val) => serialized_row.push_str(&format!("{}", int_val)),
                            Err(_) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid integer",
                                ))
                            }
                        }
                    }
                    DataType::String => serialized_row.push_str(&format!("\"{}\"", final_value)),
                }
            }
            None => {
                serialized_row.push_str("NULL");
            }
        }

        if i < row_data.len() - 1 {
            serialized_row.push_str(",");
        }
    }
    writeln!(file, "{}", serialized_row)?;
    Ok(())
}