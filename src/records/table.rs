
use std::fs::{File, OpenOptions};
use std::io::{Result, Write};
use std::path::Path;
use serde_json;
use crate::schema::{schema::Column,schema::DataType, schema::Schema};
use crate::DB_DIR;

pub fn create_table(schema: &Schema) -> Result<()> {
    let table_path = Path::new(DB_DIR).join(&schema.table_name);
    File::create(table_path)?;
    Ok(())
}

pub fn insert_row(schema: &Schema, row_data: Vec<String>) -> Result<()> {
    if row_data.len() != schema.columns.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Data length mismatch",
        ));
    }

    let table_path = Path::new(DB_DIR).join(&schema.table_name);
    let mut file = OpenOptions::new().append(true).open(table_path)?;

    let mut serialized_row = String::new();
    for (i, value) in row_data.iter().enumerate() {

        let column = &schema.columns[i];


        match column.data_type {
            DataType::Int => {
                // Attempt to parse as integer, handling errors
                match value.parse::<i64>() {
                    Ok(int_val) => serialized_row.push_str(&format!("{}", int_val)),
                    Err(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid integer",
                        ))
                    }
                }
            }

            DataType::String => {
                serialized_row.push_str(&format!("\"{}\"", value));
            }


        }

        if i < row_data.len() - 1 {
            serialized_row.push_str(",");
        }


    }
    writeln!(file, "{}", serialized_row)?;
    Ok(())
}