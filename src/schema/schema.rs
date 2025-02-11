// Schema.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Result};
use actix_web::web;
use tracing::log::debug;
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use chrono::NaiveDateTime;



// Data types for columns.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int,
    String,
    UUID,
    DateTime,
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "int" | "integer" => DataType::Int,
            "string" | "text" | "varchar" => DataType::String,
            "uuid" => DataType::UUID,
            "datetime" => DataType::DateTime,
            _ => panic!("Unsupported data type: {}", s),
        }
    }
}



#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum RuleType {
    Unique,
    NotNull,
    // Add more rule types as needed (e.g., MinLength, MaxLength, etc.)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rule {
    pub rule_type: RuleType,
    pub action: RuleAction, // Add the action field
    // You can add fields here for rule-specific parameters (e.g., min length value)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RuleAction {
    SetNull,
    SetDefault(String),
    Reject,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub rules: Vec<Rule>, // Rules applied to this column
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(default)]
pub struct Schema {
    pub tables: HashMap<String, Table>, // Logical tables (`_initial` and `_updates` handled separately).
}



pub fn load_schema(config: &DatabaseConfig) -> Result<Schema> {
    let schema_path = config.db_dir.join(&config.schema_file);
    if !schema_path.exists() {
        let default_schema = Schema::default();
        save_schema(&default_schema, config)?;
        return Ok(default_schema);
    }

    let file = File::open(&schema_path)?;
    let reader = BufReader::new(file);

    serde_json::from_reader(reader)
        .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, format!("Failed to parse schema file: {}", err)))
}


pub fn save_schema(schema: &Schema, config: &DatabaseConfig) -> Result<()> {
    let schema_path = config.db_dir.join(&config.schema_file);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&schema_path)?;
    serde_json::to_writer_pretty(file, schema)?;
    Ok(())
}


pub fn create_table(schema: &mut Schema, table: Table, config: &DatabaseConfig) -> Result<()> {
    let initial_table_name = format!("{}_initial", table.name);
    let updates_table_name = format!("{}_updates", table.name);

    // Create the initial table (cloned from the provided 'table')
    let mut initial_table = table.clone();
    initial_table.name = initial_table_name.clone();

    // Create the updates table (using the original 'table' by moving ownership)
    let mut updates_table = table; // Move ownership to avoid another clone
    updates_table.name = updates_table_name.clone();

    // The UUID column definition (same for both initial and updates tables)
    let uuid_column = Column {
        name: "UUID".to_string(),
        data_type: DataType::UUID,
        rules: vec![],
    };

    // The timestamp column definition (same for both initial and updates tables)
    let timestamp_column = Column {
        name: "timestamp".to_string(),
        data_type: DataType::DateTime, // Use the DateTime data type
        rules: vec![], // Add rules if needed
    };

    // Insert the UUID column as the first column in both tables
    initial_table.columns.insert(0, uuid_column.clone());
    updates_table.columns.insert(0, uuid_column.clone());

    // Append the timestamp column to the end of both tables
    initial_table.columns.push(timestamp_column.clone());
    updates_table.columns.push(timestamp_column.clone());

    // Add both tables to the schema
    schema.tables.insert(initial_table_name.clone(), initial_table);
    schema.tables.insert(updates_table_name.clone(), updates_table);

    // Save the updated schema to disk
    save_schema(schema, config)?;

    // Create the empty `_initial` and `_updates` table files
    let initial_table_path = config.db_dir.join(&config.table_dir).join(initial_table_name);
    if !initial_table_path.exists() {
        std::fs::File::create(initial_table_path)?;
    }

    let updates_table_path = config.db_dir.join(&config.table_dir).join(updates_table_name);
    if !updates_table_path.exists() {
        std::fs::File::create(updates_table_path)?;
    }

    Ok(())
}





pub fn check_column_rules(column: &Column, value: &str) -> Result<Option<String>> {
    for rule in &column.rules {
        let mut final_value = Some(value.to_string());  // Default, keep original value

        match rule.rule_type {
            RuleType::NotNull => {
                if value.is_empty() {
                    match &rule.action {

                        RuleAction::SetNull => { final_value = None},
                        RuleAction::SetDefault(default_value) => { final_value = Some(default_value.clone())},
                        RuleAction::Reject => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "NotNull constraint violated",
                            ));
                        }
                    }
                }
            }
            RuleType::Unique => {/*Add unique handling if needed */}
            // Handle other rule types here...

        }

        if final_value.is_none() || final_value.as_ref().unwrap().is_empty() {
            return Ok(None);
        } else {
            return Ok(Some(final_value.unwrap()));
        }
    }
    Ok(Some(value.to_string())) // Return the original value if no rules or rules passed
}

pub fn is_valid_data_type(data_type: &DataType, value: &str) -> bool {
    match data_type {
        DataType::Int => value.parse::<i64>().is_ok(), // Or your desired integer type
        DataType::String => true, // Strings are always valid (for now)
        DataType::UUID => {
            // Strip quotes from the value before validating as a UUID
            let clean_value = value.trim_matches('"');
            uuid::Uuid::parse_str(clean_value).is_ok()
        }
        DataType::DateTime => NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").is_ok(),
    }
}

pub fn get_column_names_from_schema(
    state: &web::Data<AppState>,
    table_name: &String,
) -> Result<Vec<String>> {
    let schema = state.schema.lock().unwrap();
    debug!("Getting column names from schema for table: {}", table_name);

    // Try getting the table directly. If not found, try _initial
    let table = match schema.tables.get(table_name) {
        Some(table) => {Some(table)},
        None => {
            let initial_table_name = format!("{}_initial", table_name);
            schema.tables.get(&initial_table_name)
        }
    };

    match table {
        Some(table) => {
            let column_names = table.columns.iter().map(|col| col.name.clone()).collect();
            debug!("Column names for table {}: {:?}", table_name, column_names);
            Ok(column_names)
        },
        None => {
            let not_found_msg = format!("Table '{}' or '{}_initial' not found", table_name, table_name);
            debug!("{}", not_found_msg);

            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                not_found_msg,
            ))
        }
    }
}

