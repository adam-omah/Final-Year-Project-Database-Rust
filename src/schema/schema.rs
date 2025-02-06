use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Error, ErrorKind, Result};
use crate::config::database_config::DatabaseConfig;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int,
    String,
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "int" | "integer" => DataType::Int,
            "string" | "text" | "varchar" => DataType::String,  // Handle common string type names
            _ => panic!("Unsupported data type: {}", s), // Or handle with a Result
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
pub struct Schema { //Schema now contains a map of Tables
    pub tables: HashMap<String, Table>
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

    match serde_json::from_reader(reader) {
        Ok(schema) => Ok(schema),
        Err(err) => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Failed to parse schema file: {}", err),
            ));
        }
    }
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
    schema.tables.insert(table.name.clone(), table);
    save_schema(schema, config)?;
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
        // You can add validation for String length, format, etc. here
    }
}
