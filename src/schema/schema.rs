use crate::{DB_DIR, SCHEMA_FILE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Result, Write};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int,
    String,
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


pub fn create_table(schema: &mut Schema, table: Table) -> Result<()> {

    if schema.tables.contains_key(&table.name) {
        return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "Table already exists"));
    }

    schema.tables.insert(table.name.clone(), table);  // Correctly insert into HashMap
    save_schema(schema)?;
    Ok(())
}

pub fn save_schema(schema: &Schema) -> Result<()> {
    let schema_path = Path::new(DB_DIR).join(SCHEMA_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true) // Overwrite existing schema
        .open(&schema_path)?;

    serde_json::to_writer_pretty(&mut file, &schema)?;
    Ok(())

}
pub fn load_schema() -> Result<Schema> {
    let schema_path = Path::new(DB_DIR).join(SCHEMA_FILE);

    if !schema_path.exists() {
        // Create a default schema if the file does not exist
        let default_schema = Schema::default();
        save_schema(&default_schema)?;  // Save an empty/default schema
        return Ok(default_schema);      // Return the default schema
    }

    let file = File::open(&schema_path)?;
    let reader = std::io::BufReader::new(file);

    // Handle potential JSON parsing errors
    match serde_json::from_reader(reader) {
        Ok(schema) => Ok(schema),
        Err(err) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse schema file: {}", err),
            ));
        }
    }
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