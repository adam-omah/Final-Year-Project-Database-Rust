// Schema.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Result};
use std::sync::Arc;
use actix_web::web;
use actix_web::web::Data;
use tracing::log::debug;
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use chrono::{NaiveDateTime, Utc};
use regex::Regex;
use crate::change_logging::change_logging::{ChangeLogger, ChangeType};
use crate::executer::executer::evaluate_where_clause;
use crate::query::parser::{Expression, Identifier};
use crate::records::table::{get_column_values};
use crate::replication::replication::replicate_change_to_nodes;

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
pub enum ConstraintType {
    Unique,
    NotNull,
    Check(Expression), // Expression is from your parser
}

#[derive(Serialize, Deserialize, Debug, Clone,PartialEq, Eq)]
pub struct Rule {
    pub constraint_type: ConstraintType,
    pub action: RuleAction,
}


#[derive(Serialize, Deserialize, Debug, Clone,PartialEq, Eq)]
pub enum RuleAction {
    SetNull,
    SetDefault(String),
    Reject,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub rules: Vec<Rule>, // Rules applied to this column
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default,PartialEq, Eq)]
#[serde(default)]
pub struct Schema {
    pub tables: HashMap<String, Table>, // Logical tables (`_initial` and `_updates` handled separately).
}

pub fn map_string_to_rule(rule_str: &str) -> Option<Rule> {
    let parts: Vec<&str> = rule_str.split_whitespace().collect();

    if parts.is_empty() {
        return None;
    }

    let constraint_str = parts[0].to_lowercase();
    let action = None; // CHECK constraints do not support actions directly

    let constraint_type = match constraint_str.as_str() {
        "not" => {
            if parts.len() > 1 && parts[1].to_lowercase() == "null" {
                Some(ConstraintType::NotNull)
            } else {
                None
            }
        }
        "unique" => Some(ConstraintType::Unique),
        "check" => {
            // Assume the CHECK expression starts after 'CHECK' keyword
            let expr_str = parts[1..].join(" ");
            if expr_str.trim().starts_with('(') && expr_str.trim().ends_with(')') {
                let inner_expr = &expr_str[1..expr_str.len() - 1];
                // Attempt to parse inner_expr into a valid Expression
                // (You'll need to implement this parsing function)
                let expression = parse_expression(inner_expr);
                expression.map(ConstraintType::Check)
            } else {
                None
            }
        }
        _ => None,
    };

    match constraint_type {
        Some(c_type) => Some(Rule {
            constraint_type: c_type,
            action: action.unwrap_or(RuleAction::Reject), // Default to Reject as fallback
        }),
        None => None, // Constraint type not recognized
    }
}

// Helper function to parse a CHECK constraint into an `Expression`
fn parse_expression(expr_str: &str) -> Option<Expression> {
    // Regex to match expressions like "age > 18" or "name = 'John'"
    let re = Regex::new(r#"(?i)^\s*([\w\.]+)\s*(=|!=|>|>=|<|<=|LIKE)\s*(['"]?[\w\.]+['"]?)\s*$"#).ok()?;

    // Check if expression matches the regex pattern
    let captures = re.captures(expr_str)?;

    // Extract the captured groups for left operand, operator, and right operand
    let left = captures.get(1)?.as_str().to_string();
    let operator = captures.get(2)?.as_str().to_string();
    let right = captures.get(3)?.as_str().to_string();

    // Determine the type of the left operand
    let left_identifier = if left.starts_with('"') || left.starts_with('\'') {
        Identifier::Literal(left.trim_matches(|c| c == '"' || c == '\'').to_string(), Option::from(DataType::String))
    } else {
        Identifier::Name(left)
    };


    // Determine the type of the right operand
    let right_identifier = if right.starts_with('"') || right.starts_with('\'') {
        Identifier::Literal(right.trim_matches(|c| c == '"' || c == '\'').to_string(), Option::from(DataType::String))
    } else if right.parse::<i64>().is_ok() {
        Identifier::Literal(right, Option::from(DataType::Int))
    } else {
        Identifier::Name(right)
    };

    // Construct an `Expression::Comparison` and return it
    Some(Expression::Comparison {
        left: left_identifier,
        operator,
        right: right_identifier,
    })
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

pub fn refresh_schema(config: &DatabaseConfig, state: &web::Data<AppState>) -> Result<()> {
    // Load the new schema from the configuration
    let new_schema = load_schema(config)?;
    // Acquires a mutable lock on the existing schema
    let mut current_schema = state.schema.lock().map_err(|_|
        std::io::Error::new(ErrorKind::Other, "Failed to acquire schema lock")
    )?;
    // This replaces the contents of the existing schema
    *current_schema = new_schema;
    Ok(())
}




pub fn create_table(
    schema: &mut Schema,
    table: Table,
    config: &DatabaseConfig,
    change_logger: &ChangeLogger,
    state: &web::Data<AppState>,
) -> Result<()> {
    let initial_table_name = format!("{}_initial", table.name);
    let updates_table_name = format!("{}_updates", table.name);

    // Create the initial table (cloned from the provided 'table')
    let mut initial_table = table.clone();
    initial_table.name = initial_table_name.clone();

    // Create the updates table (using the original 'table' by moving ownership)
    let mut updates_table = table.clone(); // Move ownership to avoid another clone
    updates_table.name = updates_table_name.clone();

    // The UUID column definition (same for both initial and updates tables)
    let uuid_column = Column {
        name: "UUID".to_string(),
        data_type: DataType::UUID,
        rules: vec![Rule {
            constraint_type: ConstraintType::NotNull,
            action: RuleAction::Reject,
        }],
    };

    // The timestamp column definition (same for both initial and updates tables)
    let timestamp_column = Column {
        name: "timestamp".to_string(),
        data_type: DataType::DateTime,
        rules: vec![Rule {
            constraint_type: ConstraintType::NotNull,
            action: RuleAction::Reject,
        }],
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

    let log_entry = change_logger.log_change(
        None,
        ChangeType::Create,
        table.name.clone(),
        serde_json::json!({"table_definition": table}),
        None,
        Some(config.database_name.clone()),
    )?;
    // Explicitly replicate this change now clearly added
    replicate_change_to_nodes(Arc::from(state.get_ref().clone()), log_entry);

    Ok(())
}

pub fn drop_table(
    schema: &mut Schema,
    table_name: &str,
    config: &DatabaseConfig,
    change_logger: &ChangeLogger
) -> Result<()> {
    // Create variants for initial and updates tables
    let initial_table = format!("{}_initial", table_name);
    let updates_table = format!("{}_updates", table_name);

    // Remove tables from schema
    schema.tables.remove(&initial_table);
    schema.tables.remove(&updates_table);

    // Save the updated schema
    save_schema(schema, config)?;

    // Delete corresponding table files
    let initial_file_path = config.db_dir.join(&config.table_dir).join(&initial_table);
    let updates_file_path = config.db_dir.join(&config.table_dir).join(&updates_table);

    // Remove table files if they exist
    if initial_file_path.exists() {
        std::fs::remove_file(&initial_file_path)?;
    }

    if updates_file_path.exists() {
        std::fs::remove_file(&updates_file_path)?;
    }

    // Log the table drop operation
    change_logger.log_change(
        None,
        ChangeType::Drop,
        table_name.to_string(),
        serde_json::json!({
            "dropped_tables": [initial_table, updates_table],
            "timestamp": Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        }),
        None,
        Option::from(config.database_name.clone())
    )?;

    Ok(())
}


pub async fn check_column_rules(
    column: &Column,
    value: &str, table_name: &str,
    state: &web::Data<AppState>,
    row_uuid: Option<&str>
) -> Result<Option<String>> {
    let mut final_value = Some(value.to_string()); // Start with the original value
    for rule in &column.rules {
        match &rule.constraint_type { // Use constraint_type
            ConstraintType::NotNull => {
                if value.is_empty() {
                    match &rule.action {
                        RuleAction::SetNull => final_value = None,
                        RuleAction::SetDefault(default_value) => final_value = Some(default_value.clone()),
                        RuleAction::Reject => {
                            return Err(std::io::Error::new(
                                ErrorKind::InvalidData,
                                "NotNull constraint violated",
                            ));
                        }
                    }
                }
            }
            ConstraintType::Unique => {
                let existing_values = get_column_values(table_name, &column.name, state, row_uuid).await?;
                if existing_values.contains(value) {
                    match &rule.action {
                        RuleAction::SetNull => final_value = None,
                        RuleAction::SetDefault(default_value) => final_value = Some(default_value.clone()),
                        RuleAction::Reject => {
                            return Err(std::io::Error::new(
                                ErrorKind::InvalidData,
                                format!("Unique constraint violated for column '{}', Value '{}' already exists in table.", column.name, value),
                            ));
                        }
                    }
                }
            },
            ConstraintType::Check(expression) => {
                let column_names = vec![column.name.clone()]; // Current column
                let row = vec![value.to_string()]; // Treat the value as a "row" for evaluation

                let is_valid = evaluate_where_clause(expression, &row, &column_names);

                if !is_valid {
                    match &rule.action {
                        RuleAction::SetNull => final_value = None,
                        RuleAction::SetDefault(default_value) => final_value = Some(default_value.clone()),
                        RuleAction::Reject => {
                            return Err(std::io::Error::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "Check constraint violated for column '{}'. Expression {:?} does not hold.",
                                    column.name, expression
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(final_value)
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
            Ok(column_names)
        },
        None => {
            let not_found_msg = format!("Table '{}' or '{}_initial' not found", table_name, table_name);
            Err(std::io::Error::new(
                ErrorKind::NotFound,
                not_found_msg,
            ))
        }
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    use std::path::PathBuf;
    use mockall::mock;

    // Mock AppState for testing
    mock! {
        AppState {
            fn schema(&self) -> std::sync::MutexGuard<'_, Schema>;
        }
    }

    #[test]
    fn test_data_type_from_str() {
        assert_eq!(DataType::from("int"), DataType::Int);
        assert_eq!(DataType::from("integer"), DataType::Int);
        assert_eq!(DataType::from("string"), DataType::String);
        assert_eq!(DataType::from("text"), DataType::String);
        assert_eq!(DataType::from("varchar"), DataType::String);
        assert_eq!(DataType::from("uuid"), DataType::UUID);
        assert_eq!(DataType::from("datetime"), DataType::DateTime);
    }

    #[test]
    #[should_panic(expected = "Unsupported data type: invalid")]
    fn test_data_type_from_str_invalid() {
        DataType::from("invalid");
    }

    #[test]
    fn test_map_string_to_rule() {
        // Test NOT NULL constraint
        let rule = map_string_to_rule("NOT NULL").unwrap();
        assert!(matches!(rule.constraint_type, ConstraintType::NotNull));
        assert!(matches!(rule.action, RuleAction::Reject));

        // Test UNIQUE constraint
        let rule = map_string_to_rule("UNIQUE").unwrap();
        assert!(matches!(rule.constraint_type, ConstraintType::Unique));
        assert!(matches!(rule.action, RuleAction::Reject));

        // Test CHECK constraint
        let rule = map_string_to_rule("CHECK (age > 18)").unwrap();
        match rule.constraint_type {
            ConstraintType::Check(Expression::Comparison { left, operator, right }) => {
                assert_eq!(left, Identifier::Name("age".to_string()));
                assert_eq!(operator, ">".to_string());
                assert_eq!(right, Identifier::Literal("18".to_string(), Some(DataType::Int)));
            },
            _ => panic!("Expected Check constraint"),
        }
    }

    #[test]
    fn test_parse_expression() {
        // Test basic comparison
        let expr = parse_expression("age > 18").unwrap();
        match expr {
            Expression::Comparison { left, operator, right } => {
                assert_eq!(left, Identifier::Name("age".to_string()));
                assert_eq!(operator, ">".to_string());
                assert_eq!(right, Identifier::Literal("18".to_string(), Some(DataType::Int)));
            },
            _ => panic!("Expected Comparison expression"),
        }

        // Test string comparison
        let expr = parse_expression("name = 'John'").unwrap();
        match expr {
            Expression::Comparison { left, operator, right } => {
                assert_eq!(left, Identifier::Name("name".to_string()));
                assert_eq!(operator, "=".to_string());
                assert_eq!(right, Identifier::Literal("John".to_string(), Some(DataType::String)));
            },
            _ => panic!("Expected Comparison expression"),
        }
    }

    #[test]
    fn test_is_valid_data_type() {
        // Test Integer validation
        assert!(is_valid_data_type(&DataType::Int, "123"));
        assert!(!is_valid_data_type(&DataType::Int, "abc"));

        // Test String validation
        assert!(is_valid_data_type(&DataType::String, "any string"));

        // Test UUID validation
        assert!(is_valid_data_type(&DataType::UUID, "550e8400-e29b-41d4-a716-446655440000"));
        assert!(!is_valid_data_type(&DataType::UUID, "invalid-uuid"));

        // Test DateTime validation
        assert!(is_valid_data_type(&DataType::DateTime, "2024-03-21 15:30:00"));
        assert!(!is_valid_data_type(&DataType::DateTime, "invalid-date"));
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let mut schema = Schema::default();
        let config = DatabaseConfig {
            db_dir: PathBuf::from("test_db"),
            schema_file: "schema.json".to_string().parse().unwrap(),
            table_dir: "tables".to_string().parse().unwrap(),
            database_name: "".to_string(),
            log_dir: "test_logs".parse().unwrap(),
            log_file: "test_logger.json".to_string(),
        };

        let change_logger = ChangeLogger::new(config.clone().log_dir,config.clone().log_file);

        // Create test directories
        std::fs::create_dir_all(config.db_dir.join(&config.table_dir))?;

        let table = Table {
            name: "test_table".to_string(),
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    rules: vec![],
                },
            ],
        };

        create_table(&mut schema, table, &config, &change_logger)?;

        // Verify both initial and updates tables were created
        assert!(schema.tables.contains_key("test_table_initial"));
        assert!(schema.tables.contains_key("test_table_updates"));

        // Cleanup
        std::fs::remove_dir_all("test_db")?;
        Ok(())
    }
}