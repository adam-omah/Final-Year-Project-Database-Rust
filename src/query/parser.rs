// Parser.rs

use serde::{Deserialize, Serialize};
use std::str;
use tracing::log::debug;
use uuid::Uuid;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Identifier {
    // Represents identifiers like column names, table names, etc.
    Name(String),
    // Represents literals like numbers or strings, with an optional data type.
    Literal(String, Option<DataType>), // Add Option<DataType> here
    // Represents * in SELECT statements.
    Star,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum DataType { //Make sure this exists here too
    Int,
    String,
    UUID,
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "int" | "integer" => DataType::Int,
            "string" | "text" | "varchar" => DataType::String,  // Handle common string type names
            "uuid" => DataType::UUID,
            _ => panic!("Unsupported data type: {}", s), // Or handle with a Result
        }
    }
}


#[derive(Serialize,Deserialize,Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Comparison {
        left: Identifier,
        operator: String,
        right: Identifier,
    },
}


// Abstract Syntax Trees, Needed for possible valid parsing,
// If Query strings transform into a valid AST Node
// The Executer will then action it.
#[derive(Debug, Serialize, Deserialize,PartialEq, Eq,Clone)]
pub enum ASTNode {
    Select {
        columns: Vec<Identifier>,      // Columns in the SELECT clause
        table: Identifier,             // Table in the FROM clause
        timestamp: Option<String>,     // Optional timestamp after 'AT'
    },
    Where { condition: Expression },
    Insert { table: Identifier, values: Vec<Identifier>, columns: Vec<Identifier> },
    Update { table: Identifier, values: Vec<(Identifier, Identifier)> },
    Delete { table: Identifier },
    Create { table: Identifier, columns: Vec<(Identifier, Identifier)> },
}

#[derive(Deserialize, Serialize,Debug, Clone)]
pub struct ASTNodes(pub Vec<ASTNode>);

fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}


pub fn sql_parser(query_bytes: &[u8]) -> Result<Vec<ASTNode>, String> {
    let query_str = str::from_utf8(query_bytes).map_err(|e| e.to_string())?;
    let mut tokens = tokenize_query(query_str)?;
    let mut ast_nodes = Vec::new();

    let mut index = 0;
    while index < tokens.len() {
        match tokens[index].as_str() {
            "SELECT" => {
                ast_nodes.push(parse_select_clause(&mut tokens, &mut index)?);
            }
            "WHERE" => {
                ast_nodes.push(parse_where_clause(&mut tokens, &mut index)?);
            }
            "CREATE" => {
                ast_nodes.push(parse_create_clause(&mut tokens, &mut index)?);
            }
            "INSERT" => {
                ast_nodes.push(parse_insert_clause(&mut tokens, &mut index)?);
            }
            "UPDATE" => {
                ast_nodes.push(parse_update_clause(&mut tokens, &mut index)?);
            }
            _ => {
                return Err(format!("Unexpected token: {}", tokens[index]));
            }
        }
    }

    Ok(ast_nodes)
}

fn tokenize_query(query_str: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut in_string = false;
    let mut current_token = String::new();

    for char in query_str.chars() {
        if char == '"' {
            in_string = !in_string; // Toggle string mode
            if !in_string {
                tokens.push(current_token.clone());
                current_token.clear();
            }
        } else if in_string {
            current_token.push(char);
        } else if char.is_whitespace() || "()".contains(char) {
            if !current_token.is_empty() {
                tokens.push(current_token.clone());
                current_token.clear();
            }
            if !" ".contains(char) {
                tokens.push(char.to_string());
            }
        } else {
            current_token.push(char);
        }
    }
    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    Ok(tokens)
}

fn parse_select_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    let mut columns = Vec::new();
    let mut timestamp: Option<String> = None;
    *index += 1; // Move past "SELECT"

    // Parse the column list (e.g., `*` or specific columns)
    while *index < tokens.len() && tokens[*index] != "FROM" {
        let identifier = if tokens[*index] == "*" {
            Identifier::Star
        } else {
            Identifier::Name(tokens[*index].to_string())
        };
        columns.push(identifier);
        *index += 1;
    }

    // Parse the mandatory "FROM <table>" clause
    if *index < tokens.len() && tokens[*index] == "FROM" {
        *index += 1; // Move past "FROM"
        if *index < tokens.len() {
            let table = Identifier::Name(tokens[*index].to_string());
            *index += 1; // Move past the table name

            // Parse the optional "AT <timestamp>" clause
            if *index < tokens.len() && tokens[*index] == "AT" {
                *index += 1; // Move past "AT"
                if *index < tokens.len() {
                    let raw_timestamp = tokens[*index].clone();

                    // Normalize the timestamp: Replace 'T' or '%20' with a space
                    let normalized_timestamp = raw_timestamp
                        .replace("%20", " ")  // Replace %20 with a space
                        .replace("T", " ");  // Replace T with a space if it exists

                    timestamp = Some(normalized_timestamp);
                    *index += 1; // Move past the timestamp
                } else {
                    return Err("Expected timestamp after 'AT'".to_string());
                }
            }

            // Return the SELECT AST node
            return Ok(ASTNode::Select {
                columns,
                table,
                timestamp,
            });
        } else {
            return Err("Expected table name after 'FROM'".to_string());
        }
    } else {
        return Err("Missing 'FROM' clause in SELECT statement".to_string());
    }
}

fn normalize_timestamp(input: &str) -> String {
    input.replace("%20", " ").replace("T", " ")
}


fn parse_where_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "WHERE"
    if *index + 2 >= tokens.len() {
        return Err("Invalid WHERE clause".to_string());
    }

    let left = Identifier::Name(tokens[*index].to_string());
    *index += 1;
    let operator = tokens[*index].to_string();
    *index += 1;
    let right = if is_uuid(&tokens[*index]) {
        Identifier::Literal(tokens[*index].to_string(), Some(DataType::UUID))
    } else {
        Identifier::Literal(tokens[*index].to_string(), None)
    };
    *index += 1;

    Ok(ASTNode::Where {
        condition: Expression::Comparison { left, operator, right },
    })
}

fn parse_create_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "CREATE"
    if *index < tokens.len() && tokens[*index] == "TABLE" {
        *index += 1;
        if *index < tokens.len() {
            let table = Identifier::Name(tokens[*index].clone());
            *index += 1;

            if *index < tokens.len() && tokens[*index] == "(" {
                *index += 1;
                let mut columns = Vec::new();

                while *index < tokens.len() && tokens[*index] != ")" {
                    let column_name = Identifier::Name(tokens[*index].clone());
                    *index += 1;

                    if *index < tokens.len() {
                        let column_type = Identifier::Name(tokens[*index].clone());
                        columns.push((column_name, column_type));
                        *index += 1;
                    } else {
                        return Err("Invalid column definition".to_string());
                    }

                    if *index < tokens.len() && tokens[*index] == "," {
                        *index += 1;
                    }
                }
                if *index < tokens.len() && tokens[*index] == ")" {
                    *index += 1;
                    return Ok(ASTNode::Create { table, columns });
                }
            }
        }
    }

    Err("Invalid CREATE clause".to_string())
}

fn parse_insert_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "INSERT"
    if *index < tokens.len() && tokens[*index] == "INTO" {
        *index += 1;
        if *index < tokens.len() {
            let table = Identifier::Name(tokens[*index].to_string());
            *index += 1;

            let mut columns = Vec::new();
            if *index < tokens.len() && tokens[*index] == "(" {
                *index += 1;
                while *index < tokens.len() && tokens[*index] != ")" {
                    columns.push(Identifier::Name(tokens[*index].to_string()));
                    *index += 1;
                    if *index < tokens.len() && tokens[*index] == "," {
                        *index += 1;
                    }
                }
                if *index < tokens.len() && tokens[*index] == ")" {
                    *index += 1;
                } else {
                    return Err("Expected ')' after column list".to_string());
                }
            }

            if *index < tokens.len() && tokens[*index] == "VALUES" {
                *index += 1;
                if *index < tokens.len() && tokens[*index] == "(" {
                    *index += 1;
                    let mut values = Vec::new();
                    while *index < tokens.len() && tokens[*index] != ")" {
                        let value_token = tokens[*index].clone();

                        let identifier = if is_uuid(&value_token) {
                            Identifier::Literal(value_token, Some(DataType::UUID))
                        } else {
                            Identifier::Literal(value_token, None)
                        };
                        values.push(identifier);
                        *index += 1;
                        if *index < tokens.len() && tokens[*index] == "," {
                            *index += 1;
                        }
                    }
                    if *index < tokens.len() && tokens[*index] == ")" {
                        *index += 1;
                        return Ok(ASTNode::Insert {
                            table,
                            columns,
                            values,
                        });
                    } else {
                        return Err("Expected ')' after values list".to_string());
                    }
                } else {
                    return Err("Expected '(' after VALUES".to_string());
                }
            } else {
                return Err("Expected VALUES after table name".to_string());
            }
        } else {
            return Err("Expected table name after INSERT INTO".to_string());
        }
    } else {
        return Err("Expected INTO after INSERT".to_string());
    }
}

fn parse_update_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "UPDATE"
    if *index < tokens.len() {
        let table = Identifier::Name(tokens[*index].to_string());
        *index += 1;

        if *index < tokens.len() && tokens[*index] == "SET" {
            *index += 1;
            let mut values = Vec::new();

            while *index < tokens.len() && tokens[*index] != "WHERE" {
                let column = Identifier::Name(tokens[*index].to_string());
                *index += 1;

                if *index < tokens.len() && tokens[*index] == "=" {
                    *index += 1;
                    let value_token = tokens[*index].clone();

                    let value = if is_uuid(&value_token) {
                        Identifier::Literal(value_token, Some(DataType::UUID))
                    } else {
                        Identifier::Literal(value_token, None)
                    };

                    values.push((column, value));
                    *index += 1;

                    if *index < tokens.len() && tokens[*index] == "," {
                        *index += 1;
                    }
                } else {
                    return Err("Expected '=' after column name in UPDATE".to_string());
                }
            }

            if *index < tokens.len() && tokens[*index] == "WHERE" {
                let where_clause = parse_where_clause(tokens, index)?;
                if let ASTNode::Where { condition } = where_clause {
                    return Ok(ASTNode::Update { table, values }); // WHERE clause can be processed further if needed
                }
            }

            return Ok(ASTNode::Update { table, values });
        } else {
            return Err("Expected 'SET' after table name in UPDATE".to_string());
        }
    } else {
        return Err("Expected table name after UPDATE".to_string());
    }
}



#[cfg(test)]
mod tests {
    use crate::query::parser::DataType::String;
    use super::*;

    #[test]
    fn test_basic_comparison() {
        // let query = b"SELECT id FROM my_table WHERE id = 1";
        // let ast = sql_parser(query).unwrap();
        //
        // assert_eq!(
        //     ast,
        //     vec![
        //         ASTNode::Select { columns: vec![Identifier::Name("id".to_string())] },
        //         ASTNode::From { table: Identifier::Name("my_table".to_string()) },
        //         ASTNode::Where {
        //             condition: Expression::Comparison {
        //                 left: Identifier::Name("id".to_string()),
        //                 operator: "=".to_string(),
        //                 right: Identifier::Literal("1".to_string(), None)
        //             }
        //         },
        //     ]
        // );
    }
    #[test]
    fn test_select_with_timestamp() {
        let query = b"SELECT * FROM users AT 2025-02-11T20:14:26";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Select {
                columns: vec![Identifier::Star],
                table: Identifier::Name("users".to_string()),
                timestamp: Some("2025-02-11T20:14:26".to_string()),
            }]
        );
    }

    #[test]
    fn test_select_star() {
        let query = b"SELECT * FROM users";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Select {
                columns: vec![Identifier::Star],
                table: Identifier::Name("users".to_string()),
                timestamp: None,
            }]
        );
    }

    #[test]
    fn test_create_table_custom() {
        let query = b"CREATE TABLE test_table (col1 Int, col2 String)";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![
                ASTNode::Create {
                    table: Identifier::Name("test_table".to_string()),
                    columns: vec![
                        (Identifier::Name("col1".to_string()), Identifier::Name("Int".to_string())),
                        (Identifier::Name("col2".to_string()), Identifier::Name("String".to_string()))
                    ]
                }
            ]
        );
    }

    #[test]
    fn test_invalid_create_table_missing_paren() {
        let query = b"CREATE TABLE users id INT, name TEXT";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Expected '(' after table name".to_string());
    }

    #[test]
    fn test_invalid_column_definition() {
        let query = b"CREATE TABLE users (id)";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition".to_string());
    }

    #[test]
    fn test_missing_column_type() {
        let query = b"CREATE TABLE users (id, name)";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition".to_string());
    }

    #[test]
    fn test_insert_statement() {
        let query = b"INSERT INTO users (id, name) VALUES (1, \"John Doe\")";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Insert {
                table: Identifier::Name("users".to_string()),
                columns: vec![
                    Identifier::Name("id".to_string()),
                    Identifier::Name("name".to_string())
                ],
                values: vec![
                    Identifier::Literal("1".to_string(), Some(String)),
                    Identifier::Literal("\"John Doe\"".to_string(), Some(String)) // Now parsed correctly
                ]
            }]
        );
    }


    #[test]
    fn test_insert_without_columns() {
        let query = b"INSERT INTO users VALUES (1, \"test\")";
        let ast = sql_parser(query).unwrap();
        assert_eq!(ast, vec![
            ASTNode::Insert {
                table: Identifier::Name("users".to_string()),
                columns: vec![], // No columns specified
                values: vec![
                    Identifier::Literal("1".to_string(), Some(String)),
                    Identifier::Literal("\"test\"".to_string(), Some(String)),
                ],
            },
        ]);
    }
}
