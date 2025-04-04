// Parser.rs

use serde::{Deserialize, Serialize};
use std::str;
use uuid::Uuid;
use crate::schema::schema::{DataType};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Hash)]
pub enum Identifier {
    // Represents identifiers like column names, table names, etc.
    Name(String),
    // Represents literals like numbers or strings, with an optional data type.
    Literal(String, Option<DataType>),
    // Represents * in SELECT statements.
    Star,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Hash)]
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
        limit: Option<u64>,            // Optional Limit tag
    },
    Where { condition: Expression },
    Insert { table: Identifier, values: Vec<Identifier>, columns: Vec<Identifier> },
    Update { table: Identifier, values: Vec<(Identifier, Identifier)> },
    Delete { table: Identifier },
    Create {
        table: Identifier,
        columns: Vec<(Identifier, Identifier, Vec<String>)>},
    Drop { table: Identifier },
}

#[derive(Deserialize, Serialize,Debug, Clone)]
pub struct ASTNodes(pub Vec<ASTNode>);

fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}
pub fn is_numeric_literal(s: &str) -> bool {
    s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok()
}


pub fn is_datetime(s: &str) -> bool {
    // Try parsing common datetime formats
    if chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok() {
        return true; // Example: "2025-02-16 20:10:00"
    }
    if chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok() {
        return true; // Example: "2025-02-16T20:10:00"
    }
    false // Return false if format not matched
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
            "DELETE" => {
                ast_nodes.push(parse_delete_clause(&mut tokens, &mut index)?);
            }
            "DROP" =>{
                ast_nodes.push(parse_drop_clause(&mut tokens, &mut index)?);
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
            if !in_string {  // Closing quote
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            } else { // Opening quote
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
        } else if in_string {
            current_token.push(char);
        } else if char == ',' && !in_string {  // Treat comma as a separate token outside strings
            if !current_token.is_empty() {
                tokens.push(current_token.trim().to_string()); // Trim whitespace
                current_token.clear();
            }
            tokens.push(",".to_string()); // Add the comma as a token
        }
        else if char == '(' && !in_string {
            if !current_token.is_empty() {
                tokens.push(current_token.trim().to_string());
                current_token.clear();
            }
            tokens.push("(".to_string());
        } else if char == ')' && !in_string {
            if !current_token.is_empty() {
                tokens.push(current_token.trim().to_string());
                current_token.clear();
            }
            tokens.push(")".to_string());
        }
        else if char.is_whitespace() {
            if !current_token.is_empty() {
                tokens.push(current_token.trim().to_string()); // Trim whitespace
                current_token.clear();
            }
        } else if char == ';' && !in_string {
            // end token loop
            break;
        } else {
            current_token.push(char);
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token.trim().to_string());
    }

    Ok(tokens)
}

fn parse_select_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    let mut columns = Vec::new();
    let mut timestamp: Option<String> = None;
    let mut limit: Option<u64> = None;
    *index += 1; // Move past "SELECT"

    // Parse the column list (e.g., `*` or specific columns)
    while *index < tokens.len() && tokens[*index] != "FROM" {
        let identifier = if tokens[*index] == "*" {
            Identifier::Star
        } else if tokens[*index] == "," {
            *index += 1;
            continue
        }
        else {
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

            // Parse the optional "LIMIT <number>" clause
            if *index < tokens.len() && tokens[*index].to_uppercase() == "LIMIT" { // Use uppercase
                *index += 1; // Move past "LIMIT"
                if *index < tokens.len() {
                    // Try to parse the next token as a u64 number
                    match tokens[*index].parse::<u64>() {
                        Ok(limit_val) => {
                            limit = Some(limit_val);
                            *index += 1; // Move past the number
                        }
                        Err(_) => {
                            return Err(format!(
                                "Expected a non-negative integer after 'LIMIT', found: {}",
                                tokens[*index]
                            ));
                        }
                    }
                } else {
                    return Err("Expected number after 'LIMIT'".to_string());
                }
            }

            // Return the SELECT AST node
            Ok(ASTNode::Select {
                columns,
                table,
                timestamp,
                limit,
            })
        } else {
            Err("Expected table name after 'FROM'".to_string())
        }
    } else {
        Err("Missing 'FROM' clause in SELECT statement".to_string())
    }
}

// fn normalize_timestamp(input: &str) -> String {
//     input.replace("%20", " ").replace("T", " ")
// }


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
                    // Parse column name
                    if *index >= tokens.len() {
                        return Err("Invalid column definition: Missing column name.".into());
                    }
                    let column_name = Identifier::Name(tokens[*index].clone());
                    *index += 1;
                    // Parse column type
                    if *index >= tokens.len() || tokens[*index] == "," || tokens[*index] == ")" {
                        return Err(format!(
                            "Invalid column definition for '{}': Missing data type.",
                            match &column_name {
                                Identifier::Name(name) => name,
                                _ => "unknown",
                            }
                        ));
                    }
                    let column_type_str = tokens[*index].clone();
                    let column_type = Identifier::Name(column_type_str.clone());
                    *index += 1;
                    // Parse optional constraints
                    let mut constraints = Vec::new();
                    while *index < tokens.len() && tokens[*index] != "," && tokens[*index] != ")" {
                        if tokens[*index].to_uppercase() == "CHECK" && *index + 1 < tokens.len() {
                            *index += 1;
                            // Parse CHECK constraint
                            if tokens[*index] == "(" {
                                *index += 1;
                                if *index + 3 < tokens.len() {
                                    let identifier = tokens[*index].clone(); // Column name or identifier
                                    *index += 1;
                                    let operator = tokens[*index].clone(); // Operator (e.g., >, <, =)
                                    *index += 1;
                                    let value = tokens[*index].clone(); // Value or constant
                                    *index += 1;

                                    if tokens[*index] == ")" {
                                        constraints.push(format!("CHECK ({} {} {})", identifier, operator, value));
                                        *index += 1;
                                    } else {
                                        return Err("Invalid CHECK constraint: Missing ')'.".into());
                                    }
                                } else {
                                    return Err("Invalid CHECK constraint: Incomplete expression.".into());
                                }
                            } else {
                                return Err("Invalid CHECK constraint: Missing '('.".into());
                            }
                        } else {
                            // Parse other constraints (e.g., UNIQUE or NOT NULL)
                            constraints.push(tokens[*index].clone());
                            *index += 1;
                        }
                    }
                    columns.push((column_name, column_type, constraints));

                    // Skip comma if it's a separator between columns
                    if *index < tokens.len() && tokens[*index] == "," {
                        *index += 1;
                    }
                }

                // Ensure the closing parenthesis exists
                if *index < tokens.len() && tokens[*index] == ")" {
                    *index += 1;
                    Ok(ASTNode::Create { table, columns })
                } else {
                    Err("Expected ')' after column definitions.".into())
                }
            } else {
                Err("Expected '(' after table name.".into())
            }
        } else {
            Err("Expected table name after 'CREATE TABLE'.".into())
        }
    } else {
        Err("Expected 'TABLE' after 'CREATE'.".into())
    }
}

fn parse_delete_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "DELETE"
    if *index < tokens.len() && tokens[*index] == "FROM" {
        *index += 1; // Move past "FROM"
        if *index < tokens.len() {
            let table = Identifier::Name(tokens[*index].to_string());
            *index += 1;

            if *index < tokens.len() && tokens[*index] == "WHERE" {
                Ok(ASTNode::Delete { table })
            } else {
                // Return an error if there is no WHERE clause
                Err("DELETE statement must include a WHERE clause".to_string())
            }
        } else {
            Err("Expected table name after DELETE FROM".to_string())
        }
    } else {
        Err("Expected FROM after DELETE".to_string())
    }
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
                        let identifier = match_token_to_identifier(&value_token);
                        values.push(identifier);
                        *index += 1;

                        if *index < tokens.len() && tokens[*index] == "," {
                            *index += 1; // Skip comma
                        }
                    }
                    if *index < tokens.len() && tokens[*index] == ")" {
                        *index += 1;
                        Ok(ASTNode::Insert {
                            table,
                            columns,
                            values,
                        })
                    } else {
                        Err("Expected ')' after values list".to_string())
                    }
                } else {
                    Err("Expected '(' after VALUES".to_string())
                }
            } else {
                Err("Expected VALUES after table name".to_string())
            }
        } else {
            Err("Expected table name after INSERT INTO".to_string())
        }
    } else {
        Err("Expected INTO after INSERT".to_string())
    }
}

fn parse_update_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    *index += 1; // Move past "UPDATE"

    // Parse table name
    if *index >= tokens.len() {
        return Err("Expected table name after 'UPDATE'".to_string());
    }
    let table = Identifier::Name(tokens[*index].to_string());
    *index += 1;

    // Parse `SET` keyword
    if *index >= tokens.len() || tokens[*index] != "SET" {
        return Err("Expected 'SET' after table name in 'UPDATE'".to_string());
    }
    *index += 1;

    // Parse key-value pairs in the `SET` clause
    let mut values = Vec::new();
    while *index < tokens.len() && tokens[*index] != "WHERE" {
        // Parse column name
        let column = if *index < tokens.len() {
            Identifier::Name(tokens[*index].to_string())
        } else {
            return Err("Expected column name in 'SET' clause".to_string());
        };
        *index += 1;

        // Parse assignment operator `=`
        if *index >= tokens.len() || tokens[*index] != "=" {
            return Err(format!(
                "Expected '=' after column name '{}'",
                match column {
                    Identifier::Name(ref name) => name.clone(),
                    _ => String::from("unknown"),
                }
            ));
        }
        *index += 1;

        // Parse value
        if *index >= tokens.len() {
            return Err("Expected value after '=' in 'SET' clause".to_string());
        }
        let value_token = tokens[*index].clone();
        let identifier = match_token_to_identifier(&value_token);
        *index += 1;

        // Add the parsed value pair to the set
        values.push((column, identifier));

        // Skip commas if multiple assignments
        if *index < tokens.len() && tokens[*index] == "," {
            *index += 1;
        }
    }

    // Enforce the presence of the "WHERE" clause
    if *index >= tokens.len() || tokens[*index] != "WHERE" {
        Err("Expected 'WHERE' clause after 'SET' in 'UPDATE'".to_string())
    } else {
        Ok(ASTNode::Update { table, values }) // Finalize and return the ASTNode::Update
    }
}

fn match_token_to_identifier(value_token: &String) -> Identifier {
    
    if value_token.starts_with('"') && value_token.ends_with('"') {
        // Handle string literals
        Identifier::Literal(
            value_token[1..value_token.len() - 1].to_string(),
            Some(DataType::String),
        )
    } else if is_numeric_literal(value_token) {
        // Handle numbers
        Identifier::Literal(value_token.parse().unwrap(), Some(DataType::Int)) // Adjust to `Float` if decimals are needed
    } else if is_uuid(value_token) {
        // Handle UUIDs
        Identifier::Literal(value_token.to_string(), Some(DataType::UUID))
    } else if is_datetime(value_token) {
        // Handle datetime literals
        Identifier::Literal(value_token.to_string(), Some(DataType::DateTime))
    } else if value_token.chars().any(|c| c.is_alphabetic()) {
        // Fallback: Contains alphabets, assume string
        Identifier::Literal(value_token.to_string(), Some(DataType::String))
    } else {
        // Treat anything else as a generic literal
        Identifier::Literal(value_token.parse().unwrap(), None)
    }
}

fn parse_drop_clause(tokens: &mut Vec<String>, index: &mut usize) -> Result<ASTNode, String> {
    // Check if next token is "TABLE"
    *index += 1;
    if *index >= tokens.len() || tokens[*index].to_uppercase() != "TABLE" {
        return Err("Expected TABLE keyword after DROP".to_string());
    }

    *index += 1;
    // Next token should be the table name
    if *index >= tokens.len() {
        return Err("Missing table name in DROP TABLE statement".to_string());
    }

    let table_name = tokens[*index].clone();
    *index += 1;

    Ok(ASTNode::Drop {
        table: Identifier::Name(table_name)
    })
}



#[cfg(test)]
mod parser_tests {
    use crate::query::parser::DataType::String;
    use crate::schema::schema::DataType::Int;
    use super::*;

    #[test]
    fn test_basic_comparison() {
        let query = b"WHERE age > 18";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Where {
                condition: Expression::Comparison {
                    left: Identifier::Name("age".to_string()),
                    operator: ">".to_string(),
                    right: Identifier::Literal("18".to_string(), None), // Properly parsed as integer
                }
            }]
        );
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
                timestamp: Some("2025-02-11 20:14:26".to_string()),
                limit: None,
            }]
        );
    }

    #[test]
    fn test_select_with_multiple_columns_and_alias() {
        let query = b"SELECT id,name,email FROM users";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Select {
                columns: vec![
                    Identifier::Name("id".to_string()),
                    Identifier::Name("name".to_string()),
                    Identifier::Name("email".to_string())
                ],
                table: Identifier::Name("users".to_string()),
                timestamp: None,
                limit: None,
            }]
        );
    }

    #[test]
    fn test_select_with_star_and_column() {
        let query = b"SELECT *,email FROM users";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Select {
                columns: vec![
                    Identifier::Star,
                    Identifier::Name("email".to_string()),
                ],
                table: Identifier::Name("users".to_string()),
                timestamp: None,
                limit: None,
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
                limit: None,
            }]
        );
    }

    #[test]
    fn test_invalid_create_table_missing_paren() {
        let query = b"CREATE TABLE users id INT, name TEXT";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Expected '(' after table name.".to_string());
    }

    #[test]
    fn test_invalid_column_definition() {
        let query = b"CREATE TABLE users (id)";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition for 'id': Missing data type.".to_string());
    }

    #[test]
    fn test_missing_column_type() {
        let query = b"CREATE TABLE users (id, name)";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition for 'id': Missing data type.".to_string());
    }

    #[test]
    fn test_create_table_with_column_constraints() {
        let query = b"CREATE TABLE users (id INT, name TEXT UNIQUE)";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Create {
                table: Identifier::Name("users".to_string()),
                columns: vec![
                    (
                        Identifier::Name("id".to_string()),
                        Identifier::Name("INT".to_string()),
                        vec![]
                    ),
                    (
                        Identifier::Name("name".to_string()),
                        Identifier::Name("TEXT".to_string()),
                        vec!["UNIQUE".to_string()]
                    ),
                ],
            }]
        );
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
                    Identifier::Literal("1".to_string(), Some(Int)), // Numeric literal
                    Identifier::Literal("John Doe".to_string(), Some(String)), // String literal
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
                    Identifier::Literal("1".to_string(), Some(Int)),
                    Identifier::Literal("test".to_string(), Some(String)),
                ],
            },
        ]);
    }

    #[test]
    fn test_update_with_assignment() {
        let query = b"UPDATE users SET name = \"John Doe\" WHERE id = 1";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![
                ASTNode::Update {
                    table: Identifier::Name("users".to_string()),
                    values: vec![(
                        Identifier::Name("name".to_string()),
                        Identifier::Literal("John Doe".to_string(), Some(String)),
                    )],
                },
                ASTNode::Where {
                    condition: Expression::Comparison {
                        left: Identifier::Name("id".to_string()),
                        operator: "=".to_string(),
                        right: Identifier::Literal("1".to_string(), None),
                    },
                },
            ]
        );
    }

    #[test]
    fn test_delete_statement() {
        let query = b"DELETE FROM users WHERE id = 1";
        let ast = sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![
                ASTNode::Delete {
                    table: Identifier::Name("users".to_string()),
                },
                ASTNode::Where {
                    condition: Expression::Comparison {
                        left: Identifier::Name("id".to_string()),
                        operator: "=".to_string(),
                        right: Identifier::Literal("1".to_string(), None),
                    }
                }
            ]
        );
    }


    #[test]
    fn test_invalid_syntax_missing_select_keyword() {
        let query = b"* FROM users";
        let ast = sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Unexpected token: *".to_string());
    }

    #[test]
    fn test_nested_create_query() {
        let query = b"CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))";
        let ast = sql_parser(query);

        assert!(ast.is_err()); // For now, let's err on complex unsupported syntax
    }

    #[test]
    fn test_drop_table_statement() {
        let query = b"DROP TABLE users";
        let result = sql_parser(query);

        assert!(result.is_ok());
        let nodes = result.unwrap();

        assert_eq!(nodes.len(), 1);
        match &nodes[0] {
            ASTNode::Drop { table } => {
                assert_eq!(table, &Identifier::Name("users".to_string()));
            }
            _ => panic!("Expected Drop node"),
        }
    }
}