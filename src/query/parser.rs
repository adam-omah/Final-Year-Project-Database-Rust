use serde::{Deserialize, Serialize};
use std::str;

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Identifier {
    // Represents identifiers like column names, table names, etc.
    Name(String),
    // Represents literals like numbers or strings.
    Literal(String),
    // Represents * in SELECT statements.
    Star,
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
    Select { columns: Vec<Identifier> },
    From { table: Identifier },
    Where { condition: Expression },
    Insert { table: Identifier, values: Vec<Identifier>, columns: Vec<Identifier> },
    Update { table: Identifier, values: Vec<(Identifier, Identifier)> },
    Delete { table: Identifier },
    Create { table: Identifier, columns: Vec<(Identifier, Identifier)> },
}

#[derive(Deserialize, Serialize,Debug, Clone)]
pub struct ASTNodes(pub Vec<ASTNode>);



pub fn basic_sql_parser(query_bytes: &[u8]) -> Result<Vec<ASTNode>, String> {
    let query_str = str::from_utf8(query_bytes).map_err(|e| e.to_string())?;
    // Improved and FINAL Tokenization (No re-tokenizing!)
    let mut tokens = Vec::new();
    let mut in_string = false;
    let mut current_token = String::new();

    for char in query_str.chars() {
        if char == '"' {
            in_string = !in_string;
            current_token.push(char);
            if !in_string {
                tokens.push(current_token.clone());
                current_token.clear();
            }
        } else if in_string {
            current_token.push(char);
        } else if char == '(' || char == ')' || char == ',' {
            if !current_token.is_empty() {
                tokens.push(current_token.clone());
                current_token.clear();
            }
            tokens.push(char.to_string());
        } else if char.is_whitespace() {
            if !current_token.is_empty() {
                tokens.push(current_token.clone());
                current_token.clear();
            }
        } else {
            current_token.push(char);
        }
    }
    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    if tokens.is_empty() {
        return Err("Query is empty".to_string());
    }

    let mut ast_nodes = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        match tokens[index].as_str() {
            "SELECT" => {
                let mut columns = Vec::new();
                index += 1;
                while index < tokens.len() && tokens[index] != "FROM" {
                    let identifier = if tokens[index] == "*" {
                        Identifier::Star
                    } else {
                        Identifier::Name(tokens[index].to_string())
                    };
                    columns.push(identifier);
                    index += 1;
                }
                ast_nodes.push(ASTNode::Select { columns });
            }
            "FROM" => {
                index += 1;
                if index < tokens.len() {
                    let table = Identifier::Name(tokens[index].to_string());

                    ast_nodes.push(ASTNode::From { table });
                    index += 1;
                } else {
                    return Err("Expected table name after FROM".to_string());
                }
            }
            "WHERE" => {
                index += 1;
                if index < tokens.len() {
                    // Basic comparison expression parsing
                    let left = Identifier::Name(tokens[index].to_string());
                    index += 1; // Skip left operand
                    let operator = tokens[index].to_string();
                    index += 1;  // Skip operator
                    let right = Identifier::Literal(tokens[index].to_string());
                    index += 1;

                    let condition = Expression::Comparison {
                        left,
                        operator,
                        right,
                    };
                    ast_nodes.push(ASTNode::Where { condition });
                } else {
                    return Err("Expected condition after WHERE".to_string());
                }
            }
            "CREATE" => {
                index += 1;
                if index < tokens.len() && tokens[index] == "TABLE" {
                    index += 1;
                    if index < tokens.len() {
                        let table = Identifier::Name(tokens[index].clone());
                        index += 1;

                        if index < tokens.len() && tokens[index] == "(" {
                            index += 1;
                            let mut columns = Vec::new();

                            while index < tokens.len() && tokens[index] != ")" {
                                let column_name = Identifier::Name(tokens[index].clone());
                                index += 1;

                                // Check for column type or end of columns
                                if index < tokens.len() && tokens[index] != "," && tokens[index] != ")" {
                                    let column_type = Identifier::Name(tokens[index].clone());
                                    columns.push((column_name, column_type));
                                    index += 1;
                                } else {
                                    return Err("Invalid column definition".to_string()); // Missing type
                                }

                                if index < tokens.len() && tokens[index] == "," {
                                    index += 1;
                                }
                            }


                            if index < tokens.len() {
                                if tokens[index] == ")" {
                                    index += 1;
                                    ast_nodes.push(ASTNode::Create { table, columns });
                                } else {
                                    return Err("Expected ')' or ',' after column definition".to_string());
                                }
                            } else {
                                return Err("Expected ')' after column definitions".to_string());
                            }

                        } else {
                            return Err("Expected '(' after table name".to_string());
                        }
                    } else {
                        return Err("Expected table name after CREATE TABLE".to_string());
                    }
                } else {
                    return Err("Expected TABLE after CREATE".to_string());
                }
            }
            "INSERT" => {
                index += 1;
                if index < tokens.len() && tokens[index] == "INTO" {
                    index += 1;
                    if index < tokens.len() {
                        let table = Identifier::Name(tokens[index].clone());
                        index += 1;

                        let mut columns = Vec::new();
                        if index < tokens.len() && tokens[index] == "(" {
                            index += 1;
                            while index < tokens.len() && tokens[index] != ")" {
                                columns.push(Identifier::Name(tokens[index].clone()));
                                index += 1;
                                if index < tokens.len() && tokens[index] == "," {
                                    index += 1;
                                }
                            }
                            if index < tokens.len() && tokens[index] == ")" {
                                index += 1;
                            } else {
                                return Err("Expected ')' after column list".to_string());
                            }
                        }

                        if index < tokens.len() && tokens[index] == "VALUES" {
                            index += 1;
                            if index < tokens.len() && tokens[index] == "(" {
                                index += 1;
                                let mut values = Vec::new();
                                while index < tokens.len() && tokens[index] != ")" {
                                    values.push(Identifier::Literal(tokens[index].clone())); // Treat values as literals
                                    index += 1;
                                    if index < tokens.len() && tokens[index] == "," {
                                        index += 1;
                                    }
                                }
                                if index < tokens.len() && tokens[index] == ")" {
                                    index += 1;
                                    ast_nodes.push(ASTNode::Insert { table, columns, values });
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
            _ => return Err(format!("Unexpected token: {}", tokens[index])),
        }
    }
    Ok(ast_nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_comparison() {
        let query = b"SELECT id FROM my_table WHERE id = 1";
        let ast = basic_sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![
                ASTNode::Select { columns: vec![Identifier::Name("id".to_string())] },
                ASTNode::From { table: Identifier::Name("my_table".to_string()) },
                ASTNode::Where {
                    condition: Expression::Comparison {
                        left: Identifier::Name("id".to_string()),
                        operator: "=".to_string(),
                        right: Identifier::Literal("1".to_string())
                    }
                },
            ]
        );
    }

    #[test]
    fn test_select_star() {
        let query = b"SELECT * FROM users";
        let ast = basic_sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![
                ASTNode::Select { columns: vec![Identifier::Star] },
                ASTNode::From { table: Identifier::Name("users".to_string()) },
            ]
        );
    }

    #[test]
    fn test_create_table_custom() {
        let query = b"CREATE TABLE test_table (col1 Int, col2 String)";
        let ast = basic_sql_parser(query).unwrap();

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
        let ast = basic_sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Expected '(' after table name".to_string());
    }

    #[test]
    fn test_invalid_column_definition() {
        let query = b"CREATE TABLE users (id)";
        let ast = basic_sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition".to_string());
    }

    #[test]
    fn test_missing_column_type() {
        let query = b"CREATE TABLE users (id, name)";
        let ast = basic_sql_parser(query);

        assert!(ast.is_err());
        assert_eq!(ast.err().unwrap(), "Invalid column definition".to_string());
    }

    #[test]
    fn test_insert_statement() {
        let query = b"INSERT INTO users (id, name) VALUES (1, \"John Doe\")";
        let ast = basic_sql_parser(query).unwrap();

        assert_eq!(
            ast,
            vec![ASTNode::Insert {
                table: Identifier::Name("users".to_string()),
                columns: vec![
                    Identifier::Name("id".to_string()),
                    Identifier::Name("name".to_string())
                ],
                values: vec![
                    Identifier::Literal("1".to_string()),
                    Identifier::Literal("\"John Doe\"".to_string()) // Now parsed correctly
                ]
            }]
        );
    }


    #[test]
    fn test_insert_without_columns() {
        let query = b"INSERT INTO users VALUES (1, \"test\")";
        let ast = basic_sql_parser(query).unwrap();
        assert_eq!(ast, vec![
            ASTNode::Insert {
                table: Identifier::Name("users".to_string()),
                columns: vec![], // No columns specified
                values: vec![
                    Identifier::Literal("1".to_string()),
                    Identifier::Literal("\"test\"".to_string()),
                ],
            },
        ]);
    }
}
