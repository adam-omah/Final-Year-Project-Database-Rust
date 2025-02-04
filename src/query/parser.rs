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
// If Queries are a valid AST action it
#[derive(Debug, Serialize, Deserialize,PartialEq, Eq,Clone)]
pub enum ASTNode {
    Select { columns: Vec<Identifier> },
    From { table: Identifier },
    Where { condition: Expression },
    Insert { table: Identifier, values: Vec<Identifier>, columns: Vec<Identifier> },
    Update { table: Identifier, values: Vec<(Identifier, Identifier)> },
    Delete { table: Identifier },
    CreateTable { table: Identifier, columns: Vec<(Identifier, Identifier)> },
}

#[derive(Deserialize, Serialize,Debug, Clone)]
pub struct ASTNodes(pub Vec<ASTNode>);



pub fn basic_sql_parser(query_bytes: &[u8]) -> Result<Vec<ASTNode>, String> {
    let query_str = str::from_utf8(query_bytes).map_err(|e| e.to_string())?;
    let tokens: Vec<&str> = query_str
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .collect();

    if tokens.is_empty() {
        return Err("Empty query".to_string());
    }

    let mut ast_nodes = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        match tokens[index] {
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
}
