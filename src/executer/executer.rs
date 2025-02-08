use std::collections::HashMap;
use std::path::Path;
use crate::query::parser::{basic_sql_parser, ASTNode, Expression, Identifier};
use crate::records::table::{create_table, get_table_data, insert_row, load_table_data_from_file, recalculate_current, update_row};
use crate::schema::schema;
use crate::schema::schema::{get_column_names_from_schema};
use crate::{AppState};
use actix_web::{post, web, HttpRequest, HttpResponse};
use tracing::log::{debug, info};
use uuid::Uuid;
use crate::query::parser;
use crate::records::table::extract_literal_value;

// Import your table functions



pub async fn execute_query(
    ast_nodes: Vec<ASTNode>,
    data: web::Data<AppState>,
    _req: HttpRequest,
) -> HttpResponse {
    info!("Executing query with {} AST nodes", ast_nodes.len());

    let mut where_clause: Option<Expression> = None; // Store the WHERE clause (if any)

    for i in 0..ast_nodes.len() {
        match &ast_nodes[i] {
            ASTNode::Select { columns } => {
                if i + 1 < ast_nodes.len() {
                    if let ASTNode::From { table } = &ast_nodes[i + 1] {
                        if let Identifier::Name(table_name) = table {
                            // Load initial data for recalculate_current
                            let initial_table_name = format!("{}_initial", table_name);
                            let initial_table_path = Path::new(&data.config.db_dir)
                                .join(&data.config.table_dir)
                                .join(&initial_table_name);
                            let initial_data_result = load_table_data_from_file(&initial_table_path);

                            match initial_data_result {
                                Ok(initial_data) => {
                                    // Recalculate current data (if necessary)
                                    if let Err(e) =
                                        recalculate_current(&data, table_name, initial_data.clone()).await
                                    {
                                        eprintln!("Error in recalculate_current: {}", e);
                                        // Consider returning an error response here if recalculation is critical
                                    }

                                    let table_data_result = get_table_data(data.clone(), table_name).await;

                                    match table_data_result {
                                        Ok(table_data) => {
                                            // Check for a WHERE clause
                                            if i + 2 < ast_nodes.len() {
                                                if let ASTNode::Where { condition } = &ast_nodes[i + 2] {
                                                    where_clause = Some(condition.clone());
                                                }
                                            }

                                            let result = process_select(
                                                columns,
                                                &table_data,
                                                data.clone(),
                                                table_name,
                                                where_clause.clone(),
                                            )
                                                .await;

                                            if result.is_empty() {
                                                return HttpResponse::Ok().body("No rows found");
                                            }

                                            match serde_json::to_string(&result) {
                                                Ok(json) => return HttpResponse::Ok().body(json),
                                                Err(e) => {
                                                    return HttpResponse::InternalServerError()
                                                        .body(format!("Serialization error: {}", e))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            if e.kind() == std::io::ErrorKind::NotFound {
                                                return HttpResponse::NotFound().body(e.to_string());
                                            } else {
                                                return HttpResponse::InternalServerError().body(e.to_string());
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to load initial data: {}", e);
                                    return HttpResponse::InternalServerError()
                                        .body(format!("Failed to load initial data: {}", e));
                                }
                            }
                        } else {
                            return HttpResponse::BadRequest()
                                .body("Invalid table name in FROM clause");
                        }
                    } else {
                        return HttpResponse::BadRequest().body("FROM clause missing after SELECT");
                    }
                } else {
                    return HttpResponse::BadRequest().body("FROM clause missing after SELECT");
                }
            }


            // Handle CREATE TABLE statement
            ASTNode::Create { table, columns } => {
                if let Identifier::Name(table_name) = table {
                    // Map columns to schema::Column, extracting name and data type
                    let table = schema::Table {
                        name: table_name.to_string(),
                        columns: columns
                            .iter()
                            .filter_map(|(name, col_type)| {
                                if let (Identifier::Name(col_name), Identifier::Name(type_name)) = (name, col_type)
                                {
                                    let data_type = schema::DataType::from(type_name.as_str()); // Convert type name to DataType
                                    Some(schema::Column {
                                        name: col_name.clone(), // Extract column name
                                        data_type,
                                        rules: vec![],
                                    })
                                } else {
                                    None // Skip invalid/unsupported columns
                                }
                            })
                            .collect(),
                    };

                    // Try to create the table
                    match create_table(&table, &data) {
                        Ok(_) => return HttpResponse::Ok().body("Table Created"),
                        Err(err) => {
                            return HttpResponse::InternalServerError()
                                .body(format!("Error creating table: {}", err))
                        }
                    }
                } else {
                    return HttpResponse::BadRequest().body("Invalid table name in CREATE TABLE");
                }
            }

            // Handle INSERT statement
            ASTNode::Insert { table, values, columns } => {
                if let Identifier::Name(table_name) = table {
                    // Convert values: Vec<Identifier> to Vec<String>
                    let mut row_data: Vec<String> = values
                        .iter()
                        .filter_map(|value| match value {
                            Identifier::Literal(lit, _) => Some(lit.clone()), // Grab the value for literals
                            _ => None, // Ignore invalid types (e.g., non-literal identifiers)
                        })
                        .collect();

                    // Generate a UUID for rows if needed and prepend to row_data
                    let uuid = Uuid::new_v4().to_string();
                    row_data.insert(0, uuid); // Insert UUID as the first column (if schema requires it)

                    // Validate and handle column names (if provided)
                    let column_names: Option<Vec<String>> = if columns.is_empty() {
                        None // No columns provided
                    } else {
                        // Convert columns: Vec<Identifier> to Option<Vec<String>>
                        Some(
                            columns
                                .iter()
                                .filter_map(|col| match col {
                                    Identifier::Name(col_name) => Some(col_name.clone()), // Grab column names
                                    _ => None, // Ignore invalid types
                                })
                                .collect(),
                        )
                    };

                    // Call insert_row with updated row_data (including UUID)
                    match insert_row(table_name.as_str(), row_data, &data) {
                        Ok(_) => return HttpResponse::Ok().body("Row inserted"),
                        Err(err) => {
                            return HttpResponse::InternalServerError()
                                .body(format!("Error inserting row: {}", err))
                        }
                    }
                } else {
                    return HttpResponse::BadRequest().body("Invalid table name in INSERT statement");
                }
            }
            ASTNode::Update { table, values } => { // No 'condition' here
                if let Identifier::Name(table_name) = table {
                    let column_names = get_column_names_from_schema(&data, &table_name.to_string());

                    let mut updated_values = HashMap::new();
                    let mut row_id_to_update: Option<String> = None;

                    for (col_identifier, val_identifier) in values {
                        let col_name = extract_column_name(&col_identifier);
                        let value = extract_literal_value(&val_identifier);

                        if col_name == "id" { // Assumes "id" is the UUID column name.  Adjust if different.
                            row_id_to_update = Some(value);
                        } else {
                            updated_values.insert(col_name, value);
                        }
                    }

                    if let Some(uuid) = row_id_to_update {
                        if let Err(e) = update_row(table_name, &uuid, updated_values, &data).await {
                            return HttpResponse::InternalServerError().body(format!("Error updating row: {}", e));
                        }

                        // Load initial data for recalculate_current
                        let initial_table_name = format!("{}_initial", table_name);
                        let initial_table_path = Path::new(&data.config.db_dir)
                            .join(&data.config.table_dir)
                            .join(&initial_table_name);

                        match load_table_data_from_file(&initial_table_path) {
                            Ok(initial_data) => {
                                // Call recalculate_current with initial_data
                                if let Err(e) = recalculate_current(&data, table_name, initial_data).await {
                                    return HttpResponse::InternalServerError().body(format!("Error recalculating current data: {}", e));
                                }

                                return HttpResponse::Ok().body("Row updated"); // Return Ok response here
                            }
                            Err(e) => {
                                return HttpResponse::InternalServerError()
                                    .body(format!("Error loading initial data for recalculation: {}", e));
                            }
                        }
                    } else {
                        return HttpResponse::BadRequest().body("Row ID (UUID) not provided in update statement");
                    }
                } else {
                    HttpResponse::BadRequest().body("Invalid table name in UPDATE statement");
                }
            }

            // If there's a WHERE clause without a SELECT or FROM
            ASTNode::Where { .. } => {
                return HttpResponse::BadRequest().body("WHERE clause without SELECT/FROM");
            }

            // Handle other AST nodes as needed
            _ => return HttpResponse::BadRequest().body("Unsupported AST Node type"),
        }
    }

    HttpResponse::BadRequest().body("No valid SQL query provided")
}

fn extract_column_name(identifier: &Identifier) -> String {
    match identifier {
        Identifier::Name(name) => name.to_string(),
        Identifier::Literal(value, ..) => value.to_string(),
        &parser::Identifier::Star => todo!(), // Or handle literal column names if needed
    }
}



async fn process_select(
    columns: &[Identifier],
    table_data: &[Vec<String>],
    data: web::Data<AppState>,
    table_name: &String,
    where_clause: Option<Expression>,
) -> Vec<Vec<String>> {
    let mut result = Vec::new();
    // Check if table_data is empty
    if table_data.is_empty() {
        return result; // Return an empty result
    }
    // Get column names for the table
    let column_names = match get_column_names_from_schema(&data, table_name) {
        Ok(names) => names,
        Err(_) => return result, // Return empty if column names can't be found
    };
    for row in table_data.iter() {
        // Filter rows based on WHERE clause (if present)
        if where_clause.is_none() || evaluate_where_clause(&where_clause.clone().unwrap(), row, &column_names) {
            let mut selected_row = Vec::new();
            if columns.len() == 1 && matches!(columns[0], Identifier::Star) {
                // SELECT * --> add all columns from the row
                selected_row.extend_from_slice(row);
            } else {
                // SELECT specific columns
                for col in columns {
                    match col {
                        Identifier::Name(col_name) => {
                            if let Some(index) = find_column_index(&column_names, col_name) {
                                if let Some(value) = row.get(index) {
                                    selected_row.push(value.to_string());
                                }
                            }
                        }
                        _ => (), // Ignore unsupported identifiers
                    }
                }
            }
            result.push(selected_row);
        }
    }
    result
}

pub fn evaluate_where_clause(
    condition: &Expression,
    row: &[String],
    column_names: &[String],
) -> bool {
    match condition {
        Expression::Comparison { left, operator, right } => {
            let left_value = match left {
                Identifier::Name(name) => {
                    let index_result = find_column_index(column_names, name);
                    index_result.and_then(|index| row.get(index).map(|s| s.to_string()))
                }
                Identifier::Literal(lit, _) => {
                    Some(lit.to_string())
                }
                _ => {
                    None

                }, // Unsupported identifier type
            };
            let right_value = match right {
                Identifier::Name(name) => {
                    let index_result = find_column_index(column_names, name);
                    index_result.and_then(|index| row.get(index).map(|s| s.to_string()))
                }
                Identifier::Literal(lit, _) => {
                    Some(lit.to_string())
                }
                _ => {
                    None
                }, // Unsupported identifier type
            };
            if left_value.is_none() || right_value.is_none() {
                return false; // Unable to evaluate, treat as false
            }
            let left_val = left_value.unwrap();
            let right_val = right_value.unwrap();
            let comparison_result = match operator.as_str() {
                "=" => left_val == right_val,
                "!=" => left_val != right_val,
                ">" => left_val > right_val,
                "<" => left_val < right_val,
                ">=" => left_val >= right_val,
                "<=" => left_val <= right_val,
                _ => {
                    false
                },  // Unsupported operator
            };
            comparison_result
        }
    }
}



fn find_column_index(column_names: &[String], col_name: &str) -> Option<usize> {
    column_names.iter().position(|col| col == col_name)
}

#[post("/query")]
async fn execute_query_endpoint(
    query: web::Json<String>,
    data: web::Data<AppState>,
) -> HttpResponse { // Return plain HttpResponse
    let sql_query = query.into_inner();
    debug!("Received query: {}", sql_query);
    let query_bytes = sql_query.as_bytes();
    let http_request = actix_web::test::TestRequest::default().to_http_request();

    match basic_sql_parser(query_bytes) {
        Ok(ast_nodes) => {
            // Successfully parsed query
            execute_query(ast_nodes, data, http_request).await
        }
        Err(err) => {
            // Handle parse failure with a unified HttpResponse
            HttpResponse::BadRequest()
                .body(format!("Failed to parse query: {}", err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::database_config::DatabaseConfig;
    use crate::records::table::get_table_data;
    use crate::schema::schema::load_schema;
    use crate::{init_database, AppState};
    use actix_web::{http::StatusCode, test, web, App};
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Result;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    #[actix_web::test]
    async fn test_create_table_success() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test";
        let test_db_dir = PathBuf::from(TEST_DB_DIR);
        if test_db_dir.exists() {
            fs::remove_dir_all(&test_db_dir).expect("Failed to remove test database directory");
        }

        let test_config = DatabaseConfig {
            db_dir: PathBuf::from(TEST_DB_DIR),
            ..Default::default()
        };
        init_database(&test_config)?;

        let schema = Arc::new(Mutex::new(load_schema(&test_config)?));
        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        });

        // Initialize Actix Web app
        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(crate::executer::executer::execute_query_endpoint),
        )
            .await;

        // Step 2: Send `CREATE TABLE` request
        let raw_query = r#"CREATE TABLE test_table (col1 Int, col2 String)"#;
        let req = test::TestRequest::post()
            .uri("/query")
            .set_json(&raw_query)
            .to_request();

        // Execute the request and evaluate the response
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK); // Expect 200 OK
        let body = test::read_body(resp).await;
        assert_eq!(body, "Table Created");

        // Step 3: Check schema integrity
        let schema = app_state.schema.lock().unwrap();
        assert!(schema.tables.contains_key("test_table")); // Table should exist
        let table = schema.tables.get("test_table").unwrap();
        assert_eq!(table.name, "test_table");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "col1");
        assert_eq!(table.columns[1].name, "col2");

        Ok(())
    }

    #[actix_web::test]
    async fn test_insert_row_success() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test";
        let test_db_dir = PathBuf::from(TEST_DB_DIR);
        if test_db_dir.exists() {
            fs::remove_dir_all(&test_db_dir).expect("Failed to remove test database directory");
        }

        let test_config = DatabaseConfig {
            db_dir: PathBuf::from(TEST_DB_DIR),
            ..Default::default()
        };
        init_database(&test_config)?;

        let schema = Arc::new(Mutex::new(load_schema(&test_config)?));
        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        });

        // Initialize Actix Web app
        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(crate::executer::executer::execute_query_endpoint),
        )
            .await;

        // Step 1: Create a table first
        let create_table_query = r#"CREATE TABLE users (id Int, name String)"#;
        let create_req = test::TestRequest::post()
            .uri("/query")
            .set_json(&create_table_query)
            .to_request();
        let create_resp = test::call_service(&app, create_req).await;
        assert_eq!(create_resp.status(), StatusCode::OK); // Expect 200 OK

        // Step 2: Send `INSERT` request
        let raw_query = r#"INSERT INTO users (id, name) VALUES (1, 'Alice')"#;
        let req = test::TestRequest::post()
            .uri("/query")
            .set_json(&raw_query)
            .to_request();

        // Execute the request and evaluate the response
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK); // Expect 200 OK
        let body = test::read_body(resp).await;
        assert_eq!(body, "Row inserted");

        // Step 3: Verify data
        let table_data = get_table_data(app_state.clone(), "users").await.unwrap();
        assert_eq!(table_data, vec![vec!["1".to_string(), "Alice".to_string()]]);

        Ok(())
    }

}


