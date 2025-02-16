use std::collections::HashMap;
use std::path::Path;
use crate::query::parser::{sql_parser, ASTNode, Expression, Identifier};
use crate::records::table::{create_table, delete_row, get_table_at_timestamp, get_table_data, insert_row, load_table_data_from_file, recalculate_current, update_row};
use crate::schema::schema;
use crate::schema::schema::{get_column_names_from_schema};
use crate::{AppState};
use actix_web::{post, web, HttpRequest, HttpResponse};
use tracing::log::{debug, info};
use uuid::Uuid;
use regex::Regex;
use crate::query::parser;
use crate::records::table::extract_literal_value;

pub async fn execute_query(
    ast_nodes: Vec<ASTNode>,
    data: web::Data<AppState>,
    _req: HttpRequest,
) -> HttpResponse {
    info!("Executing query with {} AST nodes", ast_nodes.len());
    let mut where_clause: Option<Expression> = None;

    // Iterate through AST nodes to extract the WHERE clause (if any)
    for ast_node in ast_nodes.iter() {
        if let ASTNode::Where { condition } = ast_node {
            where_clause = Some(condition.clone());
            break; // Extract only the first WHERE clause
        }
    }
    debug!("WHERE clause: {:?}", where_clause);
    for (i, ast_node) in ast_nodes.iter().enumerate() {
        match ast_node {
            ASTNode::Select { columns, table, timestamp } => {
                return handle_select(columns, table, timestamp, &data, where_clause.clone()).await;
            }
            ASTNode::Create { table, columns } => {
                return handle_create_table(table, columns, &data);
            }
            ASTNode::Insert { table, values, columns } => {
                return handle_insert(table, values, columns, &data).await;
            }
            ASTNode::Update { table, values } => {
                return handle_update(table, values, &data, &where_clause).await;
            }
            ASTNode::Delete { table } => {
                return handle_delete(table, &data, &where_clause).await;
            }
            _ => {
                return HttpResponse::BadRequest().body("Unsupported AST Node type");
            }
        }
    }

    HttpResponse::BadRequest().body("No valid SQL query provided")
}

async fn handle_select(
    columns: &[Identifier],
    table: &Identifier,
    timestamp: &Option<String>,
    data: &web::Data<AppState>,
    where_clause: Option<Expression>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        // Retrieve table data
        let table_data_result = if let Some(timestamp) = timestamp {
            get_table_at_timestamp(data.clone(), table_name, timestamp.clone()).await
        } else {
            get_table_data(data.clone(), table_name).await
        };

        // Handle table data retrieval
        match table_data_result {
            Ok(mut table_data) => {
                // If a WHERE clause exists, filter the rows based on it
                if let Some(condition) = &where_clause {
                    let column_names = match get_column_names_from_schema(&data, table_name) {
                        Ok(names) => names,
                        Err(_) => return HttpResponse::InternalServerError().body("Error fetching column names"),
                    };

                    table_data = table_data
                        .into_iter()
                        .filter(|row| evaluate_where_clause(condition, row, &column_names))
                        .collect();
                }

                // Process the SELECT query
                let result = process_select(columns, &table_data, data.clone(), table_name, where_clause).await;

                if result.is_empty() {
                    HttpResponse::Ok().body("No rows found")
                } else {
                    match serde_json::to_string(&result) {
                        Ok(json) => HttpResponse::Ok().body(json),
                        Err(e) => HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    HttpResponse::NotFound().body("Table data not found")
                } else {
                    HttpResponse::InternalServerError().body(format!("Error retrieving table data: {}", e))
                }
            }
        }
    } else {
        HttpResponse::BadRequest().body("Invalid table name")
    }
}

fn handle_create_table(
    table: &Identifier,
    columns: &Vec<(Identifier, Identifier, Vec<String>)>,
    data: &web::Data<AppState>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        // Build schema::Table from AST
        let schema_table = schema::Table {
            name: table_name.to_string(),
            columns: columns
                .iter()
                .filter_map(|(name, col_type, raw_rules)| {
                    if let (Identifier::Name(col_name), Identifier::Name(type_name)) = (name, col_type) {
                        let data_type = schema::DataType::from(type_name.as_str());
                        // Map Vec<String> raw_rules into Vec<Rule>
                        let rules: Vec<schema::Rule> = raw_rules
                            .iter()
                            .filter_map(|rule| schema::map_string_to_rule(rule))
                            .collect();

                        Some(schema::Column {
                            name: col_name.clone(),
                            data_type,
                            rules, // Converted rules
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        };

        // Execute the table creation
        match create_table(&schema_table, data) {
            Ok(_) => HttpResponse::Ok().body("Table Created"),
            Err(err) => HttpResponse::InternalServerError().body(format!("Error creating table: {}", err)),
        }
    } else {
        HttpResponse::BadRequest().body("Invalid table name in CREATE TABLE")
    }
}

async fn handle_insert(
    table: &Identifier,
    values: &[Identifier],
    columns: &[Identifier],
    data: &web::Data<AppState>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        let mut row_data: Vec<String> = values
            .iter()
            .filter_map(|value| match value {
                Identifier::Literal(lit, _) => Some(lit.clone()),
                _ => None,
            })
            .collect();

        let uuid = Uuid::new_v4().to_string();
        row_data.insert(0, uuid); // Add UUID as the first column

        // Optionally handle column names
        let _column_names = if columns.is_empty() {
            None
        } else {
            Some(
                columns
                    .iter()
                    .filter_map(|col| match col {
                        Identifier::Name(col_name) => Some(col_name.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )
        };

        // Insert the row into the table
        match insert_row(table_name, row_data, data).await {
            Ok(_) => HttpResponse::Ok().body("Row inserted"),
            Err(err) => HttpResponse::InternalServerError().body(format!("Error inserting row: {}", err)),
        }
    } else {
        HttpResponse::BadRequest().body("Invalid table name in INSERT statement")
    }
}

async fn handle_delete(
    table: &Identifier,
    data: &web::Data<AppState>,
    where_clause: &Option<Expression>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        // Ensure a WHERE clause is provided
        if where_clause.is_none() {
            return HttpResponse::BadRequest().body("DELETE must include a WHERE clause with a valid filter");
        }

        let condition = where_clause.as_ref().unwrap();

        // Load table data to identify rows to delete
        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&data.config.db_dir)
            .join(&data.config.table_dir)
            .join(&initial_table_name);

        match load_table_data_from_file(&initial_table_path) {
            Ok(initial_data) => {
                let column_names = get_column_names_from_schema(data, table_name).unwrap_or_default();

                // Filter rows based on the WHERE clause
                let rows_to_delete: Vec<_> = initial_data
                    .iter()
                    .filter(|row| evaluate_where_clause(condition, row, &column_names))
                    .collect();

                if rows_to_delete.is_empty() {
                    return HttpResponse::NotFound().body("No rows matched the specified condition");
                }

                // Call `delete_row` for each filtered row
                for row in &rows_to_delete {
                    if let Some(row_id) = row.get(0) {
                        if let Err(e) = delete_row(table_name, row_id, data).await {
                            return HttpResponse::InternalServerError().body(format!("Error deleting row: {}", e));
                        }
                    }
                }

                HttpResponse::Ok().body(format!("Deleted {} rows", rows_to_delete.len()))
            }
            Err(e) => HttpResponse::InternalServerError().body(format!("Error loading initial data: {}", e)),
        }
    } else {
        HttpResponse::BadRequest().body("Invalid table name in DELETE statement")
    }
}



async fn handle_update(
    table: &Identifier,
    values: &[(Identifier, Identifier)],
    data: &web::Data<AppState>,
    where_clause: &Option<Expression>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        let mut updated_values = HashMap::new();

        // Map values to a HashMap of column names and their updated values
        for (col_identifier, val_identifier) in values {
            let col_name = extract_column_name(col_identifier);
            let value = extract_literal_value(val_identifier);
            updated_values.insert(col_name, value);
        }

        // Ensure a WHERE clause is provided
        if where_clause.is_none() {
            return HttpResponse::BadRequest().body("UPDATE must include a WHERE clause with a valid UUID");
        }

        let condition = where_clause.as_ref().unwrap();
        if !is_uuid_where_clause(condition) {
            return HttpResponse::BadRequest().body(format!(
                "WHERE clause must contain a valid UUID condition for table '{}'",
                table_name
            ));
        }

        // Load data to process the update
        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&data.config.db_dir)
            .join(&data.config.table_dir)
            .join(&initial_table_name);

        match load_table_data_from_file(&initial_table_path) {
            Ok(initial_data) => {
                let column_names = get_column_names_from_schema(data, table_name).unwrap_or_default();

                // Filter rows based on the WHERE clause
                let filtered_rows: Vec<_> = initial_data
                    .iter()
                    .filter(|row| evaluate_where_clause(condition, row, &column_names))
                    .collect();

                if filtered_rows.is_empty() {
                    return HttpResponse::NotFound().body("No rows matched the specified condition");
                }

                // Apply updates to the filtered rows
                for row in &filtered_rows {
                    if let Some(row_id) = row.get(0) {
                        if let Err(e) = update_row(table_name, row_id, updated_values.clone(), data).await {
                            return HttpResponse::InternalServerError().body(format!("Error updating row: {}", e));
                        }
                    }
                }

                if let Err(e) = recalculate_current(data, table_name, initial_data).await {
                    return HttpResponse::InternalServerError()
                        .body(format!("Error recalculating data: {}", e));
                }

                HttpResponse::Ok().body("Rows updated")
            }
            Err(e) => HttpResponse::InternalServerError().body(format!("Error loading initial data: {}", e)),
        }
    } else {
        HttpResponse::BadRequest().body("Invalid table name in UPDATE statement")
    }
}



fn extract_column_name(identifier: &Identifier) -> String {
    match identifier {
        Identifier::Name(name) => name.to_string(),
        Identifier::Literal(value, ..) => value.to_string(),
        Identifier::Star => "*".parse().unwrap(),
    }
}
fn is_uuid_where_clause(condition: &Expression) -> bool {
    match condition {
        Expression::Comparison { left, operator: _, right } => {
            // Validate that the left side is the "uuid" column
            let is_left_uuid = matches!(left, Identifier::Name(name) if name == "uuid" || name == "UUID");

            // Validate that the right side is a UUID literal (basic format check)
            let is_right_a_uuid_literal = matches!(right, Identifier::Literal(lit, _) if {
                // Attempt to clean up surrounding quotes
                let cleaned_lit = lit.trim_matches('"');
                Uuid::parse_str(cleaned_lit).is_ok()
            });
            is_left_uuid && is_right_a_uuid_literal
        }
    }
}




async fn process_select(
    columns: &[Identifier],
    table_data: &[Vec<String>],
    data: web::Data<AppState>,
    table_name: &String,
    _where_clause: Option<Expression>,
) -> Vec<Vec<serde_json::Value>> {
    let mut result = Vec::new();

    if table_data.is_empty() {
        return result; // No data to process
    }

    // Get column names for the table
    let column_names = match get_column_names_from_schema(&data, table_name) {
        Ok(names) => names,
        Err(_) => return result, // Return empty if column names can't be found
    };

    for row in table_data.iter() {
        let mut selected_row = Vec::new();

        if columns.len() == 1 && matches!(columns[0], Identifier::Star) {
            // Select all columns
            for value in row.iter() {
                if let Ok(int_val) = value.parse::<i64>() {
                    selected_row.push(serde_json::Value::Number(int_val.into()));
                } else if let Ok(float_val) = value.parse::<f64>() {
                    selected_row.push(serde_json::Value::Number(
                        serde_json::Number::from_f64(float_val).unwrap(),
                    ));
                } else {
                    selected_row.push(serde_json::Value::String(value.trim_matches('"').to_string()));
                }
            }
        } else {
            for col in columns {
                if let Identifier::Name(col_name) = col {
                    if let Some(index) = find_column_index(&column_names, col_name) {
                        if let Some(value) = row.get(index) {
                            if let Ok(int_val) = value.parse::<i64>() {
                                selected_row.push(serde_json::Value::Number(int_val.into()));
                            } else if let Ok(float_val) = value.parse::<f64>() {
                                selected_row.push(serde_json::Value::Number(
                                    serde_json::Number::from_f64(float_val).unwrap(),
                                ));
                            } else {
                                selected_row.push(serde_json::Value::String(
                                    value.trim_matches('"').to_string(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        result.push(selected_row);
    }

    result
}

pub fn evaluate_where_clause(
    condition: &Expression,
    row: &[String],
    column_names: &[String],
) -> bool {
    let column_names_upper: Vec<String> = column_names.iter().map(|col| col.to_uppercase()).collect();

    match condition {
        Expression::Comparison { left, operator, right } => {
            // Resolve left value
            let left_value = match left {
                Identifier::Name(name) => {
                    let index_result = find_column_index(&column_names_upper, &name.to_uppercase());
                    index_result.and_then(|index| row.get(index).map(String::from))
                }
                Identifier::Literal(lit, _) => Some(lit.clone()),
                _ => None, // Unsupported
            };

            // Resolve right value
            let right_value = match right {
                Identifier::Name(name) => {
                    let index_result = find_column_index(&column_names_upper, &name.to_uppercase());
                    index_result.and_then(|index| row.get(index).map(String::from))
                }
                Identifier::Literal(lit, _) => Some(lit.clone()),
                _ => None, // Unsupported
            };

            if left_value.is_none() || right_value.is_none() {
                return false; // No value to compare
            }

            let mut left_val = left_value.unwrap();
            let mut right_val = right_value.unwrap();

            // Trim quotes to normalize comparison
            left_val = left_val.trim_matches('"').to_string();
            right_val = right_val.trim_matches('"').to_string();

            // Perform evaluation based on the operator
            match operator.as_str() {
                "=" => left_val == right_val,
                "!=" => left_val != right_val,
                ">" => left_val > right_val,
                "<" => left_val < right_val,
                ">=" => left_val >= right_val,
                "<=" => left_val <= right_val,
                "LIKE" => {
                    return evaluate_like_condition(&left_val, &right_val);
                }
                _ => false, // Unsupported
            }
        }
    }
}

/// Helper function to evaluate `LIKE` conditions
fn evaluate_like_condition(left: &str, pattern: &str) -> bool {
    // Normalize inputs by trimming extra quotes and converting to lowercase
    let normalized_left = left.trim_matches('"').to_lowercase();
    let normalized_pattern = pattern
        .trim_matches('"') // Removes double quotes if present
        .trim_matches('\'') // Removes single quotes if present
        .to_lowercase();

    // Convert SQL LIKE pattern to regex
    let regex_pattern = normalized_pattern
        .replace('%', ".*") // % matches any sequence of characters
        .replace('_', "."); // _ matches any single character

    // Create the regex and match
    match Regex::new(&format!("^{}$", regex_pattern)) {
        Ok(regex) => regex.is_match(&normalized_left),
        Err(e) => {
            debug!("Regex error: {}", e);
            false
        }
    }
}



fn find_column_index(column_names: &[String], col_name: &str) -> Option<usize> {
    let position = column_names
        .iter()
        .position(|col| col.eq_ignore_ascii_case(col_name)); // Use case-insensitive comparison

    if position.is_none() {
        debug!("Column {} not found in {:?}", col_name, column_names);
    }
    position
}


#[post("/query")]
async fn execute_query_endpoint(
    query: web::Json<String>,
    data: web::Data<AppState>,
) -> HttpResponse { // Return plain HttpResponse
    let sql_query = query.into_inner();
    let query_bytes = sql_query.as_bytes();
    let http_request = actix_web::test::TestRequest::default().to_http_request();

    match sql_parser(query_bytes) {
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


