use std::collections::HashMap;
use std::path::Path;
use crate::query::parser::{sql_parser, ASTNode, Expression, Identifier};
use crate::tables::table::{create_table, delete_row, get_table_at_timestamp, get_table_data, insert_row, load_table_data_from_file, recalculate_table, update_row};
use crate::schema::schema;
use crate::schema::schema::{drop_table, get_column_names_from_schema};
use crate::{AppState, USERS_TABLE};
use actix_web::{post, web, HttpRequest, HttpResponse};
use tracing::log::{debug, error, info};
use uuid::Uuid;
use regex::Regex;
use serde_json::json;
use crate::auth::auth::authenticate_request;
use crate::tables::table::extract_literal_value;

pub async fn execute_query(
    ast_nodes: Vec<ASTNode>,
    data: web::Data<AppState>,
) -> HttpResponse {
    info!("Executing query with {} AST nodes", ast_nodes.len());
    let mut where_clause: Option<Expression> = None;

    // Iterate through AST nodes to extract the WHERE clause (if any)
    for ast_node in ast_nodes.iter() {
        if let ASTNode::Where { condition } = ast_node {
            info!("Found WHERE clause: {:?}", condition);
            where_clause = Some(condition.clone());
            break; // Extract only the first WHERE clause
        }
    }
    #[allow(clippy::never_loop)]
    for (.., ast_node) in ast_nodes.iter().enumerate() {
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
            ASTNode::Drop { table } => {
                return handle_drop(table, &data).await;
            }
            _ => {
                return HttpResponse::BadRequest().json(serde_json::json!({"error": "Unsupported AST Node type"}));
            }
        }
    }
    HttpResponse::BadRequest().json(serde_json::json!({"error": "No valid SQL query provided"}))
}

async fn handle_select(
    columns: &[Identifier],
    table: &Identifier,
    timestamp: &Option<String>,
    data: &web::Data<AppState>,
    where_clause: Option<Expression>,
) -> HttpResponse {
    info!("Handling SELECT query");
    debug!("SELECT details: columns={:?}, table={:?}, timestamp={:?}", columns, table, timestamp);

    if let Identifier::Name(table_name) = table {
        debug!("Fetching data for table: {}", table_name);

        // Retrieve table data
        let table_data_result = if let Some(timestamp) = timestamp {
            debug!("Getting table at timestamp: {}", timestamp);
            get_table_at_timestamp(data.clone(), table_name, timestamp.clone()).await
        } else {
            debug!("Getting current table data for: {}", table_name);
            get_table_data(data.clone(), table_name).await
        };

        // Handle table data retrieval
        match table_data_result {
            Ok(mut table_data) => {
                debug!("Table data retrieved: {} rows", table_data.len());
                if !table_data.is_empty() {
                    debug!("First row sample: {:?}", table_data.first());
                }

                // If a WHERE clause exists, filter the rows based on it
                if let Some(condition) = &where_clause {
                    info!("Evaluating WHERE clause: {:?}", condition);
                    let column_names = match get_column_names_from_schema(data, table_name) {
                        Ok(names) => {
                            debug!("Column names from schema: {:?}", names);
                            names
                        },
                        Err(e) => {
                            error!("Error fetching column names: {}", e);
                            return HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error fetching column names: {}", e)}));
                        }
                    };

                    let original_count = table_data.len();
                    table_data.retain(|row| row == &column_names ||
                            evaluate_where_clause(condition, row, &column_names));
                    debug!("After WHERE filtering: {} rows (from {})", table_data.len(), original_count);
                    debug!("WHERE clause result: {:?}", table_data);
                }

                // Process the SELECT query
                debug!("Processing SELECT with {} rows", table_data.len());
                let result = process_select(columns, &table_data, data.clone(), table_name).await;
                debug!("SELECT result: {} rows", result.len());

                if result.len() <= 1 {
                    debug!("No matching rows found in result");
                    HttpResponse::Ok().json(serde_json::json!({ "message": "No matching rows found" }))
                } else {
                    match serde_json::to_string(&result) {
                        Ok(json) => {
                            debug!("Successfully serialized result");
                            HttpResponse::Ok().json(serde_json::from_str::<serde_json::Value>(&json).unwrap())
                        },
                        Err(e) => {
                            error!("Serialization error: {}", e);
                            HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Serialization error: {}", e)}))
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    error!("Table not found: {}", table_name);
                    HttpResponse::NotFound().json(serde_json::json!({"error": format!("Table '{}' not found", table_name)}))
                } else {
                    error!("Error retrieving table data: {}", e);
                    HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error retrieving table data: {}", e)}))
                }
            }
        }
    } else {
        error!("Invalid table name in SELECT query");
        HttpResponse::BadRequest().json(serde_json::json!({ "error": "Invalid table name" }))
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
            Ok(_) => HttpResponse::Ok().json(serde_json::json!({"message": "Table Created"})),
            Err(err) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error creating table: {}", err)}))
        }
    } else {
        HttpResponse::BadRequest().json(serde_json::json!({"error": "Invalid table name in CREATE TABLE"}))
    }
}

async fn handle_drop(
    table: &Identifier,
    data: &web::Data<AppState>
) -> HttpResponse {
    // Extract the table name
    let table_name = match table {
        Identifier::Name(name) => name,
        _ => {
            return HttpResponse::BadRequest().json(json!({
                "error": "Invalid table name"
            }))
        }
    };

    // Acquire a lock on the schema
    let mut schema = match data.schema.lock() {
        Ok(schema) => schema,
        Err(_) => {
            return HttpResponse::InternalServerError().json(json!({
                "error": "Could not acquire schema lock"
            }))
        }
    };

    // Attempt to drop the table
    match drop_table(&mut schema, table_name, data) {
        Ok(_) => {
            HttpResponse::Ok().json(json!({
                "message": format!("Table {} dropped successfully", table_name)
            }))
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(json!({
                "error": format!("Failed to drop table: {}", e)
            }))
        }
    }
}


async fn handle_insert(
    table: &Identifier,
    values: &[Identifier],
    columns: &[Identifier],
    data: &web::Data<AppState>,
) -> HttpResponse {
    if let Identifier::Name(table_name) = table {
        let row_data: Vec<String> = values
            .iter()
            .filter_map(|value| match value {
                Identifier::Literal(lit, _) => Some(lit.clone()),
                _ => None,
            })
            .collect();
        // Optionally handle column names
        let column_names = if columns.is_empty() {
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

        // Insert the row into the table and get back the UUID
        match insert_row(table_name, row_data, data, column_names).await {
            Ok(uuid) => HttpResponse::Ok().json(serde_json::json!({
                "message": "Row inserted",
                "uuid": uuid
            })),
            Err(err) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error inserting row: {}", err)})),
        }
    } else {
        HttpResponse::BadRequest().json(serde_json::json!({"error": "Invalid table name in INSERT statement"}))
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
            return HttpResponse::BadRequest().json(serde_json::json!({"error": "DELETE must include a WHERE clause with a valid filter"}));
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
                    return HttpResponse::NotFound().json(serde_json::json!({"error": "No rows matched the specified condition"}));
                }
                // Call `delete_row` for each filtered row
                for row in &rows_to_delete {
                    if let Some(row_id) = row.first() {
                        if let Err(e) = delete_row(table_name, row_id, data).await {
                            return HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error deleting row: {}", e)}));
                        }
                    }
                }
                HttpResponse::Ok().json(serde_json::json!({"message": format!("Deleted {} rows", rows_to_delete.len())}))
            }
            Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error loading initial data: {}", e)})),
        }
    } else {
        HttpResponse::BadRequest().json(serde_json::json!({"error": "Invalid table name in DELETE statement"}))
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
            return HttpResponse::BadRequest().json(serde_json::json!({"error": "UPDATE must include a WHERE clause with a valid UUID"}));
        }

        let condition = where_clause.as_ref().unwrap();
        if !is_uuid_where_clause(condition) {
            return HttpResponse::BadRequest().json(serde_json::json!({"error": format!("WHERE clause must contain a valid UUID condition for table '{}'",table_name)}));
        }

        // Load data to process the update
        let initial_table_name = format!("{}_initial", table_name);
        let initial_table_path = Path::new(&data.config.db_dir)
            .join(&data.config.table_dir)
            .join(&initial_table_name);

        match load_table_data_from_file(&initial_table_path) {
            Ok(initial_data) => {
                let column_names = get_column_names_from_schema(data, table_name).unwrap_or_default();
                info!("initial_data: {:?}", initial_data);

                // Filter rows based on the WHERE clause
                let filtered_rows: Vec<_> = initial_data
                    .iter()
                    .filter(|row| evaluate_where_clause(condition, row, &column_names))
                    .collect();

                info!("Filtered rows: {:?}", filtered_rows);

                if filtered_rows.is_empty() {
                    return HttpResponse::NotFound().json(serde_json::json!({"error": "No rows matched the specified condition"}));
                }

                // Apply updates to the filtered rows
                for row in &filtered_rows {
                    if let Some(row_id) = row.first() {
                        if let Err(e) = update_row(table_name, row_id, updated_values.clone(), data).await {
                            return HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error updating row: {}", e)}));
                        }
                    }
                }
                if let Err(e) = recalculate_table(data, table_name, initial_data).await {
                    return HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error recalculating data: {}", e)}));
                }
                HttpResponse::Ok().json(serde_json::json!({"message": "Rows updated"}))
            }
            Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({"error": format!("Error loading initial data: {}", e)})),
        }
    } else {
        HttpResponse::BadRequest().json(serde_json::json!({"error": "Invalid table name in UPDATE statement"}))
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
) -> Vec<Vec<serde_json::Value>> {
    let mut result = Vec::new();

    if table_data.len() <=1 {
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

            info!("Evaluating where clause: {} {} {}", left_val, operator, right_val);

            // Perform evaluation based on the operator
            match operator.as_str() {
                "=" => left_val == right_val,
                "!=" => left_val != right_val,
                ">" => left_val > right_val,
                "<" => left_val < right_val,
                ">=" => left_val >= right_val,
                "<=" => left_val <= right_val,
                "LIKE" => {
                    evaluate_like_condition(&left_val, &right_val)
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

pub async fn global_execute_query(ast_nodes: Vec<ASTNode>) -> anyhow::Result<HttpResponse> {
    // Attempt to retrieve the global app state
    let global_state = AppState::global_state()
        .ok_or_else(|| anyhow::anyhow!("Global app state not initialized"))?;

    // Execute the query using the global state
    let response = execute_query(
        ast_nodes,
        web::Data::from(global_state)
    ).await;
    info!("Global query execution result: {:?}", response);

    // Log the result
    match response.status() {
        actix_web::http::StatusCode::OK => {
            tracing::info!("Global query execution successful");
            Ok(response)
        },
        _ => {
            tracing::warn!("Global query execution failed with status: {}", response.status());
            Err(anyhow::anyhow!("Query execution failed"))
        }
    }
}

async fn check_for_reserved_words(sql_query: &str) -> Result<(), HttpResponse> {
    let reserved_keyword = USERS_TABLE;

    debug!("Checking for reserved keyword: {}", reserved_keyword);
    if sql_query.to_lowercase().contains(reserved_keyword) {
        // Reject the query with a clear error message
        debug!("Query contains reserved keyword: {}", reserved_keyword);
        return Err(HttpResponse::Forbidden().json(serde_json::json!({
            "error": format!("Query contains reserved keyword: {}", reserved_keyword)
        })));
    }

    Ok(())
}



#[post("/query")]
async fn execute_query_endpoint(
    req: HttpRequest,
    query: web::Json<String>,
    data: web::Data<AppState>,
) -> HttpResponse { // Return plain HttpResponse
    // Authenticate first
    match authenticate_request(&req, &data).await {
        Ok(_) => {
            let sql_query = query.into_inner();
            check_for_reserved_words(&sql_query).await.unwrap();
            let query_bytes = sql_query.as_bytes();

            match sql_parser(query_bytes) {
                Ok(ast_nodes) => {
                    // Successfully parsed query
                    execute_query(ast_nodes, data).await
                }
                Err(err) => {
                    // Handle parse failure with a unified JSON error response
                    HttpResponse::BadRequest().json(serde_json::json!({"error": format!("Failed to parse query: {}", err)}))
                }
            }
        }
        Err(auth_error) => auth_error.into()
    }
}
