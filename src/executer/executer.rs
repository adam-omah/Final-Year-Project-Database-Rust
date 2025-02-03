use crate::query::parser::{basic_sql_parser, ASTNode, Identifier};
// Import your AppState
use crate::records::table::{create_table, get_table_data, insert_row};
use crate::schema::schema;
use crate::schema::schema::DataType;
use crate::AppState;
use actix_web::{post, web, HttpRequest, HttpResponse};
// Import your table functions

pub async fn execute_query(
    ast_nodes: Vec<ASTNode>,
    data: web::Data<AppState>,
    _req: HttpRequest,
) -> HttpResponse { // Previously impl Responder
    for i in 0..ast_nodes.len() {
        match &ast_nodes[i] {
            ASTNode::Select { columns } => {
                if i + 1 < ast_nodes.len() {
                    if let ASTNode::From { table } = &ast_nodes[i + 1] {
                        if let Identifier::Name(table_name) = table {
                            let table_data_result = get_table_data(data.clone(), table_name).await;

                            match table_data_result {
                                Ok(table_data) => {
                                    let result = process_select(columns, &table_data);
                                    match serde_json::to_string(&result) {
                                        Ok(json) => return HttpResponse::Ok().body(json),
                                        Err(e) => return HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
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
                        } else {
                            return HttpResponse::BadRequest().body("Invalid table name in FROM clause");
                        }
                    } else {
                        return HttpResponse::BadRequest().body("FROM clause missing after SELECT");
                    }
                } else {
                    return HttpResponse::BadRequest().body("FROM clause missing after SELECT");
                }
            }
            ASTNode::CreateTable { table, columns } => {
                if let Identifier::Name(table_name) = table {
                    let table = schema::Table {
                        name: table_name.to_string(),
                        columns: columns
                            .iter()
                            .map(|(name, col_type)| {
                                if let (Identifier::Name(col_name), Identifier::Name(type_name)) =
                                    (name, col_type)
                                {
                                    let data_type = DataType::from(type_name.as_str()); // Direct conversion
                                    schema::Column {
                                        name: col_name.clone(),
                                        data_type,
                                        rules: vec![],
                                    }
                                } else {
                                    panic!("Invalid column definition");
                                }
                            })
                            .collect(),
                    };

                    match create_table(&table, &data) {
                        Ok(_) => return HttpResponse::Ok().body("Table Created"),
                        Err(err) => return HttpResponse::InternalServerError()
                            .body(format!("Error creating table: {}", err)),
                    }
                } else {
                    return HttpResponse::BadRequest().body("Invalid table name in CREATE TABLE");
                }
            }

            ASTNode::Insert {
                table,
                values,
                columns,
            } => {
                if let Identifier::Name(table_name) = table {
                    // Process values based on provided columns
                    let values: Vec<String> = values
                        .iter()
                        .map(|v| {
                            if let Identifier::Literal(lit) = v {
                                lit.to_string()
                            } else {
                                panic!("Invalid literal value in INSERT")
                            }
                        })
                        .collect();

                    match insert_row(table_name, values, &data) {
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

            // Handle other AST nodes as needed
            _ => return HttpResponse::BadRequest().body("Unsupported AST Node type"),
        }
    }
    HttpResponse::BadRequest().body("No valid SQL query provided")
}

fn process_select(columns: &[Identifier], table_data: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut result = Vec::new();
    for row in table_data {
        let mut selected_row = Vec::new();
        for col in columns {
            match col {
                Identifier::Name(col_name) => {

                    if let Some(index) = find_column_index(table_data, col_name) {
                        if let Some(value) = row.get(index) {
                            selected_row.push(value.to_string());
                        }
                    }
                }
                Identifier::Star => selected_row.extend_from_slice(row),
                _ => (),
            }

        }
        result.push(selected_row);
    }
    result

}

fn find_column_index(table_data: &[Vec<String>], col_name: &str) -> Option<usize> {
    if let Some(header_row) = table_data.first() {
        header_row.iter().position(|col| col == col_name)
    } else {
        None
    }
}


#[post("/query")]
async fn execute_query_endpoint(
    query: web::Json<String>,
    data: web::Data<AppState>,
) -> HttpResponse { // Return plain HttpResponse
    let sql_query = query.into_inner();
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


