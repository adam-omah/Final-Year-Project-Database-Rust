use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use std::fs::File;
use std::io::{BufRead, BufReader, Result};
use std::string::String;
use std::sync::{Arc, Mutex};

pub mod config;
pub mod schema;
pub mod records; // Use 'table' module name

use crate::config::database_config::DatabaseConfig;
use schema::{
    schema::load_schema,
    schema::Schema,
};

pub const DB_DIR: &str = "mydb"; // Make DB_DIR public
pub const SCHEMA_FILE: &str = "schema.json";
pub const TABLE_DIR: &str = "tables"; // Added TABLE_DIR constant


pub struct AppState {
    pub schema: Arc<Mutex<Schema>>,
    pub config: DatabaseConfig,
}

fn init_database(config: &DatabaseConfig) -> Result<()> {
    std::fs::create_dir_all(&config.db_dir)?;
    std::fs::create_dir_all(config.db_dir.join(config.table_dir.as_path()))?;
    Ok(())
}

#[actix_web::main]
async fn main() -> Result<()> {
    let config = DatabaseConfig::default();
    init_database(&config)?;
    let schema = Arc::new(Mutex::new(load_schema(&config)?));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                schema: schema.clone(),
                config: config.clone(),
            }))
            .service(get_table)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await?;
    Ok(())
}


#[get("/tables/{table_name}")]
async fn get_table(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let table_name = path.into_inner();
    let state = data.into_inner();
    let schema = state.schema.lock().unwrap();

    if let Some(_table) = schema.tables.get(&table_name) {
        let table_path = state.config.db_dir.join(state.config.table_dir.as_path()).join(&table_name);
        if let Ok(file) = File::open(table_path) {
            let reader = BufReader::new(file);
            let mut table_data = Vec::new();

            for line_result in reader.lines() {
                if let Ok(line) = line_result {
                    let row_values: Vec<String> = line
                        .split(',')
                        .map(|s| s.trim_matches('"').to_string()) // Remove quotes during parsing
                        .collect();
                    table_data.push(row_values)
                } else {
                    return HttpResponse::InternalServerError().body("Error reading a line");
                }
            }

            // Serialize table data to JSON
            match serde_json::to_string(&table_data) {
                Ok(json) => HttpResponse::Ok().body(json),
                Err(e) => HttpResponse::InternalServerError().body(format!("Serialization error: {}", e)),
            }
        } else {
            HttpResponse::NotFound().body("Table data file not found")
        }
    } else {
        HttpResponse::NotFound().body("Table not found in schema")
    }
}


/*
* Tests Below this line, as per rust testing paradigm
*/

#[actix_web::test]
async fn test_application() -> Result<()> {
    use std::fs;
    use actix_web::http::StatusCode;
    use actix_web::{test};
    use serde_json::Value;
    use std::path::PathBuf;
    use schema::{schema::load_schema, schema::Column, schema::Table};
    use records::{table::create_table, table::insert_row};

    const TEST_DB_DIR: &str = "mydb_test";

    let test_db_dir = PathBuf::from(TEST_DB_DIR);

    // Remove the directory and its contents if it exists.  If it doesn't exist, this is a no-op.
    if test_db_dir.exists() {
        fs::remove_dir_all(&test_db_dir).expect("Failed to remove test database directory");
    }


    let test_config = DatabaseConfig {
        db_dir: PathBuf::from(TEST_DB_DIR),
        ..Default::default() // Use default for other values
    };

    init_database(&test_config)?;
    let schema = Arc::new(Mutex::new(load_schema(&test_config)?));

    if !schema.lock().unwrap().tables.contains_key("users") {
        let user_table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: crate::schema::schema::DataType::Int,
                    rules: vec![],
                },
                Column {
                    name: "name".to_string(),
                    data_type: crate::schema::schema::DataType::String,
                    rules: vec![],
                },
            ],
        };

        create_table(&user_table, &test_config)?;
        insert_row("users", vec!["1".to_string(), "Alice".to_string()], &test_config)?;
        insert_row("users", vec!["2".to_string(), "Bob".to_string()], &test_config)?; // Correct insert_row usage
    };

    let schema = Arc::new(Mutex::new(load_schema(&test_config)?));

    // Create a test application with your route
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(AppState {
                schema: schema.clone(), // Use schema.clone() in app_data
                config: test_config.clone(),
            }))
            .service(get_table),
    )
        .await;

    // Create a test request
    let req = test::TestRequest::get().uri("/tables/users").to_request();

    // Execute the request and get the response
    let resp = test::call_service(&app, req).await;

    // Improved error reporting
    if resp.status() != StatusCode::OK {
        let status = resp.status();
        let body = test::read_body(resp).await;  // Read the error response body
        let body_str = String::from_utf8_lossy(&body); // Convert to string for printing
        panic!("Test request failed with status {}: {}", status, body_str); // Fail with details
    }


    // Parse the JSON response
    let result: Value = test::read_body_json(resp).await;

    // Assert the expected data
    assert_eq!(
        result,
        serde_json::json!([["1", "Alice"], ["2", "Bob"]])
    );
    Ok(())
}