use actix_web::{web, App, HttpServer};
use std::collections::BTreeMap;
use std::io::{Result};
use std::string::String;
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// Module Imports.
pub mod config;
pub mod schema;
pub mod records;
pub mod query;
mod executer;

use crate::config::database_config::DatabaseConfig;

use crate::records::table::get_table;
use schema::{
    schema::load_schema,
    schema::Schema,
};
use crate::executer::executer::execute_query_endpoint;

// public constants
pub const DB_DIR: &str = "mydb";
pub const SCHEMA_FILE: &str = "schema.json";
pub const TABLE_DIR: &str = "tables";

#[derive(Clone)]
pub struct AppState {
    pub schema: Arc<Mutex<Schema>>,
    pub config: DatabaseConfig,
    pub cache: Arc<Mutex<BTreeMap<String, Vec<Vec<String>>>>>
}


fn init_database(config: &DatabaseConfig) -> Result<()> {
    std::fs::create_dir_all(&config.db_dir)?;
    std::fs::create_dir_all(config.db_dir.join(config.table_dir.as_path()))?;
    Ok(())
}

#[actix_web::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = DatabaseConfig::default();
    init_database(&config)?;
    let schema = Arc::new(Mutex::new(load_schema(&config)?));
    let cache = Arc::new(Mutex::new(BTreeMap::new()));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState { schema: schema.clone(),
                config: config.clone(),
                cache: cache.clone(),}))
            .service(get_table)
            .service(execute_query_endpoint)
    })
        .bind(("0.0.0.0", 8080))?
        .run()
        .await?;
    Ok(())
}





/*
* Tests Below this line, as per rust testing paradigm
*/

#[actix_web::test]
async fn test_application() -> Result<()> {
    use std::fs;
    use actix_web::http::StatusCode;
    use actix_web::test;
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
    let cache = Arc::new(Mutex::new(BTreeMap::new()));


    // Create a test application with your route
    let app_state = web::Data::new(AppState {
        schema: schema.clone(),
        config: test_config.clone(),
        cache: cache.clone(),
    });



    if !schema.lock().unwrap().tables.contains_key("users") {
        let user_table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: schema::schema::DataType::Int,
                    rules: vec![],
                },
                Column {
                    name: "name".to_string(),
                    data_type: schema::schema::DataType::String,
                    rules: vec![],
                },
            ],
        };
        if !schema.lock().unwrap().tables.contains_key("users") {
            create_table(&user_table, &app_state)?;
            insert_row("users", vec!["1".to_string(), "Alice".to_string()], None,&app_state)?;
            insert_row("users", vec!["2".to_string(), "Bob".to_string()], None ,&app_state)?;
        };
    };



    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .service(get_table)
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