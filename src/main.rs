use actix_web::{ web, App, HttpRequest, HttpServer, Responder};
use std::collections::{BTreeMap};
use std::env;
use actix_files::Files;
use std::io::{Result};
use std::string::String;
use std::sync::{Arc, Mutex};
use tracing::log::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::executer::executer::execute_query_endpoint;
use crate::config::database_config::DatabaseConfig;
use crate::records::table::{get_column_names_api, get_table_api, get_table_at_timestamp_api, list_tables_api};
use schema::{
    schema::load_schema,
    schema::Schema,
};
use crate::change_logging::change_logging::ChangeLogger;

// Module Imports.
pub mod config;
pub mod schema;
pub mod records;
pub mod query;
pub mod executer;
pub mod change_logging;


// public constants
pub const DB_DIR: &str = "my_rust_db";
pub const SCHEMA_FILE: &str = "schema.json";
pub const TABLE_DIR: &str = "tables";

#[derive(Clone)]
pub struct AppState {
    pub schema: Arc<Mutex<Schema>>,
    pub config: DatabaseConfig,
    pub cache: Arc<Mutex<BTreeMap<String, Vec<Vec<String>>>>>, // Cache holds up-to-date data.
    pub change_logger: ChangeLogger,
}

fn init_database(config: &DatabaseConfig) -> Result<()> {
    std::fs::create_dir_all(&config.db_dir)?;
    info!("Creating database directory: '{}'", config.db_dir.display());
    std::fs::create_dir_all(config.db_dir.join(config.table_dir.as_path()))?;
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load database configuration and schema
    let config = DatabaseConfig::from_yaml(&[
        "./config.yaml",           // Current directory
        "/app/config.yaml",        // Docker container path
        "/etc/myapp/config.yaml",  // System-wide config
        "../config.yaml",          // From local
    ])
        .unwrap_or_else(|_| DatabaseConfig::default());
    init_database(&config)?;
    let schema = Arc::new(Mutex::new(load_schema(&config)?));
    let cache = Arc::new(Mutex::new(BTreeMap::new())); // Initialize the cache.

    // Read hostname from environment variable, default to 0.0.0.0
    let hostname = env::var("HOSTNAME").unwrap_or_else(|_| "0.0.0.0".to_string());

    // Read port from environment variable, default to 8080
    let port = env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("Invalid port number");

    //log directory
    let log_directory = config.db_dir.clone();

    // Initialize the database
    init_database(&config).expect("Failed to initialize database");

    // Create app state
    let app_state = AppState {
        schema: schema.clone(),
        config: config.clone(),
        cache: cache.clone(),
        change_logger: ChangeLogger::new(log_directory.clone()),
    };



    // Start the Actix Web HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            // API endpoints
            .service(get_table_api)               // Updated API for fetching table data
            .service(get_table_at_timestamp_api)  // Updated API for fetching table data at a specific timestamp
            .service(list_tables_api)             // Updated API for listing all tables
            .service(execute_query_endpoint)
            .service(get_column_names_api)// Existing route unchanged
            // Static file serving
            .service(Files::new("/static", "./static").show_files_listing())
            // Route for `/tables` -> `tables.html`
            .route("/tables", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/tables.html").unwrap()
            }))
            // Route for `/tables/{table_name}` -> `table_data.html`
            .route("/tables/{table_name}", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/table_data.html").unwrap()
            }))
            // Handle root route `/` to load `index.html`
            .route("/", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/index.html").unwrap()
            }))
})
        .bind((hostname.as_str(), port))? // Bind to all network interfaces on port 8080
        .run()
        .await?;

    Ok(())
}



/*
* Tests Below this line, as per rust testing paradigm
*/
#[cfg(test)]
mod app_tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use crate::config::database_config::DatabaseConfig;
    use crate::init_database;
    use std::fs;
    use actix_web::http::StatusCode;
    use actix_web::test;
    use serde_json::Value;
    use schema::{schema::load_schema, schema::Column, schema::Table};
    use records::{table::create_table, table::insert_row};

    #[actix_web::test]
    pub async fn test_application() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test1";
        let test_db_dir = PathBuf::from(TEST_DB_DIR);
        // Remove the directory and its contents if it exists.  If it doesn't exist, this is a no-op.
        if test_db_dir.exists() { fs::remove_dir_all(&test_db_dir).expect("Failed to remove test database directory"); }
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
            change_logger: ChangeLogger::new(test_config.db_dir.clone()),
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
            create_table(&user_table, &app_state)?;
            insert_row("users", vec!["9fe085ca-6e32-4757-86aa-b3774eb6561f".to_string(), "1".to_string(), "Alice".to_string(), "2025-02-17 11:39:41".to_string()], &app_state, None).await?;
            insert_row("users", vec!["b42cd5b1-b732-427a-8bdb-a6aad547c168".to_string(), "2".to_string(), "Bob".to_string(), "2025-02-17 11:39:41".to_string()], &app_state, None).await?;
        };



        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(get_table_api)
        )
            .await;


        // Create a test request
        let req = test::TestRequest::get().uri("/api/tables/users").to_request();
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
            serde_json::json!([["9fe085ca-6e32-4757-86aa-b3774eb6561f",1, "Alice","2025-02-17 11:39:41"], ["b42cd5b1-b732-427a-8bdb-a6aad547c168",2, "Bob","2025-02-17 11:39:41"]])
        );
        let _ = fs::remove_dir_all(TEST_DB_DIR);
        Ok(())
    }

    #[actix_web::test]
    async fn test_create_table_success() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test2";
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
            change_logger: ChangeLogger::new(test_config.db_dir.clone()),
        });

        // Initialize Actix Web app
        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(execute_query_endpoint),
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
        assert_eq!(body, "{\"message\":\"Table Created\"}");

        // Step 3: Check schema integrity
        let schema = app_state.schema.lock().unwrap();
        assert!(schema.tables.contains_key("test_table_initial")); // Table should exist
        let table = schema.tables.get("test_table_initial").unwrap();
        assert_eq!(table.name, "test_table_initial");
        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.columns[1].name, "col1");
        assert_eq!(table.columns[2].name, "col2");
        let _ = fs::remove_dir_all(TEST_DB_DIR);
        Ok(())
    }

    #[actix_web::test]
    async fn test_get_table_failure() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test3";
        let test_db_dir = PathBuf::from(TEST_DB_DIR);
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
            change_logger: ChangeLogger::new(test_config.db_dir.clone()),
        });

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(get_table_api)
        )
            .await;
        // Create a test request, accessing a non-existent table
        let req = test::TestRequest::get().uri("/tables/non_existent_table").to_request();
        // Execute the request and get the response
        let resp = test::call_service(&app, req).await;

        // Assert that the response is NOT OK (404 or similar)
        assert_ne!(resp.status(), StatusCode::OK); // Use assert_ne to check for a non-OK status
        let _ = fs::remove_dir_all(TEST_DB_DIR);
        Ok(())
    }


    #[actix_web::test]
    async fn test_create_table_failure() -> Result<()> {
        const TEST_DB_DIR: &str = "mydb_test4";
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
        let cache = Arc::new(Mutex::new(BTreeMap::new()));
        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: cache.clone(),
            change_logger: ChangeLogger::new(test_config.db_dir.clone()),
        });

        let app = test::init_service(
            App::new()
                .app_data(app_state.clone())
                .service(execute_query_endpoint),
        )
            .await;

        // Invalid query - missing parenthesis
        let raw_query = r#"CREATE TABLE test_table col1 Int, col2 String"#;
        let req = test::TestRequest::post()
            .uri("/query")
            .set_json(&raw_query)
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Assert for bad request
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let _ = fs::remove_dir_all(TEST_DB_DIR);
        Ok(())
    }
}