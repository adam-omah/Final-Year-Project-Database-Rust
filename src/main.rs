use actix_web::{ web, App, HttpRequest, HttpServer, Responder};
use std::collections::{BTreeMap};
use std::env;
use std::fmt::Debug;
use actix_files::Files;
use std::io::{Result};
use std::string::String;
use std::sync::{Arc, Mutex, OnceLock};
use actix_web::rt::spawn;
use actix_web::rt::time::Instant;
use actix_web::web::Data;
use chrono::Duration;
use tracing::log::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;
use crate::executer::executer::execute_query_endpoint;
use crate::config::database_config::DatabaseConfig;
use crate::tables::table::{create_table, get_column_names_api, get_table_api, get_table_at_timestamp_api, list_tables_api};
use schema::{
    schema::load_schema,
    schema::Schema,
};
use crate::auth::auth::{ configure_auth_routes, create_default_user};
use crate::change_logging::change_logging::{configure_logging_routes, ChangeLogger};
use crate::query::parser::sql_parser;
use crate::recovery::recovery::{configure_recovery_routes, trigger_log_recovery, trigger_specific_table_recovery, LogRecoveryManager};
use crate::replication::active_replication::configure_replication_routes;
use crate::replication::passive_replication::{
    PassiveReplicationQueue,
    PassiveReplicationService
};
use crate::replication::replication_nodes::configure_node_routes;
use crate::replication::replication_sync_checker::{configure_sync_routes, perform_replication_sync, trigger_replication_sync};
use crate::schema::schema::{Column, DataType};

// Module Imports.
pub mod config;
pub mod schema;
pub mod tables;
pub mod query;
pub mod executer;
pub mod change_logging;
pub mod recovery;
pub mod replication;
mod auth;

// public constants
pub const DB_DIR: &str = "my_rust_db";
pub const SCHEMA_FILE: &str = "schema.json";
pub const TABLE_DIR: &str = "tables";
pub const USERS_TABLE: &str = "users";


#[derive(Clone)]
pub struct AppState {
    pub schema: Arc<Mutex<Schema>>,
    pub config: DatabaseConfig,
    pub cache: Arc<Mutex<BTreeMap<String, Vec<Vec<String>>>>>, // Cache holds up-to-date data.
    pub change_logger: ChangeLogger,
    pub log_recovery_manager: LogRecoveryManager,
    pub passive_replication_queue: Arc<Mutex<PassiveReplicationQueue>>,
    pub passive_replication_service: Arc<Mutex<PassiveReplicationService>>,
}

static GLOBAL_APP_STATE: OnceLock<Arc<Mutex<Option<AppState>>>> = OnceLock::new();


impl AppState {
    pub fn new(
        schema: Arc<Mutex<Schema>>,
        config: DatabaseConfig,
        cache: Arc<Mutex<BTreeMap<String, Vec<Vec<String>>>>>,
        change_logger: ChangeLogger,
        log_recovery_manager: LogRecoveryManager,
    ) -> Self {
        let app_state = Self {
            schema: schema.clone(),
            config: config.clone(),
            cache,
            change_logger,
            log_recovery_manager,
            passive_replication_queue: Arc::new(Mutex::new(PassiveReplicationQueue::default())),
            passive_replication_service: Arc::new(Mutex::new(PassiveReplicationService::new())),
        };

        // Initialize passive replication service
        app_state.start_passive_replication_service();

        app_state
    }

    // Method to start passive replication service
    fn start_passive_replication_service(&self) {
        // Add more detailed logging
        tracing::info!("Attempting to start passive replication service");

        let config = self.config.clone();
        let app_state = Arc::new(Mutex::new(self.clone()));

        // Additional diagnostic print
        println!("DIAGNOSTIC: Preparing to start passive replication service");
        tracing::debug!("Cloned config: {:?}", config);

        // Ensure we're not swallowing any potential errors
        match self.passive_replication_service.lock() {
            Ok(mut replication_service) => {
                tracing::info!("Successfully acquired lock on passive replication service");

                // Add a guard to prevent multiple starts
                if !replication_service.is_running {
                    println!("DIAGNOSTIC: Starting passive replication service");
                    replication_service.start(
                        app_state,
                        config
                    );
                    tracing::info!("Passive replication service started");
                } else {
                    tracing::warn!("Passive replication service already running");
                }
            },
            Err(e) => {
                tracing::error!("Failed to acquire lock on passive replication service: {:?}", e);
                println!("DIAGNOSTIC: Failed to acquire lock on passive replication service");
            }
        }
    }

    pub fn set_global_state(self) {
        // Initialize the global state if it's not already set
        GLOBAL_APP_STATE.get_or_init(|| Arc::new(Mutex::new(Some(self))));
    }


    // Method to get the global app state
    pub fn global_state() -> Option<Arc<AppState>> {
        if let Some(global) = GLOBAL_APP_STATE.get() {
            global.lock().unwrap().as_ref().map(|state| Arc::new(state.clone()))
        } else {
            None
        }
    }

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
    let config_paths = vec!["config.yaml", "./config.yaml", "/app/config.yaml", "/etc/myapp/config.yaml", "../config.yaml"];

    let config = DatabaseConfig::from_yaml(&config_paths).unwrap_or_else(|e| {
        println!("Failed to load config: {}", e);
        DatabaseConfig::default() // Fallback to default
    });
    // Initialize database
    init_database(&config)?;

    let schema = Arc::new(Mutex::new(load_schema(&config)?));

    // Read hostname from environment variable, default to 0.0.0.0
    let hostname = config.node_url.replace("http://", "");
    let port = config.node_port;

    //log directory
    let log_recovery_manager = LogRecoveryManager::new(config.clone());

    // Initialize the database
    init_database(&config).expect("Failed to initialize database");

    // Create app state
    let app_state = AppState::new(
        schema,
        config.clone(),
        Arc::new(Mutex::new(BTreeMap::new())),
        ChangeLogger::new(config.log_dir.clone(), config.log_file.clone()),
        log_recovery_manager
    );

    // Set as global state
    app_state.clone().set_global_state();

    // Verify global state was set correctly
    if let Some(_global_state) = AppState::global_state() {
        info!("Global application state initialized successfully");
    } else {
        panic!("Failed to initialize global application state");
    }


    // Create default admin user if not exists
    if let Err(e) = create_default_user(&app_state).await {
        error!("Failed to create default admin user: {}", e);
    }

    let sync_interval = app_state.config.replication.sync_interval;

    if sync_interval > 0 {
        info!("Starting replication sync check scheduler with interval: {} minutes", sync_interval);
        let app_state_clone = app_state.clone(); // Clone AppState for the task

        // Use actix_rt::spawn to start the scheduled task
        spawn(async move {
            let interval_duration = std::time::Duration::from_secs(sync_interval * 60);
            let mut last_tick = Instant::now();

            loop {
                let now = Instant::now();
                let elapsed = now.duration_since(last_tick);

                if elapsed >= interval_duration {
                    last_tick = now;
                    info!("Running scheduled replication sync check at: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());

                    // Retrieve the global app state
                    if let Some(global_state) = AppState::global_state() {
                        // Clone the global app state for the task
                        let app_state = global_state.clone(); // Just clone the AppState
                        // Call the function directly
                        if let Err(e) = perform_replication_sync(Data::from(app_state)).await {
                            error!("Scheduled replication sync check failed: {}", e);
                        }
                    } else {
                        error!("Failed to retrieve global application state for scheduled sync check.");
                    }
                }
            }
        });
    } else {
        info!("Replication sync check scheduler is disabled (sync_interval = 0)");
    }


    // Start the Actix Web HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            // API endpoints
            .service(get_table_api)
            .service(get_table_at_timestamp_api)
            .service(list_tables_api)
            .service(get_column_names_api)
            .service(execute_query_endpoint)
            // Configurations
            .configure(configure_recovery_routes)
            .configure(configure_replication_routes)
            .configure(configure_node_routes)
            .configure(configure_sync_routes)
            .configure(configure_logging_routes)
            .configure(configure_auth_routes)
            // Static file serving
            .service(Files::new("/static", "./static").show_files_listing())
            .route("/login-page", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/login.html").unwrap()
            }))
            // Route for `/tables` -> `tables.html`
            .route("/tables", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/tables.html").unwrap()
            }))
            // Route for `/tables/{table_name}` -> `table_data.html`
            .route("/tables/{table_name}", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/table_data.html").unwrap()
            }))
            // Replication Routes
            .route("/replication", web::get().to(|| async {
                actix_files::NamedFile::open("./static/html/replication.html").unwrap()
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
    use tables::{table::create_table, table::insert_row};

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
        let log_recovery_manager = LogRecoveryManager::new(test_config.clone());

        // Create a test application with your route
        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: cache.clone(),
            change_logger: ChangeLogger::new(test_config.log_dir.clone(), test_config.log_file.clone()),
            log_recovery_manager: log_recovery_manager.clone(),
            passive_replication_queue: Arc::new(Mutex::new(Default::default())),
            passive_replication_service: Arc::new(Mutex::new(PassiveReplicationService::new())),
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
        let cache = Arc::new(Mutex::new(BTreeMap::new()));
        let schema = Arc::new(Mutex::new(load_schema(&test_config)?));
        let log_directory = test_config.log_dir.clone();
        let log_recovery_manager = LogRecoveryManager::new(test_config.clone());

        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: cache.clone(),
            change_logger: ChangeLogger::new(test_config.log_dir.clone(), test_config.log_file.clone()),
            log_recovery_manager: log_recovery_manager.clone(),
            passive_replication_queue: Arc::new(Mutex::new(Default::default())),
            passive_replication_service: Arc::new(Mutex::new(PassiveReplicationService::new())),
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
        let log_directory = test_config.log_dir.clone();
        let log_recovery_manager = LogRecoveryManager::new(test_config.clone());

        // Create a test application with your route
        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: cache.clone(),
            change_logger: ChangeLogger::new(test_config.log_dir.clone(), test_config.log_file.clone()),
            log_recovery_manager: log_recovery_manager.clone(),
            passive_replication_queue: Arc::new(Mutex::new(Default::default())),
            passive_replication_service: Arc::new(Mutex::new(PassiveReplicationService::new())),
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
        let log_directory = test_config.log_dir.clone();
        let log_recovery_manager = LogRecoveryManager::new(test_config.clone());

        let app_state = web::Data::new(AppState {
            schema: schema.clone(),
            config: test_config.clone(),
            cache: cache.clone(),
            change_logger: ChangeLogger::new(test_config.log_dir.clone(), test_config.log_file.clone()),
            log_recovery_manager: log_recovery_manager.clone(),
            passive_replication_queue: Arc::new(Mutex::new(Default::default())),
            passive_replication_service: Arc::new(Mutex::new(PassiveReplicationService::new())),
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