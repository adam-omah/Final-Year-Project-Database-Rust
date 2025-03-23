
use actix_web::{post, web, Error, HttpResponse};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::sync::Arc;
use std::collections::HashMap;
use std::{fs, io};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use awc::Client;
use futures::future::join_all;
use tracing::log::{error, info};
use crate::AppState;
use crate::change_logging::change_logging::{ChangeLogEntry, ChangeType};
use crate::config::database_config::DatabaseConfig;
use crate::recovery::recovery::LogRecoveryManager;
use crate::replication::passive_replication::{get_replication_queue_status, queue_passive_replication};
use crate::schema::schema::{refresh_schema, Schema};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationRequest {
    pub schema: Schema,
    pub entries: Vec<ChangeLogEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationResponse {
    pub status: String,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationNode {
    pub name: String,
    pub url: String,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodesConfig {
    pub nodes: Vec<ReplicationNode>,
}

async fn append_and_action_log(app_state: &web::Data<AppState>, entry: &ChangeLogEntry) -> Result<(), io::Error> {
    let log_manager = &app_state.log_recovery_manager;

    let log_file_path = app_state.change_logger.log_file_path();

    // First, ensure log is not duplicated
    if is_log_already_present(&log_file_path, &entry.change_id)? {
        info!("Log already present {}", entry.change_id);
        return Ok(());
    }

    // Serialize and append entry to log file.
    let serialized_entry = serde_json::to_string(&entry)?;
    append_to_file(&log_file_path, &serialized_entry)?;

    // Now directly action the change similar to recovery
    action_log_entry(log_manager, entry)
}

// Helper checking if the log is already present to avoid duplication
fn is_log_already_present(log_file_path: &std::path::PathBuf, change_id: &Uuid) -> Result<bool, std::io::Error> {
    // Attempt to open the file
    match OpenOptions::new().read(true).open(log_file_path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line_str = line?;
                if line_str.contains(&change_id.to_string()) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            // If the file doesn't exist yet, the entry can't exist either
            Ok(false)
        }
        Err(e) => Err(e),  // Other errors propagate as normal
    }
}


fn append_to_file(file_path: &std::path::PathBuf, content: &str) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().append(true).create(true).open(file_path)?;
    writeln!(file, "{}", content)?;
    Ok(())
}

// This logic mimics recovery's log handling, actions changes directly.
fn action_log_entry(log_manager: &LogRecoveryManager, entry: &ChangeLogEntry) -> Result<(), io::Error> {
    let db_config = &log_manager.config;
    let table_dir = &db_config.table_dir;
    let db_dir = &db_config.db_dir;
    let initial_table_path = format!(
        "{}/{}/{}_initial",
        db_dir.display(),
        table_dir.display(),
        entry.table_name
    );
    let updates_table_path = format!(
        "{}/{}/{}_updates",
        db_dir.display(),
        table_dir.display(),
        entry.table_name
    );
    info!("replication passed initial table path {}", initial_table_path);
    let entry_value = serde_json::to_value(entry)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    match entry.change_type {
        ChangeType::Insert =>
            log_manager.handle_row_insertion(&initial_table_path, &entry_value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Update =>
            log_manager.handle_row_update(&updates_table_path, &entry_value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Delete =>
            log_manager.handle_row_deletion(&updates_table_path, &entry_value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Create =>
            log_manager.handle_table_creation(&initial_table_path, &updates_table_path, &entry_value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Drop =>
            log_manager.handle_drop_table(&initial_table_path, &updates_table_path, &entry_value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
    }
    Ok(())
}

pub fn load_nodes(config: &DatabaseConfig) -> io::Result<NodesConfig> {
    let path = Path::new(&config.log_dir).join(&config.repl_node_file);
    info!("Loading replication nodes from: {}", path.display());
    if path.exists() {
        let file = fs::File::open(path)?;
        serde_json::from_reader(file).map_err(|e|
            io::Error::new(io::ErrorKind::InvalidData, format!("JSON parsing failed: {}", e))
        )
    } else {
        Ok(Default::default())
    }
}

pub fn save_nodes(nodes: &NodesConfig, config: &DatabaseConfig) -> io::Result<()> {
    let path = Path::new(&config.log_dir).join(&config.repl_node_file);
    let file = fs::File::create(path)?;
    serde_json::to_writer_pretty(file, nodes).map_err(|e|
        io::Error::new(io::ErrorKind::Other, format!("Failed to serialize JSON: {}", e))
    )
}

pub fn replicate_change_to_nodes(
    state: Arc<AppState>,
    log_entry: ChangeLogEntry,
) {
    // Clone necessary data
    let nodes_config = match load_nodes(&state.config) {
        Ok(config) => config,
        Err(_) => return,  // Silently fail if nodes can't be loaded
    };

    // Spawn an async task for replication
    actix_web::rt::spawn(async move {
        let client = Client::default();

        let replication_request = ReplicationRequest {
            schema: state.schema.lock().unwrap().clone(),
            entries: vec![log_entry.clone()],
        };

        let mut all_nodes_success = true;

        // Attempt to replicate to each node
        for node in &nodes_config.nodes {
            match replicate_to_single_node(&client, node, &replication_request).await {
                Ok(true) => continue,
                Ok(false) | Err(_) => {
                    all_nodes_success = false;
                    // Fallback to passive replication for this node
                    fallback_to_passive_replication(state.clone(), log_entry.clone());
                }
            }
        }
    });
}

async fn replicate_to_single_node(
    client: &Client,
    node: &ReplicationNode,
    request: &ReplicationRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut response = client
        .post(&format!("{}/api/replication/push", node.url))
        .send_json(request)
        .await?;

    if response.status().is_success() {
        let repl_response: ReplicationResponse = response.json().await?;
        Ok(repl_response.status == "success")
    } else {
        Err(format!("HTTP error: {}", response.status()).into())
    }
}

fn fallback_to_passive_replication(
    state: Arc<AppState>,
    log_entry: ChangeLogEntry
) {
    let replication_request = ReplicationRequest {
        schema: state.schema.lock().unwrap().clone(),
        entries: vec![log_entry],
    };

    let mut queue = state.passive_replication_queue.lock().unwrap();
    queue.enqueue(replication_request);
}








pub fn configure_replication_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(replication_push)
        .service(queue_passive_replication)
        .service(get_replication_queue_status);
}

#[post("/api/replication/push")]
async fn replication_push(
    app_state: web::Data<AppState>,
    payload: web::Json<ReplicationRequest>,
) -> Result<HttpResponse, Error> {
    let incoming_schema = &payload.schema;
    let current_schema = app_state.schema.lock().unwrap();
    info!("Incoming schema: {:?}", incoming_schema);
    info!("Current schema: {:?}", current_schema);

    let entries_json = serde_json::to_string(&payload.entries)
        .expect("Serialization of entries failed");
    println!("Incoming entries: {}", entries_json);


    // Check if at least one CREATE or DROP is present
    let has_schema_change = payload.entries.iter().any(|e| {
        matches!(e.change_type, ChangeType::Create | ChangeType::Drop)
    });

    // If schemas don't match and no explicit CREATE/DROP present, reject
    if *current_schema != *incoming_schema && !has_schema_change {
        return Ok(HttpResponse::BadRequest().json(ReplicationResponse {
            status: "failed".into(),
            message: Some("Schema mismatch".into()),
        }));
    }

    drop(current_schema); // explicitly drop lock before async operation

    // Process each entry as intended; schema logic inside `append_and_action_log`
    for entry in &payload.entries {
        if let Err(e) = append_and_action_log(&app_state, entry).await {
            error!("Failed processing replication log entry: {:?}", e);
            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                status: "failed".into(),
                message: Some(format!("Replication failed: {:?}", e)),
            }));
        }
        // refresh the schema after changes.
        if has_schema_change {
            refresh_schema(&app_state.config, &app_state).expect("Unable to refresh Schema!");
        }
    }
    Ok(HttpResponse::Ok().json(ReplicationResponse {
        status: "success".into(),
        message: None,
    }))
}