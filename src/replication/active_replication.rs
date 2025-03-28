
use actix_web::{post, web, Error, HttpResponse};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::sync::Arc;
use std::collections::HashMap;
use std::{fs, io};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use actix_web::error::ErrorInternalServerError;
use actix_web::rt::Runtime;
use awc::Client;
use futures::future::join_all;
use tracing::log::{error, info, trace};
use crate::AppState;
use crate::change_logging::change_logging::{ChangeLogEntry, ChangeType};
use crate::config::database_config::DatabaseConfig;
use crate::tables::table::recalculate_table_global;
use crate::recovery::recovery::LogRecoveryManager;
use crate::replication::passive_replication::{get_replication_queue_status, queue_passive_replication};
use crate::replication::replication_nodes::{load_nodes, ReplicationMode, ReplicationNode};
use crate::schema::schema::{refresh_schema, Schema};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationRequest {
    pub schema: Schema,
    pub entries: Vec<ChangeLogEntry>,
    pub target_node: ReplicationNode,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationResponse {
    pub status: String,
    pub message: Option<String>,
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
    action_log_entry(log_manager, entry).await
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
async fn action_log_entry(log_manager: &LogRecoveryManager, entry: &ChangeLogEntry) -> Result<(), io::Error> {
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
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Update =>
            log_manager.handle_row_update(&updates_table_path, &entry_value)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ChangeType::Delete =>
            log_manager.handle_row_deletion(&updates_table_path, &entry_value)
                .await
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

        let mut all_nodes_success = true;

        // Attempt to replicate to each node
        for node in nodes_config.nodes.iter().filter(|n| n.should_replicate(&log_entry.table_name)) {
            // Create a replication request specific to this node
            let replication_request = ReplicationRequest {
                schema: state.schema.lock().unwrap().clone(),
                entries: vec![log_entry.clone()],
                target_node: ReplicationNode {
                    name: node.name.clone(),
                    node_url: node.node_url.clone(),
                    description: node.description.clone(),
                    replication_mode: node.replication_mode.clone(),
                    shared_secret: node.shared_secret.clone(),
                },
            };

            match replicate_to_single_node(&client, node, &replication_request).await {
                Ok(true) => continue,
                Ok(false) | Err(_) => {
                    all_nodes_success = false;
                    // Fallback to passive replication, passing the prepared replication request
                    fallback_to_passive_replication(state.clone(), replication_request);
                }
            }
        }
        // Optional: Log or handle the case where not all nodes were successfully replicated
        if !all_nodes_success {
            tracing::warn!("Replication failed for some nodes");
        }
    });
}

pub async fn replicate_to_single_node(
    client: &Client,
    node: &ReplicationNode,
    request: &ReplicationRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    let addrs = node.resolve_node_url().map_err(|e| e.to_string())?;
    let target_addr = addrs.first().ok_or("Could not resolve any addresses")?;
    let url = if node.node_url.starts_with("https") {
        format!("https://{}/api/replication/push", target_addr)
    } else {
        format!("http://{}/api/replication/push", target_addr)
    };
    info!("Replication Node: {:#?}", node);
    info!("Replicating to URL: {}", url);  // Add trace log for URL
    trace!("Replication request: {:?}", request); // Add trace log for the request

    // Now use target_addr for awc requests
    let mut response = client
        .post(url)
        .send_json(request)
        .await?;

    if response.status().is_success() {
        let repl_response: ReplicationResponse = response.json().await?;
        trace!("Replication response: {:?}", repl_response); // Add trace log for the response
        Ok(repl_response.status == "success")
    } else {
        error!("HTTP error: {}", response.status()); // Add error log
        Err(format!("HTTP error: {}", response.status()).into())
    }
}

fn fallback_to_passive_replication(
    state: Arc<AppState>,
    replication_request: ReplicationRequest
) {
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
    // AGGRESSIVE LOGGING
    tracing::warn!("REPLICATION PUSH RECEIVED - FULL DEBUG MODE");
    tracing::warn!("Entries Count: {}", payload.entries.len());
    tracing::warn!("Target Node: {}", payload.target_node.name);

    // Load nodes configuration
    let nodes_config = match load_nodes(&app_state.config) {
        Ok(config) => config,
        Err(e) => {
            tracing::error!("Failed to load nodes configuration: {:?}", e);
            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                status: "failed".into(),
                message: Some("Could not load nodes configuration".into()),
            }));
        }
    };

    // Find the specific node configuration for the target node
    let target_node_config = nodes_config.nodes.iter()
        .find(|node| node.name == payload.target_node.name)
        .ok_or_else(|| {
            tracing::error!("No configuration found for node: {}", payload.target_node.name);
            actix_web::error::ErrorBadRequest("Invalid target node")
        })?;


    // Identify CREATE operations explicitly
    let create_or_drop_entries: Vec<&ChangeLogEntry> = payload.entries.iter()
        .filter(|entry| {
            // Check if the entry's table is in the specific tables for this node
            let is_node_table = match &target_node_config.replication_mode {
                ReplicationMode::Specific(specific_tables) => {
                    specific_tables.contains(&entry.table_name) &&
                        matches!(entry.change_type, ChangeType::Create | ChangeType::Drop)
                },
                ReplicationMode::All => {
                    // If replication mode is All, allow all tables
                    matches!(entry.change_type, ChangeType::Create | ChangeType::Drop)
                }
            };
            tracing::warn!(
                "Checking replication for table {}: {}",
                entry.table_name,
                is_node_table
            );
            is_node_table
        })
        .collect();

    tracing::warn!(
        "CREATE OR DROP ENTRIES COUNT FOR NODE {}: {}",
        payload.target_node.name,
        create_or_drop_entries.len()
    );

    // If filtered CREATE or DROP entries exist, force proceed
    if !create_or_drop_entries.is_empty() {
        tracing::error!(
            "FORCE PROCEEDING WITH REPLICATION DUE TO VALID CREATE OR DROP OPERATION FOR NODE {}",
            payload.target_node.name
        );

        for entry in &payload.entries {
            // Only process entries that are specific to this node's tables
            let should_process = match &target_node_config.replication_mode {
                ReplicationMode::Specific(specific_tables) => {
                    specific_tables.contains(&entry.table_name) &&
                        matches!(entry.change_type, ChangeType::Create | ChangeType::Drop)
                },
                ReplicationMode::All => {
                    matches!(entry.change_type, ChangeType::Create | ChangeType::Drop)
                }
            };

            if should_process {
                if let Err(e) = append_and_action_log(&app_state, entry).await {
                    tracing::error!(
                        "Forced replication failed for entry: {:?}",
                        e
                    );
                    return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                        status: "forced_failed".into(),
                        message: Some(format!("Forced replication failed: {:?}", e)),
                    }));
                }

                // Always refresh schema for node-specific CREATE or DROP
                if matches!(entry.change_type, ChangeType::Create | ChangeType::Drop) {
                    match refresh_schema(&app_state.config, &app_state) {
                        Ok(_) => tracing::warn!("SCHEMA FORCIBLY REFRESHED FOR CREATE OR DROP"),
                        Err(e) => {
                            tracing::error!("FORCED SCHEMA REFRESH FAILED: {:?}", e);
                            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                                status: "forced_schema_refresh_failed".into(),
                                message: Some("Forced schema refresh failed".into()),
                            }));
                        }
                    }
                }
            }
        }

        tracing::warn!(
            "REPLICATION COMPLETED WITH FORCE CREATE MODE FOR NODE {}",
            payload.target_node.name
        );
        return Ok(HttpResponse::Ok().json(ReplicationResponse {
            status: "force_success".into(),
            message: Some(format!(
                "Forced replication for CREATE completed for node {}",
                payload.target_node.name
            )),
        }));
    }

    let incoming_schema = &payload.schema;
    let current_schema = app_state.schema.lock().unwrap();

    let current_schema_json = match serde_json::to_value(&*current_schema) {
        Ok(json) => json,
        Err(e) => {
            tracing::error!("Failed to serialize current schema: {:?}", e);
            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                status: "failed".into(),
                message: Some("Failed to serialize current schema".into()),
            }));
        }
    };

    let incoming_schema_json = match serde_json::to_value(incoming_schema) {
        Ok(json) => json,
        Err(e) => {
            tracing::error!("Failed to serialize incoming schema: {:?}", e);
            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                status: "failed".into(),
                message: Some("Failed to serialize incoming schema".into()),
            }));
        }
    };

    let replication_node = &payload.target_node;

    match compare_schemas(&incoming_schema_json, &current_schema_json, Some(replication_node)) {
        Ok(true) => {
            tracing::debug!("Proceeding with standard replication");
            drop(current_schema);

            for (index, entry) in payload.entries.iter().enumerate() {
                if let Err(e) = append_and_action_log(&app_state, entry).await {
                    tracing::error!(
                        "Failed processing replication log entry {}: {:?}",
                        index,
                        e
                    );
                    return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                        status: "failed".into(),
                        message: Some(format!("Replication failed at entry {}: {:?}", index, e)),
                    }));
                }

                if matches!(entry.change_type, ChangeType::Create | ChangeType::Drop) {
                    match refresh_schema(&app_state.config, &app_state) {
                        Ok(_) => tracing::debug!("Schema refreshed successfully"),
                        Err(e) => {
                            tracing::error!("Failed to refresh schema: {:?}", e);
                            return Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                                status: "failed".into(),
                                message: Some("Unable to refresh schema".into()),
                            }));
                        }
                    }
                }
            }

            tracing::info!("Replication push completed successfully");
            Ok(HttpResponse::Ok().json(ReplicationResponse {
                status: "success".into(),
                message: None,
            }))
        },
        Ok(false) => {
            tracing::warn!("Schema comparison failed");
            Ok(HttpResponse::BadRequest().json(ReplicationResponse {
                status: "failed".into(),
                message: Some("Schema mismatch".into()),
            }))
        },
        Err(err) => {
            tracing::error!("Schema comparison error: {}", err);
            Ok(HttpResponse::InternalServerError().json(ReplicationResponse {
                status: "failed".into(),
                message: Some(format!("Schema comparison error: {}", err)),
            }))
        }
    }
}


fn compare_schemas(
    incoming_schema_json: &serde_json::Value,
    current_schema_json: &serde_json::Value,
    replication_node: Option<&ReplicationNode>
) -> Result<bool, String> {
    // Extract tables from both schemas
    let incoming_tables = incoming_schema_json
        .get("tables")
        .and_then(|tables| tables.as_object())
        .ok_or_else(|| "Could not extract tables from incoming schema".to_string())?;

    let current_tables = current_schema_json
        .get("tables")
        .and_then(|tables| tables.as_object())
        .ok_or_else(|| "Could not extract tables from current schema".to_string())?;

    // Debug logging of tables
    tracing::debug!("Incoming Schema Tables: {}", incoming_tables.keys().cloned().collect::<Vec<_>>().join(", "));
    tracing::debug!("Current Schema Tables: {}", current_tables.keys().cloned().collect::<Vec<_>>().join(", "));

    // Determine which tables to compare based on replication mode
    match replication_node {
        Some(node) => {
            match &node.replication_mode {
                ReplicationMode::All => {
                    // Compare all tables with detailed logging
                    // Similar logic as before
                },
                ReplicationMode::Specific(specific_tables) => {
                    tracing::debug!("Comparing specific tables: {}", specific_tables.join(", "));

                    for table_name in specific_tables {
                        // Generate both initial and updates table names
                        let initial_table_name = format!("{}_initial", table_name);
                        let updates_table_name = format!("{}_updates", table_name);

                        // Check both initial and updates tables
                        let check_table = |suffix_table_name: &str| {
                            match (
                                incoming_tables.get(suffix_table_name),
                                current_tables.get(suffix_table_name)
                            ) {
                                (Some(incoming_table), Some(current_table)) => {
                                    if incoming_table != current_table {
                                        tracing::warn!(
                                            "Table {} differs. Incoming: {:?}, Current: {:?}",
                                            suffix_table_name,
                                            incoming_table,
                                            current_table
                                        );
                                        false
                                    } else {
                                        true
                                    }
                                },
                                (None, Some(_)) => {
                                    tracing::warn!(
                                        "Table {} missing in incoming schema",
                                        suffix_table_name
                                    );
                                    false
                                },
                                (Some(_), None) => {
                                    tracing::warn!(
                                        "Table {} missing in current schema",
                                        suffix_table_name
                                    );
                                    false
                                },
                                (None, None) => {
                                    tracing::warn!(
                                        "Both {} tables are missing",
                                        suffix_table_name
                                    );
                                    false
                                }
                            }
                        };

                        // Ensure both initial and updates tables match
                        if !check_table(&initial_table_name) || !check_table(&updates_table_name) {
                            return Ok(false);
                        }
                    }
                }
            }
        }
        None => {
            tracing::warn!("No replication node configured. Schema comparison failed.");
            return Ok(false);
        }
    }

    // If we've made it this far, the relevant schemas match
    tracing::info!("Schema comparison successful");
    Ok(true)
}
