use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use std::error::Error;
use std::{fmt, thread};

use actix_web::{web, HttpResponse, HttpRequest, http, post, get, Error as ActixError, rt};
use actix_web::rt::spawn;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use chrono::Utc;
use futures::TryFutureExt;
use tokio::time::timeout;
use tracing::log::{debug, error, info, trace, warn};
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use crate::replication::active_replication::{replicate_to_single_node, replicate_to_single_node_global, ReplicationRequest, ReplicationResponse};
use crate::replication::replication_nodes::{load_nodes, ReplicationNode};

// Custom error type for replication
#[derive(Debug,Clone)]
pub enum ReplicationError {
    ConfigLoadError(String),
    NetworkError(String),
    ReplicationFailure(String),
    SerializationError(String),
}

#[derive(Default)]
pub struct StaleReplicationQueue {
    pub queue: HashMap<String, Vec<QueuedReplicationRequest>>, // Node name as key
}

impl StaleReplicationQueue {
    pub fn add_stale_request(&mut self, node_name: String, request: QueuedReplicationRequest) {
        self.queue
            .entry(node_name)
            .or_insert_with(Vec::new)
            .push(request);
    }

    pub fn get_stale_requests_for_node(&mut self, node_name: &str) -> Vec<QueuedReplicationRequest> {
        self.queue.remove(node_name).unwrap_or_default()
    }
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplicationError::ConfigLoadError(err) =>
                write!(f, "Failed to load configuration: {}", err),
            ReplicationError::NetworkError(err) =>
                write!(f, "Network error during replication: {}", err),
            ReplicationError::ReplicationFailure(msg) =>
                write!(f, "Replication failed: {}", msg),
            ReplicationError::SerializationError(msg) => write!(f, "Replication Serail failed: {}", msg),
        }
    }
}

impl Error for ReplicationError {}

// Struct to represent a queued replication request
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueuedReplicationRequest {
    pub request: ReplicationRequest,
    pub attempts: u32,
    pub last_attempt: Option<chrono::DateTime<chrono::Utc>>,
    pub first_failed_attempt: Option<chrono::DateTime<chrono::Utc>>,
    pub id: Uuid,
}


// Passive Replication Queue Manager
#[derive(Default, Clone)]
pub struct PassiveReplicationQueue {
    pub queue: Vec<QueuedReplicationRequest>,
}

impl PassiveReplicationQueue {
    pub fn enqueue(&mut self, request: ReplicationRequest) -> Uuid {
        let id = Uuid::new_v4();
        let queued_request = QueuedReplicationRequest {
            request,
            attempts: 0,
            last_attempt: None,
            first_failed_attempt: None,
            id,
        };
        self.queue.push(queued_request);
        id
    }

    pub fn get_requests(&mut self) -> Vec<QueuedReplicationRequest> {
        std::mem::take(&mut self.queue) // Take ownership of the queue
    }
}


// Passive Replication Service
pub struct PassiveReplicationService {
    pub(crate) is_running: bool,
}

impl PassiveReplicationService {
    pub fn new() -> Self {
        Self { is_running: false }
    }

    // Start the passive replication service
}

pub(crate) async fn try_replicate_request(
    app_state: &AppState,
    request: &QueuedReplicationRequest,
) -> Result<(), ReplicationError> {
    debug!(
        "Attempting to replicate request: ID = {}, Entries = {}",
        request.id,
        request.request.entries.len()
    );

    // Use reqwest which is Send
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| ReplicationError::NetworkError(e.to_string()))?;

    let config = app_state.config.clone();

    let nodes = match load_nodes(&config) {
        Ok(nodes) => {
            debug!("Loaded {} nodes from configuration", nodes.nodes.len());
            nodes
        },
        Err(e) => {
            error!("Failed to load nodes from configuration: {}", e);
            return Err(ReplicationError::ConfigLoadError(e.to_string()));
        }
    };

    let cloned_request = request.clone();

    let applicable_nodes: Vec<_> = nodes.nodes.iter()
        .filter(|n| {
            cloned_request.request.entries.iter()
                .all(|entry| n.should_replicate(&entry.table_name))
        })
        .cloned()
        .collect();

    debug!(
        "Total applicable nodes for replication: {}",
        applicable_nodes.len()
    );

    if applicable_nodes.is_empty() {
        warn!("No applicable nodes found for replication");
        return Ok(());
    }

    let replication_futures = applicable_nodes.into_iter().map(|node| {
        let cloned_request_inner = cloned_request.clone();
        let client = client.clone(); // reqwest::Client is Clone and Send

        async move {
            debug!("Starting replication for node: {}", node.name);

            let addrs = match node.resolve_node_url() {
                Ok(addrs) => {
                    debug!("Resolved {} addresses for node {}", addrs.len(), node.name);
                    addrs
                },
                Err(e) => {
                    error!(
                        "Failed to resolve address for node {}: {}",
                        node.name,
                        e
                    );
                    return Err(ReplicationError::NetworkError(e.to_string()));
                }
            };

            let target_addr = match addrs.first() {
                Some(addr) => {
                    debug!("Selected target address: {}", addr);
                    addr
                },
                None => {
                    error!("No addresses resolved for node {}", node.name);
                    return Err(ReplicationError::NetworkError(
                        "Could not resolve any addresses".to_string()
                    ));
                }
            };

            let url = if node.node_url.starts_with("https") {
                format!("https://{}/api/replication/push", target_addr)
            } else {
                format!("http://{}/api/replication/push", target_addr)
            };

            debug!(
                "Replication Details - Node: {}, URL: {}, Entries: {}",
                node.name,
                url,
                cloned_request_inner.request.entries.len()
            );

            let replication_result = client
                .post(&url)
                .json(&cloned_request_inner.request)
                .send()
                .await;

            match replication_result {
                Ok(response) => {
                    if response.status().is_success() {
                        debug!(
                            "Successful replication to node {} with status {}",
                            node.name,
                            response.status()
                        );
                        Ok(())
                    } else {
                        error!(
                            "HTTP error replicating to {} - Status: {}",
                            node.node_url,
                            response.status()
                        );
                        Err(ReplicationError::NetworkError(
                            format!("HTTP error to {}: {}", node.node_url, response.status())
                        ))
                    }
                },
                Err(e) => {
                    error!(
                        "Replication error to {} - Error: {}",
                        node.node_url,
                        e
                    );
                    Err(ReplicationError::NetworkError(
                        format!("Replication error to {}: {}", node.node_url, e)
                    ))
                }
            }
        }
    });

    let results = futures::future::join_all(replication_futures).await;

    let failed_replications: Vec<_> = results
        .into_iter()
        .filter_map(|result| result.err())
        .collect();

    if !failed_replications.is_empty() {
        error!(
            "Replication failed for {} nodes. First error: {:?}",
            failed_replications.len(),
            failed_replications[0]
        );
        Err(failed_replications[0].clone())
    } else {
        info!("All replication attempts completed successfully");
        Ok(())
    }
}

// Actix route for initiating passive replication
#[post("/api/passive-replication/queue")]
pub async fn queue_passive_replication(
    app_state: web::Data<AppState>,
    payload: web::Json<ReplicationRequest>,
) -> Result<HttpResponse, ActixError> {
    // Get the passive replication queue from app state
    let mut queue = app_state.passive_replication_queue.lock().unwrap();

    // Enqueue the replication request
    let request_id = queue.enqueue(payload.into_inner());

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "queued",
        "request_id": request_id
    })))
}

// Optional: Route to check replication queue status
#[get("/api/passive-replication/status")]
pub async fn get_replication_queue_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ActixError> {
    let queue = app_state.passive_replication_queue.lock().unwrap();

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "queue_length": queue.queue.len(),
        "requests": queue.queue.iter().map(|req| {
            serde_json::json!({
                "id": req.id,
                "attempts": req.attempts,
                "last_attempt": req.last_attempt
            })
        }).collect::<Vec<_>>()
    })))
}
