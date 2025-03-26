use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::error::Error;
use std::fmt;

use actix_web::{web, HttpResponse, HttpRequest, http, post, get, Error as ActixError};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use chrono::Utc;
use futures::TryFutureExt;
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use crate::replication::active_replication::{ ReplicationRequest, ReplicationResponse};
use crate::replication::replication_nodes::{load_nodes, ReplicationNode};

// Custom error type for replication
#[derive(Debug)]
pub enum ReplicationError {
    ConfigLoadError(std::io::Error),
    NetworkError(String),
    ReplicationFailure(String),
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
    pub failed_nodes: Vec<String>,
}


// Passive Replication Queue Manager
#[derive(Default)]
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
            failed_nodes: Vec::new(),
        };
        self.queue.push(queued_request);
        id
    }

    pub fn check_offline_nodes(
        &mut self,
        max_offline_duration: chrono::Duration,
        max_attempts: u32,
        stale_queue: &mut StaleReplicationQueue,
    ) {
        let now = chrono::Utc::now();

        // Partition the queue into active and stale requests
        let (active_requests, stale_requests): (Vec<_>, Vec<_>) = self.queue
            .drain(..)
            .partition(|queued_request| {
                if let Some(first_failed) = queued_request.first_failed_attempt {
                    let offline_duration = now - first_failed;

                    // If not exceeded limits, keep in active queue
                    !(queued_request.attempts >= max_attempts ||
                        offline_duration > max_offline_duration)
                } else {
                    // Keep requests that do not exceed the limits
                    true
                }
            });

        // Add stale requests to the stale queue
        for request in stale_requests {
            // Use the failed_nodes directly from the request
            for node_name in &request.failed_nodes {
                stale_queue.add_stale_request(node_name.clone(), request.clone());
            }
        }

        // Restore active requests
        self.queue = active_requests;
    }

    pub fn get_retriable_requests(&mut self, max_attempts: u32) -> Vec<&mut QueuedReplicationRequest> {
        self.queue.iter_mut()
            .filter(|req|
                // Requests eligible for retry:
                // 1. Have a first failed attempt
                // 2. Haven't exceeded max attempts
                req.first_failed_attempt.is_some() &&
                    req.attempts < max_attempts
            )
            .collect()
    }
}



// Passive Replication Service
pub struct PassiveReplicationService {
    is_running: bool,
}

impl PassiveReplicationService {
    pub fn new() -> Self {
        Self { is_running: false }
    }

    // Start the passive replication service
    pub fn start(
        &mut self,
        app_state: Arc<Mutex<AppState>>,
        config: DatabaseConfig
    ) {
        // Prevent multiple instances
        if self.is_running {
            return;
        }
        self.is_running = true;

        // Clone the necessary data for the async task
        let app_state_clone = Arc::clone(&app_state);
        let config_clone = config.clone();

        // Spawn the replication task using Actix runtime
        actix_web::rt::spawn(async move {
            loop {
                actix_web::rt::time::sleep(Duration::from_secs(60)).await;

                let mut processed_requests = Vec::new();

                {
                    let mut app_state_guard = app_state_clone.lock().unwrap();
                    let mut queue = app_state_guard.passive_replication_queue.lock().unwrap();

                    for (index, queued_req) in queue.queue.iter_mut().enumerate() {
                        // Check if enough time has passed since the last attempt based on config retry interval
                        let should_retry = match (queued_req.last_attempt, queued_req.attempts) {
                            (Some(last_attempt), attempts) if attempts > 0 => {
                                let time_since_last_attempt = chrono::Utc::now() - last_attempt;
                                time_since_last_attempt >= chrono::Duration::seconds(config_clone.replication.retry_interval) &&
                                    attempts < config_clone.replication.max_replication_attempts
                            },
                            (None, _) => true, // First attempt
                            _ => false
                        };

                        if should_retry {
                            // Pass the replication attempt information
                            match try_replicate_to_nodes(&config_clone, &queued_req.request).await {
                                Ok(_) => {
                                    processed_requests.push(index);
                                }
                                Err(ReplicationError::ReplicationFailure(failed_nodes_str)) => {
                                    // Parse the failed nodes string and update the request
                                    queued_req.failed_nodes = failed_nodes_str
                                        .replace("Failed nodes: ", "")
                                        .split(", ")
                                        .map(|s| s.to_string())
                                        .collect();

                                    queued_req.attempts += 1;
                                    queued_req.last_attempt = Some(chrono::Utc::now());

                                    // Set first failed attempt if not already set
                                    if queued_req.first_failed_attempt.is_none() {
                                        queued_req.first_failed_attempt = Some(chrono::Utc::now());
                                    }
                                }
                                Err(_) => {
                                    // Other errors, increment attempts
                                    queued_req.attempts += 1;
                                    queued_req.last_attempt = Some(chrono::Utc::now());
                                }
                            }
                        }
                    }

                    // Remove successfully processed requests
                    for &index in processed_requests.iter().rev() {
                        queue.queue.remove(index);
                    }
                }
            }
        });
    }
}

// Helper function to attempt replication to all nodes
async fn try_replicate_to_nodes(
    config: &DatabaseConfig,
    request: &ReplicationRequest
) -> Result<(), ReplicationError> {
    let nodes = load_nodes(config)
        .map_err(|e| ReplicationError::ConfigLoadError(e))?;

    let mut failed_nodes: Vec<String> = Vec::new();

    // Process nodes sequentially to maintain order
    for node in nodes.nodes.iter().filter(|n|
        request.entries.iter().all(|entry| n.should_replicate(&entry.table_name))
    ) {
        match replicate_to_single_node(node, request).await {
            Ok(true) => continue,
            Ok(false) | Err(_) => {
                failed_nodes.push(node.name.clone());
                return Err(ReplicationError::ReplicationFailure(
                    format!("Failed nodes: {}", failed_nodes.join(", "))
                ));
            }
        }
    }

    // If we've gone through all nodes without returning an error, it means all succeeded
    if failed_nodes.is_empty() {
        Ok(())
    } else {
        // This should not happen given the early return, but kept for completeness
        Err(ReplicationError::ReplicationFailure(
            format!("Failed nodes: {}", failed_nodes.join(", "))
        ))
    }
}




async fn replicate_to_single_node(
    node: &ReplicationNode,
    request: &ReplicationRequest
) -> Result<bool, String> {
    // Use the existing client or create a new one (assuming using awc)
    let client = awc::Client::new();

    // Prepare the replication request
    let replication_request = ReplicationRequest {
        schema: request.schema.clone(),
        entries: request.entries.clone(),
        target_node: node.clone(),
    };

    // Attempt to send the replication request to the node
    match client
        .post(format!("{}/api/replication/push", node.node_url))
        .send_json(&replication_request)
        .await
    {
        Ok(mut response) => {
            // Check if the response was successful
            if response.status().is_success() {
                // Parse the response
                match response.json::<ReplicationResponse>().await {
                    Ok(repl_response) => {
                        if repl_response.status == "success" {
                            Ok(true)
                        } else {
                            Err(format!("Replication failed: {}",
                                        repl_response.message.unwrap_or_default()))
                        }
                    }
                    Err(_) => Err("Failed to parse replication response".to_string())
                }
            } else {
                Err(format!("HTTP error: {}", response.status()))
            }
        }
        Err(e) => Err(format!("Network error: {}", e))
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
