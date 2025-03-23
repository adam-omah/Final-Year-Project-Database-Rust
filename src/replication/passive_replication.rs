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
use crate::replication::active_replication::{load_nodes, ReplicationNode, ReplicationRequest, ReplicationResponse};

// Custom error type for replication
#[derive(Debug)]
pub enum ReplicationError {
    ConfigLoadError(std::io::Error),
    NetworkError(String),
    ReplicationFailure(String),
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
    pub id: Uuid,
}

// Passive Replication Queue Manager
#[derive(Default)]
pub struct PassiveReplicationQueue {
    pub queue: Vec<QueuedReplicationRequest>,
}

impl PassiveReplicationQueue {
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    // Add a new replication request to the queue
    pub fn enqueue(&mut self, request: ReplicationRequest) -> Uuid {
        let queued_request = QueuedReplicationRequest {
            id: Uuid::new_v4(),
            request,
            attempts: 0,
            last_attempt: None,
        };
        let request_id = queued_request.id;
        self.queue.push(queued_request);
        request_id
    }

    // Remove a request from the queue
    pub fn remove(&mut self, request_id: &Uuid) {
        self.queue.retain(|req| req.id != *request_id);
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
                // Sleep for 60 seconds
                actix_web::rt::time::sleep(Duration::from_secs(60)).await;

                // Process the queue using a more straightforward async approach
                let mut processed_requests = Vec::new();

                {
                    let mut app_state_guard = app_state_clone.lock().unwrap();
                    let mut queue = app_state_guard.passive_replication_queue.lock().unwrap();

                    // Collect requests to process
                    for (index, queued_req) in queue.queue.iter_mut().enumerate() {
                        match try_replicate_to_nodes(&config_clone, &queued_req.request).await {
                            Ok(_) => {
                                // Mark for removal
                                processed_requests.push(index);
                            }
                            Err(_) => {
                                // Increment attempts
                                queued_req.attempts += 1;
                                queued_req.last_attempt = Some(chrono::Utc::now());
                            }
                        }
                    }

                    // Remove successfully processed requests (in reverse to maintain indices)
                    for &index in processed_requests.iter().rev() {
                        queue.queue.remove(index);
                    }
                }
            }
        });
    }

    // Stop method (optional, depending on your shutdown mechanism)
    pub fn stop(&mut self) {
        self.is_running = false;
    }
}

// Helper function to attempt replication to all nodes
async fn try_replicate_to_nodes(
    config: &DatabaseConfig,
    request: &ReplicationRequest
) -> Result<(), ReplicationError> {
    // Load nodes configuration
    let nodes = load_nodes(config)
        .map_err(ReplicationError::ConfigLoadError)?;

    for node in &nodes.nodes {
        // In a real scenario, you would use your specific HTTP client or service communication
        let success = replicate_to_single_node(node, request)
            .map_err(|e| ReplicationError::NetworkError(e)).await?;

        if !success {
            return Err(ReplicationError::ReplicationFailure(
                format!("Replication failed for node {}", node.name)
            ));
        }
    }
    Ok(())
}

async fn replicate_to_single_node(
    node: &ReplicationNode,
    request: &ReplicationRequest
) -> Result<bool, String> {
    // Use client from actix-web for HTTP request
    let client = awc::Client::default();

    // Attempt to send replication request to the node
    match client
        .post(&format!("{}/api/replication/push", node.url))
        .send_json(request)
        .await
    {
        Ok(mut response) => {
            // Check response status
            if response.status().is_success() {
                // Parse the response body
                match response.json::<ReplicationResponse>().await {
                    Ok(repl_response) => {
                        // Check specific replication status
                        match repl_response.status.as_str() {
                            "success" => Ok(true),
                            _ => Err(format!("Replication failed: {}",
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
