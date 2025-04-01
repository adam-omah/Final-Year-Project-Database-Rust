use std::collections::HashMap;
use std::sync::{ Arc, Mutex};
use std::error::Error;
use std::{fmt};
use std::time::Duration;
use actix_web::{web, HttpResponse, post, get, Error as ActixError};
use actix_web::web::Data;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use tokio::time::{ Instant};
use tracing::log::{debug, error, info, warn};
use crate::AppState;
use crate::replication::active_replication::{ ReplicationRequest };
use crate::replication::replication_nodes::{load_nodes};
use crate::replication::replication_sync_checker::try_perform_replication_sync;

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
            .or_default()
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

pub(crate) async fn try_replicate_request(
    app_state: &AppState,
    request: &QueuedReplicationRequest,
) -> Result<(), ReplicationError> {
    info!(
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
            info!("Loaded {} nodes from configuration", nodes.nodes.len());
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

    info!(
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
            info!("Starting replication for node: {}", node.name);

            let addrs = match node.resolve_node_url() {
                Ok(addrs) => {
                    info!("Resolved {} addresses for node {}", addrs.len(), node.name);
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
                    info!("Selected target address: {}", addr);
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

            info!(
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
                        info!(
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
    app_state: Data<AppState>,
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


pub async fn replication_scheduler_loop(passive_replication_interval: i64, sync_interval: u64) {
    info!("Starting inside the async Loop Function");
    let passive_interval_duration = std::time::Duration::from_secs(passive_replication_interval as u64);
    let sync_interval_duration = std::time::Duration::from_secs(sync_interval);
    let mut passive_last_tick =  Instant::now() - Duration::from_secs(5);
    let mut sync_last_tick = Instant::now() - Duration::from_secs(5);
    let failure_counts: Arc<Mutex<HashMap<Uuid, u32>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let now = Instant::now();
        let passive_elapsed = now.duration_since(passive_last_tick);
        let sync_elapsed = now.duration_since(sync_last_tick);

        if sync_elapsed >= sync_interval_duration && sync_interval > 0 {
            sync_last_tick = now;
            info!("Running scheduled replication sync check at: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
            // Retrieve the global app state
            if let Some(global_state) = AppState::global_state() {
                // Clone the global app state for the task
                let app_state = global_state.clone(); // Just clone the AppState
                // Call the function using try pattern
                match try_perform_replication_sync(Data::from(app_state)).await {
                    Ok(_) => info!("Scheduled replication sync check completed successfully"),
                    Err(e) => error!("Scheduled replication sync check failed: {}", e),
                }
            } else {
                error!("Failed to retrieve global application state for scheduled sync check.");
            }
        } else if passive_elapsed >= passive_interval_duration && passive_replication_interval > 0 {
            passive_last_tick = now;
            info!("Running scheduled passive replication check at: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());

            if let Some(global_state) = AppState::global_state() {
                let app_state = global_state.clone();

                // Clone requests instead of locking
                let requests = {
                    let mut queue = app_state.passive_replication_queue.lock().unwrap();
                    queue.get_requests().clone() // Ensure PassiveReplicationQueue implements Clone
                };

                info!("Passive replication queue size: {}, contents: {:#?}", requests.len(), requests);
                // Process requests without holding the lock
                for request in requests {
                    let app_state_clone = app_state.clone();
                    let failure_counts_clone = Arc::clone(&failure_counts);
                    let request_id = request.id;


                    info!("Passive replication request: {:#?}", request);
                    match try_replicate_request(&app_state_clone, &request).await {
                        Ok(_) => {
                            let mut counts = failure_counts_clone.lock().unwrap();
                            counts.remove(&request_id);
                        }
                        Err(err) => {
                            let mut counts = failure_counts_clone.lock().unwrap();
                            let count = counts.entry(request_id).or_insert(0);
                            *count += 1;

                            error!("Replication failed for request {}: {}", request_id, err);

                            if *count >= app_state_clone.config.replication.max_replication_attempts {
                                error!("Request {} failed too many times. Marking as failed.", request_id);
                            }
                        }
                    }
                }
            } else {
                error!("Failed to retrieve global application state for passive replication check.");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

// Optional: Route to check replication queue status
#[get("/api/passive-replication/status")]
pub async fn get_replication_queue_status(
    app_state: Data<AppState>,
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
