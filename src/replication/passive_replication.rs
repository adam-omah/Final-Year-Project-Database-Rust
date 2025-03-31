use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use std::error::Error;
use std::{fmt, thread};

use actix_web::{web, HttpResponse, HttpRequest, http, post, get, Error as ActixError, rt};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use chrono::Utc;
use futures::TryFutureExt;
use tokio::time::timeout;
use tracing::log::{debug, error, info};
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use crate::replication::active_replication::{replicate_to_single_node, ReplicationRequest, ReplicationResponse};
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
#[derive(Default,Clone)]
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
    pub(crate) is_running: bool,
}

impl PassiveReplicationService {
    pub fn new() -> Self {
        Self { is_running: false }
    }

    pub fn start(
        &mut self,
        app_state: Arc<Mutex<AppState>>,
        config: DatabaseConfig,
    ) {
        debug!("Entering PassiveReplicationService::start");

        if self.is_running {
            debug!("Passive replication service already running. Exiting.");
            return;
        }

        self.is_running = true;
        debug!("Passive replication service set to running.");

        // Use async move to transfer ownership
        rt::spawn(async move {
            let app_state_clone = Arc::clone(&app_state);
            let config_clone = config.clone();

            loop { // Keep the service running in a loop
                match inner_replication_loop(&app_state_clone, &config_clone).await {
                    Ok(_) => {
                        println!("Replication loop completed successfully");
                        tracing::info!("Replication loop completed successfully");
                    }
                    Err(e) => {
                        println!("Replication loop failed: {:?}", e);
                        tracing::error!("Replication loop failed: {:?}", e);
                    }
                }
            }
        });
    }

    pub fn stop(&mut self) {
        debug!("Stopping PassiveReplicationService...");
        self.is_running = false;
        debug!("PassiveReplicationService stopped.");
    }
}


async fn inner_replication_loop(
    app_state_clone: &Arc<Mutex<AppState>>,
    config_clone: &DatabaseConfig,
) -> Result<(), anyhow::Error> {
    let retry_interval = config_clone.replication.retry_interval;
    debug!("Passive replication timer set for {} seconds", retry_interval);

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        thread::sleep(Duration::from_secs(retry_interval as u64));
        tx.send(()).unwrap();
    });

    println!("DIAGNOSTIC: Before thread sleep");
    tracing::info!("DIAGNOSTIC: Before thread sleep");

    rx.recv().unwrap(); // Wait for the timer to fire

    println!("DIAGNOSTIC: After thread sleep");
    tracing::info!("DIAGNOSTIC: After thread sleep");

    debug!("Passive replication loop started");

    let mut processed_requests = Vec::new();

    // Use anyhow for more flexible error handling
    let app_state_guard = app_state_clone.lock().map_err(|e| {
        error!("Failed to acquire lock on app_state: {}", e);
        anyhow::anyhow!("Failed to acquire app_state lock: {}", e)
    })?;

    let queue_lock = app_state_guard.passive_replication_queue.lock().map_err(|e| {
        error!("Failed to acquire lock on passive_replication_queue: {}", e);
        anyhow::anyhow!("Failed to acquire passive_replication_queue lock: {}", e)
    })?;

    let mut queue = queue_lock;
    debug!("Queue Length: {}", queue.queue.len());

    for (index, queued_req) in queue.queue.iter_mut().enumerate() {
        debug!("Processing Request Index: {}, Attempts: {}", index, queued_req.attempts);
        let should_retry = match (queued_req.last_attempt, queued_req.attempts) {
            (Some(last_attempt), attempts) if attempts > 0 => {
                debug!("last_attempt: {:?}", last_attempt);
                debug!("attempts: {}", attempts);
                let time_since_last_attempt = chrono::Utc::now() - last_attempt;
                debug!("time_since_last_attempt: {:?}", time_since_last_attempt);
                debug!("config_clone.replication.retry_interval: {}", config_clone.replication.retry_interval);
                debug!("config_clone.replication.max_replication_attempts: {}", config_clone.replication.max_replication_attempts);

                let retry_condition = time_since_last_attempt >= chrono::Duration::seconds(config_clone.replication.retry_interval) &&
                    attempts < config_clone.replication.max_replication_attempts;
                debug!("retry_condition: {}", retry_condition);
                retry_condition
            },
            (None, _) => true,
            _ => {
                debug!("should_retry defaulting to false");
                false
            },
        };

        debug!("should_retry: {}", should_retry);
        if should_retry {
            debug!("Attempting replication for request index: {}", index);

            match try_replicate_to_nodes(config_clone, &queued_req.request).await {
                Ok(_) => {
                    debug!("Replication successful for request index: {}", index);
                    processed_requests.push(index);
                },
                Err(ReplicationError::ReplicationFailure(failed_nodes_str)) => {
                    error!("Replication failed: {}", failed_nodes_str);
                    queued_req.failed_nodes = failed_nodes_str
                        .replace("Failed nodes: ", "")
                        .split(", ")
                        .map(|s| s.to_string())
                        .collect();

                    queued_req.attempts += 1;
                    queued_req.last_attempt = Some(chrono::Utc::now());

                    if queued_req.first_failed_attempt.is_none() {
                        queued_req.first_failed_attempt = Some(chrono::Utc::now());
                    }
                },
                Err(e) => {
                    error!("An unexpected error occurred during replication: {:?}", e);
                    queued_req.attempts += 1;
                    queued_req.last_attempt = Some(chrono::Utc::now());
                },
            }
        } else {
            debug!("Skipping replication for request index: {}, should_retry: {}", index, should_retry);
        }
    }

    for &index in processed_requests.iter().rev() {
        queue.queue.remove(index);
        debug!("Removed processed request at index: {}", index);
    }
    debug!("Processed requests removed from queue. Queue length now: {}", queue.queue.len());
    debug!("Passive replication loop iteration finished");
    Ok(())
}


async fn inner_replication_loop_Working(
    app_state_clone: &Arc<Mutex<AppState>>,
    config_clone: &DatabaseConfig,
) -> Result<(), anyhow::Error> {
    let retry_interval = config_clone.replication.retry_interval;

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        thread::sleep(Duration::from_secs(retry_interval as u64));
        tx.send(()).unwrap();
    });

    println!("DIAGNOSTIC: Before thread sleep");
    tracing::info!("DIAGNOSTIC: Before thread sleep");

    rx.recv().unwrap(); // Wait for the timer to fire

    println!("DIAGNOSTIC: After thread sleep");
    tracing::info!("DIAGNOSTIC: After thread sleep");

    Ok(())
}

async fn inner_replication_loop4(
    app_state_clone: &Arc<Mutex<AppState>>,
    config_clone: &DatabaseConfig,
) -> Result<(), anyhow::Error> {
    let retry_interval = config_clone.replication.retry_interval;
    println!("DIAGNOSTIC: Retry interval: {}", retry_interval);
    tracing::info!("DIAGNOSTIC: Retry interval: {}", retry_interval);

    // Use Actix runtime's sleep and move ownership
    let sleep_future = async move {
        println!("DIAGNOSTIC: Before sleep");
        tracing::info!("DIAGNOSTIC: Before sleep");
        // Application is breaking on this line:
        rt::time::sleep(Duration::new(1,0)).await;

        println!("DIAGNOSTIC: After sleep");
        tracing::info!("DIAGNOSTIC: After sleep");
    };

    // Use a timeout with Actix runtime
    // Print out of this issue:
    //2025-03-31T08:05:09.140221Z TRACE actix_server::signals: setting up OS signal listener
    // DIAGNOSTIC: Retry interval: 1
    // 2025-03-31T08:05:09.140819Z  INFO Final_Year_Project_Database_Rust::replication::passive_replication: DIAGNOSTIC: Retry interval: 1
    // DIAGNOSTIC: Before sleep
    // 2025-03-31T08:05:09.141353Z  INFO Final_Year_Project_Database_Rust::replication::passive_replication: DIAGNOSTIC: Before sleep
    // Never timedout, never succeeded.
    match rt::time::timeout(
        Duration::from_secs((retry_interval + 5) as u64),
        sleep_future
    ).await {
        Ok(_) => {
            println!("DIAGNOSTIC: Sleep completed successfully");
            tracing::info!("DIAGNOSTIC: Sleep completed successfully");
        }
        Err(e) => {
            println!("DIAGNOSTIC: Sleep timed out or error: {:?}", e);
            tracing::warn!("DIAGNOSTIC: Sleep timed out or error: {:?}", e);
            return Err(anyhow::anyhow!("Sleep timed out or error: {:?}", e)); // Propagate the error
        }
    }

    println!("DIAGNOSTIC: End of function");
    tracing::info!("DIAGNOSTIC: End of function");

    Ok(())
}

async fn inner_replication_loop3(
    app_state_clone: &Arc<Mutex<AppState>>,
    config_clone: &DatabaseConfig,
) -> Result<(), anyhow::Error> {
    let retry_interval = config_clone.replication.retry_interval;
    println!("DIAGNOSTIC: Retry interval: {}", retry_interval);
    tracing::info!("DIAGNOSTIC: Retry interval: {}", retry_interval);

    // Use Actix runtime's sleep and move ownership
    let sleep_future = async move {
        println!("DIAGNOSTIC: Before sleep");
        tracing::info!("DIAGNOSTIC: Before sleep");
        // Application is breaking on this line:
        rt::time::sleep(Duration::from_secs(retry_interval as u64)).await;

        println!("DIAGNOSTIC: After sleep");
        tracing::info!("DIAGNOSTIC: After sleep");
    };

    // Use a timeout with Actix runtime
    match rt::time::timeout(
        Duration::from_secs((retry_interval + 5) as u64),
        sleep_future
    ).await {
        Ok(_) => {
            println!("DIAGNOSTIC: Sleep completed successfully");
            tracing::info!("DIAGNOSTIC: Sleep completed successfully");
        }
        Err(_) => {
            println!("DIAGNOSTIC: Sleep timed out");
            tracing::warn!("DIAGNOSTIC: Sleep timed out");
        }
    }

    println!("DIAGNOSTIC: End of function");
    tracing::info!("DIAGNOSTIC: End of function");

    Ok(())
}



async fn inner_replication_loop2(
    app_state_clone: &Arc<Mutex<AppState>>,
    config_clone: &DatabaseConfig,
) -> Result<(), anyhow::Error> {
    let retry_interval = config_clone.replication.retry_interval;
    debug!("Passive replication timer set for {} seconds", retry_interval);

    actix_web::rt::time::sleep(Duration::from_secs(retry_interval as u64)).await;

    debug!("Passive replication loop started");

    let mut processed_requests = Vec::new();

    // Use anyhow for more flexible error handling
    let app_state_guard = app_state_clone.lock().map_err(|e| {
        error!("Failed to acquire lock on app_state: {}", e);
        anyhow::anyhow!("Failed to acquire app_state lock: {}", e)
    })?;

    let queue_lock = app_state_guard.passive_replication_queue.lock().map_err(|e| {
        error!("Failed to acquire lock on passive_replication_queue: {}", e);
        anyhow::anyhow!("Failed to acquire passive_replication_queue lock: {}", e)
    })?;

    let mut queue = queue_lock;
    debug!("Queue Length: {}", queue.queue.len());

    for (index, queued_req) in queue.queue.iter_mut().enumerate() {
        debug!("Processing Request Index: {}, Attempts: {}", index, queued_req.attempts);
        let should_retry = match (queued_req.last_attempt, queued_req.attempts) {
            (Some(last_attempt), attempts) if attempts > 0 => {
                debug!("last_attempt: {:?}", last_attempt);
                debug!("attempts: {}", attempts);
                let time_since_last_attempt = chrono::Utc::now() - last_attempt;
                debug!("time_since_last_attempt: {:?}", time_since_last_attempt);
                debug!("config_clone.replication.retry_interval: {}", config_clone.replication.retry_interval);
                debug!("config_clone.replication.max_replication_attempts: {}", config_clone.replication.max_replication_attempts);

                let retry_condition = time_since_last_attempt >= chrono::Duration::seconds(config_clone.replication.retry_interval) &&
                    attempts < config_clone.replication.max_replication_attempts;
                debug!("retry_condition: {}", retry_condition);
                retry_condition
            },
            (None, _) => true,
            _ => {
                debug!("should_retry defaulting to false");
                false
            },
        };

        debug!("should_retry: {}", should_retry);
        if should_retry {
            debug!("Attempting replication for request index: {}", index);

            match try_replicate_to_nodes(config_clone, &queued_req.request).await {
                Ok(_) => {
                    debug!("Replication successful for request index: {}", index);
                    processed_requests.push(index);
                },
                Err(ReplicationError::ReplicationFailure(failed_nodes_str)) => {
                    error!("Replication failed: {}", failed_nodes_str);
                    queued_req.failed_nodes = failed_nodes_str
                        .replace("Failed nodes: ", "")
                        .split(", ")
                        .map(|s| s.to_string())
                        .collect();

                    queued_req.attempts += 1;
                    queued_req.last_attempt = Some(chrono::Utc::now());

                    if queued_req.first_failed_attempt.is_none() {
                        queued_req.first_failed_attempt = Some(chrono::Utc::now());
                    }
                },
                Err(e) => {
                    error!("An unexpected error occurred during replication: {:?}", e);
                    queued_req.attempts += 1;
                    queued_req.last_attempt = Some(chrono::Utc::now());
                },
            }
        } else {
            debug!("Skipping replication for request index: {}, should_retry: {}", index, should_retry);
        }
    }

    for &index in processed_requests.iter().rev() {
        queue.queue.remove(index);
        debug!("Removed processed request at index: {}", index);
    }
    debug!("Processed requests removed from queue. Queue length now: {}", queue.queue.len());

    debug!("Passive replication loop iteration finished");

    Ok(())
}


// Helper function to attempt replication to all nodes
async fn try_replicate_to_nodes(
    config: &DatabaseConfig,
    request: &ReplicationRequest,
) -> Result<(), ReplicationError> {
    info!("Attempting to replicate to nodes");
    let nodes = load_nodes(config)
        .map_err(|e| ReplicationError::ConfigLoadError(e))?;

    let client = awc::Client::default();

    let mut failed_nodes: Vec<String> = Vec::new();

    for node in nodes.nodes.iter().filter(|n|
        request.entries.iter().all(|entry| n.should_replicate(&entry.table_name))
    ) {
        info!("Try Passive Replicating to node: {}", node.name);
        match replicate_to_single_node(&client, node, request).await { // Pass the client here
            Ok(true) => {continue}, // Replication successful for this node
            Ok(false) | Err(_) => {
                failed_nodes.push(node.name.clone());
                return Err(ReplicationError::ReplicationFailure(
                    format!("Failed nodes: {}", failed_nodes.join(", "))
                ));
            }
        }
    }

    if failed_nodes.is_empty() {
        Ok(())
    } else {
        // This should ideally not be reached due to the early return, but it's included for safety.
        Err(ReplicationError::ReplicationFailure(
            format!("Failed nodes: {}", failed_nodes.join(", "))
        ))
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
