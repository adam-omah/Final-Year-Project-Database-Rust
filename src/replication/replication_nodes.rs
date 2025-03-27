use std::{fs, io};
use std::fs::File;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use actix_web::{get, post, web, HttpResponse, Responder};
use actix_web::web::Data;
use awc::Client;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tracing::log::{debug, error, info};
use uuid::Uuid;
use crate::AppState;
use crate::config::database_config::DatabaseConfig;
use crate::replication::active_replication::ReplicationRequest;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ReplicationMode {
    All,
    Specific(Vec<String>)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationNode {
    pub name: String,
    pub node_url: String,
    pub description: String,
    pub replication_mode: ReplicationMode,
    pub shared_secret: String,
}

impl ReplicationNode {
    pub(crate) fn resolve_node_url(&self) -> Result<Vec<SocketAddr>, std::io::Error> {
        debug!("Attempting to resolve socket addresses {}", self.node_url);

        // Parse the URL to extract just the host and port
        let url_str = &*self.node_url;
        let socket_addr = if url_str.starts_with("http://") || url_str.starts_with("https://") {
            // Extract host:port from URL
            let without_scheme = url_str.split("://").nth(1).unwrap_or(url_str);
            // Remove path if present
            let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
            host_port
        } else {
            // Assume already in host:port format
            url_str
        };

        match socket_addr.to_socket_addrs() {
            Ok(iter) => {
                let addresses: Vec<SocketAddr> = iter.collect();
                debug!("Resolved socket addresses count {}", addresses.len());

                if addresses.is_empty() {
                    error!("No socket addresses could be resolved {}", self.node_url);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("No addresses found for {}", self.node_url)
                    ))
                } else {
                    Ok(addresses)
                }
            },
            Err(e) => {
                error!("Failed to resolve socket addresses {} {}", self.node_url, e);
                Err(e)
            }
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct CrossNodeRegistrationRequest {
    source_node: ReplicationNode,
    proposed_shared_secret: String,
}

#[derive(Serialize, Deserialize)]
pub struct CrossNodeRegistrationResponse {
    target_node: ReplicationNode,
    accepted_shared_secret: String,
}


impl ReplicationNode {
    pub fn should_replicate(&self, table_name: &str) -> bool {
        match self.replication_mode {
            ReplicationMode::All => true,
            ReplicationMode::Specific(ref tables) => tables.iter().any(|t| t == table_name),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodesConfig {
    pub nodes: Vec<ReplicationNode>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateNodeReplicationRequest {
    pub node_url: String,
    pub replication_mode: ReplicationMode,
    pub shared_secret: String,
}

#[derive(Serialize, Deserialize)]
pub struct NodeRegistrationRequest {
    pub node: ReplicationNode,
    pub shared_secret: String, // For basic authentication
}

#[derive(Serialize, Deserialize)]
struct LocalNodeConfig {
    database_name: String,
    node_url: String,
    node_port: u16,
}


fn generate_shared_secret() -> String {
    Uuid::new_v4().to_string()
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

// Node Registration Endpoint
#[post("/api/register-node")]
pub async fn register_node(
    req: web::Json<NodeRegistrationRequest>,
    app_state: web::Data<AppState>
) -> impl Responder {
    let config = app_state.config.clone();
    let mut nodes_config = match load_nodes(&config) {
        Ok(config) => config,
        Err(_) => NodesConfig::default(),
    };

    // Additional validation could be added here
    let new_node = ReplicationNode {
        name: req.node.name.clone(),            // Use the incoming node's name
        node_url: req.node.node_url.clone(),    // Use the incoming node's URL
        description: req.node.description.clone(),
        replication_mode: req.node.replication_mode.clone(),
        shared_secret: req.shared_secret.clone(),
    };

    nodes_config.nodes.push(new_node);

    match save_nodes(&nodes_config, &config) {
        Ok(_) => HttpResponse::Ok().json("Node registered successfully"),
        Err(e) => HttpResponse::InternalServerError().json(format!("Failed to save nodes: {}", e)),
    }
}


// Update Node Replication Mode Endpoint
#[post("/api/update-node-replication")]
pub async fn update_node_replication(
    req: web::Json<UpdateNodeReplicationRequest>,
    app_state: web::Data<AppState>
) -> impl Responder {
    let config = app_state.config.clone();

    // Load existing nodes configuration
    let mut nodes_config = match load_nodes(&config) {
        Ok(config) => config,
        Err(_) => return HttpResponse::InternalServerError().json("Failed to load nodes"),
    };

    // Use a closure to create a separate mutable scope
    let result = (|| {
        // Find the node by URL
        let node = match nodes_config.nodes.iter_mut().find(|node| node.node_url == req.node_url) {
            Some(node) => node,
            None => return Err(HttpResponse::NotFound().json("Node not found")),
        };

        // Verify shared secret
        if node.shared_secret != req.shared_secret {
            return Err(HttpResponse::Unauthorized().json("Invalid shared secret"));
        }

        // Update replication mode
        node.replication_mode = req.replication_mode.clone();

        // Create a response node before saving
        let response_node = ReplicationNode {
            name: node.name.clone(),
            node_url: node.node_url.clone(),
            description: node.description.clone(),
            replication_mode: node.replication_mode.clone(),
            shared_secret: node.shared_secret.clone(),
        };

        Ok(response_node)
    })();

    // Save and handle the result outside the closure
    match result {
        Ok(response_node) => {
            match save_nodes(&nodes_config, &config) {
                Ok(_) => HttpResponse::Ok().json(response_node),
                Err(_) => HttpResponse::InternalServerError().json("Failed to save node configuration"),
            }
        },
        Err(response) => response,
    }
}



#[get("/api/load-nodes")]
pub async fn load_nodes_endpoint(
    app_state: web::Data<AppState>
) -> impl Responder {
    let config = app_state.config.clone();

    // Explicitly create logs directory if it doesn't exist
    std::fs::create_dir_all(&config.log_dir)
        .unwrap_or_else(|_| eprintln!("Failed to create log directory"));

    // Create nodes file if it doesn't exist
    let nodes_file_path = config.log_dir.join(&config.repl_node_file);
    if !nodes_file_path.exists() {
        match File::create(&nodes_file_path) {
            Ok(_) => {
                // Initialize with an empty NodesConfig if file doesn't exist
                let initial_nodes_config = NodesConfig { nodes: Vec::new() };
                if let Err(e) = serde_json::to_writer_pretty(
                    File::create(&nodes_file_path).unwrap(),
                    &initial_nodes_config
                ) {
                    return HttpResponse::InternalServerError()
                        .json(format!("Failed to initialize nodes file: {}", e));
                }
            }
            Err(e) => {
                return HttpResponse::InternalServerError()
                    .json(format!("Failed to create nodes file: {}", e));
            }
        }
    }

    match load_nodes(&config) {
        Ok(nodes_config) => HttpResponse::Ok().json(nodes_config),
        Err(e) => {
            HttpResponse::InternalServerError()
                .json(format!("Failed to load nodes: {}", e))
        }
    }
}

#[post("/api/cross-node-register")]
pub async fn cross_node_register(
    req: web::Json<CrossNodeRegistrationRequest>,
    app_state: web::Data<AppState>
) -> impl Responder {
    let config = app_state.config.clone();

    // 1. Validate the incoming node
    if req.source_node.node_url.is_empty() {
        return HttpResponse::BadRequest().json("Invalid source node");
    }

    // 2. Attempt to register with the target node
    match register_with_target_node(&req.source_node,app_state).await {
        Ok(target_node_response) => {
            // 3. Save local node configuration
            let mut nodes_config = match load_nodes(&config) {
                Ok(config) => config,
                Err(_) => NodesConfig::default(),
            };

            // 4. Add or update the node in local configuration
            update_local_node_configuration(
                &mut nodes_config,
                &req.source_node,
                &target_node_response.accepted_shared_secret
            );

            // 5. Save updated configuration
            match save_nodes(&nodes_config, &config) {
                Ok(_) => HttpResponse::Ok().json(target_node_response),
                Err(_) => HttpResponse::InternalServerError().json("Failed to save node configuration"),
            }
        },
        Err(error) => {
            HttpResponse::InternalServerError().json(error.to_string())
        }
    }
}

async fn register_with_target_node(source_node: &ReplicationNode, app_state: Data<AppState>) -> Result<CrossNodeRegistrationResponse, String> {
    // Construct full URL for the target node's registration endpoint
    let addrs = source_node.resolve_node_url().map_err(|e| e.to_string())?;
    let target_addr = addrs.first().ok_or("Could not resolve any addresses")?;
    let target_url = if source_node.node_url.starts_with("https") {
        format!("https://{}/api/register-node", target_addr)
    } else {
        format!("http://{}/api/register-node", target_addr)
    };

    // Create an async client
    let client = Client::default();

    // Prepare local node details for registration
    let local_config = match load_local_node_config(&app_state.config.clone()) {
        Ok(config) => config,
        Err(_) => return Err("Failed to load local node configuration".to_string()),
    };

    // Generate a shared secret for authentication
    let local_shared_secret = generate_shared_secret();

    let request_body = NodeRegistrationRequest {
        node: ReplicationNode {
            name: source_node.name.clone(),
            node_url: local_config.node_url,
            description: source_node.description.clone(),
            // Important: Use the source node's proposed replication mode
            replication_mode: source_node.replication_mode.clone(),
            shared_secret: local_shared_secret.clone(),
        },
        shared_secret: local_shared_secret.clone(),
    };

    // Send HTTP request to target node's registration endpoint
    let mut response = client.post(&target_url)
        .send_json(&request_body)
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    // Check response status
    if !response.status().is_success() {
        return Err(format!("Target node registration failed: {}", response.status()));
    }

    // If successful, construct a CrossNodeRegistrationResponse
    // This might need to be adjusted based on the actual response from the target node
    Ok(CrossNodeRegistrationResponse {
        target_node: request_body.node,
        accepted_shared_secret: local_shared_secret,
    })
}

fn load_local_node_config(config: &DatabaseConfig) -> Result<LocalNodeConfig, String> {
    let node_url = if config.node_port != 0 {
        format!("{}:{}", config.node_url, config.node_port)
    } else {
        config.node_url.clone()
    };

    Ok(LocalNodeConfig {
        database_name: config.database_name.clone(),
        node_url,
        node_port: config.node_port,
    })
}




fn update_local_node_configuration(
    nodes_config: &mut NodesConfig,
    source_node: &ReplicationNode,
    shared_secret: &str
) {
    // Check if node already exists
    if let Some(existing_node) = nodes_config.nodes.iter_mut()
        .find(|node| node.node_url == source_node.node_url) {
        // Update existing node
        existing_node.name = source_node.name.clone();
        existing_node.description = source_node.description.clone();
        existing_node.replication_mode = source_node.replication_mode.clone();
        existing_node.shared_secret = shared_secret.to_string();
    } else {
        // Add new node
        let new_node = ReplicationNode {
            name: source_node.name.clone(),
            node_url: source_node.node_url.clone(),
            description: source_node.description.clone(),
            replication_mode: source_node.replication_mode.clone(),
            shared_secret: shared_secret.to_string(),
        };
        nodes_config.nodes.push(new_node);
    }
}

pub fn configure_node_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(register_node)
        .service(update_node_replication)
        .service(cross_node_register)
        .service(load_nodes_endpoint);
}

