// replication_sync.rs
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use actix_web::{post, web, HttpResponse, Responder};
use base64::Engine;
use base64::engine::general_purpose;
use serde::Deserialize;
use serde_json::Value;
use tracing::log::{debug, error, info, trace, warn};
use crate::{ AppState};
use crate::change_logging::change_logging::ChangeLogEntry;
use crate::replication::active_replication::{replicate_to_single_node, ReplicationRequest};
use crate::replication::passive_replication::ReplicationError;
use crate::replication::replication_nodes::{load_nodes, ReplicationMode, ReplicationNode};
use crate::tables::table::get_table_data;

#[derive(Deserialize, Debug)]
struct TableResponse(Vec<Vec<Value>>);

#[derive(Deserialize)]
struct TableListResponse(Vec<String>);


// Data structure to hold table data from a node
type TableData = HashMap<String, Vec<Value>>;


// API endpoint to trigger the replication sync check
#[post("/api/replication/sync")]
pub async fn trigger_replication_sync(app_state: web::Data<AppState>) -> impl Responder {
    info!("Replication sync check triggered manually");

    match perform_replication_sync(app_state).await {
        Ok(_) => HttpResponse::Ok().body("Replication sync check completed successfully"),
        Err(e) => {
            error!("Replication sync check failed: {}", e);
            HttpResponse::InternalServerError().body(format!("Replication sync check failed: {}", e))
        }
    }
}

pub async fn perform_replication_sync(app_state: web::Data<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let config = &app_state.config;

    // 1. Load Nodes Configuration
    info!("Loading nodes config");
    let nodes_config = load_nodes(config)?;
    debug!("Nodes config loaded: {:?}", nodes_config);

    if nodes_config.nodes.is_empty() {
        warn!("No nodes configured, skipping replication sync check.");
        return Ok(()); // Not an error, just nothing to do
    }

    // Iterate through all configured nodes
    for other_node in &nodes_config.nodes {
        info!("Starting replication sync check with node: {}", other_node.name);
        // 2. Fetch Data from Each Node
        info!("Fetching current table data");
        let current_node_data = fetch_current_table_data(config).await?; // Fetch data from the current instance
        debug!("Current node data fetched: {:?}", current_node_data);
        info!("Fetching table data from other node");
        let other_node_data = fetch_table_data(other_node, config).await?; // Fetch data from the other node
        debug!("Other node data fetched: {:?}", other_node_data);

        // 3. Compare Data and Logs, and potentially reverse direction
        compare_node_data_and_replicate(Arc::from(app_state.get_ref().clone()), other_node, &current_node_data, &other_node_data).await?;
    }

    Ok(())
}



//Fetches data from the current node ( the application instance where this code runs)
async fn fetch_current_table_data(config: &crate::DatabaseConfig) -> Result<TableData, Box<dyn std::error::Error>> {
    let mut table_data: TableData = HashMap::new();
    debug!("Fetching current table data with config: {:?}", config);

    info!("Reading table directory: {}", &config.db_dir.join(&config.table_dir).display());
    for entry in std::fs::read_dir(config.db_dir.join(&config.table_dir))? {
        let entry = entry?;
        let file_name = entry.file_name().into_string().unwrap();
        debug!("Processing file: {}", file_name);

        if file_name.ends_with("_initial") {
            let table_name = file_name.replace("_initial", "");
            debug!("Extracted table name: {}", table_name);

            // Use get_table_data function from table.rs
            let state = AppState::global_state()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Global application state not initialized"))?;
            let state_data = web::Data::new((*state).clone());

            match get_table_data(state_data, &table_name).await {
                Ok(rows) => {
                    // Convert the rows (Vec<Vec<String>>) to Vec<Value>
                    let mut table_values: Vec<Value> = Vec::new();
                    for row in rows.iter().skip(1) { // Skip column names row
                        let value = serde_json::to_value(row)?;
                        table_values.push(value);
                    }
                    table_data.insert(table_name.clone(), table_values);
                }
                Err(e) => {
                    error!("Failed to get table data for {}: {}", table_name, e);
                    // Consider whether to continue or return an error
                }
            }
        }
    }
    info!("Current table data fetched: {:?}", table_data);
    Ok(table_data)
}

async fn fetch_table_data(node: &ReplicationNode, config: &crate::DatabaseConfig) -> Result<TableData, Box<dyn std::error::Error>> {
    let mut table_data: TableData = HashMap::new();
    debug!("Fetching table data for node {} with config: {:?}", node.name, config);

    let client = awc::Client::default();
    // Add Basic Auth headers
    let credentials = format!("{}:{}", node.name, node.shared_secret); // Combine node_name and shared_secret
    let encoded_credentials = general_purpose::STANDARD.encode(credentials);
    info!("Encoded credentials: {}", encoded_credentials); // Log for debugging

    let auth_header_value = format!("Basic {}", encoded_credentials);
    info!("Authorization header value: {}", auth_header_value);
    // Format API address to correct address.
    let addrs = match node.resolve_node_url() {
        Ok(addrs) => addrs,
        Err(e) => {
            error!("Failed to resolve address for node {}: {}", node.name, e);
            return Err(e.into());
        }
    };
    let target_addr = addrs.first().ok_or("Could not resolve any addresses")?;

    match &node.replication_mode {
        ReplicationMode::All => {
            let tables_url = if node.node_url.starts_with("https") {
                format!("https://{}/api/tables", target_addr)
            } else {
                format!("http://{}/api/tables", target_addr)
            };
            info!("Replication Mode ALL is enabled, fetching all tables");
            info!("Fetching table list from: {}", tables_url);

            let mut table_list_response = client.get(tables_url)
                .insert_header(("User-Agent", "Actix-web"))
                .insert_header(("Authorization", auth_header_value))
                .send()
                .await?;

            if table_list_response.status().is_success() {
                let table_list = table_list_response.json::<TableListResponse>().await?;
                info!("Fetched table list: {:?}", table_list.0);

                // loop tables and get the data
                for table_name in table_list.0 {
                    let table_url = if node.node_url.starts_with("https") {
                        format!("https://{}/api/tables/{}", target_addr, table_name)
                    } else {
                        format!("http://{}/api/tables/{}", target_addr, table_name)
                    };
                    info!("Fetching table data from: {}", table_url);

                    let mut response = client.get(table_url)
                        .insert_header(("User-Agent", "Actix-web"))
                        .send()
                        .await?;

                    if response.status().is_success() {
                        let body = response.json::<TableResponse>().await?;

                        // Convert the TableResponse (Vec<Vec<String>>) to Vec<Value>
                        let table_values: Vec<Value> = body.0.into_iter()
                            .map(Value::Array)
                            .collect();

                        table_data.insert(table_name.clone(), table_values);
                    } else {
                        error!("Failed to fetch table data from {}: {}", node.node_url, response.status());
                        // Consider whether to continue or return an error
                    }
                }
            } else {
                error!("Failed to fetch table list from {}: {}", node.node_url, table_list_response.status());
                return Err(format!("Failed to fetch table list: {}", table_list_response.status()).into());
            }
        }
        ReplicationMode::Specific(specific_tables) => {
            info!("Fetching specific tables: {}", specific_tables.join(", "));
            let client = awc::Client::default();

            for table_name in specific_tables {
                let table_url = if node.node_url.starts_with("https") {
                    format!("https://{}/api/tables/{}", target_addr, table_name)
                } else {
                    format!("http://{}/api/tables/{}", target_addr, table_name)
                };
                info!("Fetching table data from: {}", table_url);

                let response = client.get(table_url)
                    .insert_header(("User-Agent", "Actix-web"))
                    .send()
                    .await;

                match response {
                    Ok(mut response) => {
                        if response.status().is_success() {
                            let body = response.json::<TableResponse>().await?;

                            // Convert the TableResponse (Vec<Vec<String>>) to Vec<Value>
                            let table_values: Vec<Value> = body.0.into_iter()
                                .map(Value::Array)
                                .collect();

                            table_data.insert(table_name.clone(), table_values);
                        } else {
                            info!("Failed to fetch table data from {}: {}", node.node_url, response.status());
                            // Consider whether to continue or return an error
                        }
                    }
                    Err(e) => {
                        info!("Failed to send request to {}: {}", node.node_url, e);
                        return Err(format!("Failed to fetch table list: {}", e).into());
                    }
                }
            }
        }
    }
    info!("Table data fetched: {:?}", table_data);
    Ok(table_data)
}




async fn compare_node_data_and_replicate(
    app_state: Arc<AppState>,
    other_node: &ReplicationNode,
    current_node_data: &TableData,
    other_node_data: &TableData,
) -> Result<(), Box<dyn std::error::Error>> {
    match &other_node.replication_mode {
        ReplicationMode::All => {
            // Determine which set of table names to use for iteration
            let table_names = if current_node_data.len() < other_node_data.len() {
                // If the current node has fewer tables, use the other node's table names
                other_node_data.keys().cloned().collect::<Vec<_>>()
            } else {
                // Otherwise, use the current node's table names
                current_node_data.keys().cloned().collect::<Vec<_>>()
            };

            // Iterate through the tables
            for table_name in table_names {
                let data1 = current_node_data.get(&table_name);
                let data2 = other_node_data.get(&table_name);

                match (data1, data2) {
                    (Some(data1), Some(data2)) => {
                        info!("Comparing table {} on current node and node {}", table_name, other_node.name);

                        // Compare the data
                        if data1 != data2 {
                            warn!("Data mismatch found in table {} between current node and node {}", table_name, other_node.name);

                            // Fetch logs and compare
                            let logs1 = fetch_current_logs(&app_state.config, &table_name).await?;
                            let logs2 = fetch_logs_from_node(other_node, &table_name).await.unwrap_or_else(|_| Vec::new());

                            // Identify the differing ChangeLogEntry items
                            let diff_entries = compare_logs(logs1.clone(), logs2.clone(), &table_name);

                            // Replicate the changes to node2.  Add logic here for direction.
                            if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                                info!("Pushing changes to {}", other_node.name);
                                replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                            } else {
                                info!("Requesting changes from {}", other_node.name);
                                replicate_changes_from_node(other_node).await?;
                                break;
                            }
                        } else {
                            info!("No data mismatch found in table {} between current node and node {}", table_name, other_node.name);
                        }
                    }
                    (Some(_), None) => {
                        warn!("Table {} not found on node {}", table_name, other_node.name);
                        // Table exists on current node but not on the other node
                        let logs1 = fetch_current_logs(&app_state.config, &table_name).await?;
                        let logs2 = Vec::new(); // No logs to fetch from the other node

                        // Identify the differing ChangeLogEntry items
                        let diff_entries = compare_logs(logs1.clone(), logs2.clone(), &table_name);

                        // Replicate the changes to node2.  Add logic here for direction.
                        if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                            info!("Pushing changes to {}", other_node.name);
                            replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                        } else {
                            info!("Requesting changes from {}", other_node.name);
                            replicate_changes_from_node(other_node).await?;
                            break;
                        }
                    }
                    (None, Some(_)) => {
                        warn!("Table {} not found on current node", table_name);
                        // Table exists on the other node but not on the current node
                        let logs1 = Vec::new(); // No logs to fetch from the current node
                        let logs2 = fetch_logs_from_node(other_node, &table_name).await.unwrap_or_else(|_| Vec::new());

                        // Identify the differing ChangeLogEntry items
                        let diff_entries = compare_logs(logs1.clone(), logs2.clone(), &table_name);

                        // Replicate the changes to node2.  Add logic here for direction.
                        if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                            info!("Pushing changes to {}", other_node.name);
                            replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                        } else {
                            info!("Requesting changes from {}", other_node.name);
                            replicate_changes_from_node(other_node).await?;
                            break;
                        }
                    }
                    (None, None) => {
                        info!("Table {} not found on either node", table_name);
                    }
                }
            }
        }
        ReplicationMode::Specific(specific_tables) => {
            // Iterate through the specific tables defined in the node's configuration
            for table_name in specific_tables {
                let data1 = current_node_data.get(table_name);
                let data2 = other_node_data.get(table_name);

                match (data1, data2) {
                    (Some(data1), Some(data2)) => {
                        info!("Comparing table {} on current node and node {}", table_name, other_node.name);

                        // Compare the data
                        if data1 != data2 {
                            warn!("Data mismatch found in table {} between current node and node {}", table_name, other_node.name);

                            // Fetch logs and compare
                            let logs1 = fetch_current_logs(&app_state.config, table_name).await?;
                            let logs2 = fetch_logs_from_node(other_node, table_name).await.unwrap_or_else(|_| Vec::new());

                            // Identify the differing ChangeLogEntry items
                            let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

                            // Replicate the changes to node2.  Add logic here for direction.
                            if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                                info!("Pushing changes to {}", other_node.name);
                                replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                            } else {
                                info!("Requesting changes from {}", other_node.name);
                                replicate_changes_from_node(other_node).await?;
                                break;
                            }
                        } else {
                            info!("No data mismatch found in table {} between current node and node {}", table_name, other_node.name);
                        }
                    }
                    (Some(_), None) => {
                        warn!("Table {} not found on node {}", table_name, other_node.name);
                        // Table exists on current node but not on the other node
                        let logs1 = fetch_current_logs(&app_state.config, table_name).await?;
                        let logs2 = Vec::new(); // No logs to fetch from the other node

                        // Identify the differing ChangeLogEntry items
                        let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

                        // Replicate the changes to node2.  Add logic here for direction.
                        if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                            info!("Pushing changes to {}", other_node.name);
                            replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                        } else {
                            info!("Requesting changes from {}", other_node.name);
                            replicate_changes_from_node(other_node).await?;
                        }
                    }
                    (None, Some(_)) => {
                        warn!("Table {} not found on current node", table_name);
                        // Table exists on the other node but not on the current node
                        let logs1 = Vec::new(); // No logs to fetch from the current node
                        let logs2 = fetch_logs_from_node(other_node, table_name).await.unwrap_or_else(|_| Vec::new());

                        // Identify the differing ChangeLogEntry items
                        let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

                        // Replicate the changes to node2.  Add logic here for direction.
                        if should_replicate_to_node(logs1.clone(), logs2.clone()) {
                            info!("Pushing changes to {}", other_node.name);
                            replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                        } else {
                            info!("Requesting changes from {}", other_node.name);
                            replicate_changes_from_node(other_node).await?;
                        }
                    }
                    (None, None) => {
                        info!("Table {} not found on either node", table_name);
                    }
                }
            }
        }
    }
    Ok(())
}




fn should_replicate_to_node(logs1: Vec<ChangeLogEntry>, logs2: Vec<ChangeLogEntry>) -> bool{
    logs1.len() > logs2.len()
}

async fn replicate_changes_from_node(
    node: &ReplicationNode,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Requesting sync from node {}", node.name);

    // Build the URL for the sync endpoint on the other node
    let addrs = match node.resolve_node_url() {
        Ok(addrs) => addrs,
        Err(e) => {
            error!("Failed to resolve address for node {}: {}", node.name, e);
            return Err(e.into());
        }
    };
    let target_addr = addrs.first().ok_or("Could not resolve any addresses")?;

    let sync_url = if node.node_url.starts_with("https") {
        format!("https://{}/api/replication/sync", target_addr)
    } else {
        format!("http://{}/api/replication/sync", target_addr)
    };

    info!("Triggering sync on node: {}", sync_url);

    // Create an awc client
    let client = awc::Client::default();

    // Add Authorization header with node credentials
    let credentials = format!("{}:{}", node.name, node.shared_secret);
    let encoded_credentials = general_purpose::STANDARD.encode(credentials);
    let auth_header_value = format!("Basic {}", encoded_credentials);


    // Send a POST request to the sync endpoint
    let response = client
        .post(sync_url)
        .insert_header(("User-Agent", "Actix-web"))
        .insert_header(("Authorization", auth_header_value))
        .send()
        .await?;

    if response.status().is_success() {
        info!("Successfully triggered sync on node {}", node.name);
        Ok(())
    } else {
        error!("Failed to trigger sync on node {}: {}", node.name, response.status());
        Err(format!("Failed to trigger sync: {}", response.status()).into())
    }
}




// Fetch the logs from current node (the application instance where this code runs)
async fn fetch_current_logs(config: &crate::DatabaseConfig, table_name: &str) -> Result<Vec<ChangeLogEntry>, Box<dyn std::error::Error>> {
    debug!("Fetching current logs with config: {:?}", config);
    let log_file_path = std::path::Path::new(&config.log_dir).join(&config.log_file);
    info!("Log file path: {}", log_file_path.display());
    let mut entries = Vec::new();

    if log_file_path.exists() {
        let file = File::open(log_file_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if let Ok(entry) = serde_json::from_str::<ChangeLogEntry>(&line) {
                if entry.table_name == table_name{
                    entries.push(entry);
                }

            }
        }
    }
    info!("Current logs fetched: {:?}", entries);
    Ok(entries)
}


async fn fetch_logs_from_node(node: &ReplicationNode, table_name: &str) -> Result<Vec<ChangeLogEntry>, Box<dyn std::error::Error>> {
    let client = awc::Client::default();
    // Format API address to correct address.
    let addrs = match node.resolve_node_url() {
        Ok(addrs) => addrs,
        Err(e) => {
            error!("Failed to resolve address for node {}: {}", node.name, e);
            return Err(e.into());
        }
    };
    let target_addr = addrs.first().ok_or("Could not resolve any addresses")?;
    let logs_url = if node.node_url.starts_with("https") {
        format!("https://{}/api/logs/{}", target_addr, table_name)
    } else {
        format!("http://{}/api/logs/{}", target_addr, table_name)
    };
    info!("Fetching logs from: {}", logs_url);

    let credentials = format!("{}:{}", node.name, node.shared_secret); // Combine node_name and shared_secret
    let encoded_credentials = general_purpose::STANDARD.encode(credentials); // Base64 encode
    let auth_header_value = format!("Basic {}", encoded_credentials);

    let response = client.get(logs_url)
        .insert_header(("User-Agent", "Actix-web"))
        .insert_header(("Authorization", auth_header_value))
        .send()
        .await;

    match response {
        Ok(mut response) => {
            if response.status().is_success() {
                let body = response.json::<Vec<ChangeLogEntry>>().await?;
                info!("Fetched logs from node {}: {:?}", node.name, body);
                Ok(body)
            } else {
                error!("Failed to fetch logs from {}: {}", node.node_url, response.status());
                Err(format!("Failed to fetch logs: {}", response.status()).into())
            }
        }
        Err(e) => {
            error!("Failed to send request to {}: {}", node.node_url, e);
            Err(format!("Failed to fetch logs from node {}: {}", node.node_url, e).into())
        }
    }
}



fn compare_logs(logs1: Vec<ChangeLogEntry>, logs2: Vec<ChangeLogEntry>, table_name: &str) -> Vec<ChangeLogEntry> {
    // Filter logs for the specified table name
    let logs1: Vec<ChangeLogEntry> = logs1.into_iter().filter(|log| log.table_name == table_name).collect();
    let logs2: Vec<ChangeLogEntry> = logs2.into_iter().filter(|log| log.table_name == table_name).collect();
    // Convert logs2 into a HashMap for efficient lookup
    let logs2_map: HashMap<_, _> = logs2.iter().map(|log| (log.change_id, log)).collect();

    // Identify differing ChangeLogEntry items
    let diff_entries: Vec<ChangeLogEntry> = logs1
        .into_iter()
        .filter(|log1| {
            logs2_map.get(&log1.change_id).map_or(true, |log2| *log1 != **log2 )
        })
        .collect();

    diff_entries
}

async fn replicate_changes_to_node(
    app_state: Arc<AppState>,
    node: &ReplicationNode,
    diff_entries: Vec<ChangeLogEntry>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Replicating {} changes to node {}", diff_entries.len(), node.name);
    trace!("Replicating these entries: {:?}", diff_entries); // Add trace log

    // Create a ReplicationRequest
    let replication_request = ReplicationRequest {
        schema: app_state.schema.lock().unwrap().clone(),
        entries: diff_entries.clone(),
        target_node: node.clone(), // Assuming ReplicationNode is Clone
    };

    trace!("Replication request {:#?}", replication_request); // Add trace log

    // Use the replicate_to_single_node function
    let client = awc::Client::default();
    match replicate_to_single_node(&client, node, &replication_request).await {
        Ok(true) => {
            info!("Successfully replicated changes to node {}", node.name);
            Ok(())
        }
        Ok(false) => {
            error!("Failed to replicate changes to node {}", node.name);
            Err("Failed to replicate changes to node".into()) // Or a more specific error
        }
        Err(e) => {
            error!("Error during replication to node {}: {}", node.name, e);
            Err(e)
        }
    }
}


pub async fn try_perform_replication_sync(app_state: web::Data<AppState>) -> Result<(), anyhow::Error> {
    let config = &app_state.config;

    // 1. Load Nodes Configuration
    info!("Loading nodes config");
    let nodes_config = load_nodes(config)?;
    debug!("Nodes config loaded: {:?}", nodes_config);

    if nodes_config.nodes.is_empty() {
        warn!("No nodes configured, skipping replication sync check.");
        return Ok(()); // Not an error, just nothing to do
    }

    // Create a request client
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create HTTP client: {}", e))?;

    // Create futures for sync checks on all nodes
    let sync_futures = nodes_config.nodes.iter().map(|node| {
        let client = client.clone();
        let node_url = node.node_url.clone();

        async move {
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
                format!("https://{}/api/replication/sync", target_addr)
            } else {
                format!("http://{}/api/replication/sync", target_addr)
            };

            debug!("Attempting sync check on node: {}", url);

            match client.post(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        info!("Sync check successful for node: {}", url);
                        Ok(())
                    } else {
                        error!("Sync check failed for node: {} - Status: {}", url, response.status());
                        Err(ReplicationError::ReplicationFailure(format!("Sync check failed for {}: {}", url, response.status())))
                    }
                }
                Err(e) => {
                    error!("Network error during sync check on {}: {}", node_url, e);
                    Err(ReplicationError::ReplicationFailure(format!("Network error on {}: {}", node_url, e)))
                }
            }
        }
    }).collect::<Vec<_>>();

    // Wait for all sync checks to complete
    let results = futures::future::join_all(sync_futures).await;

    // Collect and handle any errors
    let failed_nodes: Vec<_> = results
        .into_iter()
        .filter_map(|result| result.err())
        .collect();

    if failed_nodes.is_empty() {
        info!("Sync check completed successfully on all nodes");
        Ok(())
    } else {
        // Construct an error message with details about failed nodes
        let error_details = failed_nodes
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");

        Err(anyhow::anyhow!("Replication sync failed: {}", error_details))
    }
}


pub fn configure_sync_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_replication_sync);
}
