// replication_sync.rs

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use actix_web::{post, web, HttpResponse, Responder};
use serde::Deserialize;
use serde_json::Value;
use tracing::log::{debug, error, info, trace, warn};
use crate::{replication, AppState};
use crate::change_logging::change_logging::ChangeLogEntry;
use crate::replication::active_replication::{replicate_to_single_node, ReplicationRequest};
use crate::replication::replication_nodes::{load_nodes, ReplicationMode, ReplicationNode};
use crate::tables::table::get_table_data;

#[derive(Deserialize, Debug)]
struct TableResponse(Vec<Vec<serde_json::Value>>);

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

async fn perform_replication_sync(app_state: web::Data<AppState>) -> Result<(), Box<dyn std::error::Error>> {
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
    for entry in std::fs::read_dir(&config.db_dir.join(&config.table_dir))? {
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

    match &node.replication_mode {
        ReplicationMode::All => {
            info!("Replication Mode ALL is enabled, fetching all tables");

            // Fetch the list of tables from the node's /api/tables endpoint
            let tables_url = format!("{}/api/tables", node.node_url);
            info!("Fetching table list from: {}", tables_url);

            let mut table_list_response = client.get(tables_url)
                .insert_header(("User-Agent", "Actix-web"))
                .send()
                .await?;

            if table_list_response.status().is_success() {
                let table_list = table_list_response.json::<TableListResponse>().await?;
                info!("Fetched table list: {:?}", table_list.0);

                // loop tables and get the data
                for table_name in table_list.0 {
                    let table_url = format!("{}/api/tables/{}", node.node_url, table_name);
                    info!("Fetching table data from: {}", table_url);

                    let mut response = client.get(table_url)
                        .insert_header(("User-Agent", "Actix-web"))
                        .send()
                        .await?;

                    if response.status().is_success() {
                        let body = response.json::<TableResponse>().await?;

                        // Convert the TableResponse (Vec<Vec<String>>) to Vec<Value>
                        let table_values: Vec<Value> = body.0.into_iter()
                            .map(|row| serde_json::Value::Array(row))
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
            info!("Comparing specific tables: {}", specific_tables.join(", "));
            let client = awc::Client::default();

            for table_name in specific_tables {
                let table_url = format!("{}/api/tables/{}", node.node_url, table_name);
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
                                .map(|row| serde_json::Value::Array(row))
                                .collect();

                            table_data.insert(table_name.clone(), table_values);
                        } else if response.status().as_u16() == 404 {
                            warn!("Table {} not found on node {}", table_name, node.name);
                            // Fetch logs and compare
                            let logs1 = fetch_logs_from_node(node, config, table_name).await?; //get logs from other node and push to current to resolve
                            let logs2 = fetch_current_logs(config, table_name).await?;  // get the current logs, so we can compare and determine what needs to be pushed.

                            // Identify the differing ChangeLogEntry items
                            let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

                            // Replicate the changes to node2.  Add logic here for direction.
                            if should_replicate_to_node(logs1.clone(),logs2.clone()){
                                info!("Pushing changes to {}", node.name);
                                replicate_changes_to_node(Arc::from(AppState::global_state().unwrap().clone()), node, diff_entries).await?;
                            }
                            else{
                                info!("Requesting changes from {}", node.name);
                                replicate_changes_from_node(Arc::from(AppState::global_state().unwrap().clone()), node, diff_entries).await?;
                            }
                        }
                        else {
                            error!("Failed to fetch table data from {}: {}", node.node_url, response.status());
                            // Consider whether to continue or return an error
                        }
                    }
                    Err(e) => {
                        error!("Failed to send request to {}: {}", node.node_url, e);
                        // Consider whether to continue or return an error
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
    // Iterate through the tables present in current_node_data
    for (table_name, data1) in current_node_data.iter() {
        if let Some(data2) = other_node_data.get(table_name) {
            info!("Comparing table {} on current node and node {}", table_name, other_node.name);

            // Compare the data
            if data1 != data2 {
                warn!("Data mismatch found in table {} between current node and node {}", table_name, other_node.name);

                // Fetch logs and compare
                let logs1 = fetch_current_logs(&app_state.config, table_name).await?;  // get the current logs, so we can compare and determine what needs to be pushed.
                let logs2 = match fetch_logs_from_node(other_node, &app_state.config, table_name).await {
                    Ok(logs) => logs,
                    Err(_) => Vec::new(), // Assume 0 logs if fetching fails
                };

                // Identify the differing ChangeLogEntry items
                let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

                // Replicate the changes to node2.  Add logic here for direction.
                if should_replicate_to_node(logs1.clone(),logs2.clone()){
                    info!("Pushing changes to {}", other_node.name);
                    replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
                }
                else{
                    info!("Requesting changes from {}", other_node.name);
                    replicate_changes_from_node(app_state.clone(), other_node, diff_entries).await?;
                }

            } else {
                info!("No data mismatch found in table {} between current node and node {}", table_name, other_node.name);
            }
        } else {
            warn!("Table {} not found on node {}", table_name, other_node.name);
            // Fetch logs and compare
            let logs1 = fetch_current_logs(&app_state.config, table_name).await?;  // get the current logs, so we can compare and determine what needs to be pushed.
            let logs2 = fetch_logs_from_node(other_node, &app_state.config, table_name).await.unwrap_or_else(|_| Vec::new());

            // Identify the differing ChangeLogEntry items
            let diff_entries = compare_logs(logs1.clone(), logs2.clone(), table_name);

            // Replicate the changes to node2.  Add logic here for direction.
            if should_replicate_to_node(logs1.clone(),logs2.clone()){
                info!("Pushing changes to {}", other_node.name);
                replicate_changes_to_node(app_state.clone(), other_node, diff_entries).await?;
            }
            else{
                info!("Requesting changes from {}", other_node.name);
                replicate_changes_from_node(app_state.clone(), other_node, diff_entries).await?;
            }
        }
    }
    Ok(())
}




fn should_replicate_to_node(logs1: Vec<ChangeLogEntry>, logs2: Vec<ChangeLogEntry>) -> bool{
    logs1.len() > logs2.len()
}

async fn replicate_changes_from_node(
    app_state: Arc<AppState>,
    node: &ReplicationNode,
    diff_entries: Vec<ChangeLogEntry>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Requesting {} changes from node {}", diff_entries.len(), node.name);

    // Create a ReplicationRequest
    let replication_request = ReplicationRequest {
        schema: app_state.schema.lock().unwrap().clone(),
        entries: diff_entries,
        target_node: node.clone(), // Assuming ReplicationNode is Clone
    };

    // Use the replicate_to_single_node function
    let client = awc::Client::default();
    match replicate_to_single_node(&client, node, &replication_request).await {
        Ok(true) => {
            info!("Successfully replicated changes from node {}", node.name);
            Ok(())
        }
        Ok(false) => {
            error!("Failed to replicate changes from node {}", node.name);
            Err("Failed to replicate changes from node".into()) // Or a more specific error
        }
        Err(e) => {
            error!("Error during replication from node {}: {}", node.name, e);
            Err(e)
        }
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


async fn fetch_logs_from_node(node: &ReplicationNode, config: &crate::DatabaseConfig, table_name: &str) -> Result<Vec<ChangeLogEntry>, Box<dyn std::error::Error>> {
    let client = awc::Client::default();
    let logs_url = format!("{}/api/logs/{}", node.node_url, table_name);
    info!("Fetching logs from: {}", logs_url);

    let response = client.get(logs_url)
        .insert_header(("User-Agent", "Actix-web"))
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
    mut diff_entries: Vec<ChangeLogEntry>,
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


pub fn configure_sync_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_replication_sync);
}
