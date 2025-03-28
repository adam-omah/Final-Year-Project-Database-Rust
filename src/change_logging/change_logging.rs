use std::fmt;
use std::fs::{OpenOptions, File};
use std::io::{Write, Result as IoResult};
use std::path::{Path, PathBuf};
use actix_web::{get, web, HttpResponse, Responder};
use serde_json;
use tracing::log::{error, info};
use uuid::Uuid;
use crate::AppState;

/// Enum to represent different types of database operations
#[derive(serde::Serialize,serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ChangeType {
    Create,
    Insert,
    Update,
    Delete,
    Drop,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChangeType::Insert => write!(f, "Insert"),
            ChangeType::Update => write!(f, "Update"),
            ChangeType::Delete => write!(f, "Delete"),
            ChangeType::Create => write!(f, "Create"),
            ChangeType::Drop => write!(f, "Drop"),
        }
    }
}


/// Struct to represent a change log entry
#[derive(serde::Serialize,serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogEntry {
    pub change_id: Uuid,
    pub change_type: ChangeType,
    pub table_name: String,
    pub data: serde_json::Value,
    pub user: Option<String>,
    pub origin_db: Option<String>,
}

/// Change Logger struct to manage logging operations
#[derive(Clone)]
pub struct ChangeLogger {
    log_directory: PathBuf,
    log_file: String,
}

impl ChangeLogger {
    /// Create a new ChangeLogger instance
    pub fn new(log_directory: impl AsRef<Path>, log_file: String) -> Self {
        // Ensure the log directory exists
        std::fs::create_dir_all(&log_directory).expect("Could not create log directory");

        ChangeLogger {
            log_directory: log_directory.as_ref().to_path_buf(),
            log_file: log_file.clone(),
        }
    }

    /// Log a change to the log file
    pub fn log_change(
        &self,
        change_id: Option<Uuid>,
        change_type: ChangeType,
        table_name: String,
        data: serde_json::Value,
        user: Option<String>,
        origin_db: Option<String>,
    ) -> IoResult<ChangeLogEntry> {
        let log_entry = ChangeLogEntry {
            change_id: change_id.unwrap_or_else(Uuid::new_v4),
            change_type,
            table_name,
            data,
            user,
            origin_db,
        };

        let log_entry_json = serde_json::to_string(&log_entry)
            .expect("Failed to serialize log entry");

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_directory.join(&self.log_file))?;

        writeln!(file, "{}", log_entry_json)?;

        Ok(log_entry) // <- Return the ChangeLogEntry explicitly
    }

    /// Read recent log entries
    pub fn read_recent_logs(&self, limit: usize) -> IoResult<Vec<ChangeLogEntry>> {
        use std::io::{BufRead, BufReader};

        let file = File::open(&self.log_directory.join(&self.log_file))?;
        let reader = BufReader::new(file);

        let mut all_logs: Vec<ChangeLogEntry> = Vec::new();

        for line in reader.lines() {
            if let Ok(line) = line {
                if let Ok(log_entry) = serde_json::from_str(&line) {
                    all_logs.push(log_entry);
                }
            }
        }

        // Reverse and truncate
        all_logs.reverse();
        all_logs.truncate(limit);

        Ok(all_logs)
    }

    pub fn read_logs_for_table(&self, table_name: &str, limit: usize) -> IoResult<Vec<ChangeLogEntry>> {
        use std::io::{BufRead, BufReader};

        let file = File::open(&self.log_directory.join(&self.log_file))?;
        let reader = BufReader::new(file);

        let mut filtered_logs: Vec<ChangeLogEntry> = Vec::new();

        for line in reader.lines().flatten() {
            if let Ok(log_entry) = serde_json::from_str::<ChangeLogEntry>(&line) {
                if log_entry.table_name == table_name {
                    filtered_logs.push(log_entry);
                }
            }
        }

        // Reverse and truncate to match the most recent entries
        filtered_logs.reverse();
        filtered_logs.truncate(limit);

        Ok(filtered_logs)
    }

    pub fn log_file_path(&self) -> PathBuf {
        self.log_directory.join(&self.log_file)
    }
}

// Add this function to change_logging.rs
#[get("/api/logs/{table_name}")]
pub async fn get_logs_for_table(
    table_name: web::Path<String>,
    app_state: web::Data<AppState>,
) -> impl Responder {
    let table_name = table_name.into_inner();
    info!("Fetching logs for table: {}", table_name);

    let change_logger = ChangeLogger::new(&app_state.config.log_dir, app_state.config.log_file.clone());

    match change_logger.read_logs_for_table(&table_name, 1000) { // Adjust limit as needed
        Ok(logs) => {
            info!("Successfully fetched {} logs for table {}", logs.len(), table_name);
            HttpResponse::Ok().json(logs)
        }
        Err(e) => {
            error!("Failed to fetch logs for table {}: {}", table_name, e);
            HttpResponse::InternalServerError().body(format!("Failed to fetch logs: {}", e))
        }
    }
}

pub fn configure_logging_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(get_logs_for_table);
}
