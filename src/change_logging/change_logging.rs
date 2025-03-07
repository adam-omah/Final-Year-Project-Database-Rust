use std::fs::{OpenOptions, File};
use std::io::{Write, Result as IoResult};
use std::path::{Path, PathBuf};
use serde_json;
use uuid::Uuid;

/// Enum to represent different types of database operations
#[derive(serde::Serialize,serde::Deserialize)]
pub enum ChangeType {
    Create,
    Insert,
    Update,
    Delete,
    Select,
    Query,
    Drop,
}

/// Struct to represent a change log entry
#[derive(serde::Serialize,serde::Deserialize)]
pub struct ChangeLogEntry {
    pub change_id: Uuid,
    pub change_type: ChangeType,
    pub table_name: String,
    pub data: serde_json::Value,
    pub user: Option<String>, // Optional user identifier
}

/// Change Logger struct to manage logging operations
#[derive(Clone)]
pub struct ChangeLogger {
    log_file_path: PathBuf,
}

impl ChangeLogger {
    /// Create a new ChangeLogger instance
    pub fn new(log_directory: impl AsRef<Path>) -> Self {
        // Ensure the log directory exists
        std::fs::create_dir_all(&log_directory).expect("Could not create log directory");

        let log_file_path = log_directory.as_ref().join("change_log.json");

        ChangeLogger {
            log_file_path,
        }
    }

    /// Alternative constructor that allows more flexible path creation
    pub fn with_custom_path(log_file_path: PathBuf) -> Self {
        // Ensure the parent directory exists
        if let Some(parent) = log_file_path.parent() {
            std::fs::create_dir_all(parent).expect("Could not create log directory");
        }

        ChangeLogger {
            log_file_path,
        }
    }


    /// Log a change to the log file
    pub fn log_change(
        &self,
        change_id: Option<Uuid>,
        change_type: ChangeType,
        table_name: String,
        data: serde_json::Value,
        user: Option<String>
    ) -> IoResult<()> {
        // Create a log entry, using the provided change_id or generating a new one
        let log_entry = ChangeLogEntry {
            change_id: change_id.unwrap_or_else(Uuid::new_v4),
            change_type,
            table_name,
            data,
            user,
        };

        // Serialize the log entry to JSON
        let log_entry_json = serde_json::to_string(&log_entry)
            .expect("Failed to serialize log entry");

        // Open the log file in append mode
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file_path)?;

        // Write the JSON log entry with a newline
        writeln!(file, "{}", log_entry_json)?;

        Ok(())
    }
    /// Read recent log entries
    pub fn read_recent_logs(&self, limit: usize) -> IoResult<Vec<ChangeLogEntry>> {
        use std::io::{BufRead, BufReader};

        let file = File::open(&self.log_file_path)?;
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

    pub fn log_file_path(&self) -> &PathBuf {
        &self.log_file_path
    }
}

/// Utility function for logging database changes
pub fn log_database_change(
    change_id: Option<Uuid>,
    change_logger: &ChangeLogger,
    change_type: ChangeType,
    table_name: String,
    data: serde_json::Value
) {
    if let Err(e) = change_logger.log_change(change_id,change_type, table_name, data, None) {
        tracing::error!("Failed to log change: {}", e);
    }
}