use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use tracing::log::info;
use crate::{DB_DIR, SCHEMA_FILE, TABLE_DIR};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseConfig {
    pub db_dir: PathBuf,
    pub schema_file: PathBuf,
    pub table_dir: PathBuf,
    pub database_name: String,
    pub log_dir: PathBuf,
    pub log_file: String,
    pub repl_node_file: PathBuf,
    pub replication: ReplicationConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from(DB_DIR),
            schema_file: PathBuf::from(SCHEMA_FILE),
            table_dir: PathBuf::from(TABLE_DIR),
            database_name: env::var("DATABASE_NAME")
                .unwrap_or_else(|_| "my_rust_db".to_string()),
            log_dir: PathBuf::from("logs"),
            log_file: "change_log.json".to_string(),
            repl_node_file: PathBuf::from("nodes.json"),
            replication: ReplicationConfig::default(),
        }
    }
}

impl DatabaseConfig {
    pub fn from_yaml(paths: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        // Try multiple potential paths
        for &path in paths {
            println!("Attempting to load config from path: {}", path); // Add debug print
            if let Ok(file) = File::open(path) {
                let mut contents = String::new();
                let mut reader = std::io::BufReader::new(file);
                if reader.read_to_string(&mut contents).is_ok() {
                    println!("File contents:\n{}", contents); // Print file contents

                    // Add more detailed error handling
                    match serde_yaml::from_str(&contents) {
                        Ok(mut config) => {
                            // Override database_name and hostname from environment variable
                            let mut config: DatabaseConfig = config;
                            config.database_name = std::env::var("DATABASE_NAME")
                                .unwrap_or_else(|_| config.database_name.clone());

                            println!("Parsed config successfully: {:?}", config);
                            return Ok(config);
                        }
                        Err(e) => {
                            println!("Deserialization error: {}", e);
                            return Err(Box::new(e));
                        }
                    }
                }
            }
        }

        // If no config file is found, return an error
        Err("No valid configuration file found".into())
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplicationConfig {
    // Use raw numeric type that can be converted
    pub max_offline_duration: i64, // Seconds
    pub max_replication_attempts: u32,
    pub retry_interval: i64, // Seconds
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            max_offline_duration: 86400, // 24 hours in seconds
            max_replication_attempts: 3,
            retry_interval: 300, // 5 minutes in seconds
        }
    }
}



