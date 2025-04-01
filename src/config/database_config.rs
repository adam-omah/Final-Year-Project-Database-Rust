use std::env;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
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
    pub node_url: String,
    pub node_port: u16,

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
            node_url:default_node_url(),
            node_port: default_node_port(),
        }
    }
}

fn default_node_port() -> u16 {
    env::var("PORT")
        .map(|p| p.parse().unwrap_or(8080))
        .unwrap_or(8080)
}

fn default_node_url() -> String {
    env::var("HOSTNAME")
        .unwrap_or_else(|_| "http://0.0.0.0".to_string())
}


impl DatabaseConfig {
    pub fn from_yaml(paths: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        // Try multiple potential paths
        for &path in paths {
            if let Ok(file) = File::open(path) {
                let mut contents = String::new();
                let mut reader = std::io::BufReader::new(file);
                if reader.read_to_string(&mut contents).is_ok() {
                    let mut config: DatabaseConfig = serde_yaml::from_str(&contents)?;

                    // Prioritize environment variable for port
                    config.node_port = env::var("PORT")
                        .map(|p| p.parse().unwrap_or(config.node_port))
                        .unwrap_or(config.node_port);

                    // Similar overrides for other env vars
                    config.database_name = env::var("DATABASE_NAME")
                        .unwrap_or_else(|_| config.database_name.clone());

                    config.node_url = env::var("HOSTNAME")
                        .unwrap_or_else(|_| config.node_url.clone());

                    return Ok(config);
                }
            }
        }
        // If no config file is found, create default config with env var checks
        Ok(DatabaseConfig::default())
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplicationConfig {
    // Use raw numeric type that can be converted
    pub max_offline_duration: i64, // Seconds
    pub max_replication_attempts: u32,
    pub retry_interval: i64, // Seconds
    pub sync_interval: u64, // Minutes - If 0, then is not configured at all
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            max_offline_duration: 86400, // 24 hours in seconds
            max_replication_attempts: 3,
            retry_interval: 300, // 5 minutes in seconds
            sync_interval: 0, // Default to disabled
        }
    }
}



