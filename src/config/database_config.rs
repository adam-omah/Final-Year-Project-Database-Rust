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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from(DB_DIR),
            schema_file: PathBuf::from(SCHEMA_FILE),
            table_dir: PathBuf::from(TABLE_DIR),
            database_name: std::env::var("DATABASE_NAME")
                .unwrap_or_else(|_| "my_rust_db".to_string()),
        }
    }
}

impl DatabaseConfig {
    pub fn from_yaml(paths: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        // Try multiple potential paths
        for &path in paths {
            if let Ok(file) = File::open(path) {
                info!("Found configuration file at {}", path);
                let mut contents = String::new();
                let mut reader = std::io::BufReader::new(file);
                if reader.read_to_string(&mut contents).is_ok() {
                    let mut config: DatabaseConfig = serde_yaml::from_str(&contents)?;
                    info!("Loaded configuration '{:#?}'",config);
                    // Override database_name and hostname from environment variable
                    config.database_name = std::env::var("DATABASE_NAME").unwrap_or_else(|_| config.database_name.clone());


                    return Ok(config);
                }
            }
        }

        // If no config file is found, return an error
        Err("No valid configuration file found".into())
    }
}
