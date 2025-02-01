use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use crate::{DB_DIR, SCHEMA_FILE, TABLE_DIR};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseConfig {
    pub db_dir: PathBuf,
    pub schema_file: PathBuf,
    pub table_dir: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_dir: PathBuf::from(DB_DIR),
            schema_file: PathBuf::from(SCHEMA_FILE),
            table_dir: PathBuf::from(TABLE_DIR),
        }
    }
}
