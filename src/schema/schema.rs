
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Result, Seek, Write};
use std::path::Path;

use crate::{DB_DIR, SCHEMA_FILE};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataType {
    Int,
    String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub table_name: String,
    pub columns: Vec<Column>,
}

pub fn create_schema(schema: &Schema) -> Result<()> {
    let schema_path = Path::new(DB_DIR).join(SCHEMA_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(schema_path)?;


    let mut schemas: Vec<Schema> = serde_json::from_reader(&file).unwrap_or_default();
    schemas.push(schema.clone());


    file.seek(std::io::SeekFrom::Start(0))?;
    serde_json::to_writer_pretty(&mut file, &schemas)?;

    Ok(())
}