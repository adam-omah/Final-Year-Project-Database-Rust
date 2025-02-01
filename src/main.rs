use std::io::Result;

mod schema;
mod records; // Use 'table' module name

use schema::{
    schema::Column,
    schema::DataType,
    schema::Schema,
    schema::Table,
    schema::load_schema,
    schema::create_table as schema_create_table // Import using an alias
};
use records::{table::create_table, table::insert_row}; // Import from the 'table' module


pub const DB_DIR: &str = "mydb"; // Make DB_DIR public
pub const SCHEMA_FILE: &str = "schema.json";
pub const TABLE_DIR: &str = "tables"; // Added TABLE_DIR constant

fn init_database() -> Result<()> {
    let db_path = std::path::Path::new(DB_DIR);
    std::fs::create_dir_all(db_path)?;

    let table_dir = db_path.join(TABLE_DIR); // Create tables subdir
    std::fs::create_dir_all(table_dir)?;

    Ok(())
}

fn main() -> Result<()> {
    init_database()?;

    let mut schema = load_schema()?;

    if !schema.tables.contains_key("users") {
        let user_table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    rules: vec![],
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    rules: vec![],
                },
            ],
        };

        create_table(&user_table)?; // No need to clone here
    }

    insert_row("users", vec!["1".to_string(), "Alice".to_string()])?;
    insert_row("users", vec!["2".to_string(), "test".to_string()])?;


    println!("Database initialized!");
    Ok(())
}