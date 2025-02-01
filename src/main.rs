use std::io::Result;

mod schema;
mod records;

use schema::{
    schema::Column,
    schema::create_schema,
    schema::DataType,
    schema::Schema};
use records::{table::create_table, table::insert_row};

const DB_DIR: &str = "mydb";
const SCHEMA_FILE: &str = "schema.json";

fn init_database() -> Result<()> {
    std::fs::create_dir_all(DB_DIR)?;
    Ok(())
}

fn main() -> Result<()> {

    init_database()?;


    let user_schema = Schema {
        table_name: "users".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::String,
            },
        ],
    };


    create_schema(&user_schema)?;

    create_table(&user_schema)?;

    insert_row(&user_schema, vec!["1".to_string(), "Alice".to_string()])?;

    println!("Database initialized!");

    Ok(())



}