// auth.rs
use actix_web::{web, error, HttpResponse, HttpRequest, Error, HttpMessage, body};
use serde::{Serialize, Deserialize};
use crate::{ AppState, USERS_TABLE};
use crate::schema::schema::{DataType, Column, Table};
use std::collections::HashMap;
use actix_web::error::{ ErrorUnauthorized};
use actix_web::web::Data;
use uuid::Uuid;
use base64::{engine::general_purpose, Engine as _};
use tracing::log::{error, info, warn};
use crate::executer::executer::{ global_execute_query};
use crate::query::parser::{sql_parser};
use crate::replication::replication_nodes::load_nodes;
use crate::tables::table::{create_table, delete_row, recalculate_table_global, update_row};
// Import sql_parser

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub uuid: String,
    pub username: String,
    pub password_hash: String,
    pub auth_group: String,
}

// Simulate fetching user from the database
async fn fetch_user_from_db(username: &str) -> Option<User> {
    let query = format!("SELECT * FROM {} WHERE username = {}", USERS_TABLE, username);
    // Log the exact query being executed
    match sql_parser(query.as_bytes()) {
        Ok(ast_nodes) => {
            match global_execute_query(ast_nodes).await {
                Ok(response) => {
                    match body::to_bytes(response.into_body()).await {
                        Ok(body_bytes) => {
                            match serde_json::from_slice::<Vec<Vec<String>>>(&body_bytes) {
                                Ok(result) => {
                                    if result.len() >= 2 {
                                        let row = &result[1]; // Get the data row

                                        // NEW: Check if the row is marked for removal
                                        if row.iter().any(|cell| cell.contains("ROW_REMOVED")) {
                                            warn!("User {} is marked as removed", username);
                                            return None;
                                        }

                                        if row.len() >= 4 { // UUID, username, password_hash, auth_group
                                            return Some(User {
                                                uuid: row[0].clone(),
                                                username: row[1].clone(),
                                                password_hash: row[2].clone(),
                                                auth_group: row[3].clone(),
                                            });
                                        } else {
                                            error!("Row doesn't have enough columns: expected at least 4, got {}", row.len());
                                        }
                                    } else {
                                        error!("No user found: expected at least 2 rows, got {}", result.len());
                                    }
                                    None
                                },
                                Err(e) => {
                                    error!("Failed to deserialize response body: {:?}", e);
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to convert body to bytes: {:?}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    error!("Global query execution error: {}", e);
                    None
                }
            }
        }
        Err(e) => {
            error!("Parse error: {}", e);
            None
        }
    }
}

//**  Authentication Routes **//
// Handler for the /login endpoint
#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

async fn login(req: web::Json<LoginRequest>) -> Result<HttpResponse, Error> {
    let login_request = req.into_inner();

    // Fetch user from the database
    let fetched_user = fetch_user_from_db(&login_request.username).await;

    match &fetched_user {
        Some(u) => {
            // Don't log the actual password hash for security reasons
            if u.password_hash == login_request.password {
                Ok(HttpResponse::Ok().json(u)) // Return user info (or a session token)
            } else {
                warn!("Failed login attempt for user: {} (password mismatch)", login_request.username);
                Err(ErrorUnauthorized("Invalid credentials"))
            }
        }
        None => {
            warn!("Failed login attempt for non-existent user: {}", login_request.username);
            Err(ErrorUnauthorized("Invalid credentials"))
        }
    }
}

// Handler for creating a new user
#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
}

async fn create_user(req: web::Json<CreateUserRequest>, state: Data<AppState>) -> Result<HttpResponse, Error> {
    let create_request = req.into_inner();

    // Check if the username already exists
    if fetch_user_from_db(&create_request.username).await.is_some() {
        return Err(error::ErrorBadRequest("Username already exists"));
    }

    // Create a new user with "pending" auth_group
    let new_user = User {
        uuid: Uuid::new_v4().to_string(),
        username: create_request.username.clone(),
        password_hash: create_request.password.clone(), // Store a HASHED password in real life!
        auth_group: "pending".to_string(),
    };

    // Insert the new user into the database
    let table_name = USERS_TABLE;
    let row_data = vec![
        new_user.uuid.clone().to_string(),
        new_user.username.clone(),
        new_user.password_hash.clone(),
        new_user.auth_group.clone()
    ];

    let result = crate::tables::table::insert_row(table_name, row_data, &state, None).await;

    match result {
        Ok(_) => {
            Ok(HttpResponse::Created().json(new_user))
        }
        Err(e) => {
            eprintln!("Failed to create user: {}", e);
            Err(error::ErrorInternalServerError("Failed to create user"))
        }
    }
}


// Handler for updating a user
#[derive(Deserialize)]
pub struct UpdateUserRequest {
    pub uuid: String,
    pub username: String,
    pub password: String,
    pub auth_group: Option<String>, // Making it optional so existing code doesn't break
}


async fn update_user(
    req: web::Json<UpdateUserRequest>,
    state: Data<AppState>,
    authenticated_user: User // Get the authenticated user from request
) -> Result<HttpResponse, Error> {
    info!("Updating user");
    let update_request = req.into_inner();

    // Ensure the user exist
    let target_user = match fetch_user_from_db(&update_request.username).await {
        Some(user) => user,
        None => return Err(error::ErrorBadRequest("User doesn't exist"))
    };

    let mut updated_values: HashMap<String, String> = HashMap::new();
    updated_values.insert("username".to_string(), update_request.username.clone());
    if !update_request.password.is_empty() {
        updated_values.insert("password_hash".to_string(), update_request.password.clone());
    }else{
        updated_values.insert("password_hash".to_string(), target_user.password_hash.clone());
    }


    // Check if auth_group is being updated
    if let Some(new_auth_group) = &update_request.auth_group {
        // Only users with "admin" auth_group can modify the auth_group field
        if authenticated_user.auth_group != "admin" {
            return Err(error::ErrorForbidden("Only administrators can change user groups"));
        }

        // Admin users can modify the auth_group
        updated_values.insert("auth_group".to_string(), new_auth_group.clone());

        // Log the auth group update
        info!(
            "User '{}' (auth_group: '{}') updating auth_group of user '{}' from '{}' to '{}'",
            authenticated_user.username,
            authenticated_user.auth_group,
            target_user.username,
            target_user.auth_group,
            new_auth_group
        );
    } else if authenticated_user.auth_group != "admin" && authenticated_user.uuid != target_user.uuid {
        // Non-admins can only update their own accounts
        return Err(error::ErrorForbidden("You can only update your own account"));
    }
    let wrapped_row_id = format!("\"{}\"", update_request.uuid);

    // Perform the update
    let result = update_row(
        USERS_TABLE,
        &wrapped_row_id,
        updated_values,
        &state
    ).await;

    actix_web::rt::spawn(async move {
        match recalculate_table_global(USERS_TABLE).await {
            Ok(_) => {
                info!("Successfully recalculated table {}", USERS_TABLE);
            },
            Err(e) => {
                error!("Failed to recalculate table {}: {}", USERS_TABLE, e);
            }
        }
    });

    match result {
        Ok(_) => {
            Ok(HttpResponse::Ok().json("User updated successfully"))
        }
        Err(e) => {
            error!("Failed to update user: {}", e);
            Err(error::ErrorInternalServerError("Failed to update user"))
        }
    }
}



// Handler for deleting a user
#[derive(Deserialize)]
pub struct DeleteUserRequest {
    pub id: String,
    pub username: String,
}

async fn delete_user(
    req: web::Json<DeleteUserRequest>,
    state: Data<AppState>,
    authenticated_user: User // Get the authenticated user from request
) -> Result<HttpResponse, Error> {
    let delete_request = req.into_inner();

    // Fetch the user to be deleted
    let target_user = match fetch_user_from_db(&delete_request.username).await {
        Some(user) => user,
        None => return Err(error::ErrorBadRequest("User doesn't exist"))
    };

    // Authorization check: Only admins or the user themselves can delete the account
    if authenticated_user.auth_group != "admin" && authenticated_user.uuid != target_user.uuid {
        return Err(error::ErrorForbidden("You can only delete your own account or must be an administrator"));
    }

    // Proceed with user deletion
    let result = delete_row(
        &USERS_TABLE.to_string(),
        &delete_request.id,
        &state
    ).await;

    // Log the deletion
    info!(
        "User '{}' (auth_group: '{}') deleting user '{}' (uuid: '{}')",
        authenticated_user.username,
        authenticated_user.auth_group,
        target_user.username,
        target_user.uuid
    );

    match result {
        Ok(_) => {
            Ok(HttpResponse::Ok().json("User deleted successfully"))
        }
        Err(e) => {
            error!("Failed to delete user: {}", e);
            Err(error::ErrorInternalServerError("Failed to delete user"))
        }
    }
}

pub async fn create_default_user(app_state: &AppState) -> std::io::Result<()> {
    info!("Starting default admin user creation process...");

    // --- 1. Ensure 'auth_users' Table Exists ---
    // Use a block scope to ensure the lock is released promptly.
    let table_exists: bool = {
        let schema_guard = app_state.schema.lock().unwrap();
        // Check for the initial table name as used internally by your storage logic
        schema_guard
            .tables
            .contains_key(&format!("{}_initial", USERS_TABLE))
    }; // schema_guard is dropped here, releasing the lock

    if !table_exists {
        info!("'{}' table does not exist in schema cache. Attempting creation...", USERS_TABLE);
        // Define the schema for the authentication users table
        let user_table = Table {
            name: USERS_TABLE.to_string(), // Use the constant
            columns: vec![
                Column {
                    name: "username".to_string(),
                    data_type: DataType::String,
                    rules: vec![],
                },
                Column {
                    name: "password_hash".to_string(),
                    data_type: DataType::String,
                    rules: vec![],
                },
                Column {
                    name: "auth_group".to_string(),
                    data_type: DataType::String,
                    rules: vec![],
                },
            ],
        };

        // Attempt to create the table using the storage function
        match create_table(&user_table, &Data::new(app_state.clone())) {
            Ok(_) => {
                info!("Successfully created '{}' table.", USERS_TABLE);
            }
            Err(e) => {
                error!(
                    "Failed to create '{}' table during default user setup: {}",
                    USERS_TABLE, e
                );
                // Propagate the error; setup cannot continue without the table
                return Err(e);
            }
        }
    } else {
        info!("'{}' table already exists in schema cache.", USERS_TABLE);
    }

    // 2. Check if 'admin' User Exists
    info!("Checking for existing 'admin' user in '{}' table...", USERS_TABLE);
    // Construct the SELECT query carefully, ensuring quotes if needed by the parser/executor
    let select_query = format!("SELECT * FROM {} WHERE username = \"admin\"", USERS_TABLE);

    match sql_parser(select_query.as_bytes()) {
        Ok(select_ast_nodes) => {
            info!("Successfully parsed SELECT query for admin user check.");
            // Execute the SELECT query using the global executor
            match global_execute_query(select_ast_nodes).await {
                // Query Execution Successful
                Ok(response) => {
                    let status = response.status();
                    info!("SELECT query execution finished with status: {}", status);
                    // Proceed only if the status code indicates success (e.g., 200 OK)
                    if status.is_success() {
                        // Read the response body
                        match body::to_bytes(response.into_body()).await {
                            Ok(body_bytes) => {
                                // Attempt to parse the body as JSON Value
                                match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                                    // JSON Parsing Successful ---
                                    Ok(json_response) => {
                                        // *** NEW LOGIC: Check if the response is an array and its length ***
                                        if let Some(array) = json_response.as_array() {
                                            info!("Response is a JSON array with length: {}", array.len());
                                            // If length is > 1, it means header + data row(s) exist.
                                            // If length is <= 1, it means only header or empty array (no admin data).
                                            if array.len() > 1 {
                                                // User 'admin' exists (header + at least one data row)
                                                info!("Admin user found (JSON array length > 1). No action needed.");
                                                Ok(()) // Indicate success, default user exists or setup not needed
                                            } else {
                                                // User 'admin' does NOT exist (empty array or header only)
                                                info!("Admin user not found (JSON array length <= 1). Proceeding to create default user...");
                                                let insert_query = format!(
                                                    "INSERT INTO {} (username, password_hash, auth_group) VALUES ( \"admin\", \"admin\", \"admin\")", // Consider hashing the password!
                                                    USERS_TABLE
                                                );
                                                info!("Constructed INSERT query: {}", insert_query); // Log the query for debugging
                                                match sql_parser(insert_query.as_bytes()) {
                                                    Ok(insert_ast_nodes) => {
                                                        // Execute the INSERT query
                                                        match global_execute_query(insert_ast_nodes).await {
                                                            Ok(insert_response) => {
                                                                let insert_status = insert_response.status();
                                                                if insert_status.is_success() {
                                                                    // Optionally read/verify insert response body if needed
                                                                    match body::to_bytes(insert_response.into_body()).await {
                                                                        Ok(insert_body) => {
                                                                            info!("Default admin user created successfully. Response body: {:?}", String::from_utf8_lossy(&insert_body));
                                                                            Ok(())
                                                                        }
                                                                        Err(e) => {
                                                                            error!("Failed to read INSERT response body, but status was success: {:?}", e);
                                                                            // Decide if this is still considered overall success
                                                                            Ok(()) // Or return error if body needed verification
                                                                        }
                                                                    }
                                                                } else {
                                                                    // INSERT query execution failed
                                                                    error!("INSERT query failed with status: {}", insert_status);
                                                                    // Attempt to read error body for more details
                                                                    let error_body = match body::to_bytes(insert_response.into_body()).await {
                                                                        Ok(b) => String::from_utf8_lossy(&b).to_string(),
                                                                        Err(_) => "Could not read error response body".to_string(),
                                                                    };
                                                                    error!("INSERT failure response body: {}", error_body);
                                                                    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create default admin user. Status: {}. Body: {}", insert_status, error_body)))
                                                                }
                                                            }
                                                            Err(e) => {
                                                                // Error executing the INSERT query itself
                                                                error!("Error executing INSERT global query: {}", e);
                                                                Err(std::io::Error::new(std::io::ErrorKind::Other, format!("INSERT global_execute_query error: {}", e)))
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        // Error parsing the INSERT query
                                                        error!("Error parsing INSERT query: {}", e);
                                                        Err(std::io::Error::new(std::io::ErrorKind::Other, format!("INSERT sql_parser error: {}", e)))
                                                    }
                                                }
                                            }
                                        } else {
                                            // Response was valid JSON, but not an array. This is unexpected.
                                            error!("Parsed JSON response is not an array as expected from handle_select. Response: {:?}", json_response);
                                            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected JSON response format (not an array)"))
                                        }
                                    }
                                    // SON Parsing Failed
                                    Err(e) => {
                                        let body_str = String::from_utf8_lossy(&body_bytes);
                                        error!("Failed to parse SELECT response body as JSON: {}. Body: '{}'", e, body_str);
                                        Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to parse JSON response: {}", e)))
                                    }
                                }
                            }
                            Err(e) => {
                                // Error reading the SELECT response body
                                error!("Failed to read SELECT response body bytes: {:?}", e);
                                Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read response body: {}", e)))
                            }
                        }
                    } else {
                        // SELECT query execution failed (non-2xx status)
                        error!("SELECT query execution failed with status: {}", status);
                        // Attempt to read error body for more details
                        let error_body = match body::to_bytes(response.into_body()).await {
                            Ok(b) => String::from_utf8_lossy(&b).to_string(),
                            Err(_) => "Could not read error response body".to_string(),
                        };
                        error!("SELECT failure response body: {}", error_body);
                        Err(std::io::Error::new(std::io::ErrorKind::Other, format!("SELECT query failed. Status: {}. Body: {}", status, error_body)))
                    }
                }
                // Query Execution Failed
                Err(e) => {
                    // Error executing the SELECT query itself
                    error!("Error executing SELECT global query: {}", e);
                    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("SELECT global_execute_query error: {}", e)))
                }
            }
        }
        Err(e) => {
            // Error parsing the SELECT query
            error!("Error parsing SELECT query: {}", e);
            Err(std::io::Error::new(std::io::ErrorKind::Other, format!("SELECT sql_parser error: {}", e)))
        }
    }
}

// Authentication wrapper function
pub async fn authenticate_request(req: &HttpRequest, state: &Data<AppState>) -> Result<(), Error> {
    // Check for Authorization header
    match req.headers().get("Authorization") {
        Some(auth_header) => {
            match auth_header.to_str() {
                Ok(auth_str) => {
                    // Check for "Basic" authentication
                    if auth_str.starts_with("Basic ") {
                        let encoded_credentials = &auth_str[6..];

                        // 1. Attempt regular user authentication first
                        match general_purpose::STANDARD.decode(encoded_credentials) {
                            Ok(decoded_credentials) => {
                                match String::from_utf8(decoded_credentials) {
                                    Ok(credentials_str) => {
                                        let parts: Vec<&str> = credentials_str.split(':').collect();
                                        if parts.len() == 2 {
                                            let (username, password) = (parts[0], parts[1]);
                                            match fetch_user_from_db(username).await {
                                                Some(user) => {
                                                    if user.password_hash == password  && user.auth_group != "pending" {
                                                        req.extensions_mut().insert(user);
                                                        return Ok(()); // Successful user authentication
                                                    } else {
                                                        warn!("Incorrect password for user: {}", username);
                                                        // Fall through to check for replication node auth
                                                    }
                                                },
                                                None => {
                                                    warn!("No user found with username: {}", username);
                                                    // Fall through to check for replication node auth
                                                }
                                            }
                                        } else {
                                            warn!("Invalid credentials format (not username:password)");
                                            return Err(ErrorUnauthorized("Invalid credentials format"));
                                        }
                                    },
                                    Err(e) => {
                                        error!("UTF-8 conversion error: {:?}", e);
                                        return Err(ErrorUnauthorized("Invalid credentials"));
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("Base64 decoding failed: {:?}", e);
                                //Might be a replication node, so fall through
                            }
                        }

                        // 2. If user authentication fails, try replication node authentication
                        let config = &state.config;
                        let nodes_config = match load_nodes(config) {
                            Ok(config) => config,
                            Err(_) => return Err(ErrorUnauthorized("Could not load node configuration")),
                        };

                        if let Some(_node) = nodes_config.nodes.iter().find(|node| {
                            let credentials = format!("{}:{}", node.name, node.shared_secret);
                            let encoded_credentials = general_purpose::STANDARD.encode(credentials);
                            format!("Basic {}", encoded_credentials) == auth_str
                        }) {
                            // Add any specific logic for handling authenticated replication nodes here.
                            return Ok(());
                        } else {
                            warn!("Invalid shared secret for replication node");
                        }
                    } else {
                        warn!("Authorization header does not start with 'Basic '");
                    }
                },

                Err(e) => {
                    error!("Failed to convert Authorization header to string: {:?}", e);
                }
            }
        },
        None => {
            error!("No Authorization header present");
        }
    }
    Err(ErrorUnauthorized("Invalid credentials"))
}




// Public function to configure authentication routes
pub fn configure_auth_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/login")
            .app_data(web::JsonConfig::default().limit(4096)) // Limit request body size
            .route(web::post().to(login)),
    );

    cfg.service(
        web::resource("/create-user")
            .app_data(web::JsonConfig::default().limit(4096)) // Limit request body size
            .route(web::post().to(create_user)),
    );

    // For routes that require authentication, we ensure User is extracted
    cfg.service(
        web::resource("/api/update-user")
            .app_data(web::JsonConfig::default().limit(4096))
            .route(web::post().to(|req: web::Json<UpdateUserRequest>,
                                   state: Data<AppState>,
                                   req_http: HttpRequest| async move {
                // Manually authenticate the request
                match authenticate_request(&req_http, &state).await {
                    Ok(_) => {
                        // Extract user from request extensions after authentication
                        match req_http.extensions().get::<User>() {
                            Some(user) => update_user(req, state, user.clone()).await,
                            None => Err(ErrorUnauthorized("Authentication failed"))
                        }
                    },
                    Err(e) => Err(e)
                }
            })),
    );

    cfg.service(
        web::resource("/api/delete-user")
            .app_data(web::JsonConfig::default().limit(4096))
            .route(web::post().to(|req: web::Json<DeleteUserRequest>,
                                   state: Data<AppState>,
                                   req_http: HttpRequest| async move {
                // Manually authenticate the request
                match authenticate_request(&req_http, &state).await {
                    Ok(_) => {
                        // Extract user from request extensions after authentication
                        match req_http.extensions().get::<User>() {
                            Some(user) => delete_user(req, state, user.clone()).await,
                            None => Err(ErrorUnauthorized("Authentication failed"))
                        }
                    },
                    Err(e) => Err(e)
                }
            })),
    );
}
