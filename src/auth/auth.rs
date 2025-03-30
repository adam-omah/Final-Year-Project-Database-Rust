// auth.rs
use actix_web::{web, error, HttpResponse, HttpRequest, Error, dev::{ServiceRequest, Service, Transform, ServiceResponse, forward_ready}, HttpMessage, body, FromRequest};
use futures::future::{ready, LocalBoxFuture, Ready};
use serde::{Serialize, Deserialize};
use crate::{schema, AppState};
use crate::schema::schema::{DataType, Column, Table, ConstraintType, Rule, RuleAction};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use actix_web::body::BoxBody;
use actix_web::error::{ErrorInternalServerError, ErrorUnauthorized};
use actix_web::middleware::Next;
use actix_web::web::Data;
use uuid::Uuid;
use chrono::Utc;
use base64::{engine::general_purpose, Engine as _};
use tracing::log::{debug, error, info, warn};
use crate::executer::executer::{ global_execute_query};
use crate::query::parser::{sql_parser, ASTNode};
use crate::tables::table::{create_table, delete_row};
// Import sql_parser

const USERS_TABLE: &str = "users";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub uuid: String,
    pub username: String,
    pub password_hash: String,
    pub auth_group: String,
}

// Simulate fetching user from the database
async fn fetch_user_from_db(username: &str, state: &web::Data<AppState>) -> Option<User> {
    let query = format!("SELECT * FROM {} WHERE username = {}", USERS_TABLE, username);

    // Parse the query string into AST nodes
    match sql_parser(query.as_bytes()) {
        Ok(ast_nodes) => {
            // Use global_execute_query instead of execute_query
            match global_execute_query(ast_nodes).await {
                Ok(response) => {
                    match body::to_bytes(response.into_body()).await {
                        Ok(body_bytes) => {
                            match serde_json::from_slice::<Vec<Vec<String>>>(&body_bytes) {
                                Ok(result) => {
                                    // Check if we have at least 2 rows (header + data)
                                    if result.len() >= 2 {
                                        let row = &result[1]; // Get the data row
                                        if row.len() >= 4 { // UUID, username, password_hash, auth_group
                                            return Some(User {
                                                uuid: row[0].clone(),
                                                username: row[1].clone(),
                                                password_hash: row[2].clone(),
                                                auth_group: row[3].clone(),
                                            });
                                        } else {
                                            eprintln!("Row doesn't have enough columns: expected at least 4, got {}", row.len());
                                        }
                                    } else {
                                        // This handles the case where user was not found (only header row)
                                        eprintln!("User not found: expected at least 2 rows, got {}", result.len());
                                    }
                                    None
                                },
                                Err(e) => {
                                    eprintln!("Failed to deserialize response body: {:?}", e);
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to convert body to bytes: {:?}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Global query execution error: {}", e);
                    None
                }
            }
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
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

async fn login(req: web::Json<LoginRequest>, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let login_request = req.into_inner();

    debug!("Login attempt for user: {}", login_request.username);

    // Fetch user from the database
    debug!("Fetching user '{}' from database", login_request.username);
    let fetched_user = fetch_user_from_db(&login_request.username, &state).await;

    match &fetched_user {
        Some(u) => {
            debug!("User '{}' found in database", login_request.username);

            // Don't log the actual password hash for security reasons
            if u.password_hash == login_request.password {
                info!("Authentication successful for user: {}", login_request.username);
                debug!("Returning successful login response for user: {}", login_request.username);
                Ok(HttpResponse::Ok().json(u)) // Return user info (or a session token)
            } else {
                warn!("Failed login attempt for user: {} (password mismatch)", login_request.username);
                debug!("Returning unauthorized response due to password mismatch");
                Err(error::ErrorUnauthorized("Invalid credentials"))
            }
        }
        None => {
            warn!("Failed login attempt for non-existent user: {}", login_request.username);
            debug!("Returning unauthorized response due to user not found");
            Err(error::ErrorUnauthorized("Invalid credentials"))
        }
    }
}

// Handler for creating a new user
#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
}

async fn create_user(req: web::Json<CreateUserRequest>, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let create_request = req.into_inner();

    // Check if the username already exists
    if fetch_user_from_db(&create_request.username, &state).await.is_some() {
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
    state: web::Data<AppState>,
    authenticated_user: User // Get the authenticated user from request
) -> Result<HttpResponse, Error> {
    let update_request = req.into_inner();

    // Ensure the user exists
    let target_user = match fetch_user_from_db(&update_request.username, &state).await {
        Some(user) => user,
        None => return Err(error::ErrorBadRequest("User doesn't exist"))
    };

    let mut updated_values: HashMap<String, String> = HashMap::new();
    updated_values.insert("username".to_string(), update_request.username.clone());
    updated_values.insert("password_hash".to_string(), update_request.password.clone());

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

    // Perform the update
    let result = crate::tables::table::update_row(
        USERS_TABLE,
        &update_request.uuid,
        updated_values,
        &state
    ).await;

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
    state: web::Data<AppState>,
    authenticated_user: User // Get the authenticated user from request
) -> Result<HttpResponse, Error> {
    let delete_request = req.into_inner();

    // Fetch the user to be deleted
    let target_user = match fetch_user_from_db(&delete_request.username, &state).await {
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
    info!("Creating default admin user");

    // Use a block scope to limit the lifetime of schema_locked
    {
        let schema_locked = app_state.schema.lock().unwrap();
        if !schema_locked.tables.contains_key(&format!("{}_initial", crate::USERS_TABLE)) {
            // Release the lock automatically at the end of this block
            drop(schema_locked);

            // Define user table schema with auth_group column
            let user_table = Table {
                name: USERS_TABLE.to_string(),
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
                    }
                ],
            };

            // Create the users table
            create_table(&user_table, &Data::new(app_state.clone()))?;
            info!("Created users table");
        } else {
            // If table exists, still release the lock before proceeding
            drop(schema_locked);
        }
    }

    info!("Checking for admin user");
    let query = format!("SELECT * FROM {} WHERE username = \"admin\"", USERS_TABLE);
    match sql_parser(query.as_bytes()) {
        Ok(ast_nodes) => {
            debug!("Query: {}", query);
            // Use global_execute_query instead of execute_query
            match global_execute_query(ast_nodes).await {
                Ok(response) => {
                    if response.status() == actix_web::http::StatusCode::OK {
                        // Rest of the existing code remains the same
                        match body::to_bytes(response.into_body()).await {
                            Ok(body_bytes) => {
                                let body_str = String::from_utf8_lossy(&body_bytes);
                                info!("Response body: {}", body_str);

                                // Parse the JSON response
                                match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                                    Ok(json_response) => {
                                        if let Some(message) = json_response.get("message") {
                                            if message == "No matching rows found" {
                                                // No admin user exists, create one with admin auth_group
                                                let insert_query = format!(
                                                    "INSERT INTO {} (username, password_hash, auth_group) VALUES ( \"admin\", \"admin\", \"admin\")",
                                                    crate::USERS_TABLE,
                                                );
                                                let insert_query_bytes = insert_query.as_bytes();

                                                match sql_parser(insert_query_bytes) {
                                                    Ok(insert_ast_nodes) => {
                                                        info!("Inserting default admin user");
                                                        match global_execute_query(insert_ast_nodes).await {
                                                            Ok(insert_response) => {
                                                                if insert_response.status() == actix_web::http::StatusCode::OK {
                                                                    match body::to_bytes(insert_response.into_body()).await {
                                                                        Ok(_) => {
                                                                            info!("Default admin user created successfully.");
                                                                            Ok(())
                                                                        }
                                                                        Err(e) => {
                                                                            error!("Failed to process default admin user creation response: {:?}", e);
                                                                            Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("Failed to create default admin user: {:?}", insert_response);
                                                                    Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to create default admin user"))
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!("Global query execution error: {}", e);
                                                                Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Error parsing insert query: {}", e);
                                                        Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                                    }
                                                }
                                            } else {
                                                // Some other message, consider as user exists
                                                info!("Admin user query returned: {}", message);
                                                Ok(())
                                            }
                                        } else if !body_bytes.is_empty() {
                                            // Non-empty response but no "message" field
                                            info!("Admin user likely exists");
                                            Ok(())
                                        } else {
                                            // Empty response
                                            error!("Empty response received");
                                            Err(std::io::Error::new(std::io::ErrorKind::Other, "Empty response"))
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to parse JSON response: {}", e);
                                        Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid response format"))
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to convert response body: {:?}", e);
                                Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                            }
                        }
                    } else {
                        error!("Query failed: {:?}", response);
                        Err(std::io::Error::new(std::io::ErrorKind::Other, "Query failed"))
                    }
                }
                Err(e) => {
                    error!("Global query execution error: {}", e);
                    Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                }
            }
        }
        Err(e) => {
            error!("Parse error: {}", e);
            Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        }
    }
}


impl FromRequest for User {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        // Extract user from request extensions (set during basic auth)
        let user = req.extensions().get::<User>().cloned();

        match user {
            Some(user) => ready(Ok(user)),
            None => ready(Err(actix_web::error::ErrorUnauthorized("Authentication required")))
        }
    }
}

// Authentication wrapper function
pub async fn authenticate_request(req: &HttpRequest, state: &Data<AppState>) -> Result<(), Error> {
    // Log request details
    debug!("Incoming request method: {}", req.method());
    debug!("Request URI: {}", req.uri());

    // Log all headers for debugging
    for (name, value) in req.headers() {
        debug!("Header - {}: {:?}", name, value);
    }

    // Check for Authorization header
    match req.headers().get("Authorization") {
        Some(auth_header) => {
            // Try to convert header to string
            match auth_header.to_str() {
                Ok(auth_str) => {
                    debug!("Authorization header found: {}", auth_str);

                    // Check for Basic auth
                    if !auth_str.starts_with("Basic ") {
                        warn!("Authorization header does not start with 'Basic '");
                        return Err(ErrorUnauthorized("Invalid authorization method"));
                    }

                    // Attempt base64 decoding
                    let encoded_credentials = &auth_str[6..];
                    debug!("Encoded credentials: {}", encoded_credentials);
                    match general_purpose::STANDARD.decode(encoded_credentials) {
                        Ok(decoded_credentials) => {
                            // Convert to UTF-8 string
                            match String::from_utf8(decoded_credentials) {
                                Ok(credentials_str) => {
                                    debug!("Decoded credentials: {}", credentials_str);

                                    // Split credentials
                                    let parts: Vec<&str> = credentials_str.split(':').collect();

                                    if parts.len() != 2 {
                                        warn!("Invalid credentials format");
                                        return Err(ErrorUnauthorized("Invalid credentials format"));
                                    }

                                    let (username, password) = (parts[0], parts[1]);
                                    debug!("Attempting to authenticate user: {} with password: {}", username, password);

                                    // Fetch user from database
                                    match fetch_user_from_db(username, state).await {
                                        Some(user) => {
                                            debug!("User found in database with stored password_hash: {}", user.password_hash);

                                            // Password check with detailed logging
                                            debug!("Comparing provided password: '{}' with stored hash: '{}'", password, user.password_hash);

                                            // Check password equality with length info
                                            let password_matches = user.password_hash == password;
                                            debug!("Password match result: {}", password_matches);
                                            debug!("Password lengths - provided: {} chars, stored: {} chars",
                                                   password.len(), user.password_hash.len());

                                            // Check for whitespace or special characters
                                            let has_whitespace_provided = password.contains(char::is_whitespace);
                                            let has_whitespace_stored = user.password_hash.contains(char::is_whitespace);
                                            debug!("Whitespace check - provided password: {}, stored hash: {}",
                                                   has_whitespace_provided, has_whitespace_stored);

                                            if password_matches {
                                                info!("Authentication successful for user: {}", username);

                                                // Insert user into request extensions
                                                req.extensions_mut().insert(user);
                                                return Ok(());
                                            } else {
                                                warn!("Password mismatch for user: {}", username);
                                                debug!("Byte-by-byte comparison:");

                                                let min_len = password.len().min(user.password_hash.len());
                                                for i in 0..min_len {
                                                    let p_char = &password[i..=i];
                                                    let h_char = &user.password_hash[i..=i];
                                                    debug!("Position {}: '{}' vs '{}', match: {}",
                                                           i, p_char, h_char, p_char == h_char);
                                                }

                                                if password.len() != user.password_hash.len() {
                                                    debug!("Length mismatch: Password has {} extra chars, hash has {} extra chars",
                                                           password.len().saturating_sub(user.password_hash.len()),
                                                           user.password_hash.len().saturating_sub(password.len()));
                                                }
                                            }
                                        },
                                        None => {
                                            warn!("No user found for username: {}", username);
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("Failed to convert decoded credentials to UTF-8: {:?}", e);
                                }
                            }
                        },
                        Err(e) => {
                            error!("Base64 decoding failed: {:?}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to convert Authorization header to string: {:?}", e);
                }
            }
        },
        None => {
            debug!("No Authorization header present in the request");
        }
    }

    // If we reach here, authentication failed
    warn!("Authentication failed - returning Unauthorized");
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
                                   state: web::Data<AppState>,
                                   user: User| {
                update_user(req, state, user)
            })),
    );

    cfg.service(
        web::resource("/api/delete-user")
            .app_data(web::JsonConfig::default().limit(4096))
            .route(web::post().to(|req: web::Json<DeleteUserRequest>,
                                   state: web::Data<AppState>,
                                   user: User| {
                delete_user(req, state, user)
            })),
    );
}
