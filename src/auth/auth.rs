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
use crate::tables::table::create_table;
// Import sql_parser

const USERS_TABLE: &str = "users";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
}

// Simulate fetching user from the database
async fn fetch_user_from_db(username: &str, state: &web::Data<AppState>) -> Option<User> {
    let query = format!("SELECT * FROM {}_initial WHERE username = '{}'", USERS_TABLE, username);

    // Parse the query string into AST nodes
    match sql_parser(query.as_bytes()) {
        Ok(ast_nodes) => {
            // Use global_execute_query instead of execute_query
            match global_execute_query(ast_nodes).await {
                Ok(response) => {
                    match body::to_bytes(response.into_body()).await {
                        Ok(body_bytes) => {
                            if let Ok(result) = serde_json::from_slice::<Vec<Vec<String>>>(&body_bytes) {
                                if let Some(row) = result.get(0) {
                                    if row.len() >= 3 { // UUID, username, password_hash
                                        return Some(User {
                                            id: row[0].clone(),
                                            username: row[1].clone(),
                                            password_hash: row[2].clone(),
                                        });
                                    }
                                }
                            } else {
                                eprintln!("Failed to deserialize response body");
                            }
                            None
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

// // Middleware Attempt
// pub async fn basic_auth_middleware(
//     mut req: ServiceRequest,
//     next: Next<impl Service<ServiceRequest> + actix_web::body::MessageBody + 'static>
// ) -> Result<ServiceResponse<BoxBody>, Error> {
//     let state_data = req.app_data::<web::Data<AppState>>()
//         .ok_or_else(|| ErrorInternalServerError("No app state"))?;
//
//     // Check for Authorization header
//     if let Some(auth_header) = req.headers().get("Authorization") {
//         let auth_str = auth_header.to_str().map_err(|_| ErrorUnauthorized("Invalid header"))?;
//
//         if auth_str.starts_with("Basic ") {
//             let encoded_credentials = &auth_str[6..];
//             let decoded_credentials = general_purpose::STANDARD
//                 .decode(encoded_credentials)
//                 .map_err(|_| ErrorUnauthorized("Invalid base64"))?;
//
//             let credentials_str = String::from_utf8(decoded_credentials)
//                 .map_err(|_| ErrorUnauthorized("Invalid credentials"))?;
//
//             let parts: Vec<&str> = credentials_str.split(':').collect();
//
//             if parts.len() == 2 {
//                 let (username, password) = (parts[0], parts[1]);
//
//                 // Fetch user and validate
//                 if let Some(user) = fetch_user_from_db(username, state_data).await {
//                     if user.password_hash == password {
//                         // Authentication successful, insert user into request extensions
//                         req.extensions_mut().insert(user);
//                         // Explicitly specify the return type
//                         return next.call(req).await.map(|res| res.map_into_boxed_body());
//                     }
//                 }
//             }
//         }
//     }
//     Err(ErrorUnauthorized("Authentication failed"))
// }



// Function to extract user from request (if available)
pub fn get_user_from_request(req: &HttpRequest) -> Option<User> {
    req.extensions().get::<User>().cloned()
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

    // Fetch user from the database
    let fetched_user = fetch_user_from_db(&login_request.username, &state).await;

    match fetched_user {
        Some(u) => {
            // In real life, you'd hash the password and compare it with the stored hash
            if u.password_hash == login_request.password {
                // Authentication successful
                Ok(HttpResponse::Ok().json(u)) // Return user info (or a session token)
            } else {
                Err(error::ErrorUnauthorized("Invalid credentials"))
            }
        }
        None => Err(error::ErrorUnauthorized("Invalid credentials")),
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

    // Create a new user
    let new_user = User {
        id: Uuid::new_v4().to_string(),
        username: create_request.username.clone(),
        password_hash: create_request.password.clone(), // Store a HASHED password in real life!
    };

    // Insert the new user into the database
    let table_name = USERS_TABLE;
    let row_data = vec![new_user.id.clone(), new_user.username.clone(), new_user.password_hash.clone()];

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
    pub id: String,
    pub username: String,
    pub password: String,
}

async fn update_user(req: web::Json<UpdateUserRequest>, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let update_request = req.into_inner();

    // Ensure the user exists
    if fetch_user_from_db(&update_request.username, &state).await.is_none() {
        return Err(error::ErrorBadRequest("User Doesn't exists"));
    }

    let mut updated_values: HashMap<String, String> = HashMap::new();
    updated_values.insert("username".to_string(), update_request.username.clone());
    updated_values.insert("password_hash".to_string(), update_request.password.clone());

    let result = crate::tables::table::update_row(USERS_TABLE, &update_request.id, updated_values, &state).await;

    match result {
        Ok(_) => {
            Ok(HttpResponse::Ok().json("User updated successfully"))
        }
        Err(e) => {
            eprintln!("Failed to update user: {}", e);
            Err(error::ErrorInternalServerError("Failed to update user"))
        }
    }
}

// Handler for deleting a user
#[derive(Deserialize)]
pub struct DeleteUserRequest {
    pub id: String,
}

async fn delete_user(req: web::Json<DeleteUserRequest>, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let delete_request = req.into_inner();

    let result = crate::tables::table::delete_row(&USERS_TABLE.to_string(), &delete_request.id, &state).await;

    match result {
        Ok(_) => {
            Ok(HttpResponse::Ok().json("User deleted successfully"))
        }
        Err(e) => {
            eprintln!("Failed to delete user: {}", e);
            Err(error::ErrorInternalServerError("Failed to delete user"))
        }
    }
}

pub async fn create_default_user(app_state: &AppState) -> std::io::Result<()> {
    info!("Creating default admin user");
    let schema_locked = app_state.schema.lock().unwrap();
    if !schema_locked.tables.contains_key(&format!("{}_initial", crate::USERS_TABLE)) {
        drop(schema_locked); // Release the lock before creating the table

        // Define user table schema
        let user_table = schema::schema::Table {
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
                }
            ],
        };

        // Create the users table
        create_table(&user_table, &Data::new(app_state.clone()))?;
        info!("Created users table");
    }

    info!("Checking for admin user");
    let query = format!("SELECT * FROM {} WHERE username = \"admin\"", crate::USERS_TABLE);
    match sql_parser(query.as_bytes()) {
        Ok(ast_nodes) => {
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
                                                // No admin user exists, create one
                                                let insert_query = format!(
                                                    "INSERT INTO {} (username, password_hash) VALUES ( \"admin\", \"admin\")",
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
pub async fn authenticate_request(req: &HttpRequest, state: &web::Data<AppState>) -> Result<(), actix_web::Error> {
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
                                    debug!("Attempting to authenticate user: {}", username);

                                    // Fetch user from database
                                    match fetch_user_from_db(username, state).await {
                                        Some(user) => {
                                            debug!("User found in database");

                                            // Password check
                                            if user.password_hash == password {
                                                info!("Authentication successful for user: {}", username);

                                                // Insert user into request extensions
                                                req.extensions_mut().insert(user);
                                                return Ok(());
                                            } else {
                                                warn!("Password mismatch for user: {}", username);
                                            }
                                        },
                                        None => {
                                            warn!("No user found for username: {}", username);
                                        }
                                    }
                                },
                                Err(_) => {
                                    error!("Failed to convert decoded credentials to UTF-8");
                                }
                            }
                        },
                        Err(e) => {
                            error!("Base64 decoding failed: {:?}", e);
                        }
                    }
                },
                Err(_) => {
                    error!("Failed to convert Authorization header to string");
                }
            }
        },
        None => {
            debug!("No Authorization header present in the request");
        }
    }

    // If we've reached this point, authentication has failed
    Err(ErrorUnauthorized("Authentication failed"))
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

    cfg.service(
        web::resource("/api/update-user")
            .app_data(web::JsonConfig::default().limit(4096)) // Limit request body size
            .route(web::post().to(update_user)),
    );

    cfg.service(
        web::resource("/api/delete-user")
            .app_data(web::JsonConfig::default().limit(4096)) // Limit request body size
            .route(web::post().to(delete_user)),
    );
}