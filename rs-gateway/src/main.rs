pub mod auth;
pub mod dto;
pub mod redis;
pub mod routes;
pub mod state;

// Backward-compatibility aliases for any existing internal paths
pub mod types {
    pub use crate::auth::User;
    pub use crate::auth::Claims;
    pub use crate::dto::*;
}
pub mod middleware {
    pub use crate::auth::*;
}

use std::collections::HashMap;
use std::sync::Arc;
use actix_web::{web, App, HttpServer};
use tokio::sync::Mutex;
pub use state::AppState;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = ::redis::Client::open(redis_url.as_str())
        .expect("Invalid Redis URL");

    let queue_id = uuid::Uuid::new_v4().to_string();
    let reply_queue = format!("{}{}", rs_shared::REPLY_PREFIX, queue_id);

    let pending: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Dedicated connection for blocking brpop response queue
    #[allow(deprecated)]
    let listener = client
        .get_async_connection()
        .await
        .expect("Failed to connect to Redis (listener)");

    // Multiplexed connection for lpush requests
    let publisher = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis (publisher)");

    // Spawn response queue listener background task
    redis::start_response_listener(listener, Arc::clone(&pending), reply_queue);

    let app_state = web::Data::new(AppState::new(publisher, pending, queue_id));

    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    println!("rs-gateway running on http://{host}:{port}");

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .configure(routes::configure)
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
