mod types;
mod middleware;
mod routes;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use actix_web::{web, App, HttpServer};
use tokio::sync::{oneshot, Mutex};
use types::User;

pub struct AppState {
    pub users:           Mutex<Vec<User>>,
    pub user_index:      AtomicU64,
    pub redis_publisher: Mutex<redis::aio::MultiplexedConnection>,
    pub pending:         Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
    pub queue_id:        String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .expect("Invalid Redis URL");

    let queue_id = uuid::Uuid::new_v4().to_string();
    let reply_queue = format!("{}{}", rs_shared::REPLY_PREFIX, queue_id);

    let pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Dedicated connection for blocking brpop response queue
    let mut listener = client
        .get_async_connection()
        .await
        .expect("Failed to connect to Redis (listener)");

    // Multiplexed connection for lpush requests
    let publisher = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis (publisher)");

    // Spawn response queue listener background task
    let pending_clone = Arc::clone(&pending);
    let reply_queue_clone = reply_queue.clone();
    tokio::spawn(async move {
        loop {
            let mut cmd = redis::cmd("BRPOP");
            cmd.arg(&reply_queue_clone).arg(1_u64);

            let result: redis::RedisResult<Option<(String, String)>> =
                cmd.query_async(&mut listener).await;

            match result {
                Ok(Some((_channel, data))) => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) {
                        if let Some(id) = v.get("identifier").and_then(|x| x.as_str()) {
                            let mut map = pending_clone.lock().await;
                            if let Some(tx) = map.remove(id) {
                                let _ = tx.send(data);
                            }
                        }
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("rs-gateway response listener error: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
    });

    let app_state = web::Data::new(AppState {
        users:           Mutex::new(Vec::new()),
        user_index:      AtomicU64::new(0),
        redis_publisher: Mutex::new(publisher),
        pending,
        queue_id,
    });

    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    println!("rs-gateway running on http://{host}:{port}");

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/signup", web::post().to(routes::signup))
            .route("/signin", web::post().to(routes::signin))
            .route("/balance", web::get().to(routes::get_balance))
            .route("/onramp", web::post().to(routes::onramp))
            .route("/deposit/{asset}", web::post().to(routes::deposit))
            .route("/order", web::post().to(routes::place_order))
            .route("/order/{order_id}", web::delete().to(routes::cancel_order))
            .route("/orderbook/{asset}", web::get().to(routes::get_orderbook))
            .route("/orders/open", web::get().to(routes::get_open_orders))
            .route("/reset", web::post().to(routes::reset))
    })
    .bind((host.as_str(), port))?
    .run()
    .await
}
