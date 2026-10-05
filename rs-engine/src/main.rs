//! rs-engine — the Redis-driven matching engine.
//!
//! Architecture:
//!   1. brpop on all known channels in a loop (see `engine.rs`).
//!   2. Deserialise the JSON payload.
//!   3. Acquire a lock on EngineState, call the handler, release lock.
//!   4. Handler publishes the reply to the gateway's response queue.

use std::sync::Arc;
use tokio::sync::Mutex;
use rs_engine::state::EngineState;
use rs_engine::engine;

#[tokio::main]
async fn main() {
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .expect("Invalid Redis URL");

    // Dedicated connection for blocking brpop.
    #[allow(deprecated)]
    let listener = client
        .get_async_connection()
        .await
        .expect("Failed to connect to Redis (listener)");

    // Multiplexed connection for lpush replies.
    let publisher = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis (publisher)");

    let state = Arc::new(Mutex::new(EngineState::new()));

    println!("rs-engine: connected to Redis, listening...");
    engine::run_engine(state, listener, publisher).await;
}
