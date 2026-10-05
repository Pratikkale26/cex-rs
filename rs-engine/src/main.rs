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
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgrespassword@127.0.0.1:5432/cex".to_string());

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok();

    if db_pool.is_some() {
        println!("rs-engine: connected to PostgreSQL (snapshots enabled).");
    } else {
        println!("rs-engine: running in memory-only mode (PostgreSQL not connected).");
    }

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .expect("Invalid Redis URL");

    // Dedicated connection for blocking stream read.
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

    println!("rs-engine: connected to Redis, listening on streams...");
    engine::run_engine_with_db(state, listener, publisher, db_pool, "0-0".to_string()).await;
}
