//! rs-engine — the Redis-driven matching engine.
//!
//! Architecture:
//!   1. brpop on all known channels in a loop.
//!   2. Deserialise the JSON payload.
//!   3. Acquire a lock on EngineState, call the handler, release lock.
//!   4. Handler publishes the reply to the gateway's response queue.
//!
//! Two separate Redis connections are used:
//!   - `listener`  — used only for brpop (blocking ops need a dedicated conn)
//!   - `publisher` — multiplexed, used for lpush replies

mod orderbook;
mod state;
mod handlers;

use std::sync::Arc;
use tokio::sync::Mutex;
use rs_shared::*;
use state::EngineState;

#[tokio::main]
async fn main() {
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .expect("Invalid Redis URL");

    // Dedicated connection for blocking brpop.
    let mut listener = client
        .get_async_connection()
        .await
        .expect("Failed to connect to Redis (listener)");

    // Multiplexed connection for lpush replies.
    let mut publisher = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect to Redis (publisher)");

    let state = Arc::new(Mutex::new(EngineState::new()));

    println!("rs-engine: connected to Redis, listening...");

    // ── Event loop ───────────────────────────────────────────────────────────

    let channels: &[&str] = &[
        CH_SIGNUP,
        CH_ORDER,
        CH_CANCEL,
        CH_BALANCE,
        CH_ONRAMP,
        CH_DEPOSIT,
        CH_ORDERBOOK,
        CH_OPEN_ORDERS,
        CH_RESET,
    ];

    loop {
        // Build the BRPOP command for all channels with a 1-second timeout.
        let mut cmd = redis::cmd("BRPOP");
        for ch in channels { cmd.arg(ch); }
        cmd.arg(1_u64); // timeout in seconds

        let result: redis::RedisResult<Option<(String, String)>> =
            cmd.query_async(&mut listener).await;

        let (channel, data) = match result {
            Ok(Some(pair)) => pair,
            Ok(None)       => continue, // timeout, no message
            Err(e)         => {
                eprintln!("rs-engine: Redis error: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut s = state.lock().await;

        match channel.as_str() {
            CH_SIGNUP => {
                if let Ok(msg) = serde_json::from_str::<SignupMsg>(&data) {
                    handlers::handle_signup(&mut s, &mut publisher, msg).await;
                }
            }
            CH_ONRAMP => {
                if let Ok(msg) = serde_json::from_str::<OnrampMsg>(&data) {
                    handlers::handle_onramp(&mut s, &mut publisher, msg).await;
                }
            }
            CH_DEPOSIT => {
                if let Ok(msg) = serde_json::from_str::<DepositMsg>(&data) {
                    handlers::handle_deposit(&mut s, &mut publisher, msg).await;
                }
            }
            CH_BALANCE => {
                if let Ok(msg) = serde_json::from_str::<BalanceQueryMsg>(&data) {
                    handlers::handle_balance(&s, &mut publisher, msg).await;
                }
            }
            CH_ORDER => {
                if let Ok(msg) = serde_json::from_str::<OrderMsg>(&data) {
                    handlers::handle_order(&mut s, &mut publisher, msg).await;
                }
            }
            CH_CANCEL => {
                if let Ok(msg) = serde_json::from_str::<CancelMsg>(&data) {
                    handlers::handle_cancel(&mut s, &mut publisher, msg).await;
                }
            }
            CH_ORDERBOOK => {
                if let Ok(msg) = serde_json::from_str::<OrderbookQueryMsg>(&data) {
                    handlers::handle_get_orderbook(&s, &mut publisher, msg).await;
                }
            }
            CH_OPEN_ORDERS => {
                if let Ok(msg) = serde_json::from_str::<OpenOrdersQueryMsg>(&data) {
                    handlers::handle_get_open_orders(&s, &mut publisher, msg).await;
                }
            }
            CH_RESET => {
                if let Ok(msg) = serde_json::from_str::<ResetMsg>(&data) {
                    handlers::handle_reset(&mut s, &mut publisher, msg).await;
                }
            }
            unknown => {
                eprintln!("rs-engine: unknown channel: {unknown}");
            }
        }
    }
}
