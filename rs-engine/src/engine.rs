//! Matching Engine event loop, Redis Stream WAL consumer, and fast-forward replay dispatcher.

use std::sync::Arc;
use tokio::sync::Mutex;
use rs_shared::*;
use crate::handlers;
use crate::state::EngineState;

/// Wait for either SIGINT (Ctrl+C) or SIGTERM on Unix systems.
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => { sig.recv().await; }
            Err(e) => eprintln!("Warning: failed to install SIGTERM handler: {e}"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Dispatch a single event payload to its corresponding handler.
/// If `publisher` is `None`, the engine executes in replay mode (mutating state without publishing duplicate replies).
pub async fn dispatch_event(
    state:     &mut EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    channel:   &str,
    data:      &str,
) {
    match channel {
        CH_SIGNUP => {
            if let Ok(msg) = serde_json::from_str::<SignupMsg>(data) {
                handlers::handle_signup(state, publisher, msg).await;
            }
        }
        CH_ONRAMP => {
            if let Ok(msg) = serde_json::from_str::<OnrampMsg>(data) {
                handlers::handle_onramp(state, publisher, msg).await;
            }
        }
        CH_DEPOSIT => {
            if let Ok(msg) = serde_json::from_str::<DepositMsg>(data) {
                handlers::handle_deposit(state, publisher, msg).await;
            }
        }
        CH_BALANCE => {
            if let Ok(msg) = serde_json::from_str::<BalanceQueryMsg>(data) {
                handlers::handle_balance(state, publisher, msg).await;
            }
        }
        CH_ORDER => {
            if let Ok(msg) = serde_json::from_str::<OrderMsg>(data) {
                handlers::handle_order(state, publisher, msg).await;
            }
        }
        CH_CANCEL => {
            if let Ok(msg) = serde_json::from_str::<CancelMsg>(data) {
                handlers::handle_cancel(state, publisher, msg).await;
            }
        }
        CH_ORDERBOOK => {
            if let Ok(msg) = serde_json::from_str::<OrderbookQueryMsg>(data) {
                handlers::handle_get_orderbook(state, publisher, msg).await;
            }
        }
        CH_OPEN_ORDERS => {
            if let Ok(msg) = serde_json::from_str::<OpenOrdersQueryMsg>(data) {
                handlers::handle_get_open_orders(state, publisher, msg).await;
            }
        }
        CH_RESET => {
            if let Ok(msg) = serde_json::from_str::<ResetMsg>(data) {
                handlers::handle_reset(state, publisher, msg).await;
            }
        }
        unknown => {
            eprintln!("rs-engine: unknown channel: {unknown}");
        }
    }
}

/// Replay all stream events that occurred strictly after `last_id` through the state machine.
/// Returns the updated stream ID after replaying.
#[allow(deprecated)]
pub async fn replay_events_since(
    state: &mut EngineState,
    connection: &mut redis::aio::Connection,
    last_id: &str,
) -> Result<String, redis::RedisError> {
    let start_arg = if last_id == "0-0" || last_id.is_empty() {
        "-".to_string()
    } else {
        format!("({last_id}") // exclusive range in Redis
    };

    let mut cmd = redis::cmd("XRANGE");
    cmd.arg(STREAM_EVENTS).arg(start_arg).arg("+");

    let reply: redis::streams::StreamRangeReply = cmd.query_async(connection).await?;
    let mut current_id = last_id.to_string();

    for record in reply.ids {
        current_id = record.id;
        let channel = match record.map.get("channel") {
            Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
            None => continue,
        };
        let data = match record.map.get("data") {
            Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
            None => continue,
        };

        // Replay in memory without sending replies (publisher is None)
        dispatch_event(state, None, &channel, &data).await;
    }

    Ok(current_id)
}

/// Run the engine loop listening for incoming events via Redis Streams.
#[allow(deprecated)]
pub async fn run_engine(
    state:         Arc<Mutex<EngineState>>,
    listener:      redis::aio::Connection,
    publisher:     redis::aio::MultiplexedConnection,
) {
    run_engine_with_db(state, listener, publisher, None, "0-0".to_string()).await;
}

/// Run the engine with snapshot persistence and fast-forward replay recovery.
#[allow(deprecated)]
pub async fn run_engine_with_db(
    state:         Arc<Mutex<EngineState>>,
    mut listener:  redis::aio::Connection,
    mut publisher: redis::aio::MultiplexedConnection,
    db:            Option<sqlx::PgPool>,
    mut last_id:   String,
) {
    // 1. If starting from 0-0 and DB is provided, check for latest snapshot
    if last_id == "0-0"
        && let Some(ref pool) = db
    {
        match crate::snapshot::load_latest_snapshot(pool).await {
            Ok(Some((snap_id, snap_state))) => {
                println!("rs-engine: restored snapshot at stream ID {snap_id}");
                let mut s = state.lock().await;
                *s = snap_state;
                last_id = snap_id;
            }
            Ok(None) => {}
            Err(e) => eprintln!("rs-engine: error loading snapshot: {e}"),
        }
    }

    // 2. Replay all stream events since last_id
    {
        let mut s = state.lock().await;
        match replay_events_since(&mut s, &mut listener, &last_id).await {
            Ok(new_last) => {
                if new_last != last_id {
                    println!("rs-engine: replayed events up to stream ID {new_last}");
                    last_id = new_last;
                }
            }
            Err(e) => {
                eprintln!("rs-engine: replay check: {e}");
            }
        }
    }

    println!("rs-engine: listening on stream {STREAM_EVENTS} starting from {last_id} (Ctrl+C to stop)...");

    let mut events_since_snapshot = 0_usize;
    const SNAPSHOT_INTERVAL: usize = 1000;

    loop {
        // Read new events strictly greater than last_id
        let mut cmd = redis::cmd("XREAD");
        cmd.arg("BLOCK").arg(1000_u64)
           .arg("STREAMS").arg(STREAM_EVENTS)
           .arg(&last_id);

        let query_fut = async {
            let res: redis::RedisResult<Option<redis::streams::StreamReadReply>> =
                cmd.query_async(&mut listener).await;
            res
        };

        let result = tokio::select! {
            _ = wait_for_shutdown_signal() => {
                println!("\nrs-engine: Received shutdown signal. Exiting engine gracefully...");
                break;
            }
            res = query_fut => res,
        };

        let read_reply = match result {
            Ok(Some(reply)) => reply,
            Ok(None)        => continue, // timeout (1s)
            Err(e)          => {
                eprintln!("rs-engine: Redis stream error: {e}");
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        let mut s = state.lock().await;

        for key in read_reply.keys {
            for record in key.ids {
                last_id = record.id;
                let channel = match record.map.get("channel") {
                    Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
                    None => continue,
                };
                let data = match record.map.get("data") {
                    Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
                    None => continue,
                };

                let is_reset = channel == CH_RESET;

                // Dispatch to handler with live publisher
                dispatch_event(&mut s, Some(&mut publisher), &channel, &data).await;

                if is_reset {
                    // On reset: wipe snapshots and reset stream
                    if let Some(ref pool) = db {
                        let _ = crate::snapshot::truncate_snapshots(pool).await;
                    }
                    let _: Result<(), _> = redis::cmd("DEL").arg(STREAM_EVENTS).query_async(&mut publisher).await;
                    last_id = "0-0".to_string();
                    events_since_snapshot = 0;
                    continue;
                }

                events_since_snapshot += 1;
                if events_since_snapshot >= SNAPSHOT_INTERVAL {
                    if let Some(ref pool) = db {
                        if let Err(e) = crate::snapshot::save_snapshot(pool, &last_id, &s).await {
                            eprintln!("rs-engine: failed to save snapshot: {e}");
                        } else {
                            println!("rs-engine: saved snapshot at stream ID {last_id}");
                        }
                    }
                    events_since_snapshot = 0;
                }
            }
        }
    }

    println!("rs-engine: Shutdown complete.");
}
