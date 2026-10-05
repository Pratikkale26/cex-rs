use std::collections::HashMap;
use std::sync::Arc;
use actix_web::web;
use tokio::sync::{oneshot, Mutex};
use crate::state::AppState;

/// Spawns the background BRPOP response listener task for this gateway instance.
#[allow(deprecated)]
pub fn start_response_listener(
    mut listener: redis::aio::Connection,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
    reply_queue: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut cmd = redis::cmd("BRPOP");
            cmd.arg(&reply_queue).arg(1_u64);

            let result: redis::RedisResult<Option<(String, String)>> =
                cmd.query_async(&mut listener).await;

            match result {
                Ok(Some((_channel, data))) => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data)
                        && let Some(id) = v.get("identifier").and_then(|x| x.as_str())
                    {
                        let mut map = pending.lock().await;
                        if let Some(tx) = map.remove(id) {
                            let _ = tx.send(data);
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
    })
}

/// Send a JSON payload to a Redis channel and await the correlated response via oneshot.
pub async fn send_and_wait<T: serde::de::DeserializeOwned, M: serde::Serialize>(
    state: &web::Data<AppState>,
    channel: &str,
    msg: &M,
    identifier: &str,
) -> Result<T, actix_web::Error> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut pending = state.pending.lock().await;
        pending.insert(identifier.to_string(), tx);
    }

    let json_str = serde_json::to_string(msg)
        .map_err(actix_web::error::ErrorInternalServerError)?;

    {
        let mut publisher = state.redis_publisher.lock().await;
        let mut cmd = redis::cmd("XADD");
        cmd.arg(rs_shared::STREAM_EVENTS)
            .arg("*")
            .arg("channel").arg(channel)
            .arg("data").arg(&json_str);

        let _: String = cmd.query_async(&mut *publisher).await
            .map_err(actix_web::error::ErrorInternalServerError)?;
    }

    let reply_str = rx.await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    serde_json::from_str::<T>(&reply_str)
        .map_err(actix_web::error::ErrorInternalServerError)
}
