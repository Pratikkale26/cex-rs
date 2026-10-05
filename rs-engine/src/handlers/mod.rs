pub mod account;
pub mod admin;
pub mod market;
pub mod order;

pub use account::*;
pub use admin::*;
pub use market::*;
pub use order::*;

use redis::AsyncCommands;
use rs_shared::*;

// ── Common reply helpers ────────────────────────────────────────────────────

pub(crate) async fn reply<T: serde::Serialize>(
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    queue_id:  &str,
    payload:   &T,
) {
    if let Some(publ) = publisher {
        let channel = format!("{}{}", REPLY_PREFIX, queue_id);
        let json    = serde_json::to_string(payload).unwrap();
        let _: ()   = publ.lpush(channel, json).await.unwrap();
    }
}

pub(crate) async fn emit_execution(
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    event: &ExecutionEvent,
) {
    if let Some(publ) = publisher {
        let json = serde_json::to_string(event).unwrap();
        let mut cmd = redis::cmd("XADD");
        cmd.arg(STREAM_EXECUTIONS)
            .arg("*")
            .arg("data")
            .arg(&json);
        let _: Result<String, _> = cmd.query_async(publ).await;
    }
}

pub(crate) async fn reply_error(
    publisher:   Option<&mut redis::aio::MultiplexedConnection>,
    queue_id:    &str,
    identifier:  &str,
    error:       &str,
    status_code: u16,
) {
    reply(publisher, queue_id, &OrderReply {
        identifier:  identifier.to_string(),
        order_id:    None,
        status:      None,
        error:       Some(error.to_string()),
        status_code: Some(status_code),
    }).await;
}

pub(crate) async fn reply_cancel_error(
    publisher:  Option<&mut redis::aio::MultiplexedConnection>,
    queue_id:   &str,
    identifier: &str,
    error:      &str,
) {
    reply(publisher, queue_id, &CancelReply {
        identifier:    identifier.to_string(),
        order_id:      None,
        remaining_qty: None,
        message:       None,
        error:         Some(error.to_string()),
    }).await;
}
