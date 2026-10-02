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
    publisher: &mut redis::aio::MultiplexedConnection,
    queue_id:  &str,
    payload:   &T,
) {
    let channel = format!("{}{}", REPLY_PREFIX, queue_id);
    let json    = serde_json::to_string(payload).unwrap();
    let _: ()   = publisher.lpush(channel, json).await.unwrap();
}

pub(crate) async fn reply_error(
    publisher:   &mut redis::aio::MultiplexedConnection,
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
    publisher:  &mut redis::aio::MultiplexedConnection,
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
