use rs_shared::*;
use crate::state::EngineState;
use super::reply;

pub async fn handle_reset(
    state:     &mut EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       ResetMsg,
) {
    state.reset();

    reply(publisher, &msg.queue_id, &GenericReply {
        identifier: msg.identifier,
        message:    "Reset successful".to_string(),
    }).await;
}
