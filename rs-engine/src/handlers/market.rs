use rs_shared::*;
use crate::state::EngineState;
use super::reply;

pub async fn handle_get_orderbook(
    state:     &EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       OrderbookQueryMsg,
) {
    let ob = match state.get_orderbook(&msg.asset) {
        Some(b) => b,
        None => {
            // Unsupported asset — return empty book.
            reply(publisher, &msg.queue_id, &OrderbookReply {
                identifier: msg.identifier,
                orderbook:  OrderbookData { bids: vec![], asks: vec![] },
            }).await;
            return;
        }
    };

    let snap = ob.get_state();

    let bids = snap.bids.into_iter()
        .map(|l| OrderbookLevel { price: l.price, qty: l.qty })
        .collect();
    let asks = snap.asks.into_iter()
        .map(|l| OrderbookLevel { price: l.price, qty: l.qty })
        .collect();

    reply(publisher, &msg.queue_id, &OrderbookReply {
        identifier: msg.identifier,
        orderbook:  OrderbookData { bids, asks },
    }).await;
}
