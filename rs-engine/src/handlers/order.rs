use rs_shared::*;
use crate::orderbook::{MatchResult, OrderbookError, Side, Trade};
use crate::state::EngineState;
use super::{emit_execution, reply, reply_cancel_error, reply_error};

pub async fn handle_order(
    state:     &mut EngineState,
    mut publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       OrderMsg,
) {
    // Only SOL is supported.
    if msg.asset != "sol" {
        reply_error(publisher, &msg.queue_id, &msg.identifier,
                    "Only SOL orders are supported", 400).await;
        return;
    }

    let side = match msg.side.as_str() {
        "bid" => Side::Bid,
        "ask" => Side::Ask,
        _ => {
            reply_error(publisher, &msg.queue_id, &msg.identifier,
                        "side must be bid or ask", 400).await;
            return;
        }
    };

    // Validate price / qty BEFORE locking funds.
    if msg.price <= 0 || msg.qty <= 0 {
        reply_error(publisher, &msg.queue_id, &msg.identifier,
                    "price and qty must be positive numbers", 500).await;
        return;
    }

    let price = msg.price as u64;
    let qty = msg.qty as u64;
    let cost = price * qty;

    // ── Lock funds upfront ────────────────────────────────────────────────
    // Both taker and maker go through this path, so fills can always
    // deduct from `locked` (not `available`).

    match side {
        Side::Bid => {
            let usd = state.usd_mut(msg.user_id);
            if usd.available < cost {
                reply_error(publisher, &msg.queue_id, &msg.identifier,
                            "Insufficient USD", 400).await;
                return;
            }
            usd.available -= cost;
            usd.locked    += cost;
        }
        Side::Ask => {
            let sol = state.sol_mut(msg.user_id);
            if sol.available < qty {
                reply_error(publisher, &msg.queue_id, &msg.identifier,
                            "Insufficient SOL", 400).await;
                return;
            }
            sol.available -= qty;
            sol.locked    += qty;
        }
    }

    // ── Place order ───────────────────────────────────────────────────────

    let results = state.sol_orderbook.add_order(msg.user_id, side, price, qty)
        .expect("price/qty already validated above");

    // ── Process results ───────────────────────────────────────────────────

    let mut resting_order: Option<crate::orderbook::OrderAccepted> = None;

    for result in results {
        match result {
            MatchResult::Trade(trade) => {
                apply_fill(state, &trade);
                let maker_rem = state.sol_orderbook.get_order(trade.maker_order_id)
                    .map(|o| o.remaining_qty)
                    .unwrap_or(0);

                emit_execution(publisher.as_deref_mut(), &ExecutionEvent::TradeExecuted {
                    maker_order_id:      trade.maker_order_id,
                    taker_order_id:      trade.taker_order_id,
                    buyer_id:            trade.buyer,
                    seller_id:           trade.seller,
                    market:              "SOL_USD".to_string(),
                    price:               trade.price,
                    qty:                 trade.qty,
                    quote_amount:        trade.price * trade.qty,
                    maker_remaining_qty: maker_rem,
                    taker_remaining_qty: 0,
                }).await;
            }
            MatchResult::OrderAccepted(acc) => {
                resting_order = Some(acc);
            }
        }
    }

    // ── Reply & Record Created Order ──────────────────────────────────────

    let (order_id, status) = match resting_order {
        Some(acc) => {
            emit_execution(publisher.as_deref_mut(), &ExecutionEvent::OrderCreated {
                order_id:      acc.order_id,
                user_id:       acc.user_id,
                market:        "SOL_USD".to_string(),
                side:          msg.side.clone(),
                price:         acc.price,
                original_qty:  qty,
                remaining_qty: acc.remaining_qty,
                status:        "open".to_string(),
            }).await;
            (Some(acc.order_id), "open")
        }
        None => (None, "filled"),
    };

    reply(publisher, &msg.queue_id, &OrderReply {
        identifier:  msg.identifier,
        order_id,
        status:      Some(status.to_string()),
        error:       None,
        status_code: None,
    }).await;
}

/// Apply a fill to the balance ledger.
///
/// Because funds are locked before the order is placed, BOTH buyer and seller
/// always have their payment in `locked` at this point — no maker/taker distinction needed.
fn apply_fill(state: &mut EngineState, trade: &Trade) {
    let value = trade.price * trade.qty;

    // Buyer: pays USD (from locked), receives SOL (into available).
    state.usd_mut(trade.buyer).locked          -= value;
    state.sol_mut(trade.buyer).available        += trade.qty;

    // Seller: gives SOL (from locked), receives USD (into available).
    state.sol_mut(trade.seller).locked          -= trade.qty;
    state.usd_mut(trade.seller).available       += value;
}

pub async fn handle_cancel(
    state:     &mut EngineState,
    mut publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       CancelMsg,
) {
    match state.sol_orderbook.cancel_order(msg.order_id, msg.user_id) {
        Err(OrderbookError::OrderNotFound) => {
            reply_cancel_error(publisher, &msg.queue_id, &msg.identifier,
                               "Open order not found").await;
        }
        Err(OrderbookError::NotOrderOwner) => {
            reply_cancel_error(publisher, &msg.queue_id, &msg.identifier,
                               "Not order owner").await;
        }
        Err(_) => unreachable!("cancel_order only returns the above errors"),
        Ok(cancelled) => {
            // Refund the locked funds for the remaining (unfilled) qty.
            // locked = remaining_qty * price  (for bids)
            // locked = remaining_qty           (for asks)
            match cancelled.side {
                Side::Bid => {
                    let refund = cancelled.price * cancelled.remaining_qty;
                    let usd    = state.usd_mut(cancelled.user_id);
                    usd.locked    -= refund;
                    usd.available += refund;
                }
                Side::Ask => {
                    let sol = state.sol_mut(cancelled.user_id);
                    sol.locked    -= cancelled.remaining_qty;
                    sol.available += cancelled.remaining_qty;
                }
            }

            emit_execution(publisher.as_deref_mut(), &ExecutionEvent::OrderCancelled {
                order_id:      cancelled.order_id,
                user_id:       cancelled.user_id,
                remaining_qty: cancelled.remaining_qty,
            }).await;

            reply(publisher, &msg.queue_id, &CancelReply {
                identifier:    msg.identifier,
                order_id:      Some(cancelled.order_id),
                remaining_qty: Some(cancelled.remaining_qty),
                message:       Some("Order cancelled".to_string()),
                error:         None,
            }).await;
        }
    }
}

pub async fn handle_get_open_orders(
    state:     &EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       OpenOrdersQueryMsg,
) {
    let orders = state.sol_orderbook
        .get_user_orders(msg.user_id)
        .into_iter()
        .map(|o| OpenOrderInfo {
            order_id:      o.order_id,
            user_id:       o.user_id,
            side:          match o.side { Side::Bid => "bid", Side::Ask => "ask" }.to_string(),
            price:         o.price,
            remaining_qty: o.remaining_qty,
        })
        .collect();

    reply(publisher, &msg.queue_id, &OpenOrdersReply {
        identifier: msg.identifier,
        orders,
    }).await;
}
