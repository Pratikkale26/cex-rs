//! One async handler per Redis channel.
//!
//! # Balance accounting — the lock-first rule
//!
//! We lock funds BEFORE calling add_order():
//!   - bid: lock `price * qty` USD  (available → locked)
//!   - ask: lock `qty` SOL          (available → locked)
//!
//! Because both sides always have their payment in `locked` by the time
//! the matching happens, the fill handler can ALWAYS do:
//!   buyer.usd.locked  -= value        (never use available)
//!   seller.sol.locked -= fill_qty     (never use available)
//!
//! This is simpler and more correct than the TypeScript approach which
//! needed a takerSide field to distinguish locked vs available.

use redis::AsyncCommands;
use rs_shared::*;
use crate::orderbook::{MatchResult, OrderbookError, Side, Trade};
use crate::state::EngineState;

// ── Helper: push reply to the gateway's reply queue ─────────────────────────

async fn reply<T: serde::Serialize>(
    publisher: &mut redis::aio::MultiplexedConnection,
    queue_id:  &str,
    payload:   &T,
) {
    let channel = format!("{}{}", REPLY_PREFIX, queue_id);
    let json    = serde_json::to_string(payload).unwrap();
    let _: ()   = publisher.lpush(channel, json).await.unwrap();
}

async fn reply_error(
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

async fn reply_cancel_error(
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

// ── Handlers ─────────────────────────────────────────────────────────────────

pub async fn handle_signup(
    state:     &mut EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       SignupMsg,
) {
    // Initialise zero balances for the new user.
    state.usd_balance.entry(msg.user_id).or_default();
    state.stock_balance.entry(msg.user_id).or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   state.usd_balance[&msg.user_id].clone(),
        stock_balance: state.stock_balance[&msg.user_id].clone(),
    }).await;
}

pub async fn handle_onramp(
    state:     &mut EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       OnrampMsg,
) {
    state.usd_mut(msg.user_id).available += msg.qty;

    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}

pub async fn handle_deposit(
    state:     &mut EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       DepositMsg,
) {
    let symbol_balance = state.stock_balance
        .entry(msg.user_id)
        .or_default()
        .entry(msg.symbol.clone())
        .or_default();

    symbol_balance.available += msg.qty;

    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}

pub async fn handle_balance(
    state:     &EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       BalanceQueryMsg,
) {
    let usd   = state.usd_balance.get(&msg.user_id).cloned().unwrap_or_default();
    let stock = state.stock_balance.get(&msg.user_id).cloned().unwrap_or_default();

    reply(publisher, &msg.queue_id, &BalanceReply {
        identifier:    msg.identifier,
        usd_balance:   usd,
        stock_balance: stock,
    }).await;
}

pub async fn handle_order(
    state:     &mut EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
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

    let mut resting_order_id: Option<u64> = None;

    for result in results {
        match result {
            MatchResult::Trade(trade)       => apply_fill(state, &trade),
            MatchResult::OrderAccepted(acc) => resting_order_id = Some(acc.order_id),
        }
    }

    // ── Reply ─────────────────────────────────────────────────────────────

    let (order_id, status) = match resting_order_id {
        Some(id) => (Some(id), "open"),
        None     => (None,     "filled"),
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
    publisher: &mut redis::aio::MultiplexedConnection,
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
            use crate::orderbook::Side;
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

pub async fn handle_get_orderbook(
    state:     &EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
    msg:       OrderbookQueryMsg,
) {
    if msg.asset != "sol" {
        // Unsupported asset — return empty book.
        reply(publisher, &msg.queue_id, &OrderbookReply {
            identifier: msg.identifier,
            orderbook:  OrderbookData { bids: vec![], asks: vec![] },
        }).await;
        return;
    }

    let snap = state.sol_orderbook.get_state();

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

pub async fn handle_get_open_orders(
    state:     &EngineState,
    publisher: &mut redis::aio::MultiplexedConnection,
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
