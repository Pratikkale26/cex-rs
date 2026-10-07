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

    // Validate qty.
    if msg.qty <= 0 {
        reply_error(publisher, &msg.queue_id, &msg.identifier,
                    "qty must be a positive number", 400).await;
        return;
    }
    let qty = msg.qty as u64;

    match msg.order_type {
        OrderType::Limit => {
            if msg.price <= 0 {
                reply_error(publisher, &msg.queue_id, &msg.identifier,
                            "price must be positive for limit orders", 400).await;
                return;
            }
            let price = msg.price as u64;
            let cost = price * qty;

            // ── Lock funds upfront ────────────────────────────────────────
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

            // ── Place limit order ─────────────────────────────────────────
            let results = state.sol_orderbook.add_order(msg.user_id, side, price, qty, msg.time_in_force)
                .expect("price/qty already validated above");

            let mut resting_order: Option<crate::orderbook::OrderAccepted> = None;
            let mut filled_qty = 0_u64;

            for result in results {
                match result {
                    MatchResult::Trade(trade) => {
                        apply_fill(state, &trade);
                        filled_qty += trade.qty;

                        // Price improvement refund for taker buyer:
                        if side == Side::Bid && price > trade.price {
                            let savings = (price - trade.price) * trade.qty;
                            let usd = state.usd_mut(msg.user_id);
                            usd.locked -= savings;
                            usd.available += savings;
                        }

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

            // Refund unfilled quantity for non-resting orders (IOC / FOK)
            let unfilled_qty = qty - filled_qty;
            if msg.time_in_force != TimeInForce::Gtc && unfilled_qty > 0 {
                match side {
                    Side::Bid => {
                        let refund = unfilled_qty * price;
                        let usd = state.usd_mut(msg.user_id);
                        usd.locked -= refund;
                        usd.available += refund;
                    }
                    Side::Ask => {
                        let sol = state.sol_mut(msg.user_id);
                        sol.locked -= unfilled_qty;
                        sol.available += unfilled_qty;
                    }
                }
            }

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
                None => {
                    let s = if filled_qty == qty {
                        "filled"
                    } else if filled_qty > 0 {
                        "partially_filled"
                    } else {
                        "unfilled"
                    };
                    (None, s)
                }
            };

            reply(publisher, &msg.queue_id, &OrderReply {
                identifier:  msg.identifier,
                order_id,
                status:      Some(status.to_string()),
                error:       None,
                status_code: None,
            }).await;
        }

        OrderType::Market => {
            // Determine slippage percentage: use user specified, or default to 5.0%
            let slippage = msg.slippage_pct.unwrap_or(5.0);

            match side {
                Side::Ask => {
                    let worst_price = if msg.price > 0 {
                        Some(msg.price as u64)
                    } else if let Some(best_bid) = state.sol_orderbook.best_bid() {
                        let floor = (best_bid as f64 * (1.0 - slippage / 100.0)).floor() as u64;
                        Some(floor.max(1))
                    } else {
                        None
                    };

                    // If FOK, check that entire qty can be filled within slippage:
                    if msg.time_in_force == TimeInForce::Fok {
                        let fillable = state.sol_orderbook.fillable_qty_for_market_sell(qty, worst_price);
                        if fillable < qty {
                            reply(publisher, &msg.queue_id, &OrderReply {
                                identifier:  msg.identifier,
                                order_id:    None,
                                status:      Some("unfilled".to_string()),
                                error:       None,
                                status_code: None,
                            }).await;
                            return;
                        }
                    }

                    // Market Sell: Lock `qty` SOL upfront.
                    let sol = state.sol_mut(msg.user_id);
                    if sol.available < qty {
                        reply_error(publisher, &msg.queue_id, &msg.identifier,
                                    "Insufficient SOL", 400).await;
                        return;
                    }
                    sol.available -= qty;
                    sol.locked    += qty;

                    let results = state.sol_orderbook.execute_market_order(msg.user_id, side, qty, worst_price)
                        .expect("qty validated");

                    let mut filled_qty = 0_u64;
                    for result in results {
                        if let MatchResult::Trade(trade) = result {
                            apply_fill(state, &trade);
                            filled_qty += trade.qty;

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
                    }

                    // Refund unsold SOL if book liquidity ran out or slippage limit was hit
                    let unfilled_qty = qty - filled_qty;
                    if unfilled_qty > 0 {
                        let sol = state.sol_mut(msg.user_id);
                        sol.locked    -= unfilled_qty;
                        sol.available += unfilled_qty;
                    }

                    let status = if filled_qty == qty {
                        "filled"
                    } else if filled_qty > 0 {
                        "partially_filled"
                    } else {
                        "unfilled"
                    };

                    reply(publisher, &msg.queue_id, &OrderReply {
                        identifier:  msg.identifier,
                        order_id:    None,
                        status:      Some(status.to_string()),
                        error:       None,
                        status_code: None,
                    }).await;
                }

                Side::Bid => {
                    let worst_price = if msg.price > 0 {
                        Some(msg.price as u64)
                    } else if let Some(best_ask) = state.sol_orderbook.best_ask() {
                        let cap = (best_ask as f64 * (1.0 + slippage / 100.0)).ceil() as u64;
                        Some(cap)
                    } else {
                        None
                    };

                    // Market Buy: Calculate required USD from available ask liquidity within slippage.
                    let (required_usd, fillable_qty) = state.sol_orderbook.quote_cost_for_market_buy(qty, worst_price);
                    if fillable_qty == 0 || (msg.time_in_force == TimeInForce::Fok && fillable_qty < qty) {
                        if msg.time_in_force == TimeInForce::Fok && fillable_qty < qty {
                            reply(publisher, &msg.queue_id, &OrderReply {
                                identifier:  msg.identifier,
                                order_id:    None,
                                status:      Some("unfilled".to_string()),
                                error:       None,
                                status_code: None,
                            }).await;
                            return;
                        }
                        reply_error(publisher, &msg.queue_id, &msg.identifier,
                                    "No ask liquidity available within slippage limit", 400).await;
                        return;
                    }

                    let usd = state.usd_mut(msg.user_id);
                    if usd.available < required_usd {
                        reply_error(publisher, &msg.queue_id, &msg.identifier,
                                    "Insufficient USD", 400).await;
                        return;
                    }
                    usd.available -= required_usd;
                    usd.locked    += required_usd;

                    let results = state.sol_orderbook.execute_market_order(msg.user_id, side, qty, worst_price)
                        .expect("qty validated");

                    let mut filled_qty = 0_u64;
                    let mut total_spent = 0_u64;

                    for result in results {
                        if let MatchResult::Trade(trade) = result {
                            apply_fill(state, &trade);
                            filled_qty  += trade.qty;
                            total_spent += trade.price * trade.qty;

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
                    }

                    // Refund unspent USD if any (e.g. partial fill)
                    let refund = required_usd.saturating_sub(total_spent);
                    if refund > 0 {
                        let usd = state.usd_mut(msg.user_id);
                        usd.locked    -= refund;
                        usd.available += refund;
                    }

                    let status = if filled_qty == qty {
                        "filled"
                    } else if filled_qty > 0 {
                        "partially_filled"
                    } else {
                        "unfilled"
                    };

                    reply(publisher, &msg.queue_id, &OrderReply {
                        identifier:  msg.identifier,
                        order_id:    None,
                        status:      Some(status.to_string()),
                        error:       None,
                        status_code: None,
                    }).await;
                }
            }
        }
    }
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

pub async fn handle_get_order_status(
    state:     &EngineState,
    publisher: Option<&mut redis::aio::MultiplexedConnection>,
    msg:       OrderStatusQueryMsg,
) {
    if let Some(order) = state.sol_orderbook.get_order(msg.order_id)
        && order.user_id == msg.user_id
    {
        let info = OpenOrderInfo {
            order_id:      order.order_id,
            user_id:       order.user_id,
            side:          match order.side { Side::Bid => "bid", Side::Ask => "ask" }.to_string(),
            price:         order.price,
            remaining_qty: order.remaining_qty,
        };
        reply(publisher, &msg.queue_id, &OrderStatusReply {
            identifier: msg.identifier,
            order:      Some(info),
            status:     Some("open".to_string()),
            error:      None,
        }).await;
        return;
    }

    reply(publisher, &msg.queue_id, &OrderStatusReply {
        identifier: msg.identifier,
        order:      None,
        status:     None,
        error:      Some("Order not found in open book".to_string()),
    }).await;
}
