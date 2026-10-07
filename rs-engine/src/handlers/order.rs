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
            let fee_rate = state.fee_schedule.taker_fee_bps.max(state.fee_schedule.maker_fee_bps);
            let fee_reserve = (cost * fee_rate) / 10_000;
            let total_lock = cost + fee_reserve;

            // ── Lock funds upfront ────────────────────────────────────────
            match side {
                Side::Bid => {
                    let usd = state.usd_mut(msg.user_id);
                    if usd.available < total_lock {
                        reply_error(publisher, &msg.queue_id, &msg.identifier,
                                    "Insufficient USD", 400).await;
                        return;
                    }
                    usd.available -= total_lock;
                    usd.locked    += total_lock;
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
            let mut buyer_total_deducted = 0_u64;

            for result in results {
                match result {
                    MatchResult::Trade(trade) => {
                        let (buyer_fee, seller_fee) = apply_fill(state, &trade);
                        filled_qty += trade.qty;
                        if side == Side::Bid {
                            buyer_total_deducted += (trade.price * trade.qty) + buyer_fee;
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
                            buyer_fee,
                            seller_fee,
                        }).await;
                    }
                    MatchResult::OrderAccepted(acc) => {
                        resting_order = Some(acc);
                    }
                }
            }

            // Reconcile and refund excess locked funds:
            match side {
                Side::Bid => {
                    let needed_locked = if let Some(ref acc) = resting_order {
                        let resting_cost = acc.price * acc.remaining_qty;
                        resting_cost + state.fee_schedule.calc_maker_fee(resting_cost)
                    } else {
                        0
                    };
                    let excess = total_lock.saturating_sub(buyer_total_deducted).saturating_sub(needed_locked);
                    if excess > 0 {
                        let usd = state.usd_mut(msg.user_id);
                        usd.locked -= excess;
                        usd.available += excess;
                    }
                }
                Side::Ask => {
                    let unfilled_qty = qty - filled_qty;
                    if msg.time_in_force != TimeInForce::Gtc && unfilled_qty > 0 {
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
                            let (buyer_fee, seller_fee) = apply_fill(state, &trade);
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
                                buyer_fee,
                                seller_fee,
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

                    let fee_reserve = state.fee_schedule.calc_taker_fee(required_usd);
                    let total_required = required_usd + fee_reserve;

                    let usd = state.usd_mut(msg.user_id);
                    if usd.available < total_required {
                        reply_error(publisher, &msg.queue_id, &msg.identifier,
                                    "Insufficient USD", 400).await;
                        return;
                    }
                    usd.available -= total_required;
                    usd.locked    += total_required;

                    let results = state.sol_orderbook.execute_market_order(msg.user_id, side, qty, worst_price)
                        .expect("qty validated");

                    let mut filled_qty = 0_u64;
                    let mut total_spent = 0_u64;

                    for result in results {
                        if let MatchResult::Trade(trade) = result {
                            let (buyer_fee, seller_fee) = apply_fill(state, &trade);
                            filled_qty  += trade.qty;
                            total_spent += (trade.price * trade.qty) + buyer_fee;

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
                                buyer_fee,
                                seller_fee,
                            }).await;
                        }
                    }

                    // Refund unspent USD if any (e.g. partial fill or lower price)
                    let refund = total_required.saturating_sub(total_spent);
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

/// Apply a fill to the balance ledger and exchange treasury.
///
/// Because funds are locked before the order is placed, BOTH buyer and seller
/// always have their payment in `locked` at this point.
/// Buyer fee is deducted from buyer's locked USD; seller fee is deducted from seller's proceeds.
/// Both fees are credited to Exchange Treasury (`user_id = 0`) available USD.
fn apply_fill(state: &mut EngineState, trade: &Trade) -> (u64, u64) {
    let value = trade.price * trade.qty;
    let (buyer_fee, seller_fee) = match trade.taker_side {
        Side::Bid => (
            state.fee_schedule.calc_taker_fee(value),
            state.fee_schedule.calc_maker_fee(value),
        ),
        Side::Ask => (
            state.fee_schedule.calc_maker_fee(value),
            state.fee_schedule.calc_taker_fee(value),
        ),
    };

    // Buyer: pays USD (value + buyer_fee from locked), receives SOL (into available).
    state.usd_mut(trade.buyer).locked          -= value + buyer_fee;
    state.sol_mut(trade.buyer).available        += trade.qty;

    // Seller: gives SOL (from locked), receives USD (value - seller_fee into available).
    state.sol_mut(trade.seller).locked          -= trade.qty;
    state.usd_mut(trade.seller).available       += value.saturating_sub(seller_fee);

    // Treasury (user_id = 0): collects both fees into available USD.
    let total_fee = buyer_fee + seller_fee;
    if total_fee > 0 {
        state.usd_mut(0).available += total_fee;
    }

    (buyer_fee, seller_fee)
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
            // locked = remaining_qty * price + maker_fee  (for bids)
            // locked = remaining_qty                       (for asks)
            match cancelled.side {
                Side::Bid => {
                    let cost = cancelled.price * cancelled.remaining_qty;
                    let fee = state.fee_schedule.calc_maker_fee(cost);
                    let refund = cost + fee;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_maker_taker_fee_accounting_and_treasury() {
        let mut state = EngineState::new().with_fee_schedule(FeeSchedule::new(10, 20)); // 10 bps maker, 20 bps taker

        // User 1 has 10 SOL
        state.sol_mut(1).available = 10;
        // User 2 has $1500 USD
        state.usd_mut(2).available = 1500;

        // User 1 places limit ask: 10 SOL @ 100 (will rest as maker)
        handle_order(&mut state, None, OrderMsg {
            user_id: 1,
            asset: "sol".to_string(),
            side: "ask".to_string(),
            price: 100,
            qty: 10,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            slippage_pct: None,
            queue_id: "q".to_string(),
            identifier: "id1".to_string(),
        }).await;

        assert_eq!(state.sol_mut(1).available, 0);
        assert_eq!(state.sol_mut(1).locked, 10);

        // User 2 places limit bid: 10 SOL @ 100 (incoming taker buy)
        // Cost = 1000. Taker fee reserve = 20 bps of 1000 = 2.
        handle_order(&mut state, None, OrderMsg {
            user_id: 2,
            asset: "sol".to_string(),
            side: "bid".to_string(),
            price: 100,
            qty: 10,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            slippage_pct: None,
            queue_id: "q".to_string(),
            identifier: "id2".to_string(),
        }).await;

        // User 2 (buyer, taker): pays $1000 + $2 taker fee = $1002.
        assert_eq!(state.usd_mut(2).available, 1500 - 1002);
        assert_eq!(state.usd_mut(2).locked, 0);
        assert_eq!(state.sol_mut(2).available, 10);

        // User 1 (seller, maker): receives $1000 - $1 maker fee = $999.
        assert_eq!(state.usd_mut(1).available, 999);
        assert_eq!(state.sol_mut(1).locked, 0);

        // Exchange Treasury (user_id = 0): collects $2 + $1 = $3.
        assert_eq!(state.usd_mut(0).available, 3);

        // Conservation: 498 + 999 + 3 = 1500 USD!
        assert_eq!(
            state.usd_mut(2).available + state.usd_mut(1).available + state.usd_mut(0).available,
            1500
        );
    }

    #[tokio::test]
    async fn test_resting_limit_buy_fee_reconciliation_and_cancellation() {
        let mut state = EngineState::new().with_fee_schedule(FeeSchedule::new(10, 20)); // 10 bps maker, 20 bps taker

        // User 3 has $1000 USD
        state.usd_mut(3).available = 1000;

        // User 3 places resting limit buy: 5 SOL @ 100.
        // Cost = 500. Upfront lock = 500 + 1 (taker reserve) = 501.
        // When it rests, maker fee needed is 500 * 10 / 10000 = 0 (integer division).
        // Excess 1 is refunded.
        handle_order(&mut state, None, OrderMsg {
            user_id: 3,
            asset: "sol".to_string(),
            side: "bid".to_string(),
            price: 100,
            qty: 5,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            slippage_pct: None,
            queue_id: "q".to_string(),
            identifier: "id3".to_string(),
        }).await;

        let open_order = state.sol_orderbook.get_user_orders(3);
        assert_eq!(open_order.len(), 1);
        let order_id = open_order[0].order_id;

        // Cancel order: refunds resting cost + maker fee
        handle_cancel(&mut state, None, CancelMsg {
            user_id: 3,
            order_id,
            queue_id: "q".to_string(),
            identifier: "id_cancel".to_string(),
        }).await;

        assert_eq!(state.usd_mut(3).available, 1000);
        assert_eq!(state.usd_mut(3).locked, 0);
    }
}

