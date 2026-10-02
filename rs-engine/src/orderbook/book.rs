use std::collections::{BTreeMap, HashMap, VecDeque};
use super::types::*;

#[derive(Debug)]
pub(crate) struct PriceLevel {
    pub(crate) orders: VecDeque<OrderId>,
}

impl PriceLevel {
    pub(crate) fn new() -> Self { Self { orders: VecDeque::new() } }
    pub(crate) fn is_empty(&self) -> bool { self.orders.is_empty() }
}

/// Price-time-priority limit order book.
///
/// Uses `BTreeMap` for naturally sorted price levels and `VecDeque` for
/// FIFO queues within each level. A global `HashMap` gives O(1) order lookup.
pub struct Orderbook {
    symbol:        String,
    next_order_id: OrderId,
    bids:          BTreeMap<Price, PriceLevel>,
    asks:          BTreeMap<Price, PriceLevel>,
    /// Global order lookup — O(1) get/remove by ID.
    orders:        HashMap<OrderId, Order>,
}

impl Orderbook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol:        symbol.into(),
            next_order_id: 1,
            bids:          BTreeMap::new(),
            asks:          BTreeMap::new(),
            orders:        HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.orders.clear();
        self.next_order_id = 1;
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(&p, _)| p)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(&p, _)| p)
    }

    pub fn order_count(&self) -> usize { self.orders.len() }
    pub fn bid_levels(&self)  -> usize { self.bids.len()   }
    pub fn ask_levels(&self)  -> usize { self.asks.len()   }

    pub fn get_order(&self, order_id: OrderId) -> Option<&Order> {
        self.orders.get(&order_id)
    }

    /// All resting orders belonging to `user_id`, across both sides.
    pub fn get_user_orders(&self, user_id: UserId) -> Vec<UserOrderSnapshot> {
        self.orders.values()
            .filter(|o| o.user_id == user_id)
            .map(|o| UserOrderSnapshot {
                order_id:      o.order_id,
                user_id:       o.user_id,
                side:          o.side,
                price:         o.price,
                remaining_qty: o.remaining_qty,
            })
            .collect()
    }

    /// Snapshot of the book for the get-orderbook endpoint.
    pub fn get_state(&self) -> OrderbookSnapshot {
        // Bids: highest price first (descending).
        let bids = self.bids.iter().rev()
            .map(|(&price, level)| LevelSnapshot {
                price,
                qty: level.orders.iter()
                    .filter_map(|id| self.orders.get(id))
                    .map(|o| o.remaining_qty)
                    .sum(),
            })
            .collect();

        // Asks: lowest price first (ascending, natural BTreeMap order).
        let asks = self.asks.iter()
            .map(|(&price, level)| LevelSnapshot {
                price,
                qty: level.orders.iter()
                    .filter_map(|id| self.orders.get(id))
                    .map(|o| o.remaining_qty)
                    .sum(),
            })
            .collect();

        OrderbookSnapshot { symbol: self.symbol.clone(), bids, asks }
    }

    /// Place an order. Returns a list of fills + possibly one resting-order event.
    pub fn add_order(
        &mut self,
        user_id: UserId,
        side:    Side,
        price:   Price,
        qty:     Quantity,
    ) -> Result<Vec<MatchResult>, OrderbookError> {
        if price == 0 { return Err(OrderbookError::InvalidPrice);    }
        if qty   == 0 { return Err(OrderbookError::InvalidQuantity); }

        let order_id       = self.next_order_id;
        self.next_order_id += 1;

        let mut remaining_qty = qty;
        let mut results       = Vec::new();

        // ── Match against the opposite side ──────────────────────────────
        while remaining_qty > 0 {
            let resting_price = match side {
                Side::Bid => self.best_ask(),
                Side::Ask => self.best_bid(),
            };
            let resting_price = match resting_price {
                Some(p) => p,
                None    => break,
            };

            // Does this order cross the spread?
            let crosses = match side {
                Side::Bid => price >= resting_price,
                Side::Ask => price <= resting_price,
            };
            if !crosses { break; }

            // Consume FIFO from this price level.
            loop {
                if remaining_qty == 0 { break; }

                let maker_id = {
                    let levels = match side {
                        Side::Bid => &self.asks,
                        Side::Ask => &self.bids,
                    };
                    match levels.get(&resting_price).and_then(|l| l.orders.front()) {
                        Some(&id) => id,
                        None      => break,
                    }
                };

                let maker = self.orders.get_mut(&maker_id)
                    .expect("order in price level must exist in map");

                let fill_qty       = remaining_qty.min(maker.remaining_qty);
                let maker_user_id  = maker.user_id;
                maker.remaining_qty -= fill_qty;
                remaining_qty       -= fill_qty;

                let trade = match side {
                    Side::Bid => Trade {
                        buyer:          user_id,
                        seller:         maker_user_id,
                        price:          resting_price,
                        qty:            fill_qty,
                        taker_order_id: order_id,
                        maker_order_id: maker_id,
                    },
                    Side::Ask => Trade {
                        buyer:          maker_user_id,
                        seller:         user_id,
                        price:          resting_price,
                        qty:            fill_qty,
                        taker_order_id: order_id,
                        maker_order_id: maker_id,
                    },
                };
                results.push(MatchResult::Trade(trade));

                // Remove maker if fully filled.
                let maker_done = self.orders.get(&maker_id)
                    .map(|o| o.remaining_qty == 0)
                    .unwrap_or(false);

                if maker_done {
                    self.orders.remove(&maker_id);

                    let level_empty = {
                        let levels = match side {
                            Side::Bid => &mut self.asks,
                            Side::Ask => &mut self.bids,
                        };
                        let level = levels.get_mut(&resting_price)
                            .expect("price level must exist");
                        let front = level.orders.pop_front()
                            .expect("maker order must be in FIFO queue");
                        debug_assert_eq!(front, maker_id);
                        level.is_empty()
                    };

                    if level_empty {
                        match side {
                            Side::Bid => { self.asks.remove(&resting_price); }
                            Side::Ask => { self.bids.remove(&resting_price); }
                        }
                        break; // outer loop picks next level
                    }
                } else {
                    // Maker partially filled — it keeps its position. Move on.
                    break;
                }
            }
        }

        // ── Rest any unfilled quantity ────────────────────────────────────
        if remaining_qty > 0 {
            let order = Order {
                order_id,
                user_id,
                side,
                price,
                remaining_qty,
            };

            let levels = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            levels.entry(price)
                .or_insert_with(PriceLevel::new)
                .orders
                .push_back(order_id);

            self.orders.insert(order_id, order);

            results.push(MatchResult::OrderAccepted(OrderAccepted {
                order_id,
                user_id,
                side,
                price,
                remaining_qty,
            }));
        }

        Ok(results)
    }

    pub fn cancel_order(
        &mut self,
        order_id: OrderId,
        user_id:  UserId,
    ) -> Result<OrderCancelled, OrderbookError> {
        let order = self.orders.get(&order_id)
            .ok_or(OrderbookError::OrderNotFound)?;

        if order.user_id != user_id {
            return Err(OrderbookError::NotOrderOwner);
        }

        let order = self.orders.remove(&order_id).unwrap();

        let levels = match order.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let level = levels.get_mut(&order.price)
            .expect("price level must exist");

        let pos = level.orders.iter().position(|&id| id == order_id)
            .expect("order must be in its price level");
        level.orders.remove(pos);

        if level.is_empty() {
            levels.remove(&order.price);
        }

        Ok(OrderCancelled {
            order_id:      order.order_id,
            user_id:       order.user_id,
            side:          order.side,
            price:         order.price,
            remaining_qty: order.remaining_qty,
        })
    }
}
