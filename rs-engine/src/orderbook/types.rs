//! Core domain types for the order book and trade execution.

pub type UserId   = u64;
pub type OrderId  = u64;
pub type Price    = u64;
pub type Quantity = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Bid, Ask }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub order_id:      OrderId,
    pub user_id:       UserId,
    pub side:          Side,
    pub price:         Price,
    pub remaining_qty: Quantity,
}

/// A fill event: two orders crossed and qty units changed hands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub buyer:          UserId,
    pub seller:         UserId,
    pub price:          Price,
    pub qty:            Quantity,
    pub taker_order_id: OrderId,
    pub maker_order_id: OrderId,
}

/// A resting (unmatched) portion of an incoming order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderAccepted {
    pub order_id:      OrderId,
    pub user_id:       UserId,
    pub side:          Side,
    pub price:         Price,
    pub remaining_qty: Quantity,
}

/// Returned by cancel_order on success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderCancelled {
    pub order_id:      OrderId,
    pub user_id:       UserId,
    pub side:          Side,
    pub price:         Price,
    pub remaining_qty: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchResult {
    Trade(Trade),
    OrderAccepted(OrderAccepted),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderbookError {
    InvalidPrice,
    InvalidQuantity,
    OrderNotFound,
    NotOrderOwner,
}

// ── Snapshot types (used for the get-orderbook reply) ───────────────────────

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub symbol: String,
    pub bids:   Vec<LevelSnapshot>,
    pub asks:   Vec<LevelSnapshot>,
}

#[derive(Debug, Clone)]
pub struct LevelSnapshot {
    pub price: Price,
    pub qty:   Quantity, // total remaining qty at this level
}

#[derive(Debug, Clone)]
pub struct UserOrderSnapshot {
    pub order_id:      OrderId,
    pub user_id:       UserId,
    pub side:          Side,
    pub price:         Price,
    pub remaining_qty: Quantity,
}
