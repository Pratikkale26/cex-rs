//! Shared types for the CEX — used by both rs-engine and rs-gateway.
//!
//! Message flow:
//!   rs-gateway  --[Redis lpush]-->  rs-engine
//!   rs-engine   --[Redis lpush]-->  rs-gateway  (reply queue)

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

// ─── Redis channel names ────────────────────────────────────────────────────

pub const CH_SIGNUP:       &str = "new-signup";
pub const CH_ORDER:        &str = "incoming-order";
pub const CH_CANCEL:       &str = "cancel-order";
pub const CH_BALANCE:      &str = "balance-request";
pub const CH_ONRAMP:       &str = "onramp";
pub const CH_DEPOSIT:      &str = "deposite"; // keep typo to match ts-cex keys
pub const CH_ORDERBOOK:    &str = "get-orderbook";
pub const CH_OPEN_ORDERS:  &str = "get-open-orders";
pub const CH_RESET:        &str = "reset";
pub const REPLY_PREFIX:    &str = "response-queue"; // + queue_id

// ─── Shared data ────────────────────────────────────────────────────────────

/// A split balance: funds ready to use vs funds locked in open orders.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Balance {
    pub available: u64,
    pub locked:    u64,
}

// ─── Gateway → Engine messages ──────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct SignupMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct OnrampMsg {
    pub user_id:    u64,
    pub qty:        u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct DepositMsg {
    pub user_id:    u64,
    pub symbol:     String,
    pub qty:        u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct OrderMsg {
    pub user_id:    u64,
    pub asset:      String,
    pub side:       String, // "bid" | "ask"
    pub price:      i64,
    pub qty:        i64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct CancelMsg {
    pub user_id:    u64,
    pub order_id:   u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct BalanceQueryMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct OrderbookQueryMsg {
    pub asset:      String,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct OpenOrdersQueryMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResetMsg {
    pub queue_id:   String,
    pub identifier: String,
}

// ─── Engine → Gateway replies ───────────────────────────────────────────────
//
// Field names here become the JSON keys the HTTP client sees,
// so we use camelCase via #[serde(rename_all = "camelCase")].

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceReply {
    pub identifier:    String,
    pub usd_balance:   Balance,
    pub stock_balance: HashMap<String, Balance>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderReply {
    pub identifier:  String,
    pub order_id:    Option<u64>,
    pub status:      Option<String>,
    pub error:       Option<String>,
    pub status_code: Option<u16>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelReply {
    pub identifier:    String,
    pub order_id:      Option<u64>,
    pub remaining_qty: Option<u64>,
    pub message:       Option<String>,
    pub error:         Option<String>,
}

/// One open order as returned to the client.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrderInfo {
    pub order_id:      u64,
    pub user_id:       u64,
    pub side:          String, // "bid" | "ask"
    pub price:         u64,
    pub remaining_qty: u64,
}

#[derive(Serialize, Deserialize)]
pub struct OpenOrdersReply {
    pub identifier: String,
    pub orders:     Vec<OpenOrderInfo>,
}

/// One price level in the orderbook snapshot.
#[derive(Serialize, Deserialize, Clone)]
pub struct OrderbookLevel {
    pub price: u64,
    pub qty:   u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OrderbookData {
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}

#[derive(Serialize, Deserialize)]
pub struct OrderbookReply {
    pub identifier: String,
    pub orderbook:  OrderbookData,
}

#[derive(Serialize, Deserialize)]
pub struct GenericReply {
    pub identifier: String,
    pub message:    String,
}
