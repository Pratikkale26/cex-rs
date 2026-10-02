use serde::{Deserialize, Serialize};

/// One open order as returned to the client.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrderInfo {
    pub order_id:      u64,
    pub user_id:       u64,
    pub side:          String, // "bid" | "ask"
    pub price:         u64,
    pub remaining_qty: u64,
}

/// One price level in the orderbook snapshot.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct OrderbookLevel {
    pub price: u64,
    pub qty:   u64,
}

/// Aggregated orderbook depth (sorted bids and asks).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct OrderbookData {
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}
