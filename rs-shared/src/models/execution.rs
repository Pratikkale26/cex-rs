use serde::{Deserialize, Serialize};

/// Events emitted by the matching engine to the cold-path execution stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    OrderCreated {
        order_id:      u64,
        user_id:       u64,
        market:        String,
        side:          String, // "bid" | "ask"
        price:         u64,
        original_qty:  u64,
        remaining_qty: u64,
        status:        String, // "open" | "filled"
    },
    TradeExecuted {
        maker_order_id:      u64,
        taker_order_id:      u64,
        buyer_id:            u64,
        seller_id:           u64,
        market:              String,
        price:               u64,
        qty:                 u64,
        quote_amount:        u64,
        maker_remaining_qty: u64,
        taker_remaining_qty: u64,
    },
    OrderCancelled {
        order_id:      u64,
        user_id:       u64,
        remaining_qty: u64,
    },
    FundingExecuted {
        user_id:        u64,
        currency:       String,
        amount:         u64,
        operation_type: String, // "onramp" | "deposit"
    },
}

/// Order record returned by API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OrderRecord {
    pub id:             i64,
    pub user_id:        i64,
    pub market:         String,
    pub side:           String,
    pub price:          i64,
    pub original_qty:   i64,
    pub remaining_qty:  i64,
    pub status:         String,
    pub created_at:     chrono::DateTime<chrono::Utc>,
    pub updated_at:     chrono::DateTime<chrono::Utc>,
}

/// Trade history record returned by API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    pub id:             i64,
    pub maker_order_id: i64,
    pub taker_order_id: i64,
    pub buyer_id:       i64,
    pub seller_id:      i64,
    pub market:         String,
    pub price:          i64,
    pub qty:            i64,
    pub quote_amount:   i64,
    pub created_at:     chrono::DateTime<chrono::Utc>,
}

/// Ledger statement record returned by API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LedgerRecord {
    pub id:             i64,
    pub user_id:        i64,
    pub currency:       String,
    pub amount:         i64,
    pub balance_type:   String,
    pub operation_type: String,
    pub reference_id:   String,
    pub created_at:     chrono::DateTime<chrono::Utc>,
}
