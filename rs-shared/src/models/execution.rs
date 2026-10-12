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
        #[serde(default)]
        buyer_fee:           u64,
        #[serde(default)]
        seller_fee:          u64,
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
    DepthUpdated {
        market:         String,
        bids:           Vec<(u64, u64)>, // (price, qty)
        asks:           Vec<(u64, u64)>, // (price, qty)
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

/// Exchange fee schedule defining maker and taker fee basis points (bps).
/// 1 bps = 0.01% (e.g. 10 bps = 0.10%, 5 bps = 0.05%).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FeeSchedule {
    pub maker_fee_bps: u64,
    pub taker_fee_bps: u64,
}

impl FeeSchedule {
    pub fn new(maker_fee_bps: u64, taker_fee_bps: u64) -> Self {
        Self { maker_fee_bps, taker_fee_bps }
    }

    /// Load fee schedule from MAKER_FEE_BPS and TAKER_FEE_BPS environment variables.
    pub fn from_env() -> Self {
        let maker_fee_bps = std::env::var("MAKER_FEE_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let taker_fee_bps = std::env::var("TAKER_FEE_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Self { maker_fee_bps, taker_fee_bps }
    }

    pub fn calc_maker_fee(&self, quote_amount: u64) -> u64 {
        (quote_amount * self.maker_fee_bps) / 10_000
    }

    pub fn calc_taker_fee(&self, quote_amount: u64) -> u64 {
        (quote_amount * self.taker_fee_bps) / 10_000
    }
}
