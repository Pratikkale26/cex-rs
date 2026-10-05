use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Trade history record returned by API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
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
