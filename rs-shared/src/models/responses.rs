use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use super::balance::Balance;
use super::order::{OpenOrderInfo, OrderbookData};

// Field names here become the JSON keys the HTTP client sees,
// so we use camelCase via #[serde(rename_all = "camelCase")].

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BalanceReply {
    pub identifier:    String,
    pub usd_balance:   Balance,
    pub stock_balance: HashMap<String, Balance>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderReply {
    pub identifier:  String,
    pub order_id:    Option<u64>,
    pub status:      Option<String>,
    pub error:       Option<String>,
    pub status_code: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CancelReply {
    pub identifier:    String,
    pub order_id:      Option<u64>,
    pub remaining_qty: Option<u64>,
    pub message:       Option<String>,
    pub error:         Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OpenOrdersReply {
    pub identifier: String,
    pub orders:     Vec<OpenOrderInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderbookReply {
    pub identifier: String,
    pub orderbook:  OrderbookData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericReply {
    pub identifier: String,
    pub message:    String,
}
