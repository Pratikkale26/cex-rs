use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignupMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OnrampMsg {
    pub user_id:    u64,
    pub qty:        u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DepositMsg {
    pub user_id:    u64,
    pub symbol:     String,
    pub qty:        u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    #[default]
    Limit,
    Market,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderMsg {
    pub user_id:      u64,
    pub asset:        String,
    pub side:         String, // "bid" | "ask"
    #[serde(default)] // it will make Limit as default if missing
    pub order_type:   OrderType, // "limit" | "market"
    pub price:        i64,
    pub qty:          i64,
    pub slippage_pct: Option<f64>,
    pub queue_id:     String,
    pub identifier:   String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CancelMsg {
    pub user_id:    u64,
    pub order_id:   u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BalanceQueryMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderbookQueryMsg {
    pub asset:      String,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OpenOrdersQueryMsg {
    pub user_id:    u64,
    pub queue_id:   String,
    pub identifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResetMsg {
    pub queue_id:   String,
    pub identifier: String,
}
