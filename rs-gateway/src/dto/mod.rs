use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct SignupBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SigninBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OnrampBody {
    pub qty: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DepositBody {
    pub qty: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OrderBody {
    pub asset: String,
    pub side:  String, // "bid" | "ask"
    pub price: i64,
    pub qty:   i64,
}
