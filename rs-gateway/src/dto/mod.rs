pub mod history;

pub use history::*;
use serde::Deserialize;

pub trait Validate {
    fn validate(&self) -> Result<(), &'static str>;
}

#[derive(Deserialize, Debug, Clone)]
pub struct SignupBody {
    pub username: String,
    pub password: String,
}

impl Validate for SignupBody {
    fn validate(&self) -> Result<(), &'static str> {
        let u = self.username.trim();
        if u.len() < 3 || u.len() > 32 {
            return Err("Username must be between 3 and 32 characters");
        }
        if !u.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err("Username must contain only alphanumeric characters and underscores");
        }
        if self.password.len() < 6 {
            return Err("Password must be at least 6 characters long");
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct SigninBody {
    pub username: String,
    pub password: String,
}

impl Validate for SigninBody {
    fn validate(&self) -> Result<(), &'static str> {
        if self.username.trim().is_empty() {
            return Err("Username cannot be empty");
        }
        if self.password.is_empty() {
            return Err("Password cannot be empty");
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct OnrampBody {
    pub qty: u64,
}

impl Validate for OnrampBody {
    fn validate(&self) -> Result<(), &'static str> {
        if self.qty == 0 {
            return Err("Onramp quantity must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct DepositBody {
    pub qty: u64,
}

impl Validate for DepositBody {
    fn validate(&self) -> Result<(), &'static str> {
        if self.qty == 0 {
            return Err("Deposit quantity must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct OrderBody {
    pub asset:        String,
    pub side:         String, // "bid" | "ask"
    #[serde(default = "default_order_type")]
    pub order_type:   String,
    #[serde(default)]
    pub price:        i64,
    pub qty:          i64,
    pub slippage_pct: Option<f64>,
}

fn default_order_type() -> String {
    "limit".to_string()
}

impl Validate for OrderBody {
    fn validate(&self) -> Result<(), &'static str> {
        if self.asset.to_lowercase() != "sol" {
            return Err("Only SOL orders are supported");
        }
        if self.side != "bid" && self.side != "ask" {
            return Err("Side must be 'bid' or 'ask'");
        }
        let otype = self.order_type.to_lowercase();
        if otype != "limit" && otype != "market" {
            return Err("Order type must be 'limit' or 'market'");
        }
        if otype == "limit" && self.price <= 0 {
            return Err("Order price must be positive");
        }
        if self.qty <= 0 {
            return Err("Order quantity must be positive");
        }
        if self.slippage_pct.is_some_and(|s| s <= 0.0 || s > 50.0) {
            return Err("Slippage percentage must be between 0 and 50");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signup_validation() {
        assert!(SignupBody { username: "bob".into(), password: "password123".into() }.validate().is_ok());
        assert!(SignupBody { username: "al".into(), password: "password123".into() }.validate().is_err());
        assert!(SignupBody { username: "user@invalid".into(), password: "password123".into() }.validate().is_err());
        assert!(SignupBody { username: "valid_user".into(), password: "123".into() }.validate().is_err());
    }

    #[test]
    fn test_order_validation() {
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "limit".into(), price: 100, qty: 10, slippage_pct: None }.validate().is_ok());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "market".into(), price: 0, qty: 10, slippage_pct: None }.validate().is_ok());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "market".into(), price: 0, qty: 10, slippage_pct: Some(2.5) }.validate().is_ok());
        assert!(OrderBody { asset: "sol".into(), side: "ask".into(), order_type: "market".into(), price: 0, qty: 10, slippage_pct: None }.validate().is_ok());
        assert!(OrderBody { asset: "btc".into(), side: "bid".into(), order_type: "limit".into(), price: 100, qty: 10, slippage_pct: None }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "hold".into(), order_type: "limit".into(), price: 100, qty: 10, slippage_pct: None }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "invalid".into(), price: 100, qty: 10, slippage_pct: None }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "limit".into(), price: 0, qty: 10, slippage_pct: None }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "limit".into(), price: 100, qty: 0, slippage_pct: None }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "market".into(), price: 0, qty: 10, slippage_pct: Some(0.0) }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), order_type: "market".into(), price: 0, qty: 10, slippage_pct: Some(60.0) }.validate().is_err());
    }

    #[test]
    fn test_funding_validation() {
        assert!(OnrampBody { qty: 100 }.validate().is_ok());
        assert!(OnrampBody { qty: 0 }.validate().is_err());
        assert!(DepositBody { qty: 5 }.validate().is_ok());
        assert!(DepositBody { qty: 0 }.validate().is_err());
    }
}
