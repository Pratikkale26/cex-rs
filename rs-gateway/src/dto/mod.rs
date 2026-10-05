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
    pub asset: String,
    pub side:  String, // "bid" | "ask"
    pub price: i64,
    pub qty:   i64,
}

impl Validate for OrderBody {
    fn validate(&self) -> Result<(), &'static str> {
        if self.asset.to_lowercase() != "sol" {
            return Err("Only SOL orders are supported");
        }
        if self.side != "bid" && self.side != "ask" {
            return Err("Side must be 'bid' or 'ask'");
        }
        if self.price <= 0 {
            return Err("Order price must be positive");
        }
        if self.qty <= 0 {
            return Err("Order quantity must be positive");
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
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), price: 100, qty: 10 }.validate().is_ok());
        assert!(OrderBody { asset: "btc".into(), side: "bid".into(), price: 100, qty: 10 }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "hold".into(), price: 100, qty: 10 }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), price: 0, qty: 10 }.validate().is_err());
        assert!(OrderBody { asset: "sol".into(), side: "bid".into(), price: 100, qty: 0 }.validate().is_err());
    }

    #[test]
    fn test_funding_validation() {
        assert!(OnrampBody { qty: 100 }.validate().is_ok());
        assert!(OnrampBody { qty: 0 }.validate().is_err());
        assert!(DepositBody { qty: 5 }.validate().is_ok());
        assert!(DepositBody { qty: 0 }.validate().is_err());
    }
}
