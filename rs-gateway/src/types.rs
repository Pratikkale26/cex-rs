//! HTTP request / response types and the in-memory user store.

use serde::{Deserialize, Serialize};

// ── User store ───────────────────────────────────────────────────────────────

/// A user stored in the gateway's in-memory list.
pub struct User {
    pub id:       u64,
    pub username: String,
    pub password: String,
}

// ── Request bodies ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SignupBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SigninBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct OnrampBody {
    pub qty: u64,
}

#[derive(Deserialize)]
pub struct DepositBody {
    pub qty: u64,
}

#[derive(Deserialize)]
pub struct OrderBody {
    pub asset: String,
    pub side:  String, // "bid" | "ask"
    pub price: i64,
    pub qty:   i64,
}

// ── JWT claims ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: u64,
    pub exp: usize,
}
