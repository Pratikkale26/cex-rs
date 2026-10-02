use serde::{Deserialize, Serialize};

/// A split balance: funds ready to use vs funds locked in open orders.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Balance {
    pub available: u64,
    pub locked:    u64,
}
