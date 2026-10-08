//! Engine state — balances and the orderbook, all in one place.
//!
//! Held behind an `Arc<Mutex<EngineState>>` so the brpop loop
//! can take a lock, mutate, and release before sending the reply.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use rs_shared::{Balance, FeeSchedule};
use crate::orderbook::Orderbook;

fn default_btc_orderbook() -> Orderbook {
    Orderbook::new("BTC-USDT")
}

fn default_eth_orderbook() -> Orderbook {
    Orderbook::new("ETH-USDT")
}

fn default_next_order_id() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineState {
    /// USD balance per user:  user_id → { available, locked }
    pub usd_balance:   HashMap<u64, Balance>,

    /// Asset balances per user per symbol:  user_id → symbol → { available, locked }
    pub stock_balance: HashMap<u64, HashMap<String, Balance>>,

    /// The SOL/USD orderbook.
    pub sol_orderbook: Orderbook,

    /// The BTC/USD orderbook.
    #[serde(default = "default_btc_orderbook")]
    pub btc_orderbook: Orderbook,

    /// The ETH/USD orderbook.
    #[serde(default = "default_eth_orderbook")]
    pub eth_orderbook: Orderbook,

    /// Monotonic sequence generator for globally unique order IDs across all markets.
    #[serde(default = "default_next_order_id")]
    pub next_order_id: u64,

    /// Fee schedule for maker and taker fees.
    #[serde(default)]
    pub fee_schedule: FeeSchedule,
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            usd_balance:   HashMap::new(),
            stock_balance: HashMap::new(),
            sol_orderbook: Orderbook::new("SOL-USDT"),
            btc_orderbook: Orderbook::new("BTC-USDT"),
            eth_orderbook: Orderbook::new("ETH-USDT"),
            next_order_id: 1,
            fee_schedule:  FeeSchedule::from_env(),
        }
    }

    pub fn with_fee_schedule(mut self, fee_schedule: FeeSchedule) -> Self {
        self.fee_schedule = fee_schedule;
        self
    }
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineState {

    /// Wipe everything — used by the reset handler between tests.
    pub fn reset(&mut self) {
        self.usd_balance.clear();
        self.stock_balance.clear();
        self.sol_orderbook.reset();
        self.btc_orderbook.reset();
        self.eth_orderbook.reset();
        self.next_order_id = 1;
    }

    // ── Multi-market convenience helpers ─────────────────────────────────────

    pub fn next_order_id(&mut self) -> u64 {
        let id = self.next_order_id;
        self.next_order_id += 1;
        id
    }

    pub fn get_orderbook(&self, asset: &str) -> Option<&Orderbook> {
        match asset.to_lowercase().as_str() {
            "sol" => Some(&self.sol_orderbook),
            "btc" => Some(&self.btc_orderbook),
            "eth" => Some(&self.eth_orderbook),
            _ => None,
        }
    }

    pub fn get_orderbook_mut(&mut self, asset: &str) -> Option<&mut Orderbook> {
        match asset.to_lowercase().as_str() {
            "sol" => Some(&mut self.sol_orderbook),
            "btc" => Some(&mut self.btc_orderbook),
            "eth" => Some(&mut self.eth_orderbook),
            _ => None,
        }
    }

    // ── Convenience helpers ──────────────────────────────────────────────────

    pub fn usd_mut(&mut self, user_id: u64) -> &mut Balance {
        self.usd_balance.entry(user_id).or_default()
    }

    pub fn asset_mut(&mut self, user_id: u64, asset: &str) -> &mut Balance {
        self.stock_balance
            .entry(user_id)
            .or_default()
            .entry(asset.to_lowercase())
            .or_default()
    }

    pub fn sol_mut(&mut self, user_id: u64) -> &mut Balance {
        self.asset_mut(user_id, "sol")
    }

    pub fn btc_mut(&mut self, user_id: u64) -> &mut Balance {
        self.asset_mut(user_id, "btc")
    }

    pub fn eth_mut(&mut self, user_id: u64) -> &mut Balance {
        self.asset_mut(user_id, "eth")
    }
}
