//! Engine state — balances and the orderbook, all in one place.
//!
//! Held behind an `Arc<Mutex<EngineState>>` so the brpop loop
//! can take a lock, mutate, and release before sending the reply.

use std::collections::HashMap;
use rs_shared::Balance;
use crate::orderbook::Orderbook;

pub struct EngineState {
    /// USD balance per user:  user_id → { available, locked }
    pub usd_balance:   HashMap<u64, Balance>,

    /// Asset balances per user per symbol:  user_id → symbol → { available, locked }
    pub stock_balance: HashMap<u64, HashMap<String, Balance>>,

    /// The SOL/USD orderbook (the only market we support for now).
    pub sol_orderbook: Orderbook,
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            usd_balance:   HashMap::new(),
            stock_balance: HashMap::new(),
            sol_orderbook: Orderbook::new("SOL-USDT"),
        }
    }

    /// Wipe everything — used by the reset handler between tests.
    pub fn reset(&mut self) {
        self.usd_balance.clear();
        self.stock_balance.clear();
        self.sol_orderbook.reset();
    }

    // ── Convenience helpers ──────────────────────────────────────────────────

    pub fn usd_mut(&mut self, user_id: u64) -> &mut Balance {
        self.usd_balance.entry(user_id).or_default()
    }

    pub fn sol_mut(&mut self, user_id: u64) -> &mut Balance {
        self.stock_balance
            .entry(user_id)
            .or_default()
            .entry("sol".to_string())
            .or_default()
    }
}
