//! Shared types for the CEX — used by both rs-engine and rs-gateway.
//!
//! Message flow:
//!   rs-gateway  --[Redis lpush]-->  rs-engine
//!   rs-engine   --[Redis lpush]-->  rs-gateway  (reply queue)

pub mod channels;
pub mod models;

pub use channels::*;
pub use models::*;
