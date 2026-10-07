//! Redis channel names and response queue prefix definitions.

pub const CH_SIGNUP:      &str = "new-signup";
pub const CH_ORDER:       &str = "incoming-order";
pub const CH_CANCEL:      &str = "cancel-order";
pub const CH_BALANCE:     &str = "balance-request";
pub const CH_ONRAMP:      &str = "onramp";
pub const CH_DEPOSIT:     &str = "deposite"; // keep typo to match ts-cex keys
pub const CH_ORDERBOOK:   &str = "get-orderbook";
pub const CH_OPEN_ORDERS: &str = "get-open-orders";
pub const CH_ORDER_STATUS: &str = "order-status";
pub const CH_RESET:       &str = "reset";
pub const REPLY_PREFIX:   &str = "response-queue"; // + queue_id
pub const STREAM_EVENTS:      &str = "engine-events";     // Redis stream WAL
pub const STREAM_EXECUTIONS:  &str = "engine-executions"; // Execution events for cold-path batching
