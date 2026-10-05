pub mod auth;
pub mod dto;
pub mod ledger;
pub mod rate_limit;
pub mod redis;
pub mod routes;
pub mod state;

// Backward-compatibility aliases for any existing internal paths
pub mod types {
    pub use crate::auth::User;
    pub use crate::auth::Claims;
    pub use crate::dto::*;
}
pub mod middleware {
    pub use crate::auth::*;
}

pub use state::AppState;
