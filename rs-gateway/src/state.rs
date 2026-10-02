use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{oneshot, Mutex};
use crate::auth::User;

pub struct AppState {
    pub users:           Mutex<Vec<User>>,
    pub user_index:      AtomicU64,
    pub redis_publisher: Mutex<redis::aio::MultiplexedConnection>,
    pub pending:         Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
    pub queue_id:        String,
}

impl AppState {
    pub fn new(
        publisher: redis::aio::MultiplexedConnection,
        pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
        queue_id: String,
    ) -> Self {
        Self {
            users:           Mutex::new(Vec::new()),
            user_index:      AtomicU64::new(0),
            redis_publisher: Mutex::new(publisher),
            pending,
            queue_id,
        }
    }
}
