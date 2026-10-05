use std::collections::HashMap;
use std::sync::Arc;
use sqlx::PgPool;
use tokio::sync::{oneshot, Mutex};

pub struct AppState {
    pub db:              PgPool,
    pub redis_publisher: Mutex<redis::aio::MultiplexedConnection>,
    pub pending:         Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
    pub queue_id:        String,
}

impl AppState {
    pub fn new(
        db: PgPool,
        publisher: redis::aio::MultiplexedConnection,
        pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
        queue_id: String,
    ) -> Self {
        Self {
            db,
            redis_publisher: Mutex::new(publisher),
            pending,
            queue_id,
        }
    }
}
