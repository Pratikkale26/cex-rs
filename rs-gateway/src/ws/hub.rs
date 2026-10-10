use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use rs_shared::WsServerMsg;

pub type ClientId = u64;

struct ClientSession {
    tx: mpsc::UnboundedSender<String>,
    user_id: Option<u64>,
}

/// Central thread-safe in-memory registry of all active WebSocket client sessions and their subscriptions.
#[derive(Default)]
pub struct BroadcasterHub {
    next_client_id: AtomicU64,
    clients: RwLock<HashMap<ClientId, ClientSession>>,
    topics: RwLock<HashMap<String, HashSet<ClientId>>>,
    user_clients: RwLock<HashMap<u64, HashSet<ClientId>>>,
}

impl BroadcasterHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_client_id: AtomicU64::new(1),
            clients: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
            user_clients: RwLock::new(HashMap::new()),
        })
    }

    /// Registers a newly connected WebSocket client and returns their unique ClientId.
    pub async fn register(&self, tx: mpsc::UnboundedSender<String>) -> ClientId {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let mut clients = self.clients.write().await;
        clients.insert(id, ClientSession { tx, user_id: None });
        id
    }

    /// Cleans up a disconnected client, removing all its topic subscriptions and user mappings.
    pub async fn unregister(&self, client_id: ClientId) {
        // 1. Remove from clients map and get user_id if authenticated
        let user_id = {
            let mut clients = self.clients.write().await;
            clients.remove(&client_id).and_then(|s| s.user_id)
        };

        // 2. Remove from topics
        {
            let mut topics = self.topics.write().await;
            for subs in topics.values_mut() {
                subs.remove(&client_id);
            }
            topics.retain(|_, subs| !subs.is_empty());
        }

        // 3. Remove from user_clients if authenticated
        if let Some(uid) = user_id {
            let mut user_clients = self.user_clients.write().await;
            if let Some(subs) = user_clients.get_mut(&uid) {
                subs.remove(&client_id);
                if subs.is_empty() {
                    user_clients.remove(&uid);
                }
            }
        }
    }

    /// Subscribes a client to a named topic (e.g. "trades:BTC_USD", "depth:BTC_USD").
    pub async fn subscribe(&self, client_id: ClientId, topic: String) {
        let mut topics = self.topics.write().await;
        topics.entry(topic).or_default().insert(client_id);
    }

    /// Unsubscribes a client from a named topic.
    pub async fn unsubscribe(&self, client_id: ClientId, topic: &str) {
        let mut topics = self.topics.write().await;
        if let Some(subs) = topics.get_mut(topic) {
            subs.remove(&client_id);
        }
    }

    /// Associates an authenticated user_id with the client session.
    pub async fn authenticate(&self, client_id: ClientId, user_id: u64) {
        {
            let mut clients = self.clients.write().await;
            if let Some(session) = clients.get_mut(&client_id) {
                session.user_id = Some(user_id);
            }
        }

        {
            let mut user_clients = self.user_clients.write().await;
            user_clients.entry(user_id).or_default().insert(client_id);
        }
    }

    /// Sends a direct message to a specific client.
    pub async fn send_to_client(&self, client_id: ClientId, msg: &WsServerMsg) {
        if let Ok(payload) = serde_json::to_string(msg) {
            let clients = self.clients.read().await;
            if let Some(session) = clients.get(&client_id) {
                let _ = session.tx.send(payload);
            }
        }
    }

    /// Broadcasts a message to all clients subscribed to a given topic.
    pub async fn broadcast_topic(&self, topic: &str, msg: &WsServerMsg) {
        let payload = match serde_json::to_string(msg) {
            Ok(p) => p,
            Err(_) => return,
        };

        let topics = self.topics.read().await;
        let subscriber_ids = match topics.get(topic) {
            Some(subs) => subs.clone(),
            None => return,
        };
        drop(topics);

        let clients = self.clients.read().await;
        for cid in subscriber_ids {
            if let Some(session) = clients.get(&cid) {
                let _ = session.tx.send(payload.clone());
            }
        }
    }

    /// Sends a private message to all active sessions owned by an authenticated user.
    pub async fn send_private(&self, user_id: u64, msg: &WsServerMsg) {
        let payload = match serde_json::to_string(msg) {
            Ok(p) => p,
            Err(_) => return,
        };

        let user_clients = self.user_clients.read().await;
        let subscriber_ids = match user_clients.get(&user_id) {
            Some(subs) => subs.clone(),
            None => return,
        };
        drop(user_clients);

        let clients = self.clients.read().await;
        for cid in subscriber_ids {
            if let Some(session) = clients.get(&cid) {
                let _ = session.tx.send(payload.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hub_register_subscribe_and_broadcast() {
        let hub = BroadcasterHub::new();

        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let cid1 = hub.register(tx1).await;
        let cid2 = hub.register(tx2).await;

        hub.subscribe(cid1, "trades:BTC_USD".to_string()).await;
        hub.subscribe(cid2, "trades:ETH_USD".to_string()).await;

        let btc_trade = WsServerMsg::Trade {
            market: "BTC_USD".to_string(),
            maker_order_id: 1,
            taker_order_id: 2,
            price: 50_000,
            qty: 1,
            quote_amount: 50_000,
            timestamp: 1000,
        };

        hub.broadcast_topic("trades:BTC_USD", &btc_trade).await;

        // Client 1 should receive BTC trade
        let msg1 = rx1.recv().await.unwrap();
        assert!(msg1.contains(r#""market":"BTC_USD""#));

        // Client 2 (only subscribed to ETH) should NOT receive it
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_hub_private_user_broadcast() {
        let hub = BroadcasterHub::new();

        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

        let cid1 = hub.register(tx1).await;
        let cid2 = hub.register(tx2).await;

        // Authenticate client 1 as user 42
        hub.authenticate(cid1, 42).await;
        // Authenticate client 2 as user 99
        hub.authenticate(cid2, 99).await;

        let order_update = WsServerMsg::OrderUpdate {
            order_id: 10,
            market: "SOL_USD".to_string(),
            side: "bid".to_string(),
            price: 150,
            remaining_qty: 0,
            status: "filled".to_string(),
        };

        hub.send_private(42, &order_update).await;

        let msg = rx1.recv().await.unwrap();
        assert!(msg.contains(r#""status":"filled""#));
        assert!(rx2.try_recv().is_err());

        // Unregister client 1 and verify cleanup
        hub.unregister(cid1).await;
        hub.send_private(42, &order_update).await;
        assert!(rx1.try_recv().is_err());
    }
}
