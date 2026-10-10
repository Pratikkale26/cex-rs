use serde::{Deserialize, Serialize};

/// Messages sent from WebSocket clients to the Exchange Gateway.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WsClientMsg {
    /// Subscribe to a public market feed or private channel
    Subscribe {
        /// Channel name: "trades" | "depth" | "orders"
        channel: String,
        /// Market symbol, required for public feeds (e.g. "BTC_USD", "ETH_USD", "SOL_USD")
        #[serde(default)]
        market: Option<String>,
    },
    /// Unsubscribe from a feed
    Unsubscribe {
        channel: String,
        #[serde(default)]
        market: Option<String>,
    },
    /// Authenticate the WebSocket session with a JWT Bearer token
    Auth {
        token: String,
    },
    /// Liveness ping
    Ping,
}

/// Messages sent from the Exchange Gateway to WebSocket clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsServerMsg {
    /// Public broadcast: A trade was executed
    Trade {
        market: String,
        maker_order_id: u64,
        taker_order_id: u64,
        price: u64,
        qty: u64,
        quote_amount: u64,
        timestamp: i64,
    },
    /// Public broadcast: Orderbook depth snapshot/update
    Depth {
        market: String,
        bids: Vec<(u64, u64)>, // (price, qty)
        asks: Vec<(u64, u64)>, // (price, qty)
    },
    /// Private notification: An authenticated user's order changed status
    OrderUpdate {
        order_id: u64,
        market: String,
        side: String,
        price: u64,
        remaining_qty: u64,
        status: String, // "open", "partially_filled", "filled", "cancelled"
    },
    /// Acknowledgment of successful subscription
    Subscribed {
        channel: String,
        market: Option<String>,
    },
    /// Acknowledgment of successful unsubscription
    Unsubscribed {
        channel: String,
        market: Option<String>,
    },
    /// Acknowledgment of successful authentication
    Authenticated {
        user_id: u64,
    },
    /// Liveness response
    Pong,
    /// Protocol or subscription error
    Error {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_client_msg_serialization() {
        let sub = WsClientMsg::Subscribe {
            channel: "trades".to_string(),
            market: Some("BTC_USD".to_string()),
        };
        let json = serde_json::to_string(&sub).unwrap();
        assert_eq!(json, r#"{"op":"subscribe","channel":"trades","market":"BTC_USD"}"#);

        let parsed: WsClientMsg = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, sub);

        let ping_json = r#"{"op":"ping"}"#;
        let ping: WsClientMsg = serde_json::from_str(ping_json).unwrap();
        assert_eq!(ping, WsClientMsg::Ping);
    }

    #[test]
    fn test_ws_server_msg_serialization() {
        let trade = WsServerMsg::Trade {
            market: "SOL_USD".to_string(),
            maker_order_id: 1,
            taker_order_id: 2,
            price: 150,
            qty: 10,
            quote_amount: 1500,
            timestamp: 1234567890,
        };
        let json = serde_json::to_string(&trade).unwrap();
        assert!(json.contains(r#""type":"trade""#));
        assert!(json.contains(r#""market":"SOL_USD""#));

        let pong = WsServerMsg::Pong;
        let pong_json = serde_json::to_string(&pong).unwrap();
        assert_eq!(pong_json, r#"{"type":"pong"}"#);
    }
}
