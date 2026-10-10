use std::sync::Arc;
use actix_ws::{Message, MessageStream, Session};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use rs_shared::{WsClientMsg, WsServerMsg};
use crate::auth::jwt::verify_token;
use super::hub::BroadcasterHub;

/// Handles a single active WebSocket connection lifecycle.
pub async fn handle_ws_session(
    mut session: Session,
    mut msg_stream: MessageStream,
    hub: Arc<BroadcasterHub>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let client_id = hub.register(tx).await;

    loop {
        tokio::select! {
            // Outgoing frames from Hub broadcast queue
            Some(payload) = rx.recv() => {
                if session.text(payload).await.is_err() {
                    break;
                }
            }

            // Incoming frames from WebSocket client
            Some(msg_res) = msg_stream.next() => {
                match msg_res {
                    Ok(Message::Text(text)) => {
                        let client_msg: WsClientMsg = match serde_json::from_str(&text) {
                            Ok(m) => m,
                            Err(e) => {
                                hub.send_to_client(client_id, &WsServerMsg::Error {
                                    message: format!("Malformed JSON: {e}"),
                                }).await;
                                continue;
                            }
                        };

                        match client_msg {
                            WsClientMsg::Subscribe { channel, market } => {
                                let topic = format_topic(&channel, market.as_deref());
                                hub.subscribe(client_id, topic).await;
                                hub.send_to_client(client_id, &WsServerMsg::Subscribed {
                                    channel,
                                    market,
                                }).await;
                            }
                            WsClientMsg::Unsubscribe { channel, market } => {
                                let topic = format_topic(&channel, market.as_deref());
                                hub.unsubscribe(client_id, &topic).await;
                                hub.send_to_client(client_id, &WsServerMsg::Unsubscribed {
                                    channel,
                                    market,
                                }).await;
                            }
                            WsClientMsg::Auth { token } => {
                                match verify_token(&token) {
                                    Ok(claims) => {
                                        hub.authenticate(client_id, claims.sub).await;
                                        hub.send_to_client(client_id, &WsServerMsg::Authenticated {
                                            user_id: claims.sub,
                                        }).await;
                                    }
                                    Err(_) => {
                                        hub.send_to_client(client_id, &WsServerMsg::Error {
                                            message: "Invalid or expired JWT token".to_string(),
                                        }).await;
                                    }
                                }
                            }
                            WsClientMsg::Ping => {
                                hub.send_to_client(client_id, &WsServerMsg::Pong).await;
                            }
                        }
                    }
                    Ok(Message::Ping(bytes)) => {
                        if session.pong(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        let _ = session.close(None).await;
                        break;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }

            else => break,
        }
    }

    // Always unregister client to prevent memory leaks on disconnect
    hub.unregister(client_id).await;
}

pub fn format_topic(channel: &str, market: Option<&str>) -> String {
    let clean_channel = channel.to_lowercase();
    if let Some(m) = market {
        let clean_market = m.to_uppercase();
        format!("{clean_channel}:{clean_market}")
    } else {
        clean_channel
    }
}
