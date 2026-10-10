use std::sync::Arc;
use redis::streams::StreamReadReply;
use rs_shared::*;
use super::hub::BroadcasterHub;

/// Spawns the background Redis Stream listener that forwards execution events to the BroadcasterHub.
pub fn start_ws_stream_listener(
    client: redis::Client,
    hub: Arc<BroadcasterHub>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_listener(client, hub).await;
    })
}

async fn run_listener(client: redis::Client, hub: Arc<BroadcasterHub>) {
    #[allow(deprecated)]
    let mut conn = match client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to Redis for WS stream listener: {e}");
            return;
        }
    };

    // Read only new events arriving from this moment forward using "$"
    let mut last_id = "$".to_string();

    loop {
        let mut cmd = redis::cmd("XREAD");
        cmd.arg("BLOCK").arg(1000)
            .arg("COUNT").arg(100)
            .arg("STREAMS").arg(STREAM_EXECUTIONS)
            .arg(&last_id);

        let reply: Result<StreamReadReply, _> = cmd.query_async(&mut conn).await;
        let read_reply = match reply {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        for stream_key in read_reply.keys {
            for record in stream_key.ids {
                last_id = record.id.clone();

                let data_str = match record.map.get("data") {
                    Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
                    None => continue,
                };

                let event: ExecutionEvent = match serde_json::from_str(&data_str) {
                    Ok(ev) => ev,
                    Err(_) => continue,
                };

                match event {
                    ExecutionEvent::TradeExecuted {
                        maker_order_id,
                        taker_order_id,
                        buyer_id,
                        seller_id,
                        market,
                        price,
                        qty,
                        quote_amount,
                        maker_remaining_qty,
                        taker_remaining_qty,
                        buyer_fee: _,
                        seller_fee: _,
                    } => {
                        let timestamp = chrono::Utc::now().timestamp_millis();

                        // 1. Broadcast public trade tick to "trades:<MARKET>"
                        let trade_msg = WsServerMsg::Trade {
                            market: market.clone(),
                            maker_order_id,
                            taker_order_id,
                            price,
                            qty,
                            quote_amount,
                            timestamp,
                        };
                        let topic = format!("trades:{}", market.to_uppercase());
                        hub.broadcast_topic(&topic, &trade_msg).await;

                        // 2. Send private order fill updates to Buyer
                        let buyer_status = if taker_remaining_qty == 0 { "filled" } else { "partially_filled" };
                        let buyer_update = WsServerMsg::OrderUpdate {
                            order_id: taker_order_id,
                            market: market.clone(),
                            side: "bid".to_string(),
                            price,
                            remaining_qty: taker_remaining_qty,
                            status: buyer_status.to_string(),
                        };
                        hub.send_private(buyer_id, &buyer_update).await;

                        // 3. Send private order fill updates to Seller
                        let seller_status = if maker_remaining_qty == 0 { "filled" } else { "partially_filled" };
                        let seller_update = WsServerMsg::OrderUpdate {
                            order_id: maker_order_id,
                            market: market.clone(),
                            side: "ask".to_string(),
                            price,
                            remaining_qty: maker_remaining_qty,
                            status: seller_status.to_string(),
                        };
                        hub.send_private(seller_id, &seller_update).await;
                    }

                    ExecutionEvent::OrderCreated {
                        order_id,
                        user_id,
                        market,
                        side,
                        price,
                        original_qty: _,
                        remaining_qty,
                        status,
                    } => {
                        let update = WsServerMsg::OrderUpdate {
                            order_id,
                            market,
                            side,
                            price,
                            remaining_qty,
                            status,
                        };
                        hub.send_private(user_id, &update).await;
                    }

                    ExecutionEvent::OrderCancelled {
                        order_id,
                        user_id,
                        remaining_qty,
                    } => {
                        let update = WsServerMsg::OrderUpdate {
                            order_id,
                            market: "UNKNOWN".to_string(),
                            side: "unknown".to_string(),
                            price: 0,
                            remaining_qty,
                            status: "cancelled".to_string(),
                        };
                        hub.send_private(user_id, &update).await;
                    }

                    ExecutionEvent::FundingExecuted { .. } => {}
                }
            }
        }
    }
}
