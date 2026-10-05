//! Async cold-path batch worker for trades, orders, and double-entry ledger entries.

use redis::streams::StreamReadReply;
use rs_shared::*;
use sqlx::PgPool;

/// Spawns the background cold-path worker task.
pub fn start_execution_worker(
    client: redis::Client,
    db_pool: PgPool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_worker(client, db_pool).await;
    })
}

async fn run_worker(client: redis::Client, db_pool: PgPool) {
    #[allow(deprecated)]
    let mut conn = match client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to Redis for execution worker: {e}");
            return;
        }
    };

    // 1. Fetch the last committed checkpoint from PostgreSQL
    let mut last_id: String = sqlx::query_scalar(
        "SELECT last_stream_id FROM execution_checkpoints WHERE id = 1"
    )
    .fetch_one(&db_pool)
    .await
    .unwrap_or_else(|_| "0-0".to_string());

    loop {
        // 2. Read events using XREAD BLOCK 1000 COUNT 100 STREAMS engine-executions <last_id>
        let mut cmd = redis::cmd("XREAD");
        cmd.arg("BLOCK").arg(1000)
            .arg("COUNT").arg(100)
            .arg("STREAMS").arg(STREAM_EXECUTIONS)
            .arg(&last_id);

        let reply: Result<StreamReadReply, _> = cmd.query_async(&mut conn).await;
        let read_reply = match reply {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        for stream_key in read_reply.keys {
            if stream_key.ids.is_empty() {
                continue;
            }

            // 3. Process the batch inside a single PostgreSQL Transaction
            let mut tx = match db_pool.begin().await {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("Failed to begin DB transaction: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    break;
                }
            };

            let mut batch_last_id = last_id.clone();

            for record in stream_key.ids {
                batch_last_id = record.id.clone();
                let data_str = match record.map.get("data") {
                    Some(val) => redis::from_redis_value::<String>(val).unwrap_or_default(),
                    None => continue,
                };

                let event: ExecutionEvent = match serde_json::from_str(&data_str) {
                    Ok(ev) => ev,
                    Err(_) => continue,
                };

                match event {
                    ExecutionEvent::OrderCreated {
                        order_id,
                        user_id,
                        market,
                        side,
                        price,
                        original_qty,
                        remaining_qty,
                        status,
                    } => {
                        let _ = sqlx::query(
                            r#"
                            INSERT INTO orders (id, user_id, market, side, price, original_qty, remaining_qty, status, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                remaining_qty = EXCLUDED.remaining_qty,
                                status = EXCLUDED.status,
                                updated_at = NOW()
                            "#
                        )
                        .bind(order_id as i64)
                        .bind(user_id as i64)
                        .bind(&market)
                        .bind(&side)
                        .bind(price as i64)
                        .bind(original_qty as i64)
                        .bind(remaining_qty as i64)
                        .bind(&status)
                        .execute(&mut *tx)
                        .await;
                    }
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
                        taker_remaining_qty: _,
                    } => {
                        // 1. Record trade
                        let _ = sqlx::query(
                            r#"
                            INSERT INTO trades (maker_order_id, taker_order_id, buyer_id, seller_id, market, price, qty, quote_amount)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            "#
                        )
                        .bind(maker_order_id as i64)
                        .bind(taker_order_id as i64)
                        .bind(buyer_id as i64)
                        .bind(seller_id as i64)
                        .bind(&market)
                        .bind(price as i64)
                        .bind(qty as i64)
                        .bind(quote_amount as i64)
                        .execute(&mut *tx)
                        .await;

                        // 2. Update maker order remaining_qty and status
                        let maker_status = if maker_remaining_qty == 0 { "filled" } else { "partially_filled" };
                        let _ = sqlx::query(
                            r#"
                            UPDATE orders
                            SET remaining_qty = $1, status = $2, updated_at = NOW()
                            WHERE id = $3
                            "#
                        )
                        .bind(maker_remaining_qty as i64)
                        .bind(maker_status)
                        .bind(maker_order_id as i64)
                        .execute(&mut *tx)
                        .await;

                        // 3. Double-entry ledger entries for trade:
                        let trade_ref = format!("{}-{}", maker_order_id, taker_order_id);

                        // Buyer: -quote_amount USD (debit from locked), +qty SOL (credit to available)
                        let _ = sqlx::query(
                            r#"
                            INSERT INTO ledger_entries (user_id, currency, amount, balance_type, operation_type, reference_id)
                            VALUES ($1, 'USD', $2, 'locked', 'trade_fill', $3),
                                   ($1, 'SOL', $4, 'available', 'trade_fill', $3)
                            "#
                        )
                        .bind(buyer_id as i64)
                        .bind(-(quote_amount as i64))
                        .bind(&trade_ref)
                        .bind(qty as i64)
                        .execute(&mut *tx)
                        .await;

                        // Seller: +quote_amount USD (credit to available), -qty SOL (debit from locked)
                        let _ = sqlx::query(
                            r#"
                            INSERT INTO ledger_entries (user_id, currency, amount, balance_type, operation_type, reference_id)
                            VALUES ($1, 'USD', $2, 'available', 'trade_fill', $3),
                                   ($1, 'SOL', $4, 'locked', 'trade_fill', $3)
                            "#
                        )
                        .bind(seller_id as i64)
                        .bind(quote_amount as i64)
                        .bind(&trade_ref)
                        .bind(-(qty as i64))
                        .execute(&mut *tx)
                        .await;
                    }
                    ExecutionEvent::OrderCancelled {
                        order_id,
                        user_id: _,
                        remaining_qty,
                    } => {
                        let _ = sqlx::query(
                            r#"
                            UPDATE orders
                            SET remaining_qty = $1, status = 'cancelled', updated_at = NOW()
                            WHERE id = $2
                            "#
                        )
                        .bind(remaining_qty as i64)
                        .bind(order_id as i64)
                        .execute(&mut *tx)
                        .await;
                    }
                    ExecutionEvent::FundingExecuted {
                        user_id,
                        currency,
                        amount,
                        operation_type,
                    } => {
                        let funding_ref = format!("{}_{}", operation_type, uuid::Uuid::new_v4());

                        // Double-entry: User credited (+amount), System Clearing (user_id = 0) debited (-amount)
                        let _ = sqlx::query(
                            r#"
                            INSERT INTO ledger_entries (user_id, currency, amount, balance_type, operation_type, reference_id)
                            VALUES ($1, $2, $3, 'available', $4, $5),
                                   (0,  $2, $6, 'clearing',  $4, $5)
                            "#
                        )
                        .bind(user_id as i64)
                        .bind(&currency)
                        .bind(amount as i64)
                        .bind(&operation_type)
                        .bind(&funding_ref)
                        .bind(-(amount as i64))
                        .execute(&mut *tx)
                        .await;
                    }
                }
            }

            // Update checkpoint
            let _ = sqlx::query(
                "UPDATE execution_checkpoints SET last_stream_id = $1, updated_at = NOW() WHERE id = 1"
            )
            .bind(&batch_last_id)
            .execute(&mut *tx)
            .await;

            if let Ok(()) = tx.commit().await {
                last_id = batch_last_id;
            }
        }
    }
}
