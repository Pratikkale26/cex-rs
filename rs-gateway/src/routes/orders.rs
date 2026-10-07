use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::auth::AuthUser;
use crate::dto::{OrderBody, OrderRecord, TradeRecord, Validate};
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn place_order(
    state: web::Data<AppState>,
    auth: AuthUser,
    body: web::Json<OrderBody>,
) -> Result<HttpResponse, actix_web::Error> {
    if let Err(err) = body.validate() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "message": err
        })));
    }

    let order_type = match body.order_type.to_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit
    };

    let time_in_force = match body.time_in_force.as_deref().map(|s| s.to_uppercase()).as_deref() {
        Some("IOC") => TimeInForce::Ioc,
        Some("FOK") => TimeInForce::Fok,
        _ => TimeInForce::Gtc,
    };

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OrderMsg {
        user_id: auth.0,
        asset: body.asset.clone(),
        side: body.side.clone(),
        order_type,
        price: body.price,
        qty: body.qty,
        slippage_pct: body.slippage_pct,
        time_in_force,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: OrderReply = send_and_wait(&state, CH_ORDER, &msg, &identifier).await?;

    if let Some(err) = reply.error {
        let code = actix_web::http::StatusCode::from_u16(reply.status_code.unwrap_or(500))
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        return Ok(HttpResponse::build(code).json(serde_json::json!({
            "message": err
        })));
    }

    Ok(HttpResponse::Created().json(serde_json::json!({
        "orderId": reply.order_id,
        "status": reply.status.as_deref().unwrap_or("open"),
    })))
}

pub async fn cancel_order(
    state: web::Data<AppState>,
    auth: AuthUser,
    path: web::Path<u64>,
) -> Result<HttpResponse, actix_web::Error> {
    let order_id = path.into_inner();
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = CancelMsg {
        user_id: auth.0,
        order_id,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: CancelReply = send_and_wait(&state, CH_CANCEL, &msg, &identifier).await?;

    if let Some(err) = reply.error {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "message": err
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "orderId": reply.order_id,
        "remainingQty": reply.remaining_qty,
        "message": reply.message.as_deref().unwrap_or("Order cancelled"),
    })))
}

pub async fn get_open_orders(
    state: web::Data<AppState>,
    auth: AuthUser,
) -> Result<HttpResponse, actix_web::Error> {
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OpenOrdersQueryMsg {
        user_id: auth.0,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: OpenOrdersReply = send_and_wait(&state, CH_OPEN_ORDERS, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "orders": reply.orders,
    })))
}

pub async fn get_order(
    state: web::Data<AppState>,
    auth: AuthUser,
    path: web::Path<u64>,
) -> Result<HttpResponse, actix_web::Error> {
    let order_id = path.into_inner();
    let user_id = auth.0 as i64;

    // 1. Check PostgreSQL cold-path database first (for settled status & fill history)
    let order_row = sqlx::query_as::<_, OrderRecord>(
        r#"
        SELECT id, user_id, market, side, price, original_qty, remaining_qty, status, created_at, updated_at
        FROM orders
        WHERE id = $1
        "#
    )
    .bind(order_id as i64)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Database error fetching order: {e}");
        actix_web::error::ErrorInternalServerError("Database error")
    })?;

    if let Some(order) = order_row {
        if order.user_id != user_id {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "message": "Order not found"
            })));
        }

        let trades = sqlx::query_as::<_, TradeRecord>(
            r#"
            SELECT id, maker_order_id, taker_order_id, buyer_id, seller_id,
                   market, price, qty, quote_amount, created_at
            FROM trades
            WHERE maker_order_id = $1 OR taker_order_id = $1
            ORDER BY created_at ASC
            "#
        )
        .bind(order_id as i64)
        .fetch_all(&state.db)
        .await
        .map_err(|e| {
            eprintln!("Database error fetching trades for order: {e}");
            actix_web::error::ErrorInternalServerError("Database error")
        })?;

        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "order": order,
            "trades": trades,
        })));
    }

    // 2. Fallback: Query live matching engine memory (for unbatched in-flight open orders)
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OrderStatusQueryMsg {
        user_id: auth.0,
        order_id,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: OrderStatusReply = send_and_wait(&state, CH_ORDER_STATUS, &msg, &identifier).await?;

    if let Some(o) = reply.order {
        let now = chrono::Utc::now();
        let fallback_order = OrderRecord {
            id:            o.order_id as i64,
            user_id:       o.user_id as i64,
            market:        "SOL_USD".to_string(),
            side:          o.side,
            price:         o.price as i64,
            original_qty:  o.remaining_qty as i64,
            remaining_qty: o.remaining_qty as i64,
            status:        reply.status.unwrap_or_else(|| "open".to_string()),
            created_at:    now,
            updated_at:    now,
        };

        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "order": fallback_order,
            "trades": [],
        })));
    }

    Ok(HttpResponse::NotFound().json(serde_json::json!({
        "message": "Order not found"
    })))
}
