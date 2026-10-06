use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::auth::AuthUser;
use crate::dto::{OrderBody, Validate};
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

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OrderMsg {
        user_id: auth.0,
        asset: body.asset.clone(),
        side: body.side.clone(),
        order_type,
        price: body.price,
        qty: body.qty,
        slippage_pct: body.slippage_pct,
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
