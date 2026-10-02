use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn get_orderbook(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, actix_web::Error> {
    let asset = path.into_inner();
    if asset != "sol" {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "message": "Only SOL is supported"
        })));
    }

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OrderbookQueryMsg {
        asset: asset.clone(),
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: OrderbookReply = send_and_wait(&state, CH_ORDERBOOK, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "asset": asset,
        "orderbook": reply.orderbook,
    })))
}
