use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::auth::AuthUser;
use crate::dto::{DepositBody, OnrampBody};
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn get_balance(
    state: web::Data<AppState>,
    auth: AuthUser,
) -> Result<HttpResponse, actix_web::Error> {
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = BalanceQueryMsg {
        user_id: auth.0,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: BalanceReply = send_and_wait(&state, CH_BALANCE, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "user Balance",
        "usdBalance": reply.usd_balance,
        "stockBalance": reply.stock_balance,
    })))
}

pub async fn onramp(
    state: web::Data<AppState>,
    auth: AuthUser,
    body: web::Json<OnrampBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OnrampMsg {
        user_id: auth.0,
        qty: body.qty,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: BalanceReply = send_and_wait(&state, CH_ONRAMP, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "onramped successfully",
        "usdBalance": reply.usd_balance,
        "stockBalance": reply.stock_balance,
    })))
}

pub async fn deposit(
    state: web::Data<AppState>,
    auth: AuthUser,
    path: web::Path<String>,
    body: web::Json<DepositBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let asset = path.into_inner();
    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = DepositMsg {
        user_id: auth.0,
        symbol: asset,
        qty: body.qty,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: BalanceReply = send_and_wait(&state, CH_DEPOSIT, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Deposited successfully",
        "usdBalance": reply.usd_balance,
        "stockBalance": reply.stock_balance,
    })))
}
