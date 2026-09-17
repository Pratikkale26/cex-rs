use actix_web::{web, HttpResponse};
use redis::AsyncCommands;
use rs_shared::*;
use crate::middleware::AuthUser;
use crate::types::*;
use crate::AppState;

pub async fn send_and_wait<T: serde::de::DeserializeOwned, M: serde::Serialize>(
    state: &web::Data<AppState>,
    channel: &str,
    msg: &M,
    identifier: &str,
) -> Result<T, actix_web::Error> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut pending = state.pending.lock().await;
        pending.insert(identifier.to_string(), tx);
    }

    let json_str = serde_json::to_string(msg)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    {
        let mut publisher = state.redis_publisher.lock().await;
        let _: () = publisher.lpush(channel, json_str).await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    }

    let reply_str = rx.await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    serde_json::from_str::<T>(&reply_str)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))
}

pub async fn signup(state: web::Data<AppState>, body: web::Json<SignupBody>) -> Result<HttpResponse, actix_web::Error> {
    let mut users = state.users.lock().await;
    if users.iter().any(|u| u.username == body.username) {
        return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "User already"
        })));
    }

    let user_id = state.user_index.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    users.push(User {
        id: user_id,
        username: body.username.clone(),
        password: body.password.clone(),
    });
    drop(users);

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = SignupMsg {
        user_id,
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let reply: BalanceReply = send_and_wait(&state, CH_SIGNUP, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Signed up successfully",
        "usdBalance": reply.usd_balance,
        "stockBalance": reply.stock_balance,
    })))
}

pub async fn signin(state: web::Data<AppState>, body: web::Json<SigninBody>) -> Result<HttpResponse, actix_web::Error> {
    let users = state.users.lock().await;
    let user = users.iter().find(|u| u.username == body.username && u.password == body.password);

    match user {
        Some(u) => {
            let exp = chrono::Utc::now().timestamp() as usize + 24 * 3600;
            let claims = Claims { sub: u.id, exp };
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &jsonwebtoken::EncodingKey::from_secret(crate::middleware::JWT_SECRET),
            ).map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
            Ok(HttpResponse::Ok().json(serde_json::json!({ "token": token })))
        }
        None => Ok(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "Incorrect credentials"
        }))),
    }
}

pub async fn get_balance(state: web::Data<AppState>, auth: AuthUser) -> Result<HttpResponse, actix_web::Error> {
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

pub async fn onramp(state: web::Data<AppState>, auth: AuthUser, body: web::Json<OnrampBody>) -> Result<HttpResponse, actix_web::Error> {
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

pub async fn place_order(
    state: web::Data<AppState>,
    auth: AuthUser,
    body: web::Json<OrderBody>,
) -> Result<HttpResponse, actix_web::Error> {
    if body.asset != "sol" {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "message": "Only SOL orders are supported"
        })));
    }

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = OrderMsg {
        user_id: auth.0,
        asset: body.asset.clone(),
        side: body.side.clone(),
        price: body.price,
        qty: body.qty,
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

pub async fn reset(
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    {
        let mut users = state.users.lock().await;
        users.clear();
        state.user_index.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    let identifier = uuid::Uuid::new_v4().to_string();
    let msg = ResetMsg {
        queue_id: state.queue_id.clone(),
        identifier: identifier.clone(),
    };

    let _reply: GenericReply = send_and_wait(&state, CH_RESET, &msg, &identifier).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Reset successful"
    })))
}
