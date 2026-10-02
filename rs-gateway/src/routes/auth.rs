use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::auth::{Claims, User, JWT_SECRET};
use crate::dto::{SigninBody, SignupBody};
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn signup(
    state: web::Data<AppState>,
    body: web::Json<SignupBody>,
) -> Result<HttpResponse, actix_web::Error> {
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

pub async fn signin(
    state: web::Data<AppState>,
    body: web::Json<SigninBody>,
) -> Result<HttpResponse, actix_web::Error> {
    let users = state.users.lock().await;
    let user = users.iter().find(|u| u.username == body.username && u.password == body.password);

    match user {
        Some(u) => {
            let exp = chrono::Utc::now().timestamp() as usize + 24 * 3600;
            let claims = Claims { sub: u.id, exp };
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &jsonwebtoken::EncodingKey::from_secret(JWT_SECRET),
            ).map_err(actix_web::error::ErrorInternalServerError)?;
            Ok(HttpResponse::Ok().json(serde_json::json!({ "token": token })))
        }
        None => Ok(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "Incorrect credentials"
        }))),
    }
}
