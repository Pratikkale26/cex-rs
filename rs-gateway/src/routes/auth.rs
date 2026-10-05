use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::auth::{hash_password, verify_password, Claims, User, JWT_SECRET};
use crate::dto::{SigninBody, SignupBody, Validate};
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn signup(
    state: web::Data<AppState>,
    body: web::Json<SignupBody>,
) -> Result<HttpResponse, actix_web::Error> {
    // 0. Validate input format
    if let Err(err) = body.validate() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "message": err
        })));
    }

    // 1. Check if user already exists
    {
        let users = state.users.lock().await;
        if users.iter().any(|u| u.username == body.username) {
            return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
                "message": "User already"
            })));
        }
    }

    // 2. Hash password with Argon2 outside the lock
    let hashed_password = hash_password(&body.password)
        .await
        .map_err(|e| {
            eprintln!("Password hashing error: {e}");
            actix_web::error::ErrorInternalServerError("Failed to hash password")
        })?;

    // 3. Register user in gateway store
    let user_id = {
        let mut users = state.users.lock().await;
        if users.iter().any(|u| u.username == body.username) {
            return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
                "message": "User already"
            })));
        }
        let id = state.user_index.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        users.push(User {
            id,
            username: body.username.clone(),
            password: hashed_password,
        });
        id
    };

    // 4. Send signup event to Engine to initialize user balances
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
    // 0. Validate input format
    if let Err(err) = body.validate() {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "message": err
        })));
    }

    // 1. Lookup user in store
    let (user_id, stored_hash) = {
        let users = state.users.lock().await;
        match users.iter().find(|u| u.username == body.username) {
            Some(u) => (u.id, u.password.clone()),
            None => {
                return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
                    "message": "Incorrect credentials"
                })));
            }
        }
    }; // Lock is dropped here!

    // 2. Verify password with Argon2 (CPU-bound task in background thread)
    let is_valid = verify_password(&body.password, &stored_hash).await;
    if !is_valid {
        return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "Incorrect credentials"
        })));
    }

    // 3. Issue JWT
    let exp = chrono::Utc::now().timestamp() as usize + 24 * 3600;
    let claims = Claims { sub: user_id, exp };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(JWT_SECRET),
    ).map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "token": token })))
}
