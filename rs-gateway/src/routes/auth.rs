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

    // 1. Hash password with Argon2
    let hashed_password = hash_password(&body.password)
        .await
        .map_err(|e| {
            eprintln!("Password hashing error: {e}");
            actix_web::error::ErrorInternalServerError("Failed to hash password")
        })?;

    // 2. Insert user into PostgreSQL (database unique constraint prevents duplicates)
    let insert_result = sqlx::query_as::<_, User>(
        "INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id, username, password_hash, created_at"
    )
    .bind(&body.username)
    .bind(&hashed_password)
    .fetch_one(&state.db)
    .await;

    let user = match insert_result {
        Ok(u) => u,
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
                "message": "User already"
            })));
        }
        Err(e) => {
            eprintln!("Database error during signup: {e}");
            return Err(actix_web::error::ErrorInternalServerError("Database error"));
        }
    };

    let user_id = user.id as u64;

    // 3. Send signup event to Engine to initialize user balances
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

    // 1. Lookup user in PostgreSQL
    let user_opt = sqlx::query_as::<_, User>(
        "SELECT id, username, password_hash, created_at FROM users WHERE username = $1"
    )
    .bind(&body.username)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Database error during signin: {e}");
        actix_web::error::ErrorInternalServerError("Database error")
    })?;

    let user = match user_opt {
        Some(u) => u,
        None => {
            return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
                "message": "Incorrect credentials"
            })));
        }
    };

    // 2. Verify password with Argon2
    let is_valid = verify_password(&body.password, &user.password_hash).await;
    if !is_valid {
        return Ok(HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "Incorrect credentials"
        })));
    }

    // 3. Issue JWT
    let exp = chrono::Utc::now().timestamp() as usize + 24 * 3600;
    let claims = Claims { sub: user.id as u64, exp };
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(JWT_SECRET),
    ).map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "token": token })))
}
