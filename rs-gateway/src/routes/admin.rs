use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn reset(
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    // Truncate users table and reset auto-increment identity
    sqlx::query("TRUNCATE TABLE users RESTART IDENTITY CASCADE")
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("Database error during reset: {e}");
            actix_web::error::ErrorInternalServerError("Database reset failed")
        })?;

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
