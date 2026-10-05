use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn reset(
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    // Truncate users, snapshots, orders, trades, and ledger tables
    sqlx::query("TRUNCATE TABLE users, engine_snapshots, orders, trades, ledger_entries RESTART IDENTITY CASCADE")
        .execute(&state.db)
        .await
        .map_err(|e| {
            eprintln!("Database error during reset: {e}");
            actix_web::error::ErrorInternalServerError("Database reset failed")
        })?;

    let _ = sqlx::query("UPDATE execution_checkpoints SET last_stream_id = '0-0', updated_at = NOW() WHERE id = 1")
        .execute(&state.db)
        .await;

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
