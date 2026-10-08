use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::redis::send_and_wait;
use crate::state::AppState;

pub async fn reset(
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    // Clear tables cleanly using DELETE to prevent ACCESS EXCLUSIVE lock deadlocks with background workers
    let _ = sqlx::query("DELETE FROM ledger_entries").execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM trades").execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM orders").execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM engine_snapshots").execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM users").execute(&state.db).await;

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
