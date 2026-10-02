use actix_web::{web, HttpResponse};
use rs_shared::*;
use crate::redis::send_and_wait;
use crate::state::AppState;

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
