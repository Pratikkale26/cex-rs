use actix_web::{web, HttpRequest, HttpResponse, Error};
use crate::state::AppState;
use crate::ws::session::handle_ws_session;

/// HTTP route handler that upgrades client connections to duplex WebSockets.
pub async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let (response, session, msg_stream) = actix_ws::handle(&req, stream)?;

    let hub = state.ws_hub.clone();
    actix_web::rt::spawn(async move {
        handle_ws_session(session, msg_stream, hub).await;
    });

    Ok(response)
}
