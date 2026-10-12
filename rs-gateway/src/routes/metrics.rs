use actix_web::{HttpResponse, Error};
use crate::metrics::render_metrics;

/// HTTP route handler for GET /metrics scraping endpoint.
pub async fn get_metrics() -> Result<HttpResponse, Error> {
    match render_metrics() {
        Ok(body) => Ok(HttpResponse::Ok()
            .content_type("text/plain; version=0.0.4; charset=utf-8")
            .body(body)),
        Err(e) => Ok(HttpResponse::InternalServerError().body(format!("Metrics encode error: {e}"))),
    }
}
