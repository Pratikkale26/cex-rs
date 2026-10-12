//! Prometheus metrics definitions and text encoder for the exchange gateway.

use once_cell::sync::Lazy;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
    Encoder, HistogramVec, IntCounterVec, IntGauge, TextEncoder,
};

/// HTTP request latency histogram broken down by method, endpoint, and response status.
pub static HTTP_REQUEST_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request latency in seconds",
        &["method", "endpoint", "status"],
        vec![0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .expect("Failed to register http_request_duration_seconds")
});

/// Total number of orders submitted to the exchange.
pub static ORDERS_SUBMITTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "orders_submitted_total",
        "Total orders submitted to the exchange",
        &["market", "side", "order_type"]
    )
    .expect("Failed to register orders_submitted_total")
});

/// Total number of trades executed across all markets.
pub static TRADES_EXECUTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "trades_executed_total",
        "Total trades executed across markets",
        &["market"]
    )
    .expect("Failed to register trades_executed_total")
});

/// Accumulating exchange fee revenues collected in the treasury (user_id = 0).
pub static TREASURY_FEES_COLLECTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "treasury_fees_collected_total",
        "Accumulating exchange fee revenue collected in treasury",
        &["currency"]
    )
    .expect("Failed to register treasury_fees_collected_total")
});

/// Current count of active, connected WebSocket client sessions.
pub static WS_ACTIVE_CONNECTIONS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "ws_active_connections",
        "Current number of active client WebSocket sessions"
    )
    .expect("Failed to register ws_active_connections")
});

/// Total number of WebSocket broadcast messages sent across topics.
pub static WS_MESSAGES_BROADCAST: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "ws_messages_broadcast_total",
        "Total WebSocket messages broadcast across topics",
        &["channel"]
    )
    .expect("Failed to register ws_messages_broadcast_total")
});

/// Encodes all gathered Prometheus metrics into standard text format.
pub fn render_metrics() -> Result<String, prometheus::Error> {
    // Ensure all lazy metrics are initialized so they appear in /metrics even before first increment
    Lazy::force(&HTTP_REQUEST_DURATION);
    Lazy::force(&ORDERS_SUBMITTED);
    Lazy::force(&TRADES_EXECUTED);
    Lazy::force(&TREASURY_FEES_COLLECTED);
    Lazy::force(&WS_ACTIVE_CONNECTIONS);
    Lazy::force(&WS_MESSAGES_BROADCAST);

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    String::from_utf8(buffer).map_err(|e| prometheus::Error::Msg(e.to_string()))
}
