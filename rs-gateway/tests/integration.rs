//! Native Rust integration tests for the CEX Gateway and Matching Engine.
//! Replaces external Node.js / Bun tests with pure Rust tests.

use actix_web::{http::StatusCode, test, web, App};
use rs_gateway::state::AppState;
use rs_gateway::*;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Helper to initialize test app state connected to local Redis.
async fn setup_app_state() -> Option<(web::Data<AppState>, tokio::task::JoinHandle<()>)> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = ::redis::Client::open(redis_url.as_str()).ok()?;

    #[allow(deprecated)]
    let listener = client.get_async_connection().await.ok()?;
    let publisher = client.get_multiplexed_async_connection().await.ok()?;

    let queue_id = uuid::Uuid::new_v4().to_string();
    let reply_queue = format!("{}{}", rs_shared::REPLY_PREFIX, queue_id);
    let pending = Arc::new(Mutex::new(HashMap::new()));

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgrespassword@127.0.0.1:5432/cex".to_string());
    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok()?;

    let _ = sqlx::migrate!("./migrations").run(&db_pool).await;

    let _listener_handle = rs_gateway::redis::start_response_listener(listener, Arc::clone(&pending), reply_queue);
    let app_state = web::Data::new(AppState::new(db_pool, publisher, pending, queue_id));

    // Spawn an in-process Matching Engine for the integration test
    #[allow(deprecated)]
    let engine_listener = client.get_async_connection().await.ok()?;
    let engine_publisher = client.get_multiplexed_async_connection().await.ok()?;
    let engine_state = Arc::new(Mutex::new(rs_engine::EngineState::new()));
    let engine_handle = tokio::spawn(async move {
        rs_engine::run_engine(engine_state, engine_listener, engine_publisher).await;
    });

    // Give engine a brief moment to connect
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Some((app_state, engine_handle))
}

#[actix_web::test]
async fn test_validation_rejects_invalid_signup() {
    let (app_state, _engine) = match setup_app_state().await {
        Some(res) => res,
        None => return,
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/signup", web::post().to(routes::auth::signup)),
    )
    .await;

    // Username too short (min 3 chars required)
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "al",
            "password": "valid_password123"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Password too short (min 6 chars required)
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "alice",
            "password": "123"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_unauthorized_endpoints_without_jwt() {
    let (app_state, _engine) = match setup_app_state().await {
        Some(res) => res,
        None => return,
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .route("/balance", web::get().to(routes::balance::get_balance))
            .route("/order", web::post().to(routes::orders::place_order))
            .route("/orders/open", web::get().to(routes::orders::get_open_orders)),
    )
    .await;

    // Missing Authorization header
    let req = test::TestRequest::get().uri("/balance").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Malformed token
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", "Bearer invalid.token.payload"))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
#[serial]
async fn test_e2e_trading_lifecycle() {
    let (app_state, engine_handle) = match setup_app_state().await {
        Some(res) => res,
        None => {
            eprintln!("Skipping E2E test: local Redis is not running");
            return;
        }
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .wrap(rate_limit::RateLimitMiddleware::default())
            .configure(routes::configure)
    ).await;

    // 1. Reset state
    let req = test::TestRequest::post().uri("/reset").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Signup
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "trader_alice",
            "password": "Password@123"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 3. Duplicate signup should be rejected
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "trader_alice",
            "password": "Password@123"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 4. Signin with wrong password
    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({
            "username": "trader_alice",
            "password": "WrongPassword!"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 5. Signin with correct password
    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({
            "username": "trader_alice",
            "password": "Password@123"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let signin_body: serde_json::Value = test::read_body_json(resp).await;
    let token = signin_body["token"].as_str().expect("Token must be present").to_string();

    // 6. Check initial balance
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 0);
    assert_eq!(bal["usdBalance"]["locked"], 0);

    // 7. Onramp USD
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({ "qty": 1000 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 1000);

    // 8. Place order (Buy 2 SOL at $100 -> costs $200 USD)
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "bid",
            "price": 100,
            "qty": 2
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let order_resp: serde_json::Value = test::read_body_json(resp).await;
    let order_id = order_resp["orderId"].as_u64().expect("orderId expected");
    assert_eq!(order_resp["status"], "open");

    // 9. Verify USD was locked
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 800);
    assert_eq!(bal["usdBalance"]["locked"], 200);

    // 10. Check open orders
    let req = test::TestRequest::get()
        .uri("/orders/open")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let open_orders: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(open_orders["orders"].as_array().unwrap().len(), 1);

    // 11. Check public orderbook depth
    let req = test::TestRequest::get().uri("/orderbook/sol").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ob: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(ob["orderbook"]["bids"][0]["price"], 100);
    assert_eq!(ob["orderbook"]["bids"][0]["qty"], 2);

    // 12. Cancel order
    let req = test::TestRequest::delete()
        .uri(&format!("/order/{order_id}"))
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 13. Verify USD was refunded
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 1000);
    assert_eq!(bal["usdBalance"]["locked"], 0);

    engine_handle.abort();
}
