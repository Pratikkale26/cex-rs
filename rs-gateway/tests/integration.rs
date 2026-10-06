//! Native Rust integration tests for the CEX Gateway and Matching Engine.
//! Replaces external Node.js / Bun tests with pure Rust tests.

use actix_web::{http::StatusCode, test, web, App};
use rs_gateway::state::AppState;
use rs_gateway::*;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Helper to initialize test environment with app state, engine, db, and redis.
async fn setup_test_env() -> Option<(
    web::Data<AppState>,
    tokio::task::JoinHandle<()>,
    sqlx::PgPool,
    ::redis::Client,
    Arc<Mutex<rs_engine::EngineState>>,
)> {
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
    let _worker_handle = rs_gateway::ledger::start_execution_worker(client.clone(), db_pool.clone());
    let app_state = web::Data::new(AppState::new(db_pool.clone(), publisher, pending, queue_id));

    // Spawn an in-process Matching Engine for the integration test
    #[allow(deprecated)]
    let engine_listener = client.get_async_connection().await.ok()?;
    let engine_publisher = client.get_multiplexed_async_connection().await.ok()?;
    let engine_state = Arc::new(Mutex::new(rs_engine::EngineState::new()));
    let db_clone = db_pool.clone();
    let engine_state_clone = Arc::clone(&engine_state);
    let engine_handle = tokio::spawn(async move {
        rs_engine::run_engine_with_db(engine_state_clone, engine_listener, engine_publisher, Some(db_clone), "0-0".to_string()).await;
    });

    // Give engine a brief moment to connect
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Some((app_state, engine_handle, db_pool, client, engine_state))
}

/// Helper to initialize test app state connected to local Redis.
async fn setup_app_state() -> Option<(web::Data<AppState>, tokio::task::JoinHandle<()>)> {
    setup_test_env().await.map(|(state, handle, _, _, _)| (state, handle))
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

#[serial]
#[actix_web::test]
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

#[serial]
#[actix_web::test]
async fn test_engine_crash_snapshot_and_replay_recovery() {
    let (app_state, engine_handle, db_pool, redis_client, engine_state) = match setup_test_env().await {
        Some(res) => res,
        None => return,
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(routes::configure),
    )
    .await;

    // 1. Reset state
    let req = test::TestRequest::post().uri("/reset").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Sign up and login
    let signup_body = serde_json::json!({
        "username": "recovery_user",
        "email": "recovery@test.com",
        "password": "Password123!"
    });
    let req = test::TestRequest::post().uri("/signup").set_json(&signup_body).to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let login_body = serde_json::json!({
        "username": "recovery_user",
        "password": "Password123!"
    });
    let req = test::TestRequest::post().uri("/signin").set_json(&login_body).to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let login_resp: serde_json::Value = test::read_body_json(resp).await;
    let token = login_resp["token"].as_str().unwrap();

    // 3. Onramp $1,000 USD
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({ "qty": 1000 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 4. Place Order #1: Buy 2 SOL @ $100
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

    // 5. Take an explicit snapshot of the current state at the latest Redis stream ID
    let mut conn = redis_client.get_multiplexed_async_connection().await.unwrap();
    let xrev: Vec<(String, HashMap<String, String>)> = ::redis::cmd("XREVRANGE")
        .arg(rs_shared::STREAM_EVENTS)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!xrev.is_empty(), "Stream should contain events from onramp and order 1");
    let snapshot_stream_id = xrev[0].0.clone();

    {
        let locked_state = engine_state.lock().await;
        rs_engine::snapshot::save_snapshot(&db_pool, &snapshot_stream_id, &locked_state)
            .await
            .expect("Failed to save snapshot to postgres");
    }

    // 6. Place Order #2: Buy 3 SOL @ $90 (occurs AFTER the snapshot in the WAL stream)
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "bid",
            "price": 90,
            "qty": 3
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 7. Verify balance before crash: locked $470 (2*100 + 3*90), available $530
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 530);
    assert_eq!(bal["usdBalance"]["locked"], 470);

    // 8. SIMULATE ENGINE CRASH: abruptly abort the background task
    engine_handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 9. RECOVERY: Spin up a brand new matching engine instance with empty state
    // It must load the snapshot (Order #1) and replay the WAL events (Order #2)
    let recovered_state = Arc::new(Mutex::new(rs_engine::EngineState::new()));
    #[allow(deprecated)]
    let new_engine_listener = redis_client.get_async_connection().await.unwrap();
    let new_engine_publisher = redis_client.get_multiplexed_async_connection().await.unwrap();
    let db_clone = db_pool.clone();
    let recovered_state_clone = Arc::clone(&recovered_state);
    let new_engine_handle = tokio::spawn(async move {
        rs_engine::run_engine_with_db(
            recovered_state_clone,
            new_engine_listener,
            new_engine_publisher,
            Some(db_clone),
            "0-0".to_string(),
        )
        .await;
    });

    // Wait for recovery replay to finish and engine to start listening for live events
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 10. VERIFY STATE RECOVERY VIA GATEWAY
    // The recovered engine should accurately report balances and orderbook depth
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 530);
    assert_eq!(bal["usdBalance"]["locked"], 470);

    let req = test::TestRequest::get().uri("/orderbook/sol").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ob: serde_json::Value = test::read_body_json(resp).await;
    let bids = ob["orderbook"]["bids"].as_array().unwrap();
    assert_eq!(bids.len(), 2);
    assert_eq!(bids[0]["price"], 100);
    assert_eq!(bids[0]["qty"], 2);
    assert_eq!(bids[1]["price"], 90);
    assert_eq!(bids[1]["qty"], 3);

    // 11. SUBMIT LIVE ORDER TO RECOVERED ENGINE: Buy 1 SOL @ $80
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "bid",
            "price": 80,
            "qty": 1
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["usdBalance"]["available"], 450);
    assert_eq!(bal["usdBalance"]["locked"], 550);

    new_engine_handle.abort();
}

#[serial]
#[actix_web::test]
async fn test_double_entry_ledger_and_trade_history() {
    let (app_state, engine_handle, db_pool, _redis_client, _engine_state) = match setup_test_env().await {
        Some(res) => res,
        None => return,
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(routes::configure),
    )
    .await;

    // 1. Reset state
    let req = test::TestRequest::post().uri("/reset").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Sign up and sign in Alice (buyer)
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "ledger_alice",
            "password": "Password123!"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({
            "username": "ledger_alice",
            "password": "Password123!"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let alice_login: serde_json::Value = test::read_body_json(resp).await;
    let alice_token = alice_login["token"].as_str().unwrap();

    // 3. Sign up and sign in Bob (seller)
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({
            "username": "ledger_bob",
            "password": "Password123!"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({
            "username": "ledger_bob",
            "password": "Password123!"
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bob_login: serde_json::Value = test::read_body_json(resp).await;
    let bob_token = bob_login["token"].as_str().unwrap();

    // 4. Fund Alice: Onramp $1,000 USD
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .set_json(serde_json::json!({ "qty": 1000 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. Fund Bob: Onramp $500 USD and Deposit 10 SOL
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({ "qty": 500 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = test::TestRequest::post()
        .uri("/deposit/sol")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({ "qty": 10 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 6. Alice places Maker Order: Buy 5 SOL @ $100
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "bid",
            "price": 100,
            "qty": 5
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 7. Bob places Taker Order: Sell 3 SOL @ $100 (matches against Alice)
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "ask",
            "price": 100,
            "qty": 3
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 8. Allow cold-path worker to drain executions into PostgreSQL
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 9. Query public market trades: GET /trades/sol
    let req = test::TestRequest::get().uri("/trades/sol").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let public_trades: serde_json::Value = test::read_body_json(resp).await;
    let trades = public_trades["trades"].as_array().expect("trades array");
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0]["price"], 100);
    assert_eq!(trades[0]["qty"], 3);
    assert_eq!(trades[0]["quoteAmount"], 300);

    // 10. Query Alice's trade history: GET /trades/my
    let req = test::TestRequest::get()
        .uri("/trades/my")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let my_trades: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(my_trades["trades"].as_array().unwrap().len(), 1);

    // 11. Query Bob's trade history: GET /trades/my
    let req = test::TestRequest::get()
        .uri("/trades/my")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bob_trades: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bob_trades["trades"].as_array().unwrap().len(), 1);

    // 12. Query Alice's financial ledger statement: GET /ledger
    let req = test::TestRequest::get()
        .uri("/ledger")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let alice_ledger: serde_json::Value = test::read_body_json(resp).await;
    let entries = alice_ledger["ledger"].as_array().expect("ledger entries");
    assert!(entries.len() >= 3, "Alice should have onramp and 2 trade leg entries (USD debit, SOL credit)");

    // 13. Query Bob's financial ledger statement: GET /ledger
    let req = test::TestRequest::get()
        .uri("/ledger")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bob_ledger: serde_json::Value = test::read_body_json(resp).await;
    let entries = bob_ledger["ledger"].as_array().expect("ledger entries");
    assert!(entries.len() >= 4, "Bob should have onramp, deposit, and 2 trade leg entries");

    // 14. MATHEMATICAL VERIFICATION OF DOUBLE-ENTRY INVARIANT:
    // Sum of all entries in the ledger table for every currency must be EXACTLY ZERO!
    let usd_sum: Option<i64> = sqlx::query_scalar("SELECT COALESCE(SUM(amount), 0)::BIGINT FROM ledger_entries WHERE currency = 'USD'")
        .fetch_one(&db_pool)
        .await
        .unwrap();
    assert_eq!(usd_sum, Some(0), "Total USD ledger sum must be exactly 0 (Double-entry conservation)");

    let sol_sum: Option<i64> = sqlx::query_scalar("SELECT COALESCE(SUM(amount), 0)::BIGINT FROM ledger_entries WHERE currency = 'SOL'")
        .fetch_one(&db_pool)
        .await
        .unwrap();
    assert_eq!(sol_sum, Some(0), "Total SOL ledger sum must be exactly 0 (Double-entry conservation)");

    engine_handle.abort();
}

#[serial]
#[actix_web::test]
async fn test_market_order_e2e_lifecycle() {
    let (app_state, engine_handle) = match setup_app_state().await {
        Some(res) => res,
        None => return,
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

    // 2. Signup Alice
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({ "username": "alice_market", "password": "Password@123" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({ "username": "alice_market", "password": "Password@123" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(resp).await;
    let alice_token = body["token"].as_str().unwrap().to_string();

    // 3. Signup Bob
    let req = test::TestRequest::post()
        .uri("/signup")
        .set_json(serde_json::json!({ "username": "bob_market", "password": "Password@123" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = test::TestRequest::post()
        .uri("/signin")
        .set_json(serde_json::json!({ "username": "bob_market", "password": "Password@123" }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(resp).await;
    let bob_token = body["token"].as_str().unwrap().to_string();

    // 4. Alice deposits 10 SOL
    let req = test::TestRequest::post()
        .uri("/deposit/sol")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .set_json(serde_json::json!({ "qty": 10 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. Bob onramps 1000 USD
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({ "qty": 1000 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 6. Alice posts 2 limit asks: 5 SOL @ $100, 5 SOL @ $110
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .set_json(serde_json::json!({ "asset": "sol", "side": "ask", "price": 100, "qty": 5 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .set_json(serde_json::json!({ "asset": "sol", "side": "ask", "price": 110, "qty": 5 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 7. Bob places a Market Buy for 7 SOL (sweeps 5 @ 100, 2 @ 110 = $720 total)
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "bid",
            "order_type": "market",
            "qty": 7
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let order_resp: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(order_resp["status"], "filled");
    assert!(order_resp["orderId"].is_null(), "Market order never rests on the book");

    // 8. Verify Bob's balances: 7 SOL available, $280 USD available, 0 locked
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["stockBalance"]["sol"]["available"], 7);
    assert_eq!(bal["stockBalance"]["sol"]["locked"], 0);
    assert_eq!(bal["usdBalance"]["available"], 280);
    assert_eq!(bal["usdBalance"]["locked"], 0);

    // 9. Verify Alice's balances: 3 SOL locked (in remaining ask), $720 USD available
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {alice_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["stockBalance"]["sol"]["available"], 0);
    assert_eq!(bal["stockBalance"]["sol"]["locked"], 3);
    assert_eq!(bal["usdBalance"]["available"], 720);

    // 10. Verify Orderbook: remaining ask is 3 SOL @ 110, bids are empty
    let req = test::TestRequest::get().uri("/orderbook/sol").to_request();
    let resp = test::call_service(&app, req).await;
    let ob: serde_json::Value = test::read_body_json(resp).await;
    let asks = ob["orderbook"]["asks"].as_array().unwrap();
    assert_eq!(asks.len(), 1);
    assert_eq!(asks[0]["price"], 110);
    assert_eq!(asks[0]["qty"], 3);
    let bids = ob["orderbook"]["bids"].as_array().unwrap();
    assert_eq!(bids.len(), 0);

    // 11. Bob places Market Sell for 5 SOL when there are NO bids in the book
    // It should fill 0, refund all 5 SOL back to available, and return "unfilled"
    let req = test::TestRequest::post()
        .uri("/order")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .set_json(serde_json::json!({
            "asset": "sol",
            "side": "ask",
            "order_type": "market",
            "qty": 5
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let order_resp: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(order_resp["status"], "unfilled");

    // Verify Bob's SOL was fully refunded: still 7 SOL available, 0 locked
    let req = test::TestRequest::get()
        .uri("/balance")
        .insert_header(("Authorization", format!("Bearer {bob_token}")))
        .to_request();
    let resp = test::call_service(&app, req).await;
    let bal: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(bal["stockBalance"]["sol"]["available"], 7);
    assert_eq!(bal["stockBalance"]["sol"]["locked"], 0);

    // Allow cold-path worker to drain executions before finishing test
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    engine_handle.abort();
}



