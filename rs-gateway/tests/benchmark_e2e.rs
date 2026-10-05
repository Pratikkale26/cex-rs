//! End-to-end performance and latency benchmark across the entire stack:
//! HTTP Client -> Actix-web Gateway -> Redis Streams WAL -> Matching Engine -> Response Queue -> Client.

use actix_web::{http::StatusCode, test, web, App};
use rs_gateway::state::AppState;
use rs_gateway::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

async fn setup_bench_env() -> Option<(
    web::Data<AppState>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    sqlx::PgPool,
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
        .max_connections(10)
        .connect(&database_url)
        .await
        .ok()?;

    let _ = sqlx::migrate!("./migrations").run(&db_pool).await;

    let _listener_handle = rs_gateway::redis::start_response_listener(listener, Arc::clone(&pending), reply_queue);
    let worker_handle = rs_gateway::ledger::start_execution_worker(client.clone(), db_pool.clone());
    let app_state = web::Data::new(AppState::new(db_pool.clone(), publisher, pending, queue_id));

    // Spawn Matching Engine
    #[allow(deprecated)]
    let engine_listener = client.get_async_connection().await.ok()?;
    let engine_publisher = client.get_multiplexed_async_connection().await.ok()?;
    let engine_state = Arc::new(Mutex::new(rs_engine::EngineState::new()));
    let db_clone = db_pool.clone();
    let engine_handle = tokio::spawn(async move {
        rs_engine::run_engine_with_db(engine_state, engine_listener, engine_publisher, Some(db_clone), "0-0".to_string()).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Some((app_state, engine_handle, worker_handle, db_pool))
}

#[actix_web::test]
#[ignore] // Run explicitly with `cargo test --test benchmark_e2e -- --ignored --nocapture`
async fn benchmark_e2e_full_stack_throughput() {
    let (app_state, engine_handle, worker_handle, _db_pool) = match setup_bench_env().await {
        Some(res) => res,
        None => {
            eprintln!("Skipping E2E benchmark: Redis / PostgreSQL not accessible");
            return;
        }
    };

    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .configure(routes::configure),
    )
    .await;

    // 1. Reset
    let req = test::TestRequest::post().uri("/reset").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Signup & Signin
    let signup_body = serde_json::json!({
        "username": "bench_trader",
        "password": "Password123!"
    });
    let req = test::TestRequest::post().uri("/signup").set_json(&signup_body).to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let login_body = serde_json::json!({
        "username": "bench_trader",
        "password": "Password123!"
    });
    let req = test::TestRequest::post().uri("/signin").set_json(&login_body).to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let login_resp: serde_json::Value = test::read_body_json(resp).await;
    let token = login_resp["token"].as_str().unwrap().to_string();

    // 3. Fund account with ample USD
    let req = test::TestRequest::post()
        .uri("/onramp")
        .insert_header(("Authorization", format!("Bearer {token}")))
        .set_json(serde_json::json!({ "qty": 10_000_000 }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    println!("\n============================================================");
    println!("  Starting E2E Full-Stack Benchmark (Gateway + Redis + Engine)");
    println!("============================================================");

    const WARMUP_COUNT: usize = 100;
    const BENCH_COUNT: usize = 1_000;

    // Warmup
    for i in 0..WARMUP_COUNT {
        let req = test::TestRequest::post()
            .uri("/order")
            .insert_header(("Authorization", format!("Bearer {token}")))
            .set_json(serde_json::json!({
                "asset": "sol",
                "side": "bid",
                "price": 100 + (i % 20),
                "qty": 1
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Benchmark Run
    let mut latencies_us: Vec<u128> = Vec::with_capacity(BENCH_COUNT);
    let start_total = Instant::now();

    for i in 0..BENCH_COUNT {
        let t0 = Instant::now();
        let req = test::TestRequest::post()
            .uri("/order")
            .insert_header(("Authorization", format!("Bearer {token}")))
            .set_json(serde_json::json!({
                "asset": "sol",
                "side": "bid",
                "price": 200 + (i % 50),
                "qty": 1
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        latencies_us.push(t0.elapsed().as_micros());
    }

    let elapsed_total = start_total.elapsed();
    latencies_us.sort_unstable();

    let count = latencies_us.len();
    let p50 = latencies_us[count * 50 / 100];
    let p90 = latencies_us[count * 90 / 100];
    let p95 = latencies_us[count * 95 / 100];
    let p99 = latencies_us[count * 99 / 100];
    let max = latencies_us[count - 1];
    let min = latencies_us[0];
    let avg = latencies_us.iter().sum::<u128>() / count as u128;
    let rps = (count as f64 / elapsed_total.as_secs_f64()) as u64;

    println!("\n📊 E2E Full-Stack Benchmark Results (1,000 HTTP Orders):");
    println!("  Total Elapsed Time:   {:.3}s", elapsed_total.as_secs_f64());
    println!("  End-to-End Throughput:{:>8} reqs / sec", rps);
    println!("  Round-Trip Latency Distribution:");
    println!("    Min:                {:>6} µs", min);
    println!("    P50 (Median):       {:>6} µs ({:.2} ms)", p50, p50 as f64 / 1000.0);
    println!("    Average:            {:>6} µs ({:.2} ms)", avg, avg as f64 / 1000.0);
    println!("    P90:                {:>6} µs ({:.2} ms)", p90, p90 as f64 / 1000.0);
    println!("    P95:                {:>6} µs ({:.2} ms)", p95, p95 as f64 / 1000.0);
    println!("    P99:                {:>6} µs ({:.2} ms)", p99, p99 as f64 / 1000.0);
    println!("    Max:                {:>6} µs ({:.2} ms)", max, max as f64 / 1000.0);
    println!("============================================================\n");

    engine_handle.abort();
    worker_handle.abort();
}
