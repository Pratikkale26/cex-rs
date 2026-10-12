mod client;

use client::{ExchangeClient, OrderRequest};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Debug)]
struct MarketConfig {
    asset: &'static str,
    market_name: &'static str,
    base_mid: u64,
    step: u64,
    levels: usize,
    maker_qty_min: u64,
    maker_qty_max: u64,
    taker_qty: u64,
}

const MARKETS: [MarketConfig; 3] = [
    MarketConfig {
        asset: "btc",
        market_name: "BTC_USD",
        base_mid: 50000,
        step: 50,
        levels: 6,
        maker_qty_min: 1,
        maker_qty_max: 3,
        taker_qty: 1,
    },
    MarketConfig {
        asset: "eth",
        market_name: "ETH_USD",
        base_mid: 3500,
        step: 5,
        levels: 6,
        maker_qty_min: 4,
        maker_qty_max: 10,
        taker_qty: 2,
    },
    MarketConfig {
        asset: "sol",
        market_name: "SOL_USD",
        base_mid: 150,
        step: 1,
        levels: 6,
        maker_qty_min: 20,
        maker_qty_max: 60,
        taker_qty: 5,
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_url =
        std::env::var("GATEWAY_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());
    println!("==================================================");
    println!(" Starting rs-bot (Liquidity Provider & Market Maker)");
    println!(" Gateway Target: {}", gateway_url);
    println!("==================================================");

    let client = Arc::new(ExchangeClient::new(gateway_url));

    // 1. Wait for rs-gateway to come online
    println!("[rs-bot] Waiting for rs-gateway to initialize...");
    if let Err(e) = client.wait_for_gateway(60).await {
        eprintln!("[rs-bot] Gateway unavailable: {e}. Exiting bot.");
        return Ok(());
    }
    println!("[rs-bot] Gateway connected successfully.");

    // 2. Setup Market Maker (Maker) Account
    let maker_user = "market_maker_alpha";
    let maker_pass = "Password123!";
    let _ = client.signup(maker_user, maker_pass).await;
    let maker_token = client
        .signin(maker_user, maker_pass)
        .await
        .expect("Failed to sign in market maker account");
    println!("[rs-bot] Maker account authenticated: {}", maker_user);

    // Fund Maker with institutional balance
    let _ = client.onramp(&maker_token, 50_000_000).await;
    let _ = client.deposit(&maker_token, "btc", 500).await;
    let _ = client.deposit(&maker_token, "eth", 5_000).await;
    let _ = client.deposit(&maker_token, "sol", 50_000).await;
    println!("[rs-bot] Maker account funded with $50M USD, 500 BTC, 5K ETH, 50K SOL.");

    // 3. Setup Noise Trader (Taker) Account
    let taker_user = "retail_noise_trader";
    let taker_pass = "Password123!";
    let _ = client.signup(taker_user, taker_pass).await;
    let taker_token = client
        .signin(taker_user, taker_pass)
        .await
        .expect("Failed to sign in taker account");
    println!("[rs-bot] Taker account authenticated: {}", taker_user);

    // Fund Taker
    let _ = client.onramp(&taker_token, 10_000_000).await;
    let _ = client.deposit(&taker_token, "btc", 100).await;
    let _ = client.deposit(&taker_token, "eth", 1_000).await;
    let _ = client.deposit(&taker_token, "sol", 10_000).await;
    println!("[rs-bot] Taker account funded with $10M USD, 100 BTC, 1K ETH, 10K SOL.");

    // 4. Genesis Liquidity Seeding (Pre-seeding 10-level Bids & Asks)
    println!("[rs-bot] Pre-seeding Genesis Liquidity across all markets...");
    let mut current_mids: Vec<u64> = MARKETS.iter().map(|m| m.base_mid).collect();

    for (idx, cfg) in MARKETS.iter().enumerate() {
        seed_market_depth(&client, &maker_token, cfg, current_mids[idx]).await;
    }
    println!("[rs-bot] Genesis Liquidity active! Orderbook ladders populated.");

    // 5. Continuous Loop: Round-Robin Noise Trading + Orderbook Replenishment
    let mut loop_count = 0_usize;
    loop {
        sleep(Duration::from_millis(1200)).await;
        let idx = loop_count % MARKETS.len();
        let cfg = &MARKETS[idx];
        loop_count += 1;

        // A. Noise Trader Execution across the spread
        execute_noise_trade(&client, &taker_token, cfg).await;

        // B. Replenish depleted orderbook depth & organic drift
        {
            let mut rng = rand::thread_rng();
            let roll: u32 = rng.gen_range(0..100);
            if roll < 20 {
                current_mids[idx] = current_mids[idx].saturating_add(cfg.step);
            } else if roll < 40 {
                current_mids[idx] = current_mids[idx].saturating_sub(cfg.step);
            }

            replenish_market_depth(&client, &maker_token, cfg, current_mids[idx]).await;
        }
    }
}

/// Seed an initial 6-level bid & ask ladder for a market
async fn seed_market_depth(client: &ExchangeClient, token: &str, cfg: &MarketConfig, mid: u64) {
    let mut rng = rand::thread_rng();

    // Place Bids (below mid)
    for i in 1..=cfg.levels {
        let price = mid.saturating_sub((i as u64) * cfg.step);
        if price == 0 {
            continue;
        }
        let qty = rng.gen_range(cfg.maker_qty_min..=cfg.maker_qty_max);
        let _ = client
            .place_order(
                token,
                &OrderRequest {
                    asset: cfg.asset.to_string(),
                    side: "bid".to_string(),
                    order_type: "limit".to_string(),
                    price,
                    qty,
                    time_in_force: "GTC".to_string(),
                },
            )
            .await;
    }

    // Place Asks (above mid)
    for i in 1..=cfg.levels {
        let price = mid + ((i as u64) * cfg.step);
        let qty = rng.gen_range(cfg.maker_qty_min..=cfg.maker_qty_max);
        let _ = client
            .place_order(
                token,
                &OrderRequest {
                    asset: cfg.asset.to_string(),
                    side: "ask".to_string(),
                    order_type: "limit".to_string(),
                    price,
                    qty,
                    time_in_force: "GTC".to_string(),
                },
            )
            .await;
    }

    println!(
        "[rs-bot] Pre-seeded {} L2 ladder: {} bids, {} asks around mid ${}",
        cfg.market_name, cfg.levels, cfg.levels, mid
    );
}

/// Inspect orderbook and replenish missing depth levels
async fn replenish_market_depth(
    client: &ExchangeClient,
    token: &str,
    cfg: &MarketConfig,
    mid: u64,
) {
    let ob = match client.get_orderbook(cfg.asset).await {
        Ok(b) => b,
        Err(_) => return,
    };

    let mut rng = rand::thread_rng();

    // If bids depth is low (< 3 levels), replenish
    if ob.bids.len() < 4 {
        for i in 1..=cfg.levels {
            let price = mid.saturating_sub((i as u64) * cfg.step);
            if price == 0 {
                continue;
            }
            let already_exists = ob.bids.iter().any(|b| b.price == price);
            if !already_exists {
                let qty = rng.gen_range(cfg.maker_qty_min..=cfg.maker_qty_max);
                let _ = client
                    .place_order(
                        token,
                        &OrderRequest {
                            asset: cfg.asset.to_string(),
                            side: "bid".to_string(),
                            order_type: "limit".to_string(),
                            price,
                            qty,
                            time_in_force: "GTC".to_string(),
                        },
                    )
                    .await;
            }
        }
    }

    // If asks depth is low (< 3 levels), replenish
    if ob.asks.len() < 4 {
        for i in 1..=cfg.levels {
            let price = mid + ((i as u64) * cfg.step);
            let already_exists = ob.asks.iter().any(|a| a.price == price);
            if !already_exists {
                let qty = rng.gen_range(cfg.maker_qty_min..=cfg.maker_qty_max);
                let _ = client
                    .place_order(
                        token,
                        &OrderRequest {
                            asset: cfg.asset.to_string(),
                            side: "ask".to_string(),
                            order_type: "limit".to_string(),
                            price,
                            qty,
                            time_in_force: "GTC".to_string(),
                        },
                    )
                    .await;
            }
        }
    }
}

/// Execute a noise trade by crossing the top of the book
async fn execute_noise_trade(client: &ExchangeClient, taker_token: &str, cfg: &MarketConfig) {
    let mut rng = rand::thread_rng();

    let ob = match client.get_orderbook(cfg.asset).await {
        Ok(b) => b,
        Err(_) => return,
    };

    let is_buy: bool = rng.gen_bool(0.5);

    if is_buy {
        // Buy against the best ask
        if let Some(best_ask) = ob.asks.first() {
            let qty = cfg.taker_qty.min(best_ask.qty.max(1));
            let res = client
                .place_order(
                    taker_token,
                    &OrderRequest {
                        asset: cfg.asset.to_string(),
                        side: "bid".to_string(),
                        order_type: "limit".to_string(),
                        price: best_ask.price,
                        qty,
                        time_in_force: "IOC".to_string(),
                    },
                )
                .await;

            if let Ok(Some(oid)) = res {
                println!(
                    "[rs-bot] Taker executed BUY on {}: {} @ ${} (Order #{})",
                    cfg.market_name, qty, best_ask.price, oid
                );
            }
        }
    } else {
        // Sell against the best bid
        if let Some(best_bid) = ob.bids.first() {
            let qty = cfg.taker_qty.min(best_bid.qty.max(1));
            let res = client
                .place_order(
                    taker_token,
                    &OrderRequest {
                        asset: cfg.asset.to_string(),
                        side: "ask".to_string(),
                        order_type: "limit".to_string(),
                        price: best_bid.price,
                        qty,
                        time_in_force: "IOC".to_string(),
                    },
                )
                .await;

            if let Ok(Some(oid)) = res {
                println!(
                    "[rs-bot] Taker executed SELL on {}: {} @ ${} (Order #{})",
                    cfg.market_name, qty, best_bid.price, oid
                );
            }
        }
    }
}
