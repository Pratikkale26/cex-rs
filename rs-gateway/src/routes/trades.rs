use actix_web::{web, HttpResponse, Responder};
use crate::auth::AuthUser;
use crate::dto::TradeRecord;
use crate::state::AppState;

/// GET /trades/my
/// Returns the authenticated user's executed trade history.
pub async fn get_my_trades(
    state: web::Data<AppState>,
    user: AuthUser,
) -> impl Responder {
    let user_id = user.0 as i64;

    let rows = sqlx::query_as::<_, TradeRecord>(
        r#"
        SELECT id, maker_order_id, taker_order_id, buyer_id, seller_id,
               market, price, qty, quote_amount, created_at
        FROM trades
        WHERE buyer_id = $1 OR seller_id = $1
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(trades) => HttpResponse::Ok().json(serde_json::json!({ "trades": trades })),
        Err(e) => {
            eprintln!("Failed to fetch user trades: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({ "error": "Database error" }))
        }
    }
}

/// GET /trades/{asset}
/// Returns recent public trades for a given asset/market.
pub async fn get_market_trades(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> impl Responder {
    let asset = path.into_inner();
    let market = format!("{}_USD", asset.to_uppercase());

    let rows = sqlx::query_as::<_, TradeRecord>(
        r#"
        SELECT id, maker_order_id, taker_order_id, buyer_id, seller_id,
               market, price, qty, quote_amount, created_at
        FROM trades
        WHERE market = $1
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(&market)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(trades) => HttpResponse::Ok().json(serde_json::json!({ "trades": trades })),
        Err(e) => {
            eprintln!("Failed to fetch market trades: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({ "error": "Database error" }))
        }
    }
}
