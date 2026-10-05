use actix_web::{web, HttpResponse, Responder};
use crate::auth::AuthUser;
use crate::dto::LedgerRecord;
use crate::state::AppState;

/// GET /ledger
/// Returns the authenticated user's financial ledger statement entries.
pub async fn get_ledger(
    state: web::Data<AppState>,
    user: AuthUser,
) -> impl Responder {
    let user_id = user.0 as i64;

    let rows = sqlx::query_as::<_, LedgerRecord>(
        r#"
        SELECT id, user_id, currency, amount, balance_type, operation_type, reference_id, created_at
        FROM ledger_entries
        WHERE user_id = $1
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(ledger) => HttpResponse::Ok().json(serde_json::json!({ "ledger": ledger })),
        Err(e) => {
            eprintln!("Failed to fetch ledger statement: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({ "error": "Database error" }))
        }
    }
}
