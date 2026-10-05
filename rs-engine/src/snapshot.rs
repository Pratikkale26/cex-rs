//! Engine state snapshotting for disaster recovery and fast-forward replay.
//!
//! Stores serialized in-memory state (balances and orderbook) along with the
//! last processed Redis Stream ID in the `engine_snapshots` PostgreSQL table.

use crate::state::EngineState;

/// Persist an in-memory snapshot of EngineState to PostgreSQL.
pub async fn save_snapshot(
    pool: &sqlx::PgPool,
    last_stream_id: &str,
    state: &EngineState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json_data = serde_json::to_value(state)?;

    sqlx::query(
        "INSERT INTO engine_snapshots (last_stream_id, state_data, created_at)
         VALUES ($1, $2, NOW())"
    )
    .bind(last_stream_id)
    .bind(json_data)
    .execute(pool)
    .await?;

    Ok(())
}

/// Load the most recent snapshot from PostgreSQL if one exists.
pub async fn load_latest_snapshot(
    pool: &sqlx::PgPool,
) -> Result<Option<(String, EngineState)>, Box<dyn std::error::Error + Send + Sync>> {
    let row: Option<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT last_stream_id, state_data
         FROM engine_snapshots
         ORDER BY id DESC
         LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;

    match row {
        Some((stream_id, val)) => {
            let state: EngineState = serde_json::from_value(val)?;
            Ok(Some((stream_id, state)))
        }
        None => Ok(None),
    }
}

/// Delete all snapshots (called on system reset).
pub async fn truncate_snapshots(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("TRUNCATE TABLE engine_snapshots RESTART IDENTITY CASCADE")
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::Side;

    #[test]
    fn test_engine_state_serialization_roundtrip() {
        let mut state = EngineState::new();
        state.usd_mut(1).available = 1000;
        state.usd_mut(1).locked = 200;
        state.sol_mut(2).available = 5;

        // Place a resting order
        state.sol_orderbook.add_order(1, Side::Bid, 100, 2).unwrap();

        // Serialize to JSON value
        let val = serde_json::to_value(&state).expect("serialize");

        // Deserialize back
        let restored: EngineState = serde_json::from_value(val).expect("deserialize");

        assert_eq!(restored.usd_balance[&1].available, 1000);
        assert_eq!(restored.usd_balance[&1].locked, 200);
        assert_eq!(restored.stock_balance[&2]["sol"].available, 5);
        assert_eq!(restored.sol_orderbook.best_bid(), Some(100));
        assert_eq!(restored.sol_orderbook.order_count(), 1);
    }
}

