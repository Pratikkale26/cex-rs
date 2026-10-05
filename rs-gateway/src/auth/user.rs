use sqlx::FromRow;

/// A user record persisted in PostgreSQL.
#[derive(Debug, Clone, FromRow)]
pub struct User {
    pub id:            i64,
    pub username:      String,
    pub password_hash: String,
    pub created_at:    chrono::DateTime<chrono::Utc>,
}
