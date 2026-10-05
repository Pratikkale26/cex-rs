//! Password hashing and verification using Argon2id.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use tokio::task;

/// Hash a plaintext password using Argon2id with a cryptographically secure random salt.
pub async fn hash_password(password: &str) -> Result<String, String> {
    let password = password.to_string();
    task::spawn_blocking(move || {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("Task execution error: {e}"))?
}

/// Verify a plaintext password against an Argon2id password hash.
pub async fn verify_password(password: &str, stored_hash: &str) -> bool {
    let password = password.to_string();
    let stored_hash = stored_hash.to_string();
    task::spawn_blocking(move || {
        if let Ok(parsed_hash) = PasswordHash::new(&stored_hash) {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed_hash)
                .is_ok()
        } else {
            false
        }
    })
    .await
    .unwrap_or(false)
}

// Aliases for convenience
pub use hash_password as hash;
pub use verify_password as verify;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hash_and_verify_success() {
        let password = "super_secret_password";
        let hashed = hash_password(password).await.expect("Hashing should succeed");

        assert!(hashed.starts_with("$argon2"));
        assert!(verify_password(password, &hashed).await);
    }

    #[tokio::test]
    async fn test_verify_wrong_password() {
        let password = "correct_password";
        let hashed = hash_password(password).await.expect("Hashing should succeed");

        assert!(!verify_password("wrong_password", &hashed).await);
    }

    #[tokio::test]
    async fn test_verify_invalid_hash() {
        assert!(!verify_password("password", "invalid_argon_hash").await);
    }
}
