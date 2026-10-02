//! JWT Bearer authentication extractor for actix-web.
//!
//! Usage: add `user: AuthUser` as a handler parameter.
//! The extractor will parse the `Authorization: Bearer <token>` header,
//! validate the JWT, and extract the user id.

use actix_web::{FromRequest, HttpRequest, HttpResponse, ResponseError, dev::Payload, error::Error};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::{Ready, ready};

pub const JWT_SECRET: &[u8] = b"secret";

/// JWT payload claims.
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: u64,
    pub exp: usize,
}

/// The authenticated user's id, extracted from the JWT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthUser(pub u64);

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct AuthError;

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid or missing token")
    }
}

impl ResponseError for AuthError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Unauthorized().json(serde_json::json!({
            "message": "Invalid or missing token"
        }))
    }
}

// ── FromRequest impl ─────────────────────────────────────────────────────────

impl FromRequest for AuthUser {
    type Error  = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let token = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_start_matches("Bearer ").trim().to_string());

        let token = match token {
            Some(t) if !t.is_empty() => t,
            _                        => return ready(Err(AuthError.into())),
        };

        let decoded = decode::<Claims>(
            &token,
            &DecodingKey::from_secret(JWT_SECRET),
            &Validation::default(),
        );

        match decoded {
            Ok(data) => ready(Ok(AuthUser(data.claims.sub))),
            Err(_)   => ready(Err(AuthError.into())),
        }
    }
}
