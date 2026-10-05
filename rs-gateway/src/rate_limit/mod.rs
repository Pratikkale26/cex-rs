//! In-memory token bucket rate limiter for anti-spam and brute-force protection.

pub mod middleware;
pub use middleware::*;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct Bucket {
    tokens:      f64,
    last_update: Instant,
}

/// Token bucket configuration and state.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    capacity:    f64,
    refill_rate: f64, // tokens per second
    buckets:     Arc<Mutex<HashMap<String, Bucket>>>,
}

impl RateLimiter {
    /// Create a new RateLimiter.
    /// `capacity`: maximum burst capacity (tokens).
    /// `refill_rate`: number of tokens replenished per second.
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            refill_rate,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if a request from `key` is allowed.
    /// Returns `Ok(())` if allowed, or `Err(retry_after_secs)` if rate limited.
    pub async fn check(&self, key: &str) -> Result<(), u64> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().await;

        let bucket = buckets.entry(key.to_string()).or_insert_with(|| Bucket {
            tokens:      self.capacity,
            last_update: now,
        });

        // Calculate refilled tokens based on elapsed time
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.capacity);
        bucket.last_update = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let missing = 1.0 - bucket.tokens;
            let wait_secs = (missing / self.refill_rate).ceil() as u64;
            Err(wait_secs.max(1))
        }
    }

    /// Periodic cleanup of stale buckets (not active for > 1 hour)
    pub async fn cleanup(&self) {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().await;
        buckets.retain(|_, b| now.duration_since(b.last_update) < Duration::from_secs(3600));
    }
}

/// Helper to extract client IP address string from Actix-web request.
pub fn extract_client_ip(req: &actix_web::dev::ServiceRequest) -> String {
    req.connection_info()
        .realip_remote_addr()
        .and_then(|addr| addr.parse::<IpAddr>().ok())
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_allows_up_to_capacity() {
        let limiter = RateLimiter::new(3.0, 1.0);
        let key = "192.168.1.1";

        assert!(limiter.check(key).await.is_ok());
        assert!(limiter.check(key).await.is_ok());
        assert!(limiter.check(key).await.is_ok());

        // 4th request should exceed capacity
        let res = limiter.check(key).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), 1);
    }

    #[tokio::test]
    async fn test_rate_limiter_refills_over_time() {
        let limiter = RateLimiter::new(1.0, 10.0); // 10 tokens per second
        let key = "10.0.0.1";

        assert!(limiter.check(key).await.is_ok());
        assert!(limiter.check(key).await.is_err());

        // Wait 150ms -> 1.5 tokens refilled
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(limiter.check(key).await.is_ok());
    }

    #[tokio::test]
    async fn test_different_keys_have_separate_buckets() {
        let limiter = RateLimiter::new(1.0, 1.0);

        assert!(limiter.check("user_a").await.is_ok());
        assert!(limiter.check("user_a").await.is_err());

        // user_b should not be affected by user_a
        assert!(limiter.check("user_b").await.is_ok());
    }
}
