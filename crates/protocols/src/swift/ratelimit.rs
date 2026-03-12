// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Rate Limiting Support for Swift API
//!
//! This module implements rate limiting to prevent abuse and ensure fair resource
//! allocation across tenants. Rate limits can be applied per-account, per-container,
//! or per-IP address.
//!
//! # Configuration
//!
//! Rate limits are configured via container metadata:
//!
//! ```bash
//! # Set account-level rate limit: 1000 requests per minute
//! swift post -m "X-Account-Meta-Rate-Limit:1000/60"
//!
//! # Set container-level rate limit: 100 requests per minute
//! swift post container -m "X-Container-Meta-Rate-Limit:100/60"
//! ```
//!
//! # Response Headers
//!
//! Rate limit information is included in all responses:
//!
//! ```http
//! HTTP/1.1 200 OK
//! X-RateLimit-Limit: 1000
//! X-RateLimit-Remaining: 950
//! X-RateLimit-Reset: 1740003600
//! ```
//!
//! When rate limit is exceeded:
//!
//! ```http
//! HTTP/1.1 429 Too Many Requests
//! X-RateLimit-Limit: 1000
//! X-RateLimit-Remaining: 0
//! X-RateLimit-Reset: 1740003600
//! Retry-After: 30
//! ```
//!
//! # Algorithm
//!
//! Uses token bucket algorithm with per-second refill rate:
//! - Each request consumes 1 token
//! - Tokens refill at configured rate
//! - Burst capacity allows temporary spikes

use super::{SwiftError, SwiftResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

/// Rate limit configuration
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimit {
    /// Maximum requests allowed in time window
    pub limit: u32,

    /// Time window in seconds
    pub window_seconds: u32,
}

impl RateLimit {
    /// Parse rate limit from metadata value
    ///
    /// Format: "limit/window_seconds" (e.g., "1000/60" = 1000 requests per 60 seconds)
    pub fn parse(value: &str) -> SwiftResult<Self> {
        let parts: Vec<&str> = value.split('/').collect();
        if parts.len() != 2 {
            return Err(SwiftError::BadRequest(format!(
                "Invalid rate limit format: {}. Expected format: limit/window_seconds",
                value
            )));
        }

        let limit = parts[0]
            .parse::<u32>()
            .map_err(|_| SwiftError::BadRequest(format!("Invalid rate limit value: {}", parts[0])))?;

        let window_seconds = parts[1]
            .parse::<u32>()
            .map_err(|_| SwiftError::BadRequest(format!("Invalid window value: {}", parts[1])))?;

        if window_seconds == 0 {
            return Err(SwiftError::BadRequest("Rate limit window cannot be zero".to_string()));
        }

        Ok(RateLimit { limit, window_seconds })
    }

    /// Calculate refill rate (tokens per second)
    pub fn refill_rate(&self) -> f64 {
        self.limit as f64 / self.window_seconds as f64
    }
}

/// Token bucket for rate limiting
#[derive(Debug, Clone)]
struct TokenBucket {
    /// Maximum tokens (burst capacity)
    capacity: u32,

    /// Current available tokens
    tokens: f64,

    /// Refill rate (tokens per second)
    refill_rate: f64,

    /// Last refill timestamp (Unix seconds)
    last_refill: u64,
}

impl TokenBucket {
    fn new(rate_limit: &RateLimit) -> Self {
        let capacity = rate_limit.limit;
        let refill_rate = rate_limit.refill_rate();

        TokenBucket {
            capacity,
            tokens: capacity as f64, // Start full
            refill_rate,
            last_refill: current_timestamp(),
        }
    }

    /// Try to consume a token
    ///
    /// Returns Ok(remaining_tokens) if successful, Err(retry_after_seconds) if rate limited
    fn try_consume(&mut self) -> Result<u32, u64> {
        // Refill tokens based on time elapsed
        let now = current_timestamp();
        let elapsed = now.saturating_sub(self.last_refill);

        if elapsed > 0 {
            let refill_amount = self.refill_rate * elapsed as f64;
            self.tokens = (self.tokens + refill_amount).min(self.capacity as f64);
            self.last_refill = now;
        }

        // Try to consume 1 token
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(self.tokens.floor() as u32)
        } else {
            // Calculate retry-after: time until 1 token is available
            let tokens_needed = 1.0 - self.tokens;
            let retry_after = (tokens_needed / self.refill_rate).ceil() as u64;
            Err(retry_after)
        }
    }

    /// Get current token count
    fn remaining(&mut self) -> u32 {
        // Refill tokens based on time elapsed
        let now = current_timestamp();
        let elapsed = now.saturating_sub(self.last_refill);

        if elapsed > 0 {
            let refill_amount = self.refill_rate * elapsed as f64;
            self.tokens = (self.tokens + refill_amount).min(self.capacity as f64);
            self.last_refill = now;
        }

        self.tokens.floor() as u32
    }

    /// Get reset timestamp (when bucket will be full)
    fn reset_timestamp(&self, now: u64) -> u64 {
        if self.tokens >= self.capacity as f64 {
            now
        } else {
            let tokens_to_refill = self.capacity as f64 - self.tokens;
            let seconds_to_full = (tokens_to_refill / self.refill_rate).ceil() as u64;
            now + seconds_to_full
        }
    }
}

/// Global rate limiter state (in-memory)
///
/// In production, this should be backed by Redis or similar distributed store
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
}

impl RateLimiter {
    /// Create new rate limiter
    pub fn new() -> Self {
        RateLimiter {
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check and consume rate limit quota
    ///
    /// Returns (remaining, reset_timestamp) if successful,
    /// or SwiftError::TooManyRequests if rate limited
    pub fn check_rate_limit(&self, key: &str, rate_limit: &RateLimit) -> SwiftResult<(u32, u64)> {
        let mut buckets = self.buckets.lock().unwrap();

        // Get or create bucket for this key
        let bucket = buckets.entry(key.to_string()).or_insert_with(|| TokenBucket::new(rate_limit));

        let now = current_timestamp();
        let reset = bucket.reset_timestamp(now);

        match bucket.try_consume() {
            Ok(remaining) => {
                debug!("Rate limit OK for {}: {} remaining", key, remaining);
                Ok((remaining, reset))
            }
            Err(retry_after) => {
                debug!("Rate limit exceeded for {}: retry after {} seconds", key, retry_after);
                Err(SwiftError::TooManyRequests {
                    retry_after,
                    limit: rate_limit.limit,
                    reset,
                })
            }
        }
    }

    /// Get current rate limit status without consuming quota
    pub fn get_status(&self, key: &str, rate_limit: &RateLimit) -> (u32, u64) {
        let mut buckets = self.buckets.lock().unwrap();

        let bucket = buckets.entry(key.to_string()).or_insert_with(|| TokenBucket::new(rate_limit));

        let now = current_timestamp();
        let remaining = bucket.remaining();
        let reset = bucket.reset_timestamp(now);

        (remaining, reset)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Extract rate limit from account or container metadata
pub fn extract_rate_limit(metadata: &HashMap<String, String>) -> Option<RateLimit> {
    // Check for rate limit in metadata
    if let Some(rate_limit_str) = metadata
        .get("x-account-meta-rate-limit")
        .or_else(|| metadata.get("x-container-meta-rate-limit"))
    {
        RateLimit::parse(rate_limit_str).ok()
    } else {
        None
    }
}

/// Build rate limit key for tracking
pub fn build_rate_limit_key(account: &str, container: Option<&str>) -> String {
    if let Some(cont) = container {
        format!("account:{}:container:{}", account, cont)
    } else {
        format!("account:{}", account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rate_limit_valid() {
        let rate_limit = RateLimit::parse("1000/60").unwrap();
        assert_eq!(rate_limit.limit, 1000);
        assert_eq!(rate_limit.window_seconds, 60);
    }

    #[test]
    fn test_parse_rate_limit_invalid_format() {
        let result = RateLimit::parse("1000");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rate_limit_invalid_limit() {
        let result = RateLimit::parse("not_a_number/60");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rate_limit_invalid_window() {
        let result = RateLimit::parse("1000/not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rate_limit_zero_window() {
        let result = RateLimit::parse("1000/0");
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limit_refill_rate() {
        let rate_limit = RateLimit {
            limit: 1000,
            window_seconds: 60,
        };
        assert!((rate_limit.refill_rate() - 16.666666).abs() < 0.001);
    }

    #[test]
    fn test_token_bucket_consume() {
        let rate_limit = RateLimit {
            limit: 10,
            window_seconds: 60,
        };
        let mut bucket = TokenBucket::new(&rate_limit);

        // Should be able to consume up to limit
        for i in 0..10 {
            let result = bucket.try_consume();
            assert!(result.is_ok(), "Token {} should succeed", i);
        }

        // 11th request should fail
        let result = bucket.try_consume();
        assert!(result.is_err());
    }

    #[test]
    fn test_token_bucket_remaining() {
        let rate_limit = RateLimit {
            limit: 100,
            window_seconds: 60,
        };
        let mut bucket = TokenBucket::new(&rate_limit);

        // Initial: 100 tokens
        assert_eq!(bucket.remaining(), 100);

        // Consume 10
        for _ in 0..10 {
            bucket.try_consume().unwrap();
        }

        assert_eq!(bucket.remaining(), 90);
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new();
        let rate_limit = RateLimit {
            limit: 5,
            window_seconds: 60,
        };

        // Should allow 5 requests
        for _ in 0..5 {
            let result = limiter.check_rate_limit("test_key", &rate_limit);
            assert!(result.is_ok());
        }

        // 6th request should fail
        let result = limiter.check_rate_limit("test_key", &rate_limit);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_rate_limit_account() {
        let mut metadata = HashMap::new();
        metadata.insert("x-account-meta-rate-limit".to_string(), "1000/60".to_string());

        let rate_limit = extract_rate_limit(&metadata);
        assert!(rate_limit.is_some());

        let rate_limit = rate_limit.unwrap();
        assert_eq!(rate_limit.limit, 1000);
        assert_eq!(rate_limit.window_seconds, 60);
    }

    #[test]
    fn test_extract_rate_limit_container() {
        let mut metadata = HashMap::new();
        metadata.insert("x-container-meta-rate-limit".to_string(), "100/60".to_string());

        let rate_limit = extract_rate_limit(&metadata);
        assert!(rate_limit.is_some());

        let rate_limit = rate_limit.unwrap();
        assert_eq!(rate_limit.limit, 100);
        assert_eq!(rate_limit.window_seconds, 60);
    }

    #[test]
    fn test_extract_rate_limit_none() {
        let metadata = HashMap::new();
        let rate_limit = extract_rate_limit(&metadata);
        assert!(rate_limit.is_none());
    }

    #[test]
    fn test_build_rate_limit_key_account() {
        let key = build_rate_limit_key("AUTH_test", None);
        assert_eq!(key, "account:AUTH_test");
    }

    #[test]
    fn test_build_rate_limit_key_container() {
        let key = build_rate_limit_key("AUTH_test", Some("my-container"));
        assert_eq!(key, "account:AUTH_test:container:my-container");
    }
}
