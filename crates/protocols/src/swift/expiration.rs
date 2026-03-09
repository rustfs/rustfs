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

//! Object Expiration Support for Swift API
//!
//! This module implements automatic object expiration, allowing objects to be
//! automatically deleted after a specified time. This is useful for temporary
//! files, cache data, and time-limited content.
//!
//! # Configuration
//!
//! Object expiration is configured via headers during PUT or POST:
//!
//! - `X-Delete-At`: Unix timestamp when object should be deleted
//! - `X-Delete-After`: Seconds from now when object should be deleted
//!
//! # Usage
//!
//! ```bash
//! # Delete object at specific time (Unix timestamp)
//! swift upload container file.txt -H "X-Delete-At: 1740000000"
//!
//! # Delete object 3600 seconds (1 hour) from now
//! swift upload container file.txt -H "X-Delete-After: 3600"
//!
//! # Update expiration on existing object
//! swift post container file.txt -H "X-Delete-At: 1750000000"
//! ```
//!
//! # Expiration Headers
//!
//! When retrieving objects with expiration set:
//! ```http
//! GET /v1/AUTH_account/container/file.txt
//!
//! HTTP/1.1 200 OK
//! X-Delete-At: 1740000000
//! ```
//!
//! # Cleanup
//!
//! Expired objects are automatically deleted by a background worker that
//! periodically scans for objects past their expiration time.

use super::{SwiftError, SwiftResult};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

/// Parse X-Delete-At header value
///
/// Returns Unix timestamp in seconds
pub fn parse_delete_at(value: &str) -> SwiftResult<u64> {
    value
        .parse::<u64>()
        .map_err(|_| SwiftError::BadRequest(format!("Invalid X-Delete-At value: {}", value)))
}

/// Parse X-Delete-After header value and convert to X-Delete-At
///
/// X-Delete-After is seconds from now, converted to absolute Unix timestamp
pub fn parse_delete_after(value: &str) -> SwiftResult<u64> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| SwiftError::BadRequest(format!("Invalid X-Delete-After value: {}", value)))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SwiftError::InternalServerError(format!("Time error: {}", e)))?
        .as_secs();

    Ok(now + seconds)
}

/// Extract expiration timestamp from request headers
///
/// Checks both X-Delete-At and X-Delete-After headers.
/// X-Delete-After takes precedence and is converted to X-Delete-At.
pub fn extract_expiration(headers: &http::HeaderMap) -> SwiftResult<Option<u64>> {
    // Check X-Delete-After first (takes precedence)
    if let Some(delete_after) = headers.get("x-delete-after")
        && let Ok(value_str) = delete_after.to_str()
    {
        let delete_at = parse_delete_after(value_str)?;
        debug!("X-Delete-After: {} seconds -> X-Delete-At: {}", value_str, delete_at);
        return Ok(Some(delete_at));
    }

    // Check X-Delete-At
    if let Some(delete_at) = headers.get("x-delete-at")
        && let Ok(value_str) = delete_at.to_str()
    {
        let timestamp = parse_delete_at(value_str)?;
        debug!("X-Delete-At: {}", timestamp);
        return Ok(Some(timestamp));
    }

    Ok(None)
}

/// Check if object has expired
///
/// Returns true if the object's X-Delete-At timestamp is in the past
pub fn is_expired(delete_at: u64) -> bool {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

    now >= delete_at
}

/// Validate expiration timestamp
///
/// Ensures the timestamp is in the future and not too far in the past
pub fn validate_expiration(delete_at: u64) -> SwiftResult<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SwiftError::InternalServerError(format!("Time error: {}", e)))?
        .as_secs();

    // Allow some clock skew (60 seconds in the past)
    if delete_at < now.saturating_sub(60) {
        return Err(SwiftError::BadRequest(format!(
            "X-Delete-At timestamp is too far in the past: {}",
            delete_at
        )));
    }

    // Warn if expiration is more than 10 years in the future
    let ten_years = 10 * 365 * 24 * 60 * 60;
    if delete_at > now + ten_years {
        debug!("X-Delete-At timestamp is more than 10 years in the future: {}", delete_at);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delete_at_valid() {
        let result = parse_delete_at("1740000000");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1740000000);
    }

    #[test]
    fn test_parse_delete_at_invalid() {
        let result = parse_delete_at("not_a_number");
        assert!(result.is_err());

        let result = parse_delete_at("-1");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_delete_after() {
        let result = parse_delete_after("3600");
        assert!(result.is_ok());

        let delete_at = result.unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        // Should be approximately now + 3600
        assert!(delete_at >= now + 3599 && delete_at <= now + 3601);
    }

    #[test]
    fn test_parse_delete_after_invalid() {
        let result = parse_delete_after("not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_expired_past() {
        let past_timestamp = 1000000000; // Year 2001
        assert!(is_expired(past_timestamp));
    }

    #[test]
    fn test_is_expired_future() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let future_timestamp = now + 3600; // 1 hour from now
        assert!(!is_expired(future_timestamp));
    }

    #[test]
    fn test_is_expired_exact() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        // At exact timestamp, should be expired (>=)
        assert!(is_expired(now));
    }

    #[test]
    fn test_validate_expiration_future() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let future = now + 3600;

        let result = validate_expiration(future);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_expiration_recent_past() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let recent_past = now - 30; // 30 seconds ago (within clock skew)

        let result = validate_expiration(recent_past);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_expiration_far_past() {
        let far_past = 1000000000; // Year 2001

        let result = validate_expiration(far_past);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_expiration_far_future() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let far_future = now + (20 * 365 * 24 * 60 * 60); // 20 years

        // Should still be valid (just logged as warning)
        let result = validate_expiration(far_future);
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_expiration_delete_at() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-delete-at", "1740000000".parse().unwrap());

        let result = extract_expiration(&headers);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(1740000000));
    }

    #[test]
    fn test_extract_expiration_delete_after() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-delete-after", "3600".parse().unwrap());

        let result = extract_expiration(&headers);
        assert!(result.is_ok());

        let delete_at = result.unwrap().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        assert!(delete_at >= now + 3599 && delete_at <= now + 3601);
    }

    #[test]
    fn test_extract_expiration_delete_after_precedence() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-delete-at", "1740000000".parse().unwrap());
        headers.insert("x-delete-after", "3600".parse().unwrap());

        let result = extract_expiration(&headers);
        assert!(result.is_ok());

        let delete_at = result.unwrap().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        // Should use X-Delete-After (precedence), not X-Delete-At
        assert!(delete_at >= now + 3599 && delete_at <= now + 3601);
        assert_ne!(delete_at, 1740000000);
    }

    #[test]
    fn test_extract_expiration_none() {
        let headers = http::HeaderMap::new();

        let result = extract_expiration(&headers);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }
}
