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

//! Temporary URL (TempURL) support for Swift API
//!
//! TempURL provides time-limited public access to objects without authentication.
//! Uses HMAC-SHA1 signatures with account-level secret keys.

use super::SwiftError;
use hmac::{Hmac, Mac, KeyInit};
use sha1::Sha1;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

/// TempURL query parameters extracted from request
#[derive(Debug, Clone)]
pub struct TempURLParams {
    /// HMAC-SHA1 signature (hex encoded)
    pub temp_url_sig: String,

    /// Unix timestamp when URL expires
    pub temp_url_expires: u64,

    /// Optional: filename for Content-Disposition header
    pub temp_url_filename: Option<String>,
}

/// Generate TempURL signature using HMAC-SHA1
///
/// Signature format: HMAC-SHA1(key, "{method}\n{expires}\n{path}")
///
/// # Arguments
/// * `method` - HTTP method (GET, PUT, HEAD, etc.)
/// * `expires` - Unix timestamp when URL expires
/// * `path` - Request path (e.g., "/v1/AUTH_test/container/object")
/// * `key` - Account-level secret key
///
/// # Returns
/// Hex-encoded HMAC-SHA1 signature
pub fn generate_tempurl_signature(
    method: &str,
    expires: u64,
    path: &str,
    key: &str,
) -> Result<String, SwiftError> {
    // Format: METHOD\nEXPIRES\nPATH
    let message = format!("{}\n{}\n{}", method.to_uppercase(), expires, path);

    let mut mac = HmacSha1::new_from_slice(key.as_bytes())
        .map_err(|e| SwiftError::InternalServerError(format!("HMAC initialization failed: {}", e)))?;

    mac.update(message.as_bytes());
    let result = mac.finalize();
    let signature = hex::encode(result.into_bytes());

    Ok(signature)
}

/// Validate TempURL request
///
/// Checks:
/// 1. URL has not expired
/// 2. Signature matches expected value
/// 3. HTTP method matches signed method
///
/// # Arguments
/// * `method` - HTTP method from request
/// * `path` - Request path
/// * `params` - Parsed TempURL parameters
/// * `key` - Account-level secret key
///
/// # Returns
/// Ok(()) if valid, Err(SwiftError) if invalid or expired
pub fn validate_tempurl(
    method: &str,
    path: &str,
    params: &TempURLParams,
    key: &str,
) -> Result<(), SwiftError> {
    // 1. Check expiration
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SwiftError::InternalServerError(format!("System time error: {}", e)))?
        .as_secs();

    if now > params.temp_url_expires {
        return Err(SwiftError::Unauthorized(format!(
            "TempURL expired at {}",
            params.temp_url_expires
        )));
    }

    // 2. Validate signature
    let expected_sig = generate_tempurl_signature(method, params.temp_url_expires, path, key)?;

    if params.temp_url_sig != expected_sig {
        return Err(SwiftError::Unauthorized("Invalid TempURL signature".to_string()));
    }

    Ok(())
}

/// Parse TempURL query parameters from request query string
///
/// Expected format: "temp_url_sig=abc123&temp_url_expires=1234567890&temp_url_filename=file.txt"
///
/// # Arguments
/// * `query` - URL query string
///
/// # Returns
/// Some(TempURLParams) if all required params present, None otherwise
pub fn parse_tempurl_params(query: &str) -> Option<TempURLParams> {
    let mut sig = None;
    let mut expires = None;
    let mut filename = None;

    for param in query.split('&') {
        if let Some((key, value)) = param.split_once('=') {
            match key {
                "temp_url_sig" => sig = Some(value.to_string()),
                "temp_url_expires" => expires = value.parse().ok(),
                "temp_url_filename" => filename = Some(value.to_string()),
                _ => {}
            }
        }
    }

    // Both sig and expires are required
    Some(TempURLParams {
        temp_url_sig: sig?,
        temp_url_expires: expires?,
        temp_url_filename: filename,
    })
}

/// Check if request is a TempURL request (has required query parameters)
pub fn is_tempurl_request(query: &str) -> bool {
    query.contains("temp_url_sig") && query.contains("temp_url_expires")
}

/// Generate a complete TempURL for an object
///
/// Helper function for generating TempURLs (typically used by clients).
///
/// # Arguments
/// * `method` - HTTP method to allow (GET, PUT, etc.)
/// * `path` - Full path to object (e.g., "/v1/AUTH_test/container/object")
/// * `key` - Account-level secret key
/// * `ttl_seconds` - Time-to-live in seconds
///
/// # Returns
/// Query string with signature and expiration
pub fn generate_tempurl_query(
    method: &str,
    path: &str,
    key: &str,
    ttl_seconds: u64,
) -> Result<String, SwiftError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SwiftError::InternalServerError(format!("System time error: {}", e)))?
        .as_secs();

    let expires = now + ttl_seconds;
    let signature = generate_tempurl_signature(method, expires, path, key)?;

    Ok(format!(
        "temp_url_sig={}&temp_url_expires={}",
        signature, expires
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_tempurl_signature() {
        let method = "GET";
        let expires = 1234567890u64;
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mySecretKey";

        let sig = generate_tempurl_signature(method, expires, path, key).unwrap();

        // Signature should be 40 hex characters (SHA1 = 160 bits = 20 bytes = 40 hex)
        assert_eq!(sig.len(), 40);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_tempurl_signature_deterministic() {
        let method = "GET";
        let expires = 1234567890u64;
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mySecretKey";

        let sig1 = generate_tempurl_signature(method, expires, path, key).unwrap();
        let sig2 = generate_tempurl_signature(method, expires, path, key).unwrap();

        // Same inputs should produce same signature
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_generate_tempurl_signature_different_method() {
        let expires = 1234567890u64;
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mySecretKey";

        let sig_get = generate_tempurl_signature("GET", expires, path, key).unwrap();
        let sig_put = generate_tempurl_signature("PUT", expires, path, key).unwrap();

        // Different methods should produce different signatures
        assert_ne!(sig_get, sig_put);
    }

    #[test]
    fn test_generate_tempurl_signature_different_path() {
        let method = "GET";
        let expires = 1234567890u64;
        let key = "mySecretKey";

        let sig1 = generate_tempurl_signature(method, expires, "/v1/AUTH_test/container/obj1", key).unwrap();
        let sig2 = generate_tempurl_signature(method, expires, "/v1/AUTH_test/container/obj2", key).unwrap();

        // Different paths should produce different signatures
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_generate_tempurl_signature_different_key() {
        let method = "GET";
        let expires = 1234567890u64;
        let path = "/v1/AUTH_test/container/object.txt";

        let sig1 = generate_tempurl_signature(method, expires, path, "key1").unwrap();
        let sig2 = generate_tempurl_signature(method, expires, path, "key2").unwrap();

        // Different keys should produce different signatures
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_parse_tempurl_params() {
        let query = "temp_url_sig=abc123&temp_url_expires=1234567890&temp_url_filename=file.txt";
        let params = parse_tempurl_params(query).unwrap();

        assert_eq!(params.temp_url_sig, "abc123");
        assert_eq!(params.temp_url_expires, 1234567890);
        assert_eq!(params.temp_url_filename, Some("file.txt".to_string()));
    }

    #[test]
    fn test_parse_tempurl_params_no_filename() {
        let query = "temp_url_sig=abc123&temp_url_expires=1234567890";
        let params = parse_tempurl_params(query).unwrap();

        assert_eq!(params.temp_url_sig, "abc123");
        assert_eq!(params.temp_url_expires, 1234567890);
        assert!(params.temp_url_filename.is_none());
    }

    #[test]
    fn test_parse_tempurl_params_missing_sig() {
        let query = "temp_url_expires=1234567890";
        assert!(parse_tempurl_params(query).is_none());
    }

    #[test]
    fn test_parse_tempurl_params_missing_expires() {
        let query = "temp_url_sig=abc123";
        assert!(parse_tempurl_params(query).is_none());
    }

    #[test]
    fn test_parse_tempurl_params_invalid_expires() {
        let query = "temp_url_sig=abc123&temp_url_expires=not-a-number";
        assert!(parse_tempurl_params(query).is_none());
    }

    #[test]
    fn test_parse_tempurl_params_extra_params() {
        let query = "temp_url_sig=abc123&temp_url_expires=1234567890&other_param=value";
        let params = parse_tempurl_params(query).unwrap();

        assert_eq!(params.temp_url_sig, "abc123");
        assert_eq!(params.temp_url_expires, 1234567890);
    }

    #[test]
    fn test_is_tempurl_request() {
        assert!(is_tempurl_request("temp_url_sig=abc&temp_url_expires=123"));
        assert!(is_tempurl_request("foo=bar&temp_url_sig=abc&temp_url_expires=123"));
        assert!(!is_tempurl_request("temp_url_sig=abc"));
        assert!(!is_tempurl_request("temp_url_expires=123"));
        assert!(!is_tempurl_request("other_params=only"));
    }

    #[test]
    fn test_validate_tempurl_success() {
        let key = "mySecretKey";
        let path = "/v1/AUTH_test/container/object.txt";
        let method = "GET";

        // Generate a TempURL that expires in 1 hour
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires = now + 3600;

        let signature = generate_tempurl_signature(method, expires, path, key).unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_filename: None,
        };

        // Should validate successfully
        assert!(validate_tempurl(method, path, &params, key).is_ok());
    }

    #[test]
    fn test_validate_tempurl_expired() {
        let key = "mySecretKey";
        let path = "/v1/AUTH_test/container/object.txt";
        let method = "GET";

        // Generate a TempURL that expired 1 hour ago
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires = now - 3600;

        let signature = generate_tempurl_signature(method, expires, path, key).unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_filename: None,
        };

        // Should fail validation (expired)
        let result = validate_tempurl(method, path, &params, key);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_validate_tempurl_wrong_signature() {
        let key = "mySecretKey";
        let path = "/v1/AUTH_test/container/object.txt";
        let method = "GET";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires = now + 3600;

        let params = TempURLParams {
            temp_url_sig: "wrong_signature".to_string(),
            temp_url_expires: expires,
            temp_url_filename: None,
        };

        // Should fail validation (wrong signature)
        let result = validate_tempurl(method, path, &params, key);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_validate_tempurl_wrong_method() {
        let key = "mySecretKey";
        let path = "/v1/AUTH_test/container/object.txt";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires = now + 3600;

        // Sign with GET
        let signature = generate_tempurl_signature("GET", expires, path, key).unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_filename: None,
        };

        // Try to validate with PUT (should fail)
        let result = validate_tempurl("PUT", path, &params, key);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_validate_tempurl_wrong_path() {
        let key = "mySecretKey";
        let method = "GET";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expires = now + 3600;

        // Sign with one path
        let signature = generate_tempurl_signature(method, expires, "/v1/AUTH_test/container/obj1", key).unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_filename: None,
        };

        // Try to validate with different path (should fail)
        let result = validate_tempurl(method, "/v1/AUTH_test/container/obj2", &params, key);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_generate_tempurl_query() {
        let method = "GET";
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mySecretKey";
        let ttl = 3600;

        let query = generate_tempurl_query(method, path, key, ttl).unwrap();

        // Should contain both required parameters
        assert!(query.contains("temp_url_sig="));
        assert!(query.contains("temp_url_expires="));

        // Should be parseable
        let params = parse_tempurl_params(&query);
        assert!(params.is_some());
    }

    #[test]
    fn test_method_case_insensitive() {
        let expires = 1234567890u64;
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mySecretKey";

        // All methods are normalized to uppercase
        let sig1 = generate_tempurl_signature("get", expires, path, key).unwrap();
        let sig2 = generate_tempurl_signature("GET", expires, path, key).unwrap();
        let sig3 = generate_tempurl_signature("Get", expires, path, key).unwrap();

        assert_eq!(sig1, sig2);
        assert_eq!(sig2, sig3);
    }
}
