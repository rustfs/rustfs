// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use base64::Engine as _;
use base64::engine::general_purpose;
use hmac::{Hmac, KeyInit, Mac};
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::Uri;
use rustfs_credentials::get_global_action_cred;
use sha2::Sha256;
use time::OffsetDateTime;
use tracing::error;

type HmacSha256 = Hmac<Sha256>;

const SIGNATURE_HEADER: &str = "x-rustfs-signature";
const TIMESTAMP_HEADER: &str = "x-rustfs-timestamp";
const SIGNATURE_VALID_DURATION: i64 = 300; // 5 minutes

/// Get the shared secret for HMAC signing
fn get_shared_secret() -> String {
    if let Some(cred) = get_global_action_cred() {
        cred.secret_key
    } else {
        // Fallback to environment variable if global credentials are not available
        std::env::var("RUSTFS_RPC_SECRET").unwrap_or_else(|_| "rustfs-default-secret".to_string())
    }
}

/// Generate HMAC-SHA256 signature for the given data
fn generate_signature(secret: &str, url: &str, method: &Method, timestamp: i64) -> String {
    let uri: Uri = url.parse().expect("Invalid URL");

    let path_and_query = uri.path_and_query().unwrap();

    let url = path_and_query.to_string();

    let data = format!("{url}|{method}|{timestamp}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    let result = mac.finalize();
    general_purpose::STANDARD.encode(result.into_bytes())
}

/// Build headers with authentication signature
pub fn build_auth_headers(url: &str, method: &Method, headers: &mut HeaderMap) {
    let secret = get_shared_secret();
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let signature = generate_signature(&secret, url, method, timestamp);

    headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
    headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());
}

/// Verify the request signature for RPC requests
pub fn verify_rpc_signature(url: &str, method: &Method, headers: &HeaderMap) -> std::io::Result<()> {
    let secret = get_shared_secret();

    // Get signature from header
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing signature header"))?;

    // Get timestamp from header
    let timestamp_str = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;

    let timestamp: i64 = timestamp_str
        .parse()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;

    // Check timestamp validity (prevent replay attacks)
    let current_time = OffsetDateTime::now_utc().unix_timestamp();

    if current_time.saturating_sub(timestamp) > SIGNATURE_VALID_DURATION {
        return Err(std::io::Error::other("Request timestamp expired"));
    }

    // Generate expected signature

    let expected_signature = generate_signature(&secret, url, method, timestamp);

    // Compare signatures
    if signature != expected_signature {
        error!(
            "verify_rpc_signature: Invalid signature: secret {}, url {}, method {}, timestamp {}, signature {}, expected_signature {}",
            secret, url, method, timestamp, signature, expected_signature
        );

        return Err(std::io::Error::other("Invalid signature"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method};
    use time::OffsetDateTime;

    #[test]
    fn test_get_shared_secret() {
        let secret = get_shared_secret();
        assert!(!secret.is_empty(), "Secret should not be empty");

        let url = "http://node1:7000/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers);

        let url = "/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_generate_signature_deterministic() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200; // Fixed timestamp

        let signature1 = generate_signature(secret, url, &method, timestamp);
        let signature2 = generate_signature(secret, url, &method, timestamp);

        assert_eq!(signature1, signature2, "Same inputs should produce same signature");
        assert!(!signature1.is_empty(), "Signature should not be empty");
    }

    #[test]
    fn test_generate_signature_different_inputs() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200;

        let signature1 = generate_signature(secret, url, &method, timestamp);
        let signature2 = generate_signature(secret, "http://different.com/api/test2", &method, timestamp);
        let signature3 = generate_signature(secret, url, &Method::POST, timestamp);
        let signature4 = generate_signature(secret, url, &method, timestamp + 1);

        assert_ne!(signature1, signature2, "Different URLs should produce different signatures");
        assert_ne!(signature1, signature3, "Different methods should produce different signatures");
        assert_ne!(signature1, signature4, "Different timestamps should produce different signatures");
    }

    #[test]
    fn test_build_auth_headers() {
        let url = "http://example.com/api/test";
        let method = Method::POST;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers);

        // Verify headers are present
        assert!(headers.contains_key(SIGNATURE_HEADER), "Should contain signature header");
        assert!(headers.contains_key(TIMESTAMP_HEADER), "Should contain timestamp header");

        // Verify header values are not empty
        let signature = headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap();
        let timestamp_str = headers.get(TIMESTAMP_HEADER).unwrap().to_str().unwrap();

        assert!(!signature.is_empty(), "Signature should not be empty");
        assert!(!timestamp_str.is_empty(), "Timestamp should not be empty");

        // Verify timestamp is a valid integer
        let timestamp: i64 = timestamp_str.parse().expect("Timestamp should be valid integer");
        let current_time = OffsetDateTime::now_utc().unix_timestamp();

        // Should be within a reasonable range (within 1 second of current time)
        assert!((current_time - timestamp).abs() <= 1, "Timestamp should be close to current time");
    }

    #[test]
    fn test_verify_rpc_signature_success() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature
        build_auth_headers(url, &method, &mut headers);

        // Verify should succeed
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_signature() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature first
        build_auth_headers(url, &method, &mut headers);

        // Tamper with the signature
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("invalid-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid signature should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_rpc_signature_expired_timestamp() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Set expired timestamp (older than SIGNATURE_VALID_DURATION)
        let expired_timestamp = OffsetDateTime::now_utc().unix_timestamp() - SIGNATURE_VALID_DURATION - 10;
        let secret = get_shared_secret();
        let signature = generate_signature(&secret, url, &method, expired_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&expired_timestamp.to_string()).unwrap());

        // Verify should fail due to expired timestamp
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Expired timestamp should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Request timestamp expired");
    }

    #[test]
    fn test_verify_rpc_signature_missing_signature_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only timestamp header, missing signature
        let timestamp = OffsetDateTime::now_utc().unix_timestamp();
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing signature header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing signature header");
    }

    #[test]
    fn test_verify_rpc_signature_missing_timestamp_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only signature header, missing timestamp
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing timestamp header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing timestamp header");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_timestamp_format() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str("invalid-timestamp").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid timestamp format should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid timestamp format");
    }

    #[test]
    fn test_verify_rpc_signature_url_mismatch() {
        let original_url = "http://example.com/api/test";
        let different_url = "http://example.com/api/different";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers for one URL
        build_auth_headers(original_url, &method, &mut headers);

        // Try to verify with a different URL
        let result = verify_rpc_signature(different_url, &method, &headers);
        assert!(result.is_err(), "URL mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_rpc_signature_method_mismatch() {
        let url = "http://example.com/api/test";
        let original_method = Method::GET;
        let different_method = Method::POST;
        let mut headers = HeaderMap::new();

        // Build headers for one method
        build_auth_headers(url, &original_method, &mut headers);

        // Try to verify with a different method
        let result = verify_rpc_signature(url, &different_method, &headers);
        assert!(result.is_err(), "Method mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_signature_valid_duration_boundary() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let secret = get_shared_secret();

        let mut headers = HeaderMap::new();
        let current_time = OffsetDateTime::now_utc().unix_timestamp();
        // Test timestamp just within valid duration
        let valid_timestamp = current_time - SIGNATURE_VALID_DURATION + 1;

        let signature = generate_signature(&secret, url, &method, valid_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&valid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Timestamp within valid duration should pass");

        // Test timestamp just outside valid duration
        let mut headers = HeaderMap::new();
        let invalid_timestamp = current_time - SIGNATURE_VALID_DURATION - 15;
        let signature = generate_signature(&secret, url, &method, invalid_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&invalid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Timestamp outside valid duration should fail");
    }

    #[test]
    fn test_round_trip_authentication() {
        let test_cases = vec![
            ("http://example.com/api/test", Method::GET),
            ("https://api.rustfs.com/v1/bucket", Method::POST),
            ("http://localhost:9000/admin/info", Method::PUT),
            ("https://storage.example.com/path/to/object?query=param", Method::DELETE),
        ];

        for (url, method) in test_cases {
            let mut headers = HeaderMap::new();

            // Build authentication headers
            build_auth_headers(url, &method, &mut headers);

            // Verify the signature should succeed
            let result = verify_rpc_signature(url, &method, &headers);
            assert!(result.is_ok(), "Round-trip test failed for {method} {url}");
        }
    }
}
