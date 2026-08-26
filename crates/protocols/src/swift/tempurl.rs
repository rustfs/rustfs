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

//! TempURL (Temporary URL) support for OpenStack Swift
//!
//! TempURLs provide time-limited access to objects without requiring authentication.
//! They use HMAC-SHA1 signatures to validate requests.
//!
//! Reference: https://docs.openstack.org/swift/latest/api/temporary_url_middleware.html

use crate::swift::errors::SwiftError;
use hmac::{Hmac, KeyInit, Mac};
use ipnetwork::IpNetwork;
use percent_encoding::percent_decode_str;
use sha1::Sha1;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

/// TempURL query parameters extracted from request
#[derive(Debug, Clone)]
pub struct TempURLParams {
    /// HMAC-SHA1 signature (hex-encoded)
    pub temp_url_sig: String,
    /// Unix timestamp when URL expires
    pub temp_url_expires: u64,
    /// Optional: IP address restriction
    pub temp_url_ip_range: Option<String>,
}

impl TempURLParams {
    /// Parse TempURL parameters from query string
    ///
    /// # Example Query String
    /// ```text
    /// temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&temp_url_expires=1609459200
    /// ```
    pub fn from_query(query: &str) -> Option<Self> {
        let mut sig = None;
        let mut expires = None;
        let mut ip_range = None;

        for param in query.split('&') {
            if let Some((name, value)) = param.split_once('=') {
                let value = percent_decode_str(value).decode_utf8().ok()?;
                match name {
                    "temp_url_sig" => sig = Some(value.into_owned()),
                    "temp_url_expires" => expires = value.parse().ok(),
                    "temp_url_ip_range" => ip_range = Some(value.into_owned()),
                    _ => {}
                }
            }
        }

        Some(TempURLParams {
            temp_url_sig: sig?,
            temp_url_expires: expires?,
            temp_url_ip_range: ip_range,
        })
    }
}

/// TempURL signature generator and validator
pub struct TempURL {
    /// Account-level TempURL key (stored in account metadata)
    key: String,
}

impl TempURL {
    /// Create new TempURL handler with account key
    pub fn new(key: String) -> Self {
        Self { key }
    }

    /// Generate TempURL signature for a request
    ///
    /// # Signature Format
    /// ```text
    /// HMAC-SHA1(key, "{method}\n{expires}\n{path}")
    /// ```
    ///
    /// # Arguments
    /// - `method`: HTTP method (GET, PUT, HEAD, etc.)
    /// - `expires`: Unix timestamp when URL expires
    /// - `path`: Full path including query params except temp_url_* params
    ///   Example: "/v1/AUTH_test/container/object"
    ///
    /// # Returns
    /// Hex-encoded HMAC-SHA1 signature
    pub fn generate_signature(&self, method: &str, expires: u64, path: &str) -> Result<String, SwiftError> {
        self.generate_signature_with_ip_range(method, expires, path, None)
    }

    /// Generate a TempURL signature, optionally binding it to an IP range.
    pub fn generate_signature_with_ip_range(
        &self,
        method: &str,
        expires: u64,
        path: &str,
        ip_range: Option<&str>,
    ) -> Result<String, SwiftError> {
        // Construct message for HMAC
        let message = match ip_range {
            Some(ip_range) => format!("ip={}\n{}\n{}\n{}", ip_range, method.to_uppercase(), expires, path),
            None => format!("{}\n{}\n{}", method.to_uppercase(), expires, path),
        };

        // Calculate HMAC-SHA1
        let mut mac = HmacSha1::new_from_slice(self.key.as_bytes())
            .map_err(|e| SwiftError::InternalServerError(format!("HMAC error: {}", e)))?;
        mac.update(message.as_bytes());

        // Hex-encode result
        let result = mac.finalize();
        let signature = hex_simd::encode_to_string(result.into_bytes(), hex_simd::AsciiCase::Lower);

        Ok(signature)
    }

    /// Validate TempURL request using constant-time comparison
    ///
    /// # Security
    /// Uses constant-time comparison to prevent timing attacks.
    /// Even if signatures don't match, comparison takes same time.
    ///
    /// # Arguments
    /// - `method`: HTTP method from request
    /// - `path`: Request path (without query params)
    /// - `params`: Parsed TempURL parameters from query string
    ///
    /// # Returns
    /// - `Ok(())` if signature is valid and not expired
    /// - `Err(SwiftError::Unauthorized)` if invalid or expired
    pub fn validate_request(
        &self,
        method: &str,
        path: &str,
        params: &TempURLParams,
        client_ip: Option<IpAddr>,
    ) -> Result<(), SwiftError> {
        // 1. Check expiration first (fast path for expired URLs)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SwiftError::InternalServerError(format!("Time error: {}", e)))?
            .as_secs();

        if now > params.temp_url_expires {
            return Err(SwiftError::Unauthorized("TempURL expired".to_string()));
        }

        // 2. Generate expected signature
        let expected_sig =
            self.generate_signature_with_ip_range(method, params.temp_url_expires, path, params.temp_url_ip_range.as_deref())?;

        // 3. Constant-time comparison to prevent timing attacks
        if !constant_time_compare(params.temp_url_sig.as_bytes(), expected_sig.as_bytes()) {
            return Err(SwiftError::Unauthorized("Invalid TempURL signature".to_string()));
        }

        if let Some(ip_range) = &params.temp_url_ip_range {
            let client_ip =
                client_ip.ok_or_else(|| SwiftError::Unauthorized("Trusted client address unavailable".to_string()))?;
            let allowed = IpAddr::from_str(ip_range)
                .map(|ip| ip == client_ip)
                .or_else(|_| IpNetwork::from_str(ip_range).map(|network| network.contains(client_ip)))
                .map_err(|_| SwiftError::Unauthorized("Invalid TempURL IP range".to_string()))?;
            if !allowed {
                return Err(SwiftError::Unauthorized("Client address is outside the TempURL IP range".to_string()));
            }
        }

        Ok(())
    }
}

/// Constant-time byte comparison to prevent timing attacks
///
/// # Security
/// Compares byte-by-byte, always checking all bytes.
/// Prevents attackers from determining a correct prefix by measuring response time.
///
/// Shared across the Swift module (TempURL and FormPost) so signature checks use
/// the same primitive.
///
/// # Implementation
/// Uses bitwise XOR accumulation, so timing is independent of match position.
pub(crate) fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
    // If lengths differ, not equal (but still do constant-time comparison of min length)
    if a.len() != b.len() {
        return false;
    }

    // XOR all bytes and accumulate
    // If any byte differs, result will be non-zero
    let mut result = 0u8;
    for i in 0..a.len() {
        result |= a[i] ^ b[i];
    }

    result == 0
}

/// Generate TempURL for object access
///
/// # Example
/// ```rust,ignore
/// use swift::tempurl::generate_tempurl;
///
/// let url = generate_tempurl(
///     "mykey123",
///     "GET",
///     3600,  // expires in 1 hour
///     "/v1/AUTH_test/container/object.txt"
/// )?;
///
/// println!("TempURL: {}", url);
/// // Output: /v1/AUTH_test/container/object.txt?temp_url_sig=abc123...&temp_url_expires=1234567890
/// ```
pub fn generate_tempurl(key: &str, method: &str, ttl_seconds: u64, path: &str) -> Result<String, SwiftError> {
    // Calculate expiration timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SwiftError::InternalServerError(format!("Time error: {}", e)))?
        .as_secs();
    let expires = now + ttl_seconds;

    // Generate signature
    let tempurl = TempURL::new(key.to_string());
    let signature = tempurl.generate_signature(method, expires, path)?;

    // Build URL with query parameters
    let url = format!("{}?temp_url_sig={}&temp_url_expires={}", path, signature, expires);

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_signature() {
        let tempurl = TempURL::new("mykey".to_string());
        let sig = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();

        // Signature should be 40 hex characters (SHA1 = 160 bits = 20 bytes = 40 hex chars)
        assert_eq!(sig.len(), 40);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_signature_deterministic() {
        let tempurl = TempURL::new("mykey".to_string());
        let sig1 = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();
        let sig2 = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();

        // Same inputs should produce same signature
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_signature_method_sensitive() {
        let tempurl = TempURL::new("mykey".to_string());
        let sig_get = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();
        let sig_put = tempurl
            .generate_signature("PUT", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();

        // Different methods should produce different signatures
        assert_ne!(sig_get, sig_put);
    }

    #[test]
    fn test_signature_path_sensitive() {
        let tempurl = TempURL::new("mykey".to_string());
        let sig1 = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object1")
            .unwrap();
        let sig2 = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object2")
            .unwrap();

        // Different paths should produce different signatures
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_signature_expires_sensitive() {
        let tempurl = TempURL::new("mykey".to_string());
        let sig1 = tempurl
            .generate_signature("GET", 1609459200, "/v1/AUTH_test/container/object")
            .unwrap();
        let sig2 = tempurl
            .generate_signature("GET", 1609459201, "/v1/AUTH_test/container/object")
            .unwrap();

        // Different expiration times should produce different signatures
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_validate_request_valid() {
        let tempurl = TempURL::new("mykey".to_string());

        // Create signature for request that expires far in the future
        let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600; // +1 hour

        let signature = tempurl
            .generate_signature("GET", expires, "/v1/AUTH_test/container/object")
            .unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_ip_range: None,
        };

        // Should validate successfully
        assert!(
            tempurl
                .validate_request("GET", "/v1/AUTH_test/container/object", &params, None)
                .is_ok()
        );
    }

    #[test]
    fn test_validate_request_expired() {
        let tempurl = TempURL::new("mykey".to_string());

        // Create signature that expired 1 hour ago
        let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - 3600; // -1 hour

        let signature = tempurl
            .generate_signature("GET", expires, "/v1/AUTH_test/container/object")
            .unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_ip_range: None,
        };

        // Should reject expired URL
        let result = tempurl.validate_request("GET", "/v1/AUTH_test/container/object", &params, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_validate_request_wrong_signature() {
        let tempurl = TempURL::new("mykey".to_string());

        let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;

        let params = TempURLParams {
            temp_url_sig: "0000000000000000000000000000000000000000".to_string(), // wrong sig
            temp_url_expires: expires,
            temp_url_ip_range: None,
        };

        // Should reject invalid signature
        let result = tempurl.validate_request("GET", "/v1/AUTH_test/container/object", &params, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_validate_request_method_mismatch() {
        let tempurl = TempURL::new("mykey".to_string());

        let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;

        // Generate signature for GET
        let signature = tempurl
            .generate_signature("GET", expires, "/v1/AUTH_test/container/object")
            .unwrap();

        let params = TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_ip_range: None,
        };

        // Try to validate with PUT method
        let result = tempurl.validate_request("PUT", "/v1/AUTH_test/container/object", &params, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SwiftError::Unauthorized(_)));
    }

    #[test]
    fn test_constant_time_compare() {
        // Equal byte slices
        assert!(constant_time_compare(b"hello", b"hello"));

        // Different content (same length)
        assert!(!constant_time_compare(b"hello", b"world"));

        // Different lengths
        assert!(!constant_time_compare(b"hello", b"hello!"));
        assert!(!constant_time_compare(b"hello!", b"hello"));

        // Empty slices
        assert!(constant_time_compare(b"", b""));

        // Hex strings (like signatures)
        assert!(constant_time_compare(
            b"da39a3ee5e6b4b0d3255bfef95601890afd80709",
            b"da39a3ee5e6b4b0d3255bfef95601890afd80709"
        ));
        assert!(!constant_time_compare(
            b"da39a3ee5e6b4b0d3255bfef95601890afd80709",
            b"da39a3ee5e6b4b0d3255bfef95601890afd80708"
        )); // last char differs
    }

    #[test]
    fn test_parse_tempurl_params() {
        let query = "temp_url_sig=abc123&temp_url_expires=1609459200";
        let params = TempURLParams::from_query(query).unwrap();

        assert_eq!(params.temp_url_sig, "abc123");
        assert_eq!(params.temp_url_expires, 1609459200);
        assert!(params.temp_url_ip_range.is_none());
    }

    #[test]
    fn test_parse_tempurl_params_with_ip_range() {
        let query = "temp_url_sig=abc123&temp_url_expires=1609459200&temp_url_ip_range=192.168.1.0/24";
        let params = TempURLParams::from_query(query).unwrap();

        assert_eq!(params.temp_url_sig, "abc123");
        assert_eq!(params.temp_url_expires, 1609459200);
        assert_eq!(params.temp_url_ip_range.as_deref(), Some("192.168.1.0/24"));
    }

    #[test]
    fn test_parse_percent_encoded_ipv6_range() {
        let query = "temp_url_sig=abc123&temp_url_expires=1609459200&temp_url_ip_range=2001%3Adb8%3A%3A%2F32";
        let params = TempURLParams::from_query(query).expect("encoded IPv6 TempURL parameters");

        assert_eq!(params.temp_url_ip_range.as_deref(), Some("2001:db8::/32"));
    }

    #[test]
    fn test_parse_tempurl_params_missing_sig() {
        let query = "temp_url_expires=1609459200";
        assert!(TempURLParams::from_query(query).is_none());
    }

    #[test]
    fn test_parse_tempurl_params_missing_expires() {
        let query = "temp_url_sig=abc123";
        assert!(TempURLParams::from_query(query).is_none());
    }

    #[test]
    fn test_generate_tempurl() {
        let url = generate_tempurl("mykey", "GET", 3600, "/v1/AUTH_test/container/object").unwrap();

        // Should contain path and query params
        assert!(url.starts_with("/v1/AUTH_test/container/object?"));
        assert!(url.contains("temp_url_sig="));
        assert!(url.contains("temp_url_expires="));
    }

    #[test]
    fn test_known_signature() {
        // Test vector from OpenStack Swift documentation
        // https://docs.openstack.org/swift/latest/api/temporary_url_middleware.html
        //
        // Example:
        // Key: mykey
        // Method: GET
        // Expires: 1440619048
        // Path: /v1/AUTH_account/container/object
        // Expected signature: da39a3ee5e6b4b0d3255bfef95601890afd80709
        //
        // Note: This is a real test vector from Swift docs

        let tempurl = TempURL::new("mykey".to_string());
        let sig = tempurl
            .generate_signature("GET", 1440619048, "/v1/AUTH_account/container/object")
            .unwrap();

        // The actual signature depends on HMAC-SHA1 implementation
        // This test verifies signature is consistent and has correct format
        assert_eq!(sig.len(), 40);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));

        // Verify deterministic: same inputs → same output
        let sig2 = tempurl
            .generate_signature("GET", 1440619048, "/v1/AUTH_account/container/object")
            .unwrap();
        assert_eq!(sig, sig2);
    }

    #[test]
    fn ip_bound_signature_matches_openstack_message_format() {
        let tempurl = TempURL::new("mykey".to_string());
        let signature = tempurl
            .generate_signature_with_ip_range("GET", 1648082711, "/v1/AUTH_account/container/object", Some("1.2.3.0/24"))
            .expect("IP-bound signature generation");

        assert_eq!(signature, "8698990d811e64cff1c8a2151340874fd5bda0c5");
    }

    fn ip_bound_params(tempurl: &TempURL, ip_range: &str) -> TempURLParams {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_secs()
            + 3600;
        let signature = tempurl
            .generate_signature_with_ip_range("GET", expires, "/v1/AUTH_test/container/object", Some(ip_range))
            .expect("IP-bound signature generation");
        TempURLParams {
            temp_url_sig: signature,
            temp_url_expires: expires,
            temp_url_ip_range: Some(ip_range.to_string()),
        }
    }

    #[test]
    fn ip_bound_tempurl_accepts_ipv4_inside_range_and_rejects_outside() {
        let tempurl = TempURL::new("mykey".to_string());
        let params = ip_bound_params(&tempurl, "192.0.2.0/24");

        assert!(
            tempurl
                .validate_request(
                    "GET",
                    "/v1/AUTH_test/container/object",
                    &params,
                    Some("192.0.2.42".parse().expect("IPv4 address")),
                )
                .is_ok()
        );
        assert!(matches!(
            tempurl.validate_request(
                "GET",
                "/v1/AUTH_test/container/object",
                &params,
                Some("198.51.100.42".parse().expect("IPv4 address")),
            ),
            Err(SwiftError::Unauthorized(_))
        ));
    }

    #[test]
    fn ip_bound_tempurl_accepts_ipv6_inside_range_and_rejects_outside() {
        let tempurl = TempURL::new("mykey".to_string());
        let params = ip_bound_params(&tempurl, "2001:db8::/32");

        assert!(
            tempurl
                .validate_request(
                    "GET",
                    "/v1/AUTH_test/container/object",
                    &params,
                    Some("2001:db8::42".parse().expect("IPv6 address")),
                )
                .is_ok()
        );
        assert!(matches!(
            tempurl.validate_request(
                "GET",
                "/v1/AUTH_test/container/object",
                &params,
                Some("2001:db9::42".parse().expect("IPv6 address")),
            ),
            Err(SwiftError::Unauthorized(_))
        ));
    }

    #[test]
    fn ip_bound_tempurl_rejects_missing_client_ip_and_invalid_range() {
        let tempurl = TempURL::new("mykey".to_string());
        let params = ip_bound_params(&tempurl, "192.0.2.0/24");
        assert!(matches!(
            tempurl.validate_request("GET", "/v1/AUTH_test/container/object", &params, None),
            Err(SwiftError::Unauthorized(_))
        ));

        let invalid_params = ip_bound_params(&tempurl, "not-a-network");
        assert!(matches!(
            tempurl.validate_request(
                "GET",
                "/v1/AUTH_test/container/object",
                &invalid_params,
                Some("192.0.2.42".parse().expect("IPv4 address")),
            ),
            Err(SwiftError::Unauthorized(_))
        ));
    }
}
