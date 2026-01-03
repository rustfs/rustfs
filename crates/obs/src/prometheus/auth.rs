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

//! JWT Bearer token authentication for Prometheus metrics endpoints.
//!
//! This module provides utilities for generating and validating JWT tokens
//! used to authenticate Prometheus scrapers accessing RustFS metrics endpoints.
//!
//! # Overview
//!
//! RustFS follows MinIO's pattern for Prometheus authentication:
//! - Prometheus scraper sends `Authorization: Bearer <token>` header
//! - Tokens are JWTs signed with the user's secret key using HS512 algorithm
//! - Configuration can be generated similar to `mc admin prometheus generate`
//!
//! # Example
//!
//! ```rust
//! use rustfs_obs::prometheus::auth::{generate_prometheus_token, verify_bearer_token};
//! use http::HeaderMap;
//!
//! // Generate a token for Prometheus scraper
//! let token = generate_prometheus_token("access-key", "secret-key", None)
//!     .expect("Failed to generate token");
//!
//! // Verify the token from incoming request headers
//! let mut headers = HeaderMap::new();
//! headers.insert(
//!     http::header::AUTHORIZATION,
//!     format!("Bearer {}", token).parse().unwrap(),
//! );
//!
//! let claims = verify_bearer_token(&headers, "secret-key")
//!     .expect("Token verification failed");
//! assert_eq!(claims.sub, "access-key");
//! ```

use http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// JWT claims for Prometheus authentication.
///
/// These claims follow standard JWT conventions and are used to identify
/// the authenticated user and validate token expiration.
///
/// Note: `iat` is optional for compatibility with MinIO CLI-generated tokens
/// which only include `iss`, `sub`, and `exp` fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrometheusClaims {
    /// Subject - the access key ID of the authenticated user
    pub sub: String,
    /// Issued at timestamp (Unix epoch seconds) - optional for mc CLI compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    /// Expiration timestamp (Unix epoch seconds)
    pub exp: u64,
    /// Issuer identifier
    pub iss: String,
}

/// Error types for Prometheus authentication operations.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The HTTP request is missing the Authorization header
    #[error("Missing authorization header")]
    MissingAuthHeader,

    /// The Authorization header format is invalid (expected "Bearer <token>")
    #[error("Invalid authorization format, expected 'Bearer <token>'")]
    InvalidAuthFormat,

    /// The JWT token is invalid or has expired
    #[error("Invalid or expired token: {0}")]
    InvalidToken(String),

    /// Token generation failed due to an internal error
    #[error("Token generation failed: {0}")]
    TokenGenerationFailed(String),
}

/// Default token expiry duration: 100 years in seconds (effectively never expires).
///
/// Prometheus scrapers run continuously and token expiration would cause
/// silent monitoring failures. Since these tokens only grant read access
/// to metrics, the security risk is minimal. If a token is compromised,
/// rotate the server's secret key to invalidate all tokens.
pub const DEFAULT_TOKEN_EXPIRY_SECS: u64 = 100 * 365 * 24 * 60 * 60;

/// Prometheus issuer identifier used in JWT claims.
const PROMETHEUS_ISSUER: &str = "prometheus";

/// Generate a JWT token for Prometheus authentication.
///
/// Creates a signed JWT token that can be used by Prometheus scrapers
/// to authenticate against RustFS metrics endpoints.
///
/// # Arguments
///
/// * `access_key` - The access key ID to include in the token's subject claim
/// * `secret_key` - The secret key used to sign the token (HS512 algorithm)
/// * `expiry_secs` - Optional expiry duration in seconds (defaults to 30 days)
///
/// # Returns
///
/// Returns the encoded JWT token string on success, or an `AuthError` on failure.
///
/// # Example
///
/// ```rust
/// use rustfs_obs::prometheus::auth::generate_prometheus_token;
///
/// // Generate with default expiry (30 days)
/// let token = generate_prometheus_token("admin", "supersecret123", None)
///     .expect("Failed to generate token");
///
/// // Generate with custom expiry (1 hour)
/// let short_lived = generate_prometheus_token("admin", "supersecret123", Some(3600))
///     .expect("Failed to generate token");
/// ```
pub fn generate_prometheus_token(access_key: &str, secret_key: &str, expiry_secs: Option<u64>) -> Result<String, AuthError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| AuthError::TokenGenerationFailed(e.to_string()))?
        .as_secs();

    let expiry = expiry_secs.unwrap_or(DEFAULT_TOKEN_EXPIRY_SECS);

    let claims = PrometheusClaims {
        sub: access_key.to_string(),
        iat: Some(now),
        exp: now + expiry,
        iss: PROMETHEUS_ISSUER.to_string(),
    };

    let header = Header::new(Algorithm::HS512);
    let key = EncodingKey::from_secret(secret_key.as_bytes());

    encode(&header, &claims, &key).map_err(|e| AuthError::TokenGenerationFailed(e.to_string()))
}

/// Verify a Bearer token from HTTP request headers.
///
/// Extracts the JWT token from the Authorization header and validates it
/// against the provided secret key.
///
/// # Arguments
///
/// * `headers` - The HTTP headers from the incoming request
/// * `secret_key` - The secret key used to verify the token signature
///
/// # Returns
///
/// Returns the decoded `PrometheusClaims` on success, or an `AuthError` on failure.
///
/// # Errors
///
/// Returns an error if:
/// - The Authorization header is missing
/// - The header doesn't have the "Bearer " prefix
/// - The token is invalid, expired, or has an invalid signature
///
/// # Example
///
/// ```rust
/// use rustfs_obs::prometheus::auth::verify_bearer_token;
/// use http::HeaderMap;
///
/// let mut headers = HeaderMap::new();
/// headers.insert(
///     http::header::AUTHORIZATION,
///     "Bearer eyJ0eXAiOiJKV1QiLC...".parse().unwrap(),
/// );
///
/// match verify_bearer_token(&headers, "secret-key") {
///     Ok(claims) => println!("Authenticated as: {}", claims.sub),
///     Err(e) => eprintln!("Authentication failed: {}", e),
/// }
/// ```
pub fn verify_bearer_token(headers: &HeaderMap, secret_key: &str) -> Result<PrometheusClaims, AuthError> {
    let auth_header = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::MissingAuthHeader)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(AuthError::InvalidAuthFormat);
    }

    let token = &auth_header[7..];
    validate_token(token, secret_key)
}

/// Verify a Bearer token from HTTP request headers or query parameter.
///
/// This function first tries to extract the JWT token from the Authorization header.
/// If the header is missing, it falls back to checking the `token` query parameter.
/// This is useful when the Authorization header cannot be used (e.g., when s3s
/// intercepts it for AWS signature parsing).
///
/// # Arguments
///
/// * `headers` - The HTTP headers from the incoming request
/// * `uri` - The request URI (used to extract query parameters)
/// * `secret_key` - The secret key used to verify the token signature
///
/// # Returns
///
/// Returns the decoded `PrometheusClaims` on success, or an `AuthError` on failure.
///
/// # Example
///
/// ```rust
/// use rustfs_obs::prometheus::auth::verify_bearer_token_with_query;
/// use http::{HeaderMap, Uri};
///
/// // With query parameter
/// let headers = HeaderMap::new();
/// let uri: Uri = "/metrics?token=eyJ...".parse().unwrap();
///
/// match verify_bearer_token_with_query(&headers, &uri, "secret-key") {
///     Ok(claims) => println!("Authenticated as: {}", claims.sub),
///     Err(e) => eprintln!("Authentication failed: {}", e),
/// }
/// ```
pub fn verify_bearer_token_with_query(
    headers: &HeaderMap,
    uri: &http::Uri,
    secret_key: &str,
) -> Result<PrometheusClaims, AuthError> {
    // First try the Authorization header
    if let Some(auth_header) = headers.get(http::header::AUTHORIZATION).and_then(|v| v.to_str().ok())
        && let Some(token) = auth_header.strip_prefix("Bearer ")
    {
        return validate_token(token, secret_key);
    }

    // Fall back to query parameter
    let token = extract_token_from_query(uri).ok_or(AuthError::MissingAuthHeader)?;
    validate_token(&token, secret_key)
}

/// Extract the token from a URI query string.
///
/// Looks for a `token` parameter in the query string and returns its value.
fn extract_token_from_query(uri: &http::Uri) -> Option<String> {
    uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            if k == "token" {
                urlencoding::decode(v).ok().map(|s| s.into_owned())
            } else {
                None
            }
        })
    })
}

/// Validate a JWT token string.
///
/// Decodes and validates the JWT token using the HS512 algorithm,
/// checking the signature, expiration, and required claims.
///
/// # Arguments
///
/// * `token` - The JWT token string to validate
/// * `secret_key` - The secret key used to verify the token signature
///
/// # Returns
///
/// Returns the decoded `PrometheusClaims` on success, or an `AuthError` on failure.
///
/// # Example
///
/// ```rust
/// use rustfs_obs::prometheus::auth::{generate_prometheus_token, validate_token};
///
/// let token = generate_prometheus_token("user", "secret", Some(3600)).unwrap();
/// let claims = validate_token(&token, "secret").expect("Validation failed");
/// assert_eq!(claims.sub, "user");
/// ```
pub fn validate_token(token: &str, secret_key: &str) -> Result<PrometheusClaims, AuthError> {
    let key = DecodingKey::from_secret(secret_key.as_bytes());
    let mut validation = Validation::new(Algorithm::HS512);
    validation.set_issuer(&[PROMETHEUS_ISSUER]);
    validation.set_required_spec_claims(&["exp", "iat", "sub", "iss"]);

    let token_data = decode::<PrometheusClaims>(token, &key, &validation).map_err(|e| AuthError::InvalidToken(e.to_string()))?;

    Ok(token_data.claims)
}

/// Generate Prometheus scrape configuration YAML.
///
/// Creates a YAML configuration snippet that can be added to Prometheus's
/// configuration file to scrape RustFS metrics endpoints.
///
/// # Arguments
///
/// * `token` - The JWT bearer token for authentication
/// * `endpoint` - The RustFS server endpoint (host:port)
/// * `scheme` - The URL scheme ("http" or "https")
///
/// # Returns
///
/// A YAML-formatted string containing scrape configurations for all
/// RustFS metrics endpoints (cluster, bucket, node, resource).
///
/// # Example
///
/// ```rust
/// use rustfs_obs::prometheus::auth::{generate_prometheus_token, generate_prometheus_config};
///
/// let token = generate_prometheus_token("admin", "secret", None).unwrap();
/// let config = generate_prometheus_config(&token, "localhost:9000", "http");
/// println!("{}", config);
/// ```
pub fn generate_prometheus_config(token: &str, endpoint: &str, scheme: &str) -> String {
    format!(
        r#"scrape_configs:
- job_name: rustfs-cluster
  bearer_token: {token}
  metrics_path: /rustfs/v2/metrics/cluster
  scheme: {scheme}
  static_configs:
  - targets: ['{endpoint}']

- job_name: rustfs-bucket
  bearer_token: {token}
  metrics_path: /rustfs/v2/metrics/bucket
  scheme: {scheme}
  static_configs:
  - targets: ['{endpoint}']

- job_name: rustfs-node
  bearer_token: {token}
  metrics_path: /rustfs/v2/metrics/node
  scheme: {scheme}
  static_configs:
  - targets: ['{endpoint}']

- job_name: rustfs-resource
  bearer_token: {token}
  metrics_path: /rustfs/v2/metrics/resource
  scheme: {scheme}
  static_configs:
  - targets: ['{endpoint}']
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_validate_token() {
        let access_key = "test-access-key";
        let secret_key = "test-secret-key-must-be-long-enough";

        let token = generate_prometheus_token(access_key, secret_key, Some(3600)).expect("Token generation should succeed");

        let claims = validate_token(&token, secret_key).expect("Token validation should succeed");

        assert_eq!(claims.sub, access_key);
        assert_eq!(claims.iss, PROMETHEUS_ISSUER);
    }

    #[test]
    fn test_generate_token_default_expiry() {
        let token = generate_prometheus_token("user", "secret-key-long-enough", None).expect("Token generation should succeed");

        let claims = validate_token(&token, "secret-key-long-enough").expect("Validation should succeed");

        // Verify expiry is approximately 100 years from now (effectively never expires)
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let expected_expiry = now + DEFAULT_TOKEN_EXPIRY_SECS;
        // Allow 5 second tolerance for test execution time
        assert!(claims.exp >= expected_expiry - 5 && claims.exp <= expected_expiry + 5);
    }

    #[test]
    fn test_invalid_token() {
        let result = validate_token("invalid.token.here", "some-secret");
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn test_wrong_secret_key() {
        let token = generate_prometheus_token("user", "correct-secret", Some(3600)).expect("Token generation should succeed");

        let result = validate_token(&token, "wrong-secret");
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn test_verify_bearer_token() {
        let access_key = "admin";
        let secret_key = "supersecretkey123456789";

        let token = generate_prometheus_token(access_key, secret_key, None).expect("Token generation should succeed");

        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let claims = verify_bearer_token(&headers, secret_key).expect("Bearer token verification failed");
        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_missing_authorization_header() {
        let headers = HeaderMap::new();
        let result = verify_bearer_token(&headers, "secret");
        assert!(matches!(result, Err(AuthError::MissingAuthHeader)));
    }

    #[test]
    fn test_missing_bearer_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, "Basic abc123".parse().unwrap());

        let result = verify_bearer_token(&headers, "secret");
        assert!(matches!(result, Err(AuthError::InvalidAuthFormat)));
    }

    #[test]
    fn test_empty_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, "Bearer ".parse().unwrap());

        let result = verify_bearer_token(&headers, "secret");
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn test_prometheus_claims_serialization() {
        let claims = PrometheusClaims {
            sub: "test-user".to_string(),
            iat: Some(1000000),
            exp: 2000000,
            iss: PROMETHEUS_ISSUER.to_string(),
        };

        let json = serde_json::to_string(&claims).expect("Serialization should succeed");
        let deserialized: PrometheusClaims = serde_json::from_str(&json).expect("Deserialization should succeed");

        assert_eq!(claims, deserialized);
    }

    #[test]
    fn test_generate_prometheus_config_http() {
        let token = "test-token-value";
        let config = generate_prometheus_config(token, "localhost:9000", "http");

        assert!(config.contains("bearer_token: test-token-value"));
        assert!(config.contains("scheme: http"));
        assert!(config.contains("targets: ['localhost:9000']"));
        assert!(config.contains("job_name: rustfs-cluster"));
        assert!(config.contains("job_name: rustfs-bucket"));
        assert!(config.contains("job_name: rustfs-node"));
        assert!(config.contains("job_name: rustfs-resource"));
        assert!(config.contains("/rustfs/v2/metrics/cluster"));
        assert!(config.contains("/rustfs/v2/metrics/bucket"));
        assert!(config.contains("/rustfs/v2/metrics/node"));
        assert!(config.contains("/rustfs/v2/metrics/resource"));
    }

    #[test]
    fn test_generate_prometheus_config_https() {
        let token = "secure-token";
        let config = generate_prometheus_config(token, "rustfs.example.com:443", "https");

        assert!(config.contains("scheme: https"));
        assert!(config.contains("targets: ['rustfs.example.com:443']"));
    }

    #[test]
    fn test_token_expiry_validation() {
        let access_key = "user";
        let secret_key = "secret-key-long-enough";

        // Create a token with 1 second expiry - this should be valid initially
        let token = generate_prometheus_token(access_key, secret_key, Some(1)).expect("Token generation should succeed");

        // Immediately validate - should succeed
        let result = validate_token(&token, secret_key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_auth_error_display() {
        let err = AuthError::MissingAuthHeader;
        assert_eq!(format!("{}", err), "Missing authorization header");

        let err = AuthError::InvalidAuthFormat;
        assert_eq!(format!("{}", err), "Invalid authorization format, expected 'Bearer <token>'");

        let err = AuthError::InvalidToken("signature mismatch".to_string());
        assert_eq!(format!("{}", err), "Invalid or expired token: signature mismatch");

        let err = AuthError::TokenGenerationFailed("time error".to_string());
        assert_eq!(format!("{}", err), "Token generation failed: time error");
    }

    #[test]
    fn test_claims_fields() {
        let secret_key = "test-secret-key-for-claims";
        let access_key = "claims-test-user";

        let token = generate_prometheus_token(access_key, secret_key, Some(7200)).expect("Token generation should succeed");

        let claims = validate_token(&token, secret_key).expect("Validation should succeed");

        // Verify all fields are populated correctly
        assert_eq!(claims.sub, access_key);
        assert_eq!(claims.iss, PROMETHEUS_ISSUER);
        let iat = claims.iat.expect("iat should be present in generated tokens");
        assert!(iat > 0);
        assert!(claims.exp > iat);
        assert_eq!(claims.exp - iat, 7200);
    }

    #[test]
    fn test_special_characters_in_access_key() {
        let access_key = "user@example.com";
        let secret_key = "secret123456";

        let token = generate_prometheus_token(access_key, secret_key, Some(3600)).expect("Token generation should succeed");

        let claims = validate_token(&token, secret_key).expect("Validation should succeed");

        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_long_secret_key() {
        let access_key = "user";
        let secret_key = "a".repeat(256);

        let token = generate_prometheus_token(access_key, &secret_key, Some(3600)).expect("Token generation should succeed");

        let claims = validate_token(&token, &secret_key).expect("Validation should succeed");

        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_short_secret_key() {
        let access_key = "user";
        let secret_key = "a"; // Very short secret

        let token = generate_prometheus_token(access_key, secret_key, Some(3600)).expect("Token generation should succeed");

        let claims = validate_token(&token, secret_key).expect("Validation should succeed");

        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_verify_bearer_token_with_query_from_header() {
        let access_key = "admin";
        let secret_key = "supersecretkey123456789";

        let token = generate_prometheus_token(access_key, secret_key, None).expect("Token generation should succeed");

        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());

        let uri: http::Uri = "/metrics".parse().unwrap();

        let claims = verify_bearer_token_with_query(&headers, &uri, secret_key).expect("Token verification should succeed");
        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_verify_bearer_token_with_query_from_query_param() {
        let access_key = "admin";
        let secret_key = "supersecretkey123456789";

        let token = generate_prometheus_token(access_key, secret_key, None).expect("Token generation should succeed");

        let headers = HeaderMap::new();
        let uri: http::Uri = format!("/metrics?token={}", token).parse().unwrap();

        let claims = verify_bearer_token_with_query(&headers, &uri, secret_key).expect("Token verification should succeed");
        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_verify_bearer_token_with_query_prefers_header() {
        let access_key_header = "header-user";
        let access_key_query = "query-user";
        let secret_key = "supersecretkey123456789";

        let token_header =
            generate_prometheus_token(access_key_header, secret_key, None).expect("Token generation should succeed");
        let token_query = generate_prometheus_token(access_key_query, secret_key, None).expect("Token generation should succeed");

        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, format!("Bearer {}", token_header).parse().unwrap());

        let uri: http::Uri = format!("/metrics?token={}", token_query).parse().unwrap();

        // Should use the header token, not the query param
        let claims = verify_bearer_token_with_query(&headers, &uri, secret_key).expect("Token verification should succeed");
        assert_eq!(claims.sub, access_key_header);
    }

    #[test]
    fn test_verify_bearer_token_with_query_missing_both() {
        let headers = HeaderMap::new();
        let uri: http::Uri = "/metrics".parse().unwrap();

        let result = verify_bearer_token_with_query(&headers, &uri, "secret");
        assert!(matches!(result, Err(AuthError::MissingAuthHeader)));
    }

    #[test]
    fn test_verify_bearer_token_with_query_invalid_token() {
        let headers = HeaderMap::new();
        let uri: http::Uri = "/metrics?token=invalid-token".parse().unwrap();

        let result = verify_bearer_token_with_query(&headers, &uri, "secret");
        assert!(matches!(result, Err(AuthError::InvalidToken(_))));
    }

    #[test]
    fn test_extract_token_from_query_url_encoded() {
        let access_key = "user";
        let secret_key = "secret123456";

        let token = generate_prometheus_token(access_key, secret_key, Some(3600)).expect("Token generation should succeed");

        // URL-encode the token (JWT tokens contain '.' which are safe, but test the mechanism)
        let encoded_token = urlencoding::encode(&token);
        let headers = HeaderMap::new();
        let uri: http::Uri = format!("/metrics?token={}", encoded_token).parse().unwrap();

        let claims = verify_bearer_token_with_query(&headers, &uri, secret_key).expect("Token verification should succeed");
        assert_eq!(claims.sub, access_key);
    }

    #[test]
    fn test_extract_token_from_query_with_other_params() {
        let access_key = "user";
        let secret_key = "secret123456";

        let token = generate_prometheus_token(access_key, secret_key, Some(3600)).expect("Token generation should succeed");

        let headers = HeaderMap::new();
        let uri: http::Uri = format!("/metrics?foo=bar&token={}&baz=qux", token).parse().unwrap();

        let claims = verify_bearer_token_with_query(&headers, &uri, secret_key).expect("Token verification should succeed");
        assert_eq!(claims.sub, access_key);
    }
}
