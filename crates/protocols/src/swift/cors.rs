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

//! CORS (Cross-Origin Resource Sharing) Support for Swift API
//!
//! This module implements CORS configuration and response header injection
//! for Swift containers and objects, enabling web browsers to access Swift
//! resources from different origins.
//!
//! # Configuration
//!
//! CORS is configured via container metadata:
//!
//! - `X-Container-Meta-Access-Control-Allow-Origin`: Allowed origins (e.g., `*` or `https://example.com`)
//! - `X-Container-Meta-Access-Control-Max-Age`: Preflight cache duration in seconds
//! - `X-Container-Meta-Access-Control-Expose-Headers`: Headers exposed to browser
//! - `X-Container-Meta-Access-Control-Allow-Credentials`: Allow credentials (true/false)
//!
//! # Usage
//!
//! ```bash
//! # Enable CORS for all origins
//! swift post my-container \
//!   -H "X-Container-Meta-Access-Control-Allow-Origin: *" \
//!   -H "X-Container-Meta-Access-Control-Max-Age: 86400"
//!
//! # Enable CORS for specific origin
//! swift post my-container \
//!   -H "X-Container-Meta-Access-Control-Allow-Origin: https://example.com"
//!
//! # Expose custom headers
//! swift post my-container \
//!   -H "X-Container-Meta-Access-Control-Expose-Headers: X-Custom-Header, X-Another-Header"
//! ```
//!
//! # Preflight Requests
//!
//! Browsers send OPTIONS requests for preflight checks. This module handles
//! these requests by returning appropriate Access-Control-* headers based on
//! container configuration.

use super::{SwiftError, SwiftResult, container};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use rustfs_credentials::Credentials;
use s3s::Body;
use tracing::debug;

/// CORS configuration for a container
#[derive(Debug, Clone, Default)]
pub struct CorsConfig {
    /// Allowed origins (e.g., "*" or "https://example.com")
    pub allow_origin: Option<String>,

    /// Maximum age for preflight cache in seconds
    pub max_age: Option<u64>,

    /// Headers exposed to browser
    pub expose_headers: Option<Vec<String>>,

    /// Allow credentials (cookies, authorization headers)
    pub allow_credentials: bool,
}

impl CorsConfig {
    /// Load CORS configuration from container metadata
    pub async fn load(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<Self> {
        // Get container metadata
        let container_info = container::get_container_metadata(account, container_name, credentials).await?;

        let mut config = CorsConfig::default();

        // Parse Access-Control-Allow-Origin
        if let Some(origin) = container_info
            .custom_metadata
            .get("x-container-meta-access-control-allow-origin")
        {
            config.allow_origin = Some(origin.clone());
        }

        // Parse Access-Control-Max-Age
        if let Some(max_age_str) = container_info.custom_metadata.get("x-container-meta-access-control-max-age") {
            config.max_age = max_age_str.parse().ok();
        }

        // Parse Access-Control-Expose-Headers
        if let Some(expose_headers_str) = container_info
            .custom_metadata
            .get("x-container-meta-access-control-expose-headers")
        {
            config.expose_headers = Some(expose_headers_str.split(',').map(|s: &str| s.trim().to_string()).collect());
        }

        // Parse Access-Control-Allow-Credentials
        if let Some(allow_creds) = container_info
            .custom_metadata
            .get("x-container-meta-access-control-allow-credentials")
        {
            config.allow_credentials = allow_creds.to_lowercase() == "true";
        }

        Ok(config)
    }

    /// Check if CORS is enabled
    pub fn is_enabled(&self) -> bool {
        self.allow_origin.is_some()
    }

    /// Add CORS headers to response
    pub fn inject_headers(&self, response: &mut Response<Body>, request_origin: Option<&str>) {
        if !self.is_enabled() {
            return;
        }

        // Add Access-Control-Allow-Origin
        if let Some(allow_origin) = &self.allow_origin {
            if allow_origin == "*" {
                // Wildcard origin
                let _ = response
                    .headers_mut()
                    .insert("access-control-allow-origin", HeaderValue::from_static("*"));
            } else if let Some(origin) = request_origin {
                // Check if request origin matches configured origin
                if allow_origin == origin
                    && let Ok(header_value) = HeaderValue::from_str(origin)
                {
                    let _ = response.headers_mut().insert("access-control-allow-origin", header_value);
                }
            }
        }

        // Add Access-Control-Expose-Headers
        if let Some(expose_headers) = &self.expose_headers {
            let headers_str = expose_headers.join(", ");
            if let Ok(header_value) = HeaderValue::from_str(&headers_str) {
                let _ = response.headers_mut().insert("access-control-expose-headers", header_value);
            }
        }

        // Add Access-Control-Allow-Credentials
        if self.allow_credentials {
            let _ = response
                .headers_mut()
                .insert("access-control-allow-credentials", HeaderValue::from_static("true"));
        }
    }
}

/// Handle OPTIONS preflight request
pub async fn handle_preflight(
    account: &str,
    container_name: &str,
    credentials: &Credentials,
    request_headers: &HeaderMap,
) -> SwiftResult<Response<Body>> {
    debug!("CORS preflight request for container: {}", container_name);

    // Load CORS configuration
    let config = CorsConfig::load(account, container_name, credentials).await?;

    if !config.is_enabled() {
        return Err(SwiftError::Forbidden("CORS not configured for this container".to_string()));
    }

    // Get request origin
    let request_origin = request_headers.get("origin").and_then(|v| v.to_str().ok());

    // Build preflight response
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?;

    // Add CORS headers
    config.inject_headers(&mut response, request_origin);

    // Add Access-Control-Allow-Methods (all Swift methods)
    response.headers_mut().insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, PUT, POST, DELETE, HEAD, OPTIONS"),
    );

    // Add Access-Control-Max-Age
    if let Some(max_age) = config.max_age
        && let Ok(header_value) = HeaderValue::from_str(&max_age.to_string())
    {
        response.headers_mut().insert("access-control-max-age", header_value);
    }

    // Echo back Access-Control-Request-Headers if present
    if let Some(request_headers_value) = request_headers.get("access-control-request-headers") {
        response
            .headers_mut()
            .insert("access-control-allow-headers", request_headers_value.clone());
    }

    let trans_id = super::handler::generate_trans_id();
    response.headers_mut().insert(
        "x-trans-id",
        HeaderValue::from_str(&trans_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    response.headers_mut().insert(
        "x-openstack-request-id",
        HeaderValue::from_str(&trans_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );

    Ok(response)
}

/// Check if CORS is enabled for a container
pub async fn is_enabled(account: &str, container_name: &str, credentials: &Credentials) -> SwiftResult<bool> {
    match CorsConfig::load(account, container_name, credentials).await {
        Ok(config) => Ok(config.is_enabled()),
        Err(_) => Ok(false), // Container doesn't exist or no CORS configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cors_config_default() {
        let config = CorsConfig::default();
        assert!(!config.is_enabled());
        assert!(config.allow_origin.is_none());
        assert!(config.max_age.is_none());
        assert!(config.expose_headers.is_none());
        assert!(!config.allow_credentials);
    }

    #[test]
    fn test_cors_config_enabled() {
        let config = CorsConfig {
            allow_origin: Some("*".to_string()),
            max_age: Some(86400),
            expose_headers: None,
            allow_credentials: false,
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_cors_config_wildcard_origin() {
        let config = CorsConfig {
            allow_origin: Some("*".to_string()),
            max_age: None,
            expose_headers: None,
            allow_credentials: false,
        };

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, Some("https://example.com"));

        let origin = response.headers().get("access-control-allow-origin");
        assert_eq!(origin.unwrap().to_str().unwrap(), "*");
    }

    #[test]
    fn test_cors_config_specific_origin_match() {
        let config = CorsConfig {
            allow_origin: Some("https://example.com".to_string()),
            max_age: None,
            expose_headers: None,
            allow_credentials: false,
        };

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, Some("https://example.com"));

        let origin = response.headers().get("access-control-allow-origin");
        assert_eq!(origin.unwrap().to_str().unwrap(), "https://example.com");
    }

    #[test]
    fn test_cors_config_specific_origin_mismatch() {
        let config = CorsConfig {
            allow_origin: Some("https://example.com".to_string()),
            max_age: None,
            expose_headers: None,
            allow_credentials: false,
        };

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, Some("https://evil.com"));

        let origin = response.headers().get("access-control-allow-origin");
        assert!(origin.is_none());
    }

    #[test]
    fn test_cors_config_expose_headers() {
        let config = CorsConfig {
            allow_origin: Some("*".to_string()),
            max_age: None,
            expose_headers: Some(vec!["X-Custom-Header".to_string(), "X-Another-Header".to_string()]),
            allow_credentials: false,
        };

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, None);

        let expose = response.headers().get("access-control-expose-headers");
        assert_eq!(expose.unwrap().to_str().unwrap(), "X-Custom-Header, X-Another-Header");
    }

    #[test]
    fn test_cors_config_allow_credentials() {
        let config = CorsConfig {
            allow_origin: Some("https://example.com".to_string()),
            max_age: None,
            expose_headers: None,
            allow_credentials: true,
        };

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, Some("https://example.com"));

        let creds = response.headers().get("access-control-allow-credentials");
        assert_eq!(creds.unwrap().to_str().unwrap(), "true");
    }

    #[test]
    fn test_cors_config_disabled() {
        let config = CorsConfig::default();

        let mut response = Response::new(Body::empty());
        config.inject_headers(&mut response, Some("https://example.com"));

        // No CORS headers should be added
        assert!(response.headers().get("access-control-allow-origin").is_none());
    }

    #[test]
    fn test_parse_expose_headers_multiple() {
        let headers_str = "X-Header-1, X-Header-2, X-Header-3";
        let headers: Vec<String> = headers_str.split(',').map(|s| s.trim().to_string()).collect();

        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0], "X-Header-1");
        assert_eq!(headers[1], "X-Header-2");
        assert_eq!(headers[2], "X-Header-3");
    }

    #[test]
    fn test_parse_expose_headers_single() {
        let headers_str = "X-Single-Header";
        let headers: Vec<String> = headers_str.split(',').map(|s| s.trim().to_string()).collect();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0], "X-Single-Header");
    }
}
