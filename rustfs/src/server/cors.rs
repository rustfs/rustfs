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

//! CORS (Cross-Origin Resource Sharing) headers and request inputs.
//!
//! This module provides centralized constants for CORS-related HTTP header names.
//! The http crate doesn't provide pre-defined constants for CORS headers,
//! so we define them here for type safety and maintainability.

/// CORS response header names
pub mod response {
    pub const ACCESS_CONTROL_ALLOW_ORIGIN: &str = "access-control-allow-origin";
    pub const ACCESS_CONTROL_ALLOW_METHODS: &str = "access-control-allow-methods";
    pub const ACCESS_CONTROL_ALLOW_HEADERS: &str = "access-control-allow-headers";
    pub const ACCESS_CONTROL_EXPOSE_HEADERS: &str = "access-control-expose-headers";
    pub const ACCESS_CONTROL_ALLOW_CREDENTIALS: &str = "access-control-allow-credentials";
    pub const ACCESS_CONTROL_MAX_AGE: &str = "access-control-max-age";
}

/// CORS request header names
pub mod request {
    pub const ACCESS_CONTROL_REQUEST_METHOD: &str = "access-control-request-method";
    pub const ACCESS_CONTROL_REQUEST_HEADERS: &str = "access-control-request-headers";
}

/// Standard HTTP header names used in CORS processing
pub mod standard {
    pub use http::header::{ORIGIN, VARY};
}

/// Only the inputs used by CORS policy evaluation. Credential payloads and
/// unrelated object metadata need not outlive the inner request.
pub(crate) struct CorsRequest {
    pub(crate) origin: Option<http::HeaderValue>,
    pub(crate) requested_method: Option<http::HeaderValue>,
    pub(crate) requested_headers: Option<http::HeaderValue>,
    pub(crate) credentialed: bool,
}

impl CorsRequest {
    pub(crate) fn from_headers(headers: &http::HeaderMap) -> Self {
        Self {
            origin: headers.get(standard::ORIGIN).cloned(),
            requested_method: headers.get(request::ACCESS_CONTROL_REQUEST_METHOD).cloned(),
            requested_headers: headers.get(request::ACCESS_CONTROL_REQUEST_HEADERS).cloned(),
            credentialed: headers.contains_key(http::header::AUTHORIZATION)
                || headers.contains_key(http::header::COOKIE)
                || headers.contains_key("x-amz-security-token")
                || headers.contains_key("x-amz-content-sha256"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CorsRequest;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn cors_request_captures_first_value_without_mutating_duplicate_headers() {
        let mut headers = HeaderMap::new();
        for (name, first, second) in [
            ("origin", "https://first.example", "https://second.example"),
            ("access-control-request-method", "PUT", "DELETE"),
            ("access-control-request-headers", "x-first", "x-second"),
        ] {
            headers.append(name, HeaderValue::from_static(first));
            headers.append(name, HeaderValue::from_static(second));
        }
        let original = headers.clone();

        let request = CorsRequest::from_headers(&headers);

        assert_eq!(request.origin, Some(HeaderValue::from_static("https://first.example")));
        assert_eq!(request.requested_method, Some(HeaderValue::from_static("PUT")));
        assert_eq!(request.requested_headers, Some(HeaderValue::from_static("x-first")));
        assert!(!request.credentialed);
        assert_eq!(headers, original);
    }

    #[test]
    fn cors_request_preserves_opaque_values_separately_from_absence() {
        let mut headers = HeaderMap::new();
        let opaque = HeaderValue::from_bytes(&[0x80]).expect("opaque HTTP header value");
        assert!(opaque.to_str().is_err());
        for name in ["origin", "access-control-request-method", "access-control-request-headers"] {
            headers.insert(name, opaque.clone());
        }
        let original = headers.clone();

        let request = CorsRequest::from_headers(&headers);

        assert_eq!(request.origin, Some(opaque.clone()));
        assert_eq!(request.requested_method, Some(opaque.clone()));
        assert_eq!(request.requested_headers, Some(opaque));
        assert!(!request.credentialed);
        assert_eq!(headers, original);
    }

    #[test]
    fn cors_request_detects_each_credential_header_by_presence() {
        for name in ["authorization", "cookie", "x-amz-security-token", "x-amz-content-sha256"] {
            for value in ["", "present"] {
                let mut headers = HeaderMap::new();
                headers.insert(name, HeaderValue::from_static(value));
                let original = headers.clone();

                let request = CorsRequest::from_headers(&headers);

                assert!(request.credentialed, "{name} with value {value:?}");
                assert!(request.origin.is_none());
                assert!(request.requested_method.is_none());
                assert!(request.requested_headers.is_none());
                assert_eq!(headers, original);
            }
        }
    }

    #[test]
    fn cors_request_ignores_unrelated_headers_and_preserves_the_request() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-large", HeaderValue::from_str(&"a".repeat(16 * 1024)).expect("large metadata"));
        for name in [
            "proxy-authorization",
            "x-amz-date",
            "x-amz-meta-authorization",
            "x-amz-security-token-extra",
        ] {
            headers.insert(name, HeaderValue::from_static("present"));
        }
        let original = headers.clone();

        let request = CorsRequest::from_headers(&headers);

        assert!(request.origin.is_none());
        assert!(request.requested_method.is_none());
        assert!(request.requested_headers.is_none());
        assert!(!request.credentialed);
        assert_eq!(headers, original);
    }
}
