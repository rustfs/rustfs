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

//! ETag normalization layer for handling If-Match and If-None-Match headers.
//!
//! This module provides a Tower layer that normalizes ETag values in HTTP headers
//! to ensure they conform to the HTTP spec format expected by s3s.
//!
//! The HTTP spec (RFC 9110) requires ETags to be quoted (e.g., `"abc123"` or `W/"abc123"`),
//! but some S3 clients send unquoted ETags. This layer normalizes such headers to ensure
//! proper parsing by the s3s library.

use http::{HeaderValue, Request as HttpRequest, header::HeaderName};
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// Header names for ETag-related conditional headers
static IF_MATCH: HeaderName = HeaderName::from_static("if-match");
static IF_NONE_MATCH: HeaderName = HeaderName::from_static("if-none-match");
static X_AMZ_COPY_SOURCE_IF_MATCH: HeaderName = HeaderName::from_static("x-amz-copy-source-if-match");
static X_AMZ_COPY_SOURCE_IF_NONE_MATCH: HeaderName = HeaderName::from_static("x-amz-copy-source-if-none-match");

/// Normalizes an ETag value to ensure it has proper quoting.
///
/// This function handles the following cases:
/// - `*` (wildcard) - returned as-is
/// - `"value"` (already quoted) - returned as-is
/// - `W/"value"` (weak ETag) - returned as-is
/// - `value` (unquoted) - wrapped in quotes to become `"value"`
///
/// # Arguments
/// * `value` - The raw ETag header value as bytes
///
/// # Returns
/// * `Some(HeaderValue)` - The normalized header value if normalization was needed
/// * `None` - If the value is already properly formatted or couldn't be parsed
pub fn normalize_etag_value(value: &[u8]) -> Option<HeaderValue> {
    // Empty value - nothing to normalize
    if value.is_empty() {
        return None;
    }

    // Wildcard - already valid, no normalization needed
    if value == b"*" {
        return None;
    }

    // Already quoted (strong ETag) - no normalization needed
    if value.starts_with(b"\"") && value.ends_with(b"\"") && value.len() >= 2 {
        return None;
    }

    // Weak ETag (W/"value") - no normalization needed
    if value.starts_with(b"W/\"") && value.ends_with(b"\"") && value.len() >= 4 {
        return None;
    }

    // Unquoted value - wrap in quotes
    let mut quoted = Vec::with_capacity(value.len() + 2);
    quoted.push(b'"');
    quoted.extend_from_slice(value);
    quoted.push(b'"');

    HeaderValue::from_bytes(&quoted).ok()
}

/// Tower layer that normalizes ETag headers in incoming requests.
///
/// This layer intercepts HTTP requests and normalizes the following headers:
/// - `If-Match`
/// - `If-None-Match`
/// - `x-amz-copy-source-if-match`
/// - `x-amz-copy-source-if-none-match`
///
/// Unquoted ETag values are wrapped in double quotes to conform to RFC 9110.
#[derive(Clone, Copy, Debug, Default)]
pub struct ETagNormalizeLayer;

impl ETagNormalizeLayer {
    /// Creates a new `ETagNormalizeLayer`.
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for ETagNormalizeLayer {
    type Service = ETagNormalizeService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ETagNormalizeService { inner }
    }
}

/// Service that normalizes ETag headers.
#[derive(Clone, Debug)]
pub struct ETagNormalizeService<S> {
    inner: S,
}

impl<S, B> Service<HttpRequest<B>> for ETagNormalizeService<S>
where
    S: Service<HttpRequest<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<B>) -> Self::Future {
        // Normalize ETag headers
        normalize_request_headers(req.headers_mut());

        self.inner.call(req)
    }
}

/// Normalizes ETag-related headers in the request.
fn normalize_request_headers(headers: &mut http::HeaderMap) {
    // List of headers to normalize
    let headers_to_check = [
        &IF_MATCH,
        &IF_NONE_MATCH,
        &X_AMZ_COPY_SOURCE_IF_MATCH,
        &X_AMZ_COPY_SOURCE_IF_NONE_MATCH,
    ];

    for header_name in headers_to_check {
        if let Some(value) = headers.get(header_name) {
            if let Some(normalized) = normalize_etag_value(value.as_bytes()) {
                debug!(
                    header = %header_name,
                    original = ?value,
                    normalized = ?normalized,
                    "Normalized ETag header value"
                );
                headers.insert(header_name.clone(), normalized);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_etag_wildcard() {
        // Wildcard should not be normalized
        assert!(normalize_etag_value(b"*").is_none());
    }

    #[test]
    fn test_normalize_etag_already_quoted() {
        // Already quoted should not be normalized
        assert!(normalize_etag_value(b"\"abc123\"").is_none());
        assert!(normalize_etag_value(b"\"143f7531a3558678c43d9e411c5c5d12\"").is_none());
    }

    #[test]
    fn test_normalize_etag_weak() {
        // Weak ETag should not be normalized
        assert!(normalize_etag_value(b"W/\"abc123\"").is_none());
    }

    #[test]
    fn test_normalize_etag_unquoted() {
        // Unquoted should be wrapped in quotes
        let result = normalize_etag_value(b"abc123");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_bytes(), b"\"abc123\"");

        let result = normalize_etag_value(b"143f7531a3558678c43d9e411c5c5d12");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_bytes(), b"\"143f7531a3558678c43d9e411c5c5d12\"");
    }

    #[test]
    fn test_normalize_etag_empty() {
        // Empty should not be normalized
        assert!(normalize_etag_value(b"").is_none());
    }

    #[test]
    fn test_normalize_etag_partial_quote_start() {
        // Starts with quote but doesn't end - should be normalized
        let result = normalize_etag_value(b"\"abc123");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_bytes(), b"\"\"abc123\"");
    }

    #[test]
    fn test_normalize_etag_partial_quote_end() {
        // Ends with quote but doesn't start - should be normalized
        let result = normalize_etag_value(b"abc123\"");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_bytes(), b"\"abc123\"\"");
    }

    #[test]
    fn test_normalize_request_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            HeaderName::from_static("if-match"),
            HeaderValue::from_static("abc123"),
        );
        headers.insert(
            HeaderName::from_static("if-none-match"),
            HeaderValue::from_static("\"already-quoted\""),
        );

        normalize_request_headers(&mut headers);

        // if-match should be normalized
        assert_eq!(
            headers.get("if-match").unwrap().as_bytes(),
            b"\"abc123\""
        );

        // if-none-match should remain unchanged
        assert_eq!(
            headers.get("if-none-match").unwrap().as_bytes(),
            b"\"already-quoted\""
        );
    }
}
