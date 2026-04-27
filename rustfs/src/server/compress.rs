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

//! HTTP Response Compression Module
//!
//! This module provides configurable HTTP response compression functionality
//! using a whitelist-based approach. Unlike traditional blacklist approaches,
//! this design only compresses explicitly configured content types, which:
//!
//! 1. Preserves Content-Length for all other responses (better browser UX)
//! 2. Aligns with MinIO's opt-in compression behavior
//! 3. Provides fine-grained control over what gets compressed
//!
//! # Configuration
//!
//! Compression can be configured via environment variables or command line options:
//!
//! - `RUSTFS_COMPRESS_ENABLE` - Enable/disable compression (default: off)
//! - `RUSTFS_COMPRESS_EXTENSIONS` - File extensions to compress (e.g., `.txt,.log,.csv`)
//! - `RUSTFS_COMPRESS_MIME_TYPES` - MIME types to compress (e.g., `text/*,application/json`)
//! - `RUSTFS_COMPRESS_MIN_SIZE` - Minimum file size for compression (default: 1000 bytes)
//!
//! # Example
//!
//! ```bash
//! RUSTFS_COMPRESS_ENABLE=on \
//! RUSTFS_COMPRESS_EXTENSIONS=.txt,.log,.csv \
//! RUSTFS_COMPRESS_MIME_TYPES=text/*,application/json \
//! RUSTFS_COMPRESS_MIN_SIZE=1000 \
//! rustfs /data
//! ```

use http::Response;
use rustfs_config::{
    DEFAULT_COMPRESS_ENABLE, DEFAULT_COMPRESS_EXTENSIONS, DEFAULT_COMPRESS_MIME_TYPES, DEFAULT_COMPRESS_MIN_SIZE,
    ENV_COMPRESS_ENABLE, ENV_COMPRESS_EXTENSIONS, ENV_COMPRESS_MIME_TYPES, ENV_COMPRESS_MIN_SIZE, EnableState,
};
use rustfs_ecstore::compress::{STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES, STANDARD_EXCLUDE_COMPRESS_EXTENSIONS};
use rustfs_utils::string::{has_pattern, has_string_suffix_in_slice};
use std::str::FromStr;
use tower_http::compression::predicate::Predicate;
use tracing::debug;

/// Response extension key for storing the request path category.
/// Set by `PathCategoryInjectionLayer` before the compression predicate evaluates.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestPathCategory(pub(crate) PathCategory);

/// Configuration for HTTP response compression.
///
/// This structure holds the whitelist-based compression settings:
/// - File extensions that should be compressed (checked via Content-Disposition header)
/// - MIME types that should be compressed (supports wildcards like `text/*`)
/// - Minimum file size threshold for compression
///
/// When compression is enabled, only responses matching these criteria will be compressed.
/// This approach aligns with MinIO's behavior where compression is opt-in rather than default.
#[derive(Clone, Debug)]
pub struct CompressionConfig {
    /// Whether compression is enabled
    pub enabled: bool,
    /// File extensions to compress (normalized to lowercase with leading dot)
    pub extensions: Vec<String>,
    /// MIME type patterns to compress (supports wildcards like `text/*`)
    pub mime_patterns: Vec<String>,
    /// Minimum file size (in bytes) for compression
    pub min_size: u64,
}

impl CompressionConfig {
    /// Create a new compression configuration from environment variables
    ///
    /// Reads the following environment variables:
    /// - `RUSTFS_COMPRESS_ENABLE` - Enable/disable compression (default: false)
    /// - `RUSTFS_COMPRESS_EXTENSIONS` - File extensions to compress (default: "")
    /// - `RUSTFS_COMPRESS_MIME_TYPES` - MIME types to compress (default: "text/*,application/json,...")
    /// - `RUSTFS_COMPRESS_MIN_SIZE` - Minimum file size for compression (default: 1000)
    pub fn from_env() -> Self {
        // Read compression enable state
        let enabled = std::env::var(ENV_COMPRESS_ENABLE)
            .ok()
            .and_then(|v| EnableState::from_str(&v).ok())
            .map(|state| state.is_enabled())
            .unwrap_or(DEFAULT_COMPRESS_ENABLE);

        // Read file extensions
        let extensions_str = std::env::var(ENV_COMPRESS_EXTENSIONS).unwrap_or_else(|_| DEFAULT_COMPRESS_EXTENSIONS.to_string());
        let extensions: Vec<String> = if extensions_str.is_empty() {
            Vec::new()
        } else {
            extensions_str
                .split(',')
                .map(|s| {
                    let s = s.trim().to_lowercase();
                    if s.starts_with('.') { s } else { format!(".{s}") }
                })
                .filter(|s| s.len() > 1)
                .collect()
        };

        // Read MIME type patterns
        let mime_types_str = std::env::var(ENV_COMPRESS_MIME_TYPES).unwrap_or_else(|_| DEFAULT_COMPRESS_MIME_TYPES.to_string());
        let mime_patterns: Vec<String> = if mime_types_str.is_empty() {
            Vec::new()
        } else {
            mime_types_str
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        };

        // Read minimum file size
        let min_size = std::env::var(ENV_COMPRESS_MIN_SIZE)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_COMPRESS_MIN_SIZE);

        Self {
            enabled,
            extensions,
            mime_patterns,
            min_size,
        }
    }

    /// Check if a MIME type matches any of the configured patterns
    pub(crate) fn matches_mime_type(&self, content_type: &str) -> bool {
        let ct_lower = content_type.to_lowercase();
        // Extract the main MIME type (before any parameters like charset)
        let main_type = ct_lower.split(';').next().unwrap_or(&ct_lower).trim();

        for pattern in &self.mime_patterns {
            if pattern.ends_with("/*") {
                // Wildcard pattern like "text/*"
                let prefix = &pattern[..pattern.len() - 1]; // "text/"
                if main_type.starts_with(prefix) {
                    return true;
                }
            } else if main_type == pattern {
                // Exact match
                return true;
            }
        }
        false
    }

    /// Check if a filename matches any of the configured extensions
    /// The filename is extracted from Content-Disposition header
    pub(crate) fn matches_extension(&self, filename: &str) -> bool {
        if self.extensions.is_empty() {
            return false;
        }

        let filename_lower = filename.to_lowercase();
        for ext in &self.extensions {
            if filename_lower.ends_with(ext) {
                return true;
            }
        }
        false
    }

    /// Extract filename from Content-Disposition header
    /// Format: attachment; filename="example.txt" or attachment; filename=example.txt
    pub(crate) fn extract_filename_from_content_disposition(header_value: &str) -> Option<String> {
        // Look for filename= or filename*= parameter
        let lower = header_value.to_lowercase();

        // Try to find filename="..." or filename=...
        if let Some(idx) = lower.find("filename=") {
            let start = idx + "filename=".len();
            let rest = &header_value[start..];

            // Check if it's quoted
            if let Some(stripped) = rest.strip_prefix('"') {
                // Find closing quote
                if let Some(end_quote) = stripped.find('"') {
                    return Some(stripped[..end_quote].to_string());
                }
            } else {
                // Unquoted - take until semicolon or end
                let end = rest.find(';').unwrap_or(rest.len());
                return Some(rest[..end].trim().to_string());
            }
        }

        None
    }

    pub(crate) fn is_excluded_filename(filename: &str) -> bool {
        has_string_suffix_in_slice(&filename.to_ascii_lowercase(), STANDARD_EXCLUDE_COMPRESS_EXTENSIONS)
    }

    pub(crate) fn is_excluded_mime_type(content_type: &str) -> bool {
        let main_type = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim()
            .to_ascii_lowercase();
        !main_type.is_empty() && has_pattern(STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES, &main_type)
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: rustfs_config::DEFAULT_COMPRESS_ENABLE,
            extensions: rustfs_config::DEFAULT_COMPRESS_EXTENSIONS
                .split(',')
                .filter_map(|s| {
                    let s = s.trim().to_lowercase();
                    if s.is_empty() {
                        None
                    } else if s.starts_with('.') {
                        Some(s)
                    } else {
                        Some(format!(".{s}"))
                    }
                })
                .collect(),
            mime_patterns: rustfs_config::DEFAULT_COMPRESS_MIME_TYPES
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect(),
            min_size: rustfs_config::DEFAULT_COMPRESS_MIN_SIZE,
        }
    }
}

/// Predicate to determine if a response should be compressed.
///
/// This predicate implements a whitelist-based compression approach:
/// - Only compresses responses that match configured file extensions OR MIME types
/// - Respects minimum file size threshold
/// - Always skips error responses (4xx, 5xx) to avoid Content-Length issues
///
/// # Design Philosophy
/// Unlike the previous blacklist approach, this whitelist approach:
/// 1. Only compresses explicitly configured content types
/// 2. Preserves Content-Length for all other responses (better browser UX)
/// 3. Aligns with MinIO's opt-in compression behavior
///
/// # Note on tower-http Integration
/// The `tower-http::CompressionLayer` automatically handles:
/// - Skipping responses with `Content-Encoding` header (already compressed)
/// - Skipping responses with `Content-Range` header (Range requests)
///
/// These checks are performed before calling this predicate, so we don't need to check them here.
///
/// # Extension Matching
/// File extension matching works by extracting the filename from the
/// `Content-Disposition` response header (e.g., `attachment; filename="file.txt"`).
///
/// # Performance
/// This predicate is evaluated per-response and has O(n) complexity where n is
/// the number of configured extensions/MIME patterns.
#[derive(Clone, Debug)]
pub struct CompressionPredicate {
    config: CompressionConfig,
}

impl CompressionPredicate {
    /// Create a new compression predicate with the given configuration
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }
}

impl Predicate for CompressionPredicate {
    fn should_compress<B>(&self, response: &Response<B>) -> bool
    where
        B: http_body::Body,
    {
        // If compression is disabled, never compress
        if !self.config.enabled {
            return false;
        }

        let status = response.status();

        // Never compress error responses (4xx and 5xx status codes)
        // This prevents Content-Length mismatch issues with error responses
        if status.is_client_error() || status.is_server_error() {
            debug!("Skipping compression for error response: status={}", status.as_u16());
            return false;
        }

        // Note: CONTENT_ENCODING and CONTENT_RANGE checks are handled by tower-http's
        // CompressionLayer before calling this predicate, so we don't need to check them here.

        // Check Content-Length header for minimum size threshold
        if let Some(content_length) = response.headers().get(http::header::CONTENT_LENGTH)
            && let Ok(length_str) = content_length.to_str()
            && let Ok(length) = length_str.parse::<u64>()
            && length < self.config.min_size
        {
            debug!(
                "Skipping compression for small response: size={} bytes, min_size={}",
                length, self.config.min_size
            );
            return false;
        }

        // Hard-stop archive/media/package MIME types even if the whitelist matches.
        // This includes tar, gzip, bzip2, xz, zstd, zip, rar, 7z, lzip, lzma, lzop variants,
        // plus video/*, audio/*, image/*, font/*, application/pdf, and application/wasm.
        if let Some(content_type) = response.headers().get(http::header::CONTENT_TYPE)
            && let Ok(ct) = content_type.to_str()
        {
            if CompressionConfig::is_excluded_mime_type(ct) {
                debug!("Skipping compression for excluded Content-Type '{}'", ct);
                return false;
            }

            if self.config.matches_mime_type(ct) {
                debug!("Compressing response: Content-Type '{}' matches configured MIME pattern", ct);
                return true;
            }
        }

        // Hard-stop archive-like attachment downloads even if the whitelist matches.
        if let Some(content_disposition) = response.headers().get(http::header::CONTENT_DISPOSITION)
            && let Ok(cd) = content_disposition.to_str()
            && let Some(filename) = CompressionConfig::extract_filename_from_content_disposition(cd)
        {
            if CompressionConfig::is_excluded_filename(&filename) {
                debug!("Skipping compression for excluded filename '{}'", filename);
                return false;
            }

            if self.config.matches_extension(&filename) {
                debug!("Compressing response: filename '{}' matches configured extension", filename);
                return true;
            }
        }

        // Default: don't compress (whitelist approach)
        debug!("Skipping compression: response does not match any configured extension or MIME type");
        false
    }
}

// ── Path-Aware Compression ──

/// Classifies request paths to determine if compression should apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathCategory {
    /// S3 data plane (bucket/key operations) — compression applies via whitelist
    S3DataPlane,
    /// Admin API paths — skip compression (small JSON responses)
    AdminApi,
    /// Console paths — skip compression (static assets, already optimized)
    Console,
    /// Internode RPC paths — skip compression (binary protocol data)
    InternodeRpc,
    /// Health/probe paths — skip compression (tiny responses)
    Probe,
}

impl PathCategory {
    /// Classify a request URI path into a category.
    pub(crate) fn classify(path: &str) -> Self {
        if path.starts_with("/rustfs/rpc/") || path.starts_with("/rustfs/peer/") {
            PathCategory::InternodeRpc
        } else if path.starts_with("/rustfs/admin/") || path.starts_with("/minio/admin/") {
            PathCategory::AdminApi
        } else if path.starts_with("/rustfs/console") {
            PathCategory::Console
        } else if path.starts_with("/minio/health/") {
            PathCategory::Probe
        } else {
            PathCategory::S3DataPlane
        }
    }

    /// Returns true if compression should be considered for this path category.
    /// Only S3 data plane paths go through the full compression predicate.
    #[inline]
    pub(crate) fn should_evaluate_compression(self) -> bool {
        matches!(self, PathCategory::S3DataPlane)
    }
}

/// A compression predicate that first checks the request path category
/// before evaluating the full compression rules.
///
/// This avoids running MIME type / extension matching for admin, RPC, console,
/// and health probe paths where compression is never beneficial.
#[derive(Clone, Debug)]
pub(crate) struct PathAwareCompressionPredicate {
    inner: CompressionPredicate,
}

impl PathAwareCompressionPredicate {
    pub(crate) fn new(config: CompressionConfig) -> Self {
        Self {
            inner: CompressionPredicate::new(config),
        }
    }
}

impl Predicate for PathAwareCompressionPredicate {
    fn should_compress<B>(&self, response: &Response<B>) -> bool
    where
        B: http_body::Body,
    {
        // Fast path: skip full predicate evaluation for non-S3 paths
        if let Some(RequestPathCategory(category)) = response.extensions().get::<RequestPathCategory>()
            && !category.should_evaluate_compression()
        {
            return false;
        }
        self.inner.should_compress(response)
    }
}

use http::Request;
use http_body::Body;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Tower layer that injects `RequestPathCategory` into each response's extensions
/// based on the incoming request URI path.
///
/// It must be placed inside `CompressionLayer` so the category is available when
/// the outer compression middleware evaluates its response predicate. With
/// `tower::ServiceBuilder`, that means adding this layer after `CompressionLayer`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PathCategoryInjectionLayer;

impl<S> Layer<S> for PathCategoryInjectionLayer {
    type Service = PathCategoryInjectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PathCategoryInjectionService { inner }
    }
}

/// Service wrapper that adds `RequestPathCategory` to response extensions.
#[derive(Clone)]
pub(crate) struct PathCategoryInjectionService<S> {
    inner: S,
}

pin_project_lite::pin_project! {
    /// Future for `PathCategoryInjectionService` that injects path category into response.
    #[project = InjectCategoryFutProj]
    pub(crate) struct InjectCategoryFut<F> {
        #[pin]
        inner: F,
        category: PathCategory,
    }
}

impl<F, ResBody, E> std::future::Future for InjectCategoryFut<F>
where
    F: std::future::Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(Ok(mut resp)) => {
                resp.extensions_mut().insert(RequestPathCategory(*this.category));
                Poll::Ready(Ok(resp))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for PathCategoryInjectionService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Body,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = InjectCategoryFut<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let category = PathCategory::classify(req.uri().path());
        InjectCategoryFut {
            inner: self.inner.call(req),
            category,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert!(!config.enabled);
        assert!(config.extensions.is_empty());
        assert!(!config.mime_patterns.is_empty());
        assert_eq!(config.min_size, 1000);
    }

    #[test]
    fn test_compression_config_mime_matching() {
        let config = CompressionConfig {
            enabled: true,
            extensions: vec![],
            mime_patterns: vec!["text/*".to_string(), "application/json".to_string()],
            min_size: 1000,
        };

        // Test wildcard matching
        assert!(config.matches_mime_type("text/plain"));
        assert!(config.matches_mime_type("text/html"));
        assert!(config.matches_mime_type("text/css"));
        assert!(config.matches_mime_type("TEXT/PLAIN")); // case insensitive

        // Test exact matching
        assert!(config.matches_mime_type("application/json"));
        assert!(config.matches_mime_type("application/json; charset=utf-8"));

        // Test non-matching types
        assert!(!config.matches_mime_type("image/png"));
        assert!(!config.matches_mime_type("application/octet-stream"));
        assert!(!config.matches_mime_type("video/mp4"));
    }

    #[test]
    fn test_compression_config_extension_matching() {
        let config = CompressionConfig {
            enabled: true,
            extensions: vec![".txt".to_string(), ".log".to_string(), ".csv".to_string()],
            mime_patterns: vec![],
            min_size: 1000,
        };

        // Test matching extensions
        assert!(config.matches_extension("file.txt"));
        assert!(config.matches_extension("path/to/file.log"));
        assert!(config.matches_extension("data.csv"));
        assert!(config.matches_extension("FILE.TXT")); // case insensitive

        // Test non-matching extensions
        assert!(!config.matches_extension("image.png"));
        assert!(!config.matches_extension("archive.zip"));
        assert!(!config.matches_extension("document.pdf"));
    }

    #[test]
    fn test_extract_filename_from_content_disposition() {
        // Quoted filename
        assert_eq!(
            CompressionConfig::extract_filename_from_content_disposition(r#"attachment; filename="example.txt""#),
            Some("example.txt".to_string())
        );

        // Unquoted filename
        assert_eq!(
            CompressionConfig::extract_filename_from_content_disposition("attachment; filename=example.log"),
            Some("example.log".to_string())
        );

        // Filename with path
        assert_eq!(
            CompressionConfig::extract_filename_from_content_disposition(r#"attachment; filename="path/to/file.csv""#),
            Some("path/to/file.csv".to_string())
        );

        // Mixed case
        assert_eq!(
            CompressionConfig::extract_filename_from_content_disposition(r#"Attachment; FILENAME="test.json""#),
            Some("test.json".to_string())
        );

        // No filename
        assert_eq!(CompressionConfig::extract_filename_from_content_disposition("inline"), None);
    }

    #[test]
    fn test_compression_config_from_empty_strings() {
        // Simulate config with empty extension and mime strings
        let config = CompressionConfig {
            enabled: true,
            extensions: ""
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect(),
            mime_patterns: ""
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect(),
            min_size: 1000,
        };

        assert!(config.extensions.is_empty());
        assert!(config.mime_patterns.is_empty());
        assert!(!config.matches_extension("file.txt"));
        assert!(!config.matches_mime_type("text/plain"));
    }

    #[test]
    fn test_compression_config_extension_normalization() {
        // Extensions should be normalized with leading dot
        let extensions: Vec<String> = "txt,.log,csv"
            .split(',')
            .map(|s| {
                let s = s.trim().to_lowercase();
                if s.starts_with('.') { s } else { format!(".{s}") }
            })
            .filter(|s| s.len() > 1)
            .collect();

        assert_eq!(extensions, vec![".txt", ".log", ".csv"]);
    }

    #[test]
    fn test_compression_predicate_creation() {
        // Test that CompressionPredicate can be created with various configs
        let config_disabled = CompressionConfig {
            enabled: false,
            extensions: vec![".txt".to_string()],
            mime_patterns: vec!["text/*".to_string()],
            min_size: 0,
        };
        let predicate = CompressionPredicate::new(config_disabled);
        assert!(!predicate.config.enabled);

        let config_enabled = CompressionConfig {
            enabled: true,
            extensions: vec![".txt".to_string(), ".log".to_string()],
            mime_patterns: vec!["text/*".to_string(), "application/json".to_string()],
            min_size: 1000,
        };
        let predicate = CompressionPredicate::new(config_enabled);
        assert!(predicate.config.enabled);
        assert_eq!(predicate.config.extensions.len(), 2);
        assert_eq!(predicate.config.mime_patterns.len(), 2);
        assert_eq!(predicate.config.min_size, 1000);
    }

    #[test]
    fn test_compression_predicate_skips_archive_mime_type_even_when_whitelisted() {
        let predicate = CompressionPredicate::new(CompressionConfig {
            enabled: true,
            extensions: vec![],
            mime_patterns: vec!["application/zip".to_string()],
            min_size: 0,
        });

        let response = Response::builder()
            .header(http::header::CONTENT_TYPE, "application/zip")
            .header(http::header::CONTENT_LENGTH, "4096")
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .expect("response");

        assert!(!predicate.should_compress(&response));
    }

    #[test]
    fn test_compression_predicate_skips_archive_filename_even_when_whitelisted() {
        let predicate = CompressionPredicate::new(CompressionConfig {
            enabled: true,
            extensions: vec![".zip".to_string()],
            mime_patterns: vec![],
            min_size: 0,
        });

        let response = Response::builder()
            .header(http::header::CONTENT_DISPOSITION, r#"attachment; filename="bundle.zip""#)
            .header(http::header::CONTENT_LENGTH, "4096")
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .expect("response");

        assert!(!predicate.should_compress(&response));
    }

    #[test]
    fn test_path_category_classify_s3() {
        assert_eq!(PathCategory::classify("/"), PathCategory::S3DataPlane);
        assert_eq!(PathCategory::classify("/mybucket"), PathCategory::S3DataPlane);
        assert_eq!(PathCategory::classify("/mybucket/mykey"), PathCategory::S3DataPlane);
        assert_eq!(PathCategory::classify("/bucket?list-type=2"), PathCategory::S3DataPlane);
    }

    #[test]
    fn test_path_category_classify_admin() {
        assert_eq!(PathCategory::classify("/rustfs/admin/v3/service"), PathCategory::AdminApi);
        assert_eq!(PathCategory::classify("/minio/admin/v3/info"), PathCategory::AdminApi);
    }

    #[test]
    fn test_path_category_classify_console() {
        assert_eq!(PathCategory::classify("/rustfs/console/index.html"), PathCategory::Console);
        assert_eq!(PathCategory::classify("/rustfs/console"), PathCategory::Console);
    }

    #[test]
    fn test_path_category_classify_rpc() {
        assert_eq!(PathCategory::classify("/rustfs/rpc/read_file_stream"), PathCategory::InternodeRpc);
        assert_eq!(PathCategory::classify("/rustfs/peer/health"), PathCategory::InternodeRpc);
    }

    #[test]
    fn test_path_category_classify_probe() {
        assert_eq!(PathCategory::classify("/minio/health/live"), PathCategory::Probe);
        assert_eq!(PathCategory::classify("/minio/health/ready"), PathCategory::Probe);
    }

    #[test]
    fn test_path_category_should_evaluate() {
        assert!(PathCategory::S3DataPlane.should_evaluate_compression());
        assert!(!PathCategory::AdminApi.should_evaluate_compression());
        assert!(!PathCategory::Console.should_evaluate_compression());
        assert!(!PathCategory::InternodeRpc.should_evaluate_compression());
        assert!(!PathCategory::Probe.should_evaluate_compression());
    }
}
