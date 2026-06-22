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

use rustfs_utils::string::{has_pattern, has_string_suffix_in_slice, match_simple};
use std::env;
use std::sync::OnceLock;
use tracing::debug;

pub const MIN_DISK_COMPRESSIBLE_SIZE: usize = 4096;

// Environment variable name to control whether object disk compression is enabled.
pub const ENV_DISK_COMPRESSION_ENABLED: &str = "RUSTFS_COMPRESSION_ENABLED";

// Environment variable for file extensions to include in object disk compression.
pub const ENV_DISK_COMPRESSION_EXTENSIONS: &str = "RUSTFS_COMPRESSION_EXTENSIONS";

// Environment variable for MIME types to include in object disk compression.
pub const ENV_DISK_COMPRESSION_MIME_TYPES: &str = "RUSTFS_COMPRESSION_MIME_TYPES";

// Environment variable for additional extensions to exclude from compression (comma-separated, e.g. ".foo,.bar")
pub const ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS: &str = "RUSTFS_ADDED_EXCLUDE_COMPRESS_EXTENSIONS";

pub const DEFAULT_DISK_COMPRESS_EXTENSIONS: &str = ".txt,.log,.csv,.json,.tar,.xml,.bin";
pub const DEFAULT_DISK_COMPRESS_MIME_TYPES: &str = "text/*,application/json,application/xml,binary/octet-stream";

#[derive(Debug)]
struct DiskCompressionConfig {
    enabled: bool,
    extensions: Vec<String>,
    mime_types: Vec<String>,
    added_exclude_extensions: Vec<String>,
}

/// Parses RUSTFS_COMPRESSION_ENABLED. Called once at first use via OnceLock.
fn parse_disk_compression_enabled() -> bool {
    env::var(ENV_DISK_COMPRESSION_ENABLED)
        .map(|s| matches!(s.to_ascii_lowercase().as_str(), "true" | "on" | "1"))
        .unwrap_or(false)
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .collect()
}

fn normalize_extensions(extensions: Vec<String>) -> Vec<String> {
    extensions
        .into_iter()
        .map(|v| if v.starts_with('.') { v } else { format!(".{v}") })
        .collect()
}

fn has_config_pattern(patterns: &[String], match_str: &str) -> bool {
    patterns.iter().any(|pattern| match_simple(pattern, match_str))
}

fn has_object_content_encoding(headers: &http::HeaderMap) -> bool {
    for value in headers.get_all(http::header::CONTENT_ENCODING) {
        let Ok(content_encoding) = value.to_str() else {
            return true;
        };

        for encoding in content_encoding.split(',').map(str::trim) {
            if !encoding.is_empty() && !encoding.eq_ignore_ascii_case("identity") && !encoding.eq_ignore_ascii_case("aws-chunked")
            {
                return true;
            }
        }
    }
    false
}

fn parse_disk_compression_extensions() -> Vec<String> {
    let extensions = env::var(ENV_DISK_COMPRESSION_EXTENSIONS).unwrap_or_else(|_| DEFAULT_DISK_COMPRESS_EXTENSIONS.to_string());
    normalize_extensions(parse_csv(&extensions))
}

fn parse_disk_compression_mime_types() -> Vec<String> {
    let mime_types = env::var(ENV_DISK_COMPRESSION_MIME_TYPES).unwrap_or_else(|_| DEFAULT_DISK_COMPRESS_MIME_TYPES.to_string());
    parse_csv(&mime_types)
}

/// Parses RUSTFS_ADDED_EXCLUDE_COMPRESS_EXTENSIONS (comma-separated). Called once at first use via OnceLock.
pub(crate) fn parse_added_exclude_extensions() -> Vec<String> {
    env::var(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS)
        .ok()
        .map(|s| normalize_extensions(parse_csv(&s)))
        .unwrap_or_default()
}

fn parse_disk_compression_config() -> DiskCompressionConfig {
    DiskCompressionConfig {
        enabled: parse_disk_compression_enabled(),
        extensions: parse_disk_compression_extensions(),
        mime_types: parse_disk_compression_mime_types(),
        added_exclude_extensions: parse_added_exclude_extensions(),
    }
}

// Parsed once at first use, then reused for all disk compression checks.
static DISK_COMPRESSION_CONFIG: OnceLock<DiskCompressionConfig> = OnceLock::new();

// Some standard object extensions which we strictly dis-allow for compression.
#[rustfmt::skip]
pub const DISK_COMPRESSION_EXCLUDED_EXTENSIONS: &[&str] = &[
    // Compressed archives
    ".gz", ".bz2", ".rar", ".zip", ".7z", ".xz", ".zst", ".lz4", ".br", ".lzo", ".sz", ".tgz",
    // Images
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".heic", ".heif", ".jxl",
    // Video
    ".mp4", ".mkv", ".mov", ".avi", ".wmv", ".flv", ".webm", ".m4v", ".mpeg", ".mpg",
    // Audio
    ".mp3", ".aac", ".ogg", ".flac", ".wma", ".m4a", ".opus",
    // Documents (internally compressed)
    ".pdf", ".docx", ".xlsx", ".pptx",
    // Package formats
    ".deb", ".rpm", ".jar", ".war", ".apk",
    // Web fonts
    ".woff", ".woff2",
];

// Some standard content-types which we strictly dis-allow for compression.
pub const DISK_COMPRESSION_EXCLUDED_CONTENT_TYPES: &[&str] = &[
    "video/*",
    "audio/*",
    "image/*",
    // Archive formats (compressed)
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-zip-compressed",
    "application/x-compress",
    "application/x-spoon",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/x-bzip",
    "application/x-bzip2",
    "application/x-xz",
    "application/x-lzip",
    "application/x-lzma",
    "application/x-lzop",
    "application/zstd",
    "application/x-zstd",
    // Archive formats (uncompressed containers that are typically not further compressible)
    "application/x-tar",
    "application/tar",
    "application/pdf",
    "application/wasm",
    "font/*",
];

pub fn is_disk_compressible(headers: &http::HeaderMap, object_name: &str) -> bool {
    is_disk_compressible_with_config(headers, object_name, DISK_COMPRESSION_CONFIG.get_or_init(parse_disk_compression_config))
}

fn is_disk_compressible_with_config(headers: &http::HeaderMap, object_name: &str, config: &DiskCompressionConfig) -> bool {
    // Check if disk compression is enabled (read once at first use, then fixed for process lifetime)
    if !config.enabled {
        debug!("Disk compression is disabled by environment variable");
        return false;
    }

    let content_type = headers.get("content-type").and_then(|s| s.to_str().ok()).unwrap_or("");

    if has_object_content_encoding(headers) {
        debug!("object_name: {} is already content-encoded; skipping disk compression", object_name);
        return false;
    }

    if has_string_suffix_in_slice(object_name, DISK_COMPRESSION_EXCLUDED_EXTENSIONS) {
        debug!("object_name: {} is not disk-compressible", object_name);
        return false;
    }

    if !config.added_exclude_extensions.is_empty() && has_string_suffix_in_slice(object_name, &config.added_exclude_extensions) {
        debug!("object_name: {} is not disk-compressible (added exclusion)", object_name);
        return false;
    }

    if !content_type.is_empty() && has_pattern(DISK_COMPRESSION_EXCLUDED_CONTENT_TYPES, content_type) {
        debug!("content_type: {} is not disk-compressible", content_type);
        return false;
    }

    if config.extensions.is_empty() && config.mime_types.is_empty() {
        return true;
    }

    if !config.extensions.is_empty() && has_string_suffix_in_slice(object_name, &config.extensions) {
        return true;
    }

    if !content_type.is_empty() && !config.mime_types.is_empty() && has_config_pattern(&config.mime_types, content_type) {
        return true;
    }

    debug!("object_name: {} does not match disk compression include filters", object_name);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_disk_compression_enabled() {
        temp_env::with_var(ENV_DISK_COMPRESSION_ENABLED, Some("true"), || {
            assert!(parse_disk_compression_enabled());
        });
        temp_env::with_var(ENV_DISK_COMPRESSION_ENABLED, Some("on"), || {
            assert!(parse_disk_compression_enabled());
        });
        temp_env::with_var(ENV_DISK_COMPRESSION_ENABLED, Some("false"), || {
            assert!(!parse_disk_compression_enabled());
        });
        temp_env::with_var(ENV_DISK_COMPRESSION_ENABLED, Some("FALSE"), || {
            assert!(!parse_disk_compression_enabled());
        });
        temp_env::with_var_unset(ENV_DISK_COMPRESSION_ENABLED, || {
            assert!(!parse_disk_compression_enabled());
        });
    }

    #[test]
    fn test_parse_disk_compression_includes() {
        temp_env::with_var(ENV_DISK_COMPRESSION_EXTENSIONS, Some(".txt,log, .json"), || {
            assert_eq!(parse_disk_compression_extensions(), [".txt", ".log", ".json"]);
        });
        temp_env::with_var(ENV_DISK_COMPRESSION_MIME_TYPES, Some("text/*, application/json"), || {
            assert_eq!(parse_disk_compression_mime_types(), ["text/*", "application/json"]);
        });
        temp_env::with_var_unset(ENV_DISK_COMPRESSION_EXTENSIONS, || {
            assert_eq!(
                parse_disk_compression_extensions(),
                [".txt", ".log", ".csv", ".json", ".tar", ".xml", ".bin"]
            );
        });
        temp_env::with_var_unset(ENV_DISK_COMPRESSION_MIME_TYPES, || {
            assert_eq!(
                parse_disk_compression_mime_types(),
                ["text/*", "application/json", "application/xml", "binary/octet-stream"]
            );
        });
    }

    fn test_config() -> DiskCompressionConfig {
        DiskCompressionConfig {
            enabled: true,
            extensions: normalize_extensions(parse_csv(DEFAULT_DISK_COMPRESS_EXTENSIONS)),
            mime_types: parse_csv(DEFAULT_DISK_COMPRESS_MIME_TYPES),
            added_exclude_extensions: Vec::new(),
        }
    }

    #[test]
    fn test_is_disk_compressible() {
        use http::HeaderMap;

        let config = test_config();
        let mut headers = HeaderMap::new();
        // Test non-compressible extensions - compressed archives
        headers.insert("content-type", "text/plain".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.gz", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.zip", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.7z", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.zst", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.br", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.tgz", &config));

        // Test non-compressible extensions - images
        assert!(!is_disk_compressible_with_config(&headers, "file.jpg", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.jpeg", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.png", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.gif", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.webp", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.avif", &config));

        // Test non-compressible extensions - video
        assert!(!is_disk_compressible_with_config(&headers, "file.mp4", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.mkv", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.webm", &config));

        // Test non-compressible extensions - audio
        assert!(!is_disk_compressible_with_config(&headers, "file.mp3", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.aac", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.ogg", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.flac", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.opus", &config));

        // Test non-compressible extensions - documents
        assert!(!is_disk_compressible_with_config(&headers, "file.pdf", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.docx", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.xlsx", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.pptx", &config));

        // Test non-compressible extensions - packages
        assert!(!is_disk_compressible_with_config(&headers, "file.deb", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.rpm", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.jar", &config));

        // Test non-compressible extensions - web fonts
        assert!(!is_disk_compressible_with_config(&headers, "file.woff2", &config));

        // Test non-compressible content types
        headers.insert("content-type", "video/mp4".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "audio/mpeg".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "image/png".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "image/webp".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "application/zip".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "application/x-gzip".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "application/x-7z-compressed".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "application/pdf".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert("content-type", "font/woff2".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        // Test compressible cases
        headers.insert("content-type", "text/plain".parse().expect("valid content type"));
        assert!(is_disk_compressible_with_config(&headers, "file.txt", &config));
        assert!(is_disk_compressible_with_config(&headers, "file.log", &config));

        headers.insert("content-type", "text/html".parse().expect("valid content type"));
        assert!(is_disk_compressible_with_config(&headers, "file.html", &config));

        headers.insert("content-type", "application/json".parse().expect("valid content type"));
        assert!(is_disk_compressible_with_config(&headers, "file.json", &config));

        headers.insert("content-type", "application/octet-stream".parse().expect("valid content type"));
        assert!(!is_disk_compressible_with_config(&headers, "file.data", &config));
    }

    #[test]
    fn test_content_encoding_skips_disk_compression() {
        use http::{HeaderMap, HeaderValue};

        let config = test_config();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().expect("valid content type"));

        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("zstd"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked,gzip"));
        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked"));
        assert!(is_disk_compressible_with_config(&headers, "file.txt", &config));

        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("identity"));
        assert!(is_disk_compressible_with_config(&headers, "file.txt", &config));
    }

    #[test]
    fn test_added_exclude_compress_extensions_parsing() {
        temp_env::with_var(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS, Some(".foo,.bar"), || {
            let added = parse_added_exclude_extensions();
            assert_eq!(added, [".foo", ".bar"]);
        });
        temp_env::with_var(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS, Some("baz, .qux"), || {
            let added = parse_added_exclude_extensions();
            assert_eq!(added, [".baz", ".qux"]);
        });
        temp_env::with_var_unset(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS, || {
            let added = parse_added_exclude_extensions();
            assert!(added.is_empty());
        });
    }

    #[test]
    fn test_added_exclude_compress_extensions_excludes_included_extension() {
        use http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().expect("valid content type"));
        let mut config = test_config();
        config.added_exclude_extensions = vec![".txt".to_string()];

        assert!(!is_disk_compressible_with_config(&headers, "file.txt", &config));
        assert!(is_disk_compressible_with_config(&headers, "file.log", &config));
    }

    #[test]
    fn test_empty_includes_compress_everything_except_exclusions() {
        use http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/octet-stream".parse().expect("valid content type"));
        let config = DiskCompressionConfig {
            enabled: true,
            extensions: Vec::new(),
            mime_types: Vec::new(),
            added_exclude_extensions: Vec::new(),
        };

        assert!(is_disk_compressible_with_config(&headers, "file.data", &config));
        assert!(!is_disk_compressible_with_config(&headers, "file.zip", &config));
    }
}
