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

use rustfs_utils::string::{has_pattern, has_string_suffix_in_slice};
use std::env;
use std::sync::OnceLock;
use tracing::debug;

pub const MIN_COMPRESSIBLE_SIZE: usize = 4096;

// Environment variable name to control whether compression is enabled
pub const ENV_COMPRESSION_ENABLED: &str = "RUSTFS_COMPRESSION_ENABLED";

// Environment variable for additional extensions to exclude from compression (comma-separated, e.g. ".foo,.bar")
pub const ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS: &str = "RUSTFS_ADDED_EXCLUDE_COMPRESS_EXTENSIONS";

/// Parses RUSTFS_ADDED_EXCLUDE_COMPRESS_EXTENSIONS (comma-separated). Called once at first use via OnceLock.
pub(crate) fn parse_added_exclude_extensions() -> Vec<String> {
    env::var(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS)
        .ok()
        .map(|s| {
            s.split(',')
                .map(|v| {
                    let v = v.trim().to_lowercase();
                    if v.is_empty() {
                        String::new()
                    } else if v.starts_with('.') {
                        v
                    } else {
                        format!(".{v}")
                    }
                })
                .filter(|v| !v.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

// Parsed once at first use, then reused for all is_compressible() checks.
static ADDED_EXCLUDE_COMPRESS_EXTENSIONS: OnceLock<Vec<String>> = OnceLock::new();

// Some standard object extensions which we strictly dis-allow for compression.
#[rustfmt::skip]
pub const STANDARD_EXCLUDE_COMPRESS_EXTENSIONS: &[&str] = &[
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
pub const STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES: &[&str] = &[
    "video/*",
    "audio/*",
    "image/*",
    "application/zip",
    "application/x-gzip",
    "application/x-zip-compressed",
    "application/x-compress",
    "application/x-spoon",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/x-bzip2",
    "application/x-xz",
    "application/zstd",
    "application/pdf",
    "application/wasm",
    "font/*",
];

pub fn is_compressible(headers: &http::HeaderMap, object_name: &str) -> bool {
    // Check if compression is enabled via environment variable, default disabled
    if let Ok(compression_enabled) = env::var(ENV_COMPRESSION_ENABLED) {
        if compression_enabled.to_lowercase() != "true" {
            debug!("Compression is disabled by environment variable");
            return false;
        }
    } else {
        // Default disabled when environment variable is not set
        return false;
    }

    let content_type = headers.get("content-type").and_then(|s| s.to_str().ok()).unwrap_or("");

    // TODO: crypto request return false

    if has_string_suffix_in_slice(object_name, STANDARD_EXCLUDE_COMPRESS_EXTENSIONS) {
        debug!("object_name: {} is not compressible", object_name);
        return false;
    }

    let added = ADDED_EXCLUDE_COMPRESS_EXTENSIONS.get_or_init(parse_added_exclude_extensions);
    if !added.is_empty() && has_string_suffix_in_slice(object_name, &added) {
        debug!("object_name: {} is not compressible (added exclusion)", object_name);
        return false;
    }

    if !content_type.is_empty() && has_pattern(STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES, content_type) {
        debug!("content_type: {} is not compressible", content_type);
        return false;
    }
    true

    // TODO: check from config
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_env;

    #[test]
    fn test_is_compressible() {
        use http::HeaderMap;

        let headers = HeaderMap::new();

        // Test environment variable control
        temp_env::with_var(ENV_COMPRESSION_ENABLED, Some("false"), || {
            assert!(!is_compressible(&headers, "file.txt"));
        });

        temp_env::with_var(ENV_COMPRESSION_ENABLED, Some("true"), || {
            assert!(is_compressible(&headers, "file.txt"));
        });

        temp_env::with_var_unset(ENV_COMPRESSION_ENABLED, || {
            assert!(!is_compressible(&headers, "file.txt"));
        });

        temp_env::with_var(ENV_COMPRESSION_ENABLED, Some("true"), || {
            let mut headers = HeaderMap::new();
            // Test non-compressible extensions - compressed archives
            headers.insert("content-type", "text/plain".parse().unwrap());
            assert!(!is_compressible(&headers, "file.gz"));
            assert!(!is_compressible(&headers, "file.zip"));
            assert!(!is_compressible(&headers, "file.7z"));
            assert!(!is_compressible(&headers, "file.zst"));
            assert!(!is_compressible(&headers, "file.br"));
            assert!(!is_compressible(&headers, "file.tgz"));

            // Test non-compressible extensions - images
            assert!(!is_compressible(&headers, "file.jpg"));
            assert!(!is_compressible(&headers, "file.jpeg"));
            assert!(!is_compressible(&headers, "file.png"));
            assert!(!is_compressible(&headers, "file.gif"));
            assert!(!is_compressible(&headers, "file.webp"));
            assert!(!is_compressible(&headers, "file.avif"));

            // Test non-compressible extensions - video
            assert!(!is_compressible(&headers, "file.mp4"));
            assert!(!is_compressible(&headers, "file.mkv"));
            assert!(!is_compressible(&headers, "file.webm"));

            // Test non-compressible extensions - audio
            assert!(!is_compressible(&headers, "file.mp3"));
            assert!(!is_compressible(&headers, "file.aac"));
            assert!(!is_compressible(&headers, "file.ogg"));
            assert!(!is_compressible(&headers, "file.flac"));
            assert!(!is_compressible(&headers, "file.opus"));

            // Test non-compressible extensions - documents
            assert!(!is_compressible(&headers, "file.pdf"));
            assert!(!is_compressible(&headers, "file.docx"));
            assert!(!is_compressible(&headers, "file.xlsx"));
            assert!(!is_compressible(&headers, "file.pptx"));

            // Test non-compressible extensions - packages
            assert!(!is_compressible(&headers, "file.deb"));
            assert!(!is_compressible(&headers, "file.rpm"));
            assert!(!is_compressible(&headers, "file.jar"));

            // Test non-compressible extensions - web fonts
            assert!(!is_compressible(&headers, "file.woff2"));

            // Test non-compressible content types
            headers.insert("content-type", "video/mp4".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "audio/mpeg".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "image/png".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "image/webp".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/zip".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/x-gzip".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/x-7z-compressed".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/pdf".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "font/woff2".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            // Test compressible cases
            headers.insert("content-type", "text/plain".parse().unwrap());
            assert!(is_compressible(&headers, "file.txt"));
            assert!(is_compressible(&headers, "file.log"));

            headers.insert("content-type", "text/html".parse().unwrap());
            assert!(is_compressible(&headers, "file.html"));

            headers.insert("content-type", "application/json".parse().unwrap());
            assert!(is_compressible(&headers, "file.json"));
        });
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
    fn test_added_exclude_compress_extensions_default_compressible() {
        use http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        temp_env::with_var(ENV_COMPRESSION_ENABLED, Some("true"), || {
            temp_env::with_var_unset(ENV_ADDED_EXCLUDE_COMPRESS_EXTENSIONS, || {
                assert!(is_compressible(&headers, "file.foo"));
                assert!(is_compressible(&headers, "file.bar"));
            });
        });
    }
}
