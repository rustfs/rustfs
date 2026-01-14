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
use tracing::error;

pub const MIN_COMPRESSIBLE_SIZE: usize = 4096;

// Environment variable name to control whether compression is enabled
pub const ENV_COMPRESSION_ENABLED: &str = "RUSTFS_COMPRESSION_ENABLED";

// Some standard object extensions which we strictly dis-allow for compression.
pub const STANDARD_EXCLUDE_COMPRESS_EXTENSIONS: &[&str] = &[
    ".gz", ".bz2", ".rar", ".zip", ".7z", ".xz", ".mp4", ".mkv", ".mov", ".jpg", ".png", ".gif",
];

// Some standard content-types which we strictly dis-allow for compression.
pub const STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES: &[&str] = &[
    "video/*",
    "audio/*",
    "application/zip",
    "application/x-gzip",
    "application/x-zip-compressed",
    "application/x-compress",
    "application/x-spoon",
];

pub fn is_compressible(headers: &http::HeaderMap, object_name: &str) -> bool {
    // Check if compression is enabled via environment variable, default disabled
    if let Ok(compression_enabled) = env::var(ENV_COMPRESSION_ENABLED) {
        if compression_enabled.to_lowercase() != "true" {
            error!("Compression is disabled by environment variable");
            return false;
        }
    } else {
        // Default disabled when environment variable is not set
        return false;
    }

    let content_type = headers.get("content-type").and_then(|s| s.to_str().ok()).unwrap_or("");

    // TODO: crypto request return false

    if has_string_suffix_in_slice(object_name, STANDARD_EXCLUDE_COMPRESS_EXTENSIONS) {
        error!("object_name: {} is not compressible", object_name);
        return false;
    }

    if !content_type.is_empty() && has_pattern(STANDARD_EXCLUDE_COMPRESS_CONTENT_TYPES, content_type) {
        error!("content_type: {} is not compressible", content_type);
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
            // Test non-compressible extensions
            headers.insert("content-type", "text/plain".parse().unwrap());
            assert!(!is_compressible(&headers, "file.gz"));
            assert!(!is_compressible(&headers, "file.zip"));
            assert!(!is_compressible(&headers, "file.mp4"));
            assert!(!is_compressible(&headers, "file.jpg"));

            // Test non-compressible content types
            headers.insert("content-type", "video/mp4".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "audio/mpeg".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/zip".parse().unwrap());
            assert!(!is_compressible(&headers, "file.txt"));

            headers.insert("content-type", "application/x-gzip".parse().unwrap());
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
}
