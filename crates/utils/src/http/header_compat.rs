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

//! HTTP header compatibility: read both x-rustfs-* and x-minio-* headers for MinIO
//! interoperability. Write both when sending replication requests.
//!
//! Use suffix-based API: `get_header(headers, SUFFIX_FORCE_DELETE)` queries both
//! x-rustfs-force-delete and x-minio-force-delete.

use super::headers::RUSTFS_ENCRYPTION_LOWER;
use http::{HeaderMap, HeaderValue};
use std::borrow::Cow;

const RUSTFS_PREFIX: &str = "x-rustfs-";
const MINIO_PREFIX: &str = "x-minio-";
const MINIO_ENCRYPTION_PREFIX: &str = "x-minio-encryption-";

// Suffix constants (part after x-rustfs- or x-minio-). Use with get_header/insert_header.
pub const SUFFIX_FORCE_DELETE: &str = "force-delete";
pub const SUFFIX_INCLUDE_DELETED: &str = "include-deleted";
pub const SUFFIX_REPLICATION_RESET_STATUS: &str = "replication-reset-status";
pub const SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE: &str = "replication-actual-object-size";
pub const SUFFIX_SOURCE_VERSION_ID: &str = "source-version-id";
pub const SUFFIX_SOURCE_MTIME: &str = "source-mtime";
pub const SUFFIX_SOURCE_ETAG: &str = "source-etag";
pub const SUFFIX_SOURCE_DELETEMARKER: &str = "source-deletemarker";
pub const SUFFIX_SOURCE_PROXY_REQUEST: &str = "source-proxy-request";
pub const SUFFIX_SOURCE_REPLICATION_REQUEST: &str = "source-replication-request";
pub const SUFFIX_SOURCE_REPLICATION_CHECK: &str = "source-replication-check";
pub const SUFFIX_REPLICATION_SSEC_CRC: &str = "replication-ssec-crc";

/// Returns true if the key is an internal encryption metadata key (x-rustfs-encryption-* or
/// x-minio-encryption-*). Case-insensitive for metadata filtering.
pub fn is_encryption_metadata_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.starts_with(RUSTFS_ENCRYPTION_LOWER) || lower.starts_with(MINIO_ENCRYPTION_PREFIX)
}

fn rustfs_key(suffix: &str) -> String {
    format!("{RUSTFS_PREFIX}{suffix}")
}

fn minio_key(suffix: &str) -> String {
    format!("{MINIO_PREFIX}{suffix}")
}

/// Get header value: tries x-rustfs-{suffix} first, then x-minio-{suffix}. Case-insensitive.
pub fn get_header<'a>(headers: &'a HeaderMap, suffix: &str) -> Option<Cow<'a, str>> {
    let rk = rustfs_key(suffix);
    let mk = minio_key(suffix);
    headers
        .get(&rk)
        .or_else(|| headers.get(&mk))
        .and_then(|v| v.to_str().ok().map(Cow::Borrowed))
}

/// Insert header with both x-rustfs-{suffix} and x-minio-{suffix}.
pub fn insert_header(headers: &mut HeaderMap, suffix: &str, value: impl AsRef<[u8]>) {
    if let Ok(v) = HeaderValue::from_bytes(value.as_ref()) {
        if let Ok(k1) = rustfs_key(suffix).parse::<http::HeaderName>() {
            headers.insert(k1, v.clone());
        }
        if let Ok(k2) = minio_key(suffix).parse::<http::HeaderName>() {
            headers.insert(k2, v);
        }
    }
}

/// Get from HashMap: tries x-rustfs-{suffix} first, then x-minio-{suffix}.
pub fn get_header_map(map: &std::collections::HashMap<String, String>, suffix: &str) -> Option<String> {
    let rk = rustfs_key(suffix);
    let mk = minio_key(suffix);
    map.get(&rk).cloned().or_else(|| map.get(&mk).cloned())
}

/// Insert into HashMap with both x-rustfs-{suffix} and x-minio-{suffix}.
pub fn insert_header_map(map: &mut std::collections::HashMap<String, String>, suffix: &str, value: impl Into<String>) {
    let v = value.into();
    map.insert(rustfs_key(suffix), v.clone());
    map.insert(minio_key(suffix), v);
}

/// Remove from HashMap both x-rustfs-{suffix} and x-minio-{suffix}.
pub fn remove_header_map(map: &mut std::collections::HashMap<String, String>, suffix: &str) {
    map.remove(&rustfs_key(suffix));
    map.remove(&minio_key(suffix));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_encryption_metadata_key() {
        assert!(is_encryption_metadata_key("x-rustfs-encryption-iv"));
        assert!(is_encryption_metadata_key("X-Rustfs-Encryption-Key"));
        assert!(is_encryption_metadata_key("x-minio-encryption-iv"));
        assert!(!is_encryption_metadata_key("x-amz-meta-custom"));
        assert!(!is_encryption_metadata_key("x-rustfs-internal-healing"));
    }

    #[test]
    fn test_get_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-minio-force-delete", HeaderValue::from_static("true"));
        assert_eq!(get_header(&headers, SUFFIX_FORCE_DELETE).as_deref(), Some("true"));

        let mut headers2 = HeaderMap::new();
        headers2.insert("X-Rustfs-Force-Delete", HeaderValue::from_static("true"));
        assert_eq!(get_header(&headers2, SUFFIX_FORCE_DELETE).as_deref(), Some("true"));
    }
}
