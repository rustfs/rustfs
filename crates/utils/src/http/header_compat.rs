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
//!
//! This module is the canonical owner of these interop values. One deliberate
//! copy exists: `crates/replication/src/http.rs` re-declares the subset it
//! needs because the wire-contract crate must stay free of internal
//! dependencies (arch guard in `scripts/check_architecture_migration_rules.sh`
//! bans replication -> rustfs-utils). When changing a value here, check the
//! pinned copy there; its tests pin the shared wire values byte-for-byte.

use http::{HeaderMap, HeaderValue};
use std::borrow::Cow;

const RUSTFS_PREFIX: &str = "x-rustfs-";
const MINIO_PREFIX: &str = "x-minio-";
pub const MINIO_ENCRYPTION_PREFIX: &str = "x-minio-encryption-";
pub const RUSTFS_ENCRYPTION_PREFIX: &str = "x-rustfs-encryption-";
const MINIO_INTERNAL_ENCRYPTION_PREFIX: &str = "x-minio-internal-server-side-encryption-";
const MINIO_INTERNAL_ENCRYPTED_MULTIPART: &str = "x-minio-internal-encrypted-multipart";
const RUSTFS_ENCRYPTION_ORIGINAL_SIZE: &str = super::object_encryption_keys::INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER;
const MINIO_ENCRYPTION_ORIGINAL_SIZE: &str = "x-minio-encryption-original-size";
const SSEC_ORIGINAL_SIZE: &str = super::object_encryption_keys::SSEC_ORIGINAL_SIZE_HEADER;

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
// LWW timestamps for replicated tag/retention/legal-hold modifications. MinIO
// declares these with mixed case (internal/http/headers.go:
// X-Minio-Source-Replication-Tagging-Timestamp / -Retention-Timestamp /
// -LegalHold-Timestamp); HTTP header names compare case-insensitively, so the
// lowercase suffix forms interoperate. Values are RFC3339 on the wire.
pub const SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP: &str = "source-replication-tagging-timestamp";
pub const SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP: &str = "source-replication-retention-timestamp";
pub const SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP: &str = "source-replication-legalhold-timestamp";
pub const SUFFIX_REPLICATION_SSEC_CRC: &str = "replication-ssec-crc";

/// Returns true if the key is object-encryption metadata understood by RustFS or MinIO.
/// Case-insensitive for metadata filtering.
pub fn is_encryption_metadata_key(key: &str) -> bool {
    if key.is_ascii() {
        return super::starts_with_ignore_ascii_case(key, RUSTFS_ENCRYPTION_PREFIX)
            || super::starts_with_ignore_ascii_case(key, MINIO_ENCRYPTION_PREFIX)
            || super::starts_with_ignore_ascii_case(key, MINIO_INTERNAL_ENCRYPTION_PREFIX)
            || key.eq_ignore_ascii_case(MINIO_INTERNAL_ENCRYPTED_MULTIPART);
    }

    let lower = key.to_lowercase();
    lower.starts_with(RUSTFS_ENCRYPTION_PREFIX)
        || lower.starts_with(MINIO_ENCRYPTION_PREFIX)
        || lower.starts_with(MINIO_INTERNAL_ENCRYPTION_PREFIX)
        || lower == MINIO_INTERNAL_ENCRYPTED_MULTIPART
}

/// Returns true when a metadata key proves that object data is encrypted.
///
/// Original-size metadata alone is not proof: older plaintext objects can
/// retain that compatibility field after metadata migration.
pub fn is_object_encryption_marker(key: &str) -> bool {
    (is_encryption_metadata_key(key)
        && !key.eq_ignore_ascii_case(RUSTFS_ENCRYPTION_ORIGINAL_SIZE)
        && !key.eq_ignore_ascii_case(MINIO_ENCRYPTION_ORIGINAL_SIZE))
        || super::is_sse_header(key)
}

/// Reads the logical object size recorded by encryption metadata.
pub fn get_object_encryption_original_size(metadata: &std::collections::HashMap<String, String>) -> std::io::Result<Option<i64>> {
    let actual_size = super::get_str(metadata, super::SUFFIX_ACTUAL_SIZE);
    let size = get_case_insensitive(metadata, RUSTFS_ENCRYPTION_ORIGINAL_SIZE)
        .or_else(|| get_case_insensitive(metadata, SSEC_ORIGINAL_SIZE))
        .or(actual_size.as_deref());

    let Some(size) = size.filter(|size| !size.is_empty()) else {
        return Ok(None);
    };
    size.parse::<i64>()
        .map(Some)
        .map_err(|error| std::io::Error::other(format!("Failed to parse encryption original size: {error}")))
}

fn get_case_insensitive<'a>(metadata: &'a std::collections::HashMap<String, String>, key: &str) -> Option<&'a str> {
    metadata.get(key).map(String::as_str).or_else(|| {
        metadata
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.as_str())
    })
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
        assert!(is_encryption_metadata_key("X-Minio-Internal-Server-Side-Encryption-Sealed-Key"));
        assert!(is_encryption_metadata_key("X-Minio-Internal-Encrypted-Multipart"));
        assert!(is_encryption_metadata_key("X-RUSTFS-ENCRYPTION-\u{212A}EY"));
        assert!(!is_encryption_metadata_key("x-amz-meta-custom"));
        assert!(!is_encryption_metadata_key("x-minio-internal-encrypted-multipart-extra"));
        assert!(!is_encryption_metadata_key("x-rustfs-internal-healing"));
    }

    #[test]
    fn object_encryption_marker_excludes_size_only_metadata() {
        assert!(!is_object_encryption_marker(RUSTFS_ENCRYPTION_ORIGINAL_SIZE));
        assert!(is_object_encryption_marker("X-Minio-Internal-Server-Side-Encryption-Sealed-Key"));
        assert!(is_object_encryption_marker("x-amz-server-side-encryption"));
    }

    #[test]
    fn object_encryption_original_size_is_case_insensitive() {
        let metadata = std::collections::HashMap::from([(
            "X-Amz-Server-Side-Encryption-Customer-Original-Size".to_string(),
            "42".to_string(),
        )]);
        assert_eq!(get_object_encryption_original_size(&metadata).expect("valid size"), Some(42));
    }

    #[test]
    fn object_encryption_original_size_prefers_rustfs_metadata() {
        let metadata = std::collections::HashMap::from([
            (SSEC_ORIGINAL_SIZE.to_string(), "21".to_string()),
            (RUSTFS_ENCRYPTION_ORIGINAL_SIZE.to_string(), "42".to_string()),
        ]);
        assert_eq!(get_object_encryption_original_size(&metadata).expect("valid size"), Some(42));
    }

    #[test]
    fn replication_timestamp_headers_match_minio_wire_names() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP, "2026-01-02T03:04:05Z");
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, "2026-01-02T03:04:06Z");
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP, "2026-01-02T03:04:07Z");

        // The exact names MinIO's object-api-options.go reads (its Get()
        // canonicalizes case, so a case-insensitive match is wire-equivalent).
        for name in [
            "X-Minio-Source-Replication-Tagging-Timestamp",
            "X-Minio-Source-Replication-Retention-Timestamp",
            "X-Minio-Source-Replication-LegalHold-Timestamp",
            "x-rustfs-source-replication-tagging-timestamp",
            "x-rustfs-source-replication-retention-timestamp",
            "x-rustfs-source-replication-legalhold-timestamp",
        ] {
            assert!(headers.contains_key(name), "replication timestamp header {name} must be written");
        }
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
