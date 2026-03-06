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

//! System metadata compatibility: write both x-rustfs-internal-* and x-minio-internal-*
//! for MinIO interoperability. Read prefers RustFS, fallback to MinIO.

use super::RESERVED_METADATA_PREFIX_LOWER;
use std::collections::HashMap;

pub const MINIO_INTERNAL_PREFIX: &str = "x-minio-internal-";

// Key suffixes (lowercase, no prefix)
pub const SUFFIX_INLINE_DATA: &str = "inline-data";
pub const SUFFIX_DATA_MOVED: &str = "data-moved";
/// Transient flag for data movement
pub const SUFFIX_DATA_MOV: &str = "data-mov";
/// Transient flag for healing
pub const SUFFIX_HEALING: &str = "healing";
pub const SUFFIX_COMPRESSION: &str = "compression";
pub const SUFFIX_COMPRESSION_SIZE: &str = "compression-size";
pub const SUFFIX_ACTUAL_SIZE: &str = "actual-size";
pub const SUFFIX_ACTUAL_OBJECT_SIZE: &str = "actual-object-size";
/// Used by replication; key stored with capital A
pub const SUFFIX_ACTUAL_OBJECT_SIZE_CAP: &str = "Actual-Object-Size";
pub const SUFFIX_CRC: &str = "crc";
pub const SUFFIX_TRANSITION_STATUS: &str = "transition-status";
pub const SUFFIX_TRANSITIONED_OBJECTNAME: &str = "transitioned-object";
pub const SUFFIX_TRANSITIONED_VERSION_ID: &str = "transitioned-versionID";
pub const SUFFIX_TRANSITION_TIER: &str = "transition-tier";
pub const SUFFIX_FREE_VERSION: &str = "free-version";
pub const SUFFIX_PURGESTATUS: &str = "purgestatus";
pub const SUFFIX_REPLICA_STATUS: &str = "replica-status";
pub const SUFFIX_REPLICA_TIMESTAMP: &str = "replica-timestamp";
pub const SUFFIX_REPLICATION_STATUS: &str = "replication-status";
pub const SUFFIX_REPLICATION_TIMESTAMP: &str = "replication-timestamp";
pub const SUFFIX_TAGGING_TIMESTAMP: &str = "tagging-timestamp";
pub const SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP: &str = "objectlock-retention-timestamp";
pub const SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP: &str = "objectlock-legalhold-timestamp";
pub const SUFFIX_REPLICATION_RESET: &str = "replication-reset";
/// Prefix for replication-reset-{arn} keys; use with internal_key_strip_suffix_prefix to extract arn.
pub const SUFFIX_REPLICATION_RESET_ARN_PREFIX: &str = "replication-reset-";
pub const SUFFIX_TIER_FV_ID: &str = "tier-free-versionID";
pub const SUFFIX_TIER_FV_MARKER: &str = "tier-free-marker";
pub const SUFFIX_TIER_SKIP_FV_ID: &str = "tier-skip-fvid";

/// Returns true if the key is an internal metadata key (x-rustfs-internal-* or x-minio-internal-*)
/// for xl.meta compatibility. Case-insensitive.
pub fn is_internal_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.starts_with(RESERVED_METADATA_PREFIX_LOWER) || lower.starts_with(MINIO_INTERNAL_PREFIX)
}

/// Returns true if the key matches the given suffix for either x-rustfs-internal-* or x-minio-internal-*.
pub fn has_internal_suffix(key: &str, suffix: &str) -> bool {
    let lower = key.to_lowercase();
    let rustfs_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{suffix}");
    let minio_key = format!("{MINIO_INTERNAL_PREFIX}{suffix}");
    lower == rustfs_key || lower == minio_key
}

/// Strips x-rustfs-internal- or x-minio-internal- prefix from key. Returns the suffix part.
/// Case-insensitive. Returns None if key is not an internal key.
pub fn strip_internal_prefix(key: &str) -> Option<String> {
    let lower = key.to_lowercase();
    lower
        .strip_prefix(RESERVED_METADATA_PREFIX_LOWER)
        .or_else(|| lower.strip_prefix(MINIO_INTERNAL_PREFIX))
        .map(|s| s.to_string())
}

/// Returns true if key is internal and its suffix part starts with the given suffix_prefix.
/// E.g. internal_key_starts_with("x-rustfs-internal-replication-reset-arn1", "replication-reset") == true.
pub fn internal_key_starts_with(key: &str, suffix_prefix: &str) -> bool {
    strip_internal_prefix(key).is_some_and(|s| s.starts_with(suffix_prefix))
}

/// For keys like x-rustfs-internal-replication-reset-{arn}, strips the internal prefix and suffix_prefix,
/// returning the remainder (e.g. "arn1"). Returns None if key does not match.
pub fn internal_key_strip_suffix_prefix(key: &str, suffix_prefix: &str) -> Option<String> {
    let rest = strip_internal_prefix(key)?;
    rest.strip_prefix(suffix_prefix).map(|s| s.to_string())
}

fn both_keys(suffix: &str) -> (String, String) {
    (
        format!("{RESERVED_METADATA_PREFIX_LOWER}{suffix}"),
        format!("{MINIO_INTERNAL_PREFIX}{suffix}"),
    )
}

/// Builds the RustFS internal key for the given suffix. Use when a single key is needed (e.g. for
/// backward compat). Prefer insert_str/get_str when both keys should be written/read.
pub fn internal_key_rustfs(suffix: &str) -> String {
    format!("{RESERVED_METADATA_PREFIX_LOWER}{suffix}")
}

// === String type (FileInfo.metadata, user_defined) ===

pub fn insert_str(map: &mut HashMap<String, String>, suffix: &str, value: String) {
    let (k1, k2) = both_keys(suffix);
    map.insert(k1, value.clone());
    map.insert(k2, value);
}

pub fn get_str(map: &HashMap<String, String>, suffix: &str) -> Option<String> {
    let (k1, k2) = both_keys(suffix);
    map.get(&k1).cloned().or_else(|| map.get(&k2).cloned())
}

pub fn contains_key_str(map: &HashMap<String, String>, suffix: &str) -> bool {
    let (k1, k2) = both_keys(suffix);
    map.contains_key(&k1) || map.contains_key(&k2)
}

pub fn remove_str(map: &mut HashMap<String, String>, suffix: &str) {
    let (k1, k2) = both_keys(suffix);
    map.remove(&k1);
    map.remove(&k2);
}

// === Vec<u8> type (meta_sys) ===

pub fn insert_bytes(map: &mut HashMap<String, Vec<u8>>, suffix: &str, value: Vec<u8>) {
    let (k1, k2) = both_keys(suffix);
    let v = value.clone();
    map.insert(k1, value);
    map.insert(k2, v);
}

pub fn get_bytes(map: &HashMap<String, Vec<u8>>, suffix: &str) -> Option<Vec<u8>> {
    let (k1, k2) = both_keys(suffix);
    map.get(&k1).cloned().or_else(|| map.get(&k2).cloned())
}

pub fn contains_key_bytes(map: &HashMap<String, Vec<u8>>, suffix: &str) -> bool {
    let (k1, k2) = both_keys(suffix);
    map.contains_key(&k1) || map.contains_key(&k2)
}

pub fn remove_bytes(map: &mut HashMap<String, Vec<u8>>, suffix: &str) {
    let (k1, k2) = both_keys(suffix);
    map.remove(&k1);
    map.remove(&k2);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_internal_key() {
        assert!(is_internal_key("x-rustfs-internal-healing"));
        assert!(is_internal_key("x-rustfs-internal-purgestatus"));
        assert!(is_internal_key("X-RustFS-Internal-purgestatus"));
        assert!(is_internal_key("x-minio-internal-compression"));
        assert!(is_internal_key("x-minio-internal-replication-status"));
        assert!(is_internal_key("X-Minio-Internal-Compression"));
        assert!(!is_internal_key("x-amz-meta-custom"));
        assert!(!is_internal_key("content-type"));
        assert!(!is_internal_key("x-rustfs-meta-custom"));
    }

    #[test]
    fn test_has_internal_suffix() {
        assert!(has_internal_suffix("x-rustfs-internal-purgestatus", SUFFIX_PURGESTATUS));
        assert!(has_internal_suffix("X-Minio-Internal-purgestatus", SUFFIX_PURGESTATUS));
        assert!(has_internal_suffix("x-minio-internal-compression", SUFFIX_COMPRESSION));
        assert!(has_internal_suffix("x-rustfs-internal-healing", SUFFIX_HEALING));
        assert!(has_internal_suffix("x-minio-internal-data-mov", SUFFIX_DATA_MOV));
        assert!(!has_internal_suffix("x-rustfs-internal-purgestatus", SUFFIX_HEALING));
        assert!(!has_internal_suffix("x-amz-meta-custom", SUFFIX_PURGESTATUS));
    }
}
