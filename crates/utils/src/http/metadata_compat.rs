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

/// Returns true if the key is an internal metadata key (x-rustfs-internal-* or x-minio-internal-*)
/// for xl.meta compatibility. Case-insensitive.
pub fn is_internal_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.starts_with(RESERVED_METADATA_PREFIX_LOWER) || lower.starts_with(MINIO_INTERNAL_PREFIX)
}

/// Returns true if the key matches the given suffix for either x-rustfs-internal-* or x-minio-internal-*.
/// Case-insensitive. E.g. has_internal_suffix("X-Minio-Internal-purgestatus", "purgestatus") == true.
pub fn has_internal_suffix(key: &str, suffix: &str) -> bool {
    let lower = key.to_lowercase();
    let rustfs_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{suffix}");
    let minio_key = format!("{MINIO_INTERNAL_PREFIX}{suffix}");
    lower == rustfs_key || lower == minio_key
}

fn both_keys(suffix: &str) -> (String, String) {
    (
        format!("{RESERVED_METADATA_PREFIX_LOWER}{suffix}"),
        format!("{MINIO_INTERNAL_PREFIX}{suffix}"),
    )
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
