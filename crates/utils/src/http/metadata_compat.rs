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

use std::collections::HashMap;

pub const RUSTFS_INTERNAL_PREFIX: &str = "x-rustfs-internal-";
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

/// Case-insensitive (ASCII) check that `s` begins with `prefix`. Equivalent to
/// `s.to_lowercase().starts_with(prefix)` when `prefix` is ASCII (as both internal prefixes are),
/// but without allocating.
fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// Allocation-free equivalent of `key.to_lowercase() == format!("{prefix}{suffix}")`.
/// The common ASCII path matches the prefix case-insensitively and ASCII-lowercases the suffix
/// region byte-for-byte. Non-ASCII keys fall back to full Unicode lowercasing so the result is
/// identical to the original for every input (e.g. U+212A KELVIN SIGN lowercases to ASCII `k`).
fn internal_key_eq(key: &str, prefix: &str, suffix: &str) -> bool {
    if !key.is_ascii() {
        return key.to_lowercase() == format!("{prefix}{suffix}");
    }
    let key = key.as_bytes();
    if key.len() != prefix.len() + suffix.len() {
        return false;
    }
    let (key_prefix, key_suffix) = key.split_at(prefix.len());
    key_prefix.eq_ignore_ascii_case(prefix.as_bytes())
        && key_suffix
            .iter()
            .zip(suffix.as_bytes())
            .all(|(k, s)| k.to_ascii_lowercase() == *s)
}

/// Returns true if the key is an internal metadata key (x-rustfs-internal-* or x-minio-internal-*)
/// for xl.meta compatibility. Case-insensitive.
pub fn is_internal_key(key: &str) -> bool {
    starts_with_ignore_ascii_case(key, RUSTFS_INTERNAL_PREFIX) || starts_with_ignore_ascii_case(key, MINIO_INTERNAL_PREFIX)
}

/// Returns true if the key matches the given suffix for either x-rustfs-internal-* or x-minio-internal-*.
pub fn has_internal_suffix(key: &str, suffix: &str) -> bool {
    internal_key_eq(key, RUSTFS_INTERNAL_PREFIX, suffix) || internal_key_eq(key, MINIO_INTERNAL_PREFIX, suffix)
}

/// Strips x-rustfs-internal- or x-minio-internal- prefix from key. Returns the suffix part.
/// Case-insensitive. Returns None if key is not an internal key.
pub fn strip_internal_prefix(key: &str) -> Option<String> {
    let rest = if starts_with_ignore_ascii_case(key, RUSTFS_INTERNAL_PREFIX) {
        &key[RUSTFS_INTERNAL_PREFIX.len()..]
    } else if starts_with_ignore_ascii_case(key, MINIO_INTERNAL_PREFIX) {
        &key[MINIO_INTERNAL_PREFIX.len()..]
    } else {
        return None;
    };
    Some(rest.to_lowercase())
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
    (format!("{RUSTFS_INTERNAL_PREFIX}{suffix}"), format!("{MINIO_INTERNAL_PREFIX}{suffix}"))
}

/// Longest known suffix ("objectlock-retention-timestamp", 30 bytes) plus the 18-byte prefix fits
/// well under this; the cap leaves ample headroom so lookups never touch the heap in practice.
const INTERNAL_KEY_STACK_CAP: usize = 96;

/// Builds the internal key `{prefix}{suffix}` in a stack buffer and invokes `f` with it, avoiding
/// the per-lookup heap allocation that `format!` incurs. Falls back to an owned `String` only for
/// unusually long suffixes that do not fit the stack buffer.
fn with_internal_key<R>(prefix: &str, suffix: &str, f: impl FnOnce(&str) -> R) -> R {
    let total = prefix.len() + suffix.len();
    if total <= INTERNAL_KEY_STACK_CAP {
        let mut buf = [0u8; INTERNAL_KEY_STACK_CAP];
        buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
        buf[prefix.len()..total].copy_from_slice(suffix.as_bytes());
        // `prefix` and `suffix` are both `&str`, so their concatenation is valid UTF-8.
        match std::str::from_utf8(&buf[..total]) {
            Ok(key) => f(key),
            Err(_) => f(&format!("{prefix}{suffix}")),
        }
    } else {
        f(&format!("{prefix}{suffix}"))
    }
}

/// Builds the RustFS internal key for the given suffix. Use when a single key is needed (e.g. for
/// backward compat). Prefer insert_str/get_str when both keys should be written/read.
pub fn internal_key_rustfs(suffix: &str) -> String {
    format!("{RUSTFS_INTERNAL_PREFIX}{suffix}")
}

// === String type (FileInfo.metadata, user_defined) ===

pub fn insert_str(map: &mut HashMap<String, String>, suffix: &str, value: String) {
    let (k1, k2) = both_keys(suffix);
    map.insert(k1, value.clone());
    map.insert(k2, value);
}

pub fn get_str(map: &HashMap<String, String>, suffix: &str) -> Option<String> {
    if let Some(v) = with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.get(k1).cloned()) {
        return Some(v);
    }
    if let Some(v) = with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.get(k2).cloned()) {
        return Some(v);
    }
    // Rare fallback: case-insensitive scan for non-canonical key casing.
    let (k1, k2) = both_keys(suffix);
    map.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(&k1) || key.eq_ignore_ascii_case(&k2))
        .map(|(_, value)| value.clone())
}

pub fn contains_key_str(map: &HashMap<String, String>, suffix: &str) -> bool {
    if with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.contains_key(k1)) {
        return true;
    }
    if with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.contains_key(k2)) {
        return true;
    }
    let (k1, k2) = both_keys(suffix);
    map.keys()
        .any(|key| key.eq_ignore_ascii_case(&k1) || key.eq_ignore_ascii_case(&k2))
}

pub fn remove_str(map: &mut HashMap<String, String>, suffix: &str) {
    with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.remove(k1));
    with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.remove(k2));
    let (k1, k2) = both_keys(suffix);
    map.retain(|key, _| !key.eq_ignore_ascii_case(&k1) && !key.eq_ignore_ascii_case(&k2));
}

// === Vec<u8> type (meta_sys) ===

pub fn insert_bytes(map: &mut HashMap<String, Vec<u8>>, suffix: &str, value: Vec<u8>) {
    let (k1, k2) = both_keys(suffix);
    let v = value.clone();
    map.insert(k1, value);
    map.insert(k2, v);
}

pub fn get_bytes(map: &HashMap<String, Vec<u8>>, suffix: &str) -> Option<Vec<u8>> {
    with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.get(k1).cloned())
        .or_else(|| with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.get(k2).cloned()))
}

pub fn contains_key_bytes(map: &HashMap<String, Vec<u8>>, suffix: &str) -> bool {
    with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.contains_key(k1))
        || with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.contains_key(k2))
}

pub fn remove_bytes(map: &mut HashMap<String, Vec<u8>>, suffix: &str) {
    with_internal_key(RUSTFS_INTERNAL_PREFIX, suffix, |k1| map.remove(k1));
    with_internal_key(MINIO_INTERNAL_PREFIX, suffix, |k2| map.remove(k2));
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

    #[test]
    fn test_str_lookup_accepts_minio_metadata_case() {
        let mut metadata = HashMap::from([
            ("X-Minio-Internal-compression".to_string(), "klauspost/compress/s2".to_string()),
            ("X-Minio-Internal-actual-size".to_string(), "268435456".to_string()),
        ]);

        assert!(contains_key_str(&metadata, SUFFIX_COMPRESSION));
        assert_eq!(get_str(&metadata, SUFFIX_COMPRESSION).as_deref(), Some("klauspost/compress/s2"));
        assert_eq!(get_str(&metadata, SUFFIX_ACTUAL_SIZE).as_deref(), Some("268435456"));

        remove_str(&mut metadata, SUFFIX_COMPRESSION);
        assert!(!contains_key_str(&metadata, SUFFIX_COMPRESSION));
        assert!(!metadata.contains_key("X-Minio-Internal-compression"));
        assert!(contains_key_str(&metadata, SUFFIX_ACTUAL_SIZE));
    }

    #[test]
    fn test_str_prefers_rustfs_key_over_minio() {
        let metadata = HashMap::from([
            (internal_key_rustfs(SUFFIX_TRANSITION_TIER), "rustfs-tier".to_string()),
            (format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_TRANSITION_TIER}"), "minio-tier".to_string()),
        ]);

        assert_eq!(get_str(&metadata, SUFFIX_TRANSITION_TIER).as_deref(), Some("rustfs-tier"));
    }

    #[test]
    fn test_bytes_lookup_falls_back_to_minio_key() {
        let mut meta_sys =
            HashMap::from([(format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_TRANSITIONED_VERSION_ID}"), b"version-1".to_vec())]);

        assert!(contains_key_bytes(&meta_sys, SUFFIX_TRANSITIONED_VERSION_ID));
        assert_eq!(get_bytes(&meta_sys, SUFFIX_TRANSITIONED_VERSION_ID), Some(b"version-1".to_vec()));

        remove_bytes(&mut meta_sys, SUFFIX_TRANSITIONED_VERSION_ID);
        assert!(!contains_key_bytes(&meta_sys, SUFFIX_TRANSITIONED_VERSION_ID));
    }

    #[test]
    fn test_bytes_prefers_rustfs_key_over_minio() {
        let meta_sys = HashMap::from([
            (internal_key_rustfs(SUFFIX_TRANSITIONED_VERSION_ID), b"rustfs-version".to_vec()),
            (
                format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_TRANSITIONED_VERSION_ID}"),
                b"minio-version".to_vec(),
            ),
        ]);

        assert_eq!(get_bytes(&meta_sys, SUFFIX_TRANSITIONED_VERSION_ID), Some(b"rustfs-version".to_vec()));
    }

    // Reference implementations mirroring the original allocation-heavy logic, used to prove the
    // optimized helpers are behavior-preserving across a battery of inputs.
    fn is_internal_key_ref(key: &str) -> bool {
        let lower = key.to_lowercase();
        lower.starts_with(RUSTFS_INTERNAL_PREFIX) || lower.starts_with(MINIO_INTERNAL_PREFIX)
    }

    fn has_internal_suffix_ref(key: &str, suffix: &str) -> bool {
        let lower = key.to_lowercase();
        lower == format!("{RUSTFS_INTERNAL_PREFIX}{suffix}") || lower == format!("{MINIO_INTERNAL_PREFIX}{suffix}")
    }

    fn strip_internal_prefix_ref(key: &str) -> Option<String> {
        let lower = key.to_lowercase();
        lower
            .strip_prefix(RUSTFS_INTERNAL_PREFIX)
            .or_else(|| lower.strip_prefix(MINIO_INTERNAL_PREFIX))
            .map(|s| s.to_string())
    }

    #[test]
    fn test_classifiers_match_reference_impl() {
        let keys = [
            "x-rustfs-internal-inline-data",
            "X-RustFS-Internal-Inline-Data",
            "x-minio-internal-compression",
            "X-MINIO-INTERNAL-actual-size",
            "x-rustfs-internal-",
            "x-rustfs-internal-replication-reset-arn:aws:s3:::bucket",
            "x-rustfs-internal-Actual-Object-Size",
            "x-amz-meta-custom",
            "content-type",
            "not-internal",
            "",
            "X",
            "x-rustfs-interna",                          // one char short of the prefix
            "x-rustfs-internal-tier-free-mar\u{212A}er", // U+212A KELVIN SIGN lowercases to ASCII 'k'
        ];
        let suffixes = [
            SUFFIX_INLINE_DATA,
            SUFFIX_COMPRESSION,
            SUFFIX_ACTUAL_SIZE,
            SUFFIX_ACTUAL_OBJECT_SIZE_CAP,
            SUFFIX_PURGESTATUS,
            SUFFIX_TIER_FV_MARKER,
            "inline-data",
            "nonexistent",
            "",
        ];

        for key in keys {
            assert_eq!(is_internal_key(key), is_internal_key_ref(key), "is_internal_key mismatch for {key:?}");
            assert_eq!(
                strip_internal_prefix(key),
                strip_internal_prefix_ref(key),
                "strip_internal_prefix mismatch for {key:?}"
            );
            for suffix in suffixes {
                assert_eq!(
                    has_internal_suffix(key, suffix),
                    has_internal_suffix_ref(key, suffix),
                    "has_internal_suffix mismatch for key {key:?} suffix {suffix:?}"
                );
            }
        }
    }

    #[test]
    fn test_has_internal_suffix_kelvin_sign_equivalence() {
        // U+212A KELVIN SIGN Unicode-lowercases to ASCII 'k'. The optimized ASCII fast path must
        // fall back to full Unicode lowercasing for non-ASCII keys so it stays byte-for-byte
        // equivalent to the original `key.to_lowercase()` implementation.
        let key = "x-rustfs-internal-tier-free-mar\u{212A}er";
        assert!(has_internal_suffix(key, SUFFIX_TIER_FV_MARKER));
        assert_eq!(
            has_internal_suffix(key, SUFFIX_TIER_FV_MARKER),
            has_internal_suffix_ref(key, SUFFIX_TIER_FV_MARKER)
        );
        // A plain ASCII 'k' key still matches, and a non-matching suffix still fails.
        assert!(has_internal_suffix("x-rustfs-internal-tier-free-marker", SUFFIX_TIER_FV_MARKER));
        assert!(!has_internal_suffix(key, SUFFIX_COMPRESSION));
    }

    #[test]
    fn test_get_str_case_insensitive_fallback() {
        // Non-canonical mixed-case key must still be found via the case-insensitive scan.
        let metadata = HashMap::from([("X-RustFS-Internal-Compression".to_string(), "s2".to_string())]);
        assert_eq!(get_str(&metadata, SUFFIX_COMPRESSION).as_deref(), Some("s2"));
        assert!(contains_key_str(&metadata, SUFFIX_COMPRESSION));
    }

    #[test]
    fn test_get_bytes_no_case_insensitive_fallback() {
        // get_bytes only checks the two canonical keys (matching the original behavior): a
        // mixed-case key is not matched.
        let meta_sys = HashMap::from([("X-RustFS-Internal-Crc".to_string(), b"z".to_vec())]);
        assert_eq!(get_bytes(&meta_sys, SUFFIX_CRC), None);

        let meta_sys = HashMap::from([(internal_key_rustfs(SUFFIX_CRC), b"z".to_vec())]);
        assert_eq!(get_bytes(&meta_sys, SUFFIX_CRC), Some(b"z".to_vec()));
    }

    #[test]
    fn test_long_suffix_falls_back_to_heap_key() {
        // A suffix longer than the stack buffer must still round-trip via the heap fallback.
        let long_suffix = "x".repeat(INTERNAL_KEY_STACK_CAP);
        let mut meta_sys = HashMap::new();
        insert_bytes(&mut meta_sys, &long_suffix, b"payload".to_vec());

        assert!(contains_key_bytes(&meta_sys, &long_suffix));
        assert_eq!(get_bytes(&meta_sys, &long_suffix), Some(b"payload".to_vec()));
        remove_bytes(&mut meta_sys, &long_suffix);
        assert!(!contains_key_bytes(&meta_sys, &long_suffix));
    }
}
