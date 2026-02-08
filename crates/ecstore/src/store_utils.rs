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

use crate::config::storageclass::STANDARD;
use crate::disk::RUSTFS_META_BUCKET;
use regex::Regex;
use rustfs_utils::http::headers::{AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS};
use std::collections::HashMap;
use std::io::{Error, Result};
use std::sync::LazyLock;

static IP_ADDRESS_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d+\.){3}\d+$").expect("valid ip address regex"));
static STRICT_BUCKET_NAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").expect("valid strict bucket name regex"));
static NON_STRICT_BUCKET_NAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-_:]{1,61}[A-Za-z0-9]$").expect("valid non-strict bucket name regex"));

pub fn clean_metadata(metadata: &mut HashMap<String, String>) {
    remove_standard_storage_class(metadata);
    clean_metadata_keys(metadata, &["md5Sum", "etag", "expires", AMZ_OBJECT_TAGGING, "last-modified"]);
}

pub fn remove_standard_storage_class(metadata: &mut HashMap<String, String>) {
    if metadata.get(AMZ_STORAGE_CLASS) == Some(&STANDARD.to_string()) {
        metadata.remove(AMZ_STORAGE_CLASS);
    }
}

pub fn clean_metadata_keys(metadata: &mut HashMap<String, String>, key_names: &[&str]) {
    for key in key_names {
        metadata.remove(key.to_owned());
    }
}

// Check whether the bucket is the metadata bucket
fn is_meta_bucket(bucket_name: &str) -> bool {
    bucket_name == RUSTFS_META_BUCKET
}

// Check whether the bucket is reserved
fn is_reserved_bucket(bucket_name: &str) -> bool {
    bucket_name == "rustfs"
}

// Check whether the bucket name is reserved or invalid
pub fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    if bucket_entry.is_empty() {
        return true;
    }

    let bucket_entry = bucket_entry.trim_end_matches('/');
    let result = check_bucket_name(bucket_entry, strict).is_err();

    result || is_meta_bucket(bucket_entry) || is_reserved_bucket(bucket_entry)
}

// Check whether the bucket name is valid
fn check_bucket_name(bucket_name: &str, strict: bool) -> Result<()> {
    if bucket_name.trim().is_empty() {
        return Err(Error::other("Bucket name cannot be empty"));
    }
    if bucket_name.len() < 3 {
        return Err(Error::other("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name.len() > 63 {
        return Err(Error::other("Bucket name cannot be longer than 63 characters"));
    }

    if IP_ADDRESS_REGEX.is_match(bucket_name) {
        return Err(Error::other("Bucket name cannot be an IP address"));
    }

    let valid_bucket_name_regex = if strict {
        &*STRICT_BUCKET_NAME_REGEX
    } else {
        &*NON_STRICT_BUCKET_NAME_REGEX
    };

    if !valid_bucket_name_regex.is_match(bucket_name) {
        return Err(Error::other("Bucket name contains invalid characters"));
    }

    // Check for "..", ".-", "-."
    if bucket_name.contains("..") || bucket_name.contains(".-") || bucket_name.contains("-.") {
        return Err(Error::other("Bucket name contains invalid characters"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Invalid bucket names (should return true) ---

    #[test]
    fn test_empty_string_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("", false));
        assert!(is_reserved_or_invalid_bucket("", true));
    }

    #[test]
    fn test_whitespace_only_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("   ", false));
        assert!(is_reserved_or_invalid_bucket("   ", true));
    }

    #[test]
    fn test_too_short_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("ab", false));
        assert!(is_reserved_or_invalid_bucket("ab", true));
    }

    #[test]
    fn test_too_long_is_invalid() {
        let long_name = "a".repeat(64);
        assert!(is_reserved_or_invalid_bucket(&long_name, false));
        assert!(is_reserved_or_invalid_bucket(&long_name, true));
    }

    #[test]
    fn test_ip_address_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("192.168.1.1", false));
        assert!(is_reserved_or_invalid_bucket("10.0.0.1", true));
    }

    #[test]
    fn test_double_dots_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("my..bucket", false));
        assert!(is_reserved_or_invalid_bucket("my..bucket", true));
    }

    #[test]
    fn test_dot_dash_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("my.-bucket", false));
        assert!(is_reserved_or_invalid_bucket("my.-bucket", true));
    }

    #[test]
    fn test_dash_dot_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("my-.bucket", false));
        assert!(is_reserved_or_invalid_bucket("my-.bucket", true));
    }

    #[test]
    fn test_reserved_name_rustfs_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("rustfs", false));
        assert!(is_reserved_or_invalid_bucket("rustfs", true));
    }

    #[test]
    fn test_meta_bucket_is_invalid() {
        assert!(is_reserved_or_invalid_bucket(RUSTFS_META_BUCKET, false));
        assert!(is_reserved_or_invalid_bucket(RUSTFS_META_BUCKET, true));
    }

    #[test]
    fn test_starts_with_invalid_char_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("-my-bucket", false));
        assert!(is_reserved_or_invalid_bucket(".my-bucket", false));
    }

    #[test]
    fn test_ends_with_invalid_char_is_invalid() {
        assert!(is_reserved_or_invalid_bucket("my-bucket-", false));
        assert!(is_reserved_or_invalid_bucket("my-bucket.", false));
    }

    // --- Strict vs non-strict differences ---

    #[test]
    fn test_uppercase_valid_non_strict_invalid_strict() {
        assert!(!is_reserved_or_invalid_bucket("MyBucket", false));
        assert!(is_reserved_or_invalid_bucket("MyBucket", true));
    }

    #[test]
    fn test_underscore_valid_non_strict_invalid_strict() {
        assert!(!is_reserved_or_invalid_bucket("my_bucket", false));
        assert!(is_reserved_or_invalid_bucket("my_bucket", true));
    }

    #[test]
    fn test_colon_valid_non_strict_invalid_strict() {
        assert!(!is_reserved_or_invalid_bucket("my:bucket", false));
        assert!(is_reserved_or_invalid_bucket("my:bucket", true));
    }

    // --- Valid bucket names (should return false) ---

    #[test]
    fn test_simple_valid_bucket() {
        assert!(!is_reserved_or_invalid_bucket("my-bucket", false));
        assert!(!is_reserved_or_invalid_bucket("my-bucket", true));
    }

    #[test]
    fn test_valid_bucket_with_dots() {
        assert!(!is_reserved_or_invalid_bucket("my.bucket.name", false));
        assert!(!is_reserved_or_invalid_bucket("my.bucket.name", true));
    }

    #[test]
    fn test_numeric_bucket() {
        assert!(!is_reserved_or_invalid_bucket("123", false));
        assert!(!is_reserved_or_invalid_bucket("123", true));
    }

    #[test]
    fn test_min_length_bucket() {
        assert!(!is_reserved_or_invalid_bucket("abc", false));
        assert!(!is_reserved_or_invalid_bucket("abc", true));
    }

    #[test]
    fn test_max_length_bucket() {
        let max_name = "a".repeat(63);
        assert!(!is_reserved_or_invalid_bucket(&max_name, false));
        assert!(!is_reserved_or_invalid_bucket(&max_name, true));
    }

    #[test]
    fn test_trailing_slash_stripped() {
        assert!(!is_reserved_or_invalid_bucket("my-bucket/", false));
        assert!(!is_reserved_or_invalid_bucket("my-bucket/", true));
    }

    #[test]
    fn test_multiple_calls_same_result() {
        // Ensures static regex caching doesn't break across calls
        for _ in 0..100 {
            assert!(!is_reserved_or_invalid_bucket("my-bucket", false));
            assert!(!is_reserved_or_invalid_bucket("my-bucket", true));
            assert!(is_reserved_or_invalid_bucket("192.168.1.1", false));
            assert!(is_reserved_or_invalid_bucket("", false));
        }
    }
}
