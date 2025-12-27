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

use crate::http::{RESERVED_METADATA_PREFIX_LOWER, is_minio_header, is_rustfs_header};
use std::collections::HashMap;

/// Extract user-defined metadata keys from object metadata.
///
/// This function filters out system-level metadata and returns only user-defined keys.
///
/// Excluded keys include:
/// - S3 standard headers: content-type, cache-control, content-encoding, content-disposition,
///   content-language, expires
/// - x-amz-* headers (except user metadata with x-amz-meta- prefix which are stripped)
/// - x-rustfs-internal-* headers (system internal metadata)
/// - Storage/replication system keys: x-amz-storage-class, x-amz-tagging, x-amz-replication-status
/// - Object metadata: etag, md5Sum, last-modified
///
/// # Arguments
/// * `metadata` - The complete metadata HashMap from ObjectInfo.user_defined
///
/// # Returns
/// A new HashMap containing only user-defined metadata entries. Keys that use
/// the user-metadata prefix (for example `x-amz-meta-`) are returned with that
/// prefix stripped.
///
/// Note: The keys in the returned map may therefore differ from the keys in
/// the input `metadata` map and cannot be used directly to remove entries
/// from `metadata`. If you need to identify which original keys to remove,
/// consider using an in-place filtering approach or returning the original
/// keys instead.
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use rustfs_utils::obj::extract_user_defined_metadata;
///
/// let mut metadata = HashMap::new();
/// metadata.insert("content-type".to_string(), "application/json".to_string());
/// metadata.insert("x-minio-key".to_string(), "application/json".to_string());
/// metadata.insert("x-amz-grant-sse".to_string(), "application/json".to_string());
/// metadata.insert("x-amz-meta-user-key".to_string(), "user-value".to_string());
/// metadata.insert("my-custom-key".to_string(), "custom-value".to_string());
///
/// let user_keys = extract_user_defined_metadata(&metadata);
/// assert_eq!(user_keys.len(), 2);
/// assert_eq!(user_keys.get("user-key"), Some(&"user-value".to_string()));
/// assert_eq!(user_keys.get("my-custom-key"), Some(&"custom-value".to_string()));
/// ```
pub fn extract_user_defined_metadata(metadata: &HashMap<String, String>) -> HashMap<String, String> {
    let mut user_metadata = HashMap::new();

    let system_headers = [
        "content-type",
        "cache-control",
        "content-encoding",
        "content-disposition",
        "content-language",
        "expires",
        "content-length",
        "content-md5",
        "content-range",
        "last-modified",
        "etag",
        "md5sum",
        "date",
    ];

    for (key, value) in metadata {
        let lower_key = key.to_ascii_lowercase();

        if lower_key.starts_with(RESERVED_METADATA_PREFIX_LOWER) {
            continue;
        }

        if system_headers.contains(&lower_key.as_str()) {
            continue;
        }

        if let Some(user_key) = lower_key.strip_prefix("x-amz-meta-") {
            if !user_key.is_empty() {
                user_metadata.insert(user_key.to_string(), value.clone());
            }
            continue;
        }

        // Check if it's x-rustfs-meta-* and extract user key
        if let Some(user_key) = lower_key.strip_prefix("x-rustfs-meta-") {
            if !user_key.is_empty() {
                user_metadata.insert(user_key.to_string(), value.clone());
            }
            continue;
        }

        // Skip other x-amz-* headers
        if lower_key.starts_with("x-amz-") {
            continue;
        }

        // Skip other RustFS headers (x-rustfs-replication-*, etc.)
        if is_rustfs_header(key) {
            continue;
        }

        // Skip MinIO headers (compatibility)
        if is_minio_header(key) {
            continue;
        }

        // All other keys are considered user-defined
        user_metadata.insert(key.clone(), value.clone());
    }

    user_metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_user_defined_metadata_basic() {
        let mut metadata = HashMap::new();
        metadata.insert("my-key".to_string(), "my-value".to_string());
        metadata.insert("custom-header".to_string(), "custom-value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 2);
        assert_eq!(user_metadata.get("my-key"), Some(&"my-value".to_string()));
        assert_eq!(user_metadata.get("custom-header"), Some(&"custom-value".to_string()));
    }

    #[test]
    fn test_extract_user_defined_metadata_exclude_system_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/json".to_string());
        metadata.insert("cache-control".to_string(), "no-cache".to_string());
        metadata.insert("content-encoding".to_string(), "gzip".to_string());
        metadata.insert("content-disposition".to_string(), "attachment".to_string());
        metadata.insert("content-language".to_string(), "en-US".to_string());
        metadata.insert("expires".to_string(), "Wed, 21 Oct 2015 07:28:00 GMT".to_string());
        metadata.insert("etag".to_string(), "abc123".to_string());
        metadata.insert("last-modified".to_string(), "Tue, 20 Oct 2015 07:28:00 GMT".to_string());
        metadata.insert("my-key".to_string(), "my-value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 1);
        assert_eq!(user_metadata.get("my-key"), Some(&"my-value".to_string()));
        assert!(!user_metadata.contains_key("content-type"));
        assert!(!user_metadata.contains_key("cache-control"));
        assert!(!user_metadata.contains_key("etag"));
    }

    #[test]
    fn test_extract_user_defined_metadata_strip_amz_meta_prefix() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-user-id".to_string(), "12345".to_string());
        metadata.insert("x-amz-meta-project".to_string(), "test-project".to_string());
        metadata.insert("x-amz-storage-class".to_string(), "STANDARD".to_string());
        metadata.insert("x-amz-tagging".to_string(), "key=value".to_string());
        metadata.insert("x-amz-replication-status".to_string(), "COMPLETED".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 2);
        assert_eq!(user_metadata.get("user-id"), Some(&"12345".to_string()));
        assert_eq!(user_metadata.get("project"), Some(&"test-project".to_string()));
        assert!(!user_metadata.contains_key("x-amz-meta-user-id"));
        assert!(!user_metadata.contains_key("x-amz-storage-class"));
        assert!(!user_metadata.contains_key("x-amz-tagging"));
    }

    #[test]
    fn test_extract_user_defined_metadata_exclude_rustfs_internal() {
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("x-rustfs-internal-healing".to_string(), "true".to_string());
        metadata.insert("x-rustfs-internal-data-mov".to_string(), "value".to_string());
        metadata.insert("X-RustFS-Internal-purgestatus".to_string(), "status".to_string());
        metadata.insert("x-rustfs-meta-custom".to_string(), "custom-value".to_string());
        metadata.insert("my-key".to_string(), "my-value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 2);
        assert_eq!(user_metadata.get("custom"), Some(&"custom-value".to_string()));
        assert_eq!(user_metadata.get("my-key"), Some(&"my-value".to_string()));
        assert!(!user_metadata.contains_key("x-rustfs-internal-healing"));
        assert!(!user_metadata.contains_key("x-rustfs-internal-data-mov"));
    }

    #[test]
    fn test_extract_user_defined_metadata_exclude_minio_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("x-minio-custom".to_string(), "minio-value".to_string());
        metadata.insert("x-minio-internal".to_string(), "internal".to_string());
        metadata.insert("my-key".to_string(), "my-value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 1);
        assert_eq!(user_metadata.get("my-key"), Some(&"my-value".to_string()));
        assert!(!user_metadata.contains_key("x-minio-custom"));
    }

    #[test]
    fn test_extract_user_defined_metadata_mixed() {
        let mut metadata = HashMap::new();
        // System headers
        metadata.insert("content-type".to_string(), "application/json".to_string());
        metadata.insert("cache-control".to_string(), "no-cache".to_string());
        // AMZ headers
        metadata.insert("x-amz-meta-version".to_string(), "1.0".to_string());
        metadata.insert("x-amz-storage-class".to_string(), "STANDARD".to_string());
        // RustFS internal
        metadata.insert("x-rustfs-internal-healing".to_string(), "true".to_string());
        metadata.insert("x-rustfs-meta-source".to_string(), "upload".to_string());
        // User defined
        metadata.insert("my-custom-key".to_string(), "custom-value".to_string());
        metadata.insert("another-key".to_string(), "another-value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 4);
        assert_eq!(user_metadata.get("version"), Some(&"1.0".to_string()));
        assert_eq!(user_metadata.get("source"), Some(&"upload".to_string()));
        assert_eq!(user_metadata.get("my-custom-key"), Some(&"custom-value".to_string()));
        assert_eq!(user_metadata.get("another-key"), Some(&"another-value".to_string()));
        assert!(!user_metadata.contains_key("content-type"));
        assert!(!user_metadata.contains_key("x-amz-storage-class"));
        assert!(!user_metadata.contains_key("x-rustfs-internal-healing"));
    }

    #[test]
    fn test_extract_user_defined_metadata_empty() {
        let metadata = HashMap::new();
        let user_metadata = extract_user_defined_metadata(&metadata);
        assert!(user_metadata.is_empty());
    }

    #[test]
    fn test_extract_user_defined_metadata_case_insensitive() {
        let mut metadata = HashMap::new();
        metadata.insert("Content-Type".to_string(), "application/json".to_string());
        metadata.insert("CACHE-CONTROL".to_string(), "no-cache".to_string());
        metadata.insert("X-Amz-Meta-UserId".to_string(), "12345".to_string());
        metadata.insert("My-Custom-Key".to_string(), "value".to_string());

        let user_metadata = extract_user_defined_metadata(&metadata);

        assert_eq!(user_metadata.len(), 2);
        assert_eq!(user_metadata.get("userid"), Some(&"12345".to_string()));
        assert_eq!(user_metadata.get("My-Custom-Key"), Some(&"value".to_string()));
        assert!(!user_metadata.contains_key("Content-Type"));
    }
}
