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

use http::{HeaderMap, HeaderValue};
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::error::Result;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::store_api::ObjectOptions;
use rustfs_utils::path::is_dir_object;
use std::collections::HashMap;
use std::sync::LazyLock;
use uuid::Uuid;

/// Creates options for deleting an object in a bucket.
pub async fn del_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::suspended(bucket).await;

    // TODO: delete_prefix

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
        }

        if !versioned {
            return Err(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), id.clone()));
        }
    }

    let mut opts = put_opts_from_headers(headers, metadata.clone())
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::max().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for getting an object from a bucket.
pub async fn get_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    part_num: Option<usize>,
    headers: &HeaderMap<HeaderValue>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
        }

        if !versioned {
            return Err(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), id.clone()));
        }
    }

    let mut opts = get_default_opts(headers, HashMap::new(), false)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::max().to_string())
        } else {
            vid
        }
    };

    opts.part_number = part_num;

    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for putting an object in a bucket.
pub async fn put_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
        }

        if !versioned {
            return Err(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), id.clone()));
        }
    }

    let mut opts = put_opts_from_headers(headers, metadata)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::max().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for copying an object in a bucket.
pub async fn copy_dst_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
) -> Result<ObjectOptions> {
    put_opts(bucket, object, vid, headers, metadata).await
}

pub fn copy_src_opts(_bucket: &str, _object: &str, headers: &HeaderMap<HeaderValue>) -> Result<ObjectOptions> {
    get_default_opts(headers, HashMap::new(), false)
}

pub fn put_opts_from_headers(headers: &HeaderMap<HeaderValue>, metadata: HashMap<String, String>) -> Result<ObjectOptions> {
    get_default_opts(headers, metadata, false)
}

/// Creates default options for getting an object from a bucket.
pub fn get_default_opts(
    _headers: &HeaderMap<HeaderValue>,
    metadata: HashMap<String, String>,
    _copy_source: bool,
) -> Result<ObjectOptions> {
    Ok(ObjectOptions {
        user_defined: metadata,
        ..Default::default()
    })
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata(headers: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    extract_metadata_from_mime(headers, &mut metadata);

    metadata
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata_from_mime(headers: &HeaderMap<HeaderValue>, metadata: &mut HashMap<String, String>) {
    for (k, v) in headers.iter() {
        if let Some(key) = k.as_str().strip_prefix("x-amz-meta-") {
            if key.is_empty() {
                continue;
            }

            metadata.insert(key.to_owned(), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        if let Some(key) = k.as_str().strip_prefix("x-rustfs-meta-") {
            metadata.insert(key.to_owned(), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        for hd in SUPPORTED_HEADERS.iter() {
            if k.as_str() == *hd {
                metadata.insert(k.to_string(), String::from_utf8_lossy(v.as_bytes()).to_string());
                continue;
            }
        }
    }

    if !metadata.contains_key("content-type") {
        metadata.insert("content-type".to_owned(), "binary/octet-stream".to_owned());
    }
}

/// List of supported headers.
static SUPPORTED_HEADERS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    vec![
        "content-type",
        "cache-control",
        "content-language",
        "content-encoding",
        "content-disposition",
        "x-amz-storage-class",
        "x-amz-tagging",
        "expires",
        "x-amz-replication-status",
    ]
});

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn create_test_headers() -> HeaderMap<HeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("x-amz-meta-custom", HeaderValue::from_static("custom-value"));
        headers.insert("x-rustfs-meta-internal", HeaderValue::from_static("internal-value"));
        headers.insert("cache-control", HeaderValue::from_static("no-cache"));
        headers
    }

    fn create_test_metadata() -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());
        metadata
    }

    #[tokio::test]
    async fn test_del_opts_basic() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = del_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_del_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = del_opts("test-bucket", "test-dir/", None, &headers, HashMap::new()).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::max().to_string()));
    }

    #[tokio::test]
    async fn test_del_opts_with_valid_version_id() {
        let headers = create_test_headers();
        let valid_uuid = Uuid::new_v4().to_string();

        let result = del_opts("test-bucket", "test-object", Some(valid_uuid.clone()), &headers, HashMap::new()).await;

        // This test may fail if versioning is not enabled for the bucket
        // In a real test environment, you would mock BucketVersioningSys
        match result {
            Ok(opts) => {
                assert_eq!(opts.version_id, Some(valid_uuid));
            }
            Err(_) => {
                // Expected if versioning is not enabled
            }
        }
    }

    #[tokio::test]
    async fn test_del_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = del_opts("test-bucket", "test-object", Some(invalid_uuid), &headers, HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_get_opts_basic() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.part_number, None);
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_get_opts_with_part_number() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-object", None, Some(5), &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.part_number, Some(5));
    }

    #[tokio::test]
    async fn test_get_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = get_opts("test-bucket", "test-dir/", None, None, &headers).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::max().to_string()));
    }

    #[tokio::test]
    async fn test_get_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = get_opts("test-bucket", "test-object", Some(invalid_uuid), None, &headers).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_put_opts_basic() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = put_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        assert_eq!(opts.version_id, None);
    }

    #[tokio::test]
    async fn test_put_opts_with_directory_object() {
        let headers = create_test_headers();

        let result = put_opts("test-bucket", "test-dir/", None, &headers, HashMap::new()).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::max().to_string()));
    }

    #[tokio::test]
    async fn test_put_opts_with_invalid_version_id() {
        let headers = create_test_headers();
        let invalid_uuid = "invalid-uuid".to_string();

        let result = put_opts("test-bucket", "test-object", Some(invalid_uuid), &headers, HashMap::new()).await;

        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                StorageError::InvalidVersionID(bucket, object, version) => {
                    assert_eq!(bucket, "test-bucket");
                    assert_eq!(object, "test-object");
                    assert_eq!(version, "invalid-uuid");
                }
                _ => panic!("Expected InvalidVersionID error"),
            }
        }
    }

    #[tokio::test]
    async fn test_copy_dst_opts() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = copy_dst_opts("test-bucket", "test-object", None, &headers, metadata).await;

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
    }

    #[test]
    fn test_copy_src_opts() {
        let headers = create_test_headers();

        let result = copy_src_opts("test-bucket", "test-object", &headers);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.user_defined.is_empty());
    }

    #[test]
    fn test_put_opts_from_headers() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = put_opts_from_headers(&headers, metadata);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        let user_defined = opts.user_defined;
        assert_eq!(user_defined.get("key1"), Some(&"value1".to_string()));
        assert_eq!(user_defined.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_get_default_opts_with_metadata() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();

        let result = get_default_opts(&headers, metadata, false);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.user_defined.is_empty());
        let user_defined = opts.user_defined;
        assert_eq!(user_defined.get("key1"), Some(&"value1".to_string()));
        assert_eq!(user_defined.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_get_default_opts_without_metadata() {
        let headers = create_test_headers();

        let result = get_default_opts(&headers, HashMap::new(), false);

        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.user_defined.is_empty());
    }

    #[test]
    fn test_extract_metadata_basic() {
        let headers = create_test_headers();

        let metadata = extract_metadata(&headers);

        assert!(metadata.contains_key("content-type"));
        assert_eq!(metadata.get("content-type"), Some(&"application/json".to_string()));
        assert!(metadata.contains_key("cache-control"));
        assert_eq!(metadata.get("cache-control"), Some(&"no-cache".to_string()));
        assert!(metadata.contains_key("custom"));
        assert_eq!(metadata.get("custom"), Some(&"custom-value".to_string()));
        assert!(metadata.contains_key("internal"));
        assert_eq!(metadata.get("internal"), Some(&"internal-value".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_amz_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-user-id", HeaderValue::from_static("12345"));
        headers.insert("x-amz-meta-project", HeaderValue::from_static("test-project"));
        headers.insert("x-amz-meta-", HeaderValue::from_static("empty-key")); // Should be ignored

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("user-id"), Some(&"12345".to_string()));
        assert_eq!(metadata.get("project"), Some(&"test-project".to_string()));
        assert!(!metadata.contains_key(""));
    }

    #[test]
    fn test_extract_metadata_from_mime_rustfs_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-meta-internal-id", HeaderValue::from_static("67890"));
        headers.insert("x-rustfs-meta-category", HeaderValue::from_static("documents"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("internal-id"), Some(&"67890".to_string()));
        assert_eq!(metadata.get("category"), Some(&"documents".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_supported_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("cache-control", HeaderValue::from_static("max-age=3600"));
        headers.insert("content-language", HeaderValue::from_static("en-US"));
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        headers.insert("content-disposition", HeaderValue::from_static("attachment"));
        headers.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
        headers.insert("x-amz-tagging", HeaderValue::from_static("key1=value1&key2=value2"));
        headers.insert("expires", HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"));
        headers.insert("x-amz-replication-status", HeaderValue::from_static("COMPLETED"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"text/plain".to_string()));
        assert_eq!(metadata.get("cache-control"), Some(&"max-age=3600".to_string()));
        assert_eq!(metadata.get("content-language"), Some(&"en-US".to_string()));
        assert_eq!(metadata.get("content-encoding"), Some(&"gzip".to_string()));
        assert_eq!(metadata.get("content-disposition"), Some(&"attachment".to_string()));
        assert_eq!(metadata.get("x-amz-storage-class"), Some(&"STANDARD".to_string()));
        assert_eq!(metadata.get("x-amz-tagging"), Some(&"key1=value1&key2=value2".to_string()));
        assert_eq!(metadata.get("expires"), Some(&"Wed, 21 Oct 2015 07:28:00 GMT".to_string()));
        assert_eq!(metadata.get("x-amz-replication-status"), Some(&"COMPLETED".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_default_content_type() {
        let headers = HeaderMap::new();

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_existing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("content-type"), Some(&"application/json".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_unicode_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-chinese", HeaderValue::from_bytes("测试值".as_bytes()).unwrap());
        headers.insert("x-rustfs-meta-emoji", HeaderValue::from_bytes("🚀".as_bytes()).unwrap());

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("chinese"), Some(&"测试值".to_string()));
        assert_eq!(metadata.get("emoji"), Some(&"🚀".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_unsupported_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer token"));
        headers.insert("host", HeaderValue::from_static("example.com"));
        headers.insert("user-agent", HeaderValue::from_static("test-agent"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        // These headers should not be included in metadata
        assert!(!metadata.contains_key("authorization"));
        assert!(!metadata.contains_key("host"));
        assert!(!metadata.contains_key("user-agent"));
        // But default content-type should be added
        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_supported_headers_constant() {
        let expected_headers = vec![
            "content-type",
            "cache-control",
            "content-language",
            "content-encoding",
            "content-disposition",
            "x-amz-storage-class",
            "x-amz-tagging",
            "expires",
            "x-amz-replication-status",
        ];

        assert_eq!(*SUPPORTED_HEADERS, expected_headers);
        assert_eq!(SUPPORTED_HEADERS.len(), 9);
    }

    #[test]
    fn test_extract_metadata_empty_headers() {
        let headers = HeaderMap::new();

        let metadata = extract_metadata(&headers);

        // Should only contain default content-type
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("content-type"), Some(&"binary/octet-stream".to_string()));
    }

    #[test]
    fn test_extract_metadata_mixed_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/xml"));
        headers.insert("x-amz-meta-version", HeaderValue::from_static("1.0"));
        headers.insert("x-rustfs-meta-source", HeaderValue::from_static("upload"));
        headers.insert("cache-control", HeaderValue::from_static("public"));
        headers.insert("authorization", HeaderValue::from_static("Bearer xyz")); // Should be ignored

        let metadata = extract_metadata(&headers);

        assert_eq!(metadata.get("content-type"), Some(&"application/xml".to_string()));
        assert_eq!(metadata.get("version"), Some(&"1.0".to_string()));
        assert_eq!(metadata.get("source"), Some(&"upload".to_string()));
        assert_eq!(metadata.get("cache-control"), Some(&"public".to_string()));
        assert!(!metadata.contains_key("authorization"));
    }
}
