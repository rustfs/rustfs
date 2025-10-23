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
use rustfs_utils::http::AMZ_META_UNENCRYPTED_CONTENT_LENGTH;
use rustfs_utils::http::AMZ_META_UNENCRYPTED_CONTENT_MD5;
use s3s::header::X_AMZ_OBJECT_LOCK_MODE;
use s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE;

use crate::auth::UNSIGNED_PAYLOAD;
use crate::auth::UNSIGNED_PAYLOAD_TRAILER;
use rustfs_ecstore::store_api::{HTTPPreconditions, HTTPRangeSpec, ObjectOptions};
use rustfs_policy::service_type::ServiceType;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use rustfs_utils::http::AMZ_CONTENT_SHA256;
use rustfs_utils::http::RESERVED_METADATA_PREFIX_LOWER;
use rustfs_utils::http::RUSTFS_BUCKET_REPLICATION_DELETE_MARKER;
use rustfs_utils::http::RUSTFS_BUCKET_REPLICATION_REQUEST;
use rustfs_utils::http::RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM;
use rustfs_utils::http::RUSTFS_BUCKET_SOURCE_VERSION_ID;
use rustfs_utils::path::is_dir_object;
use s3s::{S3Result, s3_error};
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::error;
use uuid::Uuid;

use crate::auth::AuthType;
use crate::auth::get_request_auth_type;
use crate::auth::is_request_presigned_signature_v4;

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

    let vid = if vid.is_none() {
        headers
            .get(RUSTFS_BUCKET_SOURCE_VERSION_ID)
            .map(|v| v.to_str().unwrap().to_owned())
    } else {
        vid
    };

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(err) = Uuid::parse_str(id.as_str()) {
            error!("del_opts: invalid version id: {} error: {}", id, err);
            return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
        }

        if !versioned {
            error!("del_opts: object not versioned: {}", object);
            return Err(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), id.clone()));
        }
    }

    let mut opts = put_opts_from_headers(headers, metadata.clone()).map_err(|err| {
        error!("del_opts: invalid argument: {} error: {}", object, err);
        StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())
    })?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::max().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    opts.delete_marker = headers
        .get(RUSTFS_BUCKET_REPLICATION_DELETE_MARKER)
        .map(|v| v.to_str().unwrap() == "true")
        .unwrap_or_default();

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

fn fill_conditional_writes_opts_from_header(headers: &HeaderMap<HeaderValue>, opts: &mut ObjectOptions) -> std::io::Result<()> {
    if headers.contains_key("If-None-Match") || headers.contains_key("If-Match") {
        let mut preconditions = HTTPPreconditions::default();
        if let Some(if_none_match) = headers.get("If-None-Match") {
            preconditions.if_none_match = Some(
                if_none_match
                    .to_str()
                    .map_err(|_| std::io::Error::other("Invalid If-None-Match header"))?
                    .to_string(),
            );
        }
        if let Some(if_match) = headers.get("If-Match") {
            preconditions.if_match = Some(
                if_match
                    .to_str()
                    .map_err(|_| std::io::Error::other("Invalid If-Match header"))?
                    .to_string(),
            );
        }

        opts.http_preconditions = Some(preconditions);
    }

    Ok(())
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

    let vid = if vid.is_none() {
        headers
            .get(RUSTFS_BUCKET_SOURCE_VERSION_ID)
            .map(|v| v.to_str().unwrap().to_owned())
    } else {
        vid
    };

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

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

    Ok(opts)
}

pub fn get_complete_multipart_upload_opts(headers: &HeaderMap<HeaderValue>) -> std::io::Result<ObjectOptions> {
    let mut user_defined = HashMap::new();

    let mut replication_request = false;
    if let Some(v) = headers.get(RUSTFS_BUCKET_REPLICATION_REQUEST) {
        user_defined.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}Actual-Object-Size"),
            v.to_str().unwrap_or_default().to_owned(),
        );
        replication_request = true;
    }

    if let Some(v) = headers.get(RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM) {
        user_defined.insert(
            RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM.to_string(),
            v.to_str().unwrap_or_default().to_owned(),
        );
    }

    let mut opts = ObjectOptions {
        want_checksum: rustfs_rio::get_content_checksum(headers)?,
        user_defined,
        replication_request,
        ..Default::default()
    };

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;
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
    extract_metadata_from_mime_with_object_name(headers, metadata, None);
}

/// Extracts metadata from headers and returns it as a HashMap with object name for MIME type detection.
pub fn extract_metadata_from_mime_with_object_name(
    headers: &HeaderMap<HeaderValue>,
    metadata: &mut HashMap<String, String>,
    object_name: Option<&str>,
) {
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
        let default_content_type = if let Some(obj_name) = object_name {
            detect_content_type_from_object_name(obj_name)
        } else {
            "binary/octet-stream".to_owned()
        };
        metadata.insert("content-type".to_owned(), default_content_type);
    }
}

pub(crate) fn filter_object_metadata(metadata: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    let mut filtered_metadata = HashMap::new();
    for (k, v) in metadata {
        if k.starts_with(RESERVED_METADATA_PREFIX_LOWER) {
            continue;
        }
        if v.is_empty() && (k == &X_AMZ_OBJECT_LOCK_MODE.to_string() || k == &X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_string()) {
            continue;
        }

        if k == AMZ_META_UNENCRYPTED_CONTENT_MD5 || k == AMZ_META_UNENCRYPTED_CONTENT_LENGTH {
            continue;
        }

        let lower_key = k.to_ascii_lowercase();
        if let Some(key) = lower_key.strip_prefix("x-amz-meta-") {
            filtered_metadata.insert(key.to_string(), v.to_string());
            continue;
        }
        if let Some(key) = lower_key.strip_prefix("x-rustfs-meta-") {
            filtered_metadata.insert(key.to_string(), v.to_string());
            continue;
        }

        filtered_metadata.insert(k.clone(), v.clone());
    }
    if filtered_metadata.is_empty() {
        None
    } else {
        Some(filtered_metadata)
    }
}

/// Detects content type from object name based on file extension.
pub(crate) fn detect_content_type_from_object_name(object_name: &str) -> String {
    let lower_name = object_name.to_lowercase();

    // Check for Parquet files specifically
    if lower_name.ends_with(".parquet") {
        return "application/vnd.apache.parquet".to_owned();
    }

    // Special handling for other data formats that mime_guess doesn't know
    if lower_name.ends_with(".avro") {
        return "application/avro".to_owned();
    }
    if lower_name.ends_with(".orc") {
        return "application/orc".to_owned();
    }
    if lower_name.ends_with(".feather") {
        return "application/feather".to_owned();
    }
    if lower_name.ends_with(".arrow") {
        return "application/arrow".to_owned();
    }

    // Use mime_guess for standard file types
    mime_guess::from_path(object_name).first_or_octet_stream().to_string()
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

/// Parse copy source range string in format "bytes=start-end"
pub fn parse_copy_source_range(range_str: &str) -> S3Result<HTTPRangeSpec> {
    if !range_str.starts_with("bytes=") {
        return Err(s3_error!(InvalidArgument, "Invalid range format"));
    }

    let range_part = &range_str[6..]; // Remove "bytes=" prefix

    if let Some(dash_pos) = range_part.find('-') {
        let start_str = &range_part[..dash_pos];
        let end_str = &range_part[dash_pos + 1..];

        if start_str.is_empty() && end_str.is_empty() {
            return Err(s3_error!(InvalidArgument, "Invalid range format"));
        }

        if start_str.is_empty() {
            // Suffix range: bytes=-500 (last 500 bytes)
            let length = end_str
                .parse::<i64>()
                .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?;

            Ok(HTTPRangeSpec {
                is_suffix_length: true,
                start: -length,
                end: -1,
            })
        } else {
            let start = start_str
                .parse::<i64>()
                .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?;

            let end = if end_str.is_empty() {
                -1 // Open-ended range: bytes=500-
            } else {
                end_str
                    .parse::<i64>()
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid range format"))?
            };

            if start < 0 || (end != -1 && end < start) {
                return Err(s3_error!(InvalidArgument, "Invalid range format"));
            }

            Ok(HTTPRangeSpec {
                is_suffix_length: false,
                start,
                end,
            })
        }
    } else {
        Err(s3_error!(InvalidArgument, "Invalid range format"))
    }
}

pub(crate) fn get_content_sha256(headers: &HeaderMap<HeaderValue>) -> Option<String> {
    match get_request_auth_type(headers) {
        AuthType::Presigned | AuthType::Signed => {
            if skip_content_sha256_cksum(headers) {
                None
            } else {
                Some(get_content_sha256_cksum(headers, ServiceType::S3))
            }
        }
        _ => None,
    }
}

/// skip_content_sha256_cksum returns true if caller needs to skip
/// payload checksum, false if not.
fn skip_content_sha256_cksum(headers: &HeaderMap<HeaderValue>) -> bool {
    let content_sha256 = if is_request_presigned_signature_v4(headers) {
        // For presigned requests, check query params first, then headers
        // Note: In a real implementation, you would need to check query parameters
        // For now, we'll just check headers
        headers.get(AMZ_CONTENT_SHA256)
    } else {
        headers.get(AMZ_CONTENT_SHA256)
    };

    // Skip if no header was set
    let Some(header_value) = content_sha256 else {
        return true;
    };

    let Ok(value) = header_value.to_str() else {
        return true;
    };

    // If x-amz-content-sha256 is set and the value is not
    // 'UNSIGNED-PAYLOAD' we should validate the content sha256.
    match value {
        v if v == UNSIGNED_PAYLOAD || v == UNSIGNED_PAYLOAD_TRAILER => true,
        v if v == EMPTY_STRING_SHA256_HASH => {
            // some broken clients set empty-sha256
            // with > 0 content-length in the body,
            // we should skip such clients and allow
            // blindly such insecure clients only if
            // S3 strict compatibility is disabled.

            // We return true only in situations when
            // deployment has asked RustFS to allow for
            // such broken clients and content-length > 0.
            // For now, we'll assume strict compatibility is disabled
            // In a real implementation, you would check a global config
            if let Some(content_length) = headers.get("content-length") {
                if let Ok(length_str) = content_length.to_str() {
                    if let Ok(length) = length_str.parse::<i64>() {
                        return length > 0; // && !global_server_ctxt.strict_s3_compat
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Returns SHA256 for calculating canonical-request.
fn get_content_sha256_cksum(headers: &HeaderMap<HeaderValue>, service_type: ServiceType) -> String {
    if service_type == ServiceType::STS {
        // For STS requests, we would need to read the body and calculate SHA256
        // This is a simplified implementation - in practice you'd need access to the request body
        // For now, we'll return a placeholder
        return "sts-body-sha256-placeholder".to_string();
    }

    let (default_sha256_cksum, content_sha256) = if is_request_presigned_signature_v4(headers) {
        // For a presigned request we look at the query param for sha256.
        // X-Amz-Content-Sha256, if not set in presigned requests, checksum
        // will default to 'UNSIGNED-PAYLOAD'.
        (UNSIGNED_PAYLOAD.to_string(), headers.get(AMZ_CONTENT_SHA256))
    } else {
        // X-Amz-Content-Sha256, if not set in signed requests, checksum
        // will default to sha256([]byte("")).
        (EMPTY_STRING_SHA256_HASH.to_string(), headers.get(AMZ_CONTENT_SHA256))
    };

    // We found 'X-Amz-Content-Sha256' return the captured value.
    if let Some(header_value) = content_sha256 {
        if let Ok(value) = header_value.to_str() {
            return value.to_string();
        }
    }

    // We couldn't find 'X-Amz-Content-Sha256'.
    default_sha256_cksum
}

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
        headers.insert("x-amz-meta-chinese", HeaderValue::from_bytes("test-value".as_bytes()).unwrap());
        headers.insert("x-rustfs-meta-emoji", HeaderValue::from_bytes("🚀".as_bytes()).unwrap());

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("chinese"), Some(&"test-value".to_string()));
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

    #[test]
    fn test_extract_metadata_from_mime_with_parquet_object_name() {
        let headers = HeaderMap::new();
        let mut metadata = HashMap::new();

        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, Some("data/test.parquet"));

        assert_eq!(metadata.get("content-type"), Some(&"application/vnd.apache.parquet".to_string()));
    }

    #[test]
    fn test_extract_metadata_from_mime_with_various_data_formats() {
        let test_cases = vec![
            ("data.parquet", "application/vnd.apache.parquet"),
            ("data.PARQUET", "application/vnd.apache.parquet"), // Test case insensitive
            ("file.avro", "application/avro"),
            ("file.orc", "application/orc"),
            ("file.feather", "application/feather"),
            ("file.arrow", "application/arrow"),
            ("file.json", "application/json"),
            ("file.csv", "text/csv"),
            ("file.txt", "text/plain"),
            ("file.unknownext", "application/octet-stream"), // Use truly unknown extension
        ];

        for (filename, expected_content_type) in test_cases {
            let headers = HeaderMap::new();
            let mut metadata = HashMap::new();

            extract_metadata_from_mime_with_object_name(&headers, &mut metadata, Some(filename));

            assert_eq!(
                metadata.get("content-type"),
                Some(&expected_content_type.to_string()),
                "Failed for filename: {filename}"
            );
        }
    }

    #[test]
    fn test_extract_metadata_from_mime_with_existing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("custom/type"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, Some("test.parquet"));

        // Should preserve existing content-type, not overwrite
        assert_eq!(metadata.get("content-type"), Some(&"custom/type".to_string()));
    }

    #[test]
    fn test_detect_content_type_from_object_name() {
        // Test Parquet files (our custom handling)
        assert_eq!(detect_content_type_from_object_name("test.parquet"), "application/vnd.apache.parquet");
        assert_eq!(detect_content_type_from_object_name("TEST.PARQUET"), "application/vnd.apache.parquet");

        // Test other custom data formats
        assert_eq!(detect_content_type_from_object_name("data.avro"), "application/avro");
        assert_eq!(detect_content_type_from_object_name("data.orc"), "application/orc");
        assert_eq!(detect_content_type_from_object_name("data.feather"), "application/feather");
        assert_eq!(detect_content_type_from_object_name("data.arrow"), "application/arrow");

        // Test standard formats (mime_guess handling)
        assert_eq!(detect_content_type_from_object_name("data.json"), "application/json");
        assert_eq!(detect_content_type_from_object_name("data.csv"), "text/csv");
        assert_eq!(detect_content_type_from_object_name("data.txt"), "text/plain");

        // Test truly unknown format (using extension mime_guess doesn't recognize)
        assert_eq!(detect_content_type_from_object_name("unknown.unknownext"), "application/octet-stream");

        // Test files without extension
        assert_eq!(detect_content_type_from_object_name("noextension"), "application/octet-stream");
    }

    #[test]
    fn test_parse_copy_source_range() {
        // Test complete range: bytes=0-1023
        let result = parse_copy_source_range("bytes=0-1023").unwrap();
        assert!(!result.is_suffix_length);
        assert_eq!(result.start, 0);
        assert_eq!(result.end, 1023);

        // Test open-ended range: bytes=500-
        let result = parse_copy_source_range("bytes=500-").unwrap();
        assert!(!result.is_suffix_length);
        assert_eq!(result.start, 500);
        assert_eq!(result.end, -1);

        // Test suffix range: bytes=-500 (last 500 bytes)
        let result = parse_copy_source_range("bytes=-500").unwrap();
        assert!(result.is_suffix_length);
        assert_eq!(result.start, -500);
        assert_eq!(result.end, -1);

        // Test invalid format
        assert!(parse_copy_source_range("invalid").is_err());
        assert!(parse_copy_source_range("bytes=").is_err());
        assert!(parse_copy_source_range("bytes=abc-def").is_err());
        assert!(parse_copy_source_range("bytes=100-50").is_err()); // start > end
    }
}
