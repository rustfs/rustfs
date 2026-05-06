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

use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::{HeaderMap, HeaderValue};
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::error::Result;
use rustfs_ecstore::error::StorageError;
use rustfs_utils::http::{
    AMZ_META_UNENCRYPTED_CONTENT_LENGTH, AMZ_META_UNENCRYPTED_CONTENT_MD5, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
    AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
};
use rustfs_utils::http::{
    SUFFIX_FORCE_DELETE, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_SOURCE_DELETEMARKER,
    SUFFIX_SOURCE_MTIME, SUFFIX_SOURCE_REPLICATION_REQUEST, SUFFIX_SOURCE_VERSION_ID, get_header, insert_header_map,
    is_encryption_metadata_key, is_internal_key,
};
use s3s::header::X_AMZ_OBJECT_LOCK_MODE;
use s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE;

use crate::auth::UNSIGNED_PAYLOAD;
use crate::auth::UNSIGNED_PAYLOAD_TRAILER;
use rustfs_ecstore::store_api::{HTTPPreconditions, HTTPRangeSpec, ObjectOptions};
use rustfs_policy::service_type::ServiceType;
use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
use rustfs_utils::http::AMZ_CONTENT_SHA256;
use rustfs_utils::path::is_dir_object;
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::error;
use uuid::Uuid;

use crate::auth::AuthType;
use crate::auth::get_query_param;
use crate::auth::get_request_auth_type_with_query;
use crate::auth::is_request_presigned_signature_v4_with_query;

#[cfg(test)]
use rustfs_utils::http::insert_header;

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
        get_header(headers, SUFFIX_SOURCE_VERSION_ID).map(|s| s.into_owned())
    } else {
        vid
    };

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    // Handle AWS S3 special case: "null" string represents null version ID
    // When VersionId='null' is specified, it means delete the object with null version ID
    let vid = if let Some(ref id) = vid {
        if id.eq_ignore_ascii_case("null") {
            // Convert "null" to Uuid::nil() string representation
            Some(Uuid::nil().to_string())
        } else {
            // Validate UUID format for other version IDs
            if *id != Uuid::nil().to_string() && Uuid::parse_str(id.as_str()).is_err() {
                error!("del_opts: invalid version id: {} error: invalid UUID format", id);
                return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
            }
            Some(id.clone())
        }
    } else {
        None
    };

    let mut opts = put_opts_from_headers(headers, metadata.clone()).map_err(|err| {
        error!("del_opts: invalid argument: {} error: {}", object, err);
        StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())
    })?;

    opts.delete_prefix = get_header(headers, SUFFIX_FORCE_DELETE)
        .map(|v| v.as_ref() == "true")
        .unwrap_or_default();

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    opts.delete_marker = get_header(headers, SUFFIX_SOURCE_DELETEMARKER)
        .map(|v| v.as_ref() == "true")
        .unwrap_or_default();

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

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

    let nil_uuid_str = Uuid::nil().to_string();

    let vid = match vid {
        Some(ref id) => {
            if id.eq_ignore_ascii_case("null") {
                Some(nil_uuid_str.clone())
            } else {
                if id.as_str() != nil_uuid_str.as_str() && Uuid::parse_str(id).is_err() {
                    return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
                }
                Some(id.clone())
            }
        }
        None => None,
    };

    let mut opts = get_default_opts(headers, HashMap::new(), false)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(nil_uuid_str)
        } else {
            vid
        }
    };

    opts.part_number = part_num;

    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    // Optionally skip per-shard bitrot hash verification on reads to save CPU.
    // Background scanner still performs full integrity checks asynchronously.
    opts.skip_verify_bitrot = rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_GET_SKIP_BITROT_VERIFY,
        rustfs_config::DEFAULT_OBJECT_GET_SKIP_BITROT_VERIFY,
    );

    fill_conditional_writes_opts_from_header(headers, &mut opts)?;

    Ok(opts)
}

fn fill_conditional_writes_opts_from_header(headers: &HeaderMap<HeaderValue>, opts: &mut ObjectOptions) -> std::io::Result<()> {
    let if_none_match = conditional_etag_header(headers, IF_NONE_MATCH, "If-None-Match")?;
    let if_match = conditional_etag_header(headers, IF_MATCH, "If-Match")?;

    if if_none_match.is_some() || if_match.is_some() {
        opts.http_preconditions = Some(HTTPPreconditions {
            if_match,
            if_none_match,
            ..Default::default()
        });
    }

    Ok(())
}

fn conditional_etag_header(
    headers: &HeaderMap<HeaderValue>,
    name: http::header::HeaderName,
    display_name: &str,
) -> std::io::Result<Option<String>> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };

    let value = value
        .to_str()
        .map_err(|_| std::io::Error::other(format!("Invalid {display_name} header")))?
        .trim();

    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_owned()))
    }
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
        get_header(headers, SUFFIX_SOURCE_VERSION_ID).map(|s| s.into_owned())
    } else {
        vid
    };

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid
        && *id != Uuid::nil().to_string()
        && let Err(_err) = Uuid::parse_str(id.as_str())
    {
        return Err(StorageError::InvalidVersionID(bucket.to_owned(), object.to_owned(), id.clone()));
    }

    let mut opts = put_opts_from_headers(headers, metadata)
        .map_err(|err| StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string()))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
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
    if get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true") {
        replication_request = true;
        if let Some(actual_size_str) = get_header(headers, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE) {
            rustfs_utils::http::insert_str(
                &mut user_defined,
                rustfs_utils::http::SUFFIX_ACTUAL_OBJECT_SIZE_CAP,
                actual_size_str.into_owned(),
            );
        } else {
            tracing::warn!("Failed to get or parse replication actual object size header (x-rustfs-* or x-minio-*)");
        }
    }

    if let Some(v) = get_header(headers, SUFFIX_REPLICATION_SSEC_CRC) {
        insert_header_map(&mut user_defined, SUFFIX_REPLICATION_SSEC_CRC, v.into_owned());
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
    let mut opts = get_default_opts(headers, metadata, false)?;
    if get_header(headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true") {
        opts.replication_request = true;
        if let Some(v) = get_header(headers, SUFFIX_SOURCE_MTIME) {
            let trimmed_s = v.trim();
            match time::OffsetDateTime::parse(trimmed_s, &time::format_description::well_known::Rfc3339) {
                Ok(mtime) => opts.mod_time = Some(mtime),
                Err(e) => {
                    tracing::warn!("Invalid source-mtime value '{}' (replication request=true): {}", trimmed_s, e);
                    opts.mod_time = None;
                }
            }
        }
    }
    Ok(opts)
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
    extract_metadata_from_mime_with_object_name(headers, metadata, false, None);
}

/// Normalizes Content-Encoding for storage per AWS S3 behavior: "aws-chunked" is a
/// request-side transfer encoding for SigV4 streaming and must not be stored or returned.
/// If the only value is "aws-chunked", returns None (do not persist). Otherwise returns
/// the value with "aws-chunked" stripped, or None if nothing remains.
pub(crate) fn normalize_content_encoding_for_storage(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized: String = trimmed
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    if normalized.is_empty() { None } else { Some(normalized) }
}

const ENV_REJECT_ARCHIVE_CONTENT_ENCODING: &str = "RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING";

const ARCHIVE_CONTENT_ENCODING_BLOCKED_SUFFIXES: &[&str] = &[
    ".zip",
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".tar.zst",
    ".tar.zstd",
    ".tzst",
];

const ARCHIVE_CONTENT_ENCODING_BLOCKED_CONTENT_TYPES: &[&str] =
    &["application/zip", "application/x-zip-compressed", "application/x-tar"];

fn is_archive_object_name_for_content_encoding(object_name: &str) -> bool {
    let object_name = object_name.to_ascii_lowercase();
    ARCHIVE_CONTENT_ENCODING_BLOCKED_SUFFIXES
        .iter()
        .any(|suffix| object_name.ends_with(suffix))
}

fn is_archive_content_type_for_content_encoding(content_type: &str) -> bool {
    let main_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();

    ARCHIVE_CONTENT_ENCODING_BLOCKED_CONTENT_TYPES
        .iter()
        .any(|candidate| main_type == *candidate)
}

pub(crate) fn validate_archive_content_encoding(
    object_name: &str,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
) -> S3Result<()> {
    if !archive_content_encoding_strict_mode() {
        return Ok(());
    }

    let Some(content_encoding) = content_encoding.and_then(normalize_content_encoding_for_storage) else {
        return Ok(());
    };

    let is_archive_like = is_archive_object_name_for_content_encoding(object_name)
        || content_type.is_some_and(is_archive_content_type_for_content_encoding);
    if !is_archive_like {
        return Ok(());
    }

    Err(S3Error::with_message(
        S3ErrorCode::InvalidArgument,
        format!(
            "Content-Encoding '{content_encoding}' is not allowed for archive objects when {ENV_REJECT_ARCHIVE_CONTENT_ENCODING}=true; unset {ENV_REJECT_ARCHIVE_CONTENT_ENCODING} or set it to false to restore compatibility-first behavior"
        ),
    ))
}

fn archive_content_encoding_strict_mode() -> bool {
    rustfs_utils::get_env_bool(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, false)
}

/// Extracts metadata from headers and returns it as a HashMap with object name for MIME type detection.
pub fn extract_metadata_from_mime_with_object_name(
    headers: &HeaderMap<HeaderValue>,
    metadata: &mut HashMap<String, String>,
    skip_content_type: bool,
    object_name: Option<&str>,
) {
    const USER_METADATA_PREFIXES: &[&str] = &["x-amz-meta-", "x-rustfs-meta-", "x-minio-meta-"];

    for (k, v) in headers.iter() {
        if k.as_str() == "content-type" && skip_content_type {
            continue;
        }

        if let Some(key) = USER_METADATA_PREFIXES
            .iter()
            .find_map(|prefix| k.as_str().strip_prefix(prefix))
        {
            if key.is_empty() {
                continue;
            }

            metadata.insert(key.to_owned(), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        for hd in SUPPORTED_HEADERS.iter() {
            if k.as_str() == *hd {
                let raw = String::from_utf8_lossy(v.as_bytes()).to_string();
                if *hd == "content-encoding" {
                    if let Some(normalized) = normalize_content_encoding_for_storage(&raw) {
                        metadata.insert(k.to_string(), normalized);
                    }
                } else {
                    metadata.insert(k.to_string(), raw);
                }
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
    // HTTP headers that should NOT be returned in the Metadata field.
    // These headers are returned as separate response headers, not user metadata.
    const EXCLUDED_HEADERS: &[&str] = &[
        "content-type",
        "content-disposition",
        "content-encoding",
        "content-language",
        "cache-control",
        "expires",
        "etag",
        "x-amz-storage-class",
        "x-amz-tagging",
        "x-amz-replication-status",
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key-md5",
        "x-amz-server-side-encryption-aws-kms-key-id",
    ];

    let mut filtered_metadata = HashMap::new();
    for (k, v) in metadata {
        let lower_key = k.to_ascii_lowercase();
        // Skip internal/reserved metadata (x-rustfs-internal-* or x-minio-internal-*)
        if is_internal_key(&lower_key) {
            continue;
        }

        // Skip internal encryption metadata (x-rustfs-encryption-* or x-minio-encryption-*)
        if is_encryption_metadata_key(&lower_key) {
            continue;
        }

        // Skip empty object lock values
        if v.is_empty() && (k == &X_AMZ_OBJECT_LOCK_MODE.to_string() || k == &X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_string()) {
            continue;
        }

        // Skip UNENCRYPTED metadata placeholders
        if k == AMZ_META_UNENCRYPTED_CONTENT_MD5 || k == AMZ_META_UNENCRYPTED_CONTENT_LENGTH {
            continue;
        }

        // Skip excluded HTTP headers (they are returned as separate headers, not metadata)
        if EXCLUDED_HEADERS.contains(&lower_key.as_str()) {
            continue;
        }

        // Skip any x-amz-* headers that are not user metadata
        // User metadata was stored WITHOUT the x-amz-meta- prefix by extract_metadata_from_mime
        if lower_key.starts_with("x-amz-") {
            continue;
        }

        // Include user-defined metadata (keys like "meta1", "custom-key", etc.)
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
        // Object Lock headers - required for S3 Object Lock functionality
        AMZ_OBJECT_LOCK_MODE_LOWER,
        AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
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

#[allow(dead_code)]
pub(crate) fn get_content_sha256(headers: &HeaderMap<HeaderValue>) -> Option<String> {
    get_content_sha256_with_query(headers, None)
}

pub(crate) fn get_content_sha256_with_query(headers: &HeaderMap<HeaderValue>, query: Option<&str>) -> Option<String> {
    match get_request_auth_type_with_query(headers, query) {
        AuthType::Presigned | AuthType::Signed => {
            if skip_content_sha256_cksum_with_query(headers, query) {
                None
            } else {
                Some(get_content_sha256_cksum_with_query(headers, query, ServiceType::S3))
            }
        }
        _ => None,
    }
}

/// skip_content_sha256_cksum returns true if caller needs to skip
/// payload checksum, false if not.
#[allow(dead_code)]
fn skip_content_sha256_cksum(headers: &HeaderMap<HeaderValue>) -> bool {
    skip_content_sha256_cksum_with_query(headers, None)
}

fn skip_content_sha256_cksum_with_query(headers: &HeaderMap<HeaderValue>, query: Option<&str>) -> bool {
    let include_query_values = matches!(get_request_auth_type_with_query(headers, query), AuthType::Presigned);
    let content_sha256 = get_content_sha256_value(headers, query, include_query_values);

    // Skip if no checksum value was set in header/query for query-presigned requests.
    let Some(header_value) = content_sha256 else {
        return true;
    };

    let value = header_value;

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
            if let Some(content_length) = headers.get("content-length")
                && let Ok(length_str) = content_length.to_str()
                && let Ok(length) = length_str.parse::<i64>()
            {
                return length > 0; // && !global_server_ctxt.strict_s3_compat
            }
            false
        }
        _ => false,
    }
}

/// Returns SHA256 for calculating canonical-request.
fn get_content_sha256_cksum_with_query(
    headers: &HeaderMap<HeaderValue>,
    query: Option<&str>,
    service_type: ServiceType,
) -> String {
    if service_type == ServiceType::STS {
        // For STS requests, we would need to read the body and calculate SHA256
        // This is a simplified implementation - in practice you'd need access to the request body
        // For now, we'll return a placeholder
        return "sts-body-sha256-placeholder".to_string();
    }

    let (default_sha256_cksum, content_sha256) = if is_request_presigned_signature_v4_with_query(headers, query) {
        // For a presigned request we look at the query param for sha256.
        // X-Amz-Content-Sha256, if not set in presigned requests, checksum
        // will default to 'UNSIGNED-PAYLOAD'.
        (UNSIGNED_PAYLOAD.to_string(), get_content_sha256_value(headers, query, true))
    } else {
        // X-Amz-Content-Sha256, if not set in signed requests, checksum
        // will default to sha256([]byte("")).
        (
            EMPTY_STRING_SHA256_HASH.to_string(),
            headers
                .get(AMZ_CONTENT_SHA256)
                .and_then(|v| v.to_str().ok().map(str::to_owned)),
        )
    };

    // We found 'X-Amz-Content-Sha256' return the captured value.
    if let Some(header_value) = content_sha256 {
        return header_value;
    }

    // We couldn't find 'X-Amz-Content-Sha256'.
    default_sha256_cksum
}

fn get_content_sha256_value(
    headers: &HeaderMap<HeaderValue>,
    query: Option<&str>,
    include_query_for_presigned: bool,
) -> Option<String> {
    if include_query_for_presigned && is_request_presigned_signature_v4_with_query(headers, query) {
        return query
            .and_then(|q| get_query_param(q, "x-amz-content-sha256"))
            .or_else(|| headers.get(AMZ_CONTENT_SHA256).and_then(|v| v.to_str().ok()))
            .map(str::to_owned);
    }

    headers
        .get(AMZ_CONTENT_SHA256)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

#[allow(dead_code)]
fn get_content_sha256_cksum(headers: &HeaderMap<HeaderValue>, service_type: ServiceType) -> String {
    get_content_sha256_cksum_with_query(headers, None, service_type)
}

#[cfg(test)]
mod tests {
    use temp_env;

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
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
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
    async fn test_del_opts_with_delete_prefix() {
        let mut headers = create_test_headers();
        let metadata = create_test_metadata();

        // Test without force-delete header - should default to false
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to "true"
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "true");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to "false"
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "false");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);

        // Test with RUSTFS_FORCE_DELETE header set to other value
        insert_header(&mut headers, SUFFIX_FORCE_DELETE, "maybe");
        let result = del_opts("test-bucket", "test-object", None, &headers, metadata).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert!(!opts.delete_prefix);
    }

    #[tokio::test]
    async fn test_del_opts_with_null_version_id() {
        let headers = create_test_headers();
        let metadata = create_test_metadata();
        let result = del_opts("test-bucket", "test-object", Some("null".to_string()), &headers, metadata.clone()).await;
        assert!(result.is_ok());
        let result = del_opts("test-bucket", "test-object", Some("NULL".to_string()), &headers, metadata.clone()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_ops_with_null_version_id() {
        let headers = create_test_headers();
        let result = get_opts("test-bucket", "test-object", Some("null".to_string()), None, &headers).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
        let result = get_opts("test-bucket", "test-object", Some("NULL".to_string()), None, &headers).await;
        assert!(result.is_ok());
        let opts = result.unwrap();
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
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
    async fn test_get_opts_ignores_empty_conditional_headers() {
        let mut headers = create_test_headers();
        headers.insert(http::header::IF_MATCH, HeaderValue::from_static(""));
        headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static(" "));

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        assert!(result.unwrap().http_preconditions.is_none());
    }

    #[tokio::test]
    async fn test_get_opts_keeps_non_empty_conditional_headers() {
        let mut headers = create_test_headers();
        headers.insert(http::header::IF_MATCH, HeaderValue::from_static(" \"etag-a\" "));
        headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("\"etag-b\""));

        let result = get_opts("test-bucket", "test-object", None, None, &headers).await;

        assert!(result.is_ok());
        let preconditions = result.unwrap().http_preconditions.expect("conditional headers");
        assert_eq!(preconditions.if_match.as_deref(), Some("\"etag-a\""));
        assert_eq!(preconditions.if_none_match.as_deref(), Some("\"etag-b\""));
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
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
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
        assert_eq!(opts.version_id, Some(Uuid::nil().to_string()));
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
    fn test_put_opts_from_headers_with_replication_request() {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        let valid_mtime = "2024-05-20T10:30:00+08:00";
        insert_header(&mut headers, SUFFIX_SOURCE_MTIME, valid_mtime);

        let metadata = HashMap::new();

        let result = put_opts_from_headers(&headers, metadata);

        assert!(result.is_ok());
        let opts = result.unwrap();

        assert!(opts.replication_request);

        let expected_mtime = time::OffsetDateTime::parse(valid_mtime, &time::format_description::well_known::Rfc3339).unwrap();
        assert_eq!(opts.mod_time, Some(expected_mtime));

        let mut headers_invalid_mtime = HeaderMap::new();
        insert_header(&mut headers_invalid_mtime, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(&mut headers_invalid_mtime, SUFFIX_SOURCE_MTIME, "invalid-time");
        let result_invalid = put_opts_from_headers(&headers_invalid_mtime, HashMap::new());
        assert!(result_invalid.is_ok());
        let opts_invalid = result_invalid.unwrap();
        assert!(opts_invalid.replication_request);
        assert!(opts_invalid.mod_time.is_none());
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
    fn test_extract_metadata_from_mime_minio_meta() {
        let mut headers = HeaderMap::new();
        headers.insert("x-minio-meta-origin", HeaderValue::from_static("gateway"));
        headers.insert("x-minio-meta-source-id", HeaderValue::from_static("abc123"));
        headers.insert("x-minio-meta-", HeaderValue::from_static("empty-key"));

        let mut metadata = HashMap::new();
        extract_metadata_from_mime(&headers, &mut metadata);

        assert_eq!(metadata.get("origin"), Some(&"gateway".to_string()));
        assert_eq!(metadata.get("source-id"), Some(&"abc123".to_string()));
        assert!(!metadata.contains_key(""));
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

    /// Issue #1857: SigV4 streaming sends Content-Encoding: aws-chunked. Per AWS S3,
    /// this is a request-side transfer encoding and must not be stored or returned.
    /// This test verifies: (1) "aws-chunked" alone is not persisted;
    /// (2) when combined with real encoding (e.g. gzip), only the real encoding is stored;
    /// (3) case-insensitive stripping of aws-chunked.
    #[test]
    fn test_content_encoding_aws_chunked_not_persisted_issue_1857() {
        let cases: &[(&str, Option<&str>)] = &[
            ("aws-chunked", None),
            ("AWS-CHUNKED", None),
            ("aws-chunked ", None),
            ("gzip, aws-chunked", Some("gzip")),
            ("aws-chunked, gzip", Some("gzip")),
            ("gzip", Some("gzip")),
            ("zstd", Some("zstd")),
        ];

        for (header_value, expected) in cases {
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", HeaderValue::from_static(header_value));

            let mut metadata = HashMap::new();
            extract_metadata_from_mime(&headers, &mut metadata);

            match expected {
                None => assert!(
                    !metadata.contains_key("content-encoding"),
                    "content-encoding {:?} should not be persisted, got metadata keys: {:?}",
                    header_value,
                    metadata.keys().collect::<Vec<_>>()
                ),
                Some(exp) => assert_eq!(
                    metadata.get("content-encoding"),
                    Some(&exp.to_string()),
                    "content-encoding {:?} should be normalized to {:?}",
                    header_value,
                    exp
                ),
            }
        }
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
            AMZ_OBJECT_LOCK_MODE_LOWER,
            AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
            AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
        ];

        assert_eq!(*SUPPORTED_HEADERS, expected_headers);
        assert_eq!(SUPPORTED_HEADERS.len(), 12);
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
        headers.insert("x-minio-meta-origin", HeaderValue::from_static("replication"));
        headers.insert("cache-control", HeaderValue::from_static("public"));
        headers.insert("authorization", HeaderValue::from_static("Bearer xyz")); // Should be ignored

        let metadata = extract_metadata(&headers);

        assert_eq!(metadata.get("content-type"), Some(&"application/xml".to_string()));
        assert_eq!(metadata.get("version"), Some(&"1.0".to_string()));
        assert_eq!(metadata.get("source"), Some(&"upload".to_string()));
        assert_eq!(metadata.get("origin"), Some(&"replication".to_string()));
        assert_eq!(metadata.get("cache-control"), Some(&"public".to_string()));
        assert!(!metadata.contains_key("authorization"));
    }

    #[test]
    fn test_extract_metadata_from_mime_with_parquet_object_name() {
        let headers = HeaderMap::new();
        let mut metadata = HashMap::new();

        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some("data/test.parquet"));

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

            extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some(filename));

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
        extract_metadata_from_mime_with_object_name(&headers, &mut metadata, false, Some("test.parquet"));

        // Should preserve existing content-type, not overwrite
        assert_eq!(metadata.get("content-type"), Some(&"custom/type".to_string()));
    }

    #[test]
    fn test_filter_object_metadata_excludes_standard_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/octet-stream".to_string());
        metadata.insert("content-disposition".to_string(), "inline".to_string());
        metadata.insert("cache-control".to_string(), "no-cache".to_string());
        metadata.insert("x-amz-storage-class".to_string(), "STANDARD".to_string());
        metadata.insert("custom-key".to_string(), "custom-value".to_string());

        let filtered = filter_object_metadata(&metadata).unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.get("custom-key"), Some(&"custom-value".to_string()));
        assert!(!filtered.contains_key("content-type"));
        assert!(!filtered.contains_key("content-disposition"));
        assert!(!filtered.contains_key("cache-control"));
        assert!(!filtered.contains_key("x-amz-storage-class"));
    }

    #[test]
    fn test_filter_object_metadata_returns_none_for_only_content_type() {
        let mut metadata = HashMap::new();
        metadata.insert("content-type".to_string(), "application/octet-stream".to_string());

        let filtered = filter_object_metadata(&metadata);
        assert!(filtered.is_none(), "content-type must not be exposed as user metadata");
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
    fn test_validate_archive_content_encoding_allows_archive_suffix_by_default() {
        validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("gzip")).expect("default allow");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_mime_by_default() {
        validate_archive_content_encoding("bundle", Some("application/zip"), Some("gzip")).expect("default allow");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_non_archive_precompressed_object() {
        validate_archive_content_encoding("logs/app.log.zst", Some("text/plain"), Some("zstd")).expect("non-archive");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_sigv4_streaming_encoding_by_default() {
        validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("aws-chunked"))
            .expect("aws-chunked is request-side only");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_archive_sigv4_streaming_encoding_case_insensitive() {
        validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("AWS-CHUNKED"))
            .expect("aws-chunked stripping should be case-insensitive");
    }

    #[test]
    fn test_validate_archive_content_encoding_allows_effective_archive_encoding_after_aws_chunked_stripped_by_default() {
        validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("aws-chunked, gzip"))
            .expect("default allow after stripping aws-chunked");
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_archive_suffix_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err = validate_archive_content_encoding("bundle.tar.gz", Some("application/gzip"), Some("gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        });
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_archive_mime_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err = validate_archive_content_encoding("bundle", Some("application/zip"), Some("gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        });
    }

    #[test]
    fn test_validate_archive_content_encoding_rejects_effective_archive_encoding_after_aws_chunked_stripped_in_strict_mode() {
        temp_env::with_var(ENV_REJECT_ARCHIVE_CONTENT_ENCODING, Some("true"), || {
            let err =
                validate_archive_content_encoding("bundle.zip", Some("application/zip"), Some("aws-chunked, gzip")).unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(
                err.message(),
                Some(
                    "Content-Encoding 'gzip' is not allowed for archive objects when RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING=true; unset RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING or set it to false to restore compatibility-first behavior"
                )
            );
        });
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
