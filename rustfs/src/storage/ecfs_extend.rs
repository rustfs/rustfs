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

use crate::config::workload_profiles::{
    RustFSBufferConfig, WorkloadProfile, get_global_buffer_config, is_buffer_profile_enabled,
};
use crate::error::ApiError;
use crate::server::cors;
use crate::storage::ecfs::ListObjectUnorderedQuery;
use http::{HeaderMap, HeaderValue, StatusCode};
use metrics::counter;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::metadata_sys::get_replication_config;
use rustfs_ecstore::bucket::object_lock::objectlock_sys;
use rustfs_ecstore::bucket::replication::ReplicationConfigurationExt;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::store_api::{BucketOptions, ObjectInfo, ObjectToDelete};
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_targets::EventName;
use rustfs_targets::arn::{TargetID, TargetIDError};
use rustfs_utils::http::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    RESERVED_METADATA_PREFIX_LOWER,
};
use s3s::dto::{
    Delimiter, LambdaFunctionConfiguration, NotificationConfigurationFilter, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, QueueConfiguration,
    TopicConfiguration,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result};
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::{format_description::FormatItem, macros::format_description};
use tracing::{debug, warn};

pub const RFC1123: &[FormatItem<'_>] =
    format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");

/// Apply bucket default Object Lock retention to object metadata if no explicit retention is set.
///
/// This function implements S3-compatible behavior where objects uploaded to a bucket with
/// default retention configuration automatically inherit the bucket's default retention policy.
/// The retention is only applied if:
/// 1. The bucket has Object Lock enabled
/// 2. The bucket has a default retention rule configured
/// 3. The object metadata does not already contain explicit retention headers
///
/// # Arguments
/// * `object_lock_config` - Optional bucket Object Lock configuration. If None, no retention is applied.
/// * `metadata` - Mutable reference to object metadata HashMap. Retention headers are inserted here.
pub(crate) fn apply_lock_retention(object_lock_config: Option<ObjectLockConfiguration>, metadata: &mut HashMap<String, String>) {
    if metadata.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER) || metadata.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER) {
        return;
    }

    let Some(config) = object_lock_config else { return };

    if config.object_lock_enabled.as_ref().map(|e| e.as_str()) != Some(ObjectLockEnabled::ENABLED) {
        return;
    }

    let Some(default_retention) = config.rule.and_then(|r| r.default_retention) else { return };
    let Some(mode) = default_retention.mode else { return };

    let now = OffsetDateTime::now_utc();
    let retain_until = match (default_retention.days, default_retention.years) {
        (Some(days), _) => now.saturating_add(time::Duration::days(days as i64)),
        (None, Some(years)) => objectlock_sys::add_years(now, years),
        _ => return,
    };

    if let Ok(date_str) = retain_until.format(&Rfc3339) {
        metadata.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), mode.as_str().to_string());
        metadata.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), date_str);
    }
}

/// Calculate adaptive buffer size with workload profile support.
///
/// This enhanced version supports different workload profiles for optimal performance
/// across various use cases (AI/ML, web workloads, secure storage, etc.).
///
/// # Arguments
/// * `file_size` - The size of the file in bytes, or -1 if unknown
/// * `profile` - Optional workload profile. If None, uses auto-detection or GeneralPurpose
///
/// # Returns
/// Optimal buffer size in bytes based on the workload profile and file size
///
/// # Examples
/// ```ignore
/// // Use general purpose profile (default)
/// let buffer_size = get_adaptive_buffer_size_with_profile(1024 * 1024, None);
///
/// // Use AI training profile for large model files
/// let buffer_size = get_adaptive_buffer_size_with_profile(
///     500 * 1024 * 1024,
///     Some(WorkloadProfile::AiTraining)
/// );
///
/// // Use secure storage profile for compliance scenarios
/// let buffer_size = get_adaptive_buffer_size_with_profile(
///     10 * 1024 * 1024,
///     Some(WorkloadProfile::SecureStorage)
/// );
/// ```
///
#[allow(dead_code)]
pub(crate) fn get_adaptive_buffer_size_with_profile(file_size: i64, profile: Option<WorkloadProfile>) -> usize {
    let config = match profile {
        Some(p) => RustFSBufferConfig::new(p),
        None => {
            // Auto-detect OS environment or use general purpose
            RustFSBufferConfig::with_auto_detect()
        }
    };

    config.get_buffer_size(file_size)
}

/// Get adaptive buffer size using global workload profile configuration.
///
/// This is the primary buffer sizing function that uses the workload profile
/// system configured at startup to provide optimal buffer sizes for different scenarios.
///
/// The function automatically selects buffer sizes based on:
/// - Configured workload profile (default: GeneralPurpose)
/// - File size characteristics
/// - Optional performance metrics collection
///
/// # Arguments
/// * `file_size` - The size of the file in bytes, or -1 if unknown
///
/// # Returns
/// Optimal buffer size in bytes based on the configured workload profile
///
/// # Performance Metrics
/// When compiled with the `metrics` feature flag, this function tracks:
/// - Buffer size distribution
/// - Selection frequency
/// - Buffer-to-file size ratios
///
/// # Examples
/// ```ignore
/// // Uses configured profile (default: GeneralPurpose)
/// let buffer_size = get_buffer_size_opt_in(file_size);
/// ```
pub(crate) fn get_buffer_size_opt_in(file_size: i64) -> usize {
    let buffer_size = if is_buffer_profile_enabled() {
        // Use globally configured workload profile (enabled by default in Phase 3)
        let config = get_global_buffer_config();
        config.get_buffer_size(file_size)
    } else {
        // Opt-out mode: Use GeneralPurpose profile for consistent behavior
        let config = RustFSBufferConfig::new(WorkloadProfile::GeneralPurpose);
        config.get_buffer_size(file_size)
    };

    // Optional performance metrics collection for monitoring and optimization
    #[cfg(feature = "metrics")]
    {
        use metrics::histogram;
        histogram!("rustfs.buffer.size.bytes").record(buffer_size as f64);
        counter!("rustfs.buffer.size.selections").increment(1);

        if file_size >= 0 {
            let ratio = buffer_size as f64 / file_size as f64;
            histogram!("rustfs.buffer.to.file.ratio").record(ratio);
        }
    }

    buffer_size
}

/// Validate object key for control characters and log special characters
///
/// This function:
/// 1. Rejects keys containing control characters (null bytes, newlines, carriage returns)
/// 2. Logs debug information for keys containing spaces, plus signs, or percent signs
///
/// The s3s library handles URL decoding, so keys are already decoded when they reach this function.
/// This validation ensures that invalid characters that could cause issues are rejected early.
pub(crate) fn validate_object_key(key: &str, operation: &str) -> S3Result<()> {
    // Validate object key doesn't contain control characters
    if key.contains(['\0', '\n', '\r']) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("Object key contains invalid control characters: {key:?}"),
        ));
    }

    // Log debug info for keys with special characters to help diagnose encoding issues
    if key.contains([' ', '+', '%']) {
        debug!("{} object with special characters in key: {:?}", operation, key);
    }

    Ok(())
}

/// Validate that 'allow-unordered' parameter is not used with a delimiter
///
/// This function:
/// 1. Checks if a delimiter is specified in the ListObjects request
/// 2. Parses the query string to check for the 'allow-unordered' parameter
/// 3. Rejects the request if both 'delimiter' and 'allow-unordered=true' are present
///
/// According to S3 compatibility requirements, unordered listing cannot be combined with
/// hierarchical directory traversal (delimited listing). This validation ensures
/// conflicting parameters are caught before processing the request.
pub(crate) fn validate_list_object_unordered_with_delimiter(
    delimiter: Option<&Delimiter>,
    query_string: Option<&str>,
) -> S3Result<()> {
    if delimiter.is_none() {
        return Ok(());
    }

    let Some(query) = query_string else {
        return Ok(());
    };

    if let Ok(params) = from_bytes::<ListObjectUnorderedQuery>(query.as_bytes())
        && params.allow_unordered.as_deref() == Some("true")
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "The allow-unordered parameter cannot be used when delimiter is specified.".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn parse_object_lock_retention(retention: Option<ObjectLockRetention>) -> S3Result<HashMap<String, String>> {
    let mut eval_metadata = HashMap::new();

    if let Some(v) = retention {
        let mode = match v.mode {
            Some(mode) => match mode.as_str() {
                ObjectLockRetentionMode::COMPLIANCE | ObjectLockRetentionMode::GOVERNANCE => mode.as_str().to_string(),
                _ => {
                    return Err(S3Error::with_message(
                        S3ErrorCode::MalformedXML,
                        "The XML you provided was not well-formed or did not validate against our published schema".to_string(),
                    ));
                }
            },
            None => String::default(),
        };

        let now = OffsetDateTime::now_utc();
        // Validate retain_until_date is in the future (S3 requirement)
        // Only validate when both mode and date are provided (not clearing retention)
        let retain_until_date = if let Some(date) = v.retain_until_date {
            let retain_until = OffsetDateTime::from(date);
            // Only validate future date when mode is set (not clearing retention)
            if !mode.is_empty() && retain_until <= now {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "The retain until date must be in the future".to_string(),
                ));
            }
            retain_until.format(&Rfc3339).unwrap()
        } else {
            String::default()
        };

        // This is intentional behavior. Empty string represents "retention cleared" which is different from "retention never set". Consistent with minio
        eval_metadata.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), mode);
        eval_metadata.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), retain_until_date);
        eval_metadata.insert(
            format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-retention-timestamp"),
            format!("{}.{:09}Z", now.format(&Rfc3339).unwrap(), now.nanosecond()),
        );
    }
    Ok(eval_metadata)
}

pub(crate) fn parse_object_lock_legal_hold(legal_hold: Option<ObjectLockLegalHold>) -> S3Result<HashMap<String, String>> {
    let mut eval_metadata = HashMap::new();
    if let Some(v) = legal_hold {
        let status = match v.status {
            Some(status) => match status.as_str() {
                ObjectLockLegalHoldStatus::OFF | ObjectLockLegalHoldStatus::ON => status.as_str().to_string(),
                _ => {
                    return Err(S3Error::with_message(
                        S3ErrorCode::MalformedXML,
                        "The XML you provided was not well-formed or did not validate against our published schema".to_string(),
                    ));
                }
            },
            None => String::default(),
        };
        let now = OffsetDateTime::now_utc();
        // This is intentional behavior. Empty string represents "status cleared" which is different from "status never set".
        eval_metadata.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), status);
        eval_metadata.insert(
            format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-legalhold-timestamp"),
            format!("{}.{:09}Z", now.format(&Rfc3339).unwrap(), now.nanosecond()),
        );
    }
    Ok(eval_metadata)
}

pub(crate) async fn validate_bucket_object_lock_enabled(bucket: &str) -> S3Result<()> {
    match metadata_sys::get_object_lock_config(bucket).await {
        Ok((cfg, _created)) => {
            if cfg.object_lock_enabled != Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    "Object Lock is not enabled for this bucket".to_string(),
                ));
            }
        }
        Err(err) => {
            if err == StorageError::ConfigNotFound {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    "Bucket is missing ObjectLockConfiguration".to_string(),
                ));
            }
            warn!("get_object_lock_config err {:?}", err);
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "Failed to get bucket ObjectLockConfiguration".to_string(),
            ));
        }
    }
    Ok(())
}

/// Validates HTTP conditional request headers for a single object according to
/// RFC 7232 (HTTP/1.1 conditional requests) and S3 API semantics.
///
/// This function evaluates the following headers, if present, in the standard
/// conditional request order:
/// - `If-Match`
/// - `If-None-Match`
/// - `If-Modified-Since`
/// - `If-Unmodified-Since`
///
/// The `headers` parameter provides the incoming HTTP request headers, and
/// `info` supplies the object's current metadata (ETag and modification time)
/// used to evaluate those preconditions.
///
/// The function returns `Ok(())` if either no relevant conditional headers are
/// set or all specified preconditions are satisfied. If any precondition fails,
/// it returns an `Err(S3Error)` with an appropriate S3 error code (for example,
/// `PreconditionFailed` for failed `If-Match` / `If-Unmodified-Since`, or
/// `NotModified` for `If-None-Match` / `If-Modified-Since` when the resource
/// has not changed), allowing the caller to translate this into the correct
/// HTTP response.
pub(crate) fn check_preconditions(headers: &HeaderMap, info: &ObjectInfo) -> S3Result<()> {
    let mod_time = info.mod_time;
    let etag = info.etag.as_deref();

    if mod_time.is_none() && etag.is_none() {
        return Ok(());
    }

    // If-Match: requires ETag to exist
    if let Some(if_match_val) = headers.get("if-match").and_then(|v| v.to_str().ok()) {
        match etag {
            Some(e) if is_etag_equal(e, if_match_val) => {}
            _ => return Err(S3Error::new(S3ErrorCode::PreconditionFailed)),
        }
    }

    // If-Unmodified-Since (only when If-Match is absent)
    if headers.get("if-match").is_none()
        && let Some(t) = mod_time
        && let Some(if_unmodified_since) = headers.get("if-unmodified-since").and_then(|v| v.to_str().ok())
        && let Ok(given_time) = time::PrimitiveDateTime::parse(if_unmodified_since, &RFC1123).map(|dt| dt.assume_utc())
        && t > given_time.add(time::Duration::seconds(1))
    {
        return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
    }

    // If-None-Match
    if let Some(if_none_match) = headers.get("if-none-match").and_then(|v| v.to_str().ok())
        && let Some(e) = etag
        && is_etag_equal(e, if_none_match)
    {
        let mut error_headers = HeaderMap::new();
        if let Ok(etag_header) = parse_etag(e) {
            error_headers.insert("etag", etag_header);
        }
        if let Some(t) = mod_time
            && let Ok(last_modified_str) = t.format(&RFC1123)
            && let Ok(last_modified_header) = HeaderValue::from_str(&last_modified_str)
        {
            error_headers.insert("last-modified", last_modified_header);
        }

        let mut s3_error = S3Error::new(S3ErrorCode::NotModified);
        s3_error.set_message("Not Modified".to_string());
        s3_error.set_status_code(StatusCode::NOT_MODIFIED);
        s3_error.set_headers(error_headers);
        return Err(s3_error);
    }

    // If-Modified-Since (only when If-None-Match is absent — semantics per RFC 7232; dates use RFC 1123 format)
    if headers.get("if-none-match").is_none()
        && let Some(t) = mod_time
        && let Some(if_modified_since) = headers.get("if-modified-since").and_then(|v| v.to_str().ok())
        && let Ok(given_time) = time::PrimitiveDateTime::parse(if_modified_since, &RFC1123).map(|dt| dt.assume_utc())
        && t < given_time.add(time::Duration::seconds(1))
    {
        let mut error_headers = HeaderMap::new();
        if let Some(e) = etag
            && let Ok(etag_header) = parse_etag(e)
        {
            error_headers.insert("etag", etag_header);
        }
        if let Ok(last_modified_str) = t.format(&RFC1123)
            && let Ok(last_modified_header) = HeaderValue::from_str(&last_modified_str)
        {
            error_headers.insert("last-modified", last_modified_header);
        }

        let mut s3_error = S3Error::new(S3ErrorCode::NotModified);
        s3_error.set_message("Not Modified".to_string());
        s3_error.set_status_code(StatusCode::NOT_MODIFIED);
        s3_error.set_headers(error_headers);

        return Err(s3_error);
    }

    Ok(())
}

/// Compares an object ETag with an ETag value from an HTTP header.
///
/// This helper implements HTTP ETag comparison semantics for headers such as
/// `If-Match` and `If-None-Match`:
/// - Supports the wildcard `*`, which matches any `object_etag`.
/// - Supports comma-separated ETag lists (e.g., `"etag1", "etag2"`), returning
///   `true` if any entry matches `object_etag`.
/// - Automatically trims surrounding whitespace and double quotes from both the
///   header entries and `object_etag` before comparison.
///
/// # Parameters
/// - `object_etag`: The ETag associated with the stored object.
/// - `header_etag`: The raw ETag header value received in the request, which may
///   be a wildcard, a single ETag, or a comma-separated list of ETags.
///
/// # Returns
/// `true` if the header value matches the object ETag according to the above
/// HTTP ETag comparison rules, otherwise `false`.
pub(crate) fn is_etag_equal(object_etag: &str, header_etag: &str) -> bool {
    let header_etag = header_etag.trim();
    if header_etag == "*" {
        return true;
    }
    header_etag
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .any(|e| e == object_etag.trim_matches('"'))
}

/// Converts an object ETag string into an HTTP `HeaderValue` for use in response headers.
///
/// This function normalizes ETag values by ensuring they are wrapped in double quotes,
/// which is the standard HTTP ETag format according to RFC 7232. If the input ETag
/// already contains quotes, they are removed before adding new quotes to avoid duplication.
///
/// # Parameters
/// - `object_etag`: The ETag string, which may or may not already be wrapped in quotes.
///   Must not be empty after removing quotes.
///
/// # Returns
/// - `Ok(HeaderValue)`: The normalized ETag value wrapped in double quotes, ready for use
///   in HTTP response headers.
/// - `Err(S3Error)`: Returns `InvalidArgument` if the ETag value is empty (after removing quotes)
///   or contains characters that cannot be represented in an HTTP header value.
///
/// # Note
/// When this function returns an error, callers should log the error but continue with
/// the operation (e.g., return NotModified without the ETag header) rather than propagating
/// the InvalidArgument error, as the original operation semantics should be preserved.
pub fn parse_etag(object_etag: &str) -> Result<HeaderValue, S3Error> {
    let etag_trimmed = object_etag.trim_matches('"');

    if etag_trimmed.trim().is_empty() {
        return Err(S3Error::with_message(S3ErrorCode::InvalidArgument, "ETag cannot be empty".to_string()));
    }

    let etag_quoted = format!("\"{}\"", etag_trimmed);

    HeaderValue::from_str(&etag_quoted).map_err(|e| {
        warn!(
            "Failed to convert ETag to HeaderValue (ETag: {}): {}. The ETag header will be omitted from the response.",
            etag_quoted, e
        );
        S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("Invalid ETag header value (ETag: {}): {}", etag_quoted, e),
        )
    })
}

/// Auxiliary functions: extract prefixes and suffixes
pub(crate) fn extract_prefix_suffix(filter: Option<&NotificationConfigurationFilter>) -> (String, String) {
    if let Some(filter) = filter
        && let Some(filter_rules) = &filter.key
    {
        let mut prefix = String::new();
        let mut suffix = String::new();
        if let Some(rules) = &filter_rules.filter_rules {
            for rule in rules {
                if let (Some(name), Some(value)) = (rule.name.as_ref(), rule.value.as_ref()) {
                    match name.as_str() {
                        "prefix" => prefix = value.clone(),
                        "suffix" => suffix = value.clone(),
                        _ => {}
                    }
                }
            }
        }
        return (prefix, suffix);
    }
    (String::new(), String::new())
}

/// Auxiliary functions: Handle configuration
pub(crate) fn process_queue_configurations<F>(
    event_rules: &mut Vec<(Vec<EventName>, String, String, Vec<TargetID>)>,
    configurations: Option<Vec<QueueConfiguration>>,
    target_id_parser: F,
) -> Result<(), TargetIDError>
where
    F: Fn(&str) -> Result<TargetID, TargetIDError>,
{
    if let Some(configs) = configurations {
        for cfg in configs {
            let events = cfg.events.iter().filter_map(|e| EventName::parse(e.as_ref()).ok()).collect();
            let (prefix, suffix) = extract_prefix_suffix(cfg.filter.as_ref());
            let target_id = target_id_parser(&cfg.queue_arn)?;
            event_rules.push((events, prefix, suffix, vec![target_id]));
        }
    }
    Ok(())
}

pub(crate) fn process_topic_configurations<F>(
    event_rules: &mut Vec<(Vec<EventName>, String, String, Vec<TargetID>)>,
    configurations: Option<Vec<TopicConfiguration>>,
    target_id_parser: F,
) -> Result<(), TargetIDError>
where
    F: Fn(&str) -> Result<TargetID, TargetIDError>,
{
    if let Some(configs) = configurations {
        for cfg in configs {
            let events = cfg.events.iter().filter_map(|e| EventName::parse(e.as_ref()).ok()).collect();
            let (prefix, suffix) = extract_prefix_suffix(cfg.filter.as_ref());
            let target_id = target_id_parser(&cfg.topic_arn)?;
            event_rules.push((events, prefix, suffix, vec![target_id]));
        }
    }
    Ok(())
}

pub(crate) fn process_lambda_configurations<F>(
    event_rules: &mut Vec<(Vec<EventName>, String, String, Vec<TargetID>)>,
    configurations: Option<Vec<LambdaFunctionConfiguration>>,
    target_id_parser: F,
) -> Result<(), TargetIDError>
where
    F: Fn(&str) -> Result<TargetID, TargetIDError>,
{
    if let Some(configs) = configurations {
        for cfg in configs {
            let events = cfg.events.iter().filter_map(|e| EventName::parse(e.as_ref()).ok()).collect();
            let (prefix, suffix) = extract_prefix_suffix(cfg.filter.as_ref());
            let target_id = target_id_parser(&cfg.lambda_function_arn)?;
            event_rules.push((events, prefix, suffix, vec![target_id]));
        }
    }
    Ok(())
}

pub(crate) async fn has_replication_rules(bucket: &str, objects: &[ObjectToDelete]) -> bool {
    let (cfg, _created) = match get_replication_config(bucket).await {
        Ok(replication_config) => replication_config,
        Err(_err) => {
            return false;
        }
    };

    for object in objects {
        if cfg.has_active_rules(&object.object_name, true) {
            return true;
        }
    }
    false
}

/// Helper function to get store and validate bucket exists
pub(crate) async fn get_validated_store(bucket: &str) -> S3Result<Arc<rustfs_ecstore::store::ECStore>> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    // Validate bucket exists
    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    Ok(store)
}

/// Quick check if CORS processing is needed (lightweight check for Origin header)
/// This avoids unnecessary function calls for non-CORS requests
#[inline]
pub(crate) fn needs_cors_processing(headers: &HeaderMap) -> bool {
    headers.contains_key(cors::standard::ORIGIN)
}

/// Apply CORS headers to response based on bucket CORS configuration and request origin
///
/// This function:
/// 1. Reads the Origin header from the request
/// 2. Retrieves the bucket's CORS configuration
/// 3. Matches the origin against CORS rules
/// 4. Validates AllowedHeaders if request headers are present
/// 5. Returns headers to add to the response if a match is found
///
/// Note: This function should only be called if `needs_cors_processing()` returns true
/// to avoid unnecessary overhead for non-CORS requests.
pub(crate) async fn apply_cors_headers(bucket: &str, method: &http::Method, headers: &HeaderMap) -> Option<HeaderMap> {
    use http::HeaderValue;

    // Get Origin header from request
    let origin = headers.get(cors::standard::ORIGIN)?.to_str().ok()?;

    // Get CORS configuration for the bucket
    let cors_config = match metadata_sys::get_cors_config(bucket).await {
        Ok((config, _)) => config,
        Err(_) => return None, // No CORS config, no headers to add
    };

    // Early return if no CORS rules configured
    if cors_config.cors_rules.is_empty() {
        return None;
    }

    // Check if method is supported and get its string representation
    const SUPPORTED_METHODS: &[&str] = &["GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS"];
    let method_str = method.as_str();
    if !SUPPORTED_METHODS.contains(&method_str) {
        return None;
    }

    // For OPTIONS (preflight) requests, check Access-Control-Request-Method
    let is_preflight = method == http::Method::OPTIONS;
    let requested_method = if is_preflight {
        headers
            .get(cors::request::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(method_str)
    } else {
        method_str
    };

    // Get requested headers from preflight request
    let requested_headers = if is_preflight {
        headers
            .get(cors::request::ACCESS_CONTROL_REQUEST_HEADERS)
            .and_then(|v| v.to_str().ok())
            .map(|h| h.split(',').map(|s| s.trim().to_lowercase()).collect::<Vec<_>>())
    } else {
        None
    };

    // Find matching CORS rule
    for rule in cors_config.cors_rules.iter() {
        // Check if origin matches
        let origin_matches = rule.allowed_origins.iter().any(|allowed_origin| {
            if allowed_origin == "*" {
                true
            } else {
                // Exact match or pattern match (support wildcards like https://*.example.com)
                allowed_origin == origin || matches_origin_pattern(allowed_origin, origin)
            }
        });

        if !origin_matches {
            continue;
        }

        // Check if method is allowed
        let method_allowed = rule
            .allowed_methods
            .iter()
            .any(|allowed_method| allowed_method.as_str() == requested_method);

        if !method_allowed {
            continue;
        }

        // Validate AllowedHeaders if present in the request
        if let Some(ref req_headers) = requested_headers {
            if let Some(ref allowed_headers) = rule.allowed_headers {
                // Check if all requested headers are allowed
                let all_headers_allowed = req_headers.iter().all(|req_header| {
                    allowed_headers.iter().any(|allowed_header| {
                        let allowed_lower = allowed_header.to_lowercase();
                        // "*" allows all headers, or exact match
                        allowed_lower == "*" || allowed_lower == *req_header
                    })
                });

                if !all_headers_allowed {
                    // If not all headers are allowed, skip this rule
                    continue;
                }
            } else if !req_headers.is_empty() {
                // If no AllowedHeaders specified but headers were requested, skip this rule
                // Unless the rule explicitly allows all headers
                continue;
            }
        }

        // Found matching rule, build response headers
        let mut response_headers = HeaderMap::new();

        // Access-Control-Allow-Origin
        // If origin is "*", use "*", otherwise echo back the origin
        let has_wildcard_origin = rule.allowed_origins.iter().any(|o| o == "*");
        if has_wildcard_origin {
            response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        } else if let Ok(origin_value) = HeaderValue::from_str(origin) {
            response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN, origin_value);
        }

        // Vary: Origin (required for caching, except when using wildcard)
        if !has_wildcard_origin {
            response_headers.insert(cors::standard::VARY, HeaderValue::from_static("Origin"));
        }

        // Access-Control-Allow-Methods (required for preflight)
        if is_preflight || !rule.allowed_methods.is_empty() {
            let methods_str = rule.allowed_methods.iter().map(|m| m.as_str()).collect::<Vec<_>>().join(", ");
            if let Ok(methods_value) = HeaderValue::from_str(&methods_str) {
                response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_METHODS, methods_value);
            }
        }

        // Access-Control-Allow-Headers (required for preflight if headers were requested)
        if is_preflight && let Some(ref allowed_headers) = rule.allowed_headers {
            let headers_str = allowed_headers.iter().map(|h| h.as_str()).collect::<Vec<_>>().join(", ");
            if let Ok(headers_value) = HeaderValue::from_str(&headers_str) {
                response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_HEADERS, headers_value);
            }
        }

        // Access-Control-Expose-Headers (for actual requests)
        if !is_preflight && let Some(ref expose_headers) = rule.expose_headers {
            let expose_headers_str = expose_headers.iter().map(|h| h.as_str()).collect::<Vec<_>>().join(", ");
            if let Ok(expose_value) = HeaderValue::from_str(&expose_headers_str) {
                response_headers.insert(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS, expose_value);
            }
        }

        // Access-Control-Max-Age (for preflight requests)
        if is_preflight
            && let Some(max_age) = rule.max_age_seconds
            && let Ok(max_age_value) = HeaderValue::from_str(&max_age.to_string())
        {
            response_headers.insert(cors::response::ACCESS_CONTROL_MAX_AGE, max_age_value);
        }

        return Some(response_headers);
    }

    None // No matching rule found
}
/// Check if an origin matches a pattern (supports wildcards like https://*.example.com)
pub(crate) fn matches_origin_pattern(pattern: &str, origin: &str) -> bool {
    // Simple wildcard matching: * matches any sequence
    if pattern.contains('*') {
        let pattern_parts: Vec<&str> = pattern.split('*').collect();
        if pattern_parts.len() == 2 {
            origin.starts_with(pattern_parts[0]) && origin.ends_with(pattern_parts[1])
        } else {
            false
        }
    } else {
        pattern == origin
    }
}

/// Wrap S3Response with CORS headers if needed
/// This function performs a lightweight check first to avoid unnecessary CORS processing
/// for non-CORS requests (requests without Origin header)
pub(crate) async fn wrap_response_with_cors<T>(
    bucket: &str,
    method: &http::Method,
    headers: &HeaderMap,
    output: T,
) -> S3Response<T> {
    let mut response = S3Response::new(output);

    // Quick check: only process CORS if Origin header is present
    if needs_cors_processing(headers)
        && let Some(cors_headers) = apply_cors_headers(bucket, method, headers).await
    {
        for (key, value) in cors_headers.iter() {
            response.headers.insert(key, value.clone());
        }
    }

    response
}

/// Parse part number from Option<i32> to Option<usize> with validation
/// This function checks that the part number is greater than 0 and
/// converts it to usize, returning an error if invalid
///
/// # Arguments
/// * `part_number` - The optional part number as i32
/// * `op` - The operation name for logging purposes
///
/// # Returns
/// * `Ok(Some(usize))` if part number is valid
/// * `Ok(None)` if part number is None
/// * `Err(S3Error)` if part number is invalid (0 or overflow)
#[inline]
pub(crate) fn parse_part_number_i32_to_usize(part_number: Option<i32>, op: &'static str) -> S3Result<Option<usize>> {
    match part_number {
        None => Ok(None),
        Some(n) if n <= 0 => Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("{op}: invalid partNumber {n}, must be a positive integer"),
        )),
        Some(n) => Ok(Some(n as usize)),
    }
}
