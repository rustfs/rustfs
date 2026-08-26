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

//! Cross-cutting helpers shared by the object use-case modules.

use super::*;

pub(super) const RUSTFS_EXPECTED_CURRENT_VERSION_ID: &str = "x-rustfs-expected-current-version-id";

pub(super) type S3StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub(crate) fn s3s_body_error_to_io(err: StdError) -> io::Error {
    io::Error::other(err)
}

pub(super) const ACCEPT_RANGES_BYTES: &str = "bytes";

pub(super) const LOG_COMPONENT_APP: &str = "app";

pub(super) const LOG_SUBSYSTEM_OBJECT: &str = "object";

pub(super) fn decoded_content_length_from_headers(headers: &HeaderMap) -> S3Result<Option<i64>> {
    let Some(val) = headers.get(AMZ_DECODED_CONTENT_LENGTH) else {
        return Ok(None);
    };

    match atoi::atoi::<i64>(val.as_bytes()) {
        Some(x) => Ok(Some(x)),
        None => Err(s3_error!(UnexpectedContent)),
    }
}

/// Losslessly convert an s3s [`Range`] into the internal [`HTTPRangeSpec`].
///
/// Shared by GET and HEAD so both apply identical range semantics. s3s parses
/// `first`/`last` as `u64`, but its own parser already rejects any value greater
/// than `i64::MAX`, so the int branch is a checked cast that never truncates.
///
/// The suffix length, however, is an unchecked `u64`. A naive `length as i64`
/// truncates deterministically: `bytes=-18446744073709551615` wraps to `-1` and
/// is then read as "last 1 byte", and `bytes=-0` yields a 0-length 206 instead
/// of a 416. This function instead mirrors s3s [`Range::check`] semantics:
///   * a zero-length suffix is rejected with `InvalidRange` (416), matching AWS
///     S3 and MinIO;
///   * a suffix larger than `i64::MAX` is clamped to `i64::MAX`. Object sizes in
///     this system are bounded by `i64::MAX`, so such a suffix always covers the
///     whole object, and [`HTTPRangeSpec::get_length`] clamps it to the real
///     size once the object is known.
pub(super) fn range_to_http_range_spec(range: Range) -> S3Result<HTTPRangeSpec> {
    match range {
        Range::Int { first, last } => {
            let start = i64::try_from(first).map_err(|_| s3_error!(InvalidRange, "The requested range is not satisfiable"))?;
            let end = match last {
                Some(last) => {
                    i64::try_from(last).map_err(|_| s3_error!(InvalidRange, "The requested range is not satisfiable"))?
                }
                None => -1,
            };
            Ok(HTTPRangeSpec {
                is_suffix_length: false,
                start,
                end,
            })
        }
        Range::Suffix { length } => {
            if length == 0 {
                return Err(s3_error!(InvalidRange, "The requested range is not satisfiable"));
            }
            // Clamp to i64::MAX: any suffix >= object size returns the whole
            // object, and object sizes never exceed i64::MAX.
            let start = i64::try_from(length).unwrap_or(i64::MAX);
            Ok(HTTPRangeSpec {
                is_suffix_length: true,
                start,
                end: -1,
            })
        }
    }
}

/// True when the request body actually arrived chunk-framed on the wire, i.e. the payload was
/// signed as a SigV4 streaming upload (`x-amz-content-sha256: STREAMING-*`). This is the only
/// case in which the auth layer de-frames the body; `Content-Encoding: aws-chunked` without a
/// streaming payload is just a declared encoding over an unframed body.
pub(super) fn request_body_is_aws_chunked_framed(headers: &HeaderMap) -> bool {
    headers
        .get(AMZ_CONTENT_SHA256)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.len() >= 10 && value[..10].eq_ignore_ascii_case("STREAMING-"))
}

/// Map a bucket-quota checker outcome onto the S3 admission result.
///
/// Hard is the only supported quota type, so a checker fault (bucket-config read, config parse, or usage lookup) must fail closed rather than admit the write: allowing it would silently bypass a configured hard quota. The no-quota happy path never reaches the error arm — `QuotaChecker::check_quota` returns `Ok(allowed)` via the zero-extra-I/O fast path when no quota is configured, so failing closed here cannot penalise buckets without a quota. A fault surfaces as a retryable `ServiceUnavailable` and is counted; the client-facing message stays generic so internal config/usage details are not leaked.
pub(crate) fn map_quota_check_outcome(bucket: &str, outcome: Result<QuotaCheckResult, QuotaError>) -> S3Result<QuotaCheckResult> {
    match outcome {
        Ok(result) if !result.allowed => Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!(
                "Bucket quota exceeded. Current usage: {} bytes, limit: {} bytes",
                result.current_usage.unwrap_or(0),
                result.quota_limit.unwrap_or(0)
            ),
        )),
        Err(e) => {
            counter!("rustfs_bucket_quota_check_failed_total").increment(1);
            if matches!(&e, QuotaError::UsageUnavailable { .. }) {
                debug!(bucket, error = %e, state = "usage_pending", "Bucket quota check waiting for authoritative usage");
            } else {
                warn!(bucket, error = %e, state = "checker_failed", "Bucket quota check failed closed");
            }
            Err(S3Error::with_message(
                S3ErrorCode::ServiceUnavailable,
                "Bucket quota check temporarily unavailable, please retry".to_string(),
            ))
        }
        Ok(result) => Ok(result),
    }
}

pub(crate) fn apply_quota_admission(opts: &mut ObjectOptions, result: &QuotaCheckResult) -> S3Result<()> {
    if result.uses_durable_reservations {
        return Ok(());
    }
    let Some(quota_limit) = result.quota_limit else {
        return Ok(());
    };
    let Some(current_usage) = result.current_usage else {
        return Err(S3Error::with_message(
            S3ErrorCode::ServiceUnavailable,
            "Bucket quota check temporarily unavailable, please retry".to_string(),
        ));
    };
    if current_usage > quota_limit {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("Bucket quota exceeded. Current usage: {current_usage} bytes, limit: {quota_limit} bytes"),
        ));
    }
    let _ = opts.set_quota_admission(current_usage, quota_limit);
    Ok(())
}

pub(super) fn ensure_object_size_within_quota(result: &QuotaCheckResult, new_size: u64) -> S3Result<()> {
    let (Some(current_usage), Some(quota_limit)) = (result.current_usage, result.quota_limit) else {
        return Ok(());
    };
    if new_size > quota_limit {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("Bucket quota exceeded. Current usage: {current_usage} bytes, limit: {quota_limit} bytes"),
        ));
    }
    Ok(())
}

pub(super) fn quota_accounting_object_size(info: &ObjectInfo, fail_closed: bool) -> S3Result<u64> {
    match quota_object_size(info) {
        Ok(size) => Ok(size),
        Err(err) if fail_closed => Err(ApiError::from(err).into()),
        Err(_) => Ok(info.size.max(0) as u64),
    }
}

pub(super) fn request_uses_aws_chunked(headers: &HeaderMap) -> bool {
    let has_aws_chunked = |header_name: &str| {
        headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|part| part.trim().eq_ignore_ascii_case("aws-chunked")))
    };

    has_aws_chunked("content-encoding") || has_aws_chunked("transfer-encoding")
}

pub(super) async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

pub(super) struct DeadlockRequestGuard {
    deadlock_detector: Arc<deadlock_detector::DeadlockDetector>,
    request_id: String,
}

impl DeadlockRequestGuard {
    fn new(deadlock_detector: Arc<deadlock_detector::DeadlockDetector>, request_id: String) -> Self {
        Self {
            deadlock_detector,
            request_id,
        }
    }

    pub(super) fn register_if_enabled<F>(
        deadlock_detector: Arc<deadlock_detector::DeadlockDetector>,
        request_id: &str,
        description: F,
    ) -> Option<Self>
    where
        F: FnOnce() -> String,
    {
        if !deadlock_detector.is_enabled() {
            return None;
        }

        let request_id = request_id.to_string();
        deadlock_detector.register_request(&request_id, description());
        Some(Self::new(deadlock_detector, request_id))
    }
}

impl Drop for DeadlockRequestGuard {
    fn drop(&mut self) {
        self.deadlock_detector.unregister_request(&self.request_id);
    }
}

pub(super) fn has_put_sse_request_headers(headers: &HeaderMap) -> bool {
    headers.get(AMZ_SERVER_SIDE_ENCRYPTION).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID).is_some()
}

/// Resolve the effective server-side encryption for a write against the bucket's
/// default encryption configuration.
///
/// A request-level value always wins; the bucket default only fills a gap, and
/// the unknown-algorithm fallback lives once in [`bucket_default_write_sse`].
///
/// `has_explicit_ssec` suppresses the default entirely. Only COPY passes `true`
/// today: its destination may carry SSE-C, which must not also be given managed
/// encryption. PUT and extract pass `false`, matching their current behaviour —
/// see backlog#1826 for the divergence that leaves.
///
/// Callers layering further overrides (PUT's `ciphertext_passthrough`) apply
/// them to the returned pair.
pub(super) fn resolve_bucket_default_sse(
    bucket_sse_config: Option<&ServerSideEncryptionConfiguration>,
    requested_sse: Option<ServerSideEncryption>,
    requested_kms_key_id: Option<SSEKMSKeyId>,
    has_explicit_ssec: bool,
) -> (Option<ServerSideEncryption>, Option<SSEKMSKeyId>) {
    let bucket_default = || {
        if has_explicit_ssec {
            return None;
        }
        bucket_sse_config
            .and_then(|config| config.rules.first())
            .and_then(|rule| rule.apply_server_side_encryption_by_default.as_ref())
    };

    let effective_sse = requested_sse.or_else(|| bucket_default().map(bucket_default_write_sse));
    let effective_kms_key_id = requested_kms_key_id.or_else(|| bucket_default().and_then(|sse| sse.kms_master_key_id.clone()));
    (effective_sse, effective_kms_key_id)
}

#[cfg(test)]
mod deadlock_request_guard_tests {
    use super::DeadlockRequestGuard;
    use crate::app::storage_api::object_usecase::deadlock_detector::{DeadlockDetector, RequestHangDetectionPolicy};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn deadlock_request_guard_unregisters_on_drop() {
        let detector = Arc::new(DeadlockDetector::new(RequestHangDetectionPolicy {
            enabled: true,
            ..RequestHangDetectionPolicy::default()
        }));
        let request_id = "test-request-id".to_string();

        detector.register_request(&request_id, "test request");
        assert_eq!(detector.tracked_count(), 1);

        {
            let _guard = DeadlockRequestGuard::new(Arc::clone(&detector), request_id);
            // `_guard` is dropped at the end of this scope, which should unregister the request.
        }

        assert_eq!(detector.tracked_count(), 0);
    }

    #[test]
    fn deadlock_request_guard_skips_disabled_detector() {
        let detector = Arc::new(DeadlockDetector::new(RequestHangDetectionPolicy {
            enabled: false,
            ..RequestHangDetectionPolicy::default()
        }));
        let description_built = Rc::new(Cell::new(false));
        let description_built_for_closure = Rc::clone(&description_built);

        let guard = DeadlockRequestGuard::register_if_enabled(detector, "test-request-id", || {
            description_built_for_closure.set(true);
            "test request".to_string()
        });

        assert!(guard.is_none());
        assert!(!description_built.get());
    }
}

pub(super) async fn maybe_enqueue_transition_immediate(obj_info: &ObjectInfo, src: LcEventSrc) {
    enqueue_transition_immediate(obj_info, src).await;
}

/// Inject additional-checksum response headers (XXHash3/64/128, SHA-512) that s3s
/// cannot carry on its typed `*Output` structs. Centralized so that when s3s gains
/// typed fields for these algorithms, only this one function changes (fill the typed
/// field, drop the header insert) — and there is exactly one place that could ever
/// emit a duplicate header. Header names come from `ChecksumType::key()`, so they are
/// known-valid static strings.
pub(crate) fn inject_additional_checksum_headers(headers: &mut HeaderMap, pairs: &[(&'static str, String)]) {
    for (name, value) in pairs {
        match HeaderValue::from_str(value) {
            Ok(header_value) => {
                headers.insert(http::HeaderName::from_static(name), header_value);
            }
            Err(_) => warn!("Failed to parse {name} checksum header value; skipping"),
        }
    }
}

pub(super) fn inject_accept_ranges_header(headers: &mut HeaderMap) {
    headers.insert(http::header::ACCEPT_RANGES, HeaderValue::from_static(ACCEPT_RANGES_BYTES));
}

/// Derive the response-header echo pairs for an additional-checksum algorithm
/// (XXHash3/64/128, SHA-512) from the server-computed content checksum, for
/// PutObject to echo back (#1256). Returns empty for the five s3s-typed algorithms
/// (they are echoed via typed fields) and when the value is not yet materialized
/// (e.g. a trailing checksum, whose value lands after the body — covered by e2e).
pub(crate) fn additional_checksum_echo_pairs(want: &Option<rustfs_rio::Checksum>) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(cs) = want
        && !cs.checksum_type.is_s3s_typed()
        && !cs.encoded.is_empty()
        && let Some(name) = cs.checksum_type.key()
    {
        out.push((name, cs.encoded.clone()));
    }
    out
}

/// Extract trailing-header checksum values, overriding the corresponding input fields.
pub(super) fn apply_trailing_checksums(
    algorithm: Option<&str>,
    trailing_headers: &Option<s3s::TrailingHeaders>,
    checksums: &mut PutObjectChecksums,
) {
    let Some(alg) = algorithm else { return };
    let Some(checksum_str) = trailing_headers.as_ref().and_then(|trailer| {
        let key = match alg {
            ChecksumAlgorithm::CRC32 => rustfs_rio::ChecksumType::CRC32.key(),
            ChecksumAlgorithm::CRC32C => rustfs_rio::ChecksumType::CRC32C.key(),
            ChecksumAlgorithm::SHA1 => rustfs_rio::ChecksumType::SHA1.key(),
            ChecksumAlgorithm::SHA256 => rustfs_rio::ChecksumType::SHA256.key(),
            ChecksumAlgorithm::CRC64NVME => rustfs_rio::ChecksumType::CRC64_NVME.key(),
            _ => return None,
        };
        trailer.read(|headers| {
            headers
                .get(key.unwrap_or_default())
                .and_then(|value| value.to_str().ok().map(|s| s.to_string()))
        })
    }) else {
        return;
    };

    match alg {
        ChecksumAlgorithm::CRC32 => checksums.crc32 = checksum_str,
        ChecksumAlgorithm::CRC32C => checksums.crc32c = checksum_str,
        ChecksumAlgorithm::SHA1 => checksums.sha1 = checksum_str,
        ChecksumAlgorithm::SHA256 => checksums.sha256 = checksum_str,
        ChecksumAlgorithm::CRC64NVME => checksums.crc64nvme = checksum_str,
        _ => (),
    }
}

/// Checksums resolved from stored (decrypted) metadata for a response. The five
/// legacy algorithms fill named fields; the additional algorithms land in `extra`
/// for raw-header response paths and DTOs that expose their newer typed fields.
#[derive(Default)]
pub(crate) struct ResponseChecksums {
    pub(crate) crc32: Option<String>,
    pub(crate) crc32c: Option<String>,
    pub(crate) sha1: Option<String>,
    pub(crate) sha256: Option<String>,
    pub(crate) crc64nvme: Option<String>,
    pub(crate) checksum_type: Option<ChecksumType>,
    pub(crate) extra: Vec<(&'static str, String)>,
}

/// Split decrypted checksum pairs into the five legacy fields and the additional
/// algorithm values. Single source of truth for every response
/// path (GetObject / HeadObject / GetObjectAttributes / CompleteMultipartUpload),
/// replacing what used to be five copies of this match loop.
pub(crate) fn classify_response_checksums<I>(pairs: I, is_multipart: bool) -> ResponseChecksums
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut c = ResponseChecksums::default();
    for (key, checksum) in pairs {
        if key == AMZ_CHECKSUM_TYPE {
            c.checksum_type = Some(ChecksumType::from(checksum));
            continue;
        }
        let ct = rustfs_rio::ChecksumType::from_string(key.as_str());
        match ct.base() {
            rustfs_rio::ChecksumType::CRC32 => c.crc32 = Some(checksum),
            rustfs_rio::ChecksumType::CRC32C => c.crc32c = Some(checksum),
            rustfs_rio::ChecksumType::SHA1 => c.sha1 = Some(checksum),
            rustfs_rio::ChecksumType::SHA256 => c.sha256 = Some(checksum),
            rustfs_rio::ChecksumType::CRC64_NVME => c.crc64nvme = Some(checksum),
            _ => {
                if let Some(name) = ct.key() {
                    c.extra.push((name, checksum));
                }
            }
        }
    }
    if is_multipart && c.checksum_type.is_none() {
        c.checksum_type = Some(ChecksumType::from("COMPOSITE".to_string()));
    }
    c
}

fn build_put_object_expiration_header(event: &lifecycle::Event) -> Option<String> {
    if !event.action.delete() {
        return None;
    }

    let expire_time = event.due?;

    if event.rule_id.is_empty() || expire_time == OffsetDateTime::UNIX_EPOCH {
        return None;
    }

    let expiry_date = expire_time.format(&Rfc3339).ok()?;
    Some(format!("expiry-date=\"{}\", rule-id=\"{}\"", expiry_date, event.rule_id))
}

pub(super) fn internal_object_info_lookup_opts(mut opts: ObjectOptions) -> ObjectOptions {
    opts.http_preconditions = None;
    opts
}

pub(super) fn expected_current_version_id(headers: &HeaderMap) -> S3Result<Option<String>> {
    headers
        .get(RUSTFS_EXPECTED_CURRENT_VERSION_ID)
        .map(|value| {
            let value = value
                .to_str()
                .map(str::trim)
                .map_err(|_| s3_error!(InvalidArgument, "Invalid expected current version ID header"))?;
            if value.eq_ignore_ascii_case("null") {
                return Ok(Uuid::nil().to_string());
            }
            Uuid::parse_str(value)
                .map(|version| version.to_string())
                .map_err(|_| s3_error!(InvalidArgument, "Invalid expected current version ID header"))
        })
        .transpose()
}

pub(super) fn insert_expires_metadata(metadata: &mut HashMap<String, String>, expires: Option<&Timestamp>) -> S3Result<()> {
    if let Some(expires) = expires {
        let mut formatted = Vec::new();
        expires
            .format(TimestampFormat::HttpDate, &mut formatted)
            .map_err(|e| ApiError::from(StorageError::other(format!("Invalid expires timestamp: {e}"))))?;
        metadata.insert("expires".to_string(), String::from_utf8_lossy(&formatted).into_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_standard_object_metadata(
    metadata: &mut HashMap<String, String>,
    cache_control: Option<&str>,
    content_disposition: Option<&str>,
    content_encoding: Option<&str>,
    content_language: Option<&str>,
    content_type: Option<&str>,
    expires: Option<&Timestamp>,
    website_redirect_location: Option<&str>,
) -> S3Result<()> {
    if let Some(cache_control) = cache_control {
        metadata.insert("cache-control".to_string(), cache_control.to_string());
    }
    if let Some(content_disposition) = content_disposition {
        metadata.insert("content-disposition".to_string(), content_disposition.to_string());
    }
    if let Some(content_encoding) = content_encoding
        && let Some(normalized_content_encoding) = normalize_content_encoding_for_storage(content_encoding)
    {
        metadata.insert("content-encoding".to_string(), normalized_content_encoding);
    }
    if let Some(content_language) = content_language {
        metadata.insert("content-language".to_string(), content_language.to_string());
    }
    if let Some(content_type) = content_type {
        metadata.insert("content-type".to_string(), content_type.to_string());
    }
    insert_expires_metadata(metadata, expires)?;
    if let Some(website_redirect_location) = website_redirect_location {
        metadata.insert(AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), website_redirect_location.to_string());
    }
    Ok(())
}

pub(super) fn response_storage_class(info: &ObjectInfo, metadata: &HashMap<String, String>) -> Option<StorageClass> {
    let stored_class = info
        .storage_class
        .as_deref()
        .or_else(|| metadata.get(AMZ_STORAGE_CLASS).map(String::as_str));
    let transitioned_tier = (info.transitioned_object.status == rustfs_filemeta::TRANSITION_COMPLETE
        && !info.transitioned_object.tier.is_empty())
    .then_some(info.transitioned_object.tier.as_str());
    let effective_class = storageclass::effective_class(stored_class, transitioned_tier);

    (effective_class != storageclass::STANDARD).then(|| StorageClass::from(effective_class.to_string()))
}

pub(super) fn response_storage_class_for_object_attributes(
    info: &ObjectInfo,
    metadata: &HashMap<String, String>,
    requested: bool,
) -> Option<StorageClass> {
    if !requested {
        return None;
    }

    let stored_class = info
        .storage_class
        .as_deref()
        .or_else(|| metadata.get(AMZ_STORAGE_CLASS).map(String::as_str));
    let transitioned_tier = (info.transitioned_object.status == rustfs_filemeta::TRANSITION_COMPLETE
        && !info.transitioned_object.tier.is_empty())
    .then_some(info.transitioned_object.tier.as_str());

    Some(StorageClass::from(
        storageclass::effective_class(stored_class, transitioned_tier).to_string(),
    ))
}

// Shared across Object Lock validation paths to keep the client-facing
// InvalidRequest message consistent.
pub(crate) const ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED: &str =
    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied";

pub(crate) fn build_put_like_object_lock_metadata(
    bucket: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
) -> S3Result<Option<HashMap<String, String>>> {
    if object_lock_legal_hold_status.is_none() && object_lock_mode.is_none() && object_lock_retain_until_date.is_none() {
        return Ok(None);
    }

    let retention = match (object_lock_mode, object_lock_retain_until_date) {
        (Some(mode), Some(retain_until_date)) => Some(ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from(mode.as_str().to_string())),
            retain_until_date: Some(retain_until_date),
        }),
        (Some(_), None) | (None, Some(_)) => {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED.to_string(),
            ));
        }
        (None, None) => None,
    };

    validate_bucket_object_lock_enabled_state(bucket, object_lock_config_state)?;

    let mut eval_metadata = parse_object_lock_retention(retention)?;
    eval_metadata.extend(parse_object_lock_legal_hold(
        object_lock_legal_hold_status.map(|status| ObjectLockLegalHold { status: Some(status) }),
    )?);

    if eval_metadata.is_empty() {
        return Ok(None);
    }

    Ok(Some(eval_metadata))
}

fn put_like_write_creates_new_version(opts: &ObjectOptions) -> bool {
    opts.version_id.is_none() && opts.versioned && !opts.version_suspended
}

pub(crate) fn validate_existing_object_lock_for_write(
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    existing_obj_info: &ObjectInfo,
    opts: &ObjectOptions,
) -> S3Result<()> {
    if put_like_write_creates_new_version(opts) {
        return Ok(());
    }
    // An authorized replication write may replace the locked version only
    // when the set layer's commit-lock LWW will judge every locking category,
    // judged against the bucket's authoritative lock state (default retention
    // included) exactly like the set-layer gate, which re-checks the same
    // rule under the lock. A non-authoritative state or malformed lock
    // metadata fails closed here.
    if opts.replication_request {
        let may_pass = replication_write_may_pass_worm_gate(object_lock_config_state, existing_obj_info, opts).map_err(|_| {
            S3Error::with_message(S3ErrorCode::AccessDenied, "Object Lock state could not be verified.".to_string())
        })?;
        return if may_pass {
            Ok(())
        } else {
            Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                "Object is locked and the replication write carries no source lock decision for it.".to_string(),
            ))
        };
    }

    let legal_hold = get_object_legalhold_meta(&existing_obj_info.user_defined);
    if legal_hold.is_on() {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "Object has a legal hold and cannot be overwritten. Remove the legal hold first.".to_string(),
        ));
    }

    let retention = get_object_retention_meta(&existing_obj_info.user_defined);
    if let Some(mode) = retention.mode
        && mode == RetentionMode::Compliance
        && is_retention_active(mode, retention.retain_until_date)
    {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "Object is under COMPLIANCE retention and cannot be overwritten.".to_string(),
        ));
    }

    Ok(())
}

pub(super) async fn resolve_put_object_expiration(bucket: &str, obj_info: &ObjectInfo) -> Option<String> {
    let Ok((lifecycle_config, _)) = metadata_sys::get_lifecycle_config(bucket).await else {
        debug!(bucket, state = "config_missing", "PUT object expiration config missing");
        return None;
    };

    let obj_opts = lifecycle::object_opts_from_object_info(obj_info);
    let event = predict_lifecycle_expiration(&lifecycle_config, &obj_opts).await;
    debug!(
        bucket,
        action = ?event.action,
        rule_id = %event.rule_id,
        due = ?event.due,
        "PUT object expiration resolved"
    );
    build_put_object_expiration_header(&event)
}

/// Cadence for the "I/O queue congestion detected" WARN. Under sustained
/// overload (client concurrency at or above the disk-read permit pool) every
/// GET observes >=80% utilization, so an unthrottled WARN floods the log
/// from the already saturated hot path; congestion metrics stay per-request.
const IO_QUEUE_CONGESTION_WARN_INTERVAL_MS: u64 = 5_000;

/// At-most-one-WARN-per-interval limiter for the I/O queue congestion log.
/// Callers supply monotonic milliseconds so tests can drive the clock.
pub(super) struct IoQueueCongestionWarnThrottle {
    /// Timestamp of the last emitted WARN; `u64::MAX` until the first one.
    last_warn_ms: AtomicU64,
    /// Congested requests left unlogged since the last emitted WARN.
    suppressed: AtomicU64,
}

impl IoQueueCongestionWarnThrottle {
    const fn new() -> Self {
        Self {
            last_warn_ms: AtomicU64::new(u64::MAX),
            suppressed: AtomicU64::new(0),
        }
    }

    /// Claim the right to emit one WARN. Returns the number of events
    /// suppressed since the previous emission, or `None` while the interval
    /// window is still closed (the event is counted, not logged).
    pub(super) fn claim(&self, now_ms: u64) -> Option<u64> {
        let last = self.last_warn_ms.load(Ordering::Relaxed);
        let window_open = last == u64::MAX || now_ms.saturating_sub(last) >= IO_QUEUE_CONGESTION_WARN_INTERVAL_MS;
        if window_open
            && self
                .last_warn_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            Some(self.suppressed.swap(0, Ordering::Relaxed))
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Monotonic milliseconds since the first call, for production callers.
    pub(super) fn now_ms() -> u64 {
        static ANCHOR: OnceLock<std::time::Instant> = OnceLock::new();
        ANCHOR.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64
    }
}

pub(super) static IO_QUEUE_CONGESTION_WARN_THROTTLE: IoQueueCongestionWarnThrottle = IoQueueCongestionWarnThrottle::new();

pub(super) async fn track_object_read_setup<F>(health: Option<&ObjectTrafficHealth>, future: F) -> F::Output
where
    F: std::future::Future,
{
    let _progress = health.and_then(ObjectTrafficHealth::track_read_storage);
    future.await
}

impl DefaultObjectUsecase {
    /// Headers a proxied read forwards verbatim to the replication target:
    /// only the client's SSE-C key family, so the target performs the real
    /// SSE-C decryption (never the replication-check exemption). HTTP
    /// conditional headers (If-Match & co.) are deliberately NOT forwarded —
    /// MinIO does not forward them either, and a remote 304/412 would leak a
    /// conditional evaluation against a replica the local site never saw.
    /// Range and part-number travel as typed SDK parameters instead.
    pub(super) fn proxy_read_passthrough_headers(headers: &HeaderMap) -> HeaderMap {
        const FORWARDED: &[&str] = &[
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-md5",
        ];
        let mut forwarded = HeaderMap::new();
        for name in FORWARDED {
            if let Ok(header_name) = http::HeaderName::from_str(name)
                && let Some(value) = headers.get(&header_name)
            {
                forwarded.insert(header_name, value.clone());
            }
        }
        forwarded
    }

    /// True when a proxied SDK call failed because the target does not have
    /// the object either (service-level not-found or a raw 404, which also
    /// covers NoSuchVersion): the caller tries the next target silently.
    pub(super) fn proxy_sdk_error_is_not_found<E>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
        err.raw_response().is_some_and(|resp| resp.status().as_u16() == 404)
    }
}

/// Fail closed when deciding whether an object-lock-sensitive operation may
/// skip its existing-object lookup.
pub(crate) async fn object_lock_checks_required(bucket: &str) -> bool {
    get_bucket_metadata(bucket)
        .await
        .map_or(true, |metadata| metadata.object_locking())
}

pub(super) fn object_lock_checks_required_for_state(state: &metadata_sys::ObjectLockConfigState) -> bool {
    match state {
        metadata_sys::ObjectLockConfigState::Configured { .. } | metadata_sys::ObjectLockConfigState::Fabricated => true,
        metadata_sys::ObjectLockConfigState::ConfirmedAbsent => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};
    use s3s::dto::{
        ObjectLockConfiguration, ObjectLockEnabled, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
        ServerSideEncryptionRule,
    };
    use std::sync::Arc;

    #[test]
    fn io_queue_congestion_warn_throttle_emits_once_per_interval() {
        let throttle = IoQueueCongestionWarnThrottle::new();
        // The first congested request logs immediately.
        assert_eq!(throttle.claim(0), Some(0));
        // Requests inside the window are counted, not logged.
        assert_eq!(throttle.claim(1), None);
        assert_eq!(throttle.claim(IO_QUEUE_CONGESTION_WARN_INTERVAL_MS - 1), None);
        // The next emission reports how many stayed silent.
        assert_eq!(throttle.claim(IO_QUEUE_CONGESTION_WARN_INTERVAL_MS), Some(2));
        assert_eq!(throttle.claim(IO_QUEUE_CONGESTION_WARN_INTERVAL_MS + 1), None);
    }

    // classify_response_checksums is the single point that splits decrypted checksum
    // pairs into the five s3s-typed fields and the additional-algorithm `extra`
    // headers, replacing five copies of the loop. Lock its behaviour (#1252).
    #[test]
    fn classify_response_checksums_splits_typed_and_extra() {
        // Typed algorithms fill named fields; nothing spills into extra.
        let c = classify_response_checksums(
            vec![
                ("CRC32".to_string(), "AAAAAA==".to_string()),
                ("SHA256".to_string(), "c2hhMjU2".to_string()),
                ("CRC64NVME".to_string(), "Zm9vYmFyCg==".to_string()),
            ],
            false,
        );
        assert_eq!(c.crc32.as_deref(), Some("AAAAAA=="));
        assert_eq!(c.sha256.as_deref(), Some("c2hhMjU2"));
        assert_eq!(c.crc64nvme.as_deref(), Some("Zm9vYmFyCg=="));
        assert!(c.extra.is_empty(), "typed algorithms must not land in extra");

        // Additional algorithms land in extra keyed by their response-header name.
        let c = classify_response_checksums(
            vec![
                ("XXHASH3".to_string(), "eHhoMw==".to_string()),
                ("XXHASH64".to_string(), "eHhoNjQ=".to_string()),
                ("XXHASH128".to_string(), "eHhoMTI4".to_string()),
                ("SHA512".to_string(), "c2hhNTEy".to_string()),
                ("MD5".to_string(), "bWQ1".to_string()),
            ],
            false,
        );
        assert!(c.crc32.is_none() && c.sha256.is_none() && c.crc64nvme.is_none());
        let names: Vec<&str> = c.extra.iter().map(|(n, _)| *n).collect();
        for expected in [
            "x-amz-checksum-xxhash3",
            "x-amz-checksum-xxhash64",
            "x-amz-checksum-xxhash128",
            "x-amz-checksum-sha512",
            "x-amz-checksum-md5",
        ] {
            assert!(names.contains(&expected), "extra missing {expected}: {names:?}");
        }
        assert_eq!(c.extra.len(), 5);

        // The checksum-type marker is captured as the type, not mistaken for an algorithm.
        let c = classify_response_checksums(vec![(AMZ_CHECKSUM_TYPE.to_string(), "COMPOSITE".to_string())], false);
        assert!(c.checksum_type.is_some());
        assert!(c.extra.is_empty() && c.crc32.is_none());

        let c = classify_response_checksums(vec![("CRC32".to_string(), "AAAAAA==-2".to_string())], true);
        assert_eq!(c.checksum_type.as_ref().map(ChecksumType::as_str), Some("COMPOSITE"));

        // Empty input yields an all-default result.
        let c = classify_response_checksums(Vec::<(String, String)>::new(), false);
        assert!(c.crc32.is_none() && c.extra.is_empty() && c.checksum_type.is_none());
    }

    // additional_checksum_echo_pairs derives the PutObject/UploadPart response echo for
    // additional algorithms from the server-computed checksum, and nothing for the
    // five typed ones (those go through typed output fields).
    #[test]
    fn additional_checksum_echo_pairs_only_for_new_algorithms() {
        // Typed algorithm → no echo pair.
        let sha256 = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, b"data");
        assert!(additional_checksum_echo_pairs(&sha256).is_empty());

        // Additional algorithm → exactly one (header, value) pair matching the digest.
        let xxh3 = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::XXHASH3, b"data");
        let pairs = additional_checksum_echo_pairs(&xxh3);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "x-amz-checksum-xxhash3");
        assert_eq!(pairs[0].1, xxh3.as_ref().unwrap().encoded);

        // MD5 additional checksum is echoed too.
        let md5 = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::MD5, b"data");
        let pairs = additional_checksum_echo_pairs(&md5);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "x-amz-checksum-md5");

        // None → empty.
        assert!(additional_checksum_echo_pairs(&None).is_empty());
    }

    #[test]
    fn inject_additional_checksum_headers_writes_all_pairs() {
        let mut headers = HeaderMap::new();
        inject_additional_checksum_headers(
            &mut headers,
            &[
                ("x-amz-checksum-xxhash3", "eHhoMw==".to_string()),
                ("x-amz-checksum-md5", "bWQ1".to_string()),
            ],
        );
        assert_eq!(headers.get("x-amz-checksum-xxhash3").unwrap(), "eHhoMw==");
        assert_eq!(headers.get("x-amz-checksum-md5").unwrap(), "bWQ1");
        // Empty input is a no-op.
        let mut empty = HeaderMap::new();
        inject_additional_checksum_headers(&mut empty, &[]);
        assert!(empty.is_empty());
    }

    #[test]
    fn inject_accept_ranges_header_writes_static_bytes_value() {
        let mut headers = HeaderMap::new();
        inject_accept_ranges_header(&mut headers);

        assert_eq!(headers.get(http::header::ACCEPT_RANGES).unwrap(), ACCEPT_RANGES_BYTES);
    }

    #[test]
    fn internal_object_info_lookup_opts_drops_http_preconditions() {
        let version_id = Uuid::new_v4().to_string();
        let opts = ObjectOptions {
            version_id: Some(version_id.clone()),
            no_lock: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("\"etag\"".to_string()),
                if_match: Some("\"other\"".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let lookup_opts = internal_object_info_lookup_opts(opts);

        assert!(lookup_opts.http_preconditions.is_none());
        assert_eq!(lookup_opts.version_id.as_deref(), Some(version_id.as_str()));
        assert!(lookup_opts.no_lock);
    }

    fn bucket_sse_config_with(algorithm: &str, kms_key_id: Option<&str>) -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from(String::from(algorithm)),
                    kms_master_key_id: kms_key_id.map(|id| SSEKMSKeyId::from(id.to_string())),
                }),
                bucket_key_enabled: None,
            }],
        }
    }

    #[test]
    fn resolve_bucket_default_sse_prefers_the_request_over_the_bucket_default() {
        let config = bucket_sse_config_with(ServerSideEncryption::AWS_KMS, Some("bucket-key"));

        let (sse, kms_key_id) = resolve_bucket_default_sse(
            Some(&config),
            Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
            Some(SSEKMSKeyId::from("request-key".to_string())),
            false,
        );

        assert_eq!(sse.as_ref().map(|sse| sse.as_str()), Some(ServerSideEncryption::AES256));
        assert_eq!(kms_key_id.as_deref(), Some("request-key"));
    }

    #[test]
    fn resolve_bucket_default_sse_fills_gaps_from_the_bucket_default() {
        let config = bucket_sse_config_with(ServerSideEncryption::AWS_KMS, Some("bucket-key"));

        let (sse, kms_key_id) = resolve_bucket_default_sse(Some(&config), None, None, false);

        assert_eq!(sse.as_ref().map(|sse| sse.as_str()), Some(ServerSideEncryption::AWS_KMS));
        assert_eq!(kms_key_id.as_deref(), Some("bucket-key"));
    }

    #[test]
    fn resolve_bucket_default_sse_falls_back_to_aes256_for_an_unknown_algorithm() {
        // Reachable only through corrupt or hand-edited bucket metadata;
        // PutBucketEncryption rejects unknown algorithms. All three call sites
        // now share this single decision (backlog#1826).
        let config = bucket_sse_config_with("garbage", None);

        let (sse, kms_key_id) = resolve_bucket_default_sse(Some(&config), None, None, false);

        assert_eq!(sse.as_ref().map(|sse| sse.as_str()), Some(ServerSideEncryption::AES256));
        assert!(kms_key_id.is_none());
    }

    #[test]
    fn resolve_bucket_default_sse_suppresses_the_default_for_explicit_ssec() {
        let config = bucket_sse_config_with(ServerSideEncryption::AES256, Some("bucket-key"));

        let (sse, kms_key_id) = resolve_bucket_default_sse(Some(&config), None, None, true);

        assert!(sse.is_none(), "an SSE-C destination must not also get managed encryption");
        assert!(kms_key_id.is_none());
    }

    #[test]
    fn resolve_bucket_default_sse_returns_nothing_without_a_bucket_default() {
        let (sse, kms_key_id) = resolve_bucket_default_sse(None, None, None, false);

        assert!(sse.is_none());
        assert!(kms_key_id.is_none());
    }

    #[test]
    fn build_put_like_object_lock_metadata_rejects_mode_without_retain_until_date() {
        let err = build_put_like_object_lock_metadata(
            "test-bucket",
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            None,
            Some(ObjectLockMode::from_static(ObjectLockMode::GOVERNANCE)),
            None,
        )
        .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED));
    }

    #[test]
    fn object_lock_checks_required_reuses_authoritative_state() {
        assert!(!object_lock_checks_required_for_state(
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent
        ));

        let configured = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };
        assert!(object_lock_checks_required_for_state(&configured));
        assert!(object_lock_checks_required_for_state(&metadata_sys::ObjectLockConfigState::Fabricated));
    }

    #[test]
    fn build_put_like_object_lock_metadata_rejects_retain_until_date_without_mode() {
        let retain_until = Timestamp::from(OffsetDateTime::now_utc().add(time::Duration::days(1)));
        let err = build_put_like_object_lock_metadata(
            "test-bucket",
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            None,
            None,
            Some(retain_until),
        )
        .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED));
    }

    const NO_BUCKET_LOCK: metadata_sys::ObjectLockConfigState = metadata_sys::ObjectLockConfigState::ConfirmedAbsent;

    fn bucket_default_retention_state(mode: &'static str) -> metadata_sys::ObjectLockConfigState {
        metadata_sys::ObjectLockConfigState::Configured {
            config: s3s::dto::ObjectLockConfiguration {
                object_lock_enabled: Some(s3s::dto::ObjectLockEnabled::from_static(s3s::dto::ObjectLockEnabled::ENABLED)),
                rule: Some(s3s::dto::ObjectLockRule {
                    default_retention: Some(s3s::dto::DefaultRetention {
                        mode: Some(ObjectLockRetentionMode::from_static(mode)),
                        days: Some(1),
                        years: None,
                    }),
                }),
            },
            updated_at: OffsetDateTime::now_utc(),
        }
    }

    fn object_info_with_lock_metadata(metadata: HashMap<String, String>) -> ObjectInfo {
        ObjectInfo {
            user_defined: Arc::new(metadata),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        }
    }

    fn compliance_retained_object_info() -> ObjectInfo {
        let mut metadata = HashMap::new();
        metadata.insert(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string());
        metadata.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2030-01-01T00:00:00Z".to_string());
        object_info_with_lock_metadata(metadata)
    }

    fn legal_hold_object_info() -> ObjectInfo {
        let mut metadata = HashMap::new();
        metadata.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_string(), ObjectLockLegalHoldStatus::ON.to_string());
        object_info_with_lock_metadata(metadata)
    }

    #[test]
    fn validate_existing_object_lock_allows_versioned_new_version_with_compliance_retention() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: None,
            ..Default::default()
        };

        validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &compliance_retained_object_info(), &opts)
            .expect("versioned put should create a new version");
    }

    #[test]
    fn validate_existing_object_lock_allows_versioned_new_version_with_legal_hold() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: None,
            ..Default::default()
        };

        validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &legal_hold_object_info(), &opts)
            .expect("versioned put should create a new version");
    }

    #[test]
    fn validate_existing_object_lock_blocks_unversioned_compliance_overwrite() {
        let err = validate_existing_object_lock_for_write(
            &NO_BUCKET_LOCK,
            &compliance_retained_object_info(),
            &ObjectOptions::default(),
        )
        .expect_err("unversioned overwrite should still be blocked");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn validate_existing_object_lock_blocks_suspended_version_compliance_overwrite() {
        let opts = ObjectOptions {
            versioned: true,
            version_suspended: true,
            version_id: None,
            ..Default::default()
        };
        let err = validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &compliance_retained_object_info(), &opts)
            .expect_err("suspended versioning overwrite should still be blocked");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[test]
    fn validate_existing_object_lock_blocks_explicit_version_compliance_overwrite() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let err = validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &compliance_retained_object_info(), &opts)
            .expect_err("explicit version overwrite should still be blocked");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    /// The source's lock state governs the replica (rustfs/backlog#1953):
    /// an authorized replication write carrying the locking category's source
    /// timestamp may overwrite a locked version; the set layer's LWW then
    /// decides per category.
    #[test]
    fn validate_existing_object_lock_allows_authorized_replication_overwrite() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            replication_request: true,
            replication_retention_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            replication_legalhold_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &compliance_retained_object_info(), &opts)
            .expect("replication write must bypass the destination COMPLIANCE lock");
        validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &legal_hold_object_info(), &opts)
            .expect("replication write must bypass the destination legal hold");
    }

    /// Without the locking category's source timestamp the LWW merge cannot
    /// judge it, so the write stays rejected instead of lifting the lock.
    #[test]
    fn validate_existing_object_lock_rejects_replication_overwrite_without_lock_timestamp() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            replication_request: true,
            replication_tagging_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let err = validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &compliance_retained_object_info(), &opts)
            .expect_err("COMPLIANCE lock must hold without a retention source timestamp");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        let err = validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &legal_hold_object_info(), &opts)
            .expect_err("legal hold must hold without a legal-hold source timestamp");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    /// The bucket default retention locks a version without explicit
    /// retention keys; the pre-check judges the same authoritative state as
    /// the set-layer gate, so a tagging-only replication write is rejected
    /// and one carrying the retention source timestamp passes to LWW.
    #[test]
    fn validate_existing_object_lock_judges_bucket_default_retention_for_replication_overwrite() {
        let default_protected = object_info_with_lock_metadata(HashMap::new());
        let tagging_only = ObjectOptions {
            versioned: true,
            version_id: Some(Uuid::new_v4().to_string()),
            replication_request: true,
            replication_tagging_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let with_retention_decision = ObjectOptions {
            replication_retention_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            ..tagging_only.clone()
        };

        for mode in [ObjectLockRetentionMode::COMPLIANCE, ObjectLockRetentionMode::GOVERNANCE] {
            let state = bucket_default_retention_state(mode);
            let err = validate_existing_object_lock_for_write(&state, &default_protected, &tagging_only)
                .expect_err("bucket default retention must hold without a retention source timestamp");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{mode}");
            validate_existing_object_lock_for_write(&state, &default_protected, &with_retention_decision)
                .expect("the retention source timestamp hands the default retention to LWW");
        }

        // Without a bucket default the same version is simply unlocked.
        validate_existing_object_lock_for_write(&NO_BUCKET_LOCK, &default_protected, &tagging_only)
            .expect("no bucket default, no lock");
    }

    #[test]
    fn aws_chunked_put_prefers_decoded_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("aws-chunked"));
        headers.insert(AMZ_DECODED_CONTENT_LENGTH, HeaderValue::from_static("71680"));

        let decoded = decoded_content_length_from_headers(&headers).expect("decoded content length should parse");
        assert!(request_uses_aws_chunked(&headers));
        assert_eq!(decoded, Some(71680));

        let resolved = match (request_uses_aws_chunked(&headers), decoded, Some(99999)) {
            (true, Some(decoded), _) => decoded,
            (_, _, Some(c)) => c,
            (_, Some(decoded), None) => decoded,
            _ => unreachable!("test provides a valid size source"),
        };

        assert_eq!(resolved, 71680);
    }

    #[test]
    fn s3s_body_error_to_io_preserves_upload_stream_error_source() {
        let error = s3s_body_error_to_io(Box::new(MockUploadStreamSha256Mismatch));

        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<MockUploadStreamSha256Mismatch>()),
            Some(MockUploadStreamSha256Mismatch)
        ));
    }

    #[test]
    fn response_storage_class_reports_effective_layout_and_preserves_transition_tier() {
        let metadata = HashMap::new();
        let standard_info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD.to_string()),
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };
        assert!(response_storage_class(&standard_info, &metadata).is_none());

        let mut metadata = HashMap::new();
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string());
        let label_only_info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD_IA.to_string()),
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };
        assert!(
            response_storage_class(&label_only_info, &metadata).is_none(),
            "historical STANDARD_IA labels must report the effective implicit STANDARD layout"
        );

        let rrs_info = ObjectInfo {
            storage_class: Some(storageclass::RRS.to_string()),
            ..Default::default()
        };
        assert_eq!(
            response_storage_class(&rrs_info, &HashMap::new())
                .as_ref()
                .map(StorageClass::as_str),
            Some(storageclass::RRS)
        );

        let mut transitioned_info = label_only_info;
        transitioned_info.transitioned_object.tier = "WARM-TIER".to_string();
        assert!(
            response_storage_class(&transitioned_info, &metadata).is_none(),
            "a tier name without a completed transition must not override the effective local class"
        );
        transitioned_info.transitioned_object.status = rustfs_filemeta::TRANSITION_COMPLETE.to_string();
        assert_eq!(
            response_storage_class(&transitioned_info, &metadata)
                .as_ref()
                .map(StorageClass::as_str),
            Some("WARM-TIER")
        );

        let mut metadata = HashMap::new();
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD.to_string());
        let standard_metadata_info = ObjectInfo {
            storage_class: None,
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };
        assert!(
            response_storage_class(&standard_metadata_info, &metadata).is_none(),
            "STANDARD must be omitted even when it only arrives via metadata fallback"
        );
    }

    #[test]
    fn response_storage_class_for_object_attributes_defaults_to_standard_when_requested() {
        let metadata = HashMap::new();
        let info = ObjectInfo {
            storage_class: None,
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };

        assert_eq!(
            response_storage_class_for_object_attributes(&info, &metadata, true)
                .as_ref()
                .map(StorageClass::as_str),
            Some(storageclass::STANDARD)
        );

        let legacy_info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD_IA.to_string()),
            ..Default::default()
        };
        assert_eq!(
            response_storage_class_for_object_attributes(&legacy_info, &HashMap::new(), true)
                .as_ref()
                .map(StorageClass::as_str),
            Some(storageclass::STANDARD)
        );
    }

    #[test]
    fn response_storage_class_for_object_attributes_skips_value_when_not_requested() {
        let metadata = HashMap::new();
        let info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD_IA.to_string()),
            user_defined: Arc::new(metadata.clone()),
            ..Default::default()
        };

        assert!(
            response_storage_class_for_object_attributes(&info, &metadata, false).is_none(),
            "StorageClass must only be returned when explicitly requested"
        );
    }

    #[test]
    fn expected_current_version_header_normalizes_uuid_and_null() {
        let version = Uuid::new_v4();
        let mut headers = HeaderMap::new();
        headers.insert(
            RUSTFS_EXPECTED_CURRENT_VERSION_ID,
            HeaderValue::from_str(&version.to_string().to_uppercase()).unwrap(),
        );
        assert_eq!(expected_current_version_id(&headers).unwrap(), Some(version.to_string()));

        headers.insert(RUSTFS_EXPECTED_CURRENT_VERSION_ID, HeaderValue::from_static(" null "));
        assert_eq!(expected_current_version_id(&headers).unwrap(), Some(Uuid::nil().to_string()));
    }

    #[test]
    fn expected_current_version_header_rejects_empty_and_malformed_values() {
        for value in ["", "not-a-version"] {
            let mut headers = HeaderMap::new();
            headers.insert(RUSTFS_EXPECTED_CURRENT_VERSION_ID, HeaderValue::from_str(value).unwrap());
            assert_eq!(expected_current_version_id(&headers).unwrap_err().code(), &S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn build_put_object_expiration_header_returns_none_for_non_delete_events() {
        let event = lifecycle::Event {
            action: lifecycle::IlmAction::TransitionAction,
            rule_id: "rule-1".to_string(),
            due: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            noncurrent_days: 0,
            newer_noncurrent_versions: 0,
            storage_class: String::new(),
        };

        assert!(build_put_object_expiration_header(&event).is_none());
    }

    #[test]
    fn build_put_object_expiration_header_formats_expected_value() {
        let expire_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let event = lifecycle::Event {
            action: lifecycle::IlmAction::DeleteAction,
            rule_id: "rule-1".to_string(),
            due: Some(expire_time),
            noncurrent_days: 0,
            newer_noncurrent_versions: 0,
            storage_class: String::new(),
        };

        let expiry_date = expire_time.format(&Rfc3339).unwrap();
        let expected = format!("expiry-date=\"{}\", rule-id=\"rule-1\"", expiry_date);
        assert_eq!(build_put_object_expiration_header(&event), Some(expected));
    }

    #[test]
    fn build_put_object_expiration_header_requires_rule_id_and_due_time() {
        let event = lifecycle::Event {
            action: lifecycle::IlmAction::DeleteAction,
            rule_id: String::new(),
            due: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            noncurrent_days: 0,
            newer_noncurrent_versions: 0,
            storage_class: String::new(),
        };

        assert!(build_put_object_expiration_header(&event).is_none());

        let event = lifecycle::Event {
            action: lifecycle::IlmAction::DeleteAction,
            rule_id: "rule-1".to_string(),
            due: Some(OffsetDateTime::UNIX_EPOCH),
            noncurrent_days: 0,
            newer_noncurrent_versions: 0,
            storage_class: String::new(),
        };

        assert!(build_put_object_expiration_header(&event).is_none());
    }

    // -- Range: u64 -> i64 lossless conversion (issue rustfs/backlog#1322) --

    const I64_MAX_AS_U64: u64 = i64::MAX as u64;

    /// The conversion itself: s3s `Range` (u64) -> internal `HTTPRangeSpec`
    /// (i64). This directly guards the suffix truncation fix. Reverting to
    /// `length as i64` regresses the zero-suffix, `i64::MAX + 1` and `u64::MAX`
    /// rows below.
    #[test]
    fn range_to_http_range_spec_is_lossless() {
        // Zero-length suffix (`bytes=-0`) is unsatisfiable -> InvalidRange (416),
        // never a 0-length 206.
        let zero_suffix = range_to_http_range_spec(Range::Suffix { length: 0 });
        assert_eq!(
            zero_suffix.as_ref().err().map(|e| e.code()),
            Some(&S3ErrorCode::InvalidRange),
            "bytes=-0 must map to InvalidRange (416)"
        );

        // Suffix conversions: positive `start` holds the suffix length; values
        // above i64::MAX clamp to i64::MAX (they always cover the whole object).
        let suffix_cases = [
            (1_u64, 1_i64),
            (I64_MAX_AS_U64, i64::MAX),
            (I64_MAX_AS_U64 + 1, i64::MAX), // was i64::MIN under `as i64` -> checked_neg overflow
            (u64::MAX, i64::MAX),           // was -1 under `as i64` -> read as "last 1 byte"
        ];
        for (length, expected_start) in suffix_cases {
            let spec = range_to_http_range_spec(Range::Suffix { length })
                .unwrap_or_else(|_| panic!("suffix {length} must convert losslessly"));
            assert!(spec.is_suffix_length, "suffix {length} must stay a suffix spec");
            assert_eq!(spec.start, expected_start, "suffix {length} start");
            assert_eq!(spec.end, -1, "suffix {length} end");
        }

        // Int ranges: s3s already rejects first/last > i64::MAX, so the checked
        // cast never truncates. first-last and open-ended must not regress.
        let int_first_last = range_to_http_range_spec(Range::Int {
            first: 10,
            last: Some(20),
        })
        .expect("first-last converts");
        assert!(!int_first_last.is_suffix_length);
        assert_eq!((int_first_last.start, int_first_last.end), (10, 20));

        let int_open = range_to_http_range_spec(Range::Int { first: 5, last: None }).expect("open-ended converts");
        assert_eq!((int_open.start, int_open.end), (5, -1));

        let int_max = range_to_http_range_spec(Range::Int {
            first: I64_MAX_AS_U64,
            last: Some(I64_MAX_AS_U64),
        })
        .expect("i64::MAX int converts");
        assert_eq!((int_max.start, int_max.end), (i64::MAX, i64::MAX));
    }

    /// Observable end-to-end effect the GET/HEAD handlers derive from a range
    /// spec: `HTTPRangeSpec::get_offset_length` yields the (offset, length)
    /// that becomes `Content-Length` and `Content-Range`, or an error that
    /// surfaces as 416. Covers empty / 1-byte / normal objects.
    #[test]
    fn range_suffix_offset_length_matches_s3_semantics() {
        // Expected outcome for a satisfiable range, or `None` for 416.
        #[derive(Debug, PartialEq)]
        enum Outcome {
            /// (offset, content_length, content_range)
            Partial(usize, i64, String),
            Unsatisfiable,
        }

        fn derive(range: Range, size: i64) -> Outcome {
            let spec = match range_to_http_range_spec(range) {
                Ok(spec) => spec,
                Err(_) => return Outcome::Unsatisfiable,
            };
            match spec.get_offset_length(size) {
                Ok((offset, len)) => {
                    let content_range = format!("bytes {}-{}/{}", offset, offset as i64 + len - 1, size);
                    Outcome::Partial(offset, len, content_range)
                }
                Err(_) => Outcome::Unsatisfiable,
            }
        }

        let suffix = |length: u64| Range::Suffix { length };

        // size, range, expected
        let normal = 100_i64;
        let cases = [
            // Zero suffix is always 416, whatever the size.
            (0_i64, suffix(0), Outcome::Unsatisfiable),
            (1, suffix(0), Outcome::Unsatisfiable),
            (normal, suffix(0), Outcome::Unsatisfiable),
            // Suffix within the object returns the trailing bytes.
            (normal, suffix(1), Outcome::Partial(99, 1, "bytes 99-99/100".into())),
            (normal, suffix(normal as u64), Outcome::Partial(0, 100, "bytes 0-99/100".into())),
            // Suffix >= size returns the whole object (never a truncated tail).
            (normal, suffix(normal as u64 + 1), Outcome::Partial(0, 100, "bytes 0-99/100".into())),
            (normal, suffix(I64_MAX_AS_U64), Outcome::Partial(0, 100, "bytes 0-99/100".into())),
            (normal, suffix(I64_MAX_AS_U64 + 1), Outcome::Partial(0, 100, "bytes 0-99/100".into())),
            (normal, suffix(u64::MAX), Outcome::Partial(0, 100, "bytes 0-99/100".into())),
            // 1-byte object: any non-zero suffix returns that single byte.
            (1, suffix(1), Outcome::Partial(0, 1, "bytes 0-0/1".into())),
            (1, suffix(2), Outcome::Partial(0, 1, "bytes 0-0/1".into())),
            (1, suffix(I64_MAX_AS_U64 + 1), Outcome::Partial(0, 1, "bytes 0-0/1".into())),
            (1, suffix(u64::MAX), Outcome::Partial(0, 1, "bytes 0-0/1".into())),
            // Normal first-last and open-ended int ranges must not regress.
            (
                normal,
                Range::Int {
                    first: 10,
                    last: Some(19),
                },
                Outcome::Partial(10, 10, "bytes 10-19/100".into()),
            ),
            (
                normal,
                Range::Int { first: 90, last: None },
                Outcome::Partial(90, 10, "bytes 90-99/100".into()),
            ),
        ];

        for (size, range, expected) in cases {
            let got = derive(range, size);
            assert_eq!(got, expected, "size={size} range={range:?}");
        }
    }

    fn quota_result(allowed: bool) -> QuotaCheckResult {
        QuotaCheckResult {
            allowed,
            current_usage: Some(1024),
            quota_limit: Some(2048),
            operation_size: 512,
            remaining: Some(512),
            uses_durable_reservations: true,
        }
    }

    #[test]
    fn quota_admission_allows_within_limit() {
        let result = map_quota_check_outcome("bucket", Ok(quota_result(true))).expect("an allowed result admits the write");

        assert_eq!(result.current_usage, Some(1024));
        assert_eq!(result.quota_limit, Some(2048));
        assert_eq!(result.operation_size, 512);
        assert_eq!(result.remaining, Some(512));
    }

    #[test]
    fn quota_admission_rejects_over_limit() {
        let err = map_quota_check_outcome("bucket", Ok(quota_result(false))).expect_err("an over-limit result rejects the write");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn legacy_quota_admission_rejects_already_over_limit() {
        let result = QuotaCheckResult {
            allowed: true,
            current_usage: Some(6),
            quota_limit: Some(5),
            operation_size: 0,
            remaining: Some(0),
            uses_durable_reservations: false,
        };
        let mut opts = ObjectOptions::default();
        let err =
            apply_quota_admission(&mut opts, &result).expect_err("legacy completion must not bypass an already exceeded quota");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn quota_admission_fails_closed_on_checker_error() {
        // A configured hard quota must never be bypassed by an internal fault: a checker error becomes a retryable ServiceUnavailable, not a silent allow.
        let err = map_quota_check_outcome(
            "bucket",
            Err(QuotaError::InvalidConfig {
                reason: "corrupt quota config".to_string(),
            }),
        )
        .expect_err("a checker fault must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    }

    #[test]
    fn early_quota_filter_rejects_only_an_individually_impossible_object() {
        let stale_full_usage = QuotaCheckResult {
            allowed: true,
            current_usage: Some(4096),
            quota_limit: Some(4096),
            operation_size: 0,
            remaining: Some(0),
            uses_durable_reservations: true,
        };

        ensure_object_size_within_quota(&stale_full_usage, 4096)
            .expect("commit-time ledger must decide whether stale usage was reclaimed");
        let err = ensure_object_size_within_quota(&stale_full_usage, 4097)
            .expect_err("an object larger than the whole quota can never fit");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn quota_admission_fails_closed_on_unknown_authoritative_usage() {
        let err = map_quota_check_outcome(
            "bucket",
            Err(QuotaError::UsageUnavailable {
                bucket: "bucket".to_string(),
            }),
        )
        .expect_err("unknown authoritative usage must not admit a quota-controlled write");
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    }
}
