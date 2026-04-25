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

//! Object application use-case contracts.

use crate::app::context::{AppContext, default_notify_interface, get_global_app_context};
use crate::config::RustFSBufferConfig;
use crate::error::ApiError;
use crate::storage::access::{PostObjectRequestMarker, authorize_request, has_bypass_governance_header, req_info_mut};
use crate::storage::concurrency::{
    ConcurrencyManager, GetObjectGuard, get_concurrency_aware_buffer_size, get_concurrency_manager,
};
use crate::storage::ecfs::*;
use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use crate::storage::helper::{OperationHelper, spawn_background_with_context};
use crate::storage::options::{
    copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime_with_object_name,
    filter_object_metadata, get_content_sha256_with_query, get_opts, normalize_content_encoding_for_storage, put_opts,
};
use crate::storage::request_context::spawn_traced;
use crate::storage::s3_api::multipart::parse_list_parts_params;
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use crate::storage::*;
use bytes::Bytes;
use datafusion::arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::WriterBuilder as JsonWriterBuilder, json::writer::JsonArray,
};
use futures::{StreamExt, stream};
use http::{HeaderMap, HeaderValue, StatusCode};
use md5::Context as Md5Context;
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use rustfs_object_capacity::capacity_manager::get_capacity_manager;
// Performance metrics recording (with zero-copy-metrics integration)
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{RestoreRequestOps, enqueue_transition_immediate, post_restore_opts},
        lifecycle::{self, Lifecycle, TransitionOptions},
    },
    metadata_sys,
    object_lock::{
        objectlock::{get_object_legalhold_meta, get_object_retention_meta},
        objectlock_sys::{BucketObjectLockSys, check_object_lock_for_deletion, is_retention_active},
    },
    quota::QuotaOperation,
    replication::{
        DeletedObjectReplicationInfo, ObjectOpts as ReplicationObjectOpts, ReplicationConfigurationExt, check_replicate_delete,
        get_must_replicate_options, must_replicate, schedule_replication, schedule_replication_delete,
    },
    tagging::decode_tags,
    versioning::VersioningApi,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::config::storageclass;
use rustfs_ecstore::disk::{error::DiskError, error_reduce::is_all_buckets_not_found};
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{
    HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOperations, ObjectOptions, ObjectToDelete, PutObjReader, RangedReader,
};
use rustfs_filemeta::{
    REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicateTargetDecision, ReplicationState, ReplicationStatusType,
    ReplicationType, RestoreStatusOps, VersionPurgeStatusType, parse_restore_obj_status, replication_statuses_map,
    version_purge_statuses_map,
};
use rustfs_io_metrics;
use rustfs_notify::EventArgsBuilder;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_rio::{CompressReader, DecompressReader, DynReader, HashReader, wrap_reader};
use rustfs_s3_common::S3Operation;
use rustfs_s3select_api::{
    object_store::bytes_stream,
    query::{Context, Query},
};
use rustfs_s3select_query::get_global_db;
use rustfs_targets::EventName;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE, AMZ_WEBSITE_REDIRECT_LOCATION, CONTENT_TYPE,
    SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    headers::{
        AMZ_DECODED_CONTENT_LENGTH, AMZ_MINIO_SNOWBALL_IGNORE_DIRS, AMZ_MINIO_SNOWBALL_IGNORE_ERRORS, AMZ_MINIO_SNOWBALL_PREFIX,
        AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_MODE_LOWER,
        AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS,
        AMZ_RESTORE_REQUEST_DATE, AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS, AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, AMZ_RUSTFS_SNOWBALL_PREFIX,
        AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
        AMZ_SNOWBALL_EXTRACT, AMZ_SNOWBALL_IGNORE_DIRS, AMZ_SNOWBALL_IGNORE_ERRORS, AMZ_SNOWBALL_PREFIX, AMZ_STORAGE_CLASS,
        AMZ_TAG_COUNT,
    },
    insert_str, remove_str,
};
use rustfs_utils::path::{is_dir_object, path_join_buf};
use rustfs_utils::{
    CompressionAlgorithm, extract_params_header, extract_resp_elements, get_request_host, get_request_port,
    get_request_user_agent,
};
use rustfs_zip::CompressionFormat;
use s3s::dto::*;
use s3s::header::{X_AMZ_RESTORE, X_AMZ_RESTORE_OUTPUT_PATH};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::ops::Add;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tar::Archive;
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

struct DeadlockRequestGuard {
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
}

impl Drop for DeadlockRequestGuard {
    fn drop(&mut self) {
        self.deadlock_detector.unregister_request(&self.request_id);
    }
}

struct GetObjectBootstrap {
    timeout_config: TimeoutConfig,
    wrapper: RequestTimeoutWrapper,
    request_start: std::time::Instant,
    request_guard: GetObjectGuard,
    _deadlock_request_guard: DeadlockRequestGuard,
    concurrent_requests: usize,
}

struct GetObjectIoPlanning<'a> {
    _disk_permit: tokio::sync::SemaphorePermit<'a>,
    permit_wait_duration: Duration,
    queue_status: concurrency::IoQueueStatus,
    queue_utilization: f64,
}

struct GetObjectRequestContext {
    bucket: String,
    key: String,
    version_id_for_event: String,
    part_number: Option<usize>,
    rs: Option<HTTPRangeSpec>,
    opts: ObjectOptions,
}

struct GetObjectReadSetup {
    info: ObjectInfo,
    event_info: ObjectInfo,
    final_stream: DynReader,
    rs: Option<HTTPRangeSpec>,
    content_type: Option<ContentType>,
    last_modified: Option<Timestamp>,
    response_content_length: i64,
    content_range: Option<String>,
    server_side_encryption: Option<ServerSideEncryption>,
    sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    ssekms_key_id: Option<SSEKMSKeyId>,
    encryption_applied: bool,
}

struct GetObjectPreparedRead<'a> {
    io_planning: GetObjectIoPlanning<'a>,
    read_setup: GetObjectReadSetup,
}

struct GetObjectStrategyContext {
    #[allow(dead_code)]
    io_strategy: concurrency::IoStrategy,
    optimal_buffer_size: usize,
}

struct GetObjectOutputContext {
    output: GetObjectOutput,
    event_info: ObjectInfo,
    response_content_length: i64,
    optimal_buffer_size: usize,
}

async fn enqueue_transitioned_delete_cleanup(bucket: &str, object: &str, opts: &ObjectOptions, existing: Option<&ObjectInfo>) {
    let Some(existing) = existing else {
        return;
    };

    let je = if opts.delete_prefix {
        rustfs_ecstore::bucket::lifecycle::tier_sweeper::transitioned_force_delete_journal_entry(&existing.transitioned_object)
    } else {
        let version_id = opts.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok());
        rustfs_ecstore::bucket::lifecycle::tier_sweeper::transitioned_delete_journal_entry(
            version_id,
            opts.versioned,
            opts.version_suspended,
            &existing.transitioned_object,
        )
    };
    let Some(je) = je else {
        return;
    };

    let mut expiry_state = rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
        .write()
        .await;
    if let Err(err) = expiry_state.enqueue_tier_journal_entry(&je).await {
        warn!(
            bucket,
            object,
            remote_object = %existing.transitioned_object.name,
            remote_version_id = %existing.transitioned_object.version_id,
            tier = %existing.transitioned_object.tier,
            error = ?err,
            "failed to enqueue transitioned object cleanup"
        );
    }
}

pin_project! {
    struct ExtractArchiveEtagReader<R> {
        #[pin]
        inner: R,
        md5: Md5Context,
        finished: bool,
        etag: Arc<Mutex<Option<String>>>,
    }
}

impl<R> ExtractArchiveEtagReader<R> {
    fn new(inner: R, etag: Arc<Mutex<Option<String>>>) -> Self {
        Self {
            inner,
            md5: Md5Context::new(),
            finished: false,
            etag,
        }
    }
}

impl<R: AsyncRead> AsyncRead for ExtractArchiveEtagReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Ok(())) => {
                let filled = &buf.filled()[before..];
                if !filled.is_empty() {
                    this.md5.consume(filled);
                } else if !*this.finished {
                    *this.finished = true;
                    if let Ok(mut etag) = this.etag.lock() {
                        *etag = Some(format!("{:x}", this.md5.clone().finalize()));
                    }
                }
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
        }
    }
}

/// Determine if zero-copy write should be used for this PutObject operation.
///
/// Zero-copy is beneficial for large objects without encryption or compression.
///
/// # Arguments
///
/// * `size` - Object size in bytes
/// * `headers` - HTTP headers (to check for encryption/compression)
///
/// # Returns
///
/// `true` if zero-copy should be used, `false` otherwise
fn should_use_zero_copy(size: i64, headers: &HeaderMap) -> bool {
    // Only use zero-copy for objects larger than 1MB
    const ZERO_COPY_MIN_SIZE: i64 = 1024 * 1024;

    if size <= ZERO_COPY_MIN_SIZE {
        return false;
    }

    // Don't use zero-copy if encryption is requested
    if headers.get(AMZ_SERVER_SIDE_ENCRYPTION).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID).is_some()
    {
        return false;
    }

    // Don't use zero-copy if compression is likely (compressible content types)
    // The compression check happens later in the flow
    if let Some(content_type) = headers.get(CONTENT_TYPE)
        && let Ok(ct) = content_type.to_str()
    {
        // Skip zero-copy for easily compressible content types
        // since compression will be applied
        let compressible_types = [
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/javascript",
            "application/json",
            "application/xml",
            "text/xml",
        ];
        for ct_type in compressible_types {
            if ct.contains(ct_type) {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod deadlock_request_guard_tests {
    use super::DeadlockRequestGuard;
    use crate::storage::deadlock_detector::{DeadlockDetector, DeadlockDetectorConfig};
    use std::sync::Arc;

    #[test]
    fn deadlock_request_guard_unregisters_on_drop() {
        let detector = Arc::new(DeadlockDetector::new(DeadlockDetectorConfig {
            enabled: true,
            ..DeadlockDetectorConfig::default()
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
}

async fn maybe_enqueue_transition_immediate(obj_info: &ObjectInfo, src: LcEventSrc) {
    enqueue_transition_immediate(obj_info, src).await;
}

/// Extract trailing-header checksum values, overriding the corresponding input fields.
fn apply_trailing_checksums(
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

#[derive(Default)]
struct GetObjectChecksums {
    crc32: Option<String>,
    crc32c: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
    crc64nvme: Option<String>,
    checksum_type: Option<ChecksumType>,
}

#[derive(Default)]
struct PutObjectChecksums {
    crc32: Option<String>,
    crc32c: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
    crc64nvme: Option<String>,
}

fn normalize_delete_objects_version_id(version_id: Option<String>) -> Result<(Option<String>, Option<Uuid>), String> {
    let version_id = version_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    match version_id {
        Some(id) => {
            if id.eq_ignore_ascii_case("null") {
                Ok((Some("null".to_string()), Some(Uuid::nil())))
            } else {
                let uuid = Uuid::parse_str(&id).map_err(|e| e.to_string())?;
                Ok((Some(id), Some(uuid)))
            }
        }
        None => Ok((None, None)),
    }
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

fn delete_replication_state_from_config(
    config: &ReplicationConfiguration,
    obj_info: &ObjectInfo,
    version_id: Option<Uuid>,
    replica: bool,
) -> Option<ReplicationState> {
    let opts = ReplicationObjectOpts {
        name: obj_info.name.clone(),
        user_tags: obj_info.user_tags.clone(),
        version_id,
        delete_marker: obj_info.delete_marker,
        op_type: ReplicationType::Delete,
        replica,
        ..Default::default()
    };
    let target_arns = config.filter_target_arns(&opts);
    if target_arns.is_empty() {
        return None;
    }

    let mut decision = ReplicateDecision::new();
    for target_arn in target_arns {
        let mut target_opts = opts.clone();
        target_opts.target_arn = target_arn.clone();
        decision.set(ReplicateTargetDecision::new(target_arn, config.replicate(&target_opts), false));
    }
    if !decision.replicate_any() {
        return None;
    }

    let pending_status = decision.pending_status();
    let mut state = ReplicationState {
        replicate_decision_str: decision.to_string(),
        ..Default::default()
    };
    if version_id.is_some() {
        state.version_purge_status_internal = pending_status.clone();
        state.purge_targets = version_purge_statuses_map(pending_status.as_deref().unwrap_or_default());
    } else {
        state.replication_status_internal = pending_status.clone();
        state.targets = replication_statuses_map(pending_status.as_deref().unwrap_or_default());
    }
    Some(state)
}

async fn enrich_delete_replication_state_if_needed(
    bucket: &str,
    delete_object: &mut rustfs_ecstore::store_api::DeletedObject,
    obj_info: &ObjectInfo,
) {
    let Some(replication_state) = delete_object.replication_state.as_ref() else {
        return;
    };
    if obj_info.replication_status != ReplicationStatusType::Replica
        && !replication_state.replicate_decision_str.is_empty()
        && (!replication_state.targets.is_empty() || !replication_state.purge_targets.is_empty())
    {
        return;
    }

    let Ok((config, _)) = metadata_sys::get_replication_config(bucket).await else {
        return;
    };
    let version_id = if delete_object.delete_marker {
        None
    } else if delete_object.delete_marker_version_id.is_some() {
        delete_object.delete_marker_version_id
    } else {
        delete_object.version_id
    };
    if let Some(local_state) = delete_replication_state_from_config(
        &config,
        obj_info,
        version_id,
        obj_info.replication_status == ReplicationStatusType::Replica,
    ) {
        delete_object.replication_state = Some(local_state);
    }
}

fn should_schedule_delete_replication(
    opts: &ObjectOptions,
    replication_source: &ObjectInfo,
    deleted_delete_marker_version: bool,
) -> bool {
    if opts.replication_request {
        return false;
    }

    if opts.version_id.is_some() && !deleted_delete_marker_version && !replication_source.delete_marker {
        return matches!(
            replication_source.replication_status,
            ReplicationStatusType::Replica
                | ReplicationStatusType::Pending
                | ReplicationStatusType::Completed
                | ReplicationStatusType::Failed
        );
    }

    replication_source.replication_status == ReplicationStatusType::Replica
        || replication_source.replication_status == ReplicationStatusType::Pending
        || replication_source.version_purge_status == VersionPurgeStatusType::Pending
        || (deleted_delete_marker_version && replication_source.replication_status == ReplicationStatusType::Completed)
}

async fn should_schedule_replica_delete_replication(
    bucket: &str,
    replication_source: &ObjectInfo,
    version_id: Option<Uuid>,
) -> bool {
    let Ok((config, _)) = metadata_sys::get_replication_config(bucket).await else {
        return false;
    };

    delete_replication_state_from_config(&config, replication_source, version_id, true).is_some()
}

fn delete_replication_version_id(replication_source: &ObjectInfo, deleted_delete_marker_version: bool) -> Option<Uuid> {
    if replication_source.delete_marker && !deleted_delete_marker_version {
        None
    } else {
        replication_source.version_id
    }
}

fn should_use_existing_delete_replication_info(opts: &ObjectOptions) -> bool {
    opts.version_id.is_some() && !opts.delete_marker
}

fn delete_replication_state_source<'a>(
    opts: &ObjectOptions,
    existing_object_info: Option<&'a ObjectInfo>,
    deleted_object_info: &'a ObjectInfo,
) -> &'a ObjectInfo {
    if opts.replication_request
        && deleted_object_info.delete_marker
        && let Some(existing) = existing_object_info
    {
        return existing;
    }

    deleted_object_info
}

const AMZ_SNOWBALL_EXTRACT_COMPAT: &str = "X-Amz-Snowball-Auto-Extract";
#[cfg(test)]
const AMZ_SNOWBALL_PREFIX_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Prefix";
#[cfg(test)]
const AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Dirs";
#[cfg(test)]
const AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Errors";
const AMZ_META_PREFIX_LOWER: &str = "x-amz-meta-";
const SNOWBALL_PREFIX_SUFFIX_LOWER: &str = "snowball-prefix";
const SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER: &str = "snowball-ignore-dirs";
const SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER: &str = "snowball-ignore-errors";
const SNOWBALL_PREFIX_HEADER_KEYS: &[&str] = &[AMZ_MINIO_SNOWBALL_PREFIX, AMZ_SNOWBALL_PREFIX, AMZ_RUSTFS_SNOWBALL_PREFIX];
const SNOWBALL_IGNORE_DIRS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_DIRS,
    AMZ_SNOWBALL_IGNORE_DIRS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
];
const SNOWBALL_IGNORE_ERRORS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PutObjectExtractOptions {
    prefix: Option<String>,
    ignore_dirs: bool,
    ignore_errors: bool,
}

fn header_value_is_true(headers: &HeaderMap, key: &str) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

fn is_put_object_extract_requested(headers: &HeaderMap) -> bool {
    header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT) || header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT_COMPAT)
}

fn trimmed_header_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
}

fn is_exact_snowball_meta_key(key: &str, exact_keys: &[&str]) -> bool {
    exact_keys.iter().any(|exact_key| key.eq_ignore_ascii_case(exact_key))
}

fn snowball_meta_value_by_suffix(headers: &HeaderMap, suffix_lower: &str, exact_keys: &[&str]) -> Option<String> {
    for (name, value) in headers {
        let key = name.as_str();
        if key.starts_with(AMZ_META_PREFIX_LOWER)
            && key.ends_with(suffix_lower)
            && !is_exact_snowball_meta_key(key, exact_keys)
            && let Ok(parsed) = value.to_str()
        {
            return Some(parsed.trim().to_string());
        }
    }

    None
}

fn snowball_meta_value(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> Option<String> {
    for key in exact_keys {
        if let Some(value) = trimmed_header_value(headers, key) {
            return Some(value);
        }
    }

    snowball_meta_value_by_suffix(headers, suffix_lower, exact_keys)
}

fn snowball_meta_flag(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> bool {
    snowball_meta_value(headers, exact_keys, suffix_lower).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn normalize_snowball_prefix(prefix: &str) -> Option<String> {
    let normalized = prefix.trim().trim_matches('/');
    if normalized.is_empty() {
        return None;
    }

    Some(normalized.to_string())
}

fn normalize_extract_entry_key(path: &str, prefix: Option<&str>, is_dir: bool) -> String {
    let path = path.trim_matches('/');
    let mut key = match prefix {
        Some(prefix) if !path.is_empty() => format!("{prefix}/{path}"),
        Some(prefix) => prefix.to_string(),
        None => path.to_string(),
    };

    if is_dir && !key.ends_with('/') {
        key.push('/');
    }

    key
}

fn map_extract_archive_error(err: impl std::fmt::Display) -> S3Error {
    s3_error!(InvalidArgument, "Failed to process archive entry: {}", err)
}

async fn apply_extract_entry_pax_extensions<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> S3Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry.pax_extensions().await.map_err(map_extract_archive_error)? else {
        return Ok(());
    };

    for ext in extensions {
        let ext = ext.map_err(map_extract_archive_error)?;
        let key = ext.key().map_err(map_extract_archive_error)?;
        let value = ext.value().map_err(map_extract_archive_error)?;

        if let Some(meta_key) = key.strip_prefix("minio.metadata.") {
            let meta_key = meta_key.strip_prefix("x-amz-meta-").unwrap_or(meta_key);
            if !meta_key.is_empty() {
                metadata.insert(meta_key.to_string(), value.to_string());
            }
            continue;
        }

        if key == "minio.versionId" && !value.is_empty() {
            opts.version_id = Some(value.to_string());
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_put_request_metadata(
    metadata: &mut HashMap<String, String>,
    headers: &HeaderMap,
    object_name: &str,
    cache_control: Option<CacheControl>,
    content_disposition: Option<ContentDisposition>,
    content_encoding: Option<ContentEncoding>,
    content_language: Option<ContentLanguage>,
    content_type: Option<ContentType>,
    expires: Option<Timestamp>,
    website_redirect_location: Option<WebsiteRedirectLocation>,
    tagging: Option<TaggingHeader>,
    storage_class: Option<StorageClass>,
) -> S3Result<()> {
    if let Some(cache_control) = cache_control {
        metadata.insert("cache-control".to_string(), cache_control);
    }
    if let Some(content_disposition) = content_disposition {
        metadata.insert("content-disposition".to_string(), content_disposition);
    }
    if let Some(content_encoding) = content_encoding
        && let Some(normalized_content_encoding) = normalize_content_encoding_for_storage(&content_encoding)
    {
        metadata.insert("content-encoding".to_string(), normalized_content_encoding);
    }
    if let Some(content_language) = content_language {
        metadata.insert("content-language".to_string(), content_language);
    }
    if let Some(content_type) = content_type {
        metadata.insert("content-type".to_string(), content_type);
    }
    if let Some(expires) = expires {
        let mut formatted = Vec::new();
        expires
            .format(TimestampFormat::HttpDate, &mut formatted)
            .map_err(|e| ApiError::from(StorageError::other(format!("Invalid expires timestamp: {e}"))))?;
        metadata.insert("expires".to_string(), String::from_utf8_lossy(&formatted).into_owned());
    }
    if let Some(website_redirect_location) = website_redirect_location {
        metadata.insert(AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), website_redirect_location);
    }
    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storage_class.as_str().to_string());
    }

    extract_metadata_from_mime_with_object_name(headers, metadata, true, Some(object_name));
    Ok(())
}

fn response_storage_class(info: &ObjectInfo, metadata: &HashMap<String, String>) -> Option<StorageClass> {
    info.storage_class
        .clone()
        .or_else(|| metadata.get(AMZ_STORAGE_CLASS).cloned())
        .filter(|storage_class| !storage_class.is_empty() && storage_class != storageclass::STANDARD)
        .map(StorageClass::from)
}

async fn apply_put_request_object_lock_opts(
    bucket: &str,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    if let Some(eval_metadata) = build_put_like_object_lock_metadata(
        bucket,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    )
    .await?
    {
        opts.eval_metadata = Some(eval_metadata);
    }

    Ok(())
}

pub(crate) async fn build_put_like_object_lock_metadata(
    bucket: &str,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
) -> S3Result<Option<HashMap<String, String>>> {
    if object_lock_legal_hold_status.is_none() && object_lock_mode.is_none() && object_lock_retain_until_date.is_none() {
        return Ok(None);
    }

    validate_bucket_object_lock_enabled(bucket).await?;

    let retention = match (object_lock_mode, object_lock_retain_until_date) {
        (Some(mode), retain_until_date) => Some(ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from(mode.as_str().to_string())),
            retain_until_date,
        }),
        (None, Some(retain_until_date)) => Some(ObjectLockRetention {
            mode: None,
            retain_until_date: Some(retain_until_date),
        }),
        (None, None) => None,
    };

    let mut eval_metadata = parse_object_lock_retention(retention)?;
    eval_metadata.extend(parse_object_lock_legal_hold(
        object_lock_legal_hold_status.map(|status| ObjectLockLegalHold { status: Some(status) }),
    )?);

    if eval_metadata.is_empty() {
        return Ok(None);
    }

    Ok(Some(eval_metadata))
}

pub(crate) fn validate_existing_object_lock_for_write(existing_obj_info: &ObjectInfo) -> S3Result<()> {
    let legal_hold = get_object_legalhold_meta(&existing_obj_info.user_defined);
    if legal_hold
        .status
        .as_ref()
        .is_some_and(|status| status.as_str() == ObjectLockLegalHoldStatus::ON)
    {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "Object has a legal hold and cannot be overwritten. Remove the legal hold first.".to_string(),
        ));
    }

    let retention = get_object_retention_meta(&existing_obj_info.user_defined);
    if let Some(mode) = retention.mode.as_ref()
        && mode.as_str() == ObjectLockRetentionMode::COMPLIANCE
        && is_retention_active(mode.as_str(), retention.retain_until_date.as_ref())
    {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "Object is under COMPLIANCE retention and cannot be overwritten.".to_string(),
        ));
    }

    Ok(())
}

fn delete_creates_delete_marker(opts: &ObjectOptions) -> bool {
    opts.version_id.is_none() && opts.versioned && !opts.version_suspended
}

fn resolve_put_object_extract_options(headers: &HeaderMap) -> PutObjectExtractOptions {
    let prefix = snowball_meta_value(headers, SNOWBALL_PREFIX_HEADER_KEYS, SNOWBALL_PREFIX_SUFFIX_LOWER)
        .and_then(|value| normalize_snowball_prefix(&value));
    let ignore_dirs = snowball_meta_flag(headers, SNOWBALL_IGNORE_DIRS_HEADER_KEYS, SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER);
    let ignore_errors = snowball_meta_flag(headers, SNOWBALL_IGNORE_ERRORS_HEADER_KEYS, SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER);

    PutObjectExtractOptions {
        prefix,
        ignore_dirs,
        ignore_errors,
    }
}

fn is_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    input
        .server_side_encryption
        .as_ref()
        .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || input.ssekms_key_id.is_some()
        || headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
}

fn is_post_object_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    is_sse_kms_requested(input, headers)
}

async fn resolve_put_object_expiration(bucket: &str, obj_info: &ObjectInfo) -> Option<String> {
    let Ok((lifecycle_config, _)) = metadata_sys::get_lifecycle_config(bucket).await else {
        debug!("resolve_put_object_expiration: lifecycle config not found for bucket {bucket}");
        return None;
    };

    let obj_opts = lifecycle::ObjectOpts::from_object_info(obj_info);
    let event = lifecycle_config.predict_expiration(&obj_opts).await;
    debug!(
        "resolve_put_object_expiration: bucket={bucket}, action={:?}, rule_id={}, due={:?}",
        event.action, event.rule_id, event.due
    );
    build_put_object_expiration_header(&event)
}

#[derive(Clone, Default)]
pub struct DefaultObjectUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultObjectUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    fn bucket_metadata_sys(&self) -> Option<Arc<RwLock<metadata_sys::BucketMetadataSys>>> {
        self.context.as_ref().and_then(|context| context.bucket_metadata().handle())
    }

    fn base_buffer_size(&self) -> usize {
        self.context
            .clone()
            .or_else(get_global_app_context)
            .map(|context| context.buffer_config().get().base_config.default_unknown)
            .unwrap_or_else(|| RustFSBufferConfig::default().base_config.default_unknown)
    }

    async fn check_bucket_quota(&self, bucket: &str, op: QuotaOperation, size: u64) -> S3Result<()> {
        let Some(metadata_sys) = self.bucket_metadata_sys() else {
            return Ok(());
        };
        let quota_checker = QuotaChecker::new(metadata_sys);
        match quota_checker.check_quota(bucket, op, size).await {
            Ok(result) if !result.allowed => Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!(
                    "Bucket quota exceeded. Current usage: {} bytes, limit: {} bytes",
                    result.current_usage.unwrap_or(0),
                    result.quota_limit.unwrap_or(0)
                ),
            )),
            Err(e) => {
                warn!("Quota check failed for bucket {bucket}: {e}, allowing operation");
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn build_memory_blob(buf: Vec<u8>, response_content_length: i64, _optimal_buffer_size: usize) -> Option<StreamingBlob> {
        Some(StreamingBlob::wrap(bytes_stream(
            stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(buf)) }),
            response_content_length as usize,
        )))
    }

    fn build_reader_blob<R>(reader: R, response_content_length: i64, optimal_buffer_size: usize) -> Option<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        Some(StreamingBlob::wrap(bytes_stream(
            ReaderStream::with_capacity(reader, optimal_buffer_size),
            response_content_length as usize,
        )))
    }

    fn init_get_object_bootstrap(bucket: &str, key: &str, request_id: &str) -> S3Result<GetObjectBootstrap> {
        let timeout_config = TimeoutConfig::from_env();
        let wrapper = RequestTimeoutWrapper::with_request_id(timeout_config.clone(), request_id.to_string());
        let request_start = std::time::Instant::now();
        let request_guard = ConcurrencyManager::track_request();
        let concurrent_requests = GetObjectGuard::concurrent_requests();

        let deadlock_detector = deadlock_detector::get_deadlock_detector();
        let request_id = wrapper.request_id().to_string();
        deadlock_detector.register_request(&request_id, format!("GetObject {bucket}/{key}"));
        let deadlock_request_guard = DeadlockRequestGuard::new(deadlock_detector, request_id);

        if wrapper.is_timeout() {
            warn!(
                bucket = %bucket,
                key = %key,
                timeout_secs = timeout_config.get_object_timeout.as_secs(),
                elapsed_ms = wrapper.elapsed().as_millis(),
                "GetObject request timed out before processing"
            );
            return Err(s3_error!(InternalError, "Request timeout before processing"));
        }

        rustfs_io_metrics::record_get_object_request_start(concurrent_requests);

        debug!(
            "GetObject request started with {} concurrent requests, timeout={:?}",
            concurrent_requests, timeout_config.get_object_timeout
        );

        Ok(GetObjectBootstrap {
            timeout_config,
            wrapper,
            request_start,
            request_guard,
            _deadlock_request_guard: deadlock_request_guard,
            concurrent_requests,
        })
    }

    async fn acquire_get_object_io_planning<'a>(
        manager: &'a ConcurrencyManager,
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &TimeoutConfig,
        bucket: &str,
        key: &str,
    ) -> S3Result<GetObjectIoPlanning<'a>> {
        let permit_wait_start = std::time::Instant::now();
        let disk_permit = manager
            .acquire_disk_read_permit()
            .await
            .map_err(|_| s3_error!(InternalError, "disk read semaphore closed"))?;
        let permit_wait_duration = permit_wait_start.elapsed();

        if wrapper.is_timeout() {
            warn!(
                bucket = %bucket,
                key = %key,
                wait_ms = permit_wait_duration.as_millis(),
                timeout_secs = timeout_config.get_object_timeout.as_secs(),
                elapsed_ms = wrapper.elapsed().as_millis(),
                "GetObject request timed out while waiting for disk permit"
            );

            rustfs_io_metrics::record_get_object_timeout(Some("disk_permit"), Some(wrapper.elapsed().as_secs_f64()));
            return Err(s3_error!(InternalError, "Request timeout while waiting for disk permit"));
        }

        let queue_status = manager.io_queue_status();
        let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
            queue_status.total_permits,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        );
        let queue_utilization = queue_snapshot.utilization_percent();

        if queue_snapshot.is_congested(80.0) {
            warn!(
                bucket = %bucket,
                key = %key,
                queue_utilization = format!("{:.1}%", queue_utilization),
                permits_in_use = queue_status.permits_in_use,
                total_permits = queue_status.total_permits,
                "I/O queue congestion detected"
            );

            rustfs_io_metrics::record_io_queue_congestion();
        }

        if wrapper.is_timeout() {
            warn!(
                bucket = %bucket,
                key = %key,
                timeout_secs = timeout_config.get_object_timeout.as_secs(),
                elapsed_ms = wrapper.elapsed().as_millis(),
                "GetObject request timed out before reading object"
            );
            rustfs_io_metrics::record_get_object_timeout(Some("before_read"), Some(wrapper.elapsed().as_secs_f64()));
            return Err(s3_error!(InternalError, "Request timeout before reading object"));
        }

        Ok(GetObjectIoPlanning {
            _disk_permit: disk_permit,
            permit_wait_duration,
            queue_status,
            queue_utilization,
        })
    }

    async fn prepare_get_object_request_context(req: &S3Request<GetObjectInput>) -> S3Result<GetObjectRequestContext> {
        let GetObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            ..
        } = req.input.clone();

        validate_object_key(&key, "GET")?;

        let part_number = part_number.map(|v| v as usize);

        if let Some(part_num) = part_number
            && part_num == 0
        {
            return Err(s3_error!(InvalidArgument, "Invalid part number: part number must be greater than 0"));
        }

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        Ok(GetObjectRequestContext {
            version_id_for_event: version_id.unwrap_or_default(),
            bucket,
            key,
            part_number,
            rs,
            opts,
        })
    }
    #[allow(clippy::too_many_arguments)]
    async fn prepare_get_object_read_execution<'a>(
        req: &S3Request<GetObjectInput>,
        manager: &'a ConcurrencyManager,
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &TimeoutConfig,
        bucket: &str,
        key: &str,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
        part_number: Option<usize>,
    ) -> S3Result<GetObjectPreparedRead<'a>> {
        let h = HeaderMap::new();
        let io_planning = Self::acquire_get_object_io_planning(manager, wrapper, timeout_config, bucket, key).await?;
        let store = get_validated_store(bucket).await?;

        let read_start = std::time::Instant::now();
        let read_setup =
            Self::prepare_get_object_read(req, &store, manager, bucket, key, rs, h, opts, part_number, read_start).await?;

        Ok(GetObjectPreparedRead { io_planning, read_setup })
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_get_object_read(
        req: &S3Request<GetObjectInput>,
        store: &rustfs_ecstore::store::ECStore,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        rs: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
        _part_number: Option<usize>,
        read_start: std::time::Instant,
    ) -> S3Result<GetObjectReadSetup> {
        let reader = store
            .get_object_reader(bucket, key, rs.clone(), h, opts)
            .await
            .map_err(ApiError::from)?;

        let read_plan = reader.read_plan.clone();
        let info = reader.object_info;

        use rustfs_io_metrics::{record_memory_copy_saved, record_zero_copy_read};
        let read_duration = read_start.elapsed();
        let estimated_saved = (info.size * 2) as usize;
        record_zero_copy_read(info.size as usize, read_duration.as_secs_f64() * 1000.0);
        record_memory_copy_saved(estimated_saved);

        manager.record_disk_operation(info.size as u64, read_duration, true).await;

        check_preconditions(&req.headers, &info)?;

        debug!(object_size = info.size, part_count = info.parts.len(), "GET object metadata snapshot");
        for part in &info.parts {
            debug!(
                part_number = part.number,
                part_size = part.size,
                part_actual_size = part.actual_size,
                "GET object part details"
            );
        }

        let event_info = info.clone();
        let content_type = if let Some(content_type) = &info.content_type {
            match ContentType::from_str(content_type) {
                Ok(res) => Some(res),
                Err(err) => {
                    error!("parse content-type err {} {:?}", content_type, err);
                    None
                }
            }
        } else {
            None
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        validate_sse_headers_for_read(&info.user_defined, &req.headers)?;
        let rs = read_plan.plaintext_range.as_ref().map(|range| HTTPRangeSpec {
            is_suffix_length: false,
            start: range.start as i64,
            end: range.start as i64 + range.length - 1,
        });
        let content_range = read_plan.content_range();

        debug!(
            "GET object metadata check: parts={}, provided_sse_key={:?}",
            info.parts.len(),
            req.input.sse_customer_key.is_some()
        );

        let decryption_request = DecryptionRequest {
            bucket,
            key,
            metadata: &info.user_defined,
            sse_customer_key: req.input.sse_customer_key.as_ref(),
            sse_customer_key_md5: req.input.sse_customer_key_md5.as_ref(),
            part_number: None,
            parts: &info.parts,
            etag: info.etag.as_deref(),
        };

        let response_content_length = read_plan.response_length;
        let mut final_stream: DynReader = wrap_reader(reader.stream);
        let mut server_side_encryption = None;
        let mut sse_customer_algorithm = None;
        let mut sse_customer_key_md5 = None;
        let mut ssekms_key_id = None;
        let mut encryption_applied = false;

        let mut decryption_material = sse_decryption(decryption_request).await?;

        for transform in &read_plan.transforms {
            match transform {
                rustfs_ecstore::store_api::ReadTransform::Decrypt => {
                    let Some(material) = decryption_material.take() else {
                        return Err(ApiError::from(StorageError::other("ecstore read plan requires decryption material")).into());
                    };
                    server_side_encryption = Some(material.server_side_encryption.clone());
                    sse_customer_algorithm = Some(material.algorithm.clone());
                    sse_customer_key_md5 = material.customer_key_md5.clone();
                    ssekms_key_id = material.kms_key_id.clone();
                    final_stream = material
                        .wrap_reader(final_stream, read_plan.plaintext_size)
                        .await
                        .map_err(ApiError::from)?
                        .0;
                    encryption_applied = true;
                }
                rustfs_ecstore::store_api::ReadTransform::Decompress { algorithm } => {
                    final_stream = Box::new(DecompressReader::new(final_stream, *algorithm));
                }
                rustfs_ecstore::store_api::ReadTransform::Slice {
                    offset,
                    length,
                    total_size,
                } => {
                    final_stream = Box::new(
                        RangedReader::new(
                            final_stream,
                            *offset,
                            *length,
                            usize::try_from(*total_size).map_err(|_| {
                                ApiError::from(StorageError::other(format!("invalid plaintext size {total_size}")))
                            })?,
                        )
                        .map_err(ApiError::from)?,
                    );
                }
            }
        }

        Ok(GetObjectReadSetup {
            info,
            event_info,
            final_stream,
            rs,
            content_type,
            last_modified,
            response_content_length,
            content_range,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn finalize_get_object_strategy(
        &self,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        info: &ObjectInfo,
        rs: Option<&HTTPRangeSpec>,
        response_content_length: i64,
        permit_wait_duration: Duration,
        queue_utilization: f64,
        queue_status: &concurrency::IoQueueStatus,
        concurrent_requests: usize,
    ) -> GetObjectStrategyContext {
        let base_buffer_size = self.base_buffer_size();

        let is_sequential_hint = if rs.is_none() {
            true
        } else if let Some(range_spec) = rs {
            range_spec.start == 0 && !range_spec.is_suffix_length
        } else {
            false
        };

        if let Some(range_spec) = rs
            && range_spec.start >= 0
        {
            manager.record_access(range_spec.start as u64, response_content_length as u64);
        }

        if response_content_length > 0 {
            manager.record_transfer(response_content_length as u64, permit_wait_duration);
        }

        let io_strategy =
            manager.calculate_io_strategy_with_context(info.size, base_buffer_size, permit_wait_duration, is_sequential_hint);

        debug!(
            wait_ms = permit_wait_duration.as_millis() as u64,
            load_level = ?io_strategy.load_level,
            buffer_size = io_strategy.buffer_size,
            buffer_multiplier = io_strategy.buffer_multiplier,
            readahead = io_strategy.enable_readahead,
            storage_media = ?io_strategy.storage_media,
            access_pattern = ?io_strategy.access_pattern,
            bandwidth_tier = ?io_strategy.bandwidth_tier,
            concurrent_requests = io_strategy.concurrent_requests,
            file_size = info.size,
            is_sequential = is_sequential_hint,
            "Enhanced multi-factor I/O strategy calculated"
        );

        let io_priority = manager.get_io_priority(response_content_length);

        if manager.is_priority_scheduling_enabled() {
            debug!(
                bucket = %bucket,
                key = %key,
                priority = %io_priority,
                request_size = response_content_length,
                "I/O priority assigned (based on actual request size)"
            );

            rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());
        }

        rustfs_io_metrics::record_get_object_io_state(
            permit_wait_duration.as_secs_f64(),
            queue_utilization,
            queue_status.permits_in_use,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
            io_strategy.load_level.as_str(),
            io_strategy.buffer_multiplier,
        );
        rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());

        debug!(
            actual_request_size = response_content_length,
            priority = %io_priority.as_str(),
            "I/O priority finalized with actual request size"
        );

        let base_buffer_size = get_buffer_size_opt_in(response_content_length);
        let optimal_buffer_size = if io_strategy.buffer_size > 0 {
            io_strategy.buffer_size.min(base_buffer_size)
        } else {
            get_concurrency_aware_buffer_size(response_content_length, base_buffer_size)
        };

        debug!(
            "GetObject buffer sizing: file_size={}, base={}, optimal={}, concurrent_requests={}, io_strategy={:?}",
            response_content_length, base_buffer_size, optimal_buffer_size, concurrent_requests, io_strategy.load_level
        );

        GetObjectStrategyContext {
            io_strategy,
            optimal_buffer_size,
        }
    }

    fn build_get_object_checksums(
        info: &ObjectInfo,
        headers: &HeaderMap,
        part_number: Option<usize>,
        rs: Option<&HTTPRangeSpec>,
    ) -> S3Result<GetObjectChecksums> {
        let mut checksums = GetObjectChecksums::default();

        if let Some(checksum_mode) = headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (decrypted_checksums, _is_multipart) =
                info.decrypt_checksums(part_number.unwrap_or(0), headers).map_err(|e| {
                    error!("decrypt_checksums error: {}", e);
                    ApiError::from(e)
                })?;

            for (key, checksum) in decrypted_checksums {
                if key == AMZ_CHECKSUM_TYPE {
                    checksums.checksum_type = Some(ChecksumType::from(checksum));
                    continue;
                }

                match rustfs_rio::ChecksumType::from_string(key.as_str()) {
                    rustfs_rio::ChecksumType::CRC32 => checksums.crc32 = Some(checksum),
                    rustfs_rio::ChecksumType::CRC32C => checksums.crc32c = Some(checksum),
                    rustfs_rio::ChecksumType::SHA1 => checksums.sha1 = Some(checksum),
                    rustfs_rio::ChecksumType::SHA256 => checksums.sha256 = Some(checksum),
                    rustfs_rio::ChecksumType::CRC64_NVME => checksums.crc64nvme = Some(checksum),
                    _ => (),
                }
            }
        }

        Ok(checksums)
    }
    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body<R>(
        mut final_stream: R,
        _info: &ObjectInfo,
        response_content_length: i64,
        optimal_buffer_size: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
    ) -> S3Result<Option<StreamingBlob>>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        if encryption_applied {
            let seekable_object_size_threshold = rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD;
            let should_buffer_encrypted_object = response_content_length > 0
                && response_content_length <= seekable_object_size_threshold as i64
                && part_number.is_none()
                && !has_range;

            if should_buffer_encrypted_object {
                let mut buf = Vec::with_capacity(response_content_length as usize);
                if let Err(e) = tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                    error!("Failed to read decrypted object into memory: {}", e);
                    return Err(ApiError::from(StorageError::other(format!("Failed to read decrypted object: {e}"))).into());
                }

                if buf.len() != response_content_length as usize {
                    warn!(
                        "Encrypted object size mismatch during read: expected={} actual={}",
                        response_content_length,
                        buf.len()
                    );
                }

                return Ok(Self::build_memory_blob(buf, response_content_length, optimal_buffer_size));
            }

            info!(
                "Encrypted object: Using unlimited stream for decryption with buffer size {}",
                optimal_buffer_size
            );
            return Ok(Self::build_reader_blob(final_stream, response_content_length, optimal_buffer_size));
        }

        let seekable_object_size_threshold = rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD;
        let should_provide_seek_support = response_content_length > 0
            && response_content_length <= seekable_object_size_threshold as i64
            && part_number.is_none()
            && !has_range;

        if should_provide_seek_support {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            match tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                Ok(_) => {
                    if buf.len() != response_content_length as usize {
                        warn!(
                            "Object size mismatch during seek support read: expected={} actual={}",
                            response_content_length,
                            buf.len()
                        );
                    }

                    return Ok(Self::build_memory_blob(buf, response_content_length, optimal_buffer_size));
                }
                Err(e) => {
                    error!("Failed to read object into memory for seek support: {}", e);
                }
            }
        }

        Ok(Self::build_reader_blob(final_stream, response_content_length, optimal_buffer_size))
    }

    fn put_object_execution_context(req: &S3Request<PutObjectInput>) -> (EventName, QuotaOperation, &'static str) {
        if req.extensions.get::<PostObjectRequestMarker>().is_some() {
            (EventName::ObjectCreatedPost, QuotaOperation::PostObject, "POST")
        } else {
            (EventName::ObjectCreatedPut, QuotaOperation::PutObject, "PUT")
        }
    }

    #[instrument(level = "debug", skip(self, _fs, req))]
    pub async fn execute_put_object(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let start_time = std::time::Instant::now();
        let mut req = req;

        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let (event_name, quota_operation, request_method_name) = Self::put_object_execution_context(&req);
        if req.extensions.get::<PostObjectRequestMarker>().is_some() && is_post_object_sse_kms_requested(&req.input, &req.headers)
        {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for POST object uploads"));
        }
        if let Some(ref storage_class) = req.input.storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        if is_put_object_extract_requested(&req.headers) {
            return Box::pin(self.execute_put_object_extract(req)).await;
        }

        let input = std::mem::take(&mut req.input);

        let PutObjectInput {
            body,
            bucket,
            cache_control,
            key,
            content_length,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            tagging,
            metadata,
            version_id,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            content_md5,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            storage_class,
            website_redirect_location,
            ..
        } = input;

        // Merge SSE-C params from headers (fallback when S3 layer does not populate input)
        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        // Merge server_side_encryption from headers (fallback when S3 layer does not populate input)
        let server_side_encryption = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);

        // Validate object key
        validate_object_key(&key, request_method_name)?;

        if let Some(size) = content_length {
            self.check_bucket_quota(&bucket, quota_operation, size as u64).await?;
        }

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let mut size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        if size == -1 {
            return Err(s3_error!(UnexpectedContent));
        }

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);

        // Detect zero-copy opportunity before encryption/compression decisions
        // Zero-copy is beneficial for large unencrypted, uncompressed objects
        let enable_zero_copy = should_use_zero_copy(size, &req.headers);

        if enable_zero_copy {
            // Record zero-copy write attempt
            counter!("rustfs.zero_copy.write.attempts.total").increment(1);
            histogram!("rustfs.zero_copy.write.size.bytes").record(size as f64);
            debug!("Zero-copy write enabled for {} byte object (bucket={}, key={})", size, bucket, key);
        }

        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let store = get_validated_store(&bucket).await?;

        // TDD: Get bucket default encryption configuration
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        debug!("TDD: bucket_sse_config={:?}", bucket_sse_config);

        // TDD: Determine effective encryption configuration (request overrides bucket default)
        let original_sse = server_side_encryption.clone();
        let mut effective_sse = server_side_encryption.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                debug!("TDD: Processing bucket SSE config: {:?}", config);
                config.rules.first().and_then(|rule| {
                    debug!("TDD: Processing SSE rule: {:?}", rule);
                    rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                        debug!("TDD: Found SSE default: {:?}", sse);
                        match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256), // fallback to AES256
                        }
                    })
                })
            })
        });
        debug!("TDD: effective_sse={:?} (original={:?})", effective_sse, original_sse);

        let mut effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });

        // Validate SSE-C headers early: reject partial/invalid combinations per S3 spec
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true, // PutObject requires all three: algorithm, key, key_md5
        )?;

        let mut metadata = metadata.unwrap_or_default();
        apply_put_request_metadata(
            &mut metadata,
            &req.headers,
            &key,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            website_redirect_location,
            tagging,
            storage_class.clone(),
        )?;

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id.clone(), &req.headers, metadata.clone())
            .await
            .map_err(ApiError::from)?;
        apply_put_request_object_lock_opts(
            &bucket,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            &mut opts,
        )
        .await?;

        let current_opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => validate_existing_object_lock_for_write(&existing_obj_info)?,
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
            }
        }

        let actual_size = size;

        let mut md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256_with_query(&req.headers, req.uri.query());

        let mut reader = if is_compressible(&req.headers, &key) && size > MIN_COMPRESSIBLE_SIZE as i64 {
            let algorithm = CompressionAlgorithm::default();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, algorithm.to_string());
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

            let mut hrd =
                HashReader::from_stream(body, size, size, md5hex.take(), sha256hex.take(), false).map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = hrd.checksum();
            insert_str(&mut opts.user_defined, SUFFIX_COMPRESSION, algorithm.to_string());
            insert_str(&mut opts.user_defined, SUFFIX_ACTUAL_SIZE, size.to_string());

            size = HashReader::SIZE_PRESERVE_LAYER;
            HashReader::from_reader(CompressReader::new(hrd, algorithm), size, actual_size, None, None, false)
                .map_err(ApiError::from)?
        } else {
            HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
        };

        if size >= 0 {
            if let Err(err) = reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = reader.checksum();
        }

        let mut helper = OperationHelper::new(&req, event_name, S3Operation::PutObject);

        // Apply encryption using unified SSE API.
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: None,
            part_key: None,
            part_nonce: None,
        };

        let encryption_material = match sse_encryption(encryption_request).await {
            Ok(material) => material,
            Err(err) => {
                let result = Err(err.into());
                let _ = helper.complete(&result);
                return result;
            }
        };

        if let Some(material) = encryption_material {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            let encrypted_reader = material.wrap_reader(reader);
            reader = HashReader::from_reader(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                .map_err(ApiError::from)?;

            let encryption_metadata = material.metadata;
            metadata.extend(encryption_metadata.clone());
            opts.user_defined.extend(encryption_metadata);
        }

        let mut reader = PutObjReader::new(reader);

        let mt2 = metadata.clone();
        opts.user_defined.extend(metadata);

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts.clone());
        let dsc = must_replicate(&bucket, &key, repoptions).await;

        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let obj_info = match store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(ApiError::from)
        {
            Ok(obj_info) => obj_info,
            Err(err) => {
                let result: S3Result<S3Response<PutObjectOutput>> = Err(err.into());
                let _ = helper.complete(&result);
                return result;
            }
        };

        maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;

        // Fast in-memory update for immediate quota consistency
        rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        let raw_version = obj_info.version_id.map(|v| v.to_string());

        helper = helper.object(obj_info.clone());
        if let Some(version_id) = &raw_version {
            helper = helper.version_id(version_id.clone());
        }

        let put_version = if BucketVersioningSys::prefix_enabled(&bucket, &key).await {
            raw_version
        } else {
            None
        };

        let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts);

        let dsc = must_replicate(&bucket, &key, repoptions).await;
        let expiration = resolve_put_object_expiration(&bucket, &obj_info).await;

        if dsc.replicate_any() {
            schedule_replication(obj_info.clone(), store, dsc, ReplicationType::Object).await;
        }

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &req.trailing_headers,
            &mut checksums,
        );

        let output = PutObjectOutput {
            e_tag,
            server_side_encryption: effective_sse,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            ssekms_key_id: effective_kms_key_id,
            expiration,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            version_id: put_version,
            ..Default::default()
        };

        // For browser-based POST uploads (multipart/form-data), response status/body handling
        // is decided by s3s PostObject serializer (success_action_status / redirect semantics).

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);

        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;

        // Record PutObject metrics via zero-copy-metrics
        {
            let duration_ms = start_time.elapsed().as_millis() as f64;
            rustfs_io_metrics::record_put_object(
                duration_ms,
                size,
                enable_zero_copy, // Track if zero-copy was enabled
            );
        }

        result
    }

    fn finalize_get_object_completion(
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &TimeoutConfig,
        total_duration: Duration,
        response_content_length: i64,
        optimal_buffer_size: usize,
    ) {
        rustfs_io_metrics::record_get_object_completion(
            total_duration.as_secs_f64(),
            response_content_length,
            optimal_buffer_size,
        );

        rustfs_io_metrics::record_get_object(total_duration.as_millis() as f64, response_content_length);

        if wrapper.is_timeout() {
            warn!(
                "GetObject request exceeded timeout: duration={:?} timeout={:?}",
                wrapper.elapsed(),
                timeout_config.get_object_timeout
            );
            rustfs_io_metrics::record_get_object_timeout(None, Some(wrapper.elapsed().as_secs_f64()));
        }

        debug!(
            "GetObject completed: size={} duration={:?} buffer={}",
            response_content_length, total_duration, optimal_buffer_size
        );
    }

    async fn finalize_get_object_response(
        helper: OperationHelper,
        bucket: &str,
        method: &hyper::Method,
        headers: &HeaderMap,
        event_info: ObjectInfo,
        version_id_for_event: String,
        output: GetObjectOutput,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let helper = helper.object(event_info).version_id(version_id_for_event);
        let response = wrap_response_with_cors(bucket, method, headers, output).await;
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_output_context(
        &self,
        req: &S3Request<GetObjectInput>,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        info: ObjectInfo,
        event_info: ObjectInfo,
        final_stream: DynReader,
        rs: Option<HTTPRangeSpec>,
        content_type: Option<ContentType>,
        last_modified: Option<Timestamp>,
        response_content_length: i64,
        content_range: Option<String>,
        server_side_encryption: Option<ServerSideEncryption>,
        sse_customer_algorithm: Option<SSECustomerAlgorithm>,
        sse_customer_key_md5: Option<SSECustomerKeyMD5>,
        ssekms_key_id: Option<SSEKMSKeyId>,
        encryption_applied: bool,
        permit_wait_duration: Duration,
        queue_utilization: f64,
        queue_status: &concurrency::IoQueueStatus,
        concurrent_requests: usize,
        part_number: Option<usize>,
        versioned: bool,
    ) -> S3Result<GetObjectOutputContext> {
        let strategy = self.finalize_get_object_strategy(
            manager,
            bucket,
            key,
            &info,
            rs.as_ref(),
            response_content_length,
            permit_wait_duration,
            queue_utilization,
            queue_status,
            concurrent_requests,
        );
        let GetObjectStrategyContext {
            io_strategy: _,
            optimal_buffer_size,
        } = strategy;

        let body = Self::build_get_object_body(
            final_stream,
            &info,
            response_content_length,
            optimal_buffer_size,
            part_number,
            rs.is_some(),
            encryption_applied,
        )
        .await?;

        let checksums = Self::build_get_object_checksums(&info, &req.headers, part_number, rs.as_ref())?;

        let output_version_id = if versioned {
            info.version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        // x-amz-restore: extract from object metadata
        let restore = info.user_defined.get(X_AMZ_RESTORE.as_str()).and_then(|v| {
            let rs = parse_restore_obj_status(v).ok()?;
            Some(rs.to_string2())
        });

        // x-amz-expiration: predict from lifecycle configuration
        let expiration = resolve_put_object_expiration(bucket, &info).await;
        let storage_class = response_storage_class(&info, &info.user_defined);

        let output = GetObjectOutput {
            body,
            content_length: Some(response_content_length),
            last_modified,
            content_type,
            content_encoding: info.content_encoding.clone(),
            accept_ranges: Some("bytes".to_string()),
            content_range,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata: filter_object_metadata(&info.user_defined),
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            checksum_type: checksums.checksum_type,
            version_id: output_version_id,
            restore,
            expiration,
            storage_class,
            ..Default::default()
        };

        Ok(GetObjectOutputContext {
            output,
            event_info,
            response_content_length,
            optimal_buffer_size,
        })
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let request_id = req
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| crate::storage::request_context::RequestContext::fallback().request_id);
        let bootstrap = Self::init_get_object_bootstrap(&req.input.bucket, &req.input.key, &request_id)?;
        let timeout_config = bootstrap.timeout_config;
        let wrapper = bootstrap.wrapper;
        let request_start = bootstrap.request_start;
        let concurrent_requests = bootstrap.concurrent_requests;
        let mut request_guard = bootstrap.request_guard;

        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        // mc get 3

        let request_context = Self::prepare_get_object_request_context(&req).await?;
        let GetObjectRequestContext {
            bucket,
            key,
            version_id_for_event,
            part_number,
            rs,
            opts,
        } = request_context;

        let manager = get_concurrency_manager();

        let prepared_read = Self::prepare_get_object_read_execution(
            &req,
            manager,
            &wrapper,
            &timeout_config,
            &bucket,
            &key,
            rs,
            &opts,
            part_number,
        )
        .await?;
        let GetObjectPreparedRead { io_planning, read_setup } = prepared_read;
        let permit_wait_duration = io_planning.permit_wait_duration;
        let queue_status = io_planning.queue_status;
        let queue_utilization = io_planning.queue_utilization;

        let GetObjectReadSetup {
            info,
            event_info,
            final_stream,
            rs,
            content_type,
            last_modified,
            response_content_length,
            content_range,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
        } = read_setup;

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
        let output_context = self
            .build_get_object_output_context(
                &req,
                manager,
                &bucket,
                &key,
                info,
                event_info,
                final_stream,
                rs,
                content_type,
                last_modified,
                response_content_length,
                content_range,
                server_side_encryption,
                sse_customer_algorithm,
                sse_customer_key_md5,
                ssekms_key_id,
                encryption_applied,
                permit_wait_duration,
                queue_utilization,
                &queue_status,
                concurrent_requests,
                part_number,
                versioned,
            )
            .await?;
        let GetObjectOutputContext {
            output,
            event_info,
            response_content_length,
            optimal_buffer_size,
        } = output_context;

        let total_duration = request_start.elapsed();
        Self::finalize_get_object_completion(
            &wrapper,
            &timeout_config,
            total_duration,
            response_content_length,
            optimal_buffer_size,
        );

        let result = Self::finalize_get_object_response(
            helper,
            &bucket,
            &req.method,
            &req.headers,
            event_info,
            version_id_for_event,
            output,
        )
        .await;
        if result.is_ok() {
            request_guard.finish_ok();
        } else {
            request_guard.finish_err();
        }
        result
    }

    pub async fn execute_get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedAttributes, S3Operation::GetObjectAttributes).suppress_event();
        let GetObjectAttributesInput {
            bucket,
            key,
            max_parts,
            object_attributes,
            part_number_marker,
            version_id,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let info = match store.get_object_info(&bucket, &key, &opts).await {
            Ok(info) => info,
            Err(err) => {
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!(
                                    "Failed to probe children for object attributes (bucket: {}, key: {}): {}",
                                    bucket, key, e
                                );
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                return Err(ApiError::from(err).into());
            }
        };

        if info.delete_marker {
            if opts.version_id.is_none() {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
        }

        validate_ssec_for_read(&info.user_defined, sse_customer_key.as_ref(), sse_customer_key_md5.as_ref())?;

        let metadata_map = info.user_defined.clone();
        let storage_class = info
            .storage_class
            .clone()
            .or_else(|| metadata_map.get(AMZ_STORAGE_CLASS).cloned())
            .filter(|s| !s.is_empty())
            .map(StorageClass::from);

        debug!(
            "GetObjectAttributes raw object_attributes={:?}",
            object_attributes.iter().map(|value| value.as_str()).collect::<Vec<_>>()
        );

        let requested = |name: &'static str| -> bool { object_attributes_requested(&object_attributes, name) };

        let e_tag = if requested(ObjectAttributes::ETAG) {
            info.etag.as_ref().map(|etag| to_s3s_etag(etag))
        } else {
            None
        };

        let object_size = if requested(ObjectAttributes::OBJECT_SIZE) {
            Some(info.get_actual_size().map_err(ApiError::from)?)
        } else {
            None
        };

        let checksum = if requested(ObjectAttributes::CHECKSUM) {
            let (checksums, _is_multipart) = info.decrypt_checksums(0, &req.headers).map_err(ApiError::from)?;
            let mut checksum_crc32 = None;
            let mut checksum_crc32c = None;
            let mut checksum_sha1 = None;
            let mut checksum_sha256 = None;
            let mut checksum_crc64nvme = None;
            let mut checksum_type = None;

            for (k, v) in checksums {
                if k == AMZ_CHECKSUM_TYPE {
                    checksum_type = Some(ChecksumType::from(v));
                    continue;
                }
                match rustfs_rio::ChecksumType::from_string(k.as_str()) {
                    rustfs_rio::ChecksumType::CRC32 => checksum_crc32 = Some(v),
                    rustfs_rio::ChecksumType::CRC32C => checksum_crc32c = Some(v),
                    rustfs_rio::ChecksumType::SHA1 => checksum_sha1 = Some(v),
                    rustfs_rio::ChecksumType::SHA256 => checksum_sha256 = Some(v),
                    rustfs_rio::ChecksumType::CRC64_NVME => checksum_crc64nvme = Some(v),
                    _ => (),
                }
            }

            Some(Checksum {
                checksum_crc32,
                checksum_crc32c,
                checksum_sha1,
                checksum_sha256,
                checksum_crc64nvme,
                checksum_type,
            })
        } else {
            None
        };

        let object_parts = if requested(ObjectAttributes::OBJECT_PARTS) && info.is_multipart() {
            let params = parse_list_parts_params(part_number_marker, max_parts)?;
            let mut parts = Vec::new();
            let mut marker = params.part_number_marker;
            let max_parts = params.max_parts;
            let mut start_at = 0usize;

            if let Some(marker_value) = marker {
                if let Some(index) = info.parts.iter().position(|part| part.number == marker_value) {
                    start_at = index + 1;
                } else {
                    marker = None;
                }
            }

            let max_parts: i32 = max_parts.try_into().map_err(|_| {
                S3Error::with_message(S3ErrorCode::InvalidArgument, "max-parts value is out of range".to_string())
            })?;
            let end = (start_at + params.max_parts).min(info.parts.len());
            let is_truncated = end < info.parts.len();

            for part in &info.parts[start_at..end] {
                let (checksums, _is_multipart) = info.decrypt_checksums(part.number, &req.headers).map_err(ApiError::from)?;
                let mut checksum_crc32 = None;
                let mut checksum_crc32c = None;
                let mut checksum_sha1 = None;
                let mut checksum_sha256 = None;
                let mut checksum_crc64nvme = None;

                for (k, v) in checksums {
                    match rustfs_rio::ChecksumType::from_string(k.as_str()) {
                        rustfs_rio::ChecksumType::CRC32 => checksum_crc32 = Some(v),
                        rustfs_rio::ChecksumType::CRC32C => checksum_crc32c = Some(v),
                        rustfs_rio::ChecksumType::SHA1 => checksum_sha1 = Some(v),
                        rustfs_rio::ChecksumType::SHA256 => checksum_sha256 = Some(v),
                        rustfs_rio::ChecksumType::CRC64_NVME => checksum_crc64nvme = Some(v),
                        _ => (),
                    }
                }

                let part_size = if part.actual_size > 0 {
                    part.actual_size
                } else {
                    part.size.try_into().map_err(|_| {
                        S3Error::with_message(S3ErrorCode::InvalidArgument, "Part size value is out of range".to_string())
                    })?
                };

                parts.push(ObjectPart {
                    checksum_crc32,
                    checksum_crc32c,
                    checksum_sha1,
                    checksum_sha256,
                    checksum_crc64nvme,
                    part_number: i32::try_from(part.number).ok(),
                    size: Some(part_size),
                });
            }

            let part_number_marker = marker.and_then(|v| i32::try_from(v).ok());
            let next_part_number_marker = parts.last().and_then(|part| part.part_number);

            Some(GetObjectAttributesParts {
                is_truncated: Some(is_truncated),
                max_parts: Some(max_parts),
                next_part_number_marker,
                part_number_marker,
                parts: Some(parts),
                total_parts_count: Some(i32::try_from(info.parts.len()).map_err(|_| {
                    S3Error::with_message(S3ErrorCode::InvalidArgument, "Part count is out of range".to_string())
                })?),
            })
        } else {
            None
        };

        let version_id = if BucketVersioningSys::prefix_enabled(&bucket, &key).await {
            info.version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        let output = GetObjectAttributesOutput {
            checksum,
            delete_marker: if info.delete_marker { Some(true) } else { None },
            e_tag,
            last_modified: info.mod_time.map(Timestamp::from),
            object_parts,
            object_size,
            storage_class,
            version_id: version_id.clone(),
            ..Default::default()
        };

        helper = helper.object(info).version_id(version_id.unwrap_or_default());

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedCopy, S3Operation::CopyObject);
        let CopyObjectInput {
            copy_source,
            bucket,
            key,
            version_id: dest_version_id,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            metadata_directive,
            metadata,
            copy_source_if_match,
            copy_source_if_none_match,
            content_type,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            ..
        } = req.input.clone();
        let (src_bucket, src_key, version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Validate both source and destination keys
        validate_object_key(&src_key, "COPY (source)")?;
        validate_object_key(&key, "COPY (dest)")?;

        // AWS S3 allows self-copy when metadata directive is REPLACE (used to update metadata in-place).
        // Reject only when the directive is not REPLACE.
        if metadata_directive.as_ref().map(|d| d.as_str()) != Some(MetadataDirective::REPLACE)
            && src_bucket == bucket
            && src_key == key
        {
            error!("Rejected self-copy operation: bucket={}, key={}", bucket, key);
            return Err(s3_error!(
                InvalidRequest,
                "Cannot copy an object to itself. Source and destination must be different."
            ));
        }

        // warn!("copy_object {}/{}, to {}/{}", &src_bucket, &src_key, &bucket, &key);

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;

        src_opts.version_id = version_id.clone();

        let mut src_get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let dst_opts = copy_dst_opts(&bucket, &key, version_id, &req.headers, HashMap::new())
            .await
            .map_err(ApiError::from)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);

        if cp_src_dst_same {
            src_get_opts.no_lock = true;
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let current_opts: ObjectOptions = get_opts(&bucket, &key, dest_version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => validate_existing_object_lock_for_write(&existing_obj_info)?,
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
            }
        }

        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let mut effective_sse = requested_sse.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| match sse.sse_algorithm.as_str() {
                            "AES256" => Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                            "aws:kms" => Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)),
                            _ => None,
                        })
                })
            })
        });
        let mut effective_kms_key_id = requested_kms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });

        let h = HeaderMap::new();

        let gr = store
            .get_object_reader(&src_bucket, &src_key, None, h, &src_get_opts)
            .await
            .map_err(ApiError::from)?;

        let mut src_info = gr.object_info.clone();

        // Validate copy source conditions
        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    // Weak ETag or Any (*) in If-Match should fail per RFC 9110
                    return Err(s3_error!(PreconditionFailed));
                }
            } else {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        if let Some(if_none_match) = copy_source_if_none_match
            && let Some(ref etag) = src_info.etag
            && let Some(strong_etag) = if_none_match.into_etag()
            && ETag::Strong(etag.clone()) == strong_etag
        {
            return Err(s3_error!(PreconditionFailed));
        }

        if cp_src_dst_same {
            src_info.metadata_only = true;
        }

        let decryption_request = DecryptionRequest {
            bucket: &src_bucket,
            key: &src_key,
            metadata: &src_info.user_defined,
            sse_customer_key: copy_source_sse_customer_key.as_ref(),
            sse_customer_key_md5: copy_source_sse_customer_key_md5.as_ref(),
            part_number: None,
            parts: &src_info.parts,
            etag: src_info.etag.as_deref(),
        };

        let decryption_material = sse_decryption(decryption_request).await?;

        if let Some(material) = decryption_material.as_ref()
            && let Some(original) = material.original_size
        {
            src_info.actual_size = original;
        }

        strip_managed_encryption_metadata(&mut src_info.user_defined);

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let mut length = actual_size;

        let mut compress_metadata = HashMap::new();

        let should_compress = is_compressible(&req.headers, &key) && actual_size > MIN_COMPRESSIBLE_SIZE as i64;

        if should_compress {
            insert_str(&mut compress_metadata, SUFFIX_COMPRESSION, CompressionAlgorithm::default().to_string());
            insert_str(&mut compress_metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
        } else {
            remove_str(&mut src_info.user_defined, SUFFIX_COMPRESSION);
            remove_str(&mut src_info.user_defined, SUFFIX_ACTUAL_SIZE);
            remove_str(&mut src_info.user_defined, SUFFIX_COMPRESSION_SIZE);
        }

        // Handle MetadataDirective REPLACE: replace user metadata while preserving system metadata.
        // System metadata (compression, encryption) is added after this block to ensure
        // it's not cleared by the REPLACE operation.
        if metadata_directive.as_ref().map(|d| d.as_str()) == Some(MetadataDirective::REPLACE) {
            src_info.user_defined.clear();
            if let Some(metadata) = metadata {
                src_info.user_defined.extend(metadata);
            }
            if let Some(ct) = content_type {
                src_info.content_type = Some(ct.clone());
                src_info.user_defined.insert("content-type".to_string(), ct);
            }
        }

        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
            &bucket,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )
        .await?
        {
            src_info.user_defined.extend(object_lock_metadata);
        }

        let mut reader = match decryption_material {
            Some(material) => {
                if material.is_multipart {
                    let (decrypted_stream, plaintext_size) =
                        material.wrap_reader(gr.stream, length).await.map_err(ApiError::from)?;
                    length = plaintext_size;

                    if should_compress {
                        let hrd = HashReader::from_reader(decrypted_stream, length, actual_size, None, None, false)
                            .map_err(ApiError::from)?;
                        length = HashReader::SIZE_PRESERVE_LAYER;
                        HashReader::from_reader(
                            CompressReader::new(hrd, CompressionAlgorithm::default()),
                            length,
                            actual_size,
                            None,
                            None,
                            false,
                        )
                        .map_err(ApiError::from)?
                    } else {
                        HashReader::from_reader(decrypted_stream, length, actual_size, None, None, false)
                            .map_err(ApiError::from)?
                    }
                } else if should_compress {
                    let hrd =
                        HashReader::from_stream(material.wrap_single_reader(gr.stream), length, actual_size, None, None, false)
                            .map_err(ApiError::from)?;
                    length = HashReader::SIZE_PRESERVE_LAYER;
                    HashReader::from_reader(
                        CompressReader::new(hrd, CompressionAlgorithm::default()),
                        length,
                        actual_size,
                        None,
                        None,
                        false,
                    )
                    .map_err(ApiError::from)?
                } else {
                    HashReader::from_stream(material.wrap_single_reader(gr.stream), length, actual_size, None, None, false)
                        .map_err(ApiError::from)?
                }
            }
            None => {
                if should_compress {
                    let hrd =
                        HashReader::from_stream(gr.stream, length, actual_size, None, None, false).map_err(ApiError::from)?;
                    length = HashReader::SIZE_PRESERVE_LAYER;
                    HashReader::from_reader(
                        CompressReader::new(hrd, CompressionAlgorithm::default()),
                        length,
                        actual_size,
                        None,
                        None,
                        false,
                    )
                    .map_err(ApiError::from)?
                } else {
                    HashReader::from_stream(gr.stream, length, actual_size, None, None, false).map_err(ApiError::from)?
                }
            }
        };

        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: None,
            part_key: None,
            part_nonce: None,
        };

        if let Some(material) = sse_encryption(encryption_request).await? {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            let encrypted_reader = material.wrap_reader(reader);
            reader = HashReader::from_reader(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                .map_err(ApiError::from)?;

            src_info.user_defined.extend(material.metadata);
        }

        src_info.put_object_reader = Some(PutObjReader::new(reader));

        // check quota

        for (k, v) in compress_metadata {
            src_info.user_defined.insert(k, v);
        }

        self.check_bucket_quota(&bucket, QuotaOperation::CopyObject, src_info.size as u64)
            .await?;
        let has_bucket_metadata = self.bucket_metadata_sys().is_some();

        let oi = store
            .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        maybe_enqueue_transition_immediate(&oi, LcEventSrc::S3CopyObject).await;

        // Update quota tracking after successful copy
        if has_bucket_metadata {
            rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, oi.size as u64).await;
        }

        let raw_dest_version = oi.version_id.map(|v| v.to_string());
        let dest_version = if BucketVersioningSys::prefix_enabled(&bucket, &key).await {
            raw_dest_version
        } else {
            None
        };

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: oi.mod_time.map(Timestamp::from),
            ..Default::default()
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            server_side_encryption: effective_sse,
            ssekms_key_id: effective_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            version_id: dest_version,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_objects(
        &self,
        mut req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObjects).suppress_event();
        let (bucket, delete) = {
            let bucket = req.input.bucket.clone();
            let delete = req.input.delete.clone();
            (bucket, delete)
        };

        if delete.objects.is_empty() || delete.objects.len() > 1000 {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "No objects to delete or too many objects to delete".to_string(),
            ));
        }

        let replicate_deletes = has_replication_rules(
            &bucket,
            &delete
                .objects
                .iter()
                .map(|v| ObjectToDelete {
                    object_name: v.key.clone(),
                    ..Default::default()
                })
                .collect::<Vec<ObjectToDelete>>(),
        )
        .await;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_cfg = BucketVersioningSys::get(&bucket).await.unwrap_or_default();
        let bypass_governance = has_bypass_governance_header(&req.headers);

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<rustfs_ecstore::store_api::DeletedObject>,
            error: Option<Error>,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        let mut object_to_delete = Vec::new();
        let mut object_to_delete_idx = Vec::new();
        let mut object_sizes = Vec::new();
        let mut existing_object_infos = Vec::new();
        for (idx, obj_id) in delete.objects.iter().enumerate() {
            let raw_version_id = obj_id.version_id.clone();
            let (version_id, version_uuid) = match normalize_delete_objects_version_id(raw_version_id.clone()) {
                Ok(parsed) => parsed,
                Err(err) => {
                    delete_results[idx].error = Some(Error {
                        code: Some("NoSuchVersion".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some(err),
                        version_id: raw_version_id,
                    });
                    continue;
                }
            };

            {
                let req_info = req_info_mut(&mut req)?;
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(obj_id.key.clone());
                req_info.version_id = version_id.clone();
            }

            let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::DeleteObjectAction)).await;
            if let Err(e) = auth_res {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(e.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            let mut object = ObjectToDelete {
                object_name: obj_id.key.clone(),
                version_id: version_uuid,
                ..Default::default()
            };

            let metadata = extract_metadata(&req.headers);
            let opts: ObjectOptions = del_opts(
                &bucket,
                &object.object_name,
                object.version_id.map(|f| f.to_string()),
                &req.headers,
                metadata,
            )
            .await
            .map_err(ApiError::from)?;

            let (goi, gerr) = match store.get_object_info(&bucket, &object.object_name, &opts).await {
                Ok(res) => (res, None),
                Err(e) => (ObjectInfo::default(), Some(e.to_string())),
            };

            if gerr.is_none()
                && !delete_creates_delete_marker(&opts)
                && let Some(block_reason) = check_object_lock_for_deletion(&bucket, &goi, bypass_governance).await
            {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(block_reason.error_message()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            object_sizes.push(goi.size);

            if is_dir_object(&object.object_name) && object.version_id.is_none() {
                object.version_id = Some(Uuid::nil());
            }

            if replicate_deletes {
                let dsc = check_replicate_delete(
                    &bucket,
                    &ObjectToDelete {
                        object_name: object.object_name.clone(),
                        version_id: object.version_id,
                        ..Default::default()
                    },
                    &goi,
                    &opts,
                    gerr.clone(),
                )
                .await;
                if dsc.replicate_any() {
                    if object.version_id.is_some() {
                        object.version_purge_status = Some(VersionPurgeStatusType::Pending);
                        object.version_purge_statuses = dsc.pending_status();
                    } else {
                        object.delete_marker_replication_status = dsc.pending_status();
                    }
                    object.replicate_decision_str = Some(dsc.to_string());
                }
            }

            object_to_delete_idx.push(idx);
            object_to_delete.push(object);
            existing_object_infos.push(gerr.is_none().then_some(goi));
        }

        let (mut dobjs, errs) = store
            .delete_objects(
                &bucket,
                object_to_delete.clone(),
                ObjectOptions {
                    version_suspended: version_cfg.suspended(),
                    ..Default::default()
                },
            )
            .await;

        let _manager = get_concurrency_manager();
        let _bucket_clone = bucket.clone();
        let _deleted_objects = dobjs.clone();
        if is_all_buckets_not_found(
            &errs
                .iter()
                .map(|v| v.as_ref().map(|v| v.clone().into()))
                .collect::<Vec<Option<DiskError>>>() as &[Option<DiskError>],
        ) {
            let result = Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
            let _ = helper.complete(&result);
            return result;
        }

        for (i, err) in errs.iter().enumerate() {
            let didx = object_to_delete_idx[i];

            if err.is_none()
                || err
                    .clone()
                    .is_some_and(|v| is_err_object_not_found(&v) || is_err_version_not_found(&v))
            {
                if replicate_deletes {
                    dobjs[i].replication_state = Some(object_to_delete[i].replication_state());
                }
                delete_results[didx].delete_object = Some(dobjs[i].clone());
                enqueue_transitioned_delete_cleanup(
                    &bucket,
                    &object_to_delete[i].object_name,
                    &ObjectOptions {
                        version_id: object_to_delete[i].version_id.map(|v| v.to_string()),
                        versioned: version_cfg.prefix_enabled(object_to_delete[i].object_name.as_str()),
                        version_suspended: version_cfg.suspended(),
                        ..Default::default()
                    },
                    existing_object_infos[i].as_ref(),
                )
                .await;
                let size = object_sizes[i];
                if size > 0 {
                    rustfs_ecstore::data_usage::decrement_bucket_usage_memory(&bucket, size as u64).await;
                }
                continue;
            }

            if let Some(err) = err.clone() {
                delete_results[didx].error = Some(Error {
                    code: Some(err.to_string()),
                    key: Some(object_to_delete[i].object_name.clone()),
                    message: Some(err.to_string()),
                    version_id: object_to_delete[i].version_id.map(|v| v.to_string()),
                });
            }
        }

        let deleted = delete_results
            .iter()
            .filter_map(|v| v.delete_object.clone())
            .map(|v| DeletedObject {
                delete_marker: { if v.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: v.delete_marker_version_id.map(|v| v.to_string()),
                key: Some(v.object_name.clone()),
                version_id: if is_dir_object(v.object_name.as_str()) && v.version_id == Some(Uuid::nil()) {
                    None
                } else {
                    v.version_id.map(|v| v.to_string())
                },
            })
            .collect();

        let errors = delete_results.iter().filter_map(|v| v.error.clone()).collect::<Vec<Error>>();
        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        };

        for dobjs in &delete_results {
            if let Some(dobj) = &dobjs.delete_object
                && replicate_deletes
                && (dobj.delete_marker_replication_status() == ReplicationStatusType::Pending
                    || dobj.version_purge_status() == VersionPurgeStatusType::Pending)
            {
                let mut dobj = dobj.clone();
                if is_dir_object(dobj.object_name.as_str()) && dobj.version_id.is_none() {
                    dobj.version_id = Some(Uuid::nil());
                }

                let deleted_object = DeletedObjectReplicationInfo {
                    delete_object: dobj,
                    bucket: bucket.clone(),
                    event_type: REPLICATE_INCOMING_DELETE.to_string(),
                    ..Default::default()
                };
                schedule_replication_delete(deleted_object).await;
            }
        }

        let req_headers = req.headers.clone();
        let notify = self
            .context
            .as_ref()
            .map(|context| context.notify())
            .unwrap_or_else(default_notify_interface);
        let request_context = req
            .extensions
            .get::<crate::storage::request_context::RequestContext>()
            .cloned();
        spawn_background_with_context(request_context, async move {
            for res in delete_results {
                if let Some(dobj) = res.delete_object {
                    let event_name = if dobj.delete_marker {
                        EventName::ObjectRemovedDeleteMarkerCreated
                    } else {
                        EventName::ObjectRemovedDelete
                    };
                    let event_args = EventArgsBuilder::new(
                        event_name,
                        bucket.clone(),
                        ObjectInfo {
                            name: dobj.object_name.clone(),
                            bucket: bucket.clone(),
                            ..Default::default()
                        },
                    )
                    .version_id(dobj.version_id.map(|v| v.to_string()).unwrap_or_default())
                    .req_params(extract_params_header(&req_headers))
                    .resp_elements(extract_resp_elements(&S3Response::new(DeleteObjectsOutput::default())))
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notify.notify(event_args).await;
                }
            }
        });

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_object(&self, mut req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObject);
        let DeleteObjectInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "DELETE")?;

        let replica = req
            .headers
            .get(AMZ_BUCKET_REPLICATION_STATUS)
            .map(|v| v.to_str().unwrap_or_default() == ReplicationStatusType::Replica.as_str())
            .unwrap_or_default();

        if replica {
            authorize_request(&mut req, Action::S3Action(S3Action::ReplicateDeleteAction)).await?;
        }

        let metadata = extract_metadata(&req.headers);
        // Clone version_id before it's moved
        let version_id_clone = version_id.clone();

        let mut opts: ObjectOptions = del_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;
        let force_delete = opts.delete_prefix;

        let lock_cfg = BucketObjectLockSys::get(&bucket).await;
        if lock_cfg.is_some() && opts.delete_prefix {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("force-delete is forbidden on Object Locking enabled buckets".into()),
                "force-delete is forbidden on Object Locking enabled buckets",
            ));
        }

        // let mut vid = opts.version_id.clone();

        if replica {
            opts.set_replica_status(ReplicationStatusType::Replica);

            // if opts.version_purge_status().is_empty() {
            //     vid = None;
            // }
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let replicate_force_delete = force_delete
            && !replica
            && has_replication_rules(
                &bucket,
                &[ObjectToDelete {
                    object_name: key.clone(),
                    ..Default::default()
                }],
            )
            .await;

        // Check Object Lock retention before deletion
        // TODO: Future optimization (separate PR) - If performance becomes critical under high delete load:
        // 1. Add a lightweight get_object_lock_info() that only fetches retention metadata
        // 2. Or use combined get-and-delete in storage layer with retention check callback
        let get_opts: ObjectOptions = get_opts(&bucket, &key, version_id_clone, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let existing_object_info = match store.get_object_info(&bucket, &key, &get_opts).await {
            Ok(obj_info) => {
                // Check for bypass governance retention header (permission already verified in access.rs)
                let bypass_governance = has_bypass_governance_header(&req.headers);

                if !delete_creates_delete_marker(&opts)
                    && let Some(block_reason) = check_object_lock_for_deletion(&bucket, &obj_info, bypass_governance).await
                {
                    return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
                }
                Some(obj_info)
            }
            Err(err) => {
                // If object not found, allow deletion to proceed (will return 204 No Content)
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };

        let obj_info = {
            match store.delete_object(&bucket, &key, opts.clone()).await {
                Ok(obj) => obj,
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
                    }

                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        // TODO: send event

                        return Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
                    }

                    return Err(ApiError::from(err).into());
                }
            }
        };

        enqueue_transitioned_delete_cleanup(&bucket, &key, &opts, existing_object_info.as_ref()).await;

        // Fast in-memory update for immediate quota consistency
        rustfs_ecstore::data_usage::decrement_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        if obj_info.name.is_empty() {
            if replicate_force_delete {
                schedule_replication_delete(DeletedObjectReplicationInfo {
                    delete_object: rustfs_ecstore::store_api::DeletedObject {
                        object_name: key.clone(),
                        force_delete: true,
                        ..Default::default()
                    },
                    bucket: bucket.clone(),
                    event_type: REPLICATE_INCOMING_DELETE.to_string(),
                    ..Default::default()
                })
                .await;
            }
            // Prefix/force-delete returns empty ObjectInfo; still emit bucket notification so webhooks match S3 DELETE.
            helper = helper
                .event_name(EventName::ObjectRemovedDelete)
                .object(ObjectInfo {
                    name: key.clone(),
                    bucket: bucket.clone(),
                    ..Default::default()
                })
                .version_id(String::new());
            let result = Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
            // Match non-empty delete path: capacity manager write-op telemetry.
            let manager = get_capacity_manager();
            manager.record_write_operation().await;
            let _ = helper.complete(&result);
            return result;
        }

        let deleted_replication_info = existing_object_info
            .as_ref()
            .filter(|_| should_use_existing_delete_replication_info(&opts));
        let deleted_object_source = deleted_replication_info.unwrap_or(&obj_info);
        let replication_state_source =
            delete_replication_state_source(&opts, existing_object_info.as_ref(), deleted_object_source);
        let deleted_delete_marker_version = deleted_replication_info.is_some_and(|info| info.delete_marker);

        let delete_replication_version_id = delete_replication_version_id(deleted_object_source, deleted_delete_marker_version);
        let schedule_delete_replication = if opts.replication_request && replica {
            should_schedule_replica_delete_replication(&bucket, replication_state_source, delete_replication_version_id).await
        } else {
            should_schedule_delete_replication(&opts, deleted_object_source, deleted_delete_marker_version)
        };

        if schedule_delete_replication {
            let mut deleted_object = DeletedObjectReplicationInfo {
                delete_object: rustfs_ecstore::store_api::DeletedObject {
                    delete_marker: deleted_object_source.delete_marker && !deleted_delete_marker_version,
                    delete_marker_version_id: if deleted_object_source.delete_marker {
                        deleted_object_source.version_id
                    } else {
                        None
                    },
                    object_name: key.clone(),
                    version_id: if deleted_object_source.delete_marker {
                        None
                    } else {
                        deleted_object_source.version_id
                    },
                    delete_marker_mtime: deleted_object_source.mod_time,
                    replication_state: Some(replication_state_source.replication_state()),
                    ..Default::default()
                },
                bucket: bucket.clone(),
                event_type: REPLICATE_INCOMING_DELETE.to_string(),
                ..Default::default()
            };
            enrich_delete_replication_state_if_needed(&bucket, &mut deleted_object.delete_object, replication_state_source).await;
            schedule_replication_delete(deleted_object).await;
        }

        let delete_marker = obj_info.delete_marker;
        let version_id = obj_info.version_id;

        let output = DeleteObjectOutput {
            delete_marker: Some(delete_marker),
            version_id: version_id.map(|v| v.to_string()),
            ..Default::default()
        };

        let event_name = if delete_marker {
            EventName::ObjectRemovedDeleteMarkerCreated
        } else {
            EventName::ObjectRemovedDelete
        };

        helper = helper.event_name(event_name);
        helper = helper
            .object(obj_info)
            .version_id(version_id.map(|v| v.to_string()).unwrap_or_default());

        let result = Ok(S3Response::new(output));
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedHead, S3Operation::HeadObject).suppress_event();
        // mc get 2
        let HeadObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            if_none_match,
            if_match,
            if_modified_since,
            if_unmodified_since,
            ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "HEAD")?;
        // Parse part number from Option<i32> to Option<usize> with validation
        let part_number: Option<usize> = parse_part_number_i32_to_usize(part_number, "HEAD")?;

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        // Modification Points: Explicitly handles get_object_info errors, distinguishing between object absence and other errors
        let info = match store.get_object_info(&bucket, &key, &opts).await {
            Ok(info) => info,
            Err(err) => {
                // If the error indicates the object or its version was not found, return 404 (NoSuchKey)
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!("Failed to probe children for prefix (bucket: {}, key: {}): {}", bucket, key, e);
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                // Other errors, such as insufficient permissions, still return the original error
                return Err(ApiError::from(err).into());
            }
        };
        if info.delete_marker {
            if opts.version_id.is_none() {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
        }
        if let Some(match_etag) = if_none_match
            && let Some(strong_etag) = match_etag.into_etag()
            && info
                .etag
                .as_ref()
                .is_some_and(|etag| ETag::Strong(etag.clone()) == strong_etag)
        {
            return Err(S3Error::new(S3ErrorCode::NotModified));
        }
        if let Some(modified_since) = if_modified_since {
            // obj_time < givenTime + 1s
            if info.mod_time.is_some_and(|mod_time| {
                let give_time: OffsetDateTime = modified_since.into();
                mod_time < give_time.add(time::Duration::seconds(1))
            }) {
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
        }
        if let Some(match_etag) = if_match {
            if let Some(strong_etag) = match_etag.into_etag()
                && info
                    .etag
                    .as_ref()
                    .is_some_and(|etag| ETag::Strong(etag.clone()) != strong_etag)
            {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        } else if let Some(unmodified_since) = if_unmodified_since
            && info.mod_time.is_some_and(|mod_time| {
                let give_time: OffsetDateTime = unmodified_since.into();
                mod_time > give_time.add(time::Duration::seconds(1))
            })
        {
            return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
        }
        validate_sse_headers_for_read(&info.user_defined, &req.headers)?;

        // Validate SSE-C: if the object was encrypted with a customer-provided key,
        // the caller must supply the matching key even for HEAD requests (per S3 spec).
        validate_ssec_for_read(
            &info.user_defined,
            req.input.sse_customer_key.as_ref(),
            req.input.sse_customer_key_md5.as_ref(),
        )?;

        // Compute x-amz-expiration header from lifecycle prediction (before info is partially moved)
        let expiration_header = resolve_put_object_expiration(&bucket, &info).await;
        let event_info = info.clone();
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("parse content-type err {} {:?}", &content_type, err);
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);
        let read_plan = rustfs_ecstore::store_api::ObjectReadPlan::build(&info, rs.clone(), &opts).map_err(|e| {
            error!("build head read plan error: {}", e);
            ApiError::from(e)
        })?;
        let rs = read_plan.plaintext_range.as_ref().map(|range| HTTPRangeSpec {
            is_suffix_length: false,
            start: range.start as i64,
            end: range.start as i64 + range.length - 1,
        });
        let content_length = read_plan.response_length;

        let metadata_map = info.user_defined.clone();
        let server_side_encryption = metadata_map
            .get("x-amz-server-side-encryption")
            .map(|v| ServerSideEncryption::from(v.clone()));
        let sse_customer_algorithm = metadata_map
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| SSECustomerAlgorithm::from(v.clone()));
        let sse_customer_key_md5 = metadata_map.get("x-amz-server-side-encryption-customer-key-md5").cloned();
        let sse_kms_key_id = metadata_map.get("x-amz-server-side-encryption-aws-kms-key-id").cloned();
        let storage_class = response_storage_class(&info, &metadata_map);
        let mut checksum_crc32 = None;
        let mut checksum_crc32c = None;
        let mut checksum_sha1 = None;
        let mut checksum_sha256 = None;
        let mut checksum_crc64nvme = None;
        let mut checksum_type = None;

        // checksum
        if let Some(checksum_mode) = req.headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (checksums, _is_multipart) = info
                .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                .map_err(ApiError::from)?;

            for (key, checksum) in checksums {
                if key == AMZ_CHECKSUM_TYPE {
                    checksum_type = Some(ChecksumType::from(checksum));
                    continue;
                }

                match rustfs_rio::ChecksumType::from_string(key.as_str()) {
                    rustfs_rio::ChecksumType::CRC32 => checksum_crc32 = Some(checksum),
                    rustfs_rio::ChecksumType::CRC32C => checksum_crc32c = Some(checksum),
                    rustfs_rio::ChecksumType::SHA1 => checksum_sha1 = Some(checksum),
                    rustfs_rio::ChecksumType::SHA256 => checksum_sha256 = Some(checksum),
                    rustfs_rio::ChecksumType::CRC64_NVME => checksum_crc64nvme = Some(checksum),
                    _ => (),
                }
            }
        }
        // Extract standard HTTP headers from user_defined metadata
        // Note: These headers are stored with lowercase keys by extract_metadata_from_mime
        let cache_control = metadata_map.get("cache-control").cloned();
        let content_disposition = metadata_map.get("content-disposition").cloned();
        let content_language = metadata_map.get("content-language").cloned();
        let website_redirect_location = metadata_map.get(AMZ_WEBSITE_REDIRECT_LOCATION).cloned();
        let expires = info.expires.map(Timestamp::from);

        // Calculate tag count from user_tags already in ObjectInfo
        // This avoids an additional API call since user_tags is already populated by get_object_info
        let tag_count = if !info.user_tags.is_empty() {
            let tag_set = decode_tags(&info.user_tags);
            tag_set.len()
        } else {
            0
        };
        let output = HeadObjectOutput {
            content_length: Some(content_length),
            content_type,
            content_encoding: info.content_encoding.clone(),
            cache_control,
            content_disposition,
            content_language,
            website_redirect_location,
            expires,
            last_modified,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata: filter_object_metadata(&metadata_map),
            version_id: info.version_id.map(|v| v.to_string()),
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: sse_kms_key_id,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            storage_class,
            // x-amz-restore from object metadata
            restore: metadata_map.get(X_AMZ_RESTORE.as_str()).and_then(|v| {
                let rs = parse_restore_obj_status(v).ok()?;
                Some(rs.to_string2())
            }),
            // x-amz-expiration from lifecycle prediction
            expiration: expiration_header,
            // metadata: object_metadata,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(event_info).version_id(version_id);

        // NOTE ON CORS:
        // Bucket-level CORS headers are intentionally applied only for object retrieval
        // operations (GET/HEAD) via `wrap_response_with_cors`. Other S3 operations that
        // interact with objects (PUT/POST/DELETE/LIST, etc.) rely on the system-level
        // CORS layer instead. In case both are applicable, this bucket-level CORS logic
        // takes precedence for these read operations.
        let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;

        // Add x-amz-tagging-count header if object has tags
        // Per S3 API spec, this header should be present in HEAD object response when tags exist
        if tag_count > 0 {
            let header_name = http::HeaderName::from_static(AMZ_TAG_COUNT);
            if let Ok(header_value) = tag_count.to_string().parse::<HeaderValue>() {
                response.headers.insert(header_name, header_value);
            } else {
                warn!("Failed to parse x-amz-tagging-count header value, skipping");
            }
        }
        if let Some(retain_date) = metadata_map
            .get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(retain_date)
        {
            response.headers.insert(header_name, header_value);
        }
        if let Some(mode) = metadata_map
            .get(AMZ_OBJECT_LOCK_MODE_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_MODE))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_MODE_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(mode)
        {
            response.headers.insert(header_name, header_value);
        }
        if let Some(legal_hold) = metadata_map
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_LEGAL_HOLD))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(legal_hold)
        {
            response.headers.insert(header_name, header_value);
        }

        if let Some(amz_restore) = metadata_map.get(X_AMZ_RESTORE.as_str()) {
            let Ok(restore_status) = parse_restore_obj_status(amz_restore) else {
                return Err(S3Error::with_message(S3ErrorCode::Custom("ErrMeta".into()), "parse amz_restore failed."));
            };
            if let Ok(header_value) = HeaderValue::from_str(restore_status.to_string2().as_str()) {
                response.headers.insert(X_AMZ_RESTORE, header_value);
            }
        }
        if let Some(amz_restore_request_date) = metadata_map.get(AMZ_RESTORE_REQUEST_DATE)
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_RESTORE_REQUEST_DATE.as_bytes())
        {
            let Ok(amz_restore_request_date) = OffsetDateTime::parse(amz_restore_request_date, &Rfc3339) else {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("ErrMeta".into()),
                    "parse amz_restore_request_date failed.",
                ));
            };
            let Ok(amz_restore_request_date) = amz_restore_request_date.format(&RFC1123) else {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("ErrMeta".into()),
                    "format amz_restore_request_date failed.",
                ));
            };
            if let Ok(header_value) = HeaderValue::from_str(&amz_restore_request_date) {
                response.headers.insert(header_name, header_value);
            }
        }
        if let Some(amz_restore_expiry_days) = metadata_map.get(AMZ_RESTORE_EXPIRY_DAYS)
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_RESTORE_EXPIRY_DAYS.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(amz_restore_expiry_days)
        {
            response.headers.insert(header_name, header_value);
        }

        let result = Ok(response);
        let _ = helper.complete(&result);

        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectRestorePost, S3Operation::RestoreObject);
        let RestoreObjectInput {
            bucket,
            key: object,
            restore_request: rreq,
            version_id,
            ..
        } = req.input.clone();

        let rreq = rreq.ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::Custom("ErrValidRestoreObject".into()), "restore request is required")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_str = version_id.clone().unwrap_or_default();
        let opts = post_restore_opts(&version_id_str, &bucket, &object)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrPostRestoreOpts".into()), "restore object failed."))?;

        let mut obj_info = store
            .get_object_info(&bucket, &object, &opts)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "restore object failed."))?;

        // Check if object is in a transitioned state
        if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrInvalidTransitionedState".into()),
                "restore object failed.",
            ));
        }

        // Validate restore request
        if let Err(e) = rreq.validate(store.clone()) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrValidRestoreObject".into()),
                format!("Restore object validation failed: {}", e),
            ));
        }

        // Check if restore is already in progress
        if obj_info.restore_ongoing && (rreq.type_.as_ref().is_none_or(|t| t.as_str() != "SELECT")) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrObjectRestoreAlreadyInProgress".into()),
                "restore object failed.",
            ));
        }

        let mut already_restored = false;
        if let Some(restore_expires) = obj_info.restore_expires
            && !obj_info.restore_ongoing
            && restore_expires.unix_timestamp() != 0
        {
            already_restored = true;
        }

        let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), *rreq.days.as_ref().unwrap_or(&1));
        let mut metadata = obj_info.user_defined.clone();

        let mut header = HeaderMap::new();

        let event_object_info = obj_info.clone();
        let obj_info_ = obj_info.clone();
        if rreq.type_.as_ref().is_none_or(|t| t.as_str() != "SELECT") {
            obj_info.metadata_only = true;
            metadata.insert(AMZ_RESTORE_EXPIRY_DAYS.to_string(), rreq.days.unwrap_or(1).to_string());
            let request_date = OffsetDateTime::now_utc().format(&Rfc3339).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InternalError, format!("format restore request date failed: {}", e))
            })?;
            metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), request_date);
            if already_restored {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(false),
                        restore_expiry_date: Some(Timestamp::from(restore_expiry)),
                    }
                    .to_string(),
                );
            } else {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(true),
                        restore_expiry_date: Some(Timestamp::from(OffsetDateTime::now_utc())),
                    }
                    .to_string(),
                );
            }
            obj_info.user_defined = metadata;

            store
                .clone()
                .copy_object(
                    &bucket,
                    &object,
                    &bucket,
                    &object,
                    &mut obj_info,
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        ..Default::default()
                    },
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        mod_time: obj_info_.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrCopyObject".into()), "restore object failed."))?;

            if already_restored {
                let output = RestoreObjectOutput {
                    request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
                    restore_output_path: None,
                };
                helper = helper
                    .object(event_object_info.clone())
                    .version_id(version_id_str.clone())
                    .suppress_event();
                let result = Ok(S3Response::new(output));
                let _ = helper.complete(&result);
                return result;
            }
        }

        // Handle output location for SELECT requests
        if let Some(output_location) = &rreq.output_location
            && let Some(s3) = &output_location.s3
            && !s3.bucket_name.is_empty()
        {
            let restore_object = Uuid::new_v4().to_string();
            if let Ok(header_value) = format!("{}{}{}", s3.bucket_name, s3.prefix, restore_object).parse() {
                header.insert(X_AMZ_RESTORE_OUTPUT_PATH, header_value);
            }
        }

        // Spawn restoration task in the background
        let store_clone = store.clone();
        let bucket_clone = bucket.clone();
        let object_clone = object.clone();
        let rreq_clone = rreq.clone();
        let version_id_clone = version_id.clone();

        spawn_traced(async move {
            let opts = ObjectOptions {
                transition: TransitionOptions {
                    restore_request: rreq_clone,
                    restore_expiry,
                    ..Default::default()
                },
                version_id: version_id_clone,
                ..Default::default()
            };

            if let Err(err) = store_clone
                .restore_transitioned_object(&bucket_clone, &object_clone, &opts)
                .await
            {
                warn!(
                    "unable to restore transitioned bucket/object {}/{}: {}",
                    bucket_clone,
                    object_clone,
                    err.to_string()
                );
            } else {
                info!("successfully restored transitioned object: {}/{}", bucket_clone, object_clone);
            }
        });

        let output = RestoreObjectOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
            restore_output_path: None,
        };
        helper = helper.object(event_object_info).version_id(version_id_str);
        let result = Ok(S3Response::with_headers(output, header));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        info!("handle select_object_content");

        let input = Arc::new(req.input);
        info!("{:?}", input);

        let db = get_global_db((*input).clone(), false).await.map_err(|e| {
            error!("get global db failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;
        let query = Query::new(Context { input: input.clone() }, input.request.expression.clone());
        let result = db
            .execute(&query)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?;

        let results = result
            .result()
            .chunk_result()
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?
            .to_vec();

        let mut buffer = Vec::new();
        if input.request.output_serialization.csv.is_some() {
            let mut csv_writer = CsvWriterBuilder::new().with_header(false).build(&mut buffer);
            for batch in results {
                csv_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "can't encode output to csv. e: {}", e.to_string()))?;
            }
        } else if input.request.output_serialization.json.is_some() {
            let mut json_writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut buffer);
            for batch in results {
                json_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "can't encode output to json. e: {}", e.to_string()))?;
            }
            json_writer
                .finish()
                .map_err(|e| s3_error!(InternalError, "writer output into json error, e: {}", e.to_string()))?;
        } else {
            return Err(s3_error!(
                InvalidArgument,
                "Unsupported output format. Supported formats are CSV and JSON"
            ));
        }

        let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(2);
        let stream = ReceiverStream::new(rx);
        spawn_traced(async move {
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
                .await;
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Records(RecordsEvent {
                    payload: Some(Bytes::from(buffer)),
                })))
                .await;
            let _ = tx.send(Ok(SelectObjectContentEvent::End(EndEvent::default()))).await;

            drop(tx);
        });

        Ok(S3Response::new(SelectObjectContentOutput {
            payload: Some(SelectObjectContentEventStream::new(stream)),
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, S3Operation::PutObject).suppress_event();
        let auth_method = req.method.clone();
        let auth_uri = req.uri.clone();
        let auth_headers = req.headers.clone();
        let auth_extensions = req.extensions.clone();
        let auth_credentials = req.credentials.clone();
        let auth_region = req.region.clone();
        let auth_service = req.service.clone();
        let auth_trailing_headers = req.trailing_headers.clone();
        if is_sse_kms_requested(&req.input, &req.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        let input = req.input;

        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            cache_control,
            content_disposition,
            content_encoding,
            content_length,
            content_language,
            content_type,
            content_md5,
            expires,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            storage_class,
            tagging,
            website_redirect_location,
            ..
        } = input;

        let event_version_id = version_id;
        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        let original_sse = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let mut effective_sse = original_sse.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .map(|sse| match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                        })
                })
            })
        });
        let mut effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });
        if effective_sse
            .as_ref()
            .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };
        if size == -1 {
            return Err(s3_error!(UnexpectedContent));
        }
        validate_object_key(&key, "PUT")?;
        self.check_bucket_quota(&bucket, QuotaOperation::PutObject, size as u64)
            .await?;

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let Some(ext) = Path::new(&key).extension().and_then(|s| s.to_str()) else {
            return Err(s3_error!(InvalidArgument, "key extension not found"));
        };

        let ext = ext.to_owned();

        let md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let sha256hex = get_content_sha256_with_query(&req.headers, req.uri.query());
        let actual_size = size;

        let mut archive_reader =
            HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) = archive_reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
            return Err(ApiError::from(err).into());
        }

        let archive_etag = Arc::new(Mutex::new(None));
        let decoder = CompressionFormat::from_extension(&ext)
            .get_decoder(ExtractArchiveEtagReader::new(archive_reader, archive_etag.clone()))
            .map_err(|e| {
                error!("get_decoder err {:?}", e);
                s3_error!(InvalidArgument, "get_decoder err")
            })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!("get entries err {:?}", e);
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let extract_options = resolve_put_object_extract_options(&req.headers);
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let notify = self
            .context
            .as_ref()
            .map(|context| context.notify())
            .unwrap_or_else(default_notify_interface);
        let req_params = extract_params_header(&req.headers);
        let host = get_request_host(&req.headers);
        let port = get_request_port(&req.headers);
        let user_agent = get_request_user_agent(&req.headers);

        while let Some(entry) = entries.next().await {
            let mut f = match entry {
                Ok(f) => f,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because read failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    error!("Failed to read archive entry: {}", e);
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };

            let fpath = match f.path() {
                Ok(path) => path,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because path decode failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    return Err(s3_error!(InvalidArgument, "Failed to decode archive entry path"));
                }
            };

            let is_dir = f.header().entry_type().is_dir();
            let fpath = normalize_extract_entry_key(&fpath.to_string_lossy(), extract_options.prefix.as_deref(), is_dir);

            let mut auth_req = S3Request {
                input: PutObjectInput::default(),
                method: auth_method.clone(),
                uri: auth_uri.clone(),
                headers: auth_headers.clone(),
                extensions: auth_extensions.clone(),
                credentials: auth_credentials.clone(),
                region: auth_region.clone(),
                service: auth_service.clone(),
                trailing_headers: auth_trailing_headers.clone(),
            };
            {
                let req_info = req_info_mut(&mut auth_req)?;
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(fpath.clone());
                req_info.version_id = None;
            }
            authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectAction)).await?;

            let mut size = f.header().size().unwrap_or_default() as i64;
            let archive_entry_mod_time = f
                .header()
                .mtime()
                .ok()
                .and_then(|modified_at_secs| OffsetDateTime::from_unix_timestamp(modified_at_secs as i64).ok());
            let mut metadata = HashMap::new();
            apply_put_request_metadata(
                &mut metadata,
                &req.headers,
                &fpath,
                cache_control.clone(),
                content_disposition.clone(),
                content_encoding.clone(),
                content_language.clone(),
                content_type.clone(),
                expires.clone(),
                website_redirect_location.clone(),
                tagging.clone(),
                storage_class.clone(),
            )?;
            let mut opts = put_opts(&bucket, &fpath, None, &req.headers, metadata.clone())
                .await
                .map_err(ApiError::from)?;
            apply_extract_entry_pax_extensions(&mut f, &mut metadata, &mut opts).await?;
            if archive_entry_mod_time.is_some() {
                opts.mod_time = archive_entry_mod_time;
            }

            debug!("Extracting file: {}, size: {} bytes", fpath, size);

            if is_dir {
                if extract_options.ignore_dirs {
                    debug!("Skipping directory entry during archive extract: {}", fpath);
                    continue;
                }
                size = 0;
            }

            let actual_size = size;

            let should_compress = !is_dir && is_compressible(&HeaderMap::new(), &fpath) && size > MIN_COMPRESSIBLE_SIZE as i64;

            let mut hrd = if is_dir {
                HashReader::from_stream(std::io::Cursor::new(Vec::new()), size, actual_size, None, None, false)
                    .map_err(ApiError::from)?
            } else if should_compress {
                insert_str(&mut metadata, SUFFIX_COMPRESSION, CompressionAlgorithm::default().to_string());
                insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

                let hrd = HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?;
                size = HashReader::SIZE_PRESERVE_LAYER;
                HashReader::from_reader(
                    CompressReader::new(hrd, CompressionAlgorithm::default()),
                    size,
                    actual_size,
                    None,
                    None,
                    false,
                )
                .map_err(ApiError::from)?
            } else {
                HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?
            };
            apply_put_request_object_lock_opts(
                &bucket,
                object_lock_legal_hold_status.clone(),
                object_lock_mode.clone(),
                object_lock_retain_until_date.clone(),
                &mut opts,
            )
            .await?;
            if let Some(material) = sse_encryption(EncryptionRequest {
                bucket: &bucket,
                key: &fpath,
                server_side_encryption: effective_sse.clone(),
                ssekms_key_id: effective_kms_key_id.clone(),
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
                part_number: None,
                part_key: None,
                part_nonce: None,
            })
            .await?
            {
                effective_sse = Some(material.server_side_encryption.clone());
                effective_kms_key_id = material.kms_key_id.clone();

                let encrypted_reader = material.wrap_reader(hrd);
                hrd = HashReader::from_reader(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                let encryption_metadata = material.metadata;
                metadata.extend(encryption_metadata.clone());
                opts.user_defined.extend(encryption_metadata);
            }
            opts.user_defined.extend(metadata);
            let mut reader = PutObjReader::new(hrd);

            let obj_info = match store.put_object(&bucket, &fpath, &mut reader, &opts).await {
                Ok(info) => info,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!("Skipping archive entry because object write failed and ignore-errors is enabled: {e}");
                        continue;
                    }
                    return Err(ApiError::from(e).into());
                }
            };

            let _manager = get_concurrency_manager();
            let _fpath_clone = fpath.clone();
            let _bucket_clone = bucket.clone();
            let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

            let output = PutObjectOutput {
                e_tag,
                ..Default::default()
            };

            let event_args = rustfs_notify::EventArgs {
                event_name: EventName::ObjectCreatedPut,
                bucket_name: bucket.clone(),
                object: obj_info.clone(),
                req_params: req_params.clone(),
                resp_elements: extract_resp_elements(&S3Response::new(output.clone())),
                version_id: version_id.clone(),
                host: host.clone(),
                port,
                user_agent: user_agent.clone(),
            };

            let notify = notify.clone();
            let request_context = req
                .extensions
                .get::<crate::storage::request_context::RequestContext>()
                .cloned();
            spawn_background_with_context(request_context, async move {
                notify.notify(event_args).await;
            });
        }

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &req.trailing_headers,
            &mut checksums,
        );

        warn!(
            "put object extract checksum_crc32={:?}, checksum_crc32c={:?}, checksum_sha1={:?}, checksum_sha256={:?}, checksum_crc64nvme={:?}",
            checksums.crc32, checksums.crc32c, checksums.sha1, checksums.sha256, checksums.crc64nvme,
        );

        drop(entries);
        let mut decoder = match ar.into_inner() {
            Ok(decoder) => decoder,
            Err(_) => return Err(s3_error!(InvalidArgument, "Failed to finalize archive reader")),
        };
        tokio::io::copy(&mut decoder, &mut tokio::io::sink())
            .await
            .map_err(map_extract_archive_error)?;
        let archive_etag = archive_etag
            .lock()
            .ok()
            .and_then(|etag| etag.clone())
            .map(|etag| to_s3s_etag(&etag));

        let output = PutObjectOutput {
            e_tag: archive_etag,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            ..Default::default()
        };
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}

fn object_attributes_requested(object_attributes: &[ObjectAttributes], name: &'static str) -> bool {
    object_attributes.iter().any(|value| {
        value.as_str().split(',').any(|part| {
            part.trim_matches(|c: char| c.is_whitespace() || c == '"' || c == '\'')
                .eq_ignore_ascii_case(name)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, Uri};
    use s3s::dto::{
        DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ExistingObjectReplication,
        ExistingObjectReplicationStatus, ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus,
    };

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[test]
    fn is_put_object_extract_requested_accepts_meta_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_accepts_compat_header_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT_COMPAT, HeaderValue::from_static(" TRUE "));

        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_rejects_missing_or_false_value() {
        let mut headers = HeaderMap::new();
        assert!(!is_put_object_extract_requested(&headers));

        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("false"));
        assert!(!is_put_object_extract_requested(&headers));
    }

    #[test]
    fn normalize_snowball_prefix_trims_slashes_and_whitespace() {
        assert_eq!(normalize_snowball_prefix(" /batch/incoming/ "), Some("batch/incoming".to_string()));
        assert_eq!(normalize_snowball_prefix("///"), None);
    }

    #[test]
    fn normalize_extract_entry_key_applies_prefix_and_directory_suffix() {
        assert_eq!(
            normalize_extract_entry_key("nested/path.txt", Some("imports"), false),
            "imports/nested/path.txt"
        );
        assert_eq!(normalize_extract_entry_key("nested/dir/", Some("imports"), true), "imports/nested/dir/");
        assert_eq!(normalize_extract_entry_key("top-level", None, false), "top-level");
    }

    #[test]
    fn should_use_zero_copy_rejects_boundary_at_1mb() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_small_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024 - 1, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_one_megabyte() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_sse_customer_algorithm() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_kms_key_id() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_compressible_content_types() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_allows_large_unencrypted_binary_objects() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn resolve_put_object_extract_options_defaults_when_headers_missing() {
        let headers = HeaderMap::new();
        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(
            options,
            PutObjectExtractOptions {
                prefix: None,
                ignore_dirs: false,
                ignore_errors: false
            }
        );
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_internal_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX_INTERNAL, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL, HeaderValue::from_static("true"));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("internal/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static(" /standard/prefix/ "));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static(" true "));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("standard/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_suffix_compatible_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-prefix"),
            HeaderValue::from_static(" /partner/import "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-dirs"),
            HeaderValue::from_static(" true "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-errors"),
            HeaderValue::from_static("TRUE"),
        );

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("partner/import"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_prefers_exact_headers_over_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-acme-snowball-prefix", HeaderValue::from_static("/fallback/prefix/"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_PREFIX, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("/standard/prefix/"));
        headers.insert(AMZ_MINIO_SNOWBALL_PREFIX, HeaderValue::from_static("/minio/prefix/"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("minio/prefix"));
    }

    #[test]
    fn resolve_put_object_extract_options_exact_flags_override_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-dirs", HeaderValue::from_static("true"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-errors", HeaderValue::from_static("true"));

        let options = resolve_put_object_extract_options(&headers);
        assert!(!options.ignore_dirs);
        assert!(!options.ignore_errors);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_input() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_extract_sse_kms() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_extract_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID")))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_headers() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("aws:kms"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_key_id_header() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID-STORAGE-CLASS")))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[test]
    fn response_storage_class_omits_standard_and_keeps_non_default() {
        let metadata = HashMap::new();
        let standard_info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD.to_string()),
            user_defined: metadata.clone(),
            ..Default::default()
        };
        assert!(response_storage_class(&standard_info, &metadata).is_none());

        let mut metadata = HashMap::new();
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string());
        let infrequent_access_info = ObjectInfo {
            storage_class: Some(storageclass::STANDARD_IA.to_string()),
            user_defined: metadata.clone(),
            ..Default::default()
        };
        assert_eq!(
            response_storage_class(&infrequent_access_info, &metadata)
                .as_ref()
                .map(StorageClass::as_str),
            Some(storageclass::STANDARD_IA)
        );
    }

    #[tokio::test]
    async fn execute_get_object_rejects_zero_part_number() {
        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(0))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_get_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_self_copy_without_replace_directive() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_delete_object_rejects_invalid_object_key() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("bad\0key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_delete_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_empty_object_list() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_returns_internal_error_when_store_uninitialized() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "test-key".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn should_schedule_delete_replication_skips_replica_requests() {
        let opts = ObjectOptions {
            replication_request: true,
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let replication_source = ObjectInfo {
            delete_marker: true,
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        assert!(
            !should_schedule_delete_replication(&opts, &replication_source, true),
            "replica delete requests on target sites must not enqueue a second replication delete task"
        );
    }

    #[test]
    fn should_schedule_delete_replication_keeps_delete_marker_version_purge_from_source() {
        let opts = ObjectOptions {
            replication_request: false,
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let replication_source = ObjectInfo {
            delete_marker: true,
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        assert!(
            should_schedule_delete_replication(&opts, &replication_source, true),
            "source-side delete-marker version purge still needs replication scheduling"
        );
    }

    #[test]
    fn should_schedule_delete_replication_keeps_object_version_purge_from_completed_source() {
        let opts = ObjectOptions {
            replication_request: false,
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let replication_source = ObjectInfo {
            delete_marker: false,
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        assert!(
            should_schedule_delete_replication(&opts, &replication_source, false),
            "source-side object version purge must still enqueue delete replication after the original PUT completed"
        );
    }

    #[tokio::test]
    async fn execute_get_object_attributes_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectAttributesInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_attributes(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn object_attributes_requested_with_single_value() {
        let object_attributes = vec![ObjectAttributes::from_static(ObjectAttributes::ETAG)];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_comma_separated_values() {
        let object_attributes = vec![
            ObjectAttributes::from_static("ObjectParts,etag"),
            ObjectAttributes::from_static("StorageClass"),
        ];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_PARTS));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_quotes_and_spaces() {
        let object_attributes = vec![ObjectAttributes::from_static("'ObjectSize', \"Checksum\" , \"Etag\"")];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::CHECKSUM));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
    }

    #[test]
    fn object_attributes_requested_returns_false_for_missing_name() {
        let object_attributes = vec![ObjectAttributes::from_static("Checksum")];

        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
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

    #[tokio::test]
    async fn execute_head_object_rejects_range_with_part_number() {
        let input = HeadObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(1))
            .range(Some(Range::Int { first: 0, last: Some(1) }))
            .build()
            .unwrap();

        let req = build_request(input, Method::HEAD);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_head_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_restore_object_rejects_missing_restore_request() {
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        match err.code() {
            S3ErrorCode::Custom(code) => assert_eq!(code, "ErrValidRestoreObject"),
            code => panic!("unexpected error code: {:?}", code),
        }
    }

    #[tokio::test]
    async fn execute_restore_object_returns_internal_error_when_store_uninitialized() {
        let restore_request = RestoreRequest {
            days: Some(1),
            description: None,
            glacier_job_parameters: None,
            output_location: None,
            select_parameters: None,
            tier: None,
            type_: None,
        };
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .restore_request(Some(restore_request))
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn delete_replication_state_from_config_tracks_downstream_delete_marker_targets() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn.clone(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: Some(SourceSelectionCriteria {
                    replica_modifications: Some(ReplicaModifications {
                        status: ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED),
                    }),
                    sse_kms_encrypted_objects: None,
                }),
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        let state = delete_replication_state_from_config(&config, &obj_info, None, true)
            .expect("replica delete marker should be forwarded to downstream targets");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.replication_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.targets.contains_key(&arn));
    }

    #[test]
    fn delete_replication_state_from_config_skips_replica_delete_without_replica_modifications() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn,
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        assert!(
            delete_replication_state_from_config(&config, &obj_info, None, true).is_none(),
            "replica deletes must only fan out when ReplicaModifications are enabled"
        );
    }

    #[test]
    fn delete_replication_state_from_config_tracks_delete_marker_version_purges() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let config = ReplicationConfiguration {
            role: arn.clone(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
                }),
                delete_replication: None,
                destination: Destination {
                    bucket: arn.clone(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some("test/".to_string()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };
        let obj_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "test/object.txt".to_string(),
            delete_marker: true,
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };

        let version_id = Some(Uuid::new_v4());
        let state = delete_replication_state_from_config(&config, &obj_info, version_id, false)
            .expect("delete-marker version purge should honor delete-marker replication rules");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.version_purge_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.purge_targets.contains_key(&arn));
    }

    #[test]
    fn delete_replication_state_source_prefers_existing_replica_for_replication_delete_marker_creation() {
        let opts = ObjectOptions {
            replication_request: true,
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };
        let existing = ObjectInfo {
            name: "test/object.txt".to_string(),
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };
        let deleted = ObjectInfo {
            name: "test/object.txt".to_string(),
            delete_marker: true,
            ..Default::default()
        };

        let source = delete_replication_state_source(&opts, Some(&existing), &deleted);

        assert_eq!(source.replication_status, ReplicationStatusType::Completed);
        assert!(
            !source.delete_marker,
            "downstream fanout should inherit replica identity from the pre-delete object"
        );
    }

    #[test]
    fn delete_replication_state_source_keeps_deleted_marker_for_non_replication_requests() {
        let opts = ObjectOptions::default();
        let existing = ObjectInfo {
            name: "test/object.txt".to_string(),
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };
        let deleted = ObjectInfo {
            name: "test/object.txt".to_string(),
            delete_marker: true,
            ..Default::default()
        };

        let source = delete_replication_state_source(&opts, Some(&existing), &deleted);

        assert!(
            source.delete_marker,
            "source-originated deletes should keep using the new delete marker state"
        );
    }

    #[test]
    fn replica_delete_enrichment_must_not_reuse_upstream_targets() {
        let delete_object = rustfs_ecstore::store_api::DeletedObject {
            replication_state: Some(ReplicationState {
                replicate_decision_str: "arn:aws:s3:::upstream=true;false;arn:aws:s3:::upstream;".to_string(),
                replication_status_internal: Some("arn:aws:s3:::upstream=COMPLETED;".to_string()),
                targets: replication_statuses_map("arn:aws:s3:::upstream=COMPLETED;"),
                ..Default::default()
            }),
            ..Default::default()
        };
        let obj_info = ObjectInfo {
            replication_status: ReplicationStatusType::Replica,
            ..Default::default()
        };

        let should_keep_existing = delete_object.replication_state.as_ref().is_some_and(|state| {
            obj_info.replication_status != ReplicationStatusType::Replica
                && !state.replicate_decision_str.is_empty()
                && (!state.targets.is_empty() || !state.purge_targets.is_empty())
        });

        assert!(
            !should_keep_existing,
            "replica fanout deletes must recompute targets from the local bucket config instead of reusing upstream replication state"
        );
    }

    #[test]
    fn delete_replication_version_id_uses_none_for_delete_marker_creation() {
        let source = ObjectInfo {
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        assert_eq!(
            delete_replication_version_id(&source, false),
            None,
            "delete-marker creation must stay on the delete-marker replication path"
        );
    }

    #[test]
    fn delete_replication_version_id_keeps_version_for_marker_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            delete_marker: true,
            version_id: Some(version_id),
            ..Default::default()
        };

        assert_eq!(
            delete_replication_version_id(&source, true),
            Some(version_id),
            "delete-marker version purge must preserve the concrete version id for downstream purge replication"
        );
    }

    #[test]
    fn should_use_existing_delete_replication_info_ignores_replication_delete_marker_creation() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            delete_marker: true,
            ..Default::default()
        };

        assert!(
            !should_use_existing_delete_replication_info(&opts),
            "replicated delete-marker creation carries a source version id header but must not be treated as a version purge"
        );
    }

    #[test]
    fn should_use_existing_delete_replication_info_keeps_version_delete_requests() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        };

        assert!(
            should_use_existing_delete_replication_info(&opts),
            "true version-delete requests should keep using the pre-delete object info"
        );
    }
}
