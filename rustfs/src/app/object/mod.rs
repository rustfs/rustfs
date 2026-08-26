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

// Performance metrics recording (with zero-copy-metrics integration)
use rustfs_io_metrics::buffered_write;

use crate::storage_api::table::get_bucket_metadata;

use super::storage_api::object_usecase::access::{
    PostObjectRequestMarker, apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard, authorize_request,
    has_bypass_governance_header, load_bucket_generation_from_store, recursive_force_delete_is_authorized,
    replication_request_authorized, req_info_mut, req_info_ref,
};
#[cfg(test)]
use super::storage_api::object_usecase::bucket::quota::BucketQuota;
use super::storage_api::object_usecase::bucket::quota::checker::QuotaChecker;
#[cfg(test)]
use super::storage_api::object_usecase::bucket::replication::{ReplicationState, replication_statuses_map};
use super::storage_api::object_usecase::bucket::{
    VersioningConfigExt as _,
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{enqueue_transition_immediate, post_restore_opts},
        lifecycle::{self, TransitionOptions},
    },
    metadata_sys,
    object_lock::{
        objectlock::{get_object_legalhold_meta, get_object_retention_meta},
        objectlock_sys::{check_object_lock_for_deletion, is_retention_active, replication_write_may_pass_worm_gate},
        types::RetentionMode,
    },
    predict_lifecycle_expiration,
    quota::{QuotaCheckResult, QuotaError, QuotaOperation},
    replication::{
        DeleteReplicationConfigSnapshot, REPLICATE_INCOMING_DELETE, ReplicationStatusType, commit_force_delete_intent,
        delete_replication_state_from_config, delete_replication_version_id, deleted_object_has_pending_replication_delete,
        force_delete_target_set, get_read_proxy_targets, has_active_delete_rule, load_delete_config_snapshot,
        must_replicate_object, persist_force_delete_intent, record_replication_proxy, schedule_object_replication,
        schedule_replication_delete, schedule_replication_deletes, set_deleted_object_replication_state,
        should_schedule_delete_replication, should_use_existing_delete_replication_info,
    },
    tagging::decode_tags,
    validate_restore_request,
    versioning_sys::BucketVersioningSys,
};
use super::storage_api::object_usecase::compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
use super::storage_api::object_usecase::concurrency::{
    self, ConcurrencyManager, DiskReadAdmission, GetObjectGuard, PutObjectAdmission, PutObjectGuard,
    get_concurrency_aware_buffer_size, get_concurrency_manager, get_put_concurrency_aware_buffer_size,
};
#[cfg(test)]
use super::storage_api::object_usecase::contract::http::HTTPPreconditions;
use super::storage_api::object_usecase::contract::namespace::NamespaceLocking;
use super::storage_api::object_usecase::contract::object::{ObjectIO as _, ObjectOperations as _};
use super::storage_api::object_usecase::contract::range::HTTPRangeSpec;
use super::storage_api::object_usecase::data_usage::{
    quota_object_size, record_bucket_delete_marker_memory, record_bucket_object_delete_memory,
    record_bucket_object_version_write_memory, record_bucket_object_write_memory,
    record_bucket_object_write_unknown_previous_memory,
};
use super::storage_api::object_usecase::deadlock_detector;
use super::storage_api::object_usecase::ecfs::FS;
use super::storage_api::object_usecase::error::{
    Error as EcstoreError, StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found,
};
use super::storage_api::object_usecase::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use super::storage_api::object_usecase::helper::{OperationHelper, build_event_resp_elements, spawn_background_with_context};
use super::storage_api::object_usecase::io::{DynReader, HashReader, WritePlan, compression_metadata_value, wrap_reader};
#[cfg(test)]
use super::storage_api::object_usecase::object_cache::GetObjectBodySource;
#[cfg(test)]
use super::storage_api::object_usecase::object_cache::lookup_get_object_body_cache_hook;
use super::storage_api::object_usecase::object_cache::{GetObjectBodyCacheHookLookup, get_object_body_cache_plaintext_len};
use super::storage_api::object_usecase::object_utils::to_s3s_etag;
use super::storage_api::object_usecase::options::{
    copy_dst_opts_with_replication_authorization, copy_src_opts, del_opts_with_versioning, extract_metadata,
    extract_metadata_from_mime_with_object_name, filter_object_metadata, get_content_sha256_with_query, get_opts,
    has_replication_retention_update, namespace_reserved_user_metadata, normalize_content_encoding_for_storage,
    preserve_unclassified_user_metadata, put_opts_with_replication_authorization, validate_archive_content_encoding,
};
use super::storage_api::object_usecase::request_context::{self, spawn_traced, spawn_traced_join};
use super::storage_api::object_usecase::s3_api::multipart::parse_list_parts_params;
use super::storage_api::object_usecase::set_disk::{
    get_lock_acquire_timeout, get_object_disk_read_timeout, is_valid_storage_class,
};
use super::storage_api::object_usecase::sse::{
    DecryptionRequest, EncryptionRequest, SseKmsPrincipal, apply_bucket_default_lock_retention, authorize_sse_kms_object_read,
    bucket_default_write_sse, build_ssec_read_headers, classify_sse_read_response, encryption_material_to_metadata,
    extract_server_side_encryption_from_headers, extract_ssec_params_from_headers, extract_ssekms_context_from_headers,
    get_buffer_size_opt_in, load_bucket_object_lock_config_state, map_get_object_reader_error, sse_encryption,
    validate_bucket_object_lock_enabled_state,
};
use super::storage_api::object_usecase::storage_class as storageclass;
use super::storage_api::object_usecase::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
use super::storage_api::object_usecase::{ECStore, OldCurrentSize};
use super::storage_api::object_usecase::{
    RFC1123, check_preconditions, parse_object_lock_legal_hold, parse_object_lock_retention, parse_part_number_i32_to_usize,
    remove_object_lock_metadata_for_copy, strip_managed_encryption_metadata, validate_bucket_exists, validate_object_key,
    validate_sse_headers_for_read, validate_sse_headers_for_write, validate_ssec_for_read, wrap_response_with_cors,
};
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_notify_interface_for_context, current_object_data_cache_for_context,
    current_object_store_handle_for_context,
};
use crate::config::RustFSBufferConfig;
use crate::delete_tail_activity::{DeleteTailActivityGuard, DeleteTailStage};
use crate::error::ApiError;
use crate::shared_types::convert_ecstore_object_info;
use crate::table_catalog;
use bytes::{BufMut as _, Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue, StatusCode};
use md5::{Digest as Md5Digest, Md5};
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use rustfs_audit::ObjectVersion as AuditObjectVersion;
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_config::MI_B;
use rustfs_filemeta::{NULL_VERSION_ID, RestoreStatusOps, parse_restore_obj_status};
use rustfs_io_core::{BytesPool, PooledBuffer};
use rustfs_io_metrics;
use rustfs_lock::NamespaceLockGuard;
use rustfs_notify::EventArgsBuilder;
use rustfs_object_capacity::capacity_manager::get_capacity_manager;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::{S3Operation, delete_event_name_for_marker, put_event_name_for_post_object};
use rustfs_targets::{EventName, get_request_host, get_request_port, get_request_user_agent};
use rustfs_utils::CompressionAlgorithm;
#[cfg(test)]
use rustfs_utils::http::headers::{SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER};
#[cfg(test)]
use rustfs_utils::http::insert_header;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE, AMZ_WEBSITE_REDIRECT_LOCATION, CONTENT_TYPE,
    SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_REPLICA_STATUS, SUFFIX_REPLICA_TIMESTAMP,
    SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_RESTORE_OPERATION_ID, SUFFIX_SOURCE_REPLICATION_CHECK,
    SUFFIX_SOURCE_REPLICATION_REQUEST, get_header,
    headers::{
        AMZ_CONTENT_SHA256, AMZ_DECODED_CONTENT_LENGTH, AMZ_MINIO_SNOWBALL_IGNORE_DIRS, AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
        AMZ_MINIO_SNOWBALL_PREFIX, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE,
        AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
        AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE, AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
        AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, AMZ_RUSTFS_SNOWBALL_PREFIX, AMZ_SERVER_SIDE_ENCRYPTION,
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, AMZ_SNOWBALL_EXTRACT,
        AMZ_SNOWBALL_IGNORE_DIRS, AMZ_SNOWBALL_IGNORE_ERRORS, AMZ_SNOWBALL_PREFIX, AMZ_STORAGE_CLASS, AMZ_TAG_COUNT,
    },
    insert_str, project_ssec_transport_headers, remove_str,
};
use rustfs_utils::path::{encode_dir_object, is_dir_object, path_join_buf};
use rustfs_utils::retry::{DEFAULT_RETRY_CAP, DEFAULT_RETRY_UNIT, MAX_JITTER, RetryTimer};
use rustfs_zip::{ArchiveLimits, CompressionFormat};
use s3s::StdError;
use s3s::dto::{
    CacheControl, Checksum, ChecksumAlgorithm, ChecksumType, ContentDisposition, ContentEncoding, ContentLanguage, ContentType,
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, ETag, GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectAttributesParts,
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, MetadataDirective, ObjectAttributes, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode, ObjectPart, PutObjectInput,
    PutObjectOutput, Range, RequestCharged, RestoreObjectInput, RestoreObjectOutput, RestoreStatus, SSECustomerAlgorithm,
    SSECustomerKeyMD5, SSEKMSKeyId, SelectObjectContentInput, SelectObjectContentOutput, ServerSideEncryption,
    ServerSideEncryptionConfiguration, StorageClass, StreamingBlob, TaggingDirective, TaggingHeader, Timestamp, TimestampFormat,
    WebsiteRedirectLocation,
};
use s3s::header::{X_AMZ_RESTORE, X_AMZ_RESTORE_OUTPUT_PATH};
use s3s::stream::{ByteStream, DynByteStream, RemainingLength};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

mod extract;
mod get;
mod shared;
#[cfg(test)]
mod test_support;

pub(crate) use self::extract::*;
pub(crate) use self::get::*;
pub(crate) use self::shared::*;
#[cfg(test)]
use self::test_support::*;

const DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES: i64 = 32 * 1024 * 1024;
const ENV_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: &str = "RUSTFS_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES";
const DEFAULT_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: usize = 16 * 1024 * 1024;
const PUT_EAGER_STATUS_ELIGIBLE: &str = "eligible";
const PUT_EAGER_STATUS_EXTRACT: &str = "extract";
const PUT_EAGER_STATUS_COMPRESSED: &str = "compressed";
const PUT_EAGER_STATUS_ENCRYPTED: &str = "encrypted";
const PUT_EAGER_STATUS_INVALID_SIZE: &str = "invalid_size";
const PUT_EAGER_STATUS_ABOVE_EAGER_MAX: &str = "above_eager_max";
const PUT_EAGER_STATUS_ZERO_COPY_INELIGIBLE: &str = "zero_copy_ineligible";
const PUT_EAGER_STATUS_AWS_CHUNKED_MISSING_DECODED_LENGTH: &str = "aws_chunked_missing_decoded_length";
static CACHED_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
use std::collections::HashMap;
use std::io;
use std::ops::Add;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use std::str::FromStr;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, RwLock};
use tokio_tar::Archive;
#[cfg(test)]
use tokio_util::io::ReaderStream;
use tokio_util::io::{StreamReader, poll_read_buf};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use super::storage_api::object_usecase::{
    BUCKET_LIFECYCLE_LOCK_OBJECT, GetObjectReader, StorageDeletedObject, StorageObjectInfo as ObjectInfo,
    StorageObjectLockDeleteOptions, StorageObjectOptions as ObjectOptions, StorageObjectToDelete as ObjectToDelete,
    StoragePutObjReader as PutObjReader,
};
use crate::app::object_data_cache::{
    ColdFillCoordinateOutcome, ColdFillDiskPermitOwner, ColdFillError, ColdFillProducer, GetObjectBodyCacheLookup,
    GetObjectBodyCachePlan, GetObjectBodyCacheRequest, ObjectDataCacheAdapter, build_get_object_body_cache_plan,
    build_get_object_body_cache_plan_for_revalidation, coordinate_cold_fill, current_cold_fill_disk_permit_owner,
    fill_get_object_body_cache_from_buffered_body, fill_get_object_body_cache_from_materialized_body,
    invalidate_object_data_cache_after_copy_success, invalidate_object_data_cache_after_delete_success,
    invalidate_object_data_cache_after_put_success, invalidate_object_data_cache_before_mutation,
    invalidate_object_data_cache_objects_after_delete_success, invalidate_object_data_cache_objects_before_mutation,
    invalidate_object_data_cache_prefix_after_delete, invalidate_object_data_cache_prefix_before_mutation,
    lookup_get_object_body_cache_hit, lookup_preplanned_get_object_body_cache_hook,
};
#[cfg(test)]
use crate::app::object_data_cache::{ColdFillRole, ColdFillWaitOutcome, scope_cold_fill_disk_permit_owner_for_test};
use crate::app::object_traffic_health::ObjectTrafficHealth;

const EVENT_PUT_OBJECT_STORE_INFLIGHT_SLOW: &str = "put_object_store_inflight_slow";
const EVENT_PUT_OBJECT_STORE_RETURNED: &str = "put_object_store_returned";
const EVENT_PUT_OBJECT_COMMIT_OWNER_DEADLINE: &str = "put_object_commit_owner_deadline";
const EVENT_PUT_OBJECT_BODY_READ_STALLED: &str = "put_object_body_read_stalled";
const PUT_OBJECT_STORE_WARN_THRESHOLD: Duration = Duration::from_secs(5);
// Eager PUT bodies are fully materialized before the storage owner starts. On
// request cancellation, keep the commit/publication tail alive briefly, then
// request pre-commit rollback and await cleanup so its write-health guard is
// reaped without abandoning staged shards.
const EAGER_PUT_COMMIT_CANCELLATION_GRACE: Duration =
    Duration::from_secs(rustfs_config::DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS * 4);

/// Resolve the authoritative object length that bucket-quota admission (and downstream sizing) must use.
///
/// `Content-Encoding: aws-chunked` alone only *declares* the encoding; whether the body actually arrived chunk-framed is signalled by a `STREAMING-*` `x-amz-content-sha256`, and the S3 auth layer both requires `x-amz-decoded-content-length` for those requests and hands the body down already de-framed. So when a decoded length is present it is authoritative (the wire `Content-Length` counts chunk framing and would overcount); a framed body without a decoded length is rejected rather than falling back to the framed wire length. A declared-only aws-chunked request (issue #1857 clients) carries an unframed body, so its wire `Content-Length` is the authoritative size, exactly as for a plain PUT. A negative or otherwise unknown length is rejected so it can never be reinterpreted as an enormous unsigned size downstream.
fn resolve_put_object_authoritative_size(headers: &HeaderMap, content_length: Option<i64>) -> S3Result<i64> {
    let decoded_content_length = decoded_content_length_from_headers(headers)?;
    let aws_chunked = request_uses_aws_chunked(headers) || request_body_is_aws_chunked_framed(headers);
    let size = match (aws_chunked, decoded_content_length, content_length) {
        (true, Some(decoded), _) => decoded,
        // Declared aws-chunked without a streaming payload: the body is not framed (the auth
        // layer only de-frames STREAMING-* payloads, which always carry a decoded length), so
        // the wire Content-Length is the real object size.
        (true, None, Some(raw)) if !request_body_is_aws_chunked_framed(headers) => raw,
        (true, None, _) => return Err(s3_error!(UnexpectedContent)),
        (false, _, Some(raw)) => raw,
        (false, Some(decoded), None) => decoded,
        (false, None, None) => return Err(s3_error!(UnexpectedContent)),
    };

    if size < 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(size)
}

/// Resolve the S3 request-body inter-chunk read timeout from the environment.
///
/// Returns `Duration::ZERO` when disabled (`RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT=0`),
/// in which case [`guard_put_object_body_read_timeout`] passes the body through
/// untouched.
fn put_object_body_read_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_HTTP_REQUEST_BODY_READ_TIMEOUT,
        rustfs_config::DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT,
    ))
}

/// A [`ByteStream`] decorator that aborts a request body whose peer stops
/// sending bytes without closing the connection.
///
/// A well-behaved short body ends with EOF and is rejected promptly by the
/// eager/streaming readers. The failure this guards against is different: a
/// reverse proxy or CDN forwards a *partial* body and then goes silent while
/// holding the connection open, so the inner stream neither yields more bytes
/// nor reports EOF. Without a bound, RustFS would wait forever for bytes that
/// never arrive and the client eventually sees a hang/abort with no server-side
/// explanation (issue #3076).
///
/// The timer resets on every chunk, so slow-but-progressing uploads are not
/// penalized; it only fires after `timeout` of complete silence. On timeout the
/// stall is logged with the received/expected byte counts and the read fails
/// with an `ErrorKind::TimedOut` error instead of hanging.
///
/// `remaining_length` and `size_hint` are forwarded from the inner stream so
/// wrapping is transparent to length/content handling downstream.
struct RequestBodyReadTimeout {
    inner: DynByteStream,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    received: u64,
    expected: Option<u64>,
    bucket: String,
    key: String,
    request_id: String,
    timed_out: bool,
}

impl Stream for RequestBodyReadTimeout {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Once we have surfaced a stall error, treat the stream as terminated so
        // we never poll the abandoned inner stream again.
        if this.timed_out {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.timer = None;
                this.received = this.received.saturating_add(chunk.len() as u64);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(other) => {
                this.timer = None;
                Poll::Ready(other)
            }
            Poll::Pending => {
                if this.timeout.is_zero() {
                    return Poll::Pending;
                }

                if this.timer.is_none() {
                    this.timer = Some(Box::pin(tokio::time::sleep(this.timeout)));
                }

                if let Some(timer) = this.timer.as_mut()
                    && std::future::Future::poll(timer.as_mut(), cx).is_ready()
                {
                    this.timer = None;
                    this.timed_out = true;
                    let expected_display = this.expected.map(|v| v.to_string()).unwrap_or_else(|| "unknown".to_string());
                    warn!(
                        target: "rustfs::app::object_usecase",
                        event = EVENT_PUT_OBJECT_BODY_READ_STALLED,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        request_id = %this.request_id,
                        bucket = %this.bucket,
                        key = %this.key,
                        received_bytes = this.received,
                        expected_bytes = %expected_display,
                        timeout_secs = this.timeout.as_secs(),
                        state = "stall_timeout",
                        "PutObject request body read stalled; aborting. A proxy/CDN likely forwarded a partial body without closing the connection."
                    );
                    return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "request body read stalled: received {} of {} bytes, no data for {}s",
                            this.received,
                            expected_display,
                            this.timeout.as_secs()
                        ),
                    )) as StdError)));
                }

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ByteStream for RequestBodyReadTimeout {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

/// Wrap an incoming request body with [`RequestBodyReadTimeout`] unless the
/// feature is disabled (`timeout == 0`), in which case the body is returned
/// untouched. `remaining_length` is preserved via [`StreamingBlob::new`].
fn guard_put_object_body_read_timeout(
    body: StreamingBlob,
    bucket: &str,
    key: &str,
    request_id: &str,
    expected: Option<i64>,
    timeout: Duration,
) -> StreamingBlob {
    if timeout.is_zero() {
        return body;
    }

    StreamingBlob::new(RequestBodyReadTimeout {
        inner: body.into(),
        timeout,
        timer: None,
        received: 0,
        expected: expected.and_then(|v| u64::try_from(v).ok()),
        bucket: bucket.to_string(),
        key: key.to_string(),
        request_id: request_id.to_string(),
        timed_out: false,
    })
}

struct PooledBufferReader {
    buffer: PooledBuffer,
    len: usize,
    pos: usize,
}

impl PooledBufferReader {
    fn new(buffer: PooledBuffer, len: usize) -> Self {
        Self { buffer, len, pos: 0 }
    }
}

impl AsyncRead for PooledBufferReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.pos >= self.len {
            return Poll::Ready(Ok(()));
        }

        let remaining = self.len - self.pos;
        let to_read = remaining.min(buf.remaining());
        buf.put_slice(&self.buffer[self.pos..self.pos + to_read]);
        self.pos += to_read;

        Poll::Ready(Ok(()))
    }
}

struct ChunkedBytesReader {
    chunks: Vec<Bytes>,
    chunk_index: usize,
    chunk_offset: usize,
}

impl ChunkedBytesReader {
    fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            chunk_offset: 0,
        }
    }
}

impl AsyncRead for ChunkedBytesReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        while self.chunk_index < self.chunks.len() {
            let chunk = &self.chunks[self.chunk_index];
            if self.chunk_offset >= chunk.len() {
                self.chunk_index += 1;
                self.chunk_offset = 0;
                continue;
            }

            let remaining = &chunk[self.chunk_offset..];
            let to_read = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_read]);
            self.chunk_offset += to_read;
            return Poll::Ready(Ok(()));
        }

        Poll::Ready(Ok(()))
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
fn should_use_zero_copy_eager_put_path(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> bool {
    zero_copy_eager_put_path_status(size, headers, server_side_encryption_requested, should_compress, is_extract)
        == PUT_EAGER_STATUS_ELIGIBLE
}

fn zero_copy_eager_put_path_status(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> &'static str {
    zero_copy_eager_put_path_status_with_max_size(
        size,
        headers,
        server_side_encryption_requested,
        should_compress,
        is_extract,
        zero_copy_eager_put_max_size_bytes(),
    )
}

fn zero_copy_eager_put_path_status_with_max_size(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
    max_size: i64,
) -> &'static str {
    if is_extract {
        return PUT_EAGER_STATUS_EXTRACT;
    }
    if should_compress {
        return PUT_EAGER_STATUS_COMPRESSED;
    }
    if server_side_encryption_requested {
        return PUT_EAGER_STATUS_ENCRYPTED;
    }

    if size <= 0 {
        return PUT_EAGER_STATUS_INVALID_SIZE;
    }
    if size > max_size {
        return PUT_EAGER_STATUS_ABOVE_EAGER_MAX;
    }

    if !should_use_zero_copy(size, headers) {
        return PUT_EAGER_STATUS_ZERO_COPY_INELIGIBLE;
    }

    if request_uses_aws_chunked(headers) && decoded_content_length_from_headers(headers).ok().flatten().is_none() {
        return PUT_EAGER_STATUS_AWS_CHUNKED_MISSING_DECODED_LENGTH;
    }

    PUT_EAGER_STATUS_ELIGIBLE
}

fn zero_copy_eager_put_max_size_bytes() -> i64 {
    let configured = *CACHED_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES.get_or_init(|| {
        rustfs_utils::get_env_usize(ENV_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES, DEFAULT_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES)
    });
    i64::try_from(configured).unwrap_or(i64::MAX)
}

fn should_use_small_eager_put_path(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> bool {
    const SMALL_EAGER_PUT_MAX_SIZE: i64 = 1024 * 1024;

    if is_extract || should_compress || server_side_encryption_requested {
        return false;
    }

    if size <= 0 || size > SMALL_EAGER_PUT_MAX_SIZE {
        return false;
    }

    if has_put_sse_request_headers(headers) {
        return false;
    }

    if request_uses_aws_chunked(headers) && decoded_content_length_from_headers(headers).ok().flatten().is_none() {
        return false;
    }

    true
}

/// Objects at or below this size bypass BytesPool and use direct allocation.
/// This avoids Small-tier Mutex contention under high concurrency for tiny objects
/// where the allocation cost is negligible (≤4KiB memcpy).
const POOL_BYPASS_MAX_SIZE: usize = 4 * 1024;

async fn read_small_put_body_into<R, B>(body: &mut R, buf: &mut B, size: usize) -> S3Result<()>
where
    R: AsyncRead + Unpin,
    B: bytes::BufMut,
{
    let mut filled = 0;

    while filled < size {
        let mut remaining = (&mut *buf).limit(size - filled);
        let read = tokio::io::AsyncReadExt::read_buf(&mut *body, &mut remaining)
            .await
            .map_err(ApiError::from)?;
        if read == 0 {
            return Err(s3_error!(IncompleteBody));
        }
        filled += read;
    }

    let mut extra = [0u8; 1];
    let extra_read = tokio::io::AsyncReadExt::read(&mut *body, &mut extra)
        .await
        .map_err(ApiError::from)?;
    if extra_read != 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(())
}

async fn read_small_put_body_exact_pooled<R>(mut body: R, size: usize, pool: &BytesPool) -> S3Result<PooledBuffer>
where
    R: AsyncRead + Unpin,
{
    let mut buf = pool.acquire_buffer(size).await;
    read_small_put_body_into(&mut body, &mut *buf, size).await?;
    Ok(buf)
}

/// Read small PUT body into a directly-allocated buffer, bypassing BytesPool.
/// Used for objects ≤4KiB where pool contention under high concurrency
/// outweighs the allocation cost.
async fn read_small_put_body_exact_direct<R>(mut body: R, size: usize) -> S3Result<std::io::Cursor<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(size);
    read_small_put_body_into(&mut body, &mut buf, size).await?;
    Ok(std::io::Cursor::new(buf))
}

async fn read_zero_copy_put_body_exact<S, E>(mut body: S, size: usize) -> S3Result<ChunkedBytesReader>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: Into<StdError>,
{
    let mut chunks = Vec::new();
    let mut filled = 0usize;

    while filled < size {
        let Some(chunk) = body.next().await else {
            return Err(s3_error!(IncompleteBody));
        };
        let chunk = chunk.map_err(|err| ApiError::from(s3s_body_error_to_io(err.into())))?;
        if chunk.is_empty() {
            continue;
        }
        if filled.saturating_add(chunk.len()) > size {
            return Err(s3_error!(UnexpectedContent));
        }

        rustfs_io_metrics::record_zero_copy_buffer_operation("put_chunk", chunk.len());
        filled += chunk.len();
        chunks.push(chunk);
    }

    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|err| ApiError::from(s3s_body_error_to_io(err.into())))?;
        if !chunk.is_empty() {
            return Err(s3_error!(UnexpectedContent));
        }
    }

    Ok(ChunkedBytesReader::new(chunks))
}

#[derive(Default)]
struct PutObjectChecksums {
    pub(super) crc32: Option<String>,
    pub(super) crc32c: Option<String>,
    pub(super) sha1: Option<String>,
    pub(super) sha256: Option<String>,
    pub(super) crc64nvme: Option<String>,
}

struct PutObjectCommitResult {
    obj_info: ObjectInfo,
    put_versioned: bool,
}

struct EagerPutCommitOwner<T: Send + 'static> {
    task: Option<tokio::task::JoinHandle<T>>,
    cancellation: tokio_util::sync::CancellationToken,
    cancellation_grace: Duration,
}

impl<T: Send + 'static> EagerPutCommitOwner<T> {
    fn new(
        task: tokio::task::JoinHandle<T>,
        cancellation: tokio_util::sync::CancellationToken,
        cancellation_grace: Duration,
    ) -> Self {
        Self {
            task: Some(task),
            cancellation,
            cancellation_grace,
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self.task.as_mut().expect("eager PUT commit owner task must be present").await;
        self.task = None;
        result
    }
}

impl<T: Send + 'static> Drop for EagerPutCommitOwner<T> {
    fn drop(&mut self) {
        let Some(mut task) = self.task.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            task.abort();
            return;
        }
        let cancellation = self.cancellation.clone();
        let cancellation_grace = self.cancellation_grace;
        spawn_traced(async move {
            if tokio::time::timeout(cancellation_grace, &mut task).await.is_err() {
                cancellation.cancel();
                metrics::counter!("rustfs_put_commit_owner_deadline_total", "put_path" => "eager").increment(1);
                warn!(
                    target: "rustfs::app::object_usecase",
                    event = EVENT_PUT_OBJECT_COMMIT_OWNER_DEADLINE,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    state = "cancellation_requested",
                    cancellation_grace_ms = cancellation_grace.as_millis() as u64,
                    "cancelled eager PutObject commit owner exceeded its grace period and requested storage cleanup"
                );
                let _ = task.await;
            }
        });
    }
}

fn successful_delete_audit_objects(
    delete: &s3s::dto::Delete,
    successful_results: impl IntoIterator<Item = bool>,
) -> Vec<AuditObjectVersion> {
    delete
        .objects
        .iter()
        .zip(successful_results)
        .filter(|(_, successful)| *successful)
        .map(|(requested, _)| AuditObjectVersion::new(requested.key.clone(), requested.version_id.clone()))
        .collect()
}

fn normalize_delete_objects_version_id(
    version_id: Option<String>,
) -> std::result::Result<(Option<String>, Option<Uuid>), String> {
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

#[cfg(test)]
type DeleteSnapshotTestHook = (String, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);
#[cfg(test)]
type PutPostStoreTestHook = (String, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

#[cfg(test)]
static DELETE_SNAPSHOT_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();
#[cfg(test)]
static DELETE_SOURCE_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();
#[cfg(test)]
static DELETE_OBJECTS_AUTH_TEST_HOOK: OnceLock<Mutex<Option<DeleteSnapshotTestHook>>> = OnceLock::new();
#[cfg(test)]
static PUT_POST_STORE_TEST_HOOK: OnceLock<Mutex<Option<PutPostStoreTestHook>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn install_delete_snapshot_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_SNAPSHOT_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete snapshot test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_snapshot_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_SNAPSHOT_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete snapshot test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
pub(crate) fn install_delete_source_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_SOURCE_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete source test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_source_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_SOURCE_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete source test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
pub(crate) fn install_delete_objects_auth_test_hook(
    bucket: String,
    loaded: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
) {
    *DELETE_OBJECTS_AUTH_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("delete objects auth test hook lock should not be poisoned") = Some((bucket, loaded, resume));
}

#[cfg(test)]
async fn wait_for_delete_objects_auth_test_hook(bucket: &str) {
    let hook = {
        let mut slot = DELETE_OBJECTS_AUTH_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("delete objects auth test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, loaded, resume)) = hook {
        loaded.wait().await;
        resume.wait().await;
    }
}

#[cfg(test)]
fn install_put_post_store_test_hook(bucket: String, entered: Arc<tokio::sync::Barrier>, resume: Arc<tokio::sync::Barrier>) {
    *PUT_POST_STORE_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("PUT post-store test hook lock should not be poisoned") = Some((bucket, entered, resume));
}

#[cfg(test)]
async fn wait_for_put_post_store_test_hook(bucket: &str) {
    let hook = {
        let mut slot = PUT_POST_STORE_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("PUT post-store test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, entered, resume)) = hook {
        entered.wait().await;
        resume.wait().await;
    }
}

fn enrich_delete_replication_state_if_needed(
    snapshot: &DeleteReplicationConfigSnapshot,
    delete_object: &mut StorageDeletedObject,
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

    let Some(config) = snapshot.replication_config() else {
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
        config,
        obj_info,
        version_id,
        obj_info.replication_status == ReplicationStatusType::Replica,
    ) {
        set_deleted_object_replication_state(delete_object, &local_state);
    }
}

fn should_schedule_replica_delete_replication(
    snapshot: &DeleteReplicationConfigSnapshot,
    replication_source: &ObjectInfo,
    version_id: Option<Uuid>,
) -> bool {
    let Some(config) = snapshot.replication_config() else {
        return false;
    };

    delete_replication_state_from_config(config, replication_source, version_id, true).is_some()
}

fn validate_undo_delete_version(expected: Option<&str>, requested: Option<&str>) -> S3Result<()> {
    if expected.is_some() && expected != requested {
        return Err(s3_error!(PreconditionFailed));
    }
    Ok(())
}

fn copy_namespace_lock_error(bucket: &str, object: &str, mode: &'static str, err: rustfs_lock::LockError) -> StorageError {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            required,
            achieved,
        },
        other => StorageError::Lock(other),
    }
}

async fn acquire_self_copy_namespace_lock<S>(store: &S, bucket: &str, object: &str) -> S3Result<NamespaceLockGuard>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    let object = encode_dir_object(object);
    let lock = store.new_ns_lock(bucket, &object).await.map_err(ApiError::from)?;
    lock.get_write_lock(get_lock_acquire_timeout())
        .await
        .map_err(|err| ApiError::from(copy_namespace_lock_error(bucket, &object, "write", err)).into())
}

pub(crate) async fn acquire_copy_bucket_lifecycle_lock<S>(store: &S, bucket: &str) -> S3Result<NamespaceLockGuard>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    let lock = store
        .new_ns_lock(bucket, BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .map_err(ApiError::from)?;
    lock.get_read_lock(get_lock_acquire_timeout()).await.map_err(|err| {
        ApiError::from(copy_namespace_lock_error(
            bucket,
            BUCKET_LIFECYCLE_LOCK_OBJECT,
            "bucket_lifecycle_read",
            err,
        ))
        .into()
    })
}

pub(crate) async fn acquire_copy_bucket_lifecycle_locks<S>(
    store: &S,
    source_bucket: &str,
    destination_bucket: &str,
) -> S3Result<(NamespaceLockGuard, Option<NamespaceLockGuard>)>
where
    S: NamespaceLocking<Error = EcstoreError, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + ?Sized,
{
    if source_bucket == destination_bucket {
        return Ok((acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?, None));
    }

    if source_bucket < destination_bucket {
        let source_guard = acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?;
        let destination_guard = acquire_copy_bucket_lifecycle_lock(store, destination_bucket).await?;
        Ok((source_guard, Some(destination_guard)))
    } else {
        let destination_guard = acquire_copy_bucket_lifecycle_lock(store, destination_bucket).await?;
        let source_guard = acquire_copy_bucket_lifecycle_lock(store, source_bucket).await?;
        Ok((source_guard, Some(destination_guard)))
    }
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
    namespace_reserved_user_metadata(metadata);
    apply_standard_object_metadata(
        metadata,
        cache_control.as_deref(),
        content_disposition.as_deref(),
        content_encoding.as_deref(),
        content_language.as_deref(),
        content_type.as_deref(),
        expires.as_ref(),
        website_redirect_location.as_deref(),
    )?;
    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storage_class.as_str().to_string());
    }

    extract_metadata_from_mime_with_object_name(headers, metadata, true, Some(object_name));
    Ok(())
}

fn apply_put_request_object_lock_opts(
    bucket: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    if let Some(eval_metadata) = build_put_like_object_lock_metadata(
        bucket,
        object_lock_config_state,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    )? {
        opts.eval_metadata = Some(eval_metadata);
    }

    Ok(())
}

fn delete_creates_delete_marker(opts: &ObjectOptions) -> bool {
    opts.version_id.is_none() && opts.versioned && !opts.version_suspended
}

fn delete_removes_current_object(opts: &ObjectOptions) -> bool {
    delete_request_targets_current(
        opts.version_id
            .as_deref()
            .and_then(|version_id| Uuid::parse_str(version_id).ok()),
    )
}

fn delete_request_targets_current(version_id: Option<Uuid>) -> bool {
    version_id.is_none() || version_id.is_some_and(|version_id| version_id.is_nil())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteMemoryUpdate {
    DeleteMarker,
    Object { size: u64, removed_current_object: bool },
}

fn delete_memory_update(
    creates_delete_marker: bool,
    committed_delete_marker: bool,
    requested_current: bool,
    accounting_size: Option<u64>,
    removed_current_object: bool,
) -> Option<DeleteMemoryUpdate> {
    if creates_delete_marker || (committed_delete_marker && requested_current) {
        return Some(DeleteMemoryUpdate::DeleteMarker);
    }

    (!committed_delete_marker)
        .then_some(accounting_size)
        .flatten()
        .map(|size| DeleteMemoryUpdate::Object {
            size,
            removed_current_object,
        })
}

async fn apply_delete_memory_update(bucket: &str, update: Option<DeleteMemoryUpdate>) {
    match update {
        Some(DeleteMemoryUpdate::DeleteMarker) => record_bucket_delete_marker_memory(bucket).await,
        Some(DeleteMemoryUpdate::Object {
            size,
            removed_current_object,
        }) => record_bucket_object_delete_memory(bucket, size, removed_current_object).await,
        None => {}
    }
}

/// `DeleteObjects` is idempotent. A raw filesystem `NotFound` can cross the
/// distributed delete path instead of its usual typed missing-object error.
fn is_delete_objects_not_found(error: &EcstoreError) -> bool {
    is_err_object_not_found(error)
        || is_err_version_not_found(error)
        || matches!(error, StorageError::Io(source) if source.kind() == std::io::ErrorKind::NotFound)
}

/// Bounded concurrency for the per-object pre-delete stat fanout in
/// `execute_delete_objects` (backlog#929 / HP-8). Keeps the metadata reads for
/// a 1000-key batch from serializing while capping the disk fanout pressure.
const DELETE_OBJECTS_PRE_STAT_CONCURRENCY: usize = 16;

/// backlog#929 (HP-8): whether the pre-delete `get_object_info` for one entry
/// of a DeleteObjects batch can be skipped without changing behavior.
///
/// The stat result feeds four consumers, and each must be provably idle:
/// - the app-layer object-lock admission check never runs for deletes that
///   create a delete marker, and non-lock buckets cannot hold retention or
///   legal-hold metadata (`bucket_lock_enabled == false`);
/// - replication reads its authoritative source metadata later while the
///   SetDisks write lock is held, so it does not consume this advisory stat;
/// - usage accounting for delete-marker creation goes through
///   `record_bucket_delete_marker_memory` and never reads the object size
///   (`accounting_creates_delete_marker` is computed from the same versioning
///   snapshot the accounting branch uses);
/// - transitioned-object (ILM tier) cleanup journaling is a no-op for
///   delete-marker creation because no version is removed, so `ObjSweeper`
///   produces no journal entry regardless of the stat result.
///
/// Object-lock enabled buckets always keep the stat, so their delete path is
/// byte-for-byte the pre-#929 one (see PR #4297).
fn can_skip_delete_objects_pre_stat(
    bucket_lock_enabled: bool,
    opts: &ObjectOptions,
    accounting_creates_delete_marker: bool,
) -> bool {
    !bucket_lock_enabled && delete_creates_delete_marker(opts) && accounting_creates_delete_marker
}

fn complete_delete_noop(
    helper: OperationHelper,
    bucket: String,
    key: String,
    version_id: Option<String>,
) -> (S3Result<S3Response<DeleteObjectOutput>>, OperationHelper) {
    let helper = helper
        .event_name(EventName::ObjectRemovedNoOP)
        .object(ObjectInfo {
            name: key,
            bucket,
            ..Default::default()
        })
        .version_id(version_id.unwrap_or_default());
    let result = Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
    let helper = helper.complete(&result);
    (result, helper)
}

fn delete_response_version_id(version_id: Option<Uuid>, synthetic_version_id: bool) -> Option<String> {
    if synthetic_version_id {
        None
    } else if version_id == Some(Uuid::nil()) {
        Some(NULL_VERSION_ID.to_string())
    } else {
        version_id.map(|version_id| version_id.to_string())
    }
}

fn reduce_delete_objects_result<'a>(
    object: &ObjectToDelete,
    deleted: &'a StorageDeletedObject,
    error: Option<&EcstoreError>,
    synthetic_version_id: bool,
) -> Result<&'a StorageDeletedObject, s3s::dto::Error> {
    match error {
        None => Ok(deleted),
        Some(error) if is_delete_objects_not_found(error) => Ok(deleted),
        Some(error) => {
            let api_error = ApiError::from(error.clone());
            Err(s3s::dto::Error {
                code: Some(api_error.code.as_str().to_string()),
                key: Some(object.object_name.clone()),
                message: Some(api_error.message),
                version_id: delete_response_version_id(object.version_id, synthetic_version_id),
            })
        }
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

#[derive(Clone, Default)]
pub struct DefaultObjectUsecase {
    context: Option<Arc<AppContext>>,
    #[cfg(test)]
    get_object_timeout_policy: Option<GetObjectTimeoutPolicy>,
}

impl DefaultObjectUsecase {
    fn should_use_large_put_concurrency_tuning(size: i64) -> bool {
        size >= DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES
    }

    #[cfg(test)]
    pub fn without_context() -> Self {
        Self {
            context: None,
            get_object_timeout_policy: None,
        }
    }

    pub fn from_global() -> Self {
        Self {
            context: current_app_context(),
            #[cfg(test)]
            get_object_timeout_policy: None,
        }
    }

    /// Build the use-case bound to an explicit application context
    /// (backlog#1052 S6): the per-server request path passes its own context
    /// so the use-case resolves that server's store; `None` falls back to the
    /// ambient default.
    pub fn with_context(context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>) -> Self {
        Self {
            context,
            #[cfg(test)]
            get_object_timeout_policy: None,
        }
    }

    #[cfg(test)]
    fn with_context_and_get_object_timeout_policy(
        context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>,
        get_object_timeout_policy: GetObjectTimeoutPolicy,
    ) -> Self {
        Self {
            context,
            get_object_timeout_policy: Some(get_object_timeout_policy),
        }
    }

    fn bucket_metadata_sys(&self) -> Option<Arc<RwLock<metadata_sys::BucketMetadataSys>>> {
        self.context.as_ref().and_then(|context| context.bucket_metadata().handle())
    }

    fn object_store(&self) -> Option<Arc<ECStore>> {
        current_object_store_handle_for_context(self.context.as_deref())
    }

    fn object_data_cache(&self) -> Arc<ObjectDataCacheAdapter> {
        current_object_data_cache_for_context(self.context.as_deref())
    }

    fn object_traffic_health(&self) -> Option<Arc<ObjectTrafficHealth>> {
        self.context
            .as_ref()
            .map(|context| context.object_traffic_health())
            .or_else(|| current_app_context().map(|context| context.object_traffic_health()))
    }

    fn base_buffer_size(&self) -> usize {
        self.context
            .clone()
            .or_else(current_app_context)
            .map(|context| context.buffer_config().get().base_config.default_unknown)
            .unwrap_or_else(|| RustFSBufferConfig::default().base_config.default_unknown)
    }

    async fn check_bucket_quota(&self, bucket: &str, op: QuotaOperation, size: u64) -> S3Result<Option<QuotaCheckResult>> {
        let Some(metadata_sys) = self.bucket_metadata_sys() else {
            return Ok(None);
        };
        let quota_checker = QuotaChecker::new(metadata_sys);
        map_quota_check_outcome(bucket, quota_checker.check_quota(bucket, op, size).await).map(Some)
    }

    fn put_object_execution_context(req: &S3Request<PutObjectInput>) -> (EventName, QuotaOperation, &'static str) {
        if req.extensions.get::<PostObjectRequestMarker>().is_some() {
            (put_event_name_for_post_object(true), QuotaOperation::PostObject, "POST")
        } else {
            (put_event_name_for_post_object(false), QuotaOperation::PutObject, "PUT")
        }
    }

    #[instrument(name = "execute_put_object", level = "info", skip(self, _fs, req))]
    pub async fn execute_put_object(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        self.execute_put_object_boxed(_fs, req).await
    }

    fn execute_put_object_boxed<'a>(
        &'a self,
        _fs: &'a FS,
        req: S3Request<PutObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<PutObjectOutput>>> + Send + 'a {
        Box::pin(self.execute_put_object_inner(_fs, req))
    }

    #[hotpath::measure(
        label = "rustfs::app::object_usecase::DefaultObjectUsecase::execute_put_object",
        impl_type = "DefaultObjectUsecase"
    )]
    async fn execute_put_object_inner(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
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
        // An authorized inbound replication PUT must store the replica verbatim.
        // A snowball-extracted member object keeps `x-amz-meta-snowball-auto-extract`
        // in its user metadata, and the replication client replays stored metadata
        // as headers — re-dispatching that PUT into the extract path would try to
        // untar the member's own bytes (failing replication for any non-archive
        // member) instead of writing the replica.
        let inbound_replication_put = replication_request_authorized(&req)
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true");
        if is_put_object_extract_requested(&req.headers) && !inbound_replication_put {
            return Box::pin(self.execute_put_object_extract(req)).await;
        }
        // SSE-C ciphertext passthrough (authorized replication only): the body
        // is already ciphertext and must be stored verbatim — no compression,
        // no bucket-default encryption.
        let ciphertext_passthrough =
            inbound_replication_put && rustfs_utils::http::ssec_transport_to_stored_metadata(&req.headers).is_some();

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
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        // Validate archive content encoding (reject when strict mode is enabled)
        validate_archive_content_encoding(
            &key,
            req.headers.get("content-type").and_then(|value| value.to_str().ok()),
            req.headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        // Guard against a proxy/CDN that forwards a partial body then goes silent
        // without closing the connection: bound the inter-chunk wait so the read
        // fails (with a diagnostic log) instead of hanging forever (issue #3076).
        let body = {
            let request_id = req
                .extensions
                .get::<request_context::RequestContext>()
                .map(|ctx| ctx.request_id.clone())
                .unwrap_or_default();
            guard_put_object_body_read_timeout(body, &bucket, &key, &request_id, content_length, put_object_body_read_timeout())
        };

        // Resolve the authoritative decoded/plain object length (rejecting negative/unknown) before anything else consumes it.
        let mut size = resolve_put_object_authoritative_size(&req.headers, content_length)?;

        // The app check preserves the existing S3 error contract; the storage
        // commit path reserves the exact net logical growth under its locks.
        let quota_check = self
            .check_bucket_quota(
                &bucket,
                quota_operation,
                u64::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
            .await?;
        let quota_enabled = quota_check.as_ref().is_some_and(|result| result.quota_limit.is_some());
        if quota_enabled && ciphertext_passthrough {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "SSE-C ciphertext replication is unavailable for quota-enabled buckets".to_string(),
            ));
        }

        let put_stage_metrics_enabled = rustfs_io_metrics::put_stage_metrics_enabled();
        let ingress_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let should_compress =
            is_disk_compressible(&req.headers, &key) && size > MIN_DISK_COMPRESSIBLE_SIZE as i64 && !ciphertext_passthrough;
        let server_side_encryption_requested =
            server_side_encryption.is_some() || sse_customer_algorithm.is_some() || ssekms_key_id.is_some();

        // Resolve the store through the request-bound server context
        // (backlog#1052 S6), not the process-global handle, so an embedded
        // second server never writes into the first server's store.
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let bucket_validate_stage_start = put_stage_metrics_enabled.then(Instant::now);
        validate_bucket_exists(&store, &bucket).await?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_bucket_validate", bucket_validate_stage_start);

        let put_admission = match get_concurrency_manager()
            .admit_put_object()
            .await
            .map_err(|_| s3_error!(InternalError, "foreground write admission closed"))?
        {
            PutObjectAdmission::Disabled => None,
            PutObjectAdmission::Admitted(permit) => {
                counter!("rustfs.put_object.foreground_admission.total", "result" => "admitted").increment(1);
                Some(permit)
            }
            PutObjectAdmission::Rejected => {
                counter!("rustfs.put_object.foreground_admission.total", "result" => "rejected").increment(1);
                return Err(s3_error!(
                    SlowDown,
                    "foreground write concurrency limit reached, please reduce your request rate"
                ));
            }
        };

        let mut put_request_guard = PutObjectGuard::new();
        let concurrent_put_requests = PutObjectGuard::concurrent_requests();

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        // Concurrency-aware adjustment reduces buffer size under high PUT concurrency to lower memory pressure.
        let base_buffer_size = get_buffer_size_opt_in(size);
        let use_large_put_concurrency_tuning = Self::should_use_large_put_concurrency_tuning(size);
        let buffer_size = if use_large_put_concurrency_tuning {
            get_put_concurrency_aware_buffer_size(size, base_buffer_size)
        } else {
            base_buffer_size
        };

        // Detect zero-copy opportunity before encryption/compression decisions
        // Zero-copy is beneficial for large unencrypted, uncompressed objects
        let enable_zero_copy = should_use_zero_copy(size, &req.headers);

        if enable_zero_copy {
            // Record zero-copy write attempt
            counter!("rustfs_zero_copy_write_attempts_total").increment(1);
            histogram!("rustfs_zero_copy_write_size_bytes").record(size as f64);
            debug!("Zero-copy write enabled for {} byte object (bucket={}, key={})", size, bucket, key);
        }

        let use_empty_or_small_eager_put_path = size == 0
            || should_use_small_eager_put_path(size, &req.headers, server_side_encryption_requested, should_compress, false);
        let zero_copy_eager_put_path_status =
            zero_copy_eager_put_path_status(size, &req.headers, server_side_encryption_requested, should_compress, false);
        let use_zero_copy_eager_put_path = zero_copy_eager_put_path_status == PUT_EAGER_STATUS_ELIGIBLE;
        if use_zero_copy_eager_put_path {
            counter!(buffered_write::ATTEMPTS_TOTAL).increment(1);
            histogram!(buffered_write::ATTEMPT_SIZE_BYTES).record(size as f64);
        }
        let put_path = if should_compress {
            "stream_compressed"
        } else if use_zero_copy_eager_put_path {
            "zero_copy_eager"
        } else if use_empty_or_small_eager_put_path {
            "small_eager"
        } else {
            "streaming"
        };
        rustfs_io_metrics::record_put_object_diagnostics(
            put_path,
            zero_copy_eager_put_path_status,
            size,
            buffer_size,
            use_large_put_concurrency_tuning,
        );

        let sse_config_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        rustfs_io_metrics::record_put_object_stage_duration_from("app_sse_config_lookup", sse_config_stage_start);
        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            event = "bucket_sse_config_lookup",
            bucket = %bucket,
            found = bucket_sse_config.is_some(),
            "Bucket SSE configuration lookup completed"
        );

        let original_sse = server_side_encryption.clone();
        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _timestamp)| config),
            server_side_encryption,
            ssekms_key_id,
            false,
        );
        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            event = "effective_sse_resolved",
            bucket = %bucket,
            requested = ?original_sse,
            effective = ?effective_sse,
            "Resolved effective SSE configuration"
        );

        if ciphertext_passthrough {
            // The replica keeps the source's SSE-C metadata; the bucket
            // default must not claim managed encryption on it.
            effective_sse = None;
            effective_kms_key_id = None;
        }

        // Validate SSE-C headers early: reject partial/invalid combinations per S3 spec
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            extract_ssekms_context_from_headers(&req.headers)?.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true, // PutObject requires all three: algorithm, key, key_md5
        )?;

        let mut metadata = metadata.unwrap_or_default();
        let has_explicit_object_lock_retention = object_lock_mode.is_some()
            || object_lock_retain_until_date.is_some()
            || has_replication_retention_update(&req.headers, inbound_replication_put);
        let object_lock_config_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_object_lock_config_lookup", object_lock_config_stage_start);
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
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut metadata,
            has_explicit_object_lock_retention,
        )?;

        let put_opts_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let mut opts: ObjectOptions = put_opts_with_replication_authorization(
            &bucket,
            &key,
            version_id.clone(),
            &req.headers,
            metadata.clone(),
            replication_request_authorized(&req),
        )
        .await
        .map_err(ApiError::from)?;
        if let Some(quota_check) = quota_check.as_ref() {
            apply_quota_admission(&mut opts, quota_check)?;
        }
        rustfs_io_metrics::record_put_object_stage_duration_from("app_put_opts_build", put_opts_stage_start);
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        apply_put_request_object_lock_opts(
            &bucket,
            &object_lock_config_state,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            &mut opts,
        )?;
        let eager_put_commit_cancellation =
            (use_zero_copy_eager_put_path || use_empty_or_small_eager_put_path).then(tokio_util::sync::CancellationToken::new);
        opts.put_object_cancellation = eager_put_commit_cancellation.clone();

        // rustfs/backlog#1009: the pre-PUT lookup has exactly two consumers —
        // the existing-object WORM validation and usage accounting's
        // previous_current_size. When the bucket has no object locking (WORM is
        // a provable no-op; the gate fails closed on metadata errors) and the
        // PUT targets the latest version (no explicit version_id from internal
        // replication), the lookup is skipped and accounting is backfilled from
        // the dst xl.meta that rename_data already reads, saving a full-disk
        // metadata fanout per PUT.
        let prelookup_required = version_id.is_some() || object_lock_checks_required_for_state(&object_lock_config_state);
        // Outer None = prelookup skipped (accounting comes from the commit
        // backfill); Some(inner) = the previous current size as observed by the
        // lookup, with the pre-#1009 semantics kept bit-for-bit.
        let prelookup_stage_start = (prelookup_required && put_stage_metrics_enabled).then(Instant::now);
        let prelookup_previous_current_size: Option<Option<u64>> = if prelookup_required {
            let current_opts: ObjectOptions = internal_object_info_lookup_opts(
                get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
                    .await
                    .map_err(ApiError::from)?,
            );
            let previous_current_info = {
                crate::hp_guard!("S3::put_object_prelookup");
                store.get_object_info(&bucket, &key, &current_opts).await
            };
            Some(match previous_current_info {
                Ok(existing_obj_info) => {
                    validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &opts)?;
                    Some(if quota_enabled {
                        quota_object_size(&existing_obj_info).map_err(ApiError::from)?
                    } else {
                        existing_obj_info.size.max(0) as u64
                    })
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(ApiError::from(err).into());
                    }
                    None
                }
            })
        } else {
            None
        };
        rustfs_io_metrics::record_put_object_stage_duration_from("app_prelookup", prelookup_stage_start);

        let actual_size = size;
        if !ciphertext_passthrough && let Some(quota_check) = quota_check.as_ref() {
            ensure_object_size_within_quota(
                quota_check,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )?;
        }

        let mut md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256_with_query(&req.headers, req.uri.query());

        let mut write_plan = WritePlan::new();
        // Additional-checksum (XXHash3/64/128, SHA-512) values to echo on the PutObject
        // response (#1256); captured at want_checksum set points before opts is moved.
        let mut put_extra_checksum_headers: Vec<(&'static str, String)> = Vec::new();
        let mut reader = if should_compress {
            let body = tokio::io::BufReader::with_capacity(
                buffer_size,
                StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
            );
            let algorithm = CompressionAlgorithm::default();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

            let mut hrd =
                HashReader::from_stream(body, size, size, md5hex.take(), sha256hex.take(), false).map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = hrd.checksum();
            put_extra_checksum_headers = additional_checksum_echo_pairs(&opts.want_checksum);
            insert_str(&mut opts.user_defined, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
            insert_str(&mut opts.user_defined, SUFFIX_ACTUAL_SIZE, size.to_string());

            size = HashReader::SIZE_PRESERVE_LAYER;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            if use_zero_copy_eager_put_path {
                let zero_copy_start = std::time::Instant::now();
                let eager_body = read_zero_copy_put_body_exact(body, actual_size as usize).await?;
                rustfs_io_metrics::record_zero_copy_write(actual_size as usize, zero_copy_start.elapsed().as_secs_f64() * 1000.0);
                HashReader::from_stream(eager_body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
            } else if use_empty_or_small_eager_put_path {
                if (actual_size as usize) <= POOL_BYPASS_MAX_SIZE {
                    // Bypass BytesPool for very small objects to avoid Small-tier
                    // Mutex contention under high concurrency. Direct allocation
                    // for ≤4KiB is negligible cost.
                    let eager_body = read_small_put_body_exact_direct(
                        StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                        actual_size as usize,
                    )
                    .await?;
                    HashReader::from_stream(eager_body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
                } else {
                    let pool = get_concurrency_manager().bytes_pool();
                    let eager_body = read_small_put_body_exact_pooled(
                        StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                        actual_size as usize,
                        pool.as_ref(),
                    )
                    .await?;
                    let eager_reader = PooledBufferReader::new(eager_body, actual_size as usize);
                    HashReader::from_stream(eager_reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
                }
            } else {
                let body = tokio::io::BufReader::with_capacity(
                    buffer_size,
                    StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                );
                HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
            }
        };

        if size >= 0 {
            if let Err(err) = reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = reader.checksum();
            put_extra_checksum_headers = additional_checksum_echo_pairs(&opts.want_checksum);
        }
        rustfs_io_metrics::record_put_object_path(put_path);
        rustfs_io_metrics::record_put_object_stage_duration_from("ingress_prepare", ingress_stage_start);

        let mut helper = OperationHelper::new(&req, event_name, S3Operation::PutObject);
        let ssekms_context = extract_ssekms_context_from_headers(&req.headers)?;

        // Apply encryption using unified SSE API.
        let encryption_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let write_principal = SseKmsPrincipal::from_request(&req);
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            ssekms_context,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            principal: write_principal.as_ref(),
        };

        // SSE-C ciphertext passthrough must skip sse_encryption entirely: an
        // explicit guard is required because prepare_sse_configuration inside
        // it falls back to the bucket default encryption config and would
        // double-encrypt the already-encrypted body.
        let encryption_material = if opts.preserve_ciphertext {
            None
        } else {
            match sse_encryption(encryption_request).await {
                Ok(material) => material,
                Err(err) => {
                    let result = Err(err.into());
                    let _ = helper.complete(&result);
                    return result;
                }
            }
        };

        if let Some(material) = encryption_material {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            write_plan = write_plan.with_encryption(material.write_encryption(None));

            let encryption_metadata = encryption_material_to_metadata(&material)?;
            metadata.extend(encryption_metadata.clone());
            opts.user_defined.extend(encryption_metadata);
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_encryption_prepare", encryption_stage_start);

        let reader = PutObjReader::new(reader);

        let mt2 = metadata.clone();
        opts.user_defined.extend(metadata);
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let request_id = request_context
            .as_ref()
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| request_context::RequestContext::fallback().request_id);

        // Compute the replication decision exactly once per PUT. The same
        // immutable `dsc` drives both the pending metadata written below and the
        // post-commit schedule (see the reuse site further down), so a
        // replication-config hot update can no longer split the two phases
        // (https://github.com/rustfs/backlog/issues/1320).
        let replication_decision_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let dsc =
            must_replicate_object(&bucket, &key, &mt2, "".to_string(), opts.delete_marker_replication_status(), opts.clone())
                .await;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_replication_decision", replication_decision_stage_start);

        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let cache_adapter = self.object_data_cache();
        let cache_invalidate_before_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;
        rustfs_io_metrics::record_put_object_stage_duration_from(
            "app_cache_invalidate_before",
            cache_invalidate_before_stage_start,
        );

        let store_put_watchdog = tokio_util::sync::CancellationToken::new();
        spawn_traced({
            let store_put_watchdog = store_put_watchdog.clone();
            let request_id = request_id.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let put_path = put_path.to_string();
            async move {
                tokio::select! {
                    _ = store_put_watchdog.cancelled() => {}
                    _ = tokio::time::sleep(PUT_OBJECT_STORE_WARN_THRESHOLD) => {
                        warn!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_INFLIGHT_SLOW,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            threshold_ms = PUT_OBJECT_STORE_WARN_THRESHOLD.as_millis() as u64,
                            state = "store_put_pending",
                            "PutObject store write remains in flight"
                        );
                    }
                }
            }
        });

        let object_traffic_health = if use_zero_copy_eager_put_path || use_empty_or_small_eager_put_path {
            self.object_traffic_health()
        } else {
            None
        };
        let put_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let key = key.clone();
            let opts = opts.clone();
            let cache_adapter = cache_adapter.clone();
            let request_id = request_id.clone();
            let put_path = put_path.to_string();
            let put_admission = put_admission;
            async move {
                let _put_admission = put_admission;
                let object_traffic_progress = object_traffic_health
                    .as_deref()
                    .and_then(ObjectTrafficHealth::track_write_storage);
                let mut reader = reader;
                let store_put_stage_start = put_stage_metrics_enabled.then(Instant::now);
                let (obj_info, backfilled_old_current_size) = match store
                    .put_object_with_old_current_size(&bucket, &key, &mut reader, &opts)
                    .await
                    .map_err(ApiError::from)
                {
                    Ok(obj_info) => {
                        store_put_watchdog.cancel();
                        debug!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_RETURNED,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            duration_ms = start_time.elapsed().as_millis() as u64,
                            result = "success",
                            "PutObject store write returned"
                        );
                        obj_info
                    }
                    Err(err) => {
                        store_put_watchdog.cancel();
                        rustfs_io_metrics::record_put_object_stage_duration_from("app_store_put", store_put_stage_start);
                        warn!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_RETURNED,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            duration_ms = start_time.elapsed().as_millis() as u64,
                            result = "error",
                            error = %err,
                            "PutObject store write returned"
                        );
                        return Err(err.into());
                    }
                };
                rustfs_io_metrics::record_put_object_stage_duration_from("app_store_put", store_put_stage_start);
                drop(_put_admission);
                drop(object_traffic_progress);
                #[cfg(test)]
                wait_for_put_post_store_test_hook(&bucket).await;

                let post_store_stage_start = put_stage_metrics_enabled.then(Instant::now);
                maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;
                let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &key).await;

                let put_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
                // Fast in-memory update for immediate quota and admin usage consistency.
                // The previous current size comes from the prelookup when it ran,
                // otherwise from the rename_data backfill (rustfs/backlog#1009); the
                // backfill reproduces the lookup's observation bit for bit (latest
                // version's ObjectInfo.size — 0 for a delete-marker latest — or
                // not-found → None).
                let committed_size = quota_accounting_object_size(&obj_info, quota_enabled)?;
                match prelookup_previous_current_size.or_else(|| previous_current_size_from_backfill(backfilled_old_current_size))
                {
                    Some(previous_current_size) => {
                        if put_versioned {
                            record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                        } else {
                            record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                        }
                    }
                    None => {
                        // Neither source could determine the previous state (peers
                        // predating the backfill field during a rolling upgrade, or
                        // sub-quorum metadata divergence). Record the components that
                        // are correct regardless; the next authoritative scanner
                        // refresh replaces the in-memory numbers.
                        debug!(
                            target: "rustfs::app::object_usecase",
                            bucket = %bucket,
                            key = %key,
                            put_versioned,
                            "put_object old-size backfill unknown; recording degraded usage delta"
                        );
                        record_bucket_object_write_unknown_previous_memory(&bucket, committed_size, put_versioned).await;
                    }
                }

                if dsc.replicate_any() {
                    schedule_object_replication(obj_info.clone(), store, dsc).await;
                }

                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                rustfs_io_metrics::record_put_object_stage_duration_from("app_post_store_bookkeeping", post_store_stage_start);

                let capacity_update_stage_start = put_stage_metrics_enabled.then(Instant::now);
                let manager = get_capacity_manager();
                manager.record_write_operation().await;
                rustfs_io_metrics::record_put_object_stage_duration_from("app_capacity_update", capacity_update_stage_start);

                Ok::<_, S3Error>(PutObjectCommitResult { obj_info, put_versioned })
            }
        });
        let put_commit_result = if let Some(cancellation) = eager_put_commit_cancellation {
            EagerPutCommitOwner::new(put_commit, cancellation, EAGER_PUT_COMMIT_CANCELLATION_GRACE)
                .join()
                .await
        } else {
            put_commit.await
        };
        let PutObjectCommitResult { obj_info, put_versioned } = match put_commit_result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                let result: S3Result<S3Response<PutObjectOutput>> = Err(err);
                put_request_guard.finish_err();
                let _ = helper.complete(&result);
                return result;
            }
            Err(err) => {
                let result: S3Result<S3Response<PutObjectOutput>> = Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("put object commit owner task failed: {err}"),
                ));
                put_request_guard.finish_err();
                let _ = helper.complete(&result);
                return result;
            }
        };

        let raw_version = obj_info.version_id.map(|v| v.to_string());

        helper = helper.object(obj_info.clone());
        if let Some(version_id) = &raw_version {
            helper = helper.version_id(version_id.clone());
        }

        let put_version = if put_versioned { raw_version } else { None };

        let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let expiration = resolve_put_object_expiration(&bucket, &obj_info).await;

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

        let mut response = S3Response::new(output);
        // Echo XXHash3/64/128 / SHA-512 checksums that s3s PutObjectOutput has no typed
        // field for (#1256).
        inject_additional_checksum_headers(&mut response.headers, &put_extra_checksum_headers);
        let result = Ok(response);
        let _ = helper.complete(&result);

        // Record PutObject metrics via zero-copy-metrics
        {
            let duration_ms = start_time.elapsed().as_millis() as f64;
            rustfs_io_metrics::record_put_object(
                duration_ms,
                size,
                enable_zero_copy, // Track if zero-copy was enabled
            );
        }

        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            bucket = %bucket,
            key = %key,
            concurrent_put_requests,
            buffer_size,
            "PutObject request completed"
        );

        put_request_guard.finish_ok();

        result
    }

    /// Serve a HEAD whose local lookup failed with not-found by proxying to
    /// the bucket's replication targets (MinIO `proxyHeadToRepTarget`).
    async fn proxy_head_object_to_replication_targets(
        req: &S3Request<HeadObjectInput>,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Option<HeadObjectOutput> {
        let targets = get_read_proxy_targets(bucket, key, opts).await;
        if targets.is_empty() {
            return None;
        }
        let extra_headers = Self::proxy_read_passthrough_headers(&req.headers);
        let range = req
            .headers
            .get(http::header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let part_number = req.input.part_number;

        for target in targets {
            match target
                .head_object_for_proxy(
                    &target.bucket,
                    key,
                    opts.version_id.clone(),
                    range.clone(),
                    part_number,
                    extra_headers.clone(),
                )
                .await
            {
                Ok(remote) => {
                    // MinIO-aligned accounting: one total per proxy attempt,
                    // one failed when no target served it.
                    record_replication_proxy(bucket, "HeadObject", false).await;
                    return Some(Self::proxy_sdk_head_output_to_s3s(remote));
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, key, arn = %target.arn, "read proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, key, arn = %target.arn, error = %err, "read proxy: HEAD against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "HeadObject", true).await;
        None
    }

    /// Translate a proxied SDK HEAD response into the s3s output.
    ///
    /// Known gaps: the SDK's HeadObjectOutput does not model 206/Content-Range
    /// for a ranged HEAD (the SDK exposes no content_range member on HEAD),
    /// and s3s' typed HeadObjectOutput has no tag_count field (the local path
    /// injects x-amz-tagging-count as a raw header) — both are dropped for
    /// proxied HEADs.
    fn proxy_sdk_head_output_to_s3s(remote: aws_sdk_s3::operation::head_object::HeadObjectOutput) -> HeadObjectOutput {
        HeadObjectOutput {
            content_length: remote.content_length,
            content_type: remote.content_type.as_deref().and_then(|v| ContentType::from_str(v).ok()),
            content_encoding: remote.content_encoding,
            content_disposition: remote.content_disposition,
            content_language: remote.content_language,
            cache_control: remote.cache_control,
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
            e_tag: remote.e_tag.as_deref().and_then(|v| ETag::from_str(v).ok()),
            last_modified: remote
                .last_modified
                .and_then(|dt| OffsetDateTime::from_unix_timestamp_nanos(dt.as_nanos()).ok())
                .map(Timestamp::from),
            metadata: remote.metadata,
            version_id: remote.version_id,
            server_side_encryption: remote
                .server_side_encryption
                .map(|sse| ServerSideEncryption::from(sse.as_str().to_string())),
            sse_customer_algorithm: remote.sse_customer_algorithm,
            sse_customer_key_md5: remote.sse_customer_key_md5,
            ssekms_key_id: remote.ssekms_key_id,
            parts_count: remote.parts_count,
            storage_class: remote.storage_class.map(|sc| StorageClass::from(sc.as_str().to_string())),
            expiration: remote.expiration,
            restore: remote.restore,
            checksum_crc32: remote.checksum_crc32,
            checksum_crc32c: remote.checksum_crc32_c,
            checksum_crc64nvme: remote.checksum_crc64_nvme,
            checksum_sha1: remote.checksum_sha1,
            checksum_sha256: remote.checksum_sha256,
            checksum_type: remote.checksum_type.map(|ct| ChecksumType::from(ct.as_str().to_string())),
            ..Default::default()
        }
    }

    #[hotpath::measure(
        label = "rustfs::app::object_usecase::DefaultObjectUsecase::execute_get_object",
        impl_type = "DefaultObjectUsecase"
    )]

    pub fn execute_copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<CopyObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_copy_object_inner(req))
    }

    #[instrument(name = "execute_copy_object", level = "debug", skip(self, req))]
    async fn execute_copy_object_inner(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
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
            copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            metadata_directive,
            metadata,
            tagging,
            tagging_directive,
            copy_source_if_match,
            copy_source_if_none_match,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            website_redirect_location,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            storage_class,
            checksum_algorithm,
            ..
        } = req.input.clone();
        let requested_checksum_type = checksum_algorithm
            .as_ref()
            .map(|algorithm| rustfs_rio::ChecksumType::from_string(algorithm.as_str()));
        if requested_checksum_type.is_some_and(|checksum_type| !checksum_type.is_set()) {
            return Err(s3_error!(InvalidArgument, "Unsupported checksum algorithm"));
        }
        let (src_bucket, src_key, version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Normalize the copy-source version id like GET/HEAD do: trim, treat "null" as the
        // nil UUID, and reject malformed ids up front (issue #4238).
        let version_id = match version_id {
            Some(v) => {
                let trimmed = v.trim();
                if trimmed.eq_ignore_ascii_case("null") {
                    Some(Uuid::nil().to_string())
                } else if Uuid::parse_str(trimmed).is_ok() {
                    Some(trimmed.to_string())
                } else {
                    return Err(s3_error!(InvalidArgument, "Invalid version id specified in copy source"));
                }
            }
            None => None,
        };

        if let Some(ref sc) = storage_class
            && !is_valid_storage_class(sc.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        let ssekms_context = extract_ssekms_context_from_headers(&req.headers)?;
        validate_sse_headers_for_write(
            requested_sse.as_ref(),
            requested_kms_key_id.as_ref(),
            ssekms_context.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;
        let has_explicit_ssec = sse_customer_algorithm.is_some() || sse_customer_key.is_some() || sse_customer_key_md5.is_some();

        // Validate both source and destination keys
        validate_object_key(&src_key, "COPY (source)")?;
        validate_object_key(&key, "COPY (dest)")?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;
        let replaces_metadata = match metadata_directive.as_ref().map(|directive| directive.as_str()) {
            None | Some(MetadataDirective::COPY) => false,
            Some(MetadataDirective::REPLACE) => true,
            Some(_) => {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "The MetadataDirective header is invalid".to_string(),
                ));
            }
        };
        let replacement_metadata = if replaces_metadata {
            validate_archive_content_encoding(&key, content_type.as_deref(), content_encoding.as_deref())?;
            let mut replacement_metadata = metadata.unwrap_or_default();
            namespace_reserved_user_metadata(&mut replacement_metadata);
            apply_standard_object_metadata(
                &mut replacement_metadata,
                cache_control.as_deref(),
                content_disposition.as_deref(),
                content_encoding.as_deref(),
                content_language.as_deref(),
                content_type.as_deref(),
                expires.as_ref(),
                website_redirect_location.as_deref(),
            )?;
            Some(replacement_metadata)
        } else {
            None
        };

        // AWS S3 allows self-copy when metadata directive is REPLACE (used to update metadata in-place),
        // when an explicit storage class change is requested, or when restoring a specific historical
        // version onto the current key (source carries a versionId). Reject only a true no-op self-copy
        // where none of these apply (issue #4238).
        let replacement_tags = crate::app::storage_api::object_usecase::s3_api::tagging::resolve_copy_object_tags(
            tagging.as_deref(),
            tagging_directive.as_ref(),
        )?;

        if !replaces_metadata
            && tagging_directive.as_ref().map(TaggingDirective::as_str) != Some(TaggingDirective::REPLACE)
            && storage_class.is_none()
            && version_id.is_none()
            && src_bucket == bucket
            && src_key == key
        {
            error!(bucket, key, "Rejected self-copy operation");
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
        apply_copy_source_bucket_generation_guard(&req, &src_bucket, &mut src_get_opts)?;

        let mut dst_opts = copy_dst_opts_with_replication_authorization(
            &bucket,
            &key,
            dest_version_id.clone(),
            &req.headers,
            HashMap::new(),
            replication_request_authorized(&req),
        )
        .await
        .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut dst_opts)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);
        let expected_current_version_id = expected_current_version_id(&req.headers)?;
        if expected_current_version_id.is_some()
            && (!cp_src_dst_same || version_id.is_none() || dest_version_id.is_some() || !dst_opts.versioned)
        {
            return Err(s3_error!(
                InvalidRequest,
                "Expected current version precondition requires a versioned same-object historical copy that creates a new version"
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let (source_bucket_lifecycle_guard, destination_bucket_lifecycle_guard_storage) =
            acquire_copy_bucket_lifecycle_locks(store.as_ref(), &src_bucket, &bucket).await?;
        let current_source_incarnation_id = store
            .bucket_incarnation_id_from_disk(&src_bucket)
            .await
            .map_err(ApiError::from)?;
        if src_get_opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != current_source_incarnation_id)
        {
            return Err(ApiError::from(StorageError::BucketNotFound(src_bucket.clone())).into());
        }
        let current_destination_incarnation_id = if src_bucket == bucket {
            current_source_incarnation_id
        } else {
            store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?
        };
        if dst_opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != current_destination_incarnation_id)
        {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let destination_bucket_lifecycle_guard = destination_bucket_lifecycle_guard_storage
            .as_ref()
            .unwrap_or(&source_bucket_lifecycle_guard);
        if source_bucket_lifecycle_guard.is_lock_lost() || destination_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_bucket_generation",
                bucket: bucket.clone(),
                object: key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }
        src_get_opts.expected_bucket_incarnation_id = Some(current_source_incarnation_id);
        dst_opts.expected_bucket_incarnation_id = Some(current_destination_incarnation_id);
        if src_bucket != bucket {
            dst_opts.add_bucket_lifecycle_lock_guard(&source_bucket_lifecycle_guard);
        }
        dst_opts.add_bucket_lifecycle_lock_guard(destination_bucket_lifecycle_guard);

        // Bucket metadata uses the bucket name as its namespace-lock key. Load
        // every copy-time bucket snapshot before a same-object key can collide
        // with that key (for example, copying `bucket/bucket` onto itself).
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        if cp_src_dst_same && key == bucket && expected_current_version_id.is_none() {
            dst_opts.object_lock_config_snapshot =
                Some(store.object_lock_config_snapshot(&bucket).await.map_err(ApiError::from)?);
        }
        let mut current_opts: ObjectOptions = internal_object_info_lookup_opts(
            get_opts(&bucket, &key, dest_version_id.clone(), None, &req.headers)
                .await
                .map_err(ApiError::from)?,
        );

        let _self_copy_lock_guard = if cp_src_dst_same && expected_current_version_id.is_none() {
            let guard = acquire_self_copy_namespace_lock(store.as_ref(), &bucket, &key).await?;
            src_opts.no_lock = true;
            src_get_opts.no_lock = true;
            dst_opts.no_lock = true;
            Some(guard)
        } else {
            None
        };
        if let Some(guard) = _self_copy_lock_guard.as_ref() {
            dst_opts.add_namespace_lock_guard(guard);
        }
        dst_opts.expected_current_version_id = expected_current_version_id.clone();

        if _self_copy_lock_guard.is_some() {
            current_opts.no_lock = true;
        }
        let previous_current_sizes = match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &dst_opts)?;
                if let Some(expected) = expected_current_version_id.as_deref()
                    && existing_obj_info.version_id.unwrap_or_default().to_string() != expected
                {
                    return Err(s3_error!(PreconditionFailed));
                }
                Some((existing_obj_info.size.max(0) as u64, quota_object_size(&existing_obj_info)))
            }
            Err(err) => {
                if expected_current_version_id.is_some() {
                    return Err(s3_error!(PreconditionFailed));
                }
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };

        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _)| config),
            requested_sse,
            requested_kms_key_id,
            has_explicit_ssec,
        );

        let h = build_ssec_read_headers(
            copy_source_sse_customer_algorithm.as_ref(),
            copy_source_sse_customer_key.as_ref(),
            copy_source_sse_customer_key_md5.as_ref(),
        );

        let copy_principal = SseKmsPrincipal::from_request(&req);

        if source_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_source_bucket_generation",
                bucket: src_bucket.clone(),
                object: src_key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }

        let (gr, source_cancellation) = store
            .get_object_reader_for_copy(&src_bucket, &src_key, None, h, &src_get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        // The commit owner is intentionally detached so SetDisk can finish
        // its rename/cleanup and post-commit publication if the HTTP caller
        // goes away.  Keep a request-owned guard for the source producer:
        // cancellation drops the source read promptly, while the detached
        // commit task retains the guards it needs to complete safely.
        let _source_cancellation_guard = source_cancellation.clone().drop_guard();

        let mut src_info = gr.object_info.clone();

        // A copy reads the source plaintext, so it needs the source key's decrypt permission
        // as well as the destination key's generate permission below. The source read resolves
        // its material inside the object layer, which has no request identity, so the check
        // happens here.
        authorize_sse_kms_object_read(copy_principal.as_ref(), &src_info.user_defined).await?;

        // Capture the version actually read from the source before src_info is mutated/consumed
        // below. This is the exact source version copied (issue #4976): the response must echo it
        // via x-amz-copy-source-version-id, distinct from the destination version_id.
        let src_resolved_version_id = src_info.version_id;

        // Source object's existing checksum, if any. When the copy does not request a new
        // algorithm, AWS preserves the source object's checksum on the destination (#4996); the
        // copy does not transform the plaintext, so we carry the stored value over unchanged
        // rather than re-hashing every byte.
        let src_checksum = src_info.checksum.as_ref().and_then(|bytes| {
            let (pairs, _) = rustfs_rio::read_checksums(bytes.as_ref(), 0);
            pairs
                .into_iter()
                .find_map(|(k, v)| rustfs_rio::Checksum::new_from_string(&k, &v))
        });

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

        // A same-name copy is normally serviced as a metadata-only update: the store layer
        // rewrites xl.meta in place and leaves the data blocks alone. That shortcut is only sound
        // when the destination's physical bytes are identical to the source's, and encryption
        // breaks exactly that. The destination metadata is rebuilt from scratch below —
        // `strip_managed_encryption_metadata` drops the source DEK and `sse_encryption` mints a
        // fresh one — so reusing the stored ciphertext would leave a new DEK sitting beside bytes
        // it cannot decrypt, permanently destroying the object (GET fails with an AEAD tag
        // mismatch). The mirror case is worse because it is silent: an encrypted source copied
        // without any destination SSE keeps its ciphertext while losing the key metadata, so GET
        // hands back raw ciphertext as if it were plaintext. So whenever either side is
        // encrypted, leave metadata_only = false and let the store layer do a full read/write
        // rewrite through put_object, the same resolution the versioned historical-restore path
        // uses (issue #4238, crates/ecstore/src/store/object.rs).
        //
        // This mirrors MinIO's `isSourceEncrypted || isTargetEncrypted -> metadataOnly = false`
        // in CopyObjectHandler, with one deliberate difference: MinIO decides "target encrypted"
        // from request headers alone, while `effective_sse` here also resolves the bucket default
        // encryption rule. `sse_encryption` mints a DEK from that resolved value, so a
        // header-only check would miss a self-copy under a bucket default rule. The source half
        // deliberately reuses `ObjectInfo::is_encrypted` rather than naming individual headers,
        // so a future encryption flavour is covered here the moment it is recognised there.
        //
        // The zero-copy shortcut is only recoverable for encrypted objects by *preserving* the
        // DEK that sealed the bytes and re-wrapping it under a new master key (MinIO's
        // `rotateKey` + `keyRotation` flag, which is why it may keep metadataOnly = true). RustFS
        // has no such rewrap primitive today; adding one is backlog#1637, and it would enter here
        // as an explicit exception rather than by relaxing this guard.
        let copy_changes_encryption = src_info.is_encrypted() || effective_sse.is_some() || has_explicit_ssec;
        if cp_src_dst_same && src_info.transitioned_object.tier.is_empty() && !copy_changes_encryption {
            src_info.metadata_only = true;
        }

        // Extract user_defined from Arc for mutation; it will be re-wrapped after all edits.
        let mut user_defined = (*src_info.user_defined).clone();
        let effective_tags = replacement_tags.unwrap_or_else(|| (*src_info.user_tags).clone());
        if !replaces_metadata {
            let source_expires = src_info.expires.map(Timestamp::from);
            insert_expires_metadata(&mut user_defined, source_expires.as_ref())?;
        }

        strip_managed_encryption_metadata(&mut user_defined);

        let destination_storage_class = storage_class
            .as_ref()
            .map(StorageClass::as_str)
            .unwrap_or(storageclass::STANDARD);
        src_info.storage_class = Some(destination_storage_class.to_string());

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let length = actual_size;

        let mut compress_metadata = HashMap::new();

        let should_compress = is_disk_compressible(&req.headers, &key) && actual_size > MIN_DISK_COMPRESSIBLE_SIZE as i64;

        if should_compress {
            insert_str(
                &mut compress_metadata,
                SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
            insert_str(&mut compress_metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
        } else {
            remove_str(&mut user_defined, SUFFIX_COMPRESSION);
            remove_str(&mut user_defined, SUFFIX_ACTUAL_SIZE);
            remove_str(&mut user_defined, SUFFIX_COMPRESSION_SIZE);
        }

        // Handle MetadataDirective REPLACE: replace user metadata while preserving system metadata.
        // System metadata (compression, encryption) is added after this block to ensure
        // it's not cleared by the REPLACE operation.
        if let Some(replacement_metadata) = replacement_metadata {
            user_defined = replacement_metadata;
            src_info.content_type = content_type.clone();
            src_info.content_encoding = content_encoding.as_deref().and_then(normalize_content_encoding_for_storage);
            src_info.expires = expires.map(OffsetDateTime::from);
        } else if metadata_directive.is_some() || website_redirect_location.is_some() {
            user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_WEBSITE_REDIRECT_LOCATION));
            if let Some(website_redirect_location) = website_redirect_location {
                user_defined.insert(AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), website_redirect_location);
            }
        }

        user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_STORAGE_CLASS));
        if destination_storage_class != storageclass::STANDARD {
            user_defined.insert(AMZ_STORAGE_CLASS.to_string(), destination_storage_class.to_string());
        }

        user_defined.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_OBJECT_TAGGING));
        if !effective_tags.is_empty() {
            user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), effective_tags.clone());
        }
        src_info.user_tags = Arc::new(effective_tags);

        let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
        remove_object_lock_metadata_for_copy(&mut user_defined);
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
            &bucket,
            &object_lock_config_state,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )? {
            user_defined.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut user_defined,
            has_explicit_object_lock_retention,
        )?;

        let mut write_plan = WritePlan::new();
        let mut reader = if should_compress {
            let algorithm = CompressionAlgorithm::default();
            let hrd = HashReader::from_stream(gr.stream, length, actual_size, None, None, false).map_err(ApiError::from)?;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            HashReader::from_stream(gr.stream, length, actual_size, None, None, false).map_err(ApiError::from)?
        };

        // Give the destination object a checksum so CopyObject returns it and a later checksum-mode
        // HEAD/GET matches (#4996). When the caller requests an algorithm, compute it fresh over the
        // copied plaintext (the hasher sits on the innermost reader so it digests plaintext). When
        // none is requested, carry the source object's stored checksum over unchanged — the copy
        // does not alter the plaintext, so re-hashing would be wasted work and would flatten a
        // multipart composite value.
        match requested_checksum_type {
            Some(checksum_type) => {
                reader.add_calculated_checksum(checksum_type).map_err(ApiError::from)?;
            }
            None => {
                if let Some(cs) = src_checksum {
                    reader.add_non_trailing_checksum(Some(cs), true).map_err(ApiError::from)?;
                }
            }
        }

        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            ssekms_context,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            principal: copy_principal.as_ref(),
        };

        if let Some(material) = sse_encryption(encryption_request).await? {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            write_plan = write_plan.with_encryption(material.write_encryption(None));

            user_defined.extend(encryption_material_to_metadata(&material)?);
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        src_info.put_object_reader = Some(PutObjReader::new(reader));

        // check quota

        for (k, v) in compress_metadata {
            user_defined.insert(k, v);
        }

        // The source object's replication bookkeeping (internal status/timestamp,
        // replica state, and the surfaced x-amz-replication-status) describes the
        // SOURCE's replication history; carried onto the destination it fakes a
        // COMPLETED/REPLICA state for an object that never replicated (MinIO
        // filterReplicationStatusMetadata parity). Inbound replica writes are
        // exempt: the authorized replication request owns these keys (see
        // copy_dst_opts_with_replication_authorization above).
        if !dst_opts.replication_request {
            user_defined.retain(|k, _| !k.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS));
            remove_str(&mut user_defined, SUFFIX_REPLICATION_STATUS);
            remove_str(&mut user_defined, SUFFIX_REPLICATION_TIMESTAMP);
            remove_str(&mut user_defined, SUFFIX_REPLICA_STATUS);
            remove_str(&mut user_defined, SUFFIX_REPLICA_TIMESTAMP);
        }

        // Compute the replication decision exactly once per copy. The same
        // immutable `dsc` drives both the pending metadata written below and the
        // post-commit schedule (see the reuse site after copy_object), so a
        // replication-config hot update cannot split the two phases — same
        // contract as the PUT path (https://github.com/rustfs/backlog/issues/1320).
        // `must_replicate_object` itself declines inbound replica writes
        // (replication_request / REPLICA status), so replicas are never
        // re-scheduled outbound.
        let dsc = must_replicate_object(
            &bucket,
            &key,
            &user_defined,
            "".to_string(),
            dst_opts.delete_marker_replication_status(),
            dst_opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            insert_str(&mut user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut user_defined, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }

        src_info.user_defined = Arc::new(user_defined);

        let quota_check = self
            .check_bucket_quota(
                &bucket,
                QuotaOperation::CopyObject,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
            .await?;
        let quota_enabled = quota_check.as_ref().is_some_and(|result| result.quota_limit.is_some());
        if let Some(quota_check) = quota_check.as_ref() {
            apply_quota_admission(&mut dst_opts, quota_check)?;
        }
        let previous_current_size = match previous_current_sizes {
            Some((_, Ok(logical_size))) if quota_enabled => Some(logical_size),
            Some((_, Err(err))) if quota_enabled => return Err(ApiError::from(err).into()),
            Some((physical_size, _)) => Some(physical_size),
            None => None,
        };
        if let Some(quota_check) = quota_check.as_ref() {
            ensure_object_size_within_quota(
                quota_check,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )?;
        }
        let has_bucket_metadata = self.bucket_metadata_sys().is_some();
        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let copy_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let src_bucket = src_bucket.clone();
            let src_key = src_key.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let src_opts = src_opts.clone();
            let dst_opts = dst_opts.clone();
            async move {
                let _source_bucket_lifecycle_guard = source_bucket_lifecycle_guard;
                let _destination_bucket_lifecycle_guard_storage = destination_bucket_lifecycle_guard_storage;
                let _self_copy_lock_guard = _self_copy_lock_guard;

                let oi = store
                    .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
                    .await
                    .map_err(ApiError::from)?;

                // Reuse the single pre-commit replication decision (see `dsc` above) so
                // the persisted pending marker and the schedule always agree, mirroring
                // the PUT path.
                if dsc.replicate_any() {
                    schedule_object_replication(oi.clone(), Arc::clone(&store), dsc).await;
                }

                maybe_enqueue_transition_immediate(&oi, LcEventSrc::S3CopyObject).await;
                let _ = invalidate_object_data_cache_after_copy_success(&cache_adapter, &bucket, &key).await;

                let dest_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
                if has_bucket_metadata {
                    let committed_size = quota_accounting_object_size(&oi, quota_enabled)?;
                    if dest_versioned {
                        record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                    } else {
                        record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                    }
                }

                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                Ok::<_, S3Error>((oi, dest_versioned))
            }
        });
        let (oi, dest_versioned) = copy_commit.await.map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("copy object commit owner task failed: {err}"))
        })??;

        let raw_dest_version = oi.version_id.map(|v| v.to_string());
        let dest_version = if dest_versioned { raw_dest_version } else { None };

        // Echo the source version that was copied via x-amz-copy-source-version-id (issue #4976).
        // AWS/MinIO return this whenever the source bucket carries versioning (enabled or
        // suspended); render the null version as "null" like GET/HEAD do. This is the exact source
        // version, kept distinct from the destination version_id above.
        let src_versioned = BucketVersioningSys::prefix_enabled(&src_bucket, &src_key).await
            || BucketVersioningSys::prefix_suspended(&src_bucket, &src_key).await;
        let copy_source_version_id = if src_versioned {
            src_resolved_version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        // Report the destination object's checksum in the response, decoded the same way GetObject
        // / HeadObject do so the value is identical to a later checksum-mode HEAD/GET (#4996).
        let response_checksums = oi
            .decrypt_checksums(0, &req.headers)
            .map(|(pairs, is_multipart)| classify_response_checksums(pairs, is_multipart))
            .unwrap_or_default();

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let mut checksum_md5 = None;
        let mut checksum_sha512 = None;
        let mut checksum_xxhash3 = None;
        let mut checksum_xxhash64 = None;
        let mut checksum_xxhash128 = None;
        for (name, value) in response_checksums.extra {
            match name {
                "x-amz-checksum-md5" => checksum_md5 = Some(value),
                "x-amz-checksum-sha512" => checksum_sha512 = Some(value),
                "x-amz-checksum-xxhash3" => checksum_xxhash3 = Some(value),
                "x-amz-checksum-xxhash64" => checksum_xxhash64 = Some(value),
                "x-amz-checksum-xxhash128" => checksum_xxhash128 = Some(value),
                _ => {}
            }
        }
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag.as_ref().map(|etag| to_s3s_etag(etag)),
            last_modified: oi.mod_time.map(Timestamp::from),
            checksum_crc32: response_checksums.crc32,
            checksum_crc32c: response_checksums.crc32c,
            checksum_sha1: response_checksums.sha1,
            checksum_sha256: response_checksums.sha256,
            checksum_crc64nvme: response_checksums.crc64nvme,
            checksum_md5,
            checksum_sha512,
            checksum_xxhash3,
            checksum_xxhash64,
            checksum_xxhash128,
            checksum_type: response_checksums.checksum_type,
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            copy_source_version_id,
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
        let request_context = helper.request_context_or_from_request(&req);
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

        let is_owner = req_info_ref(&req).map(|info| info.is_owner).unwrap_or(false);
        if !recursive_force_delete_is_authorized(&req.headers, is_owner, false) {
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                "Recursive force-delete is restricted to administrative requests",
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        // Capture the bucket generation before per-object authorization, but
        // do not expose a bucket-state error unless at least one object is authorized.
        let bucket_generation = load_bucket_generation_from_store(store.as_ref(), &req, &bucket).await;

        let bypass_governance = has_bypass_governance_header(&req.headers);

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<StorageDeletedObject>,
            error: Option<s3s::dto::Error>,
            synthetic_version_id: bool,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        struct AuthorizedDelete {
            idx: usize,
            object: ObjectToDelete,
        }

        let mut authorized_deletes = Vec::with_capacity(delete.objects.len());
        // Issue #5740: keep the first per-key denial of this bulk request at
        // warn and demote the rest to debug, so a denied 1000-key DeleteObjects
        // cannot flood the log.
        let mut bulk_denial_logged = false;
        for (idx, obj_id) in delete.objects.iter().enumerate() {
            let raw_version_id = obj_id.version_id.clone();
            let (version_id, version_uuid) = match normalize_delete_objects_version_id(raw_version_id.clone()) {
                Ok(parsed) => parsed,
                Err(err) => {
                    delete_results[idx].error = Some(s3s::dto::Error {
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
            if auth_res.is_err() {
                if !bulk_denial_logged {
                    bulk_denial_logged = true;
                    req_info_mut(&mut req)?.suppress_denial_log = true;
                }
                delete_results[idx].error = Some(s3s::dto::Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some("Access Denied".to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            if bypass_governance {
                let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await;
                if auth_res.is_err() {
                    if !bulk_denial_logged {
                        bulk_denial_logged = true;
                        req_info_mut(&mut req)?.suppress_denial_log = true;
                    }
                    delete_results[idx].error = Some(s3s::dto::Error {
                        code: Some("AccessDenied".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some("Access Denied".to_string()),
                        version_id: version_id.clone(),
                    });
                    continue;
                }
            }

            if let Err(err) = validate_table_catalog_object_mutation(&bucket, &obj_id.key).await {
                delete_results[idx].error = Some(s3s::dto::Error {
                    code: Some("InvalidRequest".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(err.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            let synthetic_version_id = version_id.is_none() && is_dir_object(&obj_id.key);
            let object = ObjectToDelete {
                object_name: obj_id.key.clone(),
                version_id: version_uuid,
                synthetic_version_id,
                ..Default::default()
            };
            delete_results[idx].synthetic_version_id = synthetic_version_id;

            authorized_deletes.push(AuthorizedDelete { idx, object });
        }

        if authorized_deletes.is_empty() {
            let output = DeleteObjectsOutput {
                deleted: Some(Vec::new()),
                errors: Some(delete_results.into_iter().filter_map(|result| result.error).collect()),
                ..Default::default()
            };
            let result = Ok(S3Response::new(output));
            let _ = helper.complete(&result);
            return result;
        }
        #[cfg(test)]
        wait_for_delete_objects_auth_test_hook(&bucket).await;
        req.extensions.insert(bucket_generation?);
        let bucket_lock_enabled = object_lock_checks_required(&bucket).await;

        let delete_config_snapshot = Arc::new(
            load_delete_config_snapshot(store.as_ref(), &bucket)
                .await
                .map_err(ApiError::from)?,
        );
        let version_cfg = delete_config_snapshot.versioning_config();
        let replicate_deletes = authorized_deletes
            .iter()
            .any(|authorized| has_active_delete_rule(&delete_config_snapshot, &authorized.object.object_name));

        struct PreparedDelete {
            idx: usize,
            object: ObjectToDelete,
            opts: ObjectOptions,
            skip_stat: bool,
        }

        // Phase 1 (serial): derive storage options from the request-scoped
        // configuration after every candidate has passed authorization.
        let mut prepared_deletes: Vec<PreparedDelete> = Vec::with_capacity(authorized_deletes.len());
        for authorized in authorized_deletes {
            let AuthorizedDelete { idx, object } = authorized;

            let metadata = extract_metadata(&req.headers);
            let opts: ObjectOptions = del_opts_with_versioning(
                &bucket,
                &object.object_name,
                object.version_id.map(|f| f.to_string()),
                &req.headers,
                metadata,
                version_cfg,
                false,
            )
            .map_err(ApiError::from)?;

            // backlog#929 (HP-8): the accounting branch after the store delete
            // decides delete-marker vs object-delete from this exact snapshot,
            // so evaluate it here with the same inputs to keep the stat-skip
            // decision and the accounting path provably consistent.
            let accounting_creates_delete_marker = object.version_id.is_none() && opts.versioned && !opts.version_suspended;
            let skip_stat = can_skip_delete_objects_pre_stat(bucket_lock_enabled, &opts, accounting_creates_delete_marker);

            prepared_deletes.push(PreparedDelete {
                idx,
                object,
                opts,
                skip_stat,
            });
        }

        struct AdmittedDelete {
            idx: usize,
            object: ObjectToDelete,
            versioned: bool,
            version_suspended: bool,
        }

        // Phase 2 (bounded concurrency, backlog#929 / HP-8): collect the
        // metadata needed for accounting and tier cleanup. Entries are
        // independent per key, and `buffered` preserves input order. Object
        // Lock admission is enforced later in set_disk under the write lock.
        let store_ref = &store;
        let bucket_ref = bucket.as_str();
        let admitted_deletes: Vec<AdmittedDelete> =
            futures::stream::iter(prepared_deletes.into_iter().map(|prepared| async move {
                let PreparedDelete {
                    idx,
                    mut object,
                    opts,
                    skip_stat,
                } = prepared;
                let synthetic_version_id = object.version_id.is_none() && is_dir_object(&object.object_name);
                if !skip_stat {
                    match store_ref.get_object_info(bucket_ref, &object.object_name, &opts).await {
                        Ok(_) => {}
                        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => {}
                        Err(err) => return Err(ApiError::from(err)),
                    }
                }

                if synthetic_version_id {
                    object.version_id = Some(Uuid::nil());
                }

                Ok::<_, ApiError>(AdmittedDelete {
                    idx,
                    object,
                    versioned: opts.versioned,
                    version_suspended: opts.version_suspended,
                })
            }))
            .buffered(DELETE_OBJECTS_PRE_STAT_CONCURRENCY)
            .try_collect()
            .await?;

        // Phase 3 (serial): apply outcomes in the original request order so
        // per-key success/failure reporting is unchanged.
        let mut object_to_delete = Vec::new();
        let mut object_to_delete_idx = Vec::new();
        let mut object_versioning = Vec::new();
        for admitted in admitted_deletes {
            object_to_delete_idx.push(admitted.idx);
            object_versioning.push((admitted.versioned, admitted.version_suspended));
            object_to_delete.push(admitted.object);
        }
        let cache_adapter = self.object_data_cache();
        let cache_keys_before_delete = object_to_delete
            .iter()
            .map(|object| object.object_name.clone())
            .collect::<Vec<_>>();
        invalidate_object_data_cache_objects_before_mutation(&cache_adapter, &bucket, cache_keys_before_delete.iter()).await;

        let mut storage_delete_opts = ObjectOptions {
            versioned: version_cfg.enabled(),
            version_suspended: version_cfg.suspended(),
            delete_replication_config_snapshot: Some(Arc::clone(&delete_config_snapshot)),
            object_lock_delete: Some(StorageObjectLockDeleteOptions { bypass_governance }),
            ..Default::default()
        };
        apply_bucket_generation_guard(&req, &bucket, &mut storage_delete_opts)?;
        let (dobjs, errs, accounting) = store
            .delete_objects_with_tier_delete_journal_and_accounting(&bucket, object_to_delete.clone(), storage_delete_opts)
            .await;

        let _manager = get_concurrency_manager();
        let _bucket_clone = bucket.clone();
        let _deleted_objects = dobjs.clone();
        if !errs.is_empty() && errs.iter().all(|err| err.as_ref().is_some_and(is_err_bucket_not_found)) {
            let result = Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
            let _ = helper.complete(&result);
            return result;
        }

        for (i, err) in errs.iter().enumerate() {
            let didx = object_to_delete_idx[i];

            match reduce_delete_objects_result(
                &object_to_delete[i],
                &dobjs[i],
                err.as_ref(),
                delete_results[didx].synthetic_version_id,
            ) {
                Ok(deleted_object) => {
                    delete_results[didx].delete_object = Some(deleted_object.clone());
                    let (versioned, version_suspended) = object_versioning[i];
                    let creates_delete_marker = object_to_delete[i].version_id.is_none() && versioned && !version_suspended;
                    let committed_delete_marker = dobjs[i].delete_marker;
                    let delete_accounting = accounting.get(i).and_then(Option::as_ref);
                    let update = delete_memory_update(
                        creates_delete_marker,
                        committed_delete_marker,
                        delete_request_targets_current(object_to_delete[i].version_id),
                        delete_accounting.and_then(|value| value.size),
                        delete_accounting.is_some_and(|value| value.removed_current_object),
                    );
                    apply_delete_memory_update(&bucket, update).await;
                }
                Err(error) => {
                    delete_results[didx].error = Some(error);
                }
            }
        }

        let deleted = delete_results
            .iter()
            .filter_map(|result| result.delete_object.as_ref().map(|object| (result, object)))
            .map(|(result, object)| DeletedObject {
                delete_marker: { if object.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: delete_response_version_id(
                    object.delete_marker_version_id,
                    result.synthetic_version_id,
                ),
                key: Some(object.object_name.clone()),
                version_id: delete_response_version_id(object.version_id, result.synthetic_version_id),
            })
            .collect();
        let deleted_cache_keys = delete_results
            .iter()
            .filter_map(|result| result.delete_object.as_ref().map(|deleted| deleted.object_name.clone()))
            .collect::<Vec<_>>();
        invalidate_object_data_cache_objects_after_delete_success(&cache_adapter, &bucket, deleted_cache_keys.iter()).await;

        let errors = delete_results
            .iter()
            .filter_map(|v| v.error.clone())
            .collect::<Vec<s3s::dto::Error>>();
        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        };
        let helper = if helper.wants_audit_object_info() {
            let audit_objects =
                successful_delete_audit_objects(&delete, delete_results.iter().map(|result| result.delete_object.is_some()));
            helper.audit_objects(audit_objects)
        } else {
            helper
        };

        let replication_deletes = if replicate_deletes {
            delete_results
                .iter()
                .filter_map(|result| result.delete_object.as_ref())
                .filter(|dobj| deleted_object_has_pending_replication_delete(dobj))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        if !replication_deletes.is_empty() {
            let bucket_for_replication = bucket.clone();
            let replication_task = tokio::spawn(async move {
                let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Replication);
                schedule_replication_deletes(replication_deletes, bucket_for_replication, REPLICATE_INCOMING_DELETE.to_string())
                    .await;
            });
            // The spawned task owns every locally committed delete. Dropping the
            // join handle on request cancellation therefore cannot lose the tail.
            let _ = replication_task.await;
        }

        let req_headers = req.headers.clone();
        let notify = current_notify_interface_for_context(self.context.as_deref());
        let req_params = rustfs_targets::extract_params_header(&req_headers);
        let resp_elements =
            build_event_resp_elements(&S3Response::new(DeleteObjectsOutput::default()), &request_context.request_id);
        let deleted_any = delete_results.iter().any(|result| result.delete_object.is_some());
        let notify_bucket = bucket.clone();
        spawn_background_with_context(Some(request_context), async move {
            let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Notify);
            for res in delete_results {
                if let Some(dobj) = res.delete_object {
                    let event_name = delete_event_name_for_marker(dobj.delete_marker);
                    let event_args = EventArgsBuilder::new(
                        event_name,
                        notify_bucket.clone(),
                        convert_ecstore_object_info(ObjectInfo {
                            name: dobj.object_name.clone(),
                            bucket: notify_bucket.clone(),
                            ..Default::default()
                        }),
                    )
                    .version_id(delete_response_version_id(dobj.version_id, res.synthetic_version_id).unwrap_or_default())
                    .req_params(req_params.clone())
                    .resp_elements(resp_elements.clone())
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notify.notify(event_args).await;
                }
            }
        });

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        if deleted_any {
            rustfs_scanner::record_dirty_usage_bucket(&bucket);
        }
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        result
    }

    #[instrument(level = "info", skip(self, req))]
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

        let is_owner = req_info_ref(&req).map(|info| info.is_owner).unwrap_or(false);
        if !recursive_force_delete_is_authorized(&req.headers, is_owner, replica) {
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                "Recursive force-delete is restricted to internal or administrative requests",
            ));
        }
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        // Establish bucket existence before any bucket-metadata work (matches
        // PUT/GET): nonexistent buckets fail here instead of paying the
        // versioning lookups in del_opts/get_opts first. Resolve the store
        // through the request-bound server context (backlog#1052 S6), not the
        // process-global handle.
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        validate_bucket_exists(&store, &bucket).await?;

        let metadata = extract_metadata(&req.headers);
        // Clone version_id before it's moved
        let version_id_clone = version_id.clone();
        let synthetic_version_id = version_id_clone.is_none() && is_dir_object(&key);

        let delete_config_snapshot = Arc::new(
            load_delete_config_snapshot(store.as_ref(), &bucket)
                .await
                .map_err(ApiError::from)?,
        );
        #[cfg(test)]
        wait_for_delete_snapshot_test_hook(&bucket).await;
        let version_cfg = delete_config_snapshot.versioning_config();
        let mut opts: ObjectOptions =
            del_opts_with_versioning(&bucket, &key, version_id, &req.headers, metadata, version_cfg, replica)
                .map_err(ApiError::from)?;
        opts.delete_replication_config_snapshot = Some(Arc::clone(&delete_config_snapshot));
        opts.object_lock_delete = Some(StorageObjectLockDeleteOptions {
            bypass_governance: has_bypass_governance_header(&req.headers),
        });
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        let force_delete = opts.delete_prefix;

        // let mut vid = opts.version_id.clone();

        if replica {
            opts.set_replica_status(ReplicationStatusType::Replica);

            // if opts.version_purge_status().is_empty() {
            //     vid = None;
            // }
        }

        let expected_current_version_id = expected_current_version_id(&req.headers)?;
        if expected_current_version_id.is_some() && (force_delete || !opts.versioned) {
            return Err(s3_error!(
                InvalidRequest,
                "Expected current version precondition requires a version-specific delete in a versioned bucket"
            ));
        }
        validate_undo_delete_version(expected_current_version_id.as_deref(), opts.version_id.as_deref())?;
        opts.expected_current_version_id = expected_current_version_id.clone();

        let replicate_force_delete = force_delete && !replica && has_active_delete_rule(&delete_config_snapshot, &key);
        let mut force_delete_intent = None;

        let get_opts = opts.clone();
        let existing_object_info = match store.get_object_info(&bucket, &key, &get_opts).await {
            Ok(obj_info) => Some(obj_info),
            Err(err) => {
                // If object not found, allow deletion to proceed (will return 204 No Content)
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };
        #[cfg(test)]
        wait_for_delete_source_test_hook(&bucket).await;

        let cache_adapter = self.object_data_cache();
        // A force (delete_prefix) delete removes every object under `key` as a
        // prefix, so invalidating only the exact key would strand every cached
        // body beneath it. Use the prefix primitive in that branch (ODC-27).
        if force_delete {
            let _ = invalidate_object_data_cache_prefix_before_mutation(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;
        }

        if replicate_force_delete
            && let Some((target_arns, generation)) = force_delete_target_set(&delete_config_snapshot, &key)
            && !target_arns.is_empty()
        {
            let operation_id =
                persist_force_delete_intent(store.clone(), bucket.clone(), key.clone(), target_arns.clone(), generation)
                    .await
                    .map_err(ApiError::from)?;
            force_delete_intent = Some((operation_id, target_arns, generation));
        }

        let obj_info = {
            match store
                .delete_object_with_tier_delete_journal(&bucket, &key, opts.clone())
                .await
            {
                Ok(obj) => obj,
                Err(err) => {
                    if let Some((operation_id, _, _)) = force_delete_intent.as_ref()
                        && let Err(cleanup_error) =
                            crate::storage::storage_api::complete_force_delete_intent(store.clone(), *operation_id).await
                    {
                        warn!(
                            bucket = %bucket,
                            object = %key,
                            operation_id = %operation_id,
                            error = %cleanup_error,
                            "failed to remove uncommitted force-delete intent after local delete failure"
                        );
                    }
                    if is_err_bucket_not_found(&err) {
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
                    }

                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        let (result, _helper) = complete_delete_noop(helper, bucket, key, version_id_clone);
                        return result;
                    }

                    if matches!(&err, StorageError::PrefixAccessDenied(_, _))
                        && let Some(existing_object_info) = existing_object_info.as_ref()
                        && let Some(reason) = check_object_lock_for_deletion(
                            &bucket,
                            existing_object_info,
                            has_bypass_governance_header(&req.headers),
                        )
                        .await
                    {
                        return Err(S3Error::with_message(S3ErrorCode::AccessDenied, reason.error_message()));
                    }

                    return Err(ApiError::from(err).into());
                }
            }
        };

        if force_delete {
            let _ = invalidate_object_data_cache_prefix_after_delete(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_after_delete_success(&cache_adapter, &bucket, &key).await;
        }

        // Fast in-memory update for immediate quota and admin usage consistency.
        // Prefix/force deletes and synthetic directory entries do not carry one
        // committed object identity; leave their cache delta to reconciliation.
        let update = if force_delete || obj_info.name.is_empty() || synthetic_version_id {
            None
        } else {
            // The storage commit returns this object's metadata while its
            // generation lock is held. Never fall back to a pre-delete stat:
            // an overwrite can commit between that stat and this delete.
            delete_memory_update(
                delete_creates_delete_marker(&opts),
                obj_info.delete_marker,
                opts.version_id.is_none(),
                quota_object_size(&obj_info).ok(),
                delete_removes_current_object(&opts),
            )
        };
        apply_delete_memory_update(&bucket, update).await;

        if obj_info.name.is_empty() {
            if let Some((operation_id, target_arns, generation)) = force_delete_intent {
                if let Err(error) = commit_force_delete_intent(store.clone(), operation_id).await {
                    warn!(
                        bucket = %bucket,
                        object = %key,
                        operation_id = %operation_id,
                        error = %error,
                        "failed to mark force-delete intent committed after local delete"
                    );
                }
                let generation = i64::try_from(generation.unix_timestamp_nanos()).unwrap_or(i64::MAX);
                schedule_replication_delete(
                    StorageDeletedObject {
                        object_name: key.clone(),
                        force_delete: true,
                        force_delete_id: Some(operation_id),
                        force_delete_target_arns: target_arns,
                        force_delete_generation: Some(generation),
                        ..Default::default()
                    },
                    bucket.clone(),
                    REPLICATE_INCOMING_DELETE.to_string(),
                )
                .await;
            } else if replicate_force_delete {
                let mut delete_object = StorageDeletedObject {
                    object_name: key.clone(),
                    force_delete: true,
                    ..Default::default()
                };
                if let Some(replication_state) = delete_replication_state_from_config(
                    delete_config_snapshot
                        .replication_config()
                        .unwrap_or_else(|| unreachable!("force-delete requires a replication config")),
                    &ObjectInfo {
                        bucket: bucket.clone(),
                        name: key.clone(),
                        ..Default::default()
                    },
                    None,
                    false,
                ) {
                    set_deleted_object_replication_state(&mut delete_object, &replication_state);
                }
                schedule_replication_delete(delete_object, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
            }
            // Prefix/force-delete returns empty ObjectInfo; still emit bucket notification so webhooks match S3 DELETE.
            helper = helper
                .event_name(delete_event_name_for_marker(false))
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
            rustfs_scanner::record_dirty_usage_bucket(&bucket);
            return result;
        }

        let deleted_replication_info = existing_object_info
            .as_ref()
            .filter(|_| should_use_existing_delete_replication_info(&opts, opts.version_id.is_some()));
        let _delete_tail_guard = DeleteTailActivityGuard::new(DeleteTailStage::Tail);
        let deleted_object_source = deleted_replication_info.unwrap_or(&obj_info);
        let replication_state_source = &obj_info;
        let deleted_delete_marker_version = deleted_replication_info.is_some_and(|info| info.delete_marker);

        let delete_replication_version_id = delete_replication_version_id(deleted_object_source, deleted_delete_marker_version);
        let schedule_delete_replication = if opts.replication_request && replica {
            should_schedule_replica_delete_replication(
                &delete_config_snapshot,
                replication_state_source,
                delete_replication_version_id,
            )
        } else {
            should_schedule_delete_replication(
                &opts,
                replication_state_source,
                deleted_delete_marker_version,
                opts.version_id.is_some(),
            )
        };

        if schedule_delete_replication {
            let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Replication);
            let mut deleted_object = StorageDeletedObject {
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
                replication_state: None,
                ..Default::default()
            };
            set_deleted_object_replication_state(&mut deleted_object, &replication_state_source.replication_state());
            enrich_delete_replication_state_if_needed(&delete_config_snapshot, &mut deleted_object, replication_state_source);
            schedule_replication_delete(deleted_object, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
        }

        let delete_marker = obj_info.delete_marker;
        let version_id = obj_info.version_id;
        let response_version_id = delete_response_version_id(version_id, synthetic_version_id);

        let output = DeleteObjectOutput {
            delete_marker: Some(delete_marker),
            version_id: response_version_id.clone(),
            ..Default::default()
        };

        let event_name = delete_event_name_for_marker(delete_marker);

        helper = helper.event_name(event_name);
        helper = helper.object(obj_info).version_id(response_version_id.unwrap_or_default());

        let result = Ok(S3Response::new(output));
        // Record write operation for capacity management (inline to avoid per-request tokio::spawn overhead)
        let manager = get_capacity_manager();
        manager.record_write_operation().await;
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
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

        let rs = range.map(range_to_http_range_spec).transpose()?;

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        // Establish bucket existence before any bucket-metadata work (matches
        // PUT/GET): nonexistent buckets fail here instead of paying the
        // versioning lookup in get_opts first. Resolve the store through the
        // request-bound server context (backlog#1052 S6), not the
        // process-global handle.
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        validate_bucket_exists(&store, &bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

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
                                error!(bucket, key, error = %e, "Failed to probe children for prefix");
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    // Active-active replication lag window: an object missing
                    // locally may still be served by proxying the HEAD to a
                    // replication target (backlog#1675 P1-5).
                    if let Some(output) = Self::proxy_head_object_to_replication_targets(&req, &bucket, &key, &opts).await {
                        let response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;
                        let result = Ok(response);
                        let _ = helper
                            .version_id(req.input.version_id.clone().unwrap_or_default())
                            .complete(&result);
                        return result;
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
        // An authorized replication convergence check only needs etag/size/mtime
        // to compare source and replica; it holds no customer key, so the SSE-C
        // read validation is skipped for it (and only it).
        let replication_check = replication_request_authorized(&req)
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true");
        if !replication_check {
            validate_sse_headers_for_read(&info.user_defined, &req.headers)?;

            // Validate SSE-C: if the object was encrypted with a customer-provided key,
            // the caller must supply the matching key even for HEAD requests (per S3 spec).
            validate_ssec_for_read(
                &info.user_defined,
                req.input.sse_customer_key.as_ref(),
                req.input.sse_customer_key_md5.as_ref(),
            )?;
        }

        // Compute x-amz-expiration header from lifecycle prediction (before info is partially moved)
        let expiration_header = resolve_put_object_expiration(&bucket, &info).await;
        // Clone ObjectInfo for event notification only when an event will
        // actually be built — the clone is expensive for multipart objects.
        let event_info = helper.wants_object_info().then(|| info.clone());
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!(content_type = %content_type, error = ?err, "Archive content-type parse failed");
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        let content_length = info.get_actual_size().map_err(|e| {
            error!(error = %e, "Failed to resolve actual object size");
            ApiError::from(e)
        })?;

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
        // checksum: classify once; additional algorithms (XXHash3/64/128, SHA-512, MD5)
        // land in `extra` and are emitted as raw headers below (s3s has no typed field).
        let ResponseChecksums {
            crc32: checksum_crc32,
            crc32c: checksum_crc32c,
            sha1: checksum_sha1,
            sha256: checksum_sha256,
            crc64nvme: checksum_crc64nvme,
            checksum_type,
            extra: extra_checksum_headers,
        } = if let Some(checksum_mode) = req.headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (checksums, is_multipart) = info
                .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                .map_err(ApiError::from)?;
            classify_response_checksums(checksums, is_multipart)
        } else {
            ResponseChecksums::default()
        };
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
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
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
        if let Some(event_info) = event_info {
            helper = helper.object(event_info);
        }
        helper = helper.version_id(version_id);

        // NOTE ON CORS:
        // Bucket-level CORS headers are intentionally applied only for object retrieval
        // operations (GET/HEAD) via `wrap_response_with_cors`. Other S3 operations that
        // interact with objects (PUT/POST/DELETE/LIST, etc.) rely on the system-level
        // CORS layer instead. In case both are applicable, this bucket-level CORS logic
        // takes precedence for these read operations.
        let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;

        // Emit additional-checksum headers (XXHash3/64/128, SHA-512) that s3s cannot
        // carry on the typed HeadObjectOutput (#1257).
        inject_additional_checksum_headers(&mut response.headers, &extra_checksum_headers);

        // Add x-amz-tagging-count header if object has tags
        // Per S3 API spec, this header should be present in HEAD object response when tags exist
        if tag_count > 0 {
            let header_name = http::HeaderName::from_static(AMZ_TAG_COUNT);
            if let Ok(header_value) = tag_count.to_string().parse::<HeaderValue>() {
                response.headers.insert(header_name, header_value);
            } else {
                warn!("Failed to parse x-amz-tagging-count header; skipping");
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
        if info.replication_status != ReplicationStatusType::Empty
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_BUCKET_REPLICATION_STATUS.to_ascii_lowercase().as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(info.replication_status.as_str())
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

        validate_table_catalog_object_mutation(&bucket, &object).await?;

        let rreq = rreq.ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::Custom("ErrValidRestoreObject".into()), "restore request is required")
        })?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_str = version_id.clone().unwrap_or_default();
        let mut opts = post_restore_opts(&version_id_str, &bucket, &object)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrPostRestoreOpts".into()), "restore object failed."))?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        // `apply_bucket_generation_guard` deliberately tolerates a missing guard
        // (only the S3 access layer installs one), so this must not hard-require
        // it. Resolve the current generation instead, exactly as the copy path
        // does. The fence is unaffected: the value is re-read from disk and
        // compared below, before the restore is admitted.
        let restore_bucket_incarnation_id = match opts.expected_bucket_incarnation_id {
            Some(incarnation_id) => incarnation_id,
            None => {
                let incarnation_id = store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?;
                opts.expected_bucket_incarnation_id = Some(incarnation_id);
                incarnation_id
            }
        };

        // SELECT-type restores skip both the ongoing check and the metadata
        // write below, so the accept guard would protect nothing for them —
        // they keep the plain (read-locked) accept path.
        let is_select = rreq.type_.as_ref().is_some_and(|t| t.as_str() == "SELECT");

        // Hold the restore-accept guard across the restore-status read, the
        // ongoing/already-restored decision, and the metadata write below, so
        // two concurrent (non-SELECT) POST ?restore cannot both observe
        // ongoing=false and both start a copy-back (backlog#1304). Reads and
        // writes inside this scope run with no_lock; the guard is dropped
        // before the copy-back is spawned so it never blocks readers.
        // Contention on the accept guard (e.g. a concurrent accept or an
        // in-flight commit on the same object) is transient — answer 503
        // SlowDown so SDK clients back off and retry instead of treating it
        // as a hard failure.
        let restore_bucket_lifecycle_guard = Some(acquire_copy_bucket_lifecycle_lock(store.as_ref(), &bucket).await?);
        if store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)? != restore_bucket_incarnation_id {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let accept_guard = if is_select {
            None
        } else {
            let guard = store
                .acquire_restore_accept_guard(&bucket, &object)
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."))?;
            opts.no_lock = true;
            Some(guard)
        };

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
        if let Err(e) = validate_restore_request(&rreq, store.clone()) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrValidRestoreObject".into()),
                format!("Restore object validation failed: {}", e),
            ));
        }

        // Check if restore is already in progress. AWS answers this with
        // 409 RestoreAlreadyInProgress; a Custom code would serialize as a
        // retryable 500 and make SDK clients retry the conflict (backlog#1304).
        if obj_info.restore_ongoing && !is_select {
            return Err(S3Error::with_message(
                S3ErrorCode::RestoreAlreadyInProgress,
                "Object restore is already in progress.",
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
        let mut metadata = (*obj_info.user_defined).clone();
        let restore_operation_id = (!is_select && !already_restored).then(Uuid::new_v4);

        let mut header = HeaderMap::new();

        let event_object_info = obj_info.clone();
        let obj_info_ = obj_info.clone();
        if !is_select {
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
                if let Some(id) = restore_operation_id {
                    insert_str(&mut metadata, SUFFIX_RESTORE_OPERATION_ID, id.to_string());
                }
            }
            obj_info.user_defined = Arc::new(metadata);

            // Fence the compare-and-set write: if the accept guard was lost
            // (lock-service degradation), another node may have concurrently
            // accepted this restore — back off instead of committing a second
            // ongoing flag and double-starting the copy-back.
            if accept_guard.as_ref().is_some_and(|g| g.is_lock_lost()) {
                return Err(S3Error::with_message(S3ErrorCode::SlowDown, "restore object failed."));
            }

            let mut restore_dst_opts = ObjectOptions {
                version_id: obj_info_.version_id.map(|v| v.to_string()),
                mod_time: obj_info_.mod_time,
                no_lock: true,
                expected_bucket_incarnation_id: Some(restore_bucket_incarnation_id),
                ..Default::default()
            };
            if let Some(guard) = restore_bucket_lifecycle_guard.as_ref() {
                restore_dst_opts.add_bucket_lifecycle_lock_guard(guard);
            }
            if let Some(guard) = accept_guard.as_ref() {
                guard.add_namespace_lock_fence(&mut restore_dst_opts);
            }
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
                        // Inside the accept-guard critical section (see above).
                        no_lock: true,
                        ..Default::default()
                    },
                    &restore_dst_opts,
                )
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrCopyObject".into()), "restore object failed."))?;
            rustfs_scanner::record_dirty_usage_bucket(&bucket);

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

        // The accept decision is committed; release the object write lock so
        // the background copy-back and concurrent reads are never blocked on it.
        drop(accept_guard);
        drop(restore_bucket_lifecycle_guard);

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

        // Spawn restoration task in the background. Pin the copy-back to the
        // version the accept resolved and flagged: with a versionless request
        // on a versioned bucket, a PUT landing between the accept and the
        // copy-back would otherwise re-resolve "latest" to the new version,
        // fail (not transitioned), and strand the flagged version at
        // ongoing=true forever (backlog#1304).
        let store_clone = store.clone();
        let bucket_clone = bucket.clone();
        let object_clone = object.clone();
        let rreq_clone = rreq.clone();
        let version_id_clone = obj_info_
            .version_id
            .map(|v| v.to_string())
            .or_else(|| (opts.versioned || opts.version_suspended).then(|| Uuid::nil().to_string()));
        let versioned = opts.versioned;
        let version_suspended = opts.version_suspended;
        let mut restore_operation_metadata = HashMap::new();
        if let Some(id) = restore_operation_id {
            insert_str(&mut restore_operation_metadata, SUFFIX_RESTORE_OPERATION_ID, id.to_string());
        }

        spawn_traced(async move {
            let opts = ObjectOptions {
                transition: TransitionOptions {
                    restore_request: rreq_clone,
                    restore_expiry,
                    ..Default::default()
                },
                version_id: version_id_clone,
                versioned,
                version_suspended,
                expected_bucket_incarnation_id: Some(restore_bucket_incarnation_id),
                user_defined: restore_operation_metadata,
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
                rustfs_scanner::record_dirty_usage_bucket(&bucket_clone);
                debug!(bucket = %bucket_clone, object = %object_clone, "Transitioned object restored");
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

        crate::app::select_object::execute_select_object_content(req).await
    }
}

/// rustfs/backlog#1009: map the rename_data old-size backfill onto the
/// `previous_current_size` value the usage-accounting helpers expect. Outer
/// `None` = unknown (no quorum agreement, or a peer predates the field) — the
/// caller must fall back to the degraded accounting path.
fn previous_current_size_from_backfill(backfill: Option<OldCurrentSize>) -> Option<Option<u64>> {
    backfill.map(|observation| match observation {
        OldCurrentSize::Present(size) => Some(size.max(0) as u64),
        OldCurrentSize::Absent => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderName, HeaderValue, Method};
    use s3s::dto::{
        DefaultRetention, Delete, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication,
        DeleteReplicationStatus, Destination, ExistingObjectReplication, ExistingObjectReplicationStatus, ObjectIdentifier,
        ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRule, ReplicaModifications, ReplicaModificationsStatus,
        ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, RestoreRequest, ServerSideEncryptionByDefault,
        ServerSideEncryptionConfiguration, ServerSideEncryptionRule, SourceSelectionCriteria,
    };
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    #[tokio::test]
    async fn cancelled_eager_put_commit_owner_reaps_stalled_storage_task() {
        let health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let task_health = Arc::clone(&health);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = spawn_traced_join(async move {
            let _progress = task_health.track_write_storage().expect("write tracking must be enabled");
            task_cancellation.cancelled().await;
        });
        let owner = EagerPutCommitOwner::new(task, cancellation, Duration::from_millis(10));
        let request = spawn_traced_join(owner.join());

        tokio::time::timeout(Duration::from_secs(2), async {
            while !health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stalled owner must publish write-storage progress");

        request.abort();
        let _ = request.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled owner must abort and reap the stalled storage task");
    }

    #[test]
    fn delete_response_version_id_preserves_null_and_synthetic_semantics() {
        let version_id = Uuid::new_v4();

        assert_eq!(delete_response_version_id(Some(version_id), false), Some(version_id.to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), false), Some("null".to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), true), None);
        assert_eq!(delete_response_version_id(None, false), None);
    }

    // A malformed bucket-default algorithm reaches this resolution only through
    // corrupt or hand-edited bucket metadata (PutBucketEncryption validates the
    // value), so the invariant is pinned here rather than end-to-end: the copy
    // path must resolve managed AES256 exactly like PUT/extract. With an
    // unencrypted same-name source and no SSE-C, the resolved default alone
    // keeps `copy_changes_encryption` true, so the metadata-only shortcut stays
    // off while `sse_encryption` mints a fresh DEK (backlog#1826).
    #[test]
    fn copy_bucket_default_unknown_sse_algorithm_falls_back_to_aes256() {
        let config = ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    sse_algorithm: ServerSideEncryption::from(String::from("garbage")),
                    kms_master_key_id: None,
                }),
                bucket_key_enabled: None,
            }],
        };

        let effective_sse = config
            .rules
            .first()
            .and_then(|rule| rule.apply_server_side_encryption_by_default.as_ref())
            .map(bucket_default_write_sse);

        assert_eq!(effective_sse.as_ref().map(|sse| sse.as_str()), Some(ServerSideEncryption::AES256));

        // Valid algorithms map to themselves, byte-identical to the PUT path.
        for (configured, expected) in [
            (ServerSideEncryption::AES256, ServerSideEncryption::AES256),
            (ServerSideEncryption::AWS_KMS, ServerSideEncryption::AWS_KMS),
        ] {
            let sse = ServerSideEncryptionByDefault {
                sse_algorithm: ServerSideEncryption::from_static(configured),
                kms_master_key_id: None,
            };
            assert_eq!(bucket_default_write_sse(&sse).as_str(), expected);
        }
    }

    #[test]
    fn put_request_user_metadata_cannot_suppress_bucket_default_retention() {
        let mut metadata =
            HashMap::from([(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::GOVERNANCE.to_string())]);
        apply_put_request_metadata(
            &mut metadata,
            &HeaderMap::new(),
            "object",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: Some(ObjectLockRule {
                    default_retention: Some(DefaultRetention {
                        mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                        days: Some(1),
                        years: None,
                    }),
                }),
            },
            updated_at: OffsetDateTime::now_utc(),
        };
        apply_bucket_default_lock_retention("bucket", &state, &mut metadata, false).unwrap();

        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("COMPLIANCE"));
        assert!(metadata.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
        assert_eq!(metadata.get("x-amz-meta-x-amz-object-lock-mode").map(String::as_str), Some("GOVERNANCE"));

        let mut replication_headers = HeaderMap::new();
        insert_header(&mut replication_headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(
            &mut replication_headers,
            rustfs_utils::http::SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP,
            "2026-01-01T00:00:00Z",
        );
        let mut replica_metadata = HashMap::new();
        let explicit_clear = has_replication_retention_update(&replication_headers, true);
        apply_bucket_default_lock_retention("bucket", &state, &mut replica_metadata, explicit_clear).unwrap();
        assert!(!replica_metadata.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER));
        assert!(!replica_metadata.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
    }

    /// rustfs/backlog#1009: the backfill→accounting mapping must mirror the
    /// prelookup exactly — a live latest version maps to `Some(size)` (clamped
    /// at 0 like the prelookup's `.max(0)`), absent/delete-marker maps to
    /// `None`, and an unknown backfill maps to outer `None` so the caller
    /// takes the degraded path instead of fabricating "new object".
    #[test]
    fn previous_current_size_from_backfill_mirrors_prelookup_semantics() {
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Present(42))), Some(Some(42)));
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Present(-7))), Some(Some(0)));
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Absent)), Some(None));
        assert_eq!(previous_current_size_from_backfill(None), None);
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

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn object_progress_tracks_real_get_and_small_put_lock_waits() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let context = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, Some("false"))], async {
            crate::app::gating_test_env::app_context_with_object_traffic_health(Arc::clone(&object_traffic_health)).await
        })
        .await;
        let store = context.object_store();
        let bucket = format!("object-progress-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("object progress bucket must be created");
        put_real_cold_fill_object(&store, &bucket, object, b"initial").await;

        let metadata_entered = Arc::new(tokio::sync::Barrier::new(2));
        let metadata_resume = Arc::new(tokio::sync::Barrier::new(2));
        crate::storage::options::install_versioning_config_test_hook(
            bucket.clone(),
            Arc::clone(&metadata_entered),
            Arc::clone(&metadata_resume),
        );
        let metadata_input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("metadata GET input must build");
        let metadata_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let metadata_get = tokio::spawn(async move {
            metadata_usecase
                .execute_get_object(build_request(metadata_input, Method::GET))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), metadata_entered.wait())
            .await
            .expect("GET must enter the bucket metadata stage");
        assert!(object_traffic_health.snapshot().read_stalled);
        assert!(!metadata_get.is_finished(), "GET must still be waiting in bucket metadata");
        metadata_resume.wait().await;
        let metadata_response = tokio::time::timeout(Duration::from_secs(10), metadata_get)
            .await
            .expect("metadata GET must finish after release")
            .expect("metadata GET task must join")
            .expect("metadata GET must succeed after release");
        assert!(!object_traffic_health.snapshot().read_stalled);
        drop(metadata_response);

        let read_lock = store
            .new_ns_lock(&bucket, object)
            .await
            .expect("read test namespace lock must be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("read test namespace lock must be held");
        let get_input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("GET input must build");
        let get_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let get = tokio::spawn(async move { get_usecase.execute_get_object(build_request(get_input, Method::GET)).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !object_traffic_health.read_storage_stalled_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked GET must publish a storage stall");
        assert!(!get.is_finished(), "GET must still be waiting for the held namespace lock");
        drop(read_lock);
        let get_response = tokio::time::timeout(Duration::from_secs(10), get)
            .await
            .expect("GET must finish after releasing the lock")
            .expect("GET task must join")
            .expect("GET must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().read_stalled);
        drop(get_response);

        let write_lock = store
            .new_ns_lock(&bucket, object)
            .await
            .expect("write test namespace lock must be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("write test namespace lock must be held");
        let post_store_entered = Arc::new(tokio::sync::Barrier::new(2));
        let post_store_resume = Arc::new(tokio::sync::Barrier::new(2));
        install_put_post_store_test_hook(bucket.clone(), Arc::clone(&post_store_entered), Arc::clone(&post_store_resume));
        let payload = Bytes::from_static(b"replacement");
        let put_input = PutObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let put_usecase = DefaultObjectUsecase::with_context(Some(context));
        let put = tokio::spawn(async move {
            put_usecase
                .execute_put_object(&FS::new(), build_request(put_input, Method::PUT))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked small PUT must publish a storage stall");
        assert!(!put.is_finished(), "PUT must still be waiting for the held namespace lock");
        drop(write_lock);
        tokio::time::timeout(Duration::from_secs(10), post_store_entered.wait())
            .await
            .expect("PUT must reach the first post-store hook");
        assert!(!object_traffic_health.snapshot().write_stalled);
        assert!(!put.is_finished(), "PUT must remain blocked after the store guard has ended");
        post_store_resume.wait().await;
        tokio::time::timeout(Duration::from_secs(10), put)
            .await
            .expect("PUT must finish after releasing the lock")
            .expect("PUT task must join")
            .expect("PUT must succeed after releasing the lock");
        let recovered = object_traffic_health.snapshot();
        assert!(!recovered.read_stalled);
        assert!(!recovered.write_stalled);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn cancelled_put_request_completes_post_commit_publication() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("put-owner-tail-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("PUT owner-tail bucket must be created");

        let old_body = Bytes::from_static(b"old body that must be invalidated");
        let old_info = put_real_cold_fill_object(&store, &bucket, object, &old_body).await;
        let adapter = context.object_data_cache();
        let old_plan = real_cold_fill_plan(&adapter, &bucket, object, &old_info);

        let post_store_entered = Arc::new(tokio::sync::Barrier::new(2));
        let post_store_resume = Arc::new(tokio::sync::Barrier::new(2));
        install_put_post_store_test_hook(bucket.clone(), Arc::clone(&post_store_entered), Arc::clone(&post_store_resume));

        let payload = Bytes::from_static(b"published despite caller cancellation");
        let put_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let put_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let put = tokio::spawn(async move {
            put_usecase
                .execute_put_object(&FS::new(), build_request(put_input, Method::PUT))
                .await
        });

        tokio::time::timeout(Duration::from_secs(10), post_store_entered.wait())
            .await
            .expect("PUT must reach the post-store owner-tail hook");
        assert_eq!(
            adapter.fill_body(&old_plan, old_body.clone()).await,
            rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted,
            "test must republish the old body while the owner tail is paused"
        );
        put.abort();
        post_store_resume.wait().await;
        let _ = put.await.expect_err("outer request task must be cancelled");

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    adapter.lookup_body(&old_plan).await,
                    rustfs_object_data_cache::ObjectDataCacheLookup::Miss
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-commit owner tail must invalidate stale body cache after caller cancellation");

        let recovered = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("cancelled request's owned commit must still publish the object");
        assert_eq!(recovered.size, i64::try_from(payload.len()).expect("test payload length must fit i64"));
    }

    #[tokio::test]
    async fn object_progress_tracks_zero_byte_and_zero_copy_put_lock_waits() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let context =
            crate::app::gating_test_env::app_context_with_object_traffic_health(Arc::clone(&object_traffic_health)).await;
        let store = context.object_store();
        let bucket = format!("progress-buffered-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("buffered PUT progress bucket must be created");

        let extra_body_object = "zero-byte-extra.bin";
        let extra_body_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(extra_body_object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"x")))))
            .content_length(Some(88))
            .build()
            .expect("zero-byte extra-body PUT input must build");
        let extra_body_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let mut extra_body_request = build_request(extra_body_input, Method::PUT);
        extra_body_request.headers = streaming_headers(Some("0"));
        let extra_body_err = extra_body_usecase
            .execute_put_object(&FS::new(), extra_body_request)
            .await
            .expect_err("decoded zero-byte PUT with body data must fail");
        assert_eq!(extra_body_err.code(), &S3ErrorCode::UnexpectedContent);
        assert!(!object_traffic_health.snapshot().write_stalled);
        let lookup_err = store
            .get_object_info(&bucket, extra_body_object, &ObjectOptions::default())
            .await
            .expect_err("rejected zero-byte PUT must not create an object");
        assert!(is_err_object_not_found(&lookup_err));

        let zero_object = "zero-byte.bin";
        let zero_write_lock = store
            .new_ns_lock(&bucket, zero_object)
            .await
            .expect("zero-byte PUT namespace lock must be created")
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("zero-byte PUT namespace lock must be held");
        let (body_polled_tx, body_polled_rx) = tokio::sync::oneshot::channel();
        let (body_release_tx, body_release_rx) = tokio::sync::oneshot::channel();
        let pending_zero_body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_tx.send(()).expect("zero-byte body poll signal must be received");
            body_release_rx.await.expect("zero-byte body EOF must be released");
            Ok::<Bytes, std::io::Error>(Bytes::new())
        }));
        let zero_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(zero_object.to_string())
            .body(Some(pending_zero_body))
            .content_length(Some(87))
            .build()
            .expect("zero-byte PUT input must build");
        let zero_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let mut zero_request = build_request(zero_input, Method::PUT);
        zero_request.headers = streaming_headers(Some("0"));
        let zero_put = tokio::spawn(async move { zero_usecase.execute_put_object(&FS::new(), zero_request).await });

        tokio::time::timeout(Duration::from_secs(30), body_polled_rx)
            .await
            .expect("zero-byte PUT body must be polled for EOF")
            .expect("zero-byte PUT body poll signal must be sent");
        assert!(!object_traffic_health.snapshot().write_stalled);
        assert!(!zero_put.is_finished(), "zero-byte PUT must still be waiting for request EOF");

        body_release_tx.send(()).expect("zero-byte PUT body EOF must be released");
        tokio::time::timeout(Duration::from_secs(30), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fully received zero-byte PUT must publish a storage stall");
        assert!(!zero_put.is_finished(), "zero-byte PUT must still be waiting for the held namespace lock");

        drop(zero_write_lock);
        tokio::time::timeout(Duration::from_secs(30), zero_put)
            .await
            .expect("zero-byte PUT must finish after releasing the lock")
            .expect("zero-byte PUT task must join")
            .expect("zero-byte PUT must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().write_stalled);

        let zero_copy_object = "zero-copy-eager.jpg";
        let zero_copy_payload = Bytes::from(vec![b'z'; 1024 * 1024 + 1]);
        let zero_copy_size = i64::try_from(zero_copy_payload.len()).expect("zero-copy payload length must fit i64");
        let zero_copy_headers = HeaderMap::new();
        assert!(!is_disk_compressible(&zero_copy_headers, zero_copy_object));
        assert_eq!(
            zero_copy_eager_put_path_status(zero_copy_size, &zero_copy_headers, false, false, false),
            PUT_EAGER_STATUS_ELIGIBLE,
            "test payload must exercise the production zero-copy eager path",
        );
        let zero_copy_write_lock = store
            .new_ns_lock(&bucket, zero_copy_object)
            .await
            .expect("zero-copy PUT namespace lock must be created")
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("zero-copy PUT namespace lock must be held");
        let zero_copy_input = PutObjectInput::builder()
            .bucket(bucket)
            .key(zero_copy_object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(zero_copy_payload))))
            .content_length(Some(zero_copy_size))
            .build()
            .expect("zero-copy PUT input must build");
        let zero_copy_usecase = DefaultObjectUsecase::with_context(Some(context));
        let zero_copy_put = tokio::spawn(async move {
            zero_copy_usecase
                .execute_put_object(&FS::new(), build_request(zero_copy_input, Method::PUT))
                .await
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked zero-copy eager PUT must publish a storage stall");
        assert!(
            !zero_copy_put.is_finished(),
            "zero-copy PUT must still be waiting for the held namespace lock"
        );

        drop(zero_copy_write_lock);
        tokio::time::timeout(Duration::from_secs(30), zero_copy_put)
            .await
            .expect("zero-copy PUT must finish after releasing the lock")
            .expect("zero-copy PUT task must join")
            .expect("zero-copy PUT must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().write_stalled);
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_aborts_on_stall() {
        // Inner stream never yields and never reports EOF (a proxy that forwarded
        // a partial body then went silent while holding the connection open).
        let inner = StreamingBlob::wrap(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let mut guarded = guard_put_object_body_read_timeout(
            inner,
            "test-bucket",
            "stalled-object",
            "req-1",
            Some(1024),
            Duration::from_millis(1),
        );

        let err = guarded
            .next()
            .await
            .expect("guard should yield a stall error")
            .expect_err("stalled body should return an error");
        let io_err = err
            .downcast_ref::<std::io::Error>()
            .expect("stall error should wrap an io::Error");
        assert_eq!(io_err.kind(), std::io::ErrorKind::TimedOut);

        // After a stall the guard terminates the stream instead of re-polling the
        // abandoned inner stream.
        assert!(guarded.next().await.is_none());
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_preserves_length_and_passes_through() {
        let body = StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"hello world")));
        assert_eq!(body.remaining_length().exact(), Some(11));

        let mut guarded =
            guard_put_object_body_read_timeout(body, "test-bucket", "ok-object", "req-2", Some(11), Duration::from_secs(60));
        // remaining_length must be forwarded, not reset to unknown.
        assert_eq!(guarded.remaining_length().exact(), Some(11));

        let mut collected = Vec::new();
        while let Some(chunk) = guarded.next().await {
            collected.extend_from_slice(&chunk.expect("chunk should read"));
        }
        assert_eq!(collected, b"hello world");
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_disabled_passthrough() {
        let body = StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"data")));
        let mut guarded = guard_put_object_body_read_timeout(body, "test-bucket", "ok-object", "req-3", Some(4), Duration::ZERO);

        let mut collected = Vec::new();
        while let Some(chunk) = guarded.next().await {
            collected.extend_from_slice(&chunk.expect("chunk should read"));
        }
        assert_eq!(collected, b"data");
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
    fn should_use_small_eager_put_path_allows_up_to_1mb() {
        let headers = HeaderMap::new();

        assert!(should_use_small_eager_put_path(1024, &headers, false, false, false));
        assert!(should_use_small_eager_put_path(1024 * 1024, &headers, false, false, false));
        assert!(!should_use_small_eager_put_path(1024 * 1024 + 1, &headers, false, false, false));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_sse_requests() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, true, false, false));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_compressible_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, false, true, false));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_extract_requests() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, false, false, true));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_large_or_empty_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(0, &headers, false, false, false));
        assert!(!should_use_small_eager_put_path(1024 * 1024 + 1, &headers, false, false, false));
    }

    #[test]
    fn should_use_zero_copy_eager_put_path_allows_large_plain_objects_within_cap() {
        let headers = HeaderMap::new();

        assert!(should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, false, false));
        assert!(should_use_zero_copy_eager_put_path(16 * 1024 * 1024, &headers, false, false, false));
        assert!(!should_use_zero_copy_eager_put_path(16 * 1024 * 1024 + 1, &headers, false, false, false));
        assert_eq!(
            zero_copy_eager_put_path_status(16 * 1024 * 1024, &headers, false, false, false),
            PUT_EAGER_STATUS_ELIGIBLE
        );
        assert_eq!(
            zero_copy_eager_put_path_status(16 * 1024 * 1024 + 1, &headers, false, false, false),
            PUT_EAGER_STATUS_ABOVE_EAGER_MAX
        );
    }

    #[test]
    fn zero_copy_eager_put_path_status_honors_configured_cap() {
        let headers = HeaderMap::new();
        let max_size = 64 * 1024 * 1024;

        assert_eq!(
            zero_copy_eager_put_path_status_with_max_size(33 * 1024 * 1024, &headers, false, false, false, max_size),
            PUT_EAGER_STATUS_ELIGIBLE
        );
        assert_eq!(
            zero_copy_eager_put_path_status_with_max_size(65 * 1024 * 1024, &headers, false, false, false, max_size),
            PUT_EAGER_STATUS_ABOVE_EAGER_MAX
        );
    }

    #[test]
    fn should_use_zero_copy_eager_put_path_rejects_compression_sse_and_extract() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, true, false, false));
        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, true, false));
        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, false, true));
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, true, false, false),
            PUT_EAGER_STATUS_ENCRYPTED
        );
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, false, true, false),
            PUT_EAGER_STATUS_COMPRESSED
        );
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, false, false, true),
            PUT_EAGER_STATUS_EXTRACT
        );
    }

    #[tokio::test]
    async fn read_small_put_body_maps_upload_stream_sha256_mismatch_to_bad_digest() {
        let body = StreamReader::new(futures::stream::iter(vec![Err::<Bytes, std::io::Error>(s3s_body_error_to_io(Box::new(
            MockUploadStreamSha256Mismatch,
        )))]));

        let error = read_small_put_body_exact_direct(body, 1)
            .await
            .expect_err("SHA256 mismatch should reject the small PUT body");

        assert_eq!(error.code(), &S3ErrorCode::BadDigest);
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_maps_upload_stream_sha256_mismatch_to_bad_digest() {
        let body = futures::stream::iter(vec![Err::<Bytes, MockUploadStreamSha256Mismatch>(MockUploadStreamSha256Mismatch)]);

        let error = match read_zero_copy_put_body_exact(body, 1).await {
            Ok(_) => panic!("SHA256 mismatch should reject the zero-copy PUT body"),
            Err(error) => error,
        };

        assert_eq!(error.code(), &S3ErrorCode::BadDigest);
    }

    struct FragmentedBody {
        data: std::io::Cursor<Vec<u8>>,
    }

    impl AsyncRead for FragmentedBody {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let position = usize::try_from(self.data.position()).expect("test cursor position should fit usize");
            let remaining = &self.data.get_ref()[position..];
            let copied = remaining.len().min(buf.remaining()).min(2);
            buf.put_slice(&remaining[..copied]);
            self.data
                .set_position(u64::try_from(position + copied).expect("test cursor position should fit u64"));
            Poll::Ready(Ok(()))
        }
    }

    struct InitializedLengthProbe {
        data: std::io::Cursor<Vec<u8>>,
        initialized_lengths: Arc<Mutex<Vec<usize>>>,
    }

    impl AsyncRead for InitializedLengthProbe {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")
                .push(buf.initialized().len());
            let position = usize::try_from(self.data.position()).expect("test cursor position should fit usize");
            let remaining = &self.data.get_ref()[position..];
            let copied = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..copied]);
            self.data
                .set_position(u64::try_from(position + copied).expect("test cursor position should fit u64"));
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_reads_exact_bytes_without_prefill() {
        let pool = get_concurrency_manager().bytes_pool();
        let initialized_lengths = Arc::new(Mutex::new(Vec::new()));
        let body = InitializedLengthProbe {
            data: std::io::Cursor::new(b"hello".to_vec()),
            initialized_lengths: Arc::clone(&initialized_lengths),
        };

        let buffer = read_small_put_body_exact_pooled(body, 5, pool.as_ref())
            .await
            .expect("pooled exact read should succeed");

        assert_eq!(&buffer[..5], b"hello");
        assert_eq!(buffer.len(), 5);
        assert_eq!(
            initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")[0],
            0,
            "the first pooled body read must use uninitialized spare capacity rather than a zero-filled slice"
        );
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_rejects_short_body() {
        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hell".to_vec());

        let err = match read_small_put_body_exact_pooled(body, 5, pool.as_ref()).await {
            Ok(_) => panic!("short pooled body should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_rejects_extra_body() {
        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hello!".to_vec());

        let err = match read_small_put_body_exact_pooled(body, 5, pool.as_ref()).await {
            Ok(_) => panic!("extra pooled body should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_reads_exact_bytes_without_prefill() {
        let body = std::io::Cursor::new(b"hello".to_vec());
        let reader = read_small_put_body_exact_direct(body, 5)
            .await
            .expect("direct exact read should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
        assert_eq!(reader.get_ref().len(), 5);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_short_and_extra_bodies() {
        let short = read_small_put_body_exact_direct(std::io::Cursor::new(b"hell".to_vec()), 5)
            .await
            .expect_err("short direct body should fail");
        assert_eq!(short.code(), &S3ErrorCode::IncompleteBody);

        let extra = read_small_put_body_exact_direct(std::io::Cursor::new(b"hello!".to_vec()), 5)
            .await
            .expect_err("extra direct body should fail");
        assert_eq!(extra.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_handles_empty_body_boundary() {
        let empty = read_small_put_body_exact_direct(std::io::Cursor::new(Vec::<u8>::new()), 0)
            .await
            .expect("empty direct body should succeed");
        assert!(empty.get_ref().is_empty());

        let extra = read_small_put_body_exact_direct(std::io::Cursor::new(vec![1u8]), 0)
            .await
            .expect_err("non-empty body declared as empty should fail");
        assert_eq!(extra.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_error_after_partial_read() {
        struct PartialThenError {
            delivered_prefix: bool,
        }

        impl AsyncRead for PartialThenError {
            fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                if self.delivered_prefix {
                    return Poll::Ready(Err(std::io::Error::other("body read failed")));
                }

                self.delivered_prefix = true;
                buf.put_slice(b"he");
                Poll::Ready(Ok(()))
            }
        }

        let err = read_small_put_body_exact_direct(PartialThenError { delivered_prefix: false }, 5)
            .await
            .expect_err("a partial body followed by an I/O error must fail");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_accepts_fragmented_body() {
        let reader = read_small_put_body_exact_direct(
            FragmentedBody {
                data: std::io::Cursor::new(b"hello".to_vec()),
            },
            5,
        )
        .await
        .expect("a fragmented exact-length body should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_fragmented_extra_body() {
        let err = read_small_put_body_exact_direct(
            FragmentedBody {
                data: std::io::Cursor::new(b"hello!".to_vec()),
            },
            5,
        )
        .await
        .expect_err("a fragmented body longer than declared must fail");

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_reads_into_uninitialized_spare_capacity() {
        let initialized_lengths = Arc::new(Mutex::new(Vec::new()));
        let body = InitializedLengthProbe {
            data: std::io::Cursor::new(b"hello".to_vec()),
            initialized_lengths: Arc::clone(&initialized_lengths),
        };

        let reader = read_small_put_body_exact_direct(body, 5)
            .await
            .expect("direct exact read should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
        assert_eq!(
            initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")[0],
            0,
            "the first body read must use uninitialized spare capacity rather than a zero-filled slice"
        );
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_exact_reads_chunked_body() {
        use tokio::io::AsyncReadExt;

        let body = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello ")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"world")),
        ]);

        let mut reader = read_zero_copy_put_body_exact(body, 11)
            .await
            .expect("zero-copy eager body read should succeed");
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("chunked bytes reader should be readable");

        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_exact_rejects_extra_bytes() {
        let body = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"!")),
        ]);

        let err = match read_zero_copy_put_body_exact(body, 5).await {
            Ok(_) => panic!("extra bytes should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn pooled_buffer_reader_keeps_buffer_alive_until_consumed() {
        use tokio::io::AsyncReadExt;

        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hello".to_vec());
        let buffer = read_small_put_body_exact_pooled(body, 5, pool.as_ref())
            .await
            .expect("pooled exact read should succeed");
        let mut reader = PooledBufferReader::new(buffer, 5);
        let mut out = Vec::new();

        reader.read_to_end(&mut out).await.expect("pooled reader should be readable");

        assert_eq!(out, b"hello");
    }

    #[test]
    fn should_use_zero_copy_allows_large_unencrypted_binary_objects() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
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
    async fn execute_copy_object_rejects_invalid_storage_class() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "src-bucket".into(),
                key: "src-key".into(),
                version_id: None,
            })
            .bucket("dst-bucket".to_string())
            .key("dst-key".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID")))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_with_storage_class_change() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static(storageclass::RRS)))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Self-copy with explicit storage class change must pass the self-copy guard.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_self_copy_when_object_name_equals_bucket_observes_lock_order() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        use s3s::access::S3Access as _;

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("self-copy lock-order test requires an AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let server_ctx = crate::app::runtime_sources::ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        let fs = FS::with_server_ctx(server_ctx);

        let bucket = format!("self-copy-lock-order-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create self-copy test bucket");
        let payload = b"object whose key equals its bucket".to_vec();
        let mut reader = PutObjReader::from_vec(payload.clone());
        let setup_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        store
            .put_object(&bucket, &bucket, &mut reader, &setup_opts)
            .await
            .expect("put object whose key equals its bucket");

        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut bucket_metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("self-copy bucket metadata should be cached"))
        .clone();
        bucket_metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("self-copy policy should parse"));
        bucket_metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), bucket_metadata)
            .await
            .expect("publish self-copy test policy");

        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: bucket.clone().into(),
                key: bucket.clone().into(),
                version_id: None,
            })
            .bucket(bucket.clone())
            .key(bucket.clone())
            .metadata_directive(Some(MetadataDirective::from_static(MetadataDirective::REPLACE)))
            .metadata(Some(HashMap::from([("lock-order".to_string(), "verified".to_string())])))
            .build()
            .expect("self-copy input should build");
        let mut req = build_request(input, Method::PUT);
        req.extensions.insert(crate::storage::access::ReqInfo::default());
        fs.copy_object(&mut req)
            .await
            .expect("authorize self-copy whose object key equals its bucket");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            DefaultObjectUsecase::with_context(Some(context)).execute_copy_object(req),
        )
        .await
        .expect("lifecycle, authority, and exact object locks must not deadlock")
        .expect("self-copy whose object key equals its bucket should succeed");
        assert!(response.output.copy_object_result.is_some());
        let info = store
            .get_object_info(&bucket, &bucket, &ObjectOptions::default())
            .await
            .expect("self-copied object should remain readable");
        assert_eq!(info.size, payload.len() as i64);

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("clean up self-copy test bucket");
    }

    #[tokio::test]
    async fn execute_copy_object_allows_tiered_self_copy_with_storage_class_change() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static(storageclass::STANDARD)))
            .metadata_directive(Some(MetadataDirective::from_static(MetadataDirective::REPLACE)))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Tiered self-copy with STANDARD storage class must pass all validation checks.
        // The call fails at store init (no store in unit tests), not at validation.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_ne!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_of_historical_version() {
        // Restoring a specific historical version onto the current key (same bucket/key with a
        // source versionId, default COPY directive) must pass the self-copy guard (issue #4238).
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: Some("11111111-1111-1111-1111-111111111111".into()),
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        // Must not be rejected by the self-copy guard; it fails later at store init instead.
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_copy_object_allows_self_copy_of_null_version() {
        // A "null" source version id is a restore of the null version, not a no-op self-copy.
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: Some("null".into()),
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_ne!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_malformed_copy_source_version_id() {
        // A malformed (non-null, non-UUID) source version id is rejected up front.
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "src-bucket".into(),
                key: "src-key".into(),
                version_id: Some("not-a-uuid".into()),
            })
            .bucket("dst-bucket".to_string())
            .key("dst-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_copy_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
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

    #[test]
    fn delete_not_found_completes_noop_event_with_version_context() {
        temp_env::with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"), || {
            crate::server::refresh_notify_module_enabled();
            for (version_id, expected_version) in [(None, ""), (Some("requested-version".to_string()), "requested-version")] {
                let input = DeleteObjectInput::builder()
                    .bucket("test-bucket".to_string())
                    .key("missing-key".to_string())
                    .version_id(version_id.clone())
                    .build()
                    .expect("delete input should build");
                let mut req = build_request(input, Method::DELETE);
                req.extensions.insert(crate::storage::access::ReqInfo {
                    bucket: Some("test-bucket".to_string()),
                    object: Some("missing-key".to_string()),
                    version_id: version_id.clone(),
                    ..Default::default()
                });
                let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, S3Operation::DeleteObject);

                let (result, helper) =
                    complete_delete_noop(helper, "test-bucket".to_string(), "missing-key".to_string(), version_id);
                let event = helper.event_args().expect("successful no-op delete should retain an event");

                assert_eq!(result.expect("no-op delete should succeed").status, Some(StatusCode::NO_CONTENT));
                assert_eq!(event.event_name, EventName::ObjectRemovedNoOP);
                assert_eq!(event.bucket_name, "test-bucket");
                assert_eq!(event.object.name, "missing-key");
                assert_eq!(event.version_id, expected_version);
            }
        });
        crate::server::refresh_notify_module_enabled();
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_expected_version_for_different_destination() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "source-key".into(),
                version_id: Some(Uuid::new_v4().to_string().into()),
            })
            .bucket("test-bucket".to_string())
            .key("destination-key".to_string())
            .build()
            .unwrap();
        let mut req = build_request(input, Method::PUT);
        req.headers.insert(
            RUSTFS_EXPECTED_CURRENT_VERSION_ID,
            HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
        );

        let err = Box::pin(DefaultObjectUsecase::without_context().execute_copy_object(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn undo_delete_requires_version_id_to_match_expected_current() {
        let expected = Uuid::new_v4().to_string();
        assert!(validate_undo_delete_version(Some(&expected), Some(&expected)).is_ok());
        assert_eq!(
            validate_undo_delete_version(Some(&expected), Some(&Uuid::new_v4().to_string()))
                .unwrap_err()
                .code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert_eq!(
            validate_undo_delete_version(Some(&expected), None).unwrap_err().code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert!(validate_undo_delete_version(None, None).is_ok());
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
    async fn execute_delete_objects_rejects_more_than_one_thousand_objects_before_store_lookup() {
        let objects = (0..1001)
            .map(|idx| ObjectIdentifier {
                key: format!("test-key-{idx}"),
                version_id: None,
                ..Default::default()
            })
            .collect();
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete { objects, quiet: None })
            .build()
            .expect("delete objects input should build");

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
        assert_eq!(err.message(), Some("Not init"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_delete_objects_rejects_bucket_recreated_after_authorization() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let context = current_app_context().expect("delete objects generation test requires an AppContext");
        let bucket = format!("delete-objects-generation-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create authorized bucket generation");
        let mut reader = PutObjReader::from_vec(b"old generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put old-generation object");

        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("authorized bucket metadata should be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy should parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("publish test bucket policy");

        let input = DeleteObjectsInput::builder()
            .bucket(bucket.clone())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "object".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .expect("delete objects input should build");
        let mut req = build_request(input, Method::POST);
        req.extensions.insert(crate::storage::access::ReqInfo::default());
        let loaded = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        install_delete_objects_auth_test_hook(bucket.clone(), Arc::clone(&loaded), Arc::clone(&resume));

        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let delete = tokio::spawn(async move { usecase.execute_delete_objects(req).await });
        loaded.wait().await;

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete authorized bucket generation");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate same bucket name");
        let mut reader = PutObjReader::from_vec(b"new generation".to_vec());
        store
            .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put new-generation object");
        resume.wait().await;

        let err = delete
            .await
            .expect("delete objects task should join")
            .expect_err("old authorization must not delete from the recreated bucket");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("new-generation object must survive the stale batch request");
    }

    #[tokio::test]
    async fn execute_delete_object_allows_non_force_request_without_req_info_until_store_lookup() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_object(build_request(input, Method::DELETE))
            .await
            .expect_err("an uninitialized store should be reported after non-force admission");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Not init"));
    }

    #[test]
    fn delete_objects_audit_details_include_only_successful_request_entries() {
        let requested = vec![
            ObjectIdentifier {
                key: "first-key".to_string(),
                version_id: None,
                ..Default::default()
            },
            ObjectIdentifier {
                key: "denied-key".to_string(),
                version_id: Some(Uuid::new_v4().to_string()),
                ..Default::default()
            },
            ObjectIdentifier {
                key: "versioned-key".to_string(),
                version_id: Some("requested-version".to_string()),
                ..Default::default()
            },
        ];

        let objects = successful_delete_audit_objects(
            &Delete {
                objects: requested,
                quiet: Some(true),
            },
            [true, false, true],
        );

        assert_eq!(
            objects,
            vec![
                AuditObjectVersion::new("first-key".to_string(), None),
                AuditObjectVersion::new("versioned-key".to_string(), Some("requested-version".to_string())),
            ]
        );
    }

    #[test]
    fn delete_objects_audit_details_are_empty_when_every_entry_fails() {
        let requested = vec![ObjectIdentifier {
            key: "failed-key".to_string(),
            version_id: None,
            ..Default::default()
        }];

        assert!(
            successful_delete_audit_objects(
                &Delete {
                    objects: requested,
                    quiet: None,
                },
                [false]
            )
            .is_empty()
        );
    }

    #[test]
    fn normalize_delete_objects_version_id_preserves_explicit_null_marker() {
        let (wire_version_id, internal_version_id) =
            normalize_delete_objects_version_id(Some("null".to_string())).expect("null version marker should parse");

        assert_eq!(wire_version_id.as_deref(), Some("null"));
        assert_eq!(internal_version_id, Some(Uuid::nil()));

        let (wire_version_id, internal_version_id) =
            normalize_delete_objects_version_id(Some(" \t ".to_string())).expect("empty version marker should normalize");
        assert_eq!(wire_version_id, None);
        assert_eq!(internal_version_id, None);
    }

    #[test]
    fn delete_objects_treats_raw_io_not_found_as_idempotent() {
        assert!(is_delete_objects_not_found(&StorageError::FileNotFound));
        assert!(is_delete_objects_not_found(&StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::NotFound,
        ))));
        assert!(!is_delete_objects_not_found(&StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        ))));
        assert!(!is_delete_objects_not_found(&StorageError::DiskNotFound));
    }

    #[test]
    fn delete_objects_result_reducer_reports_raw_not_found_as_deleted() {
        let object = ObjectToDelete {
            object_name: "missing-key".to_string(),
            ..Default::default()
        };
        let deleted = StorageDeletedObject {
            object_name: object.object_name.clone(),
            ..Default::default()
        };
        let error = StorageError::Io(std::io::Error::from(std::io::ErrorKind::NotFound));

        let deleted = reduce_delete_objects_result(&object, &deleted, Some(&error), false)
            .expect("raw not-found must produce a deleted result");
        assert_eq!(deleted.object_name, "missing-key");
    }

    #[test]
    fn recursive_force_delete_requires_administrative_or_replica_context() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));

        assert!(!recursive_force_delete_is_authorized(&headers, false, false));
        assert!(recursive_force_delete_is_authorized(&headers, true, false));
        assert!(recursive_force_delete_is_authorized(&headers, false, true));
        assert!(recursive_force_delete_is_authorized(&HeaderMap::new(), false, false));
    }

    #[tokio::test]
    async fn execute_delete_object_rejects_untrusted_force_delete_before_store_access() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("prefix/object".to_string())
            .build()
            .unwrap();
        let mut req = build_request(input, Method::DELETE);
        req.headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));
        req.extensions.insert(crate::storage::access::ReqInfo::default());

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_object(req)
            .await
            .expect_err("untrusted force-delete must be rejected before storage lookup");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_untrusted_force_delete_before_store_access() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "prefix/object".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .unwrap();
        let mut req = build_request(input, Method::POST);
        req.headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));
        req.extensions.insert(crate::storage::access::ReqInfo::default());

        let err = DefaultObjectUsecase::without_context()
            .execute_delete_objects(req)
            .await
            .expect_err("untrusted force-delete must be rejected before storage lookup");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    // backlog#929 (HP-8): the pre-delete stat may only be skipped when every
    // consumer of its result is provably idle. Each guard flips one condition
    // to prove the skip is fenced on all four data dependencies.
    fn delete_marker_creating_opts() -> ObjectOptions {
        ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: false,
            ..Default::default()
        }
    }

    #[test]
    fn delete_objects_pre_stat_skippable_for_delete_marker_on_plain_bucket() {
        assert!(can_skip_delete_objects_pre_stat(false, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_object_lock_buckets() {
        assert!(!can_skip_delete_objects_pre_stat(true, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_explicit_version_deletes() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            versioned: true,
            version_suspended: false,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_unversioned_buckets() {
        // Unversioned deletes remove the current object: usage accounting needs
        // the object size and ILM tier cleanup needs the transition metadata.
        let opts = ObjectOptions {
            version_id: None,
            versioned: false,
            version_suspended: false,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_suspended_versioning() {
        let opts = ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: true,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_when_accounting_snapshot_disagrees() {
        // If the accounting-side versioning snapshot does not also classify the
        // delete as a delete-marker creation, the stat must stay so usage
        // accounting keeps its size input.
        assert!(!can_skip_delete_objects_pre_stat(false, &delete_marker_creating_opts(), false));
    }

    #[test]
    fn delete_accounting_recognizes_explicit_null_as_current_object() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::nil().to_string()),
            version_suspended: true,
            ..Default::default()
        };
        assert!(delete_removes_current_object(&opts));
        assert!(delete_request_targets_current(Some(Uuid::nil())));
        assert!(!delete_request_targets_current(Some(Uuid::new_v4())));
        assert!(!delete_removes_current_object(&ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        }));
    }

    #[test]
    fn compressed_object_delete_restores_usage_baseline() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, SUFFIX_COMPRESSION, "klauspost/compress/s2".to_string());
        let object = ObjectInfo {
            size: 400,
            actual_size: 1000,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        let accounting_size = quota_object_size(&object).expect("logical compressed size should be canonical");

        assert_eq!(
            delete_memory_update(false, false, true, Some(accounting_size), true),
            Some(DeleteMemoryUpdate::Object {
                size: 1000,
                removed_current_object: true,
            })
        );
    }

    #[test]
    fn invalid_accounting_metadata_is_reconciled_without_overflow() {
        assert_eq!(delete_memory_update(false, false, true, None, true), None);
        assert_eq!(
            delete_memory_update(false, true, true, None, true),
            Some(DeleteMemoryUpdate::DeleteMarker)
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn compressed_delete_requests_update_observed_usage_without_releasing_quota_floor() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        use crate::app::storage_api::test::data_usage::apply_bucket_usage_memory_overlay;

        async fn observed_bucket_usage(bucket: &str) -> Option<u64> {
            let mut usage = rustfs_data_usage::DataUsageInfo::default();
            apply_bucket_usage_memory_overlay(&mut usage).await;
            usage.buckets_usage.get(bucket).map(|value| value.size)
        }

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let bucket = format!("compressed-delete-request-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create compressed delete request bucket");

        // Seed the process-local usage with the canonical logical bytes. The
        // direct storage PUT below intentionally does not apply an app-layer
        // usage delta; the two real DELETE requests must remove exactly this
        // amount through their request-layer wiring.
        crate::app::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 2_000).await;

        for object in ["single", "batch"] {
            let mut metadata = HashMap::new();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, "klauspost/compress/s2".to_string());
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, "1000".to_string());
            let reader = HashReader::from_stream(std::io::Cursor::new(vec![0x5a; 400]), 400, 1000, None, None, false)
                .expect("compressed fixture reader should be valid");
            let mut reader = PutObjReader::new(reader);
            store
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        user_defined: metadata,
                        ..Default::default()
                    },
                )
                .await
                .expect("compressed fixture object should be written");
        }

        let mut single_req = build_request(
            DeleteObjectInput::builder()
                .bucket(bucket.clone())
                .key("single".to_string())
                .build()
                .expect("single delete input should build"),
            Method::DELETE,
        );
        single_req.extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            is_owner: true,
            ..Default::default()
        });
        DefaultObjectUsecase::from_global()
            .execute_delete_object(single_req)
            .await
            .expect("single compressed delete should succeed");
        assert_eq!(
            observed_bucket_usage(&bucket).await,
            Some(1_000),
            "single delete must subtract the logical accounting size"
        );
        assert_eq!(
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(2_000),
            "quota must retain the pre-delete floor until scanner reconciliation"
        );

        let mut batch_req = build_request(
            DeleteObjectsInput::builder()
                .bucket(bucket.clone())
                .delete(Delete {
                    objects: vec![ObjectIdentifier {
                        key: "batch".to_string(),
                        ..Default::default()
                    }],
                    quiet: None,
                })
                .build()
                .expect("batch delete input should build"),
            Method::POST,
        );
        batch_req.extensions.insert(crate::storage::access::ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            is_owner: true,
            ..Default::default()
        });
        DefaultObjectUsecase::from_global()
            .execute_delete_objects(batch_req)
            .await
            .expect("batch compressed delete should succeed");
        assert_eq!(
            observed_bucket_usage(&bucket).await,
            Some(0),
            "batch delete must subtract the committed logical accounting size"
        );
        assert_eq!(
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(2_000),
            "quota must retain both pending deletes until scanner reconciliation"
        );

        store
            .delete_bucket(
                &bucket,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                },
            )
            .await
            .expect("clean up compressed delete request bucket");
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
    fn delete_replication_state_from_config_requires_delete_switch_for_marker_version_purges() {
        let arn = "arn:aws:s3:::target-bucket".to_string();
        let mut config = ReplicationConfiguration {
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
        assert!(
            delete_replication_state_from_config(&config, &obj_info, version_id, false).is_none(),
            "delete-marker version purge must not use DeleteMarkerReplication"
        );

        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        let state = delete_replication_state_from_config(&config, &obj_info, version_id, false)
            .expect("delete-marker version purge should honor DeleteReplication");
        let pending = format!("{arn}=PENDING;");

        assert_eq!(state.version_purge_status_internal.as_deref(), Some(pending.as_str()));
        assert_eq!(state.replicate_decision_str, format!("{arn}=true;false;{arn};"));
        assert!(state.purge_targets.contains_key(&arn));
    }

    #[test]
    fn replica_delete_enrichment_must_not_reuse_upstream_targets() {
        let upstream_state = ReplicationState {
            replicate_decision_str: "arn:aws:s3:::upstream=true;false;arn:aws:s3:::upstream;".to_string(),
            replication_status_internal: Some("arn:aws:s3:::upstream=COMPLETED;".to_string()),
            targets: replication_statuses_map("arn:aws:s3:::upstream=COMPLETED;"),
            ..Default::default()
        };
        let mut delete_object = StorageDeletedObject::default();
        set_deleted_object_replication_state(&mut delete_object, &upstream_state);
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
            !should_use_existing_delete_replication_info(&opts, true),
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
            should_use_existing_delete_replication_info(&opts, true),
            "true version-delete requests should keep using the pre-delete object info"
        );
    }

    // https://github.com/rustfs/backlog/issues/1311 — bucket-quota admission must run against the authoritative
    // decoded/plain object length, never the aws-chunked wire Content-Length, and must reject negative/unknown lengths.
    // https://github.com/rustfs/backlog/issues/1336 — but Content-Encoding: aws-chunked alone is only a declared
    // encoding: without a STREAMING-* payload the body is unframed and the wire Content-Length is authoritative.
    fn aws_chunked_headers(decoded_len: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked"));
        if let Some(decoded) = decoded_len {
            headers.insert(
                HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
                HeaderValue::from_str(decoded).unwrap(),
            );
        }
        headers
    }

    fn streaming_headers(decoded_len: Option<&str>) -> HeaderMap {
        let mut headers = aws_chunked_headers(decoded_len);
        headers.insert(
            HeaderName::from_bytes(AMZ_CONTENT_SHA256.as_bytes()).unwrap(),
            HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        );
        headers
    }

    #[test]
    fn authoritative_size_prefers_aws_chunked_decoded_over_wire_content_length() {
        // Wire Content-Length (chunk framing) differs from the decoded object length; the decoded length wins.
        let headers = streaming_headers(Some("1000"));
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(
            size, 1000,
            "aws-chunked admission must use the decoded object length, not the framed wire length"
        );

        // A declared-only aws-chunked request that still carries a decoded length behaves the same.
        let headers = aws_chunked_headers(Some("1000"));
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(size, 1000);
    }

    #[test]
    fn authoritative_size_streaming_without_content_encoding_uses_decoded_length() {
        // A streaming payload signals framing via x-amz-content-sha256 alone; Content-Encoding is optional.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(AMZ_CONTENT_SHA256.as_bytes()).unwrap(),
            HeaderValue::from_static("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        );
        headers.insert(
            HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
            HeaderValue::from_static("1000"),
        );
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(
            size, 1000,
            "a streaming payload without Content-Encoding must still use the decoded length"
        );
    }

    #[test]
    fn authoritative_size_rejects_framed_body_without_decoded_length() {
        // A genuinely framed upload without x-amz-decoded-content-length has no authoritative size;
        // the framed wire length must NOT be a fallback.
        let headers = streaming_headers(None);
        let err = resolve_put_object_authoritative_size(&headers, Some(1088))
            .expect_err("framed upload without decoded length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);

        // ... even when the wire Content-Length is also absent.
        let err =
            resolve_put_object_authoritative_size(&headers, None).expect_err("framed upload without any length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_declared_aws_chunked_without_streaming_uses_wire_content_length() {
        // backlog#1336: an SDK PUT that merely declares Content-Encoding: aws-chunked (issue #1857
        // clients) has an unframed body and no decoded length; the wire Content-Length is the real
        // object size and the request must be admitted, not rejected with UnexpectedContent.
        let headers = aws_chunked_headers(None);
        let size = resolve_put_object_authoritative_size(&headers, Some(1088))
            .expect("declared-only aws-chunked must fall back to the wire Content-Length");
        assert_eq!(size, 1088);

        // Same for a combined declared encoding (aws-chunked,gzip).
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked,gzip"));
        let size = resolve_put_object_authoritative_size(&headers, Some(2048))
            .expect("declared-only aws-chunked,gzip must fall back to the wire Content-Length");
        assert_eq!(size, 2048);

        // Without any length information it is still rejected.
        let headers = aws_chunked_headers(None);
        let err = resolve_put_object_authoritative_size(&headers, None)
            .expect_err("declared-only aws-chunked with no length at all must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_plain_put_uses_content_length() {
        let headers = HeaderMap::new();
        let size = resolve_put_object_authoritative_size(&headers, Some(4096)).expect("plain PUT uses Content-Length");
        assert_eq!(size, 4096);
    }

    #[test]
    fn authoritative_size_plain_put_falls_back_to_decoded_length() {
        // Non-chunked request that only surfaced an explicit decoded length.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
            HeaderValue::from_static("2048"),
        );
        let size = resolve_put_object_authoritative_size(&headers, None).expect("decoded length is the fallback");
        assert_eq!(size, 2048);
    }

    #[test]
    fn authoritative_size_rejects_unknown_length() {
        let headers = HeaderMap::new();
        let err = resolve_put_object_authoritative_size(&headers, None).expect_err("no length information must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_rejects_negative_length() {
        // A negative decoded length would wrap to an enormous unsigned size for quota/buffer sizing; reject it.
        let headers = aws_chunked_headers(Some("-1"));
        let err =
            resolve_put_object_authoritative_size(&headers, Some(64)).expect_err("negative decoded length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);

        let plain = HeaderMap::new();
        let err =
            resolve_put_object_authoritative_size(&plain, Some(-100)).expect_err("negative Content-Length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_accepts_exact_and_rejects_negative_boundary() {
        // Exact zero-length object is admissible (the over-by-1/exact-limit boundary is enforced by the quota checker on this value).
        let headers = aws_chunked_headers(Some("0"));
        assert_eq!(
            resolve_put_object_authoritative_size(&headers, Some(87)).expect("zero-length decoded is valid"),
            0
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn quota_rejects_ciphertext_replication_before_polling_the_body() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (_store, bucket) =
            crate::app::gating_test_env::durable_quota_test_bucket("ciphertext-replication-early-reject", 4096).await;
        let body_polled = Arc::new(AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_in_stream.store(true, Ordering::Release);
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"ciphertext"))
        }));
        let input = PutObjectInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .body(Some(body))
            .content_length(Some(10))
            .build()
            .expect("ciphertext replication PUT input should build");
        let mut request = build_request(input, Method::PUT);
        insert_header(&mut request.headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        request
            .headers
            .insert(rustfs_utils::http::REPLICATION_SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });

        let err = DefaultObjectUsecase::from_global()
            .execute_put_object(&FS::new(), request)
            .await
            .expect_err("quota-enabled ciphertext replication should fail at ingress");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!body_polled.load(Ordering::Acquire), "rejected ciphertext body must not be consumed");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_quota_rejects_full_put_before_polling_the_body() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use std::sync::atomic::{AtomicBool, Ordering};

        const GI_B: u64 = 1024 * 1024 * 1024;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("legacy-quota-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create legacy quota test bucket");
        crate::app::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 4 * GI_B).await;
        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(metadata_sys)
            .set_quota_config(
                &bucket,
                BucketQuota {
                    quota: Some(5 * GI_B),
                    ..Default::default()
                },
            )
            .await
            .expect("configure legacy quota");

        let body_polled = Arc::new(AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_in_stream.store(true, Ordering::Release);
            Ok::<Bytes, std::io::Error>(Bytes::new())
        }));
        let input = PutObjectInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .body(Some(body))
            .content_length(Some(i64::try_from(2 * GI_B).expect("test size should fit i64")))
            .build()
            .expect("legacy quota PUT input should build");

        let err = DefaultObjectUsecase::from_global()
            .execute_put_object(&FS::new(), build_request(input, Method::PUT))
            .await
            .expect_err("4 GiB used plus a 2 GiB PUT must exceed a 5 GiB legacy quota");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!body_polled.load(Ordering::Acquire), "legacy quota rejection must not consume the body");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_puts_share_durable_bucket_quota_reservations() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-put-quota", 6000).await;

        let first_opts = ObjectOptions::default();
        let second_opts = ObjectOptions::default();
        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x73; 4096]);
            first_store.put_object(&first_bucket, "first", &mut reader, &first_opts).await
        });
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x74; 4096]);
            store.put_object(&bucket, "second", &mut reader, &second_opts).await
        });
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first PUT task should not panic");
        let second = second.expect("second PUT task should not panic");

        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let denied = first.err().or_else(|| second.err()).expect("one PUT must be denied");
        assert!(matches!(
            denied,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 6000
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_within_limit_puts_keep_independent_mutation_fences() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-fence-quota", 8192).await;
        let first_barrier = PutObjectCommitBarrier::install(&bucket, "first", PutObjectCommitPause::BeforeQuotaRename);
        let second_barrier = PutObjectCommitBarrier::install(&bucket, "second", PutObjectCommitPause::BeforeQuotaRename);

        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x75; 4096]);
            first_store
                .put_object(&first_bucket, "first", &mut reader, &ObjectOptions::default())
                .await
        });
        let second_store = Arc::clone(&store);
        let second_bucket = bucket.clone();
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x76; 4096]);
            second_store
                .put_object(&second_bucket, "second", &mut reader, &ObjectOptions::default())
                .await
        });

        first_barrier.wait_until_paused().await;
        second_barrier.wait_until_paused().await;
        first_barrier.release();
        second_barrier.release();

        first
            .await
            .expect("first PUT task should not panic")
            .expect("first within-limit PUT should commit");
        second
            .await
            .expect("second PUT task should not panic")
            .expect("second within-limit PUT should commit");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn put_rejects_rotated_quota_capability_before_rename() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("rotated-proof-put-quota", 4096).await;
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x77; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        assert!(
            crate::storage::storage_api::ecstore_notification::rotate_cross_pool_fence_fleet_proof_for_test(),
            "the gating environment must have a current fleet proof"
        );
        barrier.release();

        let err = put
            .await
            .expect("PUT task should not panic")
            .expect_err("a replaced fleet proof must fence the authoritative rename");
        assert!(matches!(
            err,
            StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_reservation",
                ..
            }
        ));
        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect_err("proof rotation before rename must leave no committed object");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn durable_quota_reclaims_overwrites_and_deleted_bytes() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("quota-delta-reconcile", 4096).await;

        for byte in [0x41, 0x42] {
            let mut reader = PutObjReader::from_vec(vec![byte; 4096]);
            store
                .put_object(&bucket, "object", &mut reader, &ObjectOptions::default())
                .await
                .expect("same-size overwrite must consume no additional quota");
        }

        store
            .delete_object(&bucket, "object", ObjectOptions::default())
            .await
            .expect("delete quota-tracked object");
        let mut replacement = PutObjReader::from_vec(vec![0x43; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("deleted bytes must be reclaimed before rejecting a replacement");

        let mut excess = PutObjReader::from_vec(vec![0x44]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("one byte beyond the reclaimed exact quota must be denied");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 4096
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_put_has_zero_quota_growth() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("data-movement-put-quota", 0).await;
        let mut reader = PutObjReader::from_vec(vec![0x79; 4096]);
        let stored = store
            .put_object(
                &bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("moving an already-accounted object between pools must have zero quota growth");
        assert_eq!(stored.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_put_releases_durable_quota_reservation() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("cancelled-put-quota", 4096).await;

        let barrier = PutObjectCommitBarrier::install(&bucket, "cancelled", PutObjectCommitPause::AfterQuotaReservation);
        let cancelled_store = Arc::clone(&store);
        let cancelled_bucket = bucket.clone();
        let cancelled = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x51; 4096]);
            cancelled_store
                .put_object(&cancelled_bucket, "cancelled", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        cancelled.abort();
        let cancelled_result = cancelled.await;
        assert!(cancelled_result.is_err(), "the paused request must be cancelled");
        drop(barrier);

        let mut replacement = PutObjReader::from_vec(vec![0x52; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("cancelling before commit must release the complete reservation");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_put_after_commit_marker_is_reconciled() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("cancelled-spawned-put-quota", 4096).await;
        let commit_barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x53; 4096]);
            first_store
                .put_object(&first_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        commit_barrier.wait_until_paused().await;
        first.abort();
        assert!(first.await.is_err(), "the outer request task must be cancelled");
        drop(commit_barrier);

        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect_err("cancelling before rename must not commit the object");
        let mut replacement = PutObjReader::from_vec(vec![0x54; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("the next admission must reap the abandoned commit marker");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn committed_put_survives_quota_ledger_settlement_failure() {
        use crate::app::storage_api::test::set_disk::{
            PutObjectCommitBarrier, PutObjectCommitPause, fail_next_quota_ledger_save_for_test,
        };

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("settlement-failure-quota", 4096).await;
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x59; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        fail_next_quota_ledger_save_for_test();
        barrier.release();
        put.await
            .expect("PUT task should not panic")
            .expect("a post-commit ledger failure must not change the successful write result");
        let stored = store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("the committed object must remain visible");
        assert_eq!(stored.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn suspended_null_version_overwrite_uses_exact_quota_delta() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("suspended-version-quota", 6200).await;
        let mut versioned_reader = PutObjReader::from_vec(vec![0x61; 4096]);
        store
            .put_object(
                &bucket,
                "object",
                &mut versioned_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write UUID version");

        for (size, byte) in [(1024, 0x62), (2048, 0x63)] {
            let mut reader = PutObjReader::from_vec(vec![byte; size]);
            store
                .put_object(
                    &bucket,
                    "object",
                    &mut reader,
                    &ObjectOptions {
                        version_suspended: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("suspended write should replace only the exact null version");
        }

        let mut excess = PutObjReader::from_vec(vec![0x64; 57]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("UUID plus replacement null version must consume 6144 bytes");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 6144,
                limit: 6200
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn durable_quota_reservation_observes_lowered_config_revision() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("lowered-quota-revision", 8192).await;
        let mut initial = PutObjReader::from_vec(vec![0x71; 4096]);
        store
            .put_object(&bucket, "initial", &mut initial, &ObjectOptions::default())
            .await
            .expect("write under original quota");

        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(metadata_sys)
            .set_quota_config(&bucket, BucketQuota::new(Some(4096)))
            .await
            .expect("lower bucket quota");
        let mut excess = PutObjReader::from_vec(vec![0x72]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("reservation must not use the stale larger quota revision");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 4096
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn quota_enable_waits_for_unlimited_commit() {
        use crate::app::storage_api::test::metadata_sys::ConfigWriteLockProbe;
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("quota-config-fence", 8192).await;
        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(Arc::clone(&metadata_sys))
            .set_quota_config(&bucket, BucketQuota::new(None))
            .await
            .expect("clear quota before the fenced write");
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::AfterQuotaReservation);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x73; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let update_probe = ConfigWriteLockProbe::install(&bucket);
        let update_bucket = bucket.clone();
        let update = tokio::spawn(async move {
            QuotaChecker::new(metadata_sys)
                .set_quota_config(&update_bucket, BucketQuota::new(Some(0)))
                .await
        });
        update_probe.wait_until_attempted().await;
        assert!(
            !update.is_finished(),
            "quota mutation must wait for the reservation's metadata transaction guard"
        );

        barrier.release();
        put.await
            .expect("PUT task should not panic")
            .expect("the write linearized before the quota update must commit");
        update
            .await
            .expect("quota update task should not panic")
            .expect("quota update should proceed after commit");

        let mut excess = PutObjReader::from_vec(vec![0x74]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("writes after the zero-byte quota update must be denied");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 4096, limit: 0 }));
    }
}
