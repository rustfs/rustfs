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
    PostObjectRequestMarker, authorize_request, has_bypass_governance_header, req_info_mut,
};
use super::storage_api::object_usecase::bucket::quota::checker::QuotaChecker;
#[cfg(test)]
use super::storage_api::object_usecase::bucket::replication::{ReplicationState, replication_statuses_map};
use super::storage_api::object_usecase::bucket::{
    VersioningConfigExt as _,
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{enqueue_transition_immediate, post_restore_opts},
        lifecycle::{self, TransitionOptions},
        tier_delete_journal, tier_sweeper,
    },
    metadata_sys,
    object_lock::{
        objectlock::{get_object_legalhold_meta, get_object_retention_meta},
        objectlock_sys::{check_object_lock_for_deletion, is_retention_active},
    },
    predict_lifecycle_expiration,
    quota::{QuotaCheckResult, QuotaError, QuotaOperation},
    replication::{
        REPLICATE_INCOMING_DELETE, ReplicationStatusType, VersionPurgeStatusType, check_replicate_delete,
        delete_replication_state_from_config, delete_replication_version_id, deleted_object_has_pending_replication_delete,
        must_replicate_object, schedule_object_replication, schedule_replication_delete, set_deleted_object_replication_state,
        set_object_to_delete_version_purge_status, should_schedule_delete_replication,
        should_use_existing_delete_replication_info, should_use_existing_delete_replication_source,
    },
    tagging::decode_tags,
    validate_restore_request,
    versioning_sys::BucketVersioningSys,
};
use super::storage_api::object_usecase::compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
use super::storage_api::object_usecase::concurrency::{
    self, ConcurrencyManager, DiskReadAdmission, GetObjectGuard, PutObjectGuard, get_concurrency_aware_buffer_size,
    get_concurrency_manager, get_put_concurrency_aware_buffer_size,
};
#[cfg(test)]
use super::storage_api::object_usecase::contract::http::HTTPPreconditions;
use super::storage_api::object_usecase::contract::namespace::NamespaceLocking;
use super::storage_api::object_usecase::contract::object::{ObjectIO as _, ObjectOperations as _};
use super::storage_api::object_usecase::contract::range::HTTPRangeSpec;
use super::storage_api::object_usecase::data_usage::{
    record_bucket_delete_marker_memory, record_bucket_object_delete_memory, record_bucket_object_version_write_memory,
    record_bucket_object_write_memory, record_bucket_object_write_unknown_previous_memory,
};
use super::storage_api::object_usecase::deadlock_detector;
use super::storage_api::object_usecase::ecfs::FS;
use super::storage_api::object_usecase::error::{
    DiskError, Error as EcstoreError, StorageError, is_all_buckets_not_found, is_err_bucket_not_found, is_err_object_not_found,
    is_err_version_not_found,
};
use super::storage_api::object_usecase::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use super::storage_api::object_usecase::helper::{OperationHelper, spawn_background_with_context};
use super::storage_api::object_usecase::io::{DynReader, HashReader, WritePlan, compression_metadata_value, wrap_reader};
#[cfg(test)]
use super::storage_api::object_usecase::object_cache::GetObjectBodySource;
#[cfg(test)]
use super::storage_api::object_usecase::object_cache::lookup_get_object_body_cache_hook;
use super::storage_api::object_usecase::object_cache::{GetObjectBodyCacheHookLookup, get_object_body_cache_plaintext_len};
use super::storage_api::object_usecase::object_utils::to_s3s_etag;
use super::storage_api::object_usecase::options::{
    copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime_with_object_name,
    filter_object_metadata, get_content_sha256_with_query, get_opts, namespace_reserved_user_metadata,
    normalize_content_encoding_for_storage, put_opts, validate_archive_content_encoding,
};
use super::storage_api::object_usecase::request_context::{self, spawn_traced};
use super::storage_api::object_usecase::s3_api::multipart::parse_list_parts_params;
use super::storage_api::object_usecase::set_disk::{
    get_lock_acquire_timeout, get_object_disk_read_timeout, is_valid_storage_class,
};
use super::storage_api::object_usecase::sse::{
    DecryptionRequest, EncryptionRequest, SSEType, apply_bucket_default_lock_retention, build_ssec_read_headers,
    encryption_material_to_metadata, extract_server_side_encryption_from_headers, extract_ssec_params_from_headers,
    extract_ssekms_context_from_headers, get_buffer_size_opt_in, map_get_object_reader_error, sse_decryption, sse_encryption,
};
use super::storage_api::object_usecase::storage_class as storageclass;
use super::storage_api::object_usecase::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
use super::storage_api::object_usecase::{ECStore, OldCurrentSize};
use super::storage_api::object_usecase::{
    RFC1123, check_preconditions, get_validated_store, has_replication_rules, parse_object_lock_legal_hold,
    parse_object_lock_retention, parse_part_number_i32_to_usize, remove_object_lock_metadata_for_copy,
    strip_managed_encryption_metadata, validate_bucket_object_lock_enabled, validate_object_key, validate_sse_headers_for_read,
    validate_sse_headers_for_write, validate_ssec_for_read, wrap_response_with_cors,
};
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_expiry_state_handle, current_notify_interface_for_context,
    current_object_data_cache_for_context, current_object_store_handle_for_context,
};
use crate::config::RustFSBufferConfig;
use crate::delete_tail_activity::{DeleteTailActivityGuard, DeleteTailStage};
use crate::error::ApiError;
use crate::server::convert_ecstore_object_info;
use crate::table_catalog;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, StatusCode};
use md5::Context as Md5Context;
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_config::MI_B;
use rustfs_filemeta::{RestoreStatusOps, parse_restore_obj_status};
use rustfs_io_core::{BytesPool, PooledBuffer};
use rustfs_io_metrics;
use rustfs_lock::NamespaceLockGuard;
use rustfs_notify::EventArgsBuilder;
use rustfs_object_capacity::capacity_manager::get_capacity_manager;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::{S3Operation, delete_event_name_for_marker, put_event_name_for_post_object};
use rustfs_s3select_api::object_store::bytes_stream;
use rustfs_targets::{
    EventName, extract_params_header, extract_resp_elements, get_request_host, get_request_port, get_request_user_agent,
};
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE, AMZ_WEBSITE_REDIRECT_LOCATION, CONTENT_TYPE,
    SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    SUFFIX_RESTORE_OPERATION_ID,
    headers::{
        AMZ_CONTENT_SHA256, AMZ_DECODED_CONTENT_LENGTH, AMZ_MINIO_SNOWBALL_IGNORE_DIRS, AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
        AMZ_MINIO_SNOWBALL_PREFIX, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE,
        AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
        AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE, AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
        AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, AMZ_RUSTFS_SNOWBALL_PREFIX, AMZ_SERVER_SIDE_ENCRYPTION,
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, AMZ_SNOWBALL_EXTRACT,
        AMZ_SNOWBALL_IGNORE_DIRS, AMZ_SNOWBALL_IGNORE_ERRORS, AMZ_SNOWBALL_PREFIX, AMZ_STORAGE_CLASS, AMZ_TAG_COUNT,
    },
    insert_str, remove_str,
};
use rustfs_utils::path::{encode_dir_object, is_dir_object, path_join_buf};
use rustfs_zip::{ArchiveLimits, CompressionFormat};
use s3s::StdError;
use s3s::dto::{
    CacheControl, Checksum, ChecksumAlgorithm, ChecksumType, ContentDisposition, ContentEncoding, ContentLanguage, ContentType,
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, ETag, GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectAttributesParts,
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, MetadataDirective, ObjectAttributes, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode, ObjectPart, PutObjectInput,
    PutObjectOutput, Range, RequestCharged, RestoreObjectInput, RestoreObjectOutput, RestoreStatus, SSECustomerAlgorithm,
    SSECustomerKeyMD5, SSEKMSKeyId, SelectObjectContentInput, SelectObjectContentOutput, ServerSideEncryption, StorageClass,
    StreamingBlob, TaggingDirective, TaggingHeader, Timestamp, TimestampFormat, WebsiteRedirectLocation,
};
use s3s::header::{X_AMZ_RESTORE, X_AMZ_RESTORE_OUTPUT_PATH};
use s3s::stream::{ByteStream, DynByteStream, RemainingLength};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

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
use std::ops::Add;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, RwLock};
use tokio_tar::Archive;
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use super::storage_api::object_usecase::{
    GetObjectReader, StorageDeletedObject, StorageObjectInfo as ObjectInfo, StorageObjectLockDeleteOptions,
    StorageObjectOptions as ObjectOptions, StorageObjectToDelete as ObjectToDelete, StoragePutObjReader as PutObjReader,
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

type S3StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

struct ColdFillDiskPermitMetric {
    owner: ColdFillDiskPermitOwner,
    metric_recorded: bool,
}

#[cfg(test)]
static COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
struct ColdFillPublicationBarrier {
    reached: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
type ColdFillPublicationBarrierState = Option<(rustfs_object_data_cache::ObjectDataCacheKey, Arc<ColdFillPublicationBarrier>)>;

#[cfg(test)]
static COLD_FILL_PUBLICATION_BARRIER: OnceLock<Mutex<ColdFillPublicationBarrierState>> = OnceLock::new();

#[cfg(test)]
type ColdFillReaderOpenProbeState = Option<(rustfs_object_data_cache::ObjectDataCacheKey, Arc<AtomicU64>)>;

#[cfg(test)]
static COLD_FILL_READER_OPEN_PROBE: OnceLock<Mutex<ColdFillReaderOpenProbeState>> = OnceLock::new();

fn adjust_cold_fill_disk_permit_metric(owner: ColdFillDiskPermitOwner, acquired: bool) {
    macro_rules! adjust_gauge {
        ($name:literal) => {{
            #[cfg(not(test))]
            let gauge = {
                static HANDLE: std::sync::LazyLock<metrics::Gauge> = std::sync::LazyLock::new(|| metrics::gauge!($name));
                &*HANDLE
            };
            #[cfg(test)]
            let gauge = metrics::gauge!($name);
            if acquired {
                gauge.increment(1.0);
            } else {
                gauge.decrement(1.0);
            }
        }};
    }

    match owner {
        ColdFillDiskPermitOwner::Producer => {
            adjust_gauge!("rustfs_object_data_cache_cold_fill_producer_disk_permits");
        }
        ColdFillDiskPermitOwner::Follower => {
            adjust_gauge!("rustfs_object_data_cache_cold_fill_follower_disk_permits");
        }
    }
}

#[cfg(test)]
async fn wait_cold_fill_publication_barrier(plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan) {
    let Some(key) = plan.key() else {
        return;
    };
    let barrier = COLD_FILL_PUBLICATION_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|(barrier_key, _)| barrier_key == key)
        .map(|(_, barrier)| Arc::clone(barrier));
    if let Some(barrier) = barrier {
        barrier.reached.add_permits(1);
        if let Ok(permit) = barrier.release.acquire().await {
            permit.forget();
        }
    }
}

#[cfg(test)]
fn record_cold_fill_reader_open_for_test(plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan) {
    let Some(key) = plan.key() else {
        return;
    };
    let probe = COLD_FILL_READER_OPEN_PROBE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|(probe_key, _)| probe_key == key)
        .map(|(_, count)| Arc::clone(count));
    if let Some(count) = probe {
        count.fetch_add(1, Ordering::Relaxed);
    }
}

impl ColdFillDiskPermitMetric {
    fn new(owner: ColdFillDiskPermitOwner) -> Self {
        let metric_recorded = rustfs_io_metrics::metrics_enabled();
        if metric_recorded {
            adjust_cold_fill_disk_permit_metric(owner, true);
        }
        #[cfg(test)]
        if matches!(owner, ColdFillDiskPermitOwner::Follower) {
            COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.fetch_add(1, Ordering::Relaxed);
        }
        Self { owner, metric_recorded }
    }
}

impl Drop for ColdFillDiskPermitMetric {
    fn drop(&mut self) {
        if self.metric_recorded {
            adjust_cold_fill_disk_permit_metric(self.owner, false);
        }
        #[cfg(test)]
        if matches!(self.owner, ColdFillDiskPermitOwner::Follower) {
            COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct GetObjectDiskPermit {
    permit: Option<OwnedSemaphorePermit>,
    metric: Option<ColdFillDiskPermitMetric>,
}

impl GetObjectDiskPermit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit: Some(permit),
            metric: current_cold_fill_disk_permit_owner().map(ColdFillDiskPermitMetric::new),
        }
    }

    fn release(&mut self) {
        self.permit.take();
        self.metric.take();
    }
}

impl From<OwnedSemaphorePermit> for GetObjectDiskPermit {
    fn from(permit: OwnedSemaphorePermit) -> Self {
        Self::new(permit)
    }
}

impl Drop for GetObjectDiskPermit {
    fn drop(&mut self) {
        self.release();
    }
}

const ACCEPT_RANGES_BYTES: &str = "bytes";
const COLD_FILL_HARD_MAX_DURATION: Duration = Duration::from_secs(10 * 60);
pub(crate) const MAX_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 64 * 1024 * 1024;
const MEDIUM_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 8 * 1024 * 1024;
const HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 4 * 1024 * 1024;
const VERY_HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 1024 * 1024;
const LOG_COMPONENT_APP: &str = "app";
const LOG_SUBSYSTEM_OBJECT: &str = "object";
const EVENT_PUT_OBJECT_STORE_INFLIGHT_SLOW: &str = "put_object_store_inflight_slow";
const EVENT_PUT_OBJECT_STORE_RETURNED: &str = "put_object_store_returned";
const EVENT_GET_OBJECT_STREAM_BODY: &str = "get_object_stream_body";
const EVENT_PUT_OBJECT_BODY_READ_STALLED: &str = "put_object_body_read_stalled";
const GET_OBJECT_STAGE_PATH_S3_HANDLER: &str = "s3_handler";
const GET_OBJECT_STAGE_REQUEST_INGRESS_TO_CONTEXT: &str = "request_ingress_to_context";
const GET_OBJECT_STAGE_OUTPUT_STRATEGY: &str = "output_strategy";
const GET_OBJECT_STAGE_BODY_BUILD: &str = "body_build";
const GET_OBJECT_STAGE_BODY_ENCRYPTED_BUFFER_READ: &str = "body_encrypted_buffer_read";
const GET_OBJECT_STAGE_BODY_MEMORY_BLOB: &str = "body_memory_blob";
const GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ: &str = "body_seek_buffer_read";
const GET_OBJECT_STAGE_BODY_STREAM_STRATEGY: &str = "body_stream_strategy";
const GET_OBJECT_STAGE_BODY_STREAMING_BLOB: &str = "body_streaming_blob";
const GET_OBJECT_STAGE_CHECKSUM_HEADERS: &str = "checksum_headers";
const GET_OBJECT_STAGE_LIFECYCLE_EXPIRATION: &str = "lifecycle_expiration";
const GET_OBJECT_STAGE_METADATA_FILTER: &str = "metadata_filter";
const PUT_OBJECT_STORE_WARN_THRESHOLD: Duration = Duration::from_secs(5);
const GET_OBJECT_STREAM_WARN_THRESHOLD: Duration = Duration::from_secs(5);
static GET_OBJECT_BUFFER_THRESHOLD_WARNED: AtomicBool = AtomicBool::new(false);

fn record_get_object_s3_handler_stage_duration(stage: &'static str, start: Option<std::time::Instant>) {
    if let Some(start) = start {
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_STAGE_PATH_S3_HANDLER,
            stage,
            start.elapsed().as_secs_f64(),
        );
    }
}

fn decoded_content_length_from_headers(headers: &HeaderMap) -> S3Result<Option<i64>> {
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
fn range_to_http_range_spec(range: Range) -> S3Result<HTTPRangeSpec> {
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

/// True when the request body actually arrived chunk-framed on the wire, i.e. the payload was
/// signed as a SigV4 streaming upload (`x-amz-content-sha256: STREAMING-*`). This is the only
/// case in which the auth layer de-frames the body; `Content-Encoding: aws-chunked` without a
/// streaming payload is just a declared encoding over an unframed body.
fn request_body_is_aws_chunked_framed(headers: &HeaderMap) -> bool {
    headers
        .get(AMZ_CONTENT_SHA256)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.len() >= 10 && value[..10].eq_ignore_ascii_case("STREAMING-"))
}

/// Map a bucket-quota checker outcome onto the S3 admission result.
///
/// Hard is the only supported quota type, so a checker fault (bucket-config read, config parse, or usage lookup) must fail closed rather than admit the write: allowing it would silently bypass a configured hard quota. The no-quota happy path never reaches the error arm — `QuotaChecker::check_quota` returns `Ok(allowed)` via the zero-extra-I/O fast path when no quota is configured, so failing closed here cannot penalise buckets without a quota. A fault surfaces as a retryable `ServiceUnavailable` and is counted; the client-facing message stays generic so internal config/usage details are not leaked.
fn map_quota_check_outcome(bucket: &str, outcome: Result<QuotaCheckResult, QuotaError>) -> S3Result<()> {
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
            warn!(bucket, error = %e, state = "checker_failed", "Bucket quota check failed closed");
            Err(S3Error::with_message(
                S3ErrorCode::ServiceUnavailable,
                "Bucket quota check temporarily unavailable, please retry".to_string(),
            ))
        }
        _ => Ok(()),
    }
}

fn request_uses_aws_chunked(headers: &HeaderMap) -> bool {
    let has_aws_chunked = |header_name: &str| {
        headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|part| part.trim().eq_ignore_ascii_case("aws-chunked")))
    };

    has_aws_chunked("content-encoding") || has_aws_chunked("transfer-encoding")
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

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

    fn register_if_enabled<F>(
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

struct GetObjectBootstrap {
    timeout_config: GetObjectTimeoutPolicy,
    wrapper: RequestTimeoutWrapper,
    request_start: std::time::Instant,
    request_guard: GetObjectGuard,
    _deadlock_request_guard: Option<DeadlockRequestGuard>,
    concurrent_requests: usize,
}

struct GetObjectIoPlanning {
    /// `None` when inline fast path skips disk I/O semaphore.
    disk_permit: Option<GetObjectDiskPermit>,
    permit_wait_duration: Duration,
    queue_status: concurrency::IoQueueStatus,
    queue_utilization: f64,
}

#[derive(Clone, Copy)]
struct GetObjectRequestTimeout<'a> {
    wrapper: &'a RequestTimeoutWrapper,
    policy: &'a GetObjectTimeoutPolicy,
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
    final_stream: DynReader,
    buffered_body: Option<Bytes>,
    /// ODC-16: `buffered_body` is the body the ecstore cache hook served, so the
    /// app layer serves it as the object-data-cache source without a re-lookup.
    cache_hook_served: bool,
    /// ODC-16: the cache hook probed this read (served or missed), so the app
    /// layer must skip its own lookup.
    cache_hook_probed: bool,
    cache_fill_allowed: bool,
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

struct GetObjectPreparedRead {
    io_planning: GetObjectIoPlanning,
    read_setup: GetObjectReadSetup,
}

struct GetObjectStrategyContext {
    #[allow(dead_code)]
    io_strategy: concurrency::IoStrategy,
    optimal_buffer_size: usize,
    enable_readahead: bool,
}

struct GetObjectOutputContext {
    output: GetObjectOutput,
    event_info: Option<ObjectInfo>,
    response_content_length: i64,
    optimal_buffer_size: usize,
    extra_checksum_headers: Vec<(&'static str, String)>,
}

enum GetObjectTimeoutStage {
    BeforeProcessing,
    DiskPermitWait { permit_wait_duration: Duration },
    BeforeRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetObjectStreamStrategy {
    Standard,
    LargeSequentialReadahead,
}

impl GetObjectStreamStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::LargeSequentialReadahead => "large_sequential_readahead",
        }
    }
}

const LARGE_SEQUENTIAL_GET_THRESHOLD_BYTES: i64 = 1024 * 1024 * 1024;
const LARGE_SEQUENTIAL_GET_STREAM_BUFFER_CAP_BYTES: usize = 4 * MI_B;
const LARGE_SEQUENTIAL_GET_READAHEAD_MULTIPLIER: usize = 2;
const LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES: usize = MI_B;
const LARGE_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES: i64 = 4 * MI_B as i64;
const ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE: &str = "RUSTFS_GET_SEEK_BUFFER_ENABLE";
const ENV_RUSTFS_GET_READER_STREAM_BUFFER_SIZE: &str = "RUSTFS_GET_READER_STREAM_BUFFER_SIZE";
const ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE: &str = "RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE";
const GET_READER_STREAM_BUFFER_SOURCE_SELECTED: &str = "selected";
const GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE: &str = "env_override";
const GET_READER_STREAM_POLL_PENDING: &str = "pending";
const GET_READER_STREAM_POLL_READY_DATA: &str = "ready_data";
const GET_READER_STREAM_POLL_READY_EMPTY: &str = "ready_empty";
const GET_READER_STREAM_POLL_READY_ERROR: &str = "ready_error";
const GET_MEMORY_BODY_SOURCE_BUFFERED_BODY: &str = "buffered_body";
const GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE: &str = "object_data_cache";
const GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE_MATERIALIZED: &str = "object_data_cache_materialized";
const GET_MEMORY_BODY_SOURCE_SEEK_BUFFER: &str = "seek_buffer";
const GET_MEMORY_BODY_SOURCE_ENCRYPTED_BUFFER: &str = "encrypted_buffer";
const GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ: &str = "body_cache_materialize_read";

fn get_reader_stream_buffer_size_override() -> Option<usize> {
    static GET_READER_STREAM_BUFFER_SIZE_OVERRIDE: OnceLock<Option<usize>> = OnceLock::new();
    *GET_READER_STREAM_BUFFER_SIZE_OVERRIDE.get_or_init(|| {
        std::env::var(ENV_RUSTFS_GET_READER_STREAM_BUFFER_SIZE)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
    })
}

fn is_get_output_handoff_attribution_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE, false))
}

fn is_get_seek_buffer_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE, false))
}

fn resolve_reader_stream_buffer_size(selected_size: usize, override_size: Option<usize>) -> (usize, &'static str) {
    if let Some(override_size) = override_size.filter(|value| *value > 0) {
        return (override_size, GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE);
    }

    (selected_size.max(1), GET_READER_STREAM_BUFFER_SOURCE_SELECTED)
}

fn tune_reader_stream_buffer_size(
    selected_size: usize,
    response_content_length: i64,
    stream_strategy: GetObjectStreamStrategy,
) -> usize {
    if stream_strategy == GetObjectStreamStrategy::Standard
        && response_content_length >= LARGE_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES
    {
        return selected_size.max(LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES);
    }

    selected_size
}

async fn enqueue_transitioned_delete_cleanup(
    store: Arc<ECStore>,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
    existing: Option<&ObjectInfo>,
) -> std::io::Result<()> {
    let Some(existing) = existing else {
        return Ok(());
    };
    let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Cleanup);

    let je = if opts.delete_prefix {
        tier_sweeper::transitioned_force_delete_journal_entry(&existing.transitioned_object)
    } else {
        let version_id = opts.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok());
        tier_sweeper::transitioned_delete_journal_entry(
            version_id,
            opts.versioned,
            opts.version_suspended,
            &existing.transitioned_object,
        )
    };
    let Some(mut je) = je else {
        return Ok(());
    };

    tier_delete_journal::record_tier_delete_journal_backend_identity(&mut je, &existing.user_defined)?;
    tier_delete_journal::persist_tier_delete_journal_entry(store, &je).await?;

    let expiry_state = current_expiry_state_handle();
    let mut expiry_state = expiry_state.write().await;
    if let Err(err) = expiry_state.enqueue_tier_journal_entry(&je) {
        warn!(
            bucket,
            object,
            remote_object = %existing.transitioned_object.name,
            remote_version_id = %existing.transitioned_object.version_id,
            tier = %existing.transitioned_object.tier,
            error = ?err,
            "transitioned object cleanup journal persisted but was not queued"
        );
    }
    Ok(())
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

struct MemoryTrackedBytesStream {
    bytes: Bytes,
    emitted: bool,
    completed: bool,
    expected: usize,
    /// Set when the materialized buffer length disagrees with the declared
    /// content length. Such a body would be truncated (short) or over-long
    /// relative to the already-committed `Content-Length`, so the stream must
    /// surface an error instead of a clean short/over-long body. See #1324.
    length_mismatch: bool,
    started: std::time::Instant,
    source: &'static str,
    _guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
    lifecycle: GetObjectBodyLifecycle,
}

#[derive(Default)]
struct GetObjectBodyLifecycle {
    request_guard: Option<GetObjectGuard>,
}

impl GetObjectBodyLifecycle {
    fn tracked(request_guard: GetObjectGuard) -> Self {
        Self {
            request_guard: Some(request_guard),
        }
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self { request_guard: None }
    }

    fn is_finished(&self) -> bool {
        self.request_guard.is_none()
    }

    fn finish_ok(&mut self) {
        if let Some(mut request_guard) = self.request_guard.take() {
            request_guard.finish_ok();
        }
    }

    fn finish_err(&mut self) {
        if let Some(mut request_guard) = self.request_guard.take() {
            request_guard.finish_err();
        }
    }
}

pin_project! {
    // Keep the disk-read admission permit tied to the response body. This is
    // intentionally conservative backpressure: a streaming GET should occupy a
    // read slot until the client drains or drops the body.
    struct DiskReadPermitReader<R> {
        #[pin]
        inner: R,
        disk_permit: Option<GetObjectDiskPermit>,
    }
}

impl<R> DiskReadPermitReader<R> {
    fn new(inner: R, disk_permit: GetObjectDiskPermit) -> Self {
        Self {
            inner,
            disk_permit: Some(disk_permit),
        }
    }
}

impl<R> AsyncRead for DiskReadPermitReader<R>
where
    R: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = this.inner.poll_read(cx, buf);
        // EOF: no more disk reads can happen through this stream, so release
        // the permit instead of holding it until the client drops the body.
        if had_capacity
            && matches!(poll, Poll::Ready(Ok(())))
            && buf.filled().len() == filled_before
            && let Some(mut disk_permit) = this.disk_permit.take()
        {
            disk_permit.release();
        }
        poll
    }
}

pin_project! {
    struct GetObjectReaderStream<R> {
        #[pin]
        inner: ReaderStream<R>,
        strategy: &'static str,
        buffer_source: &'static str,
        remaining: usize,
        emitted: usize,
        expected: usize,
    }
}

impl MemoryTrackedBytesStream {
    fn new(
        bytes: Bytes,
        expected: usize,
        source: &'static str,
        guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
        lifecycle: GetObjectBodyLifecycle,
    ) -> Self {
        let length_mismatch = bytes.len() != expected;
        Self {
            bytes,
            emitted: false,
            completed: !length_mismatch && expected == 0,
            expected,
            length_mismatch,
            started: std::time::Instant::now(),
            source,
            _guard: guard,
            lifecycle,
        }
    }

    fn finish_ok(&mut self) {
        self.completed = true;
        self.lifecycle.finish_ok();
    }

    fn finish_err(&mut self) {
        self.lifecycle.finish_err();
    }
}

impl<R> GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    fn new(reader: R, capacity: usize, remaining: usize, strategy: &'static str, buffer_source: &'static str) -> Self {
        if is_get_output_handoff_attribution_enabled() {
            rustfs_io_metrics::record_get_object_reader_stream_buffer_size(strategy, buffer_source, capacity);
        }
        Self {
            inner: ReaderStream::with_capacity(reader, capacity),
            strategy,
            buffer_source,
            remaining,
            emitted: 0,
            expected: remaining,
        }
    }
}

impl futures::Stream for MemoryTrackedBytesStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll_start = is_get_output_handoff_attribution_enabled().then(std::time::Instant::now);
        if this.emitted {
            if let Some(poll_start) = poll_start {
                rustfs_io_metrics::record_get_object_memory_body_stream_poll(
                    this.source,
                    GET_READER_STREAM_POLL_READY_EMPTY,
                    0,
                    poll_start.elapsed().as_secs_f64(),
                );
            }
            return Poll::Ready(None);
        }

        // Strict materialization guard (#1324): a body whose length disagrees
        // with the declared content length must fail the transfer rather than be
        // delivered as a clean short body (truncation) or an over-long body
        // (protocol violation). The HTTP layer has already committed to
        // `Content-Length == expected`, so there is no safe way to serve a
        // differently sized body. This is a defense-in-depth backstop; the
        // buffered/cache callers reject the mismatch before headers are sent.
        if this.length_mismatch {
            this.emitted = true;
            this.finish_err();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "materialized GET body length mismatch: expected {}, got {}",
                    this.expected,
                    this.bytes.len()
                ),
            ))));
        }

        let first_byte_elapsed = (!this.bytes.is_empty()).then(|| this.started.elapsed());
        this.emitted = true;
        if let Some(elapsed) = first_byte_elapsed {
            rustfs_io_metrics::record_get_object_first_byte_latency(GET_OBJECT_STAGE_PATH_S3_HANDLER, elapsed.as_secs_f64());
        }
        if this.bytes.len() >= this.expected {
            this.finish_ok();
        }
        if let Some(poll_start) = poll_start {
            rustfs_io_metrics::record_get_object_memory_body_stream_poll(
                this.source,
                GET_READER_STREAM_POLL_READY_DATA,
                this.bytes.len(),
                poll_start.elapsed().as_secs_f64(),
            );
        }
        Poll::Ready(Some(Ok(this.bytes.clone())))
    }
}

impl Drop for MemoryTrackedBytesStream {
    fn drop(&mut self) {
        if self.lifecycle.is_finished() {
            return;
        }

        if self.completed {
            self.finish_ok();
        } else {
            self.finish_err();
        }
    }
}

/// Failure modes of strictly materializing an object body into memory (#1324).
#[derive(Debug)]
enum StrictMaterializeError {
    /// The reader produced a different number of bytes than the declared content
    /// length (short or over-long). The response has already committed to
    /// `Content-Length == expected`, so any other length is an unrecoverable,
    /// broken HTTP response and must fail before headers are sent.
    LengthMismatch { expected: usize, actual: usize },
    /// A read error occurred after `consumed` bytes were already drained from the
    /// reader. The caller MUST NOT fall back to streaming the same reader: the
    /// drained prefix is gone, so streaming would ship a body missing its prefix
    /// (the seek-buffer prefix-misalignment bug this issue closes).
    Read { consumed: usize, source: std::io::Error },
}

impl std::fmt::Display for StrictMaterializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LengthMismatch { expected, actual, .. } => {
                write!(f, "materialized length mismatch: expected {expected}, got {actual}")
            }
            Self::Read { consumed, source } => {
                write!(f, "read failed after {consumed} bytes: {source}")
            }
        }
    }
}

impl StrictMaterializeError {
    fn into_storage_error(self) -> StorageError {
        match self {
            Self::LengthMismatch { expected, actual, .. } if actual < expected => StorageError::LessData,
            Self::LengthMismatch { .. } => StorageError::MoreData,
            Self::Read { source, .. } if source.kind() == std::io::ErrorKind::TimedOut => StorageError::Timeout,
            Self::Read { source, .. } => StorageError::Io(std::io::Error::new(source.kind(), "object body read failed")),
        }
    }

    fn into_s3_error(self, _response_content_length: i64) -> S3Error {
        ApiError::from(self.into_storage_error()).into()
    }
}

/// Strictly materialize an object body into memory, enforcing an exact-length
/// contract (#1324).
///
/// Reads at most `expected + 1` bytes so an over-long stream is detected without
/// buffering it unbounded, then requires `bytes_read == expected`. A short read
/// (clean EOF before `expected`), an over-long read, or a mid-stream read error
/// all return an error; only an exact-length read yields the buffer. Because the
/// HTTP response commits to `Content-Length == expected` before the body is
/// produced, this mirrors the streaming path (which already fails a short read
/// with `UnexpectedEof`) and the ODC materialize-fill path, closing the
/// warn-and-serve holes in the encrypted, seek, and cache memory branches.
///
/// On error the reader has already been (partially) consumed, so callers must
/// propagate the error rather than fall back to streaming the same reader.
async fn strict_materialize_object_body<R>(
    reader: R,
    expected: usize,
    stage: &'static str,
) -> Result<Vec<u8>, StrictMaterializeError>
where
    R: AsyncRead + Unpin,
{
    // Stop filling before the Vec reaches capacity. Calling `read_to_end` on a
    // bounded reader can still reserve beyond `expected` before observing EOF.
    // The over-long probe below stays outside this Vec so the admitted body
    // allocation remains exactly `expected` bytes.
    let mut buf = Vec::with_capacity(expected);
    let mut reader = reader;
    let read_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
    let read_result = loop {
        if buf.len() == expected {
            break Ok(());
        }
        match tokio::io::AsyncReadExt::read_buf(&mut reader, &mut buf).await {
            Ok(0) => break Ok(()),
            Ok(_) => {}
            Err(source) => break Err(source),
        }
    };
    let actual = buf.len();
    let probe_result = if read_result.is_ok() && actual == expected {
        let mut probe = [0_u8; 1];
        tokio::io::AsyncReadExt::read(&mut reader, &mut probe).await
    } else {
        Ok(0)
    };
    record_get_object_s3_handler_stage_duration(stage, read_start);
    match (read_result, probe_result) {
        (Ok(_), Ok(extra)) => {
            let actual = actual.saturating_add(extra);
            if actual == expected {
                Ok(buf)
            } else {
                Err(StrictMaterializeError::LengthMismatch { expected, actual })
            }
        }
        (Err(source), _) | (_, Err(source)) => Err(StrictMaterializeError::Read {
            consumed: actual,
            source,
        }),
    }
}

struct ColdFillProducerExecution {
    expected: usize,
    deadline: Option<tokio::time::Instant>,
    adapter: Arc<ObjectDataCacheAdapter>,
    engine_plan: rustfs_object_data_cache::ObjectDataCacheGetPlan,
}

enum ColdFillStartupWaitError {
    Cancelled,
    DeadlineExceeded,
}

async fn await_cold_fill_startup<F>(
    future: F,
    cancellation: &tokio_util::sync::CancellationToken,
    deadline: Option<tokio::time::Instant>,
) -> Result<F::Output, ColdFillStartupWaitError>
where
    F: Future,
{
    tokio::pin!(future);
    match deadline {
        Some(deadline) => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ColdFillStartupWaitError::Cancelled),
                result = tokio::time::timeout_at(deadline, &mut future) => {
                    result.map_err(|_| ColdFillStartupWaitError::DeadlineExceeded)
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ColdFillStartupWaitError::Cancelled),
                result = &mut future => Ok(result),
            }
        }
    }
}

async fn start_cold_fill_producer<AcquireIo, AcquireIoFuture, OpenReader, OpenReaderFuture>(
    producer: ColdFillProducer,
    reservation: Option<rustfs_object_data_cache::ObjectDataCacheBodyReservation>,
    acquire_io: AcquireIo,
    open_reader: OpenReader,
    execution: ColdFillProducerExecution,
) where
    AcquireIo: FnOnce() -> AcquireIoFuture,
    AcquireIoFuture: Future<Output = Result<GetObjectIoPlanning, ColdFillError>>,
    OpenReader: FnOnce() -> OpenReaderFuture,
    OpenReaderFuture: Future<Output = Result<GetObjectReader, StorageError>>,
{
    let ColdFillProducerExecution {
        expected,
        deadline,
        adapter,
        engine_plan,
    } = execution;
    let hard_deadline = tokio::time::Instant::now() + COLD_FILL_HARD_MAX_DURATION;
    let deadline = deadline.map_or(hard_deadline, |request_deadline| request_deadline.min(hard_deadline));
    let cancellation = producer.cancellation_token();
    let Some(reservation) = reservation else {
        producer.bypass();
        return;
    };
    let acquire = acquire_io();
    tokio::pin!(acquire);
    let producer_io = tokio::select! {
        _ = cancellation.cancelled() => {
            producer.finish(Err(StorageError::OperationCanceled));
            return;
        }
        result = tokio::time::timeout_at(deadline, &mut acquire) => match result {
            Ok(result) => result,
            Err(_) => {
                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                return;
            }
        }
    };
    let producer_io = match producer_io {
        Ok(io) => io,
        Err(err) => {
            producer.relinquish_or_finish(err);
            return;
        }
    };

    let open = open_reader();
    tokio::pin!(open);
    let reader = match tokio::select! {
        _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
        result = tokio::time::timeout_at(deadline, &mut open) => {
            result.unwrap_or(Err(StorageError::Timeout))
        }
    } {
        Ok(reader) => reader,
        Err(err) => {
            producer.relinquish_or_finish(ColdFillError::Storage(err));
            return;
        }
    };
    producer.mark_reader_started();
    let materialize = async move {
        let GetObjectReader {
            stream, buffered_body, ..
        } = reader;
        let body = if let Some(body) = buffered_body {
            if body.len() == expected {
                body
            } else {
                return Err(StorageError::other(format!(
                    "cold-fill buffered body length mismatch: expected {expected}, got {}",
                    body.len()
                )));
            }
        } else {
            let stream = if let Some(permit) = producer_io.disk_permit {
                wrap_reader(DiskReadPermitReader::new(stream, permit))
            } else {
                stream
            };
            Bytes::from(
                strict_materialize_object_body(stream, expected, GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ)
                    .await
                    .map_err(StrictMaterializeError::into_storage_error)?,
            )
        };
        Ok::<_, StorageError>((body, reservation))
    };
    let materialized = tokio::select! {
            _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
            result = tokio::time::timeout_at(deadline, materialize) => {
                result.unwrap_or(Err(StorageError::Timeout))
            }
    };
    let result = match materialized {
        Ok((body, reservation)) => {
            if cancellation.is_cancelled() {
                producer.finish(Err(StorageError::OperationCanceled));
                return;
            }
            if deadline <= tokio::time::Instant::now() {
                producer.finish(Err(StorageError::Timeout));
                return;
            }
            let reserved = reservation.wrap_bytes(body);
            let shared = reserved.bytes();
            let publish = async {
                #[cfg(test)]
                wait_cold_fill_publication_barrier(&engine_plan).await;
                adapter.fill_reserved_body(&engine_plan, reserved).await
            };
            tokio::pin!(publish);
            tokio::select! {
                _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
                _ = tokio::time::sleep_until(deadline) => {
                    Err(StorageError::Timeout)
                }
                _ = &mut publish => Ok(shared),
            }
        }
        Err(err) => Err(err),
    };
    producer.finish(result);
}

fn cold_fill_deadline(
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &GetObjectTimeoutPolicy,
    response_size: u64,
) -> Option<tokio::time::Instant> {
    if !timeout_config.is_timeout_enabled() {
        return None;
    }
    Some(tokio::time::Instant::now() + wrapper.remaining_time_for_size(Some(response_size)).unwrap_or(Duration::ZERO))
}

fn cold_fill_producer_deadline(timeout_config: &GetObjectTimeoutPolicy, response_size: u64) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let hard_deadline = now + COLD_FILL_HARD_MAX_DURATION;
    if timeout_config.is_timeout_enabled() {
        (now + timeout_config.calculate_timeout_for_size(response_size)).min(hard_deadline)
    } else {
        hard_deadline
    }
}

async fn lookup_cold_fill_second_chance(
    adapter: &ObjectDataCacheAdapter,
    plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
) -> Option<Bytes> {
    match adapter.peek_body_untracked(plan).await {
        rustfs_object_data_cache::ObjectDataCacheLookup::Hit(body) => Some(body),
        _ => None,
    }
}

fn retain_cold_fill_producer_for_matching_plan(
    producer: ColdFillProducer,
    current: &GetObjectBodyCachePlan,
    expected: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
) -> Option<ColdFillProducer> {
    if current == &GetObjectBodyCachePlan::Cacheable(expected.clone()) {
        Some(producer)
    } else {
        producer.bypass();
        None
    }
}

impl<R> futures::Stream for GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    type Item = Result<Bytes, S3StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.remaining == 0 {
            return Poll::Ready(None);
        }

        let remaining_before = *this.remaining;
        let poll_start = std::time::Instant::now();
        let result: Poll<Option<Self::Item>> = match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(mut bytes))) => {
                if bytes.len() > *this.remaining {
                    bytes.truncate(*this.remaining);
                }
                *this.remaining -= bytes.len();
                #[cfg(feature = "tracing-chunk-debug")]
                {
                    *this.emitted += bytes.len();
                    tracing::debug!(
                        emitted = *this.emitted,
                        expected = *this.expected,
                        chunk_len = bytes.len(),
                        "GetObject ReaderStream emitted bytes"
                    );
                }
                if bytes.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(bytes)))
                }
            }
            Poll::Ready(Some(Err(err))) => {
                #[cfg(feature = "tracing-chunk-debug")]
                tracing::error!(
                    emitted = *this.emitted,
                    expected = *this.expected,
                    error = %err,
                    "GetObject ReaderStream returned error"
                );
                Poll::Ready(Some(Err(Box::new(err) as S3StdError)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        };

        let emitted_bytes = match &result {
            Poll::Ready(Some(Ok(bytes))) => bytes.len(),
            _ => 0,
        };
        let outcome = match &result {
            Poll::Ready(Some(Ok(bytes))) if !bytes.is_empty() => GET_READER_STREAM_POLL_READY_DATA,
            Poll::Ready(Some(Ok(_))) | Poll::Ready(None) => GET_READER_STREAM_POLL_READY_EMPTY,
            Poll::Ready(Some(Err(_))) => GET_READER_STREAM_POLL_READY_ERROR,
            Poll::Pending => GET_READER_STREAM_POLL_PENDING,
        };
        if is_get_output_handoff_attribution_enabled() {
            rustfs_io_metrics::record_get_object_reader_stream_poll(
                this.strategy,
                this.buffer_source,
                outcome,
                remaining_before,
                emitted_bytes,
                poll_start.elapsed().as_secs_f64(),
            );
        }

        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<R> ByteStream for GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::new_exact(self.remaining)
    }
}

struct GetObjectStreamingReader<R> {
    inner: R,
    bucket: String,
    key: String,
    expected: usize,
    emitted: usize,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    started: std::time::Instant,
    first_byte_reported: bool,
    completed: bool,
    lifecycle: GetObjectBodyLifecycle,
    _foreground_read_guard: rustfs_scanner::ForegroundReadGuard,
}

impl<R> GetObjectStreamingReader<R> {
    fn new(inner: R, bucket: &str, key: &str, expected: usize, timeout: Duration, lifecycle: GetObjectBodyLifecycle) -> Self {
        Self {
            inner,
            bucket: bucket.to_string(),
            key: key.to_string(),
            expected,
            emitted: 0,
            timeout,
            timer: None,
            started: std::time::Instant::now(),
            first_byte_reported: false,
            completed: expected == 0,
            lifecycle,
            _foreground_read_guard: rustfs_scanner::ForegroundReadGuard::new(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn finish_ok(&mut self) {
        self.completed = true;
        self.lifecycle.finish_ok();
    }

    fn finish_err(&mut self) {
        self.lifecycle.finish_err();
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for GetObjectStreamingReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();

        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                self.timer = None;
                let produced = buf.filled().len().saturating_sub(filled_before);
                if produced > 0 {
                    self.emitted = self.emitted.saturating_add(produced);
                    if !self.first_byte_reported {
                        self.first_byte_reported = true;
                        let elapsed = self.elapsed();
                        rustfs_io_metrics::record_get_object_first_byte_latency(
                            GET_OBJECT_STAGE_PATH_S3_HANDLER,
                            elapsed.as_secs_f64(),
                        );
                        if elapsed >= GET_OBJECT_STREAM_WARN_THRESHOLD {
                            warn!(
                                event = EVENT_GET_OBJECT_STREAM_BODY,
                                component = LOG_COMPONENT_APP,
                                subsystem = LOG_SUBSYSTEM_OBJECT,
                                bucket = %self.bucket,
                                object = %self.key,
                                expected = self.expected,
                                emitted = self.emitted,
                                elapsed_ms = elapsed.as_millis(),
                                state = "first_byte_slow",
                                "GetObject streaming body first byte was slow"
                            );
                        }
                    }
                    if self.emitted >= self.expected {
                        self.completed = true;
                        self.finish_ok();
                    }
                } else if self.emitted < self.expected {
                    warn!(
                        event = EVENT_GET_OBJECT_STREAM_BODY,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        bucket = %self.bucket,
                        object = %self.key,
                        expected = self.expected,
                        emitted = self.emitted,
                        elapsed_ms = self.elapsed().as_millis(),
                        state = "short_eof",
                        "GetObject streaming body ended before expected length"
                    );
                    self.finish_err();
                    // The inner reader signalled a clean EOF before delivering the full
                    // Content-Length. Returning Ok here would hand the client a truncated body
                    // under a full Content-Length: the peer treats the short body as complete
                    // (e.g. `mc mirror` writes a short file and considers it done — the
                    // "incomplete data mirroring" in issue #2955). Surface an error instead so
                    // the transfer fails loudly and the client retries rather than persisting
                    // truncated data.
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "get object streaming body ended before the expected content length",
                    )));
                } else {
                    self.completed = true;
                    self.finish_ok();
                }

                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => {
                self.timer = None;
                self.finish_err();
                warn!(
                    event = EVENT_GET_OBJECT_STREAM_BODY,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    bucket = %self.bucket,
                    object = %self.key,
                    expected = self.expected,
                    emitted = self.emitted,
                    elapsed_ms = self.elapsed().as_millis(),
                    state = "read_failed",
                    error = %err,
                    "GetObject streaming body read failed"
                );
                Poll::Ready(Err(err))
            }
            Poll::Pending => {
                if self.timeout.is_zero() {
                    return Poll::Pending;
                }

                if self.timer.is_none() {
                    self.timer = Some(Box::pin(tokio::time::sleep(self.timeout)));
                }

                if let Some(timer) = self.timer.as_mut()
                    && std::future::Future::poll(timer.as_mut(), cx).is_ready()
                {
                    self.timer = None;
                    warn!(
                        event = EVENT_GET_OBJECT_STREAM_BODY,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        bucket = %self.bucket,
                        object = %self.key,
                        expected = self.expected,
                        emitted = self.emitted,
                        elapsed_ms = self.elapsed().as_millis(),
                        timeout_ms = self.timeout.as_millis(),
                        state = "stall_timeout",
                        "GetObject streaming body stalled"
                    );
                    self.finish_err();
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "get object streaming body stall timeout",
                    )));
                }

                Poll::Pending
            }
        }
    }
}

impl<R> Drop for GetObjectStreamingReader<R> {
    fn drop(&mut self) {
        if self.lifecycle.is_finished() {
            return;
        }

        if self.expected == 0 || self.completed || self.emitted >= self.expected {
            self.finish_ok();
            return;
        }

        self.finish_err();
        warn!(
            event = EVENT_GET_OBJECT_STREAM_BODY,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            bucket = %self.bucket,
            object = %self.key,
            expected = self.expected,
            emitted = self.emitted,
            elapsed_ms = self.elapsed().as_millis(),
            state = "dropped_incomplete",
            "GetObject streaming body dropped before expected length"
        );
    }
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
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        match this.inner.poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let filled = &buf.filled()[before..];
                if !filled.is_empty() {
                    this.md5.consume(filled);
                } else if !*this.finished {
                    *this.finished = true;
                    if let Ok(mut etag) = this.etag.lock() {
                        *etag = Some(format!("{:x}", this.md5.clone().finalize()));
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
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

fn has_put_sse_request_headers(headers: &HeaderMap) -> bool {
    headers.get(AMZ_SERVER_SIDE_ENCRYPTION).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID).is_some()
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

async fn read_small_put_body_exact_pooled<R>(mut body: R, size: usize, pool: &BytesPool) -> S3Result<PooledBuffer>
where
    R: AsyncRead + Unpin,
{
    let mut buf = pool.acquire_buffer(size).await;
    buf.resize(size, 0);
    let mut filled = 0;

    while filled < size {
        let read = tokio::io::AsyncReadExt::read(&mut body, &mut buf[filled..size])
            .await
            .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
        if read == 0 {
            return Err(s3_error!(IncompleteBody));
        }
        filled += read;
    }

    let mut extra = [0u8; 1];
    let extra_read = tokio::io::AsyncReadExt::read(&mut body, &mut extra)
        .await
        .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
    if extra_read != 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(buf)
}

/// Read small PUT body into a directly-allocated buffer, bypassing BytesPool.
/// Used for objects ≤4KiB where pool contention under high concurrency
/// outweighs the allocation cost.
async fn read_small_put_body_exact_direct<R>(mut body: R, size: usize) -> S3Result<std::io::Cursor<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; size];
    let mut filled = 0;

    while filled < size {
        let read = tokio::io::AsyncReadExt::read(&mut body, &mut buf[filled..size])
            .await
            .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
        if read == 0 {
            return Err(s3_error!(IncompleteBody));
        }
        filled += read;
    }

    let mut extra = [0u8; 1];
    let extra_read = tokio::io::AsyncReadExt::read(&mut body, &mut extra)
        .await
        .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
    if extra_read != 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(std::io::Cursor::new(buf))
}

async fn read_zero_copy_put_body_exact<S, E>(mut body: S, size: usize) -> S3Result<ChunkedBytesReader>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut chunks = Vec::new();
    let mut filled = 0usize;

    while filled < size {
        let Some(chunk) = body.next().await else {
            return Err(s3_error!(IncompleteBody));
        };
        let chunk = chunk.map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
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
        let chunk = chunk.map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
        if !chunk.is_empty() {
            return Err(s3_error!(UnexpectedContent));
        }
    }

    Ok(ChunkedBytesReader::new(chunks))
}

pub(crate) fn object_seek_support_threshold() -> usize {
    static OBJECT_SEEK_SUPPORT_THRESHOLD: OnceLock<usize> = OnceLock::new();
    *OBJECT_SEEK_SUPPORT_THRESHOLD.get_or_init(|| {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_SEEK_SUPPORT_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD,
        )
    })
}

fn object_seek_support_concurrency_thresholds() -> (usize, usize) {
    static OBJECT_SEEK_SUPPORT_CONCURRENCY_THRESHOLDS: OnceLock<(usize, usize)> = OnceLock::new();
    *OBJECT_SEEK_SUPPORT_CONCURRENCY_THRESHOLDS.get_or_init(|| {
        let medium = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        )
        .max(1);
        let high = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        )
        .max(medium + 1);
        (medium, high)
    })
}

fn concurrency_aware_seek_support_threshold(configured_threshold: i64, concurrent_requests: usize) -> i64 {
    let (medium_threshold, high_threshold) = object_seek_support_concurrency_thresholds();
    let effective_threshold = configured_threshold.min(MAX_GET_OBJECT_MEMORY_BUFFER_BYTES);

    if concurrent_requests >= high_threshold.saturating_mul(2) {
        return effective_threshold.min(VERY_HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }
    if concurrent_requests >= high_threshold {
        return effective_threshold.min(HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }
    if concurrent_requests >= medium_threshold {
        return effective_threshold.min(MEDIUM_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }

    effective_threshold
}

fn should_buffer_get_object_in_memory(
    info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    concurrent_requests: usize,
) -> bool {
    let configured_threshold = object_seek_support_threshold() as i64;
    should_buffer_get_object_in_memory_with_threshold(
        info,
        response_content_length,
        part_number,
        has_range,
        configured_threshold,
        concurrent_requests,
        is_get_seek_buffer_enabled(),
    )
}

fn should_materialize_get_object_body_for_cache(
    info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    concurrent_requests: usize,
) -> bool {
    let configured_threshold = object_seek_support_threshold() as i64;
    should_buffer_get_object_in_memory_with_threshold(
        info,
        response_content_length,
        part_number,
        has_range,
        configured_threshold,
        concurrent_requests,
        true,
    )
}

fn should_buffer_get_object_in_memory_with_threshold(
    _info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    configured_threshold: i64,
    concurrent_requests: usize,
    seek_buffer_enabled: bool,
) -> bool {
    if !seek_buffer_enabled || part_number.is_some() || has_range || response_content_length <= 0 || configured_threshold <= 0 {
        return false;
    }
    if usize::try_from(response_content_length).is_err() {
        return false;
    }

    let effective_threshold = concurrency_aware_seek_support_threshold(configured_threshold, concurrent_requests);
    if configured_threshold > MAX_GET_OBJECT_MEMORY_BUFFER_BYTES
        && GET_OBJECT_BUFFER_THRESHOLD_WARNED
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            configured_threshold_bytes = configured_threshold,
            hard_limit_bytes = MAX_GET_OBJECT_MEMORY_BUFFER_BYTES,
            "RUSTFS_OBJECT_SEEK_SUPPORT_THRESHOLD exceeds safety cap; using capped in-memory buffer threshold"
        );
    }

    if response_content_length > effective_threshold {
        return false;
    }

    true
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

async fn maybe_enqueue_transition_immediate(obj_info: &ObjectInfo, src: LcEventSrc) {
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

/// Checksums resolved from stored (decrypted) metadata for a response. The five
/// s3s-typed algorithms fill named fields; the AWS 2026-04 additional algorithms
/// (XXHash3/64/128, SHA-512, MD5), which s3s has no typed `*Output` field for, land
/// in `extra` and are emitted as raw response headers via
/// [`inject_additional_checksum_headers`]. Built by [`classify_response_checksums`]
/// so the typed/extra split lives in exactly one place.
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

/// Split decrypted checksum (key, value) pairs into the five s3s-typed fields and the
/// additional-algorithm `extra` headers. Single source of truth for every response
/// path (GetObject / HeadObject / GetObjectAttributes / CompleteMultipartUpload),
/// replacing what used to be five copies of this match loop.
pub(crate) fn classify_response_checksums<I>(pairs: I) -> ResponseChecksums
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
    c
}

#[derive(Default)]
struct PutObjectChecksums {
    crc32: Option<String>,
    crc32c: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
    crc64nvme: Option<String>,
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

async fn enrich_delete_replication_state_if_needed(
    bucket: &str,
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
        set_deleted_object_replication_state(delete_object, &local_state);
    }
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

fn internal_object_info_lookup_opts(mut opts: ObjectOptions) -> ObjectOptions {
    opts.http_preconditions = None;
    opts
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

fn delete_replication_state_source<'a>(
    opts: &ObjectOptions,
    existing_object_info: Option<&'a ObjectInfo>,
    deleted_object_info: &'a ObjectInfo,
) -> &'a ObjectInfo {
    if should_use_existing_delete_replication_source(
        opts.replication_request,
        deleted_object_info.delete_marker,
        existing_object_info.is_some(),
    ) && let Some(existing) = existing_object_info
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

/// Validates that an archive entry path does not escape the target bucket.
///
/// Delegates to [`rustfs_utils::path::validate_extract_relative_path`] and wraps
/// the result as an S3 error on failure.
pub fn validate_extract_relative_path(path: &str) -> S3Result<()> {
    rustfs_utils::path::validate_extract_relative_path(path).map_err(|msg| s3_error!(InvalidArgument, "{msg}"))
}

fn normalize_snowball_prefix(prefix: &str) -> S3Result<Option<String>> {
    let normalized = prefix.trim().trim_matches('/');
    if normalized.is_empty() {
        return Ok(None);
    }

    validate_extract_relative_path(normalized)?;

    Ok(Some(normalized.to_string()))
}

/// Normalizes an archive entry key by applying a prefix, trimming slashes,
/// and ensuring directory entries end with `/`.
///
/// Delegates to [`rustfs_utils::path::normalize_extract_entry_key`] and wraps
/// the result as an S3 error on failure.
pub fn normalize_extract_entry_key(path: &str, prefix: Option<&str>, is_dir: bool) -> S3Result<String> {
    rustfs_utils::path::normalize_extract_entry_key(path, prefix, is_dir).map_err(|msg| s3_error!(InvalidArgument, "{msg}"))
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

fn insert_expires_metadata(metadata: &mut HashMap<String, String>, expires: Option<&Timestamp>) -> S3Result<()> {
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
fn apply_standard_object_metadata(
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

fn response_storage_class(info: &ObjectInfo, metadata: &HashMap<String, String>) -> Option<StorageClass> {
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

fn response_storage_class_for_object_attributes(
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

// Shared across Object Lock validation paths to keep the client-facing
// InvalidRequest message consistent.
pub(crate) const ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED: &str =
    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied";

pub(crate) async fn build_put_like_object_lock_metadata(
    bucket: &str,
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

    validate_bucket_object_lock_enabled(bucket).await?;

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

pub(crate) fn validate_existing_object_lock_for_write(existing_obj_info: &ObjectInfo, opts: &ObjectOptions) -> S3Result<()> {
    if put_like_write_creates_new_version(opts) {
        return Ok(());
    }

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
/// - the replication delete decision is only consulted when the bucket has
///   active replication rules for the batch (`replicate_deletes == false`);
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
    replicate_deletes: bool,
    opts: &ObjectOptions,
    accounting_creates_delete_marker: bool,
) -> bool {
    !bucket_lock_enabled && !replicate_deletes && delete_creates_delete_marker(opts) && accounting_creates_delete_marker
}

fn resolve_put_object_extract_options(headers: &HeaderMap) -> S3Result<PutObjectExtractOptions> {
    let prefix = snowball_meta_value(headers, SNOWBALL_PREFIX_HEADER_KEYS, SNOWBALL_PREFIX_SUFFIX_LOWER)
        .map(|value| normalize_snowball_prefix(&value))
        .transpose()?
        .flatten();
    let ignore_dirs = snowball_meta_flag(headers, SNOWBALL_IGNORE_DIRS_HEADER_KEYS, SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER);
    let ignore_errors = snowball_meta_flag(headers, SNOWBALL_IGNORE_ERRORS_HEADER_KEYS, SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER);

    Ok(PutObjectExtractOptions {
        prefix,
        ignore_dirs,
        ignore_errors,
    })
}

fn put_object_extract_limits() -> ArchiveLimits {
    ArchiveLimits::default()
}

fn put_object_extract_quota_exceeded(current_usage: u64, quota_limit: u64) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InvalidRequest,
        format!("Bucket quota exceeded. Current usage: {current_usage} bytes, limit: {quota_limit} bytes"),
    )
}

fn validate_put_object_extract_entry_count(count: usize, limits: ArchiveLimits) -> S3Result<()> {
    if count > limits.max_entries {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry count exceeds limit: count={}, limit={}",
            count,
            limits.max_entries
        ));
    }
    Ok(())
}

fn validate_put_object_extract_entry_size(path: &str, size: u64, limits: ArchiveLimits) -> S3Result<()> {
    if size > limits.max_entry_size {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry size exceeds limit for {}: size={}, limit={}",
            path,
            size,
            limits.max_entry_size
        ));
    }
    Ok(())
}

fn validate_put_object_extract_total_size(total_size: u64, limits: ArchiveLimits) -> S3Result<()> {
    if total_size > limits.max_total_unpacked_size {
        return Err(s3_error!(
            InvalidArgument,
            "Archive total unpacked size exceeds limit: size={}, limit={}",
            total_size,
            limits.max_total_unpacked_size
        ));
    }
    Ok(())
}

fn validate_put_object_extract_entry_path(path: &str, limits: ArchiveLimits) -> S3Result<()> {
    if path.len() > limits.max_path_length {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry path exceeds limit for {}: length={}, limit={}",
            path,
            path.len(),
            limits.max_path_length
        ));
    }
    Ok(())
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
struct IoQueueCongestionWarnThrottle {
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
    fn claim(&self, now_ms: u64) -> Option<u64> {
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
    fn now_ms() -> u64 {
        static ANCHOR: OnceLock<std::time::Instant> = OnceLock::new();
        ANCHOR.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64
    }
}

static IO_QUEUE_CONGESTION_WARN_THROTTLE: IoQueueCongestionWarnThrottle = IoQueueCongestionWarnThrottle::new();

#[derive(Clone, Default)]
pub struct DefaultObjectUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultObjectUsecase {
    fn should_use_large_put_concurrency_tuning(size: i64) -> bool {
        size >= DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES
    }

    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: current_app_context(),
        }
    }

    /// Build the use-case bound to an explicit application context
    /// (backlog#1052 S6): the per-server request path passes its own context
    /// so the use-case resolves that server's store; `None` falls back to the
    /// ambient default.
    pub fn with_context(context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>) -> Self {
        Self { context }
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

    fn base_buffer_size(&self) -> usize {
        self.context
            .clone()
            .or_else(current_app_context)
            .map(|context| context.buffer_config().get().base_config.default_unknown)
            .unwrap_or_else(|| RustFSBufferConfig::default().base_config.default_unknown)
    }

    async fn check_bucket_quota(&self, bucket: &str, op: QuotaOperation, size: u64) -> S3Result<()> {
        let Some(metadata_sys) = self.bucket_metadata_sys() else {
            return Ok(());
        };
        let quota_checker = QuotaChecker::new(metadata_sys);
        map_quota_check_outcome(bucket, quota_checker.check_quota(bucket, op, size).await)
    }

    fn build_memory_bytes_blob(
        bytes: Bytes,
        response_content_length: i64,
        source: &'static str,
        lifecycle: GetObjectBodyLifecycle,
    ) -> StreamingBlob {
        let get_stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let memory_blob_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let handoff_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let bytes_len = bytes.len();
        let guard = rustfs_io_metrics::track_get_object_buffered_bytes(bytes_len);
        let remaining = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
        let blob = StreamingBlob::wrap(bytes_stream(
            MemoryTrackedBytesStream::new(bytes, remaining, source, guard, lifecycle),
            remaining,
        ));
        if let Some(handoff_start) = handoff_start {
            rustfs_io_metrics::record_get_object_response_handoff(
                "single_chunk",
                source,
                bytes_len,
                response_content_length,
                handoff_start.elapsed().as_secs_f64(),
            );
        }
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_MEMORY_BLOB, memory_blob_start);
        blob
    }

    fn build_memory_blob(
        buf: Vec<u8>,
        response_content_length: i64,
        source: &'static str,
        lifecycle: GetObjectBodyLifecycle,
    ) -> StreamingBlob {
        Self::build_memory_bytes_blob(Bytes::from(buf), response_content_length, source, lifecycle)
    }

    fn select_stream_buffer_strategy(
        response_content_length: i64,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        has_range: bool,
    ) -> (usize, GetObjectStreamStrategy) {
        if enable_readahead && !has_range && response_content_length >= LARGE_SEQUENTIAL_GET_THRESHOLD_BYTES {
            let expanded_buffer_size = optimal_buffer_size
                .saturating_mul(LARGE_SEQUENTIAL_GET_READAHEAD_MULTIPLIER)
                .min(LARGE_SEQUENTIAL_GET_STREAM_BUFFER_CAP_BYTES)
                .max(optimal_buffer_size);
            return (expanded_buffer_size, GetObjectStreamStrategy::LargeSequentialReadahead);
        }

        (optimal_buffer_size, GetObjectStreamStrategy::Standard)
    }

    fn build_reader_blob<R>(
        reader: R,
        response_content_length: i64,
        stream_buffer_size: usize,
        stream_strategy: GetObjectStreamStrategy,
        bucket: &str,
        key: &str,
        lifecycle: GetObjectBodyLifecycle,
    ) -> StreamingBlob
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        let streaming_blob_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
        let tuned_stream_buffer_size =
            tune_reader_stream_buffer_size(stream_buffer_size, response_content_length, stream_strategy);
        let (stream_buffer_size, buffer_source) =
            resolve_reader_stream_buffer_size(tuned_stream_buffer_size, get_reader_stream_buffer_size_override());
        let get_stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        if get_stage_metrics_enabled {
            rustfs_io_metrics::record_get_object_stream_strategy(
                stream_strategy.as_str(),
                stream_buffer_size,
                response_content_length,
            );
        }
        let handoff_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let reader = GetObjectStreamingReader::new(reader, bucket, key, expected, get_object_disk_read_timeout(), lifecycle);
        let stream = GetObjectReaderStream::new(reader, stream_buffer_size, expected, stream_strategy.as_str(), buffer_source);
        let blob = StreamingBlob::new(stream);
        if let Some(handoff_start) = handoff_start {
            rustfs_io_metrics::record_get_object_response_handoff(
                stream_strategy.as_str(),
                buffer_source,
                stream_buffer_size,
                response_content_length,
                handoff_start.elapsed().as_secs_f64(),
            );
        }
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAMING_BLOB, streaming_blob_start);
        blob
    }

    fn init_get_object_bootstrap(bucket: &str, key: &str, request_id: &str) -> S3Result<GetObjectBootstrap> {
        let timeout_config = GetObjectTimeoutPolicy::cached_from_env();
        let wrapper = RequestTimeoutWrapper::with_request_id(timeout_config.clone(), request_id.to_string());
        let request_start = std::time::Instant::now();
        let request_guard = ConcurrencyManager::track_request();
        let concurrent_requests = GetObjectGuard::concurrent_requests();

        let deadlock_detector = deadlock_detector::get_deadlock_detector();
        let deadlock_request_guard = DeadlockRequestGuard::register_if_enabled(deadlock_detector, wrapper.request_id(), || {
            format!("GetObject {bucket}/{key}")
        });

        Self::ensure_get_object_not_timed_out(&wrapper, &timeout_config, bucket, key, GetObjectTimeoutStage::BeforeProcessing)?;

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

    fn validate_get_object_part_number(part_number: Option<usize>, info: &ObjectInfo) -> S3Result<()> {
        if let Some(part_number) = part_number
            && part_number > 1
            && !info.parts.iter().any(|part| part.number == part_number)
        {
            return Err(s3_error!(InvalidPart));
        }
        Ok(())
    }

    fn validate_get_object_before_cold_fill(headers: &HeaderMap, part_number: Option<usize>, info: &ObjectInfo) -> S3Result<()> {
        check_preconditions(headers, info)?;
        Self::validate_get_object_part_number(part_number, info)
    }

    /// How long a GET waits for a disk read permit before degrading to a
    /// permit-less read. Cached: consulted per GET. Zero disables the bound.
    fn disk_permit_wait_timeout() -> Duration {
        static CACHED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            Duration::from_secs(rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_DISK_PERMIT_WAIT_TIMEOUT,
                rustfs_config::DEFAULT_OBJECT_DISK_PERMIT_WAIT_TIMEOUT,
            ))
        })
    }

    async fn acquire_get_object_io_planning(
        manager: &ConcurrencyManager,
        request_timeout: Option<GetObjectRequestTimeout<'_>>,
        bucket: &str,
        key: &str,
    ) -> S3Result<GetObjectIoPlanning> {
        let permit_wait_start = std::time::Instant::now();
        let permit_wait_timeout = Self::disk_permit_wait_timeout();
        // Permits are held for the whole body transfer, so slow clients can pin
        // all of them while disks are idle. Bound the wait on the primary pool
        // and, on timeout, admit from a bounded degraded overflow lane. Total
        // concurrent disk-active GETs are hard-capped at
        // `primary_cap + degraded_cap`; once that cap is reached we reject with
        // `SlowDown` instead of reading without any admission token. Never
        // proceed permit-less.
        let disk_permit = match manager
            .admit_disk_read(permit_wait_timeout)
            .await
            .map_err(|_| s3_error!(InternalError, "disk read semaphore closed"))?
        {
            DiskReadAdmission::Primary(permit) => Some(permit),
            // Throttling disabled by config (primary cap 0): proceed without an
            // admission token. Not a saturation bypass.
            DiskReadAdmission::Unbounded => None,
            DiskReadAdmission::Degraded(permit) => {
                metrics::counter!("rustfs.get_object.disk_permit.degraded.total").increment(1);
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_start.elapsed().as_millis() as u64,
                    "GetObject admitted into bounded degraded disk-read lane after primary pool saturation"
                );
                Some(permit)
            }
            DiskReadAdmission::Rejected => {
                metrics::counter!("rustfs.get_object.disk_permit.hard_reject.total").increment(1);
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_start.elapsed().as_millis() as u64,
                    "GetObject rejected: disk-read hard concurrency cap reached"
                );
                return Err(s3_error!(
                    SlowDown,
                    "disk read concurrency limit reached, please reduce your request rate"
                ));
            }
        };
        let permit_wait_duration = permit_wait_start.elapsed();

        if let Some(timeout) = request_timeout {
            Self::ensure_get_object_not_timed_out(
                timeout.wrapper,
                timeout.policy,
                bucket,
                key,
                GetObjectTimeoutStage::DiskPermitWait { permit_wait_duration },
            )?;
        }

        let queue_status = manager.io_queue_status();
        let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
            queue_status.total_permits,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        );
        let queue_utilization = queue_snapshot.utilization_percent();

        if queue_snapshot.is_congested(80.0) {
            // Metrics count every congested request; only the WARN is rate
            // limited, because under saturation every GET crosses the
            // threshold and per-request WARNs flood the log.
            rustfs_io_metrics::record_io_queue_congestion();

            if let Some(suppressed_warns) = IO_QUEUE_CONGESTION_WARN_THROTTLE.claim(IoQueueCongestionWarnThrottle::now_ms()) {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    queue_utilization = format!("{:.1}%", queue_utilization),
                    permits_in_use = queue_status.permits_in_use,
                    total_permits = queue_status.total_permits,
                    suppressed_warns,
                    "I/O queue congestion detected"
                );
            }
        }

        if let Some(timeout) = request_timeout {
            Self::ensure_get_object_not_timed_out(
                timeout.wrapper,
                timeout.policy,
                bucket,
                key,
                GetObjectTimeoutStage::BeforeRead,
            )?;
        }

        Ok(GetObjectIoPlanning {
            disk_permit: disk_permit.map(GetObjectDiskPermit::new),
            permit_wait_duration,
            queue_status,
            queue_utilization,
        })
    }

    async fn acquire_cold_fill_io_planning(
        manager: &'static ConcurrencyManager,
        bucket: &str,
        key: &str,
    ) -> Result<GetObjectIoPlanning, ColdFillError> {
        match Self::acquire_get_object_io_planning(manager, None, bucket, key).await {
            Ok(io) => Ok(io),
            Err(err) if err.code() == &S3ErrorCode::SlowDown => Err(ColdFillError::Storage(StorageError::SlowDown)),
            Err(_) => Err(ColdFillError::DiskAdmissionClosed),
        }
    }

    fn get_object_io_planning_without_disk(manager: &ConcurrencyManager) -> GetObjectIoPlanning {
        let queue_status = manager.io_queue_status();
        let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
            queue_status.total_permits,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        );
        GetObjectIoPlanning {
            disk_permit: None,
            permit_wait_duration: Duration::ZERO,
            queue_utilization: queue_snapshot.utilization_percent(),
            queue_status,
        }
    }

    async fn prepare_get_object_request_context(req: &S3Request<GetObjectInput>) -> S3Result<GetObjectRequestContext> {
        // Clone only the fields this path needs instead of the whole input.
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let version_id = req.input.version_id.clone();
        let part_number = req.input.part_number;
        let range = req.input.range;

        validate_object_key(&key, "GET")?;

        let part_number = parse_part_number_i32_to_usize(part_number, "GET")?;

        let rs = range.map(range_to_http_range_spec).transpose()?;

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
    async fn prepare_get_object_read_execution(
        &self,
        req: &S3Request<GetObjectInput>,
        manager: &'static ConcurrencyManager,
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        bucket: &str,
        key: &str,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
        part_number: Option<usize>,
    ) -> S3Result<GetObjectPreparedRead> {
        // SF05: Store lookup first (cached via SF01 moka cache).
        let store_lookup_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let store = get_validated_store(bucket).await?;
        if let Some(store_lookup_start) = store_lookup_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "store_lookup",
                store_lookup_start.elapsed().as_secs_f64(),
            );
        }

        let read_start = std::time::Instant::now();
        let read_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then_some(read_start);
        let cache_adapter = self.object_data_cache();
        if cache_adapter.is_disabled() || !cache_adapter.materialize_fill_enabled() {
            let io_planning = Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?;
            let reader = store
                .get_object_reader(bucket, key, rs.clone(), req.headers.clone(), opts)
                .await
                .map_err(map_get_object_reader_error)?;
            let read_setup =
                Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
            return Ok(GetObjectPreparedRead { io_planning, read_setup });
        }

        // Preserve the legacy metadata-fanout bound without making followers
        // hold a body-transfer permit while they wait on the cold-fill session.
        let mut metadata_admission = Some(
            Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?,
        );
        let mut prepared = Some(
            store
                .prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts)
                .await
                .map_err(map_get_object_reader_error)?,
        );
        let mut cache_fill_allowed = true;
        let mut legacy_hook_missed = false;
        'snapshot: {
            let info = prepared
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "prepared metadata snapshot is unavailable"))?
                .object_info();
            // Preconditions, cache planning, and the authoritative hook lookup all
            // run against one namespace-locked metadata snapshot. Cacheable misses
            // release both the lock and short admission before joining cold fill.
            let Some(response_content_length) = get_object_body_cache_plaintext_len(&rs, opts, info) else {
                break 'snapshot;
            };
            let cache_plan = build_get_object_body_cache_plan(
                &cache_adapter,
                GetObjectBodyCacheRequest {
                    bucket,
                    key,
                    info,
                    response_content_length,
                    has_range: rs.is_some(),
                    part_number,
                    encryption_applied: info.is_encrypted(),
                },
            );

            // The legacy hook is evaluated once, before cold-fill coordination.
            // In-session producer retries never re-enter this snapshot block.
            let legacy_probe = lookup_preplanned_get_object_body_cache_hook(
                Arc::clone(&cache_adapter),
                cache_plan.clone(),
                bucket,
                key,
                &rs,
                opts,
                info,
            )
            .await;
            if matches!(legacy_probe, GetObjectBodyCacheHookLookup::Ineligible) {
                break 'snapshot;
            }
            Self::validate_get_object_before_cold_fill(&req.headers, part_number, info)?;
            if let GetObjectBodyCacheHookLookup::Hit(body) = legacy_probe {
                drop(metadata_admission.take());
                let info = prepared
                    .take()
                    .ok_or_else(|| s3_error!(InternalError, "prepared cache-hit reader is unavailable"))?
                    .into_object_info();
                let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                let read_setup =
                    Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
                return Ok(GetObjectPreparedRead {
                    io_planning: Self::get_object_io_planning_without_disk(manager),
                    read_setup,
                });
            }
            if matches!(legacy_probe, GetObjectBodyCacheHookLookup::Miss) {
                legacy_hook_missed = true;
            }
            if !legacy_hook_missed
                && let GetObjectBodyCacheLookup::Hit(body) = lookup_get_object_body_cache_hit(&cache_adapter, &cache_plan).await
            {
                drop(metadata_admission.take());
                let info = prepared
                    .take()
                    .ok_or_else(|| s3_error!(InternalError, "prepared cache-hit reader is unavailable"))?
                    .into_object_info();
                let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                let read_setup =
                    Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
                return Ok(GetObjectPreparedRead {
                    io_planning: Self::get_object_io_planning_without_disk(manager),
                    read_setup,
                });
            }

            let GetObjectBodyCachePlan::Cacheable(engine_plan) = &cache_plan else {
                break 'snapshot;
            };
            let Some(cache_key) = cache_plan.key().cloned() else {
                break 'snapshot;
            };
            let expected = usize::try_from(response_content_length)
                .map_err(|_| s3_error!(InternalError, "cold-fill body length is not representable"))?;
            let response_size = u64::try_from(response_content_length)
                .map_err(|_| s3_error!(InternalError, "cold-fill body length is negative"))?;
            let waiter_deadline = cold_fill_deadline(wrapper, timeout_config, response_size);
            let proposed_producer_deadline = cold_fill_producer_deadline(timeout_config, response_size);
            let coordinator = cache_adapter.cold_fill_coordinator();
            let info = prepared
                .take()
                .ok_or_else(|| s3_error!(InternalError, "prepared cold-fill reader is unavailable"))?
                .into_object_info();
            drop(metadata_admission.take());
            let outcome = coordinate_cold_fill(&coordinator, cache_key, waiter_deadline, Some(proposed_producer_deadline), {
                let adapter = &cache_adapter;
                let headers = &req.headers;
                let store = &store;
                let range = &rs;
                move |producer| {
                    let adapter = Arc::clone(adapter);
                    let engine_plan = engine_plan.clone();
                    let h = headers.clone();
                    let store = Arc::clone(store);
                    let range = range.clone();
                    let bucket = bucket.to_owned();
                    let key = key.to_owned();
                    let opts = opts.clone();
                    async move {
                        let producer_deadline = producer.deadline();
                        let cancellation = producer.cancellation_token();
                        let second_chance = match await_cold_fill_startup(
                            lookup_cold_fill_second_chance(&adapter, &engine_plan),
                            &cancellation,
                            producer_deadline,
                        )
                        .await
                        {
                            Ok(body) => body,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        };
                        if let Some(body) = second_chance {
                            producer.finish_shared(Ok(body));
                            return;
                        }

                        let acquire = Self::acquire_cold_fill_io_planning(manager, &bucket, &key);
                        let producer_io = match await_cold_fill_startup(acquire, &cancellation, producer_deadline).await {
                            Ok(result) => result,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        };
                        let producer_io = match producer_io {
                            Ok(io) => io,
                            Err(err) => {
                                producer.finish_shared(Err(err));
                                return;
                            }
                        };

                        let prepare = store.prepare_get_object_reader(&bucket, &key, range.clone(), HeaderMap::new(), &opts);
                        let prepared = match match await_cold_fill_startup(prepare, &cancellation, producer_deadline).await {
                            Ok(result) => result,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        } {
                            Ok(prepared) => prepared,
                            Err(err) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(err));
                                return;
                            }
                        };
                        let current_info = prepared.object_info();
                        let current_length = match current_info.get_actual_size() {
                            Ok(length) => length,
                            Err(err) => {
                                let _ = err;
                                producer.finish_shared(Err(ColdFillError::Storage(StorageError::FileCorrupt)));
                                return;
                            }
                        };
                        let current_plan = build_get_object_body_cache_plan_for_revalidation(
                            &adapter,
                            GetObjectBodyCacheRequest {
                                bucket: &bucket,
                                key: &key,
                                info: current_info,
                                response_content_length: current_length,
                                has_range: range.is_some(),
                                part_number,
                                encryption_applied: current_info.is_encrypted(),
                            },
                        );
                        let Some(producer) = retain_cold_fill_producer_for_matching_plan(producer, &current_plan, &engine_plan)
                        else {
                            return;
                        };

                        let reservation = adapter.reserve_body(&engine_plan);
                        #[cfg(test)]
                        let reader_open_plan = engine_plan.clone();
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            || async move { Ok(producer_io) },
                            || {
                                #[cfg(test)]
                                record_cold_fill_reader_open_for_test(&reader_open_plan);
                                prepared.with_headers(h).into_reader()
                            },
                            ColdFillProducerExecution {
                                expected,
                                deadline: producer_deadline,
                                adapter,
                                engine_plan,
                            },
                        )
                        .await;
                    }
                }
            })
            .await;

            match outcome {
                ColdFillCoordinateOutcome::Ready(result) => {
                    let body = match result {
                        Ok(body) => body,
                        Err(ColdFillError::Storage(err)) => return Err(map_get_object_reader_error(err).into()),
                        Err(ColdFillError::DiskAdmissionClosed) => {
                            return Err(s3_error!(InternalError, "disk read semaphore closed"));
                        }
                    };
                    let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                    let read_setup =
                        Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true)
                            .await?;
                    return Ok(GetObjectPreparedRead {
                        io_planning: Self::get_object_io_planning_without_disk(manager),
                        read_setup,
                    });
                }
                ColdFillCoordinateOutcome::Bypass => {
                    cache_fill_allowed = false;
                    break 'snapshot;
                }
                ColdFillCoordinateOutcome::Rejected => return Err(ApiError::from(StorageError::SlowDown).into()),
            }
        }

        let (io_planning, reader) = if let Some(prepared) = prepared.take() {
            let io_planning = metadata_admission
                .take()
                .ok_or_else(|| s3_error!(InternalError, "prepared metadata admission is unavailable"))?;
            let reader = prepared
                .with_headers(req.headers.clone())
                .into_reader()
                .await
                .map_err(map_get_object_reader_error)?;
            (io_planning, reader)
        } else {
            let io_planning = Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?;
            let reader = if legacy_hook_missed {
                store
                    .prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts)
                    .await
                    .map_err(map_get_object_reader_error)?
                    .with_headers(req.headers.clone())
                    .into_reader()
                    .await
                    .map_err(map_get_object_reader_error)?
            } else {
                store
                    .get_object_reader(bucket, key, rs.clone(), req.headers.clone(), opts)
                    .await
                    .map_err(map_get_object_reader_error)?
            };
            (io_planning, reader)
        };
        let read_setup =
            Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, cache_fill_allowed)
                .await?;
        if let Some(read_stage_start) = read_stage_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "store_reader_setup",
                read_stage_start.elapsed().as_secs_f64(),
            );
        }
        Ok(GetObjectPreparedRead { io_planning, read_setup })
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_get_object_read(
        req: &S3Request<GetObjectInput>,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        mut rs: Option<HTTPRangeSpec>,
        part_number: Option<usize>,
        read_start: std::time::Instant,
        reader: GetObjectReader,
        cache_fill_allowed: bool,
    ) -> S3Result<GetObjectReadSetup> {
        // ODC-16: capture whether the ecstore cache hook already probed this
        // read, so the app layer does not repeat the lookup it ran after fresh
        // metadata resolution.
        let cache_hook_served = reader.is_cache_hook_served();
        let cache_hook_probed = reader.cache_hook_probed();
        let info = reader.object_info;
        let stream = reader.stream;
        let buffered_body = reader.buffered_body;

        let read_duration = read_start.elapsed();

        // Conditional metrics recording to reduce overhead
        if rustfs_io_metrics::get_stage_metrics_enabled() {
            use rustfs_io_metrics::record_zero_copy_read;
            record_zero_copy_read(info.size as usize, read_duration.as_secs_f64() * 1000.0);
            manager.record_disk_operation(info.size as u64, read_duration, true).await;
        }

        check_preconditions(&req.headers, &info)?;
        Self::validate_get_object_part_number(part_number, &info)?;

        debug!(object_size = info.size, part_count = info.parts.len(), "GET object metadata snapshot");
        for part in info.parts.iter() {
            debug!(
                part_number = part.number,
                part_size = part.size,
                part_actual_size = part.actual_size,
                "GET object part details"
            );
        }

        let content_type = if let Some(content_type) = &info.content_type {
            match ContentType::from_str(content_type) {
                Ok(res) => Some(res),
                Err(err) => {
                    error!(content_type, error = ?err, "GET object content-type parse failed");
                    None
                }
            }
        } else {
            None
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        if let Some(part_number) = part_number
            && rs.is_none()
        {
            rs = HTTPRangeSpec::from_part_sizes(
                info.size,
                part_number,
                info.parts.iter().map(|part| {
                    if part.actual_size > 0 {
                        part.actual_size
                    } else {
                        i64::try_from(part.size).unwrap_or(i64::MAX)
                    }
                }),
            );
        }

        validate_sse_headers_for_read(&info.user_defined, &req.headers)?;

        let mut content_length = info.get_actual_size().map_err(ApiError::from)?;
        let content_range = if let Some(rs) = &rs {
            let total_size = content_length;
            let (start, length) = rs.get_offset_length(total_size).map_err(ApiError::from)?;
            content_length = length;
            Some(format!("bytes {}-{}/{}", start, start as i64 + length - 1, total_size))
        } else {
            None
        };

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
        };

        let response_content_length = content_length;

        let (
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
            final_stream,
            buffered_body,
        ) = match sse_decryption(decryption_request).await? {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let sse_customer_algorithm = matches!(material.sse_type, SSEType::SseC).then_some(material.algorithm.clone());
                let sse_customer_key_md5 = material.customer_key_md5.clone();
                (
                    server_side_encryption,
                    sse_customer_algorithm,
                    sse_customer_key_md5,
                    material.kms_key_id,
                    true,
                    wrap_reader(stream),
                    None,
                )
            }
            None => (None, None, None, None, false, wrap_reader(stream), buffered_body),
        };

        Ok(GetObjectReadSetup {
            info,
            final_stream,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
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
        let base_buffer_size = if response_content_length > 0 {
            get_buffer_size_opt_in(response_content_length)
        } else {
            self.base_buffer_size()
        };

        let is_sequential_hint = if rs.is_none() {
            true
        } else if let Some(range_spec) = rs {
            range_spec.start == 0 && !range_spec.is_suffix_length
        } else {
            false
        };

        // Conditional metrics recording to reduce overhead
        if rustfs_io_metrics::get_stage_metrics_enabled() {
            if let Some(range_spec) = rs
                && range_spec.start >= 0
            {
                manager.record_access(range_spec.start as u64, response_content_length as u64);
            }

            if response_content_length > 0 {
                manager.record_transfer(response_content_length as u64, permit_wait_duration);
            }
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

        let optimal_buffer_size = if io_strategy.buffer_size > 0 {
            io_strategy.buffer_size
        } else {
            get_concurrency_aware_buffer_size(response_content_length, base_buffer_size)
        };

        debug!(
            "GetObject buffer sizing: file_size={}, base={}, optimal={}, concurrent_requests={}, io_strategy={:?}",
            response_content_length, base_buffer_size, optimal_buffer_size, concurrent_requests, io_strategy.load_level
        );
        let enable_readahead = io_strategy.enable_readahead;

        GetObjectStrategyContext {
            io_strategy,
            optimal_buffer_size,
            enable_readahead,
        }
    }

    fn build_get_object_checksums(
        info: &ObjectInfo,
        headers: &HeaderMap,
        part_number: Option<usize>,
        rs: Option<&HTTPRangeSpec>,
    ) -> S3Result<ResponseChecksums> {
        if let Some(checksum_mode) = headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (decrypted_checksums, _is_multipart) =
                info.decrypt_checksums(part_number.unwrap_or(0), headers).map_err(|e| {
                    error!(error = %e, "GetObject checksum decryption failed");
                    ApiError::from(e)
                })?;

            return Ok(classify_response_checksums(decrypted_checksums));
        }

        Ok(ResponseChecksums::default())
    }
    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body<R>(
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        concurrent_requests: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
        buffered_body: Option<Bytes>,
        bucket: &str,
        key: &str,
        mut lifecycle: GetObjectBodyLifecycle,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        if encryption_applied {
            let should_buffer_encrypted_object =
                should_buffer_get_object_in_memory(info, response_content_length, part_number, has_range, concurrent_requests);

            if should_buffer_encrypted_object {
                // Strict materialization (#1324): a decrypted body that is shorter
                // or longer than the declared content length must hard-fail before
                // headers, not warn-and-serve a truncated/over-long body.
                let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
                match strict_materialize_object_body(final_stream, expected, GET_OBJECT_STAGE_BODY_ENCRYPTED_BUFFER_READ).await {
                    Ok(buf) => {
                        return Ok(Self::build_memory_blob(
                            buf,
                            response_content_length,
                            GET_MEMORY_BODY_SOURCE_ENCRYPTED_BUFFER,
                            lifecycle,
                        ));
                    }
                    Err(e) => {
                        lifecycle.finish_err();
                        error!(error = %e, "GetObject decrypted object strict materialization failed");
                        return Err(e.into_s3_error(response_content_length));
                    }
                }
            }

            debug!(buffer_size = optimal_buffer_size, "Encrypted object uses streaming decrypt path");
            let stream_strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
            let (stream_buffer_size, stream_strategy) =
                Self::select_stream_buffer_strategy(response_content_length, optimal_buffer_size, enable_readahead, has_range);
            record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAM_STRATEGY, stream_strategy_start);
            return Ok(Self::build_reader_blob(
                final_stream,
                response_content_length,
                stream_buffer_size,
                stream_strategy,
                bucket,
                key,
                lifecycle,
            ));
        }

        if let Some(buffered_body) = buffered_body {
            // Strict materialization (#1324): the buffered body is the exact
            // response payload; a length disagreement means an upstream/cache bug
            // and must hard-fail before headers rather than serve a body that does
            // not match its committed Content-Length.
            let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
            if buffered_body.len() != expected {
                lifecycle.finish_err();
                error!(
                    expected = response_content_length,
                    actual = buffered_body.len(),
                    "Buffered GetObject body length mismatch"
                );
                return Err(ApiError::from(StorageError::other(format!(
                    "Buffered GetObject body length mismatch: expected {response_content_length}, got {}",
                    buffered_body.len()
                )))
                .into());
            }

            return Ok(Self::build_memory_bytes_blob(
                buffered_body,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                lifecycle,
            ));
        }

        let should_provide_seek_support =
            should_buffer_get_object_in_memory(info, response_content_length, part_number, has_range, concurrent_requests);

        if should_provide_seek_support {
            // Strict materialization (#1324): the previous implementation only
            // logged a warning on a length mismatch, and — most dangerously — on a read
            // error it fell through to streaming the *same* reader after
            // `read_to_end` had already drained K bytes, shipping a body missing
            // its prefix (prefix-misaligned data). Both are now hard errors: an
            // exact-length read is required, and any read error returns without
            // reusing the partially consumed reader.
            let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
            match strict_materialize_object_body(final_stream, expected, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await {
                Ok(buf) => {
                    return Ok(Self::build_memory_blob(
                        buf,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_SEEK_BUFFER,
                        lifecycle,
                    ));
                }
                Err(e) => {
                    lifecycle.finish_err();
                    error!(
                        error = %e,
                        "GetObject seek-support strict materialization failed; refusing to reuse the partially consumed reader"
                    );
                    return Err(e.into_s3_error(response_content_length));
                }
            }
        }

        let stream_strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let (stream_buffer_size, stream_strategy) =
            Self::select_stream_buffer_strategy(response_content_length, optimal_buffer_size, enable_readahead, has_range);
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAM_STRATEGY, stream_strategy_start);
        Ok(Self::build_reader_blob(
            final_stream,
            response_content_length,
            stream_buffer_size,
            stream_strategy,
            bucket,
            key,
            lifecycle,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body_with_cache<R>(
        cache_adapter: &ObjectDataCacheAdapter,
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        concurrent_requests: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
        buffered_body: Option<Bytes>,
        cache_hook_served: bool,
        cache_hook_probed: bool,
        cache_fill_allowed: bool,
        bucket: &str,
        key: &str,
        mut lifecycle: GetObjectBodyLifecycle,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        // ODC-16 (backlog#1121): when the ecstore hook or shared cold fill
        // already supplied this body, the request-level plan was built before
        // the authoritative lookup. Serve it without planning a second time.
        if cache_hook_served && let Some(bytes) = buffered_body.clone() {
            return Ok(Self::build_memory_bytes_blob(
                bytes,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE,
                lifecycle,
            ));
        }

        if !cache_fill_allowed {
            return Self::build_get_object_body(
                final_stream,
                info,
                response_content_length,
                optimal_buffer_size,
                enable_readahead,
                concurrent_requests,
                part_number,
                has_range,
                encryption_applied,
                buffered_body,
                bucket,
                key,
                lifecycle,
            )
            .await;
        }

        let cache_request = GetObjectBodyCacheRequest {
            bucket,
            key,
            info,
            response_content_length,
            has_range,
            part_number,
            encryption_applied,
        };
        let cache_plan = build_get_object_body_cache_plan(cache_adapter, cache_request);

        // ODC-16: only look up when the hook did not probe this read. When it did
        // probe (a served body handled above, or a miss), its result is
        // authoritative because it ran after fresh metadata resolution, so the
        // app layer skips its own lookup and only uses the plan to fill.
        if !cache_hook_probed {
            match lookup_get_object_body_cache_hit(cache_adapter, &cache_plan).await {
                GetObjectBodyCacheLookup::Hit(bytes) => {
                    return Ok(Self::build_memory_bytes_blob(
                        bytes,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE,
                        lifecycle,
                    ));
                }
                GetObjectBodyCacheLookup::Disabled | GetObjectBodyCacheLookup::Skip | GetObjectBodyCacheLookup::Miss => {}
            }
        }

        if let Some(buffered_body) = buffered_body {
            // ODC-15: the body is already fully in hand, so keep the fill off the
            // response's critical path. For a cacheable plan, run the fill in a
            // detached task (Bytes is a cheap clone) and return immediately. For
            // a non-cacheable plan the fill is a pure metric-only skip with no
            // I/O, so record it inline to preserve observability.
            if cache_fill_allowed && matches!(cache_plan, GetObjectBodyCachePlan::Cacheable(_)) {
                let cache_adapter = cache_adapter.clone();
                let cache_plan = cache_plan.clone();
                let fill_bytes = buffered_body.clone();
                tokio::spawn(async move {
                    let _ = fill_get_object_body_cache_from_buffered_body(&cache_adapter, &cache_plan, &fill_bytes).await;
                });
            } else if cache_fill_allowed {
                let _ = fill_get_object_body_cache_from_buffered_body(cache_adapter, &cache_plan, &buffered_body).await;
            }

            return Ok(Self::build_memory_bytes_blob(
                buffered_body,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                lifecycle,
            ));
        }

        let should_materialize_for_cache = cache_adapter.materialize_fill_enabled()
            && cache_fill_allowed
            && matches!(cache_plan, GetObjectBodyCachePlan::Cacheable(_))
            && should_materialize_get_object_body_for_cache(
                info,
                response_content_length,
                part_number,
                has_range,
                concurrent_requests,
            );

        if should_materialize_for_cache {
            let Ok(materialized_capacity) = usize::try_from(response_content_length) else {
                warn!(
                    expected = response_content_length,
                    "GetObject materialize-fill skipped because content length is not representable"
                );
                return Self::build_get_object_body(
                    final_stream,
                    info,
                    response_content_length,
                    optimal_buffer_size,
                    enable_readahead,
                    concurrent_requests,
                    part_number,
                    has_range,
                    encryption_applied,
                    None,
                    bucket,
                    key,
                    lifecycle,
                )
                .await;
            };
            // ODC-07 / #1324: share the strict exact-length materialization gate
            // with the encrypted and seek memory branches. The helper bounds the
            // read to `capacity + 1` (so an over-long stream is detected without
            // buffering it unbounded), rejects short and over-long reads, and on a
            // partial-read error refuses to reuse the consumed reader.
            match strict_materialize_object_body(
                final_stream,
                materialized_capacity,
                GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ,
            )
            .await
            {
                Ok(buf) => {
                    let bytes = Bytes::from(buf);
                    // ODC-15: fill off the response's critical path (see the
                    // buffered-body branch above).
                    let cache_adapter = cache_adapter.clone();
                    let cache_plan = cache_plan.clone();
                    let fill_bytes = bytes.clone();
                    tokio::spawn(async move {
                        let _ = fill_get_object_body_cache_from_materialized_body(&cache_adapter, &cache_plan, &fill_bytes).await;
                    });

                    return Ok(Self::build_memory_bytes_blob(
                        bytes,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE_MATERIALIZED,
                        lifecycle,
                    ));
                }
                Err(e) => {
                    lifecycle.finish_err();
                    error!(error = %e, "GetObject materialize-fill strict materialization failed");
                    // A short/over-long body would ship a truncated or over-long
                    // response; a partial-read error leaves the stream consumed so
                    // falling back to streaming would send a prefix-misaligned
                    // body. Both fail the request.
                    return Err(e.into_s3_error(response_content_length));
                }
            }
        }

        Self::build_get_object_body(
            final_stream,
            info,
            response_content_length,
            optimal_buffer_size,
            enable_readahead,
            concurrent_requests,
            part_number,
            has_range,
            encryption_applied,
            None,
            bucket,
            key,
            lifecycle,
        )
        .await
    }

    fn put_object_execution_context(req: &S3Request<PutObjectInput>) -> (EventName, QuotaOperation, &'static str) {
        if req.extensions.get::<PostObjectRequestMarker>().is_some() {
            (put_event_name_for_post_object(true), QuotaOperation::PostObject, "POST")
        } else {
            (put_event_name_for_post_object(false), QuotaOperation::PutObject, "PUT")
        }
    }

    #[instrument(level = "info", skip(self, _fs, req))]
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

        // Bucket-quota admission runs exactly once, and only now that the authoritative object length is known. `size` is the same basis the settle phase records via ObjectInfo.size (actual, pre-compression/pre-encryption logical size), NOT the aws-chunked wire Content-Length. When no quota is configured this stays a zero-extra-I/O fast path; once a hard quota is set, checker/config/usage faults fail closed with a retryable error.
        self.check_bucket_quota(&bucket, quota_operation, size as u64).await?;

        let ingress_stage_start = std::time::Instant::now();
        let should_compress = is_disk_compressible(&req.headers, &key) && size > MIN_DISK_COMPRESSIBLE_SIZE as i64;
        let server_side_encryption_requested =
            server_side_encryption.is_some() || sse_customer_algorithm.is_some() || ssekms_key_id.is_some();

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

        let use_small_eager_put_path =
            should_use_small_eager_put_path(size, &req.headers, server_side_encryption_requested, should_compress, false);
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
        } else if use_small_eager_put_path {
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

        let store = get_validated_store(&bucket).await?;

        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
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
        let mut effective_sse = server_side_encryption.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                        match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256), // fallback to AES256
                        }
                    })
                })
            })
        });
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
            extract_ssekms_context_from_headers(&req.headers)?.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true, // PutObject requires all three: algorithm, key, key_md5
        )?;

        let mut metadata = metadata.unwrap_or_default();
        let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
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
        apply_bucket_default_lock_retention(&bucket, &mut metadata, has_explicit_object_lock_retention).await?;

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

        // rustfs/backlog#1009: the pre-PUT lookup has exactly two consumers —
        // the existing-object WORM validation and usage accounting's
        // previous_current_size. When the bucket has no object locking (WORM is
        // a provable no-op; the gate fails closed on metadata errors) and the
        // PUT targets the latest version (no explicit version_id from internal
        // replication), the lookup is skipped and accounting is backfilled from
        // the dst xl.meta that rename_data already reads, saving a full-disk
        // metadata fanout per PUT.
        let prelookup_required = version_id.is_some() || put_prelookup_worm_gate(&bucket).await;
        // Outer None = prelookup skipped (accounting comes from the commit
        // backfill); Some(inner) = the previous current size as observed by the
        // lookup, with the pre-#1009 semantics kept bit-for-bit.
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
                    validate_existing_object_lock_for_write(&existing_obj_info, &opts)?;
                    Some(existing_obj_info.size.max(0) as u64)
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

        let mut write_plan = WritePlan::new();
        // Additional-checksum (XXHash3/64/128, SHA-512) values to echo on the PutObject
        // response (#1256); captured at want_checksum set points before opts is moved.
        let mut put_extra_checksum_headers: Vec<(&'static str, String)> = Vec::new();
        let mut reader = if should_compress {
            let body = tokio::io::BufReader::with_capacity(
                buffer_size,
                StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
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
            } else if use_small_eager_put_path {
                if (actual_size as usize) <= POOL_BYPASS_MAX_SIZE {
                    // Bypass BytesPool for very small objects to avoid Small-tier
                    // Mutex contention under high concurrency. Direct allocation
                    // for ≤4KiB is negligible cost.
                    let eager_body = read_small_put_body_exact_direct(
                        StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
                        actual_size as usize,
                    )
                    .await?;
                    HashReader::from_stream(eager_body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
                } else {
                    let pool = get_concurrency_manager().bytes_pool();
                    let eager_body = read_small_put_body_exact_pooled(
                        StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
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
                    StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
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
        rustfs_io_metrics::record_put_object_stage_duration(
            "ingress_prepare",
            ingress_stage_start.elapsed().as_secs_f64() * 1000.0,
        );

        let mut helper = OperationHelper::new(&req, event_name, S3Operation::PutObject);
        let ssekms_context = extract_ssekms_context_from_headers(&req.headers)?;

        // Apply encryption using unified SSE API.
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

            write_plan = write_plan.with_encryption(material.write_encryption(None));

            let encryption_metadata = encryption_material_to_metadata(&material)?;
            metadata.extend(encryption_metadata.clone());
            opts.user_defined.extend(encryption_metadata);
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        let mut reader = PutObjReader::new(reader);

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
        let dsc =
            must_replicate_object(&bucket, &key, &mt2, "".to_string(), opts.delete_marker_replication_status(), opts.clone())
                .await;

        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

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
                    put_path = put_path,
                    object_size = actual_size,
                    duration_ms = start_time.elapsed().as_millis() as u64,
                    result = "success",
                    "PutObject store write returned"
                );
                obj_info
            }
            Err(err) => {
                store_put_watchdog.cancel();
                warn!(
                    target: "rustfs::app::object_usecase",
                    event = EVENT_PUT_OBJECT_STORE_RETURNED,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    request_id = %request_id,
                    bucket = %bucket,
                    key = %key,
                    put_path = put_path,
                    object_size = actual_size,
                    duration_ms = start_time.elapsed().as_millis() as u64,
                    result = "error",
                    error = %err,
                    "PutObject store write returned"
                );
                let result: S3Result<S3Response<PutObjectOutput>> = Err(err.into());
                put_request_guard.finish_err();
                let _ = helper.complete(&result);
                return result;
            }
        };

        maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;
        let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &key).await;

        let put_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
        // Fast in-memory update for immediate quota and admin usage consistency.
        // The previous current size comes from the prelookup when it ran,
        // otherwise from the rename_data backfill (rustfs/backlog#1009); the
        // backfill reproduces the lookup's observation bit for bit (latest
        // version's ObjectInfo.size — 0 for a delete-marker latest — or
        // not-found → None).
        match prelookup_previous_current_size.or_else(|| previous_current_size_from_backfill(backfilled_old_current_size)) {
            Some(previous_current_size) => {
                if put_versioned {
                    record_bucket_object_version_write_memory(&bucket, previous_current_size, obj_info.size.max(0) as u64).await;
                } else {
                    record_bucket_object_write_memory(&bucket, previous_current_size, obj_info.size.max(0) as u64).await;
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
                record_bucket_object_write_unknown_previous_memory(&bucket, obj_info.size.max(0) as u64, put_versioned).await;
            }
        }

        let raw_version = obj_info.version_id.map(|v| v.to_string());

        helper = helper.object(obj_info.clone());
        if let Some(version_id) = &raw_version {
            helper = helper.version_id(version_id.clone());
        }

        let put_version = if put_versioned { raw_version } else { None };

        let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let expiration = resolve_put_object_expiration(&bucket, &obj_info).await;

        // Reuse the single replication decision computed before commit (see `dsc`
        // above) so the pending metadata persisted with the object and the
        // post-commit schedule always derive from the same immutable decision.
        // Recomputing here would repeat the versioning/config/target traversal and,
        // worse, allow a replication-config hot update between the two phases to
        // produce a pending-without-schedule or schedule-without-pending divergence
        // (https://github.com/rustfs/backlog/issues/1320).
        if dsc.replicate_any() {
            schedule_object_replication(obj_info.clone(), store, dsc).await;
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

        let mut response = S3Response::new(output);
        // Echo XXHash3/64/128 / SHA-512 checksums that s3s PutObjectOutput has no typed
        // field for (#1256).
        inject_additional_checksum_headers(&mut response.headers, &put_extra_checksum_headers);
        let result = Ok(response);
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_bucket(&bucket);

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

    fn finalize_get_object_completion(
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
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

    fn ensure_get_object_not_timed_out(
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        bucket: &str,
        key: &str,
        stage: GetObjectTimeoutStage,
    ) -> S3Result<()> {
        if !wrapper.is_timeout() {
            return Ok(());
        }

        let timeout_secs = timeout_config.get_object_timeout.as_secs();
        let elapsed_ms = wrapper.elapsed().as_millis();

        match stage {
            GetObjectTimeoutStage::BeforeProcessing => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out before processing"
                );
                Err(s3_error!(InternalError, "Request timeout before processing"))
            }
            GetObjectTimeoutStage::DiskPermitWait { permit_wait_duration } => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_duration.as_millis(),
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out while waiting for disk permit"
                );
                rustfs_io_metrics::record_get_object_timeout(Some("disk_permit"), Some(wrapper.elapsed().as_secs_f64()));
                Err(s3_error!(InternalError, "Request timeout while waiting for disk permit"))
            }
            GetObjectTimeoutStage::BeforeRead => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out before reading object"
                );
                rustfs_io_metrics::record_get_object_timeout(Some("before_read"), Some(wrapper.elapsed().as_secs_f64()));
                Err(s3_error!(InternalError, "Request timeout before reading object"))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_get_object_response(
        helper: OperationHelper,
        bucket: &str,
        method: &hyper::Method,
        headers: &HeaderMap,
        event_info: Option<ObjectInfo>,
        version_id_for_event: String,
        output: GetObjectOutput,
        extra_checksum_headers: Vec<(&'static str, String)>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let helper = match event_info {
            Some(event_info) => helper.object(event_info),
            None => helper,
        };
        let helper = helper.version_id(version_id_for_event);
        let mut response = wrap_response_with_cors(bucket, method, headers, output).await;
        // Emit XXHash3/64/128 and SHA-512 checksums that s3s GetObjectOutput cannot
        // carry (#1257). This is the download-side integrity path AWS SDKs verify.
        inject_additional_checksum_headers(&mut response.headers, &extra_checksum_headers);
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
        event_info: Option<ObjectInfo>,
        final_stream: DynReader,
        buffered_body: Option<Bytes>,
        cache_hook_served: bool,
        cache_hook_probed: bool,
        cache_fill_allowed: bool,
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
        lifecycle: GetObjectBodyLifecycle,
    ) -> S3Result<GetObjectOutputContext> {
        let strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
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
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_OUTPUT_STRATEGY, strategy_start);
        let GetObjectStrategyContext {
            io_strategy: _,
            optimal_buffer_size,
            enable_readahead,
        } = strategy;
        let cache_adapter = self.object_data_cache();

        let body_build_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let body = Self::build_get_object_body_with_cache(
            &cache_adapter,
            final_stream,
            &info,
            response_content_length,
            optimal_buffer_size,
            enable_readahead,
            concurrent_requests,
            part_number,
            rs.is_some(),
            encryption_applied,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
            bucket,
            key,
            lifecycle,
        )
        .await?;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_BUILD, body_build_start);

        let checksum_headers_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let checksums = Self::build_get_object_checksums(&info, &req.headers, part_number, rs.as_ref())?;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_CHECKSUM_HEADERS, checksum_headers_start);

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
        let lifecycle_expiration_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let expiration = resolve_put_object_expiration(bucket, &info).await;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_LIFECYCLE_EXPIRATION, lifecycle_expiration_start);
        let storage_class = response_storage_class(&info, &info.user_defined);
        let content_disposition = info.user_defined.get("content-disposition").cloned();

        let metadata_filter_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let metadata = filter_object_metadata(&info.user_defined);
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_METADATA_FILTER, metadata_filter_start);

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(response_content_length),
            last_modified,
            content_type,
            content_encoding: info.content_encoding.clone(),
            content_disposition,
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
            content_range,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata,
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
            extra_checksum_headers: checksums.extra,
        })
    }

    #[instrument(
        level = "info",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let inbound_request_context = req.extensions.get::<request_context::RequestContext>();
        let request_id = inbound_request_context
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| request_context::RequestContext::fallback().request_id);
        if rustfs_io_metrics::get_stage_metrics_enabled()
            && let Some(context) = inbound_request_context
        {
            rustfs_io_metrics::record_get_object_stage_duration(
                GET_OBJECT_STAGE_PATH_S3_HANDLER,
                GET_OBJECT_STAGE_REQUEST_INGRESS_TO_CONTEXT,
                context.start_time.elapsed().as_secs_f64(),
            );
        }
        let bootstrap = Self::init_get_object_bootstrap(&req.input.bucket, &req.input.key, &request_id)?;
        let timeout_config = bootstrap.timeout_config;
        let wrapper = bootstrap.wrapper;
        let request_start = bootstrap.request_start;
        let concurrent_requests = bootstrap.concurrent_requests;
        let mut lifecycle = GetObjectBodyLifecycle::tracked(bootstrap.request_guard);

        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        // mc get 3

        let request_context_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let request_context = match Self::prepare_get_object_request_context(&req).await {
            Ok(request_context) => request_context,
            Err(err) => {
                lifecycle.finish_err();
                return Err(err);
            }
        };
        if let Some(request_context_start) = request_context_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "request_context",
                request_context_start.elapsed().as_secs_f64(),
            );
        }
        let GetObjectRequestContext {
            bucket,
            key,
            version_id_for_event,
            part_number,
            rs,
            opts,
        } = request_context;

        let manager = get_concurrency_manager();

        let prepared_read = match self
            .prepare_get_object_read_execution(&req, manager, &wrapper, &timeout_config, &bucket, &key, rs, &opts, part_number)
            .await
        {
            Ok(prepared_read) => prepared_read,
            Err(err) => {
                lifecycle.finish_err();
                return Err(err);
            }
        };
        let GetObjectPreparedRead { io_planning, read_setup } = prepared_read;
        let GetObjectIoPlanning {
            disk_permit,
            permit_wait_duration,
            queue_status,
            queue_utilization,
        } = io_planning;

        let GetObjectReadSetup {
            info,
            final_stream,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
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
        let final_stream = if let Some(disk_permit) = disk_permit {
            wrap_reader(DiskReadPermitReader::new(final_stream, disk_permit))
        } else {
            final_stream
        };

        // Clone ObjectInfo for event notification only when an event will
        // actually be built — the clone is expensive for multipart objects.
        let event_info = helper.wants_object_info().then(|| info.clone());

        let output_build_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let output_context = self
            .build_get_object_output_context(
                &req,
                manager,
                &bucket,
                &key,
                info,
                event_info,
                final_stream,
                buffered_body,
                cache_hook_served,
                cache_hook_probed,
                cache_fill_allowed,
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
                opts.versioned,
                lifecycle,
            )
            .await;
        let output_context = match output_context {
            Ok(output_context) => output_context,
            Err(err) => return Err(err),
        };
        if let Some(output_build_start) = output_build_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "output_build",
                output_build_start.elapsed().as_secs_f64(),
            );
        }
        let GetObjectOutputContext {
            output,
            event_info,
            response_content_length,
            optimal_buffer_size,
            extra_checksum_headers,
        } = output_context;

        let total_duration = request_start.elapsed();
        Self::finalize_get_object_completion(
            &wrapper,
            &timeout_config,
            total_duration,
            response_content_length,
            optimal_buffer_size,
        );

        Self::finalize_get_object_response(
            helper,
            &bucket,
            &req.method,
            &req.headers,
            event_info,
            version_id_for_event,
            output,
            extra_checksum_headers,
        )
        .await
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

        let Some(store) = self.object_store() else {
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
        debug!(
            "GetObjectAttributes raw object_attributes={:?}",
            object_attributes.iter().map(|value| value.as_str()).collect::<Vec<_>>()
        );

        let requested = |name: &'static str| -> bool { object_attributes_requested(&object_attributes, name) };
        let storage_class =
            response_storage_class_for_object_attributes(&info, &metadata_map, requested(ObjectAttributes::STORAGE_CLASS));

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
            // GetObjectAttributes returns checksums in the XML body, and s3s's Checksum
            // type has no field for the additional algorithms, so `extra` cannot be
            // surfaced here (unlike the header-based GET/HEAD paths) — an s3s limitation
            // tracked for when it gains typed fields.
            let ResponseChecksums {
                crc32: checksum_crc32,
                crc32c: checksum_crc32c,
                sha1: checksum_sha1,
                sha256: checksum_sha256,
                crc64nvme: checksum_crc64nvme,
                checksum_type,
                ..
            } = classify_response_checksums(checksums);

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
                // Additional algorithms cannot be surfaced in the ObjectPart XML body
                // (s3s has no field); same limitation as the object-level attributes above.
                let ResponseChecksums {
                    crc32: checksum_crc32,
                    crc32c: checksum_crc32c,
                    sha1: checksum_sha1,
                    sha256: checksum_sha256,
                    crc64nvme: checksum_crc64nvme,
                    ..
                } = classify_response_checksums(checksums);

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
        let has_replacement_metadata = metadata.is_some()
            || cache_control.is_some()
            || content_disposition.is_some()
            || content_encoding.is_some()
            || content_language.is_some()
            || content_type.is_some()
            || expires.is_some();
        if has_replacement_metadata && !replaces_metadata {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "Replacement metadata requires the REPLACE metadata directive".to_string(),
            ));
        }
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
        let replacement_tags = super::storage_api::object_usecase::s3_api::tagging::resolve_copy_object_tags(
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

        let mut dst_opts = copy_dst_opts(&bucket, &key, dest_version_id.clone(), &req.headers, HashMap::new())
            .await
            .map_err(ApiError::from)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _self_copy_lock_guard = if cp_src_dst_same {
            let guard = acquire_self_copy_namespace_lock(store.as_ref(), &bucket, &key).await?;
            src_opts.no_lock = true;
            src_get_opts.no_lock = true;
            dst_opts.no_lock = true;
            Some(guard)
        } else {
            None
        };

        let mut current_opts: ObjectOptions = internal_object_info_lookup_opts(
            get_opts(&bucket, &key, dest_version_id.clone(), None, &req.headers)
                .await
                .map_err(ApiError::from)?,
        );
        if cp_src_dst_same {
            current_opts.no_lock = true;
        }
        let previous_current_size = match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&existing_obj_info, &dst_opts)?;
                Some(existing_obj_info.size.max(0) as u64)
            }
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };

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

        let h = build_ssec_read_headers(
            copy_source_sse_customer_algorithm.as_ref(),
            copy_source_sse_customer_key.as_ref(),
            copy_source_sse_customer_key_md5.as_ref(),
        );

        let gr = store
            .get_object_reader(&src_bucket, &src_key, None, h, &src_get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        let mut src_info = gr.object_info.clone();

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
            pairs.into_iter().find_map(|(k, v)| {
                rustfs_rio::ChecksumType::from_string(&k)
                    .is_s3s_typed()
                    .then(|| rustfs_rio::Checksum::new_from_string(&k, &v))
                    .flatten()
            })
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

        if cp_src_dst_same && src_info.transitioned_object.tier.is_empty() {
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
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )
        .await?
        {
            user_defined.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(&bucket, &mut user_defined, has_explicit_object_lock_retention).await?;

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
        match checksum_algorithm.as_ref() {
            Some(algo) => {
                let ct = rustfs_rio::ChecksumType::from_string(algo.as_str());
                if ct.is_set() {
                    reader.add_calculated_checksum(ct).map_err(ApiError::from)?;
                }
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
            ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
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

        src_info.user_defined = Arc::new(user_defined);

        self.check_bucket_quota(&bucket, QuotaOperation::CopyObject, src_info.size as u64)
            .await?;
        let has_bucket_metadata = self.bucket_metadata_sys().is_some();
        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let oi = store
            .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        maybe_enqueue_transition_immediate(&oi, LcEventSrc::S3CopyObject).await;
        let _ = invalidate_object_data_cache_after_copy_success(&cache_adapter, &bucket, &key).await;

        let dest_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
        // Update quota tracking after successful copy
        if has_bucket_metadata {
            if dest_versioned {
                record_bucket_object_version_write_memory(&bucket, previous_current_size, oi.size.max(0) as u64).await;
            } else {
                record_bucket_object_write_memory(&bucket, previous_current_size, oi.size.max(0) as u64).await;
            }
        }

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
            .map(|(pairs, _)| classify_response_checksums(pairs))
            .unwrap_or_default();

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: oi.mod_time.map(Timestamp::from),
            checksum_crc32: response_checksums.crc32,
            checksum_crc32c: response_checksums.crc32c,
            checksum_sha1: response_checksums.sha1,
            checksum_sha256: response_checksums.sha256,
            checksum_crc64nvme: response_checksums.crc64nvme,
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
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
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

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_cfg = BucketVersioningSys::get(&bucket).await.unwrap_or_default();
        let bypass_governance = has_bypass_governance_header(&req.headers);
        let bucket_lock_enabled = bucket_object_locking_enabled(&bucket).await;

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<StorageDeletedObject>,
            error: Option<s3s::dto::Error>,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        struct PreparedDelete {
            idx: usize,
            object: ObjectToDelete,
            opts: ObjectOptions,
            version_id: Option<String>,
            skip_stat: bool,
        }

        // Phase 1 (serial): request-scoped validation and authorization. These
        // steps mutate the request info between authorization calls, so they
        // stay sequential; they perform no per-object disk I/O.
        let mut prepared_deletes: Vec<PreparedDelete> = Vec::with_capacity(delete.objects.len());
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
            if let Err(e) = auth_res {
                delete_results[idx].error = Some(s3s::dto::Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(e.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            if bypass_governance {
                let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await;
                if let Err(e) = auth_res {
                    delete_results[idx].error = Some(s3s::dto::Error {
                        code: Some("AccessDenied".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some(e.to_string()),
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

            let object = ObjectToDelete {
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

            // backlog#929 (HP-8): the accounting branch after the store delete
            // decides delete-marker vs object-delete from this exact snapshot,
            // so evaluate it here with the same inputs to keep the stat-skip
            // decision and the accounting path provably consistent.
            let accounting_creates_delete_marker = object.version_id.is_none()
                && version_cfg.prefix_enabled(object.object_name.as_str())
                && !version_cfg.suspended();
            let skip_stat =
                can_skip_delete_objects_pre_stat(bucket_lock_enabled, replicate_deletes, &opts, accounting_creates_delete_marker);

            prepared_deletes.push(PreparedDelete {
                idx,
                object,
                opts,
                version_id,
                skip_stat,
            });
        }

        struct AdmittedDelete {
            idx: usize,
            object: ObjectToDelete,
            size: i64,
            existing: Option<ObjectInfo>,
            blocked: Option<s3s::dto::Error>,
        }

        // Phase 2 (bounded concurrency, backlog#929 / HP-8): the per-object
        // pre-delete stat plus the admission checks that consume it. Entries
        // are independent per key, and `buffered` preserves input order so the
        // per-key result mapping below is identical to the previous serial
        // loop. The authoritative object-lock enforcement stays in the
        // set_disk layer under the held write lock (#4297); the check here is
        // the same early, advisory rejection as before.
        let store_ref = &store;
        let bucket_ref = bucket.as_str();
        let admitted_deletes: Vec<AdmittedDelete> =
            futures::stream::iter(prepared_deletes.into_iter().map(|prepared| async move {
                let PreparedDelete {
                    idx,
                    mut object,
                    opts,
                    version_id,
                    skip_stat,
                } = prepared;

                let (goi, gerr) = if skip_stat {
                    (ObjectInfo::default(), None)
                } else {
                    match store_ref.get_object_info(bucket_ref, &object.object_name, &opts).await {
                        Ok(res) => (res, None),
                        Err(e) => (ObjectInfo::default(), Some(e.to_string())),
                    }
                };

                if !skip_stat
                    && gerr.is_none()
                    && !delete_creates_delete_marker(&opts)
                    && let Some(block_reason) = check_object_lock_for_deletion(bucket_ref, &goi, bypass_governance).await
                {
                    let blocked_key = object.object_name.clone();
                    return AdmittedDelete {
                        idx,
                        object,
                        size: 0,
                        existing: None,
                        blocked: Some(s3s::dto::Error {
                            code: Some("AccessDenied".to_string()),
                            key: Some(blocked_key),
                            message: Some(block_reason.error_message()),
                            version_id,
                        }),
                    };
                }

                let size = goi.size;

                if is_dir_object(&object.object_name) && object.version_id.is_none() {
                    object.version_id = Some(Uuid::nil());
                }

                if replicate_deletes {
                    let dsc = check_replicate_delete(
                        bucket_ref,
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
                            set_object_to_delete_version_purge_status(&mut object, VersionPurgeStatusType::Pending);
                            object.version_purge_statuses = dsc.pending_status();
                        } else {
                            object.delete_marker_replication_status = dsc.pending_status();
                        }
                        object.replicate_decision_str = Some(dsc.to_string());
                    }
                }

                let existing = (!skip_stat && gerr.is_none()).then_some(goi);
                AdmittedDelete {
                    idx,
                    object,
                    size,
                    existing,
                    blocked: None,
                }
            }))
            .buffered(DELETE_OBJECTS_PRE_STAT_CONCURRENCY)
            .collect()
            .await;

        // Phase 3 (serial): apply outcomes in the original request order so
        // per-key success/failure reporting is unchanged.
        let mut object_to_delete = Vec::new();
        let mut object_to_delete_idx = Vec::new();
        let mut object_sizes = Vec::new();
        let mut existing_object_infos = Vec::new();
        for admitted in admitted_deletes {
            if let Some(err) = admitted.blocked {
                delete_results[admitted.idx].error = Some(err);
                continue;
            }

            object_sizes.push(admitted.size);
            object_to_delete_idx.push(admitted.idx);
            object_to_delete.push(admitted.object);
            existing_object_infos.push(admitted.existing);
        }

        let cache_adapter = self.object_data_cache();
        let cache_keys_before_delete = object_to_delete
            .iter()
            .map(|object| object.object_name.clone())
            .collect::<Vec<_>>();
        invalidate_object_data_cache_objects_before_mutation(&cache_adapter, &bucket, cache_keys_before_delete.iter()).await;

        let (mut dobjs, errs) = store
            .delete_objects(
                &bucket,
                object_to_delete.clone(),
                ObjectOptions {
                    versioned: version_cfg.enabled(),
                    version_suspended: version_cfg.suspended(),
                    object_lock_delete: Some(StorageObjectLockDeleteOptions { bypass_governance }),
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
                if let Err(err) = enqueue_transitioned_delete_cleanup(
                    store.clone(),
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
                .await
                {
                    warn!(
                        bucket = %bucket,
                        object = %object_to_delete[i].object_name,
                        error = ?err,
                        "failed to persist transitioned object cleanup journal"
                    );
                }
                let creates_delete_marker = object_to_delete[i].version_id.is_none()
                    && version_cfg.prefix_enabled(object_to_delete[i].object_name.as_str())
                    && !version_cfg.suspended();
                if creates_delete_marker {
                    record_bucket_delete_marker_memory(&bucket).await;
                } else {
                    let size = object_sizes[i].max(0) as u64;
                    record_bucket_object_delete_memory(
                        &bucket,
                        size,
                        existing_object_infos[i].is_some() && object_to_delete[i].version_id.is_none(),
                    )
                    .await;
                }
                continue;
            }

            if let Some(err) = err.clone() {
                delete_results[didx].error = Some(s3s::dto::Error {
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
                } else if v.version_id == Some(Uuid::nil()) {
                    Some("null".to_string())
                } else {
                    v.version_id.map(|v| v.to_string())
                },
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

        for dobjs in &delete_results {
            if let Some(dobj) = &dobjs.delete_object
                && replicate_deletes
                && deleted_object_has_pending_replication_delete(dobj)
            {
                let _activity_guard = DeleteTailActivityGuard::new(DeleteTailStage::Replication);
                let mut dobj = dobj.clone();
                if is_dir_object(dobj.object_name.as_str()) && dobj.version_id.is_none() {
                    dobj.version_id = Some(Uuid::nil());
                }

                schedule_replication_delete(dobj, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
            }
        }

        let req_headers = req.headers.clone();
        let notify = current_notify_interface_for_context(self.context.as_deref());
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let deleted_any = delete_results.iter().any(|result| result.delete_object.is_some());
        let notify_bucket = bucket.clone();
        spawn_background_with_context(request_context, async move {
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
        validate_table_catalog_object_mutation(&bucket, &key).await?;

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
        opts.object_lock_delete = Some(StorageObjectLockDeleteOptions {
            bypass_governance: has_bypass_governance_header(&req.headers),
        });
        let force_delete = opts.delete_prefix;

        if opts.delete_prefix && bucket_object_locking_enabled(&bucket).await {
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

        let Some(store) = self.object_store() else {
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

        let cache_adapter = self.object_data_cache();
        // A force (delete_prefix) delete removes every object under `key` as a
        // prefix, so invalidating only the exact key would strand every cached
        // body beneath it. Use the prefix primitive in that branch (ODC-27).
        if force_delete {
            let _ = invalidate_object_data_cache_prefix_before_mutation(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;
        }

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

        if let Err(err) =
            enqueue_transitioned_delete_cleanup(store.clone(), &bucket, &key, &opts, existing_object_info.as_ref()).await
        {
            warn!(
                bucket = %bucket,
                object = %key,
                error = ?err,
                "failed to persist transitioned object cleanup journal"
            );
        }
        if force_delete {
            let _ = invalidate_object_data_cache_prefix_after_delete(&cache_adapter, &bucket, &key).await;
        } else {
            let _ = invalidate_object_data_cache_after_delete_success(&cache_adapter, &bucket, &key).await;
        }

        // Fast in-memory update for immediate quota and admin usage consistency
        if delete_creates_delete_marker(&opts) {
            record_bucket_delete_marker_memory(&bucket).await;
        } else {
            record_bucket_object_delete_memory(&bucket, obj_info.size.max(0) as u64, opts.version_id.is_none()).await;
        }

        if obj_info.name.is_empty() {
            if replicate_force_delete {
                schedule_replication_delete(
                    StorageDeletedObject {
                        object_name: key.clone(),
                        force_delete: true,
                        ..Default::default()
                    },
                    bucket.clone(),
                    REPLICATE_INCOMING_DELETE.to_string(),
                )
                .await;
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
            .filter(|_| should_use_existing_delete_replication_info(&opts));
        let _delete_tail_guard = DeleteTailActivityGuard::new(DeleteTailStage::Tail);
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
            enrich_delete_replication_state_if_needed(&bucket, &mut deleted_object, replication_state_source).await;
            schedule_replication_delete(deleted_object, bucket.clone(), REPLICATE_INCOMING_DELETE.to_string()).await;
        }

        let delete_marker = obj_info.delete_marker;
        let version_id = obj_info.version_id;

        let output = DeleteObjectOutput {
            delete_marker: Some(delete_marker),
            version_id: version_id.map(|v| v.to_string()),
            ..Default::default()
        };

        let event_name = delete_event_name_for_marker(delete_marker);

        helper = helper.event_name(event_name);
        helper = helper
            .object(obj_info)
            .version_id(version_id.map(|v| v.to_string()).unwrap_or_default());

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

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let Some(store) = self.object_store() else {
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
                                error!(bucket, key, error = %e, "Failed to probe children for prefix");
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
            let (checksums, _is_multipart) = info
                .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                .map_err(ApiError::from)?;
            classify_response_checksums(checksums)
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
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        mod_time: obj_info_.mod_time,
                        no_lock: true,
                        ..Default::default()
                    },
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
        let version_id_clone = obj_info_.version_id.map(|v| v.to_string());
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
            extract_ssekms_context_from_headers(&req.headers)?.as_ref(),
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
        if size < 0 {
            return Err(s3_error!(UnexpectedContent));
        }
        validate_object_key(&key, "PUT")?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;
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
                error!(error = ?e, "Archive decoder creation failed");
                s3_error!(InvalidArgument, "get_decoder err")
            })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!(error = ?e, "Archive entry listing failed");
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let extract_options = resolve_put_object_extract_options(&req.headers)?;
        let extract_limits = put_object_extract_limits();
        let extract_quota_snapshot = if let Some(metadata_sys) = self.bucket_metadata_sys() {
            let quota_checker = QuotaChecker::new(metadata_sys);
            match quota_checker.get_quota_config(&bucket).await {
                Ok(quota) => {
                    if let Some(limit) = quota.quota {
                        match quota_checker.get_real_time_usage(&bucket).await {
                            Ok(current_usage) => Some((current_usage, limit)),
                            Err(err) => {
                                warn!(bucket, error = %err, state = "extract_usage_snapshot_failed", "Bucket quota snapshot degraded to allow");
                                None
                            }
                        }
                    } else {
                        None
                    }
                }
                Err(err) => {
                    warn!(bucket, error = %err, state = "extract_quota_snapshot_failed", "Bucket quota snapshot degraded to allow");
                    None
                }
            }
        } else {
            None
        };
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let notify = current_notify_interface_for_context(self.context.as_deref());
        let req_params = extract_params_header(&req.headers);
        let host = get_request_host(&req.headers);
        let port = get_request_port(&req.headers);
        let user_agent = get_request_user_agent(&req.headers);
        let mut wrote_any_entry = false;
        let mut extracted_entry_count = 0usize;
        let mut total_unpacked_size = 0u64;

        while let Some(entry) = entries.next().await {
            let mut f = match entry {
                Ok(f) => f,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!(error = %e, "Archive entry read skipped due to ignore-errors");
                        continue;
                    }
                    error!(error = %e, "Archive entry read failed");
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };
            extracted_entry_count = extracted_entry_count.saturating_add(1);
            validate_put_object_extract_entry_count(extracted_entry_count, extract_limits)?;

            let fpath = match f.path() {
                Ok(path) => path,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!(error = %e, "Archive path decode skipped due to ignore-errors");
                        continue;
                    }
                    return Err(s3_error!(InvalidArgument, "Failed to decode archive entry path"));
                }
            };

            let is_dir = f.header().entry_type().is_dir();
            let fpath = match normalize_extract_entry_key(&fpath.to_string_lossy(), extract_options.prefix.as_deref(), is_dir) {
                Ok(fpath) => fpath,
                Err(err) => {
                    if extract_options.ignore_errors {
                        warn!(error = %err, "Unsafe archive path skipped due to ignore-errors");
                        continue;
                    }
                    return Err(err);
                }
            };
            validate_put_object_extract_entry_path(&fpath, extract_limits)?;
            validate_table_catalog_object_mutation(&bucket, &fpath).await?;

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

            let entry_size = f.header().size().unwrap_or_default();
            validate_put_object_extract_entry_size(&fpath, entry_size, extract_limits)?;
            total_unpacked_size = total_unpacked_size
                .checked_add(entry_size)
                .ok_or_else(|| s3_error!(InvalidArgument, "Archive total unpacked size overflowed while processing entries"))?;
            validate_put_object_extract_total_size(total_unpacked_size, extract_limits)?;
            if let Some((current_usage, quota_limit)) = extract_quota_snapshot
                && current_usage.saturating_add(total_unpacked_size) > quota_limit
            {
                return Err(put_object_extract_quota_exceeded(current_usage, quota_limit));
            }
            let mut size =
                i64::try_from(entry_size).map_err(|_| s3_error!(InvalidArgument, "Archive entry size does not fit into i64"))?;
            // mtime 0 means "unset" in tar headers, and xl.meta cannot represent an
            // epoch mod_time anyway (0 nanos decodes as no-mod_time, making the version
            // unreadable — rustfs#4842), so fall back to the upload time instead.
            let archive_entry_mod_time = f
                .header()
                .mtime()
                .ok()
                .filter(|&modified_at_secs| modified_at_secs > 0)
                .and_then(|modified_at_secs| OffsetDateTime::from_unix_timestamp(modified_at_secs as i64).ok());
            let mut metadata = HashMap::new();
            let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
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
            apply_bucket_default_lock_retention(&bucket, &mut metadata, has_explicit_object_lock_retention).await?;
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

            let should_compress =
                !is_dir && is_disk_compressible(&HeaderMap::new(), &fpath) && size > MIN_DISK_COMPRESSIBLE_SIZE as i64;

            let mut write_plan = WritePlan::new();
            let mut hrd = if is_dir {
                HashReader::from_stream(std::io::Cursor::new(Vec::new()), size, actual_size, None, None, false)
                    .map_err(ApiError::from)?
            } else if should_compress {
                let algorithm = CompressionAlgorithm::default();
                insert_str(&mut metadata, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
                insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

                let hrd = HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?;
                write_plan = write_plan.with_compression(algorithm);
                hrd
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
                ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
            })
            .await?
            {
                effective_sse = Some(material.server_side_encryption.clone());
                effective_kms_key_id = material.kms_key_id.clone();

                write_plan = write_plan.with_encryption(material.write_encryption(None));

                let encryption_metadata = encryption_material_to_metadata(&material)?;
                metadata.extend(encryption_metadata.clone());
                opts.user_defined.extend(encryption_metadata);
            }
            hrd = write_plan.apply(hrd, actual_size).map_err(ApiError::from)?;
            opts.user_defined.extend(metadata);
            let mut reader = PutObjReader::new(hrd);
            let cache_adapter = self.object_data_cache();
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &fpath).await;

            let obj_info = match store.put_object(&bucket, &fpath, &mut reader, &opts).await {
                Ok(info) => info,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!(error = %e, "Archive object write skipped due to ignore-errors");
                        continue;
                    }
                    return Err(ApiError::from(e).into());
                }
            };
            let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &fpath).await;
            if !wrote_any_entry {
                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                wrote_any_entry = true;
            }

            let _manager = get_concurrency_manager();
            let _fpath_clone = fpath.clone();
            let _bucket_clone = bucket.clone();
            let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

            let output = PutObjectOutput {
                e_tag,
                ..Default::default()
            };

            let event_args = rustfs_notify::EventArgs {
                event_name: put_event_name_for_post_object(false),
                bucket_name: bucket.clone(),
                object: convert_ecstore_object_info(obj_info.clone()),
                req_params: req_params.clone(),
                resp_elements: extract_resp_elements(&S3Response::new(output.clone())),
                version_id: version_id.clone(),
                host: host.clone(),
                port,
                user_agent: user_agent.clone(),
            };

            let notify = notify.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
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

async fn bucket_object_locking_enabled(bucket: &str) -> bool {
    get_bucket_metadata(bucket)
        .await
        .is_ok_and(|metadata| metadata.object_locking())
}

/// Fail-closed WORM gate for skipping the pre-PUT lookup
/// (rustfs/backlog#1009): a bucket-metadata read failure counts as "locking
/// enabled" so the existing-object lock validation can never silently
/// disappear on a degraded metadata subsystem.
pub(super) async fn put_prelookup_worm_gate(bucket: &str) -> bool {
    match get_bucket_metadata(bucket).await {
        Ok(metadata) => metadata.object_locking(),
        Err(_) => true,
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
    use http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, Uri};
    use s3s::dto::{
        Delete, DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ExistingObjectReplication,
        ExistingObjectReplicationStatus, ObjectIdentifier, ReplicaModifications, ReplicaModificationsStatus,
        ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, RestoreRequest, SourceSelectionCriteria,
    };
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

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

    #[tokio::test(start_paused = true)]
    async fn cold_fill_disk_admission_preserves_slow_down() {
        let manager = Box::leak(Box::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 1)));
        let primary = match manager.admit_disk_read(Duration::from_millis(1)).await.unwrap() {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("expected primary admission, got {other:?}"),
        };
        let degraded = match manager.admit_disk_read(Duration::from_millis(1)).await.unwrap() {
            DiskReadAdmission::Degraded(permit) => permit,
            other => panic!("expected degraded admission, got {other:?}"),
        };

        let result = DefaultObjectUsecase::acquire_cold_fill_io_planning(manager, "bucket", "object").await;
        assert!(matches!(result, Err(ColdFillError::Storage(StorageError::SlowDown))));

        drop(degraded);
        drop(primary);
    }

    #[tokio::test]
    async fn cold_fill_closed_disk_admission_is_not_slow_down() {
        let manager = Box::leak(Box::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 1)));
        manager.close_disk_read_admission_for_test();

        let result = DefaultObjectUsecase::acquire_cold_fill_io_planning(manager, "bucket", "object").await;
        assert!(matches!(result, Err(ColdFillError::DiskAdmissionClosed)));
    }

    // classify_response_checksums is the single point that splits decrypted checksum
    // pairs into the five s3s-typed fields and the additional-algorithm `extra`
    // headers, replacing five copies of the loop. Lock its behaviour (#1252).
    #[test]
    fn classify_response_checksums_splits_typed_and_extra() {
        // Typed algorithms fill named fields; nothing spills into extra.
        let c = classify_response_checksums(vec![
            ("CRC32".to_string(), "AAAAAA==".to_string()),
            ("SHA256".to_string(), "c2hhMjU2".to_string()),
            ("CRC64NVME".to_string(), "Zm9vYmFyCg==".to_string()),
        ]);
        assert_eq!(c.crc32.as_deref(), Some("AAAAAA=="));
        assert_eq!(c.sha256.as_deref(), Some("c2hhMjU2"));
        assert_eq!(c.crc64nvme.as_deref(), Some("Zm9vYmFyCg=="));
        assert!(c.extra.is_empty(), "typed algorithms must not land in extra");

        // Additional algorithms land in extra keyed by their response-header name.
        let c = classify_response_checksums(vec![
            ("XXHASH3".to_string(), "eHhoMw==".to_string()),
            ("XXHASH64".to_string(), "eHhoNjQ=".to_string()),
            ("XXHASH128".to_string(), "eHhoMTI4".to_string()),
            ("SHA512".to_string(), "c2hhNTEy".to_string()),
            ("MD5".to_string(), "bWQ1".to_string()),
        ]);
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
        let c = classify_response_checksums(vec![(AMZ_CHECKSUM_TYPE.to_string(), "COMPOSITE".to_string())]);
        assert!(c.checksum_type.is_some());
        assert!(c.extra.is_empty() && c.crc32.is_none());

        // Empty input yields an all-default result.
        let c = classify_response_checksums(Vec::<(String, String)>::new());
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

    #[tokio::test]
    async fn build_put_like_object_lock_metadata_rejects_mode_without_retain_until_date() {
        let err = build_put_like_object_lock_metadata(
            "test-bucket",
            None,
            Some(ObjectLockMode::from_static(ObjectLockMode::GOVERNANCE)),
            None,
        )
        .await
        .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED));
    }

    #[tokio::test]
    async fn build_put_like_object_lock_metadata_rejects_retain_until_date_without_mode() {
        let retain_until = Timestamp::from(OffsetDateTime::now_utc().add(time::Duration::days(1)));
        let err = build_put_like_object_lock_metadata("test-bucket", None, None, Some(retain_until))
            .await
            .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some(ERR_OBJECT_LOCK_RETENTION_HEADERS_MUST_BE_PAIRED));
    }

    fn object_info_with_lock_metadata(metadata: HashMap<String, String>) -> ObjectInfo {
        ObjectInfo {
            user_defined: Arc::new(metadata),
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
    fn validate_existing_object_lock_allows_versioned_new_version_with_compliance_retention() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: None,
            ..Default::default()
        };

        validate_existing_object_lock_for_write(&compliance_retained_object_info(), &opts)
            .expect("versioned put should create a new version");
    }

    #[test]
    fn validate_existing_object_lock_allows_versioned_new_version_with_legal_hold() {
        let opts = ObjectOptions {
            versioned: true,
            version_id: None,
            ..Default::default()
        };

        validate_existing_object_lock_for_write(&legal_hold_object_info(), &opts)
            .expect("versioned put should create a new version");
    }

    #[test]
    fn validate_existing_object_lock_blocks_unversioned_compliance_overwrite() {
        let err = validate_existing_object_lock_for_write(&compliance_retained_object_info(), &ObjectOptions::default())
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
        let err = validate_existing_object_lock_for_write(&compliance_retained_object_info(), &opts)
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
        let err = validate_existing_object_lock_for_write(&compliance_retained_object_info(), &opts)
            .expect_err("explicit version overwrite should still be blocked");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
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
        assert_eq!(
            normalize_snowball_prefix(" /batch/incoming/ ").unwrap(),
            Some("batch/incoming".to_string())
        );
        assert_eq!(normalize_snowball_prefix("///").unwrap(), None);
    }

    #[test]
    fn normalize_snowball_prefix_rejects_parent_dir_components() {
        assert!(normalize_snowball_prefix("../victim-bucket").is_err());
        assert!(normalize_snowball_prefix("safe/../../victim-bucket").is_err());
        assert!(normalize_snowball_prefix("safe\\..\\victim-bucket").is_err());
    }

    #[test]
    fn normalize_extract_entry_key_applies_prefix_and_directory_suffix() {
        assert_eq!(
            normalize_extract_entry_key("nested/path.txt", Some("imports"), false).unwrap(),
            "imports/nested/path.txt"
        );
        assert_eq!(
            normalize_extract_entry_key("nested/dir/", Some("imports"), true).unwrap(),
            "imports/nested/dir/"
        );
        assert_eq!(normalize_extract_entry_key("top-level", None, false).unwrap(), "top-level");
    }

    #[test]
    fn normalize_extract_entry_key_rejects_bucket_escape_paths() {
        assert!(normalize_extract_entry_key("../victim-bucket/evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("safe/../../victim-bucket/evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("safe\\..\\victim-bucket\\evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("evil.txt", Some("../victim-bucket"), false).is_err());
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
    fn should_buffer_get_object_in_memory_respects_hard_safety_cap() {
        let info = ObjectInfo::default();
        let configured_threshold = 20_i64 * 1024 * 1024 * 1024;
        let response_len = 80_i64 * 1024 * 1024;
        let should_buffer =
            should_buffer_get_object_in_memory_with_threshold(&info, response_len, None, false, configured_threshold, 1, true);

        assert!(
            !should_buffer,
            "64MiB hard cap must force streaming when response exceeds cap even if configured threshold is much higher"
        );
    }

    #[test]
    fn should_buffer_get_object_in_memory_allows_small_non_range_requests() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            Some(1),
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            None,
            true,
            configured_threshold,
            1,
            true
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_requires_seek_buffer_opt_in() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024,
            None,
            false,
            configured_threshold,
            1,
            false
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_respects_configured_threshold_below_cap() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold + 1,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_rejects_unknown_lengths_and_disabled_thresholds() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            0,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            -1,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(&info, 1024, None, false, 0, 1, true));
    }

    #[test]
    fn should_buffer_get_object_in_memory_reduces_threshold_under_concurrency() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            32,
            true
        ));
        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            4_i64 * 1024 * 1024,
            None,
            false,
            configured_threshold,
            rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            true
        ));
    }

    /// Polls the cache until the detached fill (ODC-15) populates the entry, so
    /// a follow-up GET is a deterministic hit rather than racing the fill task.
    async fn wait_for_cache_hit(
        adapter: &crate::app::object_data_cache::ObjectDataCacheAdapter,
        bucket: &str,
        object: &str,
        etag: &str,
        size: u64,
    ) {
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket,
            object,
            version_id: None,
            etag,
            size,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        for _ in 0..400 {
            if matches!(adapter.lookup_body(&plan).await, rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("detached fill did not populate the cache within the timeout");
    }

    struct ReadProbeReader {
        reads: Arc<AtomicUsize>,
    }

    impl AsyncRead for ReadProbeReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            Poll::Ready(Ok(()))
        }
    }

    struct DataProbeReader {
        reads: Arc<AtomicUsize>,
        data: std::io::Cursor<Vec<u8>>,
    }

    struct ColdFillMatrixReader {
        inner: tokio::io::DuplexStream,
        first_poll_recorded: bool,
        completion_recorded: bool,
        first_polls: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl AsyncRead for ColdFillMatrixReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if !self.first_poll_recorded {
                self.first_poll_recorded = true;
                self.first_polls.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let before = buf.filled().len();
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len().saturating_sub(before);
                    self.bytes_read.fetch_add(read, AtomicOrdering::Relaxed);
                    if read == 0 && !self.completion_recorded {
                        self.completion_recorded = true;
                        self.completed.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    impl AsyncRead for DataProbeReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);

            let remaining = buf.remaining();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source = self.data.get_ref();
            if position >= source.len() {
                return Poll::Ready(Ok(()));
            }

            let end = position.saturating_add(remaining).min(source.len());
            buf.put_slice(&source[position..end]);
            self.data.set_position(u64::try_from(end).unwrap_or(u64::MAX));
            Poll::Ready(Ok(()))
        }
    }

    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    // Emits `fail_after` bytes from `data`, then returns a hard read error. Used
    // to inject the "read K bytes then Err" partial-read case (#1324).
    struct ErrAfterReader {
        data: std::io::Cursor<Vec<u8>>,
        fail_after: usize,
        emitted: usize,
    }

    impl AsyncRead for ErrAfterReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.emitted >= self.fail_after {
                return Poll::Ready(Err(std::io::Error::other("injected mid-stream read error")));
            }
            let remaining = buf.remaining();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            let want = (self.fail_after - self.emitted).min(remaining);
            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source = self.data.get_ref();
            let end = position.saturating_add(want).min(source.len());
            if end <= position {
                return Poll::Ready(Err(std::io::Error::other("injected mid-stream read error")));
            }
            let chunk_len = end - position;
            buf.put_slice(&source[position..end]);
            self.data.set_position(u64::try_from(end).unwrap_or(u64::MAX));
            self.emitted += chunk_len;
            Poll::Ready(Ok(()))
        }
    }

    fn cursor_reader(bytes: &[u8]) -> std::io::Cursor<Vec<u8>> {
        std::io::Cursor::new(bytes.to_vec())
    }

    // #1324: the strict materialization helper is the shared exact-length gate
    // for the encrypted, seek, and cache memory branches. For a declared length N
    // only an exact N-byte read succeeds; a short read (N-1), an over-long read
    // (N+1), and a mid-stream read error all hard-fail. This is the reversal
    // guard for every one of those sources at once: restoring WARN-and-serve or a
    // partial fallback would flip the short/over-long/error assertions to Ok.
    #[tokio::test]
    async fn strict_materialize_object_body_requires_exact_length() {
        // Exact length: the only accepted outcome.
        let buf = strict_materialize_object_body(cursor_reader(b"hello"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ)
            .await
            .expect("exact-length read must materialize");
        assert_eq!(buf, b"hello");
        assert_eq!(buf.capacity(), 5, "exact materialization must allocate only the declared body length");

        let exact_large = vec![7_u8; 64 * 1024];
        let buf = strict_materialize_object_body(
            std::io::Cursor::new(exact_large.clone()),
            exact_large.len(),
            GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ,
        )
        .await
        .expect("64 KiB exact-length read must materialize");
        assert_eq!(buf.capacity(), exact_large.len());

        let mut overlong_large = exact_large;
        overlong_large.push(9);
        let overlong = strict_materialize_object_body(
            std::io::Cursor::new(overlong_large),
            64 * 1024,
            GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ,
        )
        .await;
        assert!(matches!(
            overlong,
            Err(StrictMaterializeError::LengthMismatch {
                expected: 65_536,
                actual: 65_537
            })
        ));

        // Short read (actual = expected - 1): a clean EOF before the declared
        // length must be a hard error, never a truncated served body.
        let short = strict_materialize_object_body(cursor_reader(b"hell"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(
                short,
                Err(StrictMaterializeError::LengthMismatch {
                    expected: 5,
                    actual: 4,
                    ..
                })
            ),
            "short read must fail with a length mismatch, got {short:?}",
            short = short.as_ref().map(|b| b.len())
        );

        // Over-long read (actual = expected + 1): must fail rather than silently
        // truncate to the committed Content-Length.
        let long = strict_materialize_object_body(cursor_reader(b"hello!"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(long, Err(StrictMaterializeError::LengthMismatch { expected: 5, actual: 6 })),
            "over-long read must fail with a length mismatch, got {long:?}",
            long = long.as_ref().map(|b| b.len())
        );

        // Read K bytes then Err: must surface the read error and never return the
        // partially consumed buffer (which the caller could otherwise re-stream).
        let reader = ErrAfterReader {
            data: cursor_reader(b"hello"),
            fail_after: 3,
            emitted: 0,
        };
        let errored = strict_materialize_object_body(reader, 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(errored, Err(StrictMaterializeError::Read { consumed: 3, .. })),
            "a mid-stream read error must be reported as a read failure"
        );
    }

    #[test]
    fn cold_fill_zero_timeout_policy_disables_deadline() {
        let policy = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::ZERO,
            ..GetObjectTimeoutPolicy::default()
        };
        let wrapper = RequestTimeoutWrapper::with_request_id(policy.clone(), "cold-fill-zero-timeout");
        assert!(cold_fill_deadline(&wrapper, &policy, 1).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_producer_deadline_is_capped_at_ten_minutes() {
        let disabled = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::ZERO,
            ..GetObjectTimeoutPolicy::default()
        };
        let now = tokio::time::Instant::now();
        assert_eq!(cold_fill_producer_deadline(&disabled, 1) - now, Duration::from_secs(600));

        let long = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::from_secs(3600),
            enable_dynamic_timeout: false,
            ..GetObjectTimeoutPolicy::default()
        };
        let now = tokio::time::Instant::now();
        assert_eq!(cold_fill_producer_deadline(&long, 1) - now, Duration::from_secs(600));
    }

    #[tokio::test]
    async fn cold_fill_startup_wait_stops_when_last_consumer_cancels() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let waiting = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { await_cold_fill_startup(std::future::pending::<()>(), &cancellation, None).await }
        });
        tokio::task::yield_now().await;

        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("startup wait must observe cancellation")
            .expect("startup wait task must not panic");
        assert!(matches!(result, Err(ColdFillStartupWaitError::Cancelled)));
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_startup_wait_with_deadline_still_observes_cancellation() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let waiting = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { await_cold_fill_startup(std::future::pending::<()>(), &cancellation, Some(deadline)).await }
        });
        tokio::task::yield_now().await;

        cancellation.cancel();

        let result = waiting.await.expect("startup wait task must not panic");
        assert!(matches!(result, Err(ColdFillStartupWaitError::Cancelled)));
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_startup_wait_reports_deadline_exceeded() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1);

        let result = await_cold_fill_startup(std::future::pending::<()>(), &cancellation, Some(deadline)).await;

        assert!(matches!(result, Err(ColdFillStartupWaitError::DeadlineExceeded)));
    }

    #[tokio::test]
    async fn cold_fill_late_miss_second_chance_hits_without_reader() {
        let adapter = ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
            mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
            max_bytes: 1024 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 1024,
            min_free_memory_percent: 0,
            fill_concurrency_max: 1,
            ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
        })
        .expect("second-chance cache config must be valid");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "late-bucket",
            object: "late-object",
            version_id: None,
            etag: "late-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        let request_lookups = adapter.cache().stats().lookups;
        assert_eq!(request_lookups, 1, "the authoritative request lookup must be counted once");

        let reservation = adapter.reserve_body(&plan).expect("late producer must reserve");
        let reserved = reservation.wrap_bytes(Bytes::from_static(b"body"));
        let _ = adapter.fill_reserved_body(&plan, reserved).await;
        let coordinator = adapter.cold_fill_coordinator();
        let cache_key = plan.key().cloned().expect("late plan must be cacheable");
        let adapter = Arc::new(adapter);
        let readers = Arc::new(AtomicUsize::new(0));
        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, {
            let adapter = Arc::clone(&adapter);
            let readers = Arc::clone(&readers);
            move |producer| {
                let adapter = Arc::clone(&adapter);
                let plan = plan.clone();
                let readers = Arc::clone(&readers);
                async move {
                    if let Some(body) = lookup_cold_fill_second_chance(&adapter, &plan).await {
                        producer.finish_shared(Ok(body));
                        return;
                    }
                    readers.fetch_add(1, AtomicOrdering::Relaxed);
                    producer.bypass();
                }
            }
        })
        .await;
        let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
            panic!("late request must observe the completed fill, got {outcome:?}");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(
            adapter.cache().stats().lookups,
            request_lookups,
            "the producer second chance must not count another request lookup"
        );
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn cold_fill_timeout_is_shared_and_releases_resources() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("timeout cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "timeout-bucket",
            object: "timeout-object",
            version_id: None,
            etag: "timeout-etag",
            size: 1,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let key = plan.key().cloned().expect("timeout body must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(mut producer) = coordinator.join(key.clone()) else {
            panic!("first timeout request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let disk_permits = Arc::new(tokio::sync::Semaphore::new(1));
        let disk_gate = Arc::clone(&disk_permits);
        let readers = Arc::new(AtomicUsize::new(0));
        let reader_count = Arc::clone(&readers);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            move || async move {
                let permit = disk_gate
                    .acquire_owned()
                    .await
                    .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                let mut io = DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                io.disk_permit = Some(permit.into());
                Ok(io)
            },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(GetObjectReader {
                    stream: Box::new(PendingReader),
                    object_info: ObjectInfo {
                        size: 1,
                        actual_size: 1,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 1,
                deadline: Some(tokio::time::Instant::now() + Duration::from_millis(20)),
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while readers.load(AtomicOrdering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer reader must open");
        let ColdFillRole::Wait(follower) = coordinator.join(key.clone()) else {
            panic!("second timeout request must follow");
        };

        let (leader_result, follower_result) =
            tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(leader.wait(), follower.wait()) })
                .await
                .expect("typed timeout must wake all waiters");
        assert!(matches!(
            leader_result,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert!(matches!(
            follower_result,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(disk_permits.available_permits(), 1);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        producer_task.await.expect("producer task must join");
        assert!(adapter.reserve_body(&plan).is_some(), "timeout must release the body reservation");
        let ColdFillRole::Produce(successor) = coordinator.join(key) else {
            panic!("timeout must release the session for a successor");
        };
        drop(successor);
    }

    #[tokio::test]
    async fn cold_fill_survives_leader_request_cancellation_without_second_producer() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("cancellation cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "cancel-bucket",
            object: "cancel-object",
            version_id: None,
            etag: "cancel-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let key = plan.key().cloned().expect("cancellation body must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(mut producer) = coordinator.join(key.clone()) else {
            panic!("first cancellation request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let readers = Arc::new(AtomicUsize::new(0));
        let reader_count = Arc::clone(&readers);
        let writer_slot = Arc::new(Mutex::new(None));
        let writer_output = Arc::clone(&writer_slot);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                let (writer, reader) = tokio::io::duplex(16);
                *writer_output.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer);
                Ok(GetObjectReader {
                    stream: Box::new(reader),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while readers.load(AtomicOrdering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation producer reader must open");
        let ColdFillRole::Wait(follower) = coordinator.join(key.clone()) else {
            panic!("second cancellation request must follow");
        };
        drop(leader);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
        let ColdFillRole::Wait(late) = coordinator.join(key) else {
            panic!("leader cancellation must not open a successor session");
        };
        drop(late);

        let mut writer = writer_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("reader factory must publish writer");
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"body")
            .await
            .expect("body write must succeed");
        tokio::io::AsyncWriteExt::shutdown(&mut writer)
            .await
            .expect("body writer must close");
        let ColdFillWaitOutcome::Ready(result) = follower.wait().await else {
            panic!("follower must receive producer result");
        };
        assert_eq!(result.expect("surviving producer must succeed"), Bytes::from_static(b"body"));
        producer_task.await.expect("producer task must join");
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cold_fill_reservation_rejection_streams_without_materializing() {
        let coordinator = Arc::new(crate::app::object_data_cache::ColdFillCoordinator::default());
        let plan = rustfs_object_data_cache::ObjectDataCacheGetPlan::Disabled;
        let ColdFillRole::Produce(mut producer) = coordinator.join(rustfs_object_data_cache::ObjectDataCacheKey::new(
            "bucket",
            "object",
            None,
            "etag",
            4,
            rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        )) else {
            panic!("first rejected reservation request must produce");
        };
        let leader = producer.waiter();
        let permits = Arc::new(AtomicUsize::new(0));
        let readers = Arc::new(AtomicUsize::new(0));
        let permit_count = Arc::clone(&permits);
        let reader_count = Arc::clone(&readers);
        start_cold_fill_producer(
            producer,
            None,
            move || async move {
                permit_count.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager()))
            },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                Err(StorageError::other("reader must not open"))
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::new(ObjectDataCacheAdapter::disabled()),
                engine_plan: plan,
            },
        )
        .await;
        assert!(matches!(leader.wait().await, ColdFillWaitOutcome::Bypass));
        assert_eq!(permits.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0);

        let fallback_reads = Arc::new(AtomicUsize::new(0));
        let fallback_reader = DataProbeReader {
            reads: Arc::clone(&fallback_reads),
            data: std::io::Cursor::new(b"body".to_vec()),
        };
        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let mut fallback_body = DefaultObjectUsecase::build_get_object_body(
            fallback_reader,
            &info,
            4,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            "bucket",
            "object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("reservation bypass must construct the normal streaming fallback");
        let chunk = fallback_body
            .next()
            .await
            .expect("fallback stream must yield a body chunk")
            .expect("fallback stream must not fail");
        assert_eq!(chunk, Bytes::from_static(b"body"));
        assert!(fallback_reads.load(AtomicOrdering::Relaxed) > 0);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0, "cold-fill materialization must remain unopened");
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_internal_movement_and_restore_reads_never_join_sessions() {
        let coordinator = Arc::new(crate::app::object_data_cache::ColdFillCoordinator::default());
        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let mut restore = ObjectOptions::default();
        restore.transition.restore_request.days = Some(1);
        let cases = [
            ObjectOptions {
                raw_data_movement_read: true,
                ..Default::default()
            },
            ObjectOptions {
                data_movement: true,
                ..Default::default()
            },
            restore,
        ];

        for opts in &cases {
            assert!(matches!(
                lookup_get_object_body_cache_hook("bucket", "object", &None, opts, &info).await,
                GetObjectBodyCacheHookLookup::Ineligible
            ));
            assert_eq!(coordinator.active_session_count_for_test(), 0);
        }

        let delete_marker = ObjectInfo {
            delete_marker: true,
            etag: Some("delete-marker-etag".to_string()),
            ..Default::default()
        };
        let delete_marker_part = ObjectOptions {
            part_number: Some(2),
            ..Default::default()
        };
        assert!(matches!(
            lookup_get_object_body_cache_hook("bucket", "object", &None, &delete_marker_part, &delete_marker).await,
            GetObjectBodyCacheHookLookup::Ineligible
        ));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_generation_change_bypasses_before_opening_body() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("generation retry cache config must be valid"),
        );
        let request = |data_dir_u128| rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "generation-bucket",
            object: "generation-object",
            version_id: None,
            etag: "generation-etag",
            size: 4,
            data_dir_u128: Some(data_dir_u128),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        };
        let initial_plan = adapter.plan_get(request(1));
        let changed_plan = GetObjectBodyCachePlan::Cacheable(adapter.plan_get(request(2)));
        let cache_key = initial_plan.key().cloned().expect("initial generation must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let body_opens = Arc::new(AtomicUsize::new(0));
        let producer_attempts = Arc::new(AtomicUsize::new(0));

        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, {
            let body_opens = Arc::clone(&body_opens);
            let producer_attempts = Arc::clone(&producer_attempts);
            move |producer| {
                let body_opens = Arc::clone(&body_opens);
                let producer_attempts = Arc::clone(&producer_attempts);
                let changed_plan = changed_plan.clone();
                let initial_plan = initial_plan.clone();
                async move {
                    producer_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(producer) = retain_cold_fill_producer_for_matching_plan(producer, &changed_plan, &initial_plan)
                    else {
                        return;
                    };
                    body_opens.fetch_add(1, AtomicOrdering::Relaxed);
                    producer.bypass();
                }
            }
        })
        .await;

        assert!(matches!(outcome, ColdFillCoordinateOutcome::Bypass));
        assert_eq!(producer_attempts.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(body_opens.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    async fn real_cold_fill_test_context() -> (Arc<ECStore>, Arc<AppContext>) {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("real cold-fill tests require an ambient AppContext");
        let context = temp_env::with_vars(
            [
                (rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, Some("true")),
                (rustfs_config::ENV_OBJECT_DATA_CACHE_MODE, Some("fill_materialize_enabled")),
                (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_BYTES, Some("8388608")),
                (rustfs_config::ENV_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES, Some("2097152")),
                (rustfs_config::ENV_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT, Some("0")),
            ],
            || Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms())),
        );
        assert!(context.object_data_cache().materialize_fill_enabled());
        (store, context)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn transitioned_delete_cleanup_persists_identity_bound_and_legacy_journals() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let identity = [11_u8; 32];
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(identity),
        );
        let mut current = ObjectInfo {
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        current.transitioned_object.status = lifecycle::TRANSITION_COMPLETE.to_string();
        current.transitioned_object.tier = "WARM".to_string();
        current.transitioned_object.name = "remote/identity-bound".to_string();
        current.transitioned_object.version_id = "remote-version".to_string();

        let journal_name = |remote_object: &str, backend_identity: Option<[u8; 32]>| {
            use sha2::{Digest, Sha256};

            let mut hasher = Sha256::new();
            hasher.update(b"WARM");
            hasher.update([0]);
            hasher.update(remote_object.as_bytes());
            hasher.update([0]);
            hasher.update(b"remote-version");
            if let Some(backend_identity) = backend_identity {
                hasher.update([0]);
                hasher.update(backend_identity);
            }
            format!("ilm/tier-delete-journal/{}.json", rustfs_utils::crypto::hex(hasher.finalize().as_slice()))
        };

        enqueue_transitioned_delete_cleanup(store.clone(), "bucket", "identity-bound", &ObjectOptions::default(), Some(&current))
            .await
            .expect("normal transitioned delete should persist an identity-bound journal");
        let mut identity_bound = store
            .get_object_reader(
                ".rustfs.sys",
                &journal_name("remote/identity-bound", Some(identity)),
                None,
                http::HeaderMap::new(),
                &ObjectOptions::default(),
            )
            .await
            .expect("identity-bound journal should be readable");
        let mut identity_bound_data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut identity_bound.stream, &mut identity_bound_data)
            .await
            .expect("identity-bound journal body should be readable");
        let identity_bound: serde_json::Value =
            serde_json::from_slice(&identity_bound_data).expect("identity-bound journal should decode as JSON");
        assert_eq!(identity_bound["version"], serde_json::json!(2));
        assert_eq!(identity_bound["backend_identity"], serde_json::json!(identity));

        current.user_defined = Arc::new(HashMap::new());
        current.transitioned_object.name = "remote/legacy".to_string();
        enqueue_transitioned_delete_cleanup(
            store.clone(),
            "bucket",
            "legacy",
            &ObjectOptions {
                delete_prefix: true,
                ..Default::default()
            },
            Some(&current),
        )
        .await
        .expect("legacy force-delete cleanup should persist a fail-closed v1 journal");
        let mut legacy = store
            .get_object_reader(
                ".rustfs.sys",
                &journal_name("remote/legacy", None),
                None,
                http::HeaderMap::new(),
                &ObjectOptions::default(),
            )
            .await
            .expect("legacy journal should be readable");
        let mut legacy_data = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut legacy.stream, &mut legacy_data)
            .await
            .expect("legacy journal body should be readable");
        let legacy: serde_json::Value = serde_json::from_slice(&legacy_data).expect("legacy journal should decode as JSON");
        assert_eq!(legacy["version"], serde_json::json!(1));
        assert_eq!(legacy["backend_identity"], serde_json::Value::Null);
    }

    async fn put_real_cold_fill_object(store: &Arc<ECStore>, bucket: &str, object: &str, body: &[u8]) -> ObjectInfo {
        let mut reader = PutObjReader::from_vec(body.to_vec());
        store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("real cold-fill test object must be written")
    }

    fn real_cold_fill_plan(
        adapter: &ObjectDataCacheAdapter,
        bucket: &str,
        object: &str,
        info: &ObjectInfo,
    ) -> rustfs_object_data_cache::ObjectDataCacheGetPlan {
        let length = info
            .get_actual_size()
            .expect("real cold-fill test metadata must expose plaintext size");
        let GetObjectBodyCachePlan::Cacheable(plan) = build_get_object_body_cache_plan(
            adapter,
            GetObjectBodyCacheRequest {
                bucket,
                key: object,
                info,
                response_content_length: length,
                has_range: false,
                part_number: None,
                encryption_applied: false,
            },
        ) else {
            panic!("real cold-fill test object must be cacheable");
        };
        plan
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_rejects_conditions_before_joining_cold_fill() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-condition-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill condition bucket must be created");
        let body = vec![b'a'; 1_300_000];
        let info = put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let adapter = context.object_data_cache();
        let plan = real_cold_fill_plan(&adapter, &bucket, object, &info);
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(producer) =
            coordinator.join(plan.key().cloned().expect("real cold-fill plan must expose its key"))
        else {
            panic!("test must reserve the initial cold-fill producer");
        };

        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("real cold-fill GET input must build");
        let mut req = build_request(input, Method::GET);
        let etag = info.etag.expect("real cold-fill test object must have an ETag");
        req.headers.insert(
            http::header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("\"{etag}\"")).expect("ETag header must be valid"),
        );
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let result = tokio::time::timeout(Duration::from_secs(2), usecase.execute_get_object(req))
            .await
            .expect("conditional GET must not wait for the reserved cold-fill session")
            .expect_err("matching If-None-Match must reject the GET");

        assert_eq!(result.code(), &S3ErrorCode::NotModified);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        drop(producer);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_maps_cold_fill_session_rejection_to_slow_down_without_opening_reader() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-rejected-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill rejection bucket must be created");
        let body = vec![b'a'; 1_300_000];
        let info = put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let adapter = context.object_data_cache();
        let plan = real_cold_fill_plan(&adapter, &bucket, object, &info);
        let cache_key = plan.key().cloned().expect("real cold-fill plan must expose its key");
        let coordinator = adapter.cold_fill_coordinator();
        let mut held_producers = Vec::new();
        for index in 0..2048 {
            let saturation_key = rustfs_object_data_cache::ObjectDataCacheKey::new(
                "cold-fill-saturation",
                format!("object-{index}"),
                None,
                "etag",
                4,
                rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
            );
            match coordinator.join(saturation_key) {
                ColdFillRole::Produce(producer) => held_producers.push(producer),
                ColdFillRole::Rejected => break,
                ColdFillRole::Wait(_) | ColdFillRole::Bypass => panic!("unique saturation keys must produce or reject"),
            }
        }
        assert_eq!(coordinator.active_session_count_for_test(), held_producers.len());
        assert!(!held_producers.is_empty(), "saturation must reserve cold-fill sessions");

        let reader_opens = Arc::new(AtomicU64::new(0));
        *COLD_FILL_READER_OPEN_PROBE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((cache_key, Arc::clone(&reader_opens)));
        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("real cold-fill rejection GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let result = tokio::time::timeout(Duration::from_secs(2), usecase.execute_get_object(build_request(input, Method::GET)))
            .await
            .expect("rejected real GET must not wait for a cold-fill session")
            .expect_err("rejected real GET must return an S3 error");
        *COLD_FILL_READER_OPEN_PROBE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        assert_eq!(result.code(), &S3ErrorCode::SlowDown);
        assert_eq!(reader_opens.load(Ordering::Relaxed), 0, "rejected GET must not open its body reader");
        assert_eq!(coordinator.active_session_count_for_test(), held_producers.len());
        drop(held_producers);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_generation_change_bypasses_old_cold_fill_plan() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-generation-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill generation bucket must be created");
        let initial_body = vec![b'a'; 1_300_000];
        let changed_body = vec![b'b'; initial_body.len()];
        let initial_info = put_real_cold_fill_object(&store, &bucket, object, &initial_body).await;
        let adapter = context.object_data_cache();
        let initial_plan = real_cold_fill_plan(&adapter, &bucket, object, &initial_info);
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(producer) =
            coordinator.join(initial_plan.key().cloned().expect("real cold-fill plan must expose its key"))
        else {
            panic!("test must reserve the initial cold-fill producer");
        };

        let input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("real cold-fill GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let request = tokio::spawn(async move { usecase.execute_get_object(build_request(input, Method::GET)).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while coordinator.global_waiter_count_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real GET must join the reserved cold-fill session");

        let changed_info = put_real_cold_fill_object(&store, &bucket, object, &changed_body).await;
        assert_ne!(initial_info.etag, changed_info.etag);
        producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));

        let mut response = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("generation-changing GET must complete")
            .expect("generation-changing GET task must join")
            .expect("generation-changing GET must fall back successfully");
        let mut response_body = response.output.body.take().expect("GET response must include a body");
        let mut actual = Vec::with_capacity(changed_body.len());
        while let Some(chunk) = response_body.next().await {
            actual.extend_from_slice(&chunk.expect("fallback body chunk must be readable"));
        }

        assert_eq!(actual, changed_body);
        assert!(matches!(
            adapter.lookup_body(&initial_plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_open_error_retries_once_then_single_successor_succeeds() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("open retry cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "open-retry-bucket",
            object: "open-retry-object",
            version_id: None,
            etag: "open-retry-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("open retry plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let open_attempts = Arc::new(AtomicUsize::new(0));
        let open_attempts_for_start = Arc::clone(&open_attempts);

        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
            let reservation = adapter.reserve_body(&plan);
            let adapter = Arc::clone(&adapter);
            let plan = plan.clone();
            let open_attempts = Arc::clone(&open_attempts_for_start);
            async move {
                start_cold_fill_producer(
                    producer,
                    reservation,
                    || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
                    move || async move {
                        let attempt = open_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                        if attempt == 0 {
                            return Err(StorageError::other("first open fails"));
                        }
                        Ok(GetObjectReader {
                            stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                            object_info: ObjectInfo {
                                size: 4,
                                actual_size: 4,
                                ..Default::default()
                            },
                            buffered_body: Some(Bytes::from_static(b"body")),
                            body_source: GetObjectBodySource::HookMissed,
                        })
                    },
                    ColdFillProducerExecution {
                        expected: 4,
                        deadline: None,
                        adapter,
                        engine_plan: plan,
                    },
                )
                .await
            }
        })
        .await;

        let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
            panic!("the unique successor must publish the body");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_open_timeout_retries_once_then_is_terminal() {
        tokio::time::pause();
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("open timeout cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "open-timeout-bucket",
            object: "open-timeout-object",
            version_id: None,
            etag: "open-timeout-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("open timeout plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let open_attempts = Arc::new(AtomicUsize::new(0));

        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        let task = tokio::spawn({
            let adapter = Arc::clone(&adapter);
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            let open_attempts = Arc::clone(&open_attempts);
            async move {
                coordinate_cold_fill(&coordinator, cache_key, None, Some(deadline), move |producer| {
                    let adapter = Arc::clone(&adapter);
                    let plan = plan.clone();
                    let open_attempts = Arc::clone(&open_attempts);
                    let reservation = adapter.reserve_body(&plan);
                    let producer_deadline = producer.deadline();
                    async move {
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
                            move || async move {
                                open_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                                std::future::pending::<Result<GetObjectReader, StorageError>>().await
                            },
                            ColdFillProducerExecution {
                                expected: 4,
                                deadline: producer_deadline,
                                adapter,
                                engine_plan: plan,
                            },
                        )
                        .await
                    }
                })
                .await
            }
        });
        while open_attempts.load(AtomicOrdering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(11)).await;
        let outcome = task.await.expect("open timeout task must join");
        assert!(matches!(
            outcome,
            ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));

        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_pre_reader_failure_promotes_one_of_two_thousand_waiters() {
        const REQUESTS: usize = 2000;
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("successor cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "successor-bucket",
            object: "successor-object",
            version_id: None,
            etag: "successor-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("successor plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let admission_attempts = Arc::new(AtomicUsize::new(0));
        let open_attempts = Arc::new(AtomicUsize::new(0));
        let first_open_release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..REQUESTS {
            let adapter = Arc::clone(&adapter);
            let coordinator = Arc::clone(&coordinator);
            let cache_key = cache_key.clone();
            let plan = plan.clone();
            let admission_attempts = Arc::clone(&admission_attempts);
            let open_attempts = Arc::clone(&open_attempts);
            let first_open_release = Arc::clone(&first_open_release);
            tasks.spawn(async move {
                coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
                    let reservation = adapter.reserve_body(&plan);
                    let adapter = Arc::clone(&adapter);
                    let plan = plan.clone();
                    let admission_attempts = Arc::clone(&admission_attempts);
                    let open_attempts = Arc::clone(&open_attempts);
                    let first_open_release = Arc::clone(&first_open_release);
                    async move {
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            move || async move {
                                admission_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                                Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager()))
                            },
                            move || async move {
                                if open_attempts.fetch_add(1, AtomicOrdering::Relaxed) == 0 {
                                    first_open_release
                                        .acquire()
                                        .await
                                        .expect("first open release gate must remain open")
                                        .forget();
                                    return Err(StorageError::other("first open fails"));
                                }
                                Ok(GetObjectReader {
                                    stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                                    object_info: ObjectInfo {
                                        size: 4,
                                        actual_size: 4,
                                        ..Default::default()
                                    },
                                    buffered_body: Some(Bytes::from_static(b"body")),
                                    body_source: GetObjectBodySource::HookMissed,
                                })
                            },
                            ColdFillProducerExecution {
                                expected: 4,
                                deadline: None,
                                adapter,
                                engine_plan: plan,
                            },
                        )
                        .await
                    }
                })
                .await
            });
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if coordinator.global_waiter_count_for_test() == REQUESTS - 1 && open_attempts.load(AtomicOrdering::Relaxed) == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all followers must join before the first open fails");
        first_open_release.add_permits(1);

        while let Some(result) = tasks.join_next().await {
            let ColdFillCoordinateOutcome::Ready(Ok(body)) = result.expect("successor request task must join") else {
                panic!("all followers must receive the successor body");
            };
            assert_eq!(body, Bytes::from_static(b"body"));
        }
        assert_eq!(admission_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    fn install_cold_fill_publication_barrier(
        plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
    ) -> Arc<ColdFillPublicationBarrier> {
        let barrier = Arc::new(ColdFillPublicationBarrier {
            reached: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let key = plan.key().cloned().expect("publication barrier plan must be cacheable");
        *COLD_FILL_PUBLICATION_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((key, Arc::clone(&barrier)));
        barrier
    }

    fn clear_cold_fill_publication_barrier() {
        *COLD_FILL_PUBLICATION_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn publication_test_adapter() -> Arc<ObjectDataCacheAdapter> {
        Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("publication cache config must be valid"),
        )
    }

    fn publication_test_plan(adapter: &ObjectDataCacheAdapter, object: &str) -> rustfs_object_data_cache::ObjectDataCacheGetPlan {
        adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "publication-bucket",
            object,
            version_id: None,
            etag: "publication-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        })
    }

    #[tokio::test]
    #[serial_test::serial(cold_fill_publication_barrier)]
    async fn cold_fill_last_consumer_cancel_releases_session_before_publication_barrier() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "cancel");
        let barrier = install_cold_fill_publication_barrier(&plan);
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("publication plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("publication request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let disk_permits = Arc::new(tokio::sync::Semaphore::new(1));
        let disk_gate = Arc::clone(&disk_permits);
        let producer_task = tokio::spawn(scope_cold_fill_disk_permit_owner_for_test(
            ColdFillDiskPermitOwner::Producer,
            start_cold_fill_producer(
                producer,
                reservation,
                move || async move {
                    let permit = disk_gate
                        .acquire_owned()
                        .await
                        .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                    let mut io = DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                    io.disk_permit = Some(permit.into());
                    Ok(io)
                },
                || async {
                    Ok(GetObjectReader {
                        stream: Box::new(std::io::Cursor::new(b"body".to_vec())),
                        object_info: ObjectInfo {
                            size: 4,
                            actual_size: 4,
                            ..Default::default()
                        },
                        buffered_body: None,
                        body_source: GetObjectBodySource::HookMissed,
                    })
                },
                ColdFillProducerExecution {
                    expected: 4,
                    deadline: None,
                    adapter: Arc::clone(&adapter),
                    engine_plan: plan.clone(),
                },
            ),
        ));

        let reached = barrier.reached.acquire().await.expect("publication barrier must remain open");
        reached.forget();
        assert_eq!(
            disk_permits.available_permits(),
            1,
            "the producer disk permit and its gauge guard must end before publication"
        );
        let clear_adapter = Arc::clone(&adapter);
        let clear = tokio::spawn(async move {
            clear_adapter
                .clear(rustfs_object_data_cache::ObjectDataCacheInvalidationReason::Manual)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!clear.is_finished(), "clear must wait while publication owns its reservation");
        drop(leader);
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.active_session_count_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last-consumer cancellation must release the session immediately");
        tokio::time::timeout(Duration::from_secs(1), clear)
            .await
            .expect("clear must finish after publication cancellation")
            .expect("clear task must join");
        producer_task.await.expect("producer task must join");

        barrier.release.add_permits(1);
        clear_cold_fill_publication_barrier();
        drop(adapter.reserve_body(&plan).expect("publication reservation must be released"));
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial(cold_fill_publication_barrier)]
    async fn cold_fill_hard_deadline_releases_session_at_publication_barrier() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "deadline");
        let barrier = install_cold_fill_publication_barrier(&plan);
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("publication plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("publication request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            || async {
                Ok(GetObjectReader {
                    stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: Some(Bytes::from_static(b"body")),
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: Some(deadline),
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));

        let reached = barrier.reached.acquire().await.expect("publication barrier must remain open");
        reached.forget();
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(
            leader.wait().await,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        producer_task.await.expect("producer task must join");

        barrier.release.add_permits(1);
        clear_cold_fill_publication_barrier();
        drop(
            adapter
                .reserve_body(&plan)
                .expect("deadline must release the publication reservation"),
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            adapter.clear(rustfs_object_data_cache::ObjectDataCacheInvalidationReason::Manual),
        )
        .await
        .expect("clear must complete after publication deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_without_request_timeout_stops_at_ten_minute_hard_cap() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "hard-cap");
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("hard-cap plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("hard-cap request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            || async {
                Ok(GetObjectReader {
                    stream: Box::new(PendingReader),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        let wait = tokio::spawn(async move { leader.wait().await });

        tokio::time::advance(Duration::from_secs(599)).await;
        tokio::task::yield_now().await;
        assert!(!wait.is_finished(), "hard cap must not fire before 600 seconds");
        assert!(adapter.reserve_body(&plan).is_none(), "reservation must remain owned before the hard cap");

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            wait.await.expect("hard-cap waiter must join"),
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        producer_task.await.expect("producer task must join");
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        drop(
            adapter
                .reserve_body(&plan)
                .expect("hard cap must release the body reservation"),
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_same_key_cold_fill_consumes_one_reader() {
        const REQUESTS: usize = 2000;
        const BODY_BYTES: usize = 64 * 1024;
        const BODY_BYTES_U64: u64 = 64 * 1024;
        const BODY_BYTES_I64: i64 = 64 * 1024;

        for key_count in [1_usize, 4, 32] {
            let adapter = Arc::new(
                ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                    mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                    max_bytes: 128 * 1024 * 1024,
                    max_memory_percent: 0,
                    max_entry_bytes: 1024 * 1024,
                    min_free_memory_percent: 0,
                    fill_concurrency_per_cpu: 64,
                    fill_concurrency_max: 64,
                    ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
                })
                .expect("matrix cache config must be valid"),
            );
            let coordinator = adapter.cold_fill_coordinator();
            let disk_permits = Arc::new(tokio::sync::Semaphore::new(key_count));
            let writers = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(key_count)));
            let permit_acquires = Arc::new(AtomicUsize::new(0));
            let reader_factories = Arc::new(AtomicUsize::new(0));
            let first_polls = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let bytes_read = Arc::new(AtomicUsize::new(0));
            let mut tasks = tokio::task::JoinSet::new();

            for request in 0..REQUESTS {
                let key_index = request % key_count;
                let object = format!("matrix-object-{key_index}");
                let engine_plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
                    bucket: "matrix-bucket",
                    object: &object,
                    version_id: None,
                    etag: "matrix-etag",
                    size: BODY_BYTES_U64,
                    data_dir_u128: Some(u128::try_from(key_index).unwrap_or(u128::MAX) + 1),
                    mod_time_unix_nanos: 1,
                    body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
                });
                let cache_key = engine_plan.key().cloned().expect("matrix body must be cacheable");
                let adapter = Arc::clone(&adapter);
                let coordinator = Arc::clone(&coordinator);
                let disk_permits = Arc::clone(&disk_permits);
                let writers = Arc::clone(&writers);
                let permit_acquires = Arc::clone(&permit_acquires);
                let reader_factories = Arc::clone(&reader_factories);
                let first_polls = Arc::clone(&first_polls);
                let completed = Arc::clone(&completed);
                let bytes_read = Arc::clone(&bytes_read);
                tasks.spawn(async move {
                    let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
                        let reservation = adapter.reserve_body(&engine_plan);
                        let adapter = Arc::clone(&adapter);
                        let disk_permits = Arc::clone(&disk_permits);
                        let writers = Arc::clone(&writers);
                        let permit_acquires = Arc::clone(&permit_acquires);
                        let reader_factories = Arc::clone(&reader_factories);
                        let first_polls = Arc::clone(&first_polls);
                        let completed = Arc::clone(&completed);
                        let bytes_read = Arc::clone(&bytes_read);
                        let fill_plan = engine_plan.clone();
                        async move {
                            start_cold_fill_producer(
                                producer,
                                reservation,
                                || async move {
                                    permit_acquires.fetch_add(1, AtomicOrdering::Relaxed);
                                    let permit = disk_permits
                                        .acquire_owned()
                                        .await
                                        .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                                    let mut io =
                                        DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                                    io.disk_permit = Some(permit.into());
                                    Ok(io)
                                },
                                || async move {
                                    reader_factories.fetch_add(1, AtomicOrdering::Relaxed);
                                    let (writer, reader) = tokio::io::duplex(BODY_BYTES * 2);
                                    writers.lock().await.push(writer);
                                    Ok(GetObjectReader {
                                        stream: Box::new(ColdFillMatrixReader {
                                            inner: reader,
                                            first_poll_recorded: false,
                                            completion_recorded: false,
                                            first_polls,
                                            completed,
                                            bytes_read,
                                        }),
                                        object_info: ObjectInfo {
                                            size: BODY_BYTES_I64,
                                            actual_size: BODY_BYTES_I64,
                                            ..Default::default()
                                        },
                                        buffered_body: None,
                                        body_source: GetObjectBodySource::HookMissed,
                                    })
                                },
                                ColdFillProducerExecution {
                                    expected: BODY_BYTES,
                                    deadline: None,
                                    adapter,
                                    engine_plan: fill_plan,
                                },
                            )
                            .await
                        }
                    })
                    .await;
                    let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
                        panic!("matrix request must receive the shared body, got {outcome:?}");
                    };
                    assert_eq!(body.len(), BODY_BYTES);
                    assert!(body.iter().all(|byte| *byte == 7));
                    (key_index, body.as_ptr() as usize)
                });
            }

            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if writers.lock().await.len() == key_count
                        && coordinator.global_waiter_count_for_test() == REQUESTS - key_count
                        && first_polls.load(AtomicOrdering::Relaxed) == key_count
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all matrix followers must join before releasing bodies");

            let mut body_writers = std::mem::take(&mut *writers.lock().await);
            let body = vec![7_u8; BODY_BYTES];
            for writer in &mut body_writers {
                tokio::io::AsyncWriteExt::write_all(writer, &body)
                    .await
                    .expect("matrix body write must succeed");
                tokio::io::AsyncWriteExt::shutdown(writer)
                    .await
                    .expect("matrix body writer must close");
            }
            let mut backing_pointers = std::collections::HashMap::<usize, std::collections::HashSet<usize>>::new();
            tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = tasks.join_next().await {
                    let (key_index, body_pointer) = result.expect("matrix GET task must complete");
                    backing_pointers.entry(key_index).or_default().insert(body_pointer);
                }
            })
            .await
            .expect("matrix GET tasks must complete before the watchdog");

            assert_eq!(permit_acquires.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(reader_factories.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(first_polls.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(completed.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(bytes_read.load(AtomicOrdering::Relaxed), key_count * BODY_BYTES);
            assert_eq!(backing_pointers.len(), key_count);
            assert!(
                backing_pointers.values().all(|pointers| pointers.len() == 1),
                "all followers of one key must share one backing allocation"
            );
            assert_eq!(
                backing_pointers
                    .values()
                    .flatten()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                key_count
            );
            assert_eq!(coordinator.global_waiter_count_for_test(), 0);
            assert_eq!(coordinator.active_session_count_for_test(), 0);
            assert_eq!(disk_permits.available_permits(), key_count);

            for key_index in 0..key_count {
                let object = format!("matrix-object-{key_index}");
                let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
                    bucket: "matrix-bucket",
                    object: &object,
                    version_id: None,
                    etag: "matrix-etag",
                    size: BODY_BYTES_U64,
                    data_dir_u128: Some(u128::try_from(key_index).unwrap_or(u128::MAX) + 1),
                    mod_time_unix_nanos: 1,
                    body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
                });
                assert!(matches!(
                    adapter.lookup_body(&plan).await,
                    rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)
                ));
            }
        }
    }

    // #1324: the in-memory (buffered/cache) source is guarded by
    // MemoryTrackedBytesStream. A buffer whose length disagrees with the declared
    // content length must yield a stream error on first poll instead of a clean
    // short body or an over-long body. Reverting to the old warn-and-serve
    // behavior would make these assertions observe Ok chunks.
    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_fails_short_body() {
        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"test"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::disabled(),
        );
        let err = stream
            .next()
            .await
            .expect("mismatched memory body must yield an item")
            .expect_err("a short memory body must fail the stream instead of serving a truncated body");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(stream.next().await.is_none(), "stream must terminate after the error");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_fails_over_long_body() {
        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"hello!"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::disabled(),
        );
        let err = stream
            .next()
            .await
            .expect("mismatched memory body must yield an item")
            .expect_err("an over-long memory body must fail the stream instead of serving mismatched bytes");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_times_out_when_body_stalls() {
        let reader = GetObjectStreamingReader::new(
            PendingReader,
            "test-bucket",
            "stalled-object",
            1,
            Duration::from_millis(1),
            GetObjectBodyLifecycle::disabled(),
        );
        let mut stream = ReaderStream::with_capacity(reader, 1024);

        let err = stream
            .next()
            .await
            .expect("reader stream should yield timeout")
            .expect_err("stalled reader should return an error");

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
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

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_holds_request_guard_until_eof() {
        use tokio::io::AsyncReadExt;

        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"hello".to_vec()),
            "test-bucket",
            "complete-object",
            5,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
        );
        let mut out = Vec::new();

        reader
            .read_to_end(&mut out)
            .await
            .expect("complete streaming body should read successfully");

        assert_eq!(out, b"hello");
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_errors_on_short_eof() {
        use tokio::io::AsyncReadExt;

        // The inner reader delivers 5 bytes then a clean EOF, but the advertised
        // Content-Length is 10. The reader must surface an error rather than a clean EOF, so
        // the client sees a failed transfer instead of silently persisting a truncated body
        // (the "incomplete data mirroring" of #2955).
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "test-bucket",
            "truncated-object",
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("short body under a larger Content-Length must fail the stream");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(out, b"short", "bytes read before the short EOF are still delivered");

        drop(reader);
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[test]
    #[serial_test::serial]
    fn get_object_streaming_reader_releases_request_guard_when_dropped_incomplete() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "test-bucket",
            "dropped-object",
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
        );
        drop(reader);

        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_releases_request_guard_after_emit() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"hello"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::tracked(guard),
        );
        let chunk = stream
            .next()
            .await
            .expect("memory body should emit one chunk")
            .expect("memory body chunk should be readable");

        assert_eq!(chunk.as_ref(), b"hello");
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[test]
    #[serial_test::serial]
    fn memory_tracked_bytes_stream_releases_request_guard_for_zero_length_without_poll() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let stream = MemoryTrackedBytesStream::new(
            Bytes::new(),
            0,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::tracked(guard),
        );
        drop(stream);

        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[tokio::test]
    async fn disk_read_permit_reader_holds_permit_until_reader_is_dropped() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("test semaphore should grant owned permit");

        let reader = DiskReadPermitReader::new(std::io::Cursor::new(Vec::<u8>::new()), permit.into());
        assert_eq!(semaphore.available_permits(), 0);

        drop(reader);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    #[serial_test::serial(cold_fill_metrics_gate)]
    async fn cold_fill_follower_disk_permit_metric_tracks_actual_permit_lifetime() {
        COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.store(0, Ordering::Relaxed);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("follower test semaphore must grant an owned permit");
            let tracked = GetObjectDiskPermit::new(permit);
            assert_eq!(semaphore.available_permits(), 0);
            assert_eq!(COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.load(Ordering::Relaxed), 1);

            drop(tracked);
            assert_eq!(semaphore.available_permits(), 1);
            assert_eq!(COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.load(Ordering::Relaxed), 0);
        })
        .await;
    }

    #[test]
    #[serial_test::serial(cold_fill_metrics_gate)]
    fn cold_fill_disk_permit_metrics_obey_gate_and_return_to_zero() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let metrics_was_enabled = rustfs_io_metrics::metrics_enabled();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("metric test runtime must build");
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                rustfs_io_metrics::set_metrics_enabled(false);
                let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    rustfs_io_metrics::set_metrics_enabled(true);
                    drop(tracked);
                })
                .await;
                assert!(
                    snapshotter.snapshot().into_vec().into_iter().all(|(composite, _, _, _)| {
                        !composite.key().name().starts_with("rustfs_object_data_cache_cold_fill_")
                    }),
                    "a permit acquired while metrics were disabled must not record an unmatched decrement"
                );

                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Producer, async {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    rustfs_io_metrics::set_metrics_enabled(false);
                    drop(tracked);
                })
                .await;
                rustfs_io_metrics::set_metrics_enabled(true);
                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
                    let permit = semaphore.acquire_owned().await.expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    let _replacement = crate::app::object_data_cache::ColdFillCoordinator::default();
                    drop(tracked);
                })
                .await;
            });
        });

        let values = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                composite
                    .key()
                    .name()
                    .starts_with("rustfs_object_data_cache_cold_fill_")
                    .then_some((composite.key().name().to_string(), value))
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(values.len(), 2);
        for name in [
            "rustfs_object_data_cache_cold_fill_producer_disk_permits",
            "rustfs_object_data_cache_cold_fill_follower_disk_permits",
        ] {
            let DebugValue::Gauge(value) = values.get(name).unwrap_or_else(|| panic!("missing {name} gauge")) else {
                panic!("{name} must be a gauge");
            };
            assert_eq!(value.into_inner(), 0.0, "{name} must return to zero after permit drop");
        }
        rustfs_io_metrics::set_metrics_enabled(metrics_was_enabled);
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_large_objects_on_streaming_path_without_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 18_i64 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            18_i64 * 1024 * 1024 * 1024,
            128 * 1024,
            true,
            1,
            None,
            false,
            false,
            None,
            "test-bucket",
            "large-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("build_get_object_body should succeed for streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "large-object response construction should not pre-read object data"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_large_encrypted_objects_on_streaming_path_without_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 18_i64 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            18_i64 * 1024 * 1024 * 1024,
            128 * 1024,
            true,
            1,
            None,
            false,
            true,
            None,
            "test-bucket",
            "large-encrypted-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("build_get_object_body should succeed for encrypted streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "large encrypted object response construction should not pre-read object data"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_uses_buffered_body_without_reader_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 4,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            4,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"test")),
            "test-bucket",
            "direct-memory-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("build_get_object_body should consume buffered body");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered GetObject body must not be read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_uses_cached_body_without_reader_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.cache().fill_body(&plan, Bytes::from_static(b"hello")).await;

        assert_eq!(fill, rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted);

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("cache hit body handoff should succeed");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_rejects_size_mismatch_fill() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.cache().fill_body(&plan, Bytes::from_static(b"oops")).await;

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("size-mismatched direct fill should not create a cache hit");
        let lookup_after_mismatch = adapter.lookup_body(&plan).await;

        assert_eq!(fill, rustfs_object_data_cache::ObjectDataCacheFillResult::SkippedSizeMismatch);
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "size-mismatched rejected fill should construct the fallback stream without pre-reading"
        );
        assert!(
            matches!(lookup_after_mismatch, rustfs_object_data_cache::ObjectDataCacheLookup::Miss),
            "size-mismatched fill must not leave a reusable cache entry"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_fills_from_buffered_body_without_reader_preread() {
        let first_reads = Arc::new(AtomicUsize::new(0));
        let first_reader = ReadProbeReader {
            reads: Arc::clone(&first_reads),
        };
        let second_reads = Arc::new(AtomicUsize::new(0));
        let second_reader = ReadProbeReader {
            reads: Arc::clone(&second_reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");

        let _first_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            first_reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("buffered-body handoff should succeed");

        // ODC-15: the fill is detached from the response path, so wait for it to
        // populate the cache before the follow-up GET to keep the hit deterministic.
        wait_for_cache_hit(&adapter, "test-bucket", "cached-object", "etag", 5).await;

        let _second_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            second_reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("follow-up cache hit should succeed");

        assert_eq!(
            first_reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body fill path must not read from the fallback reader"
        );
        assert_eq!(
            second_reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit after buffered-body fill must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_skips_buffered_fill_on_size_mismatch() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"oops")),
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("size-mismatched buffered-body handoff should still return a response body");
        let lookup = adapter.lookup_body(&plan).await;

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body handoff must not read from the fallback reader"
        );
        assert!(
            matches!(lookup, rustfs_object_data_cache::ObjectDataCacheLookup::Miss),
            "size-mismatched buffered body must not be filled into cache"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_hook_served_records_no_second_lookup() {
        // ODC-16 (backlog#1121): a hook-served GET must record exactly one
        // lookup — the ecstore hook's. The app layer, handed the cache body as
        // buffered_body with cache_hook_served=true, must serve it directly
        // without a second lookup (which would double the hits and hit_bytes).
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "hook-served",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let hit_body = Bytes::from_static(b"hello");
        assert_eq!(
            adapter.cache().fill_body(&plan, hit_body.clone()).await,
            rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted
        );

        // Simulate the ecstore hook: it performs exactly one lookup after fresh
        // metadata resolution, hits, and hands the body forward as buffered_body.
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)
        ));
        let lookups_after_hook = adapter.cache().stats().lookups;
        assert_eq!(lookups_after_hook, 1, "the hook performs exactly one lookup");

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(hit_body),
            /* cache_hook_served */ true,
            /* cache_hook_probed */ true,
            /* cache_fill_allowed */ true,
            "test-bucket",
            "hook-served",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("hook-served body handoff should succeed");

        assert_eq!(
            adapter.cache().stats().lookups,
            lookups_after_hook,
            "a hook-served GET must not record a second lookup in the app layer"
        );
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "hook-served body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_hook_miss_skips_app_lookup() {
        // ODC-16: when the hook probed and missed, its miss is authoritative
        // (it ran after fresh metadata resolution), so the app layer must not
        // run a second lookup — it only fills from the buffered body.
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");

        let lookups_before = adapter.cache().stats().lookups;
        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            /* cache_hook_served */ false,
            /* cache_hook_probed */ true,
            /* cache_fill_allowed */ true,
            "test-bucket",
            "hook-missed",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("hook-miss buffered-body handoff should succeed");

        assert_eq!(
            adapter.cache().stats().lookups,
            lookups_before,
            "a hook-probed miss must not trigger an app-layer lookup"
        );
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_materializes_once_and_hits_later() {
        let first_reads = Arc::new(AtomicUsize::new(0));
        let first_reader = DataProbeReader {
            reads: Arc::clone(&first_reads),
            data: std::io::Cursor::new(b"hello".to_vec()),
        };
        let second_reads = Arc::new(AtomicUsize::new(0));
        let second_reader = ReadProbeReader {
            reads: Arc::clone(&second_reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let _first_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            first_reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "materialized-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("materialize-fill handoff should succeed");

        // ODC-15: the fill is detached from the response path, so wait for it to
        // populate the cache before the follow-up GET to keep the hit deterministic.
        wait_for_cache_hit(&adapter, "test-bucket", "materialized-object", "etag", 5).await;

        let _second_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            second_reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "materialized-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("follow-up cache hit should succeed");

        assert_eq!(
            first_reads.load(AtomicOrdering::Relaxed),
            2,
            "materialize-fill path should read the source stream once to data and once for EOF"
        );
        assert_eq!(
            second_reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit after materialize-fill must not read from the fallback reader"
        );
    }

    // ODC-07: a materialize read that yields more than the declared content
    // length must be a hard error, not a warn-and-serve, matching the
    // direct-memory GET path. The bounded `take` reads one byte past capacity so
    // the over-long stream is detected without buffering it unbounded.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_length_mismatch() {
        let reads = Arc::new(AtomicUsize::new(0));
        // Declared content length is 5, but the stream yields 6 bytes.
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hello!".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "mismatch-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await;

        assert!(
            result.is_err(),
            "an over-long materialize read must be a hard error, not a truncated served body"
        );
    }

    // #1324: a materialize-fill read that ends short of the declared content
    // length (clean EOF at N-1 for a declared N) must hard-fail, matching the
    // over-long case above. Reverting to warn-and-serve would return Ok with a
    // truncated body.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_short_read() {
        let reads = Arc::new(AtomicUsize::new(0));
        // Declared content length is 5, but the stream only yields 4 bytes.
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hell".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "short-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await;

        assert!(
            result.is_err(),
            "a short materialize read must be a hard error, not a truncated served body"
        );
    }

    // #1324: a materialize-fill read that fails after draining K bytes must
    // propagate the read error and must NOT fall back to streaming the same
    // (partially consumed) reader, which would ship a prefix-misaligned body.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_partial_read_error() {
        let reader = ErrAfterReader {
            data: std::io::Cursor::new(b"hello".to_vec()),
            fail_after: 3,
            emitted: 0,
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "partial-read-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await;

        assert!(
            result.is_err(),
            "a partial-read error during materialization must fail the request, not stream a prefix-misaligned body"
        );
    }

    // #1324: the buffered-body (direct-memory / cache-served) source must also
    // enforce the exact-length contract. A buffered body shorter than the
    // declared content length is a hard error before headers.
    #[tokio::test]
    async fn build_get_object_body_rejects_short_buffered_body() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            ..Default::default()
        };

        let result = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            // Declared length 5 but only 4 buffered bytes.
            Some(Bytes::from_static(b"hell")),
            "test-bucket",
            "short-buffered-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await;

        assert!(result.is_err(), "a buffered body shorter than the declared content length must hard-fail");
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "the mismatch must be caught without touching the fallback reader"
        );
    }

    // #1324 compatibility boundary: a legacy/backfilled object whose decoded
    // bytes exactly equal its declared content length must still serve cleanly.
    // The strict contract keys off actual-vs-declared equality only, so it never
    // flips a legitimate exact-length object into a hard failure — it only
    // rejects genuine short/over-long/errored reads.
    #[tokio::test]
    async fn build_get_object_body_serves_exact_length_buffered_body() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            "test-bucket",
            "exact-buffered-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("an exact-length buffered body must serve without error");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "an exact-length buffered body must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_skips_materialize_when_too_large_for_cache() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hello".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                max_entry_bytes: 4,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "too-large-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("too-large cache candidate should use streaming fallback");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "too-large materialize-fill candidate must not pre-read the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_small_plain_objects_on_streaming_path_by_default() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 4,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            4,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            "test-bucket",
            "small-plain-object",
            GetObjectBodyLifecycle::disabled(),
        )
        .await
        .expect("build_get_object_body should keep small plain object on streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "default GetObject response construction should not pre-read small plain object data"
        );
    }

    #[test]
    fn select_stream_buffer_strategy_expands_large_sequential_gets() {
        let (buffer_size, strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(2_i64 * 1024 * 1024 * 1024, 2 * MI_B, true, false);

        assert_eq!(strategy, GetObjectStreamStrategy::LargeSequentialReadahead);
        assert_eq!(buffer_size, 4 * MI_B);
    }

    #[test]
    fn select_stream_buffer_strategy_keeps_ranges_and_small_gets_standard() {
        let (range_buffer_size, range_strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(2_i64 * 1024 * 1024 * 1024, 2 * MI_B, true, true);
        assert_eq!(range_strategy, GetObjectStreamStrategy::Standard);
        assert_eq!(range_buffer_size, 2 * MI_B);

        let (small_buffer_size, small_strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(64 * 1024 * 1024, 512 * 1024, true, false);
        assert_eq!(small_strategy, GetObjectStreamStrategy::Standard);
        assert_eq!(small_buffer_size, 512 * 1024);
    }

    #[test]
    fn tune_reader_stream_buffer_size_raises_large_standard_streams_only() {
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(512 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(2 * MI_B, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            2 * MI_B
        );
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, MI_B as i64, GetObjectStreamStrategy::Standard),
            128 * 1024
        );
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::LargeSequentialReadahead),
            128 * 1024
        );
    }

    #[test]
    fn resolve_reader_stream_buffer_size_keeps_selected_default() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, None);

        assert_eq!(buffer_size, 128 * 1024);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_SELECTED);
    }

    #[test]
    fn resolve_reader_stream_buffer_size_applies_positive_override() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, Some(MI_B));

        assert_eq!(buffer_size, MI_B);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE);
    }

    #[test]
    fn resolve_reader_stream_buffer_size_ignores_zero_override() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, Some(0));

        assert_eq!(buffer_size, 128 * 1024);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_SELECTED);
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
    async fn read_small_put_body_exact_pooled_reads_exact_bytes() {
        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hello".to_vec());

        let buffer = read_small_put_body_exact_pooled(body, 5, pool.as_ref())
            .await
            .expect("pooled exact read should succeed");

        assert_eq!(&buffer[..5], b"hello");
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
    async fn get_object_reader_stream_tracks_remaining_length() {
        let mut stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"hello".to_vec()),
            2,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        assert_eq!(stream.remaining_length().exact(), Some(5));

        let first = stream
            .next()
            .await
            .expect("reader stream should emit first chunk")
            .expect("first chunk should read");

        assert_eq!(first.as_ref(), b"he");
        assert_eq!(stream.remaining_length().exact(), Some(3));
    }

    #[tokio::test]
    async fn get_object_reader_stream_truncates_to_expected_length() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"hello!".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let chunks = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("reader stream should read");
        let body = chunks.into_iter().fold(Vec::new(), |mut acc, chunk| {
            acc.extend_from_slice(&chunk);
            acc
        });

        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn disk_read_permit_reader_releases_permit_at_eof() {
        use tokio::io::AsyncReadExt;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.expect("acquire permit");
        assert_eq!(semaphore.available_permits(), 0);

        let mut reader = DiskReadPermitReader::new(std::io::Cursor::new(b"hello".to_vec()), permit.into());
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.expect("read body");
        assert_eq!(body, b"hello");

        // The reader is still alive (client hasn't dropped the body), but EOF
        // was observed, so the permit must already be back in the semaphore.
        assert_eq!(semaphore.available_permits(), 1);
        drop(reader);
        assert_eq!(semaphore.available_permits(), 1);
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

    #[test]
    fn resolve_put_object_extract_options_defaults_when_headers_missing() {
        let headers = HeaderMap::new();
        let options = resolve_put_object_extract_options(&headers).unwrap();
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

        let options = resolve_put_object_extract_options(&headers).unwrap();
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

        let options = resolve_put_object_extract_options(&headers).unwrap();
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

        let options = resolve_put_object_extract_options(&headers).unwrap();
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

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(options.prefix.as_deref(), Some("minio/prefix"));
    }

    #[test]
    fn resolve_put_object_extract_options_exact_flags_override_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-dirs", HeaderValue::from_static("true"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-errors", HeaderValue::from_static("true"));

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert!(!options.ignore_dirs);
        assert!(!options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_rejects_unsafe_prefix_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("../victim-bucket"));

        assert!(resolve_put_object_extract_options(&headers).is_err());
    }

    #[test]
    fn validate_put_object_extract_entry_count_rejects_limit_overflow() {
        let limits = ArchiveLimits {
            max_entries: 1,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_count(2, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_entry_size_rejects_oversized_entry() {
        let limits = ArchiveLimits {
            max_entry_size: 8,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_size("payload.bin", 9, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_total_size_rejects_cumulative_overflow() {
        let limits = ArchiveLimits {
            max_total_unpacked_size: 16,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_total_size(17, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_entry_path_rejects_overlong_path() {
        let limits = ArchiveLimits {
            max_path_length: 8,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_path("toolong-path", limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn put_object_extract_quota_exceeded_matches_existing_error_shape() {
        let err = put_object_extract_quota_exceeded(10, 8);
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("Bucket quota exceeded. Current usage: 10 bytes, limit: 8 bytes"));
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

    #[tokio::test]
    async fn build_get_object_output_context_returns_content_disposition() {
        let mut metadata = HashMap::new();
        metadata.insert("content-disposition".to_string(), "attachment; filename=\"demo.png\"".to_string());

        let info = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "path/raw".to_string(),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("path/raw".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();
        let queue_status = concurrency::IoQueueStatus::default();

        let context = usecase
            .build_get_object_output_context(
                &req,
                get_concurrency_manager(),
                "test-bucket",
                "path/raw",
                info.clone(),
                Some(info),
                wrap_reader(tokio::io::empty()),
                None,
                false,
                false,
                true,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                false,
                Duration::ZERO,
                0.0,
                &queue_status,
                1,
                None,
                false,
                GetObjectBodyLifecycle::disabled(),
            )
            .await
            .expect("get object output context");

        assert_eq!(context.output.content_disposition.as_deref(), Some("attachment; filename=\"demo.png\""));
        assert!(
            !context
                .output
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.contains_key("content-disposition"))
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

    #[test]
    fn parse_get_object_part_number_rejects_above_s3_max() {
        let err = parse_part_number_i32_to_usize(Some(10001), "GET").expect_err("partNumber above S3 max must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("GET: partNumber must be between 1 and 10000"));
    }

    #[test]
    fn validate_get_object_part_number_rejects_missing_part() {
        let info = ObjectInfo {
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let err =
            DefaultObjectUsecase::validate_get_object_part_number(Some(2), &info).expect_err("missing requested part must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
        assert!(DefaultObjectUsecase::validate_get_object_part_number(Some(1), &info).is_ok());
    }

    #[test]
    fn cold_fill_conditions_fail_before_phase_probe_advances() {
        fn run_phase_probe(headers: &HeaderMap, info: &ObjectInfo) -> (S3Result<()>, [usize; 3]) {
            let coordination = AtomicUsize::new(0);
            let permit = AtomicUsize::new(0);
            let reader = AtomicUsize::new(0);
            let result = DefaultObjectUsecase::validate_get_object_before_cold_fill(headers, None, info);
            if result.is_ok() {
                coordination.fetch_add(1, AtomicOrdering::Relaxed);
                permit.fetch_add(1, AtomicOrdering::Relaxed);
                reader.fetch_add(1, AtomicOrdering::Relaxed);
            }
            (
                result,
                [
                    coordination.load(AtomicOrdering::Relaxed),
                    permit.load(AtomicOrdering::Relaxed),
                    reader.load(AtomicOrdering::Relaxed),
                ],
            )
        }

        let info = ObjectInfo {
            etag: Some("phase-etag".to_string()),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let mut not_modified = HeaderMap::new();
        not_modified.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("\"phase-etag\""));
        let (result, phases) = run_phase_probe(&not_modified, &info);
        assert_eq!(result.expect_err("matching If-None-Match must reject").code(), &S3ErrorCode::NotModified);
        assert_eq!(phases, [0, 0, 0]);

        let mut precondition_failed = HeaderMap::new();
        precondition_failed.insert(http::header::IF_MATCH, HeaderValue::from_static("\"other-etag\""));
        let (result, phases) = run_phase_probe(&precondition_failed, &info);
        assert_eq!(
            result.expect_err("mismatched If-Match must reject").code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert_eq!(phases, [0, 0, 0]);
    }

    #[tokio::test]
    async fn execute_get_object_rejects_range_with_part_number() {
        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(1))
            .range(Some(Range::Int { first: 0, last: Some(1) }))
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
    }

    #[test]
    fn normalize_delete_objects_version_id_preserves_explicit_null_marker() {
        let (wire_version_id, internal_version_id) =
            normalize_delete_objects_version_id(Some("null".to_string())).expect("null version marker should parse");

        assert_eq!(wire_version_id.as_deref(), Some("null"));
        assert_eq!(internal_version_id, Some(Uuid::nil()));
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
        assert!(can_skip_delete_objects_pre_stat(false, false, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_object_lock_buckets() {
        assert!(!can_skip_delete_objects_pre_stat(true, false, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_when_replication_rules_match() {
        assert!(!can_skip_delete_objects_pre_stat(false, true, &delete_marker_creating_opts(), true));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_explicit_version_deletes() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            versioned: true,
            version_suspended: false,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, false, &opts, true));
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
        assert!(!can_skip_delete_objects_pre_stat(false, false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_for_suspended_versioning() {
        let opts = ObjectOptions {
            version_id: None,
            versioned: true,
            version_suspended: true,
            ..Default::default()
        };
        assert!(!can_skip_delete_objects_pre_stat(false, false, &opts, false));
    }

    #[test]
    fn delete_objects_pre_stat_kept_when_accounting_snapshot_disagrees() {
        // If the accounting-side versioning snapshot does not also classify the
        // delete as a delete-marker creation, the stat must stay so usage
        // accounting keeps its size input.
        assert!(!can_skip_delete_objects_pre_stat(false, false, &delete_marker_creating_opts(), false));
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
    #[ignore = "requires isolated global object layer state"]
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
    #[ignore = "requires isolated global object layer state"]
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

    fn quota_result(allowed: bool) -> QuotaCheckResult {
        QuotaCheckResult {
            allowed,
            current_usage: Some(1024),
            quota_limit: Some(2048),
            operation_size: 512,
            remaining: Some(512),
        }
    }

    #[test]
    fn quota_admission_allows_within_limit() {
        map_quota_check_outcome("bucket", Ok(quota_result(true))).expect("an allowed result admits the write");
    }

    #[test]
    fn quota_admission_rejects_over_limit() {
        let err = map_quota_check_outcome("bucket", Ok(quota_result(false))).expect_err("an over-limit result rejects the write");
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
}
