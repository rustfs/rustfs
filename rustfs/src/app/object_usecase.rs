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
        objectlock_sys::{check_object_lock_for_deletion, is_retention_active},
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
    DecryptionRequest, EncryptionRequest, SSEType, SseKmsPrincipal, apply_bucket_default_lock_retention,
    authorize_sse_kms_object_read, bucket_default_write_sse, build_ssec_read_headers, encryption_material_to_metadata,
    extract_server_side_encryption_from_headers, extract_ssec_params_from_headers, extract_ssekms_context_from_headers,
    get_buffer_size_opt_in, load_bucket_object_lock_config_state, map_get_object_reader_error, sse_decryption, sse_encryption,
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

const DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES: i64 = 32 * 1024 * 1024;
const RUSTFS_EXPECTED_CURRENT_VERSION_ID: &str = "x-rustfs-expected-current-version-id";
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
pub(super) fn map_quota_check_outcome(bucket: &str, outcome: Result<QuotaCheckResult, QuotaError>) -> S3Result<QuotaCheckResult> {
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

pub(super) fn apply_quota_admission(opts: &mut ObjectOptions, result: &QuotaCheckResult) -> S3Result<()> {
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

fn ensure_object_size_within_quota(result: &QuotaCheckResult, new_size: u64) -> S3Result<()> {
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

fn ensure_legacy_archive_size_within_quota(result: &QuotaCheckResult, total_unpacked_size: u64) -> S3Result<()> {
    if result.uses_durable_reservations {
        return Ok(());
    }
    let (Some(current_usage), Some(quota_limit)) = (result.current_usage, result.quota_limit) else {
        return Ok(());
    };
    let expected_usage = current_usage
        .checked_add(total_unpacked_size)
        .ok_or_else(|| s3_error!(InvalidArgument, "Archive total size overflowed quota accounting"))?;
    if expected_usage > quota_limit {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("Bucket quota exceeded. Current usage: {current_usage} bytes, limit: {quota_limit} bytes"),
        ));
    }
    Ok(())
}

fn quota_accounting_object_size(info: &ObjectInfo, fail_closed: bool) -> S3Result<u64> {
    match quota_object_size(info) {
        Ok(size) => Ok(size),
        Err(err) if fail_closed => Err(ApiError::from(err).into()),
        Err(_) => Ok(info.size.max(0) as u64),
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

/// Request fields that passed the cheap GET validations, ready for the
/// bucket-metadata work in [`DefaultObjectUsecase::prepare_get_object_request_context`].
struct GetObjectValidatedRequest {
    bucket: String,
    key: String,
    version_id: Option<String>,
    part_number: Option<usize>,
    rs: Option<HTTPRangeSpec>,
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
    /// Resolved plaintext start offset of the committed response body
    /// (`get_offset_length` output; 0 for a full-object read). Feeds the
    /// mid-stream resume offset.
    resume_range_start: i64,
    /// Resolved inclusive plaintext end offset of the committed response body;
    /// -1 when the committed body runs to the end of the object.
    resume_range_end: i64,
}

struct GetObjectPreparedRead {
    io_planning: GetObjectIoPlanning,
    read_setup: GetObjectReadSetup,
}

struct GetObjectStrategyContext {
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
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
const MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES: usize = 512 * 1024;
const MID_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES: i64 = MI_B as i64;
const ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE: &str = "RUSTFS_GET_SEEK_BUFFER_ENABLE";
const ENV_RUSTFS_GET_READER_STREAM_BUFFER_SIZE: &str = "RUSTFS_GET_READER_STREAM_BUFFER_SIZE";
const ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE: &str = "RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE";
const ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE: &str = "RUSTFS_GET_SMALL_BODY_ONCE_ENABLE";
const GET_READER_STREAM_BUFFER_SOURCE_SELECTED: &str = "selected";
const GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE: &str = "env_override";
const GET_READER_STREAM_POLL_PENDING: &str = "pending";
const GET_READER_STREAM_POLL_READY_DATA: &str = "ready_data";
const GET_READER_STREAM_POLL_READY_EMPTY: &str = "ready_empty";
const GET_READER_STREAM_POLL_READY_ERROR: &str = "ready_error";
const GET_STREAMING_BODY_FAILURE_STAGE_READER_STREAM: &str = "reader_stream";
const GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR: &str = "reader_error";
const GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF: &str = "short_eof";
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

fn is_get_small_body_once_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, false)
    }
    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, false))
    }
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

    if stream_strategy == GetObjectStreamStrategy::Standard
        && response_content_length >= MID_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES
    {
        return selected_size.max(MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES);
    }

    selected_size
}

fn get_object_stream_size_bucket(expected: usize) -> &'static str {
    rustfs_io_metrics::get_object_size_bucket(i64::try_from(expected).unwrap_or(i64::MAX))
}

fn classify_get_object_stream_read_error(err: &std::io::Error) -> &'static str {
    if let Some(inner) = err.get_ref() {
        if inner.is::<rustfs_rio::IncompleteBody>() {
            return "short_eof";
        }

        if inner.is::<rustfs_rio::ChecksumMismatch>() {
            return "bitrot";
        }

        let error_msg = inner.to_string().to_lowercase();
        if error_msg.contains("bitrot") {
            return "bitrot";
        }
        if error_msg.contains("read quorum") || error_msg.contains("insufficient read quorum") || error_msg.contains("erasure") {
            return "read_quorum";
        }
    }

    match err.kind() {
        std::io::ErrorKind::UnexpectedEof => "short_eof",
        std::io::ErrorKind::TimedOut => "timeout",
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => "range_or_length_invalid",
        _ => "io",
    }
}

fn get_object_stream_failure_reason(error_class: &'static str) -> &'static str {
    if error_class == "short_eof" {
        GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF
    } else {
        GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR
    }
}

fn record_get_object_reader_stream_failure(
    reason: &'static str,
    error_class: &'static str,
    strategy: &'static str,
    buffer_source: &'static str,
    expected: usize,
    emitted: usize,
    remaining: usize,
) {
    rustfs_io_metrics::record_get_object_streaming_body_failure(rustfs_io_metrics::GetObjectStreamingBodyFailure {
        stage: GET_STREAMING_BODY_FAILURE_STAGE_READER_STREAM,
        reason,
        error_class,
        strategy,
        buffer_source,
        size_bucket: get_object_stream_size_bucket(expected),
        emitted_bytes: emitted,
        remaining_bytes: remaining,
    });
}

pin_project! {
    struct ExtractArchiveEtagReader<R> {
        #[pin]
        inner: R,
        md5: Md5,
        finished: bool,
        etag: Arc<Mutex<Option<String>>>,
    }
}

struct MemoryTrackedBytesStream {
    bytes: Option<Bytes>,
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

struct MemoryOnceBodyOwner {
    bytes: Bytes,
    _guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
    // Body::Once has no poll hook, so this opt-in path only holds the request
    // guard until the bytes are dropped; the result status remains unknown.
    _lifecycle: GetObjectBodyLifecycle,
}

impl MemoryOnceBodyOwner {
    fn new(bytes: Bytes, guard: Option<rustfs_io_metrics::MemoryGaugeGuard>, lifecycle: GetObjectBodyLifecycle) -> Self {
        Self {
            bytes,
            _guard: guard,
            _lifecycle: lifecycle,
        }
    }
}

impl AsRef<[u8]> for MemoryOnceBodyOwner {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
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
        reader: Option<R>,
        capacity: usize,
        strategy: &'static str,
        buffer_source: &'static str,
        remaining: usize,
        emitted: usize,
        expected: usize,
        // Diagnostic-only identity for the body this stream is serving. Unset in
        // unit tests that drive the stream over a bare reader; every production
        // body carries it via `with_diagnostics`.
        diagnostics: GetObjectReaderStreamDiagnostics,
    }
}

/// Object identity carried alongside a streaming GET body purely so a
/// mid-stream failure names the object it happened on.
#[derive(Clone, Default)]
struct GetObjectReaderStreamDiagnostics {
    bucket: String,
    object: String,
    request_id: String,
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
            bytes: Some(bytes),
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
            reader: Some(reader),
            capacity,
            strategy,
            buffer_source,
            remaining,
            emitted: 0,
            expected: remaining,
            diagnostics: GetObjectReaderStreamDiagnostics::default(),
        }
    }

    /// Attach the object identity a failed body should be reported against.
    fn with_diagnostics(mut self, bucket: &str, object: &str, request_id: &str) -> Self {
        self.diagnostics = GetObjectReaderStreamDiagnostics {
            bucket: bucket.to_string(),
            object: object.to_string(),
            request_id: request_id.to_string(),
        };
        self
    }
}

impl futures::Stream for MemoryTrackedBytesStream {
    type Item = Result<Bytes, S3StdError>;

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
            let actual = this.bytes.as_ref().map_or(0, Bytes::len);
            this.emitted = true;
            this.finish_err();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("materialized GET body length mismatch: expected {}, got {}", this.expected, actual),
            )
            .into())));
        }

        let Some(bytes) = this.bytes.take() else {
            return Poll::Ready(None);
        };
        let bytes_len = bytes.len();
        let first_byte_elapsed = (!bytes.is_empty()).then(|| this.started.elapsed());
        this.emitted = true;
        if let Some(elapsed) = first_byte_elapsed {
            rustfs_io_metrics::record_get_object_first_byte_latency(GET_OBJECT_STAGE_PATH_S3_HANDLER, elapsed.as_secs_f64());
        }
        if bytes_len >= this.expected {
            this.finish_ok();
        }
        if let Some(poll_start) = poll_start {
            rustfs_io_metrics::record_get_object_memory_body_stream_poll(
                this.source,
                GET_READER_STREAM_POLL_READY_DATA,
                bytes_len,
                poll_start.elapsed().as_secs_f64(),
            );
        }
        Poll::Ready(Some(Ok(bytes)))
    }
}

impl ByteStream for MemoryTrackedBytesStream {
    fn remaining_length(&self) -> RemainingLength {
        if self.emitted || self.bytes.is_none() {
            RemainingLength::new_exact(0)
        } else {
            RemainingLength::new_exact(self.expected)
        }
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
        let attribution_enabled = is_get_output_handoff_attribution_enabled();
        let poll_start = attribution_enabled.then(std::time::Instant::now);
        let reader = match this.reader.as_mut().as_pin_mut() {
            Some(reader) => reader,
            None => return Poll::Ready(None),
        };
        let read_capacity = (*this.capacity).min(*this.remaining);
        let mut buf = BytesMut::with_capacity(read_capacity);
        let poll_read = poll_read_buf(reader, cx, &mut buf);

        let result: Poll<Option<Self::Item>> = match poll_read {
            Poll::Ready(Ok(bytes_read)) if bytes_read > 0 => {
                let bytes = buf.freeze();
                *this.remaining -= bytes.len();
                *this.emitted += bytes.len();
                #[cfg(feature = "tracing-chunk-debug")]
                {
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
            Poll::Ready(Ok(_)) => {
                this.reader.set(None);
                let remaining = i64::try_from(*this.remaining).unwrap_or(i64::MAX);
                let err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining });
                record_get_object_reader_stream_failure(
                    GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF,
                    "short_eof",
                    this.strategy,
                    this.buffer_source,
                    *this.expected,
                    *this.emitted,
                    *this.remaining,
                );
                // The inner GetObjectStreamingReader is what normally reports a
                // short body, so reaching this arm means the reader signalled a
                // clean EOF while this layer still owed bytes against an
                // already-committed Content-Length. That disagreement is a data
                // plane fault, not chunk noise: log it unconditionally so the
                // truncated object is named in the operator's log rather than
                // only in a metric counter (issue #4784).
                error!(
                    event = EVENT_GET_OBJECT_STREAM_BODY,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    bucket = %this.diagnostics.bucket,
                    object = %this.diagnostics.object,
                    request_id = %this.diagnostics.request_id,
                    size_bucket = get_object_stream_size_bucket(*this.expected),
                    expected = *this.expected,
                    emitted = *this.emitted,
                    remaining = *this.remaining,
                    strategy = this.strategy,
                    buffer_source = this.buffer_source,
                    state = "reader_stream_short_eof",
                    error = %err,
                    "GetObject reader stream ended before the committed content length"
                );
                Poll::Ready(Some(Err(Box::new(err) as S3StdError)))
            }
            Poll::Ready(Err(err)) => {
                this.reader.set(None);
                let error_class = classify_get_object_stream_read_error(&err);
                record_get_object_reader_stream_failure(
                    get_object_stream_failure_reason(error_class),
                    error_class,
                    this.strategy,
                    this.buffer_source,
                    *this.expected,
                    *this.emitted,
                    *this.remaining,
                );
                // Deliberately not logged at warn here: every production body
                // wraps a GetObjectStreamingReader, and that layer already
                // reports this same error once with `state = "read_failed"` and
                // the object identity. A second unconditional line per failed
                // GET would read as two distinct faults. The chunk-debug build
                // still gets this layer's view of the same error.
                #[cfg(feature = "tracing-chunk-debug")]
                tracing::error!(
                    emitted = *this.emitted,
                    expected = *this.expected,
                    error_class = error_class,
                    error = %err,
                    "GetObject ReaderStream returned error"
                );
                Poll::Ready(Some(Err(Box::new(err) as S3StdError)))
            }
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
        if attribution_enabled {
            rustfs_io_metrics::record_get_object_reader_stream_poll(
                this.strategy,
                this.buffer_source,
                outcome,
                remaining_before,
                emitted_bytes,
                poll_start.map_or(0.0, |start| start.elapsed().as_secs_f64()),
            );
        }

        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.remaining == 0 || self.reader.is_none() {
            (0, Some(0))
        } else {
            (1, None)
        }
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
    inner: Option<R>,
    // bucket/object + request_id + optional content_range are only used for diagnostic
    // correlation and failure bucketing; they do not alter stream behavior. The object
    // identity is what turns a mid-stream failure into an actionable report: a request_id
    // alone cannot tell an operator which object reads short (issue #4784).
    bucket: String,
    object: String,
    request_id: String,
    content_range: Option<String>,
    expected: usize,
    emitted: usize,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    started: std::time::Instant,
    first_byte_reported: bool,
    completed: bool,
    lifecycle: GetObjectBodyLifecycle,
    resume: Option<GetObjectResumeControl<R>>,
    _foreground_read_guard: rustfs_scanner::ForegroundReadGuard,
}

impl<R> GetObjectStreamingReader<R> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: R,
        bucket: &str,
        key: &str,
        request_id: &str,
        content_range: Option<String>,
        expected: usize,
        timeout: Duration,
        lifecycle: GetObjectBodyLifecycle,
        resume: Option<GetObjectResumeControl<R>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            bucket: bucket.to_string(),
            object: key.to_string(),
            request_id: request_id.to_string(),
            content_range,
            expected,
            emitted: 0,
            timeout,
            timer: None,
            started: std::time::Instant::now(),
            first_byte_reported: false,
            completed: expected == 0,
            lifecycle,
            resume,
            _foreground_read_guard: rustfs_scanner::ForegroundReadGuard::new(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    // Classify transport/read failures before logging so operators can quickly
    // distinguish truncated upstream bodies, corruption, quorum issues, and
    // genuine downstream-close disconnects.
    fn classify_read_error(err: &std::io::Error) -> &'static str {
        classify_get_object_stream_read_error(err)
    }

    fn finish_ok(&mut self) {
        self.completed = true;
        self.lifecycle.finish_ok();
    }

    fn finish_err(&mut self) {
        self.lifecycle.finish_err();
    }

    fn resume_in_flight(&self) -> bool {
        matches!(
            self.resume.as_ref().map(|resume| &resume.stage),
            Some(GetObjectResumeStage::Backoff | GetObjectResumeStage::Reopening(_))
        )
    }

    fn begin_resume(&mut self, error: std::io::Error) {
        let Some(resume) = self.resume.as_mut() else {
            return;
        };
        self.inner.take();
        resume.begin(error);
    }

    // Drive the armed resume flow: backoff ticks gate each reopen attempt, and
    // a successful reopen swaps the failed stream out for the replacement.
    fn poll_resume(&mut self, cx: &mut Context<'_>) -> GetObjectResumePoll {
        let Some(mut resume) = self.resume.take() else {
            // resume_in_flight guards every call site.
            unreachable!("poll_resume requires an armed resume control");
        };
        let outcome = loop {
            let stage = std::mem::replace(&mut resume.stage, GetObjectResumeStage::Idle);
            match stage {
                GetObjectResumeStage::Idle => unreachable!("resume control is only polled while armed"),
                GetObjectResumeStage::Backoff => match Pin::new(&mut resume.timer).poll_next(cx) {
                    Poll::Ready(Some(())) => {
                        resume.attempts += 1;
                        resume.stage = GetObjectResumeStage::Reopening(Mutex::new((resume.reopen)(self.emitted)));
                    }
                    Poll::Ready(None) => {
                        let error = resume.take_trigger_error();
                        break GetObjectResumePoll::Failed {
                            error,
                            attempts: resume.attempts,
                        };
                    }
                    Poll::Pending => {
                        resume.stage = GetObjectResumeStage::Backoff;
                        break GetObjectResumePoll::Pending;
                    }
                },
                GetObjectResumeStage::Reopening(reopening) => {
                    let poll = match reopening.try_lock() {
                        Ok(mut reopening) => reopening.as_mut().poll(cx),
                        // Only reachable when a poll of the reopen future
                        // panicked and poisoned the mutex: fail closed with the
                        // original trigger error instead of polling it again.
                        Err(_) => {
                            let error = resume.take_trigger_error();
                            break GetObjectResumePoll::Failed {
                                error,
                                attempts: resume.attempts,
                            };
                        }
                    };
                    match poll {
                        Poll::Ready(Ok(reader)) => {
                            self.inner = Some(reader);
                            break GetObjectResumePoll::Resumed {
                                attempts: resume.attempts,
                            };
                        }
                        Poll::Ready(Err(GetObjectResumeFailure::Retryable)) => {
                            resume.stage = GetObjectResumeStage::Backoff;
                        }
                        Poll::Ready(Err(GetObjectResumeFailure::Fatal)) => {
                            let error = resume.take_trigger_error();
                            break GetObjectResumePoll::Failed {
                                error,
                                attempts: resume.attempts,
                            };
                        }
                        Poll::Pending => {
                            resume.stage = GetObjectResumeStage::Reopening(reopening);
                            break GetObjectResumePoll::Pending;
                        }
                    }
                }
            }
        };
        if matches!(outcome, GetObjectResumePoll::Resumed { .. } | GetObjectResumePoll::Pending) {
            self.resume = Some(resume);
        }
        outcome
    }

    fn poll_stall_timeout(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
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
                object = %self.object,
                request_id = %self.request_id,
                range = %self.content_range.as_deref().unwrap_or("full"),
                size_bucket = get_object_stream_size_bucket(self.expected),
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

impl<R: AsyncRead + Unpin> AsyncRead for GetObjectStreamingReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();

        loop {
            // An armed resume owns the reader until it swaps in a reopened
            // stream or exhausts its budget; the failed inner stream is never
            // polled again.
            if self.resume_in_flight() {
                match self.poll_resume(cx) {
                    GetObjectResumePoll::Resumed { attempts } => {
                        debug!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            resume_attempts = attempts,
                            state = "resumed",
                            "GetObject streaming body resumed from a reopened object read"
                        );
                        // The replacement stream starts a fresh stall window.
                        self.timer = None;
                        continue;
                    }
                    GetObjectResumePoll::Pending => return self.poll_stall_timeout(cx),
                    GetObjectResumePoll::Failed { error, attempts } => {
                        self.timer = None;
                        let failure_reason = Self::classify_read_error(&error);
                        self.finish_err();
                        error!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            elapsed_ms = self.elapsed().as_millis(),
                            state = "read_failed",
                            failure_reason = failure_reason,
                            resume_attempts = attempts,
                            error = %error,
                            "GetObject streaming body read failed; mid-stream resume did not recover"
                        );
                        return Poll::Ready(Err(error));
                    }
                }
            }

            let Some(inner) = self.inner.as_mut() else {
                self.finish_err();
                return Poll::Ready(Err(std::io::Error::other(
                    "get object streaming reader lost its active read outside resume",
                )));
            };
            match Pin::new(inner).poll_read(cx, buf) {
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
                                        object = %self.object,
                                        request_id = %self.request_id,
                                        range = %self.content_range.as_deref().unwrap_or("full"),
                                        size_bucket = get_object_stream_size_bucket(self.expected),
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
                        return Poll::Ready(Ok(()));
                    }

                    if self.emitted < self.expected {
                        // The inner reader signalled a clean EOF before delivering the full
                        // Content-Length. Returning Ok here would hand the client a truncated body
                        // under a full Content-Length: the peer treats the short body as complete
                        // (e.g. `mc mirror` writes a short file and considers it done — the
                        // "incomplete data mirroring" in issue #2955). Surface an error instead so
                        // the transfer fails loudly and the client retries rather than persisting
                        // truncated data.
                        let error = std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            rustfs_rio::IncompleteBody {
                                remaining: self.expected.saturating_sub(self.emitted) as i64,
                            },
                        );
                        // A premature EOF is also how the legacy duplex read path
                        // surfaces the object data vanishing mid-stream (typed
                        // errors do not survive that pump), so arm the resume
                        // flow before failing loudly when one is attached.
                        if self.resume.is_some() {
                            self.begin_resume(error);
                            continue;
                        }
                        error!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            elapsed_ms = self.elapsed().as_millis(),
                            state = "short_eof",
                            "GetObject streaming body ended before expected length"
                        );
                        self.finish_err();
                        return Poll::Ready(Err(error));
                    }

                    self.completed = true;
                    self.finish_ok();
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(err)) => {
                    // Typed relocation errors (the codec read path delivers them
                    // in-band) mean rebalance/decommission removed the pinned
                    // object data mid-stream: reopen and continue instead of
                    // failing the download. The error is only intercepted before
                    // the committed body length has been fully delivered.
                    if self.emitted < self.expected && is_object_relocation_error(&err) && self.resume.is_some() {
                        self.begin_resume(err);
                        continue;
                    }
                    let failure_reason = Self::classify_read_error(&err);
                    self.timer = None;
                    self.finish_err();
                    error!(
                        event = EVENT_GET_OBJECT_STREAM_BODY,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        bucket = %self.bucket,
                        object = %self.object,
                        request_id = %self.request_id,
                        range = %self.content_range.as_deref().unwrap_or("full"),
                        size_bucket = get_object_stream_size_bucket(self.expected),
                        expected = self.expected,
                        emitted = self.emitted,
                        elapsed_ms = self.elapsed().as_millis(),
                        state = "read_failed",
                        failure_reason = failure_reason,
                        error = %err,
                        "GetObject streaming body read failed"
                    );
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => return self.poll_stall_timeout(cx),
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
            object = %self.object,
            request_id = %self.request_id,
            range = %self.content_range.as_deref().unwrap_or("full"),
            size_bucket = get_object_stream_size_bucket(self.expected),
            expected = self.expected,
            emitted = self.emitted,
            elapsed_ms = self.elapsed().as_millis(),
            state = "dropped_incomplete",
            "GetObject streaming body dropped before expected length"
        );
    }
}

/// Reopen budget for a single GetObject body. Three attempts against the
/// jittered 200ms/400ms RetryTimer schedule (~600ms worst case) bound the
/// metadata fan-out a storm of relocated downloads can multiply.
const GET_OBJECT_RESUME_MAX_ATTEMPTS: i64 = 3;

type GetObjectResumeFuture<R> = Pin<Box<dyn std::future::Future<Output = Result<R, GetObjectResumeFailure>> + Send>>;
type GetObjectReopen<R> = Box<dyn FnMut(usize) -> GetObjectResumeFuture<R> + Send + Sync>;

enum GetObjectResumePoll {
    Resumed { attempts: usize },
    Pending,
    Failed { error: std::io::Error, attempts: usize },
}

/// Why a single resume attempt did not produce a replacement stream.
#[derive(Debug)]
enum GetObjectResumeFailure {
    /// Reopen/admission failure that may clear on the next attempt.
    Retryable,
    /// The reopened object is not the version this response committed to (or
    /// admission is permanently unavailable): continuing would splice two
    /// versions into one 200 response, so fail with the original error.
    Fatal,
}

enum GetObjectResumeStage<R> {
    Idle,
    Backoff,
    // The store's boxed read futures are Send but not Sync, while the
    // streaming body requires the reader to be Sync, so the in-flight reopen
    // future is stored behind a mutex. It is only ever locked under `&mut
    // self` in `poll_resume`, so the lock never contends.
    Reopening(Mutex<GetObjectResumeFuture<R>>),
}

/// Mid-stream resume machinery for [`GetObjectStreamingReader`]: when the
/// pinned object data vanishes mid-body (rebalance/decommission copies the
/// version elsewhere, then deletes the source), reopen the object at the
/// emitted offset and continue instead of failing the download.
struct GetObjectResumeControl<R> {
    reopen: GetObjectReopen<R>,
    timer: RetryTimer,
    stage: GetObjectResumeStage<R>,
    original_error: Option<std::io::Error>,
    attempts: usize,
}

impl<R> GetObjectResumeControl<R> {
    fn new(reopen: GetObjectReopen<R>, timer: RetryTimer) -> Self {
        Self {
            reopen,
            timer,
            stage: GetObjectResumeStage::Idle,
            original_error: None,
            attempts: 0,
        }
    }

    fn begin(&mut self, error: std::io::Error) {
        self.original_error = Some(error);
        self.stage = GetObjectResumeStage::Backoff;
    }

    // The trigger error is always recorded by `begin`; the fallback is a
    // fail-closed internal error, never a fabricated success.
    fn take_trigger_error(&mut self) -> std::io::Error {
        self.original_error
            .take()
            .unwrap_or_else(|| std::io::Error::other("get object resume lost its trigger error"))
    }
}

/// Object-version identity captured when the response committed to a body. A
/// resumed read must serve exactly this version; `data_dir` is deliberately
/// excluded because rebalance regenerates it for the same version.
struct GetObjectResumeIdentity {
    version_id: Option<Uuid>,
    mod_time: Option<OffsetDateTime>,
    size: i64,
    etag: Option<String>,
    // The store rewrites a read's `object_info.size` to the per-read delivered
    // length for encrypted and compressed objects (readers.rs Encrypted /
    // Compressed transforms), so a reopened subrange reports `size - emitted`
    // while a plain read reports the range-invariant `oi.size`. The flag only
    // chooses the comparison arithmetic; a transform change that no longer
    // matches it fails the identity check, which is the closed direction.
    range_dependent_size: bool,
}

impl GetObjectResumeIdentity {
    fn matches(&self, info: &ObjectInfo, emitted: usize) -> bool {
        let expected_size = if self.range_dependent_size {
            self.size - emitted as i64
        } else {
            self.size
        };
        self.version_id == info.version_id
            && self.mod_time == info.mod_time
            && expected_size == info.size
            && self.etag == info.etag
    }
}

/// Reopen parameters for a mid-stream resume. Only the SSE-C headers the store
/// read path consumes are retained: the store-level `get_object_reader` spans
/// record their header argument at debug level, so retaining the full request
/// headers would re-log credentials on every attempt.
struct GetObjectResumeContext {
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    opts: ObjectOptions,
    ssec_headers: HeaderMap,
    // Resolved plaintext offsets of the committed response body, captured
    // after `HTTPRangeSpec::get_offset_length`: suffix ranges and partNumber
    // GETs are already resolved to absolute offsets at that point, so the
    // resume offset is `range_start + emitted` regardless of request shape.
    range_start: i64,
    range_end: i64,
    identity: GetObjectResumeIdentity,
}

impl GetObjectResumeContext {
    #[allow(clippy::too_many_arguments)]
    fn new(
        store: Arc<ECStore>,
        bucket: &str,
        key: &str,
        mut opts: ObjectOptions,
        request_headers: &HeaderMap,
        info: &ObjectInfo,
        range_start: i64,
        range_end: i64,
    ) -> Self {
        if opts.version_id.is_none()
            && let Some(version_id) = info.version_id
        {
            opts.version_id = Some(version_id.to_string());
        }
        // Store spans record their header argument at debug level. Retain only
        // the SSE-C inputs needed to reopen the reader and keep them redacted.
        let ssec_headers = project_ssec_transport_headers(request_headers);
        Self {
            store,
            bucket: bucket.to_string(),
            key: key.to_string(),
            opts,
            ssec_headers,
            range_start,
            range_end,
            identity: GetObjectResumeIdentity {
                version_id: info.version_id,
                mod_time: info.mod_time,
                size: info.size,
                etag: info.etag.clone(),
                range_dependent_size: info.is_encrypted() || info.is_compressed(),
            },
        }
    }

    fn resume_range(range_start: i64, range_end: i64, emitted: usize) -> Option<HTTPRangeSpec> {
        let start = range_start + emitted as i64;
        if start == 0 && range_end < 0 {
            // Nothing was emitted from a full-object read: reopen without a
            // range so the replacement stream keeps the codec fast path
            // instead of the duplex fallback a synthesized range forces.
            return None;
        }
        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end: range_end,
        })
    }

    async fn reopen(&self, emitted: usize) -> Result<DynReader, GetObjectResumeFailure> {
        #[cfg(test)]
        GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.fetch_add(1, Ordering::Relaxed);

        // A resumed read must hold disk-read admission just like the initial
        // read; otherwise recovery reads bypass the concurrency caps exactly
        // while rebalance is stressing the pool.
        let disk_permit = DefaultObjectUsecase::admit_get_object_disk_read(get_concurrency_manager(), &self.bucket, &self.key)
            .await
            .map_err(|err| {
                if err.code() == &S3ErrorCode::SlowDown {
                    GetObjectResumeFailure::Retryable
                } else {
                    GetObjectResumeFailure::Fatal
                }
            })?;
        let range = Self::resume_range(self.range_start, self.range_end, emitted);
        let reader = self
            .store
            .get_object_reader(&self.bucket, &self.key, range, self.ssec_headers.clone(), &self.opts)
            .await
            .map_err(|err| {
                debug!(
                    bucket = %self.bucket,
                    object = %self.key,
                    error = %err,
                    "GetObject mid-stream resume reopen failed"
                );
                GetObjectResumeFailure::Retryable
            })?;
        if !self.identity.matches(&reader.object_info, emitted) {
            warn!(
                bucket = %self.bucket,
                object = %self.key,
                "GetObject mid-stream resume resolved a different object version; refusing to splice content"
            );
            return Err(GetObjectResumeFailure::Fatal);
        }
        let stream = wrap_reader(reader.stream);
        Ok(match disk_permit {
            Some(disk_permit) => wrap_reader(DiskReadPermitReader::new(stream, disk_permit)),
            None => stream,
        })
    }
}

#[cfg(test)]
static GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

fn get_object_resume_control(ctx: GetObjectResumeContext) -> GetObjectResumeControl<DynReader> {
    use rand::RngExt as _;
    let ctx = Arc::new(ctx);
    let reopen: GetObjectReopen<DynReader> = Box::new(move |emitted| {
        let ctx = Arc::clone(&ctx);
        Box::pin(async move { ctx.reopen(emitted).await })
    });
    GetObjectResumeControl::new(
        reopen,
        RetryTimer::new(
            GET_OBJECT_RESUME_MAX_ATTEMPTS,
            DEFAULT_RETRY_UNIT,
            DEFAULT_RETRY_CAP,
            MAX_JITTER,
            rand::rng().random_range(10..=50),
        ),
    )
}

/// Mid-stream errors that mean the pinned object data is gone (rebalance or
/// decommission removed it after copying the version elsewhere). Only typed
/// `StorageError`s qualify; generic I/O errors and string-matched "not enough
/// disks" failures keep the existing fail-loud behavior.
fn is_object_relocation_error(err: &std::io::Error) -> bool {
    let Some(inner) = err.get_ref() else { return false };
    match inner.downcast_ref::<StorageError>() {
        Some(StorageError::FileNotFound | StorageError::ObjectNotFound(..) | StorageError::InsufficientReadQuorum(..)) => true,
        Some(StorageError::Io(source)) => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
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
            md5: Md5::new(),
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
                    this.md5.update(filled);
                } else if !*this.finished {
                    *this.finished = true;
                    if let Ok(mut etag) = this.etag.lock() {
                        *etag = Some(hex_simd::encode_to_string(this.md5.clone().finalize(), hex_simd::AsciiCase::Lower));
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
fn resolve_bucket_default_sse(
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
            .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
        if read == 0 {
            return Err(s3_error!(IncompleteBody));
        }
        filled += read;
    }

    let mut extra = [0u8; 1];
    let extra_read = tokio::io::AsyncReadExt::read(&mut *body, &mut extra)
        .await
        .map_err(|err| ApiError::from(StorageError::other(err.to_string())))?;
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

fn inject_accept_ranges_header(headers: &mut HeaderMap) {
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

#[derive(Default)]
struct PutObjectChecksums {
    crc32: Option<String>,
    crc32c: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
    crc64nvme: Option<String>,
}

struct PutObjectCommitResult {
    obj_info: ObjectInfo,
    put_versioned: bool,
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

fn internal_object_info_lookup_opts(mut opts: ObjectOptions) -> ObjectOptions {
    opts.http_preconditions = None;
    opts
}

fn expected_current_version_id(headers: &HeaderMap) -> S3Result<Option<String>> {
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

#[derive(Debug, Default)]
struct ExtractEntryPaxAuthorization {
    headers: HeaderMap,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
}

async fn apply_extract_entry_pax_extensions<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    bucket: &str,
    object_name: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> S3Result<ExtractEntryPaxAuthorization>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry.pax_extensions().await.map_err(map_extract_archive_error)? else {
        return Ok(ExtractEntryPaxAuthorization::default());
    };

    let mut pax_headers = HeaderMap::new();
    let mut pax_version_id = None;
    for ext in extensions {
        let ext = ext.map_err(map_extract_archive_error)?;
        let key = ext.key().map_err(map_extract_archive_error)?;
        let value = ext.value().map_err(map_extract_archive_error)?;

        if let Some(meta_key) = key.strip_prefix("minio.metadata.") {
            if !meta_key.is_empty() {
                let name = http::HeaderName::from_bytes(meta_key.as_bytes())
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball PAX metadata header"))?;
                let header_value = HeaderValue::from_str(value)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball PAX metadata value"))?;
                preserve_unclassified_user_metadata(metadata, name.as_str(), value);
                pax_headers.insert(name, header_value);
            }
            continue;
        }

        if key == "minio.versionId" && !value.is_empty() {
            if Uuid::parse_str(value).is_err() {
                return Err(s3_error!(InvalidArgument, "Invalid Snowball PAX version ID"));
            }
            pax_version_id = Some(value.to_string());
        }
    }

    let has_replica_status = pax_headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS);
    if let Some(value) = pax_headers.get(AMZ_BUCKET_REPLICATION_STATUS) {
        let status = value
            .to_str()
            .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball replication status"))?;
        if !status.eq_ignore_ascii_case(ReplicationStatusType::Replica.as_str()) {
            return Err(s3_error!(InvalidArgument, "Invalid Snowball replication status"));
        }
        pax_headers.insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
    }

    let authorization_headers = pax_headers.clone();

    if let Some(value) = pax_headers.remove("x-amz-tagging") {
        let value = value
            .to_str()
            .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball object tagging value"))?;
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), value.to_owned());
    }

    let object_lock_mode = pax_headers
        .remove(AMZ_OBJECT_LOCK_MODE_LOWER)
        .map(|value| {
            value
                .to_str()
                .map(|value| ObjectLockMode::from(value.to_string()))
                .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball Object Lock mode"))
        })
        .transpose()?;
    let object_lock_retain_until_date = pax_headers
        .remove(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
        .map(|value| {
            let value = value
                .to_str()
                .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball Object Lock retain-until date"))?;
            OffsetDateTime::parse(value, &Rfc3339)
                .map(Timestamp::from)
                .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball Object Lock retain-until date"))
        })
        .transpose()?;
    let object_lock_legal_hold_status = pax_headers
        .remove(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
        .map(|value| {
            value
                .to_str()
                .map(|value| ObjectLockLegalHoldStatus::from(value.to_string()))
                .map_err(|_| s3_error!(InvalidArgument, "Invalid Snowball Object Lock legal-hold status"))
        })
        .transpose()?;
    opts.version_id = pax_version_id;

    extract_metadata_from_mime_with_object_name(&pax_headers, metadata, false, Some(object_name));
    if has_replica_status {
        metadata.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS));
        metadata.insert(
            AMZ_BUCKET_REPLICATION_STATUS.to_string(),
            ReplicationStatusType::Replica.as_str().to_string(),
        );
    }
    if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
        bucket,
        object_lock_config_state,
        object_lock_legal_hold_status.clone(),
        object_lock_mode.clone(),
        object_lock_retain_until_date.clone(),
    )? {
        metadata.extend(object_lock_metadata);
    }

    Ok(ExtractEntryPaxAuthorization {
        headers: authorization_headers,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    })
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
    #[cfg(test)]
    get_object_timeout_policy: Option<GetObjectTimeoutPolicy>,
}

async fn track_object_read_setup<F>(health: Option<&ObjectTrafficHealth>, future: F) -> F::Output
where
    F: std::future::Future,
{
    let _progress = health.and_then(ObjectTrafficHealth::track_read_storage);
    future.await
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
        let blob = if is_get_small_body_once_enabled() && bytes_len == remaining {
            let owner = MemoryOnceBodyOwner::new(bytes, guard, lifecycle);
            StreamingBlob::from_bytes(Bytes::from_owner(owner))
        } else {
            StreamingBlob::new(MemoryTrackedBytesStream::new(bytes, remaining, source, guard, lifecycle))
        };
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

    #[allow(clippy::too_many_arguments)]
    fn build_reader_blob<R>(
        reader: R,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
        stream_buffer_size: usize,
        stream_strategy: GetObjectStreamStrategy,
        bucket: &str,
        key: &str,
        lifecycle: GetObjectBodyLifecycle,
        resume: Option<GetObjectResumeControl<R>>,
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
        let reader = GetObjectStreamingReader::new(
            reader,
            bucket,
            key,
            request_id,
            content_range.map(|content_range| content_range.to_string()),
            expected,
            get_object_disk_read_timeout(),
            lifecycle,
            resume,
        );
        let stream = GetObjectReaderStream::new(reader, stream_buffer_size, expected, stream_strategy.as_str(), buffer_source)
            .with_diagnostics(bucket, key, request_id);
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

    fn init_get_object_bootstrap(&self, bucket: &str, key: &str, request_id: &str) -> S3Result<GetObjectBootstrap> {
        #[cfg(test)]
        let timeout_config = self
            .get_object_timeout_policy
            .clone()
            .unwrap_or_else(GetObjectTimeoutPolicy::cached_from_env);
        #[cfg(not(test))]
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
        let disk_permit = Self::admit_get_object_disk_read(manager, bucket, key).await?;
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
            disk_permit,
            permit_wait_duration,
            queue_status,
            queue_utilization,
        })
    }

    // Shared by the initial read path and the mid-stream resume reopen, which
    // must hold the same admission token before touching disks. The permit
    // wait inside is bounded by the primary-pool timeout.
    async fn admit_get_object_disk_read(
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<GetObjectDiskPermit>> {
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
        Ok(disk_permit.map(GetObjectDiskPermit::new))
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

    /// Cheap request-shape validations, run before the bucket-existence store
    /// lookup so invalid requests keep their InvalidArgument precedence.
    fn validate_get_object_request(req: &S3Request<GetObjectInput>) -> S3Result<GetObjectValidatedRequest> {
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

        Ok(GetObjectValidatedRequest {
            bucket,
            key,
            version_id,
            part_number,
            rs,
        })
    }

    async fn prepare_get_object_request_context(
        validated: GetObjectValidatedRequest,
        headers: &HeaderMap,
    ) -> S3Result<GetObjectRequestContext> {
        let GetObjectValidatedRequest {
            bucket,
            key,
            version_id,
            part_number,
            rs,
        } = validated;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), part_number, headers)
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
        store: Arc<ECStore>,
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        bucket: &str,
        key: &str,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
        part_number: Option<usize>,
        object_traffic_health: Option<Arc<ObjectTrafficHealth>>,
    ) -> S3Result<GetObjectPreparedRead> {
        let read_start = std::time::Instant::now();
        let read_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then_some(read_start);
        let store_headers = project_ssec_transport_headers(&req.headers);
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
            let reader = track_object_read_setup(
                object_traffic_health.as_deref(),
                store.get_object_reader(bucket, key, rs.clone(), store_headers, opts),
            )
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
            track_object_read_setup(
                object_traffic_health.as_deref(),
                store.prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts),
            )
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
                let headers = &store_headers;
                let store = &store;
                let range = &rs;
                let object_traffic_health = &object_traffic_health;
                move |producer| {
                    let adapter = Arc::clone(adapter);
                    let engine_plan = engine_plan.clone();
                    let h = headers.clone();
                    let store = Arc::clone(store);
                    let range = range.clone();
                    let bucket = bucket.to_owned();
                    let key = key.to_owned();
                    let opts = opts.clone();
                    let object_traffic_health = object_traffic_health.as_ref().map(Arc::clone);
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

                        let prepare = track_object_read_setup(
                            object_traffic_health.as_deref(),
                            store.prepare_get_object_reader(&bucket, &key, range.clone(), HeaderMap::new(), &opts),
                        );
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
                                let open_reader = prepared.with_headers(h).into_reader();
                                async move { track_object_read_setup(object_traffic_health.as_deref(), open_reader).await }
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
            let reader =
                track_object_read_setup(object_traffic_health.as_deref(), prepared.with_headers(store_headers).into_reader())
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
                let prepared = track_object_read_setup(
                    object_traffic_health.as_deref(),
                    store.prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts),
                )
                .await
                .map_err(map_get_object_reader_error)?;
                track_object_read_setup(object_traffic_health.as_deref(), prepared.with_headers(store_headers).into_reader())
                    .await
                    .map_err(map_get_object_reader_error)?
            } else {
                track_object_read_setup(
                    object_traffic_health.as_deref(),
                    store.get_object_reader(bucket, key, rs.clone(), store_headers, opts),
                )
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
        let (resume_range_start, resume_range_end, content_range) = if let Some(rs) = &rs {
            let total_size = content_length;
            let (start, length) = rs.get_offset_length(total_size).map_err(ApiError::from)?;
            content_length = length;
            let start = start as i64;
            // Inclusive end of the committed body; may precede `start` when a
            // zero-length range was requested, in which case the body completes
            // immediately and the resume range is never consulted.
            (
                start,
                start + length - 1,
                Some(format!("bytes {}-{}/{}", start, start + length - 1, total_size)),
            )
        } else {
            (0, -1, None)
        };

        debug!(
            "GET object metadata check: parts={}, provided_sse_key={:?}",
            info.parts.len(),
            req.input.sse_customer_key.is_some()
        );

        let read_principal = SseKmsPrincipal::from_request(req);
        let decryption_request = DecryptionRequest {
            bucket,
            key,
            metadata: &info.user_defined,
            sse_customer_key: req.input.sse_customer_key.as_ref(),
            sse_customer_key_md5: req.input.sse_customer_key_md5.as_ref(),
            principal: read_principal.as_ref(),
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
            resume_range_start,
            resume_range_end,
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
            let (decrypted_checksums, is_multipart) = info.decrypt_checksums(part_number.unwrap_or(0), headers).map_err(|e| {
                error!(error = %e, "GetObject checksum decryption failed");
                ApiError::from(e)
            })?;

            return Ok(classify_response_checksums(decrypted_checksums, is_multipart));
        }

        Ok(ResponseChecksums::default())
    }
    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body<R, F>(
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
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
        resume: F,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<R>>,
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
                request_id,
                content_range,
                stream_buffer_size,
                stream_strategy,
                bucket,
                key,
                lifecycle,
                resume(info),
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
            request_id,
            content_range,
            stream_buffer_size,
            stream_strategy,
            bucket,
            key,
            lifecycle,
            resume(info),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body_with_cache<R, F>(
        cache_adapter: &ObjectDataCacheAdapter,
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        concurrent_requests: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
        mut buffered_body: Option<Bytes>,
        cache_hook_served: bool,
        cache_hook_probed: bool,
        cache_fill_allowed: bool,
        bucket: &str,
        key: &str,
        mut lifecycle: GetObjectBodyLifecycle,
        resume: F,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<R>>,
    {
        // ODC-16 (backlog#1121): when the ecstore hook or shared cold fill
        // already supplied this body, the request-level plan was built before
        // the authoritative lookup. Serve it without planning a second time.
        if cache_hook_served && let Some(bytes) = buffered_body.take() {
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
                request_id,
                content_range,
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
                resume,
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
                    request_id,
                    content_range,
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
                    resume,
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
            request_id,
            content_range,
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
            resume,
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
                    validate_existing_object_lock_for_write(&existing_obj_info, &opts)?;
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
            } else if use_empty_or_small_eager_put_path {
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
        let PutObjectCommitResult { obj_info, put_versioned } = match put_commit.await {
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
        inject_accept_ranges_header(&mut response.headers);
        // Emit XXHash3/64/128 and SHA-512 checksums that s3s GetObjectOutput cannot
        // carry (#1257). This is the download-side integrity path AWS SDKs verify.
        inject_additional_checksum_headers(&mut response.headers, &extra_checksum_headers);
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_output_context<F>(
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
        request_id: &str,
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
        resume: F,
    ) -> S3Result<GetObjectOutputContext>
    where
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<DynReader>>,
    {
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
            request_id,
            content_range.as_deref(),
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
            resume,
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
        let cache_control = info.user_defined.get("cache-control").cloned();
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
            cache_control,
            content_disposition,
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

    /// Headers a proxied read forwards verbatim to the replication target:
    /// only the client's SSE-C key family, so the target performs the real
    /// SSE-C decryption (never the replication-check exemption). HTTP
    /// conditional headers (If-Match & co.) are deliberately NOT forwarded —
    /// MinIO does not forward them either, and a remote 304/412 would leak a
    /// conditional evaluation against a replica the local site never saw.
    /// Range and part-number travel as typed SDK parameters instead.
    fn proxy_read_passthrough_headers(headers: &HeaderMap) -> HeaderMap {
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
    fn proxy_sdk_error_is_not_found<E>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
        err.raw_response().is_some_and(|resp| resp.status().as_u16() == 404)
    }

    /// Serve a GET whose local read failed with not-found by proxying to the
    /// bucket's replication targets (MinIO `proxyGetToReplicationTarget`,
    /// backlog#1675 P1-5). Returns None when no target can serve the object;
    /// the caller then returns the original local error.
    async fn proxy_get_object_to_replication_targets(
        req: &S3Request<GetObjectInput>,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Option<GetObjectOutput> {
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
                .get_object(
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
                    // MinIO-aligned accounting: one total per proxy attempt
                    // (targets were available), one failed when no target
                    // served it — never per target.
                    record_replication_proxy(bucket, "GetObject", false).await;
                    return Some(Self::proxy_sdk_get_output_to_s3s(remote));
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, key, arn = %target.arn, "read proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, key, arn = %target.arn, error = %err, "read proxy: GET against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "GetObject", true).await;
        None
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

    /// Translate a proxied SDK GET response into the s3s output, forwarding
    /// the body as a stream (no buffering, no local persistence).
    fn proxy_sdk_get_output_to_s3s(remote: aws_sdk_s3::operation::get_object::GetObjectOutput) -> GetObjectOutput {
        let body = remote.body;
        let body_stream = tokio_util::io::ReaderStream::with_capacity(body.into_async_read(), 64 * 1024);
        GetObjectOutput {
            body: Some(StreamingBlob::wrap(body_stream)),
            content_length: remote.content_length,
            content_range: remote.content_range,
            content_type: remote.content_type.as_deref().and_then(|v| ContentType::from_str(v).ok()),
            content_encoding: remote.content_encoding,
            content_disposition: remote.content_disposition,
            content_language: remote.content_language,
            cache_control: remote.cache_control,
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
            tag_count: remote.tag_count,
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

    #[instrument(name = "execute_get_object", level = "trace", skip(self, req))]
    pub async fn execute_get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        self.execute_get_object_boxed(req).await
    }

    fn execute_get_object_boxed(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<GetObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_get_object_inner(req))
    }

    #[hotpath::measure(
        label = "rustfs::app::object_usecase::DefaultObjectUsecase::execute_get_object",
        impl_type = "DefaultObjectUsecase"
    )]
    async fn execute_get_object_inner(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
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
        let bootstrap = self.init_get_object_bootstrap(&req.input.bucket, &req.input.key, &request_id)?;
        let timeout_config = bootstrap.timeout_config;
        let wrapper = bootstrap.wrapper;
        let request_start = bootstrap.request_start;
        let concurrent_requests = bootstrap.concurrent_requests;
        let mut lifecycle = GetObjectBodyLifecycle::tracked(bootstrap.request_guard);

        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        // mc get 3

        // Cheap request-shape validations run first so invalid requests keep
        // their InvalidArgument precedence over bucket existence.
        let validated = match Self::validate_get_object_request(&req) {
            Ok(validated) => validated,
            Err(err) => {
                lifecycle.finish_err();
                return Err(err);
            }
        };

        // SF05: Store lookup next (5s-TTL bucket-validation cache). Bucket
        // existence is established before any bucket-metadata work, so requests
        // naming nonexistent buckets fail before the versioning lookup in
        // get_opts. The store comes from the request-bound server context
        // (backlog#1052 S6), not the process-global handle.
        let object_traffic_health = self.object_traffic_health();
        let object_metadata_progress = object_traffic_health
            .as_deref()
            .and_then(ObjectTrafficHealth::track_read_metadata);
        let store_lookup_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let Some(store) = self.object_store() else {
            lifecycle.finish_err();
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        if let Err(err) = validate_bucket_exists(&store, &req.input.bucket).await {
            lifecycle.finish_err();
            return Err(err);
        }
        if let Some(store_lookup_start) = store_lookup_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "store_lookup",
                store_lookup_start.elapsed().as_secs_f64(),
            );
        }

        let request_context_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let request_context = match Self::prepare_get_object_request_context(validated, &req.headers).await {
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
        drop(object_metadata_progress);

        let manager = get_concurrency_manager();

        let prepared_read = match self
            .prepare_get_object_read_execution(
                &req,
                manager,
                store.clone(),
                &wrapper,
                &timeout_config,
                &bucket,
                &key,
                rs,
                &opts,
                part_number,
                object_traffic_health,
            )
            .await
        {
            Ok(prepared_read) => prepared_read,
            Err(err) => {
                // Active-active replication lag window: an object missing
                // locally (and only missing — other errors keep their
                // semantics) may still be served by proxying the GET to a
                // replication target (backlog#1675 P1-5).
                if matches!(*err.code(), S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion)
                    && let Some(output) = Self::proxy_get_object_to_replication_targets(&req, &bucket, &key, &opts).await
                {
                    lifecycle.finish_ok();
                    let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;
                    inject_accept_ranges_header(&mut response.headers);
                    let result = Ok(response);
                    let _ = helper.version_id(version_id_for_event).complete(&result);
                    return result;
                }
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
            resume_range_start,
            resume_range_end,
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
                &request_id,
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
                |info| {
                    Some(get_object_resume_control(GetObjectResumeContext::new(
                        store,
                        &bucket,
                        &key,
                        opts,
                        &req.headers,
                        info,
                        resume_range_start,
                        resume_range_end,
                    )))
                },
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

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.include_part_checksums = object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_PARTS);

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
            let (checksums, is_multipart) = info.decrypt_checksums(0, &req.headers).map_err(ApiError::from)?;
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
            } = classify_response_checksums(checksums, is_multipart);

            Some(Checksum {
                checksum_crc32,
                checksum_crc32c,
                checksum_sha1,
                checksum_sha256,
                checksum_crc64nvme,
                checksum_type,
                ..Default::default()
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
                let (checksums, is_multipart) = info.decrypt_checksums(part.number, &req.headers).map_err(ApiError::from)?;
                // Additional algorithms cannot be surfaced in the ObjectPart XML body
                // (s3s has no field); same limitation as the object-level attributes above.
                let ResponseChecksums {
                    crc32: checksum_crc32,
                    crc32c: checksum_crc32c,
                    sha1: checksum_sha1,
                    sha256: checksum_sha256,
                    crc64nvme: checksum_crc64nvme,
                    ..
                } = classify_response_checksums(checksums, is_multipart);

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
                    ..Default::default()
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
                validate_existing_object_lock_for_write(&existing_obj_info, &dst_opts)?;
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

        let gr = store
            .get_object_reader(&src_bucket, &src_key, None, h, &src_get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

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

    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "DefaultObjectUsecase")]
    pub async fn execute_put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        self.execute_put_object_extract_boxed(req).await
    }

    fn execute_put_object_extract_boxed(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<PutObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_put_object_extract_inner(req))
    }

    async fn execute_put_object_extract_inner(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, S3Operation::PutObject).suppress_event();
        let request_context = helper.request_context_or_from_request(&req);
        let auth_method = req.method.clone();
        let auth_uri = req.uri.clone();
        let auth_headers = req.headers.clone();
        let auth_extensions = req.extensions.clone();
        let auth_credentials = req.credentials.clone();
        let auth_region = req.region.clone();
        let auth_service = req.service.clone();
        let auth_trailing_headers = req.trailing_headers.clone();
        // Extract uploads reject SSE-KMS before reaching the SSE layer, so the principal is
        // only carried for the day that restriction lifts; the NotImplemented answer below
        // deliberately stays ahead of any key authorization.
        let extract_principal = SseKmsPrincipal::from_request(&req);
        if is_sse_kms_requested(&req.input, &req.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        let replication_authorized = replication_request_authorized(&req);
        let mut bucket_generation_opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut bucket_generation_opts)?;
        let expected_bucket_incarnation_id = bucket_generation_opts.expected_bucket_incarnation_id;
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
        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _timestamp)| config),
            original_sse,
            ssekms_key_id,
            false,
        );
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
        let _ = self
            .check_bucket_quota(
                &bucket,
                QuotaOperation::PutObject,
                u64::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
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
        let extract_quota_check = if let Some(metadata_sys) = self.bucket_metadata_sys() {
            let quota_checker = QuotaChecker::new(metadata_sys);
            let check_result =
                map_quota_check_outcome(&bucket, quota_checker.check_quota(&bucket, QuotaOperation::PutObject, 0).await)?;
            Some(check_result)
        } else {
            None
        };
        let extract_quota_enabled = extract_quota_check
            .as_ref()
            .is_some_and(|result| result.quota_limit.is_some());
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let notify = current_notify_interface_for_context(self.context.as_deref());
        let req_params = rustfs_targets::extract_params_header(&req.headers);
        let host = get_request_host(&req.headers);
        let port = get_request_port(&req.headers);
        let user_agent = get_request_user_agent(&req.headers);
        let mut wrote_any_entry = false;
        let mut extracted_entry_count = 0usize;
        let mut total_unpacked_size = 0u64;
        let object_lock_config_snapshot = store.object_lock_config_snapshot(&bucket).await.map_err(ApiError::from)?;
        let object_lock_config_state = object_lock_config_snapshot.state();

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
            let entry_size = f.header().size().unwrap_or_default();
            validate_put_object_extract_entry_size(&fpath, entry_size, extract_limits)?;
            total_unpacked_size = total_unpacked_size
                .checked_add(entry_size)
                .ok_or_else(|| s3_error!(InvalidArgument, "Archive total unpacked size overflowed while processing entries"))?;
            validate_put_object_extract_total_size(total_unpacked_size, extract_limits)?;
            if let Some(quota_check) = extract_quota_check.as_ref() {
                ensure_legacy_archive_size_within_quota(quota_check, total_unpacked_size)?;
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
            apply_bucket_default_lock_retention(
                &bucket,
                object_lock_config_state,
                &mut metadata,
                has_explicit_object_lock_retention,
            )?;
            let mut opts = put_opts_with_replication_authorization(
                &bucket,
                &fpath,
                None,
                &req.headers,
                metadata.clone(),
                replication_authorized,
            )
            .await
            .map_err(ApiError::from)?;
            if let Some(quota_check) = extract_quota_check.as_ref() {
                apply_quota_admission(&mut opts, quota_check)?;
            }
            opts.expected_bucket_incarnation_id = expected_bucket_incarnation_id;
            opts.object_lock_config_snapshot = Some(Arc::clone(&object_lock_config_snapshot));
            let pax_authorization =
                apply_extract_entry_pax_extensions(&mut f, &bucket, &fpath, object_lock_config_state, &mut metadata, &mut opts)
                    .await?;
            for (name, value) in &pax_authorization.headers {
                auth_req.headers.insert(name.clone(), value.clone());
            }
            if let Some(version_id) = opts.version_id.as_ref() {
                req_info_mut(&mut auth_req)?.version_id = Some(version_id.clone());
            }
            authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectAction)).await?;
            if pax_authorization.object_lock_mode.is_some() || pax_authorization.object_lock_retain_until_date.is_some() {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
            }
            if pax_authorization.object_lock_legal_hold_status.is_some() {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
            }
            if opts.version_id.is_some() || pax_authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS) {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
            }
            let effective_object_lock_legal_hold_status = pax_authorization
                .object_lock_legal_hold_status
                .clone()
                .or_else(|| object_lock_legal_hold_status.clone());
            let (effective_object_lock_mode, effective_object_lock_retain_until_date) =
                if pax_authorization.object_lock_mode.is_some() || pax_authorization.object_lock_retain_until_date.is_some() {
                    (
                        pax_authorization.object_lock_mode.clone(),
                        pax_authorization.object_lock_retain_until_date.clone(),
                    )
                } else {
                    (object_lock_mode.clone(), object_lock_retain_until_date.clone())
                };
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
                object_lock_config_state,
                effective_object_lock_legal_hold_status,
                effective_object_lock_mode,
                effective_object_lock_retain_until_date,
                &mut opts,
            )?;
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
                principal: extract_principal.as_ref(),
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

            // Each extracted member is an independent user write and joins
            // bucket replication like a regular PUT (MinIO PutObjectExtract
            // parity). One immutable decision drives both the pending metadata
            // and the post-commit schedule below, same contract as the PUT path
            // (https://github.com/rustfs/backlog/issues/1320); inbound replica
            // writes are declined inside `must_replicate_object`.
            let dsc = must_replicate_object(
                &bucket,
                &fpath,
                &opts.user_defined,
                "".to_string(),
                opts.delete_marker_replication_status(),
                opts.clone(),
            )
            .await;
            if dsc.replicate_any() {
                insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
                insert_str(
                    &mut opts.user_defined,
                    SUFFIX_REPLICATION_STATUS,
                    dsc.pending_status().unwrap_or_default(),
                );
            }

            let mut reader = PutObjReader::new(hrd);
            let cache_adapter = self.object_data_cache();
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &fpath).await;

            let (obj_info, backfilled_old_current_size) = match store
                .put_object_with_old_current_size(&bucket, &fpath, &mut reader, &opts)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if extract_options.ignore_errors {
                        warn!(error = %e, "Archive object write skipped due to ignore-errors");
                        continue;
                    }
                    return Err(ApiError::from(e).into());
                }
            };
            let committed_size = quota_accounting_object_size(&obj_info, extract_quota_enabled)?;
            let extract_versioned = BucketVersioningSys::prefix_enabled(&bucket, &fpath).await;
            match previous_current_size_from_backfill(backfilled_old_current_size) {
                Some(previous_current_size) => {
                    if extract_versioned {
                        record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                    } else {
                        record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                    }
                }
                None => {
                    record_bucket_object_write_unknown_previous_memory(&bucket, committed_size, extract_versioned).await;
                }
            }
            let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &fpath).await;

            // Reuse the per-entry pre-commit decision (see `dsc` above) so the
            // persisted pending marker and the schedule always agree.
            if dsc.replicate_any() {
                schedule_object_replication(obj_info.clone(), store.clone(), dsc).await;
            }

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
                resp_elements: build_event_resp_elements(&S3Response::new(output.clone()), &request_context.request_id),
                version_id: version_id.clone(),
                host: host.clone(),
                port,
                user_agent: user_agent.clone(),
            };

            let notify = notify.clone();
            spawn_background_with_context(Some(request_context.clone()), async move {
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

/// Fail closed when deciding whether an object-lock-sensitive operation may
/// skip its existing-object lookup.
pub(super) async fn object_lock_checks_required(bucket: &str) -> bool {
    get_bucket_metadata(bucket)
        .await
        .map_or(true, |metadata| metadata.object_locking())
}

fn object_lock_checks_required_for_state(state: &metadata_sys::ObjectLockConfigState) -> bool {
    match state {
        metadata_sys::ObjectLockConfigState::Configured { .. } | metadata_sys::ObjectLockConfigState::Fabricated => true,
        metadata_sys::ObjectLockConfigState::ConfirmedAbsent => false,
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
        DefaultRetention, Delete, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication,
        DeleteReplicationStatus, Destination, ExistingObjectReplication, ExistingObjectReplicationStatus, ObjectIdentifier,
        ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRule, ReplicaModifications, ReplicaModificationsStatus,
        ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, RestoreRequest, ServerSideEncryptionByDefault,
        ServerSideEncryptionConfiguration, ServerSideEncryptionRule, SourceSelectionCriteria,
    };
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio_tar::{Builder, EntryType, Header};

    #[test]
    fn delete_response_version_id_preserves_null_and_synthetic_semantics() {
        let version_id = Uuid::new_v4();

        assert_eq!(delete_response_version_id(Some(version_id), false), Some(version_id.to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), false), Some("null".to_string()));
        assert_eq!(delete_response_version_id(Some(Uuid::nil()), true), None);
        assert_eq!(delete_response_version_id(None, false), None);
    }

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

    #[tokio::test]
    async fn finalize_get_object_response_injects_accept_ranges_header() {
        let req = build_request(GetObjectInput::default(), Method::GET);
        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        let response = DefaultObjectUsecase::finalize_get_object_response(
            helper,
            "bucket",
            &req.method,
            &req.headers,
            None,
            String::new(),
            GetObjectOutput::default(),
            Vec::new(),
        )
        .await
        .expect("finalize response");

        assert_eq!(response.headers.get(http::header::ACCEPT_RANGES).unwrap(), ACCEPT_RANGES_BYTES);
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

    fn pax_record(key: &str, value: &[u8]) -> Vec<u8> {
        let body_len = 1 + key.len() + 1 + value.len() + 1;
        let mut len = body_len + 1;
        loop {
            let actual_len = len.to_string().len() + body_len;
            if actual_len == len {
                break;
            }
            len = actual_len;
        }

        let mut record = format!("{len} {key}=").into_bytes();
        record.extend_from_slice(value);
        record.push(b'\n');
        assert_eq!(record.len(), len);
        record
    }

    #[tokio::test]
    async fn snowball_pax_rejects_unpaired_object_lock_retention() {
        let record = pax_record("minio.metadata.x-amz-object-lock-mode", b"GOVERNANCE");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2030-01-01T00:00:00Z".to_string()),
        ]);
        let mut opts = ObjectOptions::default();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        let err = apply_extract_entry_pax_extensions(&mut entry, "bucket", "object", &state, &mut metadata, &mut opts)
            .await
            .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("COMPLIANCE"));
    }

    #[tokio::test]
    async fn snowball_pax_privileged_fields_require_independent_authorization() {
        let mut retention = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"GOVERNANCE");
        retention.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        let cases = [
            ("retention", retention, (true, false, false)),
            (
                "legal-hold",
                pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON"),
                (false, true, false),
            ),
            (
                "version-id",
                pax_record("minio.versionId", Uuid::nil().to_string().as_bytes()),
                (false, false, true),
            ),
            (
                "replication-status",
                pax_record("minio.metadata.x-amz-replication-status", b"REPLICA"),
                (false, false, true),
            ),
        ];
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        for (case, record, expected) in cases {
            let mut builder = Builder::new(Vec::new());
            let mut extension = Header::new_ustar();
            extension.set_size(record.len() as u64);
            extension.set_entry_type(EntryType::XHeader);
            builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();
            let mut file = Header::new_ustar();
            file.set_size(0);
            builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
            let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
            let mut entries = archive.entries().unwrap();
            let mut entry = entries.next().await.unwrap().unwrap();

            let mut opts = ObjectOptions::default();
            let authorization =
                apply_extract_entry_pax_extensions(&mut entry, "bucket", "object", &state, &mut HashMap::new(), &mut opts)
                    .await
                    .unwrap();

            assert_eq!(
                (
                    authorization.object_lock_mode.is_some() || authorization.object_lock_retain_until_date.is_some(),
                    authorization.object_lock_legal_hold_status.is_some(),
                    opts.version_id.is_some() || authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS),
                ),
                expected,
                "{case} must request only its own additional authorization"
            );
            match case {
                "retention" => {
                    assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER));
                    assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
                }
                "legal-hold" => assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)),
                "version-id" => assert_eq!(opts.version_id.as_deref(), Some(Uuid::nil().to_string().as_str())),
                "replication-status" => assert_eq!(
                    authorization
                        .headers
                        .get(AMZ_BUCKET_REPLICATION_STATUS)
                        .and_then(|value| value.to_str().ok()),
                    Some("REPLICA")
                ),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn snowball_pax_rejects_invalid_retention_and_replication_values() {
        let mut invalid_mode = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"INVALID");
        invalid_mode.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        let mut invalid_date = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"COMPLIANCE");
        invalid_date.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"not-a-date"));
        let cases = [
            ("invalid-mode", invalid_mode),
            ("invalid-date", invalid_date),
            (
                "invalid-replication-status",
                pax_record("minio.metadata.x-amz-replication-status", b"INVALID"),
            ),
            ("invalid-version-id", pax_record("minio.versionId", b"not-a-uuid")),
        ];
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        for (case, record) in cases {
            let mut builder = Builder::new(Vec::new());
            let mut extension = Header::new_ustar();
            extension.set_size(record.len() as u64);
            extension.set_entry_type(EntryType::XHeader);
            builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();
            let mut file = Header::new_ustar();
            file.set_size(0);
            builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
            let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
            let mut entries = archive.entries().unwrap();
            let mut entry = entries.next().await.unwrap().unwrap();

            let err = apply_extract_entry_pax_extensions(
                &mut entry,
                "bucket",
                "object",
                &state,
                &mut HashMap::new(),
                &mut ObjectOptions::default(),
            )
            .await
            .unwrap_err();

            assert!(
                err.code() == &S3ErrorCode::InvalidArgument || err.code() == &S3ErrorCode::MalformedXML,
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn snowball_pax_preserves_canonical_minio_metadata_and_valid_retention() {
        let mut record = pax_record("minio.metadata.Content-Type", b"text/plain");
        record.extend(pax_record("minio.metadata.X-Amz-Meta-Owner", b"alice"));
        record.extend(pax_record("minio.metadata.project", b"alpha-demo"));
        record.extend(pax_record("minio.metadata.x-amz-tagging", b"classification=public"));
        record.extend(pax_record("minio.versionId", Uuid::nil().to_string().as_bytes()));
        record.extend(pax_record("minio.metadata.x-amz-replication-status", b"REPLICA"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"GOVERNANCE"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON"));
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object.txt", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2030-01-01T00:00:00Z".to_string()),
        ]);
        let mut opts = ObjectOptions::default();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        let authorization =
            apply_extract_entry_pax_extensions(&mut entry, "bucket", "object.txt", &state, &mut metadata, &mut opts)
                .await
                .unwrap();

        assert_eq!(metadata.get("content-type").map(String::as_str), Some("text/plain"));
        assert_eq!(metadata.get("owner").map(String::as_str), Some("alice"));
        assert_eq!(metadata.get("project").map(String::as_str), Some("alpha-demo"));
        assert_eq!(metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str), Some("classification=public"));
        assert!(!metadata.contains_key("x-amz-tagging"));
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("GOVERNANCE"));
        assert_eq!(
            metadata.get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER).map(String::as_str),
            Some("2099-01-01T00:00:00Z")
        );
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str), Some("ON"));
        assert!(metadata.contains_key("x-rustfs-internal-objectlock-legalhold-timestamp"));
        assert!(metadata.contains_key("x-minio-internal-objectlock-legalhold-timestamp"));
        assert_eq!(metadata.get(AMZ_BUCKET_REPLICATION_STATUS).map(String::as_str), Some("REPLICA"));
        assert_eq!(opts.version_id.as_deref(), Some("00000000-0000-0000-0000-000000000000"));
        assert!(authorization.object_lock_mode.is_some());
        assert!(authorization.object_lock_retain_until_date.is_some());
        assert!(authorization.object_lock_legal_hold_status.is_some());
        assert!(opts.version_id.is_some());
        assert!(authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS));
    }

    #[tokio::test]
    async fn snowball_pax_rejects_legal_hold_without_bucket_object_lock() {
        let record = pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::new();

        let err = apply_extract_entry_pax_extensions(
            &mut entry,
            "bucket",
            "object",
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            &mut metadata,
            &mut ObjectOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!metadata.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER));
    }

    #[tokio::test]
    async fn snowball_pax_rejects_invalid_legal_hold_status() {
        let record = pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"INVALID");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::new();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        let err = apply_extract_entry_pax_extensions(
            &mut entry,
            "bucket",
            "object",
            &state,
            &mut metadata,
            &mut ObjectOptions::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
        assert!(!metadata.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER));
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
            "req-cold-fill",
            None,
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
            |_| None,
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
        // The request is intentionally held behind the first producer while a
        // 1.3 MiB replacement write changes its generation. Disable dynamic
        // sizing for this test so runner I/O load cannot consume the five-second
        // production minimum before the behavior under test is released.
        let usecase = DefaultObjectUsecase::with_context_and_get_object_timeout_policy(
            Some(context),
            GetObjectTimeoutPolicy {
                enable_dynamic_timeout: false,
                ..GetObjectTimeoutPolicy::default()
            },
        );
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
        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::InvalidData)
        );
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
        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn memory_blob_preserves_exact_remaining_length() {
        let blob = DefaultObjectUsecase::build_memory_bytes_blob(
            Bytes::from_static(b"hello"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            GetObjectBodyLifecycle::disabled(),
        );

        assert_eq!(blob.remaining_length().exact(), Some(5));
    }

    #[test]
    #[serial_test::serial]
    fn memory_blob_once_fast_path_holds_guard_until_bytes_drop() {
        temp_env::with_var(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, Some("true"), || {
            let initial = GetObjectGuard::concurrent_count();
            let guard = GetObjectGuard::new();
            assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

            let blob = DefaultObjectUsecase::build_memory_bytes_blob(
                Bytes::from_static(b"hello"),
                5,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                GetObjectBodyLifecycle::tracked(guard),
            );
            let mut body = s3s::Body::from(blob);
            let bytes = body.take_bytes().expect("opt-in exact memory body should stay on Body::Once");

            assert_eq!(bytes, Bytes::from_static(b"hello"));
            assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);
            drop(bytes);
            assert_eq!(GetObjectGuard::concurrent_count(), initial);
        });
    }

    #[test]
    #[serial_test::serial]
    fn memory_blob_once_fast_path_rejects_length_mismatch() {
        temp_env::with_var(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, Some("true"), || {
            let blob = DefaultObjectUsecase::build_memory_bytes_blob(
                Bytes::from_static(b"test"),
                5,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                GetObjectBodyLifecycle::disabled(),
            );
            let mut body = s3s::Body::from(blob);

            assert!(body.take_bytes().is_none(), "mismatched memory body must keep the guarded stream path");
        });
    }

    #[tokio::test]
    async fn get_object_streaming_reader_times_out_when_body_stalls() {
        let reader = GetObjectStreamingReader::new(
            PendingReader,
            "test-bucket",
            "stalled-object",
            "req-stalled-stream",
            None,
            1,
            Duration::from_millis(1),
            GetObjectBodyLifecycle::disabled(),
            None,
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
    async fn get_object_streaming_reader_fails_closed_without_active_reader() {
        use tokio::io::AsyncReadExt;

        let mut reader = GetObjectStreamingReader::new(
            cursor_reader(b"x"),
            "test-bucket",
            "missing-reader-object",
            "req-missing-reader",
            None,
            1,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            None,
        );
        reader.inner.take();

        let err = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("an impossible missing active reader must fail closed");

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(err.to_string(), "get object streaming reader lost its active read outside resume");
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
            "req-complete-stream",
            None,
            5,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
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
            "req-short-eof",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("short body under a larger Content-Length must fail the stream");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        let incomplete_body = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustfs_rio::IncompleteBody>())
            .expect("short eof should include remaining bytes as IncompleteBody");
        assert_eq!(incomplete_body.remaining, 5);
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
            "req-dropped-stream",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
        );
        drop(reader);

        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    // Emits all of `data`, then either the injected error or a clean EOF. Drives
    // the mid-stream resume state machine through its typed-error and
    // premature-EOF triggers without a store.
    struct FailAtEndReader {
        data: std::io::Cursor<Vec<u8>>,
        error: Option<std::io::Error>,
    }

    impl FailAtEndReader {
        fn new(data: &[u8], error: Option<std::io::Error>) -> Self {
            Self {
                data: std::io::Cursor::new(data.to_vec()),
                error,
            }
        }
    }

    impl AsyncRead for FailAtEndReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source_len = self.data.get_ref().len();
            if position >= source_len {
                return match self.error.take() {
                    Some(error) => Poll::Ready(Err(error)),
                    None => Poll::Ready(Ok(())),
                };
            }
            let want = buf.remaining().min(source_len - position);
            if want == 0 {
                return Poll::Ready(Ok(()));
            }
            buf.put_slice(&self.data.get_ref()[position..position + want]);
            self.data.set_position(u64::try_from(position + want).unwrap_or(u64::MAX));
            Poll::Ready(Ok(()))
        }
    }

    fn relocation_read_error() -> std::io::Error {
        std::io::Error::other(StorageError::FileNotFound)
    }

    fn counting_resume_control(
        reopen_count: Arc<AtomicUsize>,
        mut reopen: impl FnMut(usize) -> Result<FailAtEndReader, GetObjectResumeFailure> + Send + Sync + 'static,
    ) -> GetObjectResumeControl<FailAtEndReader> {
        let reopen: GetObjectReopen<FailAtEndReader> = Box::new(move |emitted| {
            reopen_count.fetch_add(1, Ordering::Relaxed);
            let outcome = reopen(emitted);
            Box::pin(async move { outcome })
        });
        GetObjectResumeControl::new(
            reopen,
            RetryTimer::new(
                GET_OBJECT_RESUME_MAX_ATTEMPTS,
                Duration::from_millis(1),
                Duration::from_millis(2),
                rustfs_utils::retry::NO_JITTER,
                0,
            ),
        )
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resumes_after_relocation_error() {
        use tokio::io::AsyncReadExt;

        // Every typed relocation variant the codec read path can surface
        // mid-body must arm the resume flow.
        for variant in [
            StorageError::FileNotFound,
            StorageError::ObjectNotFound("test-bucket".to_string(), "relocated-object".to_string()),
            StorageError::InsufficientReadQuorum("test-bucket".to_string(), "relocated-object".to_string()),
            StorageError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "relocated shard disappeared")),
        ] {
            let reopen_count = Arc::new(AtomicUsize::new(0));
            let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| {
                assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
                Ok(FailAtEndReader::new(b"world", None))
            });
            let mut reader = GetObjectStreamingReader::new(
                FailAtEndReader::new(b"hello ", Some(std::io::Error::other(variant))),
                "test-bucket",
                "relocated-object",
                "req-resume-typed-error",
                None,
                11,
                Duration::ZERO,
                GetObjectBodyLifecycle::disabled(),
                Some(control),
            );
            let mut out = Vec::new();
            reader
                .read_to_end(&mut out)
                .await
                .expect("a resumed body must deliver the full committed content");

            assert_eq!(out, b"hello world");
            assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn get_object_streaming_reader_releases_failed_disk_permit_before_reopen() {
        use tokio::io::AsyncReadExt;

        let manager = Arc::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 0));
        let initial_permit = match manager
            .admit_disk_read(Duration::ZERO)
            .await
            .expect("test disk admission must remain open")
        {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("initial read must hold the only primary permit, got {other:?}"),
        };
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let reopen: GetObjectReopen<DiskReadPermitReader<FailAtEndReader>> = Box::new({
            let manager = Arc::clone(&manager);
            let reopen_count = Arc::clone(&reopen_count);
            move |emitted| {
                assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
                reopen_count.fetch_add(1, Ordering::Relaxed);
                let manager = Arc::clone(&manager);
                Box::pin(async move {
                    match manager
                        .admit_disk_read(Duration::from_millis(1))
                        .await
                        .map_err(|_| GetObjectResumeFailure::Fatal)?
                    {
                        DiskReadAdmission::Primary(permit) => {
                            Ok(DiskReadPermitReader::new(FailAtEndReader::new(b"world", None), permit.into()))
                        }
                        _ => Err(GetObjectResumeFailure::Retryable),
                    }
                })
            }
        });
        let control = GetObjectResumeControl::new(
            reopen,
            RetryTimer::new(
                GET_OBJECT_RESUME_MAX_ATTEMPTS,
                Duration::from_millis(1),
                Duration::from_millis(2),
                rustfs_utils::retry::NO_JITTER,
                0,
            ),
        );
        let initial_reader =
            DiskReadPermitReader::new(FailAtEndReader::new(b"hello ", Some(relocation_read_error())), initial_permit.into());
        let mut reader = GetObjectStreamingReader::new(
            initial_reader,
            "test-bucket",
            "relocated-object",
            "req-resume-single-permit",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();

        reader
            .read_to_end(&mut out)
            .await
            .expect("resume must not wait on the failed reader's permit");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            0,
            "the replacement reader must release its permit at EOF"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resumes_after_premature_eof() {
        use tokio::io::AsyncReadExt;

        // The legacy duplex read path surfaces vanished object data as a clean
        // EOF before the committed length; the resume flow must treat it like
        // the typed relocation error.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| {
            assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
            Ok(FailAtEndReader::new(b"world", None))
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", None),
            "test-bucket",
            "truncated-object",
            "req-resume-short-eof",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("a resumed body must deliver the full committed content");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_clean_eof_does_not_resume() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("a cleanly completed body must never reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello world", None),
            "test-bucket",
            "complete-object",
            "req-resume-clean-eof",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("complete body must read");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_fatal_resume_failure_returns_original_error() {
        use tokio::io::AsyncReadExt;

        // A fatal reopen failure (the reopened object is a different version)
        // must surface the original trigger error after exactly one attempt,
        // with only the originally emitted prefix delivered.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| Err(GetObjectResumeFailure::Fatal));
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "replaced-object",
            "req-resume-fatal",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a fatal resume failure must fail the body with the original error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the original typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello ");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            1,
            "a fatal failure must short-circuit the retry budget"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_exhausts_resume_budget() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| Err(GetObjectResumeFailure::Retryable));
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "vanished-object",
            "req-resume-budget",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("an exhausted resume budget must fail the body with the original error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the original typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello ");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "resume must stop after its reopen budget"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_rearms_resume_after_a_successful_resume() {
        use tokio::io::AsyncReadExt;

        // A successful resume restores the armed state: a second mid-stream
        // relocation error on the replacement stream must resume again.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| match emitted {
            6 => Ok(FailAtEndReader::new(b"wo", Some(relocation_read_error()))),
            8 => Ok(FailAtEndReader::new(b"rld", None)),
            other => panic!("unexpected reopen offset {other}"),
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "twice-relocated-object",
            "req-resume-rearm",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("a re-armed resume must deliver the full committed content");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resume_budget_is_per_body() {
        use tokio::io::AsyncReadExt;

        // The retry budget is consumed across the whole body, not reset per
        // error: one successful resume plus two failed reopens exhausts it.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| match emitted {
            6 => Ok(FailAtEndReader::new(b"wo", Some(relocation_read_error()))),
            _ => Err(GetObjectResumeFailure::Retryable),
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "budget-shared-object",
            "req-resume-budget-per-body",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("the shared budget must exhaust and surface the latest trigger error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello wo");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "the budget spans every resume of the same body"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_non_relocation_error_passes_through() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("a non-relocation read error must not reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt"))),
            "test-bucket",
            "corrupt-object",
            "req-resume-passthrough",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a non-relocation error must fail the body unchanged");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(out, b"hello ");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_error_after_full_delivery_does_not_resume() {
        use tokio::io::AsyncReadExt;

        // The committed length is already delivered when the inner stream
        // errors, so the error must keep the existing fail-loud behavior
        // instead of arming a resume.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("an error after full delivery must not reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello world", Some(relocation_read_error())),
            "test-bucket",
            "fully-delivered-object",
            "req-resume-after-full",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a post-completion inner error still surfaces instead of being swallowed");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the inner typed error, got: {err}"
        );
        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn get_object_resume_identity_requires_same_version() {
        let version_id = Uuid::from_u128(0x1234);
        let mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let later_mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp");
        let identity = GetObjectResumeIdentity {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            range_dependent_size: false,
        };
        let info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        assert!(identity.matches(&info, 0));
        assert!(identity.matches(&info, 6), "a plain read reports the range-invariant oi.size");
        // Rebalance regenerates data_dir for the same version: identity must
        // still match so a relocated read can resume.
        assert!(identity.matches(
            &ObjectInfo {
                data_dir: Some(Uuid::from_u128(0xbeef)),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                version_id: Some(Uuid::from_u128(0x5678)),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                version_id: None,
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                mod_time: Some(later_mod_time),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                size: 12,
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                etag: Some("etag-b".to_string()),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(&ObjectInfo { etag: None, ..info }, 0));
    }

    #[test]
    fn get_object_resume_identity_normalizes_range_dependent_size() {
        // Encrypted and compressed reads report the per-read delivered length
        // as object_info.size, so the reopened subrange reports size - emitted
        // for the same version.
        let version_id = Uuid::from_u128(0x1234);
        let mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let identity = GetObjectResumeIdentity {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            range_dependent_size: true,
        };
        let reopened = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 5,
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        assert!(identity.matches(&reopened, 6), "the reopened subrange reports size - emitted");
        assert!(identity.matches(
            &ObjectInfo {
                size: 11,
                ..reopened.clone()
            },
            0
        ));
        assert!(
            !identity.matches(
                &ObjectInfo {
                    size: 11,
                    ..reopened.clone()
                },
                6
            ),
            "an unshrunk range-dependent size after emitted bytes is a different object"
        );
        assert!(!identity.matches(&ObjectInfo { size: 4, ..reopened }, 6));
    }

    #[test]
    fn get_object_resume_range_offsets() {
        // A full-object read that emitted nothing reopens range-free so the
        // replacement stream keeps the codec fast path.
        assert!(GetObjectResumeContext::resume_range(0, -1, 0).is_none());

        // Mid-stream full-object resume: open-ended from the emitted offset.
        let range = GetObjectResumeContext::resume_range(0, -1, 6).expect("a mid-stream resume must carry a range");
        assert!(!range.is_suffix_length);
        assert_eq!((range.start, range.end), (6, -1));

        // Ranged reads resume at absolute offsets with the committed end
        // preserved (suffix ranges and partNumber GETs are resolved to absolute
        // offsets before these values are captured).
        let range = GetObjectResumeContext::resume_range(10, 19, 0).expect("a ranged resume must carry a range");
        assert!(!range.is_suffix_length);
        assert_eq!((range.start, range.end), (10, 19));
        let range = GetObjectResumeContext::resume_range(10, 19, 5).expect("a ranged resume must carry a range");
        assert_eq!((range.start, range.end), (15, 19));
    }

    async fn real_get_resume_test_context() -> (Vec<std::path::PathBuf>, Arc<ECStore>, Arc<AppContext>) {
        let (disk_paths, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("resume wiring tests require an ambient AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        (disk_paths, store, context)
    }

    // Uploads a real multipart object through the store and returns the
    // concatenated body, so resume wiring tests can verify byte-exact delivery
    // against on-disk part files.
    async fn put_real_multipart_object(
        store: &Arc<ECStore>,
        bucket: &str,
        object: &str,
        part_size: usize,
        part_count: usize,
        fill: u8,
    ) -> Vec<u8> {
        use crate::app::storage_api::multipart_usecase::contract::multipart::{CompletePart, MultipartOperations as _};

        let upload = store
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart upload");
        let mut parts = Vec::new();
        let mut body = Vec::with_capacity(part_size * part_count);
        for part_id in 1..=part_count {
            let part_fill = fill.wrapping_add(u8::try_from(part_id - 1).expect("test part index must fit u8"));
            let part_body = vec![part_fill; part_size];
            body.extend_from_slice(&part_body);
            let mut reader = PutObjReader::from_vec(part_body);
            let part = store
                .put_object_part(bucket, object, &upload.upload_id, part_id, &mut reader, &ObjectOptions::default())
                .await
                .expect("upload multipart part");
            parts.push(CompletePart {
                part_num: part_id,
                etag: part.etag,
                ..Default::default()
            });
        }
        store
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, parts, &ObjectOptions::default())
            .await
            .expect("complete multipart upload");
        body
    }

    // Deletes the given part files from every version data dir on every disk,
    // simulating rebalance removing the object data while xl.meta stays
    // readable. Returns the number of files removed.
    fn delete_object_part_shards(disk_paths: &[std::path::PathBuf], bucket: &str, object: &str, part_numbers: &[usize]) -> usize {
        let mut deleted = 0;
        for disk_path in disk_paths {
            let object_dir = disk_path.join(bucket).join(object);
            for entry in std::fs::read_dir(&object_dir).expect("object directory must exist") {
                let entry = entry.expect("object directory entry must read");
                if !entry.file_type().expect("entry file type must read").is_dir() {
                    continue;
                }
                for part_number in part_numbers {
                    let part_file = entry.path().join(format!("part.{part_number}"));
                    if part_file.exists() {
                        std::fs::remove_file(&part_file).expect("part shard must be removable");
                        deleted += 1;
                    }
                }
            }
        }
        deleted
    }

    // The surfaced mid-stream failure must be the original trigger: a typed
    // relocation StorageError from the codec read path, or an IncompleteBody
    // (UnexpectedEof) from the duplex path. The resume flow must never
    // fabricate a different error.
    fn assert_original_trigger_error(error: &(dyn std::error::Error + Send + Sync + 'static)) {
        let Some(io_error) = error.downcast_ref::<std::io::Error>() else {
            panic!("body error must be an io::Error, got: {error}");
        };
        let is_trigger = io_error.kind() == std::io::ErrorKind::UnexpectedEof || is_object_relocation_error(io_error);
        assert!(is_trigger, "body error must be the original relocation trigger, got: {error}");
    }

    #[tokio::test]
    #[serial_test::serial]
    // SAFETY: the test mutates one process env var before any use; nextest runs
    // each test in its own process, so the mutation cannot race another test.
    #[allow(unsafe_code)]
    async fn execute_get_object_resume_exhausts_budget_when_object_data_vanishes() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        // The resume phase runs inside the body stall budget (default 10s),
        // and three real reopen attempts against missing shards approach it on
        // loaded CI disks; widen the budget so this test asserts the resume
        // outcome instead of racing the stall timer.
        unsafe { std::env::set_var(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, "120") };

        let (disk_paths, store, context) = real_get_resume_test_context().await;
        let bucket = format!("resume-vanish-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create resume failure-path bucket");
        let part_size = 6 * 1024 * 1024;
        let body = put_real_multipart_object(&store, &bucket, object, part_size, 3, 0xAA).await;

        // Remove the part.2/part.3 shards on every disk before the GET starts,
        // so no file descriptor for them can exist: the stream must fail at the
        // part-2 boundary, and every reopen resolves intact metadata whose data
        // is gone, so the whole resume budget burns down.
        let deleted = delete_object_part_shards(&disk_paths, &bucket, object, &[2, 3]);
        assert_eq!(deleted, disk_paths.len() * 2);

        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("resume failure-path GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let attempts_before = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed);
        let mut response = usecase
            .execute_get_object(build_request(input, Method::GET))
            .await
            .expect("the GET commits a response; the body fails mid-stream");
        let mut response_body = response.output.body.take().expect("GET response must include a body");
        let mut collected = Vec::new();
        let mut stream_error = None;
        while let Some(chunk) = response_body.next().await {
            match chunk {
                Ok(bytes) => collected.extend_from_slice(&bytes),
                Err(error) => {
                    stream_error = Some(error);
                    break;
                }
            }
        }

        assert_eq!(
            collected,
            &body[..part_size],
            "only the first part can be delivered before the object data vanishes"
        );
        assert_original_trigger_error(
            stream_error
                .as_deref()
                .expect("the body stream must fail at the missing part"),
        );
        let attempts = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before;
        assert_eq!(
            attempts,
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "resume must exhaust its reopen budget before failing"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_get_object_resumes_from_relocated_pool_without_splicing_body() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (_temp_dir, pool_disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("multi-pool resume test requires an ambient AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("resume-relocate-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multi-pool resume bucket");
        let part_size = 24 * 1024 * 1024;
        let body = put_real_multipart_object(&store, &bucket, object, part_size, 3, 0xA5).await;
        let upload_pool = pool_disk_paths
            .iter()
            .position(|paths| {
                paths
                    .iter()
                    .any(|path| path.join(&bucket).join(object).join("xl.meta").is_file())
            })
            .expect("multipart object must be placed in one source pool");
        if upload_pool != 0 {
            for (source_disk, target_disk) in pool_disk_paths[upload_pool].iter().zip(&pool_disk_paths[0]) {
                std::fs::rename(source_disk.join(&bucket).join(object), target_disk.join(&bucket).join(object))
                    .expect("normalize the test object into the old pool");
            }
        }
        let source_pool = 0;
        let target_pool = 1;

        // Stage the relocated data before the GET, but publish xl.meta only
        // after the initial reader opens. This mirrors rebalance's data-first,
        // metadata-last ordering and guarantees the first reader uses the
        // source pool while its later parts are already unavailable.
        for (source_disk, target_disk) in pool_disk_paths[source_pool].iter().zip(&pool_disk_paths[target_pool]) {
            let source_dir = source_disk.join(&bucket).join(object);
            let target_dir = target_disk.join(&bucket).join(object);
            std::fs::create_dir_all(&target_dir).expect("create relocated target object directory");
            for entry in std::fs::read_dir(&source_dir).expect("read source object directory") {
                let entry = entry.expect("read source object entry");
                if !entry.file_type().expect("read source object entry type").is_dir() {
                    continue;
                }
                let target_entry = target_dir.join(entry.file_name());
                std::fs::create_dir_all(&target_entry).expect("create relocated target data directory");
                for child in std::fs::read_dir(entry.path()).expect("read source object data directory") {
                    let child = child.expect("read source object data entry");
                    std::fs::copy(child.path(), target_entry.join(child.file_name())).expect("copy relocated object data entry");
                }
            }
        }
        let deleted = delete_object_part_shards(&pool_disk_paths[source_pool], &bucket, object, &[2, 3]);
        assert_eq!(deleted, pool_disk_paths[source_pool].len() * 2);

        let input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("multi-pool resume GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let mut response = usecase
            .execute_get_object(build_request(input, Method::GET))
            .await
            .expect("multi-pool GET must commit its response");
        let mut response_body = response
            .output
            .body
            .take()
            .expect("multi-pool GET response must include a body");
        for (source_disk, target_disk) in pool_disk_paths[source_pool].iter().zip(&pool_disk_paths[target_pool]) {
            let source_meta = source_disk.join(&bucket).join(object).join("xl.meta");
            std::fs::copy(&source_meta, target_disk.join(&bucket).join(object).join("xl.meta"))
                .expect("publish relocated object metadata");
            std::fs::remove_file(source_meta).expect("remove relocated source object metadata");
        }
        store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("the relocated object must resolve from the target pool");

        let attempts_before = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed);
        let mut collected = Vec::new();
        while let Some(chunk) = response_body.next().await {
            match chunk {
                Ok(chunk) => collected.extend_from_slice(&chunk),
                Err(err) => panic!(
                    "relocated GET from pool {source_pool} must resume from pool {target_pool} after {} attempts: {err:?}",
                    GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before
                ),
            }
        }

        assert_eq!(collected, body, "resumed production GET must preserve the complete body byte-for-byte");
        assert_eq!(
            GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before,
            1,
            "the relocated body must reopen exactly once"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_reopen_rejects_a_replaced_object_version() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use tokio::io::AsyncReadExt as _;

        let (_disk_paths, store, _context) = real_get_resume_test_context().await;
        let bucket = format!("resume-identity-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create resume identity bucket");
        let body = vec![0xAA; 1024 * 1024];
        put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("read the committed object metadata");

        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            &bucket,
            object,
            ObjectOptions::default(),
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );

        // Positive control: the same version reopens and streams the body.
        let manager = get_concurrency_manager();
        let permits_before = manager.io_queue_status().permits_in_use;
        let mut reader = ctx.reopen(0).await.expect("reopening the same version must succeed");
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            permits_before + 1,
            "the resumed stream must hold disk-read admission like the initial read"
        );
        let mut reopened_body = Vec::new();
        reader
            .read_to_end(&mut reopened_body)
            .await
            .expect("the reopened reader must stream the body");
        assert_eq!(reopened_body, body);
        // The reopened reader holds the object read lock; drop it before the
        // delete below requests the write lock.
        drop(reader);
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            permits_before,
            "dropping the resumed stream must release its disk-read admission"
        );

        // A nonzero-offset reopen must splice the remaining bytes exactly.
        let mut reader = ctx.reopen(1024).await.expect("reopening at a nonzero offset must succeed");
        let mut tail = Vec::new();
        reader
            .read_to_end(&mut tail)
            .await
            .expect("the offset reader must stream the remaining body");
        assert_eq!(tail, body[1024..], "the resumed stream must continue from the emitted offset exactly");
        drop(reader);

        // Delete and re-PUT the key, then the stale context must refuse to
        // splice the replacement version into the committed response.
        store
            .delete_object(&bucket, object, ObjectOptions::default())
            .await
            .expect("delete the original object");
        let replacement_body = vec![0xBB; 2 * 1024 * 1024];
        put_real_cold_fill_object(&store, &bucket, object, &replacement_body).await;
        let result = ctx.reopen(0).await;
        assert!(
            matches!(result, Err(GetObjectResumeFailure::Fatal)),
            "reopening a replaced version must fail closed"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_context_pins_latest_read_to_resolved_version() {
        let (_disk_paths, store, _context) = real_get_resume_test_context().await;
        let resolved_version = Uuid::new_v4();
        let info = ObjectInfo {
            version_id: Some(resolved_version),
            ..Default::default()
        };

        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );
        assert_eq!(
            ctx.opts.version_id,
            Some(resolved_version.to_string()),
            "latest GET resume must reopen the initially resolved version, not the moving latest"
        );

        let explicit_version = Uuid::new_v4().to_string();
        let explicit_opts = ObjectOptions {
            version_id: Some(explicit_version.clone()),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            explicit_opts,
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );
        assert_eq!(
            ctx.opts.version_id.as_deref(),
            Some(explicit_version.as_str()),
            "an explicit request version must stay authoritative"
        );

        let unversioned_info = ObjectInfo::default();
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &unversioned_info,
            0,
            -1,
        );
        assert_eq!(ctx.opts.version_id, None, "unversioned reads have no version to pin");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_context_redacts_ssec_headers_and_flags_range_dependent_size() {
        let (_disk_paths, store, _context) = real_get_resume_test_context().await;

        let mut request_headers = HeaderMap::new();
        request_headers.insert(SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request_headers.insert(SSEC_KEY_HEADER, HeaderValue::from_static("dGVzdC1rZXk="));
        request_headers.insert(SSEC_KEY_MD5_HEADER, HeaderValue::from_static("bWQ1"));
        request_headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=test"));
        request_headers.insert("x-amz-security-token", HeaderValue::from_static("session-token"));
        let store_headers = project_ssec_transport_headers(&request_headers);
        assert_eq!(store_headers.len(), 3, "only store-consumed SSE-C headers are forwarded");
        assert!(store_headers.values().all(HeaderValue::is_sensitive));
        assert!(store_headers.get(http::header::AUTHORIZATION).is_none());
        assert!(store_headers.get("x-amz-security-token").is_none());
        assert!(!format!("{store_headers:?}").contains("dGVzdC1rZXk="));
        let plain_info = ObjectInfo {
            size: 11,
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &request_headers,
            &plain_info,
            0,
            -1,
        );
        for name in [SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER] {
            let value = ctx.ssec_headers.get(name).expect("the SSE-C trio is retained");
            assert!(value.is_sensitive(), "store spans record headers at debug; {name} must be redacted there");
        }
        assert_eq!(
            ctx.ssec_headers.len(),
            3,
            "only the SSE-C trio may be retained; credential headers must never be replayed into store spans"
        );
        assert!(!ctx.identity.range_dependent_size, "plain reads report the range-invariant oi.size");

        let encrypted_info = ObjectInfo {
            size: 11,
            user_defined: Arc::new(
                [("x-amz-server-side-encryption".to_string(), "aws:kms".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &encrypted_info,
            0,
            -1,
        );
        assert!(ctx.identity.range_dependent_size, "encrypted reads report the per-read delivered length");

        let compressed_info = ObjectInfo {
            size: 11,
            user_defined: Arc::new(
                [("x-rustfs-internal-compression".to_string(), "snappy".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &compressed_info,
            0,
            -1,
        );
        assert!(ctx.identity.range_dependent_size, "compressed reads report the per-read delivered length");
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
            "req-large-object",
            None,
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
            |_| None,
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
            "req-large-encrypted-object",
            None,
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
            |_| None,
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
            "req-direct-memory-object",
            None,
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
            |_| panic!("a buffered body must not initialize streaming resume state"),
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
            "req-cached-object",
            None,
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
            |_| panic!("a cache hit must not initialize streaming resume state"),
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
            "req-rejects-size-mismatch-fill",
            None,
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
            |_| None,
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
            "req-cache-fill-first",
            None,
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
            |_| None,
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
            "req-cache-fill-second",
            None,
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
            |_| None,
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
            "req-rejects-buffered-size-mismatch",
            None,
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
            |_| None,
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
            "req-hook-served",
            None,
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
            |_| None,
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
            "req-hook-missed",
            None,
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
            |_| None,
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
            "req-materialize-first",
            None,
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
            |_| None,
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
            "req-materialize-second",
            None,
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
            |_| None,
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
            "req-materialize-mismatch",
            None,
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
            |_| None,
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
            "req-materialize-short",
            None,
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
            |_| None,
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
            "req-materialize-partial",
            None,
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
            |_| None,
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
            "req-short-buffered-object",
            None,
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
            |_| None,
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
            "req-exact-buffered-object",
            None,
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
            |_| panic!("an exact-length buffered body must not initialize streaming resume state"),
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
            "req-materialize-too-large",
            None,
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
            |_| None,
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
            "req-small-plain-object",
            None,
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
            |_| None,
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
            MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(256 * 1024, 2 * MI_B as i64, GetObjectStreamStrategy::Standard),
            MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
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
    async fn get_object_reader_stream_bounds_read_buffer_to_remaining() {
        struct RecordingReader {
            data: &'static [u8],
            pos: usize,
            observed_remaining: Arc<Mutex<Vec<usize>>>,
        }

        impl AsyncRead for RecordingReader {
            fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                let requested = buf.remaining();
                self.observed_remaining
                    .lock()
                    .expect("observed buffer sizes should not poison")
                    .push(requested);
                let available = self.data.len().saturating_sub(self.pos);
                let to_copy = requested.min(available);
                if to_copy > 0 {
                    let end = self.pos + to_copy;
                    buf.put_slice(&self.data[self.pos..end]);
                    self.pos = end;
                }
                Poll::Ready(Ok(()))
            }
        }

        let observed_remaining = Arc::new(Mutex::new(Vec::new()));
        let stream = GetObjectReaderStream::new(
            RecordingReader {
                data: b"hello",
                pos: 0,
                observed_remaining: Arc::clone(&observed_remaining),
            },
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
            .expect("reader stream should read exact payload");
        assert_eq!(chunks, vec![Bytes::from_static(b"hello")]);
        assert_eq!(
            *observed_remaining.lock().expect("observed buffer sizes should not poison"),
            vec![5],
            "stream should not ask the reader for more bytes than the response has left"
        );
    }

    #[tokio::test]
    async fn get_object_reader_stream_bounds_multi_chunk_final_read() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(vec![b'a'; 66]),
            64,
            65,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let chunks = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("reader stream should ignore bytes past declared length");
        let chunk_lengths = chunks.iter().map(Bytes::len).collect::<Vec<_>>();
        let body = chunks.into_iter().fold(Vec::new(), |mut acc, chunk| {
            acc.extend_from_slice(&chunk);
            acc
        });

        assert_eq!(chunk_lengths, vec![64, 1]);
        assert_eq!(body, vec![b'a'; 65]);
    }

    // Serial with the capture test below: both drive the same short-EOF log
    // callsite, and `tracing` caches callsite interest process-wide. Running
    // this one concurrently on a thread with no subscriber re-caches that
    // callsite as "never interested" and blinds the capture.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_reader_stream_errors_on_short_eof() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"he".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let err = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect_err("short reader should fail the streaming body");

        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::UnexpectedEof)
        );
    }

    /// Collects the structured fields of every event emitted while installed,
    /// so a test can assert what an operator would actually read in the log
    /// rather than only that an error value was returned.
    type CapturedFieldMap = std::collections::HashMap<String, String>;
    type CapturedEventLog = Arc<Mutex<Vec<CapturedFieldMap>>>;

    struct CapturedEvents(CapturedEventLog);

    struct CapturedFields(CapturedFieldMap);

    impl tracing::field::Visit for CapturedFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedEvents {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = CapturedFields(CapturedFieldMap::new());
            event.record(&mut fields);
            self.0.lock().expect("captured events should not poison").push(fields.0);
        }
    }

    fn capture_events() -> (CapturedEventLog, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::{Registry, prelude::*};

        let captured = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(CapturedEvents(Arc::clone(&captured)));
        let guard = tracing::subscriber::set_default(subscriber);
        // `tracing` caches per-callsite interest process-wide, so a subscriber
        // installed by a test running in parallel can leave the log sites below
        // cached as "never interested" and this capture would silently see
        // nothing. Force the callsites to re-ask the subscriber we just
        // installed.
        tracing::callsite::rebuild_interest_cache();
        (captured, guard)
    }

    fn find_stream_body_event(captured: &CapturedEventLog, state: &str) -> CapturedFieldMap {
        let events = captured.lock().expect("captured events should not poison");
        events
            .iter()
            .find(|fields| fields.get("state").is_some_and(|value| value == state))
            .unwrap_or_else(|| {
                panic!(
                    "a `{state}` streaming body failure must be logged, not only counted in a metric. \
                     Captured {} event(s): {:?}",
                    events.len(),
                    events
                )
            })
            .clone()
    }

    /// rustfs#4784: a GET body that ends short of its committed Content-Length
    /// is the fault that breaks every downstream copier (replication, site
    /// replication, `rclone sync`), yet this layer only fed a metric counter —
    /// its log line was compiled out unless the `tracing-chunk-debug` feature
    /// was on, so operators saw nothing on the source side.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_reader_stream_short_eof_names_the_object() {
        let (captured, _guard) = capture_events();

        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"he".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        )
        .with_diagnostics("restic-paperless", "index/41b5a4c2344edb90", "req-reader-stream-short-eof");

        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect_err("short reader should fail the streaming body");

        let event = find_stream_body_event(&captured, "reader_stream_short_eof");
        assert_eq!(event.get("bucket").map(String::as_str), Some("restic-paperless"));
        assert_eq!(event.get("object").map(String::as_str), Some("index/41b5a4c2344edb90"));
        assert_eq!(event.get("request_id").map(String::as_str), Some("req-reader-stream-short-eof"));
        assert_eq!(event.get("expected").map(String::as_str), Some("5"));
        assert_eq!(event.get("emitted").map(String::as_str), Some("2"));
        assert_eq!(event.get("remaining").map(String::as_str), Some("3"));
    }

    /// The inner reader already logged mid-stream failures, but only under a
    /// request_id — which cannot be resolved back to an object once the request
    /// is gone. Without the identity the report in #4784 was unactionable.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_short_eof_names_the_object() {
        use tokio::io::AsyncReadExt;

        let (captured, _guard) = capture_events();

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "restic-paperless",
            "index/41b5a4c2344edb90",
            "req-streaming-short-eof",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(GetObjectGuard::new()),
            None,
        );

        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect_err("short body under a larger Content-Length must fail the stream");

        let event = find_stream_body_event(&captured, "short_eof");
        assert_eq!(event.get("bucket").map(String::as_str), Some("restic-paperless"));
        assert_eq!(event.get("object").map(String::as_str), Some("index/41b5a4c2344edb90"));
        assert_eq!(event.get("request_id").map(String::as_str), Some("req-streaming-short-eof"));
    }

    #[test]
    fn get_object_stream_failure_labels_are_low_cardinality() {
        assert_eq!(get_object_stream_failure_reason("short_eof"), GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF);
        assert_eq!(
            get_object_stream_failure_reason("timeout"),
            GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR
        );
        assert_eq!(
            get_object_stream_size_bucket(4 * 1024 * 1024),
            rustfs_io_metrics::GET_OBJECT_SIZE_BUCKET_GT_1_MIB
        );
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
    async fn build_get_object_output_context_returns_standard_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("cache-control".to_string(), "public, max-age=259200".to_string());
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
                Some(Bytes::new()),
                false,
                false,
                true,
                None,
                None,
                None,
                0,
                None,
                "req-output-content-disposition",
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
                |_| panic!("a buffered output must not initialize streaming resume state"),
            )
            .await
            .expect("get object output context");

        assert_eq!(context.output.cache_control.as_deref(), Some("public, max-age=259200"));
        assert_eq!(context.output.content_disposition.as_deref(), Some("attachment; filename=\"demo.png\""));
        assert!(
            !context
                .output
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.contains_key("cache-control"))
        );
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
    async fn compressed_delete_requests_restore_usage_baseline() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};

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
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(1_000),
            "single delete must subtract the logical accounting size"
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
            crate::app::storage_api::test::data_usage::get_bucket_usage_memory(&bucket).await,
            Some(0),
            "batch delete must subtract the committed logical accounting size"
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
            uses_durable_reservations: true,
        }
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

    #[test]
    fn quota_admission_allows_within_limit() {
        let result = map_quota_check_outcome("bucket", Ok(quota_result(true))).expect("an allowed result admits the write");

        assert_eq!(result.current_usage, Some(1024));
        assert_eq!(result.quota_limit, Some(2048));
        assert_eq!(result.operation_size, 512);
        assert_eq!(result.remaining, Some(512));
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
    fn legacy_archive_quota_rejects_cumulative_size_and_overflow() {
        let legacy = QuotaCheckResult {
            allowed: true,
            current_usage: Some(4),
            quota_limit: Some(5),
            operation_size: 0,
            remaining: Some(1),
            uses_durable_reservations: false,
        };
        assert!(ensure_legacy_archive_size_within_quota(&legacy, 2).is_err());
        assert!(ensure_legacy_archive_size_within_quota(&legacy, 1).is_ok());

        let maxed = QuotaCheckResult {
            current_usage: Some(u64::MAX),
            quota_limit: Some(u64::MAX),
            ..legacy
        };
        assert!(ensure_legacy_archive_size_within_quota(&maxed, 1).is_err());
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
