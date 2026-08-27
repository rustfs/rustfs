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

mod copy;
mod delete;
mod extract;
mod get;
mod head;
mod put;
mod restore;
mod shared;
#[cfg(test)]
mod test_support;

pub(crate) use self::copy::*;
#[cfg(test)]
pub(crate) use self::delete::*;
pub(crate) use self::extract::*;
pub(crate) use self::get::*;
use self::put::*;
pub(crate) use self::put::{guard_put_object_body_read_timeout, put_object_body_read_timeout};
pub(crate) use self::shared::*;
#[cfg(test)]
use self::test_support::*;

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

#[derive(Clone, Default)]
pub struct DefaultObjectUsecase {
    context: Option<Arc<AppContext>>,
    #[cfg(test)]
    get_object_timeout_policy: Option<GetObjectTimeoutPolicy>,
}

impl DefaultObjectUsecase {
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

    #[hotpath::measure(
        label = "rustfs::app::object_usecase::DefaultObjectUsecase::execute_put_object",
        impl_type = "DefaultObjectUsecase"
    )]
    #[hotpath::measure(
        label = "rustfs::app::object_usecase::DefaultObjectUsecase::execute_get_object",
        impl_type = "DefaultObjectUsecase"
    )]
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
