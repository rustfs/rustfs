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

mod app_adapters;
mod get_object_flow;
mod get_object_zero_copy;
mod put_object_extract;
mod put_object_flow;
mod types;
#[cfg(test)]
mod zero_copy_tests;
use self::app_adapters::*;
use self::get_object_flow::{GetObjectBootstrap, GetObjectFlowRuntime};
use self::types::*;

use crate::app::context::{AppContext, default_notify_interface, get_global_app_context};
use crate::capacity::capacity_manager::get_capacity_manager;
use crate::config::RustFSBufferConfig;
use crate::error::ApiError;
use crate::storage::access::{PostObjectRequestMarker, authorize_request, has_bypass_governance_header, req_info_mut};
use crate::storage::concurrency::{CachedGetObject, ConcurrencyManager, GetObjectGuard, get_concurrency_manager};
use crate::storage::ecfs::*;
use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use crate::storage::helper::OperationHelper;
use crate::storage::options::{
    copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime_with_object_name,
    filter_object_metadata, get_content_sha256_with_query, get_opts, normalize_content_encoding_for_storage, put_opts,
    validate_archive_content_encoding,
};
use crate::storage::s3_api::multipart::parse_list_parts_params;
use crate::storage::s3_api::{acl, restore, select};
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use crate::storage::*;
use bytes::Bytes;
use datafusion::arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::WriterBuilder as JsonWriterBuilder, json::writer::JsonArray,
};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use md5::Context as Md5Context;
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    lifecycle::{
        bucket_lifecycle_audit::LcEventSrc,
        bucket_lifecycle_ops::{RestoreRequestOps, enqueue_transition_immediate, post_restore_opts},
        lifecycle::{self, Lifecycle, TransitionOptions},
    },
    metadata::{BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG},
    metadata_sys,
    object_lock::{
        objectlock::{get_object_legalhold_meta, get_object_retention_meta},
        objectlock_sys::{
            BucketObjectLockSys, check_object_lock_for_deletion, check_retention_for_modification, is_retention_active,
        },
    },
    quota::QuotaOperation,
    replication::{
        DeletedObjectReplicationInfo, check_replicate_delete, get_must_replicate_options, must_replicate, schedule_replication,
        schedule_replication_delete,
    },
    tagging::{decode_tags, encode_tags},
    utils::serialize,
    versioning::VersioningApi,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::disk::{error::DiskError, error_reduce::is_all_buckets_not_found};
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{
    BucketOperations, BucketOptions, ChunkNativePutData, HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOperations, ObjectOptions,
    ObjectToDelete,
};
use rustfs_filemeta::{
    REPLICATE_INCOMING_DELETE, ReplicationStatusType, ReplicationType, RestoreStatusOps, VersionPurgeStatusType,
    parse_restore_obj_status,
};
use rustfs_io_metrics;
use rustfs_notify::EventArgsBuilder;
use rustfs_object_io::put::{
    is_post_object_sse_kms_requested, is_put_object_extract_requested, is_sse_kms_requested, resolve_put_body_size,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_rio::{CompressReader, EtagReader, HashReader, Reader, WarpReader};
use rustfs_s3_common::S3Operation;
use rustfs_s3select_api::query::{Context, Query};
use rustfs_s3select_query::get_global_db;
use rustfs_targets::EventName;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE, AMZ_WEBSITE_REDIRECT_LOCATION, CONTENT_TYPE,
    SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, SUFFIX_COMPRESSION_SIZE, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    headers::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_MODE_LOWER,
        AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS,
        AMZ_RESTORE_REQUEST_DATE, AMZ_STORAGE_CLASS, AMZ_TAG_COUNT,
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
use tokio_util::io::StreamReader;
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
            let _guard = DeadlockRequestGuard::new(Arc::clone(&detector), request_id.clone());
            // `_guard` is dropped at the end of this scope, which should unregister the request.
        }

        assert_eq!(detector.tracked_count(), 0);
    }
}
async fn maybe_enqueue_transition_immediate(obj_info: &ObjectInfo, src: LcEventSrc) {
    enqueue_transition_immediate(obj_info, src).await;
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
    let request_content_type = content_type.as_ref().map(ToString::to_string).or_else(|| {
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    });
    let request_content_encoding = content_encoding.as_ref().map(ToString::to_string).or_else(|| {
        headers
            .get("content-encoding")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    });
    validate_archive_content_encoding(object_name, request_content_type.as_deref(), request_content_encoding.as_deref())?;

    if let Some(cache_control) = cache_control {
        metadata.insert("cache-control".to_string(), cache_control.to_string());
    }
    if let Some(content_disposition) = content_disposition {
        metadata.insert("content-disposition".to_string(), content_disposition.to_string());
    }
    if let Some(content_encoding) = content_encoding
        && let Some(normalized_content_encoding) = normalize_content_encoding_for_storage(&content_encoding)
    {
        metadata.insert("content-encoding".to_string(), normalized_content_encoding);
    }
    if let Some(content_language) = content_language {
        metadata.insert("content-language".to_string(), content_language.to_string());
    }
    if let Some(content_type) = content_type {
        metadata.insert(CONTENT_TYPE.to_string(), content_type.to_string());
    }
    if let Some(expires) = expires {
        let mut formatted = Vec::new();
        expires
            .format(TimestampFormat::HttpDate, &mut formatted)
            .map_err(|e| ApiError::from(StorageError::other(format!("Invalid expires timestamp: {e}"))))?;
        metadata.insert("expires".to_string(), String::from_utf8_lossy(&formatted).into_owned());
    }
    if let Some(website_redirect_location) = website_redirect_location {
        metadata.insert(AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), website_redirect_location.to_string());
    }
    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_string());
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storage_class.as_str().to_string());
    }

    extract_metadata_from_mime_with_object_name(headers, metadata, true, Some(object_name));
    Ok(())
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

const MAXIMUM_RETENTION_DAYS: i32 = 36_500;
const MAXIMUM_RETENTION_YEARS: i32 = 100;

fn invalid_object_lock_configuration(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::MalformedXML, message.into())
}

fn invalid_retention_period(message: impl Into<String>) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom("InvalidRetentionPeriod".into()), message.into());
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

fn validate_default_retention_configuration(default_retention: &DefaultRetention) -> S3Result<()> {
    let Some(mode) = default_retention.mode.as_ref() else {
        return Err(invalid_object_lock_configuration("retention mode must be specified"));
    };

    match mode.as_str() {
        ObjectLockRetentionMode::COMPLIANCE | ObjectLockRetentionMode::GOVERNANCE => {}
        _ => {
            return Err(invalid_object_lock_configuration(format!("unknown retention mode {}", mode.as_str())));
        }
    }

    match (default_retention.days, default_retention.years) {
        (Some(days), None) => {
            if days <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Days'",
                ));
            }
            if days > MAXIMUM_RETENTION_DAYS {
                return Err(invalid_retention_period(format!("Default retention period too large for 'Days' {days}",)));
            }
        }
        (None, Some(years)) => {
            if years <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Years'",
                ));
            }
            if years > MAXIMUM_RETENTION_YEARS {
                return Err(invalid_retention_period(format!(
                    "Default retention period too large for 'Years' {years}",
                )));
            }
        }
        (Some(_), Some(_)) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified, not both"));
        }
        (None, None) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified"));
        }
    }

    Ok(())
}

fn validate_object_lock_configuration_input(input_cfg: &ObjectLockConfiguration) -> S3Result<()> {
    let enabled = input_cfg.object_lock_enabled.as_ref().map(ObjectLockEnabled::as_str);
    if enabled != Some(ObjectLockEnabled::ENABLED) {
        return Err(invalid_object_lock_configuration(
            "only 'Enabled' value is allowed to ObjectLockEnabled element",
        ));
    }

    if let Some(rule) = input_cfg.rule.as_ref() {
        let Some(default_retention) = rule.default_retention.as_ref() else {
            return Err(invalid_object_lock_configuration("Rule must include DefaultRetention"));
        };
        validate_default_retention_configuration(default_retention)?;
    }

    Ok(())
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

    fn spawn_cache_invalidation(bucket: String, key: String, version_id: Option<String>) {
        let manager = get_concurrency_manager();
        crate::storage::request_context::spawn_traced(async move {
            manager.invalidate_cache_versioned(&bucket, &key, version_id.as_deref()).await;
        });
    }

    #[instrument(level = "debug", skip(self, _fs, req))]
    pub async fn execute_put_object(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let request_context = prepare_put_object_request_context(&req);
        let (event_name, quota_operation, request_method_name) = put_object_execution_context(&req);
        let helper = new_operation_helper(&req, event_name, S3Operation::PutObject, false);

        if request_context.is_post_object && is_post_object_sse_kms_requested(&req.input, &request_context.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for POST object uploads"));
        }
        if let Some(ref storage_class) = req.input.storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        if is_put_object_extract_requested(&request_context.headers) {
            return self.execute_put_object_extract(req).await;
        }

        let resolved_size = resolve_put_body_size(req.input.content_length, &request_context.headers)?;
        self.check_bucket_quota(&req.input.bucket, quota_operation, resolved_size as u64)
            .await?;

        let input = req.input;
        let flow_result =
            DefaultObjectUsecase::run_put_object_flow(input, request_context, request_method_name, resolved_size).await?;
        let helper = bind_helper_object(helper, flow_result.helper_object, flow_result.helper_version_id);
        complete_put_response(helper, flow_result.output)
    }

    pub async fn execute_put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAclPut, S3Operation::PutObjectAcl);
        let PutObjectAclInput {
            bucket,
            key,
            access_control_policy,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        if access_control_policy.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "ACL XML grants are not supported; use canned ACL headers or omit ACL"
            ));
        }

        let event_version_id = version_id
            .or_else(|| object_info.version_id.map(|version_id| version_id.to_string()))
            .unwrap_or_default();
        helper = helper.object(object_info).version_id(event_version_id);

        let result = Ok(S3Response::new(PutObjectAclOutput::default()));
        let _ = helper.complete(&result);
        result
    }

    pub async fn execute_put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectCreatedPutLegalHold, S3Operation::PutObjectLegalHold).suppress_event();
        let PutObjectLegalHoldInput {
            bucket,
            key,
            legal_hold,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let eval_metadata = parse_object_lock_legal_hold(legal_hold)?;

        let popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        let info = store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_object_lock_configuration_input(&input_cfg)?;

        match metadata_sys::get_object_lock_config(&bucket).await {
            Ok(_) => {}
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    // AWS S3 allows enabling Object Lock on existing buckets if versioning
                    // is already enabled. Reject only when versioning is not enabled.
                    if !BucketVersioningSys::enabled(&bucket).await {
                        return Err(S3Error::with_message(
                            S3ErrorCode::InvalidBucketState,
                            "Object Lock configuration cannot be enabled on existing buckets".to_string(),
                        ));
                    }
                } else {
                    warn!("get_object_lock_config err {:?}", err);
                    return Err(S3Error::with_message(
                        S3ErrorCode::InternalError,
                        "Failed to get bucket ObjectLockConfiguration".to_string(),
                    ));
                }
            }
        };

        let data = serialize(&input_cfg).map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)))?;

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // When Object Lock is enabled, automatically enable versioning if not already enabled.
        // This matches S3-compatible behavior.
        let versioning_config = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let enable_versioning_config = VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            };
            let versioning_data = serialize(&enable_versioning_config)
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)))?;
            metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, versioning_data)
                .await
                .map_err(ApiError::from)?;
        }

        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    pub async fn execute_put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectCreatedPutRetention, S3Operation::PutObjectRetention).suppress_event();
        let PutObjectRetentionInput {
            bucket,
            key,
            retention,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let new_retain_until = retention
            .as_ref()
            .and_then(|r| r.retain_until_date.as_ref())
            .map(|d| OffsetDateTime::from(d.clone()));
        let new_mode = retention.as_ref().and_then(|r| r.mode.as_ref()).map(|mode| mode.as_str());

        // TODO(security): Known TOCTOU race condition (fix in future PR).
        //
        // There is a time-of-check-time-of-use (TOCTOU) window between the retention
        // check below (using get_object_info + check_retention_for_modification) and
        // the actual update performed later in put_object_metadata.
        //
        // In theory:
        //   * Thread A reads retention mode = GOVERNANCE and checks the bypass header.
        //   * Thread B updates retention to COMPLIANCE mode.
        //   * Thread A then proceeds to modify retention, still assuming GOVERNANCE,
        //     and effectively bypasses what is now COMPLIANCE mode.
        //
        // This would violate the S3 spec, which states that COMPLIANCE-mode retention
        // cannot be modified even with a bypass header.
        //
        // Possible fixes (to be implemented in a future change):
        //   1. Pass the expected retention mode down to the storage layer and verify
        //      it has not changed immediately before the update.
        //   2. Use optimistic concurrency (e.g., version/etag) so that the update
        //      fails if the object changed between check and update.
        //   3. Perform the retention check inside the same lock/transaction scope as
        //      the metadata update within the storage layer.
        //
        // Current mitigation: the storage layer provides a fast_lock_manager, which
        // offers some protection, but it does not fully eliminate this race.
        let check_opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        if let Ok(existing_obj_info) = store.get_object_info(&bucket, &key, &check_opts).await {
            let bypass_governance = has_bypass_governance_header(&req.headers);
            if let Some(block_reason) =
                check_retention_for_modification(&existing_obj_info.user_defined, new_mode, new_retain_until, bypass_governance)
            {
                return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
            }
        }

        let eval_metadata = parse_object_lock_retention(retention)?;

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.eval_metadata = Some(eval_metadata);

        let object_info = store.put_object_metadata(&bucket, &key, &opts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        if tagging.tag_set.len() > 10 {
            error!("Tag set exceeds maximum of 10 tags: {}", tagging.tag_set.len());
            return Err(s3_error!(InvalidTag, "Cannot have more than 10 tags per object"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut tag_keys = std::collections::HashSet::with_capacity(tagging.tag_set.len());
        for tag in &tagging.tag_set {
            let key = tag.key.as_ref().filter(|k| !k.is_empty()).ok_or_else(|| {
                error!("Empty tag key");
                s3_error!(InvalidTag, "Tag key cannot be empty")
            })?;

            if key.len() > 128 {
                error!("Tag key too long: {} bytes", key.len());
                return Err(s3_error!(InvalidTag, "Tag key is too long, maximum allowed length is 128 characters"));
            }

            let value = tag.value.as_ref().ok_or_else(|| {
                error!("Null tag value");
                s3_error!(InvalidTag, "Tag value cannot be null")
            })?;

            if value.len() > 256 {
                error!("Tag value too long: {} bytes", value.len());
                return Err(s3_error!(InvalidTag, "Tag value is too long, maximum allowed length is 256 characters"));
            }

            if !tag_keys.insert(key) {
                error!("Duplicate tag key: {}", key);
                return Err(s3_error!(InvalidTag, "Cannot provide multiple Tags with the same key"));
            }
        }

        let tags = encode_tags(tagging.tag_set);
        debug!("Encoded tags: {}", tags);

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        store.put_object_tags(&bucket, &object, &tags, &opts).await.map_err(|e| {
            error!("Failed to put object tags: {}", e);
            counter!("rustfs.put_object_tagging.failure").increment(1);
            ApiError::from(e)
        })?;

        let event_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!(
                    bucket = %bucket,
                    object = %object,
                    version_id = ?req.input.version_id,
                    error = %err,
                    "failed to load object info for put-object-tagging notification; falling back to request context"
                );
                None
            }
        };

        let manager = get_concurrency_manager();
        let version_id = req.input.version_id.clone();
        let cache_key = ConcurrencyManager::make_cache_key(&bucket, &object, version_id.clone().as_deref());
        let cache_bucket = bucket.clone();
        let cache_object = object.clone();
        crate::storage::request_context::spawn_traced(async move {
            manager
                .invalidate_cache_versioned(&cache_bucket, &cache_object, version_id.as_deref())
                .await;
            debug!("Cache invalidated for tagged object: {}", cache_key);
        });

        counter!("rustfs.put_object_tagging.success").increment(1);

        let event_version_id = req
            .input
            .version_id
            .as_deref()
            .filter(|version_id| !version_id.is_empty())
            .map(str::to_string)
            .or_else(|| {
                event_object_info
                    .as_ref()
                    .and_then(|info| info.version_id.map(|version_id| version_id.to_string()))
            })
            .unwrap_or_default();
        if let Some(event_object_info) = event_object_info {
            helper = helper.object(event_object_info);
        }
        helper = helper.version_id(event_version_id);

        let result = Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: req.input.version_id.clone(),
        }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "put").record(duration.as_secs_f64());
        result
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
        let bootstrap = init_get_object_bootstrap(&req.input.bucket, &req.input.key, &request_id)?;
        let request_context = prepare_get_object_request_context(&req).await?;
        let base_buffer_size = self.base_buffer_size();
        let manager = get_concurrency_manager();
        let flow_runtime = GetObjectFlowRuntime {
            manager,
            bootstrap: &bootstrap,
            base_buffer_size,
        };
        let helper = new_operation_helper(&req, EventName::ObjectAccessedGet, S3Operation::GetObject, true);
        let flow_result = get_object_flow::run_get_object_flow(request_context.clone(), flow_runtime).await;

        let GetObjectBootstrap {
            mut request_guard,
            _deadlock_request_guard,
            ..
        } = bootstrap;

        let result = match flow_result {
            Ok(flow_result) => complete_get_flow_result(helper, &request_context, flow_result).await,
            Err(err) => Err(err),
        };

        if result.is_ok() {
            request_guard.finish_ok();
        } else {
            request_guard.finish_err();
        }
        result
    }

    pub async fn execute_get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetObjectAclInput {
            bucket, key, version_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        Ok(S3Response::new(acl::build_get_object_acl_output()))
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

    pub async fn execute_get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetLegalHold, S3Operation::GetObjectLegalHold).suppress_event();
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let legal_hold = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .map(|v| v.as_str().to_string());

        let status = if let Some(v) = legal_hold {
            v
        } else {
            ObjectLockLegalHoldStatus::OFF.to_string()
        };

        let output = GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from(status)),
            }),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetObjectLockConfigurationInput { bucket, .. } = req.input;

        let object_lock_configuration = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ObjectLockConfigurationNotFoundError,
                        "Object Lock configuration does not exist for this bucket".to_string(),
                    ));
                }
                warn!("get_object_lock_config err {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "Failed to load Object Lock configuration".to_string(),
                ));
            }
        };

        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration,
        }))
    }

    pub async fn execute_get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetRetention, S3Operation::GetObjectRetention).suppress_event();
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let mode = object_info
            .user_defined
            .get("x-amz-object-lock-mode")
            .map(|v| ObjectLockRetentionMode::from(v.as_str().to_string()));

        let retain_until_date = object_info
            .user_defined
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| OffsetDateTime::parse(v.as_str(), &Rfc3339).ok())
            .map(Timestamp::from);

        let output = GetObjectRetentionOutput {
            retention: Some(ObjectLockRetention { mode, retain_until_date }),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let start_time = std::time::Instant::now();
        let GetObjectTaggingInput { bucket, key: object, .. } = req.input;

        info!("Starting get_object_tagging for bucket: {}, object: {}", bucket, object);

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        let tags = store.get_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            if is_err_object_not_found(&e) {
                error!("Object not found: {}", e);
                return s3_error!(NoSuchKey);
            }
            error!("Failed to get object tags: {}", e);
            ApiError::from(e).into()
        })?;

        let tag_set = decode_tags(tags.as_str());
        debug!("Decoded tag set: {:?}", tag_set);

        counter!("rustfs.get_object_tagging.success").increment(1);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "get").record(duration.as_secs_f64());
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: req.input.version_id.clone(),
        }))
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

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(gr.stream));

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

        if let Some(material) = sse_decryption(decryption_request).await? {
            reader = material.wrap_single_reader(reader);
            if let Some(original) = material.original_size {
                src_info.actual_size = original;
            }
        }

        strip_managed_encryption_metadata(&mut src_info.user_defined);

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let mut length = actual_size;

        let mut compress_metadata = HashMap::new();

        if is_compressible(&req.headers, &key) && actual_size > MIN_COMPRESSIBLE_SIZE as i64 {
            insert_str(&mut compress_metadata, SUFFIX_COMPRESSION, CompressionAlgorithm::default().to_string());
            insert_str(&mut compress_metadata, SUFFIX_ACTUAL_SIZE, actual_size.to_string());

            let hrd = EtagReader::new(reader, None);

            // let hrd = HashReader::new(reader, length, actual_size, None, false).map_err(ApiError::from)?;

            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            length = HashReader::SIZE_PRESERVE_LAYER;
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
                src_info.user_defined.insert(CONTENT_TYPE.to_string(), ct);
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

        let mut reader = HashReader::new(reader, length, actual_size, None, None, false).map_err(ApiError::from)?;

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
            reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                .map_err(ApiError::from)?;

            src_info.user_defined.extend(material.metadata);
        }

        src_info.put_object_reader = Some(ChunkNativePutData::new(reader));

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
        Self::spawn_cache_invalidation(bucket.clone(), key.clone(), raw_dest_version.clone());
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

        let manager = get_concurrency_manager();
        let bucket_clone = bucket.clone();
        let deleted_objects = dobjs.clone();
        crate::storage::request_context::spawn_traced(async move {
            for dobj in deleted_objects {
                manager
                    .invalidate_cache_versioned(
                        &bucket_clone,
                        &dobj.object_name,
                        dobj.version_id.map(|v| v.to_string()).as_deref(),
                    )
                    .await;
            }
        });

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
        crate::storage::helper::spawn_background(async move {
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

        Self::spawn_cache_invalidation(bucket.clone(), key.clone(), obj_info.version_id.map(|v| v.to_string()));

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
            helper = helper
                .event_name(EventName::ObjectRemovedDelete)
                .object(ObjectInfo {
                    name: key.clone(),
                    bucket: bucket.clone(),
                    ..Default::default()
                })
                .version_id(String::new());
            let result = Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
            let manager = get_capacity_manager();
            manager.record_write_operation().await;
            let _ = helper.complete(&result);
            return result;
        }

        if obj_info.replication_status == ReplicationStatusType::Replica
            || obj_info.replication_status == ReplicationStatusType::Pending
            || obj_info.version_purge_status == VersionPurgeStatusType::Pending
        {
            schedule_replication_delete(DeletedObjectReplicationInfo {
                delete_object: rustfs_ecstore::store_api::DeletedObject {
                    delete_marker: obj_info.delete_marker,
                    delete_marker_version_id: if obj_info.delete_marker { obj_info.version_id } else { None },
                    object_name: key.clone(),
                    version_id: if obj_info.delete_marker { None } else { obj_info.version_id },
                    delete_marker_mtime: obj_info.mod_time,
                    replication_state: Some(obj_info.replication_state()),
                    ..Default::default()
                },
                bucket: bucket.clone(),
                event_type: REPLICATE_INCOMING_DELETE.to_string(),
                ..Default::default()
            })
            .await;
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
    pub async fn execute_delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingDelete, S3Operation::DeleteObjectTagging);
        let DeleteObjectTaggingInput {
            bucket,
            key: object,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_for_parse = version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id_for_parse)?.map(Into::into),
            ..Default::default()
        };

        store.delete_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            error!("Failed to delete object tags: {}", e);
            ApiError::from(e)
        })?;

        let event_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!(
                    bucket = %bucket,
                    object = %object,
                    version_id = ?version_id,
                    error = %err,
                    "failed to load object info for delete-object-tagging notification; falling back to request context"
                );
                None
            }
        };

        let manager = get_concurrency_manager();
        let version_id_clone = version_id.clone();
        let cache_bucket = bucket.clone();
        let cache_object = object.clone();
        crate::storage::request_context::spawn_traced(async move {
            manager
                .invalidate_cache_versioned(&cache_bucket, &cache_object, version_id_clone.as_deref())
                .await;
            debug!(
                "Cache invalidated for deleted tagged object: bucket={}, object={}, version_id={:?}",
                cache_bucket, cache_object, version_id_clone
            );
        });

        counter!("rustfs.delete_object_tagging.success").increment(1);

        let event_version_id = version_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                event_object_info
                    .as_ref()
                    .and_then(|info| info.version_id.map(|version_id| version_id.to_string()))
            })
            .unwrap_or_default();
        if let Some(event_object_info) = event_object_info {
            helper = helper.object(event_object_info);
        }
        helper = helper.version_id(event_version_id);

        let result = Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "delete").record(duration.as_secs_f64());
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

        // TODO: range download

        let content_length = info.get_actual_size().map_err(|e| {
            error!("get_actual_size error: {}", e);
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
        // Prefer explicit storage_class from object info; fall back to persisted metadata header.
        let storage_class = info
            .storage_class
            .clone()
            .or_else(|| metadata_map.get("x-amz-storage-class").cloned())
            .filter(|s| !s.is_empty())
            .map(StorageClass::from);
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
                let output =
                    restore::build_restore_object_output(Some(RequestCharged::from_static(RequestCharged::REQUESTER)), None);
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

        crate::storage::request_context::spawn_traced(async move {
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

        let output = restore::build_restore_object_output(Some(RequestCharged::from_static(RequestCharged::REQUESTER)), None);
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
        crate::storage::request_context::spawn_traced(async move {
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

        Ok(S3Response::new(select::build_select_object_content_output(
            SelectObjectContentEventStream::new(stream),
        )))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let request_context = prepare_put_object_request_context(&req);
        let helper = new_operation_helper(&req, EventName::ObjectCreatedPut, S3Operation::PutObject, true);
        if is_sse_kms_requested(&req.input, &request_context.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        let resolved_size = resolve_put_body_size(req.input.content_length, &request_context.headers)?;
        self.check_bucket_quota(&req.input.bucket, QuotaOperation::PutObject, resolved_size as u64)
            .await?;
        let notify = self
            .context
            .as_ref()
            .map(|context| context.notify())
            .unwrap_or_else(default_notify_interface);
        let input = req.input;
        let output = DefaultObjectUsecase::run_put_object_extract_flow(input, request_context, notify, resolved_size).await?;
        complete_put_response(helper, output)
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
    use http::{Extensions, HeaderMap, Method, Uri};

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
    fn put_object_execution_context_defaults_to_put() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::PUT);

        let (event_name, quota_operation, method_name) = put_object_execution_context(&req);
        assert_eq!(event_name, EventName::ObjectCreatedPut);
        assert!(matches!(quota_operation, QuotaOperation::PutObject));
        assert_eq!(method_name, "PUT");
    }

    #[test]
    fn put_object_execution_context_uses_post_marker() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();
        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);

        let (event_name, quota_operation, method_name) = put_object_execution_context(&req);
        assert_eq!(event_name, EventName::ObjectCreatedPost);
        assert!(matches!(quota_operation, QuotaOperation::PostObject));
        assert_eq!(method_name, "POST");
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

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
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

        let err = usecase.execute_copy_object(req).await.unwrap_err();
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

        let err = usecase.execute_delete_object(req).await.unwrap_err();
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

    #[tokio::test]
    async fn execute_delete_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = DeleteObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_acl_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectAclInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
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
    async fn execute_get_object_legal_hold_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectLegalHoldInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_legal_hold(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_retention_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectRetentionInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_retention(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_acl_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectAclInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_legal_hold_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectLegalHoldInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_legal_hold(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_lock_configuration_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectLockConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .object_lock_configuration(Some(ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            }))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_lock_configuration(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn validate_object_lock_configuration_rejects_disabled_status() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from("Disabled".to_string())),
            rule: None,
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn validate_object_lock_configuration_rejects_invalid_default_retention_mode() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from("abc".to_string())),
                    days: Some(1),
                    years: None,
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn validate_object_lock_configuration_rejects_days_and_years_together() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    days: Some(1),
                    years: Some(1),
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn validate_object_lock_configuration_rejects_missing_default_retention() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule { default_retention: None }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
    }

    #[test]
    fn validate_object_lock_configuration_rejects_zero_days() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    days: Some(0),
                    years: None,
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::Custom("InvalidRetentionPeriod".into()));
    }

    #[test]
    fn validate_object_lock_configuration_rejects_too_many_years() {
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    days: None,
                    years: Some(MAXIMUM_RETENTION_YEARS + 1),
                }),
            }),
        };

        let err = validate_object_lock_configuration_input(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::Custom("InvalidRetentionPeriod".into()));
    }

    #[tokio::test]
    async fn execute_put_object_retention_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectRetentionInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_retention(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .tagging(Tagging {
                tag_set: vec![Tag {
                    key: Some("k".to_string()),
                    value: Some("v".to_string()),
                }],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
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
}
