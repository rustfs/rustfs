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

//! Bucket application use-case contracts.
//!
//! The struct, admission control and helpers shared by every configuration family stay here. Each family
//! keeps its operations and their tests in its own file under `bucket_usecase/`.

use super::storage_api::bucket_usecase::ECStore;
use super::storage_api::bucket_usecase::StorageObjectInfo as ObjectInfo;
#[cfg(test)]
use super::storage_api::bucket_usecase::access::ReqInfo;
use super::storage_api::bucket_usecase::access::{
    TableDataPlaneListAccess, authorize_request, bucket_config_mutation_incarnation, log_list_buckets_iam_implicit_deny,
    prepare_list_buckets_iam_authorization, prepare_odm_read_generation, req_info_ref,
};
#[cfg(test)]
use super::storage_api::bucket_usecase::bucket::target::BucketTarget;
use super::storage_api::bucket_usecase::bucket::{
    lifecycle::bucket_lifecycle_ops::{
        LIFECYCLE_MALFORMED_XML_ERROR_KIND, enqueue_expiry_for_existing_objects, enqueue_transition_for_existing_objects,
        run_stale_multipart_upload_cleanup_once, validate_lifecycle_config, validate_transition_tier,
    },
    metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TARGETS_FILE,
        BUCKET_VERSIONING_CONFIG,
    },
    metadata_sys,
    policy_sys::PolicySys,
    replication::{
        OperatorRuleContract, ReplicationTargetValidationError, invalid_replication_config_status_field,
        merge_user_replication_config, replication_target_arns, should_remove_replication_target,
        unsupported_replication_config_field, validate_replication_config_structure, validate_replication_config_target_arns,
    },
    target::{BucketTargetType, BucketTargets},
    utils::{BucketConfigXml, serialize},
    versioning_sys::BucketVersioningSys,
};
use super::storage_api::bucket_usecase::contract::bucket::{
    BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions,
};
use super::storage_api::bucket_usecase::contract::list::{
    ListObjectVersionsInfo as StorageListObjectVersionsInfo, ListObjectsV2Info as StorageListObjectsV2Info,
};
use super::storage_api::bucket_usecase::error::StorageError;
use super::storage_api::bucket_usecase::helper::{OperationHelper, spawn_background_with_context};
use super::storage_api::bucket_usecase::object_utils::to_s3s_xml_etag;
use super::storage_api::bucket_usecase::s3_api::bucket::{
    ListObjectVersionsParams, ListObjectsV2Params, build_list_object_versions_output, build_list_objects_output,
    build_list_objects_v2_output, encode_list_objects_v2_continuation_token, parse_list_object_versions_params,
    parse_list_objects_v2_params, rustfs_owner,
};
use super::storage_api::bucket_usecase::{
    get_validated_store, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
    request_context, validate_list_object_unordered_with_delimiter,
};
#[cfg(feature = "diagnostic-read-version-caller")]
use super::storage_api::diagnostic;
use crate::app::RequestEnvelope;
use crate::app::bucket_list_through as list_through;
use crate::app::dto_values;
use crate::app::object::shared::mark_on_demand_migration_list_local_only;
use crate::app::object_data_cache::invalidate_object_data_cache_bucket_after_delete;
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_encryption_service, current_notification_system,
    current_notification_system_for_context, current_notify_interface_for_context, current_object_data_cache_for_context,
    current_object_store_handle_for_context,
};
use crate::app::table_list_isolation;
use crate::auth::get_condition_values_with_client_info;
use crate::error::ApiError;
use crate::site_replication::{
    cancel_site_replication_delete_bucket, commit_site_replication_delete_bucket, prepare_site_replication_delete_bucket,
    site_replication_bucket_meta_hook, site_replication_make_bucket_hook, with_site_replication_bucket_mutation_lock,
};
use crate::storage::storage_api::lock_bucket_targets_metadata;
use http::StatusCode;
use metrics::counter;
use rustfs_config::RUSTFS_REGION;
use rustfs_gateway_types::cors_tagging::{PersistedCorsConfiguration, PersistedTagging};
use rustfs_gateway_types::dto as gateway_dto;
use rustfs_gateway_types::persistence::{
    PersistedBucketEncryptionConfiguration, PersistedNotificationConfiguration,
    PersistedObjectLockConfiguration as ObjectLockConfiguration, PersistedPublicAccessBlockConfiguration,
    PersistedVersioningConfiguration,
};
use rustfs_gateway_types::persistence::{PersistedLifecycleConfiguration, PersistedReplicationConfiguration};
use rustfs_io_metrics::record_s3_op;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_policy::policy::{
    action::{Action, S3Action},
    {BucketPolicy, BucketPolicyArgs, Effect},
};
use rustfs_s3_ops::S3Operation;
use rustfs_targets::{
    EventName,
    arn::{ARN, TargetID, TargetIDError},
};
use rustfs_utils::http::{force_delete_header, get_header};
use rustfs_utils::obj::extract_user_defined_metadata;
use s3s::region::Region;
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::Display,
    future::Future,
    io::Write,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::{Instrument as _, debug, error, info, instrument, warn};

mod accelerate;
mod acl;
mod core;
#[cfg(any(test, feature = "gateway-stack"))]
pub(crate) use self::core::requested_location_constraint;
mod cors;
mod encryption;
#[cfg(test)]
mod guards;
mod lifecycle;
mod listing;
mod logging;
mod notification;
mod object_lock;
mod policy;
mod public_access_block;
mod replication;
mod request_payment;
mod tagging;
#[cfg(test)]
mod test_support;
mod versioning;
mod website;

const LOG_COMPONENT_APP: &str = "app";
const LOG_SUBSYSTEM_BUCKET: &str = "bucket";
const BUCKET_OPERATION_CONCURRENCY: usize = 8;
const BUCKET_OPERATION_QUEUE_CAPACITY: usize = 128;
const BUCKET_OPERATION_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);
static BUCKET_OPERATION_ADMISSION: LazyLock<Arc<BucketOperationAdmission>> = LazyLock::new(|| {
    Arc::new(BucketOperationAdmission::new(
        BUCKET_OPERATION_CONCURRENCY,
        BUCKET_OPERATION_QUEUE_CAPACITY,
    ))
});

struct BucketOperationAdmission {
    active: Arc<Semaphore>,
    waiting: Semaphore,
    in_flight: Arc<Semaphore>,
}

impl BucketOperationAdmission {
    fn new(concurrency: usize, queue_capacity: usize) -> Self {
        Self {
            active: Arc::new(Semaphore::new(concurrency)),
            waiting: Semaphore::new(queue_capacity),
            in_flight: Arc::new(Semaphore::new(concurrency + queue_capacity)),
        }
    }

    async fn acquire(&self, operation: &'static str) -> S3Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        // Reserve the entire operation before mutation, including post-commit
        // work that can outlive both its execution slot and the request.
        let in_flight = self.in_flight.clone().try_acquire_owned().map_err(|err| match err {
            TryAcquireError::NoPermits => bucket_admission_slow_down(operation, "in-flight limit reached"),
            TryAcquireError::Closed => bucket_admission_closed(operation),
        })?;
        match self.active.clone().try_acquire_owned() {
            Ok(permit) => return Ok((permit, in_flight)),
            Err(TryAcquireError::Closed) => return Err(bucket_admission_closed(operation)),
            Err(TryAcquireError::NoPermits) => {}
        }

        // Wait on the request task; only admitted mutations become detached tasks.
        // The queue permit bounds waiting requests and is released on cancellation.
        let _waiting = self.waiting.try_acquire().map_err(|err| match err {
            TryAcquireError::NoPermits => bucket_admission_slow_down(operation, "queue is full"),
            TryAcquireError::Closed => bucket_admission_closed(operation),
        })?;
        let active = tokio::time::timeout(BUCKET_OPERATION_QUEUE_TIMEOUT, self.active.clone().acquire_owned())
            .await
            .map_err(|_| bucket_admission_slow_down(operation, "queue wait timed out"))?
            .map_err(|_| bucket_admission_closed(operation))?;
        Ok((active, in_flight))
    }
}

fn bucket_admission_closed(operation: &'static str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{operation} admission closed"))
}

fn bucket_admission_slow_down(operation: &'static str, reason: &'static str) -> S3Error {
    let mut error = S3Error::with_message(S3ErrorCode::SlowDown, format!("{operation} {reason}; retry later"));
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("1"));
    error.set_headers(headers);
    error
}
use urlencoding::encode;

fn serialize_config<T: BucketConfigXml>(value: &T) -> S3Result<Vec<u8>> {
    serialize(value).map_err(to_internal_error)
}

async fn update_bucket_config_for_incarnation(
    bucket: &str,
    config_file: &str,
    data: Vec<u8>,
    expected_incarnation_id: Option<uuid::Uuid>,
) -> Result<time::OffsetDateTime, StorageError> {
    match expected_incarnation_id {
        Some(incarnation_id) => metadata_sys::update_if_incarnation(bucket, config_file, data, incarnation_id).await,
        None => metadata_sys::update(bucket, config_file, data).await,
    }
}

async fn delete_bucket_config_for_incarnation(
    bucket: &str,
    config_file: &str,
    expected_incarnation_id: Option<uuid::Uuid>,
) -> Result<time::OffsetDateTime, StorageError> {
    match expected_incarnation_id {
        Some(incarnation_id) => metadata_sys::delete_if_incarnation(bucket, config_file, incarnation_id).await,
        None => metadata_sys::delete(bucket, config_file).await,
    }
}

fn to_internal_error(err: impl Display) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{err}"))
}

fn sr_bucket_meta_item(bucket: String, item_type: &str) -> SRBucketMeta {
    SRBucketMeta {
        bucket,
        r#type: item_type.to_string(),
        updated_at: Some(time::OffsetDateTime::now_utc()),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        derived_rule_contract: true,
        ..Default::default()
    }
}

async fn notify_bucket_metadata_reload(
    bucket: String,
    operation: &'static str,
    request_context: Option<request_context::RequestContext>,
    scanner_maintenance_change: bool,
) {
    notify_bucket_metadata_reload_with_context(bucket, operation, request_context, scanner_maintenance_change, None).await;
}

pub(crate) async fn notify_table_bucket_metadata_reload(
    bucket: String,
    operation: &'static str,
    app_context: Option<Arc<AppContext>>,
) {
    notify_bucket_metadata_reload_with_context(bucket, operation, None, false, app_context).await;
}

async fn notify_bucket_metadata_reload_with_context(
    bucket: String,
    operation: &'static str,
    request_context: Option<request_context::RequestContext>,
    scanner_maintenance_change: bool,
    app_context: Option<Arc<AppContext>>,
) {
    record_local_scanner_maintenance_reload(&bucket, scanner_maintenance_change);
    // Keep reload detached across request cancellation, but wait before a healthy peer can serve the previous config.
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    spawn_background_with_context(request_context, async move {
        let notification_sys = if let Some(context) = app_context.as_deref() {
            current_notification_system_for_context(Some(context))
        } else {
            current_notification_system()
        };
        if let Some(notification_sys) = notification_sys {
            let result = if scanner_maintenance_change {
                notification_sys.load_bucket_metadata_for_scanner_maintenance(&bucket).await
            } else {
                notification_sys.load_bucket_metadata(&bucket).await
            };
            if let Err(err) = result {
                warn!(bucket = %bucket, error = %err, "failed to notify peers after {operation}");
            }
        }
        let _ = completed_tx.send(());
    });
    let _ = completed_rx.await;
}

fn record_local_scanner_maintenance_reload(bucket: &str, scanner_maintenance_change: bool) {
    if scanner_maintenance_change {
        rustfs_scanner::record_scanner_maintenance_change(bucket);
    }
}

/// Notify peers to drop their cached metadata for a bucket that was just deleted,
/// so they stop serving stale bucket configuration. Runs in the background to
/// avoid blocking the delete response.
fn notify_bucket_metadata_delete(bucket: String, request_context: Option<request_context::RequestContext>) {
    spawn_background_with_context(request_context, async move {
        if let Some(notification_sys) = current_notification_system() {
            for peer_err in notification_sys
                .delete_bucket_metadata(&bucket)
                .await
                .into_iter()
                .filter(|e| e.err.is_some())
            {
                warn!(
                    bucket = %bucket,
                    host = %peer_err.host,
                    error = ?peer_err.err,
                    "failed to notify peer to delete bucket metadata"
                );
            }
        }
    });
}

#[derive(Clone, Default)]
pub struct DefaultBucketUsecase {
    context: Option<Arc<AppContext>>,
}

async fn await_bucket_usecase_on_fresh_task<T, F, MakeFuture>(operation: &'static str, make_future: MakeFuture) -> S3Result<T>
where
    T: Send + 'static,
    F: Future<Output = S3Result<T>> + Send + 'static,
    MakeFuture: FnOnce(OwnedSemaphorePermit) -> F + Send + 'static,
{
    await_bucket_usecase_on_fresh_task_with_admission(operation, Arc::clone(&BUCKET_OPERATION_ADMISSION), make_future).await
}

async fn await_bucket_usecase_on_fresh_task_with_admission<T, F, MakeFuture>(
    operation: &'static str,
    admission: Arc<BucketOperationAdmission>,
    make_future: MakeFuture,
) -> S3Result<T>
where
    T: Send + 'static,
    F: Future<Output = S3Result<T>> + Send + 'static,
    MakeFuture: FnOnce(OwnedSemaphorePermit) -> F + Send + 'static,
{
    let (mutation_permit, in_flight) = admission.acquire(operation).await?;
    let future = async move {
        let _in_flight = in_flight;
        make_future(mutation_permit).await
    };
    #[cfg(feature = "diagnostic-read-version-caller")]
    let future = diagnostic::with_captured_read_version_context(diagnostic::ReadVersionContextBoundary::BucketTask, future);
    #[cfg(not(feature = "diagnostic-read-version-caller"))]
    let future = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        future,
    );
    tokio::spawn(future.in_current_span())
        .await
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{operation} task failed: {err}")))?
}

impl DefaultBucketUsecase {
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

    fn global_region(&self) -> Option<Region> {
        self.context.as_ref().and_then(|context| context.region().get())
    }

    fn additional_regions(&self) -> Vec<Region> {
        self.context
            .as_ref()
            .map(|context| context.region().additional())
            .unwrap_or_default()
    }

    fn object_store(&self) -> Option<Arc<ECStore>> {
        current_object_store_handle_for_context(self.context.as_deref())
    }

    /// Lists the buckets the caller of `envelope` may see, for either edge.
    ///
    /// The access phase must have installed the caller's `ReqInfo`. A caller allowed
    /// `s3:ListAllMyBuckets` sees every bucket; any other caller sees the buckets it may list or
    /// locate. `_input` is not read: RustFS answers one unpaginated listing and ignores `prefix`,
    /// `max-buckets`, `continuation-token` and `bucket-region`.
    ///
    /// # Errors
    ///
    /// `AccessDenied` for an anonymous caller and for one that may see no bucket; `InternalError`
    /// when the store or IAM is not initialized or the access phase did not run; otherwise the
    /// storage error of the listing.
    #[instrument(level = "debug", skip(self, envelope, _input))]
    pub async fn execute_list_buckets(
        &self,
        envelope: RequestEnvelope,
        _input: gateway_dto::ListBucketsInput,
    ) -> rustfs_s3_types::S3Result<gateway_dto::ListBucketsOutput> {
        let Some(store) = self.object_store() else {
            return Err(rustfs_s3_types::s3_error!(InternalError, "Not init"));
        };

        if envelope
            .credentials()
            .is_none_or(|credentials| credentials.access_key().is_empty())
        {
            return Err(rustfs_s3_types::s3_error!(AccessDenied, "Access Denied"));
        }

        let iam_authorization = prepare_list_buckets_iam_authorization(&envelope).await?;
        let bucket_infos = if iam_authorization.is_allowed("", S3Action::ListAllMyBucketsAction).await {
            store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?
        } else {
            log_list_buckets_iam_implicit_deny(&envelope)?;
            let bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
            let mut visible_bucket_infos = Vec::new();
            for info in bucket_infos {
                if iam_authorization.is_allowed(&info.name, S3Action::ListBucketAction).await
                    || iam_authorization
                        .is_allowed(&info.name, S3Action::GetBucketLocationAction)
                        .await
                {
                    visible_bucket_infos.push(info);
                }
            }

            if visible_bucket_infos.is_empty() {
                return Err(ApiError::access_denied().into());
            }
            visible_bucket_infos
        };

        let buckets = bucket_infos
            .iter()
            .map(|info| {
                Ok(gateway_dto::Bucket {
                    name: dto_values::stored_bucket_name(&info.name)?,
                    creation_date: dto_values::timestamp(info.created.unwrap_or(time::OffsetDateTime::UNIX_EPOCH)),
                    ..Default::default()
                })
            })
            .collect::<rustfs_s3_types::S3Result<_>>()?;
        Ok(gateway_dto::ListBucketsOutput {
            buckets,
            owner: dto_values::owner(),
            continuation_token: None,
            prefix: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::{build_request, build_request_with_req_info};
    use crate::app::storage_api::s3::dto::CreateBucketInput;
    use http::Method;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn diagnostic_bucket_usecase_task_keeps_entry_and_permits_after_waiter_drop() {
        use super::diagnostic::{ReadVersionEntrySource, current_read_version_entry_source, with_read_version_entry};
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, current_s3_http_operation_name, observe_s3_http_operation,
            with_s3_http_operation_context,
        };

        let mut request = S3HttpRequestGuard::new("PUT");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(S3Operation::CreateBucket);
            current_s3_http_operation_context().expect("captured bucket operation")
        });
        let admission = Arc::new(BucketOperationAdmission::new(1, 1));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let mut caller = Box::pin(with_s3_http_operation_context(Some(operation), async {
            with_read_version_entry(ReadVersionEntrySource::TierConfigReload, async {
                await_bucket_usecase_on_fresh_task_with_admission(
                    "test bucket operation",
                    admission.clone(),
                    |permit| async move {
                        entered_tx.send(()).expect("caller is present");
                        release_rx.await.expect("bucket work may finish");
                        let observed = with_read_version_entry(ReadVersionEntrySource::ConfigRead, async {
                            (current_s3_http_operation_name(), current_read_version_entry_source())
                        })
                        .await;
                        drop(permit);
                        finished_tx.send(observed).expect("completion observer is present");
                        Ok(())
                    },
                )
                .await
            })
            .await
        }));
        assert!(futures::poll!(caller.as_mut()).is_pending());
        entered_rx.await.expect("bucket task entered");
        drop(caller);
        drop(request);
        assert_eq!(current_read_version_entry_source(), None);
        assert_eq!(admission.active.available_permits(), 0);
        assert_eq!(admission.in_flight.available_permits(), 1);
        release_tx.send(()).expect("detached bucket task remains alive");
        assert_eq!(
            finished_rx.await.expect("bucket task completes"),
            (Some("s3:CreateBucket"), Some(ReadVersionEntrySource::TierConfigReload))
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while admission.in_flight.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed task releases admission");
        assert_eq!(admission.active.available_permits(), 1);
        let internal = await_bucket_usecase_on_fresh_task_with_admission("internal test", admission, |_permit| async {
            Ok((current_s3_http_operation_name(), current_read_version_entry_source()))
        })
        .await
        .expect("unscoped bucket task completes");
        assert_eq!(internal, (None, None));
    }

    #[tokio::test]
    async fn bucket_usecase_task_preserves_http_operation_context() {
        use rustfs_io_metrics::s3_http_metrics::{S3HttpRequestGuard, current_s3_http_operation_name, observe_s3_http_operation};

        for operation in [S3Operation::CreateBucket, S3Operation::DeleteBucket] {
            for fail in [false, true] {
                let mut request = S3HttpRequestGuard::new("PUT");
                request.in_scope(|| observe_s3_http_operation(operation));
                let admission = Arc::new(BucketOperationAdmission::new(1, 1));
                let mut task = Box::pin(await_bucket_usecase_on_fresh_task_with_admission(
                    "test bucket operation",
                    admission.clone(),
                    move |_permit| async move {
                        assert_eq!(current_s3_http_operation_name(), Some(operation.as_str()));
                        tokio::task::yield_now().await;
                        assert_eq!(current_s3_http_operation_name(), Some(operation.as_str()));
                        if fail {
                            Err(S3Error::with_message(S3ErrorCode::InternalError, "test failure"))
                        } else {
                            Ok(())
                        }
                    },
                ));
                let result = std::future::poll_fn(|cx| request.in_scope(|| task.as_mut().poll(cx))).await;
                assert_eq!(result.is_err(), fail);
                assert_eq!(request.in_scope(current_s3_http_operation_name), Some(operation.as_str()));
                assert_eq!(current_s3_http_operation_name(), None);
                assert_eq!(admission.active.available_permits(), 1);
                assert_eq!(admission.in_flight.available_permits(), 2);
            }
        }

        let internal = await_bucket_usecase_on_fresh_task_with_admission(
            "internal bucket operation",
            Arc::new(BucketOperationAdmission::new(1, 1)),
            |_permit| async { Ok(current_s3_http_operation_name()) },
        )
        .await
        .expect("internal task should complete");
        assert_eq!(internal, None, "internal work must not inherit a previous request");
    }

    #[tokio::test]
    async fn bucket_usecase_task_finishes_post_commit_hooks_after_parent_cancellation() {
        let admission = Arc::new(BucketOperationAdmission::new(1, 1));
        let committed = Arc::new(Notify::new());
        let committed_wait = committed.notified();
        let release_hook = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let finished_wait = finished.notified();
        let hook_ran = Arc::new(AtomicBool::new(false));
        let committed_for_task = committed.clone();
        let release_hook_for_task = release_hook.clone();
        let finished_for_task = finished.clone();
        let hook_ran_for_task = hook_ran.clone();
        let parent_task = await_bucket_usecase_on_fresh_task_with_admission(
            "test bucket operation",
            admission.clone(),
            move |mutation_permit| async move {
                drop(mutation_permit);
                committed_for_task.notify_one();
                release_hook_for_task.notified().await;
                assert_eq!(
                    rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_name(),
                    Some("s3:CreateBucket"),
                    "the detached hook retains attribution after the parent is cancelled"
                );
                hook_ran_for_task.store(true, Ordering::SeqCst);
                finished_for_task.notify_one();
                Ok(())
            },
        );
        let parent = tokio::spawn(async move {
            let mut request = rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard::new("PUT");
            request.in_scope(|| rustfs_io_metrics::s3_http_metrics::observe_s3_http_operation(S3Operation::CreateBucket));
            let mut parent_task = Box::pin(parent_task);
            std::future::poll_fn(|cx| request.in_scope(|| parent_task.as_mut().poll(cx))).await
        });
        committed_wait.await;

        parent.abort();
        let _ = parent.await;
        assert_eq!(admission.active.available_permits(), 1);
        assert_eq!(admission.in_flight.available_permits(), 1, "cancelled post-commit work stays bounded");
        release_hook.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), finished_wait)
            .await
            .expect("the post-commit hook should finish after request cancellation");

        assert!(
            hook_ran.load(Ordering::SeqCst),
            "the fresh task must own both the storage mutation and its post-commit hooks"
        );
        assert_eq!(admission.active.available_permits(), 1);
        assert_eq!(admission.in_flight.available_permits(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_usecase_admission_releases_execution_during_slow_post_commit_work() {
        let admission = Arc::new(BucketOperationAdmission::new(
            BUCKET_OPERATION_CONCURRENCY,
            BUCKET_OPERATION_QUEUE_CAPACITY,
        ));
        let active_mutations = Arc::new(AtomicUsize::new(0));
        let peak_mutations = Arc::new(AtomicUsize::new(0));
        let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let active = active_mutations.clone();
            let peak = peak_mutations.clone();
            let committed = committed_tx.clone();
            tasks.push(tokio::spawn(await_bucket_usecase_on_fresh_task_with_admission(
                "bucket creation",
                admission.clone(),
                move |mutation_permit| async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(mutation_permit);
                    committed.send(()).expect("record commit");
                    tokio::time::sleep(Duration::from_secs(4)).await;
                    Ok(())
                },
            )));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..100 {
                committed_rx.recv().await.expect("every mutation commits");
            }
        })
        .await
        .expect("peer reloads must not serialize subsequent mutations");
        assert_eq!(peak_mutations.load(Ordering::SeqCst), BUCKET_OPERATION_CONCURRENCY);
        assert!(
            tasks.iter().all(|task| !task.is_finished()),
            "requests must still await their post-commit work"
        );
        assert_eq!(admission.active.available_permits(), BUCKET_OPERATION_CONCURRENCY);
        assert_eq!(
            admission.in_flight.available_permits(),
            BUCKET_OPERATION_CONCURRENCY + BUCKET_OPERATION_QUEUE_CAPACITY - 100
        );

        await_bucket_usecase_on_fresh_task_with_admission("bucket deletion", admission.clone(), |mutation_permit| async move {
            let _mutation_permit = mutation_permit;
            Ok(())
        })
        .await
        .expect("deletion can execute while creates await peer reloads");
        for task in tasks {
            task.await.expect("request joins").expect("queued creation succeeds");
        }
        assert_eq!(
            admission.in_flight.available_permits(),
            BUCKET_OPERATION_CONCURRENCY + BUCKET_OPERATION_QUEUE_CAPACITY
        );
    }

    #[tokio::test]
    async fn bucket_usecase_admission_bounds_post_commit_work_after_request_cancellation() {
        let admission = Arc::new(BucketOperationAdmission::new(1, 1));
        let release = Arc::new(Semaphore::new(0));
        let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
        let (finished_tx, mut finished_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut parents = Vec::new();
        for _ in 0..2 {
            let release = release.clone();
            let committed = committed_tx.clone();
            let finished = finished_tx.clone();
            parents.push(tokio::spawn(await_bucket_usecase_on_fresh_task_with_admission(
                "bucket creation",
                admission.clone(),
                move |mutation_permit| async move {
                    drop(mutation_permit);
                    committed.send(()).expect("record commit");
                    release.acquire().await.expect("release hook").forget();
                    finished.send(()).expect("record completion");
                    Ok(())
                },
            )));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..2 {
                committed_rx.recv().await.expect("operation commits");
            }
        })
        .await
        .expect("both operations reach post-commit work");
        for parent in parents {
            parent.abort();
            assert!(parent.await.expect_err("caller cancelled").is_cancelled());
        }
        assert_eq!(admission.active.available_permits(), 1);
        assert_eq!(admission.in_flight.available_permits(), 0);
        let started = Arc::new(AtomicBool::new(false));
        let started_for_task = started.clone();
        let error = await_bucket_usecase_on_fresh_task_with_admission(
            "bucket creation",
            admission.clone(),
            move |mutation_permit| async move {
                let _mutation_permit = mutation_permit;
                started_for_task.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect_err("unfinished hooks consume the total operation budget");
        assert_eq!(error.code(), &S3ErrorCode::SlowDown);
        assert_eq!(error.message(), Some("bucket creation in-flight limit reached; retry later"));
        assert_eq!(error.headers().expect("retry headers")[http::header::RETRY_AFTER], "1");
        assert!(!started.load(Ordering::SeqCst));
        release.add_permits(2);
        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..2 {
                finished_rx.recv().await.expect("detached hook completes");
            }
            while admission.in_flight.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("capacity recovers after hooks complete");
        let permits = admission.acquire("bucket deletion").await.expect("new operation is admitted");
        drop(permits);
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_usecase_admission_times_out_without_starting_work() {
        let admission = Arc::new(BucketOperationAdmission::new(1, 1));
        let held = admission.active.clone().acquire_owned().await.expect("hold active slot");
        let started = Arc::new(AtomicBool::new(false));
        let started_for_task = started.clone();
        let result = await_bucket_usecase_on_fresh_task_with_admission(
            "test bucket operation",
            admission.clone(),
            move |mutation_permit| async move {
                let _mutation_permit = mutation_permit;
                started_for_task.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        let error = result.expect_err("queued request must time out");
        assert_eq!(error.code(), &S3ErrorCode::SlowDown);
        assert_eq!(error.headers().expect("retry headers")[http::header::RETRY_AFTER], "1");
        assert!(!started.load(Ordering::SeqCst), "timed-out work must not start");
        assert_eq!(admission.waiting.available_permits(), 1);
        assert_eq!(admission.in_flight.available_permits(), 2);
        drop(held);
        assert_eq!(admission.active.available_permits(), 1);
    }

    #[tokio::test]
    async fn bucket_usecase_admission_queues_bursts_with_bounded_execution() {
        let admission = Arc::new(BucketOperationAdmission::new(
            BUCKET_OPERATION_CONCURRENCY,
            BUCKET_OPERATION_QUEUE_CAPACITY,
        ));
        let release = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(100);
        for _ in 0..100 {
            let admission_for_task = admission.clone();
            let release_for_task = release.clone();
            let started_for_task = started.clone();
            tasks.push(tokio::spawn(await_bucket_usecase_on_fresh_task_with_admission(
                "test bucket operation",
                admission_for_task,
                move |mutation_permit| async move {
                    let _mutation_permit = mutation_permit;
                    started_for_task.fetch_add(1, Ordering::SeqCst);
                    release_for_task.acquire().await.expect("release gate").forget();
                    Ok(())
                },
            )));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while admission.waiting.available_permits() != BUCKET_OPERATION_QUEUE_CAPACITY - 92
                || started.load(Ordering::SeqCst) != BUCKET_OPERATION_CONCURRENCY
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all excess operations should queue");
        assert_eq!(started.load(Ordering::SeqCst), BUCKET_OPERATION_CONCURRENCY);
        release.add_permits(100);
        for task in tasks {
            task.await.expect("request task joins").expect("burst operation succeeds");
        }
        assert_eq!(started.load(Ordering::SeqCst), 100);
        assert_eq!(admission.active.available_permits(), BUCKET_OPERATION_CONCURRENCY);
        assert_eq!(admission.waiting.available_permits(), BUCKET_OPERATION_QUEUE_CAPACITY);
    }

    #[tokio::test]
    async fn bucket_usecase_admission_bounds_queue_and_cancels_waiters() {
        let admission = Arc::new(BucketOperationAdmission::new(1, 1));
        let held = admission.active.clone().acquire_owned().await.expect("hold active slot");
        let started = Arc::new(AtomicBool::new(false));
        let started_for_task = started.clone();
        let queued = tokio::spawn(await_bucket_usecase_on_fresh_task_with_admission(
            "test bucket operation",
            admission.clone(),
            move |mutation_permit| async move {
                let _mutation_permit = mutation_permit;
                started_for_task.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while admission.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should queue");
        let error = admission
            .acquire("test bucket operation")
            .await
            .expect_err("full queue rejects immediately");
        assert_eq!(error.code(), &S3ErrorCode::SlowDown);
        assert_eq!(error.headers().expect("retry headers")[http::header::RETRY_AFTER], "1");
        let response = error.to_http_response().expect("serialize admission error");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[http::header::RETRY_AFTER], "1");
        use http_body_util::BodyExt as _;
        let body = response.into_body().collect().await.expect("read error XML").to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("<Code>SlowDown</Code>"));
        queued.abort();
        assert!(queued.await.expect_err("queued request was cancelled").is_cancelled());
        assert_eq!(admission.waiting.available_permits(), 1);
        assert_eq!(admission.in_flight.available_permits(), 2);
        drop(held);
        assert!(!started.load(Ordering::SeqCst), "cancelled waiter must not mutate storage");
        await_bucket_usecase_on_fresh_task_with_admission("test bucket operation", admission, |mutation_permit| async move {
            let _mutation_permit = mutation_permit;
            Ok(())
        })
        .await
        .expect("admission recovers after cancellation");
    }

    #[tokio::test]
    async fn bucket_usecase_admission_maps_closed_semaphore_to_internal_error() {
        for close_in_flight in [false, true] {
            let admission = BucketOperationAdmission::new(1, 1);
            if close_in_flight {
                admission.in_flight.close();
            } else {
                admission.active.close();
            }
            let error = admission
                .acquire("test bucket operation")
                .await
                .expect_err("closed admission must fail");
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert_eq!(admission.in_flight.available_permits(), 2);
        }
    }

    #[tokio::test]
    async fn bucket_usecase_task_maps_panics_to_internal_errors() {
        for post_commit in [false, true] {
            let admission = Arc::new(BucketOperationAdmission::new(1, 1));
            let result: S3Result<()> = await_bucket_usecase_on_fresh_task_with_admission(
                "test bucket operation",
                admission.clone(),
                move |mutation_permit| async move {
                    let mut mutation_permit = Some(mutation_permit);
                    if post_commit {
                        drop(mutation_permit.take());
                    }
                    panic!("injected bucket task panic");
                },
            )
            .await;

            let err = result.expect_err("a bucket task panic must be returned as an error");
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
            assert!(err.message().is_some_and(|message| message.contains("task failed")));
            assert_eq!(admission.active.available_permits(), 1);
            assert_eq!(admission.in_flight.available_permits(), 2);
        }
    }

    #[test]
    #[serial_test::serial]
    fn scanner_maintenance_reload_marks_only_scanner_owned_config_changes() {
        const BUCKET: &str = "scanner-maintenance-reload-test";
        rustfs_scanner::clear_dirty_usage_bucket(BUCKET);
        let before = rustfs_scanner::scanner_maintenance_generation();

        record_local_scanner_maintenance_reload(BUCKET, false);
        assert_eq!(rustfs_scanner::scanner_maintenance_generation(), before);

        record_local_scanner_maintenance_reload(BUCKET, true);
        assert_eq!(rustfs_scanner::scanner_maintenance_generation(), before.saturating_add(1));
        rustfs_scanner::clear_dirty_usage_bucket(BUCKET);
    }

    #[test]
    fn build_request_with_req_info_preserves_owner_state() {
        let input = CreateBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();
        let req = build_request_with_req_info(
            input,
            Method::PUT,
            ReqInfo {
                is_owner: true,
                ..Default::default()
            },
        );

        assert!(req_info_ref(&req).expect("req info should be present").is_owner);
    }

    #[tokio::test]
    async fn execute_list_buckets_returns_internal_error_when_store_uninitialized() {
        let req = build_request((), Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase
            .execute_list_buckets(RequestEnvelope::from_s3s(&req), gateway_dto::ListBucketsInput::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
    }
}
