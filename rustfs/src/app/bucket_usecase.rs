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
use super::storage_api::managed;
#[cfg(test)]
use super::storage_api::managed::{
    AccountNamespaceId, BucketHandle, BucketIdentityError, BucketIncarnationFenceGuard, BucketKind, BucketPublicName,
    StorageBucketId,
};

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
        BUCKET_ACCELERATE_CONFIG, BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_SSECONFIG,
        BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, BUCKET_WEBSITE_CONFIG,
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
    ListObjectVersionsParams, ListObjectsV2Params, encode_list_objects_v2_continuation_token, parse_list_object_versions_params,
    parse_list_objects_v2_params, rustfs_owner,
};
use super::storage_api::bucket_usecase::{get_validated_store, request_context, validate_list_object_unordered_with_delimiter};
#[cfg(feature = "diagnostic-read-version-caller")]
use super::storage_api::diagnostic;
use crate::app::RequestEnvelope;
use crate::app::bucket_list_through as list_through;
use crate::app::dto_values;
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
use rustfs_utils::http::get_header;
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
#[cfg(test)]
mod delete_config_tests;
mod encryption;
#[cfg(test)]
mod guards;
mod lifecycle;
pub(crate) use lifecycle::{PreparedLifecycleConfiguration, validate_lifecycle_rule_status};
mod list_object_versions;
pub(crate) mod list_objects;
mod listing;
mod logging;
mod notification;
pub(crate) use notification::NotificationDestination;
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

#[cfg(test)]
type BucketMetadataReloadObserver = Box<dyn Fn(bool, &'static str, bool) + Send + Sync>;

#[cfg(test)]
tokio::task_local! {
    static BUCKET_METADATA_RELOAD_PROBE: (String, BucketMetadataReloadObserver);
}

#[cfg(test)]
fn record_bucket_metadata_reload(bucket: &str, completed: bool, operation: &'static str, scanner_maintenance_change: bool) {
    let _ = BUCKET_METADATA_RELOAD_PROBE.try_with(|(observed_bucket, record)| {
        if observed_bucket == bucket {
            record(completed, operation, scanner_maintenance_change);
        }
    });
}

async fn notify_bucket_metadata_reload_with_context(
    bucket: String,
    operation: &'static str,
    request_context: Option<request_context::RequestContext>,
    scanner_maintenance_change: bool,
    app_context: Option<Arc<AppContext>>,
) {
    #[cfg(test)]
    record_bucket_metadata_reload(&bucket, false, operation, scanner_maintenance_change);
    #[cfg(test)]
    let observed_bucket = bucket.clone();

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
    let completed = completed_rx.await;
    #[cfg(test)]
    if completed.is_ok() {
        record_bucket_metadata_reload(&observed_bucket, true, operation, scanner_maintenance_change);
    }
    let _ = completed;
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

#[cfg(test)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum BucketResolveError {
    #[error(transparent)]
    InvalidIdentity(#[from] BucketIdentityError),
    #[error("bucket identity is missing")]
    MissingIdentity,
    #[error("bucket kind differs from the requested kind")]
    KindMismatch,
    #[error("bucket identity differs from the requested identity")]
    IdentityMismatch,
    #[error("managed bucket deployment capability is unavailable")]
    CapabilityUnavailable,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[cfg(test)]
pub(crate) fn vector_storage_bucket_id(
    account: &AccountNamespaceId,
    region: &str,
    public_name: &BucketPublicName,
) -> Result<StorageBucketId, BucketResolveError> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"rustfs.vector-bucket.v1");
    for value in [account.as_str(), region, public_name.as_str()] {
        let length = u32::try_from(value.len()).map_err(|_| BucketIdentityError::InvalidPublicName)?;
        digest.update(length.to_be_bytes());
        digest.update(value.as_bytes());
    }
    let digest = digest.finalize();
    let mut name = String::from(managed::BUCKET_VECTOR_STORAGE_PREFIX);
    for byte in &digest[..24] {
        use std::fmt::Write;
        write!(&mut name, "{byte:02x}").map_err(|_| BucketIdentityError::InvalidStorageBucketId)?;
    }
    Ok(StorageBucketId::try_new(name)?)
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
    fn validate_managed_bucket_deployment(&self, account: &AccountNamespaceId, region: &str) -> Result<(), BucketResolveError> {
        if self.context.is_none() {
            return Err(BucketResolveError::CapabilityUnavailable);
        }
        let current_region = self
            .global_region()
            .map(|region| region.to_string())
            .unwrap_or_else(|| RUSTFS_REGION.to_owned());
        if !account.as_str().is_empty() || region != current_region {
            return Err(BucketResolveError::IdentityMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn checked_storage_bucket_id(
        &self,
        kind: BucketKind,
        account: &AccountNamespaceId,
        region: &str,
        public_name: &BucketPublicName,
    ) -> Result<StorageBucketId, BucketResolveError> {
        self.validate_managed_bucket_deployment(account, region)?;
        if kind == BucketKind::Vector {
            let name = public_name.as_str().as_bytes();
            if !(3..=63).contains(&name.len())
                || !name
                    .iter()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
                || !name.first().is_some_and(u8::is_ascii_alphanumeric)
                || !name.last().is_some_and(u8::is_ascii_alphanumeric)
            {
                return Err(BucketIdentityError::InvalidPublicName.into());
            }
            vector_storage_bucket_id(account, region, public_name)
        } else {
            Ok(StorageBucketId::try_new(public_name.as_str().to_owned())?)
        }
    }

    #[cfg(test)]
    pub(crate) async fn resolve_bucket_handle(
        &self,
        kind: BucketKind,
        account_namespace_id: &AccountNamespaceId,
        region: &str,
        public_name: &BucketPublicName,
    ) -> Result<BucketHandle, BucketResolveError> {
        let storage_id = self.checked_storage_bucket_id(kind, account_namespace_id, region, public_name)?;
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        let metadata = managed::get_bucket_metadata(&store, storage_id.as_str())
            .await
            .map_err(|error| match error {
                StorageError::ConfigNotFound => BucketResolveError::MissingIdentity,
                other => BucketResolveError::Storage(other),
            })?;
        if metadata.bucket_kind()? != kind {
            return Err(BucketResolveError::KindMismatch);
        }
        let handle = metadata.bucket_handle(account_namespace_id, region)?;
        if handle.public_name() != public_name || handle.storage_bucket_id() != &storage_id {
            return Err(BucketResolveError::IdentityMismatch);
        }
        self.validate_bucket_handle(&handle).await?;
        Ok(handle)
    }

    /// Creation retains the existing lifecycle/metadata/namespace lock order and EC commit behavior.
    #[cfg(test)]
    pub(crate) async fn create_managed_bucket(
        &self,
        identity: BucketHandle,
        encryption_config_xml: Vec<u8>,
        tagging_config_xml: Vec<u8>,
    ) -> Result<BucketHandle, BucketResolveError> {
        if identity.kind() != BucketKind::Vector {
            return Err(BucketResolveError::KindMismatch);
        }
        let expected_storage = self.checked_storage_bucket_id(
            identity.kind(),
            identity.account_namespace_id(),
            identity.region(),
            identity.public_name(),
        )?;
        if &expected_storage != identity.storage_bucket_id() {
            return Err(BucketResolveError::IdentityMismatch);
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        match managed::make_managed_bucket(
            &store,
            &identity,
            &encryption_config_xml,
            &tagging_config_xml,
            &MakeBucketOptions::default(),
        )
        .await
        {
            Ok(()) => self.validate_bucket_handle(&identity).await?,
            Err(StorageError::PreconditionFailed) => {
                let _guard = self.acquire_bucket_handle_guard(&identity).await?;
                let metadata = managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str()).await?;
                if metadata.encryption_config_xml != encryption_config_xml || metadata.tagging_config_xml != tagging_config_xml {
                    return Err(BucketResolveError::IdentityMismatch);
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(identity)
    }

    #[cfg(test)]
    pub(crate) async fn acquire_bucket_handle_guard(
        &self,
        expected: &BucketHandle,
    ) -> Result<BucketIncarnationFenceGuard, BucketResolveError> {
        if self.context.is_none() {
            return Err(BucketResolveError::CapabilityUnavailable);
        }
        if expected.kind() == BucketKind::Vector {
            let storage_id = self.checked_storage_bucket_id(
                expected.kind(),
                expected.account_namespace_id(),
                expected.region(),
                expected.public_name(),
            )?;
            if &storage_id != expected.storage_bucket_id() {
                return Err(BucketResolveError::IdentityMismatch);
            }
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        let guard = managed::acquire_bucket_incarnation_fence(
            &store,
            expected.storage_bucket_id().as_str(),
            expected.incarnation().as_uuid(),
        )
        .await?;
        let metadata = managed::get_bucket_metadata(&store, expected.storage_bucket_id().as_str()).await?;
        metadata.validate_bucket_handle(expected)?;
        if guard.is_lock_lost() {
            return Err(StorageError::PreconditionFailed.into());
        }
        Ok(guard)
    }

    #[cfg(test)]
    /// Update one authorized Vector bucket configuration through its exact store.
    ///
    /// The caller owns the accepted operation through storage and Vector audit completion.
    /// It must not retain a lifecycle guard for this bucket.
    ///
    /// # Errors
    /// Rejects a different kind, region, storage identity, or unavailable instance.
    /// The storage boundary validates the full persisted identity under its write guards.
    pub(crate) async fn mutate_managed_bucket_config(
        &self,
        identity: &BucketHandle,
        mutation: managed::ManagedBucketConfigMutation,
    ) -> Result<time::OffsetDateTime, BucketResolveError> {
        if identity.kind() != BucketKind::Vector {
            return Err(BucketResolveError::KindMismatch);
        }
        let expected_storage = self.checked_storage_bucket_id(
            identity.kind(),
            identity.account_namespace_id(),
            identity.region(),
            identity.public_name(),
        )?;
        if &expected_storage != identity.storage_bucket_id() {
            return Err(BucketResolveError::IdentityMismatch);
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        Ok(managed::mutate_managed_bucket_config(&store, identity, mutation).await?)
    }

    /// The owned Vector execution authorizes deletion before releasing its read guard.
    #[cfg(all(test, feature = "gateway-stack"))]
    pub(crate) async fn delete_managed_bucket(&self, identity: &BucketHandle) -> Result<(), BucketResolveError> {
        if identity.kind() != BucketKind::Vector {
            return Err(BucketResolveError::KindMismatch);
        }
        let expected_storage = self.checked_storage_bucket_id(
            identity.kind(),
            identity.account_namespace_id(),
            identity.region(),
            identity.public_name(),
        )?;
        if &expected_storage != identity.storage_bucket_id() {
            return Err(BucketResolveError::IdentityMismatch);
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        let catalog = crate::table_catalog::ObjectTableCatalogStore::for_vector_scope(
            crate::table_catalog::EcStoreTableCatalogObjectBackend::new(store.clone()),
            identity.clone(),
        )
        .await
        .map_err(StorageError::other)?;
        let prepared_identity = identity.clone();
        managed::delete_managed_bucket_with_preparation(&store, identity, move |guard| async move {
            catalog
                .prepare_empty_vector_bucket_delete(&prepared_identity, guard)
                .await
                .map_err(StorageError::other)
        })
        .await?;
        Ok(())
    }

    /// The service retains its admitted lifecycle guard and owns Vector authorization and audit.
    #[cfg(test)]
    pub(crate) async fn read_managed_bucket_metadata(
        &self,
        expected: &BucketHandle,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<Arc<managed::BucketMetadata>, BucketResolveError> {
        use rustfs_policy::policy::Validator;
        if expected.kind() != BucketKind::Vector {
            return Err(BucketResolveError::KindMismatch);
        }
        let storage_id = self.checked_storage_bucket_id(
            expected.kind(),
            expected.account_namespace_id(),
            expected.region(),
            expected.public_name(),
        )?;
        if &storage_id != expected.storage_bucket_id() {
            return Err(BucketResolveError::IdentityMismatch);
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        managed::validate_bucket_incarnation_fence(&store, guard, storage_id.as_str())?;
        let metadata = managed::get_bucket_metadata(&store, storage_id.as_str()).await?;
        if let Some(policy) = managed::ConfigState::of(&metadata.policy_config_json, &metadata.policy_config)
            .require(storage_id.as_str(), BUCKET_POLICY_CONFIG)?
        {
            policy.is_valid().map_err(StorageError::other)?;
        }
        managed::ConfigState::of(&metadata.encryption_config_xml, &metadata.sse_config)
            .require(storage_id.as_str(), BUCKET_SSECONFIG)?;
        managed::ConfigState::of(&metadata.tagging_config_xml, &metadata.tagging_config)
            .require(storage_id.as_str(), managed::BUCKET_TAGGING_CONFIG)?;
        metadata.validate_bucket_handle(expected)?;
        if guard.is_lock_lost() {
            return Err(StorageError::PreconditionFailed.into());
        }
        Ok(metadata)
    }

    /// Returns current validated snapshots for an already-authorized Vector list request.
    /// Later resource operations must reacquire their protection and resolve current authorization.
    #[cfg(test)]
    pub(crate) async fn list_managed_buckets(
        &self,
        account: &AccountNamespaceId,
        region: &str,
    ) -> Result<Vec<(BucketHandle, Arc<managed::BucketMetadata>)>, BucketResolveError> {
        self.validate_managed_bucket_deployment(account, region)?;
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        let buckets = store
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await?;
        let mut snapshots = Vec::new();
        for bucket in buckets {
            if !bucket.name.starts_with(managed::BUCKET_VECTOR_STORAGE_PREFIX) {
                continue;
            }
            let metadata = managed::get_bucket_metadata(&store, &bucket.name).await?;
            let identity = metadata.bucket_handle(account, region)?;
            let guard = managed::acquire_bucket_incarnation_fence(&store, &bucket.name, identity.incarnation().as_uuid()).await?;
            let snapshot = self.read_managed_bucket_metadata(&identity, &guard).await?;
            snapshots.push((identity, snapshot));
        }
        snapshots.sort_by(|left, right| left.0.public_name().as_str().cmp(right.0.public_name().as_str()));
        Ok(snapshots)
    }

    #[cfg(test)]
    pub(crate) async fn validate_bucket_handle(&self, expected: &BucketHandle) -> Result<(), BucketResolveError> {
        let _guard = self.acquire_bucket_handle_guard(expected).await?;
        Ok(())
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
            .filter(|info| !info.name.starts_with(managed::BUCKET_VECTOR_STORAGE_PREFIX))
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
    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_config_mutation_retains_locks_after_caller_cancellation() {
        use crate::app::object::ManagedEncryption;
        use crate::app::storage_api::bucket_usecase::helper::run_owned_mutation;
        use crate::app::storage_api::object_usecase::contract::namespace::NamespaceLocking as _;
        use crate::app::storage_api::test::{
            RUSTFS_META_BUCKET,
            metadata_sys::ConfigWriteLockProbe,
            set_disk::{PutObjectCommitBarrier, PutObjectCommitPause},
        };

        let (request, access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let context = request
            .server_context()
            .expect("server")
            .installed_app_context()
            .expect("context");
        let usecase = DefaultBucketUsecase::with_context(Some(context.clone()));
        let identity = access.bucket().clone();
        let bucket = identity.storage_bucket_id().as_str();
        let original = managed::get_bucket_metadata(&store, bucket).await.expect("original metadata");
        let barrier = PutObjectCommitBarrier::install(
            RUSTFS_META_BUCKET,
            &original.save_file_path(),
            PutObjectCommitPause::AfterRenameQuorum,
        );
        let policy = br#"{"Version":"2012-10-17","Statement":[]}"#.to_vec();
        let mutation_identity = identity.clone();
        let mutation_policy = policy.clone();
        let mutation_usecase = DefaultBucketUsecase::with_context(Some(context.clone()));
        let (completed, completion) = tokio::sync::oneshot::channel();
        let mut caller = tokio::spawn(async move {
            run_owned_mutation(async move {
                let result = mutation_usecase
                    .mutate_managed_bucket_config(
                        &mutation_identity,
                        managed::ManagedBucketConfigMutation::PutPolicy {
                            policy_json: mutation_policy,
                        },
                    )
                    .await
                    .map_err(to_internal_error);
                completed.send(result.is_ok()).expect("storage completion observer");
                result
            })
            .await
        });
        tokio::select! {
            () = barrier.wait_until_paused() => {}
            result = &mut caller => panic!("owned mutation must reach persistence: {result:?}"),
        }
        caller.abort();
        assert!(caller.await.expect_err("cancel HTTP waiter").is_cancelled());
        // Prove the owner's lifecycle coverage before another config reader can mask its loss.
        // Cancel this probe before starting that reader because queued writers take precedence.
        {
            let manager = crate::app::storage_api::test::bootstrap_instance_ctx().lock_manager();
            let manager = manager.as_fast_lock_manager().expect("fixture lock manager");
            let waiters_before = manager.current_lock_counts().1;
            let lock = store.new_ns_lock(bucket, "").await.expect("owner lifecycle probe");
            let acquire = lock.get_write_lock(Duration::from_secs(5));
            tokio::pin!(acquire);
            std::future::poll_fn(|cx| match std::future::Future::poll(acquire.as_mut(), cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => panic!("the detached owner must still hold its lifecycle fence: {result:?}"),
            })
            .await;
            assert!(
                manager.current_lock_counts().1 > waiters_before,
                "the lifecycle writer entered the actual wait queue"
            );
        }
        let probe = ConfigWriteLockProbe::install(bucket);
        let tag_identity = identity.clone();
        let tag_usecase = DefaultBucketUsecase::with_context(Some(context));
        let mut tag_writer = tokio::spawn(async move {
            tag_usecase
                .mutate_managed_bucket_config(
                    &tag_identity,
                    managed::ManagedBucketConfigMutation::Tag {
                        tags: std::collections::BTreeMap::from([("queued".to_owned(), "tag".to_owned())]),
                    },
                )
                .await
        });
        tokio::select! {
            () = probe.wait_until_attempted() => {}
            result = &mut tag_writer => panic!("metadata writer must remain queued after caller cancellation: {result:?}"),
        }
        let lifecycle_lock = store.new_ns_lock(bucket, "").await.expect("lifecycle lock");
        let (pending, queued) = tokio::sync::oneshot::channel();
        let (acquired, acquisition) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let lifecycle_writer = tokio::spawn(async move {
            let acquire = lifecycle_lock.get_write_lock(Duration::from_secs(5));
            tokio::pin!(acquire);
            let mut pending = Some(pending);
            let guard = std::future::poll_fn(|cx| match std::future::Future::poll(acquire.as_mut(), cx) {
                std::task::Poll::Pending => {
                    if let Some(pending) = pending.take() {
                        pending.send(()).expect("record lifecycle wait");
                    }
                    std::task::Poll::Pending
                }
                ready => ready,
            })
            .await
            .expect("lifecycle writer eventually acquires");
            acquired.send(()).expect("record lifecycle acquisition");
            released.await.expect("release lifecycle writer");
            drop(guard);
        });
        tokio::time::timeout(Duration::from_secs(5), queued)
            .await
            .expect("lifecycle writer reaches Pending")
            .expect("pending observer");
        assert!(!tag_writer.is_finished(), "the detached owner retains the metadata transaction");
        assert!(!lifecycle_writer.is_finished(), "the detached owner retains the lifecycle fence");
        drop(barrier);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), completion)
                .await
                .expect("detached storage completes")
                .expect("storage completion result")
        );
        tokio::time::timeout(Duration::from_secs(5), tag_writer)
            .await
            .expect("queued tag writer resumes")
            .expect("tag task")
            .expect("tag write");
        tokio::time::timeout(Duration::from_secs(5), acquisition)
            .await
            .expect("lifecycle writer resumes")
            .expect("acquisition observer");
        let final_metadata = managed::get_bucket_metadata(&store, bucket)
            .await
            .expect("committed metadata");
        assert_eq!(final_metadata.policy_config_json, policy);
        assert_eq!(final_metadata.bucket_identity_config_json, original.bucket_identity_config_json);
        assert_eq!(final_metadata.encryption_config_xml, original.encryption_config_xml);
        assert_eq!(
            final_metadata
                .tagging_config
                .as_ref()
                .expect("tag configuration")
                .tag_set
                .len(),
            1
        );
        release.send(()).expect("release writer");
        lifecycle_writer.await.expect("lifecycle writer completes");
        let guard = tokio::time::timeout(Duration::from_secs(5), usecase.acquire_bucket_handle_guard(&identity))
            .await
            .expect("all fences released")
            .expect("same committed identity");
        drop(guard);
    }

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

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_list_buckets_hides_managed_storage_from_owner() {
        crate::app::gating_test_env::shared_gating_ambient().await;
        let (request, access, store, _reset) = crate::app::object::managed_request_fixture().await;
        let server = request.server_context().expect("server context").clone();
        let context = server.installed_app_context().expect("installed context");
        let general = format!("general-list-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&general, &MakeBucketOptions::default())
            .await
            .expect("ordinary bucket");
        let mut request = build_request_with_req_info(
            (),
            Method::GET,
            ReqInfo {
                cred: Some(rustfs_credentials::Credentials {
                    access_key: "managed-list-owner".into(),
                    ..Default::default()
                }),
                is_owner: true,
                ..Default::default()
            },
        );
        request.credentials = Some(s3s::auth::Credentials {
            access_key: "managed-list-owner".into(),
            secret_key: "managed-list-test-secret".into(),
        });
        request.extensions.insert(server);
        let output = DefaultBucketUsecase::with_context(Some(context))
            .execute_list_buckets(RequestEnvelope::from_s3s(&request), gateway_dto::ListBucketsInput::default())
            .await
            .expect("owner bucket listing");
        assert!(output.buckets.iter().any(|bucket| bucket.name.as_str() == general));
        assert!(
            output
                .buckets
                .iter()
                .all(|bucket| bucket.name.as_str() != access.bucket().storage_bucket_id().as_str())
        );
    }

    #[test]
    fn vector_storage_id_uses_versioned_length_prefixed_identity() {
        let account = AccountNamespaceId::empty();
        let public_name = BucketPublicName::try_new("my-vectors".into()).expect("public name");
        let id = vector_storage_bucket_id(&account, "us-east-1", &public_name).expect("storage identity");
        assert_eq!(id.as_str(), "rustfs-v-5116163f2a2e3c469d61cc3623014d1bf542342a26548465");
        assert_eq!(id.as_str().len(), 57);
        assert_ne!(id, vector_storage_bucket_id(&account, "us-west-2", &public_name).expect("other region"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_resolver_checks_instance_region_kind_and_identity() {
        let (request, access, store, _reset) = crate::app::object::managed_request_fixture().await;
        let context = request
            .server_context()
            .expect("server")
            .installed_app_context()
            .expect("context");
        let usecase = DefaultBucketUsecase::with_context(Some(context));
        let identity = access.bucket();
        let account = AccountNamespaceId::empty();
        let resolved = usecase
            .resolve_bucket_handle(BucketKind::Vector, &account, identity.region(), identity.public_name())
            .await
            .expect("real committed binding");
        assert_eq!(&resolved, identity);
        assert!(matches!(
            usecase
                .resolve_bucket_handle(BucketKind::Vector, &account, "another-region", identity.public_name())
                .await,
            Err(BucketResolveError::IdentityMismatch)
        ));
        let internal_name = BucketPublicName::try_new(identity.storage_bucket_id().as_str().into()).expect("internal name");
        assert!(matches!(
            usecase
                .resolve_bucket_handle(BucketKind::General, &account, identity.region(), &internal_name)
                .await,
            Err(BucketResolveError::KindMismatch)
        ));
        for name in ["Abc", "abc.def", "-abc", "abc-", "ab"] {
            let name = BucketPublicName::try_new(name.into()).expect("basic name syntax");
            assert!(matches!(
                usecase
                    .resolve_bucket_handle(BucketKind::Vector, &account, identity.region(), &name)
                    .await,
                Err(BucketResolveError::InvalidIdentity(BucketIdentityError::InvalidPublicName))
            ));
        }
        let missing = BucketPublicName::try_new(format!("absent-{}", uuid::Uuid::new_v4().simple())).expect("missing name");
        assert!(matches!(
            usecase
                .resolve_bucket_handle(BucketKind::Vector, &account, identity.region(), &missing)
                .await,
            Err(BucketResolveError::MissingIdentity)
        ));
        let general = BucketPublicName::try_new(format!("legacy-{}", uuid::Uuid::new_v4().simple())).expect("general name");
        store
            .make_bucket(general.as_str(), &MakeBucketOptions::default())
            .await
            .expect("ordinary bucket");
        assert_eq!(
            usecase
                .resolve_bucket_handle(BucketKind::General, &account, identity.region(), &general)
                .await
                .expect("ordinary identity")
                .kind(),
            BucketKind::General
        );
        assert!(matches!(
            DefaultBucketUsecase::without_context().validate_bucket_handle(identity).await,
            Err(BucketResolveError::CapabilityUnavailable)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_create_retries_exact_identity_and_rejects_conflicts() {
        use crate::app::storage_api::managed::BucketIncarnation;
        let (request, access, store, _reset) = crate::app::object::managed_request_fixture().await;
        let context = request
            .server_context()
            .expect("server")
            .installed_app_context()
            .expect("context");
        let usecase = DefaultBucketUsecase::with_context(Some(context));
        let metadata = managed::get_bucket_metadata(&store, access.bucket().storage_bucket_id().as_str())
            .await
            .expect("SSE configuration");
        let name = BucketPublicName::try_new(format!("created-{}", uuid::Uuid::new_v4().simple())).expect("name");
        let account = AccountNamespaceId::empty();
        let storage_id = vector_storage_bucket_id(&account, access.bucket().region(), &name).expect("storage name");
        let identity = BucketHandle::try_from_parts(
            BucketKind::Vector,
            account,
            access.bucket().region().into(),
            name,
            storage_id,
            BucketIncarnation::try_new(uuid::Uuid::new_v4()).expect("incarnation"),
        )
        .expect("identity");
        for _ in 0..2 {
            assert_eq!(
                usecase
                    .create_managed_bucket(identity.clone(), metadata.encryption_config_xml.clone(), Vec::new())
                    .await
                    .expect("exact creation retry"),
                identity
            );
        }
        let conflicting = BucketHandle::try_from_parts(
            identity.kind(),
            identity.account_namespace_id().clone(),
            identity.region().into(),
            identity.public_name().clone(),
            identity.storage_bucket_id().clone(),
            BucketIncarnation::try_new(uuid::Uuid::new_v4()).expect("different incarnation"),
        )
        .expect("conflicting identity");
        assert!(
            usecase
                .create_managed_bucket(conflicting, metadata.encryption_config_xml.clone(), Vec::new())
                .await
                .is_err()
        );
        assert!(
            usecase
                .create_managed_bucket(identity.clone(), Vec::new(), Vec::new())
                .await
                .is_err()
        );
        assert_eq!(
            usecase
                .resolve_bucket_handle(
                    identity.kind(),
                    identity.account_namespace_id(),
                    identity.region(),
                    identity.public_name()
                )
                .await
                .expect("original survives"),
            identity
        );
    }
    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_read_returns_exact_configuration_snapshot() {
        use crate::app::object::ManagedEncryption;
        for encryption in [ManagedEncryption::Unencrypted, ManagedEncryption::Aes256] {
            let (request, access, store, _reset) =
                crate::app::object::managed_request_fixture_with_encryption(encryption.clone()).await;
            let usecase = DefaultBucketUsecase::with_context(request.server_context().unwrap().installed_app_context());
            let identity = access.bucket();
            let mut stored = managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str())
                .await
                .unwrap()
                .as_ref()
                .clone();
            stored.policy_config_json = br#"{"Version":"2012-10-17","Statement":[]}"#.to_vec();
            stored.tagging_config_xml =
                b"<Tagging><TagSet><Tag><Key>purpose</Key><Value>vectors</Value></Tag></TagSet></Tagging>".to_vec();
            stored.save_with_store(store.clone()).await.unwrap();
            let guard = usecase.acquire_bucket_handle_guard(identity).await.unwrap();
            let snapshot = usecase
                .read_managed_bucket_metadata(identity, &guard)
                .await
                .expect("exact managed snapshot");
            assert_eq!(
                snapshot
                    .bucket_handle(identity.account_namespace_id(), identity.region())
                    .unwrap(),
                *identity
            );
            assert_eq!(snapshot.created, stored.created);
            assert_eq!(snapshot.policy_config_json, stored.policy_config_json);
            assert!(snapshot.policy_config.is_some());
            assert_eq!(snapshot.tagging_config_xml, stored.tagging_config_xml);
            let tags = snapshot.tagging_config.as_ref().expect("parsed tags");
            assert_eq!(tags.tag_set.len(), 1);
            assert_eq!(tags.tag_set[0].key.as_deref(), Some("purpose"));
            assert_eq!(tags.tag_set[0].value.as_deref(), Some("vectors"));
            assert_eq!(snapshot.encryption_config_xml, stored.encryption_config_xml);
            assert_eq!(snapshot.sse_config.is_some(), encryption == ManagedEncryption::Aes256);
            assert!(!guard.is_lock_lost());
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_read_rejects_stale_identity_and_foreign_store_guard() {
        use crate::app::object::ManagedEncryption;
        use crate::app::storage_api::managed::BucketIncarnation;
        let (request, access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let context = request.server_context().unwrap().installed_app_context().unwrap();
        let usecase = DefaultBucketUsecase::with_context(Some(context.clone()));
        let identity = access.bucket();
        let guard = usecase.acquire_bucket_handle_guard(identity).await.unwrap();
        let stale = BucketHandle::try_from_parts(
            identity.kind(),
            identity.account_namespace_id().clone(),
            identity.region().to_owned(),
            identity.public_name().clone(),
            identity.storage_bucket_id().clone(),
            BucketIncarnation::try_new(uuid::Uuid::new_v4()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            usecase.read_managed_bucket_metadata(&stale, &guard).await,
            Err(BucketResolveError::Storage(_))
        ));
        let (_directory, _paths, other_store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let other_context = Arc::new(AppContext::new(other_store.clone(), context.iam(), context.kms()));
        let other_usecase = DefaultBucketUsecase::with_context(Some(other_context));
        let tags = b"<Tagging><TagSet><Tag><Key>instance</Key><Value>other</Value></Tag></TagSet></Tagging>".to_vec();
        other_usecase
            .create_managed_bucket(identity.clone(), Vec::new(), tags.clone())
            .await
            .unwrap();
        assert!(matches!(
            other_usecase.read_managed_bucket_metadata(identity, &guard).await,
            Err(BucketResolveError::Storage(_))
        ));
        let other_guard = other_usecase.acquire_bucket_handle_guard(identity).await.unwrap();
        let other = other_usecase
            .read_managed_bucket_metadata(identity, &other_guard)
            .await
            .expect("own instance guard");
        assert_eq!(other.tagging_config_xml, tags);
        let original = usecase.read_managed_bucket_metadata(identity, &guard).await.unwrap();
        assert!(original.tagging_config_xml.is_empty());
        let listed = other_usecase
            .list_managed_buckets(identity.account_namespace_id(), identity.region())
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(&listed[0].0, identity);
        assert_eq!(listed[0].1.tagging_config_xml, tags);
        assert!(
            managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str())
                .await
                .unwrap()
                .tagging_config_xml
                .is_empty()
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_read_reuses_guard_with_waiting_bucket_delete() {
        use crate::app::object::ManagedEncryption;
        let (request, access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let usecase = DefaultBucketUsecase::with_context(request.server_context().unwrap().installed_app_context());
        let identity = access.bucket();
        let guard = usecase.acquire_bucket_handle_guard(identity).await.unwrap();
        let lock_counts = || {
            crate::app::storage_api::test::bootstrap_instance_ctx()
                .lock_manager()
                .as_fast_lock_manager()
                .expect("fixture lock manager")
                .current_lock_counts()
        };
        let before = lock_counts().1;
        let bucket = identity.storage_bucket_id().as_str().to_owned();
        let writer = tokio::spawn(async move { store.delete_bucket(&bucket, &DeleteBucketOptions::default()).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while lock_counts().1 <= before {
                assert!(!writer.is_finished(), "bucket writer must reach the lock queue");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bucket writer queues");
        let snapshot = tokio::time::timeout(Duration::from_secs(5), usecase.read_managed_bucket_metadata(identity, &guard))
            .await
            .expect("metadata read reuses the admitted lifecycle guard")
            .expect("guarded metadata");
        assert_eq!(
            snapshot
                .bucket_handle(identity.account_namespace_id(), identity.region())
                .unwrap(),
            *identity
        );
        assert!(!writer.is_finished());
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), writer)
            .await
            .unwrap()
            .unwrap()
            .expect("empty bucket deletion finishes");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_list_returns_only_validated_vector_snapshots() {
        use crate::app::object::ManagedEncryption;
        use crate::app::storage_api::managed::BucketIncarnation;
        let (request, access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let usecase = DefaultBucketUsecase::with_context(request.server_context().unwrap().installed_app_context());
        let first = access.bucket();
        let public_name = BucketPublicName::try_new(format!("aaa-vector-{}", uuid::Uuid::new_v4().simple())).unwrap();
        let second = BucketHandle::try_from_parts(
            BucketKind::Vector,
            first.account_namespace_id().clone(),
            first.region().to_owned(),
            public_name.clone(),
            vector_storage_bucket_id(first.account_namespace_id(), first.region(), &public_name).unwrap(),
            BucketIncarnation::try_new(uuid::Uuid::new_v4()).unwrap(),
        )
        .unwrap();
        usecase
            .create_managed_bucket(second.clone(), Vec::new(), Vec::new())
            .await
            .unwrap();
        let general = format!("general-{}", uuid::Uuid::new_v4().simple());
        store.make_bucket(&general, &MakeBucketOptions::default()).await.unwrap();
        let listed = usecase
            .list_managed_buckets(first.account_namespace_id(), first.region())
            .await
            .expect("managed listing");
        assert!(listed.iter().any(|(identity, _)| identity == first));
        assert!(listed.iter().any(|(identity, _)| identity == &second));
        assert!(
            listed
                .windows(2)
                .all(|pair| pair[0].0.public_name().as_str() < pair[1].0.public_name().as_str())
        );
        for (identity, snapshot) in &listed {
            assert_eq!(identity.kind(), BucketKind::Vector);
            assert_ne!(identity.storage_bucket_id().as_str(), general);
            snapshot.validate_bucket_handle(identity).unwrap();
            assert_eq!(snapshot.name, identity.storage_bucket_id().as_str());
        }
        assert!(matches!(
            usecase
                .list_managed_buckets(first.account_namespace_id(), "another-region")
                .await,
            Err(BucketResolveError::IdentityMismatch)
        ));
        assert!(matches!(
            DefaultBucketUsecase::without_context()
                .list_managed_buckets(first.account_namespace_id(), first.region())
                .await,
            Err(BucketResolveError::CapabilityUnavailable)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bucket_read_and_list_reject_unreadable_metadata() {
        use crate::app::object::ManagedEncryption;
        let (request, access, store, _reset) =
            crate::app::object::managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let usecase = DefaultBucketUsecase::with_context(request.server_context().unwrap().installed_app_context());
        let identity = access.bucket();
        let guard = usecase.acquire_bucket_handle_guard(identity).await.unwrap();
        let original = managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str())
            .await
            .unwrap();
        for config in ["policy", "policy-version", "encryption", "tags", "binding"] {
            let mut corrupt = original.as_ref().clone();
            match config {
                "policy" => corrupt.policy_config_json = b"broken policy".to_vec(),
                "policy-version" => corrupt.policy_config_json = br#"{"Version":"invalid","Statement":[]}"#.to_vec(),
                "encryption" => corrupt.encryption_config_xml = b"<broken>".to_vec(),
                "tags" => corrupt.tagging_config_xml = b"<broken>".to_vec(),
                "binding" => {
                    let mut record: serde_json::Value = serde_json::from_slice(&corrupt.bucket_identity_config_json).unwrap();
                    record["binding"]["public_name"] = serde_json::json!("another-vector-name");
                    corrupt.bucket_identity_config_json = serde_json::to_vec(&record).unwrap();
                }
                _ => unreachable!(),
            }
            corrupt
                .save_with_store(store.clone())
                .await
                .expect("persist exact corrupt bytes");
            let read = usecase.read_managed_bucket_metadata(identity, &guard).await;
            let listed = usecase
                .list_managed_buckets(identity.account_namespace_id(), identity.region())
                .await;
            let mut restored = original.as_ref().clone();
            restored.save_with_store(store.clone()).await.unwrap();
            assert!(
                matches!(read, Err(BucketResolveError::Storage(_))),
                "{config}: unreadable metadata must fail"
            );
            assert!(
                matches!(listed, Err(BucketResolveError::Storage(_) | BucketResolveError::IdentityMismatch)),
                "{config}: listing must preserve the error"
            );
        }
        usecase
            .read_managed_bucket_metadata(identity, &guard)
            .await
            .expect("restored metadata is readable");
    }
}
