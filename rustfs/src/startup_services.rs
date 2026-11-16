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

use crate::site_replication_reconcile::spawn_site_replication_reconcile_task;
use crate::storage_api::startup::services::{ECStore, EndpointServerPools, ServerContextSlot, StorageAdminApi};
use crate::{
    config::Config,
    connect::{
        CoarseNodeSummary, HeartbeatConfig, HeartbeatError, HeartbeatRuntime, InventoryError, InventoryFlag, InventoryRuntime,
        InventorySchedule, InventorySnapshot, LocalTraceCaptureRuntime, runtime::heartbeat_failure_reason,
        spawn_heartbeat_runtime, spawn_inventory_runtime, spawn_local_trace_capture_runtime,
    },
    init::{init_buffer_profile_system, init_kms_system},
    server::ServiceStateManager,
    startup_audit::init_audit_runtime,
    startup_auth::init_auth_integrations,
    startup_background::init_background_service_runtime,
    startup_bucket_metadata::{init_bucket_metadata_runtime, init_embedded_bucket_metadata_runtime},
    startup_deadlock::init_deadlock_detector_runtime,
    startup_embedded_optional::init_embedded_optional_service_runtime,
    startup_iam::{IamBootstrapDisposition, init_embedded_iam_runtime, init_iam_runtime},
    startup_notification::{init_embedded_notification_runtime, init_notification_runtime},
    startup_observability::init_observability_runtime,
    startup_optional_runtime_sidecars::{OptionalRuntimeServices, init_optional_runtime_services},
};
use rustfs_common::GlobalReadiness;
use std::{collections::BTreeSet, io::Result, sync::Arc};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

const EVENT_DRIVE_UNAVAILABLE: &str = "drive_unavailable";
const LOG_COMPONENT_CONNECT: &str = "connect";
const LOG_SUBSYSTEM_INVENTORY: &str = "inventory";

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ManagedLanceObjects {
    usecase: crate::app::object::DefaultObjectUsecase,
    context: crate::app::object::ManagedObjectContext,
    max_bytes: usize,
    operation_guard: Option<Arc<crate::table_catalog::VectorCatalogOperationGuard>>,
}

#[cfg(test)]
pub(crate) fn managed_lance_objects(
    access: crate::storage_api::startup::services::ManagedObjectAccess,
    request: Arc<crate::app::RequestEnvelope>,
    encryption: crate::app::object::ManagedEncryption,
    limits: crate::vector::lance::ManifestIoLimits,
) -> std::result::Result<Arc<ManagedLanceObjects>, crate::vector::lance::LanceManifestError> {
    let app = request.server_context().and_then(|slot| slot.installed_app_context()).ok_or(
        crate::vector::lance::LanceManifestError::ReferenceMismatch {
            reason: "managed manifest request has no installed application context",
        },
    )?;
    let context =
        crate::app::object::ManagedObjectContext::from_authorized_request(access, request, encryption).map_err(|source| {
            crate::vector::lance::LanceManifestError::Storage {
                operation: "prepare managed manifest access",
                source: Box::new(source),
            }
        })?;
    let max_bytes =
        usize::try_from(limits.max_manifest_bytes.get()).map_err(|source| crate::vector::lance::LanceManifestError::Storage {
            operation: "convert manifest read budget",
            source: Box::new(source),
        })?;
    Ok(Arc::new(ManagedLanceObjects {
        usecase: crate::app::object::DefaultObjectUsecase::with_context(Some(app)),
        context,
        max_bytes,
        operation_guard: None,
    }))
}

#[cfg(test)]
#[async_trait::async_trait]
impl crate::table_catalog::CatalogManagedCandidateReader for ManagedLanceObjects {
    async fn read_managed_candidate(
        &self,
        bucket: &crate::storage_api::table::managed::BucketHandle,
        metadata: &crate::table_catalog::CatalogMetadataRef,
        max_size: usize,
        lifecycle: &crate::storage_api::table::managed::BucketIncarnationFenceGuard,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
        use crate::table_catalog::{CatalogIoErrorKind, TableCatalogStoreError};

        if let Some(operation_guard) = &self.operation_guard {
            let native = operation_guard.lifecycle_guard()?;
            if !std::ptr::eq(native.as_ref(), lifecycle) {
                return Err(TableCatalogStoreError::Conflict("candidate read received another lifecycle owner".into()));
            }
        }
        if bucket != self.context.access().bucket() {
            return Err(TableCatalogStoreError::Io {
                classification: CatalogIoErrorKind::Denied,
                context: "read managed candidate outside the authorized bucket",
                source: Box::new(crate::storage_api::startup::services::BucketIdentityError::ObjectOutsideScope),
            });
        }
        let revision = metadata
            .body_revision()
            .ok_or_else(|| TableCatalogStoreError::Invalid("managed candidate reference requires a body revision".to_owned()))?;
        let read = self
            .usecase
            .managed_get_object_with_guard(
                &self.context,
                metadata.object_key().as_str(),
                Some(revision.as_uuid()),
                None,
                max_size.min(self.max_bytes),
                lifecycle,
            )
            .await;
        let (info, bytes) = match read {
            Ok(read) => read,
            Err(crate::app::object::ManagedObjectError::Api(api))
                if matches!(api.code.as_str(), "NoSuchKey" | "NoSuchVersion") =>
            {
                return Ok(None);
            }
            Err(error) => return Err(managed_candidate_error(error)),
        };
        Ok(Some(crate::table_catalog::TableCatalogObject {
            data: bytes.to_vec(),
            body_revision: info.data_dir.filter(|revision| !revision.is_nil()),
            etag: info.etag,
            mod_time: info.mod_time,
        }))
    }
}

#[cfg(test)]
fn managed_candidate_error(error: crate::app::object::ManagedObjectError) -> crate::table_catalog::TableCatalogStoreError {
    use crate::app::{bucket_usecase::BucketResolveError, object::ManagedObjectError};
    use crate::table_catalog::{CatalogIoErrorKind, TableCatalogStoreError};

    let error = match error {
        ManagedObjectError::Bucket(BucketResolveError::Storage(error)) => {
            ManagedObjectError::Api(crate::error::ApiError::from(error))
        }
        error => error,
    };
    let classification = match &error {
        ManagedObjectError::Api(api) => match api.code.as_str() {
            "AccessDenied" | "KMS.AccessDeniedException" => CatalogIoErrorKind::Denied,
            "PreconditionFailed" | "NoSuchBucket" | "NoSuchKey" | "NoSuchVersion" => CatalogIoErrorKind::Conflict,
            "ServiceUnavailable" | "SlowDown" | "RequestTimeout" => CatalogIoErrorKind::Unavailable,
            _ => CatalogIoErrorKind::Internal,
        },
        ManagedObjectError::Scope(_) | ManagedObjectError::MissingPrincipal | ManagedObjectError::EncryptionMismatch => {
            CatalogIoErrorKind::Denied
        }
        ManagedObjectError::Bucket(BucketResolveError::CapabilityUnavailable) => CatalogIoErrorKind::Unavailable,
        ManagedObjectError::Bucket(_) | ManagedObjectError::BodyRevisionMismatch => CatalogIoErrorKind::Conflict,
        ManagedObjectError::InvalidRange | ManagedObjectError::SizeLimitExceeded | ManagedObjectError::MissingBodyRevision => {
            CatalogIoErrorKind::Internal
        }
    };
    TableCatalogStoreError::Io {
        classification,
        context: "read managed catalog candidate",
        source: Box::new(error),
    }
}

#[cfg(test)]
impl ManagedLanceObjects {
    fn check_access(
        &self,
        access: &crate::storage_api::startup::services::ManagedObjectAccess,
    ) -> std::result::Result<(), crate::vector::lance::LanceManifestError> {
        if access != self.context.access() {
            return Err(crate::storage_api::startup::services::BucketIdentityError::ObjectOutsideScope.into());
        }
        if let Some(guard) = &self.operation_guard {
            guard.check_held()?;
        }
        Ok(())
    }

    fn metadata(
        info: crate::storage_api::startup::services::StorageObjectInfo,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObjectMetadata, crate::vector::lance::LanceManifestError> {
        let size = info
            .get_actual_size()
            .and_then(|size| u64::try_from(size).map_err(std::io::Error::other))
            .map_err(|source| crate::vector::lance::LanceManifestError::Storage {
                operation: "read managed manifest plaintext length",
                source: Box::new(source),
            })?;
        Ok(crate::table_catalog::TableCatalogObjectMetadata {
            body_revision: info.data_dir.filter(|revision| !revision.is_nil()),
            etag: info.etag,
            mod_time: info.mod_time,
            size,
        })
    }

    fn error(
        operation: &'static str,
        key: &str,
        error: crate::app::object::ManagedObjectError,
    ) -> crate::vector::lance::LanceManifestError {
        if let crate::app::object::ManagedObjectError::Api(api) = &error {
            match api.code.as_str() {
                "NoSuchKey" | "NoSuchVersion" => {
                    return crate::vector::lance::LanceManifestError::ObjectNotFound { key: key.to_owned() };
                }
                _ => {}
            }
        }
        crate::vector::lance::LanceManifestError::Storage {
            operation,
            source: Box::new(error),
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl crate::vector::lance::ManagedManifestObjects for ManagedLanceObjects {
    async fn create_only(
        &self,
        access: &crate::storage_api::startup::services::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        content: bytes::Bytes,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObjectMetadata, crate::vector::lance::LanceManifestError> {
        self.check_access(access)?;
        let size = u64::try_from(content.len()).map_err(|source| crate::vector::lance::LanceManifestError::Storage {
            operation: "measure managed manifest",
            source: Box::new(source),
        })?;
        if let Some(guard) = &self.operation_guard {
            let native = guard.lifecycle_guard()?;
            let objects = self.clone();
            let key = key.clone();
            return run_managed_bridge_mutation(async move {
                objects.check_access(objects.context.access())?;
                let info = objects.usecase.managed_create_object_with_guard(
                    &objects.context, key.as_str(), size,
                    futures::stream::once(std::future::ready(Ok(content))), native,
                ).await.map_err(|error| {
                    if matches!(&error, crate::app::object::ManagedObjectError::Api(api) if api.code.as_str() == "PreconditionFailed") {
                        crate::vector::lance::LanceManifestError::CandidateAlreadyExists { key: key.as_str().to_owned() }
                    } else { Self::error("create Managed Dataset object", key.as_str(), error) }
                })?;
                objects.check_access(objects.context.access())?;
                Self::metadata(info)
            }).await;
        }
        let body = futures::stream::once(std::future::ready(Ok(content)));
        let info = self
            .usecase
            .managed_create_object(&self.context, key.as_str(), size, body)
            .await
            .map_err(|error| {
                if matches!(&error, crate::app::object::ManagedObjectError::Api(api) if api.code.as_str() == "PreconditionFailed")
                {
                    return crate::vector::lance::LanceManifestError::CandidateAlreadyExists {
                        key: key.as_str().to_owned(),
                    };
                }
                Self::error("create managed manifest", key.as_str(), error)
            })?;
        Self::metadata(info)
    }

    async fn create_multipart(
        &self,
        access: &crate::storage_api::startup::services::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> std::result::Result<Arc<dyn crate::vector::lance::ManagedDatasetMultipart>, crate::vector::lance::LanceManifestError>
    {
        self.check_access(access)?;
        let native = self
            .operation_guard
            .as_ref()
            .ok_or(crate::storage_api::startup::services::BucketIdentityError::OperationNotAllowed)?
            .lifecycle_guard()?;
        let objects = self.clone();
        let key = key.clone();
        run_managed_bridge_mutation(async move {
            objects.check_access(objects.context.access())?;
            let upload = objects
                .usecase
                .managed_create_multipart_upload_with_guard(&objects.context, key.as_str(), native)
                .await
                .map_err(|error| Self::error("create Managed Dataset multipart", key.as_str(), error))?;
            objects.check_access(objects.context.access())?;
            Ok(Arc::new(ManagedObjectMultipartSession {
                objects,
                key,
                upload,
                state: Arc::new(tokio::sync::Mutex::new(ManagedObjectMultipartState::default())),
            }) as Arc<dyn crate::vector::lance::ManagedDatasetMultipart>)
        })
        .await
    }

    async fn get(
        &self,
        access: &crate::storage_api::startup::services::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        range: Option<std::ops::Range<u64>>,
        expected_body_revision: Option<uuid::Uuid>,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObject, crate::vector::lance::LanceManifestError> {
        self.check_access(access)?;
        let read = if let Some(guard) = &self.operation_guard {
            let native = guard.lifecycle_guard()?;
            self.usecase
                .managed_get_object_with_guard(
                    &self.context,
                    key.as_str(),
                    expected_body_revision,
                    range,
                    self.max_bytes,
                    &native,
                )
                .await
        } else {
            self.usecase
                .managed_get_object(&self.context, key.as_str(), expected_body_revision, range, self.max_bytes)
                .await
        };
        let (info, bytes) = read.map_err(|error| Self::error("read managed manifest", key.as_str(), error))?;
        self.check_access(access)?;
        let metadata = Self::metadata(info)?;
        Ok(crate::table_catalog::TableCatalogObject {
            data: bytes.to_vec(),
            body_revision: metadata.body_revision,
            etag: metadata.etag,
            mod_time: metadata.mod_time,
        })
    }

    async fn head(
        &self,
        access: &crate::storage_api::startup::services::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObjectMetadata, crate::vector::lance::LanceManifestError> {
        self.check_access(access)?;
        let head = if let Some(guard) = &self.operation_guard {
            let native = guard.lifecycle_guard()?;
            self.usecase
                .managed_head_object_with_guard(&self.context, key.as_str(), None, &native)
                .await
        } else {
            self.usecase.managed_head_object(&self.context, key.as_str(), None).await
        };
        let info = head.map_err(|error| Self::error("inspect managed manifest", key.as_str(), error))?;
        self.check_access(access)?;
        Self::metadata(info)
    }
}

#[cfg(test)]
pub(crate) fn guarded_managed_lance_objects(
    access: crate::storage_api::startup::services::ManagedObjectAccess,
    request: Arc<crate::app::RequestEnvelope>,
    definition: &crate::table_catalog::VectorIndexDefinitionV1,
    guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
    limits: crate::vector::lance::ManifestIoLimits,
) -> std::result::Result<Arc<ManagedLanceObjects>, crate::vector::lance::LanceManifestError> {
    use crate::table_catalog::{CatalogFormat, VectorEffectiveEncryptionV1};
    definition.validate()?;
    guard.check_held()?;
    let resource = guard.resource_identity();
    if resource.vector_bucket() != Some(access.bucket())
        || resource.table_id() != access.table_id()
        || resource.format() != CatalogFormat::Lance
        || resource.dataset_prefix() != access.allowed_prefix()
    {
        return Err(crate::vector::lance::LanceManifestError::ResourceMismatch {
            reason: "Managed Dataset access differs from its Catalog operation",
        });
    }
    guard.lifecycle_guard()?;
    let encryption = match &definition.effective_encryption {
        VectorEffectiveEncryptionV1::Unencrypted => crate::app::object::ManagedEncryption::Unencrypted,
        VectorEffectiveEncryptionV1::Aes256 => crate::app::object::ManagedEncryption::Aes256,
        VectorEffectiveEncryptionV1::AwsKms { key_id, .. } => {
            crate::app::object::ManagedEncryption::AwsKms { key_id: key_id.clone() }
        }
    };
    let mut objects = managed_lance_objects(access, request, encryption, limits)?;
    Arc::get_mut(&mut objects)
        .ok_or(crate::vector::lance::LanceManifestError::ReferenceMismatch {
            reason: "new Managed Dataset bridge already has another owner",
        })?
        .operation_guard = Some(guard);
    Ok(objects)
}

#[cfg(test)]
async fn run_managed_bridge_mutation<T, F>(operation: F) -> std::result::Result<T, crate::vector::lance::LanceManifestError>
where
    T: Send + 'static,
    F: std::future::Future<Output = std::result::Result<T, crate::vector::lance::LanceManifestError>> + Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let operation = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, operation);
    let operation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        operation,
    );
    crate::storage_api::table::spawn_traced_join(operation)
        .await
        .map_err(|source| crate::vector::lance::LanceManifestError::Storage {
            operation: "join Managed Dataset mutation",
            source: Box::new(source),
        })?
}

#[cfg(test)]
#[derive(Clone)]
struct ManagedObjectMultipartSession {
    objects: ManagedLanceObjects,
    key: crate::table_catalog::CatalogObjectKey,
    upload: Arc<crate::app::object::ManagedMultipartUpload>,
    state: Arc<tokio::sync::Mutex<ManagedObjectMultipartState>>,
}

#[cfg(test)]
#[derive(Default)]
struct ManagedObjectMultipartState {
    // An absent receipt retains the identity of an interrupted or failed part.
    parts: std::collections::BTreeMap<u16, Option<(crate::storage_api::startup::services::CompletePart, u64)>>,
    completing: Option<u16>,
    completed: Option<crate::table_catalog::TableCatalogObjectMetadata>,
    aborted: bool,
}

#[cfg(test)]
impl ManagedObjectMultipartSession {
    fn invalid(reason: &'static str) -> crate::vector::lance::LanceManifestError {
        crate::vector::lance::LanceManifestError::ReferenceMismatch { reason }
    }

    async fn complete_owned(
        &self,
        expected_parts: u16,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObjectMetadata, crate::vector::lance::LanceManifestError> {
        self.objects.check_access(self.objects.context.access())?;
        let mut state = self.state.lock().await;
        if expected_parts == 0 || state.aborted || state.completing.is_some_and(|count| count != expected_parts) {
            return Err(Self::invalid("multipart completion differs from its admitted upload"));
        }
        if state.parts.len() != usize::from(expected_parts) {
            return Err(Self::invalid("multipart completion requires every part in 1..N"));
        }
        let mut total = 0_u64;
        let mut receipts = Vec::with_capacity(usize::from(expected_parts));
        for number in 1..=expected_parts {
            let (receipt, size) = state
                .parts
                .get(&number)
                .and_then(Option::as_ref)
                .ok_or_else(|| Self::invalid("multipart completion is missing an accepted part receipt"))?;
            if receipt.part_num != usize::from(number) || receipt.etag.as_ref().is_none_or(String::is_empty) {
                return Err(Self::invalid("multipart receipt does not identify its accepted part"));
            }
            total = total
                .checked_add(*size)
                .ok_or_else(|| Self::invalid("multipart byte count overflow"))?;
            receipts.push(receipt.clone());
        }
        if let Some(completed) = &state.completed {
            let revision = completed
                .body_revision
                .filter(|revision| !revision.is_nil())
                .ok_or_else(|| Self::invalid("completed multipart has no exact body revision"))?;
            let info = self
                .objects
                .usecase
                .managed_confirm_completed_multipart_upload(&self.upload, revision)
                .await
                .map_err(|error| {
                    ManagedLanceObjects::error("confirm completed Managed Dataset multipart", self.key.as_str(), error)
                })?;
            let metadata = ManagedLanceObjects::metadata(info)?;
            if &metadata != completed || metadata.size != total {
                return Err(Self::invalid("multipart completion replay changed the committed identity"));
            }
            self.objects.check_access(self.objects.context.access())?;
            return Ok(metadata);
        }
        state.completing = Some(expected_parts);
        let info = self
            .objects
            .usecase
            .managed_complete_multipart_upload(&self.upload, receipts)
            .await
            .map_err(|error| ManagedLanceObjects::error("complete Managed Dataset multipart", self.key.as_str(), error))?;
        let metadata = ManagedLanceObjects::metadata(info)?;
        if metadata.size != total || metadata.body_revision.is_none() {
            return Err(Self::invalid("completed multipart metadata differs from its accepted parts"));
        }
        state.completed = Some(metadata.clone());
        self.objects.check_access(self.objects.context.access())?;
        Ok(metadata)
    }

    async fn abort_owned(&self) -> std::result::Result<(), crate::vector::lance::LanceManifestError> {
        self.objects.check_access(self.objects.context.access())?;
        let mut state = self.state.lock().await;
        if state.aborted || state.completed.is_some() {
            return Ok(());
        }
        self.objects
            .usecase
            .managed_abort_multipart_upload(&self.upload)
            .await
            .map_err(|error| ManagedLanceObjects::error("abort Managed Dataset multipart", self.key.as_str(), error))?;
        state.aborted = true;
        self.objects.check_access(self.objects.context.access())
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl crate::vector::lance::ManagedDatasetMultipart for ManagedObjectMultipartSession {
    async fn put_part(
        &self,
        part_number: u16,
        content: Vec<bytes::Bytes>,
        size: u64,
    ) -> std::result::Result<(), crate::vector::lance::LanceManifestError> {
        self.objects.check_access(self.objects.context.access())?;
        if part_number == 0 {
            return Err(Self::invalid("multipart part numbers start at one"));
        }
        let measured = content
            .iter()
            .try_fold(0_u64, |total, chunk| total.checked_add(u64::try_from(chunk.len()).ok()?));
        if measured != Some(size) {
            return Err(Self::invalid("multipart part byte count differs from its declared length"));
        }
        let mut state = self.state.lock().await;
        if state.aborted || state.completing.is_some() || state.parts.contains_key(&part_number) {
            return Err(Self::invalid("multipart part is duplicated or the upload is sealed"));
        }
        state.parts.insert(part_number, None);
        // UploadPart keeps the application's native cancellation semantics.
        let receipt = self
            .objects
            .usecase
            .managed_upload_part(&self.upload, part_number, size, futures::stream::iter(content.into_iter().map(Ok)))
            .await
            .map_err(|error| ManagedLanceObjects::error("write Managed Dataset part", self.key.as_str(), error))?;
        if receipt.part_num != usize::from(part_number) || receipt.etag.as_ref().is_none_or(String::is_empty) {
            return Err(Self::invalid("multipart response has no exact part receipt"));
        }
        state.parts.insert(part_number, Some((receipt, size)));
        self.objects.check_access(self.objects.context.access())
    }

    async fn complete(
        &self,
        expected_parts: u16,
    ) -> std::result::Result<crate::table_catalog::TableCatalogObjectMetadata, crate::vector::lance::LanceManifestError> {
        let session = self.clone();
        run_managed_bridge_mutation(async move { session.complete_owned(expected_parts).await }).await
    }

    async fn abort(&self) -> std::result::Result<(), crate::vector::lance::LanceManifestError> {
        let session = self.clone();
        run_managed_bridge_mutation(async move { session.abort_owned().await }).await
    }
}

pub(crate) struct StartupServiceRuntime {
    pub(crate) table_maintenance: Option<crate::table_catalog::PeriodicMaintenanceRuntime>,
    pub(crate) oidc_recovery: Option<AbortOnDropHandle<()>>,
    pub(crate) optional_runtimes: OptionalRuntimeServices,
    pub(crate) heartbeat: Option<HeartbeatRuntime>,
    pub(crate) inventory: Option<InventoryRuntime>,
    pub(crate) local_trace_capture: Option<LocalTraceCaptureRuntime>,
    pub(crate) iam_bootstrap: IamBootstrapDisposition,
    pub(crate) enable_scanner: bool,
}

pub(crate) struct EmbeddedStartupServiceRuntime {
    pub(crate) iam_bootstrap: IamBootstrapDisposition,
}

pub(crate) async fn init_embedded_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<EmbeddedStartupServiceRuntime> {
    init_embedded_optional_service_runtime(config, store.clone()).await;
    let buckets = init_embedded_bucket_metadata_runtime(store.clone(), &ctx).await?;
    let iam_bootstrap = init_embedded_iam_runtime(store, ctx, readiness, server_ctx)
        .await
        .map_err(|err| std::io::Error::other(format!("IAM bootstrap setup: {err}")))?;
    init_embedded_notification_runtime(endpoint_pools, buckets).await;

    Ok(EmbeddedStartupServiceRuntime { iam_bootstrap })
}

pub(crate) async fn init_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<StartupServiceRuntime> {
    let maintenance_config = crate::table_catalog::PeriodicMaintenanceConfig::from_env().map_err(std::io::Error::other)?;
    init_kms_system(config, store.clone()).await?;

    let optional_runtimes = init_optional_runtime_services(server_ctx.clone()).await?;
    let heartbeat_config = HeartbeatConfig::from_env().map_err(std::io::Error::other)?;
    let local_trace_capture = heartbeat_config
        .as_ref()
        .and_then(HeartbeatConfig::state_root)
        .map(|state_root| spawn_local_trace_capture_runtime(state_root, &ctx))
        .transpose()
        .map_err(std::io::Error::other)?;
    let heartbeat_nodes = heartbeat_config.as_ref().map(|_| endpoint_pools.get_nodes().len());
    let inventory_drives = heartbeat_config
        .as_ref()
        .map(|_| endpoint_pools.as_ref().iter().map(|pool| pool.endpoints.as_ref().len()).sum());

    init_buffer_profile_system(config);
    init_deadlock_detector_runtime();

    let buckets = init_bucket_metadata_runtime(store.clone(), ctx.clone()).await?;
    let iam_bootstrap = init_iam_runtime(store.clone(), ctx.clone(), readiness, state_manager, server_ctx.clone()).await?;

    // Audit initialization requires the AppContext (server config + object store)
    // which is published by ensure_startup_after_iam inside init_iam_runtime.
    init_audit_runtime().await;
    // Unconditional: deferred IAM recovers in the background and has no callback into this
    // scheduler, so gating on the inline disposition would leave a recovered node with
    // self-pointing replication rules until the next restart. The task waits for IAM and
    // bucket metadata itself, and its first pass runs off this path.
    spawn_site_replication_reconcile_task(ctx.clone());
    let oidc_recovery = init_auth_integrations(ctx.child_token()).await?;
    init_notification_runtime(endpoint_pools, buckets).await?;
    let enable_scanner = init_background_service_runtime(store.clone()).await?;
    init_observability_runtime(store.clone(), ctx.clone()).await;
    let heartbeat = start_heartbeat_runtime(heartbeat_config.clone(), heartbeat_nodes, &ctx)?;
    let inventory = start_inventory_runtime(heartbeat_config, heartbeat_nodes, inventory_drives, store, &ctx)?;

    let table_maintenance =
        maintenance_config.map(|config| crate::table_catalog::PeriodicMaintenanceRuntime::spawn(config, server_ctx, &ctx));

    Ok(StartupServiceRuntime {
        table_maintenance,
        oidc_recovery,
        optional_runtimes,
        heartbeat,
        inventory,
        local_trace_capture,
        iam_bootstrap,
        enable_scanner,
    })
}

fn start_heartbeat_runtime(
    config: Option<HeartbeatConfig>,
    node_count: Option<usize>,
    shutdown: &CancellationToken,
) -> Result<Option<HeartbeatRuntime>> {
    let Some(config) = config else {
        return Ok(None);
    };
    if !config.transport_enabled() {
        return Ok(None);
    }
    let summary = u16::try_from(node_count.unwrap_or_default())
        .ok()
        .and_then(|total| CoarseNodeSummary::new(total, 0, 0).ok())
        .ok_or_else(|| std::io::Error::other("Connect heartbeat node count is outside protocol bounds"))?;
    spawn_heartbeat_runtime(Some(config), shutdown, move || summary).map_err(startup_heartbeat_error)
}

fn startup_heartbeat_error(error: HeartbeatError) -> std::io::Error {
    std::io::Error::other(heartbeat_failure_reason(&error))
}

fn start_inventory_runtime(
    config: Option<HeartbeatConfig>,
    node_count: Option<usize>,
    expected_drive_count: Option<usize>,
    store: Arc<ECStore>,
    shutdown: &CancellationToken,
) -> Result<Option<InventoryRuntime>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let node_count = node_count.unwrap_or_default();
    let expected_drive_count = expected_drive_count.unwrap_or_default();
    spawn_inventory_runtime(Some(config), InventorySchedule::default(), shutdown, move || {
        let store = store.clone();
        async move {
            let info = StorageAdminApi::storage_info(store.as_ref()).await;
            inventory_snapshot(node_count, expected_drive_count, info)
        }
    })
    .map_err(std::io::Error::other)
}

fn inventory_snapshot(
    node_count: usize,
    expected_drive_count: usize,
    info: rustfs_madmin::StorageInfo,
) -> std::result::Result<InventorySnapshot, InventoryError> {
    let drive_count = inventory_topology_slot_count(&info, expected_drive_count)?;
    let (total, free) = inventory_capacity(&info)?;
    let mut flags = Vec::with_capacity(3);
    if info.disks.iter().any(|disk| !inventory_disk_is_healthy(disk)) {
        flags.push(InventoryFlag::ClusterDegraded);
    }
    let offline_drive_count = info.disks.iter().filter(|disk| inventory_disk_is_offline(disk)).count();
    if offline_drive_count > 0 {
        tracing::warn!(
            target: "rustfs::connect::inventory",
            event = EVENT_DRIVE_UNAVAILABLE,
            component = LOG_COMPONENT_CONNECT,
            subsystem = LOG_SUBSYSTEM_INVENTORY,
            offline_drive_count,
            "Connect inventory detected unavailable drives"
        );
        flags.push(InventoryFlag::DriveOffline);
    }
    if info.disks.iter().any(|disk| disk.healing) {
        flags.push(InventoryFlag::ClusterHealing);
    }
    InventorySnapshot::current(node_count, drive_count, total, free, flags)
}

fn inventory_topology_slot_count(
    info: &rustfs_madmin::StorageInfo,
    expected_drive_count: usize,
) -> std::result::Result<usize, InventoryError> {
    let total_sets = &info.backend.total_sets;
    let drives_per_set = &info.backend.drives_per_set;
    let geometry_drive_count = if total_sets.is_empty()
        || total_sets.len() != drives_per_set.len()
        || total_sets.contains(&0)
        || drives_per_set.contains(&0)
    {
        None
    } else {
        total_sets
            .iter()
            .zip(drives_per_set)
            .try_fold(0_usize, |total, (&sets, &drives)| {
                sets.checked_mul(drives).and_then(|pool| total.checked_add(pool))
            })
    };
    if geometry_drive_count != Some(expected_drive_count) {
        return Err(InventoryError::SnapshotIncomplete {
            expected: expected_drive_count,
            observed: 0,
        });
    }

    let mut slots = BTreeSet::new();
    let mut invalid = false;
    for disk in &info.disks {
        let key = match (
            usize::try_from(disk.pool_index),
            usize::try_from(disk.set_index),
            usize::try_from(disk.disk_index),
        ) {
            (Ok(pool_index), Ok(set_index), Ok(disk_index)) => (pool_index, set_index, disk_index),
            _ => {
                invalid = true;
                continue;
            }
        };
        if key.0 >= total_sets.len() || key.1 >= total_sets[key.0] || key.2 >= drives_per_set[key.0] {
            invalid = true;
            continue;
        }
        if !slots.insert(key) {
            invalid = true;
        }
    }
    if invalid || slots.len() != expected_drive_count {
        return Err(InventoryError::SnapshotIncomplete {
            expected: expected_drive_count,
            observed: slots.len(),
        });
    }
    Ok(slots.len())
}

fn inventory_disk_is_healthy(disk: &rustfs_madmin::Disk) -> bool {
    let disk_state_is_healthy = ["ok", rustfs_madmin::ITEM_ONLINE, "unformatted"]
        .iter()
        .any(|state| disk.state.eq_ignore_ascii_case(state));
    let runtime_state_is_healthy = disk.runtime_state.as_deref().is_none_or(|runtime_state| {
        [rustfs_madmin::ITEM_ONLINE, "returning"]
            .iter()
            .any(|state| runtime_state.eq_ignore_ascii_case(state))
    });
    disk_state_is_healthy && runtime_state_is_healthy
}

fn inventory_disk_is_offline(disk: &rustfs_madmin::Disk) -> bool {
    [rustfs_madmin::ITEM_OFFLINE, "missing", "disk not found"]
        .iter()
        .any(|state| disk.state.eq_ignore_ascii_case(state))
        || disk.runtime_state.as_deref().is_some_and(|runtime_state| {
            [rustfs_madmin::ITEM_OFFLINE, "missing"]
                .iter()
                .any(|state| runtime_state.eq_ignore_ascii_case(state))
        })
}

fn inventory_capacity(info: &rustfs_madmin::StorageInfo) -> std::result::Result<(u64, u64), InventoryError> {
    let configured_data_widths = (!info.backend.standard_sc_data.is_empty()).then_some(info.backend.standard_sc_data.as_slice());
    aggregate_inventory_capacity(&info.disks, configured_data_widths)
}

fn aggregate_inventory_capacity(
    disks: &[rustfs_madmin::Disk],
    data_widths: Option<&[usize]>,
) -> std::result::Result<(u64, u64), InventoryError> {
    let mut seen = BTreeSet::new();
    let mut indexed = Vec::with_capacity(disks.len());
    for disk in disks {
        let key = match (
            usize::try_from(disk.pool_index),
            usize::try_from(disk.set_index),
            usize::try_from(disk.disk_index),
        ) {
            (Ok(pool_index), Ok(set_index), Ok(disk_index)) => (pool_index, set_index, disk_index),
            _ => {
                return Err(InventoryError::SnapshotIncomplete {
                    expected: disks.len(),
                    observed: indexed.len(),
                });
            }
        };
        if seen.insert(key) {
            indexed.push((key, disk));
        }
    }

    let usable_widths = data_widths.filter(|widths| {
        indexed
            .iter()
            .all(|((pool_index, _, _), _)| widths.get(*pool_index).is_some_and(|width| *width > 0))
    });
    let mut total = 0_u64;
    let mut free = 0_u64;
    let mut included = 0;
    for ((pool_index, _, disk_index), disk) in indexed {
        let include = match usable_widths {
            Some(widths) => disk_index < widths[pool_index],
            None => {
                let state = disk.state.trim().to_ascii_lowercase();
                !state.contains("offline") && !state.contains("not found")
            }
        };
        if !include {
            continue;
        }
        included += 1;
        total = total.checked_add(disk.total_space).ok_or(InventoryError::Capacity)?;
        free = free.checked_add(disk.available_space).ok_or(InventoryError::Capacity)?;
    }
    if !disks.is_empty() && included == 0 {
        return Err(InventoryError::SnapshotIncomplete {
            expected: disks.len(),
            observed: 0,
        });
    }
    Ok((total, free))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::table::managed::ManagedBucketStorage;
    use crate::table_catalog::CatalogManagedCandidateReader;
    use crate::vector::lance::ManagedManifestObjects;

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_lance_bridge_preserves_plaintext_reference_and_scope() {
        let (request, access, store, _provider_reset) = crate::app::object::managed_request_fixture().await;
        let objects = managed_lance_objects(
            access.clone(),
            request,
            crate::app::object::ManagedEncryption::Aes256,
            crate::vector::lance::ManifestIoLimits {
                max_manifest_bytes: std::num::NonZeroU64::new(16 * 1024 * 1024).expect("positive read budget"),
                max_concurrent_reads: std::num::NonZeroUsize::new(8).expect("positive concurrency"),
            },
        )
        .expect("bound manifest object adapter");
        let key = crate::table_catalog::CatalogObjectKey::try_new(format!("{}candidate.bin", access.allowed_prefix()))
            .expect("candidate key");
        let bytes = bytes::Bytes::from_static(b"\0manifest bridge\xff");
        let published = objects
            .create_only(&access, &key, bytes.clone())
            .await
            .expect("encrypted object write");
        let revision = published.body_revision.expect("stored body identity");
        assert!(!revision.is_nil());
        assert_eq!(published.size, bytes.len() as u64);
        let head = objects.head(&access, &key).await.expect("plaintext object head");
        assert_eq!(head.body_revision, Some(revision));
        assert_eq!(head.size, published.size);
        let read = objects
            .get(&access, &key, None, Some(revision))
            .await
            .expect("accurate full read");
        assert_eq!(read.data.as_slice(), bytes.as_ref());
        assert_eq!(read.body_revision, Some(revision));
        let candidate = crate::table_catalog::CatalogMetadataRef::from_read(key.clone(), &read).expect("candidate identity");
        let guard = store
            .acquire_managed_bucket_fence(access.bucket().storage_bucket_id().as_str(), access.bucket().incarnation().as_uuid())
            .await
            .expect("catalog bucket lifecycle guard");
        let verified = objects
            .read_managed_candidate(access.bucket(), &candidate, bytes.len(), &guard)
            .await
            .expect("catalog plaintext candidate read")
            .expect("candidate exists");
        assert_eq!(verified.data.as_slice(), bytes.as_ref());
        assert_eq!(verified.body_revision, Some(revision));
        assert!(
            objects
                .read_managed_candidate(access.bucket(), &candidate, bytes.len() - 1, &guard)
                .await
                .is_err()
        );
        drop(guard);
        let range = objects
            .get(&access, &key, Some(1..5), Some(revision))
            .await
            .expect("accurate range read");
        assert_eq!(range.data.as_slice(), &bytes[1..5]);
        assert!(objects.get(&access, &key, None, Some(uuid::Uuid::new_v4())).await.is_err());
        assert!(
            objects
                .create_only(&access, &key, bytes::Bytes::from_static(b"replacement"))
                .await
                .is_err()
        );
        let unchanged = objects
            .get(&access, &key, None, Some(revision))
            .await
            .expect("original candidate after conflict");
        assert_eq!(unchanged.data.as_slice(), bytes.as_ref());

        let narrower = crate::storage_api::startup::services::ManagedObjectAccess::try_from_parts(
            access.bucket().clone(),
            access.table_id().clone(),
            access.allowed_prefix().to_owned(),
            [crate::storage_api::startup::services::ManagedObjectOperation::Get]
                .into_iter()
                .collect(),
        )
        .expect("alternate access scope");
        assert!(matches!(
            objects.get(&narrower, &key, None, Some(revision)).await,
            Err(crate::vector::lance::LanceManifestError::ManagedAccessDenied(_))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_catalog_candidate_preserves_real_kms_denial() {
        use crate::app::object::{ManagedEncryption, ManagedObjectError};
        use crate::storage_api::startup::services::{
            RecordingKmsKeyAuthorizer, TestAuthorizationHooks, kms_sse_dek_provider_for_test, set_sse_dek_provider_for_test,
        };
        use crate::table_catalog::{CatalogIoErrorKind, CatalogMetadataRef, CatalogObjectKey, TableCatalogStoreError};
        use rustfs_kms::types::{CreateKeyRequest, KeyUsage};

        let key_id = "catalog-candidate-key";
        let encryption = ManagedEncryption::AwsKms { key_id: key_id.into() };
        let (request, access, storage, _provider_reset) =
            crate::app::object::managed_request_fixture_with_encryption(encryption.clone()).await;
        let key_directory = tempfile::TempDir::new().expect("KMS key directory");
        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
        manager
            .reconfigure(rustfs_kms::KmsConfig::local(key_directory.path().to_path_buf()).with_insecure_development_defaults())
            .await
            .expect("KMS configuration");
        manager
            .get_encryption_service()
            .await
            .expect("KMS service")
            .create_key(CreateKeyRequest {
                key_name: Some(key_id.into()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: Default::default(),
                origin: None,
            })
            .await
            .expect("candidate encryption key");
        set_sse_dek_provider_for_test(kms_sse_dek_provider_for_test(manager).await.expect("real KMS provider"));
        let with_authorizer = |authorizer| {
            let mut extensions = http::Extensions::new();
            extensions.insert(request.server_context().expect("server context").clone());
            extensions.insert(request.request_context().expect("request context").clone());
            extensions.insert(request.req_info().expect("request principal").clone());
            extensions.insert(TestAuthorizationHooks {
                enforced: true,
                authorizer,
            });
            Arc::new(crate::app::RequestEnvelope::from_parts(
                crate::app::RequestHead {
                    method: request.method().clone(),
                    uri: request.uri().clone(),
                    headers: request.headers().clone(),
                    credentials: request.credentials().cloned(),
                    region: request.region().map(str::to_owned),
                    service: request.service().map(str::to_owned),
                },
                extensions,
            ))
        };
        let limits = crate::vector::lance::ManifestIoLimits {
            max_manifest_bytes: std::num::NonZeroU64::new(16 * 1024 * 1024).expect("positive budget"),
            max_concurrent_reads: std::num::NonZeroUsize::new(8).expect("positive concurrency"),
        };
        let allowed = RecordingKmsKeyAuthorizer::new(true);
        let objects = managed_lance_objects(access.clone(), with_authorizer(allowed.clone()), encryption.clone(), limits)
            .expect("authorized candidate adapter");
        let key = CatalogObjectKey::try_new(format!("{}kms-candidate.bin", access.allowed_prefix())).expect("candidate key");
        let bytes = bytes::Bytes::from_static(b"KMS protected catalog candidate");
        let created = objects
            .create_only(&access, &key, bytes.clone())
            .await
            .expect("KMS candidate write");
        let plaintext = objects
            .get(&access, &key, None, created.body_revision)
            .await
            .expect("authorized candidate read");
        let candidate = CatalogMetadataRef::from_read(key, &plaintext).expect("candidate reference");
        let denied = RecordingKmsKeyAuthorizer::new(false);
        let reader = managed_lance_objects(access.clone(), with_authorizer(denied.clone()), encryption, limits)
            .expect("denied principal adapter");
        let guard = storage
            .acquire_managed_bucket_fence(access.bucket().storage_bucket_id().as_str(), access.bucket().incarnation().as_uuid())
            .await
            .expect("catalog lifecycle guard");
        let error = reader
            .read_managed_candidate(access.bucket(), &candidate, bytes.len(), &guard)
            .await
            .expect_err("KMS must authorize Catalog candidate reads");
        let TableCatalogStoreError::Io {
            classification, source, ..
        } = &error
        else {
            panic!("expected a classified Catalog I/O error: {error:?}");
        };
        assert_eq!(*classification, CatalogIoErrorKind::Denied);
        assert!(matches!(source.downcast_ref::<ManagedObjectError>(), Some(ManagedObjectError::Api(api))
            if api.code.as_str() == "AccessDenied"));
        let authorized_decryption = allowed
            .calls()
            .into_iter()
            .find(|(action, _)| *action == rustfs_policy::policy::action::KmsAction::DecryptAction)
            .expect("successful read authorized the encryption key");
        assert_eq!(denied.calls(), [authorized_decryption]);
        let api = crate::error::ApiError::from(error);
        assert_eq!(api.code.as_str(), "AccessDenied");
        assert!(!api.message.contains(key_id));
        assert!(std::error::Error::source(&api).is_some());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_lance_ecstore_catalog_commits_exact_files() {
        use crate::app::object::ManagedEncryption;
        use crate::storage_api::table::StorageObjectOptions;
        use crate::storage_api::table::contract::object::ObjectIO;
        use crate::table_catalog::*;
        use crate::vector::lance::{
            LanceManifestAdapter, ManifestIoLimits, ManifestPrepareRequest, VectorDistanceMetric, VectorIndexManifestSpec,
        };
        use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
        use tokio::io::AsyncReadExt;

        for encryption in [ManagedEncryption::Unencrypted, ManagedEncryption::Aes256] {
            let (request, access, storage, _provider_reset) =
                crate::app::object::managed_request_fixture_with_encryption(encryption.clone()).await;
            let resource =
                CatalogResourceIdentity::try_new(access.bucket().clone(), access.table_id().clone(), CatalogFormat::Lance)
                    .expect("resource identity");
            use crate::storage_api::startup::services::{ManagedObjectAccess, ManagedObjectOperation};
            let access = ManagedObjectAccess::try_from_parts(
                access.bucket().clone(),
                access.table_id().clone(),
                resource.dataset_prefix(),
                [
                    ManagedObjectOperation::CreateOnly,
                    ManagedObjectOperation::Head,
                    ManagedObjectOperation::Get,
                    ManagedObjectOperation::Range,
                ]
                .into_iter()
                .collect(),
            )
            .expect("exact Dataset scope");
            let limits = ManifestIoLimits {
                max_manifest_bytes: NonZeroU64::new(16 * 1024 * 1024).expect("positive manifest budget"),
                max_concurrent_reads: NonZeroUsize::new(8).expect("positive concurrency"),
            };
            let objects = managed_lance_objects(access.clone(), request, encryption.clone(), limits).expect("managed adapter");
            let backend = EcStoreTableCatalogObjectBackend::new_with_managed_candidate_reader(storage.clone(), objects.clone());
            let store = Arc::new(
                ObjectTableCatalogStore::for_vector_scope(backend.clone(), access.bucket().clone())
                    .await
                    .expect("real ECStore catalog"),
            );
            let bucket_id = access.bucket().storage_bucket_id().as_str();
            store
                .put_table_bucket(TableBucketEntry {
                    vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(access.bucket()).expect("bucket binding")),
                    version: 2,
                    table_bucket: bucket_id.to_owned(),
                    catalog_type: "s3vectors".into(),
                    warehouse_root: format!("s3://{bucket_id}/"),
                    state: TableCatalogEntryState::Active,
                    properties: Default::default(),
                    active_rename_id: None,
                    created_at: None,
                    updated_at: None,
                })
                .await
                .expect("persist Vector catalog bucket");
            let namespace = Namespace::parse("indexes").expect("namespace");
            store
                .create_namespace(NamespaceEntry {
                    version: 1,
                    table_bucket: bucket_id.to_owned(),
                    namespace: namespace.public_name(),
                    namespace_id: namespace.storage_id(),
                    state: TableCatalogEntryState::Active,
                    properties: Default::default(),
                    created_at: None,
                    updated_at: None,
                })
                .await
                .expect("persist namespace");
            let name = CatalogName::parse("index.v1").expect("index name with period");
            let encryption_descriptor: &[u8] = match &encryption {
                ManagedEncryption::Unencrypted => b"null",
                ManagedEncryption::Aes256 => br#"{"sseType":"AES256"}"#,
                ManagedEncryption::AwsKms { .. } => unreachable!("test cases are unencrypted and AES256"),
            };
            let index = VectorIndexManifestSpec {
                schema_version: 1,
                dimension: NonZeroU32::new(3).expect("positive dimension"),
                distance_metric: VectorDistanceMetric::Cosine,
                non_filterable_metadata_keys: BTreeSet::new(),
                encryption_descriptor_sha256: CatalogContentSha256::digest(encryption_descriptor),
            };
            let adapter =
                LanceManifestAdapter::new(store.clone(), objects.clone(), access.clone(), name.clone(), index.clone(), limits)
                    .expect("Lance catalog adapter");
            let mut entry = TableEntry {
                vector_head_binding: None,
                vector_index_definition: None,
                version: 2,
                table_bucket: bucket_id.to_owned(),
                namespace: namespace.public_name(),
                table: name.as_str().to_owned(),
                table_id: access.table_id().as_str().to_owned(),
                table_uuid: access.table_id().as_str().to_owned(),
                format: "lance".into(),
                format_version: 1,
                warehouse_location: format!("s3://{bucket_id}/{}", resource.dataset_prefix().trim_end_matches('/')),
                metadata_location: String::new(),
                version_token: "initial-token".into(),
                generation: 1,
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                created_at: None,
                updated_at: None,
            };
            let prepare = |expected_head| ManifestPrepareRequest {
                parts: CatalogCommitParts {
                    resource_identity: resource.clone(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    expected_head,
                    commit_id: CatalogCommitId::try_new(uuid::Uuid::new_v4().to_string()).expect("commit ID"),
                    operation: "test".into(),
                    idempotency_key: None,
                    writer: None,
                },
                index: index.clone(),
            };
            let initial_request = prepare(CatalogExpectedHead::from_entry(&entry, None, None).expect("initial head"));
            let initial_commit_id = initial_request.parts.commit_id.clone();
            let initial = adapter
                .prepare_empty_index(&access, initial_request)
                .await
                .expect("create real Lance manifest");
            let fresh_adapter =
                LanceManifestAdapter::new(store.clone(), objects.clone(), access.clone(), name.clone(), index.clone(), limits)
                    .expect("new adapter without cached references");
            let mut retry_request = prepare(CatalogExpectedHead::from_entry(&entry, None, None).expect("same initial head"));
            retry_request.parts.commit_id = initial_commit_id;
            let retried = fresh_adapter
                .prepare_empty_index(&access, retry_request)
                .await
                .expect("recover identical real candidate after adapter restart");
            assert_eq!(
                retried.candidate_metadata(),
                initial.candidate_metadata(),
                "candidate retry keeps the original body revision and exact content identity"
            );
            entry.metadata_location = initial.candidate_metadata().object_key().as_str().to_owned();
            entry.vector_head_binding = Some(
                VectorHeadBindingV1::try_new(&resource, initial.candidate_metadata(), initial.format_descriptor())
                    .expect("initial binding"),
            );
            store.create_table(entry).await.expect("publish initial manifest");
            let first = adapter.load_current(&access, &resource).await.expect("open initial dataset");
            assert_eq!(first.manifest_version, 1);
            let head = store
                .read_catalog_head(access.bucket(), &namespace, &name)
                .await
                .expect("read current head");
            let winner = adapter
                .prepare_empty_index(&access, prepare(head.expected_head()))
                .await
                .expect("prepare winner");
            let stale = adapter
                .prepare_empty_index(&access, prepare(head.expected_head()))
                .await
                .expect("prepare competing candidate");
            let winner_reference = winner.candidate_metadata().clone();
            store.commit_prepared(winner).await.expect("publish one successor");
            assert!(matches!(store.commit_prepared(stale).await, Err(TableCatalogStoreError::Conflict(_))));
            let committed: crate::vector::lance::CommittedManifestInfo = adapter
                .load_current(&access, &resource)
                .await
                .expect("open committed dataset");
            assert_eq!(committed.manifest_version, 2);
            assert_eq!(committed.reference, winner_reference);
            let plaintext = objects
                .get(
                    &access,
                    committed.reference.object_key(),
                    None,
                    committed.reference.body_revision().map(|revision| revision.as_uuid()),
                )
                .await
                .expect("read exact committed plaintext");
            assert!(plaintext.data.ends_with(b"LANC"));
            assert_eq!(CatalogContentSha256::digest(&plaintext.data), committed.reference.content_sha256());
            let mut stored = storage
                .get_object_reader(
                    bucket_id,
                    committed.reference.object_key().as_str(),
                    None,
                    http::HeaderMap::new(),
                    &StorageObjectOptions {
                        raw_data_movement_read: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("read stored representation without decryption");
            assert_eq!(
                stored.object_info.data_dir,
                committed.reference.body_revision().map(|revision| revision.as_uuid())
            );
            assert_eq!(stored.object_info.is_encrypted(), encryption != ManagedEncryption::Unencrypted);
            let stored_size = stored.object_info.size;
            assert_eq!(
                stored.object_info.decrypted_size().expect("logical object size"),
                i64::try_from(plaintext.data.len()).unwrap()
            );
            let mut stored_bytes = Vec::new();
            stored.stream.read_to_end(&mut stored_bytes).await.expect("read stored bytes");
            assert_eq!(i64::try_from(stored_bytes.len()).unwrap(), stored_size);
            match encryption {
                ManagedEncryption::Unencrypted => assert_eq!(stored_bytes, plaintext.data),
                ManagedEncryption::Aes256 => assert_ne!(stored_bytes, plaintext.data),
                ManagedEncryption::AwsKms { .. } => unreachable!("test cases are unencrypted and AES256"),
            }
            let current_head = store
                .read_catalog_head(access.bucket(), &namespace, &name)
                .await
                .expect("committed head");
            let uncommitted = adapter
                .prepare_empty_index(&access, prepare(current_head.expected_head()))
                .await
                .expect("prepare uncommitted higher version");
            assert_ne!(uncommitted.candidate_metadata().object_key(), committed.reference.object_key());
            let reopened_store = Arc::new(
                ObjectTableCatalogStore::for_vector_scope(backend, access.bucket().clone())
                    .await
                    .expect("reopen persistent catalog"),
            );
            let reopened =
                LanceManifestAdapter::new(reopened_store, objects, access.clone(), name.clone(), index.clone(), limits)
                    .expect("reopen Lance adapter");
            assert_eq!(
                reopened
                    .load_current(&access, &resource)
                    .await
                    .expect("reopened current")
                    .reference,
                committed.reference
            );
            assert_eq!(
                reopened
                    .load_committed_version(&access, &resource, NonZeroU64::new(1).expect("initial version"))
                    .await
                    .expect("accurate historical dataset")
                    .reference,
                first.reference
            );
        }
    }

    #[test]
    fn heartbeat_startup_errors_expose_only_stable_codes() {
        let error = startup_heartbeat_error(HeartbeatError::StateIo {
            path: std::path::PathBuf::from("/private/connect/canary/state.json"),
            source: std::io::Error::other("private-source-canary"),
        });

        assert_eq!(error.to_string(), "connect_heartbeat_state_io");
    }

    fn disk(state: &str, runtime_state: Option<&str>, disk_index: i32) -> rustfs_madmin::Disk {
        rustfs_madmin::Disk {
            state: state.to_owned(),
            runtime_state: runtime_state.map(str::to_owned),
            pool_index: 0,
            set_index: 0,
            disk_index,
            total_space: 100,
            available_space: 40,
            ..Default::default()
        }
    }

    fn info(disks: Vec<rustfs_madmin::Disk>) -> rustfs_madmin::StorageInfo {
        let drive_count = disks.len();
        rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![drive_count],
                total_sets: vec![1],
                drives_per_set: vec![drive_count],
                ..Default::default()
            },
            disks,
            ..Default::default()
        }
    }

    #[test]
    fn inventory_rejects_a_partial_startup_storage_snapshot() {
        let mut info = info(vec![disk("ok", Some("online"), 0)]);
        info.backend.standard_sc_data = vec![2];
        info.backend.drives_per_set = vec![2];

        assert!(matches!(
            inventory_snapshot(2, 2, info),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));
    }

    #[test]
    fn inventory_rejects_duplicate_or_invalid_topology_slots() {
        let valid = disk("ok", Some("online"), 0);
        let duplicate = valid.clone();
        assert!(matches!(
            inventory_snapshot(1, 2, info(vec![valid.clone(), duplicate])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));

        let invalid = disk("ok", Some("online"), -1);
        assert!(matches!(
            inventory_snapshot(1, 2, info(vec![valid, invalid])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 2,
                observed: 1
            })
        ));
    }

    #[test]
    fn inventory_rejects_slots_outside_the_configured_geometry() {
        for (pool_index, set_index, disk_index) in [(0, 99, 0), (1, 0, 0), (0, 0, 2)] {
            let valid = disk("ok", Some("online"), 0);
            let mut invalid = disk("ok", Some("online"), disk_index);
            invalid.pool_index = pool_index;
            invalid.set_index = set_index;
            let mut info = info(vec![valid, invalid]);
            info.backend.total_sets = vec![1];
            info.backend.drives_per_set = vec![2];

            assert!(matches!(
                inventory_snapshot(1, 2, info),
                Err(InventoryError::SnapshotIncomplete {
                    expected: 2,
                    observed: 1
                })
            ));
        }
    }

    #[test]
    fn inventory_rejects_invalid_or_overflowing_geometry() {
        for (total_sets, drives_per_set) in [
            (Vec::new(), Vec::new()),
            (vec![1], Vec::new()),
            (Vec::new(), vec![1]),
            (vec![1, 1], vec![1]),
            (vec![0], vec![1]),
            (vec![1], vec![0]),
            (vec![1], vec![2]),
            (vec![usize::MAX], vec![2]),
            (vec![usize::MAX, 1], vec![1, 1]),
        ] {
            let mut info = info(vec![disk("ok", Some("online"), 0)]);
            info.backend.total_sets = total_sets;
            info.backend.drives_per_set = drives_per_set;

            assert!(matches!(
                inventory_snapshot(1, 1, info),
                Err(InventoryError::SnapshotIncomplete {
                    expected: 1,
                    observed: 0
                })
            ));
        }
    }

    #[test]
    fn inventory_marks_a_missing_drive_as_offline() {
        let info = info(vec![disk("ok", None, 0), disk("disk not found", None, 1)]);

        assert_eq!(
            inventory_snapshot(1, 2, info).expect("complete inventory should encode"),
            InventorySnapshot::current(1, 2, 200, 80, [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline])
                .expect("expected inventory should encode")
        );
    }

    #[test]
    fn inventory_health_uses_the_readiness_allow_list() {
        let healthy = info(vec![
            disk("ok", None, 0),
            disk(rustfs_madmin::ITEM_ONLINE, Some("online"), 1),
            disk("unformatted", Some("returning"), 2),
        ]);
        assert_eq!(
            inventory_snapshot(1, 3, healthy).expect("healthy inventory should encode"),
            InventorySnapshot::current(1, 3, 300, 120, []).expect("expected inventory should encode")
        );

        for (state, runtime_state, offline) in [
            ("unknown", None, false),
            ("disk io error", Some("online"), false),
            ("ok", Some("suspect"), false),
            (rustfs_madmin::ITEM_OFFLINE, Some("offline"), true),
        ] {
            let flags = if offline {
                vec![InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline]
            } else {
                vec![InventoryFlag::ClusterDegraded]
            };
            assert_eq!(
                inventory_snapshot(1, 1, info(vec![disk(state, runtime_state, 0)])).expect("degraded inventory should encode"),
                InventorySnapshot::current(1, 1, 100, 40, flags).expect("expected inventory should encode"),
                "state={state}, runtime_state={runtime_state:?}"
            );
        }
    }

    #[test]
    fn inventory_logs_offline_drive_count_without_identifiers() {
        #[derive(Clone, Default)]
        struct CapturedLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

        struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

        impl std::io::Write for CapturedLogWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("captured log lock").extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLog {
            type Writer = CapturedLogWriter;

            fn make_writer(&'writer self) -> Self::Writer {
                CapturedLogWriter(self.0.clone())
            }
        }

        const ENDPOINT_CANARY: &str = "https://inventory-endpoint-secret.invalid";
        const PATH_CANARY: &str = "/inventory/path/secret";
        let mut first = disk(rustfs_madmin::ITEM_OFFLINE, Some("offline"), 0);
        first.endpoint = ENDPOINT_CANARY.to_owned();
        first.drive_path = PATH_CANARY.to_owned();
        let mut second = disk("ok", Some("online"), 1);
        second.endpoint = "https://second-secret.invalid".to_owned();
        second.drive_path = "/second/path/secret".to_owned();
        second.total_space = 900;
        second.available_space = 800;
        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![2],
                total_sets: vec![1],
                drives_per_set: vec![2],
                ..Default::default()
            },
            disks: vec![first, second],
            ..Default::default()
        };
        let captured = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(captured.clone())
            .finish();
        let snapshot =
            tracing::subscriber::with_default(subscriber, || inventory_snapshot(1, 2, info).expect("inventory should encode"));

        assert_eq!(
            snapshot,
            InventorySnapshot::current(1, 2, 1_000, 840, [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline])
                .expect("expected inventory should encode")
        );
        let encoded = serde_json::to_string(&snapshot).expect("snapshot JSON");
        let logs = String::from_utf8(captured.0.lock().expect("captured log lock").clone()).expect("UTF-8 logs");
        assert!(logs.contains("drive_unavailable"));
        assert!(logs.contains("offline_drive_count=1"));
        for canary in [ENDPOINT_CANARY, PATH_CANARY, "second-secret", "/second/path"] {
            assert!(!encoded.contains(canary), "snapshot exposed {canary}");
            assert!(!logs.contains(canary), "logs exposed {canary}");
        }
    }

    #[test]
    fn inventory_capacity_rejects_invalid_numeric_topology() {
        let mut invalid = disk("ok", Some("online"), -1);
        invalid.pool_index = -1;

        assert!(matches!(
            inventory_capacity(&info(vec![invalid])),
            Err(InventoryError::SnapshotIncomplete {
                expected: 1,
                observed: 0
            })
        ));
    }

    #[test]
    fn inventory_capacity_falls_back_to_unique_numeric_topology() {
        let mut pool_zero = disk("ok", Some("online"), 0);
        pool_zero.total_space = 100;
        pool_zero.available_space = 40;
        let mut pool_one = disk("ok", Some("online"), 0);
        pool_one.pool_index = 1;
        pool_one.set_index = 2;
        pool_one.total_space = 200;
        pool_one.available_space = 80;
        let empty_widths = rustfs_madmin::StorageInfo {
            disks: vec![pool_zero.clone(), pool_one.clone()],
            ..Default::default()
        };
        assert_eq!(inventory_capacity(&empty_widths).expect("numeric fallback"), (300, 120));

        let incomplete_widths = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1],
                ..Default::default()
            },
            disks: vec![pool_zero, pool_one],
            ..Default::default()
        };
        assert_eq!(inventory_capacity(&incomplete_widths).expect("numeric fallback"), (300, 120));
    }

    #[test]
    fn inventory_capacity_aggregates_multiple_pools_and_sets_once() {
        let mut pool_zero_set_zero = disk("ok", Some("online"), 0);
        pool_zero_set_zero.total_space = 100;
        pool_zero_set_zero.available_space = 40;
        let mut pool_zero_set_one = pool_zero_set_zero.clone();
        pool_zero_set_one.set_index = 1;
        let mut pool_one = pool_zero_set_zero.clone();
        pool_one.pool_index = 1;
        pool_one.total_space = 200;
        pool_one.available_space = 80;
        let info = rustfs_madmin::StorageInfo {
            backend: rustfs_madmin::BackendInfo {
                standard_sc_data: vec![1, 1],
                ..Default::default()
            },
            disks: vec![pool_zero_set_zero, pool_zero_set_one, pool_one],
            ..Default::default()
        };

        assert_eq!(inventory_capacity(&info).expect("configured topology"), (400, 160));
    }

    #[test]
    fn inventory_capacity_overflow_is_rejected() {
        let mut first = disk("ok", Some("online"), 0);
        first.total_space = u64::MAX;
        let mut second = disk("ok", Some("online"), 1);
        second.total_space = 1;
        let info = rustfs_madmin::StorageInfo {
            disks: vec![first, second],
            ..Default::default()
        };

        assert!(matches!(inventory_capacity(&info), Err(InventoryError::Capacity)));
    }
}

#[cfg(test)]
mod managed_bridge_tests {
    use super::*;
    use crate::app::object::ManagedEncryption;
    use crate::storage_api::startup::services::{ManagedObjectAccess, ManagedObjectOperation};
    use crate::table_catalog::*;
    use crate::vector::lance::{
        LanceManifestAdapter, LanceManifestError, ManagedManifestObjects, ManifestIoLimits, ManifestPrepareRequest,
        VectorDistanceMetric, VectorIndexManifestSpec,
    };
    use bytes::Bytes;
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    struct BridgeFixture {
        request: Arc<crate::app::RequestEnvelope>,
        access: ManagedObjectAccess,
        storage: Arc<ECStore>,
        definition: VectorIndexDefinitionV1,
        guard: Arc<VectorCatalogOperationGuard>,
        objects: Arc<ManagedLanceObjects>,
        limits: ManifestIoLimits,
    }

    fn dataset_operations() -> BTreeSet<ManagedObjectOperation> {
        [
            ManagedObjectOperation::CreateOnly,
            ManagedObjectOperation::Get,
            ManagedObjectOperation::Head,
            ManagedObjectOperation::Range,
            ManagedObjectOperation::CreateMultipart,
            ManagedObjectOperation::UploadPart,
            ManagedObjectOperation::CompleteMultipart,
            ManagedObjectOperation::AbortMultipart,
        ]
        .into_iter()
        .collect()
    }

    async fn fixture(encryption: ManagedEncryption) -> (BridgeFixture, impl Send) {
        fixture_with_kms_authorizer(encryption, None).await
    }

    async fn fixture_with_kms_authorizer(
        encryption: ManagedEncryption,
        authorizer: Option<Arc<dyn crate::app::storage_api::test::sse::KmsKeyAuthorizer>>,
    ) -> (BridgeFixture, impl Send) {
        let (request, initial_access, storage, reset) =
            crate::app::object::managed_request_fixture_with_encryption(encryption.clone()).await;
        let kms_key_dir = if let ManagedEncryption::AwsKms { key_id } = &encryption {
            use crate::app::storage_api::test::sse::{kms_sse_dek_provider_for_test, set_sse_dek_provider_for_test};
            use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
            let directory = tempfile::TempDir::new().expect("KMS key directory");
            let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
            manager
                .reconfigure(rustfs_kms::KmsConfig::local(directory.path().to_path_buf()).with_insecure_development_defaults())
                .await
                .expect("instance KMS configuration");
            manager
                .get_encryption_service()
                .await
                .expect("KMS service")
                .create_key(CreateKeyRequest {
                    key_name: Some(key_id.clone()),
                    key_usage: KeyUsage::EncryptDecrypt,
                    description: None,
                    policy: None,
                    tags: Default::default(),
                    origin: None,
                })
                .await
                .expect("bridge KMS key");
            set_sse_dek_provider_for_test(kms_sse_dek_provider_for_test(manager).await.expect("real KMS provider"));
            Some(directory)
        } else {
            None
        };
        let request = if let Some(authorizer) = authorizer {
            let mut extensions = request.residual_extensions().clone();
            extensions.insert(request.request_context().expect("fixture request context").clone());
            extensions.insert(request.server_context().expect("fixture server context").clone());
            extensions.insert(request.req_info().expect("fixture request identity").clone());
            extensions.insert(crate::app::storage_api::test::sse::TestAuthorizationHooks {
                enforced: true,
                authorizer,
            });
            Arc::new(crate::app::RequestEnvelope::from_parts(
                crate::app::RequestHead {
                    method: request.method().clone(),
                    uri: request.uri().clone(),
                    headers: request.headers().clone(),
                    credentials: request.credentials().cloned(),
                    region: request.region().map(str::to_owned),
                    service: request.service().map(str::to_owned),
                },
                extensions,
            ))
        } else {
            request
        };
        let resource = CatalogResourceIdentity::try_new(
            initial_access.bucket().clone(),
            initial_access.table_id().clone(),
            CatalogFormat::Lance,
        )
        .unwrap();
        let access = ManagedObjectAccess::try_from_parts(
            initial_access.bucket().clone(),
            initial_access.table_id().clone(),
            resource.dataset_prefix(),
            dataset_operations(),
        )
        .unwrap();
        let limits = ManifestIoLimits {
            max_manifest_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            max_concurrent_reads: NonZeroUsize::new(4).unwrap(),
        };
        let bootstrap = managed_lance_objects(access.clone(), request.clone(), encryption.clone(), limits).unwrap();
        let backend = EcStoreTableCatalogObjectBackend::new_with_managed_candidate_reader(storage.clone(), bootstrap.clone());
        let catalog = Arc::new(
            ObjectTableCatalogStore::for_vector_scope(backend, access.bucket().clone())
                .await
                .unwrap(),
        );
        let bucket_id = access.bucket().storage_bucket_id().as_str();
        catalog
            .put_table_bucket(TableBucketEntry {
                vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(access.bucket()).unwrap()),
                version: 2,
                table_bucket: bucket_id.to_owned(),
                catalog_type: "s3vectors".into(),
                warehouse_root: format!("s3://{bucket_id}/"),
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await
            .unwrap();
        let namespace = Namespace::parse("indexes").unwrap();
        catalog
            .create_namespace(NamespaceEntry {
                version: 1,
                table_bucket: bucket_id.to_owned(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                created_at: None,
                updated_at: None,
            })
            .await
            .unwrap();
        let definition = VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: match encryption {
                ManagedEncryption::Unencrypted => VectorEffectiveEncryptionV1::Unencrypted,
                ManagedEncryption::Aes256 => VectorEffectiveEncryptionV1::Aes256,
                ManagedEncryption::AwsKms { key_id } => VectorEffectiveEncryptionV1::AwsKms {
                    kms_key_arn: format!("arn:aws:kms:us-east-1::key/{key_id}"),
                    key_id,
                },
            },
            tags: Default::default(),
        };
        let index = VectorIndexManifestSpec {
            schema_version: 1,
            dimension: NonZeroU32::new(3).unwrap(),
            distance_metric: VectorDistanceMetric::Cosine,
            non_filterable_metadata_keys: BTreeSet::new(),
            encryption_descriptor_sha256: definition.effective_encryption.descriptor_sha256().unwrap(),
        };
        let name = CatalogName::parse("index.v3").unwrap();
        let adapter =
            LanceManifestAdapter::new(catalog.clone(), bootstrap, access.clone(), name.clone(), index.clone(), limits).unwrap();
        let mut entry = TableEntry {
            version: 2,
            table_bucket: bucket_id.to_owned(),
            namespace: namespace.public_name(),
            table: name.as_str().to_owned(),
            table_id: resource.table_id().as_str().to_owned(),
            table_uuid: resource.table_id().as_str().to_owned(),
            format: "lance".into(),
            format_version: 1,
            warehouse_location: format!("s3://{bucket_id}/{}", resource.dataset_prefix().trim_end_matches('/')),
            metadata_location: String::new(),
            version_token: "initial-token".into(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: Default::default(),
            created_at: Some("2026-11-16T00:00:00Z".into()),
            updated_at: None,
            vector_index_definition: None,
            vector_head_binding: None,
        };
        let initial = adapter
            .prepare_empty_index(
                &access,
                ManifestPrepareRequest {
                    parts: CatalogCommitParts {
                        resource_identity: resource.clone(),
                        namespace: namespace.clone(),
                        name: name.clone(),
                        expected_head: CatalogExpectedHead::from_entry(&entry, None, None).unwrap(),
                        commit_id: CatalogCommitId::try_new(uuid::Uuid::new_v4().to_string()).unwrap(),
                        operation: "test".into(),
                        idempotency_key: None,
                        writer: None,
                    },
                    index,
                },
            )
            .await
            .expect("real initial Lance manifest");
        entry.version = 3;
        entry.vector_index_definition = Some(definition.clone());
        entry.metadata_location = initial.candidate_metadata().object_key().as_str().to_owned();
        entry.vector_head_binding =
            Some(VectorHeadBindingV1::try_new(&resource, initial.candidate_metadata(), initial.format_descriptor()).unwrap());
        catalog.create_table(entry).await.expect("real v3 Catalog entry");
        let (guard, current) = catalog
            .begin_vector_operation(access.bucket(), &namespace, &name)
            .await
            .unwrap();
        assert_eq!(current.head().metadata(), initial.candidate_metadata());
        let guard = Arc::new(guard);
        guard.lifecycle_guard().expect("real ECStore bucket guard");
        let objects = guarded_managed_lance_objects(access.clone(), request.clone(), current.definition(), guard.clone(), limits)
            .expect("guard-aware Managed Dataset bridge");
        (
            BridgeFixture {
                request,
                access,
                storage,
                definition,
                guard,
                objects,
                limits,
            },
            (reset, kms_key_dir),
        )
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_exact_parts_complete_retry_and_ranges() {
        for encryption in [ManagedEncryption::Unencrypted, ManagedEncryption::Aes256] {
            let (fixture, _reset) = fixture(encryption).await;
            let key = CatalogObjectKey::try_new(format!("{}data/parts.lance", fixture.access.allowed_prefix())).unwrap();
            let upload = fixture.objects.create_multipart(&fixture.access, &key).await.unwrap();
            assert!(matches!(
                upload.put_part(0, vec![Bytes::from_static(b"zero")], 4).await,
                Err(LanceManifestError::ReferenceMismatch { .. })
            ));
            upload.put_part(2, vec![Bytes::from_static(b"tail")], 4).await.unwrap();
            assert!(matches!(upload.complete(2).await, Err(LanceManifestError::ReferenceMismatch { .. })));
            let first = Bytes::from(vec![7_u8; 5 * 1024 * 1024]);
            let first_size = u64::try_from(first.len()).unwrap();
            upload.put_part(1, vec![first.clone()], first_size).await.unwrap();
            assert!(matches!(
                upload.put_part(1, vec![Bytes::from_static(b"replacement")], 11).await,
                Err(LanceManifestError::ReferenceMismatch { .. })
            ));
            assert!(matches!(upload.complete(0).await, Err(LanceManifestError::ReferenceMismatch { .. })));
            assert!(matches!(upload.complete(3).await, Err(LanceManifestError::ReferenceMismatch { .. })));
            let completed = upload.complete(2).await.unwrap();
            assert_eq!(completed.size, first_size + 4);
            assert!(completed.body_revision.is_some());
            assert_eq!(upload.complete(2).await.unwrap(), completed);
            assert!(matches!(upload.complete(1).await, Err(LanceManifestError::ReferenceMismatch { .. })));
            assert_eq!(fixture.objects.head(&fixture.access, &key).await.unwrap(), completed);
            let data = fixture
                .objects
                .get(&fixture.access, &key, None, completed.body_revision)
                .await
                .unwrap();
            assert_eq!(&data.data[..first.len()], first.as_ref());
            assert_eq!(&data.data[first.len()..], b"tail");
            let boundary = first_size;
            let range = fixture
                .objects
                .get(&fixture.access, &key, Some(boundary - 2..boundary + 4), completed.body_revision)
                .await
                .unwrap();
            assert_eq!(range.data, [7, 7, b't', b'a', b'i', b'l']);
            assert!(
                fixture
                    .objects
                    .get(&fixture.access, &key, None, Some(uuid::Uuid::new_v4()))
                    .await
                    .is_err()
            );
            upload.abort().await.unwrap();
            assert_eq!(fixture.objects.head(&fixture.access, &key).await.unwrap(), completed);
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_abort_preserves_create_only_object() {
        let (fixture, _reset) = fixture(ManagedEncryption::Unencrypted).await;
        let key = CatalogObjectKey::try_new(format!("{}data/abort.lance", fixture.access.allowed_prefix())).unwrap();
        let upload = fixture.objects.create_multipart(&fixture.access, &key).await.unwrap();
        upload.put_part(1, vec![Bytes::from_static(b"staging")], 7).await.unwrap();
        upload.abort().await.unwrap();
        upload.abort().await.unwrap();
        assert!(matches!(
            fixture.objects.head(&fixture.access, &key).await,
            Err(LanceManifestError::ObjectNotFound { .. })
        ));
        assert!(matches!(upload.complete(1).await, Err(LanceManifestError::ReferenceMismatch { .. })));
        let created = fixture
            .objects
            .create_only(&fixture.access, &key, Bytes::from_static(b"live"))
            .await
            .unwrap();
        assert!(matches!(
            fixture
                .objects
                .create_only(&fixture.access, &key, Bytes::from_static(b"other"))
                .await,
            Err(LanceManifestError::CandidateAlreadyExists { .. })
        ));
        upload.abort().await.unwrap();
        let read = fixture
            .objects
            .get(&fixture.access, &key, None, created.body_revision)
            .await
            .unwrap();
        assert_eq!(read.data, b"live");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_rejects_another_resource_scope() {
        use crate::storage_api::table::managed::ManagedTableId;
        let (fixture, _reset) = fixture(ManagedEncryption::Unencrypted).await;
        let id = ManagedTableId::try_new(uuid::Uuid::new_v4().to_string()).unwrap();
        let other = ManagedObjectAccess::try_from_parts(
            fixture.access.bucket().clone(),
            id.clone(),
            format!(".rustfs-table/{}/dataset/", id.as_str()),
            dataset_operations(),
        )
        .unwrap();
        assert!(matches!(
            guarded_managed_lance_objects(
                other.clone(),
                fixture.request.clone(),
                &fixture.definition,
                fixture.guard.clone(),
                fixture.limits,
            ),
            Err(LanceManifestError::ResourceMismatch { .. })
        ));
        let key = CatalogObjectKey::try_new(format!("{}data/foreign.lance", other.allowed_prefix())).unwrap();
        assert!(matches!(
            fixture.objects.create_multipart(&other, &key).await,
            Err(LanceManifestError::ManagedAccessDenied(_))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_reuses_lifecycle_with_waiting_bucket_delete() {
        use crate::storage_api::startup::services::{BucketOperations as _, DeleteBucketOptions};
        let (fixture, _reset) = fixture(ManagedEncryption::Unencrypted).await;
        let lock_counts = || {
            crate::storage_api::startup::storage::bootstrap_instance_ctx()
                .lock_manager()
                .as_fast_lock_manager()
                .expect("fixture lock manager")
                .current_lock_counts()
        };
        let before = lock_counts().1;
        let storage = fixture.storage.clone();
        let bucket = fixture.access.bucket().storage_bucket_id().as_str().to_owned();
        let writer = tokio::spawn(async move { storage.delete_bucket(&bucket, &DeleteBucketOptions::default()).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while lock_counts().1 <= before {
                assert!(!writer.is_finished());
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bucket writer reaches the real lock queue");
        let key = CatalogObjectKey::try_new(format!("{}data/queued.lance", fixture.access.allowed_prefix())).unwrap();
        let upload = tokio::time::timeout(Duration::from_secs(5), fixture.objects.create_multipart(&fixture.access, &key))
            .await
            .expect("multipart must reuse the admitted bucket guard")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), upload.put_part(1, vec![Bytes::from_static(b"guarded")], 7))
            .await
            .expect("part must reuse the admitted bucket guard")
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), upload.complete(1))
            .await
            .expect("completion must reuse the admitted bucket guard")
            .unwrap();
        let head = tokio::time::timeout(Duration::from_secs(5), fixture.objects.head(&fixture.access, &key))
            .await
            .expect("HEAD must reuse the admitted bucket guard")
            .unwrap();
        assert_eq!(head, completed);
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            fixture.objects.get(&fixture.access, &key, None, completed.body_revision),
        )
        .await
        .expect("GET must reuse the admitted bucket guard")
        .unwrap();
        assert_eq!(read.data, b"guarded");
        assert!(!writer.is_finished());
        drop(upload);
        drop(fixture);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(10), writer).await.unwrap().unwrap(),
            Err(crate::storage_api::startup::services::StorageError::BucketNotEmpty(_))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_create_cancellation_retains_catalog_owner_at_native_publication() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};
        let (fixture, _reset) = fixture(ManagedEncryption::Unencrypted).await;
        let key = CatalogObjectKey::try_new(format!("{}data/cancelled-create.lance", fixture.access.allowed_prefix())).unwrap();
        let barrier = PutObjectCommitBarrier::install(
            fixture.access.bucket().storage_bucket_id().as_str(),
            key.as_str(),
            PutObjectCommitPause::AfterRenameQuorum,
        );
        let guard = Arc::downgrade(&fixture.guard);
        let objects = fixture.objects.clone();
        let access = fixture.access.clone();
        drop(fixture);
        let mut caller = tokio::spawn(async move { objects.create_only(&access, &key, Bytes::from_static(b"durable")).await });
        tokio::select! {
            result = &mut caller => panic!("create returned before native publication pause: {result:?}"),
            paused = tokio::time::timeout(Duration::from_secs(10), barrier.wait_until_paused()) => {
                paused.expect("create reaches native publication pause");
            },
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let retained = guard
            .upgrade()
            .expect("accepted native publication retains the exact Catalog owner after cancellation");
        retained
            .check_held()
            .expect("Catalog GC and lifecycle protection remain valid");
        drop(retained);
        barrier.release();
        tokio::time::timeout(Duration::from_secs(10), async {
            while guard.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Catalog owner releases after native publication finishes");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_complete_cancellation_retains_catalog_owner_at_native_publication() {
        use crate::app::storage_api::test::set_disk::{MultipartCommitBarrier, MultipartCommitPause};
        let (fixture, _reset) = fixture(ManagedEncryption::Unencrypted).await;
        let key = CatalogObjectKey::try_new(format!("{}data/cancelled-complete.lance", fixture.access.allowed_prefix())).unwrap();
        let upload = fixture.objects.create_multipart(&fixture.access, &key).await.unwrap();
        upload.put_part(1, vec![Bytes::from_static(b"durable")], 7).await.unwrap();
        let barrier = MultipartCommitBarrier::install(
            fixture.access.bucket().storage_bucket_id().as_str(),
            key.as_str(),
            MultipartCommitPause::AfterObjectPublication,
        );
        let guard = Arc::downgrade(&fixture.guard);
        drop(fixture);
        let mut caller = tokio::spawn(async move { upload.complete(1).await });
        tokio::select! {
            result = &mut caller => panic!("Complete returned before native publication pause: {result:?}"),
            paused = tokio::time::timeout(Duration::from_secs(10), barrier.wait_until_paused()) => {
                paused.expect("Complete reaches native publication pause");
            },
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let retained = guard
            .upgrade()
            .expect("accepted Complete retains the exact Catalog owner after cancellation");
        retained
            .check_held()
            .expect("Catalog GC and lifecycle protection remain valid");
        drop(retained);
        barrier.release();
        tokio::time::timeout(Duration::from_secs(10), async {
            while guard.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Catalog owner releases after native Complete finishes");
    }
    struct RevocableMultipartKmsAuthorizer {
        allow_writes: std::sync::atomic::AtomicBool,
        calls: std::sync::Mutex<Vec<(rustfs_policy::policy::action::KmsAction, String)>>,
    }

    #[async_trait::async_trait]
    impl crate::app::storage_api::test::sse::KmsKeyAuthorizer for RevocableMultipartKmsAuthorizer {
        async fn is_allowed(
            &self,
            _principal: &crate::app::storage_api::sse::SseKmsPrincipal,
            action: rustfs_policy::policy::action::KmsAction,
            key_id: &str,
        ) -> std::result::Result<bool, crate::error::ApiError> {
            self.calls
                .lock()
                .expect("KMS authorization calls")
                .push((action, key_id.to_owned()));
            Ok(action != rustfs_policy::policy::action::KmsAction::GenerateDataKeyAction
                || self.allow_writes.load(std::sync::atomic::Ordering::SeqCst))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_bridge_v3_complete_retry_rechecks_revoked_kms_write_permission() {
        use rustfs_policy::policy::action::KmsAction;
        use std::sync::atomic::{AtomicBool, Ordering};
        let authorizer = Arc::new(RevocableMultipartKmsAuthorizer {
            allow_writes: AtomicBool::new(true),
            calls: std::sync::Mutex::new(Vec::new()),
        });
        let kms_key_id = "managed-bridge-retry-key";
        let (fixture, _reset) = fixture_with_kms_authorizer(
            ManagedEncryption::AwsKms {
                key_id: kms_key_id.to_owned(),
            },
            Some(authorizer.clone()),
        )
        .await;
        let key = CatalogObjectKey::try_new(format!("{}data/kms-retry.lance", fixture.access.allowed_prefix())).unwrap();
        let upload = fixture.objects.create_multipart(&fixture.access, &key).await.unwrap();
        upload.put_part(1, vec![Bytes::from_static(b"kmsdata")], 7).await.unwrap();
        let completed = upload.complete(1).await.expect("first completion publishes the KMS object");
        assert_eq!(completed.size, 7);
        assert!(completed.body_revision.is_some());

        authorizer.allow_writes.store(false, Ordering::SeqCst);
        authorizer.calls.lock().expect("KMS authorization calls").clear();
        assert_eq!(
            fixture
                .objects
                .head(&fixture.access, &key)
                .await
                .expect("Decrypt remains authorized"),
            completed
        );
        {
            let calls = authorizer.calls.lock().expect("KMS authorization calls");
            assert!(
                calls
                    .iter()
                    .any(|(action, key)| *action == KmsAction::DecryptAction && key == kms_key_id)
            );
            assert!(calls.iter().all(|(action, _)| *action != KmsAction::GenerateDataKeyAction));
        }
        let error = upload
            .complete(1)
            .await
            .expect_err("completion retry must authorize the current KMS write permission");
        let LanceManifestError::Storage { source, .. } = error else {
            panic!("completion retry returned an unexpected error: {error}");
        };
        assert!(
            matches!(
                source.downcast_ref::<crate::app::object::ManagedObjectError>(),
                Some(crate::app::object::ManagedObjectError::Api(api)) if api.code.as_str() == "AccessDenied"
            ),
            "completion retry must preserve KMS AccessDenied: {source}"
        );
        assert!(
            authorizer
                .calls
                .lock()
                .expect("KMS authorization calls")
                .iter()
                .any(|(action, key)| *action == KmsAction::GenerateDataKeyAction && key == kms_key_id)
        );
        authorizer.allow_writes.store(true, Ordering::SeqCst);
        assert_eq!(
            upload
                .complete(1)
                .await
                .expect("restored KMS permission admits the exact completed object"),
            completed
        );
    }
}
