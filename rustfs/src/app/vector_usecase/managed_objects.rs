// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Application-owned Managed Object and Multipart bridge for Vector datasets and maintenance.

use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::BucketResolveError;
use crate::app::object::{
    DefaultObjectUsecase, ManagedEncryption, ManagedMultipartUpload, ManagedObjectContext, ManagedObjectError,
};
use crate::app::storage_api::managed::{BucketHandle, BucketIdentityError, BucketIncarnationFenceGuard, ManagedObjectAccess};
use crate::app::storage_api::multipart_usecase::contract::multipart::CompletePart;
use crate::app::storage_api::object_usecase::{StorageObjectInfo, request_context::spawn_traced_join};
use crate::runtime_sources::AppContext;
use crate::table_catalog::{
    CatalogFormat, CatalogIoErrorKind, CatalogManagedCandidateReader, CatalogMetadataRef, CatalogObjectKey, TableCatalogObject,
    TableCatalogObjectMetadata, TableCatalogStoreError, TableCatalogStoreResult, VectorCatalogOperationGuard,
    VectorEffectiveEncryptionV1, VectorIndexDefinitionV1, VectorMaintenanceDispatchGuard,
};
use crate::vector::lance::{LanceManifestError, ManagedDatasetMultipart, ManagedManifestObjects, ManifestIoLimits};
use std::{collections::BTreeMap, future::Future, ops::Range, sync::Arc};

#[derive(Clone)]
pub(crate) struct ManagedLanceObjects {
    #[cfg(test)]
    instance_id: uuid::Uuid,
    usecase: DefaultObjectUsecase,
    context: ManagedObjectContext,
    max_bytes: usize,
    operation_guard: Option<Arc<VectorCatalogOperationGuard>>,
}

pub(crate) fn managed_lance_objects(
    access: ManagedObjectAccess,
    request: Arc<RequestEnvelope>,
    encryption: ManagedEncryption,
    limits: ManifestIoLimits,
) -> Result<Arc<ManagedLanceObjects>, LanceManifestError> {
    let app =
        request
            .server_context()
            .and_then(|slot| slot.installed_app_context())
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "managed manifest request has no installed application context",
            })?;
    let context = ManagedObjectContext::from_authorized_request(access, request, encryption).map_err(|source| {
        LanceManifestError::Storage {
            operation: "prepare managed manifest access",
            source: Box::new(source),
        }
    })?;
    let max_bytes = usize::try_from(limits.max_manifest_bytes.get()).map_err(|source| LanceManifestError::Storage {
        operation: "convert manifest read budget",
        source: Box::new(source),
    })?;
    Ok(Arc::new(ManagedLanceObjects {
        #[cfg(test)]
        instance_id: app.object_store().id,
        usecase: DefaultObjectUsecase::with_context(Some(app)),
        context,
        max_bytes,
        operation_guard: None,
    }))
}

#[async_trait::async_trait]
impl CatalogManagedCandidateReader for ManagedLanceObjects {
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
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
                source: Box::new(BucketIdentityError::ObjectOutsideScope),
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
            Err(ManagedObjectError::Api(api)) if matches!(api.code.as_str(), "NoSuchKey" | "NoSuchVersion") => {
                return Ok(None);
            }
            Err(error) => return Err(managed_candidate_error(error)),
        };
        Ok(Some(TableCatalogObject {
            data: bytes.to_vec(),
            body_revision: info.data_dir.filter(|revision| !revision.is_nil()),
            etag: info.etag,
            mod_time: info.mod_time,
        }))
    }
}

fn managed_candidate_error(error: ManagedObjectError) -> TableCatalogStoreError {
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

impl ManagedLanceObjects {
    fn check_access(&self, access: &ManagedObjectAccess) -> Result<(), LanceManifestError> {
        if access != self.context.access() {
            return Err(BucketIdentityError::ObjectOutsideScope.into());
        }
        if let Some(guard) = &self.operation_guard {
            guard.check_held()?;
        }
        Ok(())
    }

    fn metadata(info: StorageObjectInfo) -> Result<TableCatalogObjectMetadata, LanceManifestError> {
        let size = info
            .get_actual_size()
            .and_then(|size| u64::try_from(size).map_err(std::io::Error::other))
            .map_err(|source| LanceManifestError::Storage {
                operation: "read managed manifest plaintext length",
                source: Box::new(source),
            })?;
        Ok(TableCatalogObjectMetadata {
            body_revision: info.data_dir.filter(|revision| !revision.is_nil()),
            etag: info.etag,
            mod_time: info.mod_time,
            size,
        })
    }

    fn error(operation: &'static str, key: &str, error: ManagedObjectError) -> LanceManifestError {
        if let ManagedObjectError::Api(api) = &error
            && matches!(api.code.as_str(), "NoSuchKey" | "NoSuchVersion")
        {
            return LanceManifestError::ObjectNotFound { key: key.to_owned() };
        }
        LanceManifestError::Storage {
            operation,
            source: Box::new(error),
        }
    }
}

#[async_trait::async_trait]
impl ManagedManifestObjects for ManagedLanceObjects {
    #[cfg(test)]
    fn fault_instance_id(&self) -> Option<uuid::Uuid> {
        Some(self.instance_id)
    }

    async fn create_only(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
        content: bytes::Bytes,
    ) -> Result<TableCatalogObjectMetadata, LanceManifestError> {
        self.check_access(access)?;
        let size = u64::try_from(content.len()).map_err(|source| LanceManifestError::Storage {
            operation: "measure managed manifest",
            source: Box::new(source),
        })?;
        if let Some(guard) = &self.operation_guard {
            let native = guard.lifecycle_guard()?;
            let objects = self.clone();
            let key = key.clone();
            return run_managed_bridge_mutation(async move {
                objects.check_access(objects.context.access())?;
                let info = objects
                    .usecase
                    .managed_create_object_with_guard(
                        &objects.context,
                        key.as_str(),
                        size,
                        futures::stream::once(std::future::ready(Ok(content))),
                        native,
                    )
                    .await
                    .map_err(|error| {
                        if matches!(&error, ManagedObjectError::Api(api) if api.code.as_str() == "PreconditionFailed") {
                            LanceManifestError::CandidateAlreadyExists {
                                key: key.as_str().to_owned(),
                            }
                        } else {
                            Self::error("create Managed Dataset object", key.as_str(), error)
                        }
                    })?;
                objects.check_access(objects.context.access())?;
                Self::metadata(info)
            })
            .await;
        }
        let body = futures::stream::once(std::future::ready(Ok(content)));
        let info = self
            .usecase
            .managed_create_object(&self.context, key.as_str(), size, body)
            .await
            .map_err(|error| {
                if matches!(&error, ManagedObjectError::Api(api) if api.code.as_str() == "PreconditionFailed") {
                    return LanceManifestError::CandidateAlreadyExists {
                        key: key.as_str().to_owned(),
                    };
                }
                Self::error("create managed manifest", key.as_str(), error)
            })?;
        Self::metadata(info)
    }

    async fn create_multipart(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
    ) -> Result<Arc<dyn ManagedDatasetMultipart>, LanceManifestError> {
        self.check_access(access)?;
        let native = self
            .operation_guard
            .as_ref()
            .ok_or(BucketIdentityError::OperationNotAllowed)?
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
            }) as Arc<dyn ManagedDatasetMultipart>)
        })
        .await
    }

    async fn get(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
        range: Option<Range<u64>>,
        expected_body_revision: Option<uuid::Uuid>,
    ) -> Result<TableCatalogObject, LanceManifestError> {
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
        Ok(TableCatalogObject {
            data: bytes.to_vec(),
            body_revision: metadata.body_revision,
            etag: metadata.etag,
            mod_time: metadata.mod_time,
        })
    }

    async fn head(
        &self,
        access: &ManagedObjectAccess,
        key: &CatalogObjectKey,
    ) -> Result<TableCatalogObjectMetadata, LanceManifestError> {
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

pub(crate) fn guarded_managed_lance_objects(
    access: ManagedObjectAccess,
    request: Arc<RequestEnvelope>,
    definition: &VectorIndexDefinitionV1,
    guard: Arc<VectorCatalogOperationGuard>,
    limits: ManifestIoLimits,
) -> Result<Arc<ManagedLanceObjects>, LanceManifestError> {
    guarded_managed_objects(access, request, definition, guard, limits, ManagedCatalogObjectScope::Dataset)
}

pub(super) fn guarded_managed_query_results(
    access: ManagedObjectAccess,
    request: Arc<RequestEnvelope>,
    definition: &VectorIndexDefinitionV1,
    guard: Arc<VectorCatalogOperationGuard>,
    limits: ManifestIoLimits,
) -> Result<Arc<ManagedLanceObjects>, LanceManifestError> {
    guarded_managed_objects(access, request, definition, guard, limits, ManagedCatalogObjectScope::QueryResults)
}

enum ManagedCatalogObjectScope {
    Dataset,
    QueryResults,
}

fn guarded_managed_objects(
    access: ManagedObjectAccess,
    request: Arc<RequestEnvelope>,
    definition: &VectorIndexDefinitionV1,
    guard: Arc<VectorCatalogOperationGuard>,
    limits: ManifestIoLimits,
    scope: ManagedCatalogObjectScope,
) -> Result<Arc<ManagedLanceObjects>, LanceManifestError> {
    definition.validate()?;
    guard.check_held()?;
    let resource = guard.resource_identity();
    let prefix = match scope {
        ManagedCatalogObjectScope::Dataset => resource.dataset_prefix(),
        ManagedCatalogObjectScope::QueryResults => format!(".rustfs-table/{}/query-results/", resource.table_id().as_str()),
    };
    if resource.vector_bucket() != Some(access.bucket())
        || resource.table_id() != access.table_id()
        || resource.format() != CatalogFormat::Lance
        || prefix != access.allowed_prefix()
    {
        return Err(LanceManifestError::ResourceMismatch {
            reason: "Managed Dataset access differs from its Catalog operation",
        });
    }
    guard.lifecycle_guard()?;
    let encryption = match &definition.effective_encryption {
        VectorEffectiveEncryptionV1::Unencrypted => ManagedEncryption::Unencrypted,
        VectorEffectiveEncryptionV1::Aes256 => ManagedEncryption::Aes256,
        VectorEffectiveEncryptionV1::AwsKms { key_id, .. } => ManagedEncryption::AwsKms { key_id: key_id.clone() },
    };
    let mut objects = managed_lance_objects(access, request, encryption, limits)?;
    Arc::get_mut(&mut objects)
        .ok_or(LanceManifestError::ReferenceMismatch {
            reason: "new Managed Dataset bridge already has another owner",
        })?
        .operation_guard = Some(guard);
    Ok(objects)
}

pub(super) fn maintenance_managed_lance_objects(
    app: Arc<AppContext>,
    context: ManagedObjectContext,
    guard: Arc<VectorCatalogOperationGuard>,
    limits: ManifestIoLimits,
) -> Result<Arc<ManagedLanceObjects>, LanceManifestError> {
    guard.check_held()?;
    let resource = guard.resource_identity();
    let access = context.access();
    if resource.vector_bucket() != Some(access.bucket())
        || resource.table_id() != access.table_id()
        || resource.dataset_prefix() != access.allowed_prefix()
    {
        return Err(LanceManifestError::ResourceMismatch {
            reason: "maintenance reader differs from its Catalog operation",
        });
    }
    guard.lifecycle_guard()?;
    let max_bytes = usize::try_from(limits.max_manifest_bytes.get()).map_err(|source| LanceManifestError::Storage {
        operation: "convert maintenance manifest budget",
        source: Box::new(source),
    })?;
    Ok(Arc::new(ManagedLanceObjects {
        #[cfg(test)]
        instance_id: app.object_store().id,
        usecase: DefaultObjectUsecase::with_context(Some(app)),
        context,
        max_bytes,
        operation_guard: Some(guard),
    }))
}

struct ManagedDispatchCandidateReader {
    objects: ManagedLanceObjects,
    guard: Arc<VectorMaintenanceDispatchGuard>,
}

#[async_trait::async_trait]
impl CatalogManagedCandidateReader for ManagedDispatchCandidateReader {
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.guard.check_held()?;
        if !std::ptr::eq(self.guard.lifecycle_owner().as_ref(), lifecycle) {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance candidate read received another lifecycle owner".into(),
            ));
        }
        let result =
            CatalogManagedCandidateReader::read_managed_candidate(&self.objects, bucket, metadata, max_size, lifecycle).await?;
        self.guard.check_held()?;
        Ok(result)
    }
}

/// Build a candidate reader that retains its admitted maintenance dispatch.
/// Catalog supplies the existing lifecycle owner for each bounded read.
///
/// Returns an error when the dispatch is lost, its resource differs from the
/// Managed capability, or the manifest limit exceeds the platform address space.
pub(super) fn maintenance_dispatch_candidate_reader(
    app: Arc<AppContext>,
    context: ManagedObjectContext,
    guard: Arc<VectorMaintenanceDispatchGuard>,
    limits: ManifestIoLimits,
) -> Result<Arc<dyn CatalogManagedCandidateReader>, LanceManifestError> {
    guard.check_held()?;
    let resource = guard.lease().resource();
    let access = context.access();
    if resource.vector_bucket() != Some(access.bucket())
        || resource.table_id() != access.table_id()
        || resource.dataset_prefix() != access.allowed_prefix()
    {
        return Err(LanceManifestError::ResourceMismatch {
            reason: "maintenance candidate reader differs from its dispatch resource",
        });
    }
    let max_bytes = usize::try_from(limits.max_manifest_bytes.get()).map_err(|source| LanceManifestError::Storage {
        operation: "convert maintenance candidate budget",
        source: Box::new(source),
    })?;
    Ok(Arc::new(ManagedDispatchCandidateReader {
        objects: ManagedLanceObjects {
            #[cfg(test)]
            instance_id: app.object_store().id,
            usecase: DefaultObjectUsecase::with_context(Some(app)),
            context,
            max_bytes,
            operation_guard: None,
        },
        guard,
    }))
}

async fn run_managed_bridge_mutation<T, F>(operation: F) -> Result<T, LanceManifestError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, LanceManifestError>> + Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let operation = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, operation);
    let operation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        operation,
    );
    spawn_traced_join(operation)
        .await
        .map_err(|source| LanceManifestError::Storage {
            operation: "join Managed Dataset mutation",
            source: Box::new(source),
        })?
}

#[derive(Clone)]
struct ManagedObjectMultipartSession {
    objects: ManagedLanceObjects,
    key: CatalogObjectKey,
    upload: Arc<ManagedMultipartUpload>,
    state: Arc<tokio::sync::Mutex<ManagedObjectMultipartState>>,
}

#[derive(Default)]
struct ManagedObjectMultipartState {
    // An absent receipt retains the identity of an interrupted or failed part.
    parts: BTreeMap<u16, Option<(CompletePart, u64)>>,
    completing: Option<u16>,
    completed: Option<TableCatalogObjectMetadata>,
    aborted: bool,
}

impl ManagedObjectMultipartSession {
    fn invalid(reason: &'static str) -> LanceManifestError {
        LanceManifestError::ReferenceMismatch { reason }
    }

    async fn complete_owned(&self, expected_parts: u16) -> Result<TableCatalogObjectMetadata, LanceManifestError> {
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

    async fn abort_owned(&self) -> Result<(), LanceManifestError> {
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

#[async_trait::async_trait]
impl ManagedDatasetMultipart for ManagedObjectMultipartSession {
    async fn put_part(&self, part_number: u16, content: Vec<bytes::Bytes>, size: u64) -> Result<(), LanceManifestError> {
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

    async fn complete(&self, expected_parts: u16) -> Result<TableCatalogObjectMetadata, LanceManifestError> {
        let session = self.clone();
        run_managed_bridge_mutation(async move { session.complete_owned(expected_parts).await }).await
    }

    async fn abort(&self) -> Result<(), LanceManifestError> {
        let session = self.clone();
        run_managed_bridge_mutation(async move { session.abort_owned().await }).await
    }
}
