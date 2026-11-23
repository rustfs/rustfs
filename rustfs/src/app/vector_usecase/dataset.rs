// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    storage_api::vector::{ManagedObjectAccess, ManagedObjectOperation},
    table_catalog::{
        CatalogManagedCandidateReader, CatalogMetadataRef, TableCatalogObject, TableCatalogStoreResult,
        VectorCatalogOperationGuard,
    },
    vector::service::{ManagedDatasetAccess, VectorRequestAccess},
};

#[derive(Default)]
pub(super) struct CandidateReader {
    objects: parking_lot::RwLock<Option<std::sync::Weak<crate::startup_services::ManagedLanceObjects>>>,
    owned_maintenance: Option<Arc<dyn CatalogManagedCandidateReader>>,
    maintenance: Option<crate::table_catalog::VectorMaintenanceControl>,
    retained_allocations: parking_lot::Mutex<Vec<Arc<crate::table_catalog::VectorMaintenanceMemoryReservation>>>,
}

impl CandidateReader {
    pub(super) fn bind(&self, objects: &Arc<crate::startup_services::ManagedLanceObjects>) {
        *self.objects.write() = Some(Arc::downgrade(objects));
    }

    /// Retain the admitted reader and its allocation budget through Catalog publication.
    pub(super) fn with_owned_maintenance_reader(
        control: crate::table_catalog::VectorMaintenanceControl,
        reader: Arc<dyn CatalogManagedCandidateReader>,
    ) -> Self {
        Self {
            owned_maintenance: Some(reader),
            maintenance: Some(control),
            ..Self::default()
        }
    }

    pub(super) fn with_maintenance_control(control: crate::table_catalog::VectorMaintenanceControl) -> Self {
        Self {
            maintenance: Some(control),
            ..Self::default()
        }
    }
}

#[async_trait::async_trait]
impl CatalogManagedCandidateReader for CandidateReader {
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let reader: Arc<dyn CatalogManagedCandidateReader> = match &self.owned_maintenance {
            Some(reader) => reader.clone(),
            None => self
                .objects
                .read()
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("request has no authorized Managed candidate reader".into())
                })?,
        };
        let Some(control) = &self.maintenance else {
            return reader.read_managed_candidate(bucket, metadata, max_size, lifecycle).await;
        };
        let size = max_size.min(usize::try_from(metadata.content_length()).map_err(|error| TableCatalogStoreError::Io {
            classification: crate::table_catalog::CatalogIoErrorKind::Unavailable,
            context: "measure maintenance candidate read",
            source: Box::new(error),
        })?);
        let budget_error = |error| TableCatalogStoreError::Io {
            classification: crate::table_catalog::CatalogIoErrorKind::Unavailable,
            context: "admit maintenance candidate read",
            source: Box::new(error),
        };
        let peak = control
            .reserve_memory(u64::try_from(size).unwrap_or(u64::MAX).saturating_mul(2))
            .map_err(budget_error)?;
        let io = control.acquire_io().await.map_err(budget_error)?;
        let result = reader.read_managed_candidate(bucket, metadata, size, lifecycle).await?;
        drop(io);
        if let Some(object) = &result {
            let retained = control
                .reserve_memory(u64::try_from(object.data.capacity()).unwrap_or(u64::MAX))
                .map_err(budget_error)?;
            // Catalog consumes an owned Vec. Its binding retains the allocation charge
            // through all validation and publication using this reader.
            self.retained_allocations.lock().push(retained);
        }
        drop(peak);
        Ok(result)
    }
}

pub(super) struct IndexRequest<'a> {
    pub owner: &'a DefaultVectorUsecase,
    pub authorization: &'a AuthorizedVectorRequest,
    pub index: &'a CatalogName,
}

impl IndexRequest<'_> {
    async fn authorized_envelope(
        &self,
        request: &Request,
        resource: &CatalogResourceIdentity,
        definition: &VectorIndexDefinitionV1,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> Result<RequestEnvelope, ProtocolError> {
        let admitted = self.authorization;
        admitted
            .bound
            .require_same(request, admitted.resource.as_ref())
            .map_err(access_error)?;
        if admitted.resource.as_ref().is_some_and(|expected| expected != resource) {
            return Err(denied());
        }
        let bucket = admitted.bucket.as_ref().ok_or_else(unavailable)?;
        if resource.vector_bucket() != Some(bucket) {
            return Err(denied());
        }
        let snapshot = self
            .owner
            .buckets()
            .read_managed_bucket_metadata(bucket, lifecycle)
            .await
            .map_err(bucket_error)?;
        let facts = bucket::authorization_facts(bucket, &snapshot)?;
        let target = if matches!(request, Request::CreateIndex(_)) {
            VectorTarget::NewIndex {
                bucket: &facts,
                index_name: self.index,
            }
        } else {
            VectorTarget::Index {
                bucket: &facts,
                index_name: self.index,
                resource,
                definition,
            }
        };
        admitted.verified.authorize(request, &target).await
    }
}

#[async_trait::async_trait]
impl VectorRequestAccess for IndexRequest<'_> {
    fn request(&self) -> &Request {
        self.authorization.verified.request()
    }
    fn request_id(&self) -> VectorRequestId {
        self.authorization.request_id
    }
    fn bucket(&self) -> &BucketHandle {
        // Construction is restricted to admitted Index targets with a resolved bucket.
        self.authorization
            .bucket
            .as_ref()
            .expect("Index admission retains its bucket")
    }
    fn index_name(&self) -> &CatalogName {
        self.index
    }

    async fn authorize_index(
        &self,
        request: &Request,
        resource: &CatalogResourceIdentity,
        definition: &VectorIndexDefinitionV1,
        operation: Option<&VectorCatalogOperationGuard>,
    ) -> Result<(), VectorServiceError> {
        let lifecycle = if let Some(operation) = operation {
            operation.lifecycle_guard()?
        } else {
            Arc::new(
                self.owner
                    .buckets()
                    .acquire_bucket_handle_guard(self.bucket())
                    .await
                    .map_err(bucket_error)?,
            )
        };
        let envelope = self.authorized_envelope(request, resource, definition, &lifecycle).await?;
        if matches!(request, Request::CreateIndex(_)) {
            encryption::preflight(&self.owner.server, &envelope, &definition.effective_encryption, true).await?;
        }
        Ok(())
    }

    async fn resolve_index_definition(
        &self,
        input: &dto::CreateIndexInput,
    ) -> Result<VectorIndexDefinitionV1, VectorServiceError> {
        let bucket = self.authorization.bucket.as_ref().ok_or_else(unavailable)?;
        let lifecycle = self
            .owner
            .buckets()
            .acquire_bucket_handle_guard(bucket)
            .await
            .map_err(bucket_error)?;
        let snapshot = self
            .owner
            .buckets()
            .read_managed_bucket_metadata(bucket, &lifecycle)
            .await
            .map_err(bucket_error)?;
        Ok(VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: encryption::requested(
                input.encryption_configuration.as_ref(),
                Some(&encryption::stored(&snapshot)?),
            )?,
            tags: input.tags.clone().unwrap_or_default(),
        })
    }

    async fn managed_dataset(
        &self,
        guard: Arc<VectorCatalogOperationGuard>,
        definition: &VectorIndexDefinitionV1,
    ) -> Result<ManagedDatasetAccess, VectorServiceError> {
        guard.check_held()?;
        let resource = guard.resource_identity();
        let lifecycle = guard.lifecycle_guard()?;
        let envelope = self
            .authorized_envelope(self.request(), resource, definition, &lifecycle)
            .await?;
        let writes = matches!(
            self.request(),
            Request::CreateIndex(_) | Request::PutVectors(_) | Request::DeleteVectors(_)
        );
        encryption::preflight(&self.owner.server, &envelope, &definition.effective_encryption, writes).await?;
        let mut operations = std::collections::BTreeSet::from([
            ManagedObjectOperation::Get,
            ManagedObjectOperation::Head,
            ManagedObjectOperation::Range,
        ]);
        if writes {
            operations.extend([
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ]);
        }
        let access = ManagedObjectAccess::try_from_parts(
            self.bucket().clone(),
            resource.table_id().clone(),
            resource.dataset_prefix(),
            operations,
        )
        .map_err(|_| denied())?;
        let objects = crate::startup_services::guarded_managed_lance_objects(
            access.clone(),
            Arc::new(envelope),
            definition,
            guard,
            self.owner.limits,
        )?;
        self.authorization.reader.bind(&objects);
        Ok(ManagedDatasetAccess { access, objects })
    }
}
