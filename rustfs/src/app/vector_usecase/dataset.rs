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
pub(super) struct CandidateReader(parking_lot::RwLock<Option<std::sync::Weak<crate::startup_services::ManagedLanceObjects>>>);

#[async_trait::async_trait]
impl CatalogManagedCandidateReader for CandidateReader {
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let reader =
            self.0.read().as_ref().and_then(std::sync::Weak::upgrade).ok_or_else(|| {
                TableCatalogStoreError::Unavailable("request has no authorized Managed candidate reader".into())
            })?;
        reader.read_managed_candidate(bucket, metadata, max_size, lifecycle).await
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
        *self.authorization.reader.0.write() = Some(Arc::downgrade(&objects));
        Ok(ManagedDatasetAccess { access, objects })
    }
}
