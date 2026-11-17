// Copyright 2026 RustFS Team
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

use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{Arc, OnceLock},
};

use crate::storage_api::vector::ManagedObjectAccess;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use lance::{Dataset, dataset::builder::DatasetBuilder, session::Session};
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{
        CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter, PredecessorIdentity,
        write_manifest_file_to_path,
    },
};
use object_store_lance::{ObjectStore as ObjectStoreTrait, path::Path};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::table_catalog::{
    CatalogCommitParts, CatalogFormat, CatalogFormatDescriptor, CatalogHead, CatalogMetadataRef, CatalogName, CatalogObjectKey,
    CatalogResourceIdentity, Namespace, ObjectTableCatalogStore, PreparedCatalogCommit, TableCatalogObjectBackend,
    TableCatalogStore, TableCatalogStoreError, VectorCatalogOperationGuard, VectorIndexDefinitionV1,
};

use super::{
    LanceManifestError, ManagedManifestObjects, ManifestIoLimits, VectorIndexManifestSpec,
    dataset::{self, DatasetError, ProjectedVector, QueryCandidate, VectorProjection, VectorRecord},
    manifest::{LanceManifestDescriptorV1, ValidatedLanceManifest, empty_manifest, encode_manifest, manifest_metadata},
    object_store::{ManagedObjectStore, ManagedStoreProvider},
    schema,
};

pub(crate) struct ManifestPrepareRequest {
    pub parts: CatalogCommitParts,
    pub index: VectorIndexManifestSpec,
}

#[derive(Debug, Clone)]
pub(crate) struct CommittedManifestInfo {
    pub reference: CatalogMetadataRef,
    pub manifest_version: u64,
    pub index: VectorIndexManifestSpec,
}

pub(crate) struct LanceManifestAdapter<B: TableCatalogObjectBackend> {
    inner: Arc<AdapterState<B>>,
}

struct AdapterState<B: TableCatalogObjectBackend> {
    catalog: Arc<ObjectTableCatalogStore<B>>,
    objects: Arc<ManagedObjectStore>,
    resource: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    index: VectorIndexManifestSpec,
    limits: ManifestIoLimits,
    session: Arc<Session>,
    uri: String,
    guard: Option<Arc<VectorCatalogOperationGuard>>,
    definition: Option<VectorIndexDefinitionV1>,
}

pub(crate) struct LanceOperationContext<B: TableCatalogObjectBackend> {
    pub catalog: Arc<ObjectTableCatalogStore<B>>,
    pub objects: Arc<dyn ManagedManifestObjects>,
    pub access: ManagedObjectAccess,
    pub guard: Arc<VectorCatalogOperationGuard>,
    pub definition: VectorIndexDefinitionV1,
}

impl<B: TableCatalogObjectBackend> LanceManifestAdapter<B> {
    pub(crate) fn new_guarded(
        context: LanceOperationContext<B>,
        name: CatalogName,
        index: VectorIndexManifestSpec,
        limits: ManifestIoLimits,
    ) -> Result<Self, LanceManifestError> {
        let LanceOperationContext {
            catalog,
            objects,
            access,
            guard,
            definition,
        } = context;
        definition.validate()?;
        if definition.effective_encryption.descriptor_sha256()? != index.encryption_descriptor_sha256 {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "Index encryption differs from the manifest binding",
            });
        }
        guard.check_held()?;
        let mut adapter = Self::build(catalog, objects, access, name, index, limits)?;
        let state = Arc::get_mut(&mut adapter.inner).ok_or(LanceManifestError::ResourceMismatch {
            reason: "new Dataset adapter already has an owner",
        })?;
        if guard.resource_identity() != &state.resource || guard.namespace() != &state.namespace || guard.name() != &state.name {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "Dataset guard identifies another resource",
            });
        }
        state.guard = Some(guard);
        state.definition = Some(definition);
        Ok(adapter)
    }

    #[cfg(test)]
    pub(crate) fn new(
        catalog: Arc<ObjectTableCatalogStore<B>>,
        objects: Arc<dyn ManagedManifestObjects>,
        access: ManagedObjectAccess,
        name: CatalogName,
        index: VectorIndexManifestSpec,
        limits: ManifestIoLimits,
    ) -> Result<Self, LanceManifestError> {
        Self::build(catalog, objects, access, name, index, limits)
    }

    fn build(
        catalog: Arc<ObjectTableCatalogStore<B>>,
        objects: Arc<dyn ManagedManifestObjects>,
        access: ManagedObjectAccess,
        name: CatalogName,
        index: VectorIndexManifestSpec,
        limits: ManifestIoLimits,
    ) -> Result<Self, LanceManifestError> {
        index.validate()?;
        let resource =
            CatalogResourceIdentity::try_new(access.bucket().clone(), access.table_id().clone(), CatalogFormat::Lance)?;
        if access.allowed_prefix() != resource.dataset_prefix() {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "managed scope must identify exactly one Dataset prefix",
            });
        }
        let objects = Arc::new(ManagedObjectStore::new(objects, access, limits));
        let registry = Arc::new(ObjectStoreRegistry::empty());
        registry.insert("rustfs-vector", Arc::new(ManagedStoreProvider { store: objects.clone() }));
        let session = Arc::new(Session::new(0, 0, registry));
        let uri = format!(
            "rustfs-vector://{}/{}",
            resource.storage_bucket_id(),
            resource.dataset_prefix().trim_end_matches('/')
        );
        let namespace = Namespace::parse("indexes").map_err(|source| LanceManifestError::Storage {
            operation: "construct Vector namespace",
            source: Box::new(source),
        })?;
        Ok(Self {
            inner: Arc::new(AdapterState {
                catalog,
                objects,
                resource,
                namespace,
                name,
                index,
                limits,
                session,
                uri,
                guard: None,
                definition: None,
            }),
        })
    }

    pub(crate) fn index_from_catalog(
        entry: &crate::table_catalog::VectorCatalogEntry,
    ) -> Result<VectorIndexManifestSpec, LanceManifestError> {
        LanceManifestDescriptorV1::index_from_catalog(entry.head(), entry.definition())
    }

    pub(crate) async fn prepare_initial_manifest(
        &self,
        access: &ManagedObjectAccess,
        request: ManifestPrepareRequest,
        plan: &super::initial::InitialManifestPlan,
        reserved: &CatalogMetadataRef,
    ) -> Result<PreparedCatalogCommit, LanceManifestError> {
        self.inner.check_access(access)?;
        self.inner.check_request(&request)?;
        plan.verify_reserved_reference(reserved)?;
        if request.parts.expected_head.generation().get() != 0
            || request.parts.expected_head.metadata().is_some()
            || request.parts.expected_head.format_descriptor().is_some()
            || request.parts.operation != "vector-create"
            || request.parts.idempotency_key.is_some()
            || request.parts.writer.is_some()
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "initial publication requires the reserved generation-zero request",
            });
        }
        super::manifest::validate_manifest_path(&self.inner.resource, reserved, 1, Some(request.parts.commit_id.as_str()))?;
        let reference = self
            .inner
            .objects
            .prepare_candidate(reserved.object_key(), plan.bytes.clone())
            .await?;
        if reference.object_key() != reserved.object_key()
            || reference.content_length() != reserved.content_length()
            || reference.content_sha256() != reserved.content_sha256()
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "initial candidate differs from the reserved content",
            });
        }
        let object = self.inner.objects.read_exact(&reference).await?;
        let validated = ValidatedLanceManifest::read_bound(
            Bytes::from(object.data),
            &reference,
            &self.inner.resource,
            &request.index,
            1,
            self.inner.limits.max_manifest_bytes.get(),
        )
        .await?;
        self.inner.validate_dataset_files(&validated, &reference).await?;
        validated.into_prepared(request.parts, &request.index, reference)
    }

    pub(crate) async fn prepare_empty_index(
        &self,
        access: &ManagedObjectAccess,
        request: ManifestPrepareRequest,
    ) -> Result<PreparedCatalogCommit, LanceManifestError> {
        self.inner.check_access(access)?;
        self.inner.check_request(&request)?;
        let version = self.inner.successor_version(&request)?;
        let mut manifest = empty_manifest(
            request.index.dimension,
            version,
            manifest_metadata(&request.parts.resource_identity, &request.index)?,
        )?;
        self.inner
            .prepare_manifest(&request, &mut manifest, None, None, write_manifest_file_to_path)
            .await
    }

    pub(crate) async fn commit_empty_index(
        &self,
        access: &ManagedObjectAccess,
        request: ManifestPrepareRequest,
    ) -> Result<CommittedManifestInfo, LanceManifestError> {
        self.inner.check_access(access)?;
        self.inner.check_request(&request)?;
        let version = self.inner.successor_version(&request)?;
        let mut manifest = empty_manifest(
            request.index.dimension,
            version,
            manifest_metadata(&request.parts.resource_identity, &request.index)?,
        )?;
        let handler = self.handler(Some(request));
        let location = handler
            .commit(
                &mut manifest,
                None,
                &self.inner.base_path(),
                &ObjectStore::memory(),
                write_manifest_file_to_path,
                ManifestNamingScheme::V1,
                None,
            )
            .await
            .map_err(from_commit_error)?;
        self.load_committed_version(
            access,
            &self.inner.resource,
            NonZeroU64::new(location.version).ok_or(LanceManifestError::VersionMismatch {
                expected: version,
                actual: location.version,
            })?,
        )
        .await
    }

    pub(crate) async fn load_current(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
    ) -> Result<CommittedManifestInfo, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let head = self.inner.head().await?;
        let descriptor = self.inner.descriptor(head.format_descriptor())?;
        self.load_selected(head.metadata(), head.format_descriptor(), descriptor.manifest_version())
            .await
    }

    pub(crate) async fn load_committed_version(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
        version: NonZeroU64,
    ) -> Result<CommittedManifestInfo, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let (reference, descriptor) = self.inner.committed_version(version.get()).await?;
        self.load_selected(&reference, &descriptor, version.get()).await
    }

    async fn open_selected(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
        version: u64,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.validate_reference(reference, descriptor).await?;
        let mut handler = self.handler(None);
        handler.selected = Some((reference.clone(), descriptor.clone()));
        let handler = Arc::new(handler);
        let dataset = DatasetBuilder::from_uri(&self.inner.uri)
            .with_session(self.inner.session.clone())
            .with_commit_handler(handler)
            .with_version(version)
            .load()
            .await
            .map_err(|source| LanceManifestError::engine("load committed Dataset", source))?;
        if dataset.version().version != version {
            return Err(LanceManifestError::VersionMismatch {
                expected: version,
                actual: dataset.version().version,
            });
        }
        let location = dataset.manifest_location();
        let expected_identity = self.inner.identity(reference, version)?;
        if location.path.as_ref() != reference.object_key().as_str()
            || location.identity.as_deref() != Some(expected_identity.as_str())
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "loaded Dataset differs from the exact selected Catalog reference",
            });
        }
        schema::validate_schema(dataset.schema(), self.inner.index.dimension)?;
        Ok(DatasetSnapshot {
            dataset: Arc::new(dataset),
            owner: self.inner.clone(),
            info: CommittedManifestInfo {
                reference: reference.clone(),
                manifest_version: version,
                index: self.inner.index.clone(),
            },
        })
    }

    async fn load_selected(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
        version: u64,
    ) -> Result<CommittedManifestInfo, LanceManifestError> {
        Ok(self.open_selected(reference, descriptor, version).await?.info)
    }

    pub(crate) async fn open_catalog_entry(
        &self,
        access: &ManagedObjectAccess,
        entry: &crate::table_catalog::VectorCatalogEntry,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_resource(access, entry.head().resource_identity())?;
        if self.inner.definition.as_ref() != Some(entry.definition()) {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "selected Catalog entry has another Index definition",
            });
        }
        let version = self.inner.descriptor(entry.head().format_descriptor())?.manifest_version();
        self.open_selected(entry.head().metadata(), entry.head().format_descriptor(), version)
            .await
    }

    pub(crate) async fn open_current(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let head = self.inner.head().await?;
        let version = self.inner.descriptor(head.format_descriptor())?.manifest_version();
        self.open_selected(head.metadata(), head.format_descriptor(), version).await
    }

    pub(crate) async fn open_committed_version(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
        version: NonZeroU64,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let (reference, descriptor) = self.inner.committed_version(version.get()).await?;
        self.open_selected(&reference, &descriptor, version.get()).await
    }

    async fn write_base(
        &self,
        access: &ManagedObjectAccess,
        request: &ManifestPrepareRequest,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_access(access)?;
        self.inner.check_request(request)?;
        if self.inner.head().await?.expected_head() != request.parts.expected_head {
            return Err(LanceManifestError::ExpectedHeadConflict);
        }
        let reference = request
            .parts
            .expected_head
            .metadata()
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "Dataset writes require an existing Catalog manifest",
            })?;
        let descriptor = request
            .parts
            .expected_head
            .format_descriptor()
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "Dataset writes require an existing Catalog descriptor",
            })?;
        let version = self.inner.descriptor(descriptor)?.manifest_version();
        self.open_selected(reference, descriptor, version).await
    }

    pub(crate) async fn put_vectors(
        &self,
        access: &ManagedObjectAccess,
        request: ManifestPrepareRequest,
        records: &[VectorRecord],
    ) -> Result<DatasetSnapshot<B>, DatasetError> {
        self.inner.check_access(access)?;
        let batch = dataset::record_batch(records, &self.inner.index)?;
        let snapshot = self.write_base(access, &request).await?;
        let handler = Arc::new(self.handler(Some(request)));
        let dataset = dataset::put(snapshot.dataset.clone(), batch, handler.clone()).await?;
        self.committed_snapshot(dataset, &handler)
    }

    pub(crate) async fn delete_vectors(
        &self,
        access: &ManagedObjectAccess,
        request: ManifestPrepareRequest,
        keys: &[String],
    ) -> Result<DatasetSnapshot<B>, DatasetError> {
        let snapshot = self.write_base(access, &request).await?;
        let handler = Arc::new(self.handler(Some(request)));
        let dataset = dataset::delete(snapshot.dataset.clone(), keys, handler.clone()).await?;
        self.committed_snapshot(dataset, &handler)
    }

    fn committed_snapshot(
        &self,
        dataset: Dataset,
        handler: &CatalogCommitHandler<B>,
    ) -> Result<DatasetSnapshot<B>, DatasetError> {
        let info = handler
            .published
            .get()
            .cloned()
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "Dataset writer returned without a successful Catalog publication",
            })?;
        if dataset.version().version != info.manifest_version {
            return Err(LanceManifestError::VersionMismatch {
                expected: info.manifest_version,
                actual: dataset.version().version,
            }
            .into());
        }
        Ok(DatasetSnapshot {
            dataset: Arc::new(dataset),
            owner: self.inner.clone(),
            info,
        })
    }

    pub(super) fn handler(&self, attempt: Option<ManifestPrepareRequest>) -> CatalogCommitHandler<B> {
        CatalogCommitHandler {
            adapter: self.inner.clone(),
            attempt,
            published: Arc::new(OnceLock::new()),
            selected: None,
        }
    }
}

pub(crate) struct DatasetSnapshot<B: TableCatalogObjectBackend> {
    dataset: Arc<Dataset>,
    owner: Arc<AdapterState<B>>,
    info: CommittedManifestInfo,
}

impl<B: TableCatalogObjectBackend> DatasetSnapshot<B> {
    pub(crate) fn info(&self) -> &CommittedManifestInfo {
        &self.info
    }

    pub(crate) async fn get(&self, keys: &[String], projection: VectorProjection) -> Result<Vec<ProjectedVector>, DatasetError> {
        self.owner.check_guard()?;
        dataset::get(&self.dataset, keys, projection).await
    }

    pub(crate) async fn list_batch(
        &self,
        offset: u64,
        max_rows: std::num::NonZeroUsize,
        projection: VectorProjection,
    ) -> Result<Vec<ProjectedVector>, DatasetError> {
        self.owner.check_guard()?;
        dataset::list_batch(&self.dataset, offset, max_rows, projection).await
    }

    pub(crate) async fn query_matches(
        &self,
        query: &[f32],
        top_k: std::num::NonZeroUsize,
        filter: Option<crate::vector::filter::ValidatedFilter>,
    ) -> Result<Vec<super::dataset::QueryMatch>, DatasetError> {
        self.owner.check_guard()?;
        dataset::query_matches(&self.dataset, query, &self.info.index, top_k, filter).await
    }

    pub(crate) async fn query_candidates(
        &self,
        query: &[f32],
        top_k: std::num::NonZeroUsize,
        filter: Option<crate::vector::filter::ValidatedFilter>,
    ) -> Result<Vec<QueryCandidate>, DatasetError> {
        self.owner.check_guard()?;
        dataset::query_candidates(&self.dataset, query, &self.info.index, top_k, filter).await
    }

    pub(crate) async fn take_rows(
        &self,
        row_ids: &[u64],
        projection: VectorProjection,
    ) -> Result<Vec<ProjectedVector>, DatasetError> {
        self.owner.check_guard()?;
        dataset::take_rows(&self.dataset, row_ids, projection).await
    }
}

impl<B: TableCatalogObjectBackend> AdapterState<B> {
    fn check_guard(&self) -> Result<(), LanceManifestError> {
        if let Some(guard) = &self.guard {
            return guard.check_held().map_err(Into::into);
        }
        #[cfg(test)]
        {
            Ok(())
        }
        #[cfg(not(test))]
        {
            Err(LanceManifestError::ResourceMismatch {
                reason: "Dataset operation has no Catalog guard",
            })
        }
    }

    fn base_path(&self) -> Path {
        Path::from(self.resource.dataset_prefix().trim_end_matches('/'))
    }

    fn check_access(&self, access: &ManagedObjectAccess) -> Result<(), LanceManifestError> {
        self.check_guard()?;
        if access != self.objects.access() {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "managed access differs from the bound adapter",
            });
        }
        Ok(())
    }

    fn check_resource(&self, access: &ManagedObjectAccess, resource: &CatalogResourceIdentity) -> Result<(), LanceManifestError> {
        self.check_access(access)?;
        if resource != &self.resource {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "resource differs from the bound adapter",
            });
        }
        Ok(())
    }

    fn check_base(&self, base: &Path) -> Result<(), LanceManifestError> {
        if base != &self.base_path() {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "engine Dataset path differs from the bound adapter",
            });
        }
        Ok(())
    }

    fn check_request(&self, request: &ManifestPrepareRequest) -> Result<(), LanceManifestError> {
        if request.parts.resource_identity != self.resource
            || request.index != self.index
            || request.parts.namespace != self.namespace
            || request.parts.name != self.name
        {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "preparation differs from the bound resource, name or Index configuration",
            });
        }
        let commit_id =
            uuid::Uuid::parse_str(request.parts.commit_id.as_str()).map_err(|_| LanceManifestError::ReferenceMismatch {
                reason: "Lance commit ID must be a canonical non-nil UUID",
            })?;
        if commit_id.is_nil() || commit_id.to_string() != request.parts.commit_id.as_str() {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "Lance commit ID must be a canonical non-nil UUID",
            });
        }
        Ok(())
    }

    fn descriptor(&self, descriptor: &CatalogFormatDescriptor) -> Result<LanceManifestDescriptorV1, LanceManifestError> {
        LanceManifestDescriptorV1::decode(descriptor, &self.resource, &self.index)
    }

    fn successor_version(&self, request: &ManifestPrepareRequest) -> Result<u64, LanceManifestError> {
        match (request.parts.expected_head.metadata(), request.parts.expected_head.format_descriptor()) {
            (None, None) => Ok(1),
            (Some(metadata), Some(descriptor)) if metadata == descriptor.metadata() => {
                let base = self.descriptor(descriptor)?.manifest_version();
                base.checked_add(1)
                    .filter(|version| !lance_table::format::is_detached_version(*version))
                    .ok_or(LanceManifestError::UnsupportedFormat {
                        reason: "Lance main-branch version is exhausted",
                    })
            }
            _ => Err(LanceManifestError::ReferenceMismatch {
                reason: "expected head metadata and descriptor are inconsistent",
            }),
        }
    }

    async fn head(&self) -> Result<CatalogHead, LanceManifestError> {
        let bucket = self.resource.vector_bucket().ok_or(LanceManifestError::ResourceMismatch {
            reason: "adapter requires a Vector bucket",
        })?;
        self.check_guard()?;
        let head = if let Some(guard) = &self.guard {
            self.catalog.read_catalog_head_with_guard(guard).await?
        } else {
            self.catalog.read_catalog_head(bucket, &self.namespace, &self.name).await?
        };
        if head.resource_identity() != &self.resource {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "Catalog returned a different resource lifetime",
            });
        }
        Ok(head)
    }

    async fn committed_versions(&self) -> Result<Vec<(CatalogMetadataRef, CatalogFormatDescriptor)>, LanceManifestError> {
        let head = self.head().await?;
        let history = if let Some(guard) = &self.guard {
            self.catalog.read_committed_history_with_guard(guard, &head).await?
        } else {
            self.catalog.read_committed_history(&head).await?
        };
        let mut versions = BTreeMap::new();
        for entry in history.versions() {
            let version = self.descriptor(entry.format_descriptor())?.manifest_version();
            let pair = (entry.metadata().clone(), entry.format_descriptor().clone());
            if versions
                .insert(version, pair.clone())
                .is_some_and(|previous| previous != pair)
            {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "Catalog history contains competing references for one Lance version",
                });
            }
        }
        Ok(versions.into_values().collect())
    }

    async fn committed_version(
        &self,
        requested: u64,
    ) -> Result<(CatalogMetadataRef, CatalogFormatDescriptor), LanceManifestError> {
        for (reference, descriptor) in self.committed_versions().await? {
            if self.descriptor(&descriptor)?.manifest_version() == requested {
                return Ok((reference, descriptor));
            }
        }
        Err(LanceManifestError::VersionNotCommitted { version: requested })
    }

    async fn validate_reference(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> Result<ValidatedLanceManifest, LanceManifestError> {
        let decoded = self.descriptor(descriptor)?;
        if descriptor.metadata() != reference {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "Catalog descriptor and selected reference differ",
            });
        }
        let object = self.objects.read_exact(reference).await?;
        let validated = ValidatedLanceManifest::read_bound(
            Bytes::from(object.data),
            reference,
            &self.resource,
            &self.index,
            decoded.manifest_version(),
            self.limits.max_manifest_bytes.get(),
        )
        .await?;
        decoded.verify_manifest(&validated)?;
        self.validate_dataset_files(&validated, reference).await?;
        Ok(validated)
    }

    async fn validate_dataset_files(
        &self,
        validated: &ValidatedLanceManifest,
        reference: &CatalogMetadataRef,
    ) -> Result<(), LanceManifestError> {
        self.check_guard()?;
        let mut file_checks = stream::iter(super::files::manifest_objects(validated.manifest(), &self.resource)?)
            .map(|object| async move { self.objects.bind_data_reference(&object).await })
            .buffer_unordered(self.limits.max_concurrent_reads.get());
        while let Some(result) = file_checks.next().await {
            result?;
        }
        if let Some(name) = &validated.manifest().transaction_file {
            let key = CatalogObjectKey::try_new(format!("{}_transactions/{name}", self.resource.dataset_prefix()))?;
            let bytes = self
                .objects
                .read_data_bytes(&key, self.limits.max_manifest_bytes.get())
                .await?;
            let transaction =
                lance_table::format::pb::Transaction::decode(bytes).map_err(|source| LanceManifestError::Protobuf { source })?;
            super::files::validate_transaction(&transaction, validated.manifest())?;
            if validated.inline_transaction().is_some_and(|inline| inline != &transaction) {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "inline and external transactions differ",
                });
            }
        }
        let manifest_bytes = lance_table::format::pb::Manifest::from(validated.manifest()).encode_to_vec();
        let validation_handler = CandidateValidationHandler {
            base: self.base_path(),
            location: ManifestLocation {
                version: validated.manifest().version,
                path: Path::from(reference.object_key().as_str()),
                size: Some(reference.content_length()),
                naming_scheme: ManifestNamingScheme::V1,
                e_tag: reference.etag().map(str::to_string),
                identity: None,
            },
        };
        // This private reader validates one exact candidate before Catalog publication.
        let dataset = DatasetBuilder::from_uri(&self.uri)
            .with_session(self.session.clone())
            .with_commit_handler(Arc::new(validation_handler))
            .with_serialized_manifest(&manifest_bytes)
            .map_err(|source| LanceManifestError::engine("bind candidate manifest", source))?
            .load()
            .await
            .map_err(|source| LanceManifestError::engine("open candidate validation reader", source))?;
        dataset
            .validate()
            .await
            .map_err(|source| LanceManifestError::engine("validate Dataset files", source))?;
        self.check_guard()
    }

    async fn prepare_manifest(
        &self,
        request: &ManifestPrepareRequest,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        transaction: Option<Transaction>,
        writer: ManifestWriter,
    ) -> Result<PreparedCatalogCommit, LanceManifestError> {
        self.check_request(request)?;
        let expected_version = self.successor_version(request)?;
        if manifest.version != expected_version {
            return Err(LanceManifestError::VersionMismatch {
                expected: expected_version,
                actual: manifest.version,
            });
        }
        let bytes = encode_manifest(manifest, indices, transaction, writer).await?;
        let key = CatalogObjectKey::try_new(format!(
            "{}_versions/{}/{}.manifest",
            self.resource.dataset_prefix(),
            request.parts.commit_id.as_str(),
            expected_version
        ))?;
        let reference = self.objects.prepare_candidate(&key, bytes.clone()).await?;
        let object = self.objects.read_exact(&reference).await?;
        if object.data.as_slice() != bytes.as_ref() {
            return Err(LanceManifestError::DigestMismatch);
        }
        let validated = ValidatedLanceManifest::read_bound(
            Bytes::from(object.data),
            &reference,
            &self.resource,
            &request.index,
            expected_version,
            self.limits.max_manifest_bytes.get(),
        )
        .await?;
        self.validate_dataset_files(&validated, &reference).await?;
        validated.into_prepared(clone_parts(&request.parts), &request.index, reference)
    }

    fn location(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> Result<ManifestLocation, LanceManifestError> {
        let version = self.descriptor(descriptor)?.manifest_version();
        self.objects.bind_reference(reference)?;
        Ok(ManifestLocation {
            version,
            path: Path::from(reference.object_key().as_str()),
            size: Some(reference.content_length()),
            naming_scheme: ManifestNamingScheme::V1,
            e_tag: reference.etag().map(str::to_string),
            identity: Some(self.identity(reference, version)?),
        })
    }

    fn identity(&self, reference: &CatalogMetadataRef, version: u64) -> Result<String, LanceManifestError> {
        let encoded = serde_json::to_vec(&(
            self.resource.incarnation(),
            self.resource.table_id().as_str(),
            version,
            reference.object_key().as_str(),
            reference.content_sha256().to_hex(),
            reference.body_revision().map(|revision| revision.as_uuid()),
        ))?;
        Ok(format!(
            "rustfs-vector:1:{}",
            hex_simd::encode_to_string(Sha256::digest(encoded), hex_simd::AsciiCase::Lower)
        ))
    }
}

#[derive(Debug)]
struct CandidateValidationHandler {
    base: Path,
    location: ManifestLocation,
}

#[async_trait]
impl CommitHandler for CandidateValidationHandler {
    async fn resolve_latest_location(&self, _base: &Path, _store: &ObjectStore) -> lance_core::Result<ManifestLocation> {
        Err(lance_core::Error::not_supported("candidate validation requires an exact version"))
    }

    async fn resolve_version_location(
        &self,
        base: &Path,
        version: u64,
        _store: &dyn ObjectStoreTrait,
    ) -> lance_core::Result<ManifestLocation> {
        if base != &self.base || version != self.location.version {
            return Err(lance_core::Error::invalid_input("candidate validation reference differs"));
        }
        Ok(self.location.clone())
    }

    async fn commit(
        &self,
        _manifest: &mut Manifest,
        _indices: Option<Vec<IndexMetadata>>,
        _base: &Path,
        _store: &ObjectStore,
        _writer: ManifestWriter,
        _scheme: ManifestNamingScheme,
        _transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        Err(CommitError::OtherError(lance_core::Error::not_supported(
            "candidate validation cannot publish",
        )))
    }
}

pub(super) struct CatalogCommitHandler<B: TableCatalogObjectBackend> {
    adapter: Arc<AdapterState<B>>,
    attempt: Option<ManifestPrepareRequest>,
    published: Arc<OnceLock<CommittedManifestInfo>>,
    selected: Option<(CatalogMetadataRef, CatalogFormatDescriptor)>,
}

impl<B: TableCatalogObjectBackend> fmt::Debug for CatalogCommitHandler<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogCommitHandler")
            .field("resource", &self.adapter.resource)
            .finish_non_exhaustive()
    }
}

impl<B: TableCatalogObjectBackend> CatalogCommitHandler<B> {
    async fn locations(
        &self,
        base: &Path,
        descending: bool,
        since: Option<u64>,
    ) -> Result<Vec<ManifestLocation>, LanceManifestError> {
        self.adapter.check_base(base)?;
        let mut locations = Vec::new();
        for (reference, descriptor) in self.adapter.committed_versions().await? {
            let location = self.adapter.location(&reference, &descriptor)?;
            if since.is_none_or(|version| location.version > version) {
                locations.push(location);
            }
        }
        if descending {
            locations.sort_by_key(|location| std::cmp::Reverse(location.version));
        }
        Ok(locations)
    }

    async fn publish(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base: &Path,
        writer: ManifestWriter,
        transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        self.adapter.check_base(base).map_err(to_commit_error)?;
        let request = self.attempt.as_ref().ok_or_else(|| {
            CommitError::OtherError(lance_core::Error::not_supported("read-only handler has no bound commit attempt"))
        })?;
        let prepared = self
            .adapter
            .prepare_manifest(request, manifest, indices, transaction, writer)
            .await
            .map_err(to_commit_error)?;
        let reference = prepared.candidate_metadata().clone();
        let descriptor = prepared.format_descriptor().clone();
        self.adapter.check_guard().map_err(to_commit_error)?;
        if let Some(guard) = &self.adapter.guard {
            let definition = self.adapter.definition.as_ref().ok_or_else(|| {
                to_commit_error(LanceManifestError::ResourceMismatch {
                    reason: "guarded Dataset adapter has no Index definition",
                })
            })?;
            self.adapter
                .catalog
                .commit_vector_prepared_with_guard(guard, prepared, definition)
                .await
                .map_err(|source| to_commit_error(source.into()))?;
        } else {
            self.adapter
                .catalog
                .commit_prepared(prepared)
                .await
                .map_err(|source| to_commit_error(source.into()))?;
        }
        let location = self.adapter.location(&reference, &descriptor).map_err(to_commit_error)?;
        let info = CommittedManifestInfo {
            reference,
            manifest_version: location.version,
            index: self.adapter.index.clone(),
        };
        if let Err(repeated) = self.published.set(info)
            && self.published.get().is_none_or(|previous| {
                previous.reference != repeated.reference || previous.manifest_version != repeated.manifest_version
            })
        {
            return Err(to_commit_error(LanceManifestError::ReferenceMismatch {
                reason: "one commit attempt published competing results",
            }));
        }
        Ok(location)
    }
}

#[async_trait]
impl<B: TableCatalogObjectBackend> CommitHandler for CatalogCommitHandler<B> {
    async fn resolve_latest_location(&self, base: &Path, _store: &ObjectStore) -> lance_core::Result<ManifestLocation> {
        self.adapter.check_base(base).map_err(to_engine_error)?;
        let head = self.adapter.head().await.map_err(to_engine_error)?;
        self.adapter
            .validate_reference(head.metadata(), head.format_descriptor())
            .await
            .map_err(to_engine_error)?;
        self.adapter
            .location(head.metadata(), head.format_descriptor())
            .map_err(to_engine_error)
    }

    async fn resolve_version_location(
        &self,
        base: &Path,
        version: u64,
        _store: &dyn ObjectStoreTrait,
    ) -> lance_core::Result<ManifestLocation> {
        self.adapter.check_base(base).map_err(to_engine_error)?;
        self.adapter.check_guard().map_err(to_engine_error)?;
        if let Some((reference, descriptor)) = &self.selected
            && self
                .adapter
                .descriptor(descriptor)
                .map_err(to_engine_error)?
                .manifest_version()
                == version
        {
            return self.adapter.location(reference, descriptor).map_err(to_engine_error);
        }
        let (reference, descriptor) = self.adapter.committed_version(version).await.map_err(to_engine_error)?;
        self.adapter
            .validate_reference(&reference, &descriptor)
            .await
            .map_err(to_engine_error)?;
        self.adapter.location(&reference, &descriptor).map_err(to_engine_error)
    }

    async fn version_exists(
        &self,
        base: &Path,
        version: u64,
        store: &dyn ObjectStoreTrait,
        _scheme: ManifestNamingScheme,
    ) -> lance_core::Result<bool> {
        self.adapter.check_base(base).map_err(to_engine_error)?;
        match self.adapter.committed_version(version).await {
            Ok((reference, descriptor)) => {
                self.adapter
                    .validate_reference(&reference, &descriptor)
                    .await
                    .map_err(to_engine_error)?;
                let _ = store;
                Ok(true)
            }
            Err(LanceManifestError::VersionNotCommitted { .. }) => Ok(false),
            Err(error) => Err(to_engine_error(error)),
        }
    }

    fn list_detached_manifest_locations<'a>(
        &self,
        base: &Path,
        _store: &'a ObjectStore,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        match self.adapter.check_base(base) {
            Ok(()) => stream::empty().boxed(),
            Err(error) => stream::once(async { Err(to_engine_error(error)) }).boxed(),
        }
    }

    fn list_manifest_locations<'a>(
        &self,
        base: &Path,
        _store: &'a ObjectStore,
        descending: bool,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        let handler = Self {
            adapter: self.adapter.clone(),
            attempt: None,
            published: Arc::new(OnceLock::new()),
            selected: None,
        };
        let base = base.clone();
        stream::once(async move { handler.locations(&base, descending, None).await.map_err(to_engine_error) })
            .flat_map(|result| match result {
                Ok(locations) => stream::iter(locations.into_iter().map(Ok)).boxed(),
                Err(error) => stream::once(async { Err(error) }).boxed(),
            })
            .boxed()
    }

    fn list_manifest_locations_since<'a>(
        &self,
        base: &Path,
        _store: &'a ObjectStore,
        since: u64,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        let handler = Self {
            adapter: self.adapter.clone(),
            attempt: None,
            published: Arc::new(OnceLock::new()),
            selected: None,
        };
        let base = base.clone();
        stream::once(async move { handler.locations(&base, true, Some(since)).await.map_err(to_engine_error) })
            .flat_map(|result| match result {
                Ok(locations) => stream::iter(locations.into_iter().map(Ok)).boxed(),
                Err(error) => stream::once(async { Err(error) }).boxed(),
            })
            .boxed()
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base: &Path,
        _store: &ObjectStore,
        writer: ManifestWriter,
        _scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> Result<ManifestLocation, CommitError> {
        self.publish(manifest, indices, base, writer, transaction).await
    }

    fn supports_predecessor_condition(&self) -> bool {
        true
    }

    async fn resolve_latest_identity(&self, base: &Path, store: &ObjectStore) -> lance_core::Result<Option<PredecessorIdentity>> {
        let location = self.resolve_latest_location(base, store).await?;
        Ok(location.identity.map(|identity| PredecessorIdentity {
            version: location.version,
            identity,
        }))
    }

    async fn resolve_identity(
        &self,
        base: &Path,
        store: &ObjectStore,
        version: u64,
    ) -> lance_core::Result<Option<PredecessorIdentity>> {
        self.adapter.check_base(base).map_err(to_engine_error)?;
        match self.adapter.committed_version(version).await {
            Ok((reference, descriptor)) => {
                self.adapter
                    .validate_reference(&reference, &descriptor)
                    .await
                    .map_err(to_engine_error)?;
                let _ = store;
                Ok(Some(PredecessorIdentity {
                    version,
                    identity: self.adapter.identity(&reference, version).map_err(to_engine_error)?,
                }))
            }
            Err(LanceManifestError::VersionNotCommitted { .. }) => Ok(None),
            Err(error) => Err(to_engine_error(error)),
        }
    }

    async fn commit_after(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base: &Path,
        _store: &ObjectStore,
        writer: ManifestWriter,
        _scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
        predecessor: &PredecessorIdentity,
    ) -> Result<ManifestLocation, CommitError> {
        let request = self
            .attempt
            .as_ref()
            .ok_or_else(|| CommitError::OtherError(lance_core::Error::prerequisite_failed("handler has no bound predecessor")))?;
        let expected = &request.parts.expected_head;
        let (Some(reference), Some(descriptor)) = (expected.metadata(), expected.format_descriptor()) else {
            return Err(CommitError::OtherError(lance_core::Error::prerequisite_failed(
                "initial creation has no predecessor",
            )));
        };
        let version = self
            .adapter
            .descriptor(descriptor)
            .map_err(to_commit_error)?
            .manifest_version();
        let identity = self.adapter.identity(reference, version).map_err(to_commit_error)?;
        if predecessor.version != version || predecessor.identity != identity {
            return Err(CommitError::OtherError(lance_core::Error::prerequisite_failed(
                "predecessor differs from the bound computation base",
            )));
        }
        let head = self.adapter.head().await.map_err(to_commit_error)?;
        if head.expected_head() != *expected {
            return Err(to_commit_error(LanceManifestError::ExpectedHeadConflict));
        }
        self.publish(manifest, indices, base, writer, transaction).await
    }

    async fn forget_version(&self, _base: &Path, _version: u64, _identity: &str) -> lance_core::Result<()> {
        Err(lance_core::Error::not_supported("Catalog retention owns version removal"))
    }
    async fn delete(&self, _base: &Path) -> lance_core::Result<()> {
        Err(lance_core::Error::not_supported("Catalog lifecycle owns Dataset removal"))
    }
}

fn clone_parts(parts: &CatalogCommitParts) -> CatalogCommitParts {
    CatalogCommitParts {
        resource_identity: parts.resource_identity.clone(),
        namespace: parts.namespace.clone(),
        name: parts.name.clone(),
        expected_head: parts.expected_head.clone(),
        commit_id: parts.commit_id.clone(),
        operation: parts.operation.clone(),
        idempotency_key: parts.idempotency_key.clone(),
        writer: parts.writer.clone(),
    }
}

fn to_engine_error(error: LanceManifestError) -> lance_core::Error {
    match error {
        LanceManifestError::VersionNotCommitted { version } => {
            lance_core::Error::not_found(format!("Catalog Lance version {version}"))
        }
        error => lance_core::Error::wrapped(Box::new(error)),
    }
}

pub(super) fn to_commit_error(error: LanceManifestError) -> CommitError {
    match error {
        LanceManifestError::Catalog(TableCatalogStoreError::Conflict(_)) | LanceManifestError::ExpectedHeadConflict => {
            CommitError::CommitConflict
        }
        error => CommitError::OtherError(to_engine_error(error)),
    }
}

fn from_commit_error(error: CommitError) -> LanceManifestError {
    match error {
        CommitError::CommitConflict => LanceManifestError::ExpectedHeadConflict,
        CommitError::OtherError(source) => LanceManifestError::engine("commit manifest", source),
    }
}
