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
    collections::{BTreeMap, VecDeque},
    fmt,
    num::NonZeroU64,
    sync::{Arc, Mutex, OnceLock},
};

use crate::storage_api::vector::ManagedObjectAccess;
use async_trait::async_trait;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use lance::{Dataset, dataset::builder::DatasetBuilder, session::Session};
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
#[cfg(test)]
use lance_table::io::commit::write_manifest_file_to_path;
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter, PredecessorIdentity},
};
use object_store_lance::{ObjectStore as ObjectStoreTrait, path::Path};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::table_catalog::{
    CatalogCommitParts, CatalogEntryUpdates, CatalogExpectedHead, CatalogFormat, CatalogFormatDescriptor, CatalogHead,
    CatalogMetadataRef, CatalogName, CatalogObjectKey, CatalogResourceIdentity, Namespace, ObjectTableCatalogStore,
    PreparedCatalogCommit, TableCatalogObjectBackend, TableCatalogStore, TableCatalogStoreError, VectorCatalogOperationGuard,
    VectorIndexDefinitionV1,
};

use super::{
    LanceManifestError, ManagedManifestObjects, ManifestIoLimits, VectorIndexManifestSpec,
    dataset::{self, DatasetError, ProjectedVector, VectorProjection, VectorRecord},
    manifest::{LanceManifestDescriptorV1, ValidatedLanceManifest, encode_manifest},
    object_store::{ManagedObjectStore, ManagedStoreProvider},
    schema,
};
#[cfg(test)]
use super::{
    dataset::QueryCandidate,
    manifest::{empty_manifest, manifest_metadata},
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
    numeric_proofs: Arc<LanceNumericProofCache>,
    maintenance_publication: Option<Arc<dyn MaintenancePublicationAuthorizer>>,
    #[cfg(test)]
    ann_completions: std::sync::atomic::AtomicUsize,
}

/// Application-owned admission for each maintenance Catalog publication.
#[async_trait]
pub(crate) trait MaintenancePublicationAuthorizer: Send + Sync {
    /// Recheck the bound resource and current write authority while retaining the original owners.
    async fn authorize(
        &self,
        resource: &CatalogResourceIdentity,
        candidate: &CatalogMetadataRef,
    ) -> Result<(), LanceManifestError>;
}

pub(crate) struct LanceOperationContext<B: TableCatalogObjectBackend> {
    pub catalog: Arc<ObjectTableCatalogStore<B>>,
    pub objects: Arc<dyn ManagedManifestObjects>,
    pub access: ManagedObjectAccess,
    pub guard: Arc<VectorCatalogOperationGuard>,
    pub definition: VectorIndexDefinitionV1,
}

#[derive(Default)]
pub(crate) struct LanceNumericProofCache {
    entries: Mutex<VecDeque<([u8; 32], usize)>>,
}

impl<B: TableCatalogObjectBackend> LanceManifestAdapter<B> {
    pub(crate) fn with_maintenance_control(
        self,
        control: crate::table_catalog::VectorMaintenanceControl,
    ) -> Result<Self, LanceManifestError> {
        self.inner.objects.set_maintenance_control(control)?;
        Ok(self)
    }

    pub(crate) fn with_maintenance_publication_authorizer(
        mut self,
        authorizer: Arc<dyn MaintenancePublicationAuthorizer>,
    ) -> Result<Self, LanceManifestError> {
        let state = Arc::get_mut(&mut self.inner).ok_or(LanceManifestError::ResourceMismatch {
            reason: "publication admission must be bound before sharing the adapter",
        })?;
        if state.maintenance_publication.is_some() {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "maintenance publication admission is already bound",
            });
        }
        state.maintenance_publication = Some(authorizer);
        Ok(self)
    }

    pub(crate) fn with_numeric_proof_cache(mut self, cache: Arc<LanceNumericProofCache>) -> Result<Self, LanceManifestError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(LanceManifestError::ResourceMismatch {
            reason: "numeric cache must be bound before sharing the adapter",
        })?;
        inner.numeric_proofs = cache;
        Ok(self)
    }

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
                numeric_proofs: Arc::new(LanceNumericProofCache::default()),
                maintenance_publication: None,
                #[cfg(test)]
                ann_completions: std::sync::atomic::AtomicUsize::new(0),
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
        let mut validated = ValidatedLanceManifest::read_bound(
            object.into_bytes(),
            &reference,
            &self.inner.resource,
            &request.index,
            1,
            self.inner.limits.max_manifest_bytes.get(),
        )
        .await?;
        self.inner.validate_dataset_files(&mut validated, &reference).await?;
        validated.into_prepared(request.parts, &request.index, reference)
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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
        computation_base: Option<CatalogExpectedHead>,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.validate_reference(reference, descriptor).await?;
        let mut handler = self.handler(None);
        handler.selected = Some((reference.clone(), descriptor.clone()));
        handler.computation_base = computation_base;
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

    #[cfg(test)]
    async fn load_selected(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
        version: u64,
    ) -> Result<CommittedManifestInfo, LanceManifestError> {
        Ok(self.open_selected(reference, descriptor, version, None).await?.info)
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
        self.open_selected(entry.head().metadata(), entry.head().format_descriptor(), version, None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn open_current(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let head = self.inner.head().await?;
        let version = self.inner.descriptor(head.format_descriptor())?.manifest_version();
        self.open_selected(head.metadata(), head.format_descriptor(), version, None)
            .await
    }

    pub(crate) async fn open_committed_version(
        &self,
        access: &ManagedObjectAccess,
        resource: &CatalogResourceIdentity,
        version: NonZeroU64,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.inner.check_resource(access, resource)?;
        let (reference, descriptor) = self.inner.committed_version(version.get()).await?;
        self.open_selected(&reference, &descriptor, version.get(), None).await
    }

    pub(super) async fn write_base(
        &self,
        access: &ManagedObjectAccess,
        request: &ManifestPrepareRequest,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.open_write_base(access, request, Some(request.parts.expected_head.clone()))
            .await
    }

    pub(super) async fn maintenance_base(
        &self,
        access: &ManagedObjectAccess,
        request: &ManifestPrepareRequest,
    ) -> Result<DatasetSnapshot<B>, LanceManifestError> {
        self.open_write_base(access, request, None).await
    }

    async fn open_write_base(
        &self,
        access: &ManagedObjectAccess,
        request: &ManifestPrepareRequest,
        computation_base: Option<CatalogExpectedHead>,
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
        self.open_selected(reference, descriptor, version, computation_base).await
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
        #[cfg(test)]
        if let Some(probe) = crate::vector::fault_test::hit(
            self.inner.objects.fault_instance_id(),
            &self.inner.resource,
            crate::vector::fault_test::FaultPoint::PutBaseOpened,
        ) {
            let keys = records.iter().map(|record| record.key.clone()).collect::<Vec<_>>();
            let present = match snapshot
                .get(
                    &keys,
                    VectorProjection {
                        data: false,
                        metadata: false,
                    },
                )
                .await
            {
                Ok(present) => present,
                Err(error) => {
                    probe.observation_failed();
                    return Err(error);
                }
            };
            let mut observation =
                crate::vector::fault_test::FaultObservation::new(&request.parts.expected_head, snapshot.info.manifest_version);
            observation.selected_keys = keys;
            observation.present_keys = present.into_iter().map(|row| row.key).collect();
            probe.pause(observation).await;
        }
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

    #[cfg(test)]
    pub(crate) async fn pause_query_snapshot(
        &self,
        snapshot: &DatasetSnapshot<B>,
        head: &crate::table_catalog::CatalogExpectedHead,
    ) {
        if let Some(probe) = crate::vector::fault_test::hit(
            self.inner.objects.fault_instance_id(),
            &self.inner.resource,
            crate::vector::fault_test::FaultPoint::QuerySnapshotOpened,
        ) {
            let observation = crate::vector::fault_test::FaultObservation::new(head, snapshot.info.manifest_version);
            probe.pause(observation).await;
        }
    }

    #[cfg(test)]
    fn maintenance_fault_context(
        &self,
        request: &ManifestPrepareRequest,
        manifest_version: u64,
        job: &crate::table_catalog::VectorMaintenanceJobV1,
        stage: &'static str,
    ) -> crate::vector::fault_test::FaultContext {
        let mut observation = crate::vector::fault_test::FaultObservation::new(&request.parts.expected_head, manifest_version);
        observation.maintenance = Some(crate::vector::fault_test::MaintenanceObservation {
            job_id: *job.job_id(),
            attempt: *job.attempt(),
            stage,
            stage_commit_id: request.parts.commit_id.as_str().to_owned(),
        });
        crate::vector::fault_test::FaultContext {
            instance_id: self.inner.objects.fault_instance_id(),
            resource: self.inner.resource.clone(),
            observation,
        }
    }

    pub(super) fn committed_snapshot(
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

    pub(super) async fn open_for_compaction(
        &self,
        version: u64,
        handler: Arc<CompactionCommitHandler<B>>,
    ) -> Result<Dataset, DatasetError> {
        Ok(DatasetBuilder::from_uri(&self.inner.uri)
            .with_session(self.inner.session.clone())
            .with_commit_handler(handler)
            .with_version(version)
            .load()
            .await?)
    }

    #[cfg(test)]
    pub(super) fn native_delete_attempts(&self) -> usize {
        self.inner.objects.delete_attempts()
    }

    pub(super) fn compaction_handler(
        &self,
        request: ManifestPrepareRequest,
        rewrite_id: crate::table_catalog::CatalogCommitId,
        expected_fragments: u32,
        control: crate::table_catalog::VectorMaintenanceControl,
    ) -> Arc<CompactionCommitHandler<B>> {
        Arc::new(CompactionCommitHandler {
            reserve: self.handler(Some(request)),
            rewrite: OnceLock::new(),
            rewrite_id,
            expected_fragments,
            control,
            #[cfg(test)]
            after_reserve: OnceLock::new(),
        })
    }

    pub(super) fn handler(&self, attempt: Option<ManifestPrepareRequest>) -> CatalogCommitHandler<B> {
        CatalogCommitHandler {
            adapter: self.inner.clone(),
            attempt,
            published: Arc::new(OnceLock::new()),
            published_head: Arc::new(OnceLock::new()),
            published_record: Arc::new(OnceLock::new()),
            selected: None,
            computation_base: None,
        }
    }
}

pub(crate) struct DatasetSnapshot<B: TableCatalogObjectBackend> {
    pub(super) dataset: Arc<Dataset>,
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
        let ann_partitions = self.ann_numeric_proof().await?;
        dataset::query_matches(
            &self.dataset,
            query,
            &self.info.index,
            top_k,
            filter,
            ann_partitions,
            #[cfg(test)]
            &self.owner.ann_completions,
        )
        .await
    }

    #[cfg(test)]
    pub(super) fn ann_completions(&self) -> usize {
        self.owner.ann_completions.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn ann_numeric_proof(&self) -> Result<Option<usize>, DatasetError> {
        use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
        let indices = self.dataset.load_indices().await?;
        let Some(current_index) = indices.first() else {
            return Ok(None);
        };
        let versions = self.owner.committed_versions().await?;
        let mut proof_digest = Sha256::new();
        proof_digest.update(self.owner.resource.table_id().as_str().as_bytes());
        proof_digest.update(self.info.index.encryption_descriptor_sha256.as_bytes());
        let mut tracked_id = current_index.uuid;
        let mut source_version = None;
        let mut lineage_found = false;
        for (reference, descriptor) in versions.iter().rev() {
            let version = self.owner.descriptor(descriptor)?.manifest_version();
            if version > self.info.manifest_version {
                continue;
            }
            let validated = self.owner.validate_reference(reference, descriptor).await?;
            proof_digest.update(self.owner.identity(reference, version)?.as_bytes());
            for identity in self.owner.reference_identities(&validated, reference)? {
                proof_digest.update(identity.object_key().as_str().as_bytes());
                proof_digest.update(identity.body_revision().as_uuid().as_bytes());
            }
            let Some(transaction) = validated.transaction() else {
                continue;
            };
            match transaction.operation.as_ref() {
                Some(lance_table::format::pb::transaction::Operation::CreateIndex(create)) => {
                    let created = super::files::decode_indices(create.new_indices.clone())?;
                    if created.iter().any(|index| index.uuid == tracked_id) {
                        source_version = Some(transaction.read_version);
                        lineage_found = true;
                        break;
                    }
                }
                Some(lance_table::format::pb::transaction::Operation::Rewrite(rewrite)) => {
                    for remap in &rewrite.rewritten_indices {
                        let next = remap.new_id.as_ref().map(uuid::Uuid::try_from).transpose()?;
                        if next == Some(tracked_id) {
                            tracked_id = remap.old_id.as_ref().map(uuid::Uuid::try_from).transpose()?.ok_or(
                                LanceManifestError::CorruptManifest {
                                    reason: "index remap source identity is absent",
                                },
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
        if !lineage_found {
            return Ok(None);
        }
        let Some(source_version) = source_version else {
            return Ok(None);
        };
        let Some((reference, descriptor)) = versions.iter().find(|(_, descriptor)| {
            self.owner
                .descriptor(descriptor)
                .is_ok_and(|descriptor| descriptor.manifest_version() == source_version)
        }) else {
            return Ok(None);
        };
        let adapter = LanceManifestAdapter {
            inner: self.owner.clone(),
        };
        let original = adapter.open_selected(reference, descriptor, source_version, None).await?;
        proof_digest.update(self.owner.identity(reference, source_version)?.as_bytes());
        let validated = self.owner.validate_reference(reference, descriptor).await?;
        for identity in self.owner.reference_identities(&validated, reference)? {
            proof_digest.update(identity.object_key().as_str().as_bytes());
            proof_digest.update(identity.body_revision().as_uuid().as_bytes());
        }
        let cache_key: [u8; 32] = proof_digest.finalize().into();
        {
            let cache = self
                .owner
                .numeric_proofs
                .entries
                .lock()
                .map_err(|_| LanceManifestError::ReferenceMismatch {
                    reason: "numeric proof cache is unavailable",
                })?;
            if let Some((_, partitions)) = cache.iter().find(|(key, _)| key == &cache_key) {
                return Ok(Some(*partitions));
            }
        }
        if dataset::numeric_eligibility(&original.dataset, &self.info.index, None)
            .await?
            .is_none()
            || dataset::numeric_eligibility(&self.dataset, &self.info.index, None)
                .await?
                .is_none()
        {
            return Ok(None);
        }
        let native = self
            .dataset
            .open_vector_index("vector", &current_index.uuid, &lance_index::metrics::NoOpMetricsCollector)
            .await?;
        let partitions = native.total_partitions();
        let mut cache = self
            .owner
            .numeric_proofs
            .entries
            .lock()
            .map_err(|_| LanceManifestError::ReferenceMismatch {
                reason: "numeric proof cache is unavailable",
            })?;
        if cache.len() == 64 {
            cache.pop_front();
        }
        cache.push_back((cache_key, partitions));
        Ok(Some(partitions))
    }

    #[cfg(test)]
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
        self.objects.check_admission()?;
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
        self.validate_reference_with_gc_limit(reference, descriptor, None).await
    }

    async fn validate_reference_with_gc_limit(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
        reference_limit: Option<usize>,
    ) -> Result<ValidatedLanceManifest, LanceManifestError> {
        let decoded = self.descriptor(descriptor)?;
        if descriptor.metadata() != reference {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "Catalog descriptor and selected reference differ",
            });
        }
        let object = self.objects.read_exact(reference).await?;
        let mut validated = ValidatedLanceManifest::read_bound(
            object.into_bytes(),
            reference,
            &self.resource,
            &self.index,
            decoded.manifest_version(),
            self.limits.max_manifest_bytes.get(),
        )
        .await?;
        if let Some(limit) = reference_limit {
            let references = validated
                .manifest()
                .fragments
                .iter()
                .try_fold(1usize, |count, fragment| {
                    count
                        .checked_add(fragment.files.len())?
                        .checked_add(usize::from(fragment.deletion_file.is_some()))
                })
                .and_then(|count| count.checked_add(usize::from(validated.manifest().transaction_file.is_some())))
                .and_then(|count| {
                    validated
                        .indices()
                        .iter()
                        .try_fold(count, |count, index| count.checked_add(index.files.as_ref().map_or(0, Vec::len)))
                });
            if references.is_none_or(|count| count > limit) {
                return Err(LanceManifestError::ReadLimitExceeded {
                    length: references.unwrap_or(usize::MAX) as u64,
                    limit: limit as u64,
                });
            }
        }
        self.validate_dataset_files(&mut validated, reference).await?;
        decoded.verify_manifest(&validated)?;
        validated.retain_format_version(descriptor.format_version());
        Ok(validated)
    }

    async fn validate_dataset_files(
        &self,
        validated: &mut ValidatedLanceManifest,
        reference: &CatalogMetadataRef,
    ) -> Result<(), LanceManifestError> {
        self.check_guard()?;
        let mut references = super::files::manifest_objects(validated.manifest(), &self.resource)?;
        references.extend(super::files::index_references(validated.indices(), &self.resource)?);
        let mut file_checks = stream::iter(references)
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
            validated.bind_external_transaction(transaction)?;
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
        for index in validated.indices() {
            use lance::index::DatasetIndexInternalExt;
            let native = dataset
                .open_vector_index_from_metadata("vector", index, &lance_index::metrics::NoOpMetricsCollector)
                .await
                .map_err(|source| LanceManifestError::engine("validate ANN files", source))?;
            let model = native.ivf_model();
            let centroids = model.centroids.as_ref().ok_or(LanceManifestError::CorruptManifest {
                reason: "ANN centroids are absent",
            })?;
            use lance::deps::arrow_array::{Array, Float32Array};
            let values =
                centroids
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or(LanceManifestError::CorruptManifest {
                        reason: "ANN centroids are not float32",
                    })?;
            if centroids.null_count() != 0
                || values.null_count() != 0
                || centroids.len() != model.num_partitions()
                || values.values().iter().any(|value| !value.is_finite())
            {
                return Err(LanceManifestError::CorruptManifest {
                    reason: "ANN centroid shape or numeric value is invalid",
                });
            }
            let declared = index.fragment_bitmap.as_ref().ok_or(LanceManifestError::CorruptManifest {
                reason: "ANN fragment coverage is absent",
            })?;
            let mut observed_rows = 0u64;
            for partition in 0..native.total_partitions() {
                use futures::TryStreamExt;
                let mut batches = native
                    .partition_reader(partition, true, &lance_index::metrics::NoOpMetricsCollector)
                    .await
                    .map_err(|source| LanceManifestError::engine("read ANN partition", source))?;
                while let Some(batch) = batches
                    .try_next()
                    .await
                    .map_err(|source| LanceManifestError::engine("validate ANN partition", source.into()))?
                {
                    let _reservation = self.objects.budget_batch(batch.get_array_memory_size() as u64)?;
                    let ids = batch
                        .column_by_name("_rowid")
                        .and_then(|array| array.as_any().downcast_ref::<lance::deps::arrow_array::UInt64Array>())
                        .ok_or(LanceManifestError::CorruptManifest {
                            reason: "ANN row identities are absent",
                        })?;
                    let vectors = batch
                        .column_by_name("flat")
                        .and_then(|array| array.as_any().downcast_ref::<lance::deps::arrow_array::FixedSizeListArray>())
                        .ok_or(LanceManifestError::CorruptManifest {
                            reason: "ANN flat vectors are absent",
                        })?;
                    if ids.null_count() != 0
                        || vectors.null_count() != 0
                        || vectors.value_length() as u32 != self.index.dimension.get()
                        || vectors.values().data_type() != &lance::deps::arrow_schema::DataType::Float32
                    {
                        return Err(LanceManifestError::CorruptManifest {
                            reason: "ANN partition schema differs from Vector definition",
                        });
                    }
                    for id in ids.values() {
                        let fragment_id = (id >> 32) as u32;
                        if !declared.contains(fragment_id)
                            || validated
                                .manifest()
                                .fragments
                                .iter()
                                .find(|fragment| fragment.id == u64::from(fragment_id))
                                .is_some_and(|fragment| {
                                    fragment
                                        .physical_rows
                                        .is_none_or(|rows| (id & u64::from(u32::MAX)) >= rows as u64)
                                })
                        {
                            return Err(LanceManifestError::CorruptManifest {
                                reason: "ANN row lies outside its declared fragment",
                            });
                        }
                    }
                    observed_rows =
                        observed_rows
                            .checked_add(batch.num_rows() as u64)
                            .ok_or(LanceManifestError::CorruptManifest {
                                reason: "ANN row count overflowed",
                            })?;
                }
            }
            if observed_rows != native.num_rows() {
                return Err(LanceManifestError::CorruptManifest {
                    reason: "ANN partition row counts differ",
                });
            }
            if validated.transaction().is_some_and(|transaction| {
                matches!(
                    transaction.operation,
                    Some(lance_table::format::pb::transaction::Operation::CreateIndex(_))
                )
            }) {
                let rows = validated
                    .manifest()
                    .fragments
                    .iter()
                    .try_fold(0u64, |sum, fragment| sum.checked_add(fragment.num_rows()? as u64))
                    .ok_or(LanceManifestError::CorruptManifest {
                        reason: "ANN source live row count is invalid",
                    })?;
                if native.num_rows() != rows {
                    return Err(LanceManifestError::CorruptManifest {
                        reason: "ANN files do not cover all live source rows",
                    });
                }
            }
            let expected_metric = match self.index.distance_metric {
                super::schema::VectorDistanceMetric::Cosine => "cosine",
                super::schema::VectorDistanceMetric::Euclidean => "l2",
            };
            if centroids.value_length()
                != i32::try_from(self.index.dimension.get()).map_err(|_| LanceManifestError::SchemaMismatch {
                    reason: "ANN dimension exceeds native range",
                })?
                || native.metric_type()
                    != expected_metric
                        .try_into()
                        .map_err(|source: lance::deps::arrow_schema::ArrowError| {
                            LanceManifestError::engine("validate ANN metric", source.into())
                        })?
                || native.index_type() != lance_index::IndexType::IvfFlat
                || model.num_partitions() == 0
                || model.num_partitions() > 64
            {
                return Err(LanceManifestError::CorruptManifest {
                    reason: "ANN dimension, metric or partitions differ",
                });
            }
        }
        if let Some(guard) = &self.guard {
            self.catalog
                .validate_vector_references_with_guard(guard, reference, &self.reference_identities(validated, reference)?)
                .await?;
        }
        self.check_guard()
    }

    fn reference_identities(
        &self,
        validated: &ValidatedLanceManifest,
        reference: &CatalogMetadataRef,
    ) -> Result<Vec<crate::table_catalog::CatalogManagedObjectIdentity>, LanceManifestError> {
        let mut objects = super::files::manifest_objects(validated.manifest(), &self.resource)?;
        objects.extend(super::files::index_references(validated.indices(), &self.resource)?);
        let mut identities = objects
            .iter()
            .map(|object| self.objects.bound_data_identity(&object.key))
            .collect::<Result<Vec<_>, _>>()?;
        identities.push(crate::table_catalog::CatalogManagedObjectIdentity::try_new(
            reference.object_key().clone(),
            reference.body_revision().ok_or(LanceManifestError::ReferenceMismatch {
                reason: "manifest body revision is absent",
            })?,
        )?);
        identities.sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));
        identities.dedup();
        Ok(identities)
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
        if let Some(transaction) = transaction.as_ref() {
            self.validate_maintenance_base(request, manifest, indices.as_deref().unwrap_or_default(), transaction)
                .await?;
        }
        let bytes = self
            .objects
            .budget_bytes(encode_manifest(manifest, indices, transaction, writer).await?)?;
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
        let mut validated = ValidatedLanceManifest::read_bound(
            object.into_bytes(),
            &reference,
            &self.resource,
            &request.index,
            expected_version,
            self.limits.max_manifest_bytes.get(),
        )
        .await?;
        self.validate_dataset_files(&mut validated, &reference).await?;
        validated.retain_format_version(
            request
                .parts
                .expected_head
                .format_descriptor()
                .map_or(1, CatalogFormatDescriptor::format_version),
        );
        let proof = if let Some(guard) = &self.guard {
            Some(
                self.catalog
                    .validate_vector_references_with_guard(guard, &reference, &self.reference_identities(&validated, &reference)?)
                    .await?,
            )
        } else {
            None
        };
        let prepared = validated.into_prepared(clone_parts(&request.parts), &request.index, reference)?;
        Ok(if let Some(proof) = proof {
            prepared.with_retirement_validation(proof)?
        } else {
            prepared
        })
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
    published_head: Arc<OnceLock<crate::table_catalog::CatalogExpectedHead>>,
    published_record: Arc<OnceLock<crate::table_catalog::VectorMaintenanceHeadV1>>,
    selected: Option<(CatalogMetadataRef, CatalogFormatDescriptor)>,
    // The Dataset's history reader owns the key-merge base; publication uses a separate handler.
    computation_base: Option<CatalogExpectedHead>,
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
        if self.adapter.objects.has_maintenance_control() {
            let authorizer = self.adapter.maintenance_publication.as_ref().ok_or_else(|| {
                to_commit_error(LanceManifestError::ResourceMismatch {
                    reason: "maintenance publication requires current write authorization",
                })
            })?;
            authorizer
                .authorize(&self.adapter.resource, &reference)
                .await
                .map_err(to_commit_error)?;
            self.adapter.check_guard().map_err(to_commit_error)?;
        }
        #[cfg(test)]
        if self.adapter.guard.is_some()
            && !self.adapter.objects.has_maintenance_control()
            && matches!(prepared.operation(), "put-vectors" | "delete-vectors")
            && let Some(probe) = crate::vector::fault_test::hit(
                self.adapter.objects.fault_instance_id(),
                &self.adapter.resource,
                crate::vector::fault_test::FaultPoint::SdkCandidatePrepared,
            )
        {
            let adapter = LanceManifestAdapter {
                inner: self.adapter.clone(),
            };
            let references = match adapter.collect_candidate_references(&reference, &descriptor).await {
                Ok(references) => references,
                Err(error) => {
                    probe.observation_failed();
                    return Err(to_commit_error(error));
                }
            };
            probe
                .fail_sdk_candidate(&prepared, manifest.version, references)
                .map_err(|source| {
                    to_commit_error(LanceManifestError::Storage {
                        operation: "observe SDK candidate before Catalog publication",
                        source: Box::new(source),
                    })
                })?;
        }
        let result = if let Some(guard) = &self.adapter.guard {
            let definition = self.adapter.definition.as_ref().ok_or_else(|| {
                to_commit_error(LanceManifestError::ResourceMismatch {
                    reason: "guarded Dataset adapter has no Index definition",
                })
            })?;
            self.adapter
                .catalog
                .commit_vector_prepared_with_guard(guard, prepared, definition)
                .await
                .map_err(|source| to_commit_error(source.into()))?
        } else {
            self.adapter
                .catalog
                .commit_prepared(prepared)
                .await
                .map_err(|source| to_commit_error(source.into()))?
        };
        if self.adapter.guard.is_some() {
            let record = crate::table_catalog::VectorMaintenanceHeadV1::from_catalog_entry(&result.table)
                .map_err(|source| to_commit_error(source.into()))?;
            let _ = self.published_record.set(record);
        }
        let published_head = crate::table_catalog::CatalogExpectedHead::from_entry(
            &result.table,
            Some(reference.clone()),
            Some(descriptor.clone()),
        )
        .map_err(|source| to_commit_error(source.into()))?;
        if self.published_head.set(published_head.clone()).is_err() && self.published_head.get() != Some(&published_head) {
            return Err(to_commit_error(LanceManifestError::ReferenceMismatch {
                reason: "one stage produced competing Catalog heads",
            }));
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
            published_head: Arc::new(OnceLock::new()),
            published_record: Arc::new(OnceLock::new()),
            selected: None,
            computation_base: None,
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
            published_head: Arc::new(OnceLock::new()),
            published_record: Arc::new(OnceLock::new()),
            selected: None,
            computation_base: self.computation_base.clone(),
        };
        let base = base.clone();
        stream::once(async move {
            let locations = handler.locations(&base, true, Some(since)).await.map_err(to_engine_error)?;
            if let Some(expected) = &handler.computation_base {
                let current = handler.adapter.head().await.map_err(to_engine_error)?;
                if current.expected_head() != *expected {
                    // This history read precedes commit; the service must recompute the key merge.
                    return Err(lance_core::Error::commit_conflict_source(
                        since,
                        Box::new(LanceManifestError::ExpectedHeadConflict),
                    ));
                }
            }
            Ok(locations)
        })
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

pub(super) fn persisted_indices_match(left: &[IndexMetadata], right: &[IndexMetadata]) -> bool {
    // Native transactions persist created_at in milliseconds; uncommitted metadata retains nanoseconds.
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            lance_table::format::pb::IndexMetadata::from(left) == lance_table::format::pb::IndexMetadata::from(right)
        })
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

#[cfg(test)]
fn from_commit_error(error: CommitError) -> LanceManifestError {
    match error {
        CommitError::CommitConflict => LanceManifestError::ExpectedHeadConflict,
        CommitError::OtherError(source) => LanceManifestError::engine("commit manifest", source),
    }
}

pub(super) struct CompactionCommitHandler<B: TableCatalogObjectBackend> {
    pub(super) reserve: CatalogCommitHandler<B>,
    pub(super) rewrite: OnceLock<CatalogCommitHandler<B>>,
    rewrite_id: crate::table_catalog::CatalogCommitId,
    expected_fragments: u32,
    control: crate::table_catalog::VectorMaintenanceControl,
    #[cfg(test)]
    after_reserve: OnceLock<Arc<CompactionStagePause>>,
}

impl<B: TableCatalogObjectBackend> fmt::Debug for CompactionCommitHandler<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CompactionCommitHandler").finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct CompactionStagePause {
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
    fault_context: OnceLock<crate::vector::fault_test::FaultContext>,
}

impl<B: TableCatalogObjectBackend> CompactionCommitHandler<B> {
    #[cfg(test)]
    pub(super) fn pause_after_reserve(&self) -> Arc<CompactionStagePause> {
        self.after_reserve
            .get_or_init(|| Arc::new(CompactionStagePause::default()))
            .clone()
    }

    fn active(&self) -> &CatalogCommitHandler<B> {
        self.rewrite.get().unwrap_or(&self.reserve)
    }

    async fn check_head(&self) -> lance_core::Result<()> {
        self.control
            .check_admission()
            .map_err(|error| lance_core::Error::prerequisite_failed(error.to_string()))?;
        let handler = self.active();
        let expected = handler
            .attempt
            .as_ref()
            .ok_or_else(|| lance_core::Error::prerequisite_failed("compaction stage has no bound request"))?;
        let head = handler.adapter.head().await.map_err(to_engine_error)?;
        if head.expected_head() != expected.parts.expected_head {
            return Err(lance_core::Error::prerequisite_failed("compaction Catalog head changed"));
        }
        Ok(())
    }
}

#[async_trait]
impl<B: TableCatalogObjectBackend> CommitHandler for CompactionCommitHandler<B> {
    async fn resolve_latest_location(&self, base: &Path, store: &ObjectStore) -> lance_core::Result<ManifestLocation> {
        self.check_head().await?;
        self.active().resolve_latest_location(base, store).await
    }
    async fn resolve_version_location(
        &self,
        base: &Path,
        version: u64,
        store: &dyn ObjectStoreTrait,
    ) -> lance_core::Result<ManifestLocation> {
        self.active().resolve_version_location(base, version, store).await
    }

    async fn version_exists(
        &self,
        base: &Path,
        version: u64,
        store: &dyn ObjectStoreTrait,
        scheme: ManifestNamingScheme,
    ) -> lance_core::Result<bool> {
        self.active().version_exists(base, version, store, scheme).await
    }

    fn list_detached_manifest_locations<'a>(
        &self,
        base: &Path,
        store: &'a ObjectStore,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        self.active().list_detached_manifest_locations(base, store)
    }

    fn list_manifest_locations<'a>(
        &self,
        base: &Path,
        store: &'a ObjectStore,
        descending: bool,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        self.active().list_manifest_locations(base, store, descending)
    }

    fn list_manifest_locations_since<'a>(
        &self,
        base: &Path,
        store: &'a ObjectStore,
        since: u64,
    ) -> BoxStream<'a, lance_core::Result<ManifestLocation>> {
        self.active().list_manifest_locations_since(base, store, since)
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
        use lance_table::format::pb::transaction::Operation;
        self.check_head().await.map_err(CommitError::OtherError)?;
        let reserve = self.rewrite.get().is_none();
        match transaction
            .as_ref()
            .and_then(|transaction| transaction.as_pb().operation.as_ref())
        {
            Some(Operation::ReserveFragments(reservation)) if reserve && reservation.num_fragments == self.expected_fragments => {
            }
            Some(Operation::Rewrite(_)) if !reserve => {}
            _ => {
                return Err(CommitError::OtherError(lance_core::Error::prerequisite_failed(
                    "unexpected compaction operation or reservation count",
                )));
            }
        }
        let location = self
            .active()
            .publish(manifest, indices, base, writer, transaction)
            .await
            .map_err(|error| match error {
                CommitError::CommitConflict => {
                    CommitError::OtherError(lance_core::Error::prerequisite_failed("compaction stage publication conflicted"))
                }
                error => error,
            })?;
        if reserve {
            let expected_head = self.reserve.published_head.get().cloned().ok_or_else(|| {
                CommitError::OtherError(lance_core::Error::prerequisite_failed("reservation has no exact published head"))
            })?;
            let mut parts = clone_parts(
                &self
                    .reserve
                    .attempt
                    .as_ref()
                    .ok_or_else(|| {
                        CommitError::OtherError(lance_core::Error::prerequisite_failed("reservation request missing"))
                    })?
                    .parts,
            );
            parts.expected_head = expected_head;
            parts.commit_id = self.rewrite_id.clone();
            parts.operation = "rewrite".to_owned();
            let next = CatalogCommitHandler {
                adapter: self.reserve.adapter.clone(),
                attempt: Some(ManifestPrepareRequest {
                    parts,
                    index: self.reserve.adapter.index.clone(),
                }),
                published: Arc::new(OnceLock::new()),
                published_head: Arc::new(OnceLock::new()),
                published_record: Arc::new(OnceLock::new()),
                selected: None,
                computation_base: None,
            };
            self.rewrite.set(next).map_err(|_| {
                CommitError::OtherError(lance_core::Error::prerequisite_failed("reservation stage already completed"))
            })?;
        }
        #[cfg(test)]
        if reserve && let Some(pause) = self.after_reserve.get() {
            pause.entered.notify_one();
            if let Some(context) = pause.fault_context.get() {
                if let Some(probe) = crate::vector::fault_test::hit(
                    context.instance_id,
                    &context.resource,
                    crate::vector::fault_test::FaultPoint::CompactionReserved,
                ) {
                    match (self.reserve.published_head.get(), self.reserve.published_record.get()) {
                        (Some(head), Some(record)) => {
                            let mut observation = context.observation.clone();
                            observation.published_head = Some(head.into());
                            observation.reservation_record = Some(record.clone());
                            probe.pause(observation).await;
                        }
                        _ => probe.observation_failed(),
                    }
                }
            } else {
                pause.release.notified().await;
            }
        }
        Ok(location)
    }
    async fn delete(&self, _: &Path) -> lance_core::Result<()> {
        Err(lance_core::Error::not_supported("compaction cannot delete Dataset files"))
    }

    async fn forget_version(&self, base: &Path, version: u64, identity: &str) -> lance_core::Result<()> {
        self.active().forget_version(base, version, identity).await
    }
}

impl<B: TableCatalogObjectBackend> AdapterState<B> {
    async fn validate_maintenance_base(
        &self,
        request: &ManifestPrepareRequest,
        candidate: &Manifest,
        indices: &[IndexMetadata],
        transaction: &Transaction,
    ) -> Result<(), LanceManifestError> {
        use lance_table::transaction::Operation;
        let transaction = lance_table::transaction::Transaction::try_from(transaction.as_pb().clone())
            .map_err(|source| LanceManifestError::engine("decode maintenance transaction", source))?;
        if !matches!(
            transaction.operation,
            Operation::CreateIndex { .. } | Operation::ReserveFragments { .. } | Operation::Rewrite { .. }
        ) {
            return Ok(());
        }
        let expected = &request.parts.expected_head;
        let base = self
            .validate_reference(
                expected.metadata().ok_or(LanceManifestError::ReferenceMismatch {
                    reason: "maintenance base is absent",
                })?,
                expected.format_descriptor().ok_or(LanceManifestError::ReferenceMismatch {
                    reason: "maintenance descriptor is absent",
                })?,
            )
            .await?;
        if candidate.schema != base.manifest().schema
            || candidate.table_metadata != base.manifest().table_metadata
            || candidate.config != base.manifest().config
            || candidate.data_storage_format != base.manifest().data_storage_format
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "maintenance changed schema, binding or configuration",
            });
        }
        super::files::validate_indices(indices, candidate, &self.index)?;
        match &transaction.operation {
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            } => {
                if transaction.read_version != base.manifest().version
                    || candidate.fragments != base.manifest().fragments
                    || candidate.max_fragment_id != base.manifest().max_fragment_id
                    || removed_indices.as_slice() != base.indices()
                    || !persisted_indices_match(new_indices, indices)
                    || indices.len() != 1
                {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "ANN transaction differs from its complete computation base",
                    });
                }
                let coverage = indices[0]
                    .fragment_bitmap
                    .as_ref()
                    .ok_or(LanceManifestError::ReferenceMismatch {
                        reason: "ANN coverage absent",
                    })?;
                if indices[0].dataset_version != transaction.read_version
                    || coverage.len() != base.manifest().fragments.len() as u64
                    || base
                        .manifest()
                        .fragments
                        .iter()
                        .any(|fragment| !coverage.contains(fragment.id as u32))
                {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "ANN coverage differs from the exact indexed snapshot",
                    });
                }
            }
            Operation::ReserveFragments { num_fragments } => {
                if transaction.read_version != base.manifest().version
                    || candidate.fragments != base.manifest().fragments
                    || indices != base.indices()
                    || base.manifest().max_fragment_id.unwrap_or(0).checked_add(*num_fragments) != candidate.max_fragment_id
                {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "fragment reservation changed the computation base",
                    });
                }
            }
            Operation::Rewrite { .. } => {
                let Some(reservation) = base.transaction() else {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite has no committed reservation",
                    });
                };
                let Some(lance_table::format::pb::transaction::Operation::ReserveFragments(reservation_count)) =
                    &reservation.operation
                else {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite predecessor is not a reservation",
                    });
                };
                if transaction.read_version != reservation.read_version
                    || transaction.read_version.checked_add(2) != Some(candidate.version)
                    || candidate.max_fragment_id != base.manifest().max_fragment_id
                {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite changed the original read version or reservation",
                    });
                }
                let (original_reference, original_descriptor) = self.committed_version(transaction.read_version).await?;
                let original = self.validate_reference(&original_reference, &original_descriptor).await?;
                let proto = lance_table::format::pb::Transaction::try_from(&transaction)
                    .map_err(|source| LanceManifestError::engine("encode maintenance transaction", source))?;
                let Some(lance_table::format::pb::transaction::Operation::Rewrite(rewrite)) = proto.operation else {
                    return Err(LanceManifestError::CorruptManifest {
                        reason: "rewrite encoding differs",
                    });
                };
                let old_max = original.manifest().max_fragment_id.unwrap_or(0);
                let mut removed = std::collections::BTreeSet::new();
                let mut added = std::collections::BTreeSet::new();
                for group in &rewrite.groups {
                    for old in &group.old_fragments {
                        if !removed.insert(old.id)
                            || !original
                                .manifest()
                                .fragments
                                .iter()
                                .any(|fragment| lance_table::format::pb::DataFragment::from(fragment) == *old)
                        {
                            return Err(LanceManifestError::ReferenceMismatch {
                                reason: "rewrite source differs from original snapshot",
                            });
                        }
                    }
                    for new in &group.new_fragments {
                        if !added.insert(new.id)
                            || new.id <= u64::from(old_max)
                            || new.id > u64::from(candidate.max_fragment_id.unwrap_or(0))
                        {
                            return Err(LanceManifestError::ReferenceMismatch {
                                reason: "rewrite output is outside its reservation",
                            });
                        }
                    }
                }
                if added.len() as u64 != u64::from(reservation_count.num_fragments) {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite output count differs from reservation",
                    });
                }
                let expected = original
                    .manifest()
                    .fragments
                    .iter()
                    .filter(|fragment| !removed.contains(&fragment.id));
                if expected
                    .clone()
                    .any(|fragment| !candidate.fragments.iter().any(|current| current == fragment))
                    || candidate.fragments.len() != expected.count() + added.len()
                {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite changed surviving fragments",
                    });
                }
                if indices.len() != original.indices().len() || rewrite.rewritten_indices.len() > indices.len() {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite changed the logical index set",
                    });
                }
                for previous in original.indices() {
                    let remap = rewrite
                        .rewritten_indices
                        .iter()
                        .find(|remap| remap.old_id.as_ref().and_then(|id| uuid::Uuid::try_from(id).ok()) == Some(previous.uuid));
                    let expected_id = remap
                        .map(|remap| remap.new_id.as_ref().and_then(|id| uuid::Uuid::try_from(id).ok()))
                        .unwrap_or(Some(previous.uuid))
                        .ok_or(LanceManifestError::ReferenceMismatch {
                            reason: "rewrite remap identity is absent",
                        })?;
                    let current =
                        indices
                            .iter()
                            .find(|index| index.uuid == expected_id)
                            .ok_or(LanceManifestError::ReferenceMismatch {
                                reason: "rewritten index is absent",
                            })?;
                    if current.name != previous.name
                        || current.fields != previous.fields
                        || current.index_version != previous.index_version
                        || current.index_details != previous.index_details
                    {
                        return Err(LanceManifestError::ReferenceMismatch {
                            reason: "rewrite changed index parameters",
                        });
                    }
                    if let Some(remap) = remap {
                        let file_table = current.files.as_ref().ok_or(LanceManifestError::ReferenceMismatch {
                            reason: "rewritten index file table absent",
                        })?;
                        if remap.new_index_files.len() != file_table.len()
                            || remap.new_index_files.iter().any(|file| {
                                !file_table
                                    .iter()
                                    .any(|current| current.path == file.path && current.size_bytes == file.size_bytes)
                            })
                        {
                            return Err(LanceManifestError::ReferenceMismatch {
                                reason: "rewrite index file table differs",
                            });
                        }
                        let mut coverage = previous
                            .fragment_bitmap
                            .clone()
                            .ok_or(LanceManifestError::ReferenceMismatch {
                                reason: "original coverage absent",
                            })?;
                        for group in &rewrite.groups {
                            if group
                                .old_fragments
                                .iter()
                                .any(|fragment| coverage.contains(fragment.id as u32))
                            {
                                for old in &group.old_fragments {
                                    coverage.remove(old.id as u32);
                                }
                                for new in &group.new_fragments {
                                    coverage.insert(new.id as u32);
                                }
                            }
                        }
                        if current.fragment_bitmap.as_ref() != Some(&coverage) {
                            return Err(LanceManifestError::ReferenceMismatch {
                                reason: "remapped coverage differs from rewrite",
                            });
                        }
                    } else if current != previous {
                        return Err(LanceManifestError::ReferenceMismatch {
                            reason: "unchanged index differs from original",
                        });
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl<B: TableCatalogObjectBackend> LanceManifestAdapter<B> {
    pub(crate) async fn execute_maintenance(
        &self,
        work: crate::table_catalog::VectorMaintenanceWork,
        control: crate::table_catalog::VectorMaintenanceControl,
    ) -> Result<crate::table_catalog::VectorMaintenanceExecutionOutcome, crate::table_catalog::VectorMaintenanceExecutionError>
    {
        use crate::table_catalog::*;
        control.check_admission()?;
        let dispatch = control.dispatch_guard()?;
        dispatch.check_held()?;
        let job = work.snapshot().job();
        if work.lease() != dispatch.lease() || job.resource().identity()? != self.inner.resource {
            return Err(
                TableCatalogStoreError::Conflict("maintenance work differs from the admitted Lance resource".into()).into(),
            );
        }
        let guard = self
            .inner
            .guard
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance requires a Catalog guard".into()))?;
        let observed = self.inner.catalog.read_vector_maintenance_resource_with_guard(guard).await?;
        if observed.head() != job.input().base_head().as_ref() {
            return Ok(VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::ResourceChanged,
            });
        }
        let stage = match job.kind() {
            VectorMaintenanceKind::AnnBuild => "ann-build",
            VectorMaintenanceKind::Compaction => "reserve-fragments",
            _ => return Err(TableCatalogStoreError::Invalid("Lance compute requires ANN or Compaction work".into()).into()),
        };
        if self
            .inner
            .catalog
            .read_vector_maintenance_stage_commit(dispatch, work.snapshot(), stage)
            .await?
            .is_some()
        {
            return Ok(VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::UnknownOutcome,
            });
        }
        let base_record = job
            .input()
            .base_head()
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("compute job has no original head".into()))?;
        let head = CatalogHead::from_maintenance_snapshot(job.resource(), base_record)?;
        let request = self
            .maintenance_stage_request(work.snapshot(), stage, head.expected_head())
            .map_err(maintenance_execution_error)?;
        self.inner
            .objects
            .set_maintenance_control(control.clone())
            .map_err(maintenance_execution_error)?;
        let candidate_key = self
            .maintenance_stage_candidate_key(&request)
            .map_err(maintenance_execution_error)?;
        if self
            .inner
            .objects
            .discover_exact_candidate(&candidate_key)
            .await
            .map_err(maintenance_execution_error)?
            .is_some()
        {
            return Ok(VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::UnknownOutcome,
            });
        }
        let snapshot = self
            .maintenance_base(self.inner.objects.access(), &request)
            .await
            .map_err(maintenance_execution_error)?;
        let outcome = match job.kind() {
            VectorMaintenanceKind::AnnBuild => {
                #[cfg(test)]
                let fault_context = self.maintenance_fault_context(&request, snapshot.info.manifest_version, job, "ann-build");
                let handler = Arc::new(self.handler(Some(request)));
                let operation = dataset::build_ann(snapshot.dataset.clone(), &self.inner.index, handler.clone(), &control);
                #[cfg(test)]
                let operation = crate::vector::fault_test::with_ann_context(fault_context, operation);
                match operation.await {
                    Ok(None) => {
                        return self
                            .maintenance_no_work(&work, VectorMaintenanceNoWorkReason::NoCandidates)
                            .await;
                    }
                    Ok(Some(_)) => {
                        let result = self
                            .maintenance_stage_result(&work, &control, "ann-build", base_record.clone(), &handler)
                            .await?;
                        vec![result]
                    }
                    Err(error) => return Err(maintenance_execution_error(error)),
                }
            }
            VectorMaintenanceKind::Compaction => {
                let mut options = dataset::compaction_options();
                let mut selected = std::collections::BTreeSet::new();
                let mut rows = 0usize;
                let mut bytes = 0u64;
                let fragments = snapshot.dataset.get_fragments();
                for fragment in &fragments {
                    let metadata = fragment.metadata();
                    let live = metadata.num_rows().ok_or_else(|| {
                        maintenance_execution_error(LanceManifestError::CorruptManifest {
                            reason: "compaction source lacks live row count",
                        })
                    })?;
                    let size = metadata
                        .files
                        .iter()
                        .try_fold(0u64, |sum, file| sum.checked_add(file.file_size_bytes.get()?.get()))
                        .ok_or_else(|| {
                            maintenance_execution_error(LanceManifestError::CorruptManifest {
                                reason: "compaction source byte count is missing or overflowed",
                            })
                        })?;
                    if selected.len() < 8
                        && rows.checked_add(live).is_some_and(|next| next <= 65_536)
                        && bytes.checked_add(size).is_some_and(|next| next <= 67_108_864)
                    {
                        selected.insert(fragment.id());
                        rows += live;
                        bytes += size;
                    }
                }
                options.excluded_fragment_ids = fragments
                    .iter()
                    .filter(|fragment| !selected.contains(&fragment.id()))
                    .map(|fragment| fragment.id() as u32)
                    .collect();
                let plan = lance::dataset::optimize::plan_compaction(&snapshot.dataset, &options)
                    .await
                    .map_err(maintenance_execution_error)?;
                if plan.num_tasks() == 0 {
                    if !options.excluded_fragment_ids.is_empty() {
                        return Err(VectorMaintenanceExecutionError::Budget(VectorMaintenanceBudgetError::Unavailable));
                    }
                    return self
                        .maintenance_no_work(&work, VectorMaintenanceNoWorkReason::NoCandidates)
                        .await;
                }
                let mut results = Vec::new();
                for task in plan.compaction_tasks() {
                    control.check_admission()?;
                    let result = task.execute(&snapshot.dataset).await.map_err(maintenance_execution_error)?;
                    if result.read_version != snapshot.info.manifest_version {
                        return Err(maintenance_execution_error(LanceManifestError::ReferenceMismatch {
                            reason: "compaction result changed the original version",
                        }));
                    }
                    results.push(result);
                }
                let expected_fragments = results
                    .iter()
                    .try_fold(0u32, |sum, result| sum.checked_add(u32::try_from(result.new_fragments.len()).ok()?))
                    .ok_or_else(|| {
                        maintenance_execution_error(LanceManifestError::CorruptManifest {
                            reason: "compaction fragment reservation overflowed",
                        })
                    })?;
                #[cfg(test)]
                let fault_context = {
                    let mut context =
                        self.maintenance_fault_context(&request, snapshot.info.manifest_version, job, "reserve-fragments");
                    context.observation.fragment_ids = results
                        .iter()
                        .flat_map(|result| result.original_fragments.iter().map(|fragment| fragment.id))
                        .collect();
                    context
                };
                let handler = self.compaction_handler(
                    request,
                    vector_maintenance_stage_commit_id(*job.job_id(), *job.attempt(), "rewrite")?,
                    expected_fragments,
                    control.clone(),
                );
                #[cfg(test)]
                handler.pause_after_reserve().fault_context.get_or_init(|| fault_context);
                let mut dataset = self
                    .open_for_compaction(snapshot.info.manifest_version, handler.clone())
                    .await
                    .map_err(maintenance_execution_error)?;
                control.check_admission()?;
                lance::dataset::optimize::commit_compaction(
                    &mut dataset,
                    results,
                    Arc::new(lance::dataset::index::DatasetIndexRemapperOptions::default()),
                    &options,
                )
                .await
                .map_err(maintenance_execution_error)?;
                let reserve = self
                    .maintenance_stage_result(&work, &control, "reserve-fragments", base_record.clone(), &handler.reserve)
                    .await?;
                let rewrite = handler.rewrite.get().ok_or_else(|| {
                    maintenance_execution_error(LanceManifestError::ReferenceMismatch {
                        reason: "compaction omitted the rewrite stage",
                    })
                })?;
                let reserve_head = handler.reserve.published_record.get().cloned().ok_or_else(|| {
                    maintenance_execution_error(LanceManifestError::ReferenceMismatch {
                        reason: "compaction omitted the reservation head",
                    })
                })?;
                let rewritten = self
                    .maintenance_stage_result(&work, &control, "rewrite", reserve_head, rewrite)
                    .await?;
                vec![reserve, rewritten]
            }
            _ => unreachable!(),
        };
        let commit_refs = outcome.iter().filter_map(|stage| stage.commit_ref().clone()).collect();
        let result = VectorMaintenanceResultV1::try_for_work(
            &work,
            VectorMaintenanceResultEffect::Published,
            outcome,
            commit_refs,
            Vec::new(),
            Vec::new(),
            None,
        )?;
        Ok(VectorMaintenanceExecutionOutcome::Completed {
            result: Box::new(result),
        })
    }

    async fn maintenance_no_work(
        &self,
        work: &crate::table_catalog::VectorMaintenanceWork,
        reason: crate::table_catalog::VectorMaintenanceNoWorkReason,
    ) -> Result<crate::table_catalog::VectorMaintenanceExecutionOutcome, crate::table_catalog::VectorMaintenanceExecutionError>
    {
        use crate::table_catalog::*;
        let guard = self
            .inner
            .guard
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance guard is absent".into()))?;
        let observed = self.inner.catalog.read_vector_maintenance_resource_with_guard(guard).await?;
        if observed.head() != work.snapshot().job().input().base_head().as_ref() {
            return Ok(VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::ResourceChanged,
            });
        }
        Ok(VectorMaintenanceExecutionOutcome::NoWork {
            observed: Box::new(observed),
            reason,
        })
    }

    async fn maintenance_stage_result(
        &self,
        work: &crate::table_catalog::VectorMaintenanceWork,
        control: &crate::table_catalog::VectorMaintenanceControl,
        stage: &str,
        expected: crate::table_catalog::VectorMaintenanceHeadV1,
        handler: &CatalogCommitHandler<B>,
    ) -> Result<crate::table_catalog::VectorMaintenanceStageResultV1, crate::table_catalog::VectorMaintenanceExecutionError> {
        use crate::table_catalog::*;
        let published = handler
            .published
            .get()
            .ok_or_else(|| TableCatalogStoreError::Invalid("stage returned without publication".into()))?;
        let (log, reference) = self
            .inner
            .catalog
            .read_vector_maintenance_stage_commit(control.dispatch_guard()?, work.snapshot(), stage)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Unavailable("published stage log is absent".into()))?;
        if log.status != CommitLogStatus::Committed || log.new_metadata_location != published.reference.object_key().as_str() {
            return Err(TableCatalogStoreError::Unavailable("stage log does not prove the publication".into()).into());
        }
        let reference = self
            .inner
            .catalog
            .copy_vector_maintenance_source_with_guard(control.dispatch_guard()?, &reference)
            .await?;
        Ok(VectorMaintenanceStageResultV1::try_new(
            stage.to_owned(),
            vector_maintenance_stage_commit_id(*work.snapshot().job().job_id(), *work.snapshot().job().attempt(), stage)?,
            Some(expected),
            Some((&published.reference).into()),
            VectorMaintenanceStageOutcome::Committed,
            Some((&reference).into()),
        )?)
    }
}

fn maintenance_execution_error(error: impl Into<DatasetError>) -> crate::table_catalog::VectorMaintenanceExecutionError {
    let error = error.into();
    let category = if error.is_definite_conflict() {
        crate::table_catalog::VectorMaintenanceFailureCode::Conflict
    } else {
        crate::table_catalog::VectorMaintenanceFailureCode::UnknownOutcome
    };
    crate::table_catalog::VectorMaintenanceExecutionError::Executor {
        category,
        source: Box::new(error),
    }
}

impl<B: TableCatalogObjectBackend> LanceManifestAdapter<B> {
    /// Parse every retained committed snapshot and the exact selected root before producing GC evidence.
    pub(crate) async fn collect_references(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> Result<Vec<CatalogMetadataRef>, LanceManifestError> {
        // The returned plain evidence is retained by the caller's proof reservation.
        // This separate reservation covers map nodes, path clones and serialization during construction.
        let _construction_memory = self.inner.objects.budget_memory(64 * 1024 * 1024)?;
        let mut roots = if let Some(guard) = &self.inner.guard {
            let history = self.inner.catalog.read_vector_maintenance_history_with_guard(guard).await?;
            if history.len() > 4096 {
                return Err(LanceManifestError::ReadLimitExceeded {
                    length: history.len() as u64,
                    limit: 4096,
                });
            }
            let descriptor_bytes = history
                .iter()
                .try_fold(0usize, |bytes, version| bytes.checked_add(version.format_descriptor().payload().len()));
            if descriptor_bytes.is_none_or(|bytes| bytes > 8 * 1024 * 1024) {
                return Err(LanceManifestError::ReadLimitExceeded {
                    length: descriptor_bytes.unwrap_or(usize::MAX) as u64,
                    limit: 8 * 1024 * 1024,
                });
            }
            history
                .into_iter()
                .map(|version| (version.metadata().clone(), version.format_descriptor().clone()))
                .collect::<Vec<_>>()
        } else {
            self.inner.committed_versions().await?
        };
        if !roots.iter().any(|(root, _)| root == reference) {
            roots.push((reference.clone(), descriptor.clone()));
        }
        self.collect_gc_reference_roots(roots, false).await
    }

    /// Include the exact candidate and its native original-base dependencies for unresolved GC evidence.
    pub(crate) async fn collect_candidate_references(
        &self,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> Result<Vec<CatalogMetadataRef>, LanceManifestError> {
        let _construction_memory = self.inner.objects.budget_memory(64 * 1024 * 1024)?;
        self.collect_gc_reference_roots(vec![(reference.clone(), descriptor.clone())], true)
            .await
    }

    async fn collect_gc_reference_roots(
        &self,
        roots: Vec<(CatalogMetadataRef, CatalogFormatDescriptor)>,
        include_original_base: bool,
    ) -> Result<Vec<CatalogMetadataRef>, LanceManifestError> {
        if roots.len() > 4096 {
            return Err(LanceManifestError::ReadLimitExceeded {
                length: roots.len() as u64,
                limit: 4096,
            });
        }
        let mut closure = BTreeMap::<String, CatalogMetadataRef>::new();
        let mut roots = VecDeque::from(roots);
        let mut first = true;
        while let Some((reference, descriptor)) = roots.pop_front() {
            let validated = self
                .inner
                .validate_reference_with_gc_limit(&reference, &descriptor, Some(4096))
                .await?;
            if include_original_base && first {
                first = false;
                if let Some(transaction) = validated.transaction().filter(|transaction| transaction.read_version > 0) {
                    let mut required = vec![transaction.read_version];
                    if matches!(transaction.operation, Some(lance_table::format::pb::transaction::Operation::Rewrite(_))) {
                        required.push(validated.manifest().version - 1);
                    }
                    let guard = self.inner.guard.as_ref().ok_or(LanceManifestError::ResourceMismatch {
                        reason: "candidate evidence requires a real Catalog guard",
                    })?;
                    let history = self.inner.catalog.read_vector_maintenance_history_with_guard(guard).await?;
                    for version in required {
                        let mut found = None;
                        for previous in &history {
                            if self.inner.descriptor(previous.format_descriptor())?.manifest_version() == version {
                                if found.is_some() {
                                    return Err(LanceManifestError::ReferenceMismatch {
                                        reason: "candidate source has competing committed manifests",
                                    });
                                }
                                found = Some((previous.metadata().clone(), previous.format_descriptor().clone()));
                            }
                        }
                        roots.push_back(found.ok_or(LanceManifestError::VersionNotCommitted { version })?);
                    }
                }
            }
            let mut files = super::files::manifest_objects(validated.manifest(), &self.inner.resource)?;
            files.extend(super::files::index_references(validated.indices(), &self.inner.resource)?);
            let references = stream::iter(files)
                .map(|file| async move { self.inner.objects.exact_data_reference(&file.key).await })
                .buffer_unordered(self.inner.limits.max_concurrent_reads.get());
            futures::pin_mut!(references);
            insert_closure_reference(&mut closure, reference)?;
            while let Some(reference) = references.next().await {
                insert_closure_reference(&mut closure, reference?)?;
            }
        }
        let closure = closure.into_values().collect::<Vec<_>>();
        let records = closure
            .iter()
            .map(crate::table_catalog::CatalogMetadataRefRecordV1::from)
            .collect::<Vec<_>>();
        let length = serde_json::to_vec(&records)?.len() as u64;
        if length > 4 * 1024 * 1024 {
            return Err(LanceManifestError::ReadLimitExceeded {
                length,
                limit: 4 * 1024 * 1024,
            });
        }
        self.inner.check_guard()?;
        Ok(closure)
    }

    /// Validate an exact Catalog log candidate, including mutations that preserve the Dataset head.
    pub(crate) async fn prepare_exact_catalog_candidate(
        &self,
        request: ManifestPrepareRequest,
        reference: &CatalogMetadataRef,
        descriptor: &CatalogFormatDescriptor,
    ) -> Result<PreparedCatalogCommit, LanceManifestError> {
        self.inner.check_request(&request)?;
        let validated = self.inner.validate_reference(reference, descriptor).await?;
        let unchanged = request.parts.expected_head.metadata() == Some(reference)
            && request.parts.expected_head.format_descriptor() == Some(descriptor);
        if !unchanged {
            let version = self.inner.successor_version(&request)?;
            if validated.manifest().version != version {
                return Err(LanceManifestError::VersionMismatch {
                    expected: version,
                    actual: validated.manifest().version,
                });
            }
            super::manifest::validate_manifest_path(
                &self.inner.resource,
                reference,
                version,
                Some(request.parts.commit_id.as_str()),
            )?;
            let transaction = validated.transaction().cloned().ok_or(LanceManifestError::CorruptManifest {
                reason: "Dataset recovery candidate has no transaction",
            })?;
            self.inner
                .validate_maintenance_base(&request, validated.manifest(), validated.indices(), &Transaction::from(transaction))
                .await?;
        }
        let guard = self.inner.guard.as_ref().ok_or(LanceManifestError::ResourceMismatch {
            reason: "Catalog recovery requires a real operation guard",
        })?;
        let proof = self
            .inner
            .catalog
            .validate_vector_references_with_guard(guard, reference, &self.inner.reference_identities(&validated, reference)?)
            .await?;
        let updates = CatalogEntryUpdates::try_new(
            Some(format!(
                "s3://{}/{}",
                self.inner.resource.storage_bucket_id(),
                self.inner.resource.dataset_prefix().trim_end_matches('/')
            )),
            Some(descriptor.format_version()),
        )?;
        Ok(PreparedCatalogCommit::try_from_validated_parts(
            request.parts,
            reference.clone(),
            descriptor.clone(),
            updates,
            Vec::new(),
        )?
        .with_retirement_validation(proof)?)
    }

    /// Reconstruct preparation from immutable candidate bytes; never rerun native work during recovery.
    pub(crate) async fn prepare_existing_maintenance_candidate(
        &self,
        request: ManifestPrepareRequest,
    ) -> Result<Option<PreparedCatalogCommit>, LanceManifestError> {
        self.inner.check_request(&request)?;
        let version = self.inner.successor_version(&request)?;
        let key = CatalogObjectKey::try_new(format!(
            "{}_versions/{}/{}.manifest",
            self.inner.resource.dataset_prefix(),
            request.parts.commit_id.as_str(),
            version
        ))?;
        let Some(reference) = self.inner.objects.discover_exact_candidate(&key).await? else {
            return Ok(None);
        };
        let object = self.inner.objects.read_exact(&reference).await?;
        let mut validated = ValidatedLanceManifest::read_bound(
            object.into_bytes(),
            &reference,
            &self.inner.resource,
            &self.inner.index,
            version,
            self.inner.limits.max_manifest_bytes.get(),
        )
        .await?;
        self.inner.validate_dataset_files(&mut validated, &reference).await?;
        let transaction = validated.transaction().cloned().ok_or(LanceManifestError::CorruptManifest {
            reason: "maintenance candidate has no transaction",
        })?;
        self.inner
            .validate_maintenance_base(&request, validated.manifest(), validated.indices(), &Transaction::from(transaction))
            .await?;
        validated.retain_format_version(2);
        let guard = self.inner.guard.as_ref().ok_or(LanceManifestError::ResourceMismatch {
            reason: "candidate recovery requires a Catalog guard",
        })?;
        let proof = self
            .inner
            .catalog
            .validate_vector_references_with_guard(guard, &reference, &self.inner.reference_identities(&validated, &reference)?)
            .await?;
        Ok(Some(
            validated
                .into_prepared(request.parts, &request.index, reference)?
                .with_retirement_validation(proof)?,
        ))
    }
}

fn insert_closure_reference(
    closure: &mut BTreeMap<String, CatalogMetadataRef>,
    reference: CatalogMetadataRef,
) -> Result<(), LanceManifestError> {
    if let Some(previous) = closure.get(reference.object_key().as_str()) {
        if previous != &reference {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "reference closure has competing object identities",
            });
        }
        return Ok(());
    }
    if closure.len() == 4096 {
        return Err(LanceManifestError::ReadLimitExceeded {
            length: 4097,
            limit: 4096,
        });
    }
    closure.insert(reference.object_key().as_str().to_owned(), reference);
    Ok(())
}

impl<B: TableCatalogObjectBackend> LanceManifestAdapter<B> {
    pub(crate) fn maintenance_stage_request(
        &self,
        original: &crate::table_catalog::VectorMaintenanceJobSnapshot,
        stage: &str,
        expected_head: crate::table_catalog::CatalogExpectedHead,
    ) -> Result<ManifestPrepareRequest, LanceManifestError> {
        use crate::table_catalog::*;
        let job = original.job();
        if job.resource().identity()? != self.inner.resource
            || !matches!(
                (job.kind(), stage),
                (VectorMaintenanceKind::AnnBuild, "ann-build")
                    | (VectorMaintenanceKind::Compaction, "reserve-fragments" | "rewrite")
            )
        {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "maintenance stage differs from the original immutable job",
            });
        }
        Ok(ManifestPrepareRequest {
            parts: CatalogCommitParts {
                resource_identity: self.inner.resource.clone(),
                namespace: self.inner.namespace.clone(),
                name: self.inner.name.clone(),
                expected_head,
                commit_id: vector_maintenance_stage_commit_id(*job.job_id(), *job.attempt(), stage)?,
                operation: stage.to_owned(),
                idempotency_key: None,
                writer: Some("vector-maintenance".into()),
            },
            index: self.inner.index.clone(),
        })
    }

    pub(crate) fn maintenance_stage_candidate_key(
        &self,
        request: &ManifestPrepareRequest,
    ) -> Result<CatalogObjectKey, LanceManifestError> {
        self.inner.check_request(request)?;
        Ok(CatalogObjectKey::try_new(format!(
            "{}_versions/{}/{}.manifest",
            self.inner.resource.dataset_prefix(),
            request.parts.commit_id.as_str(),
            self.inner.successor_version(request)?
        ))?)
    }
}
