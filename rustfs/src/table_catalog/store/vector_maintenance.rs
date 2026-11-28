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

use super::object::next_table_catalog_update_time;
use super::*;
use rustfs_lock::{NamespaceLockGuard, distributed_lock::LockLostSignal};
use std::num::NonZeroU16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorMaintenanceJobSnapshot {
    job: VectorMaintenanceJobV1,
    etag: String,
    observed_object: CatalogMetadataRef,
}
impl VectorMaintenanceJobSnapshot {
    pub(crate) fn observed_object(&self) -> &CatalogMetadataRef {
        &self.observed_object
    }

    pub(crate) fn job(&self) -> &VectorMaintenanceJobV1 {
        &self.job
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VectorMaintenanceLeaseToken {
    #[serde(skip)]
    resource: CatalogResourceIdentity,
    job_id: Uuid,
    attempt: u16,
    lease_id: Uuid,
    worker_id: String,
    worker_boot_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    #[serde(skip)]
    claim_digest: CatalogRequestDigest,
}
impl VectorMaintenanceLeaseToken {
    pub(crate) fn resource(&self) -> &CatalogResourceIdentity {
        &self.resource
    }
    pub(crate) fn job_id(&self) -> Uuid {
        self.job_id
    }
    pub(crate) fn attempt(&self) -> u16 {
        self.attempt
    }
    pub(crate) fn lease_id(&self) -> Uuid {
        self.lease_id
    }
    pub(crate) fn worker_id(&self) -> &str {
        &self.worker_id
    }
    pub(crate) fn worker_boot_id(&self) -> Uuid {
        self.worker_boot_id
    }
    pub(crate) fn execution_identity(&self) -> &VectorMaintenanceExecutionIdentityV1 {
        &self.execution_identity
    }
    fn from_job(job: &VectorMaintenanceJobV1) -> TableCatalogStoreResult<Self> {
        let lease = job
            .lease()
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("job has no lease".into()))?;
        let bytes = hex_simd::decode_to_vec(lease.claim_digest_sha256().as_bytes())
            .map_err(|e| TableCatalogStoreError::Invalid(e.to_string()))?;
        let digest: [u8; 32] = bytes
            .try_into()
            .map_err(|_| TableCatalogStoreError::Invalid("invalid claim digest length".into()))?;
        Ok(Self {
            resource: job.resource().identity()?,
            job_id: *job.job_id(),
            attempt: *job.attempt(),
            lease_id: *lease.lease_id(),
            worker_id: lease.worker_id().clone(),
            worker_boot_id: *lease.worker_boot_id(),
            execution_identity: job.execution_identity().clone(),
            claim_digest: CatalogRequestDigest::from_bytes(digest),
        })
    }
    fn matches(&self, job: &VectorMaintenanceJobV1) -> TableCatalogStoreResult<bool> {
        Ok(Self::from_job(job)? == *self)
    }
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceWork {
    snapshot: VectorMaintenanceJobSnapshot,
    lease: VectorMaintenanceLeaseToken,
}
impl VectorMaintenanceWork {
    pub(crate) fn snapshot(&self) -> &VectorMaintenanceJobSnapshot {
        &self.snapshot
    }
    pub(crate) fn lease(&self) -> &VectorMaintenanceLeaseToken {
        &self.lease
    }
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceWorker {
    worker_id: String,
    worker_boot_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
}
impl VectorMaintenanceWorker {
    pub(crate) fn try_new(
        worker_id: String,
        worker_boot_id: Uuid,
        execution_identity: VectorMaintenanceExecutionIdentityV1,
    ) -> TableCatalogStoreResult<Self> {
        if worker_id.is_empty() || worker_id.len() > 4096 || worker_id.chars().any(char::is_control) || worker_boot_id.is_nil() {
            return Err(TableCatalogStoreError::Invalid("invalid maintenance worker identity".into()));
        }
        execution_identity.validate()?;
        Ok(Self {
            worker_id,
            worker_boot_id,
            execution_identity,
        })
    }
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceClaimCondition {
    expected_revision: Uuid,
    expected_attempt: u16,
    expected_lease_id: Option<Uuid>,
    expected_resource: VectorMaintenanceResourceSnapshot,
}
impl VectorMaintenanceClaimCondition {
    pub(crate) fn from_snapshot(
        job: &VectorMaintenanceJobSnapshot,
        expected_resource: VectorMaintenanceResourceSnapshot,
    ) -> Self {
        Self {
            expected_revision: *job.job.revision(),
            expected_attempt: *job.job.attempt(),
            expected_lease_id: job.job.lease().as_ref().map(|lease| *lease.lease_id()),
            expected_resource,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMaintenanceNotEligible {
    Terminal,
    NotDue,
    Owned,
    RecoveryRequired,
    ResourceChanged,
    MaintenanceEnrollmentRequired,
    IdentityDenied,
}
#[derive(Debug, Clone)]
pub(crate) enum VectorMaintenanceClaimOutcome {
    Acquired(Box<VectorMaintenanceWork>),
    Replay(Box<VectorMaintenanceJobSnapshot>),
    NotEligible(VectorMaintenanceNotEligible),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMaintenanceResourceState {
    Pending,
    Active,
    Deleting,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorMaintenanceResourceSnapshot {
    resource: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    state: VectorMaintenanceResourceState,
    head: Option<VectorMaintenanceHeadV1>,
    source_intent: Option<CatalogMetadataRef>,
    maintenance: Option<VectorMaintenanceBindingV1>,
    observed_object: CatalogMetadataRef,
}
impl VectorMaintenanceResourceSnapshot {
    pub(crate) fn resource(&self) -> &CatalogResourceIdentity {
        &self.resource
    }
    pub(crate) fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub(crate) fn name(&self) -> &CatalogName {
        &self.name
    }
    pub(crate) fn state(&self) -> VectorMaintenanceResourceState {
        self.state
    }
    pub(crate) fn head(&self) -> Option<&VectorMaintenanceHeadV1> {
        self.head.as_ref()
    }
    pub(crate) fn source_intent(&self) -> Option<&CatalogMetadataRef> {
        self.source_intent.as_ref()
    }
    pub(crate) fn maintenance(&self) -> Option<&VectorMaintenanceBindingV1> {
        self.maintenance.as_ref()
    }
    pub(crate) fn observed_object(&self) -> &CatalogMetadataRef {
        &self.observed_object
    }
}
pub(crate) enum VectorMaintenanceExecutionOutcome {
    NoWork {
        observed: Box<VectorMaintenanceResourceSnapshot>,
        reason: VectorMaintenanceNoWorkReason,
    },
    Completed {
        result: Box<VectorMaintenanceResultV1>,
    },
    Unknown {
        reason: VectorMaintenanceFailureCode,
    },
}
#[derive(Clone)]
pub(crate) struct VectorMaintenanceOwnedCompletion {
    resource: CatalogResourceIdentity,
    job_id: Uuid,
    attempt: u16,
    lease_id: Uuid,
    worker_boot_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    completed_operation_ids: Vec<Uuid>,
    unpublished_commit_refs: Vec<CatalogMetadataRef>,
    no_delete_dispatch: bool,
}
pub(crate) struct PreparedVectorMaintenanceRecovery {
    original_job_ref: CatalogMetadataRef,
    recorded_by: VectorMaintenanceLeaseToken,
    stages: Vec<VectorMaintenanceStageResultV1>,
    owned_completion: Option<VectorMaintenanceOwnedCompletion>,
    absent_candidates: Vec<VectorMaintenanceAbsentStageCandidate>,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceAbsentStageCandidate {
    stage: String,
    object_key: CatalogObjectKey,
}
#[cfg(test)]
impl VectorMaintenanceAbsentStageCandidate {
    pub(crate) fn try_new(stage: String, object_key: CatalogObjectKey) -> TableCatalogStoreResult<Self> {
        if !matches!(stage.as_str(), "ann-build" | "reserve-fragments" | "rewrite") {
            return Err(maintenance_error("unknown absent candidate stage"));
        }
        Ok(Self { stage, object_key })
    }
}
#[derive(Clone)]
pub(crate) struct VectorMaintenanceDispatchGuard {
    inner: Arc<VectorMaintenanceDispatchInner>,
}
struct VectorMaintenanceDispatchInner {
    owner: Arc<()>,
    lease: VectorMaintenanceLeaseToken,
    lifecycle_owner: Arc<BucketIncarnationFenceGuard>,
    dispatch_owner: Arc<NamespaceLockGuard>,
    prior_dispatch_owners: Vec<WriteCommitGuard>,
    prior_dispatch_jobs: BTreeSet<Uuid>,
}
impl VectorMaintenanceDispatchGuard {
    pub(crate) fn lease(&self) -> &VectorMaintenanceLeaseToken {
        &self.inner.lease
    }
    pub(crate) fn lifecycle_owner(&self) -> Arc<BucketIncarnationFenceGuard> {
        self.inner.lifecycle_owner.clone()
    }
    pub(crate) fn dispatch_owner(&self) -> Arc<NamespaceLockGuard> {
        self.inner.dispatch_owner.clone()
    }
    pub(crate) fn retained_owner(&self) -> Arc<dyn Send + Sync> {
        self.inner.clone()
    }
    pub(crate) fn retained_loss_signals(&self) -> Vec<Arc<LockLostSignal>> {
        self.inner
            .prior_dispatch_owners
            .iter()
            .filter_map(WriteCommitGuard::lock_lost_signal)
            .collect()
    }
    pub(crate) fn check_held(&self) -> TableCatalogStoreResult<()> {
        if self.inner.lifecycle_owner.is_lock_lost()
            || self.inner.dispatch_owner.lock_lost_signal().is_some_and(|s| s.is_lost())
            || self.inner.prior_dispatch_owners.iter().any(WriteCommitGuard::is_lock_lost)
        {
            return Err(TableCatalogStoreError::Unavailable(
                "Vector maintenance dispatch protection was lost".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceBucketCursor {
    start_after: Option<String>,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceBucketPage {
    items: Vec<BucketHandle>,
    next: Option<VectorMaintenanceBucketCursor>,
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum VectorMaintenanceResourceDiscoveryPhase {
    Entries,
    CreateIntents,
    DeleteIntents,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceResourceCursor {
    bucket: BucketHandle,
    phase: VectorMaintenanceResourceDiscoveryPhase,
    continuation: Option<String>,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceResourcePage {
    items: Vec<VectorMaintenanceResourceSnapshot>,
    next: Option<VectorMaintenanceResourceCursor>,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceJobCursor {
    resource: CatalogResourceIdentity,
    continuation: Option<String>,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceJobPage {
    items: Vec<VectorMaintenanceJobSnapshot>,
    next: Option<VectorMaintenanceJobCursor>,
}
macro_rules! page_getters {
    ($page:ident, $item:ty, $cursor:ty) => {
        impl $page {
            pub(crate) fn into_parts(self) -> (Vec<$item>, Option<$cursor>) {
                (self.items, self.next)
            }
        }
    };
}
page_getters!(VectorMaintenanceBucketPage, BucketHandle, VectorMaintenanceBucketCursor);
page_getters!(
    VectorMaintenanceResourcePage,
    VectorMaintenanceResourceSnapshot,
    VectorMaintenanceResourceCursor
);
page_getters!(VectorMaintenanceJobPage, VectorMaintenanceJobSnapshot, VectorMaintenanceJobCursor);

impl TableCatalogObjectPaths {
    pub(in crate::table_catalog) fn vector_maintenance_prefix(
        &self,
        resource: &CatalogResourceIdentity,
    ) -> TableCatalogStoreResult<String> {
        if self.scope != CatalogScope::Vector || resource.format() != CatalogFormat::Lance {
            return Err(TableCatalogStoreError::Invalid("maintenance requires Vector scope".into()));
        }
        Ok(format!(
            "{}resources/{}/maintenance/v1/",
            self.table_bucket_root_prefix(resource.storage_bucket_id()),
            table_catalog_path_hash(resource.table_id().as_str())
        ))
    }
    pub(in crate::table_catalog) fn vector_maintenance_job_path(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<String> {
        if job_id.is_nil() {
            return Err(TableCatalogStoreError::Invalid("nil maintenance job ID".into()));
        }
        Ok(format!(
            "{}jobs/{}.json",
            self.vector_maintenance_prefix(resource)?,
            table_catalog_path_hash(&job_id.to_string())
        ))
    }
    pub(in crate::table_catalog) fn vector_maintenance_dispatch_path(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<String> {
        Ok(format!(
            "{}jobs/{}.dispatch.lock",
            self.vector_maintenance_prefix(resource)?,
            table_catalog_path_hash(&job_id.to_string())
        ))
    }
}

impl VectorMaintenanceHeadV1 {
    pub(crate) fn from_entry(entry: &VectorCatalogEntry) -> TableCatalogStoreResult<Self> {
        Self::from_catalog_entry(&entry.entry)
    }
    pub(crate) fn from_catalog_entry(entry: &TableEntry) -> TableCatalogStoreResult<Self> {
        CatalogHead::from_vector_entry(entry)?;
        let value = Self {
            entry_version: entry.version,
            state: entry.state.clone(),
            version_token: entry.version_token.clone(),
            generation: entry.generation,
            binding: entry
                .vector_head_binding
                .clone()
                .ok_or_else(|| TableCatalogStoreError::Invalid("missing Vector head binding".into()))?,
            maintenance: entry.vector_maintenance_binding.clone(),
        };
        value.validate()?;
        Ok(value)
    }
}
fn maintenance_error(message: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Invalid(message.into())
}
fn maintenance_digest(value: &impl Serialize) -> TableCatalogStoreResult<String> {
    Ok(hex_simd::encode_to_string(
        Sha256::digest(maintenance_canonical_bytes(value)?),
        hex_simd::AsciiCase::Lower,
    ))
}
fn maintenance_stamp(
    job: &mut VectorMaintenanceJobV1,
    operation: &str,
    expected_revision: Option<Uuid>,
    payload: serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let input = serde_json::json!({"operation":operation,"resource":job.resource,"record_id":job.job_id,"expected_revision":expected_revision,"lease_id":job.lease.as_ref().map(|l| l.lease_id),"attempt":job.attempt,"payload":payload});
    let digest: [u8; 32] = Sha256::digest(maintenance_canonical_bytes(&input)?).into();
    job.last_update_id = maintenance_uuid_from_digest(digest);
    job.last_update_sha256 = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
    job.revision = Uuid::new_v4();
    Ok(())
}
fn resource_hash_value(resource: &CatalogResourceIdentity) -> TableCatalogStoreResult<serde_json::Value> {
    let bucket = resource
        .vector_bucket()
        .ok_or_else(|| maintenance_error("maintenance requires Vector resource"))?;
    Ok(
        serde_json::json!({"bucket":VectorBucketBindingV1::from_handle(bucket)?,"table_id":resource.table_id().as_str(),"format":"lance"}),
    )
}
pub(crate) fn vector_maintenance_enroll_commit_id(
    resource: &CatalogResourceIdentity,
    expected: &VectorMaintenanceHeadV1,
) -> TableCatalogStoreResult<CatalogCommitId> {
    let bytes =
        maintenance_canonical_bytes(&serde_json::json!({"resource":resource_hash_value(resource)?,"expected_head":expected}))?;
    CatalogCommitId::try_new(maintenance_uuid("rustfs-vector-maintenance-enroll-v1", &bytes).to_string())
}
pub(crate) fn vector_maintenance_enroll_deleted_update_id(
    resource: &CatalogResourceIdentity,
    expected_intent: &CatalogMetadataRef,
) -> TableCatalogStoreResult<Uuid> {
    let bytes = maintenance_canonical_bytes(
        &serde_json::json!({"resource":resource_hash_value(resource)?,"expected_intent":CatalogMetadataRefRecordV1::from(expected_intent)}),
    )?;
    Ok(maintenance_uuid("rustfs-vector-maintenance-enroll-deleted-v1", &bytes))
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    fn maintenance_bucket(&self, resource: &CatalogResourceIdentity) -> TableCatalogStoreResult<&BucketHandle> {
        self.paths.vector_maintenance_prefix(resource)?;
        self.vector_bucket
            .as_ref()
            .filter(|bucket| resource.vector_bucket() == Some(*bucket))
            .ok_or_else(|| {
                TableCatalogStoreError::Conflict("maintenance resource differs from the installed Vector bucket".into())
            })
    }
    async fn maintenance_read_ref(&self, reference: &CatalogMetadataRef) -> TableCatalogStoreResult<TableCatalogObject> {
        let limit = usize::try_from(reference.content_length())
            .ok()
            .filter(|n| *n <= VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .ok_or_else(|| maintenance_error("maintenance reference exceeds byte limit"))?;
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), reference.object_key().as_str(), limit)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("exact maintenance reference".into()))?;
        reference.verify_read(&object)?;
        Ok(object)
    }
    async fn maintenance_read_job(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
        unlocked: bool,
    ) -> TableCatalogStoreResult<Option<VectorMaintenanceJobSnapshot>> {
        let path = self.paths.vector_maintenance_job_path(resource, job_id)?;
        let object = if unlocked {
            self.backend
                .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_JOB_MAX_BYTES)
                .await?
        } else {
            self.backend
                .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_JOB_MAX_BYTES)
                .await?
        };
        let Some(object) = object else {
            return Ok(None);
        };
        let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
        if job.resource().identity()? != *resource || *job.job_id() != job_id {
            return Err(TableCatalogStoreError::Conflict("maintenance job path or resource mismatch".into()));
        }
        let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        let etag = object
            .etag
            .ok_or_else(|| maintenance_error("maintenance job requires an ETag"))?;
        Ok(Some(VectorMaintenanceJobSnapshot {
            job,
            etag,
            observed_object,
        }))
    }
    pub(crate) async fn read_vector_maintenance_job(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<Option<VectorMaintenanceJobSnapshot>> {
        self.backend
            .revalidate_bucket_handle(self.maintenance_bucket(resource)?)
            .await?;
        self.maintenance_read_job(resource, job_id, false).await
    }
    pub(crate) async fn read_vector_maintenance_resource(
        &self,
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceSnapshot> {
        let bucket = self.maintenance_bucket(resource)?;
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let result = self.maintenance_resource_under_protection(resource, namespace, name).await?;
        if lifecycle.is_lock_lost() || gc.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("maintenance resource read lost protection".into()));
        }
        Ok(result)
    }
    async fn maintenance_resource_under_protection(
        &self,
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceSnapshot> {
        self.backend
            .revalidate_bucket_handle(self.maintenance_bucket(resource)?)
            .await?;
        if namespace.public_name() != "indexes" {
            return Err(maintenance_error("maintenance namespace must be indexes"));
        }
        let path = self.paths.table_entry_path(resource.storage_bucket_id(), namespace, name);
        if let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        {
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|e| maintenance_error(&format!("decode maintenance resource: {e}")))?;
            if entry.table_id == resource.table_id().as_str() {
                let head = CatalogHead::from_vector_entry(&entry)?;
                if head.resource_identity() != resource
                    || entry.namespace != namespace.public_name()
                    || entry.table != name.as_str()
                {
                    return Err(TableCatalogStoreError::Conflict("maintenance resource binding changed".into()));
                }
                if entry.state == TableCatalogEntryState::Active {
                    let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
                    return Ok(VectorMaintenanceResourceSnapshot {
                        resource: resource.clone(),
                        namespace: namespace.clone(),
                        name: name.clone(),
                        state: VectorMaintenanceResourceState::Active,
                        head: Some(VectorMaintenanceHeadV1::from_catalog_entry(&entry)?),
                        source_intent: None,
                        maintenance: entry.vector_maintenance_binding,
                        observed_object,
                    });
                }
            }
        }
        let path = self
            .paths
            .vector_delete_intent_path(resource.storage_bucket_id(), resource.table_id().as_str());
        if let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        {
            let intent: VectorIndexDeleteIntent =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&format!("decode delete intent: {e}")))?;
            intent.validate()?;
            if intent.bucket.to_handle()? != *self.maintenance_bucket(resource)?
                || intent.table_id != resource.table_id().as_str()
                || intent.namespace != namespace.public_name()
                || intent.name != name.as_str()
            {
                return Err(TableCatalogStoreError::Conflict("maintenance delete intent identity changed".into()));
            }
            let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
            let state = if intent.phase == VectorIndexDeletePhase::Prepared {
                VectorMaintenanceResourceState::Deleting
            } else {
                VectorMaintenanceResourceState::Deleted
            };
            return Ok(VectorMaintenanceResourceSnapshot {
                resource: resource.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                state,
                head: Some(VectorMaintenanceHeadV1::from_catalog_entry(&intent.deleted_entry)?),
                source_intent: Some(observed_object.clone()),
                maintenance: intent.maintenance_binding().cloned(),
                observed_object,
            });
        }
        let path = self
            .paths
            .vector_create_intent_path(resource.storage_bucket_id(), namespace, name);
        if let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        {
            let intent: VectorIndexCreateIntentV1 =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&format!("decode create intent: {e}")))?;
            intent.validate()?;
            if intent.bucket.to_handle()? != *self.maintenance_bucket(resource)?
                || intent.table_id != resource.table_id().as_str()
                || intent.namespace != namespace.public_name()
                || intent.name != name.as_str()
            {
                return Err(TableCatalogStoreError::Conflict("maintenance create intent identity changed".into()));
            }
            let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
            return Ok(VectorMaintenanceResourceSnapshot {
                resource: resource.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                state: VectorMaintenanceResourceState::Pending,
                head: None,
                source_intent: Some(observed_object.clone()),
                maintenance: None,
                observed_object,
            });
        }
        Err(TableCatalogStoreError::NotFound("maintenance resource identity".into()))
    }
    async fn maintenance_verify_inputs(
        &self,
        spec: &VectorMaintenanceSemanticSpec,
        observed: &VectorMaintenanceResourceSnapshot,
    ) -> TableCatalogStoreResult<()> {
        if spec.resource.identity()? != observed.resource {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance resource differs from observed identity".into(),
            ));
        }
        if let Some(expected) = spec.input.base_head()
            && observed.head.as_ref() != Some(expected)
        {
            return Err(TableCatalogStoreError::Conflict("maintenance input head changed".into()));
        }
        if let Some(source) = spec.input.source() {
            let reference = CatalogMetadataRef::try_from(source.reference())?;
            let prefix = format!("{}sources/", self.paths.vector_maintenance_prefix(&observed.resource)?);
            let authorization_prefix = format!("{}authorizations/", self.paths.vector_maintenance_prefix(&observed.resource)?);
            if !reference.object_key().as_str().starts_with(&prefix)
                && !(*source.kind() == VectorMaintenanceSourceKind::RetirementAuthorization
                    && reference.object_key().as_str().starts_with(&authorization_prefix))
            {
                return Err(maintenance_error("maintenance source must be an immutable resource-local copy"));
            }
            let object = self.maintenance_read_ref(&reference).await?;
            match source.kind() {
                VectorMaintenanceSourceKind::CreateIntent => {
                    let intent: VectorIndexCreateIntentV1 =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    intent.validate()?;
                    if intent.bucket != spec.resource.bucket || intent.table_id != spec.resource.table_id {
                        return Err(TableCatalogStoreError::Conflict("create source identity mismatch".into()));
                    }
                }
                VectorMaintenanceSourceKind::DeleteIntent => {
                    let intent: VectorIndexDeleteIntent =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    intent.validate()?;
                    if intent.bucket != spec.resource.bucket || intent.table_id != spec.resource.table_id {
                        return Err(TableCatalogStoreError::Conflict("delete source identity mismatch".into()));
                    }
                }
                VectorMaintenanceSourceKind::MaintenanceJob => {
                    let source_job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
                    if source_job.resource() != &spec.resource {
                        return Err(TableCatalogStoreError::Conflict("recovery job source identity mismatch".into()));
                    }
                }
                VectorMaintenanceSourceKind::RetirementAuthorization => {
                    let authorization = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
                    if authorization.resource() != &spec.resource {
                        return Err(TableCatalogStoreError::Conflict("retirement source identity mismatch".into()));
                    }
                }
                VectorMaintenanceSourceKind::CommitLog => {
                    let log: CommitLogEntry =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    self.paths.validate_commit_entry(
                        &log,
                        observed.resource.storage_bucket_id(),
                        observed.resource.table_id().as_str(),
                    )?;
                }
            }
        }
        if let Some(reference) = spec.input.plan_ref() {
            let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
            let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&object.data)?;
            if plan.resource() != &spec.resource {
                return Err(TableCatalogStoreError::Conflict("maintenance plan identity mismatch".into()));
            }
        }
        Ok(())
    }
    pub(crate) async fn create_vector_maintenance_job(
        &self,
        spec: VectorMaintenanceJobSpec,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        let resource = spec.resource.identity()?;
        let bucket = self.maintenance_bucket(&resource)?;
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_bucket_catalog_admission(bucket, &lifecycle).await?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let namespace = Namespace::parse(&spec.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&spec.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let observed = self
            .maintenance_resource_under_protection(&resource, &namespace, &name)
            .await?;
        let now = maintenance_now();
        let mut job = VectorMaintenanceJobV1 {
            version: 1,
            job_id: spec.job_id,
            resource: spec.resource.clone(),
            kind: spec.kind,
            origin: spec.origin,
            request_id: spec.request_id,
            spec_sha256: maintenance_digest(&spec)?,
            revision: Uuid::new_v4(),
            last_update_id: Uuid::new_v4(),
            last_update_sha256: "0".repeat(64),
            state: VectorMaintenanceJobState::Queued,
            input: spec.input.clone(),
            execution_identity: spec.execution_identity.clone(),
            initiator_ref: spec.initiator_ref.clone(),
            policy: spec.policy.clone(),
            attempt: 0,
            lease: None,
            created_at: now.clone(),
            updated_at: now,
            next_retry_at: None,
            finished_at: None,
            failure: None,
            result_ref: None,
        };
        self.maintenance_verify_inputs(&job.semantic_spec(), &observed).await?;
        maintenance_stamp(
            &mut job,
            "CREATE_JOB",
            None,
            serde_json::to_value(&spec).map_err(|e| maintenance_error(&e.to_string()))?,
        )?;
        let bytes = encode_vector_maintenance_record(&job)?;
        let path = self.paths.vector_maintenance_job_path(&resource, spec.job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let fence = TableCatalogObjectMutationFence::from_signals([
            lifecycle.lock_lost_signal(),
            gc.lock_lost_signal(),
            lock.lock_lost_signal(),
        ]);
        if let Some(existing) = self.maintenance_read_job(&resource, spec.job_id, true).await? {
            if existing.job.spec() != spec {
                return Err(TableCatalogStoreError::Conflict("maintenance job ID already binds another spec".into()));
            }
            fence.ensure_held()?;
            return Ok(existing);
        }
        let outcome = self
            .backend
            .put_object_unlocked_fenced(
                self.catalog_bucket(),
                &path,
                bytes,
                TableCatalogPutPrecondition::IfAbsent,
                lock.write_commit_guards(),
                &fence,
            )
            .await;
        let stored = self.maintenance_read_job(&resource, spec.job_id, true).await?;
        if let Some(stored) = stored
            && stored.job.spec() == spec
        {
            fence.ensure_held()?;
            return Ok(stored);
        }
        outcome?;
        Err(TableCatalogStoreError::Unavailable(
            "maintenance job creation has no matching receipt".into(),
        ))
    }
}

#[cfg(test)]
#[path = "vector_maintenance_tests.rs"]
mod tests;

fn claim_input(
    condition: &VectorMaintenanceClaimCondition,
    worker: &VectorMaintenanceWorker,
    lease_id: Uuid,
) -> TableCatalogStoreResult<serde_json::Value> {
    let resource = &condition.expected_resource;
    Ok(serde_json::json!({
        "expected_revision":condition.expected_revision,"expected_attempt":condition.expected_attempt,"expected_lease_id":condition.expected_lease_id,
        "resource":resource_hash_value(&resource.resource)?,"namespace":resource.namespace.public_name(),"name":resource.name.as_str(),
        "head":resource.head,"source_intent":resource.source_intent.as_ref().map(CatalogMetadataRefRecordV1::from),"maintenance":resource.maintenance,
        "observed_object":CatalogMetadataRefRecordV1::from(&resource.observed_object),
        "state":match resource.state {VectorMaintenanceResourceState::Pending=>"PENDING",VectorMaintenanceResourceState::Active=>"ACTIVE",VectorMaintenanceResourceState::Deleting=>"DELETING",VectorMaintenanceResourceState::Deleted=>"DELETED"},
        "worker_id":worker.worker_id,"worker_boot_id":worker.worker_boot_id,"execution_identity":worker.execution_identity,"lease_id":lease_id
    }))
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn maintenance_write_job(
        &self,
        current: &VectorMaintenanceJobSnapshot,
        next: &VectorMaintenanceJobV1,
        write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        let resource = next.resource.identity()?;
        if current.job.spec() != next.spec() {
            return Err(TableCatalogStoreError::Conflict("maintenance update changed immutable spec".into()));
        }
        let path = self.paths.vector_maintenance_job_path(&resource, next.job_id)?;
        let bytes = encode_vector_maintenance_record(next)?;
        let outcome = self
            .backend
            .put_object_unlocked_fenced(
                self.catalog_bucket(),
                &path,
                bytes,
                TableCatalogPutPrecondition::IfMatch(current.etag.clone()),
                write_guards,
                fence,
            )
            .await;
        let read = self.maintenance_read_job(&resource, next.job_id, true).await?;
        if let Some(read) = read
            && read.job == *next
        {
            fence.ensure_held()?;
            return Ok(read);
        }
        outcome?;
        Err(TableCatalogStoreError::Unavailable(
            "maintenance conditional update has no exact receipt".into(),
        ))
    }
    pub(crate) async fn claim_vector_maintenance_job(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
        condition: VectorMaintenanceClaimCondition,
        worker: VectorMaintenanceWorker,
        lease_id: Uuid,
    ) -> TableCatalogStoreResult<VectorMaintenanceClaimOutcome> {
        if lease_id.is_nil() || condition.expected_resource.resource != *resource {
            return Err(maintenance_error("invalid maintenance claim identity"));
        }
        worker.execution_identity.validate()?;
        let bucket = self.maintenance_bucket(resource)?;
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_bucket_catalog_admission(bucket, &lifecycle).await?;
        let dispatch = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &self.paths.vector_maintenance_dispatch_path(resource, job_id)?)
            .await?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let observed = self
            .maintenance_resource_under_protection(
                resource,
                &condition.expected_resource.namespace,
                &condition.expected_resource.name,
            )
            .await?;
        let path = self.paths.vector_maintenance_job_path(resource, job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let fence = TableCatalogObjectMutationFence::from_signals([
            lifecycle.lock_lost_signal(),
            dispatch.lock_lost_signal(),
            gc.lock_lost_signal(),
            lock.lock_lost_signal(),
        ]);
        let current = self
            .maintenance_read_job(resource, job_id, true)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance job".into()))?;
        let job = &current.job;
        if job.execution_identity() != &worker.execution_identity {
            return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::IdentityDenied));
        }
        let payload = claim_input(&condition, &worker, lease_id)?;
        let claim_digest = maintenance_digest(&payload)?;
        let now = OffsetDateTime::now_utc();
        use VectorMaintenanceJobState as S;
        if job.state == S::Running {
            let lease = job
                .lease
                .as_ref()
                .ok_or_else(|| maintenance_error("running job lacks lease"))?;
            if vector_maintenance_timestamp(&lease.expires_at)? <= now {
                return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::RecoveryRequired));
            }
            if lease.lease_id == lease_id
                && lease.worker_id == worker.worker_id
                && lease.worker_boot_id == worker.worker_boot_id
                && lease.claim_digest_sha256 == claim_digest
            {
                fence.ensure_held()?;
                return Ok(VectorMaintenanceClaimOutcome::Replay(Box::new(current)));
            }
            return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::Owned));
        }
        match job.state {
            S::RecoveryRequired => {
                return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::RecoveryRequired));
            }
            S::Succeeded | S::Failed | S::ManualReview | S::PartialRecovered => {
                return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::Terminal));
            }
            S::RetryScheduled
                if job
                    .next_retry_at
                    .as_ref()
                    .is_some_and(|time| vector_maintenance_timestamp(time).is_ok_and(|time| time > now)) =>
            {
                return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::NotDue));
            }
            _ => {}
        }
        if job.revision != condition.expected_revision
            || job.attempt != condition.expected_attempt
            || job.lease.as_ref().map(|l| l.lease_id) != condition.expected_lease_id
        {
            return Err(TableCatalogStoreError::Conflict("maintenance claim revision changed".into()));
        }
        if observed != condition.expected_resource {
            return Ok(VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::ResourceChanged));
        }
        if matches!(
            job.kind,
            VectorMaintenanceKind::AnnBuild
                | VectorMaintenanceKind::Compaction
                | VectorMaintenanceKind::HistoryGc
                | VectorMaintenanceKind::OrphanGc
        ) && observed.maintenance.is_none()
        {
            return Ok(VectorMaintenanceClaimOutcome::NotEligible(
                VectorMaintenanceNotEligible::MaintenanceEnrollmentRequired,
            ));
        }
        self.maintenance_verify_inputs(&job.semantic_spec(), &observed).await?;
        let mut next = job.clone();
        next.attempt = next
            .attempt
            .checked_add(1)
            .filter(|value| *value <= next.policy.max_attempts)
            .ok_or_else(|| TableCatalogStoreError::Conflict("maintenance attempts exhausted".into()))?;
        let time = maintenance_format_time(now);
        next.state = S::Running;
        next.updated_at = time.clone();
        next.next_retry_at = None;
        next.failure = None;
        next.finished_at = None;
        next.result_ref = None;
        next.lease = Some(VectorMaintenanceLeaseV1 {
            version: 1,
            lease_id,
            worker_id: worker.worker_id,
            worker_boot_id: worker.worker_boot_id,
            claim_digest_sha256: claim_digest,
            claimed_at: time.clone(),
            heartbeat_at: time,
            expires_at: maintenance_format_time(now + Duration::seconds(next.policy.lease_timeout_seconds as i64)),
        });
        maintenance_stamp(&mut next, "CLAIM", Some(current.job.revision), payload)?;
        let snapshot = self
            .maintenance_write_job(&current, &next, lock.write_commit_guards(), &fence)
            .await?;
        let lease = VectorMaintenanceLeaseToken::from_job(&snapshot.job)?;
        Ok(VectorMaintenanceClaimOutcome::Acquired(Box::new(VectorMaintenanceWork {
            snapshot,
            lease,
        })))
    }
    fn maintenance_dispatch_matches(&self, guard: &VectorMaintenanceDispatchGuard) -> TableCatalogStoreResult<()> {
        if !Arc::ptr_eq(&self.vector_owner, &guard.inner.owner) {
            return Err(TableCatalogStoreError::Conflict("maintenance dispatch belongs to another store".into()));
        }
        self.maintenance_bucket(guard.lease().resource())?;
        guard.check_held()
    }
    pub(crate) async fn begin_vector_maintenance_dispatch(
        &self,
        lease: &VectorMaintenanceLeaseToken,
    ) -> TableCatalogStoreResult<VectorMaintenanceDispatchGuard> {
        let bucket = self.maintenance_bucket(&lease.resource)?;
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_bucket_catalog_admission(bucket, &lifecycle).await?;
        let lifecycle_owner = lifecycle.lifecycle_guard.clone().ok_or_else(|| {
            TableCatalogStoreError::Unsupported("maintenance dispatch requires a native lifecycle owner".into())
        })?;
        let job = self
            .maintenance_read_job(&lease.resource, lease.job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance job".into()))?;
        if !lease.matches(&job.job)? {
            return Err(TableCatalogStoreError::Conflict("maintenance dispatch lease changed".into()));
        }
        let prior_dispatch_jobs = self.maintenance_prior_dispatch_jobs(&job.job).await?;
        let mut ordered = prior_dispatch_jobs.clone();
        ordered.insert(lease.job_id);
        let mut prior_dispatch_owners = Vec::new();
        let mut dispatch_owner = None;
        for id in ordered {
            let path = self.paths.vector_maintenance_dispatch_path(&lease.resource, id)?;
            if id == lease.job_id {
                let dispatch = self.backend.acquire_read_lock(self.catalog_bucket(), &path).await?;
                dispatch_owner = Some(dispatch.namespace_read_guard.ok_or_else(|| {
                    TableCatalogStoreError::Unsupported("maintenance dispatch requires a native namespace owner".into())
                })?);
            } else {
                let dispatch = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
                let owners = dispatch.write_commit_guards();
                if owners.is_empty() {
                    return Err(TableCatalogStoreError::Unsupported(
                        "maintenance recovery requires native prior dispatch owners".into(),
                    ));
                }
                prior_dispatch_owners.extend(owners);
            }
        }
        if self.maintenance_prior_dispatch_jobs(&job.job).await? != prior_dispatch_jobs {
            return Err(TableCatalogStoreError::Conflict("maintenance recovery dispatch set changed".into()));
        }
        let dispatch_owner = dispatch_owner.ok_or_else(|| maintenance_error("maintenance dispatch owner missing"))?;
        let guard = VectorMaintenanceDispatchGuard {
            inner: Arc::new(VectorMaintenanceDispatchInner {
                owner: self.vector_owner.clone(),
                lease: lease.clone(),
                lifecycle_owner,
                dispatch_owner,
                prior_dispatch_owners,
                prior_dispatch_jobs,
            }),
        };
        let snapshot = self.read_vector_maintenance_job_with_guard(&guard).await?;
        if snapshot.job.state != VectorMaintenanceJobState::Running
            || snapshot
                .job
                .lease
                .as_ref()
                .is_none_or(|l| !vector_maintenance_timestamp(&l.expires_at).is_ok_and(|time| time > OffsetDateTime::now_utc()))
        {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance dispatch lease is no longer eligible".into(),
            ));
        }
        guard.check_held()?;
        Ok(guard)
    }
    pub(crate) async fn read_vector_maintenance_job_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        self.backend
            .revalidate_bucket_handle(self.maintenance_bucket(&guard.lease().resource)?)
            .await?;
        let snapshot = self
            .maintenance_read_job(&guard.lease().resource, guard.lease().job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance job".into()))?;
        if !guard.lease().matches(&snapshot.job)? {
            return Err(TableCatalogStoreError::Conflict("maintenance dispatch claim changed".into()));
        }
        guard.check_held()?;
        Ok(snapshot)
    }
    pub(crate) async fn heartbeat_vector_maintenance_job(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        expected_revision: Uuid,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        let lease = guard.lease();
        let path = self.paths.vector_maintenance_job_path(&lease.resource, lease.job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let fence = TableCatalogObjectMutationFence::from_signals(
            [
                guard.inner.lifecycle_owner.lock_lost_signal(),
                guard.inner.dispatch_owner.lock_lost_signal(),
                lock.lock_lost_signal(),
            ]
            .into_iter()
            .chain(guard.retained_loss_signals().into_iter().map(Some)),
        );
        let current = self
            .maintenance_read_job(&lease.resource, lease.job_id, true)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance job".into()))?;
        if !lease.matches(&current.job)? || current.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("maintenance heartbeat claim or state changed".into()));
        }
        if current.job.revision != expected_revision {
            let mut replay = current.job.clone();
            maintenance_stamp(&mut replay, "HEARTBEAT", Some(expected_revision), serde_json::Value::Null)?;
            if replay.last_update_id == current.job.last_update_id && replay.last_update_sha256 == current.job.last_update_sha256
            {
                return Ok(current);
            }
            return Err(TableCatalogStoreError::Conflict("maintenance heartbeat revision changed".into()));
        }
        let previous = current
            .job
            .lease
            .as_ref()
            .ok_or_else(|| maintenance_error("heartbeat requires lease"))?;
        let now = OffsetDateTime::now_utc();
        if now < vector_maintenance_timestamp(&previous.heartbeat_at)?
            || now >= vector_maintenance_timestamp(&previous.expires_at)?
        {
            return Err(TableCatalogStoreError::Unavailable(
                "maintenance heartbeat clock or lease is invalid".into(),
            ));
        }
        let mut next = current.job.clone();
        next.updated_at = maintenance_format_time(now);
        if let Some(lease) = next.lease.as_mut() {
            lease.heartbeat_at = next.updated_at.clone();
            lease.expires_at = maintenance_format_time(now + Duration::seconds(next.policy.lease_timeout_seconds as i64));
        }
        maintenance_stamp(&mut next, "HEARTBEAT", Some(expected_revision), serde_json::Value::Null)?;
        self.maintenance_write_job(&current, &next, lock.write_commit_guards(), &fence)
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogManagedObjectIdentity {
    object_key: CatalogObjectKey,
    body_revision: CatalogBodyRevision,
}
impl CatalogManagedObjectIdentity {
    pub(crate) fn try_new(object_key: CatalogObjectKey, body_revision: CatalogBodyRevision) -> TableCatalogStoreResult<Self> {
        Ok(Self {
            object_key,
            body_revision,
        })
    }
    pub(crate) fn object_key(&self) -> &CatalogObjectKey {
        &self.object_key
    }
    pub(crate) fn body_revision(&self) -> CatalogBodyRevision {
        self.body_revision
    }
}
#[derive(Debug, Clone)]
pub(crate) struct CatalogRetirementValidation {
    pub(in crate::table_catalog) guard_owner: Arc<()>,
    pub(in crate::table_catalog) resource: CatalogResourceIdentity,
    pub(in crate::table_catalog) maintenance: Option<VectorMaintenanceBindingV1>,
    pub(in crate::table_catalog) metadata: CatalogMetadataRef,
    pub(in crate::table_catalog) references_sha256: CatalogContentSha256,
}
impl PartialEq for CatalogRetirementValidation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.guard_owner, &other.guard_owner)
            && self.resource == other.resource
            && self.maintenance == other.maintenance
            && self.metadata == other.metadata
            && self.references_sha256 == other.references_sha256
    }
}
impl Eq for CatalogRetirementValidation {}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn visit_vector_retirements(
        &self,
        resource: &CatalogResourceIdentity,
        binding: &VectorMaintenanceBindingV1,
        inspect: impl FnMut(&VectorRetirementAuthorizationV1) -> TableCatalogStoreResult<()>,
    ) -> TableCatalogStoreResult<()> {
        self.visit_vector_retirements_with_budget(resource, binding, None, inspect)
            .await
    }
    async fn visit_vector_retirements_with_budget(
        &self,
        resource: &CatalogResourceIdentity,
        binding: &VectorMaintenanceBindingV1,
        budget: Option<&VectorMaintenanceDiscoveryBudget>,
        mut inspect: impl FnMut(&VectorRetirementAuthorizationV1) -> TableCatalogStoreResult<()>,
    ) -> TableCatalogStoreResult<()> {
        binding.validate()?;
        let prefix = self.paths.vector_maintenance_prefix(resource)?;
        let mut next = binding.retirement_tip.clone();
        let mut seen = BTreeSet::new();
        for _ in 0..binding.retirement_count {
            let reference = next
                .take()
                .ok_or_else(|| maintenance_error("retirement chain ended before its declared count"))?;
            if !seen.insert(reference.object_key.clone()) {
                return Err(maintenance_error("cyclic retirement chain"));
            }
            if budget.is_some_and(|budget| !budget.try_start_call()) {
                return Err(TableCatalogStoreError::Unavailable(
                    "CapacityExceeded: incomplete retirement-chain validation budget".into(),
                ));
            }
            let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(&reference)?).await?;
            let authorization = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
            let path = format!(
                "{prefix}authorizations/{}.json",
                table_catalog_path_hash(&authorization.retirement_id.to_string())
            );
            if path != reference.object_key || authorization.resource.identity()? != *resource {
                return Err(TableCatalogStoreError::Conflict("retirement chain identity or path mismatch".into()));
            }
            inspect(&authorization)?;
            next = authorization.previous_ref;
        }
        if next.is_some() {
            return Err(maintenance_error("retirement chain exceeds its declared count"));
        }
        Ok(())
    }
    pub(crate) async fn validate_vector_references_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        metadata: &CatalogMetadataRef,
        exact_refs: &[CatalogManagedObjectIdentity],
    ) -> TableCatalogStoreResult<CatalogRetirementValidation> {
        self.validate_vector_operation_guard(guard)?;
        let current = self.read_vector_maintenance_resource_with_guard(guard).await?;
        let resource = guard.resource_identity();
        let prefix = resource.dataset_prefix();
        let mut identities = BTreeSet::new();
        let mut contains_metadata = false;
        for reference in exact_refs {
            if !reference.object_key.as_str().starts_with(&prefix) {
                return Err(maintenance_error("candidate reference crosses its Dataset scope"));
            }
            let body = reference.body_revision.as_uuid();
            if !identities.insert((reference.object_key.as_str().to_string(), body)) {
                return Err(maintenance_error("candidate closure repeats an exact object identity"));
            }
            contains_metadata |=
                reference.object_key == *metadata.object_key() && Some(reference.body_revision) == metadata.body_revision();
        }
        if !contains_metadata {
            return Err(maintenance_error("candidate closure must contain its exact manifest identity"));
        }
        if let Some(binding) = &current.maintenance {
            self.visit_vector_retirements(resource, binding, |authorization| {
                if authorization
                    .targets
                    .iter()
                    .any(|target| identities.contains(&(target.object_key.clone(), target.body_revision)))
                {
                    return Err(TableCatalogStoreError::Conflict("candidate references an already retired object".into()));
                }
                Ok(())
            })
            .await?;
        }
        let bytes = maintenance_canonical_bytes(&identities)?;
        guard.check_held()?;
        Ok(CatalogRetirementValidation {
            guard_owner: guard.inner.retirement_validation_owner.clone(),
            resource: resource.clone(),
            maintenance: current.maintenance,
            metadata: metadata.clone(),
            references_sha256: CatalogContentSha256::digest(&bytes),
        })
    }
    pub(super) fn validate_prepared_retirement_proof(
        &self,
        guard: &VectorCatalogOperationGuard,
        prepared: &PreparedCatalogCommit,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<()> {
        self.validate_vector_operation_guard(guard)?;
        let proof = prepared
            .retirement_validation()
            .ok_or_else(|| maintenance_error("v4 Dataset publication requires a complete reference proof"))?;
        if !Arc::ptr_eq(&guard.inner.retirement_validation_owner, &proof.guard_owner)
            || proof.resource != *prepared.resource_identity()
            || proof.metadata != *prepared.candidate_metadata()
            || proof.maintenance != entry.vector_maintenance_binding
        {
            return Err(TableCatalogStoreError::Conflict(
                "candidate retirement proof belongs to another guard or head".into(),
            ));
        }
        guard.check_held()
    }
    pub(crate) async fn begin_vector_operation_with_lifecycle(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
        name: &CatalogName,
        lifecycle: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<VectorCatalogOperationGuard> {
        let lifecycle = self.backend.reuse_bucket_lifecycle_read(bucket, lifecycle).await?;
        self.require_vector_parent_and_namespace(bucket, namespace).await?;
        let (entry, _) = self
            .read_table_with_etag(bucket.storage_bucket_id().as_str(), namespace, name)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector Index".into()))?;
        let resource = CatalogHead::from_vector_entry(&entry)?.resource_identity().clone();
        let guard = self
            .retain_vector_resource_guard(resource, namespace.clone(), name.clone(), lifecycle)
            .await?;
        self.read_vector_entry_with_guard(&guard).await?;
        Ok(guard)
    }
    pub(crate) async fn begin_vector_maintenance_enrollment(
        &self,
        expected: &VectorCatalogEntry,
    ) -> TableCatalogStoreResult<Arc<VectorCatalogOperationGuard>> {
        if expected.entry.version != 3 {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance enrollment requires an exact v3 entry".into(),
            ));
        }
        let resource = expected.head().resource_identity().clone();
        let bucket = self.maintenance_bucket(&resource)?;
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_bucket_catalog_admission(bucket, &lifecycle).await?;
        let gc = self
            .backend
            .acquire_write_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal(), gc.lock_lost_signal()]);
        let native_lifecycle = lifecycle.lifecycle_guard.clone();
        let guard = Arc::new(VectorCatalogOperationGuard {
            inner: Arc::new(VectorCatalogOperationState {
                owner: self.vector_owner.clone(),
                retirement_validation_owner: Arc::new(()),
                resource_identity: resource,
                namespace: expected.head().namespace().clone(),
                name: expected.head().name().clone(),
                lifecycle: tokio::sync::Mutex::new(lifecycle),
                native_lifecycle,
                gc: parking_lot::Mutex::new(gc),
                gc_exclusive: true,
                fence,
            }),
        });
        let current = self.read_vector_entry_with_guard(&guard).await?;
        if current.entry != expected.entry {
            return Err(TableCatalogStoreError::Conflict("enrollment expected entry changed".into()));
        }
        Ok(guard)
    }
    #[cfg(test)]
    pub(crate) async fn enroll_vector_maintenance(
        &self,
        expected: &VectorCatalogEntry,
        commit_id: CatalogCommitId,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        let guard = self.begin_vector_maintenance_enrollment(expected).await?;
        self.enroll_vector_maintenance_with_guard(expected, commit_id, &guard).await
    }
    pub(crate) async fn enroll_vector_maintenance_with_guard(
        &self,
        expected: &VectorCatalogEntry,
        commit_id: CatalogCommitId,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        self.validate_vector_operation_guard(guard)?;
        guard.check_held()?;
        let resource = expected.head().resource_identity().clone();
        let namespace = expected.head().namespace().clone();
        let name = expected.head().name().clone();
        let expected_head = VectorMaintenanceHeadV1::from_entry(expected)?;
        if expected.entry.version != 3
            || !guard.inner.gc_exclusive
            || guard.resource_identity() != &resource
            || commit_id != vector_maintenance_enroll_commit_id(&resource, &expected_head)?
        {
            return Err(maintenance_error("invalid enrollment entry, resource or commit ID"));
        }
        let current = self.read_vector_entry_with_guard(guard).await?;
        if current.entry != expected.entry {
            return Err(TableCatalogStoreError::Conflict("enrollment expected entry changed".into()));
        }
        let prepared = PreparedCatalogCommit::try_from_validated_parts(
            CatalogCommitParts {
                resource_identity: resource,
                namespace,
                name,
                expected_head: expected.head().expected_head(),
                commit_id,
                operation: "vector-maintenance-enroll".into(),
                idempotency_key: None,
                writer: None,
            },
            expected.head().metadata().clone(),
            expected.head().format_descriptor().clone(),
            CatalogEntryUpdates::try_new(Some(expected.entry.warehouse_location.clone()), Some(expected.entry.format_version))?,
            Vec::new(),
        )?
        .with_vector_resource_mutation_v2(VectorResourceMutationBindingV2 {
            version: 2,
            kind: VectorResourceMutationKind::Enroll,
            expected_definition: expected.definition.clone(),
            next_definition: expected.definition.clone(),
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Active,
            expected_maintenance: None,
            next_maintenance: VectorMaintenanceBindingV1::empty(),
        })?;
        let store = self.clone();
        let guard = guard.clone();
        object::run_vector_catalog_mutation(async move {
            let publication = TableCommitLockPublication::new(&store.backend, &store.paths);
            let result = store
                .commit_catalog_with_operation_guard(CatalogCommitInput::Prepared(Box::new(prepared)), &publication, Some(&guard))
                .await?;
            guard.check_held()?;
            VectorCatalogEntry::from_entry(result.table)
        })
        .await
    }
}

impl VectorMaintenanceOwnedCompletion {
    /// Construct only after the fixed execution owner has joined every admitted operation.
    pub(crate) fn try_from_completed_operations(
        lease: &VectorMaintenanceLeaseToken,
        completed_operation_ids: Vec<Uuid>,
        unpublished_commit_refs: Vec<CatalogMetadataRef>,
    ) -> TableCatalogStoreResult<Self> {
        if completed_operation_ids.len() > 256
            || unpublished_commit_refs.len() > 256
            || completed_operation_ids.iter().any(Uuid::is_nil)
            || completed_operation_ids.iter().collect::<BTreeSet<_>>().len() != completed_operation_ids.len()
        {
            return Err(maintenance_error("invalid maintenance completion receipts"));
        }
        Ok(Self {
            resource: lease.resource.clone(),
            job_id: lease.job_id,
            attempt: lease.attempt,
            lease_id: lease.lease_id,
            worker_boot_id: lease.worker_boot_id,
            execution_identity: lease.execution_identity.clone(),
            completed_operation_ids,
            unpublished_commit_refs,
            no_delete_dispatch: false,
        })
    }
    fn matches(&self, lease: &VectorMaintenanceLeaseToken) -> bool {
        self.resource == lease.resource
            && self.job_id == lease.job_id
            && self.attempt == lease.attempt
            && self.lease_id == lease.lease_id
            && self.worker_boot_id == lease.worker_boot_id
            && self.execution_identity == lease.execution_identity
    }
}
impl PreparedVectorMaintenanceRecovery {
    #[cfg(test)]
    pub(crate) fn try_from_verified_parts(
        original_job_ref: CatalogMetadataRef,
        recorded_by: VectorMaintenanceLeaseToken,
        stages: Vec<VectorMaintenanceStageResultV1>,
        owned_completion: VectorMaintenanceOwnedCompletion,
    ) -> TableCatalogStoreResult<Self> {
        if stages.len() > 2 || owned_completion.resource != recorded_by.resource {
            return Err(maintenance_error("maintenance recovery resource or stage mismatch"));
        }
        for stage in &stages {
            stage.validate()?;
        }
        Ok(Self {
            original_job_ref,
            recorded_by,
            stages,
            owned_completion: Some(owned_completion),
            absent_candidates: Vec::new(),
        })
    }
    pub(crate) fn try_from_published_stages(
        original_job_ref: CatalogMetadataRef,
        recorded_by: VectorMaintenanceLeaseToken,
        stages: Vec<VectorMaintenanceStageResultV1>,
    ) -> TableCatalogStoreResult<Self> {
        if stages.is_empty()
            || stages.len() > 2
            || stages.iter().any(|stage| {
                !matches!(
                    stage.outcome,
                    VectorMaintenanceStageOutcome::Committed | VectorMaintenanceStageOutcome::Superseded
                )
            })
        {
            return Err(maintenance_error("published recovery requires all exact committed stages"));
        }
        for stage in &stages {
            stage.validate()?;
        }
        Ok(Self {
            original_job_ref,
            recorded_by,
            stages,
            owned_completion: None,
            absent_candidates: Vec::new(),
        })
    }
    #[cfg(test)]
    pub(crate) fn with_absent_candidates(
        mut self,
        candidates: Vec<VectorMaintenanceAbsentStageCandidate>,
    ) -> TableCatalogStoreResult<Self> {
        let expected = self
            .stages
            .iter()
            .filter(|stage| stage.outcome == VectorMaintenanceStageOutcome::NotPrepared)
            .map(|stage| stage.stage.as_str())
            .collect::<BTreeSet<_>>();
        let actual = candidates
            .iter()
            .map(|candidate| candidate.stage.as_str())
            .collect::<BTreeSet<_>>();
        if expected != actual
            || actual.len() != candidates.len()
            || candidates.iter().any(|candidate| {
                !candidate
                    .object_key
                    .as_str()
                    .starts_with(&self.recorded_by.resource.dataset_prefix())
            })
        {
            return Err(maintenance_error("absent candidates differ from exact NotPrepared stages"));
        }
        self.absent_candidates = candidates;
        Ok(self)
    }
}
impl VectorMaintenanceResultV1 {
    pub(crate) fn try_for_work(
        work: &VectorMaintenanceWork,
        effect: VectorMaintenanceResultEffect,
        stages: Vec<VectorMaintenanceStageResultV1>,
        commit_refs: Vec<CatalogMetadataRefRecordV1>,
        retirement_refs: Vec<CatalogMetadataRefRecordV1>,
        output_refs: Vec<CatalogMetadataRefRecordV1>,
        reason: Option<VectorMaintenanceNoWorkReason>,
    ) -> TableCatalogStoreResult<Self> {
        let value = Self {
            version: 1,
            job_id: work.lease.job_id,
            recorded_by_job_id: work.lease.job_id,
            attempt: work.lease.attempt,
            resource: work.snapshot.job.resource.clone(),
            finished_at: maintenance_now(),
            effect,
            stages,
            commit_refs,
            retirement_refs,
            output_refs,
            reason,
        };
        value.validate()?;
        Ok(value)
    }
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn maintenance_write_immutable(
        &self,
        resource: &CatalogResourceIdentity,
        path: String,
        bytes: Vec<u8>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        if bytes.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES
            || !path.starts_with(&self.paths.vector_maintenance_prefix(resource)?)
        {
            return Err(maintenance_error("invalid immutable maintenance object path or size"));
        }
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let mut fence = fence.clone();
        fence.extend_signals([lock.lock_lost_signal()]);
        if let Some(object) = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        {
            if object.data != bytes {
                return Err(TableCatalogStoreError::Conflict(
                    "immutable maintenance object has different bytes".into(),
                ));
            }
            fence.ensure_held()?;
            return CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object);
        }
        let outcome = self
            .backend
            .put_object_unlocked_fenced(
                self.catalog_bucket(),
                &path,
                bytes.clone(),
                TableCatalogPutPrecondition::IfAbsent,
                lock.write_commit_guards(),
                &fence,
            )
            .await;
        if let Some(object) = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            && object.data == bytes
        {
            fence.ensure_held()?;
            return CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object);
        }
        outcome?;
        Err(TableCatalogStoreError::Unavailable(
            "immutable maintenance write has no exact receipt".into(),
        ))
    }
    async fn maintenance_verify_result(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        job: &VectorMaintenanceJobV1,
        result: &VectorMaintenanceResultV1,
    ) -> TableCatalogStoreResult<()> {
        result.validate()?;
        if result.job_id != job.job_id
            || result.recorded_by_job_id != job.job_id
            || result.attempt != job.attempt
            || result.resource != job.resource
        {
            return Err(TableCatalogStoreError::Conflict("maintenance result belongs to another execution".into()));
        }
        if result.effect == VectorMaintenanceResultEffect::PartialRecovered {
            return Err(maintenance_error(
                "partial recovery must update the original task through the recovery method",
            ));
        }
        for reference in result
            .commit_refs
            .iter()
            .chain(&result.retirement_refs)
            .chain(&result.output_refs)
        {
            if !reference
                .object_key
                .starts_with(&self.paths.vector_maintenance_prefix(&guard.lease().resource)?)
            {
                return Err(maintenance_error("maintenance result reference is outside the resource"));
            }
            self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
        }
        if result.effect == VectorMaintenanceResultEffect::Published {
            let expected: &[&str] = match job.kind {
                VectorMaintenanceKind::AnnBuild => &["ann-build"],
                VectorMaintenanceKind::Compaction => &["reserve-fragments", "rewrite"],
                _ => return Err(maintenance_error("only engine tasks publish Dataset results")),
            };
            if result.stages.iter().map(|stage| stage.stage.as_str()).collect::<Vec<_>>() != expected {
                return Err(maintenance_error("publication result does not contain all task stages"));
            }
            let namespace = Namespace::parse(&job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
            let name = CatalogName::parse(&job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
            let entry_path = self
                .paths
                .table_entry_path(guard.lease().resource.storage_bucket_id(), &namespace, &name);
            let object = self
                .backend
                .read_object_limited(self.catalog_bucket(), &entry_path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance publication head".into()))?;
            let entry: TableEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            if CatalogHead::from_vector_entry(&entry)?.resource_identity() != guard.lease().resource() {
                return Err(TableCatalogStoreError::Conflict("maintenance publication resource changed".into()));
            }
            let records = self
                .read_table_commit_logs(&entry)
                .await?
                .into_iter()
                .map(|(_, log)| log)
                .collect::<Vec<_>>();
            let history = TableCommitHistoryIndex::new(&entry, records.iter());
            for stage in &result.stages {
                let reference = stage
                    .commit_ref
                    .as_ref()
                    .ok_or_else(|| maintenance_error("published stage lacks commit reference"))?;
                let log_bytes = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                let log: CommitLogEntry =
                    serde_json::from_slice(&log_bytes.data).map_err(|e| maintenance_error(&e.to_string()))?;
                self.paths.validate_commit_entry(
                    &log,
                    guard.lease().resource.storage_bucket_id(),
                    guard.lease().resource.table_id().as_str(),
                )?;
                if log.commit_id != stage.commit_id
                    || !history.proves_committed(&log)
                    || stage.candidate.as_ref() != log.vector_commit_binding.as_ref().map(|b| &b.candidate_metadata)
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "publication stage is not proven by complete Catalog history".into(),
                    ));
                }
            }
        }
        self.maintenance_verify_control_result(guard, job, result).await?;
        guard.check_held()
    }
    pub(crate) async fn finish_vector_maintenance_job(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        expected_revision: Uuid,
        outcome: VectorMaintenanceExecutionOutcome,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        let lease = guard.lease();
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(lease.resource.storage_bucket_id(), lease.resource.table_id().as_str()),
            )
            .await?;
        let current = self.read_vector_maintenance_job_with_guard(guard).await?;
        let request_payload = maintenance_outcome_payload(&outcome);
        if current.job.revision != expected_revision {
            let mut replay = current.job.clone();
            maintenance_stamp(&mut replay, "FINISH", Some(expected_revision), request_payload.clone())?;
            if replay.last_update_id == current.job.last_update_id && replay.last_update_sha256 == current.job.last_update_sha256
            {
                return Ok(current);
            }
            return Err(TableCatalogStoreError::Conflict("maintenance finish revision changed".into()));
        }
        if current.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("maintenance finish state changed".into()));
        }
        let mut next = current.job.clone();
        let now = OffsetDateTime::now_utc();
        next.updated_at = maintenance_format_time(now);
        let mut result = None;
        match outcome {
            VectorMaintenanceExecutionOutcome::NoWork { observed, reason } => {
                if self
                    .maintenance_resource_under_protection(&lease.resource, &observed.namespace, &observed.name)
                    .await?
                    != *observed
                {
                    return Err(TableCatalogStoreError::Conflict("NoWork observation changed".into()));
                }
                result = Some(VectorMaintenanceResultV1 {
                    version: 1,
                    job_id: lease.job_id,
                    recorded_by_job_id: lease.job_id,
                    attempt: lease.attempt,
                    resource: next.resource.clone(),
                    finished_at: next.updated_at.clone(),
                    effect: VectorMaintenanceResultEffect::NoWork,
                    stages: Vec::new(),
                    commit_refs: Vec::new(),
                    retirement_refs: Vec::new(),
                    output_refs: Vec::new(),
                    reason: Some(reason),
                });
            }
            VectorMaintenanceExecutionOutcome::Completed { result: completed } => {
                self.maintenance_verify_result(guard, &next, &completed).await?;
                result = Some(*completed);
            }
            VectorMaintenanceExecutionOutcome::Unknown { reason } => {
                next.state = VectorMaintenanceJobState::RecoveryRequired;
                next.failure = Some(reason);
            }
        }
        let fence = TableCatalogObjectMutationFence::from_signals(
            [
                guard.inner.lifecycle_owner.lock_lost_signal(),
                guard.inner.dispatch_owner.lock_lost_signal(),
                gc.lock_lost_signal(),
            ]
            .into_iter()
            .chain(guard.retained_loss_signals().into_iter().map(Some)),
        );
        if let Some(result) = result {
            let bytes = encode_vector_maintenance_record(&result)?;
            let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
            let path = format!(
                "{}results/{}/attempt-{}/{sha}.json",
                self.paths.vector_maintenance_prefix(&lease.resource)?,
                table_catalog_path_hash(&lease.job_id.to_string()),
                lease.attempt
            );
            let reference = self.maintenance_write_immutable(&lease.resource, path, bytes, &fence).await?;
            next.state = VectorMaintenanceJobState::Succeeded;
            next.result_ref = Some((&reference).into());
            next.finished_at = Some(result.finished_at);
            next.failure = None;
            next.next_retry_at = None;
        }
        maintenance_stamp(&mut next, "FINISH", Some(expected_revision), request_payload)?;
        let path = self.paths.vector_maintenance_job_path(&lease.resource, lease.job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let observed = self
            .maintenance_read_job(&lease.resource, lease.job_id, true)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("maintenance job".into()))?;
        if observed != current {
            return Err(TableCatalogStoreError::Conflict("maintenance finish raced another update".into()));
        }
        let mut fence = fence;
        fence.extend_signals([lock.lock_lost_signal()]);
        self.maintenance_write_job(&current, &next, lock.write_commit_guards(), &fence)
            .await
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceAuthorizationSnapshot {
    resource: VectorMaintenanceResourceSnapshot,
    definition: VectorIndexDefinitionV1,
}
impl VectorMaintenanceAuthorizationSnapshot {
    pub(crate) fn resource(&self) -> &VectorMaintenanceResourceSnapshot {
        &self.resource
    }
    pub(crate) fn definition(&self) -> &VectorIndexDefinitionV1 {
        &self.definition
    }
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Read authorization inputs under the caller's existing lifecycle and dispatch owners.
    pub(crate) async fn read_vector_maintenance_authorization_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceAuthorizationSnapshot> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        let resource = job.job.resource.identity()?;
        let namespace = Namespace::parse(&job.job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&job.job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let result = self
            .maintenance_authorization_under_protection(&resource, &namespace, &name)
            .await?;
        guard.check_held()?;
        Ok(result)
    }
    pub(crate) async fn read_vector_maintenance_authorization(
        &self,
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<VectorMaintenanceAuthorizationSnapshot> {
        let lifecycle = self
            .backend
            .acquire_bucket_lifecycle_read(self.maintenance_bucket(resource)?)
            .await?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let result = self
            .maintenance_authorization_under_protection(resource, namespace, name)
            .await?;
        if lifecycle.is_lock_lost() || gc.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("authorization protection lost".into()));
        }
        Ok(result)
    }
    async fn maintenance_authorization_under_protection(
        &self,
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<VectorMaintenanceAuthorizationSnapshot> {
        let snapshot = self.maintenance_resource_under_protection(resource, namespace, name).await?;
        let object = self.maintenance_read_ref(&snapshot.observed_object).await?;
        let definition = match snapshot.state {
            VectorMaintenanceResourceState::Active => {
                let entry: TableEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                if CatalogHead::from_vector_entry(&entry)?.resource_identity() != resource {
                    return Err(TableCatalogStoreError::Conflict("maintenance authorization resource changed".into()));
                }
                entry
                    .vector_index_definition
                    .ok_or_else(|| maintenance_error("maintenance authorization requires an Index definition"))?
            }
            VectorMaintenanceResourceState::Pending => {
                let intent: VectorIndexCreateIntentV1 =
                    serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                intent.validate()?;
                intent.definition
            }
            VectorMaintenanceResourceState::Deleting | VectorMaintenanceResourceState::Deleted => {
                let intent: VectorIndexDeleteIntent =
                    serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                intent.validate()?;
                intent
                    .expected_entry
                    .vector_index_definition
                    .clone()
                    .ok_or_else(|| maintenance_error("deletion authorization requires the original Index definition"))?
            }
        };
        definition.validate()?;
        Ok(VectorMaintenanceAuthorizationSnapshot {
            resource: snapshot,
            definition,
        })
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn copy_vector_maintenance_source(
        &self,
        resource: &CatalogResourceIdentity,
        exact_source: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let lifecycle = self
            .backend
            .acquire_bucket_lifecycle_read(self.maintenance_bucket(resource)?)
            .await?;
        self.require_vector_bucket_catalog_admission(self.maintenance_bucket(resource)?, &lifecycle)
            .await?;
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal()]);
        self.maintenance_copy_source(resource, exact_source, &fence).await
    }
    pub(crate) async fn copy_vector_maintenance_source_with_operation_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        source: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        self.validate_vector_operation_guard(guard)?;
        self.maintenance_copy_source(guard.resource_identity(), source, &guard.inner.fence)
            .await
    }
    pub(crate) async fn copy_vector_maintenance_source_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        exact_source: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        self.maintenance_dispatch_matches(guard)?;
        let fence = TableCatalogObjectMutationFence::from_signals(
            [
                guard.inner.lifecycle_owner.lock_lost_signal(),
                guard.inner.dispatch_owner.lock_lost_signal(),
            ]
            .into_iter()
            .chain(guard.retained_loss_signals().into_iter().map(Some)),
        );
        self.maintenance_copy_source(guard.lease().resource(), exact_source, &fence)
            .await
    }
    async fn maintenance_copy_source(
        &self,
        resource: &CatalogResourceIdentity,
        exact_source: &CatalogMetadataRef,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let bucket = self.maintenance_bucket(resource)?;
        let object = self.maintenance_read_ref(exact_source).await?;
        let key = exact_source.object_key().as_str();
        let prefix = self.paths.vector_maintenance_prefix(resource)?;
        if key.starts_with(
            &self
                .paths
                .commit_log_entries_prefix(resource.storage_bucket_id(), resource.table_id().as_str()),
        ) {
            let log: CommitLogEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            self.paths
                .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())?;
            if key
                != self
                    .paths
                    .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), &log.commit_id)
            {
                return Err(maintenance_error("source commit path mismatch"));
            }
        } else if key
            == self
                .paths
                .vector_delete_intent_path(resource.storage_bucket_id(), resource.table_id().as_str())
        {
            let intent: VectorIndexDeleteIntent =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            intent.validate()?;
            if intent.table_id != resource.table_id().as_str() || intent.bucket.to_handle()? != *bucket {
                return Err(TableCatalogStoreError::Conflict("source delete identity mismatch".into()));
            }
        } else if key.starts_with(&format!("{prefix}jobs/")) {
            let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
            if job.resource.identity()? != *resource || key != self.paths.vector_maintenance_job_path(resource, job.job_id)? {
                return Err(TableCatalogStoreError::Conflict("source job identity mismatch".into()));
            }
        } else if key.starts_with(&format!("{prefix}authorizations/")) {
            let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
            if auth.resource.identity()? != *resource
                || key != format!("{prefix}authorizations/{}.json", table_catalog_path_hash(&auth.retirement_id.to_string()))
            {
                return Err(TableCatalogStoreError::Conflict("source retirement identity mismatch".into()));
            }
        } else if key.starts_with(&format!("{prefix}progress/")) {
            let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
            if progress.resource.identity()? != *resource
                || progress.state != VectorRetirementProgressState::Completed
                || key != format!("{prefix}progress/{}.json", table_catalog_path_hash(&progress.retirement_id.to_string()))
            {
                return Err(TableCatalogStoreError::Conflict(
                    "source progress is not the exact completed retirement".into(),
                ));
            }
        } else {
            let intent: VectorIndexCreateIntentV1 = serde_json::from_slice(&object.data)
                .map_err(|e| maintenance_error(&format!("unsupported maintenance source: {e}")))?;
            intent.validate()?;
            let namespace = Namespace::parse(&intent.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
            let name = CatalogName::parse(&intent.name).map_err(|e| maintenance_error(&e.to_string()))?;
            if intent.table_id != resource.table_id().as_str()
                || intent.bucket.to_handle()? != *bucket
                || key
                    != self
                        .paths
                        .vector_create_intent_path(resource.storage_bucket_id(), &namespace, &name)
            {
                return Err(TableCatalogStoreError::Conflict("source create identity mismatch".into()));
            }
        }
        let sha = hex_simd::encode_to_string(Sha256::digest(&object.data), hex_simd::AsciiCase::Lower);
        self.maintenance_write_immutable(resource, format!("{prefix}sources/{sha}.json"), object.data, fence)
            .await
    }
    pub(crate) async fn read_vector_maintenance_stage_commit(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        original: &VectorMaintenanceJobSnapshot,
        stage: &str,
    ) -> TableCatalogStoreResult<Option<(CommitLogEntry, CatalogMetadataRef)>> {
        self.maintenance_dispatch_matches(guard)?;
        let resource = original.job.resource.identity()?;
        if &resource != guard.lease().resource()
            || !matches!(
                (original.job.kind, stage),
                (VectorMaintenanceKind::AnnBuild, "ann-build")
                    | (VectorMaintenanceKind::Compaction, "reserve-fragments" | "rewrite")
            )
        {
            return Err(maintenance_error("stage query differs from its original job"));
        }
        let id = vector_maintenance_stage_commit_id(original.job.job_id, original.job.attempt, stage)?;
        let path = self
            .paths
            .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), id.as_str());
        let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        else {
            guard.check_held()?;
            return Ok(None);
        };
        let log: CommitLogEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
        self.paths
            .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())?;
        if log.commit_id != id.as_str() {
            return Err(maintenance_error("stage log commit identity mismatch"));
        }
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        guard.check_held()?;
        Ok(Some((log, reference)))
    }
}

fn maintenance_discovery_limit(limit: NonZeroU16) -> TableCatalogStoreResult<NonZeroUsize> {
    if limit.get() > 64 {
        return Err(maintenance_error("maintenance discovery limit exceeds 64"));
    }
    NonZeroUsize::new(usize::from(limit.get())).ok_or_else(|| maintenance_error("empty discovery limit"))
}
fn validate_maintenance_page(
    page: &TableCatalogCleanupPage,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> TableCatalogStoreResult<()> {
    if page.objects.len() > limit || (page.objects.is_empty() && page.is_truncated) {
        return Err(maintenance_error("invalid authoritative maintenance page size"));
    }
    let mut previous = after;
    for object in &page.objects {
        if !object.key.starts_with(prefix) || previous.is_some_and(|key| object.key.as_str() <= key) {
            return Err(maintenance_error("maintenance page repeats or crosses its prefix"));
        }
        previous = Some(&object.key);
    }
    Ok(())
}
pub(crate) async fn list_vector_maintenance_buckets<B: TableCatalogObjectBackend>(
    backend: &B,
    cursor: Option<&VectorMaintenanceBucketCursor>,
    limit: NonZeroU16,
    budget: &VectorMaintenanceDiscoveryBudget,
) -> TableCatalogStoreResult<VectorMaintenanceBucketPage> {
    let limit = maintenance_discovery_limit(limit)?;
    let paths = TableCatalogObjectPaths::for_vector();
    let prefix = paths.table_bucket_entries_prefix();
    let mut after = cursor.and_then(|c| c.start_after.clone());
    if after
        .as_ref()
        .is_some_and(|value| value.len() > 4096 || !value.starts_with(&prefix))
    {
        return Err(maintenance_error("invalid Vector bucket discovery cursor"));
    }
    let mut items = Vec::new();
    if !budget.try_start_call() {
        return Ok(VectorMaintenanceBucketPage {
            items,
            next: Some(VectorMaintenanceBucketCursor { start_after: after }),
        });
    }
    let page = backend
        .list_catalog_objects_page(RUSTFS_META_BUCKET, &prefix, after.as_deref(), limit)
        .await?;
    validate_maintenance_page(&page, &prefix, after.as_deref(), limit.get())?;
    for object in page.objects {
        let relative = object
            .key
            .strip_prefix(&prefix)
            .ok_or_else(|| maintenance_error("invalid bucket discovery prefix"))?;
        if relative.split('/').count() != 2 || !relative.ends_with("/table-bucket.json") {
            after = Some(object.key);
            continue;
        }
        if object.delete_marker {
            return Err(maintenance_error("Catalog bucket binding is a delete marker"));
        }
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceBucketPage {
                items,
                next: Some(VectorMaintenanceBucketCursor { start_after: after }),
            });
        }
        let stored = backend
            .read_object_limited(RUSTFS_META_BUCKET, &object.key, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Unavailable("discovered Vector bucket binding disappeared".into()))?;
        let entry: TableBucketEntry = serde_json::from_slice(&stored.data).map_err(|e| maintenance_error(&e.to_string()))?;
        paths.validate_bucket_entry(&entry)?;
        if paths.table_bucket_entry_path(&entry.table_bucket) != object.key {
            return Err(maintenance_error("Vector bucket discovery identity differs from path"));
        }
        let bucket = entry
            .vector_bucket_binding
            .as_ref()
            .ok_or_else(|| maintenance_error("missing Vector bucket binding"))?
            .to_handle()?;
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceBucketPage {
                items,
                next: Some(VectorMaintenanceBucketCursor { start_after: after }),
            });
        }
        backend.revalidate_bucket_handle(&bucket).await?;
        items.push(bucket);
        after = Some(object.key);
    }
    Ok(VectorMaintenanceBucketPage {
        items,
        next: page
            .is_truncated
            .then_some(VectorMaintenanceBucketCursor { start_after: after }),
    })
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn list_vector_maintenance_resources(
        &self,
        bucket: &BucketHandle,
        cursor: Option<&VectorMaintenanceResourceCursor>,
        limit: NonZeroU16,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourcePage> {
        let limit = maintenance_discovery_limit(limit)?;
        if self.paths.scope != CatalogScope::Vector
            || self.vector_bucket.as_ref() != Some(bucket)
            || cursor.is_some_and(|c| &c.bucket != bucket)
        {
            return Err(TableCatalogStoreError::Conflict("maintenance discovery bucket changed".into()));
        }
        let phase = cursor.map_or(VectorMaintenanceResourceDiscoveryPhase::Entries, |c| c.phase);
        let prefix = match phase {
            VectorMaintenanceResourceDiscoveryPhase::Entries | VectorMaintenanceResourceDiscoveryPhase::CreateIntents => {
                self.paths.namespace_entries_prefix(bucket.storage_bucket_id().as_str())
            }
            VectorMaintenanceResourceDiscoveryPhase::DeleteIntents => {
                format!("{}resources/", self.paths.table_bucket_root_prefix(bucket.storage_bucket_id().as_str()))
            }
        };
        let mut after = cursor.and_then(|c| c.continuation.clone());
        if after.as_ref().is_some_and(|v| v.len() > 4096 || !v.starts_with(&prefix)) {
            return Err(maintenance_error("invalid resource discovery cursor"));
        }
        let mut items = Vec::new();
        let continuation = |after: Option<String>| {
            Some(VectorMaintenanceResourceCursor {
                bucket: bucket.clone(),
                phase,
                continuation: after,
            })
        };
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceResourcePage {
                items,
                next: continuation(after),
            });
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceResourcePage {
                items,
                next: continuation(after),
            });
        }
        let page = self
            .backend
            .list_catalog_objects_page(self.catalog_bucket(), &prefix, after.as_deref(), limit)
            .await?;
        validate_maintenance_page(&page, &prefix, after.as_deref(), limit.get())?;
        for object in page.objects {
            let suffix = match phase {
                VectorMaintenanceResourceDiscoveryPhase::Entries => "/table-entry.json",
                VectorMaintenanceResourceDiscoveryPhase::CreateIntents => "/create-intent.json",
                VectorMaintenanceResourceDiscoveryPhase::DeleteIntents => "/delete-intent.json",
            };
            if !object.key.ends_with(suffix) {
                after = Some(object.key);
                continue;
            }
            if object.delete_marker {
                return Err(maintenance_error("Catalog resource discovery encountered a delete marker"));
            }
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceResourcePage {
                    items,
                    next: continuation(after),
                });
            }
            let stored = self
                .backend
                .read_object_limited(self.catalog_bucket(), &object.key, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Unavailable("discovered Vector resource disappeared".into()))?;
            let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(object.key.clone())?, &stored)?;
            let snapshot = match phase {
                VectorMaintenanceResourceDiscoveryPhase::Entries => {
                    let entry: TableEntry =
                        serde_json::from_slice(&stored.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    let head = CatalogHead::from_vector_entry(&entry)?;
                    if self
                        .paths
                        .table_entry_path(bucket.storage_bucket_id().as_str(), head.namespace(), head.name())
                        != object.key
                        || head.resource_identity().vector_bucket() != Some(bucket)
                    {
                        return Err(TableCatalogStoreError::Conflict("discovered Vector entry identity mismatch".into()));
                    }
                    if entry.state != TableCatalogEntryState::Active {
                        after = Some(object.key);
                        continue;
                    }
                    VectorMaintenanceResourceSnapshot {
                        resource: head.resource_identity().clone(),
                        namespace: head.namespace().clone(),
                        name: head.name().clone(),
                        state: VectorMaintenanceResourceState::Active,
                        head: Some(VectorMaintenanceHeadV1::from_catalog_entry(&entry)?),
                        source_intent: None,
                        maintenance: entry.vector_maintenance_binding,
                        observed_object,
                    }
                }
                VectorMaintenanceResourceDiscoveryPhase::CreateIntents => {
                    let intent: VectorIndexCreateIntentV1 =
                        serde_json::from_slice(&stored.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    intent.validate()?;
                    let namespace = Namespace::parse(&intent.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
                    let name = CatalogName::parse(&intent.name).map_err(|e| maintenance_error(&e.to_string()))?;
                    if intent.bucket.to_handle()? != *bucket
                        || self
                            .paths
                            .vector_create_intent_path(bucket.storage_bucket_id().as_str(), &namespace, &name)
                            != object.key
                    {
                        return Err(TableCatalogStoreError::Conflict("discovered create intent identity mismatch".into()));
                    }
                    if matches!(intent.phase, VectorIndexCreatePhase::Published | VectorIndexCreatePhase::Aborted) {
                        after = Some(object.key);
                        continue;
                    }
                    let resource = CatalogResourceIdentity::try_new(
                        bucket.clone(),
                        crate::storage_api::table::managed::ManagedTableId::try_new(intent.table_id)
                            .map_err(|e| maintenance_error(&e.to_string()))?,
                        CatalogFormat::Lance,
                    )?;
                    VectorMaintenanceResourceSnapshot {
                        resource,
                        namespace,
                        name,
                        state: VectorMaintenanceResourceState::Pending,
                        head: None,
                        source_intent: Some(observed_object.clone()),
                        maintenance: None,
                        observed_object,
                    }
                }
                VectorMaintenanceResourceDiscoveryPhase::DeleteIntents => {
                    let intent: VectorIndexDeleteIntent =
                        serde_json::from_slice(&stored.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    intent.validate()?;
                    let head = CatalogHead::from_vector_entry(&intent.deleted_entry)?;
                    if intent.bucket.to_handle()? != *bucket
                        || self
                            .paths
                            .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), &intent.table_id)
                            != object.key
                    {
                        return Err(TableCatalogStoreError::Conflict("discovered delete intent identity mismatch".into()));
                    }
                    VectorMaintenanceResourceSnapshot {
                        resource: head.resource_identity().clone(),
                        namespace: head.namespace().clone(),
                        name: head.name().clone(),
                        state: if intent.phase == VectorIndexDeletePhase::Prepared {
                            VectorMaintenanceResourceState::Deleting
                        } else {
                            VectorMaintenanceResourceState::Deleted
                        },
                        head: Some(VectorMaintenanceHeadV1::from_catalog_entry(&intent.deleted_entry)?),
                        source_intent: Some(observed_object.clone()),
                        maintenance: intent.maintenance_binding().cloned(),
                        observed_object,
                    }
                }
            };
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceResourcePage {
                    items,
                    next: continuation(after),
                });
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            items.push(snapshot);
            after = Some(object.key);
        }
        let next = if page.is_truncated {
            continuation(after)
        } else {
            match phase {
                VectorMaintenanceResourceDiscoveryPhase::Entries => Some(VectorMaintenanceResourceCursor {
                    bucket: bucket.clone(),
                    phase: VectorMaintenanceResourceDiscoveryPhase::CreateIntents,
                    continuation: None,
                }),
                VectorMaintenanceResourceDiscoveryPhase::CreateIntents => Some(VectorMaintenanceResourceCursor {
                    bucket: bucket.clone(),
                    phase: VectorMaintenanceResourceDiscoveryPhase::DeleteIntents,
                    continuation: None,
                }),
                VectorMaintenanceResourceDiscoveryPhase::DeleteIntents => None,
            }
        };
        Ok(VectorMaintenanceResourcePage { items, next })
    }
    pub(crate) async fn list_vector_maintenance_jobs(
        &self,
        resource: &CatalogResourceIdentity,
        cursor: Option<&VectorMaintenanceJobCursor>,
        limit: NonZeroU16,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobPage> {
        let limit = maintenance_discovery_limit(limit)?;
        let bucket = self.maintenance_bucket(resource)?;
        if cursor.is_some_and(|c| c.resource != *resource) {
            return Err(TableCatalogStoreError::Conflict("maintenance job discovery resource changed".into()));
        }
        let prefix = format!("{}jobs/", self.paths.vector_maintenance_prefix(resource)?);
        let mut after = cursor.and_then(|c| c.continuation.clone());
        if after.as_ref().is_some_and(|s| s.len() > 4096 || !s.starts_with(&prefix)) {
            return Err(maintenance_error("invalid job discovery cursor"));
        }
        let mut items = Vec::new();
        let continuation = |after: Option<String>| {
            Some(VectorMaintenanceJobCursor {
                resource: resource.clone(),
                continuation: after,
            })
        };
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceJobPage {
                items,
                next: continuation(after),
            });
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceJobPage {
                items,
                next: continuation(after),
            });
        }
        let page = self
            .backend
            .list_catalog_objects_page(self.catalog_bucket(), &prefix, after.as_deref(), limit)
            .await?;
        validate_maintenance_page(&page, &prefix, after.as_deref(), limit.get())?;
        for object in page.objects {
            if !object.key.ends_with(".json") {
                after = Some(object.key);
                continue;
            }
            if object.delete_marker {
                return Err(maintenance_error("maintenance job has a delete marker"));
            }
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceJobPage {
                    items,
                    next: continuation(after),
                });
            }
            let stored = self
                .backend
                .read_object_limited(self.catalog_bucket(), &object.key, VECTOR_MAINTENANCE_JOB_MAX_BYTES)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Unavailable("discovered maintenance job disappeared".into()))?;
            let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&stored.data)?;
            if job.resource.identity()? != *resource
                || object.key != self.paths.vector_maintenance_job_path(resource, job.job_id)?
            {
                return Err(TableCatalogStoreError::Conflict("discovered job identity mismatch".into()));
            }
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceJobPage {
                    items,
                    next: continuation(after),
                });
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(object.key.clone())?, &stored)?;
            items.push(VectorMaintenanceJobSnapshot {
                job,
                observed_object,
                etag: stored.etag.ok_or_else(|| maintenance_error("discovered job has no ETag"))?,
            });
            after = Some(object.key);
        }
        Ok(VectorMaintenanceJobPage {
            items,
            next: if page.is_truncated { continuation(after) } else { None },
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum VectorMaintenancePreparationAction {
    InspectRetirementRecovery {
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
        expected_revision: Uuid,
    },
    PrepareRecovery {
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        expected_source: VectorMaintenanceSourceV1,
    },
    PrepareGc {
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        expected_head: Box<VectorMaintenanceHeadV1>,
    },
    EnrollCurrent {
        expected: Box<VectorCatalogEntry>,
        commit_id: CatalogCommitId,
    },
    EnrollDeleted {
        resource: CatalogResourceIdentity,
        expected_intent: CatalogMetadataRef,
        update_id: Uuid,
    },
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn read_vector_maintenance_resource_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceSnapshot> {
        self.validate_vector_operation_guard(guard)?;
        let snapshot = self
            .maintenance_resource_under_protection(guard.resource_identity(), guard.namespace(), guard.name())
            .await?;
        guard.check_held()?;
        Ok(snapshot)
    }
}

impl<S: TableCatalogStorage> ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<S>> {
    /// Preserve operation ownership while binding its authorized candidate reader.
    pub(crate) fn with_vector_managed_candidate_reader(
        &self,
        reader: Arc<dyn CatalogManagedCandidateReader>,
    ) -> TableCatalogStoreResult<Self> {
        if self.vector_bucket.is_none() {
            return Err(maintenance_error("managed candidate reader requires Vector scope"));
        }
        let mut store = self.clone();
        store.backend.managed_candidate_reader = Some(reader);
        Ok(store)
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn enroll_deleted_vector_maintenance(
        &self,
        resource: &CatalogResourceIdentity,
        expected_intent: &CatalogMetadataRef,
        update_id: Uuid,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceSnapshot> {
        let bucket = self.maintenance_bucket(resource)?;
        let path = self
            .paths
            .vector_delete_intent_path(resource.storage_bucket_id(), resource.table_id().as_str());
        if expected_intent.object_key().as_str() != path
            || update_id != vector_maintenance_enroll_deleted_update_id(resource, expected_intent)?
        {
            return Err(maintenance_error("deleted enrollment input differs from exact intent"));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_bucket_catalog_admission(bucket, &lifecycle).await?;
        let gc = self
            .backend
            .acquire_write_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let fence = TableCatalogObjectMutationFence::from_signals([
            lifecycle.lock_lost_signal(),
            gc.lock_lost_signal(),
            lock.lock_lost_signal(),
        ]);
        self.backend.revalidate_bucket_handle(bucket).await?;
        let object = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector delete intent".into()))?;
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path.clone())?, &object)?;
        let intent: VectorIndexDeleteIntent =
            serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
        let expected_digest = maintenance_digest(
            &serde_json::json!({"operation":"enroll-deleted","resource":resource_hash_value(resource)?,"expected_intent":CatalogMetadataRefRecordV1::from(expected_intent),"update_id":update_id}),
        )?;
        if let VectorIndexDeleteIntent::V2(existing) = &intent {
            if existing.last_maintenance_update_sha256 == expected_digest {
                fence.ensure_held()?;
                drop(lock);
                return self
                    .maintenance_resource_under_protection(
                        resource,
                        &Namespace::parse(&intent.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                        &CatalogName::parse(&intent.name).map_err(|e| maintenance_error(&e.to_string()))?,
                    )
                    .await;
            }
            return Err(TableCatalogStoreError::Conflict("deleted enrollment already changed".into()));
        }
        if &reference != expected_intent
            || intent.bucket.to_handle()? != *bucket
            || intent.table_id != resource.table_id().as_str()
            || !matches!(intent.phase, VectorIndexDeletePhase::LogicallyDeleted | VectorIndexDeletePhase::Complete)
        {
            return Err(TableCatalogStoreError::Conflict("deleted enrollment expected intent changed".into()));
        }
        self.verify_vector_deleted_history(&intent, None).await?;
        let VectorIndexDeleteIntent::V1(deletion) = intent else {
            return Err(maintenance_error("enrollment requires v1 intent"));
        };
        let mut upgraded = VectorIndexDeleteIntentV2::from_deletion(deletion)?;
        upgraded.last_maintenance_update_id = maintenance_uuid_from_digest(decode_catalog_digest(&expected_digest)?);
        upgraded.last_maintenance_update_sha256 = expected_digest;
        upgraded.maintenance_revision = Uuid::new_v4();
        upgraded.deletion.updated_at = next_table_catalog_update_time(Some(&upgraded.deletion.updated_at));
        upgraded.validate()?;
        let namespace = Namespace::parse(&upgraded.deletion.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&upgraded.deletion.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let bytes = serde_json::to_vec(&upgraded).map_err(|e| maintenance_error(&e.to_string()))?;
        if bytes.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
            return Err(maintenance_error("delete intent exceeds limit"));
        }
        let etag = object.etag.ok_or_else(|| maintenance_error("delete intent requires ETag"))?;
        let result = self
            .backend
            .put_object_unlocked_fenced(
                self.catalog_bucket(),
                &path,
                bytes.clone(),
                TableCatalogPutPrecondition::IfMatch(etag),
                lock.write_commit_guards(),
                &fence,
            )
            .await;
        if let Err(error) = result {
            let saved = self
                .backend
                .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                .await?;
            if !saved.is_some_and(|saved| saved.data == bytes) {
                return Err(error);
            }
        }
        fence.ensure_held()?;
        drop(lock);
        self.maintenance_resource_under_protection(resource, &namespace, &name).await
    }
}

#[path = "vector_bucket_retirement.rs"]
mod bucket_retirement;
pub(crate) use bucket_retirement::CatalogCleanupExecution;
#[cfg(test)]
pub(crate) use bucket_retirement::{
    CatalogRetirementDeleteProbe, CatalogRetirementEvidenceCursor, CatalogRetirementEvidenceReader, CatalogRetirementObservation,
    CatalogRetirementObservationHandle, CatalogRetirementObservedOutcome, CatalogRetirementObservedPhase,
    observe_catalog_retirement, with_catalog_retirement_request,
};
#[path = "vector_retirement.rs"]
mod retirement;
pub(crate) use retirement::*;

fn maintenance_outcome_payload(outcome: &VectorMaintenanceExecutionOutcome) -> serde_json::Value {
    match outcome {
        VectorMaintenanceExecutionOutcome::NoWork { observed, reason } => {
            serde_json::json!({"outcome":"NO_WORK","observed_object":CatalogMetadataRefRecordV1::from(&observed.observed_object),"reason":reason})
        }
        VectorMaintenanceExecutionOutcome::Completed { result } => serde_json::json!({"outcome":"COMPLETED","result":result}),
        VectorMaintenanceExecutionOutcome::Unknown { reason } => serde_json::json!({"outcome":"UNKNOWN","reason":reason}),
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn resume_vector_index_creation_with_dispatch(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<(VectorIndexCreateReservation, VectorCatalogOperationGuard)> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if job.job.kind != VectorMaintenanceKind::CreateRecovery || job.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("creation recovery requires its running task".into()));
        }
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("creation recovery source missing"))?;
        if source.kind != VectorMaintenanceSourceKind::CreateIntent {
            return Err(maintenance_error("creation recovery source kind mismatch"));
        }
        let object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let intent: VectorIndexCreateIntentV1 =
            serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
        intent.validate()?;
        if intent.resource_identity()? != guard.lease().resource {
            return Err(TableCatalogStoreError::Conflict("creation recovery identity mismatch".into()));
        }
        let bucket = self.maintenance_bucket(&guard.lease().resource)?.clone();
        let lifecycle = self
            .backend
            .reuse_bucket_lifecycle_read(&bucket, guard.lifecycle_owner())
            .await?;
        let namespace = Namespace::parse(&job.job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&job.job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let store = self.clone();
        let dispatch = guard.clone();
        object::run_vector_catalog_mutation(async move {
            let result = store
                .resume_vector_index_creation_with_lifecycle(
                    &bucket,
                    &namespace,
                    &name,
                    lifecycle,
                    Some(dispatch.lease().resource()),
                )
                .await?;
            if result.0.resource_identity() != dispatch.lease().resource() || result.0.create_id().as_str() != intent.create_id {
                return Err(TableCatalogStoreError::Conflict("creation recovery reservation changed".into()));
            }
            dispatch.check_held()?;
            Ok(result)
        })
        .await
    }
    pub(crate) async fn recover_vector_index_deletion_with_dispatch(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<()> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if job.job.kind != VectorMaintenanceKind::DeleteRecovery || job.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("deletion recovery requires its running task".into()));
        }
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("deletion recovery source missing"))?;
        if source.kind != VectorMaintenanceSourceKind::DeleteIntent {
            return Err(maintenance_error("deletion recovery source kind mismatch"));
        }
        let object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let intent: VectorIndexDeleteIntent =
            serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
        if intent.bucket.to_handle()? != *self.maintenance_bucket(&guard.lease().resource)?
            || intent.table_id != guard.lease().resource.table_id().as_str()
        {
            return Err(TableCatalogStoreError::Conflict("deletion recovery identity mismatch".into()));
        }
        let bucket = self.maintenance_bucket(&guard.lease().resource)?.clone();
        let lifecycle = self
            .backend
            .reuse_bucket_lifecycle_read(&bucket, guard.lifecycle_owner())
            .await?;
        let store = self.clone();
        let dispatch = guard.clone();
        object::run_vector_catalog_mutation(async move {
            store
                .recover_vector_index_deletion_with_lifecycle(&bucket, dispatch.lease().resource.table_id().as_str(), lifecycle)
                .await?;
            dispatch.check_held()
        })
        .await
    }
}
