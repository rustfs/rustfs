// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Read-only, current-authority observations of SDK candidates retained by real fault handles.

use super::*;
use crate::app::object::{DefaultObjectUsecase, ManagedMaintenanceAuditContext, ManagedMaintenanceContext, ManagedObjectContext};
use crate::vector::fault_test::{FaultHandle, SdkCandidateReceipt};
use sha2::{Digest, Sha256};

const MAX_SDK_CANDIDATES: usize = 32;
const MAX_SDK_REFERENCES: usize = 4096;
const MAX_SDK_OBJECT_BYTES: usize = 512 * 1024;
const MAX_SDK_OBSERVATION_BYTES: usize = 3 * 1024 * 1024;

pub(super) struct SdkCandidateBinding {
    receipt: SdkCandidateReceipt,
    resource: VectorMaintenanceResourceV1,
    _memory: Arc<VectorMaintenanceMemoryReservation>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SdkCandidateRegistration {
    pub(crate) observation_token: Uuid,
    pub(crate) instance_id: Uuid,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) reference_count: usize,
    pub(crate) commit_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SdkCandidateCatalogObservation {
    pub(crate) observation_token: Uuid,
    pub(crate) instance_id: Uuid,
    pub(crate) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) observed_at: String,
    pub(crate) current: VectorMaintenanceHeadV1,
    pub(crate) current_object: CatalogMetadataRefRecordV1,
    pub(crate) commit: Option<CommitLogEntry>,
    pub(crate) history: Vec<CommittedVersionObservation>,
    pub(crate) protected_candidates: Vec<CommittedVersionObservation>,
    pub(crate) protected_references: Vec<CatalogMetadataRefRecordV1>,
    /// Absence from the checked current/history/failed-candidate closures supplies no deletion authority.
    pub(crate) unreferenced_ordinals: Vec<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SdkCandidateObjectObservation {
    pub(crate) observation_token: Uuid,
    pub(crate) reference_ordinal: usize,
    pub(crate) instance_id: Uuid,
    pub(crate) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) observed_at: String,
    pub(crate) expected_object: CatalogMetadataRefRecordV1,
    pub(crate) body_revision: Option<Uuid>,
    pub(crate) etag: Option<String>,
    pub(crate) size: u64,
    pub(crate) quota_charge_bytes: u64,
    pub(crate) mod_time: Option<VectorObjectTimeV1>,
    pub(crate) content_start: Option<u64>,
    pub(crate) content_end: Option<u64>,
    pub(crate) complete_body: bool,
    /// Present only when the complete body was read and its original SHA-256 was checked.
    pub(crate) content_sha256: Option<String>,
    pub(crate) chunk_sha256: Option<String>,
    pub(crate) content_base64: Option<String>,
}

fn validate_sdk_receipt(
    instance_id: Uuid,
    resource: &VectorMaintenanceResourceV1,
    receipt: &SdkCandidateReceipt,
) -> Result<(), VectorMaintenanceExecutionError> {
    resource.validate()?;
    if instance_id != receipt.instance_id() || resource.identity()? != *receipt.resource() {
        return Err(conflict("SDK candidate receipt belongs to another instance or resource"));
    }
    Ok(())
}

fn validate_sdk_commit(receipt: &SdkCandidateReceipt, commit: &CommitLogEntry) -> Result<(), VectorMaintenanceExecutionError> {
    let candidate = receipt.observation();
    let expected = receipt.expected_head();
    let resource = receipt.resource();
    let binding = commit
        .vector_commit_binding
        .as_ref()
        .ok_or_else(|| conflict("observed SDK commit lacks its Vector binding"))?;
    if commit.commit_id != candidate.commit_id
        || commit.table_id != resource.table_id().as_str()
        || commit.operation != candidate.operation
        || commit.expected_version_token != expected.version_token
        || Some(&binding.bucket.to_handle()?) != resource.vector_bucket()
        || binding.table_id != resource.table_id().as_str()
        || binding.expected_generation != expected.generation
        || binding.expected_metadata != expected.metadata
        || binding.expected_descriptor != expected.descriptor
        || binding.candidate_metadata != candidate.metadata
        || binding.descriptor != candidate.descriptor
        || binding.request_digest_sha256 != candidate.request_digest_sha256
    {
        return Err(conflict("observed SDK commit differs from the original candidate receipt"));
    }
    Ok(())
}

fn insert_sdk_reference(
    references: &mut BTreeMap<String, CatalogMetadataRef>,
    reference: CatalogMetadataRef,
) -> Result<(), VectorMaintenanceExecutionError> {
    let key = reference.object_key().as_str().to_owned();
    if let Some(previous) = references.get(&key) {
        if previous != &reference {
            return Err(conflict("protected SDK references contain competing object identities"));
        }
        return Ok(());
    }
    if references.len() == MAX_SDK_REFERENCES {
        return Err(invalid("protected SDK reference observation exceeds its capacity"));
    }
    references.insert(key, reference);
    Ok(())
}

fn version_observation(version: &CatalogCommittedVersion) -> CommittedVersionObservation {
    CommittedVersionObservation {
        metadata: version.metadata().into(),
        descriptor: version.format_descriptor().into(),
        commit_id: version.commit_id().map(|id| id.as_str().to_owned()),
    }
}

fn same_versions(left: &[CatalogCommittedVersion], right: &[CatalogCommittedVersion]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.metadata() == right.metadata()
                && left.format_descriptor() == right.format_descriptor()
                && left.commit_id() == right.commit_id()
        })
}

impl MaintenanceDriver {
    fn sdk_observation_control(&self) -> Result<VectorMaintenanceControl, VectorMaintenanceExecutionError> {
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        control.check_admission()?;
        Ok(control)
    }

    async fn open_sdk_candidate(
        &self,
        binding: &SdkCandidateBinding,
    ) -> Result<(Arc<Catalog>, Arc<VectorCatalogOperationGuard>, VectorCatalogEntry), VectorMaintenanceExecutionError> {
        validate_sdk_receipt(self.app.object_store().id, &binding.resource, &binding.receipt)?;
        self.authorize_inspection(&binding.resource).await?;
        let bucket = binding.resource.bucket().to_handle()?;
        let namespace = Namespace::parse(binding.resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(binding.resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Arc::new(Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?);
        let (guard, entry) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        if guard.resource_identity() != binding.receipt.resource() {
            return Err(conflict("SDK candidate resource lifetime changed"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        Ok((catalog, Arc::new(guard), entry))
    }

    async fn sdk_candidate_binding(&self, token: Uuid) -> Result<Arc<SdkCandidateBinding>, VectorMaintenanceExecutionError> {
        self.sdk_candidates
            .lock()
            .await
            .get(&token)
            .cloned()
            .ok_or_else(|| invalid("unknown SDK candidate observation token"))
    }

    /// Register evidence only from an actual fault handle, using the existing resolved resource name binding.
    pub(crate) async fn retain_sdk_candidate(
        &self,
        resource: &VectorMaintenanceResourceV1,
        fault: &FaultHandle,
        observation_token: Uuid,
    ) -> Result<SdkCandidateRegistration, VectorMaintenanceExecutionError> {
        let receipt = fault
            .observed_sdk_candidate(observation_token)
            .map_err(maintenance_execution_error)?;
        validate_sdk_receipt(self.app.object_store().id, resource, &receipt)?;
        let control = self.sdk_observation_control()?;
        let bytes = serde_json::to_vec(&(resource, receipt.expected_head(), receipt.observation()))
            .map_err(maintenance_execution_error)?;
        let memory = control.reserve_memory(
            u64::try_from(bytes.len())
                .map_err(maintenance_execution_error)?
                .saturating_mul(2),
        )?;
        let binding = Arc::new(SdkCandidateBinding {
            receipt,
            resource: resource.clone(),
            _memory: memory,
        });
        let _permit = self
            .inspection_budgets
            .try_admit(binding.receipt.resource(), VectorMaintenanceLane::Control)?;
        let (catalog, guard, _) = self.open_sdk_candidate(&binding).await?;
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        let mut candidates = self.sdk_candidates.lock().await;
        if candidates.len() == MAX_SDK_CANDIDATES || candidates.contains_key(&observation_token) {
            return Err(conflict("SDK candidate token is already retained or capacity is exhausted"));
        }
        let output = SdkCandidateRegistration {
            observation_token,
            instance_id: binding.receipt.instance_id(),
            resource: binding.resource.clone(),
            reference_count: binding.receipt.observation().references.len(),
            commit_id: binding.receipt.observation().commit_id.clone(),
        };
        candidates.insert(observation_token, binding);
        Ok(output)
    }

    pub(crate) async fn release_sdk_candidate(&self, observation_token: Uuid) -> Result<(), VectorMaintenanceExecutionError> {
        self.sdk_candidates
            .lock()
            .await
            .remove(&observation_token)
            .ok_or_else(|| invalid("unknown SDK candidate observation token"))?;
        Ok(())
    }

    /// Reads current Catalog proofs. Unresolved roots preserve their original error and yield no unreferenced ordinals.
    pub(crate) async fn inspect_sdk_candidate(
        &self,
        observation_token: Uuid,
    ) -> Result<SdkCandidateCatalogObservation, VectorMaintenanceExecutionError> {
        let binding = self.sdk_candidate_binding(observation_token).await?;
        let _permit = self
            .inspection_budgets
            .try_admit(binding.receipt.resource(), VectorMaintenanceLane::Control)?;
        let control = self.sdk_observation_control()?;
        let _memory = control.reserve_memory(64 * 1024 * 1024)?;
        let (catalog, guard, entry) = self.open_sdk_candidate(&binding).await?;
        let current = VectorMaintenanceHeadV1::from_entry(&entry)?;
        let (catalog, adapter) = self
            .executor
            .preparation_adapter(
                catalog,
                guard.clone(),
                Uuid::new_v4(),
                ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
                control.clone(),
            )
            .await?;
        let resource_snapshot = catalog.read_vector_maintenance_resource_with_guard(&guard).await?;
        if resource_snapshot.head() != Some(&current) {
            return Err(conflict("SDK candidate head changed before Catalog observation"));
        }
        let resource = binding.receipt.resource();
        let candidate = binding.receipt.observation();
        let commit = catalog
            .get_commit_by_id(resource.storage_bucket_id(), resource.table_id().as_str(), &candidate.commit_id)
            .await?;
        if let Some(commit) = &commit {
            validate_sdk_commit(&binding.receipt, commit)?;
        }
        let protected_candidates = catalog.read_vector_maintenance_candidate_roots_with_guard(&guard).await?;
        let history = catalog.read_vector_maintenance_history_with_guard(&guard).await?;
        let mut references = BTreeMap::new();
        for reference in adapter
            .collect_references(entry.head().metadata(), entry.head().format_descriptor())
            .await
            .map_err(maintenance_execution_error)?
        {
            insert_sdk_reference(&mut references, reference)?;
        }
        for root in &protected_candidates {
            for reference in adapter
                .collect_candidate_references(root.metadata(), root.format_descriptor())
                .await
                .map_err(maintenance_execution_error)?
            {
                insert_sdk_reference(&mut references, reference)?;
            }
        }
        let unreferenced_ordinals = candidate
            .references
            .iter()
            .enumerate()
            .filter_map(|(ordinal, reference)| (!references.contains_key(&reference.object_key)).then_some(ordinal))
            .collect();
        let after_commit = catalog
            .get_commit_by_id(resource.storage_bucket_id(), resource.table_id().as_str(), &candidate.commit_id)
            .await?;
        let after_roots = catalog.read_vector_maintenance_candidate_roots_with_guard(&guard).await?;
        let after_history = catalog.read_vector_maintenance_history_with_guard(&guard).await?;
        let after = catalog.read_vector_maintenance_resource_with_guard(&guard).await?;
        if after_commit != commit
            || !same_versions(&after_roots, &protected_candidates)
            || !same_versions(&after_history, &history)
            || after.head() != Some(&current)
            || after.observed_object() != resource_snapshot.observed_object()
        {
            return Err(conflict("SDK candidate Catalog evidence changed during observation"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        control.check_admission()?;
        let output = SdkCandidateCatalogObservation {
            observation_token,
            instance_id: self.app.object_store().id,
            execution_identity: self.source.identity.clone(),
            resource: binding.resource.clone(),
            observed_at: OffsetDateTime::now_utc().to_string(),
            current,
            current_object: resource_snapshot.observed_object().into(),
            commit,
            history: history.iter().map(version_observation).collect(),
            protected_candidates: protected_candidates.iter().map(version_observation).collect(),
            protected_references: references.values().map(Into::into).collect(),
            unreferenced_ordinals,
        };
        if serde_json::to_vec(&output).map_err(maintenance_execution_error)?.len() > MAX_SDK_OBSERVATION_BYTES {
            return Err(invalid("SDK candidate Catalog observation exceeds its response capacity"));
        }
        Ok(output)
    }

    /// Observe the original receipt bucket through the quota owner's existing storage locks.
    pub(crate) async fn inspect_sdk_candidate_quota(
        &self,
        observation_token: Uuid,
    ) -> Result<super::quota_observation::CandidateQuotaSnapshot, VectorMaintenanceExecutionError> {
        let binding = self.sdk_candidate_binding(observation_token).await?;
        let _permit = self
            .inspection_budgets
            .try_admit(binding.receipt.resource(), VectorMaintenanceLane::Control)?;
        let control = self.sdk_observation_control()?;
        let _memory = control.reserve_memory(4 * 1024 * 1024)?;
        // Quota reads establish their own lifecycle/metadata/ledger order without Catalog guards.
        drop(self.open_sdk_candidate(&binding).await?);
        let io = control.acquire_io().await?;
        let snapshot = super::quota_observation::read_quota_snapshot(&self.app, &binding.receipt).await?;
        drop(io);
        // The quota owner releases every guard before current authorization is reacquired.
        drop(self.open_sdk_candidate(&binding).await?);
        control.check_admission()?;
        Ok(snapshot)
    }

    /// Read one owner-observed reference. Native NotFound and all other Managed errors retain their source chains.
    pub(crate) async fn inspect_sdk_candidate_object(
        &self,
        observation_token: Uuid,
        reference_ordinal: usize,
        content_offset: Option<u64>,
    ) -> Result<SdkCandidateObjectObservation, VectorMaintenanceExecutionError> {
        let binding = self.sdk_candidate_binding(observation_token).await?;
        let expected = binding
            .receipt
            .observation()
            .references
            .get(reference_ordinal)
            .ok_or_else(|| invalid("SDK candidate reference ordinal is outside its receipt"))?;
        let exact = CatalogMetadataRef::try_from(expected)?;
        let _permit = self
            .inspection_budgets
            .try_admit(binding.receipt.resource(), VectorMaintenanceLane::Control)?;
        let control = self.sdk_observation_control()?;
        let _memory = control.reserve_memory((MAX_SDK_OBJECT_BYTES as u64) * 4)?;
        let (catalog, guard, entry) = self.open_sdk_candidate(&binding).await?;
        let resource = guard.resource_identity();
        let bucket = resource
            .vector_bucket()
            .ok_or_else(|| invalid("SDK candidate requires a Vector bucket"))?;
        let audit = ManagedMaintenanceAuditContext::new_preparation(
            self.executor.identity.clone(),
            observation_token,
            ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
            "vector-sdk-observer".into(),
            self.executor.worker_boot_id,
            None,
        );
        let authorizer = Arc::new(MaintenanceObjectAuthorizer::for_preparation(
            self.app.clone(),
            catalog.clone(),
            guard.clone(),
            audit.clone(),
            None,
        ));
        let access = ManagedObjectAccess::try_from_parts(
            bucket.clone(),
            resource.table_id().clone(),
            resource.dataset_prefix(),
            [
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
            ]
            .into(),
        )
        .map_err(maintenance_execution_error)?;
        let context = ManagedObjectContext::from_maintenance(
            access,
            managed_encryption(&entry.definition().effective_encryption),
            Arc::new(ManagedMaintenanceContext::new(audit, authorizer)),
        )
        .map_err(maintenance_execution_error)?;
        let native = guard.lifecycle_guard()?;
        let usecase = DefaultObjectUsecase::with_context(Some(self.app.clone()));
        let _io = control.acquire_io().await?;
        let head = usecase
            .managed_head_object_with_guard(
                &context,
                exact.object_key().as_str(),
                exact.body_revision().map(|revision| revision.as_uuid()),
                &native,
            )
            .await
            .map_err(maintenance_execution_error)?;
        let size =
            u64::try_from(head.get_actual_size().map_err(maintenance_execution_error)?).map_err(maintenance_execution_error)?;
        if head.data_dir != expected.body_revision || head.etag != expected.etag || size != expected.content_length {
            return Err(conflict("SDK candidate object metadata differs from its original receipt"));
        }
        let quota_charge_bytes =
            crate::app::storage_api::test::data_usage::quota_object_size(&head).map_err(maintenance_execution_error)?;
        let (content_start, content_end, complete_body, content_sha256, chunk_sha256, content_base64) =
            if let Some(start) = content_offset {
                if start > size || (start == size && size != 0) {
                    return Err(invalid("SDK candidate content offset is outside its receipt"));
                }
                let end = start.saturating_add(MAX_SDK_OBJECT_BYTES as u64).min(size);
                let complete = start == 0 && end == size;
                let (info, bytes) = usecase
                    .managed_get_object_with_guard(
                        &context,
                        exact.object_key().as_str(),
                        expected.body_revision,
                        (!complete).then_some(start..end),
                        MAX_SDK_OBJECT_BYTES,
                        &native,
                    )
                    .await
                    .map_err(maintenance_execution_error)?;
                let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
                if info.data_dir != head.data_dir
                    || info.etag != head.etag
                    || info.mod_time != head.mod_time
                    || u64::try_from(bytes.len()).map_err(maintenance_execution_error)? != end - start
                    || (complete && sha != expected.content_sha256)
                {
                    return Err(conflict("SDK candidate object bytes differ from its original receipt"));
                }
                (
                    Some(start),
                    Some(end),
                    complete,
                    complete.then(|| sha.clone()),
                    Some(sha),
                    Some(base64_simd::STANDARD.encode_to_string(&bytes)),
                )
            } else {
                (None, None, false, None, None, None)
            };
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        control.check_admission()?;
        Ok(SdkCandidateObjectObservation {
            observation_token,
            reference_ordinal,
            instance_id: self.app.object_store().id,
            execution_identity: self.source.identity.clone(),
            resource: binding.resource.clone(),
            observed_at: OffsetDateTime::now_utc().to_string(),
            expected_object: expected.clone(),
            body_revision: head.data_dir,
            etag: head.etag,
            size,
            quota_charge_bytes,
            mod_time: head.mod_time.map(VectorObjectTimeV1::from_time),
            content_start,
            content_end,
            complete_body,
            content_sha256,
            chunk_sha256,
            content_base64,
        })
    }
}

#[cfg(test)]
#[path = "sdk_observer_tests.rs"]
mod tests;
