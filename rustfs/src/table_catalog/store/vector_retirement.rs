// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) struct PreparedVectorRetirement {
    resource: CatalogResourceIdentity,
    base_snapshot: VectorMaintenanceResourceSnapshot,
    plan_ref: CatalogMetadataRef,
    evidence_ref: CatalogMetadataRef,
    targets: Vec<VectorRetirementTargetV1>,
}
impl PreparedVectorRetirement {
    pub(crate) fn try_from_validated_parts(
        resource: CatalogResourceIdentity,
        base_snapshot: VectorMaintenanceResourceSnapshot,
        plan_ref: CatalogMetadataRef,
        evidence_ref: CatalogMetadataRef,
        targets: Vec<VectorRetirementTargetV1>,
    ) -> TableCatalogStoreResult<Self> {
        if base_snapshot.resource != resource
            || base_snapshot.maintenance.is_none()
            || targets.is_empty()
            || targets.len() > VECTOR_RETIREMENT_MAX_TARGETS
        {
            return Err(maintenance_error("retirement preparation resource or targets mismatch"));
        }
        let resource_record = VectorMaintenanceResourceV1::try_new(&resource, &base_snapshot.namespace, &base_snapshot.name)?;
        VectorRetirementPlanV1 {
            version: 1,
            resource: resource_record,
            evidence_ref: (&evidence_ref).into(),
            targets: targets.clone(),
        }
        .validate()?;
        Ok(Self {
            resource,
            base_snapshot,
            plan_ref,
            evidence_ref,
            targets,
        })
    }
    pub(crate) fn plan_ref(&self) -> &CatalogMetadataRef {
        &self.plan_ref
    }
    pub(crate) fn targets(&self) -> &[VectorRetirementTargetV1] {
        &self.targets
    }
}

pub(crate) struct VectorRetirementDispatchPermit {
    authorization_ref: CatalogMetadataRef,
    retirement_id: Uuid,
    retirement_revision: Uuid,
    job_id: Uuid,
    job_revision: Uuid,
    attempt: u16,
    lease_id: Uuid,
    worker_id: String,
    worker_boot_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    ordinal: u16,
    target: VectorRetirementTargetV1,
    dispatch_id: Uuid,
    guard: VectorMaintenanceDispatchGuard,
}
macro_rules! permit_getters {
    ($($field:ident: $ty:ty),* $(,)?) => { $(pub(crate) fn $field(&self) -> &$ty { &self.$field })* };
}
impl VectorRetirementDispatchPermit {
    permit_getters! {
        authorization_ref: CatalogMetadataRef,
        retirement_id: Uuid, retirement_revision: Uuid, job_id: Uuid, job_revision: Uuid,
        attempt: u16, lease_id: Uuid, worker_id: String, worker_boot_id: Uuid,
        execution_identity: VectorMaintenanceExecutionIdentityV1, ordinal: u16,
        target: VectorRetirementTargetV1, dispatch_id: Uuid,
    }
    pub(crate) fn dispatch_guard(&self) -> &VectorMaintenanceDispatchGuard {
        &self.guard
    }
    pub(crate) fn lifecycle_owner(&self) -> Arc<BucketIncarnationFenceGuard> {
        self.guard.lifecycle_owner()
    }
    pub(crate) fn dispatch_owner(&self) -> Arc<NamespaceLockGuard> {
        self.guard.dispatch_owner()
    }
}
pub(crate) enum VectorRetirementDispatchAdmission {
    Ready(Box<VectorRetirementDispatchPermit>),
    Recorded,
    NeedsRecovery,
}
pub(crate) enum VectorRetirementDeleteObservation {
    Committed(VectorDeleteAccountingV1),
    Unknown(VectorMaintenanceFailureCode),
}
fn retirement_progress_path(
    paths: &TableCatalogObjectPaths,
    resource: &CatalogResourceIdentity,
    id: Uuid,
) -> TableCatalogStoreResult<String> {
    Ok(format!(
        "{}progress/{}.json",
        paths.vector_maintenance_prefix(resource)?,
        table_catalog_path_hash(&id.to_string())
    ))
}
fn retirement_fence(guard: &VectorMaintenanceDispatchGuard) -> TableCatalogObjectMutationFence {
    TableCatalogObjectMutationFence::from_signals(
        [
            guard.inner.lifecycle_owner.lock_lost_signal(),
            guard.inner.dispatch_owner.lock_lost_signal(),
        ]
        .into_iter()
        .chain(guard.retained_loss_signals().into_iter().map(Some)),
    )
}
fn stamp_progress(
    progress: &mut VectorRetirementProgressV1,
    operation: &str,
    expected: Option<Uuid>,
    lease: &VectorMaintenanceLeaseToken,
    payload: serde_json::Value,
) -> TableCatalogStoreResult<()> {
    let value = serde_json::json!({"operation":operation,"resource":progress.resource,"record_id":progress.retirement_id,"expected_revision":expected,"lease_id":lease.lease_id,"attempt":lease.attempt,"payload":payload});
    let digest: [u8; 32] = Sha256::digest(maintenance_canonical_bytes(&value)?).into();
    progress.last_update_id = maintenance_uuid_from_digest(digest);
    progress.last_update_sha256 = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
    progress.revision = Uuid::new_v4();
    progress.updated_at = maintenance_now();
    Ok(())
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Persist bounded evidence produced by the format owner while its exact resource is protected.
    pub(crate) async fn prepare_vector_retirement(
        &self,
        guard: &VectorCatalogOperationGuard,
        evidence: VectorRetirementEvidenceV1,
        targets: Vec<VectorRetirementTargetV1>,
    ) -> TableCatalogStoreResult<PreparedVectorRetirement> {
        self.validate_vector_operation_guard(guard)?;
        evidence.validate()?;
        let snapshot = self.read_vector_maintenance_resource_with_guard(guard).await?;
        if evidence.resource.identity()? != *guard.resource_identity() || snapshot.head.as_ref() != Some(&evidence.base_head) {
            return Err(TableCatalogStoreError::Conflict(
                "retirement evidence does not bind current resource".into(),
            ));
        }
        self.verify_retirement_intent_copy(&snapshot, evidence.delete_intent_ref.as_ref())
            .await?;
        let bytes = encode_vector_maintenance_record(&evidence)?;
        let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
        let evidence_ref = self
            .maintenance_write_immutable(
                guard.resource_identity(),
                format!("{}evidence/{sha}.json", self.paths.vector_maintenance_prefix(guard.resource_identity())?),
                bytes,
                &guard.inner.fence,
            )
            .await?;
        let plan = VectorRetirementPlanV1 {
            version: 1,
            resource: evidence.resource,
            evidence_ref: (&evidence_ref).into(),
            targets: targets.clone(),
        };
        let bytes = encode_vector_maintenance_record(&plan)?;
        let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
        let plan_ref = self
            .maintenance_write_immutable(
                guard.resource_identity(),
                format!("{}plans/{sha}.json", self.paths.vector_maintenance_prefix(guard.resource_identity())?),
                bytes,
                &guard.inner.fence,
            )
            .await?;
        guard.check_held()?;
        PreparedVectorRetirement::try_from_validated_parts(
            guard.resource_identity().clone(),
            snapshot,
            plan_ref,
            evidence_ref,
            targets,
        )
    }

    async fn retirement_authority(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        reference: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<(VectorRetirementAuthorizationV1, VectorMaintenanceResourceSnapshot)> {
        self.maintenance_dispatch_matches(guard)?;
        self.retirement_authority_under_protection(guard.lease().resource(), reference)
            .await
    }
    async fn retirement_authority_under_protection(
        &self,
        resource: &CatalogResourceIdentity,
        reference: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<(VectorRetirementAuthorizationV1, VectorMaintenanceResourceSnapshot)> {
        let object = self.maintenance_read_ref(reference).await?;
        let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
        if auth.resource.identity()? != *resource
            || reference.object_key().as_str()
                != format!(
                    "{}authorizations/{}.json",
                    self.paths.vector_maintenance_prefix(resource)?,
                    table_catalog_path_hash(&auth.retirement_id.to_string())
                )
        {
            return Err(TableCatalogStoreError::Conflict(
                "retirement authorization path or resource mismatch".into(),
            ));
        }
        let namespace = Namespace::parse(&auth.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&auth.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let current = self
            .maintenance_resource_under_protection(resource, &namespace, &name)
            .await?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()))?;
        let mut found = false;
        self.visit_vector_retirements(resource, binding, |record| {
            if record.retirement_id == auth.retirement_id {
                if record != &auth {
                    return Err(TableCatalogStoreError::Conflict("retirement identity has different bytes".into()));
                }
                found = true;
            }
            Ok(())
        })
        .await?;
        if !found {
            return Err(TableCatalogStoreError::Conflict("retirement authorization is not published".into()));
        }
        Ok((auth, current))
    }

    async fn verify_retirement_history_evidence(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        evidence: &VectorRetirementEvidenceV1,
    ) -> TableCatalogStoreResult<Vec<CatalogCommittedVersion>> {
        let snapshot_object = self.maintenance_read_ref(&current.observed_object).await?;
        let entry = if current.state == VectorMaintenanceResourceState::Active {
            serde_json::from_slice::<TableEntry>(&snapshot_object.data).map_err(|e| maintenance_error(&e.to_string()))?
        } else if current.state == VectorMaintenanceResourceState::Deleted {
            let intent: VectorIndexDeleteIntent =
                serde_json::from_slice(&snapshot_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            let bounded_logs = self
                .read_vector_maintenance_commit_logs_bounded(&intent.deleted_entry)
                .await?;
            object::verify_vector_deleted_history_with_logs(&intent, None, &bounded_logs)?;
            intent.deleted_entry.clone()
        } else {
            return Err(TableCatalogStoreError::Conflict(
                "resource cannot authorize retirement in its current state".into(),
            ));
        };
        let logs = self.read_vector_maintenance_commit_logs_bounded(&entry).await?;
        let history = vector_committed_history_versions(&entry, &logs)?;
        let all_refs = evidence
            .history_refs
            .iter()
            .chain(&evidence.protected_refs)
            .chain(&evidence.unresolved_refs)
            .collect::<Vec<_>>();
        for version in &history {
            if !all_refs.contains(&&CatalogMetadataRefRecordV1::from(version.metadata())) {
                return Err(TableCatalogStoreError::Conflict("retirement proof omits a committed manifest".into()));
            }
        }
        let history_index = TableCommitHistoryIndex::new(&entry, logs.iter());
        for log in &logs {
            if log.status == CommitLogStatus::Staged
                || (log.status == CommitLogStatus::Committed && !history_index.proves_committed(log))
            {
                return Err(TableCatalogStoreError::Conflict(
                    "NeedsRecovery: retirement publication has unresolved Catalog work".into(),
                ));
            }
            if log.status == CommitLogStatus::Failed || !history_index.proves_committed(log) {
                let binding = log
                    .vector_commit_binding
                    .as_ref()
                    .ok_or_else(|| maintenance_error("unresolved Vector log lacks binding"))?;
                if !evidence.unresolved_refs.contains(&binding.candidate_metadata) {
                    return Err(TableCatalogStoreError::Conflict(
                        "retirement proof needs unresolved commit recovery".into(),
                    ));
                }
            }
        }
        Ok(history)
    }

    /// Reuse a published complete closure after a deleted resource's earlier batches removed its files.
    pub(crate) async fn read_vector_deleted_retirement_evidence_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<Option<VectorRetirementEvidenceV1>> {
        self.validate_vector_operation_guard(guard)?;
        let current = self.read_vector_maintenance_resource_with_guard(guard).await?;
        if current.state != VectorMaintenanceResourceState::Deleted {
            return Err(TableCatalogStoreError::Conflict(
                "retirement evidence reuse requires a deleted resource".into(),
            ));
        }
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()))?;
        let mut selected = None;
        self.visit_vector_retirements(&current.resource, binding, |authorization| {
            if selected.is_none()
                && authorization.delete_intent_ref.is_some()
                && current.head.as_ref() == Some(&authorization.base_head)
            {
                selected = Some(authorization.clone());
            }
            Ok(())
        })
        .await?;
        let Some(authorization) = selected else {
            return Ok(None);
        };
        let plan_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&authorization.plan_ref)?)
            .await?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&plan_object.data)?;
        let evidence_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&authorization.reference_evidence_ref)?)
            .await?;
        let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&evidence_object.data)?;
        if plan.resource != authorization.resource
            || plan.targets != authorization.targets
            || plan.evidence_ref != authorization.reference_evidence_ref
            || evidence.resource != authorization.resource
            || evidence.base_head != authorization.base_head
            || evidence.delete_intent_ref != authorization.delete_intent_ref
        {
            return Err(TableCatalogStoreError::Conflict("published retirement evidence binding changed".into()));
        }
        let copy_ref = evidence
            .delete_intent_ref
            .as_ref()
            .ok_or_else(|| maintenance_error("deleted retirement evidence lacks its intent"))?;
        if !copy_ref
            .object_key
            .starts_with(&format!("{}sources/", self.paths.vector_maintenance_prefix(&current.resource)?))
        {
            return Err(maintenance_error("retirement evidence requires an immutable deletion source"));
        }
        let copy = self.maintenance_read_ref(&CatalogMetadataRef::try_from(copy_ref)?).await?;
        let previous: VectorIndexDeleteIntent =
            serde_json::from_slice(&copy.data).map_err(|error| maintenance_error(&error.to_string()))?;
        let object = self.maintenance_read_ref(&current.observed_object).await?;
        let present: VectorIndexDeleteIntent =
            serde_json::from_slice(&object.data).map_err(|error| maintenance_error(&error.to_string()))?;
        previous.validate()?;
        present.validate()?;
        // Maintenance publication updates only the outer binding and timestamp of this deletion.
        let mut original_deletion = (*previous).clone();
        original_deletion.updated_at = present.updated_at.clone();
        if original_deletion != *present || previous.phase != VectorIndexDeletePhase::LogicallyDeleted {
            return Err(TableCatalogStoreError::Conflict(
                "retirement evidence belongs to another deletion transaction".into(),
            ));
        }
        self.verify_retirement_history_evidence(&current, &evidence).await?;
        guard.check_held()?;
        Ok(Some(evidence))
    }

    async fn verify_retirement_plan(
        &self,
        prepared: &PreparedVectorRetirement,
        current: &VectorMaintenanceResourceSnapshot,
    ) -> TableCatalogStoreResult<()> {
        if current != &prepared.base_snapshot {
            return Err(TableCatalogStoreError::Conflict("retirement base changed".into()));
        }
        let plan_object = self.maintenance_read_ref(&prepared.plan_ref).await?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&plan_object.data)?;
        let evidence_object = self.maintenance_read_ref(&prepared.evidence_ref).await?;
        let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&evidence_object.data)?;
        if plan.resource.identity()? != prepared.resource
            || plan.targets != prepared.targets
            || plan.evidence_ref != CatalogMetadataRefRecordV1::from(&prepared.evidence_ref)
            || evidence.resource != plan.resource
            || current.head.as_ref() != Some(&evidence.base_head)
        {
            return Err(TableCatalogStoreError::Conflict("retirement plan or evidence changed".into()));
        }
        self.verify_retirement_intent_copy(current, evidence.delete_intent_ref.as_ref())
            .await?;
        let history = self.verify_retirement_history_evidence(current, &evidence).await?;
        let retirement_now = OffsetDateTime::now_utc();
        for target in &prepared.targets {
            if !vector_retirement_target_is_old_enough(target, evidence.retention.clone(), retirement_now)? {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement target is inside its safety window or lacks modification time".into(),
                ));
            }
            if evidence
                .protected_refs
                .iter()
                .chain(&evidence.unresolved_refs)
                .any(|reference| {
                    reference.object_key == target.object_key && reference.body_revision == Some(target.body_revision)
                })
            {
                return Err(TableCatalogStoreError::Conflict("retirement target remains protected".into()));
            }
            if current.state == VectorMaintenanceResourceState::Active {
                let required = match evidence.retention {
                    VectorHistoryRetentionV1::All => history.len(),
                    VectorHistoryRetentionV1::Last { count, .. } => {
                        usize::try_from(count).map_err(|_| maintenance_error("retention count overflow"))?
                    }
                };
                if history.iter().take(required).any(|version| {
                    version.metadata().object_key().as_str() == target.object_key
                        && version.metadata().body_revision().map(|v| v.as_uuid()) == Some(target.body_revision)
                }) {
                    return Err(TableCatalogStoreError::Conflict("retirement target is retained history".into()));
                }
            }
            let actual = self
                .backend
                .object_metadata(prepared.resource.storage_bucket_id(), &target.object_key)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("retirement target disappeared before authorization".into()))?;
            if VectorRetirementTargetV1::from_object_metadata(
                &prepared.resource,
                CatalogObjectKey::try_new(target.object_key.clone())?,
                &actual,
            )? != *target
            {
                return Err(TableCatalogStoreError::Conflict("retirement target identity changed".into()));
            }
        }
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()))?;
        self.visit_vector_retirements(&prepared.resource, binding, |auth| {
            if auth.targets.iter().any(|old| {
                prepared
                    .targets
                    .iter()
                    .any(|target| old.object_key == target.object_key && old.body_revision == target.body_revision)
            }) {
                return Err(TableCatalogStoreError::Conflict("retirement target already has an authorization".into()));
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn publish_vector_retirement(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        prepared: PreparedVectorRetirement,
        retirement_id: Uuid,
        retirement_revision: Uuid,
        commit_id: CatalogCommitId,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        self.maintenance_dispatch_matches(guard)?;
        if prepared.resource != guard.lease().resource || retirement_id.is_nil() || retirement_revision.is_nil() {
            return Err(maintenance_error("retirement publication identity mismatch"));
        }
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if job.job.state != VectorMaintenanceJobState::Running
            || !matches!(job.job.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
            || job.job.input.plan_ref.as_ref() != Some(&CatalogMetadataRefRecordV1::from(&prepared.plan_ref))
        {
            return Err(TableCatalogStoreError::Conflict(
                "retirement publication requires its running GC plan".into(),
            ));
        }
        let lifecycle = self
            .backend
            .reuse_bucket_lifecycle_read(self.maintenance_bucket(&prepared.resource)?, guard.lifecycle_owner())
            .await?;
        let gc_path = self
            .paths
            .vector_gc_lock_path(prepared.resource.storage_bucket_id(), prepared.resource.table_id().as_str());
        let acquire = self.backend.acquire_write_lock(self.catalog_bucket(), &gc_path);
        #[cfg(test)]
        let acquire = crate::table_catalog::maintenance::fault_observation::with_gc_acquisition(
            crate::table_catalog::maintenance::fault_observation::VectorMaintenanceGcLockBinding {
                resource: job.job().resource().clone(),
                job_id: *job.job().job_id(),
                job_revision: *job.job().revision(),
                job_object: job.observed_object().into(),
                kind: *job.job().kind(),
                lease: guard.lease().clone(),
                plan_ref: (&prepared.plan_ref).into(),
                evidence_ref: (&prepared.evidence_ref).into(),
                targets: prepared.targets.clone(),
                lock_resource: rustfs_lock::ObjectKey::new(self.catalog_bucket(), gc_path.as_str()),
            },
            acquire,
        );
        let gc = acquire.await?;
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal()]);
        let current = self
            .maintenance_resource_under_protection(
                &prepared.resource,
                &prepared.base_snapshot.namespace,
                &prepared.base_snapshot.name,
            )
            .await?;
        let prefix = self.paths.vector_maintenance_prefix(&prepared.resource)?;
        let path = format!("{prefix}authorizations/{}.json", table_catalog_path_hash(&retirement_id.to_string()));
        let existing = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?;
        if let Some(object) = &existing {
            let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
            if auth.retirement_id != retirement_id
                || auth.retirement_revision != retirement_revision
                || auth.created_by_job_id != guard.lease().job_id
                || auth.created_by_attempt != guard.lease().attempt
                || auth.plan_ref != CatalogMetadataRefRecordV1::from(&prepared.plan_ref)
                || auth.reference_evidence_ref != CatalogMetadataRefRecordV1::from(&prepared.evidence_ref)
                || auth.targets != prepared.targets
            {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement ID already binds different authorization".into(),
                ));
            }
            let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path.clone())?, object)?;
            if let Some(binding) = &current.maintenance {
                let mut published = false;
                self.visit_vector_retirements(&prepared.resource, binding, |record| {
                    published |= record == &auth;
                    Ok(())
                })
                .await?;
                if published {
                    self.ensure_retirement_progress(guard, &reference, &auth, &fence).await?;
                    fence.ensure_held()?;
                    return Ok(reference);
                }
            }
        }
        self.verify_retirement_plan(&prepared, &current).await?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()))?;
        if binding.retirement_count >= 4096 {
            return Err(TableCatalogStoreError::Unavailable("CapacityExceeded: retirement chain is full".into()));
        }
        let evidence_object = self.maintenance_read_ref(&prepared.evidence_ref).await?;
        let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&evidence_object.data)?;
        let auth = VectorRetirementAuthorizationV1 {
            version: 1,
            retirement_id,
            retirement_revision,
            created_by_job_id: guard.lease().job_id,
            created_by_attempt: guard.lease().attempt,
            resource: job.job.resource.clone(),
            execution_identity: job.job.execution_identity.clone(),
            created_at: if let Some(object) = &existing {
                decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?.created_at
            } else {
                maintenance_now()
            },
            previous_ref: binding.retirement_tip.clone(),
            base_head: current
                .head
                .clone()
                .ok_or_else(|| maintenance_error("retirement resource lacks terminal head"))?,
            delete_intent_ref: evidence.delete_intent_ref,
            plan_ref: (&prepared.plan_ref).into(),
            reference_evidence_ref: (&prepared.evidence_ref).into(),
            targets: prepared.targets,
        };
        let reference = self
            .maintenance_write_immutable(&prepared.resource, path, encode_vector_maintenance_record(&auth)?, &fence)
            .await?;
        let next_binding = VectorMaintenanceBindingV1 {
            version: 1,
            retirement_count: binding.retirement_count + 1,
            retirement_tip: Some((&reference).into()),
        };
        match current.state {
            VectorMaintenanceResourceState::Active => {
                let object = self.maintenance_read_ref(&current.observed_object).await?;
                let entry: TableEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                let expected = VectorCatalogEntry::from_entry(entry)?;
                let native_lifecycle = lifecycle.lifecycle_guard.clone();
                let operation = VectorCatalogOperationGuard {
                    inner: Arc::new(VectorCatalogOperationState {
                        owner: self.vector_owner.clone(),
                        retirement_validation_owner: Arc::new(()),
                        resource_identity: prepared.resource.clone(),
                        namespace: current.namespace.clone(),
                        name: current.name.clone(),
                        lifecycle: tokio::sync::Mutex::new(lifecycle),
                        native_lifecycle,
                        gc: parking_lot::Mutex::new(gc),
                        gc_exclusive: true,
                        fence: fence.clone(),
                    }),
                };
                let commit = PreparedCatalogCommit::try_from_validated_parts(
                    CatalogCommitParts {
                        resource_identity: prepared.resource.clone(),
                        namespace: current.namespace.clone(),
                        name: current.name.clone(),
                        expected_head: expected.head().expected_head(),
                        commit_id,
                        operation: "vector-maintenance-retire".into(),
                        idempotency_key: None,
                        writer: None,
                    },
                    expected.head().metadata().clone(),
                    expected.head().format_descriptor().clone(),
                    CatalogEntryUpdates::try_new(
                        Some(expected.entry.warehouse_location.clone()),
                        Some(expected.entry.format_version),
                    )?,
                    Vec::new(),
                )?
                .with_vector_resource_mutation_v2(VectorResourceMutationBindingV2 {
                    version: 2,
                    kind: VectorResourceMutationKind::Retire,
                    expected_definition: expected.definition.clone(),
                    next_definition: expected.definition.clone(),
                    expected_state: TableCatalogEntryState::Active,
                    next_state: TableCatalogEntryState::Active,
                    expected_maintenance: Some(binding.clone()),
                    next_maintenance: next_binding,
                })?;
                let store = self.clone();
                let dispatch = guard.clone();
                object::run_vector_catalog_mutation(async move {
                    let publication = TableCommitLockPublication::new(&store.backend, &store.paths);
                    store
                        .commit_catalog_with_operation_guard(
                            CatalogCommitInput::Prepared(Box::new(commit)),
                            &publication,
                            Some(&operation),
                        )
                        .await?;
                    operation.check_held()?;
                    dispatch.check_held()
                })
                .await?;
            }
            VectorMaintenanceResourceState::Deleted => {
                let path = current.observed_object.object_key().as_str();
                let lock = self.backend.acquire_write_lock(self.catalog_bucket(), path).await?;
                fence.extend_signals([lock.lock_lost_signal()]);
                let object = self
                    .backend
                    .read_object_unlocked_limited(self.catalog_bucket(), path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::NotFound("delete intent".into()))?;
                current.observed_object.verify_read(&object)?;
                let intent: VectorIndexDeleteIntent =
                    serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                let VectorIndexDeleteIntent::V2(mut intent) = intent else {
                    return Err(TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()));
                };
                let digest = maintenance_digest(
                    &serde_json::json!({"operation":"RETIRE","resource":auth.resource,"record_id":intent.deletion.delete_id,"expected_revision":intent.maintenance_revision,"lease_id":guard.lease().lease_id,"attempt":guard.lease().attempt,"payload":{"authorization_ref":CatalogMetadataRefRecordV1::from(&reference),"maintenance":next_binding}}),
                )?;
                intent.maintenance_binding = next_binding;
                intent.maintenance_revision = Uuid::new_v4();
                intent.last_maintenance_update_id = maintenance_uuid_from_digest(decode_catalog_digest(&digest)?);
                intent.last_maintenance_update_sha256 = digest;
                intent.deletion.updated_at = next_table_catalog_update_time(Some(&intent.deletion.updated_at));
                intent.validate()?;
                let bytes = serde_json::to_vec(&intent).map_err(|e| maintenance_error(&e.to_string()))?;
                let etag = object.etag.ok_or_else(|| maintenance_error("delete intent lacks ETag"))?;
                let result = self
                    .backend
                    .put_object_unlocked_fenced(
                        self.catalog_bucket(),
                        path,
                        bytes.clone(),
                        TableCatalogPutPrecondition::IfMatch(etag),
                        lock.write_commit_guards(),
                        &fence,
                    )
                    .await;
                let saved = self
                    .backend
                    .read_object_unlocked_limited(self.catalog_bucket(), path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                    .await?;
                if !saved.is_some_and(|object| object.data == bytes) {
                    result?;
                    return Err(TableCatalogStoreError::Unavailable("retirement intent has no exact write receipt".into()));
                }
            }
            _ => return Err(TableCatalogStoreError::Conflict("retirement resource is not eligible".into())),
        }
        self.ensure_retirement_progress(guard, &reference, &auth, &fence).await?;
        guard.check_held()?;
        Ok(reference)
    }

    async fn ensure_retirement_progress(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        reference: &CatalogMetadataRef,
        auth: &VectorRetirementAuthorizationV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorRetirementProgressV1> {
        let resource = auth.resource.identity()?;
        let path = retirement_progress_path(&self.paths, &resource, auth.retirement_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let mut fence = fence.clone();
        fence.extend_signals([lock.lock_lost_signal()]);
        if let Some(object) = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
        {
            let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
            if progress.authorization_ref != CatalogMetadataRefRecordV1::from(reference)
                || progress.resource != auth.resource
                || progress.targets.len() != auth.targets.len()
            {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement progress belongs to another authorization".into(),
                ));
            }
            return Ok(progress);
        }
        let now = maintenance_now();
        let mut progress = VectorRetirementProgressV1 {
            version: 1,
            resource: auth.resource.clone(),
            retirement_id: auth.retirement_id,
            authorization_ref: reference.into(),
            revision: Uuid::new_v4(),
            last_update_id: Uuid::new_v4(),
            last_update_sha256: "0".repeat(64),
            state: VectorRetirementProgressState::Authorized,
            created_at: now.clone(),
            updated_at: now,
            targets: auth
                .targets
                .iter()
                .enumerate()
                .map(|(ordinal, _)| VectorRetirementTargetProgressV1 {
                    ordinal: ordinal as u16,
                    state: VectorRetirementTargetState::Pending,
                    dispatch: None,
                    receipt: None,
                    failure: None,
                })
                .collect(),
        };
        let payload = serde_json::json!({"authorization_ref":progress.authorization_ref});
        stamp_progress(&mut progress, "AUTHORIZE", None, guard.lease(), payload)?;
        self.write_retirement_progress(&path, None, &progress, lock.write_commit_guards(), &fence, None)
            .await?;
        Ok(progress)
    }
    async fn write_retirement_progress(
        &self,
        path: &str,
        etag: Option<String>,
        progress: &VectorRetirementProgressV1,
        guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
        recovery_owner: Option<&VectorMaintenanceRecoveryOwner>,
    ) -> TableCatalogStoreResult<()> {
        let bytes = encode_vector_maintenance_record(progress)?;
        let condition = etag.map_or(TableCatalogPutPrecondition::IfAbsent, TableCatalogPutPrecondition::IfMatch);
        fence.ensure_held()?;
        if let Some(owner) = recovery_owner {
            // From this point a dispatched PUT can outlive its caller or lose its response.
            owner.delete_dispatch_started();
        }
        let result = self
            .backend
            .put_object_unlocked_fenced(self.catalog_bucket(), path, bytes.clone(), condition, guards, fence)
            .await;
        if self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .is_some_and(|object| object.data == bytes)
        {
            return fence.ensure_held();
        }
        result?;
        Err(TableCatalogStoreError::Unavailable(
            "retirement progress has no exact write receipt".into(),
        ))
    }

    pub(crate) async fn read_vector_retirement_progress(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        authorization: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<VectorRetirementProgressV1> {
        self.maintenance_dispatch_matches(guard)?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        let (auth, _) = self.retirement_authority(guard, authorization).await?;
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal()]);
        self.ensure_retirement_progress(guard, authorization, &auth, &fence).await
    }

    pub(crate) async fn begin_vector_retirement_delete(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        authorization: &CatalogMetadataRef,
        ordinal: u16,
        expected_progress_revision: Uuid,
        dispatch_id: Uuid,
    ) -> TableCatalogStoreResult<VectorRetirementDispatchAdmission> {
        self.maintenance_dispatch_matches(guard)?;
        if dispatch_id.is_nil() {
            return Err(maintenance_error("nil retirement dispatch ID"));
        }
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        let (auth, _) = self.retirement_authority(guard, authorization).await?;
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if job.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("retirement dispatch job is not running".into()));
        }
        let original = auth.created_by_job_id == job.job.job_id
            && auth.created_by_attempt == job.job.attempt
            && auth.execution_identity == job.job.execution_identity;
        if !original {
            let source = job
                .job
                .input
                .source
                .as_ref()
                .ok_or_else(|| maintenance_error("retirement recovery has no original authorization"))?;
            if job.job.kind != VectorMaintenanceKind::DeleteRecovery
                || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
            {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement execution is unrelated to authorization".into(),
                ));
            }
            let object = self
                .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
                .await?;
            if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)? != auth
                || !guard.inner.prior_dispatch_jobs.contains(&auth.created_by_job_id)
            {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement recovery lacks original dispatch protection".into(),
                ));
            }
        }
        let target = auth
            .targets
            .get(usize::from(ordinal))
            .ok_or_else(|| maintenance_error("retirement ordinal is outside authorization"))?
            .clone();
        let path = retirement_progress_path(&self.paths, &guard.lease().resource, auth.retirement_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal(), lock.lock_lost_signal()]);
        let object = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement progress".into()))?;
        let mut progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
        if progress.authorization_ref != CatalogMetadataRefRecordV1::from(authorization)
            || progress.resource != auth.resource
            || progress.targets.len() != auth.targets.len()
        {
            return Err(TableCatalogStoreError::Conflict("retirement progress identity changed".into()));
        }
        let saved = &progress.targets[usize::from(ordinal)];
        if matches!(
            saved.state,
            VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
        ) {
            return Ok(VectorRetirementDispatchAdmission::Recorded);
        }
        if matches!(saved.state, VectorRetirementTargetState::InFlight | VectorRetirementTargetState::Unknown) {
            return Ok(VectorRetirementDispatchAdmission::NeedsRecovery);
        }
        if progress.revision != expected_progress_revision {
            return Err(TableCatalogStoreError::Conflict("retirement progress revision changed".into()));
        }
        let actual = self
            .backend
            .object_metadata(guard.lease().resource.storage_bucket_id(), &target.object_key)
            .await?;
        let Some(actual) = actual else {
            return Ok(VectorRetirementDispatchAdmission::NeedsRecovery);
        };
        if VectorRetirementTargetV1::from_object_metadata(
            &guard.lease().resource,
            CatalogObjectKey::try_new(target.object_key.clone())?,
            &actual,
        )? != target
        {
            return Err(TableCatalogStoreError::Conflict("retirement target body changed".into()));
        }
        let dispatch = VectorRetirementDispatchV1 {
            dispatch_id,
            job_id: job.job.job_id,
            job_revision: job.job.revision,
            attempt: job.job.attempt,
            lease_id: guard.lease().lease_id,
            worker_id: guard.lease().worker_id.clone(),
            worker_boot_id: guard.lease().worker_boot_id,
            execution_identity: job.job.execution_identity.clone(),
            admitted_at: maintenance_now(),
        };
        progress.targets[usize::from(ordinal)] = VectorRetirementTargetProgressV1 {
            ordinal,
            state: VectorRetirementTargetState::InFlight,
            dispatch: Some(dispatch),
            receipt: None,
            failure: None,
        };
        progress.state = VectorRetirementProgressState::Deleting;
        stamp_progress(
            &mut progress,
            "DISPATCH",
            Some(expected_progress_revision),
            guard.lease(),
            serde_json::json!({"ordinal":ordinal,"dispatch_id":dispatch_id}),
        )?;
        self.write_retirement_progress(&path, object.etag, &progress, lock.write_commit_guards(), &fence, None)
            .await?;
        fence.ensure_held()?;
        Ok(VectorRetirementDispatchAdmission::Ready(Box::new(VectorRetirementDispatchPermit {
            authorization_ref: authorization.clone(),
            retirement_id: auth.retirement_id,
            retirement_revision: auth.retirement_revision,
            job_id: job.job.job_id,
            job_revision: job.job.revision,
            attempt: job.job.attempt,
            lease_id: guard.lease().lease_id,
            worker_id: guard.lease().worker_id.clone(),
            worker_boot_id: guard.lease().worker_boot_id,
            execution_identity: job.job.execution_identity,
            ordinal,
            target,
            dispatch_id,
            guard: guard.clone(),
        })))
    }

    pub(crate) async fn record_vector_retirement_delete(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        authorization: &CatalogMetadataRef,
        ordinal: u16,
        dispatch_id: Uuid,
        expected_progress_revision: Uuid,
        outcome: VectorRetirementDeleteObservation,
    ) -> TableCatalogStoreResult<VectorRetirementProgressV1> {
        self.maintenance_dispatch_matches(guard)?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        let (auth, _) = self.retirement_authority(guard, authorization).await?;
        let path = retirement_progress_path(&self.paths, &guard.lease().resource, auth.retirement_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal(), lock.lock_lost_signal()]);
        let object = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement progress".into()))?;
        let mut progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
        if progress.authorization_ref != CatalogMetadataRefRecordV1::from(authorization)
            || progress.resource != auth.resource
            || progress.targets.len() != auth.targets.len()
        {
            return Err(TableCatalogStoreError::Conflict("retirement progress identity changed".into()));
        }
        let target = progress
            .targets
            .get_mut(usize::from(ordinal))
            .ok_or_else(|| maintenance_error("retirement ordinal exceeds progress"))?;
        let dispatch = target
            .dispatch
            .as_ref()
            .ok_or_else(|| maintenance_error("retirement target has not been dispatched"))?;
        if dispatch.dispatch_id != dispatch_id
            || dispatch.job_id != guard.lease().job_id
            || dispatch.attempt != guard.lease().attempt
            || dispatch.lease_id != guard.lease().lease_id
            || dispatch.worker_boot_id != guard.lease().worker_boot_id
            || dispatch.execution_identity != guard.lease().execution_identity
        {
            return Err(TableCatalogStoreError::Conflict("retirement receipt differs from admitted owner".into()));
        }
        let (state, accounting, failure, counts) = match outcome {
            VectorRetirementDeleteObservation::Committed(accounting) => {
                accounting.validate()?;
                (VectorRetirementTargetState::DeleteCommitted, Some(accounting), None, (0, 0, 0))
            }
            VectorRetirementDeleteObservation::Unknown(reason) => {
                (VectorRetirementTargetState::Unknown, None, Some(reason), (0, 0, 0))
            }
        };
        let payload = serde_json::json!({"ordinal":ordinal,"dispatch_id":dispatch_id,"state":state,"accounting":accounting,"failure":failure,"recovery_pending":counts.0,"recovery_blocked":counts.1,"recovery_failed":counts.2});
        if progress.revision != expected_progress_revision {
            let mut replay = progress.clone();
            stamp_progress(&mut replay, "RECEIPT", Some(expected_progress_revision), guard.lease(), payload)?;
            if replay.last_update_id == progress.last_update_id && replay.last_update_sha256 == progress.last_update_sha256 {
                return Ok(progress);
            }
            return Err(TableCatalogStoreError::Conflict("retirement receipt revision changed".into()));
        }
        let target = &mut progress.targets[usize::from(ordinal)];
        if matches!(
            target.state,
            VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
        ) {
            return Err(TableCatalogStoreError::Conflict("retirement target already completed".into()));
        }
        target.state = state;
        target.failure = failure;
        target.receipt = if failure.is_some() {
            None
        } else {
            Some(VectorRetirementReceiptV1 {
                outcome: if state == VectorRetirementTargetState::DeleteCommitted {
                    VectorRetirementReceiptOutcome::DeleteCommitted
                } else {
                    VectorRetirementReceiptOutcome::AbsentAfterRecovery
                },
                completed_at: maintenance_now(),
                accounting,
                recovery_pending: counts.0,
                recovery_blocked: counts.1,
                recovery_failed: counts.2,
            })
        };
        progress.state = if progress.targets.iter().all(|target| {
            matches!(
                target.state,
                VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
            )
        }) {
            VectorRetirementProgressState::Completed
        } else {
            VectorRetirementProgressState::Deleting
        };
        stamp_progress(&mut progress, "RECEIPT", Some(expected_progress_revision), guard.lease(), payload)?;
        self.write_retirement_progress(&path, object.etag, &progress, lock.write_commit_guards(), &fence, None)
            .await?;
        Ok(progress)
    }
}

pub(crate) struct VectorMaintenanceObjectPage {
    items: Vec<(CatalogObjectKey, TableCatalogObjectMetadata)>,
    next: Option<String>,
}
impl VectorMaintenanceObjectPage {
    pub(crate) fn into_parts(self) -> (Vec<(CatalogObjectKey, TableCatalogObjectMetadata)>, Option<String>) {
        (self.items, self.next)
    }
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn begin_vector_maintenance_resource_operation(
        &self,
        expected: &VectorMaintenanceResourceSnapshot,
    ) -> TableCatalogStoreResult<Arc<VectorCatalogOperationGuard>> {
        if !matches!(
            expected.state,
            VectorMaintenanceResourceState::Active | VectorMaintenanceResourceState::Deleted
        ) {
            return Err(TableCatalogStoreError::Conflict("resource cannot prepare GC in its current state".into()));
        }
        let lifecycle = self
            .backend
            .acquire_bucket_lifecycle_read(self.maintenance_bucket(&expected.resource)?)
            .await?;
        let guard = self
            .retain_vector_resource_guard(expected.resource.clone(), expected.namespace.clone(), expected.name.clone(), lifecycle)
            .await?;
        let current = self.read_vector_maintenance_resource_with_guard(&guard).await?;
        if &current != expected {
            return Err(TableCatalogStoreError::Conflict("maintenance preparation resource changed".into()));
        }
        Ok(Arc::new(guard))
    }
    pub(crate) async fn read_vector_maintenance_authorization_with_operation_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceAuthorizationSnapshot> {
        self.validate_vector_operation_guard(guard)?;
        let result = self
            .maintenance_authorization_under_protection(guard.resource_identity(), guard.namespace(), guard.name())
            .await?;
        guard.check_held()?;
        Ok(result)
    }
    pub(crate) async fn read_vector_maintenance_history_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<Vec<CatalogCommittedVersion>> {
        let snapshot = self.read_vector_maintenance_resource_with_guard(guard).await?;
        let object = self.maintenance_read_ref(&snapshot.observed_object).await?;
        let entry = if snapshot.state == VectorMaintenanceResourceState::Active {
            serde_json::from_slice::<TableEntry>(&object.data).map_err(|e| maintenance_error(&e.to_string()))?
        } else if snapshot.state == VectorMaintenanceResourceState::Deleted {
            let intent: VectorIndexDeleteIntent =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            let bounded_logs = self
                .read_vector_maintenance_commit_logs_bounded(&intent.deleted_entry)
                .await?;
            object::verify_vector_deleted_history_with_logs(&intent, None, &bounded_logs)?;
            intent.deleted_entry.clone()
        } else {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance history requires a complete resource".into(),
            ));
        };
        let logs = self.read_vector_maintenance_commit_logs_bounded(&entry).await?;
        let result = vector_committed_history_versions(&entry, &logs)?;
        guard.check_held()?;
        Ok(result)
    }
    /// LIST discovers candidates; exact metadata reads bind each returned body identity.
    pub(crate) async fn list_vector_maintenance_object_candidates(
        &self,
        guard: &VectorCatalogOperationGuard,
        start_after: Option<&str>,
        limit: NonZeroU16,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorMaintenanceObjectPage> {
        self.validate_vector_operation_guard(guard)?;
        if limit.get() > 64 || start_after.is_some_and(|value| value.len() > 4096) {
            return Err(maintenance_error("invalid candidate page limit or cursor"));
        }
        let resource = guard.resource_identity();
        let prefix = resource.dataset_prefix();
        if start_after.is_some_and(|value| !value.starts_with(&prefix)) {
            return Err(maintenance_error("candidate cursor is outside resource"));
        }
        let mut after = start_after.map(str::to_owned);
        if !budget.try_start_call() {
            return Ok(VectorMaintenanceObjectPage {
                items: Vec::new(),
                next: Some(after.unwrap_or(prefix)),
            });
        }
        let page = self
            .backend
            .list_catalog_objects_page(resource.storage_bucket_id(), &prefix, after.as_deref(), limit.into())
            .await?;
        validate_maintenance_page(&page, &prefix, after.as_deref(), usize::from(limit.get()))?;
        let mut items = Vec::new();
        for candidate in page.objects {
            if candidate.delete_marker {
                return Err(maintenance_error("Vector dataset candidate has a delete marker"));
            }
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceObjectPage {
                    items,
                    next: Some(after.unwrap_or(prefix)),
                });
            }
            if let Some(metadata) = self
                .backend
                .object_metadata(resource.storage_bucket_id(), &candidate.key)
                .await?
            {
                if metadata.body_revision.is_none() {
                    return Err(maintenance_error("Vector candidate metadata lacks body revision"));
                }
                items.push((CatalogObjectKey::try_new(candidate.key.clone())?, metadata));
            }
            after = Some(candidate.key);
        }
        // Reserve the bounded resource revalidation/read calls before entering the shared helper.
        for _ in 0..3 {
            if !budget.try_start_call() {
                return Ok(VectorMaintenanceObjectPage {
                    items: Vec::new(),
                    next: Some(start_after.map(str::to_owned).unwrap_or_else(|| prefix.clone())),
                });
            }
        }
        let snapshot = self.read_vector_maintenance_resource_with_guard(guard).await?;
        if let Some(binding) = &snapshot.maintenance {
            self.visit_vector_retirements_with_budget(&snapshot.resource, binding, Some(budget), |auth| {
                items.retain(|(key, metadata)| {
                    !auth
                        .targets
                        .iter()
                        .any(|target| target.object_key == key.as_str() && metadata.body_revision == Some(target.body_revision))
                });
                Ok(())
            })
            .await?;
        }
        guard.check_held()?;
        Ok(VectorMaintenanceObjectPage {
            items,
            next: if page.is_truncated { after } else { None },
        })
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn maintenance_prior_dispatch_jobs(
        &self,
        job: &VectorMaintenanceJobV1,
    ) -> TableCatalogStoreResult<BTreeSet<Uuid>> {
        let mut ids = BTreeSet::new();
        let Some(source) = &job.input.source else {
            return Ok(ids);
        };
        match source.kind {
            VectorMaintenanceSourceKind::MaintenanceJob => {
                let object = self
                    .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
                    .await?;
                let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
                if original.resource != job.resource || original.job_id == job.job_id {
                    return Err(TableCatalogStoreError::Conflict("maintenance recovery original job mismatch".into()));
                }
                ids.insert(original.job_id);
            }
            VectorMaintenanceSourceKind::RetirementAuthorization => {
                let object = self
                    .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
                    .await?;
                let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
                if auth.resource != job.resource {
                    return Err(TableCatalogStoreError::Conflict(
                        "maintenance recovery original retirement mismatch".into(),
                    ));
                }
                ids.insert(auth.created_by_job_id);
                let path = retirement_progress_path(&self.paths, &job.resource.identity()?, auth.retirement_id)?;
                if let Some(object) = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                    .await?
                {
                    let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
                    if progress.resource != auth.resource
                        || progress.retirement_id != auth.retirement_id
                        || progress.targets.len() != auth.targets.len()
                    {
                        return Err(TableCatalogStoreError::Conflict("maintenance recovery progress mismatch".into()));
                    }
                    for target in &progress.targets {
                        if !matches!(
                            target.state,
                            VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
                        ) && let Some(dispatch) = &target.dispatch
                        {
                            ids.insert(dispatch.job_id);
                        }
                    }
                }
            }
            _ => {}
        }
        ids.remove(&job.job_id);
        if ids.len() > 257 {
            return Err(TableCatalogStoreError::Unavailable("CapacityExceeded: prior dispatch owners".into()));
        }
        Ok(ids)
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    #[cfg(test)]
    pub(crate) async fn recover_vector_maintenance_job(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        original: &VectorMaintenanceJobSnapshot,
        recovery: PreparedVectorMaintenanceRecovery,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        self.recover_vector_maintenance_job_protected(guard, original, recovery, gc)
            .await
    }
    pub(crate) async fn recover_vector_maintenance_job_with_operation_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        operation: &VectorCatalogOperationGuard,
        original: &VectorMaintenanceJobSnapshot,
        recovery: PreparedVectorMaintenanceRecovery,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        self.validate_vector_operation_guard(operation)?;
        if operation.resource_identity() != guard.lease().resource()
            || !Arc::ptr_eq(&operation.lifecycle_guard()?, &guard.lifecycle_owner())
        {
            return Err(TableCatalogStoreError::Conflict(
                "recovery operation does not share its dispatch lifecycle owner".into(),
            ));
        }
        let gc = TableCatalogLockGuard {
            _guard: Box::new(operation.clone()),
            lifecycle_guard: None,
            namespace_read_guard: None,
            lock_lost: operation.inner.gc.lock().lock_lost_signal(),
            write_commit_guard: None,
        };
        let result = self
            .recover_vector_maintenance_job_protected(guard, original, recovery, gc)
            .await?;
        operation.check_held()?;
        Ok(result)
    }
    async fn recover_vector_maintenance_job_protected(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        original: &VectorMaintenanceJobSnapshot,
        recovery: PreparedVectorMaintenanceRecovery,
        gc: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        self.maintenance_dispatch_matches(guard)?;
        let lease = guard.lease();
        let current_recovery = self.read_vector_maintenance_job_with_guard(guard).await?;
        if current_recovery.job.kind != VectorMaintenanceKind::CommitRecovery
            || current_recovery.job.state != VectorMaintenanceJobState::Running
            || !recovery.recorded_by.matches(&current_recovery.job)?
            || !guard.inner.prior_dispatch_jobs.contains(&original.job.job_id)
        {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance recovery lacks exact original execution protection".into(),
            ));
        }
        let source = current_recovery
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("maintenance recovery source missing"))?;
        if source.kind != VectorMaintenanceSourceKind::MaintenanceJob
            || CatalogMetadataRef::try_from(&source.reference)? != recovery.original_job_ref
        {
            return Err(TableCatalogStoreError::Conflict("maintenance recovery source differs".into()));
        }
        let source_object = self.maintenance_read_ref(&recovery.original_job_ref).await?;
        if decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&source_object.data)? != original.job
            || original.job.resource != current_recovery.job.resource
        {
            return Err(TableCatalogStoreError::Conflict("maintenance recovery original snapshot changed".into()));
        }
        let original_lease = VectorMaintenanceLeaseToken::from_job(&original.job)?;
        if recovery.stages.iter().any(|stage| {
            !matches!(
                stage.outcome,
                VectorMaintenanceStageOutcome::Committed | VectorMaintenanceStageOutcome::Superseded
            )
        }) && recovery.owned_completion.is_none()
        {
            return Err(TableCatalogStoreError::Conflict(
                "negative stage recovery requires actual original owner completion".into(),
            ));
        }
        if recovery
            .owned_completion
            .as_ref()
            .is_some_and(|completion| !completion.matches(&original_lease))
        {
            return Err(TableCatalogStoreError::Conflict(
                "recovery completion belongs to another original owner".into(),
            ));
        }
        let namespace = Namespace::parse(&original.job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        let name = CatalogName::parse(&original.job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let resource = self
            .maintenance_resource_under_protection(&lease.resource, &namespace, &name)
            .await?;
        let object = self.maintenance_read_ref(&resource.observed_object).await?;
        let entry = match resource.state {
            VectorMaintenanceResourceState::Active => {
                serde_json::from_slice::<TableEntry>(&object.data).map_err(|e| maintenance_error(&e.to_string()))?
            }
            VectorMaintenanceResourceState::Deleted => {
                let intent: VectorIndexDeleteIntent =
                    serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                self.verify_vector_deleted_history(&intent, None).await?;
                intent.deleted_entry.clone()
            }
            _ => return Err(TableCatalogStoreError::Conflict("maintenance recovery resource is incomplete".into())),
        };
        let logs = self
            .read_table_commit_logs(&entry)
            .await?
            .into_iter()
            .map(|(_, log)| log)
            .collect::<Vec<_>>();
        vector_committed_history_versions(&entry, &logs)?;
        let history = TableCommitHistoryIndex::new(&entry, logs.iter());
        let names: &[&str] = match original.job.kind {
            VectorMaintenanceKind::AnnBuild => &["ann-build"],
            VectorMaintenanceKind::Compaction => &["reserve-fragments", "rewrite"],
            _ => return Err(maintenance_error("maintenance job recovery requires an engine task")),
        };
        if recovery.stages.iter().map(|stage| stage.stage.as_str()).collect::<Vec<_>>() != names {
            return Err(maintenance_error("recovery omits an exact engine stage"));
        }
        let absent_stages = recovery
            .stages
            .iter()
            .filter(|stage| stage.outcome == VectorMaintenanceStageOutcome::NotPrepared)
            .map(|stage| stage.stage.as_str())
            .collect::<BTreeSet<_>>();
        if recovery
            .absent_candidates
            .iter()
            .map(|candidate| candidate.stage.as_str())
            .collect::<BTreeSet<_>>()
            != absent_stages
            || recovery.absent_candidates.len() != absent_stages.len()
        {
            return Err(maintenance_error("NotPrepared recovery lacks exact candidate absence evidence"));
        }
        let mut expected = Some(
            original
                .job
                .input
                .base_head
                .clone()
                .ok_or_else(|| maintenance_error("engine recovery base missing"))?,
        );
        let mut commit_refs = Vec::new();
        let mut published = Vec::new();
        for stage in &recovery.stages {
            stage.validate()?;
            if stage.commit_id
                != vector_maintenance_stage_commit_id(original.job.job_id, original.job.attempt, &stage.stage)?.as_str()
                || stage.expected_head != expected
            {
                return Err(TableCatalogStoreError::Conflict("recovery stage identity or predecessor changed".into()));
            }
            let matching = logs.iter().filter(|log| log.commit_id == stage.commit_id).collect::<Vec<_>>();
            match stage.outcome {
                VectorMaintenanceStageOutcome::NotPrepared => {
                    if !matching.is_empty() {
                        return Err(TableCatalogStoreError::Conflict("NotPrepared stage has Catalog history".into()));
                    }
                    let candidate = recovery
                        .absent_candidates
                        .iter()
                        .find(|candidate| candidate.stage == stage.stage)
                        .ok_or_else(|| maintenance_error("missing absent stage candidate"))?;
                    if !candidate.object_key.as_str().starts_with(&lease.resource.dataset_prefix())
                        || self
                            .backend
                            .object_metadata(lease.resource.storage_bucket_id(), candidate.object_key.as_str())
                            .await?
                            .is_some()
                    {
                        return Err(TableCatalogStoreError::Conflict(
                            "NotPrepared candidate exists or differs from resource".into(),
                        ));
                    }
                    published.push(false);
                    expected = None;
                }
                VectorMaintenanceStageOutcome::Committed
                | VectorMaintenanceStageOutcome::Superseded
                | VectorMaintenanceStageOutcome::Unpublished => {
                    let [log] = matching.as_slice() else {
                        return Err(maintenance_error("recovery has no unique stage log"));
                    };
                    let expected_head = expected
                        .as_ref()
                        .ok_or_else(|| maintenance_error("resolved stage has no prepared predecessor"))?;
                    let binding = log
                        .vector_commit_binding
                        .as_ref()
                        .ok_or_else(|| maintenance_error("stage log has no Vector binding"))?;
                    if binding.expected_generation != expected_head.generation
                        || log.expected_version_token != expected_head.version_token
                        || binding.expected_metadata.as_ref() != Some(&expected_head.binding.metadata)
                        || binding.expected_descriptor.as_ref() != Some(&expected_head.binding.descriptor)
                        || stage.candidate.as_ref() != Some(&binding.candidate_metadata)
                    {
                        return Err(TableCatalogStoreError::Conflict(
                            "recovery stage log does not match exact expected head".into(),
                        ));
                    }
                    let reference = stage
                        .commit_ref
                        .as_ref()
                        .ok_or_else(|| maintenance_error("resolved recovery stage lacks log copy"))?;
                    let copied = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let copied_log: CommitLogEntry =
                        serde_json::from_slice(&copied.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    if &copied_log != *log {
                        return Err(TableCatalogStoreError::Conflict("recovery log copy changed".into()));
                    }
                    let candidate = CatalogMetadataRef::try_from(&binding.candidate_metadata)?;
                    let descriptor = CatalogFormatDescriptor::try_from(&binding.descriptor)?;
                    if descriptor.metadata() != &candidate {
                        return Err(maintenance_error("recovery candidate descriptor mismatch"));
                    }
                    let limit = usize::try_from(candidate.content_length())
                        .ok()
                        .and_then(|size| size.checked_add(1))
                        .ok_or_else(|| maintenance_error("recovery candidate size overflow"))?;
                    let mut lifecycle = self
                        .backend
                        .reuse_bucket_lifecycle_read(self.maintenance_bucket(&lease.resource)?, guard.lifecycle_owner())
                        .await?;
                    let bytes = self
                        .backend
                        .read_managed_candidate(self.maintenance_bucket(&lease.resource)?, &candidate, limit, &mut lifecycle)
                        .await?
                        .ok_or_else(|| TableCatalogStoreError::NotFound("exact recovery candidate".into()))?;
                    candidate.verify_read(&bytes)?;
                    let committed = history.proves_committed(log);
                    if stage.outcome == VectorMaintenanceStageOutcome::Unpublished {
                        if committed || log.status != CommitLogStatus::Failed {
                            return Err(TableCatalogStoreError::Conflict("unpublished stage remains unresolved".into()));
                        }
                    } else if !committed {
                        return Err(TableCatalogStoreError::Conflict("stage publication has no complete history proof".into()));
                    }
                    if committed {
                        let mut next_head = expected_head.clone();
                        next_head.version_token = log.new_version_token.clone();
                        next_head.generation = binding
                            .expected_generation
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("stage generation overflow"))?;
                        next_head.binding.metadata = binding.candidate_metadata.clone();
                        next_head.binding.descriptor = binding.descriptor.clone();
                        next_head.entry_version = log.version;
                        expected = Some(next_head);
                    } else {
                        expected = None;
                    }
                    commit_refs.push(reference.clone());
                    published.push(committed);
                }
                VectorMaintenanceStageOutcome::Unknown => {
                    return Err(TableCatalogStoreError::Conflict("recovery retains unknown stage effects".into()));
                }
            }
        }
        commit_refs.sort_by(|a, b| a.object_key.cmp(&b.object_key));
        commit_refs.dedup();
        let mut next = original.job.clone();
        let now = OffsetDateTime::now_utc();
        next.updated_at = maintenance_format_time(now);
        let all_published = published.iter().all(|value| *value);
        let partial = published.as_slice() == [true, false];
        if published.iter().any(|value| *value) && !all_published && !partial {
            return Err(maintenance_error("unsupported partial maintenance publication"));
        }
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal()]);
        if all_published || partial {
            let effect = if partial {
                VectorMaintenanceResultEffect::PartialRecovered
            } else {
                VectorMaintenanceResultEffect::Published
            };
            let result = VectorMaintenanceResultV1 {
                version: 1,
                job_id: original.job.job_id,
                recorded_by_job_id: lease.job_id,
                attempt: original.job.attempt,
                resource: original.job.resource.clone(),
                finished_at: next.updated_at.clone(),
                effect,
                stages: recovery.stages,
                commit_refs,
                retirement_refs: Vec::new(),
                output_refs: Vec::new(),
                reason: None,
            };
            let bytes = encode_vector_maintenance_record(&result)?;
            let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
            let path = format!(
                "{}results/{}/attempt-{}/{sha}.json",
                self.paths.vector_maintenance_prefix(&lease.resource)?,
                table_catalog_path_hash(&original.job.job_id.to_string()),
                original.job.attempt
            );
            let reference = self.maintenance_write_immutable(&lease.resource, path, bytes, &fence).await?;
            next.state = if partial {
                VectorMaintenanceJobState::PartialRecovered
            } else {
                VectorMaintenanceJobState::Succeeded
            };
            next.result_ref = Some((&reference).into());
            next.finished_at = Some(next.updated_at.clone());
            next.next_retry_at = None;
            next.failure = if partial {
                Some(VectorMaintenanceFailureCode::PartialPublication)
            } else {
                None
            };
        } else if next.attempt < next.policy.max_attempts {
            next.state = VectorMaintenanceJobState::RetryScheduled;
            next.failure = Some(VectorMaintenanceFailureCode::Interrupted);
            let seconds = next
                .policy
                .retry_initial_backoff_seconds
                .saturating_mul(1u64 << (next.attempt - 1))
                .min(next.policy.retry_max_backoff_seconds);
            next.next_retry_at = Some(maintenance_format_time(now + Duration::seconds(seconds as i64)));
            next.finished_at = None;
            next.result_ref = None;
        } else {
            next.state = VectorMaintenanceJobState::Failed;
            next.failure = Some(VectorMaintenanceFailureCode::RetryExhausted);
            next.finished_at = Some(next.updated_at.clone());
            next.next_retry_at = None;
            next.result_ref = None;
        }
        let payload = serde_json::json!({"recorded_by":lease,"state":next.state,"result_ref":next.result_ref,"source_ref":CatalogMetadataRefRecordV1::from(&recovery.original_job_ref)});
        maintenance_stamp(&mut next, "RECOVER", Some(original.job.revision), payload)?;
        let path = self.paths.vector_maintenance_job_path(&lease.resource, original.job.job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        fence.extend_signals([lock.lock_lost_signal()]);
        let observed = self
            .maintenance_read_job(&lease.resource, original.job.job_id, true)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("original maintenance job".into()))?;
        if observed != *original {
            return Err(TableCatalogStoreError::Conflict("original job changed during recovery".into()));
        }
        self.maintenance_write_job(original, &next, lock.write_commit_guards(), &fence)
            .await
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog::store) async fn filter_vector_retired_versions(
        &self,
        entry: &TableEntry,
        mut versions: Vec<CatalogCommittedVersion>,
    ) -> TableCatalogStoreResult<Vec<CatalogCommittedVersion>> {
        let Some(binding) = &entry.vector_maintenance_binding else {
            return Ok(versions);
        };
        let resource = CatalogHead::from_vector_entry(entry)?.resource_identity().clone();
        self.visit_vector_retirements(&resource, binding, |authorization| {
            versions.retain(|version| {
                !authorization.targets.iter().any(|target| {
                    target.object_key == version.metadata().object_key().as_str()
                        && Some(target.body_revision) == version.metadata().body_revision().map(|body| body.as_uuid())
                })
            });
            Ok(())
        })
        .await?;
        Ok(versions)
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn read_vector_maintenance_entry_with_operation_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        let snapshot = self.read_vector_maintenance_resource_with_guard(guard).await?;
        let object = self.maintenance_read_ref(&snapshot.observed_object).await?;
        let entry = match snapshot.state {
            VectorMaintenanceResourceState::Active => {
                serde_json::from_slice::<TableEntry>(&object.data).map_err(|e| maintenance_error(&e.to_string()))?
            }
            VectorMaintenanceResourceState::Deleted => {
                let intent: VectorIndexDeleteIntent =
                    serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                let bounded_logs = self
                    .read_vector_maintenance_commit_logs_bounded(&intent.deleted_entry)
                    .await?;
                object::verify_vector_deleted_history_with_logs(&intent, None, &bounded_logs)?;
                intent.deleted_entry.clone()
            }
            _ => {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance entry requires a current or terminal resource".into(),
                ));
            }
        };
        let entry = VectorCatalogEntry::from_entry(entry)?;
        if entry.head().resource_identity() != guard.resource_identity() {
            return Err(TableCatalogStoreError::Conflict("maintenance entry identity changed".into()));
        }
        guard.check_held()?;
        Ok(entry)
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn maintenance_verify_control_result(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        job: &VectorMaintenanceJobV1,
        result: &VectorMaintenanceResultV1,
    ) -> TableCatalogStoreResult<()> {
        if result.effect == VectorMaintenanceResultEffect::Retired || !result.retirement_refs.is_empty() {
            if !matches!(
                job.kind,
                VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc | VectorMaintenanceKind::DeleteRecovery
            ) {
                return Err(maintenance_error("task kind cannot complete retirement"));
            }
            for reference in &result.retirement_refs {
                let (auth, _) = self
                    .retirement_authority(guard, &CatalogMetadataRef::try_from(reference)?)
                    .await?;
                let mut complete = false;
                for reference in &result.output_refs {
                    let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
                    if progress.retirement_id == auth.retirement_id
                        && progress.resource == auth.resource
                        && progress.targets.len() == auth.targets.len()
                        && progress.state == VectorRetirementProgressState::Completed
                    {
                        complete = true;
                    }
                }
                if !complete {
                    return Err(TableCatalogStoreError::Conflict("retirement result lacks exact complete progress".into()));
                }
            }
        }
        if result.effect != VectorMaintenanceResultEffect::Recovered {
            return Ok(());
        }
        let source = job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("recovery completion source missing"))?;
        let source_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let mut resolved = false;
        match source.kind {
            VectorMaintenanceSourceKind::CreateIntent => {
                let original: VectorIndexCreateIntentV1 =
                    serde_json::from_slice(&source_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                for reference in &result.output_refs {
                    let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let current: VectorIndexCreateIntentV1 =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    current.validate()?;
                    if current.create_id == original.create_id
                        && current.request_sha256 == original.request_sha256
                        && current.resource_identity()? == guard.lease().resource
                        && current.phase == VectorIndexCreatePhase::Published
                    {
                        let snapshot = self
                            .maintenance_resource_under_protection(
                                &guard.lease().resource,
                                &Namespace::parse(&current.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                                &CatalogName::parse(&current.name).map_err(|e| maintenance_error(&e.to_string()))?,
                            )
                            .await?;
                        resolved = snapshot.state == VectorMaintenanceResourceState::Active;
                    }
                }
            }
            VectorMaintenanceSourceKind::DeleteIntent => {
                let original: VectorIndexDeleteIntent =
                    serde_json::from_slice(&source_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                for reference in &result.output_refs {
                    let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let current: VectorIndexDeleteIntent =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    if current.delete_id == original.delete_id
                        && current.request_sha256 == original.request_sha256
                        && matches!(current.phase, VectorIndexDeletePhase::LogicallyDeleted | VectorIndexDeletePhase::Complete)
                    {
                        self.verify_vector_deleted_history(&current, None).await?;
                        resolved = true;
                    }
                }
            }
            VectorMaintenanceSourceKind::MaintenanceJob => {
                let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&source_object.data)?;
                for reference in &result.output_refs {
                    let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let current = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
                    if current.job_id == original.job_id
                        && current.spec() == original.spec()
                        && matches!(
                            current.state,
                            VectorMaintenanceJobState::Succeeded
                                | VectorMaintenanceJobState::PartialRecovered
                                | VectorMaintenanceJobState::Failed
                                | VectorMaintenanceJobState::RetryScheduled
                        )
                    {
                        let actual = self
                            .maintenance_read_job(&guard.lease().resource, current.job_id, false)
                            .await?
                            .ok_or_else(|| TableCatalogStoreError::NotFound("recovered original job".into()))?;
                        resolved = actual.job == current;
                    }
                }
            }
            VectorMaintenanceSourceKind::RetirementAuthorization => {
                let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&source_object.data)?;
                let path = format!(
                    "{}authorizations/{}.json",
                    self.paths.vector_maintenance_prefix(&guard.lease().resource)?,
                    table_catalog_path_hash(&auth.retirement_id.to_string())
                );
                resolved = result.retirement_refs.iter().any(|reference| reference.object_key == path);
            }
            VectorMaintenanceSourceKind::CommitLog => {
                let original: CommitLogEntry =
                    serde_json::from_slice(&source_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                let snapshot = self
                    .maintenance_resource_under_protection(
                        &guard.lease().resource,
                        &Namespace::parse(&job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                        &CatalogName::parse(&job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?,
                    )
                    .await?;
                let object = self.maintenance_read_ref(&snapshot.observed_object).await?;
                let entry: TableEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                let logs = self
                    .read_table_commit_logs(&entry)
                    .await?
                    .into_iter()
                    .map(|(_, log)| log)
                    .collect::<Vec<_>>();
                let history = TableCommitHistoryIndex::new(&entry, logs.iter());
                for reference in &result.commit_refs {
                    let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
                    let log: CommitLogEntry =
                        serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                    self.paths.validate_commit_entry(
                        &log,
                        guard.lease().resource.storage_bucket_id(),
                        guard.lease().resource.table_id().as_str(),
                    )?;
                    if log.commit_id == original.commit_id
                        && (history.proves_committed(&log)
                            || logs
                                .iter()
                                .any(|current| current == &log && current.status == CommitLogStatus::Failed))
                    {
                        resolved = true;
                    }
                }
            }
        }
        if !resolved {
            return Err(TableCatalogStoreError::Conflict("recovery result has no exact resolved source".into()));
        }
        Ok(())
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn read_vector_maintenance_candidate_roots_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<Vec<CatalogCommittedVersion>> {
        let entry = self.read_vector_maintenance_entry_with_operation_guard(guard).await?;
        let logs = self.read_vector_maintenance_commit_logs_bounded(&entry.entry).await?;
        vector_committed_history_versions(&entry.entry, &logs)?;
        let history = TableCommitHistoryIndex::new(&entry.entry, logs.iter());
        let mut roots = Vec::new();
        for log in &logs {
            if log.status == CommitLogStatus::Staged
                || (log.status == CommitLogStatus::Committed && !history.proves_committed(log))
            {
                return Err(TableCatalogStoreError::Conflict(
                    "NeedsRecovery: Catalog has an unresolved candidate".into(),
                ));
            }
            if log.status == CommitLogStatus::Failed {
                let binding = log
                    .vector_commit_binding
                    .as_ref()
                    .ok_or_else(|| maintenance_error("failed Vector commit lacks exact candidate binding"))?;
                roots.push(CatalogCommittedVersion::new(
                    CatalogMetadataRef::try_from(&binding.candidate_metadata)?,
                    CatalogFormatDescriptor::try_from(&binding.descriptor)?,
                    Some(CatalogCommitId::try_new(log.commit_id.clone())?),
                )?);
            }
        }
        guard.check_held()?;
        Ok(roots)
    }
    async fn verify_retirement_intent_copy(
        &self,
        snapshot: &VectorMaintenanceResourceSnapshot,
        copy: Option<&CatalogMetadataRefRecordV1>,
    ) -> TableCatalogStoreResult<()> {
        match (&snapshot.source_intent, copy) {
            (None, None) => Ok(()),
            (Some(current), Some(copy)) => {
                if !copy
                    .object_key
                    .starts_with(&format!("{}sources/", self.paths.vector_maintenance_prefix(&snapshot.resource)?))
                {
                    return Err(maintenance_error("retirement intent evidence requires immutable source copy"));
                }
                let current = self.maintenance_read_ref(current).await?;
                let copy = self.maintenance_read_ref(&CatalogMetadataRef::try_from(copy)?).await?;
                if current.data != copy.data {
                    return Err(TableCatalogStoreError::Conflict(
                        "retirement intent source copy differs from current authority".into(),
                    ));
                }
                Ok(())
            }
            _ => Err(maintenance_error("retirement intent evidence presence mismatch")),
        }
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn read_prepared_vector_retirement_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        plan_ref: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<PreparedVectorRetirement> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if !matches!(job.job.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
            || job.job.input.plan_ref.as_ref() != Some(&CatalogMetadataRefRecordV1::from(plan_ref))
        {
            return Err(TableCatalogStoreError::Conflict("task does not own retirement plan".into()));
        }
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        let object = self.maintenance_read_ref(plan_ref).await?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&object.data)?;
        if plan.resource != job.job.resource {
            return Err(TableCatalogStoreError::Conflict("retirement plan belongs to another resource".into()));
        }
        let snapshot = self
            .maintenance_resource_under_protection(
                &guard.lease().resource,
                &Namespace::parse(&job.job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&job.job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?,
            )
            .await?;
        let prepared = PreparedVectorRetirement::try_from_validated_parts(
            guard.lease().resource.clone(),
            snapshot,
            plan_ref.clone(),
            CatalogMetadataRef::try_from(&plan.evidence_ref)?,
            plan.targets,
        )?;
        self.verify_retirement_plan(&prepared, &prepared.base_snapshot).await?;
        if gc.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("retirement preparation protection lost".into()));
        }
        guard.check_held()?;
        Ok(prepared)
    }
    pub(crate) async fn read_vector_maintenance_original_job_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<(VectorMaintenanceJobSnapshot, VectorMaintenanceLeaseToken)> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("original job source missing"))?;
        if job.job.kind != VectorMaintenanceKind::CommitRecovery || source.kind != VectorMaintenanceSourceKind::MaintenanceJob {
            return Err(maintenance_error("task does not recover a maintenance job"));
        }
        let object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
        if original.resource != job.job.resource || !guard.inner.prior_dispatch_jobs.contains(&original.job_id) {
            return Err(TableCatalogStoreError::Conflict("original job dispatch identity mismatch".into()));
        }
        let current = self
            .maintenance_read_job(&guard.lease().resource, original.job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("original maintenance job".into()))?;
        if original != current.job {
            return Err(TableCatalogStoreError::Conflict("original maintenance snapshot changed".into()));
        }
        let lease = VectorMaintenanceLeaseToken::from_job(&original)?;
        guard.check_held()?;
        Ok((current, lease))
    }
    pub(crate) async fn copy_vector_maintenance_completed_source_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("completed recovery source missing"))?;
        let source_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let resource = guard.lease().resource();
        let path = match source.kind {
            VectorMaintenanceSourceKind::CreateIntent => {
                let intent: VectorIndexCreateIntentV1 =
                    serde_json::from_slice(&source_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                self.paths.vector_create_intent_path(
                    resource.storage_bucket_id(),
                    &Namespace::parse(&intent.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                    &CatalogName::parse(&intent.name).map_err(|e| maintenance_error(&e.to_string()))?,
                )
            }
            VectorMaintenanceSourceKind::DeleteIntent => self
                .paths
                .vector_delete_intent_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            VectorMaintenanceSourceKind::MaintenanceJob => {
                let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&source_object.data)?;
                self.paths.vector_maintenance_job_path(resource, original.job_id)?
            }
            VectorMaintenanceSourceKind::RetirementAuthorization => {
                let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&source_object.data)?;
                retirement_progress_path(&self.paths, resource, auth.retirement_id)?
            }
            VectorMaintenanceSourceKind::CommitLog => {
                let log: CommitLogEntry =
                    serde_json::from_slice(&source_object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                self.paths
                    .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), &log.commit_id)
            }
        };
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("completed recovery source".into()))?;
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        self.copy_vector_maintenance_source_with_guard(guard, &reference).await
    }
}

pub(crate) fn vector_retirement_target_is_old_enough(
    target: &VectorRetirementTargetV1,
    retention: VectorHistoryRetentionV1,
    now: OffsetDateTime,
) -> TableCatalogStoreResult<bool> {
    let minimum = match retention {
        VectorHistoryRetentionV1::All => TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
        VectorHistoryRetentionV1::Last { min_age_seconds, .. } => i64::try_from(min_age_seconds)
            .map_err(|_| maintenance_error("retention age overflow"))?
            .max(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS),
    };
    let Some(time) = &target.mod_time else {
        return Ok(false);
    };
    Ok(time.to_time()? <= now - Duration::seconds(minimum))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorRetirementRecoveryRequest {
    resource: CatalogResourceIdentity,
    authorization_ref: CatalogMetadataRef,
    retirement_id: Uuid,
    retirement_revision: Uuid,
    progress_revision: Uuid,
    ordinal: u16,
    target: VectorRetirementTargetV1,
    prior_dispatch: Option<VectorRetirementDispatchV1>,
}
impl VectorRetirementRecoveryRequest {
    permit_getters! { resource: CatalogResourceIdentity, authorization_ref: CatalogMetadataRef, target: VectorRetirementTargetV1, prior_dispatch: Option<VectorRetirementDispatchV1> }
}
pub(crate) struct VectorRetirementReadmissionEvidence {
    request: VectorRetirementRecoveryRequest,
    observed: Option<TableCatalogObjectMetadata>,
    completion: Option<VectorMaintenanceOwnedCompletion>,
}
impl VectorRetirementReadmissionEvidence {
    /// Construct after the fixed runtime has released all application owners and completed exact native recovery.
    pub(crate) fn try_from_native_recovery(
        request: VectorRetirementRecoveryRequest,
        counts: [u64; 3],
        observed: Option<TableCatalogObjectMetadata>,
        completion: Option<VectorMaintenanceOwnedCompletion>,
    ) -> TableCatalogStoreResult<Self> {
        if counts != [0, 0, 0] {
            return Err(TableCatalogStoreError::Conflict("native delete recovery remains unresolved".into()));
        }
        if let Some(prior) = &request.prior_dispatch {
            let completed = completion
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Conflict("prior native owner completion is unknown".into()))?;
            if completed.resource != request.resource
                || completed.job_id != prior.job_id
                || completed.attempt != prior.attempt
                || completed.lease_id != prior.lease_id
                || completed.worker_boot_id != prior.worker_boot_id
                || completed.execution_identity != prior.execution_identity
                || !completed.completed_operation_ids.contains(&prior.dispatch_id)
                || completed.no_delete_dispatch
            {
                return Err(TableCatalogStoreError::Conflict(
                    "native completion differs from prior dispatch owner".into(),
                ));
            }
        }
        if let Some(metadata) = &observed
            && VectorRetirementTargetV1::from_object_metadata(
                &request.resource,
                CatalogObjectKey::try_new(request.target.object_key.clone())?,
                metadata,
            )? != request.target
        {
            return Err(TableCatalogStoreError::Conflict("native recovery observed a replacement body".into()));
        }
        Ok(Self {
            request,
            observed,
            completion,
        })
    }
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn prepare_vector_retirement_recovery(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<Vec<VectorRetirementRecoveryRequest>> {
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("retirement recovery source missing"))?;
        if job.job.kind != VectorMaintenanceKind::DeleteRecovery
            || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
        {
            return Err(maintenance_error("task does not recover retirement"));
        }
        let source_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&source_object.data)?;
        let path = format!(
            "{}authorizations/{}.json",
            self.paths.vector_maintenance_prefix(guard.lease().resource())?,
            table_catalog_path_hash(&auth.retirement_id.to_string())
        );
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement authorization".into()))?;
        let authorization = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)? != auth {
            return Err(TableCatalogStoreError::Conflict("retirement recovery source changed".into()));
        }
        let progress = self.read_vector_retirement_progress(guard, &authorization).await?;
        Ok(progress
            .targets
            .iter()
            .filter(|target| {
                !matches!(
                    target.state,
                    VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
                )
            })
            .map(|target| VectorRetirementRecoveryRequest {
                resource: guard.lease().resource.clone(),
                authorization_ref: authorization.clone(),
                retirement_id: auth.retirement_id,
                retirement_revision: auth.retirement_revision,
                progress_revision: progress.revision,
                ordinal: target.ordinal,
                target: auth.targets[usize::from(target.ordinal)].clone(),
                prior_dispatch: target.dispatch.clone(),
            })
            .collect())
    }
    pub(crate) async fn recover_vector_retirement_delete(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        expected_progress_revision: Uuid,
        dispatch_id: Uuid,
        evidence: VectorRetirementReadmissionEvidence,
        owner: &VectorMaintenanceRecoveryOwner,
    ) -> TableCatalogStoreResult<VectorRetirementDispatchAdmission> {
        owner.check_lease(guard.lease())?;
        self.maintenance_dispatch_matches(guard)?;
        let request = evidence.request;
        if dispatch_id.is_nil() || request.resource != guard.lease().resource {
            return Err(maintenance_error("retirement recovery dispatch identity mismatch"));
        }
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("retirement recovery source missing"))?;
        if job.job.kind != VectorMaintenanceKind::DeleteRecovery
            || job.job.state != VectorMaintenanceJobState::Running
            || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
        {
            return Err(TableCatalogStoreError::Conflict(
                "retirement readmission requires current recovery task".into(),
            ));
        }
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(request.resource.storage_bucket_id(), request.resource.table_id().as_str()),
            )
            .await?;
        let (auth, _) = self.retirement_authority(guard, &request.authorization_ref).await?;
        let source_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&source_object.data)? != auth
            || request.retirement_id != auth.retirement_id
            || request.retirement_revision != auth.retirement_revision
            || auth.targets.get(usize::from(request.ordinal)) != Some(&request.target)
        {
            return Err(TableCatalogStoreError::Conflict("retirement readmission authorization changed".into()));
        }
        if auth.created_by_job_id != guard.lease().job_id && !guard.inner.prior_dispatch_jobs.contains(&auth.created_by_job_id) {
            return Err(TableCatalogStoreError::Conflict(
                "retirement readmission lacks creator dispatch owner".into(),
            ));
        }
        if let Some(prior) = &request.prior_dispatch {
            if prior.job_id != guard.lease().job_id && !guard.inner.prior_dispatch_jobs.contains(&prior.job_id) {
                return Err(TableCatalogStoreError::Conflict(
                    "retirement readmission lacks prior dispatch owner".into(),
                ));
            }
            if evidence.completion.is_none() {
                return Err(TableCatalogStoreError::Conflict("prior native completion remains unknown".into()));
            }
        }
        let observed = self
            .backend
            .object_metadata(request.resource.storage_bucket_id(), &request.target.object_key)
            .await?;
        if observed != evidence.observed {
            return Err(TableCatalogStoreError::Conflict("retirement target changed after native recovery".into()));
        }
        let path = retirement_progress_path(&self.paths, &request.resource, request.retirement_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let mut fence = retirement_fence(guard);
        fence.extend_signals([gc.lock_lost_signal(), lock.lock_lost_signal()]);
        #[cfg(test)]
        self.check_retirement_readmission_failure(guard, &path, VectorRetirementReadmissionFailurePoint::BeforeProgressRead)?;
        let object = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement progress".into()))?;
        let mut progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
        if progress.resource != auth.resource
            || progress.authorization_ref != CatalogMetadataRefRecordV1::from(&request.authorization_ref)
            || progress.targets.len() != auth.targets.len()
        {
            return Err(TableCatalogStoreError::Conflict("retirement progress binding changed".into()));
        }
        let target = &progress.targets[usize::from(request.ordinal)];
        if matches!(
            target.state,
            VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
        ) {
            return Ok(VectorRetirementDispatchAdmission::Recorded);
        }
        if target.dispatch != request.prior_dispatch {
            return Err(TableCatalogStoreError::Conflict("retirement prior dispatch changed".into()));
        }
        if progress.revision != expected_progress_revision {
            return Err(TableCatalogStoreError::Conflict("retirement recovery progress revision changed".into()));
        }
        let expected_revision = expected_progress_revision;
        let dispatch = VectorRetirementDispatchV1 {
            dispatch_id,
            job_id: job.job.job_id,
            job_revision: job.job.revision,
            attempt: job.job.attempt,
            lease_id: guard.lease().lease_id,
            worker_id: guard.lease().worker_id.clone(),
            worker_boot_id: guard.lease().worker_boot_id,
            execution_identity: job.job.execution_identity.clone(),
            admitted_at: maintenance_now(),
        };
        let absent = observed.is_none();
        progress.targets[usize::from(request.ordinal)] = VectorRetirementTargetProgressV1 {
            ordinal: request.ordinal,
            state: if absent {
                VectorRetirementTargetState::AbsentAfterRecovery
            } else {
                VectorRetirementTargetState::InFlight
            },
            dispatch: Some(dispatch),
            receipt: if absent {
                Some(VectorRetirementReceiptV1 {
                    outcome: VectorRetirementReceiptOutcome::AbsentAfterRecovery,
                    completed_at: maintenance_now(),
                    accounting: None,
                    recovery_pending: 0,
                    recovery_blocked: 0,
                    recovery_failed: 0,
                })
            } else {
                None
            },
            failure: None,
        };
        progress.state = if progress.targets.iter().all(|target| {
            matches!(
                target.state,
                VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
            )
        }) {
            VectorRetirementProgressState::Completed
        } else {
            VectorRetirementProgressState::Deleting
        };
        stamp_progress(
            &mut progress,
            "RECOVER_DELETE",
            Some(expected_revision),
            guard.lease(),
            serde_json::json!({"original_progress_revision":request.progress_revision,"prior_dispatch":request.prior_dispatch,"ordinal":request.ordinal,"dispatch_id":dispatch_id,"absent":absent}),
        )?;
        self.write_retirement_progress(&path, object.etag, &progress, lock.write_commit_guards(), &fence, Some(owner))
            .await?;
        #[cfg(test)]
        self.check_retirement_readmission_failure(guard, &path, VectorRetirementReadmissionFailurePoint::AfterProgressWrite)?;
        if absent {
            return Ok(VectorRetirementDispatchAdmission::Recorded);
        }
        Ok(VectorRetirementDispatchAdmission::Ready(Box::new(VectorRetirementDispatchPermit {
            authorization_ref: request.authorization_ref,
            retirement_id: auth.retirement_id,
            retirement_revision: auth.retirement_revision,
            job_id: job.job.job_id,
            job_revision: job.job.revision,
            attempt: job.job.attempt,
            lease_id: guard.lease().lease_id,
            worker_id: guard.lease().worker_id.clone(),
            worker_boot_id: guard.lease().worker_boot_id,
            execution_identity: job.job.execution_identity,
            ordinal: request.ordinal,
            target: request.target,
            dispatch_id,
            guard: guard.clone(),
        })))
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Recover a lost response after this recovery attempt updated the original job.
    pub(crate) async fn read_vector_maintenance_completed_original_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<Option<CatalogMetadataRef>> {
        let recovery = self.read_vector_maintenance_job_with_guard(guard).await?;
        let source = recovery
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("original job recovery source missing"))?;
        if recovery.job.kind != VectorMaintenanceKind::CommitRecovery
            || source.kind != VectorMaintenanceSourceKind::MaintenanceJob
        {
            return Err(maintenance_error("task does not recover an original maintenance job"));
        }
        let object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
        let current = self
            .maintenance_read_job(&guard.lease().resource, original.job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("original maintenance job".into()))?;
        if current.job == original {
            return Ok(None);
        }
        if current.job.spec() != original.spec()
            || current.job.attempt != original.attempt
            || VectorMaintenanceLeaseToken::from_job(&current.job)? != VectorMaintenanceLeaseToken::from_job(&original)?
            || !matches!(
                current.job.state,
                VectorMaintenanceJobState::Succeeded
                    | VectorMaintenanceJobState::PartialRecovered
                    | VectorMaintenanceJobState::RetryScheduled
                    | VectorMaintenanceJobState::Failed
            )
        {
            return Err(TableCatalogStoreError::Conflict("original job changed outside this recovery".into()));
        }
        let payload = serde_json::json!({"recorded_by":guard.lease(),"state":current.job.state,"result_ref":current.job.result_ref,"source_ref":source.reference});
        let mut replay = current.job.clone();
        maintenance_stamp(&mut replay, "RECOVER", Some(original.revision), payload)?;
        if replay.last_update_id != current.job.last_update_id || replay.last_update_sha256 != current.job.last_update_sha256 {
            return Err(TableCatalogStoreError::Conflict(
                "original job was updated by another recovery attempt".into(),
            ));
        }
        if let Some(reference) = &current.job.result_ref {
            let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
            let result = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&object.data)?;
            if result.job_id != original.job_id
                || result.attempt != original.attempt
                || result.resource != original.resource
                || result.recorded_by_job_id != guard.lease().job_id
            {
                return Err(TableCatalogStoreError::Conflict("original job result binding changed".into()));
            }
        }
        Ok(Some(
            self.copy_vector_maintenance_source_with_guard(guard, current.observed_object())
                .await?,
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VectorRetirementDiscoveryCursor {
    resource: CatalogResourceIdentity,
    binding: Option<VectorMaintenanceBindingV1>,
    next: Option<CatalogMetadataRefRecordV1>,
    remaining: u32,
}
impl VectorRetirementDiscoveryCursor {
    #[cfg(test)]
    pub(crate) fn resource(&self) -> &CatalogResourceIdentity {
        &self.resource
    }
    pub(crate) fn binding(&self) -> Option<&VectorMaintenanceBindingV1> {
        self.binding.as_ref()
    }
    pub(crate) fn next(&self) -> Option<&CatalogMetadataRefRecordV1> {
        self.next.as_ref()
    }
}
#[derive(Debug, Clone)]
pub(crate) struct VectorRetirementAuthoritySnapshot {
    authorization_ref: CatalogMetadataRef,
    state: Option<VectorRetirementProgressState>,
}
impl VectorRetirementAuthoritySnapshot {
    permit_getters! { authorization_ref:CatalogMetadataRef,state:Option<VectorRetirementProgressState> }
}
pub(crate) struct VectorRetirementDiscoveryPage {
    items: Vec<VectorRetirementAuthoritySnapshot>,
    next: Option<VectorRetirementDiscoveryCursor>,
}
impl VectorRetirementDiscoveryPage {
    pub(crate) fn into_parts(self) -> (Vec<VectorRetirementAuthoritySnapshot>, Option<VectorRetirementDiscoveryCursor>) {
        (self.items, self.next)
    }
}
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn list_vector_retirement_authorities(
        &self,
        resource: &CatalogResourceIdentity,
        namespace: &Namespace,
        name: &CatalogName,
        cursor: Option<&VectorRetirementDiscoveryCursor>,
        limit: NonZeroU16,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorRetirementDiscoveryPage> {
        if limit.get() > 64 {
            return Err(maintenance_error("retirement discovery limit exceeds 64"));
        }
        if cursor.is_some_and(|cursor| cursor.resource != *resource) {
            return Err(TableCatalogStoreError::Conflict("retirement discovery resource changed".into()));
        }
        // The resource helper performs at most one handle check and two exact object reads.
        for _ in 0..3 {
            if !budget.try_start_call() {
                return Ok(VectorRetirementDiscoveryPage {
                    items: Vec::new(),
                    next: Some(cursor.cloned().unwrap_or_else(|| VectorRetirementDiscoveryCursor {
                        resource: resource.clone(),
                        binding: None,
                        next: None,
                        remaining: 0,
                    })),
                });
            }
        }
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
        let current = self.maintenance_resource_under_protection(resource, namespace, name).await?;
        let Some(binding) = current.maintenance else {
            return Ok(VectorRetirementDiscoveryPage {
                items: Vec::new(),
                next: None,
            });
        };
        let (mut next, mut remaining) = if let Some(cursor) = cursor.filter(|cursor| cursor.binding.is_some()) {
            if cursor.resource != *resource
                || cursor.binding.as_ref() != Some(&binding)
                || cursor.remaining > binding.retirement_count
            {
                return Err(TableCatalogStoreError::Conflict("retirement discovery authority changed".into()));
            }
            (cursor.next.clone(), cursor.remaining)
        } else {
            (binding.retirement_tip.clone(), binding.retirement_count)
        };
        let mut items = Vec::new();
        let mut seen = BTreeSet::new();
        while remaining > 0 && items.len() < usize::from(limit.get()) {
            let reference = next
                .as_ref()
                .ok_or_else(|| maintenance_error("retirement discovery chain is incomplete"))?
                .clone();
            if !budget.try_start_call() {
                break;
            }
            let exact = CatalogMetadataRef::try_from(&reference)?;
            let object = self.maintenance_read_ref(&exact).await?;
            let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
            if auth.resource.identity()? != *resource
                || exact.object_key().as_str()
                    != format!(
                        "{}authorizations/{}.json",
                        self.paths.vector_maintenance_prefix(resource)?,
                        table_catalog_path_hash(&auth.retirement_id.to_string())
                    )
                || !seen.insert(auth.retirement_id)
            {
                return Err(TableCatalogStoreError::Conflict("retirement discovery chain identity mismatch".into()));
            }
            if !budget.try_start_call() {
                break;
            }
            let path = retirement_progress_path(&self.paths, resource, auth.retirement_id)?;
            let state = if let Some(object) = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                .await?
            {
                let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
                if progress.authorization_ref != reference
                    || progress.resource != auth.resource
                    || progress.retirement_id != auth.retirement_id
                    || progress.targets.len() != auth.targets.len()
                {
                    return Err(TableCatalogStoreError::Conflict("retirement discovery progress mismatch".into()));
                }
                Some(progress.state)
            } else {
                None
            };
            items.push(VectorRetirementAuthoritySnapshot {
                authorization_ref: exact,
                state,
            });
            remaining -= 1;
            next = auth.previous_ref;
        }
        if remaining == 0 && next.is_some() {
            return Err(maintenance_error("retirement discovery exceeds chain count"));
        }
        if lifecycle.is_lock_lost() || gc.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("retirement discovery protection lost".into()));
        }
        let next = if remaining == 0 {
            None
        } else {
            Some(VectorRetirementDiscoveryCursor {
                resource: resource.clone(),
                binding: Some(binding),
                next: Some(next.ok_or_else(|| maintenance_error("retirement discovery missing continuation"))?),
                remaining,
            })
        };
        Ok(VectorRetirementDiscoveryPage { items, next })
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn copy_vector_retirement_completed_progress(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        authorization: &CatalogMetadataRef,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let progress = self.read_vector_retirement_progress(guard, authorization).await?;
        if progress.state != VectorRetirementProgressState::Completed {
            return Err(TableCatalogStoreError::Conflict("retirement progress is incomplete".into()));
        }
        let path = retirement_progress_path(&self.paths, guard.lease().resource(), progress.retirement_id)?;
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("completed retirement progress".into()))?;
        if decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)? != progress {
            return Err(TableCatalogStoreError::Conflict("completed retirement progress changed".into()));
        }
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        self.copy_vector_maintenance_source_with_guard(guard, &reference).await
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Resolves only published authority belonging to this job or its exact recovery source.
    pub(crate) async fn read_vector_retirement_authorization_with_guard(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<Option<CatalogMetadataRef>> {
        self.maintenance_dispatch_matches(guard)?;
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(guard.lease().resource.storage_bucket_id(), guard.lease().resource.table_id().as_str()),
            )
            .await?;
        let job = self.read_vector_maintenance_job_with_guard(guard).await?;
        if job.job.state != VectorMaintenanceJobState::Running {
            return Err(TableCatalogStoreError::Conflict("retirement lookup requires a running job".into()));
        }
        let expected = if job.job.kind == VectorMaintenanceKind::DeleteRecovery {
            let source = job
                .job
                .input
                .source
                .as_ref()
                .ok_or_else(|| maintenance_error("recovery source missing"))?;
            if source.kind != VectorMaintenanceSourceKind::RetirementAuthorization {
                return Ok(None);
            }
            let object = self
                .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
                .await?;
            Some(decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?)
        } else if matches!(job.job.kind, VectorMaintenanceKind::OrphanGc | VectorMaintenanceKind::HistoryGc) {
            None
        } else {
            return Err(maintenance_error("job does not consume retirement authority"));
        };
        let current = self
            .maintenance_resource_under_protection(
                &guard.lease().resource,
                &Namespace::parse(&job.job.resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&job.job.resource.name).map_err(|e| maintenance_error(&e.to_string()))?,
            )
            .await?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("MaintenanceEnrollmentRequired".into()))?;
        let mut found = None;
        self.visit_vector_retirements(&guard.lease().resource, binding, |auth| {
            let matches = if let Some(expected) = &expected {
                auth == expected
            } else {
                auth.created_by_job_id == job.job.job_id && Some(&auth.plan_ref) == job.job.input.plan_ref.as_ref()
            };
            if matches && found.replace(auth.clone()).is_some() {
                return Err(maintenance_error("job has duplicate retirement authority"));
            }
            Ok(())
        })
        .await?;
        if expected.is_some() && found.is_none() {
            return Err(TableCatalogStoreError::Conflict("recovery source is not published authority".into()));
        }
        let reference = if let Some(auth) = found {
            let path = format!(
                "{}authorizations/{}.json",
                self.paths.vector_maintenance_prefix(&guard.lease().resource)?,
                table_catalog_path_hash(&auth.retirement_id.to_string())
            );
            let object = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("published retirement authority".into()))?;
            if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)? != auth {
                return Err(TableCatalogStoreError::Conflict("published retirement authority changed".into()));
            }
            Some(CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?)
        } else {
            None
        };
        if gc.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("retirement lookup protection lost".into()));
        }
        guard.check_held()?;
        Ok(reference)
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Reads the immutable source and its exact current log without choosing a candidate by listing.
    pub(crate) async fn read_vector_maintenance_commit_source_with_operation_guard(
        &self,
        dispatch: &VectorMaintenanceDispatchGuard,
        operation: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<(CommitLogEntry, CatalogCommitParts)> {
        self.maintenance_dispatch_matches(dispatch)?;
        self.validate_vector_operation_guard(operation)?;
        if operation.resource_identity() != dispatch.lease().resource()
            || !Arc::ptr_eq(&operation.lifecycle_guard()?, &dispatch.lifecycle_owner())
        {
            return Err(maintenance_error("commit recovery operation owner differs"));
        }
        let job = self.read_vector_maintenance_job_with_guard(dispatch).await?;
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("commit recovery source missing"))?;
        if job.job.kind != VectorMaintenanceKind::CommitRecovery
            || job.job.state != VectorMaintenanceJobState::Running
            || source.kind != VectorMaintenanceSourceKind::CommitLog
        {
            return Err(maintenance_error("commit recovery requires its running CommitLog job"));
        }
        let original = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let original: CommitLogEntry = serde_json::from_slice(&original.data).map_err(|e| maintenance_error(&e.to_string()))?;
        let resource = dispatch.lease().resource();
        self.paths
            .validate_commit_entry(&original, resource.storage_bucket_id(), resource.table_id().as_str())?;
        let path =
            self.paths
                .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), &original.commit_id);
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("exact recovery commit log".into()))?;
        let current: CommitLogEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
        self.paths
            .validate_commit_entry(&current, resource.storage_bucket_id(), resource.table_id().as_str())?;
        if !commit_logs_share_recovery_payload(&current, &original) {
            return Err(TableCatalogStoreError::Conflict("recovery commit payload changed".into()));
        }
        let binding = current
            .vector_commit_binding
            .as_ref()
            .ok_or_else(|| maintenance_error("Vector commit binding missing"))?;
        let metadata = binding
            .expected_metadata
            .as_ref()
            .map(CatalogMetadataRef::try_from)
            .transpose()?;
        let descriptor = binding
            .expected_descriptor
            .as_ref()
            .map(CatalogFormatDescriptor::try_from)
            .transpose()?;
        let mut expected_entry = self
            .read_vector_maintenance_entry_with_operation_guard(operation)
            .await?
            .entry;
        expected_entry.version_token = current.expected_version_token.clone();
        expected_entry.generation = binding.expected_generation;
        expected_entry.metadata_location = current.previous_metadata_location.clone();
        let parts = CatalogCommitParts {
            resource_identity: resource.clone(),
            namespace: operation.namespace().clone(),
            name: operation.name().clone(),
            expected_head: CatalogExpectedHead::from_entry(&expected_entry, metadata, descriptor)?,
            commit_id: CatalogCommitId::try_new(current.commit_id.clone())?,
            operation: current.operation.clone(),
            idempotency_key: current.idempotency_key.clone(),
            writer: current.writer.clone(),
        };
        dispatch.check_held()?;
        operation.check_held()?;
        Ok((current, parts))
    }

    /// Finalizes a proven publication only. An unresolved log returns None and retains its candidate.
    pub(crate) async fn finalize_vector_maintenance_commit_with_operation_guard(
        &self,
        dispatch: &VectorMaintenanceDispatchGuard,
        operation: &VectorCatalogOperationGuard,
        prepared: PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<Option<CatalogMetadataRef>> {
        let store = self.clone();
        let dispatch = dispatch.clone();
        let operation = operation.clone();
        object::run_vector_catalog_mutation(async move {
            let (mut log, _) = store
                .read_vector_maintenance_commit_source_with_operation_guard(&dispatch, &operation)
                .await?;
            let prepared = match log.vector_resource_mutation.clone() {
                Some(VectorResourceMutationBinding::V1(mutation)) => prepared.with_vector_resource_mutation(mutation)?,
                Some(VectorResourceMutationBinding::V2(mutation)) => prepared.with_vector_resource_mutation_v2(*mutation)?,
                None => prepared,
            };
            if !commit_log_matches_prepared(&log, &prepared, operation.resource_identity().table_id().as_str()) {
                return Err(TableCatalogStoreError::Conflict("recovery preparation differs from exact commit".into()));
            }
            let entry = store
                .read_vector_maintenance_entry_with_operation_guard(&operation)
                .await?
                .entry;
            let logs = store.read_table_commit_logs(&entry).await?;
            vector_committed_history_versions(&entry, &logs.iter().map(|(_, log)| log.clone()).collect::<Vec<_>>())?;
            if log.status == CommitLogStatus::Failed
                || !TableCommitHistoryIndex::new(&entry, logs.iter().map(|(_, log)| log)).proves_committed(&log)
            {
                return Ok(None);
            }
            let candidate = prepared.candidate_metadata();
            let limit = usize::try_from(candidate.content_length())
                .ok()
                .and_then(|size| size.checked_add(1))
                .ok_or_else(|| maintenance_error("recovery candidate size overflow"))?;
            let mut lifecycle = operation.inner.lifecycle.lock().await;
            let bytes = store
                .backend
                .read_managed_candidate(store.maintenance_bucket(dispatch.lease().resource())?, candidate, limit, &mut lifecycle)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("exact committed candidate".into()))?;
            candidate.verify_read(&bytes)?;
            drop(lifecycle);
            if entry.version == 4 {
                store.validate_prepared_retirement_proof(&operation, &prepared, &entry)?;
            }
            let path = store
                .paths
                .commit_log_entry_path(&entry.table_bucket, &entry.table_id, &log.commit_id);
            let idempotency = log.idempotency_key.as_ref().map(|key| {
                store
                    .paths
                    .commit_idempotency_entry_path(&entry.table_bucket, &entry.table_id, key)
            });
            let mut fence = retirement_fence(&dispatch);
            fence.extend_signals(operation.inner.fence.signals.iter().cloned().map(Some));
            log.status = CommitLogStatus::Committed;
            store
                .finalize_commit_log_with_fence(&path, idempotency.as_deref(), &log, Some(&fence))
                .await?;
            let object = store
                .backend
                .read_object_limited(store.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("finalized recovery log".into()))?;
            let saved: CommitLogEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
            if saved != log {
                return Err(TableCatalogStoreError::Conflict("finalized recovery log changed".into()));
            }
            let exact = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
            let result = store.copy_vector_maintenance_source_with_guard(&dispatch, &exact).await?;
            operation.check_held()?;
            dispatch.check_held()?;
            Ok(Some(result))
        })
        .await
    }
}

#[path = "vector_retirement_recovery.rs"]
mod job_recovery;
pub(crate) use job_recovery::{VectorMaintenanceRecoveryOperation, VectorMaintenanceRecoveryOwner};

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// GC proof limits apply only to maintenance readers; an incomplete history is never returned.
    async fn read_vector_maintenance_commit_logs_bounded(
        &self,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<Vec<CommitLogEntry>> {
        if self.paths.scope != CatalogScope::Vector {
            return Err(maintenance_error("bounded Vector history requires Vector scope"));
        }
        let prefix = self.paths.commit_log_entries_prefix(&entry.table_bucket, &entry.table_id);
        let limit = NonZeroUsize::new(64).ok_or_else(|| maintenance_error("invalid history page size"))?;
        let mut after = None;
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut logs = Vec::new();
        loop {
            let page = self
                .backend
                .list_catalog_objects_page(self.catalog_bucket(), &prefix, after.as_deref(), limit)
                .await?;
            validate_maintenance_page(&page, &prefix, after.as_deref(), limit.get())?;
            for item in page.objects {
                count += 1;
                if count > VECTOR_RETIREMENT_MAX_REFERENCES || item.key.len() > 4096 {
                    return Err(TableCatalogStoreError::Unavailable(
                        "CapacityExceeded: maintenance history entries".into(),
                    ));
                }
                after = Some(item.key.clone());
                if !item.key.ends_with(".json") {
                    continue;
                }
                if item.version_id.is_some() || item.delete_marker {
                    return Err(maintenance_error("maintenance commit list has a versioned record"));
                }
                let overhead = item
                    .key
                    .len()
                    .checked_add(std::mem::size_of::<CommitLogEntry>())
                    .ok_or_else(|| maintenance_error("history accounting overflow"))?;
                bytes = bytes
                    .checked_add(overhead)
                    .ok_or_else(|| maintenance_error("history accounting overflow"))?;
                let remaining = VECTOR_MAINTENANCE_RECORD_MAX_BYTES
                    .checked_sub(bytes)
                    .filter(|value| *value > 0)
                    .ok_or_else(|| TableCatalogStoreError::Unavailable("CapacityExceeded: maintenance history bytes".into()))?;
                let metadata = self
                    .backend
                    .object_metadata(self.catalog_bucket(), &item.key)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::NotFound("discovered maintenance commit".into()))?;
                if metadata.size > remaining as u64 {
                    return Err(TableCatalogStoreError::Unavailable("CapacityExceeded: maintenance history bytes".into()));
                }
                let object = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &item.key, remaining)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::NotFound("discovered maintenance commit".into()))?;
                bytes = bytes
                    .checked_add(object.data.len())
                    .filter(|value| *value <= VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                    .ok_or_else(|| TableCatalogStoreError::Unavailable("CapacityExceeded: maintenance history bytes".into()))?;
                let log: CommitLogEntry = serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&e.to_string()))?;
                object::validate_commit_log_entry_object(&self.paths, &item.key, &entry.table_bucket, &entry.table_id, &log)?;
                logs.push(log);
            }
            if !page.is_truncated {
                return Ok(logs);
            }
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VectorRetirementReadmissionFailurePoint {
    BeforeProgressRead,
    AfterProgressWrite,
}
#[cfg(test)]
struct RetirementReadmissionFailure {
    owner: Arc<()>,
    resource: CatalogResourceIdentity,
    job_id: Uuid,
    path: String,
    point: VectorRetirementReadmissionFailurePoint,
    hits: std::sync::atomic::AtomicUsize,
}
#[cfg(test)]
static RETIREMENT_READMISSION_FAILURES: std::sync::OnceLock<parking_lot::Mutex<Vec<Arc<RetirementReadmissionFailure>>>> =
    std::sync::OnceLock::new();
#[cfg(test)]
pub(crate) struct VectorRetirementReadmissionFailureGuard(Arc<RetirementReadmissionFailure>);
#[cfg(test)]
impl VectorRetirementReadmissionFailureGuard {
    pub(crate) fn hits(&self) -> usize {
        self.0.hits.load(std::sync::atomic::Ordering::SeqCst)
    }
}
#[cfg(test)]
impl Drop for VectorRetirementReadmissionFailureGuard {
    fn drop(&mut self) {
        RETIREMENT_READMISSION_FAILURES
            .get_or_init(Default::default)
            .lock()
            .retain(|failure| !Arc::ptr_eq(failure, &self.0));
    }
}
#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) fn inject_vector_retirement_readmission_failure(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
        retirement_id: Uuid,
        point: VectorRetirementReadmissionFailurePoint,
    ) -> TableCatalogStoreResult<VectorRetirementReadmissionFailureGuard> {
        let path = retirement_progress_path(&self.paths, resource, retirement_id)?;
        let failure = Arc::new(RetirementReadmissionFailure {
            owner: self.vector_owner.clone(),
            resource: resource.clone(),
            job_id,
            path,
            point,
            hits: std::sync::atomic::AtomicUsize::new(0),
        });
        RETIREMENT_READMISSION_FAILURES
            .get_or_init(Default::default)
            .lock()
            .push(failure.clone());
        Ok(VectorRetirementReadmissionFailureGuard(failure))
    }

    fn check_retirement_readmission_failure(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        path: &str,
        point: VectorRetirementReadmissionFailurePoint,
    ) -> TableCatalogStoreResult<()> {
        for failure in RETIREMENT_READMISSION_FAILURES.get_or_init(Default::default).lock().iter() {
            if Arc::ptr_eq(&failure.owner, &self.vector_owner)
                && failure.resource == guard.lease().resource
                && failure.job_id == guard.lease().job_id
                && failure.path == path
                && failure.point == point
                && failure
                    .hits
                    .compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
                    .is_ok()
            {
                return Err(TableCatalogStoreError::Unavailable(format!(
                    "injected retirement readmission failure at {point:?}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "vector_maintenance_observer.rs"]
mod fault_observer;
#[cfg(test)]
pub(crate) use fault_observer::*;

#[cfg(test)]
#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceGcLockSelector {
    pub(crate) job: VectorMaintenanceJobV1,
    pub(crate) job_object: CatalogMetadataRefRecordV1,
    pub(crate) plan_ref: CatalogMetadataRefRecordV1,
    pub(crate) evidence_ref: CatalogMetadataRefRecordV1,
    pub(crate) targets: Vec<VectorRetirementTargetV1>,
    pub(crate) lock_resource: rustfs_lock::ObjectKey,
}

#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Reads a real Queued GC plan under the caller's authorized resource protection.
    pub(crate) async fn read_vector_maintenance_test_gc_lock_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<VectorMaintenanceGcLockSelector> {
        let observed = self
            .read_vector_maintenance_test_job_with_guard(guard, job_id)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("observed GC job".into()))?;
        let job = observed.job();
        if *job.state() != VectorMaintenanceJobState::Queued
            || *job.attempt() != 0
            || job.lease().is_some()
            || !matches!(job.kind(), VectorMaintenanceKind::OrphanGc | VectorMaintenanceKind::HistoryGc)
        {
            return Err(TableCatalogStoreError::Conflict(
                "GC lock selector requires an original Queued GC job".into(),
            ));
        }
        let plan_ref = job
            .input()
            .plan_ref
            .as_ref()
            .ok_or_else(|| maintenance_error("observed GC job lacks its plan"))?;
        let plan = {
            let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(plan_ref)?).await?;
            decode_vector_maintenance_record::<VectorRetirementPlanV1>(&object.data)?
        };
        if plan.resource != *job.resource() {
            return Err(TableCatalogStoreError::Conflict("observed GC plan resource changed".into()));
        }
        {
            let object = self
                .maintenance_read_ref(&CatalogMetadataRef::try_from(&plan.evidence_ref)?)
                .await?;
            let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&object.data)?;
            if evidence.resource != *job.resource() {
                return Err(TableCatalogStoreError::Conflict("observed GC evidence resource changed".into()));
            }
        }
        let resource = guard.resource_identity();
        let selector = VectorMaintenanceGcLockSelector {
            job: job.clone(),
            job_object: observed.observed_object().into(),
            plan_ref: plan_ref.clone(),
            evidence_ref: plan.evidence_ref,
            targets: plan.targets,
            lock_resource: rustfs_lock::ObjectKey::new(
                self.catalog_bucket(),
                self.paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            ),
        };
        if !crate::table_catalog::maintenance::fault_observation::serialized_within(&selector, 32 * 1024) {
            return Err(TableCatalogStoreError::Unavailable("CapacityExceeded: GC selector exceeds 32 KiB".into()));
        }
        if self
            .read_vector_maintenance_test_job_with_guard(guard, job_id)
            .await?
            .as_ref()
            != Some(&observed)
        {
            return Err(TableCatalogStoreError::Conflict("observed GC job changed during selector read".into()));
        }
        guard.check_held()?;
        Ok(selector)
    }
}
