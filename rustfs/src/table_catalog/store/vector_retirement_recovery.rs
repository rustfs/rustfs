// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A read-only inspection contains no lock owner and grants no object mutation capability.
#[derive(Clone)]
pub(crate) struct VectorRetirementJobRecoveryInspection {
    owner: Arc<()>,
    job: VectorMaintenanceJobSnapshot,
    resource: VectorMaintenanceResourceSnapshot,
    authorization_ref: CatalogMetadataRef,
    progress_ref: CatalogMetadataRef,
    progress: VectorRetirementProgressV1,
    requests: Vec<VectorRetirementRecoveryRequest>,
}
impl VectorRetirementJobRecoveryInspection {
    pub(crate) fn job(&self) -> &VectorMaintenanceJobSnapshot {
        &self.job
    }
    pub(crate) fn resource(&self) -> &VectorMaintenanceResourceSnapshot {
        &self.resource
    }
    pub(crate) fn progress(&self) -> &VectorRetirementProgressV1 {
        &self.progress
    }
    pub(crate) fn requests(&self) -> &[VectorRetirementRecoveryRequest] {
        &self.requests
    }

    /// Merge actual receipts for this attempt, covering every nonterminal dispatch it admitted.
    pub(crate) fn merge_owned_completions(
        &self,
        completions: &[VectorMaintenanceOwnedCompletion],
    ) -> TableCatalogStoreResult<VectorMaintenanceOwnedCompletion> {
        let lease = VectorMaintenanceLeaseToken::from_job(&self.job.job)?;
        let mut required = BTreeSet::new();
        let mut terminal = BTreeSet::new();
        for target in &self.progress.targets {
            let Some(dispatch) = &target.dispatch else { continue };
            if dispatch.job_id != lease.job_id || dispatch.attempt != lease.attempt {
                continue;
            }
            if dispatch.lease_id != lease.lease_id
                || dispatch.worker_boot_id != lease.worker_boot_id
                || dispatch.execution_identity != lease.execution_identity
            {
                return Err(maintenance_error("retirement dispatch differs from original attempt"));
            }
            if matches!(
                target.state,
                VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
            ) {
                terminal.insert(dispatch.dispatch_id);
            } else {
                required.insert(dispatch.dispatch_id);
            }
        }
        if required.is_empty() && terminal.is_empty() {
            let [receipt] = completions else {
                return Err(TableCatalogStoreError::Conflict("original retirement owner completion is unknown".into()));
            };
            if !receipt.matches(&lease)
                || !receipt.no_delete_dispatch
                || receipt.completed_operation_ids.is_empty()
                || !receipt.unpublished_commit_refs.is_empty()
            {
                return Err(TableCatalogStoreError::Conflict(
                    "original retirement no-delete owner completion is unknown".into(),
                ));
            }
            return Ok(receipt.clone());
        }
        let mut completed = BTreeSet::new();
        for receipt in completions {
            if !receipt.matches(&lease) || receipt.no_delete_dispatch || !receipt.unpublished_commit_refs.is_empty() {
                return Err(maintenance_error("completion receipt differs from original retirement attempt"));
            }
            for id in &receipt.completed_operation_ids {
                if !required.contains(id) && !terminal.contains(id) {
                    return Err(maintenance_error("completion contains an unrelated dispatch"));
                }
                completed.insert(*id);
            }
        }
        if !required.is_subset(&completed) {
            return Err(TableCatalogStoreError::Conflict("original retirement owner completion is unknown".into()));
        }
        completed.extend(terminal);
        VectorMaintenanceOwnedCompletion::try_from_completed_operations(&lease, completed.into_iter().collect(), Vec::new())
    }
}

struct RetirementInspectionProtection {
    _lifecycle: Arc<BucketIncarnationFenceGuard>,
    _dispatch: Vec<WriteCommitGuard>,
    _gc: TableCatalogLockGuard,
    fence: TableCatalogObjectMutationFence,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn protect_retirement_job_inspection(
        &self,
        snapshot: &VectorMaintenanceJobSnapshot,
    ) -> TableCatalogStoreResult<RetirementInspectionProtection> {
        let resource = snapshot.job.resource.identity()?;
        let lifecycle = self
            .backend
            .acquire_bucket_lifecycle_read(self.maintenance_bucket(&resource)?)
            .await?;
        let owner = lifecycle
            .lifecycle_guard
            .clone()
            .ok_or_else(|| maintenance_error("recovery inspection requires native lifecycle ownership"))?;
        let prior = self.maintenance_prior_dispatch_jobs(&snapshot.job).await?;
        let mut ids = prior.clone();
        ids.insert(snapshot.job.job_id);
        let mut owners = Vec::new();
        let mut fence = TableCatalogObjectMutationFence::from_signals([owner.lock_lost_signal()]);
        for id in ids {
            let lock = self
                .backend
                .acquire_write_lock(self.catalog_bucket(), &self.paths.vector_maintenance_dispatch_path(&resource, id)?)
                .await?;
            let retained = lock.write_commit_guards();
            if retained.is_empty() {
                return Err(maintenance_error("recovery inspection requires native dispatch ownership"));
            }
            fence.extend_signals([lock.lock_lost_signal()]);
            owners.extend(retained);
        }
        if self.maintenance_prior_dispatch_jobs(&snapshot.job).await? != prior {
            return Err(TableCatalogStoreError::Conflict("recovery inspection dispatch set changed".into()));
        }
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        fence.extend_signals([gc.lock_lost_signal()]);
        fence.ensure_held()?;
        Ok(RetirementInspectionProtection {
            _lifecycle: owner,
            _dispatch: owners,
            _gc: gc,
            fence,
        })
    }

    /// Acquires real protection only for this read. Native recovery must run after this returns.
    pub(crate) async fn inspect_vector_retirement_job_recovery(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
        expected_revision: Uuid,
        identity: &VectorMaintenanceExecutionIdentityV1,
    ) -> TableCatalogStoreResult<VectorRetirementJobRecoveryInspection> {
        let original = self
            .maintenance_read_job(resource, job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement recovery job".into()))?;
        let mut protection = self.protect_retirement_job_inspection(&original).await?;
        self.retirement_job_inspection_under_protection(resource, job_id, expected_revision, identity, &mut protection)
            .await
    }

    async fn retirement_job_inspection_under_protection(
        &self,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
        expected_revision: Uuid,
        identity: &VectorMaintenanceExecutionIdentityV1,
        protection: &mut RetirementInspectionProtection,
    ) -> TableCatalogStoreResult<VectorRetirementJobRecoveryInspection> {
        protection.fence.ensure_held()?;
        let job = self
            .maintenance_read_job(resource, job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement recovery job".into()))?;
        let inspectable = match job.job.state {
            VectorMaintenanceJobState::RecoveryRequired | VectorMaintenanceJobState::ManualReview => true,
            VectorMaintenanceJobState::Running => {
                let lease = job
                    .job
                    .lease
                    .as_ref()
                    .ok_or_else(|| maintenance_error("running retirement recovery lacks lease"))?;
                vector_maintenance_timestamp(&lease.expires_at)? <= OffsetDateTime::now_utc()
            }
            _ => false,
        };
        if job.job.revision != expected_revision
            || &job.job.execution_identity != identity
            || job.job.kind != VectorMaintenanceKind::DeleteRecovery
            || !inspectable
        {
            return Err(TableCatalogStoreError::Conflict(
                "retirement inspection job state or identity changed".into(),
            ));
        }
        let source = job
            .job
            .input
            .source
            .as_ref()
            .ok_or_else(|| maintenance_error("retirement recovery source missing"))?;
        if source.kind != VectorMaintenanceSourceKind::RetirementAuthorization {
            return Err(maintenance_error("retirement inspection requires exact authorization source"));
        }
        let source_object = self
            .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
            .await?;
        let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&source_object.data)?;
        let path = format!(
            "{}authorizations/{}.json",
            self.paths.vector_maintenance_prefix(resource)?,
            table_catalog_path_hash(&auth.retirement_id.to_string())
        );
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("published retirement authorization".into()))?;
        let authorization_ref = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        let (published, current) = self
            .retirement_authority_under_protection(resource, &authorization_ref)
            .await?;
        if auth != published || auth.resource != job.job.resource {
            return Err(maintenance_error("retirement inspection authority differs"));
        }
        let path = retirement_progress_path(&self.paths, resource, auth.retirement_id)?;
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement inspection progress".into()))?;
        let progress_ref = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
        if progress.resource != auth.resource
            || progress.authorization_ref != CatalogMetadataRefRecordV1::from(&authorization_ref)
            || progress.retirement_id != auth.retirement_id
            || progress.targets.len() != auth.targets.len()
        {
            return Err(maintenance_error("retirement inspection progress binding differs"));
        }
        let requests = progress
            .targets
            .iter()
            .filter(|target| {
                !matches!(
                    target.state,
                    VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
                )
            })
            .map(|target| VectorRetirementRecoveryRequest {
                resource: resource.clone(),
                authorization_ref: authorization_ref.clone(),
                retirement_id: auth.retirement_id,
                retirement_revision: auth.retirement_revision,
                progress_revision: progress.revision,
                ordinal: target.ordinal,
                target: auth.targets[usize::from(target.ordinal)].clone(),
                prior_dispatch: target.dispatch.clone(),
            })
            .collect();
        protection.fence.ensure_held()?;
        Ok(VectorRetirementJobRecoveryInspection {
            owner: self.vector_owner.clone(),
            job,
            resource: current,
            authorization_ref,
            progress_ref,
            progress,
            requests,
        })
    }

    /// Rechecks exact observations; only proven completion or a bounded retry changes the original job.
    pub(crate) async fn resolve_vector_retirement_job_recovery(
        &self,
        inspection: VectorRetirementJobRecoveryInspection,
        evidence: Vec<VectorRetirementReadmissionEvidence>,
        completion: Option<VectorMaintenanceOwnedCompletion>,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        if !Arc::ptr_eq(&self.vector_owner, &inspection.owner) {
            return Err(maintenance_error("retirement inspection belongs to another store"));
        }
        let store = self.clone();
        object::run_vector_catalog_mutation(async move {
            store
                .resolve_retirement_job_inspection(inspection, evidence, completion)
                .await
        })
        .await
    }

    async fn resolve_retirement_job_inspection(
        &self,
        inspection: VectorRetirementJobRecoveryInspection,
        evidence: Vec<VectorRetirementReadmissionEvidence>,
        completion: Option<VectorMaintenanceOwnedCompletion>,
    ) -> TableCatalogStoreResult<VectorMaintenanceJobSnapshot> {
        let original = &inspection.job;
        let resource = original.job.resource.identity()?;
        let mut protection = self.protect_retirement_job_inspection(original).await?;
        let terminal = inspection.requests.is_empty() && inspection.progress.state == VectorRetirementProgressState::Completed;
        let payload = serde_json::json!({"authorization_ref": CatalogMetadataRefRecordV1::from(&inspection.authorization_ref),
            "progress_ref": CatalogMetadataRefRecordV1::from(&inspection.progress_ref), "complete": terminal});
        let current = self
            .maintenance_read_job(&resource, original.job.job_id, false)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement recovery job".into()))?;
        if current != *original {
            let mut replay = current.job.clone();
            maintenance_stamp(&mut replay, "RECOVER", Some(original.job.revision), payload.clone())?;
            if replay.last_update_id == current.job.last_update_id && replay.last_update_sha256 == current.job.last_update_sha256
            {
                protection.fence.ensure_held()?;
                return Ok(current);
            }
            return Err(TableCatalogStoreError::Conflict("retirement recovery job changed".into()));
        }
        let fresh = self
            .retirement_job_inspection_under_protection(
                &resource,
                original.job.job_id,
                original.job.revision,
                &original.job.execution_identity,
                &mut protection,
            )
            .await?;
        if fresh.resource != inspection.resource
            || fresh.authorization_ref != inspection.authorization_ref
            || fresh.progress_ref != inspection.progress_ref
            || fresh.progress != inspection.progress
            || fresh.requests != inspection.requests
        {
            return Err(TableCatalogStoreError::Conflict("retirement recovery observation changed".into()));
        }
        let mut next = original.job.clone();
        let now = OffsetDateTime::now_utc();
        next.updated_at = maintenance_format_time(now);
        if terminal {
            if !evidence.is_empty() {
                return Err(maintenance_error("completed retirement has unexpected recovery evidence"));
            }
            let progress_copy = self
                .maintenance_copy_source(&resource, &inspection.progress_ref, &protection.fence)
                .await?;
            let result = VectorMaintenanceResultV1 {
                version: 1,
                job_id: next.job_id,
                recorded_by_job_id: next.job_id,
                attempt: next.attempt,
                resource: next.resource.clone(),
                finished_at: next.updated_at.clone(),
                effect: VectorMaintenanceResultEffect::Retired,
                stages: Vec::new(),
                commit_refs: Vec::new(),
                retirement_refs: vec![(&inspection.authorization_ref).into()],
                output_refs: vec![(&progress_copy).into()],
                reason: None,
            };
            let bytes = encode_vector_maintenance_record(&result)?;
            let sha = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
            let path = format!(
                "{}results/{}/attempt-{}/{sha}.json",
                self.paths.vector_maintenance_prefix(&resource)?,
                table_catalog_path_hash(&next.job_id.to_string()),
                next.attempt
            );
            let reference = self
                .maintenance_write_immutable(&resource, path, bytes, &protection.fence)
                .await?;
            next.state = VectorMaintenanceJobState::Succeeded;
            next.result_ref = Some((&reference).into());
            next.finished_at = Some(next.updated_at.clone());
            next.failure = None;
            next.next_retry_at = None;
        } else {
            let completion = completion
                .ok_or_else(|| TableCatalogStoreError::Conflict("original retirement owner completion is unknown".into()))?;
            inspection.merge_owned_completions(&[completion])?;
            if evidence.len() != inspection.requests.len() {
                return Err(maintenance_error("retirement recovery evidence omits targets"));
            }
            let mut seen = BTreeSet::new();
            for value in evidence {
                if !seen.insert(value.request.ordinal) || !inspection.requests.contains(&value.request) {
                    return Err(maintenance_error("retirement recovery evidence target differs"));
                }
                let current = self
                    .backend
                    .object_metadata(resource.storage_bucket_id(), &value.request.target.object_key)
                    .await?;
                match (current, value.observed) {
                    (None, None) => {}
                    (Some(current), Some(observed)) => {
                        let key = CatalogObjectKey::try_new(value.request.target.object_key.clone())?;
                        if VectorRetirementTargetV1::from_object_metadata(&resource, key.clone(), &current)?
                            != value.request.target
                            || VectorRetirementTargetV1::from_object_metadata(&resource, key, &observed)? != value.request.target
                        {
                            return Err(TableCatalogStoreError::Conflict("retirement recovery target changed".into()));
                        }
                    }
                    _ => return Err(TableCatalogStoreError::Conflict("retirement recovery presence changed".into())),
                }
            }
            if next.attempt >= next.policy.max_attempts {
                protection.fence.ensure_held()?;
                return Ok(current);
            }
            let seconds = next
                .policy
                .retry_initial_backoff_seconds
                .saturating_mul(1u64 << (next.attempt - 1))
                .min(next.policy.retry_max_backoff_seconds);
            next.state = VectorMaintenanceJobState::RetryScheduled;
            next.failure = Some(VectorMaintenanceFailureCode::Interrupted);
            next.next_retry_at = Some(maintenance_format_time(now + Duration::seconds(seconds as i64)));
            next.finished_at = None;
            next.result_ref = None;
        }
        maintenance_stamp(&mut next, "RECOVER", Some(original.job.revision), payload)?;
        let path = self.paths.vector_maintenance_job_path(&resource, next.job_id)?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), &path).await?;
        let observed = self
            .maintenance_read_job(&resource, next.job_id, true)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("retirement recovery job".into()))?;
        if observed != *original {
            return Err(TableCatalogStoreError::Conflict("retirement recovery conditional update changed".into()));
        }
        protection.fence.extend_signals([lock.lock_lost_signal()]);
        self.maintenance_write_job(original, &next, lock.write_commit_guards(), &protection.fence)
            .await
    }
}

/// Tracks the actual recovery callback and the native owners it admits in this process.
pub(crate) struct VectorMaintenanceRecoveryOwner {
    lease: VectorMaintenanceLeaseToken,
    state: Arc<parking_lot::Mutex<RecoveryOwnerState>>,
}
#[derive(Default)]
struct RecoveryOwnerState {
    active: BTreeSet<Uuid>,
    completed: BTreeSet<Uuid>,
    unresolved: bool,
    delete_started: bool,
}
pub(crate) struct VectorMaintenanceRecoveryOperation {
    operation_id: Uuid,
    state: Arc<parking_lot::Mutex<RecoveryOwnerState>>,
    completed: bool,
}
impl VectorMaintenanceRecoveryOwner {
    pub(crate) fn new(lease: &VectorMaintenanceLeaseToken) -> Self {
        Self {
            lease: lease.clone(),
            state: Arc::new(parking_lot::Mutex::new(RecoveryOwnerState::default())),
        }
    }
    pub(crate) fn begin_native_operation(
        &self,
        operation_id: Uuid,
    ) -> TableCatalogStoreResult<VectorMaintenanceRecoveryOperation> {
        let mut state = self.state.lock();
        if operation_id.is_nil()
            || state.active.len() + state.completed.len() >= VECTOR_RETIREMENT_MAX_TARGETS
            || state.active.contains(&operation_id)
            || state.completed.contains(&operation_id)
            || state.unresolved
        {
            return Err(maintenance_error("native recovery owner cannot admit this operation"));
        }
        state.active.insert(operation_id);
        Ok(VectorMaintenanceRecoveryOperation {
            operation_id,
            state: self.state.clone(),
            completed: false,
        })
    }
    pub(super) fn check_lease(&self, lease: &VectorMaintenanceLeaseToken) -> TableCatalogStoreResult<()> {
        if &self.lease != lease {
            return Err(TableCatalogStoreError::Conflict("readmission tracker belongs to another attempt".into()));
        }
        Ok(())
    }
    pub(super) fn delete_dispatch_started(&self) {
        self.state.lock().delete_started = true;
    }
    /// Called by the completed callback owner with the still-valid exact attempt protection.
    pub(crate) fn complete(
        self,
        guard: &VectorMaintenanceDispatchGuard,
    ) -> TableCatalogStoreResult<VectorMaintenanceOwnedCompletion> {
        guard.check_held()?;
        let state = self.state.lock();
        let mut receipt = VectorMaintenanceOwnedCompletion::try_from_completed_operations(
            &self.lease,
            state.completed.iter().copied().collect(),
            Vec::new(),
        )?;
        if !receipt.matches(guard.lease())
            || state.completed.is_empty()
            || !state.active.is_empty()
            || state.unresolved
            || state.delete_started
        {
            return Err(TableCatalogStoreError::Conflict(
                "native recovery callback has no complete no-delete receipt".into(),
            ));
        }
        receipt.no_delete_dispatch = true;
        Ok(receipt)
    }
}
impl VectorMaintenanceRecoveryOperation {
    /// Invoke only after the actual admitted native future has returned a determined success.
    pub(crate) fn complete(mut self) -> TableCatalogStoreResult<()> {
        let mut state = self.state.lock();
        if !state.active.remove(&self.operation_id) || !state.completed.insert(self.operation_id) {
            state.unresolved = true;
            return Err(maintenance_error("native recovery completion differs from admitted owner"));
        }
        self.completed = true;
        Ok(())
    }
}
impl Drop for VectorMaintenanceRecoveryOperation {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.state.lock();
            state.active.remove(&self.operation_id);
            state.unresolved = true;
        }
    }
}
