// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Read-only, owner-bound observations for the real maintenance fault driver.

use super::*;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceTestResultObservation {
    pub(crate) observed_object: CatalogMetadataRefRecordV1,
    pub(crate) result: VectorMaintenanceResultV1,
    pub(crate) recorder_job: VectorMaintenanceJobV1,
    pub(crate) recorder_object: CatalogMetadataRefRecordV1,
}

/// An original Result write and the independently observed current job record.
/// Presence of the Result does not establish a successful job completion.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceTestWrittenResultObservation {
    pub(crate) observed_object: CatalogMetadataRefRecordV1,
    pub(crate) result: VectorMaintenanceResultV1,
    pub(crate) job_object: CatalogMetadataRefRecordV1,
    pub(crate) job: VectorMaintenanceJobV1,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceTestRetirementObservation {
    pub(crate) authorization_object: CatalogMetadataRefRecordV1,
    pub(crate) authorization: VectorRetirementAuthorizationV1,
    pub(crate) creator_job: VectorMaintenanceJobV1,
    pub(crate) creator_object: CatalogMetadataRefRecordV1,
    pub(crate) progress_object: Option<CatalogMetadataRefRecordV1>,
    pub(crate) progress: Option<VectorRetirementProgressV1>,
    pub(crate) dispatch_jobs: Vec<VectorMaintenanceJobV1>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceTestEvidence {
    pub(crate) job_object: CatalogMetadataRefRecordV1,
    pub(crate) job: VectorMaintenanceJobV1,
    pub(crate) resource_object: CatalogMetadataRefRecordV1,
    pub(crate) result: Option<VectorMaintenanceTestResultObservation>,
    pub(crate) retirements: Vec<VectorMaintenanceTestRetirementObservation>,
}

struct ObservationBudget(usize);
impl ObservationBudget {
    fn add(&mut self, size: u64) -> TableCatalogStoreResult<()> {
        let size = usize::try_from(size).map_err(|_| maintenance_error("observer record size overflow"))?;
        self.0 = self
            .0
            .checked_add(size)
            .ok_or_else(|| maintenance_error("observer size overflow"))?;
        if self.0 > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
            return Err(TableCatalogStoreError::Unavailable(
                "CapacityExceeded: maintenance observation exceeds 4 MiB".into(),
            ));
        }
        Ok(())
    }
}

fn validate_observed_stage_resource(log: &CommitLogEntry, resource: &CatalogResourceIdentity) -> TableCatalogStoreResult<()> {
    let binding = log
        .vector_commit_binding
        .as_ref()
        .ok_or_else(|| maintenance_error("observed stage lacks Vector identity"))?;
    if Some(&binding.bucket.to_handle()?) != resource.vector_bucket() || binding.table_id != resource.table_id().as_str() {
        return Err(TableCatalogStoreError::Conflict(
            "observed stage has a different complete resource identity".into(),
        ));
    }
    Ok(())
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn validate_test_observed_job(
        &self,
        guard: &VectorCatalogOperationGuard,
        observed: &VectorMaintenanceJobSnapshot,
    ) -> TableCatalogStoreResult<()> {
        self.validate_vector_operation_guard(guard)?;
        if observed.job.resource.identity()? != *guard.resource_identity()
            || observed.job.resource.namespace != guard.namespace().public_name()
            || observed.job.resource.name != guard.name().as_str()
            || observed.observed_object.object_key().as_str()
                != self
                    .paths
                    .vector_maintenance_job_path(guard.resource_identity(), observed.job.job_id)?
        {
            return Err(TableCatalogStoreError::Conflict(
                "observed job belongs to another protected resource".into(),
            ));
        }
        let object = self.maintenance_read_ref(&observed.observed_object).await?;
        if decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)? != observed.job {
            return Err(TableCatalogStoreError::Conflict("observed job bytes changed".into()));
        }
        guard.check_held()
    }

    pub(crate) async fn read_vector_maintenance_test_job_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<Option<VectorMaintenanceJobSnapshot>> {
        self.validate_vector_operation_guard(guard)?;
        if job_id.is_nil() {
            return Err(maintenance_error("nil observation job ID"));
        }
        let job = self.maintenance_read_job(guard.resource_identity(), job_id, false).await?;
        if let Some(job) = &job {
            self.validate_test_observed_job(guard, job).await?;
        }
        guard.check_held()?;
        Ok(job)
    }

    pub(crate) async fn read_vector_maintenance_test_stage_commit_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        observed: &VectorMaintenanceJobSnapshot,
        stage: &str,
    ) -> TableCatalogStoreResult<Option<(CommitLogEntry, CatalogMetadataRef)>> {
        self.validate_test_observed_job(guard, observed).await?;
        let job = &observed.job;
        if !matches!(
            (job.kind, stage),
            (VectorMaintenanceKind::AnnBuild, "ann-build") | (VectorMaintenanceKind::Compaction, "reserve-fragments" | "rewrite")
        ) {
            return Err(maintenance_error("stage does not belong to observed job kind"));
        }
        let resource = guard.resource_identity();
        let id = vector_maintenance_stage_commit_id(job.job_id, job.attempt, stage)?;
        let path = self
            .paths
            .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), id.as_str());
        let result = if let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
            .await?
        {
            let log: CommitLogEntry =
                serde_json::from_slice(&object.data).map_err(|error| maintenance_error(&error.to_string()))?;
            self.paths
                .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())?;
            validate_observed_stage_resource(&log, resource)?;
            if log.commit_id != id.as_str() {
                return Err(maintenance_error("observed stage commit ID differs from its exact path"));
            }
            let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
            Some((log, reference))
        } else {
            None
        };
        self.validate_test_observed_job(guard, observed).await?;
        Ok(result)
    }

    /// Reads the original Result selected by the native write's content identity.
    /// The current job must still match `observed`; it may remain Running without a result reference.
    /// This observation neither updates the job nor supplies scheduler completion authority.
    pub(crate) async fn read_vector_maintenance_test_written_result_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        observed: &VectorMaintenanceJobSnapshot,
        content_sha256: &str,
        content_length: u64,
    ) -> TableCatalogStoreResult<VectorMaintenanceTestWrittenResultObservation> {
        self.validate_test_observed_job(guard, observed).await?;
        decode_catalog_digest(content_sha256)?;
        if content_length == 0 || content_length > VECTOR_MAINTENANCE_RECORD_MAX_BYTES as u64 {
            return Err(maintenance_error("observed Result length is outside the record limit"));
        }
        let prefix = self.paths.vector_maintenance_prefix(guard.resource_identity())?;
        let reference = CatalogMetadataRef::try_from(&CatalogMetadataRefRecordV1 {
            object_key: format!(
                "{prefix}results/{}/attempt-{}/{content_sha256}.json",
                table_catalog_path_hash(&observed.job.job_id.to_string()),
                observed.job.attempt
            ),
            content_length,
            content_sha256: content_sha256.to_owned(),
            body_revision: None,
            etag: None,
        })?;
        let object = self.maintenance_read_ref(&reference).await?;
        let result = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&object.data)?;
        if result.resource != observed.job.resource
            || result.job_id != observed.job.job_id
            || result.recorded_by_job_id != observed.job.job_id
            || result.attempt != observed.job.attempt
        {
            return Err(TableCatalogStoreError::Conflict("observed Result belongs to another execution".into()));
        }
        let exact = CatalogMetadataRef::from_read(reference.object_key().clone(), &object)?;
        self.validate_test_observed_job(guard, observed).await?;
        Ok(VectorMaintenanceTestWrittenResultObservation {
            observed_object: (&exact).into(),
            result,
            job_object: (&observed.observed_object).into(),
            job: observed.job.clone(),
        })
    }

    /// Returns exact records only. Mutable job, head and progress reads are checked again before return.
    /// A changed observation or an unavailable historical lease returns an error, never a success proof.
    pub(crate) async fn inspect_vector_maintenance_evidence_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        observed: &VectorMaintenanceJobSnapshot,
    ) -> TableCatalogStoreResult<VectorMaintenanceTestEvidence> {
        self.validate_test_observed_job(guard, observed).await?;
        let snapshot = self.read_vector_maintenance_resource_with_guard(guard).await?;
        let resource = guard.resource_identity();
        let prefix = self.paths.vector_maintenance_prefix(resource)?;
        let mut budget = ObservationBudget(0);
        budget.add(observed.observed_object.content_length())?;
        budget.add(snapshot.observed_object.content_length())?;
        let mut checked = Vec::new();
        let mut absent_progress = Vec::new();
        let result = if let Some(reference) = &observed.job.result_ref {
            let exact = CatalogMetadataRef::try_from(reference)?;
            let expected = format!(
                "{prefix}results/{}/attempt-{}/{}.json",
                table_catalog_path_hash(&observed.job.job_id.to_string()),
                observed.job.attempt,
                reference.content_sha256
            );
            if reference.object_key != expected {
                return Err(maintenance_error("result path differs from observed job and attempt"));
            }
            budget.add(exact.content_length())?;
            let bytes = self.maintenance_read_ref(&exact).await?;
            let result = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&bytes.data)?;
            if result.job_id != observed.job.job_id
                || result.attempt != observed.job.attempt
                || result.resource != observed.job.resource
            {
                return Err(maintenance_error("result identity differs from observed job"));
            }
            let recorder = if result.recorded_by_job_id == observed.job.job_id {
                observed.clone()
            } else {
                let recorder = self
                    .maintenance_read_job(resource, result.recorded_by_job_id, false)
                    .await?
                    .ok_or_else(|| maintenance_error("result recorder job is absent"))?;
                budget.add(recorder.observed_object.content_length())?;
                let source = recorder
                    .job
                    .input
                    .source
                    .as_ref()
                    .ok_or_else(|| maintenance_error("recovery recorder has no original source"))?;
                if recorder.job.kind != VectorMaintenanceKind::CommitRecovery
                    || source.kind != VectorMaintenanceSourceKind::MaintenanceJob
                {
                    return Err(maintenance_error("result recorder is not the original job recovery"));
                }
                let source_ref = CatalogMetadataRef::try_from(&source.reference)?;
                budget.add(source_ref.content_length())?;
                let source_object = self.maintenance_read_ref(&source_ref).await?;
                let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&source_object.data)?;
                if original.job_id != observed.job.job_id
                    || original.attempt != observed.job.attempt
                    || original.resource != observed.job.resource
                    || original.lease != observed.job.lease
                    || original.execution_identity != observed.job.execution_identity
                    || original.input != observed.job.input
                {
                    return Err(maintenance_error("result recovery source does not bind the observed lease and input"));
                }
                checked.push(recorder.observed_object.clone());
                recorder
            };
            for reference in result
                .commit_refs
                .iter()
                .chain(&result.retirement_refs)
                .chain(&result.output_refs)
            {
                if !reference.object_key.starts_with(&prefix) {
                    return Err(maintenance_error("result reference is outside the maintenance resource"));
                }
                let exact = CatalogMetadataRef::try_from(reference)?;
                budget.add(exact.content_length())?;
                self.maintenance_read_ref(&exact).await?;
            }
            for stage in &result.stages {
                if let Some(reference) = &stage.commit_ref {
                    let exact = CatalogMetadataRef::try_from(reference)?;
                    let expected_path = self.paths.commit_log_entry_path(
                        resource.storage_bucket_id(),
                        resource.table_id().as_str(),
                        &stage.commit_id,
                    );
                    if reference.object_key != expected_path {
                        return Err(maintenance_error("result stage reference is outside its deterministic path"));
                    }
                    budget.add(exact.content_length())?;
                    let object = self.maintenance_read_ref(&exact).await?;
                    let log: CommitLogEntry =
                        serde_json::from_slice(&object.data).map_err(|error| maintenance_error(&error.to_string()))?;
                    self.paths
                        .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())?;
                    validate_observed_stage_resource(&log, resource)?;
                    if log.commit_id != stage.commit_id
                        || stage.candidate.as_ref()
                            != log.vector_commit_binding.as_ref().map(|binding| &binding.candidate_metadata)
                    {
                        return Err(maintenance_error("result stage log and candidate differ"));
                    }
                }
            }
            Some(VectorMaintenanceTestResultObservation {
                observed_object: reference.clone(),
                result,
                recorder_job: recorder.job,
                recorder_object: CatalogMetadataRefRecordV1::from(&recorder.observed_object),
            })
        } else {
            None
        };
        let mut retirements = Vec::new();
        if let Some(binding) = &snapshot.maintenance {
            binding.validate()?;
            let mut next = binding.retirement_tip.clone();
            let mut seen = BTreeSet::new();
            for _ in 0..binding.retirement_count {
                let reference = next
                    .take()
                    .ok_or_else(|| maintenance_error("observation retirement chain ended early"))?;
                if !seen.insert(reference.object_key.clone()) {
                    return Err(maintenance_error("observation retirement chain is cyclic"));
                }
                let exact = CatalogMetadataRef::try_from(&reference)?;
                budget.add(exact.content_length())?;
                let bytes = self.maintenance_read_ref(&exact).await?;
                let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&bytes.data)?;
                if auth.resource != observed.job.resource
                    || reference.object_key
                        != format!("{prefix}authorizations/{}.json", table_catalog_path_hash(&auth.retirement_id.to_string()))
                {
                    return Err(maintenance_error("observation retirement resource or exact path mismatch"));
                }
                let creator = self
                    .maintenance_read_job(resource, auth.created_by_job_id, false)
                    .await?
                    .ok_or_else(|| maintenance_error("retirement creator job is absent"))?;
                budget.add(creator.observed_object.content_length())?;
                if creator.job.resource != auth.resource
                    || creator.job.attempt != auth.created_by_attempt
                    || creator.job.execution_identity != auth.execution_identity
                    || creator.job.input.plan_ref.as_ref() != Some(&auth.plan_ref)
                    || !matches!(creator.job.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
                {
                    return Err(maintenance_error("retirement creator identity, attempt or plan mismatch"));
                }
                checked.push(creator.observed_object.clone());
                next = auth.previous_ref.clone();
                let path = retirement_progress_path(&self.paths, resource, auth.retirement_id)?;
                let mut dispatch_jobs = Vec::new();
                let (progress_object, progress) = if let Some(object) = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                    .await?
                {
                    budget.add(u64::try_from(object.data.len()).map_err(|_| maintenance_error("progress length overflow"))?)?;
                    let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
                    if progress.resource != auth.resource
                        || progress.retirement_id != auth.retirement_id
                        || progress.authorization_ref != reference
                        || progress.targets.len() != auth.targets.len()
                    {
                        return Err(maintenance_error("observation progress authorization mismatch"));
                    }
                    for target in &progress.targets {
                        if let Some(dispatch) = &target.dispatch {
                            if dispatch_jobs
                                .iter()
                                .any(|job: &VectorMaintenanceJobV1| job.job_id == dispatch.job_id)
                            {
                                continue;
                            }
                            let job = self
                                .maintenance_read_job(resource, dispatch.job_id, false)
                                .await?
                                .ok_or_else(|| maintenance_error("retirement dispatch job is absent"))?;
                            if job.job.resource != auth.resource {
                                return Err(maintenance_error("dispatch job has a different complete resource"));
                            }
                            if job.job.job_id != auth.created_by_job_id {
                                let source = job
                                    .job
                                    .input
                                    .source
                                    .as_ref()
                                    .ok_or_else(|| maintenance_error("dispatch recovery source is absent"))?;
                                if job.job.kind != VectorMaintenanceKind::DeleteRecovery
                                    || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
                                {
                                    return Err(maintenance_error("dispatch job is outside retirement authorization"));
                                }
                                let reference = CatalogMetadataRef::try_from(&source.reference)?;
                                budget.add(reference.content_length())?;
                                let bytes = self.maintenance_read_ref(&reference).await?;
                                if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&bytes.data)? != auth {
                                    return Err(maintenance_error("dispatch recovery source differs from current retirement"));
                                }
                            }
                            budget.add(job.observed_object.content_length())?;
                            checked.push(job.observed_object.clone());
                            dispatch_jobs.push(job.job);
                        }
                    }
                    for target in &progress.targets {
                        if let Some(dispatch) = &target.dispatch {
                            let job = dispatch_jobs
                                .iter()
                                .find(|job| job.job_id == dispatch.job_id)
                                .ok_or_else(|| maintenance_error("dispatch job observation missing"))?;
                            let lease = VectorMaintenanceLeaseToken::from_job(job)?;
                            if lease.attempt != dispatch.attempt
                                || lease.lease_id != dispatch.lease_id
                                || lease.worker_id != dispatch.worker_id
                                || lease.worker_boot_id != dispatch.worker_boot_id
                                || lease.execution_identity != dispatch.execution_identity
                            {
                                return Err(TableCatalogStoreError::Unavailable(
                                    "historical dispatch lease is not available in the current exact job".into(),
                                ));
                            }
                        }
                    }
                    let exact = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
                    checked.push(exact.clone());
                    (Some(CatalogMetadataRefRecordV1::from(&exact)), Some(progress))
                } else {
                    absent_progress.push(path);
                    (None, None)
                };
                retirements.push(VectorMaintenanceTestRetirementObservation {
                    authorization_object: reference,
                    authorization: auth,
                    creator_job: creator.job,
                    creator_object: CatalogMetadataRefRecordV1::from(&creator.observed_object),
                    progress_object,
                    progress,
                    dispatch_jobs,
                });
            }
            if next.is_some() {
                return Err(maintenance_error("observation retirement chain exceeds declared count"));
            }
        }
        if let Some(result) = &result
            && result.result.retirement_refs.iter().any(|reference| {
                !retirements
                    .iter()
                    .any(|retirement| &retirement.authorization_object == reference)
            })
        {
            return Err(maintenance_error("result retirement reference is outside current authority chain"));
        }
        for path in absent_progress {
            if self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                .await?
                .is_some()
            {
                return Err(TableCatalogStoreError::Conflict("retirement progress appeared during observation".into()));
            }
        }
        for reference in checked {
            self.maintenance_read_ref(&reference).await?;
        }
        self.maintenance_read_ref(&snapshot.observed_object).await?;
        self.validate_test_observed_job(guard, observed).await?;
        Ok(VectorMaintenanceTestEvidence {
            job_object: CatalogMetadataRefRecordV1::from(&observed.observed_object),
            job: observed.job.clone(),
            resource_object: CatalogMetadataRefRecordV1::from(&snapshot.observed_object),
            result,
            retirements,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum VectorMaintenanceTestStage {
    AnnBuild,
    ReserveFragments,
    Rewrite,
}
impl VectorMaintenanceTestStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::AnnBuild => "ann-build",
            Self::ReserveFragments => "reserve-fragments",
            Self::Rewrite => "rewrite",
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) enum VectorMaintenanceTestWriteTarget {
    Staged(VectorMaintenanceTestStage),
    Head(VectorMaintenanceTestStage),
    Finalization(VectorMaintenanceTestStage),
    Job,
    Result,
    Progress { authorization: CatalogMetadataRef },
}

impl<S: TableCatalogStorage + 'static> ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<S>> {
    pub(crate) async fn arm_vector_maintenance_test_write_fault_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        observed: &VectorMaintenanceJobSnapshot,
        target: VectorMaintenanceTestWriteTarget,
        occurrence: NonZeroU16,
        phase: crate::table_catalog::test_support::catalog_write_faults::CatalogWriteFaultPhase,
        action: crate::table_catalog::test_support::catalog_write_faults::CatalogWriteFaultAction,
    ) -> TableCatalogStoreResult<crate::table_catalog::test_support::catalog_write_faults::CatalogWriteFaultHandle> {
        self.validate_test_observed_job(guard, observed).await?;
        let resource = guard.resource_identity().clone();
        let paths = self.paths.clone();
        let prefix = paths.vector_maintenance_prefix(&resource)?;
        let catalog_bucket = self.catalog_bucket().to_owned();
        let job = observed.job.clone();
        let stage = match &target {
            VectorMaintenanceTestWriteTarget::Staged(stage)
            | VectorMaintenanceTestWriteTarget::Head(stage)
            | VectorMaintenanceTestWriteTarget::Finalization(stage) => Some(*stage),
            _ => None,
        };
        let stage_id = stage
            .map(|stage| vector_maintenance_stage_commit_id(job.job_id, job.attempt, stage.as_str()))
            .transpose()?;
        let stage_log = if let Some(stage) = stage {
            self.read_vector_maintenance_test_stage_commit_with_guard(guard, observed, stage.as_str())
                .await?
                .map(|(log, _)| log)
        } else {
            None
        };
        let progress_binding = if let VectorMaintenanceTestWriteTarget::Progress { authorization } = &target {
            let (auth, _) = self.retirement_authority_under_protection(&resource, authorization).await?;
            let source_matches = if let Some(source) = &job.input.source {
                if source.kind == VectorMaintenanceSourceKind::RetirementAuthorization {
                    let object = self
                        .maintenance_read_ref(&CatalogMetadataRef::try_from(&source.reference)?)
                        .await?;
                    decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)? == auth
                } else {
                    false
                }
            } else {
                false
            };
            if !(auth.created_by_job_id == job.job_id && auth.created_by_attempt == job.attempt || source_matches) {
                return Err(maintenance_error("progress fault authorization does not belong to the observed job"));
            }
            Some((
                retirement_progress_path(&paths, &resource, auth.retirement_id)?,
                CatalogMetadataRefRecordV1::from(authorization),
                auth,
            ))
        } else {
            None
        };
        let job_path = paths.vector_maintenance_job_path(&resource, job.job_id)?;
        let head_path = paths.table_entry_path(resource.storage_bucket_id(), guard.namespace(), guard.name());
        let stage_path = stage_id
            .as_ref()
            .map(|id| paths.commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), id.as_str()));
        let stage_log = std::sync::Mutex::new(stage_log);
        let matcher = move |bucket: &str, key: &str, data: &[u8]| -> bool {
            if bucket != catalog_bucket || data.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
                return false;
            }
            if stage_path.as_deref() == Some(key) {
                let Ok(log) = serde_json::from_slice::<CommitLogEntry>(data) else {
                    return false;
                };
                if paths
                    .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())
                    .is_err()
                    || stage_id.as_ref().is_none_or(|id| log.commit_id != id.as_str())
                {
                    return false;
                }
                if validate_observed_stage_resource(&log, &resource).is_err() {
                    return false;
                }
                let matched = match target {
                    VectorMaintenanceTestWriteTarget::Staged(_) => log.status == CommitLogStatus::Staged,
                    VectorMaintenanceTestWriteTarget::Finalization(_) => log.status != CommitLogStatus::Staged,
                    _ => false,
                };
                *stage_log.lock().expect("test stage observation") = Some(log);
                return matched;
            }
            if !key.starts_with(&prefix) && key != head_path {
                return false;
            }
            match &target {
                VectorMaintenanceTestWriteTarget::Head(_) if key == head_path => {
                    let Ok(entry) = serde_json::from_slice::<TableEntry>(data) else {
                        return false;
                    };
                    let Ok(head) = CatalogHead::from_vector_entry(&entry) else {
                        return false;
                    };
                    let log = stage_log.lock().expect("test stage observation");
                    head.resource_identity() == &resource
                        && log.as_ref().is_some_and(|log| {
                            entry.version_token == log.new_version_token
                                && entry.metadata_location == log.new_metadata_location
                                && log.vector_commit_binding.as_ref().is_some_and(|binding| {
                                    binding.expected_generation.checked_add(1) == Some(entry.generation)
                                        && CatalogMetadataRefRecordV1::from(head.metadata()) == binding.candidate_metadata
                                        && CatalogFormatDescriptorRecordV1::from(head.format_descriptor()) == binding.descriptor
                                })
                        })
                }
                VectorMaintenanceTestWriteTarget::Job if key == job_path => {
                    decode_vector_maintenance_record::<VectorMaintenanceJobV1>(data).is_ok_and(|next| {
                        next.job_id == job.job_id
                            && next.resource == job.resource
                            && next.spec_sha256 == job.spec_sha256
                            && next.attempt == job.attempt
                            && next.execution_identity == job.execution_identity
                            && next.lease.as_ref().map(|lease| {
                                (&lease.lease_id, &lease.worker_id, &lease.worker_boot_id, &lease.claim_digest_sha256)
                            }) == job.lease.as_ref().map(|lease| {
                                (&lease.lease_id, &lease.worker_id, &lease.worker_boot_id, &lease.claim_digest_sha256)
                            })
                    })
                }
                VectorMaintenanceTestWriteTarget::Result => {
                    let Ok(result) = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(data) else {
                        return false;
                    };
                    let sha = hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower);
                    result.resource == job.resource
                        && result.job_id == job.job_id
                        && result.attempt == job.attempt
                        && key
                            == format!(
                                "{prefix}results/{}/attempt-{}/{sha}.json",
                                table_catalog_path_hash(&job.job_id.to_string()),
                                job.attempt
                            )
                }
                VectorMaintenanceTestWriteTarget::Progress { .. } => {
                    let Some((path, reference, auth)) = &progress_binding else {
                        return false;
                    };
                    if key != path {
                        return false;
                    }
                    let Ok(progress) = decode_vector_maintenance_record::<VectorRetirementProgressV1>(data) else {
                        return false;
                    };
                    progress.authorization_ref == *reference
                        && progress.resource == job.resource
                        && progress.retirement_id == auth.retirement_id
                        && progress.targets.len() == auth.targets.len()
                        && progress.targets.iter().any(|target| {
                            target.dispatch.as_ref().is_some_and(|dispatch| {
                                dispatch.job_id == job.job_id
                                    && dispatch.attempt == job.attempt
                                    && job.lease.as_ref().is_some_and(|lease| {
                                        lease.lease_id == dispatch.lease_id && lease.worker_boot_id == dispatch.worker_boot_id
                                    })
                                    && dispatch.execution_identity == job.execution_identity
                            })
                        })
                }
                _ => false,
            }
        };
        guard.check_held()?;
        crate::table_catalog::test_support::catalog_write_faults::arm(
            Arc::as_ptr(&self.backend.store) as usize,
            self.backend.store.clone(),
            Box::new(matcher),
            occurrence,
            phase,
            action,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::tests::prepared_commit::prepared_v3_fixture;

    #[tokio::test]
    async fn maintenance_observer_requires_exact_current_job_bytes_and_resource() {
        let fixture = prepared_v3_fixture().await;
        let mut entry = fixture.entry.clone();
        entry.version = 4;
        entry.vector_maintenance_binding = Some(VectorMaintenanceBindingV1::empty());
        let namespace = Namespace::parse("indexes").unwrap();
        let name = CatalogName::parse(entry.table.clone()).unwrap();
        let path = fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&entry).unwrap())
            .await;
        let typed_entry = VectorCatalogEntry::from_entry(entry).unwrap();
        let resource = VectorMaintenanceResourceV1::try_new(typed_entry.head().resource_identity(), &namespace, &name).unwrap();
        let input =
            VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&typed_entry).unwrap()), None, None)
                .unwrap();
        let spec = VectorMaintenanceSemanticSpec::try_new(
            resource,
            VectorMaintenanceKind::AnnBuild,
            VectorMaintenanceOrigin::Periodic,
            input,
            VectorMaintenanceExecutionIdentityV1::try_new("observer-test".into(), [7; 16]).unwrap(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .unwrap();
        let observed = fixture
            .store
            .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(spec).unwrap())
            .await
            .unwrap();
        let (guard, _) = fixture
            .store
            .begin_vector_operation(&fixture.bucket, &namespace, &name)
            .await
            .unwrap();
        let inspected = fixture
            .store
            .inspect_vector_maintenance_evidence_with_guard(&guard, &observed)
            .await
            .unwrap();
        assert_eq!(inspected.job, observed.job);
        assert_eq!(inspected.job_object, CatalogMetadataRefRecordV1::from(&observed.observed_object));
        assert!(inspected.result.is_none());
        assert!(inspected.retirements.is_empty());
        assert!(
            fixture
                .store
                .read_vector_maintenance_test_stage_commit_with_guard(&guard, &observed, "unowned-stage")
                .await
                .is_err()
        );
        let reread = fixture
            .store
            .read_vector_maintenance_test_job_with_guard(&guard, observed.job.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reread, observed);
        fixture
            .backend
            .seed_object(
                RUSTFS_META_BUCKET,
                observed.observed_object.object_key().as_str(),
                encode_vector_maintenance_record(&observed.job).unwrap(),
            )
            .await;
        assert!(
            fixture
                .store
                .inspect_vector_maintenance_evidence_with_guard(&guard, &observed)
                .await
                .is_err()
        );
    }
    #[test]
    #[serial_test::serial]
    fn maintenance_observer_reads_unreferenced_result_without_completing_running_job() {
        use crate::table_catalog::test_support::catalog_write_faults::{
            CatalogWriteFaultAction, CatalogWriteFaultPhase, CatalogWriteFenceOutcome,
        };
        use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
        run_native_vector_delete_test("maintenance-unreferenced-result-observer", || async {
            let (store, original, _fixture) = native_vector_index_fixture().await;
            let original = VectorCatalogEntry::from_entry(original).unwrap();
            let resource = original.head().resource_identity().clone();
            let entry = store
                .enroll_vector_maintenance(
                    &original,
                    vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&original).unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let resource_record =
                VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
            let execution = VectorMaintenanceExecutionIdentityV1::try_new("observer-native".into(), [9; 16]).unwrap();
            let spec = VectorMaintenanceJobSpec::from_semantic(
                VectorMaintenanceSemanticSpec::try_new(
                    resource_record,
                    VectorMaintenanceKind::AnnBuild,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()), None, None)
                        .unwrap(),
                    execution.clone(),
                    None,
                    VectorMaintenancePolicyV1::default(),
                )
                .unwrap(),
            )
            .unwrap();
            let created = store.create_vector_maintenance_job(spec).await.unwrap();
            let current = store
                .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let worker = VectorMaintenanceWorker::try_new("observer-worker".into(), Uuid::new_v4(), execution).unwrap();
            let VectorMaintenanceClaimOutcome::Acquired(work) = store
                .claim_vector_maintenance_job(
                    &resource,
                    created.job.job_id,
                    VectorMaintenanceClaimCondition::from_snapshot(&created, current.clone()),
                    worker,
                    Uuid::new_v4(),
                )
                .await
                .unwrap()
            else {
                panic!("expected actual claim");
            };
            let (operation, _) = store
                .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let result_fault = store
                .arm_vector_maintenance_test_write_fault_with_guard(
                    &operation,
                    work.snapshot(),
                    VectorMaintenanceTestWriteTarget::Result,
                    NonZeroU16::new(1).unwrap(),
                    CatalogWriteFaultPhase::AfterNative,
                    CatalogWriteFaultAction::Fail,
                )
                .await
                .unwrap();
            let job_fault = store
                .arm_vector_maintenance_test_write_fault_with_guard(
                    &operation,
                    work.snapshot(),
                    VectorMaintenanceTestWriteTarget::Job,
                    NonZeroU16::new(1).unwrap(),
                    CatalogWriteFaultPhase::Before,
                    CatalogWriteFaultAction::Fail,
                )
                .await
                .unwrap();
            drop(operation);
            let dispatch = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
            let finish = store
                .finish_vector_maintenance_job(
                    &dispatch,
                    work.snapshot().job.revision,
                    VectorMaintenanceExecutionOutcome::NoWork {
                        observed: Box::new(current),
                        reason: VectorMaintenanceNoWorkReason::NoCandidates,
                    },
                )
                .await;
            assert!(matches!(finish, Err(TableCatalogStoreError::Unavailable(_))));
            drop(dispatch);
            let result_events = result_fault.snapshot();
            assert_eq!(result_events.hits, 1);
            assert_eq!(result_events.native_succeeded, Some(true));
            assert_eq!(result_events.response_succeeded, Some(false));
            assert!(result_events.tail_drained_requested && result_events.injected_failure);
            assert!(matches!(result_events.final_fence, Some(CatalogWriteFenceOutcome::Held)));
            assert!(!result_events.overflow);
            let native = result_events
                .events
                .iter()
                .find(|event| event.phase == "inner-completed")
                .unwrap();
            let content_length = u64::try_from(native.length).unwrap();
            let job_events = job_fault.snapshot();
            assert_eq!(job_events.hits, 1);
            assert!(!job_events.native_returned);
            assert!(job_events.injected_failure);
            assert!(!job_events.overflow);
            drop(result_fault);
            drop(job_fault);
            let (operation, _) = store
                .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let observed = store
                .read_vector_maintenance_test_job_with_guard(&operation, work.snapshot().job.job_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(observed, *work.snapshot());
            assert_eq!(observed.job.state, VectorMaintenanceJobState::Running);
            assert!(observed.job.result_ref.is_none());
            let written = store
                .read_vector_maintenance_test_written_result_with_guard(&operation, &observed, &native.sha256, content_length)
                .await
                .unwrap();
            assert_eq!(written.observed_object.object_key, native.object);
            assert_eq!(written.observed_object.content_sha256, native.sha256);
            assert_eq!(written.observed_object.content_length, content_length);
            assert!(written.observed_object.body_revision.is_some());
            assert!(written.observed_object.etag.is_some());
            assert_eq!(written.result.job_id, observed.job.job_id);
            assert_eq!(written.result.recorded_by_job_id, observed.job.job_id);
            assert_eq!(written.result.attempt, observed.job.attempt);
            assert_eq!(written.result.resource, observed.job.resource);
            assert_eq!(written.result.effect, VectorMaintenanceResultEffect::NoWork);
            assert_eq!(written.job, observed.job);
            assert_eq!(written.job_object, CatalogMetadataRefRecordV1::from(&observed.observed_object));
            let raw = store
                .maintenance_read_ref(&CatalogMetadataRef::try_from(&written.observed_object).unwrap())
                .await
                .unwrap();
            assert_eq!(
                decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&raw.data).unwrap(),
                written.result
            );
            let mut wrong_resource = observed.clone();
            wrong_resource.job.resource.name = "different-index".into();
            let mut wrong_job = observed.clone();
            wrong_job.job.job_id = Uuid::new_v4();
            let mut wrong_attempt = observed.clone();
            wrong_attempt.job.attempt = observed.job.attempt.checked_add(1).unwrap();
            for mismatched in [wrong_resource, wrong_job, wrong_attempt] {
                assert!(matches!(
                    store
                        .read_vector_maintenance_test_written_result_with_guard(
                            &operation,
                            &mismatched,
                            &native.sha256,
                            content_length,
                        )
                        .await,
                    Err(TableCatalogStoreError::Conflict(_))
                ));
            }
            let wrong_sha = format!("{}{}", if native.sha256.starts_with('0') { '1' } else { '0' }, &native.sha256[1..]);
            assert!(matches!(
                store
                    .read_vector_maintenance_test_written_result_with_guard(&operation, &observed, &wrong_sha, content_length,)
                    .await,
                Err(TableCatalogStoreError::NotFound(_))
            ));
            // Replay the unchanged native body at a corrupt digest path to exercise read verification.
            let corrupt_path = written.observed_object.object_key.replace(&native.sha256, &wrong_sha);
            store
                .maintenance_write_immutable(&resource, corrupt_path, raw.data, &operation.inner.fence)
                .await
                .unwrap();
            assert!(matches!(
                store
                    .read_vector_maintenance_test_written_result_with_guard(&operation, &observed, &wrong_sha, content_length)
                    .await,
                Err(TableCatalogStoreError::Conflict(_))
            ));
            let uppercase_sha = "A".repeat(64);
            for malformed_sha in ["../results", "", &uppercase_sha] {
                assert!(matches!(
                    store
                        .read_vector_maintenance_test_written_result_with_guard(
                            &operation,
                            &observed,
                            malformed_sha,
                            content_length,
                        )
                        .await,
                    Err(TableCatalogStoreError::Invalid(_))
                ));
            }
            for wrong_length in [
                0,
                content_length - 1,
                content_length + 1,
                VECTOR_MAINTENANCE_RECORD_MAX_BYTES as u64 + 1,
            ] {
                assert!(
                    store
                        .read_vector_maintenance_test_written_result_with_guard(
                            &operation,
                            &observed,
                            &native.sha256,
                            wrong_length,
                        )
                        .await
                        .is_err()
                );
            }
            let evidence = store
                .inspect_vector_maintenance_evidence_with_guard(&operation, &observed)
                .await
                .unwrap();
            assert_eq!(evidence.job.state, VectorMaintenanceJobState::Running);
            assert!(evidence.job.result_ref.is_none());
            assert!(evidence.result.is_none());
            let reread = store
                .read_vector_maintenance_test_job_with_guard(&operation, observed.job.job_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reread, observed);
        });
    }

    #[test]
    #[serial_test::serial]
    fn maintenance_observer_reads_actual_finish_result_after_native_response_fault() {
        use crate::table_catalog::test_support::catalog_write_faults::{
            CatalogWriteFaultAction, CatalogWriteFaultPhase, CatalogWriteFenceOutcome,
        };
        use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
        run_native_vector_delete_test("maintenance-fault-observer", || async {
            let (store, original, _fixture) = native_vector_index_fixture().await;
            let original = VectorCatalogEntry::from_entry(original).unwrap();
            let resource = original.head().resource_identity().clone();
            let entry = store
                .enroll_vector_maintenance(
                    &original,
                    vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&original).unwrap())
                        .unwrap(),
                )
                .await
                .unwrap();
            let resource_record =
                VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
            let execution = VectorMaintenanceExecutionIdentityV1::try_new("observer-native".into(), [9; 16]).unwrap();
            let spec = VectorMaintenanceJobSpec::from_semantic(
                VectorMaintenanceSemanticSpec::try_new(
                    resource_record,
                    VectorMaintenanceKind::AnnBuild,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()), None, None)
                        .unwrap(),
                    execution.clone(),
                    None,
                    VectorMaintenancePolicyV1::default(),
                )
                .unwrap(),
            )
            .unwrap();
            let created = store.create_vector_maintenance_job(spec).await.unwrap();
            let current = store
                .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let worker = VectorMaintenanceWorker::try_new("observer-worker".into(), Uuid::new_v4(), execution).unwrap();
            let VectorMaintenanceClaimOutcome::Acquired(work) = store
                .claim_vector_maintenance_job(
                    &resource,
                    created.job.job_id,
                    VectorMaintenanceClaimCondition::from_snapshot(&created, current.clone()),
                    worker,
                    Uuid::new_v4(),
                )
                .await
                .unwrap()
            else {
                panic!("expected actual claim");
            };
            let (operation, _) = store
                .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let mut mismatched = work.snapshot().clone();
            mismatched.job.resource.name = "different-index".into();
            assert!(
                store
                    .arm_vector_maintenance_test_write_fault_with_guard(
                        &operation,
                        &mismatched,
                        VectorMaintenanceTestWriteTarget::Job,
                        NonZeroU16::new(1).unwrap(),
                        CatalogWriteFaultPhase::AfterNative,
                        CatalogWriteFaultAction::Fail
                    )
                    .await
                    .is_err()
            );
            let fault = store
                .arm_vector_maintenance_test_write_fault_with_guard(
                    &operation,
                    work.snapshot(),
                    VectorMaintenanceTestWriteTarget::Job,
                    NonZeroU16::new(1).unwrap(),
                    CatalogWriteFaultPhase::AfterNative,
                    CatalogWriteFaultAction::Fail,
                )
                .await
                .unwrap();
            drop(operation);
            let dispatch = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
            let beat = store
                .heartbeat_vector_maintenance_job(&dispatch, work.snapshot().job.revision)
                .await
                .unwrap();
            let events = fault.snapshot();
            assert_eq!(events.hits, 1);
            assert_eq!(events.native_succeeded, Some(true));
            assert_eq!(events.response_succeeded, Some(false));
            assert!(events.tail_drained_requested && events.injected_failure);
            assert!(matches!(events.final_fence, Some(CatalogWriteFenceOutcome::Held)));
            assert!(!events.overflow);
            drop(fault);
            let finished = store
                .finish_vector_maintenance_job(
                    &dispatch,
                    beat.job.revision,
                    VectorMaintenanceExecutionOutcome::NoWork {
                        observed: Box::new(current),
                        reason: VectorMaintenanceNoWorkReason::NoCandidates,
                    },
                )
                .await
                .unwrap();
            drop(dispatch);
            let (operation, _) = store
                .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
                .await
                .unwrap();
            let evidence = store
                .inspect_vector_maintenance_evidence_with_guard(&operation, &finished)
                .await
                .unwrap();
            let result = evidence.result.unwrap();
            assert_eq!(result.result.job_id, finished.job.job_id);
            assert_eq!(result.result.attempt, finished.job.attempt);
            assert_eq!(Some(result.observed_object), finished.job.result_ref);
            assert_eq!(result.result.effect, VectorMaintenanceResultEffect::NoWork);
            assert_eq!(result.recorder_job.lease, finished.job.lease);
        });
    }
}
