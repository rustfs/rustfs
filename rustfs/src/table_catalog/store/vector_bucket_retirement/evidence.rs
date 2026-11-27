// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cmp::Ordering;

const PROOF_FACTS_PER_PAGE: usize = 256;
const PROOF_PAGE_BODY_BUDGET: usize = VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES - 65_536;

type Fact = VectorCatalogRetirementProofFactV1;
type Ref = CatalogMetadataRefRecordV1;
type Kind = VectorCatalogRetirementTargetKind;

fn evidence_error(message: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(message.into())
}
fn decode_source<T: serde::de::DeserializeOwned>(object: &TableCatalogObject) -> TableCatalogStoreResult<T> {
    serde_json::from_slice(&object.data).map_err(|error| maintenance_error(&error.to_string()))
}
fn exact_source(key: &str, object: &TableCatalogObject) -> TableCatalogStoreResult<Ref> {
    let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.into())?, object)?;
    if reference.etag().is_none_or(str::is_empty) || reference.content_length() == 0 {
        return Err(maintenance_error("Catalog source lacks exact nonempty bytes and ETag"));
    }
    Ok((&reference).into())
}
fn covers_reference(actual: &Ref, expected: &Ref) -> bool {
    actual.object_key == expected.object_key
        && actual.content_length == expected.content_length
        && actual.content_sha256 == expected.content_sha256
        && expected
            .body_revision
            .is_none_or(|revision| actual.body_revision == Some(revision))
        && expected.etag.as_ref().is_none_or(|etag| actual.etag.as_ref() == Some(etag))
}

#[derive(Clone)]
enum Source {
    Head(Box<TableEntry>),
    Create(Box<VectorIndexCreateIntentV1>),
    Delete(Box<VectorIndexDeleteIntent>),
    Commit(Box<CommitLogEntry>),
    Job(Box<VectorMaintenanceJobV1>),
    Result(Box<VectorMaintenanceResultV1>),
    Evidence(Box<VectorRetirementEvidenceV1>),
    Plan(Box<VectorRetirementPlanV1>),
    Authorization(Box<VectorRetirementAuthorizationV1>),
    Progress(Box<VectorRetirementProgressV1>),
}

fn decode_resource_source(object: &TableCatalogObject) -> TableCatalogStoreResult<Source> {
    let shape: serde_json::Value = decode_source(object)?;
    let source = if shape.get("spec_sha256").is_some() {
        Source::Job(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("recorded_by_job_id").is_some() {
        Source::Result(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("authorization_ref").is_some() {
        Source::Progress(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("created_by_job_id").is_some() {
        Source::Authorization(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("history_refs").is_some() {
        Source::Evidence(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("evidence_ref").is_some() {
        Source::Plan(Box::new(decode_vector_maintenance_record(&object.data)?))
    } else if shape.get("delete_id").is_some() {
        let value: VectorIndexDeleteIntent = decode_source(object)?;
        value.validate()?;
        Source::Delete(Box::new(value))
    } else if shape.get("create_id").is_some() {
        let value: VectorIndexCreateIntentV1 = decode_source(object)?;
        value.validate()?;
        Source::Create(Box::new(value))
    } else if shape.get("commit_id").is_some() {
        Source::Commit(Box::new(decode_source(object)?))
    } else if shape.get("vector_head_binding").is_some() {
        let value: TableEntry = decode_source(object)?;
        CatalogHead::from_vector_entry(&value)?;
        Source::Head(Box::new(value))
    } else {
        return Err(maintenance_error("unrecognized Vector Catalog source record"));
    };
    Ok(source)
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    fn bucket_retirement_source_path_known(&self, bucket: &BucketHandle, key: &str) -> TableCatalogStoreResult<bool> {
        let storage = bucket.storage_bucket_id().as_str();
        let namespace = Namespace::parse("indexes").map_err(|error| maintenance_error(&error.to_string()))?;
        if let Some(tail) = key.strip_prefix(&self.paths.table_entries_prefix(storage, &namespace)) {
            let mut parts = tail.split('/');
            return Ok(matches!((parts.next(), parts.next(), parts.next()), (Some(name), Some(file), None)
                if CatalogName::parse(name).is_ok() && matches!(file, TABLE_ENTRY_FILE | "create-intent.json")));
        }
        let root = self.paths.table_bucket_root_prefix(storage);
        let Some(tail) = key.strip_prefix(&root) else {
            return Ok(false);
        };
        let mut parts = tail.split('/');
        let family = parts.next();
        let owner = parts.next();
        if !owner.is_some_and(|value| decode_catalog_digest(value).is_ok()) {
            return Ok(false);
        }
        let digest_file = |value: &str| {
            value
                .strip_suffix(".json")
                .is_some_and(|value| decode_catalog_digest(value).is_ok())
        };
        if matches!(family, Some(COMMIT_LOG_ROOT | COMMIT_IDEMPOTENCY_ROOT)) {
            return Ok(parts.next().is_some_and(digest_file) && parts.next().is_none());
        }
        if family != Some("resources") {
            return Ok(false);
        }
        match parts.next() {
            Some("delete-intent.json") => Ok(parts.next().is_none()),
            Some("maintenance") if parts.next() == Some("v1") => match parts.next() {
                Some("jobs" | "sources" | "evidence" | "plans" | "authorizations" | "progress") => {
                    Ok(parts.next().is_some_and(digest_file) && parts.next().is_none())
                }
                Some("results") => {
                    let job = parts.next();
                    let attempt = parts.next();
                    Ok(job.is_some_and(|value| decode_catalog_digest(value).is_ok())
                        && attempt.is_some_and(|value| {
                            value
                                .strip_prefix("attempt-")
                                .and_then(|value| value.parse::<u16>().ok())
                                .is_some_and(|value| value > 0)
                        })
                        && parts.next().is_some_and(digest_file)
                        && parts.next().is_none())
                }
                _ => Ok(false),
            },
            _ => Ok(false),
        }
    }

    async fn bucket_source_resource(
        &self,
        bucket: &BucketHandle,
        source: &Source,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceV1> {
        let owned = match source {
            Source::Job(value) => value.resource.clone(),
            Source::Result(value) => value.resource.clone(),
            Source::Evidence(value) => value.resource.clone(),
            Source::Plan(value) => value.resource.clone(),
            Source::Authorization(value) => value.resource.clone(),
            Source::Progress(value) => value.resource.clone(),
            Source::Head(value) => VectorMaintenanceResourceV1::try_new(
                CatalogHead::from_vector_entry(value)?.resource_identity(),
                &Namespace::parse(&value.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&value.table).map_err(|e| maintenance_error(&e.to_string()))?,
            )?,
            Source::Create(value) => VectorMaintenanceResourceV1::try_new(
                &value.resource_identity()?,
                &Namespace::parse(&value.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&value.name).map_err(|e| maintenance_error(&e.to_string()))?,
            )?,
            Source::Delete(value) => VectorMaintenanceResourceV1::try_new(
                CatalogHead::from_vector_entry(&value.deleted_entry)?.resource_identity(),
                &Namespace::parse(&value.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&value.name).map_err(|e| maintenance_error(&e.to_string()))?,
            )?,
            Source::Commit(value) => {
                self.paths
                    .validate_commit_entry(value, bucket.storage_bucket_id().as_str(), &value.table_id)?;
                let binding = value
                    .vector_commit_binding
                    .as_ref()
                    .ok_or_else(|| evidence_error("commit source lacks complete Vector binding"))?;
                if binding.bucket.to_handle()? != *bucket || binding.table_id != value.table_id {
                    return Err(evidence_error("commit source belongs to another complete bucket identity"));
                }
                let path = self
                    .paths
                    .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), &value.table_id);
                let object = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                    .await?
                    .ok_or_else(|| evidence_error("commit resource has no exact deletion intent"))?;
                let intent: VectorIndexDeleteIntent = decode_source(&object)?;
                intent.validate()?;
                VectorMaintenanceResourceV1::try_new(
                    CatalogHead::from_vector_entry(&intent.deleted_entry)?.resource_identity(),
                    &Namespace::parse(&intent.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                    &CatalogName::parse(&intent.name).map_err(|e| maintenance_error(&e.to_string()))?,
                )?
            }
        };
        owned.validate()?;
        if owned.bucket.to_handle()? != *bucket {
            return Err(evidence_error("Catalog source belongs to another complete bucket identity"));
        }
        Ok(owned)
    }

    async fn bucket_deleted_snapshot(
        &self,
        resource: &VectorMaintenanceResourceV1,
    ) -> TableCatalogStoreResult<VectorMaintenanceResourceSnapshot> {
        let current = self
            .maintenance_resource_under_protection(
                &resource.identity()?,
                &Namespace::parse(&resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?,
                &CatalogName::parse(&resource.name).map_err(|e| maintenance_error(&e.to_string()))?,
            )
            .await
            .map_err(|error| match error {
                TableCatalogStoreError::NotFound(_) => evidence_error("resource lacks its complete logical deletion record"),
                error => error,
            })?;
        if current.state != VectorMaintenanceResourceState::Deleted {
            return Err(evidence_error("Catalog retirement requires a completed logical deletion"));
        }
        Ok(current)
    }

    pub(super) async fn classify_bucket_retirement_source(
        &self,
        bucket: &BucketHandle,
        key: &str,
        object: &TableCatalogObject,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetirementTargetV1> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        if object.data.len() > VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES {
            return Err(maintenance_error("Catalog source exceeds its original record limit"));
        }
        let reference = exact_source(key, object)?;
        let storage = bucket.storage_bucket_id().as_str();
        let namespace = Namespace::parse("indexes").map_err(|e| maintenance_error(&e.to_string()))?;
        let (kind, resource) = if key == self.paths.table_bucket_entry_path(storage) {
            let parent: TableBucketEntry = decode_source(object)?;
            let expected = TableBucketEntry {
                vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(bucket)?),
                version: 2,
                table_bucket: storage.into(),
                catalog_type: "s3vectors".into(),
                warehouse_root: format!("s3://{storage}/"),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            };
            if parent != expected {
                return Err(evidence_error("Catalog parent differs from its exact canonical Vector binding"));
            }
            (Kind::BucketEntry, None)
        } else if key == self.paths.namespace_entry_path(storage, &namespace) {
            let entry: NamespaceEntry = decode_source(object)?;
            let expected = NamespaceEntry {
                version: 1,
                table_bucket: storage.into(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            };
            if entry != expected {
                return Err(evidence_error("Catalog namespace differs from its canonical Vector registration"));
            }
            (Kind::NamespaceEntry, None)
        } else {
            if !self.bucket_retirement_source_path_known(bucket, key)? {
                return Err(evidence_error("unrecognized Vector Catalog source path"));
            }
            let source = decode_resource_source(object)?;
            let resource = self.bucket_source_resource(bucket, &source).await?;
            let current = self.bucket_deleted_snapshot(&resource).await?;
            let identity = resource.identity()?;
            let prefix = self.paths.vector_maintenance_prefix(&identity)?;
            let copied = key == format!("{prefix}sources/{}.json", reference.content_sha256);
            let kind = if copied {
                if !matches!(
                    source,
                    Source::Create(_)
                        | Source::Delete(_)
                        | Source::Commit(_)
                        | Source::Job(_)
                        | Source::Authorization(_)
                        | Source::Progress(_)
                ) {
                    return Err(maintenance_error("unsupported maintenance source-copy schema"));
                }
                Kind::MaintenanceSource
            } else {
                match &source {
                    Source::Head(entry) => {
                        let intent: VectorIndexDeleteIntent =
                            decode_source(&self.maintenance_read_ref(&current.observed_object).await?)?;
                        if entry.as_ref() != &intent.deleted_entry {
                            return Err(evidence_error("retained resource head differs from its deletion intent"));
                        }
                        Kind::TableEntry
                    }
                    Source::Create(intent) => {
                        if intent.phase != VectorIndexCreatePhase::Published {
                            return Err(evidence_error("create reservation is unfinished"));
                        }
                        Kind::CreateIntent
                    }
                    Source::Delete(intent) => {
                        if !matches!(intent.phase, VectorIndexDeletePhase::LogicallyDeleted | VectorIndexDeletePhase::Complete)
                            || !covers_reference(&reference, &(&current.observed_object).into())
                        {
                            return Err(evidence_error("delete intent is incomplete or differs from current resource"));
                        }
                        Kind::DeleteIntent
                    }
                    Source::Commit(log) => {
                        if log.status != CommitLogStatus::Committed {
                            return Err(evidence_error("Catalog commit has no complete committed result"));
                        }
                        if key == self.paths.commit_log_entry_path(storage, &resource.table_id, &log.commit_id) {
                            Kind::CommitLog
                        } else if log
                            .idempotency_key
                            .as_ref()
                            .is_some_and(|id| key == self.paths.commit_idempotency_entry_path(storage, &resource.table_id, id))
                        {
                            let canonical = self.paths.commit_log_entry_path(storage, &resource.table_id, &log.commit_id);
                            let original = self
                                .backend
                                .read_object_limited(self.catalog_bucket(), &canonical, VECTOR_MAINTENANCE_RECORD_MAX_BYTES)
                                .await?
                                .ok_or_else(|| evidence_error("idempotency copy has no canonical commit"))?;
                            if decode_source::<CommitLogEntry>(&original)? != **log {
                                return Err(evidence_error("idempotency copy differs from original commit"));
                            }
                            Kind::CommitIdempotency
                        } else {
                            return Err(maintenance_error("commit source path differs from its original identity"));
                        }
                    }
                    Source::Job(job) => {
                        if key != self.paths.vector_maintenance_job_path(&identity, job.job_id)? {
                            return Err(maintenance_error("maintenance job path differs from original identity"));
                        }
                        if !(job.state == VectorMaintenanceJobState::Queued && job.attempt == 0)
                            && !matches!(
                                job.state,
                                VectorMaintenanceJobState::Succeeded | VectorMaintenanceJobState::PartialRecovered
                            )
                        {
                            return Err(evidence_error("maintenance job remains unfinished or unresolved"));
                        }
                        Kind::MaintenanceJob
                    }
                    Source::Result(result) => {
                        let expected = format!(
                            "{prefix}results/{}/attempt-{}/{}.json",
                            table_catalog_path_hash(&result.job_id.to_string()),
                            result.attempt,
                            reference.content_sha256
                        );
                        if key != expected {
                            return Err(maintenance_error("maintenance result path differs from original job"));
                        }
                        Kind::MaintenanceResult
                    }
                    Source::Evidence(_) => Kind::RetirementEvidence,
                    Source::Plan(_) => Kind::RetirementPlan,
                    Source::Authorization(auth) => {
                        if key
                            != format!("{prefix}authorizations/{}.json", table_catalog_path_hash(&auth.retirement_id.to_string()))
                        {
                            return Err(maintenance_error("retirement authorization path mismatch"));
                        }
                        Kind::RetirementAuthorization
                    }
                    Source::Progress(progress) => {
                        if key
                            != format!("{prefix}progress/{}.json", table_catalog_path_hash(&progress.retirement_id.to_string()))
                        {
                            return Err(maintenance_error("retirement progress path mismatch"));
                        }
                        if progress.state != VectorRetirementProgressState::Completed {
                            return Err(evidence_error("data retirement still has unresolved targets"));
                        }
                        Kind::RetirementProgress
                    }
                }
            };
            (kind, Some(resource))
        };
        let target = VectorCatalogRetirementTargetV1 {
            kind,
            resource,
            object_key: reference.object_key,
            etag: reference
                .etag
                .ok_or_else(|| maintenance_error("Catalog target ETag is absent"))?,
            body_revision: reference.body_revision,
            content_sha256: reference.content_sha256,
            content_length: reference.content_length,
        };
        self.validate_bucket_retirement_target_path(&VectorBucketBindingV1::from_handle(bucket)?, &target)?;
        fence.ensure_held()?;
        Ok(target)
    }
}

fn visit_source_dependencies(
    source: &Source,
    mut inspect: impl FnMut(&Ref) -> TableCatalogStoreResult<()>,
) -> TableCatalogStoreResult<()> {
    match source {
        Source::Job(job) => {
            if let Some(source) = &job.input.source {
                inspect(&source.reference)?;
            }
            if let Some(reference) = &job.input.plan_ref {
                inspect(reference)?;
            }
            if let Some(reference) = &job.result_ref {
                inspect(reference)?;
            }
        }
        Source::Result(result) => {
            for reference in result
                .commit_refs
                .iter()
                .chain(&result.retirement_refs)
                .chain(&result.output_refs)
            {
                inspect(reference)?;
            }
            for stage in &result.stages {
                if let Some(reference) = &stage.commit_ref {
                    inspect(reference)?;
                }
            }
        }
        Source::Authorization(auth) => {
            for reference in [&auth.previous_ref, &auth.delete_intent_ref].into_iter().flatten() {
                inspect(reference)?;
            }
            inspect(&auth.plan_ref)?;
            inspect(&auth.reference_evidence_ref)?;
        }
        Source::Progress(progress) => inspect(&progress.authorization_ref)?,
        Source::Plan(plan) => inspect(&plan.evidence_ref)?,
        Source::Evidence(evidence) => {
            if let Some(reference) = &evidence.delete_intent_ref {
                inspect(reference)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn visit_job_references(
    job: &VectorMaintenanceJobV1,
    result: &VectorMaintenanceResultV1,
    mut inspect: impl FnMut(&Ref) -> TableCatalogStoreResult<()>,
) -> TableCatalogStoreResult<()> {
    if let Some(source) = &job.input.source {
        inspect(&source.reference)?;
    }
    if let Some(reference) = &job.input.plan_ref {
        inspect(reference)?;
    }
    if let Some(reference) = &job.result_ref {
        inspect(reference)?;
    }
    for reference in result
        .commit_refs
        .iter()
        .chain(&result.retirement_refs)
        .chain(&result.output_refs)
    {
        inspect(reference)?;
    }
    for stage in &result.stages {
        if let Some(reference) = &stage.commit_ref {
            inspect(reference)?;
        }
    }
    Ok(())
}

fn committed_head(log: &CommitLogEntry) -> TableCatalogStoreResult<VectorMaintenanceHeadV1> {
    let binding = log
        .vector_commit_binding
        .as_ref()
        .ok_or_else(|| maintenance_error("commit lacks Vector binding"))?;
    let head = VectorMaintenanceHeadV1 {
        entry_version: log.version,
        state: log
            .vector_resource_mutation
            .as_ref()
            .map_or(TableCatalogEntryState::Active, |mutation| mutation.next_state().clone()),
        version_token: log.new_version_token.clone(),
        generation: binding
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| maintenance_error("commit generation overflow"))?,
        binding: VectorHeadBindingV1 {
            version: 1,
            bucket: binding.bucket.clone(),
            table_id: binding.table_id.clone(),
            metadata: binding.candidate_metadata.clone(),
            descriptor: binding.descriptor.clone(),
        },
        maintenance: log
            .vector_resource_mutation
            .as_ref()
            .and_then(|mutation| mutation.next_maintenance().cloned()),
    };
    head.validate()?;
    Ok(head)
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn bucket_read_source(
        &self,
        reference: &Ref,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<TableCatalogObject> {
        fence.ensure_held()?;
        let object = self.maintenance_read_ref(&CatalogMetadataRef::try_from(reference)?).await?;
        fence.ensure_held()?;
        Ok(object)
    }

    async fn bucket_job_result(
        &self,
        job: &VectorMaintenanceJobV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Ref, VectorMaintenanceResultV1)> {
        job.validate()?;
        if !matches!(
            job.state,
            VectorMaintenanceJobState::Succeeded | VectorMaintenanceJobState::PartialRecovered
        ) {
            return Err(evidence_error("started maintenance job lacks a completed positive result"));
        }
        let reference = job
            .result_ref
            .as_ref()
            .ok_or_else(|| evidence_error("completed job has no original result"))?;
        let object = self.bucket_read_source(reference, fence).await?;
        let result = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&object.data)?;
        if result.job_id != job.job_id || result.attempt != job.attempt || result.resource != job.resource {
            return Err(evidence_error("job result does not bind the exact attempt and resource"));
        }
        let identity = job.resource.identity()?;
        let prefix = self.paths.vector_maintenance_prefix(&identity)?;
        if reference.object_key
            != format!(
                "{prefix}results/{}/attempt-{}/{}.json",
                table_catalog_path_hash(&job.job_id.to_string()),
                job.attempt,
                reference.content_sha256
            )
        {
            return Err(evidence_error("job result is outside its exact immutable path"));
        }
        if result.recorded_by_job_id != job.job_id {
            let recorder = self
                .maintenance_read_job(&identity, result.recorded_by_job_id, false)
                .await?
                .ok_or_else(|| evidence_error("job result recovery recorder is missing"))?;
            let source = recorder
                .job
                .input
                .source
                .as_ref()
                .ok_or_else(|| evidence_error("result recorder lacks original job source"))?;
            if recorder.job.kind != VectorMaintenanceKind::CommitRecovery
                || source.kind != VectorMaintenanceSourceKind::MaintenanceJob
            {
                return Err(evidence_error("result recorder has no job recovery authority"));
            }
            let original = self.bucket_read_source(&source.reference, fence).await?;
            let original = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&original.data)?;
            if original.job_id != job.job_id
                || original.resource != job.resource
                || original.attempt != job.attempt
                || original.lease != job.lease
                || original.execution_identity != job.execution_identity
                || original.input != job.input
            {
                return Err(evidence_error("job recovery result differs from original input or dispatch identity"));
            }
        }
        // Each list is bounded by the original result validator; references remain exact reads.
        let mut references = Vec::new();
        visit_job_references(job, &result, |reference| {
            references.push(reference.clone());
            Ok(())
        })?;
        for reference in &references {
            let object = self.bucket_read_source(reference, fence).await?;
            let target = self
                .classify_bucket_retirement_source(&job.resource.bucket.to_handle()?, &reference.object_key, &object, fence)
                .await?;
            if target.resource.as_ref() != Some(&job.resource) {
                return Err(evidence_error("job dependency belongs to another resource"));
            }
        }
        for stage in &result.stages {
            if let Some(reference) = &stage.commit_ref {
                let object = self.bucket_read_source(reference, fence).await?;
                let log: CommitLogEntry = decode_source(&object)?;
                self.paths
                    .validate_commit_entry(&log, identity.storage_bucket_id(), identity.table_id().as_str())?;
                if reference.object_key
                    != self.paths.commit_log_entry_path(
                        identity.storage_bucket_id(),
                        identity.table_id().as_str(),
                        &stage.commit_id,
                    )
                    || log.commit_id != stage.commit_id
                    || log.status != CommitLogStatus::Committed
                    || stage.candidate.as_ref() != log.vector_commit_binding.as_ref().map(|binding| &binding.candidate_metadata)
                {
                    return Err(evidence_error("job stage has no exact committed Catalog result"));
                }
                if let Some(expected) = &stage.expected_head {
                    let head = VectorMaintenanceHeadV1::from_stage_commit(expected, &log)?;
                    if head != committed_head(&log)? {
                        return Err(evidence_error("stage successor differs from original expected head"));
                    }
                }
            } else if matches!(
                stage.outcome,
                VectorMaintenanceStageOutcome::Committed | VectorMaintenanceStageOutcome::Superseded
            ) {
                return Err(evidence_error("published stage lacks original commit evidence"));
            }
        }
        Ok((exact_source(&reference.object_key, &object)?, result))
    }

    async fn bucket_completed_retirement(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        reference: &Ref,
        auth: &VectorRetirementAuthorizationV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Ref, VectorRetirementProgressV1)> {
        auth.validate()?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| evidence_error("resource has no retirement publication"))?;
        let mut published = false;
        self.visit_vector_retirements(&current.resource, binding, |original| {
            if original.retirement_id == auth.retirement_id {
                if original != auth {
                    return Err(evidence_error("authorization differs from original published retirement"));
                }
                published = true;
            }
            Ok(())
        })
        .await?;
        if !published {
            return Err(evidence_error("retirement authorization is unpublished"));
        }
        let plan = self.bucket_read_source(&auth.plan_ref, fence).await?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&plan.data)?;
        let evidence = self.bucket_read_source(&auth.reference_evidence_ref, fence).await?;
        let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&evidence.data)?;
        if auth.resource.identity()? != current.resource
            || plan.resource != auth.resource
            || evidence.resource != auth.resource
            || plan.targets != auth.targets
            || plan.evidence_ref != auth.reference_evidence_ref
            || evidence.base_head != auth.base_head
            || evidence.delete_intent_ref != auth.delete_intent_ref
        {
            return Err(evidence_error("retirement plan or evidence differs from original authorization"));
        }
        let creator = self
            .maintenance_read_job(&current.resource, auth.created_by_job_id, false)
            .await?
            .ok_or_else(|| evidence_error("retirement creator job is absent"))?;
        if creator.job.resource != auth.resource
            || creator.job.attempt != auth.created_by_attempt
            || creator.job.execution_identity != auth.execution_identity
            || creator.job.input.plan_ref.as_ref() != Some(&auth.plan_ref)
            || !matches!(creator.job.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
        {
            return Err(evidence_error("retirement creator identity or original plan differs"));
        }
        let path = format!(
            "{}progress/{}.json",
            self.paths.vector_maintenance_prefix(&current.resource)?,
            table_catalog_path_hash(&auth.retirement_id.to_string())
        );
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
            .await?
            .ok_or_else(|| evidence_error("retirement progress is missing"))?;
        let progress = decode_vector_maintenance_record::<VectorRetirementProgressV1>(&object.data)?;
        if progress.resource != auth.resource
            || progress.retirement_id != auth.retirement_id
            || progress.authorization_ref != *reference
            || progress.targets.len() != auth.targets.len()
            || progress.state != VectorRetirementProgressState::Completed
        {
            return Err(evidence_error("retirement progress has no complete original confirmation"));
        }
        for (ordinal, target) in progress.targets.iter().enumerate() {
            let dispatch = target
                .dispatch
                .as_ref()
                .ok_or_else(|| evidence_error("terminal retirement target lacks original dispatch"))?;
            let receipt = target
                .receipt
                .as_ref()
                .ok_or_else(|| evidence_error("terminal retirement target lacks original receipt"))?;
            receipt.validate()?;
            if vector_maintenance_timestamp(&receipt.completed_at)? < vector_maintenance_timestamp(&dispatch.admitted_at)?
                || vector_maintenance_timestamp(&dispatch.admitted_at)? < vector_maintenance_timestamp(&auth.created_at)?
                || receipt.accounting.as_ref().is_some_and(|accounting| {
                    accounting
                        .size
                        .is_some_and(|size| size != auth.targets[ordinal].content_length)
                        || accounting.version_id != auth.targets[ordinal].version_id
                })
            {
                return Err(evidence_error(
                    "retirement receipt differs from original dispatch time or target accounting",
                ));
            }
            if usize::from(target.ordinal) != ordinal
                || target.failure.is_some()
                || !matches!(
                    (target.state, receipt.outcome),
                    (
                        VectorRetirementTargetState::DeleteCommitted,
                        VectorRetirementReceiptOutcome::DeleteCommitted
                    ) | (
                        VectorRetirementTargetState::AbsentAfterRecovery,
                        VectorRetirementReceiptOutcome::AbsentAfterRecovery
                    )
                )
                || receipt.recovery_pending != 0
                || receipt.recovery_blocked != 0
                || receipt.recovery_failed != 0
            {
                return Err(evidence_error("retirement target is unresolved or lacks native confirmation"));
            }
            let job = self
                .maintenance_read_job(&current.resource, dispatch.job_id, false)
                .await?
                .ok_or_else(|| evidence_error("retirement dispatch job is absent"))?;
            if job.job.resource != auth.resource {
                return Err(evidence_error("retirement dispatch resource differs"));
            }
            let lease = VectorMaintenanceLeaseToken::from_job(&job.job)?;
            if lease.attempt != dispatch.attempt
                || lease.lease_id != dispatch.lease_id
                || lease.worker_id != dispatch.worker_id
                || lease.worker_boot_id != dispatch.worker_boot_id
                || lease.execution_identity != dispatch.execution_identity
            {
                return Err(evidence_error("original retirement dispatch lease is unavailable"));
            }
            if job.job.job_id != auth.created_by_job_id {
                let source = job
                    .job
                    .input
                    .source
                    .as_ref()
                    .ok_or_else(|| evidence_error("retirement recovery lacks authorization source"))?;
                if job.job.kind != VectorMaintenanceKind::DeleteRecovery
                    || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
                {
                    return Err(evidence_error("retirement dispatch lacks original recovery authority"));
                }
                let object = self.bucket_read_source(&source.reference, fence).await?;
                if decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)? != *auth {
                    return Err(evidence_error("retirement recovery source differs from original authorization"));
                }
            }
            if !vector_retirement_target_is_old_enough(
                &auth.targets[ordinal],
                evidence.retention.clone(),
                vector_maintenance_timestamp(&auth.created_at)?,
            )? {
                return Err(evidence_error("original retirement did not satisfy its safety window"));
            }
        }
        fence.ensure_held()?;
        Ok((exact_source(&path, &object)?, progress))
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn bucket_require_source_copy_owner(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        reference: &Ref,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let mut after = None;
        let mut found = false;
        while let Some((key, object)) = self.next_bucket_catalog_source(bucket, after.as_deref(), fence).await? {
            after = Some(key.clone());
            let target = self.classify_bucket_retirement_source(bucket, &key, &object, fence).await?;
            if target.resource.as_ref() != Some(resource) || key == reference.object_key {
                continue;
            }
            let source = decode_resource_source(&object)?;
            visit_source_dependencies(&source, |dependency| {
                if covers_reference(reference, dependency) {
                    found = true;
                }
                Ok(())
            })?;
        }
        if !found {
            return Err(evidence_error("immutable source copy has no exact retained owner"));
        }
        Ok(())
    }

    async fn bucket_verify_deleted_data(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| evidence_error("Deleted resource lacks retirement binding"))?;
        let evidence = self
            .read_vector_deleted_retirement_evidence_under_protection(current, fence)
            .await?
            .ok_or_else(|| evidence_error("Deleted resource lacks original complete retirement evidence"))?;
        if !evidence.unresolved_refs.is_empty() || !evidence.protected_refs.is_empty() {
            return Err(evidence_error("Deleted resource still has unresolved or protected data references"));
        }
        for history in &evidence.history_refs {
            let mut next = binding.retirement_tip.clone();
            let mut confirmed = false;
            for _ in 0..binding.retirement_count {
                let reference = next
                    .take()
                    .ok_or_else(|| evidence_error("original retirement chain ended early"))?;
                let object = self.bucket_read_source(&reference, fence).await?;
                let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
                let (_, progress) = self.bucket_completed_retirement(current, &reference, &auth, fence).await?;
                for (target, completed) in auth.targets.iter().zip(&progress.targets) {
                    if target.object_key == history.object_key
                        && Some(target.body_revision) == history.body_revision
                        && target.content_length == history.content_length
                        && history.etag.as_ref().is_none_or(|etag| target.etag.as_ref() == Some(etag))
                        && completed.receipt.is_some()
                    {
                        confirmed = true;
                    }
                }
                next = auth.previous_ref;
            }
            if next.is_some() || !confirmed {
                return Err(evidence_error("committed data reference lacks exact original retirement confirmation"));
            }
        }
        fence.ensure_held()
    }

    async fn bucket_visit_resource_facts(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        fence: &TableCatalogObjectMutationFence,
        mut inspect: impl FnMut(Fact) -> TableCatalogStoreResult<()> + Send,
    ) -> TableCatalogStoreResult<()> {
        let current = self.bucket_deleted_snapshot(resource).await?;
        let head = current
            .head
            .clone()
            .ok_or_else(|| evidence_error("Deleted resource has no retained head"))?;
        inspect(Fact::ResourceAnchor {
            delete_intent: (&current.observed_object).into(),
            deleted_head: Box::new(head),
        })?;
        let mut after = None;
        while let Some((key, object)) = self.next_bucket_catalog_source(bucket, after.as_deref(), fence).await? {
            let target = self.classify_bucket_retirement_source(bucket, &key, &object, fence).await?;
            after = Some(key.clone());
            if target.resource.as_ref() != Some(resource) {
                continue;
            }
            let reference = catalog_retirement_target_reference(&target);
            inspect(Fact::SourceRecord {
                reference: reference.clone(),
            })?;
            let source = decode_resource_source(&object)?;
            match (target.kind, source) {
                (Kind::MaintenanceSource, _) => {
                    self.bucket_require_source_copy_owner(bucket, resource, &reference, fence)
                        .await?;
                }
                (Kind::CommitLog, Source::Commit(log)) => {
                    let binding = log
                        .vector_commit_binding
                        .as_ref()
                        .ok_or_else(|| maintenance_error("commit lacks exact Vector binding"))?;
                    inspect(Fact::Commit {
                        commit_id: log.commit_id.clone(),
                        request_sha256: binding.request_digest_sha256.clone(),
                        log_ref: reference,
                        committed_head: Box::new(committed_head(&log)?),
                        history_ref_count: 1 + u64::from(binding.expected_metadata.is_some()),
                    })?;
                    inspect(Fact::CommitHistoryReference {
                        commit_id: log.commit_id.clone(),
                        ordinal: 0,
                        reference: binding.candidate_metadata.clone(),
                    })?;
                    if let Some(expected) = &binding.expected_metadata {
                        inspect(Fact::CommitHistoryReference {
                            commit_id: log.commit_id,
                            ordinal: 1,
                            reference: expected.clone(),
                        })?;
                    }
                }
                (Kind::MaintenanceJob, Source::Job(job)) => {
                    if job.state == VectorMaintenanceJobState::Queued && job.attempt == 0 {
                        if job.lease.is_some() || job.result_ref.is_some() || job.failure.is_some() {
                            return Err(evidence_error("unstarted job contains execution evidence"));
                        }
                        inspect(Fact::UnstartedJob { reference })?;
                    } else {
                        let (result_ref, result) = self.bucket_job_result(&job, fence).await?;
                        let mut count = 0_u64;
                        visit_job_references(&job, &result, |_| {
                            count = count
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                            Ok(())
                        })?;
                        inspect(Fact::Job {
                            job_ref: reference,
                            job_id: job.job_id,
                            revision: job.revision,
                            attempt: job.attempt,
                            lease_id: job.lease.as_ref().map(|lease| lease.lease_id),
                            result_ref,
                            source_ref_count: count,
                        })?;
                        let mut ordinal = 0;
                        visit_job_references(&job, &result, |reference| {
                            inspect(Fact::JobSourceReference {
                                job_id: job.job_id,
                                revision: job.revision,
                                ordinal,
                                reference: reference.clone(),
                            })?;
                            ordinal += 1;
                            Ok(())
                        })?;
                    }
                }
                (Kind::RetirementAuthorization, Source::Authorization(auth)) => {
                    let (progress_ref, progress) = self.bucket_completed_retirement(&current, &reference, &auth, fence).await?;
                    inspect(Fact::Retirement {
                        authorization_ref: reference,
                        progress_ref,
                        retirement_id: auth.retirement_id,
                        revision: progress.revision,
                        receipt_count: u64::try_from(progress.targets.len())
                            .map_err(|_| maintenance_error("receipt count overflow"))?,
                    })?;
                    for target in progress.targets {
                        inspect(Fact::RetirementReceipt {
                            retirement_id: auth.retirement_id,
                            revision: progress.revision,
                            ordinal: u64::from(target.ordinal),
                            receipt: target
                                .receipt
                                .ok_or_else(|| evidence_error("completed target receipt missing"))?,
                        })?;
                    }
                }
                (Kind::RetirementEvidence | Kind::RetirementPlan, _) => {
                    let mut referenced = false;
                    let binding = current
                        .maintenance
                        .as_ref()
                        .ok_or_else(|| evidence_error("retirement source lacks publication binding"))?;
                    self.visit_vector_retirements(&current.resource, binding, |auth| {
                        if covers_reference(&reference, &auth.plan_ref)
                            || covers_reference(&reference, &auth.reference_evidence_ref)
                        {
                            referenced = true;
                        }
                        Ok(())
                    })
                    .await?;
                    if !referenced {
                        return Err(evidence_error("retirement plan or evidence has no published owner"));
                    }
                }
                (Kind::MaintenanceResult, Source::Result(result)) => {
                    let job = self
                        .maintenance_read_job(&current.resource, result.job_id, false)
                        .await?
                        .ok_or_else(|| evidence_error("result owner job is missing"))?;
                    if job
                        .job
                        .result_ref
                        .as_ref()
                        .is_none_or(|expected| !covers_reference(&reference, expected))
                    {
                        return Err(evidence_error("result is not bound to the retained completed job"));
                    }
                    self.bucket_job_result(&job.job, fence).await?;
                }
                (Kind::RetirementProgress, Source::Progress(progress)) => {
                    let object = self.bucket_read_source(&progress.authorization_ref, fence).await?;
                    let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
                    let (observed, _) = self
                        .bucket_completed_retirement(&current, &progress.authorization_ref, &auth, fence)
                        .await?;
                    if observed != reference {
                        return Err(evidence_error("progress differs from original exact retirement progress"));
                    }
                }
                _ => {}
            }
        }
        fence.ensure_held()
    }

    async fn bucket_resource_page(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        after: Option<&Fact>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Vec<Fact>> {
        let mut page: Vec<Fact> = Vec::new();
        let mut bytes = 0_usize;
        self.bucket_visit_resource_facts(bucket, resource, fence, |fact| retain_proof_fact(&mut page, &mut bytes, after, fact))
            .await?;
        Ok(page)
    }

    pub(super) async fn prepare_bucket_retirement_resource_proof(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        retirement_id: Uuid,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetiredResourceProofV1> {
        self.require_bucket_retirement_identity(bucket)?;
        resource.validate()?;
        if resource.bucket.to_handle()? != *bucket || retirement_id.is_nil() {
            return Err(maintenance_error("proof complete bucket identity mismatch"));
        }
        let current = self.bucket_deleted_snapshot(resource).await?;
        self.bucket_verify_deleted_data(&current, fence).await?;
        let mut count = 0_u64;
        let mut pages = 0_u32;
        let mut previous = None;
        loop {
            let page = self.bucket_resource_page(bucket, resource, previous.as_ref(), fence).await?;
            if page.is_empty() {
                break;
            }
            count = count
                .checked_add(u64::try_from(page.len()).map_err(|_| maintenance_error("proof count overflow"))?)
                .ok_or_else(|| maintenance_error("proof count overflow"))?;
            pages = pages
                .checked_add(1)
                .ok_or_else(|| maintenance_error("proof page count overflow"))?;
            previous = page.last().cloned();
        }
        let proof_id = Uuid::new_v4();
        let binding = VectorBucketBindingV1::from_handle(bucket)?;
        let mut digest = CatalogRetirementProofDigest::new(&binding, resource, retirement_id, proof_id, count)?;
        let mut previous = None;
        loop {
            let page = self.bucket_resource_page(bucket, resource, previous.as_ref(), fence).await?;
            if page.is_empty() {
                break;
            }
            for fact in &page {
                digest.push(fact)?;
            }
            previous = page.last().cloned();
        }
        let proof_sha256 = digest.finish()?;
        let mut next_page = None;
        // Regenerate bounded pages under the same writer; retain only one page and its next reference.
        for wanted in (0..pages).rev() {
            let mut previous = None;
            let mut first_fact_ordinal = 0_u64;
            for ordinal in 0..=wanted {
                let facts = self.bucket_resource_page(bucket, resource, previous.as_ref(), fence).await?;
                if facts.is_empty() {
                    return Err(evidence_error("proof source changed while building immutable pages"));
                }
                if ordinal != wanted {
                    first_fact_ordinal = first_fact_ordinal
                        .checked_add(u64::try_from(facts.len()).map_err(|_| maintenance_error("proof ordinal overflow"))?)
                        .ok_or_else(|| maintenance_error("proof ordinal overflow"))?;
                    previous = facts.last().cloned();
                    continue;
                }
                let page_id = Uuid::new_v4();
                let page = VectorCatalogRetiredResourceProofPageV1 {
                    version: 1,
                    bucket: binding.clone(),
                    retirement_id,
                    resource: resource.clone(),
                    proof_id,
                    page_id,
                    page_ordinal: ordinal,
                    page_count: pages,
                    first_fact_ordinal,
                    total_fact_count: count,
                    proof_sha256: proof_sha256.clone(),
                    next_page: next_page.take(),
                    facts,
                };
                let path = self
                    .paths
                    .vector_bucket_retirement_proof_path(bucket, retirement_id, proof_id, page_id)?;
                next_page = Some((&self.write_bucket_retirement_record(&path, None, &page, fence).await?).into());
            }
        }
        fence.ensure_held()?;
        Ok(VectorCatalogRetiredResourceProofV1 {
            resource: resource.clone(),
            proof_id,
            proof_root: next_page.ok_or_else(|| evidence_error("empty resource proof"))?,
            proof_page_count: pages,
            fact_count: count,
            proof_sha256,
        })
    }
}

fn retain_proof_fact(page: &mut Vec<Fact>, bytes: &mut usize, after: Option<&Fact>, fact: Fact) -> TableCatalogStoreResult<()> {
    fact.validate()?;
    if after.is_some_and(|previous| fact.compare_order(previous) != Ordering::Greater) {
        return Ok(());
    }
    let ordinal = match page.binary_search_by(|previous| previous.compare_order(&fact)) {
        Ok(_) => return Err(evidence_error("duplicate proof fact identity")),
        Err(ordinal) => ordinal,
    };
    let length = fact_bytes(&fact)?;
    if length > PROOF_PAGE_BODY_BUDGET {
        return Err(maintenance_error("single proof fact exceeds its bounded page budget"));
    }
    if ordinal == page.len() && (page.len() == PROOF_FACTS_PER_PAGE || *bytes + length > PROOF_PAGE_BODY_BUDGET) {
        return Ok(());
    }
    *bytes += length;
    page.insert(ordinal, fact);
    while page.len() > PROOF_FACTS_PER_PAGE || *bytes > PROOF_PAGE_BODY_BUDGET {
        let removed = page
            .pop()
            .ok_or_else(|| maintenance_error("proof page budget accounting failed"))?;
        *bytes -= fact_bytes(&removed)?;
    }
    Ok(())
}

fn fact_bytes(fact: &Fact) -> TableCatalogStoreResult<usize> {
    serde_json::to_vec(fact)
        .map(|bytes| bytes.len() + 1)
        .map_err(|error| maintenance_error(&error.to_string()))
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn bucket_require_proof_source(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        expected: &Ref,
        kind: Option<Kind>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let target = self
            .bucket_retirement_target_by_key(authority, &expected.object_key, fence)
            .await?
            .ok_or_else(|| evidence_error("proof dependency has no exact planned Catalog target"))?;
        if target.target.resource.as_ref() != Some(&descriptor.resource)
            || kind.is_some_and(|kind| kind != target.target.kind)
            || if kind.is_some() {
                catalog_retirement_target_reference(&target.target) != *expected
            } else {
                !covers_reference(&catalog_retirement_target_reference(&target.target), expected)
            }
        {
            return Err(evidence_error("proof dependency differs from its planned identity or owner"));
        }
        self.require_bucket_retirement_source_fact(
            authority,
            descriptor,
            &catalog_retirement_target_reference(&target.target),
            fence,
        )
        .await
    }

    async fn bucket_validate_fact_edges(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        fact: &Fact,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        match fact {
            Fact::ResourceAnchor {
                delete_intent,
                deleted_head,
            } => {
                self.bucket_require_proof_source(authority, descriptor, delete_intent, Some(Kind::DeleteIntent), fence)
                    .await?;
                let mut final_head = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::Commit { committed_head, .. } = other {
                        if committed_head.generation > deleted_head.generation {
                            return Err(evidence_error("commit follows retained Deleted head"));
                        }
                        if committed_head.generation == deleted_head.generation {
                            if committed_head != deleted_head {
                                return Err(evidence_error("last committed head differs from Deleted anchor"));
                            }
                            final_head = final_head
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                        }
                    }
                    Ok(())
                })
                .await?;
                if final_head != 1 {
                    return Err(evidence_error("Deleted anchor lacks one exact final committed head"));
                }
            }
            Fact::SourceRecord { reference } => {
                self.bucket_require_proof_source(authority, descriptor, reference, None, fence)
                    .await?;
                let planned = self
                    .bucket_retirement_target_by_key(authority, &reference.object_key, fence)
                    .await?
                    .ok_or_else(|| evidence_error("source target disappeared from plan"))?;
                if catalog_retirement_target_reference(&planned.target) != *reference {
                    return Err(evidence_error("source proof omits an exact original identity field"));
                }
                let mut owners = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    let owned = match (planned.target.kind, other) {
                        (Kind::DeleteIntent, Fact::ResourceAnchor { delete_intent, .. }) => delete_intent == reference,
                        (Kind::CommitLog, Fact::Commit { log_ref, .. }) => log_ref == reference,
                        (Kind::MaintenanceJob, Fact::Job { job_ref, .. }) => job_ref == reference,
                        (Kind::MaintenanceJob, Fact::UnstartedJob { reference: job_ref }) => job_ref == reference,
                        (Kind::RetirementAuthorization, Fact::Retirement { authorization_ref, .. }) => {
                            authorization_ref == reference
                        }
                        (Kind::RetirementProgress, Fact::Retirement { progress_ref, .. }) => progress_ref == reference,
                        _ => false,
                    };
                    if owned {
                        owners = owners
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    Ok(())
                })
                .await?;
                if matches!(
                    planned.target.kind,
                    Kind::DeleteIntent
                        | Kind::CommitLog
                        | Kind::MaintenanceJob
                        | Kind::RetirementAuthorization
                        | Kind::RetirementProgress
                ) && owners != 1
                {
                    return Err(evidence_error("source lacks one exact proof owner"));
                }
            }
            Fact::Commit {
                commit_id,
                log_ref,
                committed_head,
                history_ref_count,
                ..
            } => {
                self.bucket_require_proof_source(authority, descriptor, log_ref, Some(Kind::CommitLog), fence)
                    .await?;
                let identity = descriptor.resource.identity()?;
                if log_ref.object_key
                    != self
                        .paths
                        .commit_log_entry_path(identity.storage_bucket_id(), identity.table_id().as_str(), commit_id)
                {
                    return Err(evidence_error("commit header identity differs from its exact original path"));
                }
                let mut count = 0_u64;
                let mut predecessor = 0_u64;
                let mut same_generation = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::CommitHistoryReference {
                        commit_id: owner,
                        ordinal,
                        reference,
                    } = other
                        && owner == commit_id
                    {
                        if *ordinal != count {
                            return Err(evidence_error("commit history edge ordinal is missing or repeated"));
                        }
                        if *ordinal == 0 && *reference != committed_head.binding.metadata {
                            return Err(evidence_error("commit candidate edge differs from committed head"));
                        }
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    if let Fact::Commit {
                        committed_head: previous,
                        ..
                    } = other
                    {
                        if previous.generation == committed_head.generation {
                            same_generation = same_generation
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                        }
                        if previous.generation.checked_add(1) == Some(committed_head.generation) {
                            predecessor = predecessor
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                        }
                    }
                    Ok(())
                })
                .await?;
                if count != *history_ref_count
                    || !(1..=2).contains(&count)
                    || same_generation != 1
                    || (committed_head.generation > 2 && predecessor != 1)
                    || committed_head.generation < 2
                {
                    return Err(evidence_error("commit proof omits history edges or a unique predecessor"));
                }
            }
            Fact::CommitHistoryReference {
                commit_id,
                ordinal,
                reference,
            } => {
                let mut owner = None;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::Commit {
                        commit_id: id,
                        committed_head,
                        history_ref_count,
                        ..
                    } = other
                        && id == commit_id
                    {
                        if owner.is_some() || *ordinal >= *history_ref_count {
                            return Err(evidence_error("history edge owner is ambiguous or count differs"));
                        }
                        owner = Some(committed_head.clone());
                    }
                    Ok(())
                })
                .await?;
                let owner = owner.ok_or_else(|| evidence_error("orphan commit history edge"))?;
                if *ordinal == 1 && owner.generation > 2 {
                    let mut matched = 0_u64;
                    self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                        if let Fact::Commit { committed_head, .. } = other
                            && committed_head.generation.checked_add(1) == Some(owner.generation)
                        {
                            if committed_head.binding.metadata != *reference {
                                return Err(evidence_error("history predecessor reference differs from prior committed head"));
                            }
                            matched = matched
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                        }
                        Ok(())
                    })
                    .await?;
                    if matched != 1 {
                        return Err(evidence_error("history edge has no exact predecessor"));
                    }
                }
                // Data references retain their original optional ETag and are bound by the original retirement proof.
                let expected_prefix = descriptor.resource.identity()?.dataset_prefix();
                if !reference.object_key.starts_with(&expected_prefix) {
                    return Err(evidence_error("commit history reference is outside its managed resource"));
                }
            }
            Fact::Job {
                job_ref,
                job_id,
                revision,
                result_ref,
                source_ref_count,
                ..
            } => {
                self.bucket_require_proof_source(authority, descriptor, job_ref, Some(Kind::MaintenanceJob), fence)
                    .await?;
                if job_ref.object_key
                    != self
                        .paths
                        .vector_maintenance_job_path(&descriptor.resource.identity()?, *job_id)?
                {
                    return Err(evidence_error("job header identity differs from its exact original path"));
                }
                self.bucket_require_proof_source(authority, descriptor, result_ref, Some(Kind::MaintenanceResult), fence)
                    .await?;
                let mut count = 0_u64;
                let mut result_edge = false;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::JobSourceReference {
                        job_id: owner,
                        revision: version,
                        ordinal,
                        reference,
                    } = other
                        && owner == job_id
                        && version == revision
                    {
                        if *ordinal != count {
                            return Err(evidence_error("job source edge ordinal is missing or repeated"));
                        }
                        if covers_reference(result_ref, reference) {
                            result_edge = true;
                        }
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    Ok(())
                })
                .await?;
                if count != *source_ref_count || !result_edge {
                    return Err(evidence_error("job source closure omits its declared edges or original result"));
                }
            }
            Fact::JobSourceReference {
                job_id,
                revision,
                ordinal,
                reference,
            } => {
                self.bucket_require_proof_source(authority, descriptor, reference, None, fence)
                    .await?;
                let mut owners = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::Job {
                        job_id: owner,
                        revision: version,
                        source_ref_count,
                        ..
                    } = other
                        && owner == job_id
                        && version == revision
                    {
                        if *ordinal >= *source_ref_count {
                            return Err(evidence_error("job edge exceeds owner count"));
                        }
                        owners = owners
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    Ok(())
                })
                .await?;
                if owners != 1 {
                    return Err(evidence_error("job source edge lacks one exact owner"));
                }
            }
            Fact::Retirement {
                authorization_ref,
                progress_ref,
                retirement_id,
                revision,
                receipt_count,
            } => {
                self.bucket_require_proof_source(
                    authority,
                    descriptor,
                    authorization_ref,
                    Some(Kind::RetirementAuthorization),
                    fence,
                )
                .await?;
                self.bucket_require_proof_source(authority, descriptor, progress_ref, Some(Kind::RetirementProgress), fence)
                    .await?;
                let prefix = self.paths.vector_maintenance_prefix(&descriptor.resource.identity()?)?;
                let owner_key = table_catalog_path_hash(&retirement_id.to_string());
                if authorization_ref.object_key != format!("{prefix}authorizations/{owner_key}.json")
                    || progress_ref.object_key != format!("{prefix}progress/{owner_key}.json")
                {
                    return Err(evidence_error("retirement header identity differs from its exact original paths"));
                }
                let mut count = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::RetirementReceipt {
                        retirement_id: owner,
                        revision: version,
                        ordinal,
                        ..
                    } = other
                        && owner == retirement_id
                        && version == revision
                    {
                        if *ordinal != count {
                            return Err(evidence_error("retirement receipt ordinal is missing or repeated"));
                        }
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    Ok(())
                })
                .await?;
                if count != *receipt_count || count == 0 {
                    return Err(evidence_error("retirement proof lacks its complete receipt set"));
                }
            }
            Fact::RetirementReceipt {
                retirement_id,
                revision,
                ordinal,
                receipt,
            } => {
                receipt.validate()?;
                if receipt.recovery_pending != 0 || receipt.recovery_blocked != 0 || receipt.recovery_failed != 0 {
                    return Err(evidence_error("retirement receipt contains unresolved recovery"));
                }
                let mut owners = 0_u64;
                self.visit_bucket_retirement_proof(authority, descriptor, fence, |other| {
                    if let Fact::Retirement {
                        retirement_id: owner,
                        revision: version,
                        receipt_count,
                        ..
                    } = other
                        && owner == retirement_id
                        && version == revision
                    {
                        if *ordinal >= *receipt_count {
                            return Err(evidence_error("receipt exceeds original target count"));
                        }
                        owners = owners
                            .checked_add(1)
                            .ok_or_else(|| maintenance_error("proof count overflow"))?;
                    }
                    Ok(())
                })
                .await?;
                if owners != 1 {
                    return Err(evidence_error("retirement receipt lacks one exact owner"));
                }
            }
            Fact::UnstartedJob { reference } => {
                self.bucket_require_proof_source(authority, descriptor, reference, Some(Kind::MaintenanceJob), fence)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn validate_bucket_retirement_resource_closure(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        self.visit_bucket_retirement_proof(authority, descriptor, fence, |_| Ok(()))
            .await?;
        let mut next = Some(descriptor.proof_root.clone());
        for _ in 0..descriptor.proof_page_count {
            let reference = next.take().ok_or_else(|| evidence_error("resource proof ended early"))?;
            let page: VectorCatalogRetiredResourceProofPageV1 = self.read_bucket_retirement_record(&reference, fence).await?;
            for fact in &page.facts {
                self.bucket_validate_fact_edges(authority, descriptor, fact, fence).await?;
            }
            next = page.next_page;
        }
        if next.is_some() {
            return Err(evidence_error("resource proof exceeds declared closure"));
        }
        fence.ensure_held()
    }
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
pub(super) mod tests;
