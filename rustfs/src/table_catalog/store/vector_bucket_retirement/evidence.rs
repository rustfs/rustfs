// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
#[cfg(test)]
use std::cmp::Ordering;

type ResourceSnapshotCache = BTreeMap<(String, String, String), (VectorMaintenanceResourceSnapshot, VectorIndexDeleteIntent)>;

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

pub(super) struct IndexedRetiredResource {
    pub(super) snapshot: VectorMaintenanceResourceSnapshot,
    items: Vec<(VectorCatalogRetirementTargetV1, Source)>,
    dependencies_by_key: BTreeMap<String, Vec<Ref>>,
}

pub(super) struct IndexedBucketCatalog {
    raw_objects: BTreeMap<String, TableCatalogObject>,
    pub(super) targets_in_order: Vec<VectorCatalogRetirementTargetV1>,
    classified_by_key: BTreeMap<String, VectorCatalogRetirementTargetV1>,
    pub(super) by_resource: BTreeMap<(String, String, String), IndexedRetiredResource>,
}

impl IndexedBucketCatalog {
    fn read_exact_ref<'a>(
        &'a self,
        reference: &Ref,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<&'a TableCatalogObject> {
        fence.ensure_held()?;
        let exact = CatalogMetadataRef::try_from(reference)?;
        let object = self
            .raw_objects
            .get(&reference.object_key)
            .ok_or_else(|| TableCatalogStoreError::NotFound("exact maintenance reference".into()))?;
        exact.verify_read(object)?;
        fence.ensure_held()?;
        Ok(object)
    }

    fn read_job(
        &self,
        paths: &TableCatalogObjectPaths,
        resource: &CatalogResourceIdentity,
        job_id: Uuid,
    ) -> TableCatalogStoreResult<Option<VectorMaintenanceJobV1>> {
        let path = paths.vector_maintenance_job_path(resource, job_id)?;
        let Some(object) = self.raw_objects.get(&path) else {
            return Ok(None);
        };
        if object.data.len() > VECTOR_MAINTENANCE_JOB_MAX_BYTES {
            return Err(maintenance_error("Catalog maintenance job exceeds byte limit"));
        }
        let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&object.data)?;
        if job.resource().identity()? != *resource || *job.job_id() != job_id {
            return Err(TableCatalogStoreError::Conflict("maintenance job path or resource mismatch".into()));
        }
        if object.etag.as_deref().is_none_or(str::is_empty) {
            return Err(maintenance_error("maintenance job requires an ETag"));
        }
        Ok(Some(job))
    }
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

    async fn read_source_object_from_raw_or_backend(
        &self,
        path: &str,
        max_bytes: usize,
        raw_objects: Option<&BTreeMap<String, TableCatalogObject>>,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        if let Some(raw_objects) = raw_objects {
            let Some(object) = raw_objects.get(path) else {
                return Ok(None);
            };
            if object.data.len() > max_bytes {
                return Err(maintenance_error("Catalog source exceeds its maximum record size"));
            }
            return Ok(Some(object.clone()));
        }
        self.backend.read_object_limited(self.catalog_bucket(), path, max_bytes).await
    }

    async fn bucket_source_resource_with_raw(
        &self,
        bucket: &BucketHandle,
        source: &Source,
        raw_objects: Option<&BTreeMap<String, TableCatalogObject>>,
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
                    .read_source_object_from_raw_or_backend(&path, VECTOR_MAINTENANCE_RECORD_MAX_BYTES, raw_objects)
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

    fn deleted_snapshot_from_raw_objects(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        raw_objects: &BTreeMap<String, TableCatalogObject>,
    ) -> TableCatalogStoreResult<(VectorMaintenanceResourceSnapshot, VectorIndexDeleteIntent)> {
        let identity = resource.identity()?;
        let namespace = Namespace::parse(&resource.namespace).map_err(|e| maintenance_error(&e.to_string()))?;
        if namespace.public_name() != "indexes" {
            return Err(maintenance_error("maintenance namespace must be indexes"));
        }
        let name = CatalogName::parse(&resource.name).map_err(|e| maintenance_error(&e.to_string()))?;
        let table_path = self.paths.table_entry_path(identity.storage_bucket_id(), &namespace, &name);
        if let Some(object) = raw_objects.get(&table_path) {
            if object.data.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
                return Err(maintenance_error("Catalog table entry exceeds maximum record size"));
            }
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|e| maintenance_error(&format!("decode maintenance resource: {e}")))?;
            if entry.table_id == identity.table_id().as_str() {
                let head = CatalogHead::from_vector_entry(&entry)?;
                if head.resource_identity() != &identity
                    || entry.namespace != namespace.public_name()
                    || entry.table != name.as_str()
                {
                    return Err(TableCatalogStoreError::Conflict("maintenance resource binding changed".into()));
                }
                if entry.state == TableCatalogEntryState::Active {
                    return Err(evidence_error("Catalog retirement requires a completed logical deletion"));
                }
            }
        }
        let delete_path = self
            .paths
            .vector_delete_intent_path(identity.storage_bucket_id(), identity.table_id().as_str());
        if let Some(object) = raw_objects.get(&delete_path) {
            if object.data.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
                return Err(maintenance_error("Catalog delete intent exceeds maximum record size"));
            }
            let intent: VectorIndexDeleteIntent =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&format!("decode delete intent: {e}")))?;
            intent.validate()?;
            if intent.bucket.to_handle()? != *bucket
                || intent.table_id != identity.table_id().as_str()
                || intent.namespace != namespace.public_name()
                || intent.name != name.as_str()
            {
                return Err(TableCatalogStoreError::Conflict("maintenance delete intent identity changed".into()));
            }
            if intent.phase == VectorIndexDeletePhase::Prepared {
                return Err(evidence_error("Catalog retirement requires a completed logical deletion"));
            }
            let observed_object = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(delete_path)?, object)?;
            let snapshot = VectorMaintenanceResourceSnapshot {
                resource: identity,
                namespace,
                name,
                state: VectorMaintenanceResourceState::Deleted,
                head: Some(VectorMaintenanceHeadV1::from_catalog_entry(&intent.deleted_entry)?),
                source_intent: Some(observed_object.clone()),
                maintenance: intent.maintenance_binding().cloned(),
                observed_object,
            };
            return Ok((snapshot, intent));
        }
        let create_path = self
            .paths
            .vector_create_intent_path(identity.storage_bucket_id(), &namespace, &name);
        if let Some(object) = raw_objects.get(&create_path) {
            if object.data.len() > VECTOR_MAINTENANCE_RECORD_MAX_BYTES {
                return Err(maintenance_error("Catalog create intent exceeds maximum record size"));
            }
            let intent: VectorIndexCreateIntentV1 =
                serde_json::from_slice(&object.data).map_err(|e| maintenance_error(&format!("decode create intent: {e}")))?;
            intent.validate()?;
            if intent.bucket.to_handle()? != *bucket
                || intent.table_id != identity.table_id().as_str()
                || intent.namespace != namespace.public_name()
                || intent.name != name.as_str()
            {
                return Err(TableCatalogStoreError::Conflict("maintenance create intent identity changed".into()));
            }
            return Err(evidence_error("Catalog retirement requires a completed logical deletion"));
        }
        Err(evidence_error("resource lacks its complete logical deletion record"))
    }

    #[cfg(test)]
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

    async fn classify_catalog_source_with_raw(
        &self,
        bucket: &BucketHandle,
        key: &str,
        object: &TableCatalogObject,
        raw_objects: Option<&BTreeMap<String, TableCatalogObject>>,
        snapshot_cache: Option<&mut ResourceSnapshotCache>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(VectorCatalogRetirementTargetV1, Option<(VectorMaintenanceResourceSnapshot, Source)>)> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        if object.data.len() > VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES {
            return Err(maintenance_error("Catalog source exceeds its original record limit"));
        }
        let reference = exact_source(key, object)?;
        let storage = bucket.storage_bucket_id().as_str();
        let namespace = Namespace::parse("indexes").map_err(|e| maintenance_error(&e.to_string()))?;
        let (kind, resource, details) = if key == self.paths.table_bucket_entry_path(storage) {
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
            (Kind::BucketEntry, None, None)
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
            (Kind::NamespaceEntry, None, None)
        } else {
            if !self.bucket_retirement_source_path_known(bucket, key)? {
                return Err(evidence_error("unrecognized Vector Catalog source path"));
            }
            let source = decode_resource_source(object)?;
            let resource = self.bucket_source_resource_with_raw(bucket, &source, raw_objects).await?;
            let (current, delete_intent) = match (raw_objects, snapshot_cache) {
                (Some(raw_objects), Some(cache)) => {
                    let rkey = resource_key(&resource);
                    if let Some(cached) = cache.get(&rkey) {
                        cached.clone()
                    } else {
                        let resolved = self.deleted_snapshot_from_raw_objects(bucket, &resource, raw_objects)?;
                        cache.insert(rkey, resolved.clone());
                        resolved
                    }
                }
                (Some(raw_objects), None) => self.deleted_snapshot_from_raw_objects(bucket, &resource, raw_objects)?,
                (None, _) => {
                    #[cfg(test)]
                    {
                        let snapshot = self.bucket_deleted_snapshot(&resource).await?;
                        let intent: VectorIndexDeleteIntent =
                            decode_source(&self.maintenance_read_ref(&snapshot.observed_object).await?)?;
                        (snapshot, intent)
                    }
                    #[cfg(not(test))]
                    {
                        return Err(maintenance_error("unindexed Catalog source classification is unavailable"));
                    }
                }
            };
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
                        if entry.as_ref() != &delete_intent.deleted_entry {
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
                                .read_source_object_from_raw_or_backend(
                                    &canonical,
                                    VECTOR_MAINTENANCE_RECORD_MAX_BYTES,
                                    raw_objects,
                                )
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
            (kind, Some(resource), Some((current, source)))
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
        Ok((target, details))
    }

    #[cfg(test)]
    pub(super) async fn classify_bucket_retirement_source(
        &self,
        bucket: &BucketHandle,
        key: &str,
        object: &TableCatalogObject,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetirementTargetV1> {
        Ok(self
            .classify_catalog_source_with_raw(bucket, key, object, None, None, fence)
            .await?
            .0)
    }

    pub(super) async fn index_bucket_catalog_sources(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<IndexedBucketCatalog> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        self.backend.revalidate_bucket_handle(bucket).await?;
        let collected = self.collect_bucket_catalog_sources(bucket, fence).await?;
        let mut raw_objects = BTreeMap::new();
        for (key, object) in collected {
            if object.data.len() > VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES {
                return Err(maintenance_error("Catalog source exceeds its original record limit"));
            }
            raw_objects.insert(key, object);
        }
        let mut targets_in_order = Vec::with_capacity(raw_objects.len());
        let mut classified_by_key = BTreeMap::new();
        let mut by_resource: BTreeMap<(String, String, String), IndexedRetiredResource> = BTreeMap::new();
        let mut snapshot_cache = BTreeMap::new();
        for (key, object) in &raw_objects {
            let (target, details) = self
                .classify_catalog_source_with_raw(bucket, key, object, Some(&raw_objects), Some(&mut snapshot_cache), fence)
                .await?;
            if let (Some(owner), Some((snapshot, source))) = (target.resource.as_ref(), details) {
                let entry = by_resource
                    .entry(resource_key(owner))
                    .or_insert_with(|| IndexedRetiredResource {
                        snapshot,
                        items: Vec::new(),
                        dependencies_by_key: BTreeMap::new(),
                    });
                visit_source_dependencies(&source, |dependency| {
                    if dependency.object_key != *key {
                        entry
                            .dependencies_by_key
                            .entry(dependency.object_key.clone())
                            .or_default()
                            .push(dependency.clone());
                    }
                    Ok(())
                })?;
                entry.items.push((target.clone(), source));
            }
            classified_by_key.insert(key.clone(), target.clone());
            targets_in_order.push(target);
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        fence.ensure_held()?;
        Ok(IndexedBucketCatalog {
            raw_objects,
            targets_in_order,
            classified_by_key,
            by_resource,
        })
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
    #[cfg(test)]
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

    fn bucket_job_result_indexed(
        &self,
        job: &VectorMaintenanceJobV1,
        index: &IndexedBucketCatalog,
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
        let object = index.read_exact_ref(reference, fence)?;
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
            let recorder = index
                .read_job(&self.paths, &identity, result.recorded_by_job_id)?
                .ok_or_else(|| evidence_error("job result recovery recorder is missing"))?;
            let source = recorder
                .input
                .source
                .as_ref()
                .ok_or_else(|| evidence_error("result recorder lacks original job source"))?;
            if recorder.kind != VectorMaintenanceKind::CommitRecovery
                || source.kind != VectorMaintenanceSourceKind::MaintenanceJob
            {
                return Err(evidence_error("result recorder has no job recovery authority"));
            }
            let original = index.read_exact_ref(&source.reference, fence)?;
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
        let mut references = Vec::new();
        visit_job_references(job, &result, |reference| {
            references.push(reference.clone());
            Ok(())
        })?;
        for dependency in &references {
            let _ = index.read_exact_ref(dependency, fence)?;
            let target = index
                .classified_by_key
                .get(&dependency.object_key)
                .ok_or_else(|| evidence_error("job dependency is outside the bucket Catalog"))?;
            if target.resource.as_ref() != Some(&job.resource) {
                return Err(evidence_error("job dependency belongs to another resource"));
            }
        }
        for stage in &result.stages {
            if let Some(reference) = &stage.commit_ref {
                let object = index.read_exact_ref(reference, fence)?;
                let log: CommitLogEntry = decode_source(object)?;
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
        Ok((exact_source(&reference.object_key, object)?, result))
    }

    fn collect_published_retirements_indexed(
        &self,
        resource: &CatalogResourceIdentity,
        binding: &VectorMaintenanceBindingV1,
        index: &IndexedBucketCatalog,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Vec<(Ref, VectorRetirementAuthorizationV1)>> {
        binding.validate()?;
        let prefix = self.paths.vector_maintenance_prefix(resource)?;
        let mut next = binding.retirement_tip.clone();
        let mut seen = BTreeSet::new();
        let mut chain = Vec::with_capacity(usize::try_from(binding.retirement_count).unwrap_or(0).min(1024));
        for _ in 0..binding.retirement_count {
            let reference = next
                .take()
                .ok_or_else(|| maintenance_error("retirement chain ended before its declared count"))?;
            if !seen.insert(reference.object_key.clone()) {
                return Err(maintenance_error("cyclic retirement chain"));
            }
            let object = index.read_exact_ref(&reference, fence)?;
            let authorization = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
            let path = format!(
                "{prefix}authorizations/{}.json",
                table_catalog_path_hash(&authorization.retirement_id.to_string())
            );
            if path != reference.object_key || authorization.resource.identity()? != *resource {
                return Err(TableCatalogStoreError::Conflict("retirement chain identity or path mismatch".into()));
            }
            let previous = authorization.previous_ref.clone();
            chain.push((reference, authorization));
            next = previous;
        }
        if next.is_some() {
            return Err(maintenance_error("retirement chain exceeds its declared count"));
        }
        Ok(chain)
    }

    fn bucket_completed_retirement_indexed(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        reference: &Ref,
        auth: &VectorRetirementAuthorizationV1,
        published_by_id: &BTreeMap<Uuid, VectorRetirementAuthorizationV1>,
        index: &IndexedBucketCatalog,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Ref, VectorRetirementProgressV1)> {
        auth.validate()?;
        match published_by_id.get(&auth.retirement_id) {
            Some(original) if original == auth => {}
            Some(_) => return Err(evidence_error("authorization differs from original published retirement")),
            None => return Err(evidence_error("retirement authorization is unpublished")),
        }
        let plan = index.read_exact_ref(&auth.plan_ref, fence)?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&plan.data)?;
        let evidence = index.read_exact_ref(&auth.reference_evidence_ref, fence)?;
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
        let creator = index
            .read_job(&self.paths, &current.resource, auth.created_by_job_id)?
            .ok_or_else(|| evidence_error("retirement creator job is absent"))?;
        if creator.resource != auth.resource
            || creator.attempt != auth.created_by_attempt
            || creator.execution_identity != auth.execution_identity
            || creator.input.plan_ref.as_ref() != Some(&auth.plan_ref)
            || !matches!(creator.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
        {
            return Err(evidence_error("retirement creator identity or original plan differs"));
        }
        let path = format!(
            "{}progress/{}.json",
            self.paths.vector_maintenance_prefix(&current.resource)?,
            table_catalog_path_hash(&auth.retirement_id.to_string())
        );
        let object = index
            .raw_objects
            .get(&path)
            .ok_or_else(|| evidence_error("retirement progress is missing"))?;
        if object.data.len() > VECTOR_RETIREMENT_PROGRESS_MAX_BYTES {
            return Err(maintenance_error("Catalog retirement progress exceeds byte limit"));
        }
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
            let job = index
                .read_job(&self.paths, &current.resource, dispatch.job_id)?
                .ok_or_else(|| evidence_error("retirement dispatch job is absent"))?;
            if job.resource != auth.resource {
                return Err(evidence_error("retirement dispatch resource differs"));
            }
            let lease = VectorMaintenanceLeaseToken::from_job(&job)?;
            if lease.attempt != dispatch.attempt
                || lease.lease_id != dispatch.lease_id
                || lease.worker_id != dispatch.worker_id
                || lease.worker_boot_id != dispatch.worker_boot_id
                || lease.execution_identity != dispatch.execution_identity
            {
                return Err(evidence_error("original retirement dispatch lease is unavailable"));
            }
            if job.job_id != auth.created_by_job_id {
                let source = job
                    .input
                    .source
                    .as_ref()
                    .ok_or_else(|| evidence_error("retirement recovery lacks authorization source"))?;
                if job.kind != VectorMaintenanceKind::DeleteRecovery
                    || source.kind != VectorMaintenanceSourceKind::RetirementAuthorization
                {
                    return Err(evidence_error("retirement dispatch lacks original recovery authority"));
                }
                let object = index.read_exact_ref(&source.reference, fence)?;
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
        Ok((exact_source(&path, object)?, progress))
    }

    #[cfg(test)]
    async fn bucket_completed_retirement(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        reference: &Ref,
        auth: &VectorRetirementAuthorizationV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Ref, VectorRetirementProgressV1)> {
        let bucket = current
            .resource
            .vector_bucket()
            .ok_or_else(|| maintenance_error("missing Vector bucket identity"))?;
        let index = self.index_bucket_catalog_sources(bucket, fence).await?;
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| evidence_error("resource has no retirement publication"))?;
        let published = self
            .collect_published_retirements_indexed(&current.resource, binding, &index, fence)?
            .into_iter()
            .map(|(_, item)| (item.retirement_id, item))
            .collect();
        self.bucket_completed_retirement_indexed(current, reference, auth, &published, &index, fence)
    }
}

type ValidatedRetirementsMap = BTreeMap<Uuid, (Ref, VectorRetirementAuthorizationV1, Ref, VectorRetirementProgressV1)>;

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    fn bucket_verify_deleted_data_indexed(
        &self,
        resource_index: &IndexedRetiredResource,
        index: &IndexedBucketCatalog,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<ValidatedRetirementsMap> {
        let current = &resource_index.snapshot;
        fence.ensure_held()?;
        if current.state != VectorMaintenanceResourceState::Deleted {
            return Err(TableCatalogStoreError::Conflict(
                "retirement evidence reuse requires a deleted resource".into(),
            ));
        }
        let binding = current
            .maintenance
            .as_ref()
            .ok_or_else(|| evidence_error("Deleted resource lacks retirement binding"))?;
        let published_chain = self.collect_published_retirements_indexed(&current.resource, binding, index, fence)?;
        let selected = published_chain
            .iter()
            .find(|(_, auth)| auth.delete_intent_ref.is_some() && current.head.as_ref() == Some(&auth.base_head))
            .map(|(_, auth)| auth.clone())
            .ok_or_else(|| evidence_error("Deleted resource lacks original complete retirement evidence"))?;
        let plan_object = index.read_exact_ref(&selected.plan_ref, fence)?;
        let plan = decode_vector_maintenance_record::<VectorRetirementPlanV1>(&plan_object.data)?;
        let evidence_object = index.read_exact_ref(&selected.reference_evidence_ref, fence)?;
        let evidence = decode_vector_maintenance_record::<VectorRetirementEvidenceV1>(&evidence_object.data)?;
        if plan.resource != selected.resource
            || plan.targets != selected.targets
            || plan.evidence_ref != selected.reference_evidence_ref
            || evidence.resource != selected.resource
            || evidence.base_head != selected.base_head
            || evidence.delete_intent_ref != selected.delete_intent_ref
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
        let copy = index.read_exact_ref(copy_ref, fence)?;
        let previous: VectorIndexDeleteIntent =
            serde_json::from_slice(&copy.data).map_err(|error| maintenance_error(&error.to_string()))?;
        let present_ref: Ref = (&current.observed_object).into();
        let present_object = index.read_exact_ref(&present_ref, fence)?;
        let present: VectorIndexDeleteIntent =
            serde_json::from_slice(&present_object.data).map_err(|error| maintenance_error(&error.to_string()))?;
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
        let logs: Vec<CommitLogEntry> = resource_index
            .items
            .iter()
            .filter_map(|(target, source)| match (target.kind, source) {
                (Kind::CommitLog, Source::Commit(log)) => Some((**log).clone()),
                _ => None,
            })
            .collect();
        if logs.len() > VECTOR_RETIREMENT_MAX_REFERENCES {
            return Err(TableCatalogStoreError::Unavailable(
                "CapacityExceeded: maintenance history entries".into(),
            ));
        }
        object::verify_vector_deleted_history_with_logs(&present, None, &logs)?;
        let history = vector_committed_history_versions(&present.deleted_entry, &logs)?;
        let mut covered_by_key: BTreeMap<&str, Vec<&Ref>> = BTreeMap::new();
        for r in evidence
            .history_refs
            .iter()
            .chain(&evidence.protected_refs)
            .chain(&evidence.unresolved_refs)
        {
            covered_by_key.entry(r.object_key.as_str()).or_default().push(r);
        }
        for version in &history {
            let expected = CatalogMetadataRefRecordV1::from(version.metadata());
            if !covered_by_key
                .get(expected.object_key.as_str())
                .is_some_and(|refs| refs.iter().any(|&r| *r == expected))
            {
                return Err(TableCatalogStoreError::Conflict("retirement proof omits a committed manifest".into()));
            }
        }
        let mut unresolved_by_key: BTreeMap<&str, Vec<&Ref>> = BTreeMap::new();
        for r in &evidence.unresolved_refs {
            unresolved_by_key.entry(r.object_key.as_str()).or_default().push(r);
        }
        let history_index = TableCommitHistoryIndex::new(&present.deleted_entry, logs.iter());
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
                if !unresolved_by_key
                    .get(binding.candidate_metadata.object_key.as_str())
                    .is_some_and(|refs| refs.iter().any(|&r| *r == binding.candidate_metadata))
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "retirement proof needs unresolved commit recovery".into(),
                    ));
                }
            }
        }
        if !evidence.unresolved_refs.is_empty() || !evidence.protected_refs.is_empty() {
            return Err(evidence_error("Deleted resource still has unresolved or protected data references"));
        }
        let published_by_id: BTreeMap<Uuid, VectorRetirementAuthorizationV1> = published_chain
            .iter()
            .map(|(_, auth)| (auth.retirement_id, auth.clone()))
            .collect();
        let mut completed_by_id = BTreeMap::new();
        for (auth_ref, auth) in published_chain {
            let (progress_ref, progress) =
                self.bucket_completed_retirement_indexed(current, &auth_ref, &auth, &published_by_id, index, fence)?;
            completed_by_id.insert(auth.retirement_id, (auth_ref, auth, progress_ref, progress));
        }
        for history in &evidence.history_refs {
            let confirmed = completed_by_id.values().any(|(_, auth, _, progress)| {
                auth.targets.iter().zip(&progress.targets).any(|(target, completed)| {
                    target.object_key == history.object_key
                        && Some(target.body_revision) == history.body_revision
                        && target.content_length == history.content_length
                        && history.etag.as_ref().is_none_or(|etag| target.etag.as_ref() == Some(etag))
                        && completed.receipt.is_some()
                })
            });
            if !confirmed {
                return Err(evidence_error("committed data reference lacks exact original retirement confirmation"));
            }
        }
        fence.ensure_held()?;
        Ok(completed_by_id)
    }

    #[cfg(test)]
    async fn bucket_verify_deleted_data(
        &self,
        current: &VectorMaintenanceResourceSnapshot,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        if current.maintenance.is_none() {
            return Err(evidence_error("Deleted resource lacks retirement binding"));
        }
        let bucket = current
            .resource
            .vector_bucket()
            .ok_or_else(|| maintenance_error("missing Vector bucket identity"))?;
        let index = self.index_bucket_catalog_sources(bucket, fence).await?;
        let resource = VectorMaintenanceResourceV1::try_new(&current.resource, &current.namespace, &current.name)?;
        let resource_index = index
            .by_resource
            .get(&resource_key(&resource))
            .ok_or_else(|| evidence_error("resource lacks its complete logical deletion record"))?;
        self.bucket_verify_deleted_data_indexed(resource_index, &index, fence)
            .map(|_| ())
    }

    fn collect_resource_facts_indexed(
        &self,
        resource_index: &IndexedRetiredResource,
        completed_retirements: &ValidatedRetirementsMap,
        published_by_id: &BTreeMap<Uuid, VectorRetirementAuthorizationV1>,
        index: &IndexedBucketCatalog,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Vec<Fact>> {
        let current = &resource_index.snapshot;
        let head = current
            .head
            .clone()
            .ok_or_else(|| evidence_error("Deleted resource has no retained head"))?;
        let mut facts = Vec::with_capacity(resource_index.items.len().saturating_mul(2).saturating_add(1));
        facts.push(Fact::ResourceAnchor {
            delete_intent: (&current.observed_object).into(),
            deleted_head: Box::new(head),
        });
        for (target, source) in &resource_index.items {
            fence.ensure_held()?;
            let reference = catalog_retirement_target_reference(target);
            facts.push(Fact::SourceRecord {
                reference: reference.clone(),
            });
            match (target.kind, source) {
                (Kind::MaintenanceSource, _) => {
                    let has_owner = resource_index
                        .dependencies_by_key
                        .get(&reference.object_key)
                        .is_some_and(|deps| deps.iter().any(|dep| covers_reference(&reference, dep)));
                    if !has_owner {
                        return Err(evidence_error("immutable source copy has no exact retained owner"));
                    }
                }
                (Kind::CommitLog, Source::Commit(log)) => {
                    let binding = log
                        .vector_commit_binding
                        .as_ref()
                        .ok_or_else(|| maintenance_error("commit lacks exact Vector binding"))?;
                    facts.push(Fact::Commit {
                        commit_id: log.commit_id.clone(),
                        request_sha256: binding.request_digest_sha256.clone(),
                        log_ref: reference,
                        committed_head: Box::new(committed_head(log)?),
                        history_ref_count: 1 + u64::from(binding.expected_metadata.is_some()),
                    });
                    facts.push(Fact::CommitHistoryReference {
                        commit_id: log.commit_id.clone(),
                        ordinal: 0,
                        reference: binding.candidate_metadata.clone(),
                    });
                    if let Some(expected) = &binding.expected_metadata {
                        facts.push(Fact::CommitHistoryReference {
                            commit_id: log.commit_id.clone(),
                            ordinal: 1,
                            reference: expected.clone(),
                        });
                    }
                }
                (Kind::MaintenanceJob, Source::Job(job)) => {
                    if job.state == VectorMaintenanceJobState::Queued && job.attempt == 0 {
                        if job.lease.is_some() || job.result_ref.is_some() || job.failure.is_some() {
                            return Err(evidence_error("unstarted job contains execution evidence"));
                        }
                        facts.push(Fact::UnstartedJob { reference });
                    } else {
                        let (result_ref, result) = self.bucket_job_result_indexed(job, index, fence)?;
                        let mut count = 0_u64;
                        visit_job_references(job, &result, |_| {
                            count = count
                                .checked_add(1)
                                .ok_or_else(|| maintenance_error("proof count overflow"))?;
                            Ok(())
                        })?;
                        facts.push(Fact::Job {
                            job_ref: reference,
                            job_id: job.job_id,
                            revision: job.revision,
                            attempt: job.attempt,
                            lease_id: job.lease.as_ref().map(|lease| lease.lease_id),
                            result_ref,
                            source_ref_count: count,
                        });
                        let mut ordinal = 0;
                        visit_job_references(job, &result, |reference| {
                            facts.push(Fact::JobSourceReference {
                                job_id: job.job_id,
                                revision: job.revision,
                                ordinal,
                                reference: reference.clone(),
                            });
                            ordinal += 1;
                            Ok(())
                        })?;
                    }
                }
                (Kind::RetirementAuthorization, Source::Authorization(auth)) => {
                    let (progress_ref, progress) = match completed_retirements.get(&auth.retirement_id) {
                        Some((expected_auth_ref, expected_auth, progress_ref, progress))
                            if expected_auth == auth.as_ref() && *expected_auth_ref == reference =>
                        {
                            (progress_ref.clone(), progress.clone())
                        }
                        _ => {
                            self.bucket_completed_retirement_indexed(current, &reference, auth, published_by_id, index, fence)?
                        }
                    };
                    facts.push(Fact::Retirement {
                        authorization_ref: reference,
                        progress_ref,
                        retirement_id: auth.retirement_id,
                        revision: progress.revision,
                        receipt_count: u64::try_from(progress.targets.len())
                            .map_err(|_| maintenance_error("receipt count overflow"))?,
                    });
                    for target in progress.targets {
                        facts.push(Fact::RetirementReceipt {
                            retirement_id: auth.retirement_id,
                            revision: progress.revision,
                            ordinal: u64::from(target.ordinal),
                            receipt: target
                                .receipt
                                .ok_or_else(|| evidence_error("completed target receipt missing"))?,
                        });
                    }
                }
                (Kind::RetirementEvidence | Kind::RetirementPlan, _) => {
                    let referenced = completed_retirements.values().any(|(_, auth, _, _)| {
                        covers_reference(&reference, &auth.plan_ref) || covers_reference(&reference, &auth.reference_evidence_ref)
                    });
                    if !referenced {
                        return Err(evidence_error("retirement plan or evidence has no published owner"));
                    }
                }
                (Kind::MaintenanceResult, Source::Result(result)) => {
                    let job = index
                        .read_job(&self.paths, &current.resource, result.job_id)?
                        .ok_or_else(|| evidence_error("result owner job is missing"))?;
                    if job
                        .result_ref
                        .as_ref()
                        .is_none_or(|expected| !covers_reference(&reference, expected))
                    {
                        return Err(evidence_error("result is not bound to the retained completed job"));
                    }
                    self.bucket_job_result_indexed(&job, index, fence)?;
                }
                (Kind::RetirementProgress, Source::Progress(progress)) => {
                    let object = index.read_exact_ref(&progress.authorization_ref, fence)?;
                    let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&object.data)?;
                    let observed = match completed_retirements.get(&auth.retirement_id) {
                        Some((expected_auth_ref, expected_auth, progress_ref, _))
                            if expected_auth == &auth && *expected_auth_ref == progress.authorization_ref =>
                        {
                            progress_ref.clone()
                        }
                        _ => {
                            self.bucket_completed_retirement_indexed(
                                current,
                                &progress.authorization_ref,
                                &auth,
                                published_by_id,
                                index,
                                fence,
                            )?
                            .0
                        }
                    };
                    if observed != reference {
                        return Err(evidence_error("progress differs from original exact retirement progress"));
                    }
                }
                _ => {}
            }
        }
        fence.ensure_held()?;
        Ok(facts)
    }

    pub(super) async fn prepare_bucket_retirement_resource_proof_indexed(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        retirement_id: Uuid,
        index: &IndexedBucketCatalog,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetiredResourceProofV1> {
        self.require_bucket_retirement_identity(bucket)?;
        resource.validate()?;
        if resource.bucket.to_handle()? != *bucket || retirement_id.is_nil() {
            return Err(maintenance_error("proof complete bucket identity mismatch"));
        }
        let resource_index = index
            .by_resource
            .get(&resource_key(resource))
            .ok_or_else(|| evidence_error("resource lacks its complete logical deletion record"))?;
        let completed_retirements = self.bucket_verify_deleted_data_indexed(resource_index, index, fence)?;
        let published_by_id: BTreeMap<Uuid, VectorRetirementAuthorizationV1> = completed_retirements
            .iter()
            .map(|(id, (_, auth, _, _))| (*id, auth.clone()))
            .collect();
        let mut facts =
            self.collect_resource_facts_indexed(resource_index, &completed_retirements, &published_by_id, index, fence)?;
        let mut lengths = Vec::with_capacity(facts.len());
        for fact in &facts {
            fact.validate()?;
            let length = fact_bytes(fact)?;
            if length > PROOF_PAGE_BODY_BUDGET {
                return Err(maintenance_error("single proof fact exceeds its bounded page budget"));
            }
            lengths.push((fact.order(), length));
        }
        let mut indices: Vec<usize> = (0..facts.len()).collect();
        indices.sort_by(|&left, &right| lengths[left].0.cmp(&lengths[right].0));
        for pair in indices.windows(2) {
            if lengths[pair[0]].0 == lengths[pair[1]].0 {
                return Err(evidence_error("duplicate proof fact identity"));
            }
        }
        let mut sorted_facts = Vec::with_capacity(facts.len());
        let mut sorted_lengths = Vec::with_capacity(facts.len());
        let mut raw_slots: Vec<Option<Fact>> = facts.drain(..).map(Some).collect();
        for idx in indices {
            let fact = raw_slots[idx]
                .take()
                .ok_or_else(|| maintenance_error("proof fact permutation failed"))?;
            sorted_facts.push(fact);
            sorted_lengths.push(lengths[idx].1);
        }
        let count = u64::try_from(sorted_facts.len()).map_err(|_| maintenance_error("proof count overflow"))?;
        let proof_id = Uuid::new_v4();
        let binding = VectorBucketBindingV1::from_handle(bucket)?;
        let mut digest = CatalogRetirementProofDigest::new(&binding, resource, retirement_id, proof_id, count)?;
        for fact in &sorted_facts {
            digest.push(fact)?;
        }
        let proof_sha256 = digest.finish()?;
        let page_chunks = paginate_sorted_proof_facts(sorted_facts, &sorted_lengths)?;
        let pages = u32::try_from(page_chunks.len()).map_err(|_| maintenance_error("proof page count overflow"))?;
        let mut next_page = None;
        for (ordinal, (first_fact_ordinal, page_facts)) in page_chunks.into_iter().enumerate().rev() {
            let page_id = Uuid::new_v4();
            let page_ordinal = u32::try_from(ordinal).map_err(|_| maintenance_error("proof page ordinal overflow"))?;
            let page = VectorCatalogRetiredResourceProofPageV1 {
                version: 1,
                bucket: binding.clone(),
                retirement_id,
                resource: resource.clone(),
                proof_id,
                page_id,
                page_ordinal,
                page_count: pages,
                first_fact_ordinal,
                total_fact_count: count,
                proof_sha256: proof_sha256.clone(),
                next_page: next_page.take(),
                facts: page_facts,
            };
            let path = self
                .paths
                .vector_bucket_retirement_proof_path(bucket, retirement_id, proof_id, page_id)?;
            next_page = Some((&self.write_bucket_retirement_record(&path, None, &page, fence).await?).into());
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

    #[cfg(test)]
    pub(super) async fn prepare_bucket_retirement_resource_proof(
        &self,
        bucket: &BucketHandle,
        resource: &VectorMaintenanceResourceV1,
        retirement_id: Uuid,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetiredResourceProofV1> {
        let index = self.index_bucket_catalog_sources(bucket, fence).await?;
        self.prepare_bucket_retirement_resource_proof_indexed(bucket, resource, retirement_id, &index, fence)
            .await
    }
}

fn paginate_sorted_proof_facts(facts: Vec<Fact>, lengths: &[usize]) -> TableCatalogStoreResult<Vec<(u64, Vec<Fact>)>> {
    let mut pages = Vec::new();
    let mut current_facts = Vec::new();
    let mut current_bytes = 0_usize;
    let mut current_first_ordinal = 0_u64;
    let mut ordinal = 0_u64;
    for (fact, &length) in facts.into_iter().zip(lengths) {
        if !current_facts.is_empty()
            && (current_facts.len() == PROOF_FACTS_PER_PAGE || current_bytes.saturating_add(length) > PROOF_PAGE_BODY_BUDGET)
        {
            pages.push((current_first_ordinal, std::mem::take(&mut current_facts)));
            current_bytes = 0;
            current_first_ordinal = ordinal;
        }
        current_bytes = current_bytes
            .checked_add(length)
            .ok_or_else(|| maintenance_error("proof page byte count overflow"))?;
        current_facts.push(fact);
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| maintenance_error("proof ordinal overflow"))?;
    }
    if !current_facts.is_empty() {
        pages.push((current_first_ordinal, current_facts));
    }
    Ok(pages)
}

#[cfg(test)]
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

fn require_proof_source_in_memory(
    descriptor: &VectorCatalogRetiredResourceProofV1,
    expected: &Ref,
    kind: Option<Kind>,
    targets_by_key: &BTreeMap<String, (usize, CatalogRetirementPlannedTarget)>,
    source_facts_by_key: &BTreeMap<&str, (&Ref, u64)>,
) -> TableCatalogStoreResult<()> {
    let (_, planned) = targets_by_key
        .get(&expected.object_key)
        .ok_or_else(|| evidence_error("proof dependency has no exact planned Catalog target"))?;
    let planned_ref = catalog_retirement_target_reference(&planned.target);
    if planned.target.resource.as_ref() != Some(&descriptor.resource)
        || kind.is_some_and(|kind| kind != planned.target.kind)
        || if kind.is_some() {
            planned_ref != *expected
        } else {
            !covers_reference(&planned_ref, expected)
        }
    {
        return Err(evidence_error("proof dependency differs from its planned identity or owner"));
    }
    match source_facts_by_key.get(planned_ref.object_key.as_str()) {
        Some((observed, count)) if *count == 1 && **observed == planned_ref => Ok(()),
        Some(_) => Err(maintenance_error("Catalog proof source identity disagrees with its edge")),
        None => Err(maintenance_error("Catalog proof edge has no exact source record")),
    }
}

type RefIdentity<'a> = (&'a str, &'a str, u64, Option<Uuid>, Option<&'a str>);

fn ref_identity(r: &Ref) -> RefIdentity<'_> {
    (
        r.object_key.as_str(),
        r.content_sha256.as_str(),
        r.content_length,
        r.body_revision,
        r.etag.as_deref(),
    )
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) fn validate_resource_facts_closure_in_memory(
        &self,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        facts: &[Fact],
        targets_by_key: &BTreeMap<String, (usize, CatalogRetirementPlannedTarget)>,
    ) -> TableCatalogStoreResult<BTreeMap<String, Ref>> {
        let mut source_facts_by_key: BTreeMap<&str, (&Ref, u64)> = BTreeMap::new();
        let mut anchors_by_delete_ref: BTreeMap<RefIdentity<'_>, u64> = BTreeMap::new();
        let mut commits_by_log_ref: BTreeMap<RefIdentity<'_>, u64> = BTreeMap::new();
        let mut commits_by_id: BTreeMap<&str, Vec<(&VectorMaintenanceHeadV1, u64)>> = BTreeMap::new();
        let mut commits_by_generation: BTreeMap<u64, Vec<&VectorMaintenanceHeadV1>> = BTreeMap::new();
        let mut commit_history_by_id: BTreeMap<&str, Vec<(u64, &Ref)>> = BTreeMap::new();
        let mut jobs_or_unstarted_by_ref: BTreeMap<RefIdentity<'_>, u64> = BTreeMap::new();
        let mut jobs_by_id_rev: BTreeMap<(Uuid, Uuid), Vec<u64>> = BTreeMap::new();
        let mut job_edges_by_id_rev: BTreeMap<(Uuid, Uuid), Vec<(u64, &Ref)>> = BTreeMap::new();
        let mut retirements_by_auth_ref: BTreeMap<RefIdentity<'_>, u64> = BTreeMap::new();
        let mut retirements_by_progress_ref: BTreeMap<RefIdentity<'_>, u64> = BTreeMap::new();
        let mut retirements_by_id_rev: BTreeMap<(Uuid, Uuid), Vec<u64>> = BTreeMap::new();
        let mut receipts_by_id_rev: BTreeMap<(Uuid, Uuid), Vec<u64>> = BTreeMap::new();

        let add_count = |slot: &mut u64| -> TableCatalogStoreResult<()> {
            *slot = slot.checked_add(1).ok_or_else(|| maintenance_error("proof count overflow"))?;
            Ok(())
        };

        for fact in facts {
            match fact {
                Fact::ResourceAnchor { delete_intent, .. } => {
                    add_count(anchors_by_delete_ref.entry(ref_identity(delete_intent)).or_default())?;
                }
                Fact::SourceRecord { reference } => {
                    let entry = source_facts_by_key
                        .entry(reference.object_key.as_str())
                        .or_insert((reference, 0));
                    if entry.0 != reference {
                        return Err(maintenance_error("Catalog proof source identity disagrees with its edge"));
                    }
                    add_count(&mut entry.1)?;
                }
                Fact::Commit {
                    commit_id,
                    log_ref,
                    committed_head,
                    history_ref_count,
                    ..
                } => {
                    add_count(commits_by_log_ref.entry(ref_identity(log_ref)).or_default())?;
                    commits_by_id
                        .entry(commit_id.as_str())
                        .or_default()
                        .push((committed_head.as_ref(), *history_ref_count));
                    commits_by_generation
                        .entry(committed_head.generation)
                        .or_default()
                        .push(committed_head.as_ref());
                }
                Fact::CommitHistoryReference {
                    commit_id,
                    ordinal,
                    reference,
                } => {
                    commit_history_by_id
                        .entry(commit_id.as_str())
                        .or_default()
                        .push((*ordinal, reference));
                }
                Fact::Job {
                    job_ref,
                    job_id,
                    revision,
                    source_ref_count,
                    ..
                } => {
                    add_count(jobs_or_unstarted_by_ref.entry(ref_identity(job_ref)).or_default())?;
                    jobs_by_id_rev
                        .entry((*job_id, *revision))
                        .or_default()
                        .push(*source_ref_count);
                }
                Fact::JobSourceReference {
                    job_id,
                    revision,
                    ordinal,
                    reference,
                } => {
                    job_edges_by_id_rev
                        .entry((*job_id, *revision))
                        .or_default()
                        .push((*ordinal, reference));
                }
                Fact::Retirement {
                    authorization_ref,
                    progress_ref,
                    retirement_id,
                    revision,
                    receipt_count,
                } => {
                    add_count(retirements_by_auth_ref.entry(ref_identity(authorization_ref)).or_default())?;
                    add_count(retirements_by_progress_ref.entry(ref_identity(progress_ref)).or_default())?;
                    retirements_by_id_rev
                        .entry((*retirement_id, *revision))
                        .or_default()
                        .push(*receipt_count);
                }
                Fact::RetirementReceipt {
                    retirement_id,
                    revision,
                    ordinal,
                    ..
                } => {
                    receipts_by_id_rev
                        .entry((*retirement_id, *revision))
                        .or_default()
                        .push(*ordinal);
                }
                Fact::UnstartedJob { reference } => {
                    add_count(jobs_or_unstarted_by_ref.entry(ref_identity(reference)).or_default())?;
                }
            }
        }

        let identity = descriptor.resource.identity()?;
        let prefix = self.paths.vector_maintenance_prefix(&identity)?;
        let dataset_prefix = identity.dataset_prefix();

        for fact in facts {
            match fact {
                Fact::ResourceAnchor {
                    delete_intent,
                    deleted_head,
                } => {
                    require_proof_source_in_memory(
                        descriptor,
                        delete_intent,
                        Some(Kind::DeleteIntent),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    if commits_by_generation
                        .last_key_value()
                        .is_some_and(|(&generation, _)| generation > deleted_head.generation)
                    {
                        return Err(evidence_error("commit follows retained Deleted head"));
                    }
                    let mut final_head = 0_u64;
                    if let Some(heads) = commits_by_generation.get(&deleted_head.generation) {
                        for head in heads {
                            if *head != deleted_head.as_ref() {
                                return Err(evidence_error("last committed head differs from Deleted anchor"));
                            }
                            add_count(&mut final_head)?;
                        }
                    }
                    if final_head != 1 {
                        return Err(evidence_error("Deleted anchor lacks one exact final committed head"));
                    }
                }
                Fact::SourceRecord { reference } => {
                    require_proof_source_in_memory(descriptor, reference, None, targets_by_key, &source_facts_by_key)?;
                    let (_, planned) = targets_by_key
                        .get(&reference.object_key)
                        .ok_or_else(|| evidence_error("source target disappeared from plan"))?;
                    if catalog_retirement_target_reference(&planned.target) != *reference {
                        return Err(evidence_error("source proof omits an exact original identity field"));
                    }
                    let ref_key = ref_identity(reference);
                    let owners = match planned.target.kind {
                        Kind::DeleteIntent => Some(anchors_by_delete_ref.get(&ref_key).copied().unwrap_or(0)),
                        Kind::CommitLog => Some(commits_by_log_ref.get(&ref_key).copied().unwrap_or(0)),
                        Kind::MaintenanceJob => Some(jobs_or_unstarted_by_ref.get(&ref_key).copied().unwrap_or(0)),
                        Kind::RetirementAuthorization => Some(retirements_by_auth_ref.get(&ref_key).copied().unwrap_or(0)),
                        Kind::RetirementProgress => Some(retirements_by_progress_ref.get(&ref_key).copied().unwrap_or(0)),
                        _ => None,
                    };
                    if owners.is_some_and(|owners| owners != 1) {
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
                    require_proof_source_in_memory(
                        descriptor,
                        log_ref,
                        Some(Kind::CommitLog),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    if log_ref.object_key
                        != self
                            .paths
                            .commit_log_entry_path(identity.storage_bucket_id(), identity.table_id().as_str(), commit_id)
                    {
                        return Err(evidence_error("commit header identity differs from its exact original path"));
                    }
                    let mut count = 0_u64;
                    if let Some(edges) = commit_history_by_id.get(commit_id.as_str()) {
                        for (ordinal, reference) in edges {
                            if *ordinal != count {
                                return Err(evidence_error("commit history edge ordinal is missing or repeated"));
                            }
                            if *ordinal == 0 && **reference != committed_head.binding.metadata {
                                return Err(evidence_error("commit candidate edge differs from committed head"));
                            }
                            add_count(&mut count)?;
                        }
                    }
                    let same_generation =
                        u64::try_from(commits_by_generation.get(&committed_head.generation).map_or(0, Vec::len))
                            .map_err(|_| maintenance_error("proof count overflow"))?;
                    let predecessor = u64::try_from(
                        committed_head
                            .generation
                            .checked_sub(1)
                            .and_then(|prev| commits_by_generation.get(&prev))
                            .map_or(0, Vec::len),
                    )
                    .map_err(|_| maintenance_error("proof count overflow"))?;
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
                    let owners = commits_by_id
                        .get(commit_id.as_str())
                        .ok_or_else(|| evidence_error("orphan commit history edge"))?;
                    let mut owner = None;
                    for (committed_head, history_ref_count) in owners {
                        if owner.is_some() || *ordinal >= *history_ref_count {
                            return Err(evidence_error("history edge owner is ambiguous or count differs"));
                        }
                        owner = Some(*committed_head);
                    }
                    let owner = owner.ok_or_else(|| evidence_error("orphan commit history edge"))?;
                    if *ordinal == 1 && owner.generation > 2 {
                        let mut matched = 0_u64;
                        if let Some(predecessors) = owner.generation.checked_sub(1).and_then(|g| commits_by_generation.get(&g)) {
                            for pred in predecessors {
                                if pred.binding.metadata != *reference {
                                    return Err(evidence_error(
                                        "history predecessor reference differs from prior committed head",
                                    ));
                                }
                                add_count(&mut matched)?;
                            }
                        }
                        if matched != 1 {
                            return Err(evidence_error("history edge has no exact predecessor"));
                        }
                    }
                    if !reference.object_key.starts_with(&dataset_prefix) {
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
                    require_proof_source_in_memory(
                        descriptor,
                        job_ref,
                        Some(Kind::MaintenanceJob),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    if job_ref.object_key != self.paths.vector_maintenance_job_path(&identity, *job_id)? {
                        return Err(evidence_error("job header identity differs from its exact original path"));
                    }
                    require_proof_source_in_memory(
                        descriptor,
                        result_ref,
                        Some(Kind::MaintenanceResult),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    let mut count = 0_u64;
                    let mut result_edge = false;
                    if let Some(edges) = job_edges_by_id_rev.get(&(*job_id, *revision)) {
                        for (ordinal, reference) in edges {
                            if *ordinal != count {
                                return Err(evidence_error("job source edge ordinal is missing or repeated"));
                            }
                            if covers_reference(result_ref, reference) {
                                result_edge = true;
                            }
                            add_count(&mut count)?;
                        }
                    }
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
                    require_proof_source_in_memory(descriptor, reference, None, targets_by_key, &source_facts_by_key)?;
                    let mut owners = 0_u64;
                    if let Some(declared_counts) = jobs_by_id_rev.get(&(*job_id, *revision)) {
                        for source_ref_count in declared_counts {
                            if *ordinal >= *source_ref_count {
                                return Err(evidence_error("job edge exceeds owner count"));
                            }
                            add_count(&mut owners)?;
                        }
                    }
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
                    require_proof_source_in_memory(
                        descriptor,
                        authorization_ref,
                        Some(Kind::RetirementAuthorization),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    require_proof_source_in_memory(
                        descriptor,
                        progress_ref,
                        Some(Kind::RetirementProgress),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                    let owner_key = table_catalog_path_hash(&retirement_id.to_string());
                    if authorization_ref.object_key != format!("{prefix}authorizations/{owner_key}.json")
                        || progress_ref.object_key != format!("{prefix}progress/{owner_key}.json")
                    {
                        return Err(evidence_error("retirement header identity differs from its exact original paths"));
                    }
                    let mut count = 0_u64;
                    if let Some(ordinals) = receipts_by_id_rev.get(&(*retirement_id, *revision)) {
                        for ordinal in ordinals {
                            if *ordinal != count {
                                return Err(evidence_error("retirement receipt ordinal is missing or repeated"));
                            }
                            add_count(&mut count)?;
                        }
                    }
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
                    if let Some(declared_counts) = retirements_by_id_rev.get(&(*retirement_id, *revision)) {
                        for receipt_count in declared_counts {
                            if *ordinal >= *receipt_count {
                                return Err(evidence_error("receipt exceeds original target count"));
                            }
                            add_count(&mut owners)?;
                        }
                    }
                    if owners != 1 {
                        return Err(evidence_error("retirement receipt lacks one exact owner"));
                    }
                }
                Fact::UnstartedJob { reference } => {
                    require_proof_source_in_memory(
                        descriptor,
                        reference,
                        Some(Kind::MaintenanceJob),
                        targets_by_key,
                        &source_facts_by_key,
                    )?;
                }
            }
        }

        let mut verified_sources = BTreeMap::new();
        for (key, (reference, count)) in source_facts_by_key {
            if count != 1 {
                return Err(maintenance_error("Catalog proof source identity disagrees with its edge"));
            }
            verified_sources.insert(key.to_owned(), reference.clone());
        }
        Ok(verified_sources)
    }

    #[cfg(test)]
    pub(super) async fn validate_bucket_retirement_resource_closure(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let loaded = self.load_bucket_retirement_plan(authority, fence).await?;
        let facts = self
            .collect_bucket_retirement_proof_facts(authority, descriptor, fence)
            .await?;
        self.validate_resource_facts_closure_in_memory(descriptor, &facts, &loaded.targets_by_key)?;
        fence.ensure_held()
    }
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
pub(super) mod tests;
