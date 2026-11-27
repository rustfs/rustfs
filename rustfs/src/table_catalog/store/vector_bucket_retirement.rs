// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures_util::future::BoxFuture;

/// Identity of the real owned cleanup executor; its boot is scoped to this execution.
pub(crate) struct CatalogCleanupExecution {
    worker_instance_id: Uuid,
    worker_boot_id: Uuid,
}
impl CatalogCleanupExecution {
    pub(crate) fn new(worker_instance_id: Uuid) -> TableCatalogStoreResult<Self> {
        if worker_instance_id.is_nil() {
            return Err(maintenance_error("Catalog cleanup requires the captured storage instance"));
        }
        Ok(Self {
            worker_instance_id,
            worker_boot_id: Uuid::new_v4(),
        })
    }
}

impl TableCatalogObjectPaths {
    fn vector_bucket_retirement_root(&self, bucket: &BucketHandle) -> TableCatalogStoreResult<String> {
        if self.scope != CatalogScope::Vector || bucket.kind() != BucketKind::Vector {
            return Err(maintenance_error("Catalog bucket retirement requires Vector scope"));
        }
        Ok(format!(
            "s3vectors/catalog-retirements/v1/{}/{}/",
            table_catalog_path_hash(bucket.storage_bucket_id().as_str()),
            bucket.incarnation().as_uuid()
        ))
    }
    pub(in crate::table_catalog::store) fn vector_bucket_retirement_authority_path(
        &self,
        bucket: &BucketHandle,
    ) -> TableCatalogStoreResult<String> {
        Ok(format!("{}authority.json", self.vector_bucket_retirement_root(bucket)?))
    }
    fn vector_bucket_retirement_page_path(
        &self,
        bucket: &BucketHandle,
        retirement_id: Uuid,
        family: &str,
        page_id: Uuid,
    ) -> TableCatalogStoreResult<String> {
        if retirement_id.is_nil() || page_id.is_nil() || !matches!(family, "plan" | "progress") {
            return Err(maintenance_error("invalid Catalog retirement page identity"));
        }
        Ok(format!(
            "{}{retirement_id}/{family}/{page_id}.json",
            self.vector_bucket_retirement_root(bucket)?
        ))
    }
    fn vector_bucket_retirement_proof_path(
        &self,
        bucket: &BucketHandle,
        retirement_id: Uuid,
        proof_id: Uuid,
        page_id: Uuid,
    ) -> TableCatalogStoreResult<String> {
        if retirement_id.is_nil() || proof_id.is_nil() || page_id.is_nil() {
            return Err(maintenance_error("invalid Catalog proof page identity"));
        }
        Ok(format!(
            "{}{retirement_id}/proof/{proof_id}/{page_id}.json",
            self.vector_bucket_retirement_root(bucket)?
        ))
    }

    fn is_uncommitted_retirement_page_subpath(&self, root: &str, key: &str) -> bool {
        fn canonical_nonnil_uuid(segment: &str) -> bool {
            Uuid::try_parse(segment).is_ok_and(|id| !id.is_nil() && id.hyphenated().to_string() == segment)
        }
        let Some(tail) = key.strip_prefix(root) else {
            return false;
        };
        let mut parts = tail.split('/');
        let Some(retirement_id) = parts.next().filter(|id| canonical_nonnil_uuid(id)) else {
            return false;
        };
        let _ = retirement_id;
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("plan" | "progress"), Some(file), None, None) => file.strip_suffix(".json").is_some_and(canonical_nonnil_uuid),
            (Some("proof"), Some(proof_id), Some(file), None) => {
                canonical_nonnil_uuid(proof_id) && file.strip_suffix(".json").is_some_and(canonical_nonnil_uuid)
            }
            _ => false,
        }
    }
}

struct CatalogBucketRetirementAuthority {
    record: VectorBucketCatalogRetirementV1,
    reference: CatalogMetadataRef,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Reuse the caller's lifecycle protection before admitting ordinary work.
    pub(in crate::table_catalog::store) fn require_vector_bucket_catalog_admission<'a>(
        &'a self,
        bucket: &'a BucketHandle,
        lifecycle: &'a TableCatalogLockGuard,
    ) -> BoxFuture<'a, TableCatalogStoreResult<()>> {
        // The returned future borrows the guard's lifetime without retaining a
        // reference to its non-Sync backend owner across await.
        let native = lifecycle.lifecycle_guard.clone();
        let lost = lifecycle.is_lock_lost();
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal()]);
        Box::pin(async move {
            if lost {
                return Err(TableCatalogStoreError::Unavailable("Catalog admission lost lifecycle protection".into()));
            }
            self.require_bucket_retirement_identity(bucket)?;
            let _native_owner = if let Some(native) = native {
                Some(self.backend.reuse_bucket_lifecycle_read(bucket, native).await?)
            } else {
                // Generic backends retain their original typed guard in the caller.
                self.backend.revalidate_bucket_handle(bucket).await?;
                None
            };
            fence.ensure_held()?;
            let authority = self.read_bucket_retirement_authority(bucket, &fence).await?;
            fence.ensure_held()?;
            if authority.is_some() {
                return Err(TableCatalogStoreError::Conflict(
                    "Vector bucket Catalog retirement prevents ordinary admission".into(),
                ));
            }
            Ok(())
        })
    }

    fn require_bucket_retirement_identity(&self, bucket: &BucketHandle) -> TableCatalogStoreResult<()> {
        if self.paths.scope != CatalogScope::Vector || self.vector_bucket.as_ref() != Some(bucket) {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog retirement differs from the installed Vector bucket".into(),
            ));
        }
        Ok(())
    }

    async fn read_bucket_retirement_authority(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<CatalogBucketRetirementAuthority>> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        let key = self.paths.vector_bucket_retirement_authority_path(bucket)?;
        let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &key, VECTOR_BUCKET_RETIREMENT_AUTHORITY_MAX_BYTES)
            .await?
        else {
            fence.ensure_held()?;
            return Ok(None);
        };
        let record = decode_vector_maintenance_record::<VectorBucketCatalogRetirementV1>(&object.data)?;
        if record.bucket.to_handle()? != *bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog retirement authority belongs to another bucket identity".into(),
            ));
        }
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key)?, &object)?;
        fence.ensure_held()?;
        Ok(Some(CatalogBucketRetirementAuthority { record, reference }))
    }

    async fn read_bucket_retirement_record<T: VectorMaintenanceRecord>(
        &self,
        reference: &CatalogMetadataRefRecordV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<T> {
        fence.ensure_held()?;
        let exact = CatalogMetadataRef::try_from(reference)?;
        let limit =
            usize::try_from(reference.content_length).map_err(|_| maintenance_error("Catalog reference size overflow"))?;
        if limit > T::MAX_BYTES || reference.etag.as_ref().is_none_or(String::is_empty) {
            return Err(maintenance_error("Catalog retirement reference lacks bounded exact bytes"));
        }
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &reference.object_key, limit)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("exact Catalog retirement record".into()))?;
        exact.verify_read(&object)?;
        let observed = CatalogMetadataRef::from_read(exact.object_key().clone(), &object)?;
        if CatalogMetadataRefRecordV1::from(&observed) != *reference {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog retirement record native identity changed".into(),
            ));
        }
        let record = decode_vector_maintenance_record::<T>(&object.data)?;
        fence.ensure_held()?;
        Ok(record)
    }

    async fn write_bucket_retirement_record<T: VectorMaintenanceRecord + Sync>(
        &self,
        path: &str,
        expected: Option<&CatalogMetadataRef>,
        record: &T,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let bytes = encode_vector_bucket_retirement_record(record)?;
        fence.ensure_held()?;
        let lock = self.backend.acquire_write_lock(self.catalog_bucket(), path).await?;
        let mut fence = fence.clone();
        fence.extend_signals([lock.lock_lost_signal()]);
        let current = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), path, T::MAX_BYTES)
            .await?;
        if let Some(current) = &current {
            if current.data == bytes {
                fence.ensure_held()?;
                return CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path.to_owned())?, current);
            }
            let expected = expected
                .ok_or_else(|| TableCatalogStoreError::Conflict("immutable Catalog retirement record already differs".into()))?;
            if expected.object_key().as_str() != path {
                return Err(maintenance_error("Catalog progress precondition belongs to another path"));
            }
            expected.verify_read(current)?;
        } else if expected.is_some() {
            return Err(TableCatalogStoreError::Conflict("Catalog retirement progress disappeared".into()));
        }
        let condition = match expected {
            None => TableCatalogPutPrecondition::IfAbsent,
            Some(reference) => TableCatalogPutPrecondition::IfMatch(
                reference
                    .etag()
                    .ok_or_else(|| maintenance_error("Catalog progress requires exact ETag"))?
                    .to_owned(),
            ),
        };
        begin_publication_mutation()?;
        let outcome = self
            .backend
            .put_object_unlocked_fenced(self.catalog_bucket(), path, bytes.clone(), condition, lock.write_commit_guards(), &fence)
            .await;
        let observed = self
            .backend
            .read_object_unlocked_limited(self.catalog_bucket(), path, T::MAX_BYTES)
            .await?;
        if let Some(observed) = observed.filter(|object| object.data == bytes) {
            fence.ensure_held()?;
            return CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path.to_owned())?, &observed);
        }
        outcome?;
        Err(TableCatalogStoreError::Unavailable(
            "Catalog retirement write has no exact read-back confirmation".into(),
        ))
    }

    /// Remove uncommitted proof/plan/progress staging objects left behind by an
    /// interrupted attempt for this exact `(storage_bucket_id, bucket_incarnation)`
    /// before `authority.json` was ever published.
    async fn cleanup_uncommitted_bucket_retirement_pages(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        if self.read_bucket_retirement_authority(bucket, fence).await?.is_some() {
            return Err(TableCatalogStoreError::Conflict("Catalog retirement authority already exists".into()));
        }
        let root = self.paths.vector_bucket_retirement_root(bucket)?;
        let authority_path = self.paths.vector_bucket_retirement_authority_path(bucket)?;
        let limit = std::num::NonZeroUsize::new(VECTOR_RETIREMENT_MAX_TARGETS)
            .ok_or_else(|| maintenance_error("invalid Catalog retirement scan limit"))?;
        let mut cursor: Option<String> = None;
        loop {
            fence.ensure_held()?;
            let page = self
                .backend
                .list_catalog_objects_page(self.catalog_bucket(), &root, cursor.as_deref(), limit)
                .await?;
            if page.objects.len() > limit.get() || (page.is_truncated && page.objects.is_empty()) {
                return Err(TableCatalogStoreError::Unavailable(
                    "Catalog retirement uncommitted cleanup received an invalid page".into(),
                ));
            }
            for candidate in page.objects {
                if !candidate.key.starts_with(&root)
                    || cursor.as_ref().is_some_and(|after| candidate.key <= *after)
                    || candidate.delete_marker
                    || candidate.version_id.is_some()
                {
                    return Err(maintenance_error(
                        "Catalog retirement uncommitted cleanup saw a foreign, repeated or versioned key",
                    ));
                }
                if candidate.key == authority_path {
                    return Err(TableCatalogStoreError::Conflict(
                        "Catalog retirement authority appeared during uncommitted cleanup".into(),
                    ));
                }
                if !self.paths.is_uncommitted_retirement_page_subpath(&root, &candidate.key) {
                    return Err(maintenance_error("unrecognized record under Catalog retirement prefix"));
                }
                let etag = candidate
                    .etag
                    .filter(|etag| !etag.is_empty())
                    .ok_or_else(|| maintenance_error("uncommitted Catalog retirement page lacks ETag"))?;
                begin_publication_mutation()?;
                self.backend
                    .delete_object_if_match_fenced(self.catalog_bucket(), &candidate.key, &etag, fence)
                    .await?;
                cursor = Some(candidate.key);
            }
            if !page.is_truncated {
                break;
            }
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        fence.ensure_held()
    }
}

#[path = "vector_bucket_retirement/pages.rs"]
mod pages;
use pages::*;
#[path = "vector_bucket_retirement/plan.rs"]
mod plan;
use plan::*;

#[path = "vector_bucket_retirement/execute.rs"]
mod execute;
use execute::*;
#[path = "vector_bucket_retirement/evidence.rs"]
mod evidence;
#[cfg(test)]
#[path = "vector_bucket_retirement/execute_tests.rs"]
mod execute_tests;

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog::store) async fn resume_vector_bucket_catalog_retirement_under_writer(
        &self,
        bucket: &BucketHandle,
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<()>> {
        let Some(authority) = self.read_bucket_retirement_authority(bucket, fence).await? else {
            return Ok(None);
        };
        let validated = self.validate_bucket_retirement_authority(authority, fence).await?;
        self.execute_bucket_retirement_after_validation(&validated, execution, fence)
            .await?;
        Ok(Some(()))
    }

    pub(in crate::table_catalog::store) async fn prepare_vector_bucket_catalog_retirement_under_writer(
        &self,
        bucket: &BucketHandle,
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let validated = self.prepare_bucket_retirement_plan_validated(bucket, fence).await?;
        self.execute_bucket_retirement_after_validation(&validated, execution, fence)
            .await
    }
}

#[cfg(test)]
#[path = "vector_bucket_retirement/observation.rs"]
mod observation;
#[cfg(test)]
pub(crate) use observation::*;
#[cfg(test)]
#[path = "vector_bucket_retirement/native_tests.rs"]
mod native_tests;
