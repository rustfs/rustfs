// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone)]
pub(super) struct CatalogRetirementPlannedTarget {
    pub(super) plan_page: CatalogMetadataRefRecordV1,
    pub(super) ordinal: u16,
    pub(super) target: VectorCatalogRetirementTargetV1,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn visit_bucket_retirement_plan(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        fence: &TableCatalogObjectMutationFence,
        mut inspect: impl FnMut(&CatalogMetadataRefRecordV1, &VectorBucketCatalogRetirementPlanPageV1) -> TableCatalogStoreResult<()>
        + Send,
    ) -> TableCatalogStoreResult<()> {
        authority.validate()?;
        let bucket = authority.bucket.to_handle()?;
        let mut next = Some(authority.plan_root.clone());
        let mut targets = 0_u64;
        let mut resources = 0_u64;
        for _ in 0..authority.plan_page_count {
            let reference = next
                .take()
                .ok_or_else(|| maintenance_error("Catalog retirement plan ended before its page count"))?;
            let page: VectorBucketCatalogRetirementPlanPageV1 = self.read_bucket_retirement_record(&reference, fence).await?;
            if page.bucket != authority.bucket
                || page.retirement_id != authority.retirement_id
                || reference.object_key
                    != self
                        .paths
                        .vector_bucket_retirement_page_path(&bucket, authority.retirement_id, "plan", page.page_id)?
            {
                return Err(TableCatalogStoreError::Conflict(
                    "Catalog retirement plan page identity or path mismatch".into(),
                ));
            }
            targets = targets
                .checked_add(u64::try_from(page.targets.len()).map_err(|_| maintenance_error("Catalog target count overflow"))?)
                .ok_or_else(|| maintenance_error("Catalog target count overflow"))?;
            resources = resources
                .checked_add(
                    u64::try_from(page.resources.len()).map_err(|_| maintenance_error("Catalog resource count overflow"))?,
                )
                .ok_or_else(|| maintenance_error("Catalog resource count overflow"))?;
            if targets > authority.target_count || resources > authority.resource_count {
                return Err(maintenance_error("Catalog retirement plan exceeds authority counts"));
            }
            inspect(&reference, &page)?;
            next = page.next_page;
        }
        if next.is_some() || targets != authority.target_count || resources != authority.resource_count {
            return Err(maintenance_error("Catalog retirement plan chain or count mismatch"));
        }
        fence.ensure_held()
    }

    pub(super) async fn next_bucket_retirement_target(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        after: Option<&str>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<CatalogRetirementPlannedTarget>> {
        let mut selected: Option<CatalogRetirementPlannedTarget> = None;
        self.visit_bucket_retirement_plan(authority, fence, |reference, page| {
            for (ordinal, target) in page.targets.iter().enumerate() {
                self.validate_bucket_retirement_target_path(&authority.bucket, target)?;
                if after.is_some_and(|after| target.object_key.as_str() <= after) {
                    continue;
                }
                match &selected {
                    Some(previous) if previous.target.object_key == target.object_key => {
                        return Err(maintenance_error("duplicate target across Catalog plan pages"));
                    }
                    Some(previous) if previous.target.object_key < target.object_key => {}
                    _ => {
                        selected = Some(CatalogRetirementPlannedTarget {
                            plan_page: reference.clone(),
                            ordinal: u16::try_from(ordinal).map_err(|_| maintenance_error("Catalog target ordinal overflow"))?,
                            target: target.clone(),
                        })
                    }
                }
            }
            Ok(())
        })
        .await?;
        Ok(selected)
    }

    pub(super) async fn next_bucket_retirement_resource(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        after: Option<&(String, String, String)>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<VectorCatalogRetiredResourceProofV1>> {
        let mut selected: Option<VectorCatalogRetiredResourceProofV1> = None;
        self.visit_bucket_retirement_plan(authority, fence, |_, page| {
            for descriptor in &page.resources {
                let key = resource_key(&descriptor.resource);
                if after.is_some_and(|after| &key <= after) {
                    continue;
                }
                match &selected {
                    Some(previous) if resource_key(&previous.resource) == key => {
                        return Err(maintenance_error("duplicate resource across Catalog plan pages"));
                    }
                    Some(previous) if resource_key(&previous.resource) < key => {}
                    _ => selected = Some(descriptor.clone()),
                }
            }
            Ok(())
        })
        .await?;
        if let Some(selected) = &selected {
            self.visit_bucket_retirement_plan(authority, fence, |_, page| {
                for other in &page.resources {
                    if other.resource != selected.resource
                        && (other.resource.table_id == selected.resource.table_id || other.proof_id == selected.proof_id)
                    {
                        return Err(maintenance_error("Catalog resource or proof identity is reused across pages"));
                    }
                }
                Ok(())
            })
            .await?;
        }
        Ok(selected)
    }

    pub(super) async fn visit_bucket_retirement_proof(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        fence: &TableCatalogObjectMutationFence,
        mut inspect: impl FnMut(&VectorCatalogRetirementProofFactV1) -> TableCatalogStoreResult<()> + Send,
    ) -> TableCatalogStoreResult<()> {
        descriptor.validate()?;
        if descriptor.resource.bucket != authority.bucket {
            return Err(TableCatalogStoreError::Conflict("Catalog proof descriptor has foreign identity".into()));
        }
        let bucket = authority.bucket.to_handle()?;
        let mut next = Some(descriptor.proof_root.clone());
        let mut count = 0_u64;
        let mut digest = CatalogRetirementProofDigest::new(
            &authority.bucket,
            &descriptor.resource,
            authority.retirement_id,
            descriptor.proof_id,
            descriptor.fact_count,
        )?;
        for ordinal in 0..descriptor.proof_page_count {
            let reference = next
                .take()
                .ok_or_else(|| maintenance_error("Catalog proof ended before its page count"))?;
            let page: VectorCatalogRetiredResourceProofPageV1 = self.read_bucket_retirement_record(&reference, fence).await?;
            if page.bucket != authority.bucket
                || page.retirement_id != authority.retirement_id
                || page.resource != descriptor.resource
                || page.proof_id != descriptor.proof_id
                || page.page_ordinal != ordinal
                || page.page_count != descriptor.proof_page_count
                || page.first_fact_ordinal != count
                || page.total_fact_count != descriptor.fact_count
                || page.proof_sha256 != descriptor.proof_sha256
                || reference.object_key
                    != self.paths.vector_bucket_retirement_proof_path(
                        &bucket,
                        authority.retirement_id,
                        descriptor.proof_id,
                        page.page_id,
                    )?
            {
                return Err(TableCatalogStoreError::Conflict(
                    "Catalog proof page identity, sequence or exact path mismatch".into(),
                ));
            }
            for fact in &page.facts {
                digest.push(fact)?;
                inspect(fact)?;
                count = count
                    .checked_add(1)
                    .ok_or_else(|| maintenance_error("Catalog proof fact count overflow"))?;
            }
            next = page.next_page;
        }
        if next.is_some() || count != descriptor.fact_count || digest.finish()? != descriptor.proof_sha256 {
            return Err(maintenance_error("Catalog proof chain, count or digest mismatch"));
        }
        fence.ensure_held()
    }

    pub(super) async fn require_bucket_retirement_source_fact(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        descriptor: &VectorCatalogRetiredResourceProofV1,
        expected: &CatalogMetadataRefRecordV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let mut found = false;
        self.visit_bucket_retirement_proof(authority, descriptor, fence, |fact| {
            if let VectorCatalogRetirementProofFactV1::SourceRecord { reference } = fact
                && reference.object_key == expected.object_key
            {
                if reference != expected || found {
                    return Err(maintenance_error("Catalog proof source identity disagrees with its edge"));
                }
                found = true;
            }
            Ok(())
        })
        .await?;
        if !found {
            return Err(maintenance_error("Catalog proof edge has no exact source record"));
        }
        Ok(())
    }

    pub(super) fn validate_bucket_retirement_target_path(
        &self,
        bucket: &VectorBucketBindingV1,
        target: &VectorCatalogRetirementTargetV1,
    ) -> TableCatalogStoreResult<()> {
        use VectorCatalogRetirementTargetKind as Kind;
        target.validate()?;
        let handle = bucket.to_handle()?;
        let storage = handle.storage_bucket_id().as_str();
        let namespace = Namespace::parse("indexes").map_err(|e| maintenance_error(&e.to_string()))?;
        let key = target.object_key.as_str();
        if target.resource.is_none() {
            let expected = match target.kind {
                Kind::BucketEntry => self.paths.table_bucket_entry_path(storage),
                Kind::NamespaceEntry => self.paths.namespace_entry_path(storage, &namespace),
                _ => return Err(maintenance_error("Catalog resource target has no owner")),
            };
            return if key == expected {
                Ok(())
            } else {
                Err(maintenance_error("Catalog bucket target path mismatch"))
            };
        }
        let owner = target
            .resource
            .as_ref()
            .ok_or_else(|| maintenance_error("Catalog target has no resource"))?;
        if owner.bucket != *bucket {
            return Err(TableCatalogStoreError::Conflict("Catalog target belongs to another incarnation".into()));
        }
        let identity = owner.identity()?;
        let name = CatalogName::parse(owner.name.clone()).map_err(|e| maintenance_error(&e.to_string()))?;
        let prefix = self.paths.vector_maintenance_prefix(&identity)?;
        let valid = match target.kind {
            Kind::TableEntry => key == self.paths.table_entry_path(storage, &namespace, &name),
            Kind::CreateIntent => key == self.paths.vector_create_intent_path(storage, &namespace, &name),
            Kind::DeleteIntent => key == self.paths.vector_delete_intent_path(storage, &owner.table_id),
            Kind::CommitLog => hashed_json_tail(key, &self.paths.commit_log_entries_prefix(storage, &owner.table_id)),
            Kind::CommitIdempotency => {
                hashed_json_tail(key, &self.paths.commit_idempotency_entries_prefix(storage, &owner.table_id))
            }
            Kind::MaintenanceJob => hashed_json_tail(key, &format!("{prefix}jobs/")),
            Kind::MaintenanceSource => key == format!("{prefix}sources/{}.json", target.content_sha256),
            Kind::RetirementEvidence => key == format!("{prefix}evidence/{}.json", target.content_sha256),
            Kind::RetirementPlan => key == format!("{prefix}plans/{}.json", target.content_sha256),
            Kind::RetirementAuthorization => hashed_json_tail(key, &format!("{prefix}authorizations/")),
            Kind::RetirementProgress => hashed_json_tail(key, &format!("{prefix}progress/")),
            Kind::MaintenanceResult => key.strip_prefix(&format!("{prefix}results/")).is_some_and(|tail| {
                let mut parts = tail.split('/');
                match (parts.next(), parts.next(), parts.next(), parts.next()) {
                    (Some(job), Some(attempt), Some(body), None) => {
                        decode_catalog_digest(job).is_ok()
                            && attempt
                                .strip_prefix("attempt-")
                                .and_then(|n| n.parse::<u16>().ok())
                                .is_some_and(|n| n > 0 && format!("attempt-{n}") == attempt)
                            && body == format!("{}.json", target.content_sha256)
                    }
                    _ => false,
                }
            }),
            Kind::BucketEntry | Kind::NamespaceEntry => false,
        };
        if valid {
            Ok(())
        } else {
            Err(maintenance_error("Catalog target kind and exact path disagree"))
        }
    }
}

pub(super) fn resource_key(resource: &VectorMaintenanceResourceV1) -> (String, String, String) {
    (resource.namespace.clone(), resource.name.clone(), resource.table_id.clone())
}
fn hashed_json_tail(key: &str, prefix: &str) -> bool {
    key.strip_prefix(prefix)
        .and_then(|tail| tail.strip_suffix(".json"))
        .is_some_and(|hash| decode_catalog_digest(hash).is_ok())
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn next_bucket_catalog_source(
        &self,
        bucket: &BucketHandle,
        after: Option<&str>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<(String, TableCatalogObject)>> {
        self.require_bucket_retirement_identity(bucket)?;
        fence.ensure_held()?;
        let prefix = self.paths.table_bucket_root_prefix(bucket.storage_bucket_id().as_str());
        let page = self
            .backend
            .list_catalog_objects_page(self.catalog_bucket(), &prefix, after, std::num::NonZeroUsize::MIN)
            .await?;
        if page.objects.len() > 1 || (page.is_truncated && page.objects.is_empty()) {
            return Err(TableCatalogStoreError::Unavailable(
                "Catalog retirement received an invalid source page".into(),
            ));
        }
        let Some(candidate) = page.objects.into_iter().next() else {
            fence.ensure_held()?;
            return Ok(None);
        };
        if !candidate.key.starts_with(&prefix)
            || after.is_some_and(|after| candidate.key.as_str() <= after)
            || candidate.delete_marker
            || candidate.version_id.is_some()
        {
            return Err(maintenance_error(
                "Catalog retirement source page has a foreign, repeated or versioned key",
            ));
        }
        let etag = candidate
            .etag
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| maintenance_error("Catalog retirement source lacks ETag"))?;
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &candidate.key, VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("Catalog retirement source disappeared after discovery".into()))?;
        if object.etag.as_deref() != Some(etag.as_str()) {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog retirement source changed after discovery".into(),
            ));
        }
        fence.ensure_held()?;
        Ok(Some((candidate.key, object)))
    }

    pub(super) async fn bucket_retirement_target_by_key(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        key: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<CatalogRetirementPlannedTarget>> {
        let mut found = None;
        self.visit_bucket_retirement_plan(authority, fence, |reference, page| {
            for (ordinal, target) in page.targets.iter().enumerate() {
                if target.object_key != key {
                    continue;
                }
                if found.is_some() {
                    return Err(maintenance_error("Catalog plan repeats an exact target"));
                }
                self.validate_bucket_retirement_target_path(&authority.bucket, target)?;
                found = Some(CatalogRetirementPlannedTarget {
                    plan_page: reference.clone(),
                    ordinal: u16::try_from(ordinal).map_err(|_| maintenance_error("Catalog target ordinal overflow"))?,
                    target: target.clone(),
                });
            }
            Ok(())
        })
        .await?;
        Ok(found)
    }

    pub(super) async fn bucket_retirement_resource_by_identity(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        owner: &VectorMaintenanceResourceV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<VectorCatalogRetiredResourceProofV1> {
        let mut found = None;
        self.visit_bucket_retirement_plan(authority, fence, |_, page| {
            for descriptor in &page.resources {
                if descriptor.resource != *owner {
                    continue;
                }
                if found.is_some() {
                    return Err(maintenance_error("Catalog plan repeats a resource proof"));
                }
                found = Some(descriptor.clone());
            }
            Ok(())
        })
        .await?;
        found.ok_or_else(|| maintenance_error("Catalog target has no resource proof"))
    }
}

pub(super) fn catalog_retirement_target_reference(target: &VectorCatalogRetirementTargetV1) -> CatalogMetadataRefRecordV1 {
    CatalogMetadataRefRecordV1 {
        object_key: target.object_key.clone(),
        content_length: target.content_length,
        content_sha256: target.content_sha256.clone(),
        body_revision: target.body_revision,
        etag: Some(target.etag.clone()),
    }
}
