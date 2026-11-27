// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) struct ValidatedBucketCatalogRetirement {
    pub(super) authority: VectorBucketCatalogRetirementV1,
    pub(super) reference: CatalogMetadataRef,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn flush_bucket_retirement_plan_page(
        &self,
        bucket: &BucketHandle,
        page: &VectorBucketCatalogRetirementPlanPageV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogMetadataRef> {
        let path = self
            .paths
            .vector_bucket_retirement_page_path(bucket, page.retirement_id, "plan", page.page_id)?;
        let reference = self.write_bucket_retirement_record(&path, None, page, fence).await?;
        let progress = VectorBucketCatalogRetirementProgressPageV1 {
            version: 1,
            bucket: page.bucket.clone(),
            retirement_id: page.retirement_id,
            plan_page: (&reference).into(),
            revision: Uuid::new_v4(),
            previous_revision: None,
            targets: page
                .targets
                .iter()
                .enumerate()
                .map(|(ordinal, _)| {
                    Ok(VectorCatalogRetirementTargetProgressV1 {
                        ordinal: u16::try_from(ordinal).map_err(|_| maintenance_error("Catalog target ordinal overflow"))?,
                        state: VectorCatalogRetirementTargetState::Pending,
                        dispatch_id: None,
                        worker_instance_id: None,
                        worker_boot_id: None,
                        started_at: None,
                        acknowledged_at: None,
                        failure: None,
                    })
                })
                .collect::<TableCatalogStoreResult<Vec<_>>>()?,
        };
        let progress_path =
            self.paths
                .vector_bucket_retirement_page_path(bucket, page.retirement_id, "progress", page.page_id)?;
        self.write_bucket_retirement_record(&progress_path, None, &progress, fence)
            .await?;
        Ok(reference)
    }

    pub(super) async fn prepare_bucket_retirement_plan(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogBucketRetirementAuthority> {
        self.require_bucket_retirement_identity(bucket)?;
        let binding = VectorBucketBindingV1::from_handle(bucket)?;
        let retirement_id = Uuid::new_v4();
        let mut page = VectorBucketCatalogRetirementPlanPageV1 {
            version: 1,
            bucket: binding.clone(),
            retirement_id,
            page_id: Uuid::new_v4(),
            next_page: None,
            resources: Vec::new(),
            targets: Vec::new(),
        };
        let mut page_count = 0_u32;
        let mut target_count = 0_u64;
        let mut resource_count = 0_u64;
        let mut cursor = None;
        while let Some((key, object)) = self.next_bucket_catalog_source(bucket, cursor.as_deref(), fence).await? {
            let target = self.classify_bucket_retirement_source(bucket, &key, &object, fence).await?;
            self.validate_bucket_retirement_target_path(&binding, &target)?;
            let proof = if target.kind == VectorCatalogRetirementTargetKind::DeleteIntent {
                let resource = target
                    .resource
                    .as_ref()
                    .ok_or_else(|| maintenance_error("Catalog deletion anchor has no resource"))?;
                Some(
                    self.prepare_bucket_retirement_resource_proof(bucket, resource, retirement_id, fence)
                        .await?,
                )
            } else {
                None
            };
            // An immutable chain is assembled backward; logical hashing has its
            // own global ordering and does not depend on this page layout.
            if page.targets.len() == VECTOR_RETIREMENT_MAX_TARGETS {
                let previous = self.flush_bucket_retirement_plan_page(bucket, &page, fence).await?;
                page_count = page_count
                    .checked_add(1)
                    .ok_or_else(|| maintenance_error("Catalog plan page count overflow"))?;
                page = VectorBucketCatalogRetirementPlanPageV1 {
                    version: 1,
                    bucket: binding.clone(),
                    retirement_id,
                    page_id: Uuid::new_v4(),
                    next_page: Some((&previous).into()),
                    resources: Vec::new(),
                    targets: Vec::new(),
                };
            }
            page.targets.push(target.clone());
            if let Some(proof) = &proof {
                page.resources.push(proof.clone());
            }
            if let Err(error) = encode_vector_bucket_retirement_record(&page) {
                page.targets.pop();
                if proof.is_some() {
                    page.resources.pop();
                }
                if page.targets.is_empty() {
                    return Err(error);
                }
                let previous = self.flush_bucket_retirement_plan_page(bucket, &page, fence).await?;
                page_count = page_count
                    .checked_add(1)
                    .ok_or_else(|| maintenance_error("Catalog plan page count overflow"))?;
                page = VectorBucketCatalogRetirementPlanPageV1 {
                    version: 1,
                    bucket: binding.clone(),
                    retirement_id,
                    page_id: Uuid::new_v4(),
                    next_page: Some((&previous).into()),
                    resources: proof.into_iter().collect(),
                    targets: vec![target],
                };
                encode_vector_bucket_retirement_record(&page)?;
            }
            target_count = target_count
                .checked_add(1)
                .ok_or_else(|| maintenance_error("Catalog target count overflow"))?;
            if page
                .targets
                .last()
                .is_some_and(|target| target.kind == VectorCatalogRetirementTargetKind::DeleteIntent)
            {
                resource_count = resource_count
                    .checked_add(1)
                    .ok_or_else(|| maintenance_error("Catalog resource count overflow"))?;
            }
            cursor = Some(key);
        }
        if page.targets.is_empty() {
            return Err(maintenance_error("Catalog retirement has no source inventory"));
        }
        let root = self.flush_bucket_retirement_plan_page(bucket, &page, fence).await?;
        page_count = page_count
            .checked_add(1)
            .ok_or_else(|| maintenance_error("Catalog plan page count overflow"))?;
        let mut record = VectorBucketCatalogRetirementV1 {
            version: 1,
            bucket: binding,
            retirement_id,
            created_at: maintenance_now(),
            plan_root: (&root).into(),
            plan_page_count: page_count,
            target_count,
            resource_count,
            inventory_sha256: "0".repeat(64),
            closure_sha256: "0".repeat(64),
        };
        let (inventory, closure) = self.bucket_retirement_complete_digests(&record, fence).await?;
        record.inventory_sha256 = inventory;
        record.closure_sha256 = closure;
        // Every initial progress record is durable before the authority exists.
        // No missing-progress recovery is allowed after this publication.
        self.verify_bucket_retirement_sources_and_progress(&record, CatalogRetirementProgressRequirement::Prepared, fence)
            .await?;
        self.backend.revalidate_bucket_handle(bucket).await?;
        fence.ensure_held()?;
        let path = self.paths.vector_bucket_retirement_authority_path(bucket)?;
        let reference = self.write_bucket_retirement_record(&path, None, &record, fence).await?;
        #[cfg(test)]
        checkpoint(CatalogRetirementObservedPhase::AuthorityPublished, &record, &reference, None).await?;
        Ok(CatalogBucketRetirementAuthority { record, reference })
    }

    pub(super) async fn bucket_retirement_complete_digests(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(String, String)> {
        let mut inventory =
            CatalogRetirementInventoryDigest::new(&authority.bucket, authority.retirement_id, authority.target_count)?;
        let mut after = None;
        for _ in 0..authority.target_count {
            let planned = self
                .next_bucket_retirement_target(authority, after.as_deref(), fence)
                .await?
                .ok_or_else(|| maintenance_error("Catalog retirement inventory is incomplete"))?;
            if let Some(owner) = &planned.target.resource {
                let descriptor = self.bucket_retirement_resource_by_identity(authority, owner, fence).await?;
                self.require_bucket_retirement_source_fact(
                    authority,
                    &descriptor,
                    &catalog_retirement_target_reference(&planned.target),
                    fence,
                )
                .await?;
            }
            inventory.push(&planned.target)?;
            after = Some(planned.target.object_key);
        }
        if self
            .next_bucket_retirement_target(authority, after.as_deref(), fence)
            .await?
            .is_some()
        {
            return Err(maintenance_error("Catalog inventory has excess targets"));
        }
        let inventory = inventory.finish()?;
        let mut closure = CatalogRetirementClosureDigest::new(
            &authority.bucket,
            authority.retirement_id,
            &inventory,
            authority.resource_count,
        )?;
        let mut after = None;
        for _ in 0..authority.resource_count {
            let descriptor = self
                .next_bucket_retirement_resource(authority, after.as_ref(), fence)
                .await?
                .ok_or_else(|| maintenance_error("Catalog resource proof inventory is incomplete"))?;
            self.visit_bucket_retirement_proof(authority, &descriptor, fence, |_| Ok(()))
                .await?;
            self.validate_bucket_retirement_resource_closure(authority, &descriptor, fence)
                .await?;
            closure.push(&descriptor.resource, descriptor.proof_id, descriptor.fact_count, &descriptor.proof_sha256)?;
            after = Some(resource_key(&descriptor.resource));
        }
        if self
            .next_bucket_retirement_resource(authority, after.as_ref(), fence)
            .await?
            .is_some()
        {
            return Err(maintenance_error("Catalog closure has excess resources"));
        }
        Ok((inventory, closure.finish()?))
    }

    pub(super) async fn validate_bucket_retirement_authority(
        &self,
        authority: CatalogBucketRetirementAuthority,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<ValidatedBucketCatalogRetirement> {
        let (inventory, closure) = self.bucket_retirement_complete_digests(&authority.record, fence).await?;
        if inventory != authority.record.inventory_sha256 || closure != authority.record.closure_sha256 {
            return Err(maintenance_error("Catalog retirement authority digest mismatch"));
        }
        self.verify_bucket_retirement_sources_and_progress(
            &authority.record,
            CatalogRetirementProgressRequirement::Recoverable,
            fence,
        )
        .await?;
        let current: VectorBucketCatalogRetirementV1 = self
            .read_bucket_retirement_record(&(&authority.reference).into(), fence)
            .await?;
        if current != authority.record {
            return Err(TableCatalogStoreError::Conflict("Catalog retirement authority changed".into()));
        }
        Ok(ValidatedBucketCatalogRetirement {
            authority: authority.record,
            reference: authority.reference,
        })
    }
}
