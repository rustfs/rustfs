// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) struct ValidatedBucketCatalogRetirement {
    pub(super) authority: VectorBucketCatalogRetirementV1,
    pub(super) reference: CatalogMetadataRef,
    pub(super) plan: LoadedBucketRetirementPlan,
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

    pub(super) async fn prepare_bucket_retirement_plan_validated(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<ValidatedBucketCatalogRetirement> {
        self.require_bucket_retirement_identity(bucket)?;
        let index = self.index_bucket_catalog_sources(bucket, fence).await?;
        if index.targets_in_order.is_empty() {
            return Err(maintenance_error("Catalog retirement has no source inventory"));
        }
        publication_recovery(self.cleanup_uncommitted_bucket_retirement_pages(bucket, fence)).await?;
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
        for target in index.targets_in_order.iter().cloned() {
            self.validate_bucket_retirement_target_path(&binding, &target)?;
            let proof = if target.kind == VectorCatalogRetirementTargetKind::DeleteIntent {
                let resource = target
                    .resource
                    .as_ref()
                    .ok_or_else(|| maintenance_error("Catalog deletion anchor has no resource"))?;
                Some(
                    self.prepare_bucket_retirement_resource_proof_indexed(bucket, resource, retirement_id, &index, fence)
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
        let (inventory, closure, loaded_plan) = self.load_and_complete_bucket_retirement_digests(&record, fence).await?;
        record.inventory_sha256 = inventory;
        record.closure_sha256 = closure;
        // Every initial progress record is durable before the authority exists.
        // No missing-progress recovery is allowed after this publication.
        self.verify_bucket_retirement_sources_and_progress_with_plan(
            &record,
            &loaded_plan,
            CatalogRetirementProgressRequirement::Prepared,
            fence,
        )
        .await?;
        self.backend.revalidate_bucket_handle(bucket).await?;
        fence.ensure_held()?;
        let path = self.paths.vector_bucket_retirement_authority_path(bucket)?;
        let reference = self.write_bucket_retirement_record(&path, None, &record, fence).await?;
        #[cfg(test)]
        checkpoint(CatalogRetirementObservedPhase::AuthorityPublished, &record, &reference, None).await?;
        Ok(ValidatedBucketCatalogRetirement {
            authority: record,
            reference,
            plan: loaded_plan,
        })
    }

    #[cfg(test)]
    pub(super) async fn prepare_bucket_retirement_plan(
        &self,
        bucket: &BucketHandle,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<CatalogBucketRetirementAuthority> {
        let validated = self.prepare_bucket_retirement_plan_validated(bucket, fence).await?;
        Ok(CatalogBucketRetirementAuthority {
            record: validated.authority,
            reference: validated.reference,
        })
    }

    pub(super) async fn load_and_complete_bucket_retirement_digests(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(String, String, LoadedBucketRetirementPlan)> {
        let loaded = self.load_bucket_retirement_plan(authority, fence).await?;
        let mut verified_sources_by_resource = BTreeMap::new();
        for (rkey, descriptor) in &loaded.resources_by_key {
            let facts = self
                .collect_bucket_retirement_proof_facts(authority, descriptor, fence)
                .await?;
            let verified_sources = self.validate_resource_facts_closure_in_memory(descriptor, &facts, &loaded.targets_by_key)?;
            verified_sources_by_resource.insert(rkey.clone(), verified_sources);
        }
        let mut inventory =
            CatalogRetirementInventoryDigest::new(&authority.bucket, authority.retirement_id, authority.target_count)?;
        for (_, planned) in loaded.targets_by_key.values() {
            if let Some(owner) = &planned.target.resource {
                let verified_sources = verified_sources_by_resource
                    .get(&resource_key(owner))
                    .ok_or_else(|| maintenance_error("Catalog target has no resource proof"))?;
                let observed_source = verified_sources
                    .get(&planned.target.object_key)
                    .ok_or_else(|| maintenance_error("Catalog proof edge has no exact source record"))?;
                if *observed_source != catalog_retirement_target_reference(&planned.target) {
                    return Err(maintenance_error("Catalog proof source identity disagrees with its edge"));
                }
            }
            inventory.push(&planned.target)?;
        }
        let inventory = inventory.finish()?;
        let mut closure = CatalogRetirementClosureDigest::new(
            &authority.bucket,
            authority.retirement_id,
            &inventory,
            authority.resource_count,
        )?;
        for descriptor in loaded.resources_by_key.values() {
            closure.push(&descriptor.resource, descriptor.proof_id, descriptor.fact_count, &descriptor.proof_sha256)?;
        }
        Ok((inventory, closure.finish()?, loaded))
    }

    #[cfg(test)]
    pub(super) async fn bucket_retirement_complete_digests(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(String, String)> {
        let (inventory, closure, _) = self.load_and_complete_bucket_retirement_digests(authority, fence).await?;
        Ok((inventory, closure))
    }

    pub(super) async fn validate_bucket_retirement_authority(
        &self,
        authority: CatalogBucketRetirementAuthority,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<ValidatedBucketCatalogRetirement> {
        let (inventory, closure, loaded_plan) = self
            .load_and_complete_bucket_retirement_digests(&authority.record, fence)
            .await?;
        if inventory != authority.record.inventory_sha256 || closure != authority.record.closure_sha256 {
            return Err(maintenance_error("Catalog retirement authority digest mismatch"));
        }
        self.verify_bucket_retirement_sources_and_progress_with_plan(
            &authority.record,
            &loaded_plan,
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
            plan: loaded_plan,
        })
    }
}
