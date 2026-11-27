// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) enum CatalogRetirementProgressRequirement {
    Prepared,
    Recoverable,
    Complete,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn load_bucket_retirement_progress_pages(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        plan: &LoadedBucketRetirementPlan,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Vec<(CatalogMetadataRef, VectorBucketCatalogRetirementProgressPageV1)>> {
        let bucket = authority.bucket.to_handle()?;
        let mut progress_pages = Vec::with_capacity(plan.pages.len());
        for loaded_page in &plan.pages {
            fence.ensure_held()?;
            let path = self.paths.vector_bucket_retirement_page_path(
                &bucket,
                authority.retirement_id,
                "progress",
                loaded_page.page.page_id,
            )?;
            let object = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("Catalog retirement progress is missing; recovery is required".into())
                })?;
            let progress: VectorBucketCatalogRetirementProgressPageV1 = decode_vector_maintenance_record(&object.data)?;
            if progress.bucket != authority.bucket
                || progress.retirement_id != authority.retirement_id
                || progress.plan_page != loaded_page.reference
                || progress.targets.len() != loaded_page.page.targets.len()
            {
                return Err(maintenance_error("Catalog progress does not bind the complete original plan page"));
            }
            let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
            if reference.etag().is_none_or(str::is_empty) {
                return Err(maintenance_error("Catalog progress has no exact native ETag"));
            }
            progress_pages.push((reference, progress));
        }
        fence.ensure_held()?;
        Ok(progress_pages)
    }

    async fn require_bucket_retirement_exact_source(
        &self,
        target: &VectorCatalogRetirementTargetV1,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let reference = CatalogMetadataRef::try_from(&catalog_retirement_target_reference(target))?;
        let limit = usize::try_from(target.content_length).map_err(|_| maintenance_error("Catalog source length overflow"))?;
        if limit > VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES {
            return Err(maintenance_error("Catalog source exceeds its validated byte bound"));
        }
        let object = self
            .backend
            .read_object_limited(self.catalog_bucket(), &target.object_key, limit)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("pending Catalog retirement source is absent".into()))?;
        reference.verify_read(&object)?;
        let observed = CatalogMetadataRef::from_read(reference.object_key().clone(), &object)?;
        if CatalogMetadataRefRecordV1::from(&observed) != catalog_retirement_target_reference(target) {
            return Err(TableCatalogStoreError::Conflict(
                "pending Catalog retirement native identity changed".into(),
            ));
        }
        fence.ensure_held()
    }

    pub(super) async fn verify_bucket_retirement_sources_and_progress_with_plan(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        plan: &LoadedBucketRetirementPlan,
        requirement: CatalogRetirementProgressRequirement,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let bucket = authority.bucket.to_handle()?;
        self.require_bucket_retirement_identity(&bucket)?;
        self.backend.revalidate_bucket_handle(&bucket).await?;
        let progress_pages = self.load_bucket_retirement_progress_pages(authority, plan, fence).await?;
        let live_sources: BTreeMap<String, TableCatalogObject> = self
            .collect_bucket_catalog_sources(&bucket, fence)
            .await?
            .into_iter()
            .collect();

        for (page_index, planned) in plan.targets_by_key.values() {
            let loaded_page = plan
                .pages
                .get(*page_index)
                .ok_or_else(|| maintenance_error("Catalog target page index is out of bounds"))?;
            if loaded_page.page.targets.get(usize::from(planned.ordinal)) != Some(&planned.target) {
                return Err(maintenance_error("Catalog target differs from its original plan page"));
            }
            let state = progress_pages[*page_index].1.targets[usize::from(planned.ordinal)].state;
            match state {
                VectorCatalogRetirementTargetState::Pending => {
                    if matches!(requirement, CatalogRetirementProgressRequirement::Complete) {
                        return Err(TableCatalogStoreError::Unavailable("Catalog retirement still has pending targets".into()));
                    }
                    let expected = catalog_retirement_target_reference(&planned.target);
                    let reference = CatalogMetadataRef::try_from(&expected)?;
                    let limit = usize::try_from(planned.target.content_length)
                        .map_err(|_| maintenance_error("Catalog source length overflow"))?;
                    if limit > VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES {
                        return Err(maintenance_error("Catalog source exceeds its validated byte bound"));
                    }
                    let object = live_sources
                        .get(&planned.target.object_key)
                        .ok_or_else(|| TableCatalogStoreError::Conflict("pending Catalog retirement source is absent".into()))?;
                    reference.verify_read(object)?;
                    let observed = CatalogMetadataRef::from_read(reference.object_key().clone(), object)?;
                    if CatalogMetadataRefRecordV1::from(&observed) != expected {
                        return Err(TableCatalogStoreError::Conflict(
                            "pending Catalog retirement native identity changed".into(),
                        ));
                    }
                }
                VectorCatalogRetirementTargetState::DeleteAcknowledged => {
                    if matches!(requirement, CatalogRetirementProgressRequirement::Prepared) {
                        return Err(maintenance_error("unpublished Catalog plan contains dispatched progress"));
                    }
                }
                VectorCatalogRetirementTargetState::InFlight | VectorCatalogRetirementTargetState::Unknown => {
                    return Err(TableCatalogStoreError::Unavailable(
                        "Catalog retirement has an unresolved native outcome; original recovery confirmation is required".into(),
                    ));
                }
            }
        }

        // Reconcile the current source inventory against the retained exact plan.
        // Completed targets may be absent; new or reappearing sources block cleanup.
        for (key, object) in &live_sources {
            let (page_index, planned) = plan
                .targets_by_key
                .get(key)
                .ok_or_else(|| TableCatalogStoreError::Conflict("Catalog source is outside the retirement inventory".into()))?;
            if progress_pages[*page_index].1.targets[usize::from(planned.ordinal)].state
                != VectorCatalogRetirementTargetState::Pending
            {
                return Err(TableCatalogStoreError::Conflict(
                    "acknowledged Catalog retirement source is present".into(),
                ));
            }
            let expected = catalog_retirement_target_reference(&planned.target);
            let observed = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone())?, object)?;
            if CatalogMetadataRefRecordV1::from(&observed) != expected {
                return Err(TableCatalogStoreError::Conflict("Catalog inventory native identity changed".into()));
            }
        }
        self.backend.revalidate_bucket_handle(&bucket).await?;
        fence.ensure_held()
    }

    async fn execute_bucket_retirement_target_with_progress(
        &self,
        validated: &ValidatedBucketCatalogRetirement,
        planned: &CatalogRetirementPlannedTarget,
        page_progress: &mut (CatalogMetadataRef, VectorBucketCatalogRetirementProgressPageV1),
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let authority = &validated.authority;
        let ordinal = usize::from(planned.ordinal);
        match page_progress.1.targets[ordinal].state {
            VectorCatalogRetirementTargetState::DeleteAcknowledged => return Ok(()),
            VectorCatalogRetirementTargetState::Pending => {}
            VectorCatalogRetirementTargetState::InFlight | VectorCatalogRetirementTargetState::Unknown => {
                return Err(TableCatalogStoreError::Unavailable(
                    "Catalog target requires original native recovery confirmation".into(),
                ));
            }
        }
        let bucket = authority.bucket.to_handle()?;
        self.backend.revalidate_bucket_handle(&bucket).await?;
        self.require_bucket_retirement_exact_source(&planned.target, fence).await?;
        let reference = page_progress.0.clone();
        let mut progress = page_progress.1.clone();
        progress.previous_revision = Some(progress.revision);
        progress.revision = Uuid::new_v4();
        progress.targets[ordinal] = VectorCatalogRetirementTargetProgressV1 {
            ordinal: planned.ordinal,
            state: VectorCatalogRetirementTargetState::InFlight,
            dispatch_id: Some(Uuid::new_v4()),
            worker_instance_id: Some(execution.worker_instance_id),
            worker_boot_id: Some(execution.worker_boot_id),
            started_at: Some(maintenance_now()),
            acknowledged_at: None,
            failure: None,
        };
        let in_flight = self
            .write_bucket_retirement_record(reference.object_key().as_str(), Some(&reference), &progress, fence)
            .await?;
        *page_progress = (in_flight.clone(), progress.clone());
        #[cfg(test)]
        {
            let mut observed = CatalogRetirementObservedTarget::new(planned, &in_flight, &progress);
            observed.previous_progress_ref = Some((&reference).into());
            observed.progress_publish = Some(CatalogRetirementObservedOutcome::Succeeded);
            checkpoint(
                CatalogRetirementObservedPhase::BeforeTargetDispatch,
                authority,
                &validated.reference,
                Some(observed),
            )
            .await?;
        }
        self.backend.revalidate_bucket_handle(&bucket).await?;
        fence.ensure_held()?;
        begin_publication_mutation()?;
        // This await owns the real conditional native call. Cancellation or loss
        // before durable acknowledgment leaves its original InFlight evidence.
        let native = self
            .backend
            .delete_object_if_match_fenced(self.catalog_bucket(), &planned.target.object_key, &planned.target.etag, fence)
            .await;
        let subsequent_fence = native.is_ok().then(|| fence.ensure_held());
        #[cfg(test)]
        let observed_before_publication = {
            let mut observed = CatalogRetirementObservedTarget::new(planned, &in_flight, &progress);
            observed.fenced_delete = Some(CatalogRetirementObservedOutcome::from_result(&native));
            observed.subsequent_fence = subsequent_fence.as_ref().map(CatalogRetirementObservedOutcome::from_result);
            observed
        };
        #[cfg(test)]
        let after_native = checkpoint(
            CatalogRetirementObservedPhase::TargetNativeReturned,
            authority,
            &validated.reference,
            Some(observed_before_publication.clone()),
        )
        .await;
        if let Err(error) = native {
            progress.previous_revision = Some(progress.revision);
            progress.revision = Uuid::new_v4();
            progress.targets[ordinal].state = VectorCatalogRetirementTargetState::Unknown;
            progress.targets[ordinal].failure = Some(VectorMaintenanceFailureCode::UnknownOutcome);
            // An unsuccessful diagnostic update leaves the durable InFlight
            // state. Preserve the original native error in either case.
            let _unknown = self
                .write_bucket_retirement_record(in_flight.object_key().as_str(), Some(&in_flight), &progress, fence)
                .await;
            #[cfg(test)]
            {
                let mut observed = observed_before_publication;
                observed.progress_publish = Some(CatalogRetirementObservedOutcome::from_result(&_unknown));
                match &_unknown {
                    Ok(reference) => {
                        observed.previous_progress_ref = Some((&in_flight).into());
                        observed.progress_ref = reference.into();
                        observed.progress = Box::new(progress.clone());
                    }
                    Err(_) => observed.attempted_progress = Some(Box::new(progress.clone())),
                }
                let _observed = checkpoint(
                    CatalogRetirementObservedPhase::TargetProgressPublished,
                    authority,
                    &validated.reference,
                    Some(observed),
                )
                .await;
            }
            if let Ok(unknown_ref) = _unknown {
                *page_progress = (unknown_ref, progress);
            }
            return Err(error);
        }
        subsequent_fence.ok_or_else(|| maintenance_error("successful Catalog delete has no final fence result"))??;
        #[cfg(test)]
        after_native?;
        progress.previous_revision = Some(progress.revision);
        progress.revision = Uuid::new_v4();
        progress.targets[ordinal].state = VectorCatalogRetirementTargetState::DeleteAcknowledged;
        progress.targets[ordinal].acknowledged_at = Some(maintenance_now());
        let published = self
            .write_bucket_retirement_record(in_flight.object_key().as_str(), Some(&in_flight), &progress, fence)
            .await;
        #[cfg(test)]
        let after_progress = {
            let mut observed = observed_before_publication;
            observed.progress_publish = Some(CatalogRetirementObservedOutcome::from_result(&published));
            match &published {
                Ok(reference) => {
                    observed.previous_progress_ref = Some((&in_flight).into());
                    observed.progress_ref = reference.into();
                    observed.progress = Box::new(progress.clone());
                }
                Err(_) => observed.attempted_progress = Some(Box::new(progress.clone())),
            }
            checkpoint(
                CatalogRetirementObservedPhase::TargetProgressPublished,
                authority,
                &validated.reference,
                Some(observed),
            )
            .await
        };
        let published_ref = published?;
        *page_progress = (published_ref, progress);
        #[cfg(test)]
        after_progress?;
        fence.ensure_held()
    }

    pub(super) async fn execute_bucket_retirement_after_validation(
        &self,
        validated: &ValidatedBucketCatalogRetirement,
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let authority = &validated.authority;
        let original: VectorBucketCatalogRetirementV1 = self
            .read_bucket_retirement_record(&(&validated.reference).into(), fence)
            .await?;
        if original != *authority {
            return Err(TableCatalogStoreError::Conflict("Catalog authority changed before execution".into()));
        }
        let mut progress_pages = self
            .load_bucket_retirement_progress_pages(authority, &validated.plan, fence)
            .await?;
        for (_, progress) in &progress_pages {
            if progress.targets.iter().any(|target| {
                matches!(
                    target.state,
                    VectorCatalogRetirementTargetState::InFlight | VectorCatalogRetirementTargetState::Unknown
                )
            }) {
                return Err(TableCatalogStoreError::Unavailable(
                    "Catalog retirement has an unresolved native outcome; original recovery confirmation is required".into(),
                ));
            }
        }
        let mut by_rank: [Vec<(usize, CatalogRetirementPlannedTarget)>; 7] = std::array::from_fn(|_| Vec::new());
        for (page_index, planned) in validated.plan.targets_by_key.values() {
            let loaded_page = validated
                .plan
                .pages
                .get(*page_index)
                .ok_or_else(|| maintenance_error("Catalog target page index is out of bounds"))?;
            if loaded_page.page.targets.get(usize::from(planned.ordinal)) != Some(&planned.target) {
                return Err(maintenance_error("Catalog target differs from its original plan page"));
            }
            let rank = usize::from(bucket_retirement_delete_rank(planned.target.kind));
            by_rank[rank].push((*page_index, planned.clone()));
        }
        for rank_targets in &by_rank {
            for (page_index, planned) in rank_targets {
                self.execute_bucket_retirement_target_with_progress(
                    validated,
                    planned,
                    &mut progress_pages[*page_index],
                    execution,
                    fence,
                )
                .await?;
            }
        }
        self.verify_bucket_retirement_sources_and_progress_with_plan(
            authority,
            &validated.plan,
            CatalogRetirementProgressRequirement::Complete,
            fence,
        )
        .await?;
        #[cfg(test)]
        checkpoint(
            CatalogRetirementObservedPhase::CatalogPreparationCompleted,
            authority,
            &validated.reference,
            None,
        )
        .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn execute_bucket_retirement(
        &self,
        validated: &ValidatedBucketCatalogRetirement,
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        self.verify_bucket_retirement_sources_and_progress_with_plan(
            &validated.authority,
            &validated.plan,
            CatalogRetirementProgressRequirement::Recoverable,
            fence,
        )
        .await?;
        self.execute_bucket_retirement_after_validation(validated, execution, fence)
            .await
    }
}

fn bucket_retirement_delete_rank(kind: VectorCatalogRetirementTargetKind) -> u8 {
    use VectorCatalogRetirementTargetKind::*;
    match kind {
        CommitIdempotency | MaintenanceSource | MaintenanceResult | RetirementEvidence | RetirementPlan | RetirementProgress => 0,
        CommitLog | MaintenanceJob => 1,
        RetirementAuthorization | CreateIntent => 2,
        DeleteIntent => 3,
        TableEntry => 4,
        NamespaceEntry => 5,
        BucketEntry => 6,
    }
}
