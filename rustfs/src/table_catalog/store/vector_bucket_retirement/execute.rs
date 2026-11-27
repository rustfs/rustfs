// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) enum CatalogRetirementProgressRequirement {
    Prepared,
    Recoverable,
    Complete,
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn read_bucket_retirement_progress(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        planned: &CatalogRetirementPlannedTarget,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(CatalogMetadataRef, VectorBucketCatalogRetirementProgressPageV1)> {
        let page: VectorBucketCatalogRetirementPlanPageV1 = self.read_bucket_retirement_record(&planned.plan_page, fence).await?;
        if page.bucket != authority.bucket
            || page.retirement_id != authority.retirement_id
            || page.targets.get(usize::from(planned.ordinal)) != Some(&planned.target)
        {
            return Err(maintenance_error("Catalog target differs from its original plan page"));
        }
        let path = self.paths.vector_bucket_retirement_page_path(
            &authority.bucket.to_handle()?,
            authority.retirement_id,
            "progress",
            page.page_id,
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
            || progress.plan_page != planned.plan_page
            || progress.targets.len() != page.targets.len()
        {
            return Err(maintenance_error("Catalog progress does not bind the complete original plan page"));
        }
        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
        if reference.etag().is_none_or(str::is_empty) {
            return Err(maintenance_error("Catalog progress has no exact native ETag"));
        }
        fence.ensure_held()?;
        Ok((reference, progress))
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

    pub(super) async fn verify_bucket_retirement_sources_and_progress(
        &self,
        authority: &VectorBucketCatalogRetirementV1,
        requirement: CatalogRetirementProgressRequirement,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let bucket = authority.bucket.to_handle()?;
        self.require_bucket_retirement_identity(&bucket)?;
        self.backend.revalidate_bucket_handle(&bucket).await?;
        let mut cursor = None;
        for _ in 0..authority.target_count {
            let planned = self
                .next_bucket_retirement_target(authority, cursor.as_deref(), fence)
                .await?
                .ok_or_else(|| maintenance_error("Catalog target inventory ended during progress verification"))?;
            let (_, progress) = self.read_bucket_retirement_progress(authority, &planned, fence).await?;
            let state = progress.targets[usize::from(planned.ordinal)].state;
            match state {
                VectorCatalogRetirementTargetState::Pending => {
                    if matches!(requirement, CatalogRetirementProgressRequirement::Complete) {
                        return Err(TableCatalogStoreError::Unavailable("Catalog retirement still has pending targets".into()));
                    }
                    self.require_bucket_retirement_exact_source(&planned.target, fence).await?;
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
            cursor = Some(planned.target.object_key);
        }
        // Reconcile the current source inventory against the retained exact plan.
        // Completed targets may be absent; new or reappearing sources block cleanup.
        let mut cursor = None;
        while let Some((key, object)) = self.next_bucket_catalog_source(&bucket, cursor.as_deref(), fence).await? {
            let planned = self
                .bucket_retirement_target_by_key(authority, &key, fence)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("Catalog source is outside the retirement inventory".into()))?;
            let (_, progress) = self.read_bucket_retirement_progress(authority, &planned, fence).await?;
            if progress.targets[usize::from(planned.ordinal)].state != VectorCatalogRetirementTargetState::Pending {
                return Err(TableCatalogStoreError::Conflict(
                    "acknowledged Catalog retirement source is present".into(),
                ));
            }
            let expected = catalog_retirement_target_reference(&planned.target);
            let observed = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone())?, &object)?;
            if CatalogMetadataRefRecordV1::from(&observed) != expected {
                return Err(TableCatalogStoreError::Conflict("Catalog inventory native identity changed".into()));
            }
            cursor = Some(key);
        }
        self.backend.revalidate_bucket_handle(&bucket).await?;
        fence.ensure_held()
    }

    async fn execute_bucket_retirement_target(
        &self,
        validated: &ValidatedBucketCatalogRetirement,
        planned: &CatalogRetirementPlannedTarget,
        execution: &CatalogCleanupExecution,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let authority = &validated.authority;
        let (reference, mut progress) = self.read_bucket_retirement_progress(authority, planned, fence).await?;
        let ordinal = usize::from(planned.ordinal);
        match progress.targets[ordinal].state {
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
        published?;
        #[cfg(test)]
        after_progress?;
        fence.ensure_held()
    }

    pub(super) async fn execute_bucket_retirement(
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
        self.verify_bucket_retirement_sources_and_progress(authority, CatalogRetirementProgressRequirement::Recoverable, fence)
            .await?;
        for rank in 0..=6 {
            let mut cursor = None;
            while let Some(planned) = self
                .next_bucket_retirement_target(authority, cursor.as_deref(), fence)
                .await?
            {
                if bucket_retirement_delete_rank(planned.target.kind) == rank {
                    self.execute_bucket_retirement_target(validated, &planned, execution, fence)
                        .await?;
                }
                cursor = Some(planned.target.object_key);
            }
        }
        self.verify_bucket_retirement_sources_and_progress(authority, CatalogRetirementProgressRequirement::Complete, fence)
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
