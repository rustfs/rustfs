// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Exact Catalog input preparation performed before a persistent job is admitted.

use super::*;
use crate::app::object::ManagedMaintenancePreparationPurpose;
use crate::table_catalog::*;
use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroUsize};

impl MaintenanceExecutor {
    pub(super) async fn prepare_action(
        &self,
        catalog: Arc<Catalog>,
        action: VectorMaintenancePreparationAction,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        control.check_admission()?;
        match action {
            VectorMaintenancePreparationAction::InspectRetirementRecovery {
                resource,
                job_id,
                expected_revision,
            } => {
                self.inspect_retirement_recovery(catalog, resource, job_id, expected_revision, control)
                    .await
            }
            VectorMaintenancePreparationAction::EnrollCurrent { expected, commit_id } => {
                let guard = catalog.begin_vector_maintenance_enrollment(&expected).await?;
                let (bound, adapter) = self
                    .preparation_adapter(
                        catalog,
                        guard.clone(),
                        uuid::Uuid::new_v4(),
                        ManagedMaintenancePreparationPurpose::Enroll,
                        control,
                    )
                    .await?;
                bound
                    .enroll_vector_maintenance_with_guard(&expected, commit_id, &guard)
                    .await?;
                drop(adapter);
                Ok(())
            }
            VectorMaintenancePreparationAction::EnrollDeleted {
                resource,
                expected_intent,
                update_id,
            } => {
                catalog
                    .enroll_deleted_vector_maintenance(&resource, &expected_intent, update_id)
                    .await?;
                Ok(())
            }
            VectorMaintenancePreparationAction::PrepareRecovery {
                resource,
                kind,
                expected_source,
            } => {
                let action = match kind {
                    VectorMaintenanceKind::CreateRecovery => S3VectorsAction::CreateIndexAction,
                    VectorMaintenanceKind::DeleteRecovery => S3VectorsAction::DeleteVectorsAction,
                    VectorMaintenanceKind::CommitRecovery => S3VectorsAction::PutVectorsAction,
                    _ => return Err(TableCatalogStoreError::Invalid("invalid recovery preparation kind".into()).into()),
                };
                self.source
                    .authorize_actions(&self.source.identity, &resource, &[action])
                    .await?;
                let identity = resource.identity()?;
                let snapshot = catalog
                    .read_vector_maintenance_resource(
                        &identity,
                        &Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?,
                        &CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?,
                    )
                    .await?;
                let source = catalog
                    .copy_vector_maintenance_source(&identity, &CatalogMetadataRef::try_from(expected_source.reference())?)
                    .await?;
                let input = VectorMaintenanceInputV1::try_new(
                    snapshot.head().cloned(),
                    Some(VectorMaintenanceSourceV1::try_new(*expected_source.kind(), (&source).into())?),
                    None,
                )?;
                self.create_job(&catalog, resource, kind, VectorMaintenanceOrigin::Recovery, input)
                    .await
            }
            VectorMaintenancePreparationAction::PrepareGc {
                resource,
                kind,
                expected_head,
            } => self.prepare_gc(catalog, resource, kind, *expected_head, control).await,
        }
    }

    async fn create_job(
        &self,
        catalog: &Catalog,
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        origin: VectorMaintenanceOrigin,
        input: VectorMaintenanceInputV1,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        let semantic = VectorMaintenanceSemanticSpec::try_new(
            resource,
            kind,
            origin,
            input,
            self.source.identity.clone(),
            None,
            self.source.policy.clone(),
        )?;
        catalog
            .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(semantic)?)
            .await?;
        Ok(())
    }

    async fn prepare_gc(
        &self,
        catalog: Arc<Catalog>,
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        expected_head: VectorMaintenanceHeadV1,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        if !matches!(kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc) {
            return Err(TableCatalogStoreError::Invalid("invalid retirement preparation kind".into()).into());
        }
        let continuation = self.source.gc_scan.lock().clone();
        if continuation
            .as_ref()
            .is_some_and(|scan| scan.resource != resource || scan.kind != kind)
        {
            return Ok(());
        }
        self.source
            .authorize_actions(
                &self.source.identity,
                &resource,
                &[S3VectorsAction::GetVectorsAction, S3VectorsAction::DeleteVectorsAction],
            )
            .await?;
        let identity = resource.identity()?;
        let snapshot = catalog
            .read_vector_maintenance_resource(
                &identity,
                &Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?,
                &CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?,
            )
            .await?;
        if snapshot.head() != Some(&expected_head) {
            return Err(TableCatalogStoreError::Conflict("GC input changed before preparation".into()).into());
        }
        // The default retention policy protects every committed Active Index version.
        if kind == VectorMaintenanceKind::HistoryGc && snapshot.state() == VectorMaintenanceResourceState::Active {
            return Ok(());
        }
        let guard = catalog.begin_vector_maintenance_resource_operation(&snapshot).await?;
        let (bound, adapter) = self
            .preparation_adapter(
                catalog.clone(),
                guard.clone(),
                uuid::Uuid::new_v4(),
                ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
                control.clone(),
            )
            .await?;
        // Account for both exact references and map keys while constructing the bounded proof.
        let _proof_memory = control.reserve_memory(64 * 1024 * 1024)?;
        let mut history_refs = BTreeMap::new();
        let mut unresolved_refs = BTreeMap::new();
        let retained_evidence = if snapshot.state() == VectorMaintenanceResourceState::Deleted {
            bound.read_vector_deleted_retirement_evidence_with_guard(&guard).await?
        } else {
            None
        };
        if let Some(evidence) = retained_evidence {
            if evidence.retention() != &VectorHistoryRetentionV1::All {
                return Err(TableCatalogStoreError::Conflict("retirement retention policy changed".into()).into());
            }
            // Earlier authorized batches may have removed these immutable files. The Catalog
            // revalidates their complete persisted proof against the same deletion transaction.
            for reference in evidence.history_refs() {
                let reference = CatalogMetadataRef::try_from(reference)?;
                history_refs.insert(reference.object_key().as_str().to_owned(), reference);
            }
            for reference in evidence.unresolved_refs() {
                let reference = CatalogMetadataRef::try_from(reference)?;
                unresolved_refs.insert(reference.object_key().as_str().to_owned(), reference);
            }
        } else {
            let history = bound.read_vector_maintenance_history_with_guard(&guard).await?;
            let candidates = bound.read_vector_maintenance_candidate_roots_with_guard(&guard).await?;
            if let Some(version) = history.first() {
                for reference in adapter
                    .collect_references(version.metadata(), version.format_descriptor())
                    .await
                    .map_err(maintenance_execution_error)?
                {
                    history_refs.insert(reference.object_key().as_str().to_owned(), reference);
                }
            }
            for version in &candidates {
                for reference in adapter
                    .collect_candidate_references(version.metadata(), version.format_descriptor())
                    .await
                    .map_err(maintenance_execution_error)?
                {
                    unresolved_refs.insert(reference.object_key().as_str().to_owned(), reference);
                }
            }
        }
        let protected_count = if snapshot.state() == VectorMaintenanceResourceState::Active {
            history_refs.len()
        } else {
            0
        };
        if history_refs
            .len()
            .saturating_add(unresolved_refs.len())
            .saturating_add(protected_count)
            > VECTOR_RETIREMENT_MAX_REFERENCES
        {
            return Err(
                TableCatalogStoreError::Unavailable("CapacityExceeded: retirement proof exceeds reference limit".into()).into(),
            );
        }
        let protected_refs = if snapshot.state() == VectorMaintenanceResourceState::Active {
            history_refs.values().map(CatalogMetadataRefRecordV1::from).collect()
        } else {
            Vec::new()
        };
        let budget = VectorMaintenanceDiscoveryBudget::new(
            control.stop_new_work.clone(),
            control
                .admission_deadline
                .min(tokio::time::Instant::now() + std::time::Duration::from_secs(30)),
            NonZeroUsize::new(64).ok_or_else(|| TableCatalogStoreError::Invalid("invalid discovery budget".into()))?,
        );
        let start_after = continuation
            .as_ref()
            .filter(|scan| scan.head == expected_head)
            .map(|scan| scan.next_key.as_str());
        let page = bound
            .list_vector_maintenance_object_candidates(
                &guard,
                start_after,
                NonZeroU16::new(32).ok_or_else(|| TableCatalogStoreError::Invalid("invalid discovery limit".into()))?,
                &budget,
            )
            .await?;
        let (objects, next_key) = page.into_parts();
        let mut targets = Vec::new();
        for (key, metadata) in objects {
            control.check_admission()?;
            if unresolved_refs.contains_key(key.as_str())
                || (snapshot.state() == VectorMaintenanceResourceState::Active && history_refs.contains_key(key.as_str()))
            {
                continue;
            }
            if kind == VectorMaintenanceKind::HistoryGc && !history_refs.contains_key(key.as_str()) {
                continue;
            }
            if kind == VectorMaintenanceKind::OrphanGc && history_refs.contains_key(key.as_str()) {
                continue;
            }
            if metadata.mod_time.is_none() || metadata.body_revision.is_none() {
                continue;
            }
            let target = VectorRetirementTargetV1::from_object_metadata(&identity, key, &metadata)?;
            if vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, OffsetDateTime::now_utc())? {
                targets.push(target);
            }
        }
        if targets.is_empty() {
            self.save_gc_continuation(resource, kind, expected_head, next_key, &control)?;
            return Ok(());
        }
        let source = match snapshot.source_intent() {
            Some(reference) => Some(
                bound
                    .copy_vector_maintenance_source_with_operation_guard(&guard, reference)
                    .await?,
            ),
            None => None,
        };
        let evidence = VectorRetirementEvidenceV1::try_new(
            resource.clone(),
            expected_head.clone(),
            source.as_ref().map(Into::into),
            history_refs.values().map(Into::into).collect(),
            protected_refs,
            unresolved_refs.values().map(Into::into).collect(),
            VectorHistoryRetentionV1::All,
        )?;
        let prepared = bound.prepare_vector_retirement(&guard, evidence, targets).await?;
        let plan = prepared.plan_ref().clone();
        drop(prepared);
        drop(adapter);
        drop(guard);
        drop(bound);
        self.create_job(
            &catalog,
            resource.clone(),
            kind,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(Some(expected_head.clone()), None, Some((&plan).into()))?,
        )
        .await?;
        self.save_gc_continuation(resource, kind, expected_head, next_key, &control)
    }

    fn save_gc_continuation(
        &self,
        resource: VectorMaintenanceResourceV1,
        kind: VectorMaintenanceKind,
        head: VectorMaintenanceHeadV1,
        next_key: Option<String>,
        control: &VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        let next = match next_key {
            Some(next_key) => {
                let size = serde_json::to_vec(&(&resource, kind, &head, &next_key))
                    .map_err(maintenance_execution_error)?
                    .len();
                let bytes = u64::try_from(
                    size.saturating_mul(2)
                        .saturating_add(std::mem::size_of::<source::GcContinuation>()),
                )
                .map_err(maintenance_execution_error)?;
                Some(Arc::new(source::GcContinuation {
                    resource,
                    kind,
                    head,
                    next_key,
                    _memory: control.reserve_memory(bytes)?,
                }))
            }
            None => None,
        };
        *self.source.gc_scan.lock() = next;
        Ok(())
    }
}
