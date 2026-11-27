// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Read-only evidence for an exact logically deleted resource.

use super::*;

#[derive(Clone)]
pub(super) struct InspectionCursor {
    resource: VectorMaintenanceResourceV1,
    snapshot: VectorMaintenanceResourceSnapshot,
    jobs: VectorMaintenanceJobCursor,
}

#[derive(Debug, Serialize)]
pub(crate) struct DeletedResourceInspection {
    instance_id: Uuid,
    execution_identity: VectorMaintenanceExecutionIdentityV1,
    resource: VectorMaintenanceResourceV1,
    observed_at: String,
    state: &'static str,
    current: VectorMaintenanceHeadV1,
    observed_object: CatalogMetadataRefRecordV1,
    source_intent: CatalogMetadataRefRecordV1,
    history: Vec<CommittedVersionObservation>,
    jobs: Vec<MaintenanceJobObservation>,
    next_cursor: Option<String>,
}

impl MaintenanceDriver {
    /// This reads retained evidence; it neither admits maintenance work nor declares GC complete.
    pub(crate) async fn inspect_deleted_resource(
        &self,
        resource: &VectorMaintenanceResourceV1,
        cursor: Option<&str>,
    ) -> Result<DeletedResourceInspection, VectorMaintenanceExecutionError> {
        resource.validate()?;
        let identity = resource.identity()?;
        let mut cursors = self.deleted_inspection_cursors.lock().await;
        let previous = cursor
            .map(|token| {
                cursors
                    .get(token)
                    .cloned()
                    .ok_or_else(|| invalid("unknown deleted-resource cursor"))
            })
            .transpose()?;
        if previous.as_ref().is_some_and(|previous| previous.resource != *resource) {
            return Err(conflict("deleted-resource cursor belongs to another identity"));
        }
        if previous.is_none() && cursors.len() >= MAX_INSPECTION_CURSORS {
            return Err(conflict("deleted-resource cursor capacity reached"));
        }
        self.authorize_inspection(resource).await?;
        let _permit = self.inspection_budgets.try_admit(&identity, VectorMaintenanceLane::Control)?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        let _memory = control.reserve_memory(64 * 1024 * 1024)?;
        let _io = control.acquire_io().await?;
        control.check_admission()?;
        let namespace = Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Catalog::for_vector_scope(self.source.backend.clone(), resource.bucket().to_handle()?).await?;
        let snapshot = catalog.read_vector_maintenance_resource(&identity, &namespace, &name).await?;
        if snapshot.state() != VectorMaintenanceResourceState::Deleted {
            return Err(conflict("deleted-resource observation requires logical deletion"));
        }
        if previous.as_ref().is_some_and(|previous| previous.snapshot != snapshot) {
            return Err(conflict("deleted-resource evidence changed between pages"));
        }
        let guard = catalog.begin_vector_maintenance_resource_operation(&snapshot).await?;
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        let current = snapshot
            .head()
            .cloned()
            .ok_or_else(|| invalid("deleted resource has no original head"))?;
        let source_intent = snapshot
            .source_intent()
            .ok_or_else(|| invalid("deleted resource has no source intent"))?;
        let history = catalog.read_vector_maintenance_history_with_guard(&guard).await?;
        let budget = VectorMaintenanceDiscoveryBudget::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.pass_admission_budget,
            self.config.discovery_calls_per_pass,
        );
        let (jobs, next) = catalog
            .list_vector_maintenance_jobs(
                &identity,
                previous.as_ref().map(|previous| &previous.jobs),
                NonZeroU16::new(8).expect("positive job observation page size"),
                &budget,
            )
            .await?
            .into_parts();
        let mut observed_jobs = Vec::with_capacity(jobs.len());
        for job in &jobs {
            control.check_admission()?;
            if !budget.try_start_call() {
                return Err(TableCatalogStoreError::Unavailable("deleted-resource evidence budget exhausted".into()).into());
            }
            let evidence = catalog.inspect_vector_maintenance_evidence_with_guard(&guard, job).await?;
            if evidence.resource_object != CatalogMetadataRefRecordV1::from(snapshot.observed_object()) {
                return Err(conflict("deleted-resource job evidence changed identity"));
            }
            observed_jobs.push(MaintenanceJobObservation {
                observed_object: job.observed_object().into(),
                job: job.job().clone(),
                result_content_inspected: evidence.result.is_some(),
                evidence,
            });
        }
        if catalog.read_vector_maintenance_resource_with_guard(&guard).await? != snapshot {
            return Err(conflict("deleted-resource evidence changed during observation"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        control.check_admission()?;
        let next_cursor = next.map(|jobs| {
            let token = Uuid::new_v4().to_string();
            cursors.insert(
                token.clone(),
                InspectionCursor {
                    resource: resource.clone(),
                    snapshot: snapshot.clone(),
                    jobs,
                },
            );
            token
        });
        if let Some(cursor) = cursor {
            cursors.remove(cursor);
        }
        Ok(DeletedResourceInspection {
            instance_id: self.app.object_store().id,
            execution_identity: self.source.identity.clone(),
            resource: resource.clone(),
            observed_at: OffsetDateTime::now_utc().to_string(),
            state: "DELETED",
            current,
            observed_object: snapshot.observed_object().into(),
            source_intent: source_intent.into(),
            history: history
                .iter()
                .map(|version| CommittedVersionObservation {
                    metadata: version.metadata().into(),
                    descriptor: version.format_descriptor().into(),
                    commit_id: version.commit_id().map(|id| id.as_str().into()),
                })
                .collect(),
            jobs: observed_jobs,
            next_cursor,
        })
    }
}
