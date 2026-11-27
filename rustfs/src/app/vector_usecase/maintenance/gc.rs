// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Exact retirement execution and native recovery under the current application identity.

use super::*;
use crate::app::object::{
    DefaultObjectUsecase, ManagedDeleteOutcome, ManagedMaintenanceAdmission, ManagedMaintenanceAuthorizer,
    ManagedMaintenancePreparationPurpose, ManagedObjectError, ManagedRetiredObject, ManagedRetirementDeletePermit,
    ManagedRetirementDeletePermitParts,
};
use crate::app::storage_api::managed::HealOpts;
use crate::app::storage_api::object_usecase::{ECStore, StorageObjectOptions};
use crate::app::storage_api::vector_usecase::{OperationHelper, StorageError};
use crate::table_catalog::*;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct CompletionResources {
    _slot: OwnedSemaphorePermit,
    _memory: Arc<VectorMaintenanceMemoryReservation>,
}

#[derive(Clone)]
struct CompletedDelete {
    completion: VectorMaintenanceOwnedCompletion,
    resources: Arc<CompletionResources>,
}

/// Only this process's completed, continuously protected operations enter this bounded map.
#[derive(Clone)]
pub(super) struct MaintenanceGcState {
    completed: Arc<Mutex<HashMap<Uuid, CompletedDelete>>>,
    completed_attempts: Arc<Mutex<HashMap<Uuid, CompletedDelete>>>,
    slots: Arc<Semaphore>,
}

impl Default for MaintenanceGcState {
    fn default() -> Self {
        Self {
            completed: Arc::new(Mutex::new(HashMap::new())),
            completed_attempts: Arc::new(Mutex::new(HashMap::new())),
            slots: Arc::new(Semaphore::new(VECTOR_RETIREMENT_MAX_TARGETS)),
        }
    }
}

impl MaintenanceGcState {
    fn forget_terminal(&self, progress: &VectorRetirementProgressV1) {
        let mut completed = self.completed.lock();
        for target in progress.targets() {
            if matches!(
                target.state(),
                VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
            ) && let Some(dispatch) = target.dispatch()
            {
                completed.remove(dispatch.dispatch_id());
            }
        }
    }

    fn reserve(
        &self,
        work: &VectorMaintenanceWork,
        control: &VectorMaintenanceControl,
    ) -> Result<Arc<CompletionResources>, VectorMaintenanceExecutionError> {
        self.reserve_with_native_tracking(work, control, false)
    }

    fn reserve_with_native_tracking(
        &self,
        work: &VectorMaintenanceWork,
        control: &VectorMaintenanceControl,
        native_tracking: bool,
    ) -> Result<Arc<CompletionResources>, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let resource = serde_json::to_vec(work.snapshot().job().resource()).map_err(maintenance_execution_error)?;
        let bytes = resource
            .len()
            .saturating_mul(2)
            .saturating_add(work.lease().execution_identity().account().len())
            .saturating_add(std::mem::size_of::<CompletedDelete>())
            .saturating_add(std::mem::size_of::<VectorMaintenanceOwnedCompletion>())
            .saturating_add(256)
            .saturating_add(if native_tracking {
                VECTOR_RETIREMENT_MAX_TARGETS.saturating_mul(128)
            } else {
                0
            });
        self.reserve_bytes(control, u64::try_from(bytes).map_err(maintenance_execution_error)?)
    }

    fn reserve_bytes(
        &self,
        control: &VectorMaintenanceControl,
        bytes: u64,
    ) -> Result<Arc<CompletionResources>, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| VectorMaintenanceBudgetError::Unavailable)?;
        let memory = control.reserve_memory(bytes)?;
        Ok(Arc::new(CompletionResources {
            _slot: slot,
            _memory: memory,
        }))
    }
}

fn unresolved() -> VectorMaintenanceExecutionOutcome {
    VectorMaintenanceExecutionOutcome::Unknown {
        reason: VectorMaintenanceFailureCode::UnknownOutcome,
    }
}

fn conflict(message: &str) -> VectorMaintenanceExecutionError {
    TableCatalogStoreError::Conflict(message.into()).into()
}

fn audit_for(work: &VectorMaintenanceWork, identity: &ManagedMaintenanceIdentityRef) -> ManagedMaintenanceAuditContext {
    ManagedMaintenanceAuditContext::new(
        identity.clone(),
        *work.snapshot().job().job_id(),
        u32::from(work.lease().attempt()),
        work.lease().worker_id().to_owned(),
        work.lease().worker_boot_id(),
        work.snapshot().job().initiator_ref().clone(),
    )
}

fn access_for(resource: &CatalogResourceIdentity, key: &str) -> Result<ManagedObjectAccess, VectorMaintenanceExecutionError> {
    let bucket = resource
        .vector_bucket()
        .ok_or_else(|| conflict("retirement requires a Vector bucket"))?;
    ManagedObjectAccess::try_from_parts(
        bucket.clone(),
        resource.table_id().clone(),
        vector_retirement_object_prefix(resource, key)?,
        BTreeSet::from([ManagedObjectOperation::Delete]),
    )
    .map_err(maintenance_execution_error)
}

/// The admission owns the actual principal; it contains no Catalog or lifecycle guard.
struct NativeAdmission {
    _admission: ManagedMaintenanceAdmission,
    helper: OperationHelper,
}

impl NativeAdmission {
    fn complete(self, success: bool) {
        let result: rustfs_s3_types::S3Result<()> = if success {
            Ok(())
        } else {
            Err(rustfs_s3_types::s3_error!(InternalError, "retirement operation remains unresolved"))
        };
        let _ = self
            .helper
            .complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
    }
}

impl MaintenanceExecutor {
    pub(super) async fn inspect_retirement_recovery(
        &self,
        catalog: Arc<Catalog>,
        resource: VectorMaintenanceResourceV1,
        job_id: Uuid,
        expected_revision: Uuid,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        control.check_admission()?;
        if control.dispatch_guard().is_ok() {
            return Err(conflict("retirement inspection requires an unguarded preparation phase"));
        }
        self.source
            .authorize_actions(&self.source.identity, &resource, &[S3VectorsAction::DeleteVectorsAction])
            .await?;
        let identity = resource.identity()?;
        let inspection = catalog
            .inspect_vector_retirement_job_recovery(&identity, job_id, expected_revision, &self.source.identity)
            .await?;
        if inspection.job().job().resource() != &resource || inspection.resource().resource() != &identity {
            return Err(conflict("retirement inspection resource changed"));
        }
        let bytes = serde_json::to_vec(inspection.progress())
            .map_err(maintenance_execution_error)?
            .len()
            .saturating_mul(2)
            .saturating_add(
                inspection
                    .requests()
                    .len()
                    .saturating_mul(std::mem::size_of::<VectorRetirementRecoveryRequest>()),
            );
        let _memory = control.reserve_memory(u64::try_from(bytes).map_err(maintenance_execution_error)?)?;
        self.gc_state.forget_terminal(inspection.progress());
        let terminal =
            inspection.requests().is_empty() && *inspection.progress().state() == VectorRetirementProgressState::Completed;
        let original_completion = if terminal {
            None
        } else {
            let mut completions = {
                let completed = self.gc_state.completed.lock();
                inspection
                    .progress()
                    .targets()
                    .iter()
                    .filter_map(|target| target.dispatch().as_ref())
                    .filter(|dispatch| {
                        dispatch.job_id() == inspection.job().job().job_id()
                            && dispatch.attempt() == inspection.job().job().attempt()
                    })
                    .filter_map(|dispatch| completed.get(dispatch.dispatch_id()).map(|entry| entry.completion.clone()))
                    .collect::<Vec<_>>()
            };
            if let Some(lease) = inspection.job().job().lease()
                && let Some(attempt) = self.gc_state.completed_attempts.lock().get(lease.lease_id())
            {
                completions.push(attempt.completion.clone());
            }
            Some(inspection.merge_owned_completions(&completions)?)
        };
        let mut evidence = Vec::with_capacity(inspection.requests().len());
        for request in inspection.requests() {
            control.check_admission()?;
            let previous = request
                .prior_dispatch()
                .as_ref()
                .and_then(|dispatch| self.gc_state.completed.lock().get(dispatch.dispatch_id()).cloned());
            if request.prior_dispatch().is_some() && previous.is_none() {
                return Err(conflict("prior native owner completion is unknown"));
            }
            let admitted = self
                .authorize_inspection_target(&resource, request, job_id, expected_revision)
                .await?;
            let io = control.acquire_io().await?;
            let bucket = identity
                .vector_bucket()
                .ok_or_else(|| conflict("retirement requires a Vector bucket"))?;
            run_admitted_native_recovery(
                self.app.object_store(),
                bucket.storage_bucket_id().as_str().to_owned(),
                request.target().object_key().clone(),
                bucket.incarnation().as_uuid(),
                admitted,
                io,
                None,
            )
            .await?;
            control.check_admission()?;
            let admitted = self
                .authorize_inspection_target(&resource, request, job_id, expected_revision)
                .await?;
            let observed = observe_recovered_target(self.app.object_store(), request).await;
            admitted.complete(observed.is_ok());
            evidence.push(VectorRetirementReadmissionEvidence::try_from_native_recovery(
                request.clone(),
                [0, 0, 0],
                observed?,
                previous.map(|entry| entry.completion),
            )?);
        }
        control.check_admission()?;
        self.source
            .authorize_actions(&self.source.identity, &resource, &[S3VectorsAction::DeleteVectorsAction])
            .await?;
        let original_lease = inspection.job().job().lease().as_ref().map(|lease| *lease.lease_id());
        let resolved = catalog
            .resolve_vector_retirement_job_recovery(inspection, evidence, original_completion)
            .await?;
        if matches!(
            resolved.job().state(),
            VectorMaintenanceJobState::Succeeded | VectorMaintenanceJobState::RetryScheduled
        ) && let Some(lease_id) = original_lease
        {
            self.gc_state.completed_attempts.lock().remove(&lease_id);
        }
        Ok(())
    }

    async fn authorize_inspection_target(
        &self,
        resource: &VectorMaintenanceResourceV1,
        request: &VectorRetirementRecoveryRequest,
        job_id: Uuid,
        expected_revision: Uuid,
    ) -> Result<NativeAdmission, VectorMaintenanceExecutionError> {
        let access = access_for(request.resource(), request.target().object_key())?;
        access
            .check(request.target().object_key(), ManagedObjectOperation::Delete)
            .map_err(maintenance_execution_error)?;
        let audit = ManagedMaintenanceAuditContext::new_preparation(
            self.identity.clone(),
            Uuid::new_v4(),
            ManagedMaintenancePreparationPurpose::RecoveryPlanning,
            "vector-maintenance-preparation".into(),
            self.worker_boot_id,
            None,
        );
        let envelope = audit.envelope(&access, request.target().object_key());
        let helper = OperationHelper::audit_only_named_from_view(&envelope.audit_view(), "ManagedMaintenanceRecovery")
            .with_audit_tags(audit.tags())
            .with_audit_tags(HashMap::from([
                ("maintenanceRecoveryJobId".to_owned(), job_id.to_string().into()),
                ("maintenanceRecoveryJobRevision".to_owned(), expected_revision.to_string().into()),
            ]));
        let authorized = async {
            self.source
                .authorize_actions(&self.source.identity, resource, &[S3VectorsAction::DeleteVectorsAction])
                .await?;
            let (identity, credential) = resolve_identity(&self.app, self.identity.account())
                .await
                .map_err(maintenance_execution_error)?;
            if identity != self.identity {
                return Err(conflict("maintenance identity changed during recovery authorization"));
            }
            let mut operation = rustfs_kms::types::OperationContext::new(credential.access_key.clone());
            operation.operation_id = audit.operation_id();
            operation.user_agent = Some("RustFS Vector maintenance".into());
            operation.additional_context.insert(
                "preparation_id".into(),
                audit
                    .preparation_id()
                    .ok_or_else(|| conflict("recovery preparation identity missing"))?
                    .to_string(),
            );
            operation
                .additional_context
                .insert("worker_boot_id".into(), self.worker_boot_id.to_string());
            let runtime = Arc::new(ManagedSseRuntime::new(self.app.kms_runtime().service_manager()));
            let principal = SseKmsPrincipal::for_managed_maintenance(
                &credential,
                identity_conditions(&self.app, &credential).map_err(maintenance_execution_error)?,
                &audit.operation_id().to_string(),
                Arc::new(MaintenanceKmsAuthorizer {
                    app: self.app.clone(),
                    identity,
                }),
                Arc::new(runtime.begin_operation(operation).await),
            );
            ManagedMaintenanceAdmission::new(principal, audit).map_err(maintenance_execution_error)
        }
        .await;
        match authorized {
            Ok(admission) => Ok(NativeAdmission {
                _admission: admission,
                helper,
            }),
            Err(error) => {
                let result: rustfs_s3_types::S3Result<()> =
                    Err(rustfs_s3_types::s3_error!(AccessDenied, "retirement recovery authorization failed"));
                let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
                Err(error)
            }
        }
    }

    async fn authorize_gc_key(
        &self,
        catalog: Arc<Catalog>,
        guard: Arc<VectorMaintenanceDispatchGuard>,
        work: &VectorMaintenanceWork,
        key: &str,
    ) -> Result<NativeAdmission, VectorMaintenanceExecutionError> {
        let access = access_for(guard.lease().resource(), key)?;
        let snapshot = catalog.read_vector_maintenance_authorization_with_guard(&guard).await?;
        let encryption = managed_encryption(&snapshot.definition().effective_encryption);
        let audit = audit_for(work, &self.identity);
        let envelope = audit.envelope(&access, key);
        let helper = OperationHelper::audit_only_named_from_view(&envelope.audit_view(), "ManagedMaintenanceRecovery")
            .with_audit_tags(audit.tags());
        let authorizer = MaintenanceObjectAuthorizer::new(self.app.clone(), catalog, guard, self.identity.clone());
        match authorizer
            .authorize(&access, ManagedObjectOperation::Delete, key, &encryption, &audit)
            .await
        {
            Ok(admission) => Ok(NativeAdmission {
                _admission: admission,
                helper,
            }),
            Err(error) => {
                let code = match &error {
                    ManagedObjectError::Api(error) => error.code.clone(),
                    _ => rustfs_s3_types::S3ErrorCode::AccessDenied,
                };
                let result: rustfs_s3_types::S3Result<()> = Err(rustfs_s3_types::S3Error::with_message(code, error.to_string()));
                let _ = helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new());
                Err(maintenance_execution_error(error))
            }
        }
    }

    async fn publish_gc_retirement(
        &self,
        catalog: Arc<Catalog>,
        guard: Arc<VectorMaintenanceDispatchGuard>,
        work: &VectorMaintenanceWork,
        control: &VectorMaintenanceControl,
        prepared: PreparedVectorRetirement,
    ) -> Result<CatalogMetadataRef, VectorMaintenanceExecutionError> {
        let first = prepared
            .targets()
            .first()
            .ok_or_else(|| conflict("retirement plan has no targets"))?;
        let admission = self
            .authorize_gc_key(catalog.clone(), guard.clone(), work, first.object_key())
            .await?;
        let result = async {
            let snapshot = catalog.read_vector_maintenance_authorization_with_guard(&guard).await?;
            let resource = guard.lease().resource();
            let access = ManagedObjectAccess::try_from_parts(
                resource
                    .vector_bucket()
                    .ok_or_else(|| conflict("retirement requires a Vector bucket"))?
                    .clone(),
                resource.table_id().clone(),
                resource.dataset_prefix(),
                BTreeSet::from([
                    ManagedObjectOperation::Get,
                    ManagedObjectOperation::Head,
                    ManagedObjectOperation::Range,
                ]),
            )
            .map_err(maintenance_execution_error)?;
            let authorizer = Arc::new(MaintenanceObjectAuthorizer::new(
                self.app.clone(),
                catalog.clone(),
                guard.clone(),
                self.identity.clone(),
            ));
            let context = ManagedObjectContext::from_maintenance(
                access,
                managed_encryption(&snapshot.definition().effective_encryption),
                Arc::new(ManagedMaintenanceContext::new(audit_for(work, &self.identity), authorizer)),
            )
            .map_err(maintenance_execution_error)?;
            let reader = super::super::managed_objects::maintenance_dispatch_candidate_reader(
                self.app.clone(),
                context,
                guard.clone(),
                self.limits,
            )
            .map_err(maintenance_execution_error)?;
            let reader = Arc::new(super::super::dataset::CandidateReader::with_owned_maintenance_reader(
                control.clone().with_dispatch(guard.clone()),
                reader,
            ));
            let bound = catalog.with_vector_managed_candidate_reader(reader)?;
            bound
                .publish_vector_retirement(
                    &guard,
                    prepared,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    CatalogCommitId::try_new(Uuid::new_v4().to_string())?,
                )
                .await
                .map_err(Into::into)
        }
        .await;
        admission.complete(result.is_ok());
        result
    }

    pub(super) async fn execute_gc(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let guard = Arc::new(control.dispatch_guard()?.clone());
        self.source
            .authorize_actions_with_guard(&catalog, &guard, &[S3VectorsAction::DeleteVectorsAction])
            .await?;
        let authorization = match catalog.read_vector_retirement_authorization_with_guard(&guard).await? {
            Some(reference) => reference,
            None => {
                let plan = work
                    .snapshot()
                    .job()
                    .input()
                    .plan_ref()
                    .as_ref()
                    .ok_or_else(|| conflict("GC task has no exact plan"))?;
                let plan = CatalogMetadataRef::try_from(plan)?;
                let prepared = catalog.read_prepared_vector_retirement_with_guard(&guard, &plan).await?;
                self.publish_gc_retirement(catalog.clone(), guard.clone(), &work, &control, prepared)
                    .await?
            }
        };
        loop {
            control.check_admission()?;
            let progress = catalog.read_vector_retirement_progress(&guard, &authorization).await?;
            self.gc_state.forget_terminal(&progress);
            if *progress.state() == VectorRetirementProgressState::Completed {
                return completed_retirement(&catalog, &guard, &work, &authorization).await;
            }
            let target = progress
                .targets()
                .iter()
                .find(|target| {
                    !matches!(
                        target.state(),
                        VectorRetirementTargetState::DeleteCommitted | VectorRetirementTargetState::AbsentAfterRecovery
                    )
                })
                .ok_or_else(|| conflict("retirement progress state differs from targets"))?;
            if *target.state() != VectorRetirementTargetState::Pending {
                return Ok(unresolved());
            }
            let resources = self.gc_state.reserve(&work, &control)?;
            let admission = catalog
                .begin_vector_retirement_delete(&guard, &authorization, *target.ordinal(), *progress.revision(), Uuid::new_v4())
                .await?;
            match admission {
                VectorRetirementDispatchAdmission::Recorded => continue,
                VectorRetirementDispatchAdmission::NeedsRecovery => return Ok(unresolved()),
                VectorRetirementDispatchAdmission::Ready(permit) => {
                    if !self
                        .execute_retirement_permit(catalog.clone(), &work, &control, *permit, resources)
                        .await?
                    {
                        return Ok(unresolved());
                    }
                }
            }
        }
    }

    pub(super) async fn execute_delete_recovery(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let guard = control.dispatch_guard()?;
        let source = work
            .snapshot()
            .job()
            .input()
            .source()
            .as_ref()
            .ok_or_else(|| conflict("delete recovery source is missing"))?;
        match source.kind() {
            VectorMaintenanceSourceKind::DeleteIntent => {
                self.source
                    .authorize_actions_with_guard(&catalog, guard, &[S3VectorsAction::DeleteVectorsAction])
                    .await?;
                catalog.recover_vector_index_deletion_with_dispatch(guard).await?;
                let output = catalog.copy_vector_maintenance_completed_source_with_guard(guard).await?;
                Ok(VectorMaintenanceExecutionOutcome::Completed {
                    result: Box::new(VectorMaintenanceResultV1::try_for_work(
                        &work,
                        VectorMaintenanceResultEffect::Recovered,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        vec![(&output).into()],
                        None,
                    )?),
                })
            }
            VectorMaintenanceSourceKind::RetirementAuthorization => {
                let authorization = catalog
                    .read_vector_retirement_authorization_with_guard(guard)
                    .await?
                    .ok_or_else(|| conflict("retirement recovery has no published authorization"))?;
                let progress = catalog.read_vector_retirement_progress(guard, &authorization).await?;
                self.gc_state.forget_terminal(&progress);
                if *progress.state() != VectorRetirementProgressState::Completed {
                    return Ok(unresolved());
                }
                completed_retirement(&catalog, guard, &work, &authorization).await
            }
            _ => Err(conflict("delete recovery source kind is invalid")),
        }
    }

    pub(super) async fn recover_native_work(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        if *work.snapshot().job().kind() != VectorMaintenanceKind::DeleteRecovery {
            return Ok(());
        }
        let source = work
            .snapshot()
            .job()
            .input()
            .source()
            .as_ref()
            .ok_or_else(|| conflict("delete recovery source is missing"))?;
        if *source.kind() != VectorMaintenanceSourceKind::RetirementAuthorization {
            return Ok(());
        }
        if control.dispatch_guard().is_ok() {
            return Err(conflict("native recovery requires an unguarded worker phase"));
        }
        let resources = self.gc_state.reserve_with_native_tracking(&work, &control, true)?;
        let owner = VectorMaintenanceRecoveryOwner::new(work.lease());
        let result = self
            .recover_native_attempt(catalog.clone(), &work, &control, &owner, &resources)
            .await;
        if result.is_err()
            && let Ok(guard) = catalog.begin_vector_maintenance_dispatch(work.lease()).await
            && let Ok(completion) = owner.complete(&guard)
        {
            self.gc_state
                .completed_attempts
                .lock()
                .insert(work.lease().lease_id(), CompletedDelete { completion, resources });
        }
        result
    }

    async fn recover_native_attempt(
        &self,
        catalog: Arc<Catalog>,
        work: &VectorMaintenanceWork,
        control: &VectorMaintenanceControl,
        owner: &VectorMaintenanceRecoveryOwner,
        resources: &Arc<CompletionResources>,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        loop {
            control.check_admission()?;
            self.source
                .authorize_actions(
                    work.lease().execution_identity(),
                    work.snapshot().job().resource(),
                    &[S3VectorsAction::DeleteVectorsAction],
                )
                .await?;
            let (request, admitted, previous) = {
                let guard = Arc::new(catalog.begin_vector_maintenance_dispatch(work.lease()).await?);
                let requests = catalog.prepare_vector_retirement_recovery(&guard).await?;
                let Some(request) = requests.into_iter().next() else { return Ok(()) };
                let previous = request
                    .prior_dispatch()
                    .as_ref()
                    .and_then(|prior| self.gc_state.completed.lock().get(prior.dispatch_id()).cloned());
                if request.prior_dispatch().is_some() && previous.is_none() {
                    return Err(conflict("prior native owner completion is unknown"));
                }
                let admitted = self
                    .authorize_gc_key(catalog.clone(), guard.clone(), work, request.target().object_key())
                    .await?;
                guard.check_held()?;
                (request, admitted, previous)
            };
            // All authorizers and real application guards ended above. Admission retains only
            // the actual principal and audit owner while ECStore obtains its own lifecycle.
            let io = control.acquire_io().await?;
            let bucket = request
                .resource()
                .vector_bucket()
                .ok_or_else(|| conflict("retirement bucket is missing"))?;
            run_admitted_native_recovery(
                self.app.object_store(),
                bucket.storage_bucket_id().as_str().to_owned(),
                request.target().object_key().clone(),
                bucket.incarnation().as_uuid(),
                admitted,
                io,
                Some((owner.begin_native_operation(Uuid::new_v4())?, resources.clone())),
            )
            .await?;
            control.check_admission()?;
            let guard = Arc::new(catalog.begin_vector_maintenance_dispatch(work.lease()).await?);
            let admitted = self
                .authorize_gc_key(catalog.clone(), guard.clone(), work, request.target().object_key())
                .await?;
            let observed = self.recovered_target_metadata(&guard, &request).await;
            admitted.complete(observed.is_ok());
            let observed = observed?;
            let progress = catalog
                .read_vector_retirement_progress(&guard, request.authorization_ref())
                .await?;
            let evidence = VectorRetirementReadmissionEvidence::try_from_native_recovery(
                request.clone(),
                [0, 0, 0],
                observed,
                previous.as_ref().map(|value| value.completion.clone()),
            )?;
            let resources = match &previous {
                Some(previous) => previous.resources.clone(),
                None => self.gc_state.reserve(work, control)?,
            };
            let readmission = catalog
                .recover_vector_retirement_delete(&guard, *progress.revision(), Uuid::new_v4(), evidence, owner)
                .await?;
            let current_control = control.clone().with_dispatch(guard.clone());
            let complete = match readmission {
                VectorRetirementDispatchAdmission::Recorded => true,
                VectorRetirementDispatchAdmission::NeedsRecovery => false,
                VectorRetirementDispatchAdmission::Ready(permit) => {
                    self.execute_retirement_permit(catalog.clone(), work, &current_control, *permit, resources)
                        .await?
                }
            };
            if let Some(prior) = request.prior_dispatch() {
                self.gc_state.completed.lock().remove(prior.dispatch_id());
            }
            if !complete {
                return Err(conflict("retirement deletion remains unresolved"));
            }
        }
    }

    async fn recovered_target_metadata(
        &self,
        guard: &VectorMaintenanceDispatchGuard,
        request: &VectorRetirementRecoveryRequest,
    ) -> Result<Option<TableCatalogObjectMetadata>, VectorMaintenanceExecutionError> {
        if guard.lease().resource() != request.resource() {
            return Err(conflict("recovery resource changed"));
        }
        let bucket = request
            .resource()
            .vector_bucket()
            .ok_or_else(|| conflict("retirement bucket is missing"))?;
        let mut options = StorageObjectOptions {
            expected_bucket_incarnation_id: Some(bucket.incarnation().as_uuid()),
            ..Default::default()
        };
        guard.lifecycle_owner().attach_to_object_options(&mut options);
        options.add_namespace_lock_guard(&guard.dispatch_owner());
        for signal in guard.retained_loss_signals() {
            options.add_namespace_lock_lost_signal(signal);
        }
        let result = self
            .app
            .object_store()
            .get_object_info_for_delete(bucket.storage_bucket_id().as_str(), request.target().object_key(), &options)
            .await;
        #[cfg(test)]
        let result = if tests::take_recovery_metadata_failure(request.target().object_key()) {
            Err(StorageError::ErasureReadQuorum)
        } else {
            result
        };
        guard.check_held()?;
        match result {
            Ok(info) if !info.delete_marker && info.version_id.is_none() => Ok(Some(TableCatalogObjectMetadata {
                body_revision: info.data_dir.filter(|id| !id.is_nil()),
                etag: info.etag,
                mod_time: info.mod_time,
                size: u64::try_from(info.size).map_err(maintenance_execution_error)?,
            })),
            Ok(_) => Err(conflict("recovery found an unexpected object version")),
            Err(StorageError::ObjectNotFound(bucket_name, key))
                if bucket_name == bucket.storage_bucket_id().as_str() && key == *request.target().object_key() =>
            {
                Ok(None)
            }
            Err(error) => Err(maintenance_execution_error(error)),
        }
    }

    async fn execute_retirement_permit(
        &self,
        catalog: Arc<Catalog>,
        work: &VectorMaintenanceWork,
        control: &VectorMaintenanceControl,
        permit: VectorRetirementDispatchPermit,
        resources: Arc<CompletionResources>,
    ) -> Result<bool, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let io = control.acquire_io().await?;
        let guard = permit.dispatch_guard().clone();
        let snapshot = catalog.read_vector_maintenance_authorization_with_guard(&guard).await?;
        let access = access_for(guard.lease().resource(), permit.target().object_key())?;
        let context = ManagedObjectContext::from_maintenance(
            access.clone(),
            managed_encryption(&snapshot.definition().effective_encryption),
            Arc::new(ManagedMaintenanceContext::new(
                audit_for(work, &self.identity),
                Arc::new(MaintenanceObjectAuthorizer::new(
                    self.app.clone(),
                    catalog.clone(),
                    Arc::new(guard.clone()),
                    self.identity.clone(),
                )),
            )),
        )
        .map_err(maintenance_execution_error)?;
        let authorization = permit.authorization_ref().clone();
        let ordinal = *permit.ordinal();
        let dispatch_id = *permit.dispatch_id();
        let target = permit.target();
        let mapped = ManagedRetirementDeletePermit::from_verified_dispatch(ManagedRetirementDeletePermitParts {
            access,
            target: ManagedRetiredObject {
                key: target.object_key().clone(),
                body_revision: *target.body_revision(),
                mod_time: target.mod_time().as_ref().map(VectorObjectTimeV1::to_time).transpose()?,
                content_length: *target.content_length(),
                version_id: *target.version_id(),
                delete_marker: *target.delete_marker(),
            },
            retirement_id: *permit.retirement_id(),
            retirement_revision: *permit.retirement_revision(),
            job_id: *permit.job_id(),
            job_revision: *permit.job_revision(),
            attempt: u32::from(*permit.attempt()),
            lease_id: *permit.lease_id(),
            worker_id: permit.worker_id().clone(),
            worker_boot_id: *permit.worker_boot_id(),
            dispatch_id,
            ordinal,
            execution_identity: ManagedMaintenanceIdentityRef::new(
                permit.execution_identity().account().to_owned(),
                *permit.execution_identity().user_generation(),
            ),
            lifecycle_owner: permit.lifecycle_owner(),
            dispatch_owner: permit.dispatch_owner(),
            retained_guard_owner: guard.retained_owner(),
            retained_guard_loss_signals: guard.retained_loss_signals(),
        })
        .map_err(maintenance_execution_error)?;
        let app = self.app.clone();
        let state = self.gc_state.clone();
        // The admitted mutation and its receipt recording retain their real owners and budget
        // even when the application waiter disappears.
        tokio::spawn(async move {
            let _io = io;
            let outcome = DefaultObjectUsecase::with_context(Some(app))
                .delete_retired_object(context, mapped)
                .await
                .map_err(maintenance_execution_error)?;
            guard.check_held()?;
            let completion =
                VectorMaintenanceOwnedCompletion::try_from_completed_operations(guard.lease(), vec![dispatch_id], Vec::new())?;
            state
                .completed
                .lock()
                .insert(dispatch_id, CompletedDelete { completion, resources });
            let (observation, complete, error) = match outcome {
                ManagedDeleteOutcome::DeleteCommitted { accounting } => (
                    VectorRetirementDeleteObservation::Committed(VectorDeleteAccountingV1::try_new(
                        accounting.size,
                        accounting.version_id,
                        accounting.removed_current_object,
                    )?),
                    true,
                    None,
                ),
                ManagedDeleteOutcome::NeedsRecovery { error } => (
                    VectorRetirementDeleteObservation::Unknown(VectorMaintenanceFailureCode::UnknownOutcome),
                    false,
                    error,
                ),
            };
            let progress = catalog.read_vector_retirement_progress(&guard, &authorization).await?;
            catalog
                .record_vector_retirement_delete(&guard, &authorization, ordinal, dispatch_id, *progress.revision(), observation)
                .await?;
            if complete {
                state.completed.lock().remove(&dispatch_id);
            }
            if let Some(error) = error {
                return Err(maintenance_execution_error(error));
            }
            Ok::<_, VectorMaintenanceExecutionError>(complete)
        })
        .await
        .map_err(VectorMaintenanceExecutionError::Join)?
    }
}

async fn run_admitted_native_recovery(
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    incarnation: Uuid,
    admission: NativeAdmission,
    io: OwnedSemaphorePermit,
    operation: Option<(VectorMaintenanceRecoveryOperation, Arc<CompletionResources>)>,
) -> Result<(), VectorMaintenanceExecutionError> {
    tokio::spawn(async move {
        let _io = io;
        let result = recover_exact_journal(store, &bucket, &key, incarnation).await;
        admission.complete(result.is_ok());
        result?;
        if let Some((operation, _resources)) = operation {
            operation.complete()?;
        }
        Ok(())
    })
    .await
    .map_err(VectorMaintenanceExecutionError::Join)?
}

async fn observe_recovered_target(
    store: Arc<ECStore>,
    request: &VectorRetirementRecoveryRequest,
) -> Result<Option<TableCatalogObjectMetadata>, VectorMaintenanceExecutionError> {
    let bucket = request
        .resource()
        .vector_bucket()
        .ok_or_else(|| conflict("retirement requires a Vector bucket"))?;
    let owner = store
        .acquire_bucket_incarnation_fence(bucket.storage_bucket_id().as_str(), bucket.incarnation().as_uuid())
        .await
        .map_err(maintenance_execution_error)?;
    let mut options = StorageObjectOptions {
        expected_bucket_incarnation_id: Some(bucket.incarnation().as_uuid()),
        ..Default::default()
    };
    owner.attach_to_object_options(&mut options);
    let observed = store
        .get_object_info_for_delete(bucket.storage_bucket_id().as_str(), request.target().object_key(), &options)
        .await;
    if owner.is_lock_lost() {
        return Err(conflict("recovery metadata protection was lost"));
    }
    match observed {
        Ok(info) if !info.delete_marker && info.version_id.is_none() => Ok(Some(TableCatalogObjectMetadata {
            body_revision: info.data_dir.filter(|id| !id.is_nil()),
            etag: info.etag,
            mod_time: info.mod_time,
            size: u64::try_from(info.size).map_err(maintenance_execution_error)?,
        })),
        Ok(_) => Err(conflict("recovery found an unexpected object version")),
        Err(StorageError::ObjectNotFound(name, key))
            if name == bucket.storage_bucket_id().as_str() && key == *request.target().object_key() =>
        {
            Ok(None)
        }
        Err(error) => Err(maintenance_execution_error(error)),
    }
}

async fn recover_exact_journal(
    store: Arc<ECStore>,
    bucket: &str,
    key: &str,
    incarnation: Uuid,
) -> Result<(), VectorMaintenanceExecutionError> {
    match store
        .recover_delete_transactions_at_incarnation(bucket, Some(key), Some(incarnation), &HealOpts::default())
        .await
    {
        Ok(None) => Ok(()),
        Ok(Some(item))
            if item.bucket == bucket
                && item.object == key
                && item.delete_recovery.is_some_and(|status| !status.has_unresolved()) =>
        {
            Ok(())
        }
        Ok(Some(_)) => Err(conflict("exact native journal recovery remains unresolved")),
        Err(error) => Err(maintenance_execution_error(error)),
    }
}

async fn completed_retirement(
    catalog: &Catalog,
    guard: &VectorMaintenanceDispatchGuard,
    work: &VectorMaintenanceWork,
    authorization: &CatalogMetadataRef,
) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
    let output = catalog
        .copy_vector_retirement_completed_progress(guard, authorization)
        .await?;
    Ok(VectorMaintenanceExecutionOutcome::Completed {
        result: Box::new(VectorMaintenanceResultV1::try_for_work(
            work,
            VectorMaintenanceResultEffect::Retired,
            Vec::new(),
            Vec::new(),
            vec![authorization.into()],
            vec![(&output).into()],
            None,
        )?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::object::managed_request_fixture_with_encryption;
    use crate::app::storage_api::object_usecase::contract::object::ObjectIO;
    use crate::app::storage_api::object_usecase::{StorageObjectToDelete, StoragePutObjReader};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    static RECOVERY_METADATA_FAILURE: Mutex<Option<String>> = Mutex::new(None);

    struct RecoveryMetadataFailure(String);

    impl RecoveryMetadataFailure {
        fn once(key: String) -> Self {
            assert!(RECOVERY_METADATA_FAILURE.lock().replace(key.clone()).is_none());
            Self(key)
        }
    }

    impl Drop for RecoveryMetadataFailure {
        fn drop(&mut self) {
            let mut failure = RECOVERY_METADATA_FAILURE.lock();
            if failure.as_ref() == Some(&self.0) {
                failure.take();
            }
        }
    }

    pub(super) fn take_recovery_metadata_failure(key: &str) -> bool {
        let mut failure = RECOVERY_METADATA_FAILURE.lock();
        if failure.as_deref() == Some(key) {
            failure.take();
            true
        } else {
            false
        }
    }

    struct ActualNativeCompletion(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for ActualNativeCompletion {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    async fn claim_recovery_test_job(catalog: &Catalog, spec: VectorMaintenanceJobSpec) -> VectorMaintenanceWork {
        let snapshot = catalog.create_vector_maintenance_job(spec).await.expect("stable job");
        let resource = snapshot.job().resource();
        let identity = resource.identity().expect("resource");
        let observed = catalog
            .read_vector_maintenance_resource(
                &identity,
                &Namespace::parse(resource.namespace()).expect("namespace"),
                &CatalogName::parse(resource.name()).expect("name"),
            )
            .await
            .expect("current resource");
        let VectorMaintenanceClaimOutcome::Acquired(work) = catalog
            .claim_vector_maintenance_job(
                &identity,
                *snapshot.job().job_id(),
                VectorMaintenanceClaimCondition::from_snapshot(&snapshot, observed),
                VectorMaintenanceWorker::try_new(
                    "recovery-metadata-failure".into(),
                    Uuid::new_v4(),
                    snapshot.job().execution_identity().clone(),
                )
                .expect("actual worker"),
                Uuid::new_v4(),
            )
            .await
            .expect("claim")
        else {
            panic!("actual work must be admitted")
        };
        *work
    }

    #[derive(Clone, Copy)]
    enum RecoveryFailurePoint {
        ObjectMetadata,
        ReadmissionProgress,
        ProgressResponse,
    }

    #[test]
    #[serial_test::serial]
    fn native_recovery_metadata_failure_retains_actual_no_delete_attempt_completion() {
        crate::app::gating_test_env::run_large_stack_test("recovery-metadata-failure", || {
            failed_native_recovery(RecoveryFailurePoint::ObjectMetadata)
        });
    }

    #[test]
    #[serial_test::serial]
    fn native_recovery_readmission_read_failure_retains_actual_no_delete_attempt_completion() {
        crate::app::gating_test_env::run_large_stack_test("recovery-readmission-read-failure", || {
            failed_native_recovery(RecoveryFailurePoint::ReadmissionProgress)
        });
    }

    #[test]
    #[serial_test::serial]
    fn native_recovery_progress_response_loss_rejects_no_delete_attempt_completion() {
        crate::app::gating_test_env::run_large_stack_test("recovery-progress-response-loss", || {
            failed_native_recovery(RecoveryFailurePoint::ProgressResponse)
        });
    }

    async fn failed_native_recovery(failure_point: RecoveryFailurePoint) {
        let (_fixture, executor, catalog, entry, control) = super::super::integration_tests::fixture().await;
        let resource = entry.head().resource_identity().clone();
        let commit_id =
            vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap();
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::EnrollCurrent {
                    expected: Box::new(entry.clone()),
                    commit_id,
                },
                control.clone(),
            )
            .await
            .expect("maintenance enrollment validates the manifest through the authorized reader");
        let (operation, entry) = catalog
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let key = format!("{}data/recovery-{}.bin", resource.dataset_prefix(), Uuid::new_v4());
        let store = executor.app.object_store();
        let mut body = StoragePutObjReader::from_vec(vec![0x65; 4096]);
        let old = store
            .put_object(
                resource.storage_bucket_id(),
                &key,
                &mut body,
                &StorageObjectOptions {
                    mod_time: Some(OffsetDateTime::now_utc() - time::Duration::seconds(1800)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let target = VectorRetirementTargetV1::from_object_metadata(
            &resource,
            CatalogObjectKey::try_new(key.clone()).unwrap(),
            &TableCatalogObjectMetadata {
                body_revision: old.data_dir,
                etag: old.etag.clone(),
                mod_time: old.mod_time,
                size: u64::try_from(old.size).unwrap(),
            },
        )
        .unwrap();
        let recorded = VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
        let prepared = catalog
            .prepare_vector_retirement(
                &operation,
                VectorRetirementEvidenceV1::try_new(
                    recorded.clone(),
                    VectorMaintenanceHeadV1::from_entry(&entry).unwrap(),
                    None,
                    vec![entry.head().metadata().into()],
                    vec![entry.head().metadata().into()],
                    Vec::new(),
                    VectorHistoryRetentionV1::All,
                )
                .unwrap(),
                vec![target],
            )
            .await
            .unwrap();
        drop(operation);
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                recorded.clone(),
                VectorMaintenanceKind::OrphanGc,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(
                    Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()),
                    None,
                    Some(prepared.plan_ref().into()),
                )
                .unwrap(),
                executor.source.identity.clone(),
                None,
                executor.source.policy.clone(),
            )
            .unwrap(),
        )
        .unwrap();
        let gc = claim_recovery_test_job(&catalog, spec).await;
        let dispatch = catalog.begin_vector_maintenance_dispatch(gc.lease()).await.unwrap();
        let authorization = executor
            .publish_gc_retirement(catalog.clone(), Arc::new(dispatch.clone()), &gc, &control, prepared)
            .await
            .unwrap();
        let progress = catalog
            .read_vector_retirement_progress(&dispatch, &authorization)
            .await
            .unwrap();
        let dispatch_id = Uuid::new_v4();
        let VectorRetirementDispatchAdmission::Ready(permit) = catalog
            .begin_vector_retirement_delete(&dispatch, &authorization, 0, *progress.revision(), dispatch_id)
            .await
            .unwrap()
        else {
            panic!("actual GC target admission")
        };
        let retirement_id = *permit.retirement_id();
        let resources = executor.gc_state.reserve(&gc, &control).unwrap();
        let (ended, completion) = tokio::sync::oneshot::channel();
        let mut options = StorageObjectOptions {
            expected_bucket_incarnation_id: Some(resource.vector_bucket().unwrap().incarnation().as_uuid()),
            delete_mutation_owner: Some(Arc::new((ActualNativeCompletion(Some(ended)), dispatch.retained_owner()))),
            ..Default::default()
        };
        options.add_namespace_lock_guard(&dispatch.dispatch_owner());
        let (_, errors, receipts) = store
            .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
                resource.storage_bucket_id(),
                vec![
                    StorageObjectToDelete {
                        object_name: key.clone(),
                        ..Default::default()
                    }
                    .with_expected_identity(old.data_dir, old.mod_time, false),
                ],
                options,
                permit.lifecycle_owner(),
            )
            .await;
        tokio::time::timeout(Duration::from_secs(10), completion)
            .await
            .unwrap()
            .unwrap();
        assert!(errors.iter().all(Option::is_none));
        assert!(receipts[0].is_some(), "actual deletion produced its receipt");
        dispatch.check_held().unwrap();
        let gc_completion =
            VectorMaintenanceOwnedCompletion::try_from_completed_operations(dispatch.lease(), vec![dispatch_id], Vec::new())
                .unwrap();
        executor.gc_state.completed.lock().insert(
            dispatch_id,
            CompletedDelete {
                completion: gc_completion.clone(),
                resources,
            },
        );
        let progress = catalog
            .read_vector_retirement_progress(&dispatch, &authorization)
            .await
            .unwrap();
        catalog
            .record_vector_retirement_delete(
                &dispatch,
                &authorization,
                0,
                dispatch_id,
                *progress.revision(),
                VectorRetirementDeleteObservation::Unknown(VectorMaintenanceFailureCode::UnknownOutcome),
            )
            .await
            .unwrap();
        catalog
            .finish_vector_maintenance_job(&dispatch, *gc.snapshot().job().revision(), unresolved())
            .await
            .unwrap();
        drop(permit);
        drop(dispatch);
        let recovery = claim_recovery_test_job(
            &catalog,
            VectorMaintenanceJobSpec::from_semantic(
                VectorMaintenanceSemanticSpec::try_new(
                    recorded.clone(),
                    VectorMaintenanceKind::DeleteRecovery,
                    VectorMaintenanceOrigin::Recovery,
                    VectorMaintenanceInputV1::try_new(
                        None,
                        Some(
                            VectorMaintenanceSourceV1::try_new(
                                VectorMaintenanceSourceKind::RetirementAuthorization,
                                (&authorization).into(),
                            )
                            .unwrap(),
                        ),
                        None,
                    )
                    .unwrap(),
                    executor.source.identity.clone(),
                    None,
                    executor.source.policy.clone(),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
        let before_recovery_progress = {
            let guard = catalog.begin_vector_maintenance_dispatch(recovery.lease()).await.unwrap();
            catalog.read_vector_retirement_progress(&guard, &authorization).await.unwrap()
        };
        let metadata_failure =
            matches!(failure_point, RecoveryFailurePoint::ObjectMetadata).then(|| RecoveryMetadataFailure::once(key.clone()));
        let readmission_point = match failure_point {
            RecoveryFailurePoint::ObjectMetadata => None,
            RecoveryFailurePoint::ReadmissionProgress => Some(VectorRetirementReadmissionFailurePoint::BeforeProgressRead),
            RecoveryFailurePoint::ProgressResponse => Some(VectorRetirementReadmissionFailurePoint::AfterProgressWrite),
        };
        let readmission_failure = readmission_point.map(|point| {
            catalog
                .inject_vector_retirement_readmission_failure(&resource, recovery.lease().job_id(), retirement_id, point)
                .unwrap()
        });
        assert!(
            executor
                .recover_native_work(catalog.clone(), recovery.clone(), control.clone())
                .await
                .is_err()
        );
        if metadata_failure.is_some() {
            assert!(
                RECOVERY_METADATA_FAILURE.lock().is_none(),
                "actual metadata boundary reached the injected failure"
            );
        }
        if let Some(failure) = &readmission_failure {
            assert_eq!(failure.hits(), 1, "actual readmission reached the exact injected boundary");
        }
        drop(metadata_failure);
        drop(readmission_failure);
        let attempt = executor
            .gc_state
            .completed_attempts
            .lock()
            .remove(&recovery.lease().lease_id());
        let dispatch = catalog.begin_vector_maintenance_dispatch(recovery.lease()).await.unwrap();
        let progress = catalog
            .read_vector_retirement_progress(&dispatch, &authorization)
            .await
            .unwrap();
        if matches!(failure_point, RecoveryFailurePoint::ProgressResponse) {
            assert!(attempt.is_none(), "an actual progress write cannot yield a no-delete proof");
            assert_ne!(progress.revision(), before_recovery_progress.revision());
            assert_eq!(*progress.state(), VectorRetirementProgressState::Completed);
            assert_eq!(*progress.targets()[0].dispatch().as_ref().unwrap().job_id(), recovery.lease().job_id());
            assert_eq!(*progress.targets()[0].state(), VectorRetirementTargetState::AbsentAfterRecovery);
            assert!(progress.targets()[0].receipt().as_ref().unwrap().accounting().is_none());
        } else {
            assert_eq!(
                progress, before_recovery_progress,
                "failure before the write must preserve exact progress"
            );
            assert_eq!(
                *progress.targets()[0].dispatch().as_ref().unwrap().job_id(),
                gc.lease().job_id(),
                "the recovery attempt admitted no delete dispatch",
            );
        }
        let unknown = catalog
            .finish_vector_maintenance_job(&dispatch, *recovery.snapshot().job().revision(), unresolved())
            .await
            .unwrap();
        drop(dispatch);
        if matches!(failure_point, RecoveryFailurePoint::ProgressResponse) {
            executor
                .inspect_retirement_recovery(
                    catalog.clone(),
                    recorded,
                    recovery.lease().job_id(),
                    *unknown.job().revision(),
                    control,
                )
                .await
                .unwrap();
            let resolved = catalog
                .read_vector_maintenance_job(&resource, recovery.lease().job_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(*resolved.job().state(), VectorMaintenanceJobState::Succeeded);
            assert_eq!(resolved.job().job_id(), unknown.job().job_id());
            assert!(
                resolved.job().result_ref().is_some(),
                "completion uses the actual durable terminal progress"
            );
            return;
        }
        let attempt = attempt.expect("actual callback retained its no-delete completion");
        let inspection = catalog
            .inspect_vector_retirement_job_recovery(
                &resource,
                recovery.lease().job_id(),
                *unknown.job().revision(),
                &executor.source.identity,
            )
            .await
            .unwrap();
        assert!(inspection.merge_owned_completions(&[]).is_err());
        assert!(
            inspection.merge_owned_completions(&[gc_completion]).is_err(),
            "prior GC completion cannot prove the new attempt ended"
        );
        assert!(
            executor
                .inspect_retirement_recovery(
                    catalog.clone(),
                    recorded.clone(),
                    recovery.lease().job_id(),
                    *unknown.job().revision(),
                    control.clone()
                )
                .await
                .is_err(),
            "missing in-process proof remains unresolved"
        );
        executor
            .gc_state
            .completed_attempts
            .lock()
            .insert(recovery.lease().lease_id(), attempt);
        executor
            .inspect_retirement_recovery(catalog.clone(), recorded, recovery.lease().job_id(), *unknown.job().revision(), control)
            .await
            .unwrap();
        let resolved = catalog
            .read_vector_maintenance_job(&resource, recovery.lease().job_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*resolved.job().state(), VectorMaintenanceJobState::RetryScheduled);
        assert_eq!(resolved.job().job_id(), unknown.job().job_id());
        assert_eq!(resolved.job().attempt(), unknown.job().attempt());
        assert!(executor.gc_state.completed_attempts.lock().is_empty());
        assert!(
            executor.gc_state.completed.lock().contains_key(&dispatch_id),
            "prior exact native proof remains independent"
        );
    }

    fn control_with_memory(bytes: u64) -> VectorMaintenanceControl {
        let config = VectorMaintenanceRuntimeConfig {
            memory_bytes: NonZeroU64::new(bytes).expect("positive budget"),
            queue_bytes: NonZeroUsize::new(65_536).expect("minimum queue budget"),
            ..Default::default()
        };
        VectorMaintenanceControl::new(
            CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_secs(60),
            VectorMaintenanceBudgets::new(&config, None).expect("budgets"),
        )
    }

    #[tokio::test]
    async fn gc_completion_tracking_reserves_capacity_and_actual_memory() {
        let state = MaintenanceGcState::default();
        let control = control_with_memory(VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES * 512);
        let mut admitted = (0..VECTOR_RETIREMENT_MAX_TARGETS)
            .map(|_| state.reserve_bytes(&control, 1024).expect("bounded completion admission"))
            .collect::<Vec<_>>();
        assert!(
            state.reserve_bytes(&control, 1024).is_err(),
            "capacity refusal precedes physical dispatch"
        );
        admitted.pop();
        assert!(
            state.reserve_bytes(&control, 1024).is_ok(),
            "released completion permits another dispatch"
        );
        drop(admitted);
        let single = control_with_memory(VECTOR_MAINTENANCE_MEMORY_UNIT_BYTES);
        let retained = state.reserve_bytes(&single, 1).expect("one memory unit");
        assert!(
            state.reserve_bytes(&single, 1).is_err(),
            "real memory reservation survives with its completion owner"
        );
        drop(retained);
        assert!(state.reserve_bytes(&single, 1).is_ok());
        single.stop_new_work.cancel();
        assert!(state.reserve_bytes(&single, 1).is_err(), "cancellation refuses new deletion tracking");
        assert!(
            MaintenanceGcState::default().completed.lock().is_empty(),
            "a new process has no original owner evidence"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_retirement_recovery_preserves_replacement_and_rejects_stale_incarnation() {
        let (request, access, store, _reset) = managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let bucket = access.bucket().storage_bucket_id().as_str();
        let key = format!("{}replacement.bin", access.allowed_prefix());
        let mut original = StoragePutObjReader::from_vec(b"original".to_vec());
        let old = store
            .put_object(bucket, &key, &mut original, &StorageObjectOptions::default())
            .await
            .expect("original body");
        let mut replacement = StoragePutObjReader::from_vec(b"replacement survives recovery".to_vec());
        let current = store
            .put_object(bucket, &key, &mut replacement, &StorageObjectOptions::default())
            .await
            .expect("new body");
        assert_ne!(old.data_dir, current.data_dir);
        assert!(
            recover_exact_journal(store.clone(), bucket, &key, Uuid::new_v4())
                .await
                .is_err()
        );
        recover_exact_journal(store.clone(), bucket, &key, access.bucket().incarnation().as_uuid())
            .await
            .expect("exact recovery");
        let context = ManagedObjectContext::from_authorized_request(access.clone(), request, ManagedEncryption::Unencrypted)
            .expect("foreground verification context");
        let usecase = DefaultObjectUsecase::from_global();
        let (_, body) = usecase
            .managed_get_object(&context, &key, current.data_dir, None, 128)
            .await
            .expect("replacement GET");
        assert_eq!(&body[..], b"replacement survives recovery");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_retirement_recovery_replays_absence_without_a_second_delete_receipt() {
        let (_request, access, store, _reset) = managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let bucket = access.bucket().storage_bucket_id().as_str();
        for size in [0usize, 4096] {
            let key = format!("{}retired-{size}.bin", access.allowed_prefix());
            let incarnation = access.bucket().incarnation().as_uuid();
            let mut body = StoragePutObjReader::from_vec(vec![0x31; size]);
            let info = store
                .put_object(bucket, &key, &mut body, &StorageObjectOptions::default())
                .await
                .expect("actual body");
            let target = StorageObjectToDelete {
                object_name: key.clone(),
                ..Default::default()
            }
            .with_expected_identity(info.data_dir, info.mod_time, false);
            let guard = Arc::new(
                store
                    .acquire_bucket_incarnation_fence(bucket, incarnation)
                    .await
                    .expect("actual lifecycle"),
            );
            let (_, errors, receipts) = store
                .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
                    bucket,
                    vec![target.clone()],
                    StorageObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                    guard,
                )
                .await;
            assert!(errors.iter().all(Option::is_none));
            assert_eq!(
                receipts[0],
                Some(crate::app::storage_api::managed::DeleteAccounting {
                    size: Some(u64::try_from(size).unwrap()),
                    version_id: None,
                    removed_current_object: true,
                }),
                "the original receipt must describe the actual current body, including an empty body"
            );
            recover_exact_journal(store.clone(), bucket, &key, incarnation)
                .await
                .expect("completed exact native recovery");
            recover_exact_journal(store.clone(), bucket, &key, incarnation)
                .await
                .expect("replayed recovery");
            assert!(matches!(
                store
                    .get_object_info_for_delete(bucket, &key, &StorageObjectOptions::default())
                    .await,
                Err(StorageError::ObjectNotFound(_, _))
            ));
            let (ordinary_deleted, ordinary_errors, ordinary_receipts) = store
                .delete_objects_with_tier_delete_journal_and_accounting(
                    bucket,
                    vec![target.clone()],
                    StorageObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                )
                .await;
            assert!(ordinary_errors.iter().all(Option::is_none), "ordinary replay errors: {ordinary_errors:?}");
            assert!(ordinary_deleted[0].source_missing);
            assert_eq!(
                ordinary_receipts[0],
                Some(crate::app::storage_api::managed::DeleteAccounting::default()),
                "ordinary idempotent deletion retains its empty accounting projection"
            );
            let guard = Arc::new(
                store
                    .acquire_bucket_incarnation_fence(bucket, incarnation)
                    .await
                    .expect("fresh lifecycle"),
            );
            let (replayed, errors, replay) = store
                .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
                    bucket,
                    vec![target],
                    StorageObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                    guard,
                )
                .await;
            assert!(errors.iter().all(Option::is_none), "guarded replay errors: {errors:?}");
            assert!(replayed[0].source_missing);
            assert_eq!(
                replay[0], None,
                "recovered absence cannot produce a committed-removal receipt: {replay:?}"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn native_retirement_recovery_waits_for_cancelled_physical_delete() {
        use crate::app::storage_api::test::{
            LocalPublicationPause, LocalPublicationStage, ReadOptions, StorageDiskRpcExt as _, find_local_disk_by_ref,
        };
        let (_request, access, store, _reset) = managed_request_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let bucket = access.bucket().storage_bucket_id().as_str().to_owned();
        let key = format!("{}cancelled-retirement.bin", access.allowed_prefix());
        let incarnation = access.bucket().incarnation().as_uuid();
        let mut body = StoragePutObjReader::from_vec(vec![0x52; 1024 * 1024]);
        let info = store
            .put_object(
                &bucket,
                &key,
                &mut body,
                &StorageObjectOptions {
                    write_completion: crate::app::storage_api::object_usecase::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("external body");
        let endpoints = store.instance_endpoints().expect("store endpoints");
        let endpoint = endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref())
            .find(|endpoint| endpoint.is_local)
            .expect("local endpoint");
        let disk = find_local_disk_by_ref(&endpoint.to_string()).await.expect("exact disk");
        let physical = disk
            .read_version("", &bucket, &key, "", &ReadOptions::default())
            .await
            .expect("physical metadata");
        assert!(!physical.inline_data());
        let part = format!("{key}/{}/part.1", physical.data_dir.expect("external body directory"));
        let mut pause = LocalPublicationPause::install(&disk, &bucket, &part, LocalPublicationStage::Remove)
            .expect("pause physical deletion");
        let guard = Arc::new(
            store
                .acquire_bucket_incarnation_fence(&bucket, incarnation)
                .await
                .expect("lifecycle"),
        );
        let deleting = store.clone();
        let delete_bucket = bucket.clone();
        let delete_key = key.clone();
        let mut delete = tokio::spawn(async move {
            deleting
                .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
                    &delete_bucket,
                    vec![
                        StorageObjectToDelete {
                            object_name: delete_key,
                            ..Default::default()
                        }
                        .with_expected_identity(info.data_dir, info.mod_time, false),
                    ],
                    StorageObjectOptions {
                        expected_bucket_incarnation_id: Some(incarnation),
                        ..Default::default()
                    },
                    guard,
                )
                .await
        });
        tokio::select! {
            result = &mut delete => panic!("delete returned before the physical pause: {}", result.is_ok()),
            entered = tokio::time::timeout(Duration::from_secs(10), pause.entered()) => {
                entered.expect("native removal reached").expect("native entry signal");
            }
        }
        delete.abort();
        assert!(delete.await.expect_err("cancelled caller").is_cancelled());
        let recovering = store.clone();
        let recovery_bucket = bucket.clone();
        let recovery_key = key.clone();
        let mut recovery =
            tokio::spawn(async move { recover_exact_journal(recovering, &recovery_bucket, &recovery_key, incarnation).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut recovery).await.is_err(),
            "recovery must await the actual native operation after caller cancellation"
        );
        drop(pause);
        tokio::time::timeout(Duration::from_secs(10), recovery)
            .await
            .expect("recovery completes after native release")
            .expect("recovery task")
            .expect("resolved exact journal");
        assert!(matches!(
            store
                .get_object_info_for_delete(&bucket, &key, &StorageObjectOptions::default())
                .await,
            Err(StorageError::ObjectNotFound(_, _))
        ));
    }
}
