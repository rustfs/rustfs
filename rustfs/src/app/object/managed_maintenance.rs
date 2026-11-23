// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Current authorization and retained ownership for bounded maintenance I/O.

use super::managed::{ManagedEncryption, ManagedObjectContext, ManagedObjectError, ManagedObjectIdentity};
use super::*;
use crate::app::RequestHead;
use crate::app::storage_api::context::{ReqInfo, RequestContext};
use crate::app::storage_api::managed::{
    BucketIncarnationFenceGuard, DeleteAccounting, ManagedObjectAccess, ManagedObjectOperation,
};
use rustfs_lock::{NamespaceLockGuard, distributed_lock::LockLostSignal};
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagedMaintenanceIdentityRef {
    account: String,
    user_generation: [u8; 16],
}

impl ManagedMaintenanceIdentityRef {
    pub(crate) fn new(account: String, user_generation: [u8; 16]) -> Self {
        Self {
            account,
            user_generation,
        }
    }
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn user_generation(&self) -> &[u8; 16] {
        &self.user_generation
    }
}

/// Server-selected preparation work that has no persisted maintenance job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedMaintenancePreparationPurpose {
    Enroll,
    GarbageCollectionPlanning,
    RecoveryPlanning,
}

impl ManagedMaintenancePreparationPurpose {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Enroll => "enroll",
            Self::GarbageCollectionPlanning => "gc-planning",
            Self::RecoveryPlanning => "recovery-planning",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ManagedMaintenanceAuditOrigin {
    Job {
        job_id: Uuid,
        attempt: u32,
    },
    Preparation {
        preparation_id: Uuid,
        purpose: ManagedMaintenancePreparationPurpose,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagedMaintenanceAuditContext {
    operation_id: Uuid,
    origin: ManagedMaintenanceAuditOrigin,
    worker_id: String,
    worker_boot_id: Uuid,
    initiator_ref: Option<String>,
    execution_identity: ManagedMaintenanceIdentityRef,
}

impl ManagedMaintenanceAuditContext {
    pub(crate) fn new(
        execution_identity: ManagedMaintenanceIdentityRef,
        job_id: Uuid,
        attempt: u32,
        worker_id: String,
        worker_boot_id: Uuid,
        initiator_ref: Option<String>,
    ) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            origin: ManagedMaintenanceAuditOrigin::Job { job_id, attempt },
            worker_id,
            worker_boot_id,
            initiator_ref,
            execution_identity,
        }
    }
    pub(crate) fn new_preparation(
        execution_identity: ManagedMaintenanceIdentityRef,
        preparation_id: Uuid,
        purpose: ManagedMaintenancePreparationPurpose,
        worker_id: String,
        worker_boot_id: Uuid,
        initiator_ref: Option<String>,
    ) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            origin: ManagedMaintenanceAuditOrigin::Preparation { preparation_id, purpose },
            worker_id,
            worker_boot_id,
            initiator_ref,
            execution_identity,
        }
    }

    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }
    pub(crate) fn job_id(&self) -> Option<Uuid> {
        match self.origin {
            ManagedMaintenanceAuditOrigin::Job { job_id, .. } => Some(job_id),
            ManagedMaintenanceAuditOrigin::Preparation { .. } => None,
        }
    }
    pub(crate) fn attempt(&self) -> Option<u32> {
        match self.origin {
            ManagedMaintenanceAuditOrigin::Job { attempt, .. } => Some(attempt),
            ManagedMaintenanceAuditOrigin::Preparation { .. } => None,
        }
    }
    pub(crate) fn preparation_id(&self) -> Option<Uuid> {
        match self.origin {
            ManagedMaintenanceAuditOrigin::Preparation { preparation_id, .. } => Some(preparation_id),
            ManagedMaintenanceAuditOrigin::Job { .. } => None,
        }
    }
    pub(crate) fn preparation_purpose(&self) -> Option<ManagedMaintenancePreparationPurpose> {
        match self.origin {
            ManagedMaintenanceAuditOrigin::Preparation { purpose, .. } => Some(purpose),
            ManagedMaintenanceAuditOrigin::Job { .. } => None,
        }
    }
    pub(crate) fn worker_id(&self) -> &str {
        &self.worker_id
    }
    pub(crate) fn worker_boot_id(&self) -> Uuid {
        self.worker_boot_id
    }
    pub(crate) fn execution_identity(&self) -> &ManagedMaintenanceIdentityRef {
        &self.execution_identity
    }

    fn next_operation(&self) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            ..self.clone()
        }
    }

    pub(crate) fn tags(&self) -> HashMap<String, serde_json::Value> {
        let mut tags = HashMap::from([
            ("maintenanceOperationId".to_owned(), self.operation_id.to_string().into()),
            (
                "maintenanceExecutionGeneration".to_owned(),
                Uuid::from_bytes(self.execution_identity.user_generation).to_string().into(),
            ),
            ("maintenanceWorkerId".to_owned(), self.worker_id.clone().into()),
            ("maintenanceWorkerBootId".to_owned(), self.worker_boot_id.to_string().into()),
            ("maintenanceInitiator".to_owned(), self.initiator_ref.clone().into()),
        ]);
        match self.origin {
            ManagedMaintenanceAuditOrigin::Job { job_id, attempt } => {
                tags.insert("maintenanceJobId".to_owned(), job_id.to_string().into());
                tags.insert("maintenanceAttempt".to_owned(), attempt.into());
            }
            ManagedMaintenanceAuditOrigin::Preparation { preparation_id, purpose } => {
                tags.insert("maintenancePreparationId".to_owned(), preparation_id.to_string().into());
                tags.insert("maintenancePreparationPurpose".to_owned(), purpose.as_str().into());
            }
        }
        tags
    }

    pub(crate) fn envelope(&self, access: &ManagedObjectAccess, key: &str) -> Arc<crate::app::RequestEnvelope> {
        let request_id = self.operation_id.to_string();
        let context = RequestContext {
            request_id: request_id.clone(),
            x_amz_request_id: request_id,
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        };
        let mut extensions = http::Extensions::new();
        extensions.insert(context.clone());
        // This projection supplies audit facts only. It carries no authentication result.
        extensions.insert(ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: self.execution_identity.account.clone(),
                ..Default::default()
            }),
            bucket: Some(access.bucket().storage_bucket_id().as_str().to_owned()),
            object: Some(key.to_owned()),
            request_context: Some(context),
            ..Default::default()
        });
        Arc::new(crate::app::RequestEnvelope::from_parts(
            RequestHead {
                method: http::Method::POST,
                uri: http::Uri::default(),
                headers: HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            extensions,
        ))
    }
}

pub(crate) struct ManagedMaintenanceContext {
    pub(super) audit: ManagedMaintenanceAuditContext,
    authorizer: Arc<dyn ManagedMaintenanceAuthorizer>,
}

impl ManagedMaintenanceContext {
    pub(crate) fn new(audit: ManagedMaintenanceAuditContext, authorizer: Arc<dyn ManagedMaintenanceAuthorizer>) -> Self {
        Self { audit, authorizer }
    }
}

pub(crate) struct ManagedMaintenanceAdmission {
    principal: SseKmsPrincipal,
    audit: ManagedMaintenanceAuditContext,
}

impl ManagedMaintenanceAdmission {
    pub(crate) fn new(principal: SseKmsPrincipal, audit: ManagedMaintenanceAuditContext) -> Result<Self, ManagedObjectError> {
        if !principal.matches_managed_operation(audit.execution_identity.account(), audit.operation_id) {
            return Err(ManagedObjectError::MissingPrincipal);
        }
        Ok(Self { principal, audit })
    }
}

#[async_trait::async_trait]
pub(crate) trait ManagedMaintenanceAuthorizer: Send + Sync {
    async fn authorize(
        &self,
        access: &ManagedObjectAccess,
        operation: ManagedObjectOperation,
        key: &str,
        encryption: &ManagedEncryption,
        audit: &ManagedMaintenanceAuditContext,
    ) -> Result<ManagedMaintenanceAdmission, ManagedObjectError>;
}

pub(super) struct ManagedOperation {
    pub(super) request: Arc<crate::app::RequestEnvelope>,
    pub(super) principal: SseKmsPrincipal,
    pub(super) helper: OperationHelper,
}

impl ManagedObjectContext {
    pub(super) async fn admit(
        &self,
        key: &str,
        operation: ManagedObjectOperation,
        api: S3Operation,
    ) -> Result<ManagedOperation, ManagedObjectError> {
        match &self.identity {
            ManagedObjectIdentity::Request { request, principal } => {
                let helper = if api == S3Operation::PutObject {
                    OperationHelper::from_view(&request.audit_view(), EventName::ObjectCreatedPut, api)
                } else {
                    OperationHelper::audit_only_from_view(&request.audit_view(), api)
                };
                if let Err(error) = self.access.check(key, operation) {
                    let result = Err(ManagedObjectError::from(error));
                    super::managed::finish_managed_read(helper, &result);
                    return result;
                }
                let principal = SseKmsPrincipal::from_request(request.as_ref()).unwrap_or_else(|| principal.as_ref().clone());
                Ok(ManagedOperation {
                    request: request.clone(),
                    principal,
                    helper,
                })
            }
            ManagedObjectIdentity::Maintenance { context } => {
                let audit = context.audit.next_operation();
                let request = audit.envelope(&self.access, key);
                let helper = OperationHelper::audit_only_named_from_view(&request.audit_view(), "ManagedMaintenanceObject")
                    .with_audit_tags(audit.tags());
                if let Err(error) = self.access.check(key, operation) {
                    let result = Err(ManagedObjectError::from(error));
                    super::managed::finish_managed_read(helper, &result);
                    return result;
                }
                let admission = match context
                    .authorizer
                    .authorize(&self.access, operation, key, self.encryption(), &audit)
                    .await
                {
                    Ok(admission) => admission,
                    Err(error) => {
                        let result = Err(error);
                        super::managed::finish_managed_read(helper, &result);
                        return result;
                    }
                };
                if admission.audit != audit {
                    let result = Err(ManagedObjectError::MissingPrincipal);
                    super::managed::finish_managed_read(helper, &result);
                    return result;
                }
                Ok(ManagedOperation {
                    request,
                    principal: admission.principal,
                    helper,
                })
            }
        }
    }
}

pub(crate) struct ManagedRetiredObject {
    pub(crate) key: String,
    pub(crate) body_revision: Uuid,
    pub(crate) mod_time: Option<time::OffsetDateTime>,
    pub(crate) content_length: u64,
    pub(crate) version_id: Option<Uuid>,
    pub(crate) delete_marker: bool,
}

/// Produced only by the fixed application mapping of a verified retirement dispatch.
pub(crate) struct ManagedRetirementDeletePermitParts {
    pub(crate) access: ManagedObjectAccess,
    pub(crate) target: ManagedRetiredObject,
    pub(crate) retirement_id: Uuid,
    pub(crate) retirement_revision: Uuid,
    pub(crate) job_id: Uuid,
    pub(crate) job_revision: Uuid,
    pub(crate) attempt: u32,
    pub(crate) lease_id: Uuid,
    pub(crate) worker_id: String,
    pub(crate) worker_boot_id: Uuid,
    pub(crate) dispatch_id: Uuid,
    pub(crate) ordinal: u16,
    pub(crate) execution_identity: ManagedMaintenanceIdentityRef,
    pub(crate) lifecycle_owner: Arc<BucketIncarnationFenceGuard>,
    pub(crate) dispatch_owner: Arc<NamespaceLockGuard>,
    pub(crate) retained_guard_owner: Arc<dyn Send + Sync>,
    pub(crate) retained_guard_loss_signals: Vec<Arc<LockLostSignal>>,
}

pub(crate) struct ManagedRetirementDeletePermit(ManagedRetirementDeletePermitParts);

impl ManagedRetirementDeletePermit {
    pub(crate) fn from_verified_dispatch(parts: ManagedRetirementDeletePermitParts) -> Result<Self, ManagedObjectError> {
        parts.access.check(&parts.target.key, ManagedObjectOperation::Delete)?;
        if parts.target.body_revision.is_nil()
            || parts.target.version_id.is_some()
            || parts.target.delete_marker
            || [
                parts.retirement_id,
                parts.retirement_revision,
                parts.job_id,
                parts.job_revision,
                parts.lease_id,
                parts.worker_boot_id,
                parts.dispatch_id,
            ]
            .iter()
            .any(Uuid::is_nil)
            || parts.worker_id.is_empty()
            || parts.lifecycle_owner.is_lock_lost()
            || parts.dispatch_owner.is_released()
            || parts.dispatch_owner.is_lock_lost()
        {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        Ok(Self(parts))
    }
}

pub(crate) enum ManagedDeleteOutcome {
    DeleteCommitted { accounting: DeleteAccounting },
    NeedsRecovery { error: Option<ManagedObjectError> },
}

struct ManagedMaintenanceOperationOwner {
    _permit: ManagedRetirementDeletePermitParts,
    helper: Mutex<(Option<OperationHelper>, bool)>,
    _principal: SseKmsPrincipal,
    completed: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for ManagedMaintenanceOperationOwner {
    fn drop(&mut self) {
        let Ok((helper, committed)) = self.helper.get_mut() else {
            return;
        };
        if let Some(helper) = helper.take() {
            let result = if *committed {
                Ok(())
            } else {
                Err(ApiError::from(StorageError::PreconditionFailed).into())
            };
            super::managed::finish_managed_read(helper, &result);
        }
        if let Some(completed) = self.completed.take() {
            let _ = completed.send(());
        }
    }
}

impl DefaultObjectUsecase {
    /// An admitted exact mutation outlives its waiter. Its real protection and audit
    /// owners remain attached until native physical work has drained.
    pub(crate) async fn delete_retired_object(
        &self,
        context: ManagedObjectContext,
        permit: ManagedRetirementDeletePermit,
    ) -> Result<ManagedDeleteOutcome, ManagedObjectError> {
        let parts = permit.0;
        let ManagedObjectIdentity::Maintenance { context: maintenance } = &context.identity else {
            return Err(ManagedObjectError::MissingPrincipal);
        };
        if parts.access != context.access
            || parts.execution_identity != maintenance.audit.execution_identity
            || Some(parts.job_id) != maintenance.audit.job_id()
            || Some(parts.attempt) != maintenance.audit.attempt()
            || parts.worker_id != maintenance.audit.worker_id
            || parts.worker_boot_id != maintenance.audit.worker_boot_id
        {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        let operation = context
            .admit(&parts.target.key, ManagedObjectOperation::Delete, S3Operation::DeleteObject)
            .await?;
        let store = self.managed_store_under_guard(&context, &parts.lifecycle_owner).await?;
        let bucket = parts.access.bucket().storage_bucket_id().as_str().to_owned();
        let object = ObjectToDelete {
            object_name: parts.target.key.clone(),
            ..Default::default()
        }
        .with_expected_identity(Some(parts.target.body_revision), parts.target.mod_time, false);
        let mut options = ObjectOptions {
            expected_bucket_incarnation_id: Some(parts.access.bucket().incarnation().as_uuid()),
            ..Default::default()
        };
        parts.lifecycle_owner.attach_to_object_options(&mut options);
        options.add_namespace_lock_guard(&parts.dispatch_owner);
        for signal in &parts.retained_guard_loss_signals {
            options.add_namespace_lock_lost_signal(signal.clone());
        }
        if parts.lifecycle_owner.is_lock_lost()
            || parts.dispatch_owner.is_lock_lost()
            || parts.dispatch_owner.is_released()
            || parts.retained_guard_loss_signals.iter().any(|signal| signal.is_lost())
        {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        let lifecycle = parts.lifecycle_owner.clone();
        let helper = operation.helper.with_audit_tags(HashMap::from([
            ("retirementId".to_owned(), parts.retirement_id.to_string().into()),
            ("retirementRevision".to_owned(), parts.retirement_revision.to_string().into()),
            ("dispatchId".to_owned(), parts.dispatch_id.to_string().into()),
            ("targetOrdinal".to_owned(), parts.ordinal.into()),
            ("targetContentLength".to_owned(), parts.target.content_length.into()),
        ]));
        let retained = parts.retained_guard_owner.clone();
        let (completed, completion) = tokio::sync::oneshot::channel();
        let owner = Arc::new(ManagedMaintenanceOperationOwner {
            _permit: parts,
            helper: Mutex::new((Some(helper), false)),
            _principal: operation.principal,
            completed: Some(completed),
        });
        options.delete_mutation_owner = Some(Arc::new((owner.clone(), retained)));
        run_owned_mutation(async move {
            let (_, errors, receipts) = store
                .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
                    &bucket,
                    vec![object],
                    options,
                    lifecycle,
                )
                .await;
            let error = errors
                .into_iter()
                .next()
                .flatten()
                .map(ApiError::from)
                .map(ManagedObjectError::from);
            let receipt = receipts.into_iter().next().flatten();
            let outcome = match (error, receipt) {
                (None, Some(accounting)) if accounting.removed_current_object && accounting.version_id.is_none() => {
                    super::delete::apply_managed_delete_accounting(&bucket, &accounting).await;
                    ManagedDeleteOutcome::DeleteCommitted { accounting }
                }
                (error, _) => ManagedDeleteOutcome::NeedsRecovery { error },
            };
            owner
                .helper
                .lock()
                .map_err(|_| ApiError::invalid_request("maintenance audit owner is unavailable"))?
                .1 = matches!(&outcome, ManagedDeleteOutcome::DeleteCommitted { .. });
            drop(owner);
            // A native timeout can return while physical I/O still retains its owner.
            // Recovery completion requires that final holder to finish as well.
            completion
                .await
                .map_err(|_| ApiError::invalid_request("maintenance native owner completion is unavailable"))?;
            Ok(outcome)
        })
        .await
    }
}
