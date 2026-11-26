// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Instance-bound application assembly for Vector maintenance.

#[cfg(test)]
pub(crate) mod fault_test;
mod gc;
#[cfg(test)]
mod integration_tests;
mod preparation;
mod recovery;
mod runtime;
mod source;

pub(crate) use runtime::spawn;

use crate::app::object::ManagedMaintenanceIdentityRef;
use crate::error::ApiError;
use crate::runtime_sources::AppContext;
use rustfs_credentials::Credentials;
use rustfs_iam::store::UserType;
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Reloads an ordinary IAM identity from the selected server instance.
/// Missing generations and derived identities cannot authorize background work.
pub(crate) async fn resolve_identity(
    app: &AppContext,
    account: &str,
) -> Result<(ManagedMaintenanceIdentityRef, Credentials), ApiError> {
    if !app.iam().is_ready() {
        return Err(ApiError::service_unavailable());
    }
    let root = app.action_credentials().get().ok_or_else(ApiError::service_unavailable)?;
    if account == root.access_key || rustfs_iam::is_root_access_key(account) {
        return Err(ApiError::access_denied());
    }
    let iam = app.iam().handle();
    iam.load_user(account, UserType::Reg).await.map_err(ApiError::from)?;
    let (identity, valid) = iam.check_key(account).await.map_err(ApiError::from)?;
    let identity = identity.ok_or_else(ApiError::access_denied)?;
    validate_identity(account, valid, &identity)?;
    let generation = identity.sts_generation.ok_or_else(ApiError::access_denied)?;
    Ok((ManagedMaintenanceIdentityRef::new(account.into(), generation), identity.credentials))
}

fn validate_identity(account: &str, valid: bool, identity: &rustfs_policy::auth::UserIdentity) -> Result<(), ApiError> {
    let credentials = &identity.credentials;
    if !valid
        || !credentials.is_valid()
        || credentials.access_key != account
        || credentials.is_temp()
        || credentials.is_service_account()
        || !credentials.parent_user.is_empty()
        || !credentials.session_token.is_empty()
        || identity.sts_generation.is_none_or(|generation| generation == [0; 16])
    {
        return Err(ApiError::access_denied());
    }
    Ok(())
}

fn identity_conditions(app: &AppContext, credentials: &Credentials) -> Result<HashMap<String, Vec<String>>, ApiError> {
    let now = OffsetDateTime::now_utc();
    let mut conditions = HashMap::from([
        ("CurrentTime".into(), vec![now.format(&Rfc3339).map_err(ApiError::other)?]),
        ("EpochTime".into(), vec![now.unix_timestamp().to_string()]),
        ("userid".into(), vec![credentials.access_key.clone()]),
        ("username".into(), vec![credentials.access_key.clone()]),
        ("principaltype".into(), vec!["User".into()]),
    ]);
    let mut groups = app.iam().handle().user_group_memberships(&credentials.access_key);
    for group in credentials.groups.iter().flatten() {
        if !groups.contains(group) {
            groups.push(group.clone());
        }
    }
    if !groups.is_empty() {
        conditions.insert("groups".into(), groups);
    }
    Ok(conditions)
}

use super::{Catalog, bucket::authorization_facts};
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::object::{
    ManagedEncryption, ManagedMaintenanceAdmission, ManagedMaintenanceAuditContext, ManagedMaintenanceAuthorizer,
    ManagedObjectError,
};
use crate::app::storage_api::managed::{ManagedObjectAccess, ManagedObjectOperation};
use crate::app::storage_api::object_usecase::sse::{
    KmsKeyAuthorizer, ManagedSseRuntime, SseKmsPrincipal, SseKmsPrincipalExt, sse_error,
};
use crate::table_catalog::{VectorEffectiveEncryptionV1, VectorMaintenanceDispatchGuard};
use rustfs_policy::policy::{
    Args,
    action::{Action, KmsAction, S3VectorsAction},
};
use std::sync::Arc;

/// One dispatch retains its exact resource owner and server services across every I/O.
pub(crate) struct MaintenanceObjectAuthorizer {
    app: Arc<AppContext>,
    catalog: Arc<Catalog>,
    authority: MaintenanceAuthority,
    identity: ManagedMaintenanceIdentityRef,
    sse: Arc<ManagedSseRuntime>,
}

enum MaintenanceAuthority {
    Dispatch(Arc<VectorMaintenanceDispatchGuard>),
    Preparation(Box<MaintenancePreparationAuthority>),
}

struct MaintenancePreparationAuthority {
    guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
    audit: ManagedMaintenanceAuditContext,
    enrollment_snapshot: Option<crate::table_catalog::VectorMaintenanceAuthorizationSnapshot>,
}

impl MaintenanceObjectAuthorizer {
    pub(crate) fn new(
        app: Arc<AppContext>,
        catalog: Arc<Catalog>,
        guard: Arc<VectorMaintenanceDispatchGuard>,
        identity: ManagedMaintenanceIdentityRef,
    ) -> Self {
        let sse = Arc::new(ManagedSseRuntime::new(app.kms_runtime().service_manager()));
        Self {
            app,
            catalog,
            authority: MaintenanceAuthority::Dispatch(guard),
            identity,
            sse,
        }
    }

    fn for_preparation(
        app: Arc<AppContext>,
        catalog: Arc<Catalog>,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        audit: ManagedMaintenanceAuditContext,
        enrollment_snapshot: Option<crate::table_catalog::VectorMaintenanceAuthorizationSnapshot>,
    ) -> Self {
        let sse = Arc::new(ManagedSseRuntime::new(app.kms_runtime().service_manager()));
        let identity = audit.execution_identity().clone();
        Self {
            app,
            catalog,
            authority: MaintenanceAuthority::Preparation(Box::new(MaintenancePreparationAuthority {
                guard,
                audit,
                enrollment_snapshot,
            })),
            identity,
            sse,
        }
    }
}

#[async_trait::async_trait]
impl ManagedMaintenanceAuthorizer for MaintenanceObjectAuthorizer {
    async fn authorize(
        &self,
        access: &ManagedObjectAccess,
        operation: ManagedObjectOperation,
        key: &str,
        encryption: &ManagedEncryption,
        audit: &ManagedMaintenanceAuditContext,
    ) -> Result<ManagedMaintenanceAdmission, ManagedObjectError> {
        access.check(key, operation)?;
        if audit.execution_identity() != &self.identity {
            return Err(ApiError::access_denied().into());
        }
        let (identity, credential) = resolve_identity(&self.app, self.identity.account()).await?;
        if identity != self.identity {
            return Err(ApiError::access_denied().into());
        }
        let (snapshot, lifecycle) = match &self.authority {
            MaintenanceAuthority::Dispatch(guard) => {
                guard.check_held().map_err(ApiError::from)?;
                let lease = guard.lease();
                if audit.job_id() != Some(lease.job_id())
                    || audit.attempt() != Some(u32::from(lease.attempt()))
                    || audit.worker_id() != lease.worker_id()
                    || audit.worker_boot_id() != lease.worker_boot_id()
                    || lease.execution_identity().account() != self.identity.account()
                    || lease.execution_identity().user_generation() != self.identity.user_generation()
                    || audit.preparation_id().is_some()
                {
                    return Err(ApiError::access_denied().into());
                }
                (
                    self.catalog
                        .read_vector_maintenance_authorization_with_guard(guard)
                        .await
                        .map_err(ApiError::from)?,
                    guard.lifecycle_owner(),
                )
            }
            MaintenanceAuthority::Preparation(preparation) => {
                let MaintenancePreparationAuthority {
                    guard,
                    audit: expected,
                    enrollment_snapshot,
                } = preparation.as_ref();
                guard.check_held().map_err(ApiError::from)?;
                if audit.preparation_id().is_none()
                    || audit.preparation_id() != expected.preparation_id()
                    || audit.preparation_purpose() != expected.preparation_purpose()
                    || audit.worker_id() != expected.worker_id()
                    || audit.worker_boot_id() != expected.worker_boot_id()
                    || audit.job_id().is_some()
                    || audit.attempt().is_some()
                    || !matches!(
                        operation,
                        ManagedObjectOperation::Get | ManagedObjectOperation::Head | ManagedObjectOperation::Range
                    )
                {
                    return Err(ApiError::access_denied().into());
                }
                let snapshot = match enrollment_snapshot {
                    Some(snapshot) if snapshot.resource().resource() == guard.resource_identity() => snapshot.clone(),
                    Some(_) => return Err(ApiError::access_denied().into()),
                    None => self
                        .catalog
                        .read_vector_maintenance_authorization_with_operation_guard(guard)
                        .await
                        .map_err(ApiError::from)?,
                };
                (snapshot, guard.lifecycle_guard().map_err(ApiError::from)?)
            }
        };
        let resource = snapshot.resource().resource();
        if resource.vector_bucket() != Some(access.bucket()) || resource.table_id() != access.table_id() {
            return Err(ApiError::access_denied().into());
        }
        let expected_encryption = match &snapshot.definition().effective_encryption {
            VectorEffectiveEncryptionV1::Unencrypted => ManagedEncryption::Unencrypted,
            VectorEffectiveEncryptionV1::Aes256 => ManagedEncryption::Aes256,
            VectorEffectiveEncryptionV1::AwsKms { key_id, .. } => ManagedEncryption::AwsKms { key_id: key_id.clone() },
        };
        if encryption != &expected_encryption {
            return Err(ApiError::access_denied().into());
        }
        let buckets = DefaultBucketUsecase::with_context(Some(self.app.clone()));
        let metadata = buckets
            .read_managed_bucket_metadata(access.bucket(), &lifecycle)
            .await
            .map_err(ApiError::other)?;
        let facts = authorization_facts(access.bucket(), &metadata).map_err(ApiError::other)?;
        let mut conditions = identity_conditions(&self.app, &credential)?;
        conditions.insert("RequestedRegion".into(), vec![access.bucket().region().into()]);
        for (key, value) in facts.tags {
            conditions.insert(format!("VectorBucketTag/{key}"), vec![value]);
        }
        for (key, value) in &snapshot.definition().tags {
            conditions.insert(format!("ResourceTag/{key}"), vec![value.clone()]);
        }
        let action = match operation {
            ManagedObjectOperation::Get | ManagedObjectOperation::Head | ManagedObjectOperation::Range => {
                S3VectorsAction::GetVectorsAction
            }
            ManagedObjectOperation::CreateOnly
            | ManagedObjectOperation::CreateMultipart
            | ManagedObjectOperation::UploadPart
            | ManagedObjectOperation::CompleteMultipart
            | ManagedObjectOperation::AbortMultipart => S3VectorsAction::PutVectorsAction,
            ManagedObjectOperation::Delete => S3VectorsAction::DeleteVectorsAction,
        };
        let preparation_action = audit.preparation_purpose().map(|purpose| match purpose {
            crate::app::object::ManagedMaintenancePreparationPurpose::Enroll
            | crate::app::object::ManagedMaintenancePreparationPurpose::RecoveryPlanning => S3VectorsAction::PutVectorsAction,
            crate::app::object::ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning => {
                S3VectorsAction::DeleteVectorsAction
            }
        });
        for action in std::iter::once(action).chain(preparation_action) {
            let args = Args {
                account: &credential.access_key,
                groups: &credential.groups,
                action: Action::S3VectorsAction(action),
                bucket: access.bucket().public_name().as_str(),
                object: snapshot.resource().name().as_str(),
                conditions: &conditions,
                is_owner: false,
                claims: credential.claims_or_empty(),
                deny_only: false,
            };
            if !self
                .app
                .iam()
                .handle()
                .is_allowed_with_resource_policy(&args, facts.policy.as_ref())
                .await
                .map_err(ApiError::from)?
            {
                return Err(ApiError::access_denied().into());
            }
        }
        let mut context = rustfs_kms::types::OperationContext::new(credential.access_key.clone());
        context.operation_id = audit.operation_id();
        context.user_agent = Some("RustFS Vector maintenance".into());
        context
            .additional_context
            .insert("worker_id".into(), audit.worker_id().into());
        context
            .additional_context
            .insert("worker_boot_id".into(), audit.worker_boot_id().to_string());
        context
            .additional_context
            .insert("resource_table_id".into(), access.table_id().as_str().into());
        context
            .additional_context
            .insert("bucket_incarnation".into(), access.bucket().incarnation().as_uuid().to_string());
        if let Some(job_id) = audit.job_id() {
            context.additional_context.insert("job_id".into(), job_id.to_string());
        }
        if let Some(attempt) = audit.attempt() {
            context.additional_context.insert("attempt".into(), attempt.to_string());
        }
        if let Some(preparation_id) = audit.preparation_id() {
            context
                .additional_context
                .insert("preparation_id".into(), preparation_id.to_string());
        }
        if let Some(purpose) = audit.preparation_purpose() {
            context
                .additional_context
                .insert("preparation_purpose".into(), purpose.as_str().into());
        }

        let principal = SseKmsPrincipal::for_managed_maintenance(
            &credential,
            conditions,
            &audit.operation_id().to_string(),
            Arc::new(MaintenanceKmsAuthorizer {
                app: self.app.clone(),
                identity,
            }),
            Arc::new(self.sse.begin_operation(context).await),
        );
        ManagedMaintenanceAdmission::new(principal, audit.clone())
    }
}

struct MaintenanceKmsAuthorizer {
    app: Arc<AppContext>,
    identity: ManagedMaintenanceIdentityRef,
}

#[async_trait::async_trait]
impl KmsKeyAuthorizer for MaintenanceKmsAuthorizer {
    async fn is_allowed(
        &self,
        principal: &SseKmsPrincipal,
        action: KmsAction,
        key_id: &str,
    ) -> Result<bool, rustfs_sse::SseError> {
        let (identity, _) = resolve_identity(&self.app, self.identity.account())
            .await
            .map_err(sse_error)?;
        if identity != self.identity {
            return Err(sse_error(ApiError::access_denied()));
        }
        Ok(self
            .app
            .iam()
            .handle()
            .is_allowed(&principal.kms_policy_args(action, key_id))
            .await)
    }
}

use crate::app::object::{ManagedMaintenanceContext, ManagedObjectContext};
use crate::table_catalog::{
    VectorMaintenanceControl, VectorMaintenanceExecutionError, VectorMaintenanceExecutionOutcome, VectorMaintenanceFailureCode,
    VectorMaintenanceWork,
};
use crate::vector::lance::{LanceManifestAdapter, LanceOperationContext, ManifestIoLimits};

pub(crate) struct MaintenanceExecutor {
    app: Arc<AppContext>,
    identity: ManagedMaintenanceIdentityRef,
    limits: ManifestIoLimits,
    worker_boot_id: uuid::Uuid,
    source: Arc<source::MaintenanceSource>,
    gc_state: gc::MaintenanceGcState,
}

struct MaintenancePublicationAuthorization {
    source: Arc<source::MaintenanceSource>,
    catalog: Arc<Catalog>,
    dispatch: Arc<crate::table_catalog::VectorMaintenanceDispatchGuard>,
    access: ManagedObjectAccess,
}

#[async_trait::async_trait]
impl crate::vector::lance::MaintenancePublicationAuthorizer for MaintenancePublicationAuthorization {
    async fn authorize(
        &self,
        resource: &crate::table_catalog::CatalogResourceIdentity,
        candidate: &crate::table_catalog::CatalogMetadataRef,
    ) -> Result<(), crate::vector::lance::LanceManifestError> {
        self.dispatch.check_held()?;
        if self.dispatch.lease().resource() != resource {
            return Err(crate::vector::lance::LanceManifestError::ReferenceMismatch {
                reason: "maintenance publication has another resource lifetime",
            });
        }
        self.access
            .check(candidate.object_key().as_str(), ManagedObjectOperation::CreateOnly)?;
        self.source
            .authorize_actions_with_guard(
                &self.catalog,
                &self.dispatch,
                &[S3VectorsAction::GetVectorsAction, S3VectorsAction::PutVectorsAction],
            )
            .await?;
        Ok(())
    }
}

impl MaintenanceExecutor {
    fn new(
        app: Arc<AppContext>,
        identity: ManagedMaintenanceIdentityRef,
        limits: ManifestIoLimits,
        source: Arc<source::MaintenanceSource>,
    ) -> Self {
        Self {
            app,
            identity,
            limits,
            worker_boot_id: uuid::Uuid::new_v4(),
            source,
            gc_state: gc::MaintenanceGcState::default(),
        }
    }

    async fn execute_lance(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let dispatch = Arc::new(control.dispatch_guard()?.clone());
        let job = work.snapshot().job();
        let expected_resource = job.resource().identity()?;
        let namespace =
            crate::table_catalog::Namespace::parse(job.resource().namespace()).map_err(maintenance_execution_error)?;
        let name = crate::table_catalog::CatalogName::parse(job.resource().name()).map_err(maintenance_execution_error)?;
        let bucket = expected_resource.vector_bucket().ok_or_else(|| {
            crate::table_catalog::TableCatalogStoreError::Invalid("maintenance requires a Vector bucket".into())
        })?;
        let operation = Arc::new(
            catalog
                .begin_vector_operation_with_lifecycle(bucket, &namespace, &name, dispatch.lifecycle_owner())
                .await?,
        );
        if operation.resource_identity() != &expected_resource {
            return Err(
                crate::table_catalog::TableCatalogStoreError::Conflict("maintenance resource lifetime changed".into()).into(),
            );
        }
        let entry = catalog.read_vector_entry_with_guard(&operation).await?;
        let index = LanceManifestAdapter::<super::Backend>::index_from_catalog(&entry).map_err(maintenance_execution_error)?;
        let reader = Arc::new(super::dataset::CandidateReader::with_maintenance_control(control.clone()));
        let catalog = Arc::new(catalog.with_vector_managed_candidate_reader(reader.clone())?);
        let authorizer = Arc::new(MaintenanceObjectAuthorizer::new(
            self.app.clone(),
            catalog.clone(),
            dispatch.clone(),
            self.identity.clone(),
        ));
        let audit = ManagedMaintenanceAuditContext::new(
            self.identity.clone(),
            *job.job_id(),
            u32::from(*job.attempt()),
            work.lease().worker_id().into(),
            work.lease().worker_boot_id(),
            job.initiator_ref().clone(),
        );
        let access = ManagedObjectAccess::try_from_parts(
            bucket.clone(),
            expected_resource.table_id().clone(),
            expected_resource.dataset_prefix(),
            std::collections::BTreeSet::from([
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ]),
        )
        .map_err(maintenance_execution_error)?;
        let encryption = managed_encryption(&entry.definition().effective_encryption);
        let context = ManagedObjectContext::from_maintenance(
            access.clone(),
            encryption,
            Arc::new(ManagedMaintenanceContext::new(audit, authorizer)),
        )
        .map_err(maintenance_execution_error)?;
        let objects =
            crate::startup_services::maintenance_managed_lance_objects(self.app.clone(), context, operation.clone(), self.limits)
                .map_err(maintenance_execution_error)?;
        reader.bind(&objects);
        let publication = Arc::new(MaintenancePublicationAuthorization {
            source: self.source.clone(),
            catalog: catalog.clone(),
            dispatch,
            access: access.clone(),
        });
        let adapter = LanceManifestAdapter::new_guarded(
            LanceOperationContext {
                catalog,
                objects,
                access,
                guard: operation,
                definition: entry.definition().clone(),
            },
            name,
            index,
            self.limits,
        )
        .map_err(maintenance_execution_error)?;
        let adapter = adapter
            .with_maintenance_publication_authorizer(publication)
            .map_err(maintenance_execution_error)?
            .with_numeric_proof_cache(self.app.vector_numeric_proof_cache())
            .map_err(maintenance_execution_error)?;
        adapter.execute_maintenance(work, control).await
    }
}

#[async_trait::async_trait]
impl crate::table_catalog::VectorMaintenanceExecutor<super::Backend> for MaintenanceExecutor {
    async fn recover_native(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        self.recover_native_work(catalog, work, control).await
    }

    async fn execute(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        use crate::table_catalog::VectorMaintenanceKind;
        match work.snapshot().job().kind() {
            VectorMaintenanceKind::AnnBuild | VectorMaintenanceKind::Compaction => {
                self.execute_lance(catalog, work, control).await
            }
            VectorMaintenanceKind::CreateRecovery => self.execute_create_recovery(catalog, work, control).await,
            VectorMaintenanceKind::CommitRecovery => self.execute_commit_recovery(catalog, work, control).await,
            VectorMaintenanceKind::DeleteRecovery => self.execute_delete_recovery(catalog, work, control).await,
            VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc => self.execute_gc(catalog, work, control).await,
        }
    }

    async fn prepare(
        &self,
        catalog: Arc<Catalog>,
        action: crate::table_catalog::VectorMaintenancePreparationAction,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        let previous = if matches!(action, crate::table_catalog::VectorMaintenancePreparationAction::PrepareGc { .. }) {
            self.source.gc_scan.lock().clone()
        } else {
            None
        };
        let result = self.prepare_action(catalog, action, control).await;
        if result.is_err()
            && let Some(previous) = previous
        {
            let mut active = self.source.gc_scan.lock();
            if active.as_ref().is_some_and(|current| Arc::ptr_eq(current, &previous)) {
                *active = None;
            }
        }
        result
    }
}

fn managed_encryption(encryption: &VectorEffectiveEncryptionV1) -> ManagedEncryption {
    match encryption {
        VectorEffectiveEncryptionV1::Unencrypted => ManagedEncryption::Unencrypted,
        VectorEffectiveEncryptionV1::Aes256 => ManagedEncryption::Aes256,
        VectorEffectiveEncryptionV1::AwsKms { key_id, .. } => ManagedEncryption::AwsKms { key_id: key_id.clone() },
    }
}

fn maintenance_execution_error(error: impl std::error::Error + Send + Sync + 'static) -> VectorMaintenanceExecutionError {
    VectorMaintenanceExecutionError::Executor {
        category: VectorMaintenanceFailureCode::StorageUnavailable,
        source: Box::new(error),
    }
}

impl MaintenanceExecutor {
    async fn preparation_adapter(
        &self,
        catalog: Arc<Catalog>,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        preparation_id: uuid::Uuid,
        purpose: crate::app::object::ManagedMaintenancePreparationPurpose,
        control: VectorMaintenanceControl,
    ) -> Result<(Arc<Catalog>, LanceManifestAdapter<super::Backend>), VectorMaintenanceExecutionError> {
        control.check_admission()?;
        let entry = catalog.read_vector_maintenance_entry_with_operation_guard(&guard).await?;
        let resource = guard.resource_identity();
        let bucket = resource.vector_bucket().ok_or_else(|| {
            crate::table_catalog::TableCatalogStoreError::Invalid("maintenance requires a Vector bucket".into())
        })?;
        let index = LanceManifestAdapter::<super::Backend>::index_from_catalog(&entry).map_err(maintenance_execution_error)?;
        let reader = Arc::new(super::dataset::CandidateReader::with_maintenance_control(control.clone()));
        let catalog = Arc::new(catalog.with_vector_managed_candidate_reader(reader.clone())?);
        let audit = ManagedMaintenanceAuditContext::new_preparation(
            self.identity.clone(),
            preparation_id,
            purpose,
            "vector-maintenance".into(),
            self.worker_boot_id,
            None,
        );
        // Enrollment holds exclusive GC protection while validating under the head write lock.
        // GC planning shares that protection and must reload mutable authorization tags per I/O.
        let authorization = if purpose == crate::app::object::ManagedMaintenancePreparationPurpose::Enroll {
            Some(
                catalog
                    .read_vector_maintenance_authorization_with_operation_guard(&guard)
                    .await?,
            )
        } else {
            None
        };
        let authorizer = Arc::new(MaintenanceObjectAuthorizer::for_preparation(
            self.app.clone(),
            catalog.clone(),
            guard.clone(),
            audit.clone(),
            authorization,
        ));
        let access = ManagedObjectAccess::try_from_parts(
            bucket.clone(),
            resource.table_id().clone(),
            resource.dataset_prefix(),
            std::collections::BTreeSet::from([
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
            ]),
        )
        .map_err(maintenance_execution_error)?;
        let context = ManagedObjectContext::from_maintenance(
            access.clone(),
            managed_encryption(&entry.definition().effective_encryption),
            Arc::new(ManagedMaintenanceContext::new(audit, authorizer)),
        )
        .map_err(maintenance_execution_error)?;
        let objects =
            crate::startup_services::maintenance_managed_lance_objects(self.app.clone(), context, guard.clone(), self.limits)
                .map_err(maintenance_execution_error)?;
        reader.bind(&objects);
        let name = guard.name().clone();
        let adapter = LanceManifestAdapter::new_guarded(
            LanceOperationContext {
                catalog: catalog.clone(),
                objects,
                access,
                guard,
                definition: entry.definition().clone(),
            },
            name,
            index,
            self.limits,
        )
        .map_err(maintenance_execution_error)?
        .with_numeric_proof_cache(self.app.vector_numeric_proof_cache())
        .map_err(maintenance_execution_error)?
        .with_maintenance_control(control)
        .map_err(maintenance_execution_error)?;
        Ok((catalog, adapter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_policy::auth::UserIdentity;

    fn regular_identity() -> UserIdentity {
        let mut identity = UserIdentity::new(Credentials {
            access_key: "maintenance-user".into(),
            secret_key: "test-maintenance-secret".into(),
            status: "on".into(),
            ..Default::default()
        });
        identity.sts_generation = Some([7; 16]);
        identity
    }

    #[test]
    fn maintenance_identity_requires_current_ordinary_account_generation() {
        let current = regular_identity();
        assert!(validate_identity("maintenance-user", true, &current).is_ok());
        assert!(validate_identity("other-user", true, &current).is_err());
        assert!(validate_identity("maintenance-user", false, &current).is_err());
        for generation in [None, Some([0; 16])] {
            let mut legacy = regular_identity();
            legacy.sts_generation = generation;
            assert!(validate_identity("maintenance-user", true, &legacy).is_err());
        }
        for derived in [true, false] {
            let mut identity = regular_identity();
            if derived {
                identity.credentials.parent_user = "parent".into();
            } else {
                identity.credentials.session_token = "session".into();
            }
            assert!(validate_identity("maintenance-user", true, &identity).is_err());
        }
        let mut disabled = regular_identity();
        disabled.credentials.status = "off".into();
        assert!(validate_identity("maintenance-user", true, &disabled).is_err());
    }
}
