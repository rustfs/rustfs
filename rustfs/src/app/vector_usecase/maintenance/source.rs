// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded, read-only discovery of exact Catalog resources and maintenance inputs.

use super::*;
use crate::table_catalog::*;
use std::num::{NonZeroU16, NonZeroUsize};

pub(super) struct MaintenanceSource {
    pub(super) app: Arc<AppContext>,
    pub(super) backend: super::super::Backend,
    pub(super) identity: VectorMaintenanceExecutionIdentityV1,
    pub(super) policy: VectorMaintenancePolicyV1,
    pub(super) gc_scan: parking_lot::Mutex<Option<Arc<GcContinuation>>>,
}

pub(super) struct GcContinuation {
    pub(super) resource: VectorMaintenanceResourceV1,
    pub(super) kind: VectorMaintenanceKind,
    pub(super) head: VectorMaintenanceHeadV1,
    pub(super) next_key: String,
    pub(super) _memory: Arc<VectorMaintenanceMemoryReservation>,
}

#[async_trait::async_trait]
impl VectorMaintenanceSource for MaintenanceSource {
    async fn check_admission(&self, expected: &VectorMaintenanceExecutionIdentityV1) -> TableCatalogStoreResult<()> {
        if expected != &self.identity {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance identity differs from the admitted worker".into(),
            ));
        }
        let (current, _) = resolve_identity(&self.app, expected.account())
            .await
            .map_err(authorization_error)?;
        if current.user_generation() != expected.user_generation() {
            return Err(authorization_error(ApiError::access_denied()));
        }
        Ok(())
    }

    async fn authorize_preparation(
        &self,
        expected: &VectorMaintenanceExecutionIdentityV1,
        resource: &VectorMaintenanceResourceV1,
    ) -> TableCatalogStoreResult<()> {
        self.authorize_actions(
            expected,
            resource,
            &[S3VectorsAction::GetVectorsAction, S3VectorsAction::PutVectorsAction],
        )
        .await
    }

    async fn next_targets(
        &self,
        previous: Option<&VectorMaintenanceDiscoveryCursor>,
        limit: NonZeroUsize,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<VectorMaintenanceTargetPage> {
        let mut cursor = previous.cloned().unwrap_or_default();
        // The scheduler replaces this reservation after measuring the returned cursor.
        cursor.memory_owner = None;
        let mut buckets_exhausted = previous.is_some() && cursor.bucket_cursor.is_none();
        let mut targets = Vec::new();
        let mut failures = Vec::new();
        let mut emitted_resources = Vec::new();
        if let Some(scan) = self.gc_scan.lock().clone() {
            emitted_resources.push(scan.resource.identity()?);
            targets.push(VectorMaintenanceTarget::PrepareGc {
                resource: scan.resource.clone(),
                kind: scan.kind,
                expected_head: Box::new(scan.head.clone()),
            });
        }
        while targets.len() + failures.len() < limit.get() && !budget.stopped() {
            if cursor.active_buckets.is_empty() {
                if buckets_exhausted {
                    break;
                }
                let count = NonZeroU16::new(
                    u16::try_from(limit.get().min(8))
                        .map_err(|_| TableCatalogStoreError::Invalid("invalid discovery limit".into()))?,
                )
                .ok_or_else(|| TableCatalogStoreError::Invalid("empty discovery limit".into()))?;
                let (buckets, next) =
                    list_vector_maintenance_buckets(&self.backend, cursor.bucket_cursor.as_ref(), count, budget)
                        .await?
                        .into_parts();
                cursor.bucket_cursor = next;
                buckets_exhausted = cursor.bucket_cursor.is_none();
                cursor
                    .active_buckets
                    .extend(buckets.into_iter().map(|bucket| VectorMaintenanceActiveBucketCursor {
                        bucket,
                        resources: None,
                        jobs: None,
                        retirements: None,
                        current_resource: None,
                        resource_stage: VectorMaintenanceResourceStage::ExistingJobs,
                    }));
                if cursor.active_buckets.is_empty() {
                    continue;
                }
                cursor.next_bucket = 0;
            }
            let Some(index) = (0..cursor.active_buckets.len())
                .map(|offset| (cursor.next_bucket + offset) % cursor.active_buckets.len())
                .find(|index| {
                    cursor.active_buckets[*index]
                        .current_resource
                        .as_ref()
                        .is_none_or(|resource| !emitted_resources.contains(resource.resource()))
                })
            else {
                break;
            };
            let active = &mut cursor.active_buckets[index];
            if !budget.try_start_call() {
                break;
            }
            let catalog = match Catalog::for_vector_scope(self.backend.clone(), active.bucket.clone()).await {
                Ok(catalog) => catalog,
                Err(_) => {
                    failures.push(VectorMaintenanceDiscoveryFailure {
                        bucket: active.bucket.clone(),
                        category: VectorMaintenanceFailureCode::StorageUnavailable,
                    });
                    cursor.active_buckets.remove(index);
                    continue;
                }
            };
            let result = self.advance_resource(&catalog, active, budget).await;
            match result {
                Ok(Advance::Target(target)) => {
                    let target = *target;
                    let resource = match &target {
                        VectorMaintenanceTarget::NewJob(spec) => spec.resource(),
                        VectorMaintenanceTarget::ExistingJob { resource, .. }
                        | VectorMaintenanceTarget::Preparation { resource, .. }
                        | VectorMaintenanceTarget::PrepareRecovery { resource, .. }
                        | VectorMaintenanceTarget::InspectRetirementRecovery { resource, .. }
                        | VectorMaintenanceTarget::PrepareGc { resource, .. } => resource,
                    };
                    emitted_resources.push(resource.identity()?);
                    targets.push(target);
                }
                Ok(Advance::Continue) => {}
                Ok(Advance::BucketComplete) => {
                    cursor.active_buckets.remove(index);
                }
                Err(error) => {
                    failures.push(VectorMaintenanceDiscoveryFailure {
                        bucket: active.bucket.clone(),
                        category: if matches!(error, TableCatalogStoreError::Invalid(_)) {
                            VectorMaintenanceFailureCode::CorruptRecord
                        } else {
                            VectorMaintenanceFailureCode::StorageUnavailable
                        },
                    });
                    cursor.active_buckets.remove(index);
                }
            }
            cursor.next_bucket = if cursor.active_buckets.is_empty() {
                0
            } else {
                (index + 1) % cursor.active_buckets.len()
            };
        }
        let next_cursor = (!cursor.active_buckets.is_empty() || !buckets_exhausted).then_some(cursor);
        Ok(VectorMaintenanceTargetPage {
            targets,
            failures,
            next_cursor,
        })
    }
}

enum Advance {
    Target(Box<VectorMaintenanceTarget>),
    Continue,
    BucketComplete,
}

impl MaintenanceSource {
    pub(super) async fn authorize_actions(
        &self,
        expected: &VectorMaintenanceExecutionIdentityV1,
        resource: &VectorMaintenanceResourceV1,
        actions: &[S3VectorsAction],
    ) -> TableCatalogStoreResult<()> {
        self.check_admission(expected).await?;
        let identity = resource.identity()?;
        let bucket = resource.bucket().to_handle()?;
        let namespace =
            Namespace::parse(resource.namespace()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let name = CatalogName::parse(resource.name()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let catalog = Catalog::for_vector_scope(self.backend.clone(), bucket.clone()).await?;
        let snapshot = catalog
            .read_vector_maintenance_authorization(&identity, &namespace, &name)
            .await?;
        let buckets = DefaultBucketUsecase::with_context(Some(self.app.clone()));
        let lifecycle = buckets
            .acquire_bucket_handle_guard(&bucket)
            .await
            .map_err(|error| authorization_error(ApiError::other(error)))?;
        self.authorize_snapshot(expected, &snapshot, &lifecycle, actions).await
    }

    #[cfg(test)]
    pub(super) async fn authorize_actions_with_operation_guard(
        &self,
        catalog: &Catalog,
        guard: &VectorCatalogOperationGuard,
        actions: &[S3VectorsAction],
    ) -> TableCatalogStoreResult<()> {
        self.check_admission(&self.identity).await?;
        guard.check_held()?;
        let snapshot = catalog
            .read_vector_maintenance_authorization_with_operation_guard(guard)
            .await?;
        let lifecycle = guard.lifecycle_guard()?;
        self.authorize_snapshot(&self.identity, &snapshot, &lifecycle, actions)
            .await?;
        guard.check_held()
    }

    pub(super) async fn authorize_actions_with_guard(
        &self,
        catalog: &Catalog,
        guard: &VectorMaintenanceDispatchGuard,
        actions: &[S3VectorsAction],
    ) -> TableCatalogStoreResult<()> {
        self.check_admission(&self.identity).await?;
        guard.check_held()?;
        if guard.lease().execution_identity() != &self.identity {
            return Err(authorization_error(ApiError::access_denied()));
        }
        let snapshot = catalog.read_vector_maintenance_authorization_with_guard(guard).await?;
        self.authorize_snapshot(&self.identity, &snapshot, &guard.lifecycle_owner(), actions)
            .await?;
        guard.check_held()
    }

    async fn authorize_snapshot(
        &self,
        expected: &VectorMaintenanceExecutionIdentityV1,
        snapshot: &VectorMaintenanceAuthorizationSnapshot,
        lifecycle: &crate::app::storage_api::managed::BucketIncarnationFenceGuard,
        actions: &[S3VectorsAction],
    ) -> TableCatalogStoreResult<()> {
        let bucket = snapshot
            .resource()
            .resource()
            .vector_bucket()
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance requires a Vector bucket".into()))?;
        let name = snapshot.resource().name();
        let buckets = DefaultBucketUsecase::with_context(Some(self.app.clone()));
        let metadata = buckets
            .read_managed_bucket_metadata(bucket, lifecycle)
            .await
            .map_err(|error| authorization_error(ApiError::other(error)))?;
        let facts = authorization_facts(bucket, &metadata).map_err(|error| authorization_error(ApiError::other(error)))?;
        let (current, credential) = resolve_identity(&self.app, expected.account())
            .await
            .map_err(authorization_error)?;
        if current.user_generation() != expected.user_generation() {
            return Err(authorization_error(ApiError::access_denied()));
        }
        let mut conditions = identity_conditions(&self.app, &credential).map_err(authorization_error)?;
        conditions.insert("RequestedRegion".into(), vec![bucket.region().into()]);
        for (key, value) in facts.tags {
            conditions.insert(format!("VectorBucketTag/{key}"), vec![value]);
        }
        for (key, value) in &snapshot.definition().tags {
            conditions.insert(format!("ResourceTag/{key}"), vec![value.clone()]);
        }
        for action in actions.iter().copied() {
            let args = Args {
                account: &credential.access_key,
                groups: &credential.groups,
                action: Action::S3VectorsAction(action),
                bucket: bucket.public_name().as_str(),
                object: name.as_str(),
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
                .map_err(|error| authorization_error(ApiError::from(error)))?
            {
                return Err(authorization_error(ApiError::access_denied()));
            }
        }
        Ok(())
    }

    async fn advance_resource(
        &self,
        catalog: &Catalog,
        active: &mut VectorMaintenanceActiveBucketCursor,
        budget: &VectorMaintenanceDiscoveryBudget,
    ) -> TableCatalogStoreResult<Advance> {
        let one = NonZeroU16::new(1).ok_or_else(|| TableCatalogStoreError::Invalid("invalid discovery size".into()))?;
        if active.current_resource.is_none()
            && active.resources.is_none()
            && active.resource_stage == VectorMaintenanceResourceStage::OrphanGc
        {
            return Ok(Advance::BucketComplete);
        }
        if active.current_resource.is_none() {
            let (mut resources, next) = catalog
                .list_vector_maintenance_resources(&active.bucket, active.resources.as_ref(), one, budget)
                .await?
                .into_parts();
            active.resources = next;
            let Some(resource) = resources.pop() else {
                return Ok(if active.resources.is_some() {
                    Advance::Continue
                } else {
                    Advance::BucketComplete
                });
            };
            active.current_resource = Some(resource);
            active.resource_stage = VectorMaintenanceResourceStage::ExistingJobs;
        }
        if active.resource_stage == VectorMaintenanceResourceStage::Recovery
            && let Some(previous) = active.current_resource.as_ref()
            && previous.state() == VectorMaintenanceResourceState::Deleted
            && previous.maintenance().is_none()
        {
            // Enrollment replaces the delete intent while this cursor retains its earlier snapshot.
            // Reserve two lock acquisitions, one identity check, and at most three bounded reads.
            if !(0..6).all(|_| budget.try_start_call()) {
                return Ok(Advance::Continue);
            }
            active.current_resource = Some(
                catalog
                    .read_vector_maintenance_resource(previous.resource(), previous.namespace(), previous.name())
                    .await?,
            );
        }
        let observed = active
            .current_resource
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("missing discovery resource".into()))?;
        let resource = VectorMaintenanceResourceV1::try_new(observed.resource(), observed.namespace(), observed.name())?;
        use VectorMaintenanceResourceStage as Stage;
        let target = match active.resource_stage {
            Stage::ExistingJobs => {
                let (jobs, next) = catalog
                    .list_vector_maintenance_jobs(observed.resource(), active.jobs.as_ref(), one, budget)
                    .await?
                    .into_parts();
                active.jobs = next;
                if active.jobs.is_none() {
                    active.resource_stage = Stage::Preparation;
                }
                match jobs.into_iter().next() {
                    Some(job) => {
                        let expired = if let Some(lease) = job.job().lease() {
                            let expires = OffsetDateTime::parse(lease.expires_at(), &Rfc3339)
                                .map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
                            expires <= OffsetDateTime::now_utc()
                        } else {
                            false
                        };
                        if *job.job().kind() == VectorMaintenanceKind::DeleteRecovery
                            && job
                                .job()
                                .input()
                                .source()
                                .as_ref()
                                .is_some_and(|source| *source.kind() == VectorMaintenanceSourceKind::RetirementAuthorization)
                            && (matches!(
                                job.job().state(),
                                VectorMaintenanceJobState::RecoveryRequired | VectorMaintenanceJobState::ManualReview
                            ) || (*job.job().state() == VectorMaintenanceJobState::Running && expired))
                        {
                            Some(VectorMaintenanceTarget::InspectRetirementRecovery {
                                resource,
                                job_id: *job.job().job_id(),
                                expected_revision: *job.job().revision(),
                            })
                        } else if matches!(job.job().kind(), VectorMaintenanceKind::AnnBuild | VectorMaintenanceKind::Compaction)
                            && (*job.job().state() == VectorMaintenanceJobState::RecoveryRequired
                                || (*job.job().state() == VectorMaintenanceJobState::Running && expired))
                        {
                            Some(VectorMaintenanceTarget::PrepareRecovery {
                                resource,
                                kind: VectorMaintenanceKind::CommitRecovery,
                                expected_source: VectorMaintenanceSourceV1::try_new(
                                    VectorMaintenanceSourceKind::MaintenanceJob,
                                    job.observed_object().into(),
                                )?,
                            })
                        } else if matches!(
                            job.job().state(),
                            VectorMaintenanceJobState::Queued
                                | VectorMaintenanceJobState::Running
                                | VectorMaintenanceJobState::RetryScheduled
                        ) {
                            Some(VectorMaintenanceTarget::ExistingJob {
                                resource,
                                job_id: *job.job().job_id(),
                            })
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            }
            Stage::Preparation => {
                active.resource_stage = Stage::Recovery;
                let kind = match observed.state() {
                    VectorMaintenanceResourceState::Active if observed.head().is_some_and(|head| *head.entry_version() == 3) => {
                        Some(VectorMaintenancePreparationKind::Current)
                    }
                    VectorMaintenanceResourceState::Deleted | VectorMaintenanceResourceState::Deleting
                        if observed.maintenance().is_none() =>
                    {
                        Some(VectorMaintenancePreparationKind::Deleted)
                    }
                    _ => None,
                };
                kind.map(|kind| VectorMaintenanceTarget::Preparation {
                    resource,
                    kind,
                    expected: observed.observed_object().into(),
                })
            }
            Stage::Recovery => {
                active.resource_stage = Stage::Retirements;
                let kind = match observed.state() {
                    VectorMaintenanceResourceState::Pending => {
                        Some((VectorMaintenanceKind::CreateRecovery, VectorMaintenanceSourceKind::CreateIntent))
                    }
                    VectorMaintenanceResourceState::Deleting | VectorMaintenanceResourceState::Deleted => {
                        Some((VectorMaintenanceKind::DeleteRecovery, VectorMaintenanceSourceKind::DeleteIntent))
                    }
                    VectorMaintenanceResourceState::Active => None,
                };
                match (kind, observed.source_intent()) {
                    (Some((kind, source_kind)), Some(reference)) => Some(VectorMaintenanceTarget::PrepareRecovery {
                        resource,
                        kind,
                        expected_source: VectorMaintenanceSourceV1::try_new(source_kind, reference.into())?,
                    }),
                    _ => None,
                }
            }
            Stage::Retirements => {
                if observed.maintenance().is_none() {
                    active.resource_stage = Stage::Ann;
                    None
                } else {
                    let (authorities, next) = catalog
                        .list_vector_retirement_authorities(
                            observed.resource(),
                            observed.namespace(),
                            observed.name(),
                            active.retirements.as_ref(),
                            one,
                            budget,
                        )
                        .await?
                        .into_parts();
                    active.retirements = next;
                    if active.retirements.is_none() {
                        active.resource_stage = Stage::Ann;
                    }
                    authorities
                        .into_iter()
                        .find(|authority| *authority.state() != Some(VectorRetirementProgressState::Completed))
                        .map(|authority| {
                            Ok(VectorMaintenanceTarget::PrepareRecovery {
                                resource,
                                kind: VectorMaintenanceKind::DeleteRecovery,
                                expected_source: VectorMaintenanceSourceV1::try_new(
                                    VectorMaintenanceSourceKind::RetirementAuthorization,
                                    authority.authorization_ref().into(),
                                )?,
                            })
                        })
                        .transpose()?
                }
            }
            Stage::Ann | Stage::Compaction => {
                let kind = if active.resource_stage == Stage::Ann {
                    VectorMaintenanceKind::AnnBuild
                } else {
                    VectorMaintenanceKind::Compaction
                };
                active.resource_stage = if active.resource_stage == Stage::Ann {
                    Stage::Compaction
                } else {
                    Stage::Gc
                };
                match observed
                    .head()
                    .filter(|head| *head.entry_version() == 4 && *head.state() == TableCatalogEntryState::Active)
                {
                    Some(head) => Some(VectorMaintenanceTarget::NewJob(Box::new(VectorMaintenanceSemanticSpec::try_new(
                        resource,
                        kind,
                        VectorMaintenanceOrigin::Periodic,
                        VectorMaintenanceInputV1::try_new(Some(head.clone()), None, None)?,
                        self.identity.clone(),
                        None,
                        self.policy.clone(),
                    )?))),
                    None => None,
                }
            }
            Stage::Gc | Stage::OrphanGc => {
                let kind = if active.resource_stage == Stage::Gc {
                    VectorMaintenanceKind::HistoryGc
                } else {
                    VectorMaintenanceKind::OrphanGc
                };
                let target =
                    observed
                        .head()
                        .filter(|_| observed.maintenance().is_some())
                        .map(|head| VectorMaintenanceTarget::PrepareGc {
                            resource,
                            kind,
                            expected_head: Box::new(head.clone()),
                        });
                if active.resource_stage == Stage::Gc {
                    active.resource_stage = Stage::OrphanGc;
                } else {
                    active.current_resource = None;
                    active.jobs = None;
                    active.retirements = None;
                    if target.is_none() && active.resources.is_none() {
                        return Ok(Advance::BucketComplete);
                    }
                }
                target
            }
        };
        Ok(target
            .map(|target| Advance::Target(Box::new(target)))
            .unwrap_or(Advance::Continue))
    }
}

fn authorization_error(source: ApiError) -> TableCatalogStoreError {
    TableCatalogStoreError::Io {
        classification: CatalogIoErrorKind::Denied,
        context: "authorize Vector maintenance preparation",
        source: Box::new(source),
    }
}
