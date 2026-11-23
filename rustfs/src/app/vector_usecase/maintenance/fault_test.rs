// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Private application maintenance driver for real process fault tests.

use super::*;
use crate::app::object::ManagedMaintenancePreparationPurpose;
use crate::app::storage_api::managed::{AccountNamespaceId, BucketKind, BucketPublicName};
use crate::table_catalog::test_support::catalog_write_faults::{
    CatalogWriteFaultAction, CatalogWriteFaultHandle, CatalogWriteFaultPhase,
};
use crate::table_catalog::*;
use crate::vector::lance::{DatasetError, LanceManifestError};
use futures::FutureExt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU64};
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_OPERATIONS: usize = 256;
const MAX_INSPECTION_CURSORS: usize = 32;
const JOIN_OBSERVATION_WINDOW: Duration = Duration::from_millis(100);

#[path = "quota_observation.rs"]
mod quota_observation;
#[path = "sdk_observer.rs"]
mod sdk_observer;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintenanceOperationKind {
    Tick,
    Collect,
    Stop,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintenanceOperationStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct MaintenancePassObservation {
    pub(crate) discovered: u64,
    pub(crate) queued: u64,
    pub(crate) skipped: u64,
    pub(crate) started: u64,
    pub(crate) completed: u64,
    pub(crate) partial_recovered: u64,
    pub(crate) retry_pending: u64,
    pub(crate) manual_review: u64,
    pub(crate) failed: u64,
    pub(crate) first_error: Option<VectorMaintenanceFailureCode>,
}

impl From<VectorMaintenancePassReport> for MaintenancePassObservation {
    fn from(value: VectorMaintenancePassReport) -> Self {
        Self {
            discovered: value.discovered,
            queued: value.queued,
            skipped: value.skipped,
            started: value.started,
            completed: value.completed,
            partial_recovered: value.partial_recovered,
            retry_pending: value.retry_pending,
            manual_review: value.manual_review,
            failed: value.failed,
            first_error: value.first_error,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct MaintenanceOperationObservation {
    pub(crate) operation_token: String,
    pub(crate) kind: MaintenanceOperationKind,
    pub(crate) status: MaintenanceOperationStatus,
    pub(crate) started_at: String,
    pub(crate) completed_at: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) error_category: Option<VectorMaintenanceFailureCode>,
    pub(crate) pass: Option<MaintenancePassObservation>,
    pub(crate) joined: Vec<MaintenancePassObservation>,
    pub(crate) completion_observation: VectorMaintenanceCompletionSnapshot,
    pub(crate) scheduler_observation_complete: bool,
    pub(crate) has_in_flight: bool,
    pub(crate) has_failure: bool,
    pub(crate) has_fatal_failure: bool,
}

struct OperationOwner {
    observation: Arc<parking_lot::Mutex<MaintenanceOperationObservation>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct DriverState {
    operations: BTreeMap<String, Arc<OperationOwner>>,
    active_operation: Option<String>,
    stop_token: Option<String>,
}

#[derive(Clone)]
struct InspectionCursor {
    resource: VectorMaintenanceResourceV1,
    head: VectorMaintenanceHeadV1,
    jobs: VectorMaintenanceJobCursor,
}

#[derive(Debug, Serialize)]
pub(crate) struct MaintenanceJobObservation {
    pub(crate) observed_object: CatalogMetadataRefRecordV1,
    pub(crate) job: VectorMaintenanceJobV1,
    pub(crate) result_content_inspected: bool,
    pub(crate) evidence: VectorMaintenanceTestEvidence,
}

#[derive(Debug, Serialize)]
pub(crate) struct CommittedVersionObservation {
    pub(crate) metadata: CatalogMetadataRefRecordV1,
    pub(crate) descriptor: CatalogFormatDescriptorRecordV1,
    pub(crate) commit_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MaintenanceStageObservation {
    pub(crate) job_id: Uuid,
    pub(crate) attempt: u16,
    pub(crate) stage: &'static str,
    pub(crate) commit_id: String,
    pub(crate) commit: Option<CommitLogEntry>,
    pub(crate) observed_object: Option<CatalogMetadataRefRecordV1>,
    pub(crate) committed_history_contains_candidate: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResourceInspection {
    pub(crate) instance_id: Uuid,
    pub(crate) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) observed_at: String,
    pub(crate) current: VectorMaintenanceHeadV1,
    pub(crate) observed_object: CatalogMetadataRefRecordV1,
    pub(crate) history: Vec<CommittedVersionObservation>,
    pub(crate) references: Vec<CatalogMetadataRefRecordV1>,
    pub(crate) references_scope: &'static str,
    pub(crate) jobs: Vec<MaintenanceJobObservation>,
    pub(crate) stage_commits: Vec<MaintenanceStageObservation>,
    pub(crate) next_cursor: Option<String>,
}

pub(crate) struct MaintenanceWriteFaultRequest {
    pub(crate) job_id: Uuid,
    pub(crate) attempt: NonZeroU16,
    pub(crate) target: VectorMaintenanceTestWriteTarget,
    pub(crate) occurrence: NonZeroU16,
    pub(crate) phase: CatalogWriteFaultPhase,
    pub(crate) action: CatalogWriteFaultAction,
}

pub(crate) struct MaintenanceWrittenResultRequest {
    pub(crate) job_id: Uuid,
    pub(crate) attempt: NonZeroU16,
    pub(crate) content_sha256: String,
    pub(crate) content_length: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MaintenanceWrittenResultObservation {
    pub(crate) instance_id: Uuid,
    pub(crate) execution_identity: VectorMaintenanceExecutionIdentityV1,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) evidence: VectorMaintenanceTestWrittenResultObservation,
}

pub(crate) struct MaintenanceDriver {
    app: Arc<AppContext>,
    config: VectorMaintenanceRuntimeConfig,
    source: Arc<source::MaintenanceSource>,
    executor: Arc<MaintenanceExecutor>,
    scheduler: Mutex<VectorMaintenanceScheduler<super::super::Backend>>,
    completion_observer: Arc<VectorMaintenanceCompletionObserver>,
    cancellation: CancellationToken,
    state: parking_lot::Mutex<DriverState>,
    inspection_cursors: Mutex<BTreeMap<String, InspectionCursor>>,
    inspection_budgets: Arc<VectorMaintenanceBudgets>,
    sdk_candidates: Mutex<BTreeMap<Uuid, Arc<sdk_observer::SdkCandidateBinding>>>,
}

impl MaintenanceDriver {
    /// Admit the current ordinary account without changing the persisted server configuration.
    pub(crate) async fn admit(app: Arc<AppContext>, execution_account: &str) -> Result<Self, VectorMaintenanceExecutionError> {
        let config = VectorMaintenanceRuntimeConfig {
            enabled: true,
            execution_account: execution_account.into(),
            ..VectorMaintenanceRuntimeConfig::default()
        };
        config.validate()?;
        let (identity, _) = resolve_identity(&app, execution_account)
            .await
            .map_err(maintenance_execution_error)?;
        let execution_identity =
            VectorMaintenanceExecutionIdentityV1::try_new(identity.account().into(), *identity.user_generation())?;
        let backend = super::super::Backend::new_with_managed_candidate_reader(
            app.object_store(),
            Arc::new(super::super::dataset::CandidateReader::default()),
        );
        let policy = VectorMaintenancePolicyV1::try_new(
            config.lease_duration.as_secs(),
            config.heartbeat_interval.as_secs(),
            config.max_attempts.get(),
            config.retry_initial_backoff.as_secs(),
            config.retry_max_backoff.as_secs(),
        )?;
        let source = Arc::new(source::MaintenanceSource {
            app: app.clone(),
            backend: backend.clone(),
            identity: execution_identity.clone(),
            policy,
            gc_scan: parking_lot::Mutex::new(None),
        });
        source.check_admission(&execution_identity).await?;
        let limits = ManifestIoLimits {
            max_manifest_bytes: NonZeroU64::new(64 * 1024 * 1024).expect("positive manifest read limit"),
            max_concurrent_reads: config.io_concurrency,
        };
        let executor = Arc::new(MaintenanceExecutor::new(app.clone(), identity, limits, source.clone()));
        let context = VectorMaintenanceExecutionContext {
            identity: execution_identity,
            source: source.clone(),
            executor: executor.clone(),
        };
        let node_admission = crate::startup_background::background_node_admission();
        let inspection_budgets = VectorMaintenanceBudgets::new(&config, node_admission.clone())?;
        let mut scheduler = VectorMaintenanceScheduler::new(
            config.clone(),
            backend,
            context.source,
            context.executor,
            context.identity,
            node_admission,
        )?;
        let completion_observer =
            VectorMaintenanceCompletionObserver::new_for_instance(classify_completion_source, app.object_store().id);
        scheduler.install_completion_observer(completion_observer.clone())?;
        Ok(Self {
            app,
            config,
            source,
            executor,
            scheduler: Mutex::new(scheduler),
            completion_observer,
            cancellation: CancellationToken::new(),
            state: parking_lot::Mutex::new(DriverState::default()),
            inspection_cursors: Mutex::new(BTreeMap::new()),
            inspection_budgets,
            sdk_candidates: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn execution_identity(&self) -> &VectorMaintenanceExecutionIdentityV1 {
        &self.source.identity
    }

    /// The returned token observes a task owned independently of its IPC waiter.
    pub(crate) fn tick(self: &Arc<Self>) -> Result<String, VectorMaintenanceExecutionError> {
        self.start_serial_operation(MaintenanceOperationKind::Tick)
    }

    /// Observe existing scheduler owners without discovering or admitting new work.
    pub(crate) fn collect(self: &Arc<Self>) -> Result<String, VectorMaintenanceExecutionError> {
        self.start_serial_operation(MaintenanceOperationKind::Collect)
    }

    fn start_serial_operation(
        self: &Arc<Self>,
        kind: MaintenanceOperationKind,
    ) -> Result<String, VectorMaintenanceExecutionError> {
        let mut state = self.state.lock();
        if state.stop_token.is_some() || self.cancellation.is_cancelled() {
            return Err(conflict("maintenance driver is stopping"));
        }
        if state.active_operation.is_some() {
            return Err(conflict("maintenance driver already has an active operation"));
        }
        if state.operations.len() >= MAX_OPERATIONS {
            return Err(conflict("maintenance driver operation observation limit reached"));
        }
        let token = self.start_operation(&mut state, kind);
        state.active_operation = Some(token.clone());
        Ok(token)
    }

    pub(crate) fn poll(&self, operation_token: &str) -> Result<MaintenanceOperationObservation, VectorMaintenanceExecutionError> {
        let owner = self
            .state
            .lock()
            .operations
            .get(operation_token)
            .cloned()
            .ok_or_else(|| invalid("unknown maintenance operation token"))?;
        let observation = owner.observation.lock().clone();
        Ok(observation)
    }

    /// Cancellation of this waiter leaves the original drain and JoinHandle retained.
    pub(crate) async fn stop(self: &Arc<Self>) -> Result<MaintenanceOperationObservation, VectorMaintenanceExecutionError> {
        let owner = {
            let mut state = self.state.lock();
            let token = if let Some(token) = &state.stop_token {
                token.clone()
            } else {
                self.cancellation.cancel();
                let token = self.start_operation(&mut state, MaintenanceOperationKind::Stop);
                state.stop_token = Some(token.clone());
                token
            };
            state
                .operations
                .get(&token)
                .cloned()
                .ok_or_else(|| invalid("missing maintenance stop owner"))?
        };
        let mut task = owner.task.lock().await;
        if let Some(handle) = task.as_mut() {
            let result = handle.await;
            *task = None;
            result.map_err(VectorMaintenanceExecutionError::Join)?;
        }
        let observation = owner.observation.lock().clone();
        Ok(observation)
    }

    fn start_operation(self: &Arc<Self>, state: &mut DriverState, kind: MaintenanceOperationKind) -> String {
        let token = Uuid::new_v4().to_string();
        let observation = Arc::new(parking_lot::Mutex::new(MaintenanceOperationObservation {
            operation_token: token.clone(),
            kind,
            status: MaintenanceOperationStatus::Running,
            started_at: OffsetDateTime::now_utc().to_string(),
            completed_at: None,
            error: None,
            error_category: None,
            pass: None,
            joined: Vec::new(),
            completion_observation: self.completion_observer.snapshot(),
            scheduler_observation_complete: false,
            has_in_flight: false,
            has_failure: false,
            has_fatal_failure: false,
        }));
        let driver = self.clone();
        let output = observation.clone();
        let operation_token = token.clone();
        let task = tokio::spawn(async move {
            let result = AssertUnwindSafe(driver.run_operation(kind, &output)).catch_unwind().await;
            let mut state = driver.state.lock();
            if state.active_operation.as_deref() == Some(operation_token.as_str()) {
                state.active_operation = None;
            }
            {
                let mut current = output.lock();
                if result.is_err() {
                    current.error = Some("maintenance driver task panicked; scheduler ownership is retained".into());
                    current.error_category = Some(VectorMaintenanceFailureCode::UnknownOutcome);
                    current.has_failure = true;
                    current.completion_observation = driver.completion_observer.snapshot();
                }
                current.completed_at = Some(OffsetDateTime::now_utc().to_string());
                current.status = if current.error.is_some() || current.has_failure || current.has_fatal_failure {
                    MaintenanceOperationStatus::Failed
                } else {
                    MaintenanceOperationStatus::Completed
                };
            }
        });
        state.operations.insert(
            token.clone(),
            Arc::new(OperationOwner {
                observation,
                task: Mutex::new(Some(task)),
            }),
        );
        token
    }

    async fn run_operation(&self, kind: MaintenanceOperationKind, output: &parking_lot::Mutex<MaintenanceOperationObservation>) {
        let mut scheduler = self.scheduler.lock().await;
        match kind {
            MaintenanceOperationKind::Tick => {
                match scheduler
                    .run_pass(&self.cancellation, Instant::now() + self.config.pass_admission_budget)
                    .await
                {
                    Ok(report) => output.lock().pass = Some(report.into()),
                    Err(error) => {
                        let error = VectorMaintenanceExecutionError::from(error);
                        let mut current = output.lock();
                        current.error_category = Some(error.category());
                        current.error = Some(error.to_string());
                    }
                }
            }
            MaintenanceOperationKind::Collect => {}
            MaintenanceOperationKind::Stop => {
                let report = scheduler.drain().await;
                output.lock().pass = Some(report.into());
            }
        }
        if matches!(kind, MaintenanceOperationKind::Tick | MaintenanceOperationKind::Collect) {
            // Dropping only this bounded join observer keeps every original task in the scheduler.
            let deadline = Instant::now() + JOIN_OBSERVATION_WINDOW;
            while scheduler.has_in_flight() {
                match tokio::time::timeout_at(deadline, scheduler.join_next()).await {
                    Ok(Some(report)) => output.lock().joined.push(report.into()),
                    Ok(None) | Err(_) => break,
                }
            }
        }
        let mut current = output.lock();
        current.completion_observation = self.completion_observer.snapshot();
        current.scheduler_observation_complete = true;
        current.has_in_flight = scheduler.has_in_flight();
        current.has_failure = scheduler.has_failure();
        current.has_fatal_failure = scheduler.has_fatal_failure();
    }

    /// Resolves an existing bucket before an Index exists, using this runtime's current identity.
    pub(crate) async fn resolve_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<VectorBucketBindingV1, VectorMaintenanceExecutionError> {
        self.source.check_admission(&self.source.identity).await?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        let _memory = control.reserve_memory(8 * 1024 * 1024)?;
        let _io = control.acquire_io().await?;
        let region = self
            .app
            .region()
            .get()
            .map(|region| region.to_string())
            .unwrap_or_else(|| rustfs_config::RUSTFS_REGION.to_owned());
        let buckets = DefaultBucketUsecase::with_context(Some(self.app.clone()));
        let bucket = buckets
            .resolve_bucket_handle(
                BucketKind::Vector,
                &AccountNamespaceId::try_new(String::new()).map_err(maintenance_execution_error)?,
                &region,
                &BucketPublicName::try_new(bucket_name.into()).map_err(maintenance_execution_error)?,
            )
            .await
            .map_err(maintenance_execution_error)?;
        let guard = buckets
            .acquire_bucket_handle_guard(&bucket)
            .await
            .map_err(maintenance_execution_error)?;
        let metadata = buckets
            .read_managed_bucket_metadata(&bucket, &guard)
            .await
            .map_err(maintenance_execution_error)?;
        let facts = authorization_facts(&bucket, &metadata).map_err(maintenance_execution_error)?;
        let (current, credentials) = resolve_identity(&self.app, self.source.identity.account())
            .await
            .map_err(maintenance_execution_error)?;
        if current.user_generation() != self.source.identity.user_generation() {
            return Err(TableCatalogStoreError::Io {
                classification: CatalogIoErrorKind::Denied,
                context: "observe Vector bucket identity",
                source: Box::new(ApiError::access_denied()),
            }
            .into());
        }
        let request = crate::vector::protocol::Request::GetVectorBucket(crate::vector::protocol::dto::GetVectorBucketInput {
            vector_bucket_name: Some(bucket_name.into()),
            vector_bucket_arn: None,
        });
        crate::vector::access_core::authorize_with_iam(
            self.app.iam().handle().as_ref(),
            &credentials,
            false,
            &request,
            &crate::vector::access_core::VectorTarget::Bucket(&facts),
            &region,
            identity_conditions(&self.app, &credentials).map_err(maintenance_execution_error)?,
        )
        .await
        .map_err(|source| TableCatalogStoreError::Io {
            classification: CatalogIoErrorKind::Denied,
            context: "authorize Vector bucket identity observation",
            source: Box::new(source),
        })?;
        self.source.check_admission(&self.source.identity).await?;
        control.check_admission()?;
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("bucket observation lost lifecycle protection".into()).into());
        }
        Ok(VectorBucketBindingV1::from_handle(&bucket)?)
    }

    /// Captures the exact queued GC job before a query begins holding its protection.
    pub(crate) async fn read_gc_lock_selector(
        &self,
        resource: &VectorMaintenanceResourceV1,
        job_id: Uuid,
    ) -> Result<VectorMaintenanceGcLockSelector, VectorMaintenanceExecutionError> {
        resource.validate()?;
        self.authorize_inspection(resource).await?;
        let identity = resource.identity()?;
        let _permit = self.inspection_budgets.try_admit(&identity, VectorMaintenanceLane::Control)?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        let _memory = control.reserve_memory(16 * 1024 * 1024)?;
        let _io = control.acquire_io().await?;
        let bucket = resource.bucket().to_handle()?;
        let namespace = Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?;
        let (guard, _) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        if guard.resource_identity() != &identity {
            return Err(conflict("GC observation resource lifetime changed"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        let selector = catalog
            .read_vector_maintenance_test_gc_lock_with_guard(&guard, job_id)
            .await?;
        if selector.job.execution_identity() != &self.source.identity {
            return Err(conflict("GC observation belongs to another execution identity"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        control.check_admission()?;
        Ok(selector)
    }

    /// Reads the original observer without reacquiring the GC protection held by the task.
    pub(crate) async fn observe_gc_lock_acquisitions(
        &self,
    ) -> Result<VectorMaintenanceGcLockSnapshot, VectorMaintenanceExecutionError> {
        self.source.check_admission(&self.source.identity).await?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        let _memory = control.reserve_memory(4 * 1024 * 1024)?;
        let observation = self.completion_observer.gc_lock_snapshot();
        control.check_admission()?;
        Ok(observation)
    }

    pub(crate) async fn resolve_resource(
        &self,
        bucket_name: &str,
        index_name: &str,
    ) -> Result<VectorMaintenanceResourceV1, VectorMaintenanceExecutionError> {
        self.source.check_admission(&self.source.identity).await?;
        let region = self
            .app
            .region()
            .get()
            .map(|region| region.to_string())
            .unwrap_or_else(|| rustfs_config::RUSTFS_REGION.to_owned());
        let buckets = DefaultBucketUsecase::with_context(Some(self.app.clone()));
        let bucket = buckets
            .resolve_bucket_handle(
                BucketKind::Vector,
                &AccountNamespaceId::try_new(String::new()).map_err(maintenance_execution_error)?,
                &region,
                &BucketPublicName::try_new(bucket_name.into()).map_err(maintenance_execution_error)?,
            )
            .await
            .map_err(maintenance_execution_error)?;
        let namespace = Namespace::parse("indexes").map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(index_name).map_err(maintenance_execution_error)?;
        let catalog = Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?;
        let (guard, entry) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        let resource = VectorMaintenanceResourceV1::try_new(entry.head().resource_identity(), &namespace, &name)?;
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        Ok(resource)
    }

    async fn authorize_inspection(&self, resource: &VectorMaintenanceResourceV1) -> Result<(), VectorMaintenanceExecutionError> {
        self.source
            .authorize_actions(
                &self.source.identity,
                resource,
                &[S3VectorsAction::GetVectorsAction, S3VectorsAction::DeleteVectorsAction],
            )
            .await?;
        Ok(())
    }

    async fn authorize_inspection_with_guard(
        &self,
        catalog: &Catalog,
        guard: &VectorCatalogOperationGuard,
    ) -> Result<(), VectorMaintenanceExecutionError> {
        self.source
            .authorize_actions_with_operation_guard(
                catalog,
                guard,
                &[S3VectorsAction::GetVectorsAction, S3VectorsAction::DeleteVectorsAction],
            )
            .await?;
        Ok(())
    }

    /// Select only an actual persisted attempt on the installed Catalog backend.
    pub(crate) async fn arm_write_fault(
        &self,
        resource: &VectorMaintenanceResourceV1,
        request: MaintenanceWriteFaultRequest,
    ) -> Result<CatalogWriteFaultHandle, VectorMaintenanceExecutionError> {
        resource.validate()?;
        self.authorize_inspection(resource).await?;
        let identity = resource.identity()?;
        let _permit = self.inspection_budgets.try_admit(&identity, VectorMaintenanceLane::Control)?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        control.check_admission()?;
        let _memory = control.reserve_memory(8 * 1024 * 1024)?;
        let bucket = resource.bucket().to_handle()?;
        let namespace = Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?;
        let (guard, _) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        if guard.resource_identity() != &identity {
            return Err(conflict("fault target resource lifetime changed"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        let job = catalog
            .read_vector_maintenance_test_job_with_guard(&guard, request.job_id)
            .await?
            .ok_or_else(|| invalid("fault target job does not exist"))?;
        if job.job().execution_identity() != &self.source.identity
            || *job.job().state() != VectorMaintenanceJobState::Running
            || *job.job().attempt() != request.attempt.get()
        {
            return Err(conflict("fault target is not this identity's running attempt"));
        }
        control.check_admission()?;
        let handle = catalog
            .arm_vector_maintenance_test_write_fault_with_guard(
                &guard,
                &job,
                request.target,
                request.occurrence,
                request.phase,
                request.action,
            )
            .await?;
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        Ok(handle)
    }

    /// Read the original Result bytes without changing the job or observing a completion.
    pub(crate) async fn read_written_result(
        &self,
        resource: &VectorMaintenanceResourceV1,
        request: MaintenanceWrittenResultRequest,
    ) -> Result<MaintenanceWrittenResultObservation, VectorMaintenanceExecutionError> {
        resource.validate()?;
        self.authorize_inspection(resource).await?;
        let identity = resource.identity()?;
        let _permit = self.inspection_budgets.try_admit(&identity, VectorMaintenanceLane::Control)?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        control.check_admission()?;
        let _memory = control.reserve_memory(8 * 1024 * 1024)?;
        let bucket = resource.bucket().to_handle()?;
        let namespace = Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?;
        let (guard, _) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        if guard.resource_identity() != &identity {
            return Err(conflict("observed Result resource lifetime changed"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        let job = catalog
            .read_vector_maintenance_test_job_with_guard(&guard, request.job_id)
            .await?
            .ok_or_else(|| invalid("observed Result job does not exist"))?;
        if job.job().execution_identity() != &self.source.identity || *job.job().attempt() != request.attempt.get() {
            return Err(conflict("observed Result belongs to another identity or attempt"));
        }
        control.check_admission()?;
        let evidence = catalog
            .read_vector_maintenance_test_written_result_with_guard(&guard, &job, &request.content_sha256, request.content_length)
            .await?;
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        Ok(MaintenanceWrittenResultObservation {
            instance_id: self.app.object_store().id,
            execution_identity: self.source.identity.clone(),
            resource: resource.clone(),
            evidence,
        })
    }

    /// Inspect one exact live resource. Old identities cannot select a recreated name.
    pub(crate) async fn inspect_resource(
        &self,
        resource: &VectorMaintenanceResourceV1,
        cursor: Option<&str>,
    ) -> Result<ResourceInspection, VectorMaintenanceExecutionError> {
        resource.validate()?;
        let identity = resource.identity()?;
        let mut cursors = self.inspection_cursors.lock().await;
        let previous = cursor
            .map(|token| {
                cursors
                    .get(token)
                    .cloned()
                    .ok_or_else(|| invalid("unknown inspection cursor"))
            })
            .transpose()?;
        if previous.as_ref().is_some_and(|previous| previous.resource != *resource) {
            return Err(conflict("inspection cursor belongs to another resource"));
        }
        if previous.is_none() && cursors.len() >= MAX_INSPECTION_CURSORS {
            return Err(conflict("inspection cursor capacity reached"));
        }
        self.authorize_inspection(resource).await?;
        let _permit = self.inspection_budgets.try_admit(&identity, VectorMaintenanceLane::Control)?;
        let control = VectorMaintenanceControl::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.execution_admission_budget,
            self.inspection_budgets.clone(),
        );
        control.check_admission()?;
        let _evidence_memory = control.reserve_memory(64 * 1024 * 1024)?;
        let bucket = resource.bucket().to_handle()?;
        let namespace = Namespace::parse(resource.namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(resource.name()).map_err(maintenance_execution_error)?;
        let catalog = Arc::new(Catalog::for_vector_scope(self.source.backend.clone(), bucket.clone()).await?);
        let (guard, entry) = catalog.begin_vector_operation(&bucket, &namespace, &name).await?;
        if guard.resource_identity() != &identity {
            return Err(conflict("inspection resource lifetime changed"));
        }
        let guard = Arc::new(guard);
        let current = VectorMaintenanceHeadV1::from_entry(&entry)?;
        if previous.as_ref().is_some_and(|previous| previous.head != current) {
            return Err(conflict("inspection head changed between pages"));
        }
        let (catalog, adapter) = self
            .executor
            .preparation_adapter(
                catalog,
                guard.clone(),
                Uuid::new_v4(),
                ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
                control,
            )
            .await?;
        let resource_snapshot = catalog.read_vector_maintenance_resource_with_guard(&guard).await?;
        if resource_snapshot.head() != Some(&current) {
            return Err(conflict("inspection head changed before exact observation"));
        }
        let history = catalog.read_vector_maintenance_history_with_guard(&guard).await?;
        let references = adapter
            .collect_references(entry.head().metadata(), entry.head().format_descriptor())
            .await
            .map_err(maintenance_execution_error)?;
        let budget = VectorMaintenanceDiscoveryBudget::new(
            self.cancellation.child_token(),
            Instant::now() + self.config.pass_admission_budget,
            self.config.discovery_calls_per_pass,
        );
        let (jobs, next) = catalog
            .list_vector_maintenance_jobs(
                &identity,
                previous.as_ref().map(|previous| &previous.jobs),
                NonZeroU16::new(8).expect("positive job inspection page size"),
                &budget,
            )
            .await?
            .into_parts();
        let mut stage_commits = Vec::new();
        let mut observed_jobs = Vec::with_capacity(jobs.len());
        for snapshot in &jobs {
            if !budget.try_start_call() {
                return Err(TableCatalogStoreError::Unavailable("job evidence inspection budget exhausted".into()).into());
            }
            let evidence = catalog
                .inspect_vector_maintenance_evidence_with_guard(&guard, snapshot)
                .await?;
            if evidence.resource_object != CatalogMetadataRefRecordV1::from(resource_snapshot.observed_object()) {
                return Err(conflict("inspection resource changed before job evidence observation"));
            }
            observed_jobs.push(MaintenanceJobObservation {
                observed_object: snapshot.observed_object().into(),
                job: snapshot.job().clone(),
                result_content_inspected: evidence.result.is_some(),
                evidence,
            });
            let job = snapshot.job();
            if *job.attempt() == 0 {
                continue;
            }
            let stages: &[&str] = match job.kind() {
                VectorMaintenanceKind::AnnBuild => &["ann-build"],
                VectorMaintenanceKind::Compaction => &["reserve-fragments", "rewrite"],
                _ => &[],
            };
            for &stage in stages {
                if !budget.try_start_call() {
                    return Err(TableCatalogStoreError::Unavailable("stage inspection budget exhausted".into()).into());
                }
                let commit_id = vector_maintenance_stage_commit_id(*job.job_id(), *job.attempt(), stage)?;
                let observed_commit = catalog
                    .read_vector_maintenance_test_stage_commit_with_guard(&guard, snapshot, stage)
                    .await?;
                let (commit, observed_object) = match observed_commit {
                    Some((commit, reference)) => (Some(commit), Some(CatalogMetadataRefRecordV1::from(&reference))),
                    None => (None, None),
                };
                let committed_history_contains_candidate = if let Some(commit) = &commit {
                    let binding = commit
                        .vector_commit_binding
                        .as_ref()
                        .ok_or_else(|| invalid("maintenance stage has no Vector binding"))?;
                    if commit.commit_id != commit_id.as_str()
                        || &binding.bucket != resource.bucket()
                        || binding.table_id != *resource.table_id()
                    {
                        return Err(conflict("maintenance stage belongs to another resource"));
                    }
                    history.iter().any(|version| {
                        version.commit_id() == Some(&commit_id)
                            && CatalogMetadataRefRecordV1::from(version.metadata()) == binding.candidate_metadata
                            && CatalogFormatDescriptorRecordV1::from(version.format_descriptor()) == binding.descriptor
                    })
                } else {
                    false
                };
                stage_commits.push(MaintenanceStageObservation {
                    job_id: *job.job_id(),
                    attempt: *job.attempt(),
                    stage,
                    commit_id: commit_id.as_str().into(),
                    commit,
                    observed_object,
                    committed_history_contains_candidate,
                });
            }
        }
        let after = catalog.read_vector_maintenance_resource_with_guard(&guard).await?;
        if after.head() != Some(&current) || after.observed_object() != resource_snapshot.observed_object() {
            return Err(conflict("inspection head changed during reference observation"));
        }
        self.authorize_inspection_with_guard(&catalog, &guard).await?;
        guard.check_held()?;
        let next_cursor = next.map(|jobs| {
            let token = Uuid::new_v4().to_string();
            cursors.insert(
                token.clone(),
                InspectionCursor {
                    resource: resource.clone(),
                    head: current.clone(),
                    jobs,
                },
            );
            token
        });
        if let Some(cursor) = cursor {
            cursors.remove(cursor);
        }
        Ok(ResourceInspection {
            instance_id: self.app.object_store().id,
            execution_identity: self.source.identity.clone(),
            resource: resource.clone(),
            observed_at: OffsetDateTime::now_utc().to_string(),
            current,
            observed_object: resource_snapshot.observed_object().into(),
            history: history
                .iter()
                .map(|version| CommittedVersionObservation {
                    metadata: version.metadata().into(),
                    descriptor: version.format_descriptor().into(),
                    commit_id: version.commit_id().map(|id| id.as_str().into()),
                })
                .collect(),
            references: references.iter().map(Into::into).collect(),
            references_scope: "current_and_retained_committed_history",
            jobs: observed_jobs,
            stage_commits,
            next_cursor,
        })
    }
}

fn classify_completion_source(error: &(dyn std::error::Error + 'static)) -> Option<VectorMaintenanceObservedSourceKind> {
    let dataset = error.downcast_ref::<DatasetError>()?;
    match dataset {
        DatasetError::Manifest(LanceManifestError::ExpectedHeadConflict) => {
            Some(VectorMaintenanceObservedSourceKind::ExpectedHeadConflict)
        }
        DatasetError::Engine(_) if dataset.is_definite_conflict() => {
            Some(VectorMaintenanceObservedSourceKind::EngineCommitConflict)
        }
        _ => None,
    }
}

fn invalid(message: &str) -> VectorMaintenanceExecutionError {
    TableCatalogStoreError::Invalid(message.into()).into()
}

fn conflict(message: &str) -> VectorMaintenanceExecutionError {
    TableCatalogStoreError::Conflict(message.into()).into()
}

#[cfg(test)]
mod bucket_observation_tests {
    use super::*;
    use crate::app::gating_test_env::run_large_stack_test;
    use crate::app::vector_usecase::tests::Fixture;
    use crate::vector::access_core::VectorAccessError;
    use crate::vector::protocol::Request;
    use serde_json::json;
    use std::error::Error;

    #[test]
    #[serial_test::serial]
    fn bucket_observation_authorizes_before_index_and_reloads_policy() {
        run_large_stack_test("vector-bucket-observation", || async {
            let fixture = Fixture::new().await;
            let name = format!("observe-{}", Uuid::new_v4().simple());
            let response = fixture
                .execute(Request::CreateVectorBucket(
                    serde_json::from_value(json!({"vectorBucketName":name})).unwrap(),
                ))
                .await
                .unwrap();
            let response: serde_json::Value = serde_json::from_slice(&response.body_bytes().unwrap()).unwrap();
            let arn = response["vectorBucketArn"].as_str().unwrap();
            let driver = MaintenanceDriver::admit(fixture.app.clone(), &fixture.credentials.access_key)
                .await
                .unwrap();
            let observed = driver.resolve_bucket(&name).await.unwrap();
            assert_eq!(observed.to_handle().unwrap().public_name().as_str(), name);
            fixture
                .execute(Request::CreateIndex(
                    serde_json::from_value(json!({
                        "vectorBucketName":name,"indexName":"observed.index","dataType":"float32",
                        "dimension":2,"distanceMetric":"euclidean"
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();
            let resource = driver.resolve_resource(&name, "observed.index").await.unwrap();
            assert_eq!(&observed, resource.bucket());
            let policy = json!({"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":arn
            }]})
            .to_string();
            fixture
                .execute(Request::PutVectorBucketPolicy(
                    serde_json::from_value(json!({"vectorBucketName":name,"policy":policy})).unwrap(),
                ))
                .await
                .unwrap();
            let error = driver.resolve_bucket(&name).await.unwrap_err();
            let mut source: Option<&(dyn Error + 'static)> = Some(&error);
            let mut denied = false;
            while let Some(current) = source {
                denied |= matches!(current.downcast_ref::<VectorAccessError>(), Some(VectorAccessError::Denied));
                source = current.source();
            }
            assert!(denied, "current bucket policy must deny the original ordinary identity: {error:?}");
        });
    }
}
