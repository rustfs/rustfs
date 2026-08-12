// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::object_store_from_extensions;
use crate::admin::storage_api::bucket::is_reserved_or_invalid_bucket;
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::lifecycle::{
    ManualTransitionCancelCheck, ManualTransitionJobRecord, ManualTransitionJobState, ManualTransitionProgressSink,
    ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport, ManualTransitionScopeAdmission,
    ManualTransitionScopeAdmissionClaim, TransitionOperatorDeleteResult, TransitionOperatorError,
    claim_manual_transition_scope_admission, delete_manual_transition_scope_admission_if_current,
    delete_transition_candidate_for_operator, enqueue_transition_for_existing_objects_scoped,
    finalize_missing_transition_transaction_for_operator, inspect_transition_transaction_for_operator,
    load_manual_transition_job_record, load_manual_transition_scope_admission, manual_transition_job_lease_expired,
    manual_transition_queue_snapshot, manual_transition_scope_admission_lease_expired,
    persist_manual_transition_job_progress_if_owned, renew_manual_transition_job_lease_if_owned,
    request_manual_transition_job_cancel, save_manual_transition_job_record, update_manual_transition_job_record,
};
use crate::admin::storage_api::runtime::ECStore;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::{
    MaskedAccessKey,
    http::{AMZ_REQUEST_ID, REQUEST_ID_HEADER},
};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration as StdDuration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

const JSON_CONTENT_TYPE: &str = "application/json";
const DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS: u64 = 10_000;
const MAX_MANUAL_TRANSITION_OBJECTS: u64 = 100_000;
const MAX_MANUAL_TRANSITION_DURATION_SECONDS: u64 = 3600;
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ILM_TRANSITION: &str = "ilm_transition";
const EVENT_ADMIN_ILM_TRANSITION_STATE: &str = "admin_ilm_transition_state";
const EVENT_ADMIN_ILM_TRANSITION_RECONCILE: &str = "admin_ilm_transition_reconcile";

static ACTIVE_MANUAL_TRANSITION_SCOPES: OnceLock<Mutex<Vec<ManualTransitionRunScope>>> = OnceLock::new();
#[cfg(feature = "e2e-test-hooks")]
const E2E_MANUAL_TRANSITION_CANCEL_BARRIER_ENV: &str = "RUSTFS_E2E_MANUAL_TRANSITION_CANCEL_BARRIER";
static ACTIVE_MANUAL_TRANSITION_JOBS: OnceLock<Mutex<HashMap<Uuid, CancellationToken>>> = OnceLock::new();
static MANUAL_TRANSITION_OWNER_ID: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManualTransitionRunScope {
    bucket: String,
    prefix: String,
    tier: Option<String>,
    dry_run: bool,
}

impl ManualTransitionRunScope {
    fn new(bucket: &str, options: &ManualTransitionRunOptions) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: options.prefix.clone(),
            tier: options.tier.as_ref().map(|tier| tier.to_ascii_uppercase()),
            dry_run: options.dry_run,
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.dry_run == other.dry_run
            && prefixes_overlap(&self.prefix, &other.prefix)
            && match (self.tier.as_deref(), other.tier.as_deref()) {
                (Some(left), Some(right)) => left == right,
                _ => true,
            }
    }
}

#[derive(Debug)]
struct ManualTransitionRunAdmission {
    scope: ManualTransitionRunScope,
}

impl Drop for ManualTransitionRunAdmission {
    fn drop(&mut self) {
        let mut scopes = lock_active_manual_transition_scopes();
        if let Some(index) = scopes.iter().position(|scope| scope == &self.scope) {
            scopes.swap_remove(index);
        }
    }
}

fn active_manual_transition_scopes() -> &'static Mutex<Vec<ManualTransitionRunScope>> {
    ACTIVE_MANUAL_TRANSITION_SCOPES.get_or_init(|| Mutex::new(Vec::new()))
}

fn lock_active_manual_transition_scopes() -> MutexGuard<'static, Vec<ManualTransitionRunScope>> {
    match active_manual_transition_scopes().lock() {
        Ok(scopes) => scopes,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn prefixes_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn manual_transition_already_running_error() -> S3Error {
    s3_error!(
        OperationAborted,
        "manual transition run already active for this bucket, prefix, tier, and dry-run mode"
    )
}

fn acquire_manual_transition_admission(scope: ManualTransitionRunScope) -> S3Result<ManualTransitionRunAdmission> {
    let mut scopes = lock_active_manual_transition_scopes();
    if scopes.iter().any(|active| active.overlaps(&scope)) {
        return Err(manual_transition_already_running_error());
    }
    scopes.push(scope.clone());
    Ok(ManualTransitionRunAdmission { scope })
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ManualTransitionRunQuery {
    bucket: Option<String>,
    prefix: Option<String>,
    marker: Option<String>,
    #[serde(rename = "versionMarker")]
    version_marker: Option<String>,
    #[serde(rename = "continuationToken")]
    continuation_token: Option<String>,
    tier: Option<String>,
    #[serde(rename = "async")]
    async_mode: Option<bool>,
    mode: Option<String>,
    #[serde(rename = "dryRun")]
    dry_run: Option<bool>,
    #[serde(rename = "maxObjects")]
    max_objects: Option<u64>,
    #[serde(rename = "maxDurationSeconds")]
    max_duration_seconds: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ManualTransitionRunResponse {
    state: &'static str,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancel_endpoint: Option<String>,
    report: ManualTransitionRunReport,
}

#[derive(Debug, Serialize)]
struct ManualTransitionJobResponse {
    status: ManualTransitionJobState,
    mode: &'static str,
    job_id: String,
    status_endpoint: String,
    cancel_endpoint: String,
    cancel_requested: bool,
    bucket: String,
    prefix: String,
    tier: Option<String>,
    dry_run: bool,
    created_at_unix_nanos: i128,
    updated_at_unix_nanos: i128,
    completed_at_unix_nanos: Option<i128>,
    report: ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
    failure_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ManualTransitionJobConflictResponse {
    state: &'static str,
    mode: &'static str,
    active_job_id: String,
    status_endpoint: String,
    cancel_endpoint: String,
    scope_key: String,
}

pub fn register_ilm_transition_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/run").as_str(),
        AdminOperation(&ManualTransitionRunHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/jobs/{{job_id}}").as_str(),
        AdminOperation(&ManualTransitionJobStatusHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/jobs/{{job_id}}").as_str(),
        AdminOperation(&ManualTransitionJobCancelHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/reconcile/{{transaction_id}}").as_str(),
        AdminOperation(&TransitionReconcileInspectHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/reconcile/{{transaction_id}}").as_str(),
        AdminOperation(&TransitionReconcileApplyHandler {}),
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualTransitionRunMode {
    EnqueueOnly,
    Async,
}

fn parse_manual_transition_query(query: Option<&str>) -> S3Result<(String, ManualTransitionRunOptions, ManualTransitionRunMode)> {
    let query: ManualTransitionRunQuery = match query {
        Some(query) => serde_urlencoded::from_bytes(query.as_bytes())
            .map_err(|_| s3_error!(InvalidArgument, "invalid manual transition query"))?,
        None => ManualTransitionRunQuery::default(),
    };

    let bucket = query
        .bucket
        .as_deref()
        .map(str::trim)
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "bucket is required"))?;
    if is_reserved_or_invalid_bucket(bucket, false) {
        return Err(s3_error!(InvalidBucketName, "invalid bucket name"));
    }

    let mode = query.mode.as_deref().map(str::trim).filter(|mode| !mode.is_empty());
    if matches!(
        (query.async_mode, mode),
        (Some(true), Some("enqueue_only")) | (Some(false), Some("async"))
    ) {
        return Err(s3_error!(InvalidArgument, "conflicting manual transition mode"));
    }
    if mode.is_some_and(|mode| mode != "enqueue_only" && mode != "async") {
        return Err(s3_error!(InvalidArgument, "unsupported manual transition mode"));
    }
    let run_mode = if query.async_mode == Some(true) || mode == Some("async") {
        ManualTransitionRunMode::Async
    } else {
        ManualTransitionRunMode::EnqueueOnly
    };
    if query.continuation_token.is_some() && (query.marker.is_some() || query.version_marker.is_some()) {
        return Err(s3_error!(
            InvalidArgument,
            "continuationToken cannot be combined with marker or versionMarker"
        ));
    }

    let max_objects = query.max_objects.unwrap_or(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS);
    if max_objects == 0 || max_objects > MAX_MANUAL_TRANSITION_OBJECTS {
        return Err(s3_error!(InvalidArgument, "maxObjects is outside the allowed range"));
    }
    if query
        .max_duration_seconds
        .is_some_and(|duration| duration == 0 || duration > MAX_MANUAL_TRANSITION_DURATION_SECONDS)
    {
        return Err(s3_error!(InvalidArgument, "maxDurationSeconds is outside the allowed range"));
    }

    Ok((
        bucket.to_string(),
        ManualTransitionRunOptions {
            prefix: query.prefix.unwrap_or_default(),
            marker: query.marker.filter(|marker| !marker.is_empty()),
            version_marker: query.version_marker.filter(|version_marker| !version_marker.is_empty()),
            continuation_token: query.continuation_token.filter(|token| !token.is_empty()),
            tier: query.tier.map(|tier| tier.trim().to_string()).filter(|tier| !tier.is_empty()),
            dry_run: query.dry_run.unwrap_or(false),
            max_objects: Some(max_objects),
            max_duration: query.max_duration_seconds.map(std::time::Duration::from_secs),
            job_id: None,
            cancel_token: None,
            cancel_check: None,
            progress_sink: None,
        },
        run_mode,
    ))
}

fn admin_request_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(REQUEST_ID_HEADER)
        .or_else(|| headers.get(AMZ_REQUEST_ID))
        .and_then(|value| value.to_str().ok())
}

fn admin_remote_addr(req: &S3Request<Body>) -> Option<String> {
    req.extensions
        .get::<Option<RemoteAddr>>()
        .and_then(|opt| opt.map(|addr| addr.0.to_string()))
}

fn log_manual_transition_rejected(reason: &str, request_id: &str, actor: &str, remote_addr: &str) {
    warn!(
        event = EVENT_ADMIN_ILM_TRANSITION_STATE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
        operation = "manual_transition_run",
        result = "rejected",
        reason,
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        "admin manual ILM transition request rejected"
    );
}

fn log_manual_transition_failed(reason: &str, request_id: &str, actor: &str, remote_addr: &str, err: &dyn std::fmt::Display) {
    error!(
        event = EVENT_ADMIN_ILM_TRANSITION_STATE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
        operation = "manual_transition_run",
        result = "failed",
        reason,
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        error = %err,
        "admin manual ILM transition request failed"
    );
}

fn log_manual_transition_completed(
    state: &str,
    request_id: &str,
    actor: &str,
    remote_addr: &str,
    max_objects: Option<u64>,
    max_duration_seconds: Option<u64>,
    report: &ManualTransitionRunReport,
) {
    info!(
        event = EVENT_ADMIN_ILM_TRANSITION_STATE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
        operation = "manual_transition_run",
        result = "success",
        state,
        mode = "enqueue_only",
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        bucket = %report.bucket,
        prefix = %report.prefix,
        tier = report.tier.as_deref().unwrap_or_default(),
        dry_run = report.dry_run,
        max_objects = max_objects.unwrap_or_default(),
        max_duration_seconds = max_duration_seconds.unwrap_or_default(),
        lifecycle_config_found = report.lifecycle_config_found,
        scanned = report.scanned,
        eligible = report.eligible,
        enqueued = report.enqueued,
        dry_run_eligible = report.dry_run_eligible,
        skipped_not_transition = report.skipped_not_transition,
        skipped_tier = report.skipped_tier,
        skipped_delete_marker = report.skipped_delete_marker,
        skipped_directory = report.skipped_directory,
        skipped_replication = report.skipped_replication,
        skipped_already_transitioned = report.skipped_already_transitioned,
        skipped_already_in_flight = report.skipped_already_in_flight,
        skipped_queue_full = report.skipped_queue_full,
        skipped_queue_closed = report.skipped_queue_closed,
        skipped_queue_timeout = report.skipped_queue_timeout,
        truncated_by_limit = report.truncated_by_limit,
        truncated_by_duration = report.truncated_by_duration,
        "admin manual ILM transition request completed"
    );
}

async fn authorize_manual_transition_request(req: &S3Request<Body>) -> S3Result<String> {
    authorize_transition_admin_request(req, AdminAction::SetTierAction).await
}

async fn authorize_transition_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<String> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let actor = MaskedAccessKey(&input_cred.access_key).to_string();

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req
        .extensions
        .get::<Option<RemoteAddr>>()
        .and_then(|opt| opt.map(|addr| addr.0));

    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(actor)
}

fn transition_transaction_id_from_params(params: &Params<'_, '_>) -> S3Result<Uuid> {
    Uuid::parse_str(params.get("transaction_id").unwrap_or(""))
        .map_err(|_| s3_error!(InvalidArgument, "invalid transition transaction id"))
}

fn map_transition_operator_error(err: TransitionOperatorError) -> S3Error {
    match err {
        TransitionOperatorError::NotFound => s3_error!(NoSuchKey, "transition transaction not found"),
        TransitionOperatorError::NotExpired => {
            s3_error!(OperationAborted, "transition transaction is still inside its active ownership window")
        }
        TransitionOperatorError::InvalidState(_) => {
            s3_error!(OperationAborted, "transition transaction is not eligible for operator reconciliation")
        }
        TransitionOperatorError::RemoteVersionRequired => {
            s3_error!(InvalidArgument, "an exact non-empty remote version is required")
        }
        TransitionOperatorError::CandidateNotMissing(_) => {
            s3_error!(OperationAborted, "remote candidate is not proven missing")
        }
        TransitionOperatorError::CandidateVersionMismatch { .. } => {
            s3_error!(OperationAborted, "remote candidate version does not match requested exact version")
        }
        TransitionOperatorError::Store(_) | TransitionOperatorError::Remote(_) => {
            s3_error!(InternalError, "transition reconciliation failed")
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransitionReconcileAction {
    DeleteCandidate,
    FinalizeMissing,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionReconcileRequest {
    action: TransitionReconcileAction,
    confirm: bool,
    #[serde(default)]
    remote_version_id: Option<String>,
}

enum ValidatedTransitionReconcileAction<'a> {
    DeleteCandidate(&'a str),
    FinalizeMissing,
}

fn validate_transition_reconcile_request(
    request: &TransitionReconcileRequest,
) -> S3Result<ValidatedTransitionReconcileAction<'_>> {
    if !request.confirm {
        return Err(s3_error!(
            InvalidRequest,
            "transition reconciliation requires confirm=true; use GET to inspect without changes"
        ));
    }
    match request.action {
        TransitionReconcileAction::DeleteCandidate => request
            .remote_version_id
            .as_deref()
            .filter(|version_id| !version_id.is_empty())
            .map(ValidatedTransitionReconcileAction::DeleteCandidate)
            .ok_or_else(|| s3_error!(InvalidArgument, "delete_candidate requires remote_version_id")),
        TransitionReconcileAction::FinalizeMissing if request.remote_version_id.is_none() => {
            Ok(ValidatedTransitionReconcileAction::FinalizeMissing)
        }
        TransitionReconcileAction::FinalizeMissing => {
            Err(s3_error!(InvalidArgument, "finalize_missing must not include remote_version_id"))
        }
    }
}

#[derive(Debug, Serialize)]
struct TransitionCandidateDeleteResponse {
    outcome: &'static str,
    result: TransitionOperatorDeleteResult,
}

#[derive(Debug, Serialize)]
struct TransitionFinalizeMissingResponse {
    outcome: &'static str,
    journal_retained: bool,
    transaction_id: Uuid,
}

fn log_transition_reconcile_applied(
    transaction_id: Uuid,
    action: &str,
    outcome: &str,
    request_id: &str,
    actor: &str,
    remote_addr: &str,
) {
    info!(
        event = EVENT_ADMIN_ILM_TRANSITION_RECONCILE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
        operation = "transition_operator_reconcile",
        transaction_id = %transaction_id,
        action,
        outcome,
        request_id = %request_id,
        actor = %actor,
        remote_addr = %remote_addr,
        "admin transition reconciliation applied"
    );
}

fn response_state(report: &ManualTransitionRunReport) -> &'static str {
    if report.was_truncated() || report.has_partial_enqueue() || report.tier_failure > 0 || report.transition_failed > 0 {
        "partial"
    } else {
        "completed"
    }
}

fn validate_manual_transition_job_id(params: &Params<'_, '_>) -> S3Result<()> {
    let job_id = params.get("job_id").unwrap_or("");
    if job_id.is_empty() {
        return Err(s3_error!(InvalidRequest, "manual transition job id is required"));
    }
    Ok(())
}

fn manual_transition_job_id_from_params(params: &Params<'_, '_>) -> S3Result<Uuid> {
    validate_manual_transition_job_id(params)?;
    Uuid::parse_str(params.get("job_id").unwrap_or(""))
        .map_err(|_| s3_error!(InvalidArgument, "invalid manual transition job id"))
}

fn active_manual_transition_jobs() -> &'static Mutex<HashMap<Uuid, CancellationToken>> {
    ACTIVE_MANUAL_TRANSITION_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn insert_active_manual_transition_job(job_id: Uuid, cancel_token: CancellationToken) {
    let mut jobs = active_manual_transition_jobs()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    jobs.insert(job_id, cancel_token);
}

fn remove_active_manual_transition_job(job_id: Uuid) {
    let mut jobs = active_manual_transition_jobs()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    jobs.remove(&job_id);
}

fn active_manual_transition_cancel_token(job_id: Uuid) -> Option<CancellationToken> {
    let jobs = active_manual_transition_jobs()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    jobs.get(&job_id).cloned()
}

fn manual_transition_status_endpoint(job_id: Uuid) -> String {
    format!("{ADMIN_PREFIX}/v3/ilm/transition/jobs/{job_id}")
}

fn manual_transition_owner_id() -> &'static str {
    MANUAL_TRANSITION_OWNER_ID.get_or_init(|| Uuid::new_v4().to_string()).as_str()
}

fn manual_transition_job_response(record: ManualTransitionJobRecord) -> ManualTransitionJobResponse {
    let status_endpoint = manual_transition_status_endpoint(record.job_id);
    ManualTransitionJobResponse {
        status: record.state,
        mode: "durable_job",
        job_id: record.job_id.to_string(),
        status_endpoint: status_endpoint.clone(),
        cancel_endpoint: status_endpoint,
        cancel_requested: record.cancel_requested,
        bucket: record.bucket,
        prefix: record.prefix,
        tier: record.tier,
        dry_run: record.dry_run,
        created_at_unix_nanos: record.created_at_unix_nanos,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
        completed_at_unix_nanos: record.completed_at_unix_nanos,
        report: record.report,
        queue_snapshot: record.queue_snapshot,
        failure_reason: record.error,
    }
}

fn manual_transition_job_conflict_response(admission: ManualTransitionScopeAdmission) -> ManualTransitionJobConflictResponse {
    let status_endpoint = manual_transition_status_endpoint(admission.job_id);
    ManualTransitionJobConflictResponse {
        state: "conflict",
        mode: "durable_job",
        active_job_id: admission.job_id.to_string(),
        status_endpoint: status_endpoint.clone(),
        cancel_endpoint: status_endpoint,
        scope_key: admission.scope_key,
    }
}

fn map_manual_transition_job_load_error(err: StorageError, job_id: Uuid) -> S3Error {
    if err == StorageError::ConfigNotFound {
        s3_error!(NoSuchKey, "manual transition job not found: {}", job_id)
    } else if err == StorageError::PreconditionFailed {
        s3_error!(OperationAborted, "manual transition job record changed concurrently; retry the request")
    } else {
        S3Error::with_message(S3ErrorCode::InternalError, format!("manual transition job store failed: {err}"))
    }
}

fn json_response<T: Serialize>(response: &T, status: StatusCode) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(response).map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode manual transition response: {err}"))
    })?;
    let content_type = HeaderValue::from_str(JSON_CONTENT_TYPE)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, content_type);
    Ok(S3Response::with_headers((status, Body::from(body)), headers))
}

async fn update_manual_transition_job_record_if_owned(
    store: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    mut update: impl FnMut(&mut ManualTransitionJobRecord) -> bool,
) -> S3Result<ManualTransitionJobRecord> {
    update_manual_transition_job_record(store, job_id, Some(expected_lease_id), |record| update(record))
        .await
        .map_err(|err| map_manual_transition_job_load_error(err, job_id))
}

fn manual_transition_durable_cancel_check(store: Arc<ECStore>, job_id: Uuid) -> ManualTransitionCancelCheck {
    let last_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let next_poll_at = Arc::new(Mutex::new(std::time::Instant::now()));
    Arc::new(move || {
        let store = store.clone();
        let last_cancelled = last_cancelled.clone();
        let next_poll_at = next_poll_at.clone();
        Box::pin(async move {
            if last_cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return true;
            }
            let should_poll = {
                let mut next_poll_at = next_poll_at.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let now = std::time::Instant::now();
                if now < *next_poll_at {
                    false
                } else {
                    *next_poll_at = now + StdDuration::from_secs(1);
                    true
                }
            };
            if !should_poll {
                return false;
            }
            match load_manual_transition_job_record(store, job_id).await {
                Ok(record) if record.cancel_requested => {
                    last_cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                    true
                }
                _ => false,
            }
        }) as Pin<Box<dyn std::future::Future<Output = bool> + Send>>
    })
}

fn manual_transition_progress_sink(store: Arc<ECStore>, job_id: Uuid, lease_id: Uuid) -> ManualTransitionProgressSink {
    Arc::new(move |report| {
        let store = store.clone();
        Box::pin(async move {
            persist_manual_transition_job_progress_if_owned(store, job_id, lease_id, &report, manual_transition_queue_snapshot())
                .await
                .map(|_| ())
        })
    })
}

fn release_manual_transition_admission(store: Arc<ECStore>, record: &ManualTransitionJobRecord) {
    let scope_key = record.scope_key.clone();
    let job_id = record.job_id;
    let lease_id = record.lease_id;
    tokio::spawn(async move {
        if let Err(err) = delete_manual_transition_scope_admission_if_current(store, &scope_key, job_id, lease_id).await {
            warn!(
                event = EVENT_ADMIN_ILM_TRANSITION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
                operation = "manual_transition_job",
                result = "failed",
                job_id = %job_id,
                error = %err,
                "failed to release manual transition admission"
            );
        }
    });
}

async fn finalize_manual_transition_job(
    store: Arc<ECStore>,
    job_id: Uuid,
    lease_id: Uuid,
    result: Result<ManualTransitionRunReport, StorageError>,
) -> Option<ManualTransitionJobRecord> {
    let updated = update_manual_transition_job_record_if_owned(store.clone(), job_id, lease_id, |record| {
        if record.is_terminal() {
            return false;
        }
        let cancel_requested = record.cancel_requested;
        match &result {
            Ok(report) => {
                let mut report = report.clone();
                if cancel_requested {
                    report.cancelled = true;
                }
                record.complete(report, manual_transition_queue_snapshot());
                if cancel_requested {
                    record.mark_cancel_requested();
                }
            }
            Err(err) => {
                record.fail(err.to_string());
                if cancel_requested {
                    record.mark_cancel_requested();
                }
            }
        }
        true
    })
    .await;
    match updated {
        Ok(record) => Some(record),
        Err(err) if err.code() == &S3ErrorCode::OperationAborted => None,
        Err(err) => {
            error!(
                event = EVENT_ADMIN_ILM_TRANSITION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
                operation = "manual_transition_job",
                result = "failed",
                job_id = %job_id,
                error = %err,
                "failed to persist manual transition job terminal state"
            );
            None
        }
    }
}

fn spawn_manual_transition_job_heartbeat(
    store: Arc<ECStore>,
    job_id: Uuid,
    lease_id: Uuid,
    scan_cancel_token: CancellationToken,
    shutdown_token: CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(StdDuration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => return,
                _ = interval.tick() => {
                    match renew_manual_transition_job_lease_if_owned(store.clone(), job_id, lease_id, manual_transition_queue_snapshot()).await {
                        Ok(record) if record.is_terminal() => {
                            remove_active_manual_transition_job(job_id);
                            scan_cancel_token.cancel();
                            return;
                        }
                        Ok(record) if record.cancel_requested => scan_cancel_token.cancel(),
                        Ok(_) => {}
                        Err(StorageError::PreconditionFailed) => {
                            remove_active_manual_transition_job(job_id);
                            scan_cancel_token.cancel();
                            return;
                        }
                        Err(err) => {
                        warn!(
                            event = EVENT_ADMIN_ILM_TRANSITION_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
                            operation = "manual_transition_job",
                            result = "failed",
                            job_id = %job_id,
                            error = %err,
                            "failed to renew manual transition job lease"
                        );
                        }
                    }
                }
            }
        }
    });
}

enum StartManualTransitionJobResult {
    Started(Box<ManualTransitionJobRecord>),
    Conflict(ManualTransitionJobConflictResponse),
}

async fn start_manual_transition_job(
    store: Arc<ECStore>,
    bucket: String,
    options: ManualTransitionRunOptions,
) -> S3Result<StartManualTransitionJobResult> {
    let job_id = Uuid::new_v4();
    let record = ManualTransitionJobRecord::new(job_id, &bucket, &options, manual_transition_owner_id());
    save_manual_transition_job_record(store.clone(), &record)
        .await
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("manual transition job store failed: {err}")))?;
    match claim_manual_transition_scope_admission(store.clone(), &ManualTransitionScopeAdmission::from_job(&record)).await {
        Ok(ManualTransitionScopeAdmissionClaim::Claimed) => {}
        Ok(ManualTransitionScopeAdmissionClaim::Conflict(active)) => {
            let _ = update_manual_transition_job_record_if_owned(store.clone(), job_id, record.lease_id, |record| {
                if record.is_terminal() {
                    return false;
                }
                record.fail("manual transition admission conflict");
                true
            })
            .await;
            return Ok(StartManualTransitionJobResult::Conflict(manual_transition_job_conflict_response(*active)));
        }
        Err(err) => {
            let _ = update_manual_transition_job_record_if_owned(store.clone(), job_id, record.lease_id, |record| {
                if record.is_terminal() {
                    return false;
                }
                record.fail(format!("manual transition admission failed: {err}"));
                true
            })
            .await;
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("manual transition admission failed: {err}"),
            ));
        }
    }

    let scan_cancel_token = CancellationToken::new();
    let heartbeat_shutdown_token = CancellationToken::new();
    insert_active_manual_transition_job(job_id, scan_cancel_token.clone());
    let mut run_options = options;
    let lease_id = record.lease_id;
    run_options.job_id = Some(job_id);
    run_options.cancel_token = Some(scan_cancel_token.clone());
    run_options.cancel_check = Some(manual_transition_durable_cancel_check(store.clone(), job_id));
    run_options.progress_sink = Some(manual_transition_progress_sink(store.clone(), job_id, lease_id));
    let run_store = store.clone();
    let job_scan_cancel_token = scan_cancel_token.clone();
    let job_heartbeat_shutdown_token = heartbeat_shutdown_token.clone();
    spawn_manual_transition_job_heartbeat(store, job_id, lease_id, scan_cancel_token, heartbeat_shutdown_token);
    tokio::spawn(async move {
        #[cfg(feature = "e2e-test-hooks")]
        if std::env::var_os(E2E_MANUAL_TRANSITION_CANCEL_BARRIER_ENV).is_some() {
            job_scan_cancel_token.cancelled().await;
        }
        let result = enqueue_transition_for_existing_objects_scoped(run_store.clone(), &bucket, run_options).await;
        if let Some(final_record) = finalize_manual_transition_job(run_store.clone(), job_id, lease_id, result).await
            && final_record.is_terminal()
        {
            release_manual_transition_admission(run_store, &final_record);
            job_scan_cancel_token.cancel();
            job_heartbeat_shutdown_token.cancel();
            remove_active_manual_transition_job(job_id);
        }
    });

    Ok(StartManualTransitionJobResult::Started(Box::new(record)))
}

pub struct ManualTransitionRunHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionRunHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        let actor = authorize_manual_transition_request(&req).await?;
        let (bucket, options, run_mode) = match parse_manual_transition_query(req.uri.query()) {
            Ok(parsed) => parsed,
            Err(err) => {
                log_manual_transition_rejected("invalid_query_parameters", &request_id, &actor, &remote_addr);
                return Err(err);
            }
        };
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            log_manual_transition_rejected("object_store_not_initialized", &request_id, &actor, &remote_addr);
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        if run_mode == ManualTransitionRunMode::Async {
            match start_manual_transition_job(store, bucket, options).await? {
                StartManualTransitionJobResult::Started(record) => {
                    let record = *record;
                    let status_endpoint = manual_transition_status_endpoint(record.job_id);
                    let response = ManualTransitionRunResponse {
                        state: "accepted",
                        mode: "durable_job",
                        job_id: Some(record.job_id.to_string()),
                        status_endpoint: Some(status_endpoint.clone()),
                        cancel_endpoint: Some(status_endpoint),
                        report: record.report,
                    };
                    return json_response(&response, StatusCode::ACCEPTED);
                }
                StartManualTransitionJobResult::Conflict(response) => {
                    return json_response(&response, StatusCode::CONFLICT);
                }
            }
        }
        let max_objects = options.max_objects;
        let max_duration_seconds = options.max_duration.map(|duration| duration.as_secs());
        let scope = ManualTransitionRunScope::new(&bucket, &options);
        let _admission = match acquire_manual_transition_admission(scope) {
            Ok(admission) => admission,
            Err(err) => {
                log_manual_transition_rejected("already_running", &request_id, &actor, &remote_addr);
                return Err(err);
            }
        };

        let report = match enqueue_transition_for_existing_objects_scoped(store, &bucket, options).await {
            Ok(report) => report,
            Err(err) => {
                log_manual_transition_failed("enqueue_failed", &request_id, &actor, &remote_addr, &err);
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("manual transition run failed: {err}"),
                ));
            }
        };
        let state = response_state(&report);
        log_manual_transition_completed(state, &request_id, &actor, &remote_addr, max_objects, max_duration_seconds, &report);
        let response = ManualTransitionRunResponse {
            state,
            mode: "enqueue_only",
            job_id: None,
            status_endpoint: None,
            cancel_endpoint: None,
            report,
        };

        json_response(&response, StatusCode::OK)
    }
}

pub struct ManualTransitionJobStatusHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobStatusHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_manual_transition_request(&req).await?;
        let job_id = manual_transition_job_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let mut record = load_manual_transition_job_record(store.clone(), job_id)
            .await
            .map_err(|err| map_manual_transition_job_load_error(err, job_id))?;
        if record.state == ManualTransitionJobState::Running {
            let local_active = active_manual_transition_cancel_token(job_id).is_some();
            let leased_elsewhere = load_manual_transition_scope_admission(store.clone(), &record.scope_key)
                .await
                .ok()
                .is_some_and(|admission| {
                    admission.job_id == record.job_id
                        && admission.lease_id == record.lease_id
                        && !manual_transition_scope_admission_lease_expired(&admission)
                });
            if !local_active && !leased_elsewhere && manual_transition_job_lease_expired(&record) {
                record = update_manual_transition_job_record_if_owned(store.clone(), job_id, record.lease_id, |record| {
                    if record.state == ManualTransitionJobState::Running && manual_transition_job_lease_expired(record) {
                        record.mark_unknown_if_unowned();
                        true
                    } else {
                        false
                    }
                })
                .await?;
                release_manual_transition_admission(store, &record);
            }
        }
        json_response(&manual_transition_job_response(record), StatusCode::OK)
    }
}

pub struct ManualTransitionJobCancelHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobCancelHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_manual_transition_request(&req).await?;
        let job_id = manual_transition_job_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let record = request_manual_transition_job_cancel(store, job_id)
            .await
            .map_err(|err| map_manual_transition_job_load_error(err, job_id))?;
        if !record.is_terminal()
            && let Some(cancel_token) = active_manual_transition_cancel_token(job_id)
        {
            cancel_token.cancel();
        }
        json_response(&manual_transition_job_response(record), StatusCode::OK)
    }
}

pub struct TransitionReconcileInspectHandler {}

#[async_trait::async_trait]
impl Operation for TransitionReconcileInspectHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_transition_admin_request(&req, AdminAction::ListTierAction).await?;
        let transaction_id = transition_transaction_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let status = inspect_transition_transaction_for_operator(store, transaction_id)
            .await
            .map_err(map_transition_operator_error)?;
        json_response(&status, StatusCode::OK)
    }
}

pub struct TransitionReconcileApplyHandler {}

#[async_trait::async_trait]
impl Operation for TransitionReconcileApplyHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        let actor = authorize_transition_admin_request(&req, AdminAction::SetTierAction).await?;
        let transaction_id = transition_transaction_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let mut input = req.input;
        let body = input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "transition reconciliation body is too large or unreadable"))?;
        let request: TransitionReconcileRequest = serde_json::from_slice(&body)
            .map_err(|_| s3_error!(InvalidRequest, "transition reconciliation request must be valid JSON"))?;

        match validate_transition_reconcile_request(&request)? {
            ValidatedTransitionReconcileAction::DeleteCandidate(remote_version_id) => {
                let result = delete_transition_candidate_for_operator(store, transaction_id, remote_version_id)
                    .await
                    .map_err(map_transition_operator_error)?;
                let outcome = if result.journal_observed_after_delete {
                    "exact_delete_completed_journal_observed"
                } else {
                    "exact_delete_completed_journal_already_finalized"
                };
                log_transition_reconcile_applied(transaction_id, "delete_candidate", outcome, &request_id, &actor, &remote_addr);
                json_response(&TransitionCandidateDeleteResponse { outcome, result }, StatusCode::OK)
            }
            ValidatedTransitionReconcileAction::FinalizeMissing => {
                finalize_missing_transition_transaction_for_operator(store, transaction_id)
                    .await
                    .map_err(map_transition_operator_error)?;
                log_transition_reconcile_applied(
                    transaction_id,
                    "finalize_missing",
                    "journal_deleted_after_missing_probe",
                    &request_id,
                    &actor,
                    &remote_addr,
                );
                json_response(
                    &TransitionFinalizeMissingResponse {
                        outcome: "journal_finalized",
                        journal_retained: false,
                        transaction_id,
                    },
                    StatusCode::OK,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matchit::Router;

    fn with_manual_transition_job_params<T>(path: &str, f: impl FnOnce(&Params<'_, '_>) -> T) -> T {
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/ilm/transition/jobs/{job_id}", ())
            .expect("route should insert");

        let matched = router.at(path).expect("route should match");
        f(&matched.params)
    }

    fn manual_transition_job_request(method: Method, path: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: path.parse().expect("valid route"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[test]
    fn transition_reconcile_request_is_explicit_and_fail_closed() {
        let unconfirmed: TransitionReconcileRequest =
            serde_json::from_slice(br#"{"action":"delete_candidate","confirm":false,"remote_version_id":"v1"}"#)
                .expect("request should decode");
        assert!(validate_transition_reconcile_request(&unconfirmed).is_err());

        let missing_version: TransitionReconcileRequest =
            serde_json::from_slice(br#"{"action":"delete_candidate","confirm":true}"#).expect("request should decode");
        assert!(validate_transition_reconcile_request(&missing_version).is_err());

        let unsafe_finalize: TransitionReconcileRequest =
            serde_json::from_slice(br#"{"action":"finalize_missing","confirm":true,"remote_version_id":"v1"}"#)
                .expect("request should decode");
        assert!(validate_transition_reconcile_request(&unsafe_finalize).is_err());

        let delete: TransitionReconcileRequest =
            serde_json::from_slice(br#"{"action":"delete_candidate","confirm":true,"remote_version_id":"opaque-v1"}"#)
                .expect("request should decode");
        assert!(matches!(
            validate_transition_reconcile_request(&delete),
            Ok(ValidatedTransitionReconcileAction::DeleteCandidate("opaque-v1"))
        ));

        let finalize: TransitionReconcileRequest =
            serde_json::from_slice(br#"{"action":"finalize_missing","confirm":true}"#).expect("request should decode");
        assert!(matches!(
            validate_transition_reconcile_request(&finalize),
            Ok(ValidatedTransitionReconcileAction::FinalizeMissing)
        ));

        assert!(
            serde_json::from_slice::<TransitionReconcileRequest>(
                br#"{"action":"finalize_missing","confirm":true,"unexpected":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn transition_reconcile_routes_use_read_and_write_tier_actions() {
        let src = include_str!("ilm_transition.rs");
        let inspect = src
            .split("impl Operation for TransitionReconcileInspectHandler")
            .nth(1)
            .and_then(|block| block.split("impl Operation for TransitionReconcileApplyHandler").next())
            .expect("inspect handler block");
        assert!(inspect.contains("AdminAction::ListTierAction"));
        assert!(!inspect.contains("AdminAction::SetTierAction"));

        let apply = src
            .split("impl Operation for TransitionReconcileApplyHandler")
            .nth(1)
            .and_then(|block| block.split("#[cfg(test)]").next())
            .expect("apply handler block");
        assert!(apply.contains("AdminAction::SetTierAction"));
        assert!(!apply.contains("AdminAction::ListTierAction"));
    }

    #[test]
    fn manual_transition_query_defaults_to_bounded_run() {
        let (bucket, options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&prefix=logs/&marker=logs/a&versionMarker=v1&tier=warm"))
                .expect("valid query should parse");

        assert_eq!(bucket, "data");
        assert_eq!(run_mode, ManualTransitionRunMode::EnqueueOnly);
        assert_eq!(options.prefix, "logs/");
        assert_eq!(options.marker.as_deref(), Some("logs/a"));
        assert_eq!(options.version_marker.as_deref(), Some("v1"));
        assert_eq!(options.tier.as_deref(), Some("warm"));
        assert!(!options.dry_run);
        assert_eq!(options.max_objects, Some(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS));
        assert_eq!(options.max_duration, None);
    }

    #[test]
    fn manual_transition_query_accepts_duration_budget() {
        let (_bucket, options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&maxDurationSeconds=30")).expect("valid query should parse");

        assert_eq!(run_mode, ManualTransitionRunMode::EnqueueOnly);
        assert_eq!(options.max_duration, Some(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn manual_transition_query_accepts_explicit_enqueue_only_mode() {
        let (_bucket, options, run_mode) = parse_manual_transition_query(Some("bucket=data&mode=enqueue_only&async=false"))
            .expect("explicit enqueue_only mode should remain compatible");

        assert_eq!(run_mode, ManualTransitionRunMode::EnqueueOnly);
        assert!(!options.dry_run);
        assert_eq!(options.max_objects, Some(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS));
    }

    #[test]
    fn manual_transition_query_accepts_durable_async_mode() {
        let (_bucket, options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&async=true")).expect("async durable jobs should parse");

        assert_eq!(run_mode, ManualTransitionRunMode::Async);
        assert_eq!(options.max_objects, Some(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS));

        let (_bucket, _options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&mode=async")).expect("mode=async should parse");

        assert_eq!(run_mode, ManualTransitionRunMode::Async);
    }

    #[test]
    fn manual_transition_query_rejects_conflicting_mode_flags() {
        let err = parse_manual_transition_query(Some("bucket=data&async=true&mode=enqueue_only"))
            .expect_err("conflicting async and enqueue_only flags must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        let err = parse_manual_transition_query(Some("bucket=data&async=false&mode=async"))
            .expect_err("conflicting async=false and mode=async flags must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_query_rejects_continuation_with_raw_markers() {
        let err = parse_manual_transition_query(Some("bucket=data&continuationToken=opaque&marker=logs/a"))
            .expect_err("continuation token and raw marker must not be mixed");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        let err = parse_manual_transition_query(Some("bucket=data&continuationToken=opaque&versionMarker=v1"))
            .expect_err("continuation token and raw version marker must not be mixed");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_query_rejects_unknown_mode() {
        let err = parse_manual_transition_query(Some("bucket=data&mode=background"))
            .expect_err("unknown mode must not be silently accepted");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_scope_ignores_resume_and_budget_parameters() {
        let (bucket, first, _run_mode) = parse_manual_transition_query(Some(
            "bucket=data&prefix=logs/&tier=warm&marker=logs/a&versionMarker=v1&maxObjects=10",
        ))
        .expect("first query should parse");
        let (_, second, _run_mode) = parse_manual_transition_query(Some(
            "bucket=data&prefix=logs/&tier=WARM&marker=logs/z&versionMarker=v9&maxObjects=20",
        ))
        .expect("second query should parse");

        assert_eq!(
            ManualTransitionRunScope::new(&bucket, &first),
            ManualTransitionRunScope::new(&bucket, &second)
        );
    }

    #[test]
    fn manual_transition_scope_distinguishes_dry_run_mode() {
        let (bucket, real, _run_mode) =
            parse_manual_transition_query(Some("bucket=data&prefix=logs/&tier=warm")).expect("real query should parse");
        let (_, dry_run, _run_mode) = parse_manual_transition_query(Some("bucket=data&prefix=logs/&tier=warm&dryRun=true"))
            .expect("dry-run query should parse");

        assert_ne!(
            ManualTransitionRunScope::new(&bucket, &real),
            ManualTransitionRunScope::new(&bucket, &dry_run)
        );
    }

    #[test]
    fn manual_transition_admission_rejects_same_scope_until_guard_drops() {
        let (bucket, options, _run_mode) =
            parse_manual_transition_query(Some("bucket=admission-test&prefix=logs/&tier=warm")).expect("query should parse");
        let scope = ManualTransitionRunScope::new(&bucket, &options);
        let first = acquire_manual_transition_admission(scope.clone()).expect("first admission should succeed");

        let err = acquire_manual_transition_admission(scope.clone()).expect_err("same scope must be rejected");

        assert_eq!(err.code(), &S3ErrorCode::OperationAborted);
        assert_eq!(err.status_code(), Some(StatusCode::CONFLICT));

        let different = ManualTransitionRunScope::new(
            "admission-test",
            &ManualTransitionRunOptions {
                prefix: "other/".into(),
                ..options
            },
        );
        let other = acquire_manual_transition_admission(different).expect("different scope should run independently");

        drop(other);
        drop(first);

        acquire_manual_transition_admission(scope).expect("scope should be released after guard drops");
    }

    #[test]
    fn manual_transition_admission_rejects_overlapping_prefix_or_tier() {
        let (bucket, options, _run_mode) =
            parse_manual_transition_query(Some("bucket=admission-overlap-test&prefix=logs/")).expect("query should parse");
        let scope = ManualTransitionRunScope::new(&bucket, &options);
        let active = acquire_manual_transition_admission(scope).expect("first admission should succeed");

        let overlapping_prefix = ManualTransitionRunScope::new(
            "admission-overlap-test",
            &ManualTransitionRunOptions {
                prefix: "logs/2026/".into(),
                tier: Some("warm".into()),
                ..ManualTransitionRunOptions::default()
            },
        );
        let err =
            acquire_manual_transition_admission(overlapping_prefix).expect_err("wildcard tier and nested prefix must conflict");

        assert_eq!(err.status_code(), Some(StatusCode::CONFLICT));

        let disjoint_prefix = ManualTransitionRunScope::new(
            "admission-overlap-test",
            &ManualTransitionRunOptions {
                prefix: "archive/".into(),
                tier: Some("warm".into()),
                ..ManualTransitionRunOptions::default()
            },
        );
        let disjoint = acquire_manual_transition_admission(disjoint_prefix).expect("disjoint prefix should run independently");

        drop(disjoint);
        drop(active);
    }

    #[test]
    fn manual_transition_handler_acquires_admission_before_enqueue() {
        let src = include_str!("ilm_transition.rs");
        let handler_block = extract_block_between_markers(
            src,
            "impl Operation for ManualTransitionRunHandler",
            "let report = match enqueue_transition_for_existing_objects_scoped",
        );

        assert!(handler_block.contains("acquire_manual_transition_admission"));
    }

    #[test]
    fn manual_transition_query_rejects_server_info_style_unscoped_request() {
        let err = parse_manual_transition_query(Some("dryRun=true")).expect_err("bucket must be required");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn manual_transition_query_rejects_unbounded_budget() {
        let err = parse_manual_transition_query(Some("bucket=data&maxObjects=0")).expect_err("zero budget must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_query_rejects_invalid_duration_budget() {
        let err = parse_manual_transition_query(Some("bucket=data&maxDurationSeconds=0")).expect_err("zero budget must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        let err = parse_manual_transition_query(Some("bucket=data&maxDurationSeconds=3601"))
            .expect_err("budget above the cap must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_response_reports_partial_for_queue_pressure() {
        let report = ManualTransitionRunReport {
            skipped_queue_full: 1,
            ..Default::default()
        };

        assert_eq!(response_state(&report), "partial");
    }

    #[test]
    fn manual_transition_response_reports_partial_for_in_flight_skip() {
        let report = ManualTransitionRunReport {
            skipped_already_in_flight: 1,
            ..Default::default()
        };

        assert_eq!(response_state(&report), "partial");
    }

    #[test]
    fn manual_transition_response_reports_partial_for_duration_budget() {
        let report = ManualTransitionRunReport {
            truncated_by_duration: true,
            ..Default::default()
        };

        assert_eq!(response_state(&report), "partial");
    }

    #[test]
    fn manual_transition_response_reports_partial_for_tier_failure() {
        let report = ManualTransitionRunReport {
            tier_failure: 1,
            ..Default::default()
        };

        assert_eq!(response_state(&report), "partial");
    }

    #[test]
    fn manual_transition_response_reports_partial_for_worker_failure() {
        let report = ManualTransitionRunReport {
            transition_failed: 1,
            ..Default::default()
        };

        assert_eq!(response_state(&report), "partial");
    }

    #[test]
    fn manual_transition_response_omits_raw_resume_markers() {
        let report = ManualTransitionRunReport {
            truncated_by_limit: true,
            next_marker: Some("private/object".to_string()),
            next_version_idmarker: Some("null".to_string()),
            ..Default::default()
        };
        let response = ManualTransitionRunResponse {
            state: response_state(&report),
            mode: "enqueue_only",
            job_id: None,
            status_endpoint: None,
            cancel_endpoint: None,
            report,
        };

        let value = serde_json::to_value(response).expect("response should serialize");
        assert!(value.get("job_id").is_none());
        assert!(value.get("status_endpoint").is_none());
        assert!(value.get("cancel_endpoint").is_none());
        assert!(value.pointer("/report/next_marker").is_none());
        assert!(value.pointer("/report/next_version_idmarker").is_none());
    }

    #[test]
    fn manual_transition_handler_requires_set_tier_action() {
        let src = include_str!("ilm_transition.rs");
        let auth_block = extract_block_between_markers(src, "async fn authorize_manual_transition_request", "fn response_state");

        assert!(auth_block.contains("AdminAction::SetTierAction"));
        assert!(!auth_block.contains("AdminAction::ServerInfoAdminAction"));
    }

    #[test]
    fn manual_transition_job_id_path_param_is_required() {
        with_manual_transition_job_params("/rustfs/admin/v3/ilm/transition/jobs/job-123", |params| {
            assert_eq!(params.get("job_id"), Some("job-123"));
            validate_manual_transition_job_id(params)
        })
        .expect("job id should validate");

        let err = validate_manual_transition_job_id(&Params::new()).expect_err("missing job id must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn manual_transition_job_response_exposes_status_and_cancel_contract() {
        let record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &ManualTransitionRunOptions::default(), "owner-a");
        let response = manual_transition_job_response(record);

        assert_eq!(response.status, ManualTransitionJobState::Running);
        assert_eq!(response.mode, "durable_job");
        assert!(response.status_endpoint.ends_with(&response.job_id));
        assert_eq!(response.cancel_endpoint, response.status_endpoint);
    }

    #[test]
    fn manual_transition_job_response_reads_back_terminal_queue_pressure_snapshot() {
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("WARM".to_string()),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "bucket", &options, "owner-a");
        let queue_snapshot = ManualTransitionQueueSnapshot {
            queue_capacity: 4,
            queued: 2,
            active: 1,
            workers: 2,
            queue_full: 3,
            queue_send_timeout: 5,
            compensation_pending: 7,
            compensation_running: 1,
        };

        record.complete(
            ManualTransitionRunReport {
                bucket: "bucket".to_string(),
                prefix: options.prefix,
                tier: options.tier,
                skipped_queue_full: 3,
                skipped_queue_timeout: 5,
                ..Default::default()
            },
            queue_snapshot,
        );

        let response = manual_transition_job_response(record);

        assert_eq!(response.status, ManualTransitionJobState::Partial);
        assert_eq!(response.report.skipped_queue_full, 3);
        assert_eq!(response.report.skipped_queue_timeout, 5);
        assert_eq!(response.queue_snapshot, queue_snapshot);
        assert!(response.completed_at_unix_nanos.is_some());
        assert_eq!(response.failure_reason, None);
    }

    #[test]
    fn manual_transition_active_job_cancel_token_round_trips() {
        let job_id = Uuid::new_v4();
        let cancel_token = CancellationToken::new();
        insert_active_manual_transition_job(job_id, cancel_token.clone());

        let active_cancel_token = active_manual_transition_cancel_token(job_id).expect("active job token should be registered");
        active_cancel_token.cancel();

        assert!(cancel_token.is_cancelled());

        remove_active_manual_transition_job(job_id);
        assert!(active_manual_transition_cancel_token(job_id).is_none());
    }

    #[test]
    fn manual_transition_heartbeat_keeps_running_after_scan_cancel() {
        let src = include_str!("ilm_transition.rs");
        let heartbeat_block =
            extract_block_between_markers(src, "fn spawn_manual_transition_job_heartbeat", "enum StartManualTransitionJobResult");

        assert!(heartbeat_block.contains("scan_cancel_token.cancel()"));
        assert!(heartbeat_block.contains("shutdown_token.cancelled()"));
        assert!(!heartbeat_block.contains("scan_cancel_token.cancelled()"));
    }

    #[tokio::test]
    async fn manual_transition_job_handlers_reject_missing_credentials_before_status_contract() {
        let status_err = ManualTransitionJobStatusHandler {}
            .call(
                manual_transition_job_request(Method::GET, "/rustfs/admin/v3/ilm/transition/jobs/job-123"),
                Params::new(),
            )
            .await
            .expect_err("status handler must reject unsigned requests");
        assert_eq!(status_err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(status_err.message(), Some("authentication required"));

        let cancel_err = ManualTransitionJobCancelHandler {}
            .call(
                manual_transition_job_request(Method::DELETE, "/rustfs/admin/v3/ilm/transition/jobs/job-123"),
                Params::new(),
            )
            .await
            .expect_err("cancel handler must reject unsigned requests");
        assert_eq!(cancel_err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(cancel_err.message(), Some("authentication required"));
    }

    #[test]
    fn manual_transition_job_handlers_authorize_validate_and_load_store() {
        let src = include_str!("ilm_transition.rs");
        let status_block = extract_block_between_markers(
            src,
            "impl Operation for ManualTransitionJobStatusHandler",
            "pub struct ManualTransitionJobCancelHandler",
        );
        let cancel_block =
            extract_block_between_markers(src, "impl Operation for ManualTransitionJobCancelHandler", "#[cfg(test)]");

        let status_load = status_block
            .find("load_manual_transition_job_record")
            .expect("status route must load the persisted job record");
        let cancel_load = cancel_block
            .find("request_manual_transition_job_cancel")
            .expect("cancel route must update the persisted job record");

        for (block, load) in [(status_block, status_load), (cancel_block, cancel_load)] {
            let auth = block
                .find("authorize_manual_transition_request(&req).await?;")
                .expect("job route must authorize with SetTierAction");
            let job_id = block
                .find("manual_transition_job_id_from_params(&params)?;")
                .expect("job route must validate the path job id");

            assert!(auth < job_id);
            assert!(job_id < load);
            assert!(!block.contains("ServerInfoAdminAction"));
        }
    }

    #[test]
    fn manual_transition_logs_masked_actor_and_aggregate_counters() {
        let src = include_str!("ilm_transition.rs");
        let auth_block = extract_block_between_markers(src, "async fn authorize_manual_transition_request", "fn response_state");
        let log_block = extract_block_between_markers(
            src,
            "fn log_manual_transition_completed",
            "async fn authorize_manual_transition_request",
        );

        assert!(auth_block.contains("MaskedAccessKey"));
        assert!(log_block.contains("EVENT_ADMIN_ILM_TRANSITION_STATE"));
        assert!(log_block.contains("request_id"));
        assert!(log_block.contains("remote_addr"));
        assert!(log_block.contains("scanned"));
        assert!(log_block.contains("eligible"));
        assert!(log_block.contains("enqueued"));
        assert!(log_block.contains("skipped_already_transitioned"));
        assert!(log_block.contains("skipped_queue_full"));
        assert!(!log_block.contains("next_marker"));
        assert!(!log_block.contains("next_version_idmarker"));
    }

    fn extract_block_between_markers<'a>(src: &'a str, start_marker: &str, end_marker: &str) -> &'a str {
        let start = src
            .find(start_marker)
            .unwrap_or_else(|| panic!("expected start marker `{start_marker}`"));
        let after_start = &src[start..];
        let end = after_start
            .find(end_marker)
            .unwrap_or_else(|| panic!("expected end marker `{end_marker}` after `{start_marker}`"));
        &after_start[..end]
    }
}
