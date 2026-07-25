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
use crate::admin::storage_api::config::{read_admin_config, save_admin_config};
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::lifecycle::{
    ManualTransitionRunOptions, ManualTransitionRunReport, enqueue_transition_for_existing_objects_scoped,
    enqueue_transition_for_existing_objects_scoped_with_cancel,
};
use crate::admin::storage_api::runtime::ECStore;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::{
    MaskedAccessKey,
    http::{AMZ_REQUEST_ID, REQUEST_ID_HEADER},
};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
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
const MANUAL_TRANSITION_JOB_SCHEMA_VERSION: u8 = 1;
const MANUAL_TRANSITION_JOB_CONFIG_PREFIX: &str = "ilm/transition/jobs";

static ACTIVE_MANUAL_TRANSITION_SCOPES: OnceLock<Mutex<Vec<ManualTransitionRunScope>>> = OnceLock::new();
static MANUAL_TRANSITION_JOBS: OnceLock<Mutex<HashMap<String, ManualTransitionJobRuntime>>> = OnceLock::new();

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

fn durable_manual_transition_jobs_not_implemented_error() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::NotImplemented,
        "durable manual transition jobs are not implemented; omit async/mode for enqueue_only",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualTransitionRunMode {
    EnqueueOnly,
    Async,
}

impl ManualTransitionRunMode {
    fn response_mode(self) -> &'static str {
        match self {
            Self::EnqueueOnly => "enqueue_only",
            Self::Async => "durable_job",
        }
    }
}

#[derive(Debug, Serialize)]
struct ManualTransitionRunResponse {
    state: &'static str,
    mode: &'static str,
    job_id: Option<String>,
    status_endpoint: Option<String>,
    report: ManualTransitionRunReport,
}

#[derive(Debug, Clone)]
struct ManualTransitionJobRuntime {
    record: ManualTransitionJobRecord,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManualTransitionJobStatus {
    Queued,
    Running,
    Completed,
    Partial,
    Cancelled,
    Failed,
    Unknown,
}

impl ManualTransitionJobStatus {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Partial | Self::Cancelled | Self::Failed | Self::Unknown)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualTransitionJobRecord {
    schema_version: u8,
    job_id: String,
    status_endpoint: String,
    status: ManualTransitionJobStatus,
    bucket: String,
    prefix: String,
    tier: Option<String>,
    dry_run: bool,
    max_objects: Option<u64>,
    max_duration_seconds: Option<u64>,
    created_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    cancel_requested: bool,
    failure_reason: Option<String>,
    report: ManualTransitionRunReport,
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
    Ok(())
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
            tier: query.tier.map(|tier| tier.trim().to_string()).filter(|tier| !tier.is_empty()),
            dry_run: query.dry_run.unwrap_or(false),
            max_objects: Some(max_objects),
            max_duration: query.max_duration_seconds.map(std::time::Duration::from_secs),
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

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::SetTierAction)],
        remote_addr,
    )
    .await?;

    Ok(actor)
}

fn response_state(report: &ManualTransitionRunReport) -> &'static str {
    if report.was_truncated() || report.has_partial_enqueue() {
        "partial"
    } else {
        "completed"
    }
}

fn json_response<T: Serialize>(status: StatusCode, response: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(response).map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode manual transition response: {err}"))
    })?;
    let content_type = HeaderValue::from_str(JSON_CONTENT_TYPE)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, content_type);
    Ok(S3Response::with_headers((status, Body::from(body)), headers))
}

fn manual_transition_job_registry() -> &'static Mutex<HashMap<String, ManualTransitionJobRuntime>> {
    MANUAL_TRANSITION_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_manual_transition_jobs() -> MutexGuard<'static, HashMap<String, ManualTransitionJobRuntime>> {
    match manual_transition_job_registry().lock() {
        Ok(jobs) => jobs,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn manual_transition_job_status_endpoint(job_id: &str) -> String {
    format!("{ADMIN_PREFIX}/v3/ilm/transition/jobs/{job_id}")
}

fn manual_transition_job_config_path(job_id: &str) -> String {
    format!("{MANUAL_TRANSITION_JOB_CONFIG_PREFIX}/{job_id}.json")
}

fn parse_manual_transition_job_id(job_id: &str) -> S3Result<String> {
    Uuid::parse_str(job_id).map_err(|_| s3_error!(InvalidArgument, "invalid manual transition job id"))?;
    Ok(job_id.to_string())
}

fn manual_transition_timestamp(now: OffsetDateTime) -> String {
    now.format(&Rfc3339).unwrap_or_else(|_| now.unix_timestamp().to_string())
}

fn initial_manual_transition_report(bucket: &str, options: &ManualTransitionRunOptions) -> ManualTransitionRunReport {
    ManualTransitionRunReport {
        bucket: bucket.to_string(),
        prefix: options.prefix.clone(),
        tier: options.tier.clone(),
        dry_run: options.dry_run,
        ..Default::default()
    }
}

fn manual_transition_status_from_report(report: &ManualTransitionRunReport) -> ManualTransitionJobStatus {
    if report.was_truncated() || report.has_partial_enqueue() {
        ManualTransitionJobStatus::Partial
    } else {
        ManualTransitionJobStatus::Completed
    }
}

fn new_manual_transition_job_record(
    job_id: String,
    bucket: &str,
    options: &ManualTransitionRunOptions,
    created_at: OffsetDateTime,
) -> ManualTransitionJobRecord {
    ManualTransitionJobRecord {
        schema_version: MANUAL_TRANSITION_JOB_SCHEMA_VERSION,
        status_endpoint: manual_transition_job_status_endpoint(&job_id),
        job_id,
        status: ManualTransitionJobStatus::Queued,
        bucket: bucket.to_string(),
        prefix: options.prefix.clone(),
        tier: options.tier.clone(),
        dry_run: options.dry_run,
        max_objects: options.max_objects,
        max_duration_seconds: options.max_duration.map(|duration| duration.as_secs()),
        created_at: manual_transition_timestamp(created_at),
        started_at: None,
        finished_at: None,
        cancel_requested: false,
        failure_reason: None,
        report: initial_manual_transition_report(bucket, options),
    }
}

fn insert_manual_transition_job(record: ManualTransitionJobRecord, cancel_token: CancellationToken) {
    let mut jobs = lock_manual_transition_jobs();
    jobs.insert(record.job_id.clone(), ManualTransitionJobRuntime { record, cancel_token });
}

fn update_manual_transition_job_record(
    job_id: &str,
    update: impl FnOnce(&mut ManualTransitionJobRecord),
) -> Option<ManualTransitionJobRecord> {
    let mut jobs = lock_manual_transition_jobs();
    let runtime = jobs.get_mut(job_id)?;
    update(&mut runtime.record);
    Some(runtime.record.clone())
}

fn in_memory_manual_transition_job_record(job_id: &str) -> Option<ManualTransitionJobRecord> {
    lock_manual_transition_jobs()
        .get(job_id)
        .map(|runtime| runtime.record.clone())
}

fn request_manual_transition_job_cancel(job_id: &str) -> Option<ManualTransitionJobRecord> {
    let mut jobs = lock_manual_transition_jobs();
    let runtime = jobs.get_mut(job_id)?;
    if !runtime.record.status.is_terminal() {
        runtime.record.cancel_requested = true;
        runtime.cancel_token.cancel();
    }
    Some(runtime.record.clone())
}

fn remove_manual_transition_job(job_id: &str) {
    let mut jobs = lock_manual_transition_jobs();
    jobs.remove(job_id);
}

async fn save_manual_transition_job_record(store: Arc<ECStore>, record: &ManualTransitionJobRecord) -> S3Result<()> {
    let data = serde_json::to_vec(record).map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode manual transition job: {err}"))
    })?;
    save_admin_config(store, &manual_transition_job_config_path(&record.job_id), data)
        .await
        .map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to persist manual transition job: {err}"))
        })
}

async fn read_manual_transition_job_record(store: Arc<ECStore>, job_id: &str) -> S3Result<Option<ManualTransitionJobRecord>> {
    match read_admin_config(store, &manual_transition_job_config_path(job_id)).await {
        Ok(data) => serde_json::from_slice(&data).map(Some).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to decode manual transition job: {err}"))
        }),
        Err(StorageError::ConfigNotFound) => Ok(None),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to read manual transition job: {err}"),
        )),
    }
}

async fn load_manual_transition_job_record(store: Arc<ECStore>, job_id: &str) -> S3Result<ManualTransitionJobRecord> {
    if let Some(record) = in_memory_manual_transition_job_record(job_id) {
        return Ok(record);
    }
    let Some(mut record) = read_manual_transition_job_record(store, job_id).await? else {
        return Err(s3_error!(NoSuchKey, "manual transition job not found"));
    };
    if !record.status.is_terminal() {
        record.status = ManualTransitionJobStatus::Unknown;
        record.finished_at = Some(manual_transition_timestamp(OffsetDateTime::now_utc()));
        record.failure_reason = Some("manual transition job owner is not active on this node".to_string());
    }
    Ok(record)
}

async fn persist_manual_transition_job_update(store: Arc<ECStore>, record: &ManualTransitionJobRecord) -> bool {
    if let Err(err) = save_manual_transition_job_record(store, record).await {
        error!(
            event = EVENT_ADMIN_ILM_TRANSITION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_ILM_TRANSITION,
            operation = "manual_transition_job",
            job_id = %record.job_id,
            error = %err,
            "failed to persist manual transition job update"
        );
        return false;
    }
    true
}

fn spawn_manual_transition_job(
    store: Arc<ECStore>,
    bucket: String,
    options: ManualTransitionRunOptions,
    job_id: String,
    cancel_token: CancellationToken,
    admission_guard: ManualTransitionRunAdmission,
) {
    tokio::spawn(async move {
        let started_record = update_manual_transition_job_record(&job_id, |record| {
            record.status = ManualTransitionJobStatus::Running;
            record.started_at = Some(manual_transition_timestamp(OffsetDateTime::now_utc()));
        });
        if let Some(record) = started_record.as_ref() {
            persist_manual_transition_job_update(store.clone(), record).await;
        }

        let result = enqueue_transition_for_existing_objects_scoped_with_cancel(
            store.clone(),
            &bucket,
            options,
            Some(cancel_token.clone()),
        )
        .await;
        let finished_at = manual_transition_timestamp(OffsetDateTime::now_utc());
        let finished_record = update_manual_transition_job_record(&job_id, |record| {
            record.finished_at = Some(finished_at);
            match result {
                Ok(execution) => {
                    record.report = execution.report;
                    record.status = if execution.cancelled || record.cancel_requested || cancel_token.is_cancelled() {
                        ManualTransitionJobStatus::Cancelled
                    } else {
                        manual_transition_status_from_report(&record.report)
                    };
                    record.failure_reason = None;
                }
                Err(err) => {
                    record.status = ManualTransitionJobStatus::Failed;
                    record.failure_reason = Some(err.to_string());
                }
            }
        });
        if let Some(record) = finished_record.as_ref()
            && persist_manual_transition_job_update(store, record).await
            && record.status.is_terminal()
        {
            remove_manual_transition_job(&job_id);
        }
        drop(admission_guard);
    });
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
        let max_objects = options.max_objects;
        let max_duration_seconds = options.max_duration.map(|duration| duration.as_secs());
        let scope = ManualTransitionRunScope::new(&bucket, &options);
        let admission = match acquire_manual_transition_admission(scope) {
            Ok(admission) => admission,
            Err(err) => {
                log_manual_transition_rejected("already_running", &request_id, &actor, &remote_addr);
                return Err(err);
            }
        };

        if run_mode == ManualTransitionRunMode::Async {
            let job_id = Uuid::new_v4().to_string();
            let record = new_manual_transition_job_record(job_id.clone(), &bucket, &options, OffsetDateTime::now_utc());
            save_manual_transition_job_record(store.clone(), &record).await?;
            let cancel_token = CancellationToken::new();
            insert_manual_transition_job(record.clone(), cancel_token.clone());
            spawn_manual_transition_job(store, bucket, options, job_id.clone(), cancel_token, admission);
            let response = ManualTransitionRunResponse {
                state: "accepted",
                mode: run_mode.response_mode(),
                job_id: Some(job_id),
                status_endpoint: Some(record.status_endpoint),
                report: record.report,
            };

            return json_response(StatusCode::ACCEPTED, &response);
        }

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
            report,
        };

        json_response(StatusCode::OK, &response)
    }
}

pub struct ManualTransitionJobStatusHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobStatusHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _actor = authorize_manual_transition_request(&req).await?;
        let job_id = parse_manual_transition_job_id(
            params
                .get("job_id")
                .ok_or_else(|| s3_error!(InvalidRequest, "manual transition job id is required"))?,
        )?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let record = load_manual_transition_job_record(store, &job_id).await?;
        json_response(StatusCode::OK, &record)
    }
}

pub struct ManualTransitionJobCancelHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobCancelHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _actor = authorize_manual_transition_request(&req).await?;
        let job_id = parse_manual_transition_job_id(
            params
                .get("job_id")
                .ok_or_else(|| s3_error!(InvalidRequest, "manual transition job id is required"))?,
        )?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };
        let record = if let Some(record) = request_manual_transition_job_cancel(&job_id) {
            record
        } else {
            let Some(mut record) = read_manual_transition_job_record(store.clone(), &job_id).await? else {
                return Err(s3_error!(NoSuchKey, "manual transition job not found"));
            };
            if !record.status.is_terminal() {
                record.status = ManualTransitionJobStatus::Unknown;
                record.cancel_requested = true;
                record.finished_at = Some(manual_transition_timestamp(OffsetDateTime::now_utc()));
                record.failure_reason = Some("manual transition job owner is not active on this node".to_string());
                save_manual_transition_job_record(store, &record).await?;
            }
            record
        };
        json_response(StatusCode::OK, &record)
    }
}

pub struct ManualTransitionJobStatusHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobStatusHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_manual_transition_request(&req).await?;
        validate_manual_transition_job_id(&params)?;
        Err(durable_manual_transition_jobs_not_implemented_error())
    }
}

pub struct ManualTransitionJobCancelHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionJobCancelHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_manual_transition_request(&req).await?;
        validate_manual_transition_job_id(&params)?;
        Err(durable_manual_transition_jobs_not_implemented_error())
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
    fn manual_transition_query_accepts_explicit_async_mode() {
        let (_bucket, _options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&async=true")).expect("async mode should parse");

        assert_eq!(run_mode, ManualTransitionRunMode::Async);

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
    fn manual_transition_response_reports_partial_for_duration_budget() {
        let report = ManualTransitionRunReport {
            truncated_by_duration: true,
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
            report,
        };

        let value = serde_json::to_value(response).expect("response should serialize");
        assert!(value.pointer("/report/next_marker").is_none());
        assert!(value.pointer("/report/next_version_idmarker").is_none());
    }

    #[test]
    fn manual_transition_job_record_omits_raw_resume_markers() {
        let (bucket, options, run_mode) =
            parse_manual_transition_query(Some("bucket=data&prefix=logs/&async=true")).expect("async query should parse");
        let mut record = new_manual_transition_job_record(
            "11111111-1111-4111-8111-111111111111".to_string(),
            &bucket,
            &options,
            OffsetDateTime::UNIX_EPOCH,
        );
        record.status = ManualTransitionJobStatus::Partial;
        record.report.next_marker = Some("private/object".to_string());
        record.report.next_version_idmarker = Some("null".to_string());

        assert_eq!(run_mode, ManualTransitionRunMode::Async);
        assert_eq!(
            record.status_endpoint,
            "/rustfs/admin/v3/ilm/transition/jobs/11111111-1111-4111-8111-111111111111"
        );

        let value = serde_json::to_value(record).expect("job record should serialize");
        assert!(value.pointer("/report/next_marker").is_none());
        assert!(value.pointer("/report/next_version_idmarker").is_none());
    }

    #[test]
    fn manual_transition_job_id_rejects_non_uuid_path_segment() {
        let err = parse_manual_transition_job_id("../config").expect_err("path-like job ids must be rejected");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn manual_transition_cancel_marks_active_job_and_token() {
        let (bucket, options, _run_mode) =
            parse_manual_transition_query(Some("bucket=data&prefix=logs/&async=true")).expect("async query should parse");
        let job_id = Uuid::new_v4().to_string();
        let cancel_token = CancellationToken::new();
        let record = new_manual_transition_job_record(job_id.clone(), &bucket, &options, OffsetDateTime::UNIX_EPOCH);
        insert_manual_transition_job(record, cancel_token.clone());

        let cancelled = request_manual_transition_job_cancel(&job_id).expect("active job should be found");

        assert!(cancelled.cancel_requested);
        assert!(cancel_token.is_cancelled());
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
    fn manual_transition_job_status_and_cancel_fail_closed_until_durable_store_exists() {
        let err = durable_manual_transition_jobs_not_implemented_error();

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
        assert_eq!(err.status_code(), Some(StatusCode::NOT_IMPLEMENTED));
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
    fn manual_transition_job_handlers_authorize_validate_and_fail_closed() {
        let src = include_str!("ilm_transition.rs");
        let status_block = extract_block_between_markers(
            src,
            "impl Operation for ManualTransitionJobStatusHandler",
            "pub struct ManualTransitionJobCancelHandler",
        );
        let cancel_block =
            extract_block_between_markers(src, "impl Operation for ManualTransitionJobCancelHandler", "#[cfg(test)]");

        for block in [status_block, cancel_block] {
            let auth = block
                .find("authorize_manual_transition_request(&req).await?;")
                .expect("job route must authorize with SetTierAction");
            let job_id = block
                .find("validate_manual_transition_job_id(&params)?;")
                .expect("job route must validate the path job id");
            let not_implemented = block
                .find("durable_manual_transition_jobs_not_implemented_error()")
                .expect("durable job routes must remain fail-closed until the job store exists");

            assert!(auth < job_id);
            assert!(job_id < not_implemented);
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

    #[test]
    fn manual_transition_background_job_uses_cancelable_scanner_entrypoint() {
        let src = include_str!("ilm_transition.rs");
        let job_block =
            extract_block_between_markers(src, "fn spawn_manual_transition_job", "pub struct ManualTransitionRunHandler");

        assert!(job_block.contains("enqueue_transition_for_existing_objects_scoped_with_cancel"));
        assert!(job_block.contains("CancellationToken"));
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
