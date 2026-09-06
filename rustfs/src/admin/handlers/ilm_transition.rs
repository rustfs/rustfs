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

use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_action_credentials, object_store_from_extensions};
use crate::admin::storage_api::bucket::is_reserved_or_invalid_bucket;
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::lifecycle::{
    IlmRecoveryClassification, IlmRecoveryControlView, IlmRecoveryExportObservation, IlmRecoveryProtocol,
    ManualTransitionCancelCheck, ManualTransitionJobRecord, ManualTransitionJobState, ManualTransitionProgressSink,
    ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport, ManualTransitionScopeAdmission,
    ManualTransitionScopeAdmissionClaim, TransitionOperatorDeleteResult, TransitionOperatorError,
    claim_manual_transition_scope_admission, create_recovery_export, delete_manual_transition_scope_admission_if_current,
    delete_transition_candidate_for_operator, enqueue_transition_for_existing_objects_scoped,
    finalize_missing_transition_transaction_for_operator, inspect_recovery_control, inspect_recovery_export_observation,
    inspect_transition_transaction_for_operator, list_recovery_controls, load_manual_transition_job_record,
    load_manual_transition_scope_admission, load_recovery_export, manual_transition_job_lease_expired,
    manual_transition_queue_snapshot, manual_transition_scope_admission_lease_expired,
    persist_manual_transition_job_progress_if_owned, renew_manual_transition_job_lease_if_owned,
    request_manual_transition_job_cancel, save_manual_transition_job_record, update_manual_transition_job_record,
};
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::storage_api::s3::{S3ErrorCode as AdminS3ErrorCode, error as admin_s3_error};
use crate::admin::utils::json_response;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use http::{HeaderMap, HeaderValue, header};
use hyper::{Method, StatusCode};
use matchit::Params;
use rand::RngExt;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_utils::{
    MaskedAccessKey, base64_decode_url_safe_no_pad, base64_encode_url_safe_no_pad,
    crypto::hex_sha256,
    http::{AMZ_REQUEST_ID, REQUEST_ID_HEADER},
};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration as StdDuration;
use time::{Duration, OffsetDateTime};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

const DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS: u64 = 10_000;
const MAX_MANUAL_TRANSITION_OBJECTS: u64 = 100_000;
const MAX_MANUAL_TRANSITION_DURATION_SECONDS: u64 = 3600;
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ILM_TRANSITION: &str = "ilm_transition";
const EVENT_ADMIN_ILM_TRANSITION_STATE: &str = "admin_ilm_transition_state";
const EVENT_ADMIN_ILM_TRANSITION_RECONCILE: &str = "admin_ilm_transition_reconcile";
const ILM_RECOVERY_OBSERVATION_RECEIPT_TTL: Duration = Duration::minutes(15);
const MAX_ILM_RECOVERY_RECEIPT_SIZE: usize = 32 * 1024;

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
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/recovery/records").as_str(),
        AdminOperation(&IlmRecoveryControlListHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/recovery/records/{{control_id}}").as_str(),
        AdminOperation(&IlmRecoveryControlInspectHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/ilm/recovery/records/{{control_id}}").as_str(),
        AdminOperation(&IlmRecoveryExportCreateHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/recovery/exports/{{export_id}}").as_str(),
        AdminOperation(&IlmRecoveryExportDownloadHandler {}),
    )?;
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryControlListQuery {
    protocol: IlmRecoveryProtocol,
    #[serde(default)]
    classification: Option<IlmRecoveryClassification>,
    #[serde(default = "default_recovery_control_list_limit")]
    limit: usize,
    #[serde(default)]
    marker: Option<String>,
}

const fn default_recovery_control_list_limit() -> usize {
    100
}

fn parse_recovery_control_list_query(query: Option<&str>) -> S3Result<IlmRecoveryControlListQuery> {
    let query = query.ok_or_else(|| admin_s3_error(AdminS3ErrorCode::InvalidRequest, "protocol is required"))?;
    let parsed: IlmRecoveryControlListQuery = serde_urlencoded::from_bytes(query.as_bytes())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InvalidArgument, "invalid ILM recovery control query"))?;
    if !(1..=1_000).contains(&parsed.limit) {
        return Err(admin_s3_error(AdminS3ErrorCode::InvalidArgument, "limit must be between 1 and 1000"));
    }
    if parsed.marker.as_ref().is_some_and(|marker| marker.is_empty()) {
        return Err(admin_s3_error(AdminS3ErrorCode::InvalidArgument, "marker must not be empty"));
    }
    Ok(parsed)
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

/// Recovery endpoints bind their audit actor to the authenticated access key
/// (hashed, never the presented key itself); the credential pre-check keeps
/// this endpoint family's historical missing-credentials message.
async fn authorize_recovery_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<String> {
    if req.credentials.is_none() {
        return Err(admin_s3_error(AdminS3ErrorCode::InvalidRequest, "authentication required"));
    }
    let credentials = authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;
    Ok(recovery_actor_sha256(&credentials))
}

/// The credential pre-check keeps this endpoint family's historical
/// missing-credentials message (the shared gate reports "get cred failed") and
/// still yields the masked actor every transition audit log records.
async fn authorize_transition_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<String> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let actor = MaskedAccessKey(&input_cred.access_key).to_string();

    authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;

    Ok(actor)
}

fn transition_transaction_id_from_params(params: &Params<'_, '_>) -> S3Result<Uuid> {
    Uuid::parse_str(params.get("transaction_id").unwrap_or(""))
        .map_err(|_| s3_error!(InvalidArgument, "invalid transition transaction id"))
}

fn recovery_control_id_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let control_id = params.get("control_id").unwrap_or("");
    validate_recovery_sha256(control_id, "invalid ILM recovery control id")?;
    Ok(control_id.to_string())
}

fn validate_recovery_sha256(value: &str, message: &'static str) -> S3Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(admin_s3_error(AdminS3ErrorCode::InvalidArgument, message));
    }
    Ok(())
}

fn map_recovery_control_error(err: StorageError) -> S3Error {
    if err == StorageError::ConfigNotFound {
        admin_s3_error(AdminS3ErrorCode::NoSuchKey, "ILM recovery control not found")
    } else {
        admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery control request failed")
    }
}

fn recovery_export_id_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let export_id = params.get("export_id").unwrap_or("");
    validate_recovery_sha256(export_id, "invalid ILM recovery export id")?;
    Ok(export_id.to_string())
}

fn map_recovery_export_error(err: StorageError) -> S3Error {
    if err == StorageError::ConfigNotFound {
        admin_s3_error(AdminS3ErrorCode::NoSuchKey, "ILM recovery export not found")
    } else if err == StorageError::SlowDown {
        admin_s3_error(AdminS3ErrorCode::SlowDown, "ILM recovery export admission capacity is exhausted")
    } else if err == StorageError::PreconditionFailed {
        admin_s3_error(AdminS3ErrorCode::OperationAborted, "ILM recovery export observation is stale")
    } else {
        admin_s3_error(AdminS3ErrorCode::OperationAborted, "ILM recovery export request cannot proceed")
    }
}

fn recovery_export_download_headers(export_id: &str, encoded_len: usize) -> S3Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"ilm-recovery-export-{export_id}.json\""))
            .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "invalid ILM recovery export filename"))?,
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&encoded_len.to_string())
            .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "invalid ILM recovery export length"))?,
    );
    Ok(headers)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryReceiptAction {
    Export,
    AbandonRemoteCleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryReceiptMode {
    DryRun,
    Execute,
}

const fn default_recovery_receipt_mode() -> IlmRecoveryReceiptMode {
    IlmRecoveryReceiptMode::Execute
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryObservationReceipt {
    schema: String,
    action: IlmRecoveryReceiptAction,
    // Receipts issued before action modes were introduced represented the
    // existing export execution path, so decode them as Execute until their
    // fixed 15-minute lifetime elapses.
    #[serde(default = "default_recovery_receipt_mode")]
    mode: IlmRecoveryReceiptMode,
    actor_sha256: String,
    issued_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
    nonce: Uuid,
    observation: IlmRecoveryExportObservation,
}

#[derive(Debug, Serialize)]
struct IlmRecoveryControlInspectResponse {
    #[serde(flatten)]
    control: IlmRecoveryControlView,
    export_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    export_not_ready_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observation_receipt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observation_receipt_expires_at_unix_nanos: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryDispositionReasonCode {
    LegacyRemoteCleanupAbandoned,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum IlmRecoveryRecordMutationRequest {
    Export {
        observation_receipt: String,
    },
    AbandonRemoteCleanup {
        mode: IlmRecoveryReceiptMode,
        observation_receipt: String,
        export_id: String,
        export_sha256: String,
        reason_code: IlmRecoveryDispositionReasonCode,
        #[serde(default)]
        confirm: Option<bool>,
        #[serde(default)]
        acknowledge_remote_cleanup_abandoned: Option<bool>,
    },
}

fn parse_recovery_record_mutation_request(body: &[u8]) -> S3Result<IlmRecoveryRecordMutationRequest> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InvalidArgument, "invalid ILM recovery request"))?;
    let request: IlmRecoveryRecordMutationRequest = serde_json::from_value(value.clone())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InvalidArgument, "invalid ILM recovery request"))?;
    if matches!(
        &request,
        IlmRecoveryRecordMutationRequest::AbandonRemoteCleanup {
            mode: IlmRecoveryReceiptMode::DryRun,
            ..
        }
    ) && value
        .as_object()
        .is_some_and(|object| object.contains_key("confirm") || object.contains_key("acknowledge_remote_cleanup_abandoned"))
    {
        return Err(admin_s3_error(
            AdminS3ErrorCode::InvalidArgument,
            "ILM recovery dry-run must not include terminal confirmation fields",
        ));
    }
    Ok(request)
}

#[allow(dead_code)]
enum ValidatedIlmRecoveryRecordMutation<'a> {
    Export {
        observation_receipt: &'a str,
    },
    AbandonDryRun {
        observation_receipt: &'a str,
        export_id: &'a str,
        export_sha256: &'a str,
        reason_code: IlmRecoveryDispositionReasonCode,
    },
    AbandonExecute {
        observation_receipt: &'a str,
        export_id: &'a str,
        export_sha256: &'a str,
        reason_code: IlmRecoveryDispositionReasonCode,
    },
}

fn validate_recovery_record_mutation_request(
    request: &IlmRecoveryRecordMutationRequest,
) -> S3Result<ValidatedIlmRecoveryRecordMutation<'_>> {
    match request {
        IlmRecoveryRecordMutationRequest::Export { observation_receipt } => {
            Ok(ValidatedIlmRecoveryRecordMutation::Export { observation_receipt })
        }
        IlmRecoveryRecordMutationRequest::AbandonRemoteCleanup {
            mode,
            observation_receipt,
            export_id,
            export_sha256,
            reason_code,
            confirm,
            acknowledge_remote_cleanup_abandoned,
        } => {
            validate_recovery_sha256(export_id, "invalid ILM recovery export id")?;
            validate_recovery_sha256(export_sha256, "invalid ILM recovery export checksum")?;
            if observation_receipt.is_empty() {
                return Err(admin_s3_error(
                    AdminS3ErrorCode::InvalidArgument,
                    "ILM recovery observation receipt must not be empty",
                ));
            }
            match mode {
                IlmRecoveryReceiptMode::DryRun if confirm.is_none() && acknowledge_remote_cleanup_abandoned.is_none() => {
                    Ok(ValidatedIlmRecoveryRecordMutation::AbandonDryRun {
                        observation_receipt,
                        export_id,
                        export_sha256,
                        reason_code: *reason_code,
                    })
                }
                IlmRecoveryReceiptMode::DryRun => Err(admin_s3_error(
                    AdminS3ErrorCode::InvalidArgument,
                    "ILM recovery dry-run must not include terminal confirmation fields",
                )),
                IlmRecoveryReceiptMode::Execute
                    if *confirm == Some(true) && *acknowledge_remote_cleanup_abandoned == Some(true) =>
                {
                    Ok(ValidatedIlmRecoveryRecordMutation::AbandonExecute {
                        observation_receipt,
                        export_id,
                        export_sha256,
                        reason_code: *reason_code,
                    })
                }
                IlmRecoveryReceiptMode::Execute => Err(admin_s3_error(
                    AdminS3ErrorCode::InvalidRequest,
                    "ILM recovery disposition requires confirm=true and acknowledge_remote_cleanup_abandoned=true",
                )),
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct IlmRecoveryExportCreateResponse {
    export_id: String,
    export_sha256: String,
    download_url: String,
    outcome: &'static str,
}

// These response envelopes pin the future disposition wire contract before
// its storage state machine is connected to this handler.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryDispositionDryRunStatus {
    Ready,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryDispositionState {
    Applying,
    Completed,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IlmRecoveryDispositionOutcome {
    AcceptedForRecovery,
    Completed,
    Replayed,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryDispositionDryRunResponse {
    action: IlmRecoveryReceiptAction,
    mode: IlmRecoveryReceiptMode,
    status: IlmRecoveryDispositionDryRunStatus,
    disposition_id: String,
    export_id: String,
    export_sha256: String,
    source_generation_sha256: String,
    copy_set_sha256: String,
    source_copy_count: usize,
    observation_receipt: String,
    observation_receipt_expires_at_unix_nanos: i64,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IlmRecoveryDispositionExecuteResponse {
    action: IlmRecoveryReceiptAction,
    mode: IlmRecoveryReceiptMode,
    disposition_id: String,
    state: IlmRecoveryDispositionState,
    outcome: IlmRecoveryDispositionOutcome,
    confirmed_absent_copy_count: usize,
    source_copy_count: usize,
}

fn recovery_actor_sha256(credentials: &Credentials) -> String {
    let access_key = &credentials.access_key;
    let mut bound = Vec::with_capacity(access_key.len() + 40);
    bound.extend_from_slice(b"rustfs-ilm-recovery-actor-v1\0");
    bound.extend_from_slice(access_key.as_bytes());
    hex_sha256(&bound, ToOwned::to_owned)
}

fn recovery_receipt_credentials() -> S3Result<Credentials> {
    current_action_credentials()
        .filter(|credentials| !credentials.secret_key.is_empty())
        .ok_or_else(|| admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery receipt key is unavailable"))
}

fn recovery_receipt_key(credentials: &Credentials) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-ilm-recovery-observation-receipt-v1\0");
    hasher.update(credentials.secret_key.as_bytes());
    hasher.finalize().into()
}

fn encode_recovery_receipt(payload: &IlmRecoveryObservationReceipt, credentials: &Credentials) -> S3Result<String> {
    let payload = serde_json::to_vec(payload)
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "failed to encode ILM recovery receipt"))?;
    if payload.len() > MAX_ILM_RECOVERY_RECEIPT_SIZE {
        return Err(admin_s3_error(
            AdminS3ErrorCode::InternalError,
            "ILM recovery receipt exceeds maximum size",
        ));
    }
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(recovery_receipt_key(credentials)));
    let mut nonce_bytes = [0_u8; 12];
    rand::rng().fill(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(&Nonce::from(nonce_bytes), payload.as_slice())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "failed to seal ILM recovery receipt"))?;
    Ok(format!(
        "{}.{}",
        base64_encode_url_safe_no_pad(&nonce_bytes),
        base64_encode_url_safe_no_pad(&ciphertext)
    ))
}

fn decode_recovery_receipt(token: &str, credentials: &Credentials) -> S3Result<IlmRecoveryObservationReceipt> {
    if token.len() > MAX_ILM_RECOVERY_RECEIPT_SIZE * 2 {
        return Err(admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"));
    }
    let Some((nonce, ciphertext)) = token.split_once('.') else {
        return Err(admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"));
    };
    if ciphertext.contains('.') {
        return Err(admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"));
    }
    let nonce = base64_decode_url_safe_no_pad(nonce.as_bytes())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"))?;
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"))?;
    let ciphertext = base64_decode_url_safe_no_pad(ciphertext.as_bytes())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"))?;
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(recovery_receipt_key(credentials)));
    let plaintext = cipher
        .decrypt(&Nonce::from(nonce), ciphertext.as_slice())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"))?;
    serde_json::from_slice(&plaintext)
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"))
}

fn issue_recovery_observation_receipt(
    observation: IlmRecoveryExportObservation,
    actor_sha256: String,
    action: IlmRecoveryReceiptAction,
    mode: IlmRecoveryReceiptMode,
    now: OffsetDateTime,
) -> S3Result<(String, i64)> {
    let expires_at = now + ILM_RECOVERY_OBSERVATION_RECEIPT_TTL;
    let receipt = IlmRecoveryObservationReceipt {
        schema: "rustfs-ilm-recovery-observation-receipt-v1".to_string(),
        action,
        mode,
        actor_sha256,
        issued_at_unix_nanos: i64::try_from(now.unix_timestamp_nanos())
            .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery receipt timestamp is invalid"))?,
        expires_at_unix_nanos: i64::try_from(expires_at.unix_timestamp_nanos())
            .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery receipt timestamp is invalid"))?,
        nonce: Uuid::new_v4(),
        observation,
    };
    let token = encode_recovery_receipt(&receipt, &recovery_receipt_credentials()?)?;
    Ok((token, receipt.expires_at_unix_nanos))
}

fn validate_recovery_observation_receipt(
    receipt: IlmRecoveryObservationReceipt,
    actor_sha256: &str,
    control_id: &str,
    expected_action: IlmRecoveryReceiptAction,
    expected_mode: IlmRecoveryReceiptMode,
    now_unix_nanos: i64,
) -> S3Result<IlmRecoveryExportObservation> {
    let ttl_nanos = i64::try_from(ILM_RECOVERY_OBSERVATION_RECEIPT_TTL.whole_nanoseconds())
        .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery receipt TTL is invalid"))?;
    if receipt.schema != "rustfs-ilm-recovery-observation-receipt-v1"
        || receipt.action != expected_action
        || receipt.mode != expected_mode
        || receipt.actor_sha256 != actor_sha256
        || receipt.observation.control_id != control_id
        || receipt.nonce.is_nil()
        || receipt.issued_at_unix_nanos <= 0
        || receipt.issued_at_unix_nanos > now_unix_nanos
        || receipt.expires_at_unix_nanos <= now_unix_nanos
        || receipt.expires_at_unix_nanos.checked_sub(receipt.issued_at_unix_nanos) != Some(ttl_nanos)
    {
        return Err(admin_s3_error(AdminS3ErrorCode::AccessDenied, "invalid or expired ILM recovery receipt"));
    }
    Ok(receipt.observation)
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
                    return json_response(StatusCode::ACCEPTED, &response);
                }
                StartManualTransitionJobResult::Conflict(response) => {
                    return json_response(StatusCode::CONFLICT, &response);
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

        json_response(StatusCode::OK, &response)
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
        json_response(StatusCode::OK, &manual_transition_job_response(record))
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
        json_response(StatusCode::OK, &manual_transition_job_response(record))
    }
}

pub struct IlmRecoveryControlListHandler {}

#[async_trait::async_trait]
impl Operation for IlmRecoveryControlListHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_transition_admin_request(&req, AdminAction::ListTierAction).await?;
        let query = parse_recovery_control_list_query(req.uri.query())?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(admin_s3_error(AdminS3ErrorCode::InternalError, "object store is not initialized"));
        };
        let page = list_recovery_controls(store, query.protocol, query.classification, query.limit, query.marker)
            .await
            .map_err(map_recovery_control_error)?;
        json_response(StatusCode::OK, &page)
    }
}

pub struct IlmRecoveryControlInspectHandler {}

#[async_trait::async_trait]
impl Operation for IlmRecoveryControlInspectHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let actor_sha256 = authorize_recovery_admin_request(&req, AdminAction::ListTierAction).await?;
        let control_id = recovery_control_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(admin_s3_error(AdminS3ErrorCode::InternalError, "object store is not initialized"));
        };
        let control = inspect_recovery_control(store.clone(), &control_id)
            .await
            .map_err(map_recovery_control_error)?;
        let now = OffsetDateTime::now_utc();
        let (export_ready, export_not_ready_reason, observation_receipt, expires_at) =
            match inspect_recovery_export_observation(store, &control_id).await {
                Ok(observation) => match issue_recovery_observation_receipt(
                    observation,
                    actor_sha256,
                    IlmRecoveryReceiptAction::Export,
                    IlmRecoveryReceiptMode::Execute,
                    now,
                ) {
                    Ok((token, expires_at)) => (true, None, Some(token), Some(expires_at)),
                    Err(_) => (false, Some("receipt_key_unavailable"), None, None),
                },
                Err(_) => (false, Some("fleet_or_source_not_ready"), None, None),
            };
        json_response(
            StatusCode::OK,
            &IlmRecoveryControlInspectResponse {
                control,
                export_ready,
                export_not_ready_reason,
                observation_receipt,
                observation_receipt_expires_at_unix_nanos: expires_at,
            },
        )
    }
}

pub struct IlmRecoveryExportCreateHandler {}

#[async_trait::async_trait]
impl Operation for IlmRecoveryExportCreateHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let actor_sha256 = authorize_recovery_admin_request(&req, AdminAction::SetTierAction).await?;
        let control_id = recovery_control_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(admin_s3_error(AdminS3ErrorCode::InternalError, "object store is not initialized"));
        };
        let body = req.input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await.map_err(|_| {
            admin_s3_error(AdminS3ErrorCode::InvalidRequest, "ILM recovery export body is too large or unreadable")
        })?;
        let request = parse_recovery_record_mutation_request(&body)?;
        let ValidatedIlmRecoveryRecordMutation::Export { observation_receipt } =
            validate_recovery_record_mutation_request(&request)?
        else {
            return Err(admin_s3_error(AdminS3ErrorCode::InvalidArgument, "unsupported ILM recovery action"));
        };
        let receipt = decode_recovery_receipt(observation_receipt, &recovery_receipt_credentials()?)?;
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos())
            .map_err(|_| admin_s3_error(AdminS3ErrorCode::InternalError, "ILM recovery receipt timestamp is invalid"))?;
        let observation = validate_recovery_observation_receipt(
            receipt,
            &actor_sha256,
            &control_id,
            IlmRecoveryReceiptAction::Export,
            IlmRecoveryReceiptMode::Execute,
            now,
        )?;
        let created = create_recovery_export(store, &observation, &actor_sha256)
            .await
            .map_err(map_recovery_export_error)?;
        let response = IlmRecoveryExportCreateResponse {
            download_url: format!("{ADMIN_PREFIX}/v3/ilm/recovery/exports/{}", created.export_id),
            outcome: if created.replayed { "replayed" } else { "created" },
            export_id: created.export_id,
            export_sha256: created.content_sha256,
        };
        json_response(StatusCode::OK, &response)
    }
}

pub struct IlmRecoveryExportDownloadHandler {}

#[async_trait::async_trait]
impl Operation for IlmRecoveryExportDownloadHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_transition_admin_request(&req, AdminAction::SetTierAction).await?;
        let export_id = recovery_export_id_from_params(&params)?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(admin_s3_error(AdminS3ErrorCode::InternalError, "object store is not initialized"));
        };
        let export = load_recovery_export(store, &export_id)
            .await
            .map_err(map_recovery_export_error)?;
        let headers = recovery_export_download_headers(&export_id, export.encoded.len())?;
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(export.encoded)), headers))
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
        json_response(StatusCode::OK, &status)
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
                json_response(StatusCode::OK, &TransitionCandidateDeleteResponse { outcome, result })
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
                    StatusCode::OK,
                    &TransitionFinalizeMissingResponse {
                        outcome: "journal_finalized",
                        journal_retained: false,
                        transaction_id,
                    },
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

    fn with_recovery_control_params<T>(path: &str, f: impl FnOnce(&Params<'_, '_>) -> T) -> T {
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/ilm/recovery/records/{control_id}", ())
            .expect("route should insert");
        let matched = router.at(path).expect("route should match");
        f(&matched.params)
    }

    #[test]
    fn recovery_control_query_is_bounded_and_strict() {
        let query = parse_recovery_control_list_query(Some("protocol=transition_transaction"))
            .expect("minimal recovery query should parse");
        assert_eq!(query.protocol, IlmRecoveryProtocol::TransitionTransaction);
        assert_eq!(query.classification, None);
        assert_eq!(query.limit, 100);

        let filtered = parse_recovery_control_list_query(Some(
            "protocol=tier_delete_journal&classification=retained_ambiguous&limit=1000&marker=opaque",
        ))
        .expect("bounded filtered query should parse");
        assert_eq!(filtered.protocol, IlmRecoveryProtocol::TierDeleteJournal);
        assert_eq!(filtered.classification, Some(IlmRecoveryClassification::RetainedAmbiguous));
        assert_eq!(filtered.limit, 1000);
        assert!(parse_recovery_control_list_query(None).is_err());
        assert!(parse_recovery_control_list_query(Some("protocol=transition_transaction&limit=0")).is_err());
        assert!(parse_recovery_control_list_query(Some("protocol=transition_transaction&limit=1001")).is_err());
        assert!(parse_recovery_control_list_query(Some("protocol=unknown")).is_err());
        assert!(parse_recovery_control_list_query(Some("protocol=transition_transaction&extra=true")).is_err());
    }

    #[test]
    fn recovery_control_id_is_canonical_lowercase_sha256() {
        let id = "ab".repeat(32);
        with_recovery_control_params(&format!("/rustfs/admin/v3/ilm/recovery/records/{id}"), |params| {
            assert_eq!(recovery_control_id_from_params(params).expect("control id should parse"), id);
        });
        let uppercase = "AB".repeat(32);
        with_recovery_control_params(&format!("/rustfs/admin/v3/ilm/recovery/records/{uppercase}"), |params| {
            assert!(recovery_control_id_from_params(params).is_err())
        });
    }

    #[test]
    fn recovery_observation_receipt_is_opaque_actor_bound_and_tamper_evident() {
        assert_eq!(ILM_RECOVERY_OBSERVATION_RECEIPT_TTL.whole_seconds(), 900);
        let control_id = "ab".repeat(32);
        let content_sha256 = hex_sha256(b"legacy", ToOwned::to_owned);
        let copy_set_sha256 = hex_sha256(
            &serde_json::to_vec(&serde_json::json!([{
                "authority": "pool-0/set-0",
                "canonical_path": "ilm/tier-delete-journal/legacy.json",
                "etag": "etag-a",
                "encoded_len": 6,
                "content_sha256": content_sha256,
            }]))
            .unwrap(),
            ToOwned::to_owned,
        );
        let observation: IlmRecoveryExportObservation = serde_json::from_value(serde_json::json!({
            "control_id": control_id,
            "protocol": "tier_delete_journal",
            "control_etag": "control-etag",
            "control_revision": 1,
            "classification": "retained_ambiguous",
            "canonical_source_path": "ilm/tier-delete-journal/legacy.json",
            "source_generation": {
                "source_schema": "rustfs-tier-delete-journal-v1",
                "source_etag": "etag-a",
                "content_sha256": content_sha256,
                "copy_set_sha256": copy_set_sha256,
                "copies": [{
                    "authority": "pool-0/set-0",
                    "canonical_path": "ilm/tier-delete-journal/legacy.json",
                    "etag": "etag-a",
                    "encoded_len": 6,
                    "content_sha256": content_sha256,
                }]
            },
            "topology_generation": hex_sha256(b"topology", ToOwned::to_owned),
            "member_epochs_sha256": hex_sha256(b"members", ToOwned::to_owned),
        }))
        .unwrap();
        let payload = IlmRecoveryObservationReceipt {
            schema: "rustfs-ilm-recovery-observation-receipt-v1".to_string(),
            action: IlmRecoveryReceiptAction::Export,
            mode: IlmRecoveryReceiptMode::Execute,
            actor_sha256: hex_sha256(b"actor-a", ToOwned::to_owned),
            issued_at_unix_nanos: 1,
            expires_at_unix_nanos: 1 + ILM_RECOVERY_OBSERVATION_RECEIPT_TTL.whole_nanoseconds() as i64,
            nonce: Uuid::new_v4(),
            observation,
        };
        let credentials = Credentials {
            access_key: "root".to_string(),
            secret_key: "secret".to_string(),
            ..Default::default()
        };
        let token = encode_recovery_receipt(&payload, &credentials).unwrap();
        assert!(!token.contains("actor-a"));
        assert!(!token.contains("ilm/tier-delete-journal"));
        assert_eq!(decode_recovery_receipt(&token, &credentials).unwrap(), payload);
        assert!(
            validate_recovery_observation_receipt(
                payload.clone(),
                &payload.actor_sha256,
                &payload.observation.control_id,
                IlmRecoveryReceiptAction::Export,
                IlmRecoveryReceiptMode::Execute,
                payload.issued_at_unix_nanos,
            )
            .is_ok()
        );
        let assert_denied = |receipt: IlmRecoveryObservationReceipt, actor: &str, control: &str, now: i64| {
            let err = validate_recovery_observation_receipt(
                receipt,
                actor,
                control,
                IlmRecoveryReceiptAction::Export,
                IlmRecoveryReceiptMode::Execute,
                now,
            )
            .expect_err("invalid observation receipt must be denied");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        };
        assert_denied(
            payload.clone(),
            &hex_sha256(b"actor-b", ToOwned::to_owned),
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        assert_denied(payload.clone(), &payload.actor_sha256, &"cd".repeat(32), payload.issued_at_unix_nanos);
        assert_denied(
            payload.clone(),
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.expires_at_unix_nanos,
        );

        let mut invalid = payload.clone();
        invalid.schema = "rustfs-ilm-recovery-observation-receipt-v2".to_string();
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        let mut invalid = payload.clone();
        invalid.action = IlmRecoveryReceiptAction::AbandonRemoteCleanup;
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        let mut invalid = payload.clone();
        invalid.mode = IlmRecoveryReceiptMode::DryRun;
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        let mut invalid = payload.clone();
        invalid.nonce = Uuid::nil();
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        let mut invalid = payload.clone();
        invalid.issued_at_unix_nanos += 1;
        invalid.expires_at_unix_nanos += 1;
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );
        let mut invalid = payload.clone();
        invalid.expires_at_unix_nanos += 1;
        assert_denied(
            invalid,
            &payload.actor_sha256,
            &payload.observation.control_id,
            payload.issued_at_unix_nanos,
        );

        let mut tampered = token.into_bytes();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'a' { b'b' } else { b'a' };
        let err = decode_recovery_receipt(std::str::from_utf8(&tampered).unwrap(), &credentials)
            .expect_err("tampered receipt must be denied");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

        let mut legacy_payload = serde_json::to_value(&payload).unwrap();
        legacy_payload.as_object_mut().unwrap().remove("mode");
        let legacy_payload: IlmRecoveryObservationReceipt = serde_json::from_value(legacy_payload).unwrap();
        assert_eq!(legacy_payload.mode, IlmRecoveryReceiptMode::Execute);
    }

    #[test]
    fn recovery_actor_binding_uses_the_authenticated_presented_access_key() {
        let first = Credentials {
            access_key: "operator-a".to_string(),
            secret_key: "first-secret".to_string(),
            ..Default::default()
        };
        let same_actor_rotated_secret = Credentials {
            access_key: first.access_key.clone(),
            secret_key: "rotated-secret".to_string(),
            ..Default::default()
        };
        let other = Credentials {
            access_key: "operator-b".to_string(),
            secret_key: first.secret_key.clone(),
            ..Default::default()
        };

        let actor = recovery_actor_sha256(&first);
        assert_eq!(actor, recovery_actor_sha256(&same_actor_rotated_secret));
        assert_ne!(actor, recovery_actor_sha256(&other));
        assert!(!actor.contains(&first.access_key));

        let production = include_str!("ilm_transition.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");
        let gate = extract_block_between_markers(
            production,
            "async fn authorize_recovery_admin_request",
            "async fn authorize_transition_admin_request",
        );
        assert!(gate.contains("let credentials = authorize_admin_request("));
        assert!(gate.contains("recovery_actor_sha256(&credentials)"));
        assert!(!gate.contains("MaskedAccessKey"));
    }

    #[test]
    fn recovery_record_mutation_wire_contract_is_strict_and_mode_specific() {
        let export = parse_recovery_record_mutation_request(br#"{"action":"export","observation_receipt":"opaque"}"#).unwrap();
        assert!(matches!(
            validate_recovery_record_mutation_request(&export),
            Ok(ValidatedIlmRecoveryRecordMutation::Export {
                observation_receipt: "opaque"
            })
        ));

        let export_id = "ab".repeat(32);
        let export_sha256 = "cd".repeat(32);
        let dry_run_json = format!(
            r#"{{"action":"abandon_remote_cleanup","mode":"dry_run","observation_receipt":"opaque-dry-run","export_id":"{export_id}","export_sha256":"{export_sha256}","reason_code":"legacy_remote_cleanup_abandoned"}}"#
        );
        let dry_run = parse_recovery_record_mutation_request(dry_run_json.as_bytes()).unwrap();
        assert!(matches!(
            validate_recovery_record_mutation_request(&dry_run),
            Ok(ValidatedIlmRecoveryRecordMutation::AbandonDryRun {
                observation_receipt: "opaque-dry-run",
                export_id: observed_export_id,
                export_sha256: observed_export_sha256,
                reason_code: IlmRecoveryDispositionReasonCode::LegacyRemoteCleanupAbandoned,
            }) if observed_export_id == export_id && observed_export_sha256 == export_sha256
        ));

        let execute_json = format!(
            r#"{{"action":"abandon_remote_cleanup","mode":"execute","confirm":true,"acknowledge_remote_cleanup_abandoned":true,"observation_receipt":"opaque-execute","export_id":"{export_id}","export_sha256":"{export_sha256}","reason_code":"legacy_remote_cleanup_abandoned"}}"#
        );
        let execute = parse_recovery_record_mutation_request(execute_json.as_bytes()).unwrap();
        assert!(matches!(
            validate_recovery_record_mutation_request(&execute),
            Ok(ValidatedIlmRecoveryRecordMutation::AbandonExecute {
                observation_receipt: "opaque-execute",
                export_id: observed_export_id,
                export_sha256: observed_export_sha256,
                reason_code: IlmRecoveryDispositionReasonCode::LegacyRemoteCleanupAbandoned,
            }) if observed_export_id == export_id && observed_export_sha256 == export_sha256
        ));

        let dry_run_with_confirmation = dry_run_json.replace(r#""mode":"dry_run""#, r#""mode":"dry_run","confirm":false"#);
        assert!(parse_recovery_record_mutation_request(dry_run_with_confirmation.as_bytes()).is_err());
        assert!(parse_recovery_record_mutation_request(dry_run_json.replace('}', r#","confirm":null}"#).as_bytes()).is_err());

        let uppercase_export_id = "AB".repeat(32);
        for invalid in [
            execute_json.replace(r#""confirm":true,"#, ""),
            execute_json.replace(r#""confirm":true"#, r#""confirm":false"#),
            execute_json.replace(
                r#""acknowledge_remote_cleanup_abandoned":true"#,
                r#""acknowledge_remote_cleanup_abandoned":false"#,
            ),
            execute_json.replace(export_id.as_str(), uppercase_export_id.as_str()),
            execute_json.replace(export_sha256.as_str(), "too-short"),
            execute_json.replace("opaque-execute", ""),
        ] {
            if let Ok(request) = parse_recovery_record_mutation_request(invalid.as_bytes()) {
                assert!(
                    validate_recovery_record_mutation_request(&request).is_err(),
                    "request should fail closed: {invalid}"
                );
            }
        }

        for invalid in [
            br#"{"action":"export","observation_receipt":"opaque","extra":true}"#.as_slice(),
            br#"{"action":"abandon_remote_cleanup","mode":"preview"}"#.as_slice(),
            br#"{"action":"unknown","observation_receipt":"opaque"}"#.as_slice(),
        ] {
            assert!(parse_recovery_record_mutation_request(invalid).is_err());
        }
    }

    #[test]
    fn recovery_disposition_response_wire_contract_is_closed() {
        let export = IlmRecoveryExportCreateResponse {
            export_id: "ab".repeat(32),
            export_sha256: "cd".repeat(32),
            download_url: "/rustfs/admin/v3/ilm/recovery/exports/export-id".to_string(),
            outcome: "created",
        };
        assert_eq!(
            serde_json::to_value(&export).unwrap(),
            serde_json::json!({
                "export_id": "ab".repeat(32),
                "export_sha256": "cd".repeat(32),
                "download_url": "/rustfs/admin/v3/ilm/recovery/exports/export-id",
                "outcome": "created",
            })
        );

        let dry_run = IlmRecoveryDispositionDryRunResponse {
            action: IlmRecoveryReceiptAction::AbandonRemoteCleanup,
            mode: IlmRecoveryReceiptMode::DryRun,
            status: IlmRecoveryDispositionDryRunStatus::Ready,
            disposition_id: "ab".repeat(32),
            export_id: "cd".repeat(32),
            export_sha256: "ef".repeat(32),
            source_generation_sha256: "12".repeat(32),
            copy_set_sha256: "34".repeat(32),
            source_copy_count: 2,
            observation_receipt: "opaque-execute".to_string(),
            observation_receipt_expires_at_unix_nanos: 900_000_000_001,
        };
        let dry_run_json = serde_json::to_value(&dry_run).unwrap();
        assert_eq!(dry_run_json["action"], "abandon_remote_cleanup");
        assert_eq!(dry_run_json["mode"], "dry_run");
        assert_eq!(dry_run_json["status"], "ready");
        assert_eq!(
            serde_json::from_value::<IlmRecoveryDispositionDryRunResponse>(dry_run_json).unwrap(),
            dry_run
        );

        let execute = IlmRecoveryDispositionExecuteResponse {
            action: IlmRecoveryReceiptAction::AbandonRemoteCleanup,
            mode: IlmRecoveryReceiptMode::Execute,
            disposition_id: "ab".repeat(32),
            state: IlmRecoveryDispositionState::Applying,
            outcome: IlmRecoveryDispositionOutcome::AcceptedForRecovery,
            confirmed_absent_copy_count: 1,
            source_copy_count: 2,
        };
        let execute_json = serde_json::to_value(&execute).unwrap();
        assert_eq!(execute_json["state"], "applying");
        assert_eq!(execute_json["outcome"], "accepted_for_recovery");
        assert_eq!(
            serde_json::from_value::<IlmRecoveryDispositionExecuteResponse>(execute_json).unwrap(),
            execute
        );

        let mut unknown = serde_json::to_value(&dry_run).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<IlmRecoveryDispositionDryRunResponse>(unknown).is_err());
    }

    #[test]
    fn recovery_export_download_headers_prevent_caching_and_force_attachment() {
        let export_id = "ab".repeat(32);
        let headers = recovery_export_download_headers(&export_id, 123).unwrap();
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "123");
        assert_eq!(
            headers.get(header::CONTENT_DISPOSITION).unwrap(),
            &format!("attachment; filename=\"ilm-recovery-export-{export_id}.json\"")
        );
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

    /// The transition wrapper now delegates to the shared admin gate, which reports
    /// "get cred failed"; its own pre-check keeps the message these endpoints have
    /// always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn transition_admin_gate_keeps_its_missing_credentials_response() {
        let err = authorize_transition_admin_request(
            &manual_transition_job_request(Method::GET, "/rustfs/admin/v3/ilm/transition/jobs/job-123"),
            AdminAction::ListTierAction,
        )
        .await
        .expect_err("a transition admin request without credentials must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("authentication required"));
    }

    #[tokio::test]
    async fn recovery_admin_gate_keeps_its_missing_credentials_response() {
        let err = authorize_recovery_admin_request(
            &manual_transition_job_request(Method::GET, "/rustfs/admin/v3/ilm/recovery/controls/control-123"),
            AdminAction::ListTierAction,
        )
        .await
        .expect_err("a recovery admin request without credentials must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("authentication required"));
    }

    #[test]
    fn transition_admin_gate_routes_through_the_shared_gate() {
        let production = include_str!("ilm_transition.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");
        let wrapper = extract_block_between_markers(
            production,
            "async fn authorize_transition_admin_request",
            "fn transition_transaction_id_from_params",
        );

        assert_eq!(
            wrapper.matches("authorize_admin_request(").count(),
            1,
            "the transition wrapper must use exactly one shared gate"
        );
        assert!(
            wrapper.contains("authorize_admin_request(req, vec![Action::AdminAction(action)])"),
            "the transition wrapper must forward its parameterized action unchanged"
        );
        assert!(
            wrapper.contains("MaskedAccessKey(&input_cred.access_key)"),
            "the transition wrapper must keep returning the masked actor"
        );
        assert!(!production.contains("check_key_valid(get_session_token"));
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
