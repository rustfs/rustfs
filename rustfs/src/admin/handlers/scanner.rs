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
use crate::admin::handlers::supervise_admin_mutation;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    app_context_from_req, current_object_store_handle_for_context, current_scanner_metrics_report,
};
use crate::admin::storage_api::ScannerDataMovementPauseStatus;
use crate::module_switches::{ENV_SCANNER_ENABLED, scanner_enabled_from_env};
use crate::server::ADMIN_PREFIX;
use chrono::Utc;
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
#[cfg(test)]
use rustfs_scanner_metrics::metrics::ScannerLifecycleTransitionSnapshot;
use rustfs_scanner_metrics::metrics::{ScannerLifecycleExpirySnapshot, ScannerMaintenanceControlSnapshot, ScannerMetricsReport};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, Serialize)]
struct ScannerStatusResponse {
    enabled: bool,
    disabled_reason: Option<String>,
    freshness: ScannerFreshnessStatus,
    metrics: ScannerMetricsReport,
    cycle_schedule: rustfs_scanner::ScannerCycleScheduleStatus,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
    cycle_recovery: rustfs_scanner::ScannerCycleRecoveryStatus,
    data_movement_pause: ScannerDataMovementPauseStatus,
    pause_backlog: rustfs_scanner::ScannerPauseBacklogStatus,
    catch_up_estimate: ScannerCatchUpEstimate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerCycleResetRequest {
    mode: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannerUsageStateResetRequest {
    mode: String,
    #[serde(default, rename = "async")]
    async_intent: bool,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScannerRecoveryIntentResponse {
    status: &'static str,
    action: String,
    mode: String,
    intent_id: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct ScannerRecoveryIntentConflictResponse {
    status: &'static str,
    intent_id: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct ScannerFreshnessStatus {
    state: &'static str,
    last_cycle_end_unix_secs: u64,
    max_expected_age_seconds: u64,
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct ScannerCatchUpEstimate {
    estimated: bool,
    movement_work_items: u64,
    dirty_usage_buckets: u64,
    discovered_expiry_items: u64,
    discovered_transition_items: u64,
    undiscovered_ilm_items_known: bool,
    usage_baseline_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct IlmExpiryStatusResponse {
    enabled: bool,
    disabled_reason: Option<String>,
    freshness: ScannerFreshnessStatus,
    lifecycle_expiry: ScannerLifecycleExpirySnapshot,
    maintenance_control: ScannerMaintenanceControlSnapshot,
    current_cycle_lifecycle_expiry_actions: u64,
    last_cycle_lifecycle_expiry_actions: u64,
    data_movement_pause: ScannerDataMovementPauseStatus,
    pause_backlog: rustfs_scanner::ScannerPauseBacklogStatus,
    catch_up_estimate: ScannerCatchUpEstimate,
}

fn scanner_catch_up_estimate(
    pause: &ScannerDataMovementPauseStatus,
    backlog: &rustfs_scanner::ScannerPauseBacklogStatus,
    metrics: &ScannerMetricsReport,
) -> ScannerCatchUpEstimate {
    ScannerCatchUpEstimate {
        estimated: pause.paused || backlog.phase != rustfs_scanner::ScannerPauseBacklogPhase::Idle,
        movement_work_items: pause.movement_backlog_work_items.max(backlog.movement_work_items),
        dirty_usage_buckets: metrics.usage_freshness.dirty_pending_buckets.max(backlog.dirty_usage_buckets),
        discovered_expiry_items: metrics
            .lifecycle_expiry
            .current_queued
            .saturating_add(metrics.lifecycle_expiry.current_active)
            .max(backlog.discovered_expiry_items),
        discovered_transition_items: metrics
            .lifecycle_transition
            .current_queued
            .saturating_add(metrics.lifecycle_transition.current_active)
            .saturating_add(metrics.lifecycle_transition.compensation_pending)
            .saturating_add(metrics.lifecycle_transition.compensation_running)
            .max(backlog.discovered_transition_items),
        undiscovered_ilm_items_known: !pause.paused && !backlog.pending_full_scan,
        usage_baseline_unix_secs: metrics.usage_freshness.last_durable_success_unix_secs,
    }
}

fn unavailable_pause_backlog(error: &str) -> rustfs_scanner::ScannerPauseBacklogStatus {
    rustfs_scanner::ScannerPauseBacklogStatus {
        persistence_state: "unavailable".to_string(),
        alerting: true,
        alert_reasons: vec![rustfs_scanner::ScannerPauseBacklogAlertReason::PersistenceUnavailable],
        thresholds: rustfs_scanner::ScannerPauseBacklogThresholds::default(),
        error: Some(error.to_string()),
        ..Default::default()
    }
}

fn scanner_disabled_reason(enabled: bool) -> Option<String> {
    (!enabled).then(|| format!("disabled by {ENV_SCANNER_ENABLED}"))
}

fn scanner_freshness_status(
    metrics: &ScannerMetricsReport,
    runtime_config: &rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
    effective_cycle_interval_seconds: u64,
) -> ScannerFreshnessStatus {
    const FRESHNESS_MULTIPLIER: u64 = 2;

    let expected_cycle_interval_seconds = runtime_config
        .cycle_interval_seconds
        .value
        .max(effective_cycle_interval_seconds);
    let max_expected_age_seconds = expected_cycle_interval_seconds.saturating_mul(FRESHNESS_MULTIPLIER);
    if metrics.last_cycle_end_unix_secs == 0 {
        return ScannerFreshnessStatus {
            state: "unknown",
            last_cycle_end_unix_secs: 0,
            max_expected_age_seconds,
            reason: Some("no completed cycle recorded"),
        };
    }

    let now = Utc::now().timestamp().max(0) as u64;
    let age = now.saturating_sub(metrics.last_cycle_end_unix_secs);
    if max_expected_age_seconds > 0 && age > max_expected_age_seconds {
        return ScannerFreshnessStatus {
            state: "stale",
            last_cycle_end_unix_secs: metrics.last_cycle_end_unix_secs,
            max_expected_age_seconds,
            reason: Some("last cycle is older than freshness window"),
        };
    }

    ScannerFreshnessStatus {
        state: "fresh",
        last_cycle_end_unix_secs: metrics.last_cycle_end_unix_secs,
        max_expected_age_seconds,
        reason: None,
    }
}

fn scanner_status_response(
    enabled: bool,
    metrics: ScannerMetricsReport,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
    cycle_schedule: rustfs_scanner::ScannerCycleScheduleStatus,
    data_movement_pause: ScannerDataMovementPauseStatus,
    pause_backlog: rustfs_scanner::ScannerPauseBacklogStatus,
) -> ScannerStatusResponse {
    let freshness = scanner_freshness_status(&metrics, &runtime_config, cycle_schedule.effective_interval_seconds());
    let catch_up_estimate = scanner_catch_up_estimate(&data_movement_pause, &pause_backlog, &metrics);
    ScannerStatusResponse {
        enabled,
        disabled_reason: scanner_disabled_reason(enabled),
        freshness,
        metrics,
        cycle_schedule,
        runtime_config,
        cycle_recovery: rustfs_scanner::scanner::scanner_cycle_recovery_status(),
        data_movement_pause,
        pause_backlog,
        catch_up_estimate,
    }
}

fn ilm_expiry_status_response(
    enabled: bool,
    metrics: ScannerMetricsReport,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
    cycle_schedule: rustfs_scanner::ScannerCycleScheduleStatus,
    data_movement_pause: ScannerDataMovementPauseStatus,
    pause_backlog: rustfs_scanner::ScannerPauseBacklogStatus,
) -> IlmExpiryStatusResponse {
    let freshness = scanner_freshness_status(&metrics, &runtime_config, cycle_schedule.effective_interval_seconds());
    let catch_up_estimate = scanner_catch_up_estimate(&data_movement_pause, &pause_backlog, &metrics);
    IlmExpiryStatusResponse {
        enabled,
        disabled_reason: scanner_disabled_reason(enabled),
        freshness,
        lifecycle_expiry: metrics.lifecycle_expiry,
        maintenance_control: metrics.maintenance_control,
        current_cycle_lifecycle_expiry_actions: metrics.current_cycle_lifecycle_expiry_actions,
        last_cycle_lifecycle_expiry_actions: metrics.last_cycle_lifecycle_expiry_actions,
        data_movement_pause,
        pause_backlog,
        catch_up_estimate,
    }
}

pub fn register_scanner_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/scanner/status").as_str(),
        AdminOperation(&ScannerStatusHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/scanner/cycle-state/reset").as_str(),
        AdminOperation(&ScannerCycleStateResetHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/scanner/usage-state/reset").as_str(),
        AdminOperation(&ScannerUsageStateResetHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/scanner/usage-state/recovery-intents/{{intent_id}}").as_str(),
        AdminOperation(&ScannerUsageStateRecoveryIntentStatusHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/ilm/expiry/status").as_str(),
        AdminOperation(&IlmExpiryStatusHandler {}),
    )?;

    Ok(())
}

/// The pre-check keeps these endpoints' historical missing-credentials message;
/// the shared gate reports "get cred failed".
async fn validate_scanner_status_request(req: &S3Request<Body>) -> S3Result<Credentials> {
    if req.credentials.is_none() {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
    }

    authorize_admin_request(req, vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)]).await
}

async fn validate_scanner_reset_request(req: &S3Request<Body>) -> S3Result<Credentials> {
    if req.credentials.is_none() {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
    }
    authorize_admin_request(req, vec![Action::AdminAction(AdminAction::ConfigUpdateAdminAction)]).await
}

fn json_response(body: Vec<u8>) -> S3Result<S3Response<(StatusCode, Body)>> {
    json_response_with_status(StatusCode::OK, body)
}

fn json_response_with_status(status: StatusCode, body: Vec<u8>) -> S3Result<S3Response<(StatusCode, Body)>> {
    let mut headers = HeaderMap::new();
    let content_type = HeaderValue::from_str(JSON_CONTENT_TYPE)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))?;
    headers.insert(CONTENT_TYPE, content_type);
    Ok(S3Response::with_headers((status, Body::from(body)), headers))
}

fn scanner_recovery_intent_error(err: rustfs_scanner::ScannerError) -> S3Error {
    let message = err.to_string();
    let code = if message.contains("action is unsupported")
        || message.contains("mode is unsupported")
        || message.contains("actor identity is invalid")
        || message.contains("intent id is invalid")
        || message.contains("idempotency key")
        || message.contains("requires")
    {
        S3ErrorCode::InvalidRequest
    } else {
        S3ErrorCode::InternalError
    };
    S3Error::with_message(code, message)
}

fn scanner_recovery_intent_record_response(
    status_code: StatusCode,
    status: &'static str,
    record: rustfs_scanner::ScannerRecoveryIntentRecord,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let response = ScannerRecoveryIntentResponse {
        status,
        action: record.action,
        mode: record.mode,
        intent_id: record.intent_id,
        state: record.state,
    };
    let body = serde_json::to_vec(&response).map_err(|err| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to encode scanner recovery intent response: {err}"),
        )
    })?;
    json_response_with_status(status_code, body)
}

fn scanner_recovery_intent_accept_response(
    result: rustfs_scanner::ScannerRecoveryIntentAcceptResult,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    match result {
        rustfs_scanner::ScannerRecoveryIntentAcceptResult::Accepted { record } => {
            scanner_recovery_intent_record_response(StatusCode::ACCEPTED, "accepted", record)
        }
        rustfs_scanner::ScannerRecoveryIntentAcceptResult::Replayed { record } => {
            scanner_recovery_intent_record_response(StatusCode::ACCEPTED, "replayed", record)
        }
        rustfs_scanner::ScannerRecoveryIntentAcceptResult::Conflict { existing } => {
            let response = ScannerRecoveryIntentConflictResponse {
                status: "conflict",
                intent_id: existing.intent_id,
                state: existing.state,
            };
            let body = serde_json::to_vec(&response).map_err(|err| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to encode scanner recovery intent conflict: {err}"),
                )
            })?;
            json_response_with_status(StatusCode::CONFLICT, body)
        }
    }
}

fn scanner_recovery_intent_executor_id(result: &rustfs_scanner::ScannerRecoveryIntentAcceptResult) -> Option<String> {
    match result {
        rustfs_scanner::ScannerRecoveryIntentAcceptResult::Accepted { record }
        | rustfs_scanner::ScannerRecoveryIntentAcceptResult::Replayed { record }
            if matches!(record.state.as_str(), "accepted" | "running") =>
        {
            Some(record.intent_id.clone())
        }
        _ => None,
    }
}

pub struct ScannerStatusHandler {}

#[async_trait::async_trait]
impl Operation for ScannerStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_status_request(&req).await?;
        let enabled = scanner_enabled_from_env();
        let metrics = current_scanner_metrics_report().await;
        let runtime_config = rustfs_scanner::scanner_runtime_config_status();
        let cycle_schedule = rustfs_scanner::scanner_cycle_schedule_status();
        let store =
            app_context_from_req(&req).and_then(|context| current_object_store_handle_for_context(Some(context.as_ref())));
        let (data_movement_pause, pause_backlog) = match store {
            Some(store) => (
                store.scanner_data_movement_pause_status().await,
                rustfs_scanner::scanner_pause_backlog_status(store).await,
            ),
            None => (
                ScannerDataMovementPauseStatus::default(),
                unavailable_pause_backlog("storage layer not initialized"),
            ),
        };
        let response =
            scanner_status_response(enabled, metrics, runtime_config, cycle_schedule, data_movement_pause, pause_backlog);
        let body = serde_json::to_vec(&response).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode scanner status: {err}"))
        })?;

        json_response(body)
    }
}

pub struct IlmExpiryStatusHandler {}

pub struct ScannerCycleStateResetHandler {}

pub struct ScannerUsageStateResetHandler {}

pub struct ScannerUsageStateRecoveryIntentStatusHandler {}

#[async_trait::async_trait]
impl Operation for ScannerCycleStateResetHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_reset_request(&req).await?;
        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|err| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid reset request body: {err}")))?;
        let reset = serde_json::from_slice::<ScannerCycleResetRequest>(&body)
            .map_err(|err| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid reset request body: {err}")))?;
        if reset.mode != "full-rescan" {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "reset mode must be full-rescan"));
        }
        let context = app_context_from_req(&req)
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        let store = current_object_store_handle_for_context(Some(context.as_ref()))
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        supervise_admin_mutation("scanner cycle state reset", async move {
            rustfs_scanner::scanner::reset_scanner_cycle_recovery(CancellationToken::new(), store)
                .await
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err.to_string()))?;
            Ok::<_, S3Error>(())
        })
        .await?;
        json_response(br#"{"status":"reset","mode":"full-rescan"}"#.to_vec())
    }
}

#[async_trait::async_trait]
impl Operation for ScannerUsageStateResetHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_reset_request(&req).await?;
        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|err| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid reset request body: {err}")))?;
        let reset = serde_json::from_slice::<ScannerUsageStateResetRequest>(&body)
            .map_err(|err| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid reset request body: {err}")))?;
        if reset.mode != "full-rebuild" {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "reset mode must be full-rebuild"));
        }
        let context = app_context_from_req(&req)
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        let store = current_object_store_handle_for_context(Some(context.as_ref()))
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        if reset.async_intent {
            let idempotency_key = reset
                .idempotency_key
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "async reset requires idempotency_key"))?;
            let request = rustfs_scanner::ScannerRecoveryIntentRequest {
                action: rustfs_scanner::SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD.to_string(),
                mode: reset.mode,
                idempotency_key,
                actor_sha256: rustfs_scanner::scanner_recovery_actor_sha256(&_cred.access_key),
            };
            let executor_store = store.clone();
            let accepted = rustfs_scanner::accept_scanner_usage_recovery_intent(store, request)
                .await
                .map_err(scanner_recovery_intent_error)?;
            if let Some(intent_id) = scanner_recovery_intent_executor_id(&accepted) {
                tokio::spawn(async move {
                    let _ = rustfs_scanner::scanner::run_scanner_usage_recovery_intent(
                        CancellationToken::new(),
                        executor_store,
                        intent_id,
                    )
                    .await;
                });
            }
            return scanner_recovery_intent_accept_response(accepted);
        }
        if reset.idempotency_key.is_some() {
            return Err(S3Error::with_message(S3ErrorCode::InvalidRequest, "idempotency_key requires async reset"));
        }
        let result = supervise_admin_mutation("scanner usage state reset", async move {
            rustfs_scanner::scanner::reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store)
                .await
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err.to_string()))
        })
        .await?;
        let body = serde_json::to_vec(&result).map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("failed to encode scanner usage reset response: {err}"),
            )
        })?;
        json_response(body)
    }
}

#[async_trait::async_trait]
impl Operation for ScannerUsageStateRecoveryIntentStatusHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_reset_request(&req).await?;
        let intent_id = params.get("intent_id").unwrap_or("");
        let context = app_context_from_req(&req)
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        let store = current_object_store_handle_for_context(Some(context.as_ref()))
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "storage layer not initialized"))?;
        let record = rustfs_scanner::get_scanner_usage_recovery_intent(store, intent_id)
            .await
            .map_err(scanner_recovery_intent_error)?
            .ok_or_else(|| {
                let mut err = S3Error::with_message(S3ErrorCode::NoSuchKey, "scanner recovery intent not found");
                err.set_status_code(StatusCode::NOT_FOUND);
                err
            })?;
        scanner_recovery_intent_record_response(StatusCode::OK, "found", record)
    }
}

#[async_trait::async_trait]
impl Operation for IlmExpiryStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_status_request(&req).await?;
        let enabled = scanner_enabled_from_env();
        let metrics = current_scanner_metrics_report().await;
        let runtime_config = rustfs_scanner::scanner_runtime_config_status();
        let cycle_schedule = rustfs_scanner::scanner_cycle_schedule_status();
        let store =
            app_context_from_req(&req).and_then(|context| current_object_store_handle_for_context(Some(context.as_ref())));
        let (data_movement_pause, pause_backlog) = match store {
            Some(store) => (
                store.scanner_data_movement_pause_status().await,
                rustfs_scanner::scanner_pause_backlog_status(store).await,
            ),
            None => (
                ScannerDataMovementPauseStatus::default(),
                unavailable_pause_backlog("storage layer not initialized"),
            ),
        };
        let response =
            ilm_expiry_status_response(enabled, metrics, runtime_config, cycle_schedule, data_movement_pause, pause_backlog);
        let body = serde_json::to_vec(&response).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode ILM expiry status: {err}"))
        })?;

        json_response(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These endpoints authorize through the shared admin gate, which reports
    /// "get cred failed" for a credential-less request. The pre-check keeps the
    /// message they have always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn scanner_status_gate_keeps_its_missing_credentials_message() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: Method::GET,
            uri: http::Uri::from_static("/rustfs/admin/v3/scanner/status"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = validate_scanner_status_request(&req)
            .await
            .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("missing credentials"));
    }

    #[tokio::test]
    async fn scanner_reset_gate_rejects_missing_credentials() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: Method::POST,
            uri: http::Uri::from_static("/rustfs/admin/v3/scanner/cycle-state/reset"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = validate_scanner_reset_request(&req)
            .await
            .expect_err("a reset request without credentials must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("missing credentials"));
    }

    #[test]
    fn admin_reset_requires_full_rescan_or_verified_cursor() {
        let full_rescan: ScannerCycleResetRequest =
            serde_json::from_str(r#"{"mode":"full-rescan"}"#).expect("full rescan must be accepted");
        assert_eq!(full_rescan.mode, "full-rescan");
        let cursor: ScannerCycleResetRequest =
            serde_json::from_str(r#"{"mode":"cursor"}"#).expect("mode validation belongs to the handler");
        assert_ne!(cursor.mode, "full-rescan");
        assert!(serde_json::from_str::<ScannerCycleResetRequest>(r#"{"mode":"full-rescan","cursor":"untrusted"}"#).is_err());
    }

    #[test]
    fn admin_usage_reset_requires_full_rebuild_without_untrusted_fields() {
        let full_rebuild: ScannerUsageStateResetRequest =
            serde_json::from_str(r#"{"mode":"full-rebuild"}"#).expect("full rebuild must be accepted");
        assert_eq!(full_rebuild.mode, "full-rebuild");
        assert!(!full_rebuild.async_intent);
        assert!(full_rebuild.idempotency_key.is_none());
        let cycle_mode: ScannerUsageStateResetRequest =
            serde_json::from_str(r#"{"mode":"full-rescan"}"#).expect("mode validation belongs to the handler");
        assert_ne!(cycle_mode.mode, "full-rebuild");
        assert!(
            serde_json::from_str::<ScannerUsageStateResetRequest>(r#"{"mode":"full-rebuild","delete_files":[".usage.v2.json"]}"#)
                .is_err()
        );
    }

    #[test]
    fn admin_usage_reset_accepts_explicit_async_intent_contract() {
        let request: ScannerUsageStateResetRequest =
            serde_json::from_str(r#"{"mode":"full-rebuild","async":true,"idempotency_key":"reset-key-0001"}"#)
                .expect("async reset intent contract should parse");

        assert_eq!(request.mode, "full-rebuild");
        assert!(request.async_intent);
        assert_eq!(request.idempotency_key.as_deref(), Some("reset-key-0001"));
    }

    #[test]
    fn scanner_recovery_intent_response_uses_accepted_status() {
        let record = rustfs_scanner::ScannerRecoveryIntentRecord {
            schema_version: 1,
            intent_id: "0".repeat(64),
            action: rustfs_scanner::SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD.to_string(),
            mode: "full-rebuild".to_string(),
            state: "accepted".to_string(),
            actor_sha256: "1".repeat(64),
            idempotency_key_sha256: "2".repeat(64),
            request_sha256: "3".repeat(64),
            accepted_at_unix_secs: 7,
        };

        let response =
            scanner_recovery_intent_accept_response(rustfs_scanner::ScannerRecoveryIntentAcceptResult::Accepted { record })
                .expect("accepted intent response");

        assert_eq!(response.output.0, StatusCode::ACCEPTED);
    }

    #[test]
    fn scanner_recovery_intent_executor_only_starts_non_terminal_work() {
        let mut record = rustfs_scanner::ScannerRecoveryIntentRecord {
            schema_version: 1,
            intent_id: "0".repeat(64),
            action: rustfs_scanner::SCANNER_RECOVERY_INTENT_ACTION_USAGE_FULL_REBUILD.to_string(),
            mode: "full-rebuild".to_string(),
            state: "accepted".to_string(),
            actor_sha256: "1".repeat(64),
            idempotency_key_sha256: "2".repeat(64),
            request_sha256: "3".repeat(64),
            accepted_at_unix_secs: 7,
        };

        assert_eq!(
            scanner_recovery_intent_executor_id(&rustfs_scanner::ScannerRecoveryIntentAcceptResult::Accepted {
                record: record.clone(),
            })
            .as_deref(),
            Some(record.intent_id.as_str())
        );
        record.state = "running".to_string();
        assert_eq!(
            scanner_recovery_intent_executor_id(&rustfs_scanner::ScannerRecoveryIntentAcceptResult::Replayed {
                record: record.clone(),
            })
            .as_deref(),
            Some(record.intent_id.as_str())
        );
        record.state = "completed".to_string();
        assert!(
            scanner_recovery_intent_executor_id(&rustfs_scanner::ScannerRecoveryIntentAcceptResult::Replayed {
                record: record.clone(),
            })
            .is_none()
        );
        assert!(
            scanner_recovery_intent_executor_id(&rustfs_scanner::ScannerRecoveryIntentAcceptResult::Conflict {
                existing: rustfs_scanner::ScannerRecoveryIntentConflict {
                    intent_id: record.intent_id,
                    state: "accepted".to_string(),
                },
            })
            .is_none()
        );
    }

    #[test]
    fn scanner_recovery_intent_response_reports_conflict_status() {
        let response = scanner_recovery_intent_accept_response(rustfs_scanner::ScannerRecoveryIntentAcceptResult::Conflict {
            existing: rustfs_scanner::ScannerRecoveryIntentConflict {
                intent_id: "0".repeat(64),
                state: "accepted".to_string(),
            },
        })
        .expect("conflict intent response");

        assert_eq!(response.output.0, StatusCode::CONFLICT);
    }

    #[test]
    fn scanner_recovery_intent_error_keeps_persisted_corruption_server_side() {
        let invalid_actor =
            scanner_recovery_intent_error(rustfs_scanner::ScannerError::Other("actor identity is invalid".to_string()));
        assert_eq!(invalid_actor.code(), &S3ErrorCode::InvalidRequest);

        let corrupt_record = scanner_recovery_intent_error(rustfs_scanner::ScannerError::Other(
            "scanner recovery intent is invalid: expected value".to_string(),
        ));
        assert_eq!(corrupt_record.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn scanner_disabled_reason_reports_startup_env_key() {
        assert_eq!(scanner_disabled_reason(true), None);
        assert_eq!(scanner_disabled_reason(false), Some(format!("disabled by {ENV_SCANNER_ENABLED}")));
    }

    #[test]
    fn scanner_freshness_reports_unknown_without_cycle_end() {
        let metrics = ScannerMetricsReport::default();
        let mut runtime_config = rustfs_scanner::scanner_runtime_config_status();
        runtime_config.cycle_interval_seconds.value = 60;

        let freshness = scanner_freshness_status(&metrics, &runtime_config, 0);

        assert_eq!(freshness.state, "unknown");
        assert_eq!(freshness.last_cycle_end_unix_secs, 0);
        assert_eq!(freshness.max_expected_age_seconds, 120);
        assert_eq!(freshness.reason, Some("no completed cycle recorded"));
    }

    #[test]
    fn scanner_freshness_reports_stale_after_window() {
        let metrics = ScannerMetricsReport {
            last_cycle_end_unix_secs: Utc::now().timestamp().max(0) as u64 - 121,
            ..Default::default()
        };
        let mut runtime_config = rustfs_scanner::scanner_runtime_config_status();
        runtime_config.cycle_interval_seconds.value = 60;

        let freshness = scanner_freshness_status(&metrics, &runtime_config, 0);

        assert_eq!(freshness.state, "stale");
        assert_eq!(freshness.max_expected_age_seconds, 120);
        assert_eq!(freshness.reason, Some("last cycle is older than freshness window"));
    }

    #[test]
    fn scanner_freshness_uses_effective_clean_idle_interval() {
        let metrics = ScannerMetricsReport {
            last_cycle_end_unix_secs: u64::try_from(Utc::now().timestamp().max(0))
                .expect("non-negative timestamp should fit in u64")
                .saturating_sub(300),
            ..Default::default()
        };
        let mut runtime_config = rustfs_scanner::scanner_runtime_config_status();
        runtime_config.cycle_interval_seconds.value = 60;

        let freshness = scanner_freshness_status(&metrics, &runtime_config, 3_600);

        assert_eq!(freshness.state, "fresh");
        assert_eq!(freshness.max_expected_age_seconds, 7_200);
        assert_eq!(freshness.reason, None);
    }

    #[test]
    fn scanner_status_serializes_cycle_schedule_contract() {
        let response = scanner_status_response(
            true,
            ScannerMetricsReport::default(),
            rustfs_scanner::scanner_runtime_config_status(),
            rustfs_scanner::ScannerCycleScheduleStatus::default(),
            ScannerDataMovementPauseStatus::default(),
            rustfs_scanner::ScannerPauseBacklogStatus::default(),
        );

        let encoded = serde_json::to_value(response).expect("scanner status should serialize");
        assert_eq!(encoded["cycle_schedule"]["execution_role"], "unknown");
        assert_eq!(encoded["cycle_schedule"]["effective_interval_available"], false);
        assert_eq!(encoded["cycle_schedule"]["effective_interval_seconds"], 0);
        assert_eq!(encoded["cycle_schedule"]["clean_idle_backoff_enabled"], false);
        assert_eq!(encoded["cycle_schedule"]["clean_idle_backoff_multiplier"], 1);
        assert_eq!(encoded["cycle_recovery"]["state"], "healthy");
        assert_eq!(
            encoded["cycle_recovery"]["quarantine_path"],
            rustfs_scanner::DATA_USAGE_BLOOM_RECOVERY_PATH.as_str()
        );
        assert_eq!(encoded["data_movement_pause"]["policy"], "global_pause");
        assert_eq!(encoded["data_movement_pause"]["paused"], false);
        assert_eq!(encoded["catch_up_estimate"]["estimated"], false);
        assert_eq!(encoded["catch_up_estimate"]["undiscovered_ilm_items_known"], true);
    }

    #[test]
    fn scanner_status_keeps_an_unavailable_storage_layer_observable() {
        let backlog = unavailable_pause_backlog("storage layer not initialized");

        assert_eq!(backlog.persistence_state, "unavailable");
        assert!(backlog.alerting);
        assert_eq!(
            backlog.alert_reasons,
            vec![rustfs_scanner::ScannerPauseBacklogAlertReason::PersistenceUnavailable]
        );
        assert_eq!(backlog.error.as_deref(), Some("storage layer not initialized"));
    }

    #[test]
    fn ilm_expiry_status_serializes_lifecycle_expiry_contract() {
        let metrics = ScannerMetricsReport {
            lifecycle_expiry: ScannerLifecycleExpirySnapshot {
                current_queue_capacity: 32,
                current_queued: 7,
                current_active: 2,
                current_workers: 4,
                queue_missed: 3,
                scanner_queued: 17,
                scanner_missed: 5,
                scanner_blocked: 11,
                scanner_not_enqueued: 13,
                delete_failed: 19,
            },
            lifecycle_transition: ScannerLifecycleTransitionSnapshot {
                current_queued: 2,
                current_active: 3,
                compensation_pending: 5,
                compensation_running: 7,
                ..Default::default()
            },
            maintenance_control: ScannerMaintenanceControlSnapshot {
                primary_control: "expiry_backlog".to_string(),
                ..Default::default()
            },
            current_cycle_lifecycle_expiry_actions: 23,
            last_cycle_lifecycle_expiry_actions: 29,
            ..Default::default()
        };
        let response = ilm_expiry_status_response(
            true,
            metrics,
            rustfs_scanner::scanner_runtime_config_status(),
            rustfs_scanner::ScannerCycleScheduleStatus::default(),
            ScannerDataMovementPauseStatus {
                paused: true,
                movement_backlog_work_items: 31,
                movement_backlog_estimated: true,
                ..Default::default()
            },
            rustfs_scanner::ScannerPauseBacklogStatus {
                phase: rustfs_scanner::ScannerPauseBacklogPhase::Paused,
                movement_work_items: 31,
                pending_full_scan: true,
                ..Default::default()
            },
        );

        let encoded = serde_json::to_value(response).expect("ILM expiry status should serialize");
        assert_eq!(encoded["lifecycle_expiry"]["current_queue_capacity"].as_u64(), Some(32));
        assert_eq!(encoded["lifecycle_expiry"]["scanner_blocked"].as_u64(), Some(11));
        assert_eq!(encoded["lifecycle_expiry"]["scanner_not_enqueued"].as_u64(), Some(13));
        assert_eq!(encoded["lifecycle_expiry"]["delete_failed"].as_u64(), Some(19));
        assert_eq!(encoded["maintenance_control"]["primary_control"].as_str(), Some("expiry_backlog"));
        assert_eq!(encoded["current_cycle_lifecycle_expiry_actions"].as_u64(), Some(23));
        assert_eq!(encoded["last_cycle_lifecycle_expiry_actions"].as_u64(), Some(29));
        assert_eq!(encoded["data_movement_pause"]["paused"], true);
        assert_eq!(encoded["pause_backlog"]["phase"], "paused");
        assert_eq!(encoded["catch_up_estimate"]["movement_work_items"].as_u64(), Some(31));
        assert_eq!(encoded["catch_up_estimate"]["discovered_expiry_items"].as_u64(), Some(9));
        assert_eq!(encoded["catch_up_estimate"]["discovered_transition_items"].as_u64(), Some(17));
        assert_eq!(encoded["catch_up_estimate"]["undiscovered_ilm_items_known"], false);
    }
}
