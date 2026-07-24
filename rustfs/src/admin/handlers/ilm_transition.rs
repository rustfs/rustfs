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
use crate::admin::storage_api::lifecycle::{
    ManualTransitionRunOptions, ManualTransitionRunReport, enqueue_transition_for_existing_objects_scoped,
};
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
use tracing::{error, info, warn};

const JSON_CONTENT_TYPE: &str = "application/json";
const DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS: u64 = 10_000;
const MAX_MANUAL_TRANSITION_OBJECTS: u64 = 100_000;
const MAX_MANUAL_TRANSITION_DURATION_SECONDS: u64 = 3600;
const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ILM_TRANSITION: &str = "ilm_transition";
const EVENT_ADMIN_ILM_TRANSITION_STATE: &str = "admin_ilm_transition_state";

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

#[derive(Debug, Serialize)]
struct ManualTransitionRunResponse {
    state: &'static str,
    mode: &'static str,
    job_id: Option<String>,
    status_endpoint: Option<String>,
    report: ManualTransitionRunReport,
}

pub fn register_ilm_transition_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/ilm/transition/run").as_str(),
        AdminOperation(&ManualTransitionRunHandler {}),
    )?;
    Ok(())
}

fn parse_manual_transition_query(query: Option<&str>) -> S3Result<(String, ManualTransitionRunOptions)> {
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
    if query.async_mode == Some(true) || mode == Some("async") {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "durable manual transition jobs are not implemented; omit async/mode for enqueue_only",
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
            tier: query.tier.map(|tier| tier.trim().to_string()).filter(|tier| !tier.is_empty()),
            dry_run: query.dry_run.unwrap_or(false),
            max_objects: Some(max_objects),
            max_duration: query.max_duration_seconds.map(std::time::Duration::from_secs),
        },
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

fn json_response(response: &ManualTransitionRunResponse) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(response).map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode manual transition response: {err}"))
    })?;
    let content_type = HeaderValue::from_str(JSON_CONTENT_TYPE)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, content_type);
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), headers))
}

pub struct ManualTransitionRunHandler {}

#[async_trait::async_trait]
impl Operation for ManualTransitionRunHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let request_id = admin_request_id(&req.headers).unwrap_or_default().to_string();
        let remote_addr = admin_remote_addr(&req).unwrap_or_default();
        let actor = authorize_manual_transition_request(&req).await?;
        let (bucket, options) = match parse_manual_transition_query(req.uri.query()) {
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

        json_response(&response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_transition_query_defaults_to_bounded_run() {
        let (bucket, options) =
            parse_manual_transition_query(Some("bucket=data&prefix=logs/&marker=logs/a&versionMarker=v1&tier=warm"))
                .expect("valid query should parse");

        assert_eq!(bucket, "data");
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
        let (_bucket, options) =
            parse_manual_transition_query(Some("bucket=data&maxDurationSeconds=30")).expect("valid query should parse");

        assert_eq!(options.max_duration, Some(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn manual_transition_query_accepts_explicit_enqueue_only_mode() {
        let (_bucket, options) = parse_manual_transition_query(Some("bucket=data&mode=enqueue_only&async=false"))
            .expect("explicit enqueue_only mode should remain compatible");

        assert!(!options.dry_run);
        assert_eq!(options.max_objects, Some(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS));
    }

    #[test]
    fn manual_transition_query_rejects_unimplemented_durable_mode() {
        let err = parse_manual_transition_query(Some("bucket=data&async=true"))
            .expect_err("async durable jobs must not be silently accepted");

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);

        let err = parse_manual_transition_query(Some("bucket=data&mode=async"))
            .expect_err("mode=async must not fall back to enqueue_only");

        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
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
    fn manual_transition_handler_requires_set_tier_action() {
        let src = include_str!("ilm_transition.rs");
        let auth_block = extract_block_between_markers(src, "async fn authorize_manual_transition_request", "fn response_state");

        assert!(auth_block.contains("AdminAction::SetTierAction"));
        assert!(!auth_block.contains("AdminAction::ServerInfoAdminAction"));
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
