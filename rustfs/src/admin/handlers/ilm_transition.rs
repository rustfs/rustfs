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
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};

const JSON_CONTENT_TYPE: &str = "application/json";
const DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS: u64 = 10_000;
const MAX_MANUAL_TRANSITION_OBJECTS: u64 = 100_000;

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ManualTransitionRunQuery {
    bucket: Option<String>,
    prefix: Option<String>,
    marker: Option<String>,
    #[serde(rename = "versionMarker")]
    version_marker: Option<String>,
    tier: Option<String>,
    #[serde(rename = "dryRun")]
    dry_run: Option<bool>,
    #[serde(rename = "maxObjects")]
    max_objects: Option<u64>,
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

    let max_objects = query.max_objects.unwrap_or(DEFAULT_MANUAL_TRANSITION_MAX_OBJECTS);
    if max_objects == 0 || max_objects > MAX_MANUAL_TRANSITION_OBJECTS {
        return Err(s3_error!(InvalidArgument, "maxObjects is outside the allowed range"));
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
        },
    ))
}

async fn authorize_manual_transition_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

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
    .await
}

fn response_state(report: &ManualTransitionRunReport) -> &'static str {
    if report.truncated_by_limit || report.has_partial_enqueue() {
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
        authorize_manual_transition_request(&req).await?;
        let (bucket, options) = parse_manual_transition_query(req.uri.query())?;
        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let report = enqueue_transition_for_existing_objects_scoped(store, &bucket, options)
            .await
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("manual transition run failed: {err}")))?;
        let response = ManualTransitionRunResponse {
            state: response_state(&report),
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
    fn manual_transition_response_reports_partial_for_queue_pressure() {
        let report = ManualTransitionRunReport {
            skipped_queue_full: 1,
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
