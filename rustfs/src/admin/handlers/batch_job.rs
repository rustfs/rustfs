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

//! MinIO-compatible batch-job admin endpoints.
//!
//! RustFS does not (yet) ship a batch-job execution engine: there is no job
//! scheduler, queue, or background worker that can run `replicate`, `keyrotate`,
//! or `expire` batch jobs. These handlers therefore implement the MinIO request,
//! response, and error *semantics* only, so that MinIO-compatible admin clients
//! (mc admin batch, madmin) receive deliberate, stable answers instead of a
//! routing 404 or a silently faked success:
//!
//! * `POST /v3/start-job` parses the job definition, validates the declared job
//!   type, and returns a deliberate `NotImplemented` compatibility error because
//!   RustFS has no backend that can execute the job. Unknown job types are
//!   rejected with `InvalidRequest`. No job is ever accepted or persisted, so no
//!   execution or success is ever faked.
//! * `GET /v3/list-jobs` returns an empty job list — no batch job can be running
//!   because none can be started.
//! * `GET /v3/status-job`, `GET /v3/describe-job`, and `DELETE /v3/cancel-job`
//!   validate their `jobId` query parameter and return a `NoSuchJob`-style error,
//!   since no job with that id can exist.
//!
//! When RustFS grows a real batch-job engine, these handlers should be rewired to
//! it; the request parsing and response shapes here are intended to stay stable.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::utils::read_compatible_admin_body;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use tracing::warn;

/// Job types recognised by the MinIO batch-job admin API.
///
/// RustFS cannot execute any of these today; the enum exists so that
/// `start-job` can distinguish "known job type, unsupported backend"
/// (`NotImplemented`) from "unknown job type" (`InvalidRequest`).
const KNOWN_JOB_TYPES: &[&str] = &["replicate", "keyrotate", "expire"];

fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }
    params
}

async fn validate_batch_job_admin_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(cred)
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

/// Best-effort detection of the declared job type from a MinIO batch-job
/// definition body.
///
/// MinIO batch jobs are YAML documents whose top-level key names the job type,
/// e.g. `replicate:`, `keyrotate:`, or `expire:`. RustFS has no YAML dependency
/// and rejects every job anyway, so instead of pulling in a full parser we look
/// for the first recognised top-level job-type key. Returns `Some(job_type)`
/// when a known key is found, `None` otherwise.
fn detect_job_type(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A top-level mapping key looks like `replicate:` with no leading indentation.
        if raw_line.starts_with(char::is_whitespace) {
            continue;
        }
        let Some((key, _)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().trim_matches('"').to_ascii_lowercase();
        if KNOWN_JOB_TYPES.contains(&key.as_str()) {
            return Some(key);
        }
    }
    None
}

#[derive(Debug, Serialize)]
struct ListBatchJobsResult {
    jobs: Vec<serde_json::Value>,
}

pub fn register_batch_job_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/start-job").as_str(),
        AdminOperation(&StartBatchJobHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/list-jobs").as_str(),
        AdminOperation(&ListBatchJobsHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/status-job").as_str(),
        AdminOperation(&StatusBatchJobHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/describe-job").as_str(),
        AdminOperation(&DescribeBatchJobHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/cancel-job").as_str(),
        AdminOperation(&CancelBatchJobHandler {}),
    )?;

    Ok(())
}

/// `POST /v3/start-job`
///
/// Parses and validates the submitted job definition, then returns a deliberate
/// `NotImplemented` compatibility error: RustFS has no batch-job engine to run
/// the job. Unknown job types are rejected with `InvalidRequest`. No job is
/// accepted, persisted, or reported as started.
pub struct StartBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for StartBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_batch_job_admin_request(&req, AdminAction::StartBatchJobAction).await?;

        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), &cred.secret_key).await?;

        if body.trim_ascii().is_empty() {
            return Err(s3_error!(InvalidRequest, "batch job definition is required"));
        }

        match detect_job_type(&body) {
            Some(job_type) => {
                warn!(
                    job_type = %job_type,
                    "batch job start requested but RustFS has no batch-job execution backend"
                );
                // Known job type, but no backend can run it: honest compatibility error.
                Err(S3Error::with_message(
                    S3ErrorCode::NotImplemented,
                    format!("batch job type '{job_type}' is not supported: RustFS has no batch-job execution backend"),
                ))
            }
            None => Err(s3_error!(
                InvalidRequest,
                "unable to determine batch job type from definition; expected one of: replicate, keyrotate, expire"
            )),
        }
    }
}

/// `GET /v3/list-jobs`
///
/// No batch job can be running (none can be started), so this always returns an
/// empty job list. The optional `jobType` filter is validated for compatibility.
pub struct ListBatchJobsHandler {}

#[async_trait::async_trait]
impl Operation for ListBatchJobsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_job_admin_request(&req, AdminAction::ListBatchJobsAction).await?;

        let queries = extract_query_params(&req.uri);
        if let Some(job_type) = queries.get("jobType").filter(|v| !v.is_empty()) {
            let job_type = job_type.to_ascii_lowercase();
            if !KNOWN_JOB_TYPES.contains(&job_type.as_str()) {
                return Err(s3_error!(InvalidRequest, "invalid jobType filter: {job_type}"));
            }
        }

        json_response(StatusCode::OK, &ListBatchJobsResult { jobs: Vec::new() })
    }
}

/// `GET /v3/status-job`
///
/// No job can exist, so a well-formed `jobId` always yields a `NoSuchJob`-style
/// error rather than a fabricated status.
pub struct StatusBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for StatusBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_job_admin_request(&req, AdminAction::DescribeBatchJobAction).await?;
        let job_id = require_job_id(&req.uri)?;
        Err(no_such_job(&job_id))
    }
}

/// `GET /v3/describe-job`
///
/// No job can exist, so a well-formed `jobId` always yields a `NoSuchJob`-style
/// error rather than a fabricated definition.
pub struct DescribeBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for DescribeBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_job_admin_request(&req, AdminAction::DescribeBatchJobAction).await?;
        let job_id = require_job_id(&req.uri)?;
        Err(no_such_job(&job_id))
    }
}

/// `DELETE /v3/cancel-job`
///
/// No job can exist, so a well-formed `jobId` always yields a `NoSuchJob`-style
/// error rather than a fabricated cancellation.
pub struct CancelBatchJobHandler {}

#[async_trait::async_trait]
impl Operation for CancelBatchJobHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_batch_job_admin_request(&req, AdminAction::CancelBatchJobAction).await?;
        let job_id = require_job_id(&req.uri)?;
        Err(no_such_job(&job_id))
    }
}

fn require_job_id(uri: &Uri) -> S3Result<String> {
    let queries = extract_query_params(uri);
    match queries.get("jobId").filter(|v| !v.is_empty()) {
        Some(job_id) => Ok(job_id.clone()),
        None => Err(s3_error!(InvalidRequest, "jobId is required")),
    }
}

fn no_such_job(job_id: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::NoSuchKey, format!("no such batch job: {job_id}"))
}

#[cfg(test)]
mod tests {
    use super::{detect_job_type, extract_query_params, require_job_id};
    use http::Uri;

    #[test]
    fn detect_job_type_recognises_top_level_replicate_key() {
        let body = b"replicate:\n  apiVersion: v1\n  source:\n    bucket: src\n";
        assert_eq!(detect_job_type(body), Some("replicate".to_string()));
    }

    #[test]
    fn detect_job_type_recognises_keyrotate_and_expire() {
        assert_eq!(detect_job_type(b"keyrotate:\n  foo: bar\n"), Some("keyrotate".to_string()));
        assert_eq!(detect_job_type(b"expire:\n  foo: bar\n"), Some("expire".to_string()));
    }

    #[test]
    fn detect_job_type_skips_comments_and_blank_lines() {
        let body = b"# a comment\n\nreplicate:\n  foo: bar\n";
        assert_eq!(detect_job_type(body), Some("replicate".to_string()));
    }

    #[test]
    fn detect_job_type_ignores_indented_keys() {
        // An indented `replicate:` is a nested field, not the top-level job type.
        let body = b"something:\n  replicate: true\n";
        assert_eq!(detect_job_type(body), None);
    }

    #[test]
    fn detect_job_type_returns_none_for_unknown_type() {
        assert_eq!(detect_job_type(b"transmogrify:\n  foo: bar\n"), None);
    }

    #[test]
    fn extract_query_params_decodes_job_id() {
        let uri: Uri = "/rustfs/admin/v3/status-job?jobId=abc%2F123"
            .parse()
            .expect("uri should parse");
        let params = extract_query_params(&uri);
        assert_eq!(params.get("jobId"), Some(&"abc/123".to_string()));
    }

    #[test]
    fn require_job_id_rejects_missing_and_empty() {
        let missing: Uri = "/rustfs/admin/v3/status-job".parse().expect("uri should parse");
        assert!(require_job_id(&missing).is_err());

        let empty: Uri = "/rustfs/admin/v3/status-job?jobId=".parse().expect("uri should parse");
        assert!(require_job_id(&empty).is_err());

        let present: Uri = "/rustfs/admin/v3/status-job?jobId=job-1".parse().expect("uri should parse");
        assert_eq!(require_job_id(&present).expect("job id"), "job-1");
    }
}
