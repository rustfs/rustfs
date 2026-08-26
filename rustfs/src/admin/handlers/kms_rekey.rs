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

//! Admin API for the bulk DEK rekey sweep: start, status and cancel.
//!
//! All three endpoints require `kms:Rekey`, a cluster-scoped action: the sweep
//! walks and rewrites object metadata across buckets, so no per-key role
//! template confers it.

use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_kms_runtime_service_manager, current_object_store_handle};
use crate::admin::storage_api::s3::{self, Body, S3ErrorCode, S3Request, S3Response, S3Result};
use crate::kms_rekey::{self, RekeyStartError};
use crate::server::ADMIN_PREFIX;
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_policy::policy::action::{Action, KmsAction};
use serde::Deserialize;

fn kms_rekey_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::RekeyAction)]
}

async fn authorize_kms_rekey_request(req: &S3Request<Body>) -> S3Result<()> {
    if req.credentials.is_none() {
        return Err(s3::error(S3ErrorCode::InvalidRequest, "authentication required"));
    }
    authorize_admin_request(req, kms_rekey_actions()).await?;
    Ok(())
}

fn json_response(status: StatusCode, body: Vec<u8>) -> S3Response<(StatusCode, Body)> {
    let mut headers = HeaderMap::new();
    headers.insert(s3::header::CONTENT_TYPE, "application/json".parse().expect("static content type"));
    S3Response::with_headers((status, Body::from(body)), headers)
}

fn snapshot_response(status: StatusCode, snapshot: &kms_rekey::RekeyJobSnapshot) -> S3Result<S3Response<(StatusCode, Body)>> {
    let body = serde_json::to_vec(snapshot)
        .map_err(|e| s3::error(S3ErrorCode::InternalError, format!("failed to serialize rekey status: {e}")))?;
    Ok(json_response(status, body))
}

/// Body of `POST /v3/kms/keys/rekey`. An empty body sweeps every bucket.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartKmsRekeyRequest {
    /// Buckets to sweep; every bucket when absent or empty.
    #[serde(default)]
    buckets: Option<Vec<String>>,
    /// Object key prefix to restrict the sweep to.
    #[serde(default)]
    prefix: Option<String>,
}

/// `POST /v3/kms/keys/rekey` — start a sweep.
pub struct StartKmsRekeyHandler;

#[async_trait::async_trait]
impl Operation for StartKmsRekeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_rekey_request(&req).await?;

        let mut req = req;
        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("failed to read request body: {e}")))?;
        let request: StartKmsRekeyRequest = if body.is_empty() {
            StartKmsRekeyRequest::default()
        } else {
            serde_json::from_slice(&body)
                .map_err(|e| s3::error(S3ErrorCode::InvalidRequest, format!("invalid rekey request body: {e}")))?
        };

        // Refuse up front when the backend cannot rewrap at all: a sweep would
        // count every encrypted object as failed while changing nothing.
        // `UnsupportedCapability` is a permanent gap of the configured backend,
        // so it surfaces as 501, mirroring the key lifecycle handlers.
        let Some(manager) = current_kms_runtime_service_manager() else {
            return Err(s3::error(S3ErrorCode::InternalError, "KMS service not initialized"));
        };
        let Some(service) = manager.get_encryption_service().await else {
            return Err(s3::error(S3ErrorCode::InternalError, "KMS service not running"));
        };
        if !service.backend_capabilities().rewrap {
            return Ok(json_response(
                StatusCode::NOT_IMPLEMENTED,
                serde_json::json!({
                    "error": "the configured KMS backend does not support rewrapping data-key envelopes"
                })
                .to_string()
                .into_bytes(),
            ));
        }

        let Some(store) = current_object_store_handle() else {
            return Err(s3::error(S3ErrorCode::InternalError, "object store is not ready"));
        };

        match kms_rekey::start(store, request.buckets, request.prefix.unwrap_or_default()).await {
            Ok(snapshot) => snapshot_response(StatusCode::OK, &snapshot),
            Err(RekeyStartError::AlreadyRunning(job_id)) => Ok(json_response(
                StatusCode::CONFLICT,
                serde_json::json!({ "error": "a rekey sweep is already running", "job_id": job_id })
                    .to_string()
                    .into_bytes(),
            )),
            Err(RekeyStartError::Storage(message)) => Err(s3::error(S3ErrorCode::InternalError, message)),
        }
    }
}

/// `GET /v3/kms/keys/rekey/status` — progress of the current or last sweep.
pub struct KmsRekeyStatusHandler;

#[async_trait::async_trait]
impl Operation for KmsRekeyStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_rekey_request(&req).await?;

        match kms_rekey::status() {
            Some(snapshot) => snapshot_response(StatusCode::OK, &snapshot),
            None => Ok(json_response(
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "no rekey sweep has run" })
                    .to_string()
                    .into_bytes(),
            )),
        }
    }
}

/// `POST /v3/kms/keys/rekey/cancel` — request cancellation of a running sweep.
pub struct CancelKmsRekeyHandler;

#[async_trait::async_trait]
impl Operation for CancelKmsRekeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_kms_rekey_request(&req).await?;

        match kms_rekey::cancel() {
            Some(snapshot) => snapshot_response(StatusCode::OK, &snapshot),
            None => Ok(json_response(
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "no rekey sweep has run" })
                    .to_string()
                    .into_bytes(),
            )),
        }
    }
}

pub fn register_kms_rekey_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/rekey").as_str(),
        AdminOperation(&StartKmsRekeyHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/rekey/status").as_str(),
        AdminOperation(&KmsRekeyStatusHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/rekey/cancel").as_str(),
        AdminOperation(&CancelKmsRekeyHandler {}),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rekey_endpoints_require_the_cluster_scoped_action() {
        assert_eq!(kms_rekey_actions(), vec![Action::KmsAction(KmsAction::RekeyAction)]);
    }

    #[test]
    fn start_request_parses_and_rejects_unknown_fields() {
        let parsed: StartKmsRekeyRequest =
            serde_json::from_slice(br#"{"buckets": ["a", "b"], "prefix": "photos/"}"#).expect("valid body must parse");
        assert_eq!(parsed.buckets.as_deref(), Some(["a".to_string(), "b".to_string()].as_slice()));
        assert_eq!(parsed.prefix.as_deref(), Some("photos/"));

        serde_json::from_slice::<StartKmsRekeyRequest>(br#"{"bucket": "typo"}"#)
            .expect_err("unknown fields must be rejected, not silently ignored");
    }
}
