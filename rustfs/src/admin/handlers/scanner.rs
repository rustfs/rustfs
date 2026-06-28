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
use crate::admin::runtime_sources::current_scanner_metrics_report;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::startup_background::{ENV_SCANNER_ENABLED, scanner_enabled_from_env};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::metrics::ScannerMetricsReport;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;

const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, Serialize)]
struct ScannerStatusResponse {
    enabled: bool,
    disabled_reason: Option<String>,
    metrics: ScannerMetricsReport,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
}

fn scanner_disabled_reason(enabled: bool) -> Option<String> {
    (!enabled).then(|| format!("disabled by {ENV_SCANNER_ENABLED}"))
}

pub fn register_scanner_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/scanner/status").as_str(),
        AdminOperation(&ScannerStatusHandler {}),
    )?;

    Ok(())
}

async fn validate_scanner_status_request(req: &S3Request<Body>) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
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
        vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
        remote_addr,
    )
    .await?;

    Ok(cred)
}

fn json_response(body: Vec<u8>) -> S3Result<S3Response<(StatusCode, Body)>> {
    let mut headers = HeaderMap::new();
    let content_type = HeaderValue::from_str(JSON_CONTENT_TYPE)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid content type: {err}")))?;
    headers.insert(CONTENT_TYPE, content_type);
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), headers))
}

pub struct ScannerStatusHandler {}

#[async_trait::async_trait]
impl Operation for ScannerStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_status_request(&req).await?;
        let enabled = scanner_enabled_from_env();
        let response = ScannerStatusResponse {
            enabled,
            disabled_reason: scanner_disabled_reason(enabled),
            metrics: current_scanner_metrics_report().await,
            runtime_config: rustfs_scanner::scanner_runtime_config_status(),
        };
        let body = serde_json::to_vec(&response).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode scanner status: {err}"))
        })?;

        json_response(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_disabled_reason_reports_startup_env_key() {
        assert_eq!(scanner_disabled_reason(true), None);
        assert_eq!(scanner_disabled_reason(false), Some(format!("disabled by {ENV_SCANNER_ENABLED}")));
    }
}
