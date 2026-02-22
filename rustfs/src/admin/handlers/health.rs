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

use super::profile::{TriggerProfileCPU, TriggerProfileMemory};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::{HEALTH_PREFIX, HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};
use serde_json::{Value, json};

pub fn register_health_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Health check endpoint for monitoring and orchestration
    r.insert(Method::GET, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::HEAD, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::GET, HEALTH_READY_PATH, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::HEAD, HEALTH_READY_PATH, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::GET, PROFILE_CPU_PATH, AdminOperation(&TriggerProfileCPU {}))?;
    r.insert(Method::GET, PROFILE_MEMORY_PATH, AdminOperation(&TriggerProfileMemory {}))?;

    Ok(())
}

/// Health check handler for endpoint monitoring
pub struct HealthCheckHandler {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HealthCheckState {
    pub(crate) status_code: StatusCode,
    pub(crate) status: &'static str,
    pub(crate) ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthProbe {
    Liveness,
    Readiness,
}

pub(crate) fn collect_dependency_readiness() -> (bool, bool) {
    let storage_ready = rustfs_ecstore::new_object_layer_fn().is_some();
    let iam_ready = rustfs_iam::get().is_ok();
    (storage_ready, iam_ready)
}

pub(crate) fn health_check_state(storage_ready: bool, iam_ready: bool, probe: HealthProbe) -> HealthCheckState {
    let ready = storage_ready && iam_ready;
    let status = if ready { "ok" } else { "degraded" };

    let status_code = match probe {
        HealthProbe::Liveness => StatusCode::OK,
        HealthProbe::Readiness if ready => StatusCode::OK,
        HealthProbe::Readiness => StatusCode::SERVICE_UNAVAILABLE,
    };

    HealthCheckState {
        status_code,
        status,
        ready,
    }
}

pub(crate) fn build_component_details(storage_ready: bool, iam_ready: bool) -> Value {
    json!({
        "storage": {
            "status": if storage_ready { "connected" } else { "disconnected" },
            "ready": storage_ready,
        },
        "iam": {
            "status": if iam_ready { "connected" } else { "disconnected" },
            "ready": iam_ready,
        }
    })
}

pub(crate) fn probe_from_path(path: &str) -> HealthProbe {
    if path == HEALTH_READY_PATH {
        HealthProbe::Readiness
    } else {
        HealthProbe::Liveness
    }
}

pub(crate) fn build_health_response(
    method: Method,
    probe: HealthProbe,
    storage_ready: bool,
    iam_ready: bool,
) -> S3Response<(StatusCode, Body)> {
    let health = health_check_state(storage_ready, iam_ready, probe);
    let health_info = json!({
        "status": health.status,
        "ready": health.ready,
        "service": "rustfs-endpoint",
        "timestamp": jiff::Zoned::now().to_string(),
        "version": env!("CARGO_PKG_VERSION"),
        "details": build_component_details(storage_ready, iam_ready)
    });

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    if method == Method::HEAD {
        return S3Response::with_headers((health.status_code, Body::empty()), headers);
    }

    let body_str = serde_json::to_string(&health_info).unwrap_or_else(|_| "{}".to_string());
    let body = Body::from(body_str);

    S3Response::with_headers((health.status_code, body), headers)
}

#[async_trait::async_trait]
impl Operation for HealthCheckHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Extract the original HTTP Method (encapsulated by s3s into S3Request)
        let method = req.method;

        // Only GET and HEAD are allowed
        if method != http::Method::GET && method != http::Method::HEAD {
            // 405 Method Not Allowed
            let mut headers = HeaderMap::new();
            headers.insert(http::header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(S3Response::with_headers(
                (StatusCode::METHOD_NOT_ALLOWED, Body::from("Method Not Allowed".to_string())),
                headers,
            ));
        }

        let probe = probe_from_path(req.uri.path());
        let (storage_ready, iam_ready) = collect_dependency_readiness();

        Ok(build_health_response(method, probe, storage_ready, iam_ready))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readiness_state_ready() {
        let state = health_check_state(true, true, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::OK);
        assert_eq!(state.status, "ok");
        assert!(state.ready);
    }

    #[test]
    fn test_readiness_state_storage_not_ready() {
        let state = health_check_state(false, true, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_liveness_state_iam_not_ready() {
        let state = health_check_state(true, false, HealthProbe::Liveness);
        assert_eq!(state.status_code, StatusCode::OK);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_readiness_state_iam_not_ready() {
        let state = health_check_state(true, false, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_health_check_component_details() {
        let details = build_component_details(true, false);

        assert_eq!(details["storage"]["status"], "connected");
        assert_eq!(details["storage"]["ready"], true);
        assert_eq!(details["iam"]["status"], "disconnected");
        assert_eq!(details["iam"]["ready"], false);
    }

    #[test]
    fn test_probe_from_path_readiness() {
        assert_eq!(probe_from_path(HEALTH_READY_PATH), HealthProbe::Readiness);
    }

    #[test]
    fn test_probe_from_path_liveness() {
        assert_eq!(probe_from_path(HEALTH_PREFIX), HealthProbe::Liveness);
        assert_eq!(probe_from_path("/random"), HealthProbe::Liveness);
    }

    #[test]
    fn test_build_health_response_readiness_returns_503_when_deps_not_ready() {
        let resp = build_health_response(Method::GET, HealthProbe::Readiness, false, true);
        assert_eq!(resp.output.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_build_health_response_readiness_returns_200_when_deps_ready() {
        let resp = build_health_response(Method::GET, HealthProbe::Readiness, true, true);
        assert_eq!(resp.output.0, StatusCode::OK);
    }

    #[test]
    fn test_build_health_response_liveness_returns_200_when_deps_not_ready() {
        let resp = build_health_response(Method::GET, HealthProbe::Liveness, false, false);
        assert_eq!(resp.output.0, StatusCode::OK);
    }

    #[test]
    fn test_build_health_response_head_returns_empty_body() {
        let resp = build_health_response(Method::HEAD, HealthProbe::Readiness, false, false);
        assert_eq!(resp.output.0, StatusCode::SERVICE_UNAVAILABLE);
    }
}
