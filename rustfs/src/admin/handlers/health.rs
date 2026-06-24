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
use crate::server::{
    HEALTH_PREFIX, HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH,
    collect_dependency_readiness_report as collect_runtime_dependency_readiness_report,
};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};
use serde_json::{Value, json};

pub fn register_health_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    if rustfs_utils::get_env_bool(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, rustfs_config::DEFAULT_HEALTH_ENDPOINT_ENABLE) {
        // Health check endpoint for monitoring and orchestration
        r.insert(Method::GET, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
        r.insert(Method::HEAD, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
        r.insert(Method::GET, HEALTH_READY_PATH, AdminOperation(&HealthCheckHandler {}))?;
        r.insert(Method::HEAD, HEALTH_READY_PATH, AdminOperation(&HealthCheckHandler {}))?;
    }

    // Profiling routes are controlled separately and must not be affected by health endpoint toggles.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HealthResponseParts {
    pub(crate) status_code: StatusCode,
    pub(crate) payload: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HealthPayloadContext<'a> {
    pub(crate) health: HealthCheckState,
    pub(crate) storage_ready: bool,
    pub(crate) iam_ready: bool,
    pub(crate) lock_quorum_ready: bool,
    pub(crate) degraded_reasons: &'a [crate::server::ReadinessDegradedReason],
    pub(crate) service: &'a str,
    pub(crate) uptime: Option<u64>,
    pub(crate) kms_ready: Option<bool>,
    pub(crate) include_dependency_details: bool,
}

pub(crate) async fn collect_dependency_readiness() -> crate::server::DependencyReadinessReport {
    collect_runtime_dependency_readiness_report().await
}

pub(crate) fn health_check_state(
    storage_ready: bool,
    iam_ready: bool,
    lock_quorum_ready: bool,
    probe: HealthProbe,
) -> HealthCheckState {
    if probe == HealthProbe::Liveness {
        return HealthCheckState {
            status_code: StatusCode::OK,
            status: "ok",
            ready: true,
        };
    }

    let ready = storage_ready && iam_ready && lock_quorum_ready;
    let status = if ready { "ok" } else { "degraded" };

    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    HealthCheckState {
        status_code,
        status,
        ready,
    }
}

pub(crate) fn health_minimal_response_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE,
        rustfs_config::DEFAULT_HEALTH_MINIMAL_RESPONSE_ENABLE,
    )
}

pub(crate) fn build_component_details(
    storage_ready: bool,
    iam_ready: bool,
    lock_quorum_ready: bool,
    kms_ready: Option<bool>,
) -> Value {
    let mut details = json!({
        "storage": {
            "status": if storage_ready { "connected" } else { "disconnected" },
            "ready": storage_ready,
        },
        "iam": {
            "status": if iam_ready { "connected" } else { "disconnected" },
            "ready": iam_ready,
        },
        "lock": {
            "status": if lock_quorum_ready { "connected" } else { "disconnected" },
            "ready": lock_quorum_ready,
        }
    });

    if let Some(kms_ready) = kms_ready {
        details["kms"] = json!({
            "status": if kms_ready { "connected" } else { "disconnected" },
            "ready": kms_ready,
        });
    }

    details
}

pub(crate) fn build_degraded_reasons(reasons: &[crate::server::ReadinessDegradedReason]) -> Value {
    Value::Array(
        reasons
            .iter()
            .map(|reason| Value::String(reason.as_str().to_string()))
            .collect(),
    )
}

pub(crate) fn probe_from_path(path: &str) -> HealthProbe {
    if path == HEALTH_READY_PATH {
        HealthProbe::Readiness
    } else {
        HealthProbe::Liveness
    }
}

pub(crate) fn build_health_response_parts(
    method: Method,
    probe: HealthProbe,
    readiness_report: Option<&crate::server::DependencyReadinessReport>,
    service: &str,
    uptime: Option<u64>,
    kms_ready: Option<bool>,
) -> HealthResponseParts {
    let (storage_ready, iam_ready, lock_quorum_ready, mut health, mut degraded_reasons, include_dependency_details) =
        match (probe, readiness_report) {
            (HealthProbe::Readiness, Some(readiness_report)) => {
                let storage_ready = readiness_report.readiness.storage_ready;
                let iam_ready = readiness_report.readiness.iam_ready;
                let lock_quorum_ready = readiness_report.readiness.lock_quorum_ready;
                (
                    storage_ready,
                    iam_ready,
                    lock_quorum_ready,
                    health_check_state(storage_ready, iam_ready, lock_quorum_ready, probe),
                    readiness_report.degraded_reasons.clone(),
                    true,
                )
            }
            (HealthProbe::Readiness, None) => (
                false,
                false,
                false,
                HealthCheckState {
                    status_code: StatusCode::SERVICE_UNAVAILABLE,
                    status: "degraded",
                    ready: false,
                },
                vec![crate::server::ReadinessDegradedReason::StorageIamAndLockUnavailable],
                true,
            ),
            (HealthProbe::Liveness, _) => {
                (false, false, false, health_check_state(false, false, false, probe), Vec::new(), false)
            }
        };

    if probe == HealthProbe::Readiness && matches!(kms_ready, Some(false)) {
        health = HealthCheckState {
            status_code: StatusCode::SERVICE_UNAVAILABLE,
            status: "degraded",
            ready: false,
        };
        if !degraded_reasons.contains(&crate::server::ReadinessDegradedReason::KmsNotReady) {
            degraded_reasons.push(crate::server::ReadinessDegradedReason::KmsNotReady);
        }
    }

    let payload = if method == Method::HEAD {
        None
    } else {
        Some(build_health_payload(HealthPayloadContext {
            health,
            storage_ready,
            iam_ready,
            lock_quorum_ready,
            degraded_reasons: &degraded_reasons,
            service,
            uptime,
            kms_ready,
            include_dependency_details,
        }))
    };

    HealthResponseParts {
        status_code: health.status_code,
        payload,
    }
}

pub(crate) fn build_health_payload(ctx: HealthPayloadContext<'_>) -> Value {
    if health_minimal_response_enabled() {
        return json!({
            "status": ctx.health.status,
            "ready": ctx.health.ready,
        });
    }

    let mut payload = json!({
        "status": ctx.health.status,
        "ready": ctx.health.ready,
        "service": ctx.service,
        "timestamp": jiff::Zoned::now().to_string(),
        "version": env!("CARGO_PKG_VERSION"),
    });

    if ctx.include_dependency_details {
        payload["details"] = build_component_details(ctx.storage_ready, ctx.iam_ready, ctx.lock_quorum_ready, ctx.kms_ready);
        payload["degradedReasons"] = build_degraded_reasons(ctx.degraded_reasons);
    }

    if let Some(uptime) = ctx.uptime {
        payload["uptime"] = json!(uptime);
    }

    payload
}

#[async_trait::async_trait]
impl Operation for HealthCheckHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Extract the original HTTP Method (encapsulated by s3s into S3Request)
        let method = req.method;

        // Only GET and HEAD are allowed
        if method != Method::GET && method != Method::HEAD {
            // 405 Method Not Allowed
            let mut headers = HeaderMap::new();
            headers.insert(http::header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(S3Response::with_headers(
                (StatusCode::METHOD_NOT_ALLOWED, Body::from("Method Not Allowed".to_string())),
                headers,
            ));
        }

        let probe = probe_from_path(req.uri.path());
        let readiness_report = if probe == HealthProbe::Readiness {
            Some(collect_dependency_readiness().await)
        } else {
            None
        };

        let response_parts =
            build_health_response_parts(method.clone(), probe, readiness_report.as_ref(), "rustfs-endpoint", None, None);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let response = if let Some(payload) = response_parts.payload {
            let body_str = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string());
            S3Response::with_headers((response_parts.status_code, Body::from(body_str)), headers)
        } else {
            S3Response::with_headers((response_parts.status_code, Body::empty()), headers)
        };

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_env::with_var;

    #[test]
    fn test_readiness_state_ready() {
        let state = health_check_state(true, true, true, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::OK);
        assert_eq!(state.status, "ok");
        assert!(state.ready);
    }

    #[test]
    fn test_readiness_state_storage_not_ready() {
        let state = health_check_state(false, true, true, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_liveness_state_iam_not_ready() {
        let state = health_check_state(true, false, true, HealthProbe::Liveness);
        assert_eq!(state.status_code, StatusCode::OK);
        assert_eq!(state.status, "ok");
        assert!(state.ready);
    }

    #[test]
    fn test_readiness_state_iam_not_ready() {
        let state = health_check_state(true, false, true, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_readiness_state_lock_not_ready() {
        let state = health_check_state(true, true, false, HealthProbe::Readiness);
        assert_eq!(state.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.status, "degraded");
        assert!(!state.ready);
    }

    #[test]
    fn test_health_check_component_details() {
        let details = build_component_details(true, false, false, None);

        assert_eq!(details["storage"]["status"], "connected");
        assert_eq!(details["storage"]["ready"], true);
        assert_eq!(details["iam"]["status"], "disconnected");
        assert_eq!(details["iam"]["ready"], false);
        assert_eq!(details["lock"]["status"], "disconnected");
        assert_eq!(details["lock"]["ready"], false);
        assert!(details.get("kms").is_none());
    }

    #[test]
    fn test_health_check_component_details_include_kms_when_present() {
        let details = build_component_details(true, true, true, Some(false));
        assert_eq!(details["kms"]["status"], "disconnected");
        assert_eq!(details["kms"]["ready"], false);
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
        let readiness_report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: false,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: vec![crate::server::ReadinessDegradedReason::StorageQuorumUnavailable],
        };
        let parts = build_health_response_parts(
            Method::GET,
            HealthProbe::Readiness,
            Some(&readiness_report),
            "rustfs-endpoint",
            None,
            None,
        );
        assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_build_health_response_readiness_returns_200_when_deps_ready() {
        let readiness_report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: true,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: Vec::new(),
        };
        let parts = build_health_response_parts(
            Method::GET,
            HealthProbe::Readiness,
            Some(&readiness_report),
            "rustfs-endpoint",
            None,
            None,
        );
        assert_eq!(parts.status_code, StatusCode::OK);
    }

    #[test]
    fn test_build_health_response_liveness_returns_200_when_deps_not_ready() {
        let readiness_report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: false,
                iam_ready: false,
                lock_quorum_ready: false,
            },
            degraded_reasons: vec![crate::server::ReadinessDegradedReason::StorageAndIamUnavailable],
        };
        let parts = build_health_response_parts(
            Method::GET,
            HealthProbe::Liveness,
            Some(&readiness_report),
            "rustfs-endpoint",
            None,
            None,
        );
        assert_eq!(parts.status_code, StatusCode::OK);
        let payload = parts.payload.expect("GET should include payload");
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["ready"], true);
        assert!(payload.get("details").is_none());
        assert!(payload.get("degradedReasons").is_none());
    }

    #[test]
    fn test_build_health_response_head_returns_empty_body() {
        let readiness_report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: false,
                iam_ready: false,
                lock_quorum_ready: false,
            },
            degraded_reasons: vec![crate::server::ReadinessDegradedReason::StorageAndIamUnavailable],
        };
        let parts = build_health_response_parts(
            Method::HEAD,
            HealthProbe::Readiness,
            Some(&readiness_report),
            "rustfs-endpoint",
            None,
            None,
        );
        assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(parts.payload.is_none());
    }

    #[test]
    fn test_build_health_payload_minimal_mode_returns_status_and_ready_only() {
        let health = health_check_state(true, false, true, HealthProbe::Readiness);
        with_var(rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("true"), || {
            let payload = build_health_payload(HealthPayloadContext {
                health,
                storage_ready: true,
                iam_ready: false,
                lock_quorum_ready: true,
                degraded_reasons: &[crate::server::ReadinessDegradedReason::IamNotReady],
                service: "rustfs-endpoint",
                uptime: Some(123),
                kms_ready: None,
                include_dependency_details: true,
            });
            assert_eq!(payload["status"], "degraded");
            assert_eq!(payload["ready"], false);
            assert!(payload.get("version").is_none());
            assert!(payload.get("details").is_none());
            assert!(payload.get("service").is_none());
            assert!(payload.get("uptime").is_none());
        });
    }

    #[test]
    fn test_build_health_payload_includes_degraded_reasons() {
        let health = health_check_state(false, false, false, HealthProbe::Readiness);
        let payload = build_health_payload(HealthPayloadContext {
            health,
            storage_ready: false,
            iam_ready: false,
            lock_quorum_ready: false,
            degraded_reasons: &[crate::server::ReadinessDegradedReason::StorageAndIamUnavailable],
            service: "rustfs-endpoint",
            uptime: None,
            kms_ready: None,
            include_dependency_details: true,
        });
        assert_eq!(payload["degradedReasons"][0], "storage_and_iam_unavailable");
    }

    #[test]
    fn test_build_health_response_parts_head_has_no_payload() {
        let report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: true,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: Vec::new(),
        };
        let parts =
            build_health_response_parts(Method::HEAD, HealthProbe::Readiness, Some(&report), "rustfs-endpoint", None, None);
        assert_eq!(parts.status_code, StatusCode::OK);
        assert!(parts.payload.is_none());
    }

    #[test]
    fn test_build_health_response_parts_get_includes_payload() {
        let report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: false,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: vec![crate::server::ReadinessDegradedReason::StorageQuorumUnavailable],
        };
        let parts =
            build_health_response_parts(Method::GET, HealthProbe::Readiness, Some(&report), "rustfs-endpoint", None, None);
        assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
        let payload = parts.payload.expect("GET should include payload");
        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["ready"], false);
        assert_eq!(payload["degradedReasons"][0], "storage_quorum_unavailable");
    }

    #[test]
    fn test_build_health_response_parts_readiness_marks_kms_not_ready() {
        let report = crate::server::DependencyReadinessReport {
            readiness: crate::server::DependencyReadiness {
                storage_ready: true,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: Vec::new(),
        };
        let parts =
            build_health_response_parts(Method::GET, HealthProbe::Readiness, Some(&report), "rustfs-endpoint", None, Some(false));
        assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
        let payload = parts.payload.expect("GET should include payload");
        assert_eq!(payload["ready"], false);
        assert_eq!(payload["details"]["lock"]["ready"], true);
        assert_eq!(payload["details"]["kms"]["ready"], false);
        assert_eq!(payload["degradedReasons"][0], "kms_not_ready");
    }
}
