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

use super::readiness::{DependencyReadinessReport, ReadinessDegradedReason};
use super::{
    HEALTH_READY_PATH, MINIO_HEALTH_CLUSTER_PATH, MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_READY_PATH,
    collect_cluster_read_health_report, collect_cluster_write_health_report, collect_node_readiness_report,
};
use http::{Method, StatusCode};
use serde_json::{Value, json};

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
    ClusterWrite,
    ClusterRead,
}

impl HealthProbe {
    const fn requires_lock_quorum(self) -> bool {
        matches!(self, Self::ClusterWrite | Self::ClusterRead)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthReadinessSource {
    Node,
    ClusterWrite,
    ClusterRead,
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
    pub(crate) degraded_reasons: &'a [ReadinessDegradedReason],
    pub(crate) service: &'a str,
    pub(crate) uptime: Option<u64>,
    pub(crate) kms_ready: Option<bool>,
    pub(crate) include_dependency_details: bool,
}

pub(crate) async fn collect_probe_readiness(probe: HealthProbe) -> Option<DependencyReadinessReport> {
    match readiness_source_for_probe(probe)? {
        HealthReadinessSource::Node => Some(collect_node_readiness_report().await),
        HealthReadinessSource::ClusterWrite => Some(collect_cluster_write_health_report().await),
        HealthReadinessSource::ClusterRead => Some(collect_cluster_read_health_report().await),
    }
}

pub(crate) fn readiness_source_for_probe(probe: HealthProbe) -> Option<HealthReadinessSource> {
    match probe {
        HealthProbe::Liveness => None,
        HealthProbe::Readiness => Some(HealthReadinessSource::Node),
        HealthProbe::ClusterWrite => Some(HealthReadinessSource::ClusterWrite),
        HealthProbe::ClusterRead => Some(HealthReadinessSource::ClusterRead),
    }
}

pub(crate) fn health_check_state(
    storage_ready: bool,
    iam_ready: bool,
    lock_quorum_ready: bool,
    peer_health_ready: bool,
    probe: HealthProbe,
) -> HealthCheckState {
    if probe == HealthProbe::Liveness {
        return HealthCheckState {
            status_code: StatusCode::OK,
            status: "ok",
            ready: true,
        };
    }

    let ready = storage_ready && iam_ready && peer_health_ready && (!probe.requires_lock_quorum() || lock_quorum_ready);
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

pub(crate) fn build_degraded_reasons(reasons: &[ReadinessDegradedReason]) -> Value {
    Value::Array(
        reasons
            .iter()
            .map(|reason| Value::String(reason.as_str().to_string()))
            .collect(),
    )
}

pub(crate) fn probe_from_path(path: &str) -> HealthProbe {
    match path {
        HEALTH_READY_PATH | MINIO_HEALTH_READY_PATH => HealthProbe::Readiness,
        MINIO_HEALTH_CLUSTER_PATH => HealthProbe::ClusterWrite,
        MINIO_HEALTH_CLUSTER_READ_PATH => HealthProbe::ClusterRead,
        _ => HealthProbe::Liveness,
    }
}

pub(crate) fn build_health_response_parts(
    method: Method,
    probe: HealthProbe,
    readiness_report: Option<&DependencyReadinessReport>,
    service: &str,
    uptime: Option<u64>,
    kms_ready: Option<bool>,
) -> HealthResponseParts {
    let (storage_ready, iam_ready, lock_quorum_ready, mut health, mut degraded_reasons, include_dependency_details) =
        match (probe, readiness_report) {
            (probe @ (HealthProbe::Readiness | HealthProbe::ClusterWrite | HealthProbe::ClusterRead), Some(readiness_report)) => {
                let storage_ready = readiness_report.readiness.storage_ready;
                let iam_ready = readiness_report.readiness.iam_ready;
                let lock_quorum_ready = readiness_report.readiness.lock_quorum_ready;
                let peer_health_ready = readiness_report.readiness.peer_health_ready;
                (
                    storage_ready,
                    iam_ready,
                    lock_quorum_ready,
                    health_check_state(storage_ready, iam_ready, lock_quorum_ready, peer_health_ready, probe),
                    readiness_report.degraded_reasons.clone(),
                    true,
                )
            }
            (HealthProbe::Readiness | HealthProbe::ClusterWrite | HealthProbe::ClusterRead, None) => (
                false,
                false,
                false,
                HealthCheckState {
                    status_code: StatusCode::SERVICE_UNAVAILABLE,
                    status: "degraded",
                    ready: false,
                },
                vec![ReadinessDegradedReason::StorageIamAndLockUnavailable],
                true,
            ),
            (HealthProbe::Liveness, _) => (
                false,
                false,
                false,
                health_check_state(false, false, false, false, probe),
                Vec::new(),
                false,
            ),
        };

    if probe == HealthProbe::Readiness && matches!(kms_ready, Some(false)) {
        health = HealthCheckState {
            status_code: StatusCode::SERVICE_UNAVAILABLE,
            status: "degraded",
            ready: false,
        };
        if !degraded_reasons.contains(&ReadinessDegradedReason::KmsNotReady) {
            degraded_reasons.push(ReadinessDegradedReason::KmsNotReady);
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
