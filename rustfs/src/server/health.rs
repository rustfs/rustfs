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

use super::readiness::{DependencyReadinessReport, ReadinessDegradedReason, record_readiness_overlay_reason};
use super::{
    HEALTH_READY_PATH, MINIO_HEALTH_CLUSTER_PATH, MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_READY_PATH,
    collect_cluster_read_health_report, collect_cluster_write_health_report, collect_node_readiness_report,
};
use crate::app::object_traffic_health::{ObjectTrafficHealth, ObjectTrafficSnapshot};
use http::{Method, StatusCode};
use rustfs_kms::ProbeStatus;
use rustfs_kms::probe::{DEFAULT_PROBE_INTERVAL, ENV_KMS_PROBE_INTERVAL_SECS, MIN_PROBE_INTERVAL};
use serde_json::{Value, json};
use std::time::Duration;

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

pub(crate) async fn collect_probe_readiness(
    probe: HealthProbe,
    object_traffic_health: Option<&ObjectTrafficHealth>,
) -> Option<DependencyReadinessReport> {
    let mut report = match readiness_source_for_probe(probe)? {
        HealthReadinessSource::Node => collect_node_readiness_report().await,
        HealthReadinessSource::ClusterWrite => collect_cluster_write_health_report().await,
        HealthReadinessSource::ClusterRead => collect_cluster_read_health_report().await,
    };
    if probe == HealthProbe::Readiness
        && let Some(object_traffic_health) = object_traffic_health
    {
        apply_object_traffic_snapshot(&mut report, object_traffic_health.snapshot());
    }
    Some(report)
}

fn apply_object_traffic_snapshot(report: &mut DependencyReadinessReport, snapshot: ObjectTrafficSnapshot) {
    if snapshot.read_stalled {
        let reason = ReadinessDegradedReason::ObjectReadStalled;
        report.degraded_reasons.push(reason);
        record_readiness_overlay_reason(reason);
    }
    if snapshot.write_stalled {
        let reason = ReadinessDegradedReason::ObjectWriteStalled;
        report.degraded_reasons.push(reason);
        record_readiness_overlay_reason(reason);
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

/// Consecutive failed probe rounds tolerated before readiness withdraws the KMS.
///
/// One failed round is routinely an external hiccup — a Vault leader election, a
/// dropped connection, a rotated token — and reporting not ready on the first one
/// would turn that hiccup into a rolling restart of every node that shares the
/// backend. Requiring the failure to survive three rounds keeps the signal while
/// costing at most three intervals of detection latency.
const KMS_PROBE_FAILURE_THRESHOLD: u32 = 3;

/// Probe rounds a snapshot may age before readiness stops acting on it.
///
/// Expressed in rounds rather than seconds so the bound follows the configured
/// interval: a deployment probing every ten minutes must not read every snapshot
/// as stale. Three rounds absorbs a round that overruns its slot or a worker that
/// misses a tick without discarding a signal that is merely late.
const KMS_PROBE_STALENESS_ROUNDS: u32 = 3;

/// Upper bound on the age of a probe snapshot that readiness is willing to act on.
pub(crate) fn kms_probe_staleness_limit() -> Duration {
    configured_kms_probe_interval().saturating_mul(KMS_PROBE_STALENESS_ROUNDS)
}

/// Probe interval currently in force.
///
/// Readiness reads the same variable as the probe worker because it only needs
/// the value to scale [`kms_probe_staleness_limit`]; the worker owns the interval
/// itself and keeps its parsing private.
fn configured_kms_probe_interval() -> Duration {
    kms_probe_interval_from_env(std::env::var(ENV_KMS_PROBE_INTERVAL_SECS).ok().as_deref())
}

/// Interval implied by a raw environment value, mirroring the worker's own rules:
/// unset or unparsable means the default, and anything below the floor is raised
/// to it. `0` disables the probe, which publishes no snapshot at all — the bound
/// is then never consulted, so the default stands in.
fn kms_probe_interval_from_env(value: Option<&str>) -> Duration {
    let Some(seconds) = value.and_then(|value| value.trim().parse::<u64>().ok()) else {
        return DEFAULT_PROBE_INTERVAL;
    };
    if seconds == 0 {
        return DEFAULT_PROBE_INTERVAL;
    }
    Duration::from_secs(seconds).max(MIN_PROBE_INTERVAL)
}

/// KMS readiness verdict from the service status bit and the background probe.
///
/// Reads only the published snapshot and never calls the backend: a readiness
/// request must not become load on a KMS that is already struggling, and a
/// Kubernetes probe timing out on an external dependency is how one slow Vault
/// becomes a fleet-wide restart.
///
/// The probe can only ever withdraw readiness from a service that is otherwise
/// running, and only on evidence that is both fresh and repeated. Everything the
/// probe cannot speak to — no worker (a backend without a data key round trip, or
/// the probe disabled), no completed round yet, or a snapshot older than
/// `staleness_limit` — leaves the status-bit verdict standing rather than
/// reporting a failure nobody observed.
pub(crate) fn kms_ready_from_probe(service_running: bool, probe: Option<&ProbeStatus>, staleness_limit: Duration) -> bool {
    if !service_running {
        return false;
    }

    let Some(probe) = probe else {
        return true;
    };

    let Some(age) = probe.last_round_age() else {
        return true;
    };

    if age > staleness_limit {
        return true;
    }

    // Every non-failure result resets the counter, so an unsupported backend or a
    // recovered round can never reach the threshold.
    probe.consecutive_failures < KMS_PROBE_FAILURE_THRESHOLD
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

    let object_traffic_stalled = degraded_reasons.iter().any(|reason| {
        matches!(
            reason,
            ReadinessDegradedReason::ObjectReadStalled | ReadinessDegradedReason::ObjectWriteStalled
        )
    });
    if probe == HealthProbe::Readiness && (object_traffic_stalled || matches!(kms_ready, Some(false))) {
        health = HealthCheckState {
            status_code: StatusCode::SERVICE_UNAVAILABLE,
            status: "degraded",
            ready: false,
        };
        if matches!(kms_ready, Some(false)) && !degraded_reasons.contains(&ReadinessDegradedReason::KmsNotReady) {
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

#[cfg(test)]
mod tests {
    use super::super::readiness::DependencyReadiness;
    use super::*;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use rustfs_kms::{ProbeFailureKind, ProbeResult};
    use serial_test::serial;
    use temp_env::with_var;
    use tokio::time::Instant;

    const STALENESS_LIMIT: Duration = Duration::from_secs(180);

    fn snapshot(result: ProbeResult, last_round_at: Option<Instant>, consecutive_failures: u32) -> ProbeStatus {
        ProbeStatus {
            result,
            last_round_at,
            last_success_at: last_round_at.filter(|_| result == ProbeResult::Success),
            last_success_unix_secs: None,
            consecutive_failures,
        }
    }

    fn ready_report() -> DependencyReadinessReport {
        DependencyReadinessReport {
            readiness: DependencyReadiness {
                storage_ready: true,
                iam_ready: true,
                lock_quorum_ready: true,
                peer_health_ready: true,
            },
            degraded_reasons: Vec::new(),
        }
    }

    #[tokio::test]
    async fn readiness_collects_object_stalls_and_recovers_on_completion() {
        let object_traffic_health = ObjectTrafficHealth::enabled_for_test(Duration::ZERO);
        let read = object_traffic_health
            .track_read_storage()
            .expect("read tracking must be enabled");
        let write = object_traffic_health
            .track_write_storage()
            .expect("write tracking must be enabled");

        let stalled = collect_probe_readiness(HealthProbe::Readiness, Some(&object_traffic_health))
            .await
            .expect("readiness must have a dependency report");
        assert!(stalled.degraded_reasons.contains(&ReadinessDegradedReason::ObjectReadStalled));
        assert!(
            stalled
                .degraded_reasons
                .contains(&ReadinessDegradedReason::ObjectWriteStalled)
        );

        drop(read);
        drop(write);
        let recovered = collect_probe_readiness(HealthProbe::Readiness, Some(&object_traffic_health))
            .await
            .expect("readiness must have a dependency report");
        assert!(
            !recovered
                .degraded_reasons
                .contains(&ReadinessDegradedReason::ObjectReadStalled)
        );
        assert!(
            !recovered
                .degraded_reasons
                .contains(&ReadinessDegradedReason::ObjectWriteStalled)
        );
    }

    #[test]
    #[serial]
    fn an_object_stall_degrades_readiness_without_changing_dependency_details() {
        with_var(rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false"), || {
            let mut report = ready_report();
            report.degraded_reasons.push(ReadinessDegradedReason::ObjectReadStalled);

            let parts =
                build_health_response_parts(Method::GET, HealthProbe::Readiness, Some(&report), "rustfs-endpoint", None, None);

            assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
            let payload = parts.payload.expect("GET should include payload");
            assert_eq!(payload["ready"], false);
            assert_eq!(payload["details"]["storage"]["ready"], true);
            assert_eq!(payload["degradedReasons"], json!(["object_read_stalled"]));
        });
    }

    #[test]
    fn object_stalls_do_not_change_liveness() {
        let mut report = ready_report();
        report.degraded_reasons.push(ReadinessDegradedReason::ObjectWriteStalled);

        let parts =
            build_health_response_parts(Method::HEAD, HealthProbe::Liveness, Some(&report), "rustfs-endpoint", None, None);

        assert_eq!(parts.status_code, StatusCode::OK);
        assert!(parts.payload.is_none());
    }

    #[test]
    fn object_stall_overlay_records_the_final_readiness_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut report = ready_report();
            apply_object_traffic_snapshot(
                &mut report,
                ObjectTrafficSnapshot {
                    read_stalled: true,
                    write_stalled: false,
                },
            );
        });

        let entries = snapshotter.snapshot().into_vec();
        let ready = entries.iter().find_map(|(composite, _, _, value)| {
            (composite.kind() == MetricKind::Gauge && composite.key().name() == "rustfs_runtime_readiness_ready").then_some(value)
        });
        assert!(matches!(ready, Some(DebugValue::Gauge(value)) if value.into_inner() == 0.0));

        let degraded = entries.iter().find_map(|(composite, _, _, value)| {
            (composite.kind() == MetricKind::Counter
                && composite.key().name() == "rustfs_runtime_readiness_degraded_total"
                && composite
                    .key()
                    .labels()
                    .any(|label| label.key() == "reason" && label.value() == "object_read_stalled"))
            .then_some(value)
        });
        assert!(matches!(degraded, Some(DebugValue::Counter(1))));
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_successful_round_keeps_the_service_ready() {
        let round_at = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let status = snapshot(ProbeResult::Success, Some(round_at), 0);
        assert!(kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_failed_round_does_not_withdraw_readiness() {
        let round_at = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let status = snapshot(
            ProbeResult::Failure(ProbeFailureKind::Decrypt),
            Some(round_at),
            KMS_PROBE_FAILURE_THRESHOLD - 1,
        );
        assert!(kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn failures_reaching_the_threshold_withdraw_readiness() {
        let round_at = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let status = snapshot(
            ProbeResult::Failure(ProbeFailureKind::Generate),
            Some(round_at),
            KMS_PROBE_FAILURE_THRESHOLD,
        );
        assert!(!kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_snapshot_falls_back_to_the_status_bit() {
        let round_at = Instant::now();
        tokio::time::advance(STALENESS_LIMIT + Duration::from_secs(1)).await;

        let status = snapshot(
            ProbeResult::Failure(ProbeFailureKind::Mismatch),
            Some(round_at),
            KMS_PROBE_FAILURE_THRESHOLD * 10,
        );
        assert!(
            kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT),
            "a snapshot nobody refreshed is unknown, not a failure"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unsupported_backend_is_not_a_failure() {
        let round_at = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let status = snapshot(ProbeResult::Unsupported, Some(round_at), 0);
        assert!(kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_service_stays_not_ready_despite_a_successful_round() {
        let round_at = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let status = snapshot(ProbeResult::Success, Some(round_at), 0);
        assert!(!kms_ready_from_probe(false, Some(&status), STALENESS_LIMIT));
    }

    #[test]
    fn a_service_without_a_probe_keeps_the_status_bit_verdict() {
        assert!(kms_ready_from_probe(true, None, STALENESS_LIMIT));
        assert!(!kms_ready_from_probe(false, None, STALENESS_LIMIT));
    }

    #[test]
    fn a_probe_with_no_completed_round_keeps_the_service_ready() {
        let status = snapshot(ProbeResult::Pending, None, 0);
        assert!(kms_ready_from_probe(true, Some(&status), STALENESS_LIMIT));
    }

    #[test]
    fn the_probe_interval_follows_the_worker_parsing_rules() {
        assert_eq!(kms_probe_interval_from_env(None), DEFAULT_PROBE_INTERVAL);
        assert_eq!(kms_probe_interval_from_env(Some("not-a-number")), DEFAULT_PROBE_INTERVAL);
        assert_eq!(kms_probe_interval_from_env(Some("0")), DEFAULT_PROBE_INTERVAL);
        assert_eq!(kms_probe_interval_from_env(Some(" 600 ")), Duration::from_secs(600));
        assert_eq!(kms_probe_interval_from_env(Some("1")), MIN_PROBE_INTERVAL);
    }

    #[test]
    #[serial]
    fn the_staleness_bound_spans_three_probe_rounds() {
        with_var(ENV_KMS_PROBE_INTERVAL_SECS, Some("30"), || {
            assert_eq!(kms_probe_staleness_limit(), Duration::from_secs(90));
        });
    }

    /// Flipping this default is a deployment behaviour change, not a code
    /// change: it would start failing readiness on clusters that never asked
    /// for the KMS to gate it. Pinned at compile time so the constant cannot
    /// drift without this line being edited too.
    const _: () = assert!(!rustfs_config::DEFAULT_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE);

    #[test]
    #[serial]
    fn a_withdrawn_kms_degrades_the_readiness_response() {
        with_var(rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false"), || {
            let report = ready_report();
            let parts = build_health_response_parts(
                Method::GET,
                HealthProbe::Readiness,
                Some(&report),
                "rustfs-endpoint",
                None,
                Some(false),
            );

            assert_eq!(parts.status_code, StatusCode::SERVICE_UNAVAILABLE);
            let payload = parts.payload.expect("GET should include payload");
            assert_eq!(payload["details"]["kms"]["ready"], false);
            assert_eq!(payload["degradedReasons"], json!(["kms_not_ready"]));
        });
    }

    #[test]
    #[serial]
    fn a_disabled_check_leaves_the_readiness_response_untouched() {
        with_var(rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false"), || {
            let report = ready_report();
            let parts =
                build_health_response_parts(Method::GET, HealthProbe::Readiness, Some(&report), "rustfs-endpoint", None, None);

            assert_eq!(parts.status_code, StatusCode::OK);
            let payload = parts.payload.expect("GET should include payload");
            assert!(payload["details"].get("kms").is_none());
            assert_eq!(payload["degradedReasons"], json!([]));
        });
    }
}
