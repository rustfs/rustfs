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
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_scanner_metrics_report;
use crate::module_switches::{ENV_SCANNER_ENABLED, scanner_enabled_from_env};
use crate::server::ADMIN_PREFIX;
use chrono::Utc;
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::metrics::{ScannerLifecycleExpirySnapshot, ScannerMaintenanceControlSnapshot, ScannerMetricsReport};
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
    freshness: ScannerFreshnessStatus,
    metrics: ScannerMetricsReport,
    cycle_schedule: rustfs_scanner::ScannerCycleScheduleStatus,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
}

#[derive(Debug, Serialize)]
struct ScannerFreshnessStatus {
    state: &'static str,
    last_cycle_end_unix_secs: u64,
    max_expected_age_seconds: u64,
    reason: Option<&'static str>,
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
) -> ScannerStatusResponse {
    let freshness = scanner_freshness_status(&metrics, &runtime_config, cycle_schedule.effective_interval_seconds());
    ScannerStatusResponse {
        enabled,
        disabled_reason: scanner_disabled_reason(enabled),
        freshness,
        metrics,
        cycle_schedule,
        runtime_config,
    }
}

fn ilm_expiry_status_response(
    enabled: bool,
    metrics: ScannerMetricsReport,
    runtime_config: rustfs_scanner::runtime_config::ScannerRuntimeConfigStatus,
    cycle_schedule: rustfs_scanner::ScannerCycleScheduleStatus,
) -> IlmExpiryStatusResponse {
    let freshness = scanner_freshness_status(&metrics, &runtime_config, cycle_schedule.effective_interval_seconds());
    IlmExpiryStatusResponse {
        enabled,
        disabled_reason: scanner_disabled_reason(enabled),
        freshness,
        lifecycle_expiry: metrics.lifecycle_expiry,
        maintenance_control: metrics.maintenance_control,
        current_cycle_lifecycle_expiry_actions: metrics.current_cycle_lifecycle_expiry_actions,
        last_cycle_lifecycle_expiry_actions: metrics.last_cycle_lifecycle_expiry_actions,
    }
}

pub fn register_scanner_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/scanner/status").as_str(),
        AdminOperation(&ScannerStatusHandler {}),
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
        let metrics = current_scanner_metrics_report().await;
        let runtime_config = rustfs_scanner::scanner_runtime_config_status();
        let cycle_schedule = rustfs_scanner::scanner_cycle_schedule_status();
        let response = scanner_status_response(enabled, metrics, runtime_config, cycle_schedule);
        let body = serde_json::to_vec(&response).map_err(|err| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode scanner status: {err}"))
        })?;

        json_response(body)
    }
}

pub struct IlmExpiryStatusHandler {}

#[async_trait::async_trait]
impl Operation for IlmExpiryStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _cred = validate_scanner_status_request(&req).await?;
        let enabled = scanner_enabled_from_env();
        let metrics = current_scanner_metrics_report().await;
        let runtime_config = rustfs_scanner::scanner_runtime_config_status();
        let cycle_schedule = rustfs_scanner::scanner_cycle_schedule_status();
        let response = ilm_expiry_status_response(enabled, metrics, runtime_config, cycle_schedule);
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
        );

        let encoded = serde_json::to_value(response).expect("scanner status should serialize");
        assert_eq!(encoded["cycle_schedule"]["effective_interval_seconds"], 0);
        assert_eq!(encoded["cycle_schedule"]["clean_idle_backoff_enabled"], false);
        assert_eq!(encoded["cycle_schedule"]["clean_idle_backoff_multiplier"], 1);
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
        );

        let encoded = serde_json::to_value(response).expect("ILM expiry status should serialize");
        assert_eq!(encoded["lifecycle_expiry"]["current_queue_capacity"].as_u64(), Some(32));
        assert_eq!(encoded["lifecycle_expiry"]["scanner_blocked"].as_u64(), Some(11));
        assert_eq!(encoded["lifecycle_expiry"]["scanner_not_enqueued"].as_u64(), Some(13));
        assert_eq!(encoded["lifecycle_expiry"]["delete_failed"].as_u64(), Some(19));
        assert_eq!(encoded["maintenance_control"]["primary_control"].as_str(), Some("expiry_backlog"));
        assert_eq!(encoded["current_cycle_lifecycle_expiry_actions"].as_u64(), Some(23));
        assert_eq!(encoded["last_cycle_lifecycle_expiry_actions"].as_u64(), Some(29));
    }
}
