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

use super::{cluster_snapshot, metrics};
use crate::admin::auth::validate_admin_request;
use crate::admin::route_policy::{
    ADMIN_ROUTE_POLICY_SPECS, DEFERRED_ADMIN_ROUTE_POLICIES, DeferredAdminRoutePolicy, DeferredRoutePolicyReason,
};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    DefaultAdminUsecase, QueryServerInfoRequest, current_endpoints_handle, default_admin_usecase, object_store_from_req,
};
use crate::admin::storage_api::bucket::replication::{
    REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION, REMOTE_TARGET_UNSUPPORTED_FIELDS, REMOTE_TARGET_WRITABLE_FIELDS,
    REPLICATION_CAPABILITY_CONTRACT_VERSION, REPLICATION_READ_ONLY_HISTORICAL_FIELDS, REPLICATION_WRITABLE_FIELDS,
};
use crate::admin::storage_api::cluster::{
    CapabilityState, CapabilityStatus, ObservabilitySnapshotProvider, TopologySnapshot, TopologySnapshotProvider,
};
use crate::admin::storage_api::storageclass as storage_class_contract;
use crate::auth::{check_key_valid, get_session_token};
use crate::runtime_capabilities::{EndpointTopologySnapshotProvider, RustFsObservabilitySnapshotProvider};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::workload_admission::workload_admission_registry_snapshot;
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_concurrency::WorkloadAdmissionRegistrySnapshot;
use rustfs_madmin::{InfoMessage, StorageInfo};
use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
use rustfs_security_governance::{AdminRouteSpec, HttpMethod};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info, warn};

/// Global service-freeze flag toggled by `POST /v3/service?action=freeze`.
///
/// NOTE: RustFS does not currently route S3 request admission through this flag,
/// so freezing is advisory only — it records intent and is reflected in the
/// response, but does not actually suspend request handling. This is documented
/// in the handler and surfaced to the caller via `frozen_effective=false`.
static SERVICE_FROZEN: AtomicBool = AtomicBool::new(false);

const LOG_COMPONENT_ADMIN_API: &str = "admin_api";
const LOG_SUBSYSTEM_SYSTEM_ADMIN: &str = "system_admin";
const EVENT_ADMIN_REQUEST_REJECTED: &str = "admin_request_rejected";
const EVENT_ADMIN_REQUEST_FAILED: &str = "admin_request_failed";
const EVENT_ADMIN_RESPONSE_EMITTED: &str = "admin_response_emitted";
const OBSERVABILITY_SUMMARY_RESOLVED: &str = "observability summary resolved from provider snapshots";
const TOPOLOGY_SUMMARY_RESOLVED: &str = "topology summary resolved from capability snapshot";
const TOPOLOGY_SNAPSHOT_NOT_AVAILABLE: &str = "endpoint topology is not available before storage endpoint pools initialize";
pub(crate) const RUNTIME_CAPABILITIES_ROUTE_SUFFIX: &str = "/v4/runtime/capabilities";
const MANUAL_TRANSITION_RUN_ROUTE: &str = "/rustfs/admin/v3/ilm/transition/run";
const MANUAL_TRANSITION_JOB_ROUTE: &str = "/rustfs/admin/v3/ilm/transition/jobs/{job_id}";
const SITE_REPLICATION_INFO_ROUTE: &str = "/rustfs/admin/v3/site-replication/info";
const SITE_REPLICATION_EDIT_ROUTE: &str = "/rustfs/admin/v3/site-replication/edit";
const SITE_REPLICATION_RESYNC_ROUTE: &str = "/rustfs/admin/v3/site-replication/resync/op";
const SITE_REPLICATION_REPAIR_ROUTE: &str = "/rustfs/admin/v3/site-replication/repair";
const SITE_REPLICATION_REPAIR_STATUS_ROUTE: &str = "/rustfs/admin/v3/site-replication/repair/status";

macro_rules! log_system_request_rejected {
    ($operation:expr, $reason:expr) => {
        warn!(
            event = EVENT_ADMIN_REQUEST_REJECTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
            operation = $operation,
            result = "rejected",
            reason = $reason,
            "admin request rejected"
        );
    };
}

macro_rules! log_system_request_failed {
    ($operation:expr, $reason:expr, $err:expr) => {
        error!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
            operation = $operation,
            result = "failed",
            reason = $reason,
            error = %$err,
            "admin request failed"
        );
    };
}

macro_rules! log_system_response_emitted {
    ($operation:expr) => {
        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
            operation = $operation,
            result = "success",
            "admin response emitted"
        );
    };
}

pub fn register_system_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/service").as_str(),
        AdminOperation(&ServiceHandle {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/update").as_str(),
        AdminOperation(&UpdateHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/info").as_str(),
        AdminOperation(&ServerInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&InspectDataHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/inspect-data").as_str(),
        AdminOperation(&InspectDataHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/storageinfo").as_str(),
        AdminOperation(&StorageInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/datausageinfo").as_str(),
        AdminOperation(&DataUsageInfoHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/metrics").as_str(),
        AdminOperation(&metrics::MetricsHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, RUNTIME_CAPABILITIES_ROUTE_SUFFIX).as_str(),
        AdminOperation(&RuntimeCapabilitiesHandler {}),
    )?;

    Ok(())
}
pub struct ServiceHandle {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceAction {
    Restart,
    Stop,
    Freeze,
    Unfreeze,
}

impl ServiceAction {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "restart" => Some(Self::Restart),
            "stop" => Some(Self::Stop),
            "freeze" => Some(Self::Freeze),
            "unfreeze" => Some(Self::Unfreeze),
            _ => None,
        }
    }
}

fn service_action_from_uri(uri: &http::Uri) -> Option<ServiceAction> {
    uri.query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes()).find_map(|(k, v)| if k == "action" { ServiceAction::parse(&v) } else { None })
    })
}

#[derive(Serialize)]
struct ServiceActionResponse {
    action: &'static str,
    accepted: bool,
    /// Whether the action takes real effect on this build (vs advisory only).
    effective: bool,
    message: &'static str,
}

/// Ask the process to shut down gracefully by raising SIGTERM to itself.
///
/// The existing `wait_for_shutdown()` signal handler observes SIGTERM and runs
/// the full graceful-shutdown sequence (drains servers, flushes audit/event
/// notifiers). RustFS has no in-process supervisor that re-execs the binary, so
/// `restart` and `stop` are both honored as a graceful stop; a process manager
/// (systemd, k8s) is responsible for restarting the binary. This is documented
/// in the response so operators are not misled into expecting an in-process
/// re-exec.
// SAFETY: the only unsafe operation is `libc::raise(SIGTERM)`, which delivers a
// signal to the current process using a compile-time constant signal number and
// no pointers. It simply routes into the existing `wait_for_shutdown()` SIGTERM
// handler that runs the graceful-shutdown sequence.
#[cfg(unix)]
#[allow(unsafe_code)]
fn request_graceful_shutdown() {
    // Defer slightly so the HTTP response can flush before shutdown begins.
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        // SAFETY: see the function-level comment; `libc::raise` with a constant
        // signal number is async-signal-safe and takes no pointer arguments.
        unsafe {
            libc::raise(libc::SIGTERM);
        }
    });
}

#[cfg(not(unix))]
fn request_graceful_shutdown() {}

#[async_trait::async_trait]
impl Operation for ServiceHandle {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials.as_ref() else {
            log_system_request_rejected!("service_handle", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let Some(action) = service_action_from_uri(&req.uri) else {
            log_system_request_rejected!("service_handle", "invalid_action");
            return Err(s3_error!(InvalidRequest, "action must be one of: restart, stop, freeze, unfreeze"));
        };

        // restart/stop are destructive; freeze/unfreeze map to their own actions.
        let admin_action = match action {
            ServiceAction::Restart => AdminAction::ServiceRestartAdminAction,
            ServiceAction::Stop => AdminAction::ServiceStopAdminAction,
            ServiceAction::Freeze | ServiceAction::Unfreeze => AdminAction::ServiceFreezeAdminAction,
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(admin_action)], remote_addr).await?;

        let response = match action {
            ServiceAction::Restart => {
                info!(
                    event = "service_control",
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
                    action = "restart",
                    "admin requested service restart; initiating graceful shutdown (process manager must relaunch)"
                );
                request_graceful_shutdown();
                ServiceActionResponse {
                    action: "restart",
                    accepted: true,
                    effective: true,
                    message: "graceful shutdown initiated; the supervising process manager is responsible for relaunch",
                }
            }
            ServiceAction::Stop => {
                info!(
                    event = "service_control",
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
                    action = "stop",
                    "admin requested service stop; initiating graceful shutdown"
                );
                request_graceful_shutdown();
                ServiceActionResponse {
                    action: "stop",
                    accepted: true,
                    effective: true,
                    message: "graceful shutdown initiated",
                }
            }
            ServiceAction::Freeze => {
                SERVICE_FROZEN.store(true, Ordering::SeqCst);
                warn!(
                    event = "service_control",
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
                    action = "freeze",
                    "service freeze flag set; note request admission is not yet gated by this flag"
                );
                ServiceActionResponse {
                    action: "freeze",
                    accepted: true,
                    effective: false,
                    message: "freeze flag recorded, but RustFS does not yet gate request admission on it (advisory only)",
                }
            }
            ServiceAction::Unfreeze => {
                SERVICE_FROZEN.store(false, Ordering::SeqCst);
                info!(
                    event = "service_control",
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
                    action = "unfreeze",
                    "service freeze flag cleared"
                );
                ServiceActionResponse {
                    action: "unfreeze",
                    accepted: true,
                    effective: false,
                    message: "freeze flag cleared (advisory only)",
                }
            }
        };

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_system_request_failed!("service_handle", "serialize_service_response_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize service response")
        })?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("service_handle");
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct UpdateHandler {}

#[derive(Serialize)]
struct ServerUpdateStatus {
    current_version: String,
    updated_version: String,
    /// Always false: RustFS ships no in-process binary self-update mechanism.
    update_applied: bool,
    message: &'static str,
}

#[async_trait::async_trait]
impl Operation for UpdateHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials.as_ref() else {
            log_system_request_rejected!("server_update", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };
        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerUpdateAdminAction)],
            remote_addr,
        )
        .await?;

        // MinIO's server-update downloads and swaps the binary in place. RustFS
        // intentionally does not implement in-process self-update: binaries are
        // managed by the packaging/orchestration layer (container image, systemd
        // unit, package manager). We honor the request/response contract and
        // report that no update was applied rather than faking success.
        let current = crate::version::get_version();
        let response = ServerUpdateStatus {
            current_version: current.clone(),
            updated_version: current,
            update_applied: false,
            message: "in-process self-update is not supported; manage the RustFS binary via your image/package/orchestrator",
        };
        info!(
            event = "server_update",
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_SYSTEM_ADMIN,
            "server update requested; self-update unsupported, returning MinIO-compatible no-op status"
        );

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_system_request_failed!("server_update", "serialize_update_status_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize update status")
        })?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("server_update");
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct ServerInfoHandler {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SystemAdminDiscovery {
    #[serde(rename = "runtimeCapabilities")]
    runtime_capabilities: String,
    #[serde(rename = "clusterSnapshot")]
    cluster_snapshot: String,
    #[serde(rename = "extensionsCatalog")]
    extensions_catalog: String,
}

#[derive(Serialize)]
struct ServerInfoResponse {
    info: InfoMessage,
    admin_discovery: SystemAdminDiscovery,
    /// Startup bitrot algorithm self-test outcome (rustfs/backlog#1873):
    /// `passed` (algorithms verified at boot), `failed` (a drifted hash
    /// implementation — the process is serving with degraded integrity
    /// checking unless `RUSTFS_BITROT_SELFTEST_STRICT` aborted it), or
    /// `unknown` (not yet run or disabled).
    #[serde(rename = "bitrotSelftest")]
    bitrot_selftest: &'static str,
}

#[derive(Serialize)]
struct StorageInfoResponse {
    info: StorageInfo,
    admin_discovery: SystemAdminDiscovery,
}

fn system_admin_discovery(usecase: &DefaultAdminUsecase) -> SystemAdminDiscovery {
    SystemAdminDiscovery {
        runtime_capabilities: usecase.runtime_capabilities_route().to_string(),
        cluster_snapshot: usecase.cluster_snapshot_route().to_string(),
        extensions_catalog: usecase.extensions_catalog_route().to_string(),
    }
}

fn bitrot_selftest_status_str() -> &'static str {
    match crate::bitrot_selftest::bitrot_selftest_passed() {
        Some(true) => "passed",
        Some(false) => "failed",
        None => "unknown",
    }
}

#[async_trait::async_trait]
impl Operation for ServerInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            log_system_request_rejected!("query_server_info", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            remote_addr,
        )
        .await?;

        let usecase = default_admin_usecase();
        let info = usecase
            .execute_query_server_info(QueryServerInfoRequest { include_pools: true })
            .await
            .map_err(S3Error::from)?
            .info;
        let response = ServerInfoResponse {
            info,
            admin_discovery: system_admin_discovery(&usecase),
            bitrot_selftest: bitrot_selftest_status_str(),
        };

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_system_request_failed!("query_server_info", "serialize_server_info_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "parse serverInfo failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("query_server_info");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct InspectDataHandler {}

/// Upper bound on how many bytes a single inspect-data response streams, to keep
/// an operator-facing diagnostic from materializing an arbitrarily large object.
const INSPECT_DATA_MAX_BYTES: usize = 64 * 1024 * 1024;

fn inspect_data_target(uri: &http::Uri) -> Option<(String, String)> {
    let mut volume: Option<String> = None;
    let mut file: Option<String> = None;
    if let Some(query) = uri.query() {
        for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
            match k.as_ref() {
                // Accept MinIO's `volume`/`file` names and the friendlier
                // `bucket`/`object` aliases.
                "volume" | "bucket" => volume = Some(v.into_owned()),
                "file" | "object" | "prefix" => file = Some(v.into_owned()),
                _ => {}
            }
        }
    }
    match (volume, file) {
        (Some(v), Some(f)) if !v.is_empty() && !f.is_empty() => Some((v, f)),
        _ => None,
    }
}

#[async_trait::async_trait]
impl Operation for InspectDataHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        use crate::admin::storage_api::contract::object::ObjectIO as _;
        use crate::admin::storage_api::object::StorageObjectOptions;
        use tokio::io::AsyncReadExt;

        let Some(input_cred) = req.credentials.as_ref() else {
            log_system_request_rejected!("inspect_data", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };
        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::InspectDataAction)],
            remote_addr,
        )
        .await?;

        // MinIO's inspect-data exports a signed archive of raw drive files for a
        // `volume`/`file` glob. RustFS erasure-codes and (optionally) encrypts
        // object data across drives, so there is no single on-disk file to hand
        // back; instead we return the reconstructed raw object bytes for the
        // requested `volume` (bucket) + `file` (object), which is the honest,
        // usable form of "inspect this object's data" against an EC store.
        let Some((bucket, object)) = inspect_data_target(&req.uri) else {
            return Err(s3_error!(
                InvalidRequest,
                "inspect-data requires `volume` (bucket) and `file` (object) query parameters"
            ));
        };

        let Some(store) = object_store_from_req(&req) else {
            log_system_request_failed!("inspect_data", "object_store_unavailable", "not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized"));
        };

        let mut reader = store
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &StorageObjectOptions::default())
            .await
            .map_err(|err| {
                log_system_request_failed!("inspect_data", "open_object_reader_failed", err);
                S3Error::with_message(S3ErrorCode::NoSuchKey, format!("failed to open object `{bucket}/{object}`: {err}"))
            })?;

        // Read up to the cap + 1 so we can detect and reject over-large targets.
        let mut buf = Vec::new();
        let mut limited = (&mut reader).take((INSPECT_DATA_MAX_BYTES as u64) + 1);
        limited
            .read_to_end(&mut buf)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to read object data: {e}")))?;
        if buf.len() > INSPECT_DATA_MAX_BYTES {
            return Err(s3_error!(
                InvalidRequest,
                "object exceeds the {INSPECT_DATA_MAX_BYTES}-byte inspect-data limit; fetch it via the S3 API instead"
            ));
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
        if let Ok(disposition) = HeaderValue::from_str(&format!("attachment; filename=\"inspect-{bucket}-{object}.bin\"")) {
            header.insert(http::header::CONTENT_DISPOSITION, disposition);
        }
        log_system_response_emitted!("inspect_data");
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(buf)), header))
    }
}

pub struct StorageInfoHandler {}

#[async_trait::async_trait]
impl Operation for StorageInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            log_system_request_rejected!("query_storage_info", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::StorageInfoAdminAction)],
            remote_addr,
        )
        .await?;

        let usecase = default_admin_usecase();
        let info = usecase.execute_query_storage_info().await.map_err(S3Error::from)?;
        let response = StorageInfoResponse {
            info,
            admin_discovery: system_admin_discovery(&usecase),
        };

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_system_request_failed!("query_storage_info", "serialize_storage_info_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize storage info")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("query_storage_info");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct DataUsageInfoHandler {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilitiesSummary {
    pub observability: CapabilityStatus,
    pub userspace_profiling: CapabilityStatus,
    pub memory_sampling: CapabilityStatus,
    pub platform: CapabilityStatus,
    pub topology: CapabilityStatus,
    pub cluster_snapshot: CapabilityStatus,
    #[serde(default)]
    pub site_replication_info: CapabilityStatus,
    #[serde(default)]
    pub site_replication_edit: CapabilityStatus,
    #[serde(default)]
    pub site_replication_resync: CapabilityStatus,
    #[serde(default)]
    pub site_replication_repair: CapabilityStatus,
    #[serde(default)]
    pub manual_transition_jobs: CapabilityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeCapabilitiesResponse {
    pub summary: RuntimeCapabilitiesSummary,
    pub replication: ReplicationCapabilities,
    pub manual_transition_jobs: ManualTransitionJobCapabilities,
    pub diagnostic_probes: DiagnosticProbeCapabilities,
    pub inspect_archive: super::inspect_archive::InspectArchiveCapability,
    pub storage_classes: StorageClassCapabilities,
    pub site_replication_repair: SiteReplicationRepairCapabilities,
    pub cluster_snapshot_path: String,
    pub cluster_snapshot_summary: Option<CapabilityStatus>,
    pub observability: crate::admin::storage_api::cluster::ObservabilitySnapshot,
    pub workload_admission: WorkloadAdmissionRegistrySnapshot,
    pub topology: Option<TopologySnapshot>,
    pub topology_status: CapabilityStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationFieldState {
    Supported,
    ReadOnlyHistorical,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReplicationFieldCapability {
    pub name: &'static str,
    pub state: ReplicationFieldState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicationFeatureCapabilities {
    pub contract_version: u32,
    pub status: CapabilityStatus,
    pub fields: Vec<ReplicationFieldCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicationCapabilities {
    pub contract_version: u32,
    pub bucket_replication: ReplicationFeatureCapabilities,
    pub remote_targets: ReplicationFeatureCapabilities,
    pub mixed_version_policy: &'static str,
}

impl ReplicationCapabilities {
    fn current() -> Self {
        let remote_target_routes = [
            admin_route_capability(HttpMethod::Get, "/rustfs/admin/v3/list-remote-targets"),
            admin_route_capability(HttpMethod::Put, "/rustfs/admin/v3/set-remote-target"),
            admin_route_capability(HttpMethod::Delete, "/rustfs/admin/v3/remove-remote-target"),
        ];
        let remote_targets_supported = remote_target_routes
            .iter()
            .all(|status| status.state == CapabilityState::Supported);

        Self {
            contract_version: REPLICATION_CAPABILITY_CONTRACT_VERSION,
            bucket_replication: ReplicationFeatureCapabilities {
                contract_version: REPLICATION_CAPABILITY_CONTRACT_VERSION,
                status: CapabilityStatus::supported()
                    .with_reason("bucket replication validation and execution contracts are registered"),
                fields: REPLICATION_WRITABLE_FIELDS
                    .iter()
                    .copied()
                    .map(|name| ReplicationFieldCapability {
                        name,
                        state: ReplicationFieldState::Supported,
                    })
                    .chain(
                        REPLICATION_READ_ONLY_HISTORICAL_FIELDS
                            .iter()
                            .copied()
                            .map(|name| ReplicationFieldCapability {
                                name,
                                state: ReplicationFieldState::ReadOnlyHistorical,
                            }),
                    )
                    .collect(),
            },
            remote_targets: ReplicationFeatureCapabilities {
                contract_version: REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION,
                status: if remote_targets_supported {
                    CapabilityStatus::supported().with_reason("remote target routes and field validation are registered")
                } else {
                    CapabilityStatus::unsupported().with_reason("one or more remote target routes are unavailable")
                },
                fields: REMOTE_TARGET_WRITABLE_FIELDS
                    .iter()
                    .copied()
                    .map(|name| ReplicationFieldCapability {
                        name,
                        state: ReplicationFieldState::Supported,
                    })
                    .chain(
                        REMOTE_TARGET_UNSUPPORTED_FIELDS
                            .iter()
                            .copied()
                            .map(|name| ReplicationFieldCapability {
                                name,
                                state: ReplicationFieldState::Unsupported,
                            }),
                    )
                    .collect(),
            },
            mixed_version_policy: "fail_closed_when_capability_unknown_or_unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManualTransitionJobCapabilities {
    pub contract_version: u32,
    pub status: CapabilityStatus,
    pub modes: [&'static str; 2],
    pub run_route: &'static str,
    pub status_route: &'static str,
    pub cancel_route: &'static str,
    pub job_id_format: &'static str,
    pub admission_scope: &'static str,
    pub mixed_version_policy: &'static str,
}

impl ManualTransitionJobCapabilities {
    fn current() -> Self {
        let run = admin_route_capability(HttpMethod::Post, MANUAL_TRANSITION_RUN_ROUTE);
        let status = admin_route_capability(HttpMethod::Get, MANUAL_TRANSITION_JOB_ROUTE);
        let cancel = admin_route_capability(HttpMethod::Delete, MANUAL_TRANSITION_JOB_ROUTE);
        let supported = run.state == CapabilityState::Supported
            && status.state == CapabilityState::Supported
            && cancel.state == CapabilityState::Supported;
        Self {
            contract_version: 1,
            status: if supported {
                CapabilityStatus::supported().with_reason("durable manual transition job routes are registered")
            } else {
                CapabilityStatus::unsupported().with_reason("one or more durable manual transition job routes are unavailable")
            },
            modes: ["enqueue_only", "async"],
            run_route: MANUAL_TRANSITION_RUN_ROUTE,
            status_route: MANUAL_TRANSITION_JOB_ROUTE,
            cancel_route: MANUAL_TRANSITION_JOB_ROUTE,
            job_id_format: "uuid",
            admission_scope: "bucket",
            mixed_version_policy: "fail_closed_when_capability_unknown_or_unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SiteReplicationRepairCapabilities {
    pub contract_version: u32,
    pub status: CapabilityStatus,
    pub modes: [&'static str; 2],
    pub execute_route: &'static str,
    pub status_route: &'static str,
    pub preflight_token_contract: &'static str,
    pub operation_id_format: &'static str,
    pub max_retained_successful_operations: usize,
    pub disabled_by_default: bool,
}

impl SiteReplicationRepairCapabilities {
    fn current() -> Self {
        let execute = admin_route_capability(HttpMethod::Put, SITE_REPLICATION_REPAIR_ROUTE);
        let status = admin_route_capability(HttpMethod::Get, SITE_REPLICATION_REPAIR_STATUS_ROUTE);
        let supported = execute.state == CapabilityState::Supported && status.state == CapabilityState::Supported;
        Self {
            contract_version: 1,
            status: if supported {
                CapabilityStatus::supported().with_reason("durable dry-run, execute, and status contracts are registered")
            } else {
                CapabilityStatus::unsupported().with_reason("one or more durable repair routes are unavailable")
            },
            modes: ["dry-run", "execute"],
            execute_route: SITE_REPLICATION_REPAIR_ROUTE,
            status_route: SITE_REPLICATION_REPAIR_STATUS_ROUTE,
            preflight_token_contract: "hmac-sha256-v1",
            operation_id_format: "uuid",
            max_retained_successful_operations: 32,
            disabled_by_default: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticProbeCapabilities {
    pub contract_version: u32,
    pub health_info: DiagnosticProbeCapability,
    pub inspect_archive: DiagnosticProbeCapability,
    pub observed_drive_metrics: DiagnosticProbeCapability,
    pub client_devnull: DiagnosticProbeCapability,
    pub object_speedtest: DiagnosticProbeCapability,
    pub inter_node_netperf: DiagnosticProbeCapability,
    pub site_speedtest: DiagnosticProbeCapability,
    pub site_replication_netperf: DiagnosticProbeCapability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiagnosticProbeCapability {
    pub status: CapabilityStatus,
    pub mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_duration_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<usize>,
}

impl DiagnosticProbeCapabilities {
    fn current() -> Self {
        let unsupported = |reason, route| DiagnosticProbeCapability {
            status: CapabilityStatus::unsupported().with_reason(reason),
            mode: "unsupported",
            route,
            max_bytes: None,
            max_duration_secs: None,
            max_concurrency: None,
        };

        Self {
            contract_version: 1,
            health_info: DiagnosticProbeCapability {
                status: CapabilityStatus::supported().with_reason("reports local host telemetry and observed storage state"),
                mode: "system_observation",
                route: Some("/rustfs/admin/v3/healthinfo"),
                max_bytes: None,
                max_duration_secs: None,
                max_concurrency: None,
            },
            inspect_archive: unsupported(
                "a bounded encrypted diagnostic archive is not implemented; v3 inspect-data returns object bytes",
                None,
            ),
            observed_drive_metrics: DiagnosticProbeCapability {
                status: CapabilityStatus::supported()
                    .with_reason("reports observed StorageInfo throughput and latency, not an active benchmark"),
                mode: "observed_storage_metrics",
                route: Some("/rustfs/admin/v3/speedtest/drive"),
                max_bytes: None,
                max_duration_secs: None,
                max_concurrency: None,
            },
            client_devnull: DiagnosticProbeCapability {
                status: CapabilityStatus::supported().with_reason("actively drains a bounded client upload"),
                mode: "active_upload_drain",
                route: Some("/rustfs/admin/v3/speedtest/client/devnull"),
                max_bytes: Some(super::diagnostics::CLIENT_DEVNULL_MAX_BYTES),
                max_duration_secs: Some(super::diagnostics::CLIENT_DEVNULL_MAX_DURATION.as_secs()),
                max_concurrency: Some(super::diagnostics::CLIENT_DEVNULL_MAX_CONCURRENCY),
            },
            object_speedtest: unsupported(
                "object PUT/GET benchmark harness is not implemented",
                Some("/rustfs/admin/v3/speedtest/object"),
            ),
            inter_node_netperf: unsupported(
                "inter-node traffic benchmark harness is not implemented",
                Some("/rustfs/admin/v3/speedtest/net"),
            ),
            site_speedtest: unsupported(
                "site traffic benchmark harness is not implemented",
                Some("/rustfs/admin/v3/speedtest/site"),
            ),
            site_replication_netperf: DiagnosticProbeCapability {
                status: CapabilityStatus::unsupported().with_reason("site-replication netperf does not perform peer traffic"),
                mode: "unsupported",
                route: Some("/rustfs/admin/v3/site-replication/netperf"),
                max_bytes: None,
                max_duration_secs: None,
                max_concurrency: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageClassCapabilities {
    pub contract_version: u32,
    pub supported_write_classes: [&'static str; 2],
    pub unsupported_write_error: &'static str,
    pub legacy_label_behavior: &'static str,
}

impl StorageClassCapabilities {
    fn current() -> Self {
        Self {
            contract_version: storage_class_contract::CAPABILITY_CONTRACT_VERSION,
            supported_write_classes: storage_class_contract::SUPPORTED_WRITE_CLASSES,
            unsupported_write_error: storage_class_contract::UNSUPPORTED_WRITE_ERROR,
            legacy_label_behavior: storage_class_contract::LEGACY_LABEL_BEHAVIOR,
        }
    }
}

pub struct RuntimeCapabilitiesHandler {}

pub(crate) async fn build_runtime_capabilities_response()
-> Result<RuntimeCapabilitiesResponse, crate::admin::storage_api::cluster::CapabilitySnapshotError> {
    let usecase = default_admin_usecase();
    let observability_provider = RustFsObservabilitySnapshotProvider;
    let observability = observability_provider.observability_snapshot().await?;
    let workload_admission = workload_admission_registry_snapshot();
    let cluster_snapshot_discovery = cluster_snapshot::build_cluster_snapshot_discovery_response().await;
    let local_drive_count = crate::storage::all_local_disk().await.len();

    let (topology, topology_status) = if let Some(endpoint_pools) = current_endpoints_handle() {
        let topology_provider = EndpointTopologySnapshotProvider::new(endpoint_pools);
        let topology = topology_provider.topology_snapshot().await?;
        (Some(topology), CapabilityStatus::supported())
    } else {
        (None, CapabilityStatus::unknown().with_reason(TOPOLOGY_SNAPSHOT_NOT_AVAILABLE))
    };
    let summary = build_runtime_capabilities_summary(
        &observability,
        topology.as_ref(),
        &topology_status,
        cluster_snapshot_discovery.summary.as_ref(),
    );

    Ok(RuntimeCapabilitiesResponse {
        summary,
        replication: ReplicationCapabilities::current(),
        manual_transition_jobs: ManualTransitionJobCapabilities::current(),
        diagnostic_probes: DiagnosticProbeCapabilities::current(),
        inspect_archive: super::inspect_archive::InspectArchiveCapability::current(local_drive_count),
        storage_classes: StorageClassCapabilities::current(),
        site_replication_repair: SiteReplicationRepairCapabilities::current(),
        cluster_snapshot_path: usecase.cluster_snapshot_route().to_string(),
        cluster_snapshot_summary: cluster_snapshot_discovery.summary,
        observability,
        workload_admission,
        topology,
        topology_status,
    })
}

fn build_runtime_capabilities_summary(
    observability: &crate::admin::storage_api::cluster::ObservabilitySnapshot,
    topology: Option<&TopologySnapshot>,
    topology_status: &CapabilityStatus,
    cluster_snapshot_summary: Option<&CapabilityStatus>,
) -> RuntimeCapabilitiesSummary {
    let userspace_profiling = summarize_named_capability_statuses(
        [
            ("cpu", &observability.userspace_profiling.cpu),
            ("memory", &observability.userspace_profiling.memory),
            ("continuous_cpu", &observability.userspace_profiling.continuous_cpu),
            ("periodic_cpu", &observability.userspace_profiling.periodic_cpu),
        ],
        "userspace profiling",
    );
    let memory_sampling = summarize_named_capability_statuses(
        [
            ("process", &observability.memory_sampling.process),
            ("system", &observability.memory_sampling.system),
            ("cgroup", &observability.memory_sampling.cgroup),
        ],
        "memory sampling",
    );
    let platform = summarize_named_capability_statuses(
        [
            ("allocator", &observability.platform.allocator),
            ("ebpf", &observability.platform.ebpf),
            ("numa", &observability.platform.numa),
        ],
        "platform support",
    );
    let observability_status = if [userspace_profiling.state, memory_sampling.state, platform.state]
        .into_iter()
        .any(|state| state == CapabilityState::Unknown)
    {
        CapabilityStatus::unknown().with_reason(OBSERVABILITY_SUMMARY_RESOLVED)
    } else {
        CapabilityStatus::supported().with_reason(OBSERVABILITY_SUMMARY_RESOLVED)
    };
    let topology_summary = topology.map_or_else(
        || topology_status.clone(),
        |topology| {
            summarize_named_capability_statuses(
                [
                    ("profiling", &topology.capabilities.profiling),
                    ("numa", &topology.capabilities.numa),
                    ("failure_domain_labels", &topology.capabilities.failure_domain_labels),
                    ("media_labels", &topology.capabilities.media_labels),
                ],
                "topology capability",
            )
            .with_reason(TOPOLOGY_SUMMARY_RESOLVED)
        },
    );

    RuntimeCapabilitiesSummary {
        observability: observability_status,
        userspace_profiling,
        memory_sampling,
        platform,
        topology: topology_summary,
        cluster_snapshot: cluster_snapshot_summary.cloned().unwrap_or_else(|| {
            CapabilityStatus::unknown().with_reason("cluster snapshot is not available before storage endpoint pools initialize")
        }),
        site_replication_info: admin_route_capability(HttpMethod::Get, SITE_REPLICATION_INFO_ROUTE),
        site_replication_edit: admin_route_capability(HttpMethod::Put, SITE_REPLICATION_EDIT_ROUTE),
        site_replication_resync: admin_route_capability(HttpMethod::Put, SITE_REPLICATION_RESYNC_ROUTE),
        site_replication_repair: SiteReplicationRepairCapabilities::current().status,
        manual_transition_jobs: ManualTransitionJobCapabilities::current().status,
    }
}

fn admin_route_capability(method: HttpMethod, path: &str) -> CapabilityStatus {
    admin_route_capability_from_inventory(method, path, ADMIN_ROUTE_POLICY_SPECS, DEFERRED_ADMIN_ROUTE_POLICIES)
}

fn admin_route_capability_from_inventory(
    method: HttpMethod,
    path: &str,
    direct: &[AdminRouteSpec],
    deferred: &[DeferredAdminRoutePolicy],
) -> CapabilityStatus {
    if direct.iter().any(|spec| spec.method() == method && spec.path() == path) {
        return CapabilityStatus::supported().with_reason("public admin route is registered with an implemented handler");
    }

    match deferred
        .iter()
        .find(|policy| policy.method() == method && policy.path() == path)
        .map(|policy| policy.reason())
    {
        Some(DeferredRoutePolicyReason::NotImplemented) | None => {
            CapabilityStatus::unsupported().with_reason("public admin route is absent or not implemented")
        }
        Some(_) => CapabilityStatus::supported().with_reason("public admin route is registered with contextual authorization"),
    }
}

fn runtime_capabilities_gate_actions() -> Vec<Action> {
    vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)]
}

fn summarize_named_capability_statuses<const N: usize>(
    statuses: [(&'static str, &CapabilityStatus); N],
    subject: &'static str,
) -> CapabilityStatus {
    let unknown = statuses
        .iter()
        .filter_map(|(name, status)| (status.state == CapabilityState::Unknown).then_some(*name))
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return CapabilityStatus::unknown().with_reason(format!("{subject} unresolved fields: {}", unknown.join(", ")));
    }

    let supported_or_disabled = statuses
        .iter()
        .any(|(_, status)| matches!(status.state, CapabilityState::Supported | CapabilityState::Disabled));
    if supported_or_disabled {
        return CapabilityStatus::supported();
    }

    let unsupported = statuses.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    CapabilityStatus::unsupported().with_reason(format!("{subject} unsupported fields: {}", unsupported.join(", ")))
}

#[async_trait::async_trait]
impl Operation for RuntimeCapabilitiesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            log_system_request_rejected!("runtime_capabilities", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(&req.headers, &cred, owner, false, runtime_capabilities_gate_actions(), remote_addr).await?;

        let response = build_runtime_capabilities_response().await.map_err(|err| {
            log_system_request_failed!("runtime_capabilities", "build_runtime_capabilities_failed", err);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to build runtime capabilities snapshot")
        })?;

        let data = serde_json::to_vec(&response).map_err(|e| {
            log_system_request_failed!("runtime_capabilities", "serialize_runtime_capabilities_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "failed to serialize runtime capabilities snapshot")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("runtime_capabilities");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

/// Authorization gate for GET datausageinfo (and prefix usage): any-of the
/// dedicated admin action OR the bucket listing action. Pinned by a unit test
/// so the gate cannot silently narrow or widen (rustfs/backlog#1306).
pub(crate) fn data_usage_info_gate_actions() -> Vec<Action> {
    vec![
        Action::AdminAction(AdminAction::DataUsageInfoAdminAction),
        Action::S3Action(S3Action::ListBucketAction),
    ]
}

#[async_trait::async_trait]
impl Operation for DataUsageInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            log_system_request_rejected!("query_data_usage_info", "missing_credentials");
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        validate_admin_request(&req.headers, &cred, owner, false, data_usage_info_gate_actions(), remote_addr).await?;

        let usecase = default_admin_usecase();
        let info = usecase.execute_query_data_usage_info().await.map_err(S3Error::from)?;

        let data = serde_json::to_vec(&info).map_err(|e| {
            log_system_request_failed!("query_data_usage_info", "serialize_data_usage_info_failed", e);
            S3Error::with_message(S3ErrorCode::InternalError, "parse DataUsageInfo failed")
        })?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        log_system_response_emitted!("query_data_usage_info");

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MANUAL_TRANSITION_JOB_ROUTE, MANUAL_TRANSITION_RUN_ROUTE, OBSERVABILITY_SUMMARY_RESOLVED, RuntimeCapabilitiesHandler,
        SITE_REPLICATION_EDIT_ROUTE, SITE_REPLICATION_INFO_ROUTE, SITE_REPLICATION_REPAIR_ROUTE,
        SITE_REPLICATION_REPAIR_STATUS_ROUTE, SITE_REPLICATION_RESYNC_ROUTE, ServerInfoResponse, TOPOLOGY_SNAPSHOT_NOT_AVAILABLE,
        TOPOLOGY_SUMMARY_RESOLVED, admin_route_capability_from_inventory, build_runtime_capabilities_response,
        build_runtime_capabilities_summary, data_usage_info_gate_actions, runtime_capabilities_gate_actions,
        system_admin_discovery,
    };
    use crate::admin::router::Operation;
    use crate::admin::runtime_sources::DefaultAdminUsecase;
    use crate::admin::storage_api::cluster::{
        CapabilityState, CapabilityStatus, MemorySamplingState, ObservabilitySnapshot, PlatformSupport, TopologyCapabilities,
        TopologySnapshot, UserspaceProfilingCapability,
    };
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
    use rustfs_concurrency::WorkloadClass;
    use rustfs_madmin::{InfoMessage, StorageInfo};
    use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
    use rustfs_security_governance::HttpMethod;
    use s3s::{Body, S3ErrorCode, S3Request};
    use serde_json::json;

    /// Authz regression pin (rustfs/backlog#1306): datausageinfo stays an
    /// any-of gate over exactly DataUsageInfoAdminAction OR ListBucketAction.
    /// The OR semantics of the multi-action loop are covered by the
    /// `evaluate_admin_actions` tests in `crate::admin::auth`.
    #[test]
    fn data_usage_info_gate_keeps_dual_action_or_semantics() {
        assert_eq!(
            data_usage_info_gate_actions(),
            vec![
                Action::AdminAction(AdminAction::DataUsageInfoAdminAction),
                Action::S3Action(S3Action::ListBucketAction),
            ]
        );
    }

    #[tokio::test]
    async fn runtime_capabilities_response_reports_missing_topology_before_storage_init() {
        let response = build_runtime_capabilities_response()
            .await
            .expect("runtime capabilities response should build");

        assert_eq!(response.topology, None);
        assert_eq!(response.topology_status.state, CapabilityState::Unknown);
        assert_eq!(response.topology_status.reason.as_deref(), Some(TOPOLOGY_SNAPSHOT_NOT_AVAILABLE));
        assert_eq!(response.summary.topology.state, CapabilityState::Unknown);
        assert_eq!(response.summary.topology.reason.as_deref(), Some(TOPOLOGY_SNAPSHOT_NOT_AVAILABLE));
        assert_eq!(response.cluster_snapshot_path, "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(response.cluster_snapshot_summary, None);
        assert_eq!(response.summary.cluster_snapshot.state, CapabilityState::Unknown);
        assert_eq!(response.observability.platform.os.as_deref(), Some(std::env::consts::OS));
        assert_eq!(response.workload_admission.entries().len(), WorkloadClass::REQUIRED.len());
        assert_eq!(response.summary.site_replication_info.state, CapabilityState::Supported);
        assert_eq!(response.summary.site_replication_edit.state, CapabilityState::Supported);
        assert_eq!(response.summary.site_replication_resync.state, CapabilityState::Supported);
        assert_eq!(response.summary.site_replication_repair.state, CapabilityState::Supported);
        assert_eq!(response.summary.manual_transition_jobs.state, CapabilityState::Supported);
        assert_eq!(response.replication.contract_version, 1);
        assert_eq!(response.replication.bucket_replication.contract_version, 1);
        // v2: disableProxy moved from unsupported to writable (per-target
        // read-proxy opt-out reached the admin API).
        assert_eq!(response.replication.remote_targets.contract_version, 2);
        assert_eq!(response.replication.bucket_replication.status.state, CapabilityState::Supported);
        assert_eq!(response.replication.remote_targets.status.state, CapabilityState::Supported);
        assert_eq!(
            response.replication.mixed_version_policy,
            "fail_closed_when_capability_unknown_or_unsupported"
        );
        assert!(
            response
                .replication
                .bucket_replication
                .fields
                .iter()
                .any(|field| field.name == "Rule.DeleteReplication.Status"
                    && field.state == super::ReplicationFieldState::Supported)
        );
        assert!(
            response
                .replication
                .bucket_replication
                .fields
                .iter()
                .any(|field| field.name == "Destination.EncryptionConfiguration"
                    && field.state == super::ReplicationFieldState::ReadOnlyHistorical)
        );
        assert!(
            response
                .replication
                .remote_targets
                .fields
                .iter()
                .any(|field| field.name == "disableProxy" && field.state == super::ReplicationFieldState::Supported)
        );
        assert!(
            response
                .replication
                .remote_targets
                .fields
                .iter()
                .any(|field| field.name == "edge" && field.state == super::ReplicationFieldState::Unsupported)
        );
        assert!(
            response
                .replication
                .remote_targets
                .fields
                .iter()
                .any(|field| field.name == "healthCheckDuration" && field.state == super::ReplicationFieldState::Supported)
        );
        assert_eq!(response.manual_transition_jobs.contract_version, 1);
        assert_eq!(response.manual_transition_jobs.status.state, CapabilityState::Supported);
        assert_eq!(response.manual_transition_jobs.modes, ["enqueue_only", "async"]);
        assert_eq!(response.manual_transition_jobs.run_route, MANUAL_TRANSITION_RUN_ROUTE);
        assert_eq!(response.manual_transition_jobs.status_route, MANUAL_TRANSITION_JOB_ROUTE);
        assert_eq!(response.manual_transition_jobs.cancel_route, MANUAL_TRANSITION_JOB_ROUTE);
        assert_eq!(response.manual_transition_jobs.job_id_format, "uuid");
        assert_eq!(response.manual_transition_jobs.admission_scope, "bucket");
        assert_eq!(
            response.manual_transition_jobs.mixed_version_policy,
            "fail_closed_when_capability_unknown_or_unsupported"
        );
        assert_eq!(response.site_replication_repair.contract_version, 1);
        assert_eq!(response.site_replication_repair.status.state, CapabilityState::Supported);
        assert_eq!(response.site_replication_repair.modes, ["dry-run", "execute"]);
        assert_eq!(response.site_replication_repair.execute_route, SITE_REPLICATION_REPAIR_ROUTE);
        assert_eq!(response.site_replication_repair.status_route, SITE_REPLICATION_REPAIR_STATUS_ROUTE);
        assert_eq!(response.site_replication_repair.preflight_token_contract, "hmac-sha256-v1");
        assert_eq!(response.site_replication_repair.operation_id_format, "uuid");
        assert_eq!(response.site_replication_repair.max_retained_successful_operations, 32);
        assert!(!response.site_replication_repair.disabled_by_default);
        assert_eq!(response.diagnostic_probes.contract_version, 1);
        assert_eq!(response.diagnostic_probes.health_info.status.state, CapabilityState::Supported);
        assert_eq!(response.diagnostic_probes.observed_drive_metrics.mode, "observed_storage_metrics");
        assert_eq!(response.diagnostic_probes.client_devnull.status.state, CapabilityState::Supported);
        assert_eq!(
            response.diagnostic_probes.client_devnull.max_bytes,
            Some(super::super::diagnostics::CLIENT_DEVNULL_MAX_BYTES)
        );
        assert_eq!(
            response.diagnostic_probes.client_devnull.max_duration_secs,
            Some(super::super::diagnostics::CLIENT_DEVNULL_MAX_DURATION.as_secs())
        );
        assert_eq!(
            response.diagnostic_probes.client_devnull.max_concurrency,
            Some(super::super::diagnostics::CLIENT_DEVNULL_MAX_CONCURRENCY)
        );
        for probe in [
            &response.diagnostic_probes.inspect_archive,
            &response.diagnostic_probes.object_speedtest,
            &response.diagnostic_probes.inter_node_netperf,
            &response.diagnostic_probes.site_speedtest,
            &response.diagnostic_probes.site_replication_netperf,
        ] {
            assert_eq!(probe.status.state, CapabilityState::Unsupported);
            assert_eq!(probe.mode, "unsupported");
            assert!(probe.status.reason.is_some());
        }
        assert_eq!(response.storage_classes.contract_version, 1);
        assert_eq!(response.storage_classes.supported_write_classes, ["STANDARD", "REDUCED_REDUNDANCY"]);
        assert_eq!(response.storage_classes.unsupported_write_error, "InvalidStorageClass");
        assert_eq!(response.storage_classes.legacy_label_behavior, "normalized_to_effective_class");

        let value = serde_json::to_value(response).expect("runtime capability response should serialize");
        assert_eq!(value["summary"]["site_replication_info"]["state"], "supported");
        assert_eq!(value["summary"]["site_replication_edit"]["state"], "supported");
        assert_eq!(value["summary"]["site_replication_resync"]["state"], "supported");
        assert_eq!(value["summary"]["site_replication_repair"]["state"], "supported");
        assert_eq!(value["summary"]["manual_transition_jobs"]["state"], "supported");
        assert_eq!(value["replication"]["contract_version"], 1);
        assert_eq!(value["replication"]["bucket_replication"]["contract_version"], 1);
        assert_eq!(value["replication"]["remote_targets"]["contract_version"], 2);
        assert_eq!(value["replication"]["bucket_replication"]["status"]["state"], "supported");
        assert_eq!(value["replication"]["remote_targets"]["status"]["state"], "supported");
        assert_eq!(
            value["replication"]["mixed_version_policy"],
            "fail_closed_when_capability_unknown_or_unsupported"
        );
        assert!(
            value["replication"]["bucket_replication"]["fields"]
                .as_array()
                .expect("bucket replication fields should be an array")
                .iter()
                .any(|field| field["name"] == "Destination.EncryptionConfiguration" && field["state"] == "read_only_historical")
        );
        assert!(
            value["replication"]["remote_targets"]["fields"]
                .as_array()
                .expect("remote target fields should be an array")
                .iter()
                .any(|field| field["name"] == "disableProxy" && field["state"] == "supported")
        );
        assert!(
            value["replication"]["remote_targets"]["fields"]
                .as_array()
                .expect("remote target fields should be an array")
                .iter()
                .any(|field| field["name"] == "edge" && field["state"] == "unsupported")
        );
        assert!(
            value["replication"]["remote_targets"]["fields"]
                .as_array()
                .expect("remote target fields should be an array")
                .iter()
                .any(|field| field["name"] == "healthCheckDuration" && field["state"] == "supported")
        );
        assert_eq!(value["manual_transition_jobs"]["contract_version"], 1);
        assert_eq!(value["manual_transition_jobs"]["status"]["state"], "supported");
        assert_eq!(value["manual_transition_jobs"]["modes"], json!(["enqueue_only", "async"]));
        assert_eq!(value["manual_transition_jobs"]["run_route"], MANUAL_TRANSITION_RUN_ROUTE);
        assert_eq!(value["manual_transition_jobs"]["status_route"], MANUAL_TRANSITION_JOB_ROUTE);
        assert_eq!(value["manual_transition_jobs"]["cancel_route"], MANUAL_TRANSITION_JOB_ROUTE);
        assert_eq!(value["manual_transition_jobs"]["job_id_format"], "uuid");
        assert_eq!(value["manual_transition_jobs"]["admission_scope"], "bucket");
        assert_eq!(
            value["manual_transition_jobs"]["mixed_version_policy"],
            "fail_closed_when_capability_unknown_or_unsupported"
        );
        assert_eq!(value["site_replication_repair"]["contract_version"], 1);
        assert_eq!(value["site_replication_repair"]["status"]["state"], "supported");
        assert_eq!(value["site_replication_repair"]["modes"], json!(["dry-run", "execute"]));
        assert_eq!(value["site_replication_repair"]["execute_route"], SITE_REPLICATION_REPAIR_ROUTE);
        assert_eq!(value["site_replication_repair"]["status_route"], SITE_REPLICATION_REPAIR_STATUS_ROUTE);
        assert_eq!(value["site_replication_repair"]["max_retained_successful_operations"], 32);
        assert_eq!(value["site_replication_repair"]["disabled_by_default"], false);
        assert_eq!(value["diagnostic_probes"]["client_devnull"]["status"]["state"], "supported");
        assert_eq!(
            value["diagnostic_probes"]["client_devnull"]["max_bytes"],
            super::super::diagnostics::CLIENT_DEVNULL_MAX_BYTES
        );
        assert_eq!(
            value["diagnostic_probes"]["client_devnull"]["max_concurrency"],
            super::super::diagnostics::CLIENT_DEVNULL_MAX_CONCURRENCY
        );
        assert_eq!(value["diagnostic_probes"]["site_replication_netperf"]["status"]["state"], "unsupported");
        assert_eq!(value["storage_classes"]["contract_version"], 1);
        assert_eq!(
            value["storage_classes"]["supported_write_classes"],
            json!(["STANDARD", "REDUCED_REDUNDANCY"])
        );
        assert_eq!(value["storage_classes"]["unsupported_write_error"], "InvalidStorageClass");
        assert_eq!(value["storage_classes"]["legacy_label_behavior"], "normalized_to_effective_class");
    }

    #[test]
    fn site_replication_capabilities_follow_public_route_inventory() {
        for (method, path) in [
            (HttpMethod::Get, SITE_REPLICATION_INFO_ROUTE),
            (HttpMethod::Put, SITE_REPLICATION_EDIT_ROUTE),
            (HttpMethod::Put, SITE_REPLICATION_RESYNC_ROUTE),
            (HttpMethod::Put, SITE_REPLICATION_REPAIR_ROUTE),
            (HttpMethod::Get, SITE_REPLICATION_REPAIR_STATUS_ROUTE),
        ] {
            assert_eq!(
                admin_route_capability_from_inventory(
                    method,
                    path,
                    crate::admin::route_policy::ADMIN_ROUTE_POLICY_SPECS,
                    crate::admin::route_policy::DEFERRED_ADMIN_ROUTE_POLICIES,
                )
                .state,
                CapabilityState::Supported,
            );
        }
    }

    #[test]
    fn manual_transition_job_capabilities_follow_public_route_inventory() {
        for (method, path) in [
            (HttpMethod::Post, MANUAL_TRANSITION_RUN_ROUTE),
            (HttpMethod::Get, MANUAL_TRANSITION_JOB_ROUTE),
            (HttpMethod::Delete, MANUAL_TRANSITION_JOB_ROUTE),
        ] {
            assert_eq!(
                admin_route_capability_from_inventory(
                    method,
                    path,
                    crate::admin::route_policy::ADMIN_ROUTE_POLICY_SPECS,
                    crate::admin::route_policy::DEFERRED_ADMIN_ROUTE_POLICIES,
                )
                .state,
                CapabilityState::Supported,
            );
        }
    }

    #[test]
    fn site_replication_capabilities_reject_absent_internal_only_and_not_implemented_routes() {
        use crate::admin::route_policy::{DeferredAdminRoutePolicy, DeferredRoutePolicyReason};

        let not_implemented = [DeferredAdminRoutePolicy::new(
            HttpMethod::Put,
            SITE_REPLICATION_EDIT_ROUTE,
            DeferredRoutePolicyReason::NotImplemented,
        )];
        assert_eq!(
            admin_route_capability_from_inventory(HttpMethod::Put, SITE_REPLICATION_EDIT_ROUTE, &[], &not_implemented).state,
            CapabilityState::Unsupported,
        );
        assert_eq!(
            admin_route_capability_from_inventory(HttpMethod::Put, SITE_REPLICATION_EDIT_ROUTE, &[], &[]).state,
            CapabilityState::Unsupported,
        );

        let internal_only = crate::admin::route_policy::ADMIN_ROUTE_POLICY_SPECS
            .iter()
            .copied()
            .filter(|spec| spec.path() == "/rustfs/admin/v3/site-replication/peer/edit-capabilities")
            .collect::<Vec<_>>();
        assert_eq!(
            admin_route_capability_from_inventory(HttpMethod::Put, SITE_REPLICATION_EDIT_ROUTE, &internal_only, &[]).state,
            CapabilityState::Unsupported,
        );
    }

    #[test]
    fn older_runtime_summary_defaults_site_replication_capabilities_to_unknown() {
        let summary: super::RuntimeCapabilitiesSummary = serde_json::from_value(json!({
            "observability": { "state": "supported" },
            "userspace_profiling": { "state": "supported" },
            "memory_sampling": { "state": "supported" },
            "platform": { "state": "supported" },
            "topology": { "state": "supported" },
            "cluster_snapshot": { "state": "supported" }
        }))
        .expect("older runtime summary should remain compatible");

        assert_eq!(summary.site_replication_info.state, CapabilityState::Unknown);
        assert_eq!(summary.site_replication_edit.state, CapabilityState::Unknown);
        assert_eq!(summary.site_replication_resync.state, CapabilityState::Unknown);
        assert_eq!(summary.site_replication_repair.state, CapabilityState::Unknown);
        assert_eq!(summary.manual_transition_jobs.state, CapabilityState::Unknown);
    }

    #[tokio::test]
    async fn runtime_capabilities_authentication_failure_is_not_reported_as_unsupported() {
        let request = S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static("/rustfs/admin/v4/runtime/capabilities"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let error = RuntimeCapabilitiesHandler {}
            .call(request, Params::new())
            .await
            .expect_err("runtime capabilities must reject unauthenticated requests before capability resolution");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    /// `ServerInfoHandler` must answer an authorized admin request with the
    /// per-pool erasure-set topology (rustfs/backlog#1839). That map is only
    /// filled when the server-info query is issued with pools included, so a
    /// handler that stopped asking for them would still return 200 with an
    /// empty `pools` object instead of failing.
    #[tokio::test]
    #[serial_test::serial]
    async fn server_info_response_carries_pool_topology() {
        use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
        use crate::admin::storage_api::runtime::bootstrap_ctx;
        use http_body_util::BodyExt as _;
        use rustfs_iam::store::{Store as _, object::IAM_CONFIG_PREFIX};
        use std::sync::Arc;

        const ROOT_ACCESS_KEY: &str = "SERVERINFOROOTACCESSKEY";
        const ROOT_SECRET_KEY: &str = "serverInfoRootSecret123";

        let _ = rustfs_credentials::init_global_action_credentials(
            Some(ROOT_ACCESS_KEY.to_string()),
            Some(ROOT_SECRET_KEY.to_string()),
        );

        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .prefix("admin_server_info_pools")
            .disk_count(1)
            .init_bucket_metadata(false)
            .build()
            .await;
        // Server startup owns this write in production; the test bootstrap
        // stops short of it, and without a topology the server-info query
        // returns before it ever looks at drives.
        bootstrap_ctx().set_endpoints(env.endpoint_pools.clone());
        rustfs_iam::store::object::ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build test IAM");
        publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
            Arc::clone(&env.ecstore),
            iam,
            Arc::new(rustfs_kms::KmsServiceManager::new()),
        )));

        let request = S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static("/rustfs/admin/v3/info"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: ROOT_ACCESS_KEY.to_string(),
                secret_key: s3s::auth::SecretKey::from(ROOT_SECRET_KEY.to_string()),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        };

        let (status, body) = super::ServerInfoHandler {}
            .call(request, Params::new())
            .await
            .expect("root admin credentials must be served server info")
            .output;
        assert_eq!(status, hyper::StatusCode::OK);

        let bytes = body.collect().await.expect("server info body should read").to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("server info must be json");
        let pools = payload["info"]["pools"]
            .as_object()
            .expect("server info must carry a pools object");
        assert!(
            pools.contains_key("0"),
            "server info must report the erasure-set topology of pool 0, got {pools:?}"
        );
    }

    /// Authorization denial for this exact action is pinned to AccessDenied by
    /// `crate::admin::auth::tests::non_admin_credential_is_denied`.
    #[test]
    fn runtime_capabilities_gate_requires_server_info_authorization() {
        assert_eq!(
            runtime_capabilities_gate_actions(),
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)]
        );
    }

    /// The startup bitrot self-test outcome must surface in server info as one
    /// of three closed-set strings, never an internal enum or a null
    /// (rustfs/backlog#1873). This test pins the string mapping; whether the
    /// process-global cell holds Some(true)/Some(false)/None is owned by
    /// `crate::bitrot_selftest`'s own tests.
    #[test]
    fn bitrot_selftest_status_str_is_a_closed_set_of_operators_strings() {
        let rendered = super::bitrot_selftest_status_str();
        assert!(matches!(rendered, "passed" | "failed" | "unknown"));
        assert_eq!(super::bitrot_selftest_status_str(), rendered);
    }

    #[test]
    fn server_info_response_exposes_admin_discovery_paths() {
        let usecase = DefaultAdminUsecase::without_context();
        let response = ServerInfoResponse {
            info: InfoMessage {
                mode: None,
                domain: None,
                region: None,
                sqs_arn: None,
                deployment_id: None,
                buckets: None,
                objects: None,
                versions: None,
                delete_markers: None,
                usage: None,
                services: None,
                backend: None,
                servers: None,
                pools: None,
            },
            admin_discovery: system_admin_discovery(&usecase),
            bitrot_selftest: super::bitrot_selftest_status_str(),
        };

        let value = serde_json::to_value(response).expect("server info response should serialize");
        assert_eq!(value["admin_discovery"]["runtimeCapabilities"], "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(value["admin_discovery"]["clusterSnapshot"], "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(value["admin_discovery"]["extensionsCatalog"], "/rustfs/admin/v4/extensions/catalog");
    }

    #[test]
    fn storage_info_response_exposes_admin_discovery_paths() {
        let usecase = DefaultAdminUsecase::without_context();
        let response = super::StorageInfoResponse {
            info: StorageInfo::default(),
            admin_discovery: system_admin_discovery(&usecase),
        };

        let value = serde_json::to_value(response).expect("storage info response should serialize");
        assert_eq!(value["admin_discovery"]["runtimeCapabilities"], "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(value["admin_discovery"]["clusterSnapshot"], "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(value["admin_discovery"]["extensionsCatalog"], "/rustfs/admin/v4/extensions/catalog");
    }

    #[test]
    fn runtime_capabilities_summary_marks_observability_supported_when_groups_are_resolved() {
        let observability = ObservabilitySnapshot {
            runtime_telemetry: CapabilityStatus::supported(),
            userspace_profiling: UserspaceProfilingCapability {
                cpu: CapabilityStatus::supported(),
                memory: CapabilityStatus::unsupported().with_reason("platform"),
                continuous_cpu: CapabilityStatus::supported(),
                periodic_cpu: CapabilityStatus::disabled(),
            },
            memory_sampling: MemorySamplingState {
                process: CapabilityStatus::supported(),
                system: CapabilityStatus::supported(),
                cgroup: CapabilityStatus::unsupported().with_reason("platform"),
            },
            platform: PlatformSupport {
                target_triple: Some("x86_64-unknown-linux-gnu".to_owned()),
                os: Some("linux".to_owned()),
                arch: Some("x86_64".to_owned()),
                allocator: CapabilityStatus::supported(),
                ebpf: CapabilityStatus::unsupported().with_reason("platform"),
                numa: CapabilityStatus::unsupported().with_reason("platform"),
            },
        };
        let topology = TopologySnapshot {
            pools: Vec::new(),
            capabilities: TopologyCapabilities {
                profiling: CapabilityStatus::supported(),
                numa: CapabilityStatus::unsupported().with_reason("platform"),
                failure_domain_labels: CapabilityStatus::supported(),
                media_labels: CapabilityStatus::unsupported().with_reason("not reported"),
            },
        };

        let summary = build_runtime_capabilities_summary(
            &observability,
            Some(&topology),
            &CapabilityStatus::supported(),
            Some(&CapabilityStatus::supported().with_reason("cluster snapshot is available")),
        );

        assert_eq!(summary.observability.state, CapabilityState::Supported);
        assert_eq!(summary.observability.reason.as_deref(), Some(OBSERVABILITY_SUMMARY_RESOLVED));
        assert_eq!(summary.userspace_profiling.state, CapabilityState::Supported);
        assert_eq!(summary.memory_sampling.state, CapabilityState::Supported);
        assert_eq!(summary.platform.state, CapabilityState::Supported);
        assert_eq!(summary.topology.state, CapabilityState::Supported);
        assert_eq!(summary.topology.reason.as_deref(), Some(TOPOLOGY_SUMMARY_RESOLVED));
        assert_eq!(summary.cluster_snapshot.state, CapabilityState::Supported);
    }

    #[test]
    fn runtime_capabilities_summary_marks_unknown_groups_when_snapshot_is_not_fully_wired() {
        let observability = ObservabilitySnapshot {
            runtime_telemetry: CapabilityStatus::supported(),
            userspace_profiling: UserspaceProfilingCapability {
                cpu: CapabilityStatus::unknown().with_reason("not wired"),
                memory: CapabilityStatus::unsupported().with_reason("platform"),
                continuous_cpu: CapabilityStatus::supported(),
                periodic_cpu: CapabilityStatus::disabled(),
            },
            memory_sampling: MemorySamplingState {
                process: CapabilityStatus::supported(),
                system: CapabilityStatus::supported(),
                cgroup: CapabilityStatus::unknown().with_reason("not wired"),
            },
            platform: PlatformSupport {
                target_triple: Some("x86_64-unknown-linux-gnu".to_owned()),
                os: Some("linux".to_owned()),
                arch: Some("x86_64".to_owned()),
                allocator: CapabilityStatus::supported(),
                ebpf: CapabilityStatus::unknown().with_reason("not wired"),
                numa: CapabilityStatus::unknown().with_reason("not wired"),
            },
        };
        let topology = TopologySnapshot {
            pools: Vec::new(),
            capabilities: TopologyCapabilities {
                profiling: CapabilityStatus::supported(),
                numa: CapabilityStatus::unknown().with_reason("not wired"),
                failure_domain_labels: CapabilityStatus::unknown().with_reason("not reported"),
                media_labels: CapabilityStatus::unknown().with_reason("not reported"),
            },
        };

        let summary = build_runtime_capabilities_summary(
            &observability,
            Some(&topology),
            &CapabilityStatus::supported(),
            Some(&CapabilityStatus::unknown().with_reason("cluster snapshot unresolved")),
        );

        assert_eq!(summary.observability.state, CapabilityState::Unknown);
        assert_eq!(summary.userspace_profiling.state, CapabilityState::Unknown);
        assert_eq!(summary.memory_sampling.state, CapabilityState::Unknown);
        assert_eq!(summary.platform.state, CapabilityState::Unknown);
        assert_eq!(summary.topology.state, CapabilityState::Unknown);
        assert_eq!(summary.cluster_snapshot.state, CapabilityState::Unknown);
        assert!(
            summary
                .userspace_profiling
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("cpu")
        );
        assert!(
            summary
                .topology
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains(TOPOLOGY_SUMMARY_RESOLVED)
        );
    }
}
