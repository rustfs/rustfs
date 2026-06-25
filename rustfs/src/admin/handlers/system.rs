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
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::resolve_endpoints_handle;
use crate::admin::storage_api::{
    CapabilityState, CapabilityStatus, ObservabilitySnapshotProvider, TopologySnapshot, TopologySnapshotProvider,
};
use crate::app::admin_usecase::{DefaultAdminUsecase, QueryServerInfoRequest};
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
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use tracing::{error, info, warn};

const LOG_COMPONENT_ADMIN_API: &str = "admin_api";
const LOG_SUBSYSTEM_SYSTEM_ADMIN: &str = "system_admin";
const EVENT_ADMIN_REQUEST_REJECTED: &str = "admin_request_rejected";
const EVENT_ADMIN_REQUEST_FAILED: &str = "admin_request_failed";
const EVENT_ADMIN_RESPONSE_EMITTED: &str = "admin_response_emitted";
const OBSERVABILITY_SUMMARY_RESOLVED: &str = "observability summary resolved from provider snapshots";
const TOPOLOGY_SUMMARY_RESOLVED: &str = "topology summary resolved from capability snapshot";
const TOPOLOGY_SNAPSHOT_NOT_AVAILABLE: &str = "endpoint topology is not available before storage endpoint pools initialize";
pub(crate) const RUNTIME_CAPABILITIES_ROUTE_SUFFIX: &str = "/v4/runtime/capabilities";

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

#[async_trait::async_trait]
impl Operation for ServiceHandle {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        log_system_request_rejected!("service_handle", "not_implemented");
        Err(s3_error!(NotImplemented))
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

        let usecase = DefaultAdminUsecase::from_global();
        let info = usecase
            .execute_query_server_info(QueryServerInfoRequest { include_pools: true })
            .await
            .map_err(S3Error::from)?
            .info;
        let response = ServerInfoResponse {
            info,
            admin_discovery: system_admin_discovery(&usecase),
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

#[async_trait::async_trait]
impl Operation for InspectDataHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        log_system_request_rejected!("inspect_data", "not_implemented");
        Err(s3_error!(NotImplemented))
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

        let usecase = DefaultAdminUsecase::from_global();
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeCapabilitiesSummary {
    pub observability: CapabilityStatus,
    pub userspace_profiling: CapabilityStatus,
    pub memory_sampling: CapabilityStatus,
    pub platform: CapabilityStatus,
    pub topology: CapabilityStatus,
    pub cluster_snapshot: CapabilityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeCapabilitiesResponse {
    pub summary: RuntimeCapabilitiesSummary,
    pub cluster_snapshot_path: String,
    pub cluster_snapshot_summary: Option<CapabilityStatus>,
    pub observability: crate::admin::storage_api::ObservabilitySnapshot,
    pub workload_admission: WorkloadAdmissionRegistrySnapshot,
    pub topology: Option<TopologySnapshot>,
    pub topology_status: CapabilityStatus,
}

pub struct RuntimeCapabilitiesHandler {}

pub(crate) async fn build_runtime_capabilities_response()
-> Result<RuntimeCapabilitiesResponse, crate::admin::storage_api::CapabilitySnapshotError> {
    let usecase = DefaultAdminUsecase::from_global();
    let observability_provider = RustFsObservabilitySnapshotProvider;
    let observability = observability_provider.observability_snapshot().await?;
    let workload_admission = workload_admission_registry_snapshot();
    let cluster_snapshot_discovery = cluster_snapshot::build_cluster_snapshot_discovery_response().await;

    let (topology, topology_status) = if let Some(endpoint_pools) = resolve_endpoints_handle() {
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
        cluster_snapshot_path: usecase.cluster_snapshot_route().to_string(),
        cluster_snapshot_summary: cluster_snapshot_discovery.summary,
        observability,
        workload_admission,
        topology,
        topology_status,
    })
}

fn build_runtime_capabilities_summary(
    observability: &crate::admin::storage_api::ObservabilitySnapshot,
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
    }
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
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
            remote_addr,
        )
        .await?;

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
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![
                Action::AdminAction(AdminAction::DataUsageInfoAdminAction),
                Action::S3Action(S3Action::ListBucketAction),
            ],
            remote_addr,
        )
        .await?;

        let usecase = DefaultAdminUsecase::from_global();
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
        OBSERVABILITY_SUMMARY_RESOLVED, ServerInfoResponse, TOPOLOGY_SNAPSHOT_NOT_AVAILABLE, TOPOLOGY_SUMMARY_RESOLVED,
        build_runtime_capabilities_response, build_runtime_capabilities_summary, system_admin_discovery,
    };
    use crate::admin::storage_api::{
        CapabilityState, CapabilityStatus, MemorySamplingState, ObservabilitySnapshot, PlatformSupport, TopologyCapabilities,
        TopologySnapshot, UserspaceProfilingCapability,
    };
    use crate::app::admin_usecase::DefaultAdminUsecase;
    use rustfs_concurrency::{AdmissionState, WorkloadClass};
    use rustfs_madmin::{InfoMessage, StorageInfo};

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
        assert_eq!(
            response
                .workload_admission
                .get(WorkloadClass::ForegroundRead)
                .map(|snapshot| snapshot.state),
            Some(AdmissionState::Open)
        );
        assert_eq!(
            response
                .workload_admission
                .get(WorkloadClass::ForegroundWrite)
                .map(|snapshot| snapshot.state),
            Some(AdmissionState::Disabled)
        );
        assert_eq!(response.workload_admission.entries().len(), WorkloadClass::REQUIRED.len());
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
