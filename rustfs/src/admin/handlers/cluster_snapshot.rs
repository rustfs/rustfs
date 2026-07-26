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

use crate::admin::storage_api::cluster::{CapabilityState, CapabilityStatus, ObservabilitySnapshot, TopologySnapshot};
use crate::admin::{
    auth::validate_admin_request,
    router::{AdminOperation, Operation, S3Router},
    runtime_sources::default_admin_usecase,
    storage_api::cluster::{
        ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage, ClusterLocalNodeStorageSnapshot,
        ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth, ClusterPeerHealthSnapshot, ClusterPoolState,
        ClusterPoolStateSnapshot, ClusterRpcBoundarySnapshot, ClusterRpcChannelSnapshot, ClusterRpcPlane, ClusterRpcTransport,
    },
    system,
};
use crate::auth::{check_key_valid, get_session_token};
use crate::cluster_snapshot::{
    ClusterReadOnlySnapshot, ClusterRuntimeReadinessState, ClusterRuntimeStatusSnapshot, cluster_has_actionable_pressure,
};
use crate::server::{ADMIN_PREFIX, ReadinessDegradedReason, RemoteAddr};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_concurrency::AdmissionState as WorkloadAdmissionState;
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadClass};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;

pub fn register_cluster_snapshot_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v4/cluster/snapshot").as_str(),
        AdminOperation(&GetClusterSnapshotHandler {}),
    )?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterSnapshotResponse {
    pub snapshot: Option<ClusterSnapshotView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterSnapshotDiscoveryResponse {
    pub path: String,
    pub summary: Option<CapabilityStatus>,
    pub topology: Option<CapabilityStatus>,
    pub peer_health: Option<CapabilityStatus>,
    pub rpc_boundary: Option<CapabilityStatus>,
    pub workload_admission: Option<CapabilityStatus>,
    pub runtime: Option<CapabilityStatus>,
    pub components: Option<ClusterComponentStatusView>,
}

async fn authorize_cluster_snapshot_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ServerInfoAdminAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

fn build_json_response(
    status: StatusCode,
    body: &impl Serialize,
    request_id: Option<&HeaderValue>,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut header = HeaderMap::new();
    header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(value) = request_id {
        header.insert("x-request-id", value.clone());
    }
    Ok(S3Response::with_headers((status, Body::from(data)), header))
}

pub struct GetClusterSnapshotHandler {}

#[async_trait::async_trait]
impl Operation for GetClusterSnapshotHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_cluster_snapshot_request(&req).await?;
        let snapshot = default_admin_usecase()
            .execute_collect_cluster_read_only_snapshot()
            .await
            .map(ClusterSnapshotView::from);
        build_json_response(StatusCode::OK, &ClusterSnapshotResponse { snapshot }, req.headers.get("x-request-id"))
    }
}

pub(crate) async fn build_cluster_snapshot_discovery_response() -> ClusterSnapshotDiscoveryResponse {
    let usecase = default_admin_usecase();
    let path = usecase.cluster_snapshot_route().to_string();
    let snapshot = usecase.execute_collect_cluster_read_only_snapshot().await;

    match snapshot {
        Some(snapshot) => {
            let components = ClusterComponentStatusView::from_snapshot(&snapshot);
            let summary = ClusterSnapshotSummary::from_snapshot_and_components(&snapshot, &components);
            ClusterSnapshotDiscoveryResponse {
                path,
                summary: Some(summary.actionable_pressure.clone()),
                topology: Some(summary.topology),
                peer_health: Some(summary.peer_health),
                rpc_boundary: Some(summary.rpc_boundary),
                workload_admission: Some(summary.workload_admission),
                runtime: Some(summary.runtime),
                components: Some(components),
            }
        }
        None => ClusterSnapshotDiscoveryResponse {
            path,
            summary: None,
            topology: None,
            peer_health: None,
            rpc_boundary: None,
            workload_admission: None,
            runtime: None,
            components: None,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterSnapshotView {
    pub summary: ClusterSnapshotSummary,
    pub runtime_capabilities_path: String,
    pub extensions_catalog_path: String,
    pub components: ClusterComponentStatusView,
    pub topology: TopologySnapshot,
    pub membership: ClusterMembershipView,
    pub pool_state: ClusterPoolStateView,
    pub local_storage: ClusterLocalStorageView,
    pub peer_health: ClusterPeerHealthView,
    pub rpc_boundary: ClusterRpcBoundaryView,
    pub observability: ObservabilitySnapshot,
    pub workload_admission: Vec<WorkloadAdmissionView>,
    pub runtime_status: ClusterRuntimeStatusView,
    pub actionable_pressure: bool,
}

impl From<ClusterReadOnlySnapshot> for ClusterSnapshotView {
    fn from(snapshot: ClusterReadOnlySnapshot) -> Self {
        let components = ClusterComponentStatusView::from_snapshot(&snapshot);
        let summary = ClusterSnapshotSummary::from_snapshot_and_components(&snapshot, &components);
        let actionable_pressure = cluster_has_actionable_pressure(&snapshot);
        Self {
            summary,
            runtime_capabilities_path: format!("{}{}", ADMIN_PREFIX, system::RUNTIME_CAPABILITIES_ROUTE_SUFFIX),
            extensions_catalog_path: format!("{}{}", ADMIN_PREFIX, "/v4/extensions/catalog"),
            components,
            topology: snapshot.topology,
            membership: ClusterMembershipView::from(snapshot.membership),
            pool_state: ClusterPoolStateView::from(snapshot.pool_state),
            local_storage: ClusterLocalStorageView::from(snapshot.local_storage),
            peer_health: ClusterPeerHealthView::from(snapshot.peer_health),
            rpc_boundary: ClusterRpcBoundaryView::from(snapshot.rpc_boundary),
            observability: snapshot.observability,
            workload_admission: workload_admission_views(snapshot.workload_admission),
            runtime_status: ClusterRuntimeStatusView::from(snapshot.runtime_status),
            actionable_pressure,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterComponentStatusView {
    pub storage: ClusterComponentStatus,
    pub peer_health: ClusterComponentStatus,
    pub listing: ClusterListingMetacacheStatus,
    pub usage: ClusterUsageFreshnessStatus,
    pub workload_admission: ClusterComponentStatus,
}

impl ClusterComponentStatusView {
    fn from_snapshot(snapshot: &ClusterReadOnlySnapshot) -> Self {
        Self {
            storage: component_status_with_condition(
                "runtime_readiness",
                summarize_storage_readiness(snapshot),
                if snapshot.runtime_status.readiness.storage_ready {
                    "healthy"
                } else {
                    "degraded"
                },
            ),
            peer_health: component_status("cluster_peer_health", summarize_peer_health(snapshot)),
            listing: summarize_listing_metacache(snapshot),
            usage: summarize_usage_freshness(snapshot),
            workload_admission: component_status("workload_admission", summarize_workload_admission(snapshot)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterComponentStatus {
    pub source: &'static str,
    pub condition: &'static str,
    pub status: CapabilityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterListingMetacacheStatus {
    pub source: &'static str,
    pub condition: &'static str,
    pub status: CapabilityStatus,
    pub internode_stall_timeouts_total: u64,
    pub hint: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterUsageFreshnessStatus {
    pub source: &'static str,
    pub condition: &'static str,
    pub status: CapabilityStatus,
    pub dirty_pending_buckets: u64,
    pub last_dirty_mark_unix_secs: u64,
    pub last_dirty_clear_unix_secs: u64,
    pub last_cycle_dirty_buckets: u64,
    pub last_cycle_cleared_dirty_buckets: u64,
    pub last_usage_save_unix_secs: u64,
    pub last_usage_save_result: String,
    pub last_success_unix_secs: Option<u64>,
    pub last_error: Option<String>,
}

fn component_status(source: &'static str, status: CapabilityStatus) -> ClusterComponentStatus {
    let condition = condition_from_capability(&status);
    component_status_with_condition(source, status, condition)
}

fn component_status_with_condition(
    source: &'static str,
    status: CapabilityStatus,
    condition: &'static str,
) -> ClusterComponentStatus {
    ClusterComponentStatus {
        source,
        condition,
        status,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterSnapshotSummary {
    pub runtime: CapabilityStatus,
    pub topology: CapabilityStatus,
    pub membership: CapabilityStatus,
    pub storage: CapabilityStatus,
    pub peer_health: CapabilityStatus,
    pub listing: CapabilityStatus,
    pub usage: CapabilityStatus,
    pub rpc_boundary: CapabilityStatus,
    pub observability: CapabilityStatus,
    pub workload_admission: CapabilityStatus,
    pub actionable_pressure: CapabilityStatus,
}

impl From<&ClusterReadOnlySnapshot> for ClusterSnapshotSummary {
    fn from(snapshot: &ClusterReadOnlySnapshot) -> Self {
        let components = ClusterComponentStatusView::from_snapshot(snapshot);
        Self::from_snapshot_and_components(snapshot, &components)
    }
}

impl ClusterSnapshotSummary {
    fn from_snapshot_and_components(snapshot: &ClusterReadOnlySnapshot, components: &ClusterComponentStatusView) -> Self {
        let topology = summarize_topology(snapshot);
        let membership = summarize_membership(snapshot);
        let rpc_boundary = summarize_rpc_boundary(snapshot);
        let observability = summarize_observability(snapshot);
        let actionable_pressure = if cluster_has_actionable_pressure(snapshot) {
            CapabilityStatus::supported().with_reason("cluster snapshot reports degraded runtime or non-open admission")
        } else {
            CapabilityStatus::disabled().with_reason("cluster snapshot reports no actionable pressure")
        };

        Self {
            runtime: summarize_runtime(snapshot),
            topology,
            membership,
            storage: components.storage.status.clone(),
            peer_health: components.peer_health.status.clone(),
            listing: components.listing.status.clone(),
            usage: components.usage.status.clone(),
            rpc_boundary,
            observability,
            workload_admission: components.workload_admission.status.clone(),
            actionable_pressure,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterMembershipView {
    pub nodes: Vec<ClusterNodeMembershipView>,
    pub drives: Vec<ClusterDriveMembershipView>,
}

impl From<ClusterMembershipSnapshot> for ClusterMembershipView {
    fn from(snapshot: ClusterMembershipSnapshot) -> Self {
        Self {
            nodes: snapshot.nodes.into_iter().map(ClusterNodeMembershipView::from).collect(),
            drives: snapshot.drives.into_iter().map(ClusterDriveMembershipView::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterNodeMembershipView {
    pub node_id: String,
    pub grid_host: String,
    pub is_local: bool,
    pub pools: Vec<usize>,
}

impl From<ClusterNodeMembership> for ClusterNodeMembershipView {
    fn from(node: ClusterNodeMembership) -> Self {
        Self {
            node_id: node.node_id,
            grid_host: node.grid_host,
            is_local: node.is_local,
            pools: node.pools,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterDriveMembershipView {
    pub pool_index: usize,
    pub set_index: usize,
    pub disk_index: usize,
    pub node_id: String,
    pub is_local: bool,
    pub endpoint_type: &'static str,
}

impl From<ClusterDriveMembership> for ClusterDriveMembershipView {
    fn from(drive: ClusterDriveMembership) -> Self {
        Self {
            pool_index: drive.pool_index,
            set_index: drive.set_index,
            disk_index: drive.disk_index,
            node_id: drive.node_id,
            is_local: drive.is_local,
            endpoint_type: endpoint_type_label(drive.endpoint_type),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterPoolStateView {
    pub pools: Vec<ClusterPoolStateItemView>,
}

impl From<ClusterPoolStateSnapshot> for ClusterPoolStateView {
    fn from(snapshot: ClusterPoolStateSnapshot) -> Self {
        Self {
            pools: snapshot.pools.into_iter().map(ClusterPoolStateItemView::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterPoolStateItemView {
    pub pool_index: usize,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoint_count: usize,
    pub local_drive_count: usize,
    pub remote_drive_count: usize,
    pub legacy: bool,
    pub endpoint_types: Vec<&'static str>,
}

impl From<ClusterPoolState> for ClusterPoolStateItemView {
    fn from(pool: ClusterPoolState) -> Self {
        Self {
            pool_index: pool.pool_index,
            set_count: pool.set_count,
            drives_per_set: pool.drives_per_set,
            endpoint_count: pool.endpoint_count,
            local_drive_count: pool.local_drive_count,
            remote_drive_count: pool.remote_drive_count,
            legacy: pool.legacy,
            endpoint_types: pool.endpoint_types.into_iter().map(endpoint_type_label).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterLocalStorageView {
    pub nodes: Vec<ClusterLocalNodeStorageView>,
}

impl From<ClusterLocalNodeStorageSnapshot> for ClusterLocalStorageView {
    fn from(snapshot: ClusterLocalNodeStorageSnapshot) -> Self {
        Self {
            nodes: snapshot.nodes.into_iter().map(ClusterLocalNodeStorageView::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterLocalNodeStorageView {
    pub node_id: String,
    pub pools: Vec<usize>,
    pub drive_count: usize,
    pub path_drive_count: usize,
    pub url_drive_count: usize,
}

impl From<ClusterLocalNodeStorage> for ClusterLocalNodeStorageView {
    fn from(node: ClusterLocalNodeStorage) -> Self {
        Self {
            node_id: node.node_id,
            pools: node.pools,
            drive_count: node.drive_count,
            path_drive_count: node.path_drive_count,
            url_drive_count: node.url_drive_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterPeerHealthView {
    pub peers: Vec<ClusterPeerHealthItemView>,
}

impl From<ClusterPeerHealthSnapshot> for ClusterPeerHealthView {
    fn from(snapshot: ClusterPeerHealthSnapshot) -> Self {
        Self {
            peers: snapshot.peers.into_iter().map(ClusterPeerHealthItemView::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterPeerHealthItemView {
    pub node_id: String,
    pub is_local: bool,
    pub status: CapabilityStatus,
}

impl From<ClusterPeerHealth> for ClusterPeerHealthItemView {
    fn from(peer: ClusterPeerHealth) -> Self {
        Self {
            node_id: peer.node_id,
            is_local: peer.is_local,
            status: peer.status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterRpcBoundaryView {
    pub control_channels: Vec<ClusterRpcChannelView>,
    pub data_channels: Vec<ClusterRpcChannelView>,
}

impl From<ClusterRpcBoundarySnapshot> for ClusterRpcBoundaryView {
    fn from(snapshot: ClusterRpcBoundarySnapshot) -> Self {
        Self {
            control_channels: snapshot
                .control_channels
                .into_iter()
                .map(ClusterRpcChannelView::from)
                .collect(),
            data_channels: snapshot.data_channels.into_iter().map(ClusterRpcChannelView::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterRpcChannelView {
    pub name: String,
    pub plane: &'static str,
    pub transport: &'static str,
    pub status: CapabilityStatus,
}

impl From<ClusterRpcChannelSnapshot> for ClusterRpcChannelView {
    fn from(channel: ClusterRpcChannelSnapshot) -> Self {
        Self {
            name: channel.name,
            plane: rpc_plane_label(channel.plane),
            transport: rpc_transport_label(channel.transport),
            status: channel.status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct WorkloadAdmissionView {
    pub class: &'static str,
    pub state: &'static str,
    pub active: Option<usize>,
    pub queued: Option<usize>,
    pub limit: Option<usize>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterRuntimeStatusView {
    pub state: &'static str,
    pub storage_ready: bool,
    pub iam_ready: bool,
    pub lock_quorum_ready: bool,
    pub peer_health_ready: bool,
    pub degraded_reasons: Vec<&'static str>,
}

impl From<ClusterRuntimeStatusSnapshot> for ClusterRuntimeStatusView {
    fn from(runtime: ClusterRuntimeStatusSnapshot) -> Self {
        Self {
            state: runtime_readiness_state_label(runtime.state),
            storage_ready: runtime.readiness.storage_ready,
            iam_ready: runtime.readiness.iam_ready,
            lock_quorum_ready: runtime.readiness.lock_quorum_ready,
            peer_health_ready: runtime.readiness.peer_health_ready,
            degraded_reasons: runtime.degraded_reasons.into_iter().map(|reason| reason.as_str()).collect(),
        }
    }
}

fn workload_admission_views(snapshot: WorkloadAdmissionRegistrySnapshot) -> Vec<WorkloadAdmissionView> {
    snapshot.entries().iter().cloned().map(WorkloadAdmissionView::from).collect()
}

impl From<WorkloadAdmissionSnapshot> for WorkloadAdmissionView {
    fn from(snapshot: WorkloadAdmissionSnapshot) -> Self {
        Self {
            class: workload_class_label(snapshot.class),
            state: admission_state_label(snapshot.state),
            active: snapshot.active,
            queued: snapshot.queued,
            limit: snapshot.limit,
            reason: snapshot.reason,
        }
    }
}

fn endpoint_type_label(endpoint_type: ClusterEndpointType) -> &'static str {
    match endpoint_type {
        ClusterEndpointType::Path => "path",
        ClusterEndpointType::Url => "url",
    }
}

fn rpc_plane_label(plane: ClusterRpcPlane) -> &'static str {
    match plane {
        ClusterRpcPlane::Control => "control",
        ClusterRpcPlane::Data => "data",
    }
}

fn rpc_transport_label(transport: ClusterRpcTransport) -> &'static str {
    match transport {
        ClusterRpcTransport::Grpc => "grpc",
        ClusterRpcTransport::InternodeDataTransport => "internode_data_transport",
    }
}

fn workload_class_label(class: WorkloadClass) -> &'static str {
    class.as_str()
}

fn admission_state_label(state: AdmissionState) -> &'static str {
    match state {
        AdmissionState::Open => "open",
        AdmissionState::Throttled => "throttled",
        AdmissionState::Saturated => "saturated",
        AdmissionState::Disabled => "disabled",
        AdmissionState::Unknown => "unknown",
    }
}

fn runtime_readiness_state_label(state: ClusterRuntimeReadinessState) -> &'static str {
    match state {
        ClusterRuntimeReadinessState::Ready => "ready",
        ClusterRuntimeReadinessState::Degraded => "degraded",
        ClusterRuntimeReadinessState::Unknown => "unknown",
    }
}

fn condition_from_capability(status: &CapabilityStatus) -> &'static str {
    match status.state {
        CapabilityState::Supported => "healthy",
        CapabilityState::Unsupported => "unsupported",
        CapabilityState::Disabled => "not_reported",
        CapabilityState::Unknown => "unknown",
    }
}

fn summarize_storage_readiness(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    if snapshot.runtime_status.readiness.storage_ready {
        return CapabilityStatus::supported().with_reason("storage readiness reports ready");
    }

    let storage_reasons = snapshot
        .runtime_status
        .degraded_reasons
        .iter()
        .filter_map(|reason| match reason {
            ReadinessDegradedReason::StorageQuorumUnavailable
            | ReadinessDegradedReason::StorageAndIamUnavailable
            | ReadinessDegradedReason::StorageAndLockUnavailable
            | ReadinessDegradedReason::StorageIamAndLockUnavailable => Some(reason.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if storage_reasons.is_empty() {
        CapabilityStatus::unknown().with_reason("storage readiness is false without a storage-specific degraded reason")
    } else {
        CapabilityStatus::unknown().with_reason(format!("storage readiness degraded: {}", storage_reasons.join(", ")))
    }
}

fn summarize_listing_readiness(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    match snapshot
        .workload_admission
        .entries()
        .iter()
        .find(|entry| entry.class == WorkloadClass::ForegroundRead)
    {
        Some(entry) if entry.state == AdmissionState::Open => {
            CapabilityStatus::supported().with_reason("foreground read admission is open")
        }
        Some(entry) => {
            let mut reason = format!("foreground read admission is {}", admission_state_label(entry.state));
            if let Some(detail) = entry.reason.as_deref() {
                reason.push_str(": ");
                reason.push_str(detail);
            }
            CapabilityStatus::unknown().with_reason(reason)
        }
        None => CapabilityStatus::unknown().with_reason("foreground read admission is not reported"),
    }
}

fn summarize_listing_metacache(snapshot: &ClusterReadOnlySnapshot) -> ClusterListingMetacacheStatus {
    const HINT: &str = "bucket and prefix attribution is emitted through metacache quorum logs; inspect operation-labelled walk_dir metrics for internode traffic";

    let admission = summarize_listing_readiness(snapshot);
    let stalls = snapshot.listing_diagnostics.internode_stall_timeouts_total;
    let (condition, status) = if admission.state == CapabilityState::Supported {
        ("healthy", admission)
    } else {
        (condition_from_capability(&admission), admission)
    };

    ClusterListingMetacacheStatus {
        source: "workload_admission+internode_metrics",
        condition,
        status,
        internode_stall_timeouts_total: stalls,
        hint: HINT,
    }
}

fn summarize_usage_freshness(snapshot: &ClusterReadOnlySnapshot) -> ClusterUsageFreshnessStatus {
    let freshness = &snapshot.usage_freshness;
    let (condition, status) = match freshness.last_usage_save_result.as_str() {
        "success" if freshness.dirty_pending_buckets == 0 => (
            "healthy",
            CapabilityStatus::supported().with_reason("usage cache was saved successfully and has no pending dirty buckets"),
        ),
        "success" | "" if freshness.dirty_pending_buckets > 0 => (
            "stale",
            CapabilityStatus::unknown()
                .with_reason(format!("usage cache has {} pending dirty buckets", freshness.dirty_pending_buckets)),
        ),
        "skipped_stale" => (
            "stale",
            CapabilityStatus::unknown().with_reason("last usage cache save was skipped because scanner data was stale"),
        ),
        "failed" => ("degraded", CapabilityStatus::unknown().with_reason("last usage cache save failed")),
        "encode_failed" => (
            "degraded",
            CapabilityStatus::unknown().with_reason("last usage cache save failed during encoding"),
        ),
        _ => (
            "unknown",
            CapabilityStatus::unknown().with_reason("no usage cache save result has been reported"),
        ),
    };
    let last_success_unix_secs = (freshness.last_usage_save_result == "success" && freshness.last_usage_save_unix_secs > 0)
        .then_some(freshness.last_usage_save_unix_secs);
    let last_error = match freshness.last_usage_save_result.as_str() {
        "failed" | "skipped_stale" | "encode_failed" => Some(freshness.last_usage_save_result.clone()),
        _ => None,
    };

    ClusterUsageFreshnessStatus {
        source: "scanner_metrics",
        condition,
        status,
        dirty_pending_buckets: freshness.dirty_pending_buckets,
        last_dirty_mark_unix_secs: freshness.last_dirty_mark_unix_secs,
        last_dirty_clear_unix_secs: freshness.last_dirty_clear_unix_secs,
        last_cycle_dirty_buckets: freshness.last_cycle_dirty_buckets,
        last_cycle_cleared_dirty_buckets: freshness.last_cycle_cleared_dirty_buckets,
        last_usage_save_unix_secs: freshness.last_usage_save_unix_secs,
        last_usage_save_result: freshness.last_usage_save_result.clone(),
        last_success_unix_secs,
        last_error,
    }
}

fn summarize_runtime(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    match snapshot.runtime_status.state {
        ClusterRuntimeReadinessState::Ready => CapabilityStatus::supported().with_reason("runtime readiness reports ready"),
        ClusterRuntimeReadinessState::Degraded => {
            let reasons = snapshot
                .runtime_status
                .degraded_reasons
                .iter()
                .map(|reason| reason.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            CapabilityStatus::unknown().with_reason(format!("runtime readiness degraded: {reasons}"))
        }
        ClusterRuntimeReadinessState::Unknown => CapabilityStatus::unknown().with_reason("runtime readiness status unknown"),
    }
}

fn summarize_topology(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    summarize_named_capability_statuses(
        [
            ("profiling", &snapshot.topology.capabilities.profiling),
            ("numa", &snapshot.topology.capabilities.numa),
            ("failure_domain_labels", &snapshot.topology.capabilities.failure_domain_labels),
            ("media_labels", &snapshot.topology.capabilities.media_labels),
        ],
        "topology capability",
    )
    .with_reason("topology summary resolved from cluster snapshot")
}

fn summarize_membership(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    if snapshot.membership.nodes.is_empty() {
        CapabilityStatus::unknown().with_reason("cluster membership has no nodes")
    } else {
        CapabilityStatus::supported().with_reason(format!(
            "cluster membership reports {} nodes and {} drives",
            snapshot.membership.nodes.len(),
            snapshot.membership.drives.len()
        ))
    }
}

fn summarize_peer_health(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    if snapshot.peer_health.peers.is_empty() {
        return CapabilityStatus::unknown().with_reason("cluster peer health has no peers");
    }

    let unknown = snapshot
        .peer_health
        .peers
        .iter()
        .filter(|peer| peer.status.state == CapabilityState::Unknown)
        .count();
    let disabled = snapshot
        .peer_health
        .peers
        .iter()
        .filter(|peer| peer.status.state == CapabilityState::Disabled)
        .count();
    if unknown > 0 {
        CapabilityStatus::unknown().with_reason(format!("cluster peer health has {unknown} unresolved peers"))
    } else if disabled > 0 {
        let noun = if disabled == 1 { "peer" } else { "peers" };
        CapabilityStatus::disabled().with_reason(format!("cluster peer health is not reported by {disabled} {noun}"))
    } else {
        CapabilityStatus::supported().with_reason("cluster peer health resolved for all peers")
    }
}

fn summarize_rpc_boundary(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    let has_control = !snapshot.rpc_boundary.control_channels.is_empty();
    let has_data = !snapshot.rpc_boundary.data_channels.is_empty();
    if has_control && has_data {
        CapabilityStatus::supported().with_reason("cluster control RPC and data streams are modeled as separate planes")
    } else {
        CapabilityStatus::unknown().with_reason("cluster RPC boundary snapshot is incomplete")
    }
}

fn summarize_workload_admission(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    let entries = snapshot.workload_admission.entries();
    if entries.is_empty() {
        return CapabilityStatus::unknown().with_reason("workload admission snapshot is empty");
    }

    let non_open = entries
        .iter()
        .filter(|entry| entry.state != WorkloadAdmissionState::Open)
        .map(|entry| entry.class.as_str())
        .collect::<Vec<_>>();
    if non_open.is_empty() {
        CapabilityStatus::supported().with_reason("all workload admission classes are open")
    } else {
        CapabilityStatus::unknown().with_reason(format!("workload admission has non-open classes: {}", non_open.join(", ")))
    }
}

fn summarize_observability(snapshot: &ClusterReadOnlySnapshot) -> CapabilityStatus {
    let userspace_profiling = summarize_named_capability_statuses(
        [
            ("cpu", &snapshot.observability.userspace_profiling.cpu),
            ("memory", &snapshot.observability.userspace_profiling.memory),
            ("continuous_cpu", &snapshot.observability.userspace_profiling.continuous_cpu),
            ("periodic_cpu", &snapshot.observability.userspace_profiling.periodic_cpu),
        ],
        "userspace profiling",
    );
    let memory_sampling = summarize_named_capability_statuses(
        [
            ("process", &snapshot.observability.memory_sampling.process),
            ("system", &snapshot.observability.memory_sampling.system),
            ("cgroup", &snapshot.observability.memory_sampling.cgroup),
        ],
        "memory sampling",
    );
    let platform = summarize_named_capability_statuses(
        [
            ("allocator", &snapshot.observability.platform.allocator),
            ("ebpf", &snapshot.observability.platform.ebpf),
            ("numa", &snapshot.observability.platform.numa),
        ],
        "platform support",
    );

    if [userspace_profiling.state, memory_sampling.state, platform.state]
        .into_iter()
        .any(|state| state == CapabilityState::Unknown)
    {
        CapabilityStatus::unknown().with_reason("observability summary resolved from cluster snapshot")
    } else {
        CapabilityStatus::supported().with_reason("observability summary resolved from cluster snapshot")
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

#[cfg(test)]
mod tests {
    use super::{ClusterSnapshotResponse, ClusterSnapshotSummary, ClusterSnapshotView};
    use crate::admin::storage_api::cluster::CapabilityState;
    use crate::admin::storage_api::cluster::{CapabilityStatus, ObservabilitySnapshot, TopologySnapshot};
    use crate::admin::storage_api::cluster::{
        ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage, ClusterLocalNodeStorageSnapshot,
        ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth, ClusterPeerHealthSnapshot, ClusterPoolState,
        ClusterPoolStateSnapshot, ClusterRpcBoundarySnapshot, ClusterRpcChannelSnapshot, ClusterRpcPlane, ClusterRpcTransport,
    };
    use crate::cluster_snapshot::{
        ClusterListingDiagnosticsSnapshot, ClusterReadOnlySnapshot, ClusterRuntimeReadinessState, ClusterRuntimeStatusSnapshot,
        ClusterUsageFreshnessSnapshot,
    };
    use crate::server::{DependencyReadiness, ReadinessDegradedReason};
    use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadClass};

    #[test]
    fn cluster_snapshot_handler_requires_server_info_admin_permission() {
        let src = include_str!("cluster_snapshot.rs");
        let handler_block = extract_block_between_markers(src, "impl Operation for GetClusterSnapshotHandler", "#[cfg(test)]");
        let auth_block =
            extract_block_between_markers(src, "async fn authorize_cluster_snapshot_request", "fn build_json_response");

        assert!(
            handler_block.contains("authorize_cluster_snapshot_request(&req).await?;"),
            "cluster snapshot handler should require admin authorization"
        );
        assert!(
            auth_block.contains("AdminAction::ServerInfoAdminAction"),
            "cluster snapshot should require server info admin permission"
        );
    }

    #[test]
    fn cluster_snapshot_response_serializes_none_snapshot() {
        let value = serde_json::to_value(ClusterSnapshotResponse { snapshot: None }).expect("serialize response");
        assert_eq!(value, serde_json::json!({ "snapshot": null }));
    }

    #[tokio::test]
    async fn cluster_snapshot_discovery_reports_path_without_snapshot() {
        let response = super::build_cluster_snapshot_discovery_response().await;

        assert_eq!(response.path, "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(response.summary, None);
        assert_eq!(response.topology, None);
        assert_eq!(response.peer_health, None);
        assert_eq!(response.rpc_boundary, None);
        assert_eq!(response.workload_admission, None);
        assert_eq!(response.runtime, None);
        assert_eq!(response.components, None);
    }

    #[test]
    fn cluster_snapshot_view_serializes_machine_readable_sections() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot {
                nodes: vec![ClusterNodeMembership {
                    node_id: "node-a".to_string(),
                    grid_host: "node-a:9000".to_string(),
                    is_local: true,
                    pools: vec![0],
                }],
                drives: vec![ClusterDriveMembership {
                    pool_index: 0,
                    set_index: 1,
                    disk_index: 2,
                    node_id: "node-a".to_string(),
                    is_local: true,
                    endpoint_type: ClusterEndpointType::Url,
                }],
            },
            pool_state: ClusterPoolStateSnapshot {
                pools: vec![ClusterPoolState {
                    pool_index: 0,
                    set_count: 1,
                    drives_per_set: 4,
                    endpoint_count: 4,
                    local_drive_count: 2,
                    remote_drive_count: 2,
                    legacy: false,
                    endpoint_types: vec![ClusterEndpointType::Url],
                }],
            },
            local_storage: ClusterLocalNodeStorageSnapshot {
                nodes: vec![ClusterLocalNodeStorage {
                    node_id: "node-a".to_string(),
                    pools: vec![0],
                    drive_count: 2,
                    path_drive_count: 0,
                    url_drive_count: 2,
                }],
            },
            peer_health: ClusterPeerHealthSnapshot {
                peers: vec![ClusterPeerHealth {
                    node_id: "node-b".to_string(),
                    is_local: false,
                    status: CapabilityStatus::unknown().with_reason("peer state unavailable"),
                }],
            },
            rpc_boundary: sample_rpc_boundary_snapshot(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![
                WorkloadAdmissionSnapshot::new(WorkloadClass::Repair, AdmissionState::Unknown)
                    .with_counts(Some(1), Some(2), Some(8))
                    .with_reason("repair backlog not yet normalized"),
            ]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: false,
                    iam_ready: true,
                    lock_quorum_ready: false,
                    peer_health_ready: true,
                },
                state: ClusterRuntimeReadinessState::Degraded,
                degraded_reasons: vec![ReadinessDegradedReason::StorageAndLockUnavailable],
            },
            usage_freshness: ClusterUsageFreshnessSnapshot {
                dirty_pending_buckets: 3,
                last_usage_save_unix_secs: 123,
                last_usage_save_result: "skipped_stale".to_string(),
                last_usage_save_result_code: 3,
                ..Default::default()
            },
            listing_diagnostics: ClusterListingDiagnosticsSnapshot {
                internode_stall_timeouts_total: 2,
            },
        };

        let value = serde_json::to_value(ClusterSnapshotView::from(snapshot)).expect("serialize view");
        assert_eq!(value["runtime_capabilities_path"], "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(value["extensions_catalog_path"], "/rustfs/admin/v4/extensions/catalog");
        assert_eq!(value["components"]["storage"]["source"], "runtime_readiness");
        assert_eq!(value["components"]["storage"]["condition"], "degraded");
        assert_eq!(value["components"]["peer_health"]["source"], "cluster_peer_health");
        assert_eq!(value["components"]["listing"]["source"], "workload_admission+internode_metrics");
        assert_eq!(value["components"]["listing"]["condition"], "unknown");
        assert_eq!(value["components"]["listing"]["internode_stall_timeouts_total"], 2);
        assert_eq!(value["components"]["usage"]["source"], "scanner_metrics");
        assert_eq!(value["components"]["usage"]["condition"], "stale");
        assert_eq!(value["membership"]["drives"][0]["endpoint_type"], "url");
        assert_eq!(value["workload_admission"][0]["class"], "repair");
        assert_eq!(value["workload_admission"][0]["state"], "unknown");
        assert_eq!(value["rpc_boundary"]["control_channels"][0]["name"], "metadata");
        assert_eq!(value["rpc_boundary"]["control_channels"][0]["transport"], "grpc");
        assert_eq!(value["rpc_boundary"]["data_channels"][0]["transport"], "internode_data_transport");
        assert_eq!(value["runtime_status"]["state"], "degraded");
        assert_eq!(value["summary"]["runtime"]["state"], "unknown");
        assert_eq!(value["summary"]["rpc_boundary"]["state"], "supported");
        assert_eq!(value["runtime_status"]["degraded_reasons"][0], "storage_and_lock_unavailable");
        assert_eq!(value["actionable_pressure"], true);
    }

    #[test]
    fn cluster_snapshot_summary_reports_cross_surface_status() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot::default(),
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot::default(),
            rpc_boundary: sample_rpc_boundary_snapshot(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(
                WorkloadClass::ForegroundRead,
                AdmissionState::Open,
            )]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: true,
                    iam_ready: true,
                    lock_quorum_ready: true,
                    peer_health_ready: true,
                },
                state: ClusterRuntimeReadinessState::Ready,
                degraded_reasons: Vec::new(),
            },
            usage_freshness: ClusterUsageFreshnessSnapshot::default(),
            listing_diagnostics: ClusterListingDiagnosticsSnapshot::default(),
        };

        let summary = ClusterSnapshotSummary::from(&snapshot);
        assert_eq!(summary.runtime.state, CapabilityState::Supported);
        assert_eq!(summary.membership.state, CapabilityState::Unknown);
        assert_eq!(summary.storage.state, CapabilityState::Supported);
        assert_eq!(summary.storage.reason.as_deref(), Some("storage readiness reports ready"));
        assert_eq!(summary.peer_health.state, CapabilityState::Unknown);
        assert_eq!(summary.listing.state, CapabilityState::Supported);
        assert_eq!(summary.listing.reason.as_deref(), Some("foreground read admission is open"));
        assert_eq!(summary.usage.state, CapabilityState::Unknown);
        assert_eq!(summary.usage.reason.as_deref(), Some("no usage cache save result has been reported"));
        assert_eq!(summary.rpc_boundary.state, CapabilityState::Supported);
        assert_eq!(summary.workload_admission.state, CapabilityState::Supported);
        assert_eq!(summary.actionable_pressure.state, CapabilityState::Disabled);
    }

    #[test]
    fn cluster_snapshot_components_split_storage_peer_listing_and_usage_states() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot {
                nodes: vec![ClusterNodeMembership {
                    node_id: "node-a".to_string(),
                    grid_host: "node-a:9000".to_string(),
                    is_local: true,
                    pools: vec![0],
                }],
                drives: Vec::new(),
            },
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot {
                peers: vec![ClusterPeerHealth {
                    node_id: "node-a".to_string(),
                    is_local: true,
                    status: CapabilityStatus::disabled().with_reason("peer health not reported by endpoints"),
                }],
            },
            rpc_boundary: sample_rpc_boundary_snapshot(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![
                WorkloadAdmissionSnapshot::new(WorkloadClass::ForegroundRead, AdmissionState::Throttled)
                    .with_counts(Some(4), Some(8), Some(4))
                    .with_reason("metacache listing backpressure"),
            ]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: false,
                    iam_ready: true,
                    lock_quorum_ready: true,
                    peer_health_ready: true,
                },
                state: ClusterRuntimeReadinessState::Degraded,
                degraded_reasons: vec![ReadinessDegradedReason::StorageQuorumUnavailable],
            },
            usage_freshness: ClusterUsageFreshnessSnapshot {
                dirty_pending_buckets: 3,
                last_usage_save_unix_secs: 123,
                last_usage_save_result: "skipped_stale".to_string(),
                last_usage_save_result_code: 3,
                ..Default::default()
            },
            listing_diagnostics: ClusterListingDiagnosticsSnapshot {
                internode_stall_timeouts_total: 0,
            },
        };

        let view = ClusterSnapshotView::from(snapshot);

        assert_eq!(view.components.storage.status.state, CapabilityState::Unknown);
        assert_eq!(
            view.components.storage.status.reason.as_deref(),
            Some("storage readiness degraded: storage_quorum_unavailable")
        );
        assert_eq!(view.components.storage.condition, "degraded");
        assert_eq!(view.components.peer_health.status.state, CapabilityState::Disabled);
        assert_eq!(view.components.peer_health.condition, "not_reported");
        assert_eq!(view.components.listing.status.state, CapabilityState::Unknown);
        assert_eq!(
            view.components.listing.status.reason.as_deref(),
            Some("foreground read admission is throttled: metacache listing backpressure")
        );
        assert_eq!(view.components.listing.condition, "unknown");
        assert_eq!(view.components.usage.status.state, CapabilityState::Unknown);
        assert_eq!(view.components.usage.condition, "stale");
        assert_eq!(view.components.usage.last_usage_save_unix_secs, 123);
        assert_eq!(view.components.usage.last_usage_save_result, "skipped_stale");
    }

    #[test]
    fn cluster_snapshot_listing_component_keeps_historical_stalls_as_evidence() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot::default(),
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot::default(),
            rpc_boundary: ClusterRpcBoundarySnapshot::default(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(
                WorkloadClass::ForegroundRead,
                AdmissionState::Open,
            )]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness::default(),
                state: ClusterRuntimeReadinessState::Unknown,
                degraded_reasons: Vec::new(),
            },
            usage_freshness: ClusterUsageFreshnessSnapshot::default(),
            listing_diagnostics: ClusterListingDiagnosticsSnapshot {
                internode_stall_timeouts_total: 2,
            },
        };

        let component = super::summarize_listing_metacache(&snapshot);

        assert_eq!(component.status.state, CapabilityState::Supported);
        assert_eq!(component.status.reason.as_deref(), Some("foreground read admission is open"));
        assert_eq!(component.condition, "healthy");
        assert_eq!(component.internode_stall_timeouts_total, 2);
    }

    #[test]
    fn cluster_component_condition_preserves_capability_states() {
        assert_eq!(super::condition_from_capability(&CapabilityStatus::supported()), "healthy");
        assert_eq!(super::condition_from_capability(&CapabilityStatus::unsupported()), "unsupported");
        assert_eq!(super::condition_from_capability(&CapabilityStatus::disabled()), "not_reported");
        assert_eq!(super::condition_from_capability(&CapabilityStatus::unknown()), "unknown");
    }

    #[test]
    fn cluster_snapshot_usage_component_reports_clean_success() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot::default(),
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot::default(),
            rpc_boundary: ClusterRpcBoundarySnapshot::default(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::default(),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness::default(),
                state: ClusterRuntimeReadinessState::Unknown,
                degraded_reasons: Vec::new(),
            },
            usage_freshness: ClusterUsageFreshnessSnapshot {
                dirty_pending_buckets: 0,
                last_usage_save_unix_secs: 456,
                last_usage_save_result: "success".to_string(),
                last_usage_save_result_code: 1,
                ..Default::default()
            },
            listing_diagnostics: ClusterListingDiagnosticsSnapshot::default(),
        };

        let component = super::summarize_usage_freshness(&snapshot);

        assert_eq!(component.status.state, CapabilityState::Supported);
        assert_eq!(component.condition, "healthy");
        assert_eq!(component.last_usage_save_unix_secs, 456);
        assert_eq!(component.last_usage_save_result, "success");
    }

    #[test]
    fn cluster_snapshot_usage_component_reports_dirty_without_save_result_as_stale() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot::default(),
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot::default(),
            rpc_boundary: ClusterRpcBoundarySnapshot::default(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::default(),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness::default(),
                state: ClusterRuntimeReadinessState::Unknown,
                degraded_reasons: Vec::new(),
            },
            usage_freshness: ClusterUsageFreshnessSnapshot {
                dirty_pending_buckets: 2,
                ..Default::default()
            },
            listing_diagnostics: ClusterListingDiagnosticsSnapshot::default(),
        };

        let component = super::summarize_usage_freshness(&snapshot);

        assert_eq!(component.status.state, CapabilityState::Unknown);
        assert_eq!(component.condition, "stale");
        assert_eq!(component.status.reason.as_deref(), Some("usage cache has 2 pending dirty buckets"));
    }

    #[test]
    fn cluster_snapshot_summary_treats_not_reported_peer_health_as_disabled() {
        let snapshot = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot {
                nodes: vec![ClusterNodeMembership {
                    node_id: "node-a".to_string(),
                    grid_host: "node-a:9000".to_string(),
                    is_local: true,
                    pools: vec![0],
                }],
                drives: Vec::new(),
            },
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot {
                peers: vec![ClusterPeerHealth {
                    node_id: "node-a".to_string(),
                    is_local: true,
                    status: CapabilityStatus::disabled().with_reason("peer health not reported by endpoints"),
                }],
            },
            rpc_boundary: sample_rpc_boundary_snapshot(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(
                WorkloadClass::ForegroundRead,
                AdmissionState::Open,
            )]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: true,
                    iam_ready: true,
                    lock_quorum_ready: true,
                    peer_health_ready: true,
                },
                state: ClusterRuntimeReadinessState::Ready,
                degraded_reasons: Vec::new(),
            },
            usage_freshness: ClusterUsageFreshnessSnapshot::default(),
            listing_diagnostics: ClusterListingDiagnosticsSnapshot::default(),
        };

        let summary = ClusterSnapshotSummary::from(&snapshot);

        assert_eq!(summary.peer_health.state, CapabilityState::Disabled);
        assert_eq!(
            summary.peer_health.reason.as_deref(),
            Some("cluster peer health is not reported by 1 peer")
        );
        assert_eq!(summary.actionable_pressure.state, CapabilityState::Disabled);
    }

    fn sample_rpc_boundary_snapshot() -> ClusterRpcBoundarySnapshot {
        ClusterRpcBoundarySnapshot {
            control_channels: vec![ClusterRpcChannelSnapshot {
                name: "metadata".to_string(),
                plane: ClusterRpcPlane::Control,
                transport: ClusterRpcTransport::Grpc,
                status: CapabilityStatus::supported().with_reason("control RPC remains on gRPC"),
            }],
            data_channels: vec![ClusterRpcChannelSnapshot {
                name: "remote_disk_stream".to_string(),
                plane: ClusterRpcPlane::Data,
                transport: ClusterRpcTransport::InternodeDataTransport,
                status: CapabilityStatus::supported().with_reason("remote disk data streams remain separate"),
            }],
        }
    }

    fn extract_block_between_markers(src: &str, start: &str, end: &str) -> String {
        let start_index = src.find(start).expect("start marker should exist");
        let end_index = src[start_index..]
            .find(end)
            .map(|index| start_index + index)
            .unwrap_or(src.len());
        src[start_index..end_index].to_string()
    }
}
