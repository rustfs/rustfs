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

use crate::admin::{
    auth::validate_admin_request,
    router::{AdminOperation, Operation, S3Router},
};
use crate::app::admin_usecase::DefaultAdminUsecase;
use crate::auth::{check_key_valid, get_session_token};
use crate::cluster_snapshot::{
    ClusterReadOnlySnapshot, ClusterRuntimeReadinessState, ClusterRuntimeStatusSnapshot, cluster_has_actionable_pressure,
};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadClass};
use rustfs_ecstore::api::cluster::{
    ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage, ClusterLocalNodeStorageSnapshot,
    ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth, ClusterPeerHealthSnapshot, ClusterPoolState,
    ClusterPoolStateSnapshot,
};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_storage_api::{CapabilityStatus, ObservabilitySnapshot, TopologySnapshot};
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
        let snapshot = DefaultAdminUsecase::from_global()
            .execute_collect_cluster_read_only_snapshot()
            .await
            .map(ClusterSnapshotView::from);
        build_json_response(StatusCode::OK, &ClusterSnapshotResponse { snapshot }, req.headers.get("x-request-id"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClusterSnapshotView {
    pub topology: TopologySnapshot,
    pub membership: ClusterMembershipView,
    pub pool_state: ClusterPoolStateView,
    pub local_storage: ClusterLocalStorageView,
    pub peer_health: ClusterPeerHealthView,
    pub observability: ObservabilitySnapshot,
    pub workload_admission: Vec<WorkloadAdmissionView>,
    pub runtime_status: ClusterRuntimeStatusView,
    pub actionable_pressure: bool,
}

impl From<ClusterReadOnlySnapshot> for ClusterSnapshotView {
    fn from(snapshot: ClusterReadOnlySnapshot) -> Self {
        let actionable_pressure = cluster_has_actionable_pressure(&snapshot);
        Self {
            topology: snapshot.topology,
            membership: ClusterMembershipView::from(snapshot.membership),
            pool_state: ClusterPoolStateView::from(snapshot.pool_state),
            local_storage: ClusterLocalStorageView::from(snapshot.local_storage),
            peer_health: ClusterPeerHealthView::from(snapshot.peer_health),
            observability: snapshot.observability,
            workload_admission: workload_admission_views(snapshot.workload_admission),
            runtime_status: ClusterRuntimeStatusView::from(snapshot.runtime_status),
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
    pub degraded_reasons: Vec<&'static str>,
}

impl From<ClusterRuntimeStatusSnapshot> for ClusterRuntimeStatusView {
    fn from(runtime: ClusterRuntimeStatusSnapshot) -> Self {
        Self {
            state: runtime_readiness_state_label(runtime.state),
            storage_ready: runtime.readiness.storage_ready,
            iam_ready: runtime.readiness.iam_ready,
            lock_quorum_ready: runtime.readiness.lock_quorum_ready,
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

#[cfg(test)]
mod tests {
    use super::{ClusterSnapshotResponse, ClusterSnapshotView};
    use crate::cluster_snapshot::{ClusterReadOnlySnapshot, ClusterRuntimeReadinessState, ClusterRuntimeStatusSnapshot};
    use crate::server::{DependencyReadiness, ReadinessDegradedReason};
    use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadClass};
    use rustfs_ecstore::api::cluster::{
        ClusterDriveMembership, ClusterEndpointType, ClusterLocalNodeStorage, ClusterLocalNodeStorageSnapshot,
        ClusterMembershipSnapshot, ClusterNodeMembership, ClusterPeerHealth, ClusterPeerHealthSnapshot, ClusterPoolState,
        ClusterPoolStateSnapshot,
    };
    use rustfs_storage_api::{CapabilityStatus, ObservabilitySnapshot, TopologySnapshot};

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
                },
                state: ClusterRuntimeReadinessState::Degraded,
                degraded_reasons: vec![ReadinessDegradedReason::StorageAndLockUnavailable],
            },
        };

        let value = serde_json::to_value(ClusterSnapshotView::from(snapshot)).expect("serialize view");
        assert_eq!(value["membership"]["drives"][0]["endpoint_type"], "url");
        assert_eq!(value["workload_admission"][0]["class"], "repair");
        assert_eq!(value["workload_admission"][0]["state"], "unknown");
        assert_eq!(value["runtime_status"]["state"], "degraded");
        assert_eq!(value["runtime_status"]["degraded_reasons"][0], "storage_and_lock_unavailable");
        assert_eq!(value["actionable_pressure"], true);
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
