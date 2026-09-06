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

use crate::admin::handlers::kms_dynamic::{current_kms_config_fingerprint, reload_persisted_kms_config};
use crate::admin::service::{
    config::{reload_dynamic_config_runtime_state, reload_runtime_config_snapshot},
    site_replication::reload_site_replication_runtime_state,
};
use crate::server::MODULE_SWITCHES_SIGNAL_SUBSYSTEM;
use crate::storage::storage_api::ecstore_tier::tier_mutation_peer::{self, TierMutationPeerState as EcTierMutationPeerState};
use crate::storage::storage_api::rpc_consumer::node_service::STORAGE_CLASS_SUB_SYS;
#[cfg(test)]
use crate::storage::storage_api::rpc_consumer::node_service::{CollectMetricsOpts, MetricType};
use crate::storage::storage_api::rpc_consumer::node_service::{
    DiskStore, ECStore, Error, KMS_SIGNAL_SUBSYSTEM, LocalPeerS3Client, PEER_RESTDRY_RUN, PEER_RESTSIGNAL, PEER_RESTSUB_SYS,
    SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION, SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION, SCANNER_ACTIVITY_V6_PROTOCOL_VERSION,
    SCANNER_PUBLICATION_LEASE_TTL_MS, SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, StorageDiskRpcExt as _,
    StorageResult, all_local_disk_path, find_local_disk_by_ref, reload_transition_tier_config,
};
use crate::storage::storage_api::runtime_sources_consumer::{EndpointServerPools, runtime_sources};
use crate::storage::storage_api::{
    sign_tonic_rpc_response_proof, verify_tonic_canonical_body_digest, verify_tonic_mutation_body_digest,
    verify_tonic_mutation_body_digest_reject_unsigned,
};
use bytes::Bytes;
use futures::Stream;
use futures_util::future::join_all;
use rmp_serde::Deserializer;
use rustfs_config::audit::{AUDIT_MQTT_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS};
use rustfs_config::notify::NOTIFY_SUB_SYSTEMS;
use rustfs_config::{HEAL_SUB_SYS, SCANNER_SUB_SYS};
use rustfs_filemeta::MetacacheReader;
use rustfs_iam::store::UserType;
use rustfs_lock::LockClient;
use rustfs_protos::{
    CanonicalMutationBody,
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{node_service_server::NodeService as Node, *},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Cursor,
    pin::Pin,
    sync::{Arc, LazyLock, OnceLock},
};
use time::OffsetDateTime;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub(crate) mod heal;

const LOG_COMPONENT_STORAGE: &str = "storage";
const HEAL_CONTROL_FINGERPRINT_MAX_SIZE: usize = 256;
const HEAL_CONTROL_PAYLOAD_MAX_SIZE: usize = 64 * 1024;
const LOG_SUBSYSTEM_RPC: &str = "rpc";
const LOG_SUBSYSTEM_REBALANCE: &str = "rebalance";
const EVENT_RPC_REQUEST_REJECTED: &str = "rpc_request_rejected";
const EVENT_RPC_REQUEST_FAILED: &str = "rpc_request_failed";
const EVENT_RPC_RESPONSE_EMITTED: &str = "rpc_response_emitted";
const EVENT_RPC_BACKGROUND_TASK_SPAWNED: &str = "rpc_background_task_spawned";
const EVENT_RPC_BACKGROUND_TASK_FAILED: &str = "rpc_background_task_failed";
const HEAL_CONTROL_REPLAY_CACHE_MAX_ENTRIES: usize = 4096;
const TIER_MUTATION_PEER_STATE_UNSPECIFIED_WIRE: i32 = 0;
const TIER_MUTATION_PEER_STATE_PREPARED_WIRE: i32 = 1;
const TIER_MUTATION_PEER_STATE_COMMITTED_WIRE: i32 = 2;
const TIER_MUTATION_PEER_STATE_ABORTED_WIRE: i32 = 3;
const TIER_MUTATION_FAILURE_CLASS_UNSPECIFIED_WIRE: i32 = 0;
const TIER_MUTATION_FAILURE_CLASS_PRE_DISPATCH_REJECTED_WIRE: i32 = 1;
const TIER_MUTATION_FAILURE_CLASS_AMBIGUOUS_WIRE: i32 = 2;

fn signal_service_response(success: bool, error_info: Option<String>) -> Response<SignalServiceResponse> {
    Response::new(SignalServiceResponse {
        success,
        error_info,
        protocol_version: rustfs_protos::DYNAMIC_CONFIG_PROTOCOL_VERSION,
        config_fingerprint: None,
    })
}

/// Answer a KMS dynamic config signal.
///
/// A dry run doubles as the cluster fingerprint probe: it reports what this
/// node is running without touching its configuration. A real signal reloads
/// the cluster-persisted configuration first, and still answers with the
/// fingerprint of whatever it ends up running so a failed reload is visible as
/// divergence rather than only as an error string.
async fn kms_dynamic_config_signal_response(dry_run: bool) -> Response<SignalServiceResponse> {
    let outcome = if dry_run {
        Ok(())
    } else {
        reload_persisted_kms_config().await
    };
    let fingerprint = current_kms_config_fingerprint().await;
    Response::new(SignalServiceResponse {
        success: outcome.is_ok(),
        error_info: outcome.err(),
        protocol_version: rustfs_protos::DYNAMIC_CONFIG_PROTOCOL_VERSION,
        config_fingerprint: fingerprint,
    })
}

fn verify_node_mutation_body<T: CanonicalMutationBody>(request: &Request<T>, operation: &'static str) -> Result<(), Status> {
    let canonical_body = request
        .get_ref()
        .canonical_body()
        .map_err(|_| Status::invalid_argument(format!("{operation} request length cannot be represented")))?;
    verify_tonic_mutation_body_digest(request, &canonical_body)
        .map_err(|err| Status::permission_denied(format!("{operation} authentication failed: {err}")))
}

fn verify_node_signal_body<T: CanonicalMutationBody>(request: &Request<T>, operation: &'static str) -> Result<(), Status> {
    let canonical_body = request
        .get_ref()
        .canonical_body()
        .map_err(|_| Status::invalid_argument(format!("{operation} request length cannot be represented")))?;
    verify_tonic_mutation_body_digest_reject_unsigned(request, &canonical_body)
        .map_err(|err| Status::permission_denied(format!("{operation} authentication failed: {err}")))
}

fn start_decommission_failure_response(err: Error) -> StartDecommissionResponse {
    match err {
        Error::InvalidArgument(_, _, reason) => StartDecommissionResponse {
            success: false,
            error_info: Some(reason),
            error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorInvalidArgument as i32),
        },
        err => StartDecommissionResponse {
            success: false,
            error_info: Some(err.to_string()),
            error_code: None,
        },
    }
}

fn supports_dynamic_config_rpc(sub_system: &str) -> bool {
    NOTIFY_SUB_SYSTEMS.contains(&sub_system)
        || matches!(
            sub_system,
            STORAGE_CLASS_SUB_SYS | AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS | SCANNER_SUB_SYS | HEAL_SUB_SYS
        )
}

#[derive(Debug)]
struct HealControlReplayEntry {
    command_digest: [u8; 32],
    expires_at_unix_ms: i64,
    result: tokio::sync::Mutex<Option<Vec<u8>>>,
}

fn remove_heal_control_replay(
    replay_cache: &mut HashMap<String, Arc<HealControlReplayEntry>>,
    request_id: &str,
    replay_entry: &Arc<HealControlReplayEntry>,
) {
    if replay_cache
        .get(request_id)
        .is_some_and(|cached| Arc::ptr_eq(cached, replay_entry))
    {
        replay_cache.remove(request_id);
    }
}

static HEAL_CONTROL_REPLAY_CACHE: OnceLock<tokio::sync::Mutex<HashMap<String, Arc<HealControlReplayEntry>>>> = OnceLock::new();
static NODE_CAPABILITY_SERVER_EPOCH: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);
// v3 additionally promises the v6 tier-delete dispatch-manifest policy; v4
// promises the sticky per-target decommission capacity fence. The
// existing periodic topology probe carries both capabilities so normal object
// operations do not add another peer RPC.
const CROSS_POOL_FENCE_SUPPORTED_VERSION: u32 = 4;

fn encode_heal_capability_response(
    topology_member: &str,
    remote_version_state_probe: bool,
    recovery_export_probe: bool,
) -> Result<Vec<u8>, Status> {
    if recovery_export_probe {
        rustfs_protos::encode_remote_version_state_capability(
            topology_member,
            crate::storage::storage_api::ilm_recovery_export_local_process_epoch().as_bytes(),
        )
        .map_err(|_| Status::internal("ILM recovery export capability length cannot be represented"))
    } else if remote_version_state_probe {
        rustfs_protos::encode_remote_version_state_capability(topology_member, NODE_CAPABILITY_SERVER_EPOCH.as_bytes())
            .map_err(|_| Status::internal("remote version state capability length cannot be represented"))
    } else {
        rustfs_protos::encode_cross_pool_fence_capability(
            CROSS_POOL_FENCE_SUPPORTED_VERSION,
            topology_member,
            NODE_CAPABILITY_SERVER_EPOCH.as_bytes(),
        )
        .map_err(|_| Status::internal("cross-pool fence capability length cannot be represented"))
    }
}

fn admit_heal_control_replay(
    replay_cache: &mut HashMap<String, Arc<HealControlReplayEntry>>,
    request_id: &str,
    command_digest: &[u8; 32],
    expires_at_unix_ms: i64,
    now_unix_ms: i64,
) -> Result<Arc<HealControlReplayEntry>, Status> {
    replay_cache.retain(|_, entry| entry.expires_at_unix_ms > now_unix_ms || Arc::strong_count(entry) > 1);
    if let Some(cached) = replay_cache.get(request_id) {
        if &cached.command_digest != command_digest {
            return Err(Status::already_exists("heal control request ID was reused with a different command"));
        }
        return Ok(Arc::clone(cached));
    }
    if replay_cache.len() >= HEAL_CONTROL_REPLAY_CACHE_MAX_ENTRIES {
        return Err(Status::resource_exhausted("heal control replay cache is full"));
    }
    let entry = Arc::new(HealControlReplayEntry {
        command_digest: *command_digest,
        expires_at_unix_ms,
        result: tokio::sync::Mutex::new(None),
    });
    replay_cache.insert(request_id.to_string(), Arc::clone(&entry));
    Ok(entry)
}

fn heal_control_now_unix_ms() -> Result<i64, Status> {
    i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| Status::internal("heal control clock is out of range"))
}

fn heal_control_remaining(expires_at_unix_ms: i64, now_unix_ms: i64) -> Result<Duration, Status> {
    let remaining_ms = expires_at_unix_ms.saturating_sub(now_unix_ms);
    let remaining_ms = u64::try_from(remaining_ms).map_err(|_| Status::deadline_exceeded("heal control request expired"))?;
    if remaining_ms == 0 {
        return Err(Status::deadline_exceeded("heal control request expired"));
    }
    Ok(Duration::from_millis(remaining_ms))
}

fn validate_admin_heal_control_start(request: &rustfs_heal_contracts::heal_channel::HealChannelRequest) -> Result<(), Status> {
    if request.source != rustfs_heal_contracts::heal_channel::HealRequestSource::Admin {
        return Err(Status::permission_denied("heal control start source must be admin"));
    }
    if !request.heal_endpoints.is_empty() {
        return Err(Status::invalid_argument(
            "admin heal control start cannot contain automatic replacement endpoints",
        ));
    }
    if request.pool_index.is_some() != request.set_index.is_some() {
        return Err(Status::invalid_argument("heal control start requires both pool and set"));
    }
    if request.bucket.is_empty() {
        if request.object_prefix.as_deref().is_some_and(|prefix| !prefix.is_empty()) {
            return Err(Status::invalid_argument("root heal control start cannot contain an object prefix"));
        }
        if request.recursive != Some(true) {
            return Err(Status::invalid_argument("root heal control start must be recursive"));
        }
        let erasure_set_target = request.pool_index.is_some();
        if request.disk.is_some() != erasure_set_target {
            return Err(Status::invalid_argument("root erasure-set heal control target is inconsistent"));
        }
    } else if request.disk.is_some() {
        return Err(Status::invalid_argument(
            "bucket heal control start cannot contain an erasure-set disk target",
        ));
    }
    Ok(())
}

fn scanner_activity_response(
    namespace_generation: u64,
    topology_digest: [u8; 32],
    data_movement_active: bool,
    dirty_usage: rustfs_scanner::ScannerDirtyUsageState,
) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
        topology_digest: topology_digest.to_vec().into(),
        data_movement_active,
        response_proof: Bytes::new(),
        dirty_usage_generation: dirty_usage.generation,
        dirty_usage_pending: dirty_usage.pending,
        movement_generation: None,
        publication_blocked: None,
    }
}

async fn scanner_dirty_usage_snapshot_response(
    store: &ECStore,
    snapshot: rustfs_scanner::ScannerDirtyUsageSnapshot,
) -> Result<ScannerDirtyUsageSnapshotResponse, Status> {
    if store.id.is_nil() {
        return Err(Status::failed_precondition("scanner dirty usage snapshot owner is unavailable"));
    }
    let mut buckets = Vec::with_capacity(snapshot.buckets.len());
    for bucket in snapshot.buckets {
        let bucket_incarnation = store
            .bucket_incarnation_id_from_disk(&bucket.bucket)
            .await
            .map_err(|_| Status::failed_precondition("scanner dirty usage bucket incarnation is unavailable"))?;
        if bucket_incarnation.is_nil() {
            return Err(Status::failed_precondition("scanner dirty usage bucket incarnation is unavailable"));
        }
        buckets.push(ScannerDirtyUsageBucket {
            bucket: bucket.bucket,
            generation: bucket.generation,
            bucket_incarnation: bucket_incarnation.as_bytes().to_vec().into(),
        });
    }
    Ok(ScannerDirtyUsageSnapshotResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        generation: snapshot.generation,
        pending_bucket_count: snapshot.pending_bucket_count,
        protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
        complete: snapshot.complete,
        buckets,
        response_proof: Bytes::new(),
        owner_id: store.id.to_string(),
    })
}

fn scanner_activity_response_v7(
    namespace_generation: u64,
    topology_digest: [u8; 32],
    data_movement_active: bool,
    dirty_usage: rustfs_scanner::ScannerDirtyUsageState,
    movement_generation: u64,
    publication_blocked: bool,
) -> ScannerActivityResponse {
    let mut response = scanner_activity_response(namespace_generation, topology_digest, data_movement_active, dirty_usage);
    response.movement_generation = Some(movement_generation);
    response.publication_blocked = Some(publication_blocked);
    response
}

fn previous_scanner_activity_response(
    namespace_generation: u64,
    topology_digest: [u8; 32],
    data_movement_active: bool,
) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION,
        topology_digest: topology_digest.to_vec().into(),
        data_movement_active,
        response_proof: Bytes::new(),
        dirty_usage_generation: 0,
        dirty_usage_pending: false,
        movement_generation: None,
        publication_blocked: None,
    }
}

fn legacy_scanner_activity_response(namespace_generation: u64) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
        topology_digest: Bytes::new(),
        data_movement_active: false,
        response_proof: Bytes::new(),
        dirty_usage_generation: 0,
        dirty_usage_pending: false,
        movement_generation: None,
        publication_blocked: None,
    }
}

fn v6_scanner_activity_response(
    namespace_generation: u64,
    topology_digest: [u8; 32],
    data_movement_active: bool,
    dirty_usage: rustfs_scanner::ScannerDirtyUsageState,
) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: SCANNER_ACTIVITY_V6_PROTOCOL_VERSION,
        topology_digest: topology_digest.to_vec().into(),
        data_movement_active,
        response_proof: Bytes::new(),
        dirty_usage_generation: dirty_usage.generation,
        dirty_usage_pending: dirty_usage.pending,
        movement_generation: None,
        publication_blocked: None,
    }
}

macro_rules! log_load_rebalance_meta_rejected {
    ($reason:expr, $start_rebalance:expr) => {
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            operation = "load_rebalance_meta",
            result = "rejected",
            reason = $reason,
            start_rebalance = $start_rebalance,
            "node rpc request rejected"
        );
    };
}

macro_rules! log_load_rebalance_meta_failed {
    ($reason:expr, $start_rebalance:expr, $err:expr) => {
        error!(
            event = EVENT_RPC_REQUEST_FAILED,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            operation = "load_rebalance_meta",
            result = "failed",
            reason = $reason,
            start_rebalance = $start_rebalance,
            error = %$err,
            "node rpc request failed"
        );
    };
}

macro_rules! log_load_rebalance_meta_response_emitted {
    ($start_rebalance:expr) => {
        info!(
            event = EVENT_RPC_RESPONSE_EMITTED,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            operation = "load_rebalance_meta",
            result = "success",
            start_rebalance = $start_rebalance,
            "node rpc response emitted"
        );
    };
}

macro_rules! log_background_rebalance_task_spawned {
    ($start_rebalance:expr) => {
        info!(
            event = EVENT_RPC_BACKGROUND_TASK_SPAWNED,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            operation = "start_rebalance",
            state = "spawned",
            start_rebalance = $start_rebalance,
            "node rpc background task spawned"
        );
    };
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn unimplemented_rpc(method: &str) -> Status {
    Status::unimplemented(format!("{method} is not implemented"))
}

fn background_rebalance_start_error_message(result: StorageResult<()>) -> Option<String> {
    result.err().map(|err| format!("start_rebalance failed: {err}"))
}

fn stop_rebalance_response(result: StorageResult<()>) -> StopRebalanceResponse {
    match result {
        Ok(_) => StopRebalanceResponse {
            error_code: None,
            success: true,
            error_info: None,
        },
        Err(err) => StopRebalanceResponse {
            error_code: None,
            success: false,
            error_info: Some(err.to_string()),
        },
    }
}

fn ensure_rpc_decommission_local_leader(store: &ECStore, idx: usize) -> StorageResult<()> {
    let endpoints = store.endpoints();
    let endpoint = endpoints
        .as_ref()
        .get(idx)
        .and_then(|pool| pool.endpoints.as_ref().first())
        .ok_or_else(|| Error::other(format!("invalid decommission pool index {idx} for {} pools", endpoints.as_ref().len())))?;

    if !endpoint.is_local {
        return Err(Error::other(format!(
            "decommission for pool {idx} must run on the pool first endpoint {endpoint}"
        )));
    }

    Ok(())
}

mod bucket;
mod disk;
mod event;
mod health;
mod lock;
mod metrics;

pub struct NodeService {
    local_peer: LocalPeerS3Client,
    context: Option<Arc<runtime_sources::AppContext>>,
}

impl std::fmt::Debug for NodeService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeService")
            .field("local_peer", &self.local_peer)
            .field("context_present", &self.context.is_some())
            .finish()
    }
}

pub(crate) fn make_scanner_control_server() -> scanner_control_service_server::ScannerControlServiceServer<NodeService> {
    let limit = rustfs_protos::scoped_dirty_usage::SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES as usize;
    scanner_control_service_server::ScannerControlServiceServer::new(make_server())
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit)
}

pub fn make_server() -> NodeService {
    let context = runtime_sources::current_app_context();
    make_server_for_context(context)
}

pub fn make_server_for_context(context: Option<Arc<runtime_sources::AppContext>>) -> NodeService {
    let local_peer = LocalPeerS3Client::new(None, None);
    NodeService { local_peer, context }
}

#[derive(Clone, Debug, Default)]
pub struct HealControlRpcService {
    #[cfg(test)]
    endpoint_pools: Option<EndpointServerPools>,
    topology_fingerprint: Arc<tokio::sync::OnceCell<String>>,
    #[cfg(test)]
    endpoint_pools_source: Option<Arc<tokio::sync::RwLock<Option<EndpointServerPools>>>>,
}

pub fn make_heal_control_server() -> HealControlRpcService {
    make_heal_control_server_with_cache(Arc::new(tokio::sync::OnceCell::new()))
}

pub(crate) fn make_heal_control_server_with_cache(
    topology_fingerprint: Arc<tokio::sync::OnceCell<String>>,
) -> HealControlRpcService {
    HealControlRpcService {
        topology_fingerprint,
        #[cfg(test)]
        endpoint_pools: None,
        #[cfg(test)]
        endpoint_pools_source: None,
    }
}

#[cfg(test)]
pub(crate) fn make_heal_control_server_for_source()
-> (HealControlRpcService, Arc<tokio::sync::RwLock<Option<EndpointServerPools>>>) {
    let source = Arc::new(tokio::sync::RwLock::new(None));
    (
        HealControlRpcService {
            endpoint_pools: None,
            topology_fingerprint: Arc::new(tokio::sync::OnceCell::new()),
            endpoint_pools_source: Some(Arc::clone(&source)),
        },
        source,
    )
}

impl HealControlRpcService {
    async fn capability_fingerprint(&self) -> Result<&str, Status> {
        if let Some(fingerprint) = self.topology_fingerprint.get() {
            return Ok(fingerprint);
        }

        #[cfg(test)]
        {
            return self
                .topology_fingerprint
                .get_or_try_init(|| async {
                    let endpoint_pools = self
                        .endpoint_pools()
                        .await
                        .ok_or_else(|| Status::failed_precondition("heal control topology is not initialized"))?;
                    tokio::task::spawn_blocking(move || heal::heal_topology_fingerprint(&endpoint_pools))
                        .await
                        .map_err(|_| Status::internal("heal control topology calculation failed"))?
                        .map_err(|_| Status::failed_precondition("heal control topology is invalid"))
                })
                .await
                .map(String::as_str);
        }

        #[cfg(not(test))]
        Err(Status::failed_precondition("heal control topology is not initialized"))
    }

    async fn endpoint_pools(&self) -> Option<EndpointServerPools> {
        #[cfg(test)]
        if let Some(source) = self.endpoint_pools_source.as_ref() {
            return source.read().await.clone();
        }
        #[cfg(test)]
        if self.endpoint_pools.is_some() {
            return self.endpoint_pools.clone();
        }
        let context = runtime_sources::current_app_context()?;
        context.endpoints().handle()
    }
}

pub(crate) async fn initialize_heal_topology_fingerprint(
    cache: Arc<tokio::sync::OnceCell<String>>,
    endpoint_pools: EndpointServerPools,
) -> Result<(), String> {
    initialize_heal_topology_fingerprint_with_probe(
        cache,
        endpoint_pools,
        crate::storage::storage_api::start_remote_version_state_fleet_probe,
    )
    .await
}

async fn initialize_heal_topology_fingerprint_with_probe(
    cache: Arc<tokio::sync::OnceCell<String>>,
    endpoint_pools: EndpointServerPools,
    start_probe: impl FnOnce(String),
) -> Result<(), String> {
    if cache.get().is_some() {
        return Ok(());
    }
    let fingerprint = tokio::task::spawn_blocking(move || heal::heal_topology_fingerprint(&endpoint_pools))
        .await
        .map_err(|_| "heal control topology calculation task failed".to_string())??;
    let _ = cache.set(fingerprint.clone());
    start_probe(fingerprint);
    Ok(())
}

pub(crate) async fn execute_heal_control_envelope(
    envelope: rustfs_protos::heal_control::Envelope,
    expected_coordinator_epoch: u64,
) -> Result<Vec<u8>, Status> {
    execute_heal_control_envelope_with_manager(envelope, expected_coordinator_epoch, None).await
}

async fn execute_heal_control_envelope_with_manager(
    envelope: rustfs_protos::heal_control::Envelope,
    expected_coordinator_epoch: u64,
    manager: Option<Arc<rustfs_heal::HealManager>>,
) -> Result<Vec<u8>, Status> {
    let now = heal_control_now_unix_ms()?;
    envelope
        .validate_execution(now, expected_coordinator_epoch)
        .map_err(Status::failed_precondition)?;
    let expires_at_unix_ms = envelope.expires_at_unix_ms();
    let canonical_envelope = rustfs_protos::heal_control::encode_envelope(&envelope).map_err(Status::invalid_argument)?;
    let command_digest = Sha256::digest(&canonical_envelope).into();
    let (request_id, coordinator_epoch, command) = envelope.into_execution().map_err(Status::invalid_argument)?;

    let replay_cache = HEAL_CONTROL_REPLAY_CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()));
    let replay_entry = {
        let mut replay_cache = timeout(heal_control_remaining(expires_at_unix_ms, now)?, replay_cache.lock())
            .await
            .map_err(|_| Status::deadline_exceeded("heal control request expired while awaiting replay admission"))?;
        admit_heal_control_replay(&mut replay_cache, &request_id, &command_digest, expires_at_unix_ms, now)?
    };
    let mut replay_result = timeout(heal_control_remaining(expires_at_unix_ms, now)?, replay_entry.result.lock())
        .await
        .map_err(|_| Status::deadline_exceeded("heal control request expired while awaiting matching execution"))?;
    let now = heal_control_now_unix_ms()?;
    if expires_at_unix_ms <= now {
        return Err(Status::deadline_exceeded("heal control request expired before execution"));
    }
    if let Some(cached) = replay_result.as_ref() {
        return Ok(cached.clone());
    }

    if let rustfs_protos::heal_control::ExecutableCommand::Start { request } = &command {
        validate_admin_heal_control_start(request)?;
    }
    let retain_completed_result = !matches!(&command, rustfs_protos::heal_control::ExecutableCommand::Query { .. });

    let manager = manager
        .or_else(|| rustfs_heal::get_heal_manager().cloned())
        .ok_or_else(|| Status::failed_precondition("heal manager is not initialized"))?;
    let processor = rustfs_heal::HealChannelProcessor::new(manager.clone());
    let remaining = heal_control_remaining(expires_at_unix_ms, now)?;
    let outcome = match command {
        rustfs_protos::heal_control::ExecutableCommand::Start { request } => {
            let receipt = timeout(remaining, processor.execute_start_request(request))
                .await
                .map_err(|_| Status::deadline_exceeded("heal control start expired before admission"))?
                .map_err(|_| Status::internal("heal control start admission failed"))?;
            rustfs_protos::heal_control::Outcome::Start {
                task_id: receipt.task_id,
                admission: receipt.result.into(),
            }
        }
        rustfs_protos::heal_control::ExecutableCommand::Query {
            heal_path,
            client_token,
            since_seq,
        } => {
            let response = timeout(remaining, processor.execute_query_request_since(heal_path, client_token, since_seq))
                .await
                .map_err(|_| Status::deadline_exceeded("heal control query expired before execution"))?
                .map_err(|_| Status::internal("heal control query failed"))?;
            rustfs_protos::heal_control::Outcome::Channel {
                success: response.success,
                data: response.data,
                error: response.error,
            }
        }
        rustfs_protos::heal_control::ExecutableCommand::Cancel { heal_path, client_token } => {
            let response = timeout(remaining, processor.execute_cancel_request(heal_path, client_token))
                .await
                .map_err(|_| Status::deadline_exceeded("heal control cancel expired before execution"))?
                .map_err(|_| Status::internal("heal control cancel failed"))?;
            rustfs_protos::heal_control::Outcome::Channel {
                success: response.success,
                data: response.data,
                error: response.error,
            }
        }
    };
    let result = rustfs_protos::heal_control::ResultEnvelope::new(request_id.clone(), coordinator_epoch, outcome)
        .and_then(|result| rustfs_protos::heal_control::encode_result(&result))
        .map_err(Status::internal)?;
    *replay_result = Some(result.clone());
    if !retain_completed_result {
        let mut replay_cache = replay_cache.lock().await;
        remove_heal_control_replay(&mut replay_cache, &request_id, &replay_entry);
    }
    Ok(result)
}

#[derive(Clone, Default)]
pub struct TierMutationControlRpcService {
    context: Option<Arc<runtime_sources::AppContext>>,
}

impl std::fmt::Debug for TierMutationControlRpcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierMutationControlRpcService")
            .field("context_present", &self.context.is_some())
            .finish()
    }
}

pub fn make_tier_mutation_control_server() -> TierMutationControlRpcService {
    TierMutationControlRpcService {
        context: runtime_sources::current_app_context(),
    }
}

#[cfg(test)]
pub(crate) fn make_tier_mutation_control_server_for_context(
    context: Option<Arc<runtime_sources::AppContext>>,
) -> TierMutationControlRpcService {
    TierMutationControlRpcService { context }
}

impl TierMutationControlRpcService {
    fn resolve_object_store(&self) -> Option<Arc<ECStore>> {
        let context = self.context.clone().or_else(runtime_sources::current_app_context);
        runtime_sources::current_object_store_handle_for_context(context.as_deref())
    }

    async fn execute_tier_mutation(
        &self,
        request: &Request<()>,
        version: u32,
        phase: rustfs_protos::TierMutationRpcPhase,
        mutation_id: &str,
        canonical_payload: &Bytes,
    ) -> Result<Response<TierMutationControlResponse>, Status> {
        validate_tier_mutation_payload_size(phase, canonical_payload.len())?;
        let mutation_id = parse_tier_mutation_id(mutation_id)?;
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(version, phase, mutation_id, canonical_payload)
            .map_err(|_| Status::invalid_argument("tier mutation request length cannot be represented"))?;
        verify_tonic_canonical_body_digest(request, &body)
            .map_err(|err| Status::permission_denied(format!("tier mutation authentication failed: {err}")))?;
        if !tier_mutation_protocol_version_is_supported(version) {
            return Err(Status::failed_precondition(format!(
                "unsupported tier mutation peer protocol version: {version}"
            )));
        }
        let store = match self.resolve_object_store() {
            Some(store) => store,
            None if version == rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION => {
                return tier_mutation_control_response(TierMutationControlResponseInput {
                    version,
                    phase,
                    mutation_id,
                    canonical_payload,
                    success: false,
                    state: TIER_MUTATION_PEER_STATE_UNSPECIFIED_WIRE,
                    applied: false,
                    error_info: Some("tier mutation object store is not initialized".to_string()),
                    failure_class: TIER_MUTATION_FAILURE_CLASS_PRE_DISPATCH_REJECTED_WIRE,
                });
            }
            None => return Err(Status::failed_precondition("tier mutation object store is not initialized")),
        };

        match tier_mutation_peer::handle_tier_mutation_peer_request(store, version, phase, mutation_id, canonical_payload).await {
            Ok(outcome) => tier_mutation_control_response(TierMutationControlResponseInput {
                version,
                phase,
                mutation_id,
                canonical_payload,
                success: true,
                state: tier_mutation_peer_state_to_proto_wire(outcome.state),
                applied: outcome.applied,
                error_info: None,
                failure_class: TIER_MUTATION_FAILURE_CLASS_UNSPECIFIED_WIRE,
            }),
            Err(err) => tier_mutation_control_response(TierMutationControlResponseInput {
                version,
                phase,
                mutation_id,
                canonical_payload,
                success: false,
                state: TIER_MUTATION_PEER_STATE_UNSPECIFIED_WIRE,
                applied: false,
                error_info: Some(bounded_tier_mutation_error_info(err.to_string())),
                failure_class: if version == rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION {
                    TIER_MUTATION_FAILURE_CLASS_AMBIGUOUS_WIRE
                } else {
                    TIER_MUTATION_FAILURE_CLASS_UNSPECIFIED_WIRE
                },
            }),
        }
    }
}

#[tonic::async_trait]
impl tier_mutation_control_service_server::TierMutationControlService for TierMutationControlRpcService {
    async fn prepare_tier_mutation(
        &self,
        request: Request<TierMutationPrepareRequest>,
    ) -> Result<Response<TierMutationControlResponse>, Status> {
        let (metadata, extensions, inner) = request.into_parts();
        let request = Request::from_parts(metadata, extensions, ());
        self.execute_tier_mutation(
            &request,
            inner.version,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            &inner.mutation_id,
            &inner.canonical_payload,
        )
        .await
    }

    async fn commit_tier_mutation(
        &self,
        request: Request<TierMutationCommitRequest>,
    ) -> Result<Response<TierMutationControlResponse>, Status> {
        let (metadata, extensions, inner) = request.into_parts();
        let request = Request::from_parts(metadata, extensions, ());
        self.execute_tier_mutation(
            &request,
            inner.version,
            rustfs_protos::TierMutationRpcPhase::Commit,
            &inner.mutation_id,
            &inner.canonical_payload,
        )
        .await
    }

    async fn abort_tier_mutation(
        &self,
        request: Request<TierMutationAbortRequest>,
    ) -> Result<Response<TierMutationControlResponse>, Status> {
        let (metadata, extensions, inner) = request.into_parts();
        let request = Request::from_parts(metadata, extensions, ());
        self.execute_tier_mutation(
            &request,
            inner.version,
            rustfs_protos::TierMutationRpcPhase::Abort,
            &inner.mutation_id,
            &inner.canonical_payload,
        )
        .await
    }
}

fn parse_tier_mutation_id(mutation_id: &str) -> Result<Uuid, Status> {
    let parsed = Uuid::parse_str(mutation_id).map_err(|_| Status::invalid_argument("tier mutation id is invalid"))?;
    if parsed.to_string() != mutation_id {
        return Err(Status::invalid_argument("tier mutation id is not canonical"));
    }
    Ok(parsed)
}

fn tier_mutation_protocol_version_is_supported(version: u32) -> bool {
    matches!(
        version,
        rustfs_protos::TIER_MUTATION_RPC_PREVIOUS_PROTOCOL_VERSION | rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION
    )
}

fn bounded_tier_mutation_error_info(mut error_info: String) -> String {
    let limit = rustfs_protos::TIER_MUTATION_RPC_MAX_ERROR_INFO_SIZE;
    if error_info.len() <= limit {
        return error_info;
    }
    let mut boundary = limit;
    while !error_info.is_char_boundary(boundary) {
        boundary -= 1;
    }
    error_info.truncate(boundary);
    error_info
}

fn validate_tier_mutation_payload_size(phase: rustfs_protos::TierMutationRpcPhase, payload_len: usize) -> Result<(), Status> {
    let limit = match phase {
        rustfs_protos::TierMutationRpcPhase::Prepare => rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE,
        rustfs_protos::TierMutationRpcPhase::Commit => rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE,
        rustfs_protos::TierMutationRpcPhase::Abort => {
            if payload_len == 0 {
                return Err(Status::invalid_argument("tier mutation abort payload is empty"));
            }
            rustfs_protos::TIER_MUTATION_RPC_MAX_ABORT_PAYLOAD_SIZE
        }
        _ => return Err(Status::invalid_argument("tier mutation rpc phase is unsupported")),
    };
    if payload_len > limit {
        return Err(Status::invalid_argument("tier mutation payload exceeds size limit"));
    }
    Ok(())
}

struct TierMutationControlResponseInput<'a> {
    version: u32,
    phase: rustfs_protos::TierMutationRpcPhase,
    mutation_id: Uuid,
    canonical_payload: &'a [u8],
    success: bool,
    state: i32,
    applied: bool,
    error_info: Option<String>,
    failure_class: i32,
}

fn tier_mutation_control_response(
    input: TierMutationControlResponseInput<'_>,
) -> Result<Response<TierMutationControlResponse>, Status> {
    let canonical_response =
        rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
            version: input.version,
            phase: input.phase,
            mutation_id: input.mutation_id,
            canonical_payload: input.canonical_payload,
            success: input.success,
            state: input.state,
            applied: input.applied,
            error_info: input.error_info.as_deref(),
            failure_class: input.failure_class,
        })
        .map_err(|_| Status::internal("tier mutation response length cannot be represented"))?;
    let response_proof = sign_tonic_rpc_response_proof(&canonical_response)
        .map_err(|_| Status::internal("tier mutation response proof is unavailable"))?;
    Ok(Response::new(TierMutationControlResponse {
        success: input.success,
        state: input.state,
        applied: input.applied,
        error_info: input.error_info,
        response_proof: response_proof.into(),
        failure_class: input.failure_class,
    }))
}

fn tier_mutation_peer_state_to_proto_wire(state: EcTierMutationPeerState) -> i32 {
    match state {
        EcTierMutationPeerState::Prepared => TIER_MUTATION_PEER_STATE_PREPARED_WIRE,
        EcTierMutationPeerState::Committed => TIER_MUTATION_PEER_STATE_COMMITTED_WIRE,
        EcTierMutationPeerState::Aborted => TIER_MUTATION_PEER_STATE_ABORTED_WIRE,
    }
}

#[tonic::async_trait]
impl heal_control_service_server::HealControlService for HealControlRpcService {
    async fn heal_control(&self, request: Request<HealControlRequest>) -> Result<Response<HealControlResponse>, Status> {
        if request.get_ref().topology_fingerprint.len() > HEAL_CONTROL_FINGERPRINT_MAX_SIZE
            || request.get_ref().command.len() > HEAL_CONTROL_PAYLOAD_MAX_SIZE
        {
            return Err(Status::invalid_argument("heal control request exceeds size limit"));
        }
        let body = rustfs_protos::canonical_heal_control_request_body(
            request.get_ref().version,
            &request.get_ref().topology_fingerprint,
            &request.get_ref().command,
        )
        .map_err(|_| Status::invalid_argument("heal control request length cannot be represented"))?;
        verify_tonic_canonical_body_digest(&request, &body)
            .map_err(|err| Status::permission_denied(format!("heal control authentication failed: {err}")))?;
        if request.get_ref().version != rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION {
            return Err(Status::failed_precondition("unsupported heal control protocol version"));
        }
        let fingerprint = self.capability_fingerprint().await?;
        if request.get_ref().topology_fingerprint != *fingerprint {
            return Err(Status::failed_precondition("heal control topology does not match"));
        }
        if rustfs_protos::is_heal_control_capability_probe(&request.get_ref().command) {
            let canonical_ack = rustfs_protos::canonical_heal_control_capability_ack(
                request.get_ref().version,
                fingerprint,
                &request.get_ref().command,
            )
            .map_err(|_| Status::internal("heal control acknowledgement length cannot be represented"))?;
            let result = sign_tonic_rpc_response_proof(&canonical_ack)
                .map_err(|_| Status::internal("heal control response proof is unavailable"))?;
            return Ok(Response::new(HealControlResponse {
                success: true,
                result: result.into(),
                error_info: None,
                response_proof: Bytes::new(),
            }));
        }
        let remote_version_state_probe = rustfs_protos::is_remote_version_state_capability_probe(&request.get_ref().command);
        let cross_pool_fence_probe = rustfs_protos::is_cross_pool_fence_capability_probe(&request.get_ref().command);
        let recovery_export_probe = rustfs_protos::is_ilm_recovery_export_capability_probe(&request.get_ref().command);
        if remote_version_state_probe || cross_pool_fence_probe || recovery_export_probe {
            let topology_member = self
                .endpoint_pools()
                .await
                .ok_or_else(|| Status::failed_precondition("heal control topology is not initialized"))?
                .peers()
                .1;
            if topology_member.is_empty() {
                return Err(Status::failed_precondition("local topology member identity is unavailable"));
            }
            let result = encode_heal_capability_response(&topology_member, remote_version_state_probe, recovery_export_probe)?;
            let canonical_response = rustfs_protos::canonical_heal_control_response_body(
                request.get_ref().version,
                &request.get_ref().topology_fingerprint,
                &request.get_ref().command,
                &result,
            )
            .map_err(|_| Status::internal("heal control response length cannot be represented"))?;
            let response_proof = sign_tonic_rpc_response_proof(&canonical_response)
                .map_err(|_| Status::internal("heal control response proof is unavailable"))?;
            return Ok(Response::new(HealControlResponse {
                success: true,
                result: result.into(),
                error_info: None,
                response_proof: response_proof.into(),
            }));
        }
        let endpoints = self
            .endpoint_pools()
            .await
            .ok_or_else(|| Status::failed_precondition("heal control topology is not initialized"))?;
        if !heal::heal_control_coordinator(&endpoints)
            .map_err(Status::failed_precondition)?
            .is_local
        {
            return Err(Status::failed_precondition("heal control request reached a non-coordinator node"));
        }
        let envelope =
            rustfs_protos::heal_control::decode_envelope(&request.get_ref().command).map_err(Status::invalid_argument)?;
        let coordinator_epoch =
            rustfs_protos::heal_control_coordinator_epoch(fingerprint).map_err(Status::failed_precondition)?;
        let result = execute_heal_control_envelope(envelope, coordinator_epoch).await?;
        let canonical_response = rustfs_protos::canonical_heal_control_response_body(
            request.get_ref().version,
            &request.get_ref().topology_fingerprint,
            &request.get_ref().command,
            &result,
        )
        .map_err(|_| Status::internal("heal control response length cannot be represented"))?;
        let response_proof = sign_tonic_rpc_response_proof(&canonical_response)
            .map_err(|_| Status::internal("heal control response proof is unavailable"))?;
        Ok(Response::new(HealControlResponse {
            success: true,
            result: result.into(),
            error_info: None,
            response_proof: response_proof.into(),
        }))
    }
}

impl NodeService {
    fn resolve_object_store(&self) -> Option<Arc<ECStore>> {
        let context = self.context.clone().or_else(runtime_sources::current_app_context);
        runtime_sources::current_object_store_handle_for_context(context.as_deref())
    }

    async fn find_disk(&self, disk_path: &str) -> Option<DiskStore> {
        find_local_disk_by_ref(disk_path).await
    }

    async fn all_disk(&self) -> Vec<String> {
        all_local_disk_path().await
    }

    /// Get the lock client, returning an error if not initialized
    fn get_lock_client(&self) -> Result<Arc<dyn LockClient>, Status> {
        runtime_sources::current_lock_client()
            .ok_or_else(|| Status::internal("Lock client not initialized. Please ensure storage is initialized first."))
    }
}

#[tonic::async_trait]
impl scanner_control_service_server::ScannerControlService for NodeService {
    async fn scanner_scoped_dirty_usage_ack(
        &self,
        request: Request<ScannerScopedDirtyUsageAckRequest>,
    ) -> Result<Response<ScannerScopedDirtyUsageAckResponse>, Status> {
        use rustfs_protos::scoped_dirty_usage::*;
        static ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

        let canonical =
            canonical_scoped_dirty_usage_request(request.get_ref()).map_err(|err| Status::invalid_argument(err.to_string()))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|_| Status::permission_denied("scoped dirty usage authentication failed"))?;
        let _admission = ADMISSION
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("scoped dirty usage receiver is busy"))?;
        let request = request.into_inner();
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        if store.id.is_nil()
            || request.owner_id != store.id.to_string()
            || request.instance_id != rustfs_scanner::scanner_activity_epoch()
        {
            return Err(Status::failed_precondition("scoped dirty usage peer or process changed"));
        }
        let cleared = timeout(Duration::from_secs(30), async {
            // Strict bucket order is validated before admission. Acquire every
            // lifecycle/metadata fence before clearing any dirty record.
            let mut guards = Vec::with_capacity(request.entries.len());
            for entry in &request.entries {
                let incarnation = Uuid::from_slice(entry.bucket_incarnation.as_ref())
                    .map_err(|_| Status::invalid_argument("invalid bucket incarnation"))?;
                let guard =
                    crate::storage::storage_api::acquire_scanner_bucket_incarnation_fence(&entry.bucket, incarnation, store.id)
                        .await
                        .map_err(|_| Status::failed_precondition("trusted bucket incarnation is unavailable"))?;
                guards.push(guard);
            }
            let entries = guards
                .iter()
                .zip(&request.entries)
                .map(|(guard, entry)| (guard, entry.generation))
                .collect::<Vec<_>>();
            rustfs_scanner::acknowledge_scoped_dirty_usage(&request.instance_id, &entries, request.probe_only)
                .map_err(|err| Status::failed_precondition(err.to_string()))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("scoped dirty usage incarnation validation timed out"))??;
        let mut response = ScannerScopedDirtyUsageAckResponse {
            protocol_version: SCOPED_DIRTY_USAGE_PROTOCOL_VERSION,
            owner_id: request.owner_id,
            instance_id: request.instance_id,
            supported: true,
            max_entries: SCOPED_DIRTY_USAGE_MAX_ENTRIES,
            max_request_bytes: SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES,
            cleared,
            response_proof: Bytes::new(),
        };
        let body = canonical_scoped_dirty_usage_response(&canonical, &response)
            .map_err(|_| Status::internal("scoped dirty usage response is too large"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&body)
            .map_err(|_| Status::unavailable("scoped dirty usage response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let ping_req = request.into_inner();
        if ping_req.body.is_empty() {
            debug!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_RPC,
                event = "ping_request",
                request_type = "liveness_probe",
                "RPC ping request received"
            );
        } else {
            let ping_body = flatbuffers::root::<PingBody>(&ping_req.body);
            if let Err(e) = ping_body {
                warn!(
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_RPC,
                    event = "ping_request_decode_failed",
                    error = %e,
                    "Failed to decode RPC ping request body"
                );
            }
        }

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello, caller");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        Ok(Response::new(PingResponse {
            version: 1,
            body: Bytes::copy_from_slice(finished_data),
        }))
    }

    async fn heal_bucket(&self, request: Request<HealBucketRequest>) -> Result<Response<HealBucketResponse>, Status> {
        verify_node_mutation_body(&request, "heal bucket")?;
        self.handle_heal_bucket(request).await
    }

    async fn list_bucket(&self, request: Request<ListBucketRequest>) -> Result<Response<ListBucketResponse>, Status> {
        self.handle_list_bucket(request).await
    }

    async fn make_bucket(&self, request: Request<MakeBucketRequest>) -> Result<Response<MakeBucketResponse>, Status> {
        verify_node_mutation_body(&request, "make bucket")?;
        self.handle_make_bucket(request).await
    }

    async fn get_bucket_info(&self, request: Request<GetBucketInfoRequest>) -> Result<Response<GetBucketInfoResponse>, Status> {
        self.handle_get_bucket_info(request).await
    }

    async fn delete_bucket(&self, request: Request<DeleteBucketRequest>) -> Result<Response<DeleteBucketResponse>, Status> {
        verify_node_mutation_body(&request, "delete bucket")?;
        self.handle_delete_bucket(request).await
    }

    async fn read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        self.handle_read_all(request).await
    }

    async fn write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        self.handle_write_all(request).await
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        self.handle_delete(request).await
    }

    async fn verify_file(&self, request: Request<VerifyFileRequest>) -> Result<Response<VerifyFileResponse>, Status> {
        self.handle_verify_file(request).await
    }
    async fn read_parts(&self, request: Request<ReadPartsRequest>) -> Result<Response<ReadPartsResponse>, Status> {
        self.handle_read_parts(request).await
    }
    async fn check_parts(&self, request: Request<CheckPartsRequest>) -> Result<Response<CheckPartsResponse>, Status> {
        self.handle_check_parts(request).await
    }

    async fn prepare_part_transaction(
        &self,
        request: Request<PreparePartTransactionRequest>,
    ) -> Result<Response<PreparePartTransactionResponse>, Status> {
        self.handle_prepare_part_transaction(request).await
    }

    async fn rename_part(&self, request: Request<RenamePartRequest>) -> Result<Response<RenamePartResponse>, Status> {
        self.handle_rename_part(request).await
    }

    async fn settle_part_transaction(
        &self,
        request: Request<SettlePartTransactionRequest>,
    ) -> Result<Response<SettlePartTransactionResponse>, Status> {
        self.handle_settle_part_transaction(request).await
    }

    async fn rename_file(&self, request: Request<RenameFileRequest>) -> Result<Response<RenameFileResponse>, Status> {
        self.handle_rename_file(request).await
    }

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        self.handle_write(request).await
    }

    type WriteStreamStream = ResponseStream<WriteResponse>;
    async fn write_stream(&self, request: Request<Streaming<WriteRequest>>) -> Result<Response<Self::WriteStreamStream>, Status> {
        let _ = request;

        Err(unimplemented_rpc("write_stream"))
    }

    type ReadAtStream = ResponseStream<ReadAtResponse>;
    async fn read_at(&self, _request: Request<Streaming<ReadAtRequest>>) -> Result<Response<Self::ReadAtStream>, Status> {
        Err(unimplemented_rpc("read_at"))
    }

    async fn list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        self.handle_list_dir(request).await
    }

    type WalkDirStream = ResponseStream<WalkDirResponse>;
    async fn walk_dir(&self, request: Request<WalkDirRequest>) -> Result<Response<Self::WalkDirStream>, Status> {
        let request = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        if let Some(disk) = self.find_disk(&request.disk).await {
            let mut buf = Deserializer::new(Cursor::new(request.walk_dir_options));
            let opts = match Deserialize::deserialize(&mut buf) {
                Ok(options) => options,
                Err(_) => {
                    return Err(Status::invalid_argument("invalid WalkDirOptions"));
                }
            };
            spawn(async {
                let (rd, mut wr) = tokio::io::duplex(64);
                let job1 = spawn(async move {
                    if let Err(err) = disk.walk_dir(opts, &mut wr).await {
                        error!(
                            component = LOG_COMPONENT_STORAGE,
                            subsystem = LOG_SUBSYSTEM_RPC,
                            event = "walk_dir_failed",
                            error = ?err,
                            "walk_dir RPC failed"
                        );
                    }
                });
                let job2 = spawn(async move {
                    let mut reader = MetacacheReader::new(rd);

                    loop {
                        match reader.peek().await {
                            Ok(res) => {
                                if let Some(info) = res {
                                    match serde_json::to_string(&info) {
                                        Ok(meta_cache_entry) => {
                                            if tx
                                                .send(Ok(WalkDirResponse {
                                                    success: true,
                                                    meta_cache_entry,
                                                    error_info: None,
                                                }))
                                                .await
                                                .is_err()
                                            {
                                                warn!(
                                                    component = LOG_COMPONENT_STORAGE,
                                                    subsystem = LOG_SUBSYSTEM_RPC,
                                                    event = "walk_dir_stream_closed",
                                                    stage = "entry_send",
                                                    "walk_dir stream receiver dropped"
                                                );
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            if tx
                                                .send(Ok(WalkDirResponse {
                                                    success: false,
                                                    meta_cache_entry: "".to_string(),
                                                    error_info: Some(e.to_string()),
                                                }))
                                                .await
                                                .is_err()
                                            {
                                                warn!(
                                                    component = LOG_COMPONENT_STORAGE,
                                                    subsystem = LOG_SUBSYSTEM_RPC,
                                                    event = "walk_dir_stream_closed",
                                                    stage = "serialization_error_send",
                                                    "walk_dir stream receiver dropped"
                                                );
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    break;
                                }
                            }
                            Err(err) => {
                                if err == rustfs_filemeta::Error::Unexpected {
                                    let _ = tx
                                        .send(Ok(WalkDirResponse {
                                            success: false,
                                            meta_cache_entry: "".to_string(),
                                            error_info: Some(err.to_string()),
                                        }))
                                        .await;

                                    break;
                                }

                                if rustfs_filemeta::is_io_eof(&err) {
                                    let _ = tx
                                        .send(Ok(WalkDirResponse {
                                            success: false,
                                            meta_cache_entry: "".to_string(),
                                            error_info: Some(err.to_string()),
                                        }))
                                        .await;

                                    break;
                                }

                                warn!(
                                    component = LOG_COMPONENT_STORAGE,
                                    subsystem = LOG_SUBSYSTEM_RPC,
                                    event = "walk_dir_metacache_read_failed",
                                    error = ?err,
                                    "walk_dir metacache read failed"
                                );

                                let _ = tx
                                    .send(Ok(WalkDirResponse {
                                        success: false,
                                        meta_cache_entry: "".to_string(),
                                        error_info: Some(err.to_string()),
                                    }))
                                    .await;
                                break;
                            }
                        }
                    }
                });
                join_all(vec![job1, job2]).await;
            });
        } else {
            return Err(Status::invalid_argument(format!("invalid disk, all disk: {:?}", self.all_disk().await)));
        }

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn rename_data(&self, request: Request<RenameDataRequest>) -> Result<Response<RenameDataResponse>, Status> {
        self.handle_rename_data(request).await
    }

    async fn make_volumes(&self, request: Request<MakeVolumesRequest>) -> Result<Response<MakeVolumesResponse>, Status> {
        self.handle_make_volumes(request).await
    }

    async fn make_volume(&self, request: Request<MakeVolumeRequest>) -> Result<Response<MakeVolumeResponse>, Status> {
        self.handle_make_volume(request).await
    }

    async fn list_volumes(&self, request: Request<ListVolumesRequest>) -> Result<Response<ListVolumesResponse>, Status> {
        self.handle_list_volumes(request).await
    }

    async fn stat_volume(&self, request: Request<StatVolumeRequest>) -> Result<Response<StatVolumeResponse>, Status> {
        self.handle_stat_volume(request).await
    }

    async fn delete_paths(&self, request: Request<DeletePathsRequest>) -> Result<Response<DeletePathsResponse>, Status> {
        self.handle_delete_paths(request).await
    }
    async fn acquire_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseRequest>,
    ) -> Result<Response<SnapshotLeaseResponse>, Status> {
        self.handle_acquire_snapshot_lease(request).await
    }
    async fn renew_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseRenewRequest>,
    ) -> Result<Response<SnapshotLeaseResponse>, Status> {
        self.handle_renew_snapshot_lease(request).await
    }
    async fn release_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseReleaseRequest>,
    ) -> Result<Response<SnapshotLeaseMutationResponse>, Status> {
        self.handle_release_snapshot_lease(request).await
    }
    async fn read_metadata(&self, request: Request<ReadMetadataRequest>) -> Result<Response<ReadMetadataResponse>, Status> {
        self.handle_read_metadata(request).await
    }

    async fn update_metadata(&self, request: Request<UpdateMetadataRequest>) -> Result<Response<UpdateMetadataResponse>, Status> {
        self.handle_update_metadata(request).await
    }

    async fn write_metadata(&self, request: Request<WriteMetadataRequest>) -> Result<Response<WriteMetadataResponse>, Status> {
        self.handle_write_metadata(request).await
    }

    async fn read_version(&self, request: Request<ReadVersionRequest>) -> Result<Response<ReadVersionResponse>, Status> {
        self.handle_read_version(request).await
    }

    async fn batch_read_version(
        &self,
        request: Request<BatchReadVersionRequest>,
    ) -> Result<Response<BatchReadVersionResponse>, Status> {
        self.handle_batch_read_version(request).await
    }

    async fn read_xl(&self, request: Request<ReadXlRequest>) -> Result<Response<ReadXlResponse>, Status> {
        self.handle_read_xl(request).await
    }

    async fn delete_version(&self, request: Request<DeleteVersionRequest>) -> Result<Response<DeleteVersionResponse>, Status> {
        self.handle_delete_version(request).await
    }

    async fn delete_versions(&self, request: Request<DeleteVersionsRequest>) -> Result<Response<DeleteVersionsResponse>, Status> {
        self.handle_delete_versions(request).await
    }

    async fn read_multiple(&self, request: Request<ReadMultipleRequest>) -> Result<Response<ReadMultipleResponse>, Status> {
        self.handle_read_multiple(request).await
    }

    async fn delete_volume(&self, request: Request<DeleteVolumeRequest>) -> Result<Response<DeleteVolumeResponse>, Status> {
        self.handle_delete_volume(request).await
    }

    async fn disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        self.handle_disk_info(request).await
    }

    async fn lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "lock")?;
        self.handle_lock(request).await
    }

    async fn un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "unlock")?;
        self.handle_un_lock(request).await
    }

    async fn force_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "force unlock")?;
        self.handle_force_un_lock(request).await
    }

    async fn refresh(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "refresh lock")?;
        self.handle_refresh(request).await
    }

    async fn lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "lock batch")?;
        self.handle_lock_batch(request).await
    }

    async fn un_lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
        verify_node_mutation_body(&request, "unlock batch")?;
        self.handle_un_lock_batch(request).await
    }

    async fn local_storage_info(
        &self,
        _request: Request<LocalStorageInfoRequest>,
    ) -> Result<Response<LocalStorageInfoResponse>, Status> {
        self.handle_local_storage_info(_request).await
    }

    async fn server_info(&self, _request: Request<ServerInfoRequest>) -> Result<Response<ServerInfoResponse>, Status> {
        self.handle_server_info(_request).await
    }

    async fn get_cpus(&self, _request: Request<GetCpusRequest>) -> Result<Response<GetCpusResponse>, Status> {
        self.handle_get_cpus(_request).await
    }

    async fn get_net_info(&self, _request: Request<GetNetInfoRequest>) -> Result<Response<GetNetInfoResponse>, Status> {
        self.handle_get_net_info(_request).await
    }

    async fn get_partitions(&self, _request: Request<GetPartitionsRequest>) -> Result<Response<GetPartitionsResponse>, Status> {
        self.handle_get_partitions(_request).await
    }

    async fn get_os_info(&self, _request: Request<GetOsInfoRequest>) -> Result<Response<GetOsInfoResponse>, Status> {
        self.handle_get_os_info(_request).await
    }

    async fn get_se_linux_info(
        &self,
        _request: Request<GetSeLinuxInfoRequest>,
    ) -> Result<Response<GetSeLinuxInfoResponse>, Status> {
        self.handle_get_se_linux_info(_request).await
    }

    async fn get_sys_config(&self, _request: Request<GetSysConfigRequest>) -> Result<Response<GetSysConfigResponse>, Status> {
        self.handle_get_sys_config(_request).await
    }

    async fn get_sys_errors(&self, _request: Request<GetSysErrorsRequest>) -> Result<Response<GetSysErrorsResponse>, Status> {
        self.handle_get_sys_errors(_request).await
    }

    async fn get_mem_info(&self, _request: Request<GetMemInfoRequest>) -> Result<Response<GetMemInfoResponse>, Status> {
        self.handle_get_mem_info(_request).await
    }

    async fn get_metrics(&self, request: Request<GetMetricsRequest>) -> Result<Response<GetMetricsResponse>, Status> {
        self.handle_get_metrics(request).await
    }

    async fn get_live_events(&self, request: Request<GetLiveEventsRequest>) -> Result<Response<GetLiveEventsResponse>, Status> {
        self.handle_get_live_events(request).await
    }

    async fn get_proc_info(&self, _request: Request<GetProcInfoRequest>) -> Result<Response<GetProcInfoResponse>, Status> {
        self.handle_get_proc_info(_request).await
    }

    async fn start_profiling(
        &self,
        _request: Request<StartProfilingRequest>,
    ) -> Result<Response<StartProfilingResponse>, Status> {
        Err(unimplemented_rpc("start_profiling"))
    }

    async fn download_profile_data(
        &self,
        _request: Request<DownloadProfileDataRequest>,
    ) -> Result<Response<DownloadProfileDataResponse>, Status> {
        Err(unimplemented_rpc("download_profile_data"))
    }

    async fn get_bucket_stats(
        &self,
        request: Request<GetBucketStatsDataRequest>,
    ) -> Result<Response<GetBucketStatsDataResponse>, Status> {
        let bucket = request.into_inner().bucket;
        if bucket.is_empty() {
            return Err(Status::invalid_argument("bucket is required"));
        }
        let context = self.context.clone().or_else(runtime_sources::current_app_context);
        let Some(stats) = runtime_sources::current_replication_stats_handle_for_context(context.as_deref()) else {
            return Ok(Response::new(GetBucketStatsDataResponse {
                success: false,
                bucket_stats: Bytes::new(),
                error_info: Some("replication statistics provider is unavailable".to_string()),
            }));
        };
        let bucket_stats = stats.get_latest_replication_stats(&bucket).await;
        let bucket_stats =
            rmp_serde::to_vec_named(&bucket_stats).map_err(|_| Status::internal("failed to serialize replication statistics"))?;
        Ok(Response::new(GetBucketStatsDataResponse {
            success: true,
            bucket_stats: bucket_stats.into(),
            error_info: None,
        }))
    }

    async fn get_sr_metrics(
        &self,
        _request: Request<GetSrMetricsDataRequest>,
    ) -> Result<Response<GetSrMetricsDataResponse>, Status> {
        Err(unimplemented_rpc("get_sr_metrics"))
    }

    async fn get_all_bucket_stats(
        &self,
        _request: Request<GetAllBucketStatsRequest>,
    ) -> Result<Response<GetAllBucketStatsResponse>, Status> {
        Err(unimplemented_rpc("get_all_bucket_stats"))
    }

    async fn load_bucket_metadata(
        &self,
        request: Request<LoadBucketMetadataRequest>,
    ) -> Result<Response<LoadBucketMetadataResponse>, Status> {
        verify_node_mutation_body(&request, "load bucket metadata")?;
        self.handle_load_bucket_metadata(request).await
    }

    async fn delete_bucket_metadata(
        &self,
        request: Request<DeleteBucketMetadataRequest>,
    ) -> Result<Response<DeleteBucketMetadataResponse>, Status> {
        verify_node_mutation_body(&request, "delete bucket metadata")?;
        self.handle_delete_bucket_metadata(request).await
    }

    async fn delete_policy(&self, request: Request<DeletePolicyRequest>) -> Result<Response<DeletePolicyResponse>, Status> {
        verify_node_mutation_body(&request, "delete policy")?;
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(DeletePolicyResponse {
                error_code: None,
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let resp = iam_sys.delete_policy(&policy, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeletePolicyResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeletePolicyResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy(&self, request: Request<LoadPolicyRequest>) -> Result<Response<LoadPolicyResponse>, Status> {
        verify_node_mutation_body(&request, "load policy")?;
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(LoadPolicyResponse {
                error_code: None,
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let resp = iam_sys.load_policy(&policy).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy_mapping(
        &self,
        request: Request<LoadPolicyMappingRequest>,
    ) -> Result<Response<LoadPolicyMappingResponse>, Status> {
        verify_node_mutation_body(&request, "load policy mapping")?;
        let request = request.into_inner();
        let user_or_group = request.user_or_group;
        if user_or_group.is_empty() {
            return Ok(Response::new(LoadPolicyMappingResponse {
                error_code: None,
                success: false,
                error_info: Some("user_or_group name is missing".to_string()),
            }));
        }
        let Some(user_type) = UserType::from_u64(request.user_type) else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                error_code: None,
                success: false,
                error_info: Some("invalid user type".to_string()),
            }));
        };
        let is_group = request.is_group;
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };
        let resp = iam_sys.load_policy_mapping(&user_or_group, user_type, is_group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyMappingResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyMappingResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn delete_user(&self, request: Request<DeleteUserRequest>) -> Result<Response<DeleteUserResponse>, Status> {
        verify_node_mutation_body(&request, "delete user")?;
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteUserResponse {
                error_code: None,
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let resp = iam_sys.delete_user(&access_key, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteUserResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteUserResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn delete_service_account(
        &self,
        request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>, Status> {
        verify_node_mutation_body(&request, "delete service account")?;
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteServiceAccountResponse {
                error_code: None,
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = self
            .context
            .as_ref()
            .map(|context| context.iam().handle())
            .or_else(runtime_sources::current_iam_handle)
        else {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };
        // This legacy RPC is a cache notification. Reloading shared state keeps a
        // delayed delete notification from removing a recreated service account.
        let resp = iam_sys.load_service_account(&access_key).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteServiceAccountResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteServiceAccountResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn load_user(&self, request: Request<LoadUserRequest>) -> Result<Response<LoadUserResponse>, Status> {
        verify_node_mutation_body(&request, "load user")?;
        let request = request.into_inner();
        let access_key = request.access_key;
        let temp = request.temp;
        if access_key.is_empty() {
            return Ok(Response::new(LoadUserResponse {
                error_code: None,
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let user_type = if temp { UserType::Sts } else { UserType::Reg };

        let resp = iam_sys.load_user(&access_key, user_type).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadUserResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadUserResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn load_service_account(
        &self,
        request: Request<LoadServiceAccountRequest>,
    ) -> Result<Response<LoadServiceAccountResponse>, Status> {
        verify_node_mutation_body(&request, "load service account")?;
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(LoadServiceAccountResponse {
                error_code: None,
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let resp = iam_sys.load_service_account(&access_key).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadServiceAccountResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadServiceAccountResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn load_group(&self, request: Request<LoadGroupRequest>) -> Result<Response<LoadGroupResponse>, Status> {
        verify_node_mutation_body(&request, "load group")?;
        let request = request.into_inner();
        let group = request.group;
        if group.is_empty() {
            return Ok(Response::new(LoadGroupResponse {
                error_code: None,
                success: false,
                error_info: Some("group name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let resp = iam_sys.load_group(&group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadGroupResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadGroupResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn reload_site_replication_config(
        &self,
        request: Request<ReloadSiteReplicationConfigRequest>,
    ) -> Result<Response<ReloadSiteReplicationConfigResponse>, Status> {
        verify_node_mutation_body(&request, "reload site replication config")?;
        let Some(_store) = self.resolve_object_store() else {
            return Ok(Response::new(ReloadSiteReplicationConfigResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };
        match reload_site_replication_runtime_state().await {
            Ok(()) => Ok(Response::new(ReloadSiteReplicationConfigResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ReloadSiteReplicationConfigResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn signal_service(&self, request: Request<SignalServiceRequest>) -> Result<Response<SignalServiceResponse>, Status> {
        verify_node_signal_body(&request, "signal service")?;
        let request = request.into_inner();
        let vars = match request.vars {
            Some(vars) => vars.value,
            None => HashMap::new(),
        };
        let raw_signal = vars.get(PEER_RESTSIGNAL).map(String::as_str);
        let signal = raw_signal.and_then(|value| value.parse::<u64>().ok());
        let sub_system = vars.get(PEER_RESTSUB_SYS).map(String::as_str).unwrap_or_default();
        let dry_run = match vars.get(PEER_RESTDRY_RUN).map(String::as_str) {
            None => false,
            Some(value) => match value.parse::<bool>() {
                Ok(value) => value,
                Err(_) => {
                    return Ok(signal_service_response(false, Some(format!("invalid dry-run value: {value}"))));
                }
            },
        };

        match signal {
            Some(SERVICE_SIGNAL_REFRESH_CONFIG) => match reload_runtime_config_snapshot().await {
                Ok(()) => Ok(signal_service_response(true, None)),
                Err(_) => Ok(signal_service_response(false, Some("runtime config snapshot reload failed".to_string()))),
            },
            Some(SERVICE_SIGNAL_RELOAD_DYNAMIC) => {
                // KMS configuration is persisted outside the server config
                // document, so it converges through its own reload rather than
                // through the server config publication fence.
                if sub_system == KMS_SIGNAL_SUBSYSTEM {
                    return Ok(kms_dynamic_config_signal_response(dry_run).await);
                }
                let supported = sub_system == MODULE_SWITCHES_SIGNAL_SUBSYSTEM || supports_dynamic_config_rpc(sub_system);
                if !supported {
                    return Ok(signal_service_response(
                        false,
                        Some(format!("unsupported dynamic config subsystem: {sub_system}")),
                    ));
                }
                if dry_run {
                    return Ok(signal_service_response(true, None));
                }
                match reload_dynamic_config_runtime_state(sub_system).await {
                    Ok(()) => Ok(signal_service_response(true, None)),
                    Err(_) => Ok(signal_service_response(
                        false,
                        Some(format!("dynamic config reload failed for {sub_system}")),
                    )),
                }
            }
            Some(other) => Ok(signal_service_response(false, Some(format!("unsupported service signal: {other}")))),
            None if raw_signal.is_some() => Ok(signal_service_response(
                false,
                Some(format!("invalid service signal value: {}", raw_signal.unwrap_or_default())),
            )),
            None => Ok(signal_service_response(false, Some("missing service signal".to_string()))),
        }
    }

    async fn scanner_activity(
        &self,
        request: Request<ScannerActivityRequest>,
    ) -> Result<Response<ScannerActivityResponse>, Status> {
        let request_protocol = request.get_ref().protocol_version;
        match request_protocol {
            // RUSTFS_COMPAT_TODO(ns-scanner-rpc-v3): legacy request body is unbound. Remove after protocol v0 peers are unsupported.
            SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION => {
                if !request.get_ref().acknowledge_instance_id.is_empty()
                    || request.get_ref().acknowledge_dirty_usage_generation != 0
                {
                    return Err(Status::invalid_argument("legacy scanner activity request cannot acknowledge dirty usage"));
                }
            }
            SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION => {
                verify_tonic_canonical_body_digest(&request, request.get_ref().challenge.as_ref())
                    .map_err(|err| Status::permission_denied(format!("scanner activity authentication failed: {err}")))?;
                if !request.get_ref().acknowledge_instance_id.is_empty()
                    || request.get_ref().acknowledge_dirty_usage_generation != 0
                {
                    return Err(Status::invalid_argument("scanner activity protocol v4 cannot acknowledge dirty usage"));
                }
            }
            SCANNER_ACTIVITY_V6_PROTOCOL_VERSION | rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION => {
                let canonical = rustfs_protos::canonical_scanner_activity_request_body(request.get_ref())
                    .map_err(|_| Status::invalid_argument("scanner activity request is too large to authenticate"))?;
                verify_tonic_canonical_body_digest(&request, &canonical)
                    .map_err(|err| Status::permission_denied(format!("scanner activity authentication failed: {err}")))?;
                let has_acknowledgement = !request.get_ref().acknowledge_instance_id.is_empty();
                if has_acknowledgement != (request.get_ref().acknowledge_dirty_usage_generation != 0) {
                    return Err(Status::invalid_argument(
                        "scanner dirty usage acknowledgement requires both instance ID and generation",
                    ));
                }
            }
            version => {
                return Err(Status::failed_precondition(format!(
                    "unsupported scanner activity request protocol {version}"
                )));
            }
        }
        let challenge_len = request.get_ref().challenge.len();
        if challenge_len != 16 && !(request_protocol == SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION && challenge_len == 0) {
            return Err(Status::invalid_argument("scanner activity challenge must be 16 bytes"));
        }
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        if request.get_ref().challenge.is_empty() {
            // Older peers send an empty protocol-0 request and cannot establish
            // the topology fence required for distributed usage publication.
            return Ok(Response::new(legacy_scanner_activity_response(
                store.scanner_namespace_mutation_generation(),
            )));
        }
        let request = request.into_inner();
        let challenge: [u8; 16] = request
            .challenge
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("scanner activity challenge must be 16 bytes"))?;
        if !request.acknowledge_instance_id.is_empty() {
            rustfs_scanner::acknowledge_dirty_usage_generation(
                &request.acknowledge_instance_id,
                request.acknowledge_dirty_usage_generation,
            )
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        }
        let topology_digest = rustfs_scanner::scanner_topology_digest(store.as_ref());
        let (data_movement_active, publication_blocked, movement_generation) = store.scanner_data_movement_activity().await;
        let namespace_generation = store.scanner_namespace_mutation_generation();
        let mut response = match request_protocol {
            SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION | SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION => {
                previous_scanner_activity_response(namespace_generation, topology_digest, data_movement_active)
            }
            SCANNER_ACTIVITY_V6_PROTOCOL_VERSION => v6_scanner_activity_response(
                namespace_generation,
                topology_digest,
                data_movement_active,
                rustfs_scanner::scanner_dirty_usage_state(),
            ),
            rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION => scanner_activity_response_v7(
                namespace_generation,
                topology_digest,
                data_movement_active,
                rustfs_scanner::scanner_dirty_usage_state(),
                movement_generation,
                publication_blocked || store.scanner_data_movement_generation_exhausted(),
            ),
            version => {
                return Err(Status::failed_precondition(format!(
                    "unsupported scanner activity request protocol {version}"
                )));
            }
        };
        let canonical = match response.protocol_version {
            SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION => {
                rustfs_protos::canonical_scanner_activity_v4_response_body(&challenge, &response)
            }
            SCANNER_ACTIVITY_V6_PROTOCOL_VERSION => {
                rustfs_protos::canonical_scanner_activity_response_body(&challenge, &response)
            }
            rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION => {
                rustfs_protos::canonical_scanner_activity_v7_response_body(&challenge, &response)
            }
            version => {
                return Err(Status::internal(format!(
                    "scanner activity response selected unsupported protocol {version}"
                )));
            }
        }
        .map_err(|_| Status::internal("scanner activity response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&canonical)
            .map_err(|_| Status::unavailable("scanner activity response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }

    async fn scanner_dirty_usage_snapshot(
        &self,
        request: Request<ScannerDirtyUsageSnapshotRequest>,
    ) -> Result<Response<ScannerDirtyUsageSnapshotResponse>, Status> {
        let canonical = rustfs_protos::canonical_scanner_dirty_usage_snapshot_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("scanner dirty usage snapshot request is too large to authenticate"))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|err| Status::permission_denied(format!("scanner dirty usage snapshot authentication failed: {err}")))?;
        if request.get_ref().protocol_version != rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported scanner dirty usage snapshot request protocol {}",
                request.get_ref().protocol_version
            )));
        }
        if request.get_ref().challenge.len() != 16 {
            return Err(Status::invalid_argument("scanner dirty usage snapshot challenge must be 16 bytes"));
        }
        let challenge: [u8; 16] = request
            .into_inner()
            .challenge
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("scanner dirty usage snapshot challenge must be 16 bytes"))?;
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        let snapshot = rustfs_scanner::scanner_dirty_usage_snapshot(rustfs_scanner::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES);
        if snapshot.generation == u64::MAX {
            return Err(Status::resource_exhausted("scanner dirty usage generation is exhausted"));
        }
        let mut response = scanner_dirty_usage_snapshot_response(&store, snapshot).await?;
        let canonical = rustfs_protos::canonical_scanner_dirty_usage_snapshot_response_body(&challenge, &response)
            .map_err(|_| Status::internal("scanner dirty usage snapshot response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&canonical)
            .map_err(|_| Status::unavailable("scanner dirty usage snapshot response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }

    async fn acquire_scanner_publication_lease(
        &self,
        request: Request<ScannerPublicationLeaseRequest>,
    ) -> Result<Response<ScannerPublicationLeaseResponse>, Status> {
        let canonical = rustfs_protos::canonical_scanner_publication_lease_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("scanner publication lease request is too large to authenticate"))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|err| Status::permission_denied(format!("scanner publication lease authentication failed: {err}")))?;
        if request.get_ref().challenge.len() != 16 {
            return Err(Status::invalid_argument("scanner publication lease challenge must be 16 bytes"));
        }
        if request.get_ref().ttl_ms != SCANNER_PUBLICATION_LEASE_TTL_MS {
            return Err(Status::invalid_argument("scanner publication lease TTL is unsupported"));
        }
        let session_id = rustfs_scanner::scanner_activity_epoch().to_string();
        if request.get_ref().expected_session_id != session_id {
            return Err(Status::failed_precondition("scanner publication lease session is stale"));
        }
        let validation_token = if request.get_ref().token.is_empty() {
            None
        } else {
            Some(
                Uuid::from_slice(request.get_ref().token.as_ref())
                    .map_err(|_| Status::invalid_argument("scanner publication lease token must be a UUID"))?,
            )
        };
        let challenge = request.get_ref().challenge.clone();
        let request = request.into_inner();
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        if store.id.is_nil() {
            return Err(Status::unavailable("storage owner identity is not initialized"));
        }
        let owner_id = store.id.to_string();
        let result = match validation_token {
            Some(token) => store
                .validate_scanner_publication_lease(token, request.expected_movement_generation)
                .await
                .map(|()| (token, request.expected_movement_generation)),
            None => {
                store
                    .acquire_scanner_publication_lease(
                        request.expected_movement_generation,
                        Duration::from_millis(request.ttl_ms),
                    )
                    .await
            }
        };
        let mut response = match result {
            Ok((token, generation)) => ScannerPublicationLeaseResponse {
                success: true,
                token: token.as_bytes().to_vec().into(),
                movement_generation: generation,
                lease_ttl_ms: request.ttl_ms,
                error: None,
                response_proof: Bytes::new(),
                owner_id: owner_id.clone(),
                session_id: session_id.clone(),
            },
            Err(err) => ScannerPublicationLeaseResponse {
                success: false,
                token: Bytes::new(),
                movement_generation: store.scanner_data_movement_generation(),
                lease_ttl_ms: 0,
                error: Some(rustfs_protos::proto_gen::node_service::Error {
                    code: 1,
                    error_info: err.to_string(),
                }),
                response_proof: Bytes::new(),
                owner_id: owner_id.clone(),
                session_id: session_id.clone(),
            },
        };
        let response_body = rustfs_protos::canonical_scanner_publication_lease_response_body(&challenge, &response)
            .map_err(|_| Status::internal("scanner publication lease response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&response_body)
            .map_err(|_| Status::unavailable("scanner publication lease response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }

    async fn release_scanner_publication_lease(
        &self,
        request: Request<ScannerPublicationLeaseReleaseRequest>,
    ) -> Result<Response<ScannerPublicationLeaseReleaseResponse>, Status> {
        let canonical = rustfs_protos::canonical_scanner_publication_lease_release_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("scanner publication lease release request is too large to authenticate"))?;
        verify_tonic_canonical_body_digest(&request, &canonical).map_err(|err| {
            Status::permission_denied(format!("scanner publication lease release authentication failed: {err}"))
        })?;
        if request.get_ref().challenge.len() != 16 {
            return Err(Status::invalid_argument("scanner publication lease challenge must be 16 bytes"));
        }
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        if store.id.is_nil() {
            return Err(Status::unavailable("storage owner identity is not initialized"));
        }
        let owner_id = store.id.to_string();
        let session_id = rustfs_scanner::scanner_activity_epoch().to_string();
        if request.get_ref().owner_id != owner_id || request.get_ref().session_id != session_id {
            return Err(Status::failed_precondition("scanner publication lease owner or session is stale"));
        }
        let token = Uuid::from_slice(request.get_ref().token.as_ref())
            .map_err(|_| Status::invalid_argument("scanner publication lease token must be a UUID"))?;
        let challenge = request.get_ref().challenge.clone();
        let request = request.into_inner();
        let released = store.release_scanner_publication_lease(token).await;
        let mut response = ScannerPublicationLeaseReleaseResponse {
            success: released,
            error: (!released).then(|| rustfs_protos::proto_gen::node_service::Error {
                code: 1,
                error_info: "scanner publication lease is unknown or expired".to_string(),
            }),
            response_proof: Bytes::new(),
        };
        let response_body =
            rustfs_protos::canonical_scanner_publication_lease_release_response_body(&challenge, &request, &response)
                .map_err(|_| Status::internal("scanner publication lease response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&response_body)
            .map_err(|_| Status::unavailable("scanner publication lease response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }

    async fn background_heal_status(
        &self,
        request: Request<BackgroundHealStatusRequest>,
    ) -> Result<Response<BackgroundHealStatusResponse>, Status> {
        if self.resolve_object_store().is_none() {
            return Ok(Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: Bytes::new(),
                error_info: Some("storage layer not initialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        }
        let snapshot = heal::capture_node_heal_status(rustfs_scanner::scanner::BackgroundHealInfo::default()).await;
        match heal::encode_node_heal_status(&snapshot, request.into_inner().protocol_version) {
            Ok(bg_heal_state) => Ok(Response::new(BackgroundHealStatusResponse {
                success: true,
                bg_heal_state: bg_heal_state.into(),
                error_info: None,
                error_code: None,
            })),
            Err(err) => Ok(Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: Bytes::new(),
                error_info: Some(err),
                error_code: None,
            })),
        }
    }

    async fn replacement_recovery_status(
        &self,
        _request: Request<ReplacementRecoveryStatusRequest>,
    ) -> Result<Response<ReplacementRecoveryStatusResponse>, Status> {
        if self.resolve_object_store().is_none() {
            return Ok(Response::new(ReplacementRecoveryStatusResponse {
                success: false,
                recovery_status: Bytes::new(),
                error_info: Some("storage layer not initialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        }
        let snapshot = heal::capture_node_replacement_recovery_status().await;
        match heal::encode_node_replacement_recovery_status(&snapshot) {
            Ok(recovery_status) => Ok(Response::new(ReplacementRecoveryStatusResponse {
                error_code: None,
                success: true,
                recovery_status: recovery_status.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ReplacementRecoveryStatusResponse {
                error_code: None,
                success: false,
                recovery_status: Bytes::new(),
                error_info: Some(err),
            })),
        }
    }

    async fn get_metacache_listing(
        &self,
        _request: Request<GetMetacacheListingRequest>,
    ) -> Result<Response<GetMetacacheListingResponse>, Status> {
        Err(unimplemented_rpc("get_metacache_listing"))
    }

    async fn update_metacache_listing(
        &self,
        _request: Request<UpdateMetacacheListingRequest>,
    ) -> Result<Response<UpdateMetacacheListingResponse>, Status> {
        Err(unimplemented_rpc("update_metacache_listing"))
    }

    async fn reload_pool_meta(
        &self,
        request: Request<ReloadPoolMetaRequest>,
    ) -> Result<Response<ReloadPoolMetaResponse>, Status> {
        verify_node_mutation_body(&request, "reload pool metadata")?;
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };
        // Recover missing workers only after the reload merged newer state; a
        // stale or duplicate reload must not spawn workers for an older generation.
        match store.reload_pool_meta().await {
            Ok(true) => match store.spawn_missing_local_decommission_routines().await {
                Ok(_) => Ok(Response::new(ReloadPoolMetaResponse {
                    error_code: None,
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(Response::new(ReloadPoolMetaResponse {
                    error_code: None,
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            },
            Ok(false) => Ok(Response::new(ReloadPoolMetaResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ReloadPoolMetaResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn stop_rebalance(&self, request: Request<StopRebalanceRequest>) -> Result<Response<StopRebalanceResponse>, Status> {
        verify_node_mutation_body(&request, "stop rebalance")?;
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(StopRebalanceResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let expected_rebalance_id = request.into_inner().expected_rebalance_id;
        let expected_rebalance_id = (!expected_rebalance_id.is_empty()).then_some(expected_rebalance_id);

        Ok(Response::new(stop_rebalance_response(
            store.stop_rebalance_for_id(expected_rebalance_id.as_deref()).await,
        )))
    }

    #[tracing::instrument(skip_all, fields(start_rebalance))]
    async fn load_rebalance_meta(
        &self,
        request: Request<LoadRebalanceMetaRequest>,
    ) -> Result<Response<LoadRebalanceMetaResponse>, Status> {
        verify_node_mutation_body(&request, "load rebalance metadata")?;
        let LoadRebalanceMetaRequest { start_rebalance } = request.into_inner();
        let Some(store) = self.resolve_object_store() else {
            log_load_rebalance_meta_rejected!("server_not_initialized", start_rebalance);
            return Ok(Response::new(LoadRebalanceMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        store.load_rebalance_meta().await.map_err(|err| {
            log_load_rebalance_meta_failed!("load_rebalance_meta_failed", start_rebalance, err);
            Status::internal(err.to_string())
        })?;
        log_load_rebalance_meta_response_emitted!(start_rebalance);

        if start_rebalance {
            log_background_rebalance_task_spawned!(start_rebalance);
            if let Some(message) = background_rebalance_start_error_message(store.start_rebalance().await) {
                error!(
                    event = EVENT_RPC_BACKGROUND_TASK_FAILED,
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    operation = "start_rebalance",
                    state = "failed",
                    start_rebalance,
                    error = %message,
                    "node rpc background task failed"
                );
                return Ok(Response::new(LoadRebalanceMetaResponse {
                    error_code: None,
                    success: false,
                    error_info: Some(message),
                }));
            }
        }

        Ok(Response::new(LoadRebalanceMetaResponse {
            error_code: None,
            success: true,
            error_info: None,
        }))
    }

    async fn start_decommission(
        &self,
        request: Request<StartDecommissionRequest>,
    ) -> Result<Response<StartDecommissionResponse>, Status> {
        verify_node_mutation_body(&request, "start decommission")?;
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(StartDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let mut indices = Vec::with_capacity(request.get_ref().pool_indices.len());
        for idx in request.into_inner().pool_indices {
            indices.push(
                usize::try_from(idx)
                    .map_err(|_| Status::invalid_argument(format!("decommission pool index {idx} exceeds local range")))?,
            );
        }

        match store.decommission(CancellationToken::new(), indices).await {
            Ok(()) => Ok(Response::new(StartDecommissionResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(start_decommission_failure_response(err))),
        }
    }

    async fn cancel_decommission(
        &self,
        request: Request<CancelDecommissionRequest>,
    ) -> Result<Response<CancelDecommissionResponse>, Status> {
        verify_node_mutation_body(&request, "cancel decommission")?;
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(CancelDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let idx = usize::try_from(request.into_inner().pool_index)
            .map_err(|_| Status::invalid_argument("decommission pool index exceeds local range"))?;
        if let Err(err) = ensure_rpc_decommission_local_leader(&store, idx) {
            return Ok(Response::new(CancelDecommissionResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        match store.decommission_cancel(idx).await {
            Ok(()) => Ok(Response::new(CancelDecommissionResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(CancelDecommissionResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn clear_decommission(
        &self,
        request: Request<ClearDecommissionRequest>,
    ) -> Result<Response<ClearDecommissionResponse>, Status> {
        verify_node_mutation_body(&request, "clear decommission")?;
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(ClearDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        let idx = usize::try_from(request.into_inner().pool_index)
            .map_err(|_| Status::invalid_argument("decommission pool index exceeds local range"))?;
        if let Err(err) = ensure_rpc_decommission_local_leader(&store, idx) {
            return Ok(Response::new(ClearDecommissionResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        match store.clear_decommission(idx).await {
            Ok(()) => Ok(Response::new(ClearDecommissionResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ClearDecommissionResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn tier_daily_stats(
        &self,
        request: Request<TierDailyStatsRequest>,
    ) -> Result<Response<TierDailyStatsResponse>, Status> {
        self.handle_tier_daily_stats(request).await
    }

    async fn load_transition_tier_config(
        &self,
        request: Request<LoadTransitionTierConfigRequest>,
    ) -> Result<Response<LoadTransitionTierConfigResponse>, Status> {
        verify_node_mutation_body(&request, "load transition tier config")?;
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(LoadTransitionTierConfigResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
                error_code: Some(ControlPlaneErrorCode::ControlPlaneErrorNotInitialized as i32),
            }));
        };

        match reload_transition_tier_config(store).await {
            Ok(_) => Ok(Response::new(LoadTransitionTierConfigResponse {
                error_code: None,
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(LoadTransitionTierConfigResponse {
                error_code: None,
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        CROSS_POOL_FENCE_SUPPORTED_VERSION, CollectMetricsOpts, DiskStore, Error, HEAL_CONTROL_PAYLOAD_MAX_SIZE,
        KMS_SIGNAL_SUBSYSTEM, MetricType, Node as _, NodeService, PEER_RESTDRY_RUN, PEER_RESTSIGNAL, PEER_RESTSUB_SYS,
        SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION, SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION, SCANNER_PUBLICATION_LEASE_TTL_MS,
        SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, STORAGE_CLASS_SUB_SYS, admit_heal_control_replay,
        background_rebalance_start_error_message, execute_heal_control_envelope_with_manager,
        initialize_heal_topology_fingerprint, initialize_heal_topology_fingerprint_with_probe, legacy_scanner_activity_response,
        make_heal_control_server, make_heal_control_server_with_cache, make_server, make_server_for_context,
        make_tier_mutation_control_server_for_context, previous_scanner_activity_response, remove_heal_control_replay,
        scanner_activity_response_v7, start_decommission_failure_response, stop_rebalance_response,
        validate_admin_heal_control_start,
    };
    use crate::storage::rpc::node_service::heal::heal_topology_fingerprint;
    use crate::storage::storage_api::rpc_consumer::node_service::{DiskError, HealBucketInfo};
    use crate::storage::storage_api::set_tonic_canonical_body_digest;
    use crate::storage::storage_api::{
        Endpoint,
        ecstore_layout::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
    use bytes::Bytes;
    use rustfs_heal::heal::{manager::HealManager, storage::HealStorageAPI};
    use rustfs_iam::{
        store::{
            Store as _,
            object::{IAM_CONFIG_PREFIX, ObjectStore},
        },
        sys::NewServiceAccountOpts,
    };
    use rustfs_kms::KmsServiceManager;
    use rustfs_protos::CanonicalMutationBody as _;
    use rustfs_protos::models::PingBodyBuilder;
    use rustfs_protos::proto_gen::node_service::scanner_control_service_server::ScannerControlService as _;
    use rustfs_protos::proto_gen::node_service::{
        BackgroundHealStatusRequest, BatchGenerallyLockRequest, CancelDecommissionRequest, CheckPartsRequest,
        ClearDecommissionRequest, ControlPlaneErrorCode, DeleteBucketMetadataRequest, DeleteBucketRequest, DeletePathsRequest,
        DeletePolicyRequest, DeleteRequest, DeleteServiceAccountRequest, DeleteUserRequest, DeleteVersionRequest,
        DeleteVersionsRequest, DeleteVolumeRequest, DiskInfoRequest, DownloadProfileDataRequest, GenerallyLockRequest,
        GetAllBucketStatsRequest, GetBucketInfoRequest, GetBucketStatsDataRequest, GetCpusRequest, GetMemInfoRequest,
        GetMetacacheListingRequest, GetMetricsRequest, GetNetInfoRequest, GetOsInfoRequest, GetPartitionsRequest,
        GetProcInfoRequest, GetSeLinuxInfoRequest, GetSrMetricsDataRequest, GetSysConfigRequest, GetSysErrorsRequest,
        HealBucketRequest, HealControlRequest, ListBucketRequest, ListDirRequest, ListVolumesRequest, LoadBucketMetadataRequest,
        LoadGroupRequest, LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest, LoadServiceAccountRequest,
        LoadTransitionTierConfigRequest, LoadUserRequest, LocalStorageInfoRequest, MakeBucketRequest, MakeVolumeRequest,
        MakeVolumesRequest, Mss, PingRequest, PreparePartTransactionRequest, ReadAllRequest, ReadAtRequest, ReadMultipleRequest,
        ReadVersionRequest, ReadXlRequest, ReloadPoolMetaRequest, ReloadSiteReplicationConfigRequest, RenameDataRequest,
        RenameFileRequest, RenamePartRequest, ScannerActivityRequest, ScannerDirtyUsageSnapshotRequest,
        ScannerPublicationLeaseReleaseRequest, ScannerPublicationLeaseRequest, ServerInfoRequest, SettlePartTransactionRequest,
        SignalServiceRequest, SnapshotLeaseReleaseRequest, SnapshotLeaseRenewRequest, SnapshotLeaseRequest,
        StartDecommissionRequest, StartProfilingRequest, StatVolumeRequest, StopRebalanceRequest, TierMutationAbortRequest,
        TierMutationFailureClass, TierMutationPeerState, TierMutationPrepareRequest, UpdateMetacacheListingRequest,
        UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest, WriteMetadataRequest, WriteRequest,
        heal_control_service_client::HealControlServiceClient,
        heal_control_service_server::{HealControlService as _, HealControlServiceServer},
        node_service_client::NodeServiceClient,
        node_service_server::NodeServiceServer,
        tier_mutation_control_service_server::TierMutationControlService as _,
    };
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };
    use time::OffsetDateTime;
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use uuid::Uuid;

    const DISK_MUTATION_RPC_METHODS: [&str; 18] = [
        "renamedata",
        "deleteversion",
        "deleteversions",
        "writemetadata",
        "updatemetadata",
        "writeall",
        "delete",
        "deletepaths",
        "renamefile",
        "renamepart",
        "prepareparttransaction",
        "settleparttransaction",
        "deletevolume",
        "makevolume",
        "makevolumes",
        "acquiresnapshotlease",
        "renewsnapshotlease",
        "releasesnapshotlease",
    ];

    fn normalized_rpc_method(method: &str) -> String {
        method.replace('_', "").to_ascii_lowercase()
    }

    fn node_service_auth_policies() -> HashMap<String, &'static str> {
        const POLICY_MARKER: &str = "// auth-policy: ";

        let schema = include_str!("../../../../crates/protos/src/node.proto");
        let service = schema
            .split_once("service NodeService {")
            .expect("NodeService must exist in node.proto")
            .1
            .split_once("\n}")
            .expect("NodeService must have a closing brace")
            .0;
        let mut policies = HashMap::new();
        for declaration in service.lines().filter_map(|line| line.trim().strip_prefix("rpc ")) {
            let (rpc, policy) = declaration
                .split_once(POLICY_MARKER)
                .expect("every NodeService RPC must declare an auth-policy beside its proto definition");
            let method = rpc.split_once('(').expect("RPC declaration must have a request type").0;
            assert!(
                policies.insert(normalized_rpc_method(method), policy.trim()).is_none(),
                "duplicate NodeService RPC {method}",
            );
        }
        assert!(!policies.is_empty(), "NodeService must declare RPC methods");
        policies
    }

    #[test]
    fn every_node_service_rpc_declares_an_auth_policy() {
        const VALID_POLICIES: [&str; 4] = ["body-bound", "read-only", "streaming", "unimplemented"];

        for (method, policy) in node_service_auth_policies() {
            assert!(
                VALID_POLICIES.contains(&policy),
                "NodeService RPC {method} has unsupported auth-policy {policy:?}",
            );
        }
    }

    #[test]
    fn start_decommission_failure_response_preserves_invalid_argument_reason() {
        let reason = "durable unresolved-entry recovery requires pool metadata V2 or V3";
        let response = start_decommission_failure_response(Error::InvalidArgument(
            "decommission".to_string(),
            "pool-metadata-version".to_string(),
            reason.to_string(),
        ));

        assert!(!response.success);
        assert_eq!(response.error_info.as_deref(), Some(reason));
        assert_eq!(response.error_code, Some(ControlPlaneErrorCode::ControlPlaneErrorInvalidArgument as i32));
    }

    struct HealControlMockStorage;

    #[async_trait::async_trait]
    impl HealStorageAPI for HealControlMockStorage {
        async fn get_object_meta(
            &self,
            _bucket: &str,
            _object: &str,
        ) -> rustfs_heal::Result<Option<rustfs_heal::heal::storage::HealObjectInfo>> {
            Ok(None)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> rustfs_heal::Result<Option<HealBucketInfo>> {
            Ok(None)
        }

        async fn list_buckets(&self) -> rustfs_heal::Result<Vec<HealBucketInfo>> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<bool> {
            Ok(false)
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &rustfs_heal_contracts::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_heal_contracts::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<rustfs_madmin::heal_commands::HealResultItem> {
            Ok(rustfs_madmin::heal_commands::HealResultItem::default())
        }

        async fn heal_format(
            &self,
            _dry_run: bool,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
            _include_lifecycle_object_info: bool,
        ) -> rustfs_heal::Result<(Vec<rustfs_heal::heal::storage::HealListItem>, Option<String>, bool)> {
            Ok((Vec::new(), None, false))
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> rustfs_heal::Result<DiskStore> {
            Err(rustfs_heal::Error::other("not implemented in heal control test"))
        }
    }

    fn create_test_node_service() -> NodeService {
        make_server()
    }

    #[tokio::test]
    async fn heal_control_replay_cache_singleflights_only_matching_request_ids() {
        let mut cache = HashMap::new();
        let first = admit_heal_control_replay(&mut cache, "request-1", &[1; 32], 200, 100).unwrap();
        let exact = admit_heal_control_replay(&mut cache, "request-1", &[1; 32], 200, 100).unwrap();
        assert!(Arc::ptr_eq(&first, &exact));

        let collision = admit_heal_control_replay(&mut cache, "request-1", &[2; 32], 200, 100)
            .expect_err("one request ID must not identify two commands");
        assert_eq!(collision.code(), tonic::Code::AlreadyExists);

        remove_heal_control_replay(&mut cache, "request-1", &first);
        assert!(!cache.contains_key("request-1"), "completed query results must not remain cached");

        let second = admit_heal_control_replay(&mut cache, "request-2", &[2; 32], 300, 100).unwrap();
        let first_execution = first.result.lock().await;
        let _second_execution = tokio::time::timeout(Duration::from_millis(50), second.result.lock())
            .await
            .expect("a different request ID must not wait behind the first request");

        drop(first_execution);
        drop(_second_execution);
        drop(exact);
        drop(first);
        drop(second);
        let _third = admit_heal_control_replay(&mut cache, "request-3", &[3; 32], 400, 300).unwrap();
        assert!(!cache.contains_key("request-1"), "expired idle entries must be purged before admission");
    }

    #[test]
    fn heal_control_admin_start_rejects_automatic_replacement_endpoints() {
        let mut request = rustfs_heal_contracts::heal_channel::create_heal_request(
            String::new(),
            None,
            false,
            Some(rustfs_heal_contracts::heal_channel::HealChannelPriority::High),
        );
        request.source = rustfs_heal_contracts::heal_channel::HealRequestSource::Admin;
        request.recursive = Some(true);
        request.heal_endpoints = vec!["/mnt/replacement".to_string()];

        let err = validate_admin_heal_control_start(&request)
            .expect_err("admin heal-control must not accept automatic replacement targets");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    fn heal_start_retry_fixture() -> (
        Arc<HealManager>,
        rustfs_heal_contracts::heal_channel::HealChannelRequest,
        rustfs_protos::heal_control::RequestMetadata,
    ) {
        let manager = Arc::new(HealManager::new(Arc::new(HealControlMockStorage), None));
        let mut request = rustfs_heal_contracts::heal_channel::create_heal_request(
            "bucket".to_string(),
            Some("prefix".to_string()),
            true,
            None,
        );
        request.source = rustfs_heal_contracts::heal_channel::HealRequestSource::Admin;
        request.recursive = Some(true);
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).expect("fixture clock fits in i64");
        let metadata = rustfs_protos::heal_control::RequestMetadata::new(*Uuid::new_v4().as_bytes(), now, now + 30_000, 7);
        (manager, request, metadata)
    }

    #[tokio::test]
    async fn heal_start_retry_exact_forced_envelope_returns_cached_admission() {
        let (manager, request, metadata) = heal_start_retry_fixture();
        let request_id = request.id.clone();
        let envelope = rustfs_protos::heal_control::Envelope::start(request, metadata).expect("valid forced start");
        let lost_response =
            execute_heal_control_envelope_with_manager(envelope.clone(), metadata.coordinator_epoch, Some(manager.clone()))
                .await
                .expect("first request is admitted before its response is lost");
        assert_eq!(manager.operations_snapshot().await.queue_length, 1);

        // The caller sees no first response, but retries the original envelope.
        let replayed = execute_heal_control_envelope_with_manager(envelope, metadata.coordinator_epoch, Some(manager.clone()))
            .await
            .expect("an exact envelope replay must recover its receipt");
        assert_eq!(replayed, lost_response);
        assert_eq!(
            manager.operations_snapshot().await.queue_length,
            1,
            "forceStart must not be executed twice"
        );
        let outcome = rustfs_protos::heal_control::decode_result(&replayed)
            .and_then(|result| result.into_outcome(&request_id, metadata.coordinator_epoch))
            .expect("matching canonical receipt");
        assert!(matches!(outcome, rustfs_protos::heal_control::Outcome::Start {
            task_id, admission: rustfs_protos::heal_control::Admission::Accepted,
        } if task_id == request_id));
    }

    #[tokio::test]
    async fn heal_start_retry_new_forced_request_is_a_distinct_start() {
        let (manager, request, metadata) = heal_start_retry_fixture();
        let first_id = request.id.clone();
        let first = rustfs_protos::heal_control::Envelope::start(request.clone(), metadata).expect("first start");
        let _lost_response = execute_heal_control_envelope_with_manager(first, metadata.coordinator_epoch, Some(manager.clone()))
            .await
            .expect("first admission");

        // A fresh HTTP forceStart request intentionally requests another start.
        let mut next_request = request;
        next_request.id = Uuid::new_v4().to_string();
        let next_id = next_request.id.clone();
        let next_metadata = rustfs_protos::heal_control::RequestMetadata {
            nonce: *Uuid::new_v4().as_bytes(),
            ..metadata
        };
        let next = rustfs_protos::heal_control::Envelope::start(next_request, next_metadata).expect("new forced start");
        let response = execute_heal_control_envelope_with_manager(next, metadata.coordinator_epoch, Some(manager.clone()))
            .await
            .expect("forceStart preserves its explicit admission semantics");
        let outcome = rustfs_protos::heal_control::decode_result(&response)
            .and_then(|result| result.into_outcome(&next_id, metadata.coordinator_epoch))
            .expect("new receipt");
        assert!(matches!(outcome, rustfs_protos::heal_control::Outcome::Start {
            task_id, admission: rustfs_protos::heal_control::Admission::Accepted,
        } if task_id == next_id && task_id != first_id));
        assert_eq!(
            manager.operations_snapshot().await.queue_length,
            2,
            "a caller must not treat a new forced request as an idempotent transport retry"
        );
    }

    #[tokio::test]
    async fn heal_start_retry_same_id_with_changed_envelope_conflicts_before_admission() {
        let (manager, request, metadata) = heal_start_retry_fixture();
        let original = rustfs_protos::heal_control::Envelope::start(request.clone(), metadata).expect("original start");
        let receipt =
            execute_heal_control_envelope_with_manager(original.clone(), metadata.coordinator_epoch, Some(manager.clone()))
                .await
                .expect("original admission");
        let mut changed_options = request.clone();
        changed_options.remove_corrupted = Some(true);
        let changed_metadata = rustfs_protos::heal_control::RequestMetadata {
            nonce: *Uuid::new_v4().as_bytes(),
            ..metadata
        };
        for changed in [
            rustfs_protos::heal_control::Envelope::start(changed_options, metadata).expect("changed options"),
            rustfs_protos::heal_control::Envelope::start(request, changed_metadata).expect("changed nonce"),
        ] {
            let error = execute_heal_control_envelope_with_manager(changed, metadata.coordinator_epoch, Some(manager.clone()))
                .await
                .expect_err("one request ID cannot identify different envelope bytes");
            assert_eq!(error.code(), tonic::Code::AlreadyExists);
            assert_eq!(manager.operations_snapshot().await.queue_length, 1);
        }
        assert_eq!(
            execute_heal_control_envelope_with_manager(original, metadata.coordinator_epoch, Some(manager))
                .await
                .expect("conflicts must preserve the original receipt"),
            receipt
        );
    }

    #[tokio::test]
    async fn heal_start_retry_wrong_coordinator_epoch_cannot_admit_locally() {
        let (manager, request, metadata) = heal_start_retry_fixture();
        let request_id = request.id.clone();
        let envelope = rustfs_protos::heal_control::Envelope::start(request, metadata).expect("start envelope");
        let error = execute_heal_control_envelope_with_manager(envelope, metadata.coordinator_epoch + 1, Some(manager.clone()))
            .await
            .expect_err("a different coordinator epoch cannot accept the request");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(manager.operations_snapshot().await.queue_length, 0);
        assert!(matches!(
            manager.get_task_status(&request_id).await,
            Err(rustfs_heal::Error::TaskNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn heal_control_executor_preserves_canonical_token_and_drops_query_results() {
        let manager = Arc::new(HealManager::new(Arc::new(HealControlMockStorage), None));
        let coordinator_epoch = 7;
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let now = i64::try_from(now).expect("test clock should fit in i64");
        let metadata = || rustfs_protos::heal_control::RequestMetadata::new(rand::random(), now, now + 30_000, coordinator_epoch);
        let start = |request_id: String| {
            let mut request = rustfs_heal_contracts::heal_channel::create_heal_request(
                "bucket".to_string(),
                Some("prefix".to_string()),
                false,
                None,
            );
            request.id = request_id;
            request.source = rustfs_heal_contracts::heal_channel::HealRequestSource::Admin;
            request
        };

        let canonical_token = uuid::Uuid::new_v4().to_string();
        let first = rustfs_protos::heal_control::Envelope::start(start(canonical_token.clone()), metadata()).unwrap();
        let first_result = execute_heal_control_envelope_with_manager(first, coordinator_epoch, Some(Arc::clone(&manager)))
            .await
            .unwrap();
        let first_outcome = rustfs_protos::heal_control::decode_result(&first_result)
            .and_then(|result| result.into_outcome(&canonical_token, coordinator_epoch))
            .unwrap();
        assert!(matches!(
            first_outcome,
            rustfs_protos::heal_control::Outcome::Start {
                task_id,
                admission: rustfs_protos::heal_control::Admission::Accepted,
            } if task_id == canonical_token
        ));

        let duplicate_id = uuid::Uuid::new_v4().to_string();
        let duplicate = rustfs_protos::heal_control::Envelope::start(start(duplicate_id.clone()), metadata()).unwrap();
        let duplicate_result =
            execute_heal_control_envelope_with_manager(duplicate, coordinator_epoch, Some(Arc::clone(&manager)))
                .await
                .unwrap();
        let duplicate_outcome = rustfs_protos::heal_control::decode_result(&duplicate_result)
            .and_then(|result| result.into_outcome(&duplicate_id, coordinator_epoch))
            .unwrap();
        assert!(matches!(
            duplicate_outcome,
            rustfs_protos::heal_control::Outcome::Start {
                task_id,
                admission: rustfs_protos::heal_control::Admission::Merged,
            } if task_id == canonical_token
        ));

        let query_id = uuid::Uuid::new_v4().to_string();
        let query = rustfs_protos::heal_control::Envelope::query(
            query_id.clone(),
            metadata(),
            "bucket/prefix".to_string(),
            canonical_token.clone(),
            None,
        )
        .unwrap();
        let query_result = execute_heal_control_envelope_with_manager(query, coordinator_epoch, Some(Arc::clone(&manager)))
            .await
            .unwrap();
        let query_outcome = rustfs_protos::heal_control::decode_result(&query_result)
            .and_then(|result| result.into_outcome(&query_id, coordinator_epoch))
            .unwrap();
        assert!(matches!(
            query_outcome,
            rustfs_protos::heal_control::Outcome::Channel { success: true, .. }
        ));

        let cancel_id = uuid::Uuid::new_v4().to_string();
        let cancel = rustfs_protos::heal_control::Envelope::cancel(
            cancel_id.clone(),
            metadata(),
            "bucket/prefix".to_string(),
            canonical_token.clone(),
        )
        .unwrap();
        let cancel_result = execute_heal_control_envelope_with_manager(cancel, coordinator_epoch, Some(Arc::clone(&manager)))
            .await
            .unwrap();
        let cancel_outcome = rustfs_protos::heal_control::decode_result(&cancel_result)
            .and_then(|result| result.into_outcome(&cancel_id, coordinator_epoch))
            .unwrap();
        assert!(matches!(
            cancel_outcome,
            rustfs_protos::heal_control::Outcome::Channel { success: true, .. }
        ));

        let stopped_query_id = uuid::Uuid::new_v4().to_string();
        let stopped_query = rustfs_protos::heal_control::Envelope::query(
            stopped_query_id.clone(),
            metadata(),
            "bucket/prefix".to_string(),
            canonical_token,
            None,
        )
        .unwrap();
        let stopped_result = execute_heal_control_envelope_with_manager(stopped_query, coordinator_epoch, Some(manager))
            .await
            .unwrap();
        let stopped_outcome = rustfs_protos::heal_control::decode_result(&stopped_result)
            .and_then(|result| result.into_outcome(&stopped_query_id, coordinator_epoch))
            .unwrap();
        assert!(matches!(
            stopped_outcome,
            rustfs_protos::heal_control::Outcome::Channel {
                success: true,
                error: Some(detail),
                ..
            } if detail == "heal task not found or expired"
        ));

        let replay_cache = super::HEAL_CONTROL_REPLAY_CACHE.get().unwrap().lock().await;
        assert!(!replay_cache.contains_key(&query_id), "completed query results must not remain cached");
        assert!(
            !replay_cache.contains_key(&stopped_query_id),
            "completed stopped queries must not remain cached"
        );
    }

    #[tokio::test]
    async fn test_make_server() {
        let service = make_server();
        // LocalPeerS3Client is a struct, not an Option, so we just check it exists
        assert!(format!("{:?}", service.local_peer).contains("LocalPeerS3Client"));
    }

    fn heal_control_request(command: &[u8]) -> Request<HealControlRequest> {
        Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: "fingerprint".to_string(),
            command: Bytes::copy_from_slice(command),
        })
    }

    fn heal_control_test_endpoints(last_host: &str) -> EndpointServerPools {
        heal_control_test_endpoints_with_coordinator(last_host, false)
    }

    fn heal_control_test_endpoints_with_coordinator(last_host: &str, coordinator_local: bool) -> EndpointServerPools {
        let endpoints = ["node-a", "node-b", "node-c", last_host]
            .into_iter()
            .enumerate()
            .map(|(index, host)| {
                let mut endpoint = Endpoint::try_from(format!("http://{host}:9000/disk{}", index + 1).as_str())
                    .expect("test endpoint should parse");
                endpoint.is_local = coordinator_local && index == 0;
                endpoint.set_pool_index(0);
                endpoint.set_set_index(index / 2);
                endpoint.set_disk_index(index % 2);
                endpoint
            })
            .collect::<Vec<_>>();
        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }])
    }

    fn mark_v2_authenticated<T>(request: &mut Request<T>) {
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-auth-version", "2".parse().expect("valid metadata value"));
    }

    fn signed_tier_prepare_request(mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Request<TierMutationPrepareRequest> {
        let mut request = Request::new(TierMutationPrepareRequest {
            version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            mutation_id: mutation_id.to_string(),
            canonical_payload,
        });
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(
            request.get_ref().version,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            &request.get_ref().canonical_payload,
        )
        .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);
        request
    }

    fn delete_request_message(options: &str) -> DeleteRequest {
        DeleteRequest {
            disk: "http://node-a:9000/data/rustfs0".to_string(),
            volume: "bucket".to_string(),
            path: "object".to_string(),
            options: options.to_string(),
            scanner_publication_lease_token: Vec::new().into(),
        }
    }

    #[tokio::test]
    async fn disk_mutation_body_digest_gate_runs_before_disk_lookup() {
        let service = make_server();

        // A digestless mutation stays accepted through the default fail-open gate (rolling
        // upgrade posture) and proceeds to the disk lookup.
        let digestless = service
            .delete(Request::new(delete_request_message("{}")))
            .await
            .expect("a digestless mutation must stay accepted while the strict gate is off");
        assert!(!digestless.into_inner().success, "the unknown test disk cannot resolve");

        // A digest bound to different request contents must be rejected before any disk work.
        let mut tampered = Request::new(delete_request_message("{}"));
        let other_body = rustfs_protos::canonical_delete_request_body(&delete_request_message("{\"recursive\":true}"))
            .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut tampered, &other_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut tampered);
        let tampered = service
            .delete(tampered)
            .await
            .expect_err("a tampered mutation must fail closed");
        assert_eq!(tampered.code(), tonic::Code::PermissionDenied);

        // A digest matching the received wire fields authenticates and proceeds to the disk lookup.
        let mut signed = Request::new(delete_request_message("{}"));
        let body = rustfs_protos::canonical_delete_request_body(signed.get_ref()).expect("small request should encode");
        set_tonic_canonical_body_digest(&mut signed, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut signed);
        let signed = service
            .delete(signed)
            .await
            .expect("a correctly body-bound mutation must pass the digest gate");
        assert!(!signed.into_inner().success, "the unknown test disk cannot resolve");
    }

    /// Per-handler wiring check for every mutating disk RPC. A mismatched digest must be rejected
    /// (catches a handler that omits its `verify_disk_mutation_digest` gate) and a correctly
    /// body-bound digest must pass the gate (catches a handler wired to the wrong
    /// `canonical_*_request_body`, which would reject legitimate traffic). Both failure modes are
    /// realistic across these copy-pasted call sites and are otherwise invisible to the digestless
    /// fail-open tests.
    #[tokio::test]
    async fn every_mutating_handler_enforces_its_body_digest() {
        let service = make_server();
        let disk = "http://node-a:9000/data/rustfs0".to_string();
        let mut covered_methods = HashSet::new();

        macro_rules! assert_gated {
            ($method:ident, $msg:expr, $canonical:path) => {{
                assert!(
                    covered_methods.insert(normalized_rpc_method(stringify!($method))),
                    concat!("duplicate disk mutation test for ", stringify!($method)),
                );
                let msg = $msg;

                // Correct digest: the gate passes and the handler proceeds to the (unknown) disk
                // lookup, so it must NOT fail with PermissionDenied.
                let mut ok = Request::new(msg.clone());
                let body = $canonical(ok.get_ref()).expect("canonical body should encode");
                set_tonic_canonical_body_digest(&mut ok, &body).expect("digest metadata should encode");
                mark_v2_authenticated(&mut ok);
                if let Err(status) = service.$method(ok).await {
                    assert_ne!(
                        status.code(),
                        tonic::Code::PermissionDenied,
                        concat!(stringify!($method), ": a correctly body-bound request must pass the digest gate"),
                    );
                }

                // Mismatched digest: the gate must reject before any disk work.
                let mut bad = Request::new(msg);
                set_tonic_canonical_body_digest(&mut bad, b"unrelated-canonical-body").expect("digest metadata should encode");
                mark_v2_authenticated(&mut bad);
                let err = service
                    .$method(bad)
                    .await
                    .expect_err(concat!(stringify!($method), ": a tampered body must be rejected"));
                assert_eq!(
                    err.code(),
                    tonic::Code::PermissionDenied,
                    concat!(stringify!($method), " must fail closed on a body-digest mismatch"),
                );
            }};
        }

        assert_gated!(
            rename_data,
            RenameDataRequest {
                disk: disk.clone(),
                src_volume: "src".into(),
                src_path: "sp".into(),
                file_info: "{}".into(),
                dst_volume: "dst".into(),
                dst_path: "dp".into(),
                file_info_bin: vec![0x80].into(),
                scanner_publication_lease_token: Vec::new().into(),
            },
            rustfs_protos::canonical_rename_data_request_body
        );
        assert_gated!(
            delete_version,
            DeleteVersionRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                file_info: "{}".into(),
                force_del_marker: false,
                opts: "{}".into(),
                file_info_bin: vec![0x80].into(),
                opts_bin: vec![0x80].into(),
            },
            rustfs_protos::canonical_delete_version_request_body
        );
        assert_gated!(
            delete_versions,
            DeleteVersionsRequest {
                disk: disk.clone(),
                volume: "v".into(),
                versions: vec!["a".into()],
                opts: "{}".into(),
                versions_bin: vec![vec![0x80].into()],
                opts_bin: vec![0x80].into(),
            },
            rustfs_protos::canonical_delete_versions_request_body
        );
        assert_gated!(
            write_metadata,
            WriteMetadataRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                file_info: "{}".into(),
                file_info_bin: vec![0x80].into(),
            },
            rustfs_protos::canonical_write_metadata_request_body
        );
        assert_gated!(
            update_metadata,
            UpdateMetadataRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                file_info: "{}".into(),
                opts: "{}".into(),
                file_info_bin: vec![0x80].into(),
                opts_bin: vec![0x80].into(),
            },
            rustfs_protos::canonical_update_metadata_request_body
        );
        assert_gated!(
            write_all,
            WriteAllRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                data: vec![0x01, 0x02].into(),
            },
            rustfs_protos::canonical_write_all_request_body
        );
        assert_gated!(
            delete,
            DeleteRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                options: "{}".into(),
                scanner_publication_lease_token: Vec::new().into(),
            },
            rustfs_protos::canonical_delete_request_body
        );
        assert_gated!(
            acquire_snapshot_lease,
            SnapshotLeaseRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                ttl_ms: 60_000,
            },
            rustfs_protos::canonical_snapshot_lease_request_body
        );
        assert_gated!(
            renew_snapshot_lease,
            SnapshotLeaseRenewRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                token: vec![1; 16].into(),
                ttl_ms: 60_000,
            },
            rustfs_protos::canonical_snapshot_lease_renew_request_body
        );
        assert_gated!(
            release_snapshot_lease,
            SnapshotLeaseReleaseRequest {
                disk: disk.clone(),
                volume: "v".into(),
                path: "p".into(),
                token: vec![1; 16].into(),
            },
            rustfs_protos::canonical_snapshot_lease_release_request_body
        );
        assert_gated!(
            delete_paths,
            DeletePathsRequest {
                disk: disk.clone(),
                volume: "v".into(),
                paths: vec!["a".into()],
            },
            rustfs_protos::canonical_delete_paths_request_body
        );
        assert_gated!(
            rename_file,
            RenameFileRequest {
                disk: disk.clone(),
                src_volume: "src".into(),
                src_path: "sp".into(),
                dst_volume: "dst".into(),
                dst_path: "dp".into(),
            },
            rustfs_protos::canonical_rename_file_request_body
        );
        assert_gated!(
            rename_part,
            RenamePartRequest {
                disk: disk.clone(),
                src_volume: "src".into(),
                src_path: "sp".into(),
                dst_volume: "dst".into(),
                dst_path: "dp".into(),
                meta: vec![0x03].into(),
            },
            rustfs_protos::canonical_rename_part_request_body
        );
        assert_gated!(
            prepare_part_transaction,
            PreparePartTransactionRequest {
                disk: disk.clone(),
                src_volume: "src".into(),
                src_path: "sp".into(),
                dst_volume: "dst".into(),
                dst_path: "dp".into(),
                meta: vec![0x04].into(),
            },
            rustfs_protos::canonical_prepare_part_transaction_request_body
        );
        assert_gated!(
            settle_part_transaction,
            SettlePartTransactionRequest {
                disk: disk.clone(),
                volume: "dst".into(),
                path: "dp".into(),
                rollback: true,
            },
            rustfs_protos::canonical_settle_part_transaction_request_body
        );
        assert_gated!(
            delete_volume,
            DeleteVolumeRequest {
                disk: disk.clone(),
                volume: "v".into(),
                force: true,
            },
            rustfs_protos::canonical_delete_volume_request_body
        );
        assert_gated!(
            make_volume,
            MakeVolumeRequest {
                disk: disk.clone(),
                volume: "v".into(),
            },
            rustfs_protos::canonical_make_volume_request_body
        );
        assert_gated!(
            make_volumes,
            MakeVolumesRequest {
                disk,
                volumes: vec!["v".into()],
            },
            rustfs_protos::canonical_make_volumes_request_body
        );

        let expected_methods = DISK_MUTATION_RPC_METHODS.into_iter().map(String::from).collect();
        assert_eq!(
            covered_methods, expected_methods,
            "the disk mutation exclusion set must exactly match handlers exercised by the independent digest test",
        );
    }

    #[tokio::test]
    async fn snapshot_lease_acquire_and_renew_handlers_fail_closed_for_missing_disk() {
        let service = make_server();
        let disk = "http://node-a:9000/data/rustfs0".to_string();

        let mut acquire = Request::new(SnapshotLeaseRequest {
            disk: disk.clone(),
            volume: "v".into(),
            path: "p".into(),
            ttl_ms: 60_000,
        });
        let acquire_body =
            rustfs_protos::canonical_snapshot_lease_request_body(acquire.get_ref()).expect("acquire request body should encode");
        set_tonic_canonical_body_digest(&mut acquire, &acquire_body).expect("acquire digest metadata should encode");
        mark_v2_authenticated(&mut acquire);
        let acquire = service
            .acquire_snapshot_lease(acquire)
            .await
            .expect("missing-disk acquire should return a protocol response")
            .into_inner();

        let mut renew = Request::new(SnapshotLeaseRenewRequest {
            disk,
            volume: "v".into(),
            path: "p".into(),
            token: vec![1; 16].into(),
            ttl_ms: 60_000,
        });
        let renew_body = rustfs_protos::canonical_snapshot_lease_renew_request_body(renew.get_ref())
            .expect("renew request body should encode");
        set_tonic_canonical_body_digest(&mut renew, &renew_body).expect("renew digest metadata should encode");
        mark_v2_authenticated(&mut renew);
        let renew = service
            .renew_snapshot_lease(renew)
            .await
            .expect("missing-disk renew should return a protocol response")
            .into_inner();

        for response in [acquire, renew] {
            assert!(!response.success);
            assert!(response.token.is_empty());
            assert_eq!(response.protocol_version, 1);
            assert_eq!(response.error, Some(DiskError::other("cannot find disk").into()));
        }
    }

    #[tokio::test]
    async fn heal_control_requires_body_bound_auth_before_topology_validation() {
        let service = make_heal_control_server();
        let unsigned = service
            .heal_control(heal_control_request(b"query"))
            .await
            .expect_err("unsigned request must fail");
        assert_eq!(unsigned.code(), tonic::Code::PermissionDenied);

        let mut tampered = heal_control_request(b"query");
        let other_body = rustfs_protos::canonical_heal_control_request_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            "fingerprint",
            b"cancel",
        )
        .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut tampered, &other_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut tampered);
        let tampered = service.heal_control(tampered).await.expect_err("tampered request must fail");
        assert_eq!(tampered.code(), tonic::Code::PermissionDenied);

        let mut signed = heal_control_request(b"query");
        let body = rustfs_protos::canonical_heal_control_request_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            "fingerprint",
            b"query",
        )
        .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut signed, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut signed);
        let unavailable = service
            .heal_control(signed)
            .await
            .expect_err("authenticated commands still require initialized topology");
        assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn tier_mutation_control_requires_body_bound_auth_before_store_lookup() {
        let _ = rustfs_credentials::set_global_rpc_secret("tier-mutation-control-auth-test-secret".to_string());
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let unsigned = service
            .prepare_tier_mutation(Request::new(TierMutationPrepareRequest {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                mutation_id: mutation_id.to_string(),
                canonical_payload: Bytes::from_static(b"intent"),
            }))
            .await
            .expect_err("unsigned request must fail before store lookup");
        assert_eq!(unsigned.code(), tonic::Code::PermissionDenied);

        let mut tampered = signed_tier_prepare_request(mutation_id, Bytes::from_static(b"intent"));
        let other_body = rustfs_protos::canonical_tier_mutation_rpc_body(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            rustfs_protos::TierMutationRpcPhase::Commit,
            mutation_id,
            b"intent",
        )
        .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut tampered, &other_body).expect("digest metadata should encode");
        let tampered = service
            .prepare_tier_mutation(tampered)
            .await
            .expect_err("phase replay must fail body-bound authentication");
        assert_eq!(tampered.code(), tonic::Code::PermissionDenied);

        let signed = signed_tier_prepare_request(mutation_id, Bytes::from_static(b"intent"));
        let unavailable = service
            .prepare_tier_mutation(signed)
            .await
            .expect("authenticated v4 pre-dispatch rejection should be signed")
            .into_inner();
        assert!(!unavailable.success);
        assert_eq!(unavailable.failure_class, TierMutationFailureClass::PreDispatchRejected as i32);
        assert_eq!(unavailable.error_info.as_deref(), Some("tier mutation object store is not initialized"));
        let canonical =
            rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: rustfs_protos::TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: b"intent",
                success: false,
                state: TierMutationPeerState::Unspecified as i32,
                applied: false,
                error_info: Some("tier mutation object store is not initialized"),
                failure_class: TierMutationFailureClass::PreDispatchRejected as i32,
            })
            .expect("small response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical, &unavailable.response_proof)
            .expect("v4 pre-dispatch rejection must authenticate its failure class");
    }

    #[tokio::test]
    async fn tier_mutation_control_requires_canonical_mutation_id() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4().to_string().to_uppercase();
        let error = service
            .prepare_tier_mutation(Request::new(TierMutationPrepareRequest {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                mutation_id,
                canonical_payload: Bytes::from_static(b"intent"),
            }))
            .await
            .expect_err("uppercase UUID must not pass canonical request binding");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn tier_mutation_control_rejects_unsupported_protocol_version_before_store_lookup() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let payload = Bytes::from_static(b"intent");
        let unsupported_version = rustfs_protos::TIER_MUTATION_RPC_PREVIOUS_PROTOCOL_VERSION - 1;
        let mut request = Request::new(TierMutationPrepareRequest {
            version: unsupported_version,
            mutation_id: mutation_id.to_string(),
            canonical_payload: payload,
        });
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(
            request.get_ref().version,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            &request.get_ref().canonical_payload,
        )
        .expect("old-version request should encode for rejection test");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);

        let error = service
            .prepare_tier_mutation(request)
            .await
            .expect_err("old tier mutation protocol version must fail closed");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            error.message(),
            format!("unsupported tier mutation peer protocol version: {}", unsupported_version)
        );
    }

    #[tokio::test]
    async fn tier_mutation_control_accepts_v3_before_store_lookup() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let payload = Bytes::from_static(b"intent");
        let mut request = Request::new(TierMutationPrepareRequest {
            version: rustfs_protos::TIER_MUTATION_RPC_PREVIOUS_PROTOCOL_VERSION,
            mutation_id: mutation_id.to_string(),
            canonical_payload: payload,
        });
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(
            request.get_ref().version,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            &request.get_ref().canonical_payload,
        )
        .expect("v3 request should encode");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);

        let error = service
            .prepare_tier_mutation(request)
            .await
            .expect_err("accepted v3 request should reach the missing-store check");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(error.message(), "tier mutation object store is not initialized");
    }

    #[tokio::test]
    async fn tier_mutation_control_rejects_oversized_prepare_before_auth_and_store_lookup() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let oversized = Bytes::from(vec![0; rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE + 1]);
        let error = service
            .prepare_tier_mutation(Request::new(TierMutationPrepareRequest {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                mutation_id: mutation_id.to_string(),
                canonical_payload: oversized,
            }))
            .await
            .expect_err("oversized prepare must fail before digest construction");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn tier_mutation_control_rejects_invalid_abort_payload_before_auth_and_store_lookup() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let empty = service
            .abort_tier_mutation(Request::new(TierMutationAbortRequest {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                mutation_id: mutation_id.to_string(),
                canonical_payload: Bytes::new(),
            }))
            .await
            .expect_err("empty abort must fail before digest construction");
        assert_eq!(empty.code(), tonic::Code::InvalidArgument);

        let oversized = Bytes::from(vec![0; rustfs_protos::TIER_MUTATION_RPC_MAX_ABORT_PAYLOAD_SIZE + 1]);
        let error = service
            .abort_tier_mutation(Request::new(TierMutationAbortRequest {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                mutation_id: mutation_id.to_string(),
                canonical_payload: oversized,
            }))
            .await
            .expect_err("oversized abort must fail before digest construction");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn tier_mutation_peer_state_wire_constants_match_generated_proto() {
        assert_eq!(
            super::TIER_MUTATION_PEER_STATE_UNSPECIFIED_WIRE,
            TierMutationPeerState::Unspecified as i32
        );
        assert_eq!(super::TIER_MUTATION_PEER_STATE_PREPARED_WIRE, TierMutationPeerState::Prepared as i32);
        assert_eq!(super::TIER_MUTATION_PEER_STATE_COMMITTED_WIRE, TierMutationPeerState::Committed as i32);
        assert_eq!(super::TIER_MUTATION_PEER_STATE_ABORTED_WIRE, TierMutationPeerState::Aborted as i32);
    }

    #[test]
    fn tier_mutation_error_info_bound_preserves_utf8_and_byte_limit() {
        let ascii = "a".repeat(rustfs_protos::TIER_MUTATION_RPC_MAX_ERROR_INFO_SIZE + 1);
        let bounded = super::bounded_tier_mutation_error_info(ascii);
        assert_eq!(bounded.len(), rustfs_protos::TIER_MUTATION_RPC_MAX_ERROR_INFO_SIZE);

        let unicode = "界".repeat(rustfs_protos::TIER_MUTATION_RPC_MAX_ERROR_INFO_SIZE);
        let bounded = super::bounded_tier_mutation_error_info(unicode);
        assert!(bounded.len() <= rustfs_protos::TIER_MUTATION_RPC_MAX_ERROR_INFO_SIZE);
        assert!(bounded.is_char_boundary(bounded.len()));
        assert!(bounded.chars().all(|character| character == '界'));
    }

    #[test]
    fn tier_mutation_control_response_proof_binds_request_and_result() {
        let _ = rustfs_credentials::set_global_rpc_secret("tier-mutation-control-response-proof-test-secret".to_string());
        let mutation_id = uuid::Uuid::new_v4();
        let payload = b"canonical-intent-record";
        let response = super::tier_mutation_control_response(super::TierMutationControlResponseInput {
            version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            phase: rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            canonical_payload: payload,
            success: false,
            state: TierMutationPeerState::Unspecified as i32,
            applied: false,
            error_info: Some("store failed".to_string()),
            failure_class: TierMutationFailureClass::Ambiguous as i32,
        })
        .expect("response proof should be signed")
        .into_inner();
        let canonical =
            rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: rustfs_protos::TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: false,
                state: TierMutationPeerState::Unspecified as i32,
                applied: false,
                error_info: Some("store failed"),
                failure_class: TierMutationFailureClass::Ambiguous as i32,
            })
            .expect("small mutation response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical, &response.response_proof)
            .expect("proof must authenticate the exact response");

        let tampered =
            rustfs_protos::canonical_tier_mutation_rpc_response_body(rustfs_protos::TierMutationRpcResponseProofInput {
                version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: rustfs_protos::TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Unspecified as i32,
                applied: false,
                error_info: Some("store failed"),
                failure_class: TierMutationFailureClass::Ambiguous as i32,
            })
            .expect("small mutation response should encode");
        let error = crate::storage::storage_api::verify_tonic_rpc_response_proof(&tampered, &response.response_proof)
            .expect_err("proof must reject a tampered success flag");
        assert_eq!(error.to_string(), "Invalid RPC response proof");
    }

    #[tokio::test]
    async fn heal_control_rejects_oversized_command_before_canonical_copy() {
        let service = make_heal_control_server();
        let oversized = service
            .heal_control(heal_control_request(&vec![0; HEAL_CONTROL_PAYLOAD_MAX_SIZE + 1]))
            .await
            .expect_err("oversized request must fail");
        assert_eq!(oversized.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn heal_control_probe_requires_exact_topology_and_coordinator() {
        let _ = rustfs_credentials::set_global_rpc_secret("heal-control-node-service-test-secret".to_string());
        let endpoints = heal_control_test_endpoints("node-d");
        let fingerprint = heal_topology_fingerprint(&endpoints).expect("test topology should hash");
        let (service, source) = super::make_heal_control_server_for_source();
        *source.write().await = Some(endpoints);

        let probe_command = rustfs_protos::heal_control_capability_probe(&[7; 16]);
        let mut probe = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from(probe_command.clone()),
        });
        let body = rustfs_protos::canonical_heal_control_request_body(
            probe.get_ref().version,
            &probe.get_ref().topology_fingerprint,
            &probe.get_ref().command,
        )
        .expect("probe should encode");
        set_tonic_canonical_body_digest(&mut probe, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut probe);
        let response = service
            .heal_control(probe)
            .await
            .expect("matching topology should be acknowledged");
        let canonical_ack = rustfs_protos::canonical_heal_control_capability_ack(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            &probe_command,
        )
        .expect("acknowledgement should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical_ack, &response.into_inner().result)
            .expect("response proof should authenticate the exact acknowledgement");

        let mut old_probe = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION - 1,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from(probe_command.clone()),
        });
        let old_body = rustfs_protos::canonical_heal_control_request_body(
            old_probe.get_ref().version,
            &old_probe.get_ref().topology_fingerprint,
            &old_probe.get_ref().command,
        )
        .expect("old probe should encode for rejection test");
        set_tonic_canonical_body_digest(&mut old_probe, &old_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut old_probe);
        let old_version = service
            .heal_control(old_probe)
            .await
            .expect_err("old coordination capability must fail closed");
        assert_eq!(old_version.code(), tonic::Code::FailedPrecondition);

        let divergent_probe = rustfs_protos::heal_control_capability_probe(&[8; 16]);
        let mut divergent = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: heal_topology_fingerprint(&heal_control_test_endpoints("node-e"))
                .expect("divergent topology should hash"),
            command: Bytes::from(divergent_probe),
        });
        let body = rustfs_protos::canonical_heal_control_request_body(
            divergent.get_ref().version,
            &divergent.get_ref().topology_fingerprint,
            &divergent.get_ref().command,
        )
        .expect("probe should encode");
        set_tonic_canonical_body_digest(&mut divergent, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut divergent);
        let mismatch = service
            .heal_control(divergent)
            .await
            .expect_err("divergent topology must fail closed");
        assert_eq!(mismatch.code(), tonic::Code::FailedPrecondition);

        let mut command = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from_static(b"start"),
        });
        let body = rustfs_protos::canonical_heal_control_request_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            b"start",
        )
        .expect("command should encode");
        set_tonic_canonical_body_digest(&mut command, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut command);
        let non_coordinator = service
            .heal_control(command)
            .await
            .expect_err("commands must be rejected by a non-coordinator node");
        assert_eq!(non_coordinator.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn remote_version_state_probe_authenticates_topology_challenge_and_process_epoch() {
        let _ = rustfs_credentials::set_global_rpc_secret("remote-version-state-node-service-test-secret".to_string());
        let endpoints = heal_control_test_endpoints_with_coordinator("node-d", true);
        let fingerprint = heal_topology_fingerprint(&endpoints).expect("test topology should hash");
        let (service, source) = super::make_heal_control_server_for_source();
        *source.write().await = Some(endpoints);
        let probe_command = rustfs_protos::remote_version_state_capability_probe(&[7; 16]);
        let mut request = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from(probe_command.clone()),
        });
        let body = rustfs_protos::canonical_heal_control_request_body(
            request.get_ref().version,
            &request.get_ref().topology_fingerprint,
            &request.get_ref().command,
        )
        .expect("probe should encode");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);
        let response = service
            .heal_control(request)
            .await
            .expect("matching topology should be acknowledged")
            .into_inner();

        let (topology_member, process_epoch) =
            rustfs_protos::decode_remote_version_state_capability(&response.result).expect("capability response should decode");
        assert_eq!(topology_member, "node-a:9000");
        let server_epoch = Uuid::from_slice(process_epoch).expect("server epoch should be a UUID");
        assert!(!server_epoch.is_nil());
        let canonical_response = rustfs_protos::canonical_heal_control_response_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            &probe_command,
            &response.result,
        )
        .expect("response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical_response, &response.response_proof)
            .expect("outer proof should bind the response to the request");

        let different_probe = rustfs_protos::remote_version_state_capability_probe(&[8; 16]);
        let different_response = rustfs_protos::canonical_heal_control_response_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            &different_probe,
            &response.result,
        )
        .expect("different response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&different_response, &response.response_proof)
            .expect_err("proof from one challenge must not be reusable");
    }

    #[test]
    fn ilm_recovery_export_probe_uses_the_shared_local_process_epoch() {
        let result = super::encode_heal_capability_response("node-a:9000", false, true)
            .expect("ILM recovery export capability should encode");
        let (member, epoch) =
            rustfs_protos::decode_remote_version_state_capability(&result).expect("ILM recovery export capability should decode");
        assert_eq!(member, "node-a:9000");
        assert_eq!(
            Uuid::from_slice(epoch).expect("capability epoch should be a UUID"),
            crate::storage::storage_api::ilm_recovery_export_local_process_epoch(),
        );
    }

    #[tokio::test]
    async fn cross_pool_fence_probe_authenticates_supported_v4_state() {
        let _ = rustfs_credentials::set_global_rpc_secret("cross-pool-fence-node-service-test-secret".to_string());
        let endpoints = heal_control_test_endpoints_with_coordinator("node-0", true);
        assert!(
            !super::heal::heal_control_coordinator(&endpoints)
                .expect("test topology should have a coordinator")
                .is_local
        );
        let fingerprint = heal_topology_fingerprint(&endpoints).expect("test topology should hash");
        let (service, source) = super::make_heal_control_server_for_source();
        *source.write().await = Some(endpoints);
        let mut probe_command = rustfs_protos::CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX.to_vec();
        probe_command.extend_from_slice(&[7; 16]);

        let unauthenticated = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from(probe_command.clone()),
        });
        let auth_error = service
            .heal_control(unauthenticated)
            .await
            .expect_err("capability probe without authentication must fail closed");
        assert_eq!(auth_error.code(), tonic::Code::PermissionDenied);

        let mut divergent = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: "different-topology".to_string(),
            command: Bytes::from(probe_command.clone()),
        });
        let divergent_body = rustfs_protos::canonical_heal_control_request_body(
            divergent.get_ref().version,
            &divergent.get_ref().topology_fingerprint,
            &divergent.get_ref().command,
        )
        .expect("divergent probe should encode");
        set_tonic_canonical_body_digest(&mut divergent, &divergent_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut divergent);
        let topology_error = service
            .heal_control(divergent)
            .await
            .expect_err("capability probe for a different topology must fail closed");
        assert_eq!(topology_error.code(), tonic::Code::FailedPrecondition);

        let mut request = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: fingerprint.clone(),
            command: Bytes::from(probe_command.clone()),
        });
        let body = rustfs_protos::canonical_heal_control_request_body(
            request.get_ref().version,
            &request.get_ref().topology_fingerprint,
            &request.get_ref().command,
        )
        .expect("probe should encode");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);
        let response = service
            .heal_control(request)
            .await
            .expect("non-coordinator peer should answer a capability probe")
            .into_inner();

        assert!(response.success);
        assert_eq!(response.error_info, None);
        assert_eq!(&response.result[..4], &CROSS_POOL_FENCE_SUPPORTED_VERSION.to_be_bytes());
        let (topology_member, process_epoch) = rustfs_protos::decode_remote_version_state_capability(&response.result[4..])
            .expect("capability identity should decode");
        assert_eq!(topology_member, "node-a:9000");
        assert!(
            !Uuid::from_slice(process_epoch)
                .expect("server epoch should be a UUID")
                .is_nil()
        );

        let canonical_response = rustfs_protos::canonical_heal_control_response_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            &probe_command,
            &response.result,
        )
        .expect("response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical_response, &response.response_proof)
            .expect("outer proof should bind the response to the request");

        let mut different_probe = rustfs_protos::CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX.to_vec();
        different_probe.extend_from_slice(&[8; 16]);
        let different_response = rustfs_protos::canonical_heal_control_response_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            &fingerprint,
            &different_probe,
            &response.result,
        )
        .expect("different response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&different_response, &response.response_proof)
            .expect_err("proof from one challenge must not be reusable");
    }

    #[tokio::test]
    async fn heal_control_coordinator_rejects_expired_and_non_admin_starts() {
        let _ = rustfs_credentials::set_global_rpc_secret("heal-control-node-service-test-secret".to_string());
        let endpoints = heal_control_test_endpoints_with_coordinator("node-d", true);
        let fingerprint = heal_topology_fingerprint(&endpoints).expect("test topology should hash");
        let coordinator_epoch =
            rustfs_protos::heal_control_coordinator_epoch(&fingerprint).expect("test topology should have an epoch");
        let (service, source) = super::make_heal_control_server_for_source();
        *source.write().await = Some(endpoints);

        fn signed_command(fingerprint: &str, command: Vec<u8>) -> Request<HealControlRequest> {
            let mut request = Request::new(HealControlRequest {
                version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
                topology_fingerprint: fingerprint.to_string(),
                command: command.into(),
            });
            let body = rustfs_protos::canonical_heal_control_request_body(
                request.get_ref().version,
                &request.get_ref().topology_fingerprint,
                &request.get_ref().command,
            )
            .expect("command should encode");
            set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
            mark_v2_authenticated(&mut request);
            request
        }

        let expired_request = rustfs_heal_contracts::heal_channel::create_heal_request("bucket".to_string(), None, false, None);
        let expired = rustfs_protos::heal_control::Envelope::start(
            expired_request,
            rustfs_protos::heal_control::RequestMetadata::new([1; 16], 1, 2, coordinator_epoch),
        )
        .and_then(|envelope| rustfs_protos::heal_control::encode_envelope(&envelope))
        .expect("expired command should encode structurally");
        let expired = service
            .heal_control(signed_command(&fingerprint, expired))
            .await
            .expect_err("expired commands must fail before admission");
        assert_eq!(expired.code(), tonic::Code::FailedPrecondition);

        let mut non_admin_request =
            rustfs_heal_contracts::heal_channel::create_heal_request("bucket".to_string(), None, false, None);
        non_admin_request.source = rustfs_heal_contracts::heal_channel::HealRequestSource::Scanner;
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let now = i64::try_from(now).expect("test clock should fit in i64");
        let non_admin = rustfs_protos::heal_control::Envelope::start(
            non_admin_request,
            rustfs_protos::heal_control::RequestMetadata::new([2; 16], now, now + 1_000, coordinator_epoch),
        )
        .and_then(|envelope| rustfs_protos::heal_control::encode_envelope(&envelope))
        .expect("non-admin command should encode structurally");
        let non_admin = service
            .heal_control(signed_command(&fingerprint, non_admin))
            .await
            .expect_err("non-admin commands must fail before admission");
        assert_eq!(non_admin.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn server_owned_heal_topology_initialization_only_publishes_valid_layouts() {
        let topology = heal_control_test_endpoints("node-d");
        let expected = heal_topology_fingerprint(&topology).expect("test topology should hash");
        let cache = Arc::new(tokio::sync::OnceCell::new());
        let started_probe = Arc::new(std::sync::Mutex::new(None));
        let started_probe_capture = Arc::clone(&started_probe);
        initialize_heal_topology_fingerprint_with_probe(Arc::clone(&cache), topology, move |fingerprint| {
            *started_probe_capture.lock().expect("probe capture should not poison") = Some(fingerprint);
        })
        .await
        .expect("valid topology should initialize");
        assert_eq!(cache.get(), Some(&expected));
        assert_eq!(started_probe.lock().expect("probe capture should not poison").as_ref(), Some(&expected));

        let mut invalid = heal_control_test_endpoints("node-d");
        invalid.as_mut()[0].endpoints.as_mut()[0].pool_idx = -1;
        let invalid_cache = Arc::new(tokio::sync::OnceCell::new());
        initialize_heal_topology_fingerprint(Arc::clone(&invalid_cache), invalid)
            .await
            .expect_err("invalid topology must fail closed");
        assert!(invalid_cache.get().is_none());
    }

    #[tokio::test]
    async fn test_ping_success() {
        let service = create_test_node_service();

        // Create a valid ping request with flatbuffer body
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"test payload");
        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let request = Request::new(PingRequest {
            version: 1,
            body: Bytes::copy_from_slice(fbb.finished_data()),
        });

        let response = service.ping(request).await;
        assert!(response.is_ok());

        let ping_response = response.unwrap().into_inner();
        assert_eq!(ping_response.version, 1);
        assert!(!ping_response.body.is_empty());
    }

    #[tokio::test]
    async fn test_ping_with_invalid_flatbuffer() {
        let service = create_test_node_service();

        let request = Request::new(PingRequest {
            version: 1,
            body: vec![0x00, 0x01, 0x02].into(), // Invalid flatbuffer data
        });

        let response = service.ping(request).await;
        assert!(response.is_ok()); // Should still succeed but log error

        let ping_response = response.unwrap().into_inner();
        assert_eq!(ping_response.version, 1);
        assert!(!ping_response.body.is_empty());
    }

    #[tokio::test]
    async fn test_ping_with_empty_body() {
        let service = create_test_node_service();

        let request = Request::new(PingRequest {
            version: 1,
            body: Bytes::new(),
        });

        let response = service.ping(request).await;
        assert!(response.is_ok());

        let ping_response = response.unwrap().into_inner();
        assert_eq!(ping_response.version, 1);
        assert!(!ping_response.body.is_empty());
    }

    #[tokio::test]
    async fn test_heal_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(HealBucketRequest {
            bucket: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.heal_bucket(request).await;
        assert!(response.is_ok());

        let heal_response = response.unwrap().into_inner();
        assert!(!heal_response.success);
        assert!(heal_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(ListBucketRequest {
            options: "invalid json".to_string(),
        });

        let response = service.list_bucket(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.bucket_infos.is_empty());
    }

    #[tokio::test]
    async fn test_make_bucket_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(MakeBucketRequest {
            name: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.make_bucket(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_get_bucket_info_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(GetBucketInfoRequest {
            bucket: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service.get_bucket_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
        assert!(info_response.bucket_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let service = create_test_node_service();

        let request = Request::new(DeleteBucketRequest {
            bucket: "test-bucket".to_string(),
            options: String::new(),
        });

        let response = service.delete_bucket(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        // Response should be valid regardless of success/failure
        assert!(delete_response.success || delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_bucket_rejects_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(DeleteBucketRequest {
            bucket: "test-bucket".to_string(),
            options: "invalid json".to_string(),
        });

        let response = service
            .delete_bucket(request)
            .await
            .expect("RPC response should be returned")
            .into_inner();
        assert!(!response.success);
        assert!(response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_all_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadAllRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
        });

        let response = service.read_all(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.data.is_empty());
    }

    #[tokio::test]
    async fn test_write_all_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(WriteAllRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            data: vec![1, 2, 3, 4].into(),
        });

        let response = service.write_all(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            options: "{}".to_string(),
            scanner_publication_lease_token: Vec::new().into(),
        });

        let response = service.delete(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_invalid_options() {
        let service = create_test_node_service();

        let request = Request::new(DeleteRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            options: "invalid json".to_string(),
            scanner_publication_lease_token: Vec::new().into(),
        });

        let response = service.delete(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_verify_file_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(VerifyFileRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
        });

        let response = service.verify_file(request).await;
        assert!(response.is_ok());

        let verify_response = response.unwrap().into_inner();
        assert!(!verify_response.success);
        assert!(verify_response.error.is_some());
        assert!(verify_response.check_parts_resp.is_empty());
    }

    #[tokio::test]
    async fn test_verify_file_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(VerifyFileRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.verify_file(request).await;
        assert!(response.is_ok());

        let verify_response = response.unwrap().into_inner();
        assert!(!verify_response.success);
        assert!(verify_response.error.is_some());
    }

    #[tokio::test]
    async fn test_check_parts_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(CheckPartsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
        });

        let response = service.check_parts(request).await;
        assert!(response.is_ok());

        let check_response = response.unwrap().into_inner();
        assert!(!check_response.success);
        assert!(check_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_part_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenamePartRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            meta: Bytes::new(),
        });

        let response = service.rename_part(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_part_transaction_invalid_disk() {
        let service = create_test_node_service();

        let prepare = service
            .prepare_part_transaction(Request::new(PreparePartTransactionRequest {
                disk: "invalid-disk-path".to_string(),
                src_volume: "src-volume".to_string(),
                src_path: "src-path".to_string(),
                dst_volume: "dst-volume".to_string(),
                dst_path: "dst-path".to_string(),
                meta: Bytes::new(),
            }))
            .await
            .expect("prepare RPC should return a structured disk error")
            .into_inner();
        assert!(!prepare.success);
        assert!(prepare.error.is_some());

        let settle = service
            .settle_part_transaction(Request::new(SettlePartTransactionRequest {
                disk: "invalid-disk-path".to_string(),
                volume: "dst-volume".to_string(),
                path: "dst-path".to_string(),
                rollback: true,
            }))
            .await
            .expect("settle RPC should return a structured disk error")
            .into_inner();
        assert!(!settle.success);
        assert!(settle.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_file_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenameFileRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
        });

        let response = service.rename_file(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_dir_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ListDirRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            dir_path: "test-dir-path".to_string(),
            count: 10,
        });

        let response = service.list_dir(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.volumes.is_empty());
    }

    #[tokio::test]
    async fn test_rename_data_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(RenameDataRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            file_info: "{}".to_string(),
            file_info_bin: Vec::new().into(),
            scanner_publication_lease_token: Vec::new().into(),
        });

        let response = service.rename_data(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rename_data_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(RenameDataRequest {
            disk: "invalid-disk-path".to_string(),
            src_volume: "src-volume".to_string(),
            src_path: "src-path".to_string(),
            dst_volume: "dst-volume".to_string(),
            dst_path: "dst-path".to_string(),
            file_info: "invalid json".to_string(),
            file_info_bin: Vec::new().into(),
            scanner_publication_lease_token: Vec::new().into(),
        });

        let response = service.rename_data(request).await;
        assert!(response.is_ok());

        let rename_response = response.unwrap().into_inner();
        assert!(!rename_response.success);
        assert!(rename_response.error.is_some());
    }

    #[tokio::test]
    async fn test_make_volumes_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(MakeVolumesRequest {
            disk: "invalid-disk-path".to_string(),
            volumes: vec!["volume1".to_string(), "volume2".to_string()],
        });

        let response = service.make_volumes(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_make_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(MakeVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
        });

        let response = service.make_volume(request).await;
        assert!(response.is_ok());

        let make_response = response.unwrap().into_inner();
        assert!(!make_response.success);
        assert!(make_response.error.is_some());
    }

    #[tokio::test]
    async fn test_list_volumes_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ListVolumesRequest {
            disk: "invalid-disk-path".to_string(),
        });

        let response = service.list_volumes(request).await;
        assert!(response.is_ok());

        let list_response = response.unwrap().into_inner();
        assert!(!list_response.success);
        assert!(list_response.error.is_some());
        assert!(list_response.volume_infos.is_empty());
    }

    #[tokio::test]
    async fn test_stat_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(StatVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
        });

        let response = service.stat_volume(request).await;
        assert!(response.is_ok());

        let stat_response = response.unwrap().into_inner();
        assert!(!stat_response.success);
        assert!(stat_response.error.is_some());
        assert!(stat_response.volume_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_paths_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeletePathsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            paths: vec!["path1".to_string(), "path2".to_string()],
        });

        let response = service.delete_paths(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            opts: "{}".to_string(),
            file_info_bin: Vec::new().into(),
            opts_bin: Vec::new().into(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
            opts: "{}".to_string(),
            file_info_bin: Vec::new().into(),
            opts_bin: Vec::new().into(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_update_metadata_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(UpdateMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            opts: "invalid json".to_string(),
            file_info_bin: Vec::new().into(),
            opts_bin: Vec::new().into(),
        });

        let response = service.update_metadata(request).await;
        assert!(response.is_ok());

        let update_response = response.unwrap().into_inner();
        assert!(!update_response.success);
        assert!(update_response.error.is_some());
    }

    #[tokio::test]
    async fn test_write_metadata_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(WriteMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            file_info_bin: Vec::new().into(),
        });

        let response = service.write_metadata(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_write_metadata_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(WriteMetadataRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
            file_info_bin: Vec::new().into(),
        });

        let response = service.write_metadata(request).await;
        assert!(response.is_ok());

        let write_response = response.unwrap().into_inner();
        assert!(!write_response.success);
        assert!(write_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_version_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            version_id: "version1".to_string(),
            opts: "{}".to_string(),
            opts_bin: Vec::new().into(),
        });

        let response = service.read_version(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.file_info.is_empty());
    }

    #[tokio::test]
    async fn test_read_version_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(ReadVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            version_id: "version1".to_string(),
            opts: "invalid json".to_string(),
            opts_bin: Vec::new().into(),
        });

        let response = service.read_version(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_xl_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadXlRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            read_data: true,
        });

        let response = service.read_xl(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.raw_file_info.is_empty());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            force_del_marker: false,
            opts: "{}".to_string(),
            ..Default::default()
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_file_info() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "invalid json".to_string(),
            force_del_marker: false,
            opts: "{}".to_string(),
            ..Default::default()
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_version_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            path: "test-path".to_string(),
            file_info: "{}".to_string(),
            force_del_marker: false,
            opts: "invalid json".to_string(),
            ..Default::default()
        });

        let response = service.delete_version(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["{}".to_string()],
            opts: "{}".to_string(),
            ..Default::default()
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_versions() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["invalid json".to_string()],
            opts: "{}".to_string(),
            ..Default::default()
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_versions_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVersionsRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            versions: vec!["{}".to_string()],
            opts: "invalid json".to_string(),
            ..Default::default()
        });

        let response = service.delete_versions(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_read_multiple_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(ReadMultipleRequest {
            disk: "invalid-disk-path".to_string(),
            read_multiple_req: "{}".to_string(),
            read_multiple_req_bin: Vec::new().into(),
        });

        let response = service.read_multiple(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
        assert!(read_response.read_multiple_resps.is_empty());
    }

    #[tokio::test]
    async fn test_read_multiple_invalid_request() {
        let service = create_test_node_service();

        let request = Request::new(ReadMultipleRequest {
            disk: "invalid-disk-path".to_string(),
            read_multiple_req: "invalid json".to_string(),
            read_multiple_req_bin: Vec::new().into(),
        });

        let response = service.read_multiple(request).await;
        assert!(response.is_ok());

        let read_response = response.unwrap().into_inner();
        assert!(!read_response.success);
        assert!(read_response.error.is_some());
    }

    #[tokio::test]
    async fn test_delete_volume_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DeleteVolumeRequest {
            disk: "invalid-disk-path".to_string(),
            volume: "test-volume".to_string(),
            force: false,
        });

        let response = service.delete_volume(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error.is_some());
    }

    #[tokio::test]
    async fn test_disk_info_invalid_disk() {
        let service = create_test_node_service();

        let request = Request::new(DiskInfoRequest {
            disk: "invalid-disk-path".to_string(),
            opts: "{}".to_string(),
        });

        let response = service.disk_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
        assert!(info_response.disk_info.is_empty());
    }

    #[tokio::test]
    async fn test_disk_info_invalid_opts() {
        let service = create_test_node_service();

        let request = Request::new(DiskInfoRequest {
            disk: "invalid-disk-path".to_string(),
            opts: "invalid json".to_string(),
        });

        let response = service.disk_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(!info_response.success);
        assert!(info_response.error.is_some());
    }

    #[tokio::test]
    async fn test_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.lock(request).await;
        assert!(response.is_ok());

        let lock_response = response.unwrap().into_inner();
        assert!(!lock_response.success);
        assert!(lock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_un_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.un_lock(request).await;
        assert!(response.is_ok());

        let unlock_response = response.unwrap().into_inner();
        assert!(!unlock_response.success);
        assert!(unlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_force_un_lock_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.force_un_lock(request).await;
        assert!(response.is_ok());

        let force_unlock_response = response.unwrap().into_inner();
        assert!(!force_unlock_response.success);
        assert!(force_unlock_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_refresh_invalid_args() {
        let service = create_test_node_service();

        let request = Request::new(GenerallyLockRequest {
            args: "invalid json".to_string(),
        });

        let response = service.refresh(request).await;
        assert!(response.is_ok());

        let refresh_response = response.unwrap().into_inner();
        assert!(!refresh_response.success);
        assert!(refresh_response.error_info.is_some());
    }

    #[tokio::test]
    async fn lock_rolling_unsigned_v2_remains_compatible_for_unknown_peer() {
        let service = create_test_node_service();
        let unsigned_request = || {
            let mut request = Request::new(GenerallyLockRequest {
                args: "invalid json".to_string(),
            });
            request
                .metadata_mut()
                .insert("x-rustfs-rpc-auth-version", "2".parse().expect("valid metadata value"));
            request
                .metadata_mut()
                .insert("x-rustfs-content-sha256", "UNSIGNED-PAYLOAD".parse().expect("valid metadata value"));
            request
        };

        let lock = service
            .lock(unsigned_request())
            .await
            .expect("unsigned lock must pass the rolling body gate");
        assert!(!lock.into_inner().success, "invalid test lock args should fail in the lock handler");
        let unlock = service
            .un_lock(unsigned_request())
            .await
            .expect("unsigned unlock must pass the rolling body gate");
        assert!(!unlock.into_inner().success, "invalid test unlock args should fail in the unlock handler");
    }

    /// Premise guard for the no-object-layer RPC tests (backlog#1830): they
    /// assert the error surface returned while the global object layer is
    /// absent. Under nextest — the authoritative runner — every test owns its
    /// process, so the premise always holds and the assertion always runs.
    /// Under the documented shared-process `cargo test` fallback a sibling test
    /// may have initialized the store first; the premise is then unattainable,
    /// so the test skips instead of asserting against a scenario it does not
    /// describe.
    fn no_object_layer_premise_holds() -> bool {
        if crate::runtime_sources::current_object_store_handle().is_some() {
            eprintln!("skipping no-object-layer assertion: a sibling test already initialized the global object layer");
            return false;
        }
        true
    }

    #[tokio::test]
    async fn test_local_storage_info() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(LocalStorageInfoRequest { metrics: false });

        let response = service.local_storage_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!info_response.success);
        assert!(info_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_server_info() {
        let service = create_test_node_service();

        let request = Request::new(ServerInfoRequest { metrics: false });

        let response = service.server_info(request).await;
        assert!(response.is_ok());

        let info_response = response.unwrap().into_inner();
        assert!(info_response.success);
        assert!(!info_response.server_properties.is_empty());
    }

    #[tokio::test]
    async fn test_get_cpus() {
        let service = create_test_node_service();

        let request = Request::new(GetCpusRequest {});

        let response = service.get_cpus(request).await;
        assert!(response.is_ok());

        let cpus_response = response.unwrap().into_inner();
        assert!(cpus_response.success);
        assert!(!cpus_response.cpus.is_empty());
    }

    #[tokio::test]
    async fn test_get_net_info() {
        let service = create_test_node_service();

        let request = Request::new(GetNetInfoRequest {});

        let response = service.get_net_info(request).await;
        assert!(response.is_ok());

        let net_response = response.unwrap().into_inner();
        assert!(net_response.success);
        assert!(!net_response.net_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_partitions() {
        let service = create_test_node_service();

        let request = Request::new(GetPartitionsRequest {});

        let response = service.get_partitions(request).await;
        assert!(response.is_ok());

        let partitions_response = response.unwrap().into_inner();
        assert!(partitions_response.success);
        assert!(!partitions_response.partitions.is_empty());
    }

    #[tokio::test]
    async fn test_get_os_info() {
        let service = create_test_node_service();

        let request = Request::new(GetOsInfoRequest {});

        let response = service.get_os_info(request).await;
        assert!(response.is_ok());

        let os_response = response.unwrap().into_inner();
        assert!(os_response.success);
        assert!(!os_response.os_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_se_linux_info() {
        let service = create_test_node_service();

        let request = Request::new(GetSeLinuxInfoRequest {});

        let response = service.get_se_linux_info(request).await;
        assert!(response.is_ok());

        let selinux_response = response.unwrap().into_inner();
        assert!(selinux_response.success);
        assert!(!selinux_response.sys_services.is_empty());
    }

    #[tokio::test]
    async fn test_get_sys_config() {
        let service = create_test_node_service();

        let request = Request::new(GetSysConfigRequest {});

        let response = service.get_sys_config(request).await;
        assert!(response.is_ok());

        let config_response = response.unwrap().into_inner();
        assert!(config_response.success);
        assert!(!config_response.sys_config.is_empty());
    }

    #[tokio::test]
    async fn test_get_sys_errors() {
        let service = create_test_node_service();

        let request = Request::new(GetSysErrorsRequest {});

        let response = service.get_sys_errors(request).await;
        assert!(response.is_ok());

        let errors_response = response.unwrap().into_inner();
        assert!(errors_response.success);
        assert!(!errors_response.sys_errors.is_empty());
    }

    #[tokio::test]
    async fn test_get_mem_info() {
        let service = create_test_node_service();

        let request = Request::new(GetMemInfoRequest {});

        let response = service.get_mem_info(request).await;
        assert!(response.is_ok());

        let mem_response = response.unwrap().into_inner();
        assert!(mem_response.success);
        assert!(!mem_response.mem_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_proc_info() {
        let service = create_test_node_service();

        let request = Request::new(GetProcInfoRequest {});

        let response = service.get_proc_info(request).await;
        assert!(response.is_ok());

        let proc_response = response.unwrap().into_inner();
        assert!(proc_response.success);
        assert!(!proc_response.proc_info.is_empty());
    }

    #[tokio::test]
    async fn test_get_proc_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_proc_info(Request::new(GetProcInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.proc_info));
        let _: rustfs_madmin::health::ProcInfo = serde::Deserialize::deserialize(&mut de).expect("ProcInfo round-trip failed");
    }

    #[tokio::test]
    async fn test_get_mem_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_mem_info(Request::new(GetMemInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.mem_info));
        let _: rustfs_madmin::health::MemInfo = serde::Deserialize::deserialize(&mut de).expect("MemInfo round-trip failed");
    }

    #[tokio::test]
    async fn test_get_sys_errors_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_sys_errors(Request::new(GetSysErrorsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.sys_errors));
        let _: rustfs_madmin::health::SysErrors = serde::Deserialize::deserialize(&mut de).expect("SysErrors round-trip failed");
    }

    #[tokio::test]
    async fn test_get_sys_config_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_sys_config(Request::new(GetSysConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.sys_config));
        let _: rustfs_madmin::health::SysConfig = serde::Deserialize::deserialize(&mut de).expect("SysConfig round-trip failed");
    }

    #[tokio::test]
    async fn test_get_se_linux_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_se_linux_info(Request::new(GetSeLinuxInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.sys_services));
        let _: rustfs_madmin::health::SysServices =
            serde::Deserialize::deserialize(&mut de).expect("SysServices round-trip failed");
    }

    #[tokio::test]
    async fn test_get_os_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_os_info(Request::new(GetOsInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.os_info));
        let _: rustfs_madmin::health::OsInfo = serde::Deserialize::deserialize(&mut de).expect("OsInfo round-trip failed");
    }

    #[tokio::test]
    async fn test_get_partitions_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_partitions(Request::new(GetPartitionsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.partitions));
        let _: rustfs_madmin::health::Partitions =
            serde::Deserialize::deserialize(&mut de).expect("Partitions round-trip failed");
    }

    #[tokio::test]
    async fn test_get_net_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .get_net_info(Request::new(GetNetInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.net_info));
        let _: rustfs_madmin::net::NetInfo = serde::Deserialize::deserialize(&mut de).expect("NetInfo round-trip failed");
    }

    #[tokio::test]
    async fn test_get_cpus_round_trip() {
        let service = create_test_node_service();
        let response = service.get_cpus(Request::new(GetCpusRequest {})).await.unwrap().into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.cpus));
        let _: rustfs_madmin::health::Cpus = serde::Deserialize::deserialize(&mut de).expect("Cpus round-trip failed");
    }

    #[tokio::test]
    async fn test_server_info_round_trip() {
        let service = create_test_node_service();
        let response = service
            .server_info(Request::new(ServerInfoRequest { metrics: false }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.server_properties));
        let _: rustfs_madmin::ServerProperties =
            serde::Deserialize::deserialize(&mut de).expect("ServerProperties round-trip failed");
    }

    #[tokio::test]
    async fn test_get_metrics_round_trip() {
        let service = create_test_node_service();
        let metric_type = MetricType::DISK;
        let opts = CollectMetricsOpts::default();
        let metric_type_bytes = rmp_serde::to_vec(&metric_type).unwrap();
        let opts_bytes = rmp_serde::to_vec(&opts).unwrap();
        let response = service
            .get_metrics(Request::new(GetMetricsRequest {
                metric_type: Bytes::from(metric_type_bytes),
                opts: Bytes::from(opts_bytes),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success);
        let mut de = rmp_serde::Deserializer::new(std::io::Cursor::new(response.realtime_metrics));
        let _: rustfs_madmin::metrics::RealtimeMetrics =
            serde::Deserialize::deserialize(&mut de).expect("RealtimeMetrics round-trip failed");
    }

    #[tokio::test]
    async fn test_reload_pool_meta() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(ReloadPoolMetaRequest {});

        let response = service.reload_pool_meta(request).await;
        assert!(response.is_ok());

        let reload_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!reload_response.success);
        assert!(reload_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_stop_rebalance() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(StopRebalanceRequest {
            expected_rebalance_id: String::new(),
        });

        let response = service.stop_rebalance(request).await;
        assert!(response.is_ok());

        let stop_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!stop_response.success);
        assert!(stop_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_load_rebalance_meta() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(LoadRebalanceMetaRequest { start_rebalance: false });

        let response = service.load_rebalance_meta(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("errServerNotInitialized"));
    }

    #[test]
    fn test_background_rebalance_start_error_message_ignores_success() {
        assert!(background_rebalance_start_error_message(Ok(())).is_none());
    }

    #[test]
    fn test_background_rebalance_start_error_message_formats_error() {
        let message = background_rebalance_start_error_message(Err(Error::other("boom")))
            .expect("background rebalance start failure should be formatted");

        assert!(message.contains("start_rebalance failed"));
        assert!(message.contains("boom"));
    }

    #[test]
    fn test_stop_rebalance_response_reports_local_stop_error() {
        let response = stop_rebalance_response(Err(Error::other("boom")));

        assert!(!response.success);
        assert!(response.error_info.as_deref().is_some_and(|message| message.contains("boom")));
    }

    #[test]
    fn test_stop_rebalance_response_reports_success() {
        let response = stop_rebalance_response(Ok(()));

        assert!(response.success);
        assert!(response.error_info.is_none());
    }

    #[tokio::test]
    async fn test_load_bucket_metadata_empty_bucket() {
        let service = create_test_node_service();
        let maintenance_generation = rustfs_scanner::scanner_maintenance_generation();

        let request = Request::new(LoadBucketMetadataRequest {
            bucket: "".to_string(),
            scanner_maintenance_change: true,
        });

        let response = service.load_bucket_metadata(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("bucket name is missing"));
        assert_eq!(
            rustfs_scanner::scanner_maintenance_generation(),
            maintenance_generation,
            "rejected metadata reloads must not advance scanner maintenance activity"
        );
    }

    #[tokio::test]
    async fn test_load_bucket_metadata_failure_skips_scanner_maintenance() {
        let service = create_test_node_service();
        let maintenance_generation = rustfs_scanner::scanner_maintenance_generation();

        let request = Request::new(LoadBucketMetadataRequest {
            bucket: "reload-miss-scanner-guard-bucket".to_string(),
            scanner_maintenance_change: true,
        });

        let response = service.load_bucket_metadata(request).await.expect("rpc should reply");
        let load_response = response.into_inner();

        // Whether the reload fails on missing server state or on the absent
        // persisted metadata, a failed reload must report failure and must
        // not tell the scanner a maintenance change landed.
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert_eq!(
            rustfs_scanner::scanner_maintenance_generation(),
            maintenance_generation,
            "a failed metadata reload must not advance scanner maintenance activity"
        );
    }

    #[tokio::test]
    async fn test_load_bucket_metadata_no_object_layer() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(LoadBucketMetadataRequest {
            bucket: "test-bucket".to_string(),
            scanner_maintenance_change: false,
        });

        let response = service.load_bucket_metadata(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("errServerNotInitialized"));
    }

    #[tokio::test]
    async fn test_load_transition_tier_config_no_object_layer() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let response = service
            .load_transition_tier_config(Request::new(LoadTransitionTierConfigRequest::default()))
            .await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("errServerNotInitialized"));
    }

    #[tokio::test]
    async fn test_delete_bucket_metadata_empty_bucket() {
        let service = create_test_node_service();

        let request = Request::new(DeleteBucketMetadataRequest { bucket: String::new() });

        let response = service.delete_bucket_metadata(request).await;
        assert!(response.is_ok());

        // An empty bucket name is rejected before touching the metadata system.
        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.unwrap().contains("bucket name is missing"));
    }

    #[tokio::test]
    async fn test_delete_policy_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(DeletePolicyRequest {
            policy_name: "".to_string(),
        });

        let response = service.delete_policy(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("policy name is missing"));
    }

    #[tokio::test]
    async fn test_load_policy_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(LoadPolicyRequest {
            policy_name: "".to_string(),
        });

        let response = service.load_policy(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("policy name is missing"));
    }

    #[tokio::test]
    async fn test_load_policy_mapping_empty_user() {
        let service = create_test_node_service();

        let request = Request::new(LoadPolicyMappingRequest {
            user_or_group: "".to_string(),
            user_type: 0,
            is_group: false,
        });

        let response = service.load_policy_mapping(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("user_or_group name is missing"));
    }

    #[tokio::test]
    async fn test_delete_user_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(DeleteUserRequest {
            access_key: "".to_string(),
        });

        let response = service.delete_user(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_delete_service_account_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(DeleteServiceAccountRequest {
            access_key: "".to_string(),
        });

        let response = service.delete_service_account(request).await;
        assert!(response.is_ok());

        let delete_response = response.unwrap().into_inner();
        assert!(!delete_response.success);
        assert!(delete_response.error_info.is_some());
        assert!(delete_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn delete_service_account_rpc_reloads_instead_of_deleting_shared_state() {
        let _ = rustfs_credentials::init_global_action_credentials(
            Some("TESTROOTACCESSKEY".to_string()),
            Some("TESTROOTSECRET123".to_string()),
        );
        let temp_dir = tempfile::tempdir().expect("service-account RPC test directory");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .init_bucket_metadata(false)
            .build()
            .await;
        ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build isolated IAM");
        let context = Arc::new(crate::runtime_sources::AppContext::with_default_interfaces(
            Arc::clone(&env.ecstore),
            Arc::clone(&iam),
            Arc::new(KmsServiceManager::new()),
        ));
        let service = make_server_for_context(Some(context));
        let access_key = "RPCRELOADSERVICE01";
        iam.new_service_account(
            "parent-user",
            None,
            NewServiceAccountOpts {
                access_key: access_key.to_string(),
                secret_key: "rpcReloadServiceSecret123".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("create service account");

        let response = service
            .delete_service_account(Request::new(DeleteServiceAccountRequest {
                access_key: access_key.to_string(),
            }))
            .await
            .expect("legacy notification RPC response")
            .into_inner();

        assert!(response.success, "cache reload notification must succeed");
        assert!(
            iam.get_service_account(access_key).await.is_ok(),
            "legacy delete notification must not delete durable service-account state"
        );
    }

    #[tokio::test]
    async fn test_load_user_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(LoadUserRequest {
            access_key: "".to_string(),
            temp: false,
        });

        let response = service.load_user(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_load_service_account_empty_access_key() {
        let service = create_test_node_service();

        let request = Request::new(LoadServiceAccountRequest {
            access_key: "".to_string(),
        });

        let response = service.load_service_account(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("access_key name is missing"));
    }

    #[tokio::test]
    async fn test_load_group_empty_name() {
        let service = create_test_node_service();

        let request = Request::new(LoadGroupRequest { group: "".to_string() });

        let response = service.load_group(request).await;
        assert!(response.is_ok());

        let load_response = response.unwrap().into_inner();
        assert!(!load_response.success);
        assert!(load_response.error_info.is_some());
        assert!(load_response.error_info.unwrap().contains("group name is missing"));
    }

    #[tokio::test]
    async fn test_reload_site_replication_config() {
        if !no_object_layer_premise_holds() {
            return;
        }
        let service = create_test_node_service();

        let request = Request::new(ReloadSiteReplicationConfigRequest {});

        let response = service.reload_site_replication_config(request).await;
        assert!(response.is_ok());

        let reload_response = response.unwrap().into_inner();
        // Should fail because object layer is not initialized in test
        assert!(!reload_response.success);
        assert!(reload_response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_signal_service_rejects_missing_signal() {
        let service = create_test_node_service();

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: HashMap::new() }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        assert_eq!(signal_response.error_info.as_deref(), Some("missing service signal"));
    }

    #[tokio::test]
    async fn test_signal_service_rejects_invalid_signal_value() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), "abc".to_string());

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        assert_eq!(signal_response.error_info.as_deref(), Some("invalid service signal value: abc"));
    }

    #[tokio::test]
    async fn test_signal_service_rejects_unsupported_signal() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), "99".to_string());

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        assert_eq!(signal_response.error_info.as_deref(), Some("unsupported service signal: 99"));
    }

    #[tokio::test]
    async fn signal_service_body_digest_gate_runs_before_request_handling() {
        let service = create_test_node_service();
        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), "99".to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), "scanner".to_string());
        vars.insert(PEER_RESTDRY_RUN.to_string(), "false".to_string());
        let message = SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        };

        let mut other = message.clone();
        other
            .vars
            .as_mut()
            .expect("signal vars should exist")
            .value
            .insert(PEER_RESTSIGNAL.to_string(), "1".to_string());
        let mut tampered = Request::new(message.clone());
        let other_body = other.canonical_body().expect("small signal request should encode");
        set_tonic_canonical_body_digest(&mut tampered, &other_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut tampered);
        let error = service
            .signal_service(tampered)
            .await
            .expect_err("a tampered signal request must fail before handler logic");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);

        let mut signed = Request::new(message);
        let body = signed.get_ref().canonical_body().expect("small signal request should encode");
        set_tonic_canonical_body_digest(&mut signed, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut signed);
        let response = service
            .signal_service(signed)
            .await
            .expect("a correctly body-bound signal request must reach handler logic")
            .into_inner();
        assert!(!response.success);
        assert_eq!(response.error_info.as_deref(), Some("unsupported service signal: 99"));
    }

    #[tokio::test]
    async fn signal_service_rejects_explicitly_unsigned_v2_body() {
        let service = create_test_node_service();
        let request = SignalServiceRequest {
            vars: Some(Mss {
                value: HashMap::from([(PEER_RESTSIGNAL.to_string(), "99".to_string())]),
            }),
        };
        let mut request = Request::new(request);
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-auth-version", "2".parse().expect("valid metadata value"));
        request
            .metadata_mut()
            .insert("x-rustfs-content-sha256", "UNSIGNED-PAYLOAD".parse().expect("valid metadata value"));

        let error = service
            .signal_service(request)
            .await
            .expect_err("an explicitly unsigned v2 signal must fail before handler logic");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn signal_service_accepts_historical_unsigned_v2_marker_during_rollout() {
        let service = create_test_node_service();
        let mut request = Request::new(SignalServiceRequest {
            vars: Some(Mss {
                value: HashMap::from([(PEER_RESTSIGNAL.to_string(), "99".to_string())]),
            }),
        });
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-auth-version", "2".parse().expect("valid metadata value"));
        request
            .metadata_mut()
            .insert("x-rustfs-content-sha256", "UNSIGNED-PAYLOAD".parse().expect("valid metadata value"));
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-nonce", "unsigned".parse().expect("valid metadata value"));

        let response = service
            .signal_service(request)
            .await
            .expect("historical unsigned v2 marker must remain compatible during rollout");
        assert!(!response.into_inner().success, "invalid signal fixture should reach handler validation");
    }

    #[tokio::test]
    async fn every_non_disk_mutation_rejects_a_mismatched_body_digest() {
        let service = create_test_node_service();
        let mut covered_methods = HashSet::new();

        macro_rules! assert_tampered {
            ($method:ident, $message:expr) => {{
                assert!(
                    covered_methods.insert(normalized_rpc_method(stringify!($method))),
                    concat!("duplicate non-disk mutation test for ", stringify!($method)),
                );
                let mut request = Request::new($message);
                set_tonic_canonical_body_digest(&mut request, b"unrelated-canonical-body")
                    .expect("digest metadata should encode");
                mark_v2_authenticated(&mut request);
                let error = service
                    .$method(request)
                    .await
                    .expect_err(concat!(stringify!($method), " must reject a mismatched body digest"));
                assert_eq!(
                    error.code(),
                    tonic::Code::PermissionDenied,
                    concat!(stringify!($method), " must authenticate before any mutation"),
                );
            }};
        }

        assert_tampered!(heal_bucket, HealBucketRequest::default());
        assert_tampered!(make_bucket, MakeBucketRequest::default());
        assert_tampered!(delete_bucket, DeleteBucketRequest::default());
        assert_tampered!(lock, GenerallyLockRequest::default());
        assert_tampered!(un_lock, GenerallyLockRequest::default());
        assert_tampered!(force_un_lock, GenerallyLockRequest::default());
        assert_tampered!(refresh, GenerallyLockRequest::default());
        assert_tampered!(lock_batch, BatchGenerallyLockRequest::default());
        assert_tampered!(un_lock_batch, BatchGenerallyLockRequest::default());
        assert_tampered!(load_bucket_metadata, LoadBucketMetadataRequest::default());
        assert_tampered!(delete_bucket_metadata, DeleteBucketMetadataRequest::default());
        assert_tampered!(delete_policy, DeletePolicyRequest::default());
        assert_tampered!(load_policy, LoadPolicyRequest::default());
        assert_tampered!(load_policy_mapping, LoadPolicyMappingRequest::default());
        assert_tampered!(delete_user, DeleteUserRequest::default());
        assert_tampered!(delete_service_account, DeleteServiceAccountRequest::default());
        assert_tampered!(load_user, LoadUserRequest::default());
        assert_tampered!(load_service_account, LoadServiceAccountRequest::default());
        assert_tampered!(load_group, LoadGroupRequest::default());
        assert_tampered!(reload_site_replication_config, ReloadSiteReplicationConfigRequest::default());
        assert_tampered!(signal_service, SignalServiceRequest::default());
        assert_tampered!(
            scanner_activity,
            ScannerActivityRequest {
                challenge: vec![7; 16].into(),
                protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
                acknowledge_instance_id: String::new(),
                acknowledge_dirty_usage_generation: 0,
            }
        );
        assert_tampered!(
            scanner_dirty_usage_snapshot,
            ScannerDirtyUsageSnapshotRequest {
                challenge: vec![7; 16].into(),
                protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
            }
        );
        assert_tampered!(
            acquire_scanner_publication_lease,
            ScannerPublicationLeaseRequest {
                challenge: vec![7; 16].into(),
                expected_movement_generation: 0,
                ttl_ms: SCANNER_PUBLICATION_LEASE_TTL_MS,
                expected_session_id: String::new(),
                token: Bytes::new(),
            }
        );
        assert_tampered!(
            release_scanner_publication_lease,
            ScannerPublicationLeaseReleaseRequest {
                challenge: vec![7; 16].into(),
                token: vec![1; 16].into(),
                owner_id: String::new(),
                session_id: String::new(),
            }
        );
        assert_tampered!(reload_pool_meta, ReloadPoolMetaRequest::default());
        assert_tampered!(stop_rebalance, StopRebalanceRequest::default());
        assert_tampered!(load_rebalance_meta, LoadRebalanceMetaRequest::default());
        assert_tampered!(start_decommission, StartDecommissionRequest::default());
        assert_tampered!(cancel_decommission, CancelDecommissionRequest::default());
        assert_tampered!(clear_decommission, ClearDecommissionRequest::default());
        assert_tampered!(load_transition_tier_config, LoadTransitionTierConfigRequest::default());

        let body_bound_methods: HashSet<_> = node_service_auth_policies()
            .into_iter()
            .filter_map(|(method, policy)| (policy == "body-bound").then_some(method))
            .collect();
        let disk_methods: HashSet<_> = DISK_MUTATION_RPC_METHODS.into_iter().map(String::from).collect();
        assert!(
            disk_methods.is_subset(&body_bound_methods),
            "every independently tested disk mutation must remain declared body-bound",
        );
        let expected_methods: HashSet<_> = body_bound_methods.difference(&disk_methods).cloned().collect();
        assert_eq!(
            covered_methods, expected_methods,
            "proto body-bound non-disk RPCs must exactly match handlers exercised by mismatch tests",
        );
    }

    fn scoped_dirty_usage_request() -> rustfs_protos::proto_gen::node_service::ScannerScopedDirtyUsageAckRequest {
        rustfs_protos::proto_gen::node_service::ScannerScopedDirtyUsageAckRequest {
            challenge: vec![7; 16].into(),
            protocol_version: 1,
            owner_id: "11111111-1111-1111-1111-111111111111".into(),
            instance_id: "a".repeat(32),
            scope: 1,
            probe_only: false,
            entries: vec![rustfs_protos::proto_gen::node_service::ScannerScopedDirtyUsageEntry {
                bucket: "photos".into(),
                bucket_incarnation: vec![1; 16].into(),
                generation: 8,
            }],
        }
    }

    #[tokio::test]
    async fn scoped_dirty_usage_authenticates_before_storage_and_rejects_tampering() {
        use rustfs_protos::scoped_dirty_usage::canonical_scoped_dirty_usage_request;
        let service = create_test_node_service();
        let unsigned = service
            .scanner_scoped_dirty_usage_ack(Request::new(scoped_dirty_usage_request()))
            .await
            .expect_err("unsigned ACK must not access storage");
        assert_eq!(unsigned.code(), tonic::Code::PermissionDenied);
        for field in 0..9 {
            let mut signed = Request::new(scoped_dirty_usage_request());
            let canonical = canonical_scoped_dirty_usage_request(signed.get_ref()).expect("canonical request");
            set_tonic_canonical_body_digest(&mut signed, &canonical).expect("digest");
            mark_v2_authenticated(&mut signed);
            match field {
                0 => signed.get_mut().challenge = vec![3; 16].into(),
                1 => signed.get_mut().owner_id = "22222222-2222-2222-2222-222222222222".into(),
                2 => signed.get_mut().instance_id = "b".repeat(32),
                3 => signed.get_mut().probe_only = true,
                4 => signed.get_mut().entries[0].bucket = "videos".into(),
                5 => signed.get_mut().entries[0].bucket_incarnation = vec![2; 16].into(),
                6 => signed.get_mut().entries[0].generation += 1,
                7 => signed.get_mut().scope += 1,
                _ => signed.get_mut().protocol_version += 1,
            }
            let error = service
                .scanner_scoped_dirty_usage_ack(signed)
                .await
                .expect_err("tampered ACK must fail");
            assert_eq!(
                error.code(),
                if field < 7 {
                    tonic::Code::PermissionDenied
                } else {
                    tonic::Code::InvalidArgument
                }
            );
        }
        let mut signed = Request::new(scoped_dirty_usage_request());
        let canonical = canonical_scoped_dirty_usage_request(signed.get_ref()).expect("canonical request");
        set_tonic_canonical_body_digest(&mut signed, &canonical).expect("digest");
        mark_v2_authenticated(&mut signed);
        assert_eq!(
            service
                .scanner_scoped_dirty_usage_ack(signed)
                .await
                .expect_err("missing owner cannot advertise capability")
                .code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn test_scanner_activity_requires_body_bound_auth_before_storage_lookup() {
        let service = create_test_node_service();

        let legacy = service
            .scanner_activity(Request::new(ScannerActivityRequest {
                challenge: vec![7; 16].into(),
                protocol_version: SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
                acknowledge_instance_id: String::new(),
                acknowledge_dirty_usage_generation: 0,
            }))
            .await
            .expect_err("a rolling-upgrade request should pass authentication before storage lookup");
        assert_eq!(legacy.code(), tonic::Code::Unavailable);

        let unsupported = service
            .scanner_activity(Request::new(ScannerActivityRequest {
                challenge: vec![7; 16].into(),
                protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION + 1,
                acknowledge_instance_id: String::new(),
                acknowledge_dirty_usage_generation: 0,
            }))
            .await
            .expect_err("an unknown request protocol must fail before storage lookup");
        assert_eq!(unsupported.code(), tonic::Code::FailedPrecondition);

        let malformed_legacy = service
            .scanner_activity(Request::new(ScannerActivityRequest {
                challenge: vec![7; 15].into(),
                protocol_version: SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
                acknowledge_instance_id: String::new(),
                acknowledge_dirty_usage_generation: 0,
            }))
            .await
            .expect_err("a malformed legacy challenge must fail before storage lookup");
        assert_eq!(malformed_legacy.code(), tonic::Code::InvalidArgument);

        let unsigned = service
            .scanner_activity(Request::new(ScannerActivityRequest {
                challenge: vec![7; 16].into(),
                protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
                acknowledge_instance_id: String::new(),
                acknowledge_dirty_usage_generation: 0,
            }))
            .await
            .expect_err("unsigned activity queries must fail before storage lookup");
        assert_eq!(unsigned.code(), tonic::Code::PermissionDenied);

        let mut malformed_current = Request::new(ScannerActivityRequest {
            challenge: vec![7; 15].into(),
            protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            acknowledge_instance_id: String::new(),
            acknowledge_dirty_usage_generation: 0,
        });
        let malformed_canonical = rustfs_protos::canonical_scanner_activity_request_body(malformed_current.get_ref())
            .expect("scanner activity request should encode");
        set_tonic_canonical_body_digest(&mut malformed_current, &malformed_canonical).expect("digest metadata should encode");
        mark_v2_authenticated(&mut malformed_current);
        let malformed_current = service
            .scanner_activity(malformed_current)
            .await
            .expect_err("a signed malformed challenge must fail before storage lookup");
        assert_eq!(malformed_current.code(), tonic::Code::InvalidArgument);

        let mut downgraded = Request::new(ScannerActivityRequest {
            challenge: vec![7; 16].into(),
            protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            acknowledge_instance_id: String::new(),
            acknowledge_dirty_usage_generation: 0,
        });
        let current_canonical = rustfs_protos::canonical_scanner_activity_request_body(downgraded.get_ref())
            .expect("scanner activity request should encode");
        set_tonic_canonical_body_digest(&mut downgraded, &current_canonical).expect("digest metadata should encode");
        downgraded.get_mut().protocol_version = SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION;
        mark_v2_authenticated(&mut downgraded);
        let downgraded = service
            .scanner_activity(downgraded)
            .await
            .expect_err("a signed current request must not be downgraded to protocol v4");
        assert_eq!(downgraded.code(), tonic::Code::PermissionDenied);

        let mut incomplete_acknowledgement = Request::new(ScannerActivityRequest {
            challenge: vec![7; 16].into(),
            protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            acknowledge_instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
            acknowledge_dirty_usage_generation: 0,
        });
        let acknowledgement_canonical =
            rustfs_protos::canonical_scanner_activity_request_body(incomplete_acknowledgement.get_ref())
                .expect("scanner activity request should encode");
        set_tonic_canonical_body_digest(&mut incomplete_acknowledgement, &acknowledgement_canonical)
            .expect("digest metadata should encode");
        mark_v2_authenticated(&mut incomplete_acknowledgement);
        let incomplete_acknowledgement = service
            .scanner_activity(incomplete_acknowledgement)
            .await
            .expect_err("dirty usage acknowledgements require an instance ID and generation");
        assert_eq!(incomplete_acknowledgement.code(), tonic::Code::InvalidArgument);

        let mut previous = Request::new(ScannerActivityRequest {
            challenge: vec![7; 16].into(),
            protocol_version: SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION,
            acknowledge_instance_id: String::new(),
            acknowledge_dirty_usage_generation: 0,
        });
        set_tonic_canonical_body_digest(&mut previous, &[7; 16]).expect("protocol v4 digest metadata should encode");
        mark_v2_authenticated(&mut previous);
        let previous = service
            .scanner_activity(previous)
            .await
            .expect_err("an authenticated protocol v4 request should reach storage lookup during rolling upgrades");
        assert_eq!(previous.code(), tonic::Code::Unavailable);

        let mut signed = Request::new(ScannerActivityRequest {
            challenge: vec![7; 16].into(),
            protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            acknowledge_instance_id: String::new(),
            acknowledge_dirty_usage_generation: 0,
        });
        let signed_canonical = rustfs_protos::canonical_scanner_activity_request_body(signed.get_ref())
            .expect("scanner activity request should encode");
        set_tonic_canonical_body_digest(&mut signed, &signed_canonical).expect("digest metadata should encode");
        mark_v2_authenticated(&mut signed);
        let unavailable = service
            .scanner_activity(signed)
            .await
            .expect_err("authenticated activity queries still require initialized storage");
        assert_eq!(unavailable.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn scanner_activity_samples_namespace_generation_after_waiting_for_movement_state() {
        use crate::storage::storage_api::{ObjectOptions, PutObjReader, contract::object::ObjectIO as _};

        let _ = rustfs_credentials::set_global_rpc_secret("scanner-activity-generation-test-secret".to_string());
        let _ = rustfs_credentials::init_global_action_credentials(
            Some("TESTROOTACCESSKEY".to_string()),
            Some("TESTROOTSECRET123".to_string()),
        );
        let temp_dir = tempfile::tempdir().expect("scanner activity RPC test directory");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .build()
            .await;
        ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build isolated IAM");
        let context = Arc::new(crate::runtime_sources::AppContext::with_default_interfaces(
            Arc::clone(&env.ecstore),
            iam,
            Arc::new(KmsServiceManager::new()),
        ));
        let service = make_server_for_context(Some(context));
        let bucket = "scanner-activity-generation";
        env.make_bucket(bucket, false).await;
        let generation_before = env.ecstore.scanner_namespace_mutation_generation();
        let mut request = Request::new(ScannerActivityRequest {
            challenge: vec![7; 16].into(),
            protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            acknowledge_instance_id: String::new(),
            acknowledge_dirty_usage_generation: 0,
        });
        let canonical = rustfs_protos::canonical_scanner_activity_request_body(request.get_ref())
            .expect("scanner activity request should encode");
        set_tonic_canonical_body_digest(&mut request, &canonical).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);

        let pool_meta = env.ecstore.pool_meta.write().await;
        drop(
            env.ecstore
                .decommission_cancelers
                .try_write()
                .expect("movement snapshot should not hold the cancelers before the RPC"),
        );
        let mut activity = Box::pin(tokio::task::unconstrained(service.scanner_activity(request)));
        assert!(futures::poll!(activity.as_mut()).is_pending());
        assert!(
            env.ecstore.decommission_cancelers.try_write().is_err(),
            "the RPC must hold the cancelers read guard while waiting for pool metadata"
        );

        // Select the existing set directly: ECStore pool selection reads the lock held by this test.
        let mut reader = PutObjReader::from_vec(b"namespace changed during activity probe".to_vec());
        tokio::time::timeout(
            Duration::from_secs(30),
            env.ecstore.pools[0].disk_set[0].put_object(
                bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("the namespace mutation must not wait for the RPC's pool lock")
        .expect("the namespace mutation must complete while the RPC waits");
        let generation_after = env.ecstore.scanner_namespace_mutation_generation();
        assert!(generation_after > generation_before);
        drop(pool_meta);

        let response = tokio::time::timeout(Duration::from_secs(30), activity)
            .await
            .expect("scanner activity RPC should resume after the pool lock is released")
            .expect("authenticated scanner activity RPC should succeed")
            .into_inner();
        assert_eq!(response.namespace_generation, generation_after);
        assert_eq!(response.publication_blocked, Some(false));
    }

    #[tokio::test]
    async fn test_scanner_dirty_usage_snapshot_requires_body_bound_auth_and_signs_a_consistent_view() {
        let _ = rustfs_credentials::set_global_rpc_secret("scanner-dirty-usage-snapshot-test-secret".to_string());
        let _ = rustfs_credentials::init_global_action_credentials(
            Some("TESTROOTACCESSKEY".to_string()),
            Some("TESTROOTSECRET123".to_string()),
        );
        let temp_dir = tempfile::tempdir().expect("scanner dirty usage snapshot RPC test directory");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .build()
            .await;
        ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build isolated IAM");
        let context = Arc::new(crate::runtime_sources::AppContext::with_default_interfaces(
            Arc::clone(&env.ecstore),
            iam,
            Arc::new(KmsServiceManager::new()),
        ));
        let service = make_server_for_context(Some(context));
        let unsigned = service
            .scanner_dirty_usage_snapshot(Request::new(ScannerDirtyUsageSnapshotRequest {
                challenge: vec![7; 16].into(),
                protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
            }))
            .await
            .expect_err("unsigned scanner dirty usage snapshot requests must fail");
        assert_eq!(unsigned.code(), tonic::Code::PermissionDenied);

        let mut unsupported = Request::new(ScannerDirtyUsageSnapshotRequest {
            challenge: vec![7; 16].into(),
            protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION + 1,
        });
        let unsupported_body = rustfs_protos::canonical_scanner_dirty_usage_snapshot_request_body(unsupported.get_ref())
            .expect("scanner dirty usage snapshot request should encode");
        set_tonic_canonical_body_digest(&mut unsupported, &unsupported_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut unsupported);
        let unsupported = service
            .scanner_dirty_usage_snapshot(unsupported)
            .await
            .expect_err("unsupported scanner dirty usage snapshot protocols must fail closed");
        assert_eq!(unsupported.code(), tonic::Code::FailedPrecondition);

        let mut malformed = Request::new(ScannerDirtyUsageSnapshotRequest {
            challenge: vec![7; 15].into(),
            protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
        });
        let malformed_body = rustfs_protos::canonical_scanner_dirty_usage_snapshot_request_body(malformed.get_ref())
            .expect("scanner dirty usage snapshot request should encode");
        set_tonic_canonical_body_digest(&mut malformed, &malformed_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut malformed);
        let malformed = service
            .scanner_dirty_usage_snapshot(malformed)
            .await
            .expect_err("malformed scanner dirty usage snapshot challenges must fail closed");
        assert_eq!(malformed.code(), tonic::Code::InvalidArgument);

        let challenge = [7; 16];
        let mut signed = Request::new(ScannerDirtyUsageSnapshotRequest {
            challenge: challenge.to_vec().into(),
            protocol_version: rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION,
        });
        let signed_body = rustfs_protos::canonical_scanner_dirty_usage_snapshot_request_body(signed.get_ref())
            .expect("scanner dirty usage snapshot request should encode");
        set_tonic_canonical_body_digest(&mut signed, &signed_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut signed);
        let response = service
            .scanner_dirty_usage_snapshot(signed)
            .await
            .expect("an authenticated scanner dirty usage snapshot request should succeed")
            .into_inner();
        assert_eq!(response.instance_id, rustfs_scanner::scanner_activity_epoch());
        assert_eq!(response.protocol_version, rustfs_scanner::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION);
        assert!(Uuid::parse_str(&response.owner_id).is_ok_and(|owner_id| !owner_id.is_nil()));
        let bucket_count = u64::try_from(response.buckets.len()).expect("snapshot bucket count should fit in u64");
        assert_eq!(response.complete, response.pending_bucket_count == bucket_count);
        let canonical = rustfs_protos::canonical_scanner_dirty_usage_snapshot_response_body(&challenge, &response)
            .expect("scanner dirty usage snapshot response should encode");
        crate::storage::storage_api::verify_tonic_rpc_response_proof(&canonical, &response.response_proof)
            .expect("scanner dirty usage snapshot response proof should verify");
    }

    #[test]
    fn test_scanner_activity_response_uses_process_epoch_and_generations() {
        let response = scanner_activity_response_v7(
            17,
            [7; 32],
            true,
            rustfs_scanner::ScannerDirtyUsageState {
                generation: 11,
                pending: true,
            },
            23,
            true,
        );

        assert_eq!(response.instance_id, rustfs_scanner::scanner_activity_epoch());
        assert_eq!(response.namespace_generation, 17);
        assert_eq!(response.maintenance_generation, rustfs_scanner::scanner_maintenance_generation());
        assert_eq!(response.protocol_version, rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION);
        assert_eq!(response.topology_digest.as_ref(), &[7; 32]);
        assert!(response.data_movement_active);
        assert_eq!(response.dirty_usage_generation, 11);
        assert!(response.dirty_usage_pending);
        assert_eq!(response.movement_generation, Some(23));
        assert_eq!(response.publication_blocked, Some(true));
    }

    #[test]
    fn test_previous_scanner_activity_response_omits_dirty_usage_fields() {
        let response = previous_scanner_activity_response(17, [7; 32], true);

        assert_eq!(response.protocol_version, SCANNER_ACTIVITY_PREVIOUS_PROTOCOL_VERSION);
        assert_eq!(response.topology_digest.as_ref(), &[7; 32]);
        assert!(response.data_movement_active);
        assert_eq!(response.dirty_usage_generation, 0);
        assert!(!response.dirty_usage_pending);
    }

    #[test]
    fn test_legacy_scanner_activity_response_omits_extended_fields() {
        let response = legacy_scanner_activity_response(17);

        assert_eq!(response.namespace_generation, 17);
        assert_eq!(response.protocol_version, SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION);
        assert!(response.topology_digest.is_empty());
        assert!(!response.data_movement_active);
        assert!(response.response_proof.is_empty());
        assert_eq!(response.dirty_usage_generation, 0);
        assert!(!response.dirty_usage_pending);
    }

    #[tokio::test]
    async fn test_signal_service_rejects_non_dynamic_subsystem() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), SERVICE_SIGNAL_RELOAD_DYNAMIC.to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), "identity_openid".to_string());

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        let error_info = signal_response.error_info.expect("expected error info");
        assert!(error_info.contains("unsupported dynamic config subsystem: identity_openid"));
    }

    #[test]
    fn dynamic_config_rpc_allowlist_matches_supported_subsystems() {
        for sub_system in rustfs_config::notify::NOTIFY_SUB_SYSTEMS {
            assert!(super::supports_dynamic_config_rpc(sub_system));
        }
        for sub_system in [
            STORAGE_CLASS_SUB_SYS,
            rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS,
            rustfs_config::audit::AUDIT_MQTT_SUB_SYS,
            rustfs_config::SCANNER_SUB_SYS,
            rustfs_config::HEAL_SUB_SYS,
        ] {
            assert!(super::supports_dynamic_config_rpc(sub_system));
        }
        assert!(!super::supports_dynamic_config_rpc("identity_openid"));
        // KMS configuration is not a server config subsystem: it converges
        // through its own branch, so it must stay out of this allow-list.
        assert!(!super::supports_dynamic_config_rpc(KMS_SIGNAL_SUBSYSTEM));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_signal_service_kms_dry_run_reports_without_reconfiguring() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), SERVICE_SIGNAL_RELOAD_DYNAMIC.to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), KMS_SIGNAL_SUBSYSTEM.to_string());
        vars.insert(PEER_RESTDRY_RUN.to_string(), true.to_string());

        let response = service
            .signal_service(Request::new(SignalServiceRequest {
                vars: Some(Mss { value: vars }),
            }))
            .await
            .expect("KMS capability probe should return a response")
            .into_inner();

        // A probe must answer even where no configuration was ever applied:
        // that answer is what makes an unconfigured node visible as divergent.
        assert!(response.success, "new nodes must advertise KMS config convergence support");
        assert!(response.error_info.is_none());
        assert_eq!(response.protocol_version, rustfs_protos::DYNAMIC_CONFIG_PROTOCOL_VERSION);
        assert_eq!(
            response.config_fingerprint,
            super::current_kms_config_fingerprint().await,
            "a probe must report the configuration this node is running"
        );
    }

    #[tokio::test]
    async fn test_signal_service_dry_run_accepts_notify_without_runtime_mutation() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), SERVICE_SIGNAL_RELOAD_DYNAMIC.to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS.to_string());
        vars.insert(PEER_RESTDRY_RUN.to_string(), true.to_string());

        let response = service
            .signal_service(Request::new(SignalServiceRequest {
                vars: Some(Mss { value: vars }),
            }))
            .await
            .expect("notify capability probe should return a response")
            .into_inner();

        assert!(response.success, "new nodes must advertise notify lifecycle reload support");
        assert!(response.error_info.is_none());
        assert_eq!(response.protocol_version, rustfs_protos::DYNAMIC_CONFIG_PROTOCOL_VERSION);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_signal_service_refresh_config_requires_object_layer() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), SERVICE_SIGNAL_REFRESH_CONFIG.to_string());

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        let error_info = signal_response.error_info.expect("expected error info");
        assert_eq!(error_info, "runtime config snapshot reload failed");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_signal_service_reload_dynamic_requires_object_layer() {
        let service = create_test_node_service();

        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), SERVICE_SIGNAL_RELOAD_DYNAMIC.to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), STORAGE_CLASS_SUB_SYS.to_string());

        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = service.signal_service(request).await;
        assert!(response.is_ok());

        let signal_response = response.unwrap().into_inner();
        assert!(!signal_response.success);
        let error_info = signal_response.error_info.expect("expected error info");
        assert_eq!(error_info, format!("dynamic config reload failed for {STORAGE_CLASS_SUB_SYS}"));
    }

    fn assert_unimplemented_status<T>(response: Result<Response<T>, Status>, method: &str) {
        let err = match response {
            Ok(_) => panic!("unimplemented RPC should return an error status"),
            Err(err) => err,
        };
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(
            err.message().contains(method),
            "expected method name in status message, got {:?}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_unimplemented_rpcs_return_status() {
        let service = create_test_node_service();

        assert_unimplemented_status(
            service.start_profiling(Request::new(StartProfilingRequest::default())).await,
            "start_profiling",
        );
        assert_unimplemented_status(
            service
                .download_profile_data(Request::new(DownloadProfileDataRequest::default()))
                .await,
            "download_profile_data",
        );
        let bucket_stats_err = service
            .get_bucket_stats(Request::new(GetBucketStatsDataRequest::default()))
            .await
            .expect_err("empty bucket statistics request should fail");
        assert_eq!(bucket_stats_err.code(), tonic::Code::InvalidArgument);
        assert_unimplemented_status(
            service.get_sr_metrics(Request::new(GetSrMetricsDataRequest::default())).await,
            "get_sr_metrics",
        );
        assert_unimplemented_status(
            service
                .get_all_bucket_stats(Request::new(GetAllBucketStatsRequest::default()))
                .await,
            "get_all_bucket_stats",
        );
        let heal_status = service
            .background_heal_status(Request::new(BackgroundHealStatusRequest::default()))
            .await
            .expect("implemented heal status RPC should return a response")
            .into_inner();
        assert!(!heal_status.success);
        assert_eq!(heal_status.error_info.as_deref(), Some("storage layer not initialized"));
        assert_unimplemented_status(
            service
                .get_metacache_listing(Request::new(GetMetacacheListingRequest::default()))
                .await,
            "get_metacache_listing",
        );
        assert_unimplemented_status(
            service
                .update_metacache_listing(Request::new(UpdateMetacacheListingRequest::default()))
                .await,
            "update_metacache_listing",
        );
    }

    async fn connect_test_node_service_client() -> Option<NodeServiceClient<tonic::transport::Channel>> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");
        let service = create_test_node_service();

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        Some(
            NodeServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("node service test client should connect"),
        )
    }

    async fn connect_test_heal_control_client() -> Option<HealControlServiceClient<tonic::transport::Channel>> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local address should be available");

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    HealControlServiceServer::new(make_heal_control_server())
                        .max_decoding_message_size(rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("heal control test server should run");
        });

        Some(
            HealControlServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("heal control test client should connect"),
        )
    }

    #[tokio::test]
    async fn scoped_dirty_usage_transport_rejects_oversized_unknown_and_duplicate_fields() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind scoped ACK transport test");
        let addr = listener.local_addr().expect("test listener address");
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(super::make_scanner_control_server())
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .expect("scoped ACK transport server");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .http2_prior_knowledge()
            .build()
            .expect("HTTP/2 client");
        let limit = rustfs_protos::scoped_dirty_usage::SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES as usize;
        for tag in [0x78, 0x0a] {
            // Unknown varint field 15, or repeated empty singular challenge:
            // both decode to a tiny default struct despite the large wire body.
            for oversized in [false, true] {
                let mut payload = [tag, 0].repeat(if oversized { (limit - 4) / 2 } else { limit / 2 });
                if oversized {
                    // Unknown fixed32 field 15 makes a valid cap+1 protobuf.
                    payload.extend_from_slice(&[0x7d, 0, 0, 0, 0]);
                }
                assert_eq!(payload.len(), limit + usize::from(oversized));
                let mut frame = vec![0];
                frame.extend_from_slice(&u32::try_from(payload.len()).expect("bounded test payload").to_be_bytes());
                frame.extend_from_slice(&payload);
                let response = client
                    .post(format!("http://{addr}/node_service.ScannerControlService/ScannerScopedDirtyUsageAck"))
                    .header("content-type", "application/grpc")
                    .header("te", "trailers")
                    .body(frame)
                    .send()
                    .await
                    .expect("send raw protobuf frame");
                let status = response.headers().get("grpc-status").expect("gRPC failure status");
                assert_eq!(
                    status.to_str().expect("status text"),
                    if oversized { "11" } else { "3" },
                    "cap+1 must fail in the codec, while cap bytes reach request validation"
                );
            }
        }
        drop(client);
        shutdown.send(()).expect("stop test server");
        server.await.expect("join test server");
    }

    #[tokio::test]
    async fn heal_control_transport_enforces_codec_limit_and_fails_closed() {
        let Some(mut client) = connect_test_heal_control_client().await else {
            return;
        };
        let mut request = heal_control_request(b"query");
        let body = rustfs_protos::canonical_heal_control_request_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            "fingerprint",
            b"query",
        )
        .expect("small request should encode");
        set_tonic_canonical_body_digest(&mut request, &body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut request);
        let rejected = client
            .heal_control(request)
            .await
            .expect_err("invalid command must fail closed");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);

        let max_command = vec![0; HEAL_CONTROL_PAYLOAD_MAX_SIZE];
        let mut max_request = heal_control_request(&max_command);
        let max_body = rustfs_protos::canonical_heal_control_request_body(
            rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            "fingerprint",
            &max_command,
        )
        .expect("maximum request should encode");
        set_tonic_canonical_body_digest(&mut max_request, &max_body).expect("digest metadata should encode");
        mark_v2_authenticated(&mut max_request);
        let rejected = client
            .heal_control(max_request)
            .await
            .expect_err("maximum valid transport payload must reach validation");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);

        let oversized = Request::new(HealControlRequest {
            version: rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION,
            topology_fingerprint: "fingerprint".to_string(),
            command: Bytes::from(vec![0; rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE]),
        });
        let rejected = client
            .heal_control(oversized)
            .await
            .expect_err("oversized protobuf message must fail in codec");
        assert_eq!(rejected.code(), tonic::Code::OutOfRange);
    }

    #[tokio::test]
    async fn test_write_stream_unimplemented() {
        let Some(mut client) = connect_test_node_service_client().await else {
            return;
        };
        let request = tokio_stream::iter([WriteRequest::default()]);

        let response = client.write_stream(request).await;

        let err = response.expect_err("write_stream should return unimplemented status");
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("write_stream"));
    }

    #[tokio::test]
    async fn test_read_at_unimplemented() {
        let Some(mut client) = connect_test_node_service_client().await else {
            return;
        };
        let request = tokio_stream::iter([ReadAtRequest::default()]);

        let response = client.read_at(request).await;

        let err = response.expect_err("read_at should return unimplemented status");
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("read_at"));
    }

    #[tokio::test]
    async fn test_node_service_debug() {
        let service = create_test_node_service();
        let debug_str = format!("{service:?}");
        assert!(debug_str.contains("NodeService"));
    }

    #[tokio::test]
    async fn test_node_service_creation() {
        let service1 = make_server();
        let service2 = make_server();

        // Both services should be created successfully
        assert!(format!("{service1:?}").contains("NodeService"));
        assert!(format!("{service2:?}").contains("NodeService"));
    }

    #[tokio::test]
    async fn test_find_disk_method() {
        let service = create_test_node_service();
        let disk = service.find_disk("non-existent-disk").await;
        // Should return None for non-existent disk
        assert!(disk.is_none());
    }

    #[tokio::test]
    async fn test_get_metrics_invalid_metric_type() {
        let service = create_test_node_service();
        let request = Request::new(GetMetricsRequest {
            metric_type: Bytes::from(vec![0x00u8, 0x01u8]), // Invalid rmp data
            opts: Bytes::new(),                             // Valid or invalid
        });
        let response = service.get_metrics(request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_info.is_some());
    }

    #[tokio::test]
    async fn test_get_metrics_invalid_opts() {
        let service = create_test_node_service();
        // Serialize a valid MetricType
        let metric_type = MetricType::DISK;
        let metric_type_bytes = rmp_serde::to_vec(&metric_type).unwrap();

        let request = Request::new(GetMetricsRequest {
            metric_type: Bytes::from(metric_type_bytes),
            opts: Bytes::from(vec![0x00u8, 0x01u8]), // Invalid rmp data
        });
        let response = service.get_metrics(request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(response.error_info.is_some());
    }
}
