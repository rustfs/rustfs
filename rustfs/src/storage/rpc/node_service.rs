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
    DiskStore, ECStore, Error, LocalPeerS3Client, PEER_RESTDRY_RUN, PEER_RESTSIGNAL, PEER_RESTSUB_SYS,
    SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC, StorageDiskRpcExt as _, StorageResult, all_local_disk_path,
    find_local_disk_by_ref, reload_transition_tier_config,
};
use crate::storage::storage_api::runtime_sources_consumer::{EndpointServerPools, runtime_sources};
use crate::storage::storage_api::{sign_tonic_rpc_response_proof, verify_tonic_canonical_body_digest};
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
    models::{PingBody, PingBodyBuilder},
    proto_gen::node_service::{node_service_server::NodeService as Node, *},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Cursor,
    pin::Pin,
    sync::{Arc, OnceLock},
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

fn validate_admin_heal_control_start(request: &rustfs_common::heal_channel::HealChannelRequest) -> Result<(), Status> {
    if request.source != rustfs_common::heal_channel::HealRequestSource::Admin {
        return Err(Status::permission_denied("heal control start source must be admin"));
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
) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION,
        topology_digest: topology_digest.to_vec().into(),
        data_movement_active,
        response_proof: Bytes::new(),
    }
}

fn legacy_scanner_activity_response(namespace_generation: u64) -> ScannerActivityResponse {
    ScannerActivityResponse {
        instance_id: rustfs_scanner::scanner_activity_epoch().to_string(),
        namespace_generation,
        maintenance_generation: rustfs_scanner::scanner_maintenance_generation(),
        protocol_version: rustfs_storage_api::SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION,
        topology_digest: Bytes::new(),
        data_movement_active: false,
        response_proof: Bytes::new(),
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
            success: true,
            error_info: None,
        },
        Err(err) => StopRebalanceResponse {
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
    if cache.get().is_some() {
        return Ok(());
    }
    let fingerprint = tokio::task::spawn_blocking(move || heal::heal_topology_fingerprint(&endpoint_pools))
        .await
        .map_err(|_| "heal control topology calculation task failed".to_string())??;
    let _ = cache.set(fingerprint);
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
        rustfs_protos::heal_control::ExecutableCommand::Query { heal_path, client_token } => {
            let response = timeout(remaining, processor.execute_query_request(heal_path, client_token))
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
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::failed_precondition("tier mutation object store is not initialized"))?;

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
            }),
            Err(err) => tier_mutation_control_response(TierMutationControlResponseInput {
                version,
                phase,
                mutation_id,
                canonical_payload,
                success: false,
                state: TIER_MUTATION_PEER_STATE_UNSPECIFIED_WIRE,
                applied: false,
                error_info: Some(err.to_string()),
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

fn validate_tier_mutation_payload_size(phase: rustfs_protos::TierMutationRpcPhase, payload_len: usize) -> Result<(), Status> {
    let limit = match phase {
        rustfs_protos::TierMutationRpcPhase::Prepare => rustfs_protos::TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE,
        rustfs_protos::TierMutationRpcPhase::Commit => rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE,
        rustfs_protos::TierMutationRpcPhase::Abort => {
            if payload_len != 0 {
                return Err(Status::invalid_argument("tier mutation abort payload must be empty"));
            }
            return Ok(());
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
        self.handle_heal_bucket(request).await
    }

    async fn list_bucket(&self, request: Request<ListBucketRequest>) -> Result<Response<ListBucketResponse>, Status> {
        self.handle_list_bucket(request).await
    }

    async fn make_bucket(&self, request: Request<MakeBucketRequest>) -> Result<Response<MakeBucketResponse>, Status> {
        self.handle_make_bucket(request).await
    }

    async fn get_bucket_info(&self, request: Request<GetBucketInfoRequest>) -> Result<Response<GetBucketInfoResponse>, Status> {
        self.handle_get_bucket_info(request).await
    }

    async fn delete_bucket(&self, request: Request<DeleteBucketRequest>) -> Result<Response<DeleteBucketResponse>, Status> {
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

    async fn rename_part(&self, request: Request<RenamePartRequest>) -> Result<Response<RenamePartResponse>, Status> {
        self.handle_rename_part(request).await
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
        self.handle_lock(request).await
    }

    async fn un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        self.handle_un_lock(request).await
    }

    async fn force_un_lock(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        self.handle_force_un_lock(request).await
    }

    async fn refresh(&self, request: Request<GenerallyLockRequest>) -> Result<Response<GenerallyLockResponse>, Status> {
        self.handle_refresh(request).await
    }

    async fn lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
        self.handle_lock_batch(request).await
    }

    async fn un_lock_batch(
        &self,
        request: Request<BatchGenerallyLockRequest>,
    ) -> Result<Response<BatchGenerallyLockResponse>, Status> {
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
        _request: Request<GetBucketStatsDataRequest>,
    ) -> Result<Response<GetBucketStatsDataResponse>, Status> {
        Err(unimplemented_rpc("get_bucket_stats"))
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
        self.handle_load_bucket_metadata(request).await
    }

    async fn delete_bucket_metadata(
        &self,
        request: Request<DeleteBucketMetadataRequest>,
    ) -> Result<Response<DeleteBucketMetadataResponse>, Status> {
        self.handle_delete_bucket_metadata(request).await
    }

    async fn delete_policy(&self, request: Request<DeletePolicyRequest>) -> Result<Response<DeletePolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.delete_policy(&policy, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeletePolicyResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeletePolicyResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy(&self, request: Request<LoadPolicyRequest>) -> Result<Response<LoadPolicyResponse>, Status> {
        let request = request.into_inner();
        let policy = request.policy_name;
        if policy.is_empty() {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("policy name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_policy(&policy).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_policy_mapping(
        &self,
        request: Request<LoadPolicyMappingRequest>,
    ) -> Result<Response<LoadPolicyMappingResponse>, Status> {
        let request = request.into_inner();
        let user_or_group = request.user_or_group;
        if user_or_group.is_empty() {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("user_or_group name is missing".to_string()),
            }));
        }
        let Some(user_type) = UserType::from_u64(request.user_type) else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("invalid user type".to_string()),
            }));
        };
        let is_group = request.is_group;
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        let resp = iam_sys.load_policy_mapping(&user_or_group, user_type, is_group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadPolicyMappingResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadPolicyMappingResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_user(&self, request: Request<DeleteUserRequest>) -> Result<Response<DeleteUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.delete_user(&access_key, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteUserResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteUserResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn delete_service_account(
        &self,
        request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }
        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        let resp = iam_sys.delete_service_account(&access_key, false).await;
        if let Err(err) = resp {
            return Ok(Response::new(DeleteServiceAccountResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(DeleteServiceAccountResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_user(&self, request: Request<LoadUserRequest>) -> Result<Response<LoadUserResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        let temp = request.temp;
        if access_key.is_empty() {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let user_type = if temp { UserType::Sts } else { UserType::Reg };

        let resp = iam_sys.load_user(&access_key, user_type).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadUserResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadUserResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_service_account(
        &self,
        request: Request<LoadServiceAccountRequest>,
    ) -> Result<Response<LoadServiceAccountResponse>, Status> {
        let request = request.into_inner();
        let access_key = request.access_key;
        if access_key.is_empty() {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("access_key name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_service_account(&access_key).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadServiceAccountResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        Ok(Response::new(LoadServiceAccountResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn load_group(&self, request: Request<LoadGroupRequest>) -> Result<Response<LoadGroupResponse>, Status> {
        let request = request.into_inner();
        let group = request.group;
        if group.is_empty() {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("group name is missing".to_string()),
            }));
        }

        let Some(iam_sys) = runtime_sources::current_iam_handle() else {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let resp = iam_sys.load_group(&group).await;
        if let Err(err) = resp {
            return Ok(Response::new(LoadGroupResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }
        Ok(Response::new(LoadGroupResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn reload_site_replication_config(
        &self,
        _request: Request<ReloadSiteReplicationConfigRequest>,
    ) -> Result<Response<ReloadSiteReplicationConfigResponse>, Status> {
        let Some(_store) = self.resolve_object_store() else {
            return Ok(Response::new(ReloadSiteReplicationConfigResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        match reload_site_replication_runtime_state().await {
            Ok(()) => Ok(Response::new(ReloadSiteReplicationConfigResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ReloadSiteReplicationConfigResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn signal_service(&self, request: Request<SignalServiceRequest>) -> Result<Response<SignalServiceResponse>, Status> {
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
                    return Ok(Response::new(SignalServiceResponse {
                        success: false,
                        error_info: Some(format!("invalid dry-run value: {value}")),
                    }));
                }
            },
        };

        match signal {
            Some(SERVICE_SIGNAL_REFRESH_CONFIG) => match reload_runtime_config_snapshot().await {
                Ok(()) => Ok(Response::new(SignalServiceResponse {
                    success: true,
                    error_info: None,
                })),
                Err(_) => Ok(Response::new(SignalServiceResponse {
                    success: false,
                    error_info: Some("runtime config snapshot reload failed".to_string()),
                })),
            },
            Some(SERVICE_SIGNAL_RELOAD_DYNAMIC) => {
                let supported = sub_system == MODULE_SWITCHES_SIGNAL_SUBSYSTEM || supports_dynamic_config_rpc(sub_system);
                if !supported {
                    return Ok(Response::new(SignalServiceResponse {
                        success: false,
                        error_info: Some(format!("unsupported dynamic config subsystem: {sub_system}")),
                    }));
                }
                if dry_run {
                    return Ok(Response::new(SignalServiceResponse {
                        success: true,
                        error_info: None,
                    }));
                }
                match reload_dynamic_config_runtime_state(sub_system).await {
                    Ok(()) => Ok(Response::new(SignalServiceResponse {
                        success: true,
                        error_info: None,
                    })),
                    Err(_) => Ok(Response::new(SignalServiceResponse {
                        success: false,
                        error_info: Some(format!("dynamic config reload failed for {sub_system}")),
                    })),
                }
            }
            Some(other) => Ok(Response::new(SignalServiceResponse {
                success: false,
                error_info: Some(format!("unsupported service signal: {other}")),
            })),
            None if raw_signal.is_some() => Ok(Response::new(SignalServiceResponse {
                success: false,
                error_info: Some(format!("invalid service signal value: {}", raw_signal.unwrap_or_default())),
            })),
            None => Ok(Response::new(SignalServiceResponse {
                success: false,
                error_info: Some("missing service signal".to_string()),
            })),
        }
    }

    async fn scanner_activity(
        &self,
        request: Request<ScannerActivityRequest>,
    ) -> Result<Response<ScannerActivityResponse>, Status> {
        let store = self
            .resolve_object_store()
            .ok_or_else(|| Status::unavailable("storage layer is not initialized"))?;
        let request = request.into_inner();
        if request.challenge.is_empty() {
            // RUSTFS_COMPAT_TODO(ns-scanner-rpc-v3): old peers send an empty activity request. Remove after every supported peer implements authenticated scanner activity protocol v4.
            return Ok(Response::new(legacy_scanner_activity_response(
                store.scanner_namespace_mutation_generation(),
            )));
        }
        let challenge: [u8; 16] = request
            .challenge
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("scanner activity challenge must be 16 bytes"))?;
        let mut response = scanner_activity_response(
            store.scanner_namespace_mutation_generation(),
            rustfs_scanner::scanner_topology_digest(store.as_ref()),
            store.scanner_data_movement_active().await,
        );
        let canonical = rustfs_protos::canonical_scanner_activity_response_body(&challenge, &response)
            .map_err(|_| Status::internal("scanner activity response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&canonical)
            .map_err(|_| Status::unavailable("scanner activity response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }

    async fn background_heal_status(
        &self,
        _request: Request<BackgroundHealStatusRequest>,
    ) -> Result<Response<BackgroundHealStatusResponse>, Status> {
        if self.resolve_object_store().is_none() {
            return Ok(Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: Bytes::new(),
                error_info: Some("storage layer not initialized".to_string()),
            }));
        }
        let snapshot = heal::capture_node_heal_status(rustfs_scanner::scanner::BackgroundHealInfo::default()).await;
        match heal::encode_node_heal_status(&snapshot) {
            Ok(bg_heal_state) => Ok(Response::new(BackgroundHealStatusResponse {
                success: true,
                bg_heal_state: bg_heal_state.into(),
                error_info: None,
            })),
            Err(err) => Ok(Response::new(BackgroundHealStatusResponse {
                success: false,
                bg_heal_state: Bytes::new(),
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
        _request: Request<ReloadPoolMetaRequest>,
    ) -> Result<Response<ReloadPoolMetaResponse>, Status> {
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };
        match store.reload_pool_meta().await {
            Ok(_) => match store.spawn_missing_local_decommission_routines().await {
                Ok(_) => Ok(Response::new(ReloadPoolMetaResponse {
                    success: true,
                    error_info: None,
                })),
                Err(err) => Ok(Response::new(ReloadPoolMetaResponse {
                    success: false,
                    error_info: Some(err.to_string()),
                })),
            },
            Err(err) => Ok(Response::new(ReloadPoolMetaResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn stop_rebalance(&self, request: Request<StopRebalanceRequest>) -> Result<Response<StopRebalanceResponse>, Status> {
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(StopRebalanceResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
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
        let LoadRebalanceMetaRequest { start_rebalance } = request.into_inner();
        let Some(store) = self.resolve_object_store() else {
            log_load_rebalance_meta_rejected!("server_not_initialized", start_rebalance);
            return Ok(Response::new(LoadRebalanceMetaResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
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
                    success: false,
                    error_info: Some(message),
                }));
            }
        }

        Ok(Response::new(LoadRebalanceMetaResponse {
            success: true,
            error_info: None,
        }))
    }

    async fn start_decommission(
        &self,
        request: Request<StartDecommissionRequest>,
    ) -> Result<Response<StartDecommissionResponse>, Status> {
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(StartDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
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
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(StartDecommissionResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn cancel_decommission(
        &self,
        request: Request<CancelDecommissionRequest>,
    ) -> Result<Response<CancelDecommissionResponse>, Status> {
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(CancelDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let idx = usize::try_from(request.into_inner().pool_index)
            .map_err(|_| Status::invalid_argument("decommission pool index exceeds local range"))?;
        if let Err(err) = ensure_rpc_decommission_local_leader(&store, idx) {
            return Ok(Response::new(CancelDecommissionResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        match store.decommission_cancel(idx).await {
            Ok(()) => Ok(Response::new(CancelDecommissionResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(CancelDecommissionResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn clear_decommission(
        &self,
        request: Request<ClearDecommissionRequest>,
    ) -> Result<Response<ClearDecommissionResponse>, Status> {
        let Some(store) = runtime_sources::current_object_store_handle() else {
            return Ok(Response::new(ClearDecommissionResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        let idx = usize::try_from(request.into_inner().pool_index)
            .map_err(|_| Status::invalid_argument("decommission pool index exceeds local range"))?;
        if let Err(err) = ensure_rpc_decommission_local_leader(&store, idx) {
            return Ok(Response::new(ClearDecommissionResponse {
                success: false,
                error_info: Some(err.to_string()),
            }));
        }

        match store.clear_decommission(idx).await {
            Ok(()) => Ok(Response::new(ClearDecommissionResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(ClearDecommissionResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    async fn load_transition_tier_config(
        &self,
        _request: Request<LoadTransitionTierConfigRequest>,
    ) -> Result<Response<LoadTransitionTierConfigResponse>, Status> {
        let Some(store) = self.resolve_object_store() else {
            return Ok(Response::new(LoadTransitionTierConfigResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        match reload_transition_tier_config(store).await {
            Ok(_) => Ok(Response::new(LoadTransitionTierConfigResponse {
                success: true,
                error_info: None,
            })),
            Err(err) => Ok(Response::new(LoadTransitionTierConfigResponse {
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
        CollectMetricsOpts, DiskStore, Error, HEAL_CONTROL_PAYLOAD_MAX_SIZE, MetricType, Node as _, NodeService,
        PEER_RESTDRY_RUN, PEER_RESTSIGNAL, PEER_RESTSUB_SYS, SERVICE_SIGNAL_REFRESH_CONFIG, SERVICE_SIGNAL_RELOAD_DYNAMIC,
        STORAGE_CLASS_SUB_SYS, admit_heal_control_replay, background_rebalance_start_error_message,
        execute_heal_control_envelope_with_manager, initialize_heal_topology_fingerprint, legacy_scanner_activity_response,
        make_heal_control_server, make_heal_control_server_with_cache, make_server,
        make_tier_mutation_control_server_for_context, remove_heal_control_replay, scanner_activity_response,
        stop_rebalance_response,
    };
    use crate::storage::rpc::node_service::heal::heal_topology_fingerprint;
    use crate::storage::storage_api::rpc_consumer::node_service::{HealBucketInfo, HealEndpoint};
    use crate::storage::storage_api::set_tonic_canonical_body_digest;
    use crate::storage::storage_api::{
        Endpoint,
        ecstore_layout::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
    use bytes::Bytes;
    use rustfs_heal::heal::{manager::HealManager, storage::HealStorageAPI};
    use rustfs_protos::models::PingBodyBuilder;
    use rustfs_protos::proto_gen::node_service::{
        BackgroundHealStatusRequest, CheckPartsRequest, DeleteBucketMetadataRequest, DeleteBucketRequest, DeletePathsRequest,
        DeletePolicyRequest, DeleteRequest, DeleteServiceAccountRequest, DeleteUserRequest, DeleteVersionRequest,
        DeleteVersionsRequest, DeleteVolumeRequest, DiskInfoRequest, DownloadProfileDataRequest, GenerallyLockRequest,
        GetAllBucketStatsRequest, GetBucketInfoRequest, GetBucketStatsDataRequest, GetCpusRequest, GetMemInfoRequest,
        GetMetacacheListingRequest, GetMetricsRequest, GetNetInfoRequest, GetOsInfoRequest, GetPartitionsRequest,
        GetProcInfoRequest, GetSeLinuxInfoRequest, GetSrMetricsDataRequest, GetSysConfigRequest, GetSysErrorsRequest,
        HealBucketRequest, HealControlRequest, ListBucketRequest, ListDirRequest, ListVolumesRequest, LoadBucketMetadataRequest,
        LoadGroupRequest, LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest, LoadServiceAccountRequest,
        LoadTransitionTierConfigRequest, LoadUserRequest, LocalStorageInfoRequest, MakeBucketRequest, MakeVolumeRequest,
        MakeVolumesRequest, Mss, PingRequest, ReadAllRequest, ReadAtRequest, ReadMultipleRequest, ReadVersionRequest,
        ReadXlRequest, ReloadPoolMetaRequest, ReloadSiteReplicationConfigRequest, RenameDataRequest, RenameFileRequest,
        RenamePartRequest, ScannerActivityRequest, ServerInfoRequest, SignalServiceRequest, StartProfilingRequest,
        StatVolumeRequest, StopRebalanceRequest, TierMutationPeerState, TierMutationPrepareRequest,
        UpdateMetacacheListingRequest, UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest, WriteMetadataRequest,
        WriteRequest,
        heal_control_service_client::HealControlServiceClient,
        heal_control_service_server::{HealControlService as _, HealControlServiceServer},
        node_service_client::NodeServiceClient,
        node_service_server::NodeServiceServer,
        tier_mutation_control_service_server::TierMutationControlService as _,
    };
    use std::{collections::HashMap, sync::Arc};
    use time::OffsetDateTime;
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

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

        async fn get_object_data(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn put_object_data(&self, _bucket: &str, _object: &str, _data: &[u8]) -> rustfs_heal::Result<()> {
            Ok(())
        }

        async fn delete_object(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<()> {
            Ok(())
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<bool> {
            Ok(true)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_disk_status(&self, _endpoint: &HealEndpoint) -> rustfs_heal::Result<rustfs_heal::heal::storage::DiskStatus> {
            Ok(rustfs_heal::heal::storage::DiskStatus::Ok)
        }

        async fn format_disk(&self, _endpoint: &HealEndpoint) -> rustfs_heal::Result<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> rustfs_heal::Result<Option<HealBucketInfo>> {
            Ok(None)
        }

        async fn heal_bucket_metadata(&self, _bucket: &str) -> rustfs_heal::Result<()> {
            Ok(())
        }

        async fn list_buckets(&self) -> rustfs_heal::Result<Vec<HealBucketInfo>> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<bool> {
            Ok(false)
        }

        async fn get_object_size(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Option<u64>> {
            Ok(None)
        }

        async fn get_object_checksum(&self, _bucket: &str, _object: &str) -> rustfs_heal::Result<Option<String>> {
            Ok(None)
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> rustfs_heal::Result<rustfs_madmin::heal_commands::HealResultItem> {
            Ok(rustfs_madmin::heal_commands::HealResultItem::default())
        }

        async fn heal_format(
            &self,
            _dry_run: bool,
        ) -> rustfs_heal::Result<(rustfs_madmin::heal_commands::HealResultItem, Option<rustfs_heal::Error>)> {
            Ok((rustfs_madmin::heal_commands::HealResultItem::default(), None))
        }

        async fn list_objects_for_heal(
            &self,
            _bucket: &str,
            _prefix: &str,
        ) -> rustfs_heal::Result<Vec<rustfs_heal::heal::storage::HealListItem>> {
            Ok(Vec::new())
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
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

    #[tokio::test]
    async fn heal_control_executor_preserves_canonical_token_and_drops_query_results() {
        let manager = Arc::new(HealManager::new(Arc::new(HealControlMockStorage), None));
        let coordinator_epoch = 7;
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let now = i64::try_from(now).expect("test clock should fit in i64");
        let metadata = || rustfs_protos::heal_control::RequestMetadata::new(rand::random(), now, now + 30_000, coordinator_epoch);
        let start = |request_id: String| {
            let mut request =
                rustfs_common::heal_channel::create_heal_request("bucket".to_string(), Some("prefix".to_string()), false, None);
            request.id = request_id;
            request.source = rustfs_common::heal_channel::HealRequestSource::Admin;
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
            .expect_err("authenticated request still requires initialized object store");
        assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
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
    async fn tier_mutation_control_rejects_old_protocol_version_before_store_lookup() {
        let service = make_tier_mutation_control_server_for_context(None);
        let mutation_id = uuid::Uuid::new_v4();
        let payload = Bytes::from_static(b"intent");
        let mut request = Request::new(TierMutationPrepareRequest {
            version: rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION - 1,
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

        let expired_request = rustfs_common::heal_channel::create_heal_request("bucket".to_string(), None, false, None);
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

        let mut non_admin_request = rustfs_common::heal_channel::create_heal_request("bucket".to_string(), None, false, None);
        non_admin_request.source = rustfs_common::heal_channel::HealRequestSource::Scanner;
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
        initialize_heal_topology_fingerprint(Arc::clone(&cache), topology)
            .await
            .expect("valid topology should initialize");
        assert_eq!(cache.get(), Some(&expected));

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
    #[ignore = "requires isolated global object layer state"]
    async fn test_local_storage_info() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_reload_pool_meta() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_stop_rebalance() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_load_rebalance_meta() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_load_bucket_metadata_no_object_layer() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_load_transition_tier_config_no_object_layer() {
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
    #[ignore = "requires isolated global object layer state"]
    async fn test_reload_site_replication_config() {
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
    async fn test_scanner_activity_requires_storage_layer() {
        let service = create_test_node_service();

        let err = service
            .scanner_activity(Request::new(ScannerActivityRequest {
                challenge: vec![7; 16].into(),
            }))
            .await
            .expect_err("activity queries must fail closed before storage is initialized");

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_scanner_activity_response_uses_process_epoch_and_generations() {
        let response = scanner_activity_response(17, [7; 32], true);

        assert_eq!(response.instance_id, rustfs_scanner::scanner_activity_epoch());
        assert_eq!(response.namespace_generation, 17);
        assert_eq!(response.maintenance_generation, rustfs_scanner::scanner_maintenance_generation());
        assert_eq!(response.protocol_version, rustfs_scanner::SCANNER_ACTIVITY_PROTOCOL_VERSION);
        assert_eq!(response.topology_digest.as_ref(), &[7; 32]);
        assert!(response.data_movement_active);
    }

    #[test]
    fn test_legacy_scanner_activity_response_omits_extended_fields() {
        let response = legacy_scanner_activity_response(17);

        assert_eq!(response.namespace_generation, 17);
        assert_eq!(response.protocol_version, rustfs_storage_api::SCANNER_ACTIVITY_LEGACY_PROTOCOL_VERSION);
        assert!(response.topology_digest.is_empty());
        assert!(!response.data_movement_active);
        assert!(response.response_proof.is_empty());
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
    }

    #[tokio::test]
    #[ignore = "requires isolated global object layer state"]
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
    #[ignore = "requires isolated global object layer state"]
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
        assert_unimplemented_status(
            service
                .get_bucket_stats(Request::new(GetBucketStatsDataRequest::default()))
                .await,
            "get_bucket_stats",
        );
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
