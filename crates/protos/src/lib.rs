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

// SAFETY: `generated` is prost/tonic-generated protocol code. The allowance is
// scoped to that module so generated internals do not relax lints elsewhere.
#[allow(unsafe_code)]
mod generated;
pub mod heal_control;
mod runtime_sources;

use proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_common::{cache_connection, cached_connection, evict_connection_with_log_level};
use std::{
    collections::HashMap,
    error::Error,
    sync::LazyLock,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::{
    Request, Status,
    service::interceptor::InterceptedService,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Type alias for the complex client type
pub type NodeServiceClientType = NodeServiceClient<
    InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
>;

pub use generated::*;
pub use rustfs_common::ConnectionEvictionLogLevel;

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

/// Default HTTPS prefix for rustfs
/// This is the default HTTPS prefix for rustfs.
/// It is used to identify HTTPS URLs.
/// Default value: https://
const RUSTFS_HTTPS_PREFIX: &str = "https://";
const TLS_GENERATION_CACHE_MAX_SIZE: usize = 512;
const INTERNODE_RPC_MSGPACK_ONLY_CACHE_UNSET: u8 = 0;
const INTERNODE_RPC_MSGPACK_ONLY_CACHE_FALSE: u8 = 1;
const INTERNODE_RPC_MSGPACK_ONLY_CACHE_TRUE: u8 = 2;
static TLS_GENERATION_CACHE: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static INTERNODE_RPC_MSGPACK_ONLY_CACHE: AtomicU8 = AtomicU8::new(INTERNODE_RPC_MSGPACK_ONLY_CACHE_UNSET);

fn enforce_tls_generation_cache_bound(generation_cache: &mut HashMap<String, u64>, generation: u64, addr: &str) {
    if generation_cache.len() < TLS_GENERATION_CACHE_MAX_SIZE || generation_cache.contains_key(addr) {
        return;
    }

    generation_cache.retain(|_, g| *g == generation);
    if generation_cache.len() >= TLS_GENERATION_CACHE_MAX_SIZE
        && let Some(victim) = generation_cache.keys().next().cloned()
    {
        generation_cache.remove(&victim);
    }
}

fn internode_connect_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_CONNECT_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS,
    ))
}

fn internode_tcp_keepalive() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_TCP_KEEPALIVE_SECS,
        rustfs_config::DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS,
    ))
}

fn internode_http2_keep_alive_interval() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
        rustfs_config::DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
    ))
}

fn internode_http2_keep_alive_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
    ))
}

fn internode_rpc_timeout() -> Duration {
    normalize_internode_rpc_timeout(Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_INTERNODE_RPC_TIMEOUT_SECS,
        rustfs_config::DEFAULT_INTERNODE_RPC_TIMEOUT_SECS,
    )))
}

fn normalize_internode_rpc_timeout(timeout: Duration) -> Duration {
    timeout.max(Duration::from_secs(1))
}

/// Budget for one heal-control execution, kept below the transport timeout so
/// the coordinator stops waiting for admission before the caller gives up.
pub fn heal_control_execution_timeout() -> Duration {
    heal_control_execution_timeout_for(internode_rpc_timeout())
}

fn heal_control_execution_timeout_for(transport_timeout: Duration) -> Duration {
    const TRANSPORT_GUARD: Duration = Duration::from_secs(1);
    let transport_timeout = transport_timeout.max(Duration::from_secs(1));
    transport_timeout
        .saturating_sub(TRANSPORT_GUARD.min(transport_timeout / 2))
        .max(Duration::from_millis(1))
        .min(Duration::from_millis(
            u64::try_from(heal_control::MAX_LIFETIME_MS).expect("positive heal control lifetime must fit u64"),
        ))
}

fn internode_rpc_tcp_nodelay() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_TCP_NODELAY,
        rustfs_config::DEFAULT_INTERNODE_RPC_TCP_NODELAY,
    )
}

/// HTTP/2 initial stream window size for the client channel, or `None` to use the
/// library default (a configured value of `0` opts out).
fn internode_rpc_http2_stream_window() -> Option<u32> {
    match rustfs_utils::get_env_u32(
        rustfs_config::ENV_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE,
        rustfs_config::DEFAULT_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE,
    ) {
        0 => None,
        v => Some(v),
    }
}

/// HTTP/2 initial connection window size for the client channel, or `None` to use the
/// library default (a configured value of `0` opts out).
fn internode_rpc_http2_conn_window() -> Option<u32> {
    match rustfs_utils::get_env_u32(
        rustfs_config::ENV_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE,
        rustfs_config::DEFAULT_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE,
    ) {
        0 => None,
        v => Some(v),
    }
}

/// Maximum encoded/decoded internode gRPC message size (bytes), shared by the client
/// `NodeServiceClient` and server `NodeServiceServer`. Defaults to
/// [`DEFAULT_GRPC_SERVER_MESSAGE_LEN`] (100 MiB) when the env var is unset.
pub fn internode_rpc_max_message_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_INTERNODE_RPC_MAX_MESSAGE_SIZE, DEFAULT_GRPC_SERVER_MESSAGE_LEN)
}

pub const HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE: usize = heal_control::RESULT_MAX_SIZE + 1024;
pub const HEAL_CONTROL_PROTOCOL_VERSION: u32 = 3;
pub const DYNAMIC_CONFIG_PROTOCOL_VERSION: u32 = 1;
pub const BACKGROUND_HEAL_STATUS_PROTOCOL_VERSION: u32 = 2;
pub const HEAL_CONTROL_CAPABILITY_PROBE_PREFIX: &[u8] = b"rustfs-heal-control-capability-v3\0";
pub const REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX: &[u8] = b"rustfs-tier-remote-version-state-capability-v1\0";
pub const CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX: &[u8] = b"rustfs-cross-pool-fence-capability-v1\0";
pub const TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE: usize = 64 * 1024;
pub const TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE: usize = 1024;
pub const TIER_MUTATION_RPC_MAX_MESSAGE_SIZE: usize = TIER_MUTATION_RPC_MAX_PREPARE_PAYLOAD_SIZE + 4096;

pub fn heal_control_coordinator_epoch(topology_fingerprint: &str) -> Result<u64, &'static str> {
    let prefix = topology_fingerprint
        .get(..16)
        .ok_or("heal control topology fingerprint is too short")?;
    let epoch = u64::from_str_radix(prefix, 16).map_err(|_| "heal control topology fingerprint is not hexadecimal")?;
    if epoch == 0 {
        return Err("heal control topology epoch is zero");
    }
    Ok(epoch)
}

pub fn heal_control_capability_probe(nonce: &[u8; 16]) -> Vec<u8> {
    let mut probe = Vec::with_capacity(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX.len() + nonce.len());
    probe.extend_from_slice(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX);
    probe.extend_from_slice(nonce);
    probe
}

pub fn is_heal_control_capability_probe(command: &[u8]) -> bool {
    command.len() == HEAL_CONTROL_CAPABILITY_PROBE_PREFIX.len() + 16 && command.starts_with(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX)
}

pub fn remote_version_state_capability_probe(nonce: &[u8; 16]) -> Vec<u8> {
    let mut probe = Vec::with_capacity(REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX.len() + nonce.len());
    probe.extend_from_slice(REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX);
    probe.extend_from_slice(nonce);
    probe
}

pub fn is_remote_version_state_capability_probe(command: &[u8]) -> bool {
    command.len() == REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX.len() + 16
        && command.starts_with(REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX)
}

pub fn is_cross_pool_fence_capability_probe(command: &[u8]) -> bool {
    command.len() == CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX.len() + 16
        && command.starts_with(CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX)
}

pub fn encode_remote_version_state_capability(
    topology_member: &str,
    process_epoch: &[u8; 16],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let topology_member = topology_member.as_bytes();
    let mut result = Vec::with_capacity(8 + topology_member.len() + process_epoch.len());
    result.extend_from_slice(&u64::try_from(topology_member.len())?.to_be_bytes());
    result.extend_from_slice(topology_member);
    result.extend_from_slice(process_epoch);
    Ok(result)
}

pub fn decode_remote_version_state_capability(result: &[u8]) -> Result<(&str, &[u8; 16]), &'static str> {
    let member_len = result
        .get(..8)
        .and_then(|value| value.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or("remote version state capability is truncated")?;
    let member_len = usize::try_from(member_len).map_err(|_| "remote version state member length cannot be represented")?;
    let member_end = 8_usize
        .checked_add(member_len)
        .ok_or("remote version state member length overflow")?;
    let topology_member = std::str::from_utf8(result.get(8..member_end).ok_or("remote version state member is truncated")?)
        .map_err(|_| "remote version state member is not UTF-8")?;
    let process_epoch = result
        .get(member_end..)
        .and_then(|value| value.try_into().ok())
        .ok_or("remote version state process epoch has an invalid length")?;
    Ok((topology_member, process_epoch))
}

pub fn encode_cross_pool_fence_capability(
    supported_version: u32,
    topology_member: &str,
    process_epoch: &[u8; 16],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let identity = encode_remote_version_state_capability(topology_member, process_epoch)?;
    let mut result = Vec::with_capacity(4 + identity.len());
    result.extend_from_slice(&supported_version.to_be_bytes());
    result.extend_from_slice(&identity);
    Ok(result)
}

/// Builds the stable byte representation authenticated for a heal-control request.
///
/// This deliberately does not reuse protobuf encoding: mixed-version peers may
/// retain unknown protobuf fields, which must not change the signed contract.
pub fn canonical_heal_control_request_body(
    version: u32,
    topology_fingerprint: &str,
    command: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-heal-control-v3\0";

    let fingerprint = topology_fingerprint.as_bytes();
    let mut body = Vec::with_capacity(DOMAIN.len() + 4 + 8 + fingerprint.len() + 8 + command.len());
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(fingerprint.len())?.to_be_bytes());
    body.extend_from_slice(fingerprint);
    body.extend_from_slice(&u64::try_from(command.len())?.to_be_bytes());
    body.extend_from_slice(command);
    Ok(body)
}

/// Builds the exact acknowledgement expected from a peer that supports the
/// heal-control protocol and agrees with the caller's storage topology.
pub fn canonical_heal_control_capability_ack(
    version: u32,
    topology_fingerprint: &str,
    probe: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-heal-control-capability-ack-v3\0";

    let fingerprint = topology_fingerprint.as_bytes();
    let mut body = Vec::with_capacity(DOMAIN.len() + 4 + 8 + fingerprint.len() + 8 + probe.len());
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(fingerprint.len())?.to_be_bytes());
    body.extend_from_slice(fingerprint);
    body.extend_from_slice(&u64::try_from(probe.len())?.to_be_bytes());
    body.extend_from_slice(probe);
    Ok(body)
}

pub fn canonical_heal_control_response_body(
    version: u32,
    topology_fingerprint: &str,
    command: &[u8],
    result: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-heal-control-response-v3\0";

    let fingerprint = topology_fingerprint.as_bytes();
    let mut body = Vec::with_capacity(DOMAIN.len() + 4 + 8 + fingerprint.len() + 8 + command.len() + 8 + result.len());
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(fingerprint.len())?.to_be_bytes());
    body.extend_from_slice(fingerprint);
    body.extend_from_slice(&u64::try_from(command.len())?.to_be_bytes());
    body.extend_from_slice(command);
    body.extend_from_slice(&u64::try_from(result.len())?.to_be_bytes());
    body.extend_from_slice(result);
    Ok(body)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TierMutationRpcPhase {
    Prepare,
    Commit,
    Abort,
}

impl TierMutationRpcPhase {
    fn as_wire_str(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Commit => "commit",
            Self::Abort => "abort",
        }
    }
}

pub const TIER_MUTATION_RPC_PROTOCOL_VERSION: u32 = 1;

pub fn canonical_tier_mutation_rpc_body(
    version: u32,
    phase: TierMutationRpcPhase,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-tier-mutation-rpc-v1\0";

    let phase = phase.as_wire_str().as_bytes();
    let mutation_id = mutation_id.as_bytes();
    let mut body = Vec::with_capacity(DOMAIN.len() + 4 + 8 + phase.len() + mutation_id.len() + 8 + canonical_payload.len());
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(phase.len())?.to_be_bytes());
    body.extend_from_slice(phase);
    body.extend_from_slice(mutation_id);
    body.extend_from_slice(&u64::try_from(canonical_payload.len())?.to_be_bytes());
    body.extend_from_slice(canonical_payload);
    Ok(body)
}

pub struct TierMutationRpcResponseProofInput<'a> {
    pub version: u32,
    pub phase: TierMutationRpcPhase,
    pub mutation_id: Uuid,
    pub canonical_payload: &'a [u8],
    pub success: bool,
    pub state: i32,
    pub applied: bool,
    pub error_info: Option<&'a str>,
}

pub fn canonical_tier_mutation_rpc_response_body(
    input: TierMutationRpcResponseProofInput<'_>,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-tier-mutation-rpc-response-v1\0";

    let phase = input.phase.as_wire_str().as_bytes();
    let mutation_id = input.mutation_id.as_bytes();
    let error_info = input.error_info.map(str::as_bytes);
    let error_info_len = error_info.map_or(0, <[u8]>::len);
    let mut body = Vec::with_capacity(
        DOMAIN.len()
            + 4
            + 8
            + phase.len()
            + mutation_id.len()
            + 8
            + input.canonical_payload.len()
            + 1
            + 4
            + 1
            + 1
            + 8
            + error_info_len,
    );
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&input.version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(phase.len())?.to_be_bytes());
    body.extend_from_slice(phase);
    body.extend_from_slice(mutation_id);
    body.extend_from_slice(&u64::try_from(input.canonical_payload.len())?.to_be_bytes());
    body.extend_from_slice(input.canonical_payload);
    body.push(u8::from(input.success));
    body.extend_from_slice(&input.state.to_be_bytes());
    body.push(u8::from(input.applied));
    body.push(u8::from(error_info.is_some()));
    body.extend_from_slice(&u64::try_from(error_info_len)?.to_be_bytes());
    if let Some(error_info) = error_info {
        body.extend_from_slice(error_info);
    }
    Ok(body)
}

/// Builds the stable byte representation authenticated for a scanner activity request.
pub fn canonical_scanner_activity_request_body(
    request: &proto_gen::node_service::ScannerActivityRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-scanner-activity-request-v1\0";

    let challenge = request.challenge.as_ref();
    let acknowledge_instance_id = request.acknowledge_instance_id.as_bytes();
    let mut body = Vec::with_capacity(DOMAIN.len() + challenge.len() + acknowledge_instance_id.len() + 4 + 8 * 3);
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&request.protocol_version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(challenge.len())?.to_be_bytes());
    body.extend_from_slice(challenge);
    body.extend_from_slice(&u64::try_from(acknowledge_instance_id.len())?.to_be_bytes());
    body.extend_from_slice(acknowledge_instance_id);
    body.extend_from_slice(&request.acknowledge_dirty_usage_generation.to_be_bytes());
    Ok(body)
}

/// Builds the protocol-v4 byte representation authenticated for a scanner activity response.
pub fn canonical_scanner_activity_v4_response_body(
    challenge: &[u8],
    response: &proto_gen::node_service::ScannerActivityResponse,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-scanner-activity-response-v1\0";

    let instance_id = response.instance_id.as_bytes();
    let topology_digest = response.topology_digest.as_ref();
    let mut body = Vec::with_capacity(DOMAIN.len() + challenge.len() + instance_id.len() + topology_digest.len() + 4 + 8 * 5 + 1);
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&u64::try_from(challenge.len())?.to_be_bytes());
    body.extend_from_slice(challenge);
    body.extend_from_slice(&u64::try_from(instance_id.len())?.to_be_bytes());
    body.extend_from_slice(instance_id);
    body.extend_from_slice(&response.namespace_generation.to_be_bytes());
    body.extend_from_slice(&response.maintenance_generation.to_be_bytes());
    body.extend_from_slice(&response.protocol_version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(topology_digest.len())?.to_be_bytes());
    body.extend_from_slice(topology_digest);
    body.push(u8::from(response.data_movement_active));
    Ok(body)
}

/// Builds the stable byte representation authenticated for a scanner activity response.
pub fn canonical_scanner_activity_response_body(
    challenge: &[u8],
    response: &proto_gen::node_service::ScannerActivityResponse,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-scanner-activity-response-v2\0";

    let instance_id = response.instance_id.as_bytes();
    let topology_digest = response.topology_digest.as_ref();
    let mut body = Vec::with_capacity(DOMAIN.len() + challenge.len() + instance_id.len() + topology_digest.len() + 4 + 8 * 6 + 2);
    body.extend_from_slice(DOMAIN);
    body.extend_from_slice(&u64::try_from(challenge.len())?.to_be_bytes());
    body.extend_from_slice(challenge);
    body.extend_from_slice(&u64::try_from(instance_id.len())?.to_be_bytes());
    body.extend_from_slice(instance_id);
    body.extend_from_slice(&response.namespace_generation.to_be_bytes());
    body.extend_from_slice(&response.maintenance_generation.to_be_bytes());
    body.extend_from_slice(&response.protocol_version.to_be_bytes());
    body.extend_from_slice(&u64::try_from(topology_digest.len())?.to_be_bytes());
    body.extend_from_slice(topology_digest);
    body.push(u8::from(response.data_movement_active));
    body.extend_from_slice(&response.dirty_usage_generation.to_be_bytes());
    body.push(u8::from(response.dirty_usage_pending));
    Ok(body)
}

/// Length-prefixed, domain-separated byte builder for the disk-mutation canonical bodies below.
/// Every variable-length field is u64-length-prefixed and every list u64-count-prefixed, so
/// distinct field values can never collide into the same canonical bytes.
struct CanonicalBodyBuilder {
    body: Vec<u8>,
}

impl CanonicalBodyBuilder {
    fn new(domain: &'static [u8]) -> Self {
        let mut body = Vec::with_capacity(domain.len() + 128);
        body.extend_from_slice(domain);
        Self { body }
    }

    fn push_bytes(&mut self, field: &[u8]) -> Result<(), std::num::TryFromIntError> {
        self.body.extend_from_slice(&u64::try_from(field.len())?.to_be_bytes());
        self.body.extend_from_slice(field);
        Ok(())
    }

    fn push_str(&mut self, field: &str) -> Result<(), std::num::TryFromIntError> {
        self.push_bytes(field.as_bytes())
    }

    fn push_optional_str(&mut self, field: Option<&str>) -> Result<(), std::num::TryFromIntError> {
        self.body.push(u8::from(field.is_some()));
        self.push_str(field.unwrap_or_default())
    }

    fn push_bool(&mut self, field: bool) {
        self.body.push(u8::from(field));
    }

    fn push_u32(&mut self, field: u32) {
        self.body.extend_from_slice(&field.to_be_bytes());
    }

    fn push_u64(&mut self, field: u64) {
        self.body.extend_from_slice(&field.to_be_bytes());
    }

    fn push_count(&mut self, count: usize) -> Result<(), std::num::TryFromIntError> {
        self.body.extend_from_slice(&u64::try_from(count)?.to_be_bytes());
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.body
    }
}

pub const PEER_RESTSIGNAL: &str = "signal";
pub const PEER_RESTSUB_SYS: &str = "sub-sys";
pub const PEER_RESTDRY_RUN: &str = "dry-run";

/// A stable semantic body for a side-effecting unary RPC.
///
/// Implementations deliberately enumerate handler-consumed fields instead of re-encoding the
/// protobuf message, whose unknown fields and map order are not a mixed-version contract.
pub trait CanonicalMutationBody {
    fn canonical_body(&self) -> Result<Vec<u8>, std::num::TryFromIntError>;
}

macro_rules! impl_canonical_mutation_body {
    ($request:ty, $domain:expr, |$value:ident, $body:ident| $fields:block) => {
        impl CanonicalMutationBody for $request {
            fn canonical_body(&self) -> Result<Vec<u8>, std::num::TryFromIntError> {
                let $value = self;
                let mut $body = CanonicalBodyBuilder::new($domain);
                $fields
                Ok($body.finish())
            }
        }
    };
    ($request:ty, $domain:expr) => {
        impl CanonicalMutationBody for $request {
            fn canonical_body(&self) -> Result<Vec<u8>, std::num::TryFromIntError> {
                Ok(CanonicalBodyBuilder::new($domain).finish())
            }
        }
    };
}

impl_canonical_mutation_body!(
    proto_gen::node_service::SignalServiceRequest,
    b"rustfs-signal-service-request-v1\0",
    |request, body| {
        let vars = request.vars.as_ref().map(|vars| &vars.value);
        body.push_optional_str(vars.and_then(|vars| vars.get(PEER_RESTSIGNAL).map(String::as_str)))?;
        body.push_optional_str(vars.and_then(|vars| vars.get(PEER_RESTSUB_SYS).map(String::as_str)))?;
        body.push_optional_str(vars.and_then(|vars| vars.get(PEER_RESTDRY_RUN).map(String::as_str)))?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::HealBucketRequest,
    b"rustfs-heal-bucket-request-v1\0",
    |request, body| {
        body.push_str(&request.bucket)?;
        body.push_str(&request.options)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::MakeBucketRequest,
    b"rustfs-make-bucket-request-v1\0",
    |request, body| {
        body.push_str(&request.name)?;
        body.push_str(&request.options)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::DeleteBucketRequest,
    b"rustfs-delete-bucket-request-v1\0",
    |request, body| {
        body.push_str(&request.bucket)?;
        body.push_str(&request.options)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::GenerallyLockRequest,
    b"rustfs-lock-request-v1\0",
    |request, body| {
        body.push_str(&request.args)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::BatchGenerallyLockRequest,
    b"rustfs-lock-batch-request-v1\0",
    |request, body| {
        body.push_count(request.args.len())?;
        for arg in &request.args {
            body.push_str(arg)?;
        }
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadBucketMetadataRequest,
    b"rustfs-load-bucket-metadata-request-v1\0",
    |request, body| {
        body.push_str(&request.bucket)?;
        body.push_bool(request.scanner_maintenance_change);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::DeleteBucketMetadataRequest,
    b"rustfs-delete-bucket-metadata-request-v1\0",
    |request, body| {
        body.push_str(&request.bucket)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::DeletePolicyRequest,
    b"rustfs-delete-policy-request-v1\0",
    |request, body| {
        body.push_str(&request.policy_name)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadPolicyRequest,
    b"rustfs-load-policy-request-v1\0",
    |request, body| {
        body.push_str(&request.policy_name)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadPolicyMappingRequest,
    b"rustfs-load-policy-mapping-request-v1\0",
    |request, body| {
        body.push_str(&request.user_or_group)?;
        body.push_u64(request.user_type);
        body.push_bool(request.is_group);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::DeleteUserRequest,
    b"rustfs-delete-user-request-v1\0",
    |request, body| {
        body.push_str(&request.access_key)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::DeleteServiceAccountRequest,
    b"rustfs-delete-service-account-request-v1\0",
    |request, body| {
        body.push_str(&request.access_key)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadUserRequest,
    b"rustfs-load-user-request-v1\0",
    |request, body| {
        body.push_str(&request.access_key)?;
        body.push_bool(request.temp);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadServiceAccountRequest,
    b"rustfs-load-service-account-request-v1\0",
    |request, body| {
        body.push_str(&request.access_key)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadGroupRequest,
    b"rustfs-load-group-request-v1\0",
    |request, body| {
        body.push_str(&request.group)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::ReloadSiteReplicationConfigRequest,
    b"rustfs-reload-site-replication-config-request-v1\0"
);
impl_canonical_mutation_body!(proto_gen::node_service::ReloadPoolMetaRequest, b"rustfs-reload-pool-meta-request-v1\0");
impl_canonical_mutation_body!(
    proto_gen::node_service::StopRebalanceRequest,
    b"rustfs-stop-rebalance-request-v1\0",
    |request, body| {
        body.push_str(&request.expected_rebalance_id)?;
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadRebalanceMetaRequest,
    b"rustfs-load-rebalance-meta-request-v1\0",
    |request, body| {
        body.push_bool(request.start_rebalance);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::StartDecommissionRequest,
    b"rustfs-start-decommission-request-v1\0",
    |request, body| {
        body.push_count(request.pool_indices.len())?;
        for pool_index in &request.pool_indices {
            body.push_u32(*pool_index);
        }
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::CancelDecommissionRequest,
    b"rustfs-cancel-decommission-request-v1\0",
    |request, body| {
        body.push_u32(request.pool_index);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::ClearDecommissionRequest,
    b"rustfs-clear-decommission-request-v1\0",
    |request, body| {
        body.push_u32(request.pool_index);
    }
);
impl_canonical_mutation_body!(
    proto_gen::node_service::LoadTransitionTierConfigRequest,
    b"rustfs-load-transition-tier-config-request-v1\0"
);

// Canonical request bodies for the mutating NodeService disk RPCs (backlog#1327 body-digest
// binding). Each covers every semantic wire field — including both the msgpack `_bin` payload and
// its JSON compatibility copy — so tampering with either encoding, or stripping `_bin` to force
// the JSON fallback decode path, breaks the signature-bound digest.

pub fn canonical_rename_data_request_body(
    request: &proto_gen::node_service::RenameDataRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-rename-data-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.src_volume)?;
    body.push_str(&request.src_path)?;
    body.push_str(&request.file_info)?;
    body.push_str(&request.dst_volume)?;
    body.push_str(&request.dst_path)?;
    body.push_bytes(&request.file_info_bin)?;
    Ok(body.finish())
}

pub fn canonical_delete_version_request_body(
    request: &proto_gen::node_service::DeleteVersionRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-delete-version-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_str(&request.file_info)?;
    body.push_bool(request.force_del_marker);
    body.push_str(&request.opts)?;
    body.push_bytes(&request.file_info_bin)?;
    body.push_bytes(&request.opts_bin)?;
    Ok(body.finish())
}

pub fn canonical_delete_versions_request_body(
    request: &proto_gen::node_service::DeleteVersionsRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-delete-versions-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_count(request.versions.len())?;
    for version in &request.versions {
        body.push_str(version)?;
    }
    body.push_str(&request.opts)?;
    body.push_count(request.versions_bin.len())?;
    for version_bin in &request.versions_bin {
        body.push_bytes(version_bin)?;
    }
    body.push_bytes(&request.opts_bin)?;
    Ok(body.finish())
}

pub fn canonical_write_metadata_request_body(
    request: &proto_gen::node_service::WriteMetadataRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-write-metadata-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_str(&request.file_info)?;
    body.push_bytes(&request.file_info_bin)?;
    Ok(body.finish())
}

pub fn canonical_update_metadata_request_body(
    request: &proto_gen::node_service::UpdateMetadataRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-update-metadata-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_str(&request.file_info)?;
    body.push_str(&request.opts)?;
    body.push_bytes(&request.file_info_bin)?;
    body.push_bytes(&request.opts_bin)?;
    Ok(body.finish())
}

pub fn canonical_write_all_request_body(
    request: &proto_gen::node_service::WriteAllRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-write-all-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_bytes(&request.data)?;
    Ok(body.finish())
}

pub fn canonical_delete_request_body(
    request: &proto_gen::node_service::DeleteRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-delete-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_str(&request.options)?;
    Ok(body.finish())
}

pub fn canonical_delete_paths_request_body(
    request: &proto_gen::node_service::DeletePathsRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-delete-paths-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_count(request.paths.len())?;
    for path in &request.paths {
        body.push_str(path)?;
    }
    Ok(body.finish())
}

pub fn canonical_snapshot_lease_request_body(
    request: &proto_gen::node_service::SnapshotLeaseRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-snapshot-lease-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_u64(request.ttl_ms);
    Ok(body.finish())
}

pub fn canonical_snapshot_lease_renew_request_body(
    request: &proto_gen::node_service::SnapshotLeaseRenewRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-snapshot-lease-renew-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_bytes(&request.token)?;
    body.push_u64(request.ttl_ms);
    Ok(body.finish())
}

pub fn canonical_snapshot_lease_release_request_body(
    request: &proto_gen::node_service::SnapshotLeaseReleaseRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-snapshot-lease-release-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_bytes(&request.token)?;
    Ok(body.finish())
}

pub fn canonical_rename_file_request_body(
    request: &proto_gen::node_service::RenameFileRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-rename-file-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.src_volume)?;
    body.push_str(&request.src_path)?;
    body.push_str(&request.dst_volume)?;
    body.push_str(&request.dst_path)?;
    Ok(body.finish())
}

pub fn canonical_rename_part_request_body(
    request: &proto_gen::node_service::RenamePartRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-rename-part-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.src_volume)?;
    body.push_str(&request.src_path)?;
    body.push_str(&request.dst_volume)?;
    body.push_str(&request.dst_path)?;
    body.push_bytes(&request.meta)?;
    Ok(body.finish())
}

pub fn canonical_prepare_part_transaction_request_body(
    request: &proto_gen::node_service::PreparePartTransactionRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-prepare-part-transaction-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.src_volume)?;
    body.push_str(&request.src_path)?;
    body.push_str(&request.dst_volume)?;
    body.push_str(&request.dst_path)?;
    body.push_bytes(&request.meta)?;
    Ok(body.finish())
}

pub fn canonical_settle_part_transaction_request_body(
    request: &proto_gen::node_service::SettlePartTransactionRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-settle-part-transaction-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    body.push_str(&request.path)?;
    body.push_bool(request.rollback);
    Ok(body.finish())
}

pub fn canonical_delete_volume_request_body(
    request: &proto_gen::node_service::DeleteVolumeRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-delete-volume-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    // Binding `force` is the point: an unbound recursive-delete flag lets an on-path attacker
    // turn a refuse-if-nonempty delete into a recursive volume wipe (backlog#1327).
    body.push_bool(request.force);
    Ok(body.finish())
}

pub fn canonical_make_volume_request_body(
    request: &proto_gen::node_service::MakeVolumeRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-make-volume-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_str(&request.volume)?;
    Ok(body.finish())
}

pub fn canonical_make_volumes_request_body(
    request: &proto_gen::node_service::MakeVolumesRequest,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-make-volumes-request-v1\0");
    body.push_str(&request.disk)?;
    body.push_count(request.volumes.len())?;
    for volume in &request.volumes {
        body.push_str(volume)?;
    }
    Ok(body.finish())
}

#[cfg(test)]
mod disk_mutation_canonical_tests {
    use super::proto_gen::node_service::{
        DeletePathsRequest, DeleteRequest, DeleteVersionRequest, DeleteVersionsRequest, DeleteVolumeRequest, MakeVolumeRequest,
        MakeVolumesRequest, PreparePartTransactionRequest, RenameDataRequest, RenameFileRequest, RenamePartRequest,
        SettlePartTransactionRequest, SnapshotLeaseReleaseRequest, SnapshotLeaseRenewRequest, SnapshotLeaseRequest,
        UpdateMetadataRequest, WriteAllRequest, WriteMetadataRequest,
    };
    use super::*;

    fn assert_all_distinct(bodies: &[Vec<u8>]) {
        for (i, a) in bodies.iter().enumerate() {
            for (j, b) in bodies.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "canonical bodies {i} and {j} must differ");
                }
            }
        }
    }

    #[test]
    fn rename_data_canonical_body_binds_every_field() {
        let baseline = RenameDataRequest {
            disk: "disk-a".into(),
            src_volume: "src-vol".into(),
            src_path: "src-path".into(),
            file_info: "{\"v\":1}".into(),
            dst_volume: "dst-vol".into(),
            dst_path: "dst-path".into(),
            file_info_bin: vec![0x81, 0x01].into(),
        };
        let mut bodies = vec![canonical_rename_data_request_body(&baseline).unwrap()];
        for mutate in [
            |r: &mut RenameDataRequest| r.disk = "disk-b".into(),
            |r: &mut RenameDataRequest| r.src_volume = "src-vol2".into(),
            |r: &mut RenameDataRequest| r.src_path = "src-path2".into(),
            |r: &mut RenameDataRequest| r.file_info = "{\"v\":2}".into(),
            |r: &mut RenameDataRequest| r.dst_volume = "dst-vol2".into(),
            |r: &mut RenameDataRequest| r.dst_path = "dst-path2".into(),
            |r: &mut RenameDataRequest| r.file_info_bin = vec![0x81, 0x02].into(),
            |r: &mut RenameDataRequest| r.file_info_bin = Vec::new().into(),
        ] {
            let mut request = baseline.clone();
            mutate(&mut request);
            bodies.push(canonical_rename_data_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);
    }

    #[test]
    fn rename_data_canonical_body_is_injective_across_field_boundaries() {
        // Length prefixes must prevent moving bytes between adjacent fields from colliding.
        let shifted_left = RenameDataRequest {
            src_volume: "ab".into(),
            src_path: "c".into(),
            ..Default::default()
        };
        let shifted_right = RenameDataRequest {
            src_volume: "a".into(),
            src_path: "bc".into(),
            ..Default::default()
        };
        assert_ne!(
            canonical_rename_data_request_body(&shifted_left).unwrap(),
            canonical_rename_data_request_body(&shifted_right).unwrap(),
        );
    }

    #[test]
    fn delete_version_canonical_body_binds_every_field() {
        let baseline = DeleteVersionRequest {
            disk: "disk-a".into(),
            volume: "vol".into(),
            path: "path".into(),
            file_info: "{\"v\":1}".into(),
            force_del_marker: false,
            opts: "{}".into(),
            file_info_bin: vec![0x81].into(),
            opts_bin: vec![0x80].into(),
        };
        let mut bodies = vec![canonical_delete_version_request_body(&baseline).unwrap()];
        for mutate in [
            |r: &mut DeleteVersionRequest| r.disk = "disk-b".into(),
            |r: &mut DeleteVersionRequest| r.volume = "vol2".into(),
            |r: &mut DeleteVersionRequest| r.path = "path2".into(),
            |r: &mut DeleteVersionRequest| r.file_info = "{\"v\":2}".into(),
            |r: &mut DeleteVersionRequest| r.force_del_marker = true,
            |r: &mut DeleteVersionRequest| r.opts = "{\"o\":1}".into(),
            |r: &mut DeleteVersionRequest| r.file_info_bin = vec![0x82].into(),
            |r: &mut DeleteVersionRequest| r.opts_bin = Vec::new().into(),
        ] {
            let mut request = baseline.clone();
            mutate(&mut request);
            bodies.push(canonical_delete_version_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);
    }

    #[test]
    fn delete_versions_canonical_body_binds_lists_and_options() {
        let baseline = DeleteVersionsRequest {
            disk: "disk-a".into(),
            volume: "vol".into(),
            versions: vec!["v1".into(), "v2".into()],
            opts: "{}".into(),
            versions_bin: vec![vec![0x01].into(), vec![0x02].into()],
            opts_bin: vec![0x80].into(),
        };
        let mut bodies = vec![canonical_delete_versions_request_body(&baseline).unwrap()];
        for mutate in [
            |r: &mut DeleteVersionsRequest| r.disk = "disk-b".into(),
            |r: &mut DeleteVersionsRequest| r.volume = "vol2".into(),
            |r: &mut DeleteVersionsRequest| r.versions = vec!["v1".into(), "v3".into()],
            |r: &mut DeleteVersionsRequest| r.versions = vec!["v1v2".into()],
            |r: &mut DeleteVersionsRequest| r.opts = "{\"o\":1}".into(),
            |r: &mut DeleteVersionsRequest| r.versions_bin = vec![vec![0x01].into(), vec![0x03].into()],
            |r: &mut DeleteVersionsRequest| r.versions_bin = vec![vec![0x01, 0x02].into()],
            |r: &mut DeleteVersionsRequest| r.versions_bin = Vec::new(),
            |r: &mut DeleteVersionsRequest| r.opts_bin = vec![0x81].into(),
        ] {
            let mut request = baseline.clone();
            mutate(&mut request);
            bodies.push(canonical_delete_versions_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);
    }

    #[test]
    fn remaining_disk_mutation_canonical_bodies_bind_every_field() {
        // Mutating each field in turn and asserting all bodies differ catches a dropped or
        // duplicated `push_*` in these hand-written builders — an unbound field is tamperable.
        let write_metadata = WriteMetadataRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            file_info: "{\"a\":1}".into(),
            file_info_bin: vec![0x81].into(),
        };
        let mut bodies = vec![canonical_write_metadata_request_body(&write_metadata).unwrap()];
        for mutate in [
            |r: &mut WriteMetadataRequest| r.disk = "d2".into(),
            |r: &mut WriteMetadataRequest| r.volume = "v2".into(),
            |r: &mut WriteMetadataRequest| r.path = "p2".into(),
            |r: &mut WriteMetadataRequest| r.file_info = "{\"a\":2}".into(),
            |r: &mut WriteMetadataRequest| r.file_info_bin = vec![0x82].into(),
        ] {
            let mut request = write_metadata.clone();
            mutate(&mut request);
            bodies.push(canonical_write_metadata_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let update_metadata = UpdateMetadataRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            file_info: "{\"a\":1}".into(),
            opts: "{\"o\":1}".into(),
            file_info_bin: vec![0x81].into(),
            opts_bin: vec![0x80].into(),
        };
        let mut bodies = vec![canonical_update_metadata_request_body(&update_metadata).unwrap()];
        for mutate in [
            |r: &mut UpdateMetadataRequest| r.disk = "d2".into(),
            |r: &mut UpdateMetadataRequest| r.volume = "v2".into(),
            |r: &mut UpdateMetadataRequest| r.path = "p2".into(),
            |r: &mut UpdateMetadataRequest| r.file_info = "{\"a\":2}".into(),
            |r: &mut UpdateMetadataRequest| r.opts = "{\"o\":2}".into(),
            |r: &mut UpdateMetadataRequest| r.file_info_bin = vec![0x82].into(),
            |r: &mut UpdateMetadataRequest| r.opts_bin = vec![0x81].into(),
        ] {
            let mut request = update_metadata.clone();
            mutate(&mut request);
            bodies.push(canonical_update_metadata_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let write_all = WriteAllRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            data: vec![0xAA, 0xBB].into(),
        };
        let mut bodies = vec![canonical_write_all_request_body(&write_all).unwrap()];
        for mutate in [
            |r: &mut WriteAllRequest| r.disk = "d2".into(),
            |r: &mut WriteAllRequest| r.volume = "v2".into(),
            |r: &mut WriteAllRequest| r.path = "p2".into(),
            |r: &mut WriteAllRequest| r.data = vec![0xAA, 0xBC].into(),
        ] {
            let mut request = write_all.clone();
            mutate(&mut request);
            bodies.push(canonical_write_all_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let delete = DeleteRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            options: "{\"o\":1}".into(),
        };
        let mut bodies = vec![canonical_delete_request_body(&delete).unwrap()];
        for mutate in [
            |r: &mut DeleteRequest| r.disk = "d2".into(),
            |r: &mut DeleteRequest| r.volume = "v2".into(),
            |r: &mut DeleteRequest| r.path = "p2".into(),
            |r: &mut DeleteRequest| r.options = "{\"recursive\":true}".into(),
        ] {
            let mut request = delete.clone();
            mutate(&mut request);
            bodies.push(canonical_delete_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let delete_paths = DeletePathsRequest {
            disk: "d".into(),
            volume: "v".into(),
            paths: vec!["a".into(), "b".into()],
        };
        let mut bodies = vec![canonical_delete_paths_request_body(&delete_paths).unwrap()];
        for mutate in [
            |r: &mut DeletePathsRequest| r.disk = "d2".into(),
            |r: &mut DeletePathsRequest| r.volume = "v2".into(),
            |r: &mut DeletePathsRequest| r.paths = vec!["a".into(), "c".into()],
            |r: &mut DeletePathsRequest| r.paths = vec!["ab".into()],
        ] {
            let mut request = delete_paths.clone();
            mutate(&mut request);
            bodies.push(canonical_delete_paths_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let rename_file = RenameFileRequest {
            disk: "d".into(),
            src_volume: "sv".into(),
            src_path: "sp".into(),
            dst_volume: "dv".into(),
            dst_path: "dp".into(),
        };
        let mut bodies = vec![canonical_rename_file_request_body(&rename_file).unwrap()];
        for mutate in [
            |r: &mut RenameFileRequest| r.disk = "d2".into(),
            |r: &mut RenameFileRequest| r.src_volume = "sv2".into(),
            |r: &mut RenameFileRequest| r.src_path = "sp2".into(),
            |r: &mut RenameFileRequest| r.dst_volume = "dv2".into(),
            |r: &mut RenameFileRequest| r.dst_path = "dp2".into(),
        ] {
            let mut request = rename_file.clone();
            mutate(&mut request);
            bodies.push(canonical_rename_file_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let prepare_part = PreparePartTransactionRequest {
            disk: "d".into(),
            src_volume: "sv".into(),
            src_path: "sp".into(),
            dst_volume: "dv".into(),
            dst_path: "dp".into(),
            meta: vec![0x01].into(),
        };
        let mut bodies = vec![canonical_prepare_part_transaction_request_body(&prepare_part).unwrap()];
        for mutate in [
            |r: &mut PreparePartTransactionRequest| r.disk = "d2".into(),
            |r: &mut PreparePartTransactionRequest| r.src_volume = "sv2".into(),
            |r: &mut PreparePartTransactionRequest| r.src_path = "sp2".into(),
            |r: &mut PreparePartTransactionRequest| r.dst_volume = "dv2".into(),
            |r: &mut PreparePartTransactionRequest| r.dst_path = "dp2".into(),
            |r: &mut PreparePartTransactionRequest| r.meta = vec![0x02].into(),
        ] {
            let mut request = prepare_part.clone();
            mutate(&mut request);
            bodies.push(canonical_prepare_part_transaction_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let settle_part = SettlePartTransactionRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            rollback: false,
        };
        let mut bodies = vec![canonical_settle_part_transaction_request_body(&settle_part).unwrap()];
        for mutate in [
            |r: &mut SettlePartTransactionRequest| r.disk = "d2".into(),
            |r: &mut SettlePartTransactionRequest| r.volume = "v2".into(),
            |r: &mut SettlePartTransactionRequest| r.path = "p2".into(),
            |r: &mut SettlePartTransactionRequest| r.rollback = true,
        ] {
            let mut request = settle_part.clone();
            mutate(&mut request);
            bodies.push(canonical_settle_part_transaction_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);

        let rename_part = RenamePartRequest {
            disk: "d".into(),
            src_volume: "sv".into(),
            src_path: "sp".into(),
            dst_volume: "dv".into(),
            dst_path: "dp".into(),
            meta: vec![0x01].into(),
        };
        let mut bodies = vec![canonical_rename_part_request_body(&rename_part).unwrap()];
        for mutate in [
            |r: &mut RenamePartRequest| r.disk = "d2".into(),
            |r: &mut RenamePartRequest| r.src_volume = "sv2".into(),
            |r: &mut RenamePartRequest| r.src_path = "sp2".into(),
            |r: &mut RenamePartRequest| r.dst_volume = "dv2".into(),
            |r: &mut RenamePartRequest| r.dst_path = "dp2".into(),
            |r: &mut RenamePartRequest| r.meta = vec![0x02].into(),
        ] {
            let mut request = rename_part.clone();
            mutate(&mut request);
            bodies.push(canonical_rename_part_request_body(&request).unwrap());
        }
        assert_all_distinct(&bodies);
    }

    #[test]
    fn volume_mutation_canonical_bodies_bind_their_fields() {
        let delete_volume = DeleteVolumeRequest {
            disk: "http://node-a:9000/data/rustfs0".into(),
            volume: "bucket".into(),
            force: false,
        };
        // The force flag must be part of the signed body: false → true is a recursive wipe.
        let mut recursive = delete_volume.clone();
        recursive.force = true;
        assert_ne!(
            canonical_delete_volume_request_body(&delete_volume).unwrap(),
            canonical_delete_volume_request_body(&recursive).unwrap(),
        );
        let mut retargeted = delete_volume.clone();
        retargeted.volume = "victim".into();
        assert_ne!(
            canonical_delete_volume_request_body(&delete_volume).unwrap(),
            canonical_delete_volume_request_body(&retargeted).unwrap(),
        );

        let make_volume = MakeVolumeRequest {
            disk: "d".into(),
            volume: "v".into(),
        };
        let mut tampered = make_volume.clone();
        tampered.volume = "v2".into();
        assert_ne!(
            canonical_make_volume_request_body(&make_volume).unwrap(),
            canonical_make_volume_request_body(&tampered).unwrap(),
        );

        let make_volumes = MakeVolumesRequest {
            disk: "d".into(),
            volumes: vec!["a".into(), "b".into()],
        };
        let mut merged = make_volumes.clone();
        merged.volumes = vec!["ab".into()];
        assert_ne!(
            canonical_make_volumes_request_body(&make_volumes).unwrap(),
            canonical_make_volumes_request_body(&merged).unwrap(),
        );
    }

    #[test]
    fn snapshot_lease_canonical_bodies_bind_every_field() {
        let acquire = SnapshotLeaseRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            ttl_ms: 60_000,
        };
        let mut acquire_bodies = vec![canonical_snapshot_lease_request_body(&acquire).unwrap()];
        for mutate in [
            |r: &mut SnapshotLeaseRequest| r.disk = "d2".into(),
            |r: &mut SnapshotLeaseRequest| r.volume = "v2".into(),
            |r: &mut SnapshotLeaseRequest| r.path = "p2".into(),
            |r: &mut SnapshotLeaseRequest| r.ttl_ms = 60_001,
        ] {
            let mut request = acquire.clone();
            mutate(&mut request);
            acquire_bodies.push(canonical_snapshot_lease_request_body(&request).unwrap());
        }
        assert_all_distinct(&acquire_bodies);

        let renew = SnapshotLeaseRenewRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            token: vec![1; 16].into(),
            ttl_ms: 60_000,
        };
        let mut changed_token = renew.clone();
        changed_token.token = vec![2; 16].into();
        let mut changed_ttl = renew.clone();
        changed_ttl.ttl_ms += 1;
        assert_all_distinct(&[
            canonical_snapshot_lease_renew_request_body(&renew).unwrap(),
            canonical_snapshot_lease_renew_request_body(&changed_token).unwrap(),
            canonical_snapshot_lease_renew_request_body(&changed_ttl).unwrap(),
        ]);

        let release = SnapshotLeaseReleaseRequest {
            disk: "d".into(),
            volume: "v".into(),
            path: "p".into(),
            token: vec![1; 16].into(),
        };
        let mut changed_release = release.clone();
        changed_release.token = vec![2; 16].into();
        assert_ne!(
            canonical_snapshot_lease_release_request_body(&release).unwrap(),
            canonical_snapshot_lease_release_request_body(&changed_release).unwrap()
        );
    }

    #[test]
    fn disk_mutation_canonical_domains_are_distinct_per_message() {
        // The same field values must never authenticate one RPC's request as another's.
        let rename_file = RenameFileRequest {
            disk: "d".into(),
            src_volume: "sv".into(),
            src_path: "sp".into(),
            dst_volume: "dv".into(),
            dst_path: "dp".into(),
        };
        let rename_part = RenamePartRequest {
            disk: "d".into(),
            src_volume: "sv".into(),
            src_path: "sp".into(),
            dst_volume: "dv".into(),
            dst_path: "dp".into(),
            meta: Vec::new().into(),
        };
        assert_ne!(
            canonical_rename_file_request_body(&rename_file).unwrap(),
            canonical_rename_part_request_body(&rename_part).unwrap(),
        );
    }
}

#[cfg(test)]
mod non_disk_mutation_canonical_tests {
    use super::*;
    use proto_gen::node_service::*;
    use std::collections::HashMap;

    macro_rules! assert_fields_bound {
        ($request:ty, {$($field:ident: $value:expr),+ $(,)?}) => {{
            let baseline = <$request>::default();
            let expected = baseline.canonical_body().expect("baseline canonical body should encode");
            $(
                let mut variant = baseline.clone();
                variant.$field = $value;
                assert_ne!(
                    expected,
                    variant.canonical_body().expect("variant canonical body should encode"),
                    concat!(stringify!($request), " omitted field ", stringify!($field)),
                );
            )+
        }};
    }

    fn signal_request(signal: Option<&str>, sub_system: Option<&str>, dry_run: Option<&str>) -> SignalServiceRequest {
        let mut value = HashMap::new();
        if let Some(signal) = signal {
            value.insert(PEER_RESTSIGNAL.to_string(), signal.to_string());
        }
        if let Some(sub_system) = sub_system {
            value.insert(PEER_RESTSUB_SYS.to_string(), sub_system.to_string());
        }
        if let Some(dry_run) = dry_run {
            value.insert(PEER_RESTDRY_RUN.to_string(), dry_run.to_string());
        }
        SignalServiceRequest {
            vars: Some(Mss { value }),
        }
    }

    #[test]
    fn signal_service_canonical_body_is_versioned_and_binds_every_semantic_field() {
        let request = signal_request(Some("2"), Some("scanner"), Some("false"));
        let baseline = request.canonical_body().expect("small signal request should encode");
        assert!(baseline.starts_with(b"rustfs-signal-service-request-v1\0"));

        for variant in [
            signal_request(Some("1"), Some("scanner"), Some("false")),
            signal_request(Some("2"), Some("heal"), Some("false")),
            signal_request(Some("2"), Some("scanner"), Some("true")),
            signal_request(None, Some("scanner"), Some("false")),
            signal_request(Some("2"), None, Some("false")),
            signal_request(Some("2"), Some("scanner"), None),
        ] {
            assert_ne!(baseline, variant.canonical_body().expect("small signal request should encode"));
        }

        let mut ignored_field = request;
        ignored_field
            .vars
            .as_mut()
            .expect("signal vars should exist")
            .value
            .insert("future-field".to_string(), "ignored".to_string());
        assert_eq!(
            baseline,
            ignored_field
                .canonical_body()
                .expect("unknown signal field should not affect the semantic body"),
        );
    }

    #[test]
    fn signal_service_canonical_body_distinguishes_missing_and_empty_fields() {
        assert_ne!(
            signal_request(None, Some("scanner"), Some("false"))
                .canonical_body()
                .expect("missing signal should encode"),
            signal_request(Some(""), Some("scanner"), Some("false"))
                .canonical_body()
                .expect("empty signal should encode"),
        );
    }

    #[test]
    fn bucket_and_lock_canonical_bodies_bind_every_semantic_field() {
        assert_fields_bound!(HealBucketRequest, { bucket: "bucket".into(), options: "opts".into() });
        assert_fields_bound!(MakeBucketRequest, { name: "bucket".into(), options: "opts".into() });
        assert_fields_bound!(DeleteBucketRequest, { bucket: "bucket".into(), options: "opts".into() });
        assert_fields_bound!(GenerallyLockRequest, { args: "lock".into() });
        assert_fields_bound!(BatchGenerallyLockRequest, { args: vec!["first".into(), "second".into()] });

        let first = BatchGenerallyLockRequest {
            args: vec!["first".into(), "second".into()],
        };
        let reversed = BatchGenerallyLockRequest {
            args: vec!["second".into(), "first".into()],
        };
        assert_ne!(first.canonical_body().unwrap(), reversed.canonical_body().unwrap());
    }

    #[test]
    fn metadata_and_iam_canonical_bodies_bind_every_semantic_field() {
        assert_fields_bound!(LoadBucketMetadataRequest, {
            bucket: "bucket".into(),
            scanner_maintenance_change: true,
        });
        assert_fields_bound!(DeleteBucketMetadataRequest, { bucket: "bucket".into() });
        assert_fields_bound!(DeletePolicyRequest, { policy_name: "policy".into() });
        assert_fields_bound!(LoadPolicyRequest, { policy_name: "policy".into() });
        assert_fields_bound!(LoadPolicyMappingRequest, {
            user_or_group: "user".into(),
            user_type: 1,
            is_group: true,
        });
        assert_fields_bound!(DeleteUserRequest, { access_key: "user".into() });
        assert_fields_bound!(DeleteServiceAccountRequest, { access_key: "service".into() });
        assert_fields_bound!(LoadUserRequest, { access_key: "user".into(), temp: true });
        assert_fields_bound!(LoadServiceAccountRequest, { access_key: "service".into() });
        assert_fields_bound!(LoadGroupRequest, { group: "group".into() });
    }

    #[test]
    fn control_plane_canonical_bodies_bind_every_semantic_field() {
        assert_fields_bound!(StopRebalanceRequest, { expected_rebalance_id: "rebalance".into() });
        assert_fields_bound!(LoadRebalanceMetaRequest, { start_rebalance: true });
        assert_fields_bound!(StartDecommissionRequest, { pool_indices: vec![1, 2] });
        assert_fields_bound!(CancelDecommissionRequest, { pool_index: 1 });
        assert_fields_bound!(ClearDecommissionRequest, { pool_index: 1 });

        let first = StartDecommissionRequest {
            pool_indices: vec![1, 2],
        };
        let reversed = StartDecommissionRequest {
            pool_indices: vec![2, 1],
        };
        assert_ne!(first.canonical_body().unwrap(), reversed.canonical_body().unwrap());

        let empty_domains = [
            ReloadSiteReplicationConfigRequest::default().canonical_body().unwrap(),
            ReloadPoolMetaRequest::default().canonical_body().unwrap(),
            LoadTransitionTierConfigRequest::default().canonical_body().unwrap(),
        ];
        assert!(empty_domains.iter().all(|body| !body.is_empty()));
        assert_ne!(empty_domains[0], empty_domains[1]);
        assert_ne!(empty_domains[0], empty_domains[2]);
        assert_ne!(empty_domains[1], empty_domains[2]);
    }
}

#[cfg(test)]
mod scanner_activity_tests {
    use super::{
        canonical_scanner_activity_request_body, canonical_scanner_activity_response_body,
        canonical_scanner_activity_v4_response_body,
        proto_gen::node_service::{ScannerActivityRequest, ScannerActivityResponse},
    };

    #[test]
    fn canonical_scanner_activity_request_binds_every_field() {
        let request = ScannerActivityRequest {
            challenge: vec![1; 16].into(),
            protocol_version: 5,
            acknowledge_instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            acknowledge_dirty_usage_generation: 11,
        };
        let baseline = canonical_scanner_activity_request_body(&request).expect("scanner activity request should encode");

        let variants = [
            ScannerActivityRequest {
                challenge: vec![2; 16].into(),
                ..request.clone()
            },
            ScannerActivityRequest {
                protocol_version: 4,
                ..request.clone()
            },
            ScannerActivityRequest {
                acknowledge_instance_id: "1123456789abcdef0123456789abcdef".to_string(),
                ..request.clone()
            },
            ScannerActivityRequest {
                acknowledge_dirty_usage_generation: 12,
                ..request
            },
        ];
        for variant in variants {
            assert_ne!(
                baseline,
                canonical_scanner_activity_request_body(&variant).expect("scanner activity request variant should encode")
            );
        }
    }

    #[test]
    fn canonical_scanner_activity_response_binds_challenge_and_every_status_field() {
        let response = ScannerActivityResponse {
            instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            namespace_generation: 7,
            maintenance_generation: 3,
            protocol_version: 4,
            topology_digest: vec![9; 32].into(),
            data_movement_active: true,
            response_proof: Vec::new().into(),
            dirty_usage_generation: 11,
            dirty_usage_pending: true,
        };
        let baseline =
            canonical_scanner_activity_response_body(&[1; 16], &response).expect("scanner activity response should encode");

        let variants = [
            ScannerActivityResponse {
                instance_id: "1123456789abcdef0123456789abcdef".to_string(),
                ..response.clone()
            },
            ScannerActivityResponse {
                namespace_generation: 8,
                ..response.clone()
            },
            ScannerActivityResponse {
                maintenance_generation: 4,
                ..response.clone()
            },
            ScannerActivityResponse {
                protocol_version: 5,
                ..response.clone()
            },
            ScannerActivityResponse {
                topology_digest: vec![8; 32].into(),
                ..response.clone()
            },
            ScannerActivityResponse {
                data_movement_active: false,
                ..response.clone()
            },
            ScannerActivityResponse {
                dirty_usage_generation: 12,
                ..response.clone()
            },
            ScannerActivityResponse {
                dirty_usage_pending: false,
                ..response.clone()
            },
        ];
        for variant in variants {
            assert_ne!(
                baseline,
                canonical_scanner_activity_response_body(&[1; 16], &variant)
                    .expect("scanner activity response variant should encode")
            );
        }
        assert_ne!(
            baseline,
            canonical_scanner_activity_response_body(&[2; 16], &response)
                .expect("scanner activity response with a different challenge should encode")
        );
    }

    #[test]
    fn scanner_activity_v4_response_canonicalization_ignores_v5_fields() {
        let response = ScannerActivityResponse {
            instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            namespace_generation: 7,
            maintenance_generation: 3,
            protocol_version: 4,
            topology_digest: vec![9; 32].into(),
            data_movement_active: true,
            response_proof: Vec::new().into(),
            dirty_usage_generation: 0,
            dirty_usage_pending: false,
        };
        let baseline =
            canonical_scanner_activity_v4_response_body(&[1; 16], &response).expect("scanner activity v4 response should encode");
        let expected = [
            b"rustfs-scanner-activity-response-v1\0".as_slice(),
            16u64.to_be_bytes().as_slice(),
            [1u8; 16].as_slice(),
            32u64.to_be_bytes().as_slice(),
            b"0123456789abcdef0123456789abcdef".as_slice(),
            7u64.to_be_bytes().as_slice(),
            3u64.to_be_bytes().as_slice(),
            4u32.to_be_bytes().as_slice(),
            32u64.to_be_bytes().as_slice(),
            [9u8; 32].as_slice(),
            [1u8].as_slice(),
        ]
        .concat();
        assert_eq!(
            baseline, expected,
            "protocol v4 response bytes must remain stable during rolling upgrades"
        );
        let extended = ScannerActivityResponse {
            dirty_usage_generation: 11,
            dirty_usage_pending: true,
            ..response
        };

        assert_eq!(
            baseline,
            canonical_scanner_activity_v4_response_body(&[1; 16], &extended)
                .expect("scanner activity v4 response should ignore v5 fields")
        );
    }
}

#[cfg(test)]
mod heal_control_tests {
    use super::{
        CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX, HEAL_CONTROL_CAPABILITY_PROBE_PREFIX, HEAL_CONTROL_PROTOCOL_VERSION,
        REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX, canonical_heal_control_capability_ack, canonical_heal_control_request_body,
        canonical_heal_control_response_body, decode_remote_version_state_capability, encode_cross_pool_fence_capability,
        encode_remote_version_state_capability, heal_control_capability_probe, heal_control_coordinator_epoch,
        heal_control_execution_timeout, heal_control_execution_timeout_for, internode_rpc_timeout,
        is_cross_pool_fence_capability_probe, is_heal_control_capability_probe, is_remote_version_state_capability_probe,
        normalize_internode_rpc_timeout, remote_version_state_capability_probe,
    };
    use crate::heal_control;
    use std::time::Duration;

    #[test]
    fn canonical_heal_control_body_binds_every_field_and_boundary() {
        let baseline = canonical_heal_control_request_body(1, "ab", b"c").expect("small request should encode");
        let mut golden = b"rustfs-heal-control-v3\0".to_vec();
        golden.extend_from_slice(&1_u32.to_be_bytes());
        golden.extend_from_slice(&2_u64.to_be_bytes());
        golden.extend_from_slice(b"ab");
        golden.extend_from_slice(&1_u64.to_be_bytes());
        golden.extend_from_slice(b"c");
        assert_eq!(baseline, golden);

        assert_ne!(
            baseline,
            canonical_heal_control_request_body(2, "ab", b"c").expect("small request should encode")
        );
        assert_ne!(
            baseline,
            canonical_heal_control_request_body(1, "ac", b"c").expect("small request should encode")
        );
        assert_ne!(
            baseline,
            canonical_heal_control_request_body(1, "ab", b"d").expect("small request should encode")
        );
        assert_ne!(
            canonical_heal_control_request_body(1, "ab", b"c").expect("small request should encode"),
            canonical_heal_control_request_body(1, "a", b"bc").expect("small request should encode")
        );
    }

    #[test]
    fn canonical_capability_ack_binds_version_and_topology() {
        assert_eq!(HEAL_CONTROL_PROTOCOL_VERSION, 3);
        assert!(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX.starts_with(b"rustfs-heal-control-capability-v3"));
        let probe = heal_control_capability_probe(&[7; 16]);
        let ack = canonical_heal_control_capability_ack(1, "ab", &probe).expect("small acknowledgement should encode");
        let mut golden = b"rustfs-heal-control-capability-ack-v3\0".to_vec();
        golden.extend_from_slice(&1_u32.to_be_bytes());
        golden.extend_from_slice(&2_u64.to_be_bytes());
        golden.extend_from_slice(b"ab");
        golden.extend_from_slice(&u64::try_from(probe.len()).unwrap().to_be_bytes());
        golden.extend_from_slice(&probe);
        assert_eq!(ack, golden);
        assert_ne!(ack, canonical_heal_control_capability_ack(2, "ab", &probe).unwrap());
        assert_ne!(ack, canonical_heal_control_capability_ack(1, "ac", &probe).unwrap());
        assert_ne!(
            ack,
            canonical_heal_control_capability_ack(1, "ab", &heal_control_capability_probe(&[8; 16])).unwrap()
        );
        assert!(is_heal_control_capability_probe(&probe));
        assert!(!is_heal_control_capability_probe(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX));
    }

    #[test]
    fn remote_version_state_capability_probe_requires_exact_nonce() {
        let probe = remote_version_state_capability_probe(&[7; 16]);
        assert!(is_remote_version_state_capability_probe(&probe));
        assert!(!is_remote_version_state_capability_probe(REMOTE_VERSION_STATE_CAPABILITY_PROBE_PREFIX));
    }

    #[test]
    fn remote_version_state_capability_binds_member_and_process_epoch() {
        let encoded =
            encode_remote_version_state_capability("node-a:9000", &[7; 16]).expect("small capability response should encode");
        assert_eq!(
            decode_remote_version_state_capability(&encoded).expect("capability response should decode"),
            ("node-a:9000", &[7; 16])
        );
        assert!(decode_remote_version_state_capability(&encoded[..encoded.len() - 1]).is_err());

        let mut invalid_utf8 =
            encode_remote_version_state_capability("node-a", &[7; 16]).expect("small capability response should encode");
        invalid_utf8[8] = 0xff;
        assert!(decode_remote_version_state_capability(&invalid_utf8).is_err());
    }

    #[test]
    fn cross_pool_fence_capability_binds_version_member_and_epoch() {
        assert_eq!(CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX, b"rustfs-cross-pool-fence-capability-v1\0");
        let mut probe = b"rustfs-cross-pool-fence-capability-v1\0".to_vec();
        probe.extend_from_slice(&[7; 16]);
        assert!(is_cross_pool_fence_capability_probe(&probe));
        assert!(!is_cross_pool_fence_capability_probe(CROSS_POOL_FENCE_CAPABILITY_PROBE_PREFIX));
        let mut wrong_prefix = probe.clone();
        wrong_prefix[0] ^= 1;
        assert!(!is_cross_pool_fence_capability_probe(&wrong_prefix));

        let encoded = encode_cross_pool_fence_capability(0x0102_0304, "node-a:9000", &[7; 16])
            .expect("small capability response should encode");
        let mut expected_response = vec![1, 2, 3, 4];
        expected_response.extend_from_slice(&11_u64.to_be_bytes());
        expected_response.extend_from_slice(b"node-a:9000");
        expected_response.extend_from_slice(&[7; 16]);
        assert_eq!(encoded, expected_response);
    }

    #[test]
    fn canonical_response_binds_request_and_result() {
        let baseline = canonical_heal_control_response_body(2, "abcdef", b"query", b"result").unwrap();
        assert_ne!(baseline, canonical_heal_control_response_body(1, "abcdef", b"query", b"result").unwrap());
        assert_ne!(baseline, canonical_heal_control_response_body(2, "bbcdef", b"query", b"result").unwrap());
        assert_ne!(baseline, canonical_heal_control_response_body(2, "abcdef", b"cancel", b"result").unwrap());
        assert_ne!(
            baseline,
            canonical_heal_control_response_body(2, "abcdef", b"query", b"tampered").unwrap()
        );
    }

    #[test]
    fn coordinator_epoch_is_stable_and_rejects_invalid_fingerprints() {
        assert_eq!(heal_control_coordinator_epoch("0123456789abcdefextra"), Ok(0x0123_4567_89ab_cdef));
        assert_eq!(
            heal_control_coordinator_epoch("0000000000000000"),
            Err("heal control topology epoch is zero")
        );
        assert_eq!(
            heal_control_coordinator_epoch("short"),
            Err("heal control topology fingerprint is too short")
        );
        assert_eq!(
            heal_control_coordinator_epoch("not-hex-value!!!!"),
            Err("heal control topology fingerprint is not hexadecimal")
        );
    }

    #[test]
    fn execution_budget_precedes_transport_timeout() {
        let execution = heal_control_execution_timeout();
        assert!(!execution.is_zero());
        assert!(execution < internode_rpc_timeout());
        assert!(execution <= std::time::Duration::from_millis(heal_control::MAX_LIFETIME_MS as u64));
    }

    #[test]
    fn execution_budget_is_nonzero_for_zero_transport_configuration() {
        let normalized_transport = Duration::from_secs(1);
        assert_eq!(normalize_internode_rpc_timeout(Duration::ZERO), normalized_transport);
        for configured_transport in [Duration::ZERO, normalized_transport] {
            let execution = heal_control_execution_timeout_for(configured_transport);
            assert!(execution > Duration::ZERO);
            assert!(execution < normalized_transport);
        }
    }
}

#[cfg(test)]
mod tier_mutation_rpc_tests {
    use super::{
        TIER_MUTATION_RPC_PROTOCOL_VERSION, TierMutationRpcPhase, TierMutationRpcResponseProofInput,
        canonical_tier_mutation_rpc_body, canonical_tier_mutation_rpc_response_body,
    };
    use crate::proto_gen::node_service::TierMutationPeerState;
    use uuid::uuid;

    #[test]
    fn canonical_tier_mutation_body_binds_phase_id_and_payload() {
        let mutation_id = uuid!("12345678-1234-5678-9abc-def012345678");
        let payload = b"canonical-intent-record";
        let baseline = canonical_tier_mutation_rpc_body(
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            payload,
        )
        .expect("small mutation body should encode");
        let mut golden = b"rustfs-tier-mutation-rpc-v1\0".to_vec();
        golden.extend_from_slice(&1_u32.to_be_bytes());
        golden.extend_from_slice(&7_u64.to_be_bytes());
        golden.extend_from_slice(b"prepare");
        golden.extend_from_slice(mutation_id.as_bytes());
        golden.extend_from_slice(&u64::try_from(payload.len()).expect("payload length should fit").to_be_bytes());
        golden.extend_from_slice(payload);
        assert_eq!(baseline, golden);

        assert_ne!(
            baseline,
            canonical_tier_mutation_rpc_body(2, TierMutationRpcPhase::Prepare, mutation_id, payload)
                .expect("small mutation body should encode")
        );
        assert_ne!(
            baseline,
            canonical_tier_mutation_rpc_body(
                TIER_MUTATION_RPC_PROTOCOL_VERSION,
                TierMutationRpcPhase::Commit,
                mutation_id,
                payload,
            )
            .expect("small mutation body should encode")
        );
        assert_ne!(
            baseline,
            canonical_tier_mutation_rpc_body(
                TIER_MUTATION_RPC_PROTOCOL_VERSION,
                TierMutationRpcPhase::Prepare,
                uuid!("22345678-1234-5678-9abc-def012345678"),
                payload,
            )
            .expect("small mutation body should encode")
        );
        assert_ne!(
            baseline,
            canonical_tier_mutation_rpc_body(
                TIER_MUTATION_RPC_PROTOCOL_VERSION,
                TierMutationRpcPhase::Prepare,
                mutation_id,
                b"canonical-intent-record-tampered",
            )
            .expect("small mutation body should encode")
        );
    }

    #[test]
    fn canonical_tier_mutation_response_binds_request_state_and_error() {
        let mutation_id = uuid!("12345678-1234-5678-9abc-def012345678");
        let payload = b"canonical-intent-record";
        let baseline = canonical_tier_mutation_rpc_response_body(TierMutationRpcResponseProofInput {
            version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
            phase: TierMutationRpcPhase::Prepare,
            mutation_id,
            canonical_payload: payload,
            success: true,
            state: TierMutationPeerState::Prepared as i32,
            applied: true,
            error_info: None,
        })
        .expect("small mutation response should encode");

        let cases = [
            TierMutationRpcResponseProofInput {
                version: 2,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Commit,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id: uuid!("22345678-1234-5678-9abc-def012345678"),
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: b"tampered-intent-record",
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: false,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Committed as i32,
                applied: true,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: false,
                error_info: None,
            },
            TierMutationRpcResponseProofInput {
                version: TIER_MUTATION_RPC_PROTOCOL_VERSION,
                phase: TierMutationRpcPhase::Prepare,
                mutation_id,
                canonical_payload: payload,
                success: true,
                state: TierMutationPeerState::Prepared as i32,
                applied: true,
                error_info: Some("error"),
            },
        ];
        for case in cases {
            assert_ne!(
                baseline,
                canonical_tier_mutation_rpc_response_body(case).expect("small mutation response should encode")
            );
        }
    }
}

/// Whether internode metadata RPCs should send only the msgpack `_bin` payloads and leave the JSON
/// compatibility strings empty (grpc-optimization P2-1). Shared by the client (`remote_disk`) and
/// server (`node_service`) send paths.
///
/// The legacy flag alone is deliberately insufficient: emptying JSON breaks old peers that only
/// decode the compatibility field. Operators must also set the fleet-confirmed guard after the
/// convergence runbook proves every peer supports `_bin` and rollback.
pub fn internode_rpc_msgpack_only() -> bool {
    match INTERNODE_RPC_MSGPACK_ONLY_CACHE.load(Ordering::Acquire) {
        INTERNODE_RPC_MSGPACK_ONLY_CACHE_FALSE => return false,
        INTERNODE_RPC_MSGPACK_ONLY_CACHE_TRUE => return true,
        _ => {}
    }

    let enabled = rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY,
        rustfs_config::DEFAULT_INTERNODE_RPC_MSGPACK_ONLY,
    ) && rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED,
        rustfs_config::DEFAULT_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED,
    );
    INTERNODE_RPC_MSGPACK_ONLY_CACHE.store(
        if enabled {
            INTERNODE_RPC_MSGPACK_ONLY_CACHE_TRUE
        } else {
            INTERNODE_RPC_MSGPACK_ONLY_CACHE_FALSE
        },
        Ordering::Release,
    );
    enabled
}

#[doc(hidden)]
pub fn reset_internode_rpc_msgpack_only_cache() {
    INTERNODE_RPC_MSGPACK_ONLY_CACHE.store(INTERNODE_RPC_MSGPACK_ONLY_CACHE_UNSET, Ordering::Release);
}

/// Consecutive-failure threshold after which an internode peer is marked offline (grpc-optimization
/// P3). Drives the `rustfs_cluster_servers_offline_total` gauge.
fn internode_offline_failure_threshold() -> u32 {
    rustfs_utils::get_env_u32(
        rustfs_config::ENV_INTERNODE_OFFLINE_FAILURE_THRESHOLD,
        rustfs_config::DEFAULT_INTERNODE_OFFLINE_FAILURE_THRESHOLD,
    )
}

/// Class of internode gRPC channel used to route an RPC (grpc-optimization P1).
///
/// - [`ChannelClass::Control`]: latency-sensitive control-plane RPCs (locks, health, small
///   metadata). Always uses the per-peer control connection keyed by the bare address.
/// - [`ChannelClass::Bulk`]: large `bytes`-carrying unary RPCs
///   (`ReadAll`/`WriteAll`/`ReadMultiple`/`BatchReadVersion`). When channel isolation is enabled
///   these are routed onto a separate per-peer bulk connection pool so a large transfer cannot
///   head-of-line block a lock RPC on the shared HTTP/2 connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelClass {
    Control,
    Bulk,
}

// Keep multiplexed unary RPCs below h2's per-connection small-frame budget.
const INTERNODE_RPC_CONCURRENCY_LIMIT: usize = 64;

/// Whether control/bulk channel isolation is enabled (env-gated, default off for safe rollout).
fn channel_isolation_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_CHANNEL_ISOLATION,
        rustfs_config::DEFAULT_INTERNODE_CHANNEL_ISOLATION,
    )
}

/// Number of bulk channels maintained per peer (>= 1).
fn bulk_channel_pool_size() -> usize {
    rustfs_utils::get_env_usize(rustfs_config::ENV_INTERNODE_BULK_CHANNELS, rustfs_config::DEFAULT_INTERNODE_BULK_CHANNELS).max(1)
}

/// Round-robin cursor for selecting a bulk channel within a peer's pool. A single global cursor
/// is sufficient: it advances once per bulk acquisition and `% pool_size` spreads consecutive
/// acquisitions across the pool.
static BULK_CHANNEL_CURSOR: AtomicUsize = AtomicUsize::new(0);

/// Connection-cache key for the `idx`-th bulk channel to `addr`. The NUL separator cannot appear
/// in a URL, so a bulk key never collides with the control key (the bare `addr`).
fn bulk_cache_key(addr: &str, idx: usize) -> String {
    format!("{addr}\u{0}bulk\u{0}{idx}")
}

/// Acquire a cached-or-newly-dialed channel for the given peer and channel class.
///
/// For [`ChannelClass::Control`] (or whenever isolation is disabled) this is exactly the legacy
/// behavior: reuse the cached control channel keyed by `addr`, else dial a new one. For
/// [`ChannelClass::Bulk`] with isolation enabled, a channel is round-robin selected from the
/// per-peer bulk pool and dialed on demand, physically isolating large transfers from
/// control-plane RPCs.
pub async fn get_channel_for_class(addr: &str, class: ChannelClass) -> Result<Channel, Box<dyn Error>> {
    if class == ChannelClass::Control || !channel_isolation_enabled() {
        if let Some(channel) = cached_connection(addr).await {
            debug!("Using cached control gRPC channel for: {}", addr);
            return Ok(channel);
        }
        return create_new_channel(addr).await;
    }

    let pool_size = bulk_channel_pool_size();
    let idx = BULK_CHANNEL_CURSOR.fetch_add(1, Ordering::Relaxed) % pool_size;
    let cache_key = bulk_cache_key(addr, idx);
    if let Some(channel) = cached_connection(&cache_key).await {
        debug!("Using cached bulk gRPC channel {} for: {}", idx, addr);
        return Ok(channel);
    }
    build_channel(addr, &cache_key).await
}

/// Creates a new gRPC channel with optimized keepalive settings for cluster resilience.
///
/// This function is designed to detect dead peers quickly using env-configurable
/// internode transport settings. Defaults come from `rustfs_config` constants:
/// - Connect timeout: `DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS` (3s)
/// - TCP keepalive: `DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS` (10s)
/// - HTTP/2 keepalive interval: `DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS` (5s)
/// - HTTP/2 keepalive timeout: `DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS` (3s)
/// - RPC timeout: `DEFAULT_INTERNODE_RPC_TIMEOUT_SECS` (10s)
pub async fn create_new_channel(addr: &str) -> Result<Channel, Box<dyn Error>> {
    // The control channel is cached under the bare address, preserving the legacy key.
    build_channel(addr, addr).await
}

/// Dial a new gRPC channel to `dial_addr` and cache it under `cache_key`.
///
/// `dial_addr` is the real peer URL used for the TCP/TLS connection and hostname verification;
/// `cache_key` is the connection-cache key. They are identical for control channels and differ
/// for isolated bulk channels (see [`bulk_cache_key`]), letting several physically distinct
/// channels to the same peer be cached independently.
async fn build_channel(dial_addr: &str, cache_key: &str) -> Result<Channel, Box<dyn Error>> {
    debug!("Creating new gRPC channel to: {} (cache key: {})", dial_addr, cache_key);
    let dial_started_at = Instant::now();
    let connect_timeout = internode_connect_timeout();
    let tcp_keepalive = internode_tcp_keepalive();
    let http2_keepalive_interval = internode_http2_keep_alive_interval();
    let http2_keepalive_timeout = internode_http2_keep_alive_timeout();
    let rpc_timeout = internode_rpc_timeout();

    let mut connector = Endpoint::from_shared(dial_addr.to_string())?
        // Fast connection timeout for dead peer detection
        .connect_timeout(connect_timeout)
        .concurrency_limit(INTERNODE_RPC_CONCURRENCY_LIMIT)
        // TCP-level keepalive - OS will probe connection
        .tcp_keepalive(Some(tcp_keepalive))
        // Disable Nagle so latency-sensitive control-plane RPCs (locks/health) are not batched
        .tcp_nodelay(internode_rpc_tcp_nodelay())
        // HTTP/2 PING frames for application-layer health check
        .http2_keep_alive_interval(http2_keepalive_interval)
        // How long to wait for PING ACK before considering connection dead
        .keep_alive_timeout(http2_keepalive_timeout)
        // Send PINGs even when no active streams (critical for idle connections)
        .keep_alive_while_idle(true)
        // Overall timeout for any RPC - fail fast on unresponsive peers
        .timeout(rpc_timeout);

    // Raise HTTP/2 flow-control windows above the 64KiB library default so larger unary
    // responses (ReadMultiple/BatchReadVersion) are not throttled by the BDP. Mirrors the
    // server-side window tuning in `rustfs/src/server/http.rs`.
    if let Some(stream_window) = internode_rpc_http2_stream_window() {
        connector = connector.initial_stream_window_size(stream_window);
    }
    if let Some(conn_window) = internode_rpc_http2_conn_window() {
        connector = connector.initial_connection_window_size(conn_window);
    }

    let outbound_tls = runtime_sources::outbound_tls_state().await;
    let generation = outbound_tls.generation.0;
    let mut stale_generation = false;
    {
        let generation_cache = TLS_GENERATION_CACHE.lock().await;
        if let Some(cached_generation) = generation_cache.get(cache_key)
            && *cached_generation != generation
        {
            stale_generation = true;
        }
    }
    if dial_addr.starts_with(RUSTFS_HTTPS_PREFIX) {
        if let Some(cert_pem) = outbound_tls.root_ca_pem.as_ref() {
            let ca = Certificate::from_pem(cert_pem);
            // Derive the hostname from the HTTPS URL for TLS hostname verification.
            let domain = dial_addr
                .trim_start_matches(RUSTFS_HTTPS_PREFIX)
                .split('/')
                .next()
                .unwrap_or("")
                .split(':')
                .next()
                .unwrap_or("");
            let tls = if !domain.is_empty() {
                let mut cfg = ClientTlsConfig::new().ca_certificate(ca).domain_name(domain);
                if let Some(id) = outbound_tls.mtls_identity.as_ref() {
                    let identity = tonic::transport::Identity::from_pem(id.cert_pem.clone(), id.key_pem.clone());
                    cfg = cfg.identity(identity);
                }
                cfg
            } else {
                // Fallback: configure TLS without explicit domain if parsing fails.
                ClientTlsConfig::new().ca_certificate(ca)
            };
            connector = connector.tls_config(tls)?;
            debug!("Configured TLS with custom root certificate for: {}", dial_addr);
        } else {
            // No custom root CA published — fall back to system roots.
            // This is the expected path when no TLS path is configured.
            debug!("No custom root certificate configured; using system roots for TLS: {}", dial_addr);
            connector = connector.tls_config(ClientTlsConfig::new())?;
        }
    }

    let channel = match connector.connect().await {
        Ok(channel) => {
            runtime_sources::record_grpc_dial_result(dial_started_at.elapsed(), true);
            // A successful dial marks the peer online (grpc-optimization P3). Keyed by the real
            // peer address, so control and bulk channels to one peer share health state.
            runtime_sources::record_peer_reachable(dial_addr);
            channel
        }
        Err(err) => {
            runtime_sources::record_grpc_dial_result(dial_started_at.elapsed(), false);
            runtime_sources::record_peer_unreachable(dial_addr, internode_offline_failure_threshold());
            return Err(err.into());
        }
    };

    cache_connection(cache_key.to_string(), channel.clone()).await;
    {
        let mut generation_cache = TLS_GENERATION_CACHE.lock().await;
        enforce_tls_generation_cache_bound(&mut generation_cache, generation, cache_key);
        generation_cache.insert(cache_key.to_string(), generation);
    }
    if stale_generation {
        runtime_sources::record_stale_grpc_channel_tls_generation();
    }

    debug!(
        "Successfully created and cached gRPC channel to: {} (cache key: {})",
        dial_addr, cache_key
    );
    Ok(channel)
}

/// Evict a connection from the cache after a failure.
/// This should be called when an RPC fails to ensure fresh connections are tried.
pub async fn evict_failed_connection(addr: &str) {
    evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Warn).await;
}

/// Evict a connection from the cache after a failure with an explicit log level.
pub async fn evict_failed_connection_with_log_level(addr: &str, log_level: ConnectionEvictionLogLevel) {
    match log_level {
        ConnectionEvictionLogLevel::Warn => {
            warn!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
        ConnectionEvictionLogLevel::Info => {
            info!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
        ConnectionEvictionLogLevel::Debug => {
            debug!(
                addr = %addr,
                "Evicting cached gRPC connection after RPC failure; the next request will attempt to reconnect automatically"
            );
        }
    }

    let cache_log_level = match log_level {
        ConnectionEvictionLogLevel::Warn => ConnectionEvictionLogLevel::Info,
        level => level,
    };
    evict_connection_with_log_level(addr, cache_log_level).await;
    TLS_GENERATION_CACHE.lock().await.remove(addr);

    // An RPC-triggered eviction is a peer-failure signal; feed it into the online/offline state so
    // the peer flips offline after enough consecutive failures (grpc-optimization P3).
    runtime_sources::record_peer_unreachable(addr, internode_offline_failure_threshold());

    // A peer failure typically affects every channel to that peer, so also drop any isolated
    // bulk channels rather than leaving a half-dead connection cached. Round-robin selection
    // means the caller cannot know which bulk index it hit, so evict the whole pool.
    if channel_isolation_enabled() {
        for idx in 0..bulk_channel_pool_size() {
            let cache_key = bulk_cache_key(addr, idx);
            evict_connection_with_log_level(&cache_key, cache_log_level).await;
            TLS_GENERATION_CACHE.lock().await.remove(&cache_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;
    use std::sync::Mutex;

    static INTERNODE_RPC_MSGPACK_ONLY_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone, PartialEq, prost::Message)]
    struct BackgroundHealStatusRequestV1 {}

    #[test]
    fn background_heal_status_request_remains_rolling_upgrade_compatible() {
        let current = proto_gen::node_service::BackgroundHealStatusRequest {
            protocol_version: BACKGROUND_HEAL_STATUS_PROTOCOL_VERSION,
        };
        let encoded = current.encode_to_vec();
        BackgroundHealStatusRequestV1::decode(encoded.as_slice()).expect("v1 server should ignore the version field");

        let encoded = BackgroundHealStatusRequestV1 {}.encode_to_vec();
        let decoded = proto_gen::node_service::BackgroundHealStatusRequest::decode(encoded.as_slice())
            .expect("v2 server should accept a v1 request");
        assert_eq!(decoded.protocol_version, 0);
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct CompatPayloadField {
        message: &'static str,
        json_field: &'static str,
        bin_field: &'static str,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RequestJsonPolicy {
        MsgpackOnlyEligible,
        AlwaysDualWriteUntilFallbackZero,
    }

    #[derive(Clone, Copy, Debug)]
    struct RequestCompatSendSite {
        field: CompatPayloadField,
        json_encoder: &'static str,
        policy: RequestJsonPolicy,
    }

    #[derive(Clone, Copy, Debug)]
    struct ResponseCompatSendSite {
        field: CompatPayloadField,
        json_encoder: &'static str,
    }

    const REQUEST_COMPAT_SEND_SITES: &[RequestCompatSendSite] = &[
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "BatchReadVersionRequest",
                json_field: "batch_read_version_req",
                bin_field: "batch_read_version_req_bin",
            },
            json_encoder: "let batch_read_version_req = compat_json(&req)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "DeleteVersionRequest",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info = serde_json::to_string(&fi)?;",
            policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "DeleteVersionRequest",
                json_field: "opts",
                bin_field: "opts_bin",
            },
            json_encoder: "let opts = serde_json::to_string(&opts)?;",
            policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "DeleteVersionsRequest",
                json_field: "opts",
                bin_field: "opts_bin",
            },
            json_encoder: "let opts = match serde_json::to_string(&opts) {",
            policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "DeleteVersionsRequest",
                json_field: "versions",
                bin_field: "versions_bin",
            },
            json_encoder: "versions_str.push(match serde_json::to_string(file_info_versions) {",
            policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "ReadMultipleRequest",
                json_field: "read_multiple_req",
                bin_field: "read_multiple_req_bin",
            },
            json_encoder: "let read_multiple_req = compat_json(&req)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "ReadVersionRequest",
                json_field: "opts",
                bin_field: "opts_bin",
            },
            json_encoder: "let encoded_opts = compat_json(opts).and_then(|opts_str| encode_msgpack(opts).map(|opts_bin| (opts_str, opts_bin)));",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "RenameDataRequest",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info = compat_json(&fi)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "UpdateMetadataRequest",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info = compat_json(&fi)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "UpdateMetadataRequest",
                json_field: "opts",
                bin_field: "opts_bin",
            },
            json_encoder: "let opts_str = compat_json(&opts)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
        RequestCompatSendSite {
            field: CompatPayloadField {
                message: "WriteMetadataRequest",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info = compat_json(&fi)?;",
            policy: RequestJsonPolicy::MsgpackOnlyEligible,
        },
    ];

    const RESPONSE_COMPAT_SEND_SITES: &[ResponseCompatSendSite] = &[
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "BatchReadVersionResponse",
                json_field: "batch_read_version_resps",
                bin_field: "batch_read_version_resps_bin",
            },
            json_encoder: "compat_response_json(batch_read_version_resp, request_decoded_from_msgpack)",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadMultipleResponse",
                json_field: "read_multiple_resps",
                bin_field: "read_multiple_resps_bin",
            },
            json_encoder: "compat_response_json(read_multiple_resp, false)",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadVersionResponse",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info_json = compat_response_json(&file_info, request_had_msgpack_payload);",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadXLResponse",
                json_field: "raw_file_info",
                bin_field: "raw_file_info_bin",
            },
            json_encoder: "let raw_file_info_json = compat_response_json(&raw_file_info, false);",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "RenameDataResponse",
                json_field: "rename_data_resp",
                bin_field: "rename_data_resp_bin",
            },
            json_encoder: "let rename_data_resp_json = compat_response_json(rename_data_resp, request_decoded_from_msgpack)",
        },
    ];

    fn proto_bin_json_fields(message_suffix: &str) -> Vec<CompatPayloadField> {
        let proto = include_str!("node.proto");
        let mut fields = Vec::new();
        let mut current_message = None;

        for line in proto.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("message ") {
                current_message = rest.split_whitespace().next();
                continue;
            }
            if line == "}" {
                current_message = None;
                continue;
            }
            let Some(message) = current_message else {
                continue;
            };
            if line.starts_with("//") || !message.ends_with(message_suffix) || !line.contains("_bin") {
                continue;
            }
            let Some(bin_field) = line.split_whitespace().find(|part| part.ends_with("_bin")) else {
                continue;
            };
            let Some(json_field) = bin_field.strip_suffix("_bin") else {
                continue;
            };
            fields.push(CompatPayloadField {
                message,
                json_field,
                bin_field,
            });
        }

        fields.sort();
        fields
    }

    fn production_source(source: &'static str, file_name: &str) -> &'static str {
        source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or_else(|| panic!("{file_name} should contain production source before tests"))
    }

    #[test]
    fn request_compat_send_site_manifest_covers_node_proto_bin_fields() {
        let mut manifest_fields = REQUEST_COMPAT_SEND_SITES
            .iter()
            .map(|send_site| send_site.field)
            .collect::<Vec<_>>();
        manifest_fields.sort();
        manifest_fields.dedup();

        assert_eq!(
            manifest_fields.len(),
            REQUEST_COMPAT_SEND_SITES.len(),
            "duplicate request send-site manifest entry"
        );
        assert_eq!(manifest_fields, proto_bin_json_fields("Request"));
    }

    #[test]
    fn request_compat_send_site_manifest_pins_json_policy_and_encoder() {
        let source = production_source(include_str!("../../ecstore/src/cluster/rpc/remote_disk.rs"), "remote_disk.rs");
        let msgpack_only_eligible = REQUEST_COMPAT_SEND_SITES
            .iter()
            .filter(|send_site| send_site.policy == RequestJsonPolicy::MsgpackOnlyEligible)
            .count();
        let always_dual_write = REQUEST_COMPAT_SEND_SITES
            .iter()
            .filter(|send_site| send_site.policy == RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero)
            .count();

        assert_eq!(msgpack_only_eligible, 7);
        assert_eq!(always_dual_write, 4);
        for send_site in REQUEST_COMPAT_SEND_SITES {
            assert!(
                source.contains(send_site.json_encoder),
                "{}.{} must keep its manifest encoder: {}",
                send_site.field.message,
                send_site.field.json_field,
                send_site.json_encoder
            );
        }
    }

    #[test]
    fn request_compat_send_site_manifest_pins_exact_json_policies() {
        let mut policies = REQUEST_COMPAT_SEND_SITES
            .iter()
            .map(|send_site| (send_site.field.message, send_site.field.json_field, send_site.policy))
            .collect::<Vec<_>>();
        policies.sort_by_key(|(message, json_field, _)| (*message, *json_field));

        assert_eq!(
            policies,
            [
                (
                    "BatchReadVersionRequest",
                    "batch_read_version_req",
                    RequestJsonPolicy::MsgpackOnlyEligible
                ),
                ("DeleteVersionRequest", "file_info", RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,),
                ("DeleteVersionRequest", "opts", RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero),
                ("DeleteVersionsRequest", "opts", RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,),
                ("DeleteVersionsRequest", "versions", RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,),
                ("ReadMultipleRequest", "read_multiple_req", RequestJsonPolicy::MsgpackOnlyEligible),
                ("ReadVersionRequest", "opts", RequestJsonPolicy::MsgpackOnlyEligible),
                ("RenameDataRequest", "file_info", RequestJsonPolicy::MsgpackOnlyEligible),
                ("UpdateMetadataRequest", "file_info", RequestJsonPolicy::MsgpackOnlyEligible),
                ("UpdateMetadataRequest", "opts", RequestJsonPolicy::MsgpackOnlyEligible),
                ("WriteMetadataRequest", "file_info", RequestJsonPolicy::MsgpackOnlyEligible),
            ]
        );
    }

    #[test]
    fn response_compat_send_site_manifest_covers_node_proto_bin_fields() {
        let mut manifest_fields = RESPONSE_COMPAT_SEND_SITES
            .iter()
            .map(|send_site| send_site.field)
            .collect::<Vec<_>>();
        manifest_fields.sort();
        manifest_fields.dedup();

        assert_eq!(
            manifest_fields.len(),
            RESPONSE_COMPAT_SEND_SITES.len(),
            "duplicate response send-site manifest entry"
        );
        assert_eq!(manifest_fields, proto_bin_json_fields("Response"));
    }

    #[test]
    fn response_compat_send_site_manifest_pins_json_encoder() {
        let source = production_source(include_str!("../../../rustfs/src/storage/rpc/node_service/disk.rs"), "disk.rs");

        for send_site in RESPONSE_COMPAT_SEND_SITES {
            assert!(
                source.contains(send_site.json_encoder),
                "{}.{} must keep its manifest encoder: {}",
                send_site.field.message,
                send_site.field.json_field,
                send_site.json_encoder
            );
        }
    }

    #[test]
    fn enforce_tls_generation_cache_bound_evicts_when_retained_entries_still_full() {
        let mut cache = HashMap::new();
        for i in 0..TLS_GENERATION_CACHE_MAX_SIZE {
            cache.insert(format!("node-{i}"), 42);
        }

        enforce_tls_generation_cache_bound(&mut cache, 42, "new-node");
        assert_eq!(cache.len(), TLS_GENERATION_CACHE_MAX_SIZE - 1);
    }

    #[tokio::test]
    async fn evict_failed_connection_with_log_level_removes_cached_connection() {
        let addr = "http://evict-failed-connection-debug-test";
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        cache_connection(addr.to_string(), channel).await;

        evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Debug).await;

        assert!(!rustfs_common::has_cached_connection(addr).await);
    }

    #[test]
    fn bulk_cache_key_is_distinct_and_cannot_collide_with_control_key() {
        let addr = "https://node-a:9000";
        let k0 = bulk_cache_key(addr, 0);
        let k1 = bulk_cache_key(addr, 1);
        assert_ne!(k0, k1);
        assert_ne!(k0, addr);
        assert!(k0.starts_with(addr));
        // The NUL separator cannot appear in a URL, so a bulk key never equals a real address.
        assert!(k0.contains('\u{0}'));
    }

    #[test]
    fn bulk_channel_pool_size_is_at_least_one() {
        // Even without env configuration the pool size is clamped to a usable minimum.
        assert!(bulk_channel_pool_size() >= 1);
    }

    #[test]
    fn internode_rpc_msgpack_only_reuses_cached_env_until_reset() {
        let _guard = INTERNODE_RPC_MSGPACK_ONLY_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        reset_internode_rpc_msgpack_only_cache();

        temp_env::with_vars(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                assert!(internode_rpc_msgpack_only());
            },
        );

        temp_env::with_vars(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                assert!(internode_rpc_msgpack_only(), "cached value should avoid repeated env reads");
                reset_internode_rpc_msgpack_only_cache();
                assert!(!internode_rpc_msgpack_only(), "reset must reload current env values");
            },
        );

        reset_internode_rpc_msgpack_only_cache();
    }

    #[test]
    fn internode_rpc_msgpack_only_requires_request_and_fleet_confirmation() {
        let _guard = INTERNODE_RPC_MSGPACK_ONLY_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        for (requested, fleet_confirmed, expected) in [
            (None, None, false),
            (Some("true"), None, false),
            (None, Some("true"), false),
            (Some("true"), Some("false"), false),
            (Some("false"), Some("true"), false),
            (Some("true"), Some("true"), true),
        ] {
            reset_internode_rpc_msgpack_only_cache();
            temp_env::with_vars(
                [
                    (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, requested),
                    (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, fleet_confirmed),
                ],
                || {
                    assert_eq!(internode_rpc_msgpack_only(), expected);
                },
            );
        }

        reset_internode_rpc_msgpack_only_cache();
    }

    #[tokio::test]
    async fn get_channel_for_class_bulk_reuses_control_cache_when_isolation_disabled() {
        // Isolation defaults off, so a Bulk request must reuse the control channel keyed by the
        // bare address and must NOT create a separate bulk-keyed entry (zero behavior change).
        let addr = "http://get-channel-isolation-off-test";
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        cache_connection(addr.to_string(), channel).await;

        let acquired = get_channel_for_class(addr, ChannelClass::Bulk).await;
        assert!(acquired.is_ok());
        assert!(!rustfs_common::has_cached_connection(&bulk_cache_key(addr, 0)).await);

        evict_failed_connection_with_log_level(addr, ConnectionEvictionLogLevel::Debug).await;
    }
}
