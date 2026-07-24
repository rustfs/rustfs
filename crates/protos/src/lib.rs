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
    sync::atomic::{AtomicUsize, Ordering},
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
static TLS_GENERATION_CACHE: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

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
pub const HEAL_CONTROL_PROTOCOL_VERSION: u32 = 2;
pub const HEAL_CONTROL_CAPABILITY_PROBE_PREFIX: &[u8] = b"rustfs-heal-control-capability-v2\0";
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

/// Builds the stable byte representation authenticated for a heal-control request.
///
/// This deliberately does not reuse protobuf encoding: mixed-version peers may
/// retain unknown protobuf fields, which must not change the signed contract.
pub fn canonical_heal_control_request_body(
    version: u32,
    topology_fingerprint: &str,
    command: &[u8],
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    const DOMAIN: &[u8] = b"rustfs-heal-control-v2\0";

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
    const DOMAIN: &[u8] = b"rustfs-heal-control-capability-ack-v2\0";

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
    const DOMAIN: &[u8] = b"rustfs-heal-control-response-v2\0";

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

#[cfg(test)]
mod heal_control_tests {
    use super::{
        HEAL_CONTROL_CAPABILITY_PROBE_PREFIX, HEAL_CONTROL_PROTOCOL_VERSION, canonical_heal_control_capability_ack,
        canonical_heal_control_request_body, canonical_heal_control_response_body, heal_control_capability_probe,
        heal_control_coordinator_epoch, heal_control_execution_timeout, heal_control_execution_timeout_for,
        internode_rpc_timeout, is_heal_control_capability_probe, normalize_internode_rpc_timeout,
    };
    use crate::heal_control;
    use std::time::Duration;

    #[test]
    fn canonical_heal_control_body_binds_every_field_and_boundary() {
        let baseline = canonical_heal_control_request_body(1, "ab", b"c").expect("small request should encode");
        let mut golden = b"rustfs-heal-control-v2\0".to_vec();
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
        assert_eq!(HEAL_CONTROL_PROTOCOL_VERSION, 2);
        assert!(HEAL_CONTROL_CAPABILITY_PROBE_PREFIX.starts_with(b"rustfs-heal-control-capability-v2"));
        let probe = heal_control_capability_probe(&[7; 16]);
        let ack = canonical_heal_control_capability_ack(1, "ab", &probe).expect("small acknowledgement should encode");
        let mut golden = b"rustfs-heal-control-capability-ack-v2\0".to_vec();
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
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY,
        rustfs_config::DEFAULT_INTERNODE_RPC_MSGPACK_ONLY,
    ) && rustfs_utils::get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED,
        rustfs_config::DEFAULT_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED,
    )
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
            json_encoder: "let opts_str = compat_json(opts)?;",
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
            json_encoder: "compat_response_json(batch_read_version_resp)",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadMultipleResponse",
                json_field: "read_multiple_resps",
                bin_field: "read_multiple_resps_bin",
            },
            json_encoder: "compat_response_json(read_multiple_resp)",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadVersionResponse",
                json_field: "file_info",
                bin_field: "file_info_bin",
            },
            json_encoder: "let file_info_json = compat_response_json(&file_info);",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "ReadXLResponse",
                json_field: "raw_file_info",
                bin_field: "raw_file_info_bin",
            },
            json_encoder: "let raw_file_info_json = compat_response_json(&raw_file_info);",
        },
        ResponseCompatSendSite {
            field: CompatPayloadField {
                message: "RenameDataResponse",
                json_field: "rename_data_resp",
                bin_field: "rename_data_resp_bin",
            },
            json_encoder: "let rename_data_resp_json = compat_response_json(&rename_data_resp);",
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
            .split("\n#[cfg(test)]")
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
