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

//! Internode RPC HMAC authentication.
//!
//! # Security regression coverage
//!
//! GHSA-r5qv-rc46-hv8q (internode RPC authentication must fail closed, fixed in
//! rustfs/rustfs#4402) is anchored by the `ghsa_r5qv_*` tests in the module
//! below, plus the broader negative-signature suite. The advisory class is: a
//! node must never accept an RPC whose auth is missing, malformed, or signed
//! with the default/empty shared secret. Body-bound v2 requests and all replay-scoped v3
//! requests additionally receive process-local replay protection. See
//! `docs/testing/security-regressions.md` for the full advisory -> test map.
//!
//! Advisory: <https://github.com/rustfs/rustfs/security/advisories/GHSA-r5qv-rc46-hv8q>

use crate::cluster::rpc::context_propagation::{inject_request_id_into_http_headers, inject_trace_context_into_http_headers};
use crate::storage_api_contracts::internode::{
    NS_SCANNER_PROTOCOL_VERSION, PUT_FILE_AUTH_TRAILER_DIGEST_LEN, PUT_FILE_AUTH_TRAILER_LEN, PUT_FILE_AUTH_TRAILER_MAC_LEN,
    PUT_FILE_AUTH_TRAILER_MAGIC, PUT_FILE_CAPABILITY_VERSION,
};

use hashbrown::HashSet;
use http::uri::Authority;
use http::{HeaderMap, HeaderValue, Method, Uri};
#[cfg(test)]
use rustfs_credentials::{DEFAULT_SECRET_KEY, RPC_SECRET_REQUIRED_MESSAGE};
use rustfs_credentials::{RPC_SECRET_REQUIRED_OPERATOR_MESSAGE, try_get_rpc_token};
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
    INTERNODE_OPERATION_GRPC_FORCE_UNLOCK, INTERNODE_OPERATION_GRPC_LOCK, INTERNODE_OPERATION_GRPC_LOCK_BATCH,
    INTERNODE_OPERATION_GRPC_OTHER, INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_READ_MULTIPLE,
    INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_OPERATION_GRPC_REFRESH, INTERNODE_OPERATION_GRPC_UNLOCK,
    INTERNODE_OPERATION_GRPC_UNLOCK_BATCH, INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC,
    global_internode_metrics,
};
use rustfs_object_data_cache::{MemoryBasis, resolve_effective_memory};
use rustfs_utils::crypto::{HmacSha256, KeyInit, Mac};
use rustfs_utils::get_env_bool;
use sha2::Digest as _;
use sha2::Sha256;
use std::collections::{BTreeMap, hash_map::RandomState};
use std::sync::{LazyLock, Mutex, Once};
use std::thread;
#[cfg(test)]
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tracing::{error, info, warn};
use uuid::Uuid;

pub(crate) const SIGNATURE_HEADER: &str = "x-rustfs-signature";
pub const RPC_BACKGROUND_ADMISSION_HEADER: &str = "x-rustfs-rpc-background-admission";
const RPC_BACKGROUND_ADMISSION_V1: &str = "v1";
const RPC_BACKGROUND_ADMISSION_DOMAIN: &[u8] = b"rustfs-rpc-background-admission-v1\0";
pub(crate) const TIMESTAMP_HEADER: &str = "x-rustfs-timestamp";
pub(crate) const RPC_AUTH_VERSION_HEADER: &str = "x-rustfs-rpc-auth-version";
const RPC_SIGNATURE_V2_HEADER: &str = "x-rustfs-rpc-signature-v2";
const RPC_NONCE_HEADER: &str = "x-rustfs-rpc-nonce";
pub(crate) const RPC_CONTENT_SHA256_HEADER: &str = "x-rustfs-content-sha256";
pub(crate) const RPC_AUTH_VERSION_V2: &str = "2";
pub const RPC_REPLAY_SCOPE_VERSION_HEADER: &str = "x-rustfs-rpc-replay-scope-version";
pub const RPC_REPLAY_SCOPE_SIGNATURE_HEADER: &str = "x-rustfs-rpc-signature-v3";
pub const RPC_REPLAY_SCOPE_NONCE_HEADER: &str = "x-rustfs-rpc-replay-nonce";
pub const RPC_BOOT_EPOCH_HEADER: &str = "x-rustfs-rpc-boot-epoch";
pub const RPC_BOOT_EPOCH_CHALLENGE_HEADER: &str = "x-rustfs-rpc-boot-epoch-challenge";
pub const RPC_BOOT_EPOCH_PROOF_HEADER: &str = "x-rustfs-rpc-boot-epoch-proof";
pub(crate) const RPC_REPLAY_CACHE_CAPABILITY_HEADER: &str = "x-rustfs-rpc-replay-cache-capability";
pub(crate) const RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER: &str = "x-rustfs-rpc-replay-cache-capability-proof";
const RPC_REPLAY_SCOPE_VERSION_V3: &str = "3";
const RPC_RESPONSE_PROOF_DOMAIN: &[u8] = b"rustfs-rpc-response-proof-v1\0";

tokio::task_local! {
    static RPC_BACKGROUND_REQUEST: bool;
}

pub(crate) fn current_rpc_background_request() -> bool {
    RPC_BACKGROUND_REQUEST
        .try_with(|background| *background)
        .unwrap_or_else(|_| crate::disk::has_resource_io_scope())
}

/// Preserve the authenticated wire class while polling locally admitted work.
/// This scope carries only the class, never node, disk, or memory ownership.
pub async fn with_rpc_background_request<F: std::future::Future>(background: bool, operation: F) -> F::Output {
    RPC_BACKGROUND_REQUEST.scope(background, operation).await
}

/// Parse the request class only; callers must verify the signature before using it.
pub fn rpc_background_admission(headers: &HeaderMap) -> std::io::Result<bool> {
    let mut values = headers.get_all(RPC_BACKGROUND_ADMISSION_HEADER).iter();
    match (values.next(), values.next()) {
        (None, None) => Ok(false),
        (Some(value), None) if value == RPC_BACKGROUND_ADMISSION_V1 => Ok(true),
        _ => Err(std::io::Error::other("Invalid RPC background admission version")),
    }
}

fn bind_background_admission(mac: &mut HmacSha256, background: bool) {
    if background {
        // Every auth lane binds the class, so stripping the marker or falling
        // back to an older signature cannot turn admitted work into foreground I/O.
        mac.update(RPC_BACKGROUND_ADMISSION_DOMAIN);
    }
}

fn insert_background_admission(headers: &mut HeaderMap, background: bool) {
    if background {
        headers.insert(RPC_BACKGROUND_ADMISSION_HEADER, HeaderValue::from_static(RPC_BACKGROUND_ADMISSION_V1));
    }
}
const RPC_REPLAY_SCOPE_DOMAIN: &[u8] = b"rustfs-rpc-replay-scope-v3\0";
const RPC_BOOT_EPOCH_PROOF_DOMAIN: &[u8] = b"rustfs-rpc-boot-epoch-proof-v1\0";
const RPC_REPLAY_CACHE_CAPABILITY_PROOF_DOMAIN: &[u8] = b"rustfs-rpc-replay-cache-capability-proof-v1\0";
const RPC_REPLAY_CACHE_CAPABILITY_V1: &str = "dynamic-replay-cache-v1";
const HTTP_PUT_FILE_AUTH_DOMAIN: &[u8] = b"rustfs-http-put-file-auth-v1\0";
const HTTP_PUT_FILE_CAPABILITY_AUTH_DOMAIN: &[u8] = b"rustfs-http-put-file-capability-v1\0";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const UNSIGNED_PAYLOAD_NONCE: &str = "unsigned";
const SIGNATURE_VALID_DURATION: i64 = 300; // 5 minutes
// Keep old monotonic test fixtures independent of signed-time expiration.
#[cfg(test)]
const REPLAY_CACHE_RETENTION: Duration = Duration::from_secs(301);
// Preserve capacity for the full validity window at maximum accepted clock skew.
const REPLAY_CACHE_CAPACITY_WINDOW_SECS: usize = 601;
// Policy budget, not a throughput estimate. Account growth before allocating.
const REPLAY_CACHE_AUTO_MEMORY_PERCENT: u64 = 13;
const REPLAY_CACHE_METADATA_BYTES: u64 = 1024 * 1024;
const REPLAY_CACHE_TABLE_OVERHEAD_BYTES: u64 = 512;
const NS_SCANNER_CAPABILITY_AUTH_DOMAIN: &[u8] = b"rustfs-ns-scanner-capability-v3";
const NS_SCANNER_CHECKPOINT_COVERAGE_AUTH_DOMAIN: &[u8] = b"rustfs-ns-scanner-checkpoint-coverage-v1";
const NS_SCANNER_TIER_REGISTRY_GENERATION_AUTH_DOMAIN: &[u8] = b"rustfs-ns-scanner-tier-registry-generation-v1";
pub const TONIC_RPC_PREFIX: &str = "/node_service.NodeService";
static INTERNODE_RPC_SIGNATURE_STRICT: LazyLock<bool> = LazyLock::new(|| {
    get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_SIGNATURE_STRICT,
        rustfs_config::DEFAULT_INTERNODE_RPC_SIGNATURE_STRICT,
    )
});
static INTERNODE_RPC_BODY_DIGEST_STRICT: LazyLock<bool> = LazyLock::new(|| {
    get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_BODY_DIGEST_STRICT,
        rustfs_config::DEFAULT_INTERNODE_RPC_BODY_DIGEST_STRICT,
    )
});

pub(crate) fn internode_rpc_body_digest_strict() -> bool {
    *INTERNODE_RPC_BODY_DIGEST_STRICT
}
static INTERNODE_RPC_REPLAY_SCOPE_STRICT: LazyLock<bool> = LazyLock::new(|| {
    get_env_bool(
        rustfs_config::ENV_INTERNODE_RPC_REPLAY_SCOPE_STRICT,
        rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_SCOPE_STRICT,
    )
});
// Automatic admission budgets physical table storage and growth peaks. An
// explicit operator count retains its compatibility floor and bypasses that policy.
static REPLAY_CACHE_CAPACITY: LazyLock<ReplayCacheCapacityDecision> = LazyLock::new(resolve_replay_cache_capacity);
static RPC_SECRET_RESOLUTION_LOG_ONCE: Once = Once::new();
static RPC_BOOT_EPOCH: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayCacheCapacitySource {
    Env,
    EnvClampedToDefault,
    Auto,
    AutoInvalidEnv,
}

impl ReplayCacheCapacitySource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::EnvClampedToDefault => "env_clamped_to_default",
            Self::Auto => "auto",
            Self::AutoInvalidEnv => "auto_invalid_env",
        }
    }

    fn is_env_clamped(self) -> bool {
        matches!(self, Self::EnvClampedToDefault)
    }

    fn is_env(self) -> bool {
        matches!(self, Self::Env)
    }

    fn is_invalid_env(self) -> bool {
        matches!(self, Self::AutoInvalidEnv)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayCacheCapacityDecision {
    capacity: usize,
    source: ReplayCacheCapacitySource,
    cpu_count: usize,
    memory_limit_bytes: Option<u64>,
    memory_basis: Option<MemoryBasis>,
    memory_based_capacity: usize,
    memory_budget_bytes: Option<u64>,
}

fn replay_cache_memory_budget(memory: u64) -> u64 {
    // Divide before multiplying so even an extreme reported limit cannot wrap.
    memory / 100 * REPLAY_CACHE_AUTO_MEMORY_PERCENT + memory % 100 * REPLAY_CACHE_AUTO_MEMORY_PERCENT / 100
}

fn replay_cache_capacity_decision(
    env: rustfs_utils::EnvParseOutcome<usize>,
    cpu_count: usize,
    memory_limit_bytes: Option<u64>,
    memory_basis: Option<MemoryBasis>,
) -> ReplayCacheCapacityDecision {
    let memory_budget_bytes = memory_limit_bytes.map(replay_cache_memory_budget);
    // This is only the absolute payload ceiling. Actual layout and growth
    // admission may reject earlier, depending on the signed-time distribution.
    let memory_based_capacity = memory_budget_bytes
        .map(|bytes| usize::try_from(bytes.saturating_sub(REPLAY_CACHE_METADATA_BYTES) / 16).unwrap_or(usize::MAX))
        .unwrap_or(rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY);
    let (capacity, source, memory_budget_bytes) = match env {
        rustfs_utils::EnvParseOutcome::Parsed(configured) => (
            configured.max(rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY),
            if configured < rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY {
                ReplayCacheCapacitySource::EnvClampedToDefault
            } else {
                ReplayCacheCapacitySource::Env
            },
            None,
        ),
        rustfs_utils::EnvParseOutcome::Absent => (memory_based_capacity, ReplayCacheCapacitySource::Auto, memory_budget_bytes),
        rustfs_utils::EnvParseOutcome::Invalid => {
            (memory_based_capacity, ReplayCacheCapacitySource::AutoInvalidEnv, memory_budget_bytes)
        }
    };
    ReplayCacheCapacityDecision {
        capacity,
        source,
        cpu_count: cpu_count.max(1),
        memory_limit_bytes,
        memory_basis,
        memory_based_capacity,
        memory_budget_bytes,
    }
}

// hashbrown rounds 7/8 occupancy to a power of two. Include conservative
// control-byte alignment before charging the production allocator allowance.
fn replay_cache_table_growth_bytes(entries: usize) -> Option<u64> {
    let entries = u64::try_from(entries).ok()?;
    let buckets = entries.checked_mul(8)?.div_ceil(7).max(4).checked_next_power_of_two()?;
    replay_cache_table_charge(buckets.checked_mul(17)?.checked_add(64)?)
}

fn replay_cache_table_charge(bytes: u64) -> Option<u64> {
    if bytes == 0 {
        return Some(0);
    }
    bytes
        .checked_add(bytes.div_ceil(4))?
        .checked_add(REPLAY_CACHE_TABLE_OVERHEAD_BYTES)
}

fn detected_replay_cache_resources() -> (usize, Option<u64>, Option<MemoryBasis>) {
    let cpu_count = thread::available_parallelism().map(usize::from).unwrap_or(1).max(1);
    let memory = resolve_effective_memory();
    let memory_limit_bytes = (memory.total_bytes > 0).then_some(memory.total_bytes);
    (cpu_count, memory_limit_bytes, Some(memory.basis))
}

fn log_replay_cache_capacity_decision(decision: ReplayCacheCapacityDecision) {
    let source = decision.source.as_str();
    if decision.source.is_env_clamped() {
        warn!(
            event = "internode_rpc_replay_cache_capacity_resolved",
            component = "ecstore",
            subsystem = "rpc_auth",
            capacity = decision.capacity,
            source,
            default_capacity = rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            env = rustfs_config::ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            "internode rpc replay cache capacity clamped to default"
        );
        return;
    }
    if decision.source.is_env() {
        info!(
            event = "internode_rpc_replay_cache_capacity_resolved",
            component = "ecstore",
            subsystem = "rpc_auth",
            capacity = decision.capacity,
            source,
            default_capacity = rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            env = rustfs_config::ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            "internode rpc replay cache capacity resolved from env"
        );
        return;
    }
    if decision.source.is_invalid_env() {
        warn!(
            event = "internode_rpc_replay_cache_capacity_resolved",
            component = "ecstore",
            subsystem = "rpc_auth",
            capacity = decision.capacity,
            source,
            cpu_count = decision.cpu_count,
            memory_limit_bytes = decision.memory_limit_bytes,
            memory_basis = decision.memory_basis.map(MemoryBasis::as_str),
            memory_based_capacity = decision.memory_based_capacity,
            memory_budget_bytes = decision.memory_budget_bytes,
            env = rustfs_config::ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            "internode rpc replay cache capacity auto-sized after invalid env"
        );
        return;
    }
    info!(
        event = "internode_rpc_replay_cache_capacity_resolved",
        component = "ecstore",
        subsystem = "rpc_auth",
        capacity = decision.capacity,
        source,
        cpu_count = decision.cpu_count,
        memory_limit_bytes = decision.memory_limit_bytes,
        memory_basis = decision.memory_basis.map(MemoryBasis::as_str),
        memory_based_capacity = decision.memory_based_capacity,
        memory_budget_bytes = decision.memory_budget_bytes,
        "internode rpc replay cache capacity resolved"
    );
}

fn resolve_replay_cache_capacity() -> ReplayCacheCapacityDecision {
    let (cpu_count, memory_limit_bytes, memory_basis) = detected_replay_cache_resources();
    let decision = replay_cache_capacity_decision(
        rustfs_utils::get_env_parse_outcome(rustfs_config::ENV_INTERNODE_RPC_REPLAY_CACHE_CAPACITY),
        cpu_count,
        memory_limit_bytes,
        memory_basis,
    );
    global_internode_metrics().record_replay_cache_state(0, decision.capacity);
    log_replay_cache_capacity_decision(decision);
    decision
}

/// Resource limits reached after request authentication; no request was admitted.
#[derive(Debug, thiserror::Error)]
pub enum RpcReplayCacheAdmissionError {
    /// The configured count ceiling has been reached.
    #[error("RPC replay cache capacity exceeded")]
    Capacity,
    /// The next allocation would exceed the automatic byte budget.
    #[error("RPC replay cache memory budget exceeded")]
    MemoryBudget,
}

/// Whether an RPC verifier failed because replay admission exhausted resources.
///
/// Invalid credentials, replayed requests and arbitrary I/O failures remain
/// authentication failures. Allocation errors preserve their concrete source.
pub fn rpc_auth_is_resource_exhausted(error: &std::io::Error) -> bool {
    error.get_ref().is_some_and(|source| {
        source.is::<RpcReplayCacheAdmissionError>()
            || (error.kind() == std::io::ErrorKind::OutOfMemory && source.is::<hashbrown::TryReserveError>())
    })
}

// Timestamp is HMAC-bound together with the nonce in every supported replay
// scope. Re-signing the same UUID at another time requires the signing secret;
// it is a new authorization, not replay of the old wire request.
#[derive(Default)]
struct RpcNonceBucket {
    nonces: HashSet<Uuid, RandomState>,
    charged_bytes: u64,
}

struct RpcNonceCache {
    signed_times: BTreeMap<i64, RpcNonceBucket>,
    entries: usize,
    table_bytes: u64,
    max_wall_time: i64,
}

impl Default for RpcNonceCache {
    fn default() -> Self {
        Self {
            signed_times: BTreeMap::new(),
            entries: 0,
            table_bytes: 0,
            max_wall_time: i64::MIN,
        }
    }
}

#[derive(Clone, Copy)]
struct RpcReplayCacheMetricScope<'a> {
    operation: &'static str,
    backend: &'static str,
    rpc_path: &'a str,
}

#[derive(Clone, Copy)]
struct RpcNonceRecord<'a> {
    nonce: Uuid,
    compatibility_nonce: Option<Uuid>,
    signed_at: i64,
    wall_time: i64,
    capacity: usize,
    memory_budget_bytes: Option<u64>,
    metric_scope: RpcReplayCacheMetricScope<'a>,
}

struct RpcNonceCacheMetrics<'a> {
    expired: usize,
    recorded: usize,
    entries: usize,
    capacity: usize,
    record_scope: Option<RpcReplayCacheMetricScope<'a>>,
    overflow_scope: Option<RpcReplayCacheMetricScope<'a>>,
}

fn publish_nonce_cache_metrics(metrics: Option<RpcNonceCacheMetrics<'_>>) {
    let Some(metrics) = metrics else {
        return;
    };
    let internode_metrics = global_internode_metrics();
    internode_metrics.record_replay_cache_evictions("expired", metrics.expired);
    internode_metrics.record_replay_cache_state(metrics.entries, metrics.capacity);
    if let Some(scope) = metrics.record_scope {
        for _ in 0..metrics.recorded {
            internode_metrics.record_replay_cache_record_for_operation_and_backend_path(
                scope.operation,
                scope.backend,
                scope.rpc_path,
            );
        }
    }
    if let Some(scope) = metrics.overflow_scope {
        internode_metrics.record_replay_cache_overflow_for_operation_and_backend_path(
            scope.operation,
            scope.backend,
            scope.rpc_path,
        );
    }
}

impl RpcNonceCache {
    #[cfg(test)]
    fn contains_nonce(&self, nonce: &Uuid) -> bool {
        self.signed_times.values().any(|bucket| bucket.nonces.contains(nonce))
    }

    fn remove_expired(&mut self, wall_time: i64) -> usize {
        self.max_wall_time = self.max_wall_time.max(wall_time);
        let first_valid = self.max_wall_time.saturating_sub(SIGNATURE_VALID_DURATION);
        let mut removed = 0;
        while self
            .signed_times
            .first_key_value()
            .is_some_and(|(signed, _)| *signed < first_valid)
        {
            if let Some((_, nonces)) = self.signed_times.pop_first() {
                removed += nonces.nonces.len();
                self.table_bytes -= nonces.charged_bytes;
            }
        }
        self.entries -= removed;
        removed
    }

    fn reserve_nonces(&mut self, record: &RpcNonceRecord<'_>) -> std::io::Result<()> {
        let bucket = self.signed_times.get(&record.signed_at);
        let (len, capacity) = bucket.map_or((0, 0), |bucket| (bucket.nonces.len(), bucket.nonces.capacity()));
        let additional = 1 + usize::from(record.compatibility_nonce.is_some());
        let needed = len
            .checked_add(additional)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::OutOfMemory, hashbrown::TryReserveError::CapacityOverflow))?;
        if needed > capacity {
            let growth = replay_cache_table_growth_bytes(needed).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::OutOfMemory, hashbrown::TryReserveError::CapacityOverflow)
            })?;
            // The old table stays live until growth completes. Do not subtract
            // it here, even though it will be replaced immediately afterwards.
            let peak = self
                .table_bytes
                .checked_add(growth)
                .and_then(|bytes| bytes.checked_add(REPLAY_CACHE_METADATA_BYTES))
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::OutOfMemory, hashbrown::TryReserveError::CapacityOverflow)
                })?;
            if record.memory_budget_bytes.is_some_and(|limit| peak > limit) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    RpcReplayCacheAdmissionError::MemoryBudget,
                ));
            }
        }
        let bucket = self.signed_times.entry(record.signed_at).or_default();
        bucket
            .nonces
            .try_reserve(additional)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        let after = u64::try_from(bucket.nonces.allocation_size())
            .ok()
            .and_then(replay_cache_table_charge)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::OutOfMemory, hashbrown::TryReserveError::CapacityOverflow))?;
        self.table_bytes = self.table_bytes - bucket.charged_bytes + after;
        bucket.charged_bytes = after;
        bucket.nonces.insert(record.nonce);
        if let Some(nonce) = record.compatibility_nonce {
            bucket.nonces.insert(nonce);
        }
        self.entries += additional;
        debug_assert!(self.signed_times.len() <= REPLAY_CACHE_CAPACITY_WINDOW_SECS);
        Ok(())
    }

    fn check_and_record<'a>(&mut self, record: RpcNonceRecord<'a>) -> (std::io::Result<()>, Option<RpcNonceCacheMetrics<'a>>) {
        self.max_wall_time = self.max_wall_time.max(record.wall_time);
        if self.max_wall_time.saturating_sub(record.signed_at) > SIGNATURE_VALID_DURATION {
            return (Err(std::io::Error::other("RPC request timestamp expired after clock regression")), None);
        }
        if record.signed_at.saturating_sub(self.max_wall_time) > SIGNATURE_VALID_DURATION {
            return (Err(std::io::Error::other("Request timestamp expired")), None);
        }
        let expired = self.remove_expired(self.max_wall_time);
        let metrics = RpcNonceCacheMetrics {
            expired,
            recorded: 0,
            entries: self.entries,
            capacity: record.capacity,
            record_scope: None,
            overflow_scope: None,
        };
        if record.compatibility_nonce == Some(record.nonce)
            || self.signed_times.get(&record.signed_at).is_some_and(|bucket| {
                bucket.nonces.contains(&record.nonce)
                    || record.compatibility_nonce.is_some_and(|nonce| bucket.nonces.contains(&nonce))
            })
        {
            return (Err(std::io::Error::other("RPC request replay detected")), Some(metrics));
        }
        let additional = 1 + usize::from(record.compatibility_nonce.is_some());
        if self
            .entries
            .checked_add(additional)
            .is_none_or(|entries| entries > record.capacity)
        {
            return (
                Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    RpcReplayCacheAdmissionError::Capacity,
                )),
                Some(RpcNonceCacheMetrics {
                    overflow_scope: Some(record.metric_scope),
                    ..metrics
                }),
            );
        }
        if let Err(error) = self.reserve_nonces(&record) {
            let overflow_scope = (error.kind() == std::io::ErrorKind::WouldBlock).then_some(record.metric_scope);
            return (
                Err(error),
                Some(RpcNonceCacheMetrics {
                    overflow_scope,
                    ..metrics
                }),
            );
        }
        (
            Ok(()),
            Some(RpcNonceCacheMetrics {
                entries: self.entries,
                recorded: additional,
                record_scope: Some(record.metric_scope),
                ..metrics
            }),
        )
    }
}

// This cache is a process-local wire-replay defense only. Mutation handlers
// still need a stable operation ID and coordinator-owned idempotency across
// retries, node failover, and restart.
static LOCAL_RPC_NONCE_CACHE: LazyLock<Mutex<RpcNonceCache>> = LazyLock::new(|| Mutex::new(RpcNonceCache::default()));

/// Get the shared secret for HMAC signing
#[cfg(test)]
fn resolve_shared_secret(env_secret: Option<&str>, global_secret: Option<&str>) -> std::io::Result<String> {
    if let Some(secret) = env_secret.map(str::trim).filter(|secret| !secret.is_empty()) {
        return (secret != DEFAULT_SECRET_KEY)
            .then(|| secret.to_string())
            .ok_or_else(|| std::io::Error::other(RPC_SECRET_REQUIRED_MESSAGE));
    }

    global_secret
        .map(str::trim)
        .filter(|secret| !secret.is_empty() && *secret != DEFAULT_SECRET_KEY)
        .map(ToOwned::to_owned)
        .ok_or_else(|| std::io::Error::other(RPC_SECRET_REQUIRED_MESSAGE))
}

fn get_shared_secret() -> std::io::Result<String> {
    try_get_rpc_token().map_err(|err| {
        RPC_SECRET_RESOLUTION_LOG_ONCE.call_once(|| {
            error!("RPC auth secret resolution failed: {}; {}", err, RPC_SECRET_REQUIRED_OPERATOR_MESSAGE);
        });
        err
    })
}

fn rpc_response_proof_mac(canonical_body: &[u8]) -> std::io::Result<HmacSha256> {
    let secret = get_shared_secret()?;
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    mac.update(RPC_RESPONSE_PROOF_DOMAIN);
    mac.update(
        &u64::try_from(canonical_body.len())
            .map_err(|_| std::io::Error::other("RPC response proof length cannot be represented"))?
            .to_be_bytes(),
    );
    mac.update(canonical_body);
    Ok(mac)
}

pub fn sign_tonic_rpc_response_proof(canonical_body: &[u8]) -> std::io::Result<Vec<u8>> {
    Ok(rpc_response_proof_mac(canonical_body)?.finalize().into_bytes().to_vec())
}

pub fn verify_tonic_rpc_response_proof(canonical_body: &[u8], proof: &[u8]) -> std::io::Result<()> {
    rpc_response_proof_mac(canonical_body)?
        .verify_slice(proof)
        .map_err(|_| std::io::Error::other("Invalid RPC response proof"))
}

/// Build the canonical payload covered by the RPC HMAC.
fn signature_payload(url: &str, method: &Method, timestamp: i64) -> String {
    let uri: Uri = url.parse().expect("Invalid URL");

    let path_and_query = uri.path_and_query().unwrap();

    let url = path_and_query.to_string();

    format!("{url}|{method}|{timestamp}")
}

fn canonical_path_and_query(url: &str) -> std::io::Result<String> {
    let uri: Uri = url.parse().map_err(|_| std::io::Error::other("Invalid RPC URL"))?;
    uri.path_and_query()
        .map(ToString::to_string)
        .ok_or_else(|| std::io::Error::other("Invalid RPC URL"))
}

fn redacted_rpc_path(url: &str) -> String {
    url.parse::<Uri>()
        .ok()
        .map(|uri| uri.path().to_string())
        .unwrap_or_else(|| "<invalid-rpc-url>".to_string())
}

/// Generate HMAC-SHA256 signature for the given data
fn generate_signature(secret: &str, url: &str, method: &Method, timestamp: i64, background: bool) -> String {
    let data = signature_payload(url, method, timestamp);
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    bind_background_admission(&mut mac, background);
    mac.update(data.as_bytes());
    let result = mac.finalize();
    base64_simd::STANDARD.encode_to_string(result.into_bytes())
}

fn verify_signature(secret: &str, url: &str, method: &Method, timestamp: i64, signature: &str, background: bool) -> bool {
    let Ok(signature) = base64_simd::STANDARD.decode_to_vec(signature) else {
        return false;
    };

    let data = signature_payload(url, method, timestamp);
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    bind_background_admission(&mut mac, background);
    mac.update(data.as_bytes());
    mac.verify_slice(&signature).is_ok()
}

fn update_put_file_auth_mac(
    mac: &mut HmacSha256,
    url: &str,
    method: &Method,
    nonce: Uuid,
    body_sha256: &str,
) -> std::io::Result<()> {
    if !valid_content_sha256(body_sha256) || body_sha256 == UNSIGNED_PAYLOAD {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }
    let path_and_query = canonical_path_and_query(url)?;
    mac.update(HTTP_PUT_FILE_AUTH_DOMAIN);
    for part in [
        path_and_query.as_bytes(),
        b"|",
        method.as_str().as_bytes(),
        b"|",
        nonce.as_bytes(),
        b"|",
        body_sha256.as_bytes(),
    ] {
        mac.update(part);
    }
    Ok(())
}

fn put_file_auth_mac(url: &str, method: &Method, nonce: Uuid, body_sha256: &str) -> std::io::Result<[u8; 32]> {
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(get_shared_secret()?.as_bytes())
        .map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_put_file_auth_mac(&mut mac, url, method, nonce, body_sha256)?;
    Ok(mac.finalize().into_bytes().into())
}

fn verify_put_file_auth_mac(url: &str, method: &Method, nonce: Uuid, body_sha256: &str, signature: &[u8]) -> std::io::Result<()> {
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(get_shared_secret()?.as_bytes())
        .map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_put_file_auth_mac(&mut mac, url, method, nonce, body_sha256)?;
    mac.verify_slice(signature)
        .map_err(|_| std::io::Error::other("Invalid put_file auth trailer"))
}

pub fn build_put_file_auth_trailer(url: &str, method: &Method, nonce: Uuid, body_sha256: &str) -> std::io::Result<Vec<u8>> {
    let mac = put_file_auth_mac(url, method, nonce, body_sha256)?;
    let mut trailer = Vec::with_capacity(PUT_FILE_AUTH_TRAILER_LEN);
    trailer.extend_from_slice(PUT_FILE_AUTH_TRAILER_MAGIC);
    trailer.extend_from_slice(body_sha256.as_bytes());
    trailer.extend_from_slice(&mac);
    Ok(trailer)
}

pub fn verify_put_file_auth_trailer(url: &str, method: &Method, nonce: Uuid, trailer: &[u8]) -> std::io::Result<String> {
    if trailer.len() != PUT_FILE_AUTH_TRAILER_LEN {
        return Err(std::io::Error::other("Invalid put_file auth trailer length"));
    }
    if &trailer[..PUT_FILE_AUTH_TRAILER_MAGIC.len()] != PUT_FILE_AUTH_TRAILER_MAGIC {
        return Err(std::io::Error::other("Invalid put_file auth trailer"));
    }
    let digest_start = PUT_FILE_AUTH_TRAILER_MAGIC.len();
    let digest_end = digest_start + PUT_FILE_AUTH_TRAILER_DIGEST_LEN;
    let body_sha256 = std::str::from_utf8(&trailer[digest_start..digest_end])
        .map_err(|_| std::io::Error::other("Invalid RPC content SHA-256"))?;
    if !valid_content_sha256(body_sha256) || body_sha256 == UNSIGNED_PAYLOAD {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }
    let mac_start = digest_end;
    let mac_end = mac_start + PUT_FILE_AUTH_TRAILER_MAC_LEN;
    verify_put_file_auth_mac(url, method, nonce, body_sha256, &trailer[mac_start..mac_end])?;
    Ok(body_sha256.to_string())
}

fn update_put_file_capability_mac(mac: &mut HmacSha256, challenge: Uuid, server_epoch: Uuid, version: u16) {
    mac.update(HTTP_PUT_FILE_CAPABILITY_AUTH_DOMAIN);
    mac.update(challenge.as_bytes());
    mac.update(server_epoch.as_bytes());
    mac.update(&version.to_be_bytes());
}

fn put_file_capability_mac(challenge: Uuid, server_epoch: Uuid, version: u16) -> std::io::Result<HmacSha256> {
    if challenge.is_nil() || server_epoch.is_nil() || version != PUT_FILE_CAPABILITY_VERSION {
        return Err(std::io::Error::other("Invalid put_file capability scope"));
    }
    let mut mac = HmacSha256::new_from_slice(get_shared_secret()?.as_bytes())
        .map_err(|_| std::io::Error::other("Invalid RPC HMAC secret"))?;
    update_put_file_capability_mac(&mut mac, challenge, server_epoch, version);
    Ok(mac)
}

pub fn sign_put_file_capability(challenge: Uuid, server_epoch: Uuid, version: u16) -> std::io::Result<Vec<u8>> {
    Ok(put_file_capability_mac(challenge, server_epoch, version)?
        .finalize()
        .into_bytes()
        .to_vec())
}

pub fn verify_put_file_capability(challenge: Uuid, server_epoch: Uuid, version: u16, proof: &[u8]) -> std::io::Result<()> {
    put_file_capability_mac(challenge, server_epoch, version)?
        .verify_slice(proof)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Invalid put_file capability proof"))
}

fn update_ns_scanner_capability_mac(
    mac: &mut HmacSha256,
    challenge: Uuid,
    server_epoch: Uuid,
    supports_tier_registry_generation: bool,
    supports_checkpoint_coverage: bool,
) {
    mac.update(NS_SCANNER_CAPABILITY_AUTH_DOMAIN);
    mac.update(&NS_SCANNER_PROTOCOL_VERSION.to_be_bytes());
    mac.update(challenge.as_bytes());
    mac.update(server_epoch.as_bytes());
    if supports_tier_registry_generation {
        // The optional response capability is part of the authenticated
        // scope. A proxy cannot turn an old/unsupported peer into a worker
        // that receives generation-fenced scanner work.
        mac.update(NS_SCANNER_TIER_REGISTRY_GENERATION_AUTH_DOMAIN);
    }
    if supports_checkpoint_coverage {
        mac.update(NS_SCANNER_CHECKPOINT_COVERAGE_AUTH_DOMAIN);
    }
}

fn generate_ns_scanner_capability_proof(
    secret: &str,
    challenge: Uuid,
    server_epoch: Uuid,
    supports_tier_registry_generation: bool,
    supports_checkpoint_coverage: bool,
) -> std::io::Result<Vec<u8>> {
    if challenge.is_nil() || server_epoch.is_nil() {
        return Err(std::io::Error::other("Invalid namespace scanner capability scope"));
    }
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_ns_scanner_capability_mac(
        &mut mac,
        challenge,
        server_epoch,
        supports_tier_registry_generation,
        supports_checkpoint_coverage,
    );
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_ns_scanner_capability_proof(
    secret: &str,
    challenge: Uuid,
    server_epoch: Uuid,
    proof: &[u8],
    supports_tier_registry_generation: bool,
    supports_checkpoint_coverage: bool,
) -> std::io::Result<()> {
    if challenge.is_nil() || server_epoch.is_nil() {
        return Err(std::io::Error::other("Invalid namespace scanner capability scope"));
    }
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_ns_scanner_capability_mac(
        &mut mac,
        challenge,
        server_epoch,
        supports_tier_registry_generation,
        supports_checkpoint_coverage,
    );
    mac.verify_slice(proof)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Invalid namespace scanner capability proof"))
}

pub fn sign_ns_scanner_capability(challenge: Uuid, server_epoch: Uuid) -> std::io::Result<Vec<u8>> {
    sign_ns_scanner_capability_with_tier_registry_generation(challenge, server_epoch, false)
}

pub fn verify_ns_scanner_capability(challenge: Uuid, server_epoch: Uuid, proof: &[u8]) -> std::io::Result<()> {
    verify_ns_scanner_capability_with_tier_registry_generation(challenge, server_epoch, proof, false)
}

pub fn sign_ns_scanner_capability_with_tier_registry_generation(
    challenge: Uuid,
    server_epoch: Uuid,
    supports_tier_registry_generation: bool,
) -> std::io::Result<Vec<u8>> {
    sign_ns_scanner_capability_with_checkpoint_coverage(challenge, server_epoch, supports_tier_registry_generation, false)
}

pub fn sign_ns_scanner_capability_with_checkpoint_coverage(
    challenge: Uuid,
    server_epoch: Uuid,
    supports_tier_registry_generation: bool,
    supports_checkpoint_coverage: bool,
) -> std::io::Result<Vec<u8>> {
    generate_ns_scanner_capability_proof(
        &get_shared_secret()?,
        challenge,
        server_epoch,
        supports_tier_registry_generation,
        supports_checkpoint_coverage,
    )
}

pub fn verify_ns_scanner_capability_with_tier_registry_generation(
    challenge: Uuid,
    server_epoch: Uuid,
    proof: &[u8],
    supports_tier_registry_generation: bool,
) -> std::io::Result<()> {
    verify_ns_scanner_capability_with_checkpoint_coverage(
        challenge,
        server_epoch,
        proof,
        supports_tier_registry_generation,
        false,
    )
}

pub fn verify_ns_scanner_capability_with_checkpoint_coverage(
    challenge: Uuid,
    server_epoch: Uuid,
    proof: &[u8],
    supports_tier_registry_generation: bool,
    supports_checkpoint_coverage: bool,
) -> std::io::Result<()> {
    verify_ns_scanner_capability_proof(
        &get_shared_secret()?,
        challenge,
        server_epoch,
        proof,
        supports_tier_registry_generation,
        supports_checkpoint_coverage,
    )
}

#[derive(Clone, Copy)]
struct SignatureV2Scope<'a> {
    background: bool,
    audience: &'a str,
    service: &'a str,
    rpc_method: &'a str,
    timestamp: &'a str,
    nonce: &'a str,
    content_sha256: &'a str,
}

fn update_signature_v2(mac: &mut HmacSha256, scope: SignatureV2Scope<'_>) {
    bind_background_admission(mac, scope.background);
    for part in [
        b"rustfs-rpc-auth-v2|".as_slice(),
        scope.audience.as_bytes(),
        b"|/",
        scope.service.as_bytes(),
        b"/",
        scope.rpc_method.as_bytes(),
        b"|POST|",
        scope.timestamp.as_bytes(),
        b"|",
        scope.nonce.as_bytes(),
        b"|",
        scope.content_sha256.as_bytes(),
    ] {
        mac.update(part);
    }
}

fn generate_signature_v2(secret: &str, scope: SignatureV2Scope<'_>) -> std::io::Result<String> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_signature_v2(&mut mac, scope);
    Ok(base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes()))
}

fn verify_signature_v2(secret: &str, scope: SignatureV2Scope<'_>, signature: &str) -> bool {
    let Ok(signature) = base64_simd::STANDARD.decode_to_vec(signature) else {
        return false;
    };
    let Ok(mut mac) = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    update_signature_v2(&mut mac, scope);
    mac.verify_slice(&signature).is_ok()
}

#[derive(Clone, Copy)]
struct ReplayScope<'a> {
    background: bool,
    audience: &'a str,
    path: &'a str,
    timestamp: &'a str,
    nonce: Uuid,
    content_sha256: &'a str,
    boot_epoch: Uuid,
}

fn update_replay_scope(mac: &mut HmacSha256, scope: ReplayScope<'_>) {
    bind_background_admission(mac, scope.background);
    mac.update(RPC_REPLAY_SCOPE_DOMAIN);
    for part in [
        scope.audience.as_bytes(),
        b"|",
        scope.path.as_bytes(),
        b"|POST|",
        scope.timestamp.as_bytes(),
        b"|",
        scope.nonce.as_bytes(),
        b"|",
        scope.content_sha256.as_bytes(),
        b"|",
        scope.boot_epoch.as_bytes(),
    ] {
        mac.update(part);
    }
}

fn generate_replay_scope_signature(secret: &str, scope: ReplayScope<'_>) -> std::io::Result<String> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_replay_scope(&mut mac, scope);
    Ok(base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes()))
}

fn verify_replay_scope_signature(secret: &str, scope: ReplayScope<'_>, signature: &str) -> bool {
    let Ok(signature) = base64_simd::STANDARD.decode_to_vec(signature) else {
        return false;
    };
    let Ok(mut mac) = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    update_replay_scope(&mut mac, scope);
    mac.verify_slice(&signature).is_ok()
}

fn update_boot_epoch_proof(mac: &mut HmacSha256, audience: &str, challenge: Uuid, boot_epoch: Uuid) {
    mac.update(RPC_BOOT_EPOCH_PROOF_DOMAIN);
    mac.update(audience.as_bytes());
    mac.update(b"|");
    mac.update(challenge.as_bytes());
    mac.update(boot_epoch.as_bytes());
}

fn generate_boot_epoch_proof(secret: &str, audience: &str, challenge: Uuid, boot_epoch: Uuid) -> std::io::Result<String> {
    if audience.is_empty() || challenge.is_nil() || boot_epoch.is_nil() {
        return Err(std::io::Error::other("Invalid RPC boot epoch proof scope"));
    }
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_boot_epoch_proof(&mut mac, audience, challenge, boot_epoch);
    Ok(base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes()))
}

fn verify_boot_epoch_proof(secret: &str, audience: &str, challenge: Uuid, boot_epoch: Uuid, proof: &str) -> std::io::Result<()> {
    if audience.is_empty() || challenge.is_nil() || boot_epoch.is_nil() {
        return Err(std::io::Error::other("Invalid RPC boot epoch proof scope"));
    }
    let proof = base64_simd::STANDARD
        .decode_to_vec(proof)
        .map_err(|_| std::io::Error::other("Invalid RPC boot epoch proof"))?;
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_boot_epoch_proof(&mut mac, audience, challenge, boot_epoch);
    mac.verify_slice(&proof)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Invalid RPC boot epoch proof"))
}

fn update_replay_cache_capability_proof(mac: &mut HmacSha256, audience: &str, challenge: Uuid, boot_epoch: Uuid) {
    mac.update(RPC_REPLAY_CACHE_CAPABILITY_PROOF_DOMAIN);
    for part in [
        audience.as_bytes(),
        b"|",
        challenge.as_bytes(),
        b"|",
        boot_epoch.as_bytes(),
        b"|",
        RPC_REPLAY_CACHE_CAPABILITY_V1.as_bytes(),
    ] {
        mac.update(part);
    }
}

fn generate_replay_cache_capability_proof(
    secret: &str,
    audience: &str,
    challenge: Uuid,
    boot_epoch: Uuid,
) -> std::io::Result<String> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_replay_cache_capability_proof(&mut mac, audience, challenge, boot_epoch);
    Ok(base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes()))
}

fn verify_replay_cache_capability_proof(
    secret: &str,
    audience: &str,
    challenge: Uuid,
    boot_epoch: Uuid,
    proof: &str,
) -> std::io::Result<()> {
    let proof = base64_simd::STANDARD
        .decode_to_vec(proof)
        .map_err(|_| std::io::Error::other("Invalid RPC replay cache capability proof"))?;
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_replay_cache_capability_proof(&mut mac, audience, challenge, boot_epoch);
    mac.verify_slice(&proof)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Invalid RPC replay cache capability proof"))
}

fn non_nil_uuid(value: &str, name: &str) -> std::io::Result<Uuid> {
    let value = Uuid::parse_str(value).map_err(|_| std::io::Error::other(format!("Invalid {name}")))?;
    (!value.is_nil())
        .then_some(value)
        .ok_or_else(|| std::io::Error::other(format!("Invalid {name}")))
}

fn parse_tonic_rpc_path(path: &str) -> std::io::Result<(&str, &str)> {
    path.strip_prefix('/')
        .and_then(|path| path.split_once('/'))
        .filter(|(service, rpc_method)| !service.is_empty() && !rpc_method.is_empty() && !rpc_method.contains('/'))
        .ok_or_else(|| std::io::Error::other("Invalid RPC request path"))
}

/// The process-unique epoch included in every replay-scoped server verification.
///
/// A fresh process gets a fresh value, so a signature captured before a server restart cannot be
/// admitted even though the bounded in-memory nonce cache necessarily starts empty again.
pub fn tonic_rpc_boot_epoch() -> Uuid {
    *RPC_BOOT_EPOCH
}

/// Build the additive replay-scope headers for a request that already carries rolling-upgrade-safe
/// v1/v2 metadata. `timestamp` and `content_sha256` are deliberately reused from the v2 scope so
/// old servers can continue validating the same request unchanged.
pub fn gen_tonic_replay_scope_headers(
    audience: &str,
    path: &str,
    timestamp: &str,
    content_sha256: &str,
    boot_epoch: Uuid,
) -> std::io::Result<HeaderMap> {
    gen_tonic_replay_scope_headers_with_admission(
        audience,
        path,
        timestamp,
        content_sha256,
        boot_epoch,
        current_rpc_background_request(),
    )
}

pub(crate) fn gen_tonic_replay_scope_headers_with_admission(
    audience: &str,
    path: &str,
    timestamp: &str,
    content_sha256: &str,
    boot_epoch: Uuid,
    background: bool,
) -> std::io::Result<HeaderMap> {
    if audience.is_empty() || !path.starts_with('/') || !valid_content_sha256(content_sha256) || boot_epoch.is_nil() {
        return Err(std::io::Error::other("Invalid replay-scoped RPC signing scope"));
    }
    parse_tonic_rpc_path(path)?;
    timestamp
        .parse::<i64>()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;

    let nonce = Uuid::new_v4();
    let signature = generate_replay_scope_signature(
        &get_shared_secret()?,
        ReplayScope {
            background,
            audience,
            path,
            timestamp,
            nonce,
            content_sha256,
            boot_epoch,
        },
    )?;
    let mut headers = HeaderMap::new();
    insert_background_admission(&mut headers, background);
    headers.insert(RPC_REPLAY_SCOPE_VERSION_HEADER, HeaderValue::from_static(RPC_REPLAY_SCOPE_VERSION_V3));
    headers.insert(
        RPC_REPLAY_SCOPE_SIGNATURE_HEADER,
        header_value(&signature, RPC_REPLAY_SCOPE_SIGNATURE_HEADER)?,
    );
    headers.insert(
        RPC_REPLAY_SCOPE_NONCE_HEADER,
        header_value(&nonce.to_string(), RPC_REPLAY_SCOPE_NONCE_HEADER)?,
    );
    headers.insert(RPC_BOOT_EPOCH_HEADER, header_value(&boot_epoch.to_string(), RPC_BOOT_EPOCH_HEADER)?);
    Ok(headers)
}

/// Parse the optional client challenge used to authenticate a server boot-epoch advertisement.
pub fn tonic_boot_epoch_challenge(headers: &HeaderMap) -> std::io::Result<Option<Uuid>> {
    headers
        .get(RPC_BOOT_EPOCH_CHALLENGE_HEADER)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| std::io::Error::other("Invalid RPC boot epoch challenge"))
                .and_then(|value| non_nil_uuid(value, "RPC boot epoch challenge"))
        })
        .transpose()
}

/// Build the authenticated response headers for a client boot-epoch challenge.
pub fn tonic_boot_epoch_response_headers(audience: &str, challenge: Uuid) -> std::io::Result<HeaderMap> {
    let boot_epoch = tonic_rpc_boot_epoch();
    let secret = get_shared_secret()?;
    let proof = generate_boot_epoch_proof(&secret, audience, challenge, boot_epoch)?;
    let capability_proof = generate_replay_cache_capability_proof(&secret, audience, challenge, boot_epoch)?;
    let mut headers = HeaderMap::new();
    headers.insert(RPC_BOOT_EPOCH_HEADER, header_value(&boot_epoch.to_string(), RPC_BOOT_EPOCH_HEADER)?);
    headers.insert(RPC_BOOT_EPOCH_PROOF_HEADER, header_value(&proof, RPC_BOOT_EPOCH_PROOF_HEADER)?);
    headers.insert(
        RPC_REPLAY_CACHE_CAPABILITY_HEADER,
        HeaderValue::from_static(RPC_REPLAY_CACHE_CAPABILITY_V1),
    );
    headers.insert(
        RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER,
        header_value(&capability_proof, RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER)?,
    );
    Ok(headers)
}

/// Verify the server boot-epoch response for a challenge generated by this client.
pub fn verify_tonic_boot_epoch_response(audience: &str, challenge: Uuid, headers: &HeaderMap) -> std::io::Result<Uuid> {
    verify_tonic_boot_epoch_response_with_secret(&get_shared_secret()?, audience, challenge, headers)
}

fn verify_tonic_boot_epoch_response_with_secret(
    secret: &str,
    audience: &str,
    challenge: Uuid,
    headers: &HeaderMap,
) -> std::io::Result<Uuid> {
    let boot_epoch = headers
        .get(RPC_BOOT_EPOCH_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC boot epoch"))
        .and_then(|value| non_nil_uuid(value, "RPC boot epoch"))?;
    let proof = headers
        .get(RPC_BOOT_EPOCH_PROOF_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC boot epoch proof"))?;
    verify_boot_epoch_proof(secret, audience, challenge, boot_epoch, proof)?;
    Ok(boot_epoch)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AuthenticatedPeerReplayCapabilities {
    pub(crate) boot_epoch: Uuid,
    pub(crate) dynamic_replay_cache: bool,
}

pub(crate) fn verify_tonic_peer_replay_capabilities_response(
    audience: &str,
    challenge: Uuid,
    headers: &HeaderMap,
) -> std::io::Result<AuthenticatedPeerReplayCapabilities> {
    let secret = get_shared_secret()?;
    let boot_epoch = verify_tonic_boot_epoch_response_with_secret(&secret, audience, challenge, headers)?;
    let capability = headers.get(RPC_REPLAY_CACHE_CAPABILITY_HEADER);
    let proof = headers.get(RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER);
    if capability.is_none() && proof.is_none() {
        return Ok(AuthenticatedPeerReplayCapabilities {
            boot_epoch,
            dynamic_replay_cache: false,
        });
    }
    let capability = capability
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC replay cache capability"))?;
    if capability != RPC_REPLAY_CACHE_CAPABILITY_V1 {
        return Err(std::io::Error::other("Unsupported RPC replay cache capability"));
    }
    let proof = proof
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC replay cache capability proof"))?;
    verify_replay_cache_capability_proof(&secret, audience, challenge, boot_epoch, proof)?;
    Ok(AuthenticatedPeerReplayCapabilities {
        boot_epoch,
        dynamic_replay_cache: true,
    })
}

fn valid_content_sha256(value: &str) -> bool {
    value == UNSIGNED_PAYLOAD
        || (value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
}

fn header_value(value: &str, name: &str) -> std::io::Result<HeaderValue> {
    HeaderValue::from_str(value).map_err(|_| std::io::Error::other(format!("Invalid {name} header value")))
}

pub fn normalize_tonic_rpc_audience(value: &str) -> std::io::Result<String> {
    let authority = value
        .parse::<Authority>()
        .map_err(|_| std::io::Error::other("Invalid gRPC peer authority"))?;
    Ok(authority.as_str().to_ascii_lowercase())
}

fn check_timestamp(timestamp: i64) -> std::io::Result<()> {
    let current_time = OffsetDateTime::now_utc().unix_timestamp();
    if current_time.saturating_sub(timestamp) > SIGNATURE_VALID_DURATION
        || timestamp.saturating_sub(current_time) > SIGNATURE_VALID_DURATION
    {
        return Err(std::io::Error::other("Request timestamp expired"));
    }
    Ok(())
}

fn tonic_rpc_metric_operation(path: &str) -> &'static str {
    match parse_tonic_rpc_path(path).ok().map(|(_, rpc_method)| rpc_method) {
        Some("ReadAll") => INTERNODE_OPERATION_GRPC_READ_ALL,
        Some("ReadMultiple") => INTERNODE_OPERATION_GRPC_READ_MULTIPLE,
        Some("ReadVersion") => INTERNODE_OPERATION_GRPC_READ_VERSION,
        Some("BatchReadVersion") => INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
        Some("WriteAll") => INTERNODE_OPERATION_GRPC_WRITE_ALL,
        Some("CompareAndUpdateFile") => INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
        Some("Lock") => INTERNODE_OPERATION_GRPC_LOCK,
        Some("UnLock") => INTERNODE_OPERATION_GRPC_UNLOCK,
        Some("LockBatch") => INTERNODE_OPERATION_GRPC_LOCK_BATCH,
        Some("UnLockBatch") => INTERNODE_OPERATION_GRPC_UNLOCK_BATCH,
        Some("Refresh") => INTERNODE_OPERATION_GRPC_REFRESH,
        Some("ForceUnLock") => INTERNODE_OPERATION_GRPC_FORCE_UNLOCK,
        _ => INTERNODE_OPERATION_GRPC_OTHER,
    }
}

fn check_and_record_nonce_with_scope(
    nonce: Uuid,
    compatibility_nonce: Option<Uuid>,
    signed_at: i64,
    rpc_path: &str,
    operation: &'static str,
    backend: &'static str,
) -> std::io::Result<()> {
    let wall_time = OffsetDateTime::now_utc().unix_timestamp();
    let (result, metrics) = {
        let mut cache = LOCAL_RPC_NONCE_CACHE
            .lock()
            .map_err(|_| std::io::Error::other("RPC replay cache unavailable"))?;
        cache.check_and_record(RpcNonceRecord {
            nonce,
            compatibility_nonce,
            signed_at,
            wall_time,
            capacity: REPLAY_CACHE_CAPACITY.capacity,
            memory_budget_bytes: REPLAY_CACHE_CAPACITY.memory_budget_bytes,
            metric_scope: RpcReplayCacheMetricScope {
                operation,
                backend,
                rpc_path,
            },
        })
    };
    publish_nonce_cache_metrics(metrics);
    result
}

fn check_and_record_tonic_nonce(
    nonce: Uuid,
    compatibility_nonce: Option<Uuid>,
    signed_at: i64,
    rpc_path: &str,
) -> std::io::Result<()> {
    check_and_record_nonce_with_scope(
        nonce,
        compatibility_nonce,
        signed_at,
        rpc_path,
        tonic_rpc_metric_operation(rpc_path),
        INTERNODE_TRANSPORT_BACKEND_GRPC,
    )
}

pub fn check_and_record_signed_rpc_nonce(
    headers: &HeaderMap,
    nonce: Uuid,
    rpc_path: &str,
    operation: &'static str,
    backend: &'static str,
) -> std::io::Result<()> {
    if nonce.is_nil() {
        return Err(std::io::Error::other("Invalid RPC nonce"));
    }
    let timestamp_header = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;
    let timestamp = timestamp_header
        .parse::<i64>()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;
    check_timestamp(timestamp)?;
    check_and_record_nonce_with_scope(nonce, None, timestamp, rpc_path, operation, backend)
}

/// Build headers with authentication signature
pub fn build_auth_headers(url: &str, method: &Method, headers: &mut HeaderMap) -> std::io::Result<()> {
    let auth_headers = gen_signature_headers(url, method)?;

    headers.extend(auth_headers);
    inject_trace_context_into_http_headers(headers);
    inject_request_id_into_http_headers(headers);
    Ok(())
}

/// Signs only external stream controls targeting members of this instance's topology.
#[cfg(feature = "internode-transport")]
#[derive(Clone)]
pub struct ExternalStreamAuth {
    peers: std::sync::Arc<std::collections::HashSet<url::Origin>>,
}

#[cfg(feature = "internode-transport")]
impl ExternalStreamAuth {
    /// The authenticated HTTP route used by peer stream controls.
    pub const PATH: &'static str = "/rustfs/rpc/external_stream";

    /// Capture the topology already configured for this host; a missing topology has no peers.
    pub fn new(instance: &crate::runtime::instance::InstanceContext) -> Self {
        let mut peers = std::collections::HashSet::new();
        if let Some(topology) = instance.endpoints() {
            for pool in topology.0 {
                for endpoint in pool.endpoints.as_ref() {
                    if matches!(endpoint.url.scheme(), "http" | "https") {
                        peers.insert(endpoint.url.origin());
                    }
                }
            }
        }
        Self {
            peers: std::sync::Arc::new(peers),
        }
    }

    /// Sign a fresh POST for one configured origin, request body digest and replay nonce.
    /// The caller must send it to the returned URL without redirects or resubmission.
    pub fn sign_control(&self, endpoint: &str, digest: &[u8; 32]) -> std::io::Result<(String, HeaderMap)> {
        let mut url = url::Url::parse(endpoint).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || !self.peers.contains(&url.origin())
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "invalid internode control endpoint",
            ));
        }
        url.set_path(Self::PATH);
        url.query_pairs_mut()
            .append_pair("body_sha256", &hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower))
            .append_pair("request_nonce", &Uuid::new_v4().to_string());
        let url = url.to_string();
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
        build_auth_headers(&url, &Method::POST, &mut headers)?;
        Ok((url, headers))
    }

    /// Derive the operation key for this stream protocol and exact request digest.
    /// The shared RPC signing credential is never exposed to the stream provider.
    pub fn transfer_key(&self, request_digest: &[u8; 32]) -> std::io::Result<[u8; 32]> {
        let secret = get_shared_secret()?;
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(std::io::Error::other)?;
        mac.update(b"rustfs-external-stream-key-v1\0");
        mac.update(request_digest);
        Ok(mac.finalize().into_bytes().into())
    }
}

pub fn gen_signature_headers(url: &str, method: &Method) -> std::io::Result<HeaderMap> {
    let secret = get_shared_secret()?;
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let background = current_rpc_background_request();

    let signature = generate_signature(&secret, url, method, timestamp, background);

    let mut headers = HeaderMap::new();
    insert_background_admission(&mut headers, background);
    headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).expect("Invalid header value"));
    headers.insert(
        TIMESTAMP_HEADER,
        HeaderValue::from_str(&timestamp.to_string()).expect("Invalid header value"),
    );

    Ok(headers)
}

/// Generate rolling-upgrade-safe gRPC auth metadata.
///
/// The legacy signature remains present for old servers. New servers prefer the
/// v2 signature and bind it to the destination authority and exact generated
/// gRPC method. A versioned canonical mutation payload can opt into the
/// additional body-digest capability.
pub fn gen_tonic_signature_headers(
    audience: &str,
    service: &str,
    rpc_method: &str,
    content_sha256: Option<&str>,
) -> std::io::Result<HeaderMap> {
    if audience.is_empty() || service.is_empty() || rpc_method.is_empty() || service.contains('/') || rpc_method.contains('/') {
        return Err(std::io::Error::other("Invalid RPC v2 signing scope"));
    }
    let content_sha256 = content_sha256.unwrap_or(UNSIGNED_PAYLOAD);
    if !valid_content_sha256(content_sha256) {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }

    let secret = get_shared_secret()?;
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let timestamp_header = timestamp.to_string();
    let body_nonce = (content_sha256 != UNSIGNED_PAYLOAD).then(|| Uuid::new_v4().to_string());
    let nonce = body_nonce.as_deref().unwrap_or(UNSIGNED_PAYLOAD_NONCE);
    let background = current_rpc_background_request();
    let legacy_signature = generate_signature(&secret, TONIC_RPC_PREFIX, &Method::GET, timestamp, background);
    let signature_v2 = generate_signature_v2(
        &secret,
        SignatureV2Scope {
            background,
            audience,
            service,
            rpc_method,
            timestamp: &timestamp_header,
            nonce,
            content_sha256,
        },
    )?;

    let mut headers = HeaderMap::new();
    insert_background_admission(&mut headers, background);
    headers.insert(SIGNATURE_HEADER, header_value(&legacy_signature, SIGNATURE_HEADER)?);
    headers.insert(TIMESTAMP_HEADER, header_value(&timestamp_header, TIMESTAMP_HEADER)?);
    headers.insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));
    headers.insert(RPC_SIGNATURE_V2_HEADER, header_value(&signature_v2, RPC_SIGNATURE_V2_HEADER)?);
    headers.insert(RPC_NONCE_HEADER, header_value(nonce, RPC_NONCE_HEADER)?);
    headers.insert(RPC_CONTENT_SHA256_HEADER, header_value(content_sha256, RPC_CONTENT_SHA256_HEADER)?);
    Ok(headers)
}

/// Bind a mutation to a versioned, deterministic canonical payload.
///
/// Do not pass a protobuf re-encoding here: unknown fields and map ordering are
/// not a stable mixed-version contract.
pub fn set_tonic_canonical_body_digest<T>(request: &mut tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    let digest = hex_simd::encode_to_string(Sha256::digest(canonical_body), hex_simd::AsciiCase::Lower);
    request
        .metadata_mut()
        .as_mut()
        .insert(RPC_CONTENT_SHA256_HEADER, header_value(&digest, RPC_CONTENT_SHA256_HEADER)?);
    Ok(())
}

pub fn set_tonic_mutation_body_digest<T: rustfs_protos::CanonicalMutationBody>(
    request: &mut tonic::Request<T>,
) -> std::io::Result<()> {
    let canonical_body = request
        .get_ref()
        .canonical_body()
        .map_err(|_| std::io::Error::other("RPC mutation body length cannot be represented"))?;
    set_tonic_canonical_body_digest(request, &canonical_body)
}

pub fn set_tonic_rolling_mutation_body_digest<T: rustfs_protos::CanonicalMutationBody>(
    request: &mut tonic::Request<T>,
) -> std::io::Result<()> {
    set_tonic_mutation_body_digest(request)?;
    request.extensions_mut().insert(RollingMutationBodyDigest);
    Ok(())
}

pub fn set_tonic_rolling_canonical_body_digest<T>(request: &mut tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    set_tonic_canonical_body_digest(request, canonical_body)?;
    request.extensions_mut().insert(RollingMutationBodyDigest);
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RollingMutationBodyDigest;

pub fn verify_tonic_canonical_body_digest<T>(request: &tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    let version = request
        .metadata()
        .get(RPC_AUTH_VERSION_HEADER)
        .and_then(|value| value.to_str().ok());
    if version != Some(RPC_AUTH_VERSION_V2) {
        return Err(std::io::Error::other("RPC mutation requires v2 authentication"));
    }
    let expected = request
        .metadata()
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC content SHA-256"))?;
    if expected == UNSIGNED_PAYLOAD || !valid_content_sha256(expected) {
        return Err(std::io::Error::other("RPC body is not bound to the signature"));
    }
    let actual = hex_simd::encode_to_string(Sha256::digest(canonical_body), hex_simd::AsciiCase::Lower);
    if actual != expected {
        return Err(std::io::Error::other("RPC content SHA-256 mismatch"));
    }
    Ok(())
}

/// Verify a mutating RPC's canonical body digest with a rolling-upgrade fallback.
///
/// When the request carries a real (non-`UNSIGNED-PAYLOAD`) content SHA-256 it is verified exactly
/// like [`verify_tonic_canonical_body_digest`]. The digest value is a member of the signed v2
/// scope, so within the v2 lane it cannot be stripped or altered without invalidating the signature
/// `check_auth` already enforced. When the request carries no digest — a peer that predates
/// body-digest signing, or an attacker who downgraded the request to the legacy signature by
/// dropping every v2 header — the request is accepted and counted on the body-digest fallback
/// counter unless `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT` is enabled. That switch is what actually
/// closes on-path body tampering for covered handlers: it rejects every digestless mutation,
/// including v1-downgraded ones. It converges independently of the signature-strict switch
/// (<https://github.com/rustfs/backlog/issues/1327>).
pub fn verify_tonic_mutation_body_digest<T>(request: &tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    verify_tonic_mutation_body_digest_with_strictness(request, canonical_body, internode_rpc_body_digest_strict())
}

/// Verify a non-disk mutation without accepting a newly-generated unsigned v2 body.
///
/// The disk mutation lane has a rolling-upgrade exception for `UNSIGNED-PAYLOAD`
/// while peer replay-cache capability is being discovered. Historical v2 peers
/// used the fixed `unsigned` nonce before body-digest rollout; preserve that
/// exact marker for mixed-version compatibility, but reject unsigned v2
/// requests that omit it or present a different nonce.
pub fn verify_tonic_mutation_body_digest_reject_unsigned<T>(
    request: &tonic::Request<T>,
    canonical_body: &[u8],
) -> std::io::Result<()> {
    let version = request
        .metadata()
        .get(RPC_AUTH_VERSION_HEADER)
        .and_then(|value| value.to_str().ok());
    let digest = request
        .metadata()
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok());
    let nonce = request.metadata().get(RPC_NONCE_HEADER).and_then(|value| value.to_str().ok());
    if version == Some(RPC_AUTH_VERSION_V2) && digest == Some(UNSIGNED_PAYLOAD) && nonce != Some("unsigned") {
        return Err(std::io::Error::other("RPC mutation requires a body-bound v2 signature"));
    }
    verify_tonic_mutation_body_digest(request, canonical_body)
}

/// [`verify_tonic_mutation_body_digest`] with the strict gate injected as a parameter, so both
/// rollout postures are unit-testable without racing on process-global environment variables.
fn verify_tonic_mutation_body_digest_with_strictness<T>(
    request: &tonic::Request<T>,
    canonical_body: &[u8],
    strict: bool,
) -> std::io::Result<()> {
    let digest = request
        .metadata()
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok());
    match digest {
        Some(digest) if digest != UNSIGNED_PAYLOAD => verify_tonic_canonical_body_digest(request, canonical_body),
        _ => {
            // RUSTFS_COMPAT_TODO(disk-mutation-body-digest): accept digestless peers during rolling upgrades. Remove after the
            // minimum supported RustFS peer version body-binds every mutating RPC.
            if strict {
                return Err(std::io::Error::other("RPC mutation requires a body-bound v2 signature"));
            }
            // Count only ACCEPTED digestless mutations: this counter is the convergence gate that
            // must read zero fleet-wide across a release window before
            // `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT` may be enabled.
            global_internode_metrics().record_body_digest_fallback();
            Ok(())
        }
    }
}

fn has_v2_auth_headers(headers: &HeaderMap) -> bool {
    [
        RPC_AUTH_VERSION_HEADER,
        RPC_SIGNATURE_V2_HEADER,
        RPC_NONCE_HEADER,
        RPC_CONTENT_SHA256_HEADER,
    ]
    .iter()
    .any(|name| headers.contains_key(*name))
}

fn has_replay_scope_headers(headers: &HeaderMap) -> bool {
    [
        RPC_REPLAY_SCOPE_VERSION_HEADER,
        RPC_REPLAY_SCOPE_SIGNATURE_HEADER,
        RPC_REPLAY_SCOPE_NONCE_HEADER,
        RPC_BOOT_EPOCH_HEADER,
    ]
    .iter()
    .any(|name| headers.contains_key(*name))
}

/// Whether the server requires target-bound v2 authentication on every internode gRPC request,
/// rejecting the legacy constant-target fallback instead of accepting it. Default-off rollout
/// lever gated on the v1-fallback counter reading zero fleet-wide; see
/// [`rustfs_config::ENV_INTERNODE_RPC_SIGNATURE_STRICT`] and
/// <https://github.com/rustfs/backlog/issues/1327>.
fn internode_rpc_signature_strict() -> bool {
    *INTERNODE_RPC_SIGNATURE_STRICT
}

fn internode_rpc_replay_scope_strict() -> bool {
    *INTERNODE_RPC_REPLAY_SCOPE_STRICT
}

fn verify_tonic_replay_scope_signature(audience: &str, path: &str, headers: &HeaderMap) -> std::io::Result<()> {
    if audience.is_empty() {
        return Err(std::io::Error::other("Missing RPC audience"));
    }
    parse_tonic_rpc_path(path)?;

    let version = headers
        .get(RPC_REPLAY_SCOPE_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC replay scope version"))?;
    if version != RPC_REPLAY_SCOPE_VERSION_V3 {
        return Err(std::io::Error::other("Unsupported RPC replay scope version"));
    }
    let signature = headers
        .get(RPC_REPLAY_SCOPE_SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC replay scope signature"))?;
    let timestamp = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;
    let signed_at = timestamp
        .parse::<i64>()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;
    check_timestamp(signed_at)?;
    let nonce = headers
        .get(RPC_REPLAY_SCOPE_NONCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC replay scope nonce"))
        .and_then(|value| non_nil_uuid(value, "RPC replay scope nonce"))?;
    let content_sha256 = headers
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC content SHA-256"))?;
    if !valid_content_sha256(content_sha256) {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }
    let boot_epoch = headers
        .get(RPC_BOOT_EPOCH_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC boot epoch"))
        .and_then(|value| non_nil_uuid(value, "RPC boot epoch"))?;
    let secret = get_shared_secret()?;
    if !verify_replay_scope_signature(
        &secret,
        ReplayScope {
            background: rpc_background_admission(headers)?,
            audience,
            path,
            timestamp,
            nonce,
            content_sha256,
            boot_epoch,
        },
        signature,
    ) {
        return Err(std::io::Error::other("Invalid RPC replay scope signature"));
    }
    if boot_epoch != tonic_rpc_boot_epoch() {
        return Err(std::io::Error::other("RPC boot epoch is stale"));
    }
    // Verify both usable authorizations before reserving either identity. A
    // resource or signature failure must not leave a partially admitted RPC.
    let compatibility_nonce = if content_sha256 != UNSIGNED_PAYLOAD {
        Some(
            verified_tonic_rpc_nonce(audience, path, headers, true)?
                .ok_or_else(|| std::io::Error::other("Invalid RPC nonce"))?
                .0,
        )
    } else {
        None
    };
    check_and_record_tonic_nonce(nonce, compatibility_nonce, signed_at, path)
}

/// Verify gRPC authentication, preferring v2 without downgrade on malformed v2 metadata.
pub fn verify_tonic_rpc_signature(audience: &str, path: &str, headers: &HeaderMap) -> std::io::Result<()> {
    verify_tonic_rpc_signature_with_policy(
        audience,
        path,
        headers,
        internode_rpc_signature_strict(),
        internode_rpc_replay_scope_strict(),
        false,
    )
}

/// Verify gRPC authentication while allowing the narrowly scoped v2 `Ping` bootstrap used to
/// obtain an authenticated server boot epoch when replay-scope strictness is enabled.
pub fn verify_tonic_rpc_signature_with_bootstrap(
    audience: &str,
    path: &str,
    headers: &HeaderMap,
    allow_replay_scope_bootstrap: bool,
) -> std::io::Result<()> {
    verify_tonic_rpc_signature_with_policy(
        audience,
        path,
        headers,
        internode_rpc_signature_strict(),
        internode_rpc_replay_scope_strict(),
        allow_replay_scope_bootstrap,
    )
}

pub fn tonic_rpc_auth_failure_reason(error: &std::io::Error) -> &'static str {
    if error.kind() == std::io::ErrorKind::OutOfMemory {
        return "replay_cache_allocation";
    }
    match error.to_string().as_str() {
        "Missing RPC audience" => "missing_audience",
        "Invalid RPC request path" => "invalid_request_path",
        "RPC replay-scoped authentication required" => "replay_scope_required",
        "Missing RPC replay scope version" => "missing_replay_scope_version",
        "Unsupported RPC replay scope version" => "unsupported_replay_scope_version",
        "Missing RPC replay scope signature" => "missing_replay_scope_signature",
        "Missing RPC replay scope nonce" => "missing_replay_scope_nonce",
        "Invalid RPC replay scope nonce" => "invalid_replay_scope_nonce",
        "Missing RPC boot epoch" => "missing_boot_epoch",
        "Invalid RPC boot epoch" => "invalid_boot_epoch",
        "Invalid RPC replay scope signature" => "invalid_replay_scope_signature",
        "RPC boot epoch is stale" => "stale_boot_epoch",
        "RPC request replay detected" => "replay_detected",
        "RPC replay cache capacity exceeded" => "replay_cache_capacity",
        "RPC replay cache memory budget exceeded" => "replay_cache_memory",
        "RPC replay cache unavailable" => "replay_cache_unavailable",
        "RPC replay expiry overflow" => "replay_expiry_overflow",
        "RPC request timestamp expired after clock regression" => "timestamp_expired_after_clock_regression",
        "RPC v2 authentication required" => "v2_required",
        "Missing RPC auth version" => "missing_v2_auth_version",
        "Unsupported RPC auth version" => "unsupported_v2_auth_version",
        "Missing RPC v2 signature" => "missing_v2_signature",
        "Invalid RPC v2 signature" => "invalid_v2_signature",
        "Missing timestamp header" => "missing_timestamp",
        "Invalid timestamp format" => "invalid_timestamp",
        "Request timestamp expired" => "timestamp_expired",
        "Missing RPC nonce" => "missing_v2_nonce",
        "Invalid RPC nonce" => "invalid_v2_nonce",
        "Invalid unsigned RPC nonce" => "invalid_unsigned_v2_nonce",
        "Missing RPC content SHA-256" => "missing_content_sha256",
        "Invalid RPC content SHA-256" => "invalid_content_sha256",
        "Invalid put_file auth trailer length" => "invalid_put_file_auth_trailer_length",
        "Invalid put_file auth trailer" => "invalid_put_file_auth_trailer",
        "Missing signature header" => "missing_v1_signature",
        "Invalid signature" => "invalid_v1_signature",
        "Invalid RPC HMAC key" => "invalid_hmac_key",
        message if message.contains(RPC_SECRET_REQUIRED_OPERATOR_MESSAGE) => "missing_rpc_secret",
        _ => "unknown",
    }
}

fn verify_tonic_rpc_signature_with_policy(
    audience: &str,
    path: &str,
    headers: &HeaderMap,
    signature_strict: bool,
    replay_scope_strict: bool,
    allow_replay_scope_bootstrap: bool,
) -> std::io::Result<()> {
    if has_replay_scope_headers(headers) {
        return verify_tonic_replay_scope_signature(audience, path, headers);
    }

    // Only a method-bound v2 Ping with a syntactically valid challenge may bootstrap a strict
    // client after its peer restarts. Legacy metadata never gets this exception.
    let bootstrap = allow_replay_scope_bootstrap
        && has_v2_auth_headers(headers)
        && tonic_boot_epoch_challenge(headers).is_ok_and(|challenge| challenge.is_some());
    if replay_scope_strict && !bootstrap {
        return Err(std::io::Error::other("RPC replay-scoped authentication required"));
    }

    verify_tonic_rpc_signature_with_strictness(audience, path, headers, signature_strict)?;
    global_internode_metrics().record_replay_scope_fallback();
    Ok(())
}

/// [`verify_tonic_rpc_signature`] with the strict gate injected as a parameter, so both rollout
/// postures are unit-testable without racing on process-global environment variables.
fn verify_tonic_rpc_signature_with_strictness(
    audience: &str,
    path: &str,
    headers: &HeaderMap,
    strict: bool,
) -> std::io::Result<()> {
    if let Some((nonce, signed_at)) = verified_tonic_rpc_nonce(audience, path, headers, strict)? {
        check_and_record_tonic_nonce(nonce, None, signed_at, path)?;
    }
    Ok(())
}

fn verified_tonic_rpc_nonce(
    audience: &str,
    path: &str,
    headers: &HeaderMap,
    strict: bool,
) -> std::io::Result<Option<(Uuid, i64)>> {
    if !has_v2_auth_headers(headers) {
        if rpc_background_admission(headers)? {
            return Err(std::io::Error::other("RPC background admission requires v2 authentication"));
        }
        // RUSTFS_COMPAT_TODO(heal-rpc-auth-v2): accept old peers during rolling upgrades. Remove after the minimum
        // supported RustFS peer version sends v2 authentication on every internode gRPC request.
        if strict {
            return Err(std::io::Error::other("RPC v2 authentication required"));
        }
        verify_rpc_signature(TONIC_RPC_PREFIX, &Method::GET, headers)?;
        // Count only ACCEPTED legacy-only requests: this counter is the convergence gate that must
        // read zero fleet-wide across a release window before
        // `RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT` may be enabled.
        global_internode_metrics().record_signature_v1_fallback();
        return Ok(None);
    }

    let path = path
        .strip_prefix('/')
        .ok_or_else(|| std::io::Error::other("Invalid RPC request path"))?;
    let (service, rpc_method) = path
        .split_once('/')
        .filter(|(service, rpc_method)| !service.is_empty() && !rpc_method.is_empty() && !rpc_method.contains('/'))
        .ok_or_else(|| std::io::Error::other("Invalid RPC request path"))?;
    if audience.is_empty() {
        return Err(std::io::Error::other("Missing RPC audience"));
    }

    let version = headers
        .get(RPC_AUTH_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC auth version"))?;
    if version != RPC_AUTH_VERSION_V2 {
        return Err(std::io::Error::other("Unsupported RPC auth version"));
    }
    let signature = headers
        .get(RPC_SIGNATURE_V2_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC v2 signature"))?;
    let timestamp_header = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;
    let timestamp = timestamp_header
        .parse::<i64>()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;
    check_timestamp(timestamp)?;
    let nonce = headers
        .get(RPC_NONCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC nonce"))?;
    let content_sha256 = headers
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC content SHA-256"))?;
    if !valid_content_sha256(content_sha256) {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }
    let parsed_nonce = if content_sha256 == UNSIGNED_PAYLOAD {
        if nonce != UNSIGNED_PAYLOAD_NONCE {
            return Err(std::io::Error::other("Invalid unsigned RPC nonce"));
        }
        None
    } else {
        let parsed_nonce = Uuid::parse_str(nonce).map_err(|_| std::io::Error::other("Invalid RPC nonce"))?;
        if parsed_nonce.is_nil() {
            return Err(std::io::Error::other("Invalid RPC nonce"));
        }
        Some(parsed_nonce)
    };

    let secret = get_shared_secret()?;
    if !verify_signature_v2(
        &secret,
        SignatureV2Scope {
            background: rpc_background_admission(headers)?,
            audience,
            service,
            rpc_method,
            timestamp: timestamp_header,
            nonce,
            content_sha256,
        },
        signature,
    ) {
        return Err(std::io::Error::other("Invalid RPC v2 signature"));
    }
    Ok(parsed_nonce.map(|nonce| (nonce, timestamp)))
}

/// Verify the request signature for RPC requests
pub fn verify_rpc_signature(url: &str, method: &Method, headers: &HeaderMap) -> std::io::Result<()> {
    // Get signature from header
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing signature header"))?;

    // Get timestamp from header
    let timestamp_str = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;

    let timestamp: i64 = timestamp_str
        .parse()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;

    check_timestamp(timestamp)?;

    // Verify signature with constant-time HMAC comparison.
    let secret = get_shared_secret()?;

    if !verify_signature(&secret, url, method, timestamp, signature, rpc_background_admission(headers)?) {
        let rpc_path = redacted_rpc_path(url);
        error!(
            rpc_path = %rpc_path,
            method = %method,
            timestamp,
            signature_len = signature.len(),
            "verify_rpc_signature: Invalid signature"
        );

        return Err(std::io::Error::other("Invalid signature"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::context_propagation::REQUEST_ID_HEADER;
    use crate::runtime::sources as runtime_sources;
    use http::{HeaderMap, Method};
    use rustfs_protos::{
        CanonicalMutationBody as _, PEER_RESTDRY_RUN, PEER_RESTSIGNAL, PEER_RESTSUB_SYS,
        proto_gen::node_service::{Mss, SignalServiceRequest},
    };
    use std::collections::HashMap;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;
    use tracing_subscriber::fmt::MakeWriter;

    fn signal_service_request(signal: &str, sub_system: &str, dry_run: &str) -> SignalServiceRequest {
        SignalServiceRequest {
            vars: Some(Mss {
                value: HashMap::from([
                    (PEER_RESTSIGNAL.to_string(), signal.to_string()),
                    (PEER_RESTSUB_SYS.to_string(), sub_system.to_string()),
                    (PEER_RESTDRY_RUN.to_string(), dry_run.to_string()),
                ]),
            }),
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn ensure_test_rpc_secret() {
        runtime_sources::ensure_test_rpc_secret();
    }

    #[tokio::test]
    async fn background_admission_is_bound_in_http_and_both_tonic_signature_lanes() {
        ensure_test_rpc_secret();
        let secret = get_shared_secret().expect("RPC secret");
        let path = "/node_service.NodeService/ReadAll";
        for background in [false, true] {
            let (http, tonic) = with_rpc_background_request(background, async {
                (
                    gen_signature_headers("/rustfs/rpc/read?disk=a", &Method::GET).expect("HTTP signature"),
                    gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "ReadAll", None)
                        .expect("gRPC signature"),
                )
            })
            .await;
            assert_eq!(rpc_background_admission(&http).expect("HTTP class"), background);
            assert_eq!(rpc_background_admission(&tonic).expect("gRPC class"), background);
            verify_rpc_signature("/rustfs/rpc/read?disk=a", &Method::GET, &http).expect("HTTP authentication");
            verify_tonic_rpc_signature_with_strictness("node-a:9000", path, &tonic, true).expect("v2 authentication");

            let timestamp = tonic[TIMESTAMP_HEADER].to_str().expect("timestamp");
            let content = tonic[RPC_CONTENT_SHA256_HEADER].to_str().expect("content digest");
            assert_eq!(
                verify_signature_v2(
                    &secret,
                    SignatureV2Scope {
                        background: false,
                        audience: "node-a:9000",
                        service: "node_service.NodeService",
                        rpc_method: "ReadAll",
                        timestamp,
                        nonce: tonic[RPC_NONCE_HEADER].to_str().expect("nonce"),
                        content_sha256: content,
                    },
                    tonic[RPC_SIGNATURE_V2_HEADER].to_str().expect("signature"),
                ),
                !background,
                "an old peer cannot authenticate a background request as foreground"
            );

            let mut replay = tonic.clone();
            // The channel can run outside the caller's task-local scope.
            replay.extend(
                gen_tonic_replay_scope_headers_with_admission(
                    "node-a:9000",
                    path,
                    timestamp,
                    content,
                    tonic_rpc_boot_epoch(),
                    background,
                )
                .expect("v3 signature"),
            );
            for mutation in ["strip-or-add", "unknown", "duplicate"] {
                let alter = |headers: &HeaderMap| {
                    let mut changed = headers.clone();
                    match mutation {
                        "strip-or-add" if background => {
                            changed.remove(RPC_BACKGROUND_ADMISSION_HEADER);
                        }
                        "strip-or-add" => insert_background_admission(&mut changed, true),
                        "unknown" => {
                            changed.insert(RPC_BACKGROUND_ADMISSION_HEADER, HeaderValue::from_static("v2"));
                        }
                        _ => {
                            insert_background_admission(&mut changed, true);
                            changed.append(RPC_BACKGROUND_ADMISSION_HEADER, HeaderValue::from_static("v1"));
                        }
                    }
                    changed
                };
                assert!(verify_rpc_signature("/rustfs/rpc/read?disk=a", &Method::GET, &alter(&http)).is_err());
                assert!(verify_tonic_rpc_signature_with_strictness("node-a:9000", path, &alter(&tonic), true).is_err());
                assert!(verify_tonic_replay_scope_signature("node-a:9000", path, &alter(&replay)).is_err());
            }
            verify_tonic_replay_scope_signature("node-a:9000", path, &replay).expect("v3 authentication");
            assert!(
                verify_tonic_replay_scope_signature("node-a:9000", path, &replay).is_err(),
                "replay rejected"
            );

            if background {
                let mut downgraded = HeaderMap::new();
                for name in [SIGNATURE_HEADER, TIMESTAMP_HEADER, RPC_BACKGROUND_ADMISSION_HEADER] {
                    downgraded.insert(name, tonic[name].clone());
                }
                assert!(verify_tonic_rpc_signature_with_strictness("node-a:9000", path, &downgraded, false).is_err());
                downgraded.remove(RPC_BACKGROUND_ADMISSION_HEADER);
                assert!(verify_tonic_rpc_signature_with_strictness("node-a:9000", path, &downgraded, false).is_err());
            }
        }
    }

    #[test]
    fn namespace_scanner_capability_proof_binds_challenge_and_server_epoch() {
        let secret = "test-scanner-capability-secret";
        let challenge = Uuid::new_v4();
        let server_epoch = Uuid::new_v4();
        let proof = generate_ns_scanner_capability_proof(secret, challenge, server_epoch, false, false)
            .expect("capability proof should be generated");

        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, false, false).is_ok());
        assert!(verify_ns_scanner_capability_proof(secret, Uuid::new_v4(), server_epoch, &proof, false, false).is_err());
        assert!(verify_ns_scanner_capability_proof(secret, challenge, Uuid::new_v4(), &proof, false, false).is_err());
        assert!(verify_ns_scanner_capability_proof("different-secret", challenge, server_epoch, &proof, false, false).is_err());
    }

    #[test]
    fn namespace_scanner_capability_proof_binds_checkpoint_coverage_support() {
        let secret = "checkpoint-capability-test-secret";
        let challenge = Uuid::new_v4();
        let server_epoch = Uuid::new_v4();
        let proof = generate_ns_scanner_capability_proof(secret, challenge, server_epoch, true, true)
            .expect("checkpoint capability proof");
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, true, true).is_ok());
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, true, false).is_err());
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, false, true).is_err());
        let legacy = generate_ns_scanner_capability_proof(secret, challenge, server_epoch, true, false)
            .expect("legacy generation capability proof");
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &legacy, true, true).is_err());
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &[], true, true).is_err());
        assert!(verify_ns_scanner_capability_proof(secret, Uuid::new_v4(), server_epoch, &proof, true, true).is_err());
    }

    #[test]
    fn namespace_scanner_capability_proof_binds_tier_registry_generation_support() {
        let secret = "test-scanner-capability-secret";
        let challenge = Uuid::new_v4();
        let server_epoch = Uuid::new_v4();
        let proof = generate_ns_scanner_capability_proof(secret, challenge, server_epoch, true, false)
            .expect("generation capability proof should be generated");

        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, true, false).is_ok());
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &proof, false, false).is_err());
        let legacy = generate_ns_scanner_capability_proof(secret, challenge, server_epoch, false, false)
            .expect("legacy capability proof should be generated");
        assert!(verify_ns_scanner_capability_proof(secret, challenge, server_epoch, &legacy, true, false).is_err());
    }

    /// Security regression for GHSA-r5qv-rc46-hv8q (internode RPC fail-closed,
    /// fixed in rustfs/rustfs#4402): secret resolution must never silently fall
    /// back to a default/empty shared secret. Missing and default secrets both
    /// resolve to an error, so a misconfigured node cannot come up with a
    /// predictable, attacker-known RPC key.
    #[test]
    fn ghsa_r5qv_resolve_shared_secret_rejects_default_fallback() {
        let err = resolve_shared_secret(None, None).expect_err("default fallback must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(None, Some(DEFAULT_SECRET_KEY)).expect_err("default global secret must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(Some(DEFAULT_SECRET_KEY), None).expect_err("default env secret must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(Some("   "), Some("   ")).expect_err("blank secrets must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);
    }

    /// Security regression for GHSA-r5qv-rc46-hv8q: `verify_rpc_signature` must
    /// fail closed for every shape of missing or malformed authentication.
    /// Consolidates the advisory's exact scenario (no valid signature/timestamp
    /// pair => rejected, never silently allowed) into one named test so the
    /// advisory maps to a discoverable regression.
    #[test]
    fn ghsa_r5qv_verify_rpc_signature_fails_closed_on_missing_or_invalid_auth() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;

        // No auth headers at all.
        let empty = HeaderMap::new();
        assert!(
            verify_rpc_signature(url, &method, &empty).is_err(),
            "request with no auth headers must be rejected"
        );

        // Signature header present but garbage; timestamp is current.
        let mut forged = HeaderMap::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        forged.insert(SIGNATURE_HEADER, HeaderValue::from_static("not-a-real-signature"));
        forged.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&now.to_string()).unwrap());
        assert!(
            verify_rpc_signature(url, &method, &forged).is_err(),
            "request with a forged signature must be rejected"
        );

        // A validly signed request for a *different* URL must not authorize this one.
        let mut cross = HeaderMap::new();
        build_auth_headers("http://example.com/api/other", &method, &mut cross).expect("auth headers should build");
        assert!(
            verify_rpc_signature(url, &method, &cross).is_err(),
            "a signature bound to a different URL must not authorize this request"
        );

        // Control: a correctly signed request for this URL still succeeds, so the
        // gate is fail-closed rather than fail-everything.
        let mut valid = HeaderMap::new();
        build_auth_headers(url, &method, &mut valid).expect("auth headers should build");
        assert!(
            verify_rpc_signature(url, &method, &valid).is_ok(),
            "a correctly signed request must be accepted"
        );
    }

    #[test]
    fn test_get_shared_secret() {
        ensure_test_rpc_secret();
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        assert!(!secret.is_empty(), "Secret should not be empty");

        let url = "http://node1:7000/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        let url = "/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_generate_signature_deterministic() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200; // Fixed timestamp

        let signature1 = generate_signature(secret, url, &method, timestamp, false);
        let signature2 = generate_signature(secret, url, &method, timestamp, false);

        assert_eq!(signature1, signature2, "Same inputs should produce same signature");
        assert!(!signature1.is_empty(), "Signature should not be empty");
    }

    #[test]
    fn test_generate_signature_different_inputs() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200;

        let signature1 = generate_signature(secret, url, &method, timestamp, false);
        let signature2 = generate_signature(secret, "http://different.com/api/test2", &method, timestamp, false);
        let signature3 = generate_signature(secret, url, &Method::POST, timestamp, false);
        let signature4 = generate_signature(secret, url, &method, timestamp + 1, false);

        assert_ne!(signature1, signature2, "Different URLs should produce different signatures");
        assert_ne!(signature1, signature3, "Different methods should produce different signatures");
        assert_ne!(signature1, signature4, "Different timestamps should produce different signatures");
    }

    #[test]
    fn test_build_auth_headers() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::POST;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Verify headers are present
        assert!(headers.contains_key(SIGNATURE_HEADER), "Should contain signature header");
        assert!(headers.contains_key(TIMESTAMP_HEADER), "Should contain timestamp header");

        // Verify header values are not empty
        let signature = headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap();
        let timestamp_str = headers.get(TIMESTAMP_HEADER).unwrap().to_str().unwrap();

        assert!(!signature.is_empty(), "Signature should not be empty");
        assert!(!timestamp_str.is_empty(), "Timestamp should not be empty");

        // Verify timestamp is a valid integer
        let timestamp: i64 = timestamp_str.parse().expect("Timestamp should be valid integer");
        let current_time = OffsetDateTime::now_utc().unix_timestamp();

        // Should be within a reasonable range (within 1 second of current time)
        assert!((current_time - timestamp).abs() <= 1, "Timestamp should be close to current time");
    }

    #[test]
    fn test_build_auth_headers_preserves_existing_request_id() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("req-upstream-123"));

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        assert_eq!(headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()), Some("req-upstream-123"));
    }

    #[test]
    fn test_build_auth_headers_may_set_request_id_from_trace_id() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        let span = tracing::info_span!("rpc-test-span");
        let _guard = span.enter();
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        if let Some(value) = headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()) {
            assert!(!value.is_empty(), "request id should not be empty");
        }
    }

    #[test]
    fn test_verify_rpc_signature_success() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Verify should succeed
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_signature() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature first
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Tamper with the signature
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("invalid-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid signature should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_signature_uses_hmac_verification() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200;
        let signature = generate_signature(secret, url, &method, timestamp, false);
        let mut tampered = base64_simd::STANDARD.decode_to_vec(&signature).unwrap();
        tampered[0] ^= 1;
        let tampered_signature = base64_simd::STANDARD.encode_to_string(tampered);

        assert!(verify_signature(secret, url, &method, timestamp, &signature, false));
        assert!(!verify_signature(secret, url, &method, timestamp, &tampered_signature, false));
        assert!(!verify_signature(secret, url, &method, timestamp, "invalid-signature", false));
    }

    #[test]
    fn walk_dir_capability_is_covered_by_the_signature() {
        let secret = "test-secret";
        let signed_url = concat!(
            "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a&walk_dir_stream_completion=error-v1",
            "&walk_dir_body_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let downgraded_url = "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a";
        let tampered_body_digest = concat!(
            "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a&walk_dir_stream_completion=error-v1",
            "&walk_dir_body_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let method = Method::GET;
        let timestamp = 1_640_995_200;
        let signature = generate_signature(secret, signed_url, &method, timestamp, false);

        assert!(verify_signature(secret, signed_url, &method, timestamp, &signature, false));
        assert!(!verify_signature(secret, downgraded_url, &method, timestamp, &signature, false));
        assert!(!verify_signature(secret, tampered_body_digest, &method, timestamp, &signature, false));
    }

    #[test]
    fn test_invalid_signature_log_contract_excludes_secrets() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test?disk=/sensitive/path&token=private";
        let method = Method::GET;
        let timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let expected_signature = generate_signature(&secret, url, &method, timestamp, false);
        let invalid_signature = "invalid-signature";
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();

        let mut headers = HeaderMap::new();
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(invalid_signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());

        tracing::subscriber::with_default(subscriber, || {
            let result = verify_rpc_signature(url, &method, &headers);
            assert!(result.is_err(), "Invalid signature should fail verification");
        });

        let captured = logs.contents();
        assert!(captured.contains("Invalid signature"));
        assert!(!captured.contains(&secret));
        assert!(!captured.contains(&expected_signature));
        assert!(!captured.contains(invalid_signature));
        assert!(!captured.contains("sensitive"));
        assert!(!captured.contains("private"));
    }

    #[test]
    fn test_verify_rpc_signature_expired_timestamp() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Set expired timestamp (older than SIGNATURE_VALID_DURATION)
        let expired_timestamp = OffsetDateTime::now_utc().unix_timestamp() - SIGNATURE_VALID_DURATION - 10;
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let signature = generate_signature(&secret, url, &method, expired_timestamp, false);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&expired_timestamp.to_string()).unwrap());

        // Verify should fail due to expired timestamp
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Expired timestamp should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Request timestamp expired");
    }

    #[test]
    fn test_verify_rpc_signature_future_timestamp_outside_window() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        let future_timestamp = OffsetDateTime::now_utc().unix_timestamp() + SIGNATURE_VALID_DURATION + 10;
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let signature = generate_signature(&secret, url, &method, future_timestamp, false);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&future_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Future timestamp outside valid window should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Request timestamp expired");
    }

    #[test]
    fn test_verify_rpc_signature_missing_signature_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only timestamp header, missing signature
        let timestamp = OffsetDateTime::now_utc().unix_timestamp();
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing signature header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing signature header");
    }

    #[test]
    fn test_verify_rpc_signature_missing_timestamp_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only signature header, missing timestamp
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing timestamp header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing timestamp header");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_timestamp_format() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str("invalid-timestamp").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid timestamp format should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid timestamp format");
    }

    #[test]
    fn test_verify_rpc_signature_url_mismatch() {
        ensure_test_rpc_secret();
        let original_url = "http://example.com/api/test";
        let different_url = "http://example.com/api/different";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers for one URL
        build_auth_headers(original_url, &method, &mut headers).expect("auth headers should build");

        // Try to verify with a different URL
        let result = verify_rpc_signature(different_url, &method, &headers);
        assert!(result.is_err(), "URL mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_rpc_signature_method_mismatch() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let original_method = Method::GET;
        let different_method = Method::POST;
        let mut headers = HeaderMap::new();

        // Build headers for one method
        build_auth_headers(url, &original_method, &mut headers).expect("auth headers should build");

        // Try to verify with a different method
        let result = verify_rpc_signature(url, &different_method, &headers);
        assert!(result.is_err(), "Method mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_signature_valid_duration_boundary() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let secret = get_shared_secret().expect("test RPC secret should resolve");

        let mut headers = HeaderMap::new();
        let current_time = OffsetDateTime::now_utc().unix_timestamp();
        // Test timestamp just within valid duration
        let valid_timestamp = current_time - SIGNATURE_VALID_DURATION + 1;

        let signature = generate_signature(&secret, url, &method, valid_timestamp, false);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&valid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Timestamp within valid duration should pass");

        // Test timestamp just outside valid duration
        let mut headers = HeaderMap::new();
        let invalid_timestamp = current_time - SIGNATURE_VALID_DURATION - 15;
        let signature = generate_signature(&secret, url, &method, invalid_timestamp, false);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&invalid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Timestamp outside valid duration should fail");
    }

    #[test]
    fn test_round_trip_authentication() {
        ensure_test_rpc_secret();
        let test_cases = vec![
            ("http://example.com/api/test", Method::GET),
            ("https://api.rustfs.com/v1/bucket", Method::POST),
            ("http://localhost:9000/admin/info", Method::PUT),
            ("https://storage.example.com/path/to/object?query=param", Method::DELETE),
        ];

        for (url, method) in test_cases {
            let mut headers = HeaderMap::new();

            // Build authentication headers
            build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

            // Verify the signature should succeed
            let result = verify_rpc_signature(url, &method, &headers);
            assert!(result.is_ok(), "Round-trip test failed for {method} {url}");
        }
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_exact_method() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers).is_ok());
        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/SignalService", &headers)
            .expect_err("signature replayed to a different method must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_exact_service() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        let error = verify_tonic_rpc_signature("node-a:9000", "/other.NodeService/Ping", &headers)
            .expect_err("signature replayed to a different service must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_destination_audience() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        let error = verify_tonic_rpc_signature("node-b:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("signature replayed to a different node must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn replay_scope_binds_path_epoch_and_random_nonce() {
        ensure_test_rpc_secret();
        let path = "/node_service.NodeService/Ping";
        let mut headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("v2 compatibility headers should build");
        let timestamp = headers
            .get(TIMESTAMP_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("v2 timestamp")
            .to_string();
        let content_sha256 = headers
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("v2 content digest")
            .to_string();
        headers.extend(
            gen_tonic_replay_scope_headers("node-a:9000", path, &timestamp, &content_sha256, tonic_rpc_boot_epoch())
                .expect("replay-scope headers should build"),
        );

        assert!(
            verify_tonic_rpc_signature_with_policy("node-a:9000", path, &headers, false, false, false).is_ok(),
            "the first replay-scoped request must be accepted"
        );
        let replay = verify_tonic_rpc_signature_with_policy("node-a:9000", path, &headers, false, false, false)
            .expect_err("the random replay-scope nonce must be single-use");
        assert_eq!(replay.to_string(), "RPC request replay detected");

        let path_error = verify_tonic_replay_scope_signature("node-a:9000", "/node_service.NodeService/SignalService", &headers)
            .expect_err("a replay-scoped signature must not move to another method");
        assert_eq!(path_error.to_string(), "Invalid RPC replay scope signature");
    }

    #[test]
    fn replay_scope_consumes_the_body_bound_v2_nonce() {
        ensure_test_rpc_secret();
        let audience = "node-a:9000";
        let path = "/node_service.NodeService/RecoverDeleteTransaction";
        let digest = "a".repeat(64);
        for v3_first in [true, false] {
            let mut headers =
                gen_tonic_signature_headers(audience, "node_service.NodeService", "RecoverDeleteTransaction", Some(&digest))
                    .expect("body-bound v2 headers should build");
            let timestamp = headers[TIMESTAMP_HEADER].to_str().expect("timestamp").to_string();
            headers.extend(
                gen_tonic_replay_scope_headers(audience, path, &timestamp, &digest, tonic_rpc_boot_epoch())
                    .expect("replay-scope headers should build"),
            );
            let mut downgraded = headers.clone();
            for header in [
                RPC_REPLAY_SCOPE_VERSION_HEADER,
                RPC_REPLAY_SCOPE_SIGNATURE_HEADER,
                RPC_REPLAY_SCOPE_NONCE_HEADER,
                RPC_BOOT_EPOCH_HEADER,
            ] {
                downgraded.remove(header);
            }
            let (first, replay) = if v3_first {
                (&headers, &downgraded)
            } else {
                (&downgraded, &headers)
            };
            verify_tonic_rpc_signature_with_policy(audience, path, first, true, false, false)
                .expect("the first delivery must be accepted");
            let error = verify_tonic_rpc_signature_with_policy(audience, path, replay, true, false, false)
                .expect_err("a mutation cannot replay through the other signature generation");
            assert_eq!(error.to_string(), "RPC request replay detected");
        }
    }

    #[test]
    fn replay_scope_rejects_partial_metadata_and_stale_epoch_without_fallback() {
        ensure_test_rpc_secret();
        let path = "/node_service.NodeService/Ping";
        let mut partial = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("v2 compatibility headers should build");
        partial.insert(RPC_REPLAY_SCOPE_VERSION_HEADER, HeaderValue::from_static(RPC_REPLAY_SCOPE_VERSION_V3));
        let error = verify_tonic_rpc_signature_with_policy("node-a:9000", path, &partial, false, false, false)
            .expect_err("partial replay-scope metadata must never downgrade to v2");
        assert_eq!(error.to_string(), "Missing RPC replay scope signature");

        let timestamp = partial
            .get(TIMESTAMP_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("v2 timestamp")
            .to_string();
        let content_sha256 = partial
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("v2 content digest")
            .to_string();
        let stale_epoch = Uuid::new_v4();
        partial.extend(
            gen_tonic_replay_scope_headers("node-a:9000", path, &timestamp, &content_sha256, stale_epoch)
                .expect("replay-scope headers should build"),
        );
        let stale = verify_tonic_rpc_signature_with_policy("node-a:9000", path, &partial, false, false, false)
            .expect_err("a signature from a prior server boot epoch must be rejected");
        assert_eq!(stale.to_string(), "RPC boot epoch is stale");
    }

    #[test]
    fn replay_scope_strictness_allows_only_authenticated_ping_bootstrap() {
        ensure_test_rpc_secret();
        let mut headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("v2 compatibility headers should build");
        let rejected =
            verify_tonic_rpc_signature_with_policy("node-a:9000", "/node_service.NodeService/Ping", &headers, false, true, false)
                .expect_err("strict replay scope must reject stripped new metadata");
        assert_eq!(rejected.to_string(), "RPC replay-scoped authentication required");

        headers.insert(
            RPC_BOOT_EPOCH_CHALLENGE_HEADER,
            HeaderValue::from_str(&Uuid::new_v4().to_string()).expect("UUID header"),
        );
        assert!(
            verify_tonic_rpc_signature_with_policy("node-a:9000", "/node_service.NodeService/Ping", &headers, false, true, true,)
                .is_ok(),
            "only the signed Ping bootstrap may obtain a new server epoch in strict mode"
        );
    }

    #[test]
    fn boot_epoch_response_proof_binds_audience_challenge_and_epoch() {
        ensure_test_rpc_secret();
        let challenge = Uuid::new_v4();
        let headers = tonic_boot_epoch_response_headers("node-a:9000", challenge).expect("proof headers should build");
        let epoch =
            verify_tonic_boot_epoch_response("node-a:9000", challenge, &headers).expect("matching proof headers should verify");
        assert_eq!(epoch, tonic_rpc_boot_epoch());
        assert!(verify_tonic_boot_epoch_response("node-b:9000", challenge, &headers).is_err());
        assert!(verify_tonic_boot_epoch_response("node-a:9000", Uuid::new_v4(), &headers).is_err());
    }

    #[test]
    fn replay_cache_capability_proof_binds_audience_challenge_epoch_and_value() {
        ensure_test_rpc_secret();
        let challenge = Uuid::new_v4();
        let headers = tonic_boot_epoch_response_headers("node-a:9000", challenge).expect("capability headers should build");
        let capabilities = verify_tonic_peer_replay_capabilities_response("node-a:9000", challenge, &headers)
            .expect("matching capability proof should verify");
        assert_eq!(capabilities.boot_epoch, tonic_rpc_boot_epoch());
        assert!(capabilities.dynamic_replay_cache);
        assert!(verify_tonic_peer_replay_capabilities_response("node-b:9000", challenge, &headers).is_err());
        assert!(verify_tonic_peer_replay_capabilities_response("node-a:9000", Uuid::new_v4(), &headers).is_err());

        let mut changed_capability = headers;
        changed_capability.insert(RPC_REPLAY_CACHE_CAPABILITY_HEADER, HeaderValue::from_static("dynamic-replay-cache-v2"));
        assert!(verify_tonic_peer_replay_capabilities_response("node-a:9000", challenge, &changed_capability).is_err());
    }

    #[test]
    fn tonic_rpc_auth_failure_reason_maps_security_relevant_errors() {
        for (message, reason) in [
            ("Invalid RPC v2 signature", "invalid_v2_signature"),
            ("RPC replay-scoped authentication required", "replay_scope_required"),
            ("Missing RPC replay scope signature", "missing_replay_scope_signature"),
            ("RPC boot epoch is stale", "stale_boot_epoch"),
            ("RPC request replay detected", "replay_detected"),
            ("Request timestamp expired", "timestamp_expired"),
            ("Missing RPC content SHA-256", "missing_content_sha256"),
            ("Invalid RPC content SHA-256", "invalid_content_sha256"),
            ("Invalid put_file auth trailer length", "invalid_put_file_auth_trailer_length"),
            ("Invalid put_file auth trailer", "invalid_put_file_auth_trailer"),
        ] {
            assert_eq!(
                tonic_rpc_auth_failure_reason(&std::io::Error::other(message)),
                reason,
                "message {message:?} should map to a stable low-cardinality reason"
            );
        }
    }

    #[test]
    fn tonic_rpc_auth_failure_reason_falls_back_for_unclassified_errors() {
        assert_eq!(tonic_rpc_auth_failure_reason(&std::io::Error::other("opaque failure")), "unknown");
    }

    #[test]
    fn malformed_v2_auth_does_not_downgrade_to_valid_legacy_signature() {
        ensure_test_rpc_secret();
        let mut headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");
        headers.insert(RPC_SIGNATURE_V2_HEADER, HeaderValue::from_static("invalid"));

        assert!(
            verify_rpc_signature(TONIC_RPC_PREFIX, &Method::GET, &headers).is_ok(),
            "the compatibility signature should remain valid for old servers"
        );
        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("new servers must not downgrade malformed v2 auth");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    // The `rpc_v1_fallback_counter` serial group covers every test that drives (or asserts on) the
    // process-global v1-fallback counter, so exact-delta assertions cannot race with each other.
    #[test]
    #[serial_test::serial(rpc_v1_fallback_counter)]
    fn legacy_tonic_signature_remains_accepted_during_rolling_upgrade() {
        ensure_test_rpc_secret();
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers).is_ok());
    }

    #[test]
    #[serial_test::serial(rpc_v1_fallback_counter)]
    fn accepted_legacy_fallback_increments_v1_fallback_counter() {
        ensure_test_rpc_secret();
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");
        let before = global_internode_metrics().snapshot().signature_v1_fallback_total;

        assert!(
            verify_tonic_rpc_signature_with_strictness("node-a:9000", "/node_service.NodeService/Ping", &headers, false).is_ok(),
            "a legacy-only peer must keep authenticating while the strict gate is off"
        );

        let after = global_internode_metrics().snapshot().signature_v1_fallback_total;
        assert_eq!(
            after,
            before + 1,
            "an accepted legacy-only request must increment the v1 fallback counter exactly once"
        );
    }

    #[test]
    #[serial_test::serial(rpc_v1_fallback_counter)]
    fn rejected_legacy_fallback_does_not_count_as_v1_fallback() {
        ensure_test_rpc_secret();
        // Legacy-shaped headers with a forged signature: the fallback path runs but must reject,
        // and a rejected request is not a rollout-convergence signal.
        let mut headers = HeaderMap::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_static("not-a-real-signature"));
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&now.to_string()).unwrap());
        let before = global_internode_metrics().snapshot().signature_v1_fallback_total;

        assert!(
            verify_tonic_rpc_signature_with_strictness("node-a:9000", "/node_service.NodeService/Ping", &headers, false).is_err(),
            "a forged legacy signature must still be rejected"
        );

        let after = global_internode_metrics().snapshot().signature_v1_fallback_total;
        assert_eq!(after, before, "a rejected legacy request must not count as an accepted fallback");
    }

    #[test]
    #[serial_test::serial(rpc_v1_fallback_counter)]
    fn strict_gate_rejects_legacy_only_auth_but_keeps_v2() {
        ensure_test_rpc_secret();
        let legacy = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");
        let before = global_internode_metrics().snapshot().signature_v1_fallback_total;
        let error = verify_tonic_rpc_signature_with_strictness("node-a:9000", "/node_service.NodeService/Ping", &legacy, true)
            .expect_err("strict mode must reject legacy-only authentication");
        assert_eq!(error.to_string(), "RPC v2 authentication required");

        let v2 = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");
        assert!(
            verify_tonic_rpc_signature_with_strictness("node-a:9000", "/node_service.NodeService/Ping", &v2, true).is_ok(),
            "strict mode must keep accepting v2-authenticated peers"
        );
        let after = global_internode_metrics().snapshot().signature_v1_fallback_total;
        assert_eq!(after, before, "neither a strict rejection nor a v2 acceptance is a legacy fallback");
    }

    #[test]
    #[serial_test::serial(rpc_v1_fallback_counter)]
    fn strict_gate_default_posture_is_fail_open_legacy_accept() {
        ensure_test_rpc_secret();
        // The public entry point resolves strictness from the environment, whose compile-time
        // default is pinned to false in `rustfs_config`. A legacy-only peer therefore keeps
        // authenticating through the default build with no configuration at all.
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");
        assert!(
            verify_tonic_rpc_signature_with_strictness(
                "node-a:9000",
                "/node_service.NodeService/Ping",
                &headers,
                rustfs_config::DEFAULT_INTERNODE_RPC_SIGNATURE_STRICT,
            )
            .is_ok(),
            "the default strict posture must accept legacy-only peers"
        );
    }

    #[test]
    fn body_bound_tonic_request_rejects_replay_and_body_tampering() {
        ensure_test_rpc_secret();
        let body = b"heal-control-request";
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers =
            gen_tonic_signature_headers("node-a:9000", "node_service.HealControlService", "HealControl", content_sha256)
                .expect("body-bound auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.HealControlService/HealControl", &headers).is_ok());
        let replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.HealControlService/HealControl", &headers)
            .expect_err("reusing a body-bound nonce must fail");
        assert_eq!(replay.to_string(), "RPC request replay detected");
        assert!(verify_tonic_canonical_body_digest(&request, body).is_ok());
        let tampered = verify_tonic_canonical_body_digest(&request, b"different-body")
            .expect_err("a different canonical request body must fail");
        assert_eq!(tampered.to_string(), "RPC content SHA-256 mismatch");
    }

    #[test]
    fn put_file_auth_trailer_binds_url_nonce_and_body_digest() {
        ensure_test_rpc_secret();
        let url = concat!(
            "/rustfs/rpc/put_file_stream?disk=disk-a&volume=bucket&path=object%2Fpart.1",
            "&append=false&size=11&put_file_auth=digest-trailer-v1&put_file_nonce=11111111-2222-4333-8444-555555555555"
        );
        let nonce = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("nonce");
        let body_sha256 = hex_simd::encode_to_string(Sha256::digest(b"hello world"), hex_simd::AsciiCase::Lower);
        let trailer = build_put_file_auth_trailer(url, &Method::PUT, nonce, &body_sha256).expect("trailer should build");

        assert_eq!(trailer.len(), PUT_FILE_AUTH_TRAILER_LEN);
        let verified = verify_put_file_auth_trailer(url, &Method::PUT, nonce, &trailer).expect("trailer should verify");
        assert_eq!(verified, body_sha256);

        let different_url = url.replace("size=11", "size=12");
        let err = verify_put_file_auth_trailer(&different_url, &Method::PUT, nonce, &trailer)
            .expect_err("trailer must bind the signed URL");
        assert_eq!(err.to_string(), "Invalid put_file auth trailer");

        let err =
            verify_put_file_auth_trailer(url, &Method::PUT, Uuid::new_v4(), &trailer).expect_err("trailer must bind the nonce");
        assert_eq!(err.to_string(), "Invalid put_file auth trailer");

        let mut tampered = trailer;
        tampered[PUT_FILE_AUTH_TRAILER_MAGIC.len()] = b'0';
        let err =
            verify_put_file_auth_trailer(url, &Method::PUT, nonce, &tampered).expect_err("trailer must bind the digest bytes");
        assert_eq!(err.to_string(), "Invalid put_file auth trailer");
    }

    #[test]
    fn put_file_capability_proof_binds_challenge_epoch_and_version() {
        ensure_test_rpc_secret();
        let challenge = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("challenge");
        let server_epoch = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").expect("server epoch");
        let proof = sign_put_file_capability(challenge, server_epoch, PUT_FILE_CAPABILITY_VERSION)
            .expect("capability proof should build");

        assert!(verify_put_file_capability(challenge, server_epoch, PUT_FILE_CAPABILITY_VERSION, &proof).is_ok());
        assert!(verify_put_file_capability(Uuid::new_v4(), server_epoch, PUT_FILE_CAPABILITY_VERSION, &proof).is_err());
        assert!(verify_put_file_capability(challenge, Uuid::new_v4(), PUT_FILE_CAPABILITY_VERSION, &proof).is_err());
        assert!(verify_put_file_capability(challenge, server_epoch, PUT_FILE_CAPABILITY_VERSION + 1, &proof).is_err());
    }

    #[test]
    fn tier_mutation_rpc_contract_requires_method_bound_v2_body_digest() {
        ensure_test_rpc_secret();
        let mutation_id = uuid::uuid!("12345678-1234-5678-9abc-def012345678");
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            b"canonical-tier-mutation-prepare",
        )
        .expect("small tier mutation body should encode");
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, &body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers = gen_tonic_signature_headers(
            "node-a:9000",
            "node_service.TierMutationControlService",
            "PrepareTierMutation",
            content_sha256,
        )
        .expect("body-bound tier mutation auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(
            verify_tonic_rpc_signature("node-a:9000", "/node_service.TierMutationControlService/PrepareTierMutation", &headers)
                .is_ok(),
            "tier mutation RPC signature must bind destination, service, method, nonce, and body digest"
        );
        let method_replay =
            verify_tonic_rpc_signature("node-a:9000", "/node_service.TierMutationControlService/CommitTierMutation", &headers)
                .expect_err("prepare auth must not replay to commit");
        assert_eq!(method_replay.to_string(), "Invalid RPC v2 signature");
        let service_replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/PrepareTierMutation", &headers)
            .expect_err("tier mutation auth must not replay to the legacy node service path");
        assert_eq!(service_replay.to_string(), "Invalid RPC v2 signature");
        let tampered_body = rustfs_protos::canonical_tier_mutation_rpc_body(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            rustfs_protos::TierMutationRpcPhase::Commit,
            mutation_id,
            b"canonical-tier-mutation-prepare",
        )
        .expect("small tier mutation body should encode");
        let tampered =
            verify_tonic_canonical_body_digest(&request, &tampered_body).expect_err("commit body must not match prepare digest");
        assert_eq!(tampered.to_string(), "RPC content SHA-256 mismatch");
    }

    #[test]
    fn partial_v2_metadata_fails_closed() {
        ensure_test_rpc_secret();
        let mut headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");
        headers.insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));

        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("partial v2 metadata must not fall back to legacy auth");
        assert_eq!(error.to_string(), "Missing RPC v2 signature");
    }

    #[test]
    fn canonical_mutation_digest_rejects_legacy_only_auth() {
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, b"heal-control-v1\0start").expect("canonical digest should be attached");

        let error = verify_tonic_canonical_body_digest(&request, b"heal-control-v1\0start")
            .expect_err("mutation body verification must also require v2 auth");
        assert_eq!(error.to_string(), "RPC mutation requires v2 authentication");
    }

    #[test]
    fn tonic_rpc_metric_operation_classifies_get_hot_path_methods() {
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/ReadAll"),
            INTERNODE_OPERATION_GRPC_READ_ALL
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/ReadMultiple"),
            INTERNODE_OPERATION_GRPC_READ_MULTIPLE
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/ReadVersion"),
            INTERNODE_OPERATION_GRPC_READ_VERSION
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/BatchReadVersion"),
            INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/WriteAll"),
            INTERNODE_OPERATION_GRPC_WRITE_ALL
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/CompareAndUpdateFile"),
            INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/Lock"),
            INTERNODE_OPERATION_GRPC_LOCK
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/UnLock"),
            INTERNODE_OPERATION_GRPC_UNLOCK
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/LockBatch"),
            INTERNODE_OPERATION_GRPC_LOCK_BATCH
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/UnLockBatch"),
            INTERNODE_OPERATION_GRPC_UNLOCK_BATCH
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/Refresh"),
            INTERNODE_OPERATION_GRPC_REFRESH
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/ForceUnLock"),
            INTERNODE_OPERATION_GRPC_FORCE_UNLOCK
        );
        assert_eq!(
            tonic_rpc_metric_operation("/node_service.NodeService/SignalService"),
            INTERNODE_OPERATION_GRPC_OTHER
        );
        assert_eq!(tonic_rpc_metric_operation("not-a-grpc-path"), INTERNODE_OPERATION_GRPC_OTHER);
    }

    #[test]
    fn replay_cache_preserves_explicit_count_compatibility_without_an_auto_budget() {
        for capacity in [1, 16_777_216, usize::MAX] {
            let decision = replay_cache_capacity_decision(
                rustfs_utils::EnvParseOutcome::Parsed(capacity),
                2,
                Some(512 * 1024 * 1024),
                Some(MemoryBasis::Cgroup),
            );
            let default = rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY;
            assert_eq!(decision.capacity, capacity.max(default));
            assert_eq!(
                decision.source,
                if capacity < default {
                    ReplayCacheCapacitySource::EnvClampedToDefault
                } else {
                    ReplayCacheCapacitySource::Env
                }
            );
            assert_eq!(decision.memory_budget_bytes, None);
        }
        let zero = replay_cache_capacity_decision(rustfs_utils::EnvParseOutcome::Parsed(0), 2, None, None);
        assert_eq!(zero.capacity, rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY);
        assert_eq!(zero.source, ReplayCacheCapacitySource::EnvClampedToDefault);
    }

    #[test]
    fn replay_cache_auto_admission_uses_memory_without_cpu_or_32m_limits() {
        for memory in [128_u64, 512, 2800, 8192, 32_768, 524_288] {
            let memory = memory * 1024 * 1024;
            let a =
                replay_cache_capacity_decision(rustfs_utils::EnvParseOutcome::Absent, 1, Some(memory), Some(MemoryBasis::Cgroup));
            let b = replay_cache_capacity_decision(
                rustfs_utils::EnvParseOutcome::Absent,
                128,
                Some(memory),
                Some(MemoryBasis::Cgroup),
            );
            assert_eq!(a.capacity, b.capacity);
            assert_eq!(a.memory_budget_bytes, Some(replay_cache_memory_budget(memory)));
            assert_eq!(
                u64::try_from(a.capacity).expect("capacity") * 16,
                replay_cache_memory_budget(memory).saturating_sub(REPLAY_CACHE_METADATA_BYTES) / 16 * 16
            );
            assert_eq!(a.source, ReplayCacheCapacitySource::Auto);
        }
        let tiny = replay_cache_capacity_decision(
            rustfs_utils::EnvParseOutcome::Absent,
            128,
            Some(16 * 1024 * 1024),
            Some(MemoryBasis::Cgroup),
        );
        assert!(
            tiny.capacity < rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY,
            "a historical floor must not exceed the detected memory budget"
        );
        let large = replay_cache_capacity_decision(
            rustfs_utils::EnvParseOutcome::Absent,
            1,
            Some(512 * 1024_u64.pow(3)),
            Some(MemoryBasis::Cgroup),
        );
        assert!(large.capacity > 33_554_432, "a fixed historical count is not a memory limit");
    }

    #[test]
    fn replay_cache_unknown_memory_retains_a_bounded_fallback() {
        let decision = replay_cache_capacity_decision(rustfs_utils::EnvParseOutcome::Invalid, 128, None, None);
        assert_eq!(decision.capacity, rustfs_config::DEFAULT_INTERNODE_RPC_REPLAY_CACHE_CAPACITY);
        assert_eq!(decision.source, ReplayCacheCapacitySource::AutoInvalidEnv);
        assert_eq!(decision.memory_budget_bytes, None);
        assert_eq!(replay_cache_memory_budget(u64::MAX), u64::MAX / 100 * 13 + u64::MAX % 100 * 13 / 100);
    }

    fn check_test_nonce_record(cache: &mut RpcNonceCache, record: RpcNonceRecord<'_>) -> std::io::Result<()> {
        let (result, metrics) = cache.check_and_record(record);
        publish_nonce_cache_metrics(metrics);
        result
    }

    fn check_test_nonce_record_with_metrics<'a>(
        cache: &mut RpcNonceCache,
        record: RpcNonceRecord<'a>,
    ) -> (std::io::Result<()>, Option<RpcNonceCacheMetrics<'a>>) {
        cache.check_and_record(record)
    }

    fn test_nonce_record(
        nonce: Uuid,
        signed_at: i64,
        _now: Instant,
        wall_time: i64,
        _expires_at: Instant,
        capacity: usize,
    ) -> RpcNonceRecord<'static> {
        RpcNonceRecord {
            nonce,
            compatibility_nonce: None,
            signed_at,
            wall_time,
            capacity,
            memory_budget_bytes: None,
            metric_scope: RpcReplayCacheMetricScope {
                operation: INTERNODE_OPERATION_GRPC_READ_ALL,
                backend: INTERNODE_TRANSPORT_BACKEND_GRPC,
                rpc_path: "/node_service.NodeService/ReadAll",
            },
        }
    }

    #[test]
    fn nonce_pair_count_admission_is_atomic_and_recovers_after_expiration() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        cache
            .check_and_record(test_nonce_record(Uuid::from_u128(1), 1000, now, 1000, now, 2))
            .0
            .expect("existing identity");
        let mut pair = test_nonce_record(Uuid::from_u128(2), 1000, now, 1000, now, 2);
        pair.compatibility_nonce = Some(Uuid::from_u128(3));
        let (result, metrics) = cache.check_and_record(pair);
        assert!(rpc_auth_is_resource_exhausted(
            &result.expect_err("one remaining slot cannot admit a pair")
        ));
        assert_eq!(cache.entries, 1);
        assert!(!cache.contains_nonce(&pair.nonce));
        assert!(!cache.contains_nonce(&pair.compatibility_nonce.expect("alias")));
        assert_eq!(metrics.expect("rejection metrics").recorded, 0);
        pair.signed_at = 1301;
        pair.wall_time = 1301;
        let (result, metrics) = cache.check_and_record(pair);
        result.expect("expired history releases both required slots");
        assert_eq!(cache.entries, 2);
        assert_eq!(metrics.expect("success metrics").recorded, 2);
        assert!(cache.contains_nonce(&pair.nonce));
        assert!(cache.contains_nonce(&pair.compatibility_nonce.expect("alias")));
    }

    #[test]
    fn nonce_pair_reserves_the_growth_peak_before_consuming_either_identity() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        for nonce in 1..=2 {
            let mut record = test_nonce_record(Uuid::from_u128(nonce), 1000, now, 1000, now, 10);
            record.memory_budget_bytes = Some(REPLAY_CACHE_METADATA_BYTES + 1000);
            cache.check_and_record(record).0.expect("small table");
        }
        let bytes = cache.table_bytes;
        let mut pair = test_nonce_record(Uuid::from_u128(3), 1000, now, 1000, now, 10);
        pair.compatibility_nonce = Some(Uuid::from_u128(4));
        pair.memory_budget_bytes = Some(REPLAY_CACHE_METADATA_BYTES + 1000);
        let error = cache
            .check_and_record(pair)
            .0
            .expect_err("the first identity fits the old table but the complete pair needs growth");
        assert_eq!(tonic_rpc_auth_failure_reason(&error), "replay_cache_memory");
        assert_eq!(cache.entries, 2);
        assert_eq!(cache.table_bytes, bytes);
        assert!(!cache.contains_nonce(&pair.nonce));
        assert!(!cache.contains_nonce(&pair.compatibility_nonce.expect("alias")));
    }

    #[test]
    fn nonce_pair_replay_or_duplicate_does_not_consume_a_fresh_identity() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        let mut pair = test_nonce_record(Uuid::from_u128(1), 1000, now, 1000, now, 10);
        pair.compatibility_nonce = Some(pair.nonce);
        assert_eq!(
            cache
                .check_and_record(pair)
                .0
                .expect_err("duplicate aliases are not a valid pair")
                .to_string(),
            "RPC request replay detected"
        );
        assert!(cache.signed_times.is_empty());
        pair.compatibility_nonce = Some(Uuid::from_u128(2));
        cache.check_and_record(pair).0.expect("first complete pair");
        pair.nonce = Uuid::from_u128(3);
        assert_eq!(
            cache
                .check_and_record(pair)
                .0
                .expect_err("a consumed v2 alias must reject the whole new pair")
                .to_string(),
            "RPC request replay detected"
        );
        assert_eq!(cache.entries, 2);
        assert!(!cache.contains_nonce(&pair.nonce));
    }

    #[test]
    fn concurrent_nonce_pairs_admit_one_complete_request_without_partial_reservations() {
        let now = Instant::now();
        let cache = std::sync::Mutex::new(RpcNonceCache::default());
        let outcomes = std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for index in 0..16 {
                let cache = &cache;
                joins.push(scope.spawn(move || {
                    let mut pair = test_nonce_record(Uuid::from_u128(100 + index * 2), 1000, now, 1000, now, 2);
                    pair.compatibility_nonce = Some(Uuid::from_u128(101 + index * 2));
                    let accepted = cache.lock().expect("cache lock").check_and_record(pair).0.is_ok();
                    (pair, accepted)
                }));
            }
            joins
                .into_iter()
                .map(|join| join.join().expect("admission task"))
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|(_, accepted)| *accepted).count(), 1);
        let cache = cache.lock().expect("final cache state");
        assert_eq!(cache.entries, 2);
        for (pair, accepted) in outcomes {
            assert_eq!(cache.contains_nonce(&pair.nonce), accepted);
            assert_eq!(cache.contains_nonce(&pair.compatibility_nonce.expect("alias")), accepted);
        }
    }

    #[test]
    fn malformed_compatibility_signature_does_not_consume_valid_replay_scope() {
        ensure_test_rpc_secret();
        let audience = "atomic-admission-test:9000";
        let path = "/node_service.NodeService/ReadAll";
        let body = "cd".repeat(32);
        let mut headers = gen_tonic_signature_headers(audience, "node_service.NodeService", "ReadAll", Some(&body))
            .expect("body-bound v2 authorization");
        let timestamp = headers[TIMESTAMP_HEADER].to_str().expect("timestamp").to_owned();
        headers.extend(
            gen_tonic_replay_scope_headers(audience, path, &timestamp, &body, tonic_rpc_boot_epoch()).expect("v3 authorization"),
        );
        let mut malformed = headers.clone();
        malformed.insert(RPC_SIGNATURE_V2_HEADER, HeaderValue::from_static("invalid"));
        assert_eq!(
            verify_tonic_rpc_signature(audience, path, &malformed)
                .expect_err("malformed alias must be rejected")
                .to_string(),
            "Invalid RPC v2 signature"
        );
        verify_tonic_rpc_signature(audience, path, &headers)
            .expect("a failed signature check must not consume either valid authorization");
        assert_eq!(
            verify_tonic_rpc_signature(audience, path, &headers)
                .expect_err("the valid request remains single-use")
                .to_string(),
            "RPC request replay detected"
        );
    }

    #[test]
    fn timestamp_bucket_identity_requires_a_fresh_valid_signature() {
        ensure_test_rpc_secret();
        let audience = "time-bucket-test:9000";
        let service = "node_service.NodeService";
        let method = "ReadAll";
        let path = "/node_service.NodeService/ReadAll";
        let body = "ab".repeat(32);
        for replay_scoped in [false, true] {
            let mut headers = gen_tonic_signature_headers(audience, service, method, Some(&body)).expect("body-bound v2 headers");
            let first_time = headers[TIMESTAMP_HEADER].to_str().expect("time").to_owned();
            if replay_scoped {
                headers.extend(
                    gen_tonic_replay_scope_headers(audience, path, &first_time, &body, tonic_rpc_boot_epoch())
                        .expect("v3 headers"),
                );
            }
            verify_tonic_rpc_signature(audience, path, &headers).expect("first authorization");
            let second_time = (first_time.parse::<i64>().expect("numeric time") + 1).to_string();
            headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&second_time).expect("new time header"));
            assert!(
                verify_tonic_rpc_signature(audience, path, &headers).is_err(),
                "a captured authorization cannot be moved to another bucket without signing it again"
            );
            let secret = get_shared_secret().expect("test signing key");
            let signature = generate_signature_v2(
                &secret,
                SignatureV2Scope {
                    background: false,
                    audience,
                    service,
                    rpc_method: method,
                    timestamp: &second_time,
                    nonce: headers[RPC_NONCE_HEADER].to_str().expect("nonce"),
                    content_sha256: &body,
                },
            )
            .expect("fresh v2 authorization");
            headers.insert(RPC_SIGNATURE_V2_HEADER, HeaderValue::from_str(&signature).expect("new v2 MAC"));
            if replay_scoped {
                let nonce = headers[RPC_REPLAY_SCOPE_NONCE_HEADER]
                    .to_str()
                    .expect("v3 nonce")
                    .parse()
                    .expect("UUID");
                let signature = generate_replay_scope_signature(
                    &secret,
                    ReplayScope {
                        background: false,
                        audience,
                        path,
                        timestamp: &second_time,
                        nonce,
                        content_sha256: &body,
                        boot_epoch: tonic_rpc_boot_epoch(),
                    },
                )
                .expect("fresh v3 authorization");
                headers.insert(RPC_REPLAY_SCOPE_SIGNATURE_HEADER, HeaderValue::from_str(&signature).expect("new v3 MAC"));
            }
            verify_tonic_rpc_signature(audience, path, &headers).expect("independently signed time is a new authorization");
            assert_eq!(
                verify_tonic_rpc_signature(audience, path, &headers)
                    .expect_err("new authorization remains single-use")
                    .to_string(),
                "RPC request replay detected"
            );
            if replay_scoped {
                headers.remove(RPC_REPLAY_SCOPE_VERSION_HEADER);
                headers.remove(RPC_REPLAY_SCOPE_SIGNATURE_HEADER);
                headers.remove(RPC_REPLAY_SCOPE_NONCE_HEADER);
                headers.remove(RPC_BOOT_EPOCH_HEADER);
                assert_eq!(
                    verify_tonic_rpc_signature(audience, path, &headers)
                        .expect_err("stripping v3 must not resurrect its consumed v2 alias")
                        .to_string(),
                    "RPC request replay detected"
                );
            }
        }
    }

    #[test]
    fn replay_resource_classification_preserves_identity_failures() {
        for source in [
            RpcReplayCacheAdmissionError::Capacity,
            RpcReplayCacheAdmissionError::MemoryBudget,
        ] {
            assert!(rpc_auth_is_resource_exhausted(&std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                source
            )));
        }
        assert!(rpc_auth_is_resource_exhausted(&std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            hashbrown::TryReserveError::CapacityOverflow
        )));
        for error in [
            std::io::Error::other("RPC request replay detected"),
            std::io::Error::new(std::io::ErrorKind::WouldBlock, "unrelated failure"),
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, "unrelated failure"),
        ] {
            assert!(!rpc_auth_is_resource_exhausted(&error));
        }
    }

    #[test]
    fn nonce_cache_releases_expired_timestamp_capacity_without_allowing_clock_rollback_replay() {
        let now = Instant::now();
        let expiry = now + REPLAY_CACHE_RETENTION;
        let later = now + Duration::from_secs(302);
        let nonce = Uuid::new_v4();
        let replacement = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();
        cache
            .check_and_record(test_nonce_record(nonce, 1_000, now, 1_000, expiry, 1))
            .0
            .expect("record initial nonce");

        let boundary = cache
            .check_and_record(test_nonce_record(nonce, 1_000, later, 1_300, later + REPLAY_CACHE_RETENTION, 1))
            .0
            .expect_err("a signature at the inclusive freshness boundary must remain protected");
        assert_eq!(boundary.to_string(), "RPC request replay detected");
        cache
            .check_and_record(test_nonce_record(replacement, 1_302, later, 1_302, later + REPLAY_CACHE_RETENTION, 1))
            .0
            .expect("an expired signature must release bounded cache capacity");
        assert!(!cache.contains_nonce(&nonce));
        assert!(cache.contains_nonce(&replacement));

        let rollback = cache
            .check_and_record(test_nonce_record(nonce, 1_000, later, 1_000, later + REPLAY_CACHE_RETENTION, 1))
            .0
            .expect_err("clock rollback must not resurrect an evicted signature");
        assert_eq!(rollback.to_string(), "RPC request timestamp expired after clock regression");
    }

    #[test]
    fn nonce_cache_keeps_future_timestamp_reserved_until_its_full_validity_window_closes() {
        let now = Instant::now();
        let expiry = now + REPLAY_CACHE_RETENTION;
        let later = now + Duration::from_secs(302);
        let nonce = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();
        cache
            .check_and_record(test_nonce_record(nonce, 1_300, now, 1_000, expiry, 1))
            .0
            .expect("record nonce signed at maximum allowed forward skew");
        let replay = cache
            .check_and_record(test_nonce_record(nonce, 1_300, later, 1_302, later + REPLAY_CACHE_RETENTION, 1))
            .0
            .expect_err("monotonic expiry must not remove a still-valid future signature");
        assert_eq!(replay.to_string(), "RPC request replay detected");
        let full = cache
            .check_and_record(test_nonce_record(Uuid::new_v4(), 1_302, later, 1_302, later + REPLAY_CACHE_RETENTION, 1))
            .0
            .expect_err("a still-valid nonce must continue consuming capacity");
        assert_eq!(full.to_string(), "RPC replay cache capacity exceeded");
        let expired = now + Duration::from_secs(602);
        cache
            .check_and_record(test_nonce_record(
                Uuid::new_v4(),
                1_602,
                expired,
                1_602,
                expired + REPLAY_CACHE_RETENTION,
                1,
            ))
            .0
            .expect("release capacity only once future timestamp validity has closed");
    }

    #[test]
    fn future_signed_nonce_does_not_block_expired_ordinary_capacity() {
        let start = Instant::now();
        let mut cache = RpcNonceCache::default();
        let future = Uuid::from_u128(1);
        let ordinary = Uuid::from_u128(2);
        cache
            .check_and_record(test_nonce_record(future, 1300, start, 1000, start + REPLAY_CACHE_RETENTION, 2))
            .0
            .expect("accepted forward timestamp");
        let next = start + Duration::from_secs(1);
        cache
            .check_and_record(test_nonce_record(ordinary, 1001, next, 1001, next + REPLAY_CACHE_RETENTION, 2))
            .0
            .expect("ordinary timestamp");
        let later = start + Duration::from_secs(303);
        cache
            .check_and_record(test_nonce_record(
                Uuid::from_u128(3),
                1303,
                later,
                1303,
                later + REPLAY_CACHE_RETENTION,
                2,
            ))
            .0
            .expect("the expired ordinary signature must release capacity despite the future signature");
        assert!(cache.contains_nonce(&future), "the future signature is still replay-protected");
        assert!(!cache.contains_nonce(&ordinary));
        let replay = cache
            .check_and_record(test_nonce_record(future, 1300, later, 1303, later + REPLAY_CACHE_RETENTION, 2))
            .0
            .expect_err("the future signature cannot be reused");
        assert_eq!(replay.to_string(), "RPC request replay detected");
    }

    #[test]
    fn nonce_cache_byte_budget_stays_bounded_during_expiry_churn() {
        let start = Instant::now();
        let budget = REPLAY_CACHE_METADATA_BYTES + 600_000;
        let mut cache = RpcNonceCache::default();
        for n in 0..32_768 {
            let signed = 1000 + i64::from(n / 13);
            let mut record =
                test_nonce_record(Uuid::from_u128(u128::try_from(n + 1).expect("nonce")), signed, start, signed, start, 4096);
            record.memory_budget_bytes = Some(budget);
            cache.check_and_record(record).0.expect("bounded live window should fit");
            assert!(cache.table_bytes + REPLAY_CACHE_METADATA_BYTES <= budget);
            assert!(cache.entries <= 4096);
            assert!(cache.signed_times.len() <= REPLAY_CACHE_CAPACITY_WINDOW_SECS);
        }
        let remaining = cache.entries;
        assert_eq!(cache.remove_expired(10_000), remaining);
        assert_eq!(cache.entries, 0);
        assert_eq!(cache.table_bytes, 0);
    }

    #[test]
    fn explicit_expiration_keeps_the_clock_rollback_fence() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        let nonce = Uuid::from_u128(1);
        cache
            .check_and_record(test_nonce_record(nonce, 1000, now, 1000, now, 1))
            .0
            .expect("initial signature");
        assert_eq!(cache.remove_expired(1301), 1);
        let error = cache
            .check_and_record(test_nonce_record(nonce, 1000, now, 1000, now, 1))
            .0
            .expect_err("cleanup must preserve the admission fence when the wall clock rolls back");
        assert_eq!(error.to_string(), "RPC request timestamp expired after clock regression");
    }

    #[test]
    fn nonce_cache_large_logical_capacity_does_not_eagerly_allocate_it() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        cache
            .check_and_record(test_nonce_record(Uuid::from_u128(1), 1000, now, 1000, now, usize::MAX))
            .0
            .expect("one nonce under a large operator ceiling");
        assert_eq!(cache.entries, 1);
        assert!(cache.table_bytes < 4096);
        assert!(replay_cache_table_growth_bytes(usize::MAX).is_none());
    }

    #[test]
    fn nonce_cache_checks_old_and_new_table_peak_before_growth() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        for nonce in 1..=3 {
            let mut record = test_nonce_record(Uuid::from_u128(nonce), 1000, now, 1000, now, 4096);
            record.memory_budget_bytes = Some(REPLAY_CACHE_METADATA_BYTES + 1000);
            cache.check_and_record(record).0.expect("small table fits");
        }
        let retained = cache.table_bytes;
        let mut fourth = test_nonce_record(Uuid::from_u128(4), 1000, now, 1000, now, 4096);
        fourth.memory_budget_bytes = Some(REPLAY_CACHE_METADATA_BYTES + 1000);
        let error = cache
            .check_and_record(fourth)
            .0
            .expect_err("old plus new table exceeds budget even though the replacement alone fits");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(tonic_rpc_auth_failure_reason(&error), "replay_cache_memory");
        assert!(rpc_auth_is_resource_exhausted(&error));
        assert_eq!(cache.entries, 3);
        assert_eq!(cache.table_bytes, retained);
        assert!(!cache.contains_nonce(&fourth.nonce));
        let mut replay = fourth;
        replay.nonce = Uuid::from_u128(1);
        assert_eq!(
            cache
                .check_and_record(replay)
                .0
                .expect_err("budget rejection must preserve earlier replay identities")
                .to_string(),
            "RPC request replay detected"
        );
        fourth.signed_at = 1301;
        fourth.wall_time = 1301;
        cache
            .check_and_record(fourth)
            .0
            .expect("natural expiration releases budget without evicting valid identities");
        assert_eq!(cache.entries, 1);
    }

    #[test]
    fn replay_cache_empty_budget_rejects_without_allocating_or_consuming_a_nonce() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        let mut record = test_nonce_record(Uuid::from_u128(1), 1000, now, 1000, now, 4096);
        record.memory_budget_bytes = Some(REPLAY_CACHE_METADATA_BYTES);
        assert_eq!(
            cache.check_and_record(record).0.expect_err("no payload budget").kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(cache.signed_times.is_empty());
        assert_eq!(cache.table_bytes, 0);
        assert_eq!(cache.entries, 0);
        record.memory_budget_bytes = None;
        cache
            .check_and_record(record)
            .0
            .expect("rejected record must not burn its identity");
    }

    #[test]
    fn nonce_cache_same_uuid_with_new_authenticated_time_is_a_new_authorization() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        let nonce = Uuid::from_u128(1);
        for signed in [1000, 1001] {
            let record = test_nonce_record(nonce, signed, now, 1000, now, 2);
            cache
                .check_and_record(record)
                .0
                .expect("freshly signed time creates a distinct authorization");
            assert_eq!(
                cache
                    .check_and_record(record)
                    .0
                    .expect_err("each authorization is one-use")
                    .to_string(),
                "RPC request replay detected"
            );
        }
        assert_eq!(cache.entries, 2);
    }

    #[test]
    fn nonce_cache_rechecks_forward_skew_when_recording_after_clock_rollback() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        let nonce = Uuid::from_u128(1);
        // A timestamp checked at wall time 1000 can reach the cache after a
        // rollback. It must not create an expiry bucket beyond the bounded window.
        let error = cache
            .check_and_record(test_nonce_record(nonce, 1300, now, 600, now, 2))
            .0
            .expect_err("recording must recheck the forward validity bound");
        assert_eq!(error.to_string(), "Request timestamp expired");
        assert_eq!(cache.entries, 0);
        assert!(cache.signed_times.is_empty());
        cache
            .check_and_record(test_nonce_record(nonce, 900, now, 600, now, 2))
            .0
            .expect("maximum valid forward skew must remain accepted");
    }

    #[test]
    fn replay_cache_growth_estimate_covers_hashbrown_layouts() {
        for entries in [1, 3, 4, 7, 8, 14, 15, 1024, 1_048_576, 3_670_016, 3_670_017, 7_340_032] {
            let mut set: HashSet<Uuid, RandomState> = HashSet::with_hasher(RandomState::new());
            set.try_reserve(entries).expect("test table allocation");
            let actual = replay_cache_table_charge(u64::try_from(set.allocation_size()).expect("layout bytes"))
                .expect("allocator allowance");
            assert!(replay_cache_table_growth_bytes(entries).expect("growth estimate") >= actual);
        }
    }

    #[test]
    fn nonce_cache_signed_time_extremes_keep_inclusive_validity() {
        let now = Instant::now();
        for time in [i64::MIN, i64::MIN + 300, -1000, 0, i64::MAX - 300, i64::MAX] {
            let mut cache = RpcNonceCache::default();
            let record = test_nonce_record(Uuid::from_u128(1), time, now, time, now, 1);
            cache.check_and_record(record).0.expect("valid extreme signed timestamp");
            assert_eq!(
                cache
                    .check_and_record(record)
                    .0
                    .expect_err("repeat extreme timestamp")
                    .to_string(),
                "RPC request replay detected"
            );
        }
    }

    #[test]
    fn nonce_cache_retains_all_601_signed_seconds_and_expires_only_invalid_buckets() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        for n in 0..601 {
            cache
                .check_and_record(test_nonce_record(
                    Uuid::from_u128(u128::try_from(n + 1).expect("nonce")),
                    700 + i64::from(n),
                    now,
                    1000,
                    now,
                    601,
                ))
                .0
                .expect("entire signed validity window");
        }
        assert_eq!(cache.signed_times.len(), 601);
        assert_eq!(cache.remove_expired(1000), 0);
        assert_eq!(cache.remove_expired(1001), 1);
        assert_eq!(cache.entries, 600);
        assert_eq!(cache.remove_expired(1601), 600);
        assert!(cache.signed_times.is_empty());
        assert_eq!(cache.table_bytes, 0);
    }

    #[test]
    fn nonce_cache_expires_by_signed_validity_and_fails_closed_at_capacity() {
        let now = Instant::now();
        let expiry = now.checked_add(REPLAY_CACHE_RETENTION).expect("test expiry should fit");
        let after_expiry = expiry.checked_add(Duration::from_secs(1)).expect("test expiry should fit");
        let nonce_a = Uuid::new_v4();
        let nonce_b = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();

        check_test_nonce_record(&mut cache, test_nonce_record(nonce_a, 100, now, 100, expiry, 1))
            .expect("first nonce should be recorded");
        let capacity = check_test_nonce_record(&mut cache, test_nonce_record(nonce_b, 100, now, 100, expiry, 1))
            .expect_err("a full replay cache must fail closed");
        assert_eq!(capacity.to_string(), "RPC replay cache capacity exceeded");
        check_test_nonce_record(&mut cache, test_nonce_record(nonce_b, 702, after_expiry, 702, after_expiry, 1))
            .expect("expired nonce should release capacity");
        assert!(!cache.contains_nonce(&nonce_a));
        assert!(cache.contains_nonce(&nonce_b));
    }

    #[test]
    fn nonce_cache_releases_expiration_storage_after_a_burst() {
        let start = Instant::now();
        let expiry = start + REPLAY_CACHE_RETENTION;
        let mut cache = RpcNonceCache::default();

        for nonce in 1..=4_096 {
            let record = test_nonce_record(Uuid::from_u128(nonce), 100, start, 100, expiry, 4_096);
            cache.check_and_record(record).0.expect("first use of nonce");
        }
        assert_eq!(cache.entries, 4_096);
        assert!(cache.table_bytes > 0);
        assert_eq!(cache.remove_expired(400), 0, "inclusive signed validity stays protected");
        assert_eq!(cache.remove_expired(401), 4_096, "invalid signatures release capacity immediately");
        assert_eq!(cache.entries, 0);
        assert!(cache.signed_times.is_empty());
        assert_eq!(cache.table_bytes, 0, "expired entries must release their expiration storage");
    }

    #[test]
    fn nonce_cache_releases_expired_bucket_without_touching_future_identities() {
        let now = Instant::now();
        let mut cache = RpcNonceCache::default();
        for n in 1..=2048 {
            let signed = if n % 2 == 0 { 1300 } else { 1000 };
            cache
                .check_and_record(test_nonce_record(Uuid::from_u128(n), signed, now, 1000, now, 4096))
                .0
                .expect("two validity buckets");
        }
        let before = cache.table_bytes;
        assert_eq!(cache.remove_expired(1301), 1024);
        assert!(cache.table_bytes < before);
        for n in 2049..=3072 {
            cache
                .check_and_record(test_nonce_record(Uuid::from_u128(n), 1301, now, 1301, now, 4096))
                .0
                .expect("new time bucket");
        }
        for n in 1..=3072 {
            assert_eq!(cache.contains_nonce(&Uuid::from_u128(n)), n > 2048 || n % 2 == 0);
        }
        assert_eq!(cache.entries, 2048);
    }

    #[test]
    fn replay_cache_field_budget_is_not_a_promise_of_fixed_entry_throughput() {
        let memory = 31_u64 * 1024 * 1024 * 1024 / 4;
        let decision =
            replay_cache_capacity_decision(rustfs_utils::EnvParseOutcome::Absent, 4, Some(memory), Some(MemoryBasis::Cgroup));
        assert_eq!(decision.memory_budget_bytes, Some(replay_cache_memory_budget(memory)));
        assert!(decision.capacity >= 40_000 * 301);
        assert_eq!(decision.capacity, decision.memory_based_capacity);
    }

    #[test]
    fn nonce_cache_metrics_mark_successful_records_only() {
        let now = Instant::now();
        let expiry = now.checked_add(REPLAY_CACHE_RETENTION).expect("test expiry should fit");
        let nonce_a = Uuid::new_v4();
        let nonce_b = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();

        let (recorded, metrics) =
            check_test_nonce_record_with_metrics(&mut cache, test_nonce_record(nonce_a, 100, now, 100, expiry, 1));
        recorded.expect("first nonce should be recorded");
        let metrics = metrics.expect("successful nonce should publish metrics");
        let record_scope = metrics.record_scope.expect("successful nonce should carry record scope");
        assert_eq!(record_scope.operation, INTERNODE_OPERATION_GRPC_READ_ALL);
        assert_eq!(record_scope.backend, INTERNODE_TRANSPORT_BACKEND_GRPC);
        assert_eq!(record_scope.rpc_path, "/node_service.NodeService/ReadAll");
        assert!(metrics.overflow_scope.is_none());

        let (replay, metrics) =
            check_test_nonce_record_with_metrics(&mut cache, test_nonce_record(nonce_a, 100, now, 100, expiry, 1));
        assert_eq!(
            replay.expect_err("duplicate nonce must fail closed").to_string(),
            "RPC request replay detected"
        );
        let metrics = metrics.expect("replay rejection should still publish cache state");
        assert!(metrics.record_scope.is_none());
        assert!(metrics.overflow_scope.is_none());

        let (overflow, metrics) =
            check_test_nonce_record_with_metrics(&mut cache, test_nonce_record(nonce_b, 100, now, 100, expiry, 1));
        assert_eq!(
            overflow.expect_err("full cache must fail closed").to_string(),
            "RPC replay cache capacity exceeded"
        );
        let metrics = metrics.expect("overflow should publish cache state");
        assert!(metrics.record_scope.is_none());
        let overflow_scope = metrics.overflow_scope.expect("overflow should keep diagnostic scope");
        assert_eq!(overflow_scope.operation, INTERNODE_OPERATION_GRPC_READ_ALL);
        assert_eq!(overflow_scope.backend, INTERNODE_TRANSPORT_BACKEND_GRPC);
        assert_eq!(overflow_scope.rpc_path, "/node_service.NodeService/ReadAll");
    }

    // The `rpc_body_digest_fallback_counter` serial group covers every test that drives (or
    // asserts on) the process-global body-digest fallback counter, so exact-delta assertions
    // cannot race with each other.
    #[test]
    #[serial_test::serial(rpc_body_digest_fallback_counter)]
    fn digestless_mutation_is_accepted_and_counted_while_strict_gate_is_off() {
        let request = tonic::Request::new(());
        let before = global_internode_metrics().snapshot().body_digest_fallback_total;

        assert!(
            verify_tonic_mutation_body_digest_with_strictness(&request, b"canonical-mutation-body", false).is_ok(),
            "a digestless peer must keep mutating while the strict gate is off"
        );

        let after = global_internode_metrics().snapshot().body_digest_fallback_total;
        assert_eq!(
            after,
            before + 1,
            "an accepted digestless mutation must increment the body-digest fallback counter exactly once"
        );
    }

    #[test]
    #[serial_test::serial(rpc_body_digest_fallback_counter)]
    fn strict_mutation_gate_rejects_digestless_but_keeps_body_bound() {
        let before = global_internode_metrics().snapshot().body_digest_fallback_total;

        let digestless = tonic::Request::new(());
        let error = verify_tonic_mutation_body_digest_with_strictness(&digestless, b"body", true)
            .expect_err("strict mode must reject a mutation without a body digest");
        assert_eq!(error.to_string(), "RPC mutation requires a body-bound v2 signature");

        let mut unsigned = tonic::Request::new(());
        unsigned
            .metadata_mut()
            .as_mut()
            .insert(RPC_CONTENT_SHA256_HEADER, HeaderValue::from_static(UNSIGNED_PAYLOAD));
        let error = verify_tonic_mutation_body_digest_with_strictness(&unsigned, b"body", true)
            .expect_err("strict mode must reject an explicitly unsigned mutation payload");
        assert_eq!(error.to_string(), "RPC mutation requires a body-bound v2 signature");

        let mut bound = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut bound, b"body").expect("digest metadata should encode");
        bound
            .metadata_mut()
            .as_mut()
            .insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));
        assert!(
            verify_tonic_mutation_body_digest_with_strictness(&bound, b"body", true).is_ok(),
            "strict mode must keep accepting body-bound mutations"
        );
        let tampered = verify_tonic_mutation_body_digest_with_strictness(&bound, b"tampered-body", true)
            .expect_err("a tampered canonical body must fail even in strict mode");
        assert_eq!(tampered.to_string(), "RPC content SHA-256 mismatch");

        let after = global_internode_metrics().snapshot().body_digest_fallback_total;
        assert_eq!(
            after, before,
            "neither strict rejections nor bound verifications are digestless fallbacks"
        );
    }

    #[test]
    #[serial_test::serial(rpc_body_digest_fallback_counter)]
    fn mutation_digest_default_posture_is_fail_open_digestless_accept() {
        // The public entry point resolves strictness from the environment, whose compile-time
        // default is pinned to false in `rustfs_config`. A digestless peer therefore keeps
        // mutating through the default build with no configuration at all.
        let request = tonic::Request::new(());
        assert!(
            verify_tonic_mutation_body_digest_with_strictness(
                &request,
                b"canonical-mutation-body",
                rustfs_config::DEFAULT_INTERNODE_RPC_BODY_DIGEST_STRICT,
            )
            .is_ok(),
            "the default strict posture must accept digestless mutations"
        );
    }

    #[test]
    fn rename_data_mutation_contract_binds_method_nonce_and_body() {
        ensure_test_rpc_secret();
        let message = rustfs_protos::proto_gen::node_service::RenameDataRequest {
            bucket_incarnation_id: Default::default(),
            disk: "http://node-a:9000/data/rustfs0".to_string(),
            src_volume: ".rustfs.sys/multipart".to_string(),
            src_path: "uploads/object".to_string(),
            file_info: "{\"volume\":\"bucket\"}".to_string(),
            dst_volume: "bucket".to_string(),
            dst_path: "object".to_string(),
            file_info_bin: vec![0x81, 0xA1, 0x76, 0x01].into(),
            scanner_publication_lease_token: Vec::new().into(),
        };
        let body = rustfs_protos::canonical_rename_data_request_body(&message).expect("small request should encode");
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, &body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "RenameData", content_sha256)
            .expect("body-bound auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(
            verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/RenameData", &headers).is_ok(),
            "the rename_data signature must bind destination, method, nonce, and body digest"
        );
        let replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/RenameData", &headers)
            .expect_err("reusing a consumed rename_data nonce must fail");
        assert_eq!(replay.to_string(), "RPC request replay detected");
        let transplant = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/DeleteVersion", &headers)
            .expect_err("a rename_data signature must not authenticate a different method");
        assert_eq!(transplant.to_string(), "Invalid RPC v2 signature");

        assert!(verify_tonic_mutation_body_digest(&request, &body).is_ok());
        let mut tampered = message;
        tampered.file_info_bin = Vec::new().into();
        let tampered_body = rustfs_protos::canonical_rename_data_request_body(&tampered).expect("small request should encode");
        let stripped = verify_tonic_mutation_body_digest(&request, &tampered_body)
            .expect_err("stripping the msgpack payload to force the JSON fallback decode must fail");
        assert_eq!(stripped.to_string(), "RPC content SHA-256 mismatch");
    }

    #[test]
    fn signal_service_mutation_contract_rejects_tampering_and_replay() {
        ensure_test_rpc_secret();
        let body = signal_service_request("2", "scanner", "false")
            .canonical_body()
            .expect("small signal request should encode");
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, &body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "SignalService", content_sha256)
            .expect("body-bound auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(
            verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/SignalService", &headers).is_ok(),
            "the first body-bound signal request must authenticate"
        );
        assert!(verify_tonic_mutation_body_digest(&request, &body).is_ok());

        let tampered = signal_service_request("1", "scanner", "false")
            .canonical_body()
            .expect("small signal request should encode");
        let error = verify_tonic_mutation_body_digest(&request, &tampered)
            .expect_err("changing the signal must invalidate the signed digest");
        assert_eq!(error.to_string(), "RPC content SHA-256 mismatch");

        let replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/SignalService", &headers)
            .expect_err("reusing the signal nonce must fail");
        assert_eq!(replay.to_string(), "RPC request replay detected");
    }

    #[test]
    #[serial_test::serial(rpc_body_digest_fallback_counter)]
    fn signal_service_mutation_contract_preserves_rollout_fallback_and_strictness() {
        let body = signal_service_request("2", "scanner", "false")
            .canonical_body()
            .expect("small signal request should encode");
        let before = global_internode_metrics().snapshot().body_digest_fallback_total;
        let digestless = tonic::Request::new(());

        assert!(
            verify_tonic_mutation_body_digest_with_strictness(&digestless, &body, false).is_ok(),
            "old peers must remain compatible while the rollout gate is open"
        );
        assert_eq!(
            global_internode_metrics().snapshot().body_digest_fallback_total,
            before + 1,
            "accepted digestless signal requests must be visible in the fallback metric"
        );

        let error = verify_tonic_mutation_body_digest_with_strictness(&digestless, &body, true)
            .expect_err("strict mode must reject a digestless signal request");
        assert_eq!(error.to_string(), "RPC mutation requires a body-bound v2 signature");

        let mut bound = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut bound, &body).expect("canonical body digest should be attached");
        bound
            .metadata_mut()
            .as_mut()
            .insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));
        assert!(verify_tonic_mutation_body_digest_with_strictness(&bound, &body, true).is_ok());
    }

    #[test]
    fn nonce_cache_rejects_replay_after_wall_clock_regression() {
        let now = Instant::now();
        let expiry = now.checked_add(REPLAY_CACHE_RETENTION).expect("test expiry should fit");
        let after_expiry = expiry.checked_add(Duration::from_secs(1)).expect("test expiry should fit");
        let nonce = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();

        check_test_nonce_record(&mut cache, test_nonce_record(nonce, 1_000, now, 1_000, expiry, 2))
            .expect("first nonce should be recorded");
        let replay = check_test_nonce_record(&mut cache, test_nonce_record(nonce, 1_000, after_expiry, 900, after_expiry, 2))
            .expect_err("wall clock regression must not make an old signature reusable");
        assert_eq!(replay.to_string(), "RPC request replay detected");

        let stale =
            check_test_nonce_record(&mut cache, test_nonce_record(Uuid::new_v4(), 600, after_expiry, 900, after_expiry, 2))
                .expect_err("the monotonic wall-clock high-water mark must fail closed");
        assert_eq!(stale.to_string(), "RPC request timestamp expired after clock regression");
    }
}
