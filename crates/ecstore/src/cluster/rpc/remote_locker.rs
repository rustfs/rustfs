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

use crate::cluster::rpc::client::{
    AuthenticatedChannel, BatchLockRpcDeadline, TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client,
};
use crate::cluster::rpc::set_tonic_rolling_mutation_body_digest;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use rustfs_lock::{
    BatchAcquireOutcome, LockCleanupOutcome, LockClient, LockError, LockInfo, LockRequest, LockResponse, LockStats, LockStatus,
    LockType, Result,
    types::{LockId, LockMetadata, LockPriority},
};
use rustfs_protos::proto_gen::node_service::{
    BatchGenerallyLockRequest, BatchGenerallyLockResponse, GenerallyLockRequest, GenerallyLockResponse, GenerallyLockResult,
    LockAcquireStatus, LockReleaseStatus, PingRequest,
};
use rustfs_protos::{
    ChannelRebuildReason, ConnectionEvictionLogLevel, evict_failed_connection_with_reason, models::PingBodyBuilder,
    proto_gen::node_service::node_service_client::NodeServiceClient,
};
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response};
use tracing::{Instrument as _, debug, info, warn};

const EVENT_NAMESPACE_LOCK_RELEASE: &str = "namespace_lock_release";
const EVENT_REMOTE_LOCK_RPC_CLEANUP: &str = "remote_lock_rpc_cleanup";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REMOTE_LOCK: &str = "remote_lock";

fn attach_lock_mutation_body_digest<T: rustfs_protos::CanonicalMutationBody>(request: &mut Request<T>) -> std::io::Result<()> {
    set_tonic_rolling_mutation_body_digest(request)
}

/// Work to run if an RPC completes after its caller stops waiting.
type LateCompletion<T> = Option<Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>;
type LateFailure = LateCompletion<()>;
type RpcFuture<T> = BoxFuture<'static, std::thread::Result<std::result::Result<T, tonic::Status>>>;

/// Own admission before connecting as well as while an RPC is in flight.
struct LockRequestGuard<'a> {
    client: &'a RemoteClient,
    op: &'static str,
    admission: Option<LockRequestAdmission>,
}

impl LockRequestGuard<'_> {
    fn into_admission(mut self) -> Option<LockRequestAdmission> {
        self.admission.take()
    }
}

impl Drop for LockRequestGuard<'_> {
    fn drop(&mut self) {
        self.client.release_lock_request(self.op, self.admission.take());
    }
}

struct RpcCleanup<'a, T> {
    late: LateCompletion<T>,
    late_failure: LateFailure,
    admission: Option<LockRequestGuard<'a>>,
    batch_deadline: Option<BatchLockRpcDeadline>,
}

impl<'a, T> RpcCleanup<'a, T> {
    fn new(late: LateCompletion<T>, late_failure: LateFailure, admission: Option<LockRequestGuard<'a>>) -> Self {
        Self {
            late,
            late_failure,
            admission,
            batch_deadline: None,
        }
    }

    fn with_batch_deadline(mut self, deadline: BatchLockRpcDeadline) -> Self {
        self.batch_deadline = Some(deadline);
        self
    }
}

/// A dropped caller hands its RPC and cleanup to the same bounded owner as a timeout.
struct InFlightRpc<'a, T: Send + 'static> {
    client: &'a RemoteClient,
    op: &'static str,
    resource_summary: &'a str,
    future: Option<RpcFuture<T>>,
    cleanup: Option<RpcCleanup<'a, T>>,
    span: tracing::Span,
}

impl<T: Send + 'static> Drop for InFlightRpc<'_, T> {
    fn drop(&mut self) {
        let _span = self.span.enter();
        if let Some(future) = self.future.take()
            && let Some(cleanup) = self.cleanup.take()
        {
            if cleanup
                .batch_deadline
                .as_ref()
                .is_some_and(BatchLockRpcDeadline::cancel_before_dispatch)
            {
                // A cancelled readiness or channel queue wait must never send lock payload later.
                drop(future);
                return;
            }
            self.client.detach_rpc(self.op, self.resource_summary, future, cleanup);
        }
    }
}

/// The liveness window is this many RPC deadlines: a peer that completed a
/// lock RPC within it is slow, not gone, and keeps its channel on a timeout.
const LOCK_RPC_LIVENESS_WINDOW_DEADLINES: u32 = 2;
const LOCK_RPC_REQUEST_BACKOFF_THRESHOLD: u32 = 3;
const LOCK_RPC_REQUEST_BACKOFF_MAX: Duration = Duration::from_secs(30);
const LATE_RELEASE_RETRY_DELAYS: [Duration; 3] = [Duration::from_millis(100), Duration::from_millis(500), Duration::from_secs(1)];
const LATE_RELEASE_IN_FLIGHT_LIMIT: usize = 32;
const TIMEOUT_LOG_STATE_MAX: usize = 1024;
const TIMEOUT_LOG_STATE_RETENTION: Duration = Duration::from_secs(600);

fn is_request_breaker_operation(op: &'static str) -> bool {
    matches!(op, "lock" | "lock_batch")
}

/// Recent history of the shared lock channel to one peer (issue #7363).
///
/// A single request deadline says nothing about the HTTP/2 connection it ran
/// on: a peer whose lock service is merely slow keeps answering other streams.
/// Evicting the cached channel on every timeout turned that slowness into a
/// `RST_STREAM`/`GOAWAY too_many_resets`/re-dial loop across the cluster, so
/// eviction now requires the peer to have gone quiet and is rate limited.
#[derive(Debug, Clone, Copy, Default)]
struct LockPeerChannelHealth {
    last_success: Option<Instant>,
    last_eviction: Option<Instant>,
    consecutive_timeouts: u32,
    request_consecutive_timeouts: u32,
    request_backoff_until: Option<Instant>,
    request_backoff: Duration,
    request_probe_generation: Option<u64>,
    next_probe_generation: u64,
    request_in_flight: usize,
    late_release_in_flight: usize,
    /// Timed-out RPCs still running in the background for this peer.
    detached_rpcs: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct TimeoutLogHealth {
    last_logged: Option<Instant>,
    suppressed: u64,
}

#[derive(Debug, Clone, Copy)]
struct LockRequestAdmission {
    probe_generation: Option<u64>,
}

impl LockRequestAdmission {
    fn is_probe(self) -> bool {
        self.probe_generation.is_some()
    }
}

fn lock_peer_channel_health() -> &'static Mutex<HashMap<String, LockPeerChannelHealth>> {
    static HEALTH: OnceLock<Mutex<HashMap<String, LockPeerChannelHealth>>> = OnceLock::new();
    HEALTH.get_or_init(Mutex::default)
}

fn timeout_log_health() -> &'static Mutex<HashMap<(String, &'static str), TimeoutLogHealth>> {
    static HEALTH: OnceLock<Mutex<HashMap<(String, &'static str), TimeoutLogHealth>>> = OnceLock::new();
    HEALTH.get_or_init(Mutex::default)
}

fn with_lock_peer_health<R>(addr: &str, update: impl FnOnce(&mut LockPeerChannelHealth) -> R) -> R {
    let mut peers = lock_peer_channel_health()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(health) = peers.get_mut(addr) {
        return update(health);
    }
    update(peers.entry(addr.to_string()).or_default())
}

#[cfg(test)]
fn lock_peer_health_for_test(addr: &str) -> LockPeerChannelHealth {
    lock_peer_channel_health()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(addr)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
fn reset_lock_peer_health_for_test(addr: &str) {
    lock_peer_channel_health()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(addr);
}

#[cfg(test)]
type EvictionPause = (tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>);

#[cfg(test)]
fn eviction_pauses_for_test() -> &'static Mutex<HashMap<String, EvictionPause>> {
    static PAUSES: OnceLock<Mutex<HashMap<String, EvictionPause>>> = OnceLock::new();
    PAUSES.get_or_init(Mutex::default)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionTrigger {
    /// The caller's deadline expired while the stream was still open.
    Timeout,
    /// The transport itself reported the failure (refused, reset, GOAWAY, ...).
    Transport,
}

impl EvictionTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionVerdict {
    Evict,
    /// The peer completed a lock RPC within the liveness window: slow, not gone.
    PeerRecentlyServed,
    /// The channel was re-dialed within the cooldown; let it prove itself first.
    CoolingDown,
}

impl EvictionVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Evict => "evict",
            Self::PeerRecentlyServed => "peer_recently_served",
            Self::CoolingDown => "cooling_down",
        }
    }
}

/// Decide whether a failed lock RPC may evict the shared channel to its peer.
fn eviction_verdict(
    health: &LockPeerChannelHealth,
    now: Instant,
    trigger: EvictionTrigger,
    liveness_window: Duration,
    cooldown: Duration,
) -> EvictionVerdict {
    if trigger == EvictionTrigger::Timeout
        && health
            .last_success
            .is_some_and(|at| now.saturating_duration_since(at) < liveness_window)
    {
        return EvictionVerdict::PeerRecentlyServed;
    }
    if health
        .last_eviction
        .is_some_and(|at| now.saturating_duration_since(at) < cooldown)
    {
        return EvictionVerdict::CoolingDown;
    }
    EvictionVerdict::Evict
}

/// Use exactly the same conservative interpretation in foreground and late cleanup.
fn batch_acquire_outcome(lock_id: &LockId, lock_type: LockType, result: Option<&GenerallyLockResult>) -> BatchAcquireOutcome {
    let ambiguous = |message: String| BatchAcquireOutcome::Ambiguous {
        error: LockError::internal(message),
        cleanup_owned_by_client: false,
    };
    let Some(result) = result else {
        return ambiguous("Lock batch response missing entry".to_string());
    };
    let Ok(status) = LockAcquireStatus::try_from(result.acquire_status) else {
        return ambiguous("Unknown remote lock acquire status".to_string());
    };
    if result.release_status != LockReleaseStatus::Unspecified as i32 {
        return ambiguous("Lock batch response contains a release status".to_string());
    }
    if result.success {
        if status != LockAcquireStatus::Unspecified || result.error_info.is_some() {
            return ambiguous("Contradictory remote lock grant".to_string());
        }
        let lock_info = match result.lock_info.as_deref() {
            Some(json) => match serde_json::from_str::<LockInfo>(json) {
                Ok(info) if info.id == *lock_id && info.resource == lock_id.resource && info.lock_type == lock_type => {
                    Some(Box::new(info))
                }
                _ => {
                    return ambiguous("Remote lock grant does not match the requested identity".to_string());
                }
            },
            None => None,
        };
        // Legacy peers report a valid grant with no acquire_status or lock_info.
        return BatchAcquireOutcome::Granted {
            lock_info,
            wait_time: Duration::ZERO,
        };
    }
    if result.lock_info.is_some() {
        return ambiguous("Remote no-grant response contains lock information".to_string());
    }
    let error = LockError::internal(
        result
            .error_info
            .clone()
            .unwrap_or_else(|| "Lock acquisition failed on remote server".to_string()),
    );
    match status {
        LockAcquireStatus::RetryableNoGrant => BatchAcquireOutcome::RetryableNoGrant { error },
        LockAcquireStatus::RejectedNoGrant => BatchAcquireOutcome::TerminalNoGrant { error },
        LockAcquireStatus::Unspecified => BatchAcquireOutcome::Ambiguous {
            error,
            cleanup_owned_by_client: false,
        },
    }
}

fn cleanup_outcome(success: bool, error_info: Option<&str>, release_status: i32) -> Result<LockCleanupOutcome> {
    let status =
        LockReleaseStatus::try_from(release_status).map_err(|_| LockError::internal("Unknown remote lock release status"))?;
    match status {
        LockReleaseStatus::AlreadyAbsent if !success => Ok(LockCleanupOutcome::AlreadyAbsent),
        LockReleaseStatus::Released if success && error_info.is_none() => Ok(LockCleanupOutcome::Released),
        LockReleaseStatus::Unspecified => {
            if let Some(error) = error_info {
                Err(LockError::internal(error))
            } else if success {
                Ok(LockCleanupOutcome::Released)
            } else {
                Ok(LockCleanupOutcome::Unconfirmed)
            }
        }
        _ => Err(LockError::internal("Inconsistent remote lock release status")),
    }
}

#[derive(Clone, Copy)]
enum AcquisitionCleanup {
    Confirmed,
    Uncertain,
}

/// Remote lock client implementation
#[derive(Debug, Clone)]
pub struct RemoteClient {
    addr: String,
}

impl RemoteClient {
    const ONLINE_CHECK_RESOURCE: &'static str = "health-lock-online";

    fn record_release_ack(&self, lock_id: &LockId, outcome: LockCleanupOutcome) {
        tracing::trace!(
            target: "rustfs_ecstore::object_lock_diag",
            event = EVENT_NAMESPACE_LOCK_RELEASE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
            state = "acknowledged",
            result = match outcome {
                LockCleanupOutcome::Released => "released",
                LockCleanupOutcome::AlreadyAbsent => "already_absent",
                LockCleanupOutcome::Unconfirmed => "unconfirmed",
            },
            peer_kind = "remote",
            lock_id = %lock_id,
            peer = %self.addr,
            "Remote namespace lock release acknowledged"
        );
    }

    pub fn new(endpoint: String) -> Self {
        Self { addr: endpoint }
    }

    fn ping_body() -> Bytes {
        static BODY: OnceLock<Bytes> = OnceLock::new();
        BODY.get_or_init(|| {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let payload = fbb.create_vector(b"health-check");
            let mut builder = PingBodyBuilder::new(&mut fbb);
            builder.add_payload(payload);
            let root = builder.finish();
            fbb.finish(root, None);
            Bytes::copy_from_slice(fbb.finished_data())
        })
        .clone()
    }

    fn build_ping_request() -> PingRequest {
        PingRequest {
            version: 1,
            body: Self::ping_body(),
        }
    }

    #[cfg(test)]
    fn build_fresh_ping_request_for_test() -> PingRequest {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"health-check");
        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        PingRequest {
            version: 1,
            body: Bytes::copy_from_slice(fbb.finished_data()),
        }
    }

    /// Create a minimal LockRequest for unlock operations using only lock_id
    fn create_unlock_request(lock_id: &LockId) -> LockRequest {
        LockRequest {
            lock_id: lock_id.clone(),
            resource: lock_id.resource.clone(),
            lock_type: LockType::Exclusive, // Type doesn't matter for unlock
            owner: String::new(),           // Owner not needed, server uses lock_id
            acquire_timeout: std::time::Duration::from_secs(30),
            ttl: std::time::Duration::from_secs(300),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            deadlock_detection: false,
            suppress_contention_logs: false,
            refresh_interval: None,
        }
    }

    pub async fn get_client(&self) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>> {
        // P3-2 offline bypass (now covering the lock path too): fast-fail a peer already marked
        // offline instead of paying the connect timeout, so dsync reaches quorum sooner. Does not
        // change quorum; the self-healing re-probe keeps the peer recoverable.
        if let Some(reason) = crate::cluster::rpc::remote_disk::internode_offline_bypass_reason(&self.addr) {
            return Err(LockError::internal(reason));
        }
        node_service_time_out_client(&self.addr, TonicInterceptor::Signature(gen_tonic_signature_interceptor()))
            .await
            .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))
    }

    fn is_scanner_leader_lock(resource_summary: &str) -> bool {
        resource_summary == ".rustfs.sys/leader.lock@latest"
    }

    /// Classify a `tonic::Status` as a transport-level failure of the cached channel.
    ///
    /// Only genuine connection problems (connect refused/reset, keepalive/deadline
    /// on the channel itself, cancelled in-flight streams) justify evicting and
    /// re-dialing the cached channel. A broken transport surfaces either as one of
    /// the codes below or carries the underlying hyper/h2 error as its `source`.
    ///
    /// Server-produced application statuses (`Unauthenticated`/`PermissionDenied`
    /// from the signature interceptor, `Internal`/`FailedPrecondition` when the
    /// peer's lock service is not ready yet, `InvalidArgument`, `ResourceExhausted`,
    /// `Unimplemented`, ...) are reconstructed from grpc-status trailers on the
    /// client and have no `source`. Evicting the channel for those cannot help —
    /// the channel is healthy — and only churns the connection while advancing the
    /// peer toward the offline threshold. See issue #4567.
    fn is_transport_failure(status: &tonic::Status) -> bool {
        use tonic::Code;
        // A valid stale-epoch request is rejected before execution. Its signed
        // response refreshes the peer epoch on the existing healthy channel.
        let epoch_rejection = status.code() == Code::Unavailable && status.message() == "RPC boot epoch changed";
        std::error::Error::source(status).is_some()
            || (!epoch_rejection
                && matches!(
                    status.code(),
                    Code::Unavailable | Code::DeadlineExceeded | Code::Unknown | Code::Cancelled
                ))
    }

    async fn evict_connection(&self, op: &'static str, reason: &str, resource_summary: &str, trigger: EvictionTrigger) {
        let log_level = if Self::is_scanner_leader_lock(resource_summary) {
            debug!(
                addr = %self.addr,
                op,
                reason,
                resource_summary,
                "Evicting cached remote lock connection for scanner leader-lock RPC failure"
            );
            ConnectionEvictionLogLevel::Debug
        } else {
            warn!(
                addr = %self.addr,
                op,
                reason,
                resource_summary,
                "Evicting cached remote lock connection after RPC failure"
            );
            ConnectionEvictionLogLevel::Warn
        };
        let rebuild_reason = match trigger {
            EvictionTrigger::Timeout => ChannelRebuildReason::LockTimeout,
            EvictionTrigger::Transport => ChannelRebuildReason::LockTransport,
        };
        evict_failed_connection_with_reason(&self.addr, rebuild_reason, log_level).await;
    }

    fn summarize_resources(requests: &[LockRequest]) -> String {
        const LIMIT: usize = 3;
        let mut resources = requests
            .iter()
            .take(LIMIT)
            .map(|request| request.resource.to_string())
            .collect::<Vec<_>>();
        if requests.len() > LIMIT {
            resources.push(format!("... (+{} more)", requests.len() - LIMIT));
        }
        resources.join(", ")
    }

    fn rpc_timeout() -> Duration {
        Duration::from_millis(
            rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS,
                rustfs_config::DEFAULT_OBJECT_LOCK_RPC_TIMEOUT_MS,
            )
            .max(1),
        )
    }

    fn online_check_timeout() -> Duration {
        Duration::from_millis(
            rustfs_utils::get_env_u64(
                rustfs_config::ENV_HEALTH_LOCK_ONLINE_TIMEOUT_MS,
                rustfs_config::DEFAULT_HEALTH_LOCK_ONLINE_TIMEOUT_MS,
            )
            .max(1),
        )
    }

    fn eviction_cooldown() -> Duration {
        Duration::from_millis(rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS,
            rustfs_config::DEFAULT_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS,
        ))
    }

    fn detached_rpc_limit() -> usize {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_LOCK_RPC_DETACHED_LIMIT,
            rustfs_config::DEFAULT_OBJECT_LOCK_RPC_DETACHED_LIMIT,
        )
    }

    fn request_limit() -> usize {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_LOCK_RPC_REQUEST_LIMIT,
            rustfs_config::DEFAULT_OBJECT_LOCK_RPC_REQUEST_LIMIT,
        )
        .max(1)
    }

    fn liveness_window(deadline: Duration) -> Duration {
        deadline.saturating_mul(LOCK_RPC_LIVENESS_WINDOW_DEADLINES)
    }

    fn record_rpc_success(&self, op: &'static str, admission: Option<LockRequestAdmission>) {
        with_lock_peer_health(&self.addr, |health| {
            health.last_success = Some(Instant::now());
            health.consecutive_timeouts = 0;
            if is_request_breaker_operation(op) {
                let is_probe = admission.is_some_and(LockRequestAdmission::is_probe);
                if health.request_backoff_until.is_none() || is_probe {
                    health.request_consecutive_timeouts = 0;
                    health.request_backoff_until = None;
                    health.request_backoff = Duration::ZERO;
                }
                if is_probe && admission.is_some_and(|admission| health.request_probe_generation == admission.probe_generation) {
                    health.request_probe_generation = None;
                }
            }
        });
        // A successful lock RPC over a cached or fresh channel proves the peer is
        // reachable; reconcile the internode peer-health gauge so it does not stay
        // stale when no fresh dial fires (issue #9119).
        rustfs_io_metrics::internode_metrics::record_peer_reachable_if_offline(&self.addr);
    }

    fn admit_late_release(&self) -> bool {
        with_lock_peer_health(&self.addr, |health| {
            if health.late_release_in_flight >= LATE_RELEASE_IN_FLIGHT_LIMIT {
                false
            } else {
                health.late_release_in_flight += 1;
                true
            }
        })
    }

    fn release_late_release(&self) {
        with_lock_peer_health(&self.addr, |health| {
            health.late_release_in_flight = health.late_release_in_flight.saturating_sub(1);
        });
    }

    /// Admit a new lock acquisition unless this peer is in the request-level
    /// backoff window. A single half-open probe is allowed after the window;
    /// channel liveness and request admission are deliberately separate.
    fn admit_lock_request(&self, op: &'static str) -> Option<LockRequestAdmission> {
        if !is_request_breaker_operation(op) {
            return Some(LockRequestAdmission { probe_generation: None });
        }

        let now = Instant::now();
        let (admission, suppression_reason) = with_lock_peer_health(&self.addr, |health| {
            let degraded = health.request_backoff_until.is_some() || health.request_consecutive_timeouts > 0;
            if degraded && health.request_in_flight >= Self::request_limit() {
                return (None, "in_flight_limit");
            }
            let Some(until) = health.request_backoff_until else {
                health.request_in_flight += 1;
                return (Some(LockRequestAdmission { probe_generation: None }), "none");
            };
            if now < until || health.request_probe_generation.is_some() {
                return (None, "breaker_open");
            }
            health.next_probe_generation = health.next_probe_generation.saturating_add(1);
            let generation = health.next_probe_generation;
            health.request_probe_generation = Some(generation);
            health.request_in_flight += 1;
            (
                Some(LockRequestAdmission {
                    probe_generation: Some(generation),
                }),
                "none",
            )
        });
        if admission.is_none() {
            rustfs_io_metrics::lock_metrics::record_remote_lock_request_suppressed(&self.addr, op, suppression_reason);
        }
        admission
    }

    fn admit_lock_request_guard(&self, op: &'static str) -> Option<LockRequestGuard<'_>> {
        self.admit_lock_request(op).map(|admission| LockRequestGuard {
            client: self,
            op,
            admission: Some(admission),
        })
    }

    fn release_lock_request(&self, op: &'static str, admission: Option<LockRequestAdmission>) {
        if is_request_breaker_operation(op) && admission.is_some() {
            with_lock_peer_health(&self.addr, |health| {
                health.request_in_flight = health.request_in_flight.saturating_sub(1);
                if admission.is_some_and(|admission| health.request_probe_generation == admission.probe_generation) {
                    health.request_probe_generation = None;
                }
            });
        }
    }

    fn record_request_timeout(&self, op: &'static str, deadline: Duration) {
        if !is_request_breaker_operation(op) {
            return;
        }

        let now = Instant::now();
        with_lock_peer_health(&self.addr, |health| {
            health.request_consecutive_timeouts = health.request_consecutive_timeouts.saturating_add(1);
            if health.request_consecutive_timeouts < LOCK_RPC_REQUEST_BACKOFF_THRESHOLD {
                return;
            }

            let initial = deadline / 2;
            let next = if health.request_backoff.is_zero() {
                initial.max(Duration::from_millis(1))
            } else {
                health.request_backoff.saturating_mul(2)
            };
            health.request_backoff = next.min(LOCK_RPC_REQUEST_BACKOFF_MAX);
            health.request_backoff_until = Some(now + health.request_backoff);
        });
    }

    fn timeout_log_decision(&self, op: &'static str, interval: Duration) -> (bool, u64) {
        let now = Instant::now();
        let mut peers = timeout_log_health().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if peers.len() >= TIMEOUT_LOG_STATE_MAX {
            peers.retain(|_, state| {
                state
                    .last_logged
                    .is_some_and(|at| now.saturating_duration_since(at) < TIMEOUT_LOG_STATE_RETENTION)
            });
        }
        let key = (self.addr.clone(), op);
        if peers.len() >= TIMEOUT_LOG_STATE_MAX
            && !peers.contains_key(&key)
            && let Some(eviction_key) = peers.keys().next().cloned()
        {
            peers.remove(&eviction_key);
        }
        let state = peers.entry(key).or_default();
        if state
            .last_logged
            .is_some_and(|at| now.saturating_duration_since(at) < interval)
        {
            state.suppressed = state.suppressed.saturating_add(1);
            return (false, state.suppressed);
        }

        let suppressed = state.suppressed;
        state.last_logged = Some(now);
        state.suppressed = 0;
        (true, suppressed)
    }

    fn bounded_acquire_request(request: &LockRequest, deadline: Duration) -> LockRequest {
        let mut bounded = request.clone();
        bounded.acquire_timeout = bounded.acquire_timeout.min(deadline);
        bounded
    }

    /// Apply the per-peer eviction policy after a failed RPC.
    async fn maybe_evict_connection(
        &self,
        op: &'static str,
        reason: &str,
        resource_summary: &str,
        trigger: EvictionTrigger,
        deadline: Duration,
    ) {
        #[cfg(test)]
        let pause = eviction_pauses_for_test()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.addr);
        #[cfg(test)]
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }

        let now = Instant::now();
        let cooldown = Self::eviction_cooldown();
        let liveness_window = Self::liveness_window(deadline);
        let (verdict, consecutive_timeouts) = with_lock_peer_health(&self.addr, |health| {
            if trigger == EvictionTrigger::Timeout {
                health.consecutive_timeouts = health.consecutive_timeouts.saturating_add(1);
            }
            let verdict = eviction_verdict(health, now, trigger, liveness_window, cooldown);
            if verdict == EvictionVerdict::Evict {
                health.last_eviction = Some(now);
            }
            (verdict, health.consecutive_timeouts)
        });
        if verdict == EvictionVerdict::Evict {
            rustfs_io_metrics::lock_metrics::record_remote_lock_channel_eviction(&self.addr, trigger.as_str());
            self.evict_connection(op, reason, resource_summary, trigger).await;
            return;
        }
        rustfs_io_metrics::lock_metrics::record_remote_lock_channel_eviction_suppressed(&self.addr, verdict.as_str());
        debug!(
            addr = %self.addr,
            op,
            resource_summary,
            trigger = trigger.as_str(),
            verdict = verdict.as_str(),
            consecutive_timeouts,
            "Keeping cached remote lock connection after RPC failure"
        );
    }

    /// Keep an abandoned RPC running instead of cancelling its stream.
    ///
    /// Dropping the future sends `RST_STREAM`; under load those resets pile up
    /// in the server's pending-accept queue until it answers `GOAWAY
    /// too_many_resets` and kills every stream on the connection. A detached
    /// stream ends on its own within the internode RPC timeout, the number per
    /// peer is bounded, and a lock granted after its caller gave up is released.
    fn detach_rpc<T: Send + 'static>(
        &self,
        op: &'static str,
        resource_summary: &str,
        future: RpcFuture<T>,
        cleanup: RpcCleanup<'_, T>,
    ) {
        let RpcCleanup {
            late,
            late_failure,
            admission,
            batch_deadline: _,
        } = cleanup;
        let admission = admission.and_then(LockRequestGuard::into_admission);
        let limit = Self::detached_rpc_limit();
        let admitted = with_lock_peer_health(&self.addr, |health| {
            if health.detached_rpcs >= limit {
                false
            } else {
                health.detached_rpcs += 1;
                true
            }
        });
        if !admitted {
            drop(future);
            self.release_lock_request(op, admission);
            if let Some(late_failure) = late_failure {
                drop(tokio::spawn(late_failure(()).in_current_span()));
            }
            rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_detached(op, "aborted");
            debug!(
                event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                state = "detached_budget_exhausted",
                addr = %self.addr,
                op,
                resource_summary,
                limit,
                "Abandoned remote lock RPC cancelled"
            );
            return;
        }
        rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_detached(op, "detached");
        let addr = self.addr.clone();
        tokio::spawn(
            async move {
                let outcome = future.await;
                with_lock_peer_health(&addr, |health| {
                    health.detached_rpcs = health.detached_rpcs.saturating_sub(1);
                    if is_request_breaker_operation(op) && admission.is_some() {
                        health.request_in_flight = health.request_in_flight.saturating_sub(1);
                        if admission.is_some_and(|admission| health.request_probe_generation == admission.probe_generation) {
                            health.request_probe_generation = None;
                        }
                    }
                });
                match outcome {
                    Ok(Ok(response)) => {
                        with_lock_peer_health(&addr, |health| {
                            health.last_success = Some(Instant::now());
                        });
                        rustfs_io_metrics::internode_metrics::record_peer_reachable_if_offline(&addr);
                        rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "success");
                        if let Some(late) = late {
                            late(response).await;
                        }
                    }
                    Ok(Err(status)) => {
                        if let Some(late_failure) = late_failure {
                            late_failure(()).await;
                        }
                        rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "error");
                        debug!(
                            event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                            state = "late_rpc_failed",
                            addr = %addr,
                            op,
                            tonic_code = ?status.code(),
                            tonic_message = status.message(),
                            "Abandoned remote lock RPC failed"
                        );
                    }
                    Err(_) => {
                        if let Some(late_failure) = late_failure {
                            late_failure(()).await;
                        }
                        rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "join_error");
                        debug!(
                            event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                            state = "late_rpc_panicked",
                            addr = %addr,
                            op,
                            "Abandoned remote lock RPC panicked"
                        );
                    }
                }
            }
            .in_current_span(),
        );
    }

    fn late_release_hook(&self, lock_id: LockId) -> LateCompletion<Response<GenerallyLockResponse>> {
        let client = self.clone();
        Some(Box::new(move |response: Response<GenerallyLockResponse>| {
            Box::pin(async move {
                if response.get_ref().success {
                    client
                        .release_late_acquisitions(vec![lock_id], AcquisitionCleanup::Confirmed)
                        .await;
                }
            })
        }))
    }

    fn late_release_batch_hook(&self, requests: Vec<(LockId, LockType)>) -> LateCompletion<Response<BatchGenerallyLockResponse>> {
        let client = self.clone();
        Some(Box::new(move |response: Response<BatchGenerallyLockResponse>| {
            Box::pin(async move {
                let mut confirmed = Vec::new();
                let mut uncertain = Vec::new();
                for (idx, (lock_id, lock_type)) in requests.into_iter().enumerate() {
                    match batch_acquire_outcome(&lock_id, lock_type, response.get_ref().results.get(idx)) {
                        BatchAcquireOutcome::Granted { .. } => confirmed.push(lock_id),
                        BatchAcquireOutcome::Ambiguous { .. } => uncertain.push(lock_id),
                        BatchAcquireOutcome::RetryableNoGrant { .. } | BatchAcquireOutcome::TerminalNoGrant { .. } => {}
                    }
                }
                tokio::join!(
                    async {
                        if !confirmed.is_empty() {
                            client
                                .release_late_acquisitions(confirmed, AcquisitionCleanup::Confirmed)
                                .await;
                        }
                    },
                    async {
                        if !uncertain.is_empty() {
                            client
                                .release_late_acquisitions(uncertain, AcquisitionCleanup::Uncertain)
                                .await;
                        }
                    },
                );
            })
        }))
    }

    fn late_release_failure_hook(&self, lock_ids: Vec<LockId>) -> LateFailure {
        let client = self.clone();
        Some(Box::new(move |_| {
            Box::pin(async move {
                // Absence cannot settle an acquisition whose grant is still
                // uncertain. Keep the conservative retry schedule.
                client
                    .release_late_acquisitions(lock_ids, AcquisitionCleanup::Uncertain)
                    .await;
            })
        }))
    }

    async fn request_release(&self, lock_id: &LockId) -> Result<GenerallyLockResponse> {
        let unlock_request = Self::create_unlock_request(lock_id);
        let request_string = serde_json::to_string(&unlock_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;
        let mut client = self.get_client().await?;
        let resource_summary = unlock_request.resource.to_string();
        let mut req = Request::new(GenerallyLockRequest { args: request_string });
        attach_lock_mutation_body_digest(&mut req)?;
        Ok(self
            .execute_rpc(
                "release",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.un_lock(req).await },
                RpcCleanup::new(None, None, None),
            )
            .await?
            .into_inner())
    }

    async fn request_release_batch(&self, lock_ids: &[LockId]) -> Result<BatchGenerallyLockResponse> {
        let unlock_requests = lock_ids.iter().map(Self::create_unlock_request).collect::<Vec<_>>();
        let mut client = self.get_client().await?;
        let resource_summary = Self::summarize_resources(&unlock_requests);
        let mut req = Request::new(BatchGenerallyLockRequest {
            args: unlock_requests
                .iter()
                .map(|request| {
                    serde_json::to_string(request).map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))
                })
                .collect::<Result<Vec<_>>>()?,
        });
        attach_lock_mutation_body_digest(&mut req)?;

        Ok(self
            .execute_rpc(
                "release_batch",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.un_lock_batch(req).await },
                RpcCleanup::new(None, None, None),
            )
            .await?
            .into_inner())
    }

    fn cleanup_batch_results(&self, lock_ids: &[LockId], results: &[GenerallyLockResult]) -> Vec<LockCleanupOutcome> {
        lock_ids
            .iter()
            .enumerate()
            .map(|(idx, lock_id)| {
                let outcome = results
                    .get(idx)
                    .and_then(|result| cleanup_outcome(result.success, result.error_info.as_deref(), result.release_status).ok())
                    .unwrap_or(LockCleanupOutcome::Unconfirmed);
                if matches!(outcome, LockCleanupOutcome::Released | LockCleanupOutcome::AlreadyAbsent) {
                    self.record_release_ack(lock_id, outcome);
                }
                outcome
            })
            .collect()
    }

    async fn cleanup_grants_batch(&self, lock_ids: &[LockId]) -> Result<Vec<LockCleanupOutcome>> {
        if lock_ids.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.request_release_batch(lock_ids).await?;
        Ok(self.cleanup_batch_results(lock_ids, &response.results))
    }

    /// A confirmed late grant can settle on authoritative absence. An
    /// uncertain acquisition must retain retries because release may race it.
    async fn release_late_acquisitions(&self, mut lock_ids: Vec<LockId>, acquisition: AcquisitionCleanup) {
        if !self.admit_late_release() {
            rustfs_io_metrics::lock_metrics::record_remote_lock_late_release("failed");
            let (log_warning, suppressed_timeouts) = self.timeout_log_decision("late_release", Self::rpc_timeout());
            if log_warning {
                warn!(
                    event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                    state = "budget_exhausted",
                    addr = %self.addr,
                    count = lock_ids.len(),
                    outcome = "budget_exhausted",
                    suppressed_timeouts,
                    "Skipped late remote lock release because the cleanup budget is exhausted"
                );
            }
            return;
        }

        let original_count = lock_ids.len();
        let mut outcome = "failed";
        for (attempt, delay) in std::iter::once(Duration::ZERO).chain(LATE_RELEASE_RETRY_DELAYS).enumerate() {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            let released = match acquisition {
                AcquisitionCleanup::Confirmed => self.cleanup_grants_batch(&lock_ids).await,
                AcquisitionCleanup::Uncertain => self.release_locks_batch(&lock_ids).await.map(|results| {
                    results
                        .into_iter()
                        .map(|released| {
                            if released {
                                LockCleanupOutcome::Released
                            } else {
                                LockCleanupOutcome::Unconfirmed
                            }
                        })
                        .collect()
                }),
            };
            match released {
                Ok(released) => {
                    lock_ids = lock_ids
                        .into_iter()
                        .zip(released)
                        .filter_map(|(lock_id, outcome)| matches!(outcome, LockCleanupOutcome::Unconfirmed).then_some(lock_id))
                        .collect();
                    if lock_ids.is_empty() {
                        outcome = "released";
                        break;
                    }
                    outcome = if attempt == LATE_RELEASE_RETRY_DELAYS.len() {
                        "partial"
                    } else {
                        "failed"
                    };
                }
                Err(_) => {
                    outcome = "failed";
                }
            }
        }
        self.release_late_release();
        rustfs_io_metrics::lock_metrics::record_remote_lock_late_release(outcome);
        if outcome == "released" {
            debug!(
                event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                state = "late_cleanup_completed",
                addr = %self.addr,
                count = original_count,
                "Confirmed cleanup of late remote lock acquisitions"
            );
        } else {
            let (log_warning, suppressed_timeouts) = self.timeout_log_decision("late_release", Self::rpc_timeout());
            if log_warning {
                warn!(
                    event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                    state = "late_cleanup_unconfirmed",
                    addr = %self.addr,
                    count = original_count,
                    outcome,
                    suppressed_timeouts,
                    "Could not confirm cleanup of every late remote lock acquisition"
                );
            } else {
                debug!(
                    event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                    state = "warning_suppressed",
                    addr = %self.addr,
                    count = original_count,
                    outcome,
                    suppressed_timeouts,
                    "Suppressed repeated late remote lock cleanup warning"
                );
            }
        }
    }

    async fn finish_rpc_eviction(
        &self,
        op: &'static str,
        reason: &str,
        resource_summary: &str,
        trigger: EvictionTrigger,
        deadline: Duration,
        background: bool,
    ) {
        if background {
            let client = self.clone();
            let reason = reason.to_owned();
            let resource_summary = resource_summary.to_owned();
            drop(tokio::spawn(
                async move {
                    client
                        .maybe_evict_connection(op, &reason, &resource_summary, trigger, deadline)
                        .await;
                }
                .in_current_span(),
            ));
        } else {
            self.maybe_evict_connection(op, reason, resource_summary, trigger, deadline)
                .await;
        }
    }

    async fn execute_rpc<T, Fut>(
        &self,
        op: &'static str,
        resource_summary: &str,
        deadline: Duration,
        future: Fut,
        cleanup: RpcCleanup<'_, T>,
    ) -> std::result::Result<T, LockError>
    where
        Fut: Future<Output = std::result::Result<T, tonic::Status>> + Send + 'static,
        T: Send + 'static,
    {
        self.execute_rpc_until(op, resource_summary, Instant::now() + deadline, future, cleanup)
            .await
    }

    async fn execute_rpc_until<T, Fut>(
        &self,
        op: &'static str,
        resource_summary: &str,
        rpc_deadline: Instant,
        future: Fut,
        cleanup: RpcCleanup<'_, T>,
    ) -> std::result::Result<T, LockError>
    where
        Fut: Future<Output = std::result::Result<T, tonic::Status>> + Send + 'static,
        T: Send + 'static,
    {
        let deadline = rpc_deadline.saturating_duration_since(Instant::now());
        let batch_deadline = cleanup.batch_deadline.clone();
        let mut rpc = InFlightRpc {
            client: self,
            op,
            resource_summary,
            future: Some(AssertUnwindSafe(future).catch_unwind().boxed()),
            cleanup: Some(cleanup),
            span: tracing::Span::current(),
        };
        let mut outcome = match rpc.future.as_mut() {
            Some(future) => tokio::select! {
                // A response becoming ready on the deadline tick must not turn an
                // expired caller budget into a transport error and channel eviction.
                biased;
                _ = tokio::time::sleep_until(rpc_deadline) => None,
                outcome = future => Some(outcome),
            },
            None => {
                return Err(LockError::internal("remote lock RPC has no in-flight future"));
            }
        };
        if batch_deadline.is_some()
            && Instant::now() >= rpc_deadline
            && let Some(completed) = outcome.take()
        {
            // A single poll can finish synchronous work after the timer was
            // polled. Preserve that response for late cleanup instead of
            // accepting an expired grant or dropping its cleanup responsibility.
            rpc.future = Some(futures::future::ready(completed).boxed());
        }
        if !matches!(&outcome, Some(Ok(Ok(_))))
            && batch_deadline
                .as_ref()
                .is_some_and(BatchLockRpcDeadline::cancel_before_dispatch)
        {
            // Readiness, signing or the channel queue never sent this request's
            // payload. Cancellation proves no grant and must not open the peer's breaker.
            rpc.future.take();
            rpc.cleanup.take();
            return match outcome {
                Some(Ok(Err(status))) => Err(LockError::internal(format!("{op} RPC failed before dispatch: {status}"))),
                Some(Err(_)) => Err(LockError::internal(format!("{op} RPC future panicked before dispatch"))),
                _ => Err(LockError::timeout(
                    format!("remote lock RPC {op} before dispatch on {}", self.addr),
                    deadline,
                )),
            };
        }
        if outcome.is_some() {
            rpc.future.take();
        }
        let Some(RpcCleanup {
            late: _,
            late_failure,
            admission,
            batch_deadline: _,
        }) = rpc.cleanup.as_mut()
        else {
            return Err(LockError::internal("remote lock RPC has no cleanup owner"));
        };
        let token = admission.as_ref().and_then(|guard| guard.admission);
        let result = match outcome {
            Some(Ok(Ok(response))) => {
                self.record_rpc_success(op, token);
                Ok(response)
            }
            Some(Ok(Err(err))) => {
                let reason = err.to_string();
                if err.code() == tonic::Code::DeadlineExceeded {
                    rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_timeout(&self.addr, op);
                    self.record_request_timeout(op, deadline);
                    let (log_timeout, suppressed_timeouts) = self.timeout_log_decision(op, deadline);
                    if Self::is_scanner_leader_lock(resource_summary) {
                        debug!(
                            addr = %self.addr,
                            op,
                            timeout_ms = deadline.as_millis(),
                            resource_summary,
                            "Remote lock RPC returned deadline exceeded for scanner leader lock"
                        );
                    } else if log_timeout {
                        warn!(
                            addr = %self.addr,
                            op,
                            timeout_ms = deadline.as_millis(),
                            resource_summary,
                            suppressed_timeouts,
                            "Remote lock RPC returned deadline exceeded"
                        );
                    } else {
                        debug!(
                            addr = %self.addr,
                            op,
                            timeout_ms = deadline.as_millis(),
                            resource_summary,
                            suppressed_timeouts,
                            "Suppressed repeated remote lock RPC deadline"
                        );
                    }
                    if let Some(late_failure) = late_failure.take() {
                        drop(tokio::spawn(late_failure(()).in_current_span()));
                    }
                    self.finish_rpc_eviction(
                        op,
                        &reason,
                        resource_summary,
                        EvictionTrigger::Timeout,
                        deadline,
                        batch_deadline.is_some(),
                    )
                    .await;
                    return Err(LockError::timeout(format!("remote lock RPC {op} on {}", self.addr), deadline));
                }
                // Only evict (and re-dial) the cached channel when the failure is a genuine
                // transport problem. A server-produced application status (auth denied, peer
                // lock service not ready, invalid args, ...) arrives on a perfectly healthy
                // channel; evicting it just churns the connection and pushes the peer toward
                // the offline threshold for no benefit. See issue #4567.
                let transport_failure = Self::is_transport_failure(&err);
                if transport_failure {
                    self.record_request_timeout(op, deadline);
                }
                if Self::is_scanner_leader_lock(resource_summary) {
                    debug!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        tonic_code = ?err.code(),
                        tonic_message = err.message(),
                        transport_failure,
                        "Remote lock RPC returned tonic error for scanner leader lock"
                    );
                } else if transport_failure {
                    let (log_failure, suppressed_failures) = self.timeout_log_decision(op, deadline);
                    if log_failure {
                        warn!(
                            addr = %self.addr,
                            op,
                            timeout_ms = deadline.as_millis(),
                            resource_summary,
                            tonic_code = ?err.code(),
                            tonic_message = err.message(),
                            transport_failure,
                            suppressed_failures,
                            "Remote lock RPC returned transport error"
                        );
                    } else {
                        debug!(
                            addr = %self.addr,
                            op,
                            timeout_ms = deadline.as_millis(),
                            resource_summary,
                            tonic_code = ?err.code(),
                            suppressed_failures,
                            "Suppressed repeated remote lock transport error"
                        );
                    }
                } else {
                    warn!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        tonic_code = ?err.code(),
                        tonic_message = err.message(),
                        transport_failure,
                        "Remote lock RPC returned tonic error"
                    );
                }
                if transport_failure || batch_deadline.is_some() {
                    // A submitted batch's application error cannot prove that no item was granted.
                    if let Some(late_failure) = late_failure.take() {
                        drop(tokio::spawn(late_failure(()).in_current_span()));
                    }
                }
                if transport_failure {
                    self.finish_rpc_eviction(
                        op,
                        &reason,
                        resource_summary,
                        EvictionTrigger::Transport,
                        deadline,
                        batch_deadline.is_some(),
                    )
                    .await;
                }
                Err(LockError::internal(format!("{op} RPC failed: {reason}")))
            }
            Some(Err(_)) => {
                warn!(
                    event = EVENT_REMOTE_LOCK_RPC_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REMOTE_LOCK,
                    state = "rpc_panicked",
                    addr = %self.addr,
                    op,
                    resource_summary,
                    "Remote lock RPC panicked"
                );
                if let Some(late_failure) = late_failure.take() {
                    drop(tokio::spawn(late_failure(()).in_current_span()));
                }
                Err(LockError::internal(format!("{op} RPC future panicked")))
            }
            None => {
                let reason = format!("RPC timed out after {deadline:?}");
                rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_timeout(&self.addr, op);
                self.record_request_timeout(op, deadline);
                let (log_timeout, suppressed_timeouts) = self.timeout_log_decision(op, deadline);
                if Self::is_scanner_leader_lock(resource_summary) {
                    debug!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        "Remote lock RPC timed out for scanner leader lock"
                    );
                } else if log_timeout {
                    warn!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        suppressed_timeouts,
                        "Remote lock RPC timed out"
                    );
                } else {
                    debug!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        suppressed_timeouts,
                        "Suppressed repeated remote lock RPC timeout"
                    );
                }
                // Continue draining the abandoned stream while channel eviction waits.
                drop(rpc);
                self.finish_rpc_eviction(
                    op,
                    &reason,
                    resource_summary,
                    EvictionTrigger::Timeout,
                    deadline,
                    batch_deadline.is_some(),
                )
                .await;
                return Err(LockError::timeout(format!("remote lock RPC {op} on {}", self.addr), deadline));
            }
        };
        if rpc.future.is_none() {
            rpc.cleanup.take();
        }
        result
    }

    fn rpc_timeout_failure_response(request: &LockRequest, err: &LockError) -> LockResponse {
        LockResponse::failure(format!("Remote lock RPC timed out: {err}"), request.acquire_timeout)
    }

    fn rpc_failure_response(_request: &LockRequest, err: &LockError) -> LockResponse {
        LockResponse::failure(format!("Remote lock RPC failed: {err}"), Duration::ZERO)
    }

    fn rpc_backoff_failure_response(request: &LockRequest) -> LockResponse {
        LockResponse::failure("Remote lock RPC timed out: peer request breaker is open", request.acquire_timeout)
    }

    fn build_lock_info(request: &LockRequest, lock_info_json: Option<String>) -> LockInfo {
        if let Some(lock_info_json) = lock_info_json {
            match serde_json::from_str::<LockInfo>(&lock_info_json) {
                Ok(info) => info,
                Err(e) => {
                    warn!("Failed to deserialize lock_info from response: {}, using request data", e);
                    LockInfo {
                        id: request.lock_id.clone(),
                        resource: request.resource.clone(),
                        lock_type: request.lock_type,
                        status: LockStatus::Acquired,
                        owner: request.owner.clone(),
                        acquired_at: std::time::SystemTime::now(),
                        expires_at: std::time::SystemTime::now() + request.ttl,
                        last_refreshed: std::time::SystemTime::now(),
                        metadata: request.metadata.clone(),
                        priority: request.priority,
                        wait_start_time: None,
                    }
                }
            }
        } else {
            LockInfo {
                id: request.lock_id.clone(),
                resource: request.resource.clone(),
                lock_type: request.lock_type,
                status: LockStatus::Acquired,
                owner: request.owner.clone(),
                acquired_at: std::time::SystemTime::now(),
                expires_at: std::time::SystemTime::now() + request.ttl,
                last_refreshed: std::time::SystemTime::now(),
                metadata: request.metadata.clone(),
                priority: request.priority,
                wait_start_time: None,
            }
        }
    }

    fn unknown_lock_info(lock_id: &LockId) -> LockInfo {
        LockInfo {
            id: lock_id.clone(),
            resource: lock_id.resource.clone(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "unknown".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        }
    }
}

#[async_trait]
impl LockClient for RemoteClient {
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        let admission = match self.admit_lock_request_guard("lock") {
            Some(admission) => admission,
            None => return Ok(Self::rpc_backoff_failure_response(request)),
        };

        let rpc_timeout = Self::rpc_timeout();
        let bounded_request = Self::bounded_acquire_request(request, rpc_timeout);
        let mut client = match self.get_client().await {
            Ok(client) => client,
            Err(err) => {
                self.record_request_timeout("lock", rpc_timeout);
                return Err(err);
            }
        };
        let resource_summary = request.resource.to_string();
        let args = match serde_json::to_string(&bounded_request) {
            Ok(args) => args,
            Err(err) => {
                return Err(LockError::internal(format!("Failed to serialize request: {err}")));
            }
        };
        let mut req = Request::new(GenerallyLockRequest { args });
        req.set_timeout(rpc_timeout);
        if let Err(err) = attach_lock_mutation_body_digest(&mut req) {
            return Err(err.into());
        }
        let late = self.late_release_hook(request.lock_id.clone());
        let late_failure = self.late_release_failure_hook(vec![request.lock_id.clone()]);

        let resp = match self
            .execute_rpc(
                "lock",
                &resource_summary,
                rpc_timeout,
                async move { client.lock(req).await },
                RpcCleanup::new(late, late_failure, Some(admission)),
            )
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(err @ LockError::Timeout { .. }) => {
                return Ok(Self::rpc_timeout_failure_response(request, &err));
            }
            Err(err) => return Ok(Self::rpc_failure_response(request, &err)),
        };

        // Check if the lock acquisition was successful
        if resp.success {
            Ok(LockResponse::success(
                Self::build_lock_info(request, resp.lock_info),
                std::time::Duration::ZERO,
            ))
        } else {
            // Lock acquisition failed
            Ok(LockResponse::failure(
                resp.error_info
                    .unwrap_or_else(|| "Lock acquisition failed on remote server".to_string()),
                std::time::Duration::ZERO,
            ))
        }
    }

    async fn acquire_locks_batch(&self, requests: &[LockRequest]) -> Result<Vec<LockResponse>> {
        let deadline = Instant::now() + Self::rpc_timeout();
        let outcomes = self.acquire_locks_batch_until(requests, deadline).await;
        Ok(requests
            .iter()
            .zip(outcomes)
            .map(|(request, outcome)| match outcome {
                BatchAcquireOutcome::Granted { lock_info, wait_time } => LockResponse::success(
                    lock_info
                        .map(|info| *info)
                        .unwrap_or_else(|| Self::build_lock_info(request, None)),
                    wait_time,
                ),
                BatchAcquireOutcome::Ambiguous {
                    error: error @ LockError::Timeout { .. },
                    ..
                } => Self::rpc_timeout_failure_response(request, &error),
                BatchAcquireOutcome::Ambiguous { error, .. } => Self::rpc_failure_response(request, &error),
                BatchAcquireOutcome::RetryableNoGrant { error } | BatchAcquireOutcome::TerminalNoGrant { error } => {
                    // An explicit contention result is a normal backend failure, not a transport failure.
                    match error {
                        error @ LockError::Timeout { .. } => Self::rpc_timeout_failure_response(request, &error),
                        LockError::Internal { message } => LockResponse::failure(message, Duration::ZERO),
                        error => LockResponse::failure(error.to_string(), Duration::ZERO),
                    }
                }
            })
            .collect())
    }

    async fn acquire_locks_batch_until(&self, requests: &[LockRequest], total_deadline: Instant) -> Vec<BatchAcquireOutcome> {
        let entered = Instant::now();
        let rpc_timeout = Self::rpc_timeout();
        let rpc_deadline = entered.checked_add(rpc_timeout).unwrap_or(total_deadline).min(total_deadline);
        let no_grant = |error: LockError| {
            requests
                .iter()
                .map(|_| BatchAcquireOutcome::RetryableNoGrant { error: error.clone() })
                .collect()
        };
        let expired = || LockError::timeout(format!("remote lock RPC lock_batch before dispatch on {}", self.addr), rpc_timeout);
        if requests.is_empty() {
            return Vec::new();
        }
        if rpc_deadline <= entered {
            return no_grant(expired());
        }
        let admission = match self.admit_lock_request_guard("lock_batch") {
            Some(admission) => admission,
            None => {
                return no_grant(LockError::internal("Remote lock RPC timed out: peer request breaker is open"));
            }
        };
        let mut client = match timeout_at(rpc_deadline, self.get_client()).await {
            Ok(Ok(client)) => client,
            Ok(Err(error)) => {
                return no_grant(LockError::internal(format!("Remote lock RPC failed: {error}")));
            }
            Err(_) => return no_grant(expired()),
        };
        let remaining = rpc_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return no_grant(expired());
        }
        // Leave half the available RPC budget for transport, encoding and response delivery.
        // One second bounds backend contention only; it is not an RPC deadline.
        let acquire_wait = (remaining / 2).min(Duration::from_secs(1));
        let bounded_requests = requests
            .iter()
            .map(|request| Self::bounded_acquire_request(request, acquire_wait))
            .collect::<Vec<_>>();
        let max_acquire_wait = bounded_requests
            .iter()
            .map(|request| request.acquire_timeout)
            .max()
            .unwrap_or_default();
        let args = match bounded_requests
            .iter()
            .map(|request| {
                serde_json::to_string(request)
                    .map_err(|error| LockError::internal(format!("Failed to serialize request: {error}")))
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(args) => args,
            Err(error) => {
                return requests
                    .iter()
                    .map(|_| BatchAcquireOutcome::TerminalNoGrant { error: error.clone() })
                    .collect();
            }
        };
        let mut req = Request::new(BatchGenerallyLockRequest { args });
        if let Err(error) = attach_lock_mutation_body_digest(&mut req) {
            return requests
                .iter()
                .map(|_| BatchAcquireOutcome::TerminalNoGrant {
                    error: LockError::internal(error.to_string()),
                })
                .collect();
        }
        // Encoding cannot be preempted, so recheck its consumed budget before submission.
        if rpc_deadline.saturating_duration_since(Instant::now()) <= max_acquire_wait {
            return no_grant(expired());
        }
        let dispatch = BatchLockRpcDeadline::new(rpc_deadline, max_acquire_wait);
        req.extensions_mut().insert(dispatch.clone());
        let identities = requests
            .iter()
            .map(|request| (request.lock_id.clone(), request.lock_type))
            .collect();
        let late = self.late_release_batch_hook(identities);
        let late_failure = self.late_release_failure_hook(requests.iter().map(|request| request.lock_id.clone()).collect());
        let resource_summary = Self::summarize_resources(requests);
        let response = match self
            .execute_rpc_until(
                "lock_batch",
                &resource_summary,
                rpc_deadline,
                async move { client.lock_batch(req).await },
                RpcCleanup::new(late, late_failure, Some(admission)).with_batch_deadline(dispatch.clone()),
            )
            .await
        {
            Ok(response) => response.into_inner(),
            Err(error) if !dispatch.was_dispatched() => return no_grant(error),
            Err(error) => {
                return requests
                    .iter()
                    .map(|_| BatchAcquireOutcome::Ambiguous {
                        error: error.clone(),
                        cleanup_owned_by_client: true,
                    })
                    .collect();
            }
        };
        let mut uncertain = Vec::new();
        let outcomes: Vec<_> = requests
            .iter()
            .enumerate()
            .map(|(idx, request)| {
                let outcome = batch_acquire_outcome(&request.lock_id, request.lock_type, response.results.get(idx));
                if let BatchAcquireOutcome::Ambiguous { error, .. } = outcome {
                    uncertain.push(request.lock_id.clone());
                    BatchAcquireOutcome::Ambiguous {
                        error,
                        cleanup_owned_by_client: true,
                    }
                } else {
                    outcome
                }
            })
            .collect();
        if Instant::now() >= rpc_deadline {
            // Decoding per-item lock metadata is synchronous and can consume
            // the remaining budget after the transport future has completed.
            let identities = requests
                .iter()
                .map(|request| (request.lock_id.clone(), request.lock_type))
                .collect();
            if let Some(late) = self.late_release_batch_hook(identities) {
                drop(tokio::spawn(late(Response::new(response)).in_current_span()));
            }
            return outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    BatchAcquireOutcome::Granted { .. } => BatchAcquireOutcome::Ambiguous {
                        error: LockError::timeout(format!("remote lock RPC lock_batch on {}", self.addr), rpc_timeout),
                        cleanup_owned_by_client: true,
                    },
                    outcome => outcome,
                })
                .collect();
        }
        if !uncertain.is_empty() {
            // Normal responses have consumed the late hook; establish a new owner before returning.
            if let Some(cleanup) = self.late_release_failure_hook(uncertain) {
                drop(tokio::spawn(cleanup(()).in_current_span()));
            }
        }
        outcomes
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        let resp = self.request_release(lock_id).await?;
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        if resp.success {
            self.record_release_ack(lock_id, LockCleanupOutcome::Released);
        }
        Ok(resp.success)
    }

    async fn cleanup_grant(&self, lock_id: &LockId) -> Result<LockCleanupOutcome> {
        let resp = self.request_release(lock_id).await?;
        let outcome = cleanup_outcome(resp.success, resp.error_info.as_deref(), resp.release_status)?;
        if matches!(outcome, LockCleanupOutcome::Released | LockCleanupOutcome::AlreadyAbsent) {
            self.record_release_ack(lock_id, outcome);
        }
        Ok(outcome)
    }

    async fn release_locks_batch(&self, lock_ids: &[LockId]) -> Result<Vec<bool>> {
        if lock_ids.is_empty() {
            return Ok(Vec::new());
        }
        let resp = self.request_release_batch(lock_ids).await?;
        Ok(lock_ids
            .iter()
            .enumerate()
            .map(|(idx, lock_id)| {
                let result = resp.results.get(idx);
                if result.is_some_and(|result| result.success && result.error_info.is_none()) {
                    self.record_release_ack(lock_id, LockCleanupOutcome::Released);
                }
                result.map(|result| result.success).unwrap_or(false)
            })
            .collect())
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote refresh for {}", lock_id);
        let refresh_request = Self::create_unlock_request(lock_id);
        let mut client = self.get_client().await?;
        let resource_summary = refresh_request.resource.to_string();
        let mut req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&refresh_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        attach_lock_mutation_body_digest(&mut req)?;
        let resp = self
            .execute_rpc(
                "refresh",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.refresh(req).await },
                RpcCleanup::new(None, None, None),
            )
            .await?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote force_release for {}", lock_id);
        let force_request = Self::create_unlock_request(lock_id);
        let mut client = self.get_client().await?;
        let resource_summary = force_request.resource.to_string();
        let mut req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&force_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        attach_lock_mutation_body_digest(&mut req)?;
        let resp = self
            .execute_rpc(
                "force_release",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.force_un_lock(req).await },
                RpcCleanup::new(None, None, None),
            )
            .await?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        info!("remote check_status for {}", lock_id);

        // Since there's no direct status query in the gRPC service,
        // we attempt a non-blocking lock acquisition to check if the resource is available
        let probe_lock_id = LockId::new_unique(&lock_id.resource);
        let rpc_timeout = Self::rpc_timeout();
        let status_request = Self::create_unlock_request(&probe_lock_id).with_ttl(rpc_timeout);
        let bounded_status_request = Self::bounded_acquire_request(&status_request, rpc_timeout);
        let resource_summary = status_request.resource.to_string();
        let mut client = self.get_client().await?;
        let args = serde_json::to_string(&bounded_status_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;

        // Try to acquire a very short-lived lock to test availability
        let mut req = Request::new(GenerallyLockRequest { args: args.clone() });
        req.set_timeout(rpc_timeout);
        attach_lock_mutation_body_digest(&mut req)?;
        // A probe lock granted after the deadline must not linger on the peer.
        let late = self.late_release_hook(probe_lock_id.clone());
        let late_failure = self.late_release_failure_hook(vec![probe_lock_id]);

        // Try exclusive lock first with very short timeout
        let resp = match self
            .execute_rpc(
                "check_status",
                &resource_summary,
                rpc_timeout,
                async move { client.lock(req).await },
                RpcCleanup::new(late, late_failure, None),
            )
            .await
        {
            Ok(response) => response.into_inner(),
            Err(_) => return Ok(Some(Self::unknown_lock_info(lock_id))),
        };

        if resp.success {
            // If we successfully acquired the lock, the resource was free.
            // Immediately release it on a best-effort basis.
            let mut release_req = Request::new(GenerallyLockRequest { args });
            release_req.set_timeout(rpc_timeout);
            attach_lock_mutation_body_digest(&mut release_req)?;
            if let Ok(mut client) = self.get_client().await {
                let _ = self
                    .execute_rpc(
                        "check_status_release",
                        &resource_summary,
                        Self::rpc_timeout(),
                        async move { client.un_lock(release_req).await },
                        RpcCleanup::new(None, None, None),
                    )
                    .await;
            }

            Ok(None)
        } else {
            // Lock acquisition failed, meaning someone is holding it.
            // We can't determine the exact details remotely, so return a generic status.
            Ok(Some(Self::unknown_lock_info(lock_id)))
        }
    }

    async fn get_stats(&self) -> Result<LockStats> {
        info!("remote get_stats from {}", self.addr);

        // Since there's no direct statistics endpoint in the gRPC service,
        // we return basic stats indicating this is a remote client
        let stats = LockStats {
            last_updated: std::time::SystemTime::now(),
            ..Default::default()
        };

        // We could potentially enhance this by:
        // 1. Keeping local counters of operations performed
        // 2. Adding a stats gRPC method to the service
        // 3. Querying server health endpoints

        // For now, return minimal stats indicating remote connectivity
        Ok(stats)
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        let online_timeout = Self::online_check_timeout();
        let mut client = match timeout(online_timeout, self.get_client()).await {
            Ok(Ok(client)) => client,
            Ok(Err(err)) => {
                debug!(
                    addr = %self.addr,
                    timeout_ms = online_timeout.as_millis(),
                    error = %err,
                    "remote lock client online check failed"
                );
                return false;
            }
            Err(_) => {
                warn!(
                    addr = %self.addr,
                    timeout_ms = online_timeout.as_millis(),
                    "remote lock client online check timed out while dialing"
                );
                return false;
            }
        };
        let ping_req = Request::new(Self::build_ping_request());
        match self
            .execute_rpc(
                "ping",
                Self::ONLINE_CHECK_RESOURCE,
                online_timeout,
                async move { client.ping(ping_req).await },
                RpcCleanup::new(None, None, None),
            )
            .await
        {
            Ok(_) => {
                debug!(addr = %self.addr, timeout_ms = online_timeout.as_millis(), "remote lock client is online");
                true
            }
            Err(err) => {
                debug!(
                    addr = %self.addr,
                    timeout_ms = online_timeout.as_millis(),
                    error = %err,
                    "remote lock client online check failed"
                );
                false
            }
        }
    }

    async fn is_local(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_outcome_requires_authoritative_absence() {
        assert_eq!(
            cleanup_outcome(false, Some("diagnostic text is not a protocol"), LockReleaseStatus::AlreadyAbsent.into())
                .expect("structured absence is authoritative"),
            LockCleanupOutcome::AlreadyAbsent
        );
        assert!(cleanup_outcome(false, Some("lock not found for release"), LockReleaseStatus::Unspecified.into()).is_err());
        assert_eq!(
            cleanup_outcome(false, None, LockReleaseStatus::Unspecified.into()).expect("legacy result is uncertain"),
            LockCleanupOutcome::Unconfirmed
        );
        for status in [LockReleaseStatus::Unspecified, LockReleaseStatus::Released] {
            assert_eq!(
                cleanup_outcome(true, None, status.into()).expect("positive release ACK"),
                LockCleanupOutcome::Released
            );
        }
    }

    #[test]
    fn cleanup_outcome_rejects_inconsistent_and_unknown_statuses() {
        for (success, error, status) in [
            (true, None, LockReleaseStatus::AlreadyAbsent.into()),
            (false, None, LockReleaseStatus::Released.into()),
            (true, Some("backend error"), LockReleaseStatus::Released.into()),
            (true, None, 99),
            (false, Some("backend error"), 99),
        ] {
            assert!(
                cleanup_outcome(success, error, status).is_err(),
                "must retain cleanup ownership for status {status}"
            );
        }
    }

    #[test]
    fn cleanup_batch_results_preserves_each_lock_id_and_unknown_entries() {
        let client = RemoteClient::new("http://cleanup-batch-results-test".to_string());
        let lock_ids = (0..5)
            .map(|idx| LockId::new_unique(&ObjectKey::new("bucket", format!("cleanup-{idx}"))))
            .collect::<Vec<_>>();
        let results = vec![
            GenerallyLockResult {
                success: true,
                release_status: LockReleaseStatus::Released.into(),
                ..Default::default()
            },
            GenerallyLockResult {
                success: false,
                error_info: Some("already released".to_string()),
                release_status: LockReleaseStatus::AlreadyAbsent.into(),
                ..Default::default()
            },
            GenerallyLockResult {
                success: false,
                error_info: Some("lock not found for release".to_string()),
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                release_status: 99,
                ..Default::default()
            },
        ];
        assert_eq!(
            client.cleanup_batch_results(&lock_ids, &results),
            vec![
                LockCleanupOutcome::Released,
                LockCleanupOutcome::AlreadyAbsent,
                LockCleanupOutcome::Unconfirmed,
                LockCleanupOutcome::Unconfirmed,
                LockCleanupOutcome::Unconfirmed,
            ],
            "missing or unrecognized entries must not hide confirmed cleanup of other IDs"
        );
    }
    use crate::runtime::sources as runtime_sources;
    use rustfs_lock::{ObjectKey, types::LockPriority};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tonic::transport::Endpoint as TonicEndpoint;

    async fn spawn_hanging_listener() -> Option<(String, JoinHandle<()>)> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = format!("http://{}", listener.local_addr().expect("listener local address should be available"));
        let task = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _stream = stream;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        Some((addr, task))
    }

    async fn closed_listener_addr() -> Option<String> {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let addr = format!("http://{}", listener.local_addr().expect("listener local address should be available"));
        drop(listener);
        Some(addr)
    }

    async fn cache_lazy_channel(addr: &str) {
        let channel = TonicEndpoint::from_shared(addr.to_string()).unwrap().connect_lazy();
        runtime_sources::cache_test_node_channel(addr.to_string(), channel).await;
    }

    fn ensure_test_rpc_secret() {
        runtime_sources::ensure_test_rpc_secret();
    }

    fn test_lock_request(timeout_duration: Duration) -> LockRequest {
        LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner-a")
            .with_acquire_timeout(timeout_duration)
            .with_priority(LockPriority::Normal)
    }

    #[test]
    fn eviction_verdict_distinguishes_slow_peers_from_dead_channels() {
        let now = Instant::now() + Duration::from_secs(3600);
        let window = Duration::from_secs(6);
        let cooldown = Duration::from_secs(5);

        let idle = LockPeerChannelHealth::default();
        assert_eq!(
            eviction_verdict(&idle, now, EvictionTrigger::Timeout, window, cooldown),
            EvictionVerdict::Evict
        );

        let serving = LockPeerChannelHealth {
            last_success: Some(now - Duration::from_secs(1)),
            ..Default::default()
        };
        assert_eq!(
            eviction_verdict(&serving, now, EvictionTrigger::Timeout, window, cooldown),
            EvictionVerdict::PeerRecentlyServed,
            "a timeout on a peer that just answered is load, not a dead channel"
        );
        assert_eq!(
            eviction_verdict(&serving, now, EvictionTrigger::Transport, window, cooldown),
            EvictionVerdict::Evict,
            "a transport failure is reported by the channel itself and still evicts"
        );

        let quiet = LockPeerChannelHealth {
            last_success: Some(now - Duration::from_secs(30)),
            ..Default::default()
        };
        assert_eq!(
            eviction_verdict(&quiet, now, EvictionTrigger::Timeout, window, cooldown),
            EvictionVerdict::Evict
        );

        let just_evicted = LockPeerChannelHealth {
            last_eviction: Some(now - Duration::from_secs(1)),
            ..Default::default()
        };
        assert_eq!(
            eviction_verdict(&just_evicted, now, EvictionTrigger::Timeout, window, cooldown),
            EvictionVerdict::CoolingDown
        );
        assert_eq!(
            eviction_verdict(&just_evicted, now, EvictionTrigger::Transport, window, cooldown),
            EvictionVerdict::CoolingDown
        );

        let cooled = LockPeerChannelHealth {
            last_eviction: Some(now - Duration::from_secs(10)),
            ..Default::default()
        };
        assert_eq!(
            eviction_verdict(&cooled, now, EvictionTrigger::Timeout, window, cooldown),
            EvictionVerdict::Evict
        );
    }

    #[test]
    fn request_breaker_opens_after_three_timeouts_and_allows_one_probe() {
        let addr = "http://breaker-test";
        let client = RemoteClient::new(addr.to_string());
        reset_lock_peer_health_for_test(addr);

        for _ in 0..LOCK_RPC_REQUEST_BACKOFF_THRESHOLD {
            client.record_request_timeout("lock", Duration::from_millis(100));
        }

        assert!(
            client.admit_lock_request("lock").is_none(),
            "open breaker must reject requests during backoff"
        );
        with_lock_peer_health(addr, |health| {
            health.request_backoff_until = Some(Instant::now() - Duration::from_millis(1));
            health.request_probe_generation = None;
            health.request_in_flight = 0;
        });

        let probe = client
            .admit_lock_request("lock")
            .expect("half-open breaker must admit one probe");
        assert!(probe.is_probe());
        assert!(client.admit_lock_request("lock").is_none(), "only one half-open probe may run");
        client.release_lock_request("lock", Some(probe));
        assert!(lock_peer_health_for_test(addr).request_probe_generation.is_none());
        reset_lock_peer_health_for_test(addr);
    }

    #[test]
    fn old_request_completion_cannot_clear_a_new_probe() {
        let addr = "http://breaker-token-test";
        let client = RemoteClient::new(addr.to_string());
        reset_lock_peer_health_for_test(addr);
        with_lock_peer_health(addr, |health| {
            health.request_backoff_until = Some(Instant::now() - Duration::from_millis(1));
            health.request_probe_generation = None;
            health.request_in_flight = 1;
        });

        let probe = client.admit_lock_request("lock").expect("probe should be admitted");
        client.release_lock_request("lock", Some(LockRequestAdmission { probe_generation: None }));
        assert!(lock_peer_health_for_test(addr).request_probe_generation.is_some());
        client.record_rpc_success("lock", Some(LockRequestAdmission { probe_generation: None }));
        assert!(lock_peer_health_for_test(addr).request_probe_generation.is_some());
        client.release_lock_request("lock", Some(probe));
        assert!(lock_peer_health_for_test(addr).request_probe_generation.is_none());
        reset_lock_peer_health_for_test(addr);
    }

    #[test]
    fn stale_probe_completion_cannot_clear_a_later_generation() {
        let addr = "http://breaker-generation-test";
        let client = RemoteClient::new(addr.to_string());
        reset_lock_peer_health_for_test(addr);
        with_lock_peer_health(addr, |health| {
            health.request_backoff_until = Some(Instant::now() - Duration::from_millis(1));
        });

        let first = client.admit_lock_request("lock").expect("first probe should be admitted");
        client.record_request_timeout("lock", Duration::from_millis(10));
        with_lock_peer_health(addr, |health| {
            health.request_backoff_until = Some(Instant::now() - Duration::from_millis(1));
            health.request_in_flight = 0;
            health.request_probe_generation = None;
            health.request_consecutive_timeouts = 0;
        });
        let second = client.admit_lock_request("lock").expect("second probe should be admitted");
        assert_ne!(first.probe_generation, second.probe_generation);

        client.release_lock_request("lock", Some(first));
        assert_eq!(lock_peer_health_for_test(addr).request_probe_generation, second.probe_generation);
        client.release_lock_request("lock", Some(second));
        reset_lock_peer_health_for_test(addr);
    }

    #[test]
    fn bounded_acquire_request_matches_transport_deadline() {
        let request = test_lock_request(Duration::from_secs(30));
        let bounded = RemoteClient::bounded_acquire_request(&request, Duration::from_secs(3));
        assert_eq!(bounded.acquire_timeout, Duration::from_secs(3));

        let request = test_lock_request(Duration::from_secs(1));
        let bounded = RemoteClient::bounded_acquire_request(&request, Duration::from_secs(3));
        assert_eq!(bounded.acquire_timeout, Duration::from_secs(1));
    }

    #[test]
    fn batch_acquire_classification_preserves_legacy_grants_and_requires_explicit_no_grant() {
        let request = test_lock_request(Duration::from_secs(30));
        let classify = |result: Option<&GenerallyLockResult>| batch_acquire_outcome(&request.lock_id, request.lock_type, result);
        let legacy_grant = GenerallyLockResult {
            success: true,
            ..Default::default()
        };
        assert!(matches!(
            classify(Some(&legacy_grant)),
            BatchAcquireOutcome::Granted { lock_info: None, .. }
        ));
        let info = RemoteClient::build_lock_info(&request, None);
        let grant = GenerallyLockResult {
            lock_info: Some(serde_json::to_string(&info).expect("encode info")),
            ..legacy_grant
        };
        let BatchAcquireOutcome::Granted {
            lock_info: Some(decoded),
            ..
        } = classify(Some(&grant))
        else {
            panic!("valid grant retains its lock information");
        };
        assert_eq!(decoded.id, request.lock_id);
        assert_eq!(decoded.resource, request.resource);
        assert_eq!(decoded.lock_type, request.lock_type);
        let no_grant = GenerallyLockResult {
            acquire_status: LockAcquireStatus::RetryableNoGrant as i32,
            ..Default::default()
        };
        assert!(matches!(classify(Some(&no_grant)), BatchAcquireOutcome::RetryableNoGrant { .. }));
        let rejected = GenerallyLockResult {
            acquire_status: LockAcquireStatus::RejectedNoGrant as i32,
            ..Default::default()
        };
        assert!(matches!(classify(Some(&rejected)), BatchAcquireOutcome::TerminalNoGrant { .. }));
        let mut wrong_identity = info;
        wrong_identity.id = test_lock_request(Duration::from_secs(30)).lock_id;
        let ambiguous = [
            GenerallyLockResult::default(),
            GenerallyLockResult {
                acquire_status: 99,
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                acquire_status: LockAcquireStatus::RetryableNoGrant as i32,
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                error_info: Some("failed".to_string()),
                ..Default::default()
            },
            GenerallyLockResult {
                acquire_status: LockAcquireStatus::RetryableNoGrant as i32,
                lock_info: Some("{}".to_string()),
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                lock_info: Some("bad json".to_string()),
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                lock_info: Some(serde_json::to_string(&wrong_identity).expect("encode wrong info")),
                ..Default::default()
            },
            GenerallyLockResult {
                success: true,
                release_status: LockReleaseStatus::Released as i32,
                ..Default::default()
            },
        ];
        for result in &ambiguous {
            assert!(
                matches!(
                    classify(Some(result)),
                    BatchAcquireOutcome::Ambiguous {
                        cleanup_owned_by_client: false,
                        ..
                    }
                ),
                "{result:?}"
            );
        }
        assert!(matches!(classify(None), BatchAcquireOutcome::Ambiguous { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_success_crossing_deadline_in_one_poll_runs_late_cleanup_once() {
        use crate::cluster::rpc::client::NoOpInterceptor;
        use tonic::service::Interceptor;
        let addr = "http://batch-single-poll-late-success";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let deadline = Instant::now() + Duration::from_millis(20);
        let dispatch = BatchLockRpcDeadline::new(deadline, Duration::ZERO);
        let mut request = Request::new(());
        request.extensions_mut().insert(dispatch.clone());
        TonicInterceptor::NoOp(NoOpInterceptor)
            .call(request)
            .expect("prepare request");
        dispatch.dispatch_payload_for_test().await.expect("payload dispatched");
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cleanup_count = count.clone();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let late: LateCompletion<bool> = Some(Box::new(move |granted| {
            Box::pin(async move {
                cleanup_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                cleanup_tx.send(granted).expect("cleanup observer");
            })
        }));
        let result = client
            .execute_rpc_until(
                "lock_batch",
                "bucket/object",
                deadline,
                async move {
                    // The paused clock guarantees the select's timer is not ready.
                    // Resume only inside this poll, then finish past its deadline.
                    tokio::time::resume();
                    std::thread::sleep(deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
                    Ok::<_, tonic::Status>(true)
                },
                RpcCleanup::new(late, None, None).with_batch_deadline(dispatch),
            )
            .await;
        assert!(
            matches!(result, Err(LockError::Timeout { .. })),
            "expired success must not escape to the caller"
        );
        assert!(
            timeout(Duration::from_secs(1), cleanup_rx)
                .await
                .expect("late cleanup runs")
                .expect("original grant response")
        );
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(lock_peer_health_for_test(addr).detached_rpcs, 0);
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn batch_readiness_deadline_cancels_without_late_submission_or_peer_timeout() {
        let addr = "http://batch-unsent-deadline";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let deadline = Instant::now() + Duration::from_millis(20);
        let dispatch = BatchLockRpcDeadline::new(deadline, Duration::ZERO);
        let (future_alive, future_dropped) = tokio::sync::oneshot::channel::<()>();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel::<()>();
        let late_failure: LateFailure = Some(Box::new(move |()| {
            Box::pin(async move {
                cleanup_tx.send(()).expect("cleanup observer");
            })
        }));
        let result = client
            .execute_rpc_until(
                "lock_batch",
                "bucket/object",
                deadline,
                async move {
                    let _alive = future_alive;
                    std::future::pending::<()>().await;
                    Ok::<_, tonic::Status>(())
                },
                RpcCleanup::new(None, late_failure, client.admit_lock_request_guard("lock_batch"))
                    .with_batch_deadline(dispatch.clone()),
            )
            .await;
        assert!(matches!(result, Err(LockError::Timeout { .. })));
        assert!(future_dropped.await.is_err(), "unsent future was cancelled");
        assert!(cleanup_rx.await.is_err(), "unsent request must not create uncertain cleanup");
        assert!(dispatch.cancel_before_dispatch());
        let health = lock_peer_health_for_test(addr);
        assert_eq!(health.detached_rpcs, 0);
        assert_eq!(health.request_in_flight, 0);
        assert_eq!(health.request_consecutive_timeouts, 0);
        assert_eq!(health.consecutive_timeouts, 0);
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn cancelled_batch_readiness_wait_does_not_detach() {
        let addr = "http://batch-cancelled-ready";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let deadline = Instant::now() + Duration::from_secs(30);
        let dispatch = BatchLockRpcDeadline::new(deadline, Duration::ZERO);
        let caller_dispatch = dispatch.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            client
                .execute_rpc_until(
                    "lock_batch",
                    "bucket/object",
                    deadline,
                    async move {
                        started_tx.send(()).expect("ready observer");
                        std::future::pending::<()>().await;
                        Ok::<_, tonic::Status>(())
                    },
                    RpcCleanup::new(None, None, client.admit_lock_request_guard("lock_batch"))
                        .with_batch_deadline(caller_dispatch),
                )
                .await
        });
        started_rx.await.expect("RPC enters readiness wait");
        caller.abort();
        assert!(caller.await.expect_err("cancelled caller").is_cancelled());
        assert!(dispatch.cancel_before_dispatch());
        assert!(!dispatch.was_dispatched());
        assert_eq!(lock_peer_health_for_test(addr).detached_rpcs, 0);
        assert_eq!(lock_peer_health_for_test(addr).request_in_flight, 0);
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn cancelled_batch_late_cleanup_retains_original_request_span() {
        let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry());
        let span = tracing::info_span!("batch_lock_request", request_id = "cancelled-batch-span");
        let expected_span = span.id();
        assert!(expected_span.is_some(), "test subscriber must retain the request span");
        let addr = "http://batch-cancelled-span";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let dispatch = BatchLockRpcDeadline::new(Instant::now() + Duration::from_secs(2), Duration::ZERO);
        dispatch.dispatch_payload_for_test().await.expect("payload dispatched");
        let (complete_tx, complete_rx) = tokio::sync::oneshot::channel::<bool>();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let late: LateCompletion<bool> = Some(Box::new(move |granted| {
            Box::pin(async move {
                cleanup_tx
                    .send((tracing::Span::current().id(), granted))
                    .expect("cleanup span observer");
            })
        }));
        let rpc = {
            let _entered = span.enter();
            InFlightRpc {
                client: &client,
                op: "lock_batch",
                resource_summary: "bucket/object",
                future: Some(
                    AssertUnwindSafe(async move {
                        complete_rx
                            .await
                            .map_err(|_| tonic::Status::cancelled("test completion dropped"))
                    })
                    .catch_unwind()
                    .boxed(),
                ),
                cleanup: Some(RpcCleanup::new(late, None, None).with_batch_deadline(dispatch)),
                span: tracing::Span::current(),
            }
        };
        assert!(tracing::Span::current().id().is_none(), "caller is outside its original request span");
        drop(rpc);
        complete_tx.send(true).expect("complete detached RPC");
        let (cleanup_span, granted) = timeout(Duration::from_secs(1), cleanup_rx)
            .await
            .expect("late cleanup runs")
            .expect("cleanup span result");
        assert_eq!(
            cleanup_span, expected_span,
            "detached cleanup must retain the original request correlation"
        );
        assert!(granted);
        assert_eq!(lock_peer_health_for_test(addr).detached_rpcs, 0);
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn submitted_batch_application_error_establishes_uncertain_cleanup() {
        use crate::cluster::rpc::client::NoOpInterceptor;
        use tonic::service::Interceptor;
        let addr = "http://batch-application-error";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let deadline = Instant::now() + Duration::from_secs(1);
        let dispatch = BatchLockRpcDeadline::new(deadline, Duration::ZERO);
        let mut request = Request::new(());
        request.extensions_mut().insert(dispatch.clone());
        TonicInterceptor::NoOp(NoOpInterceptor)
            .call(request)
            .expect("prepare request");
        dispatch.dispatch_payload_for_test().await.expect("payload dispatched");
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let late_failure: LateFailure = Some(Box::new(move |()| {
            Box::pin(async move {
                cleanup_tx.send(()).expect("cleanup observer");
            })
        }));
        assert!(
            client
                .execute_rpc_until(
                    "lock_batch",
                    "bucket/object",
                    deadline,
                    async { Err::<(), _>(tonic::Status::internal("backend returned no per-item result",)) },
                    RpcCleanup::new(None, late_failure, None).with_batch_deadline(dispatch),
                )
                .await
                .is_err()
        );
        timeout(Duration::from_secs(1), cleanup_rx)
            .await
            .expect("cleanup runs")
            .expect("cleanup signal");
        assert_eq!(
            lock_peer_health_for_test(addr).request_consecutive_timeouts,
            0,
            "application error is not a transport timeout"
        );
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn test_remote_client_cancelled_connection_releases_request_admission() {
        ensure_test_rpc_secret();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pending-handshake peer");
        let addr = format!("http://{}", listener.local_addr().expect("pending-handshake peer address"));
        let client = RemoteClient::new(addr.clone());
        let request = test_lock_request(Duration::from_secs(30));

        for batch in [false, true] {
            reset_lock_peer_health_for_test(&addr);
            with_lock_peer_health(&addr, |health| {
                health.request_backoff_until = Some(Instant::now() - Duration::from_secs(1));
            });
            let mut acquisition = Box::pin(async {
                if batch {
                    client.acquire_locks_batch(std::slice::from_ref(&request)).await.map(|_| ())
                } else {
                    client.acquire_lock(&request).await.map(|_| ())
                }
            });
            std::future::poll_fn(|cx| {
                assert!(acquisition.as_mut().poll(cx).is_pending(), "peer must wait for its HTTP/2 handshake");
                std::task::Poll::Ready(())
            })
            .await;
            let pending = lock_peer_health_for_test(&addr);
            assert_eq!(pending.request_in_flight, 1, "pending connection owns one request admission");
            assert!(pending.request_probe_generation.is_some(), "the request is the half-open probe");

            drop(acquisition);
            let cancelled = lock_peer_health_for_test(&addr);
            assert_eq!(cancelled.request_in_flight, 0, "cancellation must return request admission");
            assert!(cancelled.request_probe_generation.is_none(), "cancellation must unblock the next probe");
            let probe = client.admit_lock_request("lock").expect("cancelled peer can be probed again");
            client.release_lock_request("lock", Some(probe));
        }
        reset_lock_peer_health_for_test(&addr);
    }

    #[tokio::test]
    async fn test_remote_client_cancelled_rpc_runs_late_grant_cleanup() {
        let addr = "http://cancelled-rpc-late-grant";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (grant_tx, grant_rx) = tokio::sync::oneshot::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            let late: LateCompletion<bool> = Some(Box::new(move |granted| {
                Box::pin(async move {
                    released_tx.send(granted).expect("late grant observer remains alive");
                })
            }));
            client
                .execute_rpc(
                    "lock",
                    "bucket/object",
                    Duration::from_secs(30),
                    async move {
                        started_tx.send(()).expect("caller observes RPC dispatch");
                        Ok(grant_rx.await.expect("test decides when peer grants lock"))
                    },
                    RpcCleanup::new(late, None, None),
                )
                .await
        });
        timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("RPC must dispatch")
            .expect("dispatch signal");
        caller.abort();
        assert!(caller.await.expect_err("caller was cancelled").is_cancelled());
        assert_eq!(
            lock_peer_health_for_test(addr).detached_rpcs,
            1,
            "cancelled RPC uses bounded detached ownership"
        );
        grant_tx.send(true).expect("cancellation must preserve the running RPC");
        assert!(
            timeout(Duration::from_secs(1), released_rx)
                .await
                .expect("late grant must be cleaned up")
                .expect("cleanup result")
        );
        assert_eq!(lock_peer_health_for_test(addr).detached_rpcs, 0, "completed cleanup returns its slot");
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn test_remote_client_cancelled_rpc_runs_ambiguous_failure_cleanup() {
        let addr = "http://cancelled-rpc-late-failure";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (failure_tx, failure_rx) = tokio::sync::oneshot::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            let late_failure: LateFailure = Some(Box::new(move |()| {
                Box::pin(async move {
                    released_tx.send(()).expect("ambiguous cleanup observer remains alive");
                })
            }));
            client
                .execute_rpc(
                    "lock_batch",
                    "bucket/object",
                    Duration::from_secs(30),
                    async move {
                        started_tx.send(()).expect("caller observes RPC dispatch");
                        failure_rx.await.expect("test decides when transport fails");
                        Err::<(), _>(tonic::Status::cancelled("transport failure after caller cancellation"))
                    },
                    RpcCleanup::new(None, late_failure, None),
                )
                .await
        });
        timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("RPC must dispatch")
            .expect("dispatch signal");
        caller.abort();
        assert!(caller.await.expect_err("caller was cancelled").is_cancelled());
        failure_tx.send(()).expect("cancellation must preserve the running RPC");
        timeout(Duration::from_secs(1), released_rx)
            .await
            .expect("ambiguous grant must be cleaned up")
            .expect("cleanup signal");
        assert_eq!(lock_peer_health_for_test(addr).detached_rpcs, 0, "failed detached RPC returns its slot");
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn test_remote_client_cancelled_completed_failure_runs_cleanup() {
        for (label, status) in [
            ("transport", tonic::Status::cancelled("ambiguous transport failure")),
            ("deadline", tonic::Status::deadline_exceeded("ambiguous deadline failure")),
        ] {
            let addr = format!("http://cancelled-completed-{label}");
            reset_lock_peer_health_for_test(&addr);
            let client = RemoteClient::new(addr.clone());
            let (evicting_tx, evicting_rx) = tokio::sync::oneshot::channel();
            let (_resume_tx, resume_rx) = tokio::sync::oneshot::channel();
            eviction_pauses_for_test()
                .lock()
                .expect("eviction pause registry")
                .insert(addr.clone(), (evicting_tx, resume_rx));
            let (released_tx, released_rx) = tokio::sync::oneshot::channel();
            let caller = tokio::spawn(async move {
                let late_failure: LateFailure = Some(Box::new(move |()| {
                    Box::pin(async move {
                        released_tx
                            .send(())
                            .expect("completed-failure cleanup observer remains alive");
                    })
                }));
                client
                    .execute_rpc(
                        "lock",
                        "bucket/object",
                        Duration::from_secs(30),
                        async move { Err::<(), _>(status) },
                        RpcCleanup::new(None, late_failure, None),
                    )
                    .await
            });
            timeout(Duration::from_secs(1), evicting_rx)
                .await
                .expect("completed error must reach channel eviction")
                .expect("eviction signal");
            timeout(Duration::from_secs(1), released_rx)
                .await
                .expect("cleanup must transfer before channel eviction suspends")
                .expect("cleanup signal");
            caller.abort();
            assert!(caller.await.expect_err("caller is cancelled during eviction").is_cancelled());
            assert_eq!(
                lock_peer_health_for_test(&addr).detached_rpcs,
                0,
                "completed RPC cannot consume a detached slot"
            );
            reset_lock_peer_health_for_test(&addr);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_remote_client_deadline_wins_over_same_tick_transport_timeout() {
        let addr = "http://same-tick-rpc-deadline";
        reset_lock_peer_health_for_test(addr);
        with_lock_peer_health(addr, |health| health.last_success = Some(Instant::now()));
        let client = RemoteClient::new(addr.to_string());
        let budget = Duration::from_millis(50);
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let late_failure: LateFailure = Some(Box::new(move |()| {
            Box::pin(async move {
                released_tx.send(()).expect("same-tick cleanup observer remains alive");
            })
        }));
        let result = client
            .execute_rpc(
                "lock",
                "bucket/object",
                budget,
                async move {
                    tokio::time::sleep(budget).await;
                    Err::<(), _>(tonic::Status::cancelled("inner transport deadline expired"))
                },
                RpcCleanup::new(None, late_failure, None),
            )
            .await;
        assert!(
            matches!(result, Err(LockError::Timeout { .. })),
            "expired caller budget must retain timeout classification"
        );
        let health = lock_peer_health_for_test(addr);
        assert_eq!(health.consecutive_timeouts, 1, "same-tick completion must follow timeout eviction policy");
        assert!(
            health.last_eviction.is_none(),
            "a recently serving peer keeps its channel after a caller timeout"
        );
        timeout(Duration::from_secs(1), released_rx)
            .await
            .expect("same-tick ambiguous completion must trigger cleanup")
            .expect("cleanup signal");
        assert_eq!(
            lock_peer_health_for_test(addr).detached_rpcs,
            0,
            "same-tick completion returns detached admission"
        );
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn test_remote_client_timeout_drains_late_grant_during_eviction() {
        let addr = "http://timeout-late-grant-during-eviction";
        reset_lock_peer_health_for_test(addr);
        let client = RemoteClient::new(addr.to_string());
        let (evicting_tx, evicting_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        eviction_pauses_for_test()
            .lock()
            .expect("eviction pause registry")
            .insert(addr.to_string(), (evicting_tx, resume_rx));
        let (grant_tx, grant_rx) = tokio::sync::oneshot::channel();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            let late: LateCompletion<bool> = Some(Box::new(move |granted| {
                Box::pin(async move {
                    released_tx.send(granted).expect("late grant observer remains alive");
                })
            }));
            client
                .execute_rpc(
                    "lock",
                    "bucket/object",
                    Duration::from_millis(25),
                    async move { Ok(grant_rx.await.expect("test controls the late grant")) },
                    RpcCleanup::new(late, None, None),
                )
                .await
        });
        timeout(Duration::from_secs(1), evicting_rx)
            .await
            .expect("timed-out RPC must reach channel eviction")
            .expect("eviction signal");
        assert_eq!(
            lock_peer_health_for_test(addr).detached_rpcs,
            1,
            "eviction waits with a detached RPC owner"
        );
        grant_tx
            .send(true)
            .expect("timed-out stream remains available for a late grant");
        assert!(
            timeout(Duration::from_secs(1), released_rx)
                .await
                .expect("late grant must be cleaned up before eviction resumes")
                .expect("late cleanup result")
        );
        assert!(!caller.is_finished(), "channel eviction is still paused");
        assert_eq!(
            lock_peer_health_for_test(addr).detached_rpcs,
            0,
            "late completion returns detached admission"
        );
        resume_tx.send(()).expect("resume channel eviction");
        let result = caller.await.expect("timed-out caller completes without panicking");
        assert!(
            matches!(result, Err(LockError::Timeout { .. })),
            "caller preserves its RPC timeout result"
        );
        reset_lock_peer_health_for_test(addr);
    }

    #[tokio::test]
    async fn test_remote_client_success_polls_rpc_on_callers_task() {
        tokio::spawn(async {
            let client = RemoteClient::new("http://direct-poll-success".to_string());
            let caller_id = tokio::task::id();
            let rpc_id = client
                .execute_rpc(
                    "ping",
                    "health-lock-online",
                    Duration::from_secs(1),
                    async { Ok(tokio::task::id()) },
                    RpcCleanup::new(None, None, None),
                )
                .await
                .expect("successful RPC must return its result");
            assert_eq!(rpc_id, caller_id, "successful RPC must not allocate and schedule a separate task");
        })
        .await
        .expect("caller task must complete");
    }

    #[tokio::test]
    async fn test_remote_client_rpc_panic_isolated_and_cleans_up() {
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            let client = RemoteClient::new("http://direct-poll-panic".to_string());
            let late_failure: LateFailure = Some(Box::new(move |()| {
                Box::pin(async move {
                    released_tx.send(()).expect("panic cleanup observer remains alive");
                })
            }));
            client
                .execute_rpc::<(), _>(
                    "lock",
                    "bucket/object",
                    Duration::from_secs(1),
                    async { panic!("test peer RPC panic") },
                    RpcCleanup::new(None, late_failure, None),
                )
                .await
        });
        let result = caller.await.expect("RPC panic must not escape to its caller");
        assert!(
            matches!(result, Err(LockError::Internal { .. })),
            "RPC panic remains an internal lock failure"
        );
        timeout(Duration::from_secs(1), released_rx)
            .await
            .expect("panic must trigger ambiguous-grant cleanup")
            .expect("cleanup signal");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_timeout_keeps_channel_of_recently_serving_peer() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        with_lock_peer_health(&addr, |health| health.last_success = Some(Instant::now()));

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let response = client
                .acquire_lock(&test_lock_request(Duration::from_millis(5)))
                .await
                .unwrap();
            assert!(!response.success, "timed out lock acquisition should fail");
            assert!(
                runtime_sources::test_node_channel_is_cached(&addr).await,
                "a peer that served a lock RPC within the liveness window is slow, not gone"
            );
            assert_eq!(lock_peer_health_for_test(&addr).consecutive_timeouts, 1);
        })
        .await;

        accept_task.abort();
        reset_lock_peer_health_for_test(&addr);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_repeated_timeouts_evict_at_most_once_per_cooldown() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50")),
                (rustfs_config::ENV_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS, Some("60000")),
            ],
            async {
                let client = RemoteClient::new(addr.clone());
                let request = test_lock_request(Duration::from_millis(5));

                let _ = client.acquire_lock(&request).await.unwrap();
                assert!(
                    !runtime_sources::test_node_channel_is_cached(&addr).await,
                    "the first timeout on a quiet peer evicts the cached channel"
                );

                cache_lazy_channel(&addr).await;
                let _ = client.acquire_lock(&request).await.unwrap();
                assert!(
                    runtime_sources::test_node_channel_is_cached(&addr).await,
                    "a second timeout inside the cooldown must not tear the fresh channel down again"
                );
                assert_eq!(lock_peer_health_for_test(&addr).consecutive_timeouts, 2);
            },
        )
        .await;

        accept_task.abort();
        reset_lock_peer_health_for_test(&addr);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_detaches_timed_out_rpc_and_reclaims_its_slot() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let _ = client
                .acquire_lock(&test_lock_request(Duration::from_millis(5)))
                .await
                .unwrap();
            assert_eq!(
                lock_peer_health_for_test(&addr).detached_rpcs,
                1,
                "the timed-out stream keeps running instead of being reset"
            );

            // The hanging listener drops its socket after two seconds; the detached
            // task then observes the transport failure and frees its slot.
            let deadline = Instant::now() + Duration::from_secs(10);
            while lock_peer_health_for_test(&addr).detached_rpcs != 0 {
                assert!(Instant::now() < deadline, "detached RPC slot must be reclaimed once the stream ends");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        accept_task.abort();
        reset_lock_peer_health_for_test(&addr);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_cancels_timed_out_rpc_when_detached_budget_is_exhausted() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50")),
                (rustfs_config::ENV_OBJECT_LOCK_RPC_DETACHED_LIMIT, Some("0")),
            ],
            async {
                let client = RemoteClient::new(addr.clone());
                let response = client
                    .acquire_lock(&test_lock_request(Duration::from_millis(5)))
                    .await
                    .unwrap();
                assert!(!response.success);
                assert_eq!(
                    lock_peer_health_for_test(&addr).detached_rpcs,
                    0,
                    "an exhausted detached budget falls back to cancelling the stream"
                );
            },
        )
        .await;

        accept_task.abort();
        reset_lock_peer_health_for_test(&addr);
    }

    #[test]
    fn lock_mutation_helper_marks_single_and_batch_requests_for_rolling_auth() {
        let mut single = Request::new(GenerallyLockRequest {
            args: "single-lock".to_string(),
        });
        attach_lock_mutation_body_digest(&mut single).expect("single lock digest must be attached");
        assert!(
            single
                .extensions()
                .get::<crate::cluster::rpc::http_auth::RollingMutationBodyDigest>()
                .is_some()
        );

        let mut batch = Request::new(BatchGenerallyLockRequest {
            args: vec!["batch-lock".to_string()],
        });
        attach_lock_mutation_body_digest(&mut batch).expect("batch lock digest must be attached");
        assert!(
            batch
                .extensions()
                .get::<crate::cluster::rpc::http_auth::RollingMutationBodyDigest>()
                .is_some()
        );
    }

    #[test]
    fn cached_ping_request_matches_fresh_flatbuffer_payload() {
        let cached = RemoteClient::build_ping_request();
        let fresh = RemoteClient::build_fresh_ping_request_for_test();

        assert_eq!(cached.version, fresh.version);
        assert_eq!(cached.body, fresh.body);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_acquire_lock_uses_rpc_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let request = test_lock_request(Duration::from_millis(5));
            let started_at = tokio::time::Instant::now();

            let response = client.acquire_lock(&request).await.unwrap();
            let elapsed = started_at.elapsed();

            assert!(
                elapsed >= Duration::from_millis(40),
                "remote lock RPC should use configured transport timeout, got {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(1),
                "test RPC timeout should keep the test fast, got {elapsed:?}"
            );
            assert!(!response.success, "timed out lock acquisition should fail");
            assert!(
                response
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("Remote lock RPC timed out")),
                "expected remote RPC timeout marker, got {:?}",
                response.error
            );
            assert!(
                !runtime_sources::test_node_channel_is_cached(&addr).await,
                "transport timeout should evict cached connection"
            );
        })
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_acquire_locks_batch_submitted_timeout_preserves_cleanup_ownership() {
        use http_body_util::BodyExt as _;
        use hyper::{body::Frame, service::service_fn};
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use std::sync::Arc;
        use tonic_prost::prost::Message as _;

        ensure_test_rpc_secret();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind batch timeout peer");
        let addr = format!("http://{}", listener.local_addr().expect("batch timeout peer address"));
        let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
        let (cleanup_tx, mut cleanup_rx) = tokio::sync::mpsc::unbounded_channel();
        let release_response = Arc::new(tokio::sync::Notify::new());
        let server_release = Arc::clone(&release_response);
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.expect("accept batch timeout peer connection");
                let received_tx = received_tx.clone();
                let cleanup_tx = cleanup_tx.clone();
                let release = Arc::clone(&server_release);
                connections.spawn(async move {
                    let service = service_fn(move |request: http::Request<hyper::body::Incoming>| {
                        let received_tx = received_tx.clone();
                        let cleanup_tx = cleanup_tx.clone();
                        let release = Arc::clone(&release);
                        async move {
                            let is_acquire = request.uri().path().ends_with("/LockBatch");
                            assert!(is_acquire || request.uri().path().ends_with("/UnLockBatch"));
                            let body = request
                                .into_body()
                                .collect()
                                .await
                                .expect("complete batch payload")
                                .to_bytes();
                            assert!(body.len() >= 5, "uncompressed unary protobuf frame");
                            assert_eq!(body[0], 0);
                            let args = BatchGenerallyLockRequest::decode(&body[5..]).expect("decode batch request");
                            let ids = args
                                .args
                                .iter()
                                .map(|json| serde_json::from_str::<LockRequest>(json).expect("lock request JSON").lock_id)
                                .collect::<Vec<_>>();
                            let count = ids.len();
                            if is_acquire {
                                // Unlike an idle TCP listener, consuming the complete
                                // gRPC payload proves dispatch before the response stalls.
                                received_tx.send(ids).expect("payload observer");
                                release.notified().await;
                            } else {
                                cleanup_tx.send(ids).expect("exact-ID cleanup observer");
                            }
                            let response = BatchGenerallyLockResponse {
                                results: (0..count)
                                    .map(|_| GenerallyLockResult {
                                        success: true,
                                        release_status: if is_acquire {
                                            LockReleaseStatus::Unspecified.into()
                                        } else {
                                            LockReleaseStatus::Released.into()
                                        },
                                        ..Default::default()
                                    })
                                    .collect(),
                            }
                            .encode_to_vec();
                            let mut framed = vec![0];
                            framed.extend_from_slice(&u32::try_from(response.len()).expect("small response").to_be_bytes());
                            framed.extend_from_slice(&response);
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().expect("gRPC status"));
                            let frames = [
                                Ok::<_, tonic::Status>(Frame::data(Bytes::from(framed))),
                                Ok(Frame::trailers(trailers)),
                            ];
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .header("content-type", "application/grpc")
                                    .body(tonic::body::Body::new(http_body_util::StreamBody::new(futures::stream::iter(frames))))
                                    .expect("gRPC response"),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        reset_lock_peer_health_for_test(&addr);
        let channel = TonicEndpoint::from_shared(addr.clone())
            .expect("batch timeout endpoint")
            .connect()
            .await
            .expect("connect batch timeout peer");
        runtime_sources::cache_test_node_channel(addr.clone(), channel).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("2000"))], async {
            let client = RemoteClient::new(addr.clone());
            let request = test_lock_request(Duration::from_millis(5));
            let expected_id = request.lock_id.clone();
            let started_at = tokio::time::Instant::now();
            let pending = tokio::spawn(async move {
                client
                    .acquire_locks_batch_until(&[request], Instant::now() + Duration::from_secs(5))
                    .await
            });
            let received = timeout(Duration::from_secs(2), received_rx.recv())
                .await
                .expect("peer consumes the batch payload")
                .expect("payload event");
            assert_eq!(received, vec![expected_id.clone()]);
            assert!(!pending.is_finished(), "payload receipt must precede the response deadline");
            let outcomes = timeout(Duration::from_secs(5), pending)
                .await
                .expect("configured RPC timeout bounds the caller")
                .expect("batch acquisition task");
            let elapsed = started_at.elapsed();
            assert!(
                elapsed >= Duration::from_millis(1800),
                "remote batch lock RPC should use configured transport timeout, got {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(5),
                "test RPC timeout should keep the test fast, got {elapsed:?}"
            );
            assert!(
                matches!(
                    outcomes.as_slice(),
                    [BatchAcquireOutcome::Ambiguous {
                        error: LockError::Timeout { .. },
                        cleanup_owned_by_client: true,
                    }]
                ),
                "a submitted batch with no timely response must retain cleanup ownership: {outcomes:?}"
            );
            release_response.notify_one();
            let cleaned = timeout(Duration::from_secs(2), cleanup_rx.recv())
                .await
                .expect("late cleanup reaches the peer")
                .expect("cleanup event");
            assert_eq!(cleaned, vec![expected_id], "cleanup must retain the submitted exact lock ID");
            timeout(Duration::from_secs(2), async {
                loop {
                    let health = lock_peer_health_for_test(&addr);
                    if health.detached_rpcs == 0 && health.request_in_flight == 0 && health.late_release_in_flight == 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("late RPC and cleanup admission are released");
            assert!(
                matches!(cleanup_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
                "one successful exact-ID ACK must settle cleanup without another attempt"
            );
        })
        .await;

        server.abort();
        let _ = server.await;
        rustfs_protos::evict_failed_connection(&addr).await;
        reset_lock_peer_health_for_test(&addr);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_release_uses_rpc_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let request = test_lock_request(Duration::from_millis(5));
            let started_at = tokio::time::Instant::now();

            let err = client.release(&request.lock_id).await.expect_err("release should time out");
            let elapsed = started_at.elapsed();

            assert!(
                elapsed >= Duration::from_millis(40),
                "remote release RPC should use configured transport timeout, got {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(1),
                "test RPC timeout should keep the test fast, got {elapsed:?}"
            );
            assert!(matches!(err, LockError::Timeout { .. }), "expected remote release timeout, got {err:?}");
            assert!(
                !runtime_sources::test_node_channel_is_cached(&addr).await,
                "release timeout should evict cached connection"
            );
        })
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_is_online_uses_health_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_HEALTH_LOCK_ONLINE_TIMEOUT_MS, Some("50")),
                (rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("1000")),
            ],
            async {
                let client = RemoteClient::new(addr.clone());
                let started_at = tokio::time::Instant::now();

                let online = client.is_online().await;
                let elapsed = started_at.elapsed();

                assert!(!online, "hanging remote lock peer must not be reported online");
                assert!(
                    elapsed >= Duration::from_millis(40),
                    "remote online check should honor configured health timeout, got {elapsed:?}"
                );
                assert!(
                    elapsed < Duration::from_secs(1),
                    "health timeout should keep readiness probes bounded, got {elapsed:?}"
                );
                assert!(
                    !runtime_sources::test_node_channel_is_cached(&addr).await,
                    "online-check timeout should evict cached connection"
                );
            },
        )
        .await;

        accept_task.abort();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_refresh_tonic_error_evicts_connection() {
        ensure_test_rpc_secret();
        let Some(addr) = closed_listener_addr().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("500"))], async {
            let client = RemoteClient::new(addr.clone());
            let request = test_lock_request(Duration::from_millis(5));

            let err = client
                .refresh(&request.lock_id)
                .await
                .expect_err("refresh should report tonic failure");

            assert!(
                err.to_string().contains("refresh RPC failed"),
                "expected refresh RPC failure marker, got {err}"
            );
            assert!(
                !runtime_sources::test_node_channel_is_cached(&addr).await,
                "refresh tonic error should evict cached connection"
            );
        })
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_remote_client_check_status_timeout_evicts_connection_and_preserves_status_shape() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let request = test_lock_request(Duration::from_millis(5));
            let started_at = tokio::time::Instant::now();

            let status = client.check_status(&request.lock_id).await.unwrap();
            let elapsed = started_at.elapsed();

            assert!(
                elapsed >= Duration::from_millis(40),
                "remote check_status RPC should use configured transport timeout, got {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(1),
                "test RPC timeout should keep the test fast, got {elapsed:?}"
            );
            let info = status.expect("communication failure should preserve unknown lock status shape");
            assert_eq!(info.id, request.lock_id);
            assert_eq!(info.owner, "unknown");
            assert!(
                !runtime_sources::test_node_channel_is_cached(&addr).await,
                "check_status timeout should evict cached connection"
            );
        })
        .await;

        accept_task.abort();
    }

    #[test]
    fn test_is_transport_failure_classifies_only_channel_level_errors() {
        use tonic::{Code, Status};

        // Genuine transport failures: broken/unusable channel.
        for code in [Code::Unavailable, Code::DeadlineExceeded, Code::Unknown, Code::Cancelled] {
            assert!(
                RemoteClient::is_transport_failure(&Status::new(code, "boom")),
                "{code:?} should be treated as a transport failure"
            );
        }

        // A status carrying an underlying transport error as its source is a transport failure
        // regardless of code (tonic reports connection/h2 breakage this way).
        let sourced = Status::from_error(Box::new(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset")));
        assert!(
            RemoteClient::is_transport_failure(&sourced),
            "a status with an underlying transport source should be a transport failure"
        );

        // Server-produced application statuses arrive on a healthy channel and must NOT evict it.
        for code in [
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::Internal,
            Code::FailedPrecondition,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::ResourceExhausted,
            Code::Unimplemented,
            Code::Aborted,
            Code::OutOfRange,
        ] {
            assert!(
                !RemoteClient::is_transport_failure(&Status::new(code, "denied")),
                "{code:?} is an application status and must not be treated as a transport failure"
            );
        }
    }

    #[tokio::test]
    async fn stale_boot_epoch_rejections_do_not_open_lock_request_breaker() {
        let addr = "http://stale-boot-epoch-lock-breaker:9000";
        let client = RemoteClient::new(addr.to_string());
        reset_lock_peer_health_for_test(addr);

        for _ in 0..LOCK_RPC_REQUEST_BACKOFF_THRESHOLD {
            let error = client
                .execute_rpc::<(), _>(
                    "lock",
                    "bucket/new-object",
                    Duration::from_secs(1),
                    async { Err(tonic::Status::unavailable("RPC boot epoch changed")) },
                    RpcCleanup::new(None, None, None),
                )
                .await
                .expect_err("a stale epoch still rejects the operation before execution");
            assert!(error.to_string().contains("RPC boot epoch changed"));
        }

        let health = lock_peer_health_for_test(addr);
        assert_eq!(health.request_consecutive_timeouts, 0);
        assert!(health.request_backoff_until.is_none());
        assert!(health.last_eviction.is_none());
        let admission = client
            .admit_lock_request("lock")
            .expect("authenticated epoch refresh may proceed immediately");
        client.release_lock_request("lock", Some(admission));
        reset_lock_peer_health_for_test(addr);
    }

    #[test]
    #[serial_test::serial]
    fn test_remote_client_rpc_timeout_honors_configured_deadline() {
        temp_env::with_var(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, None::<&str>, || {
            assert_eq!(
                RemoteClient::rpc_timeout(),
                Duration::from_millis(rustfs_config::DEFAULT_OBJECT_LOCK_RPC_TIMEOUT_MS)
            );
        });
        temp_env::with_var(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"), || {
            assert_eq!(RemoteClient::rpc_timeout(), Duration::from_millis(50));
        });
        temp_env::with_var(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("0"), || {
            assert_eq!(RemoteClient::rpc_timeout(), Duration::from_millis(1));
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_remote_client_online_timeout_honors_configured_deadline() {
        temp_env::with_var(rustfs_config::ENV_HEALTH_LOCK_ONLINE_TIMEOUT_MS, None::<&str>, || {
            assert_eq!(
                RemoteClient::online_check_timeout(),
                Duration::from_millis(rustfs_config::DEFAULT_HEALTH_LOCK_ONLINE_TIMEOUT_MS)
            );
        });
        temp_env::with_var(rustfs_config::ENV_HEALTH_LOCK_ONLINE_TIMEOUT_MS, Some("50"), || {
            assert_eq!(RemoteClient::online_check_timeout(), Duration::from_millis(50));
        });
        temp_env::with_var(rustfs_config::ENV_HEALTH_LOCK_ONLINE_TIMEOUT_MS, Some("0"), || {
            assert_eq!(RemoteClient::online_check_timeout(), Duration::from_millis(1));
        });
    }
}
