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
    AuthenticatedChannel, TonicInterceptor, gen_tonic_signature_interceptor, node_service_time_out_client,
};
use crate::cluster::rpc::set_tonic_rolling_mutation_body_digest;
use async_trait::async_trait;
use bytes::Bytes;
use rustfs_lock::{
    LockClient, LockError, LockInfo, LockRequest, LockResponse, LockStats, LockStatus, LockType, Result,
    types::{LockId, LockMetadata, LockPriority},
};
use rustfs_protos::proto_gen::node_service::{
    BatchGenerallyLockRequest, BatchGenerallyLockResponse, GenerallyLockRequest, GenerallyLockResponse, GenerallyLockResult,
    PingRequest,
};
use rustfs_protos::{
    ConnectionEvictionLogLevel, evict_failed_connection_with_log_level, models::PingBodyBuilder,
    proto_gen::node_service::node_service_client::NodeServiceClient,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response};
use tracing::{debug, info, warn};

fn attach_lock_mutation_body_digest<T: rustfs_protos::CanonicalMutationBody>(request: &mut Request<T>) -> std::io::Result<()> {
    set_tonic_rolling_mutation_body_digest(request)
}

/// Work to run if an RPC that already timed out for its caller completes later.
type LateCompletion<T> = Option<Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>;

/// The liveness window is this many RPC deadlines: a peer that completed a
/// lock RPC within it is slow, not gone, and keeps its channel on a timeout.
const LOCK_RPC_LIVENESS_WINDOW_DEADLINES: u32 = 2;

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
    /// Timed-out RPCs still running in the background for this peer.
    detached_rpcs: usize,
}

fn lock_peer_channel_health() -> &'static Mutex<HashMap<String, LockPeerChannelHealth>> {
    static HEALTH: OnceLock<Mutex<HashMap<String, LockPeerChannelHealth>>> = OnceLock::new();
    HEALTH.get_or_init(Mutex::default)
}

fn with_lock_peer_health<R>(addr: &str, update: impl FnOnce(&mut LockPeerChannelHealth) -> R) -> R {
    let mut peers = lock_peer_channel_health()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// Lock ids whose batch entry the server reports as granted.
fn acquired_lock_ids(lock_ids: &[LockId], results: &[GenerallyLockResult]) -> Vec<LockId> {
    results
        .iter()
        .zip(lock_ids)
        .filter(|(result, _)| result.success)
        .map(|(_, lock_id)| lock_id.clone())
        .collect()
}

/// Remote lock client implementation
#[derive(Debug, Clone)]
pub struct RemoteClient {
    addr: String,
}

impl RemoteClient {
    const ONLINE_CHECK_RESOURCE: &'static str = "health-lock-online";

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
        std::error::Error::source(status).is_some()
            || matches!(
                status.code(),
                Code::Unavailable | Code::DeadlineExceeded | Code::Unknown | Code::Cancelled
            )
    }

    async fn evict_connection(&self, op: &'static str, reason: &str, resource_summary: &str) {
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
        evict_failed_connection_with_log_level(&self.addr, log_level).await;
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

    fn liveness_window(deadline: Duration) -> Duration {
        deadline.saturating_mul(LOCK_RPC_LIVENESS_WINDOW_DEADLINES)
    }

    fn record_rpc_success(&self) {
        with_lock_peer_health(&self.addr, |health| {
            health.last_success = Some(Instant::now());
            health.consecutive_timeouts = 0;
        });
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
            self.evict_connection(op, reason, resource_summary).await;
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

    /// Keep a timed-out RPC running instead of cancelling its stream.
    ///
    /// Dropping the future sends `RST_STREAM`; under load those resets pile up
    /// in the server's pending-accept queue until it answers `GOAWAY
    /// too_many_resets` and kills every stream on the connection. A detached
    /// stream ends on its own within the internode RPC timeout, the number per
    /// peer is bounded, and a lock granted after its caller gave up is released.
    fn detach_timed_out_rpc<T: Send + 'static>(
        &self,
        op: &'static str,
        resource_summary: &str,
        handle: JoinHandle<std::result::Result<T, tonic::Status>>,
        late: LateCompletion<T>,
    ) {
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
            handle.abort();
            rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_detached(op, "aborted");
            debug!(
                addr = %self.addr,
                op,
                resource_summary,
                limit,
                "Cancelled timed-out remote lock RPC because the detached stream budget is exhausted"
            );
            return;
        }
        rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_detached(op, "detached");
        let addr = self.addr.clone();
        tokio::spawn(async move {
            let outcome = handle.await;
            with_lock_peer_health(&addr, |health| health.detached_rpcs = health.detached_rpcs.saturating_sub(1));
            match outcome {
                Ok(Ok(response)) => {
                    with_lock_peer_health(&addr, |health| {
                        health.last_success = Some(Instant::now());
                        health.consecutive_timeouts = 0;
                    });
                    rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "success");
                    if let Some(late) = late {
                        late(response).await;
                    }
                }
                Ok(Err(status)) => {
                    rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "error");
                    debug!(
                        addr = %addr,
                        op,
                        tonic_code = ?status.code(),
                        tonic_message = status.message(),
                        "Detached remote lock RPC failed after its caller timed out"
                    );
                }
                Err(join_error) => {
                    rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_late_completion(op, "join_error");
                    debug!(addr = %addr, op, error = %join_error, "Detached remote lock RPC task ended abnormally");
                }
            }
        });
    }

    fn late_release_hook(&self, lock_id: LockId) -> LateCompletion<Response<GenerallyLockResponse>> {
        let client = self.clone();
        Some(Box::new(move |response: Response<GenerallyLockResponse>| {
            Box::pin(async move {
                if response.get_ref().success {
                    client.release_late_acquisitions(vec![lock_id]).await;
                }
            })
        }))
    }

    fn late_release_batch_hook(&self, lock_ids: Vec<LockId>) -> LateCompletion<Response<BatchGenerallyLockResponse>> {
        let client = self.clone();
        Some(Box::new(move |response: Response<BatchGenerallyLockResponse>| {
            Box::pin(async move {
                let acquired = acquired_lock_ids(&lock_ids, &response.get_ref().results);
                if !acquired.is_empty() {
                    client.release_late_acquisitions(acquired).await;
                }
            })
        }))
    }

    /// A lock granted after its caller stopped waiting is an orphan until its
    /// lease expires; hand it back right away, best effort.
    async fn release_late_acquisitions(&self, lock_ids: Vec<LockId>) {
        let outcome = match self.release_locks_batch(&lock_ids).await {
            Ok(released) if released.iter().all(|released| *released) => "released",
            Ok(_) => "partial",
            Err(_) => "failed",
        };
        rustfs_io_metrics::lock_metrics::record_remote_lock_late_release(outcome);
        if outcome == "released" {
            debug!(addr = %self.addr, count = lock_ids.len(), "Released remote locks granted after their caller timed out");
        } else {
            warn!(
                addr = %self.addr,
                count = lock_ids.len(),
                outcome,
                "Could not release every remote lock granted after its caller timed out; the server lease will expire it"
            );
        }
    }

    async fn execute_rpc<T, Fut>(
        &self,
        op: &'static str,
        resource_summary: &str,
        deadline: Duration,
        future: Fut,
        late: LateCompletion<T>,
    ) -> std::result::Result<T, LockError>
    where
        Fut: Future<Output = std::result::Result<T, tonic::Status>> + Send + 'static,
        T: Send + 'static,
    {
        let mut handle = tokio::spawn(future);
        match timeout(deadline, &mut handle).await {
            Ok(Ok(Ok(response))) => {
                self.record_rpc_success();
                Ok(response)
            }
            Ok(Ok(Err(err))) => {
                let reason = err.to_string();
                // Only evict (and re-dial) the cached channel when the failure is a genuine
                // transport problem. A server-produced application status (auth denied, peer
                // lock service not ready, invalid args, ...) arrives on a perfectly healthy
                // channel; evicting it just churns the connection and pushes the peer toward
                // the offline threshold for no benefit. See issue #4567.
                let transport_failure = Self::is_transport_failure(&err);
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
                if transport_failure {
                    self.maybe_evict_connection(op, &reason, resource_summary, EvictionTrigger::Transport, deadline)
                        .await;
                }
                Err(LockError::internal(format!("{op} RPC failed: {reason}")))
            }
            Ok(Err(join_error)) => {
                warn!(
                    addr = %self.addr,
                    op,
                    resource_summary,
                    error = %join_error,
                    "Remote lock RPC task ended abnormally"
                );
                Err(LockError::internal(format!("{op} RPC task failed: {join_error}")))
            }
            Err(_) => {
                let reason = format!("RPC timed out after {deadline:?}");
                rustfs_io_metrics::lock_metrics::record_remote_lock_rpc_timeout(&self.addr, op);
                if Self::is_scanner_leader_lock(resource_summary) {
                    debug!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        "Remote lock RPC timed out for scanner leader lock"
                    );
                } else {
                    warn!(
                        addr = %self.addr,
                        op,
                        timeout_ms = deadline.as_millis(),
                        resource_summary,
                        "Remote lock RPC timed out"
                    );
                }
                self.maybe_evict_connection(op, &reason, resource_summary, EvictionTrigger::Timeout, deadline)
                    .await;
                self.detach_timed_out_rpc(op, resource_summary, handle, late);
                Err(LockError::timeout(format!("remote lock RPC {op} on {}", self.addr), deadline))
            }
        }
    }

    fn rpc_timeout_failure_response(request: &LockRequest, err: &LockError) -> LockResponse {
        LockResponse::failure(format!("Remote lock RPC timed out: {err}"), request.acquire_timeout)
    }

    fn rpc_failure_response(_request: &LockRequest, err: &LockError) -> LockResponse {
        LockResponse::failure(format!("Remote lock RPC failed: {err}"), Duration::ZERO)
    }

    fn rpc_failure_batch(requests: &[LockRequest], err: &LockError) -> Vec<LockResponse> {
        requests
            .iter()
            .map(|request| Self::rpc_failure_response(request, err))
            .collect()
    }

    fn rpc_timeout_failure_batch(requests: &[LockRequest], err: &LockError) -> Vec<LockResponse> {
        requests
            .iter()
            .map(|request| Self::rpc_timeout_failure_response(request, err))
            .collect()
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
        info!("remote acquire_exclusive for {}", request.resource);
        let mut client = self.get_client().await?;
        let resource_summary = request.resource.to_string();
        let mut req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        attach_lock_mutation_body_digest(&mut req)?;
        let late = self.late_release_hook(request.lock_id.clone());

        let resp = match self
            .execute_rpc(
                "lock",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.lock(req).await },
                late,
            )
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(err @ LockError::Timeout { .. }) => return Ok(Self::rpc_timeout_failure_response(request, &err)),
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
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.get_client().await?;
        let resource_summary = Self::summarize_resources(requests);
        let mut req = Request::new(BatchGenerallyLockRequest {
            args: requests
                .iter()
                .map(|request| {
                    serde_json::to_string(request).map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))
                })
                .collect::<Result<Vec<_>>>()?,
        });
        attach_lock_mutation_body_digest(&mut req)?;
        let late = self.late_release_batch_hook(requests.iter().map(|request| request.lock_id.clone()).collect());

        let resp = match self
            .execute_rpc(
                "lock_batch",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.lock_batch(req).await },
                late,
            )
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(err @ LockError::Timeout { .. }) => return Ok(Self::rpc_timeout_failure_batch(requests, &err)),
            Err(err) => return Ok(Self::rpc_failure_batch(requests, &err)),
        };

        Ok(requests
            .iter()
            .enumerate()
            .map(|(idx, request)| match resp.results.get(idx) {
                Some(result) if result.success => {
                    LockResponse::success(Self::build_lock_info(request, result.lock_info.clone()), std::time::Duration::ZERO)
                }
                Some(result) => LockResponse::failure(
                    result
                        .error_info
                        .clone()
                        .unwrap_or_else(|| "Lock acquisition failed on remote server".to_string()),
                    std::time::Duration::ZERO,
                ),
                None => LockResponse::failure(
                    format!("Lock batch response missing entry for request index {idx}"),
                    std::time::Duration::ZERO,
                ),
            })
            .collect())
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release for {}", lock_id);

        let unlock_request = Self::create_unlock_request(lock_id);
        let request_string = serde_json::to_string(&unlock_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;
        let mut client = self.get_client().await?;
        let resource_summary = unlock_request.resource.to_string();
        let mut req = Request::new(GenerallyLockRequest { args: request_string });
        attach_lock_mutation_body_digest(&mut req)?;
        let resp = self
            .execute_rpc(
                "release",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.un_lock(req).await },
                None,
            )
            .await?
            .into_inner();
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    async fn release_locks_batch(&self, lock_ids: &[LockId]) -> Result<Vec<bool>> {
        if lock_ids.is_empty() {
            return Ok(Vec::new());
        }

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

        let resp = self
            .execute_rpc(
                "release_batch",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.un_lock_batch(req).await },
                None,
            )
            .await?
            .into_inner();

        Ok(lock_ids
            .iter()
            .enumerate()
            .map(|(idx, _)| resp.results.get(idx).map(|result| result.success).unwrap_or(false))
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
                None,
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
                None,
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
        let status_request = Self::create_unlock_request(lock_id);
        let resource_summary = status_request.resource.to_string();
        let mut client = self.get_client().await?;
        let args = serde_json::to_string(&status_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;

        // Try to acquire a very short-lived lock to test availability
        let mut req = Request::new(GenerallyLockRequest { args: args.clone() });
        attach_lock_mutation_body_digest(&mut req)?;
        // A probe lock granted after the deadline must not linger on the peer.
        let late = self.late_release_hook(lock_id.clone());

        // Try exclusive lock first with very short timeout
        let resp = match self
            .execute_rpc(
                "check_status",
                &resource_summary,
                Self::rpc_timeout(),
                async move { client.lock(req).await },
                late,
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
            attach_lock_mutation_body_digest(&mut release_req)?;
            if let Ok(mut client) = self.get_client().await {
                let _ = self
                    .execute_rpc(
                        "check_status_release",
                        &resource_summary,
                        Self::rpc_timeout(),
                        async move { client.un_lock(release_req).await },
                        None,
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
                None,
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
    fn acquired_lock_ids_picks_only_granted_batch_entries() {
        let lock_ids = vec![
            LockId::new_unique(&ObjectKey::new("bucket", "a")),
            LockId::new_unique(&ObjectKey::new("bucket", "b")),
            LockId::new_unique(&ObjectKey::new("bucket", "c")),
        ];
        let results = vec![
            GenerallyLockResult {
                success: true,
                ..Default::default()
            },
            GenerallyLockResult {
                success: false,
                ..Default::default()
            },
        ];
        let acquired = acquired_lock_ids(&lock_ids, &results);
        assert_eq!(
            acquired,
            vec![lock_ids[0].clone()],
            "only granted entries with a matching id are released"
        );
        assert!(acquired_lock_ids(&lock_ids, &[]).is_empty());
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
    async fn test_remote_client_acquire_locks_batch_uses_rpc_timeout_and_evicts_connection() {
        ensure_test_rpc_secret();
        let Some((addr, accept_task)) = spawn_hanging_listener().await else {
            return;
        };
        reset_lock_peer_health_for_test(&addr);
        cache_lazy_channel(&addr).await;
        assert!(runtime_sources::test_node_channel_is_cached(&addr).await);

        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_RPC_TIMEOUT_MS, Some("50"))], async {
            let client = RemoteClient::new(addr.clone());
            let requests = vec![test_lock_request(Duration::from_millis(5))];
            let started_at = tokio::time::Instant::now();

            let responses = client.acquire_locks_batch(&requests).await.unwrap();
            let elapsed = started_at.elapsed();

            assert!(
                elapsed >= Duration::from_millis(40),
                "remote batch lock RPC should use configured transport timeout, got {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_secs(1),
                "test RPC timeout should keep the test fast, got {elapsed:?}"
            );
            assert_eq!(responses.len(), 1);
            assert!(!responses[0].success, "timed out batch lock acquisition should fail");
            assert!(
                responses[0]
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("Remote lock RPC timed out")),
                "expected remote RPC timeout marker, got {:?}",
                responses[0].error
            );
            assert!(
                !runtime_sources::test_node_channel_is_cached(&addr).await,
                "batch transport timeout should evict cached connection"
            );
        })
        .await;

        accept_task.abort();
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
