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

use crate::server::runtime_sources;
use crate::server::{ServiceState, ServiceStateManager};
use crate::server::{has_path_prefix, is_table_catalog_path};
use crate::storage_api::server::{Endpoint, EndpointServerPools, StorageAdminApi, is_dist_erasure};
#[cfg(test)]
use crate::storage_api::server::{Endpoints, PoolEndpoints};
use bytes::Bytes;
use http::{Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use metrics::{counter, gauge};
use rustfs_common::GlobalReadiness;
use rustfs_madmin::{Disk, StorageInfo};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
    time::Instant,
};
use tokio::sync::Mutex;
use tower::{Layer, Service};
use tracing::{debug, info};

pub const STARTUP_RUNTIME_READINESS_MAX_WAIT: Duration = Duration::from_secs(30);
pub const STARTUP_RUNTIME_READINESS_POLL_INTERVAL: Duration = Duration::from_secs(1);
const METRIC_RUNTIME_READINESS_READY: &str = "rustfs_runtime_readiness_ready";
const METRIC_RUNTIME_READINESS_DEGRADED_TOTAL: &str = "rustfs_runtime_readiness_degraded_total";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DependencyReadiness {
    pub storage_ready: bool,
    pub iam_ready: bool,
    pub lock_quorum_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessDegradedReason {
    StorageQuorumUnavailable,
    IamNotReady,
    LockQuorumUnavailable,
    KmsNotReady,
    StorageAndIamUnavailable,
    StorageAndLockUnavailable,
    IamAndLockUnavailable,
    StorageIamAndLockUnavailable,
}

impl ReadinessDegradedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadinessDegradedReason::StorageQuorumUnavailable => "storage_quorum_unavailable",
            ReadinessDegradedReason::IamNotReady => "iam_not_ready",
            ReadinessDegradedReason::LockQuorumUnavailable => "lock_quorum_unavailable",
            ReadinessDegradedReason::KmsNotReady => "kms_not_ready",
            ReadinessDegradedReason::StorageAndIamUnavailable => "storage_and_iam_unavailable",
            ReadinessDegradedReason::StorageAndLockUnavailable => "storage_and_lock_unavailable",
            ReadinessDegradedReason::IamAndLockUnavailable => "iam_and_lock_unavailable",
            ReadinessDegradedReason::StorageIamAndLockUnavailable => "storage_iam_and_lock_unavailable",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyReadinessReport {
    pub readiness: DependencyReadiness,
    pub degraded_reasons: Vec<ReadinessDegradedReason>,
}

/// ReadinessGateLayer ensures that the system components (IAM, Storage)
/// are fully initialized before allowing any request to proceed.
#[derive(Clone)]
pub struct ReadinessGateLayer {
    readiness: Arc<GlobalReadiness>,
}

impl ReadinessGateLayer {
    /// Create a new ReadinessGateLayer
    /// # Arguments
    /// * `readiness` - An Arc to the GlobalReadiness instance
    ///
    /// # Returns
    /// A new instance of ReadinessGateLayer
    pub fn new(readiness: Arc<GlobalReadiness>) -> Self {
        Self { readiness }
    }
}

impl<S> Layer<S> for ReadinessGateLayer {
    type Service = ReadinessGateService<S>;

    /// Wrap the inner service with ReadinessGateService
    /// # Arguments
    /// * `inner` - The inner service to wrap
    /// # Returns
    /// An instance of ReadinessGateService
    fn layer(&self, inner: S) -> Self::Service {
        ReadinessGateService {
            inner,
            readiness: self.readiness.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ReadinessGateService<S> {
    inner: S,
    readiness: Arc<GlobalReadiness>,
}

fn is_probe_path(path: &str) -> bool {
    let is_exact_probe = matches!(
        path,
        crate::server::PROFILE_MEMORY_PATH
            | crate::server::PROFILE_CPU_PATH
            | crate::server::HEALTH_PREFIX
            | crate::server::HEALTH_COMPAT_LIVE_PATH
            | crate::server::HEALTH_READY_PATH
            | crate::server::MINIO_HEALTH_LIVE_PATH
            | crate::server::MINIO_HEALTH_READY_PATH
            | crate::server::MINIO_HEALTH_CLUSTER_PATH
            | crate::server::MINIO_HEALTH_CLUSTER_READ_PATH
            | crate::server::FAVICON_PATH
    );

    let is_prefix_probe = has_path_prefix(path, crate::server::RUSTFS_ADMIN_PREFIX)
        || has_path_prefix(path, crate::server::MINIO_ADMIN_V3_PREFIX)
        || is_table_catalog_path(path)
        || has_path_prefix(path, crate::server::CONSOLE_PREFIX)
        || has_path_prefix(path, crate::server::RPC_PREFIX)
        || has_path_prefix(path, crate::server::ADMIN_PREFIX)
        || has_path_prefix(path, crate::server::MINIO_ADMIN_PREFIX)
        || has_path_prefix(path, crate::server::TONIC_PREFIX);

    is_exact_probe || is_prefix_probe
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;
impl<S, B> Service<HttpRequest<Incoming>> for ReadinessGateService<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let mut inner = self.inner.clone();
        let readiness = self.readiness.clone();
        Box::pin(async move {
            let path = req.uri().path();
            debug!("ReadinessGateService: Received request for path: {}", path);
            let is_probe = is_probe_path(path);
            if !is_probe && !readiness.is_ready() {
                let body: BoxBody = Full::new(Bytes::from_static(b"Service not ready"))
                    .map_err(|e| -> BoxError { Box::new(e) })
                    .boxed_unsync();

                let resp = Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header(http::header::RETRY_AFTER, "5")
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .header(http::header::CACHE_CONTROL, "no-store")
                    .body(body)
                    .expect("failed to build not ready response");
                return Ok(resp);
            }
            let resp = inner.call(req).await?;
            // System is ready, forward to the actual S3/RPC handlers
            // Transparently converts any response body into a BoxBody, and then Trace/Cors/Compression continues to work
            let (parts, body) = resp.into_parts();
            let body: BoxBody = body.map_err(Into::into).boxed_unsync();
            Ok(Response::from_parts(parts, body))
        })
    }
}

pub async fn publish_ready_when_runtime_ready(
    readiness: &GlobalReadiness,
    state_manager: Option<&ServiceStateManager>,
) -> Result<(), std::io::Error> {
    wait_for_runtime_readiness_with(
        STARTUP_RUNTIME_READINESS_MAX_WAIT,
        STARTUP_RUNTIME_READINESS_POLL_INTERVAL,
        collect_node_readiness,
        |dependency_readiness| {
            readiness.mark_stage(rustfs_common::SystemStage::FullReady);
            if let Some(state_manager) = state_manager {
                state_manager.update(ServiceState::Ready);
            }
            info!(
                target: "rustfs::server::readiness",
                storage_ready = dependency_readiness.storage_ready,
                iam_ready = dependency_readiness.iam_ready,
                "Runtime node readiness reached; publishing ready state"
            );
        },
    )
    .await
}

#[derive(Debug, Clone, Copy)]
struct StorageReadinessCacheEntry {
    captured_at: Instant,
    storage_ready: bool,
}

#[derive(Debug, Clone, Copy)]
struct LockQuorumCacheEntry {
    captured_at: Instant,
    status: LockQuorumStatus,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockQuorumStatus {
    pub ready: bool,
    pub connected_clients: usize,
    pub total_clients: usize,
    pub required_quorum: usize,
}

const DISK_STATE_OK: &str = "ok";
const DISK_STATE_UNFORMATTED: &str = "unformatted";
const RUNTIME_STATE_RETURNING: &str = "returning";

fn health_readiness_cache_ttl() -> Duration {
    Duration::from_millis(rustfs_utils::get_env_u64(
        rustfs_config::ENV_HEALTH_READINESS_CACHE_TTL_MS,
        rustfs_config::DEFAULT_HEALTH_READINESS_CACHE_TTL_MS,
    ))
}

fn storage_readiness_cache() -> &'static Mutex<Option<StorageReadinessCacheEntry>> {
    static CACHE: OnceLock<Mutex<Option<StorageReadinessCacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

fn lock_quorum_status_cache() -> &'static Mutex<Option<LockQuorumCacheEntry>> {
    static CACHE: OnceLock<Mutex<Option<LockQuorumCacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

async fn load_cached_storage_readiness() -> Option<bool> {
    let ttl = health_readiness_cache_ttl();
    if ttl.is_zero() {
        return None;
    }

    let cache = storage_readiness_cache().lock().await;
    let entry = cache.as_ref()?;
    if entry.captured_at.elapsed() <= ttl {
        return Some(entry.storage_ready);
    }

    None
}

async fn update_storage_readiness_cache(storage_ready: bool) {
    if health_readiness_cache_ttl().is_zero() {
        return;
    }

    let mut cache = storage_readiness_cache().lock().await;
    *cache = Some(StorageReadinessCacheEntry {
        captured_at: Instant::now(),
        storage_ready,
    });
}

async fn load_cached_lock_quorum_status() -> Option<LockQuorumStatus> {
    let ttl = health_readiness_cache_ttl();
    if ttl.is_zero() {
        return None;
    }

    let cache = lock_quorum_status_cache().lock().await;
    let entry = cache.as_ref()?;
    if entry.captured_at.elapsed() <= ttl {
        return Some(entry.status);
    }

    None
}

async fn update_lock_quorum_status_cache(status: LockQuorumStatus) {
    if health_readiness_cache_ttl().is_zero() {
        return;
    }

    let mut cache = lock_quorum_status_cache().lock().await;
    *cache = Some(LockQuorumCacheEntry {
        captured_at: Instant::now(),
        status,
    });
}

fn disk_is_online_for_readiness(disk: &Disk) -> bool {
    let state_is_acceptable = disk.state.eq_ignore_ascii_case(DISK_STATE_OK)
        || disk.state.eq_ignore_ascii_case(rustfs_madmin::ITEM_ONLINE)
        || disk.state.eq_ignore_ascii_case(DISK_STATE_UNFORMATTED);

    if let Some(runtime_state) = disk.runtime_state.as_deref() {
        let runtime_state_is_acceptable = runtime_state.eq_ignore_ascii_case(rustfs_madmin::ITEM_ONLINE)
            || runtime_state.eq_ignore_ascii_case(RUNTIME_STATE_RETURNING);
        return runtime_state_is_acceptable && state_is_acceptable;
    }

    state_is_acceptable
}

fn pool_write_quorum(info: &StorageInfo, pool_idx: usize, set_drive_count: usize) -> usize {
    if set_drive_count == 0 {
        return 1;
    }

    let data_drives = info
        .backend
        .standard_sc_data
        .get(pool_idx)
        .copied()
        .filter(|count| *count > 0)
        .unwrap_or_else(|| (set_drive_count / 2).max(1));

    let parity_drives = if let Some(drives_per_set) = info.backend.drives_per_set.get(pool_idx).copied() {
        drives_per_set.saturating_sub(data_drives)
    } else if let Some(parity) = info.backend.standard_sc_parities.get(pool_idx).copied() {
        parity
    } else if let Some(parity) = info.backend.standard_sc_parity {
        parity
    } else {
        set_drive_count.saturating_sub(data_drives)
    };

    let mut write_quorum = data_drives;
    if data_drives == parity_drives {
        write_quorum += 1;
    }
    write_quorum.max(1)
}

fn pool_read_quorum(info: &StorageInfo, pool_idx: usize, set_drive_count: usize) -> usize {
    if set_drive_count == 0 {
        return 1;
    }

    info.backend
        .standard_sc_data
        .get(pool_idx)
        .copied()
        .filter(|count| *count > 0)
        .unwrap_or_else(|| (set_drive_count / 2).max(1))
        .max(1)
}

fn storage_ready_from_runtime_state_with_quorum<F>(info: &StorageInfo, quorum_for_set: F) -> bool
where
    F: Fn(&StorageInfo, usize, usize) -> usize,
{
    if info.disks.is_empty() {
        return false;
    }

    let mut total_online = 0usize;
    let mut set_online_counts: HashMap<(usize, usize), usize> = HashMap::new();
    let mut set_drive_counts: HashMap<(usize, usize), usize> = HashMap::new();
    let mut seen_disks: HashSet<(String, String, i32, i32, i32)> = HashSet::new();

    for disk in &info.disks {
        if disk.pool_index < 0 || disk.set_index < 0 {
            continue;
        }

        let dedup_key = (
            disk.endpoint.clone(),
            disk.drive_path.clone(),
            disk.pool_index,
            disk.set_index,
            disk.disk_index,
        );
        if !seen_disks.insert(dedup_key) {
            continue;
        }

        let pool_idx = disk.pool_index as usize;
        let set_idx = disk.set_index as usize;
        let key = (pool_idx, set_idx);
        *set_drive_counts.entry(key).or_default() += 1;

        if disk_is_online_for_readiness(disk) {
            total_online += 1;
            *set_online_counts.entry(key).or_default() += 1;
        }
    }

    if total_online == 0 || set_drive_counts.is_empty() {
        return false;
    }

    set_drive_counts.into_iter().all(|((pool_idx, set_idx), set_drive_count)| {
        let online = set_online_counts.get(&(pool_idx, set_idx)).copied().unwrap_or_default();
        let quorum = quorum_for_set(info, pool_idx, set_drive_count);
        online >= quorum
    })
}

fn storage_ready_from_runtime_state(info: &StorageInfo) -> bool {
    storage_ready_from_runtime_state_with_quorum(info, pool_write_quorum)
}

fn storage_read_ready_from_runtime_state(info: &StorageInfo) -> bool {
    storage_ready_from_runtime_state_with_quorum(info, pool_read_quorum)
}

fn degraded_reasons(storage_ready: bool, iam_ready_raw: bool, lock_quorum_ready: bool) -> Vec<ReadinessDegradedReason> {
    match (storage_ready, iam_ready_raw, lock_quorum_ready) {
        (true, true, true) => Vec::new(),
        (false, false, false) => vec![ReadinessDegradedReason::StorageIamAndLockUnavailable],
        (false, false, true) => vec![ReadinessDegradedReason::StorageAndIamUnavailable],
        (false, true, false) => vec![ReadinessDegradedReason::StorageAndLockUnavailable],
        (true, false, false) => vec![ReadinessDegradedReason::IamAndLockUnavailable],
        (false, true, true) => vec![ReadinessDegradedReason::StorageQuorumUnavailable],
        (true, false, true) => vec![ReadinessDegradedReason::IamNotReady],
        (true, true, false) => vec![ReadinessDegradedReason::LockQuorumUnavailable],
    }
}

fn record_readiness_report(report: &DependencyReadinessReport) {
    let ready = report.readiness.storage_ready && report.readiness.iam_ready && report.readiness.lock_quorum_ready;
    gauge!(METRIC_RUNTIME_READINESS_READY).set(if ready { 1.0 } else { 0.0 });
    for reason in &report.degraded_reasons {
        counter!(METRIC_RUNTIME_READINESS_DEGRADED_TOTAL, "reason" => reason.as_str()).increment(1);
    }
}

fn dependency_readiness_report_from_readiness(readiness: DependencyReadiness) -> DependencyReadinessReport {
    DependencyReadinessReport {
        degraded_reasons: degraded_reasons(readiness.storage_ready, readiness.iam_ready, readiness.lock_quorum_ready),
        readiness,
    }
}

pub async fn collect_dependency_readiness() -> DependencyReadiness {
    collect_dependency_readiness_report().await.readiness
}

pub async fn collect_dependency_readiness_report() -> DependencyReadinessReport {
    let iam_ready_raw = runtime_sources::iam_ready();
    let storage_ready = if let Some(cached) = load_cached_storage_readiness().await {
        cached
    } else {
        let computed = collect_storage_readiness_uncached().await;
        update_storage_readiness_cache(computed).await;
        computed
    };
    let lock_quorum_status = collect_lock_quorum_status().await;

    let readiness = DependencyReadiness {
        storage_ready,
        iam_ready: iam_ready_raw,
        lock_quorum_ready: lock_quorum_status.ready,
    };
    let report = dependency_readiness_report_from_readiness(readiness);
    record_readiness_report(&report);
    report
}

pub async fn collect_node_readiness_report() -> DependencyReadinessReport {
    let readiness = DependencyReadiness {
        storage_ready: runtime_sources::object_store_handle().is_some(),
        iam_ready: runtime_sources::iam_ready(),
        lock_quorum_ready: true,
    };
    let report = dependency_readiness_report_from_readiness(readiness);
    record_readiness_report(&report);
    report
}

async fn collect_node_readiness() -> DependencyReadiness {
    collect_node_readiness_report().await.readiness
}

pub async fn collect_cluster_read_dependency_readiness_report() -> DependencyReadinessReport {
    let iam_ready_raw = runtime_sources::iam_ready();
    let storage_ready = collect_storage_read_readiness_uncached().await;
    let lock_quorum_status = collect_lock_quorum_status().await;

    let readiness = DependencyReadiness {
        storage_ready,
        iam_ready: iam_ready_raw,
        lock_quorum_ready: lock_quorum_status.ready,
    };
    let report = dependency_readiness_report_from_readiness(readiness);
    record_readiness_report(&report);
    report
}

pub(crate) async fn snapshot_dependency_readiness_report() -> DependencyReadinessReport {
    let readiness = DependencyReadiness {
        storage_ready: collect_storage_readiness_uncached().await,
        iam_ready: runtime_sources::iam_ready(),
        lock_quorum_ready: collect_lock_quorum_status_uncached().await.ready,
    };

    dependency_readiness_report_from_readiness(readiness)
}

async fn collect_lock_quorum_status() -> LockQuorumStatus {
    if let Some(cached) = load_cached_lock_quorum_status().await {
        cached
    } else {
        let computed = collect_lock_quorum_status_uncached().await;
        update_lock_quorum_status_cache(computed).await;
        computed
    }
}

async fn collect_storage_readiness_uncached() -> bool {
    if let Some(store) = runtime_sources::object_store_handle() {
        let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;
        storage_ready_from_runtime_state(&storage_info)
    } else {
        false
    }
}

async fn collect_storage_read_readiness_uncached() -> bool {
    if let Some(store) = runtime_sources::object_store_handle() {
        let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;
        storage_read_ready_from_runtime_state(&storage_info)
    } else {
        false
    }
}

fn set_lock_quorum_status(online_hosts: &HashSet<String>, set_endpoints: &[Endpoint]) -> LockQuorumStatus {
    let total_clients = set_endpoints
        .iter()
        .map(Endpoint::host_port)
        .filter(|host| !host.is_empty())
        .collect::<HashSet<_>>();
    let total_clients_len = total_clients.len();
    if total_clients_len == 0 {
        return LockQuorumStatus::default();
    }

    let connected_clients = total_clients.iter().filter(|host| online_hosts.contains(*host)).count();
    let required_quorum = if total_clients_len > 1 {
        (total_clients_len / 2) + 1
    } else {
        1
    };

    LockQuorumStatus {
        ready: connected_clients >= required_quorum,
        connected_clients,
        total_clients: total_clients_len,
        required_quorum,
    }
}

fn aggregate_lock_quorum_status(pool_endpoints: &EndpointServerPools, online_hosts: &HashSet<String>) -> LockQuorumStatus {
    let mut connected_clients = 0usize;
    let mut total_clients = 0usize;
    let mut required_quorum = 0usize;

    for pool in pool_endpoints.as_ref() {
        for set_idx in 0..pool.set_count {
            let set_endpoints = pool
                .endpoints
                .as_ref()
                .iter()
                .filter(|endpoint| endpoint.set_idx == set_idx as i32)
                .cloned()
                .collect::<Vec<_>>();

            let status = set_lock_quorum_status(online_hosts, &set_endpoints);
            if status.total_clients == 0 {
                return LockQuorumStatus::default();
            }

            connected_clients += status.connected_clients;
            total_clients += status.total_clients;
            required_quorum += status.required_quorum;

            if !status.ready {
                return LockQuorumStatus {
                    ready: false,
                    connected_clients,
                    total_clients,
                    required_quorum,
                };
            }
        }
    }

    if total_clients == 0 {
        LockQuorumStatus::default()
    } else {
        LockQuorumStatus {
            ready: true,
            connected_clients,
            total_clients,
            required_quorum,
        }
    }
}

async fn collect_lock_quorum_status_uncached() -> LockQuorumStatus {
    if !is_dist_erasure().await {
        return LockQuorumStatus {
            ready: true,
            connected_clients: 1,
            total_clients: 1,
            required_quorum: 1,
        };
    }

    let Some(pool_endpoints) = runtime_sources::endpoints_handle() else {
        return LockQuorumStatus::default();
    };
    let Some(lock_clients) = runtime_sources::lock_clients_handle() else {
        return LockQuorumStatus::default();
    };

    let online_hosts = futures::future::join_all(lock_clients.iter().map(|(host, client)| {
        let host = host.clone();
        let client = client.clone();
        async move { (host, client.is_online().await) }
    }))
    .await
    .into_iter()
    .filter_map(|(host, online)| online.then_some(host))
    .collect::<HashSet<_>>();

    aggregate_lock_quorum_status(&pool_endpoints, &online_hosts)
}

pub async fn wait_for_runtime_readiness_with<F, Fut, ReadyFn>(
    max_wait: Duration,
    poll_interval: Duration,
    mut load_readiness: F,
    mut on_ready: ReadyFn,
) -> Result<(), std::io::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = DependencyReadiness>,
    ReadyFn: FnMut(DependencyReadiness),
{
    let startup_deadline = tokio::time::Instant::now() + max_wait;

    loop {
        let readiness = load_readiness().await;
        if readiness.storage_ready && readiness.iam_ready {
            on_ready(readiness);
            return Ok(());
        }

        if tokio::time::Instant::now() >= startup_deadline {
            let reason = format!(
                "startup readiness timed out after {}s: storage_ready={}, iam_ready={}, lock_quorum_ready={}",
                max_wait.as_secs(),
                readiness.storage_ready,
                readiness.iam_ready,
                readiness.lock_quorum_ready
            );
            return Err(std::io::Error::other(reason));
        }

        info!(
            target: "rustfs::server::readiness",
            storage_ready = readiness.storage_ready,
            iam_ready = readiness.iam_ready,
            lock_quorum_ready = readiness.lock_quorum_ready,
            "Runtime node readiness has not been reached yet; delaying ready state publication"
        );
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_madmin::{BackendInfo, Disk};
    use std::future;

    #[test]
    fn startup_runtime_readiness_wait_constants_are_ordered() {
        assert!(STARTUP_RUNTIME_READINESS_MAX_WAIT > STARTUP_RUNTIME_READINESS_POLL_INTERVAL);
        assert_eq!(STARTUP_RUNTIME_READINESS_MAX_WAIT.as_secs(), 30);
        assert_eq!(STARTUP_RUNTIME_READINESS_POLL_INTERVAL.as_secs(), 1);
    }

    #[tokio::test]
    async fn wait_for_runtime_readiness_with_does_not_publish_ready_when_runtime_readiness_is_not_reached() {
        let readiness = GlobalReadiness::new();
        let state_manager = ServiceStateManager::new();

        let err = wait_for_runtime_readiness_with(
            Duration::ZERO,
            Duration::from_millis(1),
            || {
                future::ready(DependencyReadiness {
                    storage_ready: false,
                    iam_ready: false,
                    lock_quorum_ready: false,
                })
            },
            |_| {
                readiness.mark_stage(rustfs_common::SystemStage::FullReady);
                state_manager.update(ServiceState::Ready);
            },
        )
        .await
        .expect_err("unready startup should time out");

        assert!(err.to_string().contains("startup readiness timed out"));
        assert!(!readiness.is_ready());
        assert_eq!(state_manager.current_state(), ServiceState::Starting);
    }

    #[tokio::test]
    async fn wait_for_runtime_readiness_with_publishes_ready_without_lock_quorum() {
        let readiness = GlobalReadiness::new();
        let state_manager = ServiceStateManager::new();

        let result = wait_for_runtime_readiness_with(
            Duration::ZERO,
            Duration::from_millis(1),
            || {
                future::ready(DependencyReadiness {
                    storage_ready: true,
                    iam_ready: true,
                    lock_quorum_ready: false,
                })
            },
            |_| {
                readiness.mark_stage(rustfs_common::SystemStage::FullReady);
                state_manager.update(ServiceState::Ready);
            },
        )
        .await;

        assert!(result.is_ok(), "lock quorum must not block node readiness publication");
        assert!(readiness.is_ready());
        assert_eq!(state_manager.current_state(), ServiceState::Ready);
    }

    #[test]
    fn probe_path_checks_admin_boundaries() {
        assert!(is_probe_path("/minio/admin/v3/info"));
        assert!(is_probe_path("/rustfs/admin/v3/info"));
        assert!(is_probe_path(&format!("{}/config", crate::server::TABLE_CATALOG_PREFIX)));
        assert!(is_probe_path("/_iceberg/v1/config"));
        assert!(is_probe_path("/rustfs/console/"));
        assert!(!is_probe_path("/minio/adminx/object"));
        assert!(!is_probe_path("/rustfs/adminx/object"));
        assert!(!is_probe_path("/bucket/object"));
    }

    #[test]
    fn storage_ready_from_runtime_state_returns_false_when_all_disks_faulty() {
        let info = StorageInfo {
            backend: BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![1],
                ..Default::default()
            },
            disks: vec![Disk {
                pool_index: 0,
                set_index: 0,
                state: "offline".to_string(),
                runtime_state: Some("offline".to_string()),
                ..Default::default()
            }],
        };

        assert!(!storage_ready_from_runtime_state(&info));
    }

    #[test]
    fn storage_ready_from_runtime_state_returns_true_when_set_meets_write_quorum() {
        let info = StorageInfo {
            backend: BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![1],
                ..Default::default()
            },
            disks: vec![Disk {
                pool_index: 0,
                set_index: 0,
                state: "ok".to_string(),
                runtime_state: Some("online".to_string()),
                ..Default::default()
            }],
        };

        assert!(storage_ready_from_runtime_state(&info));
    }

    #[test]
    fn storage_read_ready_from_runtime_state_allows_read_quorum_below_write_quorum() {
        let disks = (0..4)
            .map(|disk_index| Disk {
                endpoint: format!("127.0.0.1:900{disk_index}"),
                drive_path: format!("/data{disk_index}"),
                pool_index: 0,
                set_index: 0,
                disk_index,
                state: if disk_index < 2 { "ok" } else { "offline" }.to_string(),
                runtime_state: Some(if disk_index < 2 { "online" } else { "offline" }.to_string()),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let info = StorageInfo {
            backend: BackendInfo {
                standard_sc_data: vec![2],
                drives_per_set: vec![4],
                ..Default::default()
            },
            disks,
        };

        assert!(storage_read_ready_from_runtime_state(&info));
        assert!(!storage_ready_from_runtime_state(&info));
    }

    #[test]
    fn storage_ready_from_runtime_state_deduplicates_duplicate_disk_rows() {
        let duplicate_disk = Disk {
            endpoint: "127.0.0.1:9000".to_string(),
            drive_path: "/data0".to_string(),
            pool_index: 0,
            set_index: 0,
            disk_index: 0,
            state: "ok".to_string(),
            runtime_state: Some("online".to_string()),
            ..Default::default()
        };
        let info = StorageInfo {
            backend: BackendInfo {
                standard_sc_data: vec![2],
                drives_per_set: vec![4],
                ..Default::default()
            },
            disks: vec![duplicate_disk.clone(), duplicate_disk],
        };

        assert!(!storage_ready_from_runtime_state(&info), "duplicate rows must not satisfy write quorum");
    }

    #[test]
    fn disk_online_for_readiness_requires_runtime_and_state_both_acceptable() {
        let disk = Disk {
            state: "disk io error".to_string(),
            runtime_state: Some("online".to_string()),
            ..Default::default()
        };
        assert!(!disk_is_online_for_readiness(&disk));
    }

    #[test]
    fn storage_ready_from_runtime_state_requires_all_sets_meet_quorum() {
        let info = StorageInfo {
            backend: BackendInfo {
                standard_sc_data: vec![1],
                drives_per_set: vec![2],
                ..Default::default()
            },
            disks: vec![
                Disk {
                    endpoint: "127.0.0.1:9000".to_string(),
                    drive_path: "/set0d0".to_string(),
                    pool_index: 0,
                    set_index: 0,
                    disk_index: 0,
                    state: "ok".to_string(),
                    runtime_state: Some("online".to_string()),
                    ..Default::default()
                },
                Disk {
                    endpoint: "127.0.0.1:9000".to_string(),
                    drive_path: "/set1d0".to_string(),
                    pool_index: 0,
                    set_index: 1,
                    disk_index: 0,
                    state: "offline".to_string(),
                    runtime_state: Some("offline".to_string()),
                    ..Default::default()
                },
            ],
        };

        assert!(
            !storage_ready_from_runtime_state(&info),
            "if any set fails write quorum, readiness must be false"
        );
    }

    #[test]
    fn aggregate_lock_quorum_status_requires_each_set_to_meet_quorum() {
        let endpoints = vec![
            Endpoint {
                url: url::Url::parse("http://node1:9000/data1").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            },
            Endpoint {
                url: url::Url::parse("http://node2:9000/data2").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 1,
            },
            Endpoint {
                url: url::Url::parse("http://node3:9000/data3").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 1,
                disk_idx: 0,
            },
            Endpoint {
                url: url::Url::parse("http://node4:9000/data4").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 1,
                disk_idx: 1,
            },
        ];
        let pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }]);

        let status = aggregate_lock_quorum_status(
            &pools,
            &["node1:9000", "node2:9000", "node3:9000", "node4:9000"]
                .into_iter()
                .map(str::to_string)
                .collect::<HashSet<_>>(),
        );

        assert!(status.ready);
        assert_eq!(status.connected_clients, 4);
        assert_eq!(status.total_clients, 4);
        assert_eq!(status.required_quorum, 4);
    }

    #[test]
    fn aggregate_lock_quorum_status_fails_when_any_set_loses_quorum() {
        let endpoints = vec![
            Endpoint {
                url: url::Url::parse("http://node1:9000/data1").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            },
            Endpoint {
                url: url::Url::parse("http://node2:9000/data2").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 1,
            },
            Endpoint {
                url: url::Url::parse("http://node3:9000/data3").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 1,
                disk_idx: 0,
            },
            Endpoint {
                url: url::Url::parse("http://node4:9000/data4").unwrap(),
                is_local: false,
                pool_idx: 0,
                set_idx: 1,
                disk_idx: 1,
            },
        ];
        let pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }]);

        let status =
            aggregate_lock_quorum_status(&pools, &["node1:9000"].into_iter().map(str::to_string).collect::<HashSet<_>>());

        assert!(!status.ready);
    }

    #[test]
    fn degraded_reasons_include_lock_quorum_failures() {
        assert_eq!(degraded_reasons(true, true, false), vec![ReadinessDegradedReason::LockQuorumUnavailable]);
        assert_eq!(
            degraded_reasons(false, true, false),
            vec![ReadinessDegradedReason::StorageAndLockUnavailable]
        );
        assert_eq!(degraded_reasons(true, false, false), vec![ReadinessDegradedReason::IamAndLockUnavailable]);
        assert_eq!(
            degraded_reasons(false, false, false),
            vec![ReadinessDegradedReason::StorageIamAndLockUnavailable]
        );
    }

    #[tokio::test]
    async fn lock_quorum_status_cache_roundtrip() {
        let cache = lock_quorum_status_cache();
        {
            let mut guard = cache.lock().await;
            *guard = None;
        }

        update_lock_quorum_status_cache(LockQuorumStatus {
            ready: true,
            connected_clients: 2,
            total_clients: 3,
            required_quorum: 2,
        })
        .await;

        let cached = load_cached_lock_quorum_status().await;
        assert_eq!(
            cached,
            Some(LockQuorumStatus {
                ready: true,
                connected_clients: 2,
                total_clients: 3,
                required_quorum: 2,
            })
        );
    }
}
