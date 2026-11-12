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

use futures::{StreamExt, stream};

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock, OnceLock, RwLock as StdRwLock},
    time::SystemTime,
};

use crate::bucket::bandwidth::monitor::Monitor;
use crate::disk::endpoint::Endpoint;
use crate::runtime::instance::InstanceContext;
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::{ExpiryState, TransitionState},
    bucket::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys},
    bucket::replication::{DynReplicationPool, ReplicationStats},
    config::{get_global_storage_class, get_global_storage_class_snapshot, set_global_storage_class, storageclass},
    disk::{DiskAPI, DiskOption, DiskStore, error::DiskError, new_disk},
    error::{Error, Result},
    layout::{
        disks_layout::MAX_ERASURE_SET_DRIVE_COUNT,
        endpoints::{EndpointServerPools, SetupType},
    },
    runtime::global::{
        GLOBAL_BOOT_TIME, GLOBAL_LIFECYCLE_SYS, GLOBAL_LOCAL_NODE_NAME_FALLBACK, GLOBAL_ROOT_DISK_THRESHOLD,
        TypeLocalDiskSetDrives, get_background_services_cancel_token, get_global_bucket_monitor, get_global_deployment_id,
        get_global_endpoints, get_global_endpoints_opt, get_global_lock_client, get_global_lock_clients, get_global_region,
        get_global_tier_config_mgr, global_rustfs_port, init_global_bucket_monitor, is_dist_erasure, is_erasure, is_erasure_sd,
        is_first_cluster_node_local, resolve_object_store_handle, set_global_lock_client, set_global_lock_clients,
        set_object_layer, update_erasure_type,
    },
    services::batch_processor::{GlobalBatchProcessors, get_global_processors},
    services::notification_sys::{NotificationSys, get_global_notification_sys},
    services::tier::tier::TierConfigMgr,
    store::ECStore,
};
use rustfs_concurrency::WorkloadAdmissionSnapshotProvider;
use rustfs_config::server_config::{Config, get_global_server_config, set_global_server_config};
use rustfs_gateway_types::persistence::PersistedLifecycleConfiguration;
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_lock::client::LockClient;
use rustfs_s3_types::Region;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;
#[cfg(test)]
use tonic::transport::Channel;
use uuid::Uuid;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_RUNTIME_SOURCES: &str = "runtime_sources";
const EVENT_LOCAL_DISK_REGISTRATION: &str = "local_disk_registration";

#[cfg(test)]
const TEST_RPC_SECRET: &str = "test-rpc-secret";

pub(crate) type WorkloadSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;
pub type ScannerDirtyUsageMutationObserver = Arc<dyn Fn(&str, &str, ScannerDirtyUsageMutationSource) + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerDirtyUsageMutationSource {
    Replication,
    TierExpiration,
}

#[derive(Clone, Default)]
pub(crate) struct LockRegistry {
    clients: HashMap<String, Arc<dyn LockClient>>,
}

impl LockRegistry {
    pub(crate) fn new(clients: HashMap<String, Arc<dyn LockClient>>) -> Self {
        Self { clients }
    }

    pub(crate) fn clients_for_endpoints(&self, endpoints: &[Endpoint]) -> Vec<Arc<dyn LockClient>> {
        let mut seen_hosts = HashSet::with_capacity(endpoints.len());
        let mut clients = Vec::with_capacity(endpoints.len());

        for endpoint in endpoints {
            let host_port = endpoint.host_port();
            if host_port.is_empty() || !seen_hosts.insert(host_port.clone()) {
                continue;
            }

            if let Some(client) = self.clients.get(&host_port) {
                clients.push(client.clone());
            }
        }

        clients
    }
}

static WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER: OnceLock<WorkloadSnapshotProviderRef> = OnceLock::new();
static SCANNER_DIRTY_USAGE_MUTATION_OBSERVER: LazyLock<StdRwLock<Option<ScannerDirtyUsageMutationObserver>>> =
    LazyLock::new(|| StdRwLock::new(None));

pub(crate) fn set_workload_admission_snapshot_provider(
    provider: WorkloadSnapshotProviderRef,
) -> std::result::Result<(), WorkloadSnapshotProviderRef> {
    WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER.set(provider)
}

pub(crate) fn workload_admission_snapshot_provider() -> Option<WorkloadSnapshotProviderRef> {
    WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER.get().cloned()
}

pub fn set_scanner_dirty_usage_mutation_observer(
    observer: Option<ScannerDirtyUsageMutationObserver>,
) -> Option<ScannerDirtyUsageMutationObserver> {
    let mut slot = SCANNER_DIRTY_USAGE_MUTATION_OBSERVER
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::mem::replace(&mut *slot, observer)
}

pub(crate) fn notify_scanner_dirty_usage_mutation(bucket: &str, object: &str, source: ScannerDirtyUsageMutationSource) {
    if bucket.is_empty() || object.is_empty() {
        return;
    }
    let observer = SCANNER_DIRTY_USAGE_MUTATION_OBSERVER
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(observer) = observer {
        observer(bucket, object, source);
    }
}

pub(crate) fn record_erasure_write_quorum_failure(stage: &'static str, dominant_error: &'static str) {
    global_internode_metrics().record_erasure_write_quorum_failure(stage, dominant_error);
}

pub fn object_store_handle() -> Option<Arc<ECStore>> {
    resolve_object_store_handle()
}

pub fn endpoint_pools() -> Option<EndpointServerPools> {
    get_global_endpoints_opt()
}

pub(crate) fn endpoint_pools_or_default() -> EndpointServerPools {
    get_global_endpoints()
}

pub(crate) fn endpoint_erasure_set_count() -> Option<usize> {
    endpoint_pools().map(|endpoints| endpoints.es_count())
}

pub(crate) fn endpoint_pool_is_local(endpoints: &EndpointServerPools, pool_index: usize) -> bool {
    endpoints
        .as_ref()
        .get(pool_index)
        .is_some_and(|pool| pool.endpoints.as_ref().first().is_some_and(|endpoint| endpoint.is_local))
}

pub async fn first_cluster_node_is_local() -> bool {
    is_first_cluster_node_local().await
}

pub async fn setup_is_erasure() -> bool {
    is_erasure().await
}

pub async fn setup_is_dist_erasure() -> bool {
    is_dist_erasure().await
}

pub async fn setup_is_erasure_sd() -> bool {
    is_erasure_sd().await
}

#[allow(
    dead_code,
    reason = "setup-type override used only by tests across this crate (backlog#1823)"
)]
pub(crate) async fn current_setup_type() -> SetupType {
    if setup_is_dist_erasure().await {
        SetupType::DistErasure
    } else if setup_is_erasure_sd().await {
        SetupType::ErasureSD
    } else if setup_is_erasure().await {
        SetupType::Erasure
    } else {
        SetupType::Unknown
    }
}

#[allow(
    dead_code,
    reason = "setup-type override used only by tests across this crate (backlog#1823)"
)]
pub(crate) async fn set_setup_type(setup_type: SetupType) {
    update_erasure_type(setup_type).await;
}

pub(crate) async fn local_node_name() -> String {
    rustfs_common::get_global_local_node_name().await
}

pub(crate) async fn set_local_node_name(node_name: String) {
    // Also stamp the internode-metrics server label: io-metrics is a leaf
    // crate and no longer resolves node identity itself (backlog#1834).
    rustfs_io_metrics::internode_metrics::set_internode_server_label(node_name.as_str());
    rustfs_common::set_global_local_node_name(&node_name).await;
}

pub(crate) fn default_local_node_name() -> String {
    GLOBAL_LOCAL_NODE_NAME_FALLBACK.to_string()
}

pub fn rustfs_port() -> u16 {
    global_rustfs_port()
}

pub(crate) fn background_services_cancel_token() -> Option<CancellationToken> {
    get_background_services_cancel_token()
}

pub(crate) async fn rustfs_host() -> String {
    rustfs_common::get_global_rustfs_host().await
}

pub(crate) async fn rustfs_addr() -> String {
    rustfs_common::get_global_addr().await
}

pub fn boot_time() -> Option<SystemTime> {
    GLOBAL_BOOT_TIME.get().cloned()
}

pub(crate) fn boot_uptime_secs() -> u64 {
    boot_time()
        .and_then(|boot_time| SystemTime::now().duration_since(boot_time).ok())
        .unwrap_or_default()
        .as_secs()
}

pub(crate) async fn ensure_boot_time() {
    GLOBAL_BOOT_TIME.get_or_init(|| async { SystemTime::now() }).await;
}

pub(crate) async fn root_disk_threshold_for_erasure_disk() -> Option<u64> {
    if is_erasure_sd().await {
        None
    } else {
        Some(*GLOBAL_ROOT_DISK_THRESHOLD.read().await)
    }
}

#[cfg(test)]
pub(crate) async fn cache_test_node_channel(addr: String, channel: Channel) {
    rustfs_common::cache_connection(addr, channel).await;
}

#[cfg(test)]
pub(crate) async fn test_node_channel_is_cached(addr: &str) -> bool {
    rustfs_common::has_cached_connection(addr).await
}

#[cfg(test)]
pub(crate) fn ensure_test_rpc_secret() {
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_owned());
}

pub(crate) fn deployment_upload_id(upload_id: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_id).as_bytes())
}

pub fn deployment_id() -> Option<String> {
    get_global_deployment_id()
}

/// Test-only inverse of [`deployment_upload_id`]: returns the raw
/// `<uuid>x<timestamp>` suffix without the deployment-id prefix. Under plain
/// `cargo test` (thread-parallel, shared process globals) a concurrently
/// running test that re-initializes a store can swap the global deployment id
/// between create time and list time, so assertions must compare only this
/// suffix, never the full encoded upload id.
#[cfg(test)]
pub(crate) fn upload_uuid_suffix(upload_id: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(upload_id.as_bytes())
        .ok()
        .and_then(|decoded| String::from_utf8(decoded).ok())
        .and_then(|decoded| decoded.split_once('.').map(|(_, suffix)| suffix.to_owned()))
        .unwrap_or_else(|| upload_id.to_owned())
}

pub(crate) fn replication_pool() -> Option<Arc<DynReplicationPool>> {
    crate::runtime::global::current_ctx().replication_pool()
}

pub(crate) fn replication_stats() -> Option<Arc<ReplicationStats>> {
    crate::runtime::global::current_ctx().replication_stats()
}

pub(crate) fn replication_runtime_initialized() -> bool {
    crate::runtime::global::current_ctx().replication_initialized()
}

pub fn global_lock_client() -> Option<Arc<dyn LockClient>> {
    get_global_lock_client()
}

pub fn global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn LockClient>>> {
    get_global_lock_clients()
}

pub(crate) fn lock_registry() -> Option<LockRegistry> {
    global_lock_clients()
        .map(|clients| LockRegistry::new(clients.iter().map(|(host, client)| (host.clone(), client.clone())).collect()))
}

pub(crate) fn set_primary_lock_client(client: Arc<dyn LockClient>) -> std::result::Result<(), Arc<dyn LockClient>> {
    set_global_lock_client(client)
}

pub(crate) fn set_lock_clients(
    clients: HashMap<String, Arc<dyn LockClient>>,
) -> std::result::Result<(), HashMap<String, Arc<dyn LockClient>>> {
    set_global_lock_clients(clients)
}

pub(crate) async fn publish_object_store(store: Arc<ECStore>) {
    set_object_layer(store).await;
}

pub(crate) fn notification_sys() -> Option<Arc<NotificationSys>> {
    get_global_notification_sys()
}

pub(crate) fn bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    get_global_bucket_metadata_sys()
}

pub fn region() -> Option<Region> {
    get_global_region()
}

pub(crate) fn server_config() -> Option<Config> {
    get_global_server_config()
}

pub(crate) fn set_server_config(config: Config) {
    set_global_server_config(config);
}

pub(crate) fn storage_class_config() -> Option<storageclass::Config> {
    get_global_storage_class()
}

pub(crate) fn storage_class_config_snapshot() -> Arc<storageclass::Config> {
    get_global_storage_class_snapshot()
}

pub(crate) fn set_storage_class_config(config: storageclass::Config) {
    set_global_storage_class(config);
}

pub(crate) fn batch_processors() -> &'static GlobalBatchProcessors {
    get_global_processors()
}

pub fn global_tier_config_mgr() -> Arc<RwLock<TierConfigMgr>> {
    get_global_tier_config_mgr()
}

pub(crate) async fn bucket_lifecycle_config(bucket: &str) -> Option<PersistedLifecycleConfiguration> {
    GLOBAL_LIFECYCLE_SYS.get(bucket).await
}

pub(crate) fn delete_bucket_monitor_entry(bucket: &str) {
    if let Some(monitor) = get_global_bucket_monitor() {
        monitor.delete_bucket(bucket);
    }
}

pub fn bucket_monitor() -> Option<Arc<Monitor>> {
    get_global_bucket_monitor()
}

/// Acquire a read guard over the local disk map as an **owned** guard.
///
/// Returns an [`OwnedRwLockReadGuard`] (holding an `Arc` clone of the lock)
/// rather than a `'static` borrow of the process global. This decouples callers
/// (notably the heal crate, which holds the guard across `.await`) from the
/// global's `'static` lifetime, so the map can later move into the per-instance
/// `InstanceContext` (Phase 5 disk-registry migration, backlog#939) without a
/// cross-crate signature change. Single-instance behavior is unchanged.
pub async fn local_disk_map_read() -> OwnedRwLockReadGuard<HashMap<String, Option<DiskStore>>> {
    local_disk_map_handle().read_owned().await
}

pub(crate) fn init_bucket_monitor_for_current_endpoints() {
    let num_nodes = get_global_endpoints().get_nodes().len().try_into().unwrap_or(u64::MAX);
    init_global_bucket_monitor(num_nodes);
}

pub(crate) fn local_disk_map_handle() -> Arc<RwLock<HashMap<String, Option<DiskStore>>>> {
    crate::runtime::global::current_ctx().local_disk_map()
}

pub(crate) fn local_disk_id_map_handle() -> Arc<RwLock<HashMap<Uuid, String>>> {
    crate::runtime::global::current_ctx().local_disk_id_map()
}

pub(crate) fn tier_config_mgr_handle() -> Arc<RwLock<TierConfigMgr>> {
    get_global_tier_config_mgr()
}

pub fn expiry_state_handle() -> Arc<RwLock<ExpiryState>> {
    crate::runtime::global::current_ctx().expiry_state()
}

pub fn transition_state_handle() -> Arc<TransitionState> {
    crate::runtime::global::current_ctx().transition_state()
}

pub(crate) async fn local_disk_by_path(path: &str) -> Option<DiskStore> {
    local_disk_map_handle().read().await.get(path).cloned().flatten()
}

pub(crate) async fn local_disk_path_by_id(disk_id: &Uuid) -> Option<String> {
    local_disk_id_map_handle().read().await.get(disk_id).cloned()
}

#[cfg(test)]
pub(crate) async fn clear_local_disk_id_map_for_test() {
    local_disk_id_map_handle().write().await.clear();
}

pub(crate) async fn replace_local_disk_id(previous: Option<Uuid>, current: Option<Uuid>, endpoint: String) {
    let id_map = local_disk_id_map_handle();
    let mut disk_id_map = id_map.write().await;
    if let Some(previous_id) = previous
        && disk_id_map
            .get(&previous_id)
            .is_some_and(|registered_endpoint| registered_endpoint == &endpoint)
    {
        disk_id_map.remove(&previous_id);
    }
    if let Some(current_id) = current {
        disk_id_map.insert(current_id, endpoint);
    }
}

pub(crate) async fn reconcile_local_disk_ids(
    instance_ctx: &InstanceContext,
    pool_endpoints: &[String],
    selected: &[(Uuid, String)],
) {
    let pool_endpoints = pool_endpoints.iter().map(String::as_str).collect::<HashSet<_>>();
    let disk_id_map = instance_ctx.local_disk_id_map();
    let mut disk_ids = disk_id_map.write().await;
    disk_ids.retain(|_, registered_endpoint| !pool_endpoints.contains(registered_endpoint.as_str()));
    disk_ids.extend(selected.iter().cloned());
}

pub(crate) async fn quarantine_local_disks(instance_ctx: &InstanceContext, endpoints: &[Endpoint]) -> Result<()> {
    quarantine_local_disks_preserving_bootstrap_routes(instance_ctx, endpoints, &[]).await
}

/// Keep an already registered path available to its in-flight format writer.
/// All runtime voting slots are still cleared; no route is admitted or revived.
/// Only the format loader's explicit local unformatted observations may use it.
pub(crate) async fn quarantine_local_disks_preserving_bootstrap_routes(
    instance_ctx: &InstanceContext,
    endpoints: &[Endpoint],
    unformatted_bootstrap_routes: &[Endpoint],
) -> Result<()> {
    if unformatted_bootstrap_routes
        .iter()
        .any(|route| !route.is_local || !endpoints.contains(route))
    {
        return Err(Error::CorruptedFormat);
    }
    let bootstrap_routes = unformatted_bootstrap_routes.iter().collect::<HashSet<_>>();
    let targets = endpoints
        .iter()
        .map(|endpoint| {
            Ok((
                endpoint.to_string(),
                (
                    usize::try_from(endpoint.pool_idx).map_err(|_| Error::CorruptedFormat)?,
                    usize::try_from(endpoint.set_idx).map_err(|_| Error::CorruptedFormat)?,
                    usize::try_from(endpoint.disk_idx).map_err(|_| Error::CorruptedFormat)?,
                ),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // All registry writers follow registration -> set layout -> endpoint map.
    let _registration = instance_ctx.local_disk_registration_lock().lock().await;
    let set_drives = instance_ctx.local_disk_set_drives();
    let map = instance_ctx.local_disk_map();
    let mut slots = set_drives.write().await;
    let mut disks = map.write().await;
    let standalone = slots.is_empty();
    for (endpoint, (key, (pool, set, index))) in endpoints.iter().zip(&targets) {
        if !disks.contains_key(key) && !standalone {
            return Err(Error::CorruptedFormat);
        }
        if !slots.is_empty() {
            let slot = slots
                .get(*pool)
                .and_then(|sets| sets.get(*set))
                .and_then(|drives| drives.get(*index))
                .ok_or(Error::CorruptedFormat)?;
            if slot.as_ref().is_some_and(|disk| disk.endpoint() != *endpoint) {
                return Err(DiskError::InconsistentDisk.into());
            }
        }
    }
    let mut displaced = Vec::new();
    for (endpoint, (key, (pool, set, index))) in endpoints.iter().zip(targets) {
        if !bootstrap_routes.contains(endpoint)
            && let Some(Some(previous)) = disks.insert(key, None)
        {
            displaced.push(previous);
        }
        if !slots.is_empty()
            && let Some(previous) = slots[pool][set][index].take()
        {
            displaced.push(previous);
        }
    }
    drop(disks);
    drop(slots);
    drop(displaced);
    Ok(())
}

#[cfg(test)]
pub(crate) async fn record_local_disks(instance_ctx: &Arc<InstanceContext>, disks: Vec<DiskStore>) {
    let _registration = instance_ctx.local_disk_registration_lock().lock().await;
    let set_drives = instance_ctx.local_disk_set_drives();
    let standalone = set_drives.read().await.is_empty();
    let map = instance_ctx.local_disk_map();
    let mut registered = map.write().await;
    let mut displaced = Vec::new();
    for disk in disks {
        let path = disk.endpoint().to_string();
        if !standalone {
            match registered.get(&path) {
                None => continue,
                Some(Some(current)) if !Arc::ptr_eq(current, &disk) => continue,
                _ => {}
            }
        }
        if let Some(Some(previous)) = registered.insert(path, Some(disk)) {
            displaced.push(previous);
        }
    }
    drop(registered);
    drop(displaced);
}

pub(crate) async fn local_disk_set_drive(
    instance_ctx: &Arc<InstanceContext>,
    pool_idx: usize,
    set_idx: usize,
    disk_idx: usize,
) -> Option<DiskStore> {
    instance_ctx.local_disk_set_drives().read().await[pool_idx][set_idx][disk_idx].clone()
}

pub(crate) async fn local_disk_paths() -> Vec<String> {
    local_disk_map_handle().read().await.keys().cloned().collect()
}

/// Local disks registered on an explicit instance context (backlog#1052 S7).
pub(crate) async fn local_disks_in(instance_ctx: &InstanceContext) -> Vec<DiskStore> {
    instance_ctx
        .local_disk_map()
        .read()
        .await
        .values()
        .filter_map(|v| v.as_ref().cloned())
        .collect()
}

pub(crate) async fn local_disks() -> Vec<DiskStore> {
    local_disk_map_handle()
        .read()
        .await
        .values()
        .filter_map(|v| v.as_ref().cloned())
        .collect()
}

pub(crate) async fn local_disk_entries() -> Vec<Option<DiskStore>> {
    local_disk_map_handle().read().await.values().cloned().collect()
}

pub(crate) const LOCAL_DISK_OPEN_CONCURRENCY: usize = MAX_ERASURE_SET_DRIVE_COUNT;

#[derive(Clone, Copy)]
struct LocalDiskSlot {
    pool: usize,
    set: usize,
    disk: usize,
}

impl LocalDiskSlot {
    fn from_endpoint(endpoint: &Endpoint) -> Result<Self> {
        Ok(Self {
            pool: usize::try_from(endpoint.pool_idx).map_err(|_| Error::CorruptedFormat)?,
            set: usize::try_from(endpoint.set_idx).map_err(|_| Error::CorruptedFormat)?,
            disk: usize::try_from(endpoint.disk_idx).map_err(|_| Error::CorruptedFormat)?,
        })
    }

    fn get<'a>(&self, slots: &'a [Vec<Vec<Option<DiskStore>>>], endpoint: &Endpoint) -> Result<&'a Option<DiskStore>> {
        let slot = slots
            .get(self.pool)
            .and_then(|sets| sets.get(self.set))
            .and_then(|drives| drives.get(self.disk))
            .ok_or(Error::CorruptedFormat)?;
        if slot.as_ref().is_some_and(|disk| disk.endpoint() != *endpoint) {
            return Err(DiskError::InconsistentDisk.into());
        }
        Ok(slot)
    }
}

/// Publication authority captured before disk I/O. A path-only registration
/// supports callers that own a local set without a full instance layout.
pub(crate) struct LocalDiskRegistration {
    endpoint: Endpoint,
    expected: Option<DiskStore>,
    slot: Option<LocalDiskSlot>,
}

impl LocalDiskRegistration {
    fn new(endpoint: &Endpoint, expected: Option<DiskStore>, layout: &[Vec<Vec<Option<DiskStore>>>]) -> Result<Self> {
        if !endpoint.is_local {
            return Err(DiskError::InconsistentDisk.into());
        }
        let slot = LocalDiskSlot::from_endpoint(endpoint)?;
        Ok(Self {
            endpoint: endpoint.clone(),
            expected,
            slot: (!layout.is_empty()).then_some(slot),
        })
    }
}

/// Capture the expected route before a reconnect prepares a new physical handle.
pub(crate) async fn snapshot_local_disk_registration(
    instance_ctx: &InstanceContext,
    endpoint: &Endpoint,
) -> Result<LocalDiskRegistration> {
    // Snapshot readers follow the publication order: layout, then endpoint map.
    let set_drives = instance_ctx.local_disk_set_drives();
    let map = instance_ctx.local_disk_map();
    let slots = set_drives.read().await;
    let disks = map.read().await;
    if !slots.is_empty() && !disks.contains_key(&endpoint.to_string()) {
        return Err(DiskError::DiskNotFound.into());
    }
    let registration = LocalDiskRegistration::new(endpoint, disks.get(&endpoint.to_string()).cloned().flatten(), &slots)?;
    if let Some(slot) = registration.slot {
        slot.get(&slots, endpoint)?;
    }
    Ok(registration)
}

pub(crate) struct PreparedLocalDisk {
    registration: LocalDiskRegistration,
    pub(crate) disk: Option<DiskStore>,
}

impl PreparedLocalDisk {
    pub(crate) fn new(registration: LocalDiskRegistration, disk: Option<DiskStore>) -> Result<Self> {
        if disk.as_ref().is_some_and(|disk| disk.endpoint() != registration.endpoint) {
            return Err(DiskError::InconsistentDisk.into());
        }
        Ok(Self { registration, disk })
    }
}

pub(crate) struct PreparedDisk {
    pub(crate) endpoint: Endpoint,
    pub(crate) result: crate::disk::error::Result<DiskStore>,
    pub(crate) registration: crate::disk::error::Result<Option<LocalDiskRegistration>>,
}

impl PreparedDisk {
    /// Retry admits missing or inaccessible roots as offline while retaining
    /// the membership snapshot captured before physical preparation.
    pub(crate) fn into_local_registration(self) -> Result<PreparedLocalDisk> {
        let disk = local_disk_or_offline(self.result)?;
        let registration = match self.registration? {
            Some(registration) => registration,
            None => LocalDiskRegistration::new(&self.endpoint, None, &[])?,
        };
        PreparedLocalDisk::new(registration, disk)
    }
}

fn local_disk_or_offline(result: crate::disk::error::Result<DiskStore>) -> Result<Option<DiskStore>> {
    match result {
        Ok(disk) => Ok(Some(disk)),
        Err(DiskError::DiskNotFound | DiskError::FileAccessDenied) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Open or reuse one local endpoint while retaining its original disk error.
/// Registry readers are never held across root validation or filesystem work.
async fn prepare_local_disk(
    instance_ctx: &InstanceContext,
    endpoint: &Endpoint,
    registered: Option<DiskStore>,
    opt: &DiskOption,
) -> crate::disk::error::Result<DiskStore> {
    let reusable = match registered.as_ref() {
        Some(disk) if disk.endpoint() != *endpoint => return Err(DiskError::InconsistentDisk),
        Some(disk) => match disk.as_ref() {
            crate::disk::Disk::Local(wrapper) => wrapper.get_disk().has_current_root().await,
            crate::disk::Disk::Remote(_) => return Err(DiskError::InconsistentDisk),
        },
        None => false,
    };
    let disk = match registered {
        Some(disk) if reusable => disk,
        previous => {
            // A registered root can still own live staging. A fresh handle
            // for that endpoint must not run startup cleanup against it.
            let options = DiskOption {
                cleanup: opt.cleanup && previous.is_none(),
                health_check: opt.health_check,
            };
            new_disk(endpoint, &options).await?
        }
    };
    if let Some(authority) = instance_ctx.bound_multipart_proof_verification_context()
        && let crate::disk::Disk::Local(wrapper) = disk.as_ref()
    {
        wrapper.get_disk().bind_multipart_proof_authority(&authority)?;
    }
    Ok(disk)
}

/// Snapshot once, then open independent disks with bounded concurrency and
/// retain their original order and errors. The caller holds the registration
/// lock until publication; inventory readers remain available during disk I/O.
pub(crate) async fn prepare_disks(instance_ctx: &InstanceContext, endpoints: &[Endpoint], opt: &DiskOption) -> Vec<PreparedDisk> {
    let set_drives = instance_ctx.local_disk_set_drives();
    let map = instance_ctx.local_disk_map();
    let snapshots = {
        let slots = set_drives.read().await;
        let disks = map.read().await;
        endpoints
            .iter()
            .map(|endpoint| {
                let entry = endpoint.is_local.then(|| disks.get(&endpoint.to_string())).flatten();
                let previous = entry.cloned().flatten();
                let registration = if !endpoint.is_local || (slots.is_empty() && entry.is_none()) {
                    Ok(None)
                } else if entry.is_none() {
                    Err(DiskError::DiskNotFound)
                } else {
                    match LocalDiskRegistration::new(endpoint, previous.clone(), &slots) {
                        Ok(registration) => Ok(Some(registration)),
                        Err(_) if slots.is_empty() => Ok(None),
                        Err(error) => Err(error.narrow_to_disk().unwrap_or_else(DiskError::other)),
                    }
                };
                (endpoint.clone(), previous, registration)
            })
            .collect::<Vec<_>>()
    };
    let mut prepared = stream::iter(snapshots.into_iter().enumerate())
        .map(|(index, (endpoint, previous, registration))| async move {
            let result = if endpoint.is_local {
                prepare_local_disk(instance_ctx, &endpoint, previous, opt).await
            } else {
                crate::disk::new_disk_for_instance(instance_ctx, &endpoint, opt).await
            };
            (
                index,
                PreparedDisk {
                    endpoint,
                    result,
                    registration,
                },
            )
        })
        .buffer_unordered(LOCAL_DISK_OPEN_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    prepared.sort_unstable_by_key(|(index, _)| *index);
    prepared.into_iter().map(|(_, disk)| disk).collect()
}

pub(crate) fn same_local_disk(left: Option<&DiskStore>, right: Option<&DiskStore>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

/// Publish a prepared batch while its caller holds the registration lock.
/// Lock order is registration, set layout, then endpoint map. All validation
/// precedes mutation, and displaced handles are dropped outside inventory locks.
pub(crate) async fn publish_local_disks(
    instance_ctx: &InstanceContext,
    prepared: Vec<PreparedLocalDisk>,
) -> Result<Vec<Option<DiskStore>>> {
    if prepared.is_empty() {
        return Ok(Vec::new());
    }
    let set_drives = instance_ctx.local_disk_set_drives();
    let map = instance_ctx.local_disk_map();
    let mut slots = set_drives.write().await;
    let mut disks = map.write().await;
    let mut accepted = Vec::with_capacity(prepared.len());
    for prepared in &prepared {
        let registration = &prepared.registration;
        let key = registration.endpoint.to_string();
        let current = disks.get(&key);
        if let Some(slot) = registration.slot {
            slot.get(&slots, &registration.endpoint)?;
        } else if !slots.is_empty() {
            // A path-only token cannot cross a concurrent topology installation.
            return Err(DiskError::InconsistentDisk.into());
        }
        if current
            .and_then(Option::as_ref)
            .is_some_and(|disk| disk.endpoint() != registration.endpoint)
        {
            return Err(DiskError::InconsistentDisk.into());
        }
        accepted.push(
            (current.is_some() || registration.slot.is_none())
                && same_local_disk(current.and_then(Option::as_ref), registration.expected.as_ref()),
        );
    }
    let mut published = Vec::with_capacity(prepared.len());
    let mut displaced = Vec::new();
    for (prepared, accept) in prepared.into_iter().zip(accepted) {
        if !accept {
            published.push(None);
            continue;
        }
        let registration = prepared.registration;
        if let Some(slot) = registration.slot
            && let Some(previous) = std::mem::replace(&mut slots[slot.pool][slot.set][slot.disk], prepared.disk.clone())
        {
            displaced.push(previous);
        }
        if let Some(Some(previous)) = disks.insert(registration.endpoint.to_string(), prepared.disk.clone()) {
            displaced.push(previous);
        }
        published.push(prepared.disk);
    }
    drop(disks);
    drop(slots);
    drop(displaced);
    Ok(published)
}

pub(crate) async fn initialize_local_disk_maps(
    instance_ctx: &Arc<InstanceContext>,
    endpoint_pools: EndpointServerPools,
    opt: &DiskOption,
) -> Result<()> {
    let _registration = instance_ctx.local_disk_registration_lock().lock().await;
    initialize_local_disk_maps_locked(instance_ctx, endpoint_pools, opt).await
}

/// Public store constructors and the foundation share one complete registry.
/// Install membership before the constructor opens disks so its per-disk error
/// and quorum decisions remain intact. Existing handles retain live staging.
pub(crate) async fn ensure_local_disk_maps(
    instance_ctx: &Arc<InstanceContext>,
    endpoint_pools: EndpointServerPools,
) -> Result<()> {
    let _registration = instance_ctx.local_disk_registration_lock().lock().await;
    {
        let set_drives = instance_ctx.local_disk_set_drives();
        let map = instance_ctx.local_disk_map();
        let slots = set_drives.read().await;
        let disks = map.read().await;
        if local_disk_layout_matches(&endpoint_pools, &slots, &disks) {
            return Ok(());
        }
    }
    instance_ctx.bind_multipart_proof_topology(&endpoint_pools)?;
    let mut next_slots = local_disk_layout(&endpoint_pools);
    let mut next_disks = HashMap::new();
    let map = instance_ctx.local_disk_map();
    let registered = map.read().await;
    for endpoint in endpoint_pools
        .as_ref()
        .iter()
        .flat_map(|pool| pool.endpoints.as_ref())
        .filter(|endpoint| endpoint.is_local)
    {
        let slot = LocalDiskSlot::from_endpoint(endpoint)?;
        slot.get(&next_slots, endpoint)?;
        let disk = registered
            .get(&endpoint.to_string())
            .cloned()
            .flatten()
            .filter(|disk| disk.endpoint() == *endpoint);
        next_slots[slot.pool][slot.set][slot.disk] = disk.clone();
        next_disks.insert(endpoint.to_string(), disk);
    }
    drop(registered);
    replace_local_disk_inventory(instance_ctx, next_slots, next_disks).await;
    Ok(())
}

fn local_disk_layout_matches(
    endpoint_pools: &EndpointServerPools,
    slots: &[Vec<Vec<Option<DiskStore>>>],
    disks: &HashMap<String, Option<DiskStore>>,
) -> bool {
    let local_endpoint_count = endpoint_pools
        .as_ref()
        .iter()
        .flat_map(|pool| pool.endpoints.as_ref())
        .filter(|endpoint| endpoint.is_local)
        .count();
    if slots.len() != endpoint_pools.as_ref().len() || disks.len() != local_endpoint_count {
        return false;
    }
    for (pool, sets) in endpoint_pools.as_ref().iter().zip(slots) {
        if sets.len() != pool.set_count || sets.iter().any(|set| set.len() != pool.drives_per_set) {
            return false;
        }
        for endpoint in pool.endpoints.as_ref().iter().filter(|endpoint| endpoint.is_local) {
            let Ok(slot) = LocalDiskSlot::from_endpoint(endpoint) else {
                return false;
            };
            if slot.get(slots, endpoint).is_err() {
                return false;
            }
            let Some(registered) = disks.get(&endpoint.to_string()) else {
                return false;
            };
            if registered.as_ref().is_some_and(|disk| disk.endpoint() != *endpoint) {
                return false;
            }
        }
    }
    true
}

fn local_disk_layout(endpoint_pools: &EndpointServerPools) -> TypeLocalDiskSetDrives {
    endpoint_pools
        .as_ref()
        .iter()
        .map(|pool| vec![vec![None; pool.drives_per_set]; pool.set_count])
        .collect()
}

/// Replace both projections while the caller holds the registration lock.
async fn replace_local_disk_inventory(
    instance_ctx: &InstanceContext,
    next_slots: TypeLocalDiskSetDrives,
    next_disks: HashMap<String, Option<DiskStore>>,
) {
    // Publication order is registration, set layout, then endpoint map.
    let set_drives = instance_ctx.local_disk_set_drives();
    let map = instance_ctx.local_disk_map();
    let mut slots = set_drives.write().await;
    let mut disks = map.write().await;
    let previous_slots = std::mem::replace(&mut *slots, next_slots);
    let previous_disks = std::mem::replace(&mut *disks, next_disks);
    drop(disks);
    drop(slots);
    drop(previous_disks);
    drop(previous_slots);
}

async fn initialize_local_disk_maps_locked(
    instance_ctx: &Arc<InstanceContext>,
    endpoint_pools: EndpointServerPools,
    opt: &DiskOption,
) -> Result<()> {
    instance_ctx.bind_multipart_proof_topology(&endpoint_pools)?;
    let endpoints = endpoint_pools
        .as_ref()
        .iter()
        .flat_map(|pool| pool.endpoints.as_ref().iter())
        .filter(|endpoint| endpoint.is_local)
        .cloned()
        .collect::<Vec<_>>();
    let prepared = prepare_disks(instance_ctx, &endpoints, opt).await;
    let mut next_slots = local_disk_layout(&endpoint_pools);
    let mut next_disks = HashMap::with_capacity(prepared.len());
    for prepared in prepared {
        // The complete target topology grants membership independently of the
        // previous registry, which may belong to a different topology.
        let disk = local_disk_or_offline(prepared.result)?;
        let endpoint = prepared.endpoint;
        let slot = LocalDiskSlot::from_endpoint(&endpoint)?;
        slot.get(&next_slots, &endpoint)?;
        next_slots[slot.pool][slot.set][slot.disk] = disk.clone();
        next_disks.insert(endpoint.to_string(), disk);
    }
    let configured_disks = next_disks.len();
    let offline_disks = next_disks.values().filter(|disk| disk.is_none()).count();

    // Keep the existing snapshot readable during disk I/O. The replacement is
    // published only after the complete validated topology has been prepared.
    replace_local_disk_inventory(instance_ctx, next_slots, next_disks).await;

    if offline_disks > 0 {
        tracing::warn!(
            event = EVENT_LOCAL_DISK_REGISTRATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_RUNTIME_SOURCES,
            state = "degraded",
            configured_disks,
            offline_disks,
            "Local disk registration completed with unavailable slots"
        );
    }
    Ok(())
}

pub(crate) async fn init_tier_config_mgr(store: Arc<ECStore>) -> Result<()> {
    init_tier_config_mgr_handle(get_global_tier_config_mgr(), store).await
}

pub(crate) async fn init_tier_config_mgr_handle(handle: Arc<RwLock<TierConfigMgr>>, store: Arc<ECStore>) -> Result<()> {
    let initial_reload = TierConfigMgr::reload_handle(&handle, store.clone()).await;
    // Startup may continue after a transient recovery failure. Its retained
    // mutation fences still need a worker to consume later commit notifications.
    tokio::spawn(TierConfigMgr::refresh_tier_config_handle(handle.clone(), store));
    if initial_reload.is_err() {
        TierConfigMgr::request_committed_mutation_refresh(&handle).await;
    }
    initial_reload?;
    Ok(())
}

#[cfg(test)]
#[path = "local_disk_startup_tests.rs"]
mod local_disk_startup_tests;

#[cfg(test)]
mod tests {
    use super::{
        LockRegistry, ScannerDirtyUsageMutationSource, clear_local_disk_id_map_for_test, local_disk_path_by_id, local_node_name,
        notify_scanner_dirty_usage_mutation, reconcile_local_disk_ids, replace_local_disk_id, set_local_node_name,
        set_scanner_dirty_usage_mutation_observer,
    };
    use crate::{
        disk::{DiskAPI, DiskOption, endpoint::Endpoint},
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
        runtime::instance::InstanceContext,
    };
    use rustfs_lock::{LocalClient, LockClient};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use uuid::Uuid;

    fn url_endpoint(raw: &str) -> Endpoint {
        Endpoint {
            url: url::Url::parse(raw).expect("test endpoint url"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        }
    }

    fn distributed_startup_pools(directory: &std::path::Path) -> EndpointServerPools {
        let mut endpoints = Vec::with_capacity(16);
        for disk_index in 0..16 {
            let mut endpoint = if disk_index < 4 {
                let path = directory.join(format!("disk-{disk_index}"));
                Endpoint::try_from(path.to_str().expect("startup disk path should be UTF-8"))
                    .expect("local startup endpoint should parse")
            } else {
                url_endpoint(&format!("http://node-{}.invalid:9000/data/disk-{}", disk_index / 4, disk_index % 4))
            };
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
        }
        EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 16,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "degraded-startup-test".to_string(),
            platform: "test".to_string(),
        }])
    }

    #[tokio::test]
    async fn prepared_disks_preserve_endpoint_order_and_original_raw_errors() {
        let directory = tempfile::tempdir().expect("ordered disk preparation fixture");
        let roots = [
            directory.path().join("healthy"),
            directory.path().join("missing"),
            directory.path().join("corrupt"),
        ];
        std::fs::create_dir(&roots[0]).expect("healthy local root");
        let metadata = roots[2].join(crate::disk::RUSTFS_META_BUCKET);
        std::fs::create_dir_all(&metadata).expect("corrupt local root");
        std::fs::write(
            metadata.join(crate::disk::FORMAT_CONFIG_FILE),
            crate::disk::format::FormatV3::new(1, 1)
                .to_json()
                .expect("nil disk identity format"),
        )
        .expect("persist invalid disk identity");
        let endpoints = roots
            .iter()
            .map(|path| Endpoint::try_from(path.to_str().expect("UTF-8 raw endpoint")).expect("raw endpoint"))
            .collect::<Vec<_>>();
        let context = InstanceContext::new();
        let _registration = context.local_disk_registration_lock().lock().await;

        let prepared = super::prepare_disks(&context, &endpoints, &DiskOption::default()).await;

        assert_eq!(prepared.len(), endpoints.len());
        for (disk, endpoint) in prepared.iter().zip(&endpoints) {
            assert_eq!(&disk.endpoint, endpoint);
            assert!(
                matches!(disk.registration, Ok(None)),
                "raw preparation must not create registry membership"
            );
        }
        assert!(prepared[0].result.is_ok(), "healthy raw endpoint opens");
        assert!(matches!(prepared[1].result, Err(crate::disk::error::DiskError::DiskNotFound)));
        assert!(matches!(prepared[2].result, Err(crate::disk::error::DiskError::CorruptedFormat)));
        assert!(context.local_disk_map().read().await.is_empty());
        assert!(context.local_disk_set_drives().read().await.is_empty());
    }

    #[tokio::test]
    async fn ensuring_local_disk_layout_accepts_a_new_complete_topology_on_the_same_context() {
        let directory = tempfile::tempdir().expect("constructor topology replacement fixture");
        let topology = |label: &str, count: usize| {
            let endpoints = (0..count)
                .map(|index| {
                    let name = if index == 0 {
                        "shared".to_string()
                    } else {
                        format!("{label}-{index}")
                    };
                    let root = directory.path().join(name);
                    let mut endpoint = Endpoint::try_from(root.to_str().expect("UTF-8 endpoint")).expect("local endpoint");
                    endpoint.set_pool_index(0);
                    endpoint.set_set_index(0);
                    endpoint.set_disk_index(index);
                    endpoint
                })
                .collect::<Vec<_>>();
            EndpointServerPools::from(vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: count,
                endpoints: endpoints.into(),
                cmd_line: label.to_string(),
                platform: String::new(),
            }])
        };
        let previous = topology("previous", 2);
        for endpoint in previous.0[0].endpoints.as_ref() {
            std::fs::create_dir(endpoint.get_file_path()).expect("previous disk root");
        }
        let context = Arc::new(InstanceContext::new());
        super::initialize_local_disk_maps(&context, previous.clone(), &DiskOption::default())
            .await
            .expect("previous topology");
        let shared = context.local_disk_map().read().await[&previous.0[0].endpoints.as_ref()[0].to_string()]
            .clone()
            .expect("shared registered handle");
        let next = topology("next", 4);

        super::ensure_local_disk_maps(&context, next.clone())
            .await
            .expect("a complete constructor topology can replace the previous membership");

        let disks = context.local_disk_map();
        let disks = disks.read().await;
        assert_eq!(disks.len(), 4);
        assert!(!disks.contains_key(&previous.0[0].endpoints.as_ref()[1].to_string()));
        assert!(Arc::ptr_eq(
            disks[&next.0[0].endpoints.as_ref()[0].to_string()]
                .as_ref()
                .expect("shared route"),
            &shared,
        ));
        for endpoint in next.0[0].endpoints.as_ref().iter().skip(1) {
            assert!(disks[&endpoint.to_string()].is_none());
            assert!(
                !std::path::Path::new(&endpoint.get_file_path()).exists(),
                "membership installation performs no disk I/O"
            );
        }
        drop(disks);
        let slots = context.local_disk_set_drives();
        let slots = slots.read().await;
        assert_eq!((slots.len(), slots[0].len(), slots[0][0].len()), (1, 1, 4));
        assert!(Arc::ptr_eq(slots[0][0][0].as_ref().expect("shared topology slot"), &shared));
        assert!(slots[0][0][1..].iter().all(Option::is_none));
    }

    #[tokio::test]
    async fn unknown_member_snapshot_cannot_gain_authority_from_later_offline_registration() {
        let directory = tempfile::tempdir().expect("unknown member snapshot fixture");
        let previous = distributed_startup_pools(&directory.path().join("previous"));
        let next = distributed_startup_pools(&directory.path().join("next"));
        let endpoint = &next.0[0].endpoints.as_ref()[0];
        let context = Arc::new(InstanceContext::new());
        super::ensure_local_disk_maps(&context, previous)
            .await
            .expect("configured offline topology");

        let error = super::snapshot_local_disk_registration(&context, endpoint)
            .await
            .err()
            .expect("an unknown configured member has no publication token");
        assert!(matches!(error.narrow_to_disk(), Ok(crate::disk::error::DiskError::DiskNotFound)));
        let prepared = {
            let _registration = context.local_disk_registration_lock().lock().await;
            super::prepare_disks(&context, std::slice::from_ref(endpoint), &DiskOption::default()).await
        };
        assert!(matches!(prepared[0].registration, Err(crate::disk::error::DiskError::DiskNotFound)));

        super::ensure_local_disk_maps(&context, next.clone())
            .await
            .expect("a later authoritative topology may register the member offline");

        assert!(context.local_disk_map().read().await[&endpoint.to_string()].is_none());
        for prepared in prepared {
            let error = prepared
                .into_local_registration()
                .err()
                .expect("a stale unknown-member snapshot remains unauthorized");
            assert!(matches!(error.narrow_to_disk(), Ok(crate::disk::error::DiskError::DiskNotFound)));
        }
        super::snapshot_local_disk_registration(&context, endpoint)
            .await
            .expect("only a new snapshot can observe the later membership");
    }

    #[tokio::test]
    async fn ensuring_local_disk_layout_defers_physical_errors_to_the_opener() {
        let directory = tempfile::tempdir().expect("direct constructor membership fixture");
        let mut endpoints = Vec::new();
        for index in 0..2 {
            let root = directory.path().join(format!("disk-{index}"));
            std::fs::create_dir(&root).expect("local root fixture");
            let mut endpoint = Endpoint::try_from(root.to_str().expect("UTF-8 local endpoint")).expect("local endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(index);
            endpoints.push(endpoint);
        }
        let metadata = directory.path().join("disk-0").join(crate::disk::RUSTFS_META_BUCKET);
        std::fs::create_dir(&metadata).expect("corrupt disk metadata fixture");
        std::fs::write(metadata.join(crate::disk::FORMAT_CONFIG_FILE), b"invalid-format").expect("corrupt format fixture");
        let pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: endpoints.clone().into(),
            cmd_line: "direct-constructor-membership".to_string(),
            platform: String::new(),
        }]);
        let context = Arc::new(InstanceContext::new());

        super::ensure_local_disk_maps(&context, pools)
            .await
            .expect("membership installation must preserve later per-disk error aggregation");

        assert_eq!(context.local_disk_map().read().await.len(), 2);
        assert!(context.local_disk_set_drives().read().await[0][0].iter().all(Option::is_none));
        assert!(!directory.path().join("disk-1").join(crate::disk::RUSTFS_META_BUCKET).exists());
        let (disks, errors) = crate::store::init_format::init_disks(&context, &endpoints.into(), &DiskOption::default()).await;
        assert!(disks[0].is_none());
        assert!(errors[0].is_some(), "corrupt disk keeps its own open error");
        assert!(disks[1].is_some(), "healthy disk remains available to the quorum loader");
        assert!(errors[1].is_none());
    }

    #[tokio::test]
    async fn initializing_local_disk_maps_preserves_missing_drive_slots() {
        for missing_index in 0..4 {
            let directory = tempfile::tempdir().expect("startup test directory should be created");
            let pools = distributed_startup_pools(directory.path());
            let endpoints = pools.0[0].endpoints.as_ref();
            for (index, endpoint) in endpoints.iter().take(4).enumerate() {
                if index != missing_index {
                    std::fs::create_dir(endpoint.get_file_path()).expect("healthy startup disk should be created");
                }
            }
            let context = Arc::new(InstanceContext::new());
            super::initialize_local_disk_maps(&context, pools.clone(), &DiskOption::default())
                .await
                .expect("one missing local drive must not abort disk registration");

            let entries = context.local_disk_map();
            let entries = entries.read().await;
            assert_eq!(entries.len(), 4, "every local endpoint must remain registered");
            let slots = context.local_disk_set_drives();
            let slots = slots.read().await;
            assert_eq!((slots.len(), slots[0].len(), slots[0][0].len()), (1, 1, 16));
            for (index, endpoint) in endpoints.iter().take(4).enumerate() {
                let entry = entries
                    .get(&endpoint.to_string())
                    .expect("local endpoint must retain its registration");
                let slot = &slots[0][0][index];
                if index == missing_index {
                    assert!(entry.is_none(), "the missing drive must remain unavailable");
                    assert!(slot.is_none(), "the missing drive must retain its original slot");
                    assert!(
                        !std::path::Path::new(&endpoint.get_file_path()).exists(),
                        "registration must not recreate a missing drive root"
                    );
                } else {
                    let disk = entry.as_ref().expect("healthy local drive should be registered");
                    assert_eq!(disk.endpoint(), *endpoint);
                    assert!(Arc::ptr_eq(disk, slot.as_ref().expect("healthy slot should be populated")));
                }
            }
            assert!(slots[0][0][4..].iter().all(Option::is_none));
        }
    }

    #[tokio::test]
    async fn initializing_local_disk_maps_allows_all_local_drives_to_be_missing() {
        let directory = tempfile::tempdir().expect("startup test directory should be created");
        let pools = distributed_startup_pools(directory.path());
        let context = Arc::new(InstanceContext::new());

        super::initialize_local_disk_maps(&context, pools.clone(), &DiskOption::default())
            .await
            .expect("missing local drives must allow subsequent cluster quorum evaluation");

        let entries = context.local_disk_map();
        let entries = entries.read().await;
        assert_eq!(entries.len(), 4);
        for endpoint in pools.0[0].endpoints.as_ref().iter().take(4) {
            assert!(
                entries
                    .get(&endpoint.to_string())
                    .expect("missing local endpoint must retain its registration")
                    .is_none()
            );
        }
        let slots = context.local_disk_set_drives();
        let slots = slots.read().await;
        assert_eq!((slots.len(), slots[0].len(), slots[0][0].len()), (1, 1, 16));
        assert!(slots[0][0].iter().all(Option::is_none));
        assert_eq!(std::fs::read_dir(directory.path()).expect("list startup directory").count(), 0);
    }

    #[tokio::test]
    async fn reinitializing_local_disk_maps_registers_a_returned_drive() {
        let directory = tempfile::tempdir().expect("startup test directory should be created");
        let pools = distributed_startup_pools(directory.path());
        let endpoints = pools.0[0].endpoints.as_ref();
        for (index, endpoint) in endpoints.iter().take(4).enumerate() {
            if index != 1 {
                std::fs::create_dir(endpoint.get_file_path()).expect("healthy startup disk should be created");
            }
        }
        let context = Arc::new(InstanceContext::new());
        super::initialize_local_disk_maps(&context, pools.clone(), &DiskOption::default())
            .await
            .expect("missing local drive must allow registration");
        assert!(
            context
                .local_disk_map()
                .read()
                .await
                .get(&endpoints[1].to_string())
                .expect("missing endpoint must retain its registration")
                .is_none()
        );

        std::fs::create_dir(endpoints[1].get_file_path()).expect("returned disk root should be created");
        super::initialize_local_disk_maps(&context, pools.clone(), &DiskOption::default())
            .await
            .expect("returned drive must register in the original topology");

        let entries = context.local_disk_map();
        let entries = entries.read().await;
        assert_eq!(entries.len(), 4);
        let slots = context.local_disk_set_drives();
        let slots = slots.read().await;
        assert_eq!((slots.len(), slots[0].len(), slots[0][0].len()), (1, 1, 16));
        for (index, endpoint) in endpoints.iter().take(4).enumerate() {
            let disk = entries
                .get(&endpoint.to_string())
                .expect("original endpoint must remain registered")
                .as_ref()
                .expect("returned and healthy drives should be online");
            assert_eq!(disk.endpoint(), *endpoint);
            assert!(Arc::ptr_eq(disk, slots[0][0][index].as_ref().expect("original slot must be populated")));
        }
    }

    #[tokio::test]
    async fn initializing_local_disk_maps_rejects_corrupt_format() {
        let directory = tempfile::tempdir().expect("startup test directory should be created");
        let pools = distributed_startup_pools(directory.path());
        for endpoint in pools.0[0].endpoints.as_ref().iter().take(4) {
            std::fs::create_dir(endpoint.get_file_path()).expect("startup disk should be created");
        }
        let metadata = directory.path().join("disk-1/.rustfs.sys");
        std::fs::create_dir(&metadata).expect("metadata directory should be created");
        let format_path = metadata.join("format.json");
        std::fs::write(&format_path, b"invalid-format").expect("corrupt format should be written");
        let context = Arc::new(InstanceContext::new());

        super::initialize_local_disk_maps(&context, pools, &DiskOption::default())
            .await
            .expect_err("corrupt format must remain a startup error");

        assert_eq!(
            std::fs::read(&format_path).expect("corrupt format should remain readable"),
            b"invalid-format",
            "disk registration must preserve the corrupt format for diagnosis"
        );
    }

    #[tokio::test]
    async fn initializing_local_disk_maps_rejects_nil_format_identity() {
        let directory = tempfile::tempdir().expect("startup fixture");
        let pools = distributed_startup_pools(directory.path());
        let endpoint = &pools.0[0].endpoints.as_ref()[0];
        let meta = std::path::PathBuf::from(endpoint.get_file_path()).join(crate::disk::RUSTFS_META_BUCKET);
        std::fs::create_dir_all(&meta).expect("metadata fixture");
        let format = crate::disk::format::FormatV3::new(1, 16);
        let bytes = format.to_json().expect("format with nil disk identity");
        let path = meta.join(crate::disk::FORMAT_CONFIG_FILE);
        std::fs::write(&path, &bytes).expect("persist nil disk identity");
        let instance = Arc::new(InstanceContext::new());

        let error = super::initialize_local_disk_maps(&instance, pools, &DiskOption::default())
            .await
            .expect_err("a persisted nil identity is a fatal format error");

        assert_eq!(
            error.narrow_to_disk().expect("preserved disk error type"),
            crate::disk::error::DiskError::CorruptedFormat
        );
        assert_eq!(std::fs::read_to_string(path).expect("preserved format"), bytes);
    }

    #[test]
    fn lock_registry_selects_unique_clients_in_endpoint_order() {
        let client_a: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let client_b: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let registry = LockRegistry::new(HashMap::from([
            ("node-a:9000".to_string(), client_a.clone()),
            ("node-b:9000".to_string(), client_b.clone()),
        ]));
        let endpoints = vec![
            url_endpoint("http://node-a:9000/data-a"),
            url_endpoint("http://node-a:9000/data-b"),
            url_endpoint("http://node-missing:9000/data"),
            url_endpoint("http://node-b:9000/data"),
        ];

        let clients = registry.clients_for_endpoints(&endpoints);

        assert_eq!(clients.len(), 2);
        assert!(Arc::ptr_eq(&clients[0], &client_a));
        assert!(Arc::ptr_eq(&clients[1], &client_b));
    }

    #[test]
    #[serial_test::serial(scanner_dirty_usage_mutation_observer)]
    fn scanner_dirty_usage_mutation_observer_filters_empty_identity_and_preserves_source() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_clone = Arc::clone(&observed);
        let previous = set_scanner_dirty_usage_mutation_observer(Some(Arc::new(move |bucket, object, source| {
            observed_clone.lock().expect("observer lock should not be poisoned").push((
                bucket.to_string(),
                object.to_string(),
                source,
            ));
        })));

        notify_scanner_dirty_usage_mutation("photos", "2026/image.jpg", ScannerDirtyUsageMutationSource::Replication);
        notify_scanner_dirty_usage_mutation("", "2026/empty-bucket.jpg", ScannerDirtyUsageMutationSource::TierExpiration);
        notify_scanner_dirty_usage_mutation("photos", "", ScannerDirtyUsageMutationSource::TierExpiration);
        notify_scanner_dirty_usage_mutation("archive", "expired.bin", ScannerDirtyUsageMutationSource::TierExpiration);
        set_scanner_dirty_usage_mutation_observer(previous);

        assert_eq!(
            *observed.lock().expect("observer lock should not be poisoned"),
            vec![
                (
                    "photos".to_string(),
                    "2026/image.jpg".to_string(),
                    ScannerDirtyUsageMutationSource::Replication
                ),
                (
                    "archive".to_string(),
                    "expired.bin".to_string(),
                    ScannerDirtyUsageMutationSource::TierExpiration
                ),
            ]
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_node_name_round_trips_through_common_runtime_helper() {
        let previous = local_node_name().await;
        let next = "runtime-source-local-node-test".to_string();

        set_local_node_name(next.clone()).await;
        let observed = local_node_name().await;
        set_local_node_name(previous).await;

        assert_eq!(observed, next);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn clearing_a_stale_disk_id_does_not_remove_another_endpoint() {
        clear_local_disk_id_map_for_test().await;
        let disk_id = Uuid::new_v4();
        replace_local_disk_id(None, Some(disk_id), "endpoint-a".to_string()).await;

        replace_local_disk_id(Some(disk_id), None, "endpoint-b".to_string()).await;

        assert_eq!(local_disk_path_by_id(&disk_id).await, Some("endpoint-a".to_string()));
        clear_local_disk_id_map_for_test().await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reconciling_pool_disk_ids_preserves_other_endpoints() {
        let instance_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        let process_ctx = crate::runtime::global::current_ctx();
        let bootstrap_ctx = crate::runtime::instance::bootstrap_ctx();
        let retained_id = Uuid::new_v4();
        let removed_id = Uuid::new_v4();
        let selected_id = Uuid::new_v4();
        let process_sentinel = Uuid::new_v4();
        let bootstrap_sentinel = Uuid::new_v4();
        instance_ctx.local_disk_id_map().write().await.extend([
            (retained_id, "endpoint-a".to_string()),
            (removed_id, "endpoint-b".to_string()),
        ]);
        process_ctx
            .local_disk_id_map()
            .write()
            .await
            .insert(process_sentinel, "endpoint-b".to_string());
        bootstrap_ctx
            .local_disk_id_map()
            .write()
            .await
            .insert(bootstrap_sentinel, "endpoint-b".to_string());

        reconcile_local_disk_ids(
            &instance_ctx,
            &["endpoint-b".to_string(), "endpoint-c".to_string()],
            &[(selected_id, "endpoint-c".to_string())],
        )
        .await;

        let disk_ids = instance_ctx.local_disk_id_map();
        let disk_ids = disk_ids.read().await;
        assert_eq!(disk_ids.get(&retained_id).map(String::as_str), Some("endpoint-a"));
        assert_eq!(disk_ids.get(&removed_id), None);
        assert_eq!(disk_ids.get(&selected_id).map(String::as_str), Some("endpoint-c"));
        drop(disk_ids);
        assert_eq!(
            process_ctx
                .local_disk_id_map()
                .read()
                .await
                .get(&process_sentinel)
                .map(String::as_str),
            Some("endpoint-b")
        );
        assert_eq!(
            bootstrap_ctx
                .local_disk_id_map()
                .read()
                .await
                .get(&bootstrap_sentinel)
                .map(String::as_str),
            Some("endpoint-b")
        );
        process_ctx.local_disk_id_map().write().await.remove(&process_sentinel);
        bootstrap_ctx.local_disk_id_map().write().await.remove(&bootstrap_sentinel);
    }

    /// Re-initializing the same context with a WIDER topology must replace the
    /// previous registration, not append to it: the stale pool-0 drive vector
    /// (sized for the narrow topology) made `global_set_drives[0][0][disk_idx]`
    /// panic for the wider set's higher disk indices. CI's nextest
    /// process-per-test isolation never exercises re-init, so this pins it.
    #[tokio::test]
    async fn reinitializing_local_disk_maps_replaces_previous_topology() {
        use crate::disk::DiskOption;
        use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};

        let temp_dir = tempfile::tempdir().expect("reinit test directory should be created");
        let build_pools = |label: &str, disk_count: usize| {
            let mut endpoints = Vec::new();
            for disk_idx in 0..disk_count {
                let disk_path = temp_dir.path().join(format!("{label}-disk{disk_idx}"));
                std::fs::create_dir_all(&disk_path).expect("reinit test disk should be created");
                let mut endpoint =
                    Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                endpoint.set_pool_index(0);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_idx);
                endpoints.push(endpoint);
            }
            EndpointServerPools(vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: disk_count,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("reinit-test-{label}"),
                platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
            }])
        };
        let opt = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let instance_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        super::initialize_local_disk_maps(&instance_ctx, build_pools("narrow", 2), &opt)
            .await
            .expect("narrow topology should initialize");
        let first_authority = instance_ctx.multipart_proof_verification_context();
        super::initialize_local_disk_maps(&instance_ctx, build_pools("wide", 4), &opt)
            .await
            .expect("re-initializing with a wider topology must not panic or fail");
        assert!(first_authority.same_authority(&instance_ctx.multipart_proof_verification_context()));

        let set_drives = instance_ctx.local_disk_set_drives();
        let set_drives = set_drives.read().await;
        assert_eq!(set_drives.len(), 1, "stale pools must not accumulate across re-inits");
        assert_eq!(set_drives[0][0].len(), 4, "pool 0 set 0 must be sized for the new topology");
        assert!(
            set_drives[0][0].iter().all(Option::is_some),
            "every wide-topology drive slot must be registered"
        );
        drop(set_drives);

        let disk_map = instance_ctx.local_disk_map();
        let disk_map = disk_map.read().await;
        assert_eq!(disk_map.len(), 4, "stale narrow-topology disk entries must be dropped");
        assert!(
            disk_map.keys().all(|path| path.contains("wide-disk")),
            "only the new topology's disks may remain registered: {:?}",
            disk_map.keys().collect::<Vec<_>>()
        );
    }
    #[tokio::test]
    async fn multipart_invalid_registry_pool_index_has_no_authority_or_registry_effect() {
        use crate::disk::DiskOption;
        use crate::layout::endpoints::{EndpointServerPools, PoolEndpoints};
        let directory = tempfile::tempdir().unwrap();
        let make_topology = |label: &str, pool_index: usize| {
            let path = directory.path().join(label);
            std::fs::create_dir_all(&path).unwrap();
            let mut endpoint = Endpoint::try_from(path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(pool_index);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(0);
            EndpointServerPools(vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: vec![endpoint].into(),
                cmd_line: label.into(),
                platform: "test".into(),
            }])
        };
        let ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
        let valid = make_topology("registered", 0);
        let options = DiskOption {
            cleanup: false,
            health_check: false,
        };
        super::initialize_local_disk_maps(&ctx, valid.clone(), &options)
            .await
            .unwrap();
        let authority = ctx.multipart_proof_verification_context();
        let before = ctx.local_disk_map().read().await.clone();
        let invalid = make_topology("invalid", 1);
        let endpoint = &invalid.0[0].endpoints.as_ref()[0];
        assert!(
            super::initialize_local_disk_maps(&ctx, invalid.clone(), &options)
                .await
                .is_err()
        );
        assert!(authority.same_authority(&ctx.multipart_proof_verification_context()));
        assert!(authority.validate_endpoint(endpoint).is_err());
        assert!(
            !std::path::Path::new(&endpoint.get_file_path()).join(".rustfs.sys").exists(),
            "invalid registration must fail before disk initialization"
        );
        let after = ctx.local_disk_map().read().await.clone();
        assert_eq!(before.len(), after.len());
        for (path, disk) in before {
            assert!(Arc::ptr_eq(disk.as_ref().unwrap(), after.get(&path).unwrap().as_ref().unwrap()));
        }
        let drives = ctx.local_disk_set_drives();
        let drives = drives.read().await;
        assert_eq!((drives.len(), drives[0].len(), drives[0][0].len()), (1, 1, 1));
        assert_eq!(drives[0][0][0].as_ref().unwrap().endpoint(), valid.0[0].endpoints.as_ref()[0]);
    }
}
