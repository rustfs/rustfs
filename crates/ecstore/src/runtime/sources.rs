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

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
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
    disk::{DiskAPI, DiskOption, DiskStore, new_disk},
    error::{Error, Result},
    layout::endpoints::{EndpointServerPools, SetupType},
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
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_lock::client::LockClient;
use s3s::dto::BucketLifecycleConfiguration;
use s3s::region::Region;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use uuid::Uuid;

#[cfg(test)]
const TEST_RPC_SECRET: &str = "test-rpc-secret";

pub(crate) type WorkloadSnapshotProviderRef = Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>;

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

pub(crate) fn set_workload_admission_snapshot_provider(
    provider: WorkloadSnapshotProviderRef,
) -> std::result::Result<(), WorkloadSnapshotProviderRef> {
    WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER.set(provider)
}

pub(crate) fn workload_admission_snapshot_provider() -> Option<WorkloadSnapshotProviderRef> {
    WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER.get().cloned()
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

pub(crate) fn endpoint_pool_is_local(pool_index: usize) -> bool {
    get_global_endpoints()
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

pub(crate) async fn cached_node_channel(addr: &str) -> Option<Channel> {
    rustfs_common::cached_connection(addr).await
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

pub(crate) async fn bucket_lifecycle_config(bucket: &str) -> Option<BucketLifecycleConfiguration> {
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

pub(crate) fn local_disk_set_drives_handle() -> Arc<RwLock<TypeLocalDiskSetDrives>> {
    crate::runtime::global::current_ctx().local_disk_set_drives()
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
    let slots = endpoints
        .iter()
        .map(|endpoint| {
            Ok((
                usize::try_from(endpoint.pool_idx).map_err(|_| Error::CorruptedFormat)?,
                usize::try_from(endpoint.set_idx).map_err(|_| Error::CorruptedFormat)?,
                usize::try_from(endpoint.disk_idx).map_err(|_| Error::CorruptedFormat)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let local_disk_map = instance_ctx.local_disk_map();
    let mut local_disks = local_disk_map.write().await;
    for endpoint in endpoints {
        local_disks.insert(endpoint.to_string(), None);
    }
    drop(local_disks);

    let set_drives = instance_ctx.local_disk_set_drives();
    let mut local_set_drives = set_drives.write().await;
    if local_set_drives.is_empty() {
        return Ok(());
    }
    for (pool_idx, set_idx, disk_idx) in slots {
        let disk = local_set_drives
            .get_mut(pool_idx)
            .and_then(|sets| sets.get_mut(set_idx))
            .and_then(|disks| disks.get_mut(disk_idx))
            .ok_or(Error::CorruptedFormat)?;
        *disk = None;
    }
    Ok(())
}

pub(crate) async fn record_local_disks(instance_ctx: &Arc<InstanceContext>, disks: Vec<DiskStore>) {
    let map = instance_ctx.local_disk_map();
    let mut global_local_disk_map = map.write().await;
    for disk in disks {
        let path = disk.endpoint().to_string();
        global_local_disk_map.insert(path, Some(disk.clone()));
    }
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

pub(crate) async fn initialize_local_disk_maps(
    instance_ctx: &Arc<InstanceContext>,
    endpoint_pools: EndpointServerPools,
    opt: &DiskOption,
) -> Result<()> {
    // Every caller passes the FULL topology, so (re)initialization must replace
    // any previous registration wholesale: appending would leave the pool/set
    // vectors sized for a stale topology and panic on wider disk indices (seen
    // as cross-test contamination under single-process `cargo test`).
    let set_drives = instance_ctx.local_disk_set_drives();
    let mut global_set_drives = set_drives.write().await;
    global_set_drives.clear();
    for pool_eps in endpoint_pools.as_ref().iter() {
        let mut set_count_drives = Vec::with_capacity(pool_eps.set_count);
        for _ in 0..pool_eps.set_count {
            set_count_drives.push(vec![None; pool_eps.drives_per_set]);
        }

        global_set_drives.push(set_count_drives);
    }

    let map = instance_ctx.local_disk_map();
    let mut global_local_disk_map = map.write().await;
    global_local_disk_map.clear();

    for pool_eps in endpoint_pools.as_ref().iter() {
        for ep in pool_eps.endpoints.as_ref().iter() {
            if !ep.is_local {
                continue;
            }

            let disk = new_disk(ep, opt).await?;
            let path = disk.endpoint().to_string();
            let pool_idx = usize::try_from(ep.pool_idx).map_err(|err| {
                crate::error::Error::other(format!("store init failed to convert pool index `{}`: {err}", ep.pool_idx))
            })?;
            let set_idx = usize::try_from(ep.set_idx).map_err(|err| {
                crate::error::Error::other(format!("store init failed to convert set index `{}`: {err}", ep.set_idx))
            })?;
            let disk_idx = usize::try_from(ep.disk_idx).map_err(|err| {
                crate::error::Error::other(format!("store init failed to convert disk index `{}`: {err}", ep.disk_idx))
            })?;

            global_local_disk_map.insert(path, Some(disk.clone()));
            global_set_drives[pool_idx][set_idx][disk_idx] = Some(disk.clone());
        }
    }

    Ok(())
}

pub(crate) async fn init_tier_config_mgr(store: Arc<ECStore>) -> Result<()> {
    let handle = get_global_tier_config_mgr();
    TierConfigMgr::reload_handle(&handle, store.clone()).await?;
    tokio::spawn(TierConfigMgr::refresh_tier_config_handle(handle, store));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        LockRegistry, clear_local_disk_id_map_for_test, local_disk_path_by_id, local_node_name, reconcile_local_disk_ids,
        replace_local_disk_id, set_local_node_name,
    };
    use crate::disk::endpoint::Endpoint;
    use rustfs_lock::{LocalClient, LockClient};
    use std::{collections::HashMap, sync::Arc};
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
        super::initialize_local_disk_maps(&instance_ctx, build_pools("wide", 4), &opt)
            .await
            .expect("re-initializing with a wider topology must not panic or fail");

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
}
