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

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use crate::bucket::bandwidth::monitor::Monitor;
use crate::disk::endpoint::Endpoint;
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::{ExpiryState, GLOBAL_ExpiryState, GLOBAL_TransitionState, TransitionState},
    bucket::metadata_sys::{BucketMetadataSys, get_global_bucket_metadata_sys},
    bucket::replication::{DynReplicationPool, GLOBAL_REPLICATION_POOL, GLOBAL_REPLICATION_STATS, ReplicationStats},
    config::{get_global_storage_class, set_global_storage_class, storageclass},
    disk::{DiskAPI, DiskOption, DiskStore, new_disk},
    endpoints::EndpointServerPools,
    error::Result,
    event_notification::EventNotifier,
    global::{
        GLOBAL_BOOT_TIME, GLOBAL_EventNotifier, GLOBAL_IsErasureSD, GLOBAL_LOCAL_DISK_ID_MAP, GLOBAL_LOCAL_DISK_MAP,
        GLOBAL_LOCAL_DISK_SET_DRIVES, GLOBAL_LifecycleSys, GLOBAL_LocalNodeName, GLOBAL_RootDiskThreshold, GLOBAL_TierConfigMgr,
        TypeLocalDiskSetDrives, get_global_bucket_monitor, get_global_deployment_id, get_global_endpoints,
        get_global_endpoints_opt, get_global_lock_clients, get_global_region, get_global_tier_config_mgr, global_rustfs_port,
        init_global_bucket_monitor, is_dist_erasure, is_erasure, is_first_cluster_node_local, resolve_object_store_handle,
        set_global_deployment_id, set_global_lock_client, set_global_lock_clients, set_object_layer,
    },
    notification_sys::{NotificationSys, get_global_notification_sys},
    store::ECStore,
    tier::tier::TierConfigMgr,
};
use rustfs_common::{GLOBAL_CONN_MAP, GLOBAL_LOCAL_NODE_NAME, GLOBAL_RUSTFS_ADDR, GLOBAL_RUSTFS_HOST};
use rustfs_config::server_config::{Config, get_global_server_config, set_global_server_config};
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_kms::{ObjectEncryptionService, get_global_encryption_service};
use rustfs_lock::client::LockClient;
use s3s::dto::BucketLifecycleConfiguration;
use s3s::region::Region;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use uuid::Uuid;

pub(crate) fn record_erasure_write_quorum_failure(stage: &'static str, dominant_error: &'static str) {
    global_internode_metrics().record_erasure_write_quorum_failure(stage, dominant_error);
}

pub(crate) async fn object_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    get_global_encryption_service().await
}

pub(crate) fn object_store_handle() -> Option<Arc<ECStore>> {
    resolve_object_store_handle()
}

pub(crate) fn endpoint_pools() -> Option<EndpointServerPools> {
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

pub(crate) async fn first_cluster_node_is_local() -> bool {
    is_first_cluster_node_local().await
}

pub(crate) async fn setup_is_erasure() -> bool {
    is_erasure().await
}

pub(crate) async fn setup_is_dist_erasure() -> bool {
    is_dist_erasure().await
}

pub(crate) async fn setup_is_erasure_sd() -> bool {
    *GLOBAL_IsErasureSD.read().await
}

pub(crate) async fn local_node_name() -> String {
    GLOBAL_LOCAL_NODE_NAME.read().await.clone()
}

pub(crate) async fn set_local_node_name(node_name: String) {
    *GLOBAL_LOCAL_NODE_NAME.write().await = node_name;
}

pub(crate) fn default_local_node_name() -> String {
    GLOBAL_LocalNodeName.to_string()
}

pub(crate) fn rustfs_port() -> u16 {
    global_rustfs_port()
}

pub(crate) async fn rustfs_host() -> String {
    GLOBAL_RUSTFS_HOST.read().await.clone()
}

pub(crate) async fn rustfs_addr() -> String {
    GLOBAL_RUSTFS_ADDR.read().await.clone()
}

pub(crate) fn boot_uptime_secs() -> u64 {
    GLOBAL_BOOT_TIME
        .get()
        .and_then(|boot_time| SystemTime::now().duration_since(*boot_time).ok())
        .unwrap_or_default()
        .as_secs()
}

pub(crate) async fn ensure_boot_time() {
    GLOBAL_BOOT_TIME.get_or_init(|| async { SystemTime::now() }).await;
}

pub(crate) async fn scanner_init_time() -> Option<chrono::DateTime<chrono::Utc>> {
    rustfs_common::get_global_init_time().await
}

pub(crate) async fn root_disk_threshold_for_erasure_disk() -> Option<u64> {
    if *GLOBAL_IsErasureSD.read().await {
        None
    } else {
        Some(*GLOBAL_RootDiskThreshold.read().await)
    }
}

pub(crate) async fn cached_node_channel(addr: &str) -> Option<Channel> {
    GLOBAL_CONN_MAP.read().await.get(addr).cloned()
}

pub(crate) fn storage_class_parity(storage_class: Option<&str>) -> Option<usize> {
    get_global_storage_class().and_then(|sc| sc.get_parity_for_sc(storage_class.unwrap_or_default()))
}

pub(crate) fn backend_storage_class_parities(default_standard_parity: usize) -> (Option<usize>, Option<usize>) {
    if let Some(sc) = get_global_storage_class() {
        let standard = sc
            .get_parity_for_sc(crate::config::storageclass::CLASS_STANDARD)
            .or(Some(default_standard_parity));
        let reduced_redundancy = sc.get_parity_for_sc(crate::config::storageclass::RRS);
        (standard, reduced_redundancy)
    } else {
        (Some(default_standard_parity), None)
    }
}

pub(crate) fn storage_class_should_inline(shard_size: i64, versioned: bool) -> bool {
    get_global_storage_class().is_some_and(|sc| sc.should_inline(shard_size, versioned))
}

pub(crate) fn deployment_upload_id(upload_id: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .encode_to_string(format!("{}.{}", get_global_deployment_id().unwrap_or_default(), upload_id).as_bytes())
}

pub(crate) fn deployment_id() -> Option<String> {
    get_global_deployment_id()
}

pub(crate) fn replication_pool() -> Option<Arc<DynReplicationPool>> {
    GLOBAL_REPLICATION_POOL.get().cloned()
}

pub(crate) fn replication_stats() -> Option<Arc<ReplicationStats>> {
    GLOBAL_REPLICATION_STATS.get().cloned()
}

pub(crate) fn replication_runtime_initialized() -> bool {
    GLOBAL_REPLICATION_STATS.get().is_some() && GLOBAL_REPLICATION_POOL.get().is_some()
}

pub(crate) fn ensure_deployment_id(deployment_id: Uuid) {
    if get_global_deployment_id().is_none() {
        set_global_deployment_id(deployment_id);
    }
}

pub(crate) fn global_lock_manager() -> Arc<rustfs_lock::GlobalLockManager> {
    rustfs_lock::get_global_lock_manager()
}

pub(crate) fn global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn LockClient>>> {
    get_global_lock_clients()
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

pub(crate) fn notification_sys() -> Option<&'static NotificationSys> {
    get_global_notification_sys()
}

pub(crate) fn bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    get_global_bucket_metadata_sys()
}

pub(crate) fn region() -> Option<Region> {
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

pub(crate) fn set_storage_class_config(config: storageclass::Config) {
    set_global_storage_class(config);
}

pub(crate) fn global_tier_config_mgr() -> Arc<RwLock<TierConfigMgr>> {
    get_global_tier_config_mgr()
}

pub(crate) async fn bucket_lifecycle_config(bucket: &str) -> Option<BucketLifecycleConfiguration> {
    GLOBAL_LifecycleSys.get(bucket).await
}

pub(crate) fn delete_bucket_monitor_entry(bucket: &str) {
    if let Some(monitor) = get_global_bucket_monitor() {
        monitor.delete_bucket(bucket);
    }
}

pub(crate) fn bucket_monitor() -> Option<Arc<Monitor>> {
    get_global_bucket_monitor()
}

pub(crate) fn init_bucket_monitor_for_current_endpoints() {
    let num_nodes = get_global_endpoints().get_nodes().len().try_into().unwrap_or(u64::MAX);
    init_global_bucket_monitor(num_nodes);
}

pub(crate) fn local_disk_map_handle() -> Arc<RwLock<HashMap<String, Option<DiskStore>>>> {
    GLOBAL_LOCAL_DISK_MAP.clone()
}

pub(crate) fn local_disk_id_map_handle() -> Arc<RwLock<HashMap<Uuid, String>>> {
    GLOBAL_LOCAL_DISK_ID_MAP.clone()
}

pub(crate) fn local_disk_set_drives_handle() -> Arc<RwLock<TypeLocalDiskSetDrives>> {
    GLOBAL_LOCAL_DISK_SET_DRIVES.clone()
}

pub(crate) fn tier_config_mgr_handle() -> Arc<RwLock<TierConfigMgr>> {
    GLOBAL_TierConfigMgr.clone()
}

pub(crate) fn expiry_state_handle() -> Arc<RwLock<ExpiryState>> {
    GLOBAL_ExpiryState.clone()
}

pub(crate) fn transition_state_handle() -> Arc<TransitionState> {
    GLOBAL_TransitionState.clone()
}

pub(crate) fn event_notifier_handle() -> Arc<RwLock<EventNotifier>> {
    GLOBAL_EventNotifier.clone()
}

pub(crate) async fn local_disk_by_path(path: &str) -> Option<DiskStore> {
    GLOBAL_LOCAL_DISK_MAP.read().await.get(path).cloned().flatten()
}

pub(crate) async fn local_disk_path_by_id(disk_id: &Uuid) -> Option<String> {
    GLOBAL_LOCAL_DISK_ID_MAP.read().await.get(disk_id).cloned()
}

pub(crate) async fn record_local_disk_id(disk_id: Uuid, endpoint: String) {
    GLOBAL_LOCAL_DISK_ID_MAP.write().await.insert(disk_id, endpoint);
}

pub(crate) async fn replace_local_disk_id(previous: Option<Uuid>, current: Option<Uuid>, endpoint: String) {
    let mut disk_id_map = GLOBAL_LOCAL_DISK_ID_MAP.write().await;
    if let Some(previous_id) = previous {
        disk_id_map.remove(&previous_id);
    }
    if let Some(current_id) = current {
        disk_id_map.insert(current_id, endpoint);
    }
}

pub(crate) async fn record_local_disks(disks: Vec<DiskStore>) {
    let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
    for disk in disks {
        let path = disk.endpoint().to_string();
        global_local_disk_map.insert(path, Some(disk.clone()));
    }
}

pub(crate) async fn local_disk_set_drive(pool_idx: usize, set_idx: usize, disk_idx: usize) -> Option<DiskStore> {
    GLOBAL_LOCAL_DISK_SET_DRIVES.read().await[pool_idx][set_idx][disk_idx].clone()
}

pub(crate) async fn local_disk_for_endpoint(endpoint: &Endpoint) -> Option<DiskStore> {
    let global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.read().await;
    if global_set_drives.is_empty() {
        return GLOBAL_LOCAL_DISK_MAP
            .read()
            .await
            .get(&endpoint.to_string())
            .cloned()
            .unwrap_or(None);
    }

    let pool_idx = usize::try_from(endpoint.pool_idx).ok()?;
    let set_idx = usize::try_from(endpoint.set_idx).ok()?;
    let disk_idx = usize::try_from(endpoint.disk_idx).ok()?;

    global_set_drives
        .get(pool_idx)
        .and_then(|sets| sets.get(set_idx))
        .and_then(|disks| disks.get(disk_idx))
        .cloned()
        .unwrap_or(None)
}

pub(crate) async fn local_disk_paths() -> Vec<String> {
    GLOBAL_LOCAL_DISK_MAP.read().await.keys().cloned().collect()
}

pub(crate) async fn local_disks() -> Vec<DiskStore> {
    GLOBAL_LOCAL_DISK_MAP
        .read()
        .await
        .values()
        .filter_map(|v| v.as_ref().cloned())
        .collect()
}

pub(crate) async fn local_disk_entries() -> Vec<Option<DiskStore>> {
    GLOBAL_LOCAL_DISK_MAP.read().await.values().cloned().collect()
}

pub(crate) async fn initialize_local_disk_maps(endpoint_pools: EndpointServerPools, opt: &DiskOption) -> Result<()> {
    let mut global_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.write().await;
    for pool_eps in endpoint_pools.as_ref().iter() {
        let mut set_count_drives = Vec::with_capacity(pool_eps.set_count);
        for _ in 0..pool_eps.set_count {
            set_count_drives.push(vec![None; pool_eps.drives_per_set]);
        }

        global_set_drives.push(set_count_drives);
    }

    let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;

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
    GLOBAL_TierConfigMgr.write().await.init(store).await
}
