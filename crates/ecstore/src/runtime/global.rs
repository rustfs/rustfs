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

use crate::bucket::bandwidth::monitor::Monitor;
use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::LifecycleSys,
    disk::DiskStore,
    layout::endpoints::{EndpointServerPools, PoolEndpoints, SetupType},
    runtime::instance::{InstanceContext, bootstrap_ctx},
    services::event_notification::EventNotifier,
    services::tier::tier::TierConfigMgr,
    store::ECStore,
};
use lazy_static::lazy_static;
use rustfs_lock::client::LockClient;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use tokio::sync::{OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

pub const DISK_ASSUME_UNKNOWN_SIZE: u64 = 1 << 30;
pub const DISK_MIN_INODES: u64 = 1000;
pub const DISK_FILL_FRACTION: f64 = 0.99;
pub const DISK_RESERVE_FRACTION: f64 = 0.15;

// Global singletons for backward compatibility with MinIO port.
// These should be migrated to AppContext over time.
// See issue #730 for migration plan.
//
// Tier A (needs migration): GLOBAL_OBJECT_API, GLOBAL_IS_ERASURE*, GLOBAL_LOCAL_DISK_*,
//   GLOBAL_ROOT_DISK_THRESHOLD, GLOBAL_LIFECYCLE_SYS, GLOBAL_EVENT_NOTIFIER, etc.
// Tier B (keep as static): GLOBAL_RUSTFS_PORT, GLOBAL_REGION, env var caches, etc.
lazy_static! {
    static ref GLOBAL_RUSTFS_PORT: OnceLock<u16> = OnceLock::new();
    static ref GLOBAL_DEPLOYMENT_ID: OnceLock<Uuid> = OnceLock::new();
    pub static ref GLOBAL_OBJECT_API: OnceLock<Arc<ECStore>> = OnceLock::new();
    // GLOBAL_IS_ERASURE / GLOBAL_IS_DIST_ERASURE / GLOBAL_IS_ERASURE_SD were three
    // independent `RwLock<bool>` truth sources. Issue #939 Slice1 collapsed them into
    // a single `InstanceContext.erasure_kind`; the accessors below now forward there.
    pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_ID_MAP: Arc<RwLock<HashMap<Uuid, String>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_ENDPOINTS: OnceLock<EndpointServerPools> = OnceLock::new();
    pub static ref GLOBAL_ROOT_DISK_THRESHOLD: RwLock<u64> = RwLock::new(0);
    pub static ref GLOBAL_TIER_CONFIG_MGR: Arc<RwLock<TierConfigMgr>> = TierConfigMgr::new();
    pub static ref GLOBAL_LIFECYCLE_SYS: Arc<LifecycleSys> = LifecycleSys::new();
    pub static ref GLOBAL_EVENT_NOTIFIER: Arc<RwLock<EventNotifier>> = EventNotifier::new();
    pub static ref GLOBAL_BOOT_TIME: OnceCell<SystemTime> = OnceCell::new();
    pub static ref GLOBAL_LOCAL_NODE_NAME_FALLBACK: String = "127.0.0.1:9000".to_string();
    pub static ref GLOBAL_LOCAL_NODE_NAME_HEX_FALLBACK: String =
        rustfs_utils::crypto::hex(GLOBAL_LOCAL_NODE_NAME_FALLBACK.as_bytes());
    pub static ref GLOBAL_REGION: OnceLock<s3s::region::Region> = OnceLock::new();
    pub static ref GLOBAL_LOCAL_LOCK_CLIENT: OnceLock<Arc<dyn LockClient>> = OnceLock::new();
    pub static ref GLOBAL_LOCK_CLIENTS: OnceLock<HashMap<String, Arc<dyn LockClient>>> = OnceLock::new();
    pub static ref GLOBAL_BUCKET_MONITOR: OnceLock<Arc<Monitor>> = OnceLock::new();
}

pub fn init_global_bucket_monitor(num_nodes: u64) {
    if GLOBAL_BUCKET_MONITOR.set(Monitor::new(num_nodes)).is_err() {
        warn!(
            "global bucket monitor already initialized, ignoring re-initialization with num_nodes={}",
            num_nodes
        );
    }
}

pub fn get_global_bucket_monitor() -> Option<Arc<Monitor>> {
    GLOBAL_BUCKET_MONITOR.get().cloned()
}

// Startup-owned process globals intentionally fail fast on duplicate writes.
// A second write means startup published conflicting runtime scalar state.

/// Global cancellation token for background services (data scanner and auto heal)
static GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// Get the global rustfs port
///
/// # Returns
/// * `u16` - The global rustfs port
///
pub fn global_rustfs_port() -> u16 {
    if let Some(p) = GLOBAL_RUSTFS_PORT.get() {
        *p
    } else {
        rustfs_config::DEFAULT_PORT
    }
}

/// Set the global rustfs port
///
/// # Arguments
/// * `value` - The port value to set globally
///
/// # Returns
/// * None
pub fn set_global_rustfs_port(value: u16) {
    GLOBAL_RUSTFS_PORT
        .set(value)
        .expect("GLOBAL_RUSTFS_PORT should be initialized once during startup");
}

/// Set the global deployment id
///
/// # Arguments
/// * `id` - The Uuid to set as the global deployment id
///
/// # Returns
/// * None
///
pub fn set_global_deployment_id(id: Uuid) {
    GLOBAL_DEPLOYMENT_ID
        .set(id)
        .expect("GLOBAL_DEPLOYMENT_ID should be initialized once during startup");
}

/// Get the global deployment id
///
/// # Returns
/// * `Option<String>` - The global deployment id as a string, if set
///
pub fn get_global_deployment_id() -> Option<String> {
    GLOBAL_DEPLOYMENT_ID.get().map(|v| v.to_string())
}
/// Set the global endpoints
///
/// # Arguments
/// * `eps` - A vector of PoolEndpoints to set globally
///
/// # Returns
/// * None
///
pub fn set_global_endpoints(eps: Vec<PoolEndpoints>) {
    GLOBAL_ENDPOINTS
        .set(EndpointServerPools::from(eps))
        .expect("GLOBAL_ENDPOINTS should be initialized once during storage startup")
}

/// Get the global endpoints
///
/// # Returns
/// * `EndpointServerPools` - The global endpoints
///
pub fn get_global_endpoints() -> EndpointServerPools {
    if let Some(eps) = GLOBAL_ENDPOINTS.get() {
        eps.clone()
    } else {
        EndpointServerPools::default()
    }
}

pub fn get_global_endpoints_opt() -> Option<EndpointServerPools> {
    GLOBAL_ENDPOINTS.get().cloned()
}

#[cfg(test)]
pub async fn reset_local_disk_test_state() {
    GLOBAL_LOCAL_DISK_MAP.write().await.clear();
    GLOBAL_LOCAL_DISK_ID_MAP.write().await.clear();
    GLOBAL_LOCAL_DISK_SET_DRIVES.write().await.clear();
}

pub async fn is_first_cluster_node_local() -> bool {
    get_global_endpoints().first_local()
}

pub fn get_global_tier_config_mgr() -> Arc<RwLock<TierConfigMgr>> {
    GLOBAL_TIER_CONFIG_MGR.clone()
}

/// Create a new object layer instance
///
/// # Returns
/// * `Option<Arc<ECStore>>` - The global object layer instance, if set
///
pub fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    GLOBAL_OBJECT_API.get().cloned()
}

pub type ObjectStoreResolver = dyn Fn() -> Option<Arc<ECStore>> + Send + Sync + 'static;

static GLOBAL_OBJECT_STORE_RESOLVER: OnceLock<Arc<ObjectStoreResolver>> = OnceLock::new();

pub fn set_object_store_resolver(resolver: Arc<ObjectStoreResolver>) -> bool {
    GLOBAL_OBJECT_STORE_RESOLVER.set(resolver).is_ok()
}

pub fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    GLOBAL_OBJECT_STORE_RESOLVER
        .get()
        .and_then(|resolver| resolver())
        .or_else(new_object_layer_fn)
}

/// Set the global object layer
///
/// # Arguments
/// * `o` - The ECStore instance to set globally
///
/// # Returns
/// * None
pub async fn set_object_layer(o: Arc<ECStore>) {
    if GLOBAL_OBJECT_API.set(o).is_err() {
        warn!("global object layer already initialized, ignoring re-initialization");
    }
}

/// Check if the setup type is distributed erasure coding
///
/// # Returns
/// * `bool` - True if the setup type is distributed erasure coding, false otherwise
///
pub async fn is_dist_erasure() -> bool {
    current_ctx().is_dist_erasure().await
}

/// Check if the setup type is erasure coding with single data center
///
/// # Returns
/// * `bool` - True if the setup type is erasure coding with single data center, false otherwise
///
pub async fn is_erasure_sd() -> bool {
    current_ctx().is_erasure_sd().await
}

/// Check if the setup type is erasure coding
///
/// # Returns
/// * `bool` - True if the setup type is erasure coding, false otherwise
///
pub async fn is_erasure() -> bool {
    current_ctx().is_erasure().await
}

/// Update the global erasure type based on the setup type
///
/// # Arguments
/// * `setup_type` - The SetupType to update the global erasure type
///
/// # Returns
/// * None
pub async fn update_erasure_type(setup_type: SetupType) {
    current_ctx().set_erasure_kind(setup_type).await;
}

/// Resolve the runtime-identity context the legacy free-function facade should
/// act on.
///
/// This is the honest single-instance default: once the process object layer is
/// published it forwards to the live store's `ctx`; before then it forwards to
/// the process [`bootstrap_ctx`]. Because `ECStore::new` *adopts* the same
/// bootstrap `Arc`, the startup write and the post-construction read hit one
/// cell — single-instance behavior is unchanged. It does not (and cannot)
/// disambiguate between multiple concurrent instances from a free function; per
/// #939 that isolation is delivered by callers reaching `self.ctx` on the object
/// graph, not through this facade.
pub(crate) fn current_ctx() -> Arc<InstanceContext> {
    match GLOBAL_OBJECT_API.get() {
        Some(store) => store.ctx.clone(),
        None => bootstrap_ctx(),
    }
}

// pub fn is_legacy() -> bool {
//     if let Some(endpoints) = GLOBAL_ENDPOINTS.get() {
//         endpoints.as_ref().len() == 1 && endpoints.as_ref()[0].legacy
//     } else {
//         false
//     }
// }

pub(crate) type TypeLocalDiskSetDrives = Vec<Vec<Vec<Option<DiskStore>>>>;

/// Set the global region
///
/// # Arguments
/// * `region` - The Region instance to set globally
///
/// # Returns
/// * None
pub fn set_global_region(region: s3s::region::Region) {
    GLOBAL_REGION
        .set(region)
        .expect("GLOBAL_REGION should be initialized once during startup");
}

/// Get the global region
///
/// # Returns
/// * `Option<s3s::region::Region>` - The global region, if set
///
pub fn get_global_region() -> Option<s3s::region::Region> {
    GLOBAL_REGION.get().cloned()
}

/// Initialize the global background services cancellation token
///
/// # Arguments
/// * `cancel_token` - The CancellationToken instance to set globally
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(CancellationToken)` if setting fails
///
pub fn init_background_services_cancel_token(cancel_token: CancellationToken) -> Result<(), CancellationToken> {
    GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN.set(cancel_token)
}

/// Get the global background services cancellation token
///
/// # Returns
/// * `Option<&'static CancellationToken>` - The global cancellation token, if set
///
pub fn get_background_services_cancel_token() -> Option<&'static CancellationToken> {
    GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN.get()
}

/// Create and initialize the global background services cancellation token
///
/// # Returns
/// * `CancellationToken` - The newly created global cancellation token
///
pub fn create_background_services_cancel_token() -> CancellationToken {
    let cancel_token = CancellationToken::new();
    init_background_services_cancel_token(cancel_token.clone())
        .expect("background services cancel token should be initialized once during startup");
    cancel_token
}

/// Shutdown all background services gracefully
///
/// # Returns
/// * None
pub fn shutdown_background_services() {
    if let Some(cancel_token) = GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN.get() {
        cancel_token.cancel();
    }
}

/// Set the global lock client (first LocalClient created)
///
/// # Arguments
/// * `client` - The LockClient instance to set globally
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(Arc<dyn LockClient>)` if setting fails (client already set)
///
pub fn set_global_lock_client(client: Arc<dyn LockClient>) -> Result<(), Arc<dyn LockClient>> {
    GLOBAL_LOCAL_LOCK_CLIENT.set(client)
}

/// Get the global lock client
///
/// # Returns
/// * `Option<Arc<dyn LockClient>>` - The global lock client, if set
///
pub fn get_global_lock_client() -> Option<Arc<dyn LockClient>> {
    GLOBAL_LOCAL_LOCK_CLIENT.get().cloned()
}

/// Set the global lock clients map
///
/// # Arguments
/// * `clients` - The HashMap of lock clients to set globally
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(HashMap<String, Arc<dyn LockClient>>)` if setting fails (clients already set)
///
pub fn set_global_lock_clients(
    clients: HashMap<String, Arc<dyn LockClient>>,
) -> Result<(), HashMap<String, Arc<dyn LockClient>>> {
    GLOBAL_LOCK_CLIENTS.set(clients)
}

/// Get the global lock clients map
///
/// # Returns
/// * `Option<&HashMap<String, Arc<dyn LockClient>>>` - The global lock clients map, if set
///
pub fn get_global_lock_clients() -> Option<&'static HashMap<String, Arc<dyn LockClient>>> {
    GLOBAL_LOCK_CLIENTS.get()
}
