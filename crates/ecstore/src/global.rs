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

use crate::{
    bucket::lifecycle::bucket_lifecycle_ops::LifecycleSys,
    disk::DiskStore,
    endpoints::{EndpointServerPools, PoolEndpoints, SetupType},
    event_notification::EventNotifier,
    store::ECStore,
    tier::tier::TierConfigMgr,
};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use tokio::sync::{OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const DISK_ASSUME_UNKNOWN_SIZE: u64 = 1 << 30;
pub const DISK_MIN_INODES: u64 = 1000;
pub const DISK_FILL_FRACTION: f64 = 0.99;
pub const DISK_RESERVE_FRACTION: f64 = 0.15;

lazy_static! {
    static ref GLOBAL_RUSTFS_PORT: OnceLock<u16> = OnceLock::new();
    static ref globalDeploymentIDPtr: OnceLock<Uuid> = OnceLock::new();
    pub static ref GLOBAL_OBJECT_API: OnceLock<Arc<ECStore>> = OnceLock::new();
    pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore>>>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_IsErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsDistErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsErasureSD: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_Endpoints: OnceLock<EndpointServerPools> = OnceLock::new();
    pub static ref GLOBAL_RootDiskThreshold: RwLock<u64> = RwLock::new(0);
    pub static ref GLOBAL_TierConfigMgr: Arc<RwLock<TierConfigMgr>> = TierConfigMgr::new();
    pub static ref GLOBAL_LifecycleSys: Arc<LifecycleSys> = LifecycleSys::new();
    pub static ref GLOBAL_EventNotifier: Arc<RwLock<EventNotifier>> = EventNotifier::new();
    pub static ref GLOBAL_BOOT_TIME: OnceCell<SystemTime> = OnceCell::new();
    pub static ref GLOBAL_LocalNodeName: String = "127.0.0.1:9000".to_string();
    pub static ref GLOBAL_LocalNodeNameHex: String = rustfs_utils::crypto::hex(GLOBAL_LocalNodeName.as_bytes());
    pub static ref GLOBAL_NodeNamesHex: HashMap<String, ()> = HashMap::new();
    pub static ref GLOBAL_REGION: OnceLock<String> = OnceLock::new();
}

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
    GLOBAL_RUSTFS_PORT.set(value).expect("set_global_rustfs_port fail");
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
    globalDeploymentIDPtr.set(id).unwrap();
}

/// Get the global deployment id
///
/// # Returns
/// * `Option<String>` - The global deployment id as a string, if set
///
pub fn get_global_deployment_id() -> Option<String> {
    globalDeploymentIDPtr.get().map(|v| v.to_string())
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
    GLOBAL_Endpoints
        .set(EndpointServerPools::from(eps))
        .expect("GLOBAL_Endpoints set failed")
}

/// Get the global endpoints
///
/// # Returns
/// * `EndpointServerPools` - The global endpoints
///
pub fn get_global_endpoints() -> EndpointServerPools {
    if let Some(eps) = GLOBAL_Endpoints.get() {
        eps.clone()
    } else {
        EndpointServerPools::default()
    }
}

/// Create a new object layer instance
///
/// # Returns
/// * `Option<Arc<ECStore>>` - The global object layer instance, if set
///
pub fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    GLOBAL_OBJECT_API.get().cloned()
}

/// Set the global object layer
///
/// # Arguments
/// * `o` - The ECStore instance to set globally
///
/// # Returns
/// * None
pub async fn set_object_layer(o: Arc<ECStore>) {
    GLOBAL_OBJECT_API.set(o).expect("set_object_layer fail ")
}

/// Check if the setup type is distributed erasure coding
///
/// # Returns
/// * `bool` - True if the setup type is distributed erasure coding, false otherwise
///
pub async fn is_dist_erasure() -> bool {
    let lock = GLOBAL_IsDistErasure.read().await;
    *lock
}

/// Check if the setup type is erasure coding with single data center
///
/// # Returns
/// * `bool` - True if the setup type is erasure coding with single data center, false otherwise
///
pub async fn is_erasure_sd() -> bool {
    let lock = GLOBAL_IsErasureSD.read().await;
    *lock
}

/// Check if the setup type is erasure coding
///
/// # Returns
/// * `bool` - True if the setup type is erasure coding, false otherwise
///
pub async fn is_erasure() -> bool {
    let lock = GLOBAL_IsErasure.read().await;
    *lock
}

/// Update the global erasure type based on the setup type
///
/// # Arguments
/// * `setup_type` - The SetupType to update the global erasure type
///
/// # Returns
/// * None
pub async fn update_erasure_type(setup_type: SetupType) {
    let mut is_erasure = GLOBAL_IsErasure.write().await;
    *is_erasure = setup_type == SetupType::Erasure;

    let mut is_dist_erasure = GLOBAL_IsDistErasure.write().await;
    *is_dist_erasure = setup_type == SetupType::DistErasure;

    if *is_dist_erasure {
        *is_erasure = true
    }

    let mut is_erasure_sd = GLOBAL_IsErasureSD.write().await;
    *is_erasure_sd = setup_type == SetupType::ErasureSD;
}

// pub fn is_legacy() -> bool {
//     if let Some(endpoints) = GLOBAL_Endpoints.get() {
//         endpoints.as_ref().len() == 1 && endpoints.as_ref()[0].legacy
//     } else {
//         false
//     }
// }

type TypeLocalDiskSetDrives = Vec<Vec<Vec<Option<DiskStore>>>>;

/// Set the global region
///
/// # Arguments
/// * `region` - The region string to set globally
///
/// # Returns
/// * None
pub fn set_global_region(region: String) {
    GLOBAL_REGION.set(region).unwrap();
}

/// Get the global region
///
/// # Returns
/// * `Option<String>` - The global region string, if set
///
pub fn get_global_region() -> Option<String> {
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
    init_background_services_cancel_token(cancel_token.clone()).expect("Background services cancel token already initialized");
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
