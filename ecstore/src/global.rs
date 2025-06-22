use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

use crate::heal::mrf::MRFState;
use crate::{
    disk::DiskStore,
    endpoints::{EndpointServerPools, PoolEndpoints, SetupType},
    heal::{background_heal_ops::HealRoutine, heal_ops::AllHealState},
    bucket::lifecycle::bucket_lifecycle_ops::LifecycleSys,
    tier::tier::TierConfigMgr,
    event_notification::EventNotifier,
    store::ECStore,
};

pub const DISK_ASSUME_UNKNOWN_SIZE: u64 = 1 << 30;
pub const DISK_MIN_INODES: u64 = 1000;
pub const DISK_FILL_FRACTION: f64 = 0.99;
pub const DISK_RESERVE_FRACTION: f64 = 0.15;

pub const DEFAULT_PORT: u16 = 9000;

lazy_static! {
    static ref GLOBAL_RUSTFS_PORT: OnceLock<u16> = OnceLock::new();
    pub static ref GLOBAL_OBJECT_API: OnceLock<Arc<ECStore>> = OnceLock::new();
    pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore>>>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_IsErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsDistErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsErasureSD: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_Endpoints: OnceLock<EndpointServerPools> = OnceLock::new();
    pub static ref GLOBAL_RootDiskThreshold: RwLock<u64> = RwLock::new(0);
    pub static ref GLOBAL_BackgroundHealRoutine: Arc<HealRoutine> = HealRoutine::new();
    pub static ref GLOBAL_BackgroundHealState: Arc<AllHealState> = AllHealState::new(false);
    pub static ref GLOBAL_TierConfigMgr: Arc<RwLock<TierConfigMgr>> = TierConfigMgr::new();
    pub static ref GLOBAL_LifecycleSys: Arc<LifecycleSys> = LifecycleSys::new();
    pub static ref GLOBAL_EventNotifier: Arc<RwLock<EventNotifier>> = EventNotifier::new();
    //pub static ref GLOBAL_RemoteTargetTransport
    pub static ref GLOBAL_ALlHealState: Arc<AllHealState> = AllHealState::new(false);
    pub static ref GLOBAL_MRFState: Arc<MRFState> = Arc::new(MRFState::new());
    static ref globalDeploymentIDPtr: OnceLock<Uuid> = OnceLock::new();
    pub static ref GLOBAL_BOOT_TIME: OnceCell<SystemTime> = OnceCell::new();
    pub static ref GLOBAL_LocalNodeName: String = "127.0.0.1:9000".to_string();
    pub static ref GLOBAL_LocalNodeNameHex: String = rustfs_utils::crypto::hex(GLOBAL_LocalNodeName.as_bytes());
    pub static ref GLOBAL_NodeNamesHex: HashMap<String, ()> = HashMap::new();}

/// Get the global rustfs port
pub fn global_rustfs_port() -> u16 {
    if let Some(p) = GLOBAL_RUSTFS_PORT.get() {
        *p
    } else {
        rustfs_config::DEFAULT_PORT
    }
}

/// Set the global rustfs port
pub fn set_global_rustfs_port(value: u16) {
    GLOBAL_RUSTFS_PORT.set(value).expect("set_global_rustfs_port fail");
}

/// Get the global rustfs port
pub fn set_global_deployment_id(id: Uuid) {
    globalDeploymentIDPtr.set(id).unwrap();
}

/// Get the global deployment id
pub fn get_global_deployment_id() -> Option<String> {
    globalDeploymentIDPtr.get().map(|v| v.to_string())
}
/// Get the global deployment id
pub fn set_global_endpoints(eps: Vec<PoolEndpoints>) {
    GLOBAL_Endpoints
        .set(EndpointServerPools::from(eps))
        .expect("GLOBAL_Endpoints set failed")
}

/// Get the global endpoints
pub fn get_global_endpoints() -> EndpointServerPools {
    if let Some(eps) = GLOBAL_Endpoints.get() {
        eps.clone()
    } else {
        EndpointServerPools::default()
    }
}

pub fn new_object_layer_fn() -> Option<Arc<ECStore>> {
    GLOBAL_OBJECT_API.get().cloned()
}

pub async fn set_object_layer(o: Arc<ECStore>) {
    GLOBAL_OBJECT_API.set(o).expect("set_object_layer fail ")
}

pub async fn is_dist_erasure() -> bool {
    let lock = GLOBAL_IsDistErasure.read().await;
    *lock
}

pub async fn is_erasure_sd() -> bool {
    let lock = GLOBAL_IsErasureSD.read().await;
    *lock
}

pub async fn is_erasure() -> bool {
    let lock = GLOBAL_IsErasure.read().await;
    *lock
}

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
