use lazy_static::lazy_static;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    disk::DiskStore,
    endpoints::{EndpointServerPools, PoolEndpoints, SetupType},
    store::ECStore,
};

lazy_static! {
    pub static ref GLOBAL_OBJECT_API: Arc<RwLock<Option<ECStore>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore>>>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_IsErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsDistErasure: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_IsErasureSD: RwLock<bool> = RwLock::new(false);
    pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives>> = Arc::new(RwLock::new(Vec::new()));
    pub static ref GLOBAL_Endpoints: RwLock<EndpointServerPools> = RwLock::new(EndpointServerPools(Vec::new()));
    pub static ref GLOBAL_RootDiskThreshold: RwLock<u64> = RwLock::new(0);
}

pub async fn set_global_endpoints(eps: Vec<PoolEndpoints>) {
    let mut endpoints = GLOBAL_Endpoints.write().await;
    endpoints.reset(eps);
}

pub fn new_object_layer_fn() -> Arc<RwLock<Option<ECStore>>> {
    GLOBAL_OBJECT_API.clone()
}

pub async fn set_object_layer(o: ECStore) {
    let mut global_object_api = GLOBAL_OBJECT_API.write().await;
    *global_object_api = Some(o);
}

pub async fn is_dist_erasure() -> bool {
    let lock = GLOBAL_IsDistErasure.read().await;
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

pub async fn is_legacy() -> bool {
    let lock = GLOBAL_Endpoints.read().await;
    let endpoints = lock.as_ref();
    endpoints.len() == 1 && endpoints[0].legacy
}

type TypeLocalDiskSetDrives = Vec<Vec<Vec<Option<DiskStore>>>>;
