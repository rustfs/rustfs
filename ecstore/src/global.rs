use crate::error::Result;
use lazy_static::lazy_static;
use std::{collections::HashMap, sync::Arc};
use tokio::{fs, sync::RwLock};

use crate::{
    disk::{new_disk, DiskOption, DiskStore},
    endpoints::{EndpointServerPools, SetupType},
    store::ECStore,
};

lazy_static! {
    pub static ref GLOBAL_OBJECT_API: Arc<RwLock<Option<ECStore>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore>>>> = Arc::new(RwLock::new(Vec::new()));
}

pub fn new_object_layer_fn() -> Arc<RwLock<Option<ECStore>>> {
    GLOBAL_OBJECT_API.clone()
}

pub async fn set_object_layer(o: ECStore) {
    let mut global_object_api = GLOBAL_OBJECT_API.write().await;
    *global_object_api = Some(o);
}

lazy_static! {
    static ref GLOBAL_IsErasure: RwLock<bool> = RwLock::new(false);
    static ref GLOBAL_IsDistErasure: RwLock<bool> = RwLock::new(false);
    static ref GLOBAL_IsErasureSD: RwLock<bool> = RwLock::new(false);
}

pub async fn is_dist_erasure() -> bool {
    let lock = GLOBAL_IsDistErasure.read().await;
    *lock == true
}

pub async fn is_erasure() -> bool {
    let lock = GLOBAL_IsErasure.read().await;
    *lock == true
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

type TypeLocalDiskSetDrives = Vec<Vec<Vec<Option<DiskStore>>>>;

lazy_static! {
    pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore>>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives>> = Arc::new(RwLock::new(Vec::new()));
}
