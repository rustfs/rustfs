use std::{path::Path, time::{SystemTime, UNIX_EPOCH}};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    disk::{DiskStore, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result}, heal::heal_ops::HEALING_TRACKER_FILENAME, new_object_layer_fn, store_api::StorageAPI, utils::fs::read_file,
};

pub type HealScanMode = usize;
pub type HealItemType = String;

#[derive(Clone, Copy, Debug, Default)]
pub struct HealOpts {
    pub recursive: bool,
    pub dry_run: bool,
    pub remove: bool,
    pub recreate: bool,
    pub scan_mode: HealScanMode,
    pub update_parity: bool,
    pub no_lock: bool,
    pub pool: Option<usize>,
    pub set: Option<usize>,
}

#[derive(Debug)]
struct HealDriveInfo {
    uuid: String,
    endpoint: String,
    state: String,
}

#[derive(Debug)]
pub struct HealResultItem {
    pub result_index: usize,
    pub heal_item_type: HealItemType,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub detail: String,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub disk_count: usize,
    pub set_count: usize,
    pub before: Vec<HealDriveInfo>,
    pub after: Vec<HealDriveInfo>,
    pub object_size: usize,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct HealingTracker {
    #[serde(skip_serializing, skip_deserializing)]
    disk: Option<DiskStore>,

    id: String,
    pool_index: Option<usize>,
    set_index: Option<usize>,
    disk_index: Option<usize>,
    path: String,
    endpoint: String,
    started: u64,
    last_update: u64,

    objects_total_count: u64,
    objects_total_size: u64,

    items_healed: u64,
    items_failed: u64,

    bytes_done: u64,
    bytes_failed: u64,

    bucket: String,
    object: String,

    resume_items_healed: u64,
    resume_items_failed: u64,
    resume_items_skipped: u64,
    resume_bytes_done: u64,
    resume_bytes_failed: u64,
    resume_bytes_skipped: u64,

    queue_buckets: Vec<String>,

    healed_buckets: Vec<String>,

    heal_id: String,

    item_skipped: u64,
    bytes_skipped: u64,

    retry_attempts: u64,

    finished: bool,

    #[serde(skip_serializing, skip_deserializing)]
    mu: RwLock<bool>,
}

impl HealingTracker {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        serde_json::to_string(self)
            .map(|s| s.as_bytes().to_vec())
            .map_err(|err| Error::from_string(err.to_string()))
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        serde_json::from_slice::<HealingTracker>(data)
        .map_err(|err| Error::from_string(err.to_string()))
    }

    pub async fn reset_healing(&mut self) {
        self.mu.write().await;
        self.items_healed = 0;
        self.items_failed = 0;
        self.bytes_done = 0;
        self.bytes_failed = 0;
        self.resume_items_healed = 0;
        self.resume_items_failed = 0;
        self.resume_bytes_done = 0;
        self.resume_bytes_failed = 0;
        self.item_skipped = 0;
        self.bytes_skipped = 0;

        self.healed_buckets = Vec::new();
        self.bucket = String::new();
        self.object = String::new();
    }

    pub async fn get_last_update(&self) -> u64 {
        self.mu.read().await;

        self.last_update
    }

    pub async fn get_bucket(&self) -> String {
        self.mu.read().await;

        self.bucket.clone()
    }

    pub async fn set_bucket(&mut self, bucket: &str) {
        self.mu.write().await;

        self.bucket = bucket.to_string();
    }

    pub async fn get_object(&self) -> String {
        self.mu.read().await;

        self.object.clone()
    }

    pub async fn set_object(&mut self, object: &str) {
        self.mu.write().await;

        self.object = object.to_string();
    }

    pub async fn update_progress(&mut self, success: bool, skipped: bool, by: u64) {
        self.mu.write().await;

        if success {
            self.items_healed += 1;
            self.bytes_done += by;
        } else if skipped {
            self.item_skipped += 1;
            self.bytes_skipped += by;
        } else {
            self.items_failed += 1;
            self.bytes_failed += by;
        }
    }

    pub async fn update(&mut self) -> Result<()> {
        if let Some(disk) = &self.disk {
            if healing(&disk.path().to_string_lossy().to_string()).await?.is_none() {
                return Err(Error::from_string(format!("healingTracker: drive {} is not marked as healing", self.id)));
            }
            self.mu.write().await;
            if self.id.is_empty() || self.pool_index.is_none() || self.set_index.is_none() || self.disk_index.is_none() {
                self.id = disk.get_disk_id().await?.map_or("".to_string(), |id| id.to_string());
                let disk_location = disk.get_disk_location();
                self.pool_index = disk_location.pool_idx;
                self.set_index = disk_location.set_idx;
                self.disk_index = disk_location.disk_idx;
            }
        }

        self.save().await
    }

    pub async fn save(&mut self) -> Result<()> {
        self.mu.write().await;
        if self.pool_index.is_none() || self.set_index.is_none() || self.disk_index.is_none() {
            let layer = new_object_layer_fn();
            let lock = layer.read().await;
            let store = match lock.as_ref() {
                Some(s) => s,
                None => return Err(Error::from_string("Not init".to_string())),
            };
            (self.pool_index, self.set_index, self.disk_index) = store.get_pool_and_set(&self.id).await?;
        }

        self.last_update = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
        
        let htracker_bytes = self.marshal_msg()?;

        if let Some(disk) = &self.disk {
            let file_path = Path::new(BUCKET_META_PREFIX).join(HEALING_TRACKER_FILENAME);
            return disk.write_all(RUSTFS_META_BUCKET, file_path.to_str().unwrap(), htracker_bytes).await;
        }
        Ok(())
    }
}

async fn load_healing_tracker(disk: &Option<DiskStore>) -> Result<HealingTracker> {
    if let Some(disk) = disk {
        let disk_id = disk.get_disk_id().await?;
        if let Some(disk_id) = disk_id {
            let disk_id = disk_id.to_string();
            let file_path = Path::new(BUCKET_META_PREFIX).join(HEALING_TRACKER_FILENAME);
            let data = disk.read_all(RUSTFS_META_BUCKET, file_path.to_str().unwrap()).await?;
            let mut healing_tracker = HealingTracker::unmarshal_msg(&data)?;
            if healing_tracker.id != disk_id && !healing_tracker.id.is_empty() {
                return Err(Error::from_string(format!("loadHealingTracker: drive id mismatch expected {}, got {}", healing_tracker.id, disk_id)));
            }
            healing_tracker.id = disk_id;
            return Ok(healing_tracker);
        } else {
            return Err(Error::from_string("loadHealingTracker: disk not have id"));
        }
        
    } else {
        return Err(Error::from_string("loadHealingTracker: nil drive given"));
    }
}

async fn init_healing_tracker(disk: DiskStore, heal_id: String) -> Result<HealingTracker> {
    let mut healing_tracker = HealingTracker::default();
    healing_tracker.id = disk.get_disk_id().await?.map_or("".to_string(), |id| id.to_string());
    healing_tracker.heal_id = heal_id;
    healing_tracker.path = disk.to_string();
    healing_tracker.endpoint = disk.endpoint().to_string();
    healing_tracker.started = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("Time went backwards")
    .as_secs();
    let disk_location = disk.get_disk_location();
    healing_tracker.pool_index = disk_location.pool_idx;
    healing_tracker.set_index = disk_location.set_idx;
    healing_tracker.disk_index = disk_location.disk_idx;
    healing_tracker.disk = Some(disk);

    Ok(healing_tracker)
}

pub async fn healing(derive_path: &str) -> Result<Option<HealingTracker>> {
    let healing_file = Path::new(derive_path)
        .join(RUSTFS_META_BUCKET)
        .join(BUCKET_META_PREFIX)
        .join(HEALING_TRACKER_FILENAME);

    let b = read_file(healing_file).await?;
    if b.is_empty() {
        return Ok(None);
    }

    let healing_tracker = HealingTracker::unmarshal_msg(&b)?;

    Ok(Some(healing_tracker))
}
