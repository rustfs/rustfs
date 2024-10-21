use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    disk::{DeleteOptions, DiskStore, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result},
    heal::heal_ops::HEALING_TRACKER_FILENAME,
    new_object_layer_fn,
    store_api::{BucketInfo, StorageAPI},
    utils::fs::read_file,
};

pub type HealScanMode = usize;
pub type HealItemType = String;

pub const HEAL_UNKNOWN_SCAN: HealScanMode = 0;
pub const HEAL_NORMAL_SCAN: HealScanMode = 1;
pub const HEAL_DEEP_SCAN: HealScanMode = 2;

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

#[derive(Clone, Debug)]
struct HealDriveInfo {
    uuid: String,
    endpoint: String,
    state: String,
}

#[derive(Clone, Debug, Default)]
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct HealStartSuccess {
    pub client_token: String,
    pub client_address: String,
    pub start_time: u64,
}

pub type HealStopSuccess = HealStartSuccess;

pub struct HealingDisk {
    pub id: String,
    pub heal_id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub endpoint: String,
    pub path: String,
    pub started: u64,
    pub last_update: u64,
    pub retry_attempts: u64,
    pub objects_total_count: u64,
    pub objects_total_size: u64,
    pub items_healed: u64,
    pub items_failed: u64,
    pub item_skipped: u64,
    pub bytes_done: u64,
    pub bytes_failed: u64,
    pub bytes_skipped: u64,
    pub objects_healed: u64,
    pub objects_failed: u64,
    pub bucket: String,
    pub object: String,
    pub queue_buckets: Vec<String>,
    pub healed_buckets: Vec<String>,
    pub finished: bool,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct HealingTracker {
    #[serde(skip_serializing, skip_deserializing)]
    pub disk: Option<DiskStore>,
    pub id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub path: String,
    pub endpoint: String,
    pub started: u64,
    pub last_update: u64,
    pub objects_total_count: u64,
    pub objects_total_size: u64,
    pub items_healed: u64,
    pub items_failed: u64,
    pub item_skipped: u64,
    pub bytes_done: u64,
    pub bytes_failed: u64,
    pub bytes_skipped: u64,
    pub bucket: String,
    pub object: String,
    pub resume_items_healed: u64,
    pub resume_items_failed: u64,
    pub resume_items_skipped: u64,
    pub resume_bytes_done: u64,
    pub resume_bytes_failed: u64,
    pub resume_bytes_skipped: u64,
    pub queue_buckets: Vec<String>,
    pub healed_buckets: Vec<String>,
    pub heal_id: String,
    pub retry_attempts: u64,
    pub finished: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub mu: RwLock<bool>,
}

impl HealingTracker {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        serde_json::to_string(self)
            .map(|s| s.as_bytes().to_vec())
            .map_err(|err| Error::from_string(err.to_string()))
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        serde_json::from_slice::<HealingTracker>(data).map_err(|err| Error::from_string(err.to_string()))
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

        // TODO: globalBackgroundHealState

        if let Some(disk) = &self.disk {
            let file_path = Path::new(BUCKET_META_PREFIX).join(HEALING_TRACKER_FILENAME);
            return disk
                .write_all(RUSTFS_META_BUCKET, file_path.to_str().unwrap(), htracker_bytes)
                .await;
        }
        Ok(())
    }

    async fn delete(&self) -> Result<()> {
        if let Some(disk) = &self.disk {
            let file_path = Path::new(BUCKET_META_PREFIX).join(HEALING_TRACKER_FILENAME);
            return disk
                .delete(
                    RUSTFS_META_BUCKET,
                    file_path.to_str().unwrap(),
                    DeleteOptions {
                        recursive: false,
                        immediate: false,
                        ..Default::default()
                    },
                )
                .await;
        }

        Ok(())
    }

    async fn is_healed(&self, bucket: &str) -> bool {
        self.mu.read().await;
        for v in self.healed_buckets.iter() {
            if v == bucket {
                return true;
            }
        }

        false
    }

    async fn resume(&mut self) {
        self.mu.write().await;

        self.items_healed = self.resume_items_healed;
        self.items_failed = self.resume_items_failed;
        self.item_skipped = self.resume_items_skipped;
        self.bytes_done = self.resume_bytes_done;
        self.bytes_failed = self.resume_bytes_failed;
        self.bytes_skipped = self.resume_bytes_skipped;
    }

    async fn bucket_done(&mut self, bucket: &str) {
        self.mu.write().await;

        self.resume_items_healed = self.items_healed;
        self.resume_items_failed = self.items_failed;
        self.resume_items_skipped = self.item_skipped;
        self.resume_bytes_done = self.bytes_done;
        self.resume_bytes_failed = self.bytes_failed;
        self.resume_bytes_skipped = self.bytes_skipped;
        self.healed_buckets.push(bucket.to_string());

        self.queue_buckets.retain(|x| x != bucket);
    }

    async fn set_queue_buckets(&mut self, buckets: &[BucketInfo]) {
        self.mu.write().await;

        buckets.iter().for_each(|bucket| {
            if !self.healed_buckets.contains(&bucket.name) {
                self.queue_buckets.push(bucket.name.clone());
            }
        });
    }

    pub async fn to_healing_disk(&self) -> HealingDisk {
        self.mu.read().await;

        HealingDisk {
            id: self.id.clone(),
            heal_id: self.heal_id.clone(),
            pool_index: self.pool_index,
            set_index: self.set_index,
            disk_index: self.disk_index,
            endpoint: self.endpoint.clone(),
            path: self.path.clone(),
            started: self.started,
            last_update: self.last_update,
            retry_attempts: self.retry_attempts,
            objects_total_count: self.objects_total_count,
            objects_total_size: self.objects_total_size,
            items_healed: self.items_healed,
            items_failed: self.items_failed,
            item_skipped: self.item_skipped,
            bytes_done: self.bytes_done,
            bytes_failed: self.bytes_failed,
            bytes_skipped: self.bytes_skipped,
            objects_healed: self.items_healed,
            objects_failed: self.items_failed,
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            queue_buckets: self.queue_buckets.clone(),
            healed_buckets: self.healed_buckets.clone(),
            finished: self.finished,
        }
    }
}

impl Clone for HealingTracker {
    fn clone(&self) -> Self {
        Self {
            disk: self.disk.clone(),
            id: self.id.clone(),
            pool_index: self.pool_index.clone(),
            set_index: self.set_index.clone(),
            disk_index: self.disk_index.clone(),
            path: self.path.clone(),
            endpoint: self.endpoint.clone(),
            started: self.started.clone(),
            last_update: self.last_update.clone(),
            objects_total_count: self.objects_total_count.clone(),
            objects_total_size: self.objects_total_size.clone(),
            items_healed: self.items_healed.clone(),
            items_failed: self.items_failed.clone(),
            item_skipped: self.item_skipped.clone(),
            bytes_done: self.bytes_done.clone(),
            bytes_failed: self.bytes_failed.clone(),
            bytes_skipped: self.bytes_skipped.clone(),
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            resume_items_healed: self.resume_items_healed.clone(),
            resume_items_failed: self.resume_items_failed.clone(),
            resume_items_skipped: self.resume_items_skipped.clone(),
            resume_bytes_done: self.resume_bytes_done.clone(),
            resume_bytes_failed: self.resume_bytes_failed.clone(),
            resume_bytes_skipped: self.resume_bytes_skipped.clone(),
            queue_buckets: self.queue_buckets.clone(),
            healed_buckets: self.healed_buckets.clone(),
            heal_id: self.heal_id.clone(),
            retry_attempts: self.retry_attempts.clone(),
            finished: self.finished.clone(),
            mu: RwLock::new(false),
        }
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
                return Err(Error::from_string(format!(
                    "loadHealingTracker: drive id mismatch expected {}, got {}",
                    healing_tracker.id, disk_id
                )));
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
