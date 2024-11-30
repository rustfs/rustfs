use std::{path::Path, time::SystemTime};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::RwLock;

use crate::{
    disk::{DeleteOptions, DiskAPI, DiskStore, BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result},
    global::GLOBAL_BackgroundHealState,
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

pub const HEAL_ITEM_METADATA: &str = "metadata";
pub const HEAL_ITEM_BUCKET: &str = "bucket";
pub const HEAL_ITEM_BUCKET_METADATA: &str = "bucket-metadata";
pub const HEAL_ITEM_OBJECT: &str = "object";

pub const DRIVE_STATE_OK: &str = "ok";
pub const DRIVE_STATE_OFFLINE: &str = "offline";
pub const DRIVE_STATE_CORRUPT: &str = "corrupt";
pub const DRIVE_STATE_MISSING: &str = "missing";
pub const DRIVE_STATE_PERMISSION: &str = "permission-denied";
pub const DRIVE_STATE_FAULTY: &str = "faulty";
pub const DRIVE_STATE_ROOT_MOUNT: &str = "root-mount";
pub const DRIVE_STATE_UNKNOWN: &str = "unknown";
pub const DRIVE_STATE_UNFORMATTED: &str = "unformatted"; // only returned by disk

lazy_static! {
    pub static ref TIME_SENTINEL: OffsetDateTime = OffsetDateTime::from_unix_timestamp(0).unwrap();
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct HealOpts {
    pub recursive: bool,
    #[serde(rename = "dryRun")]
    pub dry_run: bool,
    pub remove: bool,
    pub recreate: bool,
    #[serde(rename = "scanMode")]
    pub scan_mode: HealScanMode,
    #[serde(rename = "updateParity")]
    pub update_parity: bool,
    #[serde(rename = "nolock")]
    pub no_lock: bool,
    pub pool: Option<usize>,
    pub set: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealDriveInfo {
    pub uuid: String,
    pub endpoint: String,
    pub state: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Infos {
    #[serde(rename = "drives")]
    pub drives: Vec<HealDriveInfo>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealResultItem {
    #[serde(rename = "resultId")]
    pub result_index: usize,
    #[serde(rename = "type")]
    pub heal_item_type: HealItemType,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "object")]
    pub object: String,
    #[serde(rename = "versionId")]
    pub version_id: String,
    #[serde(rename = "detail")]
    pub detail: String,
    #[serde(rename = "parityBlocks")]
    pub parity_blocks: usize,
    #[serde(rename = "dataBlocks")]
    pub data_blocks: usize,
    #[serde(rename = "diskCount")]
    pub disk_count: usize,
    #[serde(rename = "setCount")]
    pub set_count: usize,
    #[serde(rename = "before")]
    pub before: Infos,
    #[serde(rename = "after")]
    pub after: Infos,
    #[serde(rename = "objectSize")]
    pub object_size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealStartSuccess {
    #[serde(rename = "clientToken")]
    pub client_token: String,
    #[serde(rename = "clientAddress")]
    pub client_address: String,
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
}

impl Default for HealStartSuccess {
    fn default() -> Self {
        Self {
            client_token: Default::default(),
            client_address: Default::default(),
            start_time: Utc::now(),
        }
    }
}

pub type HealStopSuccess = HealStartSuccess;

#[derive(Debug, Default)]
pub struct HealingDisk {
    pub id: String,
    pub heal_id: String,
    pub pool_index: Option<usize>,
    pub set_index: Option<usize>,
    pub disk_index: Option<usize>,
    pub endpoint: String,
    pub path: String,
    pub started: Option<OffsetDateTime>,
    pub last_update: Option<SystemTime>,
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
    pub started: Option<OffsetDateTime>,
    pub last_update: Option<SystemTime>,
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
        serde_json::to_vec(self).map_err(|err| Error::from_string(err.to_string()))
    }

    pub fn unmarshal_msg(data: &[u8]) -> Result<Self> {
        serde_json::from_slice::<HealingTracker>(data).map_err(|err| Error::from_string(err.to_string()))
    }

    pub async fn reset_healing(&mut self) {
        let _ = self.mu.write().await;
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

    pub async fn get_last_update(&self) -> Option<SystemTime> {
        let _ = self.mu.read().await;

        self.last_update
    }

    pub async fn get_bucket(&self) -> String {
        let _ = self.mu.read().await;

        self.bucket.clone()
    }

    pub async fn set_bucket(&mut self, bucket: &str) {
        let _ = self.mu.write().await;

        self.bucket = bucket.to_string();
    }

    pub async fn get_object(&self) -> String {
        let _ = self.mu.read().await;

        self.object.clone()
    }

    pub async fn set_object(&mut self, object: &str) {
        let _ = self.mu.write().await;

        self.object = object.to_string();
    }

    pub async fn update_progress(&mut self, success: bool, skipped: bool, by: u64) {
        let _ = self.mu.write().await;

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
            if healing(disk.path().to_string_lossy().as_ref()).await?.is_none() {
                return Err(Error::from_string(format!("healingTracker: drive {} is not marked as healing", self.id)));
            }
            let _ = self.mu.write().await;
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
        let _ = self.mu.write().await;
        if self.pool_index.is_none() || self.set_index.is_none() || self.disk_index.is_none() {
            let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

            (self.pool_index, self.set_index, self.disk_index) = store.get_pool_and_set(&self.id).await?;
        }

        self.last_update = Some(SystemTime::now());

        let htracker_bytes = self.marshal_msg()?;

        GLOBAL_BackgroundHealState.update_heal_status(self).await;

        if let Some(disk) = &self.disk {
            let file_path = Path::new(BUCKET_META_PREFIX).join(HEALING_TRACKER_FILENAME);
            return disk
                .write_all(RUSTFS_META_BUCKET, file_path.to_str().unwrap(), htracker_bytes)
                .await;
        }
        Ok(())
    }

    pub async fn delete(&self) -> Result<()> {
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

    pub async fn is_healed(&self, bucket: &str) -> bool {
        let _ = self.mu.read().await;
        for v in self.healed_buckets.iter() {
            if v == bucket {
                return true;
            }
        }

        false
    }

    pub async fn resume(&mut self) {
        let _ = self.mu.write().await;

        self.items_healed = self.resume_items_healed;
        self.items_failed = self.resume_items_failed;
        self.item_skipped = self.resume_items_skipped;
        self.bytes_done = self.resume_bytes_done;
        self.bytes_failed = self.resume_bytes_failed;
        self.bytes_skipped = self.resume_bytes_skipped;
    }

    pub async fn bucket_done(&mut self, bucket: &str) {
        let _ = self.mu.write().await;

        self.resume_items_healed = self.items_healed;
        self.resume_items_failed = self.items_failed;
        self.resume_items_skipped = self.item_skipped;
        self.resume_bytes_done = self.bytes_done;
        self.resume_bytes_failed = self.bytes_failed;
        self.resume_bytes_skipped = self.bytes_skipped;
        self.healed_buckets.push(bucket.to_string());

        self.queue_buckets.retain(|x| x != bucket);
    }

    pub async fn set_queue_buckets(&mut self, buckets: &[BucketInfo]) {
        let _ = self.mu.write().await;

        buckets.iter().for_each(|bucket| {
            if !self.healed_buckets.contains(&bucket.name) {
                self.queue_buckets.push(bucket.name.clone());
            }
        });
    }

    pub async fn to_healing_disk(&self) -> HealingDisk {
        let _ = self.mu.read().await;

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
            pool_index: self.pool_index,
            set_index: self.set_index,
            disk_index: self.disk_index,
            path: self.path.clone(),
            endpoint: self.endpoint.clone(),
            started: self.started,
            last_update: self.last_update,
            objects_total_count: self.objects_total_count,
            objects_total_size: self.objects_total_size,
            items_healed: self.items_healed,
            items_failed: self.items_failed,
            item_skipped: self.item_skipped,
            bytes_done: self.bytes_done,
            bytes_failed: self.bytes_failed,
            bytes_skipped: self.bytes_skipped,
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            resume_items_healed: self.resume_items_healed,
            resume_items_failed: self.resume_items_failed,
            resume_items_skipped: self.resume_items_skipped,
            resume_bytes_done: self.resume_bytes_done,
            resume_bytes_failed: self.resume_bytes_failed,
            resume_bytes_skipped: self.resume_bytes_skipped,
            queue_buckets: self.queue_buckets.clone(),
            healed_buckets: self.healed_buckets.clone(),
            heal_id: self.heal_id.clone(),
            retry_attempts: self.retry_attempts,
            finished: self.finished,
            mu: RwLock::new(false),
        }
    }
}

pub async fn load_healing_tracker(disk: &Option<DiskStore>) -> Result<HealingTracker> {
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
            Ok(healing_tracker)
        } else {
            Err(Error::from_string("loadHealingTracker: disk not have id"))
        }
    } else {
        Err(Error::from_string("loadHealingTracker: nil drive given"))
    }
}

pub async fn init_healing_tracker(disk: DiskStore, heal_id: &str) -> Result<HealingTracker> {
    let disk_location = disk.get_disk_location();
    Ok(HealingTracker {
        id: disk.get_disk_id().await?.map_or("".to_string(), |id| id.to_string()),
        heal_id: heal_id.to_string(),
        path: disk.to_string(),
        endpoint: disk.endpoint().to_string(),
        started: Some(OffsetDateTime::now_utc()),
        pool_index: disk_location.pool_idx,
        set_index: disk_location.set_idx,
        disk_index: disk_location.disk_idx,
        disk: Some(disk),
        ..Default::default()
    })
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
