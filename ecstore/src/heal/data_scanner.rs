use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use time::{self, OffsetDateTime};

use super::{
    data_scanner_metric::{ScannerMetric, ScannerMetrics, globalScannerMetrics},
    data_usage::{DATA_USAGE_BLOOM_NAME_PATH, store_data_usage_in_backend},
    data_usage_cache::{DataUsageCache, DataUsageEntry, DataUsageHash},
    heal_commands::{HEAL_DEEP_SCAN, HEAL_NORMAL_SCAN, HealScanMode},
};
use crate::bucket::{
    object_lock::objectlock_sys::{BucketObjectLockSys, enforce_retention_for_deletion},
    utils::is_meta_bucketname,
};
use crate::cmd::bucket_replication::queue_replication_heal;
use crate::event::name::EventName;
use crate::{
    bucket::{
        lifecycle::{
            bucket_lifecycle_audit::LcEventSrc,
            bucket_lifecycle_ops::{GLOBAL_ExpiryState, GLOBAL_TransitionState, LifecycleOps, expire_transitioned_object},
            lifecycle::{self, ExpirationOptions, Lifecycle},
        },
        metadata_sys,
    },
    event_notification::{EventArgs, send_event},
    global::GLOBAL_LocalNodeName,
    store_api::{ObjectOptions, ObjectToDelete, StorageAPI},
};
use crate::{
    bucket::{versioning::VersioningApi, versioning_sys::BucketVersioningSys},
    cmd::bucket_replication::ReplicationStatusType,
    disk,
    heal::data_usage::DATA_USAGE_ROOT,
};
use crate::{
    cache_value::metacache_set::{ListPathRawOptions, list_path_raw},
    config::{
        com::{read_config, save_config},
        heal::Config,
    },
    disk::{DiskInfoOptions, DiskStore},
    global::{GLOBAL_BackgroundHealState, GLOBAL_IsErasure, GLOBAL_IsErasureSD},
    heal::{
        data_usage::BACKGROUND_HEAL_INFO_PATH,
        data_usage_cache::{DataUsageHashMap, hash_path},
        error::ERR_IGNORE_FILE_CONTRIB,
        heal_commands::{HEAL_ITEM_BUCKET, HEAL_ITEM_OBJECT},
        heal_ops::{BG_HEALING_UUID, HealSource},
    },
    new_object_layer_fn,
    store::ECStore,
    store_utils::is_reserved_or_invalid_bucket,
};
use crate::{disk::DiskAPI, store_api::ObjectInfo};
use crate::{
    disk::error::DiskError,
    error::{Error, Result},
};
use crate::{disk::local::LocalDisk, heal::data_scanner_metric::current_path_updater};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use rand::Rng;
use rmp_serde::{Deserializer, Serializer};
use rustfs_filemeta::{FileInfo, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams};
use rustfs_utils::path::encode_dir_object;
use rustfs_utils::path::{SLASH_SEPARATOR, path_join, path_to_bucket_object, path_to_bucket_object_with_base_path};
use s3s::dto::{
    BucketLifecycleConfiguration, DefaultRetention, ExpirationStatus, LifecycleRule, ReplicationConfiguration,
    ReplicationRuleStatus,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{
        RwLock, broadcast,
        mpsc::{self, Sender},
    },
    time::sleep,
};
use tracing::{error, info};

const DATA_SCANNER_SLEEP_PER_FOLDER: Duration = Duration::from_millis(1); // Time to wait between folders.
const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16; // Visit all folders every n cycles.
const DATA_SCANNER_COMPACT_LEAST_OBJECT: u64 = 500; // Compact when there are less than this many objects in a branch.
const DATA_SCANNER_COMPACT_AT_CHILDREN: u64 = 10000; // Compact when there are this many children in a branch.
const DATA_SCANNER_COMPACT_AT_FOLDERS: u64 = DATA_SCANNER_COMPACT_AT_CHILDREN / 4; // Compact when this many subfolders in a single folder.
pub const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: u64 = 250_000; // Compact when this many subfolders in a single folder (even top level).
const DATA_SCANNER_START_DELAY: Duration = Duration::from_secs(60); // Time to wait on startup and between cycles.

pub const HEAL_DELETE_DANGLING: bool = true;
const HEAL_OBJECT_SELECT_PROB: u64 = 1024; // Overall probability of a file being scanned; one in n.

static SCANNER_CYCLE: AtomicU64 = AtomicU64::new(DATA_SCANNER_START_DELAY.as_secs());
static _SCANNER_IDLE_MODE: AtomicU32 = AtomicU32::new(0); // default is throttled when idle
static SCANNER_EXCESS_OBJECT_VERSIONS: AtomicU64 = AtomicU64::new(100);
static SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE: AtomicU64 = AtomicU64::new(1024 * 1024 * 1024 * 1024); // 1 TB
static SCANNER_EXCESS_FOLDERS: AtomicU64 = AtomicU64::new(50_000);

lazy_static! {
    static ref SCANNER_SLEEPER: RwLock<DynamicSleeper> = RwLock::new(new_dynamic_sleeper(2.0, Duration::from_secs(1), true));
    pub static ref globalHealConfig: Arc<RwLock<Config>> = Arc::new(RwLock::new(Config::default()));
}

struct DynamicSleeper {
    factor: f64,
    max_sleep: Duration,
    min_sleep: Duration,
    _is_scanner: bool,
}

type TimerFn = Pin<Box<dyn Future<Output = ()> + Send>>;
impl DynamicSleeper {
    fn timer() -> TimerFn {
        let t = SystemTime::now();
        Box::pin(async move {
            let done_at = SystemTime::now().duration_since(t).unwrap_or_default();
            SCANNER_SLEEPER.read().await.sleep(done_at).await;
        })
    }

    async fn sleep(&self, base: Duration) {
        let (min_wait, max_wait) = (self.min_sleep, self.max_sleep);
        let factor = self.factor;

        let want_sleep = {
            let tmp = base.mul_f64(factor);
            if tmp < min_wait {
                return;
            }

            if max_wait > Duration::from_secs(0) && tmp > max_wait {
                max_wait
            } else {
                tmp
            }
        };
        sleep(want_sleep).await;
    }

    fn _update(&mut self, factor: f64, max_wait: Duration) -> Result<()> {
        if (self.factor - factor).abs() < 1e-10 && self.max_sleep == max_wait {
            return Ok(());
        }

        self.factor = factor;
        self.max_sleep = max_wait;

        Ok(())
    }
}

fn new_dynamic_sleeper(factor: f64, max_wait: Duration, is_scanner: bool) -> DynamicSleeper {
    DynamicSleeper {
        factor,
        max_sleep: max_wait,
        min_sleep: Duration::from_micros(100),
        _is_scanner: is_scanner,
    }
}

/// Initialize and start the data scanner in the background
///
/// This function starts a background task that continuously runs the data scanner
/// with randomized intervals between cycles to avoid resource contention.
///
/// # Features
/// - Graceful shutdown support via cancellation token
/// - Randomized sleep intervals to prevent synchronized scanning across nodes
/// - Minimum sleep duration to avoid excessive CPU usage
/// - Proper error handling and logging
///
/// # Architecture
/// 1. Initialize with random seed for sleep intervals
/// 2. Run scanner cycles in a loop
/// 3. Use randomized sleep between cycles to avoid thundering herd
/// 4. Ensure minimum sleep duration to prevent CPU thrashing
pub async fn init_data_scanner() {
    info!("Initializing data scanner background task");

    tokio::spawn(async move {
        loop {
            // Run the data scanner
            run_data_scanner().await;

            // Calculate randomized sleep duration
            // Use random factor (0.0 to 1.0) multiplied by the scanner cycle duration
            let random_factor = {
                let mut rng = rand::rng();
                rng.random_range(1.0..10.0)
            };
            let base_cycle_duration = SCANNER_CYCLE.load(Ordering::SeqCst) as f64;
            let sleep_duration_secs = random_factor * base_cycle_duration;

            let sleep_duration = Duration::from_secs_f64(sleep_duration_secs);

            info!(duration_secs = sleep_duration.as_secs(), "Data scanner sleeping before next cycle");

            // Sleep with the calculated duration
            sleep(sleep_duration).await;
        }
    });
}

/// Run a single data scanner cycle
///
/// This function performs one complete scan cycle, including:
/// - Loading and updating cycle information
/// - Determining scan mode based on healing configuration
/// - Running the namespace scanner
/// - Saving cycle completion state
///
/// # Error Handling
/// - Gracefully handles missing object layer
/// - Continues operation even if individual steps fail
/// - Logs errors appropriately without terminating the scanner
async fn run_data_scanner() {
    info!("Starting data scanner cycle");

    // Get the object layer, return early if not available
    let Some(store) = new_object_layer_fn() else {
        error!("Object layer not initialized, skipping scanner cycle");
        return;
    };

    // Load current cycle information from persistent storage
    let buf = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .unwrap_or_else(|err| {
            info!(error = %err, "Failed to read cycle info, starting fresh");
            Vec::new()
        });

    let mut cycle_info = if buf.is_empty() {
        CurrentScannerCycle::default()
    } else {
        let mut buf_cursor = Deserializer::new(Cursor::new(buf));
        Deserialize::deserialize(&mut buf_cursor).unwrap_or_else(|err| {
            error!(error = %err, "Failed to deserialize cycle info, using default");
            CurrentScannerCycle::default()
        })
    };

    // Start metrics collection for this cycle
    let stop_fn = ScannerMetrics::log(ScannerMetric::ScanCycle);

    // Update cycle information
    cycle_info.current = cycle_info.next;
    cycle_info.started = Utc::now();

    // Update global scanner metrics
    globalScannerMetrics.set_cycle(Some(cycle_info.clone())).await;

    // Read background healing information and determine scan mode
    let bg_heal_info = read_background_heal_info(store.clone()).await;
    let scan_mode =
        get_cycle_scan_mode(cycle_info.current, bg_heal_info.bitrot_start_cycle, bg_heal_info.bitrot_start_time).await;

    // Update healing info if scan mode changed
    if bg_heal_info.current_scan_mode != scan_mode {
        let mut new_heal_info = bg_heal_info;
        new_heal_info.current_scan_mode = scan_mode;
        if scan_mode == HEAL_DEEP_SCAN {
            new_heal_info.bitrot_start_time = SystemTime::now();
            new_heal_info.bitrot_start_cycle = cycle_info.current;
        }
        save_background_heal_info(store.clone(), &new_heal_info).await;
    }

    // Set up data usage storage channel
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = store_data_usage_in_backend(rx).await;
    });

    // Prepare result tracking
    let mut scan_result = HashMap::new();
    scan_result.insert("cycle".to_string(), cycle_info.current.to_string());

    info!(
        cycle = cycle_info.current,
        scan_mode = ?scan_mode,
        "Starting namespace scanner"
    );

    // Run the namespace scanner
    match store.clone().ns_scanner(tx, cycle_info.current as usize, scan_mode).await {
        Ok(_) => {
            info!(cycle = cycle_info.current, "Namespace scanner completed successfully");

            // Update cycle completion information
            cycle_info.next += 1;
            cycle_info.current = 0;
            cycle_info.cycle_completed.push(Utc::now());

            // Maintain cycle completion history (keep only recent cycles)
            if cycle_info.cycle_completed.len() > DATA_USAGE_UPDATE_DIR_CYCLES as usize {
                let _ = cycle_info.cycle_completed.remove(0);
            }

            // Update global metrics with completion info
            globalScannerMetrics.set_cycle(Some(cycle_info.clone())).await;

            // Persist updated cycle information
            // ignore error, continue.
            let mut serialized_data = Vec::new();
            if let Err(err) = cycle_info.serialize(&mut Serializer::new(&mut serialized_data)) {
                error!(error = %err, "Failed to serialize cycle info");
            } else if let Err(err) = save_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH, serialized_data).await {
                error!(error = %err, "Failed to save cycle info to storage");
            }
        }
        Err(err) => {
            error!(
                cycle = cycle_info.current,
                error = %err,
                "Namespace scanner failed"
            );
            scan_result.insert("error".to_string(), err.to_string());
        }
    }

    // Complete metrics collection for this cycle
    stop_fn(&scan_result);
}

#[derive(Debug, Serialize, Deserialize)]
struct BackgroundHealInfo {
    bitrot_start_time: SystemTime,
    bitrot_start_cycle: u64,
    current_scan_mode: HealScanMode,
}

impl Default for BackgroundHealInfo {
    fn default() -> Self {
        Self {
            bitrot_start_time: SystemTime::now(),
            bitrot_start_cycle: Default::default(),
            current_scan_mode: Default::default(),
        }
    }
}

async fn read_background_heal_info(store: Arc<ECStore>) -> BackgroundHealInfo {
    if *GLOBAL_IsErasureSD.read().await {
        return BackgroundHealInfo::default();
    }

    let buf = read_config(store, &BACKGROUND_HEAL_INFO_PATH)
        .await
        .map_or(Vec::new(), |buf| buf);
    if buf.is_empty() {
        return BackgroundHealInfo::default();
    }
    serde_json::from_slice::<BackgroundHealInfo>(&buf).map_or(BackgroundHealInfo::default(), |b| b)
}

async fn save_background_heal_info(store: Arc<ECStore>, info: &BackgroundHealInfo) {
    if *GLOBAL_IsErasureSD.read().await {
        return;
    }
    let b = match serde_json::to_vec(info) {
        Ok(info) => info,
        Err(_) => return,
    };
    let _ = save_config(store, &BACKGROUND_HEAL_INFO_PATH, b).await;
}

async fn get_cycle_scan_mode(current_cycle: u64, bitrot_start_cycle: u64, bitrot_start_time: SystemTime) -> HealScanMode {
    let bitrot_cycle = globalHealConfig.read().await.bitrot_scan_cycle();
    let v = bitrot_cycle.as_secs_f64();
    if v == -1.0 {
        return HEAL_NORMAL_SCAN;
    } else if v == 0.0 {
        return HEAL_DEEP_SCAN;
    }

    if current_cycle - bitrot_start_cycle < HEAL_OBJECT_SELECT_PROB {
        return HEAL_DEEP_SCAN;
    }

    if bitrot_start_time.duration_since(SystemTime::now()).unwrap() > bitrot_cycle {
        return HEAL_DEEP_SCAN;
    }

    HEAL_NORMAL_SCAN
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CurrentScannerCycle {
    pub current: u64,
    pub next: u64,
    pub started: DateTime<Utc>,
    pub cycle_completed: Vec<DateTime<Utc>>,
}

impl Default for CurrentScannerCycle {
    fn default() -> Self {
        Self {
            current: Default::default(),
            next: Default::default(),
            started: Utc::now(),
            cycle_completed: Default::default(),
        }
    }
}

impl CurrentScannerCycle {
    pub fn marshal_msg(&self, next_buf: &[u8]) -> Result<Vec<u8>> {
        let len: u32 = 4;
        let mut wr = Vec::new();

        // 字段数量
        rmp::encode::write_map_len(&mut wr, len)?;

        // write "current"
        rmp::encode::write_str(&mut wr, "current")?;
        rmp::encode::write_uint(&mut wr, self.current)?;

        // write "next"
        rmp::encode::write_str(&mut wr, "next")?;
        rmp::encode::write_uint(&mut wr, self.next)?;

        // write "started"
        rmp::encode::write_str(&mut wr, "started")?;
        rmp::encode::write_sint(&mut wr, system_time_to_timestamp(&self.started))?;

        // write "cycle_completed"
        rmp::encode::write_str(&mut wr, "cycle_completed")?;
        let mut buf = Vec::new();
        self.cycle_completed
            .serialize(&mut Serializer::new(&mut buf))
            .expect("Serialization failed");
        rmp::encode::write_bin(&mut wr, &buf)?;
        let mut result = next_buf.to_vec();
        result.extend(wr.iter());
        Ok(result)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            // ！！！Vec::with_capacity(str_len) 失败，vec! 正常
            let mut field_buff = vec![0u8; str_len as usize];

            cur.read_exact(&mut field_buff)?;

            let field = String::from_utf8(field_buff)?;

            match field.as_str() {
                "current" => {
                    let u: u64 = rmp::decode::read_int(&mut cur)?;
                    self.current = u;
                }

                // "next" => {
                //     let u: u64 = rmp::decode::read_int(&mut cur)?;
                //     self.next = u;
                // }
                "started" => {
                    let u: i64 = rmp::decode::read_int(&mut cur)?;
                    let started = timestamp_to_system_time(u);
                    self.started = started;
                }
                "cycleCompleted" => {
                    let mut buf = Vec::new();
                    let _ = cur.read_to_end(&mut buf)?;
                    let u: Vec<DateTime<Utc>> =
                        Deserialize::deserialize(&mut Deserializer::new(&buf[..])).expect("Deserialization failed");
                    self.cycle_completed = u;
                }
                name => return Err(Error::other(format!("not support field name {name}"))),
            }
        }

        Ok(cur.position())
    }
}

// 将 SystemTime 转换为时间戳
fn system_time_to_timestamp(time: &DateTime<Utc>) -> i64 {
    time.timestamp_micros()
}

// 将时间戳转换为 SystemTime
fn timestamp_to_system_time(timestamp: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(timestamp).unwrap_or_default()
}

#[derive(Clone, Debug, Default)]
pub struct Heal {
    enabled: bool,
    bitrot: bool,
}

#[derive(Clone)]
pub struct ScannerItem {
    pub path: String,
    pub bucket: String,
    pub prefix: String,
    pub object_name: String,
    pub replication: Option<ReplicationConfiguration>,
    pub lifecycle: Option<BucketLifecycleConfiguration>,
    // typ: fs::Permissions,
    pub heal: Heal,
    pub debug: bool,
}

impl ScannerItem {
    pub fn transform_meda_dir(&mut self) {
        let split = self.prefix.split(SLASH_SEPARATOR).map(PathBuf::from).collect::<Vec<_>>();
        if split.len() > 1 {
            self.prefix = path_join(&split[0..split.len() - 1]).to_string_lossy().to_string();
        } else {
            self.prefix = "".to_string();
        }
        self.object_name = split.last().map_or("".to_string(), |v| v.to_string_lossy().to_string());
    }

    pub fn object_path(&self) -> PathBuf {
        path_join(&[PathBuf::from(self.prefix.clone()), PathBuf::from(self.object_name.clone())])
    }

    async fn apply_lifecycle(&self, oi: &ObjectInfo) -> (lifecycle::IlmAction, i64) {
        let mut size = oi.get_actual_size().expect("err!");
        if self.debug {
            info!("apply_lifecycle debug");
        }
        if self.lifecycle.is_none() {
            return (lifecycle::IlmAction::NoneAction, size);
        }

        let version_id = oi.version_id;

        let mut vc = None;
        let mut lr = None;
        let mut rcfg = None;
        if !is_meta_bucketname(&self.bucket) {
            vc = Some(BucketVersioningSys::get(&self.bucket).await.unwrap());
            lr = BucketObjectLockSys::get(&self.bucket).await;
            rcfg = (metadata_sys::get_replication_config(&self.bucket).await).ok();
        }

        let lc_evt = eval_action_from_lifecycle(self.lifecycle.as_ref().expect("err"), lr, rcfg, oi).await;
        if self.debug {
            if version_id.is_some() {
                info!(
                    "lifecycle: {} (version-id={}), Initial scan: {}",
                    self.object_path().to_string_lossy().to_string(),
                    version_id.expect("err"),
                    lc_evt.action
                );
            } else {
                info!(
                    "lifecycle: {} Initial scan: {}",
                    self.object_path().to_string_lossy().to_string(),
                    lc_evt.action
                );
            }
        }

        match lc_evt.action {
            lifecycle::IlmAction::DeleteVersionAction
            | lifecycle::IlmAction::DeleteAllVersionsAction
            | lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => {
                size = 0;
            }
            lifecycle::IlmAction::DeleteAction => {
                if !vc.unwrap().prefix_enabled(&oi.name) {
                    size = 0
                }
            }
            _ => (),
        }

        apply_lifecycle_action(&lc_evt, &LcEventSrc::Scanner, oi).await;
        (lc_evt.action, size)
    }

    pub async fn apply_versions_actions(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let obj_infos = self.apply_newer_noncurrent_version_limit(fivs).await?;
        if obj_infos.len() >= SCANNER_EXCESS_OBJECT_VERSIONS.load(Ordering::SeqCst) as usize {
            // todo
        }

        let mut cumulative_size = 0;
        for obj_info in obj_infos.iter() {
            cumulative_size += obj_info.size;
        }

        if cumulative_size >= SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE.load(Ordering::SeqCst) as i64 {
            //todo
        }

        Ok(obj_infos)
    }

    pub async fn apply_newer_noncurrent_version_limit(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        // let done = ScannerMetrics::time(ScannerMetric::ApplyNonCurrent);

        let lock_enabled = if let Some(rcfg) = BucketObjectLockSys::get(&self.bucket).await {
            rcfg.mode.is_some()
        } else {
            false
        };
        let _vcfg = BucketVersioningSys::get(&self.bucket).await?;

        let versioned = match BucketVersioningSys::get(&self.bucket).await {
            Ok(vcfg) => vcfg.versioned(self.object_path().to_str().unwrap_or_default()),
            Err(_) => false,
        };
        let mut object_infos = Vec::with_capacity(fivs.len());

        if self.lifecycle.is_none() {
            for info in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(
                    info,
                    &self.bucket,
                    &self.object_path().to_string_lossy(),
                    versioned,
                ));
            }
            return Ok(object_infos);
        }

        let event = self
            .lifecycle
            .as_ref()
            .expect("lifecycle err.")
            .noncurrent_versions_expiration_limit(&lifecycle::ObjectOpts {
                name: self.object_path().to_string_lossy().to_string(),
                ..Default::default()
            })
            .await;
        let lim = event.newer_noncurrent_versions;
        if lim == 0 || fivs.len() <= lim + 1 {
            for fi in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(
                    fi,
                    &self.bucket,
                    &self.object_path().to_string_lossy(),
                    versioned,
                ));
            }
            return Ok(object_infos);
        }

        let overflow_versions = &fivs[lim + 1..];
        for fi in fivs[..lim + 1].iter() {
            object_infos.push(ObjectInfo::from_file_info(
                fi,
                &self.bucket,
                &self.object_path().to_string_lossy(),
                versioned,
            ));
        }

        let mut to_del = Vec::<ObjectToDelete>::with_capacity(overflow_versions.len());
        for fi in overflow_versions.iter() {
            let obj = ObjectInfo::from_file_info(fi, &self.bucket, &self.object_path().to_string_lossy(), versioned);
            if lock_enabled && enforce_retention_for_deletion(&obj) {
                //if enforce_retention_for_deletion(&obj) {
                if self.debug {
                    if obj.version_id.is_some() {
                        info!("lifecycle: {} v({}) is locked, not deleting\n", obj.name, obj.version_id.expect("err"));
                    } else {
                        info!("lifecycle: {} is locked, not deleting\n", obj.name);
                    }
                }
                object_infos.push(obj);
                continue;
            }

            if OffsetDateTime::now_utc().unix_timestamp()
                < lifecycle::expected_expiry_time(obj.successor_mod_time.expect("err"), event.noncurrent_days as i32)
                    .unix_timestamp()
            {
                object_infos.push(obj);
                continue;
            }

            to_del.push(ObjectToDelete {
                object_name: obj.name,
                version_id: obj.version_id,
            });
        }

        if !to_del.is_empty() {
            let mut expiry_state = GLOBAL_ExpiryState.write().await;
            expiry_state.enqueue_by_newer_noncurrent(&self.bucket, to_del, event).await;
        }
        // done().await;

        Ok(object_infos)
    }

    pub async fn apply_actions(&mut self, oi: &ObjectInfo, _size_s: &mut SizeSummary) -> (bool, i64) {
        let done = ScannerMetrics::time(ScannerMetric::Ilm);

        let (action, _size) = self.apply_lifecycle(oi).await;

        info!(
            "apply_actions {} {} {:?} {:?}",
            oi.bucket.clone(),
            oi.name.clone(),
            oi.version_id.clone(),
            oi.user_defined.clone()
        );

        // Create a mutable clone if you need to modify fields
        let mut oi = oi.clone();
        oi.replication_status = ReplicationStatusType::from(
            oi.user_defined
                .get("x-amz-bucket-replication-status")
                .unwrap_or(&"PENDING".to_string()),
        );
        info!("apply status is: {:?}", oi.replication_status);
        self.heal_replication(&oi, _size_s).await;
        done();

        if action.delete_all() {
            return (true, 0);
        }

        (false, oi.size)
    }

    pub async fn heal_replication(&mut self, oi: &ObjectInfo, size_s: &mut SizeSummary) {
        if oi.version_id.is_none() {
            error!(
                "heal_replication: no version_id or replication config {} {} {}",
                oi.bucket,
                oi.name,
                oi.version_id.is_none()
            );
            return;
        }

        //let config = s3s::dto::ReplicationConfiguration{ role: todo!(), rules: todo!() };
        // Use the provided variable instead of borrowing self mutably.
        let replication = match metadata_sys::get_replication_config(&oi.bucket).await {
            Ok((replication, _)) => replication,
            Err(_) => {
                error!("heal_replication: failed to get replication config for bucket: {} and object name: {}", oi.bucket, oi.name);
                return;
            }
        };
        if replication.rules.is_empty() {
            error!("heal_replication: no replication rules for bucket {} {}", oi.bucket, oi.name);
            return;
        }
        if replication.role.is_empty() {
            // error!("heal_replication: no replication role for bucket {} {}", oi.bucket, oi.name);
            // return;
        }

        //if oi.delete_marker || !oi.version_purge_status.is_empty() {
        if oi.delete_marker {
            error!(
                "heal_replication: delete marker or version purge status {} {} {:?} {} {:?}",
                oi.bucket, oi.name, oi.version_id, oi.delete_marker, oi.version_purge_status
            );
            return;
        }

        if oi.replication_status == ReplicationStatusType::Completed {
            return;
        }

        info!("replication status is: {:?} and user define {:?}", oi.replication_status, oi.user_defined);

        let roi = queue_replication_heal(&oi.bucket, oi, &replication, 3).await;

        if roi.is_none() {
            info!("not need heal {} {} {:?}", oi.bucket, oi.name, oi.version_id);
            return;
        }

        for (arn, tgt_status) in &roi.unwrap().target_statuses {
            let tgt_size_s = size_s.repl_target_stats.entry(arn.clone()).or_default();

            match tgt_status {
                ReplicationStatusType::Pending => {
                    tgt_size_s.pending_count += 1;
                    tgt_size_s.pending_size += oi.size as usize;
                    size_s.pending_count += 1;
                    size_s.pending_size += oi.size as usize;
                }
                ReplicationStatusType::Failed => {
                    tgt_size_s.failed_count += 1;
                    tgt_size_s.failed_size += oi.size as usize;
                    size_s.failed_count += 1;
                    size_s.failed_size += oi.size as usize;
                }
                ReplicationStatusType::Completed | ReplicationStatusType::CompletedLegacy => {
                    tgt_size_s.replicated_count += 1;
                    tgt_size_s.replicated_size += oi.size as usize;
                    size_s.replicated_count += 1;
                    size_s.replicated_size += oi.size as usize;
                }
                _ => {}
            }
        }

        if matches!(oi.replication_status, ReplicationStatusType::Replica) {
            size_s.replica_count += 1;
            size_s.replica_size += oi.size as usize;
        }
    }
}

#[derive(Debug, Default)]
pub struct SizeSummary {
    pub total_size: usize,
    pub versions: usize,
    pub delete_markers: usize,
    pub replicated_size: usize,
    pub replicated_count: usize,
    pub pending_size: usize,
    pub failed_size: usize,
    pub replica_size: usize,
    pub replica_count: usize,
    pub pending_count: usize,
    pub failed_count: usize,
    pub repl_target_stats: HashMap<String, ReplTargetSizeSummary>,
    // Todo: tires
}

#[derive(Debug, Default)]
pub struct ReplTargetSizeSummary {
    pub replicated_size: usize,
    pub replicated_count: usize,
    pub pending_size: usize,
    pub failed_size: usize,
    pub pending_count: usize,
    pub failed_count: usize,
}

#[derive(Debug, Clone)]
struct CachedFolder {
    name: String,
    parent: DataUsageHash,
    object_heal_prob_div: u32,
}

pub type GetSizeFn =
    Box<dyn Fn(&ScannerItem) -> Pin<Box<dyn Future<Output = std::io::Result<SizeSummary>> + Send>> + Send + Sync + 'static>;
pub type UpdateCurrentPathFn = Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type ShouldSleepFn = Option<Arc<dyn Fn() -> bool + Send + Sync + 'static>>;

struct FolderScanner {
    root: String,
    get_size: GetSizeFn,
    old_cache: DataUsageCache,
    new_cache: DataUsageCache,
    update_cache: DataUsageCache,
    data_usage_scanner_debug: bool,
    heal_object_select: u32,
    scan_mode: HealScanMode,
    disks: Vec<Option<DiskStore>>,
    disks_quorum: usize,
    updates: Sender<DataUsageEntry>,
    last_update: SystemTime,
    update_current_path: UpdateCurrentPathFn,
    skip_heal: AtomicBool,
    drive: LocalDrive,
    we_sleep: ShouldSleepFn,
}

impl FolderScanner {
    async fn should_heal(&self) -> bool {
        if self.skip_heal.load(Ordering::SeqCst) {
            return false;
        }
        if self.heal_object_select == 0 {
            return false;
        }
        if let Ok(info) = self.drive.disk_info(&DiskInfoOptions::default()).await {
            if info.healing {
                self.skip_heal.store(true, Ordering::SeqCst);
                return false;
            }
        }
        true
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn scan_folder(&mut self, folder: &CachedFolder, into: &mut DataUsageEntry) -> Result<()> {
        let this_hash = hash_path(&folder.name);
        let was_compacted = into.compacted;

        'outer: {
            let mut abandoned_children: DataUsageHashMap = if !into.compacted {
                self.old_cache.find_children_copy(this_hash.clone())
            } else {
                HashSet::new()
            };

            let (_, prefix) = path_to_bucket_object_with_base_path(&self.root, &folder.name);
            // Todo: lifeCycle
            let active_life_cycle = if let Some(lc) = self.old_cache.info.lifecycle.as_ref() {
                if lc_has_active_rules(lc, &prefix) {
                    self.old_cache.info.lifecycle.clone()
                } else {
                    None
                }
            } else {
                None
            };

            let replication_cfg = if self.old_cache.info.replication.is_some()
                && rep_has_active_rules(self.old_cache.info.replication.as_ref().unwrap(), &prefix, true)
            {
                self.old_cache.info.replication.clone()
            } else {
                None
            };

            if let Some(should_sleep) = &self.we_sleep {
                if should_sleep() {
                    SCANNER_SLEEPER.read().await.sleep(DATA_SCANNER_SLEEP_PER_FOLDER).await;
                }
            }

            let mut existing_folders = Vec::new();
            let mut new_folders = Vec::new();
            let mut found_objects: bool = false;

            let path = Path::new(&self.root).join(&folder.name);
            if path.is_dir() {
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    let sub_path = entry.path();
                    let ent_name = Path::new(&folder.name).join(&sub_path);
                    let (bucket, prefix) = path_to_bucket_object_with_base_path(&self.root, ent_name.to_str().unwrap());
                    if bucket.is_empty() {
                        continue;
                    }
                    if is_reserved_or_invalid_bucket(&bucket, false) {
                        continue;
                    }

                    if sub_path.is_dir() {
                        let h = hash_path(ent_name.to_str().unwrap());
                        if h == this_hash {
                            continue;
                        }
                        let this = CachedFolder {
                            name: ent_name.to_string_lossy().to_string(),
                            parent: this_hash.clone(),
                            object_heal_prob_div: folder.object_heal_prob_div,
                        };
                        abandoned_children.remove(&h.key());
                        if self.old_cache.cache.contains_key(&h.key()) {
                            existing_folders.push(this);
                            self.update_cache
                                .copy_with_children(&self.old_cache, &h, &Some(this_hash.clone()));
                        } else {
                            new_folders.push(this);
                        }
                        continue;
                    }

                    let _wait = if let Some(should_sleep) = &self.we_sleep {
                        if should_sleep() {
                            DynamicSleeper::timer()
                        } else {
                            Box::pin(async {})
                        }
                    } else {
                        Box::pin(async {})
                    };

                    let mut item = ScannerItem {
                        path: Path::new(&self.root).join(&ent_name).to_string_lossy().to_string(),
                        bucket,
                        prefix: Path::new(&prefix)
                            .parent()
                            .unwrap_or(Path::new(""))
                            .to_string_lossy()
                            .to_string(),
                        object_name: ent_name
                            .file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        debug: self.data_usage_scanner_debug,
                        replication: replication_cfg.clone(),
                        lifecycle: active_life_cycle.clone(),
                        heal: Heal::default(),
                    };

                    item.heal.enabled = this_hash.mod_alt(
                        self.old_cache.info.next_cycle / folder.object_heal_prob_div,
                        self.heal_object_select / folder.object_heal_prob_div,
                    ) && self.should_heal().await;
                    item.heal.bitrot = self.scan_mode == HEAL_DEEP_SCAN;

                    let (sz, err) = match (self.get_size)(&item).await {
                        Ok(sz) => (sz, None),
                        Err(err) => {
                            if err.to_string() != ERR_IGNORE_FILE_CONTRIB {
                                continue;
                            }
                            (SizeSummary::default(), Some(err))
                        }
                    };
                    // successfully read means we have a valid object.
                    found_objects = true;
                    // Remove filename i.e is the meta file to construct object name
                    item.transform_meda_dir();
                    // Object already accounted for, remove from heal map,
                    // simply because getSize() function already heals the
                    // object.
                    abandoned_children.remove(
                        &path_join(&[PathBuf::from(item.bucket.clone()), item.object_path()])
                            .to_string_lossy()
                            .to_string(),
                    );

                    if err.is_none() || err.unwrap().to_string() != ERR_IGNORE_FILE_CONTRIB {
                        into.add_sizes(&sz);
                        into.objects += 1;
                    }
                }
            }
            // if found_objects && *GLOBAL_IsErasure.read().await {
            if found_objects {
                break 'outer;
            }

            let should_compact = self.new_cache.info.name != folder.name
                && (existing_folders.len() + new_folders.len() >= DATA_SCANNER_COMPACT_AT_FOLDERS as usize
                    || existing_folders.len() + new_folders.len() >= DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS as usize);

            let total_folders = existing_folders.len() + new_folders.len();
            if total_folders > SCANNER_EXCESS_FOLDERS.load(Ordering::SeqCst) as usize {
                let _prefix_name = format!("{}/", folder.name.trim_end_matches('/'));
                // todo: notification
            }

            if !into.compacted && should_compact {
                into.compacted = true;
                new_folders.extend(existing_folders.clone());
                existing_folders.clear();
            }

            // Transfer existing
            if !into.compacted {
                for folder in existing_folders.iter() {
                    let h = hash_path(&folder.name);
                    self.update_cache
                        .copy_with_children(&self.old_cache, &h, &Some(folder.parent.clone()));
                }
            }

            // Scan new...
            for folder in new_folders.iter() {
                let h = hash_path(&folder.name);
                if !into.compacted {
                    let mut found_any = false;
                    let mut parent = this_hash.clone();
                    while parent != hash_path(&self.update_cache.info.name) {
                        let e = self.update_cache.find(&parent.key());
                        if e.is_none() || e.as_ref().unwrap().compacted {
                            found_any = true;
                            break;
                        }
                        match self.update_cache.search_parent(&parent) {
                            Some(next) => {
                                parent = next;
                            }
                            None => {
                                found_any = true;
                                break;
                            }
                        }
                    }
                    if !found_any {
                        self.update_cache
                            .replace_hashed(&h, &Some(this_hash.clone()), &DataUsageEntry::default());
                    }
                }
                (self.update_current_path)(&folder.name).await;
                scan(folder, into, self).await;
                // Add new folders if this is new and we don't have existing.
                if !into.compacted {
                    if let Some(parent) = self.update_cache.find(&this_hash.key()) {
                        if !parent.compacted {
                            self.update_cache.delete_recursive(&h);
                            self.update_cache
                                .copy_with_children(&self.new_cache, &h, &Some(this_hash.clone()));
                        }
                    }
                }
            }

            // Scan existing...
            for folder in existing_folders.iter() {
                let h = hash_path(&folder.name);
                if !into.compacted
                    && self.old_cache.is_compacted(&h)
                    && !h.mod_(self.old_cache.info.next_cycle, DATA_USAGE_UPDATE_DIR_CYCLES)
                {
                    self.new_cache
                        .copy_with_children(&self.old_cache, &h, &Some(folder.parent.clone()));
                    into.add_child(&h);
                    continue;
                }
                (self.update_current_path)(&folder.name).await;
                scan(folder, into, self).await;
            }

            // Scan for healing
            if abandoned_children.is_empty() || !self.should_heal().await {
                break 'outer;
            }

            if self.disks.is_empty() || self.disks_quorum == 0 {
                break 'outer;
            }

            let (bg_seq, found) = GLOBAL_BackgroundHealState.get_heal_sequence_by_token(BG_HEALING_UUID).await;
            if !found {
                break 'outer;
            }
            let bg_seq = bg_seq.unwrap();

            let mut resolver = MetadataResolutionParams {
                dir_quorum: self.disks_quorum,
                obj_quorum: self.disks_quorum,
                bucket: "".to_string(),
                strict: false,
                ..Default::default()
            };

            for k in abandoned_children.iter() {
                if !self.should_heal().await {
                    break;
                }

                let (bucket, prefix) = path_to_bucket_object(k);
                (self.update_current_path)(k).await;

                if bucket != resolver.bucket {
                    bg_seq
                        .clone()
                        .queue_heal_task(
                            HealSource {
                                bucket: bucket.clone(),
                                ..Default::default()
                            },
                            HEAL_ITEM_BUCKET.to_owned(),
                        )
                        .await?;
                }

                resolver.bucket = bucket.clone();
                let found_objs = Arc::new(RwLock::new(false));
                let found_objs_clone = found_objs.clone();
                let (tx, rx) = broadcast::channel(1);
                // let tx_partial = tx.clone();
                let tx_finished = tx.clone();
                let update_current_path_agreed = self.update_current_path.clone();
                let update_current_path_partial = self.update_current_path.clone();
                let resolver_clone = resolver.clone();
                let bg_seq_clone = bg_seq.clone();
                let lopts = ListPathRawOptions {
                    disks: self.disks.clone(),
                    bucket: bucket.clone(),
                    path: prefix.clone(),
                    recursice: true,
                    report_not_found: true,
                    min_disks: self.disks_quorum,
                    agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                        Box::pin({
                            let update_current_path_agreed = update_current_path_agreed.clone();
                            async move {
                                update_current_path_agreed(&entry.name).await;
                            }
                        })
                    })),
                    partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                        Box::pin({
                            let update_current_path_partial = update_current_path_partial.clone();
                            // let tx_partial = tx_partial.clone();
                            let resolver_partial = resolver_clone.clone();
                            let bucket_partial = bucket.clone();
                            let found_objs_clone = found_objs_clone.clone();
                            let bg_seq_partial = bg_seq_clone.clone();
                            async move {
                                // Todo
                                // if !fs.should_heal().await {
                                //     let _ = tx_partial.send(true);
                                //     return;
                                // }
                                let entry = match entries.resolve(resolver_partial) {
                                    Some(entry) => entry,
                                    _ => match entries.first_found() {
                                        (Some(entry), _) => entry,
                                        _ => return,
                                    },
                                };

                                update_current_path_partial(&entry.name).await;
                                let mut custom = HashMap::new();
                                if entry.is_dir() {
                                    return;
                                }

                                // We got an entry which we should be able to heal.
                                let fiv = match entry.file_info_versions(&bucket_partial) {
                                    Ok(fiv) => fiv,
                                    Err(_) => {
                                        if let Err(err) = bg_seq_partial
                                            .queue_heal_task(
                                                HealSource {
                                                    bucket: bucket_partial.clone(),
                                                    object: entry.name.clone(),
                                                    version_id: "".to_string(),
                                                    ..Default::default()
                                                },
                                                HEAL_ITEM_OBJECT.to_string(),
                                            )
                                            .await
                                        {
                                            match err {
                                                Error::FileNotFound | Error::FileVersionNotFound => {}
                                                _ => {
                                                    info!("{}", err.to_string());
                                                }
                                            }
                                        } else {
                                            let mut w = found_objs_clone.write().await;
                                            *w = true;
                                        }
                                        return;
                                    }
                                };

                                custom.insert("versions", fiv.versions.len().to_string());
                                let (mut success_versions, mut fail_versions) = (0, 0);
                                for ver in fiv.versions.iter() {
                                    match bg_seq_partial
                                        .queue_heal_task(
                                            HealSource {
                                                bucket: bucket_partial.clone(),
                                                object: fiv.name.clone(),
                                                version_id: ver.version_id.map_or("".to_string(), |ver_id| ver_id.to_string()),
                                                ..Default::default()
                                            },
                                            HEAL_ITEM_OBJECT.to_string(),
                                        )
                                        .await
                                    {
                                        Ok(_) => {
                                            success_versions += 1;

                                            let mut w = found_objs_clone.write().await;
                                            *w = true;
                                        }
                                        Err(_) => {
                                            fail_versions += 1;
                                        }
                                    }
                                }
                                custom.insert("success_versions", success_versions.to_string());
                                custom.insert("failed_versions", fail_versions.to_string());
                            }
                        })
                    })),
                    finished: Some(Box::new(move |_: &[Option<DiskError>]| {
                        Box::pin({
                            let tx_finished = tx_finished.clone();
                            async move {
                                let _ = tx_finished.send(true);
                            }
                        })
                    })),
                    ..Default::default()
                };
                let _ = list_path_raw(rx, lopts).await;

                if *found_objs.read().await {
                    let this: CachedFolder = CachedFolder {
                        name: k.clone(),
                        parent: this_hash.clone(),
                        object_heal_prob_div: 1,
                    };
                    scan(&this, into, self).await;
                }
            }
        }
        if !was_compacted {
            self.new_cache.replace_hashed(&this_hash, &Some(folder.parent.clone()), into);
        }

        if !into.compacted && self.new_cache.info.name != folder.name {
            let mut flat = self.new_cache.size_recursive(&this_hash.key()).unwrap_or_default();
            flat.compacted = true;
            let compact = if flat.objects < DATA_SCANNER_COMPACT_LEAST_OBJECT as usize {
                true
            } else {
                // Compact if we only have objects as children...
                let mut compact = true;
                for k in into.children.iter() {
                    if let Some(v) = self.new_cache.cache.get(k) {
                        if !v.children.is_empty() || v.objects > 1 {
                            compact = false;
                            break;
                        }
                    }
                }
                compact
            };
            if compact {
                self.new_cache.delete_recursive(&this_hash);
                self.new_cache.replace_hashed(&this_hash, &Some(folder.parent.clone()), &flat);
                let mut total: HashMap<String, String> = HashMap::new();
                total.insert("objects".to_string(), flat.objects.to_string());
                total.insert("size".to_string(), flat.size.to_string());
                if flat.versions > 0 {
                    total.insert("versions".to_string(), flat.versions.to_string());
                }
            }
        }
        // Compact if too many children...
        if !into.compacted {
            self.new_cache.reduce_children_of(
                &this_hash,
                DATA_SCANNER_COMPACT_AT_CHILDREN as usize,
                self.new_cache.info.name != folder.name,
            );
        }
        if self.update_cache.cache.contains_key(&this_hash.key()) && !was_compacted {
            // Replace if existed before.
            if let Some(flat) = self.new_cache.size_recursive(&this_hash.key()) {
                self.update_cache.delete_recursive(&this_hash);
                self.update_cache
                    .replace_hashed(&this_hash, &Some(folder.parent.clone()), &flat);
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn send_update(&mut self) {
        if SystemTime::now().duration_since(self.last_update).unwrap() < Duration::from_secs(60) {
            return;
        }
        if let Some(flat) = self.update_cache.size_recursive(&self.new_cache.info.name) {
            let _ = self.updates.send(flat).await;
            self.last_update = SystemTime::now();
        }
    }
}

#[tracing::instrument(level = "info", skip(into, folder_scanner))]
async fn scan(folder: &CachedFolder, into: &mut DataUsageEntry, folder_scanner: &mut FolderScanner) {
    let mut dst = if !into.compacted {
        DataUsageEntry::default()
    } else {
        into.clone()
    };

    if Box::pin(folder_scanner.scan_folder(folder, &mut dst)).await.is_err() {
        return;
    }
    if !into.compacted {
        let h = DataUsageHash(folder.name.clone());
        into.add_child(&h);
        folder_scanner.update_cache.delete_recursive(&h);
        folder_scanner
            .update_cache
            .copy_with_children(&folder_scanner.new_cache, &h, &Some(folder.parent.clone()));
        folder_scanner.send_update().await;
    }
}

fn lc_get_prefix(rule: &LifecycleRule) -> String {
    if let Some(p) = &rule.prefix {
        return p.to_string();
    } else if let Some(filter) = &rule.filter {
        if let Some(p) = &filter.prefix {
            return p.to_string();
        } else if let Some(and) = &filter.and {
            if let Some(p) = &and.prefix {
                return p.to_string();
            }
        }
    }

    "".into()
}

pub fn lc_has_active_rules(config: &BucketLifecycleConfiguration, prefix: &str) -> bool {
    if config.rules.is_empty() {
        return false;
    }

    for rule in config.rules.iter() {
        if rule.status == ExpirationStatus::from_static(ExpirationStatus::DISABLED) {
            continue;
        }
        let rule_prefix = lc_get_prefix(rule);
        if !prefix.is_empty() && !rule_prefix.is_empty() && !prefix.starts_with(&rule_prefix) && !rule_prefix.starts_with(prefix)
        {
            continue;
        }

        if let Some(e) = &rule.noncurrent_version_expiration {
            if let Some(true) = e.noncurrent_days.map(|d| d > 0) {
                return true;
            }
            if let Some(true) = e.newer_noncurrent_versions.map(|d| d > 0) {
                return true;
            }
        }

        if rule.noncurrent_version_transitions.is_some() {
            return true;
        }
        if let Some(true) = rule.expiration.as_ref().map(|e| e.date.is_some()) {
            return true;
        }

        if let Some(true) = rule.expiration.as_ref().map(|e| e.days.is_some()) {
            return true;
        }

        if let Some(Some(true)) = rule.expiration.as_ref().map(|e| e.expired_object_delete_marker) {
            return true;
        }

        if let Some(true) = rule.transitions.as_ref().map(|t| !t.is_empty()) {
            return true;
        }

        if rule.transitions.is_some() {
            return true;
        }
    }
    false
}

pub fn rep_has_active_rules(config: &ReplicationConfiguration, prefix: &str, recursive: bool) -> bool {
    if config.rules.is_empty() {
        return false;
    }

    for rule in config.rules.iter() {
        if rule
            .status
            .eq(&ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED))
        {
            continue;
        }
        if !prefix.is_empty() {
            if let Some(filter) = &rule.filter {
                if let Some(r_prefix) = &filter.prefix {
                    if !r_prefix.is_empty() {
                        // incoming prefix must be in rule prefix
                        if !recursive && !prefix.starts_with(r_prefix) {
                            continue;
                        }
                        // If recursive, we can skip this rule if it doesn't match the tested prefix or level below prefix
                        // does not match
                        if recursive && !r_prefix.starts_with(prefix) && !prefix.starts_with(r_prefix) {
                            continue;
                        }
                    }
                }
            }
        }
        return true;
    }
    false
}

pub type LocalDrive = Arc<LocalDisk>;
pub async fn scan_data_folder(
    disks: &[Option<DiskStore>],
    drive: LocalDrive,
    cache: &DataUsageCache,
    get_size_fn: GetSizeFn,
    heal_scan_mode: HealScanMode,
    should_sleep: ShouldSleepFn,
) -> disk::error::Result<DataUsageCache> {
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(DiskError::other("internal error: root scan attempted"));
    }

    let base_path = drive.to_string();
    let (update_path, close_disk) = current_path_updater(&base_path, &cache.info.name);
    let skip_heal = if *GLOBAL_IsErasure.read().await || cache.info.skip_healing {
        AtomicBool::new(true)
    } else {
        AtomicBool::new(false)
    };
    let mut s = FolderScanner {
        root: base_path,
        get_size: get_size_fn,
        old_cache: cache.clone(),
        new_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        update_cache: DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        },
        data_usage_scanner_debug: false,
        heal_object_select: 0,
        scan_mode: heal_scan_mode,
        updates: cache.info.updates.clone().unwrap(),
        last_update: SystemTime::now(),
        update_current_path: update_path,
        disks: disks.to_vec(),
        disks_quorum: disks.len() / 2,
        skip_heal,
        drive: drive.clone(),
        we_sleep: should_sleep,
    };

    if *GLOBAL_IsErasure.read().await || !cache.info.skip_healing {
        s.heal_object_select = HEAL_OBJECT_SELECT_PROB as u32;
    }

    let mut root = DataUsageEntry::default();
    let folder = CachedFolder {
        name: cache.info.name.clone(),
        object_heal_prob_div: 1,
        parent: DataUsageHash("".to_string()),
    };

    if s.scan_folder(&folder, &mut root).await.is_err() {
        close_disk().await;
    }
    s.new_cache.force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN as usize);
    s.new_cache.info.last_update = Some(SystemTime::now());
    s.new_cache.info.next_cycle = cache.info.next_cycle;
    close_disk().await;
    Ok(s.new_cache)
}

pub async fn eval_action_from_lifecycle(
    lc: &BucketLifecycleConfiguration,
    lr: Option<DefaultRetention>,
    rcfg: Option<(ReplicationConfiguration, OffsetDateTime)>,
    oi: &ObjectInfo,
) -> lifecycle::Event {
    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    //if serverDebugLog {
    info!("lifecycle: Secondary scan: {}", event.action);
    //}

    let lock_enabled = if let Some(lr) = lr { lr.mode.is_some() } else { false };

    match event.action {
        lifecycle::IlmAction::DeleteAllVersionsAction | lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => {
            if lock_enabled {
                return lifecycle::Event::default();
            }
        }
        lifecycle::IlmAction::DeleteVersionAction | lifecycle::IlmAction::DeleteRestoredVersionAction => {
            if oi.version_id.is_none() {
                return lifecycle::Event::default();
            }
            if lock_enabled && enforce_retention_for_deletion(oi) {
                //if serverDebugLog {
                if oi.version_id.is_some() {
                    info!("lifecycle: {} v({}) is locked, not deleting", oi.name, oi.version_id.expect("err"));
                } else {
                    info!("lifecycle: {} is locked, not deleting", oi.name);
                }
                //}
                return lifecycle::Event::default();
            }
            if let Some(rcfg) = rcfg {
                if rep_has_active_rules(&rcfg.0, &oi.name, true) {
                    return lifecycle::Event::default();
                }
            }
        }
        _ => (),
    }

    event
}

async fn apply_transition_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    if oi.delete_marker || oi.is_dir {
        return false;
    }
    GLOBAL_TransitionState.queue_transition_task(oi, event, src).await;
    true
}

pub async fn apply_expiry_on_transitioned_object(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    src: &LcEventSrc,
) -> bool {
    let time_ilm = ScannerMetrics::time_ilm(lc_event.action.clone());
    if let Err(_err) = expire_transitioned_object(api, oi, lc_event, src).await {
        return false;
    }
    let _ = time_ilm(1);

    true
}

pub async fn apply_expiry_on_non_transitioned_objects(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    _src: &LcEventSrc,
) -> bool {
    let mut opts = ObjectOptions {
        expiration: ExpirationOptions { expire: true },
        ..Default::default()
    };

    if lc_event.action.delete_versioned() {
        opts.version_id = Some(oi.version_id.expect("err").to_string());
    }

    opts.versioned = BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await;
    opts.version_suspended = BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await;

    if lc_event.action.delete_all() {
        opts.delete_prefix = true;
        opts.delete_prefix_object = true;
    }

    let time_ilm = ScannerMetrics::time_ilm(lc_event.action.clone());

    let mut dobj = api
        .delete_object(&oi.bucket, &encode_dir_object(&oi.name), opts)
        .await
        .unwrap();
    if dobj.name.is_empty() {
        dobj = oi.clone();
    }

    //let tags = LcAuditEvent::new(lc_event.clone(), src.clone()).tags();
    //tags["version-id"] = dobj.version_id;

    let mut event_name = EventName::ObjectRemovedDelete;
    if oi.delete_marker {
        event_name = EventName::ObjectRemovedDeleteMarkerCreated;
    }
    match lc_event.action {
        lifecycle::IlmAction::DeleteAllVersionsAction => event_name = EventName::ObjectRemovedDeleteAllVersions,
        lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => event_name = EventName::ILMDelMarkerExpirationDelete,
        _ => (),
    }
    send_event(EventArgs {
        event_name: event_name.as_ref().to_string(),
        bucket_name: dobj.bucket.clone(),
        object: dobj,
        user_agent: "Internal: [ILM-Expiry]".to_string(),
        host: GLOBAL_LocalNodeName.to_string(),
        ..Default::default()
    });

    if lc_event.action != lifecycle::IlmAction::NoneAction {
        let mut num_versions = 1_u64;
        if lc_event.action.delete_all() {
            num_versions = oi.num_versions as u64;
        }
        let _ = time_ilm(num_versions);
    }

    true
}

async fn apply_expiry_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut expiry_state = GLOBAL_ExpiryState.write().await;
    expiry_state.enqueue_by_days(oi, event, src).await;
    true
}

pub async fn apply_lifecycle_action(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut success = false;
    match event.action {
        lifecycle::IlmAction::DeleteVersionAction
        | lifecycle::IlmAction::DeleteAction
        | lifecycle::IlmAction::DeleteRestoredAction
        | lifecycle::IlmAction::DeleteRestoredVersionAction
        | lifecycle::IlmAction::DeleteAllVersionsAction
        | lifecycle::IlmAction::DelMarkerDeleteAllVersionsAction => {
            success = apply_expiry_rule(event, src, oi).await;
        }
        lifecycle::IlmAction::TransitionAction | lifecycle::IlmAction::TransitionVersionAction => {
            success = apply_transition_rule(event, src, oi).await;
        }
        _ => (),
    }
    success
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use chrono::Utc;
    use rmp_serde::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    use super::CurrentScannerCycle;

    #[test]
    fn test_current_cycle() {
        let cycle_info = CurrentScannerCycle {
            current: 0,
            next: 1,
            started: Utc::now(),
            cycle_completed: vec![Utc::now(), Utc::now()],
        };

        println!("{cycle_info:?}");

        let mut wr = Vec::new();
        cycle_info.serialize(&mut Serializer::new(&mut wr)).unwrap();

        let mut buf_t = Deserializer::new(Cursor::new(wr));
        let c: CurrentScannerCycle = Deserialize::deserialize(&mut buf_t).unwrap();

        println!("{c:?}");
    }
}
