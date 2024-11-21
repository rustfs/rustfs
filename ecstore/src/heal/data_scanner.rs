use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use rand::Rng;
use rmp_serde::{Deserializer, Serializer};
use s3s::dto::{ReplicationConfiguration, ReplicationRuleStatus};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, Sender},
        RwLock,
    },
    time::sleep,
};
use tracing::{error, info};

use super::{
    data_scanner_metric::{globalScannerMetrics, ScannerMetric, ScannerMetrics},
    data_usage::{store_data_usage_in_backend, DATA_USAGE_BLOOM_NAME_PATH},
    data_usage_cache::{DataUsageCache, DataUsageEntry, DataUsageHash},
    heal_commands::{HealScanMode, HEAL_DEEP_SCAN, HEAL_NORMAL_SCAN},
};
use crate::heal::data_usage::DATA_USAGE_ROOT;
use crate::{
    cache_value::metacache_set::{list_path_raw, ListPathRawOptions},
    config::{
        common::{read_config, save_config},
        heal::Config,
    },
    disk::{error::DiskError, DiskInfoOptions, DiskStore, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams},
    error::{Error, Result},
    global::{GLOBAL_BackgroundHealState, GLOBAL_IsErasure, GLOBAL_IsErasureSD},
    heal::{
        data_usage::BACKGROUND_HEAL_INFO_PATH,
        data_usage_cache::{hash_path, DataUsageHashMap},
        error::ERR_IGNORE_FILE_CONTRIB,
        heal_commands::{HEAL_ITEM_BUCKET, HEAL_ITEM_OBJECT},
        heal_ops::{HealSource, BG_HEALING_UUID},
    },
    new_object_layer_fn,
    peer::is_reserved_or_invalid_bucket,
    store::ECStore,
    utils::path::{path_join, path_to_bucket_object, path_to_bucket_object_with_base_path, SLASH_SEPARATOR},
};
use crate::{disk::local::LocalDisk, heal::data_scanner_metric::current_path_updater};
use crate::{
    disk::DiskAPI,
    store_api::{FileInfo, ObjectInfo},
};

const _DATA_SCANNER_SLEEP_PER_FOLDER: Duration = Duration::from_millis(1); // Time to wait between folders.
const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16; // Visit all folders every n cycles.
const DATA_SCANNER_COMPACT_LEAST_OBJECT: u64 = 500; // Compact when there are less than this many objects in a branch.
const DATA_SCANNER_COMPACT_AT_CHILDREN: u64 = 10000; // Compact when there are this many children in a branch.
const DATA_SCANNER_COMPACT_AT_FOLDERS: u64 = DATA_SCANNER_COMPACT_AT_CHILDREN / 4; // Compact when this many subfolders in a single folder.
pub const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: u64 = 250_000; // Compact when this many subfolders in a single folder (even top level).
const DATA_SCANNER_START_DELAY: Duration = Duration::from_secs(60); // Time to wait on startup and between cycles.

pub const HEAL_DELETE_DANGLING: bool = true;
const HEAL_OBJECT_SELECT_PROB: u64 = 1024; // Overall probability of a file being scanned; one in n.

// static SCANNER_SLEEPER: () = new_dynamic_sleeper(2, Duration::from_secs(1), true); // Keep defaults same as config defaults
static SCANNER_CYCLE: AtomicU64 = AtomicU64::new(DATA_SCANNER_START_DELAY.as_secs());
static _SCANNER_IDLE_MODE: AtomicU32 = AtomicU32::new(0); // default is throttled when idle
static SCANNER_EXCESS_OBJECT_VERSIONS: AtomicU64 = AtomicU64::new(100);
static SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE: AtomicU64 = AtomicU64::new(1024 * 1024 * 1024 * 1024); // 1 TB
static SCANNER_EXCESS_FOLDERS: AtomicU64 = AtomicU64::new(50_000);

lazy_static! {
    pub static ref globalHealConfig: Arc<RwLock<Config>> = Arc::new(RwLock::new(Config::default()));
}

pub async fn init_data_scanner() {
    let mut r = rand::thread_rng();
    let random = r.gen_range(0.0..1.0);
    tokio::spawn(async move {
        loop {
            run_data_scanner().await;
            let duration = Duration::from_secs_f64(random * (SCANNER_CYCLE.load(std::sync::atomic::Ordering::SeqCst) as f64));
            let sleep_duration = if duration < Duration::new(1, 0) {
                Duration::new(1, 0)
            } else {
                duration
            };
            sleep(sleep_duration).await;
        }
    });
}

async fn run_data_scanner() {
    let Some(store) = new_object_layer_fn() else {
        error!("errServerNotInitialized");
        return;
    };

    let mut cycle_info = CurrentScannerCycle::default();

    let mut buf = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .map_or(Vec::new(), |buf| buf);
    match buf.len().cmp(&8) {
        std::cmp::Ordering::Less => {}
        std::cmp::Ordering::Equal => {
            cycle_info.next = match Cursor::new(buf).read_u64::<LittleEndian>() {
                Ok(buf) => buf,
                Err(_) => {
                    error!("can not decode DATA_USAGE_BLOOM_NAME_PATH");
                    return;
                }
            };
        }
        std::cmp::Ordering::Greater => {
            cycle_info.next = match Cursor::new(buf[..8].to_vec()).read_u64::<LittleEndian>() {
                Ok(buf) => buf,
                Err(_) => {
                    error!("can not decode DATA_USAGE_BLOOM_NAME_PATH");
                    return;
                }
            };
            let _ = cycle_info.unmarshal_msg(&buf.split_off(8));
        }
    }

    loop {
        let stop_fn = ScannerMetrics::log(ScannerMetric::ScanCycle);
        cycle_info.current = cycle_info.next;
        cycle_info.started = SystemTime::now();
        {
            globalScannerMetrics.write().await.set_cycle(Some(cycle_info.clone())).await;
        }

        let bg_heal_info = read_background_heal_info(store.clone()).await;
        let scan_mode =
            get_cycle_scan_mode(cycle_info.current, bg_heal_info.bitrot_start_cycle, bg_heal_info.bitrot_start_time).await;
        if bg_heal_info.current_scan_mode != scan_mode {
            let mut new_heal_info = bg_heal_info;
            new_heal_info.current_scan_mode = scan_mode;
            if scan_mode == HEAL_DEEP_SCAN {
                new_heal_info.bitrot_start_time = SystemTime::now();
                new_heal_info.bitrot_start_cycle = cycle_info.current;
            }
            save_background_heal_info(store.clone(), &new_heal_info).await;
        }
        // Wait before starting next cycle and wait on startup.
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async {
            store_data_usage_in_backend(rx).await;
        });
        let mut res = HashMap::new();
        res.insert("cycle".to_string(), cycle_info.current.to_string());
        match store.clone().ns_scanner(tx, cycle_info.current as usize, scan_mode).await {
            Ok(_) => {
                cycle_info.next += 1;
                cycle_info.current = 0;
                cycle_info.cycle_completed.push(SystemTime::now());
                if cycle_info.cycle_completed.len() > DATA_USAGE_UPDATE_DIR_CYCLES as usize {
                    cycle_info.cycle_completed = cycle_info.cycle_completed
                        [cycle_info.cycle_completed.len() - DATA_USAGE_UPDATE_DIR_CYCLES as usize..]
                        .to_vec();
                }
                globalScannerMetrics.write().await.set_cycle(Some(cycle_info.clone())).await;
                let mut tmp = Vec::new();
                tmp.write_u64::<LittleEndian>(cycle_info.next).unwrap();
                let _ = save_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH, &tmp).await;
            }
            Err(err) => {
                res.insert("error".to_string(), err.to_string());
            }
        }
        stop_fn(&res).await;
        sleep(Duration::from_secs(SCANNER_CYCLE.load(std::sync::atomic::Ordering::SeqCst))).await;
    }
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
    let _ = save_config(store, &BACKGROUND_HEAL_INFO_PATH, &b).await;
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

#[derive(Clone, Debug)]
pub struct CurrentScannerCycle {
    pub current: u64,
    pub next: u64,
    pub started: SystemTime,
    pub cycle_completed: Vec<SystemTime>,
}

impl Default for CurrentScannerCycle {
    fn default() -> Self {
        Self {
            current: Default::default(),
            next: Default::default(),
            started: SystemTime::now(),
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
        rmp::encode::write_uint(&mut wr, system_time_to_timestamp(&self.started))?;

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

    #[tracing::instrument]
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);

        let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        while fields_len > 0 {
            fields_len -= 1;

            let str_len = rmp::decode::read_str_len(&mut cur)?;

            // ！！！ Vec::with_capacity(str_len) 失败，vec!正常
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
                    let u: u64 = rmp::decode::read_int(&mut cur)?;
                    let started = timestamp_to_system_time(u);
                    self.started = started;
                }
                "cycleCompleted" => {
                    let mut buf = Vec::new();
                    let _ = cur.read_to_end(&mut buf)?;
                    let u: Vec<SystemTime> =
                        Deserialize::deserialize(&mut Deserializer::new(&buf[..])).expect("Deserialization failed");
                    self.cycle_completed = u;
                }
                name => return Err(Error::msg(format!("not suport field name {}", name))),
            }
        }

        Ok(cur.position())
    }
}

// 将 SystemTime 转换为时间戳
fn system_time_to_timestamp(time: &SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()
}

// 将时间戳转换为 SystemTime
fn timestamp_to_system_time(timestamp: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::new(timestamp, 0)
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
    // todo: lifecycle
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

    pub async fn apply_versions_actions(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let obj_infos = self.apply_newer_noncurrent_version_limit(fivs).await?;
        if obj_infos.len() >= SCANNER_EXCESS_OBJECT_VERSIONS.load(Ordering::SeqCst).try_into().unwrap() {
            // todo
        }

        let mut cumulative_size = 0;
        for obj_info in obj_infos.iter() {
            cumulative_size += obj_info.size;
        }

        if cumulative_size
            >= SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE
                .load(Ordering::SeqCst)
                .try_into()
                .unwrap()
        {
            //todo
        }

        Ok(obj_infos)
    }

    pub async fn apply_newer_noncurrent_version_limit(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let done = ScannerMetrics::time(ScannerMetric::ApplyNonCurrent);
        let mut object_infos = Vec::new();
        for info in fivs.iter() {
            object_infos.push(info.to_object_info(&self.bucket, &self.object_path().to_string_lossy(), false));
        }
        done().await;

        Ok(object_infos)
    }

    pub async fn apply_actions(&self, _oi: &ObjectInfo, _size_s: &SizeSummary) -> (bool, usize) {
        let done = ScannerMetrics::time(ScannerMetric::Ilm);
        //todo: lifecycle
        done().await;

        (false, 0)
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
    Box<dyn Fn(&ScannerItem) -> Pin<Box<dyn Future<Output = Result<SizeSummary>> + Send>> + Send + Sync + 'static>;
pub type UpdateCurrentPathFn = Arc<dyn Fn(&str) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

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
            let replication_cfg = if self.old_cache.info.replication.is_some()
                && has_active_rules(self.old_cache.info.replication.as_ref().unwrap(), &prefix, true)
            {
                self.old_cache.info.replication.clone()
            } else {
                None
            };

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

                    if !sub_path.is_dir() {
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
            if found_objects && *GLOBAL_IsErasure.read().await {
                // If we found an object in erasure mode, we skip subdirs (only datadirs)...
                break 'outer;
            }

            let should_compact = self.new_cache.info.name != folder.name
                && existing_folders.len() + new_folders.len() >= DATA_SCANNER_COMPACT_AT_FOLDERS.try_into().unwrap()
                || existing_folders.len() + new_folders.len() >= DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS.try_into().unwrap();

            let total_folders = existing_folders.len() + new_folders.len();
            if total_folders
                > SCANNER_EXCESS_FOLDERS
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .try_into()
                    .unwrap()
            {
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

            let (bg_seq, found) = GLOBAL_BackgroundHealState
                .read()
                .await
                .get_heal_sequence_by_token(BG_HEALING_UUID)
                .await;
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
                        .write()
                        .await
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
                    partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
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
                                    Ok(Some(entry)) => entry,
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
                                            .write()
                                            .await
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
                                            match err.downcast_ref() {
                                                Some(DiskError::FileNotFound) | Some(DiskError::FileVersionNotFound) => {}
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
                                        .write()
                                        .await
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
                    finished: Some(Box::new(move |_: &[Option<Error>]| {
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
                    let this = CachedFolder {
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
            let compact = if flat.objects < DATA_SCANNER_COMPACT_LEAST_OBJECT.try_into().unwrap() {
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
                DATA_SCANNER_COMPACT_AT_CHILDREN.try_into().unwrap(),
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

pub fn has_active_rules(config: &ReplicationConfiguration, prefix: &str, recursive: bool) -> bool {
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
) -> Result<DataUsageCache> {
    if cache.info.name.is_empty() || cache.info.name == DATA_USAGE_ROOT {
        return Err(Error::from_string("internal error: root scan attempted"));
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
        new_cache: DataUsageCache::default(),
        update_cache: DataUsageCache::default(),
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
    s.new_cache
        .force_compact(DATA_SCANNER_COMPACT_AT_CHILDREN.try_into().unwrap());
    s.new_cache.info.last_update = Some(SystemTime::now());
    s.new_cache.info.next_cycle = cache.info.next_cycle;
    close_disk().await;
    Ok(s.new_cache)
}

// pub fn eval_action_from_lifecycle(lc: &BucketLifecycleConfiguration, lr: &ObjectLockConfiguration, rcfg: &ReplicationConfiguration， obj: &ObjectInfo)
