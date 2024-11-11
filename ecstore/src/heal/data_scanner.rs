use std::{
    io::{Cursor, Read},
    sync::{atomic::{AtomicU32, AtomicU64}, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use byteorder::{LittleEndian, ReadBytesExt};
use lazy_static::lazy_static;
use rand::Rng;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::{sync::{mpsc, RwLock}, time::sleep};
use tracing::{error, info};

use crate::{
    config::{common::{read_config, save_config}, heal::Config},
    error::{Error, Result},
    global::GLOBAL_IsErasureSD,
    heal::data_usage::BACKGROUND_HEAL_INFO_PATH,
    new_object_layer_fn,
    store::ECStore,
};

use super::{data_scanner_metric::globalScannerMetrics, data_usage::{store_data_usage_in_backend, DATA_USAGE_BLOOM_NAME_PATH}, heal_commands::{HealScanMode, HEAL_DEEP_SCAN, HEAL_NORMAL_SCAN}};

const DATA_SCANNER_SLEEP_PER_FOLDER: Duration = Duration::from_millis(1); // Time to wait between folders.
const DATA_USAGE_UPDATE_DIR_CYCLES: u32 = 16; // Visit all folders every n cycles.
const DATA_SCANNER_COMPACT_LEAST_OBJECT: u64 = 500; // Compact when there are less than this many objects in a branch.
const DATA_SCANNER_COMPACT_AT_CHILDREN: u64 = 10000; // Compact when there are this many children in a branch.
const DATA_SCANNER_COMPACT_AT_FOLDERS: u64 = DATA_SCANNER_COMPACT_AT_CHILDREN / 4; // Compact when this many subfolders in a single folder.
const DATA_SCANNER_FORCE_COMPACT_AT_FOLDERS: u64 = 250_000; // Compact when this many subfolders in a single folder (even top level).
const DATA_SCANNER_START_DELAY: Duration = Duration::from_secs(60); // Time to wait on startup and between cycles.

const HEAL_DELETE_DANGLING: bool = true;
const HEAL_OBJECT_SELECT_PROB: u64 = 1024; // Overall probability of a file being scanned; one in n.

// static SCANNER_SLEEPER: () = new_dynamic_sleeper(2, Duration::from_secs(1), true); // Keep defaults same as config defaults
static SCANNER_CYCLE: AtomicU64 = AtomicU64::new(DATA_SCANNER_START_DELAY.as_secs());
static SCANNER_IDLE_MODE: AtomicU32 = AtomicU32::new(0); // default is throttled when idle
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
    let mut cycle_info = CurrentScannerCycle::default();
    let layer = new_object_layer_fn();
    let lock = layer.read().await;
    let store = match lock.as_ref() {
        Some(s) => s,
        None => {
            info!("errServerNotInitialized");
            return;
        }
    };
    let mut buf = read_config(store, &DATA_USAGE_BLOOM_NAME_PATH)
        .await
        .map_or(Vec::new(), |buf| buf);
    if buf.len() == 8 {
        cycle_info.next = match Cursor::new(buf).read_u64::<LittleEndian>() {
            Ok(buf) => buf,
            Err(_) => {
                error!("can not decode DATA_USAGE_BLOOM_NAME_PATH");
                return;
            }
        };
    } else if buf.len() > 8 {
        cycle_info.next = match Cursor::new(buf[..8].to_vec()).read_u64::<LittleEndian>() {
            Ok(buf) => buf,
            Err(_) => {
                error!("can not decode DATA_USAGE_BLOOM_NAME_PATH");
                return;
            }
        };
        let _ = cycle_info.unmarshal_msg(&buf.split_off(8));
    }

    loop {
        cycle_info.current = cycle_info.next;
        cycle_info.started = SystemTime::now();
        {
            globalScannerMetrics.write().await.set_cycle(Some(cycle_info.clone())).await;
        }

        let bg_heal_info = read_background_heal_info(store).await;
        let scan_mode = get_cycle_scan_mode(cycle_info.current, bg_heal_info.bitrot_start_cycle, bg_heal_info.bitrot_start_time).await;
        if bg_heal_info.current_scan_mode != scan_mode {
            let mut new_heal_info = bg_heal_info;
            new_heal_info.current_scan_mode = scan_mode;
            if scan_mode == HEAL_DEEP_SCAN {
                new_heal_info.bitrot_start_time = SystemTime::now();
                new_heal_info.bitrot_start_cycle = cycle_info.current;
            }
            save_background_heal_info(store, &new_heal_info).await;
        }
        // Wait before starting next cycle and wait on startup.
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async {
            store_data_usage_in_backend(rx).await;
        });
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

async fn read_background_heal_info(store: &ECStore) -> BackgroundHealInfo {
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

async fn save_background_heal_info(store: &ECStore, info: &BackgroundHealInfo) {
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
    let v = bitrot_cycle.as_secs_f64() ;
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
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
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

        Ok(wr)
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

                "next" => {
                    let u: u64 = rmp::decode::read_int(&mut cur)?;
                    self.next = u;
                }
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
