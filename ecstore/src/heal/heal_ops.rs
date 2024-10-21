use crate::{
    disk::{endpoint::Endpoint, MetaCacheEntry},
    endpoints::Endpoints,
    error::{Error, Result},
    global::GLOBAL_IsDistErasure,
    heal::heal_commands::HEAL_UNKNOWN_SCAN,
    utils::path::has_profix,
};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    select, spawn,
    sync::{
        broadcast::{self, Receiver, Sender},
        mpsc::{self, Receiver as M_Receiver, Sender as M_Sender},
        RwLock,
    },
    time::{interval, sleep},
};
use uuid::Uuid;

use super::{
    background_heal_ops::HealTask,
    heal_commands::{HealItemType, HealOpts, HealResultItem, HealScanMode, HealStopSuccess, HealingDisk, HealingTracker},
};

type HealStatusSummary = String;
type ItemsMap = HashMap<HealItemType, usize>;
pub type HealObjectFn = Arc<dyn Fn(&str, &str, &str, HealScanMode) -> Result<()> + Send + Sync>;
pub type HealEntryFn =
    Box<dyn Fn(String, MetaCacheEntry, HealScanMode) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

pub const BG_HEALING_UUID: &str = "0000-0000-0000-0000";
pub const HEALING_TRACKER_FILENAME: &str = ".healing.bin";
const KEEP_HEAL_SEQ_STATE_DURATION: std::time::Duration = Duration::from_secs(10 * 60);
const HEAL_NOT_STARTED_STATUS: &str = "not started";
const HEAL_RUNNING_STATUS: &str = "running";
const HEAL_STOPPED_STATUS: &str = "stopped";
const HEAL_FINISHED_STATUS: &str = "finished";

const MAX_UNCONSUMED_HEAL_RESULT_ITEMS: usize = 1000;
const HEAL_UNCONSUMED_TIMEOUT: std::time::Duration = Duration::from_secs(24 * 60 * 60);
pub const NOP_HEAL: &str = "";

lazy_static! {}

#[derive(Clone, Debug, Default)]
pub struct HealSequenceStatus {
    pub summary: HealStatusSummary,
    pub failure_detail: String,
    pub start_time: u64,
    pub heal_setting: HealOpts,
    pub items: Vec<HealResultItem>,
}

pub struct HealSource {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub no_wait: bool,
    opts: Option<HealOpts>,
}

#[derive(Clone, Debug)]
pub struct HealSequence {
    pub bucket: String,
    pub object: String,
    pub report_progress: bool,
    pub start_time: u64,
    pub end_time: Arc<RwLock<u64>>,
    pub client_token: String,
    pub client_address: String,
    pub force_started: bool,
    pub setting: HealOpts,
    pub current_status: Arc<RwLock<HealSequenceStatus>>,
    pub last_sent_result_index: usize,
    pub scanned_items_map: ItemsMap,
    pub healed_items_map: ItemsMap,
    pub heal_failed_items_map: ItemsMap,
    pub last_heal_activity: u64,

    traverse_and_heal_done_tx: Arc<RwLock<M_Sender<Option<Error>>>>,
    traverse_and_heal_done_rx: Arc<RwLock<M_Receiver<Option<Error>>>>,

    tx: Arc<RwLock<Sender<bool>>>,
    rx: Arc<RwLock<Receiver<bool>>>,
}

impl Default for HealSequence {
    fn default() -> Self {
        let (h_tx, h_rx) = mpsc::channel(1);
        let (tx, rx) = broadcast::channel(1);
        Self {
            bucket: Default::default(),
            object: Default::default(),
            report_progress: Default::default(),
            start_time: Default::default(),
            end_time: Default::default(),
            client_token: Default::default(),
            client_address: Default::default(),
            force_started: Default::default(),
            setting: Default::default(),
            current_status: Default::default(),
            last_sent_result_index: Default::default(),
            scanned_items_map: Default::default(),
            healed_items_map: Default::default(),
            heal_failed_items_map: Default::default(),
            last_heal_activity: Default::default(),
            traverse_and_heal_done_tx: Arc::new(RwLock::new(h_tx)),
            traverse_and_heal_done_rx: Arc::new(RwLock::new(h_rx)),
            tx: Arc::new(RwLock::new(tx)),
            rx: Arc::new(RwLock::new(rx)),
        }
    }
}

impl HealSequence {
    pub fn new(bucket: &str, obj_profix: &str, client_addr: &str, hs: HealOpts, force_start: bool) -> Self {
        let client_token = Uuid::new_v4().to_string();

        Self {
            bucket: bucket.to_string(),
            object: obj_profix.to_string(),
            report_progress: true,
            client_token,
            client_address: client_addr.to_string(),
            force_started: force_start,
            setting: hs,
            current_status: Arc::new(RwLock::new(HealSequenceStatus {
                summary: HEAL_NOT_STARTED_STATUS.to_string(),
                heal_setting: hs,
                ..Default::default()
            })),
            ..Default::default()
        }
    }
}

impl HealSequence {
    fn get_scanned_items_count(&self) -> usize {
        self.scanned_items_map.values().sum()
    }

    fn get_scanned_items_map(&self) -> ItemsMap {
        self.scanned_items_map.clone()
    }

    fn get_healed_items_map(&self) -> ItemsMap {
        self.healed_items_map.clone()
    }

    fn get_heal_failed_items_map(&self) -> ItemsMap {
        self.heal_failed_items_map.clone()
    }

    fn count_failed(&mut self, heal_type: HealItemType) {
        *self.heal_failed_items_map.entry(heal_type).or_insert(0) += 1;
        self.last_heal_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    fn count_scanned(&mut self, heal_type: HealItemType) {
        *self.scanned_items_map.entry(heal_type).or_insert(0) += 1;
        self.last_heal_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    fn count_healed(&mut self, heal_type: HealItemType) {
        *self.healed_items_map.entry(heal_type).or_insert(0) += 1;
        self.last_heal_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    async fn is_quitting(&self) -> bool {
        let mut w = self.rx.write().await;
        if w.try_recv().is_ok() {
            return true;
        }
        false
    }

    async fn has_ended(&self) -> bool {
        if self.client_token == BG_HEALING_UUID.to_string() {
            return false;
        }

        !(*(self.end_time.read().await) == self.start_time)
    }

    async fn stop(&self) {
        let w = self.tx.write().await;
        w.send(true);
    }

    async fn push_heal_result_item(&self, r: &HealResultItem) -> Result<()> {
        let mut r = r.clone();
        let mut interval_timer = interval(HEAL_UNCONSUMED_TIMEOUT);
        let mut items_len = 0;
        loop {
            {
                let current_status_r = self.current_status.read().await;
                items_len = current_status_r.items.len();
            }

            if items_len == MAX_UNCONSUMED_HEAL_RESULT_ITEMS {
                select! {
                    _ = sleep(Duration::from_secs(1)) => {

                    }
                    _ = self.is_done() => {
                        return Err(Error::from_string("stopped"));
                    }
                    _ = interval_timer.tick() => {
                        return Err(Error::from_string("timeout"));
                    }
                }
            } else {
                break;
            }
        }

        let mut current_status_w = self.current_status.write().await;
        if items_len > 0 {
            r.result_index = 1 + current_status_w.items[items_len - 1].result_index;
        } else {
            r.result_index = 1 + self.last_sent_result_index;
        }

        current_status_w.items.push(r);

        Ok(())
    }

    async fn queue_heal_task(&mut self, source: HealSource, heal_type: HealItemType) -> Result<()> {
        let mut task = HealTask::new(&source.bucket, &source.object, &source.version_id, &self.setting);
        if let Some(opts) = source.opts {
            task.opts = opts;
        } else {
            task.opts.scan_mode = HEAL_UNKNOWN_SCAN;
        }

        self.count_scanned(heal_type);

        if source.no_wait {}

        todo!()
    }

    fn heal_disk_meta() -> Result<()> {
        todo!()
    }

    fn heal_items(&self, buckets_only: bool) -> Result<()> {
        if self.client_token == BG_HEALING_UUID.to_string() {
            return Ok(());
        }

        todo!()
    }

    async fn traverse_and_heal(&self) {
        let buckets_only = false;
    }

    fn heal_minio_sys_meta(&self, meta_prefix: String) -> Result<()> {
        todo!()
    }

    async fn is_done(&self) -> bool {
        let mut rx_w = self.rx.write().await;
        if let Ok(true) = rx_w.recv().await {
            return true;
        }
        false
    }
}

pub async fn heal_sequence_start(h: Arc<HealSequence>) {
    {
        let mut current_status_w = h.current_status.write().await;
        (*current_status_w).summary = HEAL_RUNNING_STATUS.to_string();
        (*current_status_w).start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    let h_clone = h.clone();
    spawn(async move {
        h_clone.traverse_and_heal().await;
    });

    let h_clone_1 = h.clone();
    let mut x = h.traverse_and_heal_done_rx.write().await;
    select! {
        _ = h.is_done() => {
            *(h.end_time.write().await) = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut current_status_w = h.current_status.write().await;
        (*current_status_w).summary = HEAL_FINISHED_STATUS.to_string();

        spawn(async move {
            let mut rx_w = h_clone_1.traverse_and_heal_done_rx.write().await;
            rx_w.recv().await;
            });
        }
        result = x.recv() => {
            match result {
                Some(err) => {
                    match err {
                        Some(err) => {
                            let mut current_status_w = h.current_status.write().await;
                            (*current_status_w).summary = HEAL_STOPPED_STATUS.to_string();
                            (*current_status_w).failure_detail = err.to_string();
                        },
                        None => {
                            let mut current_status_w = h.current_status.write().await;
                            (*current_status_w).summary = HEAL_FINISHED_STATUS.to_string();
                        }
                    }
                },
                None => {
                    return;
                }
            }

        }
    }
}

#[derive(Debug, Default)]
pub struct AllHealState {
    mu: RwLock<bool>,

    heal_seq_map: HashMap<String, HealSequence>,
    heal_local_disks: HashMap<Endpoint, bool>,
    heal_status: HashMap<String, HealingTracker>,
}

impl AllHealState {
    pub fn new(cleanup: bool) -> Self {
        let hstate = AllHealState::default();
        if cleanup {
            // spawn(f);
        }

        hstate
    }

    async fn pop_heal_local_disks(&mut self, heal_local_disks: &[Endpoint]) {
        self.mu.write().await;

        self.heal_local_disks.retain(|k, _| {
            if heal_local_disks.contains(k) {
                return false;
            }
            true
        });

        let heal_local_disks = heal_local_disks.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        self.heal_status.retain(|_, v| {
            if heal_local_disks.contains(&v.endpoint) {
                return false;
            }

            true
        });
    }

    async fn update_heal_status(&mut self, tracker: &HealingTracker) {
        self.mu.write().await;
        tracker.mu.read().await;

        self.heal_status.insert(tracker.id.clone(), tracker.clone());
    }

    async fn get_local_healing_disks(&self) -> HashMap<String, HealingDisk> {
        self.mu.read().await;

        let mut dst = HashMap::new();
        for v in self.heal_status.values() {
            dst.insert(v.endpoint.clone(), v.to_healing_disk().await);
        }

        dst
    }

    async fn get_heal_local_disk_endpoints(&self) -> Endpoints {
        self.mu.read().await;

        let mut endpoints = Vec::new();
        self.heal_local_disks.iter().for_each(|(k, v)| {
            if !v {
                endpoints.push(k.clone());
            }
        });

        Endpoints::from(endpoints)
    }

    async fn set_disk_healing_status(&mut self, ep: Endpoint, healing: bool) {
        self.mu.write().await;

        self.heal_local_disks.insert(ep, healing);
    }

    async fn push_heal_local_disks(&mut self, heal_local_disks: &[Endpoint]) {
        self.mu.write().await;

        heal_local_disks.iter().for_each(|heal_local_disk| {
            self.heal_local_disks.insert(heal_local_disk.clone(), false);
        });
    }

    async fn periodic_heal_seqs_clean(&mut self, mut rx: Receiver<bool>) {
        loop {
            select! {
                result = rx.recv() =>{
                    if let Ok(true) = result {
                        return;
                    }
                }
                _ = sleep(Duration::from_secs(5 * 60)) => {
                    self.mu.write().await;
                    let now = SystemTime::now();
                    
                    let mut keys_to_reomve = Vec::new();
                    for (k, v) in self.heal_seq_map.iter() {
                        if v.has_ended().await && (UNIX_EPOCH + Duration::from_secs(*(v.end_time.read().await)) + KEEP_HEAL_SEQ_STATE_DURATION) < now {
                            keys_to_reomve.push(k.clone())
                        }
                    }
                    for key in keys_to_reomve.iter() {
                        self.heal_seq_map.remove(key);
                    }
                }
            }
        }
    }

    async fn get_heal_sequence_by_token(&self, token: &str) -> (Option<HealSequence>, bool) {
        self.mu.read().await;

        for v in self.heal_seq_map.values() {
            if v.client_token == token {
                return (Some(v.clone()), true);
            }
        }

        return (None, false);
    }

    async fn get_heal_sequence(&self, path: &str) -> Option<HealSequence> {
        self.mu.read().await;

        self.heal_seq_map.get(path).cloned()
    }

    async fn stop_heal_sequence(&mut self, path: &str) -> Result<Vec<u8>> {
        let mut hsp = HealStopSuccess::default();
        if let Some(he) = self.get_heal_sequence(path).await {
            let client_token = he.client_token.clone();
            if *GLOBAL_IsDistErasure.read().await {
                // TODO: proxy
            }

            hsp.client_token = client_token;
            hsp.client_address = he.client_address.clone();
            hsp.start_time = he.start_time;

            he.stop();

            loop {
                if he.has_ended().await {
                    break;
                }

                sleep(Duration::from_secs(1)).await;
            }

            self.mu.write().await;
            self.heal_seq_map.remove(path);
        } else {
            hsp.client_token = "unknown".to_string();
        }

        let b = serde_json::to_string(&hsp)?;
        Ok(b.as_bytes().to_vec())
    }

    // LaunchNewHealSequence - launches a background routine that performs
    // healing according to the healSequence argument. For each heal
    // sequence, state is stored in the `globalAllHealState`, which is a
    // map of the heal path to `healSequence` which holds state about the
    // heal sequence.
    //
    // Heal results are persisted in server memory for
    // `keepHealSeqStateDuration`. This function also launches a
    // background routine to clean up heal results after the
    // aforementioned duration.
    pub async fn launch_new_heal_sequence(&mut self, heal_sequence: &HealSequence) -> Result<Vec<u8>> {
        let path = Path::new(&heal_sequence.bucket).join(heal_sequence.object.clone());
        let path_s = path.to_str().unwrap();
        if heal_sequence.force_started {
            self.stop_heal_sequence(path_s).await?;
        } else {
            if let Some(hs) = self.get_heal_sequence(path_s).await {
                if !hs.has_ended().await {
                    return Err(Error::from_string(format!("Heal is already running on the given path (use force-start option to stop and start afresh). The heal was started by IP {} at {}, token is {}", heal_sequence.client_address, heal_sequence.start_time, heal_sequence.client_token)));
                }
            }
        }

        self.mu.write().await;

        for (k, v) in self.heal_seq_map.iter() {
            if !v.has_ended().await && (has_profix(&k, path_s) || has_profix(path_s, &k)) {
                return Err(Error::from_string(format!(
                    "The provided heal sequence path overlaps with an existing heal path: {}",
                    k
                )));
            }
        }

        self.heal_seq_map.insert(path_s.to_string(), heal_sequence.clone());

        let client_token = heal_sequence.client_token.clone();
        if *GLOBAL_IsDistErasure.read().await {
            // TODO: proxy
        }

        if heal_sequence.client_token == BG_HEALING_UUID {
            // For background heal do nothing, do not spawn an unnecessary goroutine.
        } else {
        }
        todo!()
    }
}
