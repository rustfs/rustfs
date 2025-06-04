use super::{
    background_heal_ops::HealTask,
    data_scanner::HEAL_DELETE_DANGLING,
    error::ERR_SKIP_FILE,
    heal_commands::{HealOpts, HealScanMode, HealStopSuccess, HealingTracker, HEAL_ITEM_BUCKET_METADATA},
};
use crate::error::{Error, Result};
use crate::store_api::StorageAPI;
use crate::{
    config::com::CONFIG_PREFIX,
    disk::RUSTFS_META_BUCKET,
    global::GLOBAL_BackgroundHealRoutine,
    heal::{error::ERR_HEAL_STOP_SIGNALLED, heal_commands::DRIVE_STATE_OK},
};
use crate::{
    disk::endpoint::Endpoint,
    endpoints::Endpoints,
    global::GLOBAL_IsDistErasure,
    heal::heal_commands::{HealStartSuccess, HEAL_UNKNOWN_SCAN},
    new_object_layer_fn,
    utils::path::has_prefix,
};
use crate::{
    heal::heal_commands::{HEAL_ITEM_BUCKET, HEAL_ITEM_OBJECT},
    utils::path::path_join,
};
use chrono::Utc;
use futures::join;
use lazy_static::lazy_static;
use madmin::heal_commands::{HealDriveInfo, HealItemType, HealResultItem};
use rustfs_filemeta::MetaCacheEntry;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    select, spawn,
    sync::{
        broadcast,
        mpsc::{self, Receiver as M_Receiver, Sender as M_Sender},
        watch::{self, Receiver as W_Receiver, Sender as W_Sender},
        RwLock,
    },
    time::{interval, sleep},
};
use tracing::{error, info};
use uuid::Uuid;

type HealStatusSummary = String;
type ItemsMap = HashMap<HealItemType, usize>;
pub type HealEntryFn =
    Arc<dyn Fn(String, MetaCacheEntry, HealScanMode) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

pub const BG_HEALING_UUID: &str = "0000-0000-0000-0000";
pub const HEALING_TRACKER_FILENAME: &str = ".healing.bin";
const KEEP_HEAL_SEQ_STATE_DURATION: Duration = Duration::from_secs(10 * 60);
const HEAL_NOT_STARTED_STATUS: &str = "not started";
const HEAL_RUNNING_STATUS: &str = "running";
const HEAL_STOPPED_STATUS: &str = "stopped";
const HEAL_FINISHED_STATUS: &str = "finished";

pub const RUSTFS_RESERVED_BUCKET: &str = "rustfs";
pub const RUSTFS_RESERVED_BUCKET_PATH: &str = "/rustfs";
pub const LOGIN_PATH_PREFIX: &str = "/login";

const MAX_UNCONSUMED_HEAL_RESULT_ITEMS: usize = 1000;
const HEAL_UNCONSUMED_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
pub const NOP_HEAL: &str = "";

lazy_static! {}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealSequenceStatus {
    pub summary: HealStatusSummary,
    pub failure_detail: String,
    pub start_time: u64,
    pub heal_setting: HealOpts,
    pub items: Vec<HealResultItem>,
}

#[derive(Debug, Default)]
pub struct HealSource {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub no_wait: bool,
    pub opts: Option<HealOpts>,
}

#[derive(Debug)]
pub struct HealSequence {
    pub bucket: String,
    pub object: String,
    pub report_progress: bool,
    pub start_time: SystemTime,
    pub end_time: Arc<RwLock<SystemTime>>,
    pub client_token: String,
    pub client_address: String,
    pub force_started: bool,
    pub setting: HealOpts,
    pub current_status: Arc<RwLock<HealSequenceStatus>>,
    pub last_sent_result_index: RwLock<usize>,
    pub scanned_items_map: RwLock<ItemsMap>,
    pub healed_items_map: RwLock<ItemsMap>,
    pub heal_failed_items_map: RwLock<ItemsMap>,
    pub last_heal_activity: RwLock<SystemTime>,

    traverse_and_heal_done_tx: Arc<RwLock<M_Sender<Option<Error>>>>,
    traverse_and_heal_done_rx: Arc<RwLock<M_Receiver<Option<Error>>>>,

    tx: W_Sender<bool>,
    rx: W_Receiver<bool>,
}

pub fn new_bg_heal_sequence() -> HealSequence {
    let hs = HealOpts {
        remove: HEAL_DELETE_DANGLING,
        ..Default::default()
    };

    HealSequence {
        start_time: SystemTime::now(),
        client_token: BG_HEALING_UUID.to_string(),
        bucket: RUSTFS_RESERVED_BUCKET.to_string(),
        setting: hs,
        current_status: Arc::new(RwLock::new(HealSequenceStatus {
            summary: HEAL_NOT_STARTED_STATUS.to_string(),
            heal_setting: hs,
            ..Default::default()
        })),
        report_progress: false,
        scanned_items_map: HashMap::new().into(),
        healed_items_map: HashMap::new().into(),
        heal_failed_items_map: HashMap::new().into(),
        ..Default::default()
    }
}

pub fn new_heal_sequence(bucket: &str, obj_prefix: &str, client_addr: &str, hs: HealOpts, force_start: bool) -> HealSequence {
    let client_token = Uuid::new_v4().to_string();
    let (tx, rx) = mpsc::channel(10);
    HealSequence {
        bucket: bucket.to_string(),
        object: obj_prefix.to_string(),
        report_progress: true,
        start_time: SystemTime::now(),
        client_token,
        client_address: client_addr.to_string(),
        force_started: force_start,
        setting: hs,
        current_status: Arc::new(RwLock::new(HealSequenceStatus {
            summary: HEAL_NOT_STARTED_STATUS.to_string(),
            heal_setting: hs,
            ..Default::default()
        })),
        traverse_and_heal_done_tx: Arc::new(RwLock::new(tx)),
        traverse_and_heal_done_rx: Arc::new(RwLock::new(rx)),
        scanned_items_map: HashMap::new().into(),
        healed_items_map: HashMap::new().into(),
        heal_failed_items_map: HashMap::new().into(),
        ..Default::default()
    }
}

impl Default for HealSequence {
    fn default() -> Self {
        let (h_tx, h_rx) = mpsc::channel(1);
        let (tx, rx) = watch::channel(false);
        Self {
            bucket: Default::default(),
            object: Default::default(),
            report_progress: Default::default(),
            start_time: SystemTime::now(),
            end_time: Arc::new(RwLock::new(SystemTime::now())),
            client_token: Default::default(),
            client_address: Default::default(),
            force_started: Default::default(),
            setting: Default::default(),
            current_status: Default::default(),
            last_sent_result_index: Default::default(),
            scanned_items_map: Default::default(),
            healed_items_map: Default::default(),
            heal_failed_items_map: Default::default(),
            last_heal_activity: RwLock::new(SystemTime::now()),
            traverse_and_heal_done_tx: Arc::new(RwLock::new(h_tx)),
            traverse_and_heal_done_rx: Arc::new(RwLock::new(h_rx)),
            tx,
            rx,
        }
    }
}

impl HealSequence {
    pub fn new(bucket: &str, obj_prefix: &str, client_addr: &str, hs: HealOpts, force_start: bool) -> Self {
        let client_token = Uuid::new_v4().to_string();

        Self {
            bucket: bucket.to_string(),
            object: obj_prefix.to_string(),
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
    pub async fn get_scanned_items_count(&self) -> usize {
        self.scanned_items_map.read().await.values().sum()
    }

    async fn _get_scanned_items_map(&self) -> ItemsMap {
        self.scanned_items_map.read().await.clone()
    }

    async fn _get_healed_items_map(&self) -> ItemsMap {
        self.healed_items_map.read().await.clone()
    }

    async fn _get_heal_failed_items_map(&self) -> ItemsMap {
        self.heal_failed_items_map.read().await.clone()
    }

    pub async fn count_failed(&self, heal_type: HealItemType) {
        *self.heal_failed_items_map.write().await.entry(heal_type).or_insert(0) += 1;
        *self.last_heal_activity.write().await = SystemTime::now();
    }

    pub async fn count_scanned(&self, heal_type: HealItemType) {
        *self.scanned_items_map.write().await.entry(heal_type).or_insert(0) += 1;
        *self.last_heal_activity.write().await = SystemTime::now();
    }

    pub async fn count_healed(&self, heal_type: HealItemType) {
        *self.healed_items_map.write().await.entry(heal_type).or_insert(0) += 1;
        *self.last_heal_activity.write().await = SystemTime::now();
    }

    async fn is_quitting(&self) -> bool {
        if let Ok(true) = self.rx.has_changed() {
            info!("quited");
            return true;
        }
        false
    }

    async fn has_ended(&self) -> bool {
        if self.client_token == *BG_HEALING_UUID {
            return false;
        }

        *(self.end_time.read().await) != self.start_time
    }

    async fn stop(&self) {
        let _ = self.tx.send(true);
    }

    async fn push_heal_result_item(&self, r: &HealResultItem) -> Result<()> {
        let mut r = r.clone();
        let mut interval_timer = interval(HEAL_UNCONSUMED_TIMEOUT);
        #[allow(unused_assignments)]
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
                        return Err(Error::other("stopped"));
                    }
                    _ = interval_timer.tick() => {
                        return Err(Error::other("timeout"));
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
            r.result_index = 1 + *self.last_sent_result_index.read().await;
        }

        current_status_w.items.push(r);

        Ok(())
    }

    pub async fn queue_heal_task(&self, source: HealSource, heal_type: HealItemType) -> Result<()> {
        let mut task = HealTask::new(&source.bucket, &source.object, &source.version_id, &self.setting);
        info!("queue_heal_task, {:?}", task);
        if let Some(opts) = source.opts {
            task.opts = opts;
        } else {
            task.opts.scan_mode = HEAL_UNKNOWN_SCAN;
        }

        self.count_scanned(heal_type.clone()).await;

        if source.no_wait {
            let task_str = format!("{:?}", task);
            if GLOBAL_BackgroundHealRoutine.tasks_tx.try_send(task).is_ok() {
                info!("Task in the queue: {:?}", task_str);
            }
            return Ok(());
        }

        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        task.resp_tx = Some(resp_tx);

        let task_str = format!("{:?}", task);
        if GLOBAL_BackgroundHealRoutine.tasks_tx.try_send(task).is_ok() {
            info!("Task in the queue: {:?}", task_str);
        } else {
            error!("push task to queue failed");
        }
        let count_ok_drives = |drivers: &[HealDriveInfo]| {
            let mut count = 0;
            for drive in drivers.iter() {
                if drive.state == DRIVE_STATE_OK {
                    count += 1;
                }
            }
            count
        };

        match resp_rx.recv().await {
            Some(mut res) => {
                if res.err.is_none() {
                    self.count_healed(heal_type.clone()).await;
                } else {
                    self.count_failed(heal_type.clone()).await;
                }
                if !self.report_progress {
                    return if let Some(err) = res.err {
                        if err.to_string() == ERR_SKIP_FILE {
                            return Ok(());
                        }
                        Err(err)
                    } else {
                        Ok(())
                    };
                }
                res.result.heal_item_type = heal_type.clone();
                if let Some(err) = res.err.as_ref() {
                    res.result.detail = err.to_string();
                }
                if res.result.parity_blocks > 0 && res.result.data_blocks > 0 && res.result.data_blocks > res.result.parity_blocks
                {
                    let got = count_ok_drives(&res.result.after.drives);
                    if got < res.result.parity_blocks {
                        res.result.detail = format!(
                            "quorum loss - expected {} minimum, got drive states in OK {}",
                            res.result.parity_blocks, got
                        );
                    }
                }

                info!("queue_heal_task, HealResult: {:?}", res);
                self.push_heal_result_item(&res.result).await
            }
            None => Ok(()),
        }
    }

    async fn heal_disk_meta(h: Arc<HealSequence>) -> Result<()> {
        HealSequence::heal_rustfs_sys_meta(h, CONFIG_PREFIX).await
    }

    async fn heal_items(h: Arc<HealSequence>, buckets_only: bool) -> Result<()> {
        if h.client_token == *BG_HEALING_UUID {
            return Ok(());
        }

        let bucket = h.bucket.clone();
        let task1 = Self::heal_disk_meta(h.clone());
        let task2 = Self::heal_bucket(h.clone(), &bucket, buckets_only);
        let results = join!(task1, task2);
        results.0?;
        results.1?;

        Ok(())
    }

    async fn traverse_and_heal(h: Arc<HealSequence>) {
        let buckets_only = false;
        let result = Self::heal_items(h.clone(), buckets_only).await.err();
        let _ = h.traverse_and_heal_done_tx.read().await.send(result).await;
    }

    async fn heal_rustfs_sys_meta(h: Arc<HealSequence>, meta_prefix: &str) -> Result<()> {
        info!("heal_rustfs_sys_meta, h: {:?}", h);
        let Some(store) = new_object_layer_fn() else {
            return Err(Error::other("errServerNotInitialized"));
        };
        let setting = h.setting;
        store
            .heal_objects(RUSTFS_META_BUCKET, meta_prefix, &setting, h.clone(), true)
            .await
    }

    async fn is_done(&self) -> bool {
        if let Ok(true) = self.rx.has_changed() {
            return true;
        }
        false
    }

    pub async fn heal_bucket(hs: Arc<HealSequence>, bucket: &str, bucket_only: bool) -> Result<()> {
        info!("heal_bucket, hs: {:?}", hs);
        let (object, setting) = {
            hs.queue_heal_task(
                HealSource {
                    bucket: bucket.to_string(),
                    ..Default::default()
                },
                HEAL_ITEM_BUCKET.to_string(),
            )
            .await?;

            if bucket_only {
                return Ok(());
            }

            if !hs.setting.recursive {
                if !hs.object.is_empty() {
                    HealSequence::heal_object(hs.clone(), bucket, &hs.object, "", hs.setting.scan_mode).await?;
                }
                return Ok(());
            }
            (hs.object.clone(), hs.setting)
        };
        let Some(store) = new_object_layer_fn() else {
            return Err(Error::other("errServerNotInitialized"));
        };
        store.heal_objects(bucket, &object, &setting, hs.clone(), false).await
    }

    pub async fn heal_object(
        hs: Arc<HealSequence>,
        bucket: &str,
        object: &str,
        version_id: &str,
        _scan_mode: HealScanMode,
    ) -> Result<()> {
        info!("heal_object");
        if hs.is_quitting().await {
            info!("heal_object hs is quitting");
            return Err(Error::other(ERR_HEAL_STOP_SIGNALLED));
        }

        info!("will queue task");
        hs.queue_heal_task(
            HealSource {
                bucket: bucket.to_string(),
                object: object.to_string(),
                version_id: version_id.to_string(),
                opts: Some(hs.setting),
                ..Default::default()
            },
            HEAL_ITEM_OBJECT.to_string(),
        )
        .await?;

        Ok(())
    }

    pub async fn heal_meta_object(
        hs: Arc<HealSequence>,
        bucket: &str,
        object: &str,
        version_id: &str,
        _scan_mode: HealScanMode,
    ) -> Result<()> {
        if hs.is_quitting().await {
            return Err(Error::other(ERR_HEAL_STOP_SIGNALLED));
        }

        hs.queue_heal_task(
            HealSource {
                bucket: bucket.to_string(),
                object: object.to_string(),
                version_id: version_id.to_string(),
                ..Default::default()
            },
            HEAL_ITEM_BUCKET_METADATA.to_string(),
        )
        .await?;

        Ok(())
    }
}

pub async fn heal_sequence_start(h: Arc<HealSequence>) {
    {
        let mut current_status_w = h.current_status.write().await;
        current_status_w.summary = HEAL_RUNNING_STATUS.to_string();
        current_status_w.start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    let h_clone = h.clone();
    spawn(async move {
        HealSequence::traverse_and_heal(h_clone).await;
    });

    let h_clone_1 = h.clone();
    let mut x = h.traverse_and_heal_done_rx.write().await;
    select! {
        _ = h.is_done() => {
            *(h.end_time.write().await) = SystemTime::now();
            let mut current_status_w = h.current_status.write().await;
            current_status_w.summary = HEAL_FINISHED_STATUS.to_string();

            spawn(async move {
                let mut rx_w = h_clone_1.traverse_and_heal_done_rx.write().await;
                rx_w.recv().await;
            });
        }
        result = x.recv() => {
            if let Some(err) = result {
                match err {
                    Some(err) => {
                        let mut current_status_w = h.current_status.write().await;
                        current_status_w.summary = HEAL_STOPPED_STATUS.to_string();
                        current_status_w.failure_detail = err.to_string();
                    },
                    None => {
                        let mut current_status_w = h.current_status.write().await;
                        current_status_w.summary = HEAL_FINISHED_STATUS.to_string();
                    }
                }
            }

        }
    }
}

#[derive(Debug, Default)]
pub struct AllHealState {
    mu: RwLock<bool>,

    heal_seq_map: RwLock<HashMap<String, Arc<HealSequence>>>,
    heal_local_disks: RwLock<HashMap<Endpoint, bool>>,
    heal_status: RwLock<HashMap<String, HealingTracker>>,
}

impl AllHealState {
    pub fn new(cleanup: bool) -> Arc<Self> {
        let state = Arc::new(AllHealState::default());
        let (_, mut rx) = broadcast::channel(1);
        if cleanup {
            let state_clone = state.clone();
            spawn(async move {
                loop {
                    select! {
                        result = rx.recv() =>{
                            if let Ok(true) = result {
                                return;
                            }
                        }
                        _ = sleep(Duration::from_secs(5 * 60)) => {
                            state_clone.periodic_heal_seqs_clean().await;
                        }
                    }
                }
            });
        }

        state
    }

    pub async fn pop_heal_local_disks(&self, heal_local_disks: &[Endpoint]) {
        let _ = self.mu.write().await;

        self.heal_local_disks.write().await.retain(|k, _| {
            if heal_local_disks.contains(k) {
                return false;
            }
            true
        });

        let heal_local_disks = heal_local_disks.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        self.heal_status.write().await.retain(|_, v| {
            if heal_local_disks.contains(&v.endpoint) {
                return false;
            }

            true
        });
    }

    pub async fn pop_heal_status_json(&self, heal_path: &str, client_token: &str) -> Result<Vec<u8>> {
        match self.get_heal_sequence(heal_path).await {
            Some(h) => {
                if client_token != h.client_token {
                    info!("err heal invalid client token");
                    return Err(Error::other("err heal invalid client token"));
                }
                let num_items = h.current_status.read().await.items.len();
                let mut last_result_index = *h.last_sent_result_index.read().await;
                if num_items > 0 {
                    if let Some(item) = h.current_status.read().await.items.last() {
                        last_result_index = item.result_index;
                    }
                }
                *h.last_sent_result_index.write().await = last_result_index;
                let data = h.current_status.read().await.clone();
                match serde_json::to_vec(&data) {
                    Ok(b) => {
                        h.current_status.write().await.items.clear();
                        Ok(b)
                    }
                    Err(e) => {
                        h.current_status.write().await.items.clear();
                        info!("json encode err, e: {}", e);
                        Err(Error::other(e.to_string()))
                    }
                }
            }
            None => serde_json::to_vec(&HealSequenceStatus {
                summary: HEAL_FINISHED_STATUS.to_string(),
                ..Default::default()
            })
            .map_err(|e| {
                info!("json encode err, e: {}", e);
                Error::other(e.to_string())
            }),
        }
    }

    pub async fn update_heal_status(&self, tracker: &HealingTracker) {
        let _ = self.mu.write().await;
        let _ = tracker.mu.read().await;

        self.heal_status.write().await.insert(tracker.id.clone(), tracker.clone());
    }

    pub async fn get_local_healing_disks(&self) -> HashMap<String, madmin::HealingDisk> {
        let _ = self.mu.read().await;

        let mut dst = HashMap::new();
        for v in self.heal_status.read().await.values() {
            dst.insert(v.endpoint.clone(), v.to_healing_disk().await);
        }

        dst
    }

    pub async fn get_heal_local_disk_endpoints(&self) -> Endpoints {
        let _ = self.mu.read().await;

        let mut endpoints = Vec::new();
        self.heal_local_disks.read().await.iter().for_each(|(k, v)| {
            if !v {
                endpoints.push(k.clone());
            }
        });

        Endpoints::from(endpoints)
    }

    pub async fn set_disk_healing_status(&self, ep: Endpoint, healing: bool) {
        let _ = self.mu.write().await;

        self.heal_local_disks.write().await.insert(ep, healing);
    }

    pub async fn push_heal_local_disks(&self, heal_local_disks: &[Endpoint]) {
        let _ = self.mu.write().await;

        for heal_local_disk in heal_local_disks.iter() {
            self.heal_local_disks.write().await.insert(heal_local_disk.clone(), false);
        }
    }

    pub async fn periodic_heal_seqs_clean(&self) {
        let _ = self.mu.write().await;
        let now = SystemTime::now();

        let mut keys_to_remove = Vec::new();
        for (k, v) in self.heal_seq_map.read().await.iter() {
            if v.has_ended().await && now.duration_since(*(v.end_time.read().await)).unwrap() > KEEP_HEAL_SEQ_STATE_DURATION {
                keys_to_remove.push(k.clone())
            }
        }
        for key in keys_to_remove.iter() {
            self.heal_seq_map.write().await.remove(key);
        }
    }

    pub async fn get_heal_sequence_by_token(&self, token: &str) -> (Option<Arc<HealSequence>>, bool) {
        let _ = self.mu.read().await;

        for v in self.heal_seq_map.read().await.values() {
            if v.client_token == token {
                return (Some(v.clone()), true);
            }
        }

        (None, false)
    }

    pub async fn get_heal_sequence(&self, path: &str) -> Option<Arc<HealSequence>> {
        let _ = self.mu.read().await;

        self.heal_seq_map.read().await.get(path).cloned()
    }

    pub async fn stop_heal_sequence(&self, path: &str) -> Result<Vec<u8>> {
        let mut hsp = HealStopSuccess::default();
        if let Some(he) = self.get_heal_sequence(path).await {
            let client_token = he.client_token.clone();
            if *GLOBAL_IsDistErasure.read().await {
                // TODO: proxy
            }

            hsp.client_token = client_token;
            hsp.client_address = he.client_address.clone();
            hsp.start_time = Utc::now();

            he.stop().await;

            loop {
                if he.has_ended().await {
                    break;
                }

                sleep(Duration::from_secs(1)).await;
            }

            let _ = self.mu.write().await;
            self.heal_seq_map.write().await.remove(path);
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
    pub async fn launch_new_heal_sequence(&self, heal_sequence: Arc<HealSequence>) -> Result<Vec<u8>> {
        let path = path_join(&[
            PathBuf::from(heal_sequence.bucket.clone()),
            PathBuf::from(heal_sequence.object.clone()),
        ]);
        let path_s = path.to_str().unwrap();
        if heal_sequence.force_started {
            self.stop_heal_sequence(path_s).await?;
        } else if let Some(hs) = self.get_heal_sequence(path_s).await {
            if !hs.has_ended().await {
                return Err(Error::other(format!("Heal is already running on the given path (use force-start option to stop and start afresh). The heal was started by IP {} at {:?}, token is {}", heal_sequence.client_address, heal_sequence.start_time, heal_sequence.client_token)));
            }
        }

        let _ = self.mu.write().await;

        for (k, v) in self.heal_seq_map.read().await.iter() {
            if (has_prefix(k, path_s) || has_prefix(path_s, k)) && !v.has_ended().await {
                return Err(Error::other(format!(
                    "The provided heal sequence path overlaps with an existing heal path: {}",
                    k
                )));
            }
        }

        self.heal_seq_map
            .write()
            .await
            .insert(path_s.to_string(), heal_sequence.clone());

        let client_token = heal_sequence.client_token.clone();
        if *GLOBAL_IsDistErasure.read().await {
            // TODO: proxy
        }

        if heal_sequence.client_token == BG_HEALING_UUID {
            // For background heal do nothing, do not spawn an unnecessary goroutine.
        } else {
            let heal_sequence_clone = heal_sequence.clone();
            spawn(async {
                heal_sequence_start(heal_sequence_clone).await;
            });
        }

        let b = serde_json::to_vec(&HealStartSuccess {
            client_token,
            client_address: heal_sequence.client_address.clone(),
            // start_time: Utc::now(),
            start_time: heal_sequence.start_time.into(),
        })?;
        Ok(b)
    }
}
