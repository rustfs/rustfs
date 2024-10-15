use crate::{
    disk::MetaCacheEntry,
    error::{Error, Result},
};
use lazy_static::lazy_static;
use std::{collections::HashMap, time::Instant};
use uuid::Uuid;

use super::heal_commands::{HealItemType, HealOpts, HealResultItem, HealScanMode};

type HealStatusSummary = String;
type ItemsMap = HashMap<HealItemType, usize>;
pub type HealObjectFn = Box<dyn Fn(&str, &str, &str, HealScanMode) -> Result<()> + Send>;
pub type HealEntryFn = Box<dyn Fn(String, MetaCacheEntry, HealScanMode) -> Result<()> + Send>;

lazy_static! {
    static ref HEAL_NOT_STARTED_STATUS: HealStatusSummary = String::from("not started");
    static ref bgHealingUUID: String = String::from("0000-0000-0000-0000");
}

lazy_static! {}

#[derive(Debug)]
pub struct HealSequenceStatus {
    summary: HealStatusSummary,
    failure_detail: String,
    start_time: Instant,
    heal_setting: HealOpts,
    items: Vec<HealResultItem>,
}

impl Default for HealSequenceStatus {
    fn default() -> Self {
        Self {
            summary: Default::default(),
            failure_detail: Default::default(),
            start_time: Instant::now(),
            heal_setting: Default::default(),
            items: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct HealSequence {
    pub bucket: String,
    pub object: String,
    pub report_progress: bool,
    pub start_time: Instant,
    pub end_time: Instant,
    pub client_token: String,
    pub client_address: String,
    pub force_started: bool,
    pub setting: HealOpts,
    pub current_status: HealSequenceStatus,
    pub last_sent_result_index: usize,
    pub scanned_items_map: ItemsMap,
    pub healed_items_map: ItemsMap,
    pub heal_failed_items_map: ItemsMap,
    pub last_heal_activity: Instant,
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
            current_status: HealSequenceStatus {
                summary: HEAL_NOT_STARTED_STATUS.to_string(),
                heal_setting: hs,
                ..Default::default()
            },
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
        self.last_heal_activity = Instant::now();
    }

    fn count_scanned(&mut self, heal_type: HealItemType) {
        *self.scanned_items_map.entry(heal_type).or_insert(0) += 1;
        self.last_heal_activity = Instant::now()
    }

    fn count_healed(&mut self, heal_type: HealItemType) {
        *self.healed_items_map.entry(heal_type).or_insert(0) += 1;
        self.last_heal_activity = Instant::now()
    }

    fn is_quitting(&self) -> bool {
        todo!()
    }

    fn has_ended(&self) -> bool {
        if self.client_token == bgHealingUUID.to_string() {
            return false;
        }

        !(self.end_time == self.start_time)
    }

    fn stop(&self) {
        todo!()
    }

    fn push_heal_result_item(&self, r: HealResultItem) -> Result<()> {
        todo!()
    }

    fn heal_disk_meta() -> Result<()> {
        todo!()
    }

    fn heal_items(&self, buckets_only: bool) -> Result<()> {
        if self.client_token == bgHealingUUID.to_string() {
            return Ok(());
        }

        todo!()
    }

    fn traverse_and_heal(&self) {
        let buckets_only = false;
    }

    fn heal_minio_sys_meta(&self, meta_prefix: String) -> Result<()> {
        todo!()
    }
}

impl Default for HealSequence {
    fn default() -> Self {
        Self {
            bucket: Default::default(),
            object: Default::default(),
            report_progress: Default::default(),
            start_time: Instant::now(),
            end_time: Instant::now(),
            client_token: Default::default(),
            client_address: Default::default(),
            force_started: Default::default(),
            setting: Default::default(),
            current_status: Default::default(),
            last_sent_result_index: Default::default(),
            scanned_items_map: Default::default(),
            healed_items_map: Default::default(),
            heal_failed_items_map: Default::default(),
            last_heal_activity: Instant::now(),
        }
    }
}
