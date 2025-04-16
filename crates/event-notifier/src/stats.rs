// src/stats.rs
use crate::target::TargetID;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct TargetStat {
    pub current_send_calls: AtomicI64,
    pub total_events: AtomicI64,
    pub failed_events: AtomicI64,
    pub current_queue: AtomicI64,
}

impl TargetStat {
    pub fn inc_send_calls(&self) {
        self.current_send_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_send_calls(&self) {
        self.current_send_calls.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_total_events(&self) {
        self.total_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed_events(&self) {
        self.failed_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_queue_size(&self, size: i64) {
        self.current_queue.store(size, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct TargetStats {
    stats: RwLock<HashMap<TargetID, Arc<TargetStat>>>,
}

impl TargetStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&self, id: &TargetID) -> Arc<TargetStat> {
        let mut stats = self.stats.write();
        stats
            .entry(id.clone())
            .or_insert_with(|| Arc::new(TargetStat::default()))
            .clone()
    }

    pub fn get_stats(&self) -> HashMap<TargetID, TargetSnapshot> {
        self.stats
            .read()
            .iter()
            .map(|(id, stat)| {
                (
                    id.clone(),
                    TargetSnapshot {
                        current_send_calls: stat.current_send_calls.load(Ordering::Relaxed),
                        total_events: stat.total_events.load(Ordering::Relaxed),
                        failed_events: stat.failed_events.load(Ordering::Relaxed),
                        current_queue: stat.current_queue.load(Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct TargetSnapshot {
    pub current_send_calls: i64,
    pub total_events: i64,
    pub failed_events: i64,
    pub current_queue: i64,
}
