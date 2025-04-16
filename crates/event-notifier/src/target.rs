// src/target.rs
use crate::error::{Error, Result};
use crate::event::Event;
use crate::stats::{TargetSnapshot, TargetStats};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type TargetID = String;

#[async_trait]
pub trait Target: Send + Sync {
    fn id(&self) -> TargetID;
    async fn send(&self, event: Event) -> Result<()>;
    async fn is_active(&self) -> bool;
    async fn close(&self) -> Result<()>;
}

pub struct TargetList {
    targets: Arc<RwLock<HashMap<TargetID, Box<dyn Target>>>>,
    stats: Arc<TargetStats>,
}

impl TargetList {
    pub fn new() -> Self {
        Self {
            targets: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::default(),
        }
    }

    pub async fn send(&self, event: Event, target_ids: &[TargetID]) -> Vec<Result<()>> {
        let mut results = Vec::with_capacity(target_ids.len());
        let targets = self.targets.read().unwrap();

        for id in target_ids {
            let target = match targets.get(id) {
                Some(t) => t,
                None => {
                    results.push(Err(Error::TargetNotFound(id.to_string())));
                    continue;
                }
            };

            let stats = self.stats.get_or_create(id);
            stats.inc_send_calls();

            let result = target.send(event.clone()).await;

            stats.dec_send_calls();
            stats.inc_total_events();

            if result.is_err() {
                stats.inc_failed_events();
            }

            results.push(result);
        }

        results
    }

    pub fn get_stats(&self) -> HashMap<TargetID, TargetSnapshot> {
        self.stats.get_stats()
    }
}
