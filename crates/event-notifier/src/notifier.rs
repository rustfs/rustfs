// src/notifier.rs
use crate::error::Error;
use crate::event::Event;
use crate::rules::{RulesMap, TargetIDSet};
use crate::target::TargetList;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

pub struct EventNotifier {
    target_list: Arc<TargetList>,
    rules: RwLock<HashMap<String, RulesMap>>,
    tx: broadcast::Sender<Event>,
}

impl EventNotifier {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            target_list: Arc::new(TargetList::new()),
            rules: RwLock::new(HashMap::new()),
            tx,
        }
    }

    pub async fn send(&self, event: Event) -> Result<(), Error> {
        let rules = self
            .rules
            .read()
            .map_err(|_| Error::StoreError("Failed to read rules".to_string()))?;
        // 检查规则匹配
        let target_ids = if let Some(rules_map) = rules.get(&event.s3.bucket.name) {
            rules_map.match_targets(event.s3.event_name, &event.s3.object.key)
        } else {
            TargetIDSet::new()
        };

        // 发送事件
        if !target_ids.is_empty() {
            self.target_list.send(event.clone(), &target_ids).await;
        }

        // 广播给监听者
        let _ = self.tx.send(event);
        Ok(())
    }
}
