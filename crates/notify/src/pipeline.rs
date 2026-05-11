// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{Event, integration::LiveEventBatch, notifier::EventNotifier};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

const MAX_RECENT_LIVE_EVENTS: usize = 1024;

#[derive(Default)]
pub struct LiveEventHistory {
    next_sequence: u64,
    events: VecDeque<(u64, Arc<Event>)>,
}

impl LiveEventHistory {
    pub fn record(&mut self, event: Arc<Event>) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push_back((self.next_sequence, event));
        while self.events.len() > MAX_RECENT_LIVE_EVENTS {
            self.events.pop_front();
        }
    }

    pub fn snapshot_since(&self, after_sequence: u64, limit: usize) -> LiveEventBatch {
        let mut events = Vec::new();
        let mut next_sequence = after_sequence;
        let mut truncated = false;

        for (sequence, event) in self.events.iter() {
            if *sequence <= after_sequence {
                continue;
            }
            if events.len() >= limit {
                truncated = true;
                break;
            }
            next_sequence = *sequence;
            events.push(event.clone());
        }

        LiveEventBatch {
            events,
            next_sequence,
            truncated,
        }
    }
}

#[derive(Clone)]
pub struct NotifyPipeline {
    notifier: Arc<EventNotifier>,
    live_event_sender: broadcast::Sender<Arc<Event>>,
    live_event_history: Arc<RwLock<LiveEventHistory>>,
}

impl NotifyPipeline {
    pub fn new(
        notifier: Arc<EventNotifier>,
        live_event_sender: broadcast::Sender<Arc<Event>>,
        live_event_history: Arc<RwLock<LiveEventHistory>>,
    ) -> Self {
        Self {
            notifier,
            live_event_sender,
            live_event_history,
        }
    }

    pub fn has_live_listeners(&self) -> bool {
        self.live_event_sender.receiver_count() > 0
    }

    pub fn subscribe_live_events(&self) -> broadcast::Receiver<Arc<Event>> {
        self.live_event_sender.subscribe()
    }

    pub async fn recent_live_events_since(&self, after_sequence: u64, limit: usize) -> LiveEventBatch {
        let history = self.live_event_history.read().await;
        history.snapshot_since(after_sequence, limit.max(1))
    }

    pub async fn send_event(&self, event: Arc<Event>) {
        self.live_event_history.write().await.record(event.clone());
        let _ = self.live_event_sender.send(event.clone());
        self.notifier.send(event).await;
    }
}

pub type NotifyEventBridge = NotifyPipeline;

#[cfg(test)]
mod tests {
    use super::{LiveEventHistory, NotifyPipeline};
    use crate::{Event, integration::NotificationMetrics, notifier::EventNotifier, rule_engine::NotifyRuleEngine};
    use rustfs_s3_common::EventName;
    use std::sync::Arc;
    use tokio::sync::{RwLock, broadcast};

    fn build_pipeline() -> NotifyPipeline {
        let metrics = Arc::new(NotificationMetrics::new());
        let notifier = Arc::new(EventNotifier::new(metrics, NotifyRuleEngine::new()));
        let (live_event_sender, _) = broadcast::channel(16);
        NotifyPipeline::new(notifier, live_event_sender, Arc::new(RwLock::new(LiveEventHistory::default())))
    }

    #[tokio::test]
    async fn pipeline_reports_live_listener_subscription_state() {
        let pipeline = build_pipeline();
        assert!(!pipeline.has_live_listeners());

        let _rx = pipeline.subscribe_live_events();
        assert!(pipeline.has_live_listeners());
    }

    #[tokio::test]
    async fn pipeline_records_recent_live_events() {
        let pipeline = build_pipeline();
        let event = Arc::new(Event::new_test_event("bucket", "one", EventName::ObjectCreatedPut));

        pipeline.send_event(event).await;

        let batch = pipeline.recent_live_events_since(0, 16).await;
        assert_eq!(batch.next_sequence, 1);
        assert!(!batch.truncated);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].s3.object.key, "one");
    }
}
