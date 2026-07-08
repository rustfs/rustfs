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

        // A gap means the consumer's cursor points before the oldest event we
        // still retain: the events in between were evicted from the ring buffer
        // and can never be delivered. Report it so the consumer knows it lost
        // events instead of silently resuming from the oldest available one.
        let gap = match self.events.front() {
            Some((oldest, _)) => after_sequence.saturating_add(1) < *oldest,
            None => false,
        };

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
            gap,
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
        // Assign the history sequence number and publish to live subscribers under
        // the *same* critical section. `record` allocates a monotonically increasing
        // sequence and `broadcast::Sender::send` is synchronous, so holding the write
        // guard across both guarantees the broadcast order matches the recorded
        // sequence order. Otherwise two concurrent senders could record seq 1 then 2
        // but broadcast 2 then 1, so a cursor-based consumer would observe events out
        // of order relative to the replayable history (backlog#984 / #969).
        {
            let mut history = self.live_event_history.write().await;
            history.record(event.clone());
            let _ = self.live_event_sender.send(event.clone());
        }
        self.notifier.send(event).await;
    }
}

pub type NotifyEventBridge = NotifyPipeline;

#[cfg(test)]
mod tests {
    use super::{LiveEventHistory, MAX_RECENT_LIVE_EVENTS, NotifyPipeline};
    use crate::{Event, integration::NotificationMetrics, notifier::EventNotifier, rule_engine::NotifyRuleEngine};
    use rustfs_s3_types::EventName;
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
        assert!(!batch.gap);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].s3.object.key, "one");
    }

    #[test]
    fn snapshot_reports_gap_when_cursor_predates_evicted_history() {
        let mut history = LiveEventHistory::default();
        // Fill past the ring-buffer capacity so the oldest events are evicted.
        let total = MAX_RECENT_LIVE_EVENTS + 128;
        for i in 1..=total {
            history.record(Arc::new(Event::new_test_event("bucket", &i.to_string(), EventName::ObjectCreatedPut)));
        }

        let oldest_retained = (total - MAX_RECENT_LIVE_EVENTS + 1) as u64;

        // A consumer whose cursor sits inside the evicted range must be told a
        // gap exists instead of silently resuming from the oldest available.
        let batch = history.snapshot_since(10, 4096);
        assert!(batch.gap, "cursor predating evicted history must report a gap");
        assert_eq!(batch.events.first().unwrap().s3.object.key, oldest_retained.to_string());

        // A cursor contiguous with the oldest retained event must not report a gap.
        let contiguous = history.snapshot_since(oldest_retained - 1, 4096);
        assert!(!contiguous.gap, "cursor contiguous with retained history must not report a gap");

        // A caught-up cursor must not report a gap either.
        let caught_up = history.snapshot_since(total as u64, 4096);
        assert!(!caught_up.gap);
        assert!(caught_up.events.is_empty());
    }
}
