// Copyright 2026 RustFS Team
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

//! Ephemeral, bounded terminal observations. Delivery is never a heal commit gate.

use crate::{
    heal_channel::HealScanMode,
    outcome::{HealExecutionOutcome, HealObjectReceipt},
};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::broadcast;

pub const MAX_COMPLETION_IDENTITY_BYTES: usize = 4096;
const COMPLETION_CHANNEL_CAPACITY: usize = 1024;

/// A process-local observation identity. It is never persisted or sent over RPC.
pub type HealObservationId = u64;

/// Allocate one identity per accepted owner. Exhaustion is reported as unknown.
pub fn new_heal_observation_id() -> Option<HealObservationId> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .ok()
}

#[derive(Debug, Clone)]
pub struct HealCompletion {
    /// The accepted owner, never a discarded merged request ID.
    pub task_id: String,
    pub scan_mode: HealScanMode,
    pub dry_run: bool,
    pub execution: Option<HealExecutionOutcome>,
    /// Only an untruncated single-object result is retained. A missing result
    /// does not mean absence, success, or payload integrity.
    pub object: Option<HealObjectReceipt>,
}

/// A completion paired with an optional in-process owner identity.
#[derive(Debug, Clone)]
pub struct ObservedHealCompletion {
    pub completion: Arc<HealCompletion>,
    pub observation_id: Option<HealObservationId>,
}

impl std::ops::Deref for ObservedHealCompletion {
    type Target = HealCompletion;

    fn deref(&self) -> &Self::Target {
        &self.completion
    }
}

struct CompletionBroadcaster {
    legacy: broadcast::Sender<Arc<HealCompletion>>,
    observed: broadcast::Sender<Arc<ObservedHealCompletion>>,
}

impl CompletionBroadcaster {
    fn new(capacity: usize) -> Self {
        Self {
            legacy: broadcast::channel(capacity).0,
            observed: broadcast::channel(capacity).0,
        }
    }

    fn has_observers(&self) -> bool {
        self.legacy.receiver_count() > 0 || self.observed.receiver_count() > 0
    }

    fn publish(&self, completion: HealCompletion, observation_id: Option<HealObservationId>) {
        let identity_bytes = completion.object.as_ref().map_or(0, |object| {
            object
                .identity
                .bucket
                .capacity()
                .saturating_add(object.identity.object.capacity())
                .saturating_add(object.identity.version_id.as_ref().map_or(0, String::capacity))
        });
        if completion.task_id.capacity().saturating_add(identity_bytes) <= MAX_COMPLETION_IDENTITY_BYTES {
            let completion = Arc::new(completion);
            let _ = self.legacy.send(Arc::clone(&completion));
            let _ = self.observed.send(Arc::new(ObservedHealCompletion {
                completion,
                observation_id,
            }));
        }
    }
}

fn broadcaster() -> &'static CompletionBroadcaster {
    static BROADCASTER: OnceLock<CompletionBroadcaster> = OnceLock::new();
    BROADCASTER.get_or_init(|| CompletionBroadcaster::new(COMPLETION_CHANNEL_CAPACITY))
}

pub fn has_heal_completion_observers() -> bool {
    broadcaster().has_observers()
}

/// Subscribe before admission so a fast worker cannot precede subscription.
pub fn subscribe_heal_completions() -> broadcast::Receiver<Arc<HealCompletion>> {
    broadcaster().legacy.subscribe()
}

pub fn subscribe_observed_heal_completions() -> broadcast::Receiver<Arc<ObservedHealCompletion>> {
    broadcaster().observed.subscribe()
}

/// Oversized diagnostics and lagging consumers lose observations, never repairs.
pub fn publish_heal_completion(completion: HealCompletion) {
    publish_observed_heal_completion(completion, None);
}

/// Each channel is FIFO independently; relative delivery across channels is not ordered.
pub fn publish_observed_heal_completion(completion: HealCompletion, observation_id: Option<HealObservationId>) {
    broadcaster().publish(completion, observation_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_completion_facades_share_receipts_and_preserve_missing_tokens() {
        // Other completion tests use private broadcasters, keeping this global
        // facade test isolated from their capacity and lag checks.
        let mut legacy = subscribe_heal_completions();
        let mut observed = subscribe_observed_heal_completions();
        let completion = HealCompletion {
            task_id: uuid::Uuid::new_v4().to_string(),
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: None,
            object: None,
        };
        publish_heal_completion(completion.clone());
        let legacy_event = legacy.try_recv().expect("legacy subscriber receives old API event");
        let observed_event = observed.try_recv().expect("observer receives old API event");
        assert!(Arc::ptr_eq(&legacy_event, &observed_event.completion));
        assert_eq!(legacy_event.task_id, completion.task_id);
        assert_eq!(observed_event.observation_id, None);

        publish_observed_heal_completion(completion, Some(51));
        let legacy_event = legacy.try_recv().expect("legacy subscriber receives observed API event");
        let observed_event = observed.try_recv().expect("observer receives observed API event");
        assert!(Arc::ptr_eq(&legacy_event, &observed_event.completion));
        assert_eq!(observed_event.observation_id, Some(51));
    }

    #[test]
    fn legacy_and_observed_subscribers_receive_the_same_completion_in_each_channel_order() {
        let id = uuid::Uuid::new_v4().to_string();
        let bus = CompletionBroadcaster::new(COMPLETION_CHANNEL_CAPACITY);
        let mut legacy = bus.legacy.subscribe();
        let mut observed = bus.observed.subscribe();
        assert!(bus.has_observers());
        for (suffix, token) in [("first", Some(41)), ("second", None)] {
            bus.publish(
                HealCompletion {
                    task_id: format!("{id}-{suffix}"),
                    scan_mode: HealScanMode::Normal,
                    dry_run: false,
                    execution: None,
                    object: None,
                },
                token,
            );
        }
        for (suffix, token) in [("first", Some(41)), ("second", None)] {
            let expected = format!("{id}-{suffix}");
            let legacy_event = loop {
                let event = legacy.try_recv().expect("legacy completion");
                if event.task_id == expected {
                    break event;
                }
            };
            let observed_event = loop {
                let event = observed.try_recv().expect("observed completion");
                if event.task_id == expected {
                    break event;
                }
            };
            assert!(Arc::ptr_eq(&legacy_event, &observed_event.completion));
            assert_eq!(observed_event.observation_id, token);
        }
        drop(legacy);
        assert!(bus.has_observers(), "observed-only subscribers require completion construction");
        let completion = HealCompletion {
            task_id: id.clone(),
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: None,
            object: None,
        };
        bus.publish(completion, None);
        let observed_event = loop {
            let event = observed.try_recv().expect("legacy publication reaches observed channel");
            if event.task_id == id {
                break event;
            }
        };
        assert_eq!(observed_event.observation_id, None);
    }

    #[test]
    fn legacy_and_observed_channels_report_lag_independently() {
        let bus = CompletionBroadcaster::new(COMPLETION_CHANNEL_CAPACITY);
        let mut legacy = bus.legacy.subscribe();
        let mut observed = bus.observed.subscribe();
        for index in 0..=COMPLETION_CHANNEL_CAPACITY {
            bus.publish(
                HealCompletion {
                    task_id: format!("dual-lag-{index}"),
                    scan_mode: HealScanMode::Normal,
                    dry_run: false,
                    execution: None,
                    object: None,
                },
                Some(42),
            );
        }
        assert!(matches!(legacy.try_recv(), Err(broadcast::error::TryRecvError::Lagged(_))));
        assert!(matches!(observed.try_recv(), Err(broadcast::error::TryRecvError::Lagged(_))));
    }

    #[test]
    fn completion_delivery_is_bounded_and_does_not_require_a_consumer() {
        let bus = CompletionBroadcaster::new(COMPLETION_CHANNEL_CAPACITY);
        assert!(!bus.has_observers());
        let mut receiver = bus.legacy.subscribe();
        assert!(bus.has_observers());
        for index in 0..=COMPLETION_CHANNEL_CAPACITY {
            bus.publish(
                HealCompletion {
                    task_id: format!("task-{index}"),
                    scan_mode: HealScanMode::Deep,
                    dry_run: false,
                    execution: Some(HealExecutionOutcome::Completed),
                    object: None,
                },
                None,
            );
        }
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Lagged(1))));
        while receiver.try_recv().is_ok() {}
        let mut oversized = String::with_capacity(MAX_COMPLETION_IDENTITY_BYTES + 1);
        oversized.push_str("short-but-large-allocation");
        let mut observed = bus.observed.subscribe();
        bus.publish(
            HealCompletion {
                task_id: oversized,
                scan_mode: HealScanMode::Deep,
                dry_run: false,
                execution: None,
                object: None,
            },
            None,
        );
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        assert!(matches!(observed.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        drop(receiver);
        drop(observed);
        assert!(!bus.has_observers());
        bus.publish(
            HealCompletion {
                task_id: "no-consumer".into(),
                scan_mode: HealScanMode::Deep,
                dry_run: false,
                execution: Some(HealExecutionOutcome::Completed),
                object: None,
            },
            None,
        );
    }
}
