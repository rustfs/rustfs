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
use std::sync::{Arc, OnceLock};
use tokio::sync::broadcast;

pub const MAX_COMPLETION_IDENTITY_BYTES: usize = 4096;
const COMPLETION_CHANNEL_CAPACITY: usize = 1024;

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

fn sender() -> &'static broadcast::Sender<Arc<HealCompletion>> {
    static SENDER: OnceLock<broadcast::Sender<Arc<HealCompletion>>> = OnceLock::new();
    SENDER.get_or_init(|| broadcast::channel(COMPLETION_CHANNEL_CAPACITY).0)
}

pub fn has_heal_completion_observers() -> bool {
    sender().receiver_count() > 0
}

/// Subscribe before admission so a fast worker cannot precede subscription.
pub fn subscribe_heal_completions() -> broadcast::Receiver<Arc<HealCompletion>> {
    sender().subscribe()
}

/// Oversized diagnostics and lagging consumers lose observations, never repairs.
pub fn publish_heal_completion(completion: HealCompletion) {
    let identity_bytes = completion.object.as_ref().map_or(0, |object| {
        object
            .identity
            .bucket
            .capacity()
            .saturating_add(object.identity.object.capacity())
            .saturating_add(object.identity.version_id.as_ref().map_or(0, String::capacity))
    });
    if completion.task_id.capacity().saturating_add(identity_bytes) <= MAX_COMPLETION_IDENTITY_BYTES {
        let _ = sender().send(Arc::new(completion));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_delivery_is_bounded_and_does_not_require_a_consumer() {
        let mut receiver = subscribe_heal_completions();
        for index in 0..=COMPLETION_CHANNEL_CAPACITY {
            publish_heal_completion(HealCompletion {
                task_id: format!("task-{index}"),
                scan_mode: HealScanMode::Deep,
                dry_run: false,
                execution: Some(HealExecutionOutcome::Completed),
                object: None,
            });
        }
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Lagged(1))));
        while receiver.try_recv().is_ok() {}
        let mut oversized = String::with_capacity(MAX_COMPLETION_IDENTITY_BYTES + 1);
        oversized.push_str("short-but-large-allocation");
        publish_heal_completion(HealCompletion {
            task_id: oversized,
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: None,
            object: None,
        });
        assert!(matches!(receiver.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        drop(receiver);
        publish_heal_completion(HealCompletion {
            task_id: "no-consumer".into(),
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: Some(HealExecutionOutcome::Completed),
            object: None,
        });
    }
}
