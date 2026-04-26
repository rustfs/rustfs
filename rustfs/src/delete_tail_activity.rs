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

use metrics::{counter, gauge, histogram};
use std::sync::atomic::{AtomicU64, Ordering};

static DELETE_TAIL_TOTAL: AtomicU64 = AtomicU64::new(0);
static DELETE_CLEANUP_TOTAL: AtomicU64 = AtomicU64::new(0);
static DELETE_REPLICATION_TOTAL: AtomicU64 = AtomicU64::new(0);
static DELETE_NOTIFY_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub enum DeleteTailStage {
    Tail,
    Cleanup,
    Replication,
    Notify,
}

impl DeleteTailStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tail => "tail",
            Self::Cleanup => "cleanup",
            Self::Replication => "replication",
            Self::Notify => "notify",
        }
    }
}

fn stage_counter(stage: DeleteTailStage) -> &'static AtomicU64 {
    match stage {
        DeleteTailStage::Tail => &DELETE_TAIL_TOTAL,
        DeleteTailStage::Cleanup => &DELETE_CLEANUP_TOTAL,
        DeleteTailStage::Replication => &DELETE_REPLICATION_TOTAL,
        DeleteTailStage::Notify => &DELETE_NOTIFY_TOTAL,
    }
}

#[derive(Debug)]
pub struct DeleteTailActivityGuard {
    stage: DeleteTailStage,
    started_at: std::time::Instant,
}

impl DeleteTailActivityGuard {
    pub fn new(stage: DeleteTailStage) -> Self {
        let total = stage_counter(stage).fetch_add(1, Ordering::Relaxed) + 1;
        gauge!(
            "rustfs_delete_tail_activity_inflight_current",
            "stage" => stage.as_str().to_string()
        )
        .set(total as f64);
        gauge!("rustfs_delete_tail_activity_total_inflight_current").set(current_delete_tail_activity() as f64);
        counter!(
            "rustfs_delete_tail_activity_started_total",
            "stage" => stage.as_str().to_string()
        )
        .increment(1);
        Self {
            stage,
            started_at: std::time::Instant::now(),
        }
    }
}

impl Drop for DeleteTailActivityGuard {
    fn drop(&mut self) {
        let previous = stage_counter(self.stage).fetch_sub(1, Ordering::Relaxed);
        let next = previous.saturating_sub(1);
        gauge!(
            "rustfs_delete_tail_activity_inflight_current",
            "stage" => self.stage.as_str().to_string()
        )
        .set(next as f64);
        gauge!("rustfs_delete_tail_activity_total_inflight_current").set(current_delete_tail_activity() as f64);
        histogram!(
            "rustfs_delete_tail_activity_duration_seconds",
            "stage" => self.stage.as_str().to_string()
        )
        .record(self.started_at.elapsed().as_secs_f64());
    }
}

pub fn current_delete_tail_activity() -> u64 {
    DELETE_TAIL_TOTAL.load(Ordering::Relaxed)
        + DELETE_CLEANUP_TOTAL.load(Ordering::Relaxed)
        + DELETE_REPLICATION_TOTAL.load(Ordering::Relaxed)
        + DELETE_NOTIFY_TOTAL.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{DeleteTailActivityGuard, DeleteTailStage, current_delete_tail_activity};

    #[test]
    fn delete_tail_activity_guard_tracks_total_activity() {
        let before = current_delete_tail_activity();
        let guard = DeleteTailActivityGuard::new(DeleteTailStage::Cleanup);
        assert_eq!(current_delete_tail_activity(), before + 1);
        drop(guard);
        assert_eq!(current_delete_tail_activity(), before);
    }
}
