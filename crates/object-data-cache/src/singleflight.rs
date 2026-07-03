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

use crate::cache::ObjectDataCacheFillResult;
use crate::key::ObjectDataCacheKey;
use crate::metrics::{record_singleflight_join, set_inflight_fills};
use crate::stats::ObjectDataCacheStats;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

type FillMap = Mutex<HashMap<ObjectDataCacheKey, watch::Sender<Option<ObjectDataCacheFillResult>>>>;

fn lock_fills(
    fills: &FillMap,
) -> std::sync::MutexGuard<'_, HashMap<ObjectDataCacheKey, watch::Sender<Option<ObjectDataCacheFillResult>>>> {
    fills.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Shared singleflight controller for cache fill operations.
#[derive(Debug)]
pub struct ObjectDataCacheSingleflight {
    fills: FillMap,
    stats: Arc<ObjectDataCacheStats>,
}

impl ObjectDataCacheSingleflight {
    /// Creates a new singleflight controller.
    pub fn new(stats: Arc<ObjectDataCacheStats>) -> Self {
        Self {
            fills: Mutex::new(HashMap::new()),
            stats,
        }
    }

    /// Acquires a leader-or-waiter role for the supplied cache key.
    pub async fn acquire(&self, key: ObjectDataCacheKey) -> ObjectDataCacheSingleflightAcquire<'_> {
        let mut fills = lock_fills(&self.fills);
        if let Some(sender) = fills.get(&key) {
            record_singleflight_join(&self.stats);
            return ObjectDataCacheSingleflightAcquire::Waiter(ObjectDataCacheSingleflightWaiter { rx: sender.subscribe() });
        }

        let (tx, _rx) = watch::channel(None);
        fills.insert(key.clone(), tx.clone());
        set_inflight_fills(&self.stats, "moka", fills.len());
        drop(fills);

        ObjectDataCacheSingleflightAcquire::Leader(ObjectDataCacheSingleflightLeader {
            key,
            tx,
            fills: &self.fills,
            stats: Arc::clone(&self.stats),
            finished: false,
        })
    }
}

/// Leader or waiter result from the singleflight map.
pub enum ObjectDataCacheSingleflightAcquire<'a> {
    /// Caller is responsible for performing the fill operation.
    Leader(ObjectDataCacheSingleflightLeader<'a>),
    /// Caller must wait for the leader's result.
    Waiter(ObjectDataCacheSingleflightWaiter),
}

/// Leader handle for a singleflight fill operation.
pub struct ObjectDataCacheSingleflightLeader<'a> {
    key: ObjectDataCacheKey,
    tx: watch::Sender<Option<ObjectDataCacheFillResult>>,
    fills: &'a FillMap,
    stats: Arc<ObjectDataCacheStats>,
    finished: bool,
}

impl<'a> ObjectDataCacheSingleflightLeader<'a> {
    /// Completes the leader operation and publishes the shared result.
    pub async fn finish(mut self, result: ObjectDataCacheFillResult) -> ObjectDataCacheFillResult {
        self.remove_entry();
        self.finished = true;
        let _ = self.tx.send(Some(result.clone()));
        result
    }

    fn remove_entry(&self) {
        let mut fills = lock_fills(self.fills);
        fills.remove(&self.key);
        set_inflight_fills(&self.stats, "moka", fills.len());
    }
}

impl<'a> Drop for ObjectDataCacheSingleflightLeader<'a> {
    fn drop(&mut self) {
        // A leader dropped without finish() was cancelled mid-fill (e.g. the
        // GET request future was aborted). Remove the map entry so its sender
        // clone is released and waiters observe the closed channel instead of
        // blocking forever, and so a later fill can become the new leader.
        if !self.finished {
            self.remove_entry();
        }
    }
}

/// Waiter handle for a singleflight fill operation.
pub struct ObjectDataCacheSingleflightWaiter {
    rx: watch::Receiver<Option<ObjectDataCacheFillResult>>,
}

impl ObjectDataCacheSingleflightWaiter {
    /// Waits for the shared fill result.
    pub async fn wait(mut self) -> ObjectDataCacheFillResult {
        if self.rx.wait_for(|value| value.is_some()).await.is_err() {
            return ObjectDataCacheFillResult::SkippedSingleflightClosed;
        }

        self.rx
            .borrow()
            .as_ref()
            .cloned()
            .unwrap_or(ObjectDataCacheFillResult::SkippedSingleflightClosed)
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectDataCacheSingleflight, ObjectDataCacheSingleflightAcquire};
    use crate::cache::ObjectDataCacheFillResult;
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheKey};
    use crate::stats::ObjectDataCacheStats;
    use std::sync::Arc;

    fn key() -> ObjectDataCacheKey {
        ObjectDataCacheKey::new("bucket", "object", None, "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1)
    }

    #[tokio::test]
    async fn singleflight_returns_waiter_for_second_caller() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let singleflight = ObjectDataCacheSingleflight::new(Arc::clone(&stats));

        let first = singleflight.acquire(key()).await;
        let second = singleflight.acquire(key()).await;

        let leader = match first {
            ObjectDataCacheSingleflightAcquire::Leader(leader) => leader,
            ObjectDataCacheSingleflightAcquire::Waiter(_) => panic!("first caller must become leader"),
        };
        let waiter = match second {
            ObjectDataCacheSingleflightAcquire::Leader(_) => panic!("second caller must become waiter"),
            ObjectDataCacheSingleflightAcquire::Waiter(waiter) => waiter,
        };

        let waiter_task = tokio::spawn(async move { waiter.wait().await });
        let leader_result = leader.finish(ObjectDataCacheFillResult::Inserted).await;
        let waiter_result = waiter_task.await.expect("waiter task should complete");

        assert_eq!(leader_result, ObjectDataCacheFillResult::Inserted);
        assert_eq!(waiter_result, ObjectDataCacheFillResult::Inserted);
        assert_eq!(stats.snapshot().singleflight_joins, 1);
    }

    #[tokio::test]
    async fn cancelled_leader_releases_key_and_unblocks_waiters() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let singleflight = ObjectDataCacheSingleflight::new(Arc::clone(&stats));

        let leader = match singleflight.acquire(key()).await {
            ObjectDataCacheSingleflightAcquire::Leader(leader) => leader,
            ObjectDataCacheSingleflightAcquire::Waiter(_) => panic!("first caller must become leader"),
        };
        let waiter = match singleflight.acquire(key()).await {
            ObjectDataCacheSingleflightAcquire::Leader(_) => panic!("second caller must become waiter"),
            ObjectDataCacheSingleflightAcquire::Waiter(waiter) => waiter,
        };

        // Dropping without finish() simulates the leader future being cancelled.
        drop(leader);

        let waiter_result = waiter.wait().await;
        assert_eq!(waiter_result, ObjectDataCacheFillResult::SkippedSingleflightClosed);

        assert!(
            matches!(singleflight.acquire(key()).await, ObjectDataCacheSingleflightAcquire::Leader(_)),
            "key must be released after a cancelled leader"
        );
    }
}
