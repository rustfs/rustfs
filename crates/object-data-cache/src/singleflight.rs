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
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

type FillSet = Mutex<HashSet<ObjectDataCacheKey>>;

fn lock_fills(fills: &FillSet) -> std::sync::MutexGuard<'_, HashSet<ObjectDataCacheKey>> {
    fills.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Shared singleflight controller that dedups concurrent cache fills per key.
///
/// The fill path already holds the fully materialized body, so a non-leader
/// gains nothing by waiting for the leader's result — a duplicate fill is
/// simply skipped. The controller therefore only elects a leader per key and
/// reports [`Busy`](ObjectDataCacheSingleflightAcquire::Busy) to everyone else.
#[derive(Debug)]
pub struct ObjectDataCacheSingleflight {
    fills: Arc<FillSet>,
    stats: Arc<ObjectDataCacheStats>,
}

impl ObjectDataCacheSingleflight {
    /// Creates a new singleflight controller.
    pub fn new(stats: Arc<ObjectDataCacheStats>) -> Self {
        Self {
            fills: Arc::new(Mutex::new(HashSet::new())),
            stats,
        }
    }

    /// Tries to become the leader for the supplied cache key without blocking.
    ///
    /// Returns [`Leader`](ObjectDataCacheSingleflightAcquire::Leader) when no
    /// fill for the key is in flight, otherwise
    /// [`Busy`](ObjectDataCacheSingleflightAcquire::Busy). The caller already
    /// owns the body, so a `Busy` outcome skips the redundant fill rather than
    /// waiting for another request's leader to finish.
    pub fn try_acquire(&self, key: ObjectDataCacheKey) -> ObjectDataCacheSingleflightAcquire<'static> {
        // Keep the critical section to the map mutation only; emit metrics after
        // dropping the guard so the recorder round-trip never serializes fills.
        let inflight_len = {
            let mut fills = lock_fills(&self.fills);
            if fills.contains(&key) {
                None
            } else {
                fills.insert(key.clone());
                Some(fills.len())
            }
        };

        match inflight_len {
            None => {
                record_singleflight_join(&self.stats);
                ObjectDataCacheSingleflightAcquire::Busy
            }
            Some(len) => {
                set_inflight_fills(&self.stats, "moka", len);
                ObjectDataCacheSingleflightAcquire::Leader(ObjectDataCacheSingleflightLeader {
                    key,
                    fills: Arc::clone(&self.fills),
                    stats: Arc::clone(&self.stats),
                    finished: false,
                    lifetime: std::marker::PhantomData,
                })
            }
        }
    }

    /// Returns whether a leader fill is currently in flight for the key.
    ///
    /// Used by the fill hot path to keep another in-flight fill's index key
    /// (a different key under the same identity that has registered in the index
    /// but not yet published its cache entry) from being pruned as stale.
    pub fn has_inflight(&self, key: &ObjectDataCacheKey) -> bool {
        lock_fills(&self.fills).contains(key)
    }
}

/// Leader-or-busy result from the singleflight controller.
pub enum ObjectDataCacheSingleflightAcquire<'a> {
    /// Caller is responsible for performing the fill operation.
    Leader(ObjectDataCacheSingleflightLeader<'a>),
    /// A fill for the same key is already in flight; the caller skips its own.
    Busy,
}

/// Leader handle for a singleflight fill operation.
pub struct ObjectDataCacheSingleflightLeader<'a> {
    key: ObjectDataCacheKey,
    fills: Arc<FillSet>,
    stats: Arc<ObjectDataCacheStats>,
    finished: bool,
    lifetime: std::marker::PhantomData<&'a ()>,
}

impl ObjectDataCacheSingleflightLeader<'_> {
    /// Completes the leader operation and releases the key.
    pub fn finish(mut self, result: ObjectDataCacheFillResult) -> ObjectDataCacheFillResult {
        self.remove_entry();
        self.finished = true;
        result
    }

    fn remove_entry(&self) {
        // Capture the length under the guard, then emit the gauge after dropping
        // it so the recorder round-trip stays out of the critical section.
        let len = {
            let mut fills = lock_fills(&self.fills);
            fills.remove(&self.key);
            fills.len()
        };
        set_inflight_fills(&self.stats, "moka", len);
    }
}

impl Drop for ObjectDataCacheSingleflightLeader<'_> {
    fn drop(&mut self) {
        // A leader dropped without finish() was cancelled mid-fill (e.g. the
        // fill task was aborted). Release the key so a later fill can become the
        // new leader instead of seeing a phantom in-flight entry forever.
        if !self.finished {
            self.remove_entry();
        }
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

    #[test]
    fn first_caller_leads_and_second_caller_is_busy() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let singleflight = ObjectDataCacheSingleflight::new(Arc::clone(&stats));

        let first = singleflight.try_acquire(key());
        let second = singleflight.try_acquire(key());

        assert!(
            matches!(first, ObjectDataCacheSingleflightAcquire::Leader(_)),
            "first caller must become leader"
        );
        assert!(
            matches!(second, ObjectDataCacheSingleflightAcquire::Busy),
            "second caller must not wait; it is busy and skips"
        );
        assert_eq!(stats.snapshot().singleflight_joins, 1);

        // Completing the leader releases the key for a subsequent fill.
        let ObjectDataCacheSingleflightAcquire::Leader(leader) = first else {
            unreachable!("first caller must be the leader");
        };
        let result = leader.finish(ObjectDataCacheFillResult::Inserted);
        assert_eq!(result, ObjectDataCacheFillResult::Inserted);

        assert!(
            matches!(singleflight.try_acquire(key()), ObjectDataCacheSingleflightAcquire::Leader(_)),
            "key must be released after the leader finishes"
        );
    }

    #[test]
    fn cancelled_leader_releases_key() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let singleflight = ObjectDataCacheSingleflight::new(Arc::clone(&stats));

        let leader = match singleflight.try_acquire(key()) {
            ObjectDataCacheSingleflightAcquire::Leader(leader) => leader,
            ObjectDataCacheSingleflightAcquire::Busy => panic!("first caller must become leader"),
        };
        assert!(singleflight.has_inflight(&key()), "leader must register the key as in-flight");

        // Dropping without finish() simulates the fill task being cancelled.
        drop(leader);

        assert!(!singleflight.has_inflight(&key()), "a cancelled leader must release the key");
        assert!(
            matches!(singleflight.try_acquire(key()), ObjectDataCacheSingleflightAcquire::Leader(_)),
            "key must be released after a cancelled leader"
        );
    }

    #[test]
    fn distinct_keys_lead_independently() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let singleflight = ObjectDataCacheSingleflight::new(Arc::clone(&stats));
        let other = ObjectDataCacheKey::new("bucket", "other", None, "etag", 1, ObjectDataCacheBodyVariant::FullObjectPlainV1);

        let first = singleflight.try_acquire(key());
        let second = singleflight.try_acquire(other);

        assert!(matches!(first, ObjectDataCacheSingleflightAcquire::Leader(_)));
        assert!(
            matches!(second, ObjectDataCacheSingleflightAcquire::Leader(_)),
            "a distinct key must not be deduped against another in-flight fill"
        );
        assert_eq!(stats.snapshot().singleflight_joins, 0);
    }
}
