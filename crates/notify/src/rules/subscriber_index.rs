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

use crate::rules::{BucketRulesSnapshot, BucketSnapshotRef, DynRulesContainer};
use arc_swap::ArcSwap;
use rustfs_targets::EventName;
use starshard::ShardedHashMap;
use std::fmt;
use std::sync::Arc;

/// A global bucket -> snapshot index.
///
/// Read path: lock-free load (ArcSwap)
/// Write path: atomic replacement after building a new snapshot
pub struct SubscriberIndex {
    // Use starshard for sharding to reduce lock competition when the number of buckets is large
    inner: ShardedHashMap<String, Arc<ArcSwap<BucketRulesSnapshot<DynRulesContainer>>>>,
    // Cache an "empty rule container" for empty snapshots (avoids building every time)
    empty_rules: Arc<DynRulesContainer>,
}

/// Avoid deriving fields that do not support Debug
impl fmt::Debug for SubscriberIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberIndex").finish_non_exhaustive()
    }
}

impl SubscriberIndex {
    /// Create a new SubscriberIndex.
    ///
    /// # Arguments
    /// * `empty_rules` - An Arc to an empty rules container used for empty snapshots
    ///
    /// Returns a new instance of SubscriberIndex.
    pub fn new(empty_rules: Arc<DynRulesContainer>) -> Self {
        Self {
            inner: ShardedHashMap::new(64),
            empty_rules,
        }
    }

    /// Get the current snapshot of a bucket.
    /// If it does not exist, return empty snapshot.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to load.
    ///
    /// Returns the snapshot reference for the specified bucket.
    pub fn load_snapshot(&self, bucket: &str) -> BucketSnapshotRef {
        match self.inner.get(&bucket.to_string()) {
            Some(cell) => cell.load_full(),
            None => Arc::new(BucketRulesSnapshot::empty(self.empty_rules.clone())),
        }
    }

    /// Quickly determine whether the bucket has a subscription to an event.
    /// This judgment can be consistent with subsequent rule matching when reading the same snapshot.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to check.
    /// * `event` - The event name to check for subscriptions.
    ///
    /// Returns `true` if there are subscribers for the event, `false` otherwise.
    #[inline]
    pub fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        let snap = self.load_snapshot(bucket);
        if snap.event_mask == 0 {
            return false;
        }
        snap.has_event(event)
    }

    /// Atomically update a bucket's snapshot (whole package replacement).
    ///
    /// - The caller first builds the complete `BucketRulesSnapshot` (including event\_mask and rules).
    /// - This method ensures that the read path will not observe intermediate states.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to update.
    /// * `new_snapshot` - The new snapshot to store for the bucket.
    pub fn store_snapshot(&self, bucket: &str, new_snapshot: BucketRulesSnapshot<DynRulesContainer>) {
        let key = bucket.to_string();

        let cell = self.inner.get(&key).unwrap_or_else(|| {
            // Insert a default cell (empty snapshot)
            let init = Arc::new(ArcSwap::from_pointee(BucketRulesSnapshot::empty(self.empty_rules.clone())));
            self.inner.insert(key.clone(), init.clone());
            init
        });

        cell.store(Arc::new(new_snapshot));
    }

    /// Delete the bucket's subscription view (make it empty).
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to clear.
    pub fn clear_bucket(&self, bucket: &str) {
        if let Some(cell) = self.inner.get(&bucket.to_string()) {
            cell.store(Arc::new(BucketRulesSnapshot::empty(self.empty_rules.clone())));
        }
    }
}

impl Default for SubscriberIndex {
    fn default() -> Self {
        // An available empty rule container is required; here it is implemented using minimal empty
        #[derive(Debug)]
        struct EmptyRules;
        impl crate::rules::subscriber_snapshot::RulesContainer for EmptyRules {
            type Rule = dyn crate::rules::subscriber_snapshot::RuleEvents;
            fn iter_rules<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Self::Rule> + 'a> {
                Box::new(std::iter::empty())
            }
        }

        Self::new(Arc::new(EmptyRules) as Arc<DynRulesContainer>)
    }
}
