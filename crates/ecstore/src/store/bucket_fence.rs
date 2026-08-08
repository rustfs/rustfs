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

//! Memoized bucket-incarnation validation under continuous lifecycle read-lock
//! coverage.
//!
//! The PUT commit fence introduced by #5648 validated the bucket incarnation
//! with an uncached read (`get_bucket_incarnation_id_from_disk`: a distributed
//! metadata-transaction read lock plus an EC quorum read of the bucket
//! metadata) on every PUT commit. Under small-object write load that is two
//! extra quorum round-trips per PUT, and the resulting lock-manager pressure
//! produced sustained `Lock acquisition timeout` errors (~1,000 client-visible
//! failures per 5-minute 64-concurrency window in benchmarks).
//!
//! The memo exploits the fence's own locking protocol: bucket deletion and
//! recreation take the bucket lifecycle WRITE lock, while every fenced PUT
//! holds a lifecycle READ lock for the whole commit. Therefore, while at least
//! one lifecycle read guard on this node has been held continuously, no
//! lifecycle write lock can have been granted anywhere in the cluster, so the
//! bucket incarnation cannot have changed. The first fenced PUT in such a
//! coverage window pays the authoritative disk validation exactly as before;
//! subsequent PUTs whose guards overlap that window compare against the
//! memoized value. When the node's last guard drops — or any guard observes
//! `is_lock_lost` — the memo is cleared and the next PUT revalidates from
//! disk.
//!
//! The memo is deliberately per-node process state (not a cross-node cache):
//! its validity is derived purely from locks this process itself holds, so
//! best-effort peer cache invalidation (which is why the fence read from disk
//! in the first place) is irrelevant to its correctness.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustfs_lock::NamespaceLockGuard;
use uuid::Uuid;

#[derive(Default)]
struct FenceEntry {
    guards: usize,
    validated: Option<Uuid>,
}

/// Per-store registry tracking, per bucket, how many lifecycle read guards are
/// live on this node and the incarnation id validated under that coverage.
#[derive(Default)]
pub(crate) struct BucketFenceRegistry {
    entries: Mutex<HashMap<String, FenceEntry>>,
}

impl BucketFenceRegistry {
    /// Register a new live guard for `bucket` and return the memoized
    /// incarnation id if one is valid for the current coverage window.
    fn enter(&self, bucket: &str) -> Option<Uuid> {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        let entry = entries.entry(bucket.to_string()).or_default();
        entry.guards += 1;
        entry.validated
    }

    /// Memoize `incarnation` for `bucket`. Only meaningful while the caller
    /// still holds a registered guard (which it does by construction).
    fn memoize(&self, bucket: &str, incarnation: Uuid) {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        if let Some(entry) = entries.get_mut(bucket)
            && entry.guards > 0
        {
            entry.validated = Some(incarnation);
        }
    }

    /// Deregister a guard. Clears the memo when the last guard leaves or when
    /// the leaving guard lost its lock (lost coverage means a lifecycle write
    /// lock may have been granted, so the memo can no longer be trusted).
    fn exit(&self, bucket: &str, lock_lost: bool) {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        if let Some(entry) = entries.get_mut(bucket) {
            entry.guards = entry.guards.saturating_sub(1);
            if lock_lost {
                entry.validated = None;
            }
            if entry.guards == 0 {
                entries.remove(bucket);
            }
        }
    }
}

/// A held bucket lifecycle read lock plus its registration in the fence
/// registry. Dropping the guard deregisters it; the memo is cleared when the
/// last guard for the bucket drops (or a lost lock is observed).
pub(crate) struct BucketIncarnationFenceGuard {
    inner: Option<NamespaceLockGuard>,
    registry: Arc<BucketFenceRegistry>,
    bucket: String,
}

impl BucketIncarnationFenceGuard {
    pub(crate) fn is_lock_lost(&self) -> bool {
        self.inner.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost)
    }
}

impl Drop for BucketIncarnationFenceGuard {
    fn drop(&mut self) {
        let lost = self.is_lock_lost();
        self.registry.exit(&self.bucket, lost);
        self.inner.take();
    }
}

pub(super) struct FencePieces {
    pub(super) registry: Arc<BucketFenceRegistry>,
    pub(super) inner: NamespaceLockGuard,
}

impl FencePieces {
    /// Register the freshly acquired read lock and return the memoized
    /// incarnation for the coverage window, if any.
    pub(super) fn enter(&self, bucket: &str) -> Option<Uuid> {
        self.registry.enter(bucket)
    }

    pub(super) fn memoize(&self, bucket: &str, incarnation: Uuid) {
        self.registry.memoize(bucket, incarnation)
    }

    pub(super) fn lock_lost(&self) -> bool {
        self.inner.is_lock_lost()
    }

    pub(super) fn into_guard(self, bucket: &str) -> BucketIncarnationFenceGuard {
        BucketIncarnationFenceGuard {
            inner: Some(self.inner),
            registry: self.registry,
            bucket: bucket.to_string(),
        }
    }

    /// Abandon the acquisition (validation failed): deregister and release.
    pub(super) fn abandon(self, bucket: &str) {
        let lost = self.lock_lost();
        self.registry.exit(bucket, lost);
        drop(self.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn memo_valid_only_while_guards_overlap() {
        let reg = BucketFenceRegistry::default();

        assert_eq!(reg.enter("b"), None, "first guard sees no memo");
        reg.memoize("b", uuid(1));
        assert_eq!(reg.enter("b"), Some(uuid(1)), "overlapping guard reuses memo");
        reg.exit("b", false);
        reg.exit("b", false);

        // Coverage gap: all guards gone, memo must be dropped.
        assert_eq!(reg.enter("b"), None, "post-gap guard must revalidate");
        reg.exit("b", false);
    }

    #[test]
    fn lost_lock_clears_memo_but_keeps_other_guards_registered() {
        let reg = BucketFenceRegistry::default();

        assert_eq!(reg.enter("b"), None);
        reg.memoize("b", uuid(7));
        assert_eq!(reg.enter("b"), Some(uuid(7)));

        // First guard exits reporting a lost lock: memo cleared even though
        // a second guard is still live.
        reg.exit("b", true);
        assert_eq!(reg.enter("b"), None, "memo not trusted after a lost lock");
        reg.exit("b", false);
        reg.exit("b", false);
    }

    #[test]
    fn buckets_are_isolated() {
        let reg = BucketFenceRegistry::default();
        assert_eq!(reg.enter("a"), None);
        reg.memoize("a", uuid(1));
        assert_eq!(reg.enter("b"), None, "memo does not leak across buckets");
        reg.exit("b", false);
        reg.exit("a", false);
    }

    #[test]
    fn memoize_without_live_guard_is_ignored() {
        let reg = BucketFenceRegistry::default();
        reg.memoize("b", uuid(9));
        assert_eq!(reg.enter("b"), None);
        reg.exit("b", false);
    }
}
