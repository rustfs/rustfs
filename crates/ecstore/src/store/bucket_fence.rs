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
use rustfs_lock::distributed_lock::LockLostSignal;
use uuid::Uuid;

#[derive(Default)]
struct FenceEntry {
    next_token: u64,
    guards: Vec<RegisteredGuard>,
    validated: Option<Uuid>,
}

struct RegisteredGuard {
    token: u64,
    loss_probe: LockLossProbe,
}

enum LockLossProbe {
    Distributed(Arc<LockLostSignal>),
    Local,
    #[cfg(test)]
    Test(Arc<std::sync::atomic::AtomicBool>),
}

impl LockLossProbe {
    fn from_guard(guard: &NamespaceLockGuard) -> Self {
        match guard.lock_lost_signal() {
            Some(signal) => Self::Distributed(signal),
            None => Self::Local,
        }
    }

    fn is_lost(&self) -> bool {
        match self {
            Self::Distributed(signal) => signal.is_lost(),
            Self::Local => false,
            #[cfg(test)]
            Self::Test(lost) => lost.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}

pub(super) struct FenceRegistration {
    pub(super) token: u64,
    pub(super) memoized: Option<Uuid>,
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
    fn enter(&self, bucket: &str, loss_probe: LockLossProbe) -> FenceRegistration {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        let entry = entries.entry(bucket.to_string()).or_default();
        let token = entry.next_token;
        entry.next_token = entry.next_token.wrapping_add(1);
        entry.guards.push(RegisteredGuard { token, loss_probe });
        let has_lost_guard = entry.guards.iter().any(|guard| guard.loss_probe.is_lost());
        if has_lost_guard {
            entry.validated = None;
        }
        FenceRegistration {
            token,
            memoized: if has_lost_guard { None } else { entry.validated },
        }
    }

    /// Memoize `incarnation` for `bucket`. Only meaningful while the caller
    /// still holds a registered guard (which it does by construction).
    fn memoize(&self, bucket: &str, incarnation: Uuid) {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        if let Some(entry) = entries.get_mut(bucket)
            && !entry.guards.is_empty()
        {
            if entry.guards.iter().any(|guard| guard.loss_probe.is_lost()) {
                entry.validated = None;
            } else {
                entry.validated = Some(incarnation);
            }
        }
    }

    /// Deregister a guard. Clears the memo when the last guard leaves or when
    /// the leaving guard lost its lock (lost coverage means a lifecycle write
    /// lock may have been granted, so the memo can no longer be trusted).
    fn exit(&self, bucket: &str, token: u64, lock_lost: bool) {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        if let Some(entry) = entries.get_mut(bucket) {
            entry.guards.retain(|guard| guard.token != token);
            if lock_lost {
                entry.validated = None;
            }
            if entry.guards.is_empty() {
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
    token: u64,
}

impl BucketIncarnationFenceGuard {
    pub(crate) fn is_lock_lost(&self) -> bool {
        self.inner.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost)
    }

    pub(crate) fn namespace_lock_guard(&self) -> Option<&NamespaceLockGuard> {
        self.inner.as_ref()
    }
}

impl Drop for BucketIncarnationFenceGuard {
    fn drop(&mut self) {
        let lost = self.is_lock_lost();
        self.registry.exit(&self.bucket, self.token, lost);
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
    pub(super) fn enter(&self, bucket: &str) -> FenceRegistration {
        self.registry.enter(bucket, LockLossProbe::from_guard(&self.inner))
    }

    pub(super) fn memoize(&self, bucket: &str, incarnation: Uuid) {
        self.registry.memoize(bucket, incarnation)
    }

    pub(super) fn lock_lost(&self) -> bool {
        self.inner.is_lock_lost()
    }

    pub(super) fn into_guard(self, bucket: &str, token: u64) -> BucketIncarnationFenceGuard {
        BucketIncarnationFenceGuard {
            inner: Some(self.inner),
            registry: self.registry,
            bucket: bucket.to_string(),
            token,
        }
    }

    /// Abandon the acquisition (validation failed): deregister and release.
    pub(super) fn abandon(self, bucket: &str, token: u64) {
        let lost = self.lock_lost();
        self.registry.exit(bucket, token, lost);
        drop(self.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use rustfs_lock::{LocalClient, LockRequest, LockType, NamespaceLock, ObjectKey};

    fn uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn live_probe() -> LockLossProbe {
        LockLossProbe::Test(Arc::new(std::sync::atomic::AtomicBool::new(false)))
    }

    fn controllable_probe() -> (LockLossProbe, Arc<std::sync::atomic::AtomicBool>) {
        let lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (LockLossProbe::Test(lost.clone()), lost)
    }

    fn lock_request(owner: &str) -> LockRequest {
        LockRequest::new(ObjectKey::new("b", "lifecycle"), LockType::Shared, owner)
            .with_acquire_timeout(Duration::from_millis(100))
            .with_ttl(Duration::from_millis(20))
            .with_refresh_interval(Duration::from_millis(50))
    }

    #[test]
    fn memo_valid_only_while_guards_overlap() {
        let reg = BucketFenceRegistry::default();

        let first = reg.enter("b", live_probe());
        assert_eq!(first.memoized, None, "first guard sees no memo");
        reg.memoize("b", uuid(1));
        let second = reg.enter("b", live_probe());
        assert_eq!(second.memoized, Some(uuid(1)), "overlapping guard reuses memo");
        reg.exit("b", first.token, false);
        reg.exit("b", second.token, false);

        // Coverage gap: all guards gone, memo must be dropped.
        let third = reg.enter("b", live_probe());
        assert_eq!(third.memoized, None, "post-gap guard must revalidate");
        reg.exit("b", third.token, false);
    }

    #[test]
    fn lost_lock_clears_memo_but_keeps_other_guards_registered() {
        let reg = BucketFenceRegistry::default();

        let first = reg.enter("b", live_probe());
        assert_eq!(first.memoized, None);
        reg.memoize("b", uuid(7));
        let second = reg.enter("b", live_probe());
        assert_eq!(second.memoized, Some(uuid(7)));

        // First guard exits reporting a lost lock: memo cleared even though
        // a second guard is still live.
        reg.exit("b", first.token, true);
        let third = reg.enter("b", live_probe());
        assert_eq!(third.memoized, None, "memo not trusted after a lost lock");
        reg.exit("b", second.token, false);
        reg.exit("b", third.token, false);
    }

    #[test]
    fn live_lost_guard_blocks_memo_reuse_before_drop() {
        let reg = BucketFenceRegistry::default();

        let (first_probe, first_lost) = controllable_probe();
        let first = reg.enter("b", first_probe);
        assert_eq!(first.memoized, None);
        reg.memoize("b", uuid(7));

        first_lost.store(true, std::sync::atomic::Ordering::SeqCst);

        let second = reg.enter("b", live_probe());
        assert_eq!(second.memoized, None, "live lost guard must force disk revalidation");
        reg.memoize("b", uuid(8));

        let third = reg.enter("b", live_probe());
        assert_eq!(third.memoized, None, "memo remains blocked while the lost guard is live");

        reg.exit("b", first.token, true);
        reg.memoize("b", uuid(8));
        let fourth = reg.enter("b", live_probe());
        assert_eq!(fourth.memoized, Some(uuid(8)), "memo resumes after lost coverage leaves");

        reg.exit("b", second.token, false);
        reg.exit("b", third.token, false);
        reg.exit("b", fourth.token, false);
    }

    #[tokio::test]
    async fn fence_pieces_forwards_distributed_lock_loss_to_registry() {
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("bucket-fence-test".to_string(), Arc::new(LocalClient::new()));
        let first_guard = lock
            .acquire_guard(&lock_request("first"))
            .await
            .expect("distributed lock acquisition should not fail")
            .expect("distributed lock quorum should be reached");
        let first_pieces = FencePieces {
            registry: registry.clone(),
            inner: first_guard,
        };

        let first = first_pieces.enter("b");
        assert_eq!(first.memoized, None);
        first_pieces.memoize("b", uuid(7));

        tokio::time::timeout(Duration::from_secs(2), first_pieces.inner.lock_lost_notified())
            .await
            .expect("non-renewed distributed guard should lose its lease");
        assert!(first_pieces.lock_lost(), "test guard should observe lost refresh quorum");

        let second_guard = lock
            .acquire_guard(&lock_request("second"))
            .await
            .expect("second distributed lock acquisition should not fail")
            .expect("second distributed lock quorum should be reached");
        let second_pieces = FencePieces {
            registry: registry.clone(),
            inner: second_guard,
        };
        let second = second_pieces.enter("b");
        assert_eq!(
            second.memoized, None,
            "a live distributed guard whose signal is lost must block memo reuse"
        );

        second_pieces.abandon("b", second.token);
        first_pieces.abandon("b", first.token);
    }

    #[test]
    fn buckets_are_isolated() {
        let reg = BucketFenceRegistry::default();
        let first = reg.enter("a", live_probe());
        assert_eq!(first.memoized, None);
        reg.memoize("a", uuid(1));
        let second = reg.enter("b", live_probe());
        assert_eq!(second.memoized, None, "memo does not leak across buckets");
        reg.exit("b", second.token, false);
        reg.exit("a", first.token, false);
    }

    #[test]
    fn memoize_without_live_guard_is_ignored() {
        let reg = BucketFenceRegistry::default();
        reg.memoize("b", uuid(9));
        let first = reg.enter("b", live_probe());
        assert_eq!(first.memoized, None);
        reg.exit("b", first.token, false);
    }
}
