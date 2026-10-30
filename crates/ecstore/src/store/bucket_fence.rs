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
//! bucket incarnation cannot have changed. Cold PUT and multipart requests
//! validate from disk independently, preserving their existing failure budgets
//! and typed errors. Once a validation succeeds, subsequent overlapping guards
//! reuse its result. Registration belongs to the real guard before validation
//! can suspend, including when a request is cancelled. When the node's last
//! guard drops — or any guard observes
//! `is_lock_lost` — the memo is cleared and the next PUT revalidates from
//! disk.
//!
//! The memo is deliberately per-node process state (not a cross-node cache):
//! its validity is derived purely from locks this process itself holds, so
//! best-effort peer cache invalidation (which is why the fence read from disk
//! in the first place) is irrelevant to its correctness.

use crate::error::{Result, StorageError};
use std::collections::HashMap;
use std::future::Future;
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

    /// Return the memoized incarnation id for `bucket` when at least one
    /// registered lifecycle read guard is currently held without lock loss.
    pub(crate) fn validated_incarnation(&self, bucket: &str) -> Option<Uuid> {
        let mut entries = self.entries.lock().expect("bucket fence registry poisoned");
        let entry = entries.get_mut(bucket)?;
        if entry.guards.iter().any(|guard| guard.loss_probe.is_lost()) {
            entry.validated = None;
            return None;
        }
        entry.validated
    }
}

/// Owns continuous lifecycle coverage and its incarnation validation memo.
/// Registration is established before validation can suspend, and is released
/// together with the real namespace guard on cancellation or the last owner.
pub struct BucketIncarnationFenceGuard {
    inner: Option<NamespaceLockGuard>,
    registry: Arc<BucketFenceRegistry>,
    bucket: String,
    token: Option<u64>,
    memoized: Option<Uuid>,
}

impl BucketIncarnationFenceGuard {
    pub(super) fn new(registry: Arc<BucketFenceRegistry>, bucket: &str, inner: NamespaceLockGuard, cacheable: bool) -> Self {
        let registration = cacheable.then(|| registry.enter(bucket, LockLossProbe::from_guard(&inner)));
        let token = registration.as_ref().map(|registration| registration.token);
        let memoized = registration.and_then(|registration| registration.memoized);
        Self {
            inner: Some(inner),
            registry,
            bucket: bucket.to_owned(),
            token,
            memoized,
        }
    }

    /// Snapshot holders validate independently through their held transaction
    /// guard. Cold reads stay concurrent and preserve each caller's typed error.
    pub(super) async fn validate<F, Fut>(&mut self, reuse_memo: bool, load: F) -> Result<Uuid>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Uuid>>,
    {
        let cacheable = reuse_memo && self.token.is_some();
        let current = match self.memoized.filter(|_| cacheable) {
            Some(current) => current,
            None => {
                let current = load().await?;
                self.ensure_held()?;
                if cacheable {
                    self.registry.memoize(&self.bucket, current);
                    self.memoized = Some(current);
                }
                current
            }
        };
        self.ensure_held()?;
        Ok(current)
    }

    fn ensure_held(&self) -> Result<()> {
        if self.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "bucket_incarnation_validation",
                bucket: self.bucket.clone(),
                object: crate::storage_api_contracts::bucket::BUCKET_LIFECYCLE_LOCK_OBJECT.to_owned(),
                required: 1,
                achieved: 0,
            });
        }
        Ok(())
    }

    /// Propagate lifecycle lock loss into the storage commit checks.
    /// The caller still owns this guard until the complete write tail drains.
    pub fn attach_to_object_options(&self, opts: &mut crate::object_api::ObjectOptions) {
        if let Some(guard) = self.namespace_lock_guard() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }
    }

    pub fn is_lock_lost(&self) -> bool {
        self.inner.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost)
    }

    pub(crate) fn namespace_lock_guard(&self) -> Option<&NamespaceLockGuard> {
        self.inner.as_ref()
    }
}

impl Drop for BucketIncarnationFenceGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.registry.exit(&self.bucket, token, self.is_lock_lost());
        }
        self.inner.take();
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
        assert_eq!(reg.validated_incarnation("b"), None);
        reg.memoize("b", uuid(7));
        assert_eq!(reg.validated_incarnation("b"), Some(uuid(7)));

        first_lost.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            reg.validated_incarnation("b"),
            None,
            "live lost guard must clear and block validated_incarnation"
        );

        let second = reg.enter("b", live_probe());
        assert_eq!(second.memoized, None, "live lost guard must force disk revalidation");
        reg.memoize("b", uuid(8));

        let third = reg.enter("b", live_probe());
        assert_eq!(third.memoized, None, "memo remains blocked while the lost guard is live");
        assert_eq!(reg.validated_incarnation("b"), None);

        reg.exit("b", first.token, true);
        reg.memoize("b", uuid(8));
        assert_eq!(reg.validated_incarnation("b"), Some(uuid(8)));
        let fourth = reg.enter("b", live_probe());
        assert_eq!(fourth.memoized, Some(uuid(8)), "memo resumes after lost coverage leaves");

        reg.exit("b", second.token, false);
        reg.exit("b", third.token, false);
        reg.exit("b", fourth.token, false);
    }

    #[tokio::test]
    async fn fence_guard_forwards_distributed_lock_loss_to_registry() {
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("bucket-fence-test".to_string(), Arc::new(LocalClient::new()));
        let first_guard = lock
            .acquire_guard(&lock_request("first"))
            .await
            .expect("acquire first")
            .expect("quorum");
        let mut first = BucketIncarnationFenceGuard::new(registry.clone(), "b", first_guard, true);
        first
            .validate(true, || async { Ok(uuid(7)) })
            .await
            .expect("seed initial validation");
        tokio::time::timeout(Duration::from_secs(2), first.namespace_lock_guard().expect("guard").lock_lost_notified())
            .await
            .expect("non-renewed guard should lose its lease");
        let second_guard = lock
            .acquire_guard(&lock_request("second"))
            .await
            .expect("acquire second")
            .expect("quorum");
        let second = BucketIncarnationFenceGuard::new(registry.clone(), "b", second_guard, true);
        assert!(second.memoized.is_none(), "a live lost guard must prevent memo reuse");
        assert!(
            matches!(
                first.validate(true, || async { Ok(uuid(7)) }).await,
                Err(StorageError::NamespaceLockQuorumUnavailable { .. })
            ),
            "lost guards cannot return cached success"
        );
        drop(first);
        drop(second);
        assert!(registry.entries.lock().expect("registry").is_empty());
    }

    #[tokio::test]
    async fn checkpoint_options_inherit_bucket_fence_lock_loss() {
        let lock = NamespaceLock::new("bucket-fence-options".to_string(), Arc::new(LocalClient::new()));
        let inner = lock
            .acquire_guard(&lock_request("options"))
            .await
            .expect("acquire")
            .expect("quorum");
        let fence = BucketIncarnationFenceGuard::new(Arc::default(), "b", inner, true);
        let mut opts = crate::object_api::ObjectOptions::default();
        fence.attach_to_object_options(&mut opts);
        let inherited = opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .expect("checkpoint inherits lifecycle guard");
        assert!(!inherited.is_lock_lost());
        tokio::time::timeout(Duration::from_secs(2), fence.namespace_lock_guard().expect("guard").lock_lost_notified())
            .await
            .expect("distributed guard expires");
        assert!(inherited.is_lock_lost(), "the actual pre-rename options must observe lifecycle lock loss");
    }

    async fn test_fence(
        registry: Arc<BucketFenceRegistry>,
        lock: &NamespaceLock,
        cacheable: bool,
    ) -> BucketIncarnationFenceGuard {
        let inner = lock
            .get_read_lock(ObjectKey::new("b", "lifecycle"), "validation", Duration::from_secs(5))
            .await
            .expect("acquire lifecycle read guard");
        BucketIncarnationFenceGuard::new(registry, "b", inner, cacheable)
    }

    #[tokio::test]
    async fn overlapping_validations_reuse_one_success_until_coverage_gap() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("memoized-validation".to_owned(), Arc::new(LocalClient::new()));
        let calls = AtomicUsize::new(0);
        let mut first = test_fence(registry.clone(), &lock, true).await;
        first
            .validate(true, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(uuid(7))
            })
            .await
            .expect("seed authoritative validation");
        let mut guards = Vec::new();
        for _ in 0..32 {
            guards.push(test_fence(registry.clone(), &lock, true).await);
        }
        for guard in &mut guards {
            assert_eq!(
                guard
                    .validate(true, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(uuid(7))
                    })
                    .await
                    .expect("reuse authoritative validation"),
                uuid(7)
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "32 overlapping guards must reuse the successful authoritative read"
        );
        drop(first);
        drop(guards);
        assert!(registry.entries.lock().expect("registry").is_empty());
        let mut fresh = test_fence(registry.clone(), &lock, true).await;
        assert_eq!(
            fresh
                .validate(true, || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(uuid(8))
                })
                .await
                .expect("validate after coverage gap"),
            uuid(8)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "coverage gap requires another authoritative read");
    }

    #[tokio::test]
    async fn cancelled_validation_deregisters_before_revalidation() {
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("cancelled-validation".to_owned(), Arc::new(LocalClient::new()));
        let mut guard = test_fence(registry.clone(), &lock, true).await;
        let mut cancelled = Box::pin(async move { guard.validate(true, std::future::pending::<Result<Uuid>>).await });
        assert!(futures::poll!(&mut cancelled).is_pending());
        assert_eq!(registry.entries.lock().expect("registry")["b"].guards.len(), 1);
        drop(cancelled);
        assert!(
            registry.entries.lock().expect("registry").is_empty(),
            "cancellation must deregister actual coverage"
        );
        let mut replacement = test_fence(registry.clone(), &lock, true).await;
        assert_eq!(
            replacement
                .validate(true, || async { Ok(uuid(9)) })
                .await
                .expect("fresh authoritative value"),
            uuid(9)
        );
    }

    #[tokio::test]
    async fn cold_validation_failures_stay_concurrent_and_preserve_typed_errors() {
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("parallel-validation-errors".to_owned(), Arc::new(LocalClient::new()));
        let mut first = test_fence(registry.clone(), &lock, true).await;
        let mut second = test_fence(registry.clone(), &lock, true).await;
        let entered = std::sync::atomic::AtomicUsize::new(0);
        let release = tokio::sync::Notify::new();
        let mut first_read = Box::pin(first.validate(true, || async {
            entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            release.notified().await;
            Err(StorageError::Lock(rustfs_lock::LockError::timeout(
                "first-resource",
                Duration::from_secs(5),
            )))
        }));
        let mut second_read = Box::pin(second.validate(true, || async {
            entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            release.notified().await;
            Err(StorageError::ErasureReadQuorum)
        }));
        assert!(futures::poll!(&mut first_read).is_pending());
        assert!(futures::poll!(&mut second_read).is_pending());
        assert_eq!(
            entered.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "cold failures must not queue behind one initializer"
        );
        release.notify_waiters();
        assert!(
            matches!(first_read.await, Err(StorageError::Lock(rustfs_lock::LockError::Timeout { resource, .. })) if resource == "first-resource")
        );
        assert!(matches!(second_read.await, Err(StorageError::ErasureReadQuorum)));
        assert_eq!(
            second
                .validate(true, || async { Ok(uuid(8)) })
                .await
                .expect("failed validation is retryable"),
            uuid(8)
        );
        drop(first);
        drop(second);
        assert!(registry.entries.lock().expect("registry").is_empty());
    }

    #[tokio::test]
    async fn independent_and_disabled_validation_bypass_successful_memo() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let registry = Arc::new(BucketFenceRegistry::default());
        let lock = NamespaceLock::new("independent-validation".to_owned(), Arc::new(LocalClient::new()));
        let mut first = test_fence(registry.clone(), &lock, true).await;
        first
            .validate(true, || async { Ok(uuid(7)) })
            .await
            .expect("seed successful memo");
        let mut snapshot = test_fence(registry.clone(), &lock, true).await;
        assert!(
            snapshot
                .validate(false, || async { Err(StorageError::ErasureReadQuorum) })
                .await
                .is_err(),
            "an Object Lock snapshot must validate its own transaction guard despite a warm memo"
        );
        drop(first);
        drop(snapshot);
        let calls = AtomicUsize::new(0);
        let mut disabled = test_fence(registry.clone(), &lock, false).await;
        for current in [uuid(8), uuid(9)] {
            assert_eq!(
                disabled
                    .validate(true, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(current)
                    })
                    .await
                    .expect("uncached validation"),
                current
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            registry.entries.lock().expect("registry").is_empty(),
            "disabled locking cannot register coverage"
        );
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
