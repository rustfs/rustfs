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

use crate::cache::{ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup};
use crate::config::ObjectDataCacheConfig;
use crate::entry::{ObjectDataCacheEntry, projected_weight};
use crate::index::{ObjectDataCacheGenerationalInsertResult, ObjectDataCacheIdentityIndex};
use crate::key::{ObjectDataCacheIdentity, ObjectDataCacheKey};
use crate::memory::ObjectDataCacheMemoryGate;
use crate::singleflight::{ObjectDataCacheSingleflight, ObjectDataCacheSingleflightAcquire};
use crate::stats::ObjectDataCacheStats;
use bytes::Bytes;
use moka::future::{Cache, FutureExt};
use moka::ops::compute;
use std::future::ready;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore, watch};

#[derive(Debug)]
struct ClearFence {
    state: Mutex<ClearFenceState>,
    quiesced: Notify,
}

#[derive(Debug)]
struct ClearFenceState {
    /// `None` permanently rejects fills after an impossible counter failure.
    generation: Option<u64>,
    active_fills: usize,
    clear: Option<Arc<ClearOperation>>,
}

#[derive(Debug)]
struct ClearOperation {
    completion: watch::Sender<Option<ObjectDataCacheInvalidationResult>>,
}

#[derive(Debug)]
struct FillGenerationGuard {
    fence: Arc<ClearFence>,
    generation: u64,
}

impl ClearFence {
    fn new() -> Self {
        Self {
            state: Mutex::new(ClearFenceState {
                generation: Some(0),
                active_fills: 0,
                clear: None,
            }),
            quiesced: Notify::new(),
        }
    }

    fn try_register_fill(self: &Arc<Self>) -> Option<FillGenerationGuard> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.clear.is_some() {
            return None;
        }
        let generation = state.generation?;
        let Some(active_fills) = state.active_fills.checked_add(1) else {
            state.generation = None;
            return None;
        };
        state.active_fills = active_fills;
        Some(FillGenerationGuard {
            fence: Arc::clone(self),
            generation,
        })
    }

    fn begin_clear(&self) -> (Arc<ClearOperation>, bool) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(operation) = &state.clear {
            return (Arc::clone(operation), false);
        }

        let operation = Arc::new(ClearOperation::new());
        state.clear = Some(Arc::clone(&operation));
        state.generation = state.generation.and_then(|generation| generation.checked_add(1));
        (operation, true)
    }

    async fn wait_for_active_fills(&self) {
        loop {
            let notified = self.quiesced.notified();
            if self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active_fills
                == 0
            {
                return;
            }
            notified.await;
        }
    }

    fn finish_clear(&self, operation: &Arc<ClearOperation>, result: ObjectDataCacheInvalidationResult) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.clear.as_ref().is_some_and(|current| Arc::ptr_eq(current, operation)) {
            return;
        }

        let next_generation = state.generation.and_then(|generation| generation.checked_add(1));
        state.generation = next_generation;
        operation.complete(result);
        if next_generation.is_some() {
            state.clear = None;
        }
    }
}

impl ClearOperation {
    fn new() -> Self {
        let (completion, _) = watch::channel(None);
        Self { completion }
    }

    async fn wait(&self) -> ObjectDataCacheInvalidationResult {
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(result) = *completion.borrow_and_update() {
                return result;
            }
            let _ = completion.changed().await;
        }
    }

    fn complete(&self, result: ObjectDataCacheInvalidationResult) {
        self.completion.send_replace(Some(result));
    }
}

impl FillGenerationGuard {
    fn is_current(&self) -> bool {
        let state = self.fence.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.clear.is_none() && state.generation == Some(self.generation)
    }
}

impl Drop for FillGenerationGuard {
    fn drop(&mut self) {
        let mut state = self.fence.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(active_fills) = state.active_fills.checked_sub(1) else {
            state.generation = None;
            return;
        };
        state.active_fills = active_fills;
        if state.active_fills == 0 {
            // There is exactly one owned clear drain. `notify_one` retains a
            // permit if it reaches zero between the state check and `.await`.
            self.fence.quiesced.notify_one();
        }
    }
}

/// Weighted Moka backend for reusable object bodies.
#[derive(Debug)]
pub struct MokaBackend {
    cache: Cache<ObjectDataCacheKey, Arc<ObjectDataCacheEntry>>,
    index: Arc<ObjectDataCacheIdentityIndex>,
    singleflight: ObjectDataCacheSingleflight,
    memory_gate: ObjectDataCacheMemoryGate,
    /// Cache capacity in weighted bytes. The derived growth headroom remains an
    /// input to the compatible memory-gate API, although live allocation claims
    /// are no longer capped by cache residency (backlog#1331).
    max_capacity: u64,
    /// Bounds the number of concurrent distinct-key fills. Singleflight only
    /// dedups per key, so without this limiter distinct-key fills are unbounded.
    fill_semaphore: Arc<Semaphore>,
    next_generation: AtomicU64,
    /// Serializes global clears and fences fill publication by generation.
    clear_fence: Arc<ClearFence>,
    /// Test-only barrier that pauses a fill between the index insert and the
    /// cache insert so the fill-vs-invalidation race can be driven deterministically.
    #[cfg(test)]
    fill_barrier: std::sync::Mutex<Option<Arc<FillBarrier>>>,
    /// Test-only barrier that pauses identity-budget eviction after the index
    /// selects a victim and before the cache removal starts.
    #[cfg(test)]
    identity_eviction_barrier: std::sync::Mutex<Option<Arc<FillBarrier>>>,
    /// Test-only blocking rendezvous inside the conditional compute closure.
    #[cfg(test)]
    identity_compute_barrier: std::sync::Mutex<Option<Arc<ComputeBarrier>>>,
    #[cfg(test)]
    fill_before_index_barrier: std::sync::Mutex<Option<Arc<FillBarrier>>>,
    #[cfg(test)]
    clear_barrier: std::sync::Mutex<Option<Arc<FillBarrier>>>,
    #[cfg(test)]
    clear_joined: Arc<Semaphore>,
}

/// Test-only rendezvous that pauses a fill between the index insert and the
/// cache insert. The fill signals `reached` and blocks on `release`; the test
/// waits for `reached`, drives an interleaving, then grants `release`.
#[cfg(test)]
#[derive(Debug)]
struct FillBarrier {
    reached: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    completed: tokio::sync::Semaphore,
}

#[cfg(test)]
#[derive(Debug)]
struct ComputeBarrier {
    reached: tokio::sync::Semaphore,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}

#[cfg(test)]
impl ComputeBarrier {
    fn new() -> Self {
        Self {
            reached: tokio::sync::Semaphore::new(0),
            released: std::sync::Mutex::new(false),
            release: std::sync::Condvar::new(),
        }
    }

    fn wait(&self) {
        self.reached.add_permits(1);
        let mut released = self.released.lock().unwrap_or_else(|p| p.into_inner());
        while !*released {
            released = self.release.wait(released).unwrap_or_else(|p| p.into_inner());
        }
    }

    async fn wait_until_reached(&self) {
        if let Ok(permit) = self.reached.acquire().await {
            permit.forget();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.release.notify_all();
    }
}

#[cfg(test)]
impl FillBarrier {
    fn new() -> Self {
        Self {
            reached: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            completed: tokio::sync::Semaphore::new(0),
        }
    }

    /// Called from inside the fill: announce arrival, then block until released.
    async fn wait(&self) {
        self.reached.add_permits(1);
        if let Ok(permit) = self.release.acquire().await {
            permit.forget();
        }
    }

    /// Called from the test: block until a fill reaches the barrier.
    async fn wait_until_reached(&self) {
        if let Ok(permit) = self.reached.acquire().await {
            permit.forget();
        }
    }

    /// Called from the test: let the paused fill proceed.
    fn release(&self) {
        self.release.add_permits(1);
    }

    fn complete(&self) {
        self.completed.add_permits(1);
    }

    async fn wait_until_completed(&self) {
        if let Ok(permit) = self.completed.acquire().await {
            permit.forget();
        }
    }
}

impl MokaBackend {
    /// Creates a new backend from the validated configuration.
    pub fn new(
        config: &ObjectDataCacheConfig,
        stats: Arc<ObjectDataCacheStats>,
    ) -> Result<Self, crate::error::ObjectDataCacheConfigError> {
        let max_capacity = config.resolved_max_bytes()?;
        let ttl = config.ttl;
        let time_to_idle = config.time_to_idle;

        // Size the fill-concurrency limiter to
        // min(fill_concurrency_per_cpu * available_parallelism, fill_concurrency_max).
        // Both knobs are validated as non-zero, so the result is at least 1.
        let parallelism = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let fill_permits = usize::from(config.fill_concurrency_per_cpu)
            .saturating_mul(parallelism)
            .min(usize::from(config.fill_concurrency_max))
            .max(1);

        // The identity index tracks the keys cached per object so that
        // invalidation can find them without a full-cache scan. Moka evicts
        // entries on its own (TTL, time-to-idle, capacity-LRU) without going
        // through `invalidate_object`, so without a listener those evicted keys
        // would linger in the index forever and leak memory. Hold only a `Weak`
        // reference in the listener to avoid an Arc cycle keeping the index (and
        // thus the cache) alive.
        let index = Arc::new(ObjectDataCacheIdentityIndex::new(usize::from(config.identity_keys_max)));
        let index_for_eviction = Arc::downgrade(&index);

        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .weigher(|key, value: &Arc<ObjectDataCacheEntry>| value.estimated_weight(key))
            .time_to_live(ttl)
            .time_to_idle(time_to_idle)
            .async_eviction_listener(move |key, value, cause| {
                let index_for_eviction = index_for_eviction.clone();
                async move {
                    // Explicit removals and replacements are already reconciled
                    // with the index by the caller; only true evictions leak.
                    if !cause.was_evicted() {
                        return;
                    }
                    let Some(index) = index_for_eviction.upgrade() else {
                        return;
                    };
                    let identity = ObjectDataCacheIdentity::new(Arc::clone(&key.bucket), Arc::clone(&key.object));
                    // Prune by generation token: moka delivers the evicted value,
                    // so the index only drops the key when this evicted entry is
                    // still the one it tracks. An inline `Expired` upsert or a
                    // deferred `Size` notification for a superseded generation
                    // then cannot remove the key a fresh refill just registered.
                    index.remove_generation(&identity, &key, value.generation()).await;
                }
                .boxed()
            })
            .build();

        Ok(Self {
            cache,
            index,
            singleflight: ObjectDataCacheSingleflight::new(Arc::clone(&stats)),
            memory_gate: ObjectDataCacheMemoryGate::new(config, stats),
            max_capacity,
            fill_semaphore: Arc::new(Semaphore::new(fill_permits)),
            next_generation: AtomicU64::new(0),
            clear_fence: Arc::new(ClearFence::new()),
            #[cfg(test)]
            fill_barrier: std::sync::Mutex::new(None),
            #[cfg(test)]
            identity_eviction_barrier: std::sync::Mutex::new(None),
            #[cfg(test)]
            identity_compute_barrier: std::sync::Mutex::new(None),
            #[cfg(test)]
            fill_before_index_barrier: std::sync::Mutex::new(None),
            #[cfg(test)]
            clear_barrier: std::sync::Mutex::new(None),
            #[cfg(test)]
            clear_joined: Arc::new(Semaphore::new(0)),
        })
    }

    /// Installs a test-only fill barrier and returns it to the caller.
    #[cfg(test)]
    fn install_fill_barrier(&self) -> Arc<FillBarrier> {
        let barrier = Arc::new(FillBarrier::new());
        *self.fill_barrier.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&barrier));
        barrier
    }

    #[cfg(test)]
    fn install_identity_eviction_barrier(&self) -> Arc<FillBarrier> {
        let barrier = Arc::new(FillBarrier::new());
        *self.identity_eviction_barrier.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&barrier));
        barrier
    }

    #[cfg(test)]
    fn install_identity_compute_barrier(&self) -> Arc<ComputeBarrier> {
        let barrier = Arc::new(ComputeBarrier::new());
        *self.identity_compute_barrier.lock().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&barrier));
        barrier
    }

    #[cfg(test)]
    fn install_fill_before_index_barrier(&self) -> Arc<FillBarrier> {
        let barrier = Arc::new(FillBarrier::new());
        *self
            .fill_before_index_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&barrier));
        barrier
    }

    #[cfg(test)]
    fn install_clear_barrier(&self) -> Arc<FillBarrier> {
        let barrier = Arc::new(FillBarrier::new());
        *self.clear_barrier.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&barrier));
        barrier
    }

    #[cfg(test)]
    async fn wait_until_clear_joined(&self) {
        if let Ok(permit) = self.clear_joined.acquire().await {
            permit.forget();
        }
    }
    /// Returns the current cache entry count.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns the approximate weighted size of cached entries.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub(crate) const fn max_capacity(&self) -> u64 {
        self.max_capacity
    }

    /// Looks up a cached body for the supplied plan.
    pub async fn lookup_body(&self, plan: &ObjectDataCacheGetPlan) -> ObjectDataCacheLookup {
        let ObjectDataCacheGetPlan::Cacheable { key } = plan else {
            return ObjectDataCacheLookup::SkipNotCacheable;
        };

        match self.cache.get(key).await {
            Some(entry) => ObjectDataCacheLookup::Hit(entry.bytes()),
            None => ObjectDataCacheLookup::Miss,
        }
    }

    /// Inserts a cached body for the supplied plan.
    pub async fn fill_body(&self, plan: &ObjectDataCacheGetPlan, bytes: Bytes) -> ObjectDataCacheFillResult {
        let ObjectDataCacheGetPlan::Cacheable { key } = plan else {
            return ObjectDataCacheFillResult::SkippedNotCacheable;
        };
        let body_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if projected_weight(key, body_bytes) > self.max_capacity {
            return ObjectDataCacheFillResult::SkippedNotCacheable;
        }

        // The caller already owns the body, so a non-leader gains nothing by
        // waiting for another request's leader — it just skips its own fill.
        // Report it as JoinedInflightFill (ODC-18): it did no fill work, so it
        // must not be counted as an insert or record fill bytes. It also does
        // NOT wait for the leader — waiting only matters when the joiner needs
        // the result value, which the GET path never does (ODC-15).
        let ObjectDataCacheSingleflightAcquire::Leader(leader) = self.singleflight.try_acquire(key.clone()) else {
            return ObjectDataCacheFillResult::JoinedInflightFill;
        };

        // Registration and the clear transition share one short state lock,
        // closing the check/register race without holding a lock across body
        // processing or cache/index I/O. Only the singleflight leader can
        // publish, so duplicate fills stay off this global hot-path lock.
        let Some(fill_generation) = self.clear_fence.try_register_fill() else {
            return leader.finish(ObjectDataCacheFillResult::SkippedInvalidationRace);
        };

        #[cfg(test)]
        let fill_before_index_barrier = self
            .fill_before_index_barrier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        #[cfg(test)]
        if let Some(barrier) = fill_before_index_barrier {
            barrier.wait().await;
        }

        if !fill_generation.is_current() {
            return leader.finish(ObjectDataCacheFillResult::SkippedInvalidationRace);
        }

        // Bound distinct-key fill concurrency after winning leadership and
        // before the memory-gate check. Reject rather than queue: queuing would
        // reintroduce the GET-latency coupling this path is meant to avoid.
        let permit = match Arc::clone(&self.fill_semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return leader.finish(ObjectDataCacheFillResult::SkippedFillConcurrency),
        };

        // Keep passing cache growth headroom for compatibility with the public
        // gate API. Allocation-scoped reservations deliberately do not cap by
        // this value because an evicted entry can still have live body clones.
        let cache_growth_headroom = self.max_capacity.saturating_sub(self.cache.weighted_size());
        if !self.memory_gate.try_claim_buffered(body_bytes, cache_growth_headroom) {
            return leader.finish(ObjectDataCacheFillResult::SkippedMemoryPressure);
        }

        let identity = ObjectDataCacheIdentity::new(Arc::clone(&key.bucket), Arc::clone(&key.object));
        // Keep keys that are still cached or still mid-fill: another in-flight
        // fill for a different key of this identity may have registered in the
        // index but not yet published its cache entry, and must not be pruned.
        self.index
            .prune_missing(&identity, |candidate| {
                self.cache.contains_key(candidate) || self.singleflight.has_inflight(candidate)
            })
            .await;

        // The counter supplies uniqueness only; cache/index publication provides
        // the synchronization, so relaxed ordering is sufficient here.
        let generation = match self
            .next_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(1))
        {
            Ok(previous) => previous + 1,
            Err(_) => return leader.finish(ObjectDataCacheFillResult::SkippedIdentityOverflow),
        };
        let entry = Arc::new(ObjectDataCacheEntry::with_generation(bytes, generation));

        // Run the register/insert/recheck/undo sequence in a spawned task and
        // await its handle. The GET path already detaches the whole fill from
        // the response future (ODC-15), but this inner spawn is still required:
        // once the index key is registered, the recheck/undo must complete even
        // if the enclosing fill future is aborted (e.g. the detached fill task
        // is cancelled). Aborting the outer future then only detaches this
        // JoinHandle; the task still finishes the undo, so a stale body cannot
        // survive with no index entry. Dropping the leader releases the key.
        let cache = self.cache.clone();
        let index = Arc::clone(&self.index);
        let fill_key = key.clone();
        let fill_identity = identity.clone();
        #[cfg(test)]
        let fill_barrier = self.fill_barrier.lock().unwrap_or_else(|p| p.into_inner()).clone();
        #[cfg(test)]
        let identity_eviction_barrier = self
            .identity_eviction_barrier
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        #[cfg(test)]
        let identity_compute_barrier = self
            .identity_compute_barrier
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();

        let handle = tokio::spawn(async move {
            // Hold the fill-concurrency permit until the register/insert/undo
            // sequence completes, then release it on task exit.
            let _permit = permit;
            // Keep the generation registered through the complete
            // register/publish/recheck/undo sequence. If the outer caller is
            // cancelled, this owned task still acknowledges the clear fence.
            let fill_generation = fill_generation;
            if !fill_generation.is_current() {
                return ObjectDataCacheFillResult::SkippedInvalidationRace;
            }
            // Register the key in the identity index BEFORE the entry becomes
            // visible in the cache, so a concurrent invalidation always finds it.
            match index
                .insert_generation(fill_identity.clone(), fill_key.clone(), generation)
                .await
            {
                ObjectDataCacheGenerationalInsertResult::Inserted { evicted } => {
                    for evicted in evicted {
                        #[cfg(test)]
                        if let Some(barrier) = &identity_eviction_barrier {
                            barrier.wait().await;
                        }
                        let expected_generation = evicted.generation;
                        #[cfg(test)]
                        let compute_barrier = identity_compute_barrier.clone();
                        let _ = cache
                            .entry(evicted.key)
                            .and_compute_with(move |current| {
                                let remove = current
                                    .as_ref()
                                    .is_some_and(|entry| entry.value().generation() == expected_generation);
                                #[cfg(test)]
                                if let Some(barrier) = compute_barrier {
                                    barrier.wait();
                                }
                                ready(if remove { compute::Op::Remove } else { compute::Op::Nop })
                            })
                            .await;
                        #[cfg(test)]
                        if let Some(barrier) = &identity_eviction_barrier {
                            barrier.complete();
                        }
                    }
                }
                ObjectDataCacheGenerationalInsertResult::Duplicate => {}
            }

            #[cfg(test)]
            if let Some(barrier) = fill_barrier {
                barrier.wait().await;
            }

            if !fill_generation.is_current() {
                // Leave the registered key for the owned clear drain. Besides
                // making the acknowledgement visible to clear, this preserves
                // the pre-existing invalidation outcome count for admin stats.
                return ObjectDataCacheFillResult::SkippedInvalidationRace;
            }

            let _ = cache
                .entry(fill_key.clone())
                .and_compute_with(move |_| ready(compute::Op::Put(entry)))
                .await;

            // An invalidation may have raced between the index and cache
            // inserts; re-check the index and undo the fill so the stale
            // body cannot outlive the invalidation.
            let indexed = index.contains_key(&fill_identity, &fill_key).await;
            if fill_generation.is_current() && indexed {
                ObjectDataCacheFillResult::Inserted
            } else {
                cache.remove(&fill_key).await;
                if fill_generation.is_current() {
                    index.remove_generation(&fill_identity, &fill_key, generation).await;
                }
                ObjectDataCacheFillResult::SkippedInvalidationRace
            }
        });

        let result = handle.await.unwrap_or(ObjectDataCacheFillResult::SkippedInvalidationRace);

        leader.finish(result)
    }

    /// Conservatively invalidates all cached keys matching the object identity.
    ///
    /// The identity index is authoritative: fills register the key in the
    /// index before the entry becomes visible in the cache (and undo the fill
    /// if an invalidation raced in between), so no full-cache scan fallback is
    /// needed when the index has no entry for the identity.
    pub async fn invalidate_object(&self, identity: &ObjectDataCacheIdentity) -> ObjectDataCacheInvalidationResult {
        let keys_to_remove = self.index.remove_identity(identity).await;
        // ODC-36: report the removal count so the facade can label the
        // invalidation outcome (removed vs noop) and skip the gauge refresh on
        // the no-op path. The vast majority of invalidations touch identities
        // that were never cached.
        let removed = keys_to_remove.len();
        for key in keys_to_remove {
            self.cache.remove(&key).await;
        }

        Self::invalidation_result(removed)
    }

    /// Invalidates every cached key of an identity in `bucket` whose object key
    /// starts with `prefix`. Full index scan; rare force-delete/admin path only.
    pub async fn invalidate_prefix(&self, bucket: &str, prefix: &str) -> ObjectDataCacheInvalidationResult {
        let keys_to_remove = self
            .index
            .remove_matching(|identity| identity.bucket.as_ref() == bucket && identity.object.starts_with(prefix))
            .await;
        let removed = keys_to_remove.len();
        for key in keys_to_remove {
            self.cache.remove(&key).await;
        }
        Self::invalidation_result(removed)
    }

    /// Invalidates every cached key of every identity in `bucket`. Full index
    /// scan; rare bucket-delete/admin path only.
    pub async fn invalidate_bucket(&self, bucket: &str) -> ObjectDataCacheInvalidationResult {
        let keys_to_remove = self
            .index
            .remove_matching(|identity| identity.bucket.as_ref() == bucket)
            .await;
        let removed = keys_to_remove.len();
        for key in keys_to_remove {
            self.cache.remove(&key).await;
        }
        Self::invalidation_result(removed)
    }

    /// Drops every cached body and resets the identity index. Concurrent clear
    /// callers join one owned drain, so cancelling a caller cannot cancel the
    /// operation or reopen its fill generation. Lookups remain lock-free and
    /// may observe entries until the drain removes them; fills started while
    /// the clear is active skip publication.
    pub async fn clear(&self) -> ObjectDataCacheInvalidationResult {
        let (operation, should_start) = self.clear_fence.begin_clear();
        #[cfg(test)]
        if !should_start {
            self.clear_joined.add_permits(1);
        }
        if should_start {
            let cache = self.cache.clone();
            let index = Arc::clone(&self.index);
            let clear_fence = Arc::clone(&self.clear_fence);
            let owned_operation = Arc::clone(&operation);
            #[cfg(test)]
            let clear_barrier = self
                .clear_barrier
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();

            tokio::spawn(async move {
                #[cfg(test)]
                if let Some(barrier) = clear_barrier {
                    barrier.wait().await;
                }

                clear_fence.wait_for_active_fills().await;
                let result = Self::drain_all(cache, index).await;
                clear_fence.finish_clear(&owned_operation, result);
            });
        }

        operation.wait().await
    }

    async fn drain_all(
        cache: Cache<ObjectDataCacheKey, Arc<ObjectDataCacheEntry>>,
        index: Arc<ObjectDataCacheIdentityIndex>,
    ) -> ObjectDataCacheInvalidationResult {
        let keys_to_remove = index.remove_matching(|_| true).await;
        let removed = keys_to_remove.len();
        for key in keys_to_remove {
            cache.remove(&key).await;
        }

        // Moka maintenance is still needed to retire queued removals, but clear
        // must not rely on lazy invalidation alone: under heavy contention an
        // entry can remain physically resident (and still counted) even though
        // the invalidation fence already hides it from lookups. Sweep both the
        // entries visible to iteration and a fallback invalidate_all fence so
        // delayed internal writes cannot strand a counted entry.
        for _ in 0..256 {
            cache.run_pending_tasks().await;
            let lingering_keys: Vec<_> = cache.iter().map(|(key, _)| key.as_ref().clone()).collect();
            for key in lingering_keys {
                cache.remove(&key).await;
            }

            cache.invalidate_all();
            cache.run_pending_tasks().await;

            if cache.entry_count() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        Self::invalidation_result(removed)
    }

    const fn invalidation_result(removed: usize) -> ObjectDataCacheInvalidationResult {
        if removed > 0 {
            ObjectDataCacheInvalidationResult::Removed { keys: removed }
        } else {
            ObjectDataCacheInvalidationResult::NoOp
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClearFence, FillGenerationGuard, MokaBackend};
    use crate::cache::{
        ObjectDataCacheFillResult, ObjectDataCacheGetPlan, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup,
    };
    use crate::config::{ObjectDataCacheConfig, ObjectDataCacheMode};
    use crate::key::{ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};
    use crate::stats::ObjectDataCacheStats;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Duration;

    /// Fills here must succeed regardless of the live memory reading, which
    /// differs between a developer host and a CI container, so the gate is
    /// opted out of. Tests that exercise the gate re-enable it explicitly.
    fn enabled_config() -> ObjectDataCacheConfig {
        ObjectDataCacheConfig {
            mode: ObjectDataCacheMode::FillMaterializeEnabled,
            max_bytes: 8_388_608,
            max_memory_percent: 5,
            max_entry_bytes: 1_048_576,
            ttl: Duration::from_millis(100),
            time_to_idle: Duration::from_millis(100),
            min_free_memory_percent: 0,
            // >= 2 so concurrent-fill tests get at least two permits even on a
            // single-CPU CI runner (permits = min(per_cpu * parallelism, max)).
            fill_concurrency_per_cpu: 2,
            fill_concurrency_max: 32,
            identity_keys_max: 16,
        }
    }

    fn memory_gated_config() -> ObjectDataCacheConfig {
        ObjectDataCacheConfig {
            min_free_memory_percent: 20,
            ..enabled_config()
        }
    }

    fn cacheable_plan(object: &str, etag: &str) -> ObjectDataCacheGetPlan {
        cacheable_plan_with_size(object, etag, 5)
    }

    fn cacheable_plan_with_size(object: &str, etag: &str, size: u64) -> ObjectDataCacheGetPlan {
        ObjectDataCacheGetPlan::Cacheable {
            key: ObjectDataCacheKey::new("bucket", object, None, etag, size, ObjectDataCacheBodyVariant::FullObjectPlainV1),
        }
    }

    fn versioned_plan(object: &str, version: &str, etag: &str) -> ObjectDataCacheGetPlan {
        ObjectDataCacheGetPlan::Cacheable {
            key: ObjectDataCacheKey::new("bucket", object, Some(version), etag, 5, ObjectDataCacheBodyVariant::FullObjectPlainV1),
        }
    }

    fn identity_budget_fixture() -> (Arc<MokaBackend>, ObjectDataCacheGetPlan, ObjectDataCacheGetPlan, ObjectDataCacheGetPlan) {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        config.identity_keys_max = 2;
        (
            Arc::new(MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build")),
            versioned_plan("object", "v1", "etag-1"),
            versioned_plan("object", "v2", "etag-2"),
            versioned_plan("object", "v3", "etag-3"),
        )
    }

    #[tokio::test]
    async fn moka_backend_round_trips_cached_body() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let fill = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(fill, ObjectDataCacheFillResult::Inserted));
        assert!(matches!(lookup, ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }

    #[tokio::test]
    async fn moka_backend_never_reports_inserted_for_overweight_entry() {
        let mut config = enabled_config();
        config.max_bytes = 8 * 1024;
        config.max_entry_bytes = 4 * 1024;
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        // Projected weight is 8193 bytes, one byte over max_capacity.
        let object = "o".repeat(4022);
        let plan = cacheable_plan_with_size(&object, "e", 4 * 1024);
        let body = Bytes::from(vec![0_u8; 4 * 1024]);

        let fill = backend.fill_body(&plan, body).await;
        backend.cache.run_pending_tasks().await;

        assert_eq!(fill, ObjectDataCacheFillResult::SkippedNotCacheable);
        assert!(matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn accepted_boundary_entry_remains_hit_after_pending_maintenance() {
        let mut config = enabled_config();
        config.max_bytes = 8 * 1024;
        config.max_entry_bytes = 4 * 1024;
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        // Projected weight is exactly max_capacity: 4096 + 6 + 4021 + 4 + 1 + 64.
        let object = "o".repeat(4021);
        let plan = cacheable_plan_with_size(&object, "e", 4 * 1024);
        let body = Bytes::from(vec![0_u8; 4 * 1024]);

        let fill = backend.fill_body(&plan, body).await;
        backend.cache.run_pending_tasks().await;
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(fill, ObjectDataCacheFillResult::Inserted);
        assert_eq!(backend.weighted_size(), config.max_bytes, "Moka must charge the exact projected weight");
        assert!(matches!(lookup, ObjectDataCacheLookup::Hit(ref bytes) if bytes.len() == 4 * 1024));
    }

    #[tokio::test]
    async fn moka_backend_invalidates_matching_identity() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_a = cacheable_plan("object-a", "etag-a");
        let plan_b = cacheable_plan("object-b", "etag-b");

        let _ = backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;

        // object-a has exactly one cached key, so invalidation reports Removed { keys: 1 }.
        let result = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "object-a"))
            .await;
        let lookup_a = backend.lookup_body(&plan_a).await;
        let lookup_b = backend.lookup_body(&plan_b).await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 1 });
        assert!(matches!(lookup_a, ObjectDataCacheLookup::Miss));
        assert!(matches!(lookup_b, ObjectDataCacheLookup::Hit(_)));
    }

    // ODC-36: invalidating an identity that was never cached reports NoOp so the
    // facade can skip the cache-state gauge refresh on the common no-op path.
    #[tokio::test]
    async fn moka_backend_invalidate_uncached_identity_is_noop() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");

        let result = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "never-cached"))
            .await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::NoOp);
    }

    fn bucketed_plan(bucket: &str, object: &str, etag: &str) -> ObjectDataCacheGetPlan {
        ObjectDataCacheGetPlan::Cacheable {
            key: ObjectDataCacheKey::new(bucket, object, None, etag, 5, ObjectDataCacheBodyVariant::FullObjectPlainV1),
        }
    }

    // ODC-27: prefix invalidation drops only the identities under the prefix and
    // leaves every other cached body intact.
    #[tokio::test]
    async fn moka_backend_invalidate_prefix_removes_only_matching_prefix() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_a = cacheable_plan("photos/a.jpg", "etag-a");
        let plan_b = cacheable_plan("photos/b.jpg", "etag-b");
        let plan_c = cacheable_plan("videos/c.mp4", "etag-c");

        let _ = backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;
        let _ = backend.fill_body(&plan_c, Bytes::from_static(b"ccccc")).await;

        let result = backend.invalidate_prefix("bucket", "photos/").await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 2 });
        assert!(matches!(backend.lookup_body(&plan_a).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(backend.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss));
        assert!(
            matches!(backend.lookup_body(&plan_c).await, ObjectDataCacheLookup::Hit(_)),
            "an object outside the prefix must survive"
        );
    }

    // ODC-27: a prefix that matches nothing cached is a no-op.
    #[tokio::test]
    async fn moka_backend_invalidate_prefix_uncached_is_noop() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let _ = backend
            .fill_body(&cacheable_plan("photos/a.jpg", "e"), Bytes::from_static(b"aaaaa"))
            .await;

        let result = backend.invalidate_prefix("bucket", "videos/").await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::NoOp);
    }

    // ODC-28: bucket invalidation drops every identity in the bucket and leaves
    // other buckets untouched.
    #[tokio::test]
    async fn moka_backend_invalidate_bucket_removes_only_that_bucket() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_a = bucketed_plan("bucket-a", "o1", "etag-a");
        let plan_b = bucketed_plan("bucket-a", "o2", "etag-b");
        let plan_other = bucketed_plan("bucket-b", "o3", "etag-c");

        let _ = backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;
        let _ = backend.fill_body(&plan_other, Bytes::from_static(b"ccccc")).await;

        let result = backend.invalidate_bucket("bucket-a").await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 2 });
        assert!(matches!(backend.lookup_body(&plan_a).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(backend.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss));
        assert!(
            matches!(backend.lookup_body(&plan_other).await, ObjectDataCacheLookup::Hit(_)),
            "an object in another bucket must survive a bucket flush"
        );
    }

    // ODC-C2: clear drops every cached body and empties the identity index.
    #[tokio::test]
    async fn moka_backend_clear_removes_everything() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_a = bucketed_plan("bucket-a", "o1", "etag-a");
        let plan_b = bucketed_plan("bucket-b", "o2", "etag-b");

        let _ = backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await;
        let _ = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;

        let result = backend.clear().await;

        assert_eq!(result, ObjectDataCacheInvalidationResult::Removed { keys: 2 });
        assert!(matches!(backend.lookup_body(&plan_a).await, ObjectDataCacheLookup::Miss));
        assert!(matches!(backend.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss));
        assert_eq!(backend.entry_count(), 0, "clear must drain cached entries before returning");
        assert_eq!(backend.index.identity_count().await, 0, "clear must empty the identity index");
    }

    #[tokio::test]
    async fn moka_backend_clear_empty_cache_is_noop() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");

        assert_eq!(backend.clear().await, ObjectDataCacheInvalidationResult::NoOp);
    }

    #[tokio::test]
    async fn clear_fences_fill_started_before_index_registration() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let fill_barrier = backend.install_fill_before_index_barrier();
        let clear_barrier = backend.install_clear_barrier();
        let plan = cacheable_plan("object", "etag-a");

        let fill_backend = Arc::clone(&backend);
        let fill_plan = plan.clone();
        let fill = tokio::spawn(async move { fill_backend.fill_body(&fill_plan, Bytes::from_static(b"hello")).await });
        fill_barrier.wait_until_reached().await;

        let clear_backend = Arc::clone(&backend);
        let clear = tokio::spawn(async move { clear_backend.clear().await });
        clear_barrier.wait_until_reached().await;
        clear_barrier.release();
        assert!(
            !clear.is_finished(),
            "clear must wait for a pre-registration fill to acknowledge the fence"
        );

        fill_barrier.release();
        assert_eq!(
            fill.await.expect("fill task should finish"),
            ObjectDataCacheFillResult::SkippedInvalidationRace
        );
        assert_eq!(clear.await.expect("clear task should finish"), ObjectDataCacheInvalidationResult::NoOp);
        assert!(matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
        assert_eq!(backend.index.identity_count().await, 0);
    }

    #[tokio::test]
    async fn clear_fences_fill_registered_before_cache_publish() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let fill_barrier = backend.install_fill_barrier();
        let clear_barrier = backend.install_clear_barrier();
        let plan = cacheable_plan("object", "etag-a");

        let fill_backend = Arc::clone(&backend);
        let fill_plan = plan.clone();
        let fill = tokio::spawn(async move { fill_backend.fill_body(&fill_plan, Bytes::from_static(b"hello")).await });
        fill_barrier.wait_until_reached().await;

        let clear_backend = Arc::clone(&backend);
        let clear = tokio::spawn(async move { clear_backend.clear().await });
        clear_barrier.wait_until_reached().await;
        clear_barrier.release();
        assert!(!clear.is_finished(), "clear must wait for a registered fill to acknowledge the fence");

        fill_barrier.release();
        assert_eq!(
            fill.await.expect("fill task should finish"),
            ObjectDataCacheFillResult::SkippedInvalidationRace
        );
        assert_eq!(
            clear.await.expect("clear task should finish"),
            ObjectDataCacheInvalidationResult::Removed { keys: 1 }
        );
        assert!(matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
        assert_eq!(backend.index.identity_count().await, 0);
    }

    #[tokio::test]
    async fn fill_started_during_clear_skips_without_waiting() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let clear_barrier = backend.install_clear_barrier();
        let clear_backend = Arc::clone(&backend);
        let clear = tokio::spawn(async move { clear_backend.clear().await });
        clear_barrier.wait_until_reached().await;

        assert_eq!(
            backend
                .fill_body(&cacheable_plan("object", "etag-a"), Bytes::from_static(b"hello"))
                .await,
            ObjectDataCacheFillResult::SkippedInvalidationRace
        );

        clear_barrier.release();
        assert_eq!(clear.await.expect("clear task should finish"), ObjectDataCacheInvalidationResult::NoOp);
    }

    #[tokio::test]
    async fn concurrent_clear_joins_same_operation() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let _ = backend
            .fill_body(&cacheable_plan("object", "etag-a"), Bytes::from_static(b"hello"))
            .await;
        let clear_barrier = backend.install_clear_barrier();

        let first_backend = Arc::clone(&backend);
        let first = tokio::spawn(async move { first_backend.clear().await });
        clear_barrier.wait_until_reached().await;
        let second_backend = Arc::clone(&backend);
        let second = tokio::spawn(async move { second_backend.clear().await });
        backend.wait_until_clear_joined().await;
        assert!(!second.is_finished(), "a concurrent clear must join the active drain");

        clear_barrier.release();
        let expected = ObjectDataCacheInvalidationResult::Removed { keys: 1 };
        assert_eq!(first.await.expect("first clear should finish"), expected);
        assert_eq!(second.await.expect("second clear should finish"), expected);
    }

    #[tokio::test]
    async fn cancelled_clear_does_not_reopen_old_generation() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let clear_barrier = backend.install_clear_barrier();
        let first_backend = Arc::clone(&backend);
        let first = tokio::spawn(async move { first_backend.clear().await });
        clear_barrier.wait_until_reached().await;
        first.abort();

        assert_eq!(
            backend
                .fill_body(&cacheable_plan("during-clear", "etag-a"), Bytes::from_static(b"hello"))
                .await,
            ObjectDataCacheFillResult::SkippedInvalidationRace
        );

        let second_backend = Arc::clone(&backend);
        let second = tokio::spawn(async move { second_backend.clear().await });
        backend.wait_until_clear_joined().await;
        second.abort();
        clear_barrier.release();
        assert_eq!(backend.clear().await, ObjectDataCacheInvalidationResult::NoOp);
        assert_eq!(
            backend
                .fill_body(&cacheable_plan("after-clear", "etag-b"), Bytes::from_static(b"hello"))
                .await,
            ObjectDataCacheFillResult::Inserted
        );
    }

    #[tokio::test]
    async fn clear_completion_reopens_generation_atomically() {
        let fence = Arc::new(ClearFence::new());
        let (operation, should_start) = fence.begin_clear();
        assert!(should_start);

        fence.finish_clear(&operation, ObjectDataCacheInvalidationResult::NoOp);

        assert_eq!(operation.wait().await, ObjectDataCacheInvalidationResult::NoOp);
        assert!(fence.try_register_fill().is_some());
    }

    #[tokio::test]
    async fn cancelled_fill_acknowledges_clear_fence() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let fill_barrier = backend.install_fill_barrier();
        let plan = cacheable_plan("object", "etag-a");
        let fill_backend = Arc::clone(&backend);
        let fill_plan = plan.clone();
        let fill = tokio::spawn(async move { fill_backend.fill_body(&fill_plan, Bytes::from_static(b"hello")).await });
        fill_barrier.wait_until_reached().await;
        fill.abort();
        let _ = fill.await;
        assert_eq!(
            backend.clear_fence.state.lock().unwrap().active_fills,
            1,
            "the detached publish task must retain its active-fill acknowledgement"
        );

        let clear_barrier = backend.install_clear_barrier();
        let clear_backend = Arc::clone(&backend);
        let clear = tokio::spawn(async move { clear_backend.clear().await });
        clear_barrier.wait_until_reached().await;
        clear_barrier.release();
        assert!(!clear.is_finished(), "clear must wait for the detached publish task");

        fill_barrier.release();
        assert_eq!(
            clear.await.expect("clear task should finish"),
            ObjectDataCacheInvalidationResult::Removed { keys: 1 }
        );
        assert!(matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Miss));
        assert_eq!(backend.index.identity_count().await, 0);
    }

    #[test]
    fn clear_fence_generation_overflow_is_fail_closed() {
        for generation in [u64::MAX, u64::MAX - 1] {
            let fence = Arc::new(ClearFence::new());
            fence.state.lock().unwrap().generation = Some(generation);

            let (operation, should_start) = fence.begin_clear();
            assert!(should_start);
            fence.finish_clear(&operation, ObjectDataCacheInvalidationResult::NoOp);

            assert!(fence.try_register_fill().is_none());
            assert!(fence.state.lock().unwrap().generation.is_none());
            let (joined, should_start) = fence.begin_clear();
            assert!(!should_start, "a failed generation must retain the permanent clear fence");
            assert!(Arc::ptr_eq(&operation, &joined));
        }
    }

    #[test]
    fn clear_fence_active_counter_failure_is_fail_closed() {
        let overflow = Arc::new(ClearFence::new());
        overflow.state.lock().unwrap().active_fills = usize::MAX;
        assert!(overflow.try_register_fill().is_none());
        assert!(overflow.state.lock().unwrap().generation.is_none());

        let underflow = Arc::new(ClearFence::new());
        drop(FillGenerationGuard {
            fence: Arc::clone(&underflow),
            generation: 0,
        });
        assert!(underflow.state.lock().unwrap().generation.is_none());
        assert!(underflow.try_register_fill().is_none());
    }

    #[tokio::test]
    async fn moka_backend_expires_entries_by_ttl() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let _ = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn moka_backend_eviction_prunes_identity_index() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");

        // Cache several distinct identities under the short TTL from the config.
        for i in 0..8 {
            let plan = cacheable_plan(&format!("object-{i}"), "etag-a");
            let _ = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        }
        assert_eq!(backend.index.identity_count().await, 8);

        // Let every entry expire, then let moka process the expirations so the
        // eviction listener runs and prunes the identity index.
        tokio::time::sleep(Duration::from_millis(150)).await;
        backend.cache.run_pending_tasks().await;

        assert_eq!(backend.cache.entry_count(), 0, "all entries should have expired");
        assert_eq!(
            backend.index.identity_count().await,
            0,
            "identity index must not outlive evicted cache entries"
        );
    }

    #[tokio::test]
    async fn moka_backend_expires_entries_by_tti() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_millis(100);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let _ = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let lookup = backend.lookup_body(&plan).await;

        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test]
    async fn moka_backend_skips_fill_under_memory_pressure() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        // The gate must be enabled here, otherwise allows_fill short-circuits.
        let backend = MokaBackend::new(&memory_gated_config(), Arc::clone(&stats)).expect("moka backend should build");
        backend
            .memory_gate
            .set_test_snapshot(Some(crate::memory::ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 100,
            }));
        let plan = cacheable_plan("object", "etag-a");

        let result = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(result, ObjectDataCacheFillResult::SkippedMemoryPressure);
        assert!(matches!(lookup, ObjectDataCacheLookup::Miss));
        assert_eq!(stats.snapshot().memory_pressure_events, 1);
    }

    #[tokio::test]
    async fn moka_buffered_fill_holds_conservative_claim_until_refresh() {
        let backend = MokaBackend::new(&memory_gated_config(), Arc::new(ObjectDataCacheStats::default()))
            .expect("moka backend should build");
        let snapshot = crate::memory::ObjectDataCacheMemorySnapshot {
            total_bytes: 1_000,
            available_bytes: 500,
        };
        backend.memory_gate.set_test_snapshot(Some(snapshot));
        let plan = cacheable_plan("object", "etag-a");

        assert_eq!(
            backend.fill_body(&plan, Bytes::from_static(b"hello")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(backend.memory_gate.claimed_bytes_for_test(), 5);

        let _ = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "object"))
            .await;
        backend.cache.run_pending_tasks().await;
        assert_eq!(
            backend.memory_gate.claimed_bytes_for_test(),
            5,
            "an already-buffered body cannot be retrofitted with an allocation owner"
        );

        backend.memory_gate.store_raw_snapshot_for_test(snapshot);
        assert_eq!(backend.memory_gate.claimed_bytes_for_test(), 0);
    }

    // ODC-15 + ODC-18: a concurrent duplicate fill for the same key must NOT
    // wait for the in-flight leader (it already owns the body), and it must be
    // reported as JoinedInflightFill — not Inserted — so it records no fill
    // work and cannot inflate the inserted count/bytes for one real insert.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moka_backend_duplicate_fill_same_key_joins_without_waiting() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let plan = cacheable_plan("object", "etag-a");
        let barrier = backend.install_fill_barrier();

        let fill_a = {
            let backend = Arc::clone(&backend);
            let plan = plan.clone();
            tokio::spawn(async move { backend.fill_body(&plan, Bytes::from_static(b"hello")).await })
        };

        // A has registered its singleflight key and parked before the cache
        // insert. A second fill for the same key must join (skip) right away.
        barrier.wait_until_reached().await;
        let fill_b = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        assert_eq!(
            fill_b,
            ObjectDataCacheFillResult::JoinedInflightFill,
            "a duplicate fill must join the in-flight leader (not insert), and not wait for it"
        );

        barrier.release();
        let fill_a = fill_a.await.expect("leader fill task should complete");
        assert_eq!(fill_a, ObjectDataCacheFillResult::Inserted);
        assert!(matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"hello"));
    }

    // ODC-11: the fill-concurrency limiter must reject (not queue) once
    // saturated. With a single permit, an in-flight fill for one key holds the
    // permit; a concurrent fill for a DISTINCT key is rejected immediately.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moka_backend_rejects_fill_when_concurrency_saturated() {
        let mut config = enabled_config();
        // One permit: min(per_cpu * parallelism, max) == min(N, 1) == 1.
        config.fill_concurrency_per_cpu = 1;
        config.fill_concurrency_max = 1;
        let backend =
            Arc::new(MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"));
        let plan_a = versioned_plan("object", "v1", "etag-a");
        let plan_b = versioned_plan("object", "v2", "etag-b");

        let barrier = backend.install_fill_barrier();

        let fill_a = {
            let backend = Arc::clone(&backend);
            let plan_a = plan_a.clone();
            tokio::spawn(async move { backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await })
        };

        // A holds the only permit and is parked before the cache insert. Clear
        // the barrier so B (a distinct key) runs without pausing; B must be
        // rejected by the saturated limiter rather than queue behind A.
        barrier.wait_until_reached().await;
        *backend.fill_barrier.lock().unwrap() = None;
        let fill_b = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;
        assert_eq!(
            fill_b,
            ObjectDataCacheFillResult::SkippedFillConcurrency,
            "a distinct-key fill must be rejected, not queued, when the limiter is saturated"
        );
        assert!(
            matches!(backend.lookup_body(&plan_b).await, ObjectDataCacheLookup::Miss),
            "a rejected fill must not populate the cache"
        );

        barrier.release();
        let fill_a = fill_a.await.expect("leader fill task should complete");
        assert_eq!(fill_a, ObjectDataCacheFillResult::Inserted);
    }

    // ODC-12(1): the first refill after a TTL expiry must not self-destruct.
    // The expired-but-resident entry is upserted, which fires moka's eviction
    // listener inline with cause `Expired`; the generation token keeps that
    // listener from removing the index key the refill just registered.
    #[tokio::test]
    async fn moka_backend_refill_after_expiry_without_maintenance_inserts() {
        let backend =
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan = cacheable_plan("object", "etag-a");

        let first = backend.fill_body(&plan, Bytes::from_static(b"hello")).await;
        assert_eq!(first, ObjectDataCacheFillResult::Inserted);

        // Let the entry expire but do NOT run pending tasks or look it up, so the
        // expired entry is still resident when the refill upserts over it.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let refill = backend.fill_body(&plan, Bytes::from_static(b"world")).await;
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(refill, ObjectDataCacheFillResult::Inserted, "refill after expiry must not self-destruct");
        assert!(matches!(lookup, ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"world"));
    }

    // ODC-20: two concurrent fills for different keys of one identity must both
    // succeed. Fill A registers its key and pauses before publishing to the
    // cache; fill B's prune must keep A's in-flight key so A's recheck holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moka_backend_concurrent_fills_same_identity_both_insert() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let plan_a = versioned_plan("object", "v1", "etag-a");
        let plan_b = versioned_plan("object", "v2", "etag-b");

        let barrier = backend.install_fill_barrier();

        let fill_a = {
            let backend = Arc::clone(&backend);
            let plan_a = plan_a.clone();
            tokio::spawn(async move { backend.fill_body(&plan_a, Bytes::from_static(b"aaaaa")).await })
        };

        // Wait until A has registered its key and paused before the cache insert.
        barrier.wait_until_reached().await;
        // Clear the barrier so B runs to completion without pausing.
        *backend.fill_barrier.lock().unwrap() = None;

        let fill_b = backend.fill_body(&plan_b, Bytes::from_static(b"bbbbb")).await;
        assert_eq!(fill_b, ObjectDataCacheFillResult::Inserted);

        // Release A and let it finish its recheck.
        barrier.release();
        let fill_a = fill_a.await.expect("fill A task should complete");

        assert_eq!(
            fill_a,
            ObjectDataCacheFillResult::Inserted,
            "an in-flight sibling fill must not phantom-race"
        );
        assert!(matches!(backend.lookup_body(&plan_a).await, ObjectDataCacheLookup::Hit(_)));
        assert!(matches!(backend.lookup_body(&plan_b).await, ObjectDataCacheLookup::Hit(_)));
    }

    // ODC-23: exceeding the per-identity key budget evicts only the oldest key
    // and still caches the newest body, instead of clearing the whole identity.
    #[tokio::test]
    async fn moka_backend_identity_budget_evicts_oldest_and_caches_newest() {
        let (backend, plan_v1, plan_v2, plan_v3) = identity_budget_fixture();

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"aaaaa")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"bbbbb")).await,
            ObjectDataCacheFillResult::Inserted
        );
        // The third fill exceeds the budget of 2 and evicts the oldest (v1).
        assert_eq!(
            backend.fill_body(&plan_v3, Bytes::from_static(b"ccccc")).await,
            ObjectDataCacheFillResult::Inserted
        );

        assert!(
            matches!(backend.lookup_body(&plan_v1).await, ObjectDataCacheLookup::Miss),
            "oldest key must be evicted"
        );
        assert!(matches!(backend.lookup_body(&plan_v2).await, ObjectDataCacheLookup::Hit(_)));
        assert!(
            matches!(backend.lookup_body(&plan_v3).await, ObjectDataCacheLookup::Hit(_)),
            "newest body must be cached"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn identity_budget_eviction_preserves_refill_before_conditional_delete() {
        let (backend, plan_v1, plan_v2, plan_v3) = identity_budget_fixture();

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"old-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"body-b")).await,
            ObjectDataCacheFillResult::Inserted
        );

        let barrier = backend.install_identity_eviction_barrier();
        let fill_v3 = {
            let backend = Arc::clone(&backend);
            let plan_v3 = plan_v3.clone();
            tokio::spawn(async move { backend.fill_body(&plan_v3, Bytes::from_static(b"body-c")).await })
        };

        barrier.wait_until_reached().await;
        *backend.identity_eviction_barrier.lock().unwrap_or_else(|p| p.into_inner()) = None;
        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"new-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert!(matches!(
            backend.lookup_body(&plan_v1).await,
            ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"new-a"
        ));

        barrier.release();
        assert_eq!(fill_v3.await.expect("v3 fill task should complete"), ObjectDataCacheFillResult::Inserted);
        assert!(matches!(
            backend.lookup_body(&plan_v1).await,
            ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"new-a"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn identity_budget_conditional_delete_is_atomic_with_refill() {
        let (backend, plan_v1, plan_v2, plan_v3) = identity_budget_fixture();

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"old-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"body-b")).await,
            ObjectDataCacheFillResult::Inserted
        );

        let compute_barrier = backend.install_identity_compute_barrier();
        let fill_v3 = {
            let backend = Arc::clone(&backend);
            let plan_v3 = plan_v3.clone();
            tokio::spawn(async move { backend.fill_body(&plan_v3, Bytes::from_static(b"body-c")).await })
        };
        compute_barrier.wait_until_reached().await;
        *backend.identity_compute_barrier.lock().unwrap_or_else(|p| p.into_inner()) = None;

        let fill_barrier = backend.install_fill_barrier();
        let refill_v1 = {
            let backend = Arc::clone(&backend);
            let plan_v1 = plan_v1.clone();
            tokio::spawn(async move { backend.fill_body(&plan_v1, Bytes::from_static(b"new-a")).await })
        };
        fill_barrier.wait_until_reached().await;
        fill_barrier.release();
        tokio::task::yield_now().await;
        assert!(!refill_v1.is_finished(), "the refill must serialize behind the same-key compute");

        compute_barrier.release();
        assert_eq!(fill_v3.await.expect("v3 fill task should complete"), ObjectDataCacheFillResult::Inserted);
        assert_eq!(
            refill_v1.await.expect("v1 refill task should complete"),
            ObjectDataCacheFillResult::Inserted
        );
        assert!(matches!(
            backend.lookup_body(&plan_v1).await,
            ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"new-a"
        ));
    }

    #[tokio::test]
    async fn identity_budget_eviction_keeps_index_and_cache_generation_consistent() {
        let (backend, plan_v1, plan_v2, plan_v3) = identity_budget_fixture();

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"body-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"body-b")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v3, Bytes::from_static(b"body-c")).await,
            ObjectDataCacheFillResult::Inserted
        );

        let ObjectDataCacheGetPlan::Cacheable { key: key_v1 } = &plan_v1 else {
            panic!("versioned plan should be cacheable");
        };
        let ObjectDataCacheGetPlan::Cacheable { key: key_v2 } = &plan_v2 else {
            panic!("versioned plan should be cacheable");
        };
        let ObjectDataCacheGetPlan::Cacheable { key: key_v3 } = &plan_v3 else {
            panic!("versioned plan should be cacheable");
        };
        let identity = ObjectDataCacheIdentity::new("bucket", "object");
        let entry_v2 = backend.cache.get(key_v2).await.expect("v2 should remain cached");
        let entry_v3 = backend.cache.get(key_v3).await.expect("v3 should remain cached");

        assert!(backend.cache.get(key_v1).await.is_none(), "evicted v1 must not remain cached");
        assert!(!backend.index.contains_key(&identity, key_v1).await, "evicted v1 must not remain indexed");
        assert!(
            backend
                .index
                .contains_generation(&identity, key_v2, entry_v2.generation())
                .await,
            "v2 index generation must match its cache entry"
        );
        assert!(
            backend
                .index
                .contains_generation(&identity, key_v3, entry_v3.generation())
                .await,
            "v3 index generation must match its cache entry"
        );
    }

    #[tokio::test]
    async fn cache_generation_is_monotonic_across_clear_and_overflow_fails_closed() {
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        let backend = MokaBackend::new(&config, Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build");
        let plan_v1 = versioned_plan("object", "v1", "etag-1");
        let plan_v2 = versioned_plan("object", "v2", "etag-2");
        let ObjectDataCacheGetPlan::Cacheable { key: key_v1 } = &plan_v1 else {
            panic!("versioned plan should be cacheable");
        };

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"old-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        let first_generation = backend
            .cache
            .get(key_v1)
            .await
            .expect("first generation should be cached")
            .generation();
        assert_eq!(backend.clear().await, ObjectDataCacheInvalidationResult::Removed { keys: 1 });
        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"new-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        let second_generation = backend
            .cache
            .get(key_v1)
            .await
            .expect("second generation should be cached")
            .generation();
        assert!(second_generation > first_generation, "clear must not reset the generation counter");

        backend.next_generation.store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"body-b")).await,
            ObjectDataCacheFillResult::SkippedIdentityOverflow
        );
        assert!(matches!(backend.lookup_body(&plan_v2).await, ObjectDataCacheLookup::Miss));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn identity_budget_eviction_preserves_refill_after_caller_cancellation() {
        let (backend, plan_v1, plan_v2, plan_v3) = identity_budget_fixture();

        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"old-a")).await,
            ObjectDataCacheFillResult::Inserted
        );
        assert_eq!(
            backend.fill_body(&plan_v2, Bytes::from_static(b"body-b")).await,
            ObjectDataCacheFillResult::Inserted
        );

        let barrier = backend.install_identity_eviction_barrier();
        let fill_v3 = {
            let backend = Arc::clone(&backend);
            let plan_v3 = plan_v3.clone();
            tokio::spawn(async move { backend.fill_body(&plan_v3, Bytes::from_static(b"body-c")).await })
        };
        barrier.wait_until_reached().await;
        fill_v3.abort();
        *backend.identity_eviction_barrier.lock().unwrap_or_else(|p| p.into_inner()) = None;
        assert_eq!(
            backend.fill_body(&plan_v1, Bytes::from_static(b"new-a")).await,
            ObjectDataCacheFillResult::Inserted
        );

        barrier.release();
        barrier.wait_until_completed().await;
        assert!(matches!(
            backend.lookup_body(&plan_v1).await,
            ObjectDataCacheLookup::Hit(ref bytes) if bytes.as_ref() == b"new-a"
        ));
    }

    // ODC-31: the fill-vs-invalidation recheck. Pause the fill between the index
    // insert and the cache insert, invalidate the identity, then resume; the fill
    // must undo itself and report the race, leaving nothing cached.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moka_backend_fill_loses_race_to_invalidation() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let plan = cacheable_plan("object", "etag-a");
        let barrier = backend.install_fill_barrier();

        let fill = {
            let backend = Arc::clone(&backend);
            let plan = plan.clone();
            tokio::spawn(async move { backend.fill_body(&plan, Bytes::from_static(b"hello")).await })
        };

        barrier.wait_until_reached().await;
        // Invalidate while the fill is parked after registering the index key.
        let _ = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "object"))
            .await;
        barrier.release();

        let result = fill.await.expect("fill task should complete");
        let lookup = backend.lookup_body(&plan).await;

        assert_eq!(result, ObjectDataCacheFillResult::SkippedInvalidationRace);
        assert!(
            matches!(lookup, ObjectDataCacheLookup::Miss),
            "a body must not survive a racing invalidation"
        );
    }

    // ODC-13: cancelling the enclosing GET future at the recheck point must still
    // undo a fill that lost the race, so no orphaned body survives with no index
    // entry. The spawned fill task completes the recheck/undo after cancellation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moka_backend_cancelled_fill_still_undoes_lost_race() {
        let backend = Arc::new(
            MokaBackend::new(&enabled_config(), Arc::new(ObjectDataCacheStats::default())).expect("moka backend should build"),
        );
        let plan = cacheable_plan("object", "etag-a");
        let barrier = backend.install_fill_barrier();

        let fill = {
            let backend = Arc::clone(&backend);
            let plan = plan.clone();
            tokio::spawn(async move { backend.fill_body(&plan, Bytes::from_static(b"hello")).await })
        };

        barrier.wait_until_reached().await;
        // A concurrent invalidation removes the index key the fill registered.
        let _ = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "object"))
            .await;
        // Cancel the enclosing fill future before it reaches the recheck.
        fill.abort();
        // Resume the detached fill task so it runs the recheck and undo.
        barrier.release();

        // The detached task finishes the undo; poll until nothing is cached.
        let mut cached = true;
        for _ in 0..200 {
            if matches!(backend.lookup_body(&plan).await, ObjectDataCacheLookup::Miss) {
                cached = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!cached, "a cancelled fill must not leave an orphaned body after a racing invalidation");
    }

    // ---- Concurrency stress (backlog#1107 production-readiness follow-up) ----
    //
    // The deterministic race tests above pin specific interleavings with an
    // injected barrier. These drive sustained, real-parallelism contention on a
    // small key space to catch leaks and state corruption the pinned tests
    // cannot: a leaked singleflight leader, a stranded semaphore permit, an
    // index/cache divergence that only shows up under volume.

    /// Hammer fill / lookup / invalidate concurrently on a few hot identities,
    /// then assert the shared machinery is neither leaked nor corrupted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn moka_backend_concurrency_storm_leaves_no_leaked_state() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        // Long TTL so entries survive the storm — we want fill-vs-invalidate
        // races, not just expiry churn.
        let mut config = enabled_config();
        config.ttl = Duration::from_secs(30);
        config.time_to_idle = Duration::from_secs(30);
        let backend = Arc::new(MokaBackend::new(&config, Arc::clone(&stats)).expect("moka backend should build"));

        const TASKS: usize = 48;
        const ITERS: usize = 400;
        const KEYS: usize = 6; // small pool → heavy contention on shared identities

        let stress_plans: Arc<Vec<_>> = Arc::new(
            (0..KEYS)
                .map(|k| versioned_plan(&format!("object-{}", k % 3), &format!("v{k}"), &format!("etag-{k}")))
                .collect(),
        );
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let backend = Arc::clone(&backend);
            let stress_plans = Arc::clone(&stress_plans);
            handles.push(tokio::spawn(async move {
                for i in 0..ITERS {
                    let k = (t + i) % KEYS;
                    let object = format!("object-{}", k % 3);
                    let plan = &stress_plans[k];
                    // Deterministic per-(task,iter) mix: mostly fills, plus
                    // lookups and invalidations racing against them.
                    match (t + i) % 4 {
                        0 | 1 => {
                            let _ = backend.fill_body(plan, Bytes::from_static(b"hello")).await;
                        }
                        2 => {
                            let _ = backend.lookup_body(plan).await;
                        }
                        _ => {
                            let identity = ObjectDataCacheIdentity::new("bucket", object.as_str());
                            let _ = backend.invalidate_object(&identity).await;
                        }
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.expect("stress task must not panic or deadlock");
        }

        // Every fill_body call returned, so no singleflight leader may still be
        // registered: a non-zero count means a fill leaked its in-flight slot.
        assert_eq!(
            stats.snapshot().inflight_fills,
            0,
            "the concurrency storm leaked an in-flight fill (singleflight/semaphore not released)"
        );

        // The shared state is not corrupted: a fresh, uncontended sequence must
        // still fill, hit, and invalidate correctly.
        let clean = versioned_plan("fresh-object", "v1", "fresh-etag");
        assert_eq!(
            backend.fill_body(&clean, Bytes::from_static(b"hello")).await,
            ObjectDataCacheFillResult::Inserted,
            "a clean fill after the storm must still insert"
        );
        assert!(matches!(backend.lookup_body(&clean).await, ObjectDataCacheLookup::Hit(_)));
        let _ = backend
            .invalidate_object(&ObjectDataCacheIdentity::new("bucket", "fresh-object"))
            .await;
        assert!(
            matches!(backend.lookup_body(&clean).await, ObjectDataCacheLookup::Miss),
            "invalidation must still win after a concurrency storm"
        );

        let _ = backend.clear().await;
        for plan in stress_plans.iter().chain(std::iter::once(&clean)) {
            assert!(
                matches!(backend.lookup_body(plan).await, ObjectDataCacheLookup::Miss),
                "clear() must make every exercised key unobservable"
            );
        }
        assert!(
            backend.cache.iter().next().is_none(),
            "clear() must expose no entries through iteration (approximate entry count: {})",
            backend.entry_count()
        );
    }

    /// A memory-constrained container (low snapshot) must reject an entire
    /// concurrent fill burst — admission is bounded, not a check-then-act race
    /// that lets the burst through. This is the CI-runnable proxy for the
    /// OOM-under-burst concern; it does not cover the separate 5s-stale-snapshot
    /// overshoot, which needs byte-based reservation in the gate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn moka_backend_gate_bounds_admission_under_concurrent_burst() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let backend = Arc::new(MokaBackend::new(&memory_gated_config(), Arc::clone(&stats)).expect("moka backend should build"));
        // Simulate a pod whose available memory is far below the free floor.
        backend
            .memory_gate
            .set_test_snapshot(Some(crate::memory::ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000,
                available_bytes: 50,
            }));

        const TASKS: usize = 32;
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let backend = Arc::clone(&backend);
            handles.push(tokio::spawn(async move {
                let plan = versioned_plan(&format!("object-{t}"), "v1", &format!("etag-{t}"));
                backend.fill_body(&plan, Bytes::from_static(b"hello")).await
            }));
        }
        let mut rejected = 0;
        for handle in handles {
            if handle.await.expect("burst task should complete") == ObjectDataCacheFillResult::SkippedMemoryPressure {
                rejected += 1;
            }
        }

        assert_eq!(rejected, TASKS, "the gate must reject every fill in the burst when memory is constrained");
        assert_eq!(backend.entry_count(), 0, "no body may be admitted under memory pressure");
    }

    /// A burst that begins while the snapshot still reads *high* must be bounded
    /// to the real headroom, not admitted wholesale. Each fill is large enough
    /// that only a handful fit the budget; without the admitted-bytes reservation
    /// every fill would read the same stale-high snapshot and be admitted,
    /// over-allocating far past the budget before the 5 s refresh (backlog#1107).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn moka_backend_gate_reservation_bounds_burst_under_stale_snapshot() {
        let stats = Arc::new(ObjectDataCacheStats::default());
        let backend = Arc::new(MokaBackend::new(&memory_gated_config(), Arc::clone(&stats)).expect("moka backend should build"));
        // Stale-but-high snapshot: 500 KiB available, 20% floor = 200 KiB, so the
        // real budget above the floor is ~300 KiB.
        backend
            .memory_gate
            .set_test_snapshot(Some(crate::memory::ObjectDataCacheMemorySnapshot {
                total_bytes: 1_000_000,
                available_bytes: 500_000,
            }));

        // 20 concurrent fills of 40 KiB each = 800 KiB requested, far past the
        // ~300 KiB budget. Only ~7 should fit.
        const TASKS: usize = 20;
        const BODY: usize = 40_000;
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let backend = Arc::clone(&backend);
            handles.push(tokio::spawn(async move {
                let plan = versioned_plan(&format!("object-{t}"), "v1", &format!("etag-{t}"));
                backend.fill_body(&plan, Bytes::from(vec![0u8; BODY])).await
            }));
        }
        let mut admitted = 0usize;
        for handle in handles {
            if handle.await.expect("burst task should complete") == ObjectDataCacheFillResult::Inserted {
                admitted += 1;
            }
        }

        // The reservation must cap the burst: not every fill gets in (that is the
        // stale-snapshot overshoot), yet some do (the gate is not just rejecting
        // everything). The exact count varies with the check-then-reserve race,
        // but the admitted bytes must stay within a small multiple of the budget.
        assert!(
            admitted < TASKS,
            "reservation must bound the burst — not admit the whole {TASKS}-fill storm"
        );
        assert!(admitted >= 1, "the gate must still admit fills that fit the budget");
        assert!(
            admitted * BODY <= 500_000,
            "admitted bytes ({}) must not exceed the snapshot's available memory",
            admitted * BODY
        );
    }
}
