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

use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;

use crate::fast_lock::{
    guard::{GuardToken, record_lock_held_acquire, record_lock_held_release},
    metrics::ShardMetrics,
    object_pool::ObjectStatePool,
    state::{ObjectLockState, ReleaseOutcome},
    types::{LockMode, LockResult, ObjectKey, ObjectLockRequest},
};

#[derive(Debug, Default)]
struct GuardRegistration {
    tokens: SmallVec<[Arc<GuardToken>; 1]>,
    weak: Option<Arc<()>>,
    ambiguous: bool,
}

#[derive(Debug)]
struct Holder {
    owner: Arc<str>,
    mode: LockMode,
    token: Option<Arc<GuardToken>>,
}

impl Holder {
    fn is_raw(&self) -> bool {
        self.token.as_ref().is_none_or(|token| !token.tracked)
    }
}

#[derive(Debug)]
struct ShardObjectState {
    state: ObjectLockState,
    holders: parking_lot::Mutex<SmallVec<[Holder; 1]>>,
}

impl ShardObjectState {
    fn new(state: ObjectLockState) -> Self {
        Self {
            state,
            holders: parking_lot::Mutex::new(SmallVec::new()),
        }
    }
}

impl Deref for ShardObjectState {
    type Target = ObjectLockState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TestPausePoint {
    FastExisting,
    BeforeFastWrite,
    FastWriteExisting,
    SlowLookup,
    TrackedBeforeRegistration,
    GuardIdAfterLookup,
    ForceGateContended,
}

#[cfg(test)]
struct TestPause {
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static TEST_PAUSES: std::sync::LazyLock<parking_lot::Mutex<HashMap<(TestPausePoint, ObjectKey), TestPause>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

#[cfg(test)]
fn test_pause(point: TestPausePoint, key: &ObjectKey) {
    let pause = TEST_PAUSES.lock().remove(&(point, key.clone()));
    if let Some(pause) = pause {
        pause.entered.send(()).expect("pause receiver");
        pause.resume.recv_timeout(Duration::from_secs(5)).expect("resume signal");
    }
}

/// Lock shard to reduce global contention.
///
/// Lock order is `objects`, then the holder gate, then owner metadata or the
/// guard registry. Notifications run after these locks have been released.
#[derive(Debug)]
pub struct LockShard {
    /// Object lock states - using parking_lot for better performance
    objects: RwLock<HashMap<ObjectKey, Arc<ShardObjectState>>>,
    /// Object state pool for memory optimization
    object_pool: ObjectStatePool,
    /// Shard-level metrics
    metrics: ShardMetrics,
    /// Shard ID for debugging
    _shard_id: usize,
    /// Active guard IDs to prevent cleanup of locks with live guards
    active_guards: parking_lot::Mutex<HashMap<u64, GuardRegistration>>,
}

/// Cancellation-safe waiter counter ticket.
///
/// Ensures waiting counters are decremented even if the waiting future
/// is cancelled/dropped before the normal post-await path runs.
struct WaiterCounterGuard {
    state: Arc<ShardObjectState>,
    mode: LockMode,
    incremented: bool,
}

impl WaiterCounterGuard {
    fn new(state: Arc<ShardObjectState>, mode: LockMode) -> Self {
        let incremented = match mode {
            LockMode::Shared => state.atomic_state.inc_readers_waiting(),
            LockMode::Exclusive => state.atomic_state.inc_writers_waiting(),
        };
        Self {
            state,
            mode,
            incremented,
        }
    }
}

impl Drop for WaiterCounterGuard {
    fn drop(&mut self) {
        if !self.incremented {
            return;
        }
        match self.mode {
            LockMode::Shared => self.state.atomic_state.dec_readers_waiting(),
            LockMode::Exclusive => self.state.atomic_state.dec_writers_waiting(),
        }
    }
}

/// Keeps an all-or-nothing raw batch tied to the exact holders it acquired.
struct BatchAcquisition<'a> {
    shard: &'a LockShard,
    acquired: Vec<(ObjectKey, LockMode, Arc<str>, Arc<GuardToken>)>,
    rollback: bool,
}

impl Drop for BatchAcquisition<'_> {
    fn drop(&mut self) {
        if !self.rollback {
            return;
        }

        let mut cleanup_failures = 0;
        for (key, mode, owner, token) in &self.acquired {
            if token.acquired.load(Ordering::Acquire)
                && !self.shard.release_tracked(key, owner, *mode, token)
                && token.acquired.load(Ordering::Acquire)
            {
                cleanup_failures += 1;
                tracing::warn!(
                    "Failed to release lock during batch cleanup in shard: bucket={}, object={}",
                    key.bucket,
                    key.object
                );
            }
        }
        if cleanup_failures > 0 {
            tracing::error!("Shard batch lock cleanup had {} failures", cleanup_failures);
        }
    }
}

impl LockShard {
    pub fn new(shard_id: usize) -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            object_pool: ObjectStatePool::new(),
            metrics: ShardMetrics::new(),
            _shard_id: shard_id,
            active_guards: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Acquire lock with fast path optimization
    pub async fn acquire_lock(&self, request: &ObjectLockRequest) -> Result<(), LockResult> {
        self.acquire_lock_inner(request, None).await
    }

    pub(crate) async fn acquire_tracked(&self, request: &ObjectLockRequest, token: &Arc<GuardToken>) -> Result<(), LockResult> {
        self.acquire_lock_inner(request, Some(token)).await
    }

    async fn acquire_lock_inner(&self, request: &ObjectLockRequest, token: Option<&Arc<GuardToken>>) -> Result<(), LockResult> {
        let start_time = Instant::now();

        // Try fast path first
        if let Some(_state) = self.try_fast_path(request, token) {
            self.metrics.record_fast_path_success();
            return Ok(());
        }

        // Slow path with waiting
        self.acquire_lock_slow_path(request, start_time, token).await
    }

    /// Try fast path only (without fallback to slow path)
    pub fn try_fast_path_only(&self, request: &ObjectLockRequest) -> bool {
        self.try_fast_path_only_inner(request, None)
    }

    pub(crate) fn try_fast_path_tracked_only(&self, request: &ObjectLockRequest, token: &Arc<GuardToken>) -> bool {
        self.try_fast_path_only_inner(request, Some(token))
    }

    fn try_fast_path_only_inner(&self, request: &ObjectLockRequest, token: Option<&Arc<GuardToken>>) -> bool {
        // Early check to avoid unnecessary lock contention
        if let Some(state) = self.objects.read().get(&request.key)
            && !state.atomic_state.is_fast_path_available(request.mode)
        {
            return false;
        }
        self.try_fast_path(request, token).is_some()
    }

    /// Try fast path lock acquisition (lock-free when possible)
    fn try_fast_path(&self, request: &ObjectLockRequest, token: Option<&Arc<GuardToken>>) -> Option<Arc<ShardObjectState>> {
        // First try to get existing state without write lock
        {
            let objects = self.objects.read();
            if let Some(state) = objects.get(&request.key) {
                let state = state.clone();

                #[cfg(test)]
                test_pause(TestPausePoint::FastExisting, &request.key);

                // Try atomic acquisition
                let success = self.try_acquire_holder(&state, request, token);
                drop(objects);

                if success {
                    return Some(state);
                }
            }
        }

        // If object doesn't exist and we're requesting exclusive lock,
        // try to create and acquire atomically
        if request.mode == LockMode::Exclusive {
            #[cfg(test)]
            test_pause(TestPausePoint::BeforeFastWrite, &request.key);
            let mut objects = self.objects.write();

            // Double-check after acquiring write lock
            if let Some(state) = objects.get(&request.key) {
                let state = state.clone();
                let objects = parking_lot::RwLockWriteGuard::downgrade(objects);

                #[cfg(test)]
                test_pause(TestPausePoint::FastWriteExisting, &request.key);

                let acquired = self.try_acquire_holder(&state, request, token);
                drop(objects);
                if acquired {
                    return Some(state);
                }
            } else {
                // Create new state from pool and acquire immediately
                let state_box = self.object_pool.acquire();
                let state = Arc::new(ShardObjectState::new(*state_box));
                objects.insert(request.key.clone(), state.clone());
                if self.try_acquire_holder(&state, request, token) {
                    return Some(state);
                }
            }
        }

        None
    }

    fn try_acquire_holder(&self, state: &ShardObjectState, request: &ObjectLockRequest, token: Option<&Arc<GuardToken>>) -> bool {
        let mut holders = state.holders.lock();
        holders.reserve(1);
        let holder = Holder {
            owner: request.owner.clone(),
            mode: request.mode,
            token: token.cloned(),
        };
        let success = match request.mode {
            LockMode::Shared => state.try_acquire_shared_fast(&request.owner, request.lock_timeout),
            LockMode::Exclusive => state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout),
        };
        if success {
            holders.push(holder);
            if let Some(token) = token {
                token.acquired.store(true, Ordering::Release);
                if token.tracked {
                    record_lock_held_acquire(request.mode);
                    #[cfg(test)]
                    test_pause(TestPausePoint::TrackedBeforeRegistration, &request.key);
                    self.register_guard_with_info(token);
                }
            }
        }
        success
    }

    /// Slow path with async waiting
    async fn acquire_lock_slow_path(
        &self,
        request: &ObjectLockRequest,
        start_time: Instant,
        token: Option<&Arc<GuardToken>>,
    ) -> Result<(), LockResult> {
        // Use adaptive timeout based on current load and request priority
        let adaptive_timeout = self.calculate_adaptive_timeout(request);
        let deadline = start_time + adaptive_timeout;

        let mut retry_count = 0u32;
        const MAX_RETRIES: u32 = 10;
        // Upper bound for a single notification wait. The notification pool is
        // shared process-wide (a fixed set of `Notify` slots hashed by lock), so a
        // wakeup meant for this lock can be consumed by a waiter of a different
        // lock that hashes to the same slot, and this lock's waiter would then
        // sleep until the full deadline. Capping each wait turns that lost-wakeup
        // into bounded re-polling: on cap elapse we loop and re-`try_acquire`,
        // only returning `Timeout` once the real deadline passes. The notification
        // still delivers prompt wakeups in the common (no-collision) case.
        const NOTIFY_WAIT_CAP: Duration = Duration::from_millis(50);

        loop {
            // Keep the state in the map until its acquisition attempt completes.
            let (state, success) = {
                let mut objects = self.objects.write();
                let state = match objects.get(&request.key) {
                    Some(state) => state.clone(),
                    None => {
                        let state_box = self.object_pool.acquire();
                        let state = Arc::new(ShardObjectState::new(*state_box));
                        objects.insert(request.key.clone(), state.clone());
                        state
                    }
                };
                let objects = parking_lot::RwLockWriteGuard::downgrade(objects);

                #[cfg(test)]
                test_pause(TestPausePoint::SlowLookup, &request.key);
                let success = self.try_acquire_holder(&state, request, token);
                drop(objects);
                (state, success)
            };

            if success {
                self.metrics.record_slow_path_success();
                return Ok(());
            }

            // Check timeout
            if Instant::now() >= deadline {
                self.metrics.record_timeout();
                return Err(LockResult::Timeout);
            }

            // Use intelligent wait strategy: mix of notification wait and exponential backoff
            let remaining = deadline - Instant::now();

            if retry_count < MAX_RETRIES && remaining > Duration::from_millis(10) {
                // For early retries, wait for a release notification bounded by
                // an exponential backoff. The bound (not a bare sleep) matters:
                // a plain `sleep` subscribes to nothing, so a release during
                // the backoff wakes nobody — `notify_writer`/`notify_readers`
                // are gated on the waiter counters, which a sleeper never
                // increments. Under N-writer same-key contention the lock sits
                // free while every loser sleeps out its full backoff, and the
                // ladder compounds superlinearly with N. The backoff cap still
                // protects against lost/stolen wakeups, exactly as
                // NOTIFY_WAIT_CAP does for the post-retry wait below.
                let backoff_ms = std::cmp::min(10 << retry_count, 100); // 10ms, 20ms, 40ms, 80ms, 100ms max
                let backoff_duration = Duration::from_millis(backoff_ms);

                if backoff_duration < remaining {
                    match request.mode {
                        LockMode::Shared => {
                            let _waiter_guard = WaiterCounterGuard::new(state.clone(), LockMode::Shared);
                            let _ = timeout(backoff_duration, state.optimized_notify.wait_for_read()).await;
                        }
                        LockMode::Exclusive => {
                            let _waiter_guard = WaiterCounterGuard::new(state.clone(), LockMode::Exclusive);
                            let _ = timeout(backoff_duration, state.optimized_notify.wait_for_write()).await;
                        }
                    }
                    retry_count += 1;
                    continue;
                }
            }

            // If we've exhausted quick retries or have little time left, use a
            // notification wait, but bounded by NOTIFY_WAIT_CAP so a lost/stolen
            // wakeup cannot strand this waiter until the deadline.
            let wait = remaining.min(NOTIFY_WAIT_CAP);
            let wait_result = match request.mode {
                LockMode::Shared => {
                    let _waiter_guard = WaiterCounterGuard::new(state.clone(), LockMode::Shared);
                    timeout(wait, state.optimized_notify.wait_for_read()).await
                }
                LockMode::Exclusive => {
                    let _waiter_guard = WaiterCounterGuard::new(state.clone(), LockMode::Exclusive);
                    timeout(wait, state.optimized_notify.wait_for_write()).await
                }
            };

            // A capped-wait elapse is not a real timeout: loop back and re-try the
            // acquisition. The deadline check at the top of the loop is the single
            // source of truth for returning `Timeout`.
            let _ = wait_result;
            retry_count += 1;
        }
    }

    /// Release an owner-matched lock.
    ///
    /// This legacy API has no holder identity and cannot distinguish an old
    /// caller from a new acquisition by the same owner.
    pub fn release_lock(&self, key: &ObjectKey, owner: &Arc<str>, mode: LockMode) -> bool {
        self.release_matching(key, owner, mode, None, None)
    }

    pub(crate) fn release_tracked(&self, key: &ObjectKey, owner: &Arc<str>, mode: LockMode, token: &Arc<GuardToken>) -> bool {
        self.release_matching(key, owner, mode, Some(token), None)
    }

    fn release_matching(
        &self,
        key: &ObjectKey,
        owner: &Arc<str>,
        mode: LockMode,
        token: Option<&Arc<GuardToken>>,
        weak_guard: Option<(u64, &Arc<()>)>,
    ) -> bool {
        let (state, outcome, should_cleanup) = {
            let objects = self.objects.read();
            let Some(state) = objects.get(key).cloned() else {
                if let Some((id, weak)) = weak_guard {
                    let mut guards = self.active_guards.lock();
                    if guards.get(&id).is_some_and(|entry| {
                        entry.tokens.is_empty() && entry.weak.as_ref().is_some_and(|entry| Arc::ptr_eq(entry, weak))
                    }) {
                        guards.remove(&id);
                    }
                }
                return false;
            };
            let mut holders = state.holders.lock();
            let mut weak_registry = if let Some((id, weak)) = weak_guard {
                let guards = self.active_guards.lock();
                if !guards.get(&id).is_some_and(|entry| {
                    entry.tokens.is_empty()
                        && !entry.ambiguous
                        && entry.weak.as_ref().is_some_and(|entry| Arc::ptr_eq(entry, weak))
                }) {
                    return false;
                }
                Some(guards)
            } else {
                None
            };
            let matches = |holder: &&Holder| holder.owner == *owner && holder.mode == mode;
            let position = if let Some(token) = token {
                holders
                    .iter()
                    .position(|holder| matches(&holder) && holder.token.as_ref().is_some_and(|held| Arc::ptr_eq(held, token)))
            } else if weak_guard.is_some() {
                // A legacy registration has no holder identity. It may release
                // a raw holder, but must not impersonate a tracked guard.
                holders.iter().position(|holder| matches(&holder) && holder.is_raw())
            } else {
                holders
                    .iter()
                    .position(|holder| matches(&holder) && holder.is_raw())
                    .or_else(|| holders.iter().position(|holder| matches(&holder)))
            };
            let Some(position) = position else {
                return false;
            };
            let outcome = Self::release_physical(&state, owner, mode);
            let holder = outcome.success.then(|| holders.remove(position));
            if let (true, Some((id, _)), Some(guards)) = (outcome.success, weak_guard, weak_registry.as_mut()) {
                guards.remove(&id);
            }
            drop(weak_registry);
            if let Some(holder) = holder {
                self.record_holder_release(&holder);
                self.metrics.record_release();
            }
            let should_cleanup = outcome.success && !state.is_locked() && !state.atomic_state.has_waiters();
            drop(holders);
            (state, outcome, should_cleanup)
        };
        let result = outcome.finish(&state, owner);
        if should_cleanup {
            self.schedule_cleanup(key.clone());
        }
        result
    }

    fn release_physical(state: &ShardObjectState, owner: &Arc<str>, mode: LockMode) -> ReleaseOutcome {
        match mode {
            LockMode::Shared => state.release_shared_deferred(owner),
            LockMode::Exclusive => state.release_exclusive_deferred(owner),
        }
    }

    fn record_holder_release(&self, holder: &Holder) {
        if let Some(token) = &holder.token {
            token.acquired.store(false, Ordering::Release);
            if token.tracked {
                record_lock_held_release(holder.mode);
                self.unregister_guard_token(token);
            }
        }
    }

    /// Release lock with guard ID tracking for double-release prevention.
    ///
    /// Weak registrations contain no holder identity. A call beginning after
    /// an ID is reused cannot distinguish the previous registration.
    pub fn release_lock_with_guard(&self, key: &ObjectKey, owner: &Arc<str>, mode: LockMode, guard_id: u64) -> bool {
        let registration = {
            let guards = self.active_guards.lock();
            match guards.get(&guard_id) {
                Some(entry) if entry.weak.is_none() && !entry.ambiguous && entry.tokens.len() == 1 => {
                    Some(Ok(entry.tokens[0].clone()))
                }
                Some(entry) if !entry.ambiguous && entry.tokens.is_empty() => entry.weak.clone().map(Err),
                _ => None,
            }
        };

        let Some(registration) = registration else {
            tracing::debug!(
                "Double-release attempt blocked: key={}, owner={}, mode={:?}, guard_id={}",
                key,
                owner,
                mode,
                guard_id
            );
            return false;
        };

        #[cfg(test)]
        test_pause(TestPausePoint::GuardIdAfterLookup, key);
        match registration {
            Ok(token) => self.release_tracked(key, owner, mode, &token),
            Err(weak) => self.release_matching(key, owner, mode, None, Some((guard_id, &weak))),
        }
    }

    /// Register a guard to prevent premature cleanup
    pub fn register_guard(&self, guard_id: u64) {
        let mut guards = self.active_guards.lock();
        let entry = guards.entry(guard_id).or_default();
        entry.ambiguous = !entry.tokens.is_empty();
        entry.weak = Some(Arc::new(()));
    }

    fn register_guard_with_info(&self, token: &Arc<GuardToken>) {
        if token.id == 0 {
            return;
        }
        let mut guards = self.active_guards.lock();
        let entry = guards.entry(token.id).or_default();
        entry.ambiguous |= entry.weak.is_some() || entry.tokens.iter().any(|held| !Arc::ptr_eq(held, token));
        if !entry.tokens.iter().any(|held| Arc::ptr_eq(held, token)) {
            entry.tokens.push(token.clone());
        }
    }

    pub(crate) fn unregister_guard_token(&self, token: &Arc<GuardToken>) {
        if token.id == 0 {
            return;
        }
        let mut guards = self.active_guards.lock();
        let Some(entry) = guards.get_mut(&token.id) else {
            return;
        };
        entry.tokens.retain(|held| !Arc::ptr_eq(held, token));
        if entry.tokens.is_empty() && entry.weak.is_none() {
            guards.remove(&token.id);
        }
    }

    /// Unregister a guard (called when guard is dropped)
    pub fn unregister_guard(&self, guard_id: u64) {
        let mut guards = self.active_guards.lock();
        guards.remove(&guard_id);
    }

    /// Get count of active guards (for testing)
    #[cfg(test)]
    pub fn active_guard_count(&self) -> usize {
        let guards = self.active_guards.lock();
        guards.len()
    }

    /// Check if a guard is active (for testing)
    #[cfg(test)]
    pub fn is_guard_active(&self, guard_id: u64) -> bool {
        let guards = self.active_guards.lock();
        guards.contains_key(&guard_id)
    }

    /// Calculate adaptive timeout based on current system load and request priority
    fn calculate_adaptive_timeout(&self, request: &ObjectLockRequest) -> Duration {
        let base_timeout = request.acquire_timeout;

        // Get current shard load metrics
        let lock_count = {
            let objects = self.objects.read();
            objects.len()
        };

        let active_guard_count = {
            let guards = self.active_guards.lock();
            guards.len()
        };

        // Calculate load factor with more generous thresholds for database workloads
        let total_load = (lock_count + active_guard_count) as f64;
        let load_factor = total_load / 500.0; // Lowered threshold for faster scaling

        // More aggressive priority multipliers for database scenarios
        let priority_multiplier = match request.priority {
            crate::fast_lock::types::LockPriority::Critical => 3.0, // Increased
            crate::fast_lock::types::LockPriority::High => 2.0,     // Increased
            crate::fast_lock::types::LockPriority::Normal => 1.2,   // Slightly increased base
            crate::fast_lock::types::LockPriority::Low => 0.9,
        };

        // More generous load-based scaling
        let load_multiplier = if load_factor > 2.0 {
            // Very high load: drastically extend timeout
            1.0 + (load_factor * 2.0)
        } else if load_factor > 1.0 {
            // High load: significantly extend timeout
            1.0 + (load_factor * 1.8)
        } else if load_factor > 0.3 {
            // Medium load: moderately extend timeout
            1.0 + (load_factor * 1.2)
        } else {
            // Low load: still give some buffer
            1.1
        };

        let total_multiplier = priority_multiplier * load_multiplier;
        let adaptive_timeout_secs =
            (base_timeout.as_secs_f64() * total_multiplier).min(crate::fast_lock::MAX_ACQUIRE_TIMEOUT.as_secs_f64());

        // Keep the caller-provided and global acquire timeouts as hard deadlines.
        let min_timeout_secs = base_timeout.as_secs_f64() * 0.8;
        let adaptive_timeout = Duration::from_secs_f64(adaptive_timeout_secs.max(min_timeout_secs));
        adaptive_timeout.min(base_timeout.min(crate::fast_lock::MAX_ACQUIRE_TIMEOUT))
    }

    /// Batch acquire locks with ordering to prevent deadlocks
    pub async fn acquire_locks_batch(
        &self,
        mut requests: Vec<ObjectLockRequest>,
        all_or_nothing: bool,
    ) -> Result<Vec<ObjectKey>, Vec<(ObjectKey, LockResult)>> {
        // Sort requests by key to prevent deadlocks
        requests.sort_by(|a, b| a.key.cmp(&b.key));

        let mut batch = BatchAcquisition {
            shard: self,
            acquired: Vec::new(),
            rollback: all_or_nothing,
        };
        let mut failed = Vec::new();

        for request in requests {
            let token = GuardToken::raw_batch();
            match self.acquire_lock_inner(&request, Some(&token)).await {
                Ok(()) => batch
                    .acquired
                    .push((request.key.clone(), request.mode, request.owner.clone(), token)),
                Err(err) => {
                    failed.push((request.key, err));

                    if all_or_nothing {
                        return Err(failed);
                    }
                }
            }
        }

        batch.rollback = false;
        if failed.is_empty() {
            Ok(batch.acquired.iter().map(|(key, _, _, _)| key.clone()).collect())
        } else {
            Err(failed)
        }
    }

    /// Get lock information for monitoring
    pub fn get_lock_info(&self, key: &ObjectKey) -> Option<crate::fast_lock::types::ObjectLockInfo> {
        let objects = self.objects.read();
        if let Some(state) = objects.get(key)
            && let Some(mode) = state.current_mode()
        {
            let (owner, acquired_at, lock_timeout) = match mode {
                LockMode::Exclusive => {
                    let current_owner = state.current_owner.read();
                    let info = current_owner.clone()?;
                    (info.owner, info.acquired_at, info.lock_timeout)
                }
                LockMode::Shared => {
                    let shared_owners = state.shared_owners.read();
                    let entry = shared_owners.first()?.clone();
                    (entry.owner, entry.acquired_at, entry.lock_timeout)
                }
            };

            let priority = *state.priority.read();

            let expires_at = acquired_at
                .checked_add(lock_timeout)
                .unwrap_or_else(|| acquired_at + crate::fast_lock::DEFAULT_LOCK_TIMEOUT);

            return Some(crate::fast_lock::types::ObjectLockInfo {
                key: key.clone(),
                mode,
                owner,
                acquired_at,
                expires_at,
                priority,
            });
        }
        None
    }

    /// Enumerate every currently held lock in this shard.
    ///
    /// Exclusive locks yield a single entry; shared locks yield one entry per
    /// distinct owner so administrative "top locks" views can attribute every
    /// holder. Entries for objects that are tracked but not currently locked
    /// (e.g. pooled-but-idle state) are skipped.
    pub fn list_locks(&self) -> Vec<crate::fast_lock::types::ObjectLockInfo> {
        self.list_locks_with_holder_counts()
            .into_iter()
            .map(|(info, _)| info)
            .collect()
    }

    pub(crate) fn list_locks_with_holder_counts(&self) -> Vec<(crate::fast_lock::types::ObjectLockInfo, u32)> {
        self.list_lock_snapshot(false)
            .into_iter()
            .map(|(info, count, _)| (info, count))
            .collect()
    }

    pub(crate) fn list_locks_with_holder_generations(
        &self,
    ) -> Vec<(crate::fast_lock::types::ObjectLockInfo, u32, Option<Vec<u64>>)> {
        self.list_lock_snapshot(true)
    }

    fn list_lock_snapshot(
        &self,
        include_generations: bool,
    ) -> Vec<(crate::fast_lock::types::ObjectLockInfo, u32, Option<Vec<u64>>)> {
        let objects = self.objects.read();
        let mut infos = Vec::new();
        for (key, state) in objects.iter() {
            let holders = state.holders.lock();
            let Some(mode) = state.current_mode() else {
                continue;
            };
            let priority = *state.priority.read();
            let mut add = |owner: Arc<str>, acquired_at: SystemTime, lock_timeout: Duration, count: u32| {
                let expires_at = acquired_at
                    .checked_add(lock_timeout)
                    .unwrap_or_else(|| acquired_at + crate::fast_lock::DEFAULT_LOCK_TIMEOUT);
                let generations = if include_generations {
                    Self::holder_generations(&holders, &owner, mode, count)
                } else {
                    None
                };
                infos.push((
                    crate::fast_lock::types::ObjectLockInfo {
                        key: key.clone(),
                        mode,
                        owner,
                        acquired_at,
                        expires_at,
                        priority,
                    },
                    count,
                    generations,
                ));
            };
            match mode {
                LockMode::Exclusive => {
                    if let Some(info) = state.current_owner.read().clone() {
                        add(info.owner, info.acquired_at, info.lock_timeout, 1);
                    }
                }
                LockMode::Shared => {
                    for entry in state.shared_owners.read().iter() {
                        add(entry.owner.clone(), entry.acquired_at, entry.lock_timeout, entry.count);
                    }
                }
            }
        }
        infos
    }

    fn holder_generations(holders: &[Holder], owner: &Arc<str>, mode: LockMode, count: u32) -> Option<Vec<u64>> {
        let mut ids: Vec<_> = holders
            .iter()
            .filter(|holder| holder.owner == *owner && holder.mode == mode)
            .map(|holder| holder.token.as_ref().and_then(|token| (token.id != 0).then_some(token.id)))
            .collect::<Option<_>>()?;
        if u32::try_from(ids.len()).ok() != Some(count) {
            return None;
        }
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }
        Some(ids)
    }

    /// Force-release every holder of a lock on `key`, regardless of owner.
    ///
    /// Returns the number of owners that were released. Used by the admin
    /// force-unlock path to clear a stuck resource.
    pub fn force_release_all(&self, key: &ObjectKey) -> usize {
        let (state, outcomes, owners) = {
            let objects = self.objects.read();
            let Some(state) = objects.get(key).cloned() else {
                return 0;
            };
            #[cfg(test)]
            let mut holders = match state.holders.try_lock() {
                Some(holders) => holders,
                None => {
                    test_pause(TestPausePoint::ForceGateContended, key);
                    state.holders.lock()
                }
            };
            #[cfg(not(test))]
            let mut holders = state.holders.lock();
            let mut outcomes = Vec::with_capacity(holders.len());
            let mut owners: Vec<Arc<str>> = Vec::new();
            let mut index = 0;
            while index < holders.len() {
                let owner = holders[index].owner.clone();
                let outcome = Self::release_physical(&state, &owner, holders[index].mode);
                if outcome.success {
                    let holder = holders.remove(index);
                    self.record_holder_release(&holder);
                    self.metrics.record_release();
                    if !owners.contains(&owner) {
                        owners.push(owner.clone());
                    }
                } else {
                    index += 1;
                }
                outcomes.push((owner, outcome));
            }
            drop(holders);
            (state, outcomes, owners)
        };
        for (owner, outcome) in outcomes {
            outcome.finish(&state, &owner);
        }
        owners.len()
    }

    /// Get current load factor of the shard
    pub fn current_load_factor(&self) -> f64 {
        let objects = self.objects.read();
        let total_locks = objects.len();
        if total_locks == 0 {
            return 0.0;
        }

        let active_locks = objects.values().filter(|state| state.is_locked()).count();
        active_locks as f64 / total_locks as f64
    }

    /// Get count of active locks
    pub fn active_lock_count(&self) -> usize {
        let objects = self.objects.read();
        objects.values().filter(|state| state.is_locked()).count()
    }

    pub(crate) fn current_lock_counts(&self) -> (u64, u64) {
        self.objects
            .read()
            .values()
            .map(|state| state.atomic_state.holder_and_waiter_counts())
            .fold((0, 0), |(holders, waiters), (state_holders, state_waiters)| {
                (holders.saturating_add(state_holders), waiters.saturating_add(state_waiters))
            })
    }

    /// Adaptive cleanup based on current load
    pub fn adaptive_cleanup(&self) -> usize {
        let current_load = self.current_load_factor();
        let lock_count = self.lock_count();
        let active_guard_count = self.active_guards.lock().len();

        // Be much more conservative if there are active guards or very high load
        if active_guard_count > 0 && current_load > 0.8 {
            tracing::debug!(
                "Skipping aggressive cleanup due to {} active guards and high load ({:.2})",
                active_guard_count,
                current_load
            );
            // Only clean very old entries when under high load with active guards
            return self.cleanup_expired_batch(3, 1_200_000); // 20 minutes, smaller batches
        }

        // Under extreme load, skip cleanup entirely to reduce contention
        if current_load > 1.5 && active_guard_count > 10 {
            tracing::debug!(
                "Skipping all cleanup due to extreme load ({:.2}) and {} active guards",
                current_load,
                active_guard_count
            );
            return 0;
        }

        // Dynamically adjust cleanup strategy based on load
        let cleanup_batch_size = match current_load {
            load if load > 0.9 => lock_count / 50, // Much smaller batches for high load
            load if load > 0.7 => lock_count / 20, // Smaller batches for medium load
            _ => lock_count / 10,                  // More conservative even for low load
        };

        // Use much longer timeouts to prevent premature cleanup
        let cleanup_threshold_millis = match current_load {
            load if load > 0.8 => 600_000, // 10 minutes for high load
            load if load > 0.5 => 300_000, // 5 minutes for medium load
            _ => 120_000,                  // 2 minutes for low load
        };

        self.cleanup_expired_batch_protected(cleanup_batch_size.max(5), cleanup_threshold_millis)
    }

    /// Cleanup expired and unused locks
    pub fn cleanup_expired(&self, max_idle_secs: u64) -> usize {
        let max_idle_millis = max_idle_secs * 1000;
        self.cleanup_expired_millis(max_idle_millis)
    }

    /// Cleanup expired and unused locks with millisecond precision
    pub fn cleanup_expired_millis(&self, max_idle_millis: u64) -> usize {
        let mut cleaned = 0;
        let now_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let mut objects = self.objects.write();
        objects.retain(|_key, state| {
            if !state.is_locked() && !state.atomic_state.has_waiters() && state.holders.lock().is_empty() {
                let last_access_secs = state.atomic_state.last_accessed();
                let last_access_millis = last_access_secs * 1000; // Convert to millis
                let idle_time = now_millis.saturating_sub(last_access_millis);

                if idle_time > max_idle_millis {
                    cleaned += 1;
                    false // Remove this entry
                } else {
                    true // Keep this entry
                }
            } else {
                true // Keep locked or waited entries
            }
        });

        self.metrics.record_cleanup(cleaned);
        cleaned
    }

    /// Protected batch cleanup that respects active guards
    fn cleanup_expired_batch_protected(&self, max_batch_size: usize, cleanup_threshold_millis: u64) -> usize {
        let active_guards = self.active_guards.lock();
        let guard_count = active_guards.len();
        drop(active_guards); // Release lock early

        if guard_count > 0 {
            tracing::debug!("Cleanup with {} active guards, being conservative", guard_count);
        }

        self.cleanup_expired_batch(max_batch_size, cleanup_threshold_millis)
    }

    /// Batch cleanup with limited processing to avoid blocking
    fn cleanup_expired_batch(&self, max_batch_size: usize, cleanup_threshold_millis: u64) -> usize {
        let mut cleaned = 0;
        let now_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let mut objects = self.objects.write();
        let mut processed = 0;

        // Collect the keys to evict during the walk, then remove them afterward.
        // `retain` only exposes a shared `&Arc`, and cloning it keeps
        // `strong_count >= 2`, so `Arc::try_unwrap` on that clone can never
        // succeed. Removing the key after the walk hands back the owned `Arc`,
        // which can be unwrapped and recycled into the pool.
        let mut to_remove = Vec::new();
        objects.retain(|key, state| {
            if processed >= max_batch_size {
                return true; // Stop processing after batch limit
            }
            processed += 1;

            if !state.is_locked() && !state.atomic_state.has_waiters() && state.holders.lock().is_empty() {
                let last_access_millis = state.atomic_state.last_accessed() * 1000;
                let idle_time = now_millis.saturating_sub(last_access_millis);

                if idle_time > cleanup_threshold_millis {
                    to_remove.push(key.clone());
                    cleaned += 1;
                }
            }
            // Removal is deferred to the loop below so the owned `Arc` can be
            // recovered for recycling.
            true
        });

        // Evict the collected keys and recycle their states into the pool.
        // Removing the key yields the owned `Arc`; `try_unwrap` succeeds only
        // when this shard held the last reference, in which case the inner
        // state is reboxed and returned to the pool.
        for key in to_remove {
            if let Some(state) = objects.remove(&key)
                && let Ok(state) = Arc::try_unwrap(state)
            {
                self.object_pool.release(Box::new(state.state));
            }
        }

        self.metrics.record_cleanup(cleaned);
        cleaned
    }

    /// Get shard metrics
    pub fn metrics(&self) -> &ShardMetrics {
        &self.metrics
    }

    /// Get current lock count
    pub fn lock_count(&self) -> usize {
        self.objects.read().len()
    }

    /// Schedule background cleanup for a key
    fn schedule_cleanup(&self, key: ObjectKey) {
        // Don't immediately cleanup - let cleanup_expired handle it
        // This allows the cleanup test to work properly
        let _ = key; // Suppress unused variable warning
    }

    /// Get object pool statistics
    pub fn pool_stats(&self) -> (u64, u64, u64, usize) {
        self.object_pool.stats()
    }

    /// Get object pool hit rate
    pub fn pool_hit_rate(&self) -> f64 {
        self.object_pool.hit_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_lock::types::{LockPriority, ObjectKey};
    use crate::fast_lock::{FastLockGuard, FastObjectLockManager};

    fn install_pause(point: TestPausePoint, key: &ObjectKey) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let previous = TEST_PAUSES.lock().insert(
            (point, key.clone()),
            TestPause {
                entered: entered_tx,
                resume: resume_rx,
            },
        );
        assert!(previous.is_none());
        (entered_rx, resume_tx)
    }

    fn test_manager() -> Arc<FastObjectLockManager> {
        test_manager_with_shards(4)
    }

    fn test_manager_with_shards(shard_count: usize) -> Arc<FastObjectLockManager> {
        // Construct outside a runtime so no background cleanup races the test.
        Arc::new(
            std::thread::spawn(move || {
                FastObjectLockManager::with_config(crate::fast_lock::types::LockConfig {
                    shard_count,
                    ..Default::default()
                })
            })
            .join()
            .expect("manager construction"),
        )
    }

    fn start_acquisition(
        manager: &Arc<FastObjectLockManager>,
        request: ObjectLockRequest,
    ) -> tokio::task::JoinHandle<Result<FastLockGuard, LockResult>> {
        let manager = manager.clone();
        tokio::spawn(async move { manager.acquire_lock(request).await })
    }

    async fn assert_cleanup_cannot_split_state(
        manager: Arc<FastObjectLockManager>,
        key: ObjectKey,
        entered: std::sync::mpsc::Receiver<()>,
        resume: std::sync::mpsc::Sender<()>,
        pending: tokio::task::JoinHandle<Result<FastLockGuard, LockResult>>,
        seed: Option<FastLockGuard>,
    ) {
        entered.recv_timeout(Duration::from_secs(5)).expect("state lookup");
        drop(seed);

        // Idle timestamps have one-second precision, while cleanup compares milliseconds.
        let initial_millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() == initial_millis {
            std::thread::yield_now();
        }

        let can_write_map = manager.get_shard(&key).objects.try_write().is_some();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut cleanup = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::task::spawn_blocking(move || {
                started_tx.send(()).expect("cleanup start receiver");
                manager.get_shard(&key).cleanup_expired_millis(0)
            })
        };
        started_rx.recv_timeout(Duration::from_secs(5)).expect("cleanup started");
        if can_write_map {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), &mut cleanup)
                    .await
                    .expect("cleanup completed")
                    .expect("cleanup task"),
                1
            );
            let successor = manager
                .acquire_write_lock(key.clone(), "second")
                .await
                .expect("successor should acquire after cleanup");
            resume.send(()).expect("resume acquisition");
            let result = tokio::time::timeout(Duration::from_secs(5), pending)
                .await
                .expect("acquisition completed")
                .expect("acquisition task");
            assert!(result.is_err(), "a removed state must not grant a conflicting lock");
            drop(successor);
        } else {
            resume.send(()).expect("resume acquisition");
            let first = tokio::time::timeout(Duration::from_secs(5), pending)
                .await
                .expect("acquisition completed")
                .expect("acquisition task")
                .expect("first acquisition");
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), cleanup)
                    .await
                    .expect("cleanup completed")
                    .expect("cleanup task"),
                0
            );
            let second = ObjectLockRequest::new_write(key.clone(), "second").with_acquire_timeout(Duration::from_millis(100));
            assert!(
                manager.acquire_lock(second).await.is_err(),
                "conflicting locks must remain mutually exclusive"
            );
            drop(first);
            let successor = manager
                .acquire_write_lock(key, "second")
                .await
                .expect("successor after release");
            drop(successor);
        }
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_waits_for_tracked_holder_and_registration_commit() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "force-during-commit");
        drop(manager.acquire_write_lock(key.clone(), "seed").await.expect("seed"));
        let shard = manager.get_shard(&key).clone();
        let state = shard.objects.read().get(&key).cloned().expect("existing state");
        let (entered, resume) = install_pause(TestPausePoint::TrackedBeforeRegistration, &key);
        let pending = start_acquisition(&manager, ObjectLockRequest::new_write(key.clone(), "owner"));
        entered.recv_timeout(Duration::from_secs(5)).expect("tracked acquisition");
        assert!(state.holders.try_lock().is_none(), "physical holder and registration must share a gate");

        let (force_contended, resume_force) = install_pause(TestPausePoint::ForceGateContended, &key);
        let force = {
            let manager = manager.clone();
            let key = key.clone();
            tokio::task::spawn_blocking(move || manager.force_unlock(&key))
        };
        force_contended
            .recv_timeout(Duration::from_secs(5))
            .expect("force encountered the holder gate");
        resume_force.send(()).expect("resume force");
        resume.send(()).expect("resume acquisition");
        let stale = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("acquisition completed")
            .expect("acquisition task")
            .expect("initial acquisition");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), force)
                .await
                .expect("force completed")
                .expect("force task"),
            1
        );
        assert!(!shard.is_guard_active(stale.guard_id()));

        let successor = manager.acquire_write_lock(key.clone(), "owner").await.expect("successor");
        let entries = manager.list_locks_with_holder_generations();
        let (_, count, generations) = entries
            .iter()
            .find(|(info, _, _)| info.key == key)
            .expect("successor snapshot");
        assert_eq!(*count, 1);
        assert_eq!(generations.as_ref(), Some(&vec![successor.guard_id()]));
        drop(stale);
        let contender = ObjectLockRequest::new_write(key, "other").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(contender).await.is_err());
        drop(successor);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_numeric_release_does_not_release_reacquired_holder() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "stale-numeric-release");
        let owner: Arc<str> = Arc::from("owner");
        let stale = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("initial holder");
        let shard = manager.get_shard(&key).clone();
        let (entered, resume) = install_pause(TestPausePoint::GuardIdAfterLookup, &key);
        let release = {
            let key = key.clone();
            let owner = owner.clone();
            let id = stale.guard_id();
            tokio::task::spawn_blocking(move || shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id))
        };
        entered.recv_timeout(Duration::from_secs(5)).expect("numeric release lookup");
        assert_eq!(manager.force_unlock(&key), 1);
        let successor = manager.acquire_write_lock(key.clone(), owner).await.expect("successor");
        resume.send(()).expect("resume numeric release");
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), release)
                .await
                .expect("numeric release completed")
                .expect("release task"),
            "an invalidated numeric release must not release the successor"
        );
        drop(stale);
        let contender = ObjectLockRequest::new_write(key, "other").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(contender).await.is_err());
        drop(successor);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_weak_numeric_release_does_not_release_successor() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "in-flight-weak-release");
        let owner: Arc<str> = Arc::from("owner");
        let stale = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("initial holder");
        let shard = manager.get_shard(&key).clone();
        shard.unregister_guard(stale.guard_id());
        shard.register_guard(stale.guard_id());
        let (entered, resume) = install_pause(TestPausePoint::GuardIdAfterLookup, &key);
        let release = {
            let shard = shard.clone();
            let key = key.clone();
            let owner = owner.clone();
            let id = stale.guard_id();
            tokio::task::spawn_blocking(move || shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id))
        };
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("weak numeric release lookup");
        drop(stale);
        let successor = manager.acquire_write_lock(key.clone(), owner).await.expect("successor");
        resume.send(()).expect("resume weak numeric release");
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), release)
                .await
                .expect("numeric release completed")
                .expect("release task"),
            "an in-flight weak release must not release the successor"
        );
        let contender = ObjectLockRequest::new_write(key, "other").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(contender).await.is_err());
        drop(successor);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_weak_release_preserves_replacement_tracked_registration() {
        let manager = test_manager_with_shards(1);
        let key = ObjectKey::new("bucket", "weak-id-replacement");
        let owner: Arc<str> = Arc::from("owner");
        let shard = manager.get_shard(&key).clone();
        let id = 42;
        shard.register_guard(id);
        let (entered, resume) = install_pause(TestPausePoint::GuardIdAfterLookup, &key);
        let release = {
            let shard = shard.clone();
            let key = key.clone();
            let owner = owner.clone();
            tokio::task::spawn_blocking(move || shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id))
        };
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("weak numeric release lookup");
        shard.unregister_guard(id);
        let token = Arc::new(GuardToken {
            id,
            acquired: std::sync::atomic::AtomicBool::new(false),
            tracked: true,
        });
        let successor = FastLockGuard::new(key.clone(), LockMode::Exclusive, owner.clone(), shard.clone(), token.clone());
        shard
            .acquire_tracked(&ObjectLockRequest::new_write(key.clone(), owner), &token)
            .await
            .expect("replacement tracked holder");
        resume.send(()).expect("resume weak numeric release");
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), release)
                .await
                .expect("numeric release completed")
                .expect("release task")
        );
        assert!(shard.is_guard_active(id), "replacement tracked registration must remain");
        let contender = ObjectLockRequest::new_write(key, "other").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(contender).await.is_err());
        drop(successor);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_weak_release_preserves_reregistered_raw_holder() {
        let manager = test_manager_with_shards(1);
        let key = ObjectKey::new("bucket", "weak-raw-replacement");
        let owner: Arc<str> = Arc::from("owner");
        let shard = manager.get_shard(&key).clone();
        let request = ObjectLockRequest::new_write(key.clone(), owner.clone());
        let id = u64::MAX;
        shard.acquire_lock(&request).await.expect("original raw holder");
        shard.register_guard(id);
        let (entered, resume) = install_pause(TestPausePoint::GuardIdAfterLookup, &key);
        let release = {
            let shard = shard.clone();
            let key = key.clone();
            let owner = owner.clone();
            tokio::task::spawn_blocking(move || shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id))
        };
        entered.recv_timeout(Duration::from_secs(5)).expect("old weak lookup");
        assert!(shard.release_lock(&key, &owner, LockMode::Exclusive));
        shard.unregister_guard(id);
        shard.acquire_lock(&request).await.expect("replacement raw holder");
        shard.register_guard(id);
        resume.send(()).expect("resume old weak release");
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), release)
                .await
                .expect("release completed")
                .expect("release task")
        );
        assert!(shard.is_guard_active(id));
        assert!(!shard.try_fast_path_only(&ObjectLockRequest::new_write(key.clone(), "other")));
        assert!(shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id));
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn tracked_revocation_preserves_a_replacement_weak_registration() {
        let manager = test_manager_with_shards(1);
        let key = ObjectKey::new("bucket", "weak-registration-after-tracked");
        let owner: Arc<str> = Arc::from("owner");
        let tracked = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("tracked holder");
        let id = tracked.guard_id();
        let shard = manager.get_shard(&key).clone();
        shard.unregister_guard(id);
        shard.register_guard(id);
        assert_eq!(manager.force_unlock(&key), 1);
        assert!(tracked.is_released());
        assert!(shard.is_guard_active(id), "tracked cleanup must not erase the weak registration");
        shard
            .acquire_lock(&ObjectLockRequest::new_write(key.clone(), owner.clone()))
            .await
            .expect("raw successor");
        drop(tracked);
        assert!(shard.release_lock_with_guard(&key, &owner, LockMode::Exclusive, id));
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn raw_and_weak_release_prefer_raw_shared_holder() {
        let manager = test_manager_with_shards(1);
        for weak in [false, true] {
            for batch in [false, true] {
                let key = ObjectKey::new("bucket", format!("mixed-shared-{weak}-{batch}"));
                let owner: Arc<str> = Arc::from("owner");
                let tracked = manager
                    .acquire_read_lock(key.clone(), owner.clone())
                    .await
                    .expect("tracked holder");
                let shard = manager.get_shard(&key).clone();
                let request = ObjectLockRequest::new_read(key.clone(), owner.clone());
                if batch {
                    assert_eq!(
                        shard.acquire_locks_batch(vec![request], true).await.expect("raw batch"),
                        vec![key.clone()]
                    );
                } else {
                    shard.acquire_lock(&request).await.expect("raw holder");
                }
                if weak {
                    shard.register_guard(u64::MAX);
                    assert!(shard.release_lock_with_guard(&key, &owner, LockMode::Shared, u64::MAX));
                } else {
                    assert!(shard.release_lock(&key, &owner, LockMode::Shared));
                }
                assert!(!tracked.is_released());
                assert!(shard.is_guard_active(tracked.guard_id()));
                assert!(!shard.try_fast_path_only(&ObjectLockRequest::new_write(key.clone(), "other")));
                drop(tracked);
                let request = ObjectLockRequest::new_write(key.clone(), "other");
                assert!(shard.try_fast_path_only(&request));
                assert!(shard.release_lock(&key, &request.owner, LockMode::Exclusive));
            }
        }
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn releasing_one_token_preserves_a_different_token_with_the_same_numeric_id() {
        let manager = test_manager_with_shards(1);
        for force in [false, true] {
            let first_key = ObjectKey::new("bucket", format!("first-collision-{force}"));
            let second_key = ObjectKey::new("bucket", format!("second-collision-{force}"));
            let owner: Arc<str> = Arc::from("owner");
            let first = manager
                .acquire_write_lock(first_key.clone(), owner.clone())
                .await
                .expect("first holder");
            let shard = manager.get_shard(&first_key).clone();
            let token = Arc::new(GuardToken {
                id: first.guard_id(),
                acquired: std::sync::atomic::AtomicBool::new(false),
                tracked: true,
            });
            let second = FastLockGuard::new(second_key.clone(), LockMode::Exclusive, owner.clone(), shard.clone(), token.clone());
            shard
                .acquire_tracked(&ObjectLockRequest::new_write(second_key.clone(), owner.clone()), &token)
                .await
                .expect("second holder");
            if force {
                assert_eq!(manager.force_unlock(&first_key), 1);
                drop(first);
            } else {
                drop(first);
            }
            assert!(shard.is_guard_active(token.id), "different live token registration must survive");
            assert!(
                !shard.release_lock_with_guard(&second_key, &owner, LockMode::Exclusive, token.id),
                "a numeric ID that collided must remain ambiguous"
            );
            assert!(!second.is_released());
            drop(second);
            let contender = ObjectLockRequest::new_write(second_key, "other").with_acquire_timeout(Duration::from_millis(100));
            drop(manager.acquire_lock(contender).await.expect("second holder released"));
        }
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raw_batch_failure_rolls_back_a_previously_acquired_lock() {
        let manager = test_manager_with_shards(1);
        let first = ObjectKey::new("bucket", "a-first");
        let blocked = ObjectKey::new("bucket", "z-blocked");
        let blocker = manager.acquire_write_lock(blocked.clone(), "blocker").await.expect("blocker");
        let (entered, resume) = install_pause(TestPausePoint::SlowLookup, &blocked);
        let requests = vec![
            ObjectLockRequest::new_write(first.clone(), "raw"),
            ObjectLockRequest::new_write(blocked, "raw").with_acquire_timeout(Duration::from_millis(100)),
        ];
        let batch = {
            let shard = manager.get_shard(&first).clone();
            tokio::spawn(async move { shard.acquire_locks_batch(requests, true).await })
        };
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("second batch lock reached slow path");
        let first_info = manager.get_lock_info(&first).expect("first raw batch lock is held");
        assert_eq!(first_info.owner.as_ref(), "raw");
        assert!(!batch.is_finished());
        resume.send(()).expect("resume blocked batch request");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), batch)
                .await
                .expect("batch completed")
                .expect("batch task")
                .is_err()
        );
        let request = ObjectLockRequest::new_write(first, "other").with_acquire_timeout(Duration::from_millis(100));
        drop(
            manager
                .acquire_lock(request)
                .await
                .expect("failed batch released its first lock"),
        );
        drop(blocker);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_raw_batch_does_not_release_a_reacquired_holder() {
        let manager = test_manager_with_shards(1);
        let first = ObjectKey::new("bucket", "a-batch-replaced");
        let blocked = ObjectKey::new("bucket", "z-batch-blocked");
        let owner: Arc<str> = Arc::from("raw");
        let blocker = manager.acquire_write_lock(blocked.clone(), "blocker").await.expect("blocker");
        let shard = manager.get_shard(&first).clone();
        let (entered, resume) = install_pause(TestPausePoint::SlowLookup, &blocked);
        let requests = vec![
            ObjectLockRequest::new_write(first.clone(), owner.clone()),
            ObjectLockRequest::new_write(blocked, owner.clone()).with_acquire_timeout(Duration::from_millis(100)),
        ];
        let batch = {
            let shard = shard.clone();
            tokio::spawn(async move { shard.acquire_locks_batch(requests, true).await })
        };
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("batch reached blocked key");
        assert_eq!(manager.force_unlock(&first), 1);
        shard
            .acquire_lock(&ObjectLockRequest::new_write(first.clone(), owner.clone()))
            .await
            .expect("raw successor");
        resume.send(()).expect("resume batch");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), batch)
                .await
                .expect("batch completed")
                .expect("batch task")
                .is_err()
        );
        assert!(!shard.try_fast_path_only(&ObjectLockRequest::new_write(first.clone(), "other")));
        assert!(shard.release_lock(&first, &owner, LockMode::Exclusive));
        drop(blocker);
        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_raw_batch_releases_its_holders_and_waiter() {
        let manager = test_manager_with_shards(1);
        let first = ObjectKey::new("bucket", "a-cancel-batch");
        let blocked = ObjectKey::new("bucket", "z-cancel-batch");
        let blocker = manager.acquire_write_lock(blocked.clone(), "blocker").await.expect("blocker");
        let shard = manager.get_shard(&first).clone();
        let (entered, resume) = install_pause(TestPausePoint::SlowLookup, &blocked);
        let requests = vec![
            ObjectLockRequest::new_write(first.clone(), "raw"),
            ObjectLockRequest::new_write(blocked.clone(), "raw").with_acquire_timeout(Duration::from_secs(30)),
        ];
        let batch = {
            let shard = shard.clone();
            tokio::spawn(async move { shard.acquire_locks_batch(requests, true).await })
        };
        entered
            .recv_timeout(Duration::from_secs(5))
            .expect("batch reached blocked key");
        let state = shard.objects.read().get(&blocked).cloned().expect("blocked state");
        resume.send(()).expect("resume batch");
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.atomic_state.writers_waiting_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch waiter registered");
        batch.abort();
        assert!(batch.await.expect_err("batch cancelled").is_cancelled());
        assert_eq!(state.atomic_state.writers_waiting_count(), 0);
        let contender = ObjectLockRequest::new_write(first.clone(), "other");
        assert!(shard.try_fast_path_only(&contender));
        assert!(shard.release_lock(&first, &contender.owner, LockMode::Exclusive));
        drop(blocker);
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn pooled_state_does_not_revive_a_forced_guard() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "pooled-guard");
        let stale = manager
            .acquire_write_lock(key.clone(), "owner")
            .await
            .expect("initial acquisition");
        assert_eq!(manager.force_unlock(&key), 1);
        let shard = manager.get_shard(&key);

        let initial_millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        while SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() == initial_millis {
            std::thread::yield_now();
        }
        assert_eq!(shard.cleanup_expired_batch(10, 0), 1);
        let hits = shard.pool_stats().0;
        let successor = manager.acquire_write_lock(key.clone(), "owner").await.expect("successor");
        assert_eq!(shard.pool_stats().0, hits + 1);
        drop(stale);
        let contender = ObjectLockRequest::new_write(key, "other").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(contender).await.is_err());
        drop(successor);
        manager.shutdown().await;
    }

    struct ReentrantWake {
        shard: Arc<LockShard>,
        key: ObjectKey,
        woke: std::sync::mpsc::Sender<usize>,
    }

    impl std::task::Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.woke.send(self.shard.force_release_all(&self.key));
        }
    }

    async fn check_reentrant_wake(force: bool) -> bool {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        let manager = test_manager();
        let key = ObjectKey::new("bucket", if force { "force-wake" } else { "drop-wake" });
        let guard = manager.acquire_write_lock(key.clone(), "owner").await.expect("writer");
        let shard = manager.get_shard(&key).clone();
        let state = shard.objects.read().get(&key).cloned().expect("writer state");
        let (woke_tx, woke_rx) = std::sync::mpsc::channel();
        let waker = Waker::from(Arc::new(ReentrantWake {
            shard,
            key: key.clone(),
            woke: woke_tx,
        }));
        let mut context = Context::from_waker(&waker);
        let mut waiter = Box::pin(state.optimized_notify.wait_for_read());
        if !matches!(waiter.as_mut().poll(&mut context), Poll::Pending) {
            return false;
        }

        let task = {
            let manager = manager.clone();
            tokio::task::spawn_blocking(move || {
                if force {
                    let released = manager.force_unlock(&key);
                    drop(guard);
                    released
                } else {
                    drop(guard);
                    1
                }
            })
        };
        let completed = matches!(tokio::time::timeout(Duration::from_secs(2), task).await, Ok(Ok(1)));
        let callback = woke_rx.try_recv().ok() == Some(0);
        drop(waiter);
        if completed {
            manager.shutdown().await;
        }
        completed && callback
    }

    #[test]
    fn release_wakes_can_reenter_the_shard() {
        const CHILD_ENV: &str = "RUSTFS_TEST_REENTRANT_LOCK_WAKE_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "fast_lock::shard::tests::release_wakes_can_reenter_the_shard",
                    "--exact",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .output()
                .expect("isolated wake test");
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            return;
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let passed = runtime.block_on(async { check_reentrant_wake(false).await && check_reentrant_wake(true).await });
        runtime.shutdown_timeout(Duration::from_millis(100));
        assert!(passed, "waker must be able to reenter after the physical release");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cleanup_during_fast_existing_state_acquisition() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "fast-existing-cleanup");
        drop(manager.acquire_write_lock(key.clone(), "seed").await.expect("seed"));
        let (entered, resume) = install_pause(TestPausePoint::FastExisting, &key);
        let pending = start_acquisition(&manager, ObjectLockRequest::new_write(key.clone(), "first"));
        assert_cleanup_cannot_split_state(manager, key, entered, resume, pending, None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cleanup_during_fast_write_recheck_acquisition() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "fast-recheck-cleanup");
        let (first_entered, first_resume) = install_pause(TestPausePoint::BeforeFastWrite, &key);
        let pending = start_acquisition(&manager, ObjectLockRequest::new_write(key.clone(), "first"));
        first_entered
            .recv_timeout(Duration::from_secs(5))
            .expect("before write recheck");
        drop(manager.acquire_write_lock(key.clone(), "seed").await.expect("seed"));
        let (entered, resume) = install_pause(TestPausePoint::FastWriteExisting, &key);
        first_resume.send(()).expect("resume before write recheck");
        assert_cleanup_cannot_split_state(manager, key, entered, resume, pending, None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cleanup_during_slow_existing_state_acquisition() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "slow-existing-cleanup");
        let seed = manager.acquire_write_lock(key.clone(), "seed").await.expect("seed");
        let (entered, resume) = install_pause(TestPausePoint::SlowLookup, &key);
        let pending = start_acquisition(&manager, ObjectLockRequest::new_write(key.clone(), "first"));
        assert_cleanup_cannot_split_state(manager, key, entered, resume, pending, Some(seed)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cleanup_during_slow_new_state_acquisition() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "slow-new-cleanup");
        let (entered, resume) = install_pause(TestPausePoint::SlowLookup, &key);
        let pending = start_acquisition(&manager, ObjectLockRequest::new_read(key.clone(), "first"));
        assert_cleanup_cannot_split_state(manager, key, entered, resume, pending, None).await;
    }

    #[tokio::test]
    async fn test_cleanup_keeps_new_fast_state_while_held() {
        let manager = test_manager();
        let key = ObjectKey::new("bucket", "fast-new-cleanup");
        let first = manager
            .acquire_write_lock(key.clone(), "first")
            .await
            .expect("first acquisition");
        assert_eq!(manager.get_shard(&key).cleanup_expired_millis(0), 0);
        let second = ObjectLockRequest::new_write(key.clone(), "second").with_acquire_timeout(Duration::from_millis(100));
        assert!(manager.acquire_lock(second).await.is_err());
        drop(first);
        drop(
            manager
                .acquire_write_lock(key, "second")
                .await
                .expect("successor after release"),
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_shard_fast_path() {
        let shard = LockShard::new(0);
        let key = ObjectKey::new("bucket", "object");
        let owner: Arc<str> = Arc::from("owner");

        let request = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        // Should succeed via fast path
        assert!(shard.acquire_lock(&request).await.is_ok());
        assert!(shard.release_lock(&key, &owner, LockMode::Exclusive));
    }

    #[tokio::test]
    async fn test_shard_contention() {
        let shard = Arc::new(LockShard::new(0));
        let key = ObjectKey::new("bucket", "object");

        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");

        let request1 = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner1.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        let request2 = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner2.clone(),
            acquire_timeout: Duration::from_millis(100),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        // First lock should succeed
        assert!(shard.acquire_lock(&request1).await.is_ok());

        // Second lock should timeout
        assert!(matches!(shard.acquire_lock(&request2).await, Err(LockResult::Timeout)));

        // Release first lock
        assert!(shard.release_lock(&key, &owner1, LockMode::Exclusive));
    }

    // Regression for the waiter-preserving early retry (rustfs#5657).
    //
    // The early retries used a bare `sleep`, which subscribes to nothing.
    // `notify_writer`/`notify_readers` are gated on the waiter counters, so a
    // sleeping waiter is invisible to every release: the lock sits free while
    // each loser sleeps out its full 10/20/40/80/100ms rung, and under N-writer
    // same-key contention that ladder — not the hold — is what the wait costs.
    // Registration is what lets a release reach the waiter at all; the wakeup
    // itself is covered by `write_lock_waiter_is_not_stranded_by_missed_wakeup`.
    //
    // MAX_RETRIES rungs total ~750ms, so the window sampled here sits entirely
    // inside the early-retry phase, where a sleeping waiter registers nowhere.
    //
    // Wakeup *latency* is deliberately not asserted: NOTIFY_POOL is a global of
    // 128 `Notify`s shared by every lock in the process, so a waiter in another
    // concurrently-running test can consume this one's `notify_one` and push it
    // out to the end of its rung. That is the same stolen wakeup NOTIFY_WAIT_CAP
    // exists to bound, and it makes any latency budget flaky in-suite.
    #[tokio::test(flavor = "multi_thread")]
    async fn early_retry_registers_as_waiter() {
        let shard = Arc::new(LockShard::new(0));
        let key = ObjectKey::new("bucket", "object");
        let holder: Arc<str> = Arc::from("holder");
        let waiter: Arc<str> = Arc::from("waiter");

        let request = |owner: Arc<str>| ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner,
            acquire_timeout: Duration::from_secs(5),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        assert!(shard.acquire_lock(&request(holder.clone())).await.is_ok());

        let waiter_shard = shard.clone();
        let waiter_request = request(waiter.clone());
        let waiter_task = tokio::spawn(async move { waiter_shard.acquire_lock(&waiter_request).await });

        let sample_until = Instant::now() + Duration::from_millis(200);
        let mut registered = false;
        while !registered && Instant::now() < sample_until {
            registered = shard
                .objects
                .read()
                .get(&key)
                .is_some_and(|state| state.atomic_state.writers_waiting_count() > 0);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(
            registered,
            "a waiter in the early-retry backoff must be registered in the writer waiter count, \
             otherwise releases cannot reach it"
        );

        assert!(shard.release_lock(&key, &holder, LockMode::Exclusive));
        waiter_task
            .await
            .expect("waiter task should not panic")
            .expect("waiter must acquire once the holder releases");
    }

    #[test]
    fn test_adaptive_timeout_does_not_exceed_request_acquire_timeout() {
        let shard = LockShard::new(0);
        let key = ObjectKey::new("bucket", "object");
        let owner: Arc<str> = Arc::from("owner");
        let acquire_timeout = Duration::from_millis(500);

        for priority in [LockPriority::Normal, LockPriority::High, LockPriority::Critical] {
            let request = ObjectLockRequest {
                key: key.clone(),
                mode: LockMode::Exclusive,
                owner: owner.clone(),
                acquire_timeout,
                lock_timeout: Duration::from_secs(30),
                priority,
            };

            assert_eq!(
                shard.calculate_adaptive_timeout(&request),
                acquire_timeout,
                "adaptive timeout must not extend the requested acquire timeout for {priority:?} priority"
            );
        }
    }

    #[test]
    fn test_adaptive_timeout_does_not_exceed_global_max_acquire_timeout() {
        let shard = LockShard::new(0);
        let request = ObjectLockRequest {
            key: ObjectKey::new("bucket", "object"),
            mode: LockMode::Exclusive,
            owner: Arc::from("owner"),
            acquire_timeout: crate::fast_lock::MAX_ACQUIRE_TIMEOUT + Duration::from_secs(60),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Critical,
        };

        assert_eq!(
            shard.calculate_adaptive_timeout(&request),
            crate::fast_lock::MAX_ACQUIRE_TIMEOUT,
            "adaptive timeout must not extend the global max acquire timeout"
        );
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let shard = LockShard::new(0);
        let owner: Arc<str> = Arc::from("owner");

        let requests = vec![
            ObjectLockRequest {
                key: ObjectKey::new("bucket", "obj1"),
                mode: LockMode::Exclusive,
                owner: owner.clone(),
                acquire_timeout: Duration::from_secs(1),
                lock_timeout: Duration::from_secs(30),
                priority: LockPriority::Normal,
            },
            ObjectLockRequest {
                key: ObjectKey::new("bucket", "obj2"),
                mode: LockMode::Shared,
                owner: owner.clone(),
                acquire_timeout: Duration::from_secs(1),
                lock_timeout: Duration::from_secs(30),
                priority: LockPriority::Normal,
            },
        ];

        let result = shard.acquire_locks_batch(requests, true).await;
        assert!(result.is_ok());

        let acquired = result.unwrap();
        assert_eq!(acquired.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_lock_cleanup_safety() {
        let shard = LockShard::new(0);

        // First acquire a lock that will block the batch operation
        let blocking_request = ObjectLockRequest::new_write(ObjectKey::new("bucket", "obj1"), "blocking_owner")
            .with_acquire_timeout(Duration::from_secs(1));
        shard.acquire_lock(&blocking_request).await.unwrap();

        // Use short acquire timeout so the test fails fast when obj1 is already locked
        // (default is 60s which would make this test very slow)
        let requests = vec![
            ObjectLockRequest::new_read(ObjectKey::new("bucket", "obj2"), "batch_owner")
                .with_acquire_timeout(Duration::from_millis(100)), // This should succeed
            ObjectLockRequest::new_write(ObjectKey::new("bucket", "obj1"), "batch_owner")
                .with_acquire_timeout(Duration::from_millis(100)), // This should fail due to existing lock
        ];

        let result = shard.acquire_locks_batch(requests, true).await;
        assert!(result.is_err()); // Should fail due to obj1 being locked

        // Verify that obj2 lock was properly cleaned up (no resource leak)
        let obj2_key = ObjectKey::new("bucket", "obj2");
        assert!(shard.get_lock_info(&obj2_key).is_none(), "obj2 should not be locked after cleanup");

        // Verify obj1 is still locked by the original owner
        let obj1_key = ObjectKey::new("bucket", "obj1");
        let lock_info = shard.get_lock_info(&obj1_key);
        assert!(lock_info.is_some(), "obj1 should still be locked by blocking_owner");
    }

    #[tokio::test]
    async fn test_exclusive_waiter_abort_does_not_block_following_shared_lock() {
        let shard = Arc::new(LockShard::new(0));
        let key = ObjectKey::new("bucket", "abort-waiter-key");

        let owner1: Arc<str> = Arc::from("writer-owner-1");
        let owner2: Arc<str> = Arc::from("writer-owner-2");
        let reader_owner: Arc<str> = Arc::from("reader-owner");

        let hold_writer = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner1.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        assert!(shard.acquire_lock(&hold_writer).await.is_ok());

        let contended_writer = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner2.clone(),
            acquire_timeout: Duration::from_secs(5),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        let shard_for_waiter = shard.clone();
        let waiter_handle = tokio::spawn(async move { shard_for_waiter.acquire_lock(&contended_writer).await });

        // Ensure we actually enter slow-path wait registration before aborting.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(state) = shard.objects.read().get(&key).cloned()
                    && state.atomic_state.writers_waiting_count() > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for contended writer to register as waiting");
        waiter_handle.abort();
        let _ = waiter_handle.await;

        // The OptimizedNotify writer waiter counter must not leak after the
        // waiting future is dropped by the abort.
        if let Some(state) = shard.objects.read().get(&key).cloned() {
            assert_eq!(
                state
                    .optimized_notify
                    .writer_waiters
                    .load(std::sync::atomic::Ordering::Acquire),
                0,
                "optimized_notify writer waiter count must return to 0 after abort"
            );
        }

        assert!(shard.release_lock(&key, &owner1, LockMode::Exclusive));

        let followup_reader = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Shared,
            owner: reader_owner.clone(),
            acquire_timeout: Duration::from_millis(200),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        assert!(
            shard.acquire_lock(&followup_reader).await.is_ok(),
            "shared lock should succeed after writer waiter task is aborted"
        );
        assert!(shard.release_lock(&key, &reader_owner, LockMode::Shared));
    }

    #[tokio::test]
    async fn test_shared_waiter_abort_does_not_block_following_exclusive_lock() {
        let shard = Arc::new(LockShard::new(0));
        let key = ObjectKey::new("bucket", "abort-reader-waiter-key");

        let writer_owner: Arc<str> = Arc::from("writer-owner");
        let reader_owner: Arc<str> = Arc::from("reader-owner");
        let followup_owner: Arc<str> = Arc::from("followup-writer-owner");

        let hold_writer = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: writer_owner.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        assert!(shard.acquire_lock(&hold_writer).await.is_ok());

        let contended_reader = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Shared,
            owner: reader_owner.clone(),
            acquire_timeout: Duration::from_secs(5),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        let shard_for_waiter = shard.clone();
        let waiter_handle = tokio::spawn(async move { shard_for_waiter.acquire_lock(&contended_reader).await });

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(state) = shard.objects.read().get(&key).cloned()
                    && state.atomic_state.readers_waiting_count() > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for contended reader to register as waiting");
        waiter_handle.abort();
        let _ = waiter_handle.await;

        // The OptimizedNotify reader waiter counter must not leak after the
        // waiting future is dropped by the abort.
        if let Some(state) = shard.objects.read().get(&key).cloned() {
            assert_eq!(
                state
                    .optimized_notify
                    .reader_waiters
                    .load(std::sync::atomic::Ordering::Acquire),
                0,
                "optimized_notify reader waiter count must return to 0 after abort"
            );
        }

        assert!(shard.release_lock(&key, &writer_owner, LockMode::Exclusive));

        let followup_writer = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: followup_owner.clone(),
            acquire_timeout: Duration::from_millis(200),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        assert!(
            shard.acquire_lock(&followup_writer).await.is_ok(),
            "exclusive lock should succeed after reader waiter task is aborted"
        );
        assert!(shard.release_lock(&key, &followup_owner, LockMode::Exclusive));
    }

    #[tokio::test]
    async fn test_cleanup_expired_batch_recycles_into_pool() {
        let shard = LockShard::new(0);
        let owner: Arc<str> = Arc::from("owner");
        const N: usize = 8;

        // Populate the shard with idle (acquired then released) lock states.
        for i in 0..N {
            let key = ObjectKey::new("bucket", format!("obj-{i}"));
            let request = ObjectLockRequest {
                key: key.clone(),
                mode: LockMode::Exclusive,
                owner: owner.clone(),
                acquire_timeout: Duration::from_secs(1),
                lock_timeout: Duration::from_secs(30),
                priority: LockPriority::Normal,
            };
            assert!(shard.acquire_lock(&request).await.is_ok());
            assert!(shard.release_lock(&key, &owner, LockMode::Exclusive));
        }

        // The pool started empty, so every acquisition allocated a fresh state:
        // N misses and no releases yet.
        let (_hits, misses, releases_before, _pool_size) = shard.pool_stats();
        assert_eq!(misses, N as u64);
        assert_eq!(releases_before, 0);

        // Make sure the idle states are old enough to be evicted, then force a
        // batch cleanup with a zero idle threshold.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let cleaned = shard.cleanup_expired_batch(N, 0);
        assert_eq!(cleaned, N, "all idle states should be cleaned");
        assert_eq!(shard.lock_count(), 0, "cleaned entries must be removed from the shard");

        // The evicted states must actually be recycled back into the pool.
        let (_hits, _misses, releases_after, pool_size) = shard.pool_stats();
        assert_eq!(releases_after, N as u64, "cleanup must recycle evicted states into the pool");
        assert_eq!(pool_size, N, "recycled states must be available in the pool");

        // A subsequent acquire should reuse a pooled state, raising the hit rate.
        let key = ObjectKey::new("bucket", "reuse");
        let request = ObjectLockRequest {
            key,
            mode: LockMode::Exclusive,
            owner: owner.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };
        assert!(shard.acquire_lock(&request).await.is_ok());
        let (hits, _misses, _releases, _pool_size) = shard.pool_stats();
        assert!(hits >= 1, "subsequent acquire should hit the recycled pool");
        assert!(shard.pool_hit_rate() > 0.0, "pool hit rate should rise after recycling");
    }
}
