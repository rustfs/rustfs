// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded, same-session bucket mutation watches. The registry lock is a short
//! synchronous critical section shared by installation, mutation begin/end,
//! and observation. No I/O or await occurs while it is held.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const SCANNER_USAGE_WATCH_COVERAGE_VERSION: u32 = rustfs_protos::usage_watch::USAGE_WATCH_COVERAGE_VERSION;
pub const SCANNER_USAGE_WATCH_MAX_ENTRIES: usize = 256;
pub const SCANNER_USAGE_WATCH_MAX_ACTIVE_BUCKETS: usize = 4096;
pub const SCANNER_USAGE_WATCH_MAX_NAME_BYTES: usize = 16 * 1024;
pub const SCANNER_USAGE_WATCH_MAX_ACTIVE_NAME_BYTES: usize = 256 * 1024;
pub const SCANNER_USAGE_WATCH_LEASE_MS: u64 = rustfs_protos::usage_watch::USAGE_WATCH_MAX_LEASE_MS;

/// A missing watch ID installs a new watch; a supplied ID can only refresh the
/// exact, still-live token. An expired token never silently creates a new one.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScannerUsageWatchRequest {
    pub bucket: String,
    pub bucket_incarnation: Uuid,
    pub watch_id: Option<Uuid>,
    /// Release only this exact producer session and token after publication.
    pub release: bool,
    pub session_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScannerUsageWatchKnown {
    pub owner_id: Uuid,
    pub session_id: Uuid,
    pub watch_id: Uuid,
    pub bucket_incarnation: Uuid,
    pub revision: u64,
    pub inflight: u64,
    pub coverage_version: u32,
    pub coverage_epoch: u64,
    /// Duration from the receiver's observation, never a wall-clock deadline.
    pub lease_remaining_ms: u64,
}

impl ScannerUsageWatchKnown {
    pub fn is_well_formed(&self) -> bool {
        !self.owner_id.is_nil()
            && !self.session_id.is_nil()
            && !self.watch_id.is_nil()
            && !self.bucket_incarnation.is_nil()
            && self.revision != 0
            && self.revision != u64::MAX
            && self.inflight != u64::MAX
            && self.coverage_version == SCANNER_USAGE_WATCH_COVERAGE_VERSION
            && self.coverage_epoch != 0
            && self.coverage_epoch != u64::MAX
            && self.lease_remaining_ms != 0
            && self.lease_remaining_ms <= SCANNER_USAGE_WATCH_LEASE_MS
    }

    /// Equality of stable proof fields; the caller must also respect its
    /// conservative local lease deadline and validate the source/owner set.
    pub fn unchanged_since(&self, earlier: &Self) -> bool {
        self.is_well_formed()
            && earlier.is_well_formed()
            && self.owner_id == earlier.owner_id
            && self.session_id == earlier.session_id
            && self.watch_id == earlier.watch_id
            && self.bucket_incarnation == earlier.bucket_incarnation
            && self.revision == earlier.revision
            && self.inflight == 0
            && earlier.inflight == 0
            && self.coverage_version == SCANNER_USAGE_WATCH_COVERAGE_VERSION
            && self.coverage_version == earlier.coverage_version
            && self.coverage_epoch == earlier.coverage_epoch
            && self.lease_remaining_ms != 0
            && earlier.lease_remaining_ms != 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerUsageWatchObservation {
    pub bucket: String,
    /// Missing means Unknown. It is never a revision-zero clean value.
    pub known: Option<ScannerUsageWatchKnown>,
}

struct Watch {
    owner: Uuid,
    token: Uuid,
    incarnation: Uuid,
    revision: u64,
    expires_at: Instant,
}

#[derive(Default)]
struct State {
    watches: HashMap<Arc<str>, Watch>,
    active: HashMap<Arc<str>, u64>,
    watch_name_bytes: usize,
    active_name_bytes: usize,
    untracked: u64,
    exhausted: bool,
    coverage_epoch: u64,
}

pub(crate) struct UsageWatchRegistry {
    session: Uuid,
    state: Mutex<State>,
}

enum Mutation {
    Tracked(Arc<str>),
    Untracked,
}

/// The physical mutation owner retains this value through all detached tails.
pub(crate) struct UsageWatchMutationGuard {
    registry: Arc<UsageWatchRegistry>,
    mutation: Mutation,
}

impl Default for UsageWatchRegistry {
    fn default() -> Self {
        Self {
            session: Uuid::new_v4(),
            state: Mutex::new(State {
                coverage_epoch: 1,
                ..Default::default()
            }),
        }
    }
}

impl State {
    fn invalidate_all(&mut self) {
        self.watches.clear();
        self.watch_name_bytes = 0;
        match self.coverage_epoch.checked_add(1).filter(|epoch| *epoch != u64::MAX) {
            Some(epoch) => self.coverage_epoch = epoch,
            None => self.exhausted = true,
        }
    }

    fn advance_bucket(&mut self, bucket: &str) {
        if let Some(watch) = self.watches.get_mut(bucket) {
            if let Some(next) = watch.revision.checked_add(1).filter(|next| *next != u64::MAX) {
                watch.revision = next;
            } else {
                self.remove_watch(bucket);
            }
        }
    }

    fn remove_watch(&mut self, bucket: &str) {
        if let Some((name, _)) = self.watches.remove_entry(bucket) {
            self.watch_name_bytes -= name.len();
        }
    }
}

impl UsageWatchRegistry {
    pub(crate) fn session_id(&self) -> Uuid {
        self.session
    }
    pub(crate) fn begin(self: &Arc<Self>, bucket: Option<&str>) -> UsageWatchMutationGuard {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Internal metadata is outside the user bucket counters. Callers with
        // an unknown user scope must pass None, which invalidates every watch.
        let bucket = bucket.filter(|bucket| !bucket.is_empty() && bucket.len() <= 63);
        let tracked = bucket.and_then(|bucket| {
            if let Some((key, count)) = state.active.get_key_value(bucket) {
                let key = Arc::clone(key);
                let next = count.checked_add(1)?;
                state.active.insert(Arc::clone(&key), next);
                return Some(key);
            }
            if state.active.len() >= SCANNER_USAGE_WATCH_MAX_ACTIVE_BUCKETS
                || state.active_name_bytes + bucket.len() > SCANNER_USAGE_WATCH_MAX_ACTIVE_NAME_BYTES
            {
                return None;
            }
            let key: Arc<str> = Arc::from(bucket);
            state.active_name_bytes += bucket.len();
            state.active.insert(Arc::clone(&key), 1);
            Some(key)
        });
        let mutation = match tracked {
            Some(bucket) => {
                state.advance_bucket(&bucket);
                Mutation::Tracked(bucket)
            }
            None => {
                state.invalidate_all();
                match state.untracked.checked_add(1) {
                    Some(count) => state.untracked = count,
                    None => state.exhausted = true,
                }
                Mutation::Untracked
            }
        };
        UsageWatchMutationGuard {
            registry: Arc::clone(self),
            mutation,
        }
    }

    /// Replay and unsupported runtime mode transitions revoke tokens. They
    /// never copy a durable generation into a same-session watch revision.
    pub(crate) fn invalidate(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .invalidate_all();
    }

    /// A write mode without a physical completion owner cannot establish
    /// same-session coverage again. Keep the global fallback until restart.
    pub(crate) fn mark_coverage_unavailable(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.exhausted = true;
        state.invalidate_all();
    }

    pub(crate) fn observe(&self, owner: Uuid, request: &ScannerUsageWatchRequest) -> Option<ScannerUsageWatchKnown> {
        self.observe_at(owner, request, Instant::now())
    }

    fn observe_at(&self, owner: Uuid, request: &ScannerUsageWatchRequest, now: Instant) -> Option<ScannerUsageWatchKnown> {
        if owner.is_nil() || request.bucket_incarnation.is_nil() || request.bucket.is_empty() || request.bucket.len() > 63 {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if request.release {
            if request.session_id == Some(self.session)
                && state.watches.get(request.bucket.as_str()).is_some_and(|watch| {
                    watch.owner == owner
                        && watch.incarnation == request.bucket_incarnation
                        && request.watch_id == Some(watch.token)
                })
            {
                state.remove_watch(&request.bucket);
            }
            // Release is resource retirement, never an assertion of cleanliness.
            return None;
        }
        if request.session_id.is_some_and(|session| session != self.session) {
            return None;
        }
        if state.exhausted || state.untracked != 0 {
            return None;
        }
        // Sweeps are bounded by the fixed watch limit. Never accumulate tombstones.
        state.watches.retain(|_, watch| watch.expires_at > now);
        state.watch_name_bytes = state.watches.keys().map(|key| key.len()).sum();
        let matches = state.watches.get(request.bucket.as_str()).is_some_and(|watch| {
            watch.owner == owner
                && watch.incarnation == request.bucket_incarnation
                && request.watch_id.is_none_or(|id| id == watch.token)
        });
        if !matches {
            if request.watch_id.is_some() {
                return None;
            }
            state.remove_watch(&request.bucket);
            if state.watches.len() >= SCANNER_USAGE_WATCH_MAX_ENTRIES
                || state.watch_name_bytes + request.bucket.len() > SCANNER_USAGE_WATCH_MAX_NAME_BYTES
            {
                return None;
            }
            let key: Arc<str> = Arc::from(request.bucket.as_str());
            state.watch_name_bytes += key.len();
            state.watches.insert(
                key,
                Watch {
                    owner,
                    token: Uuid::new_v4(),
                    incarnation: request.bucket_incarnation,
                    revision: 1,
                    expires_at: now + Duration::from_millis(SCANNER_USAGE_WATCH_LEASE_MS),
                },
            );
        }
        let inflight = state.active.get(request.bucket.as_str()).copied().unwrap_or(0);
        let coverage_epoch = state.coverage_epoch;
        let watch = state.watches.get_mut(request.bucket.as_str())?;
        watch.expires_at = now + Duration::from_millis(SCANNER_USAGE_WATCH_LEASE_MS);
        Some(ScannerUsageWatchKnown {
            owner_id: owner,
            session_id: self.session,
            watch_id: watch.token,
            bucket_incarnation: watch.incarnation,
            revision: watch.revision,
            inflight,
            coverage_version: SCANNER_USAGE_WATCH_COVERAGE_VERSION,
            coverage_epoch,
            lease_remaining_ms: SCANNER_USAGE_WATCH_LEASE_MS,
        })
    }
}

impl Drop for UsageWatchMutationGuard {
    fn drop(&mut self) {
        let mut state = self.registry.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match &self.mutation {
            Mutation::Tracked(bucket) => {
                // Advance before exposing inflight=0, including canceled and
                // failed operations whose physical outcome may be uncertain.
                state.advance_bucket(bucket);
                if let Some(count) = state.active.get_mut(bucket) {
                    *count -= 1;
                    if *count == 0 {
                        state.active.remove(bucket);
                        state.active_name_bytes -= bucket.len();
                    }
                } else {
                    state.exhausted = true;
                    state.invalidate_all();
                }
            }
            Mutation::Untracked => {
                if state.untracked != 0 {
                    state.untracked -= 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
