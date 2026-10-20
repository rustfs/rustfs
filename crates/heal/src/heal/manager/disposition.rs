// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Node-local memory of terminal per-object heal dispositions.
//!
//! The queue releases its dedup key when a request leaves the queue, so a
//! best-effort producer (scanner, read repair, autoheal, MRF hints) can
//! resubmit the same object as soon as its task finished, and a task that
//! found nothing to repair is forgotten immediately. This cache remembers the
//! last terminal verdict per `(bucket, object, version, pool, set)` so admission can
//! answer a resubmission without queueing it again:
//!
//! - `Absent`: a locked quorum read classified the object as absent
//!   ([`crate::error::HealErrorClass::Absent`]). Resubmissions are dropped
//!   until the entry expires.
//! - `RetryExhausted` / `Permanent`: the task ended in a terminal failure.
//!   Resubmissions are admitted again on an exponential schedule that is
//!   capped at one hour, so a failing object keeps being retried but no longer
//!   on every producer tick.
//!
//! The cache is bounded (least recently used eviction) and every entry
//! expires. It is process memory: a restart starts empty, and nothing here is
//! a receipt. It never releases a durable responsibility; durable MRF repairs
//! bypass it entirely, as do administrator starts, forced starts, high and
//! urgent priorities, and dry runs.

use crate::error::{HealErrorClass, classify_heal_error};
use crate::heal::outcome::HealObjectDisposition;
use crate::heal::task::{HealTask, HealType};
use metrics::{counter, gauge};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Duration;
use tokio::time::Instant;

/// Default upper bound on retained entries.
pub(crate) const DEFAULT_DISPOSITION_CACHE_CAPACITY: usize = 100_000;
/// Default lifetime of an entry after its last record.
pub(crate) const DEFAULT_DISPOSITION_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// First re-admission delay after a terminal failure; doubles per repeat.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(60);
/// Upper bound of the re-admission delay.
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealDispositionKind {
    Absent,
    RetryExhausted,
    Permanent,
}

impl HealDispositionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::RetryExhausted => "retry_exhausted",
            Self::Permanent => "permanent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct HealDispositionKey {
    pub(crate) bucket: String,
    pub(crate) object: String,
    /// `None` is the unversioned/latest target; an empty wire string is the same thing.
    pub(crate) version_id: Option<String>,
    pub(crate) pool_index: Option<usize>,
    pub(crate) set_index: Option<usize>,
}

impl HealDispositionKey {
    pub(crate) fn new(
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        pool_index: Option<usize>,
        set_index: Option<usize>,
    ) -> Self {
        Self {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            version_id: version_id.filter(|version| !version.is_empty()).map(ToOwned::to_owned),
            pool_index,
            set_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HealDispositionEntry {
    pub(crate) kind: HealDispositionKind,
    /// Consecutive records of the same kind for this key.
    pub(crate) count: u32,
    pub(crate) recorded_at: Instant,
    /// Task that produced the latest record; returned to producers as the receipt pointer.
    pub(crate) task_id: String,
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HealDispositionVerdict {
    Admit,
    /// The last locked quorum read found the object absent and the memory is still fresh.
    Absent {
        task_id: String,
        count: u32,
    },
    /// The last task failed terminally; re-admission waits for the backoff to elapse.
    BackedOff {
        kind: HealDispositionKind,
        task_id: String,
        count: u32,
        retry_after: Duration,
    },
}

impl HealDispositionVerdict {
    pub(crate) fn rejection(&self) -> Option<(HealDispositionKind, &str, u32, Option<Duration>)> {
        match self {
            Self::Admit => None,
            Self::Absent { task_id, count } => Some((HealDispositionKind::Absent, task_id, *count, None)),
            Self::BackedOff {
                kind,
                task_id,
                count,
                retry_after,
            } => Some((*kind, task_id, *count, Some(*retry_after))),
        }
    }
}

pub(crate) struct HealDispositionCache {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<HealDispositionKey, HealDispositionEntry>,
    /// Recency order. A key can appear several times; only the pair whose
    /// sequence matches the entry is live, older pairs are skipped on eviction.
    recency: VecDeque<(u64, HealDispositionKey)>,
    next_sequence: u64,
}

impl Default for HealDispositionCache {
    fn default() -> Self {
        Self::with_limits(DEFAULT_DISPOSITION_CACHE_CAPACITY, DEFAULT_DISPOSITION_CACHE_TTL)
    }
}

impl HealDispositionCache {
    pub(crate) fn with_limits(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity: capacity.max(1),
            ttl,
            entries: HashMap::new(),
            recency: VecDeque::new(),
            next_sequence: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &HealDispositionKey) -> Option<&HealDispositionEntry> {
        self.entries.get(key)
    }

    /// Delay before a key with `count` consecutive terminal failures is admitted again.
    pub(crate) fn retry_backoff(count: u32) -> Duration {
        let exponent = count.saturating_sub(1).min(31);
        RETRY_BACKOFF_BASE.saturating_mul(1_u32 << exponent).min(RETRY_BACKOFF_MAX)
    }

    /// Decide whether a resubmission for `key` may enter the queue at `now`.
    pub(crate) fn consult(&mut self, key: &HealDispositionKey, now: Instant) -> HealDispositionVerdict {
        let Some(entry) = self.entries.get(key) else {
            return HealDispositionVerdict::Admit;
        };
        let age = now.saturating_duration_since(entry.recorded_at);
        if age >= self.ttl {
            self.entries.remove(key);
            self.publish_entries();
            return HealDispositionVerdict::Admit;
        }
        let verdict = match entry.kind {
            HealDispositionKind::Absent => HealDispositionVerdict::Absent {
                task_id: entry.task_id.clone(),
                count: entry.count,
            },
            kind @ (HealDispositionKind::RetryExhausted | HealDispositionKind::Permanent) => {
                let backoff = Self::retry_backoff(entry.count);
                if age >= backoff {
                    return HealDispositionVerdict::Admit;
                }
                HealDispositionVerdict::BackedOff {
                    kind,
                    task_id: entry.task_id.clone(),
                    count: entry.count,
                    retry_after: backoff.saturating_sub(age),
                }
            }
        };
        counter!("rustfs_heal_disposition_cache_hits_total", "disposition" => entry.kind.as_str()).increment(1);
        self.touch(key);
        verdict
    }

    /// Remember the terminal verdict of `task_id` for `key`.
    ///
    /// A repeat of the same kind increments `count`; a different kind starts
    /// a fresh count, because the backoff schedule belongs to one failure mode.
    pub(crate) fn record(&mut self, key: HealDispositionKey, kind: HealDispositionKind, task_id: &str, now: Instant) {
        let sequence = self.next_sequence();
        let count = match self.entries.get(&key) {
            Some(existing) if existing.kind == kind && now.saturating_duration_since(existing.recorded_at) < self.ttl => {
                existing.count.saturating_add(1)
            }
            _ => 1,
        };
        self.entries.insert(
            key.clone(),
            HealDispositionEntry {
                kind,
                count,
                recorded_at: now,
                task_id: task_id.to_owned(),
                sequence,
            },
        );
        self.recency.push_back((sequence, key));
        self.evict_to_capacity();
        self.compact_recency();
        self.publish_entries();
    }

    /// Forget `key` after a task completed with a positive storage result.
    pub(crate) fn clear(&mut self, key: &HealDispositionKey) {
        if self.entries.remove(key).is_some() {
            self.publish_entries();
        }
    }

    fn next_sequence(&mut self) -> u64 {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.next_sequence
    }

    fn touch(&mut self, key: &HealDispositionKey) {
        let sequence = self.next_sequence();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.sequence = sequence;
            self.recency.push_back((sequence, key.clone()));
            self.compact_recency();
        }
    }

    fn evict_to_capacity(&mut self) {
        while self.entries.len() > self.capacity {
            let Some((sequence, key)) = self.recency.pop_front() else {
                // Every live entry has a recency pair; an empty queue with
                // entries left cannot happen, but never spin if it does.
                break;
            };
            if self.entries.get(&key).is_some_and(|entry| entry.sequence == sequence) {
                self.entries.remove(&key);
            }
        }
    }

    fn compact_recency(&mut self) {
        if self.recency.len() > self.capacity.saturating_mul(2).saturating_add(16) {
            let entries = &self.entries;
            self.recency
                .retain(|(sequence, key)| entries.get(key).is_some_and(|entry| entry.sequence == *sequence));
        }
    }

    fn publish_entries(&self) {
        gauge!("rustfs_heal_disposition_cache_entries").set(self.entries.len() as f64);
    }
}

pub(super) fn lock_disposition_cache(cache: &StdMutex<HealDispositionCache>) -> StdMutexGuard<'_, HealDispositionCache> {
    match cache.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Remember the terminal verdict of a finished single-object task, or forget
/// the key after a positive completion. Scheduled retries, cancellations,
/// deadlines, dry runs and durable MRF repairs leave the memory untouched.
pub(crate) async fn record_task_disposition(
    cache: &StdMutex<HealDispositionCache>,
    task: &HealTask,
    result: &crate::Result<()>,
    will_retry: bool,
) {
    let HealType::Object {
        bucket,
        object,
        version_id,
    } = &task.heal_type
    else {
        return;
    };
    if task.options.dry_run || task.expected_mrf_bucket_incarnation_id.is_some() {
        return;
    }
    let key = HealDispositionKey::new(bucket, object, version_id.as_deref(), task.options.pool_index, task.options.set_index);
    let kind = match result {
        Ok(()) if task.object_absent() => HealDispositionKind::Absent,
        Ok(()) => {
            // A deferral (grace window, transient skip) is neither a repair
            // nor a failure; only a positive completion forgets the key.
            let deferred = task
                .get_outcome()
                .await
                .objects
                .iter()
                .any(|object| matches!(object.disposition, HealObjectDisposition::Deferred { .. }));
            if !deferred {
                lock_disposition_cache(cache).clear(&key);
            }
            return;
        }
        Err(_) if will_retry => return,
        Err(error) => match classify_heal_error(error) {
            HealErrorClass::Aborted => return,
            HealErrorClass::Absent => HealDispositionKind::Absent,
            HealErrorClass::NoHealRequired => {
                lock_disposition_cache(cache).clear(&key);
                return;
            }
            HealErrorClass::Transient | HealErrorClass::LockContention | HealErrorClass::DanglingGrace { .. } => {
                HealDispositionKind::RetryExhausted
            }
            HealErrorClass::Permanent => HealDispositionKind::Permanent,
        },
    };
    lock_disposition_cache(cache).record(key, kind, &task.id, Instant::now());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(object: &str) -> HealDispositionKey {
        HealDispositionKey::new("bucket", object, None, None, None)
    }

    #[test]
    fn disposition_cache_absent_hit_until_ttl_expires() {
        let mut cache = HealDispositionCache::with_limits(10, Duration::from_secs(60));
        let now = Instant::now();
        cache.record(key("gone"), HealDispositionKind::Absent, "task-1", now);
        assert_eq!(
            cache.consult(&key("gone"), now + Duration::from_secs(59)),
            HealDispositionVerdict::Absent {
                task_id: "task-1".into(),
                count: 1
            }
        );
        assert_eq!(cache.consult(&key("gone"), now + Duration::from_secs(60)), HealDispositionVerdict::Admit);
        assert_eq!(cache.len(), 0, "an expired entry is dropped on consult");
        assert_eq!(cache.consult(&key("other"), now), HealDispositionVerdict::Admit);
    }

    #[test]
    fn disposition_cache_retry_exhausted_backs_off_exponentially_up_to_one_hour() {
        let mut cache = HealDispositionCache::with_limits(10, Duration::from_secs(24 * 60 * 60));
        let now = Instant::now();
        cache.record(key("flaky"), HealDispositionKind::RetryExhausted, "task-1", now);
        // The fourth submission (first after the task's own three retries) is backed off.
        assert_eq!(
            cache.consult(&key("flaky"), now + Duration::from_secs(30)),
            HealDispositionVerdict::BackedOff {
                kind: HealDispositionKind::RetryExhausted,
                task_id: "task-1".into(),
                count: 1,
                retry_after: Duration::from_secs(30),
            }
        );
        assert_eq!(cache.consult(&key("flaky"), now + Duration::from_secs(60)), HealDispositionVerdict::Admit);
        let later = now + Duration::from_secs(61);
        cache.record(key("flaky"), HealDispositionKind::RetryExhausted, "task-2", later);
        assert_eq!(cache.get(&key("flaky")).map(|entry| entry.count), Some(2));
        assert!(matches!(
            cache.consult(&key("flaky"), later + Duration::from_secs(119)),
            HealDispositionVerdict::BackedOff { count: 2, .. }
        ));
        assert_eq!(
            cache.consult(&key("flaky"), later + Duration::from_secs(120)),
            HealDispositionVerdict::Admit
        );
        for count in 1..=40 {
            assert!(HealDispositionCache::retry_backoff(count) <= Duration::from_secs(3600), "count={count}");
        }
        assert_eq!(HealDispositionCache::retry_backoff(6), Duration::from_secs(1920));
        assert_eq!(HealDispositionCache::retry_backoff(7), Duration::from_secs(3600));
        assert_eq!(HealDispositionCache::retry_backoff(u32::MAX), Duration::from_secs(3600));
        // A different terminal kind starts its own count.
        cache.record(key("flaky"), HealDispositionKind::Permanent, "task-3", later);
        assert_eq!(cache.get(&key("flaky")).map(|entry| entry.count), Some(1));
    }

    #[test]
    fn disposition_cache_evicts_least_recently_used_when_full() {
        let mut cache = HealDispositionCache::with_limits(3, Duration::from_secs(600));
        let now = Instant::now();
        for object in ["a", "b", "c"] {
            cache.record(key(object), HealDispositionKind::Absent, "task", now);
        }
        // Touch "a" so "b" becomes the oldest.
        assert!(matches!(cache.consult(&key("a"), now), HealDispositionVerdict::Absent { .. }));
        cache.record(key("d"), HealDispositionKind::Absent, "task", now);
        assert_eq!(cache.len(), 3);
        assert!(cache.get(&key("b")).is_none(), "the oldest entry is evicted, not the new one");
        for object in ["a", "c", "d"] {
            assert!(cache.get(&key(object)).is_some(), "{object}");
        }
        // Stale recency pairs never evict a re-recorded key by mistake.
        cache.record(key("a"), HealDispositionKind::Absent, "task", now);
        cache.record(key("e"), HealDispositionKind::Absent, "task", now);
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("c")).is_none());
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn disposition_cache_success_clears_the_entry() {
        let mut cache = HealDispositionCache::with_limits(10, Duration::from_secs(600));
        let now = Instant::now();
        cache.record(key("fixed"), HealDispositionKind::Permanent, "task-1", now);
        cache.clear(&key("fixed"));
        assert_eq!(cache.consult(&key("fixed"), now), HealDispositionVerdict::Admit);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn disposition_cache_recency_queue_stays_bounded_under_repeated_hits() {
        let mut cache = HealDispositionCache::with_limits(4, Duration::from_secs(600));
        let now = Instant::now();
        cache.record(key("hot"), HealDispositionKind::Absent, "task", now);
        for _ in 0..10_000 {
            assert!(matches!(cache.consult(&key("hot"), now), HealDispositionVerdict::Absent { .. }));
        }
        assert!(cache.recency.len() <= 4 * 2 + 16 + 1, "recency={}", cache.recency.len());
    }

    #[test]
    fn disposition_key_normalizes_empty_version() {
        assert_eq!(
            HealDispositionKey::new("b", "o", Some(""), None, None),
            HealDispositionKey::new("b", "o", None, None, None)
        );
        assert_ne!(
            HealDispositionKey::new("b", "o", Some("v1"), None, None),
            HealDispositionKey::new("b", "o", None, None, None)
        );
    }
}
