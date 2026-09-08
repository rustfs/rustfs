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
/// process-wide dirty-usage invalidation state, its acknowledgment protocol, and snapshot helpers.
use super::*;

pub(super) static DIRTY_USAGE_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(super) static DIRTY_USAGE_BUCKETS: LazyLock<StdMutex<DirtyUsageBuckets>> = LazyLock::new(|| StdMutex::new(HashMap::new()));
// Lock order when both dirty maps are needed is `DIRTY_USAGE_BUCKETS` followed
// by `DIRTY_USAGE_BUCKET_SCOPES`. Both are held only for synchronous map
// updates, so no scanner task can observe a bucket generation without its
// matching scope.
pub(super) static DIRTY_USAGE_BUCKET_SCOPES: LazyLock<StdMutex<DirtyUsageBucketScopes>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
pub(super) static DIRTY_USAGE_BUCKET_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
pub(super) static SCANNER_ACTIVITY_EPOCH: LazyLock<String> = LazyLock::new(|| format!("{:032x}", rand::random::<u128>()));
pub(super) static SCANNER_MAINTENANCE_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(super) static SCANNER_MAINTENANCE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageState {
    pub generation: u64,
    pub pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageBucket {
    pub bucket: String,
    pub generation: u64,
}

/// A non-durable optimization hint for a dirty bucket.
///
/// A whole-bucket marker always wins over narrow path hints. The scanner never
/// publishes a prefix-only result as authoritative usage; this only controls
/// whether a complete per-bucket cache can reuse known-clean direct children.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DirtyUsageBucketScope {
    WholeBucket,
    TopLevelEntries(HashSet<String>),
}

pub(super) type DirtyUsageBucketScopes = HashMap<String, DirtyUsageBucketScope>;

const MAX_DIRTY_USAGE_TOP_LEVEL_ENTRIES_PER_BUCKET: usize = 128;

/// A point-in-time view of the local dirty bucket generations.
///
/// `complete == false` is an all-or-nothing overflow signal: `buckets` is
/// empty and callers must fall back to the global dirty generation rather than
/// treating a bounded prefix as authoritative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageSnapshot {
    pub generation: u64,
    pub pending_bucket_count: u64,
    pub complete: bool,
    pub buckets: Vec<ScannerDirtyUsageBucket>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScannerDirtyUsageAckError {
    #[error("scanner process instance changed before dirty usage acknowledgement")]
    ProcessChanged,
    #[error("scanner dirty usage generation cannot be acknowledged")]
    InvalidGeneration,
    #[error("scanner dirty usage bucket incarnation fence is unavailable")]
    IncarnationUnavailable,
}

/// A scoped ACK requires storage-owned lifecycle and incarnation fences.
/// Callers must only send ACKs backed by durable per-bucket publication.
pub fn acknowledge_scoped_dirty_usage(
    instance_id: &str,
    entries: &[(&crate::storage_api::EcstoreBucketMetadataMutationGuard, u64)],
    probe_only: bool,
) -> std::result::Result<u64, ScannerDirtyUsageAckError> {
    // Lock order: sorted bucket lifecycle/metadata fences (caller), then dirty map.
    // No await or storage operation occurs while the dirty map is locked.
    let (cleared, pending) = {
        let mut dirty = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let checked = entries
            .iter()
            .map(|(guard, generation)| {
                guard
                    .checked_bucket_incarnation()
                    .map(|(bucket, _)| (bucket, *generation))
                    .map_err(|_| ScannerDirtyUsageAckError::IncarnationUnavailable)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let cleared = apply_scoped_dirty_usage_ack(
            instance_id,
            scanner_activity_epoch(),
            DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire),
            &mut dirty,
            &mut dirty_scopes,
            &checked,
            probe_only,
        )?;
        if cleared > 0 {
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared, dirty.len())
    };
    if !probe_only {
        global_metrics().record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared), usize_to_u64_saturated(pending));
    }
    Ok(usize_to_u64_saturated(cleared))
}

fn apply_scoped_dirty_usage_ack(
    instance_id: &str,
    current_instance: &str,
    current_generation: u64,
    dirty: &mut DirtyUsageBuckets,
    dirty_scopes: &mut DirtyUsageBucketScopes,
    entries: &[(&str, u64)],
    probe_only: bool,
) -> std::result::Result<usize, ScannerDirtyUsageAckError> {
    if instance_id != current_instance {
        return Err(ScannerDirtyUsageAckError::ProcessChanged);
    }
    if current_generation == u64::MAX
        || entries
            .iter()
            .any(|(_, generation)| *generation == 0 || *generation == u64::MAX || *generation > current_generation)
    {
        return Err(ScannerDirtyUsageAckError::InvalidGeneration);
    }
    let mut cleared = 0;
    if !probe_only {
        for (bucket, generation) in entries {
            if dirty.get(*bucket) == Some(generation) {
                dirty.remove(*bucket);
                dirty_scopes.remove(*bucket);
                cleared += 1;
            }
        }
    }
    Ok(cleared)
}

#[cfg(test)]
mod scoped_dirty_usage_tests {
    use super::*;

    #[test]
    fn scoped_dirty_usage_preserves_uncovered_newer_and_replayed_generations() {
        let mut dirty = HashMap::from([("hot".to_string(), 7), ("cold".to_string(), 8)]);
        let mut scopes = HashMap::from([
            ("hot".to_string(), DirtyUsageBucketScope::WholeBucket),
            (
                "cold".to_string(),
                DirtyUsageBucketScope::TopLevelEntries(HashSet::from(["first".to_string()])),
            ),
        ]);
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], true),
            Ok(0)
        );
        assert_eq!(dirty.len(), 2);
        assert!(scopes.contains_key("cold"));
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(1)
        );
        assert_eq!(dirty.get("hot"), Some(&7));
        assert!(!scopes.contains_key("cold"));
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(0)
        );
        dirty.insert("cold".to_string(), 9);
        scopes.insert("cold".to_string(), DirtyUsageBucketScope::WholeBucket);
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 9, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(0)
        );
        assert_eq!(dirty.get("cold"), Some(&9));
        assert!(scopes.contains_key("cold"));
    }

    #[test]
    fn scoped_dirty_usage_rejects_restart_and_invalid_batch_before_clearing() {
        let original = HashMap::from([("hot".to_string(), 7), ("cold".to_string(), 8)]);
        let mut dirty = original.clone();
        let original_scopes = HashMap::from([
            ("hot".to_string(), DirtyUsageBucketScope::WholeBucket),
            ("cold".to_string(), DirtyUsageBucketScope::WholeBucket),
        ]);
        let mut scopes = original_scopes.clone();
        assert_eq!(
            apply_scoped_dirty_usage_ack("old", "new", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Err(ScannerDirtyUsageAckError::ProcessChanged)
        );
        assert_eq!(scopes, original_scopes);
        for generation in [0, 9, u64::MAX] {
            assert_eq!(
                apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8), ("hot", generation)], false,),
                Err(ScannerDirtyUsageAckError::InvalidGeneration)
            );
            assert_eq!(dirty, original);
            assert_eq!(scopes, original_scopes);
        }
    }
}

pub(super) fn dirty_usage_buckets() -> MutexGuard<'static, DirtyUsageBuckets> {
    DIRTY_USAGE_BUCKETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn dirty_usage_bucket_scopes() -> MutexGuard<'static, DirtyUsageBucketScopes> {
    DIRTY_USAGE_BUCKET_SCOPES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn advance_generation(generation: &AtomicU64) -> u64 {
    generation
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(1)))
        .map_or_else(|current| current, |previous| previous.saturating_add(1))
}

pub fn record_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        dirty_scopes.insert(bucket.to_string(), DirtyUsageBucketScope::WholeBucket);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    // A write invalidates this bucket's prefix-usage answers on the spot so
    // admin/console consumers never ride the full TTL after a change
    // (rustfs/backlog#1872).
    crate::prefix_usage::invalidate_prefix_usage_cache(bucket);
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

/// Record a mutation whose affected top-level namespace entry is known.
///
/// Object names that cannot be represented as one safe direct child retain the
/// conservative whole-bucket marker. This journal is intentionally process
/// local: after restart or any unverified distributed path the scanner falls
/// back to its ordinary bucket scan.
pub fn record_dirty_usage_object(bucket: &str, object: &str) {
    let Some(top_level_entry) = dirty_usage_top_level_entry(object) else {
        record_dirty_usage_bucket(bucket);
        return;
    };
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        let scope = dirty_scopes
            .entry(bucket.to_string())
            .or_insert_with(|| DirtyUsageBucketScope::TopLevelEntries(HashSet::new()));
        let overflowed = match scope {
            DirtyUsageBucketScope::WholeBucket => false,
            DirtyUsageBucketScope::TopLevelEntries(entries) => {
                entries.insert(top_level_entry);
                entries.len() > MAX_DIRTY_USAGE_TOP_LEVEL_ENTRIES_PER_BUCKET
            }
        };
        if overflowed {
            *scope = DirtyUsageBucketScope::WholeBucket;
        }
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    crate::prefix_usage::invalidate_prefix_usage_cache(bucket);
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

fn dirty_usage_top_level_entry(object: &str) -> Option<String> {
    let (top_level_entry, _) = object.split_once('/').unwrap_or((object, ""));
    (!top_level_entry.is_empty()
        && top_level_entry != "."
        && top_level_entry != ".."
        && !object.starts_with('/')
        && !top_level_entry.contains(['\\', '\0']))
    .then(|| top_level_entry.to_string())
}

pub fn record_scanner_maintenance_change(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    advance_generation(&SCANNER_MAINTENANCE_GENERATION);
    SCANNER_MAINTENANCE_NOTIFY.notify_one();
    record_dirty_usage_bucket(bucket);
}

pub fn scanner_maintenance_generation() -> u64 {
    SCANNER_MAINTENANCE_GENERATION.load(Ordering::Acquire)
}

pub(crate) async fn scanner_maintenance_changed() {
    SCANNER_MAINTENANCE_NOTIFY.notified().await;
}

pub fn scanner_activity_epoch() -> &'static str {
    SCANNER_ACTIVITY_EPOCH.as_str()
}

pub fn scanner_dirty_usage_state() -> ScannerDirtyUsageState {
    let dirty_buckets = dirty_usage_buckets();
    ScannerDirtyUsageState {
        generation: DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire),
        pending: !dirty_buckets.is_empty(),
    }
}

pub fn scanner_dirty_usage_snapshot(max_entries: usize) -> ScannerDirtyUsageSnapshot {
    let (generation, pending_bucket_count, complete, mut buckets) = {
        let dirty_buckets = dirty_usage_buckets();
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let pending_bucket_count = usize_to_u64_saturated(dirty_buckets.len());
        let complete = dirty_buckets.len() <= max_entries;
        let buckets = if complete {
            dirty_buckets
                .iter()
                .map(|(bucket, generation)| ScannerDirtyUsageBucket {
                    bucket: bucket.clone(),
                    generation: *generation,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        (generation, pending_bucket_count, complete, buckets)
    };
    buckets.sort_unstable_by(|left, right| left.bucket.cmp(&right.bucket));

    ScannerDirtyUsageSnapshot {
        generation,
        pending_bucket_count,
        complete,
        buckets,
    }
}

pub fn acknowledge_dirty_usage_generation(
    instance_id: &str,
    generation: u64,
) -> std::result::Result<(), ScannerDirtyUsageAckError> {
    if instance_id != scanner_activity_epoch() {
        return Err(ScannerDirtyUsageAckError::ProcessChanged);
    }

    let (cleared_buckets, pending_buckets) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let current_generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        if generation == 0 || generation == u64::MAX || current_generation == u64::MAX || generation > current_generation {
            return Err(ScannerDirtyUsageAckError::InvalidGeneration);
        }

        let before = dirty_buckets.len();
        dirty_buckets.retain(|_, dirty_generation| *dirty_generation > generation);
        dirty_scopes.retain(|bucket, _| dirty_buckets.contains_key(bucket));
        let cleared_buckets = before.saturating_sub(dirty_buckets.len());
        if cleared_buckets > 0 {
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared_buckets, dirty_buckets.len())
    };
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
    Ok(())
}

pub(crate) fn dirty_usage_generation() -> u64 {
    DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire)
}

pub fn clear_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        dirty_buckets.remove(bucket);
        dirty_scopes.remove(bucket);
        advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_clear(usize_to_u64_saturated(pending_buckets));
}

pub(super) fn snapshot_dirty_usage_buckets(buckets: &[BucketInfo], absent_generation_cutoff: u64) -> DirtyUsageSnapshot {
    let (snapshot, scopes, generation, covers_all_pending) = {
        let dirty_buckets = dirty_usage_buckets();
        let dirty_scopes = dirty_usage_bucket_scopes();
        let listed_buckets = dirty_buckets
            .values()
            .any(|generation| *generation > absent_generation_cutoff)
            .then(|| buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>());
        let snapshot = dirty_buckets
            .iter()
            .filter(|(bucket, generation)| {
                **generation <= absent_generation_cutoff
                    || listed_buckets
                        .as_ref()
                        .is_some_and(|listed_buckets| listed_buckets.contains(bucket.as_str()))
            })
            .map(|(bucket, generation)| (bucket.clone(), *generation))
            .collect::<DirtyUsageBuckets>();
        let scopes = snapshot
            .keys()
            .map(|bucket| {
                (
                    bucket.clone(),
                    dirty_scopes
                        .get(bucket)
                        .cloned()
                        .unwrap_or(DirtyUsageBucketScope::WholeBucket),
                )
            })
            .collect::<DirtyUsageBucketScopes>();
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let covers_all_pending = generation == absent_generation_cutoff && snapshot.len() == dirty_buckets.len();
        (snapshot, scopes, generation, covers_all_pending)
    };
    global_metrics().record_scanner_dirty_usage_cycle_snapshot(usize_to_u64_saturated(snapshot.len()));
    DirtyUsageSnapshot {
        buckets: Arc::new(snapshot),
        scopes: Arc::new(scopes),
        generation,
        covers_all_pending,
    }
}

pub(crate) fn dirty_usage_buckets_pending() -> bool {
    !dirty_usage_buckets().is_empty()
}

pub(crate) async fn dirty_usage_bucket_notified() {
    DIRTY_USAGE_BUCKET_NOTIFY.notified().await;
}

pub(super) fn clear_dirty_usage_buckets(snapshot: &DirtyUsageBuckets) {
    let (cleared_buckets, pending_buckets) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut cleared_buckets = 0usize;
        for (bucket, generation) in snapshot {
            if dirty_buckets.get(bucket).is_some_and(|current| current == generation) {
                dirty_buckets.remove(bucket);
                dirty_scopes.remove(bucket);
                cleared_buckets += 1;
            }
        }
        if cleared_buckets > 0 {
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared_buckets, dirty_buckets.len())
    };
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
}

pub(super) fn dirty_usage_buckets_excluding_failed(
    snapshot: &DirtyUsageBuckets,
    failed_buckets: &HashSet<String>,
) -> DirtyUsageBuckets {
    snapshot
        .iter()
        .filter(|(bucket, _)| !failed_buckets.contains(*bucket))
        .map(|(bucket, generation)| (bucket.clone(), *generation))
        .collect()
}

pub(super) fn should_clear_dirty_usage_snapshot(
    result_ok: bool,
    completed_all_sets: bool,
    budget_elapsed: bool,
    activity_and_generation_current: bool,
    dirty_buckets: &DirtyUsageBuckets,
    failed_buckets: &HashSet<String>,
) -> Option<DirtyUsageBuckets> {
    if result_ok && completed_all_sets && !budget_elapsed && activity_and_generation_current {
        return Some(dirty_usage_buckets_excluding_failed(dirty_buckets, failed_buckets));
    }

    None
}

pub(super) async fn record_failed_dirty_bucket(failed_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    failed_buckets.lock().await.insert(bucket.to_string());
}

pub(super) async fn record_partial_dirty_bucket(partial_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    partial_buckets.lock().await.insert(bucket.to_string());
}

pub(super) async fn requeue_bucket_work(
    bucket_tx: &mpsc::Sender<BucketInfo>,
    bucket: &BucketInfo,
    work_guard: &mut BucketWorkGuard,
) -> bool {
    if bucket_tx.send(bucket.clone()).await.is_err() {
        return false;
    }

    work_guard.mark_requeued();
    true
}

pub(super) async fn mark_unprocessed_bucket_work_failed(
    bucket_rx: &Mutex<mpsc::Receiver<BucketInfo>>,
    remaining: &Arc<AtomicUsize>,
    complete: &CancellationToken,
    failed_buckets: &Arc<Mutex<HashSet<String>>>,
) -> usize {
    let mut failed_count = 0;
    let mut receiver = bucket_rx.lock().await;
    while let Some(bucket) = receiver.recv().await {
        record_failed_dirty_bucket(failed_buckets, &bucket.name).await;
        drop(BucketWorkGuard::new(remaining.clone(), complete.clone()));
        failed_count += 1;
    }
    failed_count
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirtyUsageSnapshotStatus {
    Current,
    Changed,
    Unverified,
}

pub(super) fn dirty_usage_snapshot_status(snapshot: &DirtyUsageSnapshot) -> DirtyUsageSnapshotStatus {
    let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
    if generation == u64::MAX {
        DirtyUsageSnapshotStatus::Unverified
    } else if snapshot.covers_all_pending && generation == snapshot.generation {
        DirtyUsageSnapshotStatus::Current
    } else {
        DirtyUsageSnapshotStatus::Changed
    }
}

#[cfg(test)]
pub(super) fn dirty_usage_bucket_count() -> usize {
    dirty_usage_buckets().len()
}

#[cfg(test)]
pub(crate) fn clear_dirty_usage_buckets_for_tests() {
    dirty_usage_buckets().clear();
    dirty_usage_bucket_scopes().clear();
}

#[cfg(test)]
pub(crate) fn dirty_usage_buckets_for_tests() -> DirtyUsageBuckets {
    dirty_usage_buckets().clone()
}

#[cfg(test)]
pub(crate) fn dirty_usage_bucket_scopes_for_tests() -> DirtyUsageBucketScopes {
    dirty_usage_bucket_scopes().clone()
}
