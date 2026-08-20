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
pub(super) static DIRTY_USAGE_BUCKET_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
pub(super) static SCANNER_ACTIVITY_EPOCH: LazyLock<String> = LazyLock::new(|| format!("{:032x}", rand::random::<u128>()));
pub(super) static SCANNER_MAINTENANCE_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(super) static SCANNER_MAINTENANCE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageState {
    pub generation: u64,
    pub pending: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScannerDirtyUsageAckError {
    #[error("scanner process instance changed before dirty usage acknowledgement")]
    ProcessChanged,
    #[error("scanner dirty usage generation cannot be acknowledged")]
    InvalidGeneration,
}

pub(super) fn dirty_usage_buckets() -> MutexGuard<'static, DirtyUsageBuckets> {
    DIRTY_USAGE_BUCKETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn advance_generation(generation: &AtomicU64) -> u64 {
    generation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(1)))
        .map_or_else(|current| current, |previous| previous.saturating_add(1))
}

pub fn record_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    // A write invalidates this bucket's prefix-usage answers on the spot so
    // admin/console consumers never ride the full TTL after a change
    // (rustfs/backlog#1872).
    crate::prefix_usage::invalidate_prefix_usage_cache(bucket);
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
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

pub fn acknowledge_dirty_usage_generation(
    instance_id: &str,
    generation: u64,
) -> std::result::Result<(), ScannerDirtyUsageAckError> {
    if instance_id != scanner_activity_epoch() {
        return Err(ScannerDirtyUsageAckError::ProcessChanged);
    }

    let (cleared_buckets, pending_buckets) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let current_generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        if generation == 0 || generation == u64::MAX || current_generation == u64::MAX || generation > current_generation {
            return Err(ScannerDirtyUsageAckError::InvalidGeneration);
        }

        let before = dirty_buckets.len();
        dirty_buckets.retain(|_, dirty_generation| *dirty_generation > generation);
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
        dirty_buckets.remove(bucket);
        advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_clear(usize_to_u64_saturated(pending_buckets));
}

pub(super) fn snapshot_dirty_usage_buckets(buckets: &[BucketInfo], absent_generation_cutoff: u64) -> DirtyUsageSnapshot {
    let (snapshot, generation, covers_all_pending) = {
        let dirty_buckets = dirty_usage_buckets();
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
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let covers_all_pending = generation == absent_generation_cutoff && snapshot.len() == dirty_buckets.len();
        (snapshot, generation, covers_all_pending)
    };
    global_metrics().record_scanner_dirty_usage_cycle_snapshot(usize_to_u64_saturated(snapshot.len()));
    DirtyUsageSnapshot {
        buckets: Arc::new(snapshot),
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
        let mut cleared_buckets = 0usize;
        for (bucket, generation) in snapshot {
            if dirty_buckets.get(bucket).is_some_and(|current| current == generation) {
                dirty_buckets.remove(bucket);
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
}

#[cfg(test)]
pub(crate) fn dirty_usage_buckets_for_tests() -> DirtyUsageBuckets {
    dirty_usage_buckets().clone()
}
