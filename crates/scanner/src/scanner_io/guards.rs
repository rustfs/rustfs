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
/// scan concurrency accounting: gauge recorders, RAII guards, and worker limits.
use super::*;

const SCANNER_SERVICE_COHORT_MAX_MEMBERS: usize = 4096;
const SCANNER_SERVICE_COHORT_MAX_NAME_BYTES: usize = 128 * 1024;

static SERVICE_COHORT_METRICS_OWNER: StdMutex<std::sync::Weak<()>> = StdMutex::new(std::sync::Weak::new());

struct ScannerCohortMetricsOwner(Arc<()>);

impl Default for ScannerCohortMetricsOwner {
    fn default() -> Self {
        let owner = Arc::new(());
        let mut current = SERVICE_COHORT_METRICS_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = Arc::downgrade(&owner);
        write_service_cohort_metrics(0, 0.0, false);
        Self(owner)
    }
}

impl Drop for ScannerCohortMetricsOwner {
    fn drop(&mut self) {
        let mut current = SERVICE_COHORT_METRICS_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.ptr_eq(&Arc::downgrade(&self.0)) {
            *current = std::sync::Weak::new();
            write_service_cohort_metrics(0, 0.0, false);
        }
    }
}

fn write_service_cohort_metrics(waiting: usize, oldest: f64, overflowed: bool) {
    metrics::gauge!("rustfs_scanner_service_cohort_waiting").set(waiting as f64);
    metrics::gauge!("rustfs_scanner_service_cohort_oldest_wait_seconds").set(oldest);
    metrics::gauge!("rustfs_scanner_service_cohort_capacity_fallback").set(if overflowed { 1.0 } else { 0.0 });
}

struct ScannerCohortWait {
    order: u64,
    queued_at: Instant,
    admitted: bool,
    present: bool,
}

/// Leader-local admission order, never evidence of completed scan coverage.
/// Retains at most 4096 members and 128 KiB of name payload, including the
/// cursor shared with its last member. Candidate selection borrows at most
/// 4096 inventory entries; the existing full inventory is not bounded here.
pub(crate) struct ScannerServiceCohort {
    members: HashMap<DataUsageCacheSource, HashMap<Arc<str>, ScannerCohortWait>>,
    cursor: Option<(DataUsageCacheSource, Arc<str>)>,
    next_order: u64,
    max_members: usize,
    max_name_bytes: usize,
    overflowed: bool,
    metrics_owner: ScannerCohortMetricsOwner,
    waiting: usize,
    oldest_wait_at_refresh: f64,
    #[cfg(test)]
    metric_members_examined: usize,
}

impl Default for ScannerServiceCohort {
    fn default() -> Self {
        Self {
            members: HashMap::new(),
            cursor: None,
            next_order: 0,
            max_members: SCANNER_SERVICE_COHORT_MAX_MEMBERS,
            max_name_bytes: SCANNER_SERVICE_COHORT_MAX_NAME_BYTES,
            overflowed: false,
            metrics_owner: ScannerCohortMetricsOwner::default(),
            waiting: 0,
            oldest_wait_at_refresh: 0.0,
            #[cfg(test)]
            metric_members_examined: 0,
        }
    }
}

impl ScannerServiceCohort {
    pub(crate) fn refresh(&mut self, inventory: &HashMap<DataUsageCacheSource, Vec<BucketInfo>>) {
        let count = inventory
            .values()
            .fold(0usize, |count, buckets| count.saturating_add(buckets.len()));
        let name_bytes = inventory
            .values()
            .flatten()
            .fold(0usize, |bytes, bucket| bytes.saturating_add(bucket.name.len()));
        self.overflowed = count > self.max_members || name_bytes > self.max_name_bytes;
        for wait in self.members.values_mut().flat_map(HashMap::values_mut) {
            wait.present = false;
        }
        for (source, buckets) in inventory {
            for bucket in buckets {
                if let Some(wait) = self
                    .members
                    .get_mut(source)
                    .and_then(|members| members.get_mut(bucket.name.as_str()))
                {
                    wait.present = true;
                }
            }
        }
        self.members.retain(|_, buckets| {
            buckets.retain(|_, wait| wait.present);
            !buckets.is_empty()
        });
        if self.members.values().flat_map(HashMap::values).all(|wait| wait.admitted) {
            self.members.clear();
            self.next_order = 0;
        }
        if count == 0 {
            self.cursor = None;
        }
        let mut member_count = self.members.values().map(HashMap::len).sum::<usize>();
        let mut retained_bytes = self
            .members
            .values()
            .flat_map(HashMap::keys)
            .map(|name| name.len())
            .sum::<usize>();
        let mut incoming = self.admission_candidates(inventory, true);
        if incoming.is_empty() {
            incoming = self.admission_candidates(inventory, false);
        }
        for (pool, set, bucket) in incoming {
            if member_count >= self.max_members {
                break;
            }
            if retained_bytes.saturating_add(bucket.len()) > self.max_name_bytes {
                continue;
            }
            let Some(next_order) = self.next_order.checked_add(1) else {
                self.overflowed = true;
                break;
            };
            let source = DataUsageCacheSource::new(pool, set);
            let members = self.members.entry(source).or_default();
            if members.contains_key(bucket) {
                continue;
            }
            let name: Arc<str> = bucket.into();
            retained_bytes += name.len();
            member_count += 1;
            members.insert(
                name.clone(),
                ScannerCohortWait {
                    order: self.next_order,
                    queued_at: Instant::now(),
                    admitted: false,
                    present: true,
                },
            );
            self.cursor = Some((source, name));
            self.next_order = next_order;
        }
        self.refresh_metrics();
    }

    fn admission_candidates<'a>(
        &self,
        inventory: &'a HashMap<DataUsageCacheSource, Vec<BucketInfo>>,
        after_cursor: bool,
    ) -> Vec<(usize, usize, &'a str)> {
        let mut candidates = std::collections::BinaryHeap::new();
        for (source, buckets) in inventory {
            for bucket in buckets {
                let key = (source.pool_index, source.set_index, bucket.name.as_str());
                let after = self
                    .cursor
                    .as_ref()
                    .is_none_or(|(source, name)| key > (source.pool_index, source.set_index, name.as_ref()));
                if after != after_cursor
                    || bucket.name.len() > self.max_name_bytes
                    || self
                        .members
                        .get(source)
                        .is_some_and(|members| members.contains_key(bucket.name.as_str()))
                {
                    continue;
                }
                if candidates.len() < self.max_members {
                    candidates.push(key);
                } else if candidates.peek().is_some_and(|last| key < *last) {
                    candidates.pop();
                    candidates.push(key);
                }
            }
        }
        candidates.into_sorted_vec()
    }

    pub(crate) fn order_set_indices(&self, sets: &[Arc<SetDisks>]) -> Vec<usize> {
        let ranks = self
            .members
            .iter()
            .map(|(source, buckets)| {
                (
                    *source,
                    buckets
                        .values()
                        .filter(|wait| !wait.admitted)
                        .map(|wait| wait.order)
                        .min()
                        .unwrap_or(u64::MAX),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut indices = (0..sets.len()).collect::<Vec<_>>();
        indices.sort_by_key(|index| {
            (
                ranks
                    .get(&DataUsageCacheSource::new(sets[*index].pool_index, sets[*index].set_index))
                    .copied()
                    .unwrap_or(u64::MAX),
                *index,
            )
        });
        indices
    }

    pub(crate) fn order_buckets(&self, source: DataUsageCacheSource, buckets: &mut [BucketInfo]) {
        let rank = |bucket: &str| {
            self.members
                .get(&source)
                .and_then(|members| members.get(bucket))
                .filter(|wait| !wait.admitted)
                .map_or(u64::MAX, |wait| wait.order)
        };
        // Stable sorting preserves the existing dispatch order in the tail.
        buckets.sort_by_key(|bucket| rank(&bucket.name));
    }

    pub(crate) fn record_admitted(&mut self, source: DataUsageCacheSource, bucket: &str) {
        let Some(wait) = self.members.get_mut(&source).and_then(|members| members.get_mut(bucket)) else {
            return;
        };
        if wait.admitted {
            return;
        }
        wait.admitted = true;
        self.waiting -= 1;
        if self.waiting == 0 {
            self.oldest_wait_at_refresh = 0.0;
        }
        self.record_metrics();
    }

    #[cfg(test)]
    pub(super) fn admitted_members(&self) -> Vec<(DataUsageCacheSource, String)> {
        self.members
            .iter()
            .flat_map(|(source, buckets)| {
                buckets
                    .iter()
                    .filter(|(_, wait)| wait.admitted)
                    .map(|(bucket, _)| (*source, bucket.to_string()))
            })
            .collect()
    }

    fn refresh_metrics(&mut self) {
        // Oldest age is an inventory-refresh snapshot, not a per-admission
        // scan of the cohort. Clear it immediately when no waiters remain.
        let (mut waiting, mut oldest) = (0usize, 0.0f64);
        for wait in self.members.values().flat_map(HashMap::values) {
            #[cfg(test)]
            {
                self.metric_members_examined += 1;
            }
            if !wait.admitted {
                waiting += 1;
                oldest = oldest.max(wait.queued_at.elapsed().as_secs_f64());
            }
        }
        self.waiting = waiting;
        self.oldest_wait_at_refresh = oldest;
        self.record_metrics();
    }

    fn record_metrics(&self) {
        // Serialize owner replacement, publication and retirement. A retired
        // scanner must neither publish nor clear a replacement's gauges.
        let current = SERVICE_COHORT_METRICS_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.ptr_eq(&Arc::downgrade(&self.metrics_owner.0)) {
            write_service_cohort_metrics(self.waiting, self.oldest_wait_at_refresh, self.overflowed);
        }
    }
}

pub(super) async fn wait_for_bucket_scan_permit(
    semaphore: &Arc<Semaphore>,
    ctx: &CancellationToken,
    complete: &CancellationToken,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    tokio::select! {
        biased;
        _ = complete.cancelled() => None,
        _ = ctx.cancelled() => None,
        permit = semaphore.clone().acquire_owned() => permit.ok(),
    }
}

pub(super) fn bucket_usage_scan_order(
    buckets: &[BucketInfo],
    old_cache: &DataUsageCache,
    dirty_buckets: &DirtyUsageBuckets,
) -> Vec<BucketInfo> {
    let mut ordered = Vec::with_capacity(buckets.len());

    for bucket in buckets {
        if dirty_buckets.contains_key(&bucket.name) {
            ordered.push(bucket.clone());
        }
    }

    for bucket in buckets {
        if !dirty_buckets.contains_key(&bucket.name) && old_cache.find(&bucket.name).is_none() {
            ordered.push(bucket.clone());
        }
    }

    for bucket in buckets {
        if !dirty_buckets.contains_key(&bucket.name) && old_cache.find(&bucket.name).is_some() {
            ordered.push(bucket.clone());
        }
    }

    ordered
}

pub(super) fn record_set_scan_concurrency_limit(limit: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT).set(limit as f64);
    global_metrics().record_scanner_set_scan_state(Some(limit), None, None);
}

pub(super) fn record_set_scans_queued(count: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(count as f64);
    global_metrics().record_scanner_set_scan_state(None, Some(count), None);
}

pub(super) fn record_set_scans_active(count: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(count as f64);
    global_metrics().record_scanner_set_scan_state(None, None, Some(count));
}

pub(super) fn record_disk_scan_concurrency_limit(pool: &str, set: &str, limit: usize) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(limit as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, Some(limit), None, None);
}

pub(super) fn record_disk_bucket_scans_active(count: usize, pool: &str, set: &str) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(count as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, None, None, Some(count));
}

pub(super) struct SetScanActiveGuard {
    active: Arc<AtomicUsize>,
}

impl SetScanActiveGuard {
    pub(super) fn new(active: Arc<AtomicUsize>) -> Self {
        let active_count = active.fetch_add(1, Ordering::Relaxed) + 1;
        record_set_scans_active(active_count);
        Self { active }
    }
}

impl Drop for SetScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        record_set_scans_active(active_count);
    }
}

pub(super) struct DiskBucketScanActiveGuard {
    active: Arc<AtomicUsize>,
    pool: String,
    set: String,
}

pub(super) struct BucketWorkGuard {
    remaining: Arc<AtomicUsize>,
    complete: CancellationToken,
    requeued: bool,
}

impl BucketWorkGuard {
    pub(super) fn new(remaining: Arc<AtomicUsize>, complete: CancellationToken) -> Self {
        Self {
            remaining,
            complete,
            requeued: false,
        }
    }

    pub(super) fn mark_requeued(&mut self) {
        self.requeued = true;
    }
}

impl Drop for BucketWorkGuard {
    fn drop(&mut self) {
        if !self.requeued && self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.complete.cancel();
        }
    }
}

impl DiskBucketScanActiveGuard {
    pub(super) fn new(active: Arc<AtomicUsize>, pool: String, set: String) -> Self {
        let active_count = active.fetch_add(1, Ordering::Relaxed) + 1;
        record_disk_bucket_scans_active(active_count, &pool, &set);
        Self { active, pool, set }
    }
}

impl Drop for DiskBucketScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        record_disk_bucket_scans_active(active_count, &self.pool, &self.set);
    }
}

pub(super) struct BucketDriveFailureGuard {
    failed: bool,
    source: rustfs_scanner_metrics::metrics::ScannerWorkSource,
    bucket: String,
    drive: String,
}

impl BucketDriveFailureGuard {
    pub(super) fn new(source: rustfs_scanner_metrics::metrics::ScannerWorkSource, bucket: &str, drive: &str) -> Self {
        Self {
            failed: true,
            source,
            bucket: bucket.to_string(),
            drive: drive.to_string(),
        }
    }

    pub(super) fn mark_not_failed(&mut self) {
        self.failed = false;
    }
}

impl Drop for BucketDriveFailureGuard {
    fn drop(&mut self) {
        global_metrics().record_scan_bucket_drive_end(self.source, &self.bucket, &self.drive);
        if self.failed {
            global_metrics().record_scan_bucket_drive_failure();
        }
    }
}

pub(super) struct DiskBucketScanGaugeReset {
    pool: String,
    set: String,
}

impl DiskBucketScanGaugeReset {
    pub(super) fn new(pool: String, set: String) -> Self {
        Self { pool, set }
    }
}

impl Drop for DiskBucketScanGaugeReset {
    fn drop(&mut self) {
        reset_disk_bucket_scan_gauges(&self.pool, &self.set);
    }
}

pub(super) fn decrement_atomic_usize(counter: &AtomicUsize) -> usize {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(1)))
        .map(|previous| previous.saturating_sub(1))
        .unwrap_or_else(|current| current)
}

pub(super) fn increment_atomic_usize(counter: &AtomicUsize) -> usize {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(1)))
        .map(|previous| previous.saturating_add(1))
        .unwrap_or_else(|current| current)
}

pub(super) fn record_disk_bucket_scans_queued(count: usize, pool: &str, set: &str) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(count as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, None, Some(count), None);
}

pub(super) fn decrement_disk_bucket_scans_queued(counter: &AtomicUsize, pool: &str, set: &str) {
    let queued_count = decrement_atomic_usize(counter);
    record_disk_bucket_scans_queued(queued_count, pool, set);
}

pub(super) fn increment_disk_bucket_scans_queued(counter: &AtomicUsize, pool: &str, set: &str) {
    let queued_count = increment_atomic_usize(counter);
    record_disk_bucket_scans_queued(queued_count, pool, set);
}

pub(super) fn reset_set_scan_gauges() {
    record_set_scan_concurrency_limit(0);
    record_set_scans_queued(0);
    record_set_scans_active(0);
    global_metrics().reset_scanner_set_scan_state();
}

pub(super) fn reset_disk_bucket_scan_gauges(pool: &str, set: &str) {
    record_disk_scan_concurrency_limit(pool, set, 0);
    record_disk_bucket_scans_queued(0, pool, set);
    record_disk_bucket_scans_active(0, pool, set);
}

pub(super) fn scanner_concurrency_limit(configured: usize, available: usize) -> usize {
    if available == 0 {
        return 0;
    }

    if crate::workload_admission::foreground_workload_activity() > 0 {
        return 1;
    }

    if configured == 0 {
        available
    } else {
        configured.min(available).max(1)
    }
}

pub(super) fn scanner_max_concurrent_set_scans(available: usize) -> usize {
    scanner_concurrency_limit(crate::runtime_config::scanner_max_concurrent_set_scans_configured(), available)
}

pub(super) fn scanner_max_concurrent_disk_scans(available: usize) -> usize {
    scanner_concurrency_limit(crate::runtime_config::scanner_max_concurrent_disk_scans_configured(), available)
}

pub(super) fn scanner_budgeted_concurrency_limit(configured_limit: usize, requires_serial_progress_accounting: bool) -> usize {
    if requires_serial_progress_accounting {
        1
    } else {
        configured_limit
    }
}

pub(super) fn record_set_scan_failure(first_err: &mut Option<Error>, err: Error) {
    if first_err.is_none() {
        *first_err = Some(err);
    }
}

pub(super) fn scanner_task_join_error(stage: &str, err: tokio::task::JoinError) -> Error {
    Error::other(format!("{stage} task join failed: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_scanner_metrics::metrics::{ScannerWorkSource, global_metrics};
    use tokio::sync::oneshot;

    #[derive(Default)]
    struct RecordedGauge(AtomicU64);

    impl metrics::GaugeFn for RecordedGauge {
        fn increment(&self, value: f64) {
            self.set(f64::from_bits(self.0.load(Ordering::Relaxed)) + value);
        }
        fn decrement(&self, value: f64) {
            self.increment(-value);
        }
        fn set(&self, value: f64) {
            self.0.store(value.to_bits(), Ordering::Relaxed);
        }
    }

    #[derive(Default)]
    struct CohortGaugeRecorder(StdMutex<HashMap<String, Arc<RecordedGauge>>>);

    impl metrics::Recorder for CohortGaugeRecorder {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            metrics::Counter::noop()
        }
        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::from_arc(
                self.0
                    .lock()
                    .expect("gauge recorder")
                    .entry(key.name().to_string())
                    .or_default()
                    .clone(),
            )
        }
    }

    impl CohortGaugeRecorder {
        fn value(&self, name: &str) -> f64 {
            f64::from_bits(self.0.lock().expect("gauge recorder")[name].0.load(Ordering::Relaxed))
        }
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_metrics_retire_only_the_current_owner() {
        let recorder = CohortGaugeRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let mut old = ScannerServiceCohort::default();
            old.refresh(&cohort_inventory(&["old"]));
            let mut current = ScannerServiceCohort {
                max_members: 1,
                ..Default::default()
            };
            current.refresh(&cohort_inventory(&["a", "b"]));
            for wait in current.members.values_mut().flat_map(HashMap::values_mut) {
                wait.queued_at = Instant::now() - Duration::from_secs(60);
            }
            current.refresh_metrics();
            old.refresh(&cohort_inventory(&["old", "more"]));
            drop(old);
            assert_eq!(recorder.value("rustfs_scanner_service_cohort_waiting"), 1.0);
            assert_eq!(recorder.value("rustfs_scanner_service_cohort_capacity_fallback"), 1.0);
            assert!(recorder.value("rustfs_scanner_service_cohort_oldest_wait_seconds") >= 60.0);
            drop(current);
            for metric in [
                "rustfs_scanner_service_cohort_waiting",
                "rustfs_scanner_service_cohort_oldest_wait_seconds",
                "rustfs_scanner_service_cohort_capacity_fallback",
            ] {
                assert_eq!(recorder.value(metric), 0.0, "owner retirement must clear {metric}");
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_admission_metrics_do_not_rescan_a_full_window() {
        let recorder = CohortGaugeRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            let source = DataUsageCacheSource::new(0, 0);
            let names = (0..SCANNER_SERVICE_COHORT_MAX_MEMBERS)
                .map(|index| format!("bucket-{index:04}"))
                .collect::<Vec<_>>();
            let inventory = cohort_inventory(&names.iter().map(String::as_str).collect::<Vec<_>>());
            let mut cohort = ScannerServiceCohort::default();
            cohort.refresh(&inventory);
            assert_eq!(cohort.metric_members_examined, SCANNER_SERVICE_COHORT_MAX_MEMBERS);
            assert_eq!(cohort.waiting, SCANNER_SERVICE_COHORT_MAX_MEMBERS);
            for (index, name) in names.iter().enumerate() {
                cohort.record_admitted(source, name);
                for _ in 0..10 {
                    cohort.record_admitted(source, name);
                    cohort.record_admitted(source, "untracked-overflow-name");
                    cohort.record_admitted(DataUsageCacheSource::new(99, 0), name);
                }
                assert_eq!(cohort.waiting, SCANNER_SERVICE_COHORT_MAX_MEMBERS - index - 1);
                assert_eq!(
                    cohort.metric_members_examined, SCANNER_SERVICE_COHORT_MAX_MEMBERS,
                    "tracked, repeated and overflow admissions must not scan cohort members"
                );
            }
            assert_eq!(recorder.value("rustfs_scanner_service_cohort_waiting"), 0.0);
            assert_eq!(recorder.value("rustfs_scanner_service_cohort_oldest_wait_seconds"), 0.0);
            cohort.refresh(&inventory);
            assert_eq!(
                cohort.metric_members_examined,
                2 * SCANNER_SERVICE_COHORT_MAX_MEMBERS,
                "one inventory refresh performs one metrics traversal"
            );
        });
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn service_cohort_queued_permit_cancel_and_drop_return_the_same_capacity() {
        let semaphore = Arc::new(Semaphore::new(1));
        let active = Arc::new(AtomicUsize::new(0));
        let mut cohort = ScannerServiceCohort::default();
        cohort.refresh(&cohort_inventory(&["waiting"]));
        let gauge_reset = DiskBucketScanGaugeReset::new("cohort-wait".to_string(), "0".to_string());
        record_disk_bucket_scans_queued(1, "cohort-wait", "0");
        record_disk_bucket_scans_active(0, "cohort-wait", "0");
        for cancel in [true, false] {
            let held = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("hold the sole permit as a barrier");
            let ctx = CancellationToken::new();
            let complete = CancellationToken::new();
            let mut waiter = Box::pin(wait_for_bucket_scan_permit(&semaphore, &ctx, &complete));
            assert!(
                futures::poll!(&mut waiter).is_pending(),
                "the production wait must actually enqueue behind the barrier"
            );
            assert_eq!(semaphore.available_permits(), 0);
            if cancel {
                ctx.cancel();
                assert!(waiter.as_mut().await.is_none());
            }
            drop(waiter);
            assert_eq!(semaphore.available_permits(), 0, "cancelling a waiter must not release the held permit");
            assert!(cohort.admitted_members().is_empty());
            drop(held);
            assert_eq!(semaphore.available_permits(), 1, "no queued waiter may leak or steal released capacity");
        }
        let ctx = CancellationToken::new();
        let complete = CancellationToken::new();
        let permit = wait_for_bucket_scan_permit(&semaphore, &ctx, &complete)
            .await
            .expect("same semaphore remains usable");
        let active_guard = DiskBucketScanActiveGuard::new(active.clone(), "cohort-wait".to_string(), "0".to_string());
        assert_eq!(active.load(Ordering::Relaxed), 1);
        drop(active_guard);
        drop(permit);
        drop(gauge_reset);
        assert_eq!(active.load(Ordering::Relaxed), 0);
        assert_eq!(semaphore.available_permits(), 1);
        let state = global_metrics()
            .scanner_runtime_details_report()
            .disk_bucket_scan_states
            .into_iter()
            .find(|state| state.pool == "cohort-wait" && state.set == "0")
            .expect("fixture gauges");
        assert_eq!((state.queued, state.active), (0, 0));
        assert!(cohort.admitted_members().is_empty(), "permit ownership alone does not admit a bucket");
    }

    fn cohort_inventory(names: &[&str]) -> HashMap<DataUsageCacheSource, Vec<BucketInfo>> {
        HashMap::from([(
            DataUsageCacheSource::new(0, 0),
            names
                .iter()
                .map(|name| BucketInfo {
                    name: (*name).to_string(),
                    ..Default::default()
                })
                .collect(),
        )])
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_visits_fixed_members_within_service_round_bound() {
        let inventory = cohort_inventory(&["a", "b", "c", "d", "e"]);
        let source = DataUsageCacheSource::new(0, 0);
        let mut cohort = ScannerServiceCohort::default();
        let mut admitted = HashSet::new();
        for _ in 0..3 {
            cohort.refresh(&inventory);
            let mut buckets = inventory[&source].clone();
            cohort.order_buckets(source, &mut buckets);
            for bucket in buckets.iter().take(2) {
                admitted.insert(bucket.name.clone());
                cohort.record_admitted(source, &bucket.name);
            }
        }
        assert_eq!(admitted.len(), 5, "ceil(5/2) service rounds must include every member");
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_keeps_waiting_bootstrap_ahead_of_new_work() {
        let source = DataUsageCacheSource::new(0, 0);
        let mut cohort = ScannerServiceCohort::default();
        cohort.refresh(&cohort_inventory(&["a-hot", "z-bootstrap"]));
        cohort.record_admitted(source, "a-hot");
        let queued_at = cohort.members[&source]["z-bootstrap"].queued_at;
        let inventory = cohort_inventory(&["a-hot", "aaa-new-bootstrap", "z-bootstrap"]);
        for _ in 0..10 {
            cohort.refresh(&inventory);
            let mut buckets = inventory[&source].clone();
            cohort.order_buckets(source, &mut buckets);
            assert_eq!(buckets[0].name, "z-bootstrap");
            assert_eq!(buckets[1].name, "aaa-new-bootstrap");
            assert_eq!(cohort.members[&source]["z-bootstrap"].queued_at, queued_at);
        }
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_overflow_preserves_waiters_and_rotates_finished_windows() {
        let source = DataUsageCacheSource::new(0, 0);
        let mut cohort = ScannerServiceCohort {
            max_members: 2,
            max_name_bytes: 4,
            ..Default::default()
        };
        cohort.refresh(&cohort_inventory(&["aa", "bb"]));
        cohort.record_admitted(source, "aa");
        for names in [["aa", "bb", "c"], ["aa", "bb", "d"]] {
            let inventory = cohort_inventory(&names);
            cohort.refresh(&inventory);
            assert!(cohort.overflowed);
            assert_eq!(cohort.members[&source].len(), 2);
            assert!(cohort.members[&source].contains_key("aa"));
            let mut fallback = inventory[&source].clone();
            fallback.reverse();
            cohort.order_buckets(source, &mut fallback);
            assert_eq!(fallback[0].name, "bb", "overflow must not discard a waiting member's priority");
            assert_eq!(fallback.len(), 3, "unknown tail must remain dispatchable");
        }
        cohort.record_admitted(source, "bb");
        let inventory = cohort_inventory(&["aa", "bb", "c", "d"]);
        cohort.refresh(&inventory);
        assert_eq!(
            cohort.members[&source].keys().map(AsRef::as_ref).collect::<HashSet<&str>>(),
            HashSet::from(["c", "d"])
        );
        cohort.record_admitted(source, "c");
        cohort.record_admitted(source, "d");
        cohort.refresh(&inventory);
        assert!(
            cohort.members[&source].contains_key("aa"),
            "finite inventory must wrap after the last window"
        );
        cohort.refresh(&cohort_inventory(&["bb"]));
        assert!(!cohort.overflowed);
        assert_eq!(cohort.members[&source].len(), 1);
        cohort.next_order = u64::MAX;
        cohort.refresh(&cohort_inventory(&["bb", "c"]));
        assert!(cohort.overflowed);
        assert_eq!(cohort.members[&source].len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_overflow_eventually_services_every_stable_member() {
        let source = DataUsageCacheSource::new(0, 0);
        let inventory = cohort_inventory(&["aa", "bb", "cc", "dd", "ee", "ff"]);
        let mut cohort = ScannerServiceCohort {
            max_members: 2,
            max_name_bytes: 4,
            ..Default::default()
        };
        let mut admitted = HashSet::new();
        for _ in 0..3 {
            cohort.refresh(&inventory);
            let mut buckets = inventory[&source].clone();
            cohort.order_buckets(source, &mut buckets);
            for bucket in buckets.iter().take(2) {
                admitted.insert(bucket.name.clone());
                cohort.record_admitted(source, &bucket.name);
            }
        }
        assert_eq!(
            admitted,
            HashSet::from([
                "aa".to_string(),
                "bb".to_string(),
                "cc".to_string(),
                "dd".to_string(),
                "ee".to_string(),
                "ff".to_string(),
            ]),
            "stable overflow inventory must rotate every member through the tracked window"
        );
        assert!(cohort.overflowed);
    }

    #[test]
    #[serial_test::serial]
    fn service_cohort_bounds_names_and_does_not_reset_duplicate_dirty_age() {
        let source = DataUsageCacheSource::new(0, 0);
        let mut cohort = ScannerServiceCohort {
            max_members: 2,
            max_name_bytes: 4,
            ..Default::default()
        };
        cohort.refresh(&cohort_inventory(&["aa", "bb", "long-name"]));
        let queued_at = cohort.members[&source]["bb"].queued_at;
        for _ in 0..10 {
            cohort.refresh(&cohort_inventory(&["aa", "aa", "bb", "long-name"]));
            assert_eq!(cohort.members.values().map(HashMap::len).sum::<usize>(), 2);
            assert_eq!(
                cohort
                    .members
                    .values()
                    .flat_map(HashMap::keys)
                    .map(|name| name.len())
                    .sum::<usize>(),
                4
            );
            assert_eq!(cohort.members[&source]["bb"].queued_at, queued_at);
        }
        cohort.refresh(&cohort_inventory(&[]));
        assert!(cohort.members.is_empty());
        assert!(cohort.cursor.is_none());
    }

    fn active_bucket_drive_count(source: ScannerWorkSource, bucket: &str, drive: &str) -> u64 {
        global_metrics()
            .scanner_runtime_details_report()
            .active_bucket_drive_scans
            .into_iter()
            .find(|active| active.source == source.as_str() && active.bucket == bucket && active.drive == drive)
            .map_or(0, |active| active.count)
    }

    #[test]
    fn bucket_drive_failure_guard_retires_active_scan_on_drop() {
        let source = ScannerWorkSource::Usage;
        let bucket = "__guard_active_lifecycle_test__";
        let drive = "/__guard_active_lifecycle_test__";
        global_metrics().record_scan_bucket_drive_start(source, bucket, drive);
        {
            let mut guard = BucketDriveFailureGuard::new(source, bucket, drive);
            guard.mark_not_failed();
        }
        assert!(
            !global_metrics()
                .scanner_runtime_details_report()
                .active_bucket_drive_scans
                .iter()
                .any(|active| active.source == source.as_str() && active.bucket == bucket && active.drive == drive)
        );
    }

    #[tokio::test]
    async fn bucket_drive_failure_guard_retires_active_scan_after_cancellation() {
        let source = ScannerWorkSource::Usage;
        let bucket = "__guard_cancel_lifecycle_test__";
        let drive = "/__guard_cancel_lifecycle_test__";
        global_metrics().record_scan_bucket_drive_start(source, bucket, drive);

        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let worker = tokio::spawn(async move {
            let mut guard = BucketDriveFailureGuard::new(source, bucket, drive);
            worker_cancellation.cancelled().await;
            guard.mark_not_failed();
        });

        cancellation.cancel();
        worker.await.expect("cancelled scanner worker should finish");

        assert_eq!(active_bucket_drive_count(source, bucket, drive), 0);
    }

    #[tokio::test]
    async fn bucket_drive_failure_guard_retires_active_scan_when_worker_is_aborted() {
        let source = ScannerWorkSource::Bitrot;
        let bucket = "__guard_abort_lifecycle_test__";
        let drive = "/__guard_abort_lifecycle_test__";
        global_metrics().record_scan_bucket_drive_start(source, bucket, drive);

        let (started_sender, started_receiver) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let _guard = BucketDriveFailureGuard::new(source, bucket, drive);
            started_sender.send(()).expect("test should observe worker start");
            std::future::pending::<()>().await;
        });
        started_receiver.await.expect("scanner worker should start");
        assert_eq!(active_bucket_drive_count(source, bucket, drive), 1);

        worker.abort();
        worker.await.expect_err("aborted scanner worker should report cancellation");

        assert_eq!(active_bucket_drive_count(source, bucket, drive), 0);
    }

    #[tokio::test]
    async fn bucket_drive_failure_guards_track_overlapping_scans_independently() {
        let source = ScannerWorkSource::Usage;
        let bucket = "__guard_overlap_lifecycle_test__";
        let drive = "/__guard_overlap_lifecycle_test__";
        global_metrics().record_scan_bucket_drive_start(source, bucket, drive);
        global_metrics().record_scan_bucket_drive_start(source, bucket, drive);

        let (first_release_sender, first_release_receiver) = oneshot::channel();
        let (second_release_sender, second_release_receiver) = oneshot::channel();
        let (first_started_sender, first_started_receiver) = oneshot::channel();
        let (second_started_sender, second_started_receiver) = oneshot::channel();
        let first = tokio::spawn(async move {
            let _guard = BucketDriveFailureGuard::new(source, bucket, drive);
            first_started_sender.send(()).expect("test should observe first worker start");
            first_release_receiver.await.expect("first worker should be released");
        });
        let second = tokio::spawn(async move {
            let _guard = BucketDriveFailureGuard::new(source, bucket, drive);
            second_started_sender
                .send(())
                .expect("test should observe second worker start");
            second_release_receiver.await.expect("second worker should be released");
        });

        first_started_receiver.await.expect("first scanner worker should start");
        second_started_receiver.await.expect("second scanner worker should start");
        assert_eq!(active_bucket_drive_count(source, bucket, drive), 2);

        first_release_sender.send(()).expect("first worker should be released");
        first.await.expect("first scanner worker should finish");
        assert_eq!(active_bucket_drive_count(source, bucket, drive), 1);

        second_release_sender.send(()).expect("second worker should be released");
        second.await.expect("second scanner worker should finish");
        assert_eq!(active_bucket_drive_count(source, bucket, drive), 0);
    }
}
