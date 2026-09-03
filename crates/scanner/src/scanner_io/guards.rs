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

    if crate::current_foreground_read_activity() > 0 {
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
