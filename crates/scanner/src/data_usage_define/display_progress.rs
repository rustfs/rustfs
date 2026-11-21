// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use bytes::{Buf, Bytes};
use rustfs_data_usage::{
    BUCKET_SCAN_PROGRESS_MAX_BYTES, BUCKET_SCAN_PROGRESS_VERSION, BucketScanProgressSource, BucketScanStage,
    bucket_scan_progress_path,
};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

// A timed-out worker retains its permit until it exits, so repeated saves
// cannot accumulate CPU work or decoded copies of large checkpoints.
static DISPLAY_PROGRESS_WORKER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

async fn extract_progress<F>(
    admission: crate::ScannerDataUsagePublicationAdmission,
    workers: Arc<Semaphore>,
    deadline: tokio::time::Instant,
    extract: F,
) -> Option<BucketScanProgressSource>
where
    F: FnOnce(CancellationToken) -> Option<BucketScanProgressSource> + Send + 'static,
{
    // Only capture the durable movement generation under admission. CPU work
    // and waiting for the bounded worker must never retain the movement fence.
    drop(admission);
    let cancel = CancellationToken::new();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let permit = tokio::time::timeout_at(deadline, workers.acquire_owned()).await.ok()?.ok()?;
    let (complete, result) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    // A dedicated, admitted CPU worker cannot occupy the native I/O pool
    // needed to finish the primary save after this waiter has expired.
    std::thread::Builder::new()
        .name("scanner-progress".into())
        .spawn(move || {
            let _runtime = runtime.enter();
            let _permit = permit;
            let sample = (!cancel.is_cancelled() && tokio::time::Instant::now() < deadline)
                .then(|| extract(cancel))
                .flatten();
            let _ = complete.send(sample);
        })
        .ok()?;
    tokio::time::timeout_at(deadline, result).await.ok()?.ok()?
}

struct ProgressInput {
    chunks: std::collections::VecDeque<Bytes>,
    cancel: CancellationToken,
    deadline: tokio::time::Instant,
}

impl std::io::Read for ProgressInput {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.cancel.is_cancelled() || tokio::time::Instant::now() >= self.deadline {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        while self.chunks.front().is_some_and(Bytes::is_empty) {
            self.chunks.pop_front();
        }
        let Some(chunk) = self.chunks.front_mut() else { return Ok(0) };
        let count = output.len().min(chunk.len());
        output[..count].copy_from_slice(&chunk[..count]);
        chunk.advance(count);
        Ok(count)
    }
}

async fn read_display_progress<S: ScannerObjectIO>(
    store: Arc<S>,
    path: &str,
) -> StorageResult<(Option<BucketScanProgressSource>, DataUsageCacheRevision)> {
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };
    let mut reader = match store
        .get_object_reader(RUSTFS_META_BUCKET, path, None, HeaderMap::new(), &opts)
        .await
    {
        Ok(reader) => reader,
        Err(Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(..)) => {
            return Ok((None, DataUsageCacheRevision::Missing));
        }
        Err(error) => return Err(error),
    };
    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .map(|etag| DataUsageCacheRevision::Etag(etag.clone()))
        .ok_or_else(|| Error::other("scanner progress object has no revision"))?;
    let mut bytes = Vec::new();
    (&mut reader)
        .take(u64::try_from(BUCKET_SCAN_PROGRESS_MAX_BYTES).map_err(Error::other)? + 1)
        .read_to_end(&mut bytes)
        .await?;
    let sample = (bytes.len() <= BUCKET_SCAN_PROGRESS_MAX_BYTES)
        .then(|| serde_json::from_slice(&bytes).ok())
        .flatten();
    Ok((sample, revision))
}

impl DataUsageCache {
    /// Extract accepted coverage, never the mixed old/new running root total.
    /// Physical replicas are alternative samples of one logical source.
    pub(crate) fn display_scan_progress(&self, movement_generation: u64) -> Option<BucketScanProgressSource> {
        let identity = self.info.scan_identity.filter(|identity| identity.is_valid())?;
        let source = self.info.source?;
        let updated_at = u64::try_from(
            self.info
                .last_update?
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()?
                .as_millis(),
        )
        .ok()?;
        if self.info.name.is_empty() || self.info.name == DATA_USAGE_ROOT {
            return None;
        }
        let completed = if self.info.snapshot_complete {
            self.checked_flatten_complete_scope(&self.info.name)
        } else {
            self.completed_bucket_observation()
                .filter(|observation| Some(observation.completed_at) == self.info.last_update && !self.has_scan_position())
                .map(|observation| observation.entry.clone())
        };
        let mut accepted = completed.as_ref().map(|entry| (entry.objects, entry.size, updated_at));
        if completed.is_none() {
            let frontier = self.validated_scan_frontier();
            let mut replicas = BTreeMap::<&str, Vec<_>>::new();
            for checkpoint in &self.info.scan_directory_work {
                if let Some((coverage, entry, measured_at)) = checkpoint.display_usage(&self.info) {
                    replicas
                        .entry(checkpoint.endpoint())
                        .or_default()
                        .push((coverage, entry, measured_at));
                }
            }
            if let Some(active) = self.info.scan_directory_source.as_deref() {
                replicas.entry(active).or_default();
            }
            for (endpoint, mut receipts) in replicas {
                // Ancestors precede descendants. An acknowledged ancestor child
                // already contains every nested contribution below that child.
                receipts.sort_by(|a, b| a.0.parent.split('/').cmp(b.0.parent.split('/')));
                let mut selected = Vec::new();
                let mut subtotal: Option<(usize, usize)> = None;
                let mut measured_at: Option<u64> = None;
                let mut known_time = true;
                for (coverage, entry, time) in &receipts {
                    if selected.iter().any(|parent: &&crate::scanner_folder::DirectoryProgress| {
                        parent.parent == coverage.parent || parent.covers(&coverage.parent)
                    }) {
                        continue;
                    }
                    if endpoint == self.info.scan_directory_source.as_deref().unwrap_or_default()
                        && frontier.is_some_and(|through| scan_frontier_covers_path(&coverage.parent, through))
                    {
                        continue;
                    }
                    let (objects, size) = subtotal.unwrap_or_default();
                    subtotal = Some((objects.checked_add(entry.objects)?, size.checked_add(entry.size)?));
                    if let Some(time) = time {
                        measured_at = Some(measured_at.map_or(*time, |previous| previous.min(*time)));
                    } else {
                        known_time = false;
                    }
                    selected.push(coverage);
                }
                if Some(endpoint) == self.info.scan_directory_source.as_deref()
                    && let Some(through) = frontier
                {
                    // A validated receipt signs this prefix. Sum each scalar
                    // once, excluding the ranges represented by compacted work.
                    self.checked_flatten_complete_scope(&self.info.name)?;
                    let mut contributed = false;
                    for (path, entry) in &self.cache {
                        if scan_frontier_covers_path(path, through) && !selected.iter().any(|proof| proof.covers(path)) {
                            let (objects, size) = subtotal.unwrap_or_default();
                            subtotal = Some((objects.checked_add(entry.objects)?, size.checked_add(entry.size)?));
                            contributed = true;
                        }
                    }
                    if contributed {
                        if let Some(time) = self.info.scan_coverage_measured_at {
                            measured_at = Some(measured_at.map_or(time, |previous| previous.min(time)));
                        } else {
                            known_time = false;
                        }
                    }
                }
                // Replicas overlap. Select one sample; never sum or append them.
                // Legacy coverage without a measurement time cannot borrow a
                // newer checkpoint time. Keep the prior companion instead.
                if known_time
                    && let (Some((objects, size)), Some(time)) = (subtotal, measured_at)
                    && accepted.is_none_or(|previous| (objects, size, time) > previous)
                {
                    accepted = Some((objects, size, time));
                }
            }
        }
        Some(BucketScanProgressSource {
            version: BUCKET_SCAN_PROGRESS_VERSION,
            bucket: self.info.name.clone(),
            bucket_incarnation: identity.bucket_incarnation.to_string(),
            pool_index: u64::try_from(source.pool_index).ok()?,
            set_index: u64::try_from(source.set_index).ok()?,
            set_layout: identity.set_layout.0,
            publication_epoch: identity.publication_epoch,
            movement_generation: Some(movement_generation),
            scanner_epoch: self.info.leader_epoch,
            scanner_cycle: self.info.next_cycle,
            updated_at: accepted.map_or(updated_at, |(_, _, time)| time),
            stage: if completed.is_some() {
                BucketScanStage::Completed
            } else if accepted.is_some() {
                BucketScanStage::Scanning
            } else {
                BucketScanStage::Enumerating
            },
            objects: accepted.map(|(objects, _, _)| u64::try_from(objects)).transpose().ok()?,
            size: accepted.map(|(_, size, _)| u64::try_from(size)).transpose().ok()?,
        })
    }

    pub(super) async fn prepare_display_progress<S: ScannerObjectIO + ScannerConfigObjectDelete>(
        &self,
        store: Arc<S>,
        expected_epoch: Option<u64>,
        input: Option<Vec<Bytes>>,
    ) -> Option<(String, Vec<u8>, DataUsageCacheRevision)> {
        if self.info.name.is_empty() || self.info.name == DATA_USAGE_ROOT || self.info.scan_identity.is_none() {
            return None;
        }
        let expected_epoch = expected_epoch?;
        let input = input?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let admission = scanner_publication_admission_for_epoch(store.clone(), expected_epoch).await?;
        let movement_generation = store.scanner_data_usage_movement_generation().await?;
        let sample = extract_progress(admission, DISPLAY_PROGRESS_WORKER.clone(), deadline, move |cancel| {
            let reader = ProgressInput {
                chunks: input.into(),
                cancel: cancel.clone(),
                deadline,
            };
            let cache: DataUsageCache = rmp_serde::from_read(reader).ok()?;
            if cancel.is_cancelled() || tokio::time::Instant::now() >= deadline {
                return None;
            }
            cache.display_scan_progress(movement_generation)
        })
        .await?;
        if expected_epoch != sample.publication_epoch {
            return None;
        }
        let path = bucket_scan_progress_path(&sample.bucket);
        // Read before the primary commit. An old lease owner cannot overwrite
        // a newer owner's summary after losing its cache CAS or sidecar CAS.
        let (previous, revision) = timeout(Duration::from_secs(2), read_display_progress(store, &path))
            .await
            .ok()?
            .ok()?;
        // A new physical executor may first enumerate its replica. Keep the
        // previous accepted sample and its original time until it has another
        // measured range. This is a whole observation, never an added delta.
        let sample = previous
            .filter(|previous| {
                sample.objects.is_none()
                    && previous.objects.is_some()
                    && previous.stage != BucketScanStage::Completed
                    && previous.matches(
                        &sample.bucket,
                        &sample.bucket_incarnation,
                        (sample.pool_index, sample.set_index),
                        movement_generation,
                    )
                    && previous.set_layout == sample.set_layout
                    && (previous.scanner_epoch, previous.scanner_cycle) <= (sample.scanner_epoch, sample.scanner_cycle)
            })
            .unwrap_or(sample);
        Some((path, serde_json::to_vec(&sample).ok()?, revision))
    }

    pub(super) async fn persist_display_progress<S: ScannerObjectIO + ScannerConfigObjectDelete>(
        store: Arc<S>,
        prepared: Option<(String, Vec<u8>, DataUsageCacheRevision)>,
        expected_epoch: Option<u64>,
    ) {
        let (Some((path, data, revision)), Some(epoch)) = (prepared, expected_epoch) else {
            return;
        };
        // A diagnostic write failure must not invalidate a durable checkpoint.
        // The next admitted checkpoint retries, with no request-path S3 listing.
        let _ = timeout(
            Duration::from_secs(2),
            crate::save_config_with_publication_admission_for_epoch(store, &path, data, revision.preconditions(), epoch),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ReleaseWorker(Option<std::sync::mpsc::Sender<()>>);

    impl Drop for ReleaseWorker {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn display_progress_extraction_releases_movement_guard_before_cpu_work() {
        let movement = Arc::new(tokio::sync::RwLock::new(()));
        let admission = crate::ScannerDataUsagePublicationAdmission::fenced(movement.clone().read_owned().await, 0);
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let release = ReleaseWorker(Some(release));
        let task = tokio::spawn(extract_progress(
            admission,
            Arc::new(Semaphore::new(1)),
            tokio::time::Instant::now() + Duration::from_secs(5),
            move |_| {
                let _ = started.send(());
                blocked
                    .recv_timeout(Duration::from_secs(5))
                    .expect("bounded test worker release");
                None
            },
        ));
        entered.await.expect("CPU projection started");
        assert!(
            movement.try_write().is_ok(),
            "a running diagnostic projection must not hold movement admission"
        );
        drop(release);
        assert!(task.await.expect("projection task").is_none());
    }

    #[tokio::test]
    async fn display_progress_timeout_keeps_detached_cpu_work_bounded() {
        let movement = Arc::new(tokio::sync::RwLock::new(()));
        let workers = Arc::new(Semaphore::new(1));
        let (started, entered) = tokio::sync::oneshot::channel();
        let (finished, stopped) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let release = ReleaseWorker(Some(release));
        let admission = crate::ScannerDataUsagePublicationAdmission::fenced(movement.clone().read_owned().await, 0);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let first = tokio::spawn(extract_progress(admission, workers.clone(), deadline, move |cancel| {
            let _ = started.send(());
            blocked
                .recv_timeout(Duration::from_secs(5))
                .expect("bounded test worker release");
            let _ = finished.send(cancel.is_cancelled());
            None
        }));
        entered.await.expect("CPU work entered");
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(first.await.expect("expired waiter").is_none());
        assert!(
            movement.try_write().is_ok(),
            "expired diagnostics cannot block movement or primary publication"
        );
        assert_eq!(workers.available_permits(), 0, "the detached worker retains its resource admission");

        let admission = crate::ScannerDataUsagePublicationAdmission::fenced(movement.clone().read_owned().await, 0);
        let next_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let queued = tokio::spawn(extract_progress(admission, workers.clone(), next_deadline, |_| {
            panic!("an expired waiter must not queue another CPU worker")
        }));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(queued.await.expect("second expired waiter").is_none());
        drop(release);
        tokio::time::resume();
        assert!(stopped.await.expect("worker stopped after cancellation"));
        let permit = timeout(Duration::from_secs(5), workers.acquire_owned())
            .await
            .expect("worker releases its permit")
            .expect("worker gate remains open");
        drop(permit);
    }

    #[test]
    fn display_progress_cpu_work_cannot_starve_native_io_workers() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("runtime with one native I/O worker");
        runtime.block_on(async {
            let (started, entered) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let release = ReleaseWorker(Some(release));
            let task = tokio::spawn(extract_progress(
                crate::ScannerDataUsagePublicationAdmission::unfenced(),
                Arc::new(Semaphore::new(1)),
                tokio::time::Instant::now() + Duration::from_secs(5),
                move |_| {
                    let _ = started.send(());
                    blocked
                        .recv_timeout(Duration::from_secs(5))
                        .expect("bounded diagnostic worker");
                    None
                },
            ));
            entered.await.expect("diagnostic CPU worker started");
            let native_io = timeout(Duration::from_secs(1), tokio::task::spawn_blocking(|| 7)).await;
            drop(release);
            assert!(task.await.expect("released diagnostic task").is_none());
            assert_eq!(
                native_io
                    .expect("diagnostics must leave native I/O available")
                    .expect("native job"),
                7
            );
        });
    }

    #[test]
    fn display_progress_input_honors_cancellation_and_deadline() {
        use std::io::Read;
        let cancel = CancellationToken::new();
        let mut reader = ProgressInput {
            chunks: vec![Bytes::from_static(b"a"), Bytes::new(), Bytes::from_static(b"bc")].into(),
            cancel: cancel.clone(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        };
        let mut bytes = [0; 1];
        assert_eq!(reader.read(&mut bytes).expect("first shared chunk"), 1);
        assert_eq!(bytes, *b"a");
        cancel.cancel();
        assert_eq!(
            reader.read(&mut bytes).expect_err("cancelled decode").kind(),
            std::io::ErrorKind::TimedOut
        );
        reader.cancel = CancellationToken::new();
        reader.deadline = tokio::time::Instant::now();
        assert_eq!(reader.read(&mut bytes).expect_err("expired decode").kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn display_progress_decodes_shared_chunks_without_changing_usage() {
        let mut cache = checkpoint();
        cache.info.snapshot_complete = true;
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                size: 33,
                ..Default::default()
            },
        );
        let expected = cache.display_scan_progress(0).expect("complete reference sample");
        let bytes = cache.marshal_msg().expect("encode checkpoint");
        let mut chunks = std::collections::VecDeque::new();
        for part in bytes.chunks(7) {
            chunks.push_back(Bytes::new());
            chunks.push_back(Bytes::copy_from_slice(part));
        }
        let decoded: DataUsageCache = rmp_serde::from_read(ProgressInput {
            chunks,
            cancel: CancellationToken::new(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        })
        .expect("decode across fragmented and empty chunks");
        assert_eq!(decoded.display_scan_progress(0), Some(expected));
    }

    fn checkpoint() -> DataUsageCache {
        let mut cache = DataUsageCache::default();
        cache.prepare_bucket_checkpoint(
            "bucket",
            1,
            1,
            DataUsageCacheSource::new(0, 0),
            DataUsageScanPlanDigest([2; 32]),
            DataUsageScanIdentity {
                version: 1,
                bucket_incarnation: uuid::Uuid::new_v4(),
                set_layout: DataUsageScanPlanDigest([3; 32]),
                publication_epoch: 0,
                tier_registry_generation: 0,
                scan_mode: HealScanMode::Normal,
            },
        );
        cache.info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10));
        cache.info.scan_directory_source = Some("source".into());
        cache
    }

    #[test]
    fn display_progress_frontier_resealing_and_cold_source_handoff_preserve_time() {
        let mut cache = checkpoint();
        cache.replace("bucket", "", DataUsageEntry::default());
        cache.replace(
            "bucket/a",
            "bucket",
            DataUsageEntry {
                objects: 1,
                size: 11,
                ..Default::default()
            },
        );
        cache.seal_scan_frontier(Some("bucket/a")).expect("seal measured frontier");
        cache.info.scan_coverage_measured_at = Some(10_000);
        cache.info.last_update = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(70));
        cache.seal_scan_frontier(Some("bucket/a")).expect("reseal unchanged frontier");
        assert_eq!(cache.display_scan_progress(0).expect("unchanged observation").updated_at, 10_000);
        let endpoints = std::collections::HashSet::from(["source".to_owned(), "other".to_owned()]);
        cache.activate_directory_source("other".into(), &endpoints);
        cache.replace("bucket", "", DataUsageEntry::default());
        let bytes = cache.marshal_msg().expect("save inactive source with its measurement time");
        let mut cold = DataUsageCache::unmarshal(&bytes).expect("cold source handoff");
        cold.activate_directory_source("source".into(), &endpoints);
        let sample = cold.display_scan_progress(0).expect("restored measured frontier");
        assert_eq!((sample.objects, sample.size, sample.updated_at), (Some(1), Some(11), 10_000));
    }

    #[test]
    fn display_progress_legacy_frontier_cannot_borrow_a_new_checkpoint_time() {
        let mut cache = checkpoint();
        cache.replace("bucket", "", DataUsageEntry::default());
        cache.replace(
            "bucket/a",
            "bucket",
            DataUsageEntry {
                objects: 1,
                size: 11,
                ..Default::default()
            },
        );
        cache.seal_scan_frontier(Some("bucket/a")).expect("legacy coverage");
        cache.info.scan_coverage_measured_at = None;
        let sample = cache.display_scan_progress(0).expect("enumeration remains observable");
        assert_eq!((sample.objects, sample.size), (None, None));
    }

    #[test]
    fn display_progress_excludes_old_tail_and_uncommitted_ancestors() {
        let mut cache = checkpoint();
        let mut root = DataUsageEntry {
            objects: 99,
            size: 999,
            ..Default::default()
        };
        root.add_child(&hash_path("bucket/a"));
        root.add_child(&hash_path("bucket/z"));
        cache.cache.insert("bucket".into(), root);
        cache.cache.insert(
            "bucket/a".into(),
            DataUsageEntry {
                objects: 3,
                size: 30,
                ..Default::default()
            },
        );
        cache.cache.insert(
            "bucket/z".into(),
            DataUsageEntry {
                objects: 80,
                size: 800,
                ..Default::default()
            },
        );
        cache.seal_scan_frontier(Some("bucket/a")).expect("seal accepted prefix");
        let sample = cache.display_scan_progress(0).expect("accepted progress");
        assert_eq!((sample.objects, sample.size), (Some(3), Some(30)));
        assert_eq!(sample.stage, BucketScanStage::Scanning);
        let encoded = rmp_serde::to_vec(&cache).expect("encode checkpoint");
        let mut restarted: DataUsageCache = rmp_serde::from_slice(&encoded).expect("cold decode checkpoint");
        restarted.info.leader_epoch += 1;
        assert_eq!(restarted.display_scan_progress(0).expect("handoff progress").objects, Some(3));
        assert_eq!(restarted.display_scan_progress(0).expect("repeated sample").size, Some(30));
    }

    #[test]
    fn display_progress_rejects_invalid_receipt_and_preserves_enumerating_unknown() {
        let mut cache = checkpoint();
        cache.cache.insert(
            "bucket".into(),
            DataUsageEntry {
                objects: 999,
                size: 999,
                ..Default::default()
            },
        );
        let sample = cache.display_scan_progress(0).expect("enumeration state");
        assert_eq!((sample.objects, sample.size), (None, None));
        assert_eq!(sample.stage, BucketScanStage::Enumerating);
        cache.info.snapshot_complete = true;
        let complete = cache.display_scan_progress(0).expect("complete traversal");
        assert_eq!((complete.objects, complete.size), (Some(999), Some(999)));
        assert_eq!(complete.stage, BucketScanStage::Completed);
        cache.info.snapshot_complete = false;
        cache.replace(
            "bucket/a",
            "bucket",
            DataUsageEntry {
                objects: 3,
                size: 30,
                ..Default::default()
            },
        );
        cache.seal_scan_frontier(Some("bucket/a")).expect("seal complete child");
        assert_eq!(cache.display_scan_progress(0).expect("verified prefix").objects, Some(3));
        cache.cache.get_mut("bucket/a").expect("accepted child").objects = 7;
        assert_eq!(cache.display_scan_progress(0).expect("corrupt receipt is unknown").objects, None);
    }
}
