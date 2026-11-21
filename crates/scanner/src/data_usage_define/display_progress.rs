// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use rustfs_data_usage::{
    BUCKET_SCAN_PROGRESS_MAX_BYTES, BUCKET_SCAN_PROGRESS_VERSION, BucketScanProgressSource, BucketScanStage,
    bucket_scan_progress_path,
};
use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;

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
        let mut accepted = completed.as_ref().map(|entry| (entry.objects, entry.size));
        if completed.is_none() {
            let frontier = self.validated_scan_frontier();
            let mut replicas = BTreeMap::<&str, Vec<_>>::new();
            for checkpoint in &self.info.scan_directory_work {
                if let Some((coverage, entry)) = checkpoint.display_usage(&self.info) {
                    replicas.entry(checkpoint.endpoint()).or_default().push((coverage, entry));
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
                for (coverage, entry) in &receipts {
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
                    selected.push(coverage);
                }
                if Some(endpoint) == self.info.scan_directory_source.as_deref()
                    && let Some(through) = frontier
                {
                    // A validated receipt signs this prefix. Sum each scalar
                    // once, excluding the ranges represented by compacted work.
                    self.checked_flatten_complete_scope(&self.info.name)?;
                    for (path, entry) in &self.cache {
                        if scan_frontier_covers_path(path, through) && !selected.iter().any(|proof| proof.covers(path)) {
                            let (objects, size) = subtotal.unwrap_or_default();
                            subtotal = Some((objects.checked_add(entry.objects)?, size.checked_add(entry.size)?));
                        }
                    }
                }
                // Replicas overlap. Select one sample; never sum or append them.
                if subtotal.is_some_and(|sample| accepted.is_none_or(|previous| sample > previous)) {
                    accepted = subtotal;
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
            updated_at,
            stage: if completed.is_some() {
                BucketScanStage::Completed
            } else if accepted.is_some() {
                BucketScanStage::Scanning
            } else {
                BucketScanStage::Enumerating
            },
            objects: accepted.map(|(objects, _)| u64::try_from(objects)).transpose().ok()?,
            size: accepted.map(|(_, size)| u64::try_from(size)).transpose().ok()?,
        })
    }

    pub(super) async fn prepare_display_progress<S: ScannerObjectIO + ScannerConfigObjectDelete>(
        &self,
        store: Arc<S>,
        expected_epoch: Option<u64>,
    ) -> Option<(String, Vec<u8>, DataUsageCacheRevision)> {
        if self.info.name.is_empty() || self.info.name == DATA_USAGE_ROOT || self.info.scan_identity.is_none() {
            return None;
        }
        let expected_epoch = expected_epoch?;
        let admission = scanner_publication_admission_for_epoch(store.clone(), expected_epoch).await?;
        let movement_generation = store.scanner_data_usage_movement_generation().await?;
        let sample = self.display_scan_progress(movement_generation)?;
        drop(admission);
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
