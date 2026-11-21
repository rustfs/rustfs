// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use futures::StreamExt;
use rustfs_data_usage::{
    BUCKET_SCAN_PROGRESS_MAX_BYTES, BucketScanProgress, BucketScanProgressSource, aggregate_bucket_scan_progress,
    bucket_scan_progress_path,
};
use sha2::{Digest, Sha256};

/// Read compact checkpoint companions only for the caller's authorized buckets.
///
/// These diagnostics cannot seed usage caches or authorize quotas. All reads
/// share a two-second deadline, are cancelled when the request is dropped, and
/// never list S3 objects or load full scanner caches. Missing/corrupt sources
/// remain explicitly absent. Every request binds samples to current storage
/// incarnations, physical set layout, and the durable movement generation.
pub async fn load_bucket_scan_progress_for_admin(
    store: Arc<ECStore>,
    buckets: &[String],
    baseline: &DataUsageInfo,
) -> HashMap<String, BucketScanProgress> {
    let deadline = tokio::time::Instant::now() + ADMIN_USAGE_INCARNATION_TIMEOUT;
    tokio::time::timeout_at(deadline, load_progress(store, buckets, baseline))
        .await
        .unwrap_or_default()
}

async fn load_progress(store: Arc<ECStore>, buckets: &[String], baseline: &DataUsageInfo) -> HashMap<String, BucketScanProgress> {
    let Some(epoch) = store.scanner_data_usage_publication_epoch().await else {
        return HashMap::new();
    };
    let Some(movement_generation) = store.scanner_durable_data_movement_generation().await else {
        return HashMap::new();
    };
    let sets = store.all_set_disks();
    let mut expected = Vec::with_capacity(sets.len());
    for set in &sets {
        let (Ok(pool), Ok(index)) = (u64::try_from(set.pool_index), u64::try_from(set.set_index)) else {
            return HashMap::new();
        };
        expected.push((pool, index));
    }
    expected.sort_unstable();
    if expected.windows(2).any(|pair| pair[0] >= pair[1]) {
        return HashMap::new();
    }
    let mut work = futures::stream::iter(buckets.iter().cloned())
        .map(|bucket| {
            let store = store.clone();
            let sets = &sets;
            let expected = &expected;
            async move {
                let incarnation = store
                    .bucket_incarnation_id_from_disk(&bucket)
                    .await
                    .ok()
                    .filter(|id| !id.is_nil())?;
                let incarnation = incarnation.to_string();
                let path = bucket_scan_progress_path(&bucket);
                let mut reads = futures::stream::iter(sets.iter().cloned())
                    .map(|set| {
                        let bucket = bucket.clone();
                        let incarnation = incarnation.clone();
                        let path = path.clone();
                        async move {
                            let bytes =
                                crate::config::com::read_config_limited(set.clone(), &path, BUCKET_SCAN_PROGRESS_MAX_BYTES)
                                    .await
                                    .ok()?;
                            let sample: BucketScanProgressSource = serde_json::from_slice(&bytes).ok()?;
                            let source = (u64::try_from(set.pool_index).ok()?, u64::try_from(set.set_index).ok()?);
                            let disks = set.format.erasure.sets.get(set.set_index).filter(|disks| !disks.is_empty())?;
                            if set.format.id.is_nil() || disks.iter().any(uuid::Uuid::is_nil) {
                                return None;
                            }
                            let mut digest = Sha256::new();
                            digest.update(set.format.id.as_bytes());
                            for disk in disks {
                                digest.update(disk.as_bytes());
                            }
                            let layout: [u8; 32] = digest.finalize().into();
                            (sample.matches(&bucket, &incarnation, source, movement_generation) && sample.set_layout == layout)
                                .then_some(sample)
                        }
                    })
                    .buffer_unordered(4);
                let mut samples = Vec::new();
                while let Some(sample) = reads.next().await {
                    samples.extend(sample);
                }
                drop(reads);
                if let Some(certification) = baseline
                    .buckets_usage
                    .get(&bucket)
                    .and_then(|usage| usage.usage_certification)
                    && samples
                        .iter()
                        .all(|sample| (sample.scanner_epoch, sample.scanner_cycle) <= (certification.epoch, certification.cycle))
                {
                    return None;
                }
                Some((bucket, aggregate_bucket_scan_progress(samples, expected)?))
            }
        })
        .buffer_unordered(4);
    let mut progress = HashMap::new();
    while let Some(result) = work.next().await {
        if let Some((bucket, sample)) = result {
            progress.insert(bucket, sample);
        }
    }
    if store.scanner_data_usage_publication_epoch().await != Some(epoch)
        || store.scanner_durable_data_movement_generation().await != Some(movement_generation)
    {
        progress.clear();
    }
    progress
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::config::com::save_config;
    use crate::storage_api_contracts::bucket::MakeBucketOptions;
    use rustfs_data_usage::{BUCKET_SCAN_PROGRESS_VERSION, BucketScanStage};

    async fn fixture() -> (Vec<tempfile::TempDir>, Arc<ECStore>, BucketScanProgressSource) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket("progress", &MakeBucketOptions::default())
            .await
            .expect("create progress bucket");
        let incarnation = store
            .bucket_incarnation_id_from_disk("progress")
            .await
            .expect("current incarnation");
        let set = &store.pools[0].disk_set[0];
        let mut digest = Sha256::new();
        digest.update(set.format.id.as_bytes());
        for disk in &set.format.erasure.sets[set.set_index] {
            digest.update(disk.as_bytes());
        }
        let sample = BucketScanProgressSource {
            version: BUCKET_SCAN_PROGRESS_VERSION,
            bucket: "progress".into(),
            bucket_incarnation: incarnation.to_string(),
            pool_index: 0,
            set_index: 0,
            set_layout: digest.finalize().into(),
            publication_epoch: store.scanner_data_usage_publication_epoch().await.expect("publication epoch"),
            movement_generation: store.scanner_durable_data_movement_generation().await,
            scanner_epoch: 9,
            scanner_cycle: 11,
            updated_at: 1000,
            stage: BucketScanStage::Scanning,
            objects: Some(7),
            size: Some(71),
        };
        (dirs, store, sample)
    }

    async fn save(store: &ECStore, sample: &BucketScanProgressSource) {
        save_config(
            store.pools[0].disk_set[0].clone(),
            &bucket_scan_progress_path("progress"),
            serde_json::to_vec(sample).expect("encode display sample"),
        )
        .await
        .expect("persist display sample on its source set");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn display_progress_reloads_from_shared_storage_without_promoting_usage() {
        fn assert_send<T: Send>(value: T) -> T {
            value
        }
        let (_dirs, store, mut sample) = fixture().await;
        save(&store, &sample).await;
        let baseline = DataUsageInfo::default();
        for _ in 0..3 {
            let progress = assert_send(load_bucket_scan_progress_for_admin(store.clone(), &["progress".into()], &baseline)).await;
            assert_eq!((progress["progress"].objects, progress["progress"].size), (Some(7), Some(71)));
            assert_eq!(progress["progress"].sources_observed, 1);
            assert!(baseline.buckets_usage.is_empty(), "display samples never seed exact usage");
            assert!(!baseline.is_complete_bucket_usage_snapshot());
        }
        // Another node/restarted reader may have a different admission epoch
        // and wakeup count without any persisted movement-state change.
        let gate = store.ctx.data_movement_operation_gate();
        let permit = gate.write().await;
        store.ctx.advance_data_movement_operation_epoch();
        drop(permit);
        assert_ne!(store.scanner_data_usage_publication_epoch().await, Some(sample.publication_epoch));
        assert_ne!(Some(store.scanner_data_movement_generation()), sample.movement_generation);
        let other_reader = load_bucket_scan_progress_for_admin(store.clone(), &["progress".into()], &baseline).await;
        assert_eq!(
            other_reader["progress"].objects,
            Some(7),
            "local reader counters cannot hide shared progress"
        );
        sample.scanner_epoch += 1;
        sample.scanner_cycle += 1;
        sample.objects = Some(9);
        sample.size = Some(91);
        save(&store, &sample).await;
        let after_handoff = load_bucket_scan_progress_for_admin(store.clone(), &["progress".into()], &baseline).await;
        assert_eq!((after_handoff["progress"].objects, after_handoff["progress"].size), (Some(9), Some(91)));
        assert!(
            load_bucket_scan_progress_for_admin(store, &[], &baseline).await.is_empty(),
            "no unauthorized bucket probes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn display_progress_rejects_retired_identity_layout_epoch_and_corrupt_samples() {
        let (_dirs, store, sample) = fixture().await;
        for invalid in 0..6 {
            let mut stale = sample.clone();
            match invalid {
                0 => stale.bucket_incarnation = uuid::Uuid::new_v4().to_string(),
                1 => stale.set_layout = [0; 32],
                2 => stale.movement_generation = Some(stale.movement_generation.expect("shared generation") + 1),
                3 => stale.version += 1,
                4 => stale.movement_generation = None,
                _ => stale.size = None,
            }
            save(&store, &stale).await;
            assert!(
                load_bucket_scan_progress_for_admin(store.clone(), &["progress".into()], &DataUsageInfo::default())
                    .await
                    .is_empty()
            );
        }
        let path = bucket_scan_progress_path("progress");
        for bytes in [b"invalid json".to_vec(), vec![b' '; BUCKET_SCAN_PROGRESS_MAX_BYTES + 1]] {
            save_config(store.pools[0].disk_set[0].clone(), &path, bytes)
                .await
                .expect("persist malformed diagnostic fixture");
            assert!(
                load_bucket_scan_progress_for_admin(store.clone(), &["progress".into()], &DataUsageInfo::default())
                    .await
                    .is_empty()
            );
        }
    }
}
