// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::cache_cost::same_cache;
use super::*;
use crate::scanner_budget::{ScannerCycleBudget, ScannerCycleBudgetConfig};
use crate::scanner_folder::scan_data_folder;
use crate::{DiskOption, Endpoint, SCANNER_SLEEPER, ScannerError, new_disk};
use rustfs_filemeta::{FileInfo, FileMeta};
use tokio_util::sync::CancellationToken;

const OBJECTS: usize = 24;
const HISTORY: &str = "bucket/z-history";
const CAP: usize = 4;

async fn scan_trace() -> Vec<DataUsageCache> {
    let temp = tempfile::tempdir().expect("fixture disk");
    for i in 0..OBJECTS {
        let key = format!("object-{i:04}");
        let dir = temp.path().join("bucket").join(&key);
        tokio::fs::create_dir_all(&dir).await.expect("object directory");
        let mut info = FileInfo::new(&key, 4, 2);
        info.volume = "bucket".into();
        info.name = key;
        info.version_id = Some(uuid::Uuid::from_u128(u128::try_from(i + 1).expect("fixture UUID")));
        info.versioned = true;
        info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixture modification time"));
        info.size = 1;
        let mut meta = FileMeta::new();
        meta.add_version(info).expect("fixture version");
        tokio::fs::write(dir.join("xl.meta"), meta.marshal_msg().expect("fixture metadata"))
            .await
            .expect("write fixture metadata");
    }
    let endpoint = Endpoint::try_from(temp.path().to_str().expect("fixture path")).expect("local endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("local disk");
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint(
        "bucket",
        1,
        0,
        DataUsageCacheSource::new(0, 0),
        TEST_PLAN_DIGEST,
        DataUsageScanIdentity {
            version: 1,
            bucket_incarnation: uuid::Uuid::from_u128(1),
            set_layout: TEST_PLAN_DIGEST,
            publication_epoch: 1,
            tier_registry_generation: 0,
            scan_mode: HealScanMode::Normal,
        },
    );
    cache.info.skip_healing = true;
    cache.replace("bucket", "", DataUsageEntry::default());
    let mut trace = vec![cache.clone()];
    for _ in 0..32 {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(4),
                max_entries: Some(4),
                ..Default::default()
            },
        );
        let result = Box::pin(scan_data_folder(
            budget.token(),
            budget,
            vec![disk.clone()],
            disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        ))
        .await;
        cache = match result {
            Ok(cache) => cache,
            Err(ScannerError::PartialCache(cache)) => *cache,
            other => panic!("expected a bounded scan observation: {other:?}"),
        };
        let complete = cache.info.snapshot_complete;
        trace.push(cache.clone());
        if complete {
            let total = cache.checked_flatten("bucket").expect("walked bucket");
            assert_eq!((total.objects, total.versions, total.size), (OBJECTS, OBJECTS, OBJECTS));
            assert!(
                trace
                    .iter()
                    .any(|c| !c.info.snapshot_complete && c.validated_raw_enumeration_page_index().is_some())
            );
            return trace;
        }
    }
    panic!("bounded scanner did not complete the fixture");
}

fn with_history_link(observation: &DataUsageCache) -> DataUsageCache {
    let mut cache = observation.clone();
    let root = cache.cache.get_mut("bucket").expect("observed bucket");
    // Its original aggregate remains local usage. A parent with an added child cannot be compacted.
    root.compacted = false;
    root.add_child(&hash_path(HISTORY));
    cache
}

fn with_history(observation: &DataUsageCache, entries: usize) -> DataUsageCache {
    let mut cache = with_history_link(observation);
    for i in 0..entries {
        cache.replace(
            &format!("{HISTORY}/{i:04}"),
            HISTORY,
            DataUsageEntry {
                objects: 1,
                size: 1,
                ..Default::default()
            },
        );
    }
    cache
}

// The experiment knows which subtree is frozen, not which fields changed inside the walk.
// Charge the entire active map (including ancestor child sets) and all checkpoint metadata.
fn segment(previous: &DataUsageCache, next: &DataUsageCache) -> Vec<u8> {
    let removed: Vec<_> = previous.cache.keys().filter(|key| !next.cache.contains_key(*key)).collect();
    rmp_serde::to_vec(&(removed, with_history_link(next))).expect("encode active entries and removals")
}

// For this single scope, (base revision, last revision) addresses consecutive numbered blobs.
fn replay(root: &[u8], blobs: &HashMap<usize, Vec<u8>>) -> DataUsageCache {
    let (base, last): (usize, usize) = rmp_serde::from_slice(root).expect("decode root");
    assert!(base <= last && last - base <= CAP);
    let mut cache = DataUsageCache::unmarshal(&blobs[&base]).expect("decode base");
    for rev in base + 1..=last {
        let (removed, active): (Vec<String>, DataUsageCache) = rmp_serde::from_slice(&blobs[&rev]).expect("decode segment");
        for key in removed {
            cache.cache.remove(&key).expect("tombstoned active key");
        }
        cache.cache.extend(active.cache);
        cache.info = active.info;
    }
    cache
}

#[tokio::test]
#[serial_test::serial]
async fn checkpoint_segments_replay_scanner_observations() {
    let trace = tokio::time::timeout(Duration::from_secs(60), Box::pin(scan_trace()))
        .await
        .expect("bounded scanner fixture");
    assert!(trace.len() > CAP + 1, "must cross a compaction boundary");
    let deltas: Vec<_> = trace.windows(2).map(|pair| segment(&pair[0], &pair[1])).collect();
    for history in [32, 320] {
        let seed = with_history(&trace[0], history).marshal_msg().expect("initial base");
        let seed_bytes = seed.len();
        let mut base = 0;
        let mut blobs = HashMap::from([(base, seed)]);
        let mut root = rmp_serde::to_vec(&(base, base)).expect("initial root");
        let seed_root_bytes = root.len();
        let mut full_writes = Vec::new();
        let mut segment_writes = Vec::new();
        let mut compact_reads = 0;
        let mut compact_writes = 0;
        let mut compact_revisions = Vec::new();
        for (i, delta) in deltas.iter().enumerate() {
            let rev = i + 1;
            let expected = with_history(&trace[rev], history);
            let full = expected.marshal_msg().expect("production full encode");
            same_cache(&DataUsageCache::unmarshal(&full).expect("full reload"), &expected);
            full_writes.push(full.len());
            if i - base == CAP {
                compact_reads += root.len() + blobs.values().map(Vec::len).sum::<usize>();
                let merged = replay(&root, &blobs).marshal_msg().expect("compacted full encode");
                compact_writes += merged.len();
                compact_revisions.push(rev);
                base = i;
                blobs = HashMap::from([(base, merged)]);
            }
            blobs.insert(rev, delta.clone());
            root = rmp_serde::to_vec(&(base, rev)).expect("encode bounded root");
            segment_writes.push(delta.len() + root.len());
            let recovered = replay(&root, &blobs);
            same_cache(&recovered, &expected);
            assert_eq!(recovered.validated_scan_frontier(), trace[rev].validated_scan_frontier());
            let real = trace[rev].checked_flatten("bucket").expect("observed root");
            let joined = recovered
                .checked_flatten("bucket")
                .expect("connected history and observation");
            assert_eq!(
                (joined.objects, joined.versions, joined.size),
                (history + real.objects, real.versions, history + real.size)
            );
        }
        let replay_bytes = root.len() + blobs.values().map(Vec::len).sum::<usize>();
        let export = replay(&root, &blobs).marshal_msg().expect("full-format export");
        same_cache(
            &DataUsageCache::unmarshal(&export).expect("export reload"),
            &with_history(trace.last().expect("final snapshot"), history),
        );
        println!(
            "CHECKPOINT_CODEC_COST {}",
            serde_json::json!({
                "history_entries": history, "objects_scanned": OBJECTS, "chain_cap": CAP, "seed_bytes": seed_bytes,
                "seed_root_bytes": seed_root_bytes, "full_checkpoint_bytes": full_writes, "segment_plus_root_bytes": segment_writes,
                "compaction_revisions": compact_revisions, "compaction_read_bytes": compact_reads, "compaction_write_bytes": compact_writes,
                "full_total_write_bytes": seed_bytes + full_writes.iter().sum::<usize>(),
                "segment_total_write_bytes": seed_bytes + seed_root_bytes + segment_writes.iter().sum::<usize>() + compact_writes,
                "final_segment_recovery_read_bytes": replay_bytes, "full_export_bytes": export.len()
            })
        );
    }
}
