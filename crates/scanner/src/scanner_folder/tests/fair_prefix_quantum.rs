// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::enumeration_restart::{install_raw_entry_observer, observed_raw_entries, seed_flat_bucket};
use super::*;
use crate::scanner_budget::ScannerCycleBudgetConfig;

async fn open_fixture_disk(disk_root: &std::path::Path) -> Arc<Disk> {
    let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("fixture endpoint");
    new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("open synthetic disk")
}

/// A count budget is the operator's total work limit. It must also stop the
/// first listing of a directory far wider than that budget, instead of letting
/// one read_dir pass finish before the first object is charged.
#[tokio::test]
#[serial]
async fn fair_prefix_quantum_count_budget_bounds_the_first_directory_build() {
    const OBJECTS: usize = 300;
    const FIRST_BUILD_FLOOR: u64 = 128;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let cache = seed_flat_bucket(&disk_root, OBJECTS).await;
    let disk = open_fixture_disk(&disk_root).await;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(8),
            ..Default::default()
        },
    );
    let _observation = install_raw_entry_observer(disk.path());

    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![disk.clone()],
        disk,
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;

    assert!(
        matches!(result, Err(ScannerError::PartialCache(_))),
        "an exhausted first build must return recoverable progress"
    );
    assert!(
        observed_raw_entries() <= FIRST_BUILD_FLOOR,
        "first build read {} raw entries under an 8-object budget",
        observed_raw_entries()
    );
}

struct RestartRound {
    outcome: DataUsageCache,
    finished: bool,
    observed_entries: u64,
}

/// One scanner lifetime: the input was decoded from persisted bytes, and the
/// result is encoded and decoded again before the next round may use it.
async fn scan_one_restart_round(
    disk_root: &std::path::Path,
    cache: DataUsageCache,
    config: ScannerCycleBudgetConfig,
) -> RestartRound {
    let disk = open_fixture_disk(disk_root).await;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, config);
    let _observation = install_raw_entry_observer(disk.path());
    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![disk.clone()],
        disk,
        cache.clone(),
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;
    let observed_entries = observed_raw_entries();
    let (returned, finished) = match result {
        Ok(cache) => (cache, true),
        Err(ScannerError::PartialCache(cache)) => (*cache, false),
        Err(error) => panic!("a budget stop must keep recoverable progress: {error}"),
    };
    let encoded = returned.marshal_msg().expect("encode returned cache");
    RestartRound {
        outcome: DataUsageCache::unmarshal(&encoded).expect("decode returned cache"),
        finished,
        observed_entries,
    }
}

async fn run_until_finished(
    disk_root: &std::path::Path,
    mut cache: DataUsageCache,
    config: ScannerCycleBudgetConfig,
    rounds: usize,
) -> DataUsageCache {
    for round in 0..rounds {
        let result = scan_one_restart_round(disk_root, cache, config).await;
        if let Some(limit) = config.max_entries {
            assert!(result.observed_entries <= limit, "round {round} read {} entries", result.observed_entries);
        }
        cache = result.outcome;
        if result.finished {
            assert!(cache.info.snapshot_complete, "a finished sweep must certify its bucket");
            return cache;
        }
    }
    panic!("the sweep did not finish in {rounds} restarts");
}

/// Both a wide bucket root and a wide nested prefix must keep their indexes:
/// losing either one recharges its replay and a fixed budget stops advancing.
#[tokio::test]
#[serial]
async fn fair_prefix_quantum_nested_wide_directories_converge_across_restarts() {
    const WIDTH: usize = 40;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let cache = seed_flat_bucket(&disk_root, WIDTH).await;
    for index in 0..WIDTH {
        let object = format!("wide/nested-{index:04}");
        let version = Uuid::from_u128(u128::try_from(index).expect("fixture index") + 1000);
        let bytes = metadata_for_object_version("bucket", &object, Some(version));
        write_test_object_metadata_bytes(&disk_root, "bucket", &object, &bytes).await;
    }
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(8),
        max_entries: Some(8),
        ..Default::default()
    };
    // 41 root and 40 prefix entries at 8 per round, then 80 objects at the 7
    // that survive each object-budget stop. Rebuilding a dropped index costs
    // a full prefix enumeration per classification round and overruns this.
    const ROUND_BOUND: usize = 6 + 5 + 12 + 2;
    let finished = run_until_finished(&disk_root, cache, config, ROUND_BOUND).await;
    let totals = finished.checked_flatten("bucket").expect("bucket root");
    let expected = u64::try_from(WIDTH * 2).expect("fixture size");
    assert_eq!((totals.objects as u64, totals.versions as u64), (expected, expected));
    assert!(finished.info.scan_raw_enumeration_page_index.is_none());
    assert!(finished.info.scan_raw_enumeration_path_page_indexes.is_empty());
}

/// A name-byte share stops a listing the same way an entry share does, and the
/// bytes kept for the next restart grow only by what each round charged.
#[tokio::test]
#[serial]
async fn fair_prefix_quantum_name_bytes_bound_each_round_and_its_checkpoint_growth() {
    const OBJECTS: usize = 64;
    const NAME_BYTES_PER_ROUND: u64 = 88;
    const INDEX_BYTES_PER_NAME_BYTE: usize = 16;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&disk_root, OBJECTS).await;
    let config = ScannerCycleBudgetConfig {
        max_entry_name_bytes: Some(NAME_BYTES_PER_ROUND),
        ..Default::default()
    };
    let mut indexed = 0;
    let mut encoded_before = cache.marshal_msg().expect("encode initial cache").len();
    for _ in 0..8 {
        let result = scan_one_restart_round(&disk_root, cache, config).await;
        assert!(!result.finished);
        assert_eq!(result.observed_entries, 8, "each entry name in this fixture is 11 bytes");
        cache = result.outcome;
        let committed = cache
            .validated_raw_enumeration_page_index()
            .expect("durable index")
            .committed_entries()
            .expect("committed entries")
            .len();
        assert_eq!(committed, indexed + 8, "every charged entry must become durable");
        indexed = committed;
        let encoded = cache.marshal_msg().expect("encode cache").len();
        let growth = encoded.saturating_sub(encoded_before);
        assert!(
            growth <= usize::try_from(NAME_BYTES_PER_ROUND).expect("fixture size") * INDEX_BYTES_PER_NAME_BYTE,
            "checkpoint grew {growth} bytes in one round"
        );
        encoded_before = encoded;
    }
}

/// Stopping for an entry share must leave the same exact totals a full sweep
/// produces, and a Deep request must not inherit Normal raw coverage.
#[tokio::test]
#[serial]
async fn fair_prefix_quantum_budget_stop_keeps_progress_without_certifying_it() {
    const OBJECTS: usize = 24;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let cache = seed_flat_bucket(&disk_root, OBJECTS).await;
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(4),
        max_entries: Some(4),
        ..Default::default()
    };
    let round = scan_one_restart_round(&disk_root, cache, config).await;
    assert!(!round.finished);
    assert!(!round.outcome.info.snapshot_complete, "budget stops are progress-only");
    assert!(round.outcome.info.scan_plan_digest.is_none(), "a partial sweep must not be reusable");
    assert!(round.outcome.has_scan_position(), "the saved cache must carry the resume position");

    let mut deep = round.outcome.clone();
    let mut identity = deep.info.scan_identity.expect("checkpoint identity");
    identity.scan_mode = HealScanMode::Deep;
    let plan = crate::data_usage_define::DataUsageScanPlanDigest([31; 32]);
    let source = crate::data_usage_define::DataUsageCacheSource::new(0, 0);
    deep.prepare_bucket_checkpoint("bucket", 2, 0, source, plan, identity);
    assert!(!deep.has_scan_position(), "Normal raw coverage must not carry into a Deep sweep");
    assert!(deep.info.scan_raw_enumeration_path_page_indexes.is_empty());

    let finished = run_until_finished(&disk_root, round.outcome, config, 16).await;
    let totals = finished.checked_flatten("bucket").expect("bucket root");
    let expected = u64::try_from(OBJECTS).expect("fixture size");
    assert_eq!((totals.objects as u64, totals.versions as u64), (expected, expected));
}
