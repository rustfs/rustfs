// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::enumeration_restart::{install_raw_entry_observer, observed_raw_entries, seed_flat_bucket};
use super::*;
use crate::scanner_budget::ScannerCycleBudgetConfig;
use std::cell::Cell;
use std::path::PathBuf;

const PHYSICAL_READ_COST: Duration = Duration::from_secs(60);

tokio::task_local! {
    static SLOW_DIRECTORY: (PathBuf, Cell<usize>);
}

pub(in crate::scanner_folder) fn is_slow_directory(path: &str) -> bool {
    SLOW_DIRECTORY.try_with(|(directory, _)| directory.as_path() == std::path::Path::new(path)) == Ok(true)
}

/// Charge every read attempt (including replay and EOF) before entries leave
/// the native buffer, so cancelling a simulated slow read cannot skip a name.
pub(in crate::scanner_folder) async fn charge_physical_read() {
    SLOW_DIRECTORY.with(|(_, reads)| reads.set(reads.get() + 1));
    tokio::time::advance(PHYSICAL_READ_COST).await;
}

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

#[tokio::test(start_paused = true)]
#[serial]
async fn wide_directory_runtime_budget_reaches_eof() {
    const WIDTH: usize = 48;
    const ROUNDS: usize = 16;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&disk_root, WIDTH).await;
    let disk = open_fixture_disk(&disk_root).await;
    let mut anchor = FileInfo::new("large", 4, 2);
    anchor.volume = "bucket".to_owned();
    anchor.version_id = Some(Uuid::from_u128(1000));
    anchor.versioned = true;
    anchor.mod_time = Some(OffsetDateTime::now_utc());
    anchor.size = 4096;
    let mut metadata = FileMeta::new();
    metadata.add_version(anchor).expect("large fixture object");
    write_test_object_metadata_bytes(&disk_root, "bucket", "large", &metadata.marshal_msg().expect("encode large object")).await;
    let parent = CancellationToken::new();
    let expected = (WIDTH + 1, WIDTH + 4096);
    let scan = async {
        for prior_timeouts in 0..ROUNDS {
            let budget = ScannerCycleBudget::new_with_progress_tracking(
                &parent,
                ScannerCycleBudgetConfig {
                    max_duration: Some(8 * PHYSICAL_READ_COST),
                    ..Default::default()
                },
            );
            let result = scan_data_folder(
                budget.token(),
                budget.clone(),
                vec![disk.clone()],
                disk.clone(),
                cache.clone(),
                None,
                HealScanMode::Normal,
                SCANNER_SLEEPER.clone(),
            )
            .await;
            match result {
                Ok(complete) => {
                    assert!(prior_timeouts >= 2, "the wide directory must span multiple actual runtime deadlines");
                    assert!(complete.info.snapshot_complete);
                    let totals = complete.checked_flatten("bucket").expect("complete bucket root");
                    assert_eq!((totals.objects, totals.size), expected);
                    return;
                }
                Err(ScannerError::PartialCache(partial)) => {
                    assert!(!partial.info.snapshot_complete);
                    cache = DataUsageCache::unmarshal(&partial.marshal_msg().expect("persist partial")).expect("reload partial");
                }
                Err(ScannerError::Other(message)) if budget.token().is_cancelled() && message == "Operation cancelled" => {}
                Err(error) => panic!("unexpected scan failure: {error}"),
            }
            assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Runtime));
        }
        let reads = SLOW_DIRECTORY.with(|(_, count)| count.get());
        let totals = cache.checked_flatten("bucket").expect("last partial root");
        panic!(
            "wide directory still incomplete after {ROUNDS} fixed runtime deadlines and {reads} physical reads: {} objects, {} bytes",
            totals.objects, totals.size
        );
    };
    SLOW_DIRECTORY.scope((disk.path().join("bucket"), Cell::new(0)), scan).await;
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
    scan_one_replica_round(disk_root, None, cache, config).await
}

async fn scan_one_replica_round(
    disk_root: &std::path::Path,
    peer: Option<&std::path::Path>,
    cache: DataUsageCache,
    config: ScannerCycleBudgetConfig,
) -> RestartRound {
    let disk = open_fixture_disk(disk_root).await;
    let mut disks = vec![disk.clone()];
    if let Some(peer) = peer {
        disks.push(open_fixture_disk(peer).await);
    }
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, config);
    let _observation = install_raw_entry_observer(disk.path());
    let checkpoint_endpoints = cache.info.skip_healing.then(|| {
        use crate::storage_api::owner::EcstoreDiskAPI;
        let endpoints = disks.iter().map(|disk| disk.endpoint().to_string()).collect();
        disks.clear();
        endpoints
    });
    let result = scan_data_folder_scoped(
        budget.token(),
        budget.clone(),
        disks,
        disk,
        cache.clone(),
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
        None,
        checkpoint_endpoints.as_ref(),
        None,
        Some(Arc::new(crate::scanner_io::ScannerLocalStop::default())),
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

pub(super) async fn run_until_finished(
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

#[tokio::test]
#[serial]
async fn cold_compacted_directory_totals_progress_after_every_executor_restart() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&root, 40).await;
    let budget = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    super::super::TEST_FORCE_COMPACTION_WIDTH
        .scope(16, async {
            let mut partial_counts = Vec::new();
            for round in 0..16 {
                let step = scan_one_restart_round(&root, cache.clone(), budget).await;
                if !step.finished {
                    assert!(
                        step.outcome
                            .durable_scan_progress()
                            .advanced_from(&cache.durable_scan_progress()),
                        "each cold compacted round must earn a coordinator service turn, round {round}"
                    );
                }
                cache = step.outcome;
                let total = cache.checked_flatten("bucket").expect("connected compact bucket");
                if step.finished {
                    assert!(round > 1);
                    assert!(cache.info.snapshot_complete);
                    assert_eq!((total.objects, total.size, total.versions), (40, 40, 40));
                    return;
                }
                assert!(!cache.info.snapshot_complete);
                partial_counts.push(total.objects);
            }
            panic!("cold compacted scan did not finish in 16 rounds; objects per partial: {partial_counts:?}");
        })
        .await;
}

#[tokio::test]
#[serial]
async fn cold_compacted_replicas_finish_within_their_independent_object_budgets() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let first = temp.path().join("a");
    let other = temp.path().join("b");
    let mut cache = seed_flat_bucket(&first, 40).await;
    tokio::fs::create_dir_all(&other).await.expect("fresh replica");
    for index in (0..40).rev() {
        let name = format!("object-{index:04}");
        let metadata = tokio::fs::read(first.join("bucket").join(&name).join("xl.meta"))
            .await
            .expect("original metadata");
        write_test_object_metadata_bytes(&other, "bucket", &name, &metadata).await;
    }
    let budget = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    super::super::TEST_FORCE_COMPACTION_WIDTH
        .scope(16, async {
            for round in 0..(2 * 40_usize.div_ceil(6) + 1) {
                let root = if round % 2 == 0 { &first } else { &other };
                let peer = if round % 2 == 0 { &other } else { &first };
                let step = scan_one_replica_round(root, Some(peer), cache.clone(), budget).await;
                if !step.finished {
                    assert!(
                        step.outcome
                            .durable_scan_progress()
                            .advanced_from(&cache.durable_scan_progress()),
                        "cross-disk compacted work should advance durable scheduling, round {round}"
                    );
                }
                cache = step.outcome;
                let sample = cache.display_scan_progress(0).expect("durable cross-executor display sample");
                if let (Some(objects), Some(size)) = (sample.objects, sample.size) {
                    assert!(objects <= 40, "physical replicas cannot inflate logical progress");
                    assert_eq!(size, objects);
                }
                let encoded = cache.marshal_msg().expect("persist cross-executor display source");
                let cold = DataUsageCache::unmarshal(&encoded).expect("cold cross-executor display source");
                assert_eq!(cold.display_scan_progress(0), Some(sample));
                if step.finished {
                    assert!(round > 1);
                    assert!(cache.info.snapshot_complete);
                    let total = cache.checked_flatten("bucket").expect("complete switched source");
                    assert_eq!((total.objects, total.size), (40, 40));
                    return;
                }
            }
            panic!("two cold physical replicas did not converge within two independent object work budgets");
        })
        .await;
}

/// Each turn runs in another OS process and alternates the physical executor.
/// Only the committed checkpoint and compact display object cross turns.
#[tokio::test]
#[serial]
async fn display_progress_survives_process_restarts_leader_changes_and_repeated_reads() {
    use super::checkpoint_fixture::FixtureStore;
    const CHILD_ROOT: &str = "RUSTFS_TEST_SCAN_PROGRESS_CHILD_ROOT";
    const CHILD_ROUND: &str = "RUSTFS_TEST_SCAN_PROGRESS_CHILD_ROUND";
    const CACHE_NAME: &str = "bucket/checkpoint-fixture.bin";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = std::path::PathBuf::from(root);
        assert!(
            root.file_name()
                .expect("owned temporary root")
                .to_string_lossy()
                .starts_with("rustfs-progress-restart-")
        );
        let round: u64 = std::env::var(CHILD_ROUND)
            .expect("explicit child round")
            .parse()
            .expect("numeric round");
        let store = FixtureStore::reopen(root.join("cache"));
        let mut cache = DataUsageCache::default();
        let revisions = cache
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("cold shared checkpoint load");
        let identity = cache.info.scan_identity.expect("durable bucket identity");
        let prepared = cache.prepare_bucket_checkpoint(
            "bucket",
            round + 2,
            round + 1,
            crate::DataUsageCacheSource::new(0, 0),
            crate::DataUsageScanPlanDigest([31; 32]),
            identity,
        );
        if round > 0 {
            assert_eq!(prepared, crate::data_usage_define::DataUsageCachePrepareOutcome::Reused);
        }
        // Synthetic disks have no healing metadata. Reapply the fixture's
        // scan options after prepare, which may reset the initial empty cache.
        cache.info.skip_healing = true;
        SCANNER_SLEEPER.disable_disk_io_budget_for_tests();
        let (active, peer) = if round.is_multiple_of(2) {
            (root.join("a"), root.join("b"))
        } else {
            (root.join("b"), root.join("a"))
        };
        let step = super::super::TEST_FORCE_COMPACTION_WIDTH
            .scope(
                16,
                scan_one_replica_round(
                    &active,
                    Some(&peer),
                    cache,
                    ScannerCycleBudgetConfig {
                        max_objects: Some(6),
                        ..Default::default()
                    },
                ),
            )
            .await;
        step.outcome
            .save_with_revisions_for_epoch(store, CACHE_NAME, &revisions, 0)
            .await
            .expect("commit checkpoint and display progress");
        let bytes = tokio::fs::read(root.join("cache/progress"))
            .await
            .expect("durable progress after primary commit");
        let sample: rustfs_data_usage::BucketScanProgressSource =
            serde_json::from_slice(&bytes).expect("decode durable display sample");
        if let Some(objects) = sample.objects {
            assert!(objects <= 40, "physical replicas cannot double count objects");
            assert_eq!(sample.size, Some(objects));
        }
        let repeated: rustfs_data_usage::BucketScanProgressSource = serde_json::from_slice(
            &tokio::fs::read(root.join("cache/progress"))
                .await
                .expect("repeat independent progress read"),
        )
        .expect("decode repeated sample");
        assert_eq!(sample, repeated);
        if step.finished {
            assert_eq!((sample.objects, sample.size), (Some(40), Some(40)));
            assert_eq!(sample.stage, rustfs_data_usage::BucketScanStage::Completed);
        }
        tokio::fs::write(
            root.join("result.json"),
            serde_json::to_vec(&serde_json::json!({
                "pid": std::process::id(), "finished": step.finished, "sample": sample,
            }))
            .expect("encode child result"),
        )
        .await
        .expect("commit child result");
        return;
    }
    let temp = tempfile::Builder::new()
        .prefix("rustfs-progress-restart-")
        .tempdir()
        .expect("owned restart fixture");
    let root = temp.path();
    let mut initial = seed_flat_bucket(&root.join("a"), 40).await;
    seed_flat_bucket(&root.join("b"), 40).await;
    let mut identity = initial.info.scan_identity.expect("initial identity");
    identity.publication_epoch = 0;
    identity.tier_registry_generation = crate::runtime_tier_registry_for_cycle(1, 0).await.generation;
    initial.prepare_bucket_checkpoint(
        "bucket",
        1,
        0,
        crate::DataUsageCacheSource::new(0, 0),
        crate::DataUsageScanPlanDigest([31; 32]),
        identity,
    );
    initial.replace("bucket", "", DataUsageEntry::default());
    tokio::fs::create_dir(root.join("cache"))
        .await
        .expect("fresh checkpoint storage");
    tokio::fs::write(root.join("cache/main"), initial.marshal_msg().expect("initial checkpoint encoding"))
        .await
        .expect("persist initial checkpoint");
    let name = format!(
        "{}::display_progress_survives_process_restarts_leader_changes_and_repeated_reads",
        module_path!().split_once("::").expect("crate-qualified module").1
    );
    let mut pids = std::collections::HashSet::new();
    for round in 0..20 {
        let mut command = tokio::process::Command::new(std::env::current_exe().expect("current libtest binary"));
        command
            .arg("--exact")
            .arg(&name)
            .arg("--nocapture")
            .env(CHILD_ROOT, root)
            .env(CHILD_ROUND, round.to_string())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(30), command.output())
            .await
            .expect("bounded child scan turn")
            .expect("launch child process");
        assert!(
            output.status.success(),
            "child round {round}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(root.join("result.json")).await.expect("child result exists"))
                .expect("decode child result");
        let pid = result["pid"].as_u64().expect("child process id");
        assert_ne!(pid, u64::from(std::process::id()));
        assert!(pids.insert(pid), "every turn must run after the previous executor exits");
        if result["finished"] == true {
            assert!(round > 1, "the fixed six-object budget must require multiple process restarts");
            assert_eq!(result["sample"]["objects"], 40);
            assert_eq!(result["sample"]["size"], 40);
            return;
        }
    }
    panic!("cold alternating executors must finish within their fixed per-turn budgets");
}

#[tokio::test]
#[serial]
async fn cold_compacted_identical_names_do_not_mix_replica_version_sizes() {
    use super::checkpoint_fixture::write_checkpoint_object;
    let temp = tempfile::tempdir().expect("fixture directory");
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    let start = seed_flat_bucket(&a, 40).await;
    seed_flat_bucket(&b, 40).await;
    write_checkpoint_object(&b, "object-0000", &[(Some(Uuid::from_u128(100)), 11), (Some(Uuid::from_u128(101)), 23)]).await;
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    super::super::TEST_FORCE_COMPACTION_WIDTH
        .scope(16, async {
            let first = scan_one_replica_round(&a, Some(&b), start, config).await;
            assert!(!first.finished);
            let mut cache = first.outcome;
            for _ in 0..10 {
                let round = scan_one_replica_round(&b, Some(&a), cache, config).await;
                cache = round.outcome;
                if round.finished {
                    break;
                }
            }
            let complete = cache;
            assert!(complete.info.snapshot_complete);
            let counted = complete.checked_flatten("bucket").expect("complete B accounting");
            // 39 one-byte objects and both retained versions of the remaining key.
            assert_eq!((counted.objects, counted.size, counted.versions), (40, 73, 41));
        })
        .await;
}

#[tokio::test]
#[serial]
async fn legacy_unbound_partial_restarts_on_the_actual_physical_replica() {
    use super::checkpoint_fixture::write_checkpoint_object;
    let temp = tempfile::tempdir().expect("fixture");
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    let cache = seed_flat_bucket(&a, 40).await;
    seed_flat_bucket(&b, 40).await;
    write_checkpoint_object(&b, "object-0000", &[(Some(Uuid::from_u128(200)), 12), (Some(Uuid::from_u128(201)), 31)]).await;
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    let first = scan_one_restart_round(&a, cache, config).await;
    let mut legacy = first.outcome;
    assert!(legacy.validated_scan_frontier().is_some());
    legacy.info.scan_directory_source = None;
    legacy.info.scan_directory_work.clear();
    legacy.info.scan_source_trees.clear();
    let upgraded = run_until_finished(&b, legacy, config, 10).await;
    let total = upgraded.checked_flatten("bucket").expect("B complete");
    assert_eq!((total.objects, total.size, total.versions), (40, 82, 41));
}

#[tokio::test]
#[serial]
async fn cold_linked_prefixes_resume_their_own_replica_coverage() {
    use super::checkpoint_fixture::write_checkpoint_object;
    let temp = tempfile::tempdir().expect("fixture directory");
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    let mut cache = seed_flat_bucket(&a, 40).await;
    // The configured replicas still own their checkpoints when heal handles are omitted.
    cache.info.skip_healing = true;
    seed_flat_bucket(&b, 40).await;
    write_checkpoint_object(&b, "object-0000", &[(Some(Uuid::from_u128(100)), 11), (Some(Uuid::from_u128(101)), 23)]).await;
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    // Six turns per disk leave one final B quantum. Each cold process must
    // recover that disk's linked tree as well as its complete child coverage.
    for round in 0..(2 * 40_usize.div_ceil(6) + 1) {
        let (root, peer) = if round < 12 && round % 2 == 0 { (&a, &b) } else { (&b, &a) };
        let previous_source = cache.info.scan_directory_source.clone();
        let before = cache.durable_scan_progress();
        let result = scan_one_replica_round(root, Some(peer), cache, config).await;
        if !result.finished {
            assert!(
                result.outcome.durable_scan_progress().advanced_from(&before),
                "linked source must earn another turn, round {round}"
            );
        }
        cache = result.outcome;
        if round == 1 {
            let previous_source = previous_source.expect("A's physical source");
            let endpoints = [
                previous_source.as_str(),
                cache.info.scan_directory_source.as_deref().expect("B's physical source"),
            ]
            .map(|source| source.split_once('|').expect("disk generation and endpoint").1.to_owned())
            .into_iter()
            .collect();
            let mut only_switched = cache.clone();
            only_switched.activate_directory_source(previous_source, &endpoints);
            assert!(only_switched.validated_scan_frontier().is_some(), "A's linked coverage is retained");
            assert!(
                !only_switched
                    .durable_scan_progress()
                    .advanced_from(&cache.durable_scan_progress()),
                "moving a linked source between active and dormant earns no new turn"
            );
        }

        if result.finished {
            assert_eq!(root, &b);
            assert!(cache.info.snapshot_complete);
            let root = cache.find("bucket").expect("complete linked bucket");
            assert!(!root.compacted);
            assert_eq!(root.children.len(), 40);
            let counted = cache.checked_flatten("bucket").expect("full B view");
            assert_eq!((counted.objects, counted.size, counted.versions), (40, 73, 41));
            return;
        }
    }
    panic!("two independent cold linked prefix scans did not converge");
}

#[tokio::test]
#[serial]
async fn cold_compacted_changed_replica_does_not_import_another_disks_totals() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let first = temp.path().join("a");
    let other = temp.path().join("b");
    let baseline = seed_flat_bucket(&first, 40).await;
    seed_flat_bucket(&other, 40).await;
    // Object zero was already counted on A but never existed in B's view.
    // Importing A's subtotal based only on a matching child boundary would
    // produce 40 objects on B even though its authoritative listing has 39.
    tokio::fs::remove_dir_all(other.join("bucket/object-0000"))
        .await
        .expect("different replica membership");
    let config = ScannerCycleBudgetConfig {
        max_objects: Some(6),
        ..Default::default()
    };
    super::super::TEST_FORCE_COMPACTION_WIDTH
        .scope(16, async {
            let first = scan_one_restart_round(&first, baseline, config).await;
            assert!(!first.finished);
            let complete = run_until_finished(&other, first.outcome, config, 14).await;
            let root = complete.checked_flatten("bucket").expect("finished replica");
            assert_eq!((root.objects, root.versions, root.size), (39, 39, 39));
        })
        .await;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test(start_paused = true)]
#[serial]
async fn cold_wide_listing_rotates_physical_endpoints_before_eof() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let first = temp.path().join("a");
    let other = temp.path().join("b");
    let mut cache = seed_flat_bucket(&first, 49).await;
    seed_flat_bucket(&other, 49).await;
    let config = ScannerCycleBudgetConfig {
        max_duration: Some(8 * PHYSICAL_READ_COST),
        ..Default::default()
    };
    for round in 0..22 {
        let disk = if round % 2 == 0 { &first } else { &other };
        let peer = if round % 2 == 0 { &other } else { &first };
        let physical = disk.join("bucket").canonicalize().expect("physical directory");
        let before = cache.durable_scan_progress();
        let step = SLOW_DIRECTORY
            .scope((physical, Cell::new(0)), scan_one_replica_round(disk, Some(peer), cache, config))
            .await;
        if !step.finished {
            assert!(
                step.outcome.durable_scan_progress().advanced_from(&before),
                "each independent native resume must advance durable scheduling, round {round}"
            );
        }
        cache = step.outcome;
        if step.finished {
            assert!(round > 2);
            assert!(cache.info.snapshot_complete);
            let total = cache.checked_flatten("bucket").expect("switched cold listing");
            assert_eq!((total.objects, total.size), (49, 49));
            return;
        }
    }
    panic!("rotating before EOF repeatedly overwrote an endpoint's physical progress");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test(start_paused = true)]
#[serial]
async fn cold_wide_listing_reaches_eof_after_every_executor_restart() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&root, 49).await;
    let budget = ScannerCycleBudgetConfig {
        max_duration: Some(8 * PHYSICAL_READ_COST),
        ..Default::default()
    };
    SLOW_DIRECTORY
        .scope((root.join("bucket").canonicalize().expect("physical path"), Cell::new(0)), async {
            for round in 0..16 {
                let step = scan_one_restart_round(&root, cache.clone(), budget).await;
                cache = step.outcome;
                if step.finished {
                    assert!(round > 1);
                    assert!(cache.info.snapshot_complete);
                    let total = cache.checked_flatten("bucket").expect("complete bucket");
                    assert_eq!((total.objects, total.size), (49, 49));
                    return;
                }
                assert!(!cache.info.snapshot_complete);
            }
            let reads = SLOW_DIRECTORY.with(|(_, reads)| reads.get());
            panic!("cold physical listing did not reach EOF after 16 rounds and {reads} directory reads");
        })
        .await;
}

#[tokio::test]
#[serial]
async fn wide_compacted_directory_reuses_only_finished_children() {
    const WIDTH: usize = 40;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&disk_root, WIDTH).await;
    let disk = open_fixture_disk(&disk_root).await;
    let parent = CancellationToken::new();
    super::super::TEST_FORCE_COMPACTION_WIDTH
        .scope(16, async {
            for round in 0..16 {
                let budget = ScannerCycleBudget::new_with_progress_tracking(
                    &parent,
                    ScannerCycleBudgetConfig {
                        max_objects: Some(6),
                        ..Default::default()
                    },
                );
                let result = scan_data_folder(
                    budget.token(),
                    budget,
                    vec![disk.clone()],
                    disk.clone(),
                    cache.clone(),
                    None,
                    HealScanMode::Normal,
                    SCANNER_SLEEPER.clone(),
                )
                .await;
                match result {
                    Ok(completed) => {
                        assert!(round > 1);
                        assert!(completed.info.snapshot_complete);
                        let root = completed.find("bucket").expect("compacted bucket");
                        assert!(root.compacted);
                        assert_eq!((root.objects, root.size), (WIDTH, WIDTH));
                        return;
                    }
                    Err(ScannerError::PartialCache(partial)) => {
                        cache = DataUsageCache::unmarshal(&partial.marshal_msg().expect("save compacted checkpoint"))
                            .expect("reload partial");
                    }
                    Err(error) => panic!("unexpected compacted scan failure: {error}"),
                }
            }
            panic!("fixed object budget failed to finish compacted children");
        })
        .await;
}

#[tokio::test]
#[serial]
async fn wide_directory_replacement_invalidates_parked_entries() {
    const WIDTH: usize = 48;
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let initial = seed_flat_bucket(&disk_root, WIDTH).await;
    let disk = open_fixture_disk(&disk_root).await;
    let parent = CancellationToken::new();
    let config = ScannerCycleBudgetConfig {
        max_entries: Some(3),
        ..Default::default()
    };
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, config);
    let Err(ScannerError::PartialCache(partial)) = scan_data_folder(
        budget.token(),
        budget,
        vec![disk.clone()],
        disk.clone(),
        initial,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    else {
        panic!("the original listing must stop before EOF")
    };
    assert!(
        super::super::parked_bucket_disks("bucket")
            .iter()
            .any(|scope| scope.ptr_eq(&Arc::downgrade(&disk)))
    );
    let mut cache = *partial;
    // Rename pins the old inode. Reusing its open stream or its name pages
    // would visit the vanished objects and omit this replacement's metadata.
    tokio::fs::rename(disk_root.join("bucket"), temp.path().join("previous"))
        .await
        .expect("move old namespace");
    let metadata = metadata_for_object_version("bucket", "successor", Some(Uuid::from_u128(100)));
    write_test_object_metadata_bytes(&disk_root, "bucket", "successor", &metadata).await;
    for _ in 0..6 {
        let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, config);
        match scan_data_folder(
            budget.token(),
            budget,
            vec![disk.clone()],
            disk.clone(),
            cache.clone(),
            None,
            HealScanMode::Normal,
            SCANNER_SLEEPER.clone(),
        )
        .await
        {
            Ok(complete) => {
                assert!(complete.info.snapshot_complete);
                let total = complete.checked_flatten("bucket").expect("current root");
                assert_eq!((total.objects, total.size), (1, 1));
                assert!(complete.find("bucket/successor").is_some());
                assert!(
                    !super::super::parked_bucket_disks("bucket")
                        .iter()
                        .any(|scope| scope.ptr_eq(&Arc::downgrade(&disk)))
                );
                return;
            }
            Err(ScannerError::PartialCache(partial)) => cache = *partial,
            Err(error) => panic!("replacement scan failed: {error}"),
        }
    }
    panic!("replacement directory never finished");
}

#[tokio::test]
#[serial]
async fn wide_directory_deleted_bucket_releases_a_slot_under_pressure() {
    use super::super::directory_cursor::{DirectoryScope, PARKED_DIRECTORIES_PER_DISK, ScannerDirectory, is_parked};

    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let cache = seed_flat_bucket(&disk_root, 0).await;
    let disk = open_fixture_disk(&disk_root).await;
    let scope = DirectoryScope::new(&disk, &cache, FolderScanMode::Namespace, None).expect("physical source and owner");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    budget.cancel_for_runtime();
    let retired = disk_root.join("retired");
    for index in 0..PARKED_DIRECTORIES_PER_DISK {
        let path = retired.join(index.to_string());
        tokio::fs::create_dir_all(&path).await.expect("create parked directory");
        let path = path.to_str().expect("fixture path");
        let directory = ScannerDirectory::open(path, "bucket", Some(scope.clone()), budget.clone(), None)
            .await
            .expect("open source");
        drop(directory);
        assert!(is_parked(path));
    }
    let next = disk_root.join("replacement");
    tokio::fs::create_dir(&next).await.expect("current source");
    let next = next.to_str().expect("fixture path");
    let directory = ScannerDirectory::open(next, "bucket", Some(scope.clone()), budget.clone(), None)
        .await
        .expect("open next source");
    drop(directory);
    assert!(!is_parked(next), "active owners retain their bounded slots");
    tokio::fs::remove_dir_all(retired).await.expect("retire directories");
    let directory = ScannerDirectory::open(next, "bucket", Some(scope), budget, None)
        .await
        .expect("admit after retirement");
    drop(directory);
    assert!(is_parked(next), "an obsolete source must not indefinitely exclude new work");
    super::super::directory_cursor::forget_bucket(&disk, &cache.info);
}

#[tokio::test]
#[serial]
async fn raw_directory_replay_reports_discovery_without_recharging_known_entries() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&disk_root, 0).await;
    let mut index =
        RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket", 16).expect("raw index")).expect("raw index writer");
    for n in 0..64 {
        tokio::fs::create_dir_all(disk_root.join("bucket").join(format!("object-{n:04}")))
            .await
            .expect("existing directory member");
        index.record_entry(&format!("object-{n:04}")).expect("known directory member");
    }
    cache.info.scan_raw_enumeration_page_index = Some(index.checkpoint().expect("persist logical enumeration"));
    assert_eq!(
        cache
            .validated_raw_enumeration_page_index()
            .expect("valid logical checkpoint")
            .committed_entries()
            .expect("known entries")
            .len(),
        64
    );
    let disk = open_fixture_disk(&disk_root).await;
    cache.info.scan_directory_source = Some(
        super::super::directory_cursor::physical_source(&disk)
            .await
            .expect("bind the retained logical index to its physical source"),
    );
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_entries: Some(1),
            ..Default::default()
        },
    );
    let complete = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![disk.clone()],
        disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    .expect("known entries do not exhaust the one-entry budget");
    assert_eq!(budget.reason(), None);
    assert_eq!(complete.checked_flatten("bucket").expect("bucket root").objects, 0);
    assert_eq!(
        budget.entries_visited(),
        64,
        "real directory discovery must keep the remote stream alive while replaying known names"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
#[serial]
async fn ordinary_owner_cancellation_persists_directory_position_for_cold_resume() {
    use super::super::directory_cursor::is_parked;
    use super::enumeration_restart::install_raw_entry_owner_cancel;

    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let mut cache = seed_flat_bucket(&disk_root, 64).await;
    let tier = runtime_tier_registry_for_cycle(cache.info.next_cycle, cache.info.leader_epoch).await;
    cache
        .info
        .scan_identity
        .as_mut()
        .expect("scan identity")
        .tier_registry_generation = tier.generation;
    cache.info.tier_registry_generation = Some(tier.generation);
    let disk = open_fixture_disk(&disk_root).await;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let observer = install_raw_entry_owner_cancel(disk.path(), 16, parent.clone());
    let result = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![disk.clone()],
        disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;
    drop(observer);
    assert!(parent.is_cancelled());
    assert_eq!(budget.reason(), None, "the owner stopped independently of the cycle budget");
    let Err(ScannerError::PartialCache(partial)) = result else {
        panic!("ordinary cancellation must retain a partial cache");
    };
    assert_eq!(partial.info.scan_directory_work.len(), 1, "retain the physical directory receipt");
    let saved = serde_json::to_value(&partial.info.scan_directory_work[0]).expect("directory receipt");
    assert_eq!(saved["entries_read"], 16);
    assert!(!is_parked(disk.path().join("bucket").to_str().expect("fixture path")));
    let path = temp.path().join("checkpoint.bin");
    tokio::fs::write(&path, partial.marshal_msg().expect("serialize partial checkpoint"))
        .await
        .expect("persist partial checkpoint");
    let mut cache =
        DataUsageCache::unmarshal(&tokio::fs::read(&path).await.expect("read cold checkpoint")).expect("reload cold checkpoint");
    let identity = cache.info.scan_identity.expect("checkpoint identity");
    let source = cache.info.source.expect("checkpoint source");
    let plan = crate::DataUsageScanPlanDigest([31; 32]);
    assert_eq!(
        cache.prepare_bucket_checkpoint("bucket", 2, 1, source, plan, identity),
        crate::data_usage_define::DataUsageCachePrepareOutcome::Reused
    );
    let cold_disk = open_fixture_disk(&disk_root).await;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_entries: Some(1),
            ..Default::default()
        },
    );
    let result = scan_data_folder(
        budget.token(),
        budget,
        vec![cold_disk.clone()],
        cold_disk.clone(),
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await;
    let Err(ScannerError::PartialCache(partial)) = result else {
        panic!("one newly charged entry must stop the cold continuation");
    };
    let resumed = serde_json::to_value(&partial.info.scan_directory_work[0]).expect("resumed directory receipt");
    assert_eq!(
        resumed["entries_read"], 17,
        "the cold reader must continue after its saved native position"
    );
    assert_eq!(resumed["physical"], saved["physical"]);
    let mut cache = DataUsageCache::unmarshal(&partial.marshal_msg().expect("serialize resumed checkpoint"))
        .expect("reload resumed checkpoint");
    cache.prepare_bucket_checkpoint("bucket", 3, 2, source, plan, identity);
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let complete = scan_data_folder(
        budget.token(),
        budget,
        vec![cold_disk.clone()],
        cold_disk,
        cache,
        None,
        HealScanMode::Normal,
        SCANNER_SLEEPER.clone(),
    )
    .await
    .expect("cold continuation finishes the bucket");
    let total = complete.checked_flatten("bucket").expect("complete bucket root");
    assert_eq!((total.objects, total.size), (64, 64));
    assert!(complete.info.snapshot_complete);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
#[serial]
async fn changed_directory_listing_clears_its_physical_receipt() {
    use super::super::directory_cursor::{DirectoryScope, ScannerDirectory};

    let temp = tempfile::tempdir().expect("fixture directory");
    let disk_root = temp.path().join("disk");
    let cache = seed_flat_bucket(&disk_root, 16).await;
    let disk = open_fixture_disk(&disk_root).await;
    let scope = DirectoryScope::new(&disk, &cache, FolderScanMode::Namespace, None).expect("directory scope");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let saved = Arc::new(std::sync::Mutex::new(None));
    let path = disk_root.join("bucket");
    let mut directory =
        ScannerDirectory::open(path.to_str().expect("fixture path"), "bucket", Some(scope), budget, Some(saved.clone()))
            .await
            .expect("open physical directory");
    for _ in 0..8 {
        directory
            .peek_entry()
            .await
            .expect("read physical entry")
            .expect("directory member");
        directory.accept_entry();
    }
    directory.work().expect("indexed directory").checkpoint();
    assert!(saved.lock().expect("receipt").is_some());
    tokio::fs::rename(&path, disk_root.join("retired"))
        .await
        .expect("replace physical directory");
    tokio::fs::create_dir(&path).await.expect("create successor directory");
    let error = directory.verify_current().await.expect_err("the old listing is invalid");
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    drop(directory);
    assert!(
        saved.lock().expect("receipt").is_none(),
        "an invalid listing cannot retain a resumable receipt"
    );
}
