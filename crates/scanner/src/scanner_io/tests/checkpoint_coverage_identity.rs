// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A bucket sweep interrupted by the cycle budget resumes from its checkpoint
//! in a later cycle even after an event that changes the reuse identity but
//! not the namespace. The event here is a maintenance publication; a
//! coordinator restart is the same case for the coverage identity, but a
//! process cannot change its own instance id in a test. The other buckets
//! are walked again; the sweep in progress is not restarted as mixed
//! coverage (rustfs/rustfs#8494).

use super::*;

const HOT_OBJECTS: [&str; 6] = ["p0/a", "p1/b", "p2/c", "p3/d", "p4/e", "p5/f"];
const LEADER_EPOCH: u64 = 11;

#[test]
fn completed_checkpoint_reuse_requires_unpublished_unchanged_coverage() {
    let source = DataUsageCacheSource::new(0, 0);
    let plan = DataUsageScanPlanDigest([1; 32]);
    let coverage = DataUsageScanPlanDigest([2; 32]);
    let identity = crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::new_v4(),
        set_layout: DataUsageScanPlanDigest([3; 32]),
        publication_epoch: 1,
        tier_registry_generation: 2,
        scan_mode: HealScanMode::Normal,
    };
    let mut saved = DataUsageCache {
        info: DataUsageCacheInfo {
            name: "completed".to_string(),
            next_cycle: 8,
            leader_epoch: 11,
            last_update: Some(SystemTime::now()),
            source: Some(source),
            snapshot_complete: true,
            scan_plan_digest: Some(plan),
            scan_coverage_digest: Some(coverage),
            scan_identity: Some(identity),
            tier_registry_generation: Some(2),
            cache_key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            ..Default::default()
        },
        ..Default::default()
    };
    saved.replace(
        "completed",
        "",
        DataUsageEntry {
            objects: 3,
            size: 48,
            ..Default::default()
        },
    );
    for case in [
        "unpublished",
        "published",
        "missing_floor",
        "restart",
        "mutation",
        "legacy",
        "newer_cycle",
        "newer_leader",
        "tier",
        "incarnation",
        "source",
        "deep",
        "corrupt",
        "partial",
    ] {
        let mut cache = saved.clone();
        let mut options = DataUsageCacheReuseOptions {
            require_source: true,
            tier_registry_generation: Some(2),
            checkpoint_identity: Some(identity),
            checkpoint_plan_digest: Some(coverage),
            completed_checkpoint_cycle_floor: Some(7),
        };
        match case {
            "published" => options.completed_checkpoint_cycle_floor = Some(8),
            "missing_floor" => options.completed_checkpoint_cycle_floor = None,
            "restart" => cache.info.scan_plan_digest = Some(DataUsageScanPlanDigest([4; 32])),
            "mutation" => cache.info.scan_coverage_digest = Some(DataUsageScanPlanDigest([4; 32])),
            "legacy" => cache.info.scan_coverage_digest = None,
            "newer_cycle" => cache.info.next_cycle = 10,
            "newer_leader" => cache.info.leader_epoch = 13,
            "tier" => options.tier_registry_generation = Some(3),
            "incarnation" => options.checkpoint_identity.as_mut().expect("identity").bucket_incarnation = Uuid::new_v4(),
            "source" => cache.info.source = Some(DataUsageCacheSource::new(1, 0)),
            "deep" => options.checkpoint_identity.as_mut().expect("identity").scan_mode = HealScanMode::Deep,
            "corrupt" => cache.cache.clear(),
            "partial" => cache.info.scan_resume_after = Some("completed/a".to_string()),
            "unpublished" => {}
            _ => unreachable!("all fixture cases are handled"),
        }
        let state = current_cache_root_or_prepare_with_generation(&mut cache, "completed", source, 9, 12, plan, options);
        assert_eq!(matches!(state, DataUsageCacheScanState::Current(_)), case == "unpublished", "{case}");
        if case == "unpublished" {
            assert_eq!(cache.info.next_cycle, 8, "adoption must not extend the saved receipt's lifetime");
            let DataUsageCacheScanState::Current(root) = state else { unreachable!() };
            assert_eq!(root.entry.objects, 3);
        }
    }
}

#[test]
fn completed_checkpoint_floor_requires_an_authoritative_baseline() {
    let source = DataUsageCacheSource::new(0, 0);
    let sources = HashSet::from([source]);
    let baseline: serde_json::Value =
        serde_json::from_slice(&complete_usage_baseline(source, DataUsageScanPlanDigest([1; 32]), 7, 11))
            .expect("complete baseline");
    for case in [
        "complete",
        "bootstrap",
        "observational",
        "partial",
        "incomplete",
        "legacy",
        "no_cycle",
    ] {
        let mut value = baseline.clone();
        match case {
            "bootstrap" => {
                value = serde_json::to_value(crate::scanner::scanner_usage_bootstrap_marker(SystemTime::now(), Some(11)))
                    .expect("bootstrap marker");
            }
            "observational" => value["usage_snapshot_observational"] = true.into(),
            "partial" => value["usage_snapshot_partial"] = true.into(),
            "incomplete" => value["usage_snapshot_complete"] = false.into(),
            "legacy" => value["scanner_epoch"] = serde_json::Value::Null,
            "no_cycle" => value["scanner_cycle"] = serde_json::Value::Null,
            "complete" => {}
            _ => unreachable!("all fixture cases are handled"),
        }
        let bytes = serde_json::to_vec(&value).expect("baseline fixture");
        assert_eq!(
            completed_checkpoint_cycle_floor(Some(&bytes), &sources, 9, 12),
            match case {
                "complete" => Some(7),
                "bootstrap" => Some(0),
                _ => None,
            },
            "{case}"
        );
    }
    assert_eq!(completed_checkpoint_cycle_floor(None, &sources, 9, 12), None);
    assert_eq!(completed_checkpoint_cycle_floor(Some(b"invalid"), &sources, 9, 12), None);
}

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

async fn run_full_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    max_objects: Option<u64>,
) -> (ScannerCycleResult, Option<DataUsageInfo>, Arc<ScannerCycleBudget>) {
    run_full_cycle_at_epoch(store, cycle, LEADER_EPOCH, max_objects, None).await
}

async fn run_full_cycle_at_epoch(
    store: &Arc<ECStore>,
    cycle: u64,
    leader_epoch: u64,
    max_objects: Option<u64>,
    persisted_usage_baseline: Option<Bytes>,
) -> (ScannerCycleResult, Option<DataUsageInfo>, Arc<ScannerCycleBudget>) {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects,
            ..Default::default()
        },
    );
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx: budget.token(),
                budget: Arc::clone(&budget),
                updates,
                want_cycle: cycle,
                leader_epoch,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline,
                observed_usage_candidate: None,
                requires_full_scan: true,
                service_cohort: None,
                resolved_scope_observer: None,
                post_walk_gate: None,
                peer_observations: None,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    let snapshot = receiver.recv().await;
    assert!(receiver.recv().await.is_none(), "a cycle publishes at most one terminal snapshot");
    (result, snapshot, budget)
}

#[tokio::test]
#[serial]
async fn checkpoint_small_budget_accumulates_completed_buckets_across_leaders() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    let names = ["bounded-a", "bounded-b", "bounded-c", "bounded-d"];
    for bucket in names {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bounded fixture bucket");
        for object in ["a", "b", "c"] {
            put_fixture_object(&store, bucket, object).await;
        }
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let baseline = Bytes::from(
        serde_json::to_vec(&crate::scanner::scanner_usage_bootstrap_marker(SystemTime::now(), Some(LEADER_EPOCH)))
            .expect("bootstrap baseline"),
    );
    let mut observations = Vec::new();
    for cycle in 1..=20 {
        let (result, usage, budget) =
            run_full_cycle_at_epoch(&store, cycle, LEADER_EPOCH + cycle, Some(2), Some(baseline.clone())).await;
        observations.push((cycle, result.status, budget.progress()));
        if result.status == ScannerCycleStatus::Complete {
            let usage = usage.expect("completed bounded sweep publishes usage");
            assert_eq!(usage.objects_total_count, 12);
            assert_eq!(usage.usage_snapshot_converged, Some(true));
            for bucket in names {
                assert_eq!(usage.buckets_usage[bucket].objects_count, 3);
            }
            clear_dirty_usage_buckets_for_tests();
            return;
        }
    }
    panic!("fixed budget must accumulate the completed buckets: {observations:?}");
}

#[tokio::test]
#[serial]
async fn checkpoint_resumes_across_a_maintenance_publication() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in ["hot-bucket", "idle-bucket"] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
    }
    for object in HOT_OBJECTS {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    put_fixture_object(&store, "idle-bucket", "p0/a").await;
    record_dirty_usage_bucket("hot-bucket");
    record_dirty_usage_bucket("idle-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: a quiet full walk certifies both buckets.
    let (result, first, _) = run_full_cycle(&store, 1, None).await;
    let first = first.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert_eq!(first.buckets_usage["hot-bucket"].objects_count, 6);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());

    // Cycle 2: the object budget stops the written bucket's sweep with a
    // checkpoint; the cycle itself is incomplete. Service order follows the
    // wait, so the budget leaves it two objects even behind the idle bucket.
    for object in ["p6/g", "p7/h"] {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    record_dirty_usage_bucket("hot-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (result, _, budget) = run_full_cycle(&store, 2, Some(3)).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(budget.budget_elapsed());
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let mut checkpoint = DataUsageCache::default();
    checkpoint
        .load(Arc::clone(&set), &path_join_buf(&["hot-bucket", DATA_USAGE_CACHE_NAME]))
        .await
        .expect("the interrupted sweep should leave a bucket cache");
    assert!(!checkpoint.info.snapshot_complete);
    assert!(checkpoint.has_scan_position(), "the interrupted sweep must keep a durable position");
    let started_plan = checkpoint
        .info
        .scan_progress
        .expect("the interrupted sweep records its coverage plan")
        .started_plan;

    // A maintenance publication between the cycles changes the reuse
    // identity (finished results are walked again) but not the coverage
    // identity (a sweep in progress keeps its plan).
    let before = crate::scanner::probe_scanner_activity(store.as_ref(), false)
        .await
        .expect("the fixture activity should be observable");
    record_scanner_maintenance_change("idle-bucket");
    let after = crate::scanner::probe_scanner_activity(store.as_ref(), false)
        .await
        .expect("the fixture activity should be observable");
    assert_ne!(
        crate::scanner::scanner_activity_structural_digest(&before, crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE),
        crate::scanner::scanner_activity_structural_digest(&after, crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE),
        "a maintenance publication invalidates reusable results"
    );
    assert_eq!(
        crate::scanner::scanner_activity_coverage_digest(&before, crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE),
        crate::scanner::scanner_activity_coverage_digest(&after, crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE),
        "a maintenance publication keeps the coverage plan"
    );

    // Cycle 3: the hot bucket resumes from its checkpoint and certifies;
    // a fresh walk would have read all nine objects.
    let (result, third, budget) = run_full_cycle(&store, 3, None).await;
    let third = third.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(third.usage_snapshot_converged, Some(true));
    assert_eq!(third.buckets_usage["hot-bucket"].objects_count, 8);
    assert_eq!(third.buckets_usage["idle-bucket"].objects_count, 1);
    assert!(
        budget.progress().0 < 9,
        "the resumed sweep must not restart from the first object: {} objects read",
        budget.progress().0
    );
    let mut completed = DataUsageCache::default();
    completed
        .load(set, &path_join_buf(&["hot-bucket", DATA_USAGE_CACHE_NAME]))
        .await
        .expect("the certified bucket cache should load");
    assert!(completed.info.snapshot_complete);
    assert!(completed.info.scan_progress.is_none());
    assert_ne!(
        completed.info.scan_plan_digest,
        Some(started_plan),
        "a complete cache is current under the reuse plan, not the coverage plan"
    );
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn remote_checkpoint_resumes_across_restart_and_maintenance_identity_changes() {
    #[derive(serde::Serialize)]
    struct WorkerRequest<'a> {
        version: u16,
        request_id: uuid::Uuid,
        server_epoch: uuid::Uuid,
        session_id: uuid::Uuid,
        session_sequence: u64,
        bucket: &'a str,
        next_cycle: u64,
        leader_epoch: u64,
        scan_plan_digest: DataUsageScanPlanDigest,
        checkpoint_plan_digest: DataUsageScanPlanDigest,
        skip_healing: bool,
        scan_mode: HealScanMode,
        budget: serde_json::Value,
    }
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let resolved = Arc::downgrade(&store);
    assert!(crate::storage_api::owner::ecstore_set_object_store_resolver(Arc::new(
        move || resolved.upgrade()
    )));
    rustfs_credentials::set_global_rpc_secret("remote-checkpoint-test-secret".to_string()).expect("test RPC secret");
    let bucket = "remote-checkpoint";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("remote bucket");
    for object in HOT_OBJECTS {
        put_fixture_object(&store, bucket, object).await;
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let set = store.pools[0].disk_set[0].clone();
    let disk = scanner_set_disk_inventory(&set).await.pop().expect("local worker disk");
    let cache_name = path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]);

    for (index, restart) in [true, false].into_iter().enumerate() {
        let before = if restart {
            std::collections::BTreeMap::from([(
                "<local>".to_string(),
                crate::scanner::scanner_node_activity_for_tests("instance-before", 0, 0, false),
            )])
        } else {
            crate::scanner::probe_scanner_activity(store.as_ref(), false)
                .await
                .expect("activity before maintenance")
        };
        let coverage = DataUsageScanPlanDigest(crate::scanner::scanner_activity_coverage_digest(
            &before,
            crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE,
        ));
        let reuse = DataUsageScanPlanDigest(crate::scanner::scanner_activity_structural_digest(
            &before,
            crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE,
        ));
        let cycle = 100 + u64::try_from(index).expect("fixture index") * 2;
        for resumed in [false, true] {
            let activity = if resumed && restart {
                std::collections::BTreeMap::from([(
                    "<local>".to_string(),
                    crate::scanner::scanner_node_activity_for_tests("instance-after", 0, 0, false),
                )])
            } else if resumed {
                record_scanner_maintenance_change(bucket);
                crate::scanner::probe_scanner_activity(store.as_ref(), false)
                    .await
                    .expect("activity after maintenance")
            } else {
                before.clone()
            };
            let requested_reuse = DataUsageScanPlanDigest(crate::scanner::scanner_activity_structural_digest(
                &activity,
                crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE,
            ));
            assert_eq!(
                coverage,
                DataUsageScanPlanDigest(crate::scanner::scanner_activity_coverage_digest(
                    &activity,
                    crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE
                ))
            );
            if resumed {
                assert_ne!(reuse, requested_reuse, "the event must invalidate completed cache reuse");
            }
            let next_cycle = cycle + u64::from(resumed);
            save_config(
                store.clone(),
                DATA_USAGE_BLOOM_NAME_PATH.as_str(),
                crate::scanner::encode_scanner_cycle_fence_for_test(next_cycle, LEADER_EPOCH),
            )
            .await
            .expect("remote request cycle fence");
            let body = WorkerRequest {
                version: crate::storage_api::owner::NS_SCANNER_PROTOCOL_VERSION,
                request_id: uuid::Uuid::new_v4(),
                server_epoch: uuid::Uuid::new_v4(),
                session_id: uuid::Uuid::new_v4(),
                session_sequence: 0,
                bucket,
                next_cycle,
                leader_epoch: LEADER_EPOCH,
                scan_plan_digest: requested_reuse,
                checkpoint_plan_digest: coverage,
                skip_healing: true,
                scan_mode: HealScanMode::Normal,
                budget: serde_json::json!({"max_objects": if resumed { None } else { Some(2_u64) }}),
            };
            let body = rmp_serde::to_vec_named(&body).expect("remote worker request body");
            let request = crate::decode_remote_scanner_request(&body).expect("remote worker decodes coverage identity");
            tokio::time::timeout(
                Duration::from_secs(60),
                crate::serve_remote_scanner_request(disk.clone(), request, tokio::io::sink(), CancellationToken::new()),
            )
            .await
            .expect("bounded remote worker")
            .expect("remote worker stream");
            let mut cache = DataUsageCache::default();
            cache
                .load(set.clone(), &cache_name)
                .await
                .expect("durable remote worker cache");
            if resumed {
                assert!(
                    cache.info.snapshot_complete,
                    "a resumed remote sweep must complete without mixed coverage"
                );
                assert!(cache.info.scan_progress.is_none());
                assert_eq!(cache.info.scan_plan_digest, Some(requested_reuse));
                assert_eq!(cache.flatten(cache.find(bucket).expect("bucket root")).objects, 6);
            } else {
                assert!(!cache.info.snapshot_complete);
                assert!(cache.has_scan_position(), "budget must leave real checkpoint progress");
                let progress = cache.info.scan_progress.expect("interrupted worker records coverage");
                assert_eq!(progress.started_plan, coverage);
                assert_eq!(progress.requested_plan, coverage);
            }
        }
    }
}
