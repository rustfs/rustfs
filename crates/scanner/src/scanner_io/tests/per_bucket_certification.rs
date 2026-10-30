// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Per-bucket certification on the single-disk fixture: a bucket written
//! during the walk is reported pending while the other buckets are certified,
//! the publication stays authoritative, and the next cycle is scoped to the
//! pending bucket. A PUT tail pending across the walk is ordinary namespace
//! activity; a guardrail veto still supersedes the whole cycle.

use super::*;

const BUCKETS: [&str; 3] = ["cold-bucket", "hot-bucket", "idle-bucket"];

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

struct CycleHooks {
    resolved_scope_observer: Option<tokio::sync::oneshot::Sender<ScannerBucketScanScope>>,
    post_walk_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    peer_observations: Option<super::super::io_cycle::ScannerCyclePeerObservations>,
}

async fn run_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    baseline: Option<Bytes>,
    requires_full_scan: bool,
    hooks: CycleHooks,
) -> (ScannerCycleResult, DataUsageInfo) {
    let peer_cycle = hooks.peer_observations.is_some();
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx,
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: baseline,
                observed_usage_candidate: None,
                requires_full_scan,
                service_cohort: None,
                resolved_scope_observer: hooks.resolved_scope_observer,
                post_walk_gate: hooks.post_walk_gate,
                peer_observations: hooks.peer_observations,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    if peer_cycle {
        assert_eq!(
            result.status,
            ScannerCycleStatus::Complete,
            "one remote hot bucket cannot supersede certified cold buckets"
        );
    }
    let snapshot = receiver.recv().await.expect("cycle should publish a snapshot");
    assert!(receiver.recv().await.is_none(), "a cycle publishes one terminal snapshot");
    (result, snapshot)
}

fn baseline_bytes(snapshot: &DataUsageInfo) -> Bytes {
    Bytes::from(serde_json::to_vec(snapshot).expect("snapshot should encode"))
}

#[tokio::test]
#[serial]
async fn write_during_walk_leaves_only_that_bucket_pending_and_scopes_the_next_cycle() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: a quiet full walk certifies every bucket.
    let (result, first) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
            peer_observations: None,
        },
    )
    .await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert!(!first.usage_snapshot_observational);
    assert!(first.usage_snapshot_pending_buckets.is_empty());
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty(), "a converged cycle clears the dirty map");

    // Cycle 2: cold and idle are dirty at the start snapshot; hot is written
    // after the scope resolved and before the post-walk proof runs.
    for bucket in ["cold-bucket", "idle-bucket"] {
        put_fixture_object(&store, bucket, "second").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
        put_fixture_object(&writer_store, "hot-bucket", "during-walk").await;
        record_dirty_usage_bucket("hot-bucket");
        wait_for_namespace_commit_tails(writer_store.as_ref()).await;
        release.send(()).expect("the cycle should still be waiting on the gate");
        scope
    });
    let (result, second) = run_cycle(
        &store,
        2,
        Some(baseline_bytes(&first)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: Some(gate),
            peer_observations: None,
        },
    )
    .await;
    let scope = writer.await.expect("writer task should finish");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["cold-bucket".to_string(), "idle-bucket".to_string()])),
        "the converged baseline scopes cycle 2 to the buckets dirty at its start"
    );

    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "a write during the walk no longer supersedes"
    );
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert!(!second.usage_snapshot_observational, "pending buckets publish on the authoritative route");
    assert_eq!(second.usage_snapshot_converged, Some(false));
    assert_eq!(second.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert_eq!(second.usage_snapshot_pending_bucket_count, 1);
    assert!(second.is_complete_bucket_usage_snapshot());
    assert_eq!(second.buckets_usage["cold-bucket"].objects_count, 2);
    assert_eq!(second.buckets_usage["idle-bucket"].objects_count, 2);
    assert_eq!(
        second.buckets_usage["hot-bucket"].objects_count, 1,
        "the pending bucket carries its last walked value"
    );
    assert!(result.has_dirty_usage_to_acknowledge());
    assert!(result.clear_verified_usage().is_empty());
    let dirty = dirty_usage_buckets_for_tests();
    assert_eq!(
        dirty.keys().cloned().collect::<Vec<_>>(),
        vec!["hot-bucket".to_string()],
        "certification clears the unchanged buckets and keeps the written one"
    );

    // Cycle 3: the unconverged authoritative snapshot is still a structural
    // baseline, so the walk is scoped to the pending bucket.
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (result, third) = run_cycle(
        &store,
        3,
        Some(baseline_bytes(&second)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: None,
            peer_observations: None,
        },
    )
    .await;
    let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["hot-bucket".to_string()])),
        "the next cycle walks only the pending bucket"
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(third.usage_snapshot_converged, Some(true));
    assert!(third.usage_snapshot_pending_buckets.is_empty());
    assert_eq!(third.buckets_usage["hot-bucket"].objects_count, 2);
    assert_eq!(third.buckets_usage["cold-bucket"].objects_count, 2);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn pending_put_tail_across_the_walk_no_longer_vetoes_the_cycle() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let vetoes_before = global_metrics().report().await.usage_freshness.guardrail_veto_total;

    // A PUT whose rename fanout is still pending when the post-walk probe
    // runs used to flip the local publication barrier and supersede the
    // whole cycle (rustfs/rustfs#8494). It is ordinary namespace activity:
    // the walk certifies every bucket and publishes authority.
    let pending_commit = ecstore_hold_namespace_commit(store.as_ref());
    assert!(ecstore_namespace_commits_pending(store.as_ref()));
    assert!(!store.scanner_data_usage_publication_blocked().await);
    let (result, snapshot) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
            peer_observations: None,
        },
    )
    .await;
    drop(pending_commit);

    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert!(!snapshot.usage_snapshot_observational, "the publication takes the authoritative route");
    assert_eq!(snapshot.usage_snapshot_converged, Some(true));
    assert!(snapshot.usage_snapshot_pending_buckets.is_empty());
    assert!(snapshot.is_complete_bucket_usage_snapshot());
    for bucket in BUCKETS {
        assert_eq!(snapshot.buckets_usage[bucket].objects_count, 1);
    }
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty(), "a converged cycle clears the dirty map");
    let freshness = global_metrics().report().await.usage_freshness;
    assert_eq!(
        freshness.guardrail_veto_total, vetoes_before,
        "a pending PUT tail is not a guardrail veto"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn guardrail_veto_still_supersedes_the_cycle_onto_the_observed_route() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let vetoes_before = global_metrics().report().await.usage_freshness.guardrail_veto_total;

    // A maintenance publication during the walk moves a guardrail field. It
    // is not part of the coverage plan, so bucket checkpoints keep their
    // plan, but the cycle it moved in is still superseded regardless of the
    // dirty map: no cycle records whether it published that work.
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let maintainer = tokio::spawn(async move {
        resolved_scope.await.expect("the cycle should report its resolved scope");
        record_scanner_maintenance_change("hot-bucket");
        release.send(()).expect("the cycle should still be waiting on the gate");
    });
    let (result, snapshot) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: Some(gate),
            peer_observations: None,
        },
    )
    .await;
    maintainer.await.expect("the maintenance task should finish");

    assert_eq!(result.status, ScannerCycleStatus::Superseded);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert!(snapshot.usage_snapshot_observational, "a vetoed cycle routes to the observed object");
    assert_eq!(snapshot.usage_snapshot_converged, Some(false));
    assert!(snapshot.usage_snapshot_pending_buckets.is_empty());
    assert!(snapshot.is_complete_bucket_usage_snapshot());
    assert!(!result.has_dirty_usage_to_acknowledge(), "nothing is certified under a veto");
    assert_eq!(dirty_usage_buckets_for_tests().len(), BUCKETS.len(), "the dirty map is untouched");
    let freshness = global_metrics().report().await.usage_freshness;
    assert_eq!(freshness.guardrail_veto_total, vetoes_before + 1);
    assert_eq!(
        freshness.last_guardrail_veto_reason,
        format!("maintenance_generation@{}", crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE)
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn remote_hot_bucket_keeps_cold_buckets_certified_and_scopes_the_next_cycle() {
    run_remote_certification_cycle(false).await;
}

#[tokio::test]
#[serial]
async fn a_verified_empty_ack_on_a_full_scope_keeps_the_remote_hot_bucket_pending() {
    run_remote_certification_cycle(true).await;
}

async fn run_remote_certification_cycle(empty_ack: bool) {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    let mut incarnations = BTreeMap::new();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("fixture bucket");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
        incarnations.insert(
            bucket.to_string(),
            store.all_set_disks()[0]
                .find_bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("read actual incarnation")
                .expect("fixture bucket has an incarnation"),
        );
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let peers = Arc::new(StdMutex::new(BTreeMap::from([
        (
            "peer-a:9000".to_string(),
            peer_dirty_usage_snapshot(&Uuid::from_u128(101).to_string(), 7, true, &[]),
        ),
        (
            "peer-b:9000".to_string(),
            peer_dirty_usage_snapshot(&Uuid::from_u128(102).to_string(), 7, true, &[]),
        ),
    ])));
    // These are the RPC observations only. Bucket listing, traversal, cache
    // coverage, classification and candidate production use the real cycle.
    let activity_peers = Arc::clone(&peers);
    let dirty_peers = Arc::clone(&peers);
    let observations = super::super::io_cycle::ScannerCyclePeerObservations {
        activity: Arc::new(move || {
            activity_peers
                .lock()
                .expect("peer fixture lock")
                .iter()
                .map(|(host, peer)| {
                    (
                        host.clone(),
                        crate::scanner::scanner_node_activity_for_tests(
                            &peer.instance_id,
                            peer.generation,
                            peer.generation,
                            !peer.buckets.is_empty(),
                        ),
                    )
                })
                .collect()
        }),
        dirty_usage: Arc::new(move || {
            dirty_peers
                .lock()
                .expect("peer fixture lock")
                .iter()
                .map(|(host, snapshot)| (host.clone(), snapshot.clone()))
                .collect()
        }),
        scoped_ack_capable: true,
    };
    let (result, baseline) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
            peer_observations: Some(observations.clone()),
        },
    )
    .await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert!(result.clear_verified_usage().is_empty());
    assert!(baseline.is_complete_bucket_usage_snapshot());
    let initial_dirty = if empty_ack {
        vec!["hot-bucket"]
    } else {
        vec!["cold-bucket", "idle-bucket"]
    };
    for bucket in initial_dirty {
        put_fixture_object(&store, bucket, "second").await;
        let mut locked = peers.lock().expect("peer fixture lock");
        let peer = locked.get_mut("peer-a:9000").expect("remote producer exists");
        peer.generation += 1;
        peer.buckets.insert(
            bucket.to_string(),
            crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                bucket_incarnation: incarnations[bucket],
                generation: peer.generation,
            },
        );
        peer.pending_bucket_count = u64::try_from(peer.buckets.len()).expect("bounded fixture");
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let writer_store = Arc::clone(&store);
    let writer_peers = Arc::clone(&peers);
    let hot_incarnation = incarnations["hot-bucket"];
    let writer = tokio::spawn(async move {
        let scope = resolved_scope.await.expect("real cycle resolved its scope");
        put_fixture_object(&writer_store, "hot-bucket", "during-walk").await;
        {
            let mut locked = writer_peers.lock().expect("peer fixture lock");
            let peer = locked.get_mut("peer-a:9000").expect("remote producer exists");
            peer.generation += 1;
            peer.buckets.insert(
                "hot-bucket".to_string(),
                crate::storage_api::EcstoreScannerPeerDirtyUsageBucket {
                    bucket_incarnation: hot_incarnation,
                    generation: peer.generation,
                },
            );
            peer.pending_bucket_count = u64::try_from(peer.buckets.len()).expect("bounded fixture");
        }
        wait_for_namespace_commit_tails(writer_store.as_ref()).await;
        release.send(()).expect("cycle awaits its actual post-walk proof");
        scope
    });
    let (result, published) = run_cycle(
        &store,
        2,
        (!empty_ack).then(|| baseline_bytes(&baseline)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: Some(gate),
            peer_observations: Some(observations.clone()),
        },
    )
    .await;
    let scope = writer.await.expect("writer completed");
    if empty_ack {
        assert!(scope.is_default(), "this case exercises the full-scope generation fallback");
    } else {
        assert_eq!(
            scope.selected_buckets_for_tests(),
            Some(&HashSet::from(["cold-bucket".to_string(), "idle-bucket".to_string()]))
        );
    }
    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "one remote hot bucket cannot supersede certified cold buckets"
    );
    assert_eq!(published.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert_eq!(published.usage_snapshot_converged, Some(false));
    assert!(!published.usage_snapshot_observational);
    assert_eq!(published.buckets_usage["cold-bucket"].objects_count, if empty_ack { 1 } else { 2 });
    assert_eq!(published.buckets_usage["idle-bucket"].objects_count, if empty_ack { 1 } else { 2 });
    if !empty_ack {
        assert_eq!(published.buckets_usage["hot-bucket"].objects_count, 1);
    }
    assert!(result.has_remote_dirty_usage_pending());
    let acknowledgements = &result.remote_dirty_usage_acknowledgements;
    if empty_ack {
        assert!(acknowledgements.is_empty(), "a verified empty scoped ACK cannot become a generation ACK");
    } else {
        assert_eq!(acknowledgements.len(), 1, "the clean peer is proved without a fabricated ACK");
        let crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped { owner_id, entries } = &acknowledgements[0].kind else {
            panic!("certified remote buckets require exact scoped ACKs");
        };
        assert_eq!(
            entries.iter().map(|entry| entry.bucket.as_str()).collect::<BTreeSet<_>>(),
            BTreeSet::from(["cold-bucket", "idle-bucket"])
        );
        {
            let mut locked = peers.lock().expect("peer fixture lock");
            let peer = locked
                .get_mut(&acknowledgements[0].host)
                .expect("ACK targets the original peer");
            assert_eq!(&peer.owner_id, owner_id);
            assert_eq!(peer.instance_id, acknowledgements[0].instance_id);
            // Model the RPC receiver only after asserting the production sender's
            // exact scope; this does not claim runtime publication/receiver proof.
            for entry in entries {
                let current = peer.buckets.get(&entry.bucket).expect("ACK names an observed tuple");
                assert_eq!(
                    (current.bucket_incarnation, current.generation),
                    (entry.bucket_incarnation, entry.generation)
                );
                peer.buckets.remove(&entry.bucket);
            }
            peer.pending_bucket_count = u64::try_from(peer.buckets.len()).expect("bounded fixture");
        }
    }
    let _ = result.clear_verified_usage();
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (result, next) = run_cycle(
        &store,
        3,
        Some(baseline_bytes(&published)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: None,
            peer_observations: Some(observations),
        },
    )
    .await;
    assert_eq!(
        resolved_scope.await.expect("next cycle scope").selected_buckets_for_tests(),
        Some(&HashSet::from(["hot-bucket".to_string()]))
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(next.buckets_usage["hot-bucket"].objects_count, if empty_ack { 3 } else { 2 });
    assert_eq!(next.usage_snapshot_converged, Some(true));
    clear_dirty_usage_buckets_for_tests();
}
