// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::storage_api::scan::BucketOperations as _;

fn known(owner: u128, incarnation: Uuid) -> ScannerUsageWatchKnown {
    ScannerUsageWatchKnown {
        owner_id: Uuid::from_u128(owner),
        session_id: Uuid::from_u128(owner + 10),
        watch_id: Uuid::from_u128(owner + 20),
        bucket_incarnation: incarnation,
        revision: 2,
        inflight: 0,
        coverage_version: 1,
        coverage_epoch: 1,
        lease_remaining_ms: 600_000,
    }
}

fn fixture() -> (ScannerUsageWatchProof, BTreeMap<Uuid, Option<String>>) {
    let incarnation = Uuid::from_u128(90);
    let peers = BTreeMap::from([
        (Uuid::from_u128(1), known(1, incarnation)),
        (Uuid::from_u128(2), known(2, incarnation)),
    ]);
    let routes = BTreeMap::from([(Uuid::from_u128(1), None), (Uuid::from_u128(2), Some("peer".into()))]);
    (
        ScannerUsageWatchProof {
            version: WATCH_PROOF_VERSION,
            bucket: "bucket".into(),
            bucket_incarnation: incarnation,
            structure: DataUsageScanPlanDigest([7; 32]),
            peers,
        },
        routes,
    )
}

fn responses(proof: &ScannerUsageWatchProof) -> BTreeMap<Uuid, Vec<ScannerUsageWatchObservation>> {
    proof
        .peers
        .iter()
        .map(|(owner, known)| {
            (
                *owner,
                vec![ScannerUsageWatchObservation {
                    bucket: proof.bucket.clone(),
                    known: Some(known.clone()),
                }],
            )
        })
        .collect()
}

fn renewal_request(proof: &ScannerUsageWatchProof) -> ScannerUsageWatchRequest {
    let local = proof.peers.get(&Uuid::from_u128(1)).expect("fixture local owner");
    ScannerUsageWatchRequest {
        bucket: proof.bucket.clone(),
        bucket_incarnation: proof.bucket_incarnation,
        watch_id: Some(local.watch_id),
        session_id: Some(local.session_id),
        release: false,
    }
}

#[test]
fn renewals_keep_the_original_revision_and_charge_the_full_round_trip() {
    let (proof, routes) = fixture();
    let mut request = renewal_request(&proof);
    let now = Instant::now();
    let mut replies = responses(&proof);
    for entries in replies.values_mut() {
        entries[0].known.as_mut().unwrap().lease_remaining_ms = 50_000;
    }
    let renewed = accept_watch_observations(
        &request,
        &routes,
        replies,
        Some(&proof),
        proof.structure,
        now,
        now + Duration::from_secs(2),
    )
    .expect("unchanged owners renew the same starting proof");
    assert_eq!(
        renewed.start, proof,
        "renewal must not replace the persisted starting revision or lease metadata"
    );
    assert_eq!(renewed.deadline, now + Duration::from_millis(49_750));
    let mut replies = responses(&proof);
    replies.values_mut().next().unwrap()[0]
        .known
        .as_mut()
        .unwrap()
        .lease_remaining_ms = 2_250;
    assert!(
        accept_watch_observations(
            &request,
            &routes,
            replies,
            Some(&proof),
            proof.structure,
            now,
            now + Duration::from_secs(2),
        )
        .is_none(),
        "a lease exhausted by RTT and safety margin is Unknown"
    );
    request.release = true;
    assert!(
        accept_watch_observations(&request, &routes, responses(&proof), Some(&proof), proof.structure, now, now).is_none(),
        "resource retirement cannot establish a clean observation"
    );
}

#[test]
fn every_owner_and_stable_start_field_is_required_for_renewal() {
    let (proof, routes) = fixture();
    let request = renewal_request(&proof);
    let now = Instant::now();
    for field in 0..10 {
        let mut replies = responses(&proof);
        let peer = replies.get_mut(&Uuid::from_u128(2)).unwrap()[0].known.as_mut().unwrap();
        match field {
            0 => peer.owner_id = Uuid::new_v4(),
            1 => peer.session_id = Uuid::new_v4(),
            2 => peer.watch_id = Uuid::new_v4(),
            3 => peer.bucket_incarnation = Uuid::new_v4(),
            4 => peer.revision += 1,
            5 => peer.inflight = 1,
            6 => peer.coverage_version += 1,
            7 => peer.coverage_epoch += 1,
            8 => {
                replies.remove(&Uuid::from_u128(2));
            }
            9 => replies.get_mut(&Uuid::from_u128(2)).unwrap()[0].known = None,
            _ => unreachable!(),
        }
        assert!(
            accept_watch_observations(&request, &routes, replies, Some(&proof), proof.structure, now, now).is_none(),
            "changed or missing field {field} must retain strict coverage"
        );
    }
    let mut fewer = routes;
    fewer.remove(&Uuid::from_u128(2));
    let mut replies = responses(&proof);
    replies.remove(&Uuid::from_u128(2));
    assert!(
        accept_watch_observations(&request, &fewer, replies, Some(&proof), proof.structure, now, now).is_none(),
        "matching response count cannot hide a removed expected owner"
    );
}

#[test]
fn watch_structure_ignores_unrelated_dirty_generations_but_requires_every_owner() {
    let owner = Uuid::from_u128(1);
    let peer = Uuid::from_u128(2);
    let routes = BTreeMap::from([(owner, None), (peer, Some("peer".into()))]);
    let before = BTreeMap::from([
        (
            LOCAL_SCANNER_ACTIVITY_NODE.into(),
            crate::scanner::scanner_node_activity_for_tests("local", 1, 2, true),
        ),
        ("peer".into(), crate::scanner::scanner_node_activity_for_tests("remote", 3, 4, true)),
    ]);
    let original = watch_structure_digest(&before, &routes).unwrap();
    let mut unrelated = before.clone();
    unrelated.insert("peer".into(), crate::scanner::scanner_node_activity_for_tests("remote", 103, 104, false));
    assert_eq!(original, watch_structure_digest(&unrelated, &routes).unwrap());
    assert_ne!(
        crate::scanner::scanner_activity_coverage_digest(&before),
        crate::scanner::scanner_activity_coverage_digest(&unrelated)
    );
    unrelated.remove("peer");
    assert!(watch_structure_digest(&unrelated, &routes).is_none());
}

#[tokio::test]
#[serial_test::serial]
async fn instance_layouts_stay_distinct_even_when_legacy_global_topology_is_equal() {
    let (_first_dir, first) = crate::scanner_io::tests::setup_local_scanner_store(1, 1).await;
    let (_second_dir, second) = crate::scanner_io::tests::setup_local_scanner_store(1, 1).await;
    assert_eq!(
        crate::scanner::scanner_topology_digest(&first),
        crate::scanner::scanner_topology_digest(&second),
        "the legacy facade reads the same process-global endpoints for both stores"
    );
    let activity = BTreeMap::from([(
        LOCAL_SCANNER_ACTIVITY_NODE.into(),
        crate::scanner::scanner_node_activity_for_tests("same-global-process", 1, 2, false),
    )]);
    let first_layout = watch_instance_layout_digest(&first.instance_endpoints().unwrap(), &activity, false).unwrap();
    let second_layout = watch_instance_layout_digest(&second.instance_endpoints().unwrap(), &activity, false).unwrap();
    assert_ne!(first_layout, second_layout, "C5 must bind each instance's own physical endpoint paths");
}

#[test]
fn instance_peer_inventory_rejects_missing_or_substituted_global_notification_peers() {
    let mut local = crate::Endpoint::try_from("http://watch-local.example:9000/disk1").unwrap();
    local.is_local = true;
    let remote = crate::Endpoint::try_from("http://watch-remote.example:9000/disk2").unwrap();
    let endpoints = crate::EndpointServerPools::from(vec![crate::PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: 2,
        endpoints: crate::Endpoints::from(vec![local, remote]),
        cmd_line: "watch-layout".into(),
        platform: String::new(),
    }]);
    let expected_remote = endpoints
        .peer_grid_host_slots_sorted()
        .into_iter()
        .find_map(|(_, grid, local)| (!local).then_some(grid).flatten())
        .unwrap();
    let mut activity = BTreeMap::from([
        (
            LOCAL_SCANNER_ACTIVITY_NODE.into(),
            crate::scanner::scanner_node_activity_for_tests("local", 1, 2, false),
        ),
        (
            expected_remote.clone(),
            crate::scanner::scanner_node_activity_for_tests("remote", 3, 4, false),
        ),
    ]);
    assert!(watch_instance_layout_digest(&endpoints, &activity, true).is_some());
    let remote_activity = activity.remove(&expected_remote).unwrap();
    assert!(watch_instance_layout_digest(&endpoints, &activity, true).is_none());
    activity.insert("http://different-global-peer.example:9000".into(), remote_activity);
    assert!(
        watch_instance_layout_digest(&endpoints, &activity, true).is_none(),
        "a complete but different global peer set cannot cover this store's remote disk"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn releasing_finished_watches_admits_the_257th_real_bucket_without_waiting_for_ttl() {
    let (_dir, store) = crate::scanner_io::tests::setup_local_scanner_store(1, 1).await;
    let cohort = Arc::new(StdMutex::new(ScannerUsageWatchCohort::default()));
    let mut proofs = Vec::new();
    for index in 0..=MAX_WATCHES {
        let bucket = format!("watch-capacity-{index:03}");
        store
            .make_bucket(&bucket, &crate::storage_api::scan::MakeBucketOptions::default())
            .await
            .expect("empty capacity-test bucket");
        if index < MAX_WATCHES {
            let proof = ScannerUsageWatchProof::install_for_test(store.as_ref(), &bucket)
                .await
                .expect("available producer slot");
            let slot = cohort.lock().unwrap().admit(&bucket).expect("available coordinator slot");
            *slot.lock().await = Some(LiveUsageWatch {
                start: proof.clone(),
                deadline: Instant::now() + Duration::from_secs(60),
            });
            proofs.push(proof);
        }
    }
    let waiting = format!("watch-capacity-{MAX_WATCHES:03}");
    assert!(cohort.lock().unwrap().admit(&waiting).is_none());
    assert!(
        ScannerUsageWatchProof::install_for_test(store.as_ref(), &waiting)
            .await
            .is_none()
    );
    let released = proofs.remove(0);
    assert!(
        ScannerUsageWatchReleaseGuard::new(Some(store.clone()), vec![released.clone()])
            .with_cohort(Some(cohort.clone()))
            .finish()
            .await
    );
    assert!(cohort.lock().unwrap().admit(&waiting).is_some());
    let admitted = ScannerUsageWatchProof::install_for_test(store.as_ref(), &waiting)
        .await
        .expect("the waiting bucket enters immediately after exact release");
    assert!(admitted.verify_with_store(store.as_ref()).await);
    assert!(
        !released.verify_with_store(store.as_ref()).await,
        "an old token cannot recreate its released proof"
    );
    proofs.push(admitted);
    assert!(
        ScannerUsageWatchReleaseGuard::new(Some(store), proofs)
            .with_cohort(Some(cohort))
            .finish()
            .await
    );
}

#[tokio::test]
#[serial_test::serial]
async fn background_keepalive_preserves_the_start_until_owner_cancel_and_expiry_is_sticky() {
    let (_dir, store) = crate::scanner_io::tests::setup_local_scanner_store(1, 1).await;
    let bucket = "watch-keepalive";
    store
        .make_bucket(bucket, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("keepalive bucket");
    let proof = ScannerUsageWatchProof::install_for_test(&store, bucket).await.unwrap();
    let cohort = Arc::new(StdMutex::new(ScannerUsageWatchCohort::default()));
    let owner = CancellationToken::new();
    let slot = cohort.lock().unwrap().admit(bucket).unwrap();
    *slot.lock().await = Some(LiveUsageWatch {
        start: proof.clone(),
        deadline: Instant::now() + Duration::from_secs(1),
    });
    {
        let mut state = cohort.lock().unwrap();
        assert!(start_watch_keepalive_with_interval(
            &cohort,
            &mut state,
            &store,
            &owner,
            Duration::from_millis(5),
        ));
    }
    // Shorten only the coordinator's conservative local deadline. Actual
    // storage observations still use the unchanged ten-minute producer lease.
    // The background task must refresh it without any scan admission/EOF call.
    for _ in 0..3 {
        slot.lock().await.as_mut().unwrap().deadline = Instant::now() + Duration::from_secs(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let renewed = slot.lock().await.clone().expect("unchanged watch remains live");
                assert_eq!(renewed.start, proof, "keepalive must not replace the original revision");
                if renewed.deadline > Instant::now() + Duration::from_secs(500) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background keeper renews the real producer without scan progress");
    }
    owner.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !cohort.lock().unwrap().keepalive.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner cancellation stops the keeper");
    let next_cycle_owner = CancellationToken::new();
    let resumed = admit_scanner_usage_watch(&cohort, &store, &next_cycle_owner, bucket, proof.bucket_incarnation)
        .await
        .expect("the next cycle restarts the same-store keeper instead of falling back forever");
    assert_eq!(
        resumed, proof,
        "a surviving lease keeps its original starting proof across owner contexts"
    );
    assert!(!cohort.lock().unwrap().keepalive.as_ref().unwrap().is_finished());
    next_cycle_owner.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !cohort.lock().unwrap().keepalive.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the replacement owner also stops its keeper");
    slot.lock().await.as_mut().unwrap().deadline = Instant::now() - Duration::from_secs(1);
    renew_watch_slots(&store, vec![slot.clone()]).await;
    assert!(slot.lock().await.is_none(), "an expired local lease is never renewed");
    assert!(
        !proof.verify_with_store(&store).await,
        "expiry synchronously revoked the exact local token, even though storage's lease had not expired"
    );
    let fresh = ScannerUsageWatchProof::install_for_test(&store, bucket).await.unwrap();
    assert_ne!(fresh.digest(), proof.digest());
    assert!(proof.release_with_store(&store).await);
    assert!(fresh.verify_with_store(&store).await, "retiring an old token cannot revoke a replacement");
    assert!(fresh.release_with_store(&store).await);
    let weak_cohort = Arc::downgrade(&cohort);
    drop(cohort);
    assert!(weak_cohort.upgrade().is_none(), "the keeper must not retain its coordinator");
}

#[tokio::test]
async fn watch_admission_is_bounded_and_releases_completed_members() {
    let (proof, _) = fixture();
    let mut cohort = ScannerUsageWatchCohort::default();
    for index in 0..MAX_WATCHES {
        let name = format!("bucket-{index:03}");
        let slot = cohort.admit(&name).expect("within watch bound");
        *slot.lock().await = Some(LiveUsageWatch {
            start: ScannerUsageWatchProof {
                bucket: name,
                ..proof.clone()
            },
            deadline: Instant::now() + Duration::from_secs(60),
        });
    }
    assert!(cohort.admit("waiting-bucket").is_none());
    assert_eq!(cohort.slots.len(), MAX_WATCHES);
    assert!(cohort.name_bytes <= MAX_WATCH_NAME_BYTES);
    cohort.retire_completed(&HashSet::from(["bucket-000".into()]));
    assert!(
        cohort.admit("waiting-bucket").is_some(),
        "completion frees admission for the existing service order"
    );
}

#[test]
fn persisted_checkpoint_keeps_the_starting_watch_across_mixed_requests() {
    let (first, _) = fixture();
    let start_plan = first.digest();
    let mut cache = DataUsageCache::default();
    cache.info.scan_progress = Some(crate::DataUsageScanProgress {
        started_plan: start_plan,
        requested_plan: start_plan,
    });
    bind_usage_watch_to_checkpoint(&mut cache, Some(&first), start_plan);
    let mut next = first.clone();
    next.peers.values_mut().next().unwrap().revision += 2;
    let next_plan = next.digest();
    cache.info.scan_progress.as_mut().unwrap().requested_plan = next_plan;
    bind_usage_watch_to_checkpoint(&mut cache, Some(&next), next_plan);
    let bytes = rmp_serde::to_vec_named(&cache).expect("checkpoint serialize");
    let loaded: DataUsageCache = rmp_serde::from_slice(&bytes).expect("checkpoint reload");
    assert_eq!(loaded.info.scan_usage_watch, Some(first));
    assert_eq!(loaded.info.scan_progress.unwrap().started_plan, start_plan);
    assert_eq!(loaded.info.scan_progress.unwrap().requested_plan, next_plan);
}
