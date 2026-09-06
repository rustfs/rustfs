// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::super::usage_store::DataUsagePublicationResult;
use super::*;
use crate::scanner_io::ScannerBucketScanScope;
use rustfs_utils::path::path_join_buf;
use sha2::Digest;
use std::time::SystemTime;

const PROOF_BUCKET: &str = "publication-proof-bucket";
const PROOF_EPOCH: u64 = 7;
const PROOF_CYCLE: u64 = 11;

async fn settle_namespace_commits(store: &ECStore) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while store.scanner_data_usage_publication_blocked().await {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fixture namespace commits must settle before collecting complete coverage");
}

async fn complete_candidate(store: &Arc<ECStore>, cycle: u64) -> (crate::scanner_io::ScannerCycleResult, DataUsageInfo) {
    settle_namespace_commits(store).await;
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects: Some(8),
            ..Default::default()
        },
    );
    let (updates, mut receiver) = mpsc::channel(1);
    let result = crate::scanner_io::nsscanner_with_storage_status_scoped(
        store.as_ref(),
        crate::scanner_io::ScannerCycleRequest {
            ctx,
            budget,
            updates,
            want_cycle: cycle,
            leader_epoch: PROOF_EPOCH,
            scan_mode: HealScanMode::Normal,
            scan_scope: ScannerBucketScanScope::default(),
            persisted_usage_baseline: None,
            observed_usage_candidate: None,
            requires_full_scan: true,
            service_cohort: None,
            resolved_scope_observer: None,
        },
    )
    .await
    .expect("real scanner must produce the fixture candidate");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let candidate = receiver.recv().await.expect("complete scanner snapshot");
    assert!(candidate.usage_snapshot_complete);
    assert_eq!(candidate.usage_snapshot_converged, Some(true));
    assert_eq!(candidate.scanner_cycle, Some(cycle));
    assert_eq!(candidate.scanner_epoch, Some(PROOF_EPOCH));
    (result, candidate)
}

async fn candidate_store() -> (tempfile::TempDir, Arc<ECStore>) {
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    let (directory, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    store
        .make_bucket(PROOF_BUCKET, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("create proof fixture bucket through the owner");
    let mut reader = PutObjReader::from_vec(b"proof".to_vec());
    store.pools[0].disk_set[0]
        .put_object(PROOF_BUCKET, "initial", &mut reader, &ObjectOptions::default())
        .await
        .expect("persist fixture object through the owner");
    crate::scanner_io::record_dirty_usage_bucket(PROOF_BUCKET);
    settle_namespace_commits(&store).await;
    (directory, store)
}

async fn read_root(store: &Arc<ECStore>) -> (Option<Vec<u8>>, DataUsageCacheRevision) {
    read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("read actual v2 root bytes and revision")
}

async fn publish_candidate(
    store: &Arc<ECStore>,
    scan: &crate::scanner_io::ScannerCycleResult,
    candidate: DataUsageInfo,
    baseline: Option<DataUsagePersistBaseline>,
) -> DataUsagePublicationResult {
    let expectation = scan.publication_expectation();
    assert!(expectation.is_some(), "only a real complete scan may supply the expectation");
    let (sender, receiver) = mpsc::channel(1);
    sender.send(candidate).await.expect("enqueue the real scan candidate");
    drop(sender);
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
        CancellationToken::new(),
        store.clone(),
        receiver,
        Some(PROOF_EPOCH),
        baseline,
        ScannerPublicationFence::new(scan.publication_epoch(), None, None).with_ack_expectation(expectation),
        || async { None },
    )
    .await
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_companion_only_does_not_authorize_root_ack() {
    for companion in [
        format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
        LEGACY_DATA_USAGE_OBJ_NAME_PATH.to_string(),
        format!("{}.bkp", LEGACY_DATA_USAGE_OBJ_NAME_PATH.as_str()),
    ] {
        let (_directory, store) = candidate_store().await;
        let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
        let bytes = serde_json::to_vec(&candidate).expect("actual candidate JSON");
        save_config(store.clone(), &companion, bytes.clone())
            .await
            .expect("persist the companion on real disks");
        let baseline = read_data_usage_persist_baseline(store.clone())
            .await
            .expect("companion fallback baseline");
        assert_eq!(baseline.data.as_deref(), Some(bytes.as_slice()));
        assert_eq!(baseline.revision, DataUsageCacheRevision::Missing);
        assert_eq!(read_root(&store).await.0, None);
        let dirty = crate::scanner_io::dirty_usage_buckets_for_tests();

        let publication = publish_candidate(&store, &scan, candidate, Some(baseline)).await;
        assert_eq!(publication.outcome(), DataUsagePersistOutcome::AlreadyDurable);
        let (_, pending, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
        assert!(pending, "unacknowledged durable companion work must remain pending");
        assert!(acknowledgements.is_empty());
        assert_eq!(
            crate::scanner_io::dirty_usage_buckets_for_tests(),
            dirty,
            "a companion is not the v2 root target"
        );
        assert_eq!(read_root(&store).await, (None, DataUsageCacheRevision::Missing));
        assert_eq!(read_config(store.clone(), &companion).await.expect("companion retained"), bytes);
    }
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_actual_root_readback_accepts_semantic_json_equivalence() {
    let (_directory, store) = candidate_store().await;
    let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    let canonical = serde_json::to_vec(&candidate).expect("candidate encoding");
    let mut value = serde_json::to_value(&candidate).expect("candidate value");
    value
        .as_object_mut()
        .expect("usage object")
        .insert("fixture_unknown_field".into(), serde_json::json!({"retained": true}));
    let different_bytes = serde_json::to_vec_pretty(&value).expect("noncanonical primary JSON");
    assert_ne!(different_bytes, canonical);
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&different_bytes).expect("semantic primary"),
        candidate
    );
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), different_bytes.clone())
        .await
        .expect("persist actual primary representation");
    let before = read_root(&store).await;
    assert!(matches!(&before.1, DataUsageCacheRevision::Etag(etag) if !etag.is_empty()));
    let baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("real primary revision");
    assert!(crate::scanner_io::dirty_usage_buckets_pending());

    let publication = publish_candidate(&store, &scan, candidate.clone(), Some(baseline)).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::AlreadyDurable);
    let (_, proof) = publication.into_parts();
    let proof = proof.expect("actual primary readback must produce its own root proof");
    let expected = scan.publication_expectation().expect("real scan expectation");
    let (etag, raw_digest) = proof.verified_version_for(&expected).expect("proof must bind this candidate");
    let DataUsageCacheRevision::Etag(expected_etag) = &before.1 else { panic!("actual root ETag") };
    assert_eq!(etag, expected_etag);
    let expected_digest: [u8; 32] = sha2::Sha256::digest(&different_bytes).into();
    assert_eq!(
        *raw_digest, expected_digest,
        "proof must record actual bytes, not reserialized candidate bytes"
    );
    // Obtain another proof through the same real readback path rather than
    // fabricating a publication result from the inspected proof above.
    let publication = publish_candidate(&store, &scan, candidate, None).await;
    let (outcome, _, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(acknowledgements.is_empty(), "the single-node fixture has no remote targets");
    assert!(
        !crate::scanner_io::dirty_usage_buckets_pending(),
        "actual root bytes plus a real revision authorize this scan"
    );
    assert_eq!(read_root(&store).await, before, "readback must not rewrite unknown fields or whitespace");
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_successful_root_cas_authorizes_its_scan() {
    let (_directory, store) = candidate_store().await;
    let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    let baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("initial root revision");
    assert_eq!(baseline.revision, DataUsageCacheRevision::Missing);
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    let publication = publish_candidate(&store, &scan, candidate.clone(), Some(baseline)).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::Saved);
    let (bytes, revision) = read_root(&store).await;
    assert!(matches!(revision, DataUsageCacheRevision::Etag(etag) if !etag.is_empty()));
    assert_eq!(
        serde_json::from_slice::<DataUsageInfo>(&bytes.expect("actual saved root")).expect("root JSON"),
        candidate
    );
    let (outcome, pending, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(!pending);
    assert!(acknowledgements.is_empty(), "the single-node fixture has no remote targets");
    assert!(
        !crate::scanner_io::dirty_usage_buckets_pending(),
        "the real root CAS must authorize its matching scan"
    );
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_observed_candidate_reuse_requires_a_new_root_proof() {
    let (_directory, store) = candidate_store().await;
    let bootstrap = scanner_usage_bootstrap_marker(SystemTime::now(), Some(PROOF_EPOCH));
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&bootstrap).expect("bootstrap root encoding"),
    )
    .await
    .expect("persist authoritative bootstrap root");
    let (prior_scan, mut observed_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    // Seed a complete but unconverged observation from real scanner coverage;
    // the production writer attaches its authoritative baseline identity.
    observed_candidate.usage_snapshot_converged = Some(false);
    let observation = publish_candidate(&store, &prior_scan, observed_candidate, None).await;
    let (outcome, proof) = observation.into_parts();
    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    assert!(proof.is_none(), "an observational write cannot authorize a root ACK");
    let (root_before, revision_before) = read_root(&store).await;
    let observed = read_config(store.clone(), DATA_USAGE_OBSERVED_OBJ_NAME_PATH.as_str())
        .await
        .expect("read real persisted observation");
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let (observer, selected) = tokio::sync::oneshot::channel();
    let scan = crate::scanner_io::nsscanner_with_storage_status_scoped(
        store.as_ref(),
        crate::scanner_io::ScannerCycleRequest {
            ctx,
            budget,
            updates,
            want_cycle: PROOF_CYCLE + 1,
            leader_epoch: PROOF_EPOCH,
            scan_mode: HealScanMode::Normal,
            scan_scope: ScannerBucketScanScope::default(),
            persisted_usage_baseline: root_before.clone().map(Bytes::from),
            observed_usage_candidate: Some(Bytes::from(observed)),
            requires_full_scan: false,
            service_cohort: None,
            resolved_scope_observer: Some(observer),
        },
    )
    .await
    .expect("observation-backed scope must run through the real scanner");
    let scope = selected.await.expect("production resolver decision");
    assert_eq!(scope.selected_buckets_for_tests(), Some(&HashSet::from([PROOF_BUCKET.to_string()])));
    assert_eq!(scan.status, ScannerCycleStatus::Complete);
    let expectation = scan.publication_expectation().expect("reused coverage must be revalidated");
    assert!(
        !expectation.same_candidate(&prior_scan.publication_expectation().expect("prior real candidate")),
        "the observation cannot transfer the previous scan's expectation"
    );
    assert_eq!(read_root(&store).await, (root_before, revision_before));
    assert!(crate::scanner_io::dirty_usage_buckets_pending());
    let candidate = receiver.recv().await.expect("new validated root candidate");
    assert_eq!(candidate.scanner_cycle, Some(PROOF_CYCLE + 1));
    assert_eq!(candidate.usage_snapshot_converged, Some(true));
    let publication = publish_candidate(&store, &scan, candidate, None).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::Saved);
    let (outcome, pending, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
    assert_eq!(outcome, ScannerCycleOutcome::Completed);
    assert!(!pending);
    assert!(acknowledgements.is_empty());
    assert!(!crate::scanner_io::dirty_usage_buckets_pending());
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_stale_root_cas_keeps_dirty_after_bucket_save() {
    let (_directory, store) = candidate_store().await;
    let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    let mut bucket_cache = DataUsageCache::default();
    bucket_cache
        .load(store.pools[0].disk_set[0].clone(), &path_join_buf(&[PROOF_BUCKET, DATA_USAGE_CACHE_NAME]))
        .await
        .expect("real bucket checkpoint must be persisted before root publication");
    assert!(bucket_cache.info.snapshot_complete);
    assert_eq!(
        bucket_cache
            .checked_flatten(PROOF_BUCKET)
            .expect("persisted bucket root")
            .objects,
        1
    );
    let stale_baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("missing root revision");
    assert_eq!(stale_baseline.revision, DataUsageCacheRevision::Missing);
    let mut competing = candidate.clone();
    competing.scanner_epoch = Some(PROOF_EPOCH + 1);
    competing.scanner_cycle = Some(PROOF_CYCLE + 1);
    for state in &mut competing.usage_snapshot_set_states {
        state.scanner_epoch = Some(PROOF_EPOCH + 1);
        state.scanner_cycle = Some(PROOF_CYCLE + 1);
    }
    let competing_bytes = serde_json::to_vec(&competing).expect("competing root");
    save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), competing_bytes.clone())
        .await
        .expect("another publisher wins the actual root slot");
    let before = read_root(&store).await;
    let dirty = crate::scanner_io::dirty_usage_buckets_for_tests();

    let publication = publish_candidate(&store, &scan, candidate, Some(stale_baseline)).await;
    assert_eq!(
        publication.outcome(),
        DataUsagePersistOutcome::Current,
        "the old missing revision loses CAS and reconciles the newer root"
    );
    let (_, _, acknowledgements) = finalize_scanner_cycle_result(scan, publication);
    assert!(acknowledgements.is_empty());
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty);
    assert_eq!(
        read_root(&store).await,
        before,
        "bucket durability must not authorize replacing the winning root"
    );
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_cannot_transfer_proof_between_real_scan_results() {
    let (_directory, store) = candidate_store().await;
    let (first_scan, first_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    let (second_scan, second_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    assert_eq!(first_candidate.scanner_epoch, second_candidate.scanner_epoch);
    assert_eq!(first_candidate.scanner_cycle, second_candidate.scanner_cycle);
    assert_eq!(first_candidate.objects_total_count, second_candidate.objects_total_count);
    let baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("initial root revision");
    let dirty = crate::scanner_io::dirty_usage_buckets_for_tests();
    let publication = publish_candidate(&store, &first_scan, first_candidate, Some(baseline)).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::Saved);
    assert!(read_root(&store).await.0.is_some(), "the first scan really published its root");

    let (_, pending, acknowledgements) = finalize_scanner_cycle_result(second_scan, publication);
    assert!(pending, "another scan's publication must not finish this scan's dirty maintenance work");
    assert!(acknowledgements.is_empty());
    assert_eq!(
        crate::scanner_io::dirty_usage_buckets_for_tests(),
        dirty,
        "same counters and cycle cannot transfer another scan's proof"
    );

    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_stale_baseline_cannot_prove_a_replaced_root() {
    let (_directory, store) = candidate_store().await;
    let (first_scan, first_candidate) = complete_candidate(&store, PROOF_CYCLE).await;
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&first_candidate).expect("first candidate"),
    )
    .await
    .expect("persist the first candidate on real disks");
    let stale_baseline = read_data_usage_persist_baseline(store.clone())
        .await
        .expect("capture the genuine first root revision");
    let dirty = crate::scanner_io::dirty_usage_buckets_for_tests();
    let mut reader = PutObjReader::from_vec(b"second".to_vec());
    store.pools[0].disk_set[0]
        .put_object(PROOF_BUCKET, "second", &mut reader, &ObjectOptions::default())
        .await
        .expect("commit a real namespace change");
    assert_eq!(
        crate::scanner_io::dirty_usage_buckets_for_tests(),
        dirty,
        "direct storage writes leave this fixture's scanner hint generation unchanged"
    );
    let (_, replacement) = complete_candidate(&store, PROOF_CYCLE).await;
    assert_eq!(first_candidate.scanner_epoch, replacement.scanner_epoch);
    assert_eq!(first_candidate.scanner_cycle, replacement.scanner_cycle);
    assert_eq!((first_candidate.objects_total_count, replacement.objects_total_count), (1, 2));
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&replacement).expect("replacement candidate"),
    )
    .await
    .expect("publish the replacement root");
    let current = read_root(&store).await;
    assert_ne!(current.1, stale_baseline.revision);

    // The supplied baseline still equals candidate A, but the actual target
    // now contains B. Compatibility's AlreadyDurable outcome is not proof.
    let publication = publish_candidate(&store, &first_scan, first_candidate, Some(stale_baseline)).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::AlreadyDurable);
    let (_, pending, acknowledgements) = finalize_scanner_cycle_result(first_scan, publication);
    assert!(pending);
    assert!(acknowledgements.is_empty());
    assert_eq!(crate::scanner_io::dirty_usage_buckets_for_tests(), dirty);
    assert_eq!(read_root(&store).await, current);
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_ack_publication_rejects_builder_mutation_after_real_root_publish() {
    for mutation in ["remote_ack_target", "publication_epoch", "remote_lease_targets"] {
        let (_directory, store) = candidate_store().await;
        let (scan, candidate) = complete_candidate(&store, PROOF_CYCLE).await;
        let baseline = read_data_usage_persist_baseline(store.clone())
            .await
            .expect("initial root revision");
        let dirty = crate::scanner_io::dirty_usage_buckets_for_tests();
        let changed_generation = dirty
            .get(PROOF_BUCKET)
            .expect("the real scan has dirty work")
            .checked_add(1)
            .expect("bounded fixture generation");
        let changed_epoch = scan
            .publication_epoch()
            .expect("real scan publication epoch")
            .checked_add(1)
            .expect("bounded fixture epoch");
        let publication = publish_candidate(&store, &scan, candidate.clone(), Some(baseline)).await;
        assert_eq!(publication.outcome(), DataUsagePersistOutcome::Saved, "{mutation}");
        let root_before = read_root(&store).await;
        assert_eq!(
            serde_json::from_slice::<DataUsageInfo>(root_before.0.as_deref().expect("actual saved root"))
                .expect("persisted root JSON"),
            candidate,
            "{mutation}: the original candidate really reached root storage"
        );

        let changed = match mutation {
            "remote_ack_target" => scan.with_remote_dirty_usage_acknowledgements(vec![ScannerDirtyUsageAcknowledgement {
                host: "proof-peer:9000".to_string(),
                instance_id: crate::scanner_activity_epoch().to_string(),
                kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Generation(changed_generation),
            }]),
            "publication_epoch" => scan.with_publication_epoch(Some(changed_epoch)),
            "remote_lease_targets" => scan.with_remote_publication_lease_targets(vec![(
                "proof-peer:9000".to_string(),
                crate::scanner_activity_epoch().to_string(),
                changed_generation,
            )]),
            _ => unreachable!("fixed mutation cases"),
        };
        let (_, pending, acknowledgements) = finalize_scanner_cycle_result(changed, publication);
        assert!(
            acknowledgements.is_empty(),
            "{mutation}: the old root proof must not authorize changed ACK work"
        );
        assert!(pending, "{mutation}: changed maintenance work must remain pending");
        assert_eq!(
            crate::scanner_io::dirty_usage_buckets_for_tests(),
            dirty,
            "{mutation}: the changed scan must not clear local dirty work"
        );
        assert_eq!(read_root(&store).await, root_before, "{mutation}: the durable original root is retained");
    }
    crate::scanner_io::clear_dirty_usage_buckets_for_tests();
}
