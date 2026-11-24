// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::storage_api::owner::ObjectOperations as _;
use rustfs_data_usage::{BucketUsageCertification, BucketUsageInfo, CertifiedBucketsScope, CertifiedBucketsSource};

async fn fixture() -> (tempfile::TempDir, Arc<ECStore>, DataUsageInfo, DataUsageInfo) {
    let (directory, store) = setup_scanner_cycle_store_with_usage_baseline(false).await;
    for name in ["large", "small"] {
        store
            .make_bucket(name, &Default::default())
            .await
            .expect("create snapshot fixture bucket");
    }
    let large_id = store
        .bucket_incarnation_id_from_disk("large")
        .await
        .expect("large incarnation");
    let small_id = store
        .bucket_incarnation_id_from_disk("small")
        .await
        .expect("small incarnation");
    let measured_at = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut large = BucketUsageInfo {
        objects_count: 10,
        size: 100,
        bucket_incarnation: Some(large_id.to_string()),
        ..Default::default()
    };
    large.certify(BucketUsageCertification {
        epoch: 9,
        cycle: 1,
        certified_at: measured_at,
    });
    let mut primary = DataUsageInfo {
        last_update: Some(measured_at),
        scanner_epoch: Some(9),
        scanner_cycle: Some(1),
        usage_snapshot_complete: true,
        usage_snapshot_initial_view_version: Some(1),
        buckets_count: 1,
        buckets_usage: HashMap::from([("large".into(), large)]),
        ..Default::default()
    };
    assert!(primary.checked_calculate_totals());
    save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&primary).expect("primary encoding"),
    )
    .await
    .expect("persist fixture primary");
    let set = store.all_set_disks().into_iter().next().expect("source set");
    let mut digest = Sha256::new();
    digest.update(set.format.id.as_bytes());
    for disk in &set.format.erasure.sets[set.set_index] {
        digest.update(disk.as_bytes());
    }
    let layout: [u8; 32] = digest.finalize().into();
    let updated_at = measured_at + Duration::from_secs(10);
    let mut small = BucketUsageInfo {
        objects_count: 3,
        size: 30,
        bucket_incarnation: Some(small_id.to_string()),
        ..Default::default()
    };
    small.certify(BucketUsageCertification {
        epoch: 9,
        cycle: 2,
        certified_at: updated_at,
    });
    let mut candidate = DataUsageInfo {
        last_update: Some(updated_at),
        scanner_epoch: Some(9),
        scanner_cycle: Some(2),
        usage_snapshot_partial: true,
        usage_snapshot_converged: Some(false),
        buckets_count: 1,
        buckets_usage: HashMap::from([("small".into(), small)]),
        usage_snapshot_authoritative_baseline: Some(primary.snapshot_identity()),
        usage_snapshot_certified_buckets: Some(CertifiedBucketsScope {
            version: 1,
            baseline: primary.snapshot_identity(),
            baseline_initial_bootstrap: false,
            baseline_bootstrap_pending: false,
            baseline_rebuild_generation: None,
            movement_generation: store
                .scanner_data_usage_movement_generation()
                .await
                .expect("durable movement"),
            sources: vec![CertifiedBucketsSource {
                pool_index: 0,
                set_index: 0,
                layout,
            }],
            bucket_incarnations: HashMap::from([("large".into(), large_id.to_string()), ("small".into(), small_id.to_string())]),
        }),
        ..Default::default()
    };
    assert!(candidate.checked_calculate_totals());
    assert!(candidate.is_certified_buckets_snapshot());
    (directory, store, primary, candidate)
}

async fn publish(store: Arc<ECStore>, candidate: DataUsageInfo) -> usage_store::DataUsagePublicationResult {
    let epoch = scanner_publication_epoch(store.clone()).await.expect("publication epoch");
    let (sender, receiver) = mpsc::channel(1);
    sender.send(candidate).await.expect("enqueue completed bucket snapshot");
    drop(sender);
    store_data_usage_in_backend_with_outcome_for_epoch_and_baseline_and_route_probe_for_publication_epoch_and_lease_fence(
        CancellationToken::new(),
        store,
        receiver,
        Some(9),
        None,
        ScannerPublicationFence::new(Some(epoch), None, None),
        || async { None },
    )
    .await
}

#[tokio::test]
#[serial]
async fn completed_bucket_snapshot_persists_history_and_survives_reopen_without_mutating_primary_values() {
    let (_directory, store, primary, candidate) = fixture().await;
    let publication = publish(store.clone(), candidate).await;
    let (outcome, proof) = publication.into_parts();
    assert_eq!(outcome, DataUsagePersistOutcome::Saved);
    assert!(proof.is_none(), "a bucket sidecar cannot acknowledge the canonical root");
    let saved: DataUsageInfo = serde_json::from_slice(
        &read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("read primary"),
    )
    .expect("primary JSON");
    assert_eq!(saved.buckets_usage, primary.buckets_usage);
    assert_eq!((saved.last_update, saved.objects_total_count), (primary.last_update, 10));
    assert!(saved.usage_snapshot_noninitial_buckets.contains_key("small"));
    let reopened = restart_scanner_cycle_store_from(&store).await;
    let view = crate::storage_api::ecstore_load_admin_usage_snapshot(reopened.clone(), &["large".into(), "small".into()])
        .await
        .expect("read reopened strict snapshot");
    assert_eq!((view.usage.buckets_count, view.usage.objects_total_count), (2, 13));
    assert_eq!(
        view.usage.buckets_usage["large"].usage_certification,
        primary.buckets_usage["large"].usage_certification
    );
    assert!(view.initial_scan_progress.is_empty());
    reopened
        .delete_object(
            RUSTFS_META_BUCKET,
            DATA_USAGE_CERTIFIED_BUCKETS_OBJ_NAME_PATH.as_str(),
            Default::default(),
        )
        .await
        .expect("lose sidecar without losing primary history");
    let lost = crate::storage_api::ecstore_load_admin_usage_snapshot(reopened, &["large".into(), "small".into()])
        .await
        .expect("read primary after sidecar loss");
    assert_eq!(lost.usage.buckets_usage["large"].objects_count, 10);
    assert_eq!(lost.unmeasured_buckets, ["small"]);
    assert!(lost.initial_scan_progress.is_empty());
}

#[tokio::test]
#[serial]
async fn completed_bucket_snapshot_rejects_changed_baseline_without_writing_history_or_sidecar() {
    let (_directory, store, primary, mut candidate) = fixture().await;
    candidate
        .usage_snapshot_certified_buckets
        .as_mut()
        .expect("scope")
        .baseline
        .last_update = Some(std::time::SystemTime::UNIX_EPOCH);
    candidate.usage_snapshot_authoritative_baseline =
        Some(candidate.usage_snapshot_certified_buckets.as_ref().expect("scope").baseline);
    let publication = publish(store.clone(), candidate).await;
    assert_eq!(publication.outcome(), DataUsagePersistOutcome::Failed);
    let saved: DataUsageInfo = serde_json::from_slice(
        &read_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("read primary"),
    )
    .expect("primary JSON");
    assert_eq!(saved, primary);
    assert!(matches!(
        read_config(store, DATA_USAGE_CERTIFIED_BUCKETS_OBJ_NAME_PATH.as_str()).await,
        Err(EcstoreError::ConfigNotFound)
    ));
}
