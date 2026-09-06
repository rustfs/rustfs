// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::storage_api::owner::ObjectOperations as _;

const BUCKET: &str = "quota-reset-preservation";
const OPERATION: &str = "00000000-0000-0000-0000-000000000002";

async fn reservation_fixture() -> (tempfile::TempDir, Arc<ECStore>, Uuid, String, Vec<u8>) {
    let (directory, store) = setup_scanner_cycle_store().await;
    store
        .make_bucket(BUCKET, &crate::storage_api::scan::MakeBucketOptions::default())
        .await
        .expect("create the reservation fixture bucket through its owner");
    let incarnation = store
        .bucket_incarnation_id_from_disk(BUCKET)
        .await
        .expect("durable bucket incarnation");
    assert!(!incarnation.is_nil());
    let path = format!("config/quota-ledger/{BUCKET}.json");
    let bytes = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "bucket_incarnation": incarnation,
        "quota_revision_unix_nanos": 1,
        "accounted_usage": 100,
        "reservations": {
            OPERATION: {
                "object": "pending-object",
                "old_size": 0,
                "new_size": 64,
                "created_at": 1,
                "pool_index": 0,
                "set_index": 0,
                "commit_started": true
            }
        }
    }))
    .expect("encode the committed reservation fixture");
    save_config(store.clone(), &path, bytes.clone())
        .await
        .expect("persist reservation bytes through the real storage owner");
    (directory, store, incarnation, path, bytes)
}

async fn assert_reservation_retained(store: &Arc<ECStore>, path: &str, expected: &[u8], incarnation: Uuid) {
    let bytes = read_config(store.clone(), path)
        .await
        .expect("read the actual reservation ledger");
    assert_eq!(bytes, expected, "scanner reset must not rewrite the reservation ledger");
    let ledger: serde_json::Value = serde_json::from_slice(&bytes).expect("persisted ledger JSON");
    assert_eq!(ledger["version"], 1);
    assert_eq!(ledger["bucket_incarnation"], incarnation.to_string());
    assert_eq!(ledger["accounted_usage"], 100);
    let reservations = ledger["reservations"].as_object().expect("reservation map");
    assert_eq!(reservations.len(), 1);
    let pending = &reservations[OPERATION];
    assert_eq!(pending["old_size"], 0);
    assert_eq!(pending["new_size"], 64);
    assert_eq!(pending["commit_started"], true);
    assert_eq!(
        store
            .bucket_incarnation_id_from_disk(BUCKET)
            .await
            .expect("owner incarnation after restart"),
        incarnation
    );
}

#[tokio::test]
#[serial]
async fn quota_reset_preservation_survives_storage_owner_reconstruction() {
    let (_directory, store, incarnation, path, bytes) = reservation_fixture().await;
    let reset = reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
        .await
        .expect("reset scanner usage through the fenced production entry");
    assert_eq!(reset.usage_state, "bootstrap-pending");
    let restarted = restart_scanner_cycle_store_from(&store).await;
    assert!(
        !Arc::ptr_eq(&store, &restarted),
        "the assertion must read through a newly constructed ECStore"
    );
    assert_reservation_retained(&restarted, &path, &bytes, incarnation).await;
    let usage = read_config(restarted.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("read reset usage through the reconstructed owner");
    let usage: DataUsageInfo = serde_json::from_slice(&usage).expect("bootstrap usage JSON");
    assert!(data_usage_info_is_bootstrap_pending(&usage));
    assert!(!data_usage_info_has_persisted_baseline_identity(&usage));
}

#[tokio::test]
#[serial]
async fn quota_reset_preservation_unknown_protocol_rejects_put_after_restart() {
    for quota_shape in ["zero", "null", "missing"] {
        let (_directory, store, incarnation, path, bytes) = reservation_fixture().await;
        let mut quota = serde_json::json!({
            "quota_type": "Hard",
            "reservation_protocol": 2,
            "reservation_quota": 1024
        });
        match quota_shape {
            "zero" => quota["quota"] = serde_json::json!(0),
            "null" => quota["quota"] = serde_json::Value::Null,
            "missing" => {}
            _ => unreachable!("fixed quota shapes"),
        }
        let unknown_quota = serde_json::to_vec(&quota).expect("unknown but syntactically valid quota protocol");
        store
            .update_bucket_metadata_config(BUCKET, rustfs_config::QUOTA_CONFIG_FILE, unknown_quota)
            .await
            .expect("persist a future protocol using the real metadata owner");
        assert_eq!(
            store
                .bucket_incarnation_id_from_disk(BUCKET)
                .await
                .expect("same metadata owner incarnation"),
            incarnation
        );
        reset_scanner_usage_state_for_full_rebuild(CancellationToken::new(), store.clone())
            .await
            .expect("scanner reset must not change quota metadata");
        let restarted = restart_scanner_cycle_store_from(&store).await;
        assert!(!Arc::ptr_eq(&store, &restarted));
        assert_reservation_retained(&restarted, &path, &bytes, incarnation).await;
        let mut reader = PutObjReader::from_vec(b"must-not-commit".to_vec());
        let result = restarted.pools[0].disk_set[0]
            .put_object(BUCKET, "rejected-object", &mut reader, &ObjectOptions::default())
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("unknown reservation protocol with quota={quota_shape} must not admit a PUT"),
        };
        assert!(
            matches!(error, EcstoreError::PartMissingOrCorrupt),
            "unexpected protocol rejection: {error}"
        );
        let missing = restarted.pools[0].disk_set[0]
            .get_object_info(BUCKET, "rejected-object", &ObjectOptions::default())
            .await
            .expect_err("the rejected PUT must not create an object");
        assert!(
            matches!(missing, EcstoreError::FileNotFound | EcstoreError::ObjectNotFound(_, _)),
            "object absence must not be confused with another storage failure: {missing}"
        );
        assert_reservation_retained(&restarted, &path, &bytes, incarnation).await;
    }
}
