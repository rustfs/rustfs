// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use sha2::Digest;

async fn backup_recovery_commit_request(
    backend: &TestCatalogObjectBackend,
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    commit_id: &str,
    metadata_file: &str,
) -> TableCommitRequest {
    let current = store.load_table("analytics", "sales", "orders").await.unwrap().unwrap();
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_path = default_table_metadata_file_path(&namespace, &table, metadata_file);
    let mut metadata = table_metadata_json_for_backup("analytics", &current.table_id, &current.table_uuid);
    metadata["last-updated-ms"] = serde_json::json!(current.generation + 2);
    metadata["properties"]["recovery-fixture-commit"] = serde_json::json!(commit_id);
    validate_supported_table_metadata(&metadata).expect("commit fixture must contain valid Iceberg metadata");
    backend
        .seed_object("analytics", &metadata_path, serde_json::to_vec(&metadata).unwrap())
        .await;
    TableCommitRequest {
        table_bucket: "analytics".to_string(),
        namespace: "sales".to_string(),
        table: "orders".to_string(),
        commit_id: commit_id.to_string(),
        idempotency_key: Some(format!("request-{commit_id}")),
        operation: "append".to_string(),
        expected_version_token: current.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: metadata_path,
        requirements: Vec::new(),
        writer: Some("backup-recovery-fixture".to_string()),
    }
}

fn assert_backup_recovery_conflict(error: &TableCatalogStoreError, expected_state: &str) {
    assert!(
        matches!(error, TableCatalogStoreError::Conflict(message)
        if message.contains("commit recovery required") && message.contains(expected_state)),
        "{error}"
    );
}

async fn assert_unhealthy_backup_has_no_writes(
    backend: &TestCatalogObjectBackend,
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    expected_state: &str,
) {
    let diagnostics = store.diagnose_table_catalog("analytics", "sales", "orders", 0).await.unwrap();
    assert_eq!(
        diagnostics.current_metadata_status,
        TableMetadataPointerStatus::Valid,
        "the regression must reach recovery policy instead of failing metadata validation"
    );
    let before = maintenance_test_object_versions(backend).await;
    let attempts = backend.state.lock().await.put_attempts.clone();
    let error = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, store.create_object_catalog_backup("analytics", None))
        .await
        .expect("backup rejects unstable recovery without waiting for another lock")
        .unwrap_err();
    assert_backup_recovery_conflict(&error, expected_state);
    assert_eq!(
        maintenance_test_object_versions(backend).await,
        before,
        "rejected backup changed persisted objects"
    );
    assert_eq!(backend.state.lock().await.put_attempts, attempts, "rejected backup attempted a write");
}

#[tokio::test]
async fn backup_rejects_pre_cas_staging_until_the_same_commit_retry_converges() {
    // This public fixture already sealed a healthy backup with real metadata.
    let (backend, baseline, _) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let original = store.load_table("analytics", "sales", "orders").await.unwrap().unwrap();
    let request = backup_recovery_commit_request(&backend, &store, "before-cas", "00002.metadata.json").await;
    let table_path = TableCatalogObjectPaths::default().table_entry_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
    );
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert!(matches!(
        store.commit_table(request.clone()).await,
        Err(TableCatalogStoreError::Internal(_))
    ));
    assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap(), Some(original.clone()));
    let recovery = store
        .plan_table_commit_recovery("analytics", "sales", "orders")
        .await
        .unwrap();
    assert_eq!(recovery.staged_before_table_update_count, 1);
    assert_eq!(recovery.commits[0].recovery_state, TableCommitRecoveryState::StagedBeforeTableUpdate);
    assert_unhealthy_backup_has_no_writes(&backend, &store, "StagedBeforeTableUpdate").await;

    let committed = store
        .commit_table(request.clone())
        .await
        .expect("same commit id safely retries its original staging");
    assert_eq!(committed.table.table_uuid, original.table_uuid);
    assert_eq!(committed.table.warehouse_location, original.warehouse_location);
    assert_eq!(committed.table.generation, original.generation + 1);
    assert_eq!(committed.table.metadata_location, request.new_metadata_location);
    assert_eq!(committed.commit_log.status, CommitLogStatus::Committed);
    let replay = store.commit_table(request).await.expect("same-id committed replay");
    assert_eq!(replay.table, committed.table, "replay advanced the catalog a second time");
    assert_eq!(replay.commit_log, committed.commit_log);
    let backup = store
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("healthy retry is sealable");
    assert_ne!(backup.catalog_fingerprint, baseline.catalog_fingerprint);
    let diagnostics = store.diagnose_table_catalog("analytics", "sales", "orders", 0).await.unwrap();
    assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::Healthy);
}

#[tokio::test]
async fn backup_rejects_post_cas_log_or_index_finalization_until_explicit_recovery() {
    for fail_index in [false, true] {
        let (backend, _, _) = backup_fixture().await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        let request = backup_recovery_commit_request(&backend, &store, "after-cas", "00002.metadata.json").await;
        let paths = TableCatalogObjectPaths::default();
        let failed_path = if fail_index {
            paths.commit_idempotency_entry_path("analytics", "table-id", request.idempotency_key.as_deref().unwrap())
        } else {
            paths.commit_log_entry_path("analytics", "table-id", &request.commit_id)
        };
        // First PUT is staging; the second is the durable finalization boundary.
        backend.fail_put_attempt(RUSTFS_META_BUCKET, &failed_path, 2).await;
        let committed = store
            .commit_table(request.clone())
            .await
            .expect("a successful pointer CAS remains a committed response despite finalization failure");
        let recovery = store
            .plan_table_commit_recovery("analytics", "sales", "orders")
            .await
            .unwrap();
        let expected = if fail_index {
            assert_eq!(recovery.idempotency_repair_required_count, 1);
            "IdempotencyIndexRepairRequired"
        } else {
            assert_eq!(recovery.finalization_required_count, 1);
            "FinalizationRequired"
        };
        assert_unhealthy_backup_has_no_writes(&backend, &store, expected).await;
        let table_path = paths.table_entry_path(
            "analytics",
            &Namespace::parse("sales").unwrap(),
            &IdentifierSegment::parse("orders").unwrap(),
        );
        let table_before = backend.read_object(RUSTFS_META_BUCKET, &table_path).await.unwrap().unwrap();
        let metadata_before = backend
            .read_object("analytics", &request.new_metadata_location)
            .await
            .unwrap()
            .unwrap();

        let repaired = store
            .recover_table_commits("analytics", "sales", "orders")
            .await
            .expect("explicit recovery repairs receipts");
        assert_eq!(repaired.finalized_count, 1);
        assert_eq!(repaired.finalization_required_count, 0);
        assert_eq!(repaired.idempotency_repair_required_count, 0);
        assert_eq!(repaired.staged_before_table_update_count, 0);
        assert_eq!(repaired.manual_review_count, 0);
        assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap(), Some(committed.table));
        assert_eq!(
            backend.read_object(RUSTFS_META_BUCKET, &table_path).await.unwrap(),
            Some(table_before),
            "receipt repair rewrote the table pointer, token, generation, bytes or ETag"
        );
        assert_eq!(
            backend
                .read_object("analytics", &request.new_metadata_location)
                .await
                .unwrap(),
            Some(metadata_before)
        );
        store
            .create_object_catalog_backup("analytics", None)
            .await
            .expect("recovered catalog can be sealed");
        let diagnostics = store.diagnose_table_catalog("analytics", "sales", "orders", 0).await.unwrap();
        assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::Healthy);
    }
}

// The historical v1 object field order is part of its identity encoding.
// This fixture encoder deliberately omits the v2 references field entirely.
#[derive(serde::Serialize, serde::Deserialize)]
struct RecoveryLegacyBackupObject {
    bucket: String,
    object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    data: Vec<u8>,
}

fn backup_recovery_digest(data: &[u8]) -> String {
    hex_simd::encode_to_string(sha2::Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

fn reseal_recovery_legacy_artifact(mut artifact: serde_json::Value) -> (String, Vec<u8>) {
    artifact.as_object_mut().unwrap().remove("references");
    artifact["version"] = serde_json::json!(1);
    let mut objects: Vec<RecoveryLegacyBackupObject> = serde_json::from_value(artifact["objects"].clone()).unwrap();
    for object in &mut objects {
        object.size_bytes = u64::try_from(object.data.len()).unwrap();
        object.sha256 = backup_recovery_digest(&object.data);
    }
    objects.sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    let fingerprint = backup_recovery_digest(
        &serde_json::to_vec(
            &objects
                .iter()
                .map(|object| (&object.bucket, &object.object, &object.sha256))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    );
    let id = backup_recovery_digest(&serde_json::to_vec(&(1u16, "analytics", &fingerprint, &objects)).unwrap());
    artifact["source_catalog_etag"] = serde_json::json!(fingerprint);
    artifact["backup_id"] = serde_json::json!(id);
    artifact["objects"] = serde_json::to_value(objects).unwrap();
    (id, serde_json::to_vec(&artifact).unwrap())
}

#[tokio::test]
async fn raw_legacy_v1_import_keeps_pending_commit_state_but_restore_rejects_it_before_any_writes() {
    let (source, baseline, _) = backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let baseline_bytes = source_store
        .export_object_catalog_backup("analytics", &baseline.backup_id)
        .await
        .unwrap();
    let mut artifact: serde_json::Value = serde_json::from_slice(&baseline_bytes).unwrap();
    let request = backup_recovery_commit_request(&source, &source_store, "legacy-pending", "00002.metadata.json").await;
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
    );
    source.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert!(source_store.commit_table(request.clone()).await.is_err());
    for path in [
        paths.commit_log_entry_path("analytics", "table-id", &request.commit_id),
        paths.commit_idempotency_entry_path("analytics", "table-id", request.idempotency_key.as_deref().unwrap()),
    ] {
        let stored = source.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
        artifact["objects"].as_array_mut().unwrap().push(serde_json::json!({
            "bucket": RUSTFS_META_BUCKET,
            "object": path,
            "size_bytes": stored.data.len(),
            "etag": stored.etag,
            "sha256": backup_recovery_digest(&stored.data),
            "data": stored.data,
        }));
    }
    let (backup_id, bytes) = reseal_recovery_legacy_artifact(artifact);
    let target = TestCatalogObjectBackend::default();
    let target_store = ObjectTableCatalogStore::new(target.clone());
    let imported = target_store
        .import_object_catalog_backup("analytics", bytes.clone())
        .await
        .expect("raw import stores a valid legacy candidate without publishing its unsafe recovery state");
    assert_eq!(imported.backup_id, backup_id);
    assert_eq!(imported.source_snapshot_version, 1);
    assert_eq!(
        target_store
            .export_object_catalog_backup("analytics", &backup_id)
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(
        maintenance_test_object_versions(&target).await.len(),
        1,
        "import published more than its artifact"
    );
    let before = maintenance_test_object_versions(&target).await;
    let attempts = target.state.lock().await.put_attempts.clone();
    let error = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some("legacy-recovery"))
        .await
        .unwrap_err();
    assert_backup_recovery_conflict(&error, "StagedBeforeTableUpdate");
    assert_eq!(maintenance_test_object_versions(&target).await, before);
    assert_eq!(target.state.lock().await.put_attempts, attempts);
    assert!(
        !target
            .object_exists(RUSTFS_META_BUCKET, &paths.catalog_restore_state_path("analytics"))
            .await
            .unwrap()
    );
    assert!(
        !target
            .object_exists(RUSTFS_META_BUCKET, &paths.catalog_restore_intent_path("analytics", "legacy-recovery"))
            .await
            .unwrap()
    );
    assert!(target_store.get_table_bucket("analytics").await.unwrap().is_none());
    assert_eq!(
        target_store
            .export_object_catalog_backup("analytics", &backup_id)
            .await
            .unwrap(),
        bytes,
        "failed restore silently repaired or rewrote the historical artifact"
    );
}

#[tokio::test]
async fn allow_replace_cannot_overwrite_a_target_with_pending_commit_recovery() {
    let (source, _, _) = backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let source_request = backup_recovery_commit_request(&source, &source_store, "source-stable", "00002.metadata.json").await;
    source_store.commit_table(source_request).await.unwrap();
    let backup = source_store.create_object_catalog_backup("analytics", None).await.unwrap();
    let bytes = source_store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();

    let (target, _, _) = backup_fixture().await;
    let target_store = ObjectTableCatalogStore::new(target.clone());
    let target_request = backup_recovery_commit_request(&target, &target_store, "target-pending", "00003.metadata.json").await;
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
    );
    target.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert!(target_store.commit_table(target_request).await.is_err());
    target_store.import_object_catalog_backup("analytics", bytes).await.unwrap();
    let before = maintenance_test_object_versions(&target).await;
    let attempts = target.state.lock().await.put_attempts.clone();
    let error = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("replace-pending-target"))
        .await
        .unwrap_err();
    assert_backup_recovery_conflict(&error, "StagedBeforeTableUpdate");
    assert_eq!(maintenance_test_object_versions(&target).await, before);
    assert_eq!(target.state.lock().await.put_attempts, attempts);
    assert!(
        !target
            .object_exists(RUSTFS_META_BUCKET, &paths.catalog_restore_state_path("analytics"))
            .await
            .unwrap()
    );
    assert!(
        !target
            .object_exists(
                RUSTFS_META_BUCKET,
                &paths.catalog_restore_intent_path("analytics", "replace-pending-target")
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn restored_receipts_replay_and_completed_restore_never_rolls_back_a_later_commit() {
    let (source, _, _) = backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let request = backup_recovery_commit_request(&source, &source_store, "portable-receipt", "00002.metadata.json").await;
    let original = source_store.commit_table(request.clone()).await.unwrap();
    let backup = source_store.create_object_catalog_backup("analytics", None).await.unwrap();
    let bytes = source_store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();
    let target = TestCatalogObjectBackend::default();
    let target_store = ObjectTableCatalogStore::new(target.clone());
    target_store.import_object_catalog_backup("analytics", bytes).await.unwrap();
    let mut receipt = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restore-portable-receipts"))
        .await
        .expect("healthy committed receipts restore into an isolated backend");
    assert_eq!(receipt.status, TableCatalogRestoreStatus::Restored);
    assert_eq!(
        target_store.load_table("analytics", "sales", "orders").await.unwrap(),
        Some(original.table.clone())
    );
    let replay = target_store
        .commit_table(request)
        .await
        .expect("restored same-id request returns its committed result");
    assert_eq!(replay.table, original.table);
    assert_eq!(replay.commit_log, original.commit_log);
    assert_eq!(
        target_store
            .get_commit_by_idempotency_key("analytics", "table-id", "request-portable-receipt")
            .await
            .unwrap(),
        Some(original.commit_log)
    );

    let later_request =
        backup_recovery_commit_request(&target, &target_store, "later-pending-finalization", "00003.metadata.json").await;
    let later_log = TableCatalogObjectPaths::default().commit_log_entry_path("analytics", "table-id", &later_request.commit_id);
    target.fail_put_attempt(RUSTFS_META_BUCKET, &later_log, 2).await;
    let later = target_store.commit_table(later_request).await.unwrap();
    assert_eq!(later.table.generation, replay.table.generation + 1);
    assert_eq!(
        target_store
            .plan_table_commit_recovery("analytics", "sales", "orders")
            .await
            .unwrap()
            .finalization_required_count,
        1
    );
    let before = maintenance_test_object_versions(&target).await;
    let attempts = target.state.lock().await.put_attempts.clone();
    receipt.status = TableCatalogRestoreStatus::AlreadyRestored;
    let repeated = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restore-portable-receipts"))
        .await
        .expect("completed receipt wins before evaluating later target recovery state");
    assert_eq!(repeated, receipt);
    assert_eq!(target_store.load_table("analytics", "sales", "orders").await.unwrap(), Some(later.table));
    assert_eq!(
        maintenance_test_object_versions(&target).await,
        before,
        "completed restore rolled back later catalog or receipts"
    );
    assert_eq!(
        target.state.lock().await.put_attempts,
        attempts,
        "completed restore tried to publish again"
    );
    assert_eq!(
        target_store
            .plan_table_commit_recovery("analytics", "sales", "orders")
            .await
            .unwrap()
            .finalization_required_count,
        1
    );
}

struct SupersededBackupFixture {
    backend: TestCatalogObjectBackend,
    loser: TableCommitRequest,
    winner: TableCommitRequest,
    committed: TableCommitResult,
}

async fn superseded_backup_fixture() -> SupersededBackupFixture {
    let (backend, _, _) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let loser = backup_recovery_commit_request(&backend, &store, "losing-backup-cas", "00002.metadata.json").await;
    let table_path = TableCatalogObjectPaths::default().table_entry_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
    );
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert!(matches!(
        store.commit_table(loser.clone()).await,
        Err(TableCatalogStoreError::Internal(_))
    ));
    let winner = backup_recovery_commit_request(&backend, &store, "winning-backup-cas", "00003.metadata.json").await;
    let committed = store.commit_table(winner.clone()).await.unwrap();
    let report = store.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(report.manual_review_count, 0);
    assert_eq!(report.finalized_count, 0);
    assert_eq!(
        report
            .commits
            .iter()
            .find(|entry| entry.commit_id == loser.commit_id)
            .unwrap()
            .recovery_state,
        TableCommitRecoveryState::Superseded
    );
    SupersededBackupFixture {
        backend,
        loser,
        winner,
        committed,
    }
}

#[tokio::test]
async fn backup_seals_proven_losers_without_changing_their_staged_receipts_or_the_winner() {
    let SupersededBackupFixture {
        backend,
        loser,
        winner,
        committed,
    } = superseded_backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let paths = TableCatalogObjectPaths::default();
    let protected = [
        paths.table_entry_path(
            "analytics",
            &Namespace::parse("sales").unwrap(),
            &IdentifierSegment::parse("orders").unwrap(),
        ),
        paths.commit_log_entry_path("analytics", &committed.table.table_id, &loser.commit_id),
        paths.commit_idempotency_entry_path("analytics", &committed.table.table_id, loser.idempotency_key.as_deref().unwrap()),
        paths.commit_log_entry_path("analytics", &committed.table.table_id, &winner.commit_id),
        paths.commit_idempotency_entry_path("analytics", &committed.table.table_id, winner.idempotency_key.as_deref().unwrap()),
    ];
    let mut before = Vec::new();
    for path in &protected {
        before.push(backend.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap());
    }
    let backup = store
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("proven losers are sealable");
    for (path, original) in protected.iter().zip(&before) {
        assert_eq!(
            backend.read_object(RUSTFS_META_BUCKET, path).await.unwrap().as_ref(),
            Some(original),
            "backup rewrote {path}"
        );
    }
    let bytes = store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();
    let artifact: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let archived = artifact["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["object"].as_str() == Some(protected[1].as_str()))
        .unwrap();
    let staged_bytes: Vec<u8> = serde_json::from_value(archived["data"].clone()).unwrap();
    assert_eq!(staged_bytes, before[1].data);
    let staged: CommitLogEntry = serde_json::from_slice(&staged_bytes).unwrap();
    assert_eq!(staged.status, CommitLogStatus::Staged);
    assert_eq!(staged.idempotency_key, loser.idempotency_key);
    assert!(matches!(store.commit_table(loser).await, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap(), Some(committed.table));
}

#[tokio::test]
async fn isolated_restore_retains_loser_proof_retry_conflicts_and_later_commits() {
    let SupersededBackupFixture {
        backend: source,
        loser,
        winner,
        committed,
    } = superseded_backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let backup = source_store.create_object_catalog_backup("analytics", None).await.unwrap();
    let bytes = source_store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();
    let source_before = maintenance_test_object_versions(&source).await;
    let loser_path =
        TableCatalogObjectPaths::default().commit_log_entry_path("analytics", &committed.table.table_id, &loser.commit_id);
    let original_loser = source.read_object(RUSTFS_META_BUCKET, &loser_path).await.unwrap().unwrap();
    let target = TestCatalogObjectBackend::default();
    let target_store = ObjectTableCatalogStore::new(target.clone());
    target_store
        .import_object_catalog_backup("analytics", bytes.clone())
        .await
        .unwrap();
    let mut receipt = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restore-superseded-proof"))
        .await
        .unwrap();
    assert_eq!(receipt.status, TableCatalogRestoreStatus::Restored);
    assert_eq!(
        target_store.load_table("analytics", "sales", "orders").await.unwrap(),
        Some(committed.table.clone())
    );
    let restored_loser = target.read_object(RUSTFS_META_BUCKET, &loser_path).await.unwrap().unwrap();
    assert_eq!(restored_loser.data, original_loser.data);
    let restarted = ObjectTableCatalogStore::new(target.clone());
    let recovery = restarted.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(recovery.manual_review_count, 0);
    assert_eq!(recovery.finalized_count, 0);
    assert_eq!(
        recovery
            .commits
            .iter()
            .find(|entry| entry.commit_id == loser.commit_id)
            .unwrap()
            .recovery_state,
        TableCommitRecoveryState::Superseded
    );
    assert!(matches!(
        restarted.commit_table(loser.clone()).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    let replay = restarted.commit_table(winner).await.unwrap();
    assert_eq!(replay.table, committed.table);
    assert_eq!(replay.commit_log, committed.commit_log);
    assert_eq!(
        target.read_object(RUSTFS_META_BUCKET, &loser_path).await.unwrap(),
        Some(restored_loser.clone())
    );
    let indexed = restarted
        .get_commit_by_idempotency_key("analytics", &committed.table.table_id, loser.idempotency_key.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(indexed.status, CommitLogStatus::Staged);
    assert_eq!(indexed.commit_id, loser.commit_id);

    let next_request = backup_recovery_commit_request(&target, &restarted, "reloaded-after-restore", "00004.metadata.json").await;
    let next = restarted.commit_table(next_request.clone()).await.unwrap();
    assert_eq!(next.table.generation, committed.table.generation + 1);
    assert_eq!(restarted.commit_table(next_request).await.unwrap().table, next.table);
    let versions = maintenance_test_object_versions(&target).await;
    receipt.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(
        restarted
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restore-superseded-proof"))
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        maintenance_test_object_versions(&target).await,
        versions,
        "Completed replay republished old state"
    );
    let restarted = ObjectTableCatalogStore::new(target.clone());
    assert_eq!(restarted.load_table("analytics", "sales", "orders").await.unwrap(), Some(next.table));
    assert_eq!(
        restarted
            .diagnose_table_catalog("analytics", "sales", "orders", 0)
            .await
            .unwrap()
            .recovery_status,
        TableCatalogRecoveryStatus::Healthy
    );
    assert_eq!(target.read_object(RUSTFS_META_BUCKET, &loser_path).await.unwrap(), Some(restored_loser));
    restarted
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("historical winner proof remains sealable after a later commit");
    assert_eq!(
        restarted
            .export_object_catalog_backup("analytics", &backup.backup_id)
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(
        maintenance_test_object_versions(&source).await,
        source_before,
        "isolated target changed the source"
    );
}

#[tokio::test]
async fn raw_v1_restore_rejects_a_loser_whose_winner_proof_is_missing_without_writes() {
    let SupersededBackupFixture {
        backend: source,
        winner,
        committed,
        ..
    } = superseded_backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source);
    let backup = source_store.create_object_catalog_backup("analytics", None).await.unwrap();
    let bytes = source_store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();
    let mut artifact: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let paths = TableCatalogObjectPaths::default();
    let winner_log = paths.commit_log_entry_path("analytics", &committed.table.table_id, &winner.commit_id);
    let winner_index =
        paths.commit_idempotency_entry_path("analytics", &committed.table.table_id, winner.idempotency_key.as_deref().unwrap());
    artifact["objects"].as_array_mut().unwrap().retain(|object| {
        object["object"].as_str() != Some(winner_log.as_str()) && object["object"].as_str() != Some(winner_index.as_str())
    });
    let (backup_id, malformed) = reseal_recovery_legacy_artifact(artifact);
    let target = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(target.clone());
    store
        .import_object_catalog_backup("analytics", malformed.clone())
        .await
        .unwrap();
    let before = maintenance_test_object_versions(&target).await;
    let attempts = target.state.lock().await.put_attempts.clone();
    let error = store
        .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some("missing-loser-proof"))
        .await
        .unwrap_err();
    assert_backup_recovery_conflict(&error, "ManualReview");
    assert_eq!(maintenance_test_object_versions(&target).await, before);
    assert_eq!(target.state.lock().await.put_attempts, attempts);
    assert!(store.get_table_bucket("analytics").await.unwrap().is_none());
    assert_eq!(store.export_object_catalog_backup("analytics", &backup_id).await.unwrap(), malformed);
}
