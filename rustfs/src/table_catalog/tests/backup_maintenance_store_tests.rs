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

#[derive(Debug, PartialEq)]
struct BackupMaintenanceStoreWrites {
    objects: BTreeMap<(String, String), TableCatalogObject>,
    puts: BTreeMap<(String, String), usize>,
    deletes: BTreeMap<(String, String), usize>,
}

async fn backup_maintenance_store_writes(backend: &TestCatalogObjectBackend) -> BackupMaintenanceStoreWrites {
    let objects = maintenance_test_object_versions(backend).await;
    let state = backend.state.lock().await;
    BackupMaintenanceStoreWrites {
        objects,
        puts: state.put_attempts.clone(),
        deletes: state.delete_attempts.clone(),
    }
}

fn backup_maintenance_store_report_paths(report: &TableMetadataMaintenanceReport) -> [String; 3] {
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse(&report.job.namespace).expect("report namespace");
    let table = IdentifierSegment::parse(&report.job.table).expect("report table");
    [
        paths.table_maintenance_job_path(&report.job.table_bucket, &namespace, &table, &report.job.table_id, &report.job.job_id),
        paths.table_maintenance_latest_job_path(&report.job.table_bucket, &namespace, &table, &report.job.table_id),
        paths.table_maintenance_current_job_path(&report.job.table_bucket, &namespace, &table, &report.job.table_id),
    ]
}

async fn backup_maintenance_store_report(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    job_id: &str,
    status: TableMetadataMaintenanceJobStatus,
) -> TableMetadataMaintenanceReport {
    let mut report = store
        .plan_table_metadata_maintenance("analytics", "sales", "orders", 0)
        .await
        .expect("plan against valid Iceberg metadata through the store");
    report.job.job_id = job_id.to_string();
    report.job.status = status;
    report.job.worker_id = Some("retained-worker".to_string());
    report.job.lease_id = "retained-lease".to_string();
    report.job.attempt = 3;
    let expired = maintenance_timestamp(OffsetDateTime::UNIX_EPOCH);
    report.job.started_at = Some(expired.clone());
    report.job.heartbeat_at = Some(expired);
    report.job.finished_at = if matches!(
        report.job.status,
        TableMetadataMaintenanceJobStatus::Queued | TableMetadataMaintenanceJobStatus::Running
    ) {
        None
    } else {
        Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1)))
    };
    report
}

fn assert_backup_maintenance_store_recovery<T>(result: TableCatalogStoreResult<T>) {
    match result {
        Err(TableCatalogStoreError::Conflict(message)) => {
            assert!(message.contains("maintenance recovery required"), "{message}");
        }
        Err(error) => panic!("expected the maintenance recovery boundary, received {error}"),
        Ok(_) => panic!("an authoritative maintenance job was accepted by backup or restore"),
    }
}

#[tokio::test]
async fn backup_create_fences_expired_authoritative_jobs_without_writing_or_repairing_aliases() {
    for status in [
        TableMetadataMaintenanceJobStatus::Queued,
        TableMetadataMaintenanceJobStatus::Running,
    ] {
        let (backend, _, _) = backup_fixture().await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        let mut report = backup_maintenance_store_report(&store, "expired-authoritative-job", status).await;
        store
            .put_table_metadata_maintenance_report(&report)
            .await
            .expect("publish named, latest and current authority");
        let before = backup_maintenance_store_writes(&backend).await;

        assert_backup_maintenance_store_recovery(
            ObjectTableCatalogStore::new(backend.clone())
                .create_object_catalog_backup("analytics", None)
                .await,
        );
        assert_eq!(
            backup_maintenance_store_writes(&backend).await,
            before,
            "a backup attempt must neither expire a lease nor repair or rewrite reports"
        );

        report.job.status = TableMetadataMaintenanceJobStatus::Successful;
        report.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH + Duration::seconds(1)));
        store
            .put_table_metadata_maintenance_report(&report)
            .await
            .expect("publish all terminal aliases through the store");
        let backup = ObjectTableCatalogStore::new(backend.clone())
            .create_object_catalog_backup("analytics", None)
            .await
            .expect("the same valid metadata is acceptable after maintenance reaches a stable terminal state");
        assert_eq!(backup.maintenance_object_count, backup_maintenance_store_report_paths(&report).len());
    }
}

// Match the saved v1 field order, as the existing transfer fixtures do, so a
// negative authority fixture reaches the maintenance gate with valid digests.
#[derive(serde::Deserialize, serde::Serialize)]
struct BackupMaintenanceStoreArtifactObject {
    bucket: String,
    object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    data: Vec<u8>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BackupMaintenanceStoreArtifact {
    version: u16,
    backup_id: String,
    table_bucket: String,
    created_at: String,
    source_catalog_etag: String,
    objects: Vec<BackupMaintenanceStoreArtifactObject>,
}

fn backup_maintenance_store_digest(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(<sha2::Sha256 as sha2::Digest>::digest(bytes), hex_simd::AsciiCase::Lower)
}

fn reseal_backup_maintenance_store_artifact(artifact: &mut BackupMaintenanceStoreArtifact) -> Vec<u8> {
    for object in &mut artifact.objects {
        object.size_bytes = u64::try_from(object.data.len()).expect("fixture object length fits u64");
        object.sha256 = backup_maintenance_store_digest(&object.data);
    }
    artifact
        .objects
        .sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    artifact.source_catalog_etag = backup_maintenance_store_digest(
        &serde_json::to_vec(
            &artifact
                .objects
                .iter()
                .map(|object| (&object.bucket, &object.object, &object.sha256))
                .collect::<Vec<_>>(),
        )
        .expect("encode fixture fingerprint"),
    );
    artifact.backup_id = backup_maintenance_store_digest(
        &serde_json::to_vec(&(artifact.version, &artifact.table_bucket, &artifact.source_catalog_etag, &artifact.objects))
            .expect("encode fixture identity"),
    );
    serde_json::to_vec(artifact).expect("encode the saved v1 artifact")
}

#[tokio::test]
async fn unsafe_v1_maintenance_artifact_transfers_raw_but_restore_rejects_it_before_any_write() {
    let (source, baseline, _) = backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let report =
        backup_maintenance_store_report(&source_store, "legacy-running-job", TableMetadataMaintenanceJobStatus::Running).await;
    source_store
        .put_table_metadata_maintenance_report(&report)
        .await
        .expect("publish the legacy running report through the store");
    let saved = source_store
        .export_object_catalog_backup("analytics", &baseline.backup_id)
        .await
        .expect("export the valid metadata baseline");
    let mut legacy: serde_json::Value = serde_json::from_slice(&saved).expect("decode healthy v2 baseline");
    assert_eq!(legacy["version"], 2);
    let references = legacy
        .as_object_mut()
        .unwrap()
        .remove("references")
        .expect("the v2 backup carries an inventory");
    assert!(references.is_array());
    // The fixture's complete current metadata remains embedded in objects.
    // Emit the historical v1 shape and recompute its v1 identity explicitly.
    legacy["version"] = serde_json::json!(1);
    let mut artifact: BackupMaintenanceStoreArtifact = serde_json::from_value(legacy).expect("construct v1 baseline");
    let report_paths = backup_maintenance_store_report_paths(&report);
    for path in &report_paths {
        let object = source.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap();
        artifact.objects.push(BackupMaintenanceStoreArtifactObject {
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: path.clone(),
            size_bytes: u64::try_from(object.data.len()).unwrap(),
            etag: object.etag,
            sha256: backup_maintenance_store_digest(&object.data),
            data: object.data,
        });
    }
    let bytes = reseal_backup_maintenance_store_artifact(&mut artifact);
    let target = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(target.clone());
    let imported = store
        .import_object_catalog_backup("analytics", bytes.clone())
        .await
        .expect("raw import validates the v1 format without granting execution authority");
    assert_eq!(imported.maintenance_object_count, report_paths.len());
    assert_eq!(imported.source_snapshot_version, 1);
    assert_eq!(
        store
            .export_object_catalog_backup("analytics", &imported.backup_id)
            .await
            .unwrap(),
        bytes,
        "export must preserve the exact unsafe historical artifact bytes"
    );
    let before = backup_maintenance_store_writes(&target).await;
    assert_eq!(before.objects.len(), 1, "import must only publish the artifact");

    assert_backup_maintenance_store_recovery(
        ObjectTableCatalogStore::new(target.clone())
            .restore_object_catalog_backup_with_id("analytics", &imported.backup_id, None, false, Some("unsafe-v1-maintenance"))
            .await,
    );
    assert_eq!(
        backup_maintenance_store_writes(&target).await,
        before,
        "rejected restore must not write an intent, barrier, catalog, report or metadata object"
    );
}

#[tokio::test]
async fn backup_restores_terminal_maintenance_bytes_and_replays_its_original_count_after_later_jobs() {
    let (source, _, _) = backup_fixture().await;
    let source_store = ObjectTableCatalogStore::new(source.clone());
    let config = TableMaintenanceConfig {
        background_enabled: true,
        quarantine_enabled: true,
        quarantine_retention_seconds: 86_400,
        max_retry_attempts: 2,
        ..Default::default()
    };
    source
        .seed_object(
            RUSTFS_META_BUCKET,
            &source_store.paths.table_bucket_maintenance_config_path("analytics"),
            serde_json::to_vec(&config).unwrap(),
        )
        .await;
    source_store
        .put_table_maintenance_config("analytics", "sales", "orders", config.clone())
        .await
        .expect("persist table maintenance config");
    let mut terminal =
        backup_maintenance_store_report(&source_store, "terminal-quarantined-job", TableMetadataMaintenanceJobStatus::Failed)
            .await;
    terminal.job.failure_reason = Some("cleanup candidates remain quarantined".to_string());
    terminal.job.quarantine_enabled = true;
    terminal.job.quarantine_retention_seconds = 86_400;
    terminal.job.quarantined_object_count = 2;
    terminal.audit_events.push(TableMaintenanceAuditEvent {
        timestamp: terminal.job.finished_at.clone().expect("terminal timestamp"),
        actor: TableMaintenanceAuditActor::Worker,
        action: TableMaintenanceAuditAction::WorkerFailed,
        reason: terminal.job.failure_reason.clone(),
        before_status: Some(TableMetadataMaintenanceJobStatus::Running),
        after_status: Some(TableMetadataMaintenanceJobStatus::Failed),
        before_quarantined_object_count: Some(0),
        after_quarantined_object_count: Some(2),
        recommended_actions: Vec::new(),
    });
    source_store
        .put_table_metadata_maintenance_report(&terminal)
        .await
        .expect("publish a terminal quarantine report with its audit event");
    let expected_report = source_store
        .get_table_metadata_maintenance_report("analytics", "sales", "orders", &terminal.job.job_id)
        .await
        .unwrap()
        .unwrap();
    let entry = source_store
        .load_table("analytics", "sales", "orders")
        .await
        .unwrap()
        .unwrap();
    let paths = TableCatalogObjectPaths::default();
    let mut sidecar_paths = backup_maintenance_store_report_paths(&terminal).to_vec();
    sidecar_paths.push(paths.table_bucket_maintenance_config_path("analytics"));
    sidecar_paths.push(paths.table_maintenance_config_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
        &entry.table_id,
    ));
    let mut expected_bytes = BTreeMap::new();
    for path in &sidecar_paths {
        let object = source.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap();
        assert!(expected_bytes.insert(path.clone(), object.data).is_none());
    }
    assert_eq!(expected_bytes.len(), 5, "two configs and three report aliases are stored");

    let backup = source_store.create_object_catalog_backup("analytics", None).await.unwrap();
    assert_eq!(backup.maintenance_object_count, expected_bytes.len());
    let raw = source_store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .unwrap();
    let artifact: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(artifact["version"], 2);
    assert!(artifact["references"].is_array(), "new backups include a v2 inventory");
    let target = TestCatalogObjectBackend::default();
    let target_store = ObjectTableCatalogStore::new(target.clone());
    let imported = target_store.import_object_catalog_backup("analytics", raw).await.unwrap();
    assert_eq!(imported.maintenance_object_count, expected_bytes.len());
    let restore_id = "terminal-maintenance-roundtrip";
    let mut first = target_store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("restore stable maintenance through the isolated target store");
    assert_eq!(first.maintenance_object_count, expected_bytes.len());
    assert_eq!(first.object_count, backup.object_count);
    assert_eq!(target_store.load_table("analytics", "sales", "orders").await.unwrap(), Some(entry));
    assert_eq!(
        target_store
            .get_table_maintenance_config("analytics", "sales", "orders")
            .await
            .unwrap(),
        config
    );
    for (path, bytes) in &expected_bytes {
        let object = target.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap();
        assert_eq!(&object.data, bytes, "restoring rewrote config, quarantine or audit bytes: {path}");
    }
    assert_eq!(
        target_store
            .get_table_metadata_maintenance_report("analytics", "sales", "orders", &terminal.job.job_id)
            .await
            .unwrap(),
        Some(expected_report)
    );

    let mut later_config = config;
    later_config.max_retry_attempts = 4;
    target_store
        .put_table_maintenance_config("analytics", "sales", "orders", later_config.clone())
        .await
        .expect("a later normal config update is allowed after restore");
    let later =
        backup_maintenance_store_report(&target_store, "later-successful-job", TableMetadataMaintenanceJobStatus::Successful)
            .await;
    target_store
        .put_table_metadata_maintenance_report(&later)
        .await
        .expect("publish a later normal job through the restored store");
    let later_report = target_store
        .get_table_metadata_maintenance_report("analytics", "sales", "orders", &later.job.job_id)
        .await
        .unwrap()
        .unwrap();
    let before_replay = backup_maintenance_store_writes(&target).await;

    let replay = ObjectTableCatalogStore::new(target.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("completed restore returns its original receipt");
    first.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, first, "the receipt count describes its original restore, not later maintenance");
    assert_eq!(backup_maintenance_store_writes(&target).await, before_replay);
    assert_eq!(
        target_store
            .get_table_maintenance_config("analytics", "sales", "orders")
            .await
            .unwrap(),
        later_config
    );
    assert_eq!(
        target_store
            .get_table_metadata_maintenance_report("analytics", "sales", "orders", &later.job.job_id)
            .await
            .unwrap(),
        Some(later_report)
    );
}

#[tokio::test]
async fn legacy_v1_restore_controls_replay_an_explicit_zero_count_and_reject_a_missing_required_count() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let backup = store
        .import_object_catalog_backup("analytics", include_bytes!("fixtures/object-catalog-backup-v1.json").to_vec())
        .await
        .expect("import the captured original v1 artifact");
    assert_eq!(backup.source_snapshot_version, 1);
    let restore_id = "legacy-v1-maintenance-count";
    let mut first = store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("complete the no-maintenance restore");
    assert_eq!(first.maintenance_object_count, 0);
    let paths = TableCatalogObjectPaths::default();
    let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
    let barrier_path = paths.catalog_restore_state_path("analytics");
    let mut intent = restore_control_json(&backend, &intent_path).await;
    intent["version"] = serde_json::json!(1);
    assert_eq!(
        intent["maintenance_object_count"], 0,
        "the historical v1 intent requires an explicit zero"
    );
    let mut barrier = restore_control_json(&backend, &barrier_path).await;
    barrier["version"] = serde_json::json!(1);
    let fields = barrier.as_object_mut().unwrap();
    assert!(fields.remove("completed_intent_etag").is_some());
    assert!(fields.remove("backup_etag").is_some());
    backend
        .seed_object(RUSTFS_META_BUCKET, &intent_path, serde_json::to_vec(&intent).unwrap())
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &barrier_path, serde_json::to_vec(&barrier).unwrap())
        .await;
    let before = backup_maintenance_store_writes(&backend).await;

    assert!(
        ObjectTableCatalogStore::new(backend.clone())
            .load_table("analytics", "sales", "orders")
            .await
            .expect("read existing v1 controls without a format migration")
            .is_some()
    );
    let replay = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("old v1 receipts remain replayable");
    first.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, first);
    assert_eq!(
        backup_maintenance_store_writes(&backend).await,
        before,
        "reading v1 controls must not rewrite them"
    );

    assert_eq!(
        intent.as_object_mut().unwrap().remove("maintenance_object_count"),
        Some(serde_json::json!(0))
    );
    backend
        .seed_object(RUSTFS_META_BUCKET, &intent_path, serde_json::to_vec(&intent).unwrap())
        .await;
    let before_missing_count = backup_maintenance_store_writes(&backend).await;
    assert!(matches!(
        ObjectTableCatalogStore::new(backend.clone())
            .load_table("analytics", "sales", "orders")
            .await,
        Err(TableCatalogStoreError::Invalid(_) | TableCatalogStoreError::Unavailable(_))
    ));
    assert!(matches!(
        ObjectTableCatalogStore::new(backend.clone())
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
            .await,
        Err(TableCatalogStoreError::Invalid(_) | TableCatalogStoreError::Unavailable(_))
    ));
    assert_eq!(
        backup_maintenance_store_writes(&backend).await,
        before_missing_count,
        "a missing required v1 field must fail closed without defaulting or repairing it"
    );
}
