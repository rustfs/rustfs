// Copyright 2024 RustFS Team
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

fn planning_report_paths(entry: &TableEntry, job_id: &str) -> [String; 3] {
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse(&entry.namespace).unwrap();
    let table = IdentifierSegment::parse(&entry.table).unwrap();
    [
        paths.table_maintenance_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id, job_id),
        paths.table_maintenance_current_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id),
        paths.table_maintenance_latest_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id),
    ]
}

async fn assert_only_planning_sidecars_changed(
    backend: &TestCatalogObjectBackend,
    before: BTreeMap<(String, String), TableCatalogObject>,
    entry: &TableEntry,
    report: &TableMetadataMaintenanceReport,
) {
    let permitted = planning_report_paths(entry, &report.job.job_id)
        .map(|path| (RUSTFS_META_BUCKET.to_string(), path))
        .into_iter()
        .collect::<BTreeSet<_>>();
    let after = maintenance_test_object_versions(backend).await;
    for key in &permitted {
        let persisted = after.get(key).expect("all three report views must be published");
        assert_eq!(
            serde_json::from_slice::<TableMetadataMaintenanceReport>(&persisted.data).unwrap(),
            *report
        );
    }
    let unchanged_before = before
        .into_iter()
        .filter(|(key, _)| !permitted.contains(key))
        .collect::<BTreeMap<_, _>>();
    let unchanged_after = after
        .into_iter()
        .filter(|(key, _)| !permitted.contains(key))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        unchanged_after, unchanged_before,
        "planning must not change catalog, metadata, manifests or data"
    );
}

#[tokio::test]
async fn durable_strong_planning_uses_authoritative_entry_and_persists_only_audited_sidecars() {
    for requested_retention in [0, 2] {
        let backend = TestCatalogObjectBackend::default();
        let store = StrongTableCatalogStore::new(backend.clone());
        let entry = seed_strong_maintenance_table(&store, &backend).await;
        let namespace = Namespace::parse(&entry.namespace).unwrap();
        let table = IdentifierSegment::parse(&entry.table).unwrap();
        let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let recent = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        for path in [&orphan, &recent] {
            backend
                .seed_object(&entry.table_bucket, path, br#"{"metadata-log":[]}"#.to_vec())
                .await;
        }
        let config = TableMaintenanceConfig {
            retain_recent_metadata_files: 3,
            delete_enabled: true,
            max_retry_attempts: 2,
            ..Default::default()
        };
        store
            .put_table_maintenance_config(&entry.table_bucket, &entry.namespace, &entry.table, config.clone())
            .await
            .unwrap();
        assert!(
            ObjectTableCatalogStore::new(backend.clone())
                .load_table(&entry.table_bucket, &entry.namespace, &entry.table)
                .await
                .unwrap()
                .is_none()
        );
        let before = maintenance_test_object_versions(&backend).await;
        let report = store
            .run_table_metadata_maintenance_planning(
                &entry.table_bucket,
                &entry.namespace,
                &entry.table,
                requested_retention,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
        assert_eq!(report.job.table_id, entry.table_id);
        assert_eq!(report.current_metadata_location, entry.metadata_location);
        assert_eq!(report.job.current_generation, entry.generation);
        assert_eq!(report.job.config_source, TableMaintenanceConfigSource::TableOverride);
        assert_eq!(report.job.retain_recent_metadata_files, if requested_retention == 0 { 3 } else { 2 });
        assert_eq!(report.job.max_retry_attempts, 2);
        assert_eq!(report.job.worker_id.as_deref(), Some("rustfs-admin"));
        assert_eq!(report.job.attempt, 1);
        assert!(report.job.started_at.is_some() && report.job.finished_at.is_some());
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert_eq!(report.job.deleted_object_count, 0);
        assert_eq!(
            report.cleanup_candidate_locations,
            if requested_retention == 0 { vec![] } else { vec![orphan] }
        );
        for action in [
            TableMaintenanceAuditAction::Planned,
            TableMaintenanceAuditAction::WorkerStarted,
            TableMaintenanceAuditAction::WorkerSucceeded,
        ] {
            assert!(report.audit_events.iter().any(|event| event.action == action));
        }
        assert_only_planning_sidecars_changed(&backend, before, &entry, &report).await;
        let reloaded = StrongTableCatalogStore::new(backend.clone());
        assert_eq!(
            reloaded
                .load_table(&entry.table_bucket, &entry.namespace, &entry.table)
                .await
                .unwrap()
                .unwrap(),
            entry
        );
        assert_eq!(
            reloaded
                .get_table_maintenance_config(&entry.table_bucket, &entry.namespace, &entry.table)
                .await
                .unwrap(),
            config
        );
        assert_eq!(
            reloaded
                .get_table_metadata_maintenance_report(&entry.table_bucket, &entry.namespace, &entry.table, &report.job.job_id)
                .await
                .unwrap()
                .unwrap(),
            report
        );
    }
}

#[tokio::test]
async fn durable_strong_planning_rejects_pending_work_quarantine_and_incomplete_report_views() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let entry = seed_strong_maintenance_table(&store, &backend).await;
    let previous = store
        .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
        .await
        .unwrap();
    let paths = planning_report_paths(&entry, &previous.job.job_id);
    for (status, quarantined) in [
        (TableMetadataMaintenanceJobStatus::Queued, 0),
        (TableMetadataMaintenanceJobStatus::Running, 0),
        (TableMetadataMaintenanceJobStatus::Failed, 0),
        (TableMetadataMaintenanceJobStatus::Successful, 1),
    ] {
        let mut pending = previous.clone();
        pending.job.status = status;
        pending.job.quarantined_object_count = quarantined;
        pending.job.quarantine_enabled = quarantined > 0;
        pending.job.quarantine_retention_seconds = if quarantined > 0 { 86_400 } else { 0 };
        for path in &paths {
            backend
                .seed_object(RUSTFS_META_BUCKET, path, serde_json::to_vec(&pending).unwrap())
                .await;
        }
        let before = maintenance_test_object_versions(&backend).await;
        assert_matches!(
            store
                .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
    for mismatched_view in [0, 2] {
        for path in &paths {
            backend
                .seed_object(RUSTFS_META_BUCKET, path, serde_json::to_vec(&previous).unwrap())
                .await;
        }
        let mut inconsistent = previous.clone();
        inconsistent.job.attempt += 1;
        backend
            .seed_object(RUSTFS_META_BUCKET, &paths[mismatched_view], serde_json::to_vec(&inconsistent).unwrap())
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        assert_matches!(
            store
                .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

#[tokio::test]
async fn durable_strong_planning_revalidates_config_entry_and_job_after_unlocked_reads() {
    for change in ["config", "entry", "job", "completed-job"] {
        let backend = TestCatalogObjectBackend::default();
        let store = Arc::new(StrongTableCatalogStore::new(backend.clone()));
        let entry = seed_strong_maintenance_table(&store, &backend).await;
        let pause = backend.pause_next_read("analytics", &entry.metadata_location).await;
        let planning_store = Arc::clone(&store);
        let planning = tokio::spawn(async move {
            planning_store
                .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
                .await
        });
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
            .await
            .expect("planning must reach metadata read");
        match change {
            "config" => {
                let mut config = store
                    .get_table_maintenance_config("analytics", "sales", "orders")
                    .await
                    .unwrap();
                config.max_retry_attempts += 1;
                tokio::time::timeout(
                    TABLE_CATALOG_TEST_TIMEOUT,
                    store.put_table_maintenance_config("analytics", "sales", "orders", config),
                )
                .await
                .expect("planning must release config publication lock")
                .unwrap();
            }
            "entry" => {
                let next = default_table_metadata_file_path(
                    &Namespace::parse("sales").unwrap(),
                    &IdentifierSegment::parse("orders").unwrap(),
                    "00004.metadata.json",
                );
                backend.seed_object("analytics", &next, b"{}".to_vec()).await;
                tokio::time::timeout(
                    TABLE_CATALOG_TEST_TIMEOUT,
                    store.commit_table(TableCommitRequest {
                        table_bucket: "analytics".to_string(),
                        namespace: "sales".to_string(),
                        table: "orders".to_string(),
                        commit_id: "concurrent-planning-commit".to_string(),
                        idempotency_key: None,
                        operation: "append".to_string(),
                        expected_version_token: entry.version_token.clone(),
                        expected_metadata_location: entry.metadata_location.clone(),
                        new_metadata_location: next,
                        requirements: Vec::new(),
                        writer: Some("planning-test".to_string()),
                    }),
                )
                .await
                .expect("planning must release catalog publication lock")
                .unwrap();
            }
            "job" => {
                let winning = tokio::time::timeout(
                    TABLE_CATALOG_TEST_TIMEOUT,
                    store.run_table_maintenance_scheduler_once(
                        "analytics",
                        "sales",
                        "orders",
                        "concurrent-scheduler".to_string(),
                    ),
                )
                .await
                .expect("planning must release job publication lock")
                .unwrap();
                assert_eq!(winning.report.job.status, TableMetadataMaintenanceJobStatus::Queued);
            }
            "completed-job" => {
                let winner = tokio::time::timeout(
                    TABLE_CATALOG_TEST_TIMEOUT,
                    store.run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None),
                )
                .await
                .expect("a second planner must complete while the first reads metadata")
                .unwrap();
                assert_eq!(winner.job.status, TableMetadataMaintenanceJobStatus::Successful);
            }
            _ => unreachable!(),
        }
        let before_publish = maintenance_test_object_versions(&backend).await;
        pause.release();
        let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, planning)
            .await
            .unwrap()
            .unwrap();
        if change == "completed-job" {
            assert_matches!(result, Err(TableCatalogStoreError::Conflict(reason))
                if reason == "current maintenance job changed during planning");
        } else {
            assert_matches!(result, Err(TableCatalogStoreError::Conflict(_)));
        }
        assert_eq!(
            maintenance_test_object_versions(&backend).await,
            before_publish,
            "stale {change} planning must publish nothing"
        );
    }
}

#[tokio::test]
async fn durable_strong_planning_partial_terminal_publication_preserves_safe_retry_boundary() {
    // A named-only completed plan is harmless history. A published latest alias
    // without its current alias is observable incomplete publication and conflicts.
    for failed_view in [1, 2] {
        let backend = TestCatalogObjectBackend::default();
        let store = StrongTableCatalogStore::new(backend.clone());
        let entry = seed_strong_maintenance_table(&store, &backend).await;
        let paths = planning_report_paths(&entry, "unused");
        let before = maintenance_test_object_versions(&backend).await;
        backend.fail_next_put(RUSTFS_META_BUCKET, &paths[failed_view]).await;
        let result = store
            .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
            .await;
        assert!(result.is_err(), "an interrupted report publication must not return success");
        let interrupted = maintenance_test_object_versions(&backend).await;
        for (key, value) in &before {
            assert_eq!(interrupted.get(key), Some(value));
        }
        let (named_key, named_value) = interrupted
            .iter()
            .find(|(key, _)| !before.contains_key(*key) && key.1 != paths[1] && key.1 != paths[2])
            .expect("the completed named report must be retained");
        let original: TableMetadataMaintenanceReport = serde_json::from_slice(&named_value.data).unwrap();
        assert_eq!(original.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(original.job.operation, TableMetadataMaintenanceOperation::DryRun);
        assert_eq!(original.job.deleted_object_count, 0);
        assert_eq!(original.job.deleted_metadata_file_count, 0);
        let retry = store
            .run_table_metadata_maintenance_planning("analytics", "sales", "orders", 0, None, None)
            .await;
        if failed_view == 2 {
            assert_eq!(interrupted.len(), before.len() + 1);
            let retried = retry.expect("named-only completed dry-run history permits safe replanning");
            assert_ne!(retried.job.job_id, original.job.job_id);
            assert_eq!(retried.job.status, TableMetadataMaintenanceJobStatus::Successful);
            assert_eq!(maintenance_test_object_versions(&backend).await.get(named_key), Some(named_value));
            assert_only_planning_sidecars_changed(&backend, interrupted.clone(), &entry, &retried).await;
        } else {
            assert_eq!(interrupted.len(), before.len() + 2);
            assert_matches!(retry, Err(TableCatalogStoreError::Conflict(_)));
            assert_eq!(maintenance_test_object_versions(&backend).await, interrupted);
        }
        assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap().unwrap(), entry);
    }
}
