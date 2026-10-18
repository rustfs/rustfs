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

#[tokio::test]
async fn durable_strong_planning_renamed_paths_preserve_identity_and_fail_closed() {
    for (target_namespace, delete_content, foreign_manifest) in [
        ("sales", None, false),
        ("archive", None, false),
        ("archive", Some(1), false),
        ("archive", Some(2), false),
        ("archive", None, true),
    ] {
        let backend = TestCatalogObjectBackend::default();
        let store = StrongTableCatalogStore::new(backend.clone());
        let entry = seed_strong_maintenance_table(&store, &backend).await;
        let bucket = entry.table_bucket.as_str();
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
        let data_dir = default_table_data_dir_path(&namespace, &table);
        let manifest_list = format!("{metadata_dir}/snap-20.avro");
        let manifest = if foreign_manifest {
            default_table_metadata_file_path(&namespace, &IdentifierSegment::parse("foreign").unwrap(), "manifest.avro")
        } else {
            format!("{metadata_dir}/manifest-20.avro")
        };
        let left = format!("{data_dir}/left.parquet");
        let right = format!("{data_dir}/right.parquet");
        let delete = format!("{}/delete.parquet", default_table_delete_dir_path(&namespace, &table));
        let mut files = vec![(left.as_str(), 0), (right.as_str(), 0)];
        if let Some(content) = delete_content {
            files.push((delete.as_str(), content));
            backend.seed_object(bucket, &delete, b"delete fixture".to_vec()).await;
        }
        let manifest_bytes = manifest_avro_bytes(&files);
        backend
            .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
            .await;
        backend.seed_object(bucket, &manifest, manifest_bytes).await;
        backend.seed_object(bucket, &left, parquet_i32_bytes(&[1])).await;
        backend.seed_object(bucket, &right, parquet_i32_bytes(&[2])).await;
        backend
            .seed_object(
                bucket,
                &entry.metadata_location,
                serde_json::to_vec(&serde_json::json!({
                    "format-version": 2,
                    "table-uuid": entry.table_uuid,
                    "location": entry.warehouse_location,
                    "metadata-log": [],
                    "snapshots": [
                        {"snapshot-id": 10, "timestamp-ms": 1000, "manifest-list": manifest_list},
                        {"snapshot-id": 20, "timestamp-ms": 2000, "manifest-list": manifest_list}
                    ],
                    "current-snapshot-id": 20,
                    "refs": {"main": {"snapshot-id": 20, "type": "branch"}}
                }))
                .unwrap(),
            )
            .await;
        if target_namespace != "sales" {
            store
                .create_namespace(test_namespace_entry(bucket, &Namespace::parse(target_namespace).unwrap()))
                .await
                .unwrap();
        }
        store
            .rename_table(bucket, "sales", "orders", target_namespace, "renamed")
            .await
            .unwrap();
        let renamed = store.load_table(bucket, target_namespace, "renamed").await.unwrap().unwrap();
        let before = maintenance_test_object_versions(&backend).await;
        let report = store
            .run_table_metadata_maintenance_planning(
                bucket,
                target_namespace,
                "renamed",
                0,
                Some(TableSnapshotExpirationConfig {
                    min_snapshots_to_keep: 1,
                    max_snapshot_age_ms: 1000,
                }),
                Some(TableCompactionPlanningConfig {
                    target_file_size_bytes: 1024 * 1024,
                    small_file_threshold_bytes: 64 * 1024,
                    min_input_files: 2,
                    max_rewrite_bytes_per_job: 2 * 1024 * 1024,
                }),
            )
            .await
            .unwrap();
        assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert_eq!(report.job.deleted_object_count, 0);
        let expiration = report.snapshot_expiration.as_ref().unwrap();
        assert_eq!(expiration.namespace, target_namespace);
        assert_eq!(expiration.table, "renamed");
        assert_eq!(expiration.table_id, entry.table_id);
        assert_eq!(expiration.current_metadata_location, entry.metadata_location);
        assert_eq!(expiration.expiration_candidate_count, 1);
        assert!(expiration.committed_metadata_location.is_none());
        let compaction = report.compaction.as_ref().unwrap();
        assert_eq!(compaction.namespace, target_namespace);
        assert_eq!(compaction.table, "renamed");
        assert_eq!(compaction.table_id, entry.table_id);
        assert_eq!(compaction.current_metadata_location, entry.metadata_location);
        assert_eq!(compaction.current_snapshot_id, Some(20));
        assert!(compaction.committed_metadata_location.is_none());
        if delete_content.is_some() || foreign_manifest {
            assert_eq!(compaction.status, TableCompactionPlanningStatus::ManualReviewRequired);
            assert!(compaction.rewrite_groups.is_empty());
            assert!(compaction.manual_review_count > 0);
            if delete_content.is_some() {
                assert_eq!(compaction.row_level_planning.delete_file_count, 1);
            }
        } else {
            assert_eq!(compaction.status, TableCompactionPlanningStatus::RewriteCandidates);
            assert_eq!(compaction.candidate_file_count, 2);
            assert_eq!(compaction.rewrite_group_count, 1);
            assert_eq!(compaction.rewrite_groups[0].input_file_locations, vec![left, right]);
            assert!(compaction.rewrite_groups[0].output_file_location.is_none());
        }
        let after = maintenance_test_object_versions(&backend).await;
        for (key, value) in &before {
            assert_eq!(after.get(key), Some(value), "planning changed an existing object: {key:?}");
        }
        assert_eq!(after.len(), before.len() + 3, "only named/current/latest reports may be added");
        let reloaded = StrongTableCatalogStore::new(backend.clone());
        assert_eq!(
            reloaded
                .load_table(bucket, target_namespace, "renamed")
                .await
                .unwrap()
                .unwrap(),
            renamed
        );
        for job in [
            report.job.job_id.as_str(),
            MAINTENANCE_JOB_ALIAS_CURRENT,
            MAINTENANCE_JOB_ALIAS_LATEST,
        ] {
            assert_eq!(
                reloaded
                    .get_table_metadata_maintenance_report(bucket, target_namespace, "renamed", job)
                    .await
                    .unwrap()
                    .unwrap(),
                report
            );
        }
    }
}
