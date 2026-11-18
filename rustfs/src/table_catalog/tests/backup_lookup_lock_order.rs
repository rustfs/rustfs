// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[tokio::test]
async fn backup_waiting_for_publication_does_not_block_authorization_lookup() {
    for scenario in ["new-bucket", "cold-index", "missing-entry", "warm-index"] {
        let bucket = "analytics";
        let backend = if scenario == "new-bucket" {
            let backend = TestCatalogObjectBackend::default();
            ObjectTableCatalogStore::new(backend.clone())
                .put_table_bucket(test_bucket_entry(bucket))
                .await
                .expect("enable a new empty table bucket");
            backend
        } else {
            backup_fixture().await.0
        };
        let store = ObjectTableCatalogStore::new(backend.clone());
        let paths = TableCatalogObjectPaths::default();
        let index_path = paths.warehouse_index_entry_path(bucket, "tables/table-id/");
        if matches!(scenario, "cold-index" | "missing-entry") {
            backend
                .delete_object(RUSTFS_META_BUCKET, &index_path)
                .await
                .expect("remove index entry");
        }
        if scenario == "cold-index" {
            backend
                .delete_object(RUSTFS_META_BUCKET, &paths.warehouse_index_state_path(bucket))
                .await
                .expect("remove ready marker");
        }
        let publication_path = default_table_bucket_publication_lock_path();
        let reader_guard = backend
            .acquire_read_lock(bucket, &publication_path)
            .await
            .expect("retain S3 publication read guard");
        let publication_attempts = backend.write_lock_acquisition_count(bucket, &publication_path).await;
        let backup_store = store.clone();
        let backup = tokio::spawn(async move { backup_store.create_object_catalog_backup(bucket, None).await });
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
            while backend.write_lock_acquisition_count(bucket, &publication_path).await == publication_attempts {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backup owns its catalog fences and is waiting for publication");
        assert!(!backup.is_finished(), "backup must wait for the S3 request");
        let objects_before = backend.state.lock().await.objects.clone();
        let catalog_lock_attempts = backend.lock_attempts.lock().await.clone();
        let resource = tokio::time::timeout(
            TABLE_CATALOG_TEST_TIMEOUT,
            store.resolve_table_data_plane_resource_for_authorization(bucket, "tables/table-id/data/part.parquet"),
        )
        .await
        .expect("S3 lookup must not wait for the backup fence")
        .expect("authorization lookup succeeds");
        if scenario == "new-bucket" {
            assert!(resource.is_none());
        } else {
            let resource = resource.expect("the table retains data-plane protection");
            assert_eq!(resource.table_id, "table-id");
            assert_eq!(resource.namespace, "sales");
            assert_eq!(resource.table, "orders");
        }
        assert_eq!(
            backend.state.lock().await.objects,
            objects_before,
            "authorization must not mutate or repair the catalog"
        );
        assert_eq!(
            *backend.lock_attempts.lock().await,
            catalog_lock_attempts,
            "authorization must not acquire catalog locks behind publication"
        );
        assert!(!backup.is_finished());
        drop(reader_guard);
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, backup)
            .await
            .expect("backup progresses after S3 completion")
            .expect("backup task joins")
            .expect("backup remains valid");
        if scenario != "new-bucket" {
            let repaired = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part.parquet")
                .await
                .expect("normal catalog lookup can still repair indexes")
                .expect("the table still resolves");
            assert_eq!(repaired.table_id, "table-id");
            assert!(store.warehouse_index_ready(bucket).await.expect("index readiness"));
            assert!(
                backend
                    .read_object(RUSTFS_META_BUCKET, &index_path)
                    .await
                    .expect("read repaired index")
                    .is_some()
            );
        }
    }
}

#[tokio::test]
async fn cold_authorization_lookup_keeps_deleted_table_protection_without_repair() {
    let (backend, _, _) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store.drop_table(bucket, "sales", "orders").await.expect("drop the table");
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_state_path(bucket))
        .await
        .expect("make the warehouse index cold");
    let before = backend.state.lock().await.objects.clone();
    let resource = store
        .resolve_table_data_plane_resource_for_authorization(bucket, "tables/table-id/data/retained.parquet")
        .await
        .expect("read deleted table scope")
        .expect("the tombstone must retain data-plane protection");
    assert_eq!(resource.table_id, "table-id");
    assert_eq!(backend.state.lock().await.objects, before);
    let repaired = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/retained.parquet")
        .await
        .expect("normal lookup may repair the cold index");
    assert_eq!(repaired, Some(resource));
}

#[tokio::test]
async fn cold_authorization_lookup_rejects_overlapping_tables_behind_partial_index() {
    let (backend, _, _) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").expect("namespace");
    let second = IdentifierSegment::parse("other_orders").expect("second table");
    let mut entry = test_table_entry(
        "analytics",
        &namespace,
        &second,
        default_table_metadata_file_path(&namespace, &second, "00001.metadata.json"),
    );
    entry.table_id = "other-id".to_string();
    entry.warehouse_location = "s3://analytics/tables/table-id/".to_string();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.table_entry_path("analytics", &namespace, &second),
            serde_json::to_vec(&entry).expect("encode inconsistent catalog entry"),
        )
        .await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_state_path("analytics"))
        .await
        .expect("clear the incomplete index readiness marker");
    let before = backend.state.lock().await.objects.clone();
    assert_matches!(
        store.resolve_table_data_plane_resource_for_authorization("analytics", "tables/table-id/data/part.parquet").await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("overlapping active table warehouse prefixes")
    );
    assert_eq!(backend.state.lock().await.objects, before);
}
