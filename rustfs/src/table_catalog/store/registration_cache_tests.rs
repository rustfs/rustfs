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
use crate::table_catalog::test_support::{RecordingLeaseClient, TestCatalogObjectBackend};
use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};
use rustfs_lock::LockClient;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) fn registration_entry(namespace: &Namespace, name: &str) -> TableEntry {
    let table = IdentifierSegment::parse(name).expect("valid test table name");
    let mut entry = test_table_entry(
        "analytics",
        namespace,
        &table,
        default_table_metadata_file_path(namespace, &table, "00001.metadata.json"),
    );
    entry.table_id = format!("id-{name}");
    entry.warehouse_location = format!("s3://analytics/tables/id-{name}");
    entry
}

pub(super) async fn cache_fixture() -> (TestCatalogObjectBackend, ObjectTableCatalogStore<TestCatalogObjectBackend>, Namespace) {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").unwrap();
    store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .unwrap();
    store.create_table(registration_entry(&namespace, "orders")).await.unwrap();
    store.create_table(registration_entry(&namespace, "warmup")).await.unwrap();
    (backend, store, namespace)
}

#[tokio::test]
async fn registration_cache_binds_projection_to_the_get_etag() {
    let (backend, store, namespace) = cache_fixture().await;
    backend.registration_cache.clear();
    let path = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    let key = (RUSTFS_META_BUCKET.to_string(), path.clone());
    let original = backend.state.lock().await.objects.get(&key).unwrap().clone();
    let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &path).await;
    let task_store = store.clone();
    let entry = registration_entry(&namespace, "unrelated");
    let task = tokio::spawn(async move { task_store.create_table(entry).await });
    tokio::time::timeout(Duration::from_secs(5), pause.wait_started())
        .await
        .unwrap();
    let mut changed = registration_entry(&namespace, "orders");
    changed.warehouse_location = "s3://analytics/changed".to_string();
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&changed).unwrap())
        .await;
    pause.release();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    backend.state.lock().await.objects.insert(key, original);
    let mut candidate = registration_entry(&namespace, "candidate");
    candidate.warehouse_location = "s3://analytics/changed/child".to_string();
    store
        .create_table(candidate)
        .await
        .expect("a GET projection cannot inherit an earlier inventory ETag");
}

#[tokio::test]
async fn registration_cache_observes_changed_and_removed_objects() {
    let (backend, store, namespace) = cache_fixture().await;
    let path = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    let mut changed = registration_entry(&namespace, "orders");
    changed.warehouse_location = "s3://analytics/relocated".to_string();
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&changed).unwrap())
        .await;
    let mut candidate = registration_entry(&namespace, "candidate");
    candidate.warehouse_location = "s3://analytics/relocated/child".to_string();
    assert!(matches!(
        store.create_table(candidate.clone()).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    backend.delete_object(RUSTFS_META_BUCKET, &path).await.unwrap();
    candidate.table_id = changed.table_id;
    store
        .create_table(candidate)
        .await
        .expect("removed records cannot remain active through the cache");
}

#[tokio::test]
async fn registration_cache_revalidates_missing_etags_and_corrupt_changed_indexes() {
    let (backend, store, namespace) = cache_fixture().await;
    let path = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    backend
        .state
        .lock()
        .await
        .etagless_objects
        .insert((RUSTFS_META_BUCKET.to_string(), path.clone()));
    backend.fail_next_read(RUSTFS_META_BUCKET, &path).await;
    assert!(store.create_table(registration_entry(&namespace, "candidate")).await.is_err());
    backend.state.lock().await.etagless_objects.clear();
    let index = store.paths.warehouse_index_entry_path("analytics", "tables/id-orders/");
    backend
        .seed_object(RUSTFS_META_BUCKET, &index, b"invalid json".to_vec())
        .await;
    assert!(store.create_table(registration_entry(&namespace, "candidate")).await.is_err());
    assert!(store.load_table("analytics", "sales", "candidate").await.unwrap().is_none());
}

#[tokio::test]
async fn registration_cache_rejects_incomplete_and_versioned_inventories() {
    let (backend, store, namespace) = cache_fixture().await;
    let namespace_prefix = store.paths.namespace_entries_prefix("analytics");
    let index_prefix = store.paths.warehouse_index_entries_prefix("analytics");
    for prefix in [&namespace_prefix, &index_prefix] {
        backend.state.lock().await.failed_inventory_prefixes.insert(prefix.clone());
        assert!(store.create_table(registration_entry(&namespace, "candidate")).await.is_err());
        backend.state.lock().await.failed_inventory_prefixes.clear();
    }
    for (version_id, delete_marker) in [(Some(Uuid::new_v4().to_string()), false), (None, true)] {
        backend.state.lock().await.inventory_overrides.insert(
            namespace_prefix.clone(),
            vec![TableCatalogCleanupObject {
                key: format!("{namespace_prefix}unexpected"),
                version_id,
                etag: Some("etag".to_string()),
                delete_marker,
            }],
        );
        assert!(store.create_table(registration_entry(&namespace, "candidate")).await.is_err());
    }
    backend.state.lock().await.inventory_overrides.clear();
    assert!(store.load_table("analytics", "sales", "candidate").await.unwrap().is_none());
    let mut duplicate = registration_entry(&namespace, "candidate");
    duplicate.table_id = "id-orders".to_string();
    assert!(matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_))));
}

#[tokio::test]
async fn registration_cache_cancellation_does_not_publish_a_partial_catalog() {
    let (backend, store, namespace) = cache_fixture().await;
    backend.registration_cache.clear();
    let path = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("warmup").unwrap());
    let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &path).await;
    let task_store = store.clone();
    let candidate = registration_entry(&namespace, "cancelled");
    let task = tokio::spawn(async move { task_store.create_table(candidate).await });
    tokio::time::timeout(Duration::from_secs(5), pause.wait_started())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    pause.release();
    assert!(store.load_table("analytics", "sales", "cancelled").await.unwrap().is_none());
    let mut changed = registration_entry(&namespace, "warmup");
    changed.warehouse_location = "s3://analytics/changed".to_string();
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&changed).unwrap())
        .await;
    let mut candidate = registration_entry(&namespace, "after_cancel");
    candidate.warehouse_location = "s3://analytics/changed/child".to_string();
    assert!(matches!(store.create_table(candidate).await, Err(TableCatalogStoreError::Conflict(_))));
}

#[tokio::test]
async fn registration_cache_isolated_runtimes_do_not_share_matching_etags() {
    let (first, first_store, namespace) = cache_fixture().await;
    let (second, second_store, _) = cache_fixture().await;
    let path = first_store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    let mut changed = registration_entry(&namespace, "orders");
    changed.warehouse_location = "s3://analytics/second-only".to_string();
    let first_etag = first
        .state
        .lock()
        .await
        .objects
        .get(&(RUSTFS_META_BUCKET.to_string(), path.clone()))
        .unwrap()
        .etag
        .clone();
    second.registration_cache.clear();
    {
        let mut state = second.state.lock().await;
        let object = state.objects.get_mut(&(RUSTFS_META_BUCKET.to_string(), path)).unwrap();
        object.data = serde_json::to_vec(&changed).unwrap();
        object.etag = first_etag;
    }
    let mut candidate = registration_entry(&namespace, "candidate");
    candidate.warehouse_location = "s3://analytics/second-only/child".to_string();
    assert!(matches!(
        second_store.create_table(candidate.clone()).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    first_store
        .create_table(candidate)
        .await
        .expect("the first runtime has no such owner");
}

#[tokio::test]
async fn registration_cache_preserves_identity_after_lost_response_and_failed_rollback() {
    for lost_response in [false, true] {
        let (backend, store, namespace) = cache_fixture().await;
        let entry = registration_entry(&namespace, "ambiguous");
        let path = store
            .paths
            .table_entry_path("analytics", &namespace, &IdentifierSegment::parse(&entry.table).unwrap());
        let index = store.paths.warehouse_index_entry_path("analytics", "tables/id-ambiguous/");
        if lost_response {
            backend.fail_after_next_put(RUSTFS_META_BUCKET, &path).await;
        } else {
            backend.fail_next_put(RUSTFS_META_BUCKET, &path).await;
        }
        backend.fail_delete_attempt(RUSTFS_META_BUCKET, &index, 1).await;
        assert!(store.create_table(entry.clone()).await.is_err());
        let mut candidate = registration_entry(&namespace, "candidate");
        candidate.table_id = entry.table_id;
        let result = store.create_table(candidate).await;
        if lost_response {
            assert!(matches!(result, Err(TableCatalogStoreError::Conflict(_))));
        } else {
            result.expect("failed primary writes cannot reserve identity through cached results");
        }
    }
}

async fn leased_registration_publication<'a>(
    backend: &'a TestCatalogObjectBackend,
    lock: &rustfs_lock::NamespaceLock,
    owner: &str,
) -> (
    TableCommitLockPublication<'a, TestCatalogObjectBackend>,
    Arc<rustfs_lock::distributed_lock::LockLostSignal>,
) {
    let guard = lock
        .lock_guard(
            rustfs_lock::ObjectKey::new("analytics", default_table_bucket_publication_lock_path()),
            owner,
            Duration::from_secs(5),
            Duration::from_secs(3),
        )
        .await
        .expect("publication lease acquisition succeeds")
        .expect("publication lease is acquired");
    let signal = guard.lock_lost_signal().expect("distributed publication has a loss signal");
    let publication = TableCommitLockPublication::new(backend);
    {
        let mut state = publication.state.lock();
        state.table_bucket = Some("analytics".to_string());
        state.guards.push(TableCatalogLockGuard::namespace(guard));
    }
    (publication, signal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_cache_rejects_delayed_table_write_after_publication_lease_loss() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let stalled_namespace = Namespace::parse("stalled").unwrap();
    let active_namespace = Namespace::parse("active").unwrap();
    store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
    for namespace in [&stalled_namespace, &active_namespace] {
        store
            .create_namespace(test_namespace_entry("analytics", namespace))
            .await
            .unwrap();
    }
    let initial = registration_entry(&active_namespace, "initial");
    store.create_table(initial.clone()).await.unwrap();
    let client = Arc::new(RecordingLeaseClient {
        inner: rustfs_lock::LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::Enabled(Arc::new(
            rustfs_lock::FastObjectLockManager::new(),
        )))),
        acquired: Mutex::new(None),
    });
    let lock = rustfs_lock::NamespaceLock::with_clients_and_quorum(
        "registration-publication-loss".to_string(),
        vec![client.clone() as Arc<dyn LockClient>],
        1,
    );
    let (old_publication, old_signal) = leased_registration_publication(&backend, &lock, "old-writer").await;
    let old_lease = client.acquired.lock().unwrap().clone().unwrap();
    let delayed = registration_entry(&stalled_namespace, "delayed");
    let delayed_path =
        store
            .paths
            .table_entry_path("analytics", &stalled_namespace, &IdentifierSegment::parse(&delayed.table).unwrap());
    let pause = backend.pause_put_attempt(RUSTFS_META_BUCKET, &delayed_path, 1).await;
    let replacement = registration_entry(&active_namespace, "replacement");
    let delayed_write = store.register_table_with_publication(delayed.clone(), &old_publication);
    let takeover = async {
        tokio::time::timeout(Duration::from_secs(5), pause.wait_started())
            .await
            .expect("old writer reaches the table-entry commit");
        assert!(client.force_release(&old_lease).await.unwrap());
        tokio::time::timeout(Duration::from_secs(10), old_signal.notified())
            .await
            .expect("heartbeat observes publication lease loss");
        assert!(!old_publication.holds_table_bucket("analytics"));

        let (new_publication, _) = leased_registration_publication(&backend, &lock, "new-writer").await;
        backend.reset_call_counts().await;
        tokio::time::timeout(
            Duration::from_secs(5),
            store.register_table_with_publication(replacement.clone(), &new_publication),
        )
        .await
        .expect("replacement writer completes while the old put is paused")
        .expect("replacement writer publishes its table after a fresh inventory");
        assert!(backend.list_call_count().await > 0, "takeover inventories the catalog");
        pause.release();
    };
    let (delayed_result, ()) = tokio::time::timeout(Duration::from_secs(25), async { tokio::join!(delayed_write, takeover) })
        .await
        .expect("both writers finish after the paused put is resumed");
    assert!(
        matches!(delayed_result, Err(TableCatalogStoreError::Unavailable(_))),
        "the delayed storage commit must reject the lost publication fence: {delayed_result:?}"
    );
    assert_eq!(backend.put_attempt_count(RUSTFS_META_BUCKET, &delayed_path).await, 1);
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &delayed_path)
            .await
            .unwrap()
            .is_none()
    );
    for entry in [&initial, &replacement] {
        assert_eq!(
            store.load_table("analytics", &entry.namespace, &entry.table).await.unwrap(),
            Some(entry.clone())
        );
    }

    let subsequent = registration_entry(&active_namespace, "subsequent");
    store.create_table(subsequent.clone()).await.unwrap();
    for entry in [&initial, &replacement, &subsequent] {
        let mut duplicate = registration_entry(&active_namespace, "duplicate");
        duplicate.table_id = entry.table_id.clone();
        assert!(matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_))));
    }
    store
        .create_table(delayed)
        .await
        .expect("the rejected writer left no active table");
}
