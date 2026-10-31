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

use super::registration_tree::{RegistrationTree, RegistrationTreeRoot};
use super::*;
use crate::table_catalog::test_support::{RecordingLeaseClient, TestCatalogObjectBackend};
use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};
use rustfs_lock::LockClient;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn registration_entry(namespace: &Namespace, name: &str) -> TableEntry {
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

async fn registration_root(backend: &TestCatalogObjectBackend, root_path: &str) -> serde_json::Value {
    let object = backend
        .read_object(RUSTFS_META_BUCKET, root_path)
        .await
        .expect("registration root is readable")
        .expect("registration root exists");
    serde_json::from_slice(&object.data).expect("registration root is valid JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_index_rejects_delayed_table_write_after_publication_lease_loss() {
    let backend = TestCatalogObjectBackend {
        registration_index_epoch: Some(Uuid::new_v4()),
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
    let prefix = format!("{}registration-index/", store.paths.table_bucket_root_prefix("analytics"));
    let root_path = format!("{prefix}root.json");
    assert_eq!(registration_root(&backend, &root_path).await["ready"], true);

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
        assert_eq!(registration_root(&backend, &root_path).await["ready"], false);
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
        .expect("replacement writer publishes its table and rebuilt index");
        assert!(backend.list_call_count().await > 0, "takeover rebuilds the dirty registration index");
        assert_eq!(registration_root(&backend, &root_path).await["ready"], true);
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

    // Reservation rollback can invalidate the replacement root. The next
    // registration must rebuild a complete index without the rejected table.
    let subsequent = registration_entry(&active_namespace, "subsequent");
    store.create_table(subsequent.clone()).await.unwrap();
    let root = registration_root(&backend, &root_path).await;
    assert_eq!(root["ready"], true);
    let tree_root: RegistrationTreeRoot = serde_json::from_value(root["tree"].clone()).unwrap();
    let fence = TableCatalogObjectMutationFence::default();
    let tree = RegistrationTree::new(&backend, RUSTFS_META_BUCKET, &prefix, &fence);
    for entry in [&initial, &replacement, &subsequent] {
        let indexed = tree
            .get(&tree_root, &format!("id:{}", entry.table_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexed.namespace, entry.namespace);
        assert_eq!(indexed.table, entry.table);
    }
    assert!(
        tree.get(&tree_root, &format!("id:{}", delayed.table_id))
            .await
            .unwrap()
            .is_none()
    );
}
