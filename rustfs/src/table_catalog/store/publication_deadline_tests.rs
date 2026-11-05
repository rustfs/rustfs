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
use crate::table_catalog::store::registration_cache_tests::{cache_fixture, registration_entry};
use crate::table_catalog::test_support::TestCatalogObjectBackend;
use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};

fn is_preparation_timeout<T>(result: &TableCatalogStoreResult<T>) -> bool {
    matches!(result, Err(TableCatalogStoreError::Unavailable(message)) if message.contains("preparation deadline"))
}

#[tokio::test(start_paused = true)]
async fn publication_deadline_registration_inventory_releases_fence_without_writes() {
    let (backend, store, namespace) = cache_fixture().await;
    backend.registration_cache.clear();
    let path = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &path).await;
    let before = backend.state.lock().await.put_attempts.clone();
    let registering = {
        let store = store.clone();
        let entry = registration_entry(&namespace, "candidate");
        tokio::spawn(async move { store.register_table(entry).await })
    };
    pause.wait_started().await;
    let writer = {
        let backend = backend.clone();
        tokio::spawn(async move {
            // Data-plane mutations acquire this same read fence before writing.
            let _guard = backend
                .acquire_read_lock("analytics", &default_table_bucket_publication_lock_path())
                .await?;
            backend
                .put_object(
                    "analytics",
                    "tables/other-tenant/data/new",
                    b"new".to_vec(),
                    TableCatalogPutPrecondition::IfAbsent,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!writer.is_finished());
    tokio::time::advance(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
    assert!(is_preparation_timeout(&registering.await.unwrap()));
    writer.await.unwrap().unwrap();
    pause.release();
    assert!(store.load_table("analytics", "sales", "candidate").await.unwrap().is_none());
    let after = backend.state.lock().await.put_attempts.clone();
    for (key, attempts) in after {
        if key.0 == RUSTFS_META_BUCKET {
            assert_eq!(Some(&attempts), before.get(&key), "timed-out preparation must not stage catalog writes");
        }
    }
    store
        .create_table(registration_entry(&namespace, "after_timeout"))
        .await
        .unwrap();
}

async fn commit_fixture(strong: bool) -> (TestCatalogObjectBackend, Arc<dyn TableCatalogStore>, TableCommitRequest) {
    let backend = TestCatalogObjectBackend::default();
    let store: Arc<dyn TableCatalogStore> = if strong {
        Arc::new(StrongTableCatalogStore::new(backend.clone()))
    } else {
        Arc::new(ObjectTableCatalogStore::new(backend.clone()))
    };
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry("analytics", &namespace, &table, current.clone()))
        .await
        .unwrap();
    backend.seed_object("analytics", &next, b"{}".to_vec()).await;
    (
        backend,
        store,
        TableCommitRequest {
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
            commit_id: "deadline-commit".to_string(),
            idempotency_key: Some("deadline-idempotency".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current,
            new_metadata_location: next,
            requirements: Vec::new(),
            writer: None,
        },
    )
}

// The durable-strong backing does not take part in the preparation deadline; it is retired separately,
// so only the object backing is exercised here.
#[tokio::test(start_paused = true)]
async fn publication_deadline_commit_metadata_read_does_not_publish_object_backing() {
    let (backend, store, request) = commit_fixture(false).await;
    let pause = backend
        .pause_before_next_read("analytics", &request.new_metadata_location)
        .await;
    let before = backend.state.lock().await.put_attempts.clone();
    let committing = {
        let store = store.clone();
        let request = request.clone();
        tokio::spawn(async move { store.commit_table(request).await })
    };
    pause.wait_started().await;
    tokio::time::advance(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
    assert!(is_preparation_timeout(&committing.await.unwrap()));
    assert_eq!(backend.state.lock().await.put_attempts, before);
    assert_eq!(
        store
            .load_table("analytics", "sales", "orders")
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    pause.release();
    // Positive retry exercises parsing a nontrivial valid metadata body.
    backend
        .seed_object(
            "analytics",
            &request.new_metadata_location,
            serde_json::to_vec(&serde_json::json!({"properties": {"padding": "x".repeat(1024 * 1024)}})).unwrap(),
        )
        .await;
    tokio::time::resume();
    let committed = store.commit_table(request.clone()).await.unwrap();
    assert_eq!(committed.table.generation, 2);
    assert_eq!(
        store.commit_table(request).await.unwrap().table.generation,
        2,
        "retry must remain idempotent"
    );
}

#[tokio::test(start_paused = true)]
async fn publication_deadline_create_finishes_after_first_write_on_either_backing() {
    for strong in [false, true] {
        let (backend, store, _) = commit_fixture(strong).await;
        let namespace = Namespace::parse("sales").unwrap();
        let candidate = registration_entry(&namespace, "candidate");
        let paths = TableCatalogObjectPaths::default();
        let first_write = if strong {
            StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path()
        } else {
            paths.warehouse_index_entry_path("analytics", "tables/id-candidate/")
        };
        let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &first_write).await;
        let creating = {
            let store = store.clone();
            tokio::spawn(async move { store.create_table(candidate).await })
        };
        pause.wait_started().await;
        tokio::time::advance(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!creating.is_finished(), "deadline must not cancel a possible durable write");
        pause.release();
        creating.await.unwrap().unwrap();
        assert!(store.load_table("analytics", "sales", "candidate").await.unwrap().is_some());
    }
}

#[tokio::test(start_paused = true)]
async fn publication_deadline_queueing_and_repeated_prepare_share_one_budget() {
    let backend = TestCatalogObjectBackend::default();
    let publication = TableCommitLockPublication::new(&backend);
    let budget = publication.preparation_budget();
    let first_deadline = budget.deadline();
    tokio::time::advance(publication_preparation_timeout() - StdDuration::from_millis(1)).await;
    publication.begin_table_bucket("analytics").await.unwrap();
    publication.prepare("analytics", "sales", "orders").await.unwrap();
    assert_eq!(budget.deadline(), first_deadline);
    let result = publication_preparation(&publication, async {
        publication.prepare("analytics", "sales", "orders").await?;
        tokio::time::sleep(StdDuration::from_millis(2)).await;
        begin_publication_mutation()
    })
    .await;
    assert!(is_preparation_timeout(&result));
    assert!(!publication.holds_table_bucket("analytics"));
    assert_eq!(budget.deadline(), first_deadline, "cleanup must not renew the admission budget");
}

#[tokio::test(start_paused = true)]
async fn publication_deadline_completed_recovery_cannot_exempt_a_new_transaction() {
    let backend = TestCatalogObjectBackend::default();
    let publication = TableCommitLockPublication::new(&backend);
    let result = publication_preparation(&publication, async {
        publication.begin_table_bucket("analytics").await?;
        publication_recovery(async {
            begin_publication_mutation()?;
            backend
                .put_object("analytics", "recovery", b"recovered".to_vec(), TableCatalogPutPrecondition::IfAbsent)
                .await?;
            tokio::time::sleep(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
            Ok(())
        })
        .await?;
        backend
            .put_object(
                "analytics",
                "new-transaction",
                b"unexpected".to_vec(),
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await
    })
    .await;
    assert!(is_preparation_timeout(&result));
    assert!(backend.object_exists("analytics", "recovery").await.unwrap());
    assert!(!backend.object_exists("analytics", "new-transaction").await.unwrap());
    assert!(!publication.holds_table_bucket("analytics"));
}

// The durable-strong backing does not take part in the preparation deadline; it is retired separately,
// so only the object backing is exercised here.
#[tokio::test(start_paused = true)]
async fn publication_deadline_create_queue_wait_cannot_restart_after_admission() {
    let (backend, store, _) = commit_fixture(false).await;
    let key = default_table_bucket_publication_lock_path();
    let blocker = backend.acquire_read_lock("analytics", &key).await.unwrap();
    let attempts = backend.write_lock_acquisition_count("analytics", &key).await;
    let candidate = registration_entry(&Namespace::parse("sales").unwrap(), "queued");
    let creating = {
        let store = store.clone();
        tokio::spawn(async move { store.create_table(candidate).await })
    };
    while backend.write_lock_acquisition_count("analytics", &key).await == attempts {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
    assert!(is_preparation_timeout(&creating.await.unwrap()));
    drop(blocker);
    assert!(store.load_table("analytics", "sales", "queued").await.unwrap().is_none());
    store
        .create_table(registration_entry(&Namespace::parse("sales").unwrap(), "fresh"))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn publication_deadline_active_rename_recovery_finishes_before_rejecting_new_registration() {
    let (backend, store, namespace) = cache_fixture().await;
    let source = store
        .paths
        .table_entry_path("analytics", &namespace, &IdentifierSegment::parse("orders").unwrap());
    backend.fail_next_put(RUSTFS_META_BUCKET, &source).await;
    assert!(
        store
            .rename_table("analytics", "sales", "orders", "sales", "renamed_orders")
            .await
            .is_err()
    );
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_some()
    );
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &source).await;
    let registering = {
        let store = store.clone();
        let candidate = registration_entry(&namespace, "after_recovery");
        tokio::spawn(async move { store.register_table(candidate).await })
    };
    pause.wait_started().await;
    tokio::time::advance(publication_preparation_timeout() + StdDuration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !registering.is_finished(),
        "in-flight recovery must not be cancelled by the preparation deadline"
    );
    pause.release();
    assert!(is_preparation_timeout(&registering.await.unwrap()));
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_none()
    );
    assert!(store.load_table("analytics", "sales", "orders").await.unwrap().is_none());
    assert!(
        store
            .load_table("analytics", "sales", "renamed_orders")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .load_table("analytics", "sales", "after_recovery")
            .await
            .unwrap()
            .is_none()
    );
    store
        .register_table(registration_entry(&namespace, "fresh_request"))
        .await
        .unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn publication_deadline_timeout_is_clamped_to_its_ceiling() {
    temp_env::async_with_vars(
        [(
            crate::table_catalog::ENV_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS,
            Some(u64::MAX.to_string().as_str()),
        )],
        async {
            let ceiling = StdDuration::from_secs(crate::table_catalog::MAX_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS);
            assert_eq!(publication_preparation_timeout(), ceiling.max(get_lock_acquire_timeout()));
            // `Instant + Duration` must not overflow for the first deadline.
            let preparation = TablePublicationPreparation::default();
            assert!(preparation.deadline() > tokio::time::Instant::now());
            preparation.check_deadline().unwrap();
        },
    )
    .await;
    temp_env::async_with_vars(
        [(crate::table_catalog::ENV_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS, Some("0"))],
        async {
            assert_eq!(publication_preparation_timeout(), get_lock_acquire_timeout());
        },
    )
    .await;
}
