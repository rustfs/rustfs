// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::bucket_usecase::ListManagedBucketsPauseGuard;
use crate::app::storage_api::test::RUSTFS_META_BUCKET;
use crate::app::vector_usecase::bucket::{
    VectorBucketPauseGuard, VectorBucketPausePoint, map_list_indexes_non_race_conflict, normalize_resource_list_error,
    with_list_indexes_scan_budget_for_test,
};
use crate::table_catalog::{
    EcStoreTableCatalogObjectBackend, TableBucketEntry, TableCatalogObjectBackend as _, TableCatalogPutPrecondition,
};
use std::sync::Mutex as StdMutex;
use tokio::sync::oneshot;

type OneShotPauseState = Arc<StdMutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>>;

fn clone_fixture(fixture: &Fixture) -> Fixture {
    Fixture {
        app: fixture.app.clone(),
        service: fixture.service.clone(),
        credentials: fixture.credentials.clone(),
        policy: fixture.policy.clone(),
    }
}

fn one_shot_pause() -> (oneshot::Receiver<()>, oneshot::Sender<()>, OneShotPauseState) {
    let (paused_tx, paused_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    (paused_rx, release_tx, Arc::new(StdMutex::new(Some((paused_tx, release_rx)))))
}

async fn trigger_one_shot_pause(state: &StdMutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>) {
    let pair = state.lock().expect("pause state mutex").take();
    if let Some((paused_tx, release_rx)) = pair {
        let _ = paused_tx.send(());
        let _ = release_rx.await;
    }
}

async fn resolve_handle(fixture: &Fixture, bucket: &str) -> BucketHandle {
    fixture
        .service
        .buckets()
        .resolve_bucket_handle(
            BucketKind::Vector,
            &AccountNamespaceId::try_new(String::new()).expect("empty account"),
            &fixture.service.region,
            &BucketPublicName::try_new(bucket.to_owned()).expect("valid bucket name"),
        )
        .await
        .expect("resolved vector bucket handle")
}

fn create_index_request(bucket: &str, index: &str) -> Request {
    Request::CreateIndex(
        serde_json::from_value(json!({
            "vectorBucketName": bucket,
            "indexName": index,
            "dataType": "float32",
            "dimension": 2,
            "distanceMetric": "euclidean"
        }))
        .expect("valid CreateIndex request"),
    )
}

#[test]
fn vector_list_error_normalization_rejects_unmodeled_conflicts_and_maps_catalog_conflicts() {
    assert_eq!(
        normalize_resource_list_error(ProtocolError::new(ErrorCode::Conflict, "transient fence conflict")).code(),
        ErrorCode::ServiceUnavailable
    );
    assert_eq!(
        normalize_resource_list_error(ProtocolError::new(ErrorCode::AccessDenied, "denied")).code(),
        ErrorCode::AccessDenied
    );
    assert_eq!(
        map_list_indexes_non_race_conflict("Vector bucket Catalog retirement prevents ordinary admission").code(),
        ErrorCode::ServiceUnavailable
    );
    assert_eq!(
        map_list_indexes_non_race_conflict("stored Vector bucket identity changed").code(),
        ErrorCode::InternalServer
    );
    assert_eq!(
        map_list_indexes_non_race_conflict("bucket does not match this Vector catalog").code(),
        ErrorCode::InternalServer
    );
}

#[test]
#[serial_test::serial]
fn vector_create_index_reuses_lifecycle_guard_without_deadlocking_queued_bucket_delete() {
    run_large_stack_test("vector-f01-no-lifecycle-deadlock", || async {
        let fixture = Fixture::new().await;
        let lock_counts = || {
            crate::app::storage_api::test::bootstrap_instance_ctx()
                .lock_manager()
                .as_fast_lock_manager()
                .expect("fast lock manager")
                .current_lock_counts()
        };

        // Sub-case 1: Empty bucket (first-time parent scaffolding creation under reused lifecycle guard).
        // `initialize_index_catalog` completes without deadlocking against the queued `DeleteVectorBucket`
        // writer, then drops `lifecycle` before `reserve_vector_index`; `FastLock` wakes the queued writer,
        // which deletes the empty bucket (200 OK), and `CreateIndex` cleanly returns 404 NotFound (never 500).
        {
            let empty_bucket = format!("vectors-f01-empty-{}", Uuid::new_v4().simple());
            create_bucket(&fixture, &empty_bucket).await;
            let expected = resolve_handle(&fixture, &empty_bucket).await;

            let (paused_rx, release_tx, pause_state) = one_shot_pause();
            let pause_bucket = expected.clone();
            let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
                let pause_state = Arc::clone(&pause_state);
                let pause_bucket = pause_bucket.clone();
                async move {
                    if observed == pause_bucket && stage == VectorBucketPausePoint::BeforeIndexCatalogParent {
                        trigger_one_shot_pause(&pause_state).await;
                    }
                }
            });

            let create_fixture = clone_fixture(&fixture);
            let create_bucket_name = empty_bucket.clone();
            let mut create_task = tokio::spawn(async move {
                create_fixture
                    .execute(create_index_request(&create_bucket_name, "index.one"))
                    .await
            });

            tokio::time::timeout(Duration::from_secs(5), paused_rx)
                .await
                .expect("CreateIndex reaches BeforeIndexCatalogParent on empty bucket")
                .expect("pause notification");

            let waiters_before = lock_counts().1;
            let delete_fixture = clone_fixture(&fixture);
            let delete_bucket_name = empty_bucket.clone();
            let delete_task = tokio::spawn(async move { delete_fixture.execute(delete_request(&delete_bucket_name)).await });

            tokio::time::timeout(Duration::from_secs(5), async {
                while lock_counts().1 <= waiters_before {
                    assert!(
                        !delete_task.is_finished(),
                        "DeleteVectorBucket must block waiting for the lifecycle writer lock"
                    );
                    assert!(
                        !create_task.is_finished(),
                        "CreateIndex must remain paused holding the lifecycle read guard"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("DeleteVectorBucket enters FastLock writer wait queue on empty bucket");

            release_tx.send(()).expect("release CreateIndex on empty bucket");

            let delete_response = tokio::time::timeout(Duration::from_secs(5), delete_task)
                .await
                .expect("DeleteVectorBucket wakes without deadlock after initialize_index_catalog drops lifecycle")
                .expect("DeleteVectorBucket task")
                .expect("DeleteVectorBucket deletes bucket before index reservation");
            assert!(matches!(delete_response, Response::DeleteVectorBucket(_)));

            let create_err = tokio::time::timeout(Duration::from_secs(5), &mut create_task)
                .await
                .expect("CreateIndex finishes without deadlock on concurrently deleted empty bucket")
                .expect("CreateIndex task")
                .expect_err("CreateIndex must report NotFound after queued DeleteVectorBucket deletes empty bucket");
            assert_eq!(create_err.code(), ErrorCode::NotFound);
        }

        // Sub-case 2: Non-empty bucket (`ensure_vector_catalog_parent` still runs `reuse_bucket_lifecycle_read`
        // under a queued `DeleteVectorBucket` writer without deadlocking; `DeleteVectorBucket` rejects with
        // 409 Conflict because `index.keep` is active, and `CreateIndex("index.one")` succeeds).
        {
            let populated_bucket = format!("vectors-f01-nonempty-{}", Uuid::new_v4().simple());
            create_bucket(&fixture, &populated_bucket).await;
            fixture
                .execute(create_index_request(&populated_bucket, "index.keep"))
                .await
                .expect("seed active index.keep");
            let expected = resolve_handle(&fixture, &populated_bucket).await;

            let (paused_rx, release_tx, pause_state) = one_shot_pause();
            let pause_bucket = expected.clone();
            let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
                let pause_state = Arc::clone(&pause_state);
                let pause_bucket = pause_bucket.clone();
                async move {
                    if observed == pause_bucket && stage == VectorBucketPausePoint::BeforeIndexCatalogParent {
                        trigger_one_shot_pause(&pause_state).await;
                    }
                }
            });

            let create_fixture = clone_fixture(&fixture);
            let create_bucket_name = populated_bucket.clone();
            let mut create_task = tokio::spawn(async move {
                create_fixture
                    .execute(create_index_request(&create_bucket_name, "index.one"))
                    .await
            });

            tokio::time::timeout(Duration::from_secs(5), paused_rx)
                .await
                .expect("CreateIndex reaches BeforeIndexCatalogParent on non-empty bucket")
                .expect("pause notification");

            let waiters_before = lock_counts().1;
            let delete_fixture = clone_fixture(&fixture);
            let delete_bucket_name = populated_bucket.clone();
            let delete_task = tokio::spawn(async move { delete_fixture.execute(delete_request(&delete_bucket_name)).await });

            tokio::time::timeout(Duration::from_secs(5), async {
                while lock_counts().1 <= waiters_before {
                    assert!(
                        !delete_task.is_finished(),
                        "DeleteVectorBucket must block waiting for the lifecycle writer lock"
                    );
                    assert!(
                        !create_task.is_finished(),
                        "CreateIndex must remain paused holding the lifecycle read guard"
                    );
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("DeleteVectorBucket enters FastLock writer wait queue on non-empty bucket");

            release_tx.send(()).expect("release CreateIndex on non-empty bucket");

            let create_result = tokio::time::timeout(Duration::from_secs(5), &mut create_task)
                .await
                .expect("CreateIndex must not deadlock against queued lifecycle writer")
                .expect("CreateIndex task")
                .expect("CreateIndex succeeds on non-empty bucket");
            assert!(matches!(create_result, Response::CreateIndex(_)));

            let delete_err = tokio::time::timeout(Duration::from_secs(5), delete_task)
                .await
                .expect("DeleteVectorBucket wakes after CreateIndex releases lifecycle guard")
                .expect("DeleteVectorBucket task")
                .expect_err("DeleteVectorBucket must reject non-empty bucket");
            assert_eq!(delete_err.code(), ErrorCode::Conflict);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_bucket_mutations_enforce_latest_policy_and_abac_tags_under_storage_locks() {
    run_large_stack_test("vector-f03-atomic-bucket-auth", || async {
        // Part 1: Concurrent PutVectorBucketPolicy(Deny) racing all 5 bucket control-plane mutations.
        for kind in ["delete", "put-policy", "delete-policy", "tag", "untag"] {
            let fixture = Fixture::new().await;
            let bucket = format!("vectors-f03-{kind}-{}", Uuid::new_v4().simple());
            create_bucket(&fixture, &bucket).await;
            let arn = format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region);
            let identity = resolve_handle(&fixture, &bucket).await;

            let candidate = match kind {
                "delete" => delete_request(&bucket),
                "put-policy" => {
                    let allow_policy = json!({
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect": "Allow",
                            "Principal": {"AWS": fixture.credentials.access_key},
                            "Action": "s3vectors:GetVectorBucket",
                            "Resource": arn
                        }]
                    })
                    .to_string();
                    Request::PutVectorBucketPolicy(
                        serde_json::from_value(json!({"vectorBucketName": bucket, "policy": allow_policy})).unwrap(),
                    )
                }
                "delete-policy" => {
                    Request::DeleteVectorBucketPolicy(serde_json::from_value(json!({"vectorBucketName": bucket})).unwrap())
                }
                "tag" => {
                    Request::TagResource(serde_json::from_value(json!({"resourceArn": arn, "tags": {"mutated": "yes"}})).unwrap())
                }
                "untag" => {
                    Request::UntagResource(serde_json::from_value(json!({"resourceArn": arn, "tagKeys": ["team"]})).unwrap())
                }
                _ => unreachable!(),
            };

            let admitted = fixture.service.admit(fixture.verified(candidate.clone())).await.unwrap();
            let (paused_rx, release_tx, pause_state) = one_shot_pause();
            let target_identity = identity.clone();
            let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
                let pause_state = Arc::clone(&pause_state);
                let target_identity = target_identity.clone();
                async move {
                    if observed == target_identity && stage == VectorBucketPausePoint::BeforeBucketControlMutationLock {
                        trigger_one_shot_pause(&pause_state).await;
                    }
                }
            });

            let service = fixture.service.clone();
            let raced = tokio::spawn(async move { service.execute_authorized(admitted, candidate).await });
            tokio::time::timeout(Duration::from_secs(5), paused_rx)
                .await
                .expect("raced mutation reaches pre-lock pause")
                .expect("paused signal");

            let deny_policy = json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": [
                        "s3vectors:DeleteVectorBucket",
                        "s3vectors:PutVectorBucketPolicy",
                        "s3vectors:DeleteVectorBucketPolicy",
                        "s3vectors:TagResource",
                        "s3vectors:UntagResource"
                    ],
                    "Resource": arn
                }]
            })
            .to_string();
            fixture
                .execute(Request::PutVectorBucketPolicy(
                    serde_json::from_value(json!({"vectorBucketName": bucket, "policy": deny_policy})).unwrap(),
                ))
                .await
                .expect("concurrent Deny policy commits");
            let expected_bytes = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();

            release_tx.send(()).expect("release raced mutation");
            let err = tokio::time::timeout(Duration::from_secs(5), raced)
                .await
                .expect("raced mutation finishes")
                .expect("raced task")
                .expect_err("concurrent Deny policy must reject mutation under lock");
            assert_eq!(err.code(), ErrorCode::AccessDenied, "operation={kind}");

            let after_bytes = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();
            assert_eq!(after_bytes, expected_bytes, "operation={kind} must not mutate metadata");
        }

        // Part 2: Concurrent ABAC bucket tag mutation racing DeleteVectorBucket and PutVectorBucketPolicy.
        for kind in ["delete", "put-policy"] {
            let fixture = Fixture::new().await;
            let bucket = format!("vectors-f03-abac-{kind}-{}", Uuid::new_v4().simple());
            create_bucket(&fixture, &bucket).await;
            let arn = format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region);
            let identity = resolve_handle(&fixture, &bucket).await;

            fixture
                .set_statements(json!([
                    {
                        "Effect": "Allow",
                        "Action": ["s3vectors:TagResource", "s3vectors:UntagResource", "s3vectors:GetVectorBucket"],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3vectors:DeleteVectorBucket", "s3vectors:PutVectorBucketPolicy"],
                        "Resource": arn,
                        "Condition": {"StringEquals": {"s3vectors:VectorBucketTag/team": "search"}}
                    },
                    {"Effect": "Deny", "Action": "s3:*", "Resource": "*"}
                ]))
                .await;

            let candidate = if kind == "delete" {
                delete_request(&bucket)
            } else {
                let policy = json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {"AWS": fixture.credentials.access_key},
                        "Action": "s3vectors:GetVectorBucket",
                        "Resource": arn
                    }]
                })
                .to_string();
                Request::PutVectorBucketPolicy(
                    serde_json::from_value(json!({"vectorBucketName": bucket, "policy": policy})).unwrap(),
                )
            };

            let admitted = fixture.service.admit(fixture.verified(candidate.clone())).await.unwrap();
            let (paused_rx, release_tx, pause_state) = one_shot_pause();
            let target_identity = identity.clone();
            let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
                let pause_state = Arc::clone(&pause_state);
                let target_identity = target_identity.clone();
                async move {
                    if observed == target_identity && stage == VectorBucketPausePoint::BeforeBucketControlMutationLock {
                        trigger_one_shot_pause(&pause_state).await;
                    }
                }
            });

            let service = fixture.service.clone();
            let raced = tokio::spawn(async move { service.execute_authorized(admitted, candidate).await });
            tokio::time::timeout(Duration::from_secs(5), paused_rx)
                .await
                .expect("ABAC mutation reaches pre-lock pause")
                .expect("paused signal");

            fixture
                .execute(Request::TagResource(
                    serde_json::from_value(json!({"resourceArn": arn, "tags": {"team": "restricted"}})).unwrap(),
                ))
                .await
                .expect("concurrent ABAC tag update commits");
            let expected_bytes = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();

            release_tx.send(()).expect("release ABAC mutation");
            let err = tokio::time::timeout(Duration::from_secs(5), raced)
                .await
                .expect("ABAC mutation finishes")
                .expect("ABAC task")
                .expect_err("changed ABAC tag must reject mutation under lock");
            assert_eq!(err.code(), ErrorCode::AccessDenied, "abac operation={kind}");
            let after_bytes = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();
            assert_eq!(after_bytes, expected_bytes, "abac operation={kind}");
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_list_buckets_skips_concurrent_deletes_and_uncommitted_intents_while_failing_closed_on_corruption() {
    run_large_stack_test("vector-f06-list-buckets-concurrency", || async {
        let fixture = Fixture::new().await;
        let prefix = format!("f06-bkt-{}", Uuid::new_v4().simple());
        let alpha = format!("{prefix}-alpha");
        let beta = format!("{prefix}-beta");
        let gamma = format!("{prefix}-gamma");
        let uncommitted = format!("{prefix}-uncommitted");
        for name in [&alpha, &beta, &gamma, &uncommitted] {
            create_bucket(&fixture, name).await;
        }

        // Simulate an in-progress CreateVectorBucket whose physical directory and
        // intent metadata exist on disk, but `bucket_creation_committed` is still false.
        let uncommitted_handle = resolve_handle(&fixture, &uncommitted).await;
        let store = fixture.app.object_store();
        let mut pending_meta = (*store
            .get_bucket_metadata_from_disk(uncommitted_handle.storage_bucket_id().as_str())
            .await
            .expect("uncommitted metadata"))
        .clone();
        pending_meta.mark_creation_uncommitted_for_test();
        pending_meta
            .save_with_store(store.clone())
            .await
            .expect("persist uncommitted vector bucket intent");

        let (paused_rx, release_tx, pause_state) = one_shot_pause();
        let _pause = ListManagedBucketsPauseGuard::install(move || {
            let pause_state = Arc::clone(&pause_state);
            async move {
                trigger_one_shot_pause(&pause_state).await;
            }
        });

        let list_fixture = clone_fixture(&fixture);
        let list_prefix = prefix.clone();
        let list_task = tokio::spawn(async move {
            list_fixture
                .execute(Request::ListVectorBuckets(
                    serde_json::from_value(json!({"prefix": list_prefix})).unwrap(),
                ))
                .await
        });

        tokio::time::timeout(Duration::from_secs(5), paused_rx)
            .await
            .expect("ListVectorBuckets pauses after directory enumeration")
            .expect("paused signal");

        // Concurrently delete `beta` after `store.list_bucket` already observed its physical volume.
        fixture
            .execute(delete_request(&beta))
            .await
            .expect("concurrent DeleteVectorBucket succeeds");

        release_tx.send(()).expect("release ListVectorBuckets");
        let response = tokio::time::timeout(Duration::from_secs(5), list_task)
            .await
            .expect("ListVectorBuckets finishes")
            .expect("ListVectorBuckets task")
            .expect("ListVectorBuckets succeeds without 404/500");
        let Response::ListVectorBuckets(output) = response else {
            panic!("expected ListVectorBuckets output");
        };
        let returned_names: Vec<_> = output
            .vector_buckets
            .iter()
            .map(|entry| entry.vector_bucket_name.as_str())
            .collect();
        assert_eq!(returned_names, vec![alpha.as_str(), gamma.as_str()]);
        drop(_pause);

        // Verify real metadata corruption still fails closed rather than being skipped.
        let gamma_handle = resolve_handle(&fixture, &gamma).await;
        let mut damaged = (*store
            .get_bucket_metadata_from_disk(gamma_handle.storage_bucket_id().as_str())
            .await
            .expect("gamma metadata"))
        .clone();
        damaged.tagging_config_xml = b"<Tagging><TagSet><Tag>".to_vec();
        damaged
            .save_with_store(store.clone())
            .await
            .expect("persist corrupted tagging config");

        let corrupt_err = fixture
            .execute(Request::ListVectorBuckets(serde_json::from_value(json!({"prefix": prefix})).unwrap()))
            .await
            .expect_err("corrupted bucket metadata must fail closed");
        assert_eq!(corrupt_err.code(), ErrorCode::InternalServer);
    });
}

#[test]
#[serial_test::serial]
fn vector_list_indexes_handles_concurrent_delete_recreate_lookahead_parent_delete_and_prefix_budget() {
    run_large_stack_test("vector-f06-list-indexes-concurrency", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-f06-idx-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &bucket).await;
        for name in ["idx-a", "idx-b", "idx-c", "idx-d"] {
            fixture
                .execute(create_index_request(&bucket, name))
                .await
                .expect("create initial index");
        }
        let handle = resolve_handle(&fixture, &bucket).await;

        // 1. Concurrent sibling delete (`idx-a`) + sibling delete & recreate (`idx-b`)
        //    after `list_tables_page` already returned `[idx-a, idx-b, idx-c]`.
        let (paused_rx, release_tx, pause_state) = one_shot_pause();
        let target_handle = handle.clone();
        let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
            let pause_state = Arc::clone(&pause_state);
            let target_handle = target_handle.clone();
            async move {
                if observed == target_handle && stage == VectorBucketPausePoint::AfterListIndexesPageFetched {
                    trigger_one_shot_pause(&pause_state).await;
                }
            }
        });

        let list_fixture = clone_fixture(&fixture);
        let list_bucket = bucket.clone();
        let list_task = tokio::spawn(async move {
            list_fixture
                .execute(Request::ListIndexes(
                    serde_json::from_value(json!({"vectorBucketName": list_bucket, "maxResults": 2})).unwrap(),
                ))
                .await
        });

        tokio::time::timeout(Duration::from_secs(5), paused_rx)
            .await
            .expect("ListIndexes pauses after first catalog page")
            .expect("paused signal");

        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName": bucket, "indexName": "idx-a"})).unwrap(),
            ))
            .await
            .expect("concurrent DeleteIndex(idx-a)");
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName": bucket, "indexName": "idx-b"})).unwrap(),
            ))
            .await
            .expect("concurrent DeleteIndex(idx-b)");
        fixture
            .execute(create_index_request(&bucket, "idx-b"))
            .await
            .expect("concurrent recreate CreateIndex(idx-b)");

        release_tx.send(()).expect("release ListIndexes");
        let first_page = tokio::time::timeout(Duration::from_secs(5), list_task)
            .await
            .expect("ListIndexes finishes")
            .expect("ListIndexes task")
            .expect("ListIndexes succeeds despite concurrent delete and recreate");
        let Response::ListIndexes(first_page) = first_page else {
            panic!("expected ListIndexes output");
        };
        let first_names: Vec<_> = first_page.indexes.iter().map(|item| item.index_name.as_str()).collect();
        assert_eq!(first_names, vec!["idx-b", "idx-c"]);
        let token = first_page.next_token.expect("idx-d remains for page 2");
        drop(_pause);

        // Delete the cursor anchor `idx-c` itself before continuing with `nextToken`;
        // ListIndexes must advance cleanly from `idx-c`'s lexical position without 404/409.
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName": bucket, "indexName": "idx-c"})).unwrap(),
            ))
            .await
            .expect("DeleteIndex(idx-c) cursor anchor");

        let second_page = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({
                    "vectorBucketName": bucket,
                    "maxResults": 2,
                    "nextToken": token
                }))
                .unwrap(),
            ))
            .await
            .expect("second page succeeds even when cursor anchor was deleted");
        let Response::ListIndexes(second_page) = second_page else {
            panic!("expected ListIndexes second page");
        };
        let second_names: Vec<_> = second_page.indexes.iter().map(|item| item.index_name.as_str()).collect();
        assert_eq!(second_names, vec!["idx-d"]);
        assert!(second_page.next_token.is_none());

        // 2. Sparse `prefix` scan budget bounds unbounded catalog walks with 503 ServiceUnavailable,
        //    and succeeds when within budget.
        for i in 0..5 {
            fixture
                .execute(create_index_request(&bucket, &format!("skip-{i}")))
                .await
                .expect("create non-matching index");
        }
        fixture
            .execute(create_index_request(&bucket, "zzz-target"))
            .await
            .expect("create sparse target index");

        // Catalog table pages are ordered by `table_catalog_path_hash(index_name)`, so query both
        // a zero-match prefix (`nomatch-`, maxResults = 1) and a 1-match sparse prefix (`zzz-`,
        // maxResults = 2) over the 8 active indexes to deterministically exceed `max_scanned_pages = 2`.
        for (sparse_prefix, max_results) in [("nomatch-", 1), ("zzz-", 2)] {
            let budget_err = with_list_indexes_scan_budget_for_test(
                &handle,
                2,
                2,
                fixture.execute(Request::ListIndexes(
                    serde_json::from_value(json!({
                        "vectorBucketName": bucket,
                        "prefix": sparse_prefix,
                        "maxResults": max_results
                    }))
                    .unwrap(),
                )),
            )
            .await
            .expect_err("sparse prefix exceeding page budget must fail boundedly");
            assert_eq!(budget_err.code(), ErrorCode::ServiceUnavailable);
        }

        let within_budget = with_list_indexes_scan_budget_for_test(
            &handle,
            2,
            16,
            fixture.execute(Request::ListIndexes(
                serde_json::from_value(json!({
                    "vectorBucketName": bucket,
                    "prefix": "zzz-",
                    "maxResults": 2
                }))
                .unwrap(),
            )),
        )
        .await
        .expect("sparse prefix within budget succeeds");
        let Response::ListIndexes(within_budget) = within_budget else {
            panic!("expected ListIndexes output");
        };
        assert_eq!(within_budget.indexes.len(), 1);
        assert_eq!(within_budget.indexes[0].index_name, "zzz-target");
        assert!(within_budget.next_token.is_none());

        // 3. Non-index-race Catalog conflict (corrupted TableBucketEntry binding while
        //    the physical ECStore bucket still exists) must fail closed with a modeled 500
        //    InternalServerException — never 200 OK (omitting entries), 400 ValidationException
        //    on a valid continuation token, or unmodeled 409 ConflictException.
        let valid_page = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({"vectorBucketName": bucket, "maxResults": 1})).unwrap(),
            ))
            .await
            .expect("valid baseline page before catalog parent corruption");
        let Response::ListIndexes(valid_page) = valid_page else {
            panic!("expected ListIndexes output");
        };
        let valid_token = valid_page.next_token.expect("valid continuation token");

        let backend = EcStoreTableCatalogObjectBackend::new(fixture.app.object_store());
        let parent_path = crate::table_catalog::test_support::vector_catalog_paths()
            .table_bucket_entry_path(handle.storage_bucket_id().as_str());
        let stored_parent = backend
            .read_object(RUSTFS_META_BUCKET, &parent_path)
            .await
            .expect("read TableBucketEntry")
            .expect("TableBucketEntry exists");
        let mut damaged_parent: TableBucketEntry =
            serde_json::from_slice(&stored_parent.data).expect("deserialize TableBucketEntry");
        damaged_parent
            .vector_bucket_binding
            .as_mut()
            .expect("vector binding")
            .bucket_incarnation = Uuid::new_v4();
        backend
            .put_object(
                RUSTFS_META_BUCKET,
                &parent_path,
                serde_json::to_vec(&damaged_parent).expect("serialize damaged TableBucketEntry"),
                TableCatalogPutPrecondition::Any,
            )
            .await
            .expect("overwrite TableBucketEntry with mismatched binding");

        let unpaged_conflict_err = fixture
            .execute(Request::ListIndexes(serde_json::from_value(json!({"vectorBucketName": bucket})).unwrap()))
            .await
            .expect_err("corrupted parent binding must fail closed instead of skipping entries");
        assert_eq!(unpaged_conflict_err.code(), ErrorCode::InternalServer);

        let token_conflict_err = fixture
            .execute(Request::ListIndexes(
                serde_json::from_value(json!({
                    "vectorBucketName": bucket,
                    "nextToken": valid_token
                }))
                .unwrap(),
            ))
            .await
            .expect_err("corrupted parent binding with valid nextToken must not return 400 or 409");
        assert_eq!(token_conflict_err.code(), ErrorCode::InternalServer);

        // 4. Deleting the parent Vector bucket itself during ListIndexes still returns 404 NotFoundException.
        let ephemeral_bucket = format!("vectors-f06-parent-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &ephemeral_bucket).await;
        let ephemeral_handle = resolve_handle(&fixture, &ephemeral_bucket).await;

        let (paused_rx, release_tx, pause_state) = one_shot_pause();
        let target_ephemeral = ephemeral_handle.clone();
        let _pause = VectorBucketPauseGuard::install(move |observed, stage| {
            let pause_state = Arc::clone(&pause_state);
            let target_ephemeral = target_ephemeral.clone();
            async move {
                if observed == target_ephemeral && stage == VectorBucketPausePoint::AfterListIndexesPageFetched {
                    trigger_one_shot_pause(&pause_state).await;
                }
            }
        });

        let parent_list_fixture = clone_fixture(&fixture);
        let parent_bucket_name = ephemeral_bucket.clone();
        let parent_list_task = tokio::spawn(async move {
            parent_list_fixture
                .execute(Request::ListIndexes(
                    serde_json::from_value(json!({"vectorBucketName": parent_bucket_name})).unwrap(),
                ))
                .await
        });

        tokio::time::timeout(Duration::from_secs(5), paused_rx)
            .await
            .expect("ListIndexes on ephemeral bucket pauses")
            .expect("paused signal");

        fixture
            .execute(delete_request(&ephemeral_bucket))
            .await
            .expect("DeleteVectorBucket on ephemeral parent succeeds");

        release_tx.send(()).expect("release ephemeral ListIndexes");
        let parent_err = tokio::time::timeout(Duration::from_secs(5), parent_list_task)
            .await
            .expect("ephemeral ListIndexes finishes")
            .expect("ephemeral ListIndexes task")
            .expect_err("deleted parent Vector bucket must fail ListIndexes with 404");
        assert_eq!(parent_err.code(), ErrorCode::NotFound);
    });
}
