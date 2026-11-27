// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::storage_api::vector_usecase::{NamespaceLocking as _, StorageError};
use crate::table_catalog::{CatalogRetirementObservationHandle, VectorBucketBindingV1, observe_catalog_retirement};
use std::time::Duration;

fn observe_delete(identity: &BucketHandle) -> CatalogRetirementObservationHandle {
    observe_catalog_retirement(
        VectorBucketBindingV1::from_handle(identity).unwrap(),
        Arc::new(|_| Box::pin(async { Ok(()) })),
    )
    .unwrap()
}

fn assert_successful_delete_diagnostics(
    observer: &CatalogRetirementObservationHandle,
    identity: &BucketHandle,
    instance_id: Uuid,
    expected_invocations: usize,
) {
    let snapshot = observer.managed_delete_diagnostics();
    assert_eq!(snapshot["overflow"], false);
    let events = snapshot["events"].as_array().unwrap();
    assert_eq!(events.len(), expected_invocations * 6);
    let binding = json!(VectorBucketBindingV1::from_handle(identity).unwrap());
    for invocation in events.as_chunks::<6>().0 {
        let phases = invocation
            .iter()
            .map(|event| event["phase"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            phases,
            [
                "CatalogScopeStarted",
                "CatalogScopeReturned",
                "ManagedStarted",
                "CallbackStarted",
                "CallbackReturned",
                "ManagedReturned"
            ]
        );
        for event in invocation {
            assert_eq!(event["bucket"], binding);
            assert_eq!(event["instance_id"], json!(instance_id));
            assert_eq!(event["invocation_id"], invocation[0]["invocation_id"]);
            assert_eq!(event["request"], invocation[0]["request"]);
            assert!(
                !Uuid::parse_str(event["request"]["request_id"].as_str().unwrap())
                    .unwrap()
                    .is_nil()
            );
        }
        for index in [1, 4, 5] {
            assert_eq!(invocation[index]["outcome"], json!({"result":"Succeeded"}));
        }
    }
}

fn assert_native_nonempty_diagnostics(
    observer: &CatalogRetirementObservationHandle,
    identity: &BucketHandle,
    instance_id: Uuid,
    expected_invocations: usize,
) {
    let snapshot = observer.managed_delete_diagnostics();
    assert_eq!(snapshot["overflow"], false);
    let events = snapshot["events"].as_array().unwrap();
    assert_eq!(events.len(), expected_invocations * 4, "{snapshot}");
    let binding = json!(VectorBucketBindingV1::from_handle(identity).unwrap());
    let mut invocation_ids = std::collections::BTreeSet::new();
    for invocation in events.as_chunks::<4>().0 {
        let phases = invocation
            .iter()
            .map(|event| event["phase"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            phases,
            [
                "CatalogScopeStarted",
                "CatalogScopeReturned",
                "ManagedStarted",
                "ManagedReturned"
            ]
        );
        let invocation_id = Uuid::parse_str(invocation[0]["invocation_id"].as_str().unwrap()).unwrap();
        assert!(!invocation_id.is_nil());
        assert!(invocation_ids.insert(invocation_id));
        for event in invocation {
            assert_eq!(event["bucket"], binding);
            assert_eq!(event["instance_id"], json!(instance_id));
            assert_eq!(event["invocation_id"], invocation[0]["invocation_id"]);
            assert_eq!(event["request"], invocation[0]["request"]);
            assert!(
                !Uuid::parse_str(event["request"]["request_id"].as_str().unwrap())
                    .unwrap()
                    .is_nil()
            );
        }
        assert_eq!(invocation[0]["outcome"], Value::Null);
        assert_eq!(invocation[1]["outcome"], json!({"result":"Succeeded"}));
        assert_eq!(invocation[2]["outcome"], Value::Null);
        // Native object residue rejects deletion before the Catalog callback is polled.
        assert_eq!(
            invocation[3]["outcome"],
            json!({"result":"Failed","error":{"domain":"Storage","category":"BucketNotEmpty"}}),
            "{snapshot}"
        );
    }
}

fn delete_request(bucket: &str) -> Request {
    Request::DeleteVectorBucket(serde_json::from_value(json!({"vectorBucketName": bucket})).unwrap())
}

async fn create_bucket(fixture: &Fixture, bucket: &str) {
    fixture
        .execute(Request::CreateVectorBucket(
            serde_json::from_value(json!({"vectorBucketName": bucket, "tags": {"team": "search"}})).unwrap(),
        ))
        .await
        .unwrap();
}

async fn current_metadata(fixture: &Fixture, identity: &BucketHandle) -> Arc<BucketMetadata> {
    let buckets = fixture.service.buckets();
    let guard = buckets.acquire_bucket_handle_guard(identity).await.unwrap();
    buckets.read_managed_bucket_metadata(identity, &guard).await.unwrap()
}

#[test]
#[serial_test::serial]
fn vector_bucket_delete_recreates_name_and_rejects_old_admission() {
    run_large_stack_test("vector-delete-recreate", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &bucket).await;
        let request = delete_request(&bucket);
        let admitted = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        let stale = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        let previous = admitted.bucket.as_ref().unwrap().clone();
        let observer = observe_delete(&previous);
        let request_id = admitted.request_id.as_uuid();
        let catalog = fixture.service.catalog(&previous, admitted.reader.clone()).await.unwrap();
        let lifecycle = fixture
            .service
            .buckets()
            .acquire_bucket_handle_guard(&previous)
            .await
            .unwrap();
        catalog
            .ensure_vector_catalog_parent(&previous, Arc::new(lifecycle))
            .await
            .unwrap();

        assert!(matches!(
            fixture.service.execute_authorized(admitted, request.clone()).await.unwrap(),
            Response::DeleteVectorBucket(_)
        ));
        assert_successful_delete_diagnostics(&observer, &previous, fixture.app.object_store().id, 1);
        assert_eq!(
            observer.managed_delete_diagnostics()["events"][0]["request"]["request_id"],
            json!(request_id)
        );
        assert_eq!(
            fixture
                .execute(Request::GetVectorBucket(
                    serde_json::from_value(json!({"vectorBucketName": bucket})).unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );

        create_bucket(&fixture, &bucket).await;
        let replacement = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        let replacement = replacement.bucket.as_ref().unwrap().clone();
        assert_eq!(previous.storage_bucket_id(), replacement.storage_bucket_id());
        assert_ne!(previous.incarnation(), replacement.incarnation());
        let before = current_metadata(&fixture, &replacement).await;

        // Existing admission guard validation reports NotFound for a replaced incarnation.
        // The storage writer independently reports Conflict for an obsolete supplied handle.
        let error = fixture.service.execute_authorized(stale, request).await.unwrap_err();
        assert_eq!(error.code(), ErrorCode::NotFound);
        let after = current_metadata(&fixture, &replacement).await;
        assert_eq!(before.bucket_incarnation_id, after.bucket_incarnation_id);
        assert_eq!(before.marshal_msg().unwrap(), after.marshal_msg().unwrap());
        assert_eq!(
            after
                .bucket_handle(replacement.account_namespace_id(), replacement.region())
                .unwrap(),
            replacement
        );
    });
}

#[test]
#[serial_test::serial]
fn vector_bucket_delete_rechecks_current_identity_and_resource_policies() {
    run_large_stack_test("vector-delete-current-permissions", || async {
        for denial in ["missing-permission", "explicit-deny", "resource-policy", "revoked-credential"] {
            let fixture = Fixture::new().await;
            let bucket = format!("vectors-{}", Uuid::new_v4().simple());
            create_bucket(&fixture, &bucket).await;
            let audit = crate::storage::helper::MutationCompletionProbe::install(&bucket);
            let request = delete_request(&bucket);
            let admitted = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
            let identity = admitted.bucket.as_ref().unwrap().clone();
            match denial {
                "missing-permission" => {
                    fixture
                        .set_statements(json!([
                            {"Effect":"Allow","Action":"s3vectors:GetVectorBucket","Resource":"*"},
                            {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                        ]))
                        .await;
                }
                "explicit-deny" => {
                    fixture
                        .set_statements(json!([
                            {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                            {"Effect":"Deny","Action":"s3vectors:DeleteVectorBucket","Resource":"*"},
                            {"Effect":"Deny","Action":"s3:*","Resource":"*"}
                        ]))
                        .await;
                }
                "resource-policy" => {
                    let arn = format!("arn:aws:s3vectors:{}::bucket/{bucket}", fixture.service.region);
                    let policy = json!({"Version":"2012-10-17","Statement":[{
                        "Effect":"Deny","Principal":"*","Action":"s3vectors:DeleteVectorBucket","Resource":arn
                    }]})
                    .to_string();
                    fixture
                        .execute(Request::PutVectorBucketPolicy(
                            serde_json::from_value(json!({"vectorBucketName":bucket,"policy":policy})).unwrap(),
                        ))
                        .await
                        .unwrap();
                }
                "revoked-credential" => {
                    fixture
                        .app
                        .iam()
                        .handle()
                        .delete_user(&fixture.credentials.access_key, false)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let before = current_metadata(&fixture, &identity).await;
            let error = fixture.service.execute_authorized(admitted, request).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::AccessDenied, "{denial}");
            let after = current_metadata(&fixture, &identity).await;
            assert_eq!(before.bucket_incarnation_id, after.bucket_incarnation_id, "{denial}");
            assert_eq!(before.marshal_msg().unwrap(), after.marshal_msg().unwrap(), "{denial}");
            audit.wait_for(if denial == "resource-policy" { 2 } else { 1 }, 0).await;
            let audits = audit.audits.lock().unwrap();
            let deletes = audits
                .iter()
                .filter(|entry| entry.api.name.as_deref() == Some("DeleteVectorBucket"))
                .collect::<Vec<_>>();
            assert_eq!(deletes.len(), 1, "{denial}");
            assert_eq!(deletes[0].access_key.as_deref(), Some(fixture.credentials.access_key.as_str()));
            assert_eq!(deletes[0].api.status_code, Some(403));
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_bucket_delete_rejects_active_and_retained_indexes() {
    run_large_stack_test("vector-delete-protected-index", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &bucket).await;
        fixture
            .execute(Request::CreateIndex(
                serde_json::from_value(json!({
                    "vectorBucketName":bucket,"indexName":"index.one","dataType":"float32",
                    "dimension":2,"distanceMetric":"euclidean"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let identity = fixture
            .service
            .buckets()
            .resolve_bucket_handle(
                BucketKind::Vector,
                &AccountNamespaceId::try_new(String::new()).unwrap(),
                &fixture.service.region,
                &BucketPublicName::try_new(bucket.clone()).unwrap(),
            )
            .await
            .unwrap();
        let observer = observe_delete(&identity);
        assert_eq!(fixture.execute(delete_request(&bucket)).await.unwrap_err().code(), ErrorCode::Conflict);
        assert_native_nonempty_diagnostics(&observer, &identity, fixture.app.object_store().id, 1);
        fixture
            .execute(Request::GetIndex(
                serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one"})).unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one"})).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(fixture.execute(delete_request(&bucket)).await.unwrap_err().code(), ErrorCode::Conflict);
        assert_native_nonempty_diagnostics(&observer, &identity, fixture.app.object_store().id, 2);
        fixture
            .execute(Request::GetVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
    });
}

#[test]
#[serial_test::serial]
fn vector_bucket_delete_retains_writer_and_audit_after_waiter_cancellation() {
    run_large_stack_test("vector-delete-cancel-owner", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &bucket).await;
        let audit = crate::storage::helper::MutationCompletionProbe::install(&bucket);
        let request = delete_request(&bucket);
        let admitted = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
        let identity = admitted.bucket.as_ref().unwrap().clone();
        let store = fixture.app.object_store();
        let storage_bucket = identity.storage_bucket_id().as_str();
        let namespace = store.new_ns_lock(storage_bucket, storage_bucket).await.unwrap();
        let blocker = namespace.get_read_lock(Duration::from_secs(5)).await.unwrap();
        let lock_counts = || {
            crate::app::storage_api::test::bootstrap_instance_ctx()
                .lock_manager()
                .as_fast_lock_manager()
                .unwrap()
                .current_lock_counts()
        };
        let before = lock_counts().1;
        let owner = fixture.service.clone();
        let waiter = tokio::spawn(async move { owner.execute_authorized(admitted, request).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while lock_counts().1 <= before {
                assert!(!waiter.is_finished(), "deletion must wait for the exact bucket namespace writer");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deletion reaches the blocked namespace after acquiring its lifecycle writer");

        let buckets = fixture.service.buckets();
        let mut next_operation = Box::pin(buckets.acquire_bucket_handle_guard(&identity));
        assert!(futures::poll!(next_operation.as_mut()).is_pending());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(futures::poll!(next_operation.as_mut()).is_pending());
        assert!(audit.audits.lock().unwrap().is_empty());
        drop(blocker);

        let next_result = tokio::time::timeout(Duration::from_secs(10), next_operation).await.unwrap();
        assert!(next_result.is_err(), "a completed delete cannot admit an operation for the old identity");
        audit.wait_for(1, 0).await;
        {
            let audits = audit.audits.lock().unwrap();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0].access_key.as_deref(), Some(fixture.credentials.access_key.as_str()));
            assert_eq!(audits[0].api.name.as_deref(), Some("DeleteVectorBucket"));
            assert_eq!(audits[0].api.status_code, Some(200));
        }
        assert_eq!(
            fixture
                .execute(Request::GetVectorBucket(
                    serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap()
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
    });
}

#[test]
fn vector_bucket_delete_preserves_typed_catalog_errors() {
    use crate::app::bucket_usecase::BucketResolveError;
    for (source, expected) in [
        (TableCatalogStoreError::Conflict("retained Index history".into()), ErrorCode::Conflict),
        (
            TableCatalogStoreError::Unavailable("lost deletion fence".into()),
            ErrorCode::ServiceUnavailable,
        ),
        (
            TableCatalogStoreError::Invalid("invalid parent binding".into()),
            ErrorCode::InternalServer,
        ),
        (
            TableCatalogStoreError::Io {
                classification: crate::table_catalog::CatalogIoErrorKind::Denied,
                context: "read deletion records",
                source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            },
            ErrorCode::AccessDenied,
        ),
    ] {
        let error = BucketResolveError::Storage(StorageError::other(source));
        assert_eq!(bucket_error(error).code(), expected);
    }
    let native = BucketResolveError::Storage(StorageError::BucketNotEmpty("retained-data".into()));
    assert_eq!(bucket_error(native).code(), ErrorCode::Conflict);
}

#[test]
#[serial_test::serial]
fn vector_bucket_delete_diagnostics_record_direct_callback_drop() {
    run_large_stack_test("vector-delete-diagnostic-drop", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        create_bucket(&fixture, &bucket).await;
        let identity = fixture
            .service
            .buckets()
            .resolve_bucket_handle(
                BucketKind::Vector,
                &AccountNamespaceId::try_new(String::new()).unwrap(),
                &fixture.service.region,
                &BucketPublicName::try_new(bucket.clone()).unwrap(),
            )
            .await
            .unwrap();
        let before = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();
        let observer = observe_delete(&identity);
        let store = fixture.app.object_store();
        let namespace = store
            .new_ns_lock(".rustfs.sys", "s3vectors/catalog/registry.lock")
            .await
            .unwrap();
        let blocker = namespace.get_write_lock(Duration::from_secs(5)).await.unwrap();
        let owner = fixture.service.clone();
        let expected = identity.clone();
        // This directly tests dropping the lower-level future; owned HTTP execution is tested separately.
        let waiter = tokio::spawn(async move {
            let buckets = owner.buckets();
            buckets.delete_managed_bucket(&expected).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = observer.managed_delete_diagnostics();
                if snapshot["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|event| event["phase"] == "CallbackStarted")
                {
                    break;
                }
                assert!(!waiter.is_finished(), "the direct delete must remain blocked by the registry writer");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original callback is actually polled");
        assert!(!waiter.is_finished());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let snapshot = observer.managed_delete_diagnostics();
        assert_eq!(snapshot["overflow"], false);
        let events = snapshot["events"].as_array().unwrap();
        assert_eq!(events.len(), 6);
        for (index, phase) in [
            "CatalogScopeStarted",
            "CatalogScopeReturned",
            "ManagedStarted",
            "CallbackStarted",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(events[index]["phase"], phase);
        }
        for phase in ["CallbackDroppedWithoutResult", "ManagedDroppedWithoutResult"] {
            assert_eq!(events.iter().filter(|event| event["phase"] == phase).count(), 1);
        }
        for event in events {
            assert_eq!(event["bucket"], json!(VectorBucketBindingV1::from_handle(&identity).unwrap()));
            assert_eq!(event["instance_id"], json!(store.id));
            assert_eq!(event["invocation_id"], events[0]["invocation_id"]);
            assert_eq!(event["request"], Value::Null);
        }
        drop(blocker);
        let after = current_metadata(&fixture, &identity).await.marshal_msg().unwrap();
        assert_eq!(before, after);
        fixture
            .execute(Request::GetVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
    });
}
