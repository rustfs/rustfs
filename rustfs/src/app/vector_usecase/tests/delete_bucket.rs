// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::storage_api::vector_usecase::{NamespaceLocking as _, StorageError};
use std::time::Duration;

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
        assert_eq!(fixture.execute(delete_request(&bucket)).await.unwrap_err().code(), ErrorCode::Conflict);
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
