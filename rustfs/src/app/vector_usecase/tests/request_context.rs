// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::object::ManagedObjectError;
use crate::vector::{lance::LanceManifestError, service::VectorRequestAccess};

#[test]
#[serial_test::serial]
fn vector_missing_request_context_preserves_managed_error_cause() {
    run_large_stack_test("vector-missing-request-context", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
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
        let request =
            Request::GetIndex(serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"index.one"})).unwrap());
        let mut extensions = http::Extensions::new();
        extensions.insert(fixture.service.server.clone());
        let envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: http::Method::POST,
                uri: "/GetIndex".parse().unwrap(),
                headers: http::HeaderMap::new(),
                credentials: Some(
                    RequestCredentials::new(fixture.credentials.access_key.clone(), fixture.credentials.secret_key.clone(), None)
                        .unwrap(),
                ),
                region: Some(fixture.service.region.clone()),
                service: Some("s3vectors".into()),
            },
            extensions,
        );
        let verified = admission::VerifiedVectorRequest::for_test(
            fixture.service.server.clone(),
            envelope,
            request,
            fixture.service.region.clone(),
        );
        let authorized = fixture.service.admit(verified).await.unwrap();
        let current = authorized.verified.current_envelope().await.unwrap();
        assert!(current.request_context().is_none());
        assert_eq!(
            current.req_info().unwrap().cred.as_ref().unwrap().access_key,
            fixture.credentials.access_key
        );
        let index = CatalogName::parse("index.one").unwrap();
        let (operation, entry) = authorized
            .catalog
            .as_ref()
            .unwrap()
            .begin_vector_operation(authorized.bucket.as_ref().unwrap(), &index_namespace().unwrap(), &index)
            .await
            .unwrap();
        let context = dataset::IndexRequest {
            owner: &fixture.service,
            authorization: &authorized,
            index: &index,
        };
        let error = match context.managed_dataset(Arc::new(operation), entry.definition()).await {
            Ok(_) => panic!("Managed I/O requires the native request context"),
            Err(error) => error,
        };
        match &error {
            VectorServiceError::Manifest(LanceManifestError::Storage { operation, source }) => {
                assert_eq!(*operation, "prepare managed manifest access");
                assert!(matches!(
                    source.downcast_ref::<ManagedObjectError>(),
                    Some(ManagedObjectError::MissingPrincipal)
                ));
                assert_eq!(source.to_string(), "managed operation requires an authenticated request principal");
            }
            other => panic!("unexpected missing-context error: {other:?}"),
        }
        assert_eq!(service_error(error).code(), ErrorCode::InternalServer);
    });
}
