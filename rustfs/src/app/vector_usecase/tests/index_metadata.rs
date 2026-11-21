// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::BTreeMap;

async fn execute_metadata(fixture: &Fixture, request: Request) -> Response {
    let operation = request.operation();
    let authorization = fixture.service.admit(fixture.verified(request.clone())).await.unwrap();
    authorization
        .bound
        .require_same(&request, authorization.resource.as_ref())
        .unwrap();
    let RequestTarget::Index { index, .. } = &authorization.target else {
        panic!("metadata operation requires an existing Index");
    };
    let context = dataset::IndexRequest {
        owner: &fixture.service,
        authorization: &authorization,
        index,
    };
    let execution = VectorService::new(
        authorization.catalog.as_ref().unwrap().clone(),
        fixture.service.limits,
        fixture.service.max_write_attempts,
    )
    .execute(&context)
    .await
    .unwrap_or_else(|error| panic!("{} failed with its original service error: {error:?}", operation.name()));
    let VectorExecution::Complete(response) = execution else {
        panic!("metadata mutation must complete in the service");
    };
    response
}

#[test]
#[serial_test::serial]
fn vector_index_metadata_mutations_retain_managed_candidate_access() {
    run_large_stack_test("vector-index-metadata-access", || async {
        let fixture = Fixture::new().await;
        let bucket = format!("vectors-{}", Uuid::new_v4().simple());
        fixture
            .execute(Request::CreateVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
            ))
            .await
            .unwrap();
        let created = fixture
            .execute(Request::CreateIndex(
                serde_json::from_value(json!({
                    "vectorBucketName":bucket,"indexName":"index.one","dataType":"float32",
                    "dimension":2,"distanceMetric":"euclidean"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let Response::CreateIndex(created) = created else {
            panic!("expected CreateIndex response");
        };
        let index_arn = created.index_arn;
        let response = execute_metadata(
            &fixture,
            Request::TagResource(serde_json::from_value(json!({"resourceArn":index_arn,"tags":{"owner":"test"}})).unwrap()),
        )
        .await;
        assert!(matches!(response, Response::TagResource(_)));
        let response = fixture
            .execute(Request::ListTagsForResource(
                serde_json::from_value(json!({"resourceArn":index_arn})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListTagsForResource(tags) = response else {
            panic!("expected Index tags");
        };
        assert_eq!(tags.tags, BTreeMap::from([("owner".into(), "test".into())]));
        let response = execute_metadata(
            &fixture,
            Request::UntagResource(serde_json::from_value(json!({"resourceArn":index_arn,"tagKeys":["owner"]})).unwrap()),
        )
        .await;
        assert!(matches!(response, Response::UntagResource(_)));
        let response = fixture
            .execute(Request::ListTagsForResource(
                serde_json::from_value(json!({"resourceArn":index_arn})).unwrap(),
            ))
            .await
            .unwrap();
        let Response::ListTagsForResource(tags) = response else {
            panic!("expected Index tags");
        };
        assert!(tags.tags.is_empty());
        fixture
            .execute(Request::PutVectors(
                serde_json::from_value(json!({
                    "indexArn":index_arn,"vectors":[{"key":"one","data":{"float32":[1.0,2.0]}}]
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let response = execute_metadata(
            &fixture,
            Request::DeleteIndex(serde_json::from_value(json!({"indexArn":index_arn})).unwrap()),
        )
        .await;
        assert!(matches!(response, Response::DeleteIndex(_)));
        let error = fixture
            .execute(Request::GetIndex(serde_json::from_value(json!({"indexArn":index_arn})).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::NotFound);
        let response = fixture
            .execute(Request::ListIndexes(serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap()))
            .await
            .unwrap();
        let Response::ListIndexes(indexes) = response else {
            panic!("expected Index list");
        };
        assert!(indexes.indexes.is_empty());
    });
}
