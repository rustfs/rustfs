// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::vector::{
    protocol::{ErrorCode, Request, Response, dto},
    service::{VectorExecution, VectorServiceError},
};

fn query(token: Option<String>) -> Request {
    Request::QueryVectors(dto::QueryVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        top_k: 205,
        query_vector: dto::VectorData { float32: vec![0.0; 3] },
        filter: None,
        query_mode: Some(dto::IndexMode::Enhanced),
        return_metadata: Some(true),
        return_distance: Some(true),
        next_token: token,
    })
}

fn put(rows: impl IntoIterator<Item = (u16, f32)>) -> Request {
    Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: rows
            .into_iter()
            .map(|(ordinal, value)| dto::PutInputVector {
                key: format!("row-{ordinal:03}"),
                data: dto::VectorData {
                    float32: vec![value, 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"value": value})),
            })
            .collect(),
    })
}

async fn populated() -> (CatalogFixture, ServiceRequestFixture) {
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let Request::CreateIndex(input) = &mut context.request else {
        panic!("create Index");
    };
    input.distance_metric = dto::DistanceMetric::Euclidean;
    execute(&fixture, &context).await.unwrap();
    context.request = put((0..205).map(|ordinal| (ordinal, f32::from(ordinal))));
    execute(&fixture, &context).await.unwrap();
    (fixture, context)
}

// Reuse the boxed request boundary to bound the multi-page orchestration poll frame.
async fn execute(
    fixture: &CatalogFixture,
    context: &ServiceRequestFixture,
) -> Result<VectorExecution<crate::table_catalog::test_support::TestCatalogObjectBackend>, VectorServiceError> {
    let service = index_service(fixture);
    service_retry_request(&service, context).await
}

async fn query_page(fixture: &CatalogFixture, context: &ServiceRequestFixture) -> dto::QueryVectorsOutput {
    // Each page uses a fresh service instance; only Catalog and object storage are shared.
    let VectorExecution::QueryVectors(page) = execute(fixture, context).await.unwrap() else {
        panic!("query page");
    };
    page.into_output().unwrap()
}

#[tokio::test]
async fn query_pages_reopen_original_snapshot_after_writes_and_repeat_last_page() {
    let (fixture, mut context) = populated().await;
    context.request = query(None);
    let first = query_page(&fixture, &context).await;
    assert_eq!(first.vectors.len(), 100);
    let first_token = first.next_token.clone().unwrap();
    context.request = put([(200, 0.25), (250, 0.5)]);
    execute(&fixture, &context).await.unwrap();
    context.request = Request::DeleteVectors(dto::DeleteVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["row-201".into()],
    });
    execute(&fixture, &context).await.unwrap();
    context.request = query(Some(first_token));
    let second = query_page(&fixture, &context).await;
    assert_eq!(second.vectors.len(), 100);
    let last_token = second.next_token.clone().unwrap();
    context.request = query(Some(last_token.clone()));
    let third = query_page(&fixture, &context).await;
    assert_eq!(third.vectors.len(), 5);
    assert!(third.next_token.is_none());
    for (ordinal, row) in first.vectors.iter().chain(&second.vectors).chain(&third.vectors).enumerate() {
        assert_eq!(row.key, format!("row-{ordinal:03}"));
        let value = f32::from(u16::try_from(ordinal).unwrap());
        assert_eq!(row.distance, Some(value));
        assert_eq!(row.metadata.as_ref().unwrap()["value"], serde_json::json!(value));
    }
    context.request = query(Some(last_token));
    assert_eq!(query_page(&fixture, &context).await, third);
}

#[tokio::test]
async fn query_tokens_bind_principal_parameters_permissions_and_fixed_expiry() {
    let (fixture, mut context) = populated().await;
    context.request = query(None);
    let first = query_page(&fixture, &context).await;
    let next = first.next_token.unwrap();
    context.request = query(Some(next.clone()));
    let original = context.clone();
    context.pagination_principal = "another-user".into();
    assert_invalid_token(execute(&fixture, &context).await);
    context = original.clone();
    let Request::QueryVectors(input) = &mut context.request else {
        panic!("query");
    };
    input.top_k = 204;
    assert_invalid_token(execute(&fixture, &context).await);
    context = original.clone();
    context.deny = true;
    assert!(matches!(execute(&fixture, &context).await,
        Err(VectorServiceError::Protocol(error)) if error.code() == ErrorCode::AccessDenied));
    context = original.clone();
    context.pagination_now += 899;
    let second = query_page(&fixture, &context).await;
    context.request = query(second.next_token);
    context.pagination_now += 1;
    assert_invalid_token(execute(&fixture, &context).await);
    context = original;
    let Request::QueryVectors(input) = &mut context.request else {
        panic!("query");
    };
    let mut bytes = next.into_bytes();
    let index = bytes.len() - 3;
    bytes[index] = if bytes[index] == b'A' { b'B' } else { b'A' };
    input.next_token = Some(String::from_utf8(bytes).unwrap());
    assert_invalid_token(execute(&fixture, &context).await);
}

fn assert_invalid_token<T>(result: Result<T, VectorServiceError>) {
    assert!(matches!(result, Err(VectorServiceError::Protocol(error)) if error.code() == ErrorCode::Validation));
}

#[tokio::test]
async fn list_pages_keep_snapshot_and_resume_each_segment_without_duplicates() {
    let (fixture, mut context) = populated().await;
    let mut keys = std::collections::BTreeSet::new();
    for segment in 0..3 {
        let mut token = None;
        loop {
            context.request = Request::ListVectors(dto::ListVectorsInput {
                vector_bucket_name: Some("vectors".into()),
                index_name: Some("service-index".into()),
                index_arn: None,
                max_results: Some(17),
                next_token: token,
                segment_count: Some(3),
                segment_index: Some(segment),
                return_data: Some(true),
                return_metadata: Some(true),
            });
            let VectorExecution::Complete(Response::ListVectors(page)) = execute(&fixture, &context).await.unwrap() else {
                panic!("list page");
            };
            assert!(page.vectors.len() <= 17);
            for row in page.vectors {
                assert!(keys.insert(row.key));
                assert!(row.data.is_some() && row.metadata.is_some());
            }
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
    }
    assert_eq!(keys.len(), 205);
}

#[tokio::test]
async fn query_token_cannot_resume_after_index_recreation() {
    let (fixture, mut context) = populated().await;
    context.request = query(None);
    let token = query_page(&fixture, &context).await.next_token.unwrap();
    context.request = Request::DeleteIndex(dto::DeleteIndexInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
    });
    execute(&fixture, &context).await.unwrap();
    let creation = service_request(&fixture);
    execute(&fixture, &creation).await.unwrap();
    context.request = query(Some(token));
    assert_invalid_token(execute(&fixture, &context).await);
}

#[tokio::test]
async fn single_page_query_does_not_persist_a_session() {
    use crate::table_catalog::TableCatalogObjectBackend;
    let (fixture, mut context) = populated().await;
    context.request = query(None);
    let Request::QueryVectors(input) = &mut context.request else {
        panic!("query");
    };
    input.top_k = 100;
    let future_size = std::mem::size_of_val(&execute(&fixture, &context));
    assert!(future_size <= 4096, "pagination request orchestration occupies {future_size} bytes");
    let page = query_page(&fixture, &context).await;
    assert!(page.next_token.is_none());
    let objects = fixture
        .backend
        .list_objects(context.bucket.storage_bucket_id().as_str(), ".rustfs-table/")
        .await
        .unwrap();
    assert!(objects.iter().all(|object| !object.contains("/query-results/")));
}
