// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
#[serial_test::serial]
fn vector_aws_sdk_pagination_shares_sessions_between_gateway_instances() {
    run_large_stack_test("vector-sdk-pagination", || async {
        let fixture = Fixture::new().await;
        let first_server = SdkServer::new(&fixture).await;
        let second_server = SdkServer::new(&fixture).await;
        let first = &first_server.client;
        let second = &second_server.client;
        let bucket = format!("sdk-pages-{}", Uuid::new_v4().simple());
        let result = AssertUnwindSafe(async {
            first.create_vector_bucket().vector_bucket_name(&bucket).send().await.unwrap();
            first
                .create_index()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .send()
                .await
                .unwrap();
            first
                .put_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .set_vectors(Some(
                    (0..205_u16)
                        .map(|i| vector(&format!("row-{i:03}"), [f32::from(i), 0.0], "original"))
                        .collect(),
                ))
                .send()
                .await
                .unwrap();
            let page = first
                .query_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .top_k(205)
                .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                .return_metadata(true)
                .return_distance(true)
                .send()
                .await
                .unwrap();
            assert_eq!(page.vectors().len(), 100);
            let mut token = page.next_token().unwrap().to_owned();
            let mut rows = page.vectors;
            let list = first
                .list_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .max_results(17)
                .return_data(true)
                .return_metadata(true)
                .send()
                .await
                .unwrap();
            let mut list_token = list.next_token;
            let mut listed = list.vectors;

            // Change both rank and membership after issuing the two session tokens.
            second
                .put_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .vectors(vector("row-200", [0.25, 0.0], "updated"))
                .vectors(vector("row-250", [0.5, 0.0], "new"))
                .send()
                .await
                .unwrap();
            second
                .delete_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .keys("row-201")
                .send()
                .await
                .unwrap();
            loop {
                let page = second
                    .query_vectors()
                    .vector_bucket_name(&bucket)
                    .index_name("pages")
                    .top_k(205)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .return_metadata(true)
                    .return_distance(true)
                    .next_token(&token)
                    .send()
                    .await
                    .unwrap();
                rows.extend(page.vectors);
                match page.next_token {
                    Some(next) => token = next,
                    None => break,
                }
            }
            assert_eq!(rows.len(), 205);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.key(), format!("row-{i:03}"));
                assert_eq!(row.distance(), Some(f32::from(u16::try_from(i).unwrap())));
                assert_eq!(row.metadata(), Some(&metadata("original")));
            }
            while let Some(token) = list_token {
                let page = second
                    .list_vectors()
                    .vector_bucket_name(&bucket)
                    .index_name("pages")
                    .max_results(17)
                    .return_data(true)
                    .return_metadata(true)
                    .next_token(token)
                    .send()
                    .await
                    .unwrap();
                listed.extend(page.vectors);
                list_token = page.next_token;
            }
            assert_eq!(listed.len(), 205);
            let keys: BTreeSet<_> = listed.iter().map(|row| row.key()).collect();
            assert_eq!(keys.len(), 205);
            assert!(keys.contains("row-201") && !keys.contains("row-250"));
            assert_eq!(
                listed.iter().find(|row| row.key() == "row-200").unwrap().data(),
                Some(&VectorData::Float32(vec![200.0, 0.0]))
            );

            let error = second
                .query_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .top_k(204)
                .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                .return_metadata(true)
                .return_distance(true)
                .next_token(&token)
                .send()
                .await
                .unwrap_err();
            assert_eq!(error.as_service_error().unwrap().code(), Some("ValidationException"));
            assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
            fixture
                .set_statements(json!([{ "Effect":"Deny","Action":"s3vectors:QueryVectors","Resource":"*" }]))
                .await;
            let error = first
                .query_vectors()
                .vector_bucket_name(&bucket)
                .index_name("pages")
                .top_k(205)
                .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                .return_metadata(true)
                .return_distance(true)
                .next_token(token)
                .send()
                .await
                .unwrap_err();
            assert_eq!(error.as_service_error().unwrap().code(), Some("AccessDeniedException"));
            assert_eq!(error.raw_response().unwrap().status().as_u16(), 403);
        })
        .catch_unwind()
        .await;
        first_server.finish().await;
        second_server.finish().await;
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}
