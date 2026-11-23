// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::gating_test_env::run_large_stack_test;
use crate::app::vector_usecase::{maintenance::integration_tests::fixture, tests::Fixture};
use crate::vector::fault_test::{self, FaultPoint};
use crate::vector::protocol::Request;
use serde_json::json;
use std::num::NonZeroU32;

async fn uploaded_candidate() -> (Fixture, MaintenanceDriver, FaultHandle, VectorMaintenanceResourceV1, Uuid) {
    let (fixture, _, _, entry, _) = fixture().await;
    let resource = entry.head().resource_identity().clone();
    let resolved = VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
    let fault = fault_test::arm(
        fixture.app.object_store().id,
        resource.clone(),
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    let result = fixture
        .execute(Request::PutVectors(
            serde_json::from_value(json!({
                "vectorBucketName": resource.vector_bucket().unwrap().public_name().as_str(),
                "indexName": "maintenance.index",
                "vectors": [{"key":"sdk-observed", "data":{"float32":[1.0,2.0]}}]
            }))
            .unwrap(),
        ))
        .await;
    assert!(result.is_err(), "the original App request must return its injected failure");
    let snapshot = fault.snapshot();
    assert_eq!(snapshot.hits, 1);
    assert_eq!(snapshot.arrivals, 1);
    assert!(snapshot.injected_before_catalog);
    let token = snapshot.observation.unwrap().sdk_candidate.unwrap().observation_token;
    let driver = MaintenanceDriver::admit(fixture.app.clone(), &fixture.credentials.access_key)
        .await
        .unwrap();
    (fixture, driver, fault, resolved, token)
}

#[test]
#[serial_test::serial]
fn sdk_observer_reads_actual_receipt_catalog_and_bytes() {
    run_large_stack_test("sdk-observer-actual-receipt", || async {
        let (_fixture, driver, fault, resource, token) = uploaded_candidate().await;
        let receipt = fault.observed_sdk_candidate(token).unwrap();
        assert!(validate_sdk_receipt(Uuid::new_v4(), &resource, &receipt).is_err());
        assert!(driver.retain_sdk_candidate(&resource, &fault, Uuid::new_v4()).await.is_err());
        let retained = driver.retain_sdk_candidate(&resource, &fault, token).await.unwrap();
        assert_eq!(retained.reference_count, receipt.observation().references.len());
        assert_eq!(retained.commit_id, receipt.observation().commit_id);
        assert!(driver.retain_sdk_candidate(&resource, &fault, token).await.is_err());
        let observed = driver.inspect_sdk_candidate(token).await.unwrap();
        assert!(observed.commit.is_none());
        assert!(observed.protected_candidates.is_empty());
        assert!(!observed.unreferenced_ordinals.is_empty());
        let ordinal = receipt
            .observation()
            .references
            .iter()
            .position(|reference| reference == &receipt.observation().metadata)
            .unwrap();
        assert!(observed.unreferenced_ordinals.contains(&ordinal));
        let metadata = driver.inspect_sdk_candidate_object(token, ordinal, None).await.unwrap();
        assert_eq!(metadata.expected_object, receipt.observation().metadata);
        assert!(metadata.mod_time.is_some());
        assert!(metadata.quota_charge_bytes >= metadata.size);
        assert!(metadata.content_base64.is_none());
        assert!(metadata.content_sha256.is_none());
        let content = driver.inspect_sdk_candidate_object(token, ordinal, Some(0)).await.unwrap();
        assert!(content.complete_body);
        assert_eq!(content.quota_charge_bytes, metadata.quota_charge_bytes);
        assert_eq!(content.content_start, Some(0));
        assert_eq!(content.content_end, Some(content.size));
        assert_eq!(content.chunk_sha256, content.content_sha256);
        assert_eq!(content.body_revision, metadata.body_revision);
        assert_eq!(content.etag, metadata.etag);
        assert_eq!(content.mod_time, metadata.mod_time);
        let body = base64_simd::STANDARD.decode_to_vec(content.content_base64.unwrap()).unwrap();
        assert_eq!(u64::try_from(body.len()).unwrap(), receipt.observation().metadata.content_length);
        assert_eq!(
            hex_simd::encode_to_string(Sha256::digest(&body), hex_simd::AsciiCase::Lower),
            receipt.observation().metadata.content_sha256
        );
        assert!(
            driver
                .inspect_sdk_candidate_object(token, retained.reference_count, None)
                .await
                .is_err()
        );
        assert!(driver.inspect_sdk_candidate(Uuid::new_v4()).await.is_err());
        assert!(
            driver
                .inspect_sdk_candidate_object(token, ordinal, Some(u64::MAX))
                .await
                .is_err()
        );
        let tail = driver.inspect_sdk_candidate_object(token, ordinal, Some(1)).await.unwrap();
        assert!(!tail.complete_body);
        assert_eq!(tail.quota_charge_bytes, metadata.quota_charge_bytes);
        assert!(tail.content_sha256.is_none());
        assert_eq!(tail.content_start, Some(1));
        assert_eq!(base64_simd::STANDARD.decode_to_vec(tail.content_base64.unwrap()).unwrap(), body[1..]);
        driver.release_sdk_candidate(token).await.unwrap();
        assert!(driver.inspect_sdk_candidate(token).await.is_err());
        assert!(driver.inspect_sdk_candidate_quota(token).await.is_err());
        assert!(driver.release_sdk_candidate(token).await.is_err());
    });
}

#[test]
#[serial_test::serial]
fn sdk_observer_revalidates_current_iam_and_resource() {
    run_large_stack_test("sdk-observer-current-authority", || async {
        let (fixture, driver, fault, resource, token) = uploaded_candidate().await;
        let bucket = resource.bucket().to_handle().unwrap().public_name().as_str().to_owned();
        fixture
            .execute(Request::CreateIndex(
                serde_json::from_value(json!({
                    "vectorBucketName":bucket,"indexName":"observer.other","dataType":"float32",
                    "dimension":2,"distanceMetric":"euclidean"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let other = driver.resolve_resource(&bucket, "observer.other").await.unwrap();
        assert!(driver.retain_sdk_candidate(&other, &fault, token).await.is_err());
        driver.retain_sdk_candidate(&resource, &fault, token).await.unwrap();
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]))
            .await;
        let denied = driver.inspect_sdk_candidate(token).await.unwrap_err();
        assert_eq!(denied.category(), VectorMaintenanceFailureCode::IdentityDenied);
        assert!(std::error::Error::source(&denied).is_some());
        assert!(driver.inspect_sdk_candidate_object(token, 0, None).await.is_err());
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]))
            .await;
        driver.inspect_sdk_candidate(token).await.unwrap();
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({"vectorBucketName":bucket,"indexName":"maintenance.index"})).unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .execute(Request::CreateIndex(
                serde_json::from_value(json!({
                    "vectorBucketName":bucket,"indexName":"maintenance.index","dataType":"float32",
                    "dimension":2,"distanceMetric":"euclidean"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        assert!(driver.inspect_sdk_candidate(token).await.is_err());
        assert!(driver.inspect_sdk_candidate_object(token, 0, Some(0)).await.is_err());
    });
}
