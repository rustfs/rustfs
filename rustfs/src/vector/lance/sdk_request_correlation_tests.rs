// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::vector::fault_test::{
    FaultHandle, SdkRequestCorrelationFailure, SdkRequestFailureContext, SdkRequestFailureSource, observe_sdk_request_failure,
};

async fn injected_request(occurrence: u32) -> (ObservedRequest, CatalogResourceIdentity, FaultHandle, VectorServiceError) {
    let fixture = catalog_fixture().await;
    let service = index_service(&fixture);
    let mut request = ObservedRequest {
        owner: service_request(&fixture),
        instance_id: Uuid::new_v4(),
    };
    service.execute(&request).await.unwrap();
    let (guard, _) = fixture
        .store
        .begin_vector_operation(request.bucket(), &Namespace::parse("indexes").unwrap(), request.index_name())
        .await
        .unwrap();
    let resource = guard.resource_identity().clone();
    drop(guard);
    let fault = fault_test::arm(
        request.instance_id,
        resource.clone(),
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(occurrence).unwrap(),
        Duration::from_secs(300),
    )
    .unwrap();
    for ordinal in 1..occurrence {
        request.owner.request = put_request(&format!("published-{ordinal}"));
        service.execute(&request).await.unwrap();
    }
    request.owner.request = put_request("unpublished-correlated");
    let error = match service.execute(&request).await {
        Err(error) => error,
        Ok(_) => panic!("the original service call must return its actual injected error"),
    };
    assert!(injected_token(&error).is_some());
    assert!(fault.snapshot().injected_before_catalog);
    (request, resource, fault, error)
}

fn context<'a>(request: &'a ObservedRequest, resource: &'a CatalogResourceIdentity) -> SdkRequestFailureContext<'a> {
    SdkRequestFailureContext {
        instance_id: Some(request.instance_id),
        resource: Some(resource),
        request_id: request.request_id().as_uuid(),
        http_request_id: Some("sdk-component-http-request"),
        account: Some("sdk-component-account"),
    }
}

#[tokio::test]
async fn sdk_request_correlation_survives_concurrent_arm_and_rejects_duplicate_callback() {
    let (request, resource, fault, error) = injected_request(1).await;
    let token = injected_token(&error).unwrap();
    let receipt = fault.observed_sdk_candidate(token).unwrap();
    // A different thread performs the original registry cleanup after injection and before the callback.
    let other_resource = resource.clone();
    let other = tokio::task::spawn_blocking(move || {
        fault_test::arm(
            Uuid::new_v4(),
            other_resource,
            FaultPoint::SdkCandidatePrepared,
            NonZeroU32::new(1).unwrap(),
            Duration::from_secs(60),
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(matches!(
        fault_test::arm(
            request.instance_id,
            resource.clone(),
            FaultPoint::SdkCandidatePrepared,
            NonZeroU32::new(1).unwrap(),
            Duration::from_secs(60),
        ),
        Err(FaultError::Unavailable)
    ));
    observe_sdk_request_failure(context(&request, &resource), &std::io::Error::other("an unrelated service failure"));
    assert!(fault.snapshot().observation.unwrap().sdk_request_failure.is_none());
    observe_sdk_request_failure(context(&request, &resource), &error);
    assert_eq!(injected_token(&error), Some(token));
    let snapshot = fault.snapshot();
    assert!(!snapshot.observation_failed && !snapshot.overflow);
    assert!(snapshot.sdk_request_failure_error.is_none());
    let observed = snapshot.observation.unwrap().sdk_request_failure.unwrap();
    assert_eq!(observed.observation_token, token);
    assert_eq!(observed.request_id, request.request_id().as_uuid());
    assert_eq!(observed.http_request_id, "sdk-component-http-request");
    assert_eq!(observed.account, "sdk-component-account");
    assert_eq!(observed.commit_id, receipt.observation().commit_id);
    assert_eq!(observed.request_digest_sha256, receipt.observation().request_digest_sha256);
    assert_eq!(observed.source, SdkRequestFailureSource::InjectedBeforeCatalog);
    assert_eq!(other.snapshot().hits, 0);
    observe_sdk_request_failure(context(&request, &resource), &error);
    let duplicate = fault.snapshot();
    assert!(duplicate.observation_failed);
    assert_eq!(duplicate.sdk_request_failure_error, Some(SdkRequestCorrelationFailure::Duplicate));
    assert_eq!(duplicate.observation.unwrap().sdk_request_failure, Some(observed));
    assert!(fault.observed_sdk_candidate(token).is_err());
    assert_eq!(injected_token(&error), Some(token));
}

#[tokio::test]
async fn sdk_request_correlation_rejects_missing_and_mismatched_context() {
    for case in 0..10 {
        let (request, resource, fault, error) = injected_request(1).await;
        let foreign_resource = CatalogResourceIdentity::try_new(
            request.bucket().clone(),
            crate::storage_api::vector::ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
            CatalogFormat::Lance,
        )
        .unwrap();
        let oversized_account = "a".repeat(257);
        let mut selected = context(&request, &resource);
        let expected = match case {
            0 => {
                selected.instance_id = None;
                SdkRequestCorrelationFailure::InvalidInstance
            }
            1 => {
                selected.instance_id = Some(Uuid::new_v4());
                SdkRequestCorrelationFailure::IdentityMismatch
            }
            2 => {
                selected.resource = None;
                SdkRequestCorrelationFailure::MissingResource
            }
            3 => {
                selected.resource = Some(&foreign_resource);
                SdkRequestCorrelationFailure::IdentityMismatch
            }
            4 => {
                selected.request_id = Uuid::nil();
                SdkRequestCorrelationFailure::InvalidRequestId
            }
            5 => {
                selected.http_request_id = None;
                SdkRequestCorrelationFailure::InvalidHttpRequestId
            }
            6 => {
                selected.http_request_id = Some("invalid request id\n");
                SdkRequestCorrelationFailure::InvalidHttpRequestId
            }
            7 => {
                selected.account = None;
                SdkRequestCorrelationFailure::InvalidAccount
            }
            8 => {
                selected.account = Some(&oversized_account);
                SdkRequestCorrelationFailure::InvalidAccount
            }
            _ => {
                selected.account = Some("account\n");
                SdkRequestCorrelationFailure::InvalidAccount
            }
        };
        observe_sdk_request_failure(selected, &error);
        let snapshot = fault.snapshot();
        assert!(snapshot.observation_failed);
        assert_eq!(snapshot.sdk_request_failure_error, Some(expected));
        assert!(snapshot.observation.unwrap().sdk_request_failure.is_none());
        assert!(injected_token(&error).is_some());
    }
}

#[tokio::test]
async fn sdk_request_correlation_rejects_unissued_token_and_bounded_source_overflow() {
    let (request, resource, fault, error) = injected_request(1).await;
    let unissued = FaultError::InjectedBeforeCatalog {
        observation_token: Uuid::new_v4(),
    };
    observe_sdk_request_failure(context(&request, &resource), &unissued);
    let failed = fault.snapshot();
    assert!(failed.observation_failed);
    assert_eq!(failed.sdk_request_failure_error, Some(SdkRequestCorrelationFailure::UnissuedToken));
    assert!(failed.observation.unwrap().sdk_request_failure.is_none());
    assert!(injected_token(&error).is_some());

    #[derive(Debug, thiserror::Error)]
    #[error("component source wrapper")]
    struct SourceWrapper(#[source] Box<dyn std::error::Error + Send + Sync>);
    let (request, resource, fault, error) = injected_request(1).await;
    let mut wrapped: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
    for _ in 0..65 {
        wrapped = Box::new(SourceWrapper(wrapped));
    }
    observe_sdk_request_failure(context(&request, &resource), wrapped.as_ref());
    let failed = fault.snapshot();
    assert!(failed.observation_failed);
    assert_eq!(failed.sdk_request_failure_error, Some(SdkRequestCorrelationFailure::SourceChainLimit));
    assert!(failed.observation.unwrap().sdk_request_failure.is_none());

    let (request, resource, fault, error) = injected_request(1).await;
    fault.release();
    observe_sdk_request_failure(context(&request, &resource), &error);
    let released = fault.snapshot();
    assert!(released.observation_failed);
    assert_eq!(
        released.sdk_request_failure_error,
        Some(SdkRequestCorrelationFailure::UnavailableCandidate)
    );
    assert!(released.observation.unwrap().sdk_request_failure.is_none());
}

#[tokio::test]
async fn sdk_request_correlation_reports_real_event_capacity_exhaustion() {
    // Sixty successful original publications plus the selected publication consume all 64 events.
    let (request, resource, fault, error) = injected_request(61).await;
    let snapshot = fault.snapshot();
    assert_eq!(snapshot.hits, 61);
    assert_eq!(snapshot.events.len(), 64);
    assert!(snapshot.injected_before_catalog && !snapshot.overflow);
    observe_sdk_request_failure(context(&request, &resource), &error);
    let overflow = fault.snapshot();
    assert!(overflow.observation_failed && overflow.overflow);
    assert_eq!(overflow.events.len(), 64);
    assert_eq!(overflow.sdk_request_failure_error, Some(SdkRequestCorrelationFailure::CapacityExceeded));
    assert!(overflow.observation.unwrap().sdk_request_failure.is_none());
    assert!(injected_token(&error).is_some());
}
