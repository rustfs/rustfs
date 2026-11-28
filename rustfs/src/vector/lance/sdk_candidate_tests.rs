// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Real service and Managed-object tests for the SDK candidate observation boundary.

use super::*;
use crate::table_catalog::{
    CatalogFormat, CatalogMetadataRef, CatalogMetadataRefRecordV1, CatalogResourceIdentity, Namespace, TableCatalogStore,
};
use crate::vector::fault_test::{self, FaultError, FaultEventKind, FaultPoint};
use crate::vector::lance::ManagedManifestObjects;
use crate::vector::protocol::{Request, Response, dto};
use crate::vector::service::{ManagedDatasetAccess, VectorExecution, VectorRequestAccess, VectorRequestId, VectorServiceError};
use std::time::Duration;
use uuid::Uuid;

struct ObservedObjects {
    owner: Arc<dyn ManagedManifestObjects>,
    instance_id: Uuid,
}

#[async_trait::async_trait]
impl ManagedManifestObjects for ObservedObjects {
    fn fault_instance_id(&self) -> Option<Uuid> {
        Some(self.instance_id)
    }

    async fn create_only(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        content: Bytes,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        self.owner.create_only(access, key, content).await
    }

    async fn create_multipart(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<Arc<dyn crate::vector::lance::ManagedDatasetMultipart>, LanceManifestError> {
        self.owner.create_multipart(access, key).await
    }

    async fn get(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        range: Option<std::ops::Range<u64>>,
        expected_body_revision: Option<Uuid>,
    ) -> Result<crate::table_catalog::TableCatalogObject, LanceManifestError> {
        self.owner.get(access, key, range, expected_body_revision).await
    }

    async fn head(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        self.owner.head(access, key).await
    }
}

struct ObservedRequest {
    owner: ServiceRequestFixture,
    instance_id: Uuid,
}

#[async_trait::async_trait]
impl VectorRequestAccess for ObservedRequest {
    fn request(&self) -> &Request {
        self.owner.request()
    }

    fn request_id(&self) -> VectorRequestId {
        self.owner.request_id()
    }

    fn bucket(&self) -> &crate::storage_api::vector::BucketHandle {
        self.owner.bucket()
    }

    fn index_name(&self) -> &crate::table_catalog::CatalogName {
        self.owner.index_name()
    }

    fn pagination_principal(&self) -> Result<[u8; 32], VectorServiceError> {
        self.owner.pagination_principal()
    }
    fn pagination_now(&self) -> i64 {
        self.owner.pagination_now()
    }
    async fn managed_query_results(
        &self,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
    ) -> Result<ManagedDatasetAccess, VectorServiceError> {
        self.owner.managed_query_results(guard, definition).await
    }

    async fn authorize_index(
        &self,
        request: &Request,
        resource: &CatalogResourceIdentity,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
        operation: Option<&crate::table_catalog::VectorCatalogOperationGuard>,
    ) -> Result<(), VectorServiceError> {
        self.owner.authorize_index(request, resource, definition, operation).await
    }

    async fn resolve_index_definition(
        &self,
        input: &dto::CreateIndexInput,
    ) -> Result<crate::table_catalog::VectorIndexDefinitionV1, VectorServiceError> {
        self.owner.resolve_index_definition(input).await
    }

    async fn managed_dataset(
        &self,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
    ) -> Result<ManagedDatasetAccess, VectorServiceError> {
        let owned = self.owner.managed_dataset(guard, definition).await?;
        Ok(ManagedDatasetAccess {
            access: owned.access,
            objects: Arc::new(ObservedObjects {
                owner: owned.objects,
                instance_id: self.instance_id,
            }),
        })
    }
}

fn put_request(key: &str) -> Request {
    Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: vec![dto::PutInputVector {
            key: key.into(),
            data: dto::VectorData {
                float32: vec![1.0, 0.0, 0.0],
            },
            metadata: Some(serde_json::json!({"source":"sdk-candidate"})),
        }],
    })
}

fn injected_token(error: &(dyn Error + 'static)) -> Option<Uuid> {
    let mut source = Some(error);
    while let Some(current) = source {
        if let Some(FaultError::InjectedBeforeCatalog { observation_token }) = current.downcast_ref::<FaultError>() {
            return Some(*observation_token);
        }
        source = current.source();
    }
    None
}

#[tokio::test]
async fn sdk_candidate_failure_retains_real_uploaded_closure_without_catalog_publication() {
    let fixture = catalog_fixture().await;
    let service = index_service(&fixture);
    let mut request = ObservedRequest {
        owner: service_request(&fixture),
        instance_id: Uuid::new_v4(),
    };
    service.execute(&request).await.unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    let (guard, before) = fixture
        .store
        .begin_vector_operation(request.bucket(), &namespace, request.index_name())
        .await
        .unwrap();
    let resource = guard.resource_identity().clone();
    let expected = before.head().expected_head();
    drop(guard);
    let fault = fault_test::arm(
        request.instance_id,
        resource.clone(),
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    request.owner.request = put_request("unpublished-key");
    let error = match service.execute(&request).await {
        Err(error) => error,
        Ok(_) => panic!("the original SDK request must fail before Catalog publication"),
    };
    let token = injected_token(&error).expect("the original typed injected error must remain in the source chain");
    let snapshot = fault.snapshot();
    assert_eq!(snapshot.instance_id, request.instance_id);
    assert_eq!(snapshot.hits, 1);
    assert_eq!(snapshot.arrivals, 1);
    assert_eq!(snapshot.completions, 1);
    assert!(snapshot.injected_before_catalog);
    assert!(!snapshot.timed_out && !snapshot.cancelled && !snapshot.observation_failed && !snapshot.overflow);
    assert_eq!(
        snapshot
            .events
            .iter()
            .filter(|event| matches!(event.kind, FaultEventKind::FailureInjected))
            .count(),
        1
    );
    let observation = snapshot.observation.unwrap();
    assert_eq!(observation.expected_head.version_token, expected.version_token().as_str());
    assert_eq!(observation.expected_head.generation, expected.generation().get());
    assert_eq!(observation.expected_head.metadata, expected.metadata().map(Into::into));
    assert_eq!(observation.expected_head.descriptor, expected.format_descriptor().map(Into::into));
    let candidate = observation.sdk_candidate.unwrap();
    assert_eq!(candidate.observation_token, token);
    assert_eq!(candidate.operation, "put-vectors");
    assert_eq!(candidate.request_digest_sha256.len(), 64);
    assert_eq!(candidate.descriptor.metadata, candidate.metadata);
    assert!(candidate.references.contains(&candidate.metadata));
    assert!(
        candidate
            .references
            .iter()
            .any(|reference| reference.object_key.contains("/data/") && reference.object_key.ends_with(".lance"))
    );
    assert!(
        candidate
            .references
            .iter()
            .all(|reference| reference.body_revision.is_some() && reference.etag.is_some())
    );
    let receipt = fault.observed_sdk_candidate(token).unwrap();
    assert_eq!(receipt.instance_id(), request.instance_id);
    assert_eq!(receipt.resource(), &resource);
    assert_eq!(receipt.observation().metadata, candidate.metadata);
    assert_eq!(receipt.expected_head().metadata, expected.metadata().map(Into::into));
    assert_eq!(receipt.manifest_version(), observation.manifest_version);
    assert!(matches!(fault.observed_sdk_candidate(Uuid::new_v4()), Err(FaultError::InvalidIdentity)));

    let (guard, after) = fixture
        .store
        .begin_vector_operation(request.bucket(), &namespace, request.index_name())
        .await
        .unwrap();
    assert_eq!(after.head().expected_head(), expected);
    assert!(
        fixture
            .store
            .get_commit_by_id(resource.storage_bucket_id(), resource.table_id().as_str(), &candidate.commit_id)
            .await
            .unwrap()
            .is_none()
    );
    let protected = fixture
        .store
        .read_vector_maintenance_candidate_roots_with_guard(&guard)
        .await
        .unwrap();
    assert!(protected.is_empty());
    let history = fixture
        .store
        .read_vector_maintenance_history_with_guard(&guard)
        .await
        .unwrap();
    assert!(
        history
            .iter()
            .all(|version| CatalogMetadataRefRecordV1::from(version.metadata()) != candidate.metadata)
    );
    request
        .authorize_index(request.request(), &resource, after.definition(), Some(&guard))
        .await
        .unwrap();
    let guard = Arc::new(guard);
    let managed = request.managed_dataset(guard.clone(), after.definition()).await.unwrap();
    let access = managed.access.clone();
    // A new ordinary service request observes the same committed rows after the original failure.
    request.owner.request = Request::GetVectors(dto::GetVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["unpublished-key".into()],
        return_data: Some(true),
        return_metadata: Some(true),
    });
    let VectorExecution::Complete(Response::GetVectors(get)) = service.execute(&request).await.unwrap() else {
        panic!("expected GetVectors");
    };
    assert!(get.vectors.is_empty());
    for record in &candidate.references {
        guard.check_held().unwrap();
        let exact = CatalogMetadataRef::try_from(record).unwrap();
        let bytes = managed
            .objects
            .get(
                &access,
                exact.object_key(),
                None,
                exact.body_revision().map(|revision| revision.as_uuid()),
            )
            .await
            .unwrap();
        assert_eq!(CatalogMetadataRef::from_read(exact.object_key().clone(), &bytes).unwrap(), exact);
    }
    guard.check_held().unwrap();
    drop(guard);
    // The consumed one-shot selector does not inject a later publication from the same service.
    request.owner.request = put_request("published-later");
    service.execute(&request).await.unwrap();
    assert_eq!(fault.snapshot().hits, 1);
}

#[tokio::test]
async fn sdk_candidate_selector_preserves_other_instances_resources_and_denials() {
    let fixture = catalog_fixture().await;
    let service = index_service(&fixture);
    let mut request = ObservedRequest {
        owner: service_request(&fixture),
        instance_id: Uuid::new_v4(),
    };
    service.execute(&request).await.unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    let (guard, _) = fixture
        .store
        .begin_vector_operation(request.bucket(), &namespace, request.index_name())
        .await
        .unwrap();
    let resource = guard.resource_identity().clone();
    drop(guard);
    let other_instance = fault_test::arm(
        Uuid::new_v4(),
        resource.clone(),
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    let foreign_resource = CatalogResourceIdentity::try_new(
        request.bucket().clone(),
        crate::storage_api::vector::ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    let other_resource = fault_test::arm(
        request.instance_id,
        foreign_resource,
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    request.owner.request = put_request("unmatched-publication");
    service.execute(&request).await.unwrap();
    assert_eq!(other_instance.snapshot().hits, 0);
    assert_eq!(other_resource.snapshot().hits, 0);
    let actual = fault_test::arm(
        request.instance_id,
        resource,
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    request.owner.deny = true;
    let calls = request.owner.managed_calls.load(std::sync::atomic::Ordering::SeqCst);
    let error = match service.execute(&request).await {
        Err(error) => error,
        Ok(_) => panic!("expected original authorization denial"),
    };
    assert!(injected_token(&error).is_none());
    assert_eq!(actual.snapshot().hits, 0);
    assert_eq!(request.owner.managed_calls.load(std::sync::atomic::Ordering::SeqCst), calls);
    request.owner.deny = false;
    actual.release();
    request.owner.request = put_request("released-publication");
    service.execute(&request).await.unwrap();
    assert_eq!(actual.snapshot().hits, 0);
    assert!(actual.snapshot().observation.is_none());
}

#[tokio::test]
async fn sdk_candidate_selector_does_not_capture_native_maintenance_compaction() {
    use crate::vector::lance::{LanceManifestAdapter, LanceOperationContext};
    let fixture = catalog_fixture().await;
    let service = index_service(&fixture);
    let mut request = ObservedRequest {
        owner: service_request(&fixture),
        instance_id: Uuid::new_v4(),
    };
    service.execute(&request).await.unwrap();
    for key in ["compaction-first", "compaction-second"] {
        request.owner.request = put_request(key);
        service.execute(&request).await.unwrap();
    }
    let namespace = Namespace::parse("indexes").unwrap();
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(request.bucket(), &namespace, request.index_name())
        .await
        .unwrap();
    let resource = guard.resource_identity().clone();
    let fault = fault_test::arm(
        request.instance_id,
        resource.clone(),
        FaultPoint::SdkCandidatePrepared,
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .unwrap();
    let guard = Arc::new(guard);
    let managed = request.managed_dataset(guard.clone(), entry.definition()).await.unwrap();
    let access = managed.access.clone();
    let index =
        LanceManifestAdapter::<crate::table_catalog::test_support::TestCatalogObjectBackend>::index_from_catalog(&entry).unwrap();
    let authorizer = Arc::new(TestPublicationAdmission {
        resource: resource.clone(),
        pause_at: usize::MAX,
        allowed: std::sync::atomic::AtomicBool::new(true),
        candidates: tokio::sync::Mutex::new(Vec::new()),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let control = maintenance_control();
    let adapter = LanceManifestAdapter::new_guarded(
        LanceOperationContext {
            catalog: fixture.store.clone(),
            objects: managed.objects,
            access: access.clone(),
            guard,
            definition: entry.definition().clone(),
        },
        request.index_name().clone(),
        index.clone(),
        io_limits(),
    )
    .unwrap()
    .with_maintenance_publication_authorizer(authorizer.clone())
    .unwrap()
    .with_maintenance_control(control.clone())
    .unwrap();
    let commit_request = maintenance_request(&fixture, &access, &index, 94_001).await;
    let base = adapter.write_base(&access, &commit_request).await.unwrap();
    assert_eq!(base.dataset.get_fragments().len(), 2);
    let options = crate::vector::lance::dataset::compaction_options();
    let plan = lance::dataset::optimize::plan_compaction(&base.dataset, &options)
        .await
        .unwrap();
    assert_eq!(plan.num_tasks(), 1);
    let mut results = Vec::new();
    for task in plan.compaction_tasks() {
        results.push(task.execute(&base.dataset).await.unwrap());
    }
    let expected_fragments = results
        .iter()
        .map(|result| u32::try_from(result.new_fragments.len()).unwrap())
        .sum();
    let handler = adapter.compaction_handler(
        commit_request,
        crate::table_catalog::CatalogCommitId::try_new(Uuid::from_u128(94_002).to_string()).unwrap(),
        expected_fragments,
        control,
    );
    let mut working = adapter
        .open_for_compaction(base.info().manifest_version, handler)
        .await
        .unwrap();
    lance::dataset::optimize::commit_compaction(
        &mut working,
        results,
        Arc::new(lance::dataset::index::DatasetIndexRemapperOptions::default()),
        &options,
    )
    .await
    .unwrap();
    assert_eq!(working.version().version, base.info().manifest_version + 2);
    assert_eq!(working.get_fragments().len(), 1);
    assert_eq!(working.count_rows(None).await.unwrap(), 2);
    assert_eq!(authorizer.candidates.lock().await.len(), 2);
    assert_eq!(fault.snapshot().hits, 0);
    assert!(fault.snapshot().observation.is_none());
}

#[path = "sdk_request_correlation_tests.rs"]
mod request_correlation_tests;
