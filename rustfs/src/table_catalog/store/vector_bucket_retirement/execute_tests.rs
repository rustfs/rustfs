// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::table_catalog::test_support::TestCatalogObjectBackend;

struct Fixture {
    backend: TestCatalogObjectBackend,
    store: ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: BucketHandle,
    parent: String,
    namespace: String,
    _lifecycle: TableCatalogLockGuard,
    fence: TableCatalogObjectMutationFence,
}

struct UnlockedFixture {
    backend: TestCatalogObjectBackend,
    store: ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: BucketHandle,
    parent: String,
    namespace: String,
}

impl UnlockedFixture {
    async fn into_locked(self) -> Fixture {
        let lifecycle = self
            .backend
            .acquire_write_lock(self.bucket.storage_bucket_id().as_str(), "")
            .await
            .unwrap();
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal()]);
        Fixture {
            backend: self.backend,
            store: self.store,
            bucket: self.bucket,
            parent: self.parent,
            namespace: self.namespace,
            _lifecycle: lifecycle,
            fence,
        }
    }
}

async fn unretired_bucket_fixture() -> UnlockedFixture {
    let backend = TestCatalogObjectBackend::default();
    let binding = VectorBucketBindingV1 {
        version: 1,
        storage_bucket_id: "catalog-retirement-storage".into(),
        bucket_incarnation: Uuid::new_v4(),
        account_namespace_id: String::new(),
        region: "us-east-1".into(),
        public_name: "retirement-test".into(),
    };
    let bucket = binding.to_handle().unwrap();
    backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(binding.storage_bucket_id.clone(), bucket.clone());
    let store = ObjectTableCatalogStore::for_vector_scope(backend.clone(), bucket.clone())
        .await
        .unwrap();
    let parent = store.paths.table_bucket_entry_path(bucket.storage_bucket_id().as_str());
    let namespace_id = Namespace::parse("indexes").unwrap();
    let namespace = store
        .paths
        .namespace_entry_path(bucket.storage_bucket_id().as_str(), &namespace_id);
    let parent_body = TableBucketEntry {
        vector_bucket_binding: Some(binding),
        version: 2,
        table_bucket: bucket.storage_bucket_id().as_str().into(),
        catalog_type: "s3vectors".into(),
        warehouse_root: format!("s3://{}/", bucket.storage_bucket_id().as_str()),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        active_rename_id: None,
        created_at: None,
        updated_at: None,
    };
    let namespace_body = NamespaceEntry {
        version: 1,
        table_bucket: bucket.storage_bucket_id().as_str().into(),
        namespace: namespace_id.public_name(),
        namespace_id: namespace_id.storage_id(),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    backend
        .seed_object(RUSTFS_META_BUCKET, &parent, serde_json::to_vec(&parent_body).unwrap())
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &namespace, serde_json::to_vec(&namespace_body).unwrap())
        .await;
    UnlockedFixture {
        backend,
        store,
        bucket,
        parent,
        namespace,
    }
}

async fn retirement_fixture() -> Fixture {
    unretired_bucket_fixture().await.into_locked().await
}

async fn delete_count(fixture: &Fixture, key: &str) -> usize {
    fixture
        .backend
        .state
        .lock()
        .await
        .delete_attempts
        .get(&(RUSTFS_META_BUCKET.into(), key.into()))
        .copied()
        .unwrap_or(0)
}

async fn prepare(fixture: &Fixture) -> CatalogBucketRetirementAuthority {
    fixture
        .store
        .prepare_bucket_retirement_plan(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
}

async fn progress(
    fixture: &Fixture,
    authority: &VectorBucketCatalogRetirementV1,
) -> (String, VectorBucketCatalogRetirementProgressPageV1) {
    let page: VectorBucketCatalogRetirementPlanPageV1 = fixture
        .store
        .read_bucket_retirement_record(&authority.plan_root, &fixture.fence)
        .await
        .unwrap();
    let path = fixture
        .store
        .paths
        .vector_bucket_retirement_page_path(&fixture.bucket, authority.retirement_id, "progress", page.page_id)
        .unwrap();
    let object = fixture
        .backend
        .read_object_limited(RUSTFS_META_BUCKET, &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
        .await
        .unwrap()
        .unwrap();
    (path, decode_vector_maintenance_record(&object.data).unwrap())
}

#[tokio::test]
async fn catalog_retirement_requires_all_pending_pages_before_authority() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let (_, initial) = progress(&fixture, &authority.record).await;
    assert!(
        initial
            .targets
            .iter()
            .all(|target| target.state == VectorCatalogRetirementTargetState::Pending)
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
    let (path, _) = progress(&fixture, &authority.record).await;
    fixture.backend.delete_object(RUSTFS_META_BUCKET, &path).await.unwrap();
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
}

#[tokio::test]
async fn catalog_retirement_rechecks_pending_exact_source_before_dispatch() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &fixture.parent, b"changed-source".to_vec())
        .await;
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
}

#[tokio::test]
async fn catalog_retirement_native_unknown_and_missing_source_never_become_ack() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let record = authority.record.clone();
    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    fixture
        .backend
        .fail_after_next_delete(RUSTFS_META_BUCKET, &fixture.namespace)
        .await;
    let execution = CatalogCleanupExecution::new(Uuid::new_v4()).unwrap();
    assert!(
        fixture
            .store
            .execute_bucket_retirement(&validated, &execution, &fixture.fence)
            .await
            .is_err()
    );
    assert!(
        fixture
            .backend
            .read_object_limited(RUSTFS_META_BUCKET, &fixture.namespace, 8192)
            .await
            .unwrap()
            .is_none()
    );
    let (_, observed) = progress(&fixture, &record).await;
    assert!(observed.targets.iter().any(|target| matches!(
        target.state,
        VectorCatalogRetirementTargetState::InFlight | VectorCatalogRetirementTargetState::Unknown
    )));
    assert!(observed.targets.iter().all(|target| target.acknowledged_at.is_none()));
    let authority = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 1);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
}

#[tokio::test]
async fn catalog_retirement_original_ack_resumes_without_redeleting_absent_source() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let record = authority.record.clone();
    let (path, _) = progress(&fixture, &record).await;
    // Initial Pending is write 1, then namespace InFlight/ACK are 2/3.
    // The next target's failed InFlight write leaves its original Pending state.
    fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &path, 4).await;
    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    let execution = CatalogCleanupExecution::new(Uuid::new_v4()).unwrap();
    assert!(
        fixture
            .store
            .execute_bucket_retirement(&validated, &execution, &fixture.fence)
            .await
            .is_err()
    );
    let (_, before) = progress(&fixture, &record).await;
    let ack = before
        .targets
        .iter()
        .find(|target| target.state == VectorCatalogRetirementTargetState::DeleteAcknowledged)
        .unwrap()
        .clone();
    assert!(
        before
            .targets
            .iter()
            .any(|target| target.state == VectorCatalogRetirementTargetState::Pending)
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 1);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    let authority = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    fixture
        .store
        .execute_bucket_retirement(&validated, &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(), &fixture.fence)
        .await
        .unwrap();
    let (_, after) = progress(&fixture, &record).await;
    assert_eq!(after.targets[usize::from(ack.ordinal)], ack);
    assert!(
        after
            .targets
            .iter()
            .all(|target| target.state == VectorCatalogRetirementTargetState::DeleteAcknowledged)
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 1);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 1);
}

#[tokio::test]
async fn catalog_retirement_completed_catalog_reopens_for_physical_retry_without_initialization() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    fixture
        .store
        .execute_bucket_retirement(&validated, &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(), &fixture.fence)
        .await
        .unwrap();
    // Keep the physical bucket identity, as when the caller's later physical step
    // fails. The ordinary factory must still reach the original authority.
    let reopened = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    let authority = reopened
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    let again = reopened
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    reopened
        .execute_bucket_retirement(&again, &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(), &fixture.fence)
        .await
        .unwrap();
    assert!(
        fixture
            .backend
            .read_object_limited(RUSTFS_META_BUCKET, &fixture.parent, 8192)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .backend
            .read_object_limited(RUSTFS_META_BUCKET, &fixture.namespace, 8192)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 1);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 1);
}

#[tokio::test]
async fn catalog_retirement_pending_absence_requires_original_native_confirmation() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let record = authority.record.clone();
    fixture
        .backend
        .delete_object(RUSTFS_META_BUCKET, &fixture.namespace)
        .await
        .unwrap();
    let deletes = delete_count(&fixture, &fixture.namespace).await;
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    let (_, progress) = progress(&fixture, &record).await;
    assert!(
        progress
            .targets
            .iter()
            .all(|target| target.state == VectorCatalogRetirementTargetState::Pending)
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, deletes);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
}

#[tokio::test]
async fn catalog_retirement_rejects_new_source_outside_original_inventory() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let unexpected = format!(
        "{}unexpected.json",
        fixture
            .store
            .paths
            .table_bucket_root_prefix(fixture.bucket.storage_bucket_id().as_str())
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &unexpected, b"{}".to_vec())
        .await;
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
    assert_eq!(delete_count(&fixture, &unexpected).await, 0);
}

#[tokio::test]
async fn catalog_retirement_failed_unknown_publication_retains_inflight_and_original_error() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let record = authority.record.clone();
    let (path, _) = progress(&fixture, &record).await;
    fixture
        .backend
        .fail_after_next_delete(RUSTFS_META_BUCKET, &fixture.namespace)
        .await;
    // Pending is write 1, InFlight is write 2, and the Unknown diagnostic is write 3.
    fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &path, 3).await;
    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    let error = fixture
        .store
        .execute_bucket_retirement(&validated, &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(), &fixture.fence)
        .await
        .unwrap_err();
    assert!(
        matches!(error, TableCatalogStoreError::Internal(message) if message == format!("injected post-commit delete failure for {} attempt 1", fixture.namespace))
    );
    let (_, observed) = progress(&fixture, &record).await;
    assert!(
        observed
            .targets
            .iter()
            .any(|target| target.state == VectorCatalogRetirementTargetState::InFlight)
    );
    assert!(observed.targets.iter().all(|target| target.acknowledged_at.is_none()));
    let authority = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 1);
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
}

async fn repaginate_test_plan(
    fixture: &Fixture,
    original: CatalogBucketRetirementAuthority,
    repeat_target: bool,
) -> (CatalogBucketRetirementAuthority, String) {
    let original_page: VectorBucketCatalogRetirementPlanPageV1 = fixture
        .store
        .read_bucket_retirement_record(&original.record.plan_root, &fixture.fence)
        .await
        .unwrap();
    assert_eq!(original_page.targets.len(), 2);
    let mut root = None;
    let mut tail_path = String::new();
    for ordinal in 0..2 {
        let target = original_page.targets[if repeat_target { 0 } else { ordinal }].clone();
        let page = VectorBucketCatalogRetirementPlanPageV1 {
            version: 1,
            bucket: original.record.bucket.clone(),
            retirement_id: original.record.retirement_id,
            page_id: Uuid::new_v4(),
            next_page: root,
            resources: Vec::new(),
            targets: vec![target],
        };
        let path = fixture
            .store
            .paths
            .vector_bucket_retirement_page_path(&fixture.bucket, page.retirement_id, "plan", page.page_id)
            .unwrap();
        let reference = fixture
            .store
            .write_bucket_retirement_record(&path, None, &page, &fixture.fence)
            .await
            .unwrap();
        if ordinal == 0 {
            tail_path = path;
        }
        let progress = VectorBucketCatalogRetirementProgressPageV1 {
            version: 1,
            bucket: page.bucket.clone(),
            retirement_id: page.retirement_id,
            plan_page: (&reference).into(),
            revision: Uuid::new_v4(),
            previous_revision: None,
            targets: vec![VectorCatalogRetirementTargetProgressV1 {
                ordinal: 0,
                state: VectorCatalogRetirementTargetState::Pending,
                dispatch_id: None,
                worker_instance_id: None,
                worker_boot_id: None,
                started_at: None,
                acknowledged_at: None,
                failure: None,
            }],
        };
        let progress_path = fixture
            .store
            .paths
            .vector_bucket_retirement_page_path(&fixture.bucket, page.retirement_id, "progress", page.page_id)
            .unwrap();
        fixture
            .store
            .write_bucket_retirement_record(&progress_path, None, &progress, &fixture.fence)
            .await
            .unwrap();
        root = Some((&reference).into());
    }
    let mut record = original.record;
    record.plan_root = root.unwrap();
    record.plan_page_count = 2;
    // This decoder/reader fixture replaces only its own test authority to exercise
    // alternate page layouts and corruptions. It creates no deletion confirmation.
    let path = fixture
        .store
        .paths
        .vector_bucket_retirement_authority_path(&fixture.bucket)
        .unwrap();
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, encode_vector_bucket_retirement_record(&record).unwrap())
        .await;
    (
        fixture
            .store
            .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
            .await
            .unwrap()
            .unwrap(),
        tail_path,
    )
}

#[tokio::test]
async fn catalog_retirement_plan_reader_requires_complete_unique_page_inventory() {
    let fixture = retirement_fixture().await;
    let original = prepare(&fixture).await;
    let (authority, later_page) = repaginate_test_plan(&fixture, original, false).await;
    let original_record = authority.record.clone();
    fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    fixture.backend.delete_object(RUSTFS_META_BUCKET, &later_page).await.unwrap();
    let authority = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.record, original_record);
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);

    let fixture = retirement_fixture().await;
    let original = prepare(&fixture).await;
    let (authority, _) = repaginate_test_plan(&fixture, original, true).await;
    assert!(
        fixture
            .store
            .validate_bucket_retirement_authority(authority, &fixture.fence)
            .await
            .is_err()
    );
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
}

#[tokio::test]
async fn catalog_retirement_authority_requires_original_full_bucket_binding() {
    let fixture = retirement_fixture().await;
    let authority = prepare(&fixture).await;
    let path = fixture
        .store
        .paths
        .vector_bucket_retirement_authority_path(&fixture.bucket)
        .unwrap();
    for field in [
        "bucket_incarnation",
        "account_namespace_id",
        "region",
        "public_name",
        "storage_bucket_id",
    ] {
        let mut record = serde_json::to_value(&authority.record).unwrap();
        record["bucket"][field] = if field == "bucket_incarnation" {
            serde_json::json!(Uuid::new_v4())
        } else {
            serde_json::json!("different-identity")
        };
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&record).unwrap())
            .await;
        assert!(
            fixture
                .store
                .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
                .await
                .is_err(),
            "{field}"
        );
    }
    assert_eq!(delete_count(&fixture, &fixture.parent).await, 0);
    assert_eq!(delete_count(&fixture, &fixture.namespace).await, 0);
}

async fn seed_retired_index_with_multipage_history(
    fixture: &UnlockedFixture,
    index_name: &str,
    extra_commits: usize,
    extra_data_files: usize,
) -> VectorMaintenanceResourceV1 {
    seed_retired_index_with_multipage_history_opts(fixture, index_name, extra_commits, extra_data_files, false).await
}

async fn seed_retired_index_with_multipage_history_opts(
    fixture: &UnlockedFixture,
    index_name: &str,
    extra_commits: usize,
    extra_data_files: usize,
    omit_first_history_ref: bool,
) -> VectorMaintenanceResourceV1 {
    use crate::storage_api::table::managed::ManagedTableId;

    let storage = fixture.bucket.storage_bucket_id().as_str();
    let namespace = Namespace::parse("indexes").unwrap();
    let name = CatalogName::parse(index_name).unwrap();
    let table_id = Uuid::new_v4().to_string();
    let resource_identity = CatalogResourceIdentity::try_new(
        fixture.bucket.clone(),
        ManagedTableId::try_new(table_id.clone()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    let create_id = Uuid::new_v4().to_string();
    let initial_key = format!("{}_versions/{create_id}/1.manifest", resource_identity.dataset_prefix());
    let initial_object_key = CatalogObjectKey::try_new(initial_key.clone()).unwrap();
    let planned_manifest = CatalogMetadataRef::from_read(
        initial_object_key.clone(),
        &TableCatalogObject {
            data: b"initial-manifest-bytes".to_vec(),
            body_revision: None,
            etag: None,
            mod_time: None,
        },
    )
    .unwrap();
    let request = VectorIndexCreateRequest {
        resource_identity: resource_identity.clone(),
        namespace: namespace.clone(),
        name: name.clone(),
        create_id: CatalogCommitId::try_new(create_id).unwrap(),
        request_id: Uuid::new_v4(),
        definition: VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
            tags: BTreeMap::from([("suite".into(), "d09-scale".into())]),
        },
        initialization_payload:
            br#"{"encoding_version":1,"data_type":"float32","dimension":3,"distance_metric":"cosine","non_filterable_metadata_keys":[]}"#
                .to_vec(),
        planned_manifest,
    };
    let (reservation, create_guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    fixture
        .backend
        .seed_object(storage, &initial_key, b"initial-manifest-bytes".to_vec())
        .await;
    let initial_obj = fixture.backend.read_object(storage, &initial_key).await.unwrap().unwrap();
    let uploaded_initial = CatalogMetadataRef::from_read(initial_object_key, &initial_obj).unwrap();
    let initial_descriptor =
        CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, uploaded_initial.clone(), b"initial-desc".to_vec()).unwrap();
    let initial_prepared = PreparedCatalogCommit::try_from_validated_parts(
        CatalogCommitParts {
            resource_identity: resource_identity.clone(),
            namespace: namespace.clone(),
            name: name.clone(),
            expected_head: reservation.expected_head().clone(),
            commit_id: request.create_id.clone(),
            operation: "vector-create".into(),
            idempotency_key: None,
            writer: None,
        },
        uploaded_initial.clone(),
        initial_descriptor,
        CatalogEntryUpdates::try_new(
            Some(format!("s3://{storage}/{}", resource_identity.dataset_prefix().trim_end_matches('/'))),
            Some(1),
        )
        .unwrap(),
        Vec::new(),
    )
    .unwrap();
    let mut current_entry = fixture
        .store
        .publish_vector_index_initial_head(&create_guard, &reservation, initial_prepared)
        .await
        .unwrap();
    drop(create_guard);

    let mut history_refs: Vec<CatalogMetadataRefRecordV1> = vec![(&uploaded_initial).into()];
    let mut retirement_keys = vec![initial_key];

    let (op_guard, _) = fixture
        .store
        .begin_vector_operation(&fixture.bucket, &namespace, &name)
        .await
        .unwrap();
    for step in 0..extra_commits {
        let commit_id = Uuid::new_v4().to_string();
        let version = u64::try_from(step + 2).unwrap();
        let manifest_key = format!("{}_versions/{commit_id}/{version}.manifest", resource_identity.dataset_prefix());
        let payload = format!("manifest-{table_id}-{version}").into_bytes();
        fixture.backend.seed_object(storage, &manifest_key, payload).await;
        let obj = fixture.backend.read_object(storage, &manifest_key).await.unwrap().unwrap();
        let candidate = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(manifest_key.clone()).unwrap(), &obj).unwrap();
        let descriptor =
            CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, candidate.clone(), b"step-desc".to_vec()).unwrap();
        let prepared = PreparedCatalogCommit::try_from_validated_parts(
            CatalogCommitParts {
                resource_identity: resource_identity.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                expected_head: current_entry.head().expected_head(),
                commit_id: CatalogCommitId::try_new(commit_id.clone()).unwrap(),
                operation: "append".into(),
                idempotency_key: Some(commit_id),
                writer: None,
            },
            candidate.clone(),
            descriptor,
            CatalogEntryUpdates::try_new(Some(current_entry.entry.warehouse_location.clone()), Some(1)).unwrap(),
            Vec::new(),
        )
        .unwrap();
        let committed = fixture
            .store
            .commit_vector_prepared_with_guard(&op_guard, prepared, current_entry.definition())
            .await
            .unwrap();
        current_entry = VectorCatalogEntry::from_entry(committed.table).unwrap();
        history_refs.push((&candidate).into());
        retirement_keys.push(manifest_key);
    }
    drop(op_guard);

    for idx in 0..extra_data_files {
        let data_key = format!("{}data/fragment-{idx:04}.lance", resource_identity.dataset_prefix());
        fixture
            .backend
            .seed_object(storage, &data_key, format!("fragment-{idx}").into_bytes())
            .await;
        retirement_keys.push(data_key);
    }

    let enrolled = fixture
        .store
        .enroll_vector_maintenance(
            &current_entry,
            vector_maintenance_enroll_commit_id(
                &resource_identity,
                &VectorMaintenanceHeadV1::from_entry(&current_entry).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resource = VectorMaintenanceResourceV1::try_new(&resource_identity, &namespace, &name).unwrap();
    let (delete_guard, _) = fixture
        .store
        .begin_vector_operation(&fixture.bucket, &namespace, &name)
        .await
        .unwrap();
    fixture
        .store
        .delete_vector_index_with_guard(&delete_guard, &enrolled, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
        .await
        .unwrap();
    drop(delete_guard);

    let snapshot = fixture
        .store
        .read_vector_maintenance_resource(&resource_identity, &namespace, &name)
        .await
        .unwrap();
    let maint_op = fixture
        .store
        .begin_vector_maintenance_resource_operation(&snapshot)
        .await
        .unwrap();
    let copied_intent = fixture
        .store
        .copy_vector_maintenance_source_with_operation_guard(&maint_op, snapshot.source_intent().unwrap())
        .await
        .unwrap();
    if omit_first_history_ref {
        assert!(history_refs.len() >= 3);
        history_refs.remove(0);
    }
    let evidence = VectorRetirementEvidenceV1::try_new(
        resource.clone(),
        snapshot.head.clone().unwrap(),
        Some((&copied_intent).into()),
        history_refs,
        Vec::new(),
        Vec::new(),
        VectorHistoryRetentionV1::All,
    )
    .unwrap();

    retirement_keys.sort();
    let aged_mod_time = VectorObjectTimeV1::from_time(OffsetDateTime::now_utc() - Duration::seconds(3600));
    let mut targets = Vec::with_capacity(retirement_keys.len());
    for key in &retirement_keys {
        let meta = fixture.backend.object_metadata(storage, key).await.unwrap().unwrap();
        let mut target = VectorRetirementTargetV1::from_object_metadata(
            &resource_identity,
            CatalogObjectKey::try_new(key.clone()).unwrap(),
            &meta,
        )
        .unwrap();
        target.mod_time = Some(aged_mod_time.clone());
        targets.push(target);
    }
    let prepared_retirement = fixture
        .store
        .prepare_vector_retirement(&maint_op, evidence, targets.clone())
        .await
        .unwrap();
    let plan_ref_record: CatalogMetadataRefRecordV1 = prepared_retirement.plan_ref().into();
    let plan_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &plan_ref_record.object_key)
        .await
        .unwrap()
        .unwrap();
    let plan_record: VectorRetirementPlanV1 = decode_vector_maintenance_record(&plan_obj.data).unwrap();
    let spec = VectorMaintenanceJobSpec::from_semantic(
        VectorMaintenanceSemanticSpec::try_new(
            resource.clone(),
            VectorMaintenanceKind::HistoryGc,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(snapshot.head.clone(), None, Some(plan_ref_record.clone())).unwrap(),
            VectorMaintenanceExecutionIdentityV1::try_new("d09-scale-account".into(), [7; 16]).unwrap(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .unwrap(),
    )
    .unwrap();
    drop(maint_op);

    let created = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    let observed = fixture
        .store
        .read_vector_maintenance_resource(&resource_identity, &namespace, &name)
        .await
        .unwrap();
    let condition = VectorMaintenanceClaimCondition::from_snapshot(&created, observed);
    let worker = VectorMaintenanceWorker::try_new("d09-scale-worker".into(), Uuid::new_v4(), spec.execution_identity).unwrap();
    let VectorMaintenanceClaimOutcome::Acquired(work) = fixture
        .store
        .claim_vector_maintenance_job(&resource_identity, spec.job_id, condition, worker, Uuid::new_v4())
        .await
        .unwrap()
    else {
        panic!("claim maintenance job");
    };

    let job_path = fixture
        .store
        .paths
        .vector_maintenance_job_path(&resource_identity, spec.job_id)
        .unwrap();
    let job_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &job_path)
        .await
        .unwrap()
        .unwrap();
    let mut claimed_job: VectorMaintenanceJobV1 = decode_vector_maintenance_record(&job_obj.data).unwrap();
    let prefix = fixture.store.paths.vector_maintenance_prefix(&resource_identity).unwrap();
    let ts = claimed_job.updated_at.clone();
    let retirement_id = Uuid::new_v4();
    let auth = VectorRetirementAuthorizationV1 {
        version: 1,
        retirement_id,
        retirement_revision: Uuid::new_v4(),
        created_by_job_id: claimed_job.job_id,
        created_by_attempt: claimed_job.attempt,
        resource: resource.clone(),
        execution_identity: claimed_job.execution_identity.clone(),
        created_at: ts.clone(),
        previous_ref: None,
        base_head: snapshot.head.unwrap(),
        delete_intent_ref: Some((&copied_intent).into()),
        plan_ref: plan_ref_record,
        reference_evidence_ref: plan_record.evidence_ref,
        targets: targets.clone(),
    };
    let auth_key = format!("{prefix}authorizations/{}.json", table_catalog_path_hash(&retirement_id.to_string()));
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &auth_key, encode_vector_maintenance_record(&auth).unwrap())
        .await;
    let auth_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &auth_key)
        .await
        .unwrap()
        .unwrap();
    let auth_ref: CatalogMetadataRefRecordV1 =
        (&CatalogMetadataRef::from_read(CatalogObjectKey::try_new(auth_key).unwrap(), &auth_obj).unwrap()).into();

    for key in &retirement_keys {
        fixture.backend.delete_object(storage, key).await.unwrap();
    }

    let progress_targets = {
        let claimed_lease = claimed_job.lease.as_ref().unwrap();
        targets
            .into_iter()
            .enumerate()
            .map(|(ordinal, t)| VectorRetirementTargetProgressV1 {
                ordinal: u16::try_from(ordinal).unwrap(),
                state: VectorRetirementTargetState::DeleteCommitted,
                dispatch: Some(VectorRetirementDispatchV1 {
                    dispatch_id: Uuid::new_v4(),
                    job_id: claimed_job.job_id,
                    job_revision: claimed_job.revision,
                    attempt: claimed_job.attempt,
                    lease_id: claimed_lease.lease_id,
                    worker_id: claimed_lease.worker_id.clone(),
                    worker_boot_id: claimed_lease.worker_boot_id,
                    execution_identity: claimed_job.execution_identity.clone(),
                    admitted_at: ts.clone(),
                }),
                receipt: Some(VectorRetirementReceiptV1 {
                    outcome: VectorRetirementReceiptOutcome::DeleteCommitted,
                    completed_at: ts.clone(),
                    accounting: Some(VectorDeleteAccountingV1::try_new(Some(t.content_length), None, true).unwrap()),
                    recovery_pending: 0,
                    recovery_blocked: 0,
                    recovery_failed: 0,
                }),
                failure: None,
            })
            .collect()
    };
    let p_sha = CatalogContentSha256::digest(b"progress-complete");
    let progress = VectorRetirementProgressV1 {
        version: 1,
        resource: resource.clone(),
        retirement_id,
        authorization_ref: auth_ref.clone(),
        revision: Uuid::new_v4(),
        last_update_id: crate::table_catalog::vector_maintenance_model::maintenance_uuid_from_digest(*p_sha.as_bytes()),
        last_update_sha256: p_sha.to_hex(),
        state: VectorRetirementProgressState::Completed,
        created_at: ts.clone(),
        updated_at: ts,
        targets: progress_targets,
    };
    let progress_bytes = encode_vector_maintenance_record(&progress).unwrap();
    let progress_key = format!("{prefix}progress/{}.json", table_catalog_path_hash(&retirement_id.to_string()));
    let output_copy_key = format!("{prefix}sources/{}.json", CatalogContentSha256::digest(&progress_bytes).to_hex());
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &progress_key, progress_bytes.clone())
        .await;
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &output_copy_key, progress_bytes)
        .await;
    let output_copy_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &output_copy_key)
        .await
        .unwrap()
        .unwrap();
    let output_copy_ref: CatalogMetadataRefRecordV1 =
        (&CatalogMetadataRef::from_read(CatalogObjectKey::try_new(output_copy_key).unwrap(), &output_copy_obj).unwrap()).into();

    let result = VectorMaintenanceResultV1::try_for_work(
        &work,
        VectorMaintenanceResultEffect::Retired,
        Vec::new(),
        Vec::new(),
        vec![auth_ref.clone()],
        vec![output_copy_ref],
        None,
    )
    .unwrap();
    let finished_at = result.finished_at.clone();
    let result_bytes = encode_vector_maintenance_record(&result).unwrap();
    let result_key = format!(
        "{prefix}results/{}/attempt-{}/{}.json",
        table_catalog_path_hash(&claimed_job.job_id.to_string()),
        claimed_job.attempt,
        CatalogContentSha256::digest(&result_bytes).to_hex(),
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &result_key, result_bytes)
        .await;
    let result_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &result_key)
        .await
        .unwrap()
        .unwrap();
    let result_ref: CatalogMetadataRefRecordV1 =
        (&CatalogMetadataRef::from_read(CatalogObjectKey::try_new(result_key).unwrap(), &result_obj).unwrap()).into();

    let j_sha = CatalogContentSha256::digest(b"job-finish");
    claimed_job.state = VectorMaintenanceJobState::Succeeded;
    claimed_job.result_ref = Some(result_ref);
    claimed_job.finished_at = Some(finished_at.clone());
    claimed_job.updated_at = finished_at;
    claimed_job.revision = Uuid::new_v4();
    claimed_job.last_update_id = crate::table_catalog::vector_maintenance_model::maintenance_uuid_from_digest(*j_sha.as_bytes());
    claimed_job.last_update_sha256 = j_sha.to_hex();
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &job_path, encode_vector_maintenance_record(&claimed_job).unwrap())
        .await;

    let delete_path = fixture.store.paths.vector_delete_intent_path(storage, &resource.table_id);
    let delete_obj = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &delete_path)
        .await
        .unwrap()
        .unwrap();
    let mut delete_json: serde_json::Value = serde_json::from_slice(&delete_obj.data).unwrap();
    delete_json["maintenance_binding"] = serde_json::json!({
        "version": 1,
        "retirement_count": 1,
        "retirement_tip": auth_ref,
    });
    let updated_delete_bytes = serde_json::to_vec(&delete_json).unwrap();
    serde_json::from_slice::<VectorIndexDeleteIntent>(&updated_delete_bytes)
        .unwrap()
        .validate()
        .unwrap();
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &delete_path, updated_delete_bytes)
        .await;
    drop(work);
    resource
}

#[tokio::test]
async fn catalog_retirement_multi_resource_multipage_scales_linearly_and_handles_recreated_anchor() {
    let unretired = unretired_bucket_fixture().await;
    // Five logically deleted indexes, including a recreated "pagination.anchor" dual-resource.
    // Each index produces > 400 proof facts (>= 2 proof pages) and ~114 Catalog source records
    // (~572 total Catalog targets -> 3 plan pages and 3 progress pages).
    let mut resources = Vec::new();
    for name in [
        "pagination.anchor",
        "pagination.anchor",
        "alpha.index",
        "beta.index",
        "gamma.index",
    ] {
        resources.push(seed_retired_index_with_multipage_history(&unretired, name, 102, 40).await);
    }
    assert_eq!(resources[0].name, "pagination.anchor");
    assert_eq!(resources[1].name, "pagination.anchor");
    assert_ne!(resources[0].table_id, resources[1].table_id);

    let fixture = unretired.into_locked().await;
    fixture.backend.state.lock().await.read_attempts.clear();

    let execution = CatalogCleanupExecution::new(Uuid::new_v4()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        fixture
            .store
            .prepare_vector_bucket_catalog_retirement_under_writer(&fixture.bucket, &execution, &fixture.fence),
    )
    .await
    .expect("linear multi-resource retirement completes well within deadline")
    .unwrap();

    let authority = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    let target_count = authority.record.target_count;
    assert_eq!(authority.record.resource_count, 5);
    assert!(authority.record.plan_page_count >= 3);
    assert!(target_count > 550);

    let validated = fixture
        .store
        .validate_bucket_retirement_authority(authority, &fixture.fence)
        .await
        .unwrap();
    for resource in &resources {
        let descriptor = validated
            .plan
            .resources_by_key
            .get(&resource_key(resource))
            .expect("every retired index has an exact proof descriptor");
        assert!(descriptor.proof_page_count >= 2);
        assert!(descriptor.fact_count > 256);
    }
    assert!(
        fixture
            .store
            .collect_bucket_catalog_sources(&fixture.bucket, &fixture.fence)
            .await
            .unwrap()
            .is_empty()
    );

    let source_prefix = fixture
        .store
        .paths
        .table_bucket_root_prefix(fixture.bucket.storage_bucket_id().as_str());
    let state = fixture.backend.state.lock().await;
    let total_reads: usize = state.read_attempts.values().sum();
    let total_source_reads: usize = state
        .read_attempts
        .iter()
        .filter(|((bucket, key), _)| bucket == RUSTFS_META_BUCKET && key.starts_with(&source_prefix))
        .map(|(_, &count)| count)
        .sum();
    let max_source_reads = state
        .read_attempts
        .iter()
        .filter(|((bucket, key), _)| bucket == RUSTFS_META_BUCKET && key.starts_with(&source_prefix))
        .map(|(_, &count)| count)
        .max()
        .unwrap_or(0);
    // Including the post-completion validate_bucket_retirement_authority call above,
    // every source object was read at most 3 times (initial scan, pre-authority check, pre-delete check).
    let target_count_usize = usize::try_from(target_count).unwrap();
    assert!(
        max_source_reads <= 3 && total_source_reads <= target_count_usize * 3,
        "expected <= 3 reads per Catalog source object (<= {}), observed max={max_source_reads}, sum={total_source_reads}",
        target_count_usize * 3,
    );
    // Each target also performs 2 durable progress writes (InFlight + DeleteAcknowledged) * 2 reads
    // (precheck + read-back confirmation in write_bucket_retirement_record) = 4 progress reads/target,
    // giving ~7 * target_count + ~70 page reads total (strictly linear vs > 500,000 quadratic before F-02).
    let linear_ceiling = target_count_usize * 8 + 250;
    assert!(
        total_reads <= linear_ceiling,
        "expected linear total reads <= {linear_ceiling}, observed {total_reads}"
    );
}

#[tokio::test]
async fn catalog_retirement_pre_authority_interruption_cleans_uncommitted_pages_on_retry() {
    let unretired = unretired_bucket_fixture().await;
    for name in ["index.one", "index.two", "index.three"] {
        seed_retired_index_with_multipage_history(&unretired, name, 85, 30).await;
    }
    let fixture = unretired.into_locked().await;

    // Seed a foreign incarnation record for the same storage_bucket_id to prove reincarnation isolation.
    let mut other_binding = VectorBucketBindingV1::from_handle(&fixture.bucket).unwrap();
    other_binding.bucket_incarnation = Uuid::new_v4();
    let other_handle = other_binding.to_handle().unwrap();
    let other_authority_path = fixture
        .store
        .paths
        .vector_bucket_retirement_authority_path(&other_handle)
        .unwrap();
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &other_authority_path, b"retained-other-incarnation".to_vec())
        .await;

    let authority_path = fixture
        .store
        .paths
        .vector_bucket_retirement_authority_path(&fixture.bucket)
        .unwrap();
    let retirement_root = fixture.store.paths.vector_bucket_retirement_root(&fixture.bucket).unwrap();

    // Fail the very first write of authority.json after all proof, plan, and progress pages are persisted.
    fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &authority_path, 1).await;
    let first_attempt = fixture
        .store
        .prepare_vector_bucket_catalog_retirement_under_writer(
            &fixture.bucket,
            &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(),
            &fixture.fence,
        )
        .await;
    assert!(first_attempt.is_err());

    let aborted_pages: Vec<String> = {
        let state = fixture.backend.state.lock().await;
        assert!(
            !state
                .objects
                .contains_key(&(RUSTFS_META_BUCKET.into(), authority_path.clone()))
        );
        state
            .objects
            .keys()
            .filter(|(bucket, key)| bucket == RUSTFS_META_BUCKET && key.starts_with(&retirement_root))
            .map(|(_, key)| key.clone())
            .collect()
    };
    // 3 indexes * >=2 proof pages + >=2 plan pages + >=2 progress pages >= 10 uncommitted staging pages.
    assert!(
        aborted_pages.len() >= 10,
        "expected >= 10 uncommitted pages before authority.json failure, saw {}",
        aborted_pages.len()
    );
    let source_prefix = fixture
        .store
        .paths
        .table_bucket_root_prefix(fixture.bucket.storage_bucket_id().as_str());
    {
        let state = fixture.backend.state.lock().await;
        let deleted_sources: usize = state
            .delete_attempts
            .iter()
            .filter(|((bucket, key), _)| bucket == RUSTFS_META_BUCKET && key.starts_with(&source_prefix))
            .map(|(_, &count)| count)
            .sum();
        assert_eq!(deleted_sources, 0, "no Catalog source may be deleted before authority.json");
    }

    // Retry succeeds cleanly and sweeps every uncommitted page from the aborted attempt.
    fixture
        .store
        .prepare_vector_bucket_catalog_retirement_under_writer(
            &fixture.bucket,
            &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(),
            &fixture.fence,
        )
        .await
        .unwrap();

    let committed = fixture
        .store
        .read_bucket_retirement_authority(&fixture.bucket, &fixture.fence)
        .await
        .unwrap()
        .unwrap();
    let committed_prefix = format!("{retirement_root}{}/", committed.record.retirement_id);
    let remaining_retirement_keys: Vec<String> = {
        let state = fixture.backend.state.lock().await;
        assert!(
            state.objects.contains_key(&(RUSTFS_META_BUCKET.into(), other_authority_path)),
            "other incarnation records must remain untouched"
        );
        state
            .objects
            .keys()
            .filter(|(bucket, key)| bucket == RUSTFS_META_BUCKET && key.starts_with(&retirement_root))
            .map(|(_, key)| key.clone())
            .collect()
    };
    for aborted in &aborted_pages {
        assert!(
            !remaining_retirement_keys.contains(aborted),
            "orphan page from aborted pre-authority attempt leaked: {aborted}"
        );
    }
    for key in &remaining_retirement_keys {
        assert!(
            key == &authority_path || key.starts_with(&committed_prefix),
            "unexpected object under retirement root: {key}"
        );
    }
    assert!(
        fixture
            .store
            .collect_bucket_catalog_sources(&fixture.bucket, &fixture.fence)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn catalog_retirement_rejects_omitted_committed_manifest_and_tampered_delete_source_copy() {
    // Case A: omitting a committed manifest from evidence.history_refs is rejected even with a complete job/progress chain.
    let unretired = unretired_bucket_fixture().await;
    seed_retired_index_with_multipage_history_opts(&unretired, "omitted.manifest", 3, 1, true).await;
    let fixture = unretired.into_locked().await;
    let err = fixture
        .store
        .prepare_vector_bucket_catalog_retirement_under_writer(
            &fixture.bucket,
            &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(),
            &fixture.fence,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, TableCatalogStoreError::Conflict(msg) if msg == "retirement proof omits a committed manifest"),
        "unexpected error for omitted manifest: {err:?}"
    );

    // Case B: divergence between live delete-intent and immutable deletion-intent source copy is rejected.
    let unretired = unretired_bucket_fixture().await;
    let resource = seed_retired_index_with_multipage_history(&unretired, "tampered.intent", 2, 1).await;
    let delete_path = unretired
        .store
        .paths
        .vector_delete_intent_path(unretired.bucket.storage_bucket_id().as_str(), &resource.table_id);
    let delete_obj = unretired
        .backend
        .read_object(RUSTFS_META_BUCKET, &delete_path)
        .await
        .unwrap()
        .unwrap();
    let mut present: VectorIndexDeleteIntent = serde_json::from_slice(&delete_obj.data).unwrap();
    present.phase = VectorIndexDeletePhase::Complete;
    present.validate().unwrap();
    unretired
        .backend
        .seed_object(RUSTFS_META_BUCKET, &delete_path, serde_json::to_vec(&present).unwrap())
        .await;

    let fixture = unretired.into_locked().await;
    let err = fixture
        .store
        .prepare_vector_bucket_catalog_retirement_under_writer(
            &fixture.bucket,
            &CatalogCleanupExecution::new(Uuid::new_v4()).unwrap(),
            &fixture.fence,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, TableCatalogStoreError::Conflict(msg) if msg == "retirement evidence belongs to another deletion transaction"),
        "unexpected error for tampered deletion source copy: {err:?}"
    );
}
