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

async fn retirement_fixture() -> Fixture {
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
    let lifecycle = backend
        .acquire_write_lock(bucket.storage_bucket_id().as_str(), "")
        .await
        .unwrap();
    let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal()]);
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
    Fixture {
        backend,
        store,
        bucket,
        parent,
        namespace,
        _lifecycle: lifecycle,
        fence,
    }
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
