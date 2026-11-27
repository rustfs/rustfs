// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::table::managed::BucketIncarnation;
use crate::table_catalog::tests::prepared_commit::native_vector_parent_fixture;
use std::collections::BTreeMap;

#[tokio::test]
#[serial_test::serial]
async fn real_catalog_backend_reuses_native_guard_for_vector_parent_admission() {
    let (fixture, _reset) = native_vector_parent_fixture().await;
    let backend = RealCatalogBackend::new(fixture.backend.clone());
    let owners = Arc::strong_count(&fixture.guard);
    let reused = backend
        .reuse_bucket_lifecycle_read(&fixture.bucket, fixture.guard.clone())
        .await
        .expect("fault backend reuses the actual native lifecycle owner");
    assert!(std::ptr::eq(
        reused.bucket_lifecycle_guard().expect("reused native guard"),
        fixture.guard.as_ref()
    ));
    assert_eq!(Arc::strong_count(&fixture.guard), owners + 1);
    assert!(!reused.is_lock_lost());
    drop(reused);
    assert_eq!(Arc::strong_count(&fixture.guard), owners);

    let catalog = ObjectTableCatalogStore::for_vector_scope(backend.clone(), fixture.bucket.clone())
        .await
        .expect("real wrapped Vector scope");
    let storage_name = fixture.bucket.storage_bucket_id().as_str();
    let path = catalog.paths.table_bucket_entry_path(storage_name);
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .expect("read exact parent before admission")
            .is_none()
    );
    let entry = TableBucketEntry {
        vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(&fixture.bucket).expect("complete bucket identity")),
        version: 2,
        table_bucket: storage_name.into(),
        catalog_type: "s3vectors".into(),
        warehouse_root: format!("s3://{storage_name}/"),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        active_rename_id: None,
        created_at: None,
        updated_at: None,
    };
    catalog
        .put_table_bucket(entry.clone())
        .await
        .expect("worker create's actual parent admission persists through the fault backend");
    let stored = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &path)
        .await
        .expect("native parent read")
        .expect("native parent exists");
    assert_eq!(serde_json::from_slice::<TableBucketEntry>(&stored.data).expect("stored parent"), entry);
    assert!(stored.body_revision.is_some());
    assert!(stored.etag.is_some());
    assert_eq!(Arc::strong_count(&fixture.guard), owners);
    fixture
        .storage
        .validate_managed_bucket_fence(&fixture.guard, storage_name)
        .expect("original lifecycle owner remains held after admission");
}

#[tokio::test]
#[serial_test::serial]
async fn real_catalog_backend_preserves_native_shared_guard_identity_rejections() {
    let (fixture, _reset) = native_vector_parent_fixture().await;
    let changed_incarnation = BucketHandle::try_from_parts(
        fixture.bucket.kind(),
        fixture.bucket.account_namespace_id().clone(),
        fixture.bucket.region().into(),
        fixture.bucket.public_name().clone(),
        fixture.bucket.storage_bucket_id().clone(),
        BucketIncarnation::try_new(Uuid::new_v4()).expect("different incarnation"),
    )
    .expect("same bucket name with a distinct valid incarnation");
    let path = fixture
        .catalog
        .paths
        .table_bucket_entry_path(fixture.bucket.storage_bucket_id().as_str());
    let owners = Arc::strong_count(&fixture.guard);
    let native = fixture.backend.clone();
    assert!(
        native
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .expect("native parent before rejected reuse")
            .is_none()
    );
    let direct_error = match native
        .reuse_bucket_lifecycle_read(&changed_incarnation, fixture.guard.clone())
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("native backend must reject the mismatched identity"),
    };
    assert!(matches!(&direct_error, TableCatalogStoreError::Internal(message)
        if message.contains("validate managed bucket identity") && message.contains("bucket identity changed")));
    let backend = RealCatalogBackend::new(native.clone());
    let wrapped_error = match backend
        .reuse_bucket_lifecycle_read(&changed_incarnation, fixture.guard.clone())
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("fault backend must retain the native identity rejection"),
    };
    assert_eq!(std::mem::discriminant(&wrapped_error), std::mem::discriminant(&direct_error));
    assert_eq!(wrapped_error.to_string(), direct_error.to_string());
    assert!(backend.events().is_empty());
    assert!(
        native
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .expect("native parent after rejected reuse")
            .is_none()
    );
    assert_eq!(Arc::strong_count(&fixture.guard), owners);
    fixture
        .storage
        .validate_managed_bucket_fence(&fixture.guard, fixture.bucket.storage_bucket_id().as_str())
        .expect("rejected reuse preserves the original valid owner");
}
