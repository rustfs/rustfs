// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::table_catalog::tests::prepared_commit::run_native_vector_delete_test;

#[test]
#[serial_test::serial]
fn catalog_retirement_used_bucket_retries_production_entry_after_catalog_completion() {
    run_native_vector_delete_test("catalog-retirement-production-retry", || async {
        let (store, resource, _fixture) = evidence::tests::completed_native_resource().await;
        let bucket = resource.bucket.to_handle().unwrap();
        let physical = store.backend.store.clone();
        let instance = physical.id;
        let output = Arc::new(std::sync::Mutex::new(None));
        let callback_output = output.clone();
        let original = store.clone();
        let captured = bucket.clone();
        let first = physical
            .delete_managed_bucket_with_preparation(&bucket, move |writer| async move {
                let result = original
                    .prepare_vector_bucket_delete(&captured, writer, CatalogCleanupExecution::new(instance).unwrap())
                    .await;
                *callback_output.lock().unwrap() = Some(result);
                Err(crate::storage_api::table::StorageError::other(std::io::Error::other(
                    "injected interruption after successful Catalog preparation",
                )))
            })
            .await;
        assert!(first.is_err());
        output
            .lock()
            .unwrap()
            .take()
            .expect("actual native callback was entered")
            .unwrap();
        let fence = TableCatalogObjectMutationFence::default();
        let original_authority = store
            .read_bucket_retirement_authority(&bucket, &fence)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original_authority.record.resource_count, 1);
        assert!(original_authority.record.target_count > 2);
        assert!(
            store
                .next_bucket_catalog_source(&bucket, None, &fence)
                .await
                .unwrap()
                .is_none()
        );

        // The same application factory must reach original authority with both
        // registrations already removed and the physical identity still present.
        let resumed = ObjectTableCatalogStore::for_vector_scope(store.backend.clone(), bucket.clone())
            .await
            .unwrap();
        let observed = resumed
            .read_bucket_retirement_authority(&bucket, &fence)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.record, original_authority.record);
        assert_eq!(observed.reference, original_authority.reference);
        let validated = resumed.validate_bucket_retirement_authority(observed, &fence).await.unwrap();
        let mut original_progress = std::collections::BTreeMap::new();
        let mut cursor = None;
        while let Some(target) = resumed
            .next_bucket_retirement_target(&validated.authority, cursor.as_deref(), &fence)
            .await
            .unwrap()
        {
            let page: VectorBucketCatalogRetirementPlanPageV1 = resumed
                .read_bucket_retirement_record(&target.plan_page, &fence)
                .await
                .unwrap();
            let path = resumed
                .paths
                .vector_bucket_retirement_page_path(&bucket, validated.authority.retirement_id, "progress", page.page_id)
                .unwrap();
            let object = resumed
                .backend
                .read_object_limited(resumed.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                .await
                .unwrap()
                .unwrap();
            let progress: VectorBucketCatalogRetirementProgressPageV1 = decode_vector_maintenance_record(&object.data).unwrap();
            assert!(
                progress
                    .targets
                    .iter()
                    .all(|target| target.state == VectorCatalogRetirementTargetState::DeleteAcknowledged)
            );
            original_progress.insert(path, object);
            cursor = Some(target.target.object_key);
        }
        let callback_store = resumed.clone();
        let captured = bucket.clone();
        physical
            .delete_managed_bucket_with_preparation(&bucket, move |writer| async move {
                callback_store
                    .prepare_vector_bucket_delete(&captured, writer, CatalogCleanupExecution::new(instance).unwrap())
                    .await
                    .map_err(crate::storage_api::table::StorageError::other)
            })
            .await
            .unwrap();

        // Retained evidence is read by captured exact reference after physical deletion.
        let authority: VectorBucketCatalogRetirementV1 = store
            .read_bucket_retirement_record(&(&original_authority.reference).into(), &fence)
            .await
            .unwrap();
        assert_eq!(authority, original_authority.record);
        for (path, original) in original_progress {
            let observed = store
                .backend
                .read_object_limited(store.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(observed, original, "ACK progress identity must survive the new executor");
        }
        store
            .visit_bucket_retirement_plan(&authority, &fence, |_, page| {
                assert!(
                    page.targets
                        .iter()
                        .all(|target| target.resource.as_ref().is_none_or(|owner| owner == &resource))
                );
                Ok(())
            })
            .await
            .unwrap();
        let descriptor = store
            .bucket_retirement_resource_by_identity(&authority, &resource, &fence)
            .await
            .unwrap();
        store
            .visit_bucket_retirement_proof(&authority, &descriptor, &fence, |_| Ok(()))
            .await
            .unwrap();
    });
}

type NativeStore = ObjectTableCatalogStore<
    EcStoreTableCatalogObjectBackend<crate::table_catalog::tests::prepared_commit::NativeVectorParentStorage>,
>;

async fn prepare_with_actual_bucket_writer(store: &NativeStore, bucket: &BucketHandle) -> TableCatalogStoreResult<()> {
    let physical = store.backend.store.clone();
    let instance = physical.id;
    let output = Arc::new(std::sync::Mutex::new(None));
    let captured_output = output.clone();
    let captured_store = store.clone();
    let captured_bucket = bucket.clone();
    let result = physical
        .delete_managed_bucket_with_preparation(bucket, move |writer| async move {
            let result = captured_store
                .prepare_vector_bucket_delete(&captured_bucket, writer, CatalogCleanupExecution::new(instance).unwrap())
                .await;
            *captured_output.lock().unwrap() = Some(result);
            Err(crate::storage_api::table::StorageError::other(std::io::Error::other(
                "retain physical fixture after Catalog callback",
            )))
        })
        .await;
    assert!(result.is_err());
    output
        .lock()
        .unwrap()
        .take()
        .expect("actual ECStore writer invoked the preparation callback")
}

fn interrupt_one_confirmed_target(
    bucket: VectorBucketBindingV1,
    phase: CatalogRetirementObservedPhase,
) -> (
    CatalogRetirementObservationHandle,
    Arc<parking_lot::Mutex<Option<CatalogRetirementObservation>>>,
) {
    let observed = Arc::new(parking_lot::Mutex::new(None));
    let output = observed.clone();
    let registration = observe_catalog_retirement(
        bucket,
        Arc::new(move |event| {
            let selected = event.phase == phase
                && event.target.as_ref().is_some_and(|target| {
                    matches!(target.fenced_delete, Some(CatalogRetirementObservedOutcome::Succeeded))
                        && matches!(target.subsequent_fence, Some(CatalogRetirementObservedOutcome::Succeeded))
                        && (phase != CatalogRetirementObservedPhase::TargetProgressPublished
                            || matches!(target.progress_publish, Some(CatalogRetirementObservedOutcome::Succeeded))
                                && target.progress.targets[usize::from(target.ordinal)].state
                                    == VectorCatalogRetirementTargetState::DeleteAcknowledged)
                });
            let interrupt = if selected {
                let mut observed = output.lock();
                if observed.is_none() {
                    *observed = Some(event);
                    true
                } else {
                    false
                }
            } else {
                false
            };
            Box::pin(async move {
                if interrupt {
                    Err(TableCatalogStoreError::Unavailable("injected original cleanup owner interruption".into()))
                } else {
                    Ok(())
                }
            })
        }),
    )
    .unwrap();
    (registration, observed)
}

#[test]
#[serial_test::serial]
fn catalog_retirement_native_success_without_durable_ack_blocks_production_retry() {
    run_native_vector_delete_test("catalog-retirement-native-unknown", || async {
        let (store, resource, _fixture) = evidence::tests::completed_native_resource().await;
        let bucket = resource.bucket.to_handle().unwrap();
        let (registration, observed) =
            interrupt_one_confirmed_target(resource.bucket.clone(), CatalogRetirementObservedPhase::TargetNativeReturned);
        let first = prepare_with_actual_bucket_writer(&store, &bucket).await;
        assert!(
            matches!(first, Err(TableCatalogStoreError::Unavailable(message)) if message == "injected original cleanup owner interruption")
        );
        let event = observed
            .lock()
            .clone()
            .expect("actual fenced native delete and final fence completed");
        drop(registration);
        let target = event.target.as_ref().unwrap();
        assert_eq!(
            target.progress.targets[usize::from(target.ordinal)].state,
            VectorCatalogRetirementTargetState::InFlight
        );
        assert!(target.progress.targets[usize::from(target.ordinal)].acknowledged_at.is_none());
        assert!(
            store
                .backend
                .read_object_limited(store.catalog_bucket(), &target.target.object_key, VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES)
                .await
                .unwrap()
                .is_none()
        );
        let original_progress = store
            .backend
            .read_object_limited(
                store.catalog_bucket(),
                &target.progress_ref.object_key,
                VECTOR_RETIREMENT_PROGRESS_MAX_BYTES,
            )
            .await
            .unwrap()
            .unwrap();
        let again = ObjectTableCatalogStore::for_vector_scope(store.backend.clone(), bucket.clone())
            .await
            .unwrap();
        let retry = prepare_with_actual_bucket_writer(&again, &bucket).await;
        assert!(
            matches!(retry, Err(TableCatalogStoreError::Unavailable(message)) if message.contains("unresolved native outcome"))
        );
        let retained = store
            .backend
            .read_object_limited(
                store.catalog_bucket(),
                &target.progress_ref.object_key,
                VECTOR_RETIREMENT_PROGRESS_MAX_BYTES,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained, original_progress);
        let fence = TableCatalogObjectMutationFence::default();
        let authority = store
            .read_bucket_retirement_authority(&bucket, &fence)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authority.record, *event.authority);
        assert_eq!(CatalogMetadataRefRecordV1::from(&authority.reference), event.authority_ref);
    });
}

#[test]
#[serial_test::serial]
fn catalog_retirement_native_ack_resumes_production_entry_with_original_proof() {
    run_native_vector_delete_test("catalog-retirement-native-ack", || async {
        let (store, resource, _fixture) = evidence::tests::completed_native_resource().await;
        let bucket = resource.bucket.to_handle().unwrap();
        let (registration, observed) =
            interrupt_one_confirmed_target(resource.bucket.clone(), CatalogRetirementObservedPhase::TargetProgressPublished);
        let first = prepare_with_actual_bucket_writer(&store, &bucket).await;
        assert!(
            matches!(first, Err(TableCatalogStoreError::Unavailable(message)) if message == "injected original cleanup owner interruption")
        );
        let event = observed
            .lock()
            .clone()
            .expect("real native success and durable ACK were observed");
        drop(registration);
        let target = event.target.as_ref().unwrap();
        let original_ack = target.progress.targets[usize::from(target.ordinal)].clone();
        assert_eq!(original_ack.state, VectorCatalogRetirementTargetState::DeleteAcknowledged);
        assert!(
            store
                .backend
                .read_object_limited(store.catalog_bucket(), &target.target.object_key, VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES)
                .await
                .unwrap()
                .is_none()
        );
        let again = ObjectTableCatalogStore::for_vector_scope(store.backend.clone(), bucket.clone())
            .await
            .unwrap();
        prepare_with_actual_bucket_writer(&again, &bucket).await.unwrap();
        let object = store
            .backend
            .read_object_limited(
                store.catalog_bucket(),
                &target.progress_ref.object_key,
                VECTOR_RETIREMENT_PROGRESS_MAX_BYTES,
            )
            .await
            .unwrap()
            .unwrap();
        let progress: VectorBucketCatalogRetirementProgressPageV1 = decode_vector_maintenance_record(&object.data).unwrap();
        assert_eq!(progress.targets[usize::from(target.ordinal)], original_ack);
        let fence = TableCatalogObjectMutationFence::default();
        let authority = store
            .read_bucket_retirement_authority(&bucket, &fence)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authority.record, *event.authority);
        assert_eq!(CatalogMetadataRefRecordV1::from(&authority.reference), event.authority_ref);
        let descriptor = store
            .bucket_retirement_resource_by_identity(&authority.record, &resource, &fence)
            .await
            .unwrap();
        store
            .visit_bucket_retirement_proof(&authority.record, &descriptor, &fence, |_| Ok(()))
            .await
            .unwrap();
        assert!(
            store
                .next_bucket_catalog_source(&bucket, None, &fence)
                .await
                .unwrap()
                .is_none()
        );
    });
}
