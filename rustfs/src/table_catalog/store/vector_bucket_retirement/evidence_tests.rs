// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn exact_reference(key: &str) -> Ref {
    Ref {
        object_key: key.into(),
        etag: Some("actual-etag".into()),
        body_revision: Some(Uuid::new_v4()),
        content_sha256: "a".repeat(64),
        content_length: 7,
    }
}

#[test]
fn retirement_edge_preserves_original_optional_etag_and_exact_identity() {
    let actual = exact_reference("s3vectors/catalog/source.json");
    let mut edge = actual.clone();
    edge.etag = None;
    assert!(covers_reference(&actual, &edge));
    edge.content_sha256 = "b".repeat(64);
    assert!(!covers_reference(&actual, &edge));
    edge = actual.clone();
    edge.body_revision = Some(Uuid::new_v4());
    assert!(!covers_reference(&actual, &edge));
    edge = actual.clone();
    edge.etag = Some("another-etag".into());
    assert!(!covers_reference(&actual, &edge));
}

#[test]
fn retirement_source_reader_rejects_missing_native_identity() {
    let object = TableCatalogObject {
        data: b"{}".to_vec(),
        etag: None,
        body_revision: None,
        mod_time: None,
    };
    assert!(exact_source("source.json", &object).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_missing_binding_preserves_lost_fence_error() {
    use crate::table_catalog::test_support::RecordingLeaseClient;
    use rustfs_lock::LockClient;
    use std::sync::{Arc, Mutex};
    use std::time::Duration as StdDuration;

    let fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
    let namespace = Namespace::parse(&fixture.entry.namespace).unwrap();
    let name = CatalogName::parse(&fixture.entry.table).unwrap();
    let (operation, entry) = fixture
        .store
        .begin_vector_operation(&fixture.bucket, &namespace, &name)
        .await
        .unwrap();
    let resource = entry.head().resource_identity().clone();
    fixture
        .store
        .delete_vector_index_with_guard(&operation, &entry, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
        .await
        .unwrap();
    drop(operation);
    let current = fixture
        .store
        .read_vector_maintenance_resource(&resource, &namespace, &name)
        .await
        .unwrap();
    assert_eq!(current.state, VectorMaintenanceResourceState::Deleted);
    assert!(current.maintenance.is_none());

    let client = Arc::new(RecordingLeaseClient {
        inner: rustfs_lock::LocalClient::with_manager(Arc::new(rustfs_lock::GlobalLockManager::Enabled(Arc::new(
            rustfs_lock::FastObjectLockManager::new(),
        )))),
        acquired: Mutex::new(None),
    });
    let lock = rustfs_lock::NamespaceLock::with_clients_and_quorum(
        "catalog-retirement-missing-binding-fence".into(),
        vec![client.clone() as Arc<dyn LockClient>],
        1,
    );
    let guard = lock
        .lock_guard(
            rustfs_lock::ObjectKey::new(fixture.bucket.storage_bucket_id().as_str(), "retirement-proof.lock"),
            "retirement-proof-test",
            StdDuration::from_secs(5),
            StdDuration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    let signal = guard.lock_lost_signal().unwrap();
    let fence = TableCatalogObjectMutationFence::from_signals([Some(signal.clone())]);
    let before = {
        let state = fixture.backend.state.lock().await;
        (state.objects.clone(), state.put_attempts.clone(), state.delete_attempts.clone())
    };
    assert!(!signal.is_lost());
    assert!(matches!(
        fixture.store.bucket_verify_deleted_data(&current, &fence).await,
        Err(TableCatalogStoreError::Conflict(message)) if message == "Deleted resource lacks retirement binding"
    ));

    let lease = client.acquired.lock().unwrap().clone().unwrap();
    assert!(client.force_release(&lease).await.unwrap());
    tokio::time::timeout(StdDuration::from_secs(10), signal.notified())
        .await
        .expect("heartbeat observes the released lease");
    assert!(signal.is_lost());
    assert!(current.maintenance.is_none());
    assert!(matches!(
        fixture.store.bucket_verify_deleted_data(&current, &fence).await,
        Err(TableCatalogStoreError::Unavailable(message)) if message == "catalog backup or restore lost its fencing lock"
    ));
    let state = fixture.backend.state.lock().await;
    assert_eq!(state.objects, before.0);
    assert_eq!(state.put_attempts, before.1);
    assert_eq!(state.delete_attempts, before.2);
    drop(state);
    drop(guard);
}

use crate::table_catalog::tests::prepared_commit::{
    NativeVectorParentStorage, native_vector_index_fixture_at, run_native_vector_delete_test,
};

pub(in crate::table_catalog::store) type NativeProofStore =
    ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<NativeVectorParentStorage>>;

/// Builds eligibility through original Catalog operations and actual native delete accounting.
pub(in crate::table_catalog::store) async fn completed_native_resource()
-> (NativeProofStore, VectorMaintenanceResourceV1, impl Send) {
    let (store, original, fixture) = native_vector_index_fixture_at(OffsetDateTime::now_utc() - Duration::seconds(1800)).await;
    let entry = VectorCatalogEntry::from_entry(original).unwrap();
    let identity = entry.head().resource_identity().clone();
    let entry = store
        .enroll_vector_maintenance(
            &entry,
            vector_maintenance_enroll_commit_id(&identity, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let namespace = entry.head().namespace().clone();
    let name = entry.head().name().clone();
    let resource = VectorMaintenanceResourceV1::try_new(&identity, &namespace, &name).unwrap();
    let (operation, _) = store
        .begin_vector_operation(identity.vector_bucket().unwrap(), &namespace, &name)
        .await
        .unwrap();
    store
        .delete_vector_index_with_guard(&operation, &entry, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
        .await
        .unwrap();
    drop(operation);
    let snapshot = store
        .read_vector_maintenance_resource(&identity, &namespace, &name)
        .await
        .unwrap();
    let operation = store.begin_vector_maintenance_resource_operation(&snapshot).await.unwrap();
    let source = store
        .copy_vector_maintenance_source_with_operation_guard(&operation, snapshot.source_intent().unwrap())
        .await
        .unwrap();
    let evidence = VectorRetirementEvidenceV1::try_new(
        resource.clone(),
        snapshot.head.clone().unwrap(),
        Some((&source).into()),
        vec![entry.head().metadata().into()],
        Vec::new(),
        Vec::new(),
        VectorHistoryRetentionV1::All,
    )
    .unwrap();
    let key = entry.head().metadata().object_key().as_str();
    let metadata = store
        .backend
        .object_metadata(identity.storage_bucket_id(), key)
        .await
        .unwrap()
        .unwrap();
    let target =
        VectorRetirementTargetV1::from_object_metadata(&identity, entry.head().metadata().object_key().clone(), &metadata)
            .unwrap();
    let prepared = store
        .prepare_vector_retirement(&operation, evidence, vec![target])
        .await
        .unwrap();
    let spec = VectorMaintenanceJobSpec::from_semantic(
        VectorMaintenanceSemanticSpec::try_new(
            resource.clone(),
            VectorMaintenanceKind::HistoryGc,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(snapshot.head.clone(), None, Some(prepared.plan_ref().into())).unwrap(),
            VectorMaintenanceExecutionIdentityV1::try_new("retirement-proof-test".into(), [8; 16]).unwrap(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .unwrap(),
    )
    .unwrap();
    drop(operation);
    let created = store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    let observed = store
        .read_vector_maintenance_resource(&identity, &namespace, &name)
        .await
        .unwrap();
    let condition = VectorMaintenanceClaimCondition::from_snapshot(&created, observed);
    let worker =
        VectorMaintenanceWorker::try_new("retirement-proof-worker".into(), Uuid::new_v4(), spec.execution_identity).unwrap();
    let VectorMaintenanceClaimOutcome::Acquired(work) = store
        .claim_vector_maintenance_job(&identity, spec.job_id, condition, worker, Uuid::new_v4())
        .await
        .unwrap()
    else {
        panic!("actual maintenance claim");
    };
    let dispatch = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
    let authorization = store
        .publish_vector_retirement(
            &dispatch,
            prepared,
            Uuid::new_v4(),
            Uuid::new_v4(),
            CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap(),
        )
        .await
        .unwrap();
    let progress = store
        .read_vector_retirement_progress(&dispatch, &authorization)
        .await
        .unwrap();
    let dispatch_id = Uuid::new_v4();
    let VectorRetirementDispatchAdmission::Ready(permit) = store
        .begin_vector_retirement_delete(&dispatch, &authorization, 0, progress.revision, dispatch_id)
        .await
        .unwrap()
    else {
        panic!("actual retirement admission");
    };
    let before = store
        .backend
        .store
        .get_object_info(identity.storage_bucket_id(), key, &ObjectOptions::default())
        .await
        .unwrap();
    let mut options = ObjectOptions {
        expected_bucket_incarnation_id: Some(resource.bucket.to_handle().unwrap().incarnation().as_uuid()),
        delete_mutation_owner: Some(dispatch.retained_owner()),
        ..Default::default()
    };
    options.add_namespace_lock_guard(&dispatch.dispatch_owner());
    let (_, errors, receipts) = store
        .backend
        .store
        .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
            identity.storage_bucket_id(),
            vec![
                ObjectToDelete {
                    object_name: key.into(),
                    ..Default::default()
                }
                .with_expected_identity(before.data_dir, before.mod_time, false),
            ],
            options,
            permit.lifecycle_owner(),
        )
        .await;
    assert!(errors.iter().all(Option::is_none), "actual native delete: {errors:?}");
    let receipt = receipts
        .into_iter()
        .next()
        .flatten()
        .expect("actual native accounting receipt");
    dispatch.check_held().unwrap();
    let progress = store
        .read_vector_retirement_progress(&dispatch, &authorization)
        .await
        .unwrap();
    let completed = store
        .record_vector_retirement_delete(
            &dispatch,
            &authorization,
            0,
            dispatch_id,
            progress.revision,
            VectorRetirementDeleteObservation::Committed(
                VectorDeleteAccountingV1::try_new(receipt.size, receipt.version_id, receipt.removed_current_object).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(completed.state, VectorRetirementProgressState::Completed);
    let output = store
        .copy_vector_retirement_completed_progress(&dispatch, &authorization)
        .await
        .unwrap();
    let result = VectorMaintenanceResultV1::try_for_work(
        &work,
        VectorMaintenanceResultEffect::Retired,
        Vec::new(),
        Vec::new(),
        vec![(&authorization).into()],
        vec![(&output).into()],
        None,
    )
    .unwrap();
    let latest = store.read_vector_maintenance_job_with_guard(&dispatch).await.unwrap();
    store
        .finish_vector_maintenance_job(
            &dispatch,
            latest.job.revision,
            VectorMaintenanceExecutionOutcome::Completed {
                result: Box::new(result),
            },
        )
        .await
        .unwrap();
    assert!(
        store
            .backend
            .object_metadata(identity.storage_bucket_id(), key)
            .await
            .unwrap()
            .is_none()
    );
    drop(permit);
    drop(dispatch);
    drop(work);
    (store, resource, fixture)
}

#[test]
#[serial_test::serial]
fn retirement_proof_requires_actual_completed_native_receipts() {
    run_native_vector_delete_test("catalog-retirement-proof-native", || async {
        let (store, resource, _fixture) = completed_native_resource().await;
        let bucket = resource.bucket.to_handle().unwrap();
        let lifecycle = store.backend.acquire_bucket_lifecycle_read(&bucket).await.unwrap();
        let gc = store
            .backend
            .acquire_write_lock(
                store.catalog_bucket(),
                &store
                    .paths
                    .vector_gc_lock_path(bucket.storage_bucket_id().as_str(), &resource.table_id),
            )
            .await
            .unwrap();
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal(), gc.lock_lost_signal()]);
        let descriptor = store
            .prepare_bucket_retirement_resource_proof(&bucket, &resource, Uuid::new_v4(), &fence)
            .await
            .unwrap();
        assert!(descriptor.fact_count > 1);
        assert!(descriptor.proof_page_count > 0);
        let page: VectorCatalogRetiredResourceProofPageV1 = store
            .read_bucket_retirement_record(&descriptor.proof_root, &fence)
            .await
            .unwrap();
        assert!(matches!(page.facts.first(), Some(Fact::ResourceAnchor { .. })));
        let source_prefix = store.paths.table_bucket_root_prefix(bucket.storage_bucket_id().as_str());
        assert!(!descriptor.proof_root.object_key.starts_with(&source_prefix));
        let current = store.bucket_deleted_snapshot(&resource).await.unwrap();
        let auth_ref = current.maintenance.as_ref().unwrap().retirement_tip.as_ref().unwrap();
        let auth_object = store.bucket_read_source(auth_ref, &fence).await.unwrap();
        let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&auth_object.data).unwrap();
        let (progress_ref, _) = store
            .bucket_completed_retirement(&current, auth_ref, &auth, &fence)
            .await
            .unwrap();
        store
            .backend
            .delete_object(store.catalog_bucket(), &progress_ref.object_key)
            .await
            .unwrap();
        let missing = store
            .prepare_bucket_retirement_resource_proof(&bucket, &resource, Uuid::new_v4(), &fence)
            .await;
        assert!(matches!(missing, Err(TableCatalogStoreError::Conflict(message)) if message.contains("progress is missing")));
    });
}

#[test]
#[serial_test::serial]
fn retirement_source_rejects_active_resource_and_wrong_incarnation() {
    run_native_vector_delete_test("catalog-retirement-source-identity", || async {
        let (store, entry, _fixture) = native_vector_index_fixture_at(OffsetDateTime::now_utc() - Duration::seconds(1800)).await;
        let resource = CatalogHead::from_vector_entry(&entry).unwrap().resource_identity().clone();
        let bucket = resource.vector_bucket().unwrap();
        let namespace = Namespace::parse(&entry.namespace).unwrap();
        let name = CatalogName::parse(&entry.table).unwrap();
        let path = store
            .paths
            .table_entry_path(bucket.storage_bucket_id().as_str(), &namespace, &name);
        let object = store
            .backend
            .read_object(store.catalog_bucket(), &path)
            .await
            .unwrap()
            .unwrap();
        let lifecycle = store.backend.acquire_bucket_lifecycle_read(bucket).await.unwrap();
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal()]);
        assert!(matches!(store.classify_bucket_retirement_source(bucket, &path, &object, &fence).await,
            Err(TableCatalogStoreError::Conflict(message)) if message.contains("completed logical deletion")));
        let mut binding = VectorBucketBindingV1::from_handle(bucket).unwrap();
        binding.bucket_incarnation = Uuid::new_v4();
        let other = binding.to_handle().unwrap();
        assert!(
            store
                .classify_bucket_retirement_source(&other, &path, &object, &fence)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .backend
                .read_object(store.catalog_bucket(), &path)
                .await
                .unwrap()
                .unwrap()
                .data,
            object.data
        );
    });
}

#[test]
fn retirement_proof_pages_cover_large_history_with_bounded_buffer() {
    let mut after = None;
    let mut observed = 0_usize;
    let mut pages = 0;
    loop {
        let mut page = Vec::new();
        let mut bytes = 0;
        for ordinal in (0..777).rev() {
            let reference = exact_reference(&format!("s3vectors/catalog/source-{ordinal:04}.json"));
            retain_proof_fact(&mut page, &mut bytes, after.as_ref(), Fact::SourceRecord { reference }).unwrap();
            assert!(page.len() <= PROOF_FACTS_PER_PAGE);
            assert!(bytes <= PROOF_PAGE_BODY_BUDGET);
        }
        if page.is_empty() {
            break;
        }
        for fact in &page {
            let Fact::SourceRecord { reference } = fact else {
                panic!("exact source fact");
            };
            assert_eq!(reference.object_key, format!("s3vectors/catalog/source-{observed:04}.json"));
            observed += 1;
        }
        after = page.last().cloned();
        pages += 1;
    }
    assert_eq!(observed, 777);
    assert_eq!(pages, 4);
}

#[test]
fn retirement_proof_page_rejects_duplicate_and_missing_source_identity() {
    let mut page = Vec::new();
    let mut bytes = 0;
    let reference = exact_reference("s3vectors/catalog/source.json");
    retain_proof_fact(
        &mut page,
        &mut bytes,
        None,
        Fact::SourceRecord {
            reference: reference.clone(),
        },
    )
    .unwrap();
    assert!(
        retain_proof_fact(
            &mut page,
            &mut bytes,
            None,
            Fact::SourceRecord {
                reference: reference.clone()
            }
        )
        .is_err()
    );
    let mut missing = reference;
    missing.etag = None;
    assert!(retain_proof_fact(&mut page, &mut bytes, None, Fact::SourceRecord { reference: missing }).is_err());
    assert_eq!(page.len(), 1);
}

#[tokio::test]
async fn retirement_classification_unknown_source_keeps_original_conflict() {
    let fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
    let path = format!(
        "{}zz/unknown.json",
        fixture
            .store
            .paths
            .table_bucket_root_prefix(fixture.bucket.storage_bucket_id().as_str())
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, b"retained unknown record".to_vec())
        .await;
    let object = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    let fence = TableCatalogObjectMutationFence::from_signals([]);
    assert!(matches!(
        fixture
            .store
            .classify_bucket_retirement_source(&fixture.bucket, &path, &object, &fence)
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    assert_eq!(
        fixture
            .backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .unwrap()
            .unwrap()
            .data,
        object.data
    );
}

async fn altered_native_proof(
    store: &NativeProofStore,
    authority: &VectorBucketCatalogRetirementV1,
    original: &VectorCatalogRetiredResourceProofV1,
    fence: &TableCatalogObjectMutationFence,
    alter: impl FnOnce(&mut Vec<Fact>),
) -> VectorCatalogRetiredResourceProofV1 {
    // This fixture is small; production proof generation retains only bounded pages.
    let mut facts = Vec::new();
    store
        .visit_bucket_retirement_proof(authority, original, fence, |fact| {
            facts.push(fact.clone());
            Ok(())
        })
        .await
        .unwrap();
    assert!(facts.len() < PROOF_FACTS_PER_PAGE);
    alter(&mut facts);
    facts.sort_by(Fact::compare_order);
    let proof_id = Uuid::new_v4();
    let page_id = Uuid::new_v4();
    let fact_count = u64::try_from(facts.len()).unwrap();
    let mut digest =
        CatalogRetirementProofDigest::new(&authority.bucket, &original.resource, authority.retirement_id, proof_id, fact_count)
            .unwrap();
    for fact in &facts {
        digest.push(fact).unwrap();
    }
    let proof_sha256 = digest.finish().unwrap();
    let page = VectorCatalogRetiredResourceProofPageV1 {
        version: 1,
        bucket: authority.bucket.clone(),
        retirement_id: authority.retirement_id,
        resource: original.resource.clone(),
        proof_id,
        page_id,
        page_ordinal: 0,
        page_count: 1,
        first_fact_ordinal: 0,
        total_fact_count: fact_count,
        proof_sha256: proof_sha256.clone(),
        next_page: None,
        facts,
    };
    let path = store
        .paths
        .vector_bucket_retirement_proof_path(&authority.bucket.to_handle().unwrap(), authority.retirement_id, proof_id, page_id)
        .unwrap();
    let proof_root = (&store.write_bucket_retirement_record(&path, None, &page, fence).await.unwrap()).into();
    VectorCatalogRetiredResourceProofV1 {
        resource: original.resource.clone(),
        proof_id,
        proof_root,
        proof_page_count: 1,
        fact_count,
        proof_sha256,
    }
}

#[test]
#[serial_test::serial]
fn retirement_resource_closure_rejects_rehashed_missing_edges_and_owners() {
    run_native_vector_delete_test("catalog-retirement-resource-closure", || async {
        let (store, resource, _fixture) = completed_native_resource().await;
        let bucket = resource.bucket.to_handle().unwrap();
        let lifecycle = store.backend.acquire_bucket_lifecycle_read(&bucket).await.unwrap();
        let gc = store
            .backend
            .acquire_write_lock(
                store.catalog_bucket(),
                &store
                    .paths
                    .vector_gc_lock_path(bucket.storage_bucket_id().as_str(), &resource.table_id),
            )
            .await
            .unwrap();
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal(), gc.lock_lost_signal()]);
        let authority = store.prepare_bucket_retirement_plan(&bucket, &fence).await.unwrap();
        let original = store
            .bucket_retirement_resource_by_identity(&authority.record, &resource, &fence)
            .await
            .unwrap();
        store
            .validate_bucket_retirement_resource_closure(&authority.record, &original, &fence)
            .await
            .unwrap();
        for alteration in [
            "missing-source",
            "missing-job-edge",
            "wrong-job-owner",
            "wrong-receipt-owner",
            "extra-job-count",
        ] {
            let malformed = altered_native_proof(&store, &authority.record, &original, &fence, |facts| {
                let index = facts
                    .iter()
                    .position(|fact| match alteration {
                        "missing-source" => matches!(fact, Fact::SourceRecord { reference } if facts.iter().any(|anchor| matches!(anchor, Fact::ResourceAnchor { delete_intent, .. } if delete_intent == reference))),
                        "missing-job-edge" | "wrong-job-owner" => matches!(fact, Fact::JobSourceReference { .. }),
                        "wrong-receipt-owner" => matches!(fact, Fact::RetirementReceipt { .. }),
                        "extra-job-count" => matches!(fact, Fact::Job { .. }),
                        _ => unreachable!(),
                    })
                    .expect("real fixture supplies the targeted fact");
                match alteration {
                    "missing-source" | "missing-job-edge" => {
                        facts.remove(index);
                    }
                    "wrong-job-owner" => {
                        let Fact::JobSourceReference { job_id, .. } = &mut facts[index] else { unreachable!() };
                        *job_id = Uuid::new_v4();
                    }
                    "wrong-receipt-owner" => {
                        let Fact::RetirementReceipt { retirement_id, .. } = &mut facts[index] else { unreachable!() };
                        *retirement_id = Uuid::new_v4();
                    }
                    "extra-job-count" => {
                        let Fact::Job { source_ref_count, .. } = &mut facts[index] else { unreachable!() };
                        *source_ref_count += 1;
                    }
                    _ => unreachable!(),
                }
            })
            .await;
            // All pages and digests remain valid; the resource closure itself must reject these facts.
            store
                .visit_bucket_retirement_proof(&authority.record, &malformed, &fence, |_| Ok(()))
                .await
                .unwrap();
            assert!(
                store
                    .validate_bucket_retirement_resource_closure(&authority.record, &malformed, &fence)
                    .await
                    .is_err(),
                "{alteration}"
            );
        }
        let mut wrong = original.clone();
        wrong.resource.table_id = Uuid::new_v4().to_string();
        assert!(
            store
                .validate_bucket_retirement_resource_closure(&authority.record, &wrong, &fence)
                .await
                .is_err()
        );
        let mut missing = original;
        missing.proof_root.object_key.push_str(".missing");
        assert!(
            store
                .validate_bucket_retirement_resource_closure(&authority.record, &missing, &fence)
                .await
                .is_err()
        );
    });
}

#[tokio::test]
async fn retirement_classification_known_path_preserves_corrupt_record_error() {
    let fixture = crate::table_catalog::tests::prepared_commit::prepared_v3_fixture().await;
    let path = fixture.store.paths.table_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &Namespace::parse("indexes").unwrap(),
        &CatalogName::parse(&fixture.entry.table).unwrap(),
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, b"invalid known record".to_vec())
        .await;
    let object = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    let fence = TableCatalogObjectMutationFence::from_signals([]);
    assert!(matches!(
        fixture
            .store
            .classify_bucket_retirement_source(&fixture.bucket, &path, &object, &fence)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    ));
}
