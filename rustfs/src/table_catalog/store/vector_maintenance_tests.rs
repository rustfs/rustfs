use super::*;
use crate::table_catalog::tests::prepared_commit::{PreparedFixture, prepared_v3_fixture};

async fn maintenance_fixture() -> (PreparedFixture, VectorMaintenanceJobSpec) {
    let mut fixture = prepared_v3_fixture().await;
    fixture.entry.version = 4;
    fixture.entry.vector_maintenance_binding = Some(VectorMaintenanceBindingV1::empty());
    let namespace = Namespace::parse("indexes").unwrap();
    let name = CatalogName::parse(fixture.entry.table.clone()).unwrap();
    let path = fixture
        .store
        .paths
        .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&fixture.entry).unwrap())
        .await;
    let entry = VectorCatalogEntry::from_entry(fixture.entry.clone()).unwrap();
    let resource = VectorMaintenanceResourceV1::try_new(entry.head().resource_identity(), &namespace, &name).unwrap();
    let input =
        VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()), None, None).unwrap();
    let spec = VectorMaintenanceSemanticSpec::try_new(
        resource,
        VectorMaintenanceKind::AnnBuild,
        VectorMaintenanceOrigin::Periodic,
        input,
        VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [7; 16]).unwrap(),
        None,
        VectorMaintenancePolicyV1::default(),
    )
    .unwrap();
    (fixture, VectorMaintenanceJobSpec::from_semantic(spec).unwrap())
}

#[tokio::test]
async fn object_maintenance_job_replay_preserves_revision_and_single_write() {
    let (fixture, spec) = maintenance_fixture().await;
    let first = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    let second = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    assert_eq!(first, second);
    let path = fixture
        .store
        .paths
        .vector_maintenance_job_path(&spec.resource.identity().unwrap(), spec.job_id)
        .unwrap();
    assert_eq!(fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &path).await, 1);
    assert_eq!(first.job().state(), &VectorMaintenanceJobState::Queued);
}

#[tokio::test]
async fn object_maintenance_job_recovers_lost_create_response() {
    let (fixture, spec) = maintenance_fixture().await;
    let path = fixture
        .store
        .paths
        .vector_maintenance_job_path(&spec.resource.identity().unwrap(), spec.job_id)
        .unwrap();
    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &path).await;
    let created = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    assert_eq!(created.job().job_id(), &spec.job_id);
    assert_eq!(fixture.store.create_vector_maintenance_job(spec).await.unwrap(), created);
    assert_eq!(fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &path).await, 1);
}

#[tokio::test]
async fn object_maintenance_claim_replays_exact_lease_and_rejects_old_revision() {
    let (fixture, spec) = maintenance_fixture().await;
    let snapshot = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    let resource = spec.resource.identity().unwrap();
    let observed = fixture
        .store
        .read_vector_maintenance_resource(
            &resource,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse(&spec.resource.name).unwrap(),
        )
        .await
        .unwrap();
    let condition = VectorMaintenanceClaimCondition::from_snapshot(&snapshot, observed);
    let worker = VectorMaintenanceWorker::try_new("worker-a".into(), Uuid::new_v4(), spec.execution_identity.clone()).unwrap();
    let lease = Uuid::new_v4();
    let acquired = fixture
        .store
        .claim_vector_maintenance_job(&resource, spec.job_id, condition.clone(), worker.clone(), lease)
        .await
        .unwrap();
    let VectorMaintenanceClaimOutcome::Acquired(work) = acquired else {
        panic!("job was not acquired")
    };
    assert_eq!(work.lease().attempt(), 1);
    assert!(matches!(
        fixture
            .store
            .claim_vector_maintenance_job(&resource, spec.job_id, condition.clone(), worker.clone(), lease)
            .await
            .unwrap(),
        VectorMaintenanceClaimOutcome::Replay(_)
    ));
    assert!(matches!(
        fixture
            .store
            .claim_vector_maintenance_job(&resource, spec.job_id, condition, worker, Uuid::new_v4())
            .await
            .unwrap(),
        VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::Owned)
    ));
}

#[tokio::test]
async fn object_maintenance_job_rejects_recreated_bucket_identity() {
    let (fixture, spec) = maintenance_fixture().await;
    fixture
        .backend
        .state
        .lock()
        .await
        .bucket_handles
        .remove(fixture.bucket.storage_bucket_id().as_str());
    assert!(matches!(
        fixture.store.create_vector_maintenance_job(spec).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
}

#[test]
#[serial_test::serial]
fn object_maintenance_enrollment_and_v4_delete_preserve_exact_terminal() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-v4-delete", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original.clone()).unwrap();
        let resource = entry.head().resource_identity().clone();
        let commit_id =
            vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap();
        let enrolled = store.enroll_vector_maintenance(&entry, commit_id).await.unwrap();
        assert_eq!(enrolled.entry.version, 4);
        assert_eq!(enrolled.head().metadata(), entry.head().metadata());
        assert_eq!(enrolled.entry.vector_maintenance_binding, Some(VectorMaintenanceBindingV1::empty()));
        let (guard, _) = store
            .begin_vector_operation(resource.vector_bucket().unwrap(), enrolled.head().namespace(), enrolled.head().name())
            .await
            .unwrap();
        store
            .delete_vector_index_with_guard(&guard, &enrolled, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
            .await
            .unwrap();
        drop(guard);
        let state = store
            .read_vector_maintenance_resource(&resource, enrolled.head().namespace(), enrolled.head().name())
            .await
            .unwrap();
        assert_eq!(state.state, VectorMaintenanceResourceState::Deleted);
        assert_eq!(state.maintenance, Some(VectorMaintenanceBindingV1::empty()));
        let object = store.maintenance_read_ref(&state.observed_object).await.unwrap();
        let intent: VectorIndexDeleteIntent = serde_json::from_slice(&object.data).unwrap();
        assert!(matches!(intent, VectorIndexDeleteIntent::V2(_)));
        assert_eq!(intent.expected_entry, enrolled.entry);
        assert_eq!(intent.deleted_entry.state, TableCatalogEntryState::Deleted);
        let mut malformed = serde_json::from_slice::<serde_json::Value>(&object.data).unwrap();
        malformed.as_object_mut().unwrap().remove("maintenance_binding");
        assert!(serde_json::from_value::<VectorIndexDeleteIntent>(malformed).is_err());
        assert!(serde_json::from_slice::<VectorIndexDeleteIntentV1>(&object.data).is_err());
    });
}

#[test]
#[serial_test::serial]
fn object_maintenance_deleted_enrollment_preserves_v1_receipt_and_replays() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-deleted-enroll", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let (guard, _) = store
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        store
            .delete_vector_index_with_guard(&guard, &entry, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
            .await
            .unwrap();
        drop(guard);
        let previous = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let before = store.maintenance_read_ref(previous.observed_object()).await.unwrap();
        let original: VectorIndexDeleteIntentV1 = serde_json::from_slice(&before.data).unwrap();
        let update_id = vector_maintenance_enroll_deleted_update_id(&resource, previous.observed_object()).unwrap();
        let upgraded = store
            .enroll_deleted_vector_maintenance(&resource, previous.observed_object(), update_id)
            .await
            .unwrap();
        assert_eq!(upgraded.maintenance, Some(VectorMaintenanceBindingV1::empty()));
        assert_eq!(
            upgraded,
            store
                .enroll_deleted_vector_maintenance(&resource, previous.observed_object(), update_id)
                .await
                .unwrap()
        );
        let after = store.maintenance_read_ref(upgraded.observed_object()).await.unwrap();
        let intent: VectorIndexDeleteIntent = serde_json::from_slice(&after.data).unwrap();
        assert_eq!(intent.expected_entry, original.expected_entry);
        assert_eq!(intent.deleted_entry, original.deleted_entry);
        assert_eq!(intent.request_sha256, original.request_sha256);
        assert_eq!(intent.phase, VectorIndexDeletePhase::LogicallyDeleted);
        assert!(upgraded.observed_object().content_length() > previous.observed_object().content_length());
        assert_ne!(upgraded.observed_object().content_sha256(), previous.observed_object().content_sha256());
        assert_ne!(upgraded.observed_object().body_revision(), previous.observed_object().body_revision());
        let sources = format!("{}sources/", store.paths.vector_maintenance_prefix(&resource).unwrap());
        let stale = store
            .copy_vector_maintenance_source(&resource, previous.observed_object())
            .await;
        assert!(matches!(stale, Err(TableCatalogStoreError::Invalid(message)) if message.contains("exceeds the maximum size")));
        let retained = store
            .backend
            .list_catalog_objects_page(store.catalog_bucket(), &sources, None, std::num::NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        assert!(retained.objects.is_empty() && !retained.is_truncated);
        assert_eq!(store.maintenance_read_ref(upgraded.observed_object()).await.unwrap().data, after.data);
        let copied = store
            .copy_vector_maintenance_source(&resource, upgraded.observed_object())
            .await
            .unwrap();
        assert_eq!(copied.content_length(), upgraded.observed_object().content_length());
        assert_eq!(copied.content_sha256(), upgraded.observed_object().content_sha256());
        assert_ne!(copied.object_key(), upgraded.observed_object().object_key());
        assert_eq!(store.maintenance_read_ref(&copied).await.unwrap().data, after.data);
    });
}

async fn claim_native_job<B: TableCatalogObjectBackend>(
    store: &ObjectTableCatalogStore<B>,
    spec: VectorMaintenanceJobSpec,
) -> VectorMaintenanceWork {
    let created = store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    let resource = spec.resource.identity().unwrap();
    let observed = store
        .read_vector_maintenance_resource(
            &resource,
            &Namespace::parse(&spec.resource.namespace).unwrap(),
            &CatalogName::parse(&spec.resource.name).unwrap(),
        )
        .await
        .unwrap();
    let condition = VectorMaintenanceClaimCondition::from_snapshot(&created, observed);
    let worker =
        VectorMaintenanceWorker::try_new("native-maintenance-test".into(), Uuid::new_v4(), spec.execution_identity).unwrap();
    let VectorMaintenanceClaimOutcome::Acquired(work) = store
        .claim_vector_maintenance_job(&resource, spec.job_id, condition, worker, Uuid::new_v4())
        .await
        .unwrap()
    else {
        panic!("expected native maintenance claim");
    };
    *work
}

#[test]
#[serial_test::serial]
fn object_maintenance_heartbeat_and_finish_replay_preserve_latest_update() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-native-replay", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let entry = store
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let resource_record =
            VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                resource_record,
                VectorMaintenanceKind::AnnBuild,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()), None, None)
                    .unwrap(),
                VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [9; 16]).unwrap(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let work = claim_native_job(&store, spec).await;
        let guard = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
        assert!(VectorMaintenanceRecoveryOwner::new(work.lease()).complete(&guard).is_err());
        let pending_owner = VectorMaintenanceRecoveryOwner::new(work.lease());
        let pending = pending_owner.begin_native_operation(Uuid::new_v4()).unwrap();
        assert!(pending_owner.complete(&guard).is_err());
        drop(pending);
        let cancelled_owner = VectorMaintenanceRecoveryOwner::new(work.lease());
        drop(cancelled_owner.begin_native_operation(Uuid::new_v4()).unwrap());
        assert!(cancelled_owner.complete(&guard).is_err());
        let beat = store
            .heartbeat_vector_maintenance_job(&guard, *work.snapshot().job.revision())
            .await
            .unwrap();
        assert_eq!(
            beat,
            store
                .heartbeat_vector_maintenance_job(&guard, *work.snapshot().job.revision())
                .await
                .unwrap()
        );
        let observed = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let reason = VectorMaintenanceNoWorkReason::NoCandidates;
        let finished = store
            .finish_vector_maintenance_job(
                &guard,
                *beat.job.revision(),
                VectorMaintenanceExecutionOutcome::NoWork {
                    observed: Box::new(observed.clone()),
                    reason,
                },
            )
            .await
            .unwrap();
        assert_eq!(finished.job.state, VectorMaintenanceJobState::Succeeded);
        assert_eq!(
            finished,
            store
                .finish_vector_maintenance_job(
                    &guard,
                    *beat.job.revision(),
                    VectorMaintenanceExecutionOutcome::NoWork {
                        observed: Box::new(observed),
                        reason
                    }
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .heartbeat_vector_maintenance_job(&guard, *beat.job.revision())
                .await
                .is_err()
        );
    });
}

#[test]
#[serial_test::serial]
fn object_maintenance_retirement_publishes_exact_chain_and_rejects_reintroduction() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-retirement", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let entry = store
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let (operation, _) = store
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let key = format!("{}data/orphan.bin", resource.dataset_prefix());
        let mut reader = PutObjReader::from_vec(b"retirement-fixture".to_vec());
        store
            .backend
            .store
            .put_object(
                resource.storage_bucket_id(),
                &key,
                &mut reader,
                &ObjectOptions {
                    mod_time: Some(OffsetDateTime::now_utc() - Duration::seconds(1800)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let metadata = store
            .backend
            .object_metadata(resource.storage_bucket_id(), &key)
            .await
            .unwrap()
            .unwrap();
        let target =
            VectorRetirementTargetV1::from_object_metadata(&resource, CatalogObjectKey::try_new(key.clone()).unwrap(), &metadata)
                .unwrap();
        let resource_record =
            VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
        let evidence = VectorRetirementEvidenceV1::try_new(
            resource_record.clone(),
            VectorMaintenanceHeadV1::from_entry(&entry).unwrap(),
            None,
            vec![entry.head().metadata().into()],
            vec![entry.head().metadata().into()],
            Vec::new(),
            VectorHistoryRetentionV1::All,
        )
        .unwrap();
        let prepared = store
            .prepare_vector_retirement(&operation, evidence, vec![target.clone()])
            .await
            .unwrap();
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                resource_record,
                VectorMaintenanceKind::OrphanGc,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(
                    Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()),
                    None,
                    Some(prepared.plan_ref().into()),
                )
                .unwrap(),
                VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [8; 16]).unwrap(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        drop(operation);
        let work = claim_native_job(&store, spec).await;
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
        let current = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        assert_eq!(current.maintenance.as_ref().unwrap().retirement_count, 1);
        assert_eq!(current.maintenance.as_ref().unwrap().retirement_tip, Some((&authorization).into()));
        assert_eq!(
            store
                .read_vector_retirement_authorization_with_guard(&dispatch)
                .await
                .unwrap(),
            Some(authorization.clone())
        );
        let (operation, head) = store
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let refs = vec![
            CatalogManagedObjectIdentity::try_new(
                head.head().metadata().object_key().clone(),
                head.head().metadata().body_revision().unwrap(),
            )
            .unwrap(),
            CatalogManagedObjectIdentity::try_new(
                CatalogObjectKey::try_new(key.clone()).unwrap(),
                CatalogBodyRevision::try_new(target.body_revision).unwrap(),
            )
            .unwrap(),
        ];
        assert!(matches!(
            store
                .validate_vector_references_with_guard(&operation, head.head().metadata(), &refs)
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        ));
        drop(operation);
        let progress = store
            .read_vector_retirement_progress(&dispatch, &authorization)
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let VectorRetirementDispatchAdmission::Ready(permit) = store
            .begin_vector_retirement_delete(&dispatch, &authorization, 0, progress.revision, id)
            .await
            .unwrap()
        else {
            panic!("expected exact retirement permit");
        };
        assert_eq!(permit.target(), &target);
        assert_eq!(permit.dispatch_id(), &id);
        let in_flight = store
            .read_vector_retirement_progress(&dispatch, &authorization)
            .await
            .unwrap();
        assert!(matches!(
            store
                .begin_vector_retirement_delete(&dispatch, &authorization, 0, in_flight.revision, id)
                .await
                .unwrap(),
            VectorRetirementDispatchAdmission::NeedsRecovery
        ));
        let unknown = store
            .record_vector_retirement_delete(
                &dispatch,
                &authorization,
                0,
                id,
                in_flight.revision,
                VectorRetirementDeleteObservation::Unknown(VectorMaintenanceFailureCode::UnknownOutcome),
            )
            .await
            .unwrap();
        assert_eq!(unknown.targets[0].state, VectorRetirementTargetState::Unknown);
        assert_eq!(
            unknown,
            store
                .record_vector_retirement_delete(
                    &dispatch,
                    &authorization,
                    0,
                    id,
                    in_flight.revision,
                    VectorRetirementDeleteObservation::Unknown(VectorMaintenanceFailureCode::UnknownOutcome)
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .backend
                .object_metadata(resource.storage_bucket_id(), &key)
                .await
                .unwrap()
                .is_some()
        );
        drop(permit);
        drop(dispatch);
        let budget = VectorMaintenanceDiscoveryBudget::new(
            tokio_util::sync::CancellationToken::new(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(60),
            NonZeroUsize::new(3).unwrap(),
        );
        let (items, cursor) = store
            .list_vector_retirement_authorities(
                &resource,
                entry.head().namespace(),
                entry.head().name(),
                None,
                NonZeroU16::new(1).unwrap(),
                &budget,
            )
            .await
            .unwrap()
            .into_parts();
        assert!(items.is_empty());
        let cursor = cursor.unwrap();
        assert_eq!(cursor.resource(), &resource);
        assert_eq!(cursor.binding(), current.maintenance.as_ref());
        assert_eq!(cursor.next(), Some(&CatalogMetadataRefRecordV1::from(&authorization)));
        let budget = VectorMaintenanceDiscoveryBudget::new(
            tokio_util::sync::CancellationToken::new(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(60),
            NonZeroUsize::new(8).unwrap(),
        );
        let (items, next) = store
            .list_vector_retirement_authorities(
                &resource,
                entry.head().namespace(),
                entry.head().name(),
                Some(&cursor),
                NonZeroU16::new(1).unwrap(),
                &budget,
            )
            .await
            .unwrap()
            .into_parts();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].authorization_ref(), &authorization);
        assert!(next.is_none());
        let recovered_identity = VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [10; 16]).unwrap();
        let recovery_spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap(),
                VectorMaintenanceKind::DeleteRecovery,
                VectorMaintenanceOrigin::Recovery,
                VectorMaintenanceInputV1::try_new(
                    None,
                    Some(
                        VectorMaintenanceSourceV1::try_new(
                            VectorMaintenanceSourceKind::RetirementAuthorization,
                            (&authorization).into(),
                        )
                        .unwrap(),
                    ),
                    None,
                )
                .unwrap(),
                recovered_identity,
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let recovery = claim_native_job(&store, recovery_spec).await;
        assert!(
            store
                .inspect_vector_retirement_job_recovery(
                    &resource,
                    recovery.lease().job_id(),
                    *recovery.snapshot().job.revision(),
                    recovery.lease().execution_identity()
                )
                .await
                .is_err(),
            "a live Running lease cannot enter recovery inspection"
        );
        let recovery_guard = store.begin_vector_maintenance_dispatch(recovery.lease()).await.unwrap();
        assert_eq!(
            store
                .read_vector_retirement_authorization_with_guard(&recovery_guard)
                .await
                .unwrap(),
            Some(authorization.clone())
        );
        let mut requests = store.prepare_vector_retirement_recovery(&recovery_guard).await.unwrap();
        assert_eq!(requests.len(), 1);
        let observed = store
            .backend
            .object_metadata(resource.storage_bucket_id(), &key)
            .await
            .unwrap();
        assert!(
            VectorRetirementReadmissionEvidence::try_from_native_recovery(requests.remove(0), [0, 0, 0], observed, None).is_err()
        );
        let preserved = store
            .read_vector_retirement_progress(&recovery_guard, &authorization)
            .await
            .unwrap();
        assert_eq!(preserved, unknown);

        let unresolved = store
            .finish_vector_maintenance_job(
                &recovery_guard,
                *recovery.snapshot().job.revision(),
                VectorMaintenanceExecutionOutcome::Unknown {
                    reason: VectorMaintenanceFailureCode::UnknownOutcome,
                },
            )
            .await
            .unwrap();
        drop(recovery_guard);
        let inspection = store
            .inspect_vector_retirement_job_recovery(
                &resource,
                unresolved.job.job_id,
                unresolved.job.revision,
                &unresolved.job.execution_identity,
            )
            .await
            .unwrap();
        assert_eq!(inspection.job(), &unresolved);
        assert_eq!(inspection.progress(), &unknown);
        assert_eq!(inspection.requests().len(), 1);
        assert!(inspection.merge_owned_completions(&[]).is_err());
        assert!(
            store
                .resolve_vector_retirement_job_recovery(inspection.clone(), Vec::new(), None)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .read_vector_maintenance_job(&resource, unresolved.job.job_id)
                .await
                .unwrap()
                .unwrap(),
            unresolved
        );
        assert!(
            store
                .inspect_vector_retirement_job_recovery(
                    &resource,
                    unresolved.job.job_id,
                    Uuid::new_v4(),
                    &unresolved.job.execution_identity
                )
                .await
                .is_err()
        );
        assert!(
            store
                .inspect_vector_retirement_job_recovery(
                    &resource,
                    unresolved.job.job_id,
                    unresolved.job.revision,
                    &VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [11; 16]).unwrap()
                )
                .await
                .is_err()
        );
        let condition = VectorMaintenanceClaimCondition::from_snapshot(&unresolved, inspection.resource().clone());
        let worker = VectorMaintenanceWorker::try_new(
            "recovery-inspection".into(),
            Uuid::new_v4(),
            unresolved.job.execution_identity.clone(),
        )
        .unwrap();
        assert!(matches!(
            store
                .claim_vector_maintenance_job(&resource, unresolved.job.job_id, condition, worker, Uuid::new_v4())
                .await
                .unwrap(),
            VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::RecoveryRequired)
        ));
        let mut manual = unresolved.job.clone();
        manual.state = VectorMaintenanceJobState::ManualReview;
        manual.finished_at = Some(maintenance_now());
        maintenance_stamp(
            &mut manual,
            "RECOVER",
            Some(unresolved.job.revision),
            serde_json::json!({"fixture":"manual-review"}),
        )
        .unwrap();
        let path = store.paths.vector_maintenance_job_path(&resource, manual.job_id).unwrap();
        store
            .backend
            .put_object(
                RUSTFS_META_BUCKET,
                &path,
                encode_vector_maintenance_record(&manual).unwrap(),
                TableCatalogPutPrecondition::IfMatch(unresolved.etag.clone()),
            )
            .await
            .unwrap();
        let inspected = store
            .inspect_vector_retirement_job_recovery(&resource, manual.job_id, manual.revision, &manual.execution_identity)
            .await
            .unwrap();
        assert_eq!(inspected.job().job(), &manual);
        let worker =
            VectorMaintenanceWorker::try_new("recovery-inspection".into(), Uuid::new_v4(), manual.execution_identity.clone())
                .unwrap();
        let condition = VectorMaintenanceClaimCondition::from_snapshot(inspected.job(), inspected.resource().clone());
        assert!(matches!(
            store
                .claim_vector_maintenance_job(&resource, manual.job_id, condition, worker, Uuid::new_v4())
                .await
                .unwrap(),
            VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::Terminal)
        ));
        assert!(
            store
                .resolve_vector_retirement_job_recovery(inspected, Vec::new(), None)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .read_vector_maintenance_job(&resource, manual.job_id)
                .await
                .unwrap()
                .unwrap()
                .job(),
            &manual
        );
        let manual_snapshot = store
            .read_vector_maintenance_job(&resource, manual.job_id)
            .await
            .unwrap()
            .unwrap();
        let mut expired = manual.clone();
        expired.state = VectorMaintenanceJobState::Running;
        expired.failure = None;
        expired.finished_at = None;
        let expires_at = OffsetDateTime::now_utc() - Duration::seconds(1);
        let heartbeat_at = expires_at - Duration::seconds(expired.policy.lease_timeout_seconds as i64);
        let lease = expired.lease.as_mut().unwrap();
        lease.claimed_at = maintenance_format_time(heartbeat_at);
        lease.heartbeat_at = maintenance_format_time(heartbeat_at);
        lease.expires_at = maintenance_format_time(expires_at);
        expired.created_at = maintenance_format_time(heartbeat_at);
        expired.updated_at = maintenance_format_time(heartbeat_at);
        maintenance_stamp(
            &mut expired,
            "RECOVER",
            Some(manual.revision),
            serde_json::json!({"fixture":"expired-running"}),
        )
        .unwrap();
        store
            .backend
            .put_object(
                RUSTFS_META_BUCKET,
                &path,
                encode_vector_maintenance_record(&expired).unwrap(),
                TableCatalogPutPrecondition::IfMatch(manual_snapshot.etag),
            )
            .await
            .unwrap();
        let inspection = store
            .inspect_vector_retirement_job_recovery(&resource, expired.job_id, expired.revision, &expired.execution_identity)
            .await
            .unwrap();
        assert_eq!(inspection.job().job(), &expired);
        let condition = VectorMaintenanceClaimCondition::from_snapshot(inspection.job(), inspection.resource().clone());
        let worker =
            VectorMaintenanceWorker::try_new("recovery-inspection".into(), Uuid::new_v4(), expired.execution_identity.clone())
                .unwrap();
        assert!(matches!(
            store
                .claim_vector_maintenance_job(&resource, expired.job_id, condition, worker, Uuid::new_v4())
                .await
                .unwrap(),
            VectorMaintenanceClaimOutcome::NotEligible(VectorMaintenanceNotEligible::RecoveryRequired)
        ));
        assert!(
            store
                .resolve_vector_retirement_job_recovery(inspection, Vec::new(), None)
                .await
                .is_err(),
            "lease expiry does not establish original owner completion"
        );
        assert_eq!(
            store
                .read_vector_maintenance_job(&resource, expired.job_id)
                .await
                .unwrap()
                .unwrap()
                .job(),
            &expired
        );
    });
}

async fn persist_observer_test_result<B: TableCatalogObjectBackend>(
    store: &ObjectTableCatalogStore<B>,
    observed: &VectorMaintenanceJobSnapshot,
    result: &VectorMaintenanceResultV1,
) -> VectorMaintenanceJobSnapshot {
    let resource = observed.job.resource.identity().unwrap();
    let bytes = encode_vector_maintenance_record(result).unwrap();
    let digest = hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower);
    let path = format!(
        "{}results/{}/attempt-{}/{digest}.json",
        store.paths.vector_maintenance_prefix(&resource).unwrap(),
        table_catalog_path_hash(&observed.job.job_id.to_string()),
        observed.job.attempt
    );
    if let Some(existing) = store.backend.read_object(store.catalog_bucket(), &path).await.unwrap() {
        assert_eq!(existing.data, bytes);
    } else {
        store
            .backend
            .put_object(store.catalog_bucket(), &path, bytes, TableCatalogPutPrecondition::IfAbsent)
            .await
            .unwrap();
    }
    let object = store
        .backend
        .read_object(store.catalog_bucket(), &path)
        .await
        .unwrap()
        .unwrap();
    let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path).unwrap(), &object).unwrap();
    let mut changed = observed.job.clone();
    changed.result_ref = Some((&reference).into());
    store
        .backend
        .put_object(
            store.catalog_bucket(),
            observed.observed_object.object_key().as_str(),
            encode_vector_maintenance_record(&changed).unwrap(),
            TableCatalogPutPrecondition::IfMatch(observed.etag.clone()),
        )
        .await
        .unwrap();
    let actual = store
        .read_vector_maintenance_job(&resource, changed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.job, changed);
    actual
}

async fn assert_partial_compaction_observer_references<B: TableCatalogObjectBackend>(
    store: &ObjectTableCatalogStore<B>,
    recovered: &VectorMaintenanceJobSnapshot,
    result: &VectorMaintenanceResultV1,
    original_log: &CatalogMetadataRef,
) {
    let resource = recovered.job.resource.identity().unwrap();
    let snapshot = store
        .read_vector_maintenance_resource(
            &resource,
            &Namespace::parse(&recovered.job.resource.namespace).unwrap(),
            &CatalogName::parse(&recovered.job.resource.name).unwrap(),
        )
        .await
        .unwrap();
    let guard = store.begin_vector_maintenance_resource_operation(&snapshot).await.unwrap();
    let mut observed = store
        .read_vector_maintenance_test_job_with_guard(&guard, recovered.job.job_id)
        .await
        .unwrap()
        .unwrap();
    let evidence = store
        .inspect_vector_maintenance_evidence_with_guard(&guard, &observed)
        .await
        .expect("PartialRecovered must preserve the observed original Compaction result");
    assert_eq!(evidence.job, recovered.job);
    assert_eq!(evidence.result.unwrap().result, *result);
    let reference = CatalogMetadataRef::try_from(result.stages[0].commit_ref.as_ref().unwrap()).unwrap();
    let prefix = store.paths.vector_maintenance_prefix(&resource).unwrap();
    assert_eq!(
        reference.object_key().as_str(),
        format!("{prefix}sources/{}.json", reference.content_sha256().to_hex())
    );
    let copied = store.maintenance_read_ref(&reference).await.unwrap();
    let original = store.maintenance_read_ref(original_log).await.unwrap();
    assert_eq!(copied.data, original.data);
    assert_eq!(result.stages[0].outcome, VectorMaintenanceStageOutcome::Committed);
    assert_eq!(result.stages[1].outcome, VectorMaintenanceStageOutcome::NotPrepared);

    // Persist valid native bytes and exact references so rejection exercises the stage path check.
    let wrong_digest_path = format!("{prefix}sources/{}.json", "0".repeat(64));
    assert_ne!(wrong_digest_path, reference.object_key().as_str());
    let foreign_path = format!(
        "{}resources/{}/maintenance/v1/sources/{}.json",
        store.paths.table_bucket_root_prefix(resource.storage_bucket_id()),
        table_catalog_path_hash(&Uuid::new_v4().to_string()),
        reference.content_sha256().to_hex()
    );
    for path in [wrong_digest_path, foreign_path] {
        store
            .backend
            .put_object(store.catalog_bucket(), &path, copied.data.clone(), TableCatalogPutPrecondition::IfAbsent)
            .await
            .unwrap();
        let bytes = store
            .backend
            .read_object(store.catalog_bucket(), &path)
            .await
            .unwrap()
            .unwrap();
        let exact = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path).unwrap(), &bytes).unwrap();
        let mut altered = result.clone();
        altered.stages[0].commit_ref = Some((&exact).into());
        observed = persist_observer_test_result(store, &observed, &altered).await;
        assert!(matches!(
            store.inspect_vector_maintenance_evidence_with_guard(&guard, &observed).await,
            Err(TableCatalogStoreError::Invalid(message)) if message == "result stage reference is outside its deterministic path"
        ));
    }
    let mut altered = result.clone();
    let different_candidate = altered.stages[0].expected_head.as_ref().unwrap().binding.metadata.clone();
    assert_ne!(altered.stages[0].candidate.as_ref(), Some(&different_candidate));
    altered.stages[0].candidate = Some(different_candidate);
    observed = persist_observer_test_result(store, &observed, &altered).await;
    assert!(matches!(
        store.inspect_vector_maintenance_evidence_with_guard(&guard, &observed).await,
        Err(TableCatalogStoreError::Invalid(message)) if message == "result stage log and candidate differ"
    ));

    let mut direct = result.clone();
    direct.stages[0].commit_ref = Some(original_log.into());
    observed = persist_observer_test_result(store, &observed, &direct).await;
    assert_eq!(
        store
            .inspect_vector_maintenance_evidence_with_guard(&guard, &observed)
            .await
            .unwrap()
            .result
            .unwrap()
            .result,
        direct
    );
    observed = persist_observer_test_result(store, &observed, result).await;
    assert_eq!(observed.job, recovered.job);
    assert_eq!(
        store
            .inspect_vector_maintenance_evidence_with_guard(&guard, &observed)
            .await
            .unwrap()
            .result
            .unwrap()
            .result,
        *result
    );
    assert_eq!(store.read_vector_maintenance_resource_with_guard(&guard).await.unwrap(), snapshot);
}

#[test]
#[serial_test::serial]
fn object_maintenance_recovers_partial_compaction_without_repeating_reservation() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-partial-compaction", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let entry = store
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let base = VectorMaintenanceHeadV1::from_entry(&entry).unwrap();
        let resource_record =
            VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
        let identity = VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [6; 16]).unwrap();
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                resource_record.clone(),
                VectorMaintenanceKind::Compaction,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(Some(base.clone()), None, None).unwrap(),
                identity.clone(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let work = claim_native_job(&store, spec).await;
        let dispatch = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
        let operation = store
            .begin_vector_operation_with_lifecycle(
                resource.vector_bucket().unwrap(),
                entry.head().namespace(),
                entry.head().name(),
                dispatch.lifecycle_owner(),
            )
            .await
            .unwrap();
        let reserve_id =
            vector_maintenance_stage_commit_id(work.lease().job_id(), work.lease().attempt(), "reserve-fragments").unwrap();
        let rewrite_id = vector_maintenance_stage_commit_id(work.lease().job_id(), work.lease().attempt(), "rewrite").unwrap();
        let original_bytes = store
            .backend
            .read_object(resource.storage_bucket_id(), entry.head().metadata().object_key().as_str())
            .await
            .unwrap()
            .unwrap();
        let candidate_key =
            CatalogObjectKey::try_new(format!("{}_versions/{}/2.manifest", resource.dataset_prefix(), reserve_id.as_str()))
                .unwrap();
        store
            .backend
            .put_object(
                resource.storage_bucket_id(),
                candidate_key.as_str(),
                original_bytes.data,
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await
            .unwrap();
        let candidate_bytes = store
            .backend
            .read_object(resource.storage_bucket_id(), candidate_key.as_str())
            .await
            .unwrap()
            .unwrap();
        let candidate = CatalogMetadataRef::from_read(candidate_key.clone(), &candidate_bytes).unwrap();
        let descriptor = CatalogFormatDescriptor::try_new(
            1,
            CatalogFormat::Lance,
            entry.head().format_descriptor().format_version(),
            candidate.clone(),
            entry.head().format_descriptor().payload().to_vec(),
        )
        .unwrap();
        let proof = store
            .validate_vector_references_with_guard(
                &operation,
                &candidate,
                &[CatalogManagedObjectIdentity::try_new(candidate_key, candidate.body_revision().unwrap()).unwrap()],
            )
            .await
            .unwrap();
        let prepared = PreparedCatalogCommit::try_from_validated_parts(
            CatalogCommitParts {
                resource_identity: resource.clone(),
                namespace: entry.head().namespace().clone(),
                name: entry.head().name().clone(),
                expected_head: entry.head().expected_head(),
                commit_id: reserve_id.clone(),
                operation: "reserve-fragments".into(),
                idempotency_key: None,
                writer: None,
            },
            candidate.clone(),
            descriptor,
            CatalogEntryUpdates::try_new(Some(entry.entry.warehouse_location.clone()), Some(entry.entry.format_version)).unwrap(),
            Vec::new(),
        )
        .unwrap()
        .with_retirement_validation(proof)
        .unwrap();
        let published = store
            .commit_vector_prepared_with_guard(&operation, prepared, entry.definition())
            .await
            .unwrap();
        let reserve_head = VectorMaintenanceHeadV1::from_catalog_entry(&published.table).unwrap();
        dispatch.check_held().unwrap();
        let completion = VectorMaintenanceOwnedCompletion::try_from_completed_operations(
            work.lease(),
            vec![Uuid::parse_str(reserve_id.as_str()).unwrap()],
            Vec::new(),
        )
        .unwrap();
        drop(operation);
        let original = store
            .finish_vector_maintenance_job(
                &dispatch,
                *work.snapshot().job.revision(),
                VectorMaintenanceExecutionOutcome::Unknown {
                    reason: VectorMaintenanceFailureCode::PartialPublication,
                },
            )
            .await
            .unwrap();
        drop(dispatch);
        let original_source = store
            .copy_vector_maintenance_source(&resource, original.observed_object())
            .await
            .unwrap();
        let recovery_spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                resource_record,
                VectorMaintenanceKind::CommitRecovery,
                VectorMaintenanceOrigin::Recovery,
                VectorMaintenanceInputV1::try_new(
                    None,
                    Some(
                        VectorMaintenanceSourceV1::try_new(
                            VectorMaintenanceSourceKind::MaintenanceJob,
                            (&original_source).into(),
                        )
                        .unwrap(),
                    ),
                    None,
                )
                .unwrap(),
                identity,
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let recovery = claim_native_job(&store, recovery_spec).await;
        let recovery_guard = store.begin_vector_maintenance_dispatch(recovery.lease()).await.unwrap();
        let (_, log_ref) = store
            .read_vector_maintenance_stage_commit(&recovery_guard, &original, "reserve-fragments")
            .await
            .unwrap()
            .unwrap();
        let log_copy = store
            .copy_vector_maintenance_source_with_guard(&recovery_guard, &log_ref)
            .await
            .unwrap();
        let stages = vec![
            VectorMaintenanceStageResultV1::try_new(
                "reserve-fragments".into(),
                reserve_id,
                Some(base),
                Some((&candidate).into()),
                VectorMaintenanceStageOutcome::Committed,
                Some((&log_copy).into()),
            )
            .unwrap(),
            VectorMaintenanceStageResultV1::try_new(
                "rewrite".into(),
                rewrite_id.clone(),
                Some(reserve_head.clone()),
                None,
                VectorMaintenanceStageOutcome::NotPrepared,
                None,
            )
            .unwrap(),
        ];
        let evidence = PreparedVectorMaintenanceRecovery::try_from_verified_parts(
            original_source,
            recovery.lease().clone(),
            stages,
            completion,
        )
        .unwrap()
        .with_absent_candidates(vec![
            VectorMaintenanceAbsentStageCandidate::try_new(
                "rewrite".into(),
                CatalogObjectKey::try_new(format!("{}_versions/{}/3.manifest", resource.dataset_prefix(), rewrite_id.as_str()))
                    .unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
        let recovered = store
            .recover_vector_maintenance_job(&recovery_guard, &original, evidence)
            .await
            .unwrap();
        assert_eq!(recovered.job.state, VectorMaintenanceJobState::PartialRecovered);
        assert_eq!(recovered.job.failure, Some(VectorMaintenanceFailureCode::PartialPublication));
        assert_eq!(recovered.job.spec(), original.job.spec());
        assert!(recovered.job.next_retry_at.is_none());
        let result = store
            .maintenance_read_ref(&CatalogMetadataRef::try_from(recovered.job.result_ref.as_ref().unwrap()).unwrap())
            .await
            .unwrap();
        let result = decode_vector_maintenance_record::<VectorMaintenanceResultV1>(&result.data).unwrap();
        assert_eq!(result.effect, VectorMaintenanceResultEffect::PartialRecovered);
        assert_eq!(result.recorded_by_job_id, recovery.lease().job_id());
        let current = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        assert_eq!(current.head, Some(reserve_head));
        let replay = store
            .read_vector_maintenance_completed_original_with_guard(&recovery_guard)
            .await
            .unwrap()
            .unwrap();
        let replay = store.maintenance_read_ref(&replay).await.unwrap();
        assert_eq!(
            decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&replay.data).unwrap(),
            recovered.job
        );
        drop(recovery_guard);
        assert_partial_compaction_observer_references(&store, &recovered, &result, &log_ref).await;

        let ordinary_spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap(),
                VectorMaintenanceKind::CommitRecovery,
                VectorMaintenanceOrigin::Recovery,
                VectorMaintenanceInputV1::try_new(
                    None,
                    Some(VectorMaintenanceSourceV1::try_new(VectorMaintenanceSourceKind::CommitLog, (&log_copy).into()).unwrap()),
                    None,
                )
                .unwrap(),
                VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [6; 16]).unwrap(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let ordinary = claim_native_job(&store, ordinary_spec).await;
        let guard = store.begin_vector_maintenance_dispatch(ordinary.lease()).await.unwrap();
        let operation = store
            .begin_vector_operation_with_lifecycle(
                resource.vector_bucket().unwrap(),
                entry.head().namespace(),
                entry.head().name(),
                guard.lifecycle_owner(),
            )
            .await
            .unwrap();
        let (log, parts) = store
            .read_vector_maintenance_commit_source_with_operation_guard(&guard, &operation)
            .await
            .unwrap();
        let binding = log.vector_commit_binding.as_ref().unwrap();
        let proof = store
            .validate_vector_references_with_guard(
                &operation,
                &candidate,
                &[
                    CatalogManagedObjectIdentity::try_new(candidate.object_key().clone(), candidate.body_revision().unwrap())
                        .unwrap(),
                ],
            )
            .await
            .unwrap();
        let prepared = PreparedCatalogCommit::try_from_validated_parts(
            parts,
            candidate.clone(),
            CatalogFormatDescriptor::try_from(&binding.descriptor).unwrap(),
            CatalogEntryUpdates::try_new(Some(entry.entry.warehouse_location.clone()), Some(entry.entry.format_version)).unwrap(),
            Vec::new(),
        )
        .unwrap()
        .with_retirement_validation(proof)
        .unwrap();
        let result = store
            .finalize_vector_maintenance_commit_with_operation_guard(&guard, &operation, prepared.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .finalize_vector_maintenance_commit_with_operation_guard(&guard, &operation, prepared)
                .await
                .unwrap(),
            Some(result.clone())
        );
        let saved = store.maintenance_read_ref(&result).await.unwrap();
        let saved: CommitLogEntry = serde_json::from_slice(&saved.data).unwrap();
        assert_eq!(saved.status, CommitLogStatus::Committed);
        assert_eq!(saved.commit_id, log.commit_id);
        let current = store.read_vector_maintenance_resource_with_guard(&operation).await.unwrap();
        assert_eq!(current.head, Some(VectorMaintenanceHeadV1::from_catalog_entry(&published.table).unwrap()));
    });
}

#[test]
fn object_maintenance_retirement_enforces_modification_age_and_unknown_time() {
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(3000);
    let mut target = VectorRetirementTargetV1 {
        object_key: ".rustfs-table/22222222-2222-4222-8222-222222222222/dataset/data/fixture".into(),
        body_revision: Uuid::new_v4(),
        mod_time: Some(VectorObjectTimeV1::from_time(now - Duration::seconds(900))),
        content_length: 1,
        etag: None,
        version_id: None,
        delete_marker: false,
    };
    assert!(vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, now).unwrap());
    target.mod_time = Some(VectorObjectTimeV1::from_time(now - Duration::seconds(899)));
    assert!(!vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, now).unwrap());
    target.mod_time = Some(VectorObjectTimeV1::from_time(now - Duration::seconds(1000)));
    assert!(
        !vector_retirement_target_is_old_enough(
            &target,
            VectorHistoryRetentionV1::Last {
                count: 1,
                min_age_seconds: 1800
            },
            now
        )
        .unwrap()
    );
    target.mod_time = None;
    assert!(!vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, now).unwrap());
}

#[tokio::test]
async fn object_maintenance_history_rejects_bytes_before_decoding() {
    let fixture = prepared_v3_fixture().await;
    let prefix = fixture
        .store
        .paths
        .commit_log_entries_prefix(&fixture.entry.table_bucket, &fixture.entry.table_id);
    let path = format!("{prefix}oversized.json");
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, vec![b' '; VECTOR_MAINTENANCE_RECORD_MAX_BYTES + 1])
        .await;
    let (guard, _) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let result = fixture.store.read_vector_maintenance_history_with_guard(&guard).await;
    assert!(matches!(result, Err(TableCatalogStoreError::Unavailable(message)) if message.contains("CapacityExceeded")));
    assert!(
        fixture
            .backend
            .object_metadata(RUSTFS_META_BUCKET, &path)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn object_maintenance_history_rejects_excess_discovery_without_partial_proof() {
    let fixture = prepared_v3_fixture().await;
    let prefix = fixture
        .store
        .paths
        .commit_log_entries_prefix(&fixture.entry.table_bucket, &fixture.entry.table_id);
    for index in 0..=VECTOR_RETIREMENT_MAX_REFERENCES {
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &format!("{prefix}{index:05}.unresolved"), vec![1])
            .await;
    }
    let (guard, _) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let result = fixture.store.read_vector_maintenance_candidate_roots_with_guard(&guard).await;
    assert!(matches!(result, Err(TableCatalogStoreError::Unavailable(message)) if message.contains("CapacityExceeded")));
    assert_eq!(
        fixture.backend.list_objects(RUSTFS_META_BUCKET, &prefix).await.unwrap().len(),
        VECTOR_RETIREMENT_MAX_REFERENCES + 1
    );
}

#[test]
#[serial_test::serial]
fn object_maintenance_deleted_batches_reuse_published_complete_evidence() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture_at, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-deleted-evidence", || async {
        let old_time = OffsetDateTime::now_utc() - Duration::seconds(1800);
        let (store, original, _fixture) = native_vector_index_fixture_at(old_time).await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let entry = store
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let namespace = entry.head().namespace().clone();
        let name = entry.head().name().clone();
        let second_key = format!("{}data/second-batch.bin", resource.dataset_prefix());
        let mut reader = PutObjReader::from_vec(b"second retained file".to_vec());
        store
            .backend
            .store
            .put_object(
                resource.storage_bucket_id(),
                &second_key,
                &mut reader,
                &ObjectOptions {
                    mod_time: Some(old_time),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let second_object = store
            .backend
            .read_object(resource.storage_bucket_id(), &second_key)
            .await
            .unwrap()
            .unwrap();
        let second_ref =
            CatalogMetadataRef::from_read(CatalogObjectKey::try_new(second_key.clone()).unwrap(), &second_object).unwrap();
        let (operation, _) = store
            .begin_vector_operation(resource.vector_bucket().unwrap(), &namespace, &name)
            .await
            .unwrap();
        store
            .delete_vector_index_with_guard(&operation, &entry, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
            .await
            .unwrap();
        drop(operation);
        let resource_record = VectorMaintenanceResourceV1::try_new(&resource, &namespace, &name).unwrap();
        let original_refs = vec![entry.head().metadata().into(), (&second_ref).into()];
        let mut latest_auth = None;
        for (batch, target_ref) in [entry.head().metadata(), &second_ref].into_iter().enumerate() {
            let snapshot = store
                .read_vector_maintenance_resource(&resource, &namespace, &name)
                .await
                .unwrap();
            let operation = store.begin_vector_maintenance_resource_operation(&snapshot).await.unwrap();
            let history_refs = if batch == 0 {
                assert!(
                    store
                        .read_vector_deleted_retirement_evidence_with_guard(&operation)
                        .await
                        .unwrap()
                        .is_none()
                );
                original_refs.clone()
            } else {
                assert!(
                    store
                        .backend
                        .object_metadata(resource.storage_bucket_id(), entry.head().metadata().object_key().as_str())
                        .await
                        .unwrap()
                        .is_none()
                );
                let reused = store
                    .read_vector_deleted_retirement_evidence_with_guard(&operation)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(reused.history_refs, original_refs);
                reused.history_refs
            };
            let source = store
                .copy_vector_maintenance_source_with_operation_guard(&operation, snapshot.source_intent().unwrap())
                .await
                .unwrap();
            let evidence = VectorRetirementEvidenceV1::try_new(
                resource_record.clone(),
                snapshot.head.clone().unwrap(),
                Some((&source).into()),
                history_refs,
                Vec::new(),
                Vec::new(),
                VectorHistoryRetentionV1::All,
            )
            .unwrap();
            let metadata = store
                .backend
                .object_metadata(resource.storage_bucket_id(), target_ref.object_key().as_str())
                .await
                .unwrap()
                .unwrap();
            let target =
                VectorRetirementTargetV1::from_object_metadata(&resource, target_ref.object_key().clone(), &metadata).unwrap();
            let prepared = store
                .prepare_vector_retirement(&operation, evidence, vec![target])
                .await
                .unwrap();
            let spec = VectorMaintenanceJobSpec::from_semantic(
                VectorMaintenanceSemanticSpec::try_new(
                    resource_record.clone(),
                    VectorMaintenanceKind::HistoryGc,
                    VectorMaintenanceOrigin::Periodic,
                    VectorMaintenanceInputV1::try_new(snapshot.head.clone(), None, Some(prepared.plan_ref().into())).unwrap(),
                    VectorMaintenanceExecutionIdentityV1::try_new("maintenance-test".into(), [8; 16]).unwrap(),
                    None,
                    VectorMaintenancePolicyV1::default(),
                )
                .unwrap(),
            )
            .unwrap();
            drop(operation);
            let work = claim_native_job(&store, spec).await;
            let dispatch = store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
            let auth = store
                .publish_vector_retirement(
                    &dispatch,
                    prepared,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap(),
                )
                .await
                .unwrap();
            assert!(
                store
                    .read_vector_retirement_authorization_with_guard(&dispatch)
                    .await
                    .unwrap()
                    .is_some()
            );
            drop(dispatch);
            // The native backend removes the exact fixture body after its retirement became authoritative.
            store
                .backend
                .delete_object(resource.storage_bucket_id(), target_ref.object_key().as_str())
                .await
                .unwrap();
            latest_auth = Some(auth);
        }
        let snapshot = store
            .read_vector_maintenance_resource(&resource, &namespace, &name)
            .await
            .unwrap();
        let operation = store.begin_vector_maintenance_resource_operation(&snapshot).await.unwrap();
        assert_eq!(snapshot.maintenance.as_ref().unwrap().retirement_count, 2);
        assert_eq!(
            store
                .read_vector_deleted_retirement_evidence_with_guard(&operation)
                .await
                .unwrap()
                .unwrap()
                .history_refs,
            original_refs
        );
        let intent_object = store.maintenance_read_ref(snapshot.observed_object()).await.unwrap();
        let mut different: VectorIndexDeleteIntent = serde_json::from_slice(&intent_object.data).unwrap();
        different.created_at = (old_time - Duration::seconds(1))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        different.validate().unwrap();
        store
            .backend
            .put_object(
                store.catalog_bucket(),
                snapshot.observed_object().object_key().as_str(),
                serde_json::to_vec(&different).unwrap(),
                TableCatalogPutPrecondition::Any,
            )
            .await
            .unwrap();
        assert!(matches!(store.read_vector_deleted_retirement_evidence_with_guard(&operation).await,
            Err(TableCatalogStoreError::Conflict(message)) if message.contains("another deletion transaction")));
        store
            .backend
            .put_object(
                store.catalog_bucket(),
                snapshot.observed_object().object_key().as_str(),
                intent_object.data,
                TableCatalogPutPrecondition::Any,
            )
            .await
            .unwrap();
        let entry = store
            .read_vector_maintenance_entry_with_operation_guard(&operation)
            .await
            .unwrap();
        let (_, mut log) = store.read_table_commit_logs(&entry.entry).await.unwrap().pop().unwrap();
        log.commit_id = Uuid::new_v4().to_string();
        log.new_version_token = format!("unpublished-{}", log.commit_id);
        log.status = CommitLogStatus::Staged;
        store
            .paths
            .validate_commit_entry(&log, resource.storage_bucket_id(), resource.table_id().as_str())
            .unwrap();
        let log_path =
            store
                .paths
                .commit_log_entry_path(resource.storage_bucket_id(), resource.table_id().as_str(), &log.commit_id);
        store
            .backend
            .put_object(
                store.catalog_bucket(),
                &log_path,
                serde_json::to_vec(&log).unwrap(),
                TableCatalogPutPrecondition::Any,
            )
            .await
            .unwrap();
        let history = store
            .read_vector_maintenance_history_with_guard(&operation)
            .await
            .expect("an unpublished stage must preserve the complete committed history");
        assert!(!history.is_empty());
        assert!(
            history
                .iter()
                .all(|version| version.commit_id().is_none_or(|id| id.as_str() != log.commit_id))
        );
        let rejected = store.read_vector_deleted_retirement_evidence_with_guard(&operation).await;
        assert!(
            matches!(&rejected, Err(TableCatalogStoreError::Conflict(message)) if message.contains("NeedsRecovery")),
            "unresolved stage must require recovery: {rejected:?}"
        );
        store.backend.delete_object(store.catalog_bucket(), &log_path).await.unwrap();
        let auth = store.maintenance_read_ref(&latest_auth.unwrap()).await.unwrap();
        let auth = decode_vector_maintenance_record::<VectorRetirementAuthorizationV1>(&auth.data).unwrap();
        store
            .backend
            .put_object(
                store.catalog_bucket(),
                &auth.reference_evidence_ref.object_key,
                b"corrupt evidence".to_vec(),
                TableCatalogPutPrecondition::Any,
            )
            .await
            .unwrap();
        assert!(
            store
                .read_vector_deleted_retirement_evidence_with_guard(&operation)
                .await
                .is_err()
        );
    });
}

#[tokio::test]
async fn object_maintenance_query_sessions_have_exact_scope_and_expiry_window() {
    let fixture = prepared_v3_fixture().await;
    let (guard, _) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse(&fixture.entry.namespace).unwrap(),
            &CatalogName::parse(&fixture.entry.table).unwrap(),
        )
        .await
        .unwrap();
    let resource = guard.resource_identity();
    let prefix = format!(".rustfs-table/{}/query-results/", resource.table_id().as_str());
    let key = format!("{prefix}{}.session", "a".repeat(64));
    assert_eq!(vector_retirement_object_prefix(resource, &key).unwrap(), prefix);
    for key in [
        format!("{prefix}session"),
        format!("{prefix}../dataset/object"),
        format!("{prefix}{}.session/extra", "a".repeat(64)),
        format!(".rustfs-table/{}/query-results/{}.session", Uuid::new_v4(), "a".repeat(64)),
    ] {
        assert!(vector_retirement_object_prefix(resource, &key).is_err());
    }
    let now = OffsetDateTime::now_utc();
    let mut target = VectorRetirementTargetV1 {
        object_key: key,
        body_revision: Uuid::new_v4(),
        mod_time: Some(VectorObjectTimeV1::from_time(now - Duration::seconds(3599))),
        content_length: 1,
        etag: None,
        version_id: None,
        delete_marker: false,
    };
    assert!(!vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, now).unwrap());
    target.mod_time = Some(VectorObjectTimeV1::from_time(now - Duration::seconds(3600)));
    assert!(vector_retirement_target_is_old_enough(&target, VectorHistoryRetentionV1::All, now).unwrap());
}

async fn seed_catalog_retirement_admission_header<B: TableCatalogObjectBackend>(
    store: &ObjectTableCatalogStore<B>,
    bucket: &BucketHandle,
) -> String {
    use crate::table_catalog::vector_bucket_retirement_model::VectorBucketCatalogRetirementV1;
    let authority_path = store.paths.vector_bucket_retirement_authority_path(bucket).unwrap();
    let prefix = authority_path.strip_suffix("/authority.json").unwrap();
    let retirement_id = Uuid::new_v4();
    let plan_path = format!("{prefix}/{retirement_id}/plan/{}.json", Uuid::new_v4());
    // This header-only fixture exercises admission and supplies no executable cleanup proof.
    store
        .backend
        .put_object(
            RUSTFS_META_BUCKET,
            &plan_path,
            br#"{"fixture":"maintenance-admission"}"#.to_vec(),
            TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .unwrap();
    let plan = store
        .backend
        .read_object(RUSTFS_META_BUCKET, &plan_path)
        .await
        .unwrap()
        .unwrap();
    let plan_ref = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(plan_path).unwrap(), &plan).unwrap();
    let body = serde_json::to_vec(&serde_json::json!({
        "version":1,"bucket":VectorBucketBindingV1::from_handle(bucket).unwrap(),
        "retirement_id":retirement_id,"created_at":maintenance_now(),
        "plan_root":CatalogMetadataRefRecordV1::from(&plan_ref),
        "plan_page_count":1,"target_count":1,"resource_count":0,
        "inventory_sha256":"1".repeat(64),"closure_sha256":"2".repeat(64)
    }))
    .unwrap();
    decode_vector_maintenance_record::<VectorBucketCatalogRetirementV1>(&body).unwrap();
    store
        .backend
        .put_object(RUSTFS_META_BUCKET, &authority_path, body, TableCatalogPutPrecondition::IfAbsent)
        .await
        .unwrap();
    authority_path
}

#[tokio::test]
async fn object_maintenance_retirement_admission_rejects_create_claim_and_source_copy() {
    for corrupt in [false, true] {
        let (fixture, spec) = maintenance_fixture().await;
        let snapshot = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
        let resource = spec.resource.identity().unwrap();
        let observed = fixture
            .store
            .read_vector_maintenance_resource(
                &resource,
                &Namespace::parse(&spec.resource.namespace).unwrap(),
                &CatalogName::parse(&spec.resource.name).unwrap(),
            )
            .await
            .unwrap();
        let condition = VectorMaintenanceClaimCondition::from_snapshot(&snapshot, observed);
        let worker =
            VectorMaintenanceWorker::try_new("retirement-admission".into(), Uuid::new_v4(), spec.execution_identity.clone())
                .unwrap();
        fixture
            .store
            .copy_vector_maintenance_source(&resource, snapshot.observed_object())
            .await
            .unwrap();
        let mut next_spec = snapshot.job().semantic_spec();
        next_spec.execution_identity = VectorMaintenanceExecutionIdentityV1::try_new("next-account".into(), [9; 16]).unwrap();
        let next_spec = VectorMaintenanceJobSpec::from_semantic(next_spec).unwrap();
        assert_ne!(next_spec.job_id, spec.job_id);
        let authority = seed_catalog_retirement_admission_header(&fixture.store, &fixture.bucket).await;
        if corrupt {
            fixture
                .backend
                .seed_object(RUSTFS_META_BUCKET, &authority, b"{".to_vec())
                .await;
        }
        let before = {
            let state = fixture.backend.state.lock().await;
            (state.objects.clone(), state.put_attempts.clone(), state.delete_attempts.clone())
        };
        let created = fixture.store.create_vector_maintenance_job(next_spec).await;
        let claimed = fixture
            .store
            .claim_vector_maintenance_job(&resource, spec.job_id, condition, worker, Uuid::new_v4())
            .await;
        let copied = fixture
            .store
            .copy_vector_maintenance_source(&resource, snapshot.observed_object())
            .await;
        if corrupt {
            assert!(created.is_err() && claimed.is_err() && copied.is_err());
        } else {
            assert!(matches!(created, Err(TableCatalogStoreError::Conflict(_))));
            assert!(matches!(claimed, Err(TableCatalogStoreError::Conflict(_))));
            assert!(matches!(copied, Err(TableCatalogStoreError::Conflict(_))));
        }
        let state = fixture.backend.state.lock().await;
        assert_eq!(state.objects, before.0);
        assert_eq!(state.put_attempts, before.1);
        assert_eq!(state.delete_attempts, before.2);
    }
}

#[tokio::test]
async fn object_maintenance_retirement_admission_uses_exact_incarnation() {
    let (fixture, spec) = maintenance_fixture().await;
    let mut old = VectorBucketBindingV1::from_handle(&fixture.bucket).unwrap();
    old.bucket_incarnation = Uuid::new_v4();
    let old = old.to_handle().unwrap();
    let old_path = seed_catalog_retirement_admission_header(&fixture.store, &old).await;
    assert_ne!(
        old_path,
        fixture
            .store
            .paths
            .vector_bucket_retirement_authority_path(&fixture.bucket)
            .unwrap()
    );
    let old_before = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &old_path)
        .await
        .unwrap()
        .unwrap();
    let created = fixture.store.create_vector_maintenance_job(spec.clone()).await.unwrap();
    assert_eq!(created.job().resource().bucket().to_handle().unwrap(), fixture.bucket);
    let old_after = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &old_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_before.data, old_after.data);
    assert_eq!(old_before.etag, old_after.etag);
    assert_eq!(old_before.body_revision, old_after.body_revision);
}

#[test]
#[serial_test::serial]
fn object_maintenance_retirement_admission_rejects_dispatch_and_enrollment() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-retirement-admission", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let bucket = resource.vector_bucket().unwrap();
        drop(store.begin_vector_maintenance_enrollment(&entry).await.unwrap());
        let enrollment_entry = entry.clone();
        let entry = store
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap(),
                VectorMaintenanceKind::AnnBuild,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()), None, None)
                    .unwrap(),
                VectorMaintenanceExecutionIdentityV1::try_new("retirement-admission".into(), [7; 16]).unwrap(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let work = claim_native_job(&store, spec).await;
        drop(store.begin_vector_maintenance_dispatch(work.lease()).await.unwrap());
        let current = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        drop(store.begin_vector_maintenance_resource_operation(&current).await.unwrap());
        let head_before = store.maintenance_read_ref(current.observed_object()).await.unwrap();
        let job_before = store.maintenance_read_ref(work.snapshot().observed_object()).await.unwrap();
        let authority = seed_catalog_retirement_admission_header(&store, bucket).await;
        assert!(matches!(
            store.begin_vector_maintenance_dispatch(work.lease()).await,
            Err(TableCatalogStoreError::Conflict(_))
        ));
        let enrollment = store.begin_vector_maintenance_enrollment(&enrollment_entry).await;
        assert!(matches!(&enrollment, Err(TableCatalogStoreError::Conflict(_))));
        assert!(matches!(enrollment,
            Err(TableCatalogStoreError::Conflict(message))
                if message == "Vector bucket Catalog retirement prevents ordinary admission"));
        assert!(matches!(
            store.begin_vector_maintenance_resource_operation(&current).await,
            Err(TableCatalogStoreError::Conflict(_))
        ));
        let head_after = store.maintenance_read_ref(current.observed_object()).await.unwrap();
        let job_after = store.maintenance_read_ref(work.snapshot().observed_object()).await.unwrap();
        assert_eq!(head_after.data, head_before.data);
        assert_eq!(job_after.data, job_before.data);
        assert!(
            store
                .backend
                .read_object(RUSTFS_META_BUCKET, &authority)
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
#[serial_test::serial]
fn object_maintenance_retirement_admission_rejects_deleted_enrollment() {
    use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
    run_native_vector_delete_test("maintenance-retirement-deleted", || async {
        let (store, original, _fixture) = native_vector_index_fixture().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let bucket = resource.vector_bucket().unwrap();
        let (guard, _) = store
            .begin_vector_operation(bucket, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        store
            .delete_vector_index_with_guard(&guard, &entry, CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap())
            .await
            .unwrap();
        drop(guard);
        let current = store
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        assert_eq!(current.state(), VectorMaintenanceResourceState::Deleted);
        let before = store.maintenance_read_ref(current.observed_object()).await.unwrap();
        let intent: VectorIndexDeleteIntent = serde_json::from_slice(&before.data).unwrap();
        assert!(matches!(intent, VectorIndexDeleteIntent::V1(_)));
        let update_id = vector_maintenance_enroll_deleted_update_id(&resource, current.observed_object()).unwrap();
        seed_catalog_retirement_admission_header(&store, bucket).await;
        assert!(matches!(
            store
                .enroll_deleted_vector_maintenance(&resource, current.observed_object(), update_id)
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        ));
        let after = store.maintenance_read_ref(current.observed_object()).await.unwrap();
        assert_eq!(after.data, before.data);
    });
}
