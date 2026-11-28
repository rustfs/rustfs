// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::table::test_support::HealOpts;
use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};

async fn claim<B: TableCatalogObjectBackend>(
    catalog: &ObjectTableCatalogStore<B>,
    spec: VectorMaintenanceJobSpec,
) -> VectorMaintenanceWork {
    let created = catalog.create_vector_maintenance_job(spec).await.expect("create exact job");
    let resource = created.job().resource();
    let identity = resource.identity().expect("resource identity");
    let observed = catalog
        .read_vector_maintenance_resource(
            &identity,
            &Namespace::parse(resource.namespace()).expect("namespace"),
            &CatalogName::parse(resource.name()).expect("name"),
        )
        .await
        .expect("current resource");
    let worker = VectorMaintenanceWorker::try_new(
        "native-retirement-recovery".into(),
        Uuid::new_v4(),
        created.job().execution_identity().clone(),
    )
    .expect("worker");
    let VectorMaintenanceClaimOutcome::Acquired(work) = catalog
        .claim_vector_maintenance_job(
            &identity,
            *created.job().job_id(),
            VectorMaintenanceClaimCondition::from_snapshot(&created, observed),
            worker,
            Uuid::new_v4(),
        )
        .await
        .expect("claim")
    else {
        panic!("expected actual claim")
    };
    *work
}

struct NativeCompletion(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for NativeCompletion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn actual_retirement_recovery(committed_receipt: bool) {
    let (catalog, original, _fixture) = native_vector_index_fixture().await;
    let storage = crate::app::gating_test_env::shared_gating_ecstore().await;
    let head = CatalogHead::from_vector_entry(&original).expect("Vector head");
    let resource = head.resource_identity().clone();
    let (initial_guard, entry) = catalog
        .begin_vector_operation(resource.vector_bucket().expect("Vector bucket"), head.namespace(), head.name())
        .await
        .expect("typed Vector entry");
    drop(initial_guard);
    let entry = catalog
        .enroll_vector_maintenance(
            &entry,
            vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).expect("head"))
                .expect("enroll identity"),
        )
        .await
        .expect("enrollment");
    let (operation, _) = catalog
        .begin_vector_operation(
            resource.vector_bucket().expect("Vector bucket"),
            entry.head().namespace(),
            entry.head().name(),
        )
        .await
        .expect("retirement plan protection");
    let key = format!("{}data/recovery-target.bin", resource.dataset_prefix());
    let mut reader = PutObjReader::from_vec(vec![0x61; 4096]);
    let info = storage
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
        .expect("actual retired body");
    let metadata = catalog
        .backend
        .object_metadata(resource.storage_bucket_id(), &key)
        .await
        .expect("metadata")
        .expect("body exists");
    assert_eq!(metadata.body_revision, info.data_dir);
    let target = VectorRetirementTargetV1::from_object_metadata(
        &resource,
        CatalogObjectKey::try_new(key.clone()).expect("key"),
        &metadata,
    )
    .expect("exact target");
    let recorded_resource =
        VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).expect("resource");
    let evidence = VectorRetirementEvidenceV1::try_new(
        recorded_resource.clone(),
        VectorMaintenanceHeadV1::from_entry(&entry).expect("head"),
        None,
        vec![entry.head().metadata().into()],
        vec![entry.head().metadata().into()],
        Vec::new(),
        VectorHistoryRetentionV1::All,
    )
    .expect("reference evidence");
    let prepared = catalog
        .prepare_vector_retirement(&operation, evidence, vec![target])
        .await
        .expect("actual retirement plan");
    let identity = VectorMaintenanceExecutionIdentityV1::try_new("native-recovery-user".into(), [42; 16]).expect("identity");
    let gc = VectorMaintenanceJobSpec::from_semantic(
        VectorMaintenanceSemanticSpec::try_new(
            recorded_resource.clone(),
            VectorMaintenanceKind::OrphanGc,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(
                Some(VectorMaintenanceHeadV1::from_entry(&entry).expect("head")),
                None,
                Some(prepared.plan_ref().into()),
            )
            .expect("plan input"),
            identity.clone(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .expect("GC semantics"),
    )
    .expect("stable GC job");
    drop(operation);
    let work = claim(&catalog, gc).await;
    let dispatch = catalog
        .begin_vector_maintenance_dispatch(work.lease())
        .await
        .expect("GC dispatch");
    let authorization = catalog
        .publish_vector_retirement(
            &dispatch,
            prepared,
            Uuid::new_v4(),
            Uuid::new_v4(),
            CatalogCommitId::try_new(Uuid::new_v4().to_string()).expect("commit"),
        )
        .await
        .expect("published retirement chain");
    catalog
        .finish_vector_maintenance_job(
            &dispatch,
            *work.snapshot().job().revision(),
            VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::Interrupted,
            },
        )
        .await
        .expect("GC yields before physical dispatch");
    drop(dispatch);
    let spec = VectorMaintenanceJobSpec::from_semantic(
        VectorMaintenanceSemanticSpec::try_new(
            recorded_resource,
            VectorMaintenanceKind::DeleteRecovery,
            VectorMaintenanceOrigin::Recovery,
            VectorMaintenanceInputV1::try_new(
                None,
                Some(
                    VectorMaintenanceSourceV1::try_new(
                        VectorMaintenanceSourceKind::RetirementAuthorization,
                        (&authorization).into(),
                    )
                    .expect("exact recovery source"),
                ),
                None,
            )
            .expect("recovery input"),
            identity.clone(),
            None,
            VectorMaintenancePolicyV1::default(),
        )
        .expect("recovery semantics"),
    )
    .expect("stable recovery job");
    let work = claim(&catalog, spec).await;
    let dispatch = catalog
        .begin_vector_maintenance_dispatch(work.lease())
        .await
        .expect("recovery dispatch");
    let progress = catalog
        .read_vector_retirement_progress(&dispatch, &authorization)
        .await
        .expect("progress");
    let dispatch_id = Uuid::new_v4();
    let VectorRetirementDispatchAdmission::Ready(permit) = catalog
        .begin_vector_retirement_delete(&dispatch, &authorization, 0, *progress.revision(), dispatch_id)
        .await
        .expect("actual exact permit")
    else {
        panic!("expected ready target")
    };
    let (ended, completion) = tokio::sync::oneshot::channel();
    let mut options = ObjectOptions {
        expected_bucket_incarnation_id: Some(resource.vector_bucket().expect("bucket").incarnation().as_uuid()),
        delete_mutation_owner: Some(Arc::new((NativeCompletion(Some(ended)), dispatch.retained_owner()))),
        ..Default::default()
    };
    options.add_namespace_lock_guard(&dispatch.dispatch_owner());
    for signal in dispatch.retained_loss_signals() {
        options.add_namespace_lock_lost_signal(signal);
    }
    let (_, errors, mut receipts) = storage
        .delete_objects_with_tier_delete_journal_and_accounting_under_bucket_fence(
            resource.storage_bucket_id(),
            vec![
                ObjectToDelete {
                    object_name: key.clone(),
                    ..Default::default()
                }
                .with_expected_identity(info.data_dir, info.mod_time, false),
            ],
            options,
            permit.lifecycle_owner(),
        )
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(10), completion)
        .await
        .expect("native owner ended")
        .expect("actual holder completion");
    dispatch.check_held().expect("original protection remained healthy");
    assert!(errors.iter().all(Option::is_none));
    let accounting = receipts[0].take().expect("actual native accounting receipt");
    let completed =
        VectorMaintenanceOwnedCompletion::try_from_completed_operations(dispatch.lease(), vec![dispatch_id], Vec::new())
            .expect("actual completed operation");
    let progress = catalog
        .read_vector_retirement_progress(&dispatch, &authorization)
        .await
        .expect("in-flight progress");
    let observation = if committed_receipt {
        VectorRetirementDeleteObservation::Committed(
            VectorDeleteAccountingV1::try_new(accounting.size, accounting.version_id, accounting.removed_current_object)
                .expect("actual accounting"),
        )
    } else {
        // The physical operation completed; its application receipt is deliberately lost.
        let _lost_accounting = accounting;
        VectorRetirementDeleteObservation::Unknown(VectorMaintenanceFailureCode::UnknownOutcome)
    };
    let recorded = catalog
        .record_vector_retirement_delete(&dispatch, &authorization, 0, dispatch_id, *progress.revision(), observation)
        .await
        .expect("record observed result");
    let unresolved = catalog
        .finish_vector_maintenance_job(
            &dispatch,
            *work.snapshot().job().revision(),
            VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::UnknownOutcome,
            },
        )
        .await
        .expect("application completion interrupted");
    drop(permit);
    drop(dispatch);
    let inspection = catalog
        .inspect_vector_retirement_job_recovery(&resource, *unresolved.job().job_id(), *unresolved.job().revision(), &identity)
        .await
        .expect("exact job inspection");
    assert_eq!(inspection.progress(), &recorded);
    let mut native_evidence = Vec::new();
    let original_completion = if committed_receipt {
        None
    } else {
        Some(
            inspection
                .merge_owned_completions(std::slice::from_ref(&completed))
                .expect("strict exact dispatch coverage"),
        )
    };
    for request in inspection.requests() {
        let replay = storage
            .recover_delete_transactions_at_incarnation(
                resource.storage_bucket_id(),
                Some(request.target().object_key()),
                Some(resource.vector_bucket().expect("bucket").incarnation().as_uuid()),
                &HealOpts::default(),
            )
            .await
            .expect("actual exact native replay");
        if let Some(item) = replay {
            assert_eq!(item.object, *request.target().object_key());
            assert!(!item.delete_recovery.expect("typed native status").has_unresolved());
        }
        assert!(matches!(
            storage
                .get_object_info_for_delete(resource.storage_bucket_id(), &key, &ObjectOptions::default())
                .await,
            Err(StorageError::ObjectNotFound(_, _))
        ));
        native_evidence.push(
            VectorRetirementReadmissionEvidence::try_from_native_recovery(
                request.clone(),
                [0, 0, 0],
                None,
                Some(completed.clone()),
            )
            .expect("actual native completion and absence"),
        );
    }
    let resolved = catalog
        .resolve_vector_retirement_job_recovery(inspection, native_evidence, original_completion)
        .await
        .expect("resolve same job");
    assert_eq!(resolved.job().job_id(), unresolved.job().job_id());
    assert_eq!(resolved.job().attempt(), unresolved.job().attempt());
    assert_eq!(resolved.job().execution_identity(), unresolved.job().execution_identity());
    assert_eq!(
        *resolved.job().state(),
        if committed_receipt {
            VectorMaintenanceJobState::Succeeded
        } else {
            VectorMaintenanceJobState::RetryScheduled
        }
    );
    assert_eq!(resolved.job().result_ref().is_some(), committed_receipt);
    assert_eq!(resolved.job().next_retry_at().is_some(), !committed_receipt);
    assert!(matches!(
        storage
            .get_object_info_for_delete(resource.storage_bucket_id(), &key, &ObjectOptions::default())
            .await,
        Err(StorageError::ObjectNotFound(_, _))
    ));
}

#[test]
#[serial_test::serial]
fn retirement_inspection_schedules_same_job_after_actual_native_reply_loss() {
    run_native_vector_delete_test("retirement-actual-retry", || actual_retirement_recovery(false));
}

#[test]
#[serial_test::serial]
fn retirement_inspection_finishes_same_job_from_actual_committed_progress() {
    run_native_vector_delete_test("retirement-actual-terminal", || actual_retirement_recovery(true));
}
