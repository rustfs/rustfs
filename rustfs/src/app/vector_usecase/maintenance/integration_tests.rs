// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::gating_test_env::run_large_stack_test;
use crate::app::vector_usecase::{Backend, tests::Fixture};
use crate::table_catalog::*;
use crate::vector::lance::MaintenancePublicationAuthorizer;
use crate::vector::protocol::Request;
use serde_json::json;
use tokio_util::sync::CancellationToken;

pub(super) async fn fixture() -> (Fixture, MaintenanceExecutor, Arc<Catalog>, VectorCatalogEntry, VectorMaintenanceControl) {
    let fixture = Fixture::new().await;
    let bucket = format!("maintenance-{}", uuid::Uuid::new_v4().simple());
    fixture
        .execute(Request::CreateVectorBucket(
            serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap(),
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
    let request = Request::GetIndex(
        serde_json::from_value(json!({
            "vectorBucketName":bucket,"indexName":"maintenance.index"
        }))
        .unwrap(),
    );
    let authorized = fixture.service.admit(fixture.verified(request)).await.unwrap();
    let catalog = authorized.catalog.as_ref().unwrap().clone();
    let (guard, entry) = catalog
        .begin_vector_operation(
            authorized.bucket.as_ref().unwrap(),
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("maintenance.index").unwrap(),
        )
        .await
        .unwrap();
    drop(guard);
    let (identity, _) = resolve_identity(&fixture.app, &fixture.credentials.access_key).await.unwrap();
    let config = VectorMaintenanceRuntimeConfig::default();
    let source = Arc::new(source::MaintenanceSource {
        app: fixture.app.clone(),
        backend: Backend::new_with_managed_candidate_reader(
            fixture.app.object_store(),
            Arc::new(super::super::dataset::CandidateReader::default()),
        ),
        identity: VectorMaintenanceExecutionIdentityV1::try_new(identity.account().into(), *identity.user_generation()).unwrap(),
        policy: VectorMaintenancePolicyV1::try_new(900, 60, 3, 5, 300).unwrap(),
        gc_scan: parking_lot::Mutex::new(None),
    });
    let executor = MaintenanceExecutor::new(fixture.app.clone(), identity, fixture.service.limits, source);
    let control = VectorMaintenanceControl::new(
        CancellationToken::new(),
        tokio::time::Instant::now() + std::time::Duration::from_secs(60),
        VectorMaintenanceBudgets::new(&config, None).unwrap(),
    );
    (fixture, executor, catalog, entry, control)
}

#[test]
#[serial_test::serial]
fn vector_maintenance_preparation_uses_real_iam_and_preserves_manifest() {
    run_large_stack_test("vector-maintenance-preparation", || async {
        let (_fixture, executor, catalog, before, control) = fixture().await;
        let resource = before.head().resource_identity().clone();
        let commit_id =
            vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&before).unwrap()).unwrap();
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::EnrollCurrent {
                    expected: Box::new(before.clone()),
                    commit_id,
                },
                control.clone(),
            )
            .await
            .expect("ordinary Vector IAM permission authorizes real manifest validation");
        let (guard, after) = catalog
            .begin_vector_operation(resource.vector_bucket().unwrap(), before.head().namespace(), before.head().name())
            .await
            .unwrap();
        assert_eq!(*VectorMaintenanceHeadV1::from_entry(&after).unwrap().entry_version(), 4);
        assert_eq!(after.head().metadata(), before.head().metadata());
        drop(guard);
        let head = VectorMaintenanceHeadV1::from_entry(&after).unwrap();
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::PrepareGc {
                    resource: VectorMaintenanceResourceV1::try_new(&resource, after.head().namespace(), after.head().name())
                        .unwrap(),
                    kind: VectorMaintenanceKind::HistoryGc,
                    expected_head: Box::new(head.clone()),
                },
                control,
            )
            .await
            .unwrap();
        let snapshot = catalog
            .read_vector_maintenance_resource(&resource, after.head().namespace(), after.head().name())
            .await
            .unwrap();
        assert_eq!(snapshot.head(), Some(&head), "ALL retention does not retire Active history");
    });
}

#[test]
#[serial_test::serial]
fn vector_maintenance_preparation_rechecks_explicit_deny_before_manifest_access() {
    run_large_stack_test("vector-maintenance-preparation-denied", || async {
        let (fixture, executor, catalog, before, control) = fixture().await;
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3vectors:GetVectors","Resource":"*"}
            ]))
            .await;
        let resource = before.head().resource_identity().clone();
        let head = VectorMaintenanceHeadV1::from_entry(&before).unwrap();
        let commit_id = vector_maintenance_enroll_commit_id(&resource, &head).unwrap();
        assert!(
            executor
                .prepare(
                    catalog.clone(),
                    VectorMaintenancePreparationAction::EnrollCurrent {
                        expected: Box::new(before.clone()),
                        commit_id,
                    },
                    control
                )
                .await
                .is_err()
        );
        let snapshot = catalog
            .read_vector_maintenance_resource(&resource, before.head().namespace(), before.head().name())
            .await
            .unwrap();
        assert_eq!(snapshot.head(), Some(&head), "denied preparation must leave the original Catalog head");
        assert!(snapshot.maintenance().is_none());
    });
}

#[test]
#[serial_test::serial]
fn vector_maintenance_publication_rechecks_current_vector_policy() {
    run_large_stack_test("vector-maintenance-publication-policy", || async {
        let (fixture, executor, catalog, before, control) = fixture().await;
        let resource = before.head().resource_identity().clone();
        let commit_id =
            vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&before).unwrap()).unwrap();
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::EnrollCurrent {
                    expected: Box::new(before.clone()),
                    commit_id,
                },
                control,
            )
            .await
            .unwrap();
        let snapshot = catalog
            .read_vector_maintenance_resource(&resource, before.head().namespace(), before.head().name())
            .await
            .unwrap();
        let spec = VectorMaintenanceSemanticSpec::try_new(
            VectorMaintenanceResourceV1::try_new(&resource, before.head().namespace(), before.head().name()).unwrap(),
            VectorMaintenanceKind::AnnBuild,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(snapshot.head().cloned(), None, None).unwrap(),
            executor.source.identity.clone(),
            None,
            executor.source.policy.clone(),
        )
        .unwrap();
        let job = catalog
            .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(spec).unwrap())
            .await
            .unwrap();
        let claim = catalog
            .claim_vector_maintenance_job(
                &resource,
                *job.job().job_id(),
                VectorMaintenanceClaimCondition::from_snapshot(&job, snapshot),
                VectorMaintenanceWorker::try_new(
                    "publication-test".into(),
                    uuid::Uuid::new_v4(),
                    executor.source.identity.clone(),
                )
                .unwrap(),
                uuid::Uuid::new_v4(),
            )
            .await
            .unwrap();
        let VectorMaintenanceClaimOutcome::Acquired(work) = claim else {
            panic!("the actual maintenance job must be acquired");
        };
        let dispatch = Arc::new(catalog.begin_vector_maintenance_dispatch(work.lease()).await.unwrap());
        let authorization = MaintenancePublicationAuthorization {
            source: executor.source.clone(),
            catalog: catalog.clone(),
            dispatch,
            access: ManagedObjectAccess::try_from_parts(
                resource.vector_bucket().unwrap().clone(),
                resource.table_id().clone(),
                resource.dataset_prefix(),
                std::collections::BTreeSet::from([ManagedObjectOperation::CreateOnly]),
            )
            .unwrap(),
        };
        authorization
            .authorize(&resource, before.head().metadata())
            .await
            .expect("Vector permissions authorize publication while ordinary S3 remains denied");
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:GetVectors","Resource":"*"},
                {"Effect":"Deny","Action":"s3vectors:PutVectors","Resource":"*"}
            ]))
            .await;
        assert!(matches!(
            authorization.authorize(&resource, before.head().metadata()).await,
            Err(crate::vector::lance::LanceManifestError::Catalog(TableCatalogStoreError::Io {
                classification: CatalogIoErrorKind::Denied,
                ..
            }))
        ));
    });
}

fn fresh_control() -> VectorMaintenanceControl {
    VectorMaintenanceControl::new(
        CancellationToken::new(),
        tokio::time::Instant::now() + std::time::Duration::from_secs(60),
        VectorMaintenanceBudgets::new(&VectorMaintenanceRuntimeConfig::default(), None).unwrap(),
    )
}

#[test]
#[serial_test::serial]
fn vector_maintenance_deleted_gc_reuses_complete_proof_after_first_batch() {
    use crate::app::storage_api::object_usecase::StorageObjectOptions;
    use crate::app::storage_api::object_usecase::contract::object::ObjectOperations;
    use crate::app::storage_api::vector_usecase::StorageError;
    use std::num::{NonZeroU16, NonZeroUsize};
    use time::{Duration, OffsetDateTime};

    run_large_stack_test("vector-maintenance-deleted-gc-batches", || async {
        let (fixture, executor, catalog, initial, _) = fixture().await;
        let identity = initial.head().resource_identity().clone();
        let bucket = identity.vector_bucket().unwrap().clone();
        let public_name = bucket.public_name().as_str();
        for index in 0..18 {
            fixture
                .execute(Request::PutVectors(
                    serde_json::from_value(json!({
                        "vectorBucketName":public_name,"indexName":"maintenance.index",
                        "vectors":[{"key":format!("key-{index}"),"data":{"float32":[1.0,2.0]}}]
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();
        }
        let (guard, current) = catalog
            .begin_vector_operation(&bucket, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        drop(guard);
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::EnrollCurrent {
                    commit_id: vector_maintenance_enroll_commit_id(
                        &identity,
                        &VectorMaintenanceHeadV1::from_entry(&current).unwrap(),
                    )
                    .unwrap(),
                    expected: Box::new(current),
                },
                fresh_control(),
            )
            .await
            .unwrap();
        let snapshot = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        let resource =
            VectorMaintenanceResourceV1::try_new(&identity, initial.head().namespace(), initial.head().name()).unwrap();
        let executor = Arc::new(executor);
        let compact = VectorMaintenanceSemanticSpec::try_new(
            resource.clone(),
            VectorMaintenanceKind::Compaction,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(snapshot.head().cloned(), None, None).unwrap(),
            executor.source.identity.clone(),
            None,
            executor.source.policy.clone(),
        )
        .unwrap();
        let compact = catalog
            .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(compact).unwrap())
            .await
            .unwrap();
        let result = execute_vector_maintenance_job(
            catalog.as_ref().clone(),
            resource.clone(),
            *compact.job().job_id(),
            VectorMaintenanceWorker::try_new("native-compaction".into(), uuid::Uuid::new_v4(), executor.source.identity.clone())
                .unwrap(),
            VectorMaintenanceExecutionContext {
                identity: executor.source.identity.clone(),
                source: executor.source.clone(),
                executor: executor.clone(),
            },
            fresh_control(),
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded)
        ));
        let compacted = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        let rows = fixture
            .execute(Request::GetVectors(
                serde_json::from_value(json!({
                    "vectorBucketName": public_name, "indexName": "maintenance.index",
                    "keys": (0..18).map(|index| format!("key-{index}")).collect::<Vec<_>>(), "returnData": true
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let crate::vector::protocol::Response::GetVectors(rows) = rows else {
            panic!("the Vector API must return the requested rows after Compaction");
        };
        assert_eq!(rows.vectors.len(), 18, "Compaction must preserve all committed keys");
        assert_ne!(
            compacted.head(),
            snapshot.head(),
            "native Compaction must publish through the authorized Catalog path"
        );
        let guard = catalog.begin_vector_maintenance_resource_operation(&compacted).await.unwrap();
        let (bound, adapter) = executor
            .preparation_adapter(
                catalog.clone(),
                guard.clone(),
                uuid::Uuid::new_v4(),
                crate::app::object::ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
                fresh_control(),
            )
            .await
            .unwrap();
        let history = bound.read_vector_maintenance_history_with_guard(&guard).await.unwrap();
        let refs = adapter
            .collect_references(history[0].metadata(), history[0].format_descriptor())
            .await
            .unwrap();
        assert!(refs.len() > 32, "the actual Lance graph must require multiple deletion batches");
        drop(adapter);
        drop(bound);
        drop(guard);

        // Backdate native metadata without changing any referenced bytes or body identity.
        let native = fixture.app.object_store();
        let old_time = OffsetDateTime::now_utc() - Duration::seconds(1800);
        for reference in &refs {
            let key = reference.object_key().as_str();
            let mut info = native
                .get_object_info(identity.storage_bucket_id(), key, &StorageObjectOptions::default())
                .await
                .unwrap();
            let body_revision = info.data_dir;
            info.metadata_only = true;
            info.mod_time = Some(old_time);
            let changed = native
                .copy_object(
                    identity.storage_bucket_id(),
                    key,
                    identity.storage_bucket_id(),
                    key,
                    &mut info,
                    &StorageObjectOptions::default(),
                    &StorageObjectOptions {
                        mod_time: Some(old_time),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(changed.data_dir, body_revision, "timestamp injection must retain the referenced body");
            assert_eq!(changed.mod_time, Some(old_time));
        }
        fixture
            .execute(Request::DeleteIndex(
                serde_json::from_value(json!({
                    "vectorBucketName":public_name,"indexName":"maintenance.index"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let snapshot = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        assert_eq!(snapshot.state(), VectorMaintenanceResourceState::Deleted);
        let resource =
            VectorMaintenanceResourceV1::try_new(&identity, initial.head().namespace(), initial.head().name()).unwrap();
        let expected_head = snapshot.head().unwrap().clone();
        let mut completed_batches = 0;
        let mut removed = 0;
        for _ in 0..8 {
            executor
                .prepare(
                    catalog.clone(),
                    VectorMaintenancePreparationAction::PrepareGc {
                        resource: resource.clone(),
                        kind: VectorMaintenanceKind::HistoryGc,
                        expected_head: Box::new(expected_head.clone()),
                    },
                    fresh_control(),
                )
                .await
                .expect("later GC batches must retain the original complete reference proof");
            let budget = VectorMaintenanceDiscoveryBudget::new(
                CancellationToken::new(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                NonZeroUsize::new(64).unwrap(),
            );
            let (jobs, continuation) = catalog
                .list_vector_maintenance_jobs(&identity, None, NonZeroU16::new(64).unwrap(), &budget)
                .await
                .unwrap()
                .into_parts();
            assert!(continuation.is_none(), "this fixture must fit in one job discovery page");
            for job in jobs
                .into_iter()
                .filter(|job| *job.job().state() == VectorMaintenanceJobState::Queued)
            {
                let result = execute_vector_maintenance_job(
                    catalog.as_ref().clone(),
                    resource.clone(),
                    *job.job().job_id(),
                    VectorMaintenanceWorker::try_new(
                        "gc-batch-test".into(),
                        uuid::Uuid::new_v4(),
                        executor.source.identity.clone(),
                    )
                    .unwrap(),
                    VectorMaintenanceExecutionContext {
                        identity: executor.source.identity.clone(),
                        source: executor.source.clone(),
                        executor: executor.clone(),
                    },
                    fresh_control(),
                )
                .await
                .unwrap();
                assert!(matches!(
                    result,
                    VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded)
                ));
                completed_batches += 1;
            }
            removed = 0;
            for reference in &refs {
                match native
                    .get_object_info(
                        identity.storage_bucket_id(),
                        reference.object_key().as_str(),
                        &StorageObjectOptions::default(),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(StorageError::ObjectNotFound(_, _)) => removed += 1,
                    Err(error) => panic!("unexpected native observation error: {error}"),
                }
            }
            if completed_batches == 1 {
                assert!(
                    removed > 0 && removed < refs.len(),
                    "the first physical deletion must precede a later proof preparation"
                );
            }
            if removed == refs.len() {
                break;
            }
        }
        assert!(completed_batches >= 2, "the real worker must complete multiple retirement plans");
        assert_eq!(removed, refs.len(), "every unprotected historical file must be physically removed");
    });
}

#[test]
#[serial_test::serial]
fn vector_maintenance_gc_planning_rechecks_changed_resource_tags() {
    run_large_stack_test("vector-maintenance-gc-tag-policy", || async {
        let (fixture, executor, catalog, initial, _) = fixture().await;
        let identity = initial.head().resource_identity().clone();
        let public_name = identity.vector_bucket().unwrap().public_name().as_str();
        let index = fixture
            .execute(Request::GetIndex(
                serde_json::from_value(json!({
                    "vectorBucketName": public_name, "indexName": "maintenance.index"
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        let crate::vector::protocol::Response::GetIndex(index) = index else {
            panic!("the fixture must expose its actual Index ARN");
        };
        fixture
            .execute(Request::TagResource(
                serde_json::from_value(json!({
                    "resourceArn": index.index.index_arn, "tags": {"project":"search"}
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:TagResource","Resource":"*"},
                {"Effect":"Allow","Action":["s3vectors:GetVectors","s3vectors:DeleteVectors"],"Resource":"*",
                 "Condition":{"StringEquals":{"aws:ResourceTag/project":"search"}}},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]))
            .await;
        let snapshot = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        let guard = catalog.begin_vector_maintenance_resource_operation(&snapshot).await.unwrap();
        let (_bound, adapter) = executor
            .preparation_adapter(
                catalog.clone(),
                guard.clone(),
                uuid::Uuid::new_v4(),
                crate::app::object::ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
                fresh_control(),
            )
            .await
            .unwrap();
        adapter
            .collect_references(initial.head().metadata(), initial.head().format_descriptor())
            .await
            .expect("current resource tags authorize real managed reads");
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fixture.execute(Request::TagResource(
                serde_json::from_value(json!({"resourceArn":index.index.index_arn,"tags":{"project":"revoked"}})).unwrap(),
            )),
        )
        .await
        .expect("tag mutation must complete while GC planning holds its shared protection")
        .unwrap();
        let error = adapter
            .collect_references(initial.head().metadata(), initial.head().format_descriptor())
            .await
            .expect_err("the same preparation must reject reads after its resource-tag condition is revoked");
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut denied = false;
        while let Some(current) = source {
            let api = match current.downcast_ref::<ManagedObjectError>() {
                Some(ManagedObjectError::Api(api)) => Some(api),
                _ => current.downcast_ref::<ApiError>(),
            };
            if let Some(api) = api {
                denied |= api.code == rustfs_s3_types::S3ErrorCode::AccessDenied;
            }
            source = current.source();
        }
        assert!(denied, "the actual failure must retain the IAM denial: {error:?}");
        guard.check_held().unwrap();
    });
}

#[path = "pagination_session_tests.rs"]
mod pagination_session_tests;
