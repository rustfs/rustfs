// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::app::storage_api::object_usecase::{StorageObjectOptions, contract::object::ObjectOperations};
use crate::app::vector_usecase::{DefaultVectorUsecase, dataset::IndexRequest};
use crate::vector::pagination::{PaginationSession, ParsedToken, SessionBinding, SessionResults, SessionState};
use crate::vector::protocol::{ErrorCode, Response};
use crate::vector::service::VectorRequestAccess;
use std::num::{NonZeroU16, NonZeroUsize};
use time::Duration;

fn query(bucket: &str, token: Option<String>) -> Request {
    Request::QueryVectors(
        serde_json::from_value(json!({
            "vectorBucketName":bucket,"indexName":"maintenance.index","topK":205,
            "queryVector":{"float32":[0.0,0.0]},"returnDistance":true,"nextToken":token
        }))
        .unwrap(),
    )
}

async fn another_service_page(fixture: &Fixture, request: Request) -> crate::vector::protocol::dto::QueryVectorsOutput {
    let service =
        DefaultVectorUsecase::new(fixture.service.server.clone(), fixture.service.limits, NonZeroUsize::new(3).unwrap()).unwrap();
    let authorization = service.admit(fixture.verified(request.clone())).await.unwrap();
    let Response::QueryVectors(page) = service.execute_authorized(authorization, request).await.unwrap() else {
        panic!("query response");
    };
    page
}

#[test]
#[serial_test::serial]
fn native_pagination_rechecks_iam_and_retires_only_expired_sessions() {
    run_large_stack_test("native-pagination-session", || async {
        let (fixture, executor, catalog, initial, _) = fixture().await;
        let identity = initial.head().resource_identity().clone();
        let bucket = identity.vector_bucket().unwrap();
        let public = bucket.public_name().as_str();
        fixture
            .execute(Request::PutVectors(
                serde_json::from_value(json!({
                    "vectorBucketName":public,"indexName":"maintenance.index",
                    "vectors":(0..205).map(|i| json!({"key":format!("row-{i:03}"),"data":{"float32":[i,0]}})).collect::<Vec<_>>()
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .execute(Request::PutVectors(
                serde_json::from_value(json!({
                    "vectorBucketName":public,"indexName":"maintenance.index",
                    "vectors":[{"key":"row-204","data":{"float32":[204.0,0.0]}}]
                }))
                .unwrap(),
            ))
            .await
            .unwrap();
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:QueryVectors","Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]))
            .await;
        let first = another_service_page(&fixture, query(public, None)).await;
        assert_eq!(first.vectors.len(), 100);
        let token = first.next_token.unwrap();
        let session_id = ParsedToken::parse(&token).unwrap().session_id().to_owned();
        let live_key = format!(".rustfs-table/{}/query-results/{session_id}.session", identity.table_id().as_str());
        let next = another_service_page(&fixture, query(public, Some(token.clone()))).await;
        assert_eq!(next.vectors[0].key, "row-100");
        fixture
            .set_statements(json!([
                {"Effect":"Deny","Action":"s3vectors:QueryVectors","Resource":"*"}
            ]))
            .await;
        assert_eq!(
            fixture.execute(query(public, Some(token.clone()))).await.unwrap_err().code(),
            ErrorCode::AccessDenied
        );
        fixture
            .set_statements(json!([
                {"Effect":"Allow","Action":"s3vectors:*","Resource":"*"},
                {"Effect":"Deny","Action":"s3:*","Resource":"*"}
            ]))
            .await;

        // Persist an already expired session through the same restricted, audited object I/O.
        let issued_at = OffsetDateTime::now_utc().unix_timestamp() - 7200;
        let expired = PaginationSession::new(SessionState {
            binding: SessionBinding {
                resource: [1; 32],
                principal: [2; 32],
                request: [3; 32],
            },
            snapshot_version: 1,
            snapshot_digest: [4; 32],
            issued_at,
            expires_at: issued_at + 900,
            results: SessionResults::List,
        })
        .unwrap();
        let expired_key = format!(".rustfs-table/{}/query-results/{}.session", identity.table_id().as_str(), expired.id());
        {
            let admitted = fixture.service.admit(fixture.verified(query(public, None))).await.unwrap();
            let (guard, entry) = catalog
                .begin_vector_operation(bucket, initial.head().namespace(), initial.head().name())
                .await
                .unwrap();
            let index = CatalogName::parse("maintenance.index").unwrap();
            let context = IndexRequest {
                owner: &fixture.service,
                authorization: &admitted,
                index: &index,
            };
            let managed = context
                .managed_query_results(Arc::new(guard), entry.definition())
                .await
                .unwrap();
            managed
                .objects
                .create_only(
                    &managed.access,
                    &CatalogObjectKey::try_new(expired_key.clone()).unwrap(),
                    bytes::Bytes::from_owner(expired.encode().unwrap()),
                )
                .await
                .unwrap();
        }
        // The injected age is observable in native metadata and preserves the exact body revision.
        let native = fixture.app.object_store();
        let mut info = native
            .get_object_info(identity.storage_bucket_id(), &expired_key, &StorageObjectOptions::default())
            .await
            .unwrap();
        let body_revision = info.data_dir;
        let old_time = OffsetDateTime::now_utc() - Duration::seconds(7200);
        info.metadata_only = true;
        info.mod_time = Some(old_time);
        let aged = native
            .copy_object(
                identity.storage_bucket_id(),
                &expired_key,
                identity.storage_bucket_id(),
                &expired_key,
                &mut info,
                &StorageObjectOptions::default(),
                &StorageObjectOptions {
                    mod_time: Some(old_time),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(aged.mod_time, Some(old_time));
        assert_eq!(aged.data_dir, body_revision);

        let (guard, current) = catalog
            .begin_vector_operation(bucket, initial.head().namespace(), initial.head().name())
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
        let resource =
            VectorMaintenanceResourceV1::try_new(&identity, initial.head().namespace(), initial.head().name()).unwrap();
        let snapshot = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        let executor = Arc::new(executor);
        let compaction = VectorMaintenanceSemanticSpec::try_new(
            resource.clone(),
            VectorMaintenanceKind::Compaction,
            VectorMaintenanceOrigin::Periodic,
            VectorMaintenanceInputV1::try_new(snapshot.head().cloned(), None, None).unwrap(),
            executor.source.identity.clone(),
            None,
            executor.source.policy.clone(),
        )
        .unwrap();
        let compaction = catalog
            .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(compaction).unwrap())
            .await
            .unwrap();
        let outcome = execute_vector_maintenance_job(
            catalog.as_ref().clone(),
            resource.clone(),
            *compaction.job().job_id(),
            VectorMaintenanceWorker::try_new(
                "pagination-compaction".into(),
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
            outcome,
            VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded)
        ));
        let compacted = catalog
            .read_vector_maintenance_resource(&identity, initial.head().namespace(), initial.head().name())
            .await
            .unwrap();
        assert_ne!(
            compacted.head(),
            snapshot.head(),
            "compaction must publish a different committed manifest"
        );
        let snapshot = compacted;
        executor
            .prepare(
                catalog.clone(),
                VectorMaintenancePreparationAction::PrepareGc {
                    resource: resource.clone(),
                    kind: VectorMaintenanceKind::OrphanGc,
                    expected_head: Box::new(snapshot.head().unwrap().clone()),
                },
                fresh_control(),
            )
            .await
            .unwrap();
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
        assert!(continuation.is_none());
        let jobs: Vec<_> = jobs
            .into_iter()
            .filter(|job| *job.job().state() == VectorMaintenanceJobState::Queued)
            .collect();
        assert_eq!(jobs.len(), 1, "exactly one expired session must produce a retirement job");
        let outcome = execute_vector_maintenance_job(
            catalog.as_ref().clone(),
            resource,
            *jobs[0].job().job_id(),
            VectorMaintenanceWorker::try_new("pagination-gc".into(), uuid::Uuid::new_v4(), executor.source.identity.clone())
                .unwrap(),
            VectorMaintenanceExecutionContext {
                identity: executor.source.identity.clone(),
                source: executor.source.clone(),
                executor,
            },
            fresh_control(),
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            VectorMaintenanceWorkerResult::Finished(VectorMaintenanceJobState::Succeeded)
        ));
        assert!(matches!(
            native
                .get_object_info(identity.storage_bucket_id(), &expired_key, &StorageObjectOptions::default())
                .await,
            Err(crate::app::storage_api::vector_usecase::StorageError::ObjectNotFound(_, _))
        ));
        native
            .get_object_info(identity.storage_bucket_id(), &live_key, &StorageObjectOptions::default())
            .await
            .unwrap();
        let next = another_service_page(&fixture, query(public, Some(token))).await;
        assert_eq!(next.vectors[0].key, "row-100");
    });
}
