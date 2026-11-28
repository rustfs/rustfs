// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{storage::helper::MutationCompletionProbe, table_catalog::VectorMaintenanceExecutionIdentityV1};

async fn create_bucket(fixture: &Fixture, name: &str) -> BucketHandle {
    fixture
        .execute(Request::CreateVectorBucket(
            serde_json::from_value(json!({"vectorBucketName":name,"tags":{"team":"search"}}))
                .expect("valid tagged bucket request"),
        ))
        .await
        .expect("create the actual managed bucket");
    resolve_bucket(fixture, name).await
}

async fn resolve_bucket(fixture: &Fixture, name: &str) -> BucketHandle {
    fixture
        .service
        .buckets()
        .resolve_bucket_handle(
            BucketKind::Vector,
            &AccountNamespaceId::try_new(String::new()).expect("deployment account namespace"),
            &fixture.service.region,
            &BucketPublicName::try_new(name.to_owned()).expect("valid generated public name"),
        )
        .await
        .expect("resolve the actual managed bucket identity")
}

async fn metadata_bytes(fixture: &Fixture, bucket: &BucketHandle) -> Vec<u8> {
    let buckets = fixture.service.buckets();
    let guard = buckets
        .acquire_bucket_handle_guard(bucket)
        .await
        .expect("retain current lifecycle reader");
    buckets
        .read_managed_bucket_metadata(bucket, &guard)
        .await
        .expect("read original native bucket metadata under its lifecycle reader")
        .marshal_msg()
        .expect("encode the actual native metadata")
}

async fn execution_identity(fixture: &Fixture) -> VectorMaintenanceExecutionIdentityV1 {
    let (identity, _) = maintenance::resolve_identity(&fixture.app, &fixture.credentials.access_key)
        .await
        .expect("resolve the current ordinary IAM identity");
    VectorMaintenanceExecutionIdentityV1::try_new(identity.account().into(), *identity.user_generation())
        .expect("retain actual account and generation")
}

async fn assert_denied_audit(
    probe: &MutationCompletionProbe,
    fixture: &Fixture,
    bucket: &str,
    previous_audits: usize,
    previous_events: usize,
    error: &ProtocolError,
) {
    assert_eq!(error.code(), ErrorCode::AccessDenied);
    assert_eq!(error.status_code(), http::StatusCode::FORBIDDEN);
    probe.wait_for(previous_audits + 1, previous_events).await;
    let audits = probe.audits.lock().expect("actual mutation audit records");
    assert_eq!(audits.len(), previous_audits + 1, "one completion for the rejected captured request");
    let denied = &audits[previous_audits];
    assert_eq!(denied.api.name.as_deref(), Some("DeleteVectorBucket"));
    assert_eq!(denied.api.bucket.as_deref(), Some(bucket));
    assert_eq!(denied.access_key.as_deref(), Some(fixture.credentials.access_key.as_str()));
    assert_eq!(denied.api.status.as_deref(), Some("failure"));
    assert_eq!(denied.api.status_code, Some(403));
    assert_eq!(denied.error.as_deref(), Some(error.to_string().as_str()));
    assert_eq!(probe.events.lock().expect("actual mutation events").len(), previous_events);
}

#[test]
#[serial_test::serial]
fn catalog_worker_replay_rechecks_deleted_user_with_denied_audit() {
    run_large_stack_test("catalog-worker-replay-deleted-user", || async {
        let fixture = Fixture::new().await;
        let name = format!("vectors-{}", Uuid::new_v4().simple());
        let audit = MutationCompletionProbe::install(&name);
        let bucket = create_bucket(&fixture, &name).await;
        audit.wait_for(1, 0).await;
        let events = audit.events.lock().expect("setup events").len();
        let identity = execution_identity(&fixture).await;
        let before = metadata_bytes(&fixture, &bucket).await;
        let replay = catalog_worker_capture_bucket_delete(fixture.service.server.clone(), &bucket, &identity)
            .await
            .expect("capture actual ordinary deletion authorization");
        assert_eq!(audit.audits.lock().expect("pending capture audit").len(), 1);
        fixture
            .app
            .iam()
            .handle()
            .delete_user(&fixture.credentials.access_key, false)
            .await
            .expect("revoke the captured ordinary account");
        let error = replay
            .execute(&identity)
            .await
            .expect_err("revoked identity cannot execute the captured deletion");
        assert_denied_audit(&audit, &fixture, &name, 1, events, &error).await;
        assert_eq!(resolve_bucket(&fixture, &name).await, bucket);
        assert_eq!(metadata_bytes(&fixture, &bucket).await, before, "denial preserves native metadata bytes");
    });
}

#[test]
#[serial_test::serial]
fn catalog_worker_replay_rechecks_recreated_user_generation_with_denied_audit() {
    run_large_stack_test("catalog-worker-replay-user-generation", || async {
        let fixture = Fixture::new().await;
        let name = format!("vectors-{}", Uuid::new_v4().simple());
        let audit = MutationCompletionProbe::install(&name);
        let bucket = create_bucket(&fixture, &name).await;
        audit.wait_for(1, 0).await;
        let events = audit.events.lock().expect("setup events").len();
        let identity = execution_identity(&fixture).await;
        let before = metadata_bytes(&fixture, &bucket).await;
        let replay = catalog_worker_capture_bucket_delete(fixture.service.server.clone(), &bucket, &identity)
            .await
            .expect("capture actual ordinary deletion authorization");
        assert_eq!(audit.audits.lock().expect("pending capture audit").len(), 1);
        let iam = fixture.app.iam().handle();
        iam.delete_user(&fixture.credentials.access_key, false)
            .await
            .expect("remove the original account generation");
        iam.create_user(
            &fixture.credentials.access_key,
            &AddOrUpdateUserReq {
                secret_key: fixture.credentials.secret_key.clone(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("recreate the same enabled account and credential");
        iam.policy_db_set(&fixture.credentials.access_key, UserType::Reg, false, &fixture.policy)
            .await
            .expect("restore the original existing Vector policy");
        let replacement = execution_identity(&fixture).await;
        assert_eq!(replacement.account(), identity.account());
        assert_ne!(
            replacement.user_generation(),
            identity.user_generation(),
            "recreation must change actual IAM generation"
        );
        let error = replay
            .execute(&identity)
            .await
            .expect_err("original capture cannot execute as a new account generation");
        assert_denied_audit(&audit, &fixture, &name, 1, events, &error).await;
        assert_eq!(resolve_bucket(&fixture, &name).await, bucket);
        assert_eq!(
            metadata_bytes(&fixture, &bucket).await,
            before,
            "generation denial preserves native metadata bytes"
        );
    });
}

#[test]
#[serial_test::serial]
fn catalog_worker_capture_rejects_old_bucket_incarnation_with_denied_audit() {
    run_large_stack_test("catalog-worker-capture-bucket-incarnation", || async {
        let fixture = Fixture::new().await;
        let name = format!("vectors-{}", Uuid::new_v4().simple());
        let audit = MutationCompletionProbe::install(&name);
        let previous = create_bucket(&fixture, &name).await;
        audit.wait_for(1, 0).await;
        let deleted = fixture
            .execute(Request::DeleteVectorBucket(
                serde_json::from_value(json!({"vectorBucketName":name})).expect("valid original deletion request"),
            ))
            .await
            .expect("delete the original empty bucket through the actual use case");
        assert!(matches!(deleted, Response::DeleteVectorBucket(_)));
        audit.wait_for(2, 0).await;
        let replacement = create_bucket(&fixture, &name).await;
        audit.wait_for(3, 0).await;
        let events = audit.events.lock().expect("setup events").len();
        assert_eq!(replacement.public_name(), previous.public_name());
        assert_eq!(replacement.storage_bucket_id(), previous.storage_bucket_id());
        assert_ne!(replacement.incarnation(), previous.incarnation());
        let identity = execution_identity(&fixture).await;
        let before = metadata_bytes(&fixture, &replacement).await;
        let Err(error) = catalog_worker_capture_bucket_delete(fixture.service.server.clone(), &previous, &identity).await else {
            panic!("capture must reject the supplied previous incarnation after current-name admission");
        };
        assert_denied_audit(&audit, &fixture, &name, 3, events, &error).await;
        assert_eq!(resolve_bucket(&fixture, &name).await, replacement);
        assert_eq!(
            metadata_bytes(&fixture, &replacement).await,
            before,
            "capture denial preserves replacement metadata bytes"
        );
    });
}
