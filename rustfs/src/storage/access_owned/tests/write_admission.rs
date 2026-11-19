// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Write and read admission on the owned request: the action a DeleteObject version selects and
//! the bucket-policy tag hint it implies, the PostObject marker, the write-offset refusal that
//! precedes PutObject authorization, the on-demand-migration source capture the read hooks
//! defer to the use case, and the PutObject order between a policy refusal and a generation
//! read-quorum failure.
//!
//! Not covered here: the policy evaluator itself, request-body decoding, or any legacy request
//! type.
//! Upstream: `access_owned::{object_read, object_write, listing_multipart}`, the authorization
//! guards and `storage::authorization::tests::fixture`; downstream: none.

use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::object_usecase::{DefaultObjectUsecase, GetObjectFields, HeadObjectFields};
use crate::storage::StorageError;
use crate::storage::access_owned::listing_multipart::authorize_list_objects_v2;
use crate::storage::access_owned::object_read::{authorize_get_object, authorize_head_object};
use crate::storage::access_owned::object_write::{PutObjectAuthorization, authorize_post_object, authorize_put_object};
use crate::storage::authorization::tests::fixture::{request, server_context};
use crate::storage::authorization::{
    AMZ_WRITE_OFFSET_BYTES_HEADER, BucketPolicyExistingObjectTagHint, OdmReadGenerationGuard, PUT_GENERATION_READ_QUORUM_FAILURE,
    bucket_policy_needs_existing_object_tag_from_hint,
};
use crate::storage::authorization_actions::delete_object_authorize_action;
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::Method;
use rustfs_gateway_types::dto as gateway_dto;
use rustfs_gateway_types::{BucketName, ByteRange};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{BucketPolicy, BucketPolicyArgs};
use rustfs_s3_types::S3ErrorCode;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn delete_authorization_selects_the_addressed_version() {
    assert_eq!(delete_object_authorize_action(None), Action::S3Action(S3Action::DeleteObjectAction));
    for version_id in ["null", "8f418ad0-f9f4-4458-83b2-cc72bc6f1b70"] {
        assert_eq!(
            delete_object_authorize_action(Some(version_id)),
            Action::S3Action(S3Action::DeleteObjectVersionAction)
        );
    }
}

#[tokio::test]
async fn test_anonymous_version_delete_uses_version_policy_and_tag_hint() {
    let policy: BucketPolicy = serde_json::from_str(
        r#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:DeleteObject"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:DeleteObjectVersion"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
    )
    .expect("bucket policy should parse");
    let hint = BucketPolicyExistingObjectTagHint::Parsed(policy.clone());
    let no_groups: Option<Vec<String>> = None;
    let conditions = HashMap::new();

    let args_delete = BucketPolicyArgs {
        bucket: "bucket",
        action: Action::S3Action(S3Action::DeleteObjectAction),
        is_owner: false,
        account: "",
        groups: &no_groups,
        conditions: &conditions,
        object: "obj",
    };
    assert!(
        policy.is_allowed(&args_delete).await,
        "anonymous DeleteObject can be allowed by bucket policy"
    );

    let args_delete_version = BucketPolicyArgs {
        bucket: "bucket",
        action: Action::S3Action(S3Action::DeleteObjectVersionAction),
        is_owner: false,
        account: "",
        groups: &no_groups,
        conditions: &conditions,
        object: "obj",
    };
    assert!(
        !policy.is_allowed(&args_delete_version).await,
        "DeleteObjectVersion should still be denied without matching ExistingObjectTag conditions"
    );

    let needs_tag_current = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete).await;
    let needs_tag_version = bucket_policy_needs_existing_object_tag_from_hint(&hint, &args_delete_version).await;
    assert!(!needs_tag_current, "DeleteObject statement itself does not require ExistingObjectTag");
    assert!(
        needs_tag_version,
        "DeleteObjectVersion statement requires ExistingObjectTag when version delete is evaluated"
    );
    assert!(
        needs_tag_version,
        "the selected version action must require tag fetch for DeleteObject(versionId)"
    );
}

#[tokio::test]
async fn post_object_marks_request_extensions() {
    let mut req = request(Method::POST).req_info().build();

    let _ = authorize_post_object(&mut req, "test-bucket", "test-key", None).await;

    assert!(
        req.post_object().is_some(),
        "post object request should carry the marker for downstream handling"
    );
}

#[tokio::test]
async fn put_object_rejects_write_offset_bytes_before_authorization() {
    let mut req = request(Method::PUT)
        .uri("/test-bucket/test-key")
        .header(AMZ_WRITE_OFFSET_BYTES_HEADER, "0")
        .req_info()
        .build();

    let server = ServerContextSlot::new();
    let err = authorize_put_object(&mut req, &server, put_object_authorization("test-bucket", "test-key"), |_| Ok(()))
        .await
        .expect_err("write-offset-bytes requests should be rejected");

    assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    // The generic NotImplemented message (`ApiError::error_code_to_message(NotImplemented)`).
    assert_eq!(err.message(), Some("A header you provided implies functionality that is not implemented"));

    let req_info = req.req_info().expect("request info should remain available");
    assert_eq!(req_info.bucket.as_deref(), Some("test-bucket"));
    assert_eq!(req_info.object.as_deref(), Some("test-key"));
}

/// The facts the PutObject hook derives from an input that sets nothing beyond bucket and key.
fn put_object_authorization<'a>(bucket: &'a str, key: &'a str) -> PutObjectAuthorization<'a> {
    PutObjectAuthorization {
        bucket,
        key,
        version_id: None,
        retain_until: None,
        specific_etag: false,
        legal_hold: false,
        retention: false,
        post: false,
    }
}

/// An owner request as the ODM read tests authorize it.
fn odm_owner_request(method: Method, slot: &Arc<ServerContextSlot>) -> RequestEnvelope {
    request(method)
        .extension(ReqInfo {
            cred: Some(rustfs_credentials::Credentials::default()),
            is_owner: true,
            ..Default::default()
        })
        .server_context(slot)
        .build()
}

fn get_fields(bucket: &str, key: &str, part_number: Option<i32>) -> GetObjectFields {
    GetObjectFields {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        version_id: None,
        part_number,
        range: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
    }
}

fn head_fields(bucket: &str, key: &str, part_number: Option<i32>, range: Option<ByteRange>) -> HeadObjectFields {
    HeadObjectFields {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        version_id: None,
        part_number,
        range,
        sse_customer_key: None,
        sse_customer_key_md5: None,
    }
}

fn list_input(bucket: &str) -> gateway_dto::ListObjectsV2Input {
    gateway_dto::ListObjectsV2Input {
        bucket: BucketName::new(bucket).expect("test bucket name"),
        ..Default::default()
    }
}

#[test]
#[serial]
fn odm_read_capture_preserves_access_and_parameter_error_order() {
    crate::app::gating_test_env::run_large_stack_test("odm-access-order", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
        let enabled_before = sys.is_module_enabled();
        let bucket = format!("odm-access-missing-{}", uuid::Uuid::new_v4());
        for enabled in [false, true] {
            sys.set_module_enabled(enabled);
            let mut get = odm_owner_request(Method::GET, &server_ctx);
            let mut head = odm_owner_request(Method::HEAD, &server_ctx);
            let mut list = odm_owner_request(Method::GET, &server_ctx);
            authorize_get_object(&server_ctx, &mut get, &bucket, "key", None)
                .await
                .expect("ordinary GET access must not require bucket identity");
            authorize_head_object(&server_ctx, &mut head, &bucket, "key", None)
                .await
                .expect("ordinary HEAD access must not require bucket identity");
            authorize_list_objects_v2(&server_ctx, &mut list, &bucket)
                .await
                .expect("ordinary LIST access must not require bucket identity");
            assert!(matches!(get.odm_read_generation(), Some(OdmReadGenerationGuard::Unavailable)));
            assert!(matches!(head.odm_read_generation(), Some(OdmReadGenerationGuard::Unavailable)));
            assert!(matches!(list.odm_read_generation(), Some(OdmReadGenerationGuard::Unavailable)));
            let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
            assert_eq!(
                usecase
                    .execute_get_object_fields(get, get_fields(&bucket, "key", Some(0)))
                    .await
                    .expect_err("bad GET part number")
                    .code(),
                &S3ErrorCode::InvalidArgument
            );
            assert_eq!(
                usecase
                    .execute_head_object_fields(
                        head,
                        head_fields(&bucket, "key", Some(1), Some(ByteRange::FromTo { first: 0, last: 1 }))
                    )
                    .await
                    .expect_err("range and part number conflict")
                    .code(),
                &S3ErrorCode::InvalidArgument
            );
            assert_eq!(
                DefaultBucketUsecase::with_context(Some(Arc::clone(&context)))
                    .execute_list_objects_v2(
                        list,
                        gateway_dto::ListObjectsV2Input {
                            max_keys: Some(-1),
                            ..list_input(&bucket)
                        }
                    )
                    .await
                    .expect_err("negative max keys")
                    .code(),
                &S3ErrorCode::InvalidArgument
            );
        }
        sys.set_module_enabled(enabled_before);
    });
}

#[test]
#[serial]
fn odm_read_capture_failure_is_deferred_until_source_miss() {
    crate::app::gating_test_env::run_large_stack_test("odm-access-failure", || async {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = context.object_store();
        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::clone(&context)));
        let sys = crate::on_demand_migration::OnDemandMigrationSys::get();
        let enabled_before = sys.is_module_enabled();
        sys.set_module_enabled(true);
        let bucket = format!("odm-capture-failure-{}", uuid::Uuid::new_v4());
        let mut config: crate::on_demand_migration::OnDemandMigrationConfig = serde_json::from_str(r#"{"source":{"provider":"minio","endpoint":"https://source.example.com","region":"us-east-1","bucket":"source","credentials":{"access_key":"test","secret_key":"test"}}}"#).expect("source config");
        config.policy.list_through = true;
        sys.apply_for_incarnation(&bucket, uuid::Uuid::new_v4(), Some(&config)).await;
        let mut get = odm_owner_request(Method::GET, &server_ctx);
        let mut head = odm_owner_request(Method::HEAD, &server_ctx);
        let mut list = odm_owner_request(Method::GET, &server_ctx);
        authorize_list_objects_v2(&server_ctx, &mut list, &bucket)
            .await
            .expect("capture failure must not preempt LIST authorization");
        authorize_get_object(&server_ctx, &mut get, &bucket, "local", None)
            .await
            .expect("capture failure must not preempt GET authorization");
        authorize_head_object(&server_ctx, &mut head, &bucket, "local", None)
            .await
            .expect("capture failure must not preempt HEAD authorization");
        assert!(matches!(get.odm_read_generation(), Some(OdmReadGenerationGuard::Failed { .. })));
        assert!(matches!(head.odm_read_generation(), Some(OdmReadGenerationGuard::Failed { .. })));
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create bucket after capture");
        store
            .put_object(
                &bucket,
                "local",
                &mut crate::storage::PutObjReader::from_vec(b"local".to_vec()),
                &crate::storage::ObjectOptions::default(),
            )
            .await
            .expect("create local hit");
        // Publishing a usable source later must not repair a failed capture.
        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("created identity");
        sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
        let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let bucket_usecase = || DefaultBucketUsecase::with_context(Some(Arc::clone(&context)));
        usecase
            .execute_get_object_fields(get.fork_for_authorization(), get_fields(&bucket, "local", None))
            .await
            .expect("a local GET hit ignores source capture failure");
        usecase
            .execute_head_object_fields(head.fork_for_authorization(), head_fields(&bucket, "local", None, None))
            .await
            .expect("a local HEAD hit ignores source capture failure");
        assert_eq!(
            usecase
                .execute_get_object_fields(get.fork_for_authorization(), get_fields(&bucket, "missing", None))
                .await
                .expect_err("failed capture cannot rebind on GET miss")
                .code(),
            &S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            usecase
                .execute_head_object_fields(head.fork_for_authorization(), head_fields(&bucket, "missing", None, None))
                .await
                .expect_err("failed capture cannot rebind on HEAD miss")
                .code(),
            &S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            bucket_usecase()
                .execute_list_objects_v2(list.fork_for_authorization(), list_input(&bucket))
                .await
                .expect_err("failed capture cannot rebind on LIST")
                .code(),
            &S3ErrorCode::NoSuchBucket
        );
        config.filter.prefix = Some("remote/".into());
        sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
        assert_eq!(
            usecase
                .execute_get_object_fields(get.fork_for_authorization(), get_fields(&bucket, "missing", None))
                .await
                .expect_err("filtered GET remains local")
                .code(),
            &S3ErrorCode::NoSuchKey
        );
        assert_eq!(
            usecase
                .execute_head_object_fields(head.fork_for_authorization(), head_fields(&bucket, "missing", None, None))
                .await
                .expect_err("filtered HEAD remains local")
                .code(),
            &S3ErrorCode::NoSuchKey
        );
        let listing = bucket_usecase()
            .execute_list_objects_v2(
                list.fork_for_authorization(),
                gateway_dto::ListObjectsV2Input {
                    prefix: Some("local/".into()),
                    ..list_input(&bucket)
                },
            )
            .await
            .expect("disjoint prefix needs no source identity");
        assert_eq!(listing.key_count, 0);
        let listing = bucket_usecase()
            .execute_list_objects_v2(
                list,
                gateway_dto::ListObjectsV2Input {
                    prefix: Some("remote/".into()),
                    max_keys: Some(0),
                    ..list_input(&bucket)
                },
            )
            .await
            .expect("an empty page needs no source identity");
        assert_eq!(listing.key_count, 0);

        config.filter.prefix = None;
        config.policy.head = crate::on_demand_migration::HeadPolicy::LocalOnly;
        sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
        assert_eq!(
            usecase
                .execute_head_object_fields(head.fork_for_authorization(), head_fields(&bucket, "missing", None, None))
                .await
                .expect_err("local-only HEAD needs no source identity")
                .code(),
            &S3ErrorCode::NoSuchKey
        );
        config.policy.head = crate::on_demand_migration::HeadPolicy::Proxy;
        sys.apply_for_incarnation(&bucket, incarnation, Some(&config)).await;
        store
            .delete_object(
                &bucket,
                "missing",
                crate::storage::ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create local delete marker");
        assert_eq!(
            usecase
                .execute_get_object_fields(get, get_fields(&bucket, "missing", None))
                .await
                .expect_err("GET respects the delete marker before capture failure")
                .code(),
            &S3ErrorCode::NoSuchKey
        );
        assert_eq!(
            usecase
                .execute_head_object_fields(head, head_fields(&bucket, "missing", None, None))
                .await
                .expect_err("HEAD respects the delete marker before capture failure")
                .code(),
            &S3ErrorCode::NoSuchKey
        );
        sys.remove(&bucket);
        sys.set_module_enabled(enabled_before);
    });
}

#[tokio::test]
#[serial]
async fn put_read_quorum_access_keeps_authorization_precedence() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = server_context(Arc::clone(&store));

    for effect in ["Allow", "Deny"] {
        let bucket = format!("put-quorum-access-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("authorization precedence bucket must be created");
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"{effect}","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
        );
        let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
            .await
            .expect("test bucket metadata must be cached"))
        .clone();
        metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test policy must parse"));
        metadata.policy_config_json = policy_json.into_bytes();
        crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
            .await
            .expect("test policy must be installed");

        let mut req = request(Method::PUT).req_info().server_context(&server_ctx).build();
        let err = PUT_GENERATION_READ_QUORUM_FAILURE
            .scope(
                (),
                authorize_put_object(&mut req, &server_ctx, put_object_authorization(&bucket, "object"), |_| Ok(())),
            )
            .await
            .expect_err("a generation failure must never admit a write");
        if effect == "Allow" {
            assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(err.status_code(), Some(http::StatusCode::SERVICE_UNAVAILABLE));
            assert!(matches!(
                err.source().and_then(|source| source.downcast_ref::<StorageError>()),
                Some(StorageError::ErasureReadQuorum)
            ));
        } else {
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
            assert_eq!(err.status_code(), Some(http::StatusCode::FORBIDDEN));
        }
        assert!(req.bucket_generation().is_none());
    }
}
