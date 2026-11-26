// Copyright 2026 RustFS Team
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

//! Reserved internal metadata buckets (`.rustfs.sys` and `.minio.sys`) must never be authorized
//! as S3 resources, even under wildcard `arn:aws:s3:::*` Allow policies or for the deployment
//! owner.

use crate::storage::authorization::tests::fixture::{TestKms, request, server_context};
use crate::storage::authorization::{authorize_request, s3_action_potential};
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, ServerContextSlot};
use http::Method;
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{Store as _, UserType, object::IAM_CONFIG_PREFIX, object::ObjectStore},
    sys::IamSys,
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use serde_json::json;
use serial_test::serial;
use std::sync::Arc;

struct ReadyIam(Arc<IamSys<ObjectStore>>);

impl IamInterface for ReadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.0.clone()
    }

    fn is_ready(&self) -> bool {
        true
    }
}

#[tokio::test]
#[serial]
async fn authorize_request_denies_reserved_buckets_before_store_or_iam_lookup() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = server_context(Arc::clone(&store));

    for bucket in [
        ".rustfs.sys",
        ".rustfs.sys/multipart",
        ".rustfs.sys/config",
        ".minio.sys",
        ".minio.sys/config",
        ".RUSTFS.SYS",
        " .rustfs.sys ",
    ] {
        // Without the reserved-bucket gate, an owner request reaches UnreadyIam and fails with
        // InternalError instead of AccessDenied.
        let mut owner_req = request(Method::GET)
            .server_context(&server_ctx)
            .extension(ReqInfo {
                cred: Some(Credentials {
                    access_key: "owner-key".to_owned(),
                    ..Default::default()
                }),
                is_owner: true,
                bucket: Some(bucket.to_owned()),
                object: Some("config/iam/users/admin/identity.json".to_owned()),
                ..Default::default()
            })
            .build();

        let mut anon_req = request(Method::GET)
            .server_context(&server_ctx)
            .extension(ReqInfo {
                cred: None,
                is_owner: false,
                bucket: Some(bucket.to_owned()),
                object: Some("config/iam/users/admin/identity.json".to_owned()),
                ..Default::default()
            })
            .build();

        for action in [
            S3Action::ListBucketAction,
            S3Action::ListBucketVersionsAction,
            S3Action::GetObjectAction,
            S3Action::PutObjectAction,
            S3Action::DeleteObjectAction,
        ] {
            let owner_err = authorize_request(&mut owner_req, Action::S3Action(action))
                .await
                .expect_err("reserved bucket must be denied even for owner");
            assert_eq!(owner_err.code(), &S3ErrorCode::AccessDenied, "owner {bucket} {action:?}");

            let anon_err = authorize_request(&mut anon_req, Action::S3Action(action))
                .await
                .expect_err("reserved bucket must be denied for anonymous caller");
            assert_eq!(anon_err.code(), &S3ErrorCode::AccessDenied, "anon {bucket} {action:?}");
        }
    }
}

#[test]
#[serial]
fn wildcard_s3_allow_policy_cannot_authorize_reserved_buckets() {
    crate::app::gating_test_env::run_large_stack_test("wildcard-policy-reserved-buckets", || async {
        let (_temp_dir, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialize IAM");

        let normal_bucket = format!("regular-bucket-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&normal_bucket, &MakeBucketOptions::default())
            .await
            .expect("create ordinary bucket");

        iam.set_policy(
            "wildcard-all-s3",
            serde_json::from_str(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}"#,
            )
            .expect("parse wildcard policy"),
        )
        .await
        .expect("save wildcard policy");
        iam.create_user(
            "wildcard-user",
            &AddOrUpdateUserReq {
                secret_key: "wildcard-user-secret".to_owned(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create wildcard user");
        iam.policy_db_set("wildcard-user", UserType::Reg, false, "wildcard-all-s3")
            .await
            .expect("attach wildcard policy");
        let cred = iam.get_user("wildcard-user").await.expect("load user").credentials;

        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam)), Arc::new(TestKms),))));

        // Positive control: the wildcard policy still authorizes ordinary buckets.
        for action in [
            S3Action::ListBucketAction,
            S3Action::GetObjectAction,
            S3Action::PutObjectAction,
        ] {
            let mut req = request(Method::GET)
                .server_context(&server_ctx)
                .extension(ReqInfo {
                    cred: Some(cred.clone()),
                    is_owner: false,
                    bucket: Some(normal_bucket.clone()),
                    object: Some("notes.txt".to_owned()),
                    ..Default::default()
                })
                .build();
            authorize_request(&mut req, Action::S3Action(action))
                .await
                .expect("wildcard policy must still allow ordinary bucket");
        }
        let normal_probe = request(Method::GET)
            .server_context(&server_ctx)
            .extension(ReqInfo {
                cred: Some(cred.clone()),
                is_owner: false,
                bucket: Some(normal_bucket.clone()),
                ..Default::default()
            })
            .build();
        assert!(
            s3_action_potential(&normal_probe, Action::S3Action(S3Action::GetObjectAction))
                .await
                .expect("action potential for normal bucket")
        );

        // Negative: reserved system buckets are rejected even with `s3:*` on `arn:aws:s3:::*`.
        for reserved in [".rustfs.sys", ".rustfs.sys/multipart", ".minio.sys", ".MINIO.SYS"] {
            for action in [
                S3Action::ListBucketAction,
                S3Action::ListBucketVersionsAction,
                S3Action::GetObjectAction,
                S3Action::PutObjectAction,
                S3Action::DeleteObjectAction,
            ] {
                let mut req = request(Method::GET)
                    .server_context(&server_ctx)
                    .extension(ReqInfo {
                        cred: Some(cred.clone()),
                        is_owner: false,
                        bucket: Some(reserved.to_owned()),
                        object: Some("config/iam/users/admin/identity.json".to_owned()),
                        ..Default::default()
                    })
                    .build();
                let err = authorize_request(&mut req, Action::S3Action(action))
                    .await
                    .expect_err("wildcard policy must never authorize a reserved system bucket");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{reserved} {action:?}");
            }

            let probe = request(Method::GET)
                .server_context(&server_ctx)
                .extension(ReqInfo {
                    cred: Some(cred.clone()),
                    is_owner: false,
                    bucket: Some(reserved.to_owned()),
                    ..Default::default()
                })
                .build();
            assert!(
                !s3_action_potential(&probe, Action::S3Action(S3Action::GetObjectAction))
                    .await
                    .expect("action potential for reserved bucket"),
                "{reserved} must have no action potential"
            );
        }
    });
}
