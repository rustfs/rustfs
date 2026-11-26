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

//! Replication-only write headers must be refused on presigned requests and must authorize
//! `s3:PutObjectRetention` / `s3:PutObjectLegalHold` when carrying replication lock timestamps.

use crate::storage::authorization::tests::fixture::{TestKms, request, server_context};
use crate::storage::authorization::{authorize_replication_only_put_headers, replication_request_authorized};
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
async fn presigned_requests_refuse_replication_only_put_headers_even_for_owner() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = server_context(Arc::clone(&store));

    let presigned_uris = [
        "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host%3Bx-rustfs-source-version-id&X-Amz-Credential=root/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=deadbeef",
        "/bucket/key?AWSAccessKeyId=root&Expires=2000000000&Signature=abc",
    ];
    let headers = [
        ("x-rustfs-source-version-id", "0194e0f1-0000-7000-8000-000000000000"),
        ("x-rustfs-source-mtime", "2099-01-01T00:00:00Z"),
        ("x-rustfs-source-replication-request", "true"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-minio-source-version-id", "0194e0f1-0000-7000-8000-000000000000"),
    ];

    for uri in presigned_uris {
        for (name, value) in headers {
            let mut req = request(Method::PUT)
                .uri(uri)
                .header(name, value)
                .server_context(&server_ctx)
                .extension(ReqInfo {
                    cred: Some(Credentials {
                        access_key: "owner-root".to_owned(),
                        ..Default::default()
                    }),
                    is_owner: true,
                    bucket: Some("bucket".to_owned()),
                    object: Some("key".to_owned()),
                    ..Default::default()
                })
                .build();
            let err = authorize_replication_only_put_headers(&mut req)
                .await
                .expect_err("presigned request must never authorize replication-only headers");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{uri} {name}");
            assert!(!replication_request_authorized(&req));
        }
    }
}

#[test]
#[serial]
fn replication_lock_timestamp_headers_require_lock_actions() {
    crate::app::gating_test_env::run_large_stack_test("replication-lock-timestamp-authz", || async {
        let (_temp_dir, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialize IAM");

        let bucket = format!("repl-lock-authz-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");

        iam.set_policy(
            "replicate-only",
            serde_json::from_str(&format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:PutObject","s3:ReplicateObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
            ))
            .expect("parse replicate-only policy"),
        )
        .await
        .expect("save replicate-only policy");
        iam.set_policy(
            "replicate-with-locks",
            serde_json::from_str(&format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:PutObject","s3:ReplicateObject","s3:PutObjectRetention","s3:PutObjectLegalHold"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
            ))
            .expect("parse replicate-with-locks policy"),
        )
        .await
        .expect("save replicate-with-locks policy");

        for (user, policy) in [
            ("repl-only-user", "replicate-only"),
            ("repl-lock-user", "replicate-with-locks"),
        ] {
            iam.create_user(
                user,
                &AddOrUpdateUserReq {
                    secret_key: format!("{user}-secret"),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("create user");
            iam.policy_db_set(user, UserType::Reg, false, policy)
                .await
                .expect("attach policy");
        }
        let repl_only = iam.get_user("repl-only-user").await.expect("load user").credentials;
        let repl_lock = iam.get_user("repl-lock-user").await.expect("load user").credentials;

        let server_ctx = ServerContextSlot::new();
        assert!(server_ctx.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam)), Arc::new(TestKms),))));

        let build_req = |cred: &Credentials, extra: &[(&str, &str)]| {
            let mut builder = request(Method::PUT)
                .uri(&format!("/{bucket}/obj"))
                .header("x-rustfs-source-replication-request", "true")
                .server_context(&server_ctx)
                .extension(ReqInfo {
                    cred: Some(cred.clone()),
                    bucket: Some(bucket.clone()),
                    object: Some("obj".to_owned()),
                    ..Default::default()
                });
            for (name, value) in extra {
                builder = builder.header(name, value);
            }
            builder.build()
        };

        // Plain replication without lock timestamps still succeeds with only s3:ReplicateObject.
        let mut plain = build_req(&repl_only, &[("x-rustfs-source-mtime", "2026-01-01T00:00:00Z")]);
        authorize_replication_only_put_headers(&mut plain)
            .await
            .expect("plain replication should succeed with ReplicateObject");
        assert!(replication_request_authorized(&plain));

        // Retention and legal-hold replication timestamps require the corresponding lock action.
        for header in [
            "x-rustfs-source-replication-retention-timestamp",
            "x-minio-source-replication-retention-timestamp",
            "x-rustfs-source-replication-legalhold-timestamp",
            "x-minio-source-replication-legalhold-timestamp",
        ] {
            let mut denied_req = build_req(&repl_only, &[(header, "2026-01-01T00:00:00Z")]);
            let err = authorize_replication_only_put_headers(&mut denied_req)
                .await
                .expect_err("lock timestamp header must require lock permission");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{header}");
            assert!(!replication_request_authorized(&denied_req));

            let mut allowed_req = build_req(&repl_lock, &[(header, "2026-01-01T00:00:00Z")]);
            authorize_replication_only_put_headers(&mut allowed_req)
                .await
                .expect("caller with lock permission must be allowed");
            assert!(replication_request_authorized(&allowed_req));
        }
    });
}
