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

//! Bucket-policy explicit `Deny` statements naming an IAM user must also bind STS AssumeRole
//! sessions and service accounts derived from that user (including tag-conditioned denials,
//! permission probes, and `s3_action_potential`), while bucket-policy `Allow` fallback remains
//! scoped to the signing access key so restricted sessions cannot be widened.

use crate::storage::authorization::tests::fixture::{TestKms, request};
use crate::storage::authorization::{authorize_request, bucket_policy_deny_accounts, s3_action_potential};
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG;
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, ServerContextSlot};
use crate::storage::{ObjectOptions, PutObjReader};
use http::Method;
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{Store as _, UserType, object::IAM_CONFIG_PREFIX, object::ObjectStore},
    sys::{IamSys, NewServiceAccountOpts, SESSION_POLICY_NAME},
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use serde_json::json;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;

struct ReadyIam(Arc<IamSys<ObjectStore>>);

impl IamInterface for ReadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.0.clone()
    }

    fn is_ready(&self) -> bool {
        true
    }
}

#[test]
fn bucket_policy_deny_accounts_include_distinct_parent_user() {
    let regular = Credentials {
        access_key: "alice".to_owned(),
        ..Default::default()
    };
    assert_eq!(bucket_policy_deny_accounts(&regular), ["alice"]);

    let derived = Credentials {
        access_key: "sts-session-key".to_owned(),
        parent_user: "alice".to_owned(),
        ..Default::default()
    };
    assert_eq!(bucket_policy_deny_accounts(&derived), ["sts-session-key", "alice"]);

    let same_parent = Credentials {
        access_key: "alice".to_owned(),
        parent_user: "alice".to_owned(),
        ..Default::default()
    };
    assert_eq!(bucket_policy_deny_accounts(&same_parent), ["alice"]);
}

#[test]
#[serial]
fn bucket_policy_deny_naming_user_binds_derived_credentials() {
    crate::app::gating_test_env::run_large_stack_test(
        "bucket-policy-deny-derived-credentials",
        bucket_policy_deny_naming_user_binds_derived_credentials_inner,
    );
}

async fn bucket_policy_deny_naming_user_binds_derived_credentials_inner() {
    async fn load_cred(iam: &IamSys<ObjectStore>, server_ctx: &Arc<ServerContextSlot>, access_key: &str) -> Credentials {
        let raw = iam
            .check_key(access_key)
            .await
            .expect("credentials should load")
            .0
            .expect("credentials should exist")
            .credentials;
        let app_ctx = server_ctx.installed_app_context().expect("app context installed");
        let (cred, _) = crate::auth::check_key_valid_with_context(&raw.session_token, &raw.access_key, Some(app_ctx.as_ref()))
            .await
            .expect("credentials should validate with claims");
        cred
    }

    async fn get_object_as(
        iam: &IamSys<ObjectStore>,
        server_ctx: &Arc<ServerContextSlot>,
        access_key: &str,
        bucket: &str,
        object: &str,
    ) -> rustfs_s3_types::S3Result<()> {
        let cred = load_cred(iam, server_ctx, access_key).await;
        let mut req = request(Method::GET)
            .server_context(server_ctx)
            .extension(ReqInfo {
                cred: Some(cred),
                bucket: Some(bucket.to_owned()),
                object: Some(object.to_owned()),
                ..Default::default()
            })
            .build();
        authorize_request(&mut req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    async fn probe_get_object_as(
        iam: &IamSys<ObjectStore>,
        server_ctx: &Arc<ServerContextSlot>,
        access_key: &str,
        bucket: &str,
        object: &str,
    ) -> rustfs_s3_types::S3Result<()> {
        let cred = load_cred(iam, server_ctx, access_key).await;
        let caller = request(Method::POST)
            .uri("/rustfs/admin/v4/account/permissions")
            .server_context(server_ctx)
            .credentials(&cred.access_key, &cred.secret_key)
            .build();
        let mut probe = caller.fork_for_permission_probe(
            Method::GET,
            format!("/{bucket}/{object}").parse().expect("probe URI"),
            ReqInfo {
                cred: Some(cred),
                bucket: Some(bucket.to_owned()),
                object: Some(object.to_owned()),
                suppress_denial_log: true,
                suppress_denial_audit: true,
                ..Default::default()
            },
        );
        authorize_request(&mut probe, Action::S3Action(S3Action::GetObjectAction)).await
    }

    async fn potential_get_object_as(
        iam: &IamSys<ObjectStore>,
        server_ctx: &Arc<ServerContextSlot>,
        access_key: &str,
        bucket: &str,
    ) -> bool {
        let cred = load_cred(iam, server_ctx, access_key).await;
        let req = request(Method::GET)
            .server_context(server_ctx)
            .extension(ReqInfo {
                cred: Some(cred),
                bucket: Some(bucket.to_owned()),
                ..Default::default()
            })
            .build();
        s3_action_potential(&req, Action::S3Action(S3Action::GetObjectAction))
            .await
            .expect("action potential evaluation")
    }

    // Service account and STS session tokens are signed with the process root secret.
    let _ = rustfs_credentials::init_global_action_credentials(
        Some("deny-derived-root".to_owned()),
        Some("deny-derived-root-secret".to_owned()),
    );
    let root_cred = rustfs_credentials::get_global_action_cred().expect("root credentials should be published");
    let signing_secret = root_cred.secret_key.clone();

    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    ObjectStore::new(store.clone())
        .save_iam_config(json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
        .await
        .expect("seed IAM format");
    let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("IAM should initialize");
    let app_context = Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam.clone())), Arc::new(TestKms)));
    let _ = app_context.publish_action_credentials(root_cred);
    let server_ctx = ServerContextSlot::new();
    assert!(server_ctx.install(app_context));

    let bucket = "deny-derived-credentials";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create test bucket");
    for object in ["tagged", "untagged", "allowed-prefix/ok.txt", "other-prefix/secret.txt"] {
        let mut reader = PutObjReader::from_vec(b"content".to_vec());
        store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("create test object");
    }
    store
        .put_object_tags(bucket, "tagged", "classification=restricted", &ObjectOptions::default())
        .await
        .expect("tag test object");

    let identity_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    iam.set_policy(
        "deny-derived-reader",
        serde_json::from_str(&identity_policy).expect("identity policy should parse"),
    )
    .await
    .expect("save identity policy");

    let mut callers = Vec::new();
    let mut alice_sa = String::new();
    let mut alice_session = String::new();
    let mut bob_session = String::new();
    for (user, named) in [("deny-derived-alice", true), ("deny-derived-bob", false)] {
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
        iam.policy_db_set(user, UserType::Reg, false, "deny-derived-reader")
            .await
            .expect("attach identity policy");

        let service_account = format!("{user}-service-account");
        iam.new_service_account(
            user,
            None,
            NewServiceAccountOpts {
                access_key: service_account.clone(),
                secret_key: format!("{service_account}-secret"),
                ..Default::default()
            },
        )
        .await
        .expect("create service account");

        let parent_cred = iam.get_user(user).await.expect("load created parent user").credentials;
        let claims = HashMap::from([
            (
                "exp".to_owned(),
                json!((OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()),
            ),
            ("parent".to_owned(), json!(user)),
        ]);
        let mut session =
            rustfs_policy::auth::get_new_credentials_with_metadata(&claims, &signing_secret).expect("generate STS session");
        session.parent_user = user.to_owned();
        iam.set_temp_user_from_parent(&session, &parent_cred)
            .await
            .expect("store STS session");

        if named {
            alice_sa = service_account.clone();
            alice_session = session.access_key.clone();
        } else {
            bob_session = session.access_key.clone();
        }

        for access_key in [user.to_owned(), service_account, session.access_key] {
            callers.push((access_key, named));
        }
    }

    // 1. Baseline: without a bucket policy, every credential is allowed by identity policy.
    for (access_key, _) in &callers {
        get_object_as(&iam, &server_ctx, access_key, bucket, "tagged")
            .await
            .expect("identity policy should allow read when no bucket policy is set");
    }

    let deny_named_user = |principal: &str, condition: &str| {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":["{principal}"]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket}/*"]{condition}}}]}}"#
        )
        .into_bytes()
    };

    // 2. Unconditional Deny naming alice binds alice, alice's service account, and alice's STS session.
    store
        .update_bucket_metadata_config(bucket, BUCKET_POLICY_CONFIG, deny_named_user("deny-derived-alice", ""))
        .await
        .expect("publish Deny naming alice");
    for (access_key, named) in &callers {
        let result = get_object_as(&iam, &server_ctx, access_key, bucket, "untagged").await;
        if *named {
            let err = result.expect_err("Deny naming user must bind derived credentials");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{access_key}");
        } else {
            result.expect("Deny naming one user must not bind another user");
        }
    }

    // Permission probe and action potential must agree for derived credentials.
    let probe_err = probe_get_object_as(&iam, &server_ctx, &alice_session, bucket, "untagged")
        .await
        .expect_err("permission probe must report AccessDenied for denied parent's STS session");
    assert_eq!(probe_err.code(), &S3ErrorCode::AccessDenied);
    probe_get_object_as(&iam, &server_ctx, &bob_session, bucket, "untagged")
        .await
        .expect("permission probe must allow another user's STS session");
    assert!(
        !potential_get_object_as(&iam, &server_ctx, &alice_session, bucket).await,
        "s3_action_potential must be false under bucket-wide parent Deny"
    );
    assert!(
        potential_get_object_as(&iam, &server_ctx, &bob_session, bucket).await,
        "s3_action_potential must remain true for unaffected user's STS session"
    );

    // 3. Unknown-condition Deny naming alice must return NotImplemented on permission probe.
    store
        .update_bucket_metadata_config(
            bucket,
            BUCKET_POLICY_CONFIG,
            deny_named_user(
                "deny-derived-alice",
                r#","Condition":{"StringEquals":{"s3:x-amz-server-side-encryption":"AES256"}}"#,
            ),
        )
        .await
        .expect("publish unknown-condition Deny naming alice");
    let unknown_err = probe_get_object_as(&iam, &server_ctx, &alice_session, bucket, "untagged")
        .await
        .expect_err("unknown-condition Deny naming parent must make probe indeterminate");
    assert_eq!(unknown_err.code(), &S3ErrorCode::NotImplemented);

    // 4. Tag-conditioned Deny naming alice loads existing object tags for derived credentials too.
    store
        .update_bucket_metadata_config(
            bucket,
            BUCKET_POLICY_CONFIG,
            deny_named_user(
                "deny-derived-alice",
                r#","Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":"restricted"}}"#,
            ),
        )
        .await
        .expect("publish tag-conditioned Deny naming alice");
    for (access_key, named) in &callers {
        get_object_as(&iam, &server_ctx, access_key, bucket, "untagged")
            .await
            .expect("tag-conditioned Deny must not bind an untagged object");
        let result = get_object_as(&iam, &server_ctx, access_key, bucket, "tagged").await;
        if *named {
            let err = result.expect_err("tag-conditioned Deny naming user must bind derived credentials");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied, "{access_key}");
        } else {
            result.expect("tag-conditioned Deny naming one user must not bind another user");
        }
    }

    // 5. Deny naming a service account's own key still binds only that service account.
    store
        .update_bucket_metadata_config(bucket, BUCKET_POLICY_CONFIG, deny_named_user(&alice_sa, ""))
        .await
        .expect("publish Deny naming service account key");
    let sa_err = get_object_as(&iam, &server_ctx, &alice_sa, bucket, "untagged")
        .await
        .expect_err("Deny naming service account key must bind that service account");
    assert_eq!(sa_err.code(), &S3ErrorCode::AccessDenied);
    get_object_as(&iam, &server_ctx, "deny-derived-alice", bucket, "untagged")
        .await
        .expect("Deny naming service account must not bind parent user");
    get_object_as(&iam, &server_ctx, &alice_session, bucket, "untagged")
        .await
        .expect("Deny naming service account must not bind sibling STS session");

    // 6. Bucket-policy Allow naming parent must NOT widen a restricted STS session policy.
    let session_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket}/allowed-prefix/*"]}}]}}"#
    );
    let restricted_claims = HashMap::from([
        (
            "exp".to_owned(),
            json!((OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp()),
        ),
        ("parent".to_owned(), json!("deny-derived-alice")),
        (
            SESSION_POLICY_NAME.to_owned(),
            json!(base64_simd::URL_SAFE_NO_PAD.encode_to_string(session_policy.as_bytes())),
        ),
    ]);
    let alice_parent_cred = iam
        .get_user("deny-derived-alice")
        .await
        .expect("load alice parent credentials")
        .credentials;
    let mut restricted_session = rustfs_policy::auth::get_new_credentials_with_metadata(&restricted_claims, &signing_secret)
        .expect("generate restricted STS session");
    restricted_session.parent_user = "deny-derived-alice".to_owned();
    iam.set_temp_user_from_parent(&restricted_session, &alice_parent_cred)
        .await
        .expect("store restricted STS session");

    let allow_alice = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":["deny-derived-alice"]}},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    )
    .into_bytes();
    store
        .update_bucket_metadata_config(bucket, BUCKET_POLICY_CONFIG, allow_alice)
        .await
        .expect("publish Allow naming alice");

    get_object_as(&iam, &server_ctx, &restricted_session.access_key, bucket, "allowed-prefix/ok.txt")
        .await
        .expect("restricted session must still access its permitted prefix");
    let widened_err = get_object_as(&iam, &server_ctx, &restricted_session.access_key, bucket, "other-prefix/secret.txt")
        .await
        .expect_err("bucket-policy Allow naming parent must not widen a restricted STS session");
    assert_eq!(widened_err.code(), &S3ErrorCode::AccessDenied);
}
