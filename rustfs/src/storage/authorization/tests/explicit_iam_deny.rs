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

//! Explicit identity-policy Deny statements must override bucket-policy Allow
//! statements for every S3 action, while implicit IAM denials (an unrelated
//! identity policy or no identity policy) still fall back to a bucket Allow.

use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::storage::authorization::authorize_request_core;
use crate::storage::authorization::tests::fixture::TestKms;
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG;
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, ECStore, IamInterface, ServerContextSlot};
use http::{Extensions, HeaderMap, Method};
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{Store as _, UserType, object::ObjectStore},
    sys::IamSys,
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use serde_json::{Value, json};
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

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<ECStore>,
    iam: Arc<IamSys<ObjectStore>>,
    slot: Arc<ServerContextSlot>,
    bucket: String,
}

impl Fixture {
    async fn new() -> Self {
        let (directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(
                json!({"version": 1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed isolated IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone())
            .await
            .expect("initialize isolated IAM");
        let bucket = format!("iam-deny-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create test bucket");
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::new(AppContext::new(
            store.clone(),
            Arc::new(ReadyIam(iam.clone())),
            Arc::new(TestKms)
        ))));
        Self {
            _directory: directory,
            store,
            iam,
            slot,
            bucket,
        }
    }

    async fn create_user(&self, name: &str, policy: Option<Value>) -> Credentials {
        self.iam
            .create_user(
                name,
                &AddOrUpdateUserReq {
                    secret_key: format!("{name}-secret-key-12345"),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("create IAM user");
        if let Some(doc) = policy {
            let bytes = serde_json::to_vec(&doc).expect("serialize IAM policy");
            let parsed = Policy::parse_config(&bytes).expect("parse IAM policy");
            let policy_name = format!("{name}-policy");
            self.iam.set_policy(&policy_name, parsed).await.expect("store IAM policy");
            self.iam
                .policy_db_set(name, UserType::Reg, false, &policy_name)
                .await
                .expect("attach IAM policy");
        }
        self.iam.get_user(name).await.expect("load IAM user").credentials
    }

    async fn set_bucket_policy(&self, policy: Value) {
        self.store
            .update_bucket_metadata_config(
                &self.bucket,
                BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).expect("serialize bucket policy"),
            )
            .await
            .expect("store bucket policy");
    }

    fn request(&self, cred: Option<&Credentials>, object: Option<&str>) -> RequestEnvelope {
        let mut extensions = Extensions::new();
        extensions.insert(self.slot.clone());
        extensions.insert(ReqInfo {
            cred: cred.cloned(),
            bucket: Some(self.bucket.clone()),
            object: object.map(str::to_owned),
            suppress_denial_log: true,
            suppress_denial_audit: true,
            ..Default::default()
        });
        let uri = match object {
            Some(key) => format!("/{}/{}", self.bucket, key),
            None => format!("/{}", self.bucket),
        };
        RequestEnvelope::from_parts(
            RequestHead {
                method: Method::GET,
                uri: uri.parse().expect("request URI"),
                headers: HeaderMap::new(),
                credentials: cred.map(|c| RequestCredentials::without_secret_key(c.access_key.clone(), None)),
                region: None,
                service: None,
            },
            extensions,
        )
    }
}

#[test]
fn explicit_iam_deny_overrides_bucket_policy_allow_for_all_s3_actions() {
    crate::app::gating_test_env::run_large_stack_test("explicit-iam-deny-all-actions", || async {
        let fixture = Fixture::new().await;
        let denied_user = fixture
            .create_user(
                "carol-denied",
                Some(json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Deny",
                        "Action": ["s3:*"],
                        "Resource": ["arn:aws:s3:::*"]
                    }]
                })),
            )
            .await;
        let carveout_user = fixture
            .create_user(
                "frank-carveout",
                Some(json!({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:*"],
                            "Resource": ["arn:aws:s3:::*"]
                        },
                        {
                            "Effect": "Deny",
                            "Action": ["s3:GetObject", "s3:PutObject"],
                            "Resource": [format!("arn:aws:s3:::{}/secret/*", fixture.bucket)]
                        }
                    ]
                })),
            )
            .await;
        let unrelated_user = fixture
            .create_user(
                "dave-unrelated",
                Some(json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": ["arn:aws:s3:::other-bucket/*"]
                    }]
                })),
            )
            .await;
        let no_policy_user = fixture.create_user("eve-no-policy", None).await;

        fixture
            .set_bucket_policy(json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": ["carol-denied", "frank-carveout", "dave-unrelated", "eve-no-policy"]},
                    "Action": ["s3:*"],
                    "Resource": [
                        format!("arn:aws:s3:::{}", fixture.bucket),
                        format!("arn:aws:s3:::{}/*", fixture.bucket)
                    ]
                }]
            }))
            .await;

        let checked_actions = [
            (S3Action::GetObjectAction, Some("plan.txt")),
            (S3Action::PutObjectAction, Some("plan.txt")),
            (S3Action::DeleteObjectAction, Some("plan.txt")),
            (S3Action::ListBucketAction, None),
            (S3Action::PutBucketPolicyAction, None),
            (S3Action::PutBucketPublicAccessBlockAction, None),
            (S3Action::DeleteBucketPublicAccessBlockAction, None),
        ];

        for (action, object) in checked_actions {
            let mut req = fixture.request(Some(&denied_user), object);
            let err = authorize_request_core(&mut req, Action::S3Action(action), None, false)
                .await
                .expect_err("explicit IAM Deny must override bucket-policy Allow");
            assert_eq!(
                err.code(),
                &S3ErrorCode::AccessDenied,
                "expected AccessDenied for {action:?} under explicit IAM Deny"
            );

            let mut unrelated_req = fixture.request(Some(&unrelated_user), object);
            authorize_request_core(&mut unrelated_req, Action::S3Action(action), None, false)
                .await
                .unwrap_or_else(|e| panic!("implicit IAM denial (unrelated policy) must allow {action:?}: {e:?}"));

            let mut no_policy_req = fixture.request(Some(&no_policy_user), object);
            authorize_request_core(&mut no_policy_req, Action::S3Action(action), None, false)
                .await
                .unwrap_or_else(|e| panic!("implicit IAM denial (no policy) must allow {action:?}: {e:?}"));
        }

        // Prefix carve-out Deny overrides bucket-policy Allow on the denied prefix only.
        for action in [S3Action::GetObjectAction, S3Action::PutObjectAction] {
            let mut denied_prefix = fixture.request(Some(&carveout_user), Some("secret/plan.txt"));
            let err = authorize_request_core(&mut denied_prefix, Action::S3Action(action), None, false)
                .await
                .expect_err("prefix carve-out IAM Deny must override bucket-policy Allow");
            assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

            let mut allowed_prefix = fixture.request(Some(&carveout_user), Some("public/plan.txt"));
            authorize_request_core(&mut allowed_prefix, Action::S3Action(action), None, false)
                .await
                .unwrap_or_else(|e| panic!("non-denied prefix must stay allowed for {action:?}: {e:?}"));
        }

        // Wildcard Principal Allow still grants anonymous access while refusing the denied IAM user.
        fixture
            .set_bucket_policy(json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": [format!("arn:aws:s3:::{}/*", fixture.bucket)]
                }]
            }))
            .await;

        let mut denied_req = fixture.request(Some(&denied_user), Some("plan.txt"));
        let err = authorize_request_core(&mut denied_req, Action::S3Action(S3Action::GetObjectAction), None, false)
            .await
            .expect_err("explicit IAM Deny must override wildcard bucket-policy Allow");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);

        let mut anon_req = fixture.request(None, Some("plan.txt"));
        authorize_request_core(&mut anon_req, Action::S3Action(S3Action::GetObjectAction), None, false)
            .await
            .expect("anonymous caller remains allowed by wildcard bucket policy");
    });
}
