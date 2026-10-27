// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
//! Real handler, persisted IAM and four-disk storage; this is not a signed HTTP/S3 end-to-end test.
use super::*;
use crate::admin::{
    runtime_sources::{AppContext, IamInterface, KmsInterface, ServerContextSlot},
    storage_api::{
        ECStore,
        contract::bucket::{BucketOperations as _, MakeBucketOptions},
        contract::object::ObjectIO as _,
    },
};
use crate::storage::storage_api::{StorageObjectOptions, StoragePutObjReader, new_instance_ctx};
use http_body_util::BodyExt as _;
use rustfs_iam::{
    store::{
        Store as _, UserType,
        object::{IAM_CONFIG_PREFIX, ObjectStore},
    },
    sys::IamSys,
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use serde_json::{Value, json};
use std::sync::Arc;

const PARENT: &str = "cross-parent";
const BROKER: &str = "cross-broker";
const SECRET: &str = "cross-bucket-test-secret";
const ROOT: &str = "cross-root";
const ROOT_SECRET: &str = "cross-bucket-root-test-secret";

struct RequestIam(Arc<IamSys<ObjectStore>>);
impl IamInterface for RequestIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.0.clone()
    }
    fn is_ready(&self) -> bool {
        true
    }
}
struct RequestKms;
impl KmsInterface for RequestKms {
    fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
        Arc::new(rustfs_kms::KmsServiceManager::new())
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    store: Arc<ECStore>,
    iam: Arc<IamSys<ObjectStore>>,
    context: Arc<AppContext>,
    input_version: String,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().expect("cross-bucket storage root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .disk_count(4)
            .instance_ctx(new_instance_ctx())
            .build()
            .await;
        assert_eq!(env.disk_paths.len(), 4);
        let store = env.ecstore;
        for bucket in ["photos", "artifacts", "third"] {
            store
                .make_bucket(
                    bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("real versioned bucket");
        }
        let object = store
            .put_object(
                "photos",
                "input/photo.png",
                &mut StoragePutObjReader::from_vec(vec![0xA5; 273]),
                &StorageObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("real input version");
        let input_version = object.version_id.expect("exact input version").to_string();
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("persistent IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("real IAM");
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(RequestIam(iam.clone())), Arc::new(RequestKms)));
        assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
            access_key: ROOT.into(),
            secret_key: ROOT_SECRET.into(),
            status: "on".into(),
            ..Default::default()
        }));
        let fixture = Self {
            _root: root,
            store,
            iam,
            context,
            input_version,
        };
        fixture.user(PARENT, Self::parent_policy(true, false)).await;
        fixture
            .user(
                BROKER,
                json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"admin:IssuePluginAttempt"}]}),
            )
            .await;
        fixture
    }
    fn parent_policy(output: bool, deny: bool) -> Value {
        let mut resources = vec!["arn:aws:s3:::photos/output/*", "arn:aws:s3:::third/output/*"];
        if output {
            resources.push("arn:aws:s3:::artifacts/output/*");
        }
        let mut statements = vec![
            json!({"Effect":"Allow","Action":"s3:GetObjectVersion","Resource":"arn:aws:s3:::photos/input/*"}),
            json!({"Effect":"Allow","Action":"s3:PutObject","Resource":resources}),
        ];
        if deny {
            statements.push(json!({"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::artifacts/*"}));
        }
        json!({"Version":"2012-10-17","Statement":statements})
    }
    async fn policy(&self, name: &str, value: Value) {
        let policy = Policy::parse_config(&serde_json::to_vec(&value).expect("policy JSON")).expect("real IAM policy");
        self.iam.set_policy(name, policy).await.expect("persist policy");
    }
    async fn user(&self, name: &str, policy: Value) {
        self.iam
            .create_user(
                name,
                &AddOrUpdateUserReq {
                    secret_key: SECRET.into(),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("create real user");
        self.policy(name, policy).await;
        self.iam
            .policy_db_set(name, UserType::Reg, false, name)
            .await
            .expect("attach actual IAM policy");
    }
    fn request(&self, body: &Value) -> S3Request<Body> {
        let slot = ServerContextSlot::new();
        assert!(slot.install(self.context.clone()));
        let mut req = S3Request {
            input: Body::from(serde_json::to_vec(body).expect("attempt JSON")),
            method: Method::POST,
            uri: http::Uri::default(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.extensions.insert(slot);
        req.method = Method::POST;
        req.credentials = Some(s3::auth::Credentials {
            access_key: BROKER.into(),
            secret_key: s3::auth::SecretKey::from(SECRET.to_owned()),
        });
        req
    }
    async fn issue(&self, record: &Delegation, body: &Value) -> S3Result<S3Response<(StatusCode, Body)>> {
        let mut router = matchit::Router::new();
        router.insert("/{id}", ()).expect("parameter route");
        let path = format!("/{}", record.id);
        Issue
            .call(self.request(body), router.at(&path).expect("actual delegation ID").params)
            .await
    }
    async fn allowed(
        &self,
        credential: &rustfs_credentials::Credentials,
        action: S3Action,
        bucket: &str,
        object: &str,
        version: Option<&str>,
    ) -> bool {
        let conditions = version
            .map(|v| HashMap::from([("versionid".into(), vec![v.into()])]))
            .unwrap_or_default();
        self.iam
            .is_allowed(&Args {
                account: &credential.access_key,
                groups: &credential.groups,
                action: Action::S3Action(action),
                bucket,
                object,
                conditions: &conditions,
                claims: credential.claims_or_empty(),
                is_owner: false,
                deny_only: false,
            })
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn delegation_v2_persists_and_vends_with_real_parent_intersection() {
    temp_env::async_with_vars(
        [
            ("RUSTFS_PLUGIN_BROKERS", Some(r#"{"cross-broker":"runner"}"#)),
            ("RUSTFS_SKIP_BACKGROUND_TASK", Some("1")),
        ],
        async {
            let fixture = Fixture::new().await;
            let now = now().expect("clock");
            let legacy = json!({"deployment":"runner","bucket":"photos","input_prefix":"input/",
            "output_prefix":"output/","expires_at_unix_seconds":now+3600});
            let v1 = fixture
                .iam
                .create_plugin_delegation(PARENT, serde_json::from_value(legacy.clone()).expect("v1"), now)
                .await
                .expect("persist legacy delegation");
            assert_eq!(v1.version, 1);
            assert_eq!(serde_json::to_value(&v1.restrictions).expect("v1 wire"), legacy);
            let mut v2 = legacy;
            v2["version"] = json!(2);
            v2["output_bucket"] = json!("artifacts");
            let record = fixture
                .iam
                .create_plugin_delegation(PARENT, serde_json::from_value(v2.clone()).expect("v2"), now)
                .await
                .expect("persist v2 delegation");
            assert_eq!(record.version, 2);
            let loaded = fixture
                .iam
                .read_active_plugin_delegation(PARENT, &record.id, now)
                .await
                .expect("strict stored v2");
            assert!(loaded == record, "same persisted v2 identity");
            assert_eq!(serde_json::to_value(&loaded.restrictions).expect("v2 wire"), v2);
            // The record envelope and restriction profile cannot disagree on a persisted read.
            let path = format!("config/iam/plugin-delegations/v1/{}/record.json", record.id);
            let mut corrupt = serde_json::to_value(&record).expect("record JSON");
            corrupt["version"] = json!(1);
            ObjectStore::new(fixture.store.clone())
                .save_iam_config(corrupt, &path)
                .await
                .expect("inject incompatible stored version");
            assert!(
                fixture
                    .iam
                    .read_active_plugin_delegation(PARENT, &record.id, now)
                    .await
                    .is_err(),
                "mixed v1/v2 stored profiles fail closed"
            );
            ObjectStore::new(fixture.store.clone())
                .save_iam_config(&record, &path)
                .await
                .expect("restore exact stored record");
            let run = uuid::Uuid::new_v4();
            let attempt = uuid::Uuid::new_v4();
            let prefix = format!("output/74656e616e74/{run}/{attempt}/");
            let body = json!({"tenant":"tenant","run_id":run,"attempt_id":attempt,"revision":record.revision,
            "deployment":"runner","input_bucket":"photos","input_key":"input/photo.png","input_version":fixture.input_version,
            "output_bucket":"artifacts","output_prefix":prefix,"ttl_seconds":60});
            let loaded_v1 = fixture
                .iam
                .read_active_plugin_delegation(PARENT, &v1.id, now)
                .await
                .expect("read legacy record");
            assert!(loaded_v1 == v1, "legacy persistent identity");
            let mut legacy_body = body.clone();
            legacy_body["revision"] = json!(v1.revision);
            legacy_body["output_bucket"] = json!("photos");
            let legacy_response = fixture.issue(&v1, &legacy_body).await.expect("legacy same-bucket issuance");
            assert_eq!(legacy_response.output.0, StatusCode::OK);
            let mut wrong = body.clone();
            wrong["output_bucket"] = json!("third");
            match fixture.issue(&record, &wrong).await {
                Err(error) => assert_eq!(error.code(), &S3ErrorCode::AccessDenied),
                Ok(_) => panic!("A03_CROSS_BUCKET_OUTPUT_BOUND"),
            }
            let response = fixture.issue(&record, &body).await.expect("actual v2 issuance");
            assert_eq!(response.output.0, StatusCode::OK);
            assert_eq!(response.headers[http::header::CACHE_CONTROL], "no-store");
            let issued: Value =
                serde_json::from_slice(&response.output.1.collect().await.expect("issued body").to_bytes()).expect("issued JSON");
            let (credential, root) = check_key_valid_with_context(
                issued["session_token"].as_str().expect("token"),
                issued["access_key_id"].as_str().expect("access key"),
                Some(fixture.context.as_ref()),
            )
            .await
            .expect("persisted actual temporary identity");
            assert!(!root && credential.is_temp());
            let output = format!("{prefix}image.png");
            for (action, bucket, object, version, expected) in [
                (
                    S3Action::GetObjectVersionAction,
                    "photos",
                    "input/photo.png",
                    Some(fixture.input_version.as_str()),
                    true,
                ),
                (
                    S3Action::GetObjectVersionAction,
                    "photos",
                    "input/photo.png",
                    Some("wrong-version"),
                    false,
                ),
                (S3Action::GetObjectAction, "photos", "input/photo.png", None, false),
                (S3Action::PutObjectAction, "artifacts", output.as_str(), None, true),
                (S3Action::PutObjectAction, "photos", output.as_str(), None, false),
                (S3Action::PutObjectAction, "third", output.as_str(), None, false),
                (S3Action::PutObjectAction, "artifacts", "output/other/image.png", None, false),
            ] {
                assert_eq!(
                    fixture.allowed(&credential, action, bucket, object, version).await,
                    expected,
                    "A03_CROSS_BUCKET_PARENT_INTERSECTION: bucket={bucket} object={object}"
                );
            }
            fixture.policy(PARENT, Fixture::parent_policy(true, true)).await;
            assert!(
                !fixture
                    .allowed(&credential, S3Action::PutObjectAction, "artifacts", &output, None)
                    .await,
                "explicit parent Deny remains effective"
            );
            fixture.policy(PARENT, Fixture::parent_policy(false, false)).await;
            assert!(
                !fixture
                    .allowed(&credential, S3Action::PutObjectAction, "artifacts", &output, None)
                    .await,
                "removing parent output permission remains effective"
            );
            assert!(
                fixture
                    .allowed(
                        &credential,
                        S3Action::GetObjectVersionAction,
                        "photos",
                        "input/photo.png",
                        Some(&fixture.input_version)
                    )
                    .await
            );
            fixture
                .iam
                .revoke_plugin_delegation(PARENT, &record.id)
                .await
                .expect("revoke v2");
            assert!(fixture.issue(&record, &body).await.is_err(), "revoked v2 cannot issue again");
        },
    )
    .await;
}
