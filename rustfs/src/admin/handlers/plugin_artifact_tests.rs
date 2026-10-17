// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Internal handler calls with real request IAM and four-disk storage; no HTTP signing is simulated.
use super::*;
use crate::admin::{
    runtime_sources::{AppContext, IamInterface, KmsInterface},
    storage_api::{
        ECStore, Error,
        contract::bucket::{BucketOperations as _, MakeBucketOptions},
        contract::object::ObjectIO as _,
        read_admin_config,
    },
};
use crate::storage::storage_api::{
    StorageObjectOptions, StoragePutObjReader, ecstore_disk::BUCKET_META_PREFIX, ecstore_object::WriteCompletion,
    new_instance_ctx,
};
use rustfs_iam::{
    store::{
        Store as _, UserType,
        object::{IAM_CONFIG_PREFIX, ObjectStore},
    },
    sys::{IamSys, NewServiceAccountOpts},
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

const BUCKET: &str = "handler-artifacts";
const USER: &str = "artifact-controller";
const SECRET: &str = "artifact-controller-secret";
const ROOT: &str = "artifact-root";
const ROOT_SECRET: &str = "artifact-root-secret";

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
    incarnation: Uuid,
    prefix: String,
}

impl Fixture {
    async fn new(prefix: String) -> Self {
        let root = tempfile::tempdir().expect("handler storage root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .disk_count(4)
            .instance_ctx(new_instance_ctx())
            .build()
            .await;
        assert_eq!(env.disk_paths.len(), 4);
        let store = env.ecstore;
        store
            .make_bucket(
                BUCKET,
                &MakeBucketOptions {
                    artifact_base_prefix: Some("outputs/".to_owned()),
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create versioned artifact bucket");
        let incarnation = store.bucket_incarnation_id(BUCKET).await.expect("bucket incarnation");
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("real request IAM");
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(RequestIam(iam.clone())), Arc::new(RequestKms)));
        assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
            access_key: ROOT.to_owned(),
            secret_key: ROOT_SECRET.to_owned(),
            status: "on".to_owned(),
            ..Default::default()
        }));
        let fixture = Self {
            _root: root,
            store,
            iam,
            context,
            incarnation,
            prefix,
        };
        fixture
            .user(
                USER,
                Some(vec![fixture.attempt_arn(), format!("arn:aws:s3:::{BUCKET}/{}*", fixture.prefix)]),
            )
            .await;
        fixture
    }

    async fn user(&self, name: &str, resources: Option<Vec<String>>) {
        self.iam
            .create_user(
                name,
                &AddOrUpdateUserReq {
                    secret_key: SECRET.to_owned(),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("create actual IAM user");
        let mut statement =
            json!({"Effect":"Allow", "Action":["admin:SealPluginArtifactAttempt", "admin:RetirePluginArtifactVersion"]});
        if let Some(resources) = resources {
            statement["Resource"] = json!(resources);
        }
        let policy =
            Policy::parse_config(&serde_json::to_vec(&json!({"Version":"2012-10-17", "Statement":[statement]})).unwrap())
                .expect("artifact policy parses");
        self.iam.set_policy(name, policy).await.expect("persist actual IAM policy");
        self.iam
            .policy_db_set(name, UserType::Reg, false, name)
            .await
            .expect("attach actual IAM policy");
    }

    fn attempt_arn(&self) -> String {
        format!("arn:aws:s3:::{BUCKET}/{}", self.prefix.trim_end_matches('/'))
    }

    fn target(&self) -> Value {
        json!({"bucket":BUCKET, "bucket_incarnation":self.incarnation, "attempt_prefix":self.prefix})
    }

    fn seal(&self) -> Value {
        json!({"version":1,"target":self.target()})
    }

    fn retire(&self, object: &str, version: Uuid) -> Value {
        json!({"version":1,"target":self.target(),"object":object,"version_id":version})
    }

    fn request(&self, body: &Value, user: Option<&str>) -> S3Request<Body> {
        let slot = ServerContextSlot::new();
        assert!(slot.install(self.context.clone()));
        let mut req = S3Request::new(Body::from(serde_json::to_vec(body).unwrap()));
        req.extensions.insert(slot);
        req.method = Method::POST;
        req.credentials = user.map(|access_key| s3s::auth::Credentials {
            access_key: access_key.to_owned(),
            secret_key: s3s::auth::SecretKey::from(if access_key == ROOT { ROOT_SECRET } else { SECRET }.to_owned()),
        });
        req
    }

    async fn put(&self, object: &str, byte: u8) -> Result<Uuid, Error> {
        self.store
            .put_object(
                BUCKET,
                object,
                &mut StoragePutObjReader::from_vec(vec![byte; 273]),
                &StorageObjectOptions {
                    versioned: true,
                    write_completion: WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .map(|info| info.version_id.expect("artifact PUT has an exact version"))
    }

    fn seal_path(&self) -> String {
        format!(
            "{BUCKET_META_PREFIX}/{BUCKET}/artifact-put-seals/{}/{}.json",
            self.incarnation,
            digest(&self.prefix)
        )
    }

    fn retirement_path(&self, object: &str, version: Uuid) -> String {
        format!(
            "{BUCKET_META_PREFIX}/{BUCKET}/artifact-version-retirements/{}/{}/{}/{version}.json",
            self.incarnation,
            digest(&self.prefix),
            digest(&rustfs_utils::path::encode_dir_object(object))
        )
    }

    async fn absent(&self, path: &str) {
        assert!(
            matches!(read_admin_config(self.store.clone(), path).await, Err(Error::ConfigNotFound)),
            "rejected handler must leave no durable artifact record: {path}"
        );
    }

    async fn record(&self, path: &str) -> Value {
        serde_json::from_slice(
            &read_admin_config(self.store.clone(), path)
                .await
                .expect("read actual durable record"),
        )
        .expect("record JSON")
    }
}

fn digest(value: &str) -> String {
    hex_simd::encode_to_string(Sha256::digest(value.as_bytes()), hex_simd::AsciiCase::Lower)
}

fn prefix() -> String {
    format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4())
}

async fn receipt(response: S3Result<S3Response<(StatusCode, Body)>>) -> Value {
    let response = response.expect("authorized handler succeeds");
    assert_eq!(response.output.0, StatusCode::OK);
    assert_eq!(response.headers[http::header::CACHE_CONTROL], "no-store");
    serde_json::from_slice(&response.output.1.collect().await.expect("response body").to_bytes()).expect("receipt JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_handlers_persist_seal_and_exact_retirement_with_idempotent_retry() {
    let fixture = Fixture::new(prefix()).await;
    let object = format!("{}image.png", fixture.prefix);
    let first = fixture.put(&object, 0xA5).await.expect("first real version");
    let second = fixture.put(&object, 0xBB).await.expect("second real version");
    assert_ne!(first, second);
    let retirement = fixture.retire(&object, first);
    let error = Retire
        .call(fixture.request(&retirement, Some(USER)), Params::new())
        .await
        .expect_err("retirement requires a seal");
    assert_eq!(error.code(), &s3s::S3ErrorCode::ServiceUnavailable);
    fixture.absent(&fixture.retirement_path(&object, first)).await;

    for _ in 0..2 {
        let seal = receipt(Seal.call(fixture.request(&fixture.seal(), Some(USER)), Params::new()).await).await;
        assert_eq!(seal["ordinary_put_sealed"], true);
        assert_eq!(seal["target"], fixture.target());
        assert!(seal.get("gc_safe").is_none());
    }
    let persisted_seal = fixture.record(&fixture.seal_path()).await;
    assert_eq!(persisted_seal["incarnation_id"], json!(fixture.incarnation));
    assert_eq!(persisted_seal["attempt_prefix"], fixture.prefix);
    assert!(fixture.put(&object, 0xCC).await.is_err(), "handler seal fences genuine subsequent PUTs");
    for _ in 0..2 {
        let retired = receipt(Retire.call(fixture.request(&retirement, Some(USER)), Params::new()).await).await;
        assert_eq!(retired["exact_version_repair_retired"], true);
        assert_eq!(retired["version_id"], json!(first));
        assert!(retired.get("gc_safe").is_none());
    }
    let persisted = fixture.record(&fixture.retirement_path(&object, first)).await;
    assert_eq!(persisted["object"], object);
    assert_eq!(persisted["version_id"], json!(first));
    fixture.absent(&fixture.retirement_path(&object, second)).await;
    for (version, byte) in [(first, 0xA5), (second, 0xBB)] {
        let mut reader = fixture
            .store
            .get_object_reader(
                BUCKET,
                &object,
                None,
                Default::default(),
                &StorageObjectOptions {
                    version_id: Some(version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("retirement preserves real versioned GET");
        let mut bytes = Vec::new();
        reader.stream.read_to_end(&mut bytes).await.expect("exact payload");
        assert_eq!(bytes, vec![byte; 273]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_handlers_reject_unauthorized_principals_and_wrong_scope_without_records() {
    let fixture = Fixture::new(prefix()).await;
    fixture
        .user("wrong-scope", Some(vec![format!("arn:aws:s3:::{BUCKET}/elsewhere/*")]))
        .await;
    fixture.user("empty-resource", None).await;
    let (service, _) = fixture
        .iam
        .new_service_account(
            USER,
            None,
            NewServiceAccountOpts {
                access_key: "artifact-service".to_owned(),
                secret_key: SECRET.to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("persist actual service account");
    let expiry = (time::OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp();
    let mut temporary = rustfs_policy::auth::create_new_credentials_with_metadata(
        "artifact-temporary",
        SECRET,
        &std::collections::HashMap::from([("parent".to_owned(), json!(USER)), ("exp".to_owned(), json!(expiry))]),
        ROOT_SECRET,
    )
    .expect("create valid temporary credentials");
    temporary.parent_user = USER.to_owned();
    fixture
        .iam
        .set_temp_user(&temporary.access_key, &temporary, None)
        .await
        .expect("persist actual STS identity");
    for credential in [&service, &temporary] {
        let (_, root) = crate::auth::check_key_valid_with_context(
            &credential.session_token,
            &credential.access_key,
            Some(fixture.context.as_ref()),
        )
        .await
        .expect("stored child identities authenticate before the controller's original-user restriction");
        assert!(!root);
    }
    let object = format!("{}image.png", fixture.prefix);
    let version = fixture.put(&object, 0xA5).await.expect("real version");
    for user in [
        None,
        Some(ROOT),
        Some("wrong-scope"),
        Some("empty-resource"),
        Some(service.access_key.as_str()),
        Some(temporary.access_key.as_str()),
    ] {
        for retire in [false, true] {
            let body = if retire {
                fixture.retire(&object, version)
            } else {
                fixture.seal()
            };
            let mut req = fixture.request(&body, user);
            if user == Some(temporary.access_key.as_str()) {
                req.headers
                    .insert("x-amz-security-token", temporary.session_token.parse().unwrap());
            }
            let result = if retire {
                Retire.call(req, Params::new()).await
            } else {
                Seal.call(req, Params::new()).await
            };
            assert_eq!(
                result
                    .expect_err("original user and exact resource policy are required")
                    .code(),
                &s3s::S3ErrorCode::AccessDenied
            );
            fixture.absent(&fixture.seal_path()).await;
            fixture.absent(&fixture.retirement_path(&object, version)).await;
        }
    }
    fixture
        .put(&object, 0xBB)
        .await
        .expect("rejected requests did not seal the attempt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_handlers_bind_incarnation_and_request_instance() {
    let prefix = prefix();
    let first = Fixture::new(prefix.clone()).await;
    let second = Fixture::new(prefix).await;
    assert_ne!(first.incarnation, second.incarnation);
    let object = format!("{}image.png", first.prefix);
    let version = first.put(&object, 0xA5).await.expect("first store object");
    second.put(&object, 0xBB).await.expect("second store object");
    for retire in [false, true] {
        let mut body = if retire {
            first.retire(&object, version)
        } else {
            first.seal()
        };
        body["target"]["bucket_incarnation"] = json!(second.incarnation);
        let req = first.request(&body, Some(USER));
        let result = if retire {
            Retire.call(req, Params::new()).await
        } else {
            Seal.call(req, Params::new()).await
        };
        assert_eq!(
            result.expect_err("other instance incarnation is rejected").code(),
            &s3s::S3ErrorCode::ServiceUnavailable
        );
        first.absent(&first.seal_path()).await;
        second.absent(&second.seal_path()).await;
        first.absent(&first.retirement_path(&object, version)).await;
        second.absent(&second.retirement_path(&object, version)).await;
    }
    receipt(Seal.call(first.request(&first.seal(), Some(USER)), Params::new()).await).await;
    assert!(first.put(&object, 0xCC).await.is_err());
    second
        .put(&object, 0xDD)
        .await
        .expect("first instance seal cannot affect second store");
    second.absent(&second.seal_path()).await;
    first.record(&first.seal_path()).await;
}
