// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Internal handler calls with real request IAM and four-disk storage; no HTTP signing is simulated.
use super::*;
use crate::admin::{
    runtime_sources::{AppContext, IamInterface, KmsInterface},
    storage_api::{
        ECStore, Error,
        contract::bucket::{BucketOperations as _, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
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
    instance: Arc<crate::storage::storage_api::InstanceContext>,
}

impl Fixture {
    async fn new(prefix: String) -> Self {
        let root = tempfile::tempdir().expect("handler storage root");
        let instance = new_instance_ctx();
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .disk_count(4)
            .instance_ctx(instance.clone())
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
            instance,
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
        let mut req = S3Request {
            input: Body::from(serde_json::to_vec(body).unwrap()),
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
    serde_json::from_slice(
        &http_body_util::BodyExt::collect(response.output.1)
            .await
            .expect("response body")
            .to_bytes(),
    )
    .expect("receipt JSON")
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

impl Fixture {
    async fn grant_creation(&self, bucket: &str) {
        let policy = Policy::parse_config(
            &serde_json::to_vec(&json!({
                "Version":"2012-10-17", "Statement":[{
                    "Effect":"Allow", "Action":"admin:CreatePluginArtifactBucket",
                    "Resource":format!("arn:aws:s3:::{bucket}")
                }]
            }))
            .expect("creation policy JSON"),
        )
        .expect("creation bucket policy");
        self.iam.set_policy(USER, policy).await.expect("replace controller policy");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_persists_current_identity_and_recreation() {
    use crate::admin::storage_api::AdminVersioningConfigExt as _;
    use crate::storage::storage_api::ecstore_bucket::metadata::load_bucket_metadata;
    let fixture = Fixture::new(prefix()).await;
    let bucket = "new-handler-artifacts";
    fixture.grant_creation(bucket).await;
    let body = json!({"version":1,"bucket":bucket,"base_prefix":"outputs/"});
    let first = receipt(Create.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
    let first_id = Uuid::parse_str(first["bucket_incarnation"].as_str().expect("receipt UUID")).expect("UUID");
    let metadata = load_bucket_metadata(fixture.store.clone(), bucket)
        .await
        .expect("read persistent metadata");
    assert_eq!(first_id, metadata.bucket_incarnation_id, "ARTIFACT_CREATE_CURRENT_GENERATION");
    assert!(!first_id.is_nil());
    assert_eq!(metadata.artifact_base_prefix.as_deref(), Some("outputs/"));
    let config = metadata.versioning_config.expect("persistent versioning");
    assert!(config.enabled());
    assert!(!config.exclude_folders.unwrap_or(false));
    assert!(config.excluded_prefixes.as_ref().is_none_or(Vec::is_empty));
    assert_eq!(first["versioning_enabled"], true);
    assert_eq!(first["bucket"], bucket);
    assert_eq!(first["base_prefix"], "outputs/");
    assert!(first.get("gc_safe").is_none());
    fixture
        .store
        .delete_bucket(bucket, &DeleteBucketOptions::default())
        .await
        .expect("delete first empty generation");
    let second = receipt(Create.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
    let second_id = Uuid::parse_str(second["bucket_incarnation"].as_str().expect("new UUID")).expect("UUID");
    assert_ne!(first_id, second_id, "ARTIFACT_CREATE_CURRENT_GENERATION");
    let current = load_bucket_metadata(fixture.store.clone(), bucket)
        .await
        .expect("read current metadata");
    assert_eq!(second_id, current.bucket_incarnation_id, "ARTIFACT_CREATE_CURRENT_GENERATION");
    assert_eq!(current.artifact_base_prefix.as_deref(), Some("outputs/"));
    assert!(
        fixture
            .store
            .clone()
            .seal_artifact_attempt_puts(bucket, first_id, &fixture.prefix)
            .await
            .is_err(),
        "old receipt cannot seal replacement"
    );
    fixture
        .store
        .clone()
        .seal_artifact_attempt_puts(bucket, second_id, &fixture.prefix)
        .await
        .expect("current incarnation with the same prefix seals successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_existing_bucket_preserves_type() {
    use crate::storage::storage_api::ecstore_bucket::metadata::load_bucket_metadata;
    let fixture = Fixture::new(prefix()).await;
    let ordinary = "ordinary-handler-bucket";
    fixture
        .store
        .make_bucket(ordinary, &MakeBucketOptions::default())
        .await
        .expect("ordinary bucket");
    for bucket in [ordinary, BUCKET] {
        fixture.grant_creation(bucket).await;
        let before = load_bucket_metadata(fixture.store.clone(), bucket)
            .await
            .expect("initial metadata");
        for base_prefix in ["outputs/", "different/"] {
            let body = json!({"version":1,"bucket":bucket,"base_prefix":base_prefix});
            assert!(
                Create.call(fixture.request(&body, Some(USER)), Params::new()).await.is_err(),
                "ARTIFACT_CREATE_NO_RETYPE"
            );
            let after = load_bucket_metadata(fixture.store.clone(), bucket)
                .await
                .expect("unchanged metadata");
            assert_eq!(before.bucket_incarnation_id, after.bucket_incarnation_id);
            assert_eq!(before.artifact_base_prefix, after.artifact_base_prefix);
            assert_eq!(before.versioning_config_xml, after.versioning_config_xml);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_resource_and_instance_isolation() {
    let first = Fixture::new(prefix()).await;
    let second = Fixture::new(prefix()).await;
    let bucket = "isolated-handler-bucket";
    first.grant_creation(bucket).await;
    second.grant_creation(bucket).await;
    let wrong = json!({"version":1,"bucket":"forbidden-handler-bucket","base_prefix":"outputs/"});
    assert_eq!(
        Create
            .call(first.request(&wrong, Some(USER)), Params::new())
            .await
            .expect_err("ARTIFACT_CREATE_RESOURCE")
            .code(),
        &s3s::S3ErrorCode::AccessDenied
    );
    assert!(
        first
            .store
            .get_bucket_info("forbidden-handler-bucket", &BucketOptions::default())
            .await
            .is_err()
    );
    let body = json!({"version":1,"bucket":bucket,"base_prefix":"outputs/"});
    let mut request = first.request(&body, Some(USER));
    let slot = ServerContextSlot::new();
    assert!(slot.install(second.context.clone()));
    request.extensions.insert(slot);
    let created = receipt(Create.call(request, Params::new()).await).await;
    assert_eq!(
        created["bucket_incarnation"],
        json!(
            second
                .store
                .bucket_incarnation_id_from_disk(bucket)
                .await
                .expect("second instance created")
        )
    );
    assert!(
        first.store.get_bucket_info(bucket, &BucketOptions::default()).await.is_err(),
        "request never creates in another instance"
    );
    let mut unready = first.request(&body, Some(USER));
    unready.extensions.insert(ServerContextSlot::new());
    assert!(Create.call(unready, Params::new()).await.is_err());
    assert!(first.store.get_bucket_info(bucket, &BucketOptions::default()).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_create_topology_and_input_fail_closed() {
    let fixture = Fixture::new(prefix()).await;
    let bucket = "unsupported-handler-bucket";
    fixture.grant_creation(bucket).await;
    let body = json!({"version":1,"bucket":bucket,"base_prefix":"outputs/"});
    for (key, value) in [
        ("base_prefix", json!("../")),
        ("base_prefix", json!("")),
        ("unknown", json!(true)),
        ("version", json!(2)),
    ] {
        let mut bad = body.clone();
        bad[key] = value;
        assert_eq!(
            Create
                .call(fixture.request(&bad, Some(USER)), Params::new())
                .await
                .expect_err("invalid request")
                .code(),
            &s3s::S3ErrorCode::InvalidRequest
        );
    }
    fixture
        .instance
        .update_erasure_type(crate::storage::storage_api::ecstore_layout::SetupType::DistErasure)
        .await;
    assert_eq!(
        Create
            .call(fixture.request(&body, Some(USER)), Params::new())
            .await
            .expect_err("remote capability unsupported")
            .code(),
        &s3s::S3ErrorCode::ServiceUnavailable
    );
    assert!(
        fixture
            .store
            .get_bucket_info(bucket, &BucketOptions::default())
            .await
            .is_err(),
        "ARTIFACT_CREATE_LOCAL_ONLY"
    );
}
