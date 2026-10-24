// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Internal handler calls with real request IAM and four-disk storage; no HTTP signing is simulated.
use super::*;
use crate::admin::storage_api::s3;
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
        let mut statement = json!({"Effect":"Allow", "Action":["admin:SealPluginArtifactAttempt", "admin:SealPluginArtifactRepairs", "admin:SealPluginArtifactWrites", "admin:RetirePluginArtifactVersion"]});
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
        req.credentials = user.map(|access_key| s3::auth::Credentials {
            access_key: access_key.to_owned(),
            secret_key: s3::auth::SecretKey::from(if access_key == ROOT { ROOT_SECRET } else { SECRET }.to_owned()),
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

    fn repair_seal_path(&self) -> String {
        format!(
            "{BUCKET_META_PREFIX}/{BUCKET}/artifact-repair-seals/{}/{}.json",
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
    assert_eq!(error.code(), &s3::S3ErrorCode::ServiceUnavailable);
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
async fn artifact_repair_seal_handler_requires_put_seal_and_confirms_only_attempt_repairs() {
    let fixture = Fixture::new(prefix()).await;
    let body = fixture.seal();
    for mutate in ["version", "unknown", "target"] {
        let mut invalid = body.clone();
        match mutate {
            "version" => invalid["version"] = json!(2),
            "unknown" => invalid["unknown"] = json!(true),
            _ => invalid["target"]["unknown"] = json!(true),
        }
        assert_eq!(
            SealRepairs
                .call(fixture.request(&invalid, Some(USER)), Params::new())
                .await
                .expect_err("repair seal request is strict")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    for unready_slot in [false, true] {
        let mut req = fixture.request(&body, Some(USER));
        req.extensions.remove::<Arc<ServerContextSlot>>();
        if unready_slot {
            req.extensions.insert(Arc::new(ServerContextSlot::default()));
        }
        assert_eq!(
            SealRepairs
                .call(req, Params::new())
                .await
                .expect_err("repair seal requires a ready request instance")
                .code(),
            &s3::S3ErrorCode::ServiceUnavailable
        );
    }
    let error = SealRepairs
        .call(fixture.request(&body, Some(USER)), Params::new())
        .await
        .expect_err("ordinary PUT seal is a prerequisite");
    assert_eq!(error.code(), &s3::S3ErrorCode::ServiceUnavailable);
    assert_eq!(error.message(), Some("artifact operation unconfirmed; retry the same identity"));
    fixture.absent(&fixture.repair_seal_path()).await;
    fixture.absent(&fixture.seal_path()).await;
    let ordinary = receipt(Seal.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
    assert_eq!(ordinary["ordinary_put_sealed"], true);
    assert!(ordinary.get("attempt_repairs_sealed").is_none());
    fixture.absent(&fixture.repair_seal_path()).await;
    for _ in 0..2 {
        let sealed = receipt(SealRepairs.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
        assert_eq!(sealed, json!({"version":1,"target":fixture.target(),"attempt_repairs_sealed":true}));
    }
    let persisted = fixture.record(&fixture.repair_seal_path()).await;
    assert_eq!(persisted["incarnation_id"], json!(fixture.incarnation));
    assert_eq!(persisted["attempt_prefix"], fixture.prefix);
    assert_eq!(persisted["bucket"], BUCKET);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_writes_artifact_handlers_reject_unauthorized_principals_and_wrong_scope_without_records() {
    let fixture = Fixture::new(prefix()).await;
    fixture
        .user("wrong-scope", Some(vec![format!("arn:aws:s3:::{BUCKET}/elsewhere/*")]))
        .await;
    fixture.user("empty-resource", None).await;
    fixture
        .user("wrong-bucket", Some(vec![format!("arn:aws:s3:::another/{}*", fixture.prefix)]))
        .await;
    fixture.user("old-controls-only", Some(vec![fixture.attempt_arn()])).await;
    let old_controls = Policy::parse_config(
        &serde_json::to_vec(&json!({"Version":"2012-10-17", "Statement":[{
            "Effect":"Allow", "Action":["admin:SealPluginArtifactAttempt", "admin:RetirePluginArtifactVersion"],
            "Resource":fixture.attempt_arn()
        }]}))
        .expect("old controls policy JSON"),
    )
    .expect("old controls policy");
    fixture
        .iam
        .set_policy("old-controls-only", old_controls)
        .await
        .expect("old controls only policy");
    assert_eq!(
        SealRepairs
            .call(fixture.request(&fixture.seal(), Some("old-controls-only")), Params::new())
            .await
            .expect_err("old controls do not authorize repair sealing")
            .code(),
        &s3::S3ErrorCode::AccessDenied
    );
    fixture.absent(&fixture.repair_seal_path()).await;
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
        Some("wrong-bucket"),
        Some(service.access_key.as_str()),
        Some(temporary.access_key.as_str()),
    ] {
        for (operation, body) in [
            (&Seal as &dyn Operation, fixture.seal()),
            (&SealRepairs as &dyn Operation, fixture.seal()),
            (&SealWrites as &dyn Operation, fixture.seal()),
            (&Retire as &dyn Operation, fixture.retire(&object, version)),
        ] {
            let mut req = fixture.request(&body, user);
            if user == Some(temporary.access_key.as_str()) {
                req.headers
                    .insert("x-amz-security-token", temporary.session_token.parse().unwrap());
            }
            let result = operation.call(req, Params::new()).await;
            assert_eq!(
                result
                    .expect_err("original user and exact resource policy are required")
                    .code(),
                &s3::S3ErrorCode::AccessDenied
            );
            fixture.absent(&fixture.seal_path()).await;
            fixture.absent(&fixture.repair_seal_path()).await;
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
async fn artifact_writes_artifact_handlers_bind_incarnation_and_request_instance() {
    let prefix = prefix();
    let first = Fixture::new(prefix.clone()).await;
    let second = Fixture::new(prefix).await;
    assert_ne!(first.incarnation, second.incarnation);
    let object = format!("{}image.png", first.prefix);
    let version = first.put(&object, 0xA5).await.expect("first store object");
    second.put(&object, 0xBB).await.expect("second store object");
    for (operation, mut body) in [
        (&Seal as &dyn Operation, first.seal()),
        (&SealRepairs as &dyn Operation, first.seal()),
        (&SealWrites as &dyn Operation, first.seal()),
        (&Retire as &dyn Operation, first.retire(&object, version)),
    ] {
        body["target"]["bucket_incarnation"] = json!(second.incarnation);
        let req = first.request(&body, Some(USER));
        let result = operation.call(req, Params::new()).await;
        assert_eq!(
            result.expect_err("other instance incarnation is rejected").code(),
            &s3::S3ErrorCode::ServiceUnavailable
        );
        first.absent(&first.seal_path()).await;
        second.absent(&second.seal_path()).await;
        first.absent(&first.repair_seal_path()).await;
        second.absent(&second.repair_seal_path()).await;
        first.absent(&first.retirement_path(&object, version)).await;
        second.absent(&second.retirement_path(&object, version)).await;
    }
    receipt(Seal.call(first.request(&first.seal(), Some(USER)), Params::new()).await).await;
    let mut wrong_incarnation = first.seal();
    wrong_incarnation["target"]["bucket_incarnation"] = json!(second.incarnation);
    let error = SealRepairs
        .call(first.request(&wrong_incarnation, Some(USER)), Params::new())
        .await
        .expect_err("repair seal checks incarnation even after the PUT seal exists");
    assert_eq!(error.code(), &s3::S3ErrorCode::ServiceUnavailable);
    first.absent(&first.repair_seal_path()).await;
    second.absent(&second.repair_seal_path()).await;
    assert!(first.put(&object, 0xCC).await.is_err());
    second
        .put(&object, 0xDD)
        .await
        .expect("first instance seal cannot affect second store");
    second.absent(&second.seal_path()).await;
    first.record(&first.seal_path()).await;
    let repaired = receipt(
        SealRepairs
            .call(first.request(&first.seal(), Some(USER)), Params::new())
            .await,
    )
    .await;
    assert_eq!(repaired["target"], first.target());
    assert_eq!(repaired["attempt_repairs_sealed"], true);
    first.record(&first.repair_seal_path()).await;
    second.absent(&second.repair_seal_path()).await;
}

impl Fixture {
    async fn grant_creation(&self, bucket: &str) {
        self.grant_bucket_action(bucket, "admin:CreatePluginArtifactBucket").await;
    }

    async fn grant_bucket_action(&self, bucket: &str, action: &str) {
        let policy = Policy::parse_config(
            &serde_json::to_vec(&json!({
                "Version":"2012-10-17", "Statement":[{
                    "Effect":"Allow", "Action":action,
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
        &s3::S3ErrorCode::AccessDenied
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
            &s3::S3ErrorCode::InvalidRequest
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
        &s3::S3ErrorCode::ServiceUnavailable
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_inspect_current_identity_and_recreation() {
    let fixture = Fixture::new(prefix()).await;
    fixture.grant_bucket_action(BUCKET, "admin:GetPluginArtifactBucket").await;
    let body = json!({"version":1,"bucket":BUCKET,"base_prefix":"outputs/"});
    let observed = receipt(Inspect.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
    assert_eq!(observed["bucket_incarnation"], json!(fixture.incarnation));
    assert_eq!(observed["bucket"], BUCKET);
    assert_eq!(observed["base_prefix"], "outputs/");
    assert_eq!(observed["versioning_enabled"], true);
    assert_eq!(observed["version"], 1);
    for absent in ["gc_safe", "ordinary_put_sealed", "all_writes_sealed"] {
        assert!(observed.get(absent).is_none());
    }
    fixture
        .store
        .delete_bucket(BUCKET, &DeleteBucketOptions::default())
        .await
        .expect("delete first generation");
    let current = fixture
        .store
        .clone()
        .make_artifact_bucket(BUCKET, "outputs/")
        .await
        .expect("same-prefix recreation");
    assert_ne!(current, fixture.incarnation);
    let observed = receipt(Inspect.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
    assert_eq!(observed["bucket_incarnation"], json!(current));
    assert!(
        fixture
            .store
            .clone()
            .seal_artifact_attempt_puts(BUCKET, fixture.incarnation, &fixture.prefix)
            .await
            .is_err()
    );
    fixture
        .store
        .clone()
        .seal_artifact_attempt_puts(BUCKET, current, &fixture.prefix)
        .await
        .expect("current generation accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_inspect_resource_and_instance_isolation() {
    let first = Fixture::new(prefix()).await;
    let second = Fixture::new(prefix()).await;
    let body = json!({"version":1,"bucket":BUCKET,"base_prefix":"outputs/"});
    first.grant_creation(BUCKET).await;
    assert_eq!(
        Inspect
            .call(first.request(&body, Some(USER)), Params::new())
            .await
            .expect_err("create cannot inspect")
            .code(),
        &s3::S3ErrorCode::AccessDenied
    );
    first.grant_bucket_action(BUCKET, "admin:GetPluginArtifactBucket").await;
    second.grant_bucket_action(BUCKET, "admin:GetPluginArtifactBucket").await;
    let other = "inspect-other-artifacts";
    let other_id = first
        .store
        .clone()
        .make_artifact_bucket(other, "outputs/")
        .await
        .expect("real wrong-bucket identity exists");
    let wrong = json!({"version":1,"bucket":other,"base_prefix":"outputs/"});
    // The mutation must reach a real healthy identity read, not a storage failure.
    let leaked = match Inspect.call(first.request(&wrong, Some(USER)), Params::new()).await {
        Ok(response) => {
            let observed = receipt(Ok(response)).await;
            assert_eq!(observed["bucket_incarnation"], json!(other_id));
            true
        }
        Err(error) => {
            assert_eq!(error.code(), &s3::S3ErrorCode::AccessDenied);
            false
        }
    };
    assert!(!leaked, "ARTIFACT_INSPECT_RESOURCE");
    let mut request = first.request(&body, Some(USER));
    let slot = ServerContextSlot::new();
    assert!(slot.install(second.context.clone()));
    request.extensions.insert(slot);
    let observed = receipt(Inspect.call(request, Params::new()).await).await;
    assert_eq!(observed["bucket_incarnation"], json!(second.incarnation));
    assert_ne!(first.incarnation, second.incarnation);
    for user in [None, Some(ROOT)] {
        assert_eq!(
            Inspect
                .call(first.request(&body, user), Params::new())
                .await
                .expect_err("original IAM required")
                .code(),
            &s3::S3ErrorCode::AccessDenied
        );
    }
    let mut unready = first.request(&body, Some(USER));
    unready.extensions.insert(ServerContextSlot::new());
    assert_eq!(
        Inspect
            .call(unready, Params::new())
            .await
            .expect_err("no ambient fallback")
            .code(),
        &s3::S3ErrorCode::ServiceUnavailable
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_bucket_inspect_rejections_leave_storage_unchanged() {
    use crate::storage::storage_api::ecstore_bucket::metadata::load_bucket_metadata;
    let fixture = Fixture::new(prefix()).await;
    let ordinary = "inspect-ordinary";
    fixture
        .store
        .make_bucket(ordinary, &MakeBucketOptions::default())
        .await
        .expect("ordinary bucket");
    for (bucket, base_prefix) in [(ordinary, "outputs/"), ("inspect-missing", "outputs/"), (BUCKET, "wrong/")] {
        fixture.grant_bucket_action(bucket, "admin:GetPluginArtifactBucket").await;
        let body = json!({"version":1,"bucket":bucket,"base_prefix":base_prefix});
        assert_eq!(
            Inspect
                .call(fixture.request(&body, Some(USER)), Params::new())
                .await
                .expect_err("not a matching managed bucket")
                .code(),
            &s3::S3ErrorCode::ServiceUnavailable
        );
    }
    let ordinary_metadata = load_bucket_metadata(fixture.store.clone(), ordinary)
        .await
        .expect("ordinary unchanged");
    assert!(ordinary_metadata.artifact_base_prefix.is_none());
    assert!(ordinary_metadata.versioning_config_xml.is_empty());
    assert!(
        fixture
            .store
            .get_bucket_info("inspect-missing", &BucketOptions::default())
            .await
            .is_err()
    );
    assert_eq!(fixture.store.bucket_incarnation_id(BUCKET).await.unwrap(), fixture.incarnation);
    fixture.grant_bucket_action(BUCKET, "admin:GetPluginArtifactBucket").await;
    let body = json!({"version":1,"bucket":BUCKET,"base_prefix":"outputs/"});
    for (key, value) in [
        ("unknown", json!(true)),
        ("version", json!(4)),
        ("base_prefix", json!(null)),
        ("base_prefix", json!("../")),
    ] {
        let mut invalid = body.clone();
        invalid[key] = value;
        assert_eq!(
            Inspect
                .call(fixture.request(&invalid, Some(USER)), Params::new())
                .await
                .expect_err("strict input")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    fixture
        .instance
        .update_erasure_type(crate::storage::storage_api::ecstore_layout::SetupType::DistErasure)
        .await;
    assert_eq!(
        Inspect
            .call(fixture.request(&body, Some(USER)), Params::new())
            .await
            .expect_err("unsupported topology")
            .code(),
        &s3::S3ErrorCode::ServiceUnavailable
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_writes_handler_strict_requests_and_independent_action() {
    let fixture = Fixture::new(prefix()).await;
    for old in ["admin:SealPluginArtifactAttempt", "admin:SealPluginArtifactRepairs"] {
        let name = format!("old-{}", old.rsplit(':').next().expect("action suffix"));
        fixture.user(&name, Some(vec![fixture.attempt_arn()])).await;
        let policy = Policy::parse_config(
            &serde_json::to_vec(&json!({"Version":"2012-10-17", "Statement":[{
                "Effect":"Allow", "Action":old, "Resource":fixture.attempt_arn()
            }]}))
            .expect("old policy bytes"),
        )
        .expect("old narrow policy");
        fixture.iam.set_policy(&name, policy).await.expect("grant only old action");
        assert_eq!(
            SealWrites
                .call(fixture.request(&fixture.seal(), Some(&name)), Params::new())
                .await
                .expect_err("old narrow action cannot authorize all writes")
                .code(),
            &s3::S3ErrorCode::AccessDenied
        );
    }
    for mutate in ["version", "unknown", "target", "nil"] {
        let mut invalid = fixture.seal();
        match mutate {
            "version" => invalid["version"] = json!(2),
            "unknown" => invalid["unknown"] = json!(true),
            "target" => invalid["target"]["unknown"] = json!(true),
            _ => invalid["target"]["bucket_incarnation"] = json!(Uuid::nil()),
        }
        assert_eq!(
            SealWrites
                .call(fixture.request(&invalid, Some(USER)), Params::new())
                .await
                .expect_err("writes request must be strict")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    for raw in [
        "{".to_owned(),
        format!(r#"{{"version":1,"version":1,"target":{}}}"#, fixture.target()),
    ] {
        let mut req = fixture.request(&fixture.seal(), Some(USER));
        req.input = Body::from(raw);
        assert_eq!(
            SealWrites
                .call(req, Params::new())
                .await
                .expect_err("malformed and duplicate JSON must fail")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    for slot in [None, Some(Arc::new(ServerContextSlot::default()))] {
        let mut req = fixture.request(&fixture.seal(), Some(USER));
        req.extensions.remove::<Arc<ServerContextSlot>>();
        if let Some(slot) = slot {
            req.extensions.insert(slot);
        }
        assert_eq!(
            SealWrites
                .call(req, Params::new())
                .await
                .expect_err("writes seal requires ready instance")
                .code(),
            &s3::S3ErrorCode::ServiceUnavailable
        );
    }
    let mut wrong = fixture.seal();
    wrong["target"]["bucket_incarnation"] = json!(Uuid::new_v4());
    let error = SealWrites
        .call(fixture.request(&wrong, Some(USER)), Params::new())
        .await
        .expect_err("unconfirmed incarnation");
    assert_eq!(error.code(), &s3::S3ErrorCode::ServiceUnavailable);
    assert_eq!(error.message(), Some("artifact operation unconfirmed; retry the same identity"));
    fixture.absent(&fixture.seal_path()).await;
    fixture.absent(&fixture.repair_seal_path()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_writes_handler_confirms_both_records_on_first_and_exact_retry() {
    let fixture = Fixture::new(prefix()).await;
    let policy = Policy::parse_config(
        &serde_json::to_vec(&json!({"Version":"2012-10-17", "Statement":[{
            "Effect":"Allow", "Action":"admin:SealPluginArtifactWrites", "Resource":fixture.attempt_arn()
        }]}))
        .expect("writes-only policy bytes"),
    )
    .expect("writes-only policy");
    fixture
        .iam
        .set_policy(USER, policy)
        .await
        .expect("grant independent writes authority");
    let key = format!("{}existing.bin", fixture.prefix);
    let version = fixture.put(&key, 0xA5).await.expect("existing real version");
    for _ in 0..2 {
        let sealed = receipt(
            SealWrites
                .call(fixture.request(&fixture.seal(), Some(USER)), Params::new())
                .await,
        )
        .await;
        assert_eq!(
            sealed,
            json!({"version":1,"target":fixture.target(),"ordinary_put_sealed":true,"attempt_repairs_sealed":true,"all_writes_sealed":true})
        );
        assert!(fixture.record(&fixture.seal_path()).await.is_object());
        assert!(fixture.record(&fixture.repair_seal_path()).await.is_object());
    }
    fixture.absent(&fixture.retirement_path(&key, version)).await;
    assert!(fixture.put(&key, 0xBB).await.is_err(), "combined seal must actually reject PUT");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_writes_upload_part_copy_rejects_real_managed_staging_and_preserves_it() {
    use crate::admin::storage_api::s3::{CopySource, S3 as _, S3Access, UploadPartCopyInput};
    use crate::storage::storage_api::contract::{
        bucket::BUCKET_LIFECYCLE_LOCK_OBJECT, multipart::MultipartOperations as _, namespace::NamespaceLocking as _,
    };
    let fixture = Fixture::new(prefix()).await;
    let ordinary = "artifact-copy-control";
    fixture
        .store
        .make_bucket(
            ordinary,
            &MakeBucketOptions {
                versioning_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("ordinary source bucket");
    fixture
        .store
        .put_object(
            ordinary,
            "source.bin",
            &mut StoragePutObjReader::from_vec(vec![0xBB; 273]),
            &StorageObjectOptions {
                versioned: true,
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("actual copy source");
    let key = format!("{}legacy-multipart.bin", fixture.prefix);
    fixture.put(&key, 0xA5).await.expect("existing managed version");
    // Model persisted staging predating the managed profile. Use the existing
    // pool API under an actual lifecycle owner; no invented upload ID or
    // bucket-authority override can stand in for a valid copy destination.
    let namespace = fixture
        .store
        .new_ns_lock(BUCKET, BUCKET_LIFECYCLE_LOCK_OBJECT)
        .await
        .expect("managed lifecycle namespace");
    let lifecycle = namespace
        .get_read_lock(Duration::from_secs(30))
        .await
        .expect("managed lifecycle owner");
    let mut options = StorageObjectOptions {
        expected_bucket_incarnation_id: Some(fixture.incarnation),
        versioned: true,
        ..Default::default()
    };
    options.add_bucket_lifecycle_lock_guard(&lifecycle);
    let upload = fixture.store.pools[0]
        .new_multipart_upload(BUCKET, &key, &options)
        .await
        .expect("real historical managed upload");
    let original = fixture.store.pools[0]
        .put_object_part(
            BUCKET,
            &key,
            &upload.upload_id,
            1,
            &mut StoragePutObjReader::from_vec(vec![0xAA; 273]),
            &options,
        )
        .await
        .expect("real historical staged part");
    drop(options);
    drop(lifecycle);
    let parts = fixture
        .store
        .list_object_parts(BUCKET, &key, &upload.upload_id, None, 1000, &StorageObjectOptions::default())
        .await
        .expect("historical staging is readable before copy");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].etag, original.etag);
    let slot = ServerContextSlot::new();
    assert!(slot.install(fixture.context.clone()));
    let fs = crate::storage::ecfs::FS::with_server_ctx(slot);
    // S3Access::check authenticates this principal in a network request. Keep
    // that same real context-bound identity when entering the access hook here.
    let (credential, is_owner) = crate::auth::check_key_valid_with_context("", ROOT, Some(fixture.context.as_ref()))
        .await
        .expect("authenticate the fixture's actual root identity");
    assert!(is_owner);
    let source_incarnation = fixture
        .store
        .bucket_incarnation_id(ordinary)
        .await
        .expect("actual source incarnation");
    let copy_request = |bucket: &str, key: &str, upload_id: &str| {
        let input = UploadPartCopyInput::builder()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .upload_id(upload_id.to_owned())
            .part_number(1)
            .copy_source(CopySource::Bucket {
                bucket: ordinary.to_owned().into(),
                key: "source.bin".into(),
                version_id: None,
            })
            .build()
            .expect("valid UploadPartCopy request");
        let mut req = fixture.request(&json!({}), Some(ROOT));
        req.extensions.insert(crate::storage::storage_api::access_consumer::ReqInfo {
            cred: Some(credential.clone()),
            is_owner,
            ..Default::default()
        });
        S3Request {
            input,
            method: Method::PUT,
            uri: req.uri,
            headers: req.headers,
            extensions: req.extensions,
            credentials: req.credentials,
            region: req.region,
            service: req.service,
            trailing_headers: None,
        }
    };
    let disk_snapshot = || {
        let mut pending = vec![fixture._root.path().to_owned()];
        let mut snapshot = std::collections::BTreeMap::new();
        while let Some(path) = pending.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(&path).expect("fixture directory") {
                    pending.push(entry.expect("fixture entry").path());
                }
                snapshot.insert(path, None);
            } else {
                let bytes = std::fs::read(&path).expect("fixture file");
                snapshot.insert(path, Some(bytes));
            }
        }
        snapshot
    };
    let mut managed_request = copy_request(BUCKET, &key, &upload.upload_id);
    S3Access::upload_part_copy(&fs, &mut managed_request)
        .await
        .expect("real source/destination authorization captures both generation guards");
    let mut source_options = StorageObjectOptions::default();
    crate::storage::storage_api::access_consumer::apply_copy_source_bucket_generation_guard(
        &managed_request,
        ordinary,
        &mut source_options,
    )
    .expect("source generation middleware contract");
    assert_eq!(source_options.expected_bucket_incarnation_id, Some(source_incarnation));
    let mut destination_options = StorageObjectOptions::default();
    crate::storage::storage_api::access_consumer::apply_bucket_generation_guard(
        &managed_request,
        BUCKET,
        &mut destination_options,
    )
    .expect("destination generation middleware contract");
    assert_eq!(destination_options.expected_bucket_incarnation_id, Some(fixture.incarnation));
    let before = disk_snapshot();
    let error = Box::pin(fs.upload_part_copy(managed_request))
        .await
        .expect_err("managed copy must reject at its actual write entrance");
    assert_eq!(error.code(), &s3::S3ErrorCode::InternalError);
    assert_eq!(error.message(), Some("We encountered an internal error, please try again."));
    assert!(
        disk_snapshot() == before,
        "rejected copy changed existing metadata, shards, or multipart staging"
    );
    let after = fixture
        .store
        .list_object_parts(BUCKET, &key, &upload.upload_id, None, 1000, &StorageObjectOptions::default())
        .await
        .expect("original staged part remains readable");
    assert_eq!(after.parts.len(), 1);
    assert_eq!(after.parts[0].etag, original.etag);
    let control = fixture
        .store
        .new_multipart_upload(ordinary, "destination.bin", &StorageObjectOptions::default())
        .await
        .expect("ordinary MPU");
    let mut control_request = copy_request(ordinary, "destination.bin", &control.upload_id);
    S3Access::upload_part_copy(&fs, &mut control_request)
        .await
        .expect("ordinary copy uses the same real authorization/generation middleware");
    let copied = Box::pin(fs.upload_part_copy(control_request))
        .await
        .expect("same S3 handler must copy into ordinary staging");
    assert!(copied.output.copy_part_result.and_then(|part| part.e_tag).is_some());
    let parts = fixture
        .store
        .list_object_parts(
            ordinary,
            "destination.bin",
            &control.upload_id,
            None,
            1000,
            &StorageObjectOptions::default(),
        )
        .await
        .expect("copied ordinary part");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].size, 273);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_inventory_handler_requires_request_context_and_independent_action() {
    let fixture = Fixture::new(prefix()).await;
    fixture.user(USER, Some(vec![fixture.attempt_arn()])).await;
    let request = json!({"version":1,"invocation_id":Uuid::new_v4(),"target":fixture.target()});
    assert_eq!(
        Inventory
            .call(fixture.request(&request, Some(USER)), Params::new())
            .await
            .expect_err("seal grants cannot inventory")
            .code(),
        &s3::S3ErrorCode::AccessDenied
    );
    for slot in [None, Some(Arc::new(ServerContextSlot::default()))] {
        let mut req = fixture.request(&request, Some(USER));
        req.extensions.remove::<Arc<ServerContextSlot>>();
        if let Some(slot) = slot {
            req.extensions.insert(slot);
        }
        assert_eq!(
            Inventory
                .call(req, Params::new())
                .await
                .expect_err("inventory needs owning context")
                .code(),
            &s3::S3ErrorCode::ServiceUnavailable
        );
    }
    for mutation in ["version", "invocation_id", "unknown", "target", "nil", "prefix"] {
        let mut invalid = request.clone();
        match mutation {
            "version" => invalid["version"] = json!(2),
            "invocation_id" => invalid["invocation_id"] = json!(Uuid::nil()),
            "unknown" => invalid["unknown"] = json!(true),
            "target" => invalid["target"]["unknown"] = json!(true),
            "nil" => invalid["target"]["bucket_incarnation"] = json!(Uuid::nil()),
            _ => invalid["target"]["attempt_prefix"] = json!("outputs/../attempt/"),
        }
        assert_eq!(
            Inventory
                .call(fixture.request(&invalid, Some(USER)), Params::new())
                .await
                .expect_err("strict inventory identity")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    fixture.absent(&fixture.seal_path()).await;
    fixture.absent(&fixture.repair_seal_path()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_delete_handler_enforces_scope_seals_and_exact_version() {
    let fixture = Fixture::new(prefix()).await;
    let key = format!("{}image.bin", fixture.prefix);
    let first = fixture.put(&key, 0xA5).await.expect("first real version");
    let second = fixture.put(&key, 0xBB).await.expect("second real version");
    let request = fixture.retire(&key, first);
    for principal in [None, Some(USER), Some(ROOT)] {
        assert_eq!(
            DeleteVersion
                .call(fixture.request(&request, principal), Params::new())
                .await
                .expect_err("A03GC2_INVARIANT old seal/retire authority cannot delete")
                .code(),
            &s3::S3ErrorCode::AccessDenied,
        );
    }
    let policy = Policy::parse_config(
        &serde_json::to_vec(&json!({
            "Version":"2012-10-17", "Statement":[{
                "Effect":"Allow", "Action":"admin:DeletePluginArtifactVersion",
                "Resource":format!("arn:aws:s3:::{BUCKET}/{key}")
            }]
        }))
        .expect("delete policy bytes"),
    )
    .expect("independent delete policy");
    const CLEANUP: &str = "artifact-cleanup";
    fixture
        .user(CLEANUP, Some(vec![format!("arn:aws:s3:::{BUCKET}/{key}")]))
        .await;
    fixture
        .iam
        .set_policy(CLEANUP, policy)
        .await
        .expect("grant exact key delete authority");
    assert!(
        DeleteVersion
            .call(fixture.request(&request, Some(CLEANUP)), Params::new())
            .await
            .is_err(),
        "A03GC2_INVARIANT unsealed attempt is protected"
    );
    receipt(Seal.call(fixture.request(&fixture.seal(), Some(USER)), Params::new()).await).await;
    assert!(
        DeleteVersion
            .call(fixture.request(&request, Some(CLEANUP)), Params::new())
            .await
            .is_err(),
        "A03GC2_INVARIANT PUT seal alone cannot authorize deletion"
    );
    receipt(
        SealWrites
            .call(fixture.request(&fixture.seal(), Some(USER)), Params::new())
            .await,
    )
    .await;
    let mut wrong_identity = request.clone();
    wrong_identity["target"]["bucket_incarnation"] = json!(Uuid::new_v4());
    assert!(
        DeleteVersion
            .call(fixture.request(&wrong_identity, Some(CLEANUP)), Params::new())
            .await
            .is_err(),
        "A03GC2_INVARIANT incarnation mismatch cannot delete"
    );
    let outside_scope = fixture.retire(&format!("{}other.bin", fixture.prefix), first);
    assert_eq!(
        DeleteVersion
            .call(fixture.request(&outside_scope, Some(CLEANUP)), Params::new())
            .await
            .expect_err("A03GC2_INVARIANT exact object IAM scope")
            .code(),
        &s3::S3ErrorCode::AccessDenied
    );
    for _ in 0..2 {
        let deleted = receipt(
            DeleteVersion
                .call(fixture.request(&request, Some(CLEANUP)), Params::new())
                .await,
        )
        .await;
        assert_eq!(
            deleted,
            json!({"version":1,"target":fixture.target(),"object":key,
            "version_id":first,"exact_version_deleted":true}),
            "A03GC2_INVARIANT exact retry receipt"
        );
    }
    let absent = fixture
        .store
        .get_object_reader(
            BUCKET,
            &key,
            None,
            Default::default(),
            &StorageObjectOptions {
                version_id: Some(first.to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(absent, Err(Error::VersionNotFound(_, _, _)) | Err(Error::FileVersionNotFound)),
        "A03GC2_INVARIANT real exact version is absent"
    );
    let mut preserved = fixture
        .store
        .get_object_reader(
            BUCKET,
            &key,
            None,
            Default::default(),
            &StorageObjectOptions {
                version_id: Some(second.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("A03GC2_INVARIANT other exact version survives");
    let mut bytes = Vec::new();
    preserved.stream.read_to_end(&mut bytes).await.expect("surviving bytes");
    assert_eq!(bytes, vec![0xBB; 273]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_delete_inspect_negotiates_without_changing_v1() {
    let fixture = Fixture::new(prefix()).await;
    let policy = Policy::parse_config(
        &serde_json::to_vec(&json!({
            "Version":"2012-10-17", "Statement":[{
                "Effect":"Allow", "Action":"admin:GetPluginArtifactBucket",
                "Resource":format!("arn:aws:s3:::{BUCKET}")
            }]
        }))
        .expect("inspector policy bytes"),
    )
    .expect("inspector policy");
    fixture.iam.set_policy(USER, policy).await.expect("grant inspect authority");
    for version in [1, 2, 3] {
        let request = json!({"version":version,"bucket":BUCKET,"base_prefix":"outputs/"});
        let observed = receipt(Inspect.call(fixture.request(&request, Some(USER)), Params::new()).await).await;
        let mut expected = json!({"version":version,"bucket":BUCKET,"base_prefix":"outputs/",
            "bucket_incarnation":fixture.incarnation,"versioning_enabled":true});
        if version >= 2 {
            expected["exact_delete_supported"] = json!(true);
        }
        if version == 3 {
            expected["backup_retained"] = json!(false);
        }
        assert_eq!(observed, expected, "A03GC2_INVARIANT explicit capability negotiation");
    }
    for version in [0, 4] {
        let request = json!({"version":version,"bucket":BUCKET,"base_prefix":"outputs/"});
        assert_eq!(
            Inspect
                .call(fixture.request(&request, Some(USER)), Params::new())
                .await
                .expect_err("unsupported version")
                .code(),
            &s3::S3ErrorCode::InvalidRequest
        );
    }
    let request = json!({"version":2,"bucket":BUCKET,"base_prefix":"outputs/"});
    assert_eq!(
        Create
            .call(fixture.request(&request, Some(USER)), Params::new())
            .await
            .expect_err("create remains version 1")
            .code(),
        &s3::S3ErrorCode::InvalidRequest
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn artifact_backup_retention_requires_independent_scoped_authority() {
    let fixture = Fixture::new(prefix()).await;
    let body = json!({"version":1,"bucket":BUCKET,"base_prefix":"outputs/","bucket_incarnation":fixture.incarnation});
    for action in [
        "admin:GetPluginArtifactBucket",
        "admin:CreatePluginArtifactBucket",
        "admin:DeletePluginArtifactVersion",
    ] {
        fixture.grant_bucket_action(BUCKET, action).await;
        assert_eq!(
            RetainBackups
                .call(fixture.request(&body, Some(USER)), Params::new())
                .await
                .expect_err("unrelated action denied")
                .code(),
            &s3::S3ErrorCode::AccessDenied,
            "O02P3_SEPARATE_BACKUP_AUTHORITY"
        );
    }
    fixture
        .grant_bucket_action("another-bucket", "admin:RetainPluginArtifactBackups")
        .await;
    assert_eq!(
        RetainBackups
            .call(fixture.request(&body, Some(USER)), Params::new())
            .await
            .expect_err("wrong resource denied")
            .code(),
        &s3::S3ErrorCode::AccessDenied,
        "O02P3_BACKUP_BUCKET_SCOPE"
    );
    fixture.grant_bucket_action(BUCKET, "admin:RetainPluginArtifactBackups").await;
    let mut mismatch = body.clone();
    mismatch["bucket_incarnation"] = json!(Uuid::new_v4());
    assert!(
        RetainBackups
            .call(fixture.request(&mismatch, Some(USER)), Params::new())
            .await
            .is_err(),
        "O02P3_BACKUP_WRONG_GENERATION"
    );
    assert!(
        !fixture
            .store
            .inspect_artifact_backup_retention(BUCKET, fixture.incarnation, "outputs/")
            .await
            .expect("still unprotected")
    );
    let mut unknown = body.clone();
    unknown["retain"] = json!(false);
    assert!(
        RetainBackups
            .call(fixture.request(&unknown, Some(USER)), Params::new())
            .await
            .is_err(),
        "O02P3_NO_UNRETAIN_PROTOCOL"
    );
    for _ in 0..2 {
        let observed = receipt(RetainBackups.call(fixture.request(&body, Some(USER)), Params::new()).await).await;
        assert_eq!(
            observed,
            json!({"version":1,"bucket":BUCKET,"base_prefix":"outputs/","bucket_incarnation":fixture.incarnation,"backup_retained":true}),
            "O02P3_DURABLE_RETENTION_RECEIPT"
        );
    }
    fixture.grant_bucket_action(BUCKET, "admin:GetPluginArtifactBucket").await;
    for version in [1, 2, 3] {
        let request = json!({"version":version,"bucket":BUCKET,"base_prefix":"outputs/"});
        let observed = receipt(Inspect.call(fixture.request(&request, Some(USER)), Params::new()).await).await;
        if version == 3 {
            assert_eq!(observed["backup_retained"], true);
        } else {
            assert!(observed.get("backup_retained").is_none(), "O02P3_INSPECT_LEGACY_SHAPE");
        }
    }
    assert!(
        fixture
            .store
            .delete_bucket(
                BUCKET,
                &DeleteBucketOptions {
                    force: true,
                    ..Default::default()
                }
            )
            .await
            .is_err(),
        "O02P3_RETAINED_BUCKET_CANNOT_RECREATE"
    );
}
