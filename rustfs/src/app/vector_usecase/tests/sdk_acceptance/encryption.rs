// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{Fixture, SdkServer, cleanup_iam, run_large_stack_test, vector};
use crate::app::storage_api::test::contract::{list::ListOperations as _, object::ObjectIO as _};
use crate::app::storage_api::test::{
    StorageObjectOptions,
    sse::{
        EncryptionResolutionError, EncryptionResolutionErrorKind, kms_sse_dek_provider_for_test, reset_sse_dek_provider,
        set_sse_dek_provider_for_test,
    },
};
use crate::app::vector_usecase::{DefaultVectorUsecase, RequestTarget, dataset, index_namespace};
use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::table_catalog::{CatalogMetadataRef, CatalogObjectKey, VectorEffectiveEncryptionV1};
use crate::vector::{
    protocol::Request,
    service::{ManagedDatasetAccess, VectorRequestAccess as _},
};
use aws_sdk_s3vectors::{
    config::{ConfigBag, Intercept, RuntimeComponents, interceptors::AfterDeserializationInterceptorContextRef},
    error::{BoxError, ProvideErrorMetadata as _},
    types::{DataType, DistanceMetric, EncryptionConfiguration, IndexMode, SseType, VectorData},
};
use futures_util::FutureExt as _;
use rustfs_kms::{KmsConfig, KmsError, KmsServiceManager};
use rustfs_policy::policy::statement::RequestedKmsKey;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

const MAX_OBJECT_BYTES: u64 = 8 * 1024 * 1024;

struct ProviderReset;

impl Drop for ProviderReset {
    fn drop(&mut self) {
        reset_sse_dek_provider();
    }
}

pub(super) struct EncryptionFixture {
    pub(super) request: Fixture,
    pub(super) manager: Arc<KmsServiceManager>,
    config: KmsConfig,
    pub(super) key_arn: String,
    _reset: Option<ProviderReset>,
}

fn key_material() -> String {
    let mut bytes = Vec::from(Uuid::new_v4().as_bytes().as_slice());
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    base64_simd::STANDARD.encode_to_string(bytes)
}

impl EncryptionFixture {
    pub(super) async fn new(aes_default: bool) -> Self {
        let mut request = Fixture::new().await;
        assert!(request.app.kms().handle().get_encryption_service().await.is_none());
        let key_arn = format!("arn:aws:kms:{}:123456789012:key/{}", request.service.region, Uuid::new_v4());
        // SSE-S3 resolves the ambient default name before calling the injected provider.
        let key_id = if aes_default { "default".to_string() } else { key_arn.clone() };
        let config = KmsConfig::static_kms(key_id, key_material());
        let manager = Arc::new(KmsServiceManager::new());
        manager
            .reconfigure(config.clone())
            .await
            .expect("real Static KMS configuration");
        let app = Arc::new(AppContext::with_default_interfaces(
            request.app.object_store(),
            request.app.iam().handle(),
            manager.clone(),
        ));
        let slot = ServerContextSlot::new();
        assert!(slot.install(app.clone()));
        request.service = DefaultVectorUsecase::new(slot, request.service.limits, request.service.max_write_attempts).unwrap();
        request.app = app;
        reset_sse_dek_provider();
        let reset = ProviderReset;
        set_sse_dek_provider_for_test(kms_sse_dek_provider_for_test(manager.clone()).await.unwrap());
        let fixture = Self {
            request,
            manager,
            config,
            key_arn,
            _reset: Some(reset),
        };
        if aes_default {
            fixture.permissions_for_keys(&[], None).await;
        } else {
            fixture.permissions(None).await;
        }
        fixture
    }

    async fn permissions(&self, denied_action: Option<&str>) {
        let configured_key = self.config.default_key_id.as_deref().expect("fixture KMS default key");
        self.permissions_for_keys(&[configured_key], denied_action).await;
    }

    async fn permissions_for_keys(&self, key_ids: &[&str], denied_action: Option<&str>) {
        assert!(key_ids.len() <= 2, "the fixture grants at most two exact KMS resources");
        let key_resources: Vec<_> = key_ids
            .iter()
            .map(|key| {
                let RequestedKmsKey::Key(key_id) = RequestedKmsKey::parse(key) else {
                    panic!("fixture KMS key must have a concrete identifier");
                };
                assert!(!key_id.contains(['*', '?']), "fixture KMS resources must name exact keys");
                format!("arn:aws:kms:::key/{key_id}")
            })
            .collect();
        let mut statements = vec![
            json!({"Effect":"Allow","Action":"s3vectors:*","Resource":"*"}),
            json!({"Effect":"Deny","Action":"s3:*","Resource":"*"}),
        ];
        if !key_resources.is_empty() {
            statements.push(json!({"Effect":"Allow","Action":["kms:GenerateDataKey","kms:Decrypt"],"Resource":key_resources}));
        }
        if let Some(action) = denied_action {
            assert!(!key_resources.is_empty(), "a KMS Deny must target the fixture keys");
            statements.push(json!({"Effect":"Deny","Action":action,"Resource":key_resources}));
        }
        self.request.set_statements(serde_json::Value::Array(statements)).await;
    }

    fn kms(&self) -> EncryptionConfiguration {
        EncryptionConfiguration::builder()
            .sse_type(SseType::AwsKms)
            .kms_key_arn(&self.key_arn)
            .build()
    }
}

fn aes() -> EncryptionConfiguration {
    EncryptionConfiguration::builder().sse_type(SseType::Aes256).build()
}

macro_rules! sdk_error {
    ($result:expr, $code:literal, $status:literal) => {{
        let error = $result.expect_err("SDK request must fail");
        assert_eq!(error.as_service_error().unwrap().code(), Some($code));
        assert_eq!(error.raw_response().unwrap().status().as_u16(), $status);
    }};
}

async fn create_index(
    fixture: &EncryptionFixture,
    server: &SdkServer,
    bucket_encryption: Option<EncryptionConfiguration>,
    index_encryption: Option<EncryptionConfiguration>,
    expected: EncryptionConfiguration,
) -> String {
    let bucket = format!("sdk-encrypted-{}", Uuid::new_v4().simple());
    let bucket_wire = WireJson::default();
    let created_bucket = server
        .client
        .create_vector_bucket()
        .vector_bucket_name(&bucket)
        .set_encryption_configuration(bucket_encryption.clone())
        .customize()
        .interceptor(bucket_wire.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(bucket_wire.take(), json!({"vectorBucketArn":created_bucket.vector_bucket_arn().unwrap()}));
    let index_wire = WireJson::default();
    let found = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(found.vector_bucket().unwrap().encryption_configuration(), bucket_encryption.as_ref());
    let output = server
        .client
        .create_index()
        .vector_bucket_name(&bucket)
        .index_name("encrypted.index")
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .set_encryption_configuration(index_encryption)
        .customize()
        .interceptor(index_wire.clone())
        .send()
        .await
        .unwrap();
    let index_arn = output.index_arn().unwrap().to_string();
    assert_eq!(index_wire.take(), json!({"indexArn":index_arn}));
    let found = server.client.get_index().index_arn(&index_arn).send().await.unwrap();
    assert_eq!(found.index().unwrap().encryption_configuration(), Some(&expected));
    assert!(index_arn.starts_with(&format!("arn:aws:s3vectors:{}::bucket/", fixture.request.service.region)));
    index_arn
}

async fn managed_access(
    fixture: &Fixture,
    index_arn: &str,
    override_encryption: Option<VectorEffectiveEncryptionV1>,
) -> (ManagedDatasetAccess, CatalogMetadataRef, u64) {
    let request = Request::GetVectors(
        serde_json::from_value(json!({
            "indexArn":index_arn,"keys":["alpha"],"returnData":true,"returnMetadata":true
        }))
        .unwrap(),
    );
    let admitted = fixture.service.admit(fixture.verified(request)).await.unwrap();
    let catalog = admitted.catalog.as_ref().unwrap();
    let RequestTarget::Index { index, .. } = &admitted.target else {
        panic!("managed access requires an admitted Index target");
    };
    let index = index.clone();
    let (operation, entry) = catalog
        .begin_vector_operation(admitted.bucket.as_ref().unwrap(), &index_namespace().unwrap(), &index)
        .await
        .unwrap();
    let context = dataset::IndexRequest {
        owner: &fixture.service,
        authorization: &admitted,
        index: &index,
    };
    let mut definition = entry.definition().clone();
    if let Some(encryption) = override_encryption {
        definition.effective_encryption = encryption;
    }
    let access = context.managed_dataset(Arc::new(operation), &definition).await.unwrap();
    (access, entry.head().metadata().clone(), entry.head().generation().get())
}

#[derive(Debug, PartialEq, Eq)]
struct StoredObject {
    bytes: Vec<u8>,
    metadata: BTreeMap<String, String>,
    revision: Option<Uuid>,
    etag: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    bucket_incarnation: Uuid,
    table_id: String,
    prefix: String,
    generation: u64,
    manifest: String,
    objects: BTreeMap<String, StoredObject>,
}

pub(super) async fn snapshot(fixture: &EncryptionFixture, index_arn: &str, decrypt: bool) -> Snapshot {
    let (managed, manifest, generation) = managed_access(&fixture.request, index_arn, None).await;
    let store = fixture.request.app.object_store();
    let bucket = managed.access.bucket().storage_bucket_id().as_str();
    let page = store
        .clone()
        .list_objects_v2(bucket, managed.access.allowed_prefix(), None, None, 1000, false, None, false)
        .await
        .unwrap();
    assert!(!page.is_truncated, "the bounded test fixture must inspect every Dataset object");
    let mut objects = BTreeMap::new();
    let mut data_files = 0;
    for object in page.objects {
        let options = StorageObjectOptions {
            raw_data_movement_read: true,
            suppress_read_repair: true,
            expected_bucket_incarnation_id: Some(managed.access.bucket().incarnation().as_uuid()),
            ..Default::default()
        };
        let reader = store
            .get_object_reader(bucket, &object.name, None, http::HeaderMap::new(), &options)
            .await
            .unwrap();
        let info = reader.object_info;
        assert!(info.is_encrypted(), "every Dataset object must carry encryption metadata");
        let mut bytes = Vec::new();
        reader
            .stream
            .take(MAX_OBJECT_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert!(u64::try_from(bytes.len()).unwrap() <= MAX_OBJECT_BYTES);
        assert_eq!(i64::try_from(bytes.len()).unwrap(), info.size);
        let metadata: BTreeMap<_, _> = info
            .user_defined
            .iter()
            .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
            .collect();
        let mut mirrored = 0;
        for (key, value) in &metadata {
            if let Some(suffix) = key.strip_prefix("x-rustfs-internal-") {
                assert_eq!(metadata.get(&format!("x-minio-internal-{suffix}")), Some(value));
                mirrored += 1;
            }
        }
        assert!(mirrored > 0, "encrypted metadata must include both internal prefixes");
        if decrypt {
            let key = CatalogObjectKey::try_new(object.name.clone()).unwrap();
            let plaintext = managed.objects.get(&managed.access, &key, None, info.data_dir).await.unwrap();
            assert_eq!(i64::try_from(plaintext.data.len()).unwrap(), info.get_actual_size().unwrap());
            assert_ne!(bytes.as_slice(), plaintext.data.as_slice(), "stored bytes must be ciphertext");
            assert!(bytes.len() > plaintext.data.len(), "authenticated encryption adds framing");
            if key == *manifest.object_key() {
                assert_eq!(u64::try_from(plaintext.data.len()).unwrap(), manifest.content_length());
                assert_eq!(
                    hex_simd::encode_to_string(Sha256::digest(&plaintext.data), hex_simd::AsciiCase::Lower),
                    manifest.content_sha256().to_hex()
                );
            }
        }
        if object.name.ends_with(".lance") {
            data_files += 1;
        }
        objects.insert(
            object.name,
            StoredObject {
                bytes,
                metadata,
                revision: info.data_dir,
                etag: info.etag,
            },
        );
    }
    assert!(data_files > 0, "the fixture must contain persisted vector data");
    assert!(
        objects.contains_key(manifest.object_key().as_str()),
        "the exact Catalog manifest must be inspected"
    );
    Snapshot {
        bucket_incarnation: managed.access.bucket().incarnation().as_uuid(),
        table_id: managed.access.table_id().as_str().to_owned(),
        prefix: managed.access.allowed_prefix().to_owned(),
        generation,
        manifest: manifest.object_key().as_str().to_string(),
        objects,
    }
}

async fn put_and_read(server: &SdkServer, index_arn: &str) {
    server
        .client
        .put_vectors()
        .index_arn(index_arn)
        .vectors(vector("alpha", [1.0, 2.0], "encrypted-alpha"))
        .vectors(vector("beta", [3.0, 4.0], "encrypted-beta"))
        .send()
        .await
        .unwrap();
    assert_read(server, index_arn).await;
    let listed = server
        .client
        .list_vectors()
        .index_arn(index_arn)
        .return_data(true)
        .return_metadata(true)
        .max_results(100)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.vectors().len(), 2);
    assert!(listed.next_token().is_none());
    let queried = server
        .client
        .query_vectors()
        .index_arn(index_arn)
        .query_vector(VectorData::Float32(vec![0.0, 0.0]))
        .top_k(1)
        .return_metadata(true)
        .return_distance(true)
        .send()
        .await
        .unwrap();
    assert_eq!(queried.vectors()[0].key(), "alpha");
    let distance = queried.vectors()[0].distance().expect("requested Euclidean distance");
    let expected_distance = 5.0_f32.sqrt();
    assert!(
        (distance - expected_distance).abs() <= expected_distance * f32::EPSILON,
        "expected Euclidean distance {expected_distance}, received {distance}"
    );
    assert_eq!(queried.vectors()[0].metadata(), Some(&super::metadata("encrypted-alpha")));
}

async fn assert_read(server: &SdkServer, index_arn: &str) {
    let read = server
        .client
        .get_vectors()
        .index_arn(index_arn)
        .keys("alpha")
        .return_data(true)
        .return_metadata(true)
        .send()
        .await
        .unwrap();
    assert_eq!(read.vectors().len(), 1);
    assert_eq!(read.vectors()[0].key(), "alpha");
    assert_eq!(read.vectors()[0].data(), Some(&VectorData::Float32(vec![1.0, 2.0])));
    assert_eq!(read.vectors()[0].metadata(), Some(&super::metadata("encrypted-alpha")));
}

async fn success_case(
    fixture: &EncryptionFixture,
    server: &SdkServer,
    bucket: Option<EncryptionConfiguration>,
    index: Option<EncryptionConfiguration>,
    expected: EncryptionConfiguration,
) {
    let arn = create_index(fixture, server, bucket, index, expected).await;
    put_and_read(server, &arn).await;
    assert_configuration_stability(fixture, server, &arn).await;
    snapshot(fixture, &arn, true).await;
    server
        .client
        .tag_resource()
        .resource_arn(&arn)
        .tags("encrypted", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(
        server
            .client
            .list_tags_for_resource()
            .resource_arn(&arn)
            .send()
            .await
            .unwrap()
            .tags()
            .get("encrypted")
            .map(String::as_str),
        Some("true")
    );
    server
        .client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("encrypted")
        .send()
        .await
        .unwrap();
    server
        .client
        .delete_vectors()
        .index_arn(&arn)
        .keys("beta")
        .send()
        .await
        .unwrap();
    assert_read(server, &arn).await;
    let listed = server
        .client
        .list_vectors()
        .index_arn(&arn)
        .return_data(true)
        .return_metadata(true)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.vectors().len(), 1);
    assert!(listed.next_token().is_none());
    snapshot(fixture, &arn, true).await;
    server
        .client
        .delete_vectors()
        .index_arn(&arn)
        .keys("alpha")
        .send()
        .await
        .unwrap();
    server.client.delete_index().index_arn(&arn).send().await.unwrap();
    sdk_error!(server.client.get_index().index_arn(&arn).send().await, "NotFoundException", 404);
}

fn assert_snapshot_encryption(snapshot: &Snapshot, expected: &EncryptionConfiguration) {
    let expected_type = expected.sse_type().as_str();
    for object in snapshot.objects.values() {
        assert_eq!(
            object.metadata.get("x-amz-server-side-encryption").map(String::as_str),
            Some(expected_type)
        );
        assert_eq!(
            object
                .metadata
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(String::as_str),
            expected.kms_key_arn()
        );
    }
}

async fn create_override_index(
    server: &SdkServer,
    bucket: &str,
    encryption: EncryptionConfiguration,
    empty_configuration: bool,
) -> String {
    let changed = Arc::new(AtomicBool::new(false));
    let mut request = server
        .client
        .create_index()
        .vector_bucket_name(bucket)
        .index_name("override.index")
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .encryption_configuration(encryption)
        .customize();
    if empty_configuration {
        let changed = changed.clone();
        request = request.mutate_request(move |request| {
            let mut body: serde_json::Value =
                serde_json::from_slice(request.body().bytes().expect("buffered SDK JSON request")).unwrap();
            assert_eq!(body["encryptionConfiguration"], json!({"sseType":"AES256"}));
            body["encryptionConfiguration"] = json!({});
            let bytes = serde_json::to_vec(&body).unwrap();
            request.headers_mut().insert("content-length", bytes.len().to_string());
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(bytes);
            let outgoing: serde_json::Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            assert_eq!(outgoing["encryptionConfiguration"], json!({}));
            changed.store(true, Ordering::SeqCst);
        });
    }
    let output = request.send().await.unwrap();
    assert_eq!(changed.load(Ordering::SeqCst), empty_configuration);
    output.index_arn().unwrap().to_owned()
}

async fn override_case(
    fixture: &EncryptionFixture,
    server: &SdkServer,
    bucket_encryption: EncryptionConfiguration,
    index_encryption: EncryptionConfiguration,
    replacement_config: KmsConfig,
    empty_configuration: bool,
) {
    let original_arn = create_index(fixture, server, Some(bucket_encryption.clone()), None, bucket_encryption.clone()).await;
    put_and_read(server, &original_arn).await;
    let original = server.client.get_index().index_arn(&original_arn).send().await.unwrap();
    let bucket_name = original.index().unwrap().vector_bucket_name();
    let bucket_before = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(bucket_name)
        .send()
        .await
        .unwrap();
    let before = snapshot(fixture, &original_arn, true).await;
    assert_snapshot_encryption(&before, &bucket_encryption);
    let base_keys: Vec<_> = bucket_encryption.kms_key_arn().into_iter().collect();
    let override_keys: Vec<_> = if matches!(index_encryption.sse_type(), SseType::AwsKms) {
        bucket_encryption
            .kms_key_arn()
            .into_iter()
            .chain(index_encryption.kms_key_arn())
            .collect()
    } else {
        Vec::new()
    };
    let result = AssertUnwindSafe(async {
        // Static serves one key at a time; each operation uses the currently configured key.
        fixture.manager.reconfigure(replacement_config.clone()).await.unwrap();
        fixture.permissions_for_keys(&override_keys, None).await;
        let overridden_arn = create_override_index(server, bucket_name, index_encryption.clone(), empty_configuration).await;
        let overridden = server.client.get_index().index_arn(&overridden_arn).send().await.unwrap();
        assert_eq!(overridden.index().unwrap().encryption_configuration(), Some(&index_encryption));
        put_and_read(server, &overridden_arn).await;
        let overridden_before = snapshot(fixture, &overridden_arn, true).await;
        assert_snapshot_encryption(&overridden_before, &index_encryption);
        assert_eq!(before.bucket_incarnation, overridden_before.bucket_incarnation);
        assert_ne!(before.table_id, overridden_before.table_id);
        assert_ne!(before.prefix, overridden_before.prefix);

        fixture.manager.reconfigure(fixture.config.clone()).await.unwrap();
        fixture.permissions_for_keys(&base_keys, None).await;
        assert_read(server, &original_arn).await;
        let original_after = server.client.get_index().index_arn(&original_arn).send().await.unwrap();
        assert_eq!(original.index(), original_after.index());
        let bucket_after = server
            .client
            .get_vector_bucket()
            .vector_bucket_name(bucket_name)
            .send()
            .await
            .unwrap();
        assert_eq!(bucket_before.vector_bucket(), bucket_after.vector_bucket());
        // The captured generation belongs to this Index's TableEntry, together with its identity.
        assert_eq!(before, snapshot(fixture, &original_arn, true).await);

        fixture.manager.reconfigure(replacement_config).await.unwrap();
        fixture.permissions_for_keys(&override_keys, None).await;
        assert_read(server, &overridden_arn).await;
        assert_eq!(overridden_before, snapshot(fixture, &overridden_arn, true).await);
        server
            .client
            .delete_vectors()
            .index_arn(&overridden_arn)
            .keys("beta")
            .send()
            .await
            .unwrap();
        assert_read(server, &overridden_arn).await;
        assert_snapshot_encryption(&snapshot(fixture, &overridden_arn, true).await, &index_encryption);
        server.client.delete_index().index_arn(&overridden_arn).send().await.unwrap();
        sdk_error!(
            server.client.get_index().index_arn(&overridden_arn).send().await,
            "NotFoundException",
            404
        );
    })
    .catch_unwind()
    .await;
    fixture.manager.reconfigure(fixture.config.clone()).await.unwrap();
    fixture.permissions_for_keys(&base_keys, None).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert_read(server, &original_arn).await;
    assert_eq!(before, snapshot(fixture, &original_arn, true).await);
    server.client.delete_index().index_arn(&original_arn).send().await.unwrap();
    sdk_error!(server.client.get_index().index_arn(&original_arn).send().await, "NotFoundException", 404);
}

async fn permissions_case(fixture: &EncryptionFixture, server: &SdkServer) {
    let arn = create_index(fixture, server, Some(fixture.kms()), None, fixture.kms()).await;
    put_and_read(server, &arn).await;
    let before = snapshot(fixture, &arn, true).await;
    fixture.permissions(Some("kms:GenerateDataKey")).await;
    sdk_error!(
        server
            .client
            .put_vectors()
            .index_arn(&arn)
            .vectors(vector("blocked", [5.0, 6.0], "blocked"))
            .send()
            .await,
        "AccessDeniedException",
        403
    );
    sdk_error!(
        server.client.delete_vectors().index_arn(&arn).keys("alpha").send().await,
        "AccessDeniedException",
        403
    );
    let absent = format!("denied-encryption-{}", Uuid::new_v4().simple());
    sdk_error!(
        server
            .client
            .create_vector_bucket()
            .vector_bucket_name(&absent)
            .encryption_configuration(fixture.kms())
            .send()
            .await,
        "AccessDeniedException",
        403
    );
    assert_read(server, &arn).await;
    fixture.permissions(Some("kms:Decrypt")).await;
    sdk_error!(
        server
            .client
            .get_vectors()
            .index_arn(&arn)
            .keys("alpha")
            .return_data(true)
            .send()
            .await,
        "AccessDeniedException",
        403
    );
    sdk_error!(
        server.client.list_vectors().index_arn(&arn).return_data(true).send().await,
        "AccessDeniedException",
        403
    );
    sdk_error!(
        server
            .client
            .query_vectors()
            .index_arn(&arn)
            .query_vector(VectorData::Float32(vec![0.0, 0.0]))
            .top_k(1)
            .send()
            .await,
        "AccessDeniedException",
        403
    );
    sdk_error!(
        server
            .client
            .put_vectors()
            .index_arn(&arn)
            .vectors(vector("blocked", [5.0, 6.0], "blocked"))
            .send()
            .await,
        "AccessDeniedException",
        403
    );
    fixture.permissions(None).await;
    assert_eq!(
        before,
        snapshot(fixture, &arn, true).await,
        "denied requests must preserve the complete Dataset and Catalog head"
    );
    assert_read(server, &arn).await;
    sdk_error!(
        server.client.get_vector_bucket().vector_bucket_name(&absent).send().await,
        "NotFoundException",
        404
    );
}

#[derive(Clone, Copy, Debug)]
enum MockKmsKeyState {
    Disabled,
    InvalidState,
    InvalidKeyUsage,
    NotFound,
}

impl MockKmsKeyState {
    fn data_plane_error_code(self) -> &'static str {
        match self {
            Self::Disabled => "KmsDisabledException",
            Self::InvalidState => "KmsInvalidStateException",
            Self::InvalidKeyUsage => "KmsInvalidKeyUsageException",
            Self::NotFound => "KmsNotFoundException",
        }
    }

    fn expected_message(self) -> &'static str {
        match self {
            Self::Disabled => "KMS key is disabled",
            Self::InvalidState => "KMS key is in an invalid state",
            Self::InvalidKeyUsage => "KMS key cannot encrypt Vector data",
            Self::NotFound => "KMS key was not found",
        }
    }
}

struct MockKmsDescribeServer {
    endpoint: String,
    state: Arc<Mutex<MockKmsKeyState>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockKmsDescribeServer {
    async fn start(key_arn: String) -> Self {
        use bytes::Bytes;
        use http_body_util::{BodyExt as _, Full};

        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockKmsKeyState::Disabled));
        let observed_state = state.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    incoming = listener.accept() => {
                        let (stream, _) = incoming.unwrap();
                        let state = observed_state.clone();
                        let key_arn = key_arn.clone();
                        connections.spawn(async move {
                            let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
                                let state = state.clone();
                                let key_arn = key_arn.clone();
                                async move {
                                    let target = request
                                        .headers()
                                        .get("x-amz-target")
                                        .and_then(|value| value.to_str().ok())
                                        .unwrap_or_default()
                                        .to_owned();
                                    let raw_body = request.into_body().collect().await.unwrap().to_bytes();
                                    let req_json: serde_json::Value =
                                        serde_json::from_slice(&raw_body).unwrap_or_else(|_| json!({}));
                                    let (status, payload) = match target.as_str() {
                                        "TrentService.ListKeys" => (
                                            http::StatusCode::OK,
                                            json!({
                                                "Keys": [],
                                                "Truncated": false
                                            }),
                                        ),
                                        "TrentService.DescribeKey" if req_json["KeyId"].as_str() != Some(key_arn.as_str()) => (
                                            http::StatusCode::BAD_REQUEST,
                                            json!({
                                                "__type": "NotFoundException",
                                                "message": "Key does not exist"
                                            }),
                                        ),
                                        "TrentService.DescribeKey" => {
                                            let mode = *state.lock().unwrap();
                                            match mode {
                                                MockKmsKeyState::Disabled => (
                                                    http::StatusCode::OK,
                                                    json!({
                                                        "KeyMetadata": {
                                                            "KeyId": key_arn,
                                                            "Arn": key_arn,
                                                            "CreationDate": 1_700_000_000,
                                                            "Enabled": false,
                                                            "Description": "disabled test key",
                                                            "KeyUsage": "ENCRYPT_DECRYPT",
                                                            "KeyState": "Disabled",
                                                            "Origin": "AWS_KMS",
                                                            "KeyManager": "CUSTOMER"
                                                        }
                                                    }),
                                                ),
                                                MockKmsKeyState::InvalidState => (
                                                    http::StatusCode::OK,
                                                    json!({
                                                        "KeyMetadata": {
                                                            "KeyId": key_arn,
                                                            "Arn": key_arn,
                                                            "CreationDate": 1_700_000_000,
                                                            "Enabled": false,
                                                            "Description": "pending deletion test key",
                                                            "KeyUsage": "ENCRYPT_DECRYPT",
                                                            "KeyState": "PendingDeletion",
                                                            "Origin": "AWS_KMS",
                                                            "KeyManager": "CUSTOMER"
                                                        }
                                                    }),
                                                ),
                                                MockKmsKeyState::InvalidKeyUsage => (
                                                    http::StatusCode::OK,
                                                    json!({
                                                        "KeyMetadata": {
                                                            "KeyId": key_arn,
                                                            "Arn": key_arn,
                                                            "CreationDate": 1_700_000_000,
                                                            "Enabled": true,
                                                            "Description": "sign-verify test key",
                                                            "KeyUsage": "SIGN_VERIFY",
                                                            "KeyState": "Enabled",
                                                            "Origin": "AWS_KMS",
                                                            "KeyManager": "CUSTOMER"
                                                        }
                                                    }),
                                                ),
                                                MockKmsKeyState::NotFound => (
                                                    http::StatusCode::BAD_REQUEST,
                                                    json!({
                                                        "__type": "NotFoundException",
                                                        "message": "Key does not exist"
                                                    }),
                                                ),
                                            }
                                        }
                                        unexpected => panic!("unexpected KMS target: {unexpected}"),
                                    };
                                    let bytes = serde_json::to_vec(&payload).unwrap();
                                    let response = http::Response::builder()
                                        .status(status)
                                        .header(http::header::CONTENT_TYPE, "application/x-amz-json-1.1")
                                        .header(http::header::CONTENT_LENGTH, bytes.len())
                                        .body(Full::new(Bytes::from(bytes)))
                                        .unwrap();
                                    Ok::<_, std::convert::Infallible>(response)
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .keep_alive(false)
                                .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                                .await;
                        });
                    }
                    completed = connections.join_next(), if !connections.is_empty() => {
                        completed.unwrap().unwrap();
                    }
                }
            }
            while let Some(completed) = connections.join_next().await {
                completed.unwrap();
            }
        });
        Self {
            endpoint: format!("http://{address}"),
            state,
            stop: Some(stop),
            task,
        }
    }

    fn set_state(&self, next: MockKmsKeyState) {
        *self.state.lock().unwrap() = next;
    }

    async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.await.unwrap();
    }
}

fn assert_modeled_sdk_error<T, E>(
    result: Result<T, aws_sdk_s3vectors::error::SdkError<E>>,
    expected_code: &str,
    expected_status: u16,
    expected_message: Option<&str>,
    matches_variant: impl FnOnce(&E) -> bool,
) where
    T: std::fmt::Debug,
    E: std::fmt::Debug + aws_sdk_s3vectors::error::ProvideErrorMetadata,
{
    let error = result.expect_err("SDK request must fail");
    let response = error.raw_response().expect("real HTTP error response");
    assert_eq!(response.status().as_u16(), expected_status);
    assert_eq!(response.headers().get("content-type"), Some("application/json"));
    assert_eq!(response.headers().get("x-amzn-errortype"), Some(expected_code));
    let body: serde_json::Value = serde_json::from_slice(response.body().bytes().expect("buffered error JSON")).unwrap();
    assert_eq!(body["__type"], expected_code);
    let typed = error.as_service_error().expect("modeled SDK service error");
    assert!(
        matches_variant(typed),
        "SDK error variant mismatch (or unmodeled Unhandled variant) for {expected_code}: {typed:?}"
    );
    assert_eq!(typed.code(), Some(expected_code));
    if let Some(expected_message) = expected_message {
        assert_eq!(body["message"].as_str(), Some(expected_message));
        assert_eq!(typed.message(), Some(expected_message));
    }
}

async fn kms_key_states_case(fixture: &EncryptionFixture, server: &SdkServer) {
    let arn = create_index(fixture, server, Some(fixture.kms()), None, fixture.kms()).await;
    put_and_read(server, &arn).await;
    let index = server.client.get_index().index_arn(&arn).send().await.unwrap();
    let kms_bucket = index.index().unwrap().vector_bucket_name().to_owned();
    let plain_bucket = format!("sdk-plain-host-{}", Uuid::new_v4().simple());
    server
        .client
        .create_vector_bucket()
        .vector_bucket_name(&plain_bucket)
        .send()
        .await
        .unwrap();
    let first_page = server
        .client
        .list_vectors()
        .index_arn(&arn)
        .max_results(1)
        .return_data(true)
        .send()
        .await
        .unwrap();
    let list_next_token = first_page
        .next_token()
        .expect("two-vector fixture yields a second page")
        .to_owned();
    let before = snapshot(fixture, &arn, true).await;

    // Verify 403 AccessDeniedException remains untouched across non-vector-data operations too.
    fixture.permissions(Some("kms:Decrypt")).await;
    assert_modeled_sdk_error(
        server
            .client
            .create_index()
            .vector_bucket_name(&kms_bucket)
            .index_name("denied.index")
            .data_type(DataType::Float32)
            .dimension(2)
            .distance_metric(DistanceMetric::Euclidean)
            .send()
            .await,
        "AccessDeniedException",
        403,
        None,
        |e| e.is_access_denied_exception(),
    );
    assert_modeled_sdk_error(
        server.client.delete_index().index_arn(&arn).send().await,
        "AccessDeniedException",
        403,
        None,
        |e| e.is_access_denied_exception(),
    );
    assert_modeled_sdk_error(
        server
            .client
            .tag_resource()
            .resource_arn(&arn)
            .tags("env", "denied")
            .send()
            .await,
        "AccessDeniedException",
        403,
        None,
        |e| e.is_access_denied_exception(),
    );
    assert_modeled_sdk_error(
        server.client.untag_resource().resource_arn(&arn).tag_keys("env").send().await,
        "AccessDeniedException",
        403,
        None,
        |e| e.is_access_denied_exception(),
    );
    fixture.permissions(None).await;

    let kms_server = MockKmsDescribeServer::start(fixture.key_arn.clone()).await;
    let mut aws_kms = KmsConfig::aws(Some(fixture.request.service.region.clone()));
    if let rustfs_kms::config::BackendConfig::Aws(aws) = &mut aws_kms.backend_config {
        aws.endpoint_url = Some(kms_server.endpoint.clone());
    }
    aws_kms.allow_insecure_dev_defaults = true;
    aws_kms.enable_cache = false;
    aws_kms.retry_attempts = 1;

    let absent_bucket = format!("kms-state-bucket-{}", Uuid::new_v4().simple());
    let absent_inherited_index = "kms-state-inherited.index";
    let absent_explicit_index = "kms-state-explicit.index";

    let outcome = AssertUnwindSafe(async {
        fixture.manager.reconfigure(aws_kms).await.unwrap();

        for state in [
            MockKmsKeyState::Disabled,
            MockKmsKeyState::InvalidState,
            MockKmsKeyState::InvalidKeyUsage,
            MockKmsKeyState::NotFound,
        ] {
            kms_server.set_state(state);
            let data_code = state.data_plane_error_code();
            let message = state.expected_message();

            // 1. PutVectors / GetVectors / DeleteVectors / QueryVectors keep their 4 specific Kms*Exception variants (E10).
            assert_modeled_sdk_error(
                server
                    .client
                    .put_vectors()
                    .index_arn(&arn)
                    .vectors(vector("blocked", [5.0, 6.0], "blocked"))
                    .send()
                    .await,
                data_code,
                400,
                Some(message),
                |e| match state {
                    MockKmsKeyState::Disabled => e.is_kms_disabled_exception(),
                    MockKmsKeyState::InvalidState => e.is_kms_invalid_state_exception(),
                    MockKmsKeyState::InvalidKeyUsage => e.is_kms_invalid_key_usage_exception(),
                    MockKmsKeyState::NotFound => e.is_kms_not_found_exception(),
                },
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .get_vectors()
                    .index_arn(&arn)
                    .keys("alpha")
                    .return_data(true)
                    .send()
                    .await,
                data_code,
                400,
                Some(message),
                |e| match state {
                    MockKmsKeyState::Disabled => e.is_kms_disabled_exception(),
                    MockKmsKeyState::InvalidState => e.is_kms_invalid_state_exception(),
                    MockKmsKeyState::InvalidKeyUsage => e.is_kms_invalid_key_usage_exception(),
                    MockKmsKeyState::NotFound => e.is_kms_not_found_exception(),
                },
            );
            assert_modeled_sdk_error(
                server.client.delete_vectors().index_arn(&arn).keys("alpha").send().await,
                data_code,
                400,
                Some(message),
                |e| match state {
                    MockKmsKeyState::Disabled => e.is_kms_disabled_exception(),
                    MockKmsKeyState::InvalidState => e.is_kms_invalid_state_exception(),
                    MockKmsKeyState::InvalidKeyUsage => e.is_kms_invalid_key_usage_exception(),
                    MockKmsKeyState::NotFound => e.is_kms_not_found_exception(),
                },
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .query_vectors()
                    .index_arn(&arn)
                    .query_vector(VectorData::Float32(vec![0.0, 0.0]))
                    .top_k(1)
                    .send()
                    .await,
                data_code,
                400,
                Some(message),
                |e| match state {
                    MockKmsKeyState::Disabled => e.is_kms_disabled_exception(),
                    MockKmsKeyState::InvalidState => e.is_kms_invalid_state_exception(),
                    MockKmsKeyState::InvalidKeyUsage => e.is_kms_invalid_key_usage_exception(),
                    MockKmsKeyState::NotFound => e.is_kms_not_found_exception(),
                },
            );

            // 2. Operations that do not declare Kms*Exception normalize to modeled ValidationException (400) (E11).
            assert_modeled_sdk_error(
                server
                    .client
                    .create_vector_bucket()
                    .vector_bucket_name(&absent_bucket)
                    .encryption_configuration(fixture.kms())
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .create_index()
                    .vector_bucket_name(&kms_bucket)
                    .index_name(absent_inherited_index)
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .create_index()
                    .vector_bucket_name(&plain_bucket)
                    .index_name(absent_explicit_index)
                    .data_type(DataType::Float32)
                    .dimension(2)
                    .distance_metric(DistanceMetric::Euclidean)
                    .encryption_configuration(fixture.kms())
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server.client.list_vectors().index_arn(&arn).return_data(true).send().await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .list_vectors()
                    .index_arn(&arn)
                    .next_token(&list_next_token)
                    .return_data(true)
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server.client.delete_index().index_arn(&arn).send().await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .tag_resource()
                    .resource_arn(&arn)
                    .tags("state", "blocked")
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
            assert_modeled_sdk_error(
                server
                    .client
                    .untag_resource()
                    .resource_arn(&arn)
                    .tag_keys("state")
                    .send()
                    .await,
                "ValidationException",
                400,
                Some(message),
                |e| e.is_validation_exception(),
            );
        }
    })
    .catch_unwind()
    .await;

    fixture.manager.reconfigure(fixture.config.clone()).await.unwrap();
    kms_server.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }

    assert_eq!(
        before,
        snapshot(fixture, &arn, true).await,
        "KMS state rejections must preserve the Dataset and Catalog head"
    );
    assert!(
        server
            .client
            .list_tags_for_resource()
            .resource_arn(&arn)
            .send()
            .await
            .unwrap()
            .tags()
            .is_empty()
    );
    assert_read(server, &arn).await;
    sdk_error!(
        server
            .client
            .get_vector_bucket()
            .vector_bucket_name(&absent_bucket)
            .send()
            .await,
        "NotFoundException",
        404
    );
    sdk_error!(
        server
            .client
            .get_index()
            .vector_bucket_name(&kms_bucket)
            .index_name(absent_inherited_index)
            .send()
            .await,
        "NotFoundException",
        404
    );
    sdk_error!(
        server
            .client
            .get_index()
            .vector_bucket_name(&plain_bucket)
            .index_name(absent_explicit_index)
            .send()
            .await,
        "NotFoundException",
        404
    );
}

fn has_error<E: std::error::Error + 'static>(error: &(dyn std::error::Error + 'static), predicate: impl Fn(&E) -> bool) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.downcast_ref::<E>().is_some_and(&predicate) {
            return true;
        }
        current = if let Some(error) = error.downcast_ref::<std::io::Error>() {
            error.get_ref().map(|inner| inner as &(dyn std::error::Error + 'static))
        } else {
            error.source()
        };
    }
    false
}

#[test]
fn vector_encryption_error_probe_preserves_io_wrapped_types() {
    let error = crate::app::storage_api::vector_usecase::StorageError::other(KmsError::cryptographic_error(
        "decrypt",
        "fixture key material mismatch",
    ));
    assert!(has_error::<KmsError>(&error, |error| matches!(
        error,
        KmsError::CryptographicError { .. }
    )));
    assert!(!has_error::<KmsError>(&error, |error| matches!(error, KmsError::KeyNotFound { .. })));
    assert!(!has_error::<crate::app::object::ManagedObjectError>(&error, |_| true));
    assert!(!has_error::<KmsError>(&std::io::Error::from(std::io::ErrorKind::Other), |_| true));

    for kind in [
        EncryptionResolutionErrorKind::InvalidRequest,
        EncryptionResolutionErrorKind::InvalidMetadata,
        EncryptionResolutionErrorKind::ServiceUnavailable,
        EncryptionResolutionErrorKind::KeyNotFound,
        EncryptionResolutionErrorKind::AccessDenied,
        EncryptionResolutionErrorKind::NotImplemented,
        EncryptionResolutionErrorKind::DecryptionFailed,
    ] {
        let storage = crate::app::storage_api::vector_usecase::StorageError::other(EncryptionResolutionError::new(
            kind,
            "fixture envelope resolution failure",
        ));
        let error = crate::vector::lance::LanceManifestError::Storage {
            operation: "read managed manifest",
            source: Box::new(crate::app::object::ManagedObjectError::Api(crate::error::ApiError::from(storage))),
        };
        assert!(has_error::<EncryptionResolutionError>(&error, |error| error.kind() == kind));
        assert_eq!(
            has_error::<EncryptionResolutionError>(&error, |error| error.kind()
                == EncryptionResolutionErrorKind::DecryptionFailed),
            kind == EncryptionResolutionErrorKind::DecryptionFailed,
            "only the envelope decryption failure may satisfy the mismatch assertion"
        );
    }
}

async fn mismatch_case(fixture: &EncryptionFixture, server: &SdkServer) {
    let arn = create_index(fixture, server, Some(fixture.kms()), None, fixture.kms()).await;
    put_and_read(server, &arn).await;
    let before = snapshot(fixture, &arn, true).await;
    let absent = format!("invalid-encryption-{}", Uuid::new_v4().simple());
    let invalid = EncryptionConfiguration::builder()
        .sse_type(SseType::Aes256)
        .kms_key_arn(&fixture.key_arn)
        .build();
    sdk_error!(
        server
            .client
            .create_vector_bucket()
            .vector_bucket_name(&absent)
            .encryption_configuration(invalid)
            .send()
            .await,
        "ValidationException",
        400
    );
    let (managed, manifest, _) = managed_access(&fixture.request, &arn, Some(VectorEffectiveEncryptionV1::Aes256)).await;
    let error = managed
        .objects
        .get(&managed.access, manifest.object_key(), None, None)
        .await
        .unwrap_err();
    assert!(
        has_error::<crate::app::object::ManagedObjectError>(&error, |error| matches!(
            error,
            crate::app::object::ManagedObjectError::EncryptionMismatch
        )),
        "{error:?}"
    );
    drop(managed);
    fixture
        .manager
        .reconfigure(KmsConfig::static_kms(fixture.key_arn.clone(), key_material()))
        .await
        .unwrap();
    let (managed, manifest, _) = managed_access(&fixture.request, &arn, None).await;
    let error = managed
        .objects
        .get(&managed.access, manifest.object_key(), None, None)
        .await
        .unwrap_err();
    assert!(
        has_error::<EncryptionResolutionError>(&error, |error| error.kind() == EncryptionResolutionErrorKind::DecryptionFailed),
        "{error:?}"
    );
    drop(managed);
    let error = server
        .client
        .get_vectors()
        .index_arn(&arn)
        .keys("alpha")
        .return_data(true)
        .send()
        .await
        .expect_err("wrong KMS material must not return plaintext");
    assert!((500..600).contains(&error.raw_response().unwrap().status().as_u16()));
    fixture.manager.reconfigure(fixture.config.clone()).await.unwrap();
    assert_read(server, &arn).await;
    assert_eq!(before, snapshot(fixture, &arn, true).await);
    sdk_error!(
        server.client.get_vector_bucket().vector_bucket_name(&absent).send().await,
        "NotFoundException",
        404
    );
}

#[derive(Clone, Copy)]
enum Case {
    EmptyBucket,
    Aes,
    Kms,
    Permissions,
    KeyStates,
    Mismatch,
}

fn run_case(name: &'static str, case: Case) {
    run_large_stack_test(name, move || async move {
        let fixture = EncryptionFixture::new(matches!(case, Case::Aes | Case::EmptyBucket)).await;
        let server = SdkServer::new(&fixture.request).await;
        let result = AssertUnwindSafe(async {
            match case {
                Case::EmptyBucket => empty_bucket_case(&fixture, &server).await,
                Case::Aes => {
                    success_case(&fixture, &server, Some(aes()), None, aes()).await;
                    success_case(&fixture, &server, None, Some(aes()), aes()).await;
                    success_case(&fixture, &server, Some(aes()), Some(EncryptionConfiguration::builder().build()), aes()).await;
                    override_case(
                        &fixture,
                        &server,
                        aes(),
                        fixture.kms(),
                        KmsConfig::static_kms(fixture.key_arn.clone(), key_material()),
                        false,
                    )
                    .await;
                }
                Case::Kms => {
                    success_case(&fixture, &server, Some(fixture.kms()), None, fixture.kms()).await;
                    success_case(&fixture, &server, None, Some(fixture.kms()), fixture.kms()).await;
                    success_case(&fixture, &server, Some(fixture.kms()), Some(fixture.kms()), fixture.kms()).await;
                    for empty_configuration in [false, true] {
                        override_case(
                            &fixture,
                            &server,
                            fixture.kms(),
                            aes(),
                            KmsConfig::static_kms("default".to_owned(), key_material()),
                            empty_configuration,
                        )
                        .await;
                    }
                    let other_key_arn =
                        format!("arn:aws:kms:{}:123456789012:key/{}", fixture.request.service.region, Uuid::new_v4());
                    assert_ne!(other_key_arn, fixture.key_arn);
                    let other_encryption = EncryptionConfiguration::builder()
                        .sse_type(SseType::AwsKms)
                        .kms_key_arn(&other_key_arn)
                        .build();
                    override_case(
                        &fixture,
                        &server,
                        fixture.kms(),
                        other_encryption,
                        KmsConfig::static_kms(other_key_arn, key_material()),
                        false,
                    )
                    .await;
                }
                Case::Permissions => permissions_case(&fixture, &server).await,
                Case::KeyStates => kms_key_states_case(&fixture, &server).await,
                Case::Mismatch => mismatch_case(&fixture, &server).await,
            }
        })
        .catch_unwind()
        .await;
        server.finish().await;
        cleanup_iam(&fixture.request).await;
        fixture.manager.stop().await.unwrap();
        drop(fixture);
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_aes256_inheritance_encrypts_persisted_dataset() {
    run_case("vector-sdk-aes256", Case::Aes);
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_kms_inheritance_encrypts_persisted_dataset() {
    run_case("vector-sdk-kms", Case::Kms);
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_kms_revocation_preserves_dataset() {
    run_case("vector-sdk-kms-revocation", Case::Permissions);
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_kms_key_states_stay_within_operation_error_models() {
    temp_env::with_vars(
        [
            ("AWS_ACCESS_KEY_ID", Some("test-kms-access-key")),
            ("AWS_SECRET_ACCESS_KEY", Some("test-kms-secret-key")),
            ("AWS_EC2_METADATA_DISABLED", Some("true")),
            ("NO_PROXY", Some("127.0.0.1,localhost")),
            ("no_proxy", Some("127.0.0.1,localhost")),
            ("HTTP_PROXY", None),
            ("HTTPS_PROXY", None),
            ("http_proxy", None),
            ("https_proxy", None),
            ("ALL_PROXY", None),
            ("all_proxy", None),
        ],
        || run_case("vector-sdk-kms-key-states", Case::KeyStates),
    );
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_encryption_mismatch_preserves_ciphertext() {
    run_case("vector-sdk-encryption-mismatch", Case::Mismatch);
}

#[derive(Clone, Debug, Default)]
struct WireJson(Arc<Mutex<Option<serde_json::Value>>>);

impl Intercept for WireJson {
    fn name(&self) -> &'static str {
        "VectorEncryptionWireJson"
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let bytes = context.response().body().bytes().expect("buffered SDK JSON response");
        let value = serde_json::from_slice(bytes)?;
        assert!(self.0.lock().unwrap().replace(value).is_none(), "one response without retries");
        Ok(())
    }
}

impl WireJson {
    fn take(&self) -> serde_json::Value {
        self.0.lock().unwrap().take().expect("the SDK observed an HTTP response body")
    }
}

fn assert_no_encryption_member(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            assert!(!object.contains_key("encryptionConfiguration"));
            for value in object.values() {
                assert_no_encryption_member(value);
            }
        }
        serde_json::Value::Array(array) => {
            for value in array {
                assert_no_encryption_member(value);
            }
        }
        _ => {}
    }
}

async fn assert_list_responses(server: &SdkServer, bucket: &str, index_arn: &str) {
    let wire = WireJson::default();
    let buckets = server
        .client
        .list_vector_buckets()
        .prefix(bucket)
        .customize()
        .interceptor(wire.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(buckets.vector_buckets().len(), 1);
    assert_eq!(buckets.vector_buckets()[0].vector_bucket_name(), bucket);
    assert!(buckets.next_token().is_none());
    assert_no_encryption_member(&wire.take());
    let wire = WireJson::default();
    let indexes = server
        .client
        .list_indexes()
        .vector_bucket_name(bucket)
        .customize()
        .interceptor(wire.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(indexes.indexes().len(), 1);
    assert_eq!(indexes.indexes()[0].index_arn(), index_arn);
    assert!(indexes.next_token().is_none());
    assert_no_encryption_member(&wire.take());
}

async fn assert_configuration_stability(fixture: &EncryptionFixture, server: &SdkServer, index_arn: &str) {
    let original_index = server.client.get_index().index_arn(index_arn).send().await.unwrap();
    let index = original_index.index().unwrap();
    let bucket = index.vector_bucket_name();
    let original_bucket = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(bucket)
        .send()
        .await
        .unwrap();
    let before = snapshot(fixture, index_arn, true).await;
    assert_list_responses(server, bucket, index_arn).await;

    for configuration in [index.encryption_configuration().cloned(), None] {
        let error = server
            .client
            .create_index()
            .vector_bucket_name(bucket)
            .index_name(index.index_name())
            .data_type(DataType::Float32)
            .dimension(2)
            .distance_metric(DistanceMetric::Euclidean)
            .set_encryption_configuration(configuration)
            .send()
            .await
            .expect_err("a duplicate Index must preserve its original definition");
        assert_eq!(error.as_service_error().unwrap().code(), Some("ConflictException"));
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 409);
        assert_eq!(snapshot(fixture, index_arn, true).await, before);
    }
    for configuration in [
        original_bucket.vector_bucket().unwrap().encryption_configuration().cloned(),
        None,
    ] {
        let error = server
            .client
            .create_vector_bucket()
            .vector_bucket_name(bucket)
            .set_encryption_configuration(configuration)
            .send()
            .await
            .expect_err("a duplicate Vector bucket must preserve its original identity");
        assert_eq!(error.as_service_error().unwrap().code(), Some("ConflictException"));
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 409);
        assert_eq!(snapshot(fixture, index_arn, true).await, before);
    }
    server
        .client
        .put_vector_bucket_default_index_mode()
        .vector_bucket_name(bucket)
        .default_index_mode(IndexMode::Enhanced)
        .send()
        .await
        .unwrap();
    server
        .client
        .update_index_mode()
        .index_arn(index_arn)
        .index_mode(IndexMode::Enhanced)
        .send()
        .await
        .unwrap();
    sdk_error!(
        server
            .client
            .put_vector_bucket_default_index_mode()
            .vector_bucket_name(bucket)
            .default_index_mode(IndexMode::Classic)
            .send()
            .await,
        "ValidationException",
        400
    );
    sdk_error!(
        server
            .client
            .update_index_mode()
            .index_arn(index_arn)
            .index_mode(IndexMode::Classic)
            .send()
            .await,
        "ValidationException",
        400
    );
    let after_bucket = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(bucket)
        .send()
        .await
        .unwrap();
    let after_index = server.client.get_index().index_arn(index_arn).send().await.unwrap();
    assert_eq!(after_bucket.vector_bucket(), original_bucket.vector_bucket());
    assert_eq!(after_index.index(), original_index.index());
    assert_eq!(snapshot(fixture, index_arn, true).await, before);
    assert_read(server, index_arn).await;
}

async fn empty_bucket_case(fixture: &EncryptionFixture, server: &SdkServer) {
    let bucket = format!("sdk-empty-encryption-{}", Uuid::new_v4().simple());
    let changed = Arc::new(AtomicBool::new(false));
    let observed = changed.clone();
    let wire = WireJson::default();
    let created = server
        .client
        .create_vector_bucket()
        .vector_bucket_name(&bucket)
        .encryption_configuration(aes())
        .customize()
        .mutate_request(move |request| {
            let mut body: serde_json::Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            assert_eq!(body["encryptionConfiguration"], json!({"sseType":"AES256"}));
            body["encryptionConfiguration"] = json!({});
            let bytes = serde_json::to_vec(&body).unwrap();
            request.headers_mut().insert("content-length", bytes.len().to_string());
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(bytes);
            let outgoing: serde_json::Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
            assert_eq!(outgoing["encryptionConfiguration"], json!({}));
            observed.store(true, Ordering::SeqCst);
        })
        .interceptor(wire.clone())
        .send()
        .await
        .unwrap();
    assert!(changed.load(Ordering::SeqCst));
    assert_eq!(wire.take(), json!({"vectorBucketArn":created.vector_bucket_arn().unwrap()}));
    let found = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(found.vector_bucket().unwrap().encryption_configuration(), Some(&aes()));
    let wire = WireJson::default();
    let created = server
        .client
        .create_index()
        .vector_bucket_name(&bucket)
        .index_name("encrypted.index")
        .data_type(DataType::Float32)
        .dimension(2)
        .distance_metric(DistanceMetric::Euclidean)
        .customize()
        .interceptor(wire.clone())
        .send()
        .await
        .unwrap();
    let index_arn = created.index_arn().unwrap();
    assert_eq!(wire.take(), json!({"indexArn":index_arn}));
    let found = server.client.get_index().index_arn(index_arn).send().await.unwrap();
    assert_eq!(found.index().unwrap().encryption_configuration(), Some(&aes()));
    put_and_read(server, index_arn).await;
    assert_snapshot_encryption(&snapshot(fixture, index_arn, true).await, &aes());
    assert_configuration_stability(fixture, server, index_arn).await;
    server.client.delete_index().index_arn(index_arn).send().await.unwrap();
    sdk_error!(
        server.client.delete_vector_bucket().vector_bucket_name(&bucket).send().await,
        "ConflictException",
        409
    );
    let retained = server
        .client
        .get_vector_bucket()
        .vector_bucket_name(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(retained.vector_bucket().unwrap().encryption_configuration(), Some(&aes()));
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_empty_bucket_encryption_configuration_defaults_to_aes256() {
    run_case("vector-sdk-empty-bucket-encryption", Case::EmptyBucket);
}

async fn plaintext_snapshot(fixture: &Fixture, index_arn: &str) -> Snapshot {
    let (managed, manifest, generation) = managed_access(fixture, index_arn, None).await;
    let store = fixture.app.object_store();
    let bucket = managed.access.bucket().storage_bucket_id().as_str();
    let page = store
        .clone()
        .list_objects_v2(bucket, managed.access.allowed_prefix(), None, None, 1000, false, None, false)
        .await
        .unwrap();
    assert!(!page.is_truncated);
    let mut objects = BTreeMap::new();
    for object in page.objects {
        let options = StorageObjectOptions {
            raw_data_movement_read: true,
            suppress_read_repair: true,
            expected_bucket_incarnation_id: Some(managed.access.bucket().incarnation().as_uuid()),
            ..Default::default()
        };
        let reader = store
            .get_object_reader(bucket, &object.name, None, http::HeaderMap::new(), &options)
            .await
            .unwrap();
        let info = reader.object_info;
        assert!(!info.is_encrypted());
        assert!(
            info.user_defined
                .keys()
                .all(|key| !rustfs_utils::http::is_object_encryption_marker(key))
        );
        let mut bytes = Vec::new();
        reader
            .stream
            .take(MAX_OBJECT_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert!(u64::try_from(bytes.len()).unwrap() <= MAX_OBJECT_BYTES);
        assert_eq!(i64::try_from(bytes.len()).unwrap(), info.size);
        let key = CatalogObjectKey::try_new(object.name.clone()).unwrap();
        let read = managed.objects.get(&managed.access, &key, None, info.data_dir).await.unwrap();
        assert_eq!(bytes, read.data);
        if key == *manifest.object_key() {
            assert_eq!(u64::try_from(bytes.len()).unwrap(), manifest.content_length());
            assert_eq!(
                hex_simd::encode_to_string(Sha256::digest(&bytes), hex_simd::AsciiCase::Lower),
                manifest.content_sha256().to_hex()
            );
        }
        objects.insert(
            object.name,
            StoredObject {
                bytes,
                metadata: info
                    .user_defined
                    .iter()
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
                    .collect(),
                revision: info.data_dir,
                etag: info.etag,
            },
        );
    }
    assert!(objects.keys().any(|key| key.ends_with(".lance")));
    assert!(objects.contains_key(manifest.object_key().as_str()));
    Snapshot {
        bucket_incarnation: managed.access.bucket().incarnation().as_uuid(),
        table_id: managed.access.table_id().as_str().to_owned(),
        prefix: managed.access.allowed_prefix().to_owned(),
        generation,
        manifest: manifest.object_key().as_str().to_owned(),
        objects,
    }
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_unencrypted_index_ignores_cached_s3_default_encryption() {
    run_large_stack_test("vector-sdk-plain-cached-sse", || async {
        use crate::app::storage_api::{
            managed::get_bucket_metadata,
            test::{get_global_bucket_metadata_sys, set_bucket_metadata},
        };
        use rustfs_gateway_types::persistence::{
            PersistedBucketEncryptionConfiguration, PersistedBucketEncryptionRule, PersistedEncryptionByDefault,
        };

        let fixture = Fixture::new().await;
        assert!(fixture.app.kms().handle().get_encryption_service().await.is_none());
        reset_sse_dek_provider();
        let _reset = ProviderReset;
        let server = SdkServer::new(&fixture).await;
        let mut restore = None;
        let result = AssertUnwindSafe(async {
            let bucket = format!("sdk-plain-default-{}", Uuid::new_v4().simple());
            let wire = WireJson::default();
            let created = server
                .client
                .create_vector_bucket()
                .vector_bucket_name(&bucket)
                .customize()
                .interceptor(wire.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(wire.take(), json!({"vectorBucketArn":created.vector_bucket_arn().unwrap()}));
            let request = Request::GetVectorBucket(serde_json::from_value(json!({"vectorBucketName":bucket})).unwrap());
            let admitted = fixture.service.admit(fixture.verified(request)).await.unwrap();
            let handle = admitted.bucket.as_ref().unwrap();
            let storage_id = handle.storage_bucket_id().as_str();
            let original = get_bucket_metadata(&fixture.app.object_store(), storage_id).await.unwrap();
            assert!(original.encryption_config_xml.is_empty());
            assert!(original.sse_config.is_none());
            let mut cached = original.as_ref().clone();
            cached.encryption_config_xml =
                crate::app::vector_usecase::encryption::configuration_xml(&VectorEffectiveEncryptionV1::Aes256).unwrap();
            cached.sse_config = Some(PersistedBucketEncryptionConfiguration {
                rules: vec![PersistedBucketEncryptionRule {
                    apply_server_side_encryption_by_default: Some(PersistedEncryptionByDefault {
                        sse_algorithm: "AES256".into(),
                        kms_master_key_id: None,
                    }),
                    ..Default::default()
                }],
            });
            restore = Some((storage_id.to_owned(), original.as_ref().clone()));
            set_bucket_metadata(storage_id.to_owned(), cached).await.unwrap();
            let ambient = get_global_bucket_metadata_sys().unwrap();
            let (default_sse, _) = ambient.read().await.get_sse_config(storage_id).await.unwrap();
            assert_eq!(
                default_sse.rules[0]
                    .apply_server_side_encryption_by_default
                    .as_ref()
                    .unwrap()
                    .sse_algorithm
                    .as_str(),
                "AES256"
            );
            let wire = WireJson::default();
            let found = server
                .client
                .get_vector_bucket()
                .vector_bucket_name(&bucket)
                .customize()
                .interceptor(wire.clone())
                .send()
                .await
                .unwrap();
            assert!(found.vector_bucket().unwrap().encryption_configuration().is_none());
            assert_no_encryption_member(&wire.take());
            let wire = WireJson::default();
            let created = server
                .client
                .create_index()
                .vector_bucket_name(&bucket)
                .index_name("plain.index")
                .data_type(DataType::Float32)
                .dimension(2)
                .distance_metric(DistanceMetric::Euclidean)
                .customize()
                .interceptor(wire.clone())
                .send()
                .await
                .unwrap();
            let index_arn = created.index_arn().unwrap();
            assert_eq!(wire.take(), json!({"indexArn":index_arn}));
            let wire = WireJson::default();
            let found = server
                .client
                .get_index()
                .index_arn(index_arn)
                .customize()
                .interceptor(wire.clone())
                .send()
                .await
                .unwrap();
            assert!(found.index().unwrap().encryption_configuration().is_none());
            assert_no_encryption_member(&wire.take());
            assert_list_responses(&server, &bucket, index_arn).await;
            put_and_read(&server, index_arn).await;
            let before = plaintext_snapshot(&fixture, index_arn).await;
            assert_read(&server, index_arn).await;
            assert_eq!(plaintext_snapshot(&fixture, index_arn).await, before);
            let persisted = get_bucket_metadata(&fixture.app.object_store(), storage_id).await.unwrap();
            assert_eq!(persisted.encryption_config_xml, original.encryption_config_xml);
            assert_eq!(persisted.sse_config, original.sse_config);
            assert!(fixture.app.kms().handle().get_encryption_service().await.is_none());
            let (storage_id, original) = restore.take().unwrap();
            set_bucket_metadata(storage_id, original).await.unwrap();
            server.client.delete_index().index_arn(index_arn).send().await.unwrap();
            sdk_error!(
                server.client.delete_vector_bucket().vector_bucket_name(&bucket).send().await,
                "ConflictException",
                409
            );
            let retained = server
                .client
                .get_vector_bucket()
                .vector_bucket_name(&bucket)
                .send()
                .await
                .unwrap();
            assert!(retained.vector_bucket().unwrap().encryption_configuration().is_none());
        })
        .catch_unwind()
        .await;
        if let Some((storage_id, original)) = restore {
            set_bucket_metadata(storage_id, original).await.unwrap();
        }
        server.finish().await;
        cleanup_iam(&fixture).await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    });
}

#[test]
#[serial_test::serial]
fn vector_aws_sdk_aes256_uses_production_kms_startup_context() {
    const STARTUP_KEY: &str = "startup-aes-key";
    let secret = format!("{STARTUP_KEY}:{}", key_material());
    temp_env::with_vars(
        [
            ("RUSTFS_KMS_STATIC_SECRET_KEY", Some(secret.as_str())),
            ("RUSTFS_KMS_STATIC_SECRET_KEY_FILE", None),
            ("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", Some("true")),
        ],
        || {
            run_large_stack_test("vector-sdk-production-kms-startup", || async {
                assert!(
                    crate::runtime_sources::current_app_context().is_none(),
                    "requires an isolated nextest process"
                );
                let store = crate::app::gating_test_env::shared_gating_ecstore().await;
                assert!(crate::runtime_sources::current_app_context().is_none());
                let mut startup = crate::config::Config::new("127.0.0.1:0", Vec::new());
                startup.kms_enable = true;
                startup.kms_backend = "static".to_owned();
                startup.kms_default_key_id = Some(STARTUP_KEY.to_owned());
                crate::init::init_kms_system(&startup, store.clone()).await.unwrap();
                let manager = AppContext::ensure_startup_kms_interface();
                assert_eq!(manager.get_status().await, rustfs_kms::KmsServiceStatus::Running);
                let config = manager.get_config().await.unwrap();
                assert_eq!(config.backend, rustfs_kms::config::KmsBackend::Static);
                assert_eq!(config.default_key_id.as_deref(), Some(STARTUP_KEY));
                let iam = rustfs_iam::init_iam_sys(store.clone()).await.unwrap();
                let slot = ServerContextSlot::new();
                AppContext::ensure_startup_after_iam(store.clone(), manager.clone(), &slot, iam.clone()).unwrap();
                let installed = slot.installed_app_context().unwrap();
                assert!(Arc::ptr_eq(&installed.object_store(), &store));
                assert!(Arc::ptr_eq(&installed.iam().handle(), &iam));
                assert!(Arc::ptr_eq(&installed.kms().handle(), &manager));
                assert!(Arc::ptr_eq(&crate::runtime_sources::current_app_context().unwrap(), &installed));
                let encryption_service = manager.get_encryption_service().await.unwrap();
                assert_eq!(encryption_service.get_default_key_id().map(String::as_str), Some(STARTUP_KEY));
                assert!(Arc::ptr_eq(
                    &crate::runtime_sources::current_encryption_service().await.unwrap(),
                    &encryption_service
                ));

                // Fixture creates the ordinary IAM user using the context already installed by startup.
                let mut request = Fixture::new().await;
                assert!(Arc::ptr_eq(&request.app, &installed));
                request.service =
                    DefaultVectorUsecase::new(slot, request.service.limits, request.service.max_write_attempts).unwrap();
                assert!(Arc::ptr_eq(&request.service.server.installed_app_context().unwrap(), &installed));
                let fixture = EncryptionFixture {
                    request,
                    manager,
                    config,
                    key_arn: String::new(),
                    _reset: None,
                };
                let server = SdkServer::new(&fixture.request).await;
                let result = AssertUnwindSafe(async {
                    success_case(&fixture, &server, Some(aes()), None, aes()).await;
                    success_case(&fixture, &server, None, Some(aes()), aes()).await;
                    assert!(Arc::ptr_eq(
                        &crate::runtime_sources::current_encryption_service().await.unwrap(),
                        &encryption_service
                    ));
                })
                .catch_unwind()
                .await;
                server.finish().await;
                cleanup_iam(&fixture.request).await;
                fixture.manager.stop().await.unwrap();
                drop(fixture);
                if let Err(panic) = result {
                    std::panic::resume_unwind(panic);
                }
            });
        },
    );
}
