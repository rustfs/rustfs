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
    error::ProvideErrorMetadata as _,
    types::{DataType, DistanceMetric, EncryptionConfiguration, SseType, VectorData},
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
        Arc,
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

struct EncryptionFixture {
    request: Fixture,
    manager: Arc<KmsServiceManager>,
    config: KmsConfig,
    key_arn: String,
    _reset: ProviderReset,
}

fn key_material() -> String {
    let mut bytes = Vec::from(Uuid::new_v4().as_bytes().as_slice());
    bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    base64_simd::STANDARD.encode_to_string(bytes)
}

impl EncryptionFixture {
    async fn new(aes_default: bool) -> Self {
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
            _reset: reset,
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
    server
        .client
        .create_vector_bucket()
        .vector_bucket_name(&bucket)
        .set_encryption_configuration(bucket_encryption.clone())
        .send()
        .await
        .unwrap();
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
        .send()
        .await
        .unwrap();
    let index_arn = output.index_arn().unwrap().to_string();
    let found = server.client.get_index().index_arn(&index_arn).send().await.unwrap();
    assert_eq!(found.index().unwrap().encryption_configuration(), Some(&expected));
    assert!(index_arn.starts_with(&format!("arn:aws:s3vectors:{}::bucket/", fixture.request.service.region)));
    index_arn
}

async fn managed_access(
    fixture: &EncryptionFixture,
    index_arn: &str,
    override_encryption: Option<VectorEffectiveEncryptionV1>,
) -> (ManagedDatasetAccess, CatalogMetadataRef, u64) {
    let request = Request::GetVectors(
        serde_json::from_value(json!({
            "indexArn":index_arn,"keys":["alpha"],"returnData":true,"returnMetadata":true
        }))
        .unwrap(),
    );
    let admitted = fixture
        .request
        .service
        .admit(fixture.request.verified(request))
        .await
        .unwrap();
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
        owner: &fixture.request.service,
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
struct Snapshot {
    bucket_incarnation: Uuid,
    table_id: String,
    prefix: String,
    generation: u64,
    manifest: String,
    objects: BTreeMap<String, StoredObject>,
}

async fn snapshot(fixture: &EncryptionFixture, index_arn: &str, decrypt: bool) -> Snapshot {
    let (managed, manifest, generation) = managed_access(fixture, index_arn, None).await;
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
    let (managed, manifest, _) = managed_access(fixture, &arn, Some(VectorEffectiveEncryptionV1::Aes256)).await;
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
    let (managed, manifest, _) = managed_access(fixture, &arn, None).await;
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
    Aes,
    Kms,
    Permissions,
    Mismatch,
}

fn run_case(name: &'static str, case: Case) {
    run_large_stack_test(name, move || async move {
        let fixture = EncryptionFixture::new(matches!(case, Case::Aes)).await;
        let server = SdkServer::new(&fixture.request).await;
        let result = AssertUnwindSafe(async {
            match case {
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
fn vector_aws_sdk_encryption_mismatch_preserves_ciphertext() {
    run_case("vector-sdk-encryption-mismatch", Case::Mismatch);
}
