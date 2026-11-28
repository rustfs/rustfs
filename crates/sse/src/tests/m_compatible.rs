// Copyright 2024 RustFS Team
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

//! Tests of what only an `m-compatible` build compiles: the sealed object key and the stored shapes read through it.

use super::*;

use crate::core::keying::object::{
    DARE_CIPHER_AES_256_GCM, DARE_CIPHER_CHACHA20_POLY1305, MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM, SEALED_KEY_IV_SIZE,
    SEALED_KEY_SIZE, is_legacy_rustfs_managed_metadata, is_supported_sealed_object_key_cipher,
};

#[tokio::test]
async fn test_sse_prepare_encryption_ssec_with_customer_key_stores_sealed_key_metadata() {
    let bucket = "test-bucket";
    let key = "test-key";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let sse_key_md5 = md5_base64(customer_key_bytes);

    let request = PrepareEncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(customer_key),
        sse_customer_key_md5: Some(sse_key_md5),
        principal: None,
    };

    let material = sse_prepare_encryption(&HOST, request)
        .await
        .expect("prepare should accept full ssec headers")
        .expect("ssec metadata should be generated");
    assert_eq!(material.key_kind, EncryptionKeyKind::Object);

    let metadata = encryption_material_to_metadata(&material).expect("ssec metadata should serialize");
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));
}

#[tokio::test]
async fn test_sse_kms_roundtrip_persists_and_uses_minio_context() {
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
    let _guard = lock_sse_test_state().await;

    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;
    manager
        .get_encryption_service()
        .await
        .expect("encryption service should exist")
        .create_key(CreateKeyRequest {
            key_name: Some("kms-test".to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        })
        .await
        .expect("kms test key should be created");

    let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
        .await
        .expect("kms provider should initialize from the configured test manager");
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));

    let client_context = HashMap::from([("tenant".to_string(), "alpha".to_string())]);
    let request = EncryptionRequest {
        bucket: "bucket",
        key: "dir/object",
        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
        ssekms_key_id: Some("kms-test".to_string()),
        ssekms_context: Some(client_context.clone()),
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 4096,
        principal: None,
    };

    let material = sse_encryption(&HOST, request)
        .await
        .expect("sse-kms encryption")
        .expect("managed sse-kms material");
    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
    let encoded_context = metadata
        .get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
        .expect("minio kms context header should exist");
    let decoded_context: HashMap<String, String> =
        serde_json::from_slice(&BASE64_STANDARD.decode_to_vec(encoded_context).expect("decode base64 context"))
            .expect("decode json context");
    assert_eq!(decoded_context, client_context);

    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("sse-kms decryption should succeed")
    .expect("sse-kms material should exist");
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    let mut wrong_metadata = metadata.clone();
    wrong_metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
        BASE64_STANDARD
            .encode_to_string(serde_json::to_vec(&HashMap::from([("tenant".to_string(), "beta".to_string())])).unwrap()),
    );
    let err = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &wrong_metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect_err("mismatched kms context should fail");
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    assert_eq!(
        err.message,
        "Encryption context mismatch: Context mismatch for key 'tenant': expected 'alpha', got 'beta'"
    );
    assert_eq!(crate::core::audit::kms_data_plane_error_class(&err), "context_mismatch");

    manager.stop().await.expect("kms service should stop cleanly");
    reset_sse_dek_provider();
}

#[test]
fn test_encryption_material_to_metadata_persists_minio_managed_headers() {
    let encoded_nonce = BASE64_STANDARD.encode_to_string([9u8; 12]);
    let encoded_key = BASE64_STANDARD.encode_to_string([1u8, 2, 3, 4]);
    let metadata = encryption_material_to_metadata(&EncryptionMaterial {
        sse_type: SSEType::SseKms,
        server_side_encryption: String::from(AMZ_ENCRYPTION_KMS),
        kms_key_id: Some("test-key".to_string()),
        algorithm: AMZ_ENCRYPTION_KMS.to_string(),
        key_bytes: [7u8; 32],
        base_nonce: [9u8; 12],
        encrypted_data_key: Some(vec![1, 2, 3, 4]),
        customer_key_md5: None,
        original_size: Some(1024),
        key_kind: EncryptionKeyKind::Direct,
        managed_kms_context: None,
        managed_sealed_key: None,
    })
    .expect("managed SSE metadata should serialize");

    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str),
        Some(encoded_nonce.as_str())
    );
    assert_eq!(
        metadata
            .get(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
            .map(String::as_str),
        Some(encoded_key.as_str())
    );
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER).map(String::as_str),
        Some("test-key")
    );
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
        Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
    );
}

#[test]
fn test_legacy_managed_metadata_excludes_sealed_keys() {
    let legacy_metadata = HashMap::from([
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
    ]);
    assert!(is_legacy_rustfs_managed_metadata(&legacy_metadata));

    let sealed_metadata = HashMap::from([
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
        (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), "sealed-key".to_string()),
    ]);
    assert!(!is_legacy_rustfs_managed_metadata(&sealed_metadata));
}

#[test]
fn test_normalize_managed_metadata_accepts_minio_only_headers() {
    let metadata = HashMap::from([
        (
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(b"encrypted-key"),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string([0x11u8; 12]),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
            MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
        ),
        (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
    ]);

    let normalized = normalize_managed_metadata(&metadata, Some(recode_minio_kms_context));

    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_KEY_HEADER),
        Some(&BASE64_STANDARD.encode_to_string(b"encrypted-key"))
    );
    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_IV_HEADER),
        Some(&BASE64_STANDARD.encode_to_string([0x11u8; 12]))
    );
    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER),
        Some(&MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string())
    );
    assert_eq!(normalized.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER), Some(&"default".to_string()));
}

#[test]
fn test_supported_sealed_object_key_cipher_accepts_current_minio_fixture_value() {
    assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_AES_256_GCM));
    assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_CHACHA20_POLY1305));
    assert!(!is_supported_sealed_object_key_cipher(0x02));
}

/// Rewrites rio-v2 SSE-C metadata into the shape MinIO actually persists:
/// only the internal sealed-key slot, IV, and seal algorithm — the public
/// scheme, customer-algorithm, and customer-key-MD5 keys are synthesized
/// onto responses by MinIO and never hit disk (backlog#1638).
fn strip_to_minio_ssec_shape(metadata: &mut HashMap<String, String>) {
    metadata.remove("x-amz-server-side-encryption");
    metadata.remove("x-amz-server-side-encryption-customer-algorithm");
    metadata.remove("x-amz-server-side-encryption-customer-key-md5");
}

#[tokio::test]
async fn test_minio_shaped_ssec_metadata_decrypts_with_the_right_key_only() {
    let customer_key_bytes = [0x42u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");

    let mut metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    strip_to_minio_ssec_shape(&mut metadata);
    assert!(
        !metadata.contains_key("x-amz-server-side-encryption-customer-key-md5"),
        "the MinIO shape stores no customer-key MD5"
    );

    // Correct key: detected via the sealed slot, proven by the unseal.
    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped sse-c decryption")
    .expect("minio-shaped sse-c material");
    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    // Wrong key with a self-consistent MD5: the missing stored MD5 cannot
    // catch it, so the AEAD unseal must — the relaxation is not a bypass.
    let wrong_key_bytes = [0x43u8; 32];
    let wrong_key = BASE64_STANDARD.encode_to_string(wrong_key_bytes);
    let wrong_key_md5 = md5_base64(wrong_key_bytes);
    let result = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&wrong_key),
            sse_customer_key_md5: Some(&wrong_key_md5),
            principal: None,
        },
    )
    .await;
    assert!(result.is_err(), "a wrong customer key must fail the sealed-key unseal");

    // No key at all: detected as SSE-C, so the caller is asked for the
    // parameters instead of getting an opaque unreadable-object error.
    let result = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await;
    let error = result.expect_err("missing SSE-C parameters must fail closed");
    assert!(error.message.contains("must be provided"), "got: {}", error.message);
}

#[test]
fn test_mark_encrypted_multipart_metadata_sets_minio_marker() {
    let mut metadata = HashMap::new();
    mark_encrypted_multipart_metadata(&mut metadata);
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER));
}

/// A KMS stand-in that owns bare data keys and records what it is asked to unwrap.
struct BareKmsProvider {
    data_key: [u8; 32],
    unwrapped: std::sync::Mutex<Vec<Vec<u8>>>,
}

#[async_trait::async_trait]
impl SseDekProvider for BareKmsProvider {
    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        Err(SseError::internal("not exercised"))
    }

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        self.unwrapped.lock().expect("unwrap log").push(encrypted_dek.to_vec());
        Ok(self.data_key)
    }

    async fn reads_bare_data_key_ciphertext(&self) -> bool {
        true
    }
}

/// Seal a data key the way MinIO's builtin KMS does, in its raw layout:
/// `sealed || iv[16] || nonce[12]`, AES-256-GCM under `HMAC-SHA256(master, iv)`.
fn seal_minio_kms_data_key(data_key: &[u8; 32], master_key: &[u8; 32], context: &ObjectEncryptionContext) -> Vec<u8> {
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
    use hmac::Mac;
    let (iv, nonce) = ([0x21u8; 16], [0x42u8; 12]);
    let mut mac = crate::core::keying::object::HmacSha256::new_from_slice(master_key).expect("hmac key");
    mac.update(&iv);
    let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
    let aad = crate::kms::minio::minio_kms_associated_data(context).expect("associated data");
    let mut sealed = Aes256Gcm::new_from_slice(&sealing_key)
        .expect("sealing key")
        .encrypt(
            &aes_gcm::Nonce::from(nonce),
            aes_gcm::aead::Payload {
                msg: data_key,
                aad: &aad,
            },
        )
        .expect("seal");
    sealed.extend_from_slice(&iv);
    sealed.extend_from_slice(&nonce);
    sealed
}

/// A sealed object key with no RustFS envelope comes from MinIO or from a
/// `m-compatible` build on a backend that stored its ciphertext bare. Opening it
/// as MinIO's is an authenticated proof, so the genuine MinIO object keeps
/// its route and only the remainder reaches the KMS.
#[tokio::test]
async fn unenveloped_sealed_data_key_goes_to_minio_first_and_the_kms_otherwise() {
    let context = ObjectEncryptionContext::new("bucket".to_string(), "dir/object".to_string());
    let master_key = [0x5Au8; 32];
    let minio_data_key = [0x11u8; 32];
    let minio_ciphertext = seal_minio_kms_data_key(&minio_data_key, &master_key, &context);
    let aws_blob = vec![
        0x01, 0x02, 0x02, 0x00, 0x78, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
    ];
    let kms = || {
        Arc::new(BareKmsProvider {
            data_key: [0x22u8; 32],
            unwrapped: std::sync::Mutex::new(Vec::new()),
        })
    };
    let local = || -> Result<Arc<dyn SseDekProvider>, SseError> { Ok(Arc::new(TestSseDekProvider::new_with_key(master_key))) };
    let no_local = || -> Result<Arc<dyn SseDekProvider>, SseError> {
        Err(SseError {
            code: S3ErrorCode::InvalidRequest,
            message: "no local master key".to_string(),
            source: None,
        })
    };

    // MinIO's own object on a deployment whose backend also reads bare keys.
    let provider = kms();
    let data_key =
        crate::kms::provider::decrypt_unenveloped_sealed_dek(Some(provider.clone()), local(), &minio_ciphertext, "key", &context)
            .await
            .expect("a MinIO-sealed data key must open under the local master key");
    assert_eq!(data_key, minio_data_key);
    assert!(
        provider.unwrapped.lock().expect("unwrap log").is_empty(),
        "a MinIO-sealed data key must never be sent to the KMS"
    );

    // A bare backend blob: with a local master key that cannot open it, and with none at all.
    for local in [local(), no_local()] {
        let provider = kms();
        let data_key =
            crate::kms::provider::decrypt_unenveloped_sealed_dek(Some(provider.clone()), local, &aws_blob, "key", &context)
                .await
                .expect("a bare backend blob must be unwrapped by the KMS");
        assert_eq!(data_key, [0x22u8; 32]);
        assert_eq!(*provider.unwrapped.lock().expect("unwrap log"), vec![aws_blob.clone()]);
    }

    // Without a backend that reads bare keys, the MinIO route is the only one.
    let data_key = crate::kms::provider::decrypt_unenveloped_sealed_dek(None, local(), &minio_ciphertext, "key", &context)
        .await
        .expect("the MinIO route is unchanged");
    assert_eq!(data_key, minio_data_key);
    crate::kms::provider::decrypt_unenveloped_sealed_dek(None, local(), &aws_blob, "key", &context)
        .await
        .expect_err("bytes MinIO did not seal have no owner without such a backend");
    let error = crate::kms::provider::decrypt_unenveloped_sealed_dek(None, no_local(), &minio_ciphertext, "key", &context)
        .await
        .expect_err("a missing local master key is still reported as such");
    assert_eq!((error.code, error.message.as_str()), (S3ErrorCode::InvalidRequest, "no local master key"));
}

#[tokio::test]
async fn test_managed_sse_rio_v2_uses_object_key_metadata_roundtrip() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    if let Some(manager) = rustfs_kms::get_global_kms_service_manager() {
        let _ = manager.stop().await;
    }
    let local_sse_master_key = local_sse_master_key_b64();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
        ],
        async {
            let request = EncryptionRequest {
                bucket: "bucket",
                key: "object",
                server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                content_size: 4096,
                principal: None,
            };

            let material = sse_encryption(&HOST, request)
                .await
                .expect("managed sse encryption")
                .expect("managed sse material");
            assert_eq!(material.key_kind, EncryptionKeyKind::Object);

            let metadata = encryption_material_to_metadata(&material).expect("managed SSE metadata should serialize");
            assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER));
            assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));

            let sealing_iv = metadata
                .get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER)
                .expect("minio sealing iv should be stored");
            let sealed_key = metadata
                .get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
                .expect("minio sealed key should be stored");
            assert_eq!(BASE64_STANDARD.decode_to_vec(sealing_iv).expect("decode iv").len(), SEALED_KEY_IV_SIZE);
            assert_eq!(
                BASE64_STANDARD.decode_to_vec(sealed_key).expect("decode sealed key").len(),
                SEALED_KEY_SIZE
            );

            let decrypted = sse_decryption(
                &HOST,
                DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                },
            )
            .await
            .expect("managed sse decryption")
            .expect("managed decryption material");

            assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
            assert_eq!(decrypted.key_bytes, material.key_bytes);
        },
    )
    .await;

    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_ssec_rio_v2_uses_sealed_object_key_metadata_roundtrip() {
    let customer_key_bytes = [0x42u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");

    assert_eq!(material.key_kind, EncryptionKeyKind::Object);

    let metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
        Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
    );
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));

    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("sse-c decryption")
    .expect("sse-c decryption material");

    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);
}

#[tokio::test]
async fn test_minio_shaped_ssec_metadata_resolves_and_classifies() {
    let customer_key_bytes = [0x51u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");
    let mut metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    strip_to_minio_ssec_shape(&mut metadata);

    let mut headers = HeaderMap::new();
    headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&customer_key).expect("customer key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&customer_key_md5).expect("customer key MD5 header"),
    );
    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped SSE-C decryption")
    .expect("minio-shaped SSE-C material");
    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    // HEAD-path key validation: no stored MD5 to compare against, so a
    // self-consistent key passes here and the unseal remains the proof.
    crate::core::s3::ssec::validate_ssec_for_read(&metadata, Some(&customer_key), Some(&customer_key_md5))
        .expect("self-consistent SSE-C parameters validate");
    crate::core::s3::ssec::validate_ssec_for_read(&metadata, None, None).expect_err("missing SSE-C parameters must be rejected");

    // Response classification reports SSE-C, not managed SSE.
    let classified = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped sse-c classification")
    .expect("minio-shaped sse-c response headers");
    assert!(classified.sse_customer_algorithm.is_some());
    assert!(classified.ssekms_key_id.is_none());
}

/// Object key `0x40..=0x5f`, the key the sealed-ETag reference vectors use.
fn sealed_etag_object_key() -> [u8; 32] {
    std::array::from_fn(|index| 0x40 + u8::try_from(index).expect("an index below 32 fits in u8"))
}

/// `MD5("rustfs-minio-etag-vector")`, and that digest sealed under
/// [`sealed_etag_object_key`] with MinIO's own `sio` library exactly as
/// `ObjectKey.SealETag` does, once per DARE cipher suite.
const SEALED_ETAG_PLAINTEXT_HEX: &str = "b228928307afa1ffeb9aa36de4526de8";

const SEALED_ETAG_AES_GCM_HEX: &str =
    "20000f00ae1b28a996b85ecef920fb8da6843e2838328dbfef3a9a266201f27ee3f8c64555ea36a2196275c2261d745d";

const SEALED_ETAG_CHACHA20_HEX: &str =
    "20010f00cbc498738108113178f20c5507f319f350e962a89c610c16699bca30b4f4914b6552c532550cca4ec1eb39b3";

#[test]
fn unseal_etag_recovers_the_md5_minio_sealed_under_either_cipher_suite() {
    use crate::core::keying::object::unseal_etag;

    for sealed in [SEALED_ETAG_AES_GCM_HEX, SEALED_ETAG_CHACHA20_HEX] {
        let sealed = hex_simd::decode_to_vec(sealed).expect("test vector is valid hex");
        let etag = unseal_etag(sealed_etag_object_key(), &sealed).expect("unseal a MinIO-sealed ETag");
        assert_eq!(hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower), SEALED_ETAG_PLAINTEXT_HEX);
    }
}

#[test]
fn unseal_etag_rejects_a_wrong_key_and_malformed_packages() {
    use crate::core::keying::object::unseal_etag;

    let sealed = hex_simd::decode_to_vec(SEALED_ETAG_AES_GCM_HEX).expect("test vector is valid hex");
    unseal_etag([0x11; 32], &sealed).expect_err("a different object key must not authenticate");

    let mut tampered = sealed.clone();
    *tampered.last_mut().expect("a sealed ETag is not empty") ^= 0x01;
    unseal_etag(sealed_etag_object_key(), &tampered).expect_err("a flipped bit must not authenticate");

    unseal_etag(sealed_etag_object_key(), &sealed[..sealed.len() - 1]).expect_err("length disagrees with the header");
    unseal_etag(sealed_etag_object_key(), &sealed[..8]).expect_err("shorter than a DARE header");

    let mut not_final = sealed.clone();
    not_final[4] &= 0x7F;
    unseal_etag(sealed_etag_object_key(), &not_final).expect_err("a sealed ETag is one final package");

    let mut unknown_cipher = sealed;
    unknown_cipher[1] = 0x7F;
    unseal_etag(sealed_etag_object_key(), &unknown_cipher).expect_err("an unknown cipher suite");
}

/// Metadata of a single-part managed-SSE object as MinIO's builtin KMS stores
/// it: internal sealed-key slots only, no public scheme header.
fn minio_managed_object_metadata(
    sse_type: SSEType,
    master_key: &[u8; 32],
    bucket: &str,
    object: &str,
    object_key: [u8; 32],
) -> HashMap<String, String> {
    let data_key = [0x33u8; 32];
    let context = ObjectEncryptionContext::new(bucket.to_string(), object.to_string());
    let wrapped_data_key = seal_minio_kms_data_key(&data_key, master_key, &context);
    let sealed = crate::core::keying::object::seal_object_key(object_key, data_key, sse_type, bucket, object)
        .expect("seal the object key under the data key");
    let sealed_key_slot = match sse_type {
        SSEType::SseS3 => MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        SSEType::SseKms => MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        SSEType::SseC => MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
    };

    HashMap::from([
        (
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
            crate::core::keying::object::MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(sealed.iv),
        ),
        (sealed_key_slot.to_string(), BASE64_STANDARD.encode_to_string(sealed.sealed_key)),
        (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "compat-key".to_string()),
        (
            MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(wrapped_data_key),
        ),
    ])
}

fn decryption_request<'a>(metadata: &'a HashMap<String, String>) -> DecryptionRequest<'a> {
    DecryptionRequest {
        bucket: "bucket",
        key: "dir/object",
        metadata,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        principal: None,
    }
}

/// The ETag half of reading a MinIO drive set: what MinIO stored is not what
/// MinIO answered with, and a client only ever saw the answer.
#[tokio::test]
async fn sealed_etag_is_presented_the_way_minio_presented_it() {
    use crate::core::read::sealed_etag_for_response;

    let _guard = lock_sse_test_state().await;
    let master_key = [0x5Au8; 32];
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(TestSseDekProvider::new_with_key(master_key)));

    // SSE-S3: the content MD5, recovered with the object key.
    let sse_s3 = minio_managed_object_metadata(SSEType::SseS3, &master_key, "bucket", "dir/object", sealed_etag_object_key());
    for sealed in [SEALED_ETAG_AES_GCM_HEX, SEALED_ETAG_CHACHA20_HEX] {
        assert_eq!(
            sealed_etag_for_response(&HOST, decryption_request(&sse_s3), sealed)
                .await
                .as_deref(),
            Some(SEALED_ETAG_PLAINTEXT_HEX)
        );
    }

    // SSE-KMS and SSE-C: the tail of the sealed value, no key involved.
    let sealed_tail = &SEALED_ETAG_CHACHA20_HEX[SEALED_ETAG_CHACHA20_HEX.len() - 32..];
    let sse_kms = minio_managed_object_metadata(SSEType::SseKms, &master_key, "bucket", "dir/object", sealed_etag_object_key());
    let sse_c = minio_managed_object_metadata(SSEType::SseC, &master_key, "bucket", "dir/object", sealed_etag_object_key());
    for metadata in [&sse_kms, &sse_c] {
        assert_eq!(
            sealed_etag_for_response(&HOST, decryption_request(metadata), SEALED_ETAG_CHACHA20_HEX)
                .await
                .as_deref(),
            Some(sealed_tail)
        );
    }

    // Stored as shown: an unsealed digest, a multipart ETag, a marked multipart
    // object, and an object without MinIO's sealed object key.
    for stored in [SEALED_ETAG_PLAINTEXT_HEX, "b228928307afa1ffeb9aa36de4526de8-3", "not-an-etag"] {
        assert_eq!(
            sealed_etag_for_response(&HOST, decryption_request(&sse_s3), stored).await,
            None,
            "{stored}"
        );
    }
    let mut multipart = sse_s3.clone();
    mark_encrypted_multipart_metadata(&mut multipart);
    assert_eq!(
        sealed_etag_for_response(&HOST, decryption_request(&multipart), SEALED_ETAG_AES_GCM_HEX).await,
        None
    );
    let plaintext_object = HashMap::from([("content-type".to_string(), "text/plain".to_string())]);
    assert_eq!(
        sealed_etag_for_response(&HOST, decryption_request(&plaintext_object), SEALED_ETAG_AES_GCM_HEX).await,
        None
    );

    // SSE-S3 whose object key cannot be recovered keeps the stored value, as
    // MinIO does: another object's key, then no usable master key at all.
    let other_object = minio_managed_object_metadata(SSEType::SseS3, &master_key, "bucket", "dir/object", [0x77u8; 32]);
    assert_eq!(
        sealed_etag_for_response(&HOST, decryption_request(&other_object), SEALED_ETAG_AES_GCM_HEX).await,
        None
    );
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(TestSseDekProvider::new_with_key([0x01u8; 32])));
    assert_eq!(
        sealed_etag_for_response(&HOST, decryption_request(&sse_s3), SEALED_ETAG_AES_GCM_HEX).await,
        None
    );

    reset_sse_dek_provider();
}

#[test]
fn unrecorded_sse_response_synthesizes_what_minio_synthesized() {
    use crate::core::read::classify_unrecorded_sse_response;

    let master_key = [0x5Au8; 32];
    let object_key = [0x67u8; 32];

    let sse_s3 = minio_managed_object_metadata(SSEType::SseS3, &master_key, "bucket", "dir/object", object_key);
    let headers = classify_unrecorded_sse_response(&sse_s3, None).expect("MinIO SSE-S3 object");
    assert_eq!(headers.server_side_encryption, "AES256");
    // MinIO records a key id on SSE-S3 objects too; only aws:kms advertises it.
    assert_eq!((headers.ssekms_key_id, headers.sse_customer_algorithm), (None, None));

    let mut sse_kms = minio_managed_object_metadata(SSEType::SseKms, &master_key, "bucket", "dir/object", object_key);
    let headers = classify_unrecorded_sse_response(&sse_kms, None).expect("MinIO SSE-KMS object");
    assert_eq!(headers.server_side_encryption, "aws:kms");
    assert_eq!(
        headers.ssekms_key_id.as_deref(),
        Some("arn:aws:kms:compat-key"),
        "MinIO answers in ARN form"
    );
    sse_kms.insert(
        MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(),
        "arn:aws:kms:compat-key".to_string(),
    );
    let headers = classify_unrecorded_sse_response(&sse_kms, None).expect("MinIO SSE-KMS object");
    assert_eq!(
        headers.ssekms_key_id.as_deref(),
        Some("arn:aws:kms:compat-key"),
        "an ARN is not prefixed twice"
    );

    let sse_c = HashMap::from([(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(), "sealed".to_string())]);
    let key_md5 = "customer-key-md5".to_string();
    let headers = classify_unrecorded_sse_response(&sse_c, Some(&key_md5)).expect("MinIO SSE-C object");
    assert_eq!(headers.sse_customer_algorithm.as_deref(), Some("AES256"));
    assert_eq!(headers.sse_customer_key_md5.as_deref(), Some("customer-key-md5"));
    assert_eq!(headers.ssekms_key_id, None);
}

/// GET of a MinIO-written `aws:kms` object names its key the way MinIO did,
/// while authorization keeps naming the key itself.
#[tokio::test]
async fn read_response_reports_a_minio_written_kms_key_in_arn_form() {
    let master_key = [0x5Au8; 32];
    let sse_kms = minio_managed_object_metadata(SSEType::SseKms, &master_key, "bucket", "dir/object", [0x67u8; 32]);

    let headers = classify_sse_read_response(&HOST, decryption_request(&sse_kms))
        .await
        .expect("classification needs no key")
        .expect("MinIO SSE-KMS object");
    assert_eq!(headers.ssekms_key_id.as_deref(), Some("arn:aws:kms:compat-key"));

    // An object that stores its public header keeps the key id it recorded.
    let recorded = sse_kms_object_metadata();
    let headers = classify_sse_read_response(&HOST, decryption_request(&recorded))
        .await
        .expect("classification needs no key")
        .expect("RustFS SSE-KMS object");
    assert_eq!(headers.ssekms_key_id.as_deref(), Some("finance-key"));
}

/// A caller that cannot await the presented ETag still has to know when the
/// stored one may not be it. The form test must say so for every stored ETag
/// that is presented differently, and only a sealed form may make it say so.
#[test]
fn every_sealed_etag_has_the_sealed_form() {
    use crate::core::read::stored_etag_may_be_sealed;

    for sealed in [SEALED_ETAG_AES_GCM_HEX, SEALED_ETAG_CHACHA20_HEX] {
        assert!(stored_etag_may_be_sealed(sealed), "{sealed}");
    }
    for shown in [
        SEALED_ETAG_PLAINTEXT_HEX,
        "b228928307afa1ffeb9aa36de4526de8-3",
        "not-an-etag",
        "",
    ] {
        assert!(!stored_etag_may_be_sealed(shown), "{shown}");
    }
}
