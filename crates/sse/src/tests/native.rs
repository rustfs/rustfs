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

//! Tests of what only a default build compiles: the key modes it writes.

use super::*;

// ------------------------------------------------------------------------
// SSE-C Direct (default, non-`m-compatible` build) random-nonce persistence
// ------------------------------------------------------------------------

async fn ssec_direct_put_metadata(
    bucket: &str,
    key: &str,
    customer_key: &str,
    customer_key_md5: &str,
) -> HashMap<String, String> {
    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.to_string()),
            sse_customer_key_md5: Some(customer_key_md5.to_string()),
            content_size: 128,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");
    assert_eq!(material.key_kind, EncryptionKeyKind::Direct);
    encryption_material_to_metadata(&material).expect("sse-c metadata should serialize")
}

// (a) Overwriting the same bucket/key under the same SSE-C key must persist a DIFFERENT
// random IV each time, and each stored IV must be read back on decrypt. This is the core
// of the fix: no (key, nonce) reuse across overwrites.
#[tokio::test]
async fn test_ssec_direct_random_nonce_is_unique_per_put_and_read_back() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let metadata_one = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;
    let metadata_two = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

    let iv_one = metadata_one
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("first put persists a random IV");
    let iv_two = metadata_two
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("second put persists a random IV");

    assert_ne!(iv_one, iv_two, "overwriting the same object must not reuse the nonce");

    // Dual-key persistence for MinIO interop: both headers carry the same value.
    assert_eq!(metadata_one.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_one));
    assert_eq!(metadata_two.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_two));

    for (metadata, iv) in [(&metadata_one, iv_one), (&metadata_two, iv_two)] {
        let decrypted =
            crate::core::read::apply_ssec_decryption_material(bucket, key, metadata, &customer_key, &customer_key_md5)
                .await
                .expect("sse-c decryption material");
        assert_eq!(
            BASE64_STANDARD.encode_to_string(decrypted.base_nonce),
            *iv,
            "decrypt must read the persisted random nonce back"
        );
        assert_eq!(decrypted.key_bytes, customer_key_bytes);
    }
}

// (b) Legacy compatibility: objects encrypted before this change carry no IV header. Decrypt
// must fall back to the deterministic `generate_ssec_nonce(bucket, key)` they were encrypted
// with, so previously stored objects keep decrypting.
#[tokio::test]
async fn test_ssec_direct_legacy_object_without_iv_falls_back_to_deterministic_nonce() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(!metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));

    let decrypted = crate::core::read::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
        .await
        .expect("sse-c decryption material");

    assert_eq!(
        decrypted.base_nonce,
        generate_ssec_nonce(bucket, key),
        "legacy objects must decrypt via the deterministic nonce fallback"
    );
}

// (c) Byte-level round trip: data encrypted with the material's key + persisted nonce must
// decrypt with the key + nonce resolved from the stored metadata.
#[tokio::test]
async fn test_ssec_direct_persisted_nonce_round_trips_plaintext() {
    use aes_gcm::{
        Aes256Gcm, Nonce,
        aead::{Aead, KeyInit},
    };

    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x51u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);
    let plaintext = b"attack at dawn - sse-c round trip".to_vec();

    let metadata = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

    // Encrypt with the key + nonce that were persisted at PUT time.
    let enc_iv = BASE64_STANDARD
        .decode_to_vec(metadata.get(INTERNAL_ENCRYPTION_IV_HEADER).expect("persisted IV"))
        .expect("valid base64 IV");
    let cipher = Aes256Gcm::new_from_slice(&customer_key_bytes).expect("cipher");
    let ciphertext = cipher
        .encrypt(&Nonce::try_from(enc_iv.as_slice()).expect("nonce"), plaintext.as_ref())
        .expect("encrypt");

    // Resolve key + nonce purely from stored metadata (as decrypt does in production).
    let decrypted_material =
        crate::core::read::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
            .await
            .expect("sse-c decryption material");
    let dec_cipher = Aes256Gcm::new_from_slice(&decrypted_material.key_bytes).expect("cipher");
    let recovered = dec_cipher
        .decrypt(
            &Nonce::try_from(decrypted_material.base_nonce.as_slice()).expect("nonce"),
            ciphertext.as_ref(),
        )
        .expect("decrypt with resolved key + nonce");

    assert_eq!(recovered, plaintext);
}

// (d) Multipart: the random nonce generated at CreateMultipartUpload is persisted in the
// session metadata, and every part resolves the SAME nonce via `sse_decryption` on that
// session metadata. Parts must never diverge, and the value must not be deterministic.
#[tokio::test]
async fn test_ssec_direct_multipart_all_parts_share_one_persisted_nonce() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x33u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_prepare_encryption(
        &HOST,
        PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            principal: None,
        },
    )
    .await
    .expect("prepare ssec")
    .expect("prepare material");
    assert_eq!(material.key_kind, EncryptionKeyKind::Direct);

    let session_metadata = encryption_material_to_metadata(&material).expect("session metadata should serialize");
    let session_iv = session_metadata
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("multipart session persists a random IV")
        .clone();
    // Random, not the deterministic bucket/key derivation.
    assert_ne!(
        BASE64_STANDARD.decode_to_vec(&session_iv).expect("valid IV")[..],
        generate_ssec_nonce(bucket, key)[..]
    );

    let resolve_part_nonce = |part_number: usize| {
        let session_metadata = session_metadata.clone();
        let customer_key = customer_key.clone();
        let customer_key_md5 = customer_key_md5.clone();
        async move {
            let _ = part_number;
            sse_decryption(
                &HOST,
                DecryptionRequest {
                    bucket,
                    key,
                    metadata: &session_metadata,
                    sse_customer_key: Some(&customer_key),
                    sse_customer_key_md5: Some(&customer_key_md5),
                    principal: None,
                },
            )
            .await
            .expect("part decryption")
            .expect("part material")
            .base_nonce
        }
    };

    let part_one_nonce = resolve_part_nonce(1).await;
    let part_two_nonce = resolve_part_nonce(2).await;

    assert_eq!(part_one_nonce, part_two_nonce, "all parts of one upload must share the persisted nonce");
    assert_eq!(BASE64_STANDARD.encode_to_string(part_one_nonce), session_iv);
}

/// A build without the MinIO read path never rewrites an ETag.
#[tokio::test]
async fn sealed_etag_is_left_alone_without_the_minio_read_path() {
    let metadata = HashMap::from([(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), "sealed".to_string())]);
    let sealed = "20010f00cbc498738108113178f20c5507f319f350e962a89c610c16699bca30b4f4914b6552c532550cca4ec1eb39b3";

    let presented = crate::core::read::sealed_etag_for_response(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
        sealed,
    )
    .await;

    assert_eq!(presented, None);
}

/// Without the MinIO read path the scheme of such an object cannot be
/// established, so no header claims one.
#[test]
fn unrecorded_sse_response_reports_nothing_without_the_minio_read_path() {
    use crate::core::read::classify_unrecorded_sse_response;

    for slot in [
        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
    ] {
        let metadata = HashMap::from([(slot.to_string(), "sealed".to_string())]);
        assert!(classify_unrecorded_sse_response(&metadata, None).is_none(), "{slot}");
    }
}

/// A default build answers with every stored ETag as it is stored.
#[test]
fn no_stored_etag_has_a_sealed_form_in_a_default_build() {
    assert!(!crate::core::read::stored_etag_may_be_sealed(
        "20000f00ae1b28a996b85ecef920fb8da6843e2838328dbfef3a9a266201f27ee3f8c64555ea36a2196275c2261d745d"
    ));
}
