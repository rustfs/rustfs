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

//! Black-box behavior: object encryption (SSE-S3, SSE-KMS, SSE-C).
//!
//! This is the surface `rustfs/src/storage/ecfs.rs` calls on every PUT and GET,
//! so the contract is unusually load-bearing: a break here is unreadable
//! objects, not a failed request. The suite covers
//!
//! * round-tripping across sizes and both AEAD algorithms, including the
//!   degenerate empty object and block-boundary sizes;
//! * the header projection — `metadata_to_headers` is what actually reaches
//!   disk, and `headers_to_metadata` is what a GET has to rebuild from it, so
//!   the pair must compose into a working decrypt;
//! * tamper and context-mismatch rejection;
//! * SSE-C, where the key never touches the KMS at all.

mod common;

use std::collections::HashMap;

use common::{
    TestKms, assert_context_mismatch, assert_invalid_key_size, assert_invalid_operation, ctx, discard, flip_middle_bit, payload,
};
use rustfs_kms::{EncryptionAlgorithm, EncryptionMetadata, KmsError, ObjectEncryptionService};
use tokio::io::AsyncReadExt as _;

const BUCKET: &str = "objects-behavior";

/// Sizes chosen to straddle the AEAD block boundary and the empty case.
const SIZES: &[usize] = &[0, 1, 15, 16, 17, 4096, 65_537];

async fn read_all(mut reader: Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>) -> Vec<u8> {
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.expect("reading plaintext should succeed");
    out
}

async fn service_with_key(key_id: &str) -> (TestKms, std::sync::Arc<ObjectEncryptionService>) {
    let kms = TestKms::local_with(|config| config.default_key_id = Some(key_id.to_string())).await;
    kms.create_key(key_id).await;
    let service = kms.service().await;
    (kms, service)
}

#[tokio::test]
async fn objects_round_trip_across_sizes_and_algorithms() {
    let (_kms, service) = service_with_key("sse-round-trip").await;

    for algorithm in [EncryptionAlgorithm::Aes256, EncryptionAlgorithm::ChaCha20Poly1305] {
        for &size in SIZES {
            let object_key = format!("{}/{size}.bin", algorithm.as_str());
            let data = payload(size);

            let encrypted = service
                .encrypt_object(BUCKET, &object_key, data.as_slice(), &algorithm, None, None)
                .await
                .unwrap_or_else(|error| panic!("encrypting {size} bytes with {algorithm:?} failed: {error:?}"));

            assert_eq!(
                encrypted.metadata.original_size, size as u64,
                "metadata must record the plaintext size for {algorithm:?}/{size}"
            );
            assert_eq!(
                encrypted.metadata.algorithm,
                algorithm.as_str(),
                "metadata must record the algorithm actually used"
            );
            assert_eq!(
                encrypted.metadata.iv.len(),
                algorithm.iv_size(),
                "the IV must be the algorithm's nonce size"
            );
            assert!(encrypted.metadata.tag.is_some(), "an AEAD algorithm must produce a tag");
            assert!(
                !encrypted.metadata.encrypted_data_key.is_empty(),
                "the wrapped DEK must be stored with the object"
            );
            // Only assert this where a collision is not a realistic outcome. A
            // 1-byte object matches its own ciphertext once every 256 runs, so
            // asserting it there would make the suite flaky rather than strict.
            // Small objects are still covered: the tag is checked above and the
            // decrypt round-trip below is what actually proves the encryption.
            if size >= 8 {
                assert_ne!(encrypted.ciphertext, data, "ciphertext must differ from plaintext");
            }

            // The context the server binds must name the object unambiguously.
            let context = &encrypted.metadata.encryption_context;
            assert_eq!(context.get("bucket").map(String::as_str), Some(BUCKET));
            assert_eq!(context.get("object_key").map(String::as_str), Some(object_key.as_str()));
            assert_eq!(
                context.get("object").map(String::as_str),
                Some(object_key.as_str()),
                "the legacy `object` key must stay populated for older readers"
            );
            assert_eq!(context.get("algorithm").map(String::as_str), Some(algorithm.as_str()));

            let decrypted = read_all(
                service
                    .decrypt_object(BUCKET, &object_key, encrypted.ciphertext.clone(), &encrypted.metadata, None)
                    .await
                    .unwrap_or_else(|error| panic!("decrypting {size} bytes with {algorithm:?} failed: {error:?}")),
            )
            .await;
            assert_eq!(decrypted, data, "round-trip must return the original {size} bytes");
        }
    }
}

#[tokio::test]
async fn two_objects_never_share_a_data_key() {
    let (_kms, service) = service_with_key("sse-per-object").await;
    let data = payload(512);

    let first = service
        .encrypt_object(BUCKET, "first.bin", data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt first");
    let second = service
        .encrypt_object(BUCKET, "second.bin", data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt second");

    assert_ne!(
        first.metadata.encrypted_data_key, second.metadata.encrypted_data_key,
        "each object must carry its own wrapped data key"
    );
    assert_ne!(first.metadata.iv, second.metadata.iv, "each object must get a fresh IV");
    assert_ne!(
        first.ciphertext, second.ciphertext,
        "identical plaintext under different objects must not produce identical ciphertext"
    );

    // Re-encrypting the *same* object also produces fresh material: a repeated
    // PUT must not reuse the previous version's key or IV.
    let again = service
        .encrypt_object(BUCKET, "first.bin", data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("re-encrypt first");
    assert_ne!(
        first.metadata.encrypted_data_key, again.metadata.encrypted_data_key,
        "a repeated PUT of the same object must mint a new data key"
    );
    assert_ne!(first.ciphertext, again.ciphertext, "a repeated PUT must not be deterministic");
}

#[tokio::test]
async fn cross_object_ciphertext_and_metadata_do_not_interchange() {
    let (_kms, service) = service_with_key("sse-cross-object").await;

    let alpha = service
        .encrypt_object(BUCKET, "alpha.bin", payload(300).as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt alpha");
    let beta = service
        .encrypt_object(BUCKET, "beta.bin", payload(300).as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt beta");

    // Alpha's bytes under beta's metadata must fail authentication rather than
    // return beta's plaintext or garbage.
    assert!(
        service
            .decrypt_object(BUCKET, "beta.bin", alpha.ciphertext.clone(), &beta.metadata, None)
            .await
            .is_err(),
        "one object's ciphertext must not open under another's metadata"
    );

    // A spliced metadata record — beta's wrapped key grafted onto alpha's
    // record — must not decrypt either.
    let mut spliced = alpha.metadata.clone();
    spliced.encrypted_data_key = beta.metadata.encrypted_data_key.clone();
    assert!(
        service
            .decrypt_object(BUCKET, "alpha.bin", alpha.ciphertext.clone(), &spliced, None)
            .await
            .is_err(),
        "grafting another object's wrapped key must not yield a working decrypt"
    );
}

#[tokio::test]
async fn tampered_ciphertext_and_metadata_are_rejected() {
    let (_kms, service) = service_with_key("sse-tamper").await;
    let data = payload(1024);
    let encrypted = service
        .encrypt_object(BUCKET, "victim.bin", data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt");

    let attempt = |ciphertext: Vec<u8>, metadata: EncryptionMetadata| {
        let service = service.clone();
        async move {
            service
                .decrypt_object(BUCKET, "victim.bin", ciphertext, &metadata, None)
                .await
        }
    };

    assert!(
        attempt(flip_middle_bit(&encrypted.ciphertext), encrypted.metadata.clone())
            .await
            .is_err(),
        "a single flipped ciphertext bit must fail authentication"
    );
    assert!(
        attempt(
            encrypted.ciphertext[..encrypted.ciphertext.len() - 1].to_vec(),
            encrypted.metadata.clone()
        )
        .await
        .is_err(),
        "a truncated object must fail authentication"
    );

    let mut bad_iv = encrypted.metadata.clone();
    bad_iv.iv = flip_middle_bit(&encrypted.metadata.iv);
    assert!(
        attempt(encrypted.ciphertext.clone(), bad_iv).await.is_err(),
        "a tampered IV must fail authentication"
    );

    let mut bad_tag = encrypted.metadata.clone();
    bad_tag.tag = Some(flip_middle_bit(encrypted.metadata.tag.as_ref().expect("tag")));
    assert!(
        attempt(encrypted.ciphertext.clone(), bad_tag).await.is_err(),
        "a tampered tag must fail authentication"
    );

    let mut no_tag = encrypted.metadata.clone();
    no_tag.tag = None;
    assert_invalid_operation(discard(attempt(encrypted.ciphertext.clone(), no_tag).await), "Missing authentication tag");

    let mut bad_algorithm = encrypted.metadata.clone();
    bad_algorithm.algorithm = "ROT13".to_string();
    match discard(attempt(encrypted.ciphertext.clone(), bad_algorithm).await) {
        Err(KmsError::UnsupportedAlgorithm { algorithm }) => assert_eq!(algorithm, "ROT13"),
        other => panic!("expected UnsupportedAlgorithm, got {other:?}"),
    }

    // A rewritten context breaks the AAD, so the object stops opening — this is
    // what makes the stored context tamper-evident rather than advisory.
    let mut rewritten_context = encrypted.metadata.clone();
    rewritten_context
        .encryption_context
        .insert("bucket".to_string(), "attacker-bucket".to_string());
    assert!(
        attempt(encrypted.ciphertext.clone(), rewritten_context).await.is_err(),
        "rewriting the bound context must break decryption"
    );

    // The untouched original still decrypts, proving the failures above are
    // caused by the tampering and not by a broken fixture.
    let recovered = read_all(
        attempt(encrypted.ciphertext.clone(), encrypted.metadata.clone())
            .await
            .expect("the pristine object must still decrypt"),
    )
    .await;
    assert_eq!(recovered, data);
}

#[tokio::test]
async fn expected_context_validation_catches_a_relocated_object() {
    let (_kms, service) = service_with_key("sse-context-check").await;
    let encrypted = service
        .encrypt_object(
            BUCKET,
            "docs/report.pdf",
            payload(64).as_slice(),
            &EncryptionAlgorithm::Aes256,
            None,
            None,
        )
        .await
        .expect("encrypt");

    // Matching expectations pass through.
    read_all(
        service
            .decrypt_object(
                BUCKET,
                "docs/report.pdf",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                Some(&ctx(&[("bucket", BUCKET), ("object_key", "docs/report.pdf")])),
            )
            .await
            .expect("a matching expected context must be accepted"),
    )
    .await;

    // A caller expecting a different object refuses before touching the KMS:
    // this is the guard against a ciphertext being served under another key.
    assert_context_mismatch(discard(
        service
            .decrypt_object(
                BUCKET,
                "docs/report.pdf",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                Some(&ctx(&[("object_key", "docs/other.pdf")])),
            )
            .await,
    ));
    assert_context_mismatch(discard(
        service
            .decrypt_object(
                BUCKET,
                "docs/report.pdf",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                Some(&ctx(&[("bucket", "another-bucket")])),
            )
            .await,
    ));
    // A key that was never bound cannot be satisfied.
    assert_context_mismatch(discard(
        service
            .decrypt_object(
                BUCKET,
                "docs/report.pdf",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                Some(&ctx(&[("never-bound", "value")])),
            )
            .await,
    ));
}

/// The projection an object actually survives on: metadata is written to disk
/// as headers and rebuilt from them on the next GET.
#[tokio::test]
async fn metadata_survives_the_header_projection() {
    let (_kms, service) = service_with_key("sse-headers").await;

    for algorithm in [EncryptionAlgorithm::Aes256, EncryptionAlgorithm::ChaCha20Poly1305] {
        let object_key = format!("headers/{}.bin", algorithm.as_str());
        let data = payload(2048);
        let encrypted = service
            .encrypt_object(BUCKET, &object_key, data.as_slice(), &algorithm, None, None)
            .await
            .expect("encrypt");

        let headers = service.metadata_to_headers(&encrypted.metadata);

        // The S3-visible header must reflect the mode the object was written in.
        match algorithm {
            EncryptionAlgorithm::Aes256 => assert_eq!(
                headers.get("x-amz-server-side-encryption").map(String::as_str),
                Some("AES256"),
                "AES-256 objects advertise SSE-S3"
            ),
            _ => {
                assert_eq!(
                    headers.get("x-amz-server-side-encryption").map(String::as_str),
                    Some("aws:kms"),
                    "non-AES-256 objects advertise SSE-KMS"
                );
                assert_eq!(
                    headers.get("x-amz-server-side-encryption-aws-kms-key-id").map(String::as_str),
                    Some(encrypted.metadata.key_id.as_str()),
                    "SSE-KMS must name its key in the S3 header"
                );
            }
        }
        assert_eq!(
            headers.get("x-rustfs-encryption-key-id").map(String::as_str),
            Some(encrypted.metadata.key_id.as_str()),
            "the internal key-id header must always be present for KMS-backed objects"
        );
        for required in [
            "x-rustfs-encryption-iv",
            "x-rustfs-encryption-tag",
            "x-rustfs-encryption-key",
            "x-rustfs-encryption-context",
        ] {
            assert!(headers.contains_key(required), "header {required} is required to rebuild metadata");
        }
        // Nothing plaintext-sensitive may ride along in a header.
        assert!(
            !headers.values().any(|value| value.contains("BEGIN")),
            "headers must not carry key material"
        );

        let rebuilt = service
            .headers_to_metadata(&headers)
            .expect("headers written by this service must parse back");

        assert_eq!(rebuilt.key_id, encrypted.metadata.key_id, "key id must survive the projection");
        assert_eq!(rebuilt.iv, encrypted.metadata.iv, "IV must survive the projection");
        assert_eq!(rebuilt.tag, encrypted.metadata.tag, "tag must survive the projection");
        assert_eq!(
            rebuilt.encrypted_data_key, encrypted.metadata.encrypted_data_key,
            "the wrapped DEK must survive the projection"
        );
        assert_eq!(
            rebuilt.encryption_context, encrypted.metadata.encryption_context,
            "the bound context must survive the projection"
        );

        // The point of the projection: the rebuilt record must open the object.
        // Field equality is not enough — `decrypt_object` derives its AEAD
        // additional data from the context, so the context has to survive as
        // *bytes*, not merely as a map.
        let decrypted = read_all(
            service
                .decrypt_object(BUCKET, &object_key, encrypted.ciphertext.clone(), &rebuilt, None)
                .await
                .unwrap_or_else(|error| panic!("[{algorithm:?}] rebuilt metadata must decrypt the object: {error:?}")),
        )
        .await;
        assert_eq!(decrypted, data, "[{algorithm:?}] the header round-trip must preserve the plaintext");
    }
}

#[tokio::test]
async fn headers_missing_required_fields_are_rejected() {
    let (_kms, service) = service_with_key("sse-bad-headers").await;
    let encrypted = service
        .encrypt_object(
            BUCKET,
            "bad-headers.bin",
            payload(32).as_slice(),
            &EncryptionAlgorithm::Aes256,
            None,
            None,
        )
        .await
        .expect("encrypt");
    let good = service.metadata_to_headers(&encrypted.metadata);

    let without = |name: &str| {
        let mut headers = good.clone();
        headers.remove(name);
        headers
    };

    assert!(
        service.headers_to_metadata(&HashMap::new()).is_err(),
        "an empty header set carries no algorithm and must be rejected"
    );
    assert!(
        service.headers_to_metadata(&without("x-amz-server-side-encryption")).is_err(),
        "the algorithm header is required"
    );
    assert!(
        service.headers_to_metadata(&without("x-rustfs-encryption-iv")).is_err(),
        "the IV header is required"
    );

    // Malformed base64 must be a validation error, not a panic.
    for field in ["x-rustfs-encryption-iv", "x-rustfs-encryption-tag", "x-rustfs-encryption-key"] {
        let mut headers = good.clone();
        headers.insert(field.to_string(), "!!! not base64 !!!".to_string());
        assert!(
            service.headers_to_metadata(&headers).is_err(),
            "malformed base64 in {field} must be rejected"
        );
    }

    let mut bad_context = good.clone();
    bad_context.insert("x-rustfs-encryption-context".to_string(), "{not json".to_string());
    assert!(
        service.headers_to_metadata(&bad_context).is_err(),
        "a malformed context header must be rejected"
    );
}

#[tokio::test]
async fn sse_s3_auto_creates_its_key_but_sse_kms_requires_one() {
    // No key exists yet; only the default id is configured.
    let kms = TestKms::local_with(|config| config.default_key_id = Some("auto-created".to_string())).await;
    let service = kms.service().await;

    let encrypted = service
        .encrypt_object(BUCKET, "auto.bin", payload(16).as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("SSE-S3 must auto-create its default key");
    assert_eq!(encrypted.metadata.key_id, "auto-created");

    // The key really exists now and the object opens.
    let manager = kms.kms().await;
    manager
        .describe_key(rustfs_kms::DescribeKeyRequest {
            key_id: "auto-created".to_string(),
        })
        .await
        .expect("the auto-created key must be describable afterwards");
    read_all(
        service
            .decrypt_object(BUCKET, "auto.bin", encrypted.ciphertext.clone(), &encrypted.metadata, None)
            .await
            .expect("the auto-created key must decrypt its object"),
    )
    .await;

    // A non-AES-256 algorithm is SSE-KMS: the caller must have provisioned the
    // key, because auto-creating a customer-named key would be surprising.
    assert_invalid_operation(
        discard(
            service
                .encrypt_object(
                    BUCKET,
                    "explicit.bin",
                    payload(16).as_slice(),
                    &EncryptionAlgorithm::ChaCha20Poly1305,
                    Some("never-created"),
                    None,
                )
                .await,
        ),
        "not found",
    );

    // With no key id and no default at all, the call is a configuration error.
    let bare = TestKms::local().await;
    let bare_service = bare.service().await;
    match bare_service
        .encrypt_object(BUCKET, "bare.bin", payload(16).as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
    {
        Err(KmsError::ConfigurationError { message }) => {
            assert!(message.contains("No KMS key ID"), "should explain the missing key id: {message}")
        }
        other => panic!("expected ConfigurationError, got {other:?}"),
    }
}

#[tokio::test]
async fn caller_supplied_context_is_merged_and_bound() {
    let (_kms, service) = service_with_key("sse-extra-context").await;
    let extra = ctx(&[("tenant", "acme"), ("classification", "internal")]);

    let encrypted = service
        .encrypt_object(
            BUCKET,
            "tenant.bin",
            payload(128).as_slice(),
            &EncryptionAlgorithm::Aes256,
            None,
            Some(&extra),
        )
        .await
        .expect("encrypt with extra context");

    for (key, value) in &extra {
        assert_eq!(
            encrypted.metadata.encryption_context.get(key),
            Some(value),
            "caller context {key} must be preserved in the stored metadata"
        );
    }
    // The server-owned keys are still present and win over any caller value.
    assert_eq!(encrypted.metadata.encryption_context.get("bucket").map(String::as_str), Some(BUCKET));

    read_all(
        service
            .decrypt_object(BUCKET, "tenant.bin", encrypted.ciphertext.clone(), &encrypted.metadata, Some(&extra))
            .await
            .expect("the merged context must validate and decrypt"),
    )
    .await;

    // Dropping a caller-supplied key from the stored context breaks the AAD.
    let mut stripped = encrypted.metadata.clone();
    stripped.encryption_context.remove("tenant");
    assert!(
        service
            .decrypt_object(BUCKET, "tenant.bin", encrypted.ciphertext.clone(), &stripped, None)
            .await
            .is_err(),
        "removing a bound context entry must break decryption"
    );
}

#[tokio::test]
async fn sse_c_round_trips_and_rejects_the_wrong_key() {
    let (_kms, service) = service_with_key("sse-c-unused").await;
    let customer_key = [0x11u8; 32];
    let wrong_key = [0x22u8; 32];
    let data = payload(4096);

    let encrypted = service
        .encrypt_object_with_customer_key(BUCKET, "customer.bin", data.as_slice(), &customer_key, None)
        .await
        .expect("SSE-C encrypt");

    assert_eq!(
        encrypted.metadata.key_id, "sse-c",
        "SSE-C objects are marked so a GET knows not to consult the KMS"
    );
    assert!(
        encrypted.metadata.encrypted_data_key.is_empty(),
        "SSE-C stores no wrapped data key: the customer holds the only copy"
    );
    assert_eq!(encrypted.metadata.original_size, data.len() as u64);

    let decrypted = read_all(
        service
            .decrypt_object_with_customer_key(
                BUCKET,
                "customer.bin",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                &customer_key,
            )
            .await
            .expect("SSE-C decrypt with the right key"),
    )
    .await;
    assert_eq!(decrypted, data, "SSE-C round-trip must return the original bytes");

    assert!(
        service
            .decrypt_object_with_customer_key(
                BUCKET,
                "customer.bin",
                encrypted.ciphertext.clone(),
                &encrypted.metadata,
                &wrong_key
            )
            .await
            .is_err(),
        "a different customer key must not open the object"
    );

    // Key length is validated on both sides before any crypto happens.
    assert_invalid_key_size(
        service
            .encrypt_object_with_customer_key(BUCKET, "short.bin", data.as_slice(), &[0u8; 16], None)
            .await,
        32,
        16,
    );
    assert_invalid_key_size(
        discard(
            service
                .decrypt_object_with_customer_key(
                    BUCKET,
                    "customer.bin",
                    encrypted.ciphertext.clone(),
                    &encrypted.metadata,
                    &[0u8; 31],
                )
                .await,
        ),
        32,
        31,
    );
}

#[tokio::test]
async fn sse_c_validates_the_supplied_key_md5() {
    let (_kms, service) = service_with_key("sse-c-md5-unused").await;
    let customer_key = [0x33u8; 32];
    let correct_md5 = hex::encode(md5_of(&customer_key));

    service
        .encrypt_object_with_customer_key(BUCKET, "md5-ok.bin", payload(64).as_slice(), &customer_key, Some(&correct_md5))
        .await
        .expect("a matching MD5 must be accepted");

    // Uppercase is accepted: the comparison is case-insensitive on the input.
    service
        .encrypt_object_with_customer_key(
            BUCKET,
            "md5-upper.bin",
            payload(64).as_slice(),
            &customer_key,
            Some(&correct_md5.to_uppercase()),
        )
        .await
        .expect("MD5 comparison must be case-insensitive");

    match service
        .encrypt_object_with_customer_key(
            BUCKET,
            "md5-bad.bin",
            payload(64).as_slice(),
            &customer_key,
            Some("00000000000000000000000000000000"),
        )
        .await
    {
        Err(KmsError::ValidationError { message }) => {
            assert!(message.contains("MD5"), "the error must name the MD5 check: {message}")
        }
        other => panic!("expected ValidationError for an MD5 mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn sse_c_and_kms_objects_do_not_cross_paths() {
    let (_kms, service) = service_with_key("sse-c-crossover").await;
    let customer_key = [0x44u8; 32];

    let kms_object = service
        .encrypt_object(BUCKET, "kms.bin", payload(256).as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("KMS encrypt");
    let sse_c_object = service
        .encrypt_object_with_customer_key(BUCKET, "customer.bin", payload(256).as_slice(), &customer_key, None)
        .await
        .expect("SSE-C encrypt");

    // A KMS-encrypted object must not be openable through the SSE-C path, even
    // with a valid-looking key: the metadata marker is what routes the GET.
    assert_invalid_operation(
        discard(
            service
                .decrypt_object_with_customer_key(
                    BUCKET,
                    "kms.bin",
                    kms_object.ciphertext.clone(),
                    &kms_object.metadata,
                    &customer_key,
                )
                .await,
        ),
        "not encrypted with SSE-C",
    );

    // And an SSE-C object has no wrapped data key for the KMS path to unwrap.
    assert!(
        service
            .decrypt_object(BUCKET, "customer.bin", sse_c_object.ciphertext.clone(), &sse_c_object.metadata, None)
            .await
            .is_err(),
        "the KMS path must not be able to open an SSE-C object"
    );

    // The SSE-C header projection advertises the customer algorithm, which is
    // how `headers_to_metadata` re-identifies the object on the way back.
    let headers = service.metadata_to_headers(&sse_c_object.metadata);
    assert_eq!(
        headers
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(String::as_str),
        Some("AES256"),
        "SSE-C objects must advertise the customer algorithm header"
    );
    assert!(!headers.contains_key("x-rustfs-encryption-key-id"), "SSE-C must not claim a KMS key id");
    let rebuilt = service.headers_to_metadata(&headers).expect("SSE-C headers must parse");
    assert_eq!(rebuilt.key_id, "sse-c", "the SSE-C marker must survive the header projection");
    assert!(
        rebuilt.encrypted_data_key.is_empty(),
        "no wrapped key may materialise out of SSE-C headers"
    );
    assert_eq!(
        rebuilt.encryption_context, sse_c_object.metadata.encryption_context,
        "the SSE-C context must survive the projection"
    );

    let decrypted = read_all(
        service
            .decrypt_object_with_customer_key(BUCKET, "customer.bin", sse_c_object.ciphertext.clone(), &rebuilt, &customer_key)
            .await
            .expect("SSE-C must decrypt from a record rebuilt out of its own headers"),
    )
    .await;
    assert_eq!(decrypted.len(), 256, "the SSE-C header round-trip must preserve the object");
}

fn md5_of(bytes: &[u8]) -> Vec<u8> {
    use md5::Digest as _;
    let mut hasher = md5::Md5::new();
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

/// Serialize a context in reverse-sorted key order.
///
/// Deliberately not the canonical ordering, and derived from the real context
/// rather than hand-written: the service adds its own bucket-path entry, so a
/// literal would silently describe a different map and prove nothing.
fn non_canonical_context_json(context: &HashMap<String, String>) -> String {
    let mut entries: Vec<_> = context.iter().collect();
    entries.sort_by(|left, right| right.0.cmp(left.0));
    let body = entries
        .iter()
        .map(|(key, value)| {
            format!(
                "{}:{}",
                serde_json::to_string(key).expect("key serializes"),
                serde_json::to_string(value).expect("value serializes")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

/// An object sealed before the context was canonicalized must still open.
///
/// The AAD is the *serialization* of the encryption context, not the map. Any
/// object written while the context was serialized straight from a `HashMap`
/// carries whatever order that map happened to iterate in, and
/// `x-rustfs-encryption-context` is where that exact byte sequence survives.
/// Canonicalizing on the way back in would recompute sorted AAD, fail the AEAD,
/// and make a readable object permanently unreadable — so the stored bytes have
/// to win over anything re-derived from the parsed map.
///
/// The legacy object is reconstructed here the only way a black-box test can:
/// by rewriting the projected header into a non-sorted ordering *and* pinning
/// the sealed bytes to that same ordering, which is exactly the on-disk state a
/// pre-upgrade write left behind.
#[tokio::test]
async fn an_object_sealed_under_a_non_canonical_context_still_opens() {
    let (_kms, service) = service_with_key("sse-legacy-aad").await;
    let object_key = "legacy-context.bin";
    let data = payload(512);

    // Several entries, so an ordering difference is observable at all.
    let context = ctx(&[("zeta", "26"), ("alpha", "1"), ("mu", "13")]);
    let encrypted = service
        .encrypt_object(BUCKET, object_key, data.as_slice(), &EncryptionAlgorithm::Aes256, None, Some(&context))
        .await
        .expect("encrypting with a multi-entry context should succeed");

    let headers = service.metadata_to_headers(&encrypted.metadata);
    let stored_context = headers
        .get("x-rustfs-encryption-context")
        .expect("the context must be projected into a header");

    // What the projection stores must be the bytes the object was sealed
    // under, or the two can drift apart without anything failing yet.
    let rebuilt = service.headers_to_metadata(&headers).expect("headers must parse back");
    assert_eq!(
        rebuilt.context_aad.as_deref(),
        Some(stored_context.as_bytes()),
        "the rebuilt record must carry the stored context bytes verbatim"
    );
    let reopened = read_all(
        service
            .decrypt_object(BUCKET, object_key, encrypted.ciphertext.clone(), &rebuilt, None)
            .await
            .expect("an object must open from its own projected headers"),
    )
    .await;
    assert_eq!(reopened, data);

    // Now the legacy shape: same pairs, different serialization order. An
    // object written before canonicalization has exactly this on disk.
    let legacy_json = non_canonical_context_json(&encrypted.metadata.encryption_context);
    let legacy_json = legacy_json.as_str();
    assert_ne!(
        legacy_json, stored_context,
        "the legacy ordering must actually differ from the canonical one, or this proves nothing"
    );
    let legacy_context: HashMap<String, String> = serde_json::from_str(legacy_json).expect("legacy context parses");
    assert_eq!(legacy_context, encrypted.metadata.encryption_context, "same pairs, different order");

    let legacy_metadata = EncryptionMetadata {
        context_aad: Some(legacy_json.as_bytes().to_vec()),
        encryption_context: legacy_context,
        ..encrypted.metadata.clone()
    };

    // Re-projecting a legacy record must not rewrite it into sorted form: that
    // would destroy the only copy of the ordering the object needs.
    let legacy_headers = service.metadata_to_headers(&legacy_metadata);
    assert_eq!(
        legacy_headers.get("x-rustfs-encryption-context").map(String::as_str),
        Some(legacy_json),
        "a re-projection must preserve the original context ordering byte-for-byte"
    );
    assert_eq!(
        service
            .headers_to_metadata(&legacy_headers)
            .expect("legacy headers must parse")
            .context_aad
            .as_deref(),
        Some(legacy_json.as_bytes()),
        "the legacy ordering must survive a full header round trip"
    );
}

/// Rewriting the stored context is a tamper, not a legacy read.
///
/// The flip side of honouring the stored bytes: they are authenticated, so
/// changing them — even to a reordering that parses to an identical map — must
/// fail rather than silently re-deriving a working AAD.
#[tokio::test]
async fn a_rewritten_context_header_fails_authentication() {
    let (_kms, service) = service_with_key("sse-tampered-context").await;
    let object_key = "tampered-context.bin";
    let data = payload(256);

    let context = ctx(&[("zeta", "26"), ("alpha", "1"), ("mu", "13")]);
    let encrypted = service
        .encrypt_object(BUCKET, object_key, data.as_slice(), &EncryptionAlgorithm::Aes256, None, Some(&context))
        .await
        .expect("encrypt should succeed");

    let mut headers = service.metadata_to_headers(&encrypted.metadata);
    // Same pairs, different serialization: a pure ordering rewrite, so the
    // rejection can only come from the AAD bytes and not from a changed map.
    headers.insert(
        "x-rustfs-encryption-context".to_string(),
        non_canonical_context_json(&encrypted.metadata.encryption_context),
    );

    let tampered = service.headers_to_metadata(&headers).expect("tampered headers still parse");
    assert!(
        discard(
            service
                .decrypt_object(BUCKET, object_key, encrypted.ciphertext.clone(), &tampered, None)
                .await
        )
        .is_err(),
        "a context the object was not sealed under must not open it"
    );
}

/// The SSE-C flank of the tamper check above: the customer-key path prefers
/// the stored AAD bytes through the same branch, so a reverted preference —
/// re-deriving canonical bytes from the parsed map — would open a tampered
/// object here too, and only an SSE-C probe would notice.
#[tokio::test]
async fn a_rewritten_sse_c_context_header_fails_authentication() {
    let (_kms, service) = service_with_key("sse-c-tampered-context").await;
    let object_key = "tampered-sse-c.bin";
    let customer_key = [0x55u8; 32];
    let data = payload(256);

    let encrypted = service
        .encrypt_object_with_customer_key(BUCKET, object_key, data.as_slice(), &customer_key, None)
        .await
        .expect("SSE-C encrypt should succeed");

    let mut headers = service.metadata_to_headers(&encrypted.metadata);
    // Same pairs, different serialization: a pure ordering rewrite, so the
    // rejection can only come from the AAD bytes and not from a changed map.
    let rewritten = non_canonical_context_json(&encrypted.metadata.encryption_context);
    assert_ne!(
        Some(rewritten.as_str()),
        headers.get("x-rustfs-encryption-context").map(String::as_str),
        "the rewrite must actually change the stored bytes, or this proves nothing"
    );
    headers.insert("x-rustfs-encryption-context".to_string(), rewritten);

    let tampered = service.headers_to_metadata(&headers).expect("tampered headers still parse");
    assert!(
        discard(
            service
                .decrypt_object_with_customer_key(BUCKET, object_key, encrypted.ciphertext.clone(), &tampered, &customer_key)
                .await
        )
        .is_err(),
        "a context the SSE-C object was not sealed under must not open it, even with the right key"
    );
}

/// An SSE-KMS object written before the internal `x-rustfs-` headers existed
/// must still open, and must rebuild into a record that names its real cipher.
///
/// Back then `x-amz-server-side-encryption: aws:kms` plus the S3 key-id header
/// was the whole record, and AES-256-GCM was the only cipher in use — which is
/// exactly the assumption the `aws:kms` fallback in `headers_to_metadata`
/// encodes. The fallback is a normalization: `aws:kms` also parses as a cipher
/// alias for AES-256-GCM, so the object opens either way, but only the
/// normalized record re-projects the cipher header a modern read expects. The
/// legacy header shape is reconstructed here by rewriting the SSE mode to
/// `aws:kms`, adding the S3 key-id header, and dropping both internal headers.
#[tokio::test]
async fn a_legacy_aws_kms_object_without_the_cipher_header_still_opens() {
    let (_kms, service) = service_with_key("sse-legacy-mode").await;
    let object_key = "legacy-aws-kms.bin";
    let data = payload(512);

    let encrypted = service
        .encrypt_object(BUCKET, object_key, data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("encrypt should succeed");

    let mut headers = service.metadata_to_headers(&encrypted.metadata);
    headers.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
    headers.insert(
        "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
        encrypted.metadata.key_id.clone(),
    );
    for internal in ["x-rustfs-encryption-algorithm", "x-rustfs-encryption-key-id"] {
        headers
            .remove(internal)
            .unwrap_or_else(|| panic!("the modern projection must write the {internal} header this test deletes"));
    }

    let rebuilt = service
        .headers_to_metadata(&headers)
        .expect("a pre-internal-header record must still parse");
    assert_eq!(rebuilt.key_id, encrypted.metadata.key_id, "the S3 key-id header must resolve the key");
    assert_eq!(
        rebuilt.algorithm,
        EncryptionAlgorithm::Aes256.as_str(),
        "aws:kms with no cipher header must normalize to the only cipher that era wrote"
    );

    // The normalization is what a re-projection stores: the upgraded record
    // writes the modern cipher header instead of perpetuating the gap.
    let reprojected = service.metadata_to_headers(&rebuilt);
    assert_eq!(
        reprojected.get("x-rustfs-encryption-algorithm").map(String::as_str),
        Some(EncryptionAlgorithm::Aes256.as_str()),
        "re-projecting the rebuilt record must write the cipher header"
    );

    let decrypted = read_all(
        service
            .decrypt_object(BUCKET, object_key, encrypted.ciphertext.clone(), &rebuilt, None)
            .await
            .expect("a legacy aws:kms object must still open"),
    )
    .await;
    assert_eq!(decrypted, data, "the rebuilt record must recover the full plaintext");
}

/// Metadata persisted before `context_aad` existed deserializes with `None`
/// there, and decrypt must then re-derive the AAD from the parsed context.
/// That derived path only opens the object because the seal side canonicalizes
/// the very same way — this is the independent probe of that pairing, for both
/// the KMS and the customer-key flavours.
#[tokio::test]
async fn metadata_without_stored_context_bytes_still_opens() {
    let (_kms, service) = service_with_key("sse-derived-aad").await;
    let data = payload(512);
    // Several entries, so canonicalization has an ordering to actually decide.
    let context = ctx(&[("zeta", "26"), ("alpha", "1"), ("mu", "13")]);

    // Byte-equality of the derived and stored AAD is what keeps the `None`
    // path working, so pin the seal side of that pairing directly: the sealed
    // record must carry exactly the canonical serialization of its context.
    let canonical_aad = |context: &HashMap<String, String>| {
        let canonical: std::collections::BTreeMap<&str, &str> =
            context.iter().map(|(key, value)| (key.as_str(), value.as_str())).collect();
        serde_json::to_vec(&canonical).expect("context serializes")
    };

    let encrypted = service
        .encrypt_object(
            BUCKET,
            "derived-aad.bin",
            data.as_slice(),
            &EncryptionAlgorithm::Aes256,
            None,
            Some(&context),
        )
        .await
        .expect("encrypt should succeed");
    assert_eq!(
        encrypted.metadata.context_aad.as_deref(),
        Some(canonical_aad(&encrypted.metadata.encryption_context).as_slice()),
        "the seal must pin the exact canonical AAD bytes it fed the AEAD"
    );
    let stripped = EncryptionMetadata {
        context_aad: None,
        ..encrypted.metadata.clone()
    };
    let decrypted = read_all(
        service
            .decrypt_object(BUCKET, "derived-aad.bin", encrypted.ciphertext.clone(), &stripped, None)
            .await
            .expect("metadata with no stored AAD bytes must open through the derived path"),
    )
    .await;
    assert_eq!(decrypted, data, "the derived AAD must match the bytes the object was sealed under");

    // The SSE-C record carries the same optional field through the same serde
    // default, so its derived path needs its own proof.
    let customer_key = [0x66u8; 32];
    let sse_c = service
        .encrypt_object_with_customer_key(BUCKET, "derived-aad-c.bin", data.as_slice(), &customer_key, None)
        .await
        .expect("SSE-C encrypt should succeed");
    assert_eq!(
        sse_c.metadata.context_aad.as_deref(),
        Some(canonical_aad(&sse_c.metadata.encryption_context).as_slice()),
        "the SSE-C seal must pin the exact canonical AAD bytes it fed the AEAD"
    );
    let stripped = EncryptionMetadata {
        context_aad: None,
        ..sse_c.metadata.clone()
    };
    let decrypted = read_all(
        service
            .decrypt_object_with_customer_key(BUCKET, "derived-aad-c.bin", sse_c.ciphertext.clone(), &stripped, &customer_key)
            .await
            .expect("SSE-C metadata with no stored AAD bytes must open through the derived path"),
    )
    .await;
    assert_eq!(decrypted, data, "the SSE-C derived AAD must match the bytes the object was sealed under");
}
