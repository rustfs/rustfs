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

//! Black-box behavior: master-key crypto and data-encryption-key semantics.
//!
//! Three invariants carry most of the weight here:
//!
//! 1. **Every DEK is fresh.** `lib.rs` forbids caching a generated data key by
//!    master key id: a DEK and its ciphertext are bound to one object's
//!    encryption context, so reuse would both break context validation and
//!    violate the per-object DEK model SSE-S3 and SSE-KMS assume.
//! 2. **The encryption context is authenticated.** It is the AEAD's additional
//!    data, so a wrong value must fail decryption rather than silently return
//!    the wrong plaintext.
//! 3. **Corrupt input fails cleanly.** Tampered, truncated, or foreign
//!    ciphertext returns a typed error and never panics — this input is
//!    attacker-reachable through object metadata.
//!
//! One deliberate carve-out is pinned below: an *empty* request context skips
//! the "missing context key" check so legacy objects, written before contexts
//! were bound, stay readable. A *wrong* value is still rejected.

mod common;

use common::{BackendCase, TestKms, assert_context_mismatch, ctx, flip_middle_bit, for_each_backend, payload};
use rustfs_kms::{
    DecryptRequest, EncryptRequest, GenerateDataKeyRequest, KeySpec, KmsError, ObjectEncryptionContext, is_data_key_envelope,
};

fn context() -> std::collections::HashMap<String, String> {
    ctx(&[("bucket", "crypto-behavior"), ("object", "alpha.bin")])
}

#[tokio::test]
async fn master_key_encrypt_decrypt_round_trips_under_the_same_context() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let plaintext = payload(1024);

        let encrypted = manager
            .encrypt(EncryptRequest {
                key_id: case.key_id.clone(),
                plaintext: plaintext.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] encrypt should succeed: {error:?}"));

        assert!(!encrypted.ciphertext.is_empty(), "[{label}] ciphertext must not be empty");
        assert_ne!(encrypted.ciphertext, plaintext, "[{label}] ciphertext must not equal the plaintext");
        assert_eq!(encrypted.key_id, case.key_id, "[{label}] the response names the key used");
        assert!(!encrypted.algorithm.is_empty(), "[{label}] the algorithm must be reported");

        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: encrypted.ciphertext.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] decrypt should succeed: {error:?}"));
        assert_eq!(decrypted.plaintext, plaintext, "[{label}] round-trip must return the input");
        assert_eq!(decrypted.key_id, case.key_id, "[{label}] decrypt reports the key it used");

        // Encrypting the same plaintext twice must not produce the same
        // ciphertext: a fresh nonce per call is what keeps AES-GCM safe.
        let again = manager
            .encrypt(EncryptRequest {
                key_id: case.key_id.clone(),
                plaintext: plaintext.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] second encrypt should succeed: {error:?}"));
        assert_ne!(
            again.ciphertext, encrypted.ciphertext,
            "[{label}] repeated encryption of identical plaintext must not be deterministic"
        );
    })
    .await;
}

#[tokio::test]
async fn empty_plaintext_round_trips() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        let encrypted = manager
            .encrypt(EncryptRequest {
                key_id: case.key_id.clone(),
                plaintext: Vec::new(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] encrypting nothing should still succeed: {error:?}"));

        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: encrypted.ciphertext,
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] decrypt should succeed: {error:?}"));
        assert!(decrypted.plaintext.is_empty(), "[{label}] empty in, empty out");
    })
    .await;
}

#[tokio::test]
async fn encryption_context_is_authenticated() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        let dek = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: case.key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] generate should succeed: {error:?}"));

        // A changed value for a bound key is a mismatch.
        assert_context_mismatch(
            manager
                .decrypt(DecryptRequest {
                    ciphertext: dek.ciphertext_blob.clone(),
                    encryption_context: ctx(&[("bucket", "crypto-behavior"), ("object", "other.bin")]),
                    grant_tokens: Vec::new(),
                })
                .await,
        );

        // A non-empty context that omits a bound key is also a mismatch.
        assert_context_mismatch(
            manager
                .decrypt(DecryptRequest {
                    ciphertext: dek.ciphertext_blob.clone(),
                    encryption_context: ctx(&[("bucket", "crypto-behavior")]),
                    grant_tokens: Vec::new(),
                })
                .await,
        );

        // Extra keys beyond the bound set are tolerated: only the bound pairs
        // are authenticated, so adding context cannot lock an object out.
        let with_extra = manager
            .decrypt(DecryptRequest {
                ciphertext: dek.ciphertext_blob.clone(),
                encryption_context: ctx(&[("bucket", "crypto-behavior"), ("object", "alpha.bin"), ("unrelated", "value")]),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] a superset context should decrypt: {error:?}"));
        assert_eq!(with_extra.plaintext, dek.plaintext_key);

        // The documented legacy carve-out: a fully empty request context skips
        // the missing-key check so pre-context objects stay readable.
        let legacy = manager
            .decrypt(DecryptRequest {
                ciphertext: dek.ciphertext_blob.clone(),
                encryption_context: Default::default(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] the empty-context legacy path must work: {error:?}"));
        assert_eq!(
            legacy.plaintext, dek.plaintext_key,
            "[{label}] the legacy path must return the same data key"
        );
    })
    .await;
}

#[tokio::test]
async fn every_generated_data_key_is_fresh() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        // Same key id, same context, repeated: nothing may be reused.
        let mut plaintexts = Vec::new();
        let mut ciphertexts = Vec::new();
        for _ in 0..8 {
            let dek = manager
                .generate_data_key(GenerateDataKeyRequest {
                    key_id: case.key_id.clone(),
                    key_spec: KeySpec::Aes256,
                    encryption_context: context(),
                })
                .await
                .unwrap_or_else(|error| panic!("[{label}] generate should succeed: {error:?}"));

            assert_eq!(dek.plaintext_key.len(), 32, "[{label}] an AES-256 DEK is 32 bytes");
            assert!(!dek.ciphertext_blob.is_empty(), "[{label}] the wrapped DEK must not be empty");
            assert!(
                !dek.ciphertext_blob
                    .windows(dek.plaintext_key.len())
                    .any(|window| window == dek.plaintext_key),
                "[{label}] the wrapped blob must never contain the plaintext data key"
            );
            plaintexts.push(dek.plaintext_key);
            ciphertexts.push(dek.ciphertext_blob);
        }

        for i in 0..plaintexts.len() {
            for j in (i + 1)..plaintexts.len() {
                assert_ne!(
                    plaintexts[i], plaintexts[j],
                    "[{label}] data keys must not repeat across calls (indices {i} and {j})"
                );
                assert_ne!(
                    ciphertexts[i], ciphertexts[j],
                    "[{label}] wrapped data keys must not repeat across calls (indices {i} and {j})"
                );
            }
        }

        // Each wrapped blob still opens to exactly its own plaintext.
        for (index, (expected, blob)) in plaintexts.iter().zip(ciphertexts.iter()).enumerate() {
            let decrypted = manager
                .decrypt(DecryptRequest {
                    ciphertext: blob.clone(),
                    encryption_context: context(),
                    grant_tokens: Vec::new(),
                })
                .await
                .unwrap_or_else(|error| panic!("[{label}] blob {index} should decrypt: {error:?}"));
            assert_eq!(&decrypted.plaintext, expected, "[{label}] blob {index} opened to the wrong key");
        }
    })
    .await;
}

#[tokio::test]
async fn data_key_spec_controls_the_length_of_the_generated_key() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        // A backend that accepts a `key_spec` must honour it. Silently
        // returning a different size means the caller builds a cipher from
        // material it did not ask for, and the envelope records a spec its
        // payload does not match.
        for spec in [KeySpec::Aes256, KeySpec::Aes128] {
            let generated = manager
                .generate_data_key(GenerateDataKeyRequest {
                    key_id: case.key_id.clone(),
                    key_spec: spec.clone(),
                    encryption_context: context(),
                })
                .await
                .unwrap_or_else(|error| panic!("[{label}] {spec:?} generate should succeed: {error:?}"));
            assert_eq!(
                generated.plaintext_key.len(),
                spec.key_size(),
                "[{label}] {spec:?} must yield a {}-byte data key",
                spec.key_size()
            );
        }

        let aes128 = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: case.key_id.clone(),
                key_spec: KeySpec::Aes128,
                encryption_context: context(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] AES-128 generate should succeed: {error:?}"));

        // Whatever the length, the blob still round-trips.
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: aes128.ciphertext_blob,
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] AES-128 blob should decrypt: {error:?}"));
        assert_eq!(decrypted.plaintext, aes128.plaintext_key);
    })
    .await;
}

#[tokio::test]
async fn corrupt_ciphertext_fails_cleanly() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        let dek = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: case.key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] generate should succeed: {error:?}"));

        let decrypt = |ciphertext: Vec<u8>| {
            let manager = manager.clone();
            async move {
                manager
                    .decrypt(DecryptRequest {
                        ciphertext,
                        encryption_context: context(),
                        grant_tokens: Vec::new(),
                    })
                    .await
            }
        };

        // A single flipped bit anywhere in the envelope must not decrypt.
        let tampered = flip_middle_bit(&dek.ciphertext_blob);
        assert!(decrypt(tampered).await.is_err(), "[{label}] a bit-flipped envelope must not decrypt");

        // Truncation, emptiness, and non-envelope bytes are all typed errors.
        for (name, input) in [
            ("truncated", dek.ciphertext_blob[..dek.ciphertext_blob.len() / 2].to_vec()),
            ("empty", Vec::new()),
            ("not-json", b"absolutely not an envelope".to_vec()),
            ("json-but-wrong-shape", br#"{"hello":"world"}"#.to_vec()),
        ] {
            let error = decrypt(input)
                .await
                .expect_err(&format!("[{label}] {name} input must be rejected"));
            assert!(
                !matches!(error, KmsError::InternalError { .. }),
                "[{label}] {name} input must map to a specific error, not InternalError: {error:?}"
            );
        }

        // Truncating only the AEAD tail (keeping the envelope parseable) must
        // fail authentication rather than return partial plaintext.
        let mut short_envelope = dek.ciphertext_blob.clone();
        short_envelope.pop();
        assert!(decrypt(short_envelope).await.is_err(), "[{label}] a truncated envelope must not decrypt");
    })
    .await;
}

#[tokio::test]
async fn a_data_key_is_not_transferable_between_master_keys() {
    // Two independent master keys on the same backend: a blob wrapped by one
    // must not open under the other, even with an identical context.
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    kms.create_key("wrapper-a").await;
    kms.create_key("wrapper-b").await;

    let from_a = manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "wrapper-a".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context(),
        })
        .await
        .expect("generate under wrapper-a");
    let from_b = manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "wrapper-b".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context(),
        })
        .await
        .expect("generate under wrapper-b");

    assert_ne!(
        from_a.plaintext_key, from_b.plaintext_key,
        "different master keys must produce different data keys"
    );

    // The envelope names its own master key, so each opens under its own.
    for (label, dek) in [("wrapper-a", &from_a), ("wrapper-b", &from_b)] {
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: dek.ciphertext_blob.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("{label} blob should decrypt under its own key: {error:?}"));
        assert_eq!(&decrypted.plaintext, &dek.plaintext_key);
    }

    // Deleting wrapper-a makes its blobs undecryptable while wrapper-b's keep
    // working — the blobs are genuinely bound to distinct material.
    manager
        .delete_key(rustfs_kms::DeleteKeyRequest {
            key_id: "wrapper-a".to_string(),
            pending_window_in_days: None,
            force_immediate: Some(true),
        })
        .await
        .expect("forced deletion should succeed");

    assert!(
        manager
            .decrypt(DecryptRequest {
                ciphertext: from_a.ciphertext_blob.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .is_err(),
        "a blob wrapped by a destroyed key must not decrypt"
    );
    manager
        .decrypt(DecryptRequest {
            ciphertext: from_b.ciphertext_blob.clone(),
            encryption_context: context(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("an unrelated key's blobs must be unaffected by the deletion");
}

#[tokio::test]
async fn data_key_envelope_detection_matches_produced_blobs() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();

        let dek = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: case.key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] generate should succeed: {error:?}"));

        assert!(
            is_data_key_envelope(&dek.ciphertext_blob),
            "[{label}] a freshly wrapped data key must be recognised as an envelope"
        );

        for (name, input) in [
            ("empty", Vec::new()),
            ("raw bytes", vec![0x00, 0x01, 0x02]),
            ("plain text", b"not an envelope".to_vec()),
            ("unrelated json", br#"{"unrelated":true}"#.to_vec()),
            ("json array", b"[]".to_vec()),
        ] {
            assert!(
                !is_data_key_envelope(&input),
                "[{label}] {name} must not be mistaken for a data key envelope"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn object_data_keys_are_bound_to_their_object() {
    // `ObjectEncryptionService::create_data_key` derives the encryption context
    // from bucket + object key, so a DEK minted for one object must not open
    // under another object's context.
    let kms = TestKms::local().await;
    let service = kms.service().await;
    kms.create_key("object-binding").await;
    let key_id = Some("object-binding".to_string());

    let alpha = ObjectEncryptionContext::new("bucket-x".to_string(), "alpha.bin".to_string());
    let beta = ObjectEncryptionContext::new("bucket-x".to_string(), "beta.bin".to_string());

    let (alpha_key, alpha_blob) = service
        .create_data_key(&key_id, &alpha)
        .await
        .expect("create_data_key for alpha");
    let (beta_key, beta_blob) = service
        .create_data_key(&key_id, &beta)
        .await
        .expect("create_data_key for beta");

    assert_ne!(alpha_key.plaintext_key, beta_key.plaintext_key, "each object gets its own data key");
    assert_ne!(alpha_blob, beta_blob, "each object gets its own wrapped data key");
    assert_ne!(alpha_key.nonce, beta_key.nonce, "each object gets its own base nonce for streaming");

    let recovered = service
        .decrypt_data_key(&alpha_blob, &alpha)
        .await
        .expect("alpha's blob must open under alpha's context");
    assert_eq!(
        recovered.plaintext_key, alpha_key.plaintext_key,
        "the recovered data key must match the one handed out"
    );

    assert_context_mismatch(service.decrypt_data_key(&alpha_blob, &beta).await);
    assert_context_mismatch(service.decrypt_data_key(&beta_blob, &alpha).await);

    // A different bucket with the same object name is also a different object.
    let other_bucket = ObjectEncryptionContext::new("bucket-y".to_string(), "alpha.bin".to_string());
    assert_context_mismatch(service.decrypt_data_key(&alpha_blob, &other_bucket).await);

    // The legacy path intentionally drops the context; it is the only way to
    // read objects written before per-object binding existed.
    let legacy = service
        .decrypt_legacy_data_key(&alpha_blob)
        .await
        .expect("the legacy path must still open the blob");
    assert_eq!(legacy.plaintext_key, alpha_key.plaintext_key);
    assert_eq!(
        legacy.nonce, [0u8; 12],
        "the legacy path returns a zero nonce; callers substitute the stored one"
    );
}

#[tokio::test]
async fn create_data_key_requires_a_resolvable_key_id() {
    let kms = TestKms::local().await;
    let service = kms.service().await;
    let context = ObjectEncryptionContext::new("bucket".to_string(), "object".to_string());

    // No explicit id and no configured default: a configuration error, not a
    // silent fallback to some arbitrary key.
    match service.create_data_key(&None, &context).await {
        Err(KmsError::ConfigurationError { message }) => {
            assert!(message.contains("No KMS key ID"), "should explain the missing key id: {message}")
        }
        other => panic!("expected ConfigurationError, got {other:?}"),
    }

    // With a default configured, the same call resolves to it.
    let kms = TestKms::local_with(|config| config.default_key_id = Some("default-key".to_string())).await;
    let service = kms.service().await;
    kms.create_key("default-key").await;
    assert_eq!(
        service.get_default_key_id().map(String::as_str),
        Some("default-key"),
        "the configured default must be visible to callers"
    );
    let (_key, blob) = service
        .create_data_key(&None, &context)
        .await
        .expect("the default key must be used when none is given");
    service
        .decrypt_data_key(&blob, &context)
        .await
        .expect("the default key's blob must round-trip");
}
