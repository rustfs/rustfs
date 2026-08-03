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

//! Black-box behavior: key rotation and the version history it must preserve.
//!
//! `BackendCapabilities::rotate` is documented as "rotation that retains prior
//! versions for decryption", and `versioning` as "multiple key versions
//! addressable after rotation". Those are the two claims this file exists to
//! hold, because breaking them destroys data silently: a rotation that dropped
//! the outgoing version would leave every object sealed before it permanently
//! unreadable, while every rotation itself still reported success.
//!
//! Only the Vault backends advertise these capabilities, so this file is the
//! working side of a contract the rest of the suite only ever sees refused.
//! Without the Vault lane on (`RUSTFS_KMS_VAULT_TOKEN`) these specs still run,
//! but they only assert the `UnsupportedCapability` half — see `common`.

mod common;

use common::{BackendCase, assert_unsupported_capability, ctx, for_each_backend, payload};
use rustfs_kms::{DecryptRequest, EncryptRequest, GenerateDataKeyRequest, KeySpec};

fn context() -> std::collections::HashMap<String, String> {
    ctx(&[("bucket", "rotation-behavior"), ("object", "alpha.bin")])
}

fn generate_request(key_id: &str) -> GenerateDataKeyRequest {
    GenerateDataKeyRequest {
        key_id: key_id.to_string(),
        key_spec: KeySpec::Aes256,
        encryption_context: context(),
    }
}

/// The core promise: material sealed before a rotation still opens after it.
///
/// This is the assertion that a "rotation" which merely overwrote the key
/// would fail. Everything else about rotation is recoverable; this is not.
#[tokio::test]
async fn ciphertext_from_before_a_rotation_still_decrypts_after_it() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        let before = manager
            .generate_data_key(generate_request(&key_id))
            .await
            .unwrap_or_else(|error| panic!("[{label}] generate before rotation should succeed: {error:?}"));

        if !caps.rotate {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
            return;
        }

        manager
            .rotate_key(&key_id)
            .await
            .unwrap_or_else(|error| panic!("[{label}] a backend advertising rotate must rotate: {error:?}"));

        let reopened = manager
            .decrypt(DecryptRequest {
                ciphertext: before.ciphertext_blob.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("[{label}] rotation must retain the prior version; pre-rotation ciphertext failed to open: {error:?}")
            });

        assert_eq!(
            reopened.plaintext, before.plaintext_key,
            "[{label}] the pre-rotation data key must come back byte-identical"
        );
    })
    .await;
}

/// A rotation must not stop the key from being used going forward, and the
/// material it produces afterwards must be independent of the old version.
#[tokio::test]
async fn a_rotated_key_keeps_working_and_issues_fresh_material() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        if !caps.rotate {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
            return;
        }

        let before = manager
            .generate_data_key(generate_request(&key_id))
            .await
            .expect("generate before rotation should succeed");

        manager.rotate_key(&key_id).await.expect("rotate should succeed");

        let after = manager
            .generate_data_key(generate_request(&key_id))
            .await
            .unwrap_or_else(|error| panic!("[{label}] the key must still issue data keys after rotation: {error:?}"));

        assert_ne!(
            after.plaintext_key, before.plaintext_key,
            "[{label}] a data key issued after rotation must not repeat the earlier one"
        );
        assert_ne!(
            after.ciphertext_blob, before.ciphertext_blob,
            "[{label}] the wrapped blob must differ across a rotation"
        );

        // Both generations must be openable at the same time — this is what
        // `versioning` means in practice.
        for (name, dek) in [("pre-rotation", &before), ("post-rotation", &after)] {
            let opened = manager
                .decrypt(DecryptRequest {
                    ciphertext: dek.ciphertext_blob.clone(),
                    encryption_context: context(),
                    grant_tokens: Vec::new(),
                })
                .await
                .unwrap_or_else(|error| panic!("[{label}] the {name} data key must stay decryptable: {error:?}"));
            assert_eq!(opened.plaintext, dek.plaintext_key, "[{label}] {name} round-trip");
        }
    })
    .await;
}

/// Master-key encryption must survive a rotation on the same terms as data
/// keys: the ciphertext is what a caller stored, and it has to keep opening.
#[tokio::test]
async fn master_key_ciphertext_survives_a_rotation() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        if !caps.rotate {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
            return;
        }

        let plaintext = payload(512);
        let sealed = manager
            .encrypt(EncryptRequest {
                key_id: key_id.clone(),
                plaintext: plaintext.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("encrypt before rotation should succeed");

        manager.rotate_key(&key_id).await.expect("rotate should succeed");

        let opened = manager
            .decrypt(DecryptRequest {
                ciphertext: sealed.ciphertext.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("[{label}] pre-rotation ciphertext must open after rotation: {error:?}"));

        assert_eq!(opened.plaintext, plaintext, "[{label}] the plaintext must survive the rotation");
    })
    .await;
}

/// Repeated rotations must accumulate versions, not overwrite a single spare.
///
/// A backend that kept only "current and previous" would pass a single-rotation
/// test and still lose the oldest objects on the second rotation.
#[tokio::test]
async fn every_generation_survives_repeated_rotations() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        if !caps.rotate || !caps.versioning {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
            return;
        }

        let mut generations = Vec::new();
        for round in 0..3 {
            let dek = manager
                .generate_data_key(generate_request(&key_id))
                .await
                .unwrap_or_else(|error| panic!("[{label}] generate in round {round} should succeed: {error:?}"));
            generations.push(dek);
            manager
                .rotate_key(&key_id)
                .await
                .unwrap_or_else(|error| panic!("[{label}] rotation {round} should succeed: {error:?}"));
        }

        for (round, dek) in generations.iter().enumerate() {
            let opened = manager
                .decrypt(DecryptRequest {
                    ciphertext: dek.ciphertext_blob.clone(),
                    encryption_context: context(),
                    grant_tokens: Vec::new(),
                })
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "[{label}] the data key from round {round} was lost after {} rotations: {error:?}",
                        generations.len()
                    )
                });
            assert_eq!(
                opened.plaintext, dek.plaintext_key,
                "[{label}] round {round} must round-trip after every later rotation"
            );
        }
    })
    .await;
}

/// Version history must live in the backend, not in process memory.
#[tokio::test]
async fn rotation_history_survives_a_restart() {
    for_each_backend(|case: BackendCase| async move {
        let mut case = case;
        let label = case.kind().name();
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        {
            let manager = case.kms.kms().await;
            if !caps.rotate {
                assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
                return;
            }
        }

        let before = {
            let manager = case.kms.kms().await;
            let dek = manager
                .generate_data_key(generate_request(&key_id))
                .await
                .expect("generate before rotation should succeed");
            manager.rotate_key(&key_id).await.expect("rotate should succeed");
            dek
        };

        case.kms.restart().await;

        let manager = case.kms.kms().await;
        let opened = manager
            .decrypt(DecryptRequest {
                ciphertext: before.ciphertext_blob.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("[{label}] a pre-rotation key must still open after a restart — the version history must be durable: {error:?}")
            });

        assert_eq!(
            opened.plaintext, before.plaintext_key,
            "[{label}] the retained version must survive a restart intact"
        );
    })
    .await;
}

/// Rotating a key that does not exist must fail as a missing key, not be
/// silently treated as a no-op that a caller would read as success.
#[tokio::test]
async fn rotating_an_unknown_key_fails() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let label = case.kind().name();
        let caps = case.caps().await;

        let result = manager.rotate_key("rotation-no-such-key").await;
        if !caps.rotate {
            assert_unsupported_capability(result, "rotate_key");
            return;
        }
        assert!(result.is_err(), "[{label}] rotating a key that does not exist must not report success");
    })
    .await;
}
