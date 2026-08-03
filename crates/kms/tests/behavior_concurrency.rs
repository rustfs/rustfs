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

//! Black-box behavior: concurrency and durability across a restart.
//!
//! Two things a single-threaded test can never show:
//!
//! * **Concurrency.** Lifecycle operations are serialized behind one lock, and
//!   key operations run in parallel against a shared on-disk store. Under load
//!   the guarantees that matter are that a race has exactly one winner, that no
//!   interleaving produces a panic, and that parallel data-key generation never
//!   collides.
//! * **Durability.** Everything the KMS promises is worthless if a restart
//!   loses it. The restart cases below drop the whole service and bring a
//!   brand-new manager up over the same directory, so anything that still holds
//!   afterwards genuinely came off disk rather than out of a warm cache.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use common::{TestKms, assert_invalid_operation, ctx};
use rustfs_kms::{
    CreateKeyRequest, DecryptRequest, DeleteKeyRequest, DescribeKeyRequest, GenerateDataKeyRequest, KeySpec, KeyState, KmsConfig,
    KmsError, KmsServiceManager, KmsServiceStatus,
};
use tempfile::TempDir;

fn context() -> std::collections::HashMap<String, String> {
    ctx(&[("bucket", "concurrency-behavior")])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reconfiguration_is_serialized_and_versions_stay_monotonic() {
    let dir = TempDir::new().expect("temp dir");
    let manager = Arc::new(KmsServiceManager::new());
    let config = KmsConfig::local(dir.path().to_path_buf()).with_insecure_development_defaults();
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");
    assert_eq!(manager.get_service_version().await, Some(1));

    const RECONFIGURATIONS: u64 = 8;
    let mut handles = Vec::new();
    for index in 0..RECONFIGURATIONS {
        let manager = manager.clone();
        let mut candidate = config.clone();
        // Vary a field that is allowed to change so each call does real work.
        candidate.timeout = Duration::from_secs(30 + index);
        handles.push(tokio::spawn(async move { manager.reconfigure(candidate).await }));
    }

    for (index, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .expect("reconfigure task must not panic")
            .unwrap_or_else(|error| panic!("reconfigure {index} should succeed: {error:?}"));
    }

    assert_eq!(
        manager.get_service_version().await,
        Some(1 + RECONFIGURATIONS),
        "each serialized reconfigure must consume exactly one version"
    );
    assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    assert!(manager.health_check().await.expect("health check"), "the survivor must be healthy");

    // Exactly one candidate won, and the published config is one of the ones
    // that was actually submitted.
    let published = manager.get_config().await.expect("config").timeout;
    assert!(
        (30..30 + RECONFIGURATIONS).contains(&published.as_secs()),
        "the published timeout must be one of the submitted candidates, got {published:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_race_to_create_the_same_key_has_exactly_one_winner() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;

    const RACERS: usize = 8;
    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let manager = manager.clone();
        handles.push(tokio::spawn(async move {
            manager
                .create_key(CreateKeyRequest {
                    key_name: Some("contested".to_string()),
                    ..Default::default()
                })
                .await
        }));
    }

    let mut winners = 0;
    let mut conflicts = 0;
    for handle in handles {
        match handle.await.expect("create task must not panic") {
            Ok(response) => {
                assert_eq!(response.key_id, "contested");
                winners += 1;
            }
            Err(KmsError::KeyAlreadyExists { key_id }) => {
                assert_eq!(key_id, "contested");
                conflicts += 1;
            }
            Err(other) => panic!("a create race must resolve to success or KeyAlreadyExists, got {other:?}"),
        }
    }
    assert_eq!(winners, 1, "exactly one racer may create the key");
    assert_eq!(conflicts, RACERS - 1, "every other racer must see a conflict");

    // The single surviving key is intact and usable.
    let described = manager
        .describe_key(DescribeKeyRequest {
            key_id: "contested".to_string(),
        })
        .await
        .expect("the contested key must exist");
    assert_eq!(described.key_metadata.key_state, KeyState::Enabled);
    let dek = manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "contested".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context(),
        })
        .await
        .expect("the contested key must work");
    manager
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob,
            encryption_context: context(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("the contested key's material must be coherent, not a torn write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_data_key_generation_never_collides() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("parallel-dek").await;

    const TASKS: usize = 16;
    const PER_TASK: usize = 8;
    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let manager = manager.clone();
        let key_id = key_id.clone();
        handles.push(tokio::spawn(async move {
            let mut produced = Vec::new();
            for _ in 0..PER_TASK {
                let dek = manager
                    .generate_data_key(GenerateDataKeyRequest {
                        key_id: key_id.clone(),
                        key_spec: KeySpec::Aes256,
                        encryption_context: context(),
                    })
                    .await
                    .expect("generate should succeed under load");
                produced.push((dek.plaintext_key, dek.ciphertext_blob));
            }
            produced
        }));
    }

    let mut plaintexts = HashSet::new();
    let mut ciphertexts = HashSet::new();
    let mut all = Vec::new();
    for handle in handles {
        for (plaintext, ciphertext) in handle.await.expect("generation task must not panic") {
            assert!(plaintexts.insert(plaintext.clone()), "a data key was handed out twice under load");
            assert!(
                ciphertexts.insert(ciphertext.clone()),
                "a wrapped data key was handed out twice under load"
            );
            all.push((plaintext, ciphertext));
        }
    }
    assert_eq!(all.len(), TASKS * PER_TASK, "every request must be answered");

    // Every blob still opens to its own key: concurrency must not have crossed
    // wires between requests.
    for (index, (expected, blob)) in all.into_iter().enumerate() {
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: blob,
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("blob {index} should decrypt: {error:?}"));
        assert_eq!(decrypted.plaintext, expected, "blob {index} opened to another request's key");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_churn_against_live_traffic_stays_coherent() {
    // A disable/enable loop running against concurrent data-key generation.
    // Each generation must either succeed outright or be refused by the state
    // gate — never panic, never return a broken key, never see a torn record.
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("churned").await;

    let churn = {
        let manager = manager.clone();
        let key_id = key_id.clone();
        tokio::spawn(async move {
            for _ in 0..20 {
                manager.disable_key(&key_id).await.expect("disable should succeed");
                tokio::task::yield_now().await;
                manager.enable_key(&key_id).await.expect("enable should succeed");
                tokio::task::yield_now().await;
            }
        })
    };

    let mut workers = Vec::new();
    for _ in 0..4 {
        let manager = manager.clone();
        let key_id = key_id.clone();
        workers.push(tokio::spawn(async move {
            let mut succeeded = 0usize;
            let mut refused = 0usize;
            for _ in 0..40 {
                match manager
                    .generate_data_key(GenerateDataKeyRequest {
                        key_id: key_id.clone(),
                        key_spec: KeySpec::Aes256,
                        encryption_context: context(),
                    })
                    .await
                {
                    Ok(dek) => {
                        assert_eq!(dek.plaintext_key.len(), 32, "a key handed out under churn must be well formed");
                        // A key produced while the master key was enabled must
                        // remain decryptable regardless of later state changes.
                        let decrypted = manager
                            .decrypt(DecryptRequest {
                                ciphertext: dek.ciphertext_blob,
                                encryption_context: context(),
                                grant_tokens: Vec::new(),
                            })
                            .await
                            .expect("a key issued under churn must stay decryptable");
                        assert_eq!(decrypted.plaintext, dek.plaintext_key);
                        succeeded += 1;
                    }
                    Err(KmsError::InvalidOperation { message }) => {
                        assert!(
                            message.contains("disabled"),
                            "the only acceptable refusal under this churn is the disabled gate, got {message:?}"
                        );
                        refused += 1;
                    }
                    Err(other) => panic!("unexpected error under lifecycle churn: {other:?}"),
                }
                tokio::task::yield_now().await;
            }
            (succeeded, refused)
        }));
    }

    churn.await.expect("churn task must not panic");
    let mut total = 0usize;
    for worker in workers {
        let (succeeded, refused) = worker.await.expect("worker task must not panic");
        total += succeeded + refused;
    }
    assert_eq!(total, 4 * 40, "every request must be accounted for");

    // The totals above say nothing about the state gate on their own: if the
    // disable/enable loop happens to fall entirely between request windows,
    // every request succeeds and the count still balances — and an
    // implementation that refused everything would balance too. Asserting
    // `refused > 0` on the concurrent phase would only trade that hole for a
    // scheduling-dependent flake, so both branches are pinned deterministically
    // here instead. Removing the state gate, or breaking progress in the
    // enabled state, now fails this test.
    let gated_request = || GenerateDataKeyRequest {
        key_id: key_id.clone(),
        key_spec: KeySpec::Aes256,
        encryption_context: context(),
    };

    manager.disable_key(&key_id).await.expect("disable for the gated check");
    assert_invalid_operation(manager.generate_data_key(gated_request()).await, "is disabled");

    manager.enable_key(&key_id).await.expect("enable for the gated check");
    let after_enable = manager
        .generate_data_key(gated_request())
        .await
        .expect("an enabled key must generate again after the churn");
    assert_eq!(after_enable.plaintext_key.len(), 32, "the post-churn key must be well formed");

    // The key survives the churn in a well-defined state.
    manager.enable_key(&key_id).await.expect("final enable");
    assert_eq!(
        manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe")
            .key_metadata
            .key_state,
        KeyState::Enabled
    );
}

#[tokio::test]
async fn a_reconfigure_mid_flight_does_not_orphan_in_progress_work() {
    let dir = TempDir::new().expect("temp dir");
    let manager = Arc::new(KmsServiceManager::new());
    let config = KmsConfig::local(dir.path().to_path_buf()).with_insecure_development_defaults();
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");

    let old_kms = manager.get_manager().await.expect("manager v1");
    old_kms
        .create_key(CreateKeyRequest {
            key_name: Some("spans-reconfigure".to_string()),
            ..Default::default()
        })
        .await
        .expect("create");

    // A caller that grabbed the handle before the swap keeps working with it.
    let dek = old_kms
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "spans-reconfigure".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context(),
        })
        .await
        .expect("generate on the old generation");

    let mut next = config.clone();
    next.timeout = Duration::from_secs(42);
    manager.reconfigure(next).await.expect("reconfigure");
    let new_kms = manager.get_manager().await.expect("manager v2");
    assert!(!Arc::ptr_eq(&old_kms, &new_kms), "the reconfigure must have swapped the handle");

    // The old handle finishes its work...
    let via_old = old_kms
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob.clone(),
            encryption_context: context(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("the pre-swap handle must complete its in-flight work");
    assert_eq!(via_old.plaintext, dek.plaintext_key);

    // ...and the new handle can read what the old one wrote, because both are
    // backed by the same key directory.
    let via_new = new_kms
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob.clone(),
            encryption_context: context(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("the post-swap handle must read the old generation's output");
    assert_eq!(via_new.plaintext, dek.plaintext_key);
    new_kms
        .describe_key(DescribeKeyRequest {
            key_id: "spans-reconfigure".to_string(),
        })
        .await
        .expect("a key created before the swap must be visible after it");
}

/// The durability case: several keys in different states, a full restart, and
/// then every promise re-checked against the new process.
#[tokio::test]
async fn key_states_and_ciphertext_survive_a_restart() {
    let mut kms = TestKms::local().await;
    let manager = kms.kms().await;

    for key_id in ["survivor-enabled", "survivor-disabled", "survivor-pending"] {
        kms.create_key(key_id).await;
    }

    // Mint ciphertext under each key *before* the restart, so the assertions
    // afterwards prove the material itself survived, not just the metadata.
    let mut blobs = Vec::new();
    for key_id in ["survivor-enabled", "survivor-disabled", "survivor-pending"] {
        let dek = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await
            .expect("generate before restart");
        blobs.push((key_id, dek.plaintext_key, dek.ciphertext_blob));
    }

    manager.disable_key("survivor-disabled").await.expect("disable");
    manager
        .delete_key(DeleteKeyRequest {
            key_id: "survivor-pending".to_string(),
            pending_window_in_days: Some(7),
            force_immediate: None,
            confirm_key_id: None,
        })
        .await
        .expect("schedule deletion");

    // --- restart ---------------------------------------------------------
    kms.restart().await;
    let manager = kms.kms().await;

    // Every state came back exactly as it was left.
    for (key_id, expected) in [
        ("survivor-enabled", KeyState::Enabled),
        ("survivor-disabled", KeyState::Disabled),
        ("survivor-pending", KeyState::PendingDeletion),
    ] {
        let described = manager
            .describe_key(DescribeKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .unwrap_or_else(|error| panic!("{key_id} must survive the restart: {error:?}"))
            .key_metadata;
        assert_eq!(described.key_state, expected, "{key_id} must come back in its persisted state");
        if expected == KeyState::PendingDeletion {
            assert!(described.deletion_date.is_some(), "{key_id} must come back with its deadline intact");
        }
    }

    // Ciphertext written before the restart still opens under every state,
    // including the disabled and pending-deletion keys.
    for (key_id, expected_plaintext, blob) in &blobs {
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: blob.clone(),
                encryption_context: context(),
                grant_tokens: Vec::new(),
            })
            .await
            .unwrap_or_else(|error| panic!("{key_id}'s pre-restart ciphertext must still decrypt: {error:?}"));
        assert_eq!(&decrypted.plaintext, expected_plaintext, "{key_id} decrypted to the wrong key");
    }

    // The state gate is re-applied from persisted state, not re-derived as
    // Enabled: a disabled key must still refuse new work after a restart.
    assert_invalid_operation(
        manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: "survivor-disabled".to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await,
        "is disabled",
    );
    assert_invalid_operation(
        manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: "survivor-pending".to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await,
        "pending deletion",
    );
    manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "survivor-enabled".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context(),
        })
        .await
        .expect("the enabled key must accept new work after a restart");

    // Recovery still works across the restart boundary.
    manager
        .enable_key("survivor-disabled")
        .await
        .expect("re-enable after restart");
    manager
        .cancel_key_deletion(rustfs_kms::CancelKeyDeletionRequest {
            key_id: "survivor-pending".to_string(),
        })
        .await
        .expect("cancel after restart");
    for key_id in ["survivor-disabled", "survivor-pending"] {
        manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: context(),
            })
            .await
            .unwrap_or_else(|error| panic!("{key_id} must be usable after recovery: {error:?}"));
    }
}

#[tokio::test]
async fn a_destroyed_key_stays_destroyed_across_a_restart() {
    let mut kms = TestKms::local_with(|config| config.allow_immediate_deletion = true).await;
    let manager = kms.kms().await;
    kms.create_key("gone-for-good").await;
    kms.create_key("kept").await;

    manager
        .delete_key(DeleteKeyRequest {
            key_id: "gone-for-good".to_string(),
            pending_window_in_days: None,
            force_immediate: Some(true),
            confirm_key_id: Some("gone-for-good".to_string()),
        })
        .await
        .expect("forced deletion");

    kms.restart().await;
    let manager = kms.kms().await;

    assert!(
        manager
            .describe_key(DescribeKeyRequest {
                key_id: "gone-for-good".to_string(),
            })
            .await
            .is_err(),
        "a destroyed key must not reappear after a restart"
    );
    assert!(
        !manager
            .list_keys(rustfs_kms::ListKeysRequest::default())
            .await
            .expect("list")
            .keys
            .iter()
            .any(|key| key.key_id == "gone-for-good"),
        "a destroyed key must not reappear in listings after a restart"
    );
    manager
        .describe_key(DescribeKeyRequest {
            key_id: "kept".to_string(),
        })
        .await
        .expect("the untouched key must survive the restart");
}
