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

//! Black-box behavior: master key lifecycle through `KmsManager`.
//!
//! The state × operation matrix is the load-bearing part. RustFS deliberately
//! deviates from AWS KMS in one direction: **decryption stays available while a
//! key is Disabled or PendingDeletion**, because refusing it would make every
//! object encrypted under that key unreadable the instant an operator disables
//! it. The rest of the matrix is:
//!
//! | state           | encrypt / generate DEK | enable / disable | schedule deletion | cancel deletion | decrypt |
//! |-----------------|------------------------|------------------|-------------------|-----------------|---------|
//! | Enabled         | allowed                | allowed          | allowed           | rejected        | allowed |
//! | Disabled        | rejected               | allowed          | allowed           | rejected        | allowed |
//! | PendingDeletion | rejected               | rejected         | rejected          | allowed         | allowed |
//!
//! `crates/kms/src/backends/contract_tests.rs` pins the same matrix at the
//! backend trait; this file pins it one layer up, where the metadata cache and
//! the manager's invalidation logic also participate — a cache that served a
//! stale `Enabled` snapshot would break the gate without the backend noticing.
//!
//! Not covered here on purpose: tag / description mutation. Those types are
//! re-exported by this crate but their only entry point lives in the admin
//! handlers, outside this crate's public surface.

mod common;

use common::{
    BackendCase, BackendKind, TestKms, assert_invalid_operation, assert_key_already_exists, assert_key_not_found,
    assert_unsupported_capability, ctx, for_each_backend, without_probe_key,
};
use rustfs_kms::{
    CancelKeyDeletionRequest, CreateKeyRequest, DecryptRequest, DeleteKeyRequest, DescribeKeyRequest, EncryptRequest,
    GenerateDataKeyRequest, KeySpec, KeyState, KeyStatus, KeyUsage, KmsManager, ListKeysRequest,
};

async fn describe_state(kms: &KmsManager, key_id: &str) -> KeyState {
    kms.describe_key(DescribeKeyRequest {
        key_id: key_id.to_string(),
    })
    .await
    .expect("describe should succeed")
    .key_metadata
    .key_state
}

fn generate_request(key_id: &str) -> GenerateDataKeyRequest {
    GenerateDataKeyRequest {
        key_id: key_id.to_string(),
        key_spec: KeySpec::Aes256,
        encryption_context: ctx(&[("bucket", "keys-behavior")]),
    }
}

fn encrypt_request(key_id: &str) -> EncryptRequest {
    EncryptRequest {
        key_id: key_id.to_string(),
        plaintext: b"state-gated plaintext".to_vec(),
        encryption_context: ctx(&[("bucket", "keys-behavior")]),
        grant_tokens: Vec::new(),
    }
}

#[tokio::test]
async fn created_key_is_enabled_and_fully_described() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;

    let created = manager
        .create_key(CreateKeyRequest {
            key_name: Some("described-key".to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("a described key".to_string()),
            ..Default::default()
        })
        .await
        .expect("create should succeed");

    assert_eq!(created.key_id, "described-key", "an explicit key name becomes the key id");
    assert_eq!(created.key_metadata.key_id, created.key_id, "metadata must agree with the id");
    assert_eq!(created.key_metadata.key_state, KeyState::Enabled, "a new key is immediately usable");
    assert_eq!(created.key_metadata.key_usage, KeyUsage::EncryptDecrypt);
    assert!(created.key_metadata.deletion_date.is_none(), "a new key has no deletion deadline");

    let described = manager
        .describe_key(DescribeKeyRequest {
            key_id: created.key_id.clone(),
        })
        .await
        .expect("describe should succeed")
        .key_metadata;
    assert_eq!(described.key_id, created.key_id);
    assert_eq!(described.key_state, KeyState::Enabled);
    assert_eq!(
        described.description, created.key_metadata.description,
        "describe must return the description supplied at creation"
    );
    assert_eq!(
        described.creation_date, created.key_metadata.creation_date,
        "the creation timestamp is stable across reads"
    );
}

#[tokio::test]
async fn auto_generated_key_ids_are_unique() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;

    let first = manager
        .create_key(CreateKeyRequest::default())
        .await
        .expect("first auto-named key");
    let second = manager
        .create_key(CreateKeyRequest::default())
        .await
        .expect("second auto-named key");

    assert!(!first.key_id.is_empty(), "an auto-generated key id must not be empty");
    assert_ne!(first.key_id, second.key_id, "auto-generated key ids must not collide");
    for created in [&first, &second] {
        assert_eq!(
            describe_state(&manager, &created.key_id).await,
            KeyState::Enabled,
            "auto-named keys are Enabled like named ones"
        );
    }
}

#[tokio::test]
async fn duplicate_key_name_is_rejected_without_disturbing_the_original() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    kms.create_key("duplicate-me").await;

    assert_key_already_exists(
        manager
            .create_key(CreateKeyRequest {
                key_name: Some("duplicate-me".to_string()),
                description: Some("an impostor".to_string()),
                ..Default::default()
            })
            .await,
        "duplicate-me",
    );

    // The rejected create must not have overwritten the original's material:
    // a DEK generated before the conflict still decrypts afterwards.
    let context = ctx(&[("bucket", "duplicate")]);
    let dek = manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "duplicate-me".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("data key generation should still work");
    let decrypted = manager
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob,
            encryption_context: context,
            grant_tokens: Vec::new(),
        })
        .await
        .expect("the original key material must be intact");
    assert_eq!(decrypted.plaintext, dek.plaintext_key, "round-trip after a rejected create");
}

#[tokio::test]
async fn describing_an_unknown_key_reports_key_not_found() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        assert_key_not_found(
            manager
                .describe_key(DescribeKeyRequest {
                    key_id: "no-such-key".to_string(),
                })
                .await,
            "no-such-key",
        );
        assert_key_not_found(manager.generate_data_key(generate_request("no-such-key")).await, "no-such-key");
        assert_key_not_found(manager.encrypt(encrypt_request("no-such-key")).await, "no-such-key");
    })
    .await;
}

#[tokio::test]
async fn list_keys_reports_created_keys_and_honours_filters() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    for name in ["list-a", "list-b", "list-c"] {
        kms.create_key(name).await;
    }

    let all = manager
        .list_keys(ListKeysRequest::default())
        .await
        .expect("list should succeed");
    let mut ids = without_probe_key(all.keys.iter().map(|key| key.key_id.clone()));
    ids.sort();
    assert_eq!(ids, vec!["list-a", "list-b", "list-c"], "every created key must be listed");

    // `limit` caps the page, and a capped page must say so. A client that
    // paginates by looking at `truncated` would otherwise stop after the first
    // page and silently act on a partial key list — for a KMS, that means
    // believing keys do not exist when they do.
    let limited = manager
        .list_keys(ListKeysRequest {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .expect("limited list should succeed");
    assert_eq!(limited.keys.len(), 2, "limit must cap the returned page");
    assert!(
        limited.truncated,
        "a page that was cut short by `limit` must be reported as truncated; 3 keys exist and only 2 were returned"
    );
    assert!(
        limited.next_marker.is_some(),
        "a truncated page must carry a continuation marker so the caller can fetch the rest"
    );

    // A status filter narrows the result to keys in that state.
    manager.disable_key("list-b").await.expect("disable should succeed");
    let disabled = manager
        .list_keys(ListKeysRequest {
            status_filter: Some(KeyStatus::Disabled),
            ..Default::default()
        })
        .await
        .expect("filtered list should succeed");
    assert_eq!(
        disabled.keys.iter().map(|k| k.key_id.as_str()).collect::<Vec<_>>(),
        vec!["list-b"],
        "only the disabled key matches the Disabled filter"
    );

    let active = manager
        .list_keys(ListKeysRequest {
            status_filter: Some(KeyStatus::Active),
            ..Default::default()
        })
        .await
        .expect("filtered list should succeed");
    let mut active_ids = without_probe_key(active.keys.iter().map(|k| k.key_id.clone()));
    active_ids.sort();
    assert_eq!(active_ids, vec!["list-a", "list-c"], "the disabled key drops out of the Active filter");

    // A usage filter that matches nothing yields an empty page, not an error.
    let none = manager
        .list_keys(ListKeysRequest {
            usage_filter: Some(KeyUsage::SignVerify),
            ..Default::default()
        })
        .await
        .expect("non-matching filter should still succeed");
    assert!(none.keys.is_empty(), "a filter matching nothing returns an empty page");
}

#[tokio::test]
async fn disable_and_enable_round_trip_through_the_metadata_cache() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("toggle-me").await;

    // Warm the cache first: a stale cached Enabled snapshot would defeat the
    // Disabled gate below without the backend ever being consulted.
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);

    manager.disable_key(&key_id).await.expect("disable should succeed");
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::Disabled,
        "describe must observe the post-mutation state"
    );

    // Disabling again is idempotent, not an error.
    manager.disable_key(&key_id).await.expect("repeat disable is idempotent");
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Disabled);

    manager.enable_key(&key_id).await.expect("enable should succeed");
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);
    manager.enable_key(&key_id).await.expect("repeat enable is idempotent");
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);
}

#[tokio::test]
async fn scheduled_deletion_carries_a_deadline_and_can_be_cancelled() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("deletable").await;

    let scheduled = manager
        .delete_key(DeleteKeyRequest {
            key_id: key_id.clone(),
            pending_window_in_days: Some(7),
            force_immediate: None,
            confirm_key_id: None,
        })
        .await
        .expect("scheduling deletion should succeed");

    assert_eq!(scheduled.key_id, key_id);
    assert!(
        scheduled.deletion_date.is_some(),
        "a scheduled deletion must report when the key will actually go away"
    );
    assert_eq!(scheduled.key_metadata.key_state, KeyState::PendingDeletion);
    assert!(
        scheduled.key_metadata.deletion_date.is_some(),
        "the metadata must carry the same deadline"
    );
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::PendingDeletion,
        "the pending state must be visible to a subsequent describe"
    );

    let cancelled = manager
        .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
        .await
        .expect("cancelling should succeed");
    assert_eq!(cancelled.key_id, key_id);
    assert_eq!(cancelled.key_metadata.key_state, KeyState::Enabled, "cancelling restores an usable key");
    assert!(cancelled.key_metadata.deletion_date.is_none(), "cancelling must clear the deadline");
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);

    // The key really is usable again, not merely reported as such.
    manager
        .generate_data_key(generate_request(&key_id))
        .await
        .expect("a cancelled key must accept new cryptographic work");

    // Cancelling a key that is not pending deletion is a state error.
    assert_invalid_operation(
        manager
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await,
        "not pending deletion",
    );
}

#[tokio::test]
async fn deletion_pending_window_is_bounded() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;

    for (name, days) in [("window-too-short", 6u32), ("window-too-long", 31)] {
        let key_id = kms.create_key(name).await;
        assert_invalid_operation(
            manager
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    pending_window_in_days: Some(days),
                    force_immediate: None,
                    confirm_key_id: None,
                })
                .await,
            "between 7 and 30",
        );
        assert_eq!(
            describe_state(&manager, &key_id).await,
            KeyState::Enabled,
            "a rejected deletion window must leave the key untouched"
        );
    }

    // The documented bounds themselves are accepted.
    for (name, days) in [("window-min", 7u32), ("window-max", 30)] {
        let key_id = kms.create_key(name).await;
        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(days),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await
            .unwrap_or_else(|error| panic!("{days} days must be accepted: {error:?}"));
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::PendingDeletion);
    }
}

#[tokio::test]
async fn forced_immediate_deletion_removes_the_key() {
    let kms = TestKms::local_with(|config| config.allow_immediate_deletion = true).await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("burn-now").await;

    let deleted = manager
        .delete_key(DeleteKeyRequest {
            key_id: key_id.clone(),
            pending_window_in_days: None,
            force_immediate: Some(true),
            confirm_key_id: Some(key_id.clone()),
        })
        .await
        .expect("forced deletion should succeed");
    assert!(deleted.deletion_date.is_none(), "an immediate deletion has no future deadline to report");

    assert_key_not_found(manager.describe_key(DescribeKeyRequest { key_id: key_id.clone() }).await, &key_id);
    assert_key_not_found(manager.generate_data_key(generate_request(&key_id)).await, &key_id);
    assert!(
        !manager
            .list_keys(ListKeysRequest::default())
            .await
            .expect("list should succeed")
            .keys
            .iter()
            .any(|key| key.key_id == key_id),
        "a physically deleted key must disappear from listings"
    );

    // The name is free again, and the replacement is a genuinely new key.
    let recreated = kms.create_key(&key_id).await;
    assert_eq!(describe_state(&manager, &recreated).await, KeyState::Enabled);
}

/// The full state × operation matrix, run against every offline backend.
///
/// Backends that cannot reach a state (the static backend has no lifecycle at
/// all) assert the refusal instead — the capability flags are a two-way
/// contract, not just an advertisement.
#[tokio::test]
async fn key_state_gates_every_operation() {
    for_each_backend(|case: BackendCase| async move {
        let manager = case.kms.kms().await;
        let caps = case.caps().await;
        let key_id = case.key_id.clone();

        // --- Enabled: everything is permitted -----------------------------
        assert_eq!(
            describe_state(&manager, &key_id).await,
            KeyState::Enabled,
            "[{}] the seeded key starts Enabled",
            case.kind().name()
        );
        let enabled_dek = manager
            .generate_data_key(generate_request(&key_id))
            .await
            .expect("Enabled must permit data key generation");
        manager
            .encrypt(encrypt_request(&key_id))
            .await
            .expect("Enabled must permit encryption");

        // Rotation is capability-gated even in the Enabled state.
        if !caps.rotate {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
        }

        if !caps.enable_disable {
            assert_unsupported_capability(manager.disable_key(&key_id).await, "disable_key");
            assert_unsupported_capability(manager.enable_key(&key_id).await, "enable_key");
        }
        if !caps.schedule_deletion {
            // A read-only backend refuses deletion outright rather than
            // pretending to schedule one.
            assert!(
                manager
                    .delete_key(DeleteKeyRequest {
                        key_id: key_id.clone(),
                        pending_window_in_days: Some(7),
                        force_immediate: None,
                        confirm_key_id: None,
                    })
                    .await
                    .is_err(),
                "[{}] a backend without deletion support must refuse delete_key",
                case.kind().name()
            );
        }

        if case.kind() == BackendKind::Static {
            // No further states are reachable on a read-only backend; the
            // decrypt-still-works half of the matrix is checked below instead.
            let decrypted = manager
                .decrypt(DecryptRequest {
                    ciphertext: enabled_dek.ciphertext_blob.clone(),
                    encryption_context: ctx(&[("bucket", "keys-behavior")]),
                    grant_tokens: Vec::new(),
                })
                .await
                .expect("static backend must decrypt its own envelope");
            assert_eq!(decrypted.plaintext, enabled_dek.plaintext_key);
            return;
        }

        // --- Disabled: no new crypto, but reads and lifecycle recovery ----
        manager.disable_key(&key_id).await.expect("disable should succeed");
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Disabled);

        assert_invalid_operation(manager.generate_data_key(generate_request(&key_id)).await, "is disabled");
        assert_invalid_operation(manager.encrypt(encrypt_request(&key_id)).await, "is disabled");
        if caps.rotate {
            assert_invalid_operation(manager.rotate_key(&key_id).await, "is disabled");
        } else {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
        }

        // The deliberate deviation from AWS KMS: data written before the key
        // was disabled must stay readable.
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: enabled_dek.ciphertext_blob.clone(),
                encryption_context: ctx(&[("bucket", "keys-behavior")]),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("a Disabled key must still decrypt existing ciphertext");
        assert_eq!(
            decrypted.plaintext, enabled_dek.plaintext_key,
            "decryption under a Disabled key must return the original data key"
        );

        // Disabled still permits enabling, disabling, and scheduling deletion.
        manager
            .disable_key(&key_id)
            .await
            .expect("disable is idempotent while Disabled");
        manager.enable_key(&key_id).await.expect("Disabled must permit re-enabling");
        manager
            .disable_key(&key_id)
            .await
            .expect("back to Disabled for the next step");

        // --- PendingDeletion: only cancellation and decryption ------------
        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await
            .expect("Disabled must permit scheduling deletion");
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::PendingDeletion);

        assert_invalid_operation(manager.generate_data_key(generate_request(&key_id)).await, "pending deletion");
        assert_invalid_operation(manager.encrypt(encrypt_request(&key_id)).await, "pending deletion");
        assert_invalid_operation(manager.enable_key(&key_id).await, "pending deletion");
        assert_invalid_operation(manager.disable_key(&key_id).await, "pending deletion");
        assert_invalid_operation(
            manager
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    pending_window_in_days: Some(7),
                    force_immediate: None,
                    confirm_key_id: None,
                })
                .await,
            "pending deletion",
        );
        if caps.rotate {
            assert_invalid_operation(manager.rotate_key(&key_id).await, "pending deletion");
        } else {
            assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");
        }

        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: enabled_dek.ciphertext_blob.clone(),
                encryption_context: ctx(&[("bucket", "keys-behavior")]),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("a PendingDeletion key must still decrypt existing ciphertext");
        assert_eq!(decrypted.plaintext, enabled_dek.plaintext_key);

        // Cancellation is the one way out, and it restores full capability.
        manager
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await
            .expect("PendingDeletion must permit cancellation");
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);
        manager
            .generate_data_key(generate_request(&key_id))
            .await
            .expect("a cancelled key is fully usable again");
    })
    .await;
}

#[tokio::test]
async fn static_backend_refuses_every_lifecycle_mutation() {
    let kms = TestKms::static_backend().await;
    let manager = kms.kms().await;
    let caps = kms.capabilities().await;

    // The capability report is the contract; assert it explicitly so a backend
    // that silently gains a capability has to update this test.
    assert!(caps.encrypt && caps.decrypt && caps.generate_data_key, "static must do crypto");
    assert!(
        !caps.rotate && !caps.enable_disable && !caps.schedule_deletion && !caps.versioning && !caps.physical_delete,
        "static must advertise no lifecycle capability: {caps:?}"
    );

    assert_invalid_operation(
        manager
            .create_key(CreateKeyRequest {
                key_name: Some("another-key".to_string()),
                ..Default::default()
            })
            .await,
        "read-only",
    );
    // Re-creating the configured key is a conflict, not a generic refusal.
    assert_key_already_exists(
        manager
            .create_key(CreateKeyRequest {
                key_name: Some(kms.config().static_config().expect("static config").key_id.clone()),
                ..Default::default()
            })
            .await,
        &kms.config().static_config().expect("static config").key_id,
    );

    let key_id = kms.config().static_config().expect("static config").key_id.clone();
    assert_invalid_operation(
        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await,
        "read-only",
    );
    assert_invalid_operation(
        manager
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await,
        "read-only",
    );
    assert_unsupported_capability(manager.enable_key(&key_id).await, "enable_key");
    assert_unsupported_capability(manager.disable_key(&key_id).await, "disable_key");
    assert_unsupported_capability(manager.rotate_key(&key_id).await, "rotate_key");

    // Operations aimed at any other key id are "not found", not "read-only":
    // the distinction matters to the admin API's status mapping.
    assert_key_not_found(
        manager
            .delete_key(DeleteKeyRequest {
                key_id: "other".to_string(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await,
        "other",
    );
    assert_key_not_found(
        manager
            .cancel_key_deletion(CancelKeyDeletionRequest {
                key_id: "other".to_string(),
            })
            .await,
        "other",
    );
    assert_key_not_found(
        manager
            .describe_key(DescribeKeyRequest {
                key_id: "other".to_string(),
            })
            .await,
        "other",
    );

    // Despite refusing every mutation, it must still do its actual job.
    let dek = manager
        .generate_data_key(generate_request(&key_id))
        .await
        .expect("static backend must generate data keys");
    assert_eq!(dek.plaintext_key.len(), 32, "AES-256 data key is 32 bytes");
    let listed = manager
        .list_keys(ListKeysRequest::default())
        .await
        .expect("list should succeed");
    assert_eq!(
        listed.keys.iter().map(|k| k.key_id.as_str()).collect::<Vec<_>>(),
        vec![key_id.as_str()],
        "the static backend lists exactly its one configured key"
    );
}
