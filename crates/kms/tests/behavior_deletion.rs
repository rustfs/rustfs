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

//! Black-box behavior: completing a scheduled key deletion.
//!
//! `KmsBackend::remove_expired_key` is the only operation in this crate that
//! destroys key material, and the background sweep may call it concurrently on
//! several nodes and again after a crash. Its documented contract is therefore
//! unusually strict, and each clause is asserted below:
//!
//! * the deadline is honoured — a key that is not yet due is never removed;
//! * a cancellation observed after the caller inspected the key **wins**, so a
//!   racing sweep reports `StateChanged` rather than destroying a key the
//!   operator just rescued;
//! * removal is idempotent — re-running it after a crash, or on a key that is
//!   already gone, succeeds rather than erroring;
//! * the deadline is persisted, so a restart does not reset the clock.
//!
//! The clock is a parameter of the operation, so every case here is
//! deterministic: nothing sleeps and nothing waits on wall-clock time.
//!
//! Backends without deletion support must report the capability gap instead of
//! silently doing nothing.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestKms, assert_key_not_found, assert_unsupported_capability, ctx};
use jiff::Zoned;
use rustfs_kms::backends::local::LocalKmsBackend;
use rustfs_kms::backends::static_kms::StaticKmsBackend;
use rustfs_kms::backends::{ExpiredKeyRemoval, KmsBackend};
use rustfs_kms::{
    CancelKeyDeletionRequest, CreateKeyRequest, DecryptRequest, DeleteKeyRequest, DescribeKeyRequest, GenerateDataKeyRequest,
    KeySpec, KeyState, KeyUsage, KmsConfig,
};
use tempfile::TempDir;

/// A backend built the way the service manager builds it, so the deletion
/// contract is exercised on the same object production uses.
async fn local_backend(dir: &TempDir) -> Arc<LocalKmsBackend> {
    let config = KmsConfig::local(dir.path().to_path_buf()).with_insecure_development_defaults();
    Arc::new(LocalKmsBackend::new(config).await.expect("local backend should build"))
}

async fn create(backend: &LocalKmsBackend, key_id: &str) {
    backend
        .create_key(CreateKeyRequest {
            key_name: Some(key_id.to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            ..Default::default()
        })
        .await
        .expect("key should be created");
}

async fn schedule(backend: &LocalKmsBackend, key_id: &str, days: u32) {
    backend
        .delete_key(DeleteKeyRequest {
            key_id: key_id.to_string(),
            pending_window_in_days: Some(days),
            force_immediate: None,
            confirm_key_id: None,
        })
        .await
        .expect("deletion should be scheduled");
}

fn days_from_now(days: u64) -> Zoned {
    Zoned::now() + Duration::from_secs(days * 86_400)
}

async fn exists(backend: &LocalKmsBackend, key_id: &str) -> bool {
    backend
        .describe_key(DescribeKeyRequest {
            key_id: key_id.to_string(),
        })
        .await
        .is_ok()
}

#[tokio::test]
async fn removal_waits_for_the_deadline() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    create(&backend, "not-yet-due").await;
    schedule(&backend, "not-yet-due", 30).await;

    // Right now, and at every point strictly inside the window, the key stays.
    for elapsed_days in [0u64, 1, 15, 29] {
        assert_eq!(
            backend
                .remove_expired_key("not-yet-due", &days_from_now(elapsed_days))
                .await
                .expect("the check itself must succeed"),
            ExpiredKeyRemoval::NotExpired,
            "a key {elapsed_days} days into a 30-day window is not due"
        );
        assert!(exists(&backend, "not-yet-due").await, "a key that is not due must survive");
    }

    // Past the deadline it goes.
    assert_eq!(
        backend
            .remove_expired_key("not-yet-due", &days_from_now(31))
            .await
            .expect("removal should succeed"),
        ExpiredKeyRemoval::Removed
    );
    assert!(!exists(&backend, "not-yet-due").await, "an expired key must be gone");
}

#[tokio::test]
async fn removal_is_idempotent_across_restarts_and_nodes() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    create(&backend, "idempotent").await;
    schedule(&backend, "idempotent", 7).await;

    let past_deadline = days_from_now(8);

    assert_eq!(
        backend
            .remove_expired_key("idempotent", &past_deadline)
            .await
            .expect("first removal"),
        ExpiredKeyRemoval::Removed
    );

    // A second sweep — another node, or the same node after a restart — must
    // report success rather than failing on the now-missing record.
    for attempt in 0..3 {
        assert_eq!(
            backend
                .remove_expired_key("idempotent", &past_deadline)
                .await
                .unwrap_or_else(|error| panic!("repeat removal {attempt} must succeed: {error:?}")),
            ExpiredKeyRemoval::Removed,
            "removing an already-removed key is a no-op success"
        );
    }

    // A key that never existed is treated the same way, so a stale sweep entry
    // cannot wedge the worker.
    assert_eq!(
        backend
            .remove_expired_key("never-existed", &past_deadline)
            .await
            .expect("unknown key must not error"),
        ExpiredKeyRemoval::Removed
    );
}

#[tokio::test]
async fn a_cancellation_beats_a_racing_sweep() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    create(&backend, "rescued").await;
    create(&backend, "doomed").await;
    schedule(&backend, "rescued", 7).await;
    schedule(&backend, "doomed", 7).await;

    // The sweep has already decided both keys are due; the operator cancels one
    // before the removal actually runs.
    backend
        .cancel_key_deletion(CancelKeyDeletionRequest {
            key_id: "rescued".to_string(),
        })
        .await
        .expect("cancel should succeed");

    let past_deadline = days_from_now(8);
    assert_eq!(
        backend
            .remove_expired_key("rescued", &past_deadline)
            .await
            .expect("the check must succeed"),
        ExpiredKeyRemoval::StateChanged,
        "a key rescued after inspection must report StateChanged, not be destroyed"
    );
    assert_eq!(
        backend
            .remove_expired_key("doomed", &past_deadline)
            .await
            .expect("removal should succeed"),
        ExpiredKeyRemoval::Removed
    );

    // The rescued key is not merely present: it is fully usable again.
    let described = backend
        .describe_key(DescribeKeyRequest {
            key_id: "rescued".to_string(),
        })
        .await
        .expect("the rescued key must survive");
    assert_eq!(described.key_metadata.key_state, KeyState::Enabled);
    assert!(described.key_metadata.deletion_date.is_none(), "the rescued key must carry no deadline");
    backend
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "rescued".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: ctx(&[("bucket", "deletion-behavior")]),
        })
        .await
        .expect("the rescued key must accept new work");

    assert!(!exists(&backend, "doomed").await, "the un-rescued key must be gone");
}

#[tokio::test]
async fn keys_that_are_not_pending_deletion_are_never_removed() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    create(&backend, "healthy").await;
    create(&backend, "disabled").await;
    backend.disable_key("disabled").await.expect("disable should succeed");

    let far_future = days_from_now(3_650);
    for key_id in ["healthy", "disabled"] {
        assert_eq!(
            backend
                .remove_expired_key(key_id, &far_future)
                .await
                .expect("the check must succeed"),
            ExpiredKeyRemoval::StateChanged,
            "{key_id} is not pending deletion, so no deadline can apply to it"
        );
        assert!(exists(&backend, key_id).await, "{key_id} must survive");
    }

    // Their states are untouched by the attempt.
    assert_eq!(
        backend
            .describe_key(DescribeKeyRequest {
                key_id: "healthy".to_string()
            })
            .await
            .expect("describe")
            .key_metadata
            .key_state,
        KeyState::Enabled
    );
    assert_eq!(
        backend
            .describe_key(DescribeKeyRequest {
                key_id: "disabled".to_string()
            })
            .await
            .expect("describe")
            .key_metadata
            .key_state,
        KeyState::Disabled
    );
}

#[tokio::test]
async fn the_deletion_deadline_survives_a_restart() {
    let dir = TempDir::new().expect("temp dir");

    // Schedule the deletion, then drop the backend entirely — a process restart.
    {
        let backend = local_backend(&dir).await;
        create(&backend, "scheduled-before-restart").await;
        schedule(&backend, "scheduled-before-restart", 7).await;
    }

    let backend = local_backend(&dir).await;
    let described = backend
        .describe_key(DescribeKeyRequest {
            key_id: "scheduled-before-restart".to_string(),
        })
        .await
        .expect("the key must survive the restart");
    assert_eq!(
        described.key_metadata.key_state,
        KeyState::PendingDeletion,
        "the pending state must be persisted, not held in memory"
    );
    assert!(
        described.key_metadata.deletion_date.is_some(),
        "the deadline must be persisted so a restart does not reset the clock"
    );

    // The restarted process is still inside the window, then past it.
    assert_eq!(
        backend
            .remove_expired_key("scheduled-before-restart", &days_from_now(1))
            .await
            .expect("check"),
        ExpiredKeyRemoval::NotExpired,
        "a restart must not make an un-due key removable"
    );
    assert_eq!(
        backend
            .remove_expired_key("scheduled-before-restart", &days_from_now(8))
            .await
            .expect("removal"),
        ExpiredKeyRemoval::Removed
    );
    assert!(!exists(&backend, "scheduled-before-restart").await);
}

#[tokio::test]
async fn removed_key_material_is_actually_destroyed() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    create(&backend, "material-gone").await;

    let context = ctx(&[("bucket", "deletion-behavior"), ("object", "doomed.bin")]);
    let dek = backend
        .generate_data_key(GenerateDataKeyRequest {
            key_id: "material-gone".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("generate a data key while the key still exists");

    // While pending deletion, existing ciphertext must still open — this is the
    // whole point of the pending window.
    schedule(&backend, "material-gone", 7).await;
    let during_window = backend
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob.clone(),
            encryption_context: context.clone(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("a pending-deletion key must still decrypt");
    assert_eq!(during_window.plaintext, dek.plaintext_key);

    // After the deadline the material is gone and the ciphertext is dead.
    assert_eq!(
        backend
            .remove_expired_key("material-gone", &days_from_now(8))
            .await
            .expect("removal"),
        ExpiredKeyRemoval::Removed
    );
    assert_key_not_found(
        backend
            .describe_key(DescribeKeyRequest {
                key_id: "material-gone".to_string(),
            })
            .await,
        "material-gone",
    );
    assert!(
        backend
            .decrypt(DecryptRequest {
                ciphertext: dek.ciphertext_blob.clone(),
                encryption_context: context,
                grant_tokens: Vec::new(),
            })
            .await
            .is_err(),
        "ciphertext wrapped by a destroyed key must no longer decrypt"
    );

    // And a fresh backend over the same directory agrees: the removal was
    // durable, not just an in-memory state change.
    let restarted = local_backend(&dir).await;
    assert!(!exists(&restarted, "material-gone").await, "the removal must survive a restart");
}

#[tokio::test]
async fn deletion_only_touches_its_own_key() {
    let dir = TempDir::new().expect("temp dir");
    let backend = local_backend(&dir).await;
    for key_id in ["neighbour-a", "target", "neighbour-b"] {
        create(&backend, key_id).await;
    }
    schedule(&backend, "target", 7).await;

    assert_eq!(
        backend
            .remove_expired_key("target", &days_from_now(8))
            .await
            .expect("removal"),
        ExpiredKeyRemoval::Removed
    );

    for neighbour in ["neighbour-a", "neighbour-b"] {
        let described = backend
            .describe_key(DescribeKeyRequest {
                key_id: neighbour.to_string(),
            })
            .await
            .unwrap_or_else(|error| panic!("{neighbour} must be untouched: {error:?}"));
        assert_eq!(described.key_metadata.key_state, KeyState::Enabled);
        backend
            .generate_data_key(GenerateDataKeyRequest {
                key_id: neighbour.to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: ctx(&[("bucket", "deletion-behavior")]),
            })
            .await
            .unwrap_or_else(|error| panic!("{neighbour} must still work: {error:?}"));
    }
}

#[tokio::test]
async fn a_backend_without_deletion_support_reports_the_capability_gap() {
    let config = KmsConfig::static_kms(common::STATIC_KEY_ID.to_string(), common::static_secret_key());
    let backend = StaticKmsBackend::new(config).await.expect("static backend should build");

    assert!(
        !backend.capabilities().schedule_deletion,
        "the static backend must not advertise deletion scheduling"
    );
    assert_unsupported_capability(
        backend.remove_expired_key(common::STATIC_KEY_ID, &Zoned::now()).await,
        "remove_expired_key",
    );
}

/// The service manager runs a background sweep for backends that support
/// deletion. It uses wall-clock time, so a real expiry cannot be forced from
/// outside; what *is* checkable from here — and what a spurious-deletion bug
/// would break — is that the sweep leaves un-due keys alone while it runs.
#[tokio::test(start_paused = true)]
async fn the_background_sweep_never_removes_an_un_due_key() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("swept-but-not-due").await;
    manager
        .delete_key(DeleteKeyRequest {
            key_id: key_id.clone(),
            pending_window_in_days: Some(7),
            force_immediate: None,
            confirm_key_id: None,
        })
        .await
        .expect("schedule deletion");

    // With the clock paused, tokio auto-advances through the sweep interval, so
    // this loop drives many sweeps in a fraction of a second.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    let described = manager
        .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
        .await
        .expect("a key inside its window must survive every sweep");
    assert_eq!(described.key_metadata.key_state, KeyState::PendingDeletion);
    assert!(
        described.key_metadata.deletion_date.is_some(),
        "the sweep must not clear the deadline it is waiting on"
    );

    // And it can still be rescued afterwards.
    manager
        .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
        .await
        .expect("a key the sweep left alone must still be cancellable");
}
