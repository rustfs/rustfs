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

//! Black-box behavior: how the service reacts to an unreachable backend.
//!
//! The rule that matters operationally is **a bad configuration must never take
//! down a working KMS**. Applying a config that points at a dead Vault is a
//! routine operator mistake; if it stopped the running service, every encrypted
//! object in the deployment would become unreadable until someone noticed. So
//! the candidate is health-checked *before* the swap, and a failing candidate
//! is discarded with the incumbent still serving.
//!
//! Everything here runs offline: an unreachable backend is a loopback port with
//! nothing listening, which is deterministic and needs no external server.
//!
//! Error *classification* and retry accounting for transport faults live in
//! `tests/vault_fault_injection.rs`, which drives the same public API with a
//! metrics recorder attached. This file deliberately does not duplicate it.
//!
//! Not covered offline: throttling (429) and recoverable 5xx responses. Forcing
//! those needs the crate's scripted Vault responder, which is `pub(crate)` and
//! therefore out of reach from an integration test; they are pinned by the
//! in-crate wiring tests in `backends::vault` instead.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestKms, assert_configuration_error, ctx};
use rustfs_kms::{
    BackendConfig, DecryptRequest, GenerateDataKeyRequest, KeySpec, KmsBackend as KmsBackendKind, KmsConfig, KmsServiceManager,
    KmsServiceStatus, VaultAuthMethod, VaultConfig,
};

/// A loopback address with nothing listening on it: reserve a port, then
/// release it so a connection there is refused immediately.
fn dead_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let address = format!("http://{}", listener.local_addr().expect("reserved port addr"));
    drop(listener);
    address
}

fn unreachable_vault_config() -> KmsConfig {
    KmsConfig {
        backend: KmsBackendKind::VaultKv2,
        backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
            address: dead_address(),
            auth_method: VaultAuthMethod::Token {
                token: "unused-token".to_string(),
            },
            namespace: None,
            mount_path: "transit".to_string(),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/resilience".to_string(),
            tls: None,
        })),
        allow_insecure_dev_defaults: true,
        // Keep the failure fast: one short attempt is enough to prove the point.
        timeout: Duration::from_millis(300),
        retry_attempts: 1,
        ..KmsConfig::default()
    }
}

#[tokio::test]
async fn starting_against_an_unreachable_backend_fails_without_publishing_a_service() {
    let manager = KmsServiceManager::new();
    let config = unreachable_vault_config();
    manager
        .configure(config.clone())
        .await
        .expect("configuring an unreachable backend is allowed: validation is not connectivity");
    assert_eq!(manager.get_status().await, KmsServiceStatus::Configured);

    let error = manager
        .start()
        .await
        .expect_err("starting must fail when the backend is unreachable");
    assert!(
        format!("{error}").contains("KMS backend"),
        "the failure must name the backend, got {error}"
    );

    match manager.get_status().await {
        KmsServiceStatus::Error(message) => assert!(!message.is_empty(), "a failed start must record why"),
        other => panic!("a failed start must leave an Error status, got {other:?}"),
    }
    assert!(
        manager.get_encryption_service().await.is_none(),
        "a failed start must not publish a half-built service"
    );
    assert!(manager.get_manager().await.is_none(), "a failed start must not publish a manager either");
    assert!(
        manager.get_service_version().await.is_none(),
        "a failed start must not claim a service version"
    );
    assert!(
        manager.get_config().await.is_some(),
        "the configuration survives so an operator can fix and retry"
    );
    assert!(
        !manager
            .health_check()
            .await
            .expect("health check must not error when nothing runs"),
        "a service that never started is unhealthy"
    );
}

/// The load-bearing case: pointing a *running* KMS at a dead backend must be a
/// rejected reconfigure, not an outage.
#[tokio::test]
async fn a_failing_candidate_never_replaces_a_healthy_service() {
    let kms = TestKms::local().await;
    let manager = kms.manager().clone();
    let key_id = kms.create_key("survives-bad-config").await;
    let context = ctx(&[("bucket", "resilience-behavior")]);

    let incumbent = manager.get_encryption_service().await.expect("service v1");
    let incumbent_manager = manager.get_manager().await.expect("manager v1");
    let dek = incumbent_manager
        .generate_data_key(GenerateDataKeyRequest {
            key_id: key_id.clone(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("the incumbent works before the bad reconfigure");

    // Attempt the bad swap. The Local backend's identity is also frozen, so a
    // cross-backend move is refused before connectivity is even attempted —
    // assert the refusal, then assert nothing moved.
    let error = manager
        .reconfigure(unreachable_vault_config())
        .await
        .expect_err("a reconfigure onto an unreachable backend must fail");
    assert!(!format!("{error}").is_empty(), "the failure must be reported");

    assert_eq!(
        manager.get_status().await,
        KmsServiceStatus::Running,
        "the incumbent must still be Running after a rejected reconfigure"
    );
    assert_eq!(
        manager.get_service_version().await,
        Some(1),
        "a rejected candidate must not consume a service version"
    );
    assert!(
        Arc::ptr_eq(&incumbent, &manager.get_encryption_service().await.expect("service")),
        "the published service must still be the incumbent instance"
    );
    assert!(
        manager.get_config().await.expect("config").local_config().is_some(),
        "the published configuration must still be the Local one"
    );

    // And it is not merely present — it still does real work, on both old and
    // freshly fetched handles.
    let decrypted = manager
        .get_manager()
        .await
        .expect("manager")
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob.clone(),
            encryption_context: context.clone(),
            grant_tokens: Vec::new(),
        })
        .await
        .expect("the surviving service must still decrypt");
    assert_eq!(decrypted.plaintext, dek.plaintext_key);
    assert!(manager.health_check().await.expect("health check"), "the survivor is healthy");
}

#[tokio::test]
async fn a_vault_backend_reconfigure_onto_a_dead_address_is_rejected() {
    // Start from a Vault-shaped (never-started) configuration so the transition
    // guard does not short-circuit the connectivity check, and confirm that the
    // candidate's health check is what refuses it.
    let manager = KmsServiceManager::new();
    manager
        .configure(unreachable_vault_config())
        .await
        .expect("configure is allowed");

    // Reconfigure while nothing is running: the candidate must still be
    // health-checked, so an unreachable backend cannot be published.
    let error = manager
        .reconfigure(unreachable_vault_config())
        .await
        .expect_err("an unreachable candidate must not be published even from a stopped state");
    assert!(
        format!("{error}").contains("reconfigure") || format!("{error}").contains("backend"),
        "the failure must point at the backend, got {error}"
    );
    assert!(
        manager.get_encryption_service().await.is_none(),
        "no service may be published by a failed reconfigure"
    );
    assert!(
        manager.get_service_version().await.is_none(),
        "no version may be consumed by a failed reconfigure"
    );
}

#[tokio::test]
async fn credentials_that_cannot_be_read_fail_closed_at_start() {
    // A Vault Agent token sink that is not there: the service must refuse to
    // start rather than come up and send unauthenticated requests.
    let missing = std::path::PathBuf::from("/nonexistent/rustfs-kms-behavior/vault-token");
    let config = KmsConfig {
        backend: KmsBackendKind::VaultKv2,
        backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
            address: dead_address(),
            auth_method: VaultAuthMethod::token_file(missing),
            namespace: None,
            mount_path: "transit".to_string(),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/resilience".to_string(),
            tls: None,
        })),
        allow_insecure_dev_defaults: true,
        timeout: Duration::from_millis(300),
        retry_attempts: 1,
        ..KmsConfig::default()
    };

    let manager = KmsServiceManager::new();
    manager.configure(config).await.expect("configure");
    assert!(
        manager.start().await.is_err(),
        "a missing credential source must keep the service from starting"
    );
    assert!(
        manager.get_encryption_service().await.is_none(),
        "no service may be published without usable credentials"
    );

    // An empty token-file path is a configuration error, caught before start.
    let empty_path = KmsConfig {
        backend: KmsBackendKind::VaultKv2,
        backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
            address: "https://vault.example.com:8200".to_string(),
            auth_method: VaultAuthMethod::token_file(std::path::PathBuf::new()),
            namespace: None,
            mount_path: "transit".to_string(),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/resilience".to_string(),
            tls: None,
        })),
        ..KmsConfig::default()
    };
    assert_configuration_error(empty_path.validate(), "token file path cannot be empty");
}

#[tokio::test]
async fn a_stopped_service_refuses_work_without_losing_its_state() {
    // Stopping is not a failure mode, but it is an unavailability the callers
    // must handle: handles disappear, the config stays, and a restart recovers.
    let kms = TestKms::local().await;
    let manager = kms.manager().clone();
    let key_id = kms.create_key("stop-and-recover").await;
    let context = ctx(&[("bucket", "resilience-behavior")]);

    let dek = kms
        .kms()
        .await
        .generate_data_key(GenerateDataKeyRequest {
            key_id: key_id.clone(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await
        .expect("generate before stopping");

    manager.stop().await.expect("stop");
    assert!(manager.get_encryption_service().await.is_none(), "a stopped service hands out no handles");
    assert!(!manager.health_check().await.expect("health check"), "a stopped service is unhealthy");

    // Stopping twice is idempotent, not an error.
    manager.stop().await.expect("a second stop must be a no-op");

    manager.start().await.expect("restart after stop");
    let recovered = kms
        .kms()
        .await
        .decrypt(DecryptRequest {
            ciphertext: dek.ciphertext_blob,
            encryption_context: context,
            grant_tokens: Vec::new(),
        })
        .await
        .expect("work done before the stop must still be readable after the restart");
    assert_eq!(recovered.plaintext, dek.plaintext_key);
}
