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

//! Black-box behavior: KMS service lifecycle, configuration, and redaction.
//!
//! Pins the contract the admin API depends on:
//!
//! * an unconfigured manager hands out nothing and refuses to start;
//! * `configure` is for the stopped state only — changing a running service is
//!   `reconfigure`, which never exposes a stopped interval;
//! * a configuration is validated *before* anything is published, and a failing
//!   persistence callback leaves the previous state completely intact;
//! * the Local backend's identity (directory, master key, dev-mode) is frozen
//!   once configured, because changing it silently orphans existing key files;
//! * redacted views never carry key material.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{STATIC_KEY_ID, TestKms, assert_configuration_error, assert_no_secret_leak, static_secret_key};
use rustfs_kms::{BackendConfig, KmsConfig, KmsError, KmsServiceManager, KmsServiceStatus, KmsStartOutcome, LocalConfig};
use tempfile::TempDir;
use url::Url;

fn local_dev_config(dir: &TempDir) -> KmsConfig {
    KmsConfig::local(dir.path().to_path_buf()).with_insecure_development_defaults()
}

#[tokio::test]
async fn unconfigured_manager_exposes_nothing_and_refuses_to_start() {
    let manager = KmsServiceManager::new();

    assert_eq!(manager.get_status().await, KmsServiceStatus::NotConfigured);
    assert!(manager.get_config().await.is_none(), "no config before configure");
    assert!(manager.get_redacted_config().await.is_none(), "no redacted config either");
    assert!(manager.get_manager().await.is_none(), "no manager handle before start");
    assert!(manager.get_encryption_service().await.is_none(), "no service handle before start");
    assert!(manager.get_service_version().await.is_none(), "no version before start");

    // A health check on a service that was never started is a definite "not
    // healthy", not an error: callers use it as a readiness probe.
    assert!(
        !manager.health_check().await.expect("health check must not error when idle"),
        "an unstarted service must report unhealthy"
    );

    assert_configuration_error(manager.start().await, "no configuration provided");

    // The failed start records the reason and still exposes no service.
    match manager.get_status().await {
        KmsServiceStatus::Error(message) => {
            assert!(message.contains("no configuration"), "error status should explain why: {message}")
        }
        other => panic!("expected Error status after starting without config, got {other:?}"),
    }
    assert!(manager.get_encryption_service().await.is_none());
    assert!(manager.get_service_version().await.is_none());
}

#[tokio::test]
async fn configure_start_stop_restart_state_machine() {
    let dir = TempDir::new().expect("temp dir");
    let manager = KmsServiceManager::new();
    let config = local_dev_config(&dir);

    manager.configure(config.clone()).await.expect("configure should succeed");
    assert_eq!(manager.get_status().await, KmsServiceStatus::Configured);
    assert!(
        manager.get_encryption_service().await.is_none(),
        "configured but not started must not expose a service"
    );
    assert!(manager.get_config().await.is_some(), "configure publishes the config");

    manager.start().await.expect("start should succeed");
    assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    assert_eq!(manager.get_service_version().await, Some(1), "first start is version 1");
    assert!(manager.get_manager().await.is_some());
    assert!(manager.get_encryption_service().await.is_some());
    assert!(manager.health_check().await.expect("health check"), "a started local KMS is healthy");

    manager.stop().await.expect("stop should succeed");
    assert_eq!(
        manager.get_status().await,
        KmsServiceStatus::Configured,
        "stop falls back to Configured, not NotConfigured, because the config survives"
    );
    assert!(manager.get_encryption_service().await.is_none(), "stop withdraws the service");
    assert!(manager.get_service_version().await.is_none(), "stop clears the published version");
    assert!(manager.get_config().await.is_some(), "stop keeps the configuration");
    assert!(!manager.health_check().await.expect("health check"), "a stopped service is unhealthy");

    // Restart after stop reuses the surviving config and takes the next version.
    manager.start().await.expect("restart after stop should succeed");
    assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    assert_eq!(
        manager.get_service_version().await,
        Some(2),
        "the version counter is monotonic across a stop/start cycle"
    );
}

#[tokio::test]
async fn start_or_restart_reports_the_action_it_took() {
    let dir = TempDir::new().expect("temp dir");
    let manager = KmsServiceManager::new();
    manager
        .configure(local_dev_config(&dir))
        .await
        .expect("configure should succeed");

    assert_eq!(
        manager.start_or_restart(false).await.expect("first start"),
        KmsStartOutcome::Started,
        "starting a stopped service reports Started"
    );
    assert_eq!(manager.get_service_version().await, Some(1));

    let service_v1 = manager.get_encryption_service().await.expect("service after start");

    assert_eq!(
        manager.start_or_restart(false).await.expect("non-forced start while running"),
        KmsStartOutcome::AlreadyRunning,
        "a non-forced start on a running service is a no-op"
    );
    assert_eq!(manager.get_service_version().await, Some(1), "the no-op path must not consume a version");
    assert!(
        Arc::ptr_eq(&service_v1, &manager.get_encryption_service().await.expect("service still running")),
        "a no-op start must not swap the live service instance"
    );

    assert_eq!(
        manager.start_or_restart(true).await.expect("forced restart"),
        KmsStartOutcome::Restarted,
        "a forced start on a running service reports Restarted"
    );
    assert_eq!(manager.get_service_version().await, Some(2), "a forced restart takes a new version");
    assert!(
        !Arc::ptr_eq(&service_v1, &manager.get_encryption_service().await.expect("service after restart")),
        "a forced restart must publish a different instance"
    );

    // The withdrawn instance keeps working for operations that already hold it.
    assert!(
        service_v1.health_check().await.expect("old handle health check"),
        "a replaced service stays usable while callers still hold it"
    );
}

#[tokio::test]
async fn configure_is_rejected_while_running() {
    let dir = TempDir::new().expect("temp dir");
    let manager = KmsServiceManager::new();
    let config = local_dev_config(&dir);
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");

    assert_configuration_error(manager.configure(config.clone()).await, "use reconfigure instead");

    // The rejected call must not have disturbed the running service.
    assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    assert_eq!(manager.get_service_version().await, Some(1), "a rejected configure takes no version");
}

#[tokio::test]
async fn reconfigure_swaps_the_service_without_a_stopped_interval() {
    let dir = TempDir::new().expect("temp dir");
    let manager = KmsServiceManager::new();
    let config = local_dev_config(&dir);
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");

    let service_v1 = manager.get_encryption_service().await.expect("service v1");
    let manager_v1 = manager.get_manager().await.expect("manager v1");

    let mut next = config.clone();
    next.timeout = Duration::from_secs(45);
    manager.reconfigure(next).await.expect("reconfigure should succeed");

    assert_eq!(manager.get_status().await, KmsServiceStatus::Running, "reconfigure never reports stopped");
    assert_eq!(manager.get_service_version().await, Some(2));
    assert_eq!(
        manager.get_config().await.expect("config after reconfigure").timeout,
        Duration::from_secs(45),
        "the published config must be the new one"
    );

    let service_v2 = manager.get_encryption_service().await.expect("service v2");
    let manager_v2 = manager.get_manager().await.expect("manager v2");
    assert!(!Arc::ptr_eq(&service_v1, &service_v2), "reconfigure publishes a new service");
    assert!(!Arc::ptr_eq(&manager_v1, &manager_v2), "reconfigure publishes a new manager");

    // Both generations stay functional: that is the whole point of the swap.
    assert!(service_v1.health_check().await.expect("v1 health"), "old generation still serves");
    assert!(service_v2.health_check().await.expect("v2 health"), "new generation serves");
}

#[tokio::test]
async fn failed_persistence_does_not_publish_the_new_configuration() {
    let dir = TempDir::new().expect("temp dir");
    let manager = KmsServiceManager::new();

    // configure_with_persistence: nothing is published when persistence fails.
    let result = manager
        .configure_with_persistence(local_dev_config(&dir), || async {
            Err(KmsError::backend_error("simulated persistence failure"))
        })
        .await;
    assert!(
        matches!(result, Err(KmsError::BackendError { .. })),
        "the persistence error must propagate verbatim, got {result:?}"
    );
    assert_eq!(
        manager.get_status().await,
        KmsServiceStatus::NotConfigured,
        "a failed persist must leave the manager unconfigured"
    );
    assert!(manager.get_config().await.is_none(), "a failed persist must publish no config");

    // Now bring it up for real, then fail persistence on a reconfigure.
    let config = local_dev_config(&dir);
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");
    let service_v1 = manager.get_encryption_service().await.expect("service v1");

    let mut next = config.clone();
    next.timeout = Duration::from_secs(99);
    let result = manager
        .reconfigure_with_persistence(next, || async { Err(KmsError::backend_error("simulated persistence failure")) })
        .await;
    assert!(result.is_err(), "reconfigure must fail when persistence fails, got {result:?}");

    assert_eq!(manager.get_status().await, KmsServiceStatus::Running, "the old service keeps running");
    assert_eq!(
        manager.get_config().await.expect("config").timeout,
        config.timeout,
        "the old configuration must still be the published one"
    );
    assert!(
        Arc::ptr_eq(&service_v1, &manager.get_encryption_service().await.expect("service")),
        "a failed reconfigure must not swap the live service"
    );
}

#[tokio::test]
async fn local_backend_identity_is_frozen_after_configuration() {
    let dir = TempDir::new().expect("temp dir");
    let other_dir = TempDir::new().expect("other temp dir");
    let manager = KmsServiceManager::new();
    let config = local_dev_config(&dir);
    manager.configure(config.clone()).await.expect("configure");
    manager.start().await.expect("start");

    // Moving the key directory would orphan every existing key file.
    let mut moved = config.clone();
    moved.backend_config = BackendConfig::Local(LocalConfig {
        key_dir: other_dir.path().to_path_buf(),
        master_key: None,
        file_permissions: Some(0o600),
    });
    assert_configuration_error(manager.reconfigure(moved).await, "key directory cannot be changed");

    // Changing the at-rest master key would make stored material undecryptable.
    let mut rekeyed = config.clone();
    rekeyed.backend_config = BackendConfig::Local(LocalConfig {
        key_dir: dir.path().to_path_buf(),
        master_key: Some("a-different-master-key".to_string()),
        file_permissions: Some(0o600),
    });
    assert_configuration_error(manager.reconfigure(rekeyed).await, "master key cannot be changed");

    // Flipping dev mode changes the at-rest protection of the same directory.
    let mut hardened = config.clone();
    hardened.allow_insecure_dev_defaults = false;
    hardened.backend_config = BackendConfig::Local(LocalConfig {
        key_dir: dir.path().to_path_buf(),
        master_key: Some("a-master-key".to_string()),
        file_permissions: Some(0o600),
    });
    assert!(
        manager.reconfigure(hardened).await.is_err(),
        "flipping development mode on a configured local backend must be refused"
    );

    // Switching away from Local entirely is refused for the same reason.
    let switched = KmsConfig::static_kms(STATIC_KEY_ID.to_string(), static_secret_key());
    assert_configuration_error(manager.reconfigure(switched).await, "backend cannot be changed");

    // Every rejection above left the original service untouched.
    assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    assert_eq!(
        manager.get_service_version().await,
        Some(1),
        "rejected transitions must not consume service versions"
    );
}

#[tokio::test]
async fn invalid_configurations_are_rejected_before_anything_starts() {
    let dir = TempDir::new().expect("temp dir");

    // A zero timeout or zero retry budget would make every operation fail.
    let mut zero_timeout = local_dev_config(&dir);
    zero_timeout.timeout = Duration::ZERO;
    assert_configuration_error(zero_timeout.validate(), "Timeout must be greater than 0");

    let mut zero_retries = local_dev_config(&dir);
    zero_retries.retry_attempts = 0;
    assert_configuration_error(zero_retries.validate(), "Retry attempts must be greater than 0");

    // Cache enabled with no capacity is contradictory.
    let mut empty_cache = local_dev_config(&dir);
    empty_cache.enable_cache = true;
    empty_cache.cache_config.max_keys = 0;
    assert_configuration_error(empty_cache.validate(), "max_keys must be greater than 0");

    // A relative key directory is ambiguous relative to the server's cwd.
    let mut relative = local_dev_config(&dir);
    relative.backend_config = BackendConfig::Local(LocalConfig {
        key_dir: "relative/kms".into(),
        master_key: None,
        file_permissions: Some(0o600),
    });
    assert_configuration_error(relative.validate(), "must be an absolute path");

    // Production-shaped Local config: no master key is refused without opt-in.
    let production_without_master_key = KmsConfig::local("/var/lib/rustfs/kms-behavior".into());
    assert_configuration_error(production_without_master_key.validate(), "requires a master key");

    // ...and so is a key directory under the process temp dir.
    let temp_backed = KmsConfig {
        backend_config: BackendConfig::Local(LocalConfig {
            key_dir: dir.path().to_path_buf(),
            master_key: Some("a-master-key".to_string()),
            file_permissions: Some(0o600),
        }),
        ..KmsConfig::local(dir.path().to_path_buf())
    };
    assert_configuration_error(temp_backed.validate(), "temp directory");

    // Static backend: the secret must be base64 and exactly 32 bytes.
    assert_configuration_error(
        KmsConfig::static_kms("k".to_string(), "not base64!!".to_string()).validate(),
        "not valid base64",
    );
    assert_configuration_error(
        KmsConfig::static_kms("k".to_string(), base64_of(&[0u8; 16])).validate(),
        "exactly 32 bytes",
    );
    assert_configuration_error(
        KmsConfig::static_kms(String::new(), static_secret_key()).validate(),
        "key_id cannot be empty",
    );
    assert_configuration_error(
        KmsConfig::static_kms("k".to_string(), String::new()).validate(),
        "secret_key cannot be empty",
    );

    // Vault: plaintext HTTP and the built-in dev token are dev-only.
    let http = Url::parse("http://127.0.0.1:8200").expect("url");
    assert_configuration_error(KmsConfig::vault(http.clone(), "a-real-token".to_string()).validate(), "requires HTTPS");
    let https = Url::parse("https://vault.example.com:8200").expect("url");
    assert_configuration_error(
        KmsConfig::vault(https.clone(), "dev-token".to_string()).validate(),
        "dev-token is not allowed",
    );
    // An AppRole with no credential at all cannot authenticate.
    let approle = KmsConfig::vault_approle(https, "role".to_string(), String::new());
    assert_configuration_error(approle.validate(), "requires a secret_id");

    // None of the above may be startable through the manager either.
    let manager = KmsServiceManager::new();
    assert_configuration_error(manager.configure(zero_timeout).await, "Timeout must be greater than 0");
    assert_eq!(
        manager.get_status().await,
        KmsServiceStatus::NotConfigured,
        "a rejected configure must not move the manager out of NotConfigured"
    );
}

#[tokio::test]
async fn redacted_views_never_carry_key_material() {
    let secret = static_secret_key();
    let manager = KmsServiceManager::new();
    manager
        .configure(KmsConfig::static_kms(STATIC_KEY_ID.to_string(), secret.clone()))
        .await
        .expect("configure static");
    manager.start().await.expect("start static");

    let redacted = manager.get_redacted_config().await.expect("redacted config");
    let static_config = redacted.static_config().expect("static config");
    assert!(static_config.secret_key.is_empty(), "the redacted view must zero the secret key");
    assert_eq!(static_config.key_id, STATIC_KEY_ID, "the key id is not secret and must survive");

    let (status, from_state) = manager.get_redacted_state().await;
    assert_eq!(status, KmsServiceStatus::Running, "redacted state carries the live status");
    let from_state = from_state.expect("redacted state config");
    assert!(
        from_state.static_config().expect("static config").secret_key.is_empty(),
        "get_redacted_state must redact exactly like get_redacted_config"
    );

    // Neither the Debug rendering nor a JSON serialization may carry material.
    assert_no_secret_leak(&format!("{redacted:?}"), &[&secret]);
    assert_no_secret_leak(&serde_json::to_string(&redacted).expect("redacted config serializes"), &[&secret]);

    // Even the unredacted config must keep the secret out of Debug and JSON:
    // `Debug` is what ends up in logs, and `secret_key` is `skip_serializing`.
    let live = manager.get_config().await.expect("live config");
    assert_no_secret_leak(&format!("{live:?}"), &[&secret]);
    assert_no_secret_leak(&serde_json::to_string(&live).expect("live config serializes"), &[&secret]);

    // A Vault token is subject to the same rule in Debug output.
    let vault = KmsConfig::vault(
        Url::parse("https://vault.example.com:8200").expect("url"),
        "super-secret-vault-token".to_string(),
    );
    assert_no_secret_leak(&format!("{vault:?}"), &["super-secret-vault-token"]);
}

#[tokio::test]
async fn harness_restart_brings_the_service_back_over_the_same_state() {
    let mut kms = TestKms::local().await;
    assert_eq!(kms.manager().get_service_version().await, Some(1));

    kms.restart().await;

    assert_eq!(kms.manager().get_status().await, KmsServiceStatus::Running);
    assert_eq!(
        kms.manager().get_service_version().await,
        Some(1),
        "a restarted process starts its version counter over"
    );
    assert!(
        kms.manager().health_check().await.expect("health check"),
        "the restarted service is healthy"
    );
}

fn base64_of(bytes: &[u8]) -> String {
    base64_simd::STANDARD.encode_to_string(bytes)
}
