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

//! Shared harness for the `rustfs-kms` black-box behavior suite.
//!
//! Everything here drives the crate through the same public entry points the
//! server uses (`KmsServiceManager` -> `KmsManager` / `ObjectEncryptionService`),
//! so the suite keeps holding after internal refactors. Two rules keep it
//! black-box:
//!
//! * no `pub(crate)` internals, no on-disk key format parsing;
//! * assertions target observable contract — error *variants*, returned values,
//!   and state that survives a restart — never implementation details.
//!
//! Every harness instance owns its own `KmsServiceManager`; the process-global
//! singleton is deliberately avoided so tests never cross-talk under nextest.

#![allow(dead_code)] // each test binary uses a different slice of the harness

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustfs_kms::backends::BackendCapabilities;
use rustfs_kms::{
    CreateKeyRequest, DeleteKeyRequest, KeyUsage, KmsConfig, KmsError, KmsManager, KmsServiceManager, KmsServiceStatus,
    ObjectEncryptionService, Result,
};
use tempfile::TempDir;

/// Key id configured for the static backend harness.
pub const STATIC_KEY_ID: &str = "behavior-static-key";

/// Deterministic 32-byte secret for the static backend, base64 encoded.
///
/// Fixed rather than random so a failure is reproducible; it is test-only
/// material and never leaves this crate's test binaries.
pub fn static_secret_key() -> String {
    BASE64.encode([0x5au8; 32])
}

/// Which backend a harness instance is running.
///
/// Local and Static always run. The two Vault backends are **opt-in**: they
/// need a reachable server, so they join the matrix only when
/// `RUSTFS_KMS_VAULT_TOKEN` is set (see [`live_vault_backends`]).
///
/// This matters for how a green run should be read. `rotate` and `versioning`
/// are advertised *only* by the Vault backends, so without the Vault lane every
/// capability-gated branch for them in a `for_each_backend` spec runs the
/// `UnsupportedCapability` side and never the working side — a rotation that
/// silently dropped prior key versions would pass. An offline-only run says
/// nothing about whether the Vault backends work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Local,
    Static,
    VaultKv2,
    VaultTransit,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Static => "static",
            Self::VaultKv2 => "vault-kv2",
            Self::VaultTransit => "vault-transit",
        }
    }

    /// Whether this backend keeps its state on an external server that outlives
    /// the harness, so key names must not collide between runs.
    pub fn is_vault(self) -> bool {
        matches!(self, Self::VaultKv2 | Self::VaultTransit)
    }
}

/// Address of the live Vault, defaulting to the usual local dev server.
pub fn vault_address() -> String {
    std::env::var("RUSTFS_KMS_VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string())
}

/// Token for the live Vault, or `None` when the Vault lane is switched off.
///
/// Presence of this variable is the single switch that adds the Vault backends
/// to every `for_each_backend` spec.
pub fn vault_token() -> Option<String> {
    std::env::var("RUSTFS_KMS_VAULT_TOKEN").ok().filter(|token| !token.is_empty())
}

/// The Vault backends to include in the matrix for this run.
pub fn live_vault_backends() -> Vec<BackendKind> {
    match vault_token() {
        Some(_) => vec![BackendKind::VaultKv2, BackendKind::VaultTransit],
        None => Vec::new(),
    }
}

/// A key name that cannot collide with another run against the same Vault.
///
/// Vault state is persistent and shared, unlike the per-test temp directory the
/// local backend gets, so a fixed name would make a rerun collide with its own
/// leftovers and turn every assertion into a function of run order.
pub fn unique_key_name(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

/// A running KMS service, reachable only through the crate's public API.
pub struct TestKms {
    manager: Arc<KmsServiceManager>,
    kind: BackendKind,
    config: KmsConfig,
    /// Ids of the keys [`TestKms::create_key`] created, so a run against a
    /// persistent Vault can remove them afterwards instead of accumulating
    /// `behavior-*` keys forever (rustfs/backlog#1774). Shared through an Arc
    /// because the harness instance is consumed by the spec while the cleanup
    /// runs after it.
    created_keys: Arc<Mutex<Vec<String>>>,
    /// Held for the harness lifetime so the local key directory outlives a
    /// simulated process restart.
    _dir: Option<TempDir>,
}

impl TestKms {
    /// Local backend with development defaults and no default key id.
    pub async fn local() -> Self {
        Self::local_with(|_| {}).await
    }

    /// Local backend with `tweak` applied to the configuration before start.
    pub async fn local_with(tweak: impl FnOnce(&mut KmsConfig)) -> Self {
        let dir = TempDir::new().expect("create temp key dir");
        let mut config = KmsConfig::local(dir.path().to_path_buf()).with_insecure_development_defaults();
        tweak(&mut config);
        let manager = start_manager(&config).await;
        Self {
            manager,
            kind: BackendKind::Local,
            config,
            created_keys: Arc::new(Mutex::new(Vec::new())),
            _dir: Some(dir),
        }
    }

    /// Vault KV v2 backend against the live server.
    ///
    /// Panics when the Vault lane is off — callers gate on
    /// [`live_vault_backends`] rather than calling this blind.
    pub async fn vault_kv2() -> Self {
        let token = vault_token().expect("RUSTFS_KMS_VAULT_TOKEN must be set to run the Vault lane");
        let address = vault_address().parse().expect("RUSTFS_KMS_VAULT_ADDR must be a URL");
        // A local dev Vault speaks plain HTTP, which the config guard refuses
        // unless development mode is declared explicitly.
        let config = KmsConfig::vault(address, token).with_insecure_development_defaults();
        let manager = start_manager(&config).await;
        Self {
            manager,
            kind: BackendKind::VaultKv2,
            config,
            created_keys: Arc::new(Mutex::new(Vec::new())),
            _dir: None,
        }
    }

    /// Vault Transit backend against the live server.
    pub async fn vault_transit() -> Self {
        let token = vault_token().expect("RUSTFS_KMS_VAULT_TOKEN must be set to run the Vault lane");
        let address = vault_address().parse().expect("RUSTFS_KMS_VAULT_ADDR must be a URL");
        let config = KmsConfig::vault_transit(address, token).with_insecure_development_defaults();
        let manager = start_manager(&config).await;
        Self {
            manager,
            kind: BackendKind::VaultTransit,
            config,
            created_keys: Arc::new(Mutex::new(Vec::new())),
            _dir: None,
        }
    }

    /// Static single-key backend with a fixed key id and secret.
    pub async fn static_backend() -> Self {
        let config = KmsConfig::static_kms(STATIC_KEY_ID.to_string(), static_secret_key());
        let manager = start_manager(&config).await;
        Self {
            manager,
            kind: BackendKind::Static,
            config,
            created_keys: Arc::new(Mutex::new(Vec::new())),
            _dir: None,
        }
    }

    /// Simulate a process restart: stop the running service and bring a brand
    /// new manager up over the same configuration and key directory.
    ///
    /// A fresh manager (rather than `stop` + `start` on the same one) is what
    /// makes this meaningful — it discards every in-memory cache and version
    /// counter, so anything that still holds afterwards came off disk.
    pub async fn restart(&mut self) {
        self.manager.stop().await.expect("stop should succeed");
        self.manager = start_manager(&self.config).await;
    }

    pub fn manager(&self) -> &Arc<KmsServiceManager> {
        &self.manager
    }

    pub fn kind(&self) -> BackendKind {
        self.kind
    }

    pub fn config(&self) -> &KmsConfig {
        &self.config
    }

    /// Key directory of the local backend, for restart-over-same-state setups.
    pub fn key_dir(&self) -> Option<PathBuf> {
        self.config.local_config().map(|local| local.key_dir.clone())
    }

    pub async fn kms(&self) -> Arc<KmsManager> {
        self.manager.get_manager().await.expect("KMS manager should be running")
    }

    pub async fn service(&self) -> Arc<ObjectEncryptionService> {
        self.manager
            .get_encryption_service()
            .await
            .expect("encryption service should be running")
    }

    pub async fn capabilities(&self) -> BackendCapabilities {
        self.kms().await.backend_capabilities()
    }

    /// Create a key and return its id, failing loudly on backends that cannot.
    pub async fn create_key(&self, name: &str) -> String {
        let response = self
            .kms()
            .await
            .create_key(CreateKeyRequest {
                key_name: Some(name.to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: Some(format!("black-box behavior key {name}")),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|error| panic!("create_key({name}) should succeed on {}: {error:?}", self.kind.name()));
        assert_eq!(response.key_id, name, "created key id must be the requested name");
        self.created_keys
            .lock()
            .expect("created-keys lock")
            .push(response.key_id.clone());
        response.key_id
    }

    /// Handle to the ids [`Self::create_key`] recorded, for cleanup that runs
    /// after a spec consumed the harness instance.
    pub fn created_keys_handle(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.created_keys)
    }

    /// Remove this instance's recorded Vault keys; see [`cleanup_vault_keys`].
    pub async fn cleanup(&self) {
        cleanup_vault_keys(self.kind, &self.config, self.created_keys_handle()).await;
    }
}

/// Best-effort removal of the Vault keys a harness instance created, so a
/// persistent dev Vault does not accumulate `behavior-*` keys across runs
/// (rustfs/backlog#1774). A no-op for the Local and Static backends, whose
/// state dies with the per-test temp directory.
///
/// The deletion runs on a fresh manager over the same configuration — the
/// case's own manager is consumed by the spec and may have been stopped by a
/// restart scenario — with the immediate-deletion gate enabled on the cleanup
/// configuration only, so the configuration under test keeps the gate at its
/// production default and specs asserting the gate's refusal stay honest.
///
/// Failures are reported but never panic: cleanup runs after the spec's own
/// assertions, and a Vault hiccup here must not turn a green behavior run red.
pub async fn cleanup_vault_keys(kind: BackendKind, config: &KmsConfig, created_keys: Arc<Mutex<Vec<String>>>) {
    if !kind.is_vault() {
        return;
    }
    let key_ids: Vec<String> = created_keys.lock().expect("created-keys lock").drain(..).collect();
    if key_ids.is_empty() {
        return;
    }
    let config = config.clone().with_immediate_deletion_allowed();
    let manager = start_manager(&config).await;
    let kms = manager.get_manager().await.expect("KMS manager should be running");
    for key_id in key_ids {
        // The Transit backend deletes in two steps (first call parks the key in
        // PendingDeletion, the next call destroys it); KV2 destroys on the
        // first call and reports KeyNotFound on the second.
        for _ in 0..2 {
            match kms
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    pending_window_in_days: None,
                    force_immediate: Some(true),
                    confirm_key_id: Some(key_id.clone()),
                })
                .await
            {
                Ok(_) => continue,
                Err(KmsError::KeyNotFound { .. }) => break,
                Err(error) => {
                    eprintln!("vault key cleanup: could not delete {key_id}: {error:?}");
                    break;
                }
            }
        }
    }
    if let Err(error) = manager.stop().await {
        eprintln!("vault key cleanup: could not stop the cleanup manager: {error:?}");
    }
}

async fn start_manager(config: &KmsConfig) -> Arc<KmsServiceManager> {
    let manager = Arc::new(KmsServiceManager::new());
    manager.configure(config.clone()).await.expect("configure should succeed");
    manager.start().await.expect("start should succeed");
    assert_eq!(
        manager.get_status().await,
        KmsServiceStatus::Running,
        "manager must report Running right after a successful start"
    );
    manager
}

/// One backend under the shared behavior spec, pre-seeded with a usable key.
pub struct BackendCase {
    pub kms: TestKms,
    /// A key that exists and is Enabled on this backend.
    pub key_id: String,
}

impl BackendCase {
    async fn new(kind: BackendKind) -> Self {
        match kind {
            BackendKind::Local => {
                let kms = TestKms::local().await;
                let key_id = kms.create_key("behavior-local-key").await;
                Self { kms, key_id }
            }
            BackendKind::Static => {
                let kms = TestKms::static_backend().await;
                Self {
                    kms,
                    key_id: STATIC_KEY_ID.to_string(),
                }
            }
            BackendKind::VaultKv2 => {
                let kms = TestKms::vault_kv2().await;
                let key_id = kms.create_key(&unique_key_name("behavior-kv2")).await;
                Self { kms, key_id }
            }
            BackendKind::VaultTransit => {
                let kms = TestKms::vault_transit().await;
                let key_id = kms.create_key(&unique_key_name("behavior-transit")).await;
                Self { kms, key_id }
            }
        }
    }

    pub fn kind(&self) -> BackendKind {
        self.kms.kind()
    }

    pub async fn caps(&self) -> BackendCapabilities {
        self.kms.capabilities().await
    }
}

/// Run one behavior spec against every backend in this run's matrix.
///
/// Always Local and Static; plus the Vault backends when the Vault lane is on
/// (see [`live_vault_backends`]).
///
/// The spec is expected to branch on `case.caps()`: a capability a backend
/// advertises must behave correctly, and one it does not advertise must be
/// rejected with `UnsupportedCapability` (or the backend's documented
/// read-only refusal). The contract is deliberately two-directional.
pub async fn for_each_backend<F, Fut>(spec: F)
where
    F: Fn(BackendCase) -> Fut,
    Fut: Future<Output = ()>,
{
    let kinds = [BackendKind::Local, BackendKind::Static]
        .into_iter()
        .chain(live_vault_backends());
    for kind in kinds {
        let case = BackendCase::new(kind).await;
        // Captured before the spec consumes the case; keys the spec creates
        // through the harness afterwards still land in the shared list.
        let config = case.kms.config().clone();
        let created_keys = case.kms.created_keys_handle();
        spec(case).await;
        cleanup_vault_keys(kind, &config, created_keys).await;
    }
}

/// Drop the service's own startup probe key from a listing.
///
/// Starting the service provisions the reserved [`rustfs_kms::probe::PROBE_KEY_ID`]
/// to verify the backend is actually usable, so it exists on every running
/// service and is not something a spec created. Exact-set assertions filter it
/// out: it is startup machinery, not behavior under test, and asserting it in
/// every expected list would couple those specs to the probe's naming.
pub fn without_probe_key(ids: impl IntoIterator<Item = String>) -> Vec<String> {
    ids.into_iter().filter(|id| id != rustfs_kms::probe::PROBE_KEY_ID).collect()
}

/// Build an encryption context from literal pairs.
pub fn ctx(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

/// Deterministic pseudo-random payload of `len` bytes.
///
/// Avoids a RNG dependency in the assertions while still producing data that a
/// broken cipher cannot accidentally round-trip (unlike an all-zero buffer).
pub fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 31 + 17) % 251) as u8).collect()
}

/// Drop a successful value so a result whose `Ok` type is not `Debug` (an
/// `AsyncRead` trait object, for instance) can still go through the error
/// assertions below.
pub fn discard<T>(result: Result<T>) -> Result<()> {
    result.map(|_| ())
}

/// Flip one bit in the middle of `bytes`, returning the tampered copy.
pub fn flip_middle_bit(bytes: &[u8]) -> Vec<u8> {
    assert!(!bytes.is_empty(), "cannot tamper with empty bytes");
    let mut tampered = bytes.to_vec();
    let index = tampered.len() / 2;
    tampered[index] ^= 0b0000_1000;
    tampered
}

// ---------------------------------------------------------------------------
// Error-variant assertions
//
// Every failure path is pinned to a KmsError *variant*, never to message text:
// messages are diagnostics and may be reworded, whereas the variant is what
// callers (admin handlers, ecfs) actually match on.
// ---------------------------------------------------------------------------

#[track_caller]
pub fn assert_key_not_found<T: Debug>(result: Result<T>, expected_key_id: &str) {
    match result {
        Err(KmsError::KeyNotFound { key_id }) => assert!(
            key_id.contains(expected_key_id),
            "KeyNotFound should name {expected_key_id:?}, got {key_id:?}"
        ),
        other => panic!("expected KeyNotFound({expected_key_id}), got {other:?}"),
    }
}

#[track_caller]
pub fn assert_key_already_exists<T: Debug>(result: Result<T>, expected_key_id: &str) {
    match result {
        Err(KmsError::KeyAlreadyExists { key_id }) => {
            assert_eq!(key_id, expected_key_id, "KeyAlreadyExists must name the conflicting key")
        }
        other => panic!("expected KeyAlreadyExists({expected_key_id}), got {other:?}"),
    }
}

#[track_caller]
pub fn assert_invalid_operation<T: Debug>(result: Result<T>, message_fragment: &str) {
    match result {
        Err(KmsError::InvalidOperation { message }) => assert!(
            message.contains(message_fragment),
            "InvalidOperation should mention {message_fragment:?}, got {message:?}"
        ),
        other => panic!("expected InvalidOperation containing {message_fragment:?}, got {other:?}"),
    }
}

#[track_caller]
pub fn assert_unsupported_capability<T: Debug>(result: Result<T>, expected_operation: &str) {
    match result {
        Err(KmsError::UnsupportedCapability { operation, .. }) => {
            assert_eq!(operation, expected_operation, "UnsupportedCapability must name the refused operation")
        }
        other => panic!("expected UnsupportedCapability({expected_operation}), got {other:?}"),
    }
}

#[track_caller]
pub fn assert_context_mismatch<T: Debug>(result: Result<T>) {
    match result {
        Err(KmsError::ContextMismatch { .. }) => {}
        other => panic!("expected ContextMismatch, got {other:?}"),
    }
}

#[track_caller]
pub fn assert_configuration_error<T: Debug>(result: Result<T>, message_fragment: &str) {
    match result {
        Err(KmsError::ConfigurationError { message }) => assert!(
            message.contains(message_fragment),
            "ConfigurationError should mention {message_fragment:?}, got {message:?}"
        ),
        other => panic!("expected ConfigurationError containing {message_fragment:?}, got {other:?}"),
    }
}

#[track_caller]
pub fn assert_invalid_key_size<T: Debug>(result: Result<T>, expected: usize, actual: usize) {
    match result {
        Err(KmsError::InvalidKeySize {
            expected: got_expected,
            actual: got_actual,
        }) => {
            assert_eq!(got_expected, expected, "InvalidKeySize.expected");
            assert_eq!(got_actual, actual, "InvalidKeySize.actual");
        }
        other => panic!("expected InvalidKeySize({expected}, {actual}), got {other:?}"),
    }
}

/// Assert that a rendered representation carries none of the given secrets.
///
/// Used against `Debug` and serde output of configs and responses: the crate's
/// security rule is that key material never reaches a log or an API payload.
#[track_caller]
pub fn assert_no_secret_leak(rendered: &str, secrets: &[&str]) {
    for secret in secrets {
        assert!(
            !rendered.contains(secret),
            "rendered output leaked a secret ({} chars of it): {rendered}",
            secret.len()
        );
    }
}
