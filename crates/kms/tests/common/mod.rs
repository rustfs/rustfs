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
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustfs_kms::backends::BackendCapabilities;
use rustfs_kms::{
    CreateKeyRequest, KeyUsage, KmsConfig, KmsError, KmsManager, KmsServiceManager, KmsServiceStatus, ObjectEncryptionService,
    Result,
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

/// Which offline backend a harness instance is running.
///
/// **Coverage gap — read before trusting this runner.** Only Local and Static
/// are here, because the two Vault backends need a live server. That is not a
/// neutral omission:
///
/// * the Vault KV2 and Vault Transit backends get **no business-capability
///   coverage at all** from this suite — every Vault-shaped config elsewhere in
///   it is exercising configuration validation, serde, or unreachable-backend
///   handling, never a working Vault;
/// * `rotate` and `versioning` are advertised *only* by the Vault backends, so
///   every capability-gated branch for them in `for_each_backend` specs runs
///   the `UnsupportedCapability` side and never the working side. A rotation
///   that silently lost prior key versions would not fail anything here.
///
/// Closing this needs either a stateful fake Vault exposed for integration
/// tests, or a CI lane that runs the `#[ignore]`d live-Vault cases against a
/// dev server. Do not read a green run as "the Vault backends work".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Local,
    Static,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Static => "static",
        }
    }
}

/// A running KMS service, reachable only through the crate's public API.
pub struct TestKms {
    manager: Arc<KmsServiceManager>,
    kind: BackendKind,
    config: KmsConfig,
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
            _dir: Some(dir),
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
        response.key_id
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
        }
    }

    pub fn kind(&self) -> BackendKind {
        self.kms.kind()
    }

    pub async fn caps(&self) -> BackendCapabilities {
        self.kms.capabilities().await
    }
}

/// Run one behavior spec against every offline backend.
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
    for kind in [BackendKind::Local, BackendKind::Static] {
        let case = BackendCase::new(kind).await;
        spec(case).await;
    }
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
pub fn assert_validation_error<T: Debug>(result: Result<T>) {
    match result {
        Err(KmsError::ValidationError { .. }) => {}
        other => panic!("expected ValidationError, got {other:?}"),
    }
}

#[track_caller]
pub fn assert_cryptographic_error<T: Debug>(result: Result<T>) {
    match result {
        Err(KmsError::CryptographicError { .. }) => {}
        other => panic!("expected CryptographicError, got {other:?}"),
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
