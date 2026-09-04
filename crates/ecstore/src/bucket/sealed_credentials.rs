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

//! Sealed remote credentials shared by the three stores that persist one
//! (rustfs/backlog#2168): replication targets (`bucket-targets.json`), remote
//! tiers (`tier-config.bin`) and on-demand migration sources
//! (`on-demand-migration.json`).
//!
//! The design record is `docs/architecture/remote-credential-sealing-adr.md`.
//! What this module owns: the versioned envelope, the encryption context that
//! binds a ciphertext to the record owning it, the sealer registration point,
//! and the fail-closed error type. What it deliberately does not own: any KMS
//! call (ECStore does not depend on `rustfs-kms`; the binary installs a
//! sealer, exactly like `ON_DEMAND_MIGRATION_CONFIG_HOOK` and the event
//! dispatch hook in `crates/ecstore/src/services/event_notification.rs`), and
//! any decision about which stored field a consumer writes.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

/// Envelope format this build writes. A reader accepts only versions it
/// knows; an unknown version is a typed error, never a fallback.
pub const SEALED_CREDENTIAL_VERSION: u8 = 1;

/// Which store a sealed value belongs to. Part of the encryption context, so
/// a ciphertext cannot be replayed into a different store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealedCredentialStore {
    /// `bucket-targets.json` (replication and other bucket targets).
    BucketTargets,
    /// `tier-config.bin` (remote tiers).
    TierConfig,
    /// `on-demand-migration.json` (migration sources).
    OnDemandMigration,
}

impl SealedCredentialStore {
    pub fn as_str(self) -> &'static str {
        match self {
            SealedCredentialStore::BucketTargets => "bucket-targets",
            SealedCredentialStore::TierConfig => "tier-config",
            SealedCredentialStore::OnDemandMigration => "on-demand-migration",
        }
    }
}

/// Identity of the record a secret belongs to: the store, its owner (bucket
/// name, tier name, or target ARN) and the field name. Rendered into the KMS
/// encryption context so a ciphertext moved between buckets, tiers or fields
/// fails to decrypt instead of silently authorizing a different remote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealScope {
    pub store: SealedCredentialStore,
    pub owner: String,
    pub field: &'static str,
}

impl SealScope {
    pub fn new(store: SealedCredentialStore, owner: impl Into<String>, field: &'static str) -> Self {
        Self {
            store,
            owner: owner.into(),
            field,
        }
    }

    /// The encryption context handed to the sealer. Keys are stable: they are
    /// part of the on-disk contract, because a ciphertext only decrypts under
    /// the same context.
    pub fn encryption_context(&self) -> HashMap<String, String> {
        HashMap::from([
            ("rustfs:store".to_string(), self.store.as_str().to_string()),
            ("rustfs:owner".to_string(), self.owner.clone()),
            ("rustfs:field".to_string(), self.field.to_string()),
        ])
    }
}

/// A sealed secret as persisted. `Debug` prints no ciphertext: a sealed value
/// is not a secret, but it is noise in a log line and an operator reading one
/// should see the key it is wrapped under, not the bytes.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedCredential {
    /// Envelope version; see [`SEALED_CREDENTIAL_VERSION`].
    pub v: u8,
    /// KMS master key id the data key is wrapped under.
    pub key_id: String,
    /// Master key version, when the backend reports one. Carried so the KMS
    /// re-wrap job (`docs/architecture/kms-bulk-rekey-contract.md`) can tell
    /// stale envelopes apart; nothing here rotates on its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_version: Option<String>,
    /// Algorithm label reported by the sealer, for forensics and migration.
    pub alg: String,
    /// Ciphertext blob as produced by the sealer, base64 (standard, padded)
    /// in the JSON stores and raw inside the tier msgpack payload.
    pub ct: String,
}

impl fmt::Debug for SealedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedCredential")
            .field("v", &self.v)
            .field("key_id", &self.key_id)
            .field("key_version", &self.key_version)
            .field("alg", &self.alg)
            .field("ct", &format_args!("<{} bytes sealed>", self.ct.len()))
            .finish()
    }
}

impl SealedCredential {
    /// Rejects an envelope this build cannot read. Called before every
    /// unseal so an unknown version fails here rather than inside a backend.
    pub fn check_version(&self) -> Result<(), SealedCredentialError> {
        if self.v == SEALED_CREDENTIAL_VERSION {
            Ok(())
        } else {
            Err(SealedCredentialError::UnsupportedVersion(self.v))
        }
    }
}

/// Why a seal or unseal did not produce a usable value. Every variant is
/// terminal for the record that carried it: a caller reports the remote as
/// unusable, and never substitutes a default or empty credential.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SealedCredentialError {
    /// No sealer is installed: KMS is not configured, or the process has not
    /// finished startup. Reading a sealed record is impossible here.
    #[error("no credential sealer is installed")]
    NoSealer,
    /// The stored envelope is from a newer (or otherwise unknown) format.
    #[error("unsupported sealed credential envelope version {0}")]
    UnsupportedVersion(u8),
    /// The stored bytes are not a well-formed envelope.
    #[error("malformed sealed credential: {0}")]
    Malformed(String),
    /// The sealer refused: wrong encryption context, missing key, revoked
    /// access, or a failed authentication tag.
    #[error("sealed credential could not be unsealed: {0}")]
    Kms(String),
}

/// The KMS-backed half, installed by the binary at startup.
#[async_trait]
pub trait CredentialSealer: Send + Sync + 'static {
    /// Wraps `plaintext` under the scope's encryption context.
    async fn seal(&self, plaintext: &str, scope: &SealScope) -> Result<SealedCredential, SealedCredentialError>;

    /// Unwraps a stored envelope. Must fail when the envelope was sealed
    /// under a different scope.
    async fn unseal(&self, sealed: &SealedCredential, scope: &SealScope) -> Result<String, SealedCredentialError>;
}

static CREDENTIAL_SEALER: OnceLock<Arc<dyn CredentialSealer>> = OnceLock::new();

/// Installs the process-wide sealer. Returns `false` when one is already
/// installed, matching the other ECStore hooks.
pub fn install_credential_sealer(sealer: Arc<dyn CredentialSealer>) -> bool {
    CREDENTIAL_SEALER.set(sealer).is_ok()
}

/// The installed sealer, or `None` when KMS is not wired. Callers that only
/// need to know whether sealing is possible use this; callers that must have
/// it use [`seal_secret`] / [`unseal_secret`] and get the typed error.
pub fn credential_sealer() -> Option<Arc<dyn CredentialSealer>> {
    CREDENTIAL_SEALER.get().cloned()
}

/// Seals one secret field. Fails closed: without a sealer the caller must
/// reject the write rather than persist the secret in clear text after the
/// operator asked for sealing.
pub async fn seal_secret(plaintext: &str, scope: &SealScope) -> Result<SealedCredential, SealedCredentialError> {
    let sealer = credential_sealer().ok_or(SealedCredentialError::NoSealer)?;
    sealer.seal(plaintext, scope).await
}

/// Unseals one secret field, rejecting an unknown envelope version first.
pub async fn unseal_secret(sealed: &SealedCredential, scope: &SealScope) -> Result<String, SealedCredentialError> {
    sealed.check_version()?;
    let sealer = credential_sealer().ok_or(SealedCredentialError::NoSealer)?;
    sealer.unseal(sealed, scope).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    fn encode_context(context: &HashMap<String, String>) -> String {
        let ordered = context.iter().collect::<BTreeMap<_, _>>();
        serde_json::to_string(&ordered).expect("context serializes")
    }

    /// Stands in for the KMS-backed sealer: records the context it was called
    /// with, and refuses a ciphertext presented under a different one.
    #[derive(Default)]
    struct FakeSealer {
        sealed_contexts: Mutex<Vec<HashMap<String, String>>>,
    }

    #[async_trait]
    impl CredentialSealer for FakeSealer {
        async fn seal(&self, plaintext: &str, scope: &SealScope) -> Result<SealedCredential, SealedCredentialError> {
            let context = scope.encryption_context();
            self.sealed_contexts.lock().push(context.clone());
            let mut bound = encode_context(&context);
            bound.push('|');
            bound.push_str(plaintext);
            Ok(SealedCredential {
                v: SEALED_CREDENTIAL_VERSION,
                key_id: "key-1".to_string(),
                key_version: Some("3".to_string()),
                alg: "AES-256-GCM".to_string(),
                ct: base64_simd::STANDARD.encode_to_string(bound.as_bytes()),
            })
        }

        async fn unseal(&self, sealed: &SealedCredential, scope: &SealScope) -> Result<String, SealedCredentialError> {
            let raw = base64_simd::STANDARD
                .decode_to_vec(sealed.ct.as_bytes())
                .map_err(|err| SealedCredentialError::Malformed(err.to_string()))?;
            let bound = String::from_utf8(raw).map_err(|err| SealedCredentialError::Malformed(err.to_string()))?;
            let expected = encode_context(&scope.encryption_context());
            bound
                .strip_prefix(&expected)
                .and_then(|rest| rest.strip_prefix('|'))
                .map(str::to_string)
                .ok_or_else(|| SealedCredentialError::Kms("encryption context mismatch".to_string()))
        }
    }

    fn scope(owner: &str) -> SealScope {
        SealScope::new(SealedCredentialStore::OnDemandMigration, owner, "secret_key")
    }

    #[tokio::test]
    async fn seal_round_trips_and_binds_the_scope() {
        let sealer = Arc::new(FakeSealer::default());
        let sealed = sealer.seal("super-secret", &scope("photos")).await.expect("seal");
        assert_eq!(sealed.v, SEALED_CREDENTIAL_VERSION);
        assert_eq!(sealed.key_version.as_deref(), Some("3"));
        assert_eq!(sealer.unseal(&sealed, &scope("photos")).await.expect("unseal"), "super-secret");

        // The same ciphertext under another bucket must not unseal.
        let err = sealer
            .unseal(&sealed, &scope("other-bucket"))
            .await
            .expect_err("a ciphertext must not move between owners");
        assert!(matches!(err, SealedCredentialError::Kms(_)), "{err}");

        // Nor under another field of the same record.
        let other_field = SealScope::new(SealedCredentialStore::OnDemandMigration, "photos", "session_token");
        let err = sealer
            .unseal(&sealed, &other_field)
            .await
            .expect_err("a ciphertext must not move between fields");
        assert!(matches!(err, SealedCredentialError::Kms(_)), "{err}");

        let contexts = sealer.sealed_contexts.lock();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0]["rustfs:store"], "on-demand-migration");
        assert_eq!(contexts[0]["rustfs:owner"], "photos");
        assert_eq!(contexts[0]["rustfs:field"], "secret_key");
    }

    #[tokio::test]
    async fn an_unknown_envelope_version_is_rejected_before_the_sealer_is_asked() {
        let sealed = SealedCredential {
            v: SEALED_CREDENTIAL_VERSION + 1,
            key_id: "key-1".to_string(),
            key_version: None,
            alg: "AES-256-GCM".to_string(),
            ct: "Zm9v".to_string(),
        };
        assert_eq!(
            sealed.check_version().expect_err("a newer envelope must not be read"),
            SealedCredentialError::UnsupportedVersion(SEALED_CREDENTIAL_VERSION + 1)
        );
        // The global helper reports the version, not "no sealer", even in a
        // process where none is installed.
        assert_eq!(
            unseal_secret(&sealed, &scope("photos")).await.expect_err("version first"),
            SealedCredentialError::UnsupportedVersion(SEALED_CREDENTIAL_VERSION + 1)
        );
    }

    #[tokio::test]
    async fn without_a_sealer_both_directions_fail_closed() {
        // This test binary installs no sealer, so the global helpers must
        // report NoSealer rather than fall back to clear text.
        assert!(credential_sealer().is_none(), "no sealer is installed in unit tests");
        assert_eq!(
            seal_secret("super-secret", &scope("photos")).await.expect_err("seal"),
            SealedCredentialError::NoSealer
        );
        let sealed = SealedCredential {
            v: SEALED_CREDENTIAL_VERSION,
            key_id: "key-1".to_string(),
            key_version: None,
            alg: "AES-256-GCM".to_string(),
            ct: "Zm9v".to_string(),
        };
        assert_eq!(
            unseal_secret(&sealed, &scope("photos")).await.expect_err("unseal"),
            SealedCredentialError::NoSealer
        );
    }

    #[test]
    fn debug_and_serde_keep_the_on_disk_shape_stable() {
        let sealed = SealedCredential {
            v: 1,
            key_id: "key-1".to_string(),
            key_version: None,
            alg: "AES-256-GCM".to_string(),
            ct: "Zm9v".to_string(),
        };
        // key_version is omitted when absent, so an envelope from a backend
        // without version history stays compact.
        assert_eq!(
            serde_json::to_string(&sealed).expect("serialize"),
            r#"{"v":1,"key_id":"key-1","alg":"AES-256-GCM","ct":"Zm9v"}"#
        );
        let parsed: SealedCredential = serde_json::from_str(r#"{"v":1,"key_id":"key-1","alg":"AES-256-GCM","ct":"Zm9v"}"#)
            .expect("an envelope without key_version parses");
        assert_eq!(parsed, sealed);

        let rendered = format!("{sealed:?}");
        assert!(rendered.contains("key-1"), "{rendered}");
        assert!(!rendered.contains("Zm9v"), "Debug must not print the ciphertext: {rendered}");
    }

    #[test]
    fn a_malformed_envelope_is_a_typed_error() {
        let err = serde_json::from_str::<SealedCredential>(r#"{"v":1,"key_id":"key-1"}"#)
            .map_err(|err| SealedCredentialError::Malformed(err.to_string()))
            .expect_err("a truncated envelope must not parse");
        assert!(matches!(err, SealedCredentialError::Malformed(_)), "{err}");
    }
}
