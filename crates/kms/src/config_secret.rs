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

//! Sealing of secret fields inside the persisted KMS configuration.
//!
//! The dynamic-configuration flow persists `KmsConfig` as JSON in cluster
//! storage. Without protection, inline authentication material — the Vault
//! token, an AppRole `secret_id`, the Local backend master key — lands there in
//! cleartext, readable by anyone with access to the backing store.
//!
//! When the per-node environment variable [`ENV_KMS_CONFIG_SECRET`] is set,
//! the values of those fields are individually sealed before the JSON is
//! written: Argon2id (same parameters as the Local key store) derives an
//! AES-256-GCM key from the operator secret and a per-value random salt, and
//! the field's logical label is bound as AEAD associated data so sealed values
//! cannot be swapped between fields. The sealed value replaces the plaintext
//! string in place, so the persisted document keeps its shape and every
//! non-secret field stays readable by older loaders.
//!
//! Compatibility contract (deliberate, operator-decided):
//! - **Unset secret is never an error on save.** The caller warns, naming the
//!   exposed field labels, and persists plaintext exactly as before.
//! - **Plaintext values load forever.** A value without the sealed prefix is
//!   used as-is; a node that gains the secret later reseals on its next save.
//! - **Sealed values fail closed.** A sealed value with no secret, a wrong
//!   secret, or a tampered payload is a load error — never silently treated
//!   as plaintext.

use crate::backends::local::{
    LOCAL_KMS_ARGON2_M_COST_KIB, LOCAL_KMS_ARGON2_P_COST, LOCAL_KMS_ARGON2_T_COST, LOCAL_KMS_MASTER_KEY_LEN,
    LOCAL_KMS_MASTER_KEY_SALT_LEN,
};
use crate::config::{BackendConfig, KmsConfig};
use crate::error::{KmsError, Result};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use base64_simd::STANDARD as BASE64_STANDARD;
use rand::RngExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Per-node environment variable holding the config-sealing secret.
pub const ENV_KMS_CONFIG_SECRET: &str = "RUSTFS_KMS_CONFIG_SECRET";

/// Marker prefix of a sealed value. Versioned so the payload layout can evolve.
pub const SEALED_VALUE_PREFIX: &str = "RUSTFS-KMS-ENC[v1]:";

const NONCE_LEN: usize = 12;

/// One secret leaf inside the persisted `KmsConfig` JSON document.
///
/// `segments` lists the object keys from the document root to the string leaf;
/// each segment carries the accepted spellings for that key (serde aliases mean
/// an old document can use a historical tag). `label` is the stable logical
/// name — it doubles as the AEAD associated data and as the identifier named
/// in logs and errors, aligned with `KMS_CONFIG_REDACTION_RULES`.
struct SecretField {
    segments: &'static [&'static [&'static str]],
    label: &'static str,
}

const SECRET_FIELDS: &[SecretField] = &[
    SecretField {
        segments: &[&["backend_config"], &["Local"], &["master_key"]],
        label: "kms.local.master_key",
    },
    SecretField {
        segments: &[
            &["backend_config"],
            &["VaultKV2", "Vault"],
            &["auth_method"],
            &["Token"],
            &["token"],
        ],
        label: "kms.vault.token",
    },
    SecretField {
        segments: &[
            &["backend_config"],
            &["VaultKV2", "Vault"],
            &["auth_method"],
            &["AppRole"],
            &["secret_id"],
        ],
        label: "kms.vault.approle.secret_id",
    },
    SecretField {
        segments: &[
            &["backend_config"],
            &["VaultTransit"],
            &["auth_method"],
            &["Token"],
            &["token"],
        ],
        label: "kms.vault_transit.token",
    },
    SecretField {
        segments: &[
            &["backend_config"],
            &["VaultTransit"],
            &["auth_method"],
            &["AppRole"],
            &["secret_id"],
        ],
        label: "kms.vault_transit.approle.secret_id",
    },
];

/// What a seal or open pass found, by logical field label.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConfigSecretOutcome {
    /// Fields carried (or now carry) a sealed value.
    pub sealed: Vec<&'static str>,
    /// Fields carried (or still carry) a non-empty plaintext value.
    pub plaintext: Vec<&'static str>,
}

/// The config-sealing secret from the process environment, if set and non-empty.
pub fn config_secret_from_env() -> Option<String> {
    std::env::var(ENV_KMS_CONFIG_SECRET).ok().filter(|value| !value.is_empty())
}

/// Whether a persisted string value is a sealed config secret.
pub fn is_sealed_config_value(value: &str) -> bool {
    value.starts_with(SEALED_VALUE_PREFIX)
}

/// Refuse a sealing secret that reuses key material the configuration itself
/// protects — the whole point is an independent trust root. Mirrors the backup
/// KEK rule. Comparison is constant-time over SHA-256 digests.
pub fn ensure_config_secret_is_independent(secret: &str, config: &KmsConfig) -> Result<()> {
    let mut protected: Vec<(&str, &str)> = Vec::new();
    match &config.backend_config {
        BackendConfig::Local(local) => {
            if let Some(master_key) = local.master_key.as_deref() {
                protected.push(("kms.local.master_key", master_key));
            }
        }
        BackendConfig::Static(static_config) => {
            protected.push(("kms.static.secret_key", &static_config.secret_key));
        }
        BackendConfig::VaultKv2(_) | BackendConfig::VaultTransit(_) | BackendConfig::Aws(_) => {}
    }

    let secret_digest = Sha256::digest(secret.as_bytes());
    for (label, value) in protected {
        if Sha256::digest(value.as_bytes()).ct_eq(&secret_digest).into() {
            return Err(KmsError::configuration_error(format!(
                "{ENV_KMS_CONFIG_SECRET} must not reuse the secret protected as {label}: the config-sealing secret is a separate trust root"
            )));
        }
    }
    Ok(())
}

/// Seal every present secret field of a persisted-config JSON document in place.
///
/// With `secret == None` nothing is modified; the returned outcome names the
/// plaintext fields so the caller can warn. Values already carrying the sealed
/// prefix are counted as sealed and left untouched, and empty strings are
/// skipped (an AppRole entry using `secret_id_file` persists an empty inline
/// `secret_id`).
pub fn seal_config_secrets(document: &mut Value, secret: Option<&str>) -> Result<ConfigSecretOutcome> {
    let mut outcome = ConfigSecretOutcome::default();
    for field in SECRET_FIELDS {
        let Some(slot) = resolve_field_mut(document, field.segments) else {
            continue;
        };
        let Value::String(current) = slot else {
            continue;
        };
        if current.is_empty() {
            continue;
        }
        if is_sealed_config_value(current) {
            outcome.sealed.push(field.label);
            continue;
        }
        match secret {
            Some(secret) => {
                *current = seal_value(field.label, current, secret)?;
                outcome.sealed.push(field.label);
            }
            None => outcome.plaintext.push(field.label),
        }
    }
    Ok(outcome)
}

/// Open every sealed secret field of a persisted-config JSON document in place.
///
/// Plaintext values pass through untouched (and are reported, so the caller
/// can warn about unsealed persisted secrets). A sealed value fails closed
/// when `secret` is absent, wrong, or the payload was tampered with.
pub fn open_config_secrets(document: &mut Value, secret: Option<&str>) -> Result<ConfigSecretOutcome> {
    let mut outcome = ConfigSecretOutcome::default();
    for field in SECRET_FIELDS {
        let Some(slot) = resolve_field_mut(document, field.segments) else {
            continue;
        };
        let Value::String(current) = slot else {
            continue;
        };
        if current.is_empty() {
            continue;
        }
        if !is_sealed_config_value(current) {
            outcome.plaintext.push(field.label);
            continue;
        }
        let Some(secret) = secret else {
            return Err(KmsError::configuration_error(format!(
                "persisted KMS configuration field {} is sealed; set {ENV_KMS_CONFIG_SECRET} to the secret used by the node that saved it",
                field.label
            )));
        };
        *current = open_value(field.label, current, secret)?;
        outcome.sealed.push(field.label);
    }
    Ok(outcome)
}

fn resolve_field_mut<'a>(document: &'a mut Value, segments: &[&[&str]]) -> Option<&'a mut Value> {
    let mut cursor = document;
    for alternatives in segments {
        let object = cursor.as_object_mut()?;
        let key = alternatives.iter().find(|key| object.contains_key(**key))?;
        cursor = object.get_mut(*key)?;
    }
    Some(cursor)
}

fn derive_sealing_key(secret: &str, salt: &[u8]) -> Result<Key<Aes256Gcm>> {
    let params = Params::new(
        LOCAL_KMS_ARGON2_M_COST_KIB,
        LOCAL_KMS_ARGON2_T_COST,
        LOCAL_KMS_ARGON2_P_COST,
        Some(LOCAL_KMS_MASTER_KEY_LEN),
    )
    .map_err(|err| KmsError::configuration_error(format!("invalid config-secret Argon2 params: {err}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut derived = [0u8; LOCAL_KMS_MASTER_KEY_LEN];
    argon2
        .hash_password_into(secret.as_bytes(), salt, &mut derived)
        .map_err(|err| KmsError::cryptographic_error("config_secret_kdf", err.to_string()))?;
    Ok(Key::<Aes256Gcm>::from(derived))
}

fn seal_value(label: &str, plaintext: &str, secret: &str) -> Result<String> {
    let mut salt = [0u8; LOCAL_KMS_MASTER_KEY_SALT_LEN];
    rand::rng().fill(&mut salt[..]);
    let mut nonce = [0u8; NONCE_LEN];
    rand::rng().fill(&mut nonce[..]);

    let key = derive_sealing_key(secret, &salt)?;
    let cipher = Aes256Gcm::new(&key);
    let sealed_nonce = Nonce::try_from(nonce.as_slice())
        .map_err(|_| KmsError::cryptographic_error("config_secret_seal", "invalid nonce length"))?;
    let ciphertext = cipher
        .encrypt(
            &sealed_nonce,
            Payload {
                msg: plaintext.as_bytes(),
                aad: label.as_bytes(),
            },
        )
        .map_err(|_| KmsError::cryptographic_error("config_secret_seal", format!("failed to seal {label}")))?;

    let mut payload = Vec::with_capacity(salt.len() + nonce.len() + ciphertext.len());
    payload.extend_from_slice(&salt);
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);
    Ok(format!("{SEALED_VALUE_PREFIX}{}", BASE64_STANDARD.encode_to_string(payload)))
}

fn open_value(label: &str, sealed: &str, secret: &str) -> Result<String> {
    let encoded = sealed
        .strip_prefix(SEALED_VALUE_PREFIX)
        .expect("caller checks the sealed prefix");
    let payload = BASE64_STANDARD
        .decode_to_vec(encoded)
        .map_err(|_| sealed_value_unreadable(label))?;
    if payload.len() <= LOCAL_KMS_MASTER_KEY_SALT_LEN + NONCE_LEN {
        return Err(sealed_value_unreadable(label));
    }
    let (salt, rest) = payload.split_at(LOCAL_KMS_MASTER_KEY_SALT_LEN);
    let (nonce, ciphertext) = rest.split_at(NONCE_LEN);

    let key = derive_sealing_key(secret, salt)?;
    let cipher = Aes256Gcm::new(&key);
    let sealed_nonce = Nonce::try_from(nonce).map_err(|_| sealed_value_unreadable(label))?;
    let plaintext = cipher
        .decrypt(
            &sealed_nonce,
            Payload {
                msg: ciphertext,
                aad: label.as_bytes(),
            },
        )
        .map_err(|_| {
            KmsError::configuration_error(format!(
                "persisted KMS configuration field {label} could not be unsealed: {ENV_KMS_CONFIG_SECRET} does not match the secret used by the node that saved it, or the value was modified"
            ))
        })?;
    String::from_utf8(plaintext).map_err(|_| sealed_value_unreadable(label))
}

fn sealed_value_unreadable(label: &str) -> KmsError {
    KmsError::configuration_error(format!("persisted KMS configuration field {label} carries a malformed sealed value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LocalConfig, StaticConfig, VaultAuthMethod, VaultConfig};

    fn vault_config_document(auth_method: VaultAuthMethod) -> Value {
        let config = KmsConfig {
            backend: crate::config::KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                auth_method,
                ..VaultConfig::default()
            })),
            ..KmsConfig::default()
        };
        serde_json::to_value(&config).expect("config serializes")
    }

    fn token_at(document: &Value) -> &str {
        document["backend_config"]["VaultKV2"]["auth_method"]["Token"]["token"]
            .as_str()
            .expect("token leaf exists")
    }

    #[test]
    fn seal_and_open_round_trip_a_vault_token() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });

        let sealed = seal_config_secrets(&mut document, Some("operator-secret")).expect("seal succeeds");
        assert_eq!(sealed.sealed, vec!["kms.vault.token"]);
        assert!(sealed.plaintext.is_empty());
        let sealed_value = token_at(&document).to_string();
        assert!(is_sealed_config_value(&sealed_value));
        assert!(!sealed_value.contains("super-secret"));

        let opened = open_config_secrets(&mut document, Some("operator-secret")).expect("open succeeds");
        assert_eq!(opened.sealed, vec!["kms.vault.token"]);
        assert_eq!(token_at(&document), "s.super-secret");
    }

    #[test]
    fn sealing_without_a_secret_reports_plaintext_and_changes_nothing() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });
        let before = document.clone();

        let outcome = seal_config_secrets(&mut document, None).expect("plaintext save stays allowed");
        assert_eq!(outcome.plaintext, vec!["kms.vault.token"]);
        assert!(outcome.sealed.is_empty());
        assert_eq!(document, before, "warn-only mode must not modify the document");
    }

    #[test]
    fn opening_a_sealed_value_without_the_secret_fails_closed() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });
        seal_config_secrets(&mut document, Some("operator-secret")).expect("seal succeeds");

        let error = open_config_secrets(&mut document, None).expect_err("a sealed value must not load without the secret");
        let rendered = error.to_string();
        assert!(rendered.contains(ENV_KMS_CONFIG_SECRET), "error must name the env var: {rendered}");
        assert!(rendered.contains("kms.vault.token"), "error must name the field: {rendered}");
    }

    #[test]
    fn opening_with_the_wrong_secret_fails_closed() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });
        seal_config_secrets(&mut document, Some("operator-secret")).expect("seal succeeds");

        open_config_secrets(&mut document, Some("not-the-secret")).expect_err("a wrong secret must not unseal");
    }

    #[test]
    fn sealed_values_cannot_be_swapped_between_fields() {
        let mut document = vault_config_document(VaultAuthMethod::approle("role".to_string(), "approle-secret".to_string()));
        // Give the same document a Local master key leaf by rebuilding as Local.
        let mut local_document = serde_json::to_value(&KmsConfig {
            backend_config: BackendConfig::Local(LocalConfig {
                master_key: Some("local-master".to_string()),
                ..LocalConfig::default()
            }),
            ..KmsConfig::default()
        })
        .expect("config serializes");

        seal_config_secrets(&mut document, Some("operator-secret")).expect("seal approle");
        seal_config_secrets(&mut local_document, Some("operator-secret")).expect("seal local");

        let approle_sealed = document["backend_config"]["VaultKV2"]["auth_method"]["AppRole"]["secret_id"]
            .as_str()
            .expect("sealed approle secret")
            .to_string();
        local_document["backend_config"]["Local"]["master_key"] = Value::String(approle_sealed);

        open_config_secrets(&mut local_document, Some("operator-secret"))
            .expect_err("a sealed value moved to another field must not unseal");
    }

    #[test]
    fn plaintext_documents_open_untouched_and_are_reported() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });

        let outcome = open_config_secrets(&mut document, Some("operator-secret")).expect("plaintext loads");
        assert_eq!(outcome.plaintext, vec!["kms.vault.token"]);
        assert_eq!(token_at(&document), "s.super-secret");
    }

    #[test]
    fn legacy_vault_alias_tag_is_still_sealed() {
        let mut document = vault_config_document(VaultAuthMethod::Token {
            token: "s.super-secret".to_string(),
        });
        // Rewrite the enum tag to the historical alias an old document may carry.
        let backend = document["backend_config"]["VaultKV2"].take();
        document["backend_config"] = serde_json::json!({ "Vault": backend });

        let outcome = seal_config_secrets(&mut document, Some("operator-secret")).expect("seal succeeds");
        assert_eq!(outcome.sealed, vec!["kms.vault.token"]);
        assert!(is_sealed_config_value(
            document["backend_config"]["Vault"]["auth_method"]["Token"]["token"]
                .as_str()
                .expect("token leaf exists")
        ));
    }

    #[test]
    fn empty_inline_approle_secret_is_skipped() {
        let mut document = vault_config_document(VaultAuthMethod::AppRole {
            role_id: "role".to_string(),
            secret_id: String::new(),
            secret_id_file: Some(std::path::PathBuf::from("/etc/rustfs/approle-secret")),
            mount: "approle".to_string(),
            refresh_safety_window_secs: None,
        });

        let outcome = seal_config_secrets(&mut document, Some("operator-secret")).expect("seal succeeds");
        assert!(outcome.sealed.is_empty());
        assert!(outcome.plaintext.is_empty(), "an empty inline secret is not an exposure");
    }

    #[test]
    fn config_secret_must_not_reuse_a_protected_secret() {
        let local = KmsConfig {
            backend_config: BackendConfig::Local(LocalConfig {
                master_key: Some("shared-secret".to_string()),
                ..LocalConfig::default()
            }),
            ..KmsConfig::default()
        };
        ensure_config_secret_is_independent("shared-secret", &local).expect_err("reusing the local master key must fail");
        ensure_config_secret_is_independent("independent", &local).expect("an independent secret passes");

        let static_config = KmsConfig {
            backend_config: BackendConfig::Static(StaticConfig {
                key_id: "static-key".to_string(),
                secret_key: "shared-secret".to_string(),
            }),
            ..KmsConfig::default()
        };
        ensure_config_secret_is_independent("shared-secret", &static_config).expect_err("reusing the static key must fail");
    }
}
