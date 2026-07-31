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

//! Backup responsibility matrix: what a RustFS backup bundle owns per backend.
//!
//! The matrix is two-dimensional on purpose: responsibility is a function of
//! the backend *and* its at-rest protection state, not of the backend alone.
//! This keeps the schema stable when a backend changes protection direction —
//! switching Vault KV2 between storage-only and Transit-wrapped operation
//! selects a different existing row instead of changing the contract.
//!
//! These enums are backup-domain contract types. Once the backlog#1571
//! capability-discovery contract lands, the discovery surface is expected to
//! converge on (or map onto) the states defined here.

use serde::{Deserialize, Serialize};

/// Backend discriminant recorded in a backup manifest.
///
/// Wire names are aligned with [`crate::config::BackendConfig`] and
/// [`crate::config::KmsBackend`] (including the legacy `Vault` alias) so that
/// a manifest and a persisted KMS configuration never disagree about how the
/// same backend is spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupBackendKind {
    /// Local file-based backend.
    Local,
    /// Vault KV v2 storage backend.
    #[serde(rename = "VaultKV2", alias = "Vault")]
    VaultKv2,
    /// Vault Transit backend.
    VaultTransit,
    /// Static single-key backend.
    Static,
}

/// At-rest protection state of master key material, as observed at snapshot
/// time.
///
/// The first three states mirror the Local backend's on-disk protection
/// marker (`StoredKeyProtection` in `backends/local.rs`, kebab-case wire
/// names). The remaining states describe the non-local backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AtRestProtection {
    /// Local key files AEAD-encrypted under the Argon2id-derived master key.
    EncryptedMasterKey,
    /// Local development-only plaintext key files. Such files must never
    /// enter a bundle as-is; bundle artifacts are always re-wrapped under the
    /// backup KEK.
    PlaintextDevOnly,
    /// Pre-beta.9 local key files without a protection marker; the effective
    /// mode is resolved at read time. Treated like [`Self::PlaintextDevOnly`]
    /// for bundling purposes: re-wrap is mandatory.
    LegacyUnspecified,
    /// Vault KV2 as currently shipped: material confidentiality relies on
    /// Vault ACLs, KV2 at-rest encryption, and TLS only (the backend reports
    /// `at_rest_protection = "vault-kv2-acl"`); RustFS applies no
    /// cryptographic wrapping of its own.
    StorageOnly,
    /// Vault KV2 with material wrapped by Vault Transit before storage. Not
    /// produced by any current backend; the row exists so a future direction
    /// change selects a state instead of changing the schema.
    TransitWrapped,
    /// Vault Transit: the cryptographic root lives in Vault and is not
    /// exportable. RustFS can only ever own metadata and references.
    ExternalNonExportable,
    /// Static backend: the secret is delivered externally at startup and
    /// RustFS persists no key material at all.
    ExternalSecretDelivery,
}

/// What a RustFS backup bundle is responsible for, per (backend, protection)
/// combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackupResponsibility {
    /// The bundle carries the complete recoverable state: encrypted key
    /// material for every version, salt, metadata, and configuration.
    ///
    /// Restore precondition for the Local backend: the operator re-supplies
    /// the master key out of band. The master key itself is outside the
    /// backup domain; the manifest stores at most an opaque verifier.
    FullMaterial,
    /// The bundle carries only non-sensitive references and verification
    /// information. The source of truth is external secret delivery and the
    /// operator re-provides the secret during restore. Embedding the secret
    /// itself in a bundle is forbidden.
    ReferenceOnly,
    /// The bundle carries RustFS-side metadata, configuration references and
    /// verification data, while the cryptographic root is protected by the
    /// external system's native snapshot/disaster-recovery flow (Vault/HSM).
    /// Restore must re-establish the external trust root first.
    MetadataPlusExternalRoot,
}

impl BackupResponsibility {
    /// Resolve the responsibility matrix for one (backend, protection) cell.
    ///
    /// Returns `None` for combinations that no supported deployment can
    /// produce; manifests declaring such a combination are rejected as
    /// corrupted. This function is total and the unit tests anchor every
    /// cell, so any change to the matrix is a deliberate contract change.
    pub fn for_backend(backend: BackupBackendKind, protection: AtRestProtection) -> Option<Self> {
        use AtRestProtection::*;
        use BackupBackendKind::*;

        match (backend, protection) {
            (Local, EncryptedMasterKey | PlaintextDevOnly | LegacyUnspecified) => Some(Self::FullMaterial),
            (Local, _) => None,
            // Storage-only KV2 offers no external cryptographic root, so the
            // bundle must own the material (re-wrapped under the backup KEK).
            (VaultKv2, StorageOnly) => Some(Self::FullMaterial),
            (VaultKv2, TransitWrapped) => Some(Self::MetadataPlusExternalRoot),
            (VaultKv2, _) => None,
            (VaultTransit, ExternalNonExportable) => Some(Self::MetadataPlusExternalRoot),
            (VaultTransit, _) => None,
            (Static, ExternalSecretDelivery) => Some(Self::ReferenceOnly),
            (Static, _) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsBackend;

    fn json<T: Serialize>(value: &T) -> String {
        serde_json::to_string(value).expect("serialization should succeed")
    }

    #[test]
    fn backend_kind_wire_names_match_kms_backend() {
        let pairs = [
            (BackupBackendKind::Local, KmsBackend::Local),
            (BackupBackendKind::VaultKv2, KmsBackend::VaultKv2),
            (BackupBackendKind::VaultTransit, KmsBackend::VaultTransit),
            (BackupBackendKind::Static, KmsBackend::Static),
        ];
        for (backup_kind, config_kind) in pairs {
            assert_eq!(json(&backup_kind), json(&config_kind), "wire name drifted for {backup_kind:?}");
        }
    }

    #[test]
    fn backend_kind_accepts_legacy_vault_alias() {
        let decoded: BackupBackendKind = serde_json::from_str("\"Vault\"").expect("legacy alias should decode");
        assert_eq!(decoded, BackupBackendKind::VaultKv2);
    }

    #[test]
    fn responsibility_matrix_is_anchored_cell_by_cell() {
        use AtRestProtection::*;
        use BackupBackendKind::*;
        use BackupResponsibility::*;

        // Every (backend, protection) cell, exhaustively. Changing any row is
        // a contract change and must be made here consciously.
        let matrix = [
            (Local, EncryptedMasterKey, Some(FullMaterial)),
            (Local, PlaintextDevOnly, Some(FullMaterial)),
            (Local, LegacyUnspecified, Some(FullMaterial)),
            (Local, StorageOnly, None),
            (Local, TransitWrapped, None),
            (Local, ExternalNonExportable, None),
            (Local, ExternalSecretDelivery, None),
            (VaultKv2, EncryptedMasterKey, None),
            (VaultKv2, PlaintextDevOnly, None),
            (VaultKv2, LegacyUnspecified, None),
            (VaultKv2, StorageOnly, Some(FullMaterial)),
            (VaultKv2, TransitWrapped, Some(MetadataPlusExternalRoot)),
            (VaultKv2, ExternalNonExportable, None),
            (VaultKv2, ExternalSecretDelivery, None),
            (VaultTransit, EncryptedMasterKey, None),
            (VaultTransit, PlaintextDevOnly, None),
            (VaultTransit, LegacyUnspecified, None),
            (VaultTransit, StorageOnly, None),
            (VaultTransit, TransitWrapped, None),
            (VaultTransit, ExternalNonExportable, Some(MetadataPlusExternalRoot)),
            (VaultTransit, ExternalSecretDelivery, None),
            (Static, EncryptedMasterKey, None),
            (Static, PlaintextDevOnly, None),
            (Static, LegacyUnspecified, None),
            (Static, StorageOnly, None),
            (Static, TransitWrapped, None),
            (Static, ExternalNonExportable, None),
            (Static, ExternalSecretDelivery, Some(ReferenceOnly)),
        ];
        assert_eq!(matrix.len(), 28, "matrix must stay exhaustive: 4 backends x 7 protection states");
        for (backend, protection, expected) in matrix {
            assert_eq!(
                BackupResponsibility::for_backend(backend, protection),
                expected,
                "matrix cell drifted for ({backend:?}, {protection:?})"
            );
        }
    }

    #[test]
    fn local_protection_wire_names_match_stored_key_protection() {
        use crate::backends::local::StoredKeyProtection;

        // The manifest must record exactly the marker values the Local
        // backend writes to disk, or a restore could misread protection.
        let pairs = [
            (AtRestProtection::EncryptedMasterKey, StoredKeyProtection::EncryptedMasterKey),
            (AtRestProtection::PlaintextDevOnly, StoredKeyProtection::PlaintextDevOnly),
            (AtRestProtection::LegacyUnspecified, StoredKeyProtection::LegacyUnspecified),
        ];
        for (backup_state, stored_state) in pairs {
            assert_eq!(json(&backup_state), json(&stored_state), "wire name drifted for {backup_state:?}");
        }
    }

    #[test]
    fn responsibility_wire_names_are_frozen() {
        assert_eq!(json(&BackupResponsibility::FullMaterial), "\"full-material\"");
        assert_eq!(json(&BackupResponsibility::ReferenceOnly), "\"reference-only\"");
        assert_eq!(json(&BackupResponsibility::MetadataPlusExternalRoot), "\"metadata-plus-external-root\"");
    }
}
