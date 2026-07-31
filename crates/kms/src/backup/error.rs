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

//! Typed failures for the backup/restore bundle contract.

use thiserror::Error;

/// Typed failures raised while decoding or validating a backup bundle.
///
/// Every variant is a fail-closed condition: a restore surface observing any
/// of them must abort before touching target state. Messages carry only
/// identifiers (backup ids, KEK ids, artifact kinds and paths) — never key
/// material, bundle plaintext, or credentials.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BackupError {
    /// Manifest or bundle content fails structural, schema, or integrity
    /// validation (unknown fields, duplicate fields, digest mismatch,
    /// contradictory responsibility declarations, ...).
    #[error("backup bundle corrupted: {reason}")]
    Corrupted { reason: String },

    /// Input ended before a complete manifest could be decoded.
    #[error("backup manifest truncated: {reason}")]
    Truncated { reason: String },

    /// Manifest declares a format version this build does not understand.
    /// Unknown versions are always rejected; there is no best-effort read.
    #[error("unknown backup manifest format version {found} (this build supports version {supported})")]
    UnknownVersion { found: u32, supported: u32 },

    /// Bundle is protected by a different backup KEK than the one supplied.
    #[error(
        "backup bundle requires KEK '{required_kek_id}' version {required_kek_version}; \
         supplied KEK '{supplied_kek_id}' version {supplied_kek_version} cannot open it"
    )]
    WrongKek {
        required_kek_id: String,
        required_kek_version: u32,
        supplied_kek_id: String,
        supplied_kek_version: u32,
    },

    /// Manifest requires an artifact that is not present in the bundle.
    #[error("backup bundle is missing a required artifact: {artifact}")]
    MissingArtifact { artifact: String },

    /// Bundle has no completeness marker or records an in-progress state.
    /// A bundle that never reached its completeness marker must never be
    /// restored, regardless of how much of it is readable.
    #[error("backup bundle is incomplete ({reason}); incomplete bundles must never be restored")]
    IncompleteBundle { reason: String },
}

impl BackupError {
    /// Create a corrupted-bundle error.
    pub fn corrupted<S: Into<String>>(reason: S) -> Self {
        Self::Corrupted { reason: reason.into() }
    }

    /// Create a truncated-manifest error.
    pub fn truncated<S: Into<String>>(reason: S) -> Self {
        Self::Truncated { reason: reason.into() }
    }

    /// Create an incomplete-bundle error.
    pub fn incomplete_bundle<S: Into<String>>(reason: S) -> Self {
        Self::IncompleteBundle { reason: reason.into() }
    }

    /// Create a missing-artifact error.
    pub fn missing_artifact<S: Into<String>>(artifact: S) -> Self {
        Self::MissingArtifact {
            artifact: artifact.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KmsError;

    #[test]
    fn backup_errors_convert_into_kms_error_transparently() {
        let error = BackupError::corrupted("manifest digest mismatch");
        let kms_error: KmsError = error.clone().into();
        assert_eq!(kms_error.to_string(), error.to_string());
        assert!(matches!(kms_error, KmsError::Backup(inner) if inner == error));
    }

    #[test]
    fn error_messages_carry_identifiers_only() {
        // The display strings must stay descriptive without ever embedding
        // material or bundle plaintext; each variant only interpolates the
        // identifiers below.
        let wrong_kek = BackupError::WrongKek {
            required_kek_id: "backup-kek-1".to_string(),
            required_kek_version: 3,
            supplied_kek_id: "backup-kek-2".to_string(),
            supplied_kek_version: 1,
        };
        assert_eq!(
            wrong_kek.to_string(),
            "backup bundle requires KEK 'backup-kek-1' version 3; supplied KEK 'backup-kek-2' version 1 cannot open it"
        );

        let unknown = BackupError::UnknownVersion { found: 9, supported: 1 };
        assert_eq!(
            unknown.to_string(),
            "unknown backup manifest format version 9 (this build supports version 1)"
        );

        assert_eq!(
            BackupError::missing_artifact("key-material").to_string(),
            "backup bundle is missing a required artifact: key-material"
        );
        assert_eq!(
            BackupError::incomplete_bundle("manifest has no completeness marker").to_string(),
            "backup bundle is incomplete (manifest has no completeness marker); incomplete bundles must never be restored"
        );
    }
}
