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

//! Restore dry-run report contract.
//!
//! A restore dry-run is a zero-write preflight: it evaluates a bundle against
//! a target and reports every blocker, conflict, and external dependency
//! mismatch without modifying the target in any way. The report itself is
//! plain data — producing, serializing, or discarding it has no side effects,
//! and an implementation that writes anything during a dry-run violates this
//! contract. All values in a report are identifiers and references; secrets,
//! tokens, and key material never appear in it.

use crate::backup::error::BackupError;
use serde::{Deserialize, Serialize};

/// Machine-readable category of a restore blocker.
///
/// The first six codes mirror the [`BackupError`] variants — an unknown
/// format version reports the same code whether it came from the manifest or
/// from a bundled key record — and the remaining codes cover preflight
/// conditions that are not bundle defects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestoreBlockerCode {
    /// The bundle failed structural or integrity validation.
    BundleCorrupted,
    /// The manifest input ended prematurely.
    BundleTruncated,
    /// The manifest format version is unknown to this build.
    UnknownFormatVersion,
    /// The supplied backup KEK does not match the bundle's KEK.
    WrongBackupKek,
    /// A required artifact is absent from the bundle.
    MissingArtifact,
    /// The bundle has no completeness marker or is marked in-progress.
    IncompleteBundle,
    /// The target backend cannot satisfy the bundle's responsibility model.
    UnsupportedBackend,
    /// The bundle was produced by a different deployment than the target and
    /// no explicit cross-deployment authorization applies.
    DeploymentMismatch,
    /// An external dependency (Vault cluster, mount, Transit key, ...) that
    /// the bundle references is unreachable or missing.
    ExternalDependencyUnavailable,
}

/// One condition that forbids the restore outright.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreBlocker {
    /// Machine-readable category.
    pub code: RestoreBlockerCode,
    /// Human-readable detail. Identifiers only; never secrets or material.
    pub detail: String,
}

impl From<&BackupError> for RestoreBlocker {
    fn from(error: &BackupError) -> Self {
        let code = match error {
            BackupError::Corrupted { .. } => RestoreBlockerCode::BundleCorrupted,
            BackupError::Truncated { .. } => RestoreBlockerCode::BundleTruncated,
            BackupError::UnknownVersion { .. } => RestoreBlockerCode::UnknownFormatVersion,
            BackupError::UnsupportedFormatVersion { .. } => RestoreBlockerCode::UnknownFormatVersion,
            BackupError::WrongKek { .. } => RestoreBlockerCode::WrongBackupKek,
            BackupError::MissingArtifact { .. } => RestoreBlockerCode::MissingArtifact,
            BackupError::IncompleteBundle { .. } => RestoreBlockerCode::IncompleteBundle,
            BackupError::UnsupportedRecordVersion { .. } => RestoreBlockerCode::UnknownFormatVersion,
        };
        Self {
            code,
            detail: error.to_string(),
        }
    }
}

/// Kind of a conflict between bundle state and existing target state.
///
/// Restore is non-destructive by default: every conflict blocks the restore
/// unless an explicit, audited conflict policy resolves it. Silent overwrite
/// or merge is never an option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestoreConflictKind {
    /// The target already has a key with this stable id.
    KeyAlreadyExists,
    /// Restoring would lower a key version the target has already observed.
    VersionRegression,
    /// Restoring would lower the snapshot generation the target has already
    /// observed.
    GenerationRegression,
    /// Restoring would revive a key the target has deleted or scheduled for
    /// deletion.
    StateRegression,
}

/// One conflict with existing target state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreConflict {
    /// Stable key id the conflict concerns.
    pub key_id: String,
    /// Machine-readable category.
    pub kind: RestoreConflictKind,
    /// Human-readable detail. Identifiers only; never secrets or material.
    pub detail: String,
}

/// A mismatch between an external dependency reference recorded in the bundle
/// and what the target environment observes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalDependencyMismatch {
    /// Which dependency is affected (for example a Vault mount or Transit
    /// key name). References only; never credentials.
    pub dependency: String,
    /// The value the bundle recorded.
    pub expected: String,
    /// The value the target environment reports.
    pub observed: String,
}

/// Result of a restore dry-run preflight.
///
/// # Zero-write contract
///
/// A dry-run must not write to the target: no staging directories, no
/// repaired records, no metadata fixes triggered along the read path. The
/// report is pure data over values already known to the caller.
///
/// ```
/// use rustfs_kms::backup::{RestoreBlocker, RestoreBlockerCode, RestoreDryRunReport};
///
/// let clean = RestoreDryRunReport {
///     backup_id: "backup-0001".to_string(),
///     target_deployment_identity: "deployment-a".to_string(),
///     blockers: Vec::new(),
///     conflicts: Vec::new(),
///     external_mismatches: Vec::new(),
/// };
/// assert!(clean.restore_permitted());
///
/// let blocked = RestoreDryRunReport {
///     blockers: vec![RestoreBlocker {
///         code: RestoreBlockerCode::IncompleteBundle,
///         detail: "manifest has no completeness marker".to_string(),
///     }],
///     ..clean
/// };
/// assert!(!blocked.restore_permitted());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreDryRunReport {
    /// Identifier of the evaluated bundle.
    pub backup_id: String,
    /// Identity of the restore target the bundle was evaluated against.
    pub target_deployment_identity: String,
    /// Conditions that forbid the restore outright.
    pub blockers: Vec<RestoreBlocker>,
    /// Conflicts with existing target state.
    pub conflicts: Vec<RestoreConflict>,
    /// External dependency mismatches.
    pub external_mismatches: Vec<ExternalDependencyMismatch>,
}

impl RestoreDryRunReport {
    /// Whether the restore may proceed: true only when the preflight found
    /// no blockers, no conflicts, and no external dependency mismatches.
    pub fn restore_permitted(&self) -> bool {
        self.blockers.is_empty() && self.conflicts.is_empty() && self.external_mismatches.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> RestoreDryRunReport {
        RestoreDryRunReport {
            backup_id: "backup-0001".to_string(),
            target_deployment_identity: "deployment-b".to_string(),
            blockers: vec![RestoreBlocker {
                code: RestoreBlockerCode::DeploymentMismatch,
                detail: "bundle was produced by deployment-a".to_string(),
            }],
            conflicts: vec![RestoreConflict {
                key_id: "object-key".to_string(),
                kind: RestoreConflictKind::VersionRegression,
                detail: "target observed version 5, bundle carries version 3".to_string(),
            }],
            external_mismatches: vec![ExternalDependencyMismatch {
                dependency: "vault transit key rustfs-master".to_string(),
                expected: "min_version=2".to_string(),
                observed: "min_version=4".to_string(),
            }],
        }
    }

    #[test]
    fn report_round_trips_through_json() {
        let report = sample_report();
        let json = serde_json::to_string(&report).expect("serialization should succeed");
        let decoded: RestoreDryRunReport = serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(decoded, report);
    }

    #[test]
    fn restore_permitted_requires_every_section_empty() {
        assert!(!sample_report().restore_permitted());

        let clean = RestoreDryRunReport {
            blockers: Vec::new(),
            conflicts: Vec::new(),
            external_mismatches: Vec::new(),
            ..sample_report()
        };
        assert!(clean.restore_permitted());

        for section in 0..3 {
            let mut report = clean.clone();
            match section {
                0 => report.blockers = sample_report().blockers,
                1 => report.conflicts = sample_report().conflicts,
                _ => report.external_mismatches = sample_report().external_mismatches,
            }
            assert!(!report.restore_permitted(), "section {section} alone must block the restore");
        }
    }

    #[test]
    fn every_backup_error_maps_to_a_blocker_code() {
        let cases = [
            (BackupError::corrupted("x"), RestoreBlockerCode::BundleCorrupted),
            (BackupError::truncated("x"), RestoreBlockerCode::BundleTruncated),
            (
                BackupError::UnknownVersion { found: 2, supported: 1 },
                RestoreBlockerCode::UnknownFormatVersion,
            ),
            (
                BackupError::WrongKek {
                    required_kek_id: "a".to_string(),
                    required_kek_version: 1,
                    supplied_kek_id: "b".to_string(),
                    supplied_kek_version: 1,
                },
                RestoreBlockerCode::WrongBackupKek,
            ),
            (BackupError::missing_artifact("key-material"), RestoreBlockerCode::MissingArtifact),
            (BackupError::incomplete_bundle("x"), RestoreBlockerCode::IncompleteBundle),
        ];
        for (error, expected_code) in cases {
            let blocker = RestoreBlocker::from(&error);
            assert_eq!(blocker.code, expected_code, "wrong code for {error:?}");
            assert_eq!(blocker.detail, error.to_string());
        }
    }

    /// The zero-write contract in practice: a report is plain serializable
    /// data with no handles, no I/O, and no drop side effects.
    #[test]
    fn report_types_are_plain_data() {
        fn assert_plain_data<T>()
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Clone + PartialEq + std::fmt::Debug + Send + Sync + 'static,
        {
        }
        assert_plain_data::<RestoreDryRunReport>();
        assert_plain_data::<RestoreBlocker>();
        assert_plain_data::<RestoreConflict>();
        assert_plain_data::<ExternalDependencyMismatch>();
    }
}
