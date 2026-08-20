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

//! Vault restore orchestration: stage ordering, external reference
//! verification, and mismatch classification.
//!
//! # What this module does and does not own
//!
//! The cryptographic root of a Vault deployment is restored by Vault's own
//! disaster-recovery flow: Transit keys are non-exportable and RustFS never
//! attempts to import them. What RustFS owns is the *orchestration* around
//! that operator action — proving the recovered Vault is the one the bundle
//! was captured against, publishing the RustFS-side KV records in the right
//! order, and refusing every combination it cannot prove safe. Taking or
//! restoring the native Vault snapshot itself stays an operator step.
//!
//! # Restore order
//!
//! [`VaultRestoreStage`] is walked strictly in order and
//! [`VaultRestoreSequence`] enforces it structurally:
//!
//! 1. [`VaultRestoreStage::ExternalTrustRoot`] — verify the Transit/HSM trust
//!    root the operator restored matches the bundle's references. Read-only.
//! 2. [`VaultRestoreStage::MaterialAndVersions`] — publish material and
//!    version records ([`ArtifactKind::KeyMaterial`]).
//! 3. [`VaultRestoreStage::KvMetadata`] — publish metadata records
//!    ([`ArtifactKind::KeyMetadata`]); alias and policy artifacts are reserved
//!    names for features that do not exist yet.
//! 4. [`VaultRestoreStage::ConfigAndCutover`] — drop the commit marker. The
//!    sanitized configuration artifact is the admin layer's to apply.
//!
//! # Mismatch classification
//!
//! Every way the recovered Vault can disagree with the bundle is a distinct
//! [`VaultRestoreMismatch`] variant: a missing Transit key, a
//! `min_decryption_version` raised above what the bundle still needs, a
//! Transit version history restored below the bundle's snapshot point, a KV
//! generation rolled back, and cluster/namespace/mount identity drift. A
//! dry-run reports them all; an actual restore refuses on the first one.
//!
//! Verification is split by what it costs: every coordinate that follows from
//! the bundle and the caller's target description — cluster, namespace, KV
//! mount, KV prefix, Transit mount — is settled first, and any disagreement
//! there is returned before a single Vault request goes out. A target whose
//! coordinates already drifted is not the deployment the bundle describes, so
//! it is neither probed nor read through.
//!
//! # Writes
//!
//! [`dry_run_vault_restore`] issues reads only. Every record
//! [`restore_vault_backup`] publishes goes through create-only
//! check-and-set: a record that already exists is never overwritten, so the
//! default [`RestoreConflictPolicy::Fail`] and the empty-target policy differ
//! only in whether writing is attempted at all. Version regression, generation
//! rollback, and reviving deleted state are structurally impossible rather
//! than merely rejected. The single non-create-only write is the restore's own
//! commit marker, updated check-and-set against the exact version the restore
//! last observed.
//!
//! # Crash re-entry
//!
//! The commit marker is a reserved KV record published (create-only) before
//! the first write and removed after the last. Before it, the target is
//! untouched; with it published, re-running the same bundle rolls forward
//! idempotently. Every interruption converges to the complete old or the
//! complete new state.
//!
//! # Abort and ownership
//!
//! The marker names the key ids a restore *intends* to publish, which is not
//! evidence that it published them: the marker goes out first, and a record
//! write can afterwards be lost to a concurrent creator, or never be reached
//! at all. So each successful create-only write appends a receipt to the
//! marker carrying the KV version it landed at, and
//! [`abort_vault_restore`] removes a record only when the path still holds
//! exactly that version. Anything else — no receipt, a different version, an
//! already-empty path — is left alone and reported as skipped. An abort can
//! therefore leave records behind, but it can never delete a record this
//! restore did not write.

use crate::backends::local::validate_key_id;
use crate::backends::vault_credentials::{
    VaultClientHandle, VaultConnectionSettings, VaultCredentialPolicy, VaultCredentialProvider, token_source_for,
};
use crate::backup::dry_run::{
    ExternalDependencyMismatch, RestoreBlocker, RestoreBlockerCode, RestoreConflict, RestoreConflictKind, RestoreDryRunReport,
};
use crate::backup::error::BackupError;
use crate::backup::local_export::{BackupKek, decrypt_bundle_artifact, read_bundle_manifest};
use crate::backup::local_restore::RestoreConflictPolicy;
use crate::backup::manifest::{ArtifactKind, BackupManifest, ContentDigest, VaultExternalReferences, VaultTransitReference};
use crate::config::{KmsConfig, VaultAuthMethod};
use crate::error::{KmsError, Result};
use crate::policy::{self, AttemptError, OpClass, RetryPolicy};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use vaultrs::api::kv2::requests::SetSecretRequestOptions;
use vaultrs::api::transit::responses::ReadKeyData;
use vaultrs::error::ClientError;
use vaultrs::{kv2, transit::key};

// The bundle layout names are pub(crate) so the drill harness
// (`crate::backup::drill`) addresses the exact paths a Vault bundle uses
// instead of copies that could drift.
/// Bundle-relative directory holding one KV record artifact per key.
pub(crate) const VAULT_RECORD_ARTIFACT_DIR: &str = "vault/records";
/// Bundle-relative suffix of a KV record artifact.
pub(crate) const VAULT_RECORD_ARTIFACT_SUFFIX: &str = ".json.enc";
/// KV path segment (under the bundle's path prefix) of the restore commit
/// marker. The leading dot keeps it out of the key id space the backends
/// address; a bundle naming a key id equal to it is rejected.
const VAULT_RESTORE_MARKER_KEY: &str = ".rustfs-restore-commit";
/// Format version of the commit marker record.
const VAULT_RESTORE_MARKER_FORMAT_VERSION: u32 = 1;
/// Sentinel key id for report entries that concern the whole target.
const WHOLE_TARGET_CONFLICT_ID: &str = "*";

/// The restore stages, in the only order they may run.
///
/// Restoring RustFS-side state before the external trust root is back would
/// leave metadata pointing at key versions Vault cannot serve, so the order is
/// enforced rather than documented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VaultRestoreStage {
    /// Verify the Vault/HSM trust root the operator restored. Read-only:
    /// RustFS never imports non-exportable material.
    ExternalTrustRoot,
    /// Publish key material and version records.
    MaterialAndVersions,
    /// Publish KV metadata (and, once those features exist, policy and alias)
    /// records.
    KvMetadata,
    /// Apply configuration and complete the cutover.
    ConfigAndCutover,
}

impl VaultRestoreStage {
    /// The stages in execution order.
    pub const ORDER: [Self; 4] = [
        Self::ExternalTrustRoot,
        Self::MaterialAndVersions,
        Self::KvMetadata,
        Self::ConfigAndCutover,
    ];

    fn index(self) -> usize {
        Self::ORDER
            .iter()
            .position(|stage| *stage == self)
            .expect("ORDER lists every stage")
    }
}

/// Guard that makes the restore order structural.
///
/// A stage may only be entered when its predecessor completed, and only one
/// stage may be open at a time. Callers that skip, repeat, or reorder a stage
/// get a typed refusal instead of a partially ordered restore.
#[derive(Debug, Default)]
pub struct VaultRestoreSequence {
    next: usize,
    open: bool,
}

impl VaultRestoreSequence {
    /// A sequence positioned before the first stage.
    pub fn new() -> Self {
        Self::default()
    }

    /// The stage that may be entered next, or `None` once all stages ran.
    pub fn expected(&self) -> Option<VaultRestoreStage> {
        VaultRestoreStage::ORDER.get(self.next).copied()
    }

    /// Begin `stage`, failing closed when it is not the expected one.
    pub fn enter(&mut self, stage: VaultRestoreStage) -> Result<()> {
        if self.open || stage.index() != self.next {
            return Err(self.order_violation(stage, "start"));
        }
        self.open = true;
        Ok(())
    }

    /// Finish `stage`, failing closed when it is not the open one.
    pub fn complete(&mut self, stage: VaultRestoreStage) -> Result<()> {
        if !self.open || stage.index() != self.next {
            return Err(self.order_violation(stage, "complete"));
        }
        self.open = false;
        self.next += 1;
        Ok(())
    }

    fn order_violation(&self, stage: VaultRestoreStage, action: &str) -> KmsError {
        KmsError::invalid_operation(format!(
            "vault restore may not {action} stage {stage:?}: the restore order is {:?} and the next permitted stage is {:?}",
            VaultRestoreStage::ORDER,
            self.expected()
        ))
    }
}

/// One way the recovered Vault disagrees with what the bundle references.
///
/// Values are identifiers and version numbers only; no credential or key
/// material is representable here.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VaultRestoreMismatch {
    /// The target Vault cluster is not the one the bundle was captured from.
    #[error("bundle references Vault cluster '{expected}' but the restore target reports '{observed}'")]
    ClusterIdentity { expected: String, observed: String },

    /// The target namespace differs from the bundle's.
    #[error("bundle references Vault namespace {expected:?} but the restore target uses {observed:?}")]
    Namespace {
        expected: Option<String>,
        observed: Option<String>,
    },

    /// A mount path (KV, KV prefix, or Transit) differs from the bundle's.
    #[error("bundle references {dependency} '{expected}' but the restore target uses '{observed}'")]
    Mount {
        dependency: String,
        expected: String,
        observed: String,
    },

    /// The Transit key the bundle depends on does not exist in the target.
    /// The operator's native Vault restore has not completed, or completed
    /// against the wrong mount.
    #[error("Transit key '{key_name}' is absent from mount '{mount_path}'; restore the Vault trust root first")]
    TransitKeyMissing { mount_path: String, key_name: String },

    /// The Transit key no longer decrypts the oldest content in the bundle.
    #[error(
        "Transit key '{key_name}' has min_decryption_version {observed}, above the version {required} the bundle still needs"
    )]
    TransitMinDecryptionVersionTooHigh { key_name: String, required: u32, observed: u64 },

    /// The Transit key's version history was restored below the bundle's
    /// snapshot point.
    #[error(
        "Transit key '{key_name}' has latest version {observed_latest}, below the version {expected_current} the bundle was captured at"
    )]
    TransitVersionHistoryRegressed {
        key_name: String,
        expected_current: u32,
        observed_latest: u32,
    },

    /// A KV record's generation is below the bundle's snapshot point: the
    /// target's Vault was restored to an earlier moment than the bundle.
    #[error(
        "KV record for key '{key_id}' is at generation {observed}, below the generation {expected} the bundle was captured at"
    )]
    KvGenerationRegressed { key_id: String, expected: u64, observed: u64 },
}

impl VaultRestoreMismatch {
    /// Whether the mismatch means the dependency is absent rather than
    /// inconsistent. Absent dependencies are reported as blockers, the rest as
    /// external dependency mismatches; either way the restore is refused.
    pub fn is_dependency_unavailable(&self) -> bool {
        matches!(self, Self::TransitKeyMissing { .. })
    }

    /// Name of the affected dependency, for the dry-run report.
    pub fn dependency(&self) -> String {
        match self {
            Self::ClusterIdentity { .. } => "vault cluster identity".to_string(),
            Self::Namespace { .. } => "vault namespace".to_string(),
            Self::Mount { dependency, .. } => dependency.clone(),
            Self::TransitKeyMissing { mount_path, key_name } => format!("vault transit key {mount_path}/{key_name}"),
            Self::TransitMinDecryptionVersionTooHigh { key_name, .. } | Self::TransitVersionHistoryRegressed { key_name, .. } => {
                format!("vault transit key {key_name}")
            }
            Self::KvGenerationRegressed { key_id, .. } => format!("vault kv record for key {key_id}"),
        }
    }

    fn expected_value(&self) -> String {
        match self {
            Self::ClusterIdentity { expected, .. } | Self::Mount { expected, .. } => expected.clone(),
            Self::Namespace { expected, .. } => optional_value(expected),
            Self::TransitKeyMissing { key_name, .. } => format!("key {key_name} present"),
            Self::TransitMinDecryptionVersionTooHigh { required, .. } => format!("min_decryption_version<={required}"),
            Self::TransitVersionHistoryRegressed { expected_current, .. } => format!("latest_version>={expected_current}"),
            Self::KvGenerationRegressed { expected, .. } => format!("kv_generation>={expected}"),
        }
    }

    fn observed_value(&self) -> String {
        match self {
            Self::ClusterIdentity { observed, .. } | Self::Mount { observed, .. } => observed.clone(),
            Self::Namespace { observed, .. } => optional_value(observed),
            Self::TransitKeyMissing { .. } => "absent".to_string(),
            Self::TransitMinDecryptionVersionTooHigh { observed, .. } => format!("min_decryption_version={observed}"),
            Self::TransitVersionHistoryRegressed { observed_latest, .. } => format!("latest_version={observed_latest}"),
            Self::KvGenerationRegressed { observed, .. } => format!("kv_generation={observed}"),
        }
    }

    /// Record the mismatch in a dry-run report.
    fn record_in(&self, report: &mut RestoreDryRunReport) {
        if self.is_dependency_unavailable() {
            report.blockers.push(RestoreBlocker {
                code: RestoreBlockerCode::ExternalDependencyUnavailable,
                detail: self.to_string(),
            });
        } else {
            report.external_mismatches.push(ExternalDependencyMismatch {
                dependency: self.dependency(),
                expected: self.expected_value(),
                observed: self.observed_value(),
            });
        }
    }
}

impl From<VaultRestoreMismatch> for KmsError {
    fn from(mismatch: VaultRestoreMismatch) -> Self {
        KmsError::invalid_operation(mismatch.to_string())
    }
}

fn optional_value(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "<none>".to_string())
}

/// Coordinates of the Vault deployment a bundle is restored into.
///
/// `cluster_id` is injected by the caller rather than probed: the admin layer
/// owns how a deployment's Vault cluster identity is established, and this
/// module freezes only that the bundle's recorded identity must equal it.
#[derive(Debug, Clone)]
pub struct VaultRestoreTarget {
    /// Vault server URL.
    pub address: String,
    /// Authentication method for the restore credentials.
    pub auth_method: VaultAuthMethod,
    /// Vault Enterprise namespace, when the deployment uses one.
    pub namespace: Option<String>,
    /// Identity of the Vault cluster as the caller established it.
    pub cluster_id: String,
    /// KV v2 mount the RustFS records live under.
    pub kv_mount: String,
    /// Path prefix under `kv_mount` for the RustFS records.
    pub kv_path_prefix: String,
    /// Transit engine mount, for deployments whose trust root is in Transit.
    pub transit_mount: Option<String>,
}

/// Parameters of one Vault restore (or dry-run) attempt.
#[derive(Debug, Clone)]
pub struct VaultRestoreRequest {
    /// Bundle directory produced by a Vault export. Read-only throughout.
    pub bundle_dir: PathBuf,
    /// Where the bundle is being restored to.
    pub target: VaultRestoreTarget,
    /// Deployment identity of the restore target; must match the manifest.
    pub target_deployment_identity: String,
    /// Highest snapshot generation the target has already observed, when the
    /// caller tracks one. Equal generations stay allowed so a drill can be
    /// repeated; a strictly lower bundle generation is refused.
    pub observed_generation: Option<u64>,
    /// Conflict policy; see [`RestoreConflictPolicy`].
    pub conflict_policy: RestoreConflictPolicy,
}

impl VaultRestoreRequest {
    fn validate(&self) -> Result<()> {
        if self.target_deployment_identity.is_empty() {
            return Err(KmsError::validation_error("restore target_deployment_identity must not be empty"));
        }
        if self.target.cluster_id.is_empty() {
            return Err(KmsError::validation_error("restore target cluster_id must not be empty"));
        }
        Ok(())
    }
}

/// Serializable outcome of a completed Vault restore. Identifiers only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultRestoreReport {
    /// Identifier of the restored bundle.
    pub backup_id: String,
    /// Snapshot generation the target now holds.
    pub snapshot_generation: u64,
    /// Key ids this restore published.
    pub restored_key_ids: Vec<String>,
    /// Key ids the target already held with exactly the bundle's content, and
    /// which the restore therefore left alone.
    pub already_present_key_ids: Vec<String>,
    /// Transit key the bundle's trust root was verified against, if any.
    pub transit_key_name: Option<String>,
    /// Whether this run rolled forward a previously interrupted restore.
    pub resumed: bool,
}

/// Read-only client for the Vault deployment being restored into, plus the one
/// create-only write the restore performs.
///
/// Deliberately separate from the KMS backends: a restore runs against a Vault
/// that is not yet serving traffic, addresses reserved paths the backends do
/// not know about, and must never inherit a backend's read-repair or
/// lazy-create behavior.
pub struct VaultRestoreClient {
    credentials: Arc<VaultCredentialProvider>,
    kv_mount: String,
    retry: RetryPolicy,
    cancel: CancellationToken,
}

/// Transit key state as the target reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedTransitKey {
    min_decryption_version: u64,
    latest_version: u32,
}

/// One KV record as the target holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedKvRecord {
    generation: u64,
    content: serde_json::Value,
}

impl VaultRestoreClient {
    /// Authenticate against the restore target.
    pub async fn connect(target: &VaultRestoreTarget, kms_config: &KmsConfig) -> Result<Self> {
        let settings = VaultConnectionSettings {
            address: target.address.clone(),
            namespace: target.namespace.clone(),
            attempt_timeout: kms_config.effective_timeout(),
            // A restore target carries no TLS settings, so certificates are
            // always verified: recovery is the last path that should accept an
            // unauthenticated Vault.
            skip_tls_verify: false,
        };
        let source = token_source_for(&target.auth_method, &settings)?;
        let policy = VaultCredentialPolicy::from_kms_config(
            kms_config,
            &target.auth_method,
            "vault-restore",
            &target.address,
            target.namespace.as_deref(),
        );
        let credentials = Arc::new(VaultCredentialProvider::new(settings, source, policy).await?);
        Ok(Self {
            credentials,
            kv_mount: target.kv_mount.clone(),
            retry: RetryPolicy::for_backend(
                kms_config,
                "vault-restore",
                &target.address,
                target.namespace.as_deref(),
                "operations",
            ),
            cancel: CancellationToken::new(),
        })
    }

    fn vault(&self) -> Result<Arc<VaultClientHandle>> {
        self.credentials.current()
    }

    async fn run<T, F, Fut>(&self, operation: &'static str, class: OpClass, attempt: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, AttemptError>>,
    {
        policy::execute(operation, class, &self.retry, &self.cancel, attempt).await
    }

    /// Read a Transit key's version window. `None` means the key is absent.
    async fn read_transit_key(&self, mount_path: &str, key_name: &str) -> Result<Option<ObservedTransitKey>> {
        let response = self
            .run("vault_restore_read_transit_key", OpClass::ReadIdempotent, move || async move {
                match key::read(&self.vault().map_err(AttemptError::fatal)?.client, mount_path, key_name).await {
                    Ok(response) => Ok(Some(response)),
                    Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(None),
                    Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                        KmsError::backend_error(format!("Failed to read Transit key {key_name} during restore: {error}"))
                    })),
                }
            })
            .await?;

        Ok(response.map(|response| ObservedTransitKey {
            min_decryption_version: response.min_decryption_version,
            latest_version: latest_transit_version(&response.keys),
        }))
    }

    /// Read the version a KV path currently sits at. `None` means it carries
    /// no record.
    async fn read_kv_generation(&self, path: &str) -> Result<Option<u64>> {
        self.run("vault_restore_read_kv_generation", OpClass::ReadIdempotent, move || async move {
            match kv2::read_metadata(&self.vault().map_err(AttemptError::fatal)?.client, &self.kv_mount, path).await {
                Ok(metadata) => Ok(Some(metadata.current_version)),
                Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(None),
                Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                    KmsError::backend_error(format!("Failed to read KV generation of {path} during restore: {error}"))
                })),
            }
        })
        .await
    }

    /// Read a KV record together with the secret version holding it. `None`
    /// means the path carries no readable record.
    async fn read_kv_record(&self, path: &str) -> Result<Option<ObservedKvRecord>> {
        let Some(generation) = self.read_kv_generation(path).await? else {
            return Ok(None);
        };

        let content = self
            .run("vault_restore_read_kv_record", OpClass::ReadIdempotent, move || async move {
                match kv2::read::<serde_json::Value>(&self.vault().map_err(AttemptError::fatal)?.client, &self.kv_mount, path)
                    .await
                {
                    Ok(content) => Ok(Some(content)),
                    Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(None),
                    Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                        KmsError::backend_error(format!("Failed to read KV record {path} during restore: {error}"))
                    })),
                }
            })
            .await?;

        Ok(content.map(|content| ObservedKvRecord { generation, content }))
    }

    /// Create a KV record, never overwriting: the check-and-set precondition is
    /// "no version exists". `Ok(None)` means a concurrent writer got there
    /// first, which the caller surfaces instead of clobbering. `Ok(Some(v))`
    /// returns the version the write landed at, which is the only evidence
    /// that this restore — and not some other writer — owns the path.
    async fn create_kv_record(&self, path: &str, content: &serde_json::Value) -> Result<Option<u64>> {
        self.run("vault_restore_create_kv_record", OpClass::MutatingNonIdempotent, move || async move {
            match kv2::set_with_options(
                &self.vault().map_err(AttemptError::fatal)?.client,
                &self.kv_mount,
                path,
                content,
                SetSecretRequestOptions { cas: 0 },
            )
            .await
            {
                Ok(metadata) => Ok(Some(metadata.version)),
                Err(error) if is_cas_conflict(&error) => Ok(None),
                Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                    KmsError::backend_error(format!("Failed to create KV record {path} during restore: {error}"))
                })),
            }
        })
        .await
    }

    /// Replace a KV record, check-and-set against the exact version the caller
    /// last observed. Applied only to the restore's own commit marker: the
    /// restore is the marker's sole writer, so losing this race means another
    /// actor is writing the reserved path and the run must stop rather than
    /// re-read and retry.
    async fn update_kv_record(&self, path: &str, content: &serde_json::Value, expected_version: u64) -> Result<u64> {
        let cas = u32::try_from(expected_version).map_err(|_| {
            KmsError::internal_error(format!(
                "KV record {path} sits at version {expected_version}, beyond what check-and-set can address"
            ))
        })?;
        self.run("vault_restore_update_kv_record", OpClass::MutatingNonIdempotent, move || async move {
            match kv2::set_with_options(
                &self.vault().map_err(AttemptError::fatal)?.client,
                &self.kv_mount,
                path,
                content,
                SetSecretRequestOptions { cas },
            )
            .await
            {
                Ok(metadata) => Ok(metadata.version),
                Err(error) if is_cas_conflict(&error) => Err(AttemptError::fatal(KmsError::invalid_operation(format!(
                    "the restore commit marker at {path} was modified concurrently; another restore or \
                     operator is acting on this target"
                )))),
                Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                    KmsError::backend_error(format!("Failed to update KV record {path} during restore: {error}"))
                })),
            }
        })
        .await
    }

    /// Remove a KV record and all its versions. Only ever applied to the
    /// commit marker and to records whose current version the caller has just
    /// matched against the marker's write receipt for them.
    async fn delete_kv_record(&self, path: &str) -> Result<()> {
        self.run("vault_restore_delete_kv_record", OpClass::MutatingNonIdempotent, move || async move {
            match kv2::delete_metadata(&self.vault().map_err(AttemptError::fatal)?.client, &self.kv_mount, path).await {
                Ok(()) => Ok(()),
                Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => Ok(()),
                Err(error) => Err(AttemptError::from_vaultrs(error, |error| {
                    KmsError::backend_error(format!("Failed to remove KV record {path} during restore: {error}"))
                })),
            }
        })
        .await
    }
}

/// Whether a KV2 write failed its check-and-set precondition. Mirrors the
/// helper of the same name in the Vault backends, which share no private
/// module with this one.
fn is_cas_conflict(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::APIError { code: 400, errors } if errors.iter().any(|message| message.contains("check-and-set"))
    )
}

/// Highest version present in a Transit key's version map.
fn latest_transit_version(keys: &ReadKeyData) -> u32 {
    let versions: Vec<&String> = match keys {
        ReadKeyData::Symmetric(map) => map.keys().collect(),
        ReadKeyData::Asymmetric(map) => map.keys().collect(),
    };
    versions
        .into_iter()
        .filter_map(|name| name.parse::<u32>().ok())
        .max()
        .unwrap_or(0)
}

/// The commit marker: its create-only publication is the single commit point
/// of a Vault restore, and from then on it doubles as the restore's write
/// ledger.
///
/// `key_ids` is intent — what this restore set out to publish — and drives
/// re-entry. `written` is evidence: one receipt per record the restore
/// actually created, carrying the KV version its create-only write landed at.
/// Intent alone proves nothing about ownership (a write can be lost to a
/// concurrent creator after the marker is published), so only `written` may
/// authorize an abort to remove anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VaultRestoreCommitMarker {
    format_version: u32,
    backup_id: String,
    manifest_digest: ContentDigest,
    key_ids: Vec<String>,
    /// Absent in a marker published before the first record write.
    #[serde(default)]
    written: Vec<VaultRestoreWriteReceipt>,
}

/// Proof that this restore, and nothing else, created one KV record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VaultRestoreWriteReceipt {
    key_id: String,
    /// Version the create-only write landed at.
    kv_version: u64,
}

impl VaultRestoreCommitMarker {
    fn new(manifest: &BackupManifest, key_ids: Vec<String>) -> Self {
        Self {
            format_version: VAULT_RESTORE_MARKER_FORMAT_VERSION,
            backup_id: manifest.backup_id.clone(),
            manifest_digest: manifest.manifest_digest.clone(),
            key_ids,
            written: Vec::new(),
        }
    }

    /// Strict decode: an out-of-contract marker fails closed, because both
    /// roll-forward and abort would otherwise guess what it was committing.
    fn from_value(value: serde_json::Value) -> Result<Self> {
        let marker: Self = serde_json::from_value(value)
            .map_err(|error| BackupError::corrupted(format!("vault restore commit marker does not decode: {error}")))?;
        if marker.format_version != VAULT_RESTORE_MARKER_FORMAT_VERSION {
            return Err(BackupError::corrupted(format!(
                "vault restore commit marker has unknown format version {}",
                marker.format_version
            ))
            .into());
        }
        for key_id in &marker.key_ids {
            validate_key_id(key_id)?;
        }
        for receipt in &marker.written {
            validate_key_id(&receipt.key_id)?;
            if !marker.key_ids.contains(&receipt.key_id) {
                return Err(BackupError::corrupted(format!(
                    "vault restore commit marker carries a write receipt for key '{}', which it never claimed",
                    receipt.key_id
                ))
                .into());
            }
        }
        Ok(marker)
    }

    /// The version this restore wrote key `key_id` at, if it got that far.
    fn receipt_for(&self, key_id: &str) -> Option<u64> {
        self.written
            .iter()
            .find(|receipt| receipt.key_id == key_id)
            .map(|receipt| receipt.kv_version)
    }

    fn matches_bundle(&self, manifest: &BackupManifest) -> bool {
        self.backup_id == manifest.backup_id && self.manifest_digest == manifest.manifest_digest
    }

    fn to_content(&self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| KmsError::internal_error(format!("restore commit marker serialization failed: {error}")))
    }
}

/// A commit marker as the target holds it, with the version to check-and-set
/// against when the restore appends its next write receipt.
struct PublishedMarker {
    marker: VaultRestoreCommitMarker,
    kv_version: u64,
}

/// One KV record decoded out of the bundle.
struct DecodedVaultRecord {
    key_id: String,
    /// Stage that publishes this record, derived from its artifact kind.
    stage: VaultRestoreStage,
    /// KV generation the record was captured at.
    kv_version: u64,
    content: serde_json::Value,
}

/// The fully decoded and cross-checked Vault bundle content.
struct DecodedVaultBundle {
    manifest: BackupManifest,
    references: VaultExternalReferences,
    records: Vec<DecodedVaultRecord>,
}

/// Decode and cross-verify a Vault bundle in memory.
///
/// Every artifact is read, digest-verified, and AEAD-authenticated; every
/// record is re-parsed, identity-checked against its artifact path, and
/// matched against the manifest's KV references in both directions, so a
/// bundle that references state it does not carry (or carries state it does
/// not reference) is rejected before anything is inspected.
async fn decode_vault_bundle(bundle_dir: &Path, kek: &BackupKek) -> Result<DecodedVaultBundle> {
    let manifest = read_bundle_manifest(bundle_dir).await?;
    let references = manifest
        .external_references
        .clone()
        .ok_or_else(|| BackupError::corrupted("vault bundle manifest carries no external Vault references"))?;

    let mut records = Vec::with_capacity(manifest.artifacts.len());
    for artifact in &manifest.artifacts {
        let stage = match artifact.kind {
            ArtifactKind::KeyMaterial => VaultRestoreStage::MaterialAndVersions,
            ArtifactKind::KeyMetadata => VaultRestoreStage::KvMetadata,
            // Restoring a bundle that carries state this implementation
            // cannot apply would silently drop it, so fail closed instead.
            other => {
                return Err(KmsError::unsupported_capability(
                    "vault-restore",
                    format!("bundle carries artifact kind {other:?}, which this restore implementation cannot apply"),
                ));
            }
        };

        let key_id = vault_record_key_id(&artifact.path)?;
        let reference = references
            .kv_records
            .iter()
            .find(|record| record.key_id == key_id)
            .ok_or_else(|| {
                BackupError::corrupted(format!("bundle carries a record for key '{key_id}' with no KV generation reference"))
            })?;

        let plaintext = decrypt_bundle_artifact(bundle_dir, &manifest, artifact, kek).await?;
        let content: serde_json::Value = serde_json::from_slice(&plaintext)
            .map_err(|error| BackupError::corrupted(format!("bundled record for key '{key_id}' does not decode: {error}")))?;
        if !content.is_object() {
            return Err(BackupError::corrupted(format!("bundled record for key '{key_id}' is not a KV object")).into());
        }

        records.push(DecodedVaultRecord {
            key_id,
            stage,
            kv_version: reference.kv_version,
            content,
        });
    }

    for reference in &references.kv_records {
        if !records.iter().any(|record| record.key_id == reference.key_id) {
            return Err(BackupError::missing_artifact(format!("{VAULT_RECORD_ARTIFACT_DIR}/{}", reference.key_id)).into());
        }
    }

    // Deterministic order: stage first (the restore order), then key id, so a
    // restore issues the same request sequence on every run.
    records.sort_by(|left, right| (left.stage, &left.key_id).cmp(&(right.stage, &right.key_id)));

    Ok(DecodedVaultBundle {
        manifest,
        references,
        records,
    })
}

/// Extract and validate the key id from a bundle record artifact path.
fn vault_record_key_id(artifact_path: &str) -> Result<String> {
    let key_id = artifact_path
        .strip_prefix(&format!("{VAULT_RECORD_ARTIFACT_DIR}/"))
        .and_then(|name| name.strip_suffix(VAULT_RECORD_ARTIFACT_SUFFIX))
        .ok_or_else(|| {
            BackupError::corrupted(format!(
                "vault record artifact path {artifact_path:?} does not follow the \
                 '{VAULT_RECORD_ARTIFACT_DIR}/<key_id>{VAULT_RECORD_ARTIFACT_SUFFIX}' layout"
            ))
        })?
        .to_string();
    validate_key_id(&key_id)?;
    if key_id == VAULT_RESTORE_MARKER_KEY {
        return Err(BackupError::corrupted(format!(
            "bundle names a key id colliding with the reserved restore marker path {VAULT_RESTORE_MARKER_KEY:?}"
        ))
        .into());
    }
    Ok(key_id)
}

fn record_kv_path(prefix: &str, key_id: &str) -> String {
    format!("{prefix}/{key_id}")
}

fn marker_kv_path(prefix: &str) -> String {
    format!("{prefix}/{VAULT_RESTORE_MARKER_KEY}")
}

/// Compare every coordinate that is decidable from the bundle and the caller's
/// target description alone: cluster identity, namespace, and all three mount
/// paths. Issues no request of any kind.
///
/// Returns the Transit coordinates to probe only when they agree, so a caller
/// cannot accidentally probe a mount the bundle does not describe.
fn compare_coordinates<'a>(
    references: &'a VaultExternalReferences,
    target: &'a VaultRestoreTarget,
) -> (Vec<VaultRestoreMismatch>, Option<(&'a VaultTransitReference, &'a str)>) {
    let mut mismatches = Vec::new();

    if references.cluster_id != target.cluster_id {
        mismatches.push(VaultRestoreMismatch::ClusterIdentity {
            expected: references.cluster_id.clone(),
            observed: target.cluster_id.clone(),
        });
    }
    if references.namespace != target.namespace {
        mismatches.push(VaultRestoreMismatch::Namespace {
            expected: references.namespace.clone(),
            observed: target.namespace.clone(),
        });
    }
    for (dependency, expected, observed) in [
        ("vault kv mount", &references.kv_mount, &target.kv_mount),
        ("vault kv path prefix", &references.kv_path_prefix, &target.kv_path_prefix),
    ] {
        if expected != observed {
            mismatches.push(VaultRestoreMismatch::Mount {
                dependency: dependency.to_string(),
                expected: expected.clone(),
                observed: observed.clone(),
            });
        }
    }

    let probe = match (references.transit.as_ref(), target.transit_mount.as_deref()) {
        (None, _) => None,
        (Some(transit), Some(mount)) if transit.mount_path == mount => Some((transit, mount)),
        (Some(transit), observed) => {
            mismatches.push(VaultRestoreMismatch::Mount {
                dependency: "vault transit mount".to_string(),
                expected: transit.mount_path.clone(),
                observed: observed.unwrap_or("<none>").to_string(),
            });
            None
        }
    };
    (mismatches, probe)
}

/// Verify the recovered external trust root against the bundle's references.
///
/// Locally decidable coordinates are settled first and, when any of them
/// disagrees, the verdict is returned without issuing a single Vault request:
/// a target whose coordinates already drifted is by definition not the one the
/// bundle describes, and probing it would both read through the wrong mount
/// and touch a deployment this restore has no business talking to. Only once
/// they all agree does the Transit read happen. Reads only.
async fn verify_trust_root(
    client: &VaultRestoreClient,
    references: &VaultExternalReferences,
    target: &VaultRestoreTarget,
) -> Result<Vec<VaultRestoreMismatch>> {
    let (mut mismatches, probe) = compare_coordinates(references, target);
    if !mismatches.is_empty() {
        return Ok(mismatches);
    }
    // Every remaining check needs the network; nothing above this line did.
    let Some((transit, target_mount)) = probe else {
        return Ok(mismatches);
    };

    match client.read_transit_key(target_mount, &transit.key_name).await? {
        None => mismatches.push(VaultRestoreMismatch::TransitKeyMissing {
            mount_path: target_mount.to_string(),
            key_name: transit.key_name.clone(),
        }),
        Some(observed) => {
            if observed.min_decryption_version > u64::from(transit.required_min_version) {
                mismatches.push(VaultRestoreMismatch::TransitMinDecryptionVersionTooHigh {
                    key_name: transit.key_name.clone(),
                    required: transit.required_min_version,
                    observed: observed.min_decryption_version,
                });
            }
            if observed.latest_version < transit.current_version {
                mismatches.push(VaultRestoreMismatch::TransitVersionHistoryRegressed {
                    key_name: transit.key_name.clone(),
                    expected_current: transit.current_version,
                    observed_latest: observed.latest_version,
                });
            }
        }
    }
    Ok(mismatches)
}

/// What the restore must do with one bundled record.
enum RecordPlan {
    /// The target holds nothing at this path; publish it.
    Publish,
    /// The target already holds exactly this content — either it pre-existed
    /// or an interrupted attempt wrote it. Either way, nothing to do.
    AlreadyApplied,
    /// The target holds different content and the restore must not overwrite.
    Conflict(RestoreConflict),
    /// The target's Vault sits at an earlier point than the bundle.
    Mismatch(VaultRestoreMismatch),
}

/// Classify one record against the target, reading only.
///
/// Content equality is checked before generations: after a crash the records
/// this restore already wrote sit at generation 1 while the bundle references
/// a much higher one, and treating that as a rollback would make roll-forward
/// impossible.
async fn plan_record(client: &VaultRestoreClient, prefix: &str, record: &DecodedVaultRecord) -> Result<RecordPlan> {
    let path = record_kv_path(prefix, &record.key_id);
    let Some(observed) = client.read_kv_record(&path).await? else {
        return Ok(RecordPlan::Publish);
    };
    if observed.content == record.content {
        return Ok(RecordPlan::AlreadyApplied);
    }

    Ok(match observed.generation.cmp(&record.kv_version) {
        std::cmp::Ordering::Greater => RecordPlan::Conflict(RestoreConflict {
            key_id: record.key_id.clone(),
            kind: RestoreConflictKind::VersionRegression,
            detail: format!(
                "target holds generation {} of key '{}', ahead of the bundle's generation {}; \
                 restoring would roll it back",
                observed.generation, record.key_id, record.kv_version
            ),
        }),
        std::cmp::Ordering::Less => RecordPlan::Mismatch(VaultRestoreMismatch::KvGenerationRegressed {
            key_id: record.key_id.clone(),
            expected: record.kv_version,
            observed: observed.generation,
        }),
        std::cmp::Ordering::Equal => RecordPlan::Conflict(RestoreConflict {
            key_id: record.key_id.clone(),
            kind: RestoreConflictKind::KeyAlreadyExists,
            detail: format!(
                "target holds generation {} of key '{}' with content that differs from the bundle's",
                observed.generation, record.key_id
            ),
        }),
    })
}

async fn read_marker(client: &VaultRestoreClient, prefix: &str) -> Result<Option<PublishedMarker>> {
    match client.read_kv_record(&marker_kv_path(prefix)).await? {
        Some(record) => Ok(Some(PublishedMarker {
            marker: VaultRestoreCommitMarker::from_value(record.content)?,
            kv_version: record.generation,
        })),
        None => Ok(None),
    }
}

/// Zero-write restore preflight: evaluate the bundle against the target and
/// report every blocker, conflict, and external dependency mismatch.
///
/// The bundle is decoded fully in memory (including AEAD verification of every
/// artifact); the target Vault is only read. Environment failures surface as
/// errors, verdicts about the bundle or the target surface inside the report.
pub async fn dry_run_vault_restore(
    kek: &BackupKek,
    client: &VaultRestoreClient,
    request: &VaultRestoreRequest,
) -> Result<RestoreDryRunReport> {
    request.validate()?;
    let mut report = RestoreDryRunReport {
        backup_id: String::new(),
        target_deployment_identity: request.target_deployment_identity.clone(),
        blockers: Vec::new(),
        conflicts: Vec::new(),
        external_mismatches: Vec::new(),
    };

    let decoded = match decode_vault_bundle(&request.bundle_dir, kek).await {
        Ok(decoded) => decoded,
        Err(error) => match blocker_for(&error) {
            Some(blocker) => {
                report.blockers.push(blocker);
                return Ok(report);
            }
            None => return Err(error),
        },
    };
    report.backup_id = decoded.manifest.backup_id.clone();

    if decoded.manifest.deployment_identity != request.target_deployment_identity {
        report.blockers.push(RestoreBlocker {
            code: RestoreBlockerCode::DeploymentMismatch,
            detail: format!(
                "bundle was produced by deployment '{}', restore target is '{}'",
                decoded.manifest.deployment_identity, request.target_deployment_identity
            ),
        });
    }
    if let Some(observed) = request.observed_generation
        && decoded.manifest.snapshot_generation < observed
    {
        report.conflicts.push(RestoreConflict {
            key_id: WHOLE_TARGET_CONFLICT_ID.to_string(),
            kind: RestoreConflictKind::GenerationRegression,
            detail: format!(
                "target observed snapshot generation {observed}, bundle carries {}; equal generations stay allowed for repeated drills",
                decoded.manifest.snapshot_generation
            ),
        });
    }

    let mismatches = verify_trust_root(client, &decoded.references, &request.target).await?;
    let trust_root_intact = mismatches.is_empty();
    for mismatch in &mismatches {
        mismatch.record_in(&mut report);
    }

    // Probing per-key state through a mount set the bundle does not describe
    // would report conflicts about unrelated records, so the target-state
    // section is only meaningful once the coordinates agree.
    if !trust_root_intact {
        return Ok(report);
    }

    let prefix = request.target.kv_path_prefix.as_str();
    let marker = match read_marker(client, prefix).await {
        Ok(marker) => marker,
        Err(error) => {
            report.conflicts.push(RestoreConflict {
                key_id: VAULT_RESTORE_MARKER_KEY.to_string(),
                kind: RestoreConflictKind::KeyAlreadyExists,
                detail: format!("target holds an undecodable restore marker: {error}"),
            });
            return Ok(report);
        }
    };
    if let Some(published) = &marker
        && !published.marker.matches_bundle(&decoded.manifest)
    {
        report.conflicts.push(RestoreConflict {
            key_id: VAULT_RESTORE_MARKER_KEY.to_string(),
            kind: RestoreConflictKind::KeyAlreadyExists,
            detail: format!("target holds a restore marker for a different bundle ('{}')", published.marker.backup_id),
        });
        return Ok(report);
    }

    for record in selected_records(&decoded, marker.as_ref().map(|published| &published.marker)) {
        match plan_record(client, prefix, record).await? {
            RecordPlan::Publish | RecordPlan::AlreadyApplied => {}
            RecordPlan::Conflict(conflict) => report.conflicts.push(conflict),
            RecordPlan::Mismatch(mismatch) => mismatch.record_in(&mut report),
        }
    }
    Ok(report)
}

/// The records an attempt is responsible for: everything in the bundle on a
/// fresh run, and exactly what a published marker names on a re-entry — a
/// record the first run found already present must stay untouched.
fn selected_records<'a>(
    decoded: &'a DecodedVaultBundle,
    marker: Option<&VaultRestoreCommitMarker>,
) -> Vec<&'a DecodedVaultRecord> {
    match marker {
        Some(marker) => decoded
            .records
            .iter()
            .filter(|record| marker.key_ids.contains(&record.key_id))
            .collect(),
        None => decoded.records.iter().collect(),
    }
}

/// Restore a sealed Vault bundle into the target deployment.
///
/// Fail-fast counterpart of the dry-run: the first violated precondition
/// aborts with its typed error. Only [`RestoreConflictPolicy::RestoreIntoEmptyTarget`]
/// ever writes, and even then every write is create-only.
pub async fn restore_vault_backup(
    kek: &BackupKek,
    client: &VaultRestoreClient,
    request: &VaultRestoreRequest,
) -> Result<VaultRestoreReport> {
    request.validate()?;
    if request.conflict_policy == RestoreConflictPolicy::Fail {
        return Err(KmsError::invalid_operation(
            "restore conflict policy 'fail' never writes to the target; review the dry-run report and \
             re-run with the 'restore-into-empty-target' policy to proceed",
        ));
    }

    let decoded = decode_vault_bundle(&request.bundle_dir, kek).await?;
    if decoded.manifest.deployment_identity != request.target_deployment_identity {
        return Err(KmsError::invalid_operation(format!(
            "bundle was produced by deployment '{}' but the restore target is '{}'",
            decoded.manifest.deployment_identity, request.target_deployment_identity
        )));
    }
    if let Some(observed) = request.observed_generation
        && decoded.manifest.snapshot_generation < observed
    {
        return Err(KmsError::invalid_operation(format!(
            "restore would regress the snapshot generation: target observed {observed}, bundle carries {}",
            decoded.manifest.snapshot_generation
        )));
    }

    let mut sequence = VaultRestoreSequence::new();

    // Stage 1: the external trust root must be back and provably the right
    // one before any RustFS-side state is written.
    sequence.enter(VaultRestoreStage::ExternalTrustRoot)?;
    if let Some(mismatch) = verify_trust_root(client, &decoded.references, &request.target)
        .await?
        .into_iter()
        .next()
    {
        return Err(mismatch.into());
    }
    sequence.complete(VaultRestoreStage::ExternalTrustRoot)?;

    let prefix = request.target.kv_path_prefix.as_str();
    let existing_marker = read_marker(client, prefix).await?;
    if let Some(published) = &existing_marker
        && !published.marker.matches_bundle(&decoded.manifest)
    {
        return Err(KmsError::invalid_operation(format!(
            "target holds a restore marker for a different bundle ('{}'); roll that restore forward \
             with its own bundle or abort it explicitly",
            published.marker.backup_id
        )));
    }

    let mut plans = Vec::new();
    for record in selected_records(&decoded, existing_marker.as_ref().map(|published| &published.marker)) {
        match plan_record(client, prefix, record).await? {
            RecordPlan::Publish => plans.push((record, true)),
            RecordPlan::AlreadyApplied => plans.push((record, false)),
            RecordPlan::Conflict(conflict) => {
                return Err(KmsError::invalid_operation(format!(
                    "restore refused, {:?} conflict on key '{}': {}",
                    conflict.kind, conflict.key_id, conflict.detail
                )));
            }
            RecordPlan::Mismatch(mismatch) => return Err(mismatch.into()),
        }
    }

    // The marker is the commit point: it names exactly the records this
    // restore is responsible for, computed before the first write and never
    // recomputed on re-entry.
    let (mut marker, mut marker_version, resumed) = match existing_marker {
        Some(published) => (published.marker, published.kv_version, true),
        None => {
            let key_ids = plans
                .iter()
                .filter(|(_, publish)| *publish)
                .map(|(record, _)| record.key_id.clone())
                .collect();
            let marker = VaultRestoreCommitMarker::new(&decoded.manifest, key_ids);
            let Some(version) = client
                .create_kv_record(&marker_kv_path(prefix), &marker.to_content()?)
                .await?
            else {
                return Err(KmsError::invalid_operation(
                    "another restore published a commit marker for this target concurrently; \
                     resolve it before retrying",
                ));
            };
            (marker, version, false)
        }
    };

    for stage in [VaultRestoreStage::MaterialAndVersions, VaultRestoreStage::KvMetadata] {
        sequence.enter(stage)?;
        for (record, publish) in plans.iter().filter(|(record, _)| record.stage == stage) {
            if !*publish || !marker.key_ids.contains(&record.key_id) {
                continue;
            }
            let Some(kv_version) = client
                .create_kv_record(&record_kv_path(prefix, &record.key_id), &record.content)
                .await?
            else {
                return Err(KmsError::invalid_operation(format!(
                    "a concurrent writer created the Vault record for key '{}' during restore; \
                     restore never overwrites, so it stopped before touching it",
                    record.key_id
                )));
            };
            // Record the receipt before moving on: it is what later lets an
            // abort tell this restore's record apart from one that merely sits
            // at the same path, and it is the one write here that is not
            // create-only. Crashing between the two leaves the record without
            // a receipt, which an abort then declines to remove — the safe
            // direction, and roll-forward still recognizes it by content.
            marker.written.push(VaultRestoreWriteReceipt {
                key_id: record.key_id.clone(),
                kv_version,
            });
            marker_version = client
                .update_kv_record(&marker_kv_path(prefix), &marker.to_content()?, marker_version)
                .await?;
        }
        sequence.complete(stage)?;
    }

    // Stage 4: the cutover completes when the marker is gone. Applying the
    // sanitized configuration artifact belongs to the admin layer.
    sequence.enter(VaultRestoreStage::ConfigAndCutover)?;
    client.delete_kv_record(&marker_kv_path(prefix)).await?;
    sequence.complete(VaultRestoreStage::ConfigAndCutover)?;

    let already_present_key_ids = decoded
        .records
        .iter()
        .map(|record| record.key_id.clone())
        .filter(|key_id| !marker.key_ids.contains(key_id))
        .collect();
    Ok(VaultRestoreReport {
        backup_id: decoded.manifest.backup_id.clone(),
        snapshot_generation: decoded.manifest.snapshot_generation,
        restored_key_ids: marker.key_ids,
        already_present_key_ids,
        transit_key_name: decoded.references.transit.map(|transit| transit.key_name),
        resumed,
    })
}

/// Explicitly abort an interrupted Vault restore, returning the target to its
/// pre-restore state.
///
/// Removes only records the marker's write receipts prove this restore created
/// and that still sit at the version it wrote them at; everything else the
/// marker merely intended to write is left in place and reported. The marker
/// itself goes last, so an abort that leaves records behind still clears the
/// in-progress state. Fails closed on a marker it cannot strictly decode.
pub async fn abort_vault_restore(client: &VaultRestoreClient, target: &VaultRestoreTarget) -> Result<VaultRestoreAbortReport> {
    let prefix = target.kv_path_prefix.as_str();
    let Some(published) = read_marker(client, prefix).await? else {
        return Err(KmsError::invalid_operation(format!(
            "no vault restore is in progress under {}/{prefix}",
            target.kv_mount
        )));
    };
    let marker = published.marker;

    let mut removed_key_ids = Vec::new();
    let mut skipped = Vec::new();
    for key_id in &marker.key_ids {
        let Some(written) = marker.receipt_for(key_id) else {
            skipped.push(VaultRestoreAbortSkip {
                key_id: key_id.clone(),
                reason: VaultRestoreAbortSkipReason::NeverWritten,
            });
            continue;
        };
        let path = record_kv_path(prefix, key_id);
        let observed = client.read_kv_generation(&path).await?;
        if observed != Some(written) {
            skipped.push(VaultRestoreAbortSkip {
                key_id: key_id.clone(),
                reason: VaultRestoreAbortSkipReason::NotAtWrittenVersion { written, observed },
            });
            continue;
        }
        client.delete_kv_record(&path).await?;
        removed_key_ids.push(key_id.clone());
    }

    client.delete_kv_record(&marker_kv_path(prefix)).await?;
    Ok(VaultRestoreAbortReport {
        backup_id: marker.backup_id,
        removed_key_ids,
        skipped,
    })
}

/// Serializable outcome of an abort. Identifiers and versions only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultRestoreAbortReport {
    /// Bundle the aborted restore was publishing.
    pub backup_id: String,
    /// Records the abort proved this restore wrote, and took back.
    pub removed_key_ids: Vec<String>,
    /// Records the marker named that the abort deliberately left in place.
    /// A non-empty list means the target still needs an operator's eye.
    pub skipped: Vec<VaultRestoreAbortSkip>,
}

/// One record an abort refused to remove, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultRestoreAbortSkip {
    /// Key whose record was left in place.
    pub key_id: String,
    /// Why ownership could not be proven.
    pub reason: VaultRestoreAbortSkipReason,
}

/// Why an abort could not prove it owns a record the marker names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VaultRestoreAbortSkipReason {
    /// The marker carries no write receipt: the restore never created this
    /// record, so whatever sits at the path is somebody else's.
    NeverWritten,
    /// The path no longer holds the version the restore wrote — it was
    /// modified, or removed, after the fact. `observed` is `None` when the
    /// path holds nothing at all.
    NotAtWrittenVersion { written: u64, observed: Option<u64> },
}

fn blocker_for(error: &KmsError) -> Option<RestoreBlocker> {
    match error {
        KmsError::Backup(inner) => Some(RestoreBlocker::from(inner)),
        KmsError::UnsupportedCapability { .. } => Some(RestoreBlocker {
            code: RestoreBlockerCode::UnsupportedBackend,
            detail: error.to_string(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::scripted_vault::{ScriptedResponse, ScriptedVault};
    use crate::backup::capability::{AtRestProtection, BackupBackendKind, BackupResponsibility};
    use crate::backup::local_export::{AEAD_NONCE_LEN, LOCAL_BUNDLE_MANIFEST_FILE, artifact_aad};
    use crate::backup::manifest::{
        AeadAlgorithm, ArtifactDescriptor, CompletenessState, DigestAlgorithm, VaultKvRecordReference, VaultTransitReference,
    };
    use aes_gcm::{
        Nonce,
        aead::{Aead, Payload},
    };
    use std::time::Duration;
    use tempfile::TempDir;

    const DEPLOYMENT: &str = "deployment-test";
    const BACKUP_ID: &str = "backup-0001";
    const SNAPSHOT_GENERATION: u64 = 7;
    const CLUSTER_ID: &str = "vault-cluster-a";
    const KV_MOUNT: &str = "secret";
    const KV_PREFIX: &str = "rustfs/kms/transit-metadata";
    const TRANSIT_MOUNT: &str = "transit";
    const TRANSIT_KEY: &str = "rustfs-master";
    const KEY_ID: &str = "object-key";
    const KV_GENERATION: u64 = 4;

    /// One way a restore target can drift from the bundle's coordinates.
    type TargetDrift = fn(&mut VaultRestoreTarget);

    fn test_kek() -> BackupKek {
        BackupKek::new("backup-kek-test", 1, [0x42; 32]).expect("kek")
    }

    fn record_content() -> serde_json::Value {
        serde_json::json!({ "key_state": "Enabled", "current_version": 3 })
    }

    fn references() -> VaultExternalReferences {
        VaultExternalReferences {
            cluster_id: CLUSTER_ID.to_string(),
            namespace: None,
            kv_mount: KV_MOUNT.to_string(),
            kv_path_prefix: KV_PREFIX.to_string(),
            transit: Some(VaultTransitReference {
                mount_path: TRANSIT_MOUNT.to_string(),
                key_name: TRANSIT_KEY.to_string(),
                required_min_version: 2,
                current_version: 5,
                native_snapshot_reference: None,
            }),
            kv_records: vec![VaultKvRecordReference {
                key_id: KEY_ID.to_string(),
                kv_version: KV_GENERATION,
            }],
        }
    }

    /// Seal a Vault bundle on disk the way a Vault export will: artifacts
    /// first, sealed manifest last.
    async fn write_bundle(dir: &Path, references: VaultExternalReferences, records: &[(&str, serde_json::Value)]) -> PathBuf {
        let bundle = dir.join("bundle");
        tokio::fs::create_dir_all(bundle.join(VAULT_RECORD_ARTIFACT_DIR))
            .await
            .expect("artifact dir");

        let kek = test_kek();
        let mut artifacts = Vec::new();
        for (key_id, content) in records {
            let path = format!("{VAULT_RECORD_ARTIFACT_DIR}/{key_id}{VAULT_RECORD_ARTIFACT_SUFFIX}");
            let plaintext = serde_json::to_vec(content).expect("serialize record");
            let nonce = [0x11u8; AEAD_NONCE_LEN];
            let ciphertext = kek
                .cipher()
                .encrypt(
                    &Nonce::from(nonce),
                    Payload {
                        msg: &plaintext,
                        aad: &artifact_aad(BACKUP_ID, SNAPSHOT_GENERATION, &path),
                    },
                )
                .expect("seal artifact");
            let mut payload = nonce.to_vec();
            payload.extend_from_slice(&ciphertext);
            tokio::fs::write(bundle.join(&path), &payload).await.expect("write artifact");
            artifacts.push(ArtifactDescriptor {
                kind: ArtifactKind::KeyMetadata,
                path,
                len: payload.len() as u64,
                aead_algorithm: AeadAlgorithm::Aes256Gcm,
                encrypted_digest: ContentDigest::sha256_of(&payload),
            });
        }

        let manifest = BackupManifest {
            format_version: BackupManifest::FORMAT_VERSION,
            backup_id: BACKUP_ID.to_string(),
            created_at: "2026-08-01T00:00:00+00:00[UTC]".parse().expect("timestamp"),
            rustfs_version: "1.0.0-test".to_string(),
            deployment_identity: DEPLOYMENT.to_string(),
            backend: BackupBackendKind::VaultTransit,
            at_rest_protection: AtRestProtection::ExternalNonExportable,
            responsibility: BackupResponsibility::MetadataPlusExternalRoot,
            snapshot_generation: SNAPSHOT_GENERATION,
            backup_kek: kek.descriptor(),
            artifacts,
            local_kdf: None,
            external_references: Some(references),
            key_versions: None,
            capability_discovery: None,
            completeness: CompletenessState::InProgress,
            manifest_digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: String::new(),
            },
        };
        let sealed = manifest.seal().expect("seal manifest");
        tokio::fs::write(bundle.join(LOCAL_BUNDLE_MANIFEST_FILE), sealed.encode().expect("encode manifest"))
            .await
            .expect("write manifest");
        bundle
    }

    async fn standard_bundle(dir: &Path) -> PathBuf {
        write_bundle(dir, references(), &[(KEY_ID, record_content())]).await
    }

    fn target(address: &str) -> VaultRestoreTarget {
        VaultRestoreTarget {
            address: address.to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "scripted-token".to_string(),
            },
            namespace: None,
            cluster_id: CLUSTER_ID.to_string(),
            kv_mount: KV_MOUNT.to_string(),
            kv_path_prefix: KV_PREFIX.to_string(),
            transit_mount: Some(TRANSIT_MOUNT.to_string()),
        }
    }

    async fn scripted_client(responses: Vec<ScriptedResponse>) -> (ScriptedVault, VaultRestoreClient, VaultRestoreTarget) {
        let vault = ScriptedVault::serve(responses).await;
        let target = target(&vault.address);
        let kms_config = KmsConfig {
            timeout: Duration::from_secs(5),
            retry_attempts: 1,
            ..KmsConfig::default()
        };
        let client = VaultRestoreClient::connect(&target, &kms_config)
            .await
            .expect("scripted restore client");
        (vault, client, target)
    }

    fn request(bundle_dir: PathBuf, target: VaultRestoreTarget, policy: RestoreConflictPolicy) -> VaultRestoreRequest {
        VaultRestoreRequest {
            bundle_dir,
            target,
            target_deployment_identity: DEPLOYMENT.to_string(),
            observed_generation: None,
            conflict_policy: policy,
        }
    }

    /// Transit key read payload with the given version window.
    fn transit_key_response(min_decryption_version: u64, latest_version: u32) -> ScriptedResponse {
        let keys: serde_json::Map<String, serde_json::Value> = (1..=latest_version)
            .map(|version| (version.to_string(), serde_json::json!(1_700_000_000u64)))
            .collect();
        ScriptedResponse::ok(serde_json::json!({
            "type": "aes256-gcm96",
            "deletion_allowed": false,
            "derived": false,
            "exportable": false,
            "allow_plaintext_backup": false,
            "keys": keys,
            "min_decryption_version": min_decryption_version,
            "min_encryption_version": 0,
            "name": TRANSIT_KEY,
            "supports_encryption": true,
            "supports_decryption": true,
            "supports_derivation": true,
            "supports_signing": false,
        }))
    }

    fn healthy_transit_key() -> ScriptedResponse {
        transit_key_response(1, 5)
    }

    fn kv_metadata_response(current_version: u64) -> ScriptedResponse {
        ScriptedResponse::ok(serde_json::json!({
            "cas_required": false,
            "created_time": "2026-08-01T00:00:00Z",
            "current_version": current_version,
            "delete_version_after": "0s",
            "max_versions": 0,
            "oldest_version": 0,
            "updated_time": "2026-08-01T00:00:00Z",
            "custom_metadata": null,
            "versions": {},
        }))
    }

    fn kv_read_response(content: serde_json::Value, version: u64) -> ScriptedResponse {
        ScriptedResponse::ok(serde_json::json!({
            "data": content,
            "metadata": {
                "created_time": "2026-08-01T00:00:00Z",
                "deletion_time": "",
                "custom_metadata": null,
                "destroyed": false,
                "version": version,
            },
        }))
    }

    fn kv_write_response_at(version: u64) -> ScriptedResponse {
        ScriptedResponse::ok(serde_json::json!({
            "created_time": "2026-08-01T00:00:00Z",
            "deletion_time": "",
            "custom_metadata": null,
            "destroyed": false,
            "version": version,
        }))
    }

    /// The response a create-only write gets: a fresh path lands at version 1.
    fn kv_write_response() -> ScriptedResponse {
        kv_write_response_at(1)
    }

    fn not_found() -> ScriptedResponse {
        ScriptedResponse::error(404, "not found")
    }

    fn empty_ok() -> ScriptedResponse {
        ScriptedResponse::ok(serde_json::json!({}))
    }

    /// A marker as the target would hold it: `key_ids` is what the restore set
    /// out to publish, `written` the receipts for what it actually created.
    fn marker_value(key_ids: &[&str], written: &[(&str, u64)], manifest_digest: &ContentDigest) -> serde_json::Value {
        serde_json::json!({
            "format_version": VAULT_RESTORE_MARKER_FORMAT_VERSION,
            "backup_id": BACKUP_ID,
            "manifest_digest": manifest_digest,
            "key_ids": key_ids,
            "written": written
                .iter()
                .map(|(key_id, kv_version)| serde_json::json!({ "key_id": key_id, "kv_version": kv_version }))
                .collect::<Vec<_>>(),
        })
    }

    fn test_digest() -> ContentDigest {
        ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: "a".repeat(64),
        }
    }

    async fn bundle_manifest(bundle: &Path) -> BackupManifest {
        read_bundle_manifest(bundle).await.expect("manifest")
    }

    #[test]
    fn stage_order_is_enforced() {
        let mut sequence = VaultRestoreSequence::new();
        assert_eq!(sequence.expected(), Some(VaultRestoreStage::ExternalTrustRoot));

        // Skipping ahead, repeating, and completing an unopened stage all fail.
        sequence
            .enter(VaultRestoreStage::MaterialAndVersions)
            .expect_err("material may not run before the trust root");
        sequence
            .complete(VaultRestoreStage::ExternalTrustRoot)
            .expect_err("a stage that was never entered may not complete");

        sequence.enter(VaultRestoreStage::ExternalTrustRoot).expect("first stage");
        sequence
            .enter(VaultRestoreStage::ExternalTrustRoot)
            .expect_err("a stage may not be entered twice");
        sequence.complete(VaultRestoreStage::ExternalTrustRoot).expect("complete");

        sequence
            .enter(VaultRestoreStage::KvMetadata)
            .expect_err("metadata may not run before material");
        for stage in [
            VaultRestoreStage::MaterialAndVersions,
            VaultRestoreStage::KvMetadata,
            VaultRestoreStage::ConfigAndCutover,
        ] {
            sequence.enter(stage).expect("in-order stage");
            sequence.complete(stage).expect("in-order completion");
        }
        assert_eq!(sequence.expected(), None);
        sequence
            .enter(VaultRestoreStage::ExternalTrustRoot)
            .expect_err("the sequence does not restart");
    }

    #[tokio::test]
    async fn dry_run_writes_nothing_and_reports_a_clean_target() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            not_found(), // marker absent
            not_found(), // key record absent
        ])
        .await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(report.restore_permitted(), "clean target must permit restore: {report:?}");

        // The zero-write contract, asserted at the wire: a dry-run may only
        // ever read.
        for line in vault.requests() {
            assert!(
                line.starts_with("GET ") || line.starts_with("LIST "),
                "dry-run issued a non-read request: {line}"
            );
        }
    }

    #[tokio::test]
    async fn dry_run_classifies_a_missing_transit_key() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![not_found()]).await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        assert_eq!(report.blockers.len(), 1);
        assert_eq!(report.blockers[0].code, RestoreBlockerCode::ExternalDependencyUnavailable);
        assert!(report.blockers[0].detail.contains(TRANSIT_KEY));
    }

    #[tokio::test]
    async fn dry_run_classifies_a_raised_min_decryption_version() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        // The bundle still needs version 2; the recovered key refuses anything
        // below 4.
        let (_vault, client, target) = scripted_client(vec![transit_key_response(4, 5)]).await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        assert_eq!(report.external_mismatches.len(), 1);
        assert_eq!(report.external_mismatches[0].expected, "min_decryption_version<=2");
        assert_eq!(report.external_mismatches[0].observed, "min_decryption_version=4");
    }

    #[tokio::test]
    async fn dry_run_classifies_a_regressed_transit_version_history() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        // Native restore landed on a snapshot with only 3 of the 5 versions.
        let (_vault, client, target) = scripted_client(vec![transit_key_response(1, 3)]).await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        assert_eq!(report.external_mismatches[0].expected, "latest_version>=5");
        assert_eq!(report.external_mismatches[0].observed, "latest_version=3");
    }

    #[tokio::test]
    async fn dry_run_classifies_namespace_and_mount_drift() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        // No Vault call at all: coordinate drift is decided before any probe.
        let (vault, client, mut target) = scripted_client(Vec::new()).await;
        target.namespace = Some("tenant-b".to_string());
        target.kv_mount = "other-kv".to_string();
        target.transit_mount = Some("other-transit".to_string());

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        let dependencies: Vec<&str> = report
            .external_mismatches
            .iter()
            .map(|mismatch| mismatch.dependency.as_str())
            .collect();
        assert_eq!(dependencies, ["vault namespace", "vault kv mount", "vault transit mount"]);
        assert!(vault.requests().is_empty(), "coordinate drift must not probe the target");
    }

    #[tokio::test]
    async fn every_locally_decidable_drift_is_settled_without_a_vault_request() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;

        // Each of these leaves the Transit mount agreeing with the bundle, so
        // only the ordering of the checks — coordinates first, network second
        // — can keep the Transit read from going out.
        let drifts: [(&str, TargetDrift); 4] = [
            ("vault cluster identity", |target| target.cluster_id = "vault-cluster-b".to_string()),
            ("vault namespace", |target| target.namespace = Some("tenant-b".to_string())),
            ("vault kv mount", |target| target.kv_mount = "other-kv".to_string()),
            ("vault kv path prefix", |target| {
                target.kv_path_prefix = "rustfs/kms/elsewhere".to_string()
            }),
        ];

        for (dependency, drift) in drifts {
            let (vault, client, mut target) = scripted_client(Vec::new()).await;
            drift(&mut target);

            let report =
                dry_run_vault_restore(&test_kek(), &client, &request(bundle.clone(), target, RestoreConflictPolicy::Fail))
                    .await
                    .expect("dry-run should report rather than fail");

            assert!(!report.restore_permitted(), "{dependency} drift must refuse the restore");
            let dependencies: Vec<&str> = report
                .external_mismatches
                .iter()
                .map(|mismatch| mismatch.dependency.as_str())
                .collect();
            assert_eq!(dependencies, [dependency]);
            assert!(
                vault.requests().is_empty(),
                "{dependency} drift probed the target: {:?}",
                vault.requests()
            );
        }
    }

    #[tokio::test]
    async fn dry_run_classifies_a_rolled_back_kv_generation() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            not_found(), // marker absent
            kv_metadata_response(2),
            kv_read_response(serde_json::json!({ "key_state": "Enabled", "current_version": 1 }), 2),
        ])
        .await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        assert_eq!(report.external_mismatches[0].dependency, format!("vault kv record for key {KEY_ID}"));
        assert_eq!(report.external_mismatches[0].expected, "kv_generation>=4");
        assert_eq!(report.external_mismatches[0].observed, "kv_generation=2");
    }

    #[tokio::test]
    async fn dry_run_reports_a_target_that_moved_ahead_of_the_bundle() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            not_found(),
            kv_metadata_response(9),
            kv_read_response(serde_json::json!({ "key_state": "Disabled", "current_version": 8 }), 9),
        ])
        .await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should succeed");
        assert!(!report.restore_permitted());
        assert_eq!(report.conflicts[0].kind, RestoreConflictKind::VersionRegression);
        assert_eq!(report.conflicts[0].key_id, KEY_ID);
    }

    #[tokio::test]
    async fn fail_policy_never_writes() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(Vec::new()).await;

        restore_vault_backup(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect_err("the default policy refuses to write");
        assert!(vault.requests().is_empty(), "the refusal must precede every Vault call");
    }

    #[tokio::test]
    async fn restore_publishes_records_in_stage_order_and_clears_the_marker() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            not_found(),             // marker absent
            not_found(),             // key record absent
            kv_write_response(),     // marker published
            kv_write_response(),     // record published
            kv_write_response_at(2), // write receipt appended to the marker
            empty_ok(),              // marker removed
        ])
        .await;

        let report = restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect("restore should succeed");
        assert_eq!(report.restored_key_ids, [KEY_ID]);
        assert!(report.already_present_key_ids.is_empty());
        assert!(!report.resumed);
        assert_eq!(report.transit_key_name.as_deref(), Some(TRANSIT_KEY));

        let marker_path = format!("POST /v1/{KV_MOUNT}/data/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}");
        let record_path = format!("POST /v1/{KV_MOUNT}/data/{KV_PREFIX}/{KEY_ID}");
        let requests = vault.requests();
        assert_eq!(
            requests,
            [
                format!("GET /v1/{TRANSIT_MOUNT}/keys/{TRANSIT_KEY}"),
                format!("GET /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
                format!("GET /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{KEY_ID}"),
                marker_path.clone(),
                record_path.clone(),
                marker_path,
                format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
            ],
            "the trust root is verified first, the marker brackets every write, \
             and each published record is receipted before the run moves on"
        );

        let writes: Vec<(String, serde_json::Value)> = requests
            .iter()
            .zip(vault.request_bodies())
            .filter(|(_, body)| !body.is_empty())
            .map(|(line, body)| (line.clone(), serde_json::from_str(&body).expect("write body is JSON")))
            .collect();

        // Record writes are create-only: restore can structurally not
        // overwrite one. The marker is the sole exception, and even it moves
        // only by check-and-set against the version just observed.
        for (line, body) in &writes {
            let expected_cas = if *line == record_path || body["data"]["written"] == serde_json::json!([]) {
                0
            } else {
                1
            };
            assert_eq!(body["options"]["cas"], expected_cas, "unexpected check-and-set on {line}: {body}");
        }

        let receipt = &writes.last().expect("the marker receipt is the last write").1;
        assert_eq!(
            receipt["data"]["written"],
            serde_json::json!([{ "key_id": KEY_ID, "kv_version": 1 }]),
            "the marker must record the version the record write landed at"
        );
    }

    #[tokio::test]
    async fn restore_refuses_the_trust_root_before_touching_kv() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(vec![transit_key_response(4, 5)]).await;

        let error = restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect_err("a raised min_decryption_version must abort the restore");
        assert!(error.to_string().contains("min_decryption_version"), "{error}");
        assert_eq!(
            vault.requests(),
            [format!("GET /v1/{TRANSIT_MOUNT}/keys/{TRANSIT_KEY}")],
            "the restore must stop at the trust-root stage"
        );
    }

    #[tokio::test]
    async fn a_lost_check_and_set_race_stops_the_restore() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            not_found(),
            not_found(),
            kv_write_response(),
            ScriptedResponse::error(400, "check-and-set parameter did not match the current version"),
        ])
        .await;

        let error = restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect_err("a concurrent writer must stop the restore");
        assert!(error.to_string().contains("concurrent writer"), "{error}");
        // The write was attempted exactly once: a create-only write is never
        // replayed, and the marker stays published for roll-forward or abort.
        assert_eq!(
            vault
                .requests()
                .iter()
                .filter(|line| *line == &format!("POST /v1/{KV_MOUNT}/data/{KV_PREFIX}/{KEY_ID}"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn re_entry_after_a_crash_between_writes_is_idempotent() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let manifest = bundle_manifest(&bundle).await;
        // The interrupted attempt published the record and receipted it.
        let marker = marker_value(&[KEY_ID], &[(KEY_ID, 1)], &manifest.manifest_digest);
        let (vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            kv_metadata_response(2), // marker present
            kv_read_response(marker, 2),
            kv_metadata_response(1), // the record this run already wrote
            kv_read_response(record_content(), 1),
            empty_ok(), // marker removed
        ])
        .await;

        let report = restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect("roll-forward should succeed");
        assert!(report.resumed);
        assert_eq!(report.restored_key_ids, [KEY_ID]);
        // The already-published record is recognized by content, not by
        // generation: it sits at generation 1 while the bundle references 4.
        assert!(
            !vault.requests().iter().any(|line| line.starts_with("POST")),
            "a completed record must not be rewritten: {:?}",
            vault.requests()
        );
    }

    #[tokio::test]
    async fn a_marker_for_another_bundle_fails_closed() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let mut foreign = marker_value(&[KEY_ID], &[], &bundle_manifest(&bundle).await.manifest_digest);
        foreign["backup_id"] = serde_json::json!("backup-other");
        let (_vault, client, target) =
            scripted_client(vec![healthy_transit_key(), kv_metadata_response(1), kv_read_response(foreign, 1)]).await;

        let error = restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect_err("a foreign marker must fail closed");
        assert!(error.to_string().contains("backup-other"), "{error}");
    }

    #[tokio::test]
    async fn an_undecodable_marker_fails_closed() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![
            healthy_transit_key(),
            kv_metadata_response(1),
            kv_read_response(serde_json::json!({ "format_version": 99, "backup_id": BACKUP_ID }), 1),
        ])
        .await;

        restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect_err("an out-of-contract marker must fail closed");
    }

    #[tokio::test]
    async fn abort_takes_back_exactly_the_records_it_receipted() {
        let (vault, client, target) = scripted_client(vec![
            kv_metadata_response(2),
            kv_read_response(marker_value(&[KEY_ID], &[(KEY_ID, 1)], &test_digest()), 2),
            kv_metadata_response(1), // the record still sits at the version we wrote
            empty_ok(),              // record removed
            empty_ok(),              // marker removed
        ])
        .await;

        let report = abort_vault_restore(&client, &target).await.expect("abort should succeed");
        assert_eq!(report.removed_key_ids, [KEY_ID]);
        assert!(report.skipped.is_empty(), "{report:?}");
        assert_eq!(
            vault.requests(),
            [
                format!("GET /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
                format!("GET /v1/{KV_MOUNT}/data/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
                format!("GET /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{KEY_ID}"),
                format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{KEY_ID}"),
                format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
            ],
            "ownership is re-checked at the path before anything is removed"
        );
    }

    /// The marker names what a restore *meant* to write, which after a crash
    /// or a lost race is not what it wrote. Neither case may be deleted.
    #[tokio::test]
    async fn abort_skips_every_record_it_cannot_prove_it_wrote() {
        const CRASHED_BEFORE: &str = "second-key";
        const OVERWRITTEN: &str = "third-key";

        let (vault, client, target) = scripted_client(vec![
            kv_metadata_response(3),
            kv_read_response(
                marker_value(
                    &[KEY_ID, CRASHED_BEFORE, OVERWRITTEN],
                    // No receipt for CRASHED_BEFORE: the restore died before
                    // reaching it. OVERWRITTEN was written, then moved on by
                    // somebody else.
                    &[(KEY_ID, 1), (OVERWRITTEN, 1)],
                    &test_digest(),
                ),
                3,
            ),
            kv_metadata_response(1), // KEY_ID still at the version we wrote
            empty_ok(),              // KEY_ID removed
            kv_metadata_response(4), // OVERWRITTEN has moved past our version
            empty_ok(),              // marker removed
        ])
        .await;

        let report = abort_vault_restore(&client, &target).await.expect("abort should succeed");
        assert_eq!(report.removed_key_ids, [KEY_ID]);
        assert_eq!(
            report.skipped,
            [
                VaultRestoreAbortSkip {
                    key_id: CRASHED_BEFORE.to_string(),
                    reason: VaultRestoreAbortSkipReason::NeverWritten,
                },
                VaultRestoreAbortSkip {
                    key_id: OVERWRITTEN.to_string(),
                    reason: VaultRestoreAbortSkipReason::NotAtWrittenVersion {
                        written: 1,
                        observed: Some(4),
                    },
                },
            ]
        );

        let requests = vault.requests();
        let deletes: Vec<&str> = requests
            .iter()
            .filter(|line| line.starts_with("DELETE "))
            .map(String::as_str)
            .collect();
        assert_eq!(
            deletes,
            [
                format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{KEY_ID}"),
                format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{VAULT_RESTORE_MARKER_KEY}"),
            ],
            "only the receipted, unchanged record and the marker may be removed"
        );
    }

    /// The review case: the marker is published, the record write then loses
    /// the create-only race, and the abort that follows must leave the
    /// winner's record where it is.
    #[tokio::test]
    async fn abort_after_a_lost_write_race_leaves_the_other_writer_alone() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (vault, client, target) = scripted_client(vec![
            // Restore: trust root, empty target, marker published, then the
            // record write is beaten to the path.
            healthy_transit_key(),
            not_found(),
            not_found(),
            kv_write_response(),
            ScriptedResponse::error(400, "check-and-set parameter did not match the current version"),
            // Abort: the marker is there and still carries no receipt.
            kv_metadata_response(1),
            kv_read_response(marker_value(&[KEY_ID], &[], &bundle_manifest(&bundle).await.manifest_digest), 1),
            empty_ok(), // marker removed
        ])
        .await;

        restore_vault_backup(
            &test_kek(),
            &client,
            &request(bundle, target.clone(), RestoreConflictPolicy::RestoreIntoEmptyTarget),
        )
        .await
        .expect_err("a concurrent writer must stop the restore");

        let report = abort_vault_restore(&client, &target).await.expect("abort should succeed");
        assert!(report.removed_key_ids.is_empty(), "{report:?}");
        assert_eq!(
            report.skipped,
            [VaultRestoreAbortSkip {
                key_id: KEY_ID.to_string(),
                reason: VaultRestoreAbortSkipReason::NeverWritten,
            }]
        );
        assert!(
            !vault
                .requests()
                .contains(&format!("DELETE /v1/{KV_MOUNT}/metadata/{KV_PREFIX}/{KEY_ID}")),
            "the abort deleted a record this restore never wrote: {:?}",
            vault.requests()
        );
    }

    #[tokio::test]
    async fn abort_without_a_marker_is_refused() {
        let (_vault, client, target) = scripted_client(vec![not_found()]).await;
        abort_vault_restore(&client, &target)
            .await
            .expect_err("aborting without an in-flight restore must be refused");
    }

    #[tokio::test]
    async fn a_bundle_referencing_a_record_it_does_not_carry_is_rejected() {
        let temp = TempDir::new().expect("temp dir");
        let mut references = references();
        references.kv_records.push(VaultKvRecordReference {
            key_id: "ghost-key".to_string(),
            kv_version: 1,
        });
        let bundle = write_bundle(temp.path(), references, &[(KEY_ID, record_content())]).await;
        let (_vault, client, target) = scripted_client(Vec::new()).await;

        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should report rather than fail");
        assert_eq!(report.blockers[0].code, RestoreBlockerCode::MissingArtifact);
    }

    #[tokio::test]
    async fn a_tampered_artifact_is_rejected_before_the_target_is_probed() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let artifact = bundle.join(format!("{VAULT_RECORD_ARTIFACT_DIR}/{KEY_ID}{VAULT_RECORD_ARTIFACT_SUFFIX}"));
        let mut payload = tokio::fs::read(&artifact).await.expect("read artifact");
        let last = payload.len() - 1;
        payload[last] ^= 0xff;
        tokio::fs::write(&artifact, &payload).await.expect("tamper artifact");

        let (vault, client, target) = scripted_client(Vec::new()).await;
        let report = dry_run_vault_restore(&test_kek(), &client, &request(bundle, target, RestoreConflictPolicy::Fail))
            .await
            .expect("dry-run should report rather than fail");
        assert_eq!(report.blockers[0].code, RestoreBlockerCode::BundleCorrupted);
        assert!(vault.requests().is_empty(), "a broken bundle must not reach the target");
    }

    #[tokio::test]
    async fn a_bundle_from_another_deployment_is_blocked() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![healthy_transit_key(), not_found(), not_found()]).await;
        let mut request = request(bundle, target, RestoreConflictPolicy::Fail);
        request.target_deployment_identity = "deployment-other".to_string();

        let report = dry_run_vault_restore(&test_kek(), &client, &request)
            .await
            .expect("dry-run should succeed");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::DeploymentMismatch)
        );
    }

    #[tokio::test]
    async fn a_generation_rollback_is_reported_and_refused() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let (_vault, client, target) = scripted_client(vec![healthy_transit_key(), not_found(), not_found()]).await;
        let mut request = request(bundle, target, RestoreConflictPolicy::Fail);
        request.observed_generation = Some(SNAPSHOT_GENERATION + 1);

        let report = dry_run_vault_restore(&test_kek(), &client, &request)
            .await
            .expect("dry-run should succeed");
        assert!(
            report
                .conflicts
                .iter()
                .any(|conflict| conflict.kind == RestoreConflictKind::GenerationRegression)
        );
    }

    /// End-to-end drill against a real Vault. Never run in CI (no server);
    /// kept compiling so the wiring cannot rot.
    #[tokio::test]
    #[ignore = "requires a live Vault with a restored Transit trust root"]
    async fn real_vault_restore_round_trip() {
        let temp = TempDir::new().expect("temp dir");
        let bundle = standard_bundle(temp.path()).await;
        let mut target = target("http://127.0.0.1:8200");
        target.auth_method = VaultAuthMethod::Token {
            token: std::env::var("VAULT_TOKEN").expect("VAULT_TOKEN"),
        };
        let client = VaultRestoreClient::connect(&target, &KmsConfig::default())
            .await
            .expect("connect to Vault");

        let request = request(bundle, target, RestoreConflictPolicy::RestoreIntoEmptyTarget);
        let preflight = dry_run_vault_restore(&test_kek(), &client, &request)
            .await
            .expect("dry-run should succeed");
        assert!(preflight.restore_permitted(), "{preflight:?}");
        let report = restore_vault_backup(&test_kek(), &client, &request)
            .await
            .expect("restore should succeed");
        assert_eq!(report.restored_key_ids, [KEY_ID]);
    }
}
