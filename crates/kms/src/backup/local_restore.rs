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

//! Local backend restore: consume a sealed backup bundle into a key directory.
//!
//! The consumer side of [`crate::backup::local_export`]. The admin API is not
//! wired here — callers construct the request and supply the backup KEK and
//! the operator-re-supplied master key explicitly. The bundle source is
//! strictly read-only throughout: restore never writes to or deletes from it.
//!
//! # Four-phase protocol
//!
//! 1. **Dry-run (zero writes)** — [`dry_run_local_restore`] decodes the full
//!    bundle chain in memory, checks the KDF descriptor against this build,
//!    enumerates target conflicts, and verifies the operator-supplied master
//!    key against the manifest's one-way verifier. It writes nothing.
//! 2. **Staging** — decrypted artifacts are committed durably into the
//!    `.restore-staging/` subdirectory of the target. The leading dot and the
//!    directory shape keep it invisible to the backend's key scan and its
//!    orphan-temp matcher, so startup recovery can never delete staged state.
//!    Every record is decryption-probed with the derived master key before
//!    and after staging.
//! 3. **Commit marker + cutover** — the durably published
//!    `.restore-commit.json` marker is the single commit point. Cutover
//!    publishes each staged file into the target top level via the no-clobber
//!    [`durable_file::link_durably`] primitive (salt first, keys after), then
//!    durably removes the marker and drops the staging directory.
//! 4. **Crash re-entry** — before the marker, the target top level is
//!    untouched and a re-run starts over; with the marker published, backend
//!    startup fails closed and re-running the restore with the same bundle
//!    rolls the cutover forward, while [`abort_local_restore`] rolls it back.
//!    Every interruption converges to the complete old or complete new state.
//!
//! # Deliberately out of scope
//!
//! The `explicit-remap` conflict policy is not implemented: remapping stable
//! key ids requires proving that object envelopes migrate in lockstep, and a
//! bulk rekey is a non-goal of this change. Local has no rotation history
//! (backlog#1565), so per-key version and deletion-state regression cannot
//! become write operations under the empty-target policy; the snapshot
//! generation check freezes the injected observe-and-reject-strictly-lower
//! semantics for the admin layer.

use crate::backends::local::{
    LOCAL_KMS_MASTER_KEY_SALT_FILE, LOCAL_KMS_MASTER_KEY_SALT_LEN, LOCAL_RESTORE_COMMIT_MARKER_FILE, LocalKmsClient,
    StoredKeyProtection, durable_file, is_orphan_commit_temp_name, validate_key_id,
};
use crate::backup::capability::AtRestProtection;
use crate::backup::dry_run::{
    ExternalDependencyMismatch, RestoreBlocker, RestoreBlockerCode, RestoreConflict, RestoreConflictKind, RestoreDryRunReport,
};
use crate::backup::error::BackupError;
use crate::backup::local_export::{
    BackupKek, MASTER_KEY_VERIFIER_ARGON2ID_PREFIX, MASTER_KEY_VERIFIER_LEGACY_PREFIX, compute_master_key_verifier,
    decrypt_bundle_artifact, fsync_dir, read_local_bundle_manifest,
};
use crate::backup::manifest::{ArtifactKind, BackupManifest, ContentDigest, LocalKeyDerivation};
use crate::error::{KmsError, Result};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::warn;
use zeroize::Zeroizing;

/// Name of the staging subdirectory inside the restore target.
const RESTORE_STAGING_DIR: &str = ".restore-staging";
/// Format version of the cutover commit marker.
const RESTORE_MARKER_FORMAT_VERSION: u32 = 1;
/// Sentinel key id for conflicts that concern the whole target rather than
/// one key (the snapshot generation).
const WHOLE_TARGET_CONFLICT_ID: &str = "*";

/// How restore resolves conflicts with existing target state.
///
/// Silent overwrite or merge is never an option; `explicit-remap` is
/// deliberately not offered (see the module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestoreConflictPolicy {
    /// Never write to the target. The default: any actual restore attempt is
    /// refused, so mutating a key directory always requires the explicit
    /// opt-in below. Dry-runs are unaffected.
    #[default]
    Fail,
    /// Restore only into an empty target. A target counts as non-empty when
    /// it holds any key file, a KDF salt (an orphan salt would poison every
    /// key created later), a marker of a different restore, or any other
    /// unexpected entry; only leftovers of an interrupted run of this same
    /// restore are tolerated.
    RestoreIntoEmptyTarget,
}

/// Parameters of one restore (or dry-run) attempt.
///
/// `observed_generation` is injected by the caller, mirroring the export
/// request's `snapshot_generation`: the admin layer owns where the target's
/// highest observed generation is tracked; this module freezes only the
/// strictly-lower-is-rejected semantics (equal generations stay allowed so
/// drills can be repeated).
pub struct LocalRestoreRequest {
    /// Bundle directory produced by the Local export. Read-only.
    pub bundle_dir: PathBuf,
    /// Key directory to restore into.
    pub target_key_dir: PathBuf,
    /// Deployment identity of the restore target; must match the manifest.
    pub target_deployment_identity: String,
    /// Highest snapshot generation the target has already observed, when the
    /// caller tracks one.
    pub observed_generation: Option<u64>,
    /// Operator-re-supplied master key string. The master key is outside the
    /// backup domain, so it must arrive out of band; required whenever the
    /// bundle carries encrypted-at-rest records.
    pub master_key: Option<String>,
    /// Conflict policy; see [`RestoreConflictPolicy`].
    pub conflict_policy: RestoreConflictPolicy,
    /// Unix permission bits for restored files, mirroring
    /// `LocalConfig::file_permissions`.
    pub file_permissions: Option<u32>,
}

impl std::fmt::Debug for LocalRestoreRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalRestoreRequest")
            .field("bundle_dir", &self.bundle_dir)
            .field("target_key_dir", &self.target_key_dir)
            .field("target_deployment_identity", &self.target_deployment_identity)
            .field("observed_generation", &self.observed_generation)
            .field("master_key", &self.master_key.as_ref().map(|_| "<redacted>"))
            .field("conflict_policy", &self.conflict_policy)
            .field("file_permissions", &self.file_permissions)
            .finish()
    }
}

impl LocalRestoreRequest {
    fn validate(&self) -> Result<()> {
        if self.target_deployment_identity.is_empty() {
            return Err(KmsError::validation_error("restore target_deployment_identity must not be empty"));
        }
        Ok(())
    }
}

/// Serializable outcome of a completed restore. Identifiers only; never key
/// material or secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalRestoreReport {
    /// Identifier of the restored bundle.
    pub backup_id: String,
    /// Snapshot generation the target now holds.
    pub snapshot_generation: u64,
    /// Stable key ids restored into the target.
    pub restored_key_ids: Vec<String>,
    /// Whether the master-key KDF salt was restored.
    pub salt_restored: bool,
    /// Whether this run rolled forward a previously interrupted cutover.
    pub resumed: bool,
}

/// The cutover commit marker: its durable publication is the single commit
/// point of a restore. It names the exact bundle and the exact top-level
/// files the cutover publishes, so re-entry can verify it is completing the
/// same restore and an abort knows precisely what to take back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreCommitMarker {
    format_version: u32,
    backup_id: String,
    manifest_digest: ContentDigest,
    /// Final top-level file names in link order (salt first, then keys).
    files: Vec<String>,
}

impl RestoreCommitMarker {
    fn for_bundle(decoded: &DecodedBundle) -> Self {
        let mut files = Vec::with_capacity(decoded.records.len() + 1);
        if decoded.salt.is_some() {
            files.push(LOCAL_KMS_MASTER_KEY_SALT_FILE.to_string());
        }
        files.extend(decoded.records.iter().map(|record| record.file_name.clone()));
        Self {
            format_version: RESTORE_MARKER_FORMAT_VERSION,
            backup_id: decoded.manifest.backup_id.clone(),
            manifest_digest: decoded.manifest.manifest_digest.clone(),
            files,
        }
    }

    /// Strict decode: an unreadable or out-of-contract marker fails closed —
    /// abort and roll-forward both refuse to guess what a foreign or damaged
    /// marker was committing.
    fn decode(bytes: &[u8]) -> Result<Self> {
        let marker: Self = serde_json::from_slice(bytes)
            .map_err(|error| BackupError::corrupted(format!("restore commit marker does not decode: {error}")))?;
        if marker.format_version != RESTORE_MARKER_FORMAT_VERSION {
            return Err(BackupError::corrupted(format!(
                "restore commit marker has unknown format version {}",
                marker.format_version
            ))
            .into());
        }
        if marker.files.is_empty() {
            return Err(BackupError::corrupted("restore commit marker lists no files").into());
        }
        for name in &marker.files {
            if name == LOCAL_KMS_MASTER_KEY_SALT_FILE {
                continue;
            }
            match name.strip_suffix(".key") {
                Some(stem) if validate_key_id(stem).is_ok() => {}
                _ => {
                    return Err(BackupError::corrupted(format!(
                        "restore commit marker lists a file name outside the restore contract: {name:?}"
                    ))
                    .into());
                }
            }
        }
        Ok(marker)
    }

    fn matches_bundle(&self, manifest: &BackupManifest) -> bool {
        self.backup_id == manifest.backup_id && self.manifest_digest == manifest.manifest_digest
    }
}

/// One key record decoded out of the bundle, fully re-verified.
struct DecodedRecord {
    key_id: String,
    /// Final on-disk file name (`<key_id>.key`).
    file_name: String,
    protection: StoredKeyProtection,
    /// Whether the record's material is AEAD-protected under the master key
    /// (explicit marker, or a legacy record with a nonce).
    effectively_encrypted: bool,
    /// Base64-decoded `encrypted_key_material` field.
    material: Zeroizing<Vec<u8>>,
    nonce: Vec<u8>,
    /// Raw record bytes exactly as exported; what staging writes.
    plaintext: Zeroizing<Vec<u8>>,
}

/// The fully decoded and cross-checked bundle content.
struct DecodedBundle {
    manifest: BackupManifest,
    records: Vec<DecodedRecord>,
    salt: Option<Zeroizing<Vec<u8>>>,
}

impl DecodedBundle {
    fn any_record_encrypted(&self) -> bool {
        self.records.iter().any(|record| record.effectively_encrypted)
    }
}

/// Minimal projection of a stored key record, mirroring the export-side
/// probe. Unknown fields are ignored on purpose: the record is restored
/// byte-identical, so restore must not constrain its schema.
#[derive(Deserialize)]
struct RestoredRecordProbe {
    key_id: String,
    #[serde(default)]
    at_rest_protection: StoredKeyProtection,
    encrypted_key_material: String,
    #[serde(default)]
    nonce: Vec<u8>,
}

/// Zero-write restore preflight: evaluate the bundle against the target and
/// report every blocker, conflict, and external dependency mismatch.
///
/// The bundle is decoded fully in memory (including AEAD verification of
/// every artifact); the target directory is only read. Environment failures
/// (unreadable directories) surface as errors, verdicts about the bundle or
/// the target surface inside the report.
pub async fn dry_run_local_restore(kek: &BackupKek, request: &LocalRestoreRequest) -> Result<RestoreDryRunReport> {
    request.validate()?;
    let mut report = RestoreDryRunReport {
        backup_id: String::new(),
        target_deployment_identity: request.target_deployment_identity.clone(),
        blockers: Vec::new(),
        conflicts: Vec::new(),
        external_mismatches: Vec::new(),
    };

    let decoded = match decode_bundle(&request.bundle_dir, kek).await {
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

    if let Some(detail) = kdf_drift_detail(&decoded.manifest) {
        report.blockers.push(RestoreBlocker {
            code: RestoreBlockerCode::UnsupportedBackend,
            detail,
        });
    }
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

    match check_master_key(&decoded, request.master_key.as_deref())? {
        MasterKeyCheck::Ok => {}
        MasterKeyCheck::MissingPassphrase(detail) => report.blockers.push(RestoreBlocker {
            code: RestoreBlockerCode::ExternalDependencyUnavailable,
            detail,
        }),
        MasterKeyCheck::UnknownVerifierScheme(detail) => report.blockers.push(RestoreBlocker {
            code: RestoreBlockerCode::BundleCorrupted,
            detail,
        }),
        MasterKeyCheck::Mismatch { expected, observed } => report.external_mismatches.push(ExternalDependencyMismatch {
            dependency: "local KMS master key (operator-supplied)".to_string(),
            expected,
            observed,
        }),
    }

    report
        .conflicts
        .extend(enumerate_target_conflicts(&request.target_key_dir, &decoded.manifest).await?);
    Ok(report)
}

/// Restore a sealed Local bundle into the target key directory.
///
/// Fail-fast counterpart of the dry-run: the first violated precondition
/// aborts with its typed error before the target top level is touched. Only
/// [`RestoreConflictPolicy::RestoreIntoEmptyTarget`] ever writes.
pub async fn restore_local_backup(kek: &BackupKek, request: &LocalRestoreRequest) -> Result<LocalRestoreReport> {
    request.validate()?;
    if request.conflict_policy == RestoreConflictPolicy::Fail {
        return Err(KmsError::invalid_operation(
            "restore conflict policy 'fail' never writes to the target; review the dry-run report and \
             re-run with the 'restore-into-empty-target' policy to proceed",
        ));
    }

    let decoded = decode_bundle(&request.bundle_dir, kek).await?;
    if let Some(detail) = kdf_drift_detail(&decoded.manifest) {
        return Err(KmsError::unsupported_capability("local-restore", detail));
    }
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
    match check_master_key(&decoded, request.master_key.as_deref())? {
        MasterKeyCheck::Ok => {}
        MasterKeyCheck::MissingPassphrase(detail) => return Err(KmsError::invalid_operation(detail)),
        MasterKeyCheck::UnknownVerifierScheme(detail) => return Err(BackupError::corrupted(detail).into()),
        MasterKeyCheck::Mismatch { .. } => {
            return Err(KmsError::validation_error(
                "operator-supplied master key does not match the bundle's master-key verifier",
            ));
        }
    }

    // Decryption-probe every record in memory before anything is written: a
    // wrong master key on a verifier-less legacy bundle must fail here, with
    // the target untouched.
    let ciphers = derive_ciphers(request.master_key.as_deref(), decoded.salt.as_deref().map(Vec::as_slice))?;
    for record in &decoded.records {
        probe_record_decryption(record, &ciphers)?;
    }

    let mode = inspect_target_for_restore(&request.target_key_dir, &decoded.manifest).await?;
    let expected_marker = RestoreCommitMarker::for_bundle(&decoded);
    let resumed = match &mode {
        TargetMode::Fresh { .. } => false,
        TargetMode::RollForward(published) => {
            if *published != expected_marker {
                return Err(BackupError::corrupted(
                    "published restore marker names the same bundle but a different file set; \
                     abort the interrupted restore before retrying",
                )
                .into());
            }
            true
        }
    };

    // Phase B: staging.
    let staging = request.target_key_dir.join(RESTORE_STAGING_DIR);
    if let TargetMode::Fresh { stale_entries } = &mode {
        // A fresh run owns no prior state: drop staging leftovers of an
        // earlier interrupted attempt and any orphan commit temps, then start
        // over from the bundle.
        if fs::try_exists(&staging).await? {
            remove_dir_all_durably(&request.target_key_dir, &staging).await?;
        }
        for stale in stale_entries {
            durable_file::remove_durably(request.target_key_dir.join(stale)).await?;
        }
    }
    fs::create_dir_all(&staging).await?;
    if let Some(salt) = &decoded.salt {
        stage_file(&staging, LOCAL_KMS_MASTER_KEY_SALT_FILE, salt, request.file_permissions).await?;
    }
    for record in &decoded.records {
        stage_file(&staging, &record.file_name, &record.plaintext, request.file_permissions).await?;
    }
    // Make the staging directory entry itself durable before the commit
    // point: the marker must never survive a crash that its staged files did
    // not.
    fsync_dir(&request.target_key_dir).await?;
    probe_staged_bundle(&staging, &decoded, &ciphers).await?;

    // Phase C: commit point, then cutover.
    if !resumed {
        publish_marker(&request.target_key_dir, &expected_marker, request.file_permissions).await?;
    }
    cutover_and_finish(&request.target_key_dir, &staging, &expected_marker).await?;

    Ok(LocalRestoreReport {
        backup_id: decoded.manifest.backup_id.clone(),
        snapshot_generation: decoded.manifest.snapshot_generation,
        restored_key_ids: decoded.records.iter().map(|record| record.key_id.clone()).collect(),
        salt_restored: decoded.salt.is_some(),
        resumed,
    })
}

/// Explicitly abort an interrupted restore, returning the target to its
/// pre-restore (empty) state.
///
/// With a published marker this takes back exactly the files the marker
/// names, then the marker, then the staging directory. Without a marker only
/// staging leftovers exist and are removed. Fails closed on a marker it
/// cannot strictly decode.
pub async fn abort_local_restore(target_key_dir: &Path) -> Result<()> {
    let marker_path = target_key_dir.join(LOCAL_RESTORE_COMMIT_MARKER_FILE);
    let staging = target_key_dir.join(RESTORE_STAGING_DIR);
    match fs::read(&marker_path).await {
        Ok(bytes) => {
            let marker = RestoreCommitMarker::decode(&bytes)?;
            for file_name in &marker.files {
                let path = target_key_dir.join(file_name);
                if fs::try_exists(&path).await? {
                    durable_file::remove_durably(path).await?;
                }
            }
            durable_file::remove_durably(marker_path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if !fs::try_exists(&staging).await? {
                return Err(KmsError::invalid_operation(format!(
                    "no restore is in progress under {}",
                    target_key_dir.display()
                )));
            }
        }
        Err(error) => return Err(error.into()),
    }
    if fs::try_exists(&staging).await? {
        remove_dir_all_durably(target_key_dir, &staging).await?;
    }
    Ok(())
}

/// Decode and cross-verify the complete bundle in memory.
///
/// Every artifact is read, digest-verified, and AEAD-authenticated; every key
/// record is re-parsed, identity-checked against its artifact path, and
/// checked for protection consistency with the manifest's KDF descriptor.
async fn decode_bundle(bundle_dir: &Path, kek: &BackupKek) -> Result<DecodedBundle> {
    let manifest = read_local_bundle_manifest(bundle_dir).await?;
    let descriptor = manifest
        .local_kdf
        .clone()
        .ok_or_else(|| BackupError::corrupted("local bundle manifest carries no KDF descriptor"))?;

    let mut records = Vec::new();
    let mut salt: Option<Zeroizing<Vec<u8>>> = None;
    let mut config_seen = false;
    for artifact in &manifest.artifacts {
        match artifact.kind {
            ArtifactKind::KeyMaterial => {
                let plaintext = decrypt_bundle_artifact(bundle_dir, &manifest, artifact, kek).await?;
                records.push(decode_key_record(artifact.path.as_str(), plaintext, &descriptor.protection_modes)?);
            }
            ArtifactKind::MasterKeySalt => {
                if salt.is_some() {
                    return Err(BackupError::corrupted("bundle carries more than one master-key salt artifact").into());
                }
                let plaintext = decrypt_bundle_artifact(bundle_dir, &manifest, artifact, kek).await?;
                if plaintext.len() != LOCAL_KMS_MASTER_KEY_SALT_LEN {
                    return Err(BackupError::corrupted(format!(
                        "bundled master-key salt is {} bytes, expected {}",
                        plaintext.len(),
                        LOCAL_KMS_MASTER_KEY_SALT_LEN
                    ))
                    .into());
                }
                salt = Some(plaintext);
            }
            // Configuration is evidence, not restorable state: the artifact is
            // verified so a tampered bundle still fails closed, but restore
            // never writes a configuration into the target. Presenting the
            // difference against the running configuration, and any decision to
            // adopt it, belongs to the admin layer.
            ArtifactKind::KmsConfig => {
                if config_seen {
                    return Err(BackupError::corrupted("bundle carries more than one KMS configuration artifact").into());
                }
                config_seen = true;
                decrypt_bundle_artifact(bundle_dir, &manifest, artifact, kek).await?;
            }
            // No producer emits these for a Local bundle yet; restoring a
            // bundle that carries state this implementation cannot apply
            // would silently drop it, so fail closed instead.
            other => {
                return Err(KmsError::unsupported_capability(
                    "local-restore",
                    format!("bundle carries artifact kind {other:?}, which this restore implementation cannot apply"),
                ));
            }
        }
    }

    // The salt must be exactly as coherent with the KDF descriptor as the
    // export invariants promise; anything else is a broken or tampered
    // bundle.
    match (&descriptor.derivation, salt.is_some()) {
        (LocalKeyDerivation::Argon2id { .. }, false) => {
            return Err(
                BackupError::corrupted("KDF descriptor declares Argon2id but the bundle carries no salt artifact").into(),
            );
        }
        (LocalKeyDerivation::LegacySha256, true) => {
            return Err(
                BackupError::corrupted("KDF descriptor declares the legacy derivation but the bundle carries a salt").into(),
            );
        }
        _ => {}
    }

    Ok(DecodedBundle { manifest, records, salt })
}

fn decode_key_record(
    artifact_path: &str,
    plaintext: Zeroizing<Vec<u8>>,
    allowed_modes: &[AtRestProtection],
) -> Result<DecodedRecord> {
    let stem = Path::new(artifact_path)
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".key.enc"))
        .ok_or_else(|| {
            BackupError::corrupted(format!(
                "key material artifact path {artifact_path:?} does not follow the '<key_id>.key.enc' layout"
            ))
        })?
        .to_string();
    validate_key_id(&stem)?;

    let probe: RestoredRecordProbe = serde_json::from_slice(&plaintext)
        .map_err(|error| BackupError::corrupted(format!("bundled key record '{stem}' does not deserialize: {error}")))?;
    if probe.key_id != stem {
        return Err(BackupError::corrupted(format!(
            "bundled key record identity mismatch: artifact names {stem:?}, record names {:?}",
            probe.key_id
        ))
        .into());
    }
    if probe.encrypted_key_material.is_empty() {
        return Err(BackupError::corrupted(format!("bundled key record '{stem}' carries no key material")).into());
    }
    let material =
        Zeroizing::new(BASE64.decode(&probe.encrypted_key_material).map_err(|error| {
            BackupError::corrupted(format!("bundled key record '{stem}' material is not valid base64: {error}"))
        })?);
    if !allowed_modes.contains(&protection_mode(probe.at_rest_protection)) {
        return Err(BackupError::corrupted(format!(
            "bundled key record '{stem}' declares protection {:?}, absent from the manifest's KDF descriptor",
            probe.at_rest_protection
        ))
        .into());
    }

    let effectively_encrypted = match probe.at_rest_protection {
        StoredKeyProtection::EncryptedMasterKey => true,
        StoredKeyProtection::PlaintextDevOnly => false,
        StoredKeyProtection::LegacyUnspecified => !probe.nonce.is_empty(),
    };
    if effectively_encrypted && probe.nonce.len() != 12 {
        return Err(BackupError::corrupted(format!(
            "bundled key record '{stem}' has an invalid nonce length ({} bytes, expected 12)",
            probe.nonce.len()
        ))
        .into());
    }

    Ok(DecodedRecord {
        file_name: format!("{stem}.key"),
        key_id: stem,
        protection: probe.at_rest_protection,
        effectively_encrypted,
        material,
        nonce: probe.nonce,
        plaintext,
    })
}

fn protection_mode(protection: StoredKeyProtection) -> AtRestProtection {
    match protection {
        StoredKeyProtection::EncryptedMasterKey => AtRestProtection::EncryptedMasterKey,
        StoredKeyProtection::PlaintextDevOnly => AtRestProtection::PlaintextDevOnly,
        StoredKeyProtection::LegacyUnspecified => AtRestProtection::LegacyUnspecified,
    }
}

/// Detail message when the bundle's recorded KDF differs from what this build
/// compiles in; restoring such a bundle would produce keys this build cannot
/// decrypt.
fn kdf_drift_detail(manifest: &BackupManifest) -> Option<String> {
    match &manifest.local_kdf.as_ref()?.derivation {
        derivation @ LocalKeyDerivation::Argon2id { .. } => {
            let current = LocalKeyDerivation::current_argon2id();
            (*derivation != current).then(|| {
                format!("bundle KDF descriptor {derivation:?} does not match this build's compiled-in derivation {current:?}")
            })
        }
        LocalKeyDerivation::LegacySha256 => None,
    }
}

enum MasterKeyCheck {
    Ok,
    MissingPassphrase(String),
    UnknownVerifierScheme(String),
    Mismatch { expected: String, observed: String },
}

/// Verify the operator-supplied master key against the manifest's one-way
/// verifier, and require one whenever the bundle carries encrypted records.
///
/// Bundles without a verifier (produced before the verifier landed) fall
/// back to the staging-phase decryption probe; unknown verifier schemes fail
/// closed.
fn check_master_key(decoded: &DecodedBundle, master_key: Option<&str>) -> Result<MasterKeyCheck> {
    let Some(master_key) = master_key else {
        if decoded.any_record_encrypted() {
            return Ok(MasterKeyCheck::MissingPassphrase(
                "bundle carries encrypted-at-rest key records; the operator must re-supply the master key before restore"
                    .to_string(),
            ));
        }
        return Ok(MasterKeyCheck::Ok);
    };

    let verifier = decoded
        .manifest
        .local_kdf
        .as_ref()
        .and_then(|descriptor| descriptor.master_key_verifier.as_deref());
    let Some(verifier) = verifier else {
        return Ok(MasterKeyCheck::Ok);
    };

    let observed = if verifier.starts_with(MASTER_KEY_VERIFIER_ARGON2ID_PREFIX) {
        let Some(salt) = decoded.salt.as_deref() else {
            return Err(
                BackupError::corrupted("manifest carries an Argon2id master-key verifier but the bundle has no salt").into(),
            );
        };
        compute_master_key_verifier(master_key, Some(salt), &decoded.manifest.backup_id)?
    } else if verifier.starts_with(MASTER_KEY_VERIFIER_LEGACY_PREFIX) {
        compute_master_key_verifier(master_key, None, &decoded.manifest.backup_id)?
    } else {
        return Ok(MasterKeyCheck::UnknownVerifierScheme(
            "manifest carries a master-key verifier with an unknown scheme prefix; failing closed".to_string(),
        ));
    };

    if observed != verifier {
        return Ok(MasterKeyCheck::Mismatch {
            expected: verifier.to_string(),
            observed,
        });
    }
    Ok(MasterKeyCheck::Ok)
}

/// AEAD ciphers derived from the operator-supplied master key, mirroring the
/// backend's current-plus-legacy pair.
struct DerivedCiphers {
    current: Option<Aes256Gcm>,
    legacy: Option<Aes256Gcm>,
}

fn derive_ciphers(master_key: Option<&str>, salt: Option<&[u8]>) -> Result<DerivedCiphers> {
    let Some(master_key) = master_key else {
        return Ok(DerivedCiphers {
            current: None,
            legacy: None,
        });
    };
    let legacy = Aes256Gcm::new(&LocalKmsClient::derive_legacy_master_key(master_key)?);
    let current = match salt {
        Some(salt) => Aes256Gcm::new(&LocalKmsClient::derive_master_key(master_key, salt)?),
        None => legacy.clone(),
    };
    Ok(DerivedCiphers {
        current: Some(current),
        legacy: Some(legacy),
    })
}

/// Decryption probe for one record, mirroring the backend's read path: the
/// current cipher first, with the legacy fallback only for legacy-unmarked
/// records. Local has no rotation history, so probing every record is the
/// full-coverage probe.
fn probe_record_decryption(record: &DecodedRecord, ciphers: &DerivedCiphers) -> Result<()> {
    if !record.effectively_encrypted {
        return Ok(());
    }
    let Some(current) = ciphers.current.as_ref() else {
        return Err(KmsError::invalid_operation(format!(
            "bundled key record '{}' is encrypted at rest and requires the operator-supplied master key",
            record.key_id
        )));
    };
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&record.nonce);
    let nonce = Nonce::from(nonce);
    match current.decrypt(&nonce, record.material.as_slice()) {
        Ok(material) => {
            drop(Zeroizing::new(material));
            Ok(())
        }
        Err(_) if record.protection == StoredKeyProtection::LegacyUnspecified => {
            let legacy = ciphers
                .legacy
                .as_ref()
                .expect("legacy cipher exists whenever the current cipher does");
            let material = legacy
                .decrypt(&nonce, record.material.as_slice())
                .map_err(|_| KmsError::material_authentication_failed(&record.key_id))?;
            drop(Zeroizing::new(material));
            Ok(())
        }
        Err(_) => Err(KmsError::material_authentication_failed(&record.key_id)),
    }
}

/// Re-run the decryption probe against the bytes that actually landed in
/// staging, so cutover only ever publishes state proven readable from disk.
async fn probe_staged_bundle(staging: &Path, decoded: &DecodedBundle, ciphers: &DerivedCiphers) -> Result<()> {
    if let Some(salt) = &decoded.salt {
        let staged_salt = fs::read(staging.join(LOCAL_KMS_MASTER_KEY_SALT_FILE)).await?;
        if staged_salt.as_slice() != salt.as_slice() {
            return Err(BackupError::corrupted("staged master-key salt does not match the bundle").into());
        }
    }
    for record in &decoded.records {
        let staged = Zeroizing::new(fs::read(staging.join(&record.file_name)).await?);
        if staged.as_slice() != record.plaintext.as_slice() {
            return Err(
                BackupError::corrupted(format!("staged key record '{}' does not match the bundle", record.key_id)).into(),
            );
        }
        let staged_record = decode_key_record(
            &format!("staged/{}.enc", record.file_name),
            staged,
            &decoded
                .manifest
                .local_kdf
                .as_ref()
                .expect("decoded bundle always carries a KDF descriptor")
                .protection_modes,
        )?;
        probe_record_decryption(&staged_record, ciphers)?;
    }
    Ok(())
}

enum TargetMode {
    /// The target is empty apart from removable leftovers of an interrupted
    /// earlier attempt (staging, orphan commit temps).
    Fresh { stale_entries: Vec<String> },
    /// A published marker of this same bundle: complete its cutover.
    RollForward(RestoreCommitMarker),
}

/// Fail-fast target inspection for an actual restore.
async fn inspect_target_for_restore(target: &Path, manifest: &BackupManifest) -> Result<TargetMode> {
    if !fs::try_exists(target).await? {
        return Ok(TargetMode::Fresh {
            stale_entries: Vec::new(),
        });
    }

    let marker = read_marker(target).await?;
    if let Some(marker) = marker {
        if !marker.matches_bundle(manifest) {
            return Err(KmsError::invalid_operation(format!(
                "target key directory holds a restore marker for a different bundle ('{}'); \
                 roll that restore forward with its own bundle or abort it explicitly",
                marker.backup_id
            )));
        }
        // Everything at the top level must belong to this restore: the
        // marker's files (partially linked) and our own working entries.
        let allowed: Vec<&str> = marker.files.iter().map(String::as_str).collect();
        for entry in list_target_entries(target).await? {
            if !allowed.contains(&entry.as_str()) && !is_orphan_commit_temp_name(&entry) {
                return Err(KmsError::invalid_operation(format!(
                    "target key directory holds an entry outside the interrupted restore: {entry:?}; \
                     clean it up manually before resuming"
                )));
            }
        }
        return Ok(TargetMode::RollForward(marker));
    }

    let mut stale_entries = Vec::new();
    for entry in list_target_entries(target).await? {
        if is_orphan_commit_temp_name(&entry) {
            // Unpublished commit temps are never authoritative; a re-run may
            // remove them exactly as startup recovery would.
            stale_entries.push(entry);
            continue;
        }
        let detail = if entry == LOCAL_KMS_MASTER_KEY_SALT_FILE {
            "an orphan KDF salt would poison every key created after the restore; clean it up manually"
        } else {
            "restore only ever writes into an empty target"
        };
        return Err(KmsError::invalid_operation(format!(
            "target key directory is not empty (found {entry:?}); {detail}"
        )));
    }
    Ok(TargetMode::Fresh { stale_entries })
}

/// Top-level entries of the target, excluding the staging directory and the
/// commit marker (both handled separately).
async fn list_target_entries(target: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut entries = fs::read_dir(target).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry
            .file_name()
            .to_str()
            .ok_or_else(|| KmsError::invalid_operation("target key directory holds a non-UTF-8 entry; clean it up manually"))?
            .to_string();
        if name == RESTORE_STAGING_DIR || name == LOCAL_RESTORE_COMMIT_MARKER_FILE {
            continue;
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

async fn read_marker(target: &Path) -> Result<Option<RestoreCommitMarker>> {
    match fs::read(target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE)).await {
        Ok(bytes) => Ok(Some(RestoreCommitMarker::decode(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Conflict enumeration for the dry-run: every target entry that would block
/// the restore, without failing on the first one.
async fn enumerate_target_conflicts(target: &Path, manifest: &BackupManifest) -> Result<Vec<RestoreConflict>> {
    if !fs::try_exists(target).await? {
        return Ok(Vec::new());
    }
    let mut conflicts = Vec::new();

    let marker = match read_marker(target).await {
        Ok(marker) => marker,
        // A marker that does not strictly decode blocks exactly like a
        // foreign one; the dry-run reports it instead of failing.
        Err(error) => {
            conflicts.push(RestoreConflict {
                key_id: LOCAL_RESTORE_COMMIT_MARKER_FILE.to_string(),
                kind: RestoreConflictKind::KeyAlreadyExists,
                detail: format!("target holds an undecodable restore marker: {error}"),
            });
            None
        }
    };
    let own_files: Vec<String> = match &marker {
        Some(marker) if marker.matches_bundle(manifest) => marker.files.clone(),
        Some(marker) => {
            conflicts.push(RestoreConflict {
                key_id: LOCAL_RESTORE_COMMIT_MARKER_FILE.to_string(),
                kind: RestoreConflictKind::KeyAlreadyExists,
                detail: format!("target holds a restore marker for a different bundle ('{}')", marker.backup_id),
            });
            Vec::new()
        }
        None => Vec::new(),
    };

    for entry in list_target_entries(target).await? {
        if own_files.contains(&entry) || is_orphan_commit_temp_name(&entry) {
            continue;
        }
        let (key_id, detail) = if let Some(stem) = entry.strip_suffix(".key") {
            (stem.to_string(), format!("target already has key file {entry:?}"))
        } else if entry == LOCAL_KMS_MASTER_KEY_SALT_FILE {
            (
                entry.clone(),
                "target holds an orphan master-key KDF salt; it would poison every key created after the restore".to_string(),
            )
        } else {
            (entry.clone(), format!("target holds an unexpected entry {entry:?}"))
        };
        conflicts.push(RestoreConflict {
            key_id,
            kind: RestoreConflictKind::KeyAlreadyExists,
            detail,
        });
    }
    Ok(conflicts)
}

/// Durably commit one staged file. Idempotent across roll-forward re-entry:
/// an already staged file is accepted only byte-identical.
async fn stage_file(staging: &Path, file_name: &str, content: &[u8], permissions: Option<u32>) -> Result<()> {
    let final_path = staging.join(file_name);
    match fs::read(&final_path).await {
        Ok(existing) => {
            let existing = Zeroizing::new(existing);
            if existing.as_slice() == content {
                return Ok(());
            }
            return Err(BackupError::corrupted(format!(
                "staging already holds '{file_name}' with different content; abort the interrupted restore before retrying"
            ))
            .into());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temp_path = staging.join(format!("{file_name}.tmp-{}", uuid::Uuid::new_v4()));
    durable_file::commit(temp_path, final_path, content.to_vec(), permissions, durable_file::Publish::NoClobber)
        .await
        .map_err(KmsError::from)
}

/// Durably publish the commit marker. This is the restore's single commit
/// point: before it, the target top level is untouched; after it, the only
/// ways out are roll-forward or explicit abort.
async fn publish_marker(target: &Path, marker: &RestoreCommitMarker, permissions: Option<u32>) -> Result<()> {
    let content = serde_json::to_vec_pretty(marker)
        .map_err(|error| KmsError::internal_error(format!("restore commit marker serialization failed: {error}")))?;
    let final_path = target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE);
    let temp_path = target.join(format!("{LOCAL_RESTORE_COMMIT_MARKER_FILE}.tmp-{}", uuid::Uuid::new_v4()));
    durable_file::commit(temp_path, final_path, content, permissions, durable_file::Publish::NoClobber)
        .await
        .map_err(KmsError::from)
}

/// The atomic cutover: publish every staged file under its final name (salt
/// first, keys after — the marker froze that order), durably remove the
/// marker, then drop the staging directory.
async fn cutover_and_finish(target: &Path, staging: &Path, marker: &RestoreCommitMarker) -> Result<()> {
    for file_name in &marker.files {
        durable_file::link_durably(staging.join(file_name), target.join(file_name))
            .await
            .map_err(|error| KmsError::io_error(format!("restore cutover failed publishing '{file_name}': {error}")))?;
    }
    durable_file::remove_durably(target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE))
        .await
        .map_err(|error| KmsError::io_error(format!("restore cutover failed removing the commit marker: {error}")))?;
    // The restore is complete once the marker is gone; a leftover staging
    // directory is inert (its entries are hard links of the published files),
    // so its removal is cleanup, not protocol.
    if let Err(error) = remove_dir_all_durably(target, staging).await {
        warn!(%error, "restored Local KMS key directory kept an inert staging directory");
    }
    Ok(())
}

async fn remove_dir_all_durably(parent: &Path, dir: &Path) -> Result<()> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir))
        .await
        .map_err(|error| KmsError::io_error(error.to_string()))??;
    fsync_dir(parent).await
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
    use crate::backup::local_export::{LOCAL_BUNDLE_MANIFEST_FILE, LocalBackupExportRequest, export_local_backup};
    use crate::config::LocalConfig;
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;
    use tempfile::TempDir;

    const TEST_MASTER_KEY: &str = "test-master-key";
    const DEPLOYMENT: &str = "deployment-test";
    const BACKUP_ID: &str = "backup-0001";
    const SNAPSHOT_GENERATION: u64 = 7;

    fn local_config(dir: &Path, master_key: Option<&str>) -> LocalConfig {
        LocalConfig {
            key_dir: dir.to_path_buf(),
            master_key: master_key.map(str::to_string),
            file_permissions: Some(0o600),
        }
    }

    async fn source_with_keys(master_key: Option<&str>, keys: &[&str]) -> (LocalKmsClient, TempDir) {
        let temp = TempDir::new().expect("temp dir");
        let client = LocalKmsClient::new(local_config(temp.path(), master_key))
            .await
            .expect("client should initialize");
        for key in keys {
            client.create_key(key, "AES_256", None).await.expect("create key");
        }
        (client, temp)
    }

    fn test_kek() -> BackupKek {
        BackupKek::new("backup-kek-test", 1, [0x42; 32]).expect("kek")
    }

    async fn export_bundle(client: &LocalKmsClient, dir: &Path) -> (PathBuf, BackupManifest) {
        export_bundle_with_config(client, dir, None).await
    }

    async fn export_bundle_with_config(
        client: &LocalKmsClient,
        dir: &Path,
        sanitized_config: Option<Vec<u8>>,
    ) -> (PathBuf, BackupManifest) {
        let destination = dir.join("bundle");
        let manifest = export_local_backup(
            client,
            &test_kek(),
            &LocalBackupExportRequest {
                backup_id: BACKUP_ID.to_string(),
                deployment_identity: DEPLOYMENT.to_string(),
                rustfs_version: "1.0.0-test".to_string(),
                snapshot_generation: SNAPSHOT_GENERATION,
                destination: destination.clone(),
                sanitized_config,
            },
        )
        .await
        .expect("export should succeed");
        (destination, manifest)
    }

    fn restore_request(bundle: &Path, target: &Path) -> LocalRestoreRequest {
        LocalRestoreRequest {
            bundle_dir: bundle.to_path_buf(),
            target_key_dir: target.to_path_buf(),
            target_deployment_identity: DEPLOYMENT.to_string(),
            observed_generation: None,
            master_key: Some(TEST_MASTER_KEY.to_string()),
            conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
            file_permissions: Some(0o600),
        }
    }

    /// Recursive (path, content SHA-256, mtime) snapshot; equality across an
    /// operation proves it performed no persistent write at all.
    fn snapshot_tree(dir: &Path) -> Vec<(String, Vec<u8>, SystemTime)> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>, SystemTime)>) {
            for entry in std::fs::read_dir(dir).expect("read dir") {
                let entry = entry.expect("dir entry");
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, out);
                    continue;
                }
                let relative = path.strip_prefix(root).expect("under root").to_string_lossy().into_owned();
                let content = std::fs::read(&path).expect("read file");
                let mtime = entry.metadata().expect("metadata").modified().expect("mtime");
                out.push((relative, Sha256::digest(&content).to_vec(), mtime));
            }
        }
        let mut out = Vec::new();
        walk(dir, dir, &mut out);
        out.sort();
        out
    }

    async fn top_level_names(dir: &Path) -> Vec<String> {
        let mut names = Vec::new();
        let mut entries = fs::read_dir(dir).await.expect("read dir");
        while let Some(entry) = entries.next_entry().await.expect("dir entry") {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        names
    }

    /// Re-seal the bundle manifest after mutating it, keeping the digest
    /// contract intact so only the mutated aspect is under test.
    async fn reseal_manifest(bundle_dir: &Path, mutate: impl FnOnce(&mut BackupManifest)) {
        let path = bundle_dir.join(LOCAL_BUNDLE_MANIFEST_FILE);
        let mut manifest = BackupManifest::decode(&fs::read(&path).await.expect("manifest bytes")).expect("manifest decodes");
        mutate(&mut manifest);
        let sealed = manifest.seal().expect("re-seal");
        fs::write(&path, sealed.encode().expect("encode"))
            .await
            .expect("write manifest");
    }

    /// The full acceptance check for a restored directory: raw files
    /// byte-identical to the source, and every record decodable through the
    /// read-only export constructor with byte-identical material.
    async fn assert_restored_matches_source(source: &LocalKmsClient, target: &Path, keys: &[&str]) {
        for key in keys {
            let source_bytes = fs::read(source.key_directory().join(format!("{key}.key")))
                .await
                .expect("source record");
            let restored_bytes = fs::read(target.join(format!("{key}.key"))).await.expect("restored record");
            assert_eq!(source_bytes, restored_bytes, "record {key} must restore byte-for-byte");
        }
        let source_salt = fs::read(source.master_key_salt_file()).await.expect("source salt");
        let restored_salt = fs::read(target.join(LOCAL_KMS_MASTER_KEY_SALT_FILE))
            .await
            .expect("restored salt");
        assert_eq!(source_salt, restored_salt, "salt must restore byte-for-byte");

        let restored = LocalKmsClient::new_for_key_export(local_config(target, Some(TEST_MASTER_KEY)))
            .await
            .expect("restored directory must open read-only");
        for key in keys {
            let source_material = source.decrypt_key_material_for_export(key).await.expect("source material");
            let restored_material = restored
                .decrypt_key_material_for_export(key)
                .await
                .expect("restored material");
            assert_eq!(
                source_material.as_ref(),
                restored_material.as_ref(),
                "key material for {key} must survive the restore byte-for-byte"
            );
        }
    }

    #[tokio::test]
    async fn dry_run_is_zero_write_and_permits_clean_restore() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha", "beta"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");
        fs::create_dir_all(&target).await.expect("create target");

        let bundle_before = snapshot_tree(&bundle);
        let target_before = snapshot_tree(&target);
        let report = dry_run_local_restore(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("dry run should succeed");

        assert!(report.restore_permitted(), "clean bundle and empty target must permit: {report:?}");
        assert_eq!(report.backup_id, BACKUP_ID);
        assert_eq!(report.target_deployment_identity, DEPLOYMENT);
        assert_eq!(snapshot_tree(&bundle), bundle_before, "dry run must not touch the bundle");
        assert_eq!(snapshot_tree(&target), target_before, "dry run must not touch the target");

        // A missing target directory stays missing.
        let missing = work.path().join("missing-target");
        let report = dry_run_local_restore(&test_kek(), &restore_request(&bundle, &missing))
            .await
            .expect("dry run should succeed");
        assert!(report.restore_permitted());
        assert!(!missing.exists(), "dry run must not create the target directory");
    }

    #[tokio::test]
    async fn dry_run_flags_deployment_mismatch_and_generation_regression() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let mut request = restore_request(&bundle, &target);
        request.target_deployment_identity = "deployment-other".to_string();
        request.observed_generation = Some(SNAPSHOT_GENERATION + 1);
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::DeploymentMismatch),
            "expected a deployment mismatch blocker: {report:?}"
        );
        assert!(
            report
                .conflicts
                .iter()
                .any(|conflict| conflict.kind == RestoreConflictKind::GenerationRegression),
            "expected a generation regression conflict: {report:?}"
        );

        // Equal generations are allowed so drills can repeat.
        let mut request = restore_request(&bundle, &target);
        request.observed_generation = Some(SNAPSHOT_GENERATION);
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(report.restore_permitted(), "equal generation must not conflict: {report:?}");
    }

    #[tokio::test]
    async fn dry_run_checks_the_operator_master_key() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let verifier = manifest
            .local_kdf
            .as_ref()
            .and_then(|kdf| kdf.master_key_verifier.as_deref())
            .expect("encrypted bundle must carry a verifier");
        assert!(verifier.starts_with(MASTER_KEY_VERIFIER_ARGON2ID_PREFIX), "got {verifier:?}");

        // No passphrase: encrypted records make the master key a required
        // external dependency.
        let mut request = restore_request(&bundle, &target);
        request.master_key = None;
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::ExternalDependencyUnavailable),
            "expected a missing-master-key blocker: {report:?}"
        );

        // Wrong passphrase: reported as an external dependency mismatch.
        let mut request = restore_request(&bundle, &target);
        request.master_key = Some("wrong-master-key".to_string());
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert_eq!(report.external_mismatches.len(), 1, "{report:?}");
        assert_eq!(report.external_mismatches[0].expected, verifier);
        assert!(!report.restore_permitted());
    }

    #[tokio::test]
    async fn dry_run_enumerates_existing_target_state() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;

        // A populated target: a key file, the salt, and a stray file.
        let (_occupant, target_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["occupied"]).await;
        fs::write(target_dir.path().join("operator-notes.txt"), b"leftover")
            .await
            .expect("stray file");

        let report = dry_run_local_restore(&test_kek(), &restore_request(&bundle, target_dir.path()))
            .await
            .expect("dry run");
        let conflicting_ids: Vec<&str> = report.conflicts.iter().map(|conflict| conflict.key_id.as_str()).collect();
        assert!(conflicting_ids.contains(&"occupied"), "{report:?}");
        assert!(conflicting_ids.contains(&LOCAL_KMS_MASTER_KEY_SALT_FILE), "{report:?}");
        assert!(conflicting_ids.contains(&"operator-notes.txt"), "{report:?}");
        assert!(!report.restore_permitted());
    }

    #[tokio::test]
    async fn restore_into_empty_target_round_trips_byte_for_byte() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha", "beta"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let bundle_before = snapshot_tree(&bundle);
        let report = restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("restore should succeed");

        assert_eq!(
            report,
            LocalRestoreReport {
                backup_id: BACKUP_ID.to_string(),
                snapshot_generation: SNAPSHOT_GENERATION,
                restored_key_ids: vec!["alpha".to_string(), "beta".to_string()],
                salt_restored: true,
                resumed: false,
            }
        );
        assert_eq!(snapshot_tree(&bundle), bundle_before, "the bundle source must stay byte-identical");
        assert_eq!(
            top_level_names(&target).await,
            vec![
                LOCAL_KMS_MASTER_KEY_SALT_FILE.to_string(),
                "alpha.key".to_string(),
                "beta.key".to_string()
            ],
            "cutover must leave no marker, staging, or temp files behind"
        );
        assert_restored_matches_source(&client, &target, &["alpha", "beta"]).await;

        // The restored directory boots as a normal backend directory.
        let restored = LocalKmsClient::new(local_config(&target, Some(TEST_MASTER_KEY)))
            .await
            .expect("restored directory must pass startup validation");
        restored.describe_key("alpha", None).await.expect("restored key describes");
    }

    #[tokio::test]
    async fn default_fail_policy_never_writes() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let mut request = restore_request(&bundle, &target);
        request.conflict_policy = RestoreConflictPolicy::Fail;
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("the default policy must refuse to write");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(!target.exists(), "the fail policy must not create the target");
    }

    #[tokio::test]
    async fn non_empty_targets_are_rejected() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, manifest) = export_bundle(&client, work.path()).await;

        // Existing key material.
        let (_occupant, occupied) = source_with_keys(Some(TEST_MASTER_KEY), &["occupied"]).await;
        let before = snapshot_tree(occupied.path());
        let error = restore_local_backup(&test_kek(), &restore_request(&bundle, occupied.path()))
            .await
            .expect_err("occupied target must be rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert_eq!(snapshot_tree(occupied.path()), before, "the rejected target must stay untouched");

        // An orphan salt alone makes the target non-empty: a foreign salt
        // would poison every key created after the restore.
        let orphan = TempDir::new().expect("orphan dir");
        fs::write(orphan.path().join(LOCAL_KMS_MASTER_KEY_SALT_FILE), [7u8; 16])
            .await
            .expect("seed orphan salt");
        let before = snapshot_tree(orphan.path());
        let error = restore_local_backup(&test_kek(), &restore_request(&bundle, orphan.path()))
            .await
            .expect_err("orphan salt must be rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(error.to_string().contains("salt"), "got {error}");
        assert_eq!(snapshot_tree(orphan.path()), before);

        // A marker of a different restore.
        let foreign = TempDir::new().expect("foreign dir");
        let mut digest = manifest.manifest_digest.clone();
        digest.hex = digest.hex.chars().rev().collect();
        let foreign_marker = RestoreCommitMarker {
            format_version: RESTORE_MARKER_FORMAT_VERSION,
            backup_id: "backup-foreign".to_string(),
            manifest_digest: digest,
            files: vec!["foreign.key".to_string()],
        };
        fs::write(
            foreign.path().join(LOCAL_RESTORE_COMMIT_MARKER_FILE),
            serde_json::to_vec_pretty(&foreign_marker).expect("marker json"),
        )
        .await
        .expect("seed foreign marker");
        let before = snapshot_tree(foreign.path());
        let error = restore_local_backup(&test_kek(), &restore_request(&bundle, foreign.path()))
            .await
            .expect_err("foreign marker must be rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(error.to_string().contains("different bundle"), "got {error}");
        assert_eq!(snapshot_tree(foreign.path()), before);
    }

    #[tokio::test]
    async fn wrong_passphrase_fails_via_verifier_before_any_target_write() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let mut request = restore_request(&bundle, &target);
        request.master_key = Some("wrong-master-key".to_string());
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("wrong master key must be rejected by the verifier");
        assert!(matches!(error, KmsError::ValidationError { .. }), "got {error:?}");
        assert!(!target.exists(), "the verifier must reject before any target write");
    }

    #[tokio::test]
    async fn wrong_passphrase_fails_via_probe_when_bundle_has_no_verifier() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        // Simulate a bundle produced before the verifier landed.
        reseal_manifest(&bundle, |manifest| {
            manifest
                .local_kdf
                .as_mut()
                .expect("local bundle has a KDF descriptor")
                .master_key_verifier = None;
        })
        .await;

        let mut request = restore_request(&bundle, &target);
        request.master_key = Some("wrong-master-key".to_string());
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("wrong master key must be caught by the decryption probe");
        assert!(matches!(error, KmsError::MaterialAuthenticationFailed { .. }), "got {error:?}");
        assert!(!target.exists(), "the in-memory probe must reject before any target write");

        // The right passphrase still restores the verifier-less bundle.
        restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("verifier-less bundle must restore with the right master key");
        assert_restored_matches_source(&client, &target, &["alpha"]).await;
    }

    #[tokio::test]
    async fn bundle_defects_fail_closed_on_restore() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["victim"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");
        let request = restore_request(&bundle, &target);

        let artifact_file = bundle.join(&manifest.artifacts[0].path);
        let original_artifact = std::fs::read(&artifact_file).expect("artifact bytes");

        // Tampered artifact byte.
        let mut tampered = original_artifact.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        std::fs::write(&artifact_file, &tampered).expect("write tampered");
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("tampered artifact must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::BundleCorrupted),
            "{report:?}"
        );

        // Truncated artifact.
        std::fs::write(&artifact_file, &original_artifact[..original_artifact.len() - 4]).expect("truncate");
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("truncated artifact must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Truncated { .. })), "got {error:?}");
        std::fs::write(&artifact_file, &original_artifact).expect("restore artifact");

        // Wrong KEK identity and wrong KEK material.
        let wrong_id = BackupKek::new("other-kek", 1, [0x42; 32]).expect("kek");
        let error = restore_local_backup(&wrong_id, &request)
            .await
            .expect_err("wrong KEK id must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::WrongKek { .. })), "got {error:?}");
        let wrong_material = BackupKek::new("backup-kek-test", 1, [0x24; 32]).expect("kek");
        let error = restore_local_backup(&wrong_material, &request)
            .await
            .expect_err("wrong KEK material must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");

        // Missing manifest: the bundle never sealed.
        std::fs::remove_file(bundle.join(LOCAL_BUNDLE_MANIFEST_FILE)).expect("remove manifest");
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("unsealed bundle must be rejected");
        assert!(matches!(error, KmsError::Backup(BackupError::IncompleteBundle { .. })), "got {error:?}");
        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::IncompleteBundle),
            "{report:?}"
        );

        assert!(!target.exists(), "no rejected bundle may touch the target");
    }

    #[tokio::test]
    async fn kdf_parameter_drift_is_rejected() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        reseal_manifest(&bundle, |manifest| {
            let descriptor = manifest.local_kdf.as_mut().expect("local bundle has a KDF descriptor");
            match &mut descriptor.derivation {
                LocalKeyDerivation::Argon2id { memory_kib, .. } => *memory_kib *= 2,
                other => panic!("expected an Argon2id descriptor, got {other:?}"),
            }
        })
        .await;

        let request = restore_request(&bundle, &target);
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("a KDF parameter drift must be rejected");
        assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "got {error:?}");
        assert!(!target.exists());

        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::UnsupportedBackend),
            "{report:?}"
        );
    }

    #[tokio::test]
    async fn unknown_verifier_scheme_fails_closed() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        reseal_manifest(&bundle, |manifest| {
            manifest
                .local_kdf
                .as_mut()
                .expect("local bundle has a KDF descriptor")
                .master_key_verifier = Some("post-quantum-v2:0123abcd".to_string());
        })
        .await;

        let request = restore_request(&bundle, &target);
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("an unknown verifier scheme must fail closed");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");
        assert!(!target.exists());

        let report = dry_run_local_restore(&test_kek(), &request).await.expect("dry run");
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.code == RestoreBlockerCode::BundleCorrupted),
            "{report:?}"
        );
    }

    #[tokio::test]
    async fn generation_regression_is_rejected_and_equal_is_allowed() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let mut request = restore_request(&bundle, &target);
        request.observed_generation = Some(SNAPSHOT_GENERATION + 1);
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("a strictly lower bundle generation must be rejected");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
        assert!(!target.exists());

        request.observed_generation = Some(SNAPSHOT_GENERATION);
        restore_local_backup(&test_kek(), &request)
            .await
            .expect("an equal generation must stay restorable for repeated drills");
        assert_restored_matches_source(&client, &target, &["alpha"]).await;
    }

    #[tokio::test]
    async fn dev_mode_bundle_round_trips_without_passphrase() {
        let (client, _source_dir) = source_with_keys(None, &["dev-key"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let kdf = manifest.local_kdf.as_ref().expect("local kdf descriptor");
        assert_eq!(kdf.master_key_verifier, None, "dev-mode bundles have no master key to verify");

        let mut request = restore_request(&bundle, &target);
        request.master_key = None;
        let report = restore_local_backup(&test_kek(), &request)
            .await
            .expect("dev-mode restore must succeed without a passphrase");
        assert!(!report.salt_restored);
        assert_eq!(report.restored_key_ids, vec!["dev-key".to_string()]);

        let source_bytes = fs::read(client.key_directory().join("dev-key.key")).await.expect("source");
        let restored_bytes = fs::read(target.join("dev-key.key")).await.expect("restored");
        assert_eq!(source_bytes, restored_bytes);

        let restored = LocalKmsClient::new(local_config(&target, None))
            .await
            .expect("restored dev directory must boot");
        restored.describe_key("dev-key", None).await.expect("restored key describes");
    }

    #[tokio::test]
    async fn legacy_sha256_bundle_round_trips() {
        // A pre-beta.9 directory: legacy SHA-256 encrypted record, no salt,
        // no protection marker (same fixture as the backend's beta.5 test;
        // the material decrypts to 32 x 0x42 under this master key).
        let source_dir = TempDir::new().expect("legacy dir");
        let master_key = "beta5-test-master-key";
        let record = serde_json::json!({
            "key_id": "beta5-key",
            "version": 1u32,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "description": serde_json::Value::Null,
            "metadata": std::collections::HashMap::<String, String>::new(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "rotated_at": serde_json::Value::Null,
            "created_by": "beta5-fixture",
            "encrypted_key_material": "xjwGa4Lj4qzKg6XQl8s2btyFkPHPChMAkjqs268TFGyvFUv8WjDD5HQCUDLViZmt",
            "nonce": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        });
        fs::write(
            source_dir.path().join("beta5-key.key"),
            serde_json::to_vec_pretty(&record).expect("record json"),
        )
        .await
        .expect("write legacy record");

        // Export through the read-only constructor so no Argon2 salt gets
        // created: the bundle must record the legacy derivation.
        let client = LocalKmsClient::new_for_key_export(local_config(source_dir.path(), Some(master_key)))
            .await
            .expect("open legacy dir read-only");
        let work = TempDir::new().expect("work dir");
        let (bundle, manifest) = export_bundle(&client, work.path()).await;
        let kdf = manifest.local_kdf.as_ref().expect("local kdf descriptor");
        assert_eq!(kdf.derivation, LocalKeyDerivation::LegacySha256);
        let verifier = kdf.master_key_verifier.as_deref().expect("legacy verifier");
        assert!(verifier.starts_with(MASTER_KEY_VERIFIER_LEGACY_PREFIX), "got {verifier:?}");

        let target = work.path().join("target");
        let mut request = restore_request(&bundle, &target);
        request.master_key = Some("wrong-master-key".to_string());
        let error = restore_local_backup(&test_kek(), &request)
            .await
            .expect_err("the legacy verifier must reject a wrong master key");
        assert!(matches!(error, KmsError::ValidationError { .. }), "got {error:?}");
        assert!(!target.exists());

        request.master_key = Some(master_key.to_string());
        let report = restore_local_backup(&test_kek(), &request).await.expect("legacy restore");
        assert!(!report.salt_restored, "a legacy bundle carries no salt");

        // The restored directory boots (creating a fresh Argon2 salt is
        // legitimate for legacy records) and the material still decrypts via
        // the legacy fallback.
        let restored = LocalKmsClient::new(local_config(&target, Some(master_key)))
            .await
            .expect("restored legacy directory must boot");
        let material = restored
            .decrypt_key_material_for_export("beta5-key")
            .await
            .expect("legacy material must decrypt after restore");
        assert_eq!(material.as_ref(), &[0x42; 32]);
    }

    #[tokio::test]
    async fn crash_during_staging_keeps_top_level_untouched_and_rerun_converges() {
        use durable_file::{CommitStep, failpoint};

        for step in [
            CommitStep::TempWritten,
            CommitStep::FileSynced,
            CommitStep::Published,
            CommitStep::DirSynced,
        ] {
            let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
            let work = TempDir::new().expect("work dir");
            let (bundle, _manifest) = export_bundle(&client, work.path()).await;
            let target = work.path().join("target");
            let request = restore_request(&bundle, &target);

            failpoint::arm(&target, step);
            let error = restore_local_backup(&test_kek(), &request)
                .await
                .expect_err("armed staging commit must simulate a crash");
            failpoint::disarm(&target);
            assert!(error.to_string().contains("injected crash"), "step {step:?}: {error}");

            // Top level: only the staging directory, never a marker or a key.
            assert_eq!(
                top_level_names(&target).await,
                vec![RESTORE_STAGING_DIR.to_string()],
                "step {step:?}: a staging crash must leave the top level untouched"
            );

            restore_local_backup(&test_kek(), &request)
                .await
                .unwrap_or_else(|error| panic!("step {step:?}: re-run must converge: {error}"));
            assert_restored_matches_source(&client, &target, &["alpha"]).await;
        }
    }

    /// Drive the phases manually up to the marker commit, crash the commit at
    /// every step, and prove both re-entry outcomes: an unpublished marker
    /// temp is removable leftover state, a published marker rolls forward.
    #[tokio::test]
    async fn crash_around_marker_publish_recovers() {
        use durable_file::{CommitStep, failpoint};

        for step in [
            CommitStep::TempWritten,
            CommitStep::FileSynced,
            CommitStep::Published,
            CommitStep::DirSynced,
        ] {
            let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha", "beta"]).await;
            let work = TempDir::new().expect("work dir");
            let (bundle, _manifest) = export_bundle(&client, work.path()).await;
            let target = work.path().join("target");
            let request = restore_request(&bundle, &target);

            // Stage everything through the real phase functions.
            let decoded = decode_bundle(&bundle, &test_kek()).await.expect("decode bundle");
            let staging = target.join(RESTORE_STAGING_DIR);
            fs::create_dir_all(&staging).await.expect("create staging");
            let salt = decoded.salt.as_ref().expect("bundle salt");
            stage_file(&staging, LOCAL_KMS_MASTER_KEY_SALT_FILE, salt, Some(0o600))
                .await
                .expect("stage salt");
            for record in &decoded.records {
                stage_file(&staging, &record.file_name, &record.plaintext, Some(0o600))
                    .await
                    .expect("stage record");
            }
            let marker = RestoreCommitMarker::for_bundle(&decoded);

            failpoint::arm(&target, step);
            let error = publish_marker(&target, &marker, Some(0o600))
                .await
                .expect_err("armed marker commit must simulate a crash");
            failpoint::disarm(&target);
            assert!(error.to_string().contains("injected crash"), "step {step:?}: {error}");

            let marker_path = target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE);
            let published = matches!(step, CommitStep::Published | CommitStep::DirSynced);
            assert_eq!(marker_path.exists(), published, "step {step:?}");
            if !published {
                // The unpublished marker temp is exactly the shape startup
                // recovery and a restore re-run both classify as removable.
                let temp = top_level_names(&target)
                    .await
                    .into_iter()
                    .find(|name| is_orphan_commit_temp_name(name));
                assert!(temp.is_some(), "step {step:?}: expected a leftover marker temp");
            } else {
                // A published marker makes backend startup fail closed.
                let error = match LocalKmsClient::new(local_config(&target, Some(TEST_MASTER_KEY))).await {
                    Ok(_) => panic!("step {step:?}: startup must fail closed on a published marker"),
                    Err(error) => error,
                };
                assert!(matches!(error, KmsError::ConfigurationError { .. }), "step {step:?}: {error:?}");
                assert!(error.to_string().contains("unfinished restore"), "step {step:?}: {error}");
            }

            let report = restore_local_backup(&test_kek(), &request)
                .await
                .unwrap_or_else(|error| panic!("step {step:?}: re-run must converge: {error}"));
            assert_eq!(report.resumed, published, "step {step:?}");
            assert_restored_matches_source(&client, &target, &["alpha", "beta"]).await;
        }
    }

    #[tokio::test]
    async fn crash_mid_cutover_fails_startup_then_rolls_forward_or_aborts() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha", "beta"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;

        // Reconstruct the exact state of a cutover killed after k of n links.
        let build_partial_cutover = |target: PathBuf| {
            let bundle = bundle.clone();
            async move {
                let decoded = decode_bundle(&bundle, &test_kek()).await.expect("decode bundle");
                let staging = target.join(RESTORE_STAGING_DIR);
                fs::create_dir_all(&staging).await.expect("create staging");
                stage_file(
                    &staging,
                    LOCAL_KMS_MASTER_KEY_SALT_FILE,
                    decoded.salt.as_ref().expect("bundle salt"),
                    Some(0o600),
                )
                .await
                .expect("stage salt");
                for record in &decoded.records {
                    stage_file(&staging, &record.file_name, &record.plaintext, Some(0o600))
                        .await
                        .expect("stage record");
                }
                let marker = RestoreCommitMarker::for_bundle(&decoded);
                publish_marker(&target, &marker, Some(0o600)).await.expect("publish marker");
                // First link only (the salt), then "power loss".
                durable_file::link_durably(
                    staging.join(LOCAL_KMS_MASTER_KEY_SALT_FILE),
                    target.join(LOCAL_KMS_MASTER_KEY_SALT_FILE),
                )
                .await
                .expect("link salt");
            }
        };

        // Outcome 1: roll forward with the same bundle.
        let target = work.path().join("target-forward");
        build_partial_cutover(target.clone()).await;
        let error = match LocalKmsClient::new(local_config(&target, Some(TEST_MASTER_KEY))).await {
            Ok(_) => panic!("startup must fail closed mid-cutover"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unfinished restore"), "got {error}");
        assert!(matches!(
            LocalKmsClient::new_for_key_export(local_config(&target, Some(TEST_MASTER_KEY))).await,
            Err(KmsError::ConfigurationError { .. })
        ));
        let report = restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("roll-forward must converge");
        assert!(report.resumed);
        assert_restored_matches_source(&client, &target, &["alpha", "beta"]).await;

        // Outcome 2: explicit abort returns the complete old (empty) state.
        let target = work.path().join("target-abort");
        build_partial_cutover(target.clone()).await;
        abort_local_restore(&target).await.expect("abort must succeed");
        assert_eq!(
            top_level_names(&target).await,
            Vec::<String>::new(),
            "abort must take back every published file, the marker, and staging"
        );
        // The aborted target is a healthy empty directory again: both a fresh
        // backend start and a fresh restore work.
        restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("a fresh restore after abort must succeed");
        assert_restored_matches_source(&client, &target, &["alpha", "beta"]).await;
    }

    #[tokio::test]
    async fn roll_forward_requires_identical_file_set() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha", "beta"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");

        let decoded = decode_bundle(&bundle, &test_kek()).await.expect("decode bundle");
        fs::create_dir_all(target.join(RESTORE_STAGING_DIR))
            .await
            .expect("create staging");
        let mut marker = RestoreCommitMarker::for_bundle(&decoded);
        marker.files.pop();
        publish_marker(&target, &marker, Some(0o600)).await.expect("publish marker");

        let error = restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect_err("a marker with a diverging file set must fail closed");
        assert!(matches!(error, KmsError::Backup(BackupError::Corrupted { .. })), "got {error:?}");
        assert!(error.to_string().contains("different file set"), "got {error}");
    }

    #[tokio::test]
    async fn completed_restore_tolerates_inert_staging_leftover() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let work = TempDir::new().expect("work dir");
        let (bundle, _manifest) = export_bundle(&client, work.path()).await;
        let target = work.path().join("target");
        restore_local_backup(&test_kek(), &restore_request(&bundle, &target))
            .await
            .expect("restore");

        // A crash between marker removal and staging cleanup leaves an inert
        // staging directory; startup must ignore it entirely.
        let staging = target.join(RESTORE_STAGING_DIR);
        fs::create_dir_all(&staging).await.expect("recreate staging");
        fs::write(staging.join("leftover"), b"inert").await.expect("seed leftover");

        let restored = LocalKmsClient::new(local_config(&target, Some(TEST_MASTER_KEY)))
            .await
            .expect("startup must ignore a leftover staging directory");
        restored.describe_key("alpha", None).await.expect("key describes");
        assert!(staging.exists(), "startup must not touch the staging leftover");
    }

    #[tokio::test]
    async fn startup_removes_unpublished_marker_temp() {
        let (client, source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        drop(client);
        let temp_name = format!("{LOCAL_RESTORE_COMMIT_MARKER_FILE}.tmp-{}", uuid::Uuid::new_v4());
        let temp_path = source_dir.path().join(&temp_name);
        fs::write(&temp_path, b"unpublished marker").await.expect("seed marker temp");

        let client = LocalKmsClient::new(local_config(source_dir.path(), Some(TEST_MASTER_KEY)))
            .await
            .expect("an unpublished marker temp must not block startup");
        assert!(!temp_path.exists(), "startup recovery must remove the unpublished marker temp");
        client.describe_key("alpha", None).await.expect("key describes");
    }

    #[tokio::test]
    async fn abort_without_restore_in_progress_errors_and_staging_only_is_cleaned() {
        let empty = TempDir::new().expect("empty dir");
        let error = abort_local_restore(empty.path())
            .await
            .expect_err("nothing to abort must be an error");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");

        let staging_only = TempDir::new().expect("staging dir");
        let staging = staging_only.path().join(RESTORE_STAGING_DIR);
        fs::create_dir_all(&staging).await.expect("create staging");
        fs::write(staging.join("partial.key"), b"partial")
            .await
            .expect("seed partial");
        abort_local_restore(staging_only.path())
            .await
            .expect("a pre-marker abort only removes staging");
        assert_eq!(top_level_names(staging_only.path()).await, Vec::<String>::new());
    }

    /// A bundle carrying the sanitized configuration artifact stays fully
    /// restorable, and the configuration never becomes target state: restore
    /// verifies it and moves on, leaving the decision to the admin layer.
    #[tokio::test]
    async fn a_config_artifact_is_verified_but_never_restored() {
        let (client, _source_dir) = source_with_keys(Some(TEST_MASTER_KEY), &["alpha"]).await;
        let bundle_parent = TempDir::new().expect("bundle parent");
        let config_bytes = br#"{"backend":"local","note":"sanitized"}"#.to_vec();
        let (bundle, manifest) = export_bundle_with_config(&client, bundle_parent.path(), Some(config_bytes.clone())).await;
        drop(client);

        assert!(
            manifest
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == ArtifactKind::KmsConfig),
            "the bundle must carry the configuration artifact"
        );

        let target = TempDir::new().expect("target");
        let report = dry_run_local_restore(&test_kek(), &restore_request(&bundle, target.path()))
            .await
            .expect("dry-run should evaluate a bundle with a config artifact");
        assert!(report.blockers.is_empty(), "unexpected blockers: {:?}", report.blockers);

        let mut request = restore_request(&bundle, target.path());
        request.conflict_policy = RestoreConflictPolicy::RestoreIntoEmptyTarget;
        let report = restore_local_backup(&test_kek(), &request)
            .await
            .expect("restore should succeed");
        assert_eq!(report.restored_key_ids, vec!["alpha".to_string()]);

        // Only key material and the salt land in the target; the configuration
        // is evidence that stays in the bundle.
        let names = top_level_names(target.path()).await;
        assert!(names.contains(&"alpha.key".to_string()), "{names:?}");
        for name in &names {
            assert!(
                !name.contains("config"),
                "restore must not write a configuration into the key directory: {names:?}"
            );
        }
        let restored = std::fs::read(target.path().join("alpha.key")).expect("restored record");
        assert_ne!(restored, config_bytes);

        // A tampered config artifact must still fail the bundle closed.
        let descriptor = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::KmsConfig)
            .expect("config artifact");
        let artifact_path = bundle.join(&descriptor.path);
        let mut payload = std::fs::read(&artifact_path).expect("artifact bytes");
        let last = payload.len() - 1;
        payload[last] ^= 0xff;
        std::fs::write(&artifact_path, &payload).expect("tamper artifact");

        let fresh_target = TempDir::new().expect("target");
        dry_run_local_restore(&test_kek(), &restore_request(&bundle, fresh_target.path()))
            .await
            .expect("dry-run reports rather than errors")
            .blockers
            .iter()
            .find(|blocker| blocker.code == RestoreBlockerCode::BundleCorrupted)
            .expect("a tampered config artifact must be reported as corruption");
    }
}
