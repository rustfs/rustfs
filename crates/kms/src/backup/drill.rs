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

//! Disaster-recovery drill harness for the Local backend.
//!
//! A backup that has never been restored is a hypothesis. This module turns
//! it into evidence: [`run_local_drill`] rehearses the complete loop —
//! seed a deployment, seal real objects through the production encryption
//! path, export a bundle, destroy the persistence layer, preflight, restore,
//! and then *decrypt the pre-disaster ciphertext again* — and returns a
//! machine-readable [`DrillEvidence`] record of what happened and how long it
//! took.
//!
//! # Why the object probe is the acceptance criterion
//!
//! Manifest digests and artifact counts only prove the bundle is intact. The
//! question a disaster actually asks is whether historical objects are still
//! readable, so the drill keeps the ciphertext and the encryption metadata of
//! every object it sealed before the disaster and, after the restore, decrypts
//! each one through a freshly opened backend. A drill whose probes do not
//! reproduce the original plaintext byte for byte is a failed drill regardless
//! of how clean the bundle looked.
//!
//! # Sandbox only
//!
//! The drill owns everything under its workspace: it creates the deployment it
//! later destroys. It never reads, writes, or points a restore at a running
//! deployment's key directory, and the destructive step is applied to the
//! workspace copy only. Sizing the dataset to production is what makes the
//! recovery-time number transferable — the code path is identical either way.
//!
//! # RPO and RTO
//!
//! The drill writes one key *after* the export fence closed and proves it is
//! absent after the restore: the recovery point is the snapshot generation,
//! not the moment of the disaster, and the drill measures that window instead
//! of asserting it. The recovery time is the sum of the phases an operator
//! actually waits on (quarantine, preflight, restore, verification); human
//! decision time is deliberately excluded and belongs to the runbook.
//!
//! # What this module does not re-test
//!
//! Bundle-side defects (tampered, truncated, wrong-KEK, incomplete,
//! unknown-version) and every commit-point crash are covered exhaustively by
//! the unit tests of [`crate::backup::local_export`] and
//! [`crate::backup::local_restore`]. The drill exercises the operator-facing
//! loop on top of them rather than restating them.
//!
//! The procedure this harness automates, how to read the evidence, and the
//! Vault variant are documented in
//! `docs/operations/kms-disaster-recovery-drill.md`.

use crate::backends::local::{LOCAL_KMS_MASTER_KEY_SALT_FILE, LOCAL_RESTORE_COMMIT_MARKER_FILE, LocalKmsBackend};
use crate::backends::{KmsBackend as KmsBackendTrait, local::validate_key_id};
use crate::backup::capability::BackupBackendKind;
use crate::backup::dry_run::RestoreDryRunReport;
use crate::backup::local_export::{BackupKek, LocalBackupExportRequest, export_local_backup, read_local_bundle_manifest};
use crate::backup::local_restore::{
    LocalRestoreReport, LocalRestoreRequest, RestoreConflictPolicy, dry_run_local_restore, restore_local_backup,
};
use crate::backup::manifest::ContentDigest;
use crate::config::{BackendConfig, KmsConfig, LocalConfig};
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use crate::service::ObjectEncryptionService;
use crate::types::{CreateKeyRequest, EncryptionAlgorithm, EncryptionMetadata, KeyUsage};
use jiff::Zoned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Format version of [`DrillEvidence`], so an evidence consumer can reject a
/// shape it does not know instead of guessing.
pub const DRILL_EVIDENCE_FORMAT_VERSION: u32 = 1;

/// Bucket name the drill seals its sample objects under.
const DRILL_BUCKET: &str = "rustfs-kms-drill";
/// Workspace subdirectory holding the deployment the drill destroys.
const LIVE_DIR: &str = "live";
/// Workspace subdirectory holding the exported bundle.
const BUNDLE_DIR: &str = "bundle";
/// Workspace subdirectory the damaged key directory is moved aside into.
const QUARANTINE_DIR: &str = "quarantine";
/// Upper bound on a single drill run, guarding against a dataset that would
/// take an operator by surprise mid-incident-rehearsal.
const MAX_DRILL_OBJECTS: usize = 10_000;

/// Size of the deployment a drill rehearses against.
///
/// The numbers are evidence: a recovery time only transfers to production if
/// the drill restored a comparable number of keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillDataset {
    /// Master keys created before the backup.
    pub keys: usize,
    /// Objects sealed under each key and re-read after the restore.
    pub objects_per_key: usize,
    /// Plaintext size of each sample object.
    pub object_bytes: usize,
}

impl Default for DrillDataset {
    fn default() -> Self {
        Self {
            keys: 4,
            objects_per_key: 2,
            object_bytes: 4096,
        }
    }
}

/// How the drill destroys the KMS persistence layer.
///
/// Each variant is one row of the Local disaster matrix. All of them converge
/// on the same recovery procedure — quarantine what survived, restore into a
/// clean directory — which is exactly the point: the operator does not have to
/// diagnose the failure mode before acting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrillDisaster {
    /// The whole key directory is gone (lost volume, wiped host).
    KeyDirectoryLost,
    /// The key records survive but the KDF salt is gone, so none of their
    /// material can be unwrapped.
    MasterKeySaltLost,
    /// One key record is truncated in place (torn write, partial media
    /// failure).
    KeyRecordCorrupted,
}

/// One drill phase, in execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrillPhase {
    /// Create the deployment and its master keys.
    Seed,
    /// Seal the sample objects that the restore will later have to make
    /// readable again.
    SealObjects,
    /// Export the backup bundle.
    Export,
    /// Write past the snapshot fence, establishing the recovery point.
    PostSnapshotWrite,
    /// Destroy the persistence layer.
    Disaster,
    /// Move what survived aside so the restore target is clean.
    Quarantine,
    /// Zero-write restore preflight.
    DryRun,
    /// Execute the restore.
    Restore,
    /// Decrypt every pre-disaster sample object again.
    Verify,
    /// Re-run the restore against the recovered target.
    RepeatRestore,
}

impl DrillPhase {
    /// Whether the phase is part of the recovery an operator waits on.
    ///
    /// Seeding, sealing, exporting and the disaster itself are drill
    /// scaffolding, not recovery work, so they stay out of the recovery-time
    /// total.
    fn counts_towards_recovery_time(self) -> bool {
        matches!(
            self,
            DrillPhase::Quarantine | DrillPhase::DryRun | DrillPhase::Restore | DrillPhase::Verify
        )
    }
}

/// Wall-clock cost of one phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillPhaseTiming {
    /// Which phase.
    pub phase: DrillPhase,
    /// Duration in milliseconds.
    pub millis: u64,
}

/// Outcome of decrypting one pre-disaster sample object after the restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvelopeProbe {
    /// Master key the object's data key was wrapped under.
    pub key_id: String,
    /// Bucket the object was sealed under.
    pub bucket: String,
    /// Object key.
    pub object_key: String,
    /// Digest of the plaintext as it was before the disaster.
    pub plaintext_digest: ContentDigest,
    /// Whether the restored deployment reproduced that plaintext exactly.
    pub verified: bool,
    /// Failure detail when it did not. Identifiers and error text only.
    pub detail: Option<String>,
}

/// What the bundle was, and what it still was after the recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillBundleEvidence {
    /// Bundle identifier.
    pub backup_id: String,
    /// Snapshot generation the bundle belongs to.
    pub snapshot_generation: u64,
    /// Sealed manifest digest recorded at export time.
    pub manifest_digest: ContentDigest,
    /// Number of artifacts in the bundle.
    pub artifact_count: usize,
    /// Total bundle size on disk.
    pub bundle_bytes: u64,
    /// Manifest digest re-read after the whole recovery.
    pub manifest_digest_after_recovery: ContentDigest,
    /// Whether every byte of the bundle directory survived the recovery
    /// unchanged. Restore must treat its source as strictly read-only.
    pub source_unchanged: bool,
}

/// What the disaster destroyed and how the recovery proceeded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillRecoveryEvidence {
    /// Top-level names the disaster removed or damaged.
    pub destroyed_files: Vec<String>,
    /// Where the damaged directory was preserved. Nothing is ever destroyed
    /// by the recovery itself: the pre-restore state stays available for
    /// forensics and rollback.
    pub quarantine_path: PathBuf,
    /// Top-level names that were moved into quarantine.
    pub quarantined_files: Vec<String>,
    /// The preflight report, verbatim.
    pub dry_run: RestoreDryRunReport,
    /// Whether the target was byte-for-byte identical before and after the
    /// preflight.
    pub dry_run_zero_write: bool,
    /// The restore report, verbatim. Absent when the preflight refused the
    /// recovery and the drill therefore never restored.
    pub restore: Option<LocalRestoreReport>,
    /// Whether the cutover commit marker was gone once the restore returned.
    /// A surviving marker means the target is still mid-cutover.
    pub commit_marker_cleared: bool,
    /// Whether re-running the same restore against the recovered target was
    /// refused.
    pub repeat_restore_refused: bool,
    /// Whether that refused re-run left the recovered target untouched.
    pub repeat_restore_left_target_unchanged: bool,
}

/// The measured recovery point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillRpoEvidence {
    /// When the export sealed the bundle.
    #[serde(with = "crate::time_serde::zoned")]
    pub snapshot_completed_at: Zoned,
    /// When the disaster struck.
    #[serde(with = "crate::time_serde::zoned")]
    pub disaster_at: Zoned,
    /// The data-loss window: work committed inside it is not in the bundle.
    pub rpo_window_millis: u64,
    /// Keys created after the fence closed, which the bundle cannot contain.
    pub post_snapshot_key_ids: Vec<String>,
    /// Objects sealed under those keys.
    pub post_snapshot_objects: usize,
    /// How many of them were readable again after the restore. Anything above
    /// zero means the bundle was not a point-in-time snapshot.
    pub post_snapshot_objects_recovered: usize,
}

/// Overall drill outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrillVerdict {
    /// Every check held.
    Passed,
    /// At least one check did not; see the findings.
    Failed,
}

/// Machine-readable record of one drill run.
///
/// Identifiers, digests, and durations only: no key material, no master key,
/// no plaintext, no ciphertext. It is meant to be archived next to the
/// incident record as the immutable evidence a drill actually happened and
/// what it proved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillEvidence {
    /// Evidence schema version.
    pub format_version: u32,
    /// Caller-chosen identifier of this run.
    pub drill_id: String,
    /// Backend the drill exercised.
    pub backend: BackupBackendKind,
    /// Deployment identity the rehearsal deployment carried.
    pub deployment_identity: String,
    /// RustFS version recorded in the bundle manifest.
    pub rustfs_version: String,
    /// Disaster that was simulated.
    pub disaster: DrillDisaster,
    /// Size of the rehearsed deployment.
    pub dataset: DrillDataset,
    /// When the run started.
    #[serde(with = "crate::time_serde::zoned")]
    pub started_at: Zoned,
    /// Bundle evidence.
    pub bundle: DrillBundleEvidence,
    /// Recovery evidence.
    pub recovery: DrillRecoveryEvidence,
    /// Recovery-point evidence.
    pub rpo: DrillRpoEvidence,
    /// Per-phase durations.
    pub timings: Vec<DrillPhaseTiming>,
    /// Recovery time: the phases an operator waits on.
    pub rto_millis: u64,
    /// One entry per sample object re-read after the restore.
    pub envelope_probes: Vec<EnvelopeProbe>,
    /// Every check that did not hold, in the order they were observed.
    pub findings: Vec<String>,
    /// Whether the drill passed.
    pub verdict: DrillVerdict,
}

impl DrillEvidence {
    /// Serialize the evidence for archival.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|error| KmsError::internal_error(format!("drill evidence does not serialize: {error}")))
    }
}

/// Parameters of one drill run.
pub struct DrillRequest {
    /// Identifier of this run; also the prefix of every key the drill creates.
    pub drill_id: String,
    /// Scratch directory the drill owns entirely. Created if absent; must not
    /// already contain a drill workspace.
    pub workspace: PathBuf,
    /// Deployment identity stamped into the bundle and required to match on
    /// restore.
    pub deployment_identity: String,
    /// RustFS version string recorded in the manifest.
    pub rustfs_version: String,
    /// Snapshot generation for the exported bundle.
    pub snapshot_generation: u64,
    /// Size of the rehearsed deployment.
    pub dataset: DrillDataset,
    /// Disaster to simulate.
    pub disaster: DrillDisaster,
    /// At-rest master key of the rehearsal deployment. Supply a throwaway
    /// value: the drill's own deployment is the only thing it protects.
    pub master_key: String,
    /// Unix permission bits for key files.
    pub file_permissions: Option<u32>,
}

impl std::fmt::Debug for DrillRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrillRequest")
            .field("drill_id", &self.drill_id)
            .field("workspace", &self.workspace)
            .field("deployment_identity", &self.deployment_identity)
            .field("rustfs_version", &self.rustfs_version)
            .field("snapshot_generation", &self.snapshot_generation)
            .field("dataset", &self.dataset)
            .field("disaster", &self.disaster)
            .field("master_key", &"<redacted>")
            .field("file_permissions", &self.file_permissions)
            .finish()
    }
}

impl DrillRequest {
    fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("drill_id", &self.drill_id),
            ("deployment_identity", &self.deployment_identity),
            ("rustfs_version", &self.rustfs_version),
            ("master_key", &self.master_key),
        ] {
            if value.is_empty() {
                return Err(KmsError::validation_error(format!("drill {field} must not be empty")));
            }
        }
        // Key ids are derived from the drill id, so an id that cannot name a
        // key file has to fail here rather than half way through seeding.
        validate_key_id(&self.drill_id)?;
        if !self.workspace.is_absolute() {
            return Err(KmsError::validation_error("drill workspace must be an absolute path"));
        }
        if self.dataset.keys == 0 || self.dataset.objects_per_key == 0 || self.dataset.object_bytes == 0 {
            return Err(KmsError::validation_error(
                "drill dataset must contain at least one key, one object per key, and one byte per object",
            ));
        }
        let objects = self
            .dataset
            .keys
            .checked_mul(self.dataset.objects_per_key)
            .ok_or_else(|| KmsError::validation_error("drill dataset size overflows"))?;
        if objects > MAX_DRILL_OBJECTS {
            return Err(KmsError::validation_error(format!(
                "drill dataset of {objects} objects exceeds the {MAX_DRILL_OBJECTS} object limit"
            )));
        }
        Ok(())
    }

    fn key_id(&self, index: usize) -> String {
        format!("{}-key-{index:04}", self.drill_id)
    }

    fn post_snapshot_key_id(&self) -> String {
        format!("{}-post-snapshot", self.drill_id)
    }

    fn local_config(&self, key_dir: PathBuf) -> KmsConfig {
        let mut config = KmsConfig::local(key_dir.clone());
        config.backend_config = BackendConfig::Local(LocalConfig {
            key_dir,
            master_key: Some(self.master_key.clone()),
            file_permissions: self.file_permissions,
        });
        // The drill re-opens the backend after the restore to prove the
        // material on disk decrypts; a warm cache would prove nothing.
        config.enable_cache = false;
        // The rehearsal deployment is throwaway by construction: it is created
        // inside the drill workspace, destroyed by the drill, and never
        // registered as a server's KMS. The production-hygiene guard on where
        // a key directory may live therefore does not apply to it — a drill
        // workspace under the temp directory is the normal case, not a
        // misconfiguration. The one guard that still matters, a non-empty
        // at-rest master key, is enforced by `DrillRequest::validate`.
        config.allow_insecure_dev_defaults = true;
        config
    }
}

/// One object sealed before the disaster, kept so the restore can be asked to
/// make it readable again.
struct SealedObject {
    key_id: String,
    object_key: String,
    plaintext_digest: ContentDigest,
    ciphertext: Vec<u8>,
    metadata: EncryptionMetadata,
}

/// Collects per-phase wall-clock cost.
struct PhaseClock {
    timings: Vec<DrillPhaseTiming>,
    started: Instant,
}

impl PhaseClock {
    fn new() -> Self {
        Self {
            timings: Vec::new(),
            started: Instant::now(),
        }
    }

    /// Close the current phase and open the next one.
    fn finish(&mut self, phase: DrillPhase) {
        let now = Instant::now();
        self.timings.push(DrillPhaseTiming {
            phase,
            millis: now.duration_since(self.started).as_millis().min(u128::from(u64::MAX)) as u64,
        });
        self.started = now;
    }

    fn recovery_time_millis(&self) -> u64 {
        self.timings
            .iter()
            .filter(|timing| timing.phase.counts_towards_recovery_time())
            .map(|timing| timing.millis)
            .sum()
    }
}

/// Run one disaster-recovery drill and return its evidence.
///
/// Environment failures (an unusable workspace, a backend that will not start)
/// are errors. Verdicts about the recovery itself — a probe that did not
/// reproduce its plaintext, a preflight that wrote, a bundle the restore
/// modified — are findings inside the returned evidence, so a failed drill
/// still produces the record of *how* it failed.
pub async fn run_local_drill(kek: &BackupKek, request: &DrillRequest) -> Result<DrillEvidence> {
    request.validate()?;
    let started_at = Zoned::now();
    let mut clock = PhaseClock::new();
    let mut findings = Vec::new();

    let live_dir = request.workspace.join(LIVE_DIR);
    let bundle_dir = request.workspace.join(BUNDLE_DIR);
    let quarantine_dir = request.workspace.join(QUARANTINE_DIR);
    prepare_workspace(&request.workspace, &live_dir).await?;

    let config = request.local_config(live_dir.clone());

    // The seeded deployment and its bundle. The backend is dropped before the
    // disaster so no live handle outlives the state it points at.
    let (sealed, rpo_sealed, manifest, snapshot_completed_at) = {
        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await?);
        let service = ObjectEncryptionService::new(KmsManager::new(backend.clone(), config.clone()));

        let mut key_ids = Vec::with_capacity(request.dataset.keys);
        for index in 0..request.dataset.keys {
            let key_id = request.key_id(index);
            create_drill_key(&service, &key_id).await?;
            key_ids.push(key_id);
        }
        clock.finish(DrillPhase::Seed);

        let mut sealed = Vec::with_capacity(request.dataset.keys * request.dataset.objects_per_key);
        for key_id in &key_ids {
            for index in 0..request.dataset.objects_per_key {
                sealed.push(seal_object(&service, key_id, index, request.dataset.object_bytes).await?);
            }
        }
        clock.finish(DrillPhase::SealObjects);

        let client = backend
            .local_backup_client()
            .ok_or_else(|| KmsError::internal_error("local backend did not expose its backup client"))?;
        let manifest = export_local_backup(
            client,
            kek,
            &LocalBackupExportRequest {
                backup_id: request.drill_id.clone(),
                deployment_identity: request.deployment_identity.clone(),
                rustfs_version: request.rustfs_version.clone(),
                snapshot_generation: request.snapshot_generation,
                destination: bundle_dir.clone(),
                sanitized_config: None,
            },
        )
        .await?;
        let snapshot_completed_at = Zoned::now();
        clock.finish(DrillPhase::Export);

        // Past the fence: this key belongs to the window the bundle cannot
        // cover, and the restore must not resurrect it.
        let post_snapshot_key = request.post_snapshot_key_id();
        create_drill_key(&service, &post_snapshot_key).await?;
        let mut rpo_sealed = Vec::with_capacity(request.dataset.objects_per_key);
        for index in 0..request.dataset.objects_per_key {
            rpo_sealed.push(seal_object(&service, &post_snapshot_key, index, request.dataset.object_bytes).await?);
        }
        clock.finish(DrillPhase::PostSnapshotWrite);

        (sealed, rpo_sealed, manifest, snapshot_completed_at)
    };

    let bundle_digest_before = tree_digest(&bundle_dir).await?;

    let destroyed_files = apply_disaster(&live_dir, request.disaster).await?;
    let disaster_at = Zoned::now();
    clock.finish(DrillPhase::Disaster);

    // Recovery step one is always the same: preserve what survived, then
    // restore into a clean directory. Nothing damaged is deleted.
    let quarantined_files = quarantine(&live_dir, &quarantine_dir).await?;
    clock.finish(DrillPhase::Quarantine);

    let restore_request = LocalRestoreRequest {
        bundle_dir: bundle_dir.clone(),
        target_key_dir: live_dir.clone(),
        target_deployment_identity: request.deployment_identity.clone(),
        observed_generation: None,
        master_key: Some(request.master_key.clone()),
        conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
        file_permissions: request.file_permissions,
    };

    let target_before_dry_run = tree_digest(&live_dir).await?;
    let dry_run = dry_run_local_restore(kek, &restore_request).await?;
    let dry_run_zero_write = tree_digest(&live_dir).await? == target_before_dry_run;
    clock.finish(DrillPhase::DryRun);
    if !dry_run_zero_write {
        findings.push("restore preflight modified the target; a dry-run must be zero-write".to_string());
    }
    // A refused preflight ends the drill: proceeding anyway is exactly the
    // behavior the restore contract forbids. The evidence is still produced,
    // because *why* a rehearsed recovery was refused is the finding.
    let permitted = dry_run.restore_permitted();
    if !permitted {
        findings.push(format!(
            "restore preflight refused the recovery: {} blocker(s), {} conflict(s)",
            dry_run.blockers.len(),
            dry_run.conflicts.len()
        ));
    }

    let mut restore_report = None;
    let mut commit_marker_cleared = false;
    let mut envelope_probes = Vec::new();
    let mut post_snapshot_objects_recovered = 0usize;
    let mut repeat_restore_refused = false;
    let mut repeat_restore_left_target_unchanged = false;

    if permitted {
        let restore = restore_local_backup(kek, &restore_request).await?;
        commit_marker_cleared = !fs::try_exists(live_dir.join(LOCAL_RESTORE_COMMIT_MARKER_FILE)).await?;
        clock.finish(DrillPhase::Restore);
        if !commit_marker_cleared {
            findings.push("the cutover commit marker survived a completed restore".to_string());
        }
        if restore.backup_id != manifest.backup_id || restore.snapshot_generation != manifest.snapshot_generation {
            findings.push(format!(
                "restore reported bundle '{}' at generation {}, the exported bundle was '{}' at generation {}",
                restore.backup_id, restore.snapshot_generation, manifest.backup_id, manifest.snapshot_generation
            ));
        }
        restore_report = Some(restore);

        // The acceptance criterion: a freshly opened backend over the restored
        // directory must reproduce every pre-disaster plaintext.
        {
            let backend = Arc::new(LocalKmsBackend::new(config.clone()).await?);
            let service = ObjectEncryptionService::new(KmsManager::new(backend, config.clone()));
            for object in &sealed {
                envelope_probes.push(probe_object(&service, object).await);
            }
            for object in &rpo_sealed {
                if probe_object(&service, object).await.verified {
                    post_snapshot_objects_recovered += 1;
                }
            }
        }
        clock.finish(DrillPhase::Verify);

        for probe in &envelope_probes {
            if !probe.verified {
                findings.push(format!(
                    "object '{}' under key '{}' did not decrypt to its pre-disaster plaintext",
                    probe.object_key, probe.key_id
                ));
            }
        }
        if post_snapshot_objects_recovered != 0 {
            findings.push(format!(
                "{post_snapshot_objects_recovered} object(s) written after the snapshot fence were readable again; \
                 the bundle is not a point-in-time snapshot"
            ));
        }

        // A second restore into the recovered target must be refused without
        // touching it: repeating a drill, or an operator running the runbook
        // twice, must never be able to damage a healthy deployment.
        let target_before_repeat = tree_digest(&live_dir).await?;
        repeat_restore_refused = restore_local_backup(kek, &restore_request).await.is_err();
        repeat_restore_left_target_unchanged = tree_digest(&live_dir).await? == target_before_repeat;
        clock.finish(DrillPhase::RepeatRestore);
        if !repeat_restore_refused {
            findings.push("re-running the restore against the recovered target was accepted".to_string());
        }
        if !repeat_restore_left_target_unchanged {
            findings.push("the refused re-run modified the recovered target".to_string());
        }
    }

    let manifest_after = read_local_bundle_manifest(&bundle_dir).await?;
    let source_unchanged = tree_digest(&bundle_dir).await? == bundle_digest_before;
    if !source_unchanged || manifest_after.manifest_digest != manifest.manifest_digest {
        findings.push("the bundle changed during the recovery; a restore source must be read-only".to_string());
    }

    let verdict = if findings.is_empty() {
        DrillVerdict::Passed
    } else {
        DrillVerdict::Failed
    };

    Ok(DrillEvidence {
        format_version: DRILL_EVIDENCE_FORMAT_VERSION,
        drill_id: request.drill_id.clone(),
        backend: BackupBackendKind::Local,
        deployment_identity: request.deployment_identity.clone(),
        rustfs_version: request.rustfs_version.clone(),
        disaster: request.disaster,
        dataset: request.dataset,
        started_at,
        bundle: DrillBundleEvidence {
            backup_id: manifest.backup_id.clone(),
            snapshot_generation: manifest.snapshot_generation,
            manifest_digest: manifest.manifest_digest.clone(),
            artifact_count: manifest.artifacts.len(),
            bundle_bytes: directory_bytes(&bundle_dir).await?,
            manifest_digest_after_recovery: manifest_after.manifest_digest,
            source_unchanged,
        },
        recovery: DrillRecoveryEvidence {
            destroyed_files,
            quarantine_path: quarantine_dir,
            quarantined_files,
            dry_run,
            dry_run_zero_write,
            restore: restore_report,
            commit_marker_cleared,
            repeat_restore_refused,
            repeat_restore_left_target_unchanged,
        },
        rpo: DrillRpoEvidence {
            rpo_window_millis: elapsed_millis(&snapshot_completed_at, &disaster_at),
            snapshot_completed_at,
            disaster_at,
            post_snapshot_key_ids: vec![request.post_snapshot_key_id()],
            post_snapshot_objects: rpo_sealed.len(),
            post_snapshot_objects_recovered,
        },
        rto_millis: clock.recovery_time_millis(),
        timings: clock.timings,
        envelope_probes,
        findings,
        verdict,
    })
}

async fn prepare_workspace(workspace: &Path, live_dir: &Path) -> Result<()> {
    fs::create_dir_all(workspace).await?;
    if fs::try_exists(live_dir).await? {
        return Err(KmsError::invalid_operation(format!(
            "drill workspace {} already holds a rehearsal deployment; point the drill at a fresh directory",
            workspace.display()
        )));
    }
    fs::create_dir_all(live_dir).await?;
    Ok(())
}

async fn create_drill_key(service: &ObjectEncryptionService, key_id: &str) -> Result<()> {
    service
        .create_key(CreateKeyRequest {
            key_name: Some(key_id.to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("rustfs kms disaster-recovery drill".to_string()),
            policy: None,
            tags: HashMap::new(),
            origin: Some("kms-dr-drill".to_string()),
        })
        .await?;
    Ok(())
}

/// Seal one sample object through the production encryption path and keep
/// exactly what a real deployment would keep: the ciphertext and its
/// encryption metadata.
async fn seal_object(service: &ObjectEncryptionService, key_id: &str, index: usize, bytes: usize) -> Result<SealedObject> {
    let object_key = format!("{key_id}/object-{index:04}");
    let plaintext = drill_payload(&object_key, bytes);
    let plaintext_digest = ContentDigest::sha256_of(&plaintext);
    let result = service
        .encrypt_object(
            DRILL_BUCKET,
            &object_key,
            Cursor::new(plaintext),
            &EncryptionAlgorithm::Aes256,
            Some(key_id),
            None,
        )
        .await?;
    Ok(SealedObject {
        key_id: key_id.to_string(),
        object_key,
        plaintext_digest,
        ciphertext: result.ciphertext,
        metadata: result.metadata,
    })
}

/// Ask the restored deployment to unwrap one object's data key and reproduce
/// its plaintext.
async fn probe_object(service: &ObjectEncryptionService, object: &SealedObject) -> EnvelopeProbe {
    let mut probe = EnvelopeProbe {
        key_id: object.key_id.clone(),
        bucket: DRILL_BUCKET.to_string(),
        object_key: object.object_key.clone(),
        plaintext_digest: object.plaintext_digest.clone(),
        verified: false,
        detail: None,
    };

    let decrypted = service
        .decrypt_object(DRILL_BUCKET, &object.object_key, object.ciphertext.clone(), &object.metadata, None)
        .await;
    let mut reader = match decrypted {
        Ok(reader) => reader,
        Err(error) => {
            probe.detail = Some(error.to_string());
            return probe;
        }
    };

    let mut plaintext = Vec::new();
    if let Err(error) = reader.read_to_end(&mut plaintext).await {
        probe.detail = Some(error.to_string());
        return probe;
    }
    if ContentDigest::sha256_of(&plaintext) == object.plaintext_digest {
        probe.verified = true;
    } else {
        probe.detail = Some("decrypted plaintext does not match the pre-disaster digest".to_string());
    }
    probe
}

/// Deterministic, incompressible-enough sample payload derived from the object
/// key, so a drill never needs a fixture file and two runs of the same drill
/// seal the same bytes.
fn drill_payload(object_key: &str, bytes: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(bytes);
    let mut block = Sha256::digest(object_key.as_bytes());
    while payload.len() < bytes {
        let take = block.len().min(bytes - payload.len());
        payload.extend_from_slice(&block[..take]);
        block = Sha256::digest(block);
    }
    payload
}

/// Destroy the persistence layer and report what was destroyed.
async fn apply_disaster(live_dir: &Path, disaster: DrillDisaster) -> Result<Vec<String>> {
    match disaster {
        DrillDisaster::KeyDirectoryLost => {
            let names = top_level_names(live_dir).await?;
            fs::remove_dir_all(live_dir).await?;
            Ok(names)
        }
        DrillDisaster::MasterKeySaltLost => {
            let salt = live_dir.join(LOCAL_KMS_MASTER_KEY_SALT_FILE);
            if !fs::try_exists(&salt).await? {
                return Err(KmsError::internal_error(
                    "drill deployment has no master-key salt to destroy; the seed phase did not produce one",
                ));
            }
            fs::remove_file(&salt).await?;
            Ok(vec![LOCAL_KMS_MASTER_KEY_SALT_FILE.to_string()])
        }
        DrillDisaster::KeyRecordCorrupted => {
            let mut records: Vec<String> = top_level_names(live_dir)
                .await?
                .into_iter()
                .filter(|name| name.ends_with(".key"))
                .collect();
            records.sort();
            let victim = records
                .first()
                .ok_or_else(|| KmsError::internal_error("drill deployment has no key record to corrupt"))?;
            let path = live_dir.join(victim);
            let mut content = fs::read(&path).await?;
            content.truncate(content.len() / 2);
            fs::write(&path, &content).await?;
            Ok(vec![victim.clone()])
        }
    }
}

/// Move whatever survived the disaster aside, leaving a clean restore target.
///
/// The damaged state is preserved, never deleted: a restore that turns out to
/// be the wrong decision must remain reversible, and forensics need the
/// original bytes.
async fn quarantine(live_dir: &Path, quarantine_dir: &Path) -> Result<Vec<String>> {
    if !fs::try_exists(live_dir).await? {
        fs::create_dir_all(live_dir).await?;
        return Ok(Vec::new());
    }
    let names = top_level_names(live_dir).await?;
    if names.is_empty() {
        return Ok(Vec::new());
    }
    fs::rename(live_dir, quarantine_dir).await?;
    fs::create_dir_all(live_dir).await?;
    Ok(names)
}

async fn top_level_names(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut entries = match fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(names),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    Ok(names)
}

async fn directory_bytes(dir: &Path) -> Result<u64> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut total = 0u64;
        for path in walk_files(&dir)? {
            total = total.saturating_add(std::fs::metadata(&path)?.len());
        }
        Ok(total)
    })
    .await
    .map_err(|error| KmsError::internal_error(format!("drill directory sizing failed: {error}")))?
}

/// Canonical digest over a directory tree: relative path, length, content
/// digest, and modification time of every file. Equality across an operation
/// is proof that the operation performed no persistent write.
async fn tree_digest(root: &Path) -> Result<ContentDigest> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut lines = Vec::new();
        for path in walk_files(&root)? {
            let metadata = std::fs::metadata(&path)?;
            let modified = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos());
            let relative = path
                .strip_prefix(&root)
                .unwrap_or(path.as_path())
                .to_string_lossy()
                .into_owned();
            let content = std::fs::read(&path)?;
            lines.push(format!(
                "{relative}\u{1f}{}\u{1f}{modified}\u{1f}{}",
                metadata.len(),
                hex::encode(Sha256::digest(&content))
            ));
        }
        lines.sort();
        Ok(ContentDigest::sha256_of(lines.join("\n").as_bytes()))
    })
    .await
    .map_err(|error| KmsError::internal_error(format!("drill tree digest failed: {error}")))?
}

/// Every regular file under `root`, recursively. A missing root is an empty
/// tree, not an error: the drill digests directories the disaster may have
/// removed.
fn walk_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn elapsed_millis(from: &Zoned, to: &Zoned) -> u64 {
    let nanos = to.timestamp().as_nanosecond() - from.timestamp().as_nanosecond();
    u64::try_from(nanos / 1_000_000).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::local::durable_file::{CommitStep, failpoint};
    use crate::backends::local::is_orphan_commit_temp_name;
    use crate::backup::capability::{AtRestProtection, BackupResponsibility};
    use crate::backup::local_export::{AEAD_NONCE_LEN, LOCAL_BUNDLE_MANIFEST_FILE, artifact_aad};
    use crate::backup::local_restore::abort_local_restore;
    use crate::backup::manifest::{
        AeadAlgorithm, ArtifactDescriptor, ArtifactKind, BackupManifest, CompletenessState, DigestAlgorithm,
        VaultExternalReferences, VaultKvRecordReference, VaultTransitReference,
    };
    use crate::backup::vault_restore::{
        VAULT_RECORD_ARTIFACT_DIR, VAULT_RECORD_ARTIFACT_SUFFIX, VaultRestoreClient, VaultRestoreRequest, VaultRestoreTarget,
        dry_run_vault_restore,
    };
    use crate::config::VaultAuthMethod;
    use aes_gcm::{
        Nonce,
        aead::{Aead, Payload},
    };
    use tempfile::TempDir;

    /// Deliberately distinctive so the evidence assertion below cannot pass by
    /// accident.
    const DRILL_MASTER_KEY: &str = "drill-master-key-never-in-evidence";
    const DEPLOYMENT: &str = "deployment-drill";

    fn test_kek() -> BackupKek {
        BackupKek::new("backup-kek-drill", 1, [0x37; 32]).expect("kek")
    }

    fn drill_request(workspace: &Path, disaster: DrillDisaster) -> DrillRequest {
        DrillRequest {
            drill_id: "drill-0001".to_string(),
            workspace: workspace.to_path_buf(),
            deployment_identity: DEPLOYMENT.to_string(),
            rustfs_version: "1.0.0-test".to_string(),
            snapshot_generation: 11,
            dataset: DrillDataset {
                keys: 2,
                objects_per_key: 2,
                object_bytes: 1024,
            },
            disaster,
            master_key: DRILL_MASTER_KEY.to_string(),
            file_permissions: Some(0o600),
        }
    }

    /// Open the restored directory and ask it to reproduce one object.
    async fn probe_after_restore(config: &KmsConfig, object: &SealedObject) -> EnvelopeProbe {
        let backend = Arc::new(
            LocalKmsBackend::new(config.clone())
                .await
                .expect("restored backend must open"),
        );
        let service = ObjectEncryptionService::new(KmsManager::new(backend, config.clone()));
        probe_object(&service, object).await
    }

    /// Seed a deployment, seal one object, and export a bundle.
    ///
    /// The interrupted-cutover legs drive the restore themselves — they have
    /// to crash it — so they cannot go through [`run_local_drill`], but they
    /// use the same production path to produce the state they crash on.
    async fn interrupted_cutover_fixture(work: &Path) -> (KmsConfig, LocalRestoreRequest, BackupManifest, SealedObject) {
        let request = drill_request(work, DrillDisaster::KeyDirectoryLost);
        let source_dir = work.join("source");
        fs::create_dir_all(&source_dir).await.expect("source dir");
        let source_config = request.local_config(source_dir);
        let backend = Arc::new(LocalKmsBackend::new(source_config.clone()).await.expect("source backend"));
        let service = ObjectEncryptionService::new(KmsManager::new(backend.clone(), source_config.clone()));

        let key_id = request.key_id(0);
        create_drill_key(&service, &key_id).await.expect("create key");
        let sealed = seal_object(&service, &key_id, 0, 512).await.expect("seal object");

        let bundle_dir = work.join("bundle");
        let manifest = export_local_backup(
            backend.local_backup_client().expect("local backup client"),
            &test_kek(),
            &LocalBackupExportRequest {
                backup_id: request.drill_id.clone(),
                deployment_identity: DEPLOYMENT.to_string(),
                rustfs_version: request.rustfs_version.clone(),
                snapshot_generation: request.snapshot_generation,
                destination: bundle_dir.clone(),
                sanitized_config: None,
            },
        )
        .await
        .expect("export");

        let target = work.join("target");
        let restore_request = LocalRestoreRequest {
            bundle_dir,
            target_key_dir: target.clone(),
            target_deployment_identity: DEPLOYMENT.to_string(),
            observed_generation: None,
            master_key: Some(DRILL_MASTER_KEY.to_string()),
            conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
            file_permissions: Some(0o600),
        };
        (request.local_config(target), restore_request, manifest, sealed)
    }

    /// Crash a restore precisely at its commit point: arming the failpoint on
    /// the marker path itself leaves the staging commits untouched, so what
    /// survives is a fully staged restore with a published marker and no
    /// cutover — the state a power loss produces between the two.
    async fn crash_at_commit_point(restore_request: &LocalRestoreRequest) {
        let marker_path = restore_request.target_key_dir.join(LOCAL_RESTORE_COMMIT_MARKER_FILE);
        failpoint::arm(&marker_path, CommitStep::DirSynced);
        let error = restore_local_backup(&test_kek(), restore_request)
            .await
            .expect_err("the armed commit point must simulate a crash");
        failpoint::disarm(&marker_path);
        assert!(error.to_string().contains("injected crash"), "got {error}");
        assert!(marker_path.exists(), "the commit marker must be published before the crash");
    }

    #[tokio::test]
    async fn every_local_disaster_recovers_and_replays_historical_object_deks() {
        for disaster in [
            DrillDisaster::KeyDirectoryLost,
            DrillDisaster::MasterKeySaltLost,
            DrillDisaster::KeyRecordCorrupted,
        ] {
            let work = TempDir::new().expect("work dir");
            let request = drill_request(work.path(), disaster);
            let evidence = run_local_drill(&test_kek(), &request)
                .await
                .unwrap_or_else(|error| panic!("{disaster:?}: drill failed to run: {error}"));

            assert_eq!(evidence.verdict, DrillVerdict::Passed, "{disaster:?}: {:?}", evidence.findings);
            assert!(evidence.findings.is_empty(), "{disaster:?}: {:?}", evidence.findings);

            // The acceptance criterion: every object sealed before the
            // disaster decrypts again.
            assert_eq!(evidence.envelope_probes.len(), 4, "{disaster:?}");
            assert!(evidence.envelope_probes.iter().all(|probe| probe.verified), "{disaster:?}");

            // ... and the recovery point holds: work past the fence stays lost.
            assert_eq!(evidence.rpo.post_snapshot_objects, 2, "{disaster:?}");
            assert_eq!(evidence.rpo.post_snapshot_objects_recovered, 0, "{disaster:?}");
            assert_eq!(evidence.rpo.post_snapshot_key_ids, vec![request.post_snapshot_key_id()], "{disaster:?}");

            assert!(evidence.recovery.dry_run_zero_write, "{disaster:?}");
            assert!(evidence.recovery.dry_run.restore_permitted(), "{disaster:?}");
            let restore = evidence.recovery.restore.as_ref().expect("restore report");
            assert_eq!(restore.restored_key_ids.len(), 2, "{disaster:?}: the post-fence key is not in the bundle");
            assert!(restore.salt_restored, "{disaster:?}");
            assert!(!restore.resumed, "{disaster:?}");
            assert!(evidence.recovery.commit_marker_cleared, "{disaster:?}");
            assert!(evidence.recovery.repeat_restore_refused, "{disaster:?}");
            assert!(evidence.recovery.repeat_restore_left_target_unchanged, "{disaster:?}");

            assert!(evidence.bundle.source_unchanged, "{disaster:?}");
            assert_eq!(
                evidence.bundle.manifest_digest, evidence.bundle.manifest_digest_after_recovery,
                "{disaster:?}"
            );
            assert_eq!(evidence.bundle.snapshot_generation, request.snapshot_generation, "{disaster:?}");

            assert!(!evidence.recovery.destroyed_files.is_empty(), "{disaster:?}");
            assert_eq!(
                evidence.timings.iter().map(|timing| timing.phase).collect::<Vec<_>>(),
                vec![
                    DrillPhase::Seed,
                    DrillPhase::SealObjects,
                    DrillPhase::Export,
                    DrillPhase::PostSnapshotWrite,
                    DrillPhase::Disaster,
                    DrillPhase::Quarantine,
                    DrillPhase::DryRun,
                    DrillPhase::Restore,
                    DrillPhase::Verify,
                    DrillPhase::RepeatRestore,
                ],
                "{disaster:?}"
            );
        }
    }

    /// Whatever survives a disaster is preserved, never deleted: a restore
    /// that turns out to be the wrong call has to stay reversible.
    #[tokio::test]
    async fn the_damaged_key_directory_is_preserved_for_rollback() {
        let work = TempDir::new().expect("work dir");
        let request = drill_request(work.path(), DrillDisaster::KeyRecordCorrupted);
        let evidence = run_local_drill(&test_kek(), &request).await.expect("drill");

        assert_eq!(evidence.recovery.destroyed_files, vec![format!("{}.key", request.key_id(0))]);
        let quarantined = &evidence.recovery.quarantined_files;
        assert!(quarantined.contains(&LOCAL_KMS_MASTER_KEY_SALT_FILE.to_string()), "got {quarantined:?}");
        assert!(quarantined.contains(&format!("{}.key", request.key_id(1))), "got {quarantined:?}");
        assert_eq!(
            top_level_names(&evidence.recovery.quarantine_path).await.expect("quarantine"),
            *quarantined,
            "the quarantined directory must still hold exactly what was moved into it"
        );
    }

    #[tokio::test]
    async fn drill_evidence_is_archivable_and_names_no_secret() {
        let work = TempDir::new().expect("work dir");
        let request = drill_request(work.path(), DrillDisaster::KeyDirectoryLost);
        let evidence = run_local_drill(&test_kek(), &request).await.expect("drill");

        let encoded = evidence.encode().expect("evidence must serialize");
        let text = String::from_utf8(encoded.clone()).expect("evidence is utf-8");
        assert!(!text.contains(DRILL_MASTER_KEY), "the evidence must not carry the master key");
        assert!(
            !text.contains(&hex::encode([0x37u8; 32])),
            "the evidence must not carry backup KEK material"
        );

        let decoded: DrillEvidence = serde_json::from_slice(&encoded).expect("evidence must round-trip");
        assert_eq!(decoded.format_version, DRILL_EVIDENCE_FORMAT_VERSION);
        assert_eq!(decoded.verdict, evidence.verdict);
        assert_eq!(decoded.bundle, evidence.bundle);
        assert_eq!(decoded.envelope_probes, evidence.envelope_probes);
        assert_eq!(decoded.recovery.restore, evidence.recovery.restore);
    }

    #[tokio::test]
    async fn an_interrupted_cutover_leaves_a_marker_bound_to_its_bundle_and_rolls_forward() {
        let work = TempDir::new().expect("work dir");
        let (target_config, restore_request, manifest, sealed) = interrupted_cutover_fixture(work.path()).await;
        let target = restore_request.target_key_dir.clone();
        crash_at_commit_point(&restore_request).await;

        // The marker is read as the on-disk artifact an operator would find,
        // not through the type that wrote it: it must name this exact bundle
        // and exactly the files the cutover still owes.
        let marker_bytes = fs::read(target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE))
            .await
            .expect("commit marker");
        let marker: serde_json::Value = serde_json::from_slice(&marker_bytes).expect("marker json");
        assert_eq!(marker["backup_id"], serde_json::json!(manifest.backup_id));
        assert_eq!(marker["manifest_digest"]["hex"], serde_json::json!(manifest.manifest_digest.hex));
        let files: Vec<String> = serde_json::from_value(marker["files"].clone()).expect("marker file list");
        assert_eq!(files, vec![LOCAL_KMS_MASTER_KEY_SALT_FILE.to_string(), format!("{}.key", sealed.key_id)]);

        // A directory mid-cutover must not serve requests.
        let error = LocalKmsBackend::new(target_config.clone())
            .await
            .err()
            .expect("startup must fail closed mid-cutover");
        assert!(error.to_string().contains("unfinished restore"), "got {error}");

        let report = restore_local_backup(&test_kek(), &restore_request)
            .await
            .expect("roll forward");
        assert!(report.resumed, "re-running the same bundle must roll the cutover forward");
        assert!(!target.join(LOCAL_RESTORE_COMMIT_MARKER_FILE).exists());
        assert!(probe_after_restore(&target_config, &sealed).await.verified);
    }

    #[tokio::test]
    async fn an_interrupted_cutover_can_be_aborted_back_to_the_pre_restore_state() {
        let work = TempDir::new().expect("work dir");
        let (target_config, restore_request, _manifest, sealed) = interrupted_cutover_fixture(work.path()).await;
        let target = restore_request.target_key_dir.clone();
        crash_at_commit_point(&restore_request).await;

        abort_local_restore(&target).await.expect("abort must succeed");
        // Only the commit temp of the crashed marker write may survive: it was
        // never published, carries no state, and is exactly what the backend's
        // startup recovery and a fresh restore both classify as removable.
        let leftovers = top_level_names(&target).await.expect("target names");
        assert!(
            leftovers.iter().all(|name| is_orphan_commit_temp_name(name)),
            "abort must take back the marker, the staged state, and anything already published; left {leftovers:?}"
        );

        // The rolled-back target is a clean slate again, so the drill is
        // repeatable rather than a one-shot.
        let report = restore_local_backup(&test_kek(), &restore_request)
            .await
            .expect("a fresh restore after an abort must succeed");
        assert!(!report.resumed);
        assert!(probe_after_restore(&target_config, &sealed).await.verified);
    }

    #[tokio::test]
    async fn a_drill_refuses_a_workspace_that_already_holds_a_rehearsal() {
        let work = TempDir::new().expect("work dir");
        let request = drill_request(work.path(), DrillDisaster::KeyDirectoryLost);
        run_local_drill(&test_kek(), &request).await.expect("first drill");

        let error = run_local_drill(&test_kek(), &request)
            .await
            .expect_err("a used workspace must be refused");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "got {error:?}");
    }

    #[tokio::test]
    async fn drill_requests_are_validated_before_anything_is_created() {
        let work = TempDir::new().expect("work dir");

        let mut request = drill_request(work.path(), DrillDisaster::KeyDirectoryLost);
        request.master_key = String::new();
        assert!(run_local_drill(&test_kek(), &request).await.is_err());

        let mut request = drill_request(work.path(), DrillDisaster::KeyDirectoryLost);
        request.drill_id = "../escape".to_string();
        assert!(run_local_drill(&test_kek(), &request).await.is_err());

        let mut request = drill_request(work.path(), DrillDisaster::KeyDirectoryLost);
        request.dataset.keys = MAX_DRILL_OBJECTS;
        request.dataset.objects_per_key = 2;
        assert!(run_local_drill(&test_kek(), &request).await.is_err());

        assert!(
            !work.path().join(LIVE_DIR).exists(),
            "a rejected request must not have created a deployment"
        );
    }

    #[test]
    fn drill_payloads_are_deterministic_and_sized() {
        for bytes in [1usize, 31, 32, 33, 4096] {
            let payload = drill_payload("bucket/object", bytes);
            assert_eq!(payload.len(), bytes);
            assert_eq!(payload, drill_payload("bucket/object", bytes));
        }
        assert_ne!(drill_payload("a", 64), drill_payload("b", 64));
    }

    /// Seal a minimal Vault bundle: one KV metadata record plus the external
    /// references that name the Transit trust root an operator has to restore
    /// natively.
    async fn vault_drill_bundle(dir: &Path, transit_key: &str, cluster_id: &str, kv_mount: &str, kv_prefix: &str) -> PathBuf {
        const BACKUP_ID: &str = "drill-vault-0001";
        const SNAPSHOT_GENERATION: u64 = 11;
        let key_id = "drill-vault-key";
        let bundle = dir.join("vault-bundle");
        fs::create_dir_all(bundle.join(VAULT_RECORD_ARTIFACT_DIR))
            .await
            .expect("artifact dir");

        let kek = test_kek();
        let path = format!("{VAULT_RECORD_ARTIFACT_DIR}/{key_id}{VAULT_RECORD_ARTIFACT_SUFFIX}");
        let plaintext = serde_json::to_vec(&serde_json::json!({ "key_state": "Enabled", "current_version": 1 })).expect("record");
        let nonce = [0x21u8; AEAD_NONCE_LEN];
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
        fs::write(bundle.join(&path), &payload).await.expect("write artifact");

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
            artifacts: vec![ArtifactDescriptor {
                kind: ArtifactKind::KeyMetadata,
                path,
                len: payload.len() as u64,
                aead_algorithm: AeadAlgorithm::Aes256Gcm,
                encrypted_digest: ContentDigest::sha256_of(&payload),
            }],
            local_kdf: None,
            external_references: Some(VaultExternalReferences {
                cluster_id: cluster_id.to_string(),
                namespace: None,
                kv_mount: kv_mount.to_string(),
                kv_path_prefix: kv_prefix.to_string(),
                transit: Some(VaultTransitReference {
                    mount_path: "transit".to_string(),
                    key_name: transit_key.to_string(),
                    required_min_version: 1,
                    current_version: 1,
                    native_snapshot_reference: None,
                }),
                kv_records: vec![VaultKvRecordReference {
                    key_id: key_id.to_string(),
                    kv_version: 1,
                }],
            }),
            key_versions: None,
            capability_discovery: None,
            completeness: CompletenessState::InProgress,
            manifest_digest: ContentDigest {
                algorithm: DigestAlgorithm::Sha256,
                hex: String::new(),
            },
        };
        let sealed = manifest.seal().expect("seal manifest");
        fs::write(bundle.join(LOCAL_BUNDLE_MANIFEST_FILE), sealed.encode().expect("encode manifest"))
            .await
            .expect("write manifest");
        bundle
    }

    /// Vault leg of the drill matrix, against a real server.
    ///
    /// Transit keys are non-exportable, so a Vault bundle never carries the
    /// cryptographic root: restoring it is a Vault-native operator step, and
    /// what RustFS owes is a refusal to proceed until that step has actually
    /// happened. This drills exactly that refusal — the bundle names a Transit
    /// key the recovered Vault does not have, and the preflight must report it
    /// instead of publishing anything.
    ///
    /// Prerequisites: a Vault with the transit engine enabled
    /// (`vault server -dev`, then `vault secrets enable transit`). Address and
    /// token come from `RUSTFS_KMS_VAULT_ADDR` (default
    /// `http://127.0.0.1:8200`) and `RUSTFS_KMS_VAULT_TOKEN` (default `root`).
    #[tokio::test]
    #[ignore = "needs a Vault with the transit engine enabled; see RUSTFS_KMS_VAULT_ADDR"]
    async fn a_vault_bundle_is_refused_until_its_transit_trust_root_is_restored() {
        let address = std::env::var("RUSTFS_KMS_VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
        let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "root".to_string());
        let cluster_id = "vault-drill-cluster";
        let kv_mount = "secret";
        let kv_prefix = "rustfs/kms/drill";
        // A name no operator would have restored, so the trust root is
        // provably absent whatever else the server holds.
        let transit_key = format!("rustfs-kms-drill-absent-{}", uuid::Uuid::new_v4());

        let work = TempDir::new().expect("work dir");
        let bundle = vault_drill_bundle(work.path(), &transit_key, cluster_id, kv_mount, kv_prefix).await;

        let target = VaultRestoreTarget {
            address,
            auth_method: VaultAuthMethod::Token { token },
            namespace: None,
            cluster_id: cluster_id.to_string(),
            kv_mount: kv_mount.to_string(),
            kv_path_prefix: kv_prefix.to_string(),
            transit_mount: Some("transit".to_string()),
        };
        let client = VaultRestoreClient::connect(&target, &KmsConfig::default())
            .await
            .expect("vault restore client");
        let report = dry_run_vault_restore(
            &test_kek(),
            &client,
            &VaultRestoreRequest {
                bundle_dir: bundle,
                target,
                target_deployment_identity: DEPLOYMENT.to_string(),
                observed_generation: None,
                conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
            },
        )
        .await
        .expect("preflight must produce a report, not an error");

        assert!(!report.restore_permitted(), "got {report:?}");
        assert!(
            report
                .external_mismatches
                .iter()
                .any(|mismatch| mismatch.observed.contains(&transit_key) || mismatch.expected.contains(&transit_key)),
            "the missing Transit trust root must be named in the report: {report:?}"
        );
    }
}
