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

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    BUCKET_META_PREFIX, DiskError, DiskStore, HealDiskExt as _, RUSTFS_META_BUCKET,
    storage_api::owner::{EcstoreConditionalFileUpdate, EcstoreDiskBytes},
};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_RESUME: &str = "resume";
const EVENT_HEAL_RESUME_STATE: &str = "heal_resume_state";
const EVENT_HEAL_CHECKPOINT_STATE: &str = "heal_checkpoint_state";

/// resume state file constants
const RESUME_STATE_FILE: &str = "ahm_resume_state.json";
// Replacement intents must not use the ordinary resume-state suffix. Older
// binaries enumerate that suffix and can otherwise resume a replacement as a
// normal heal without its identity and format fences.
const REPLACEMENT_INTENT_FILE: &str = "ahm_replacement_intent.json";
const RESUME_PROGRESS_FILE: &str = "ahm_progress.json";
pub(super) const RESUME_CHECKPOINT_FILE: &str = "ahm_checkpoint.json";
const REPLACEMENT_COMPLETION_PROOF_FILE: &str = "ahm_replacement_completion_proof.json";
const REPLACEMENT_RECOVERY_DIR: &str = "ahm-replacement";
const REPLACEMENT_INTENT_SEAL_FILE: &str = "ahm_replacement_intent_seal";
const LEGACY_REPLACEMENT_RECOVERY_MARKER_FILE: &str = "ahm_replacement_recovery.json";
const CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA: u32 = 1;

/// Current on-disk schema version for `ResumeState`. Snapshots written by an
/// older schema (which tracked latest-only object names and a positional
/// cursor) are incompatible with the per-version resume cursor, so they are
/// discarded on load and the scan restarts from the beginning.
const CURRENT_RESUME_SCHEMA: u32 = 5;
/// Current on-disk schema version for `ResumeCheckpoint`. Same rationale as
/// `CURRENT_RESUME_SCHEMA`: pre-per-version dedup identities are not comparable
/// to the new `compose_key` identities, so a stale checkpoint is discarded.
const CURRENT_CHECKPOINT_SCHEMA: u32 = 5;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementPhase {
    #[default]
    None,
    Intent,
    Rebuilding,
    Verified,
    CleanupPending,
    Abandoned,
}

/// Target-specific state for a durable automatic replacement generation.
///
/// This is deliberately separate from the legacy background-heal status
/// contract. Consumers must treat [`Self::Unknown`] as non-definitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementRecoveryState {
    WaitingForReplacement,
    Running,
    Incomplete,
    Unrecoverable,
    CleanupPending,
    Completed,
    Unknown,
}

/// Read-only status derived from one durable replacement generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementRecoveryRecord {
    pub task_id: String,
    pub state: ReplacementRecoveryState,
    pub generation: Option<String>,
    pub set_disk_id: Option<String>,
    pub target_slots: Vec<String>,
    pub reason: Option<String>,
    pub verified_at: Option<u64>,
}

impl ReplacementRecoveryRecord {
    fn from_state(state: ResumeState) -> Option<Self> {
        if !is_replacement_intent(&state) {
            return None;
        }

        let invariant_holds = state.replacement_generation.as_deref() == Some(state.task_id.as_str())
            && replacement_targets_match_identities(&state.replacement_targets, &state.replacement_target_identities);
        if !invariant_holds {
            return Some(Self::unknown(
                state.task_id,
                "durable replacement state violates its generation or target identity binding",
            ));
        }

        let (state_kind, reason) = if !state.completed && state.retry_count >= state.max_retries {
            (
                ReplacementRecoveryState::Unrecoverable,
                Some("replacement retry budget exhausted".to_string()),
            )
        } else if let Some(reason) = state.error_message.clone() {
            (ReplacementRecoveryState::Incomplete, Some(reason))
        } else {
            match state.replacement_phase {
                ReplacementPhase::Intent => (ReplacementRecoveryState::WaitingForReplacement, None),
                ReplacementPhase::Rebuilding => (ReplacementRecoveryState::Running, None),
                ReplacementPhase::Verified | ReplacementPhase::CleanupPending => (ReplacementRecoveryState::CleanupPending, None),
                ReplacementPhase::Abandoned => (
                    ReplacementRecoveryState::Unrecoverable,
                    Some("replacement generation was abandoned".to_string()),
                ),
                ReplacementPhase::None => (ReplacementRecoveryState::Unknown, Some("replacement phase is missing".to_string())),
            }
        };

        Some(Self {
            task_id: state.task_id,
            state: state_kind,
            generation: state.replacement_generation,
            set_disk_id: Some(state.set_disk_id),
            target_slots: state.replacement_targets,
            reason,
            verified_at: None,
        })
    }

    fn from_completion_proof(proof: &ReplacementCompletionProof) -> Self {
        Self {
            task_id: proof.task_id.clone(),
            state: ReplacementRecoveryState::Completed,
            generation: Some(proof.replacement_generation.clone()),
            set_disk_id: Some(proof.set_disk_id.clone()),
            target_slots: proof.replacement_targets.clone(),
            reason: None,
            verified_at: Some(proof.verified_at),
        }
    }

    fn unknown(task_id: String, reason: &str) -> Self {
        Self {
            task_id,
            state: ReplacementRecoveryState::Unknown,
            generation: None,
            set_disk_id: None,
            target_slots: Vec::new(),
            reason: Some(reason.to_string()),
            verified_at: None,
        }
    }
}

fn replacement_targets_match_identities(targets: &[String], identities: &[ReplacementTargetIdentity]) -> bool {
    !targets.is_empty()
        && targets.len() == identities.len()
        && targets.iter().collect::<HashSet<_>>().len() == targets.len()
        && identities.iter().map(|identity| &identity.endpoint).eq(targets.iter())
}

/// Stable evidence for the mounted replacement instance that owns a repair
/// generation. Endpoint text alone is not sufficient because a later disk can
/// be mounted at the same configured path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacementTargetIdentity {
    pub endpoint: String,
    pub canonical_path: String,
    pub physical_device_ids: Vec<String>,
    pub filesystem_identity: String,
}

/// Durable terminal evidence for one automatic replacement generation. This
/// lives on the healthy non-target anchor rather than in the resumable state,
/// because resume cleanup must not erase proof that the generation completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplacementCompletionProof {
    pub schema_version: u32,
    pub task_id: String,
    pub replacement_generation: String,
    pub set_disk_id: String,
    pub replacement_targets: Vec<String>,
    pub replacement_target_identities: Vec<ReplacementTargetIdentity>,
    pub verified_at: u64,
}

impl ReplacementCompletionProof {
    fn from_state(state: &ResumeState, verified_at: u64) -> Result<Self> {
        let replacement_generation = state
            .replacement_generation
            .clone()
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Replacement completion has no generation for task {}", state.task_id),
            })?;
        if replacement_generation != state.task_id
            || state.replacement_targets.is_empty()
            || state
                .replacement_target_identities
                .iter()
                .map(|identity| &identity.endpoint)
                .collect::<Vec<_>>()
                != state.replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion identity does not match task {}", state.task_id),
            });
        }

        Ok(Self {
            schema_version: CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA,
            task_id: state.task_id.clone(),
            replacement_generation,
            set_disk_id: state.set_disk_id.clone(),
            replacement_targets: state.replacement_targets.clone(),
            replacement_target_identities: state.replacement_target_identities.clone(),
            verified_at,
        })
    }

    fn matches_state(&self, state: &ResumeState) -> bool {
        self.schema_version == CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA
            && self.task_id == state.task_id
            && state.replacement_generation.as_deref() == Some(self.replacement_generation.as_str())
            && self.set_disk_id == state.set_disk_id
            && self.replacement_targets == state.replacement_targets
            && self.replacement_target_identities == state.replacement_target_identities
    }

    fn validate(&self, expected_task_id: &str) -> Result<()> {
        if self.schema_version != CURRENT_REPLACEMENT_COMPLETION_PROOF_SCHEMA {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof schema {} is unsupported", self.schema_version),
            });
        }
        validate_resume_task_id(expected_task_id)?;
        if self.task_id != expected_task_id
            || self.replacement_generation != self.task_id
            || self.set_disk_id.is_empty()
            || self.verified_at == 0
            || !replacement_targets_match_identities(&self.replacement_targets, &self.replacement_target_identities)
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof does not match task {expected_task_id}"),
            });
        }
        Ok(())
    }
}

pub(crate) fn replacement_target_identities_match(
    expected: &[ReplacementTargetIdentity],
    actual: &[ReplacementTargetIdentity],
) -> bool {
    let mut expected = expected.to_vec();
    let mut actual = actual.to_vec();
    expected.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    actual.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
    expected == actual
}

/// Build the canonical, provably-injective dedup identity for an object
/// version. Length-prefixing the object key makes the encoding injective: no
/// two distinct `(object, version_id)` pairs can collide, even for adversarial
/// keys containing `:` or embedded null bytes. This is the single source of
/// truth for per-version dedup across the heal loop and the checkpoint sets.
pub fn compose_key(object: &str, version_id: Option<&str>) -> String {
    format!("{}:{}{}", object.len(), object, version_id.unwrap_or(""))
}

/// Persistence throttle for per-object bookkeeping: flush after this many
/// buffered mutations or once the interval elapses, whichever comes first.
/// Object heal is idempotent, so a crash re-heals at most one throttle window.
const PERSIST_EVERY_MUTATIONS: usize = 1000;
const PERSIST_INTERVAL: Duration = Duration::from_secs(5);

/// Tracks buffered mutations between persisted snapshots.
#[derive(Debug)]
struct PersistThrottle {
    pending: usize,
    last_save: Instant,
}

impl PersistThrottle {
    fn new() -> Self {
        Self {
            pending: 0,
            last_save: Instant::now(),
        }
    }

    /// Record one mutation; returns true when the batch should be flushed.
    fn record(&mut self) -> bool {
        self.pending += 1;
        self.pending >= PERSIST_EVERY_MUTATIONS || self.last_save.elapsed() >= PERSIST_INTERVAL
    }

    fn mark_saved(&mut self) {
        self.pending = 0;
        self.last_save = Instant::now();
    }
}

/// Helper function to convert Path to &str, returning an error if conversion fails
fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::other(format!("Invalid UTF-8 path: {path:?}")))
}

/// Resume task IDs become part of metadata file names. Persisted filenames are
/// untrusted, so only accept a UUID encoded as one normal path component.
fn validate_resume_task_id(task_id: &str) -> Result<()> {
    let mut components = Path::new(task_id).components();
    let is_single_normal_component = matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();

    let Ok(uuid) = Uuid::parse_str(task_id) else {
        return Err(Error::TaskExecutionFailed {
            message: "Invalid resume task id".to_string(),
        });
    };

    if is_single_normal_component && uuid.hyphenated().to_string() == task_id {
        return Ok(());
    }

    Err(Error::TaskExecutionFailed {
        message: "Invalid resume task id".to_string(),
    })
}

#[cfg(test)]
pub(super) struct ResumeDeleteFailure {
    path: String,
}

#[cfg(test)]
fn resume_delete_failures() -> &'static Mutex<HashMap<String, DiskError>> {
    static FAILURES: std::sync::OnceLock<Mutex<HashMap<String, DiskError>>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl ResumeDeleteFailure {
    pub(super) fn install(path: String, error: DiskError) -> Self {
        let previous = resume_delete_failures()
            .lock()
            .expect("resume delete failure registry should not poison")
            .insert(path.clone(), error);
        assert!(previous.is_none(), "resume delete failure already installed");
        Self { path }
    }
}

#[cfg(test)]
impl Drop for ResumeDeleteFailure {
    fn drop(&mut self) {
        resume_delete_failures()
            .lock()
            .expect("resume delete failure registry should not poison")
            .remove(&self.path);
    }
}

#[cfg(test)]
fn injected_resume_delete_error(path: &str) -> Option<DiskError> {
    resume_delete_failures()
        .lock()
        .expect("resume delete failure registry should not poison")
        .get(path)
        .cloned()
}

#[cfg(not(test))]
fn injected_resume_delete_error(_path: &str) -> Option<DiskError> {
    None
}

#[cfg(test)]
struct ReplacementProofWriteFailure {
    path: String,
}

#[cfg(test)]
fn replacement_proof_write_failures() -> &'static Mutex<HashMap<String, DiskError>> {
    static FAILURES: std::sync::OnceLock<Mutex<HashMap<String, DiskError>>> = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
impl ReplacementProofWriteFailure {
    fn install(path: String, error: DiskError) -> Self {
        let previous = replacement_proof_write_failures()
            .lock()
            .expect("replacement proof write failure registry should not poison")
            .insert(path.clone(), error);
        assert!(previous.is_none(), "replacement proof write failure already installed");
        Self { path }
    }
}

#[cfg(test)]
impl Drop for ReplacementProofWriteFailure {
    fn drop(&mut self) {
        replacement_proof_write_failures()
            .lock()
            .expect("replacement proof write failure registry should not poison")
            .remove(&self.path);
    }
}

#[cfg(test)]
fn injected_replacement_proof_write_error(path: &str) -> Option<DiskError> {
    replacement_proof_write_failures()
        .lock()
        .expect("replacement proof write failure registry should not poison")
        .get(path)
        .cloned()
}

#[cfg(not(test))]
fn injected_replacement_proof_write_error(_path: &str) -> Option<DiskError> {
    None
}

async fn delete_resume_file(disk: &DiskStore, path: &Path) -> Result<()> {
    let path_str = path_to_str(path)?;
    if let Some(err) = injected_resume_delete_error(path_str) {
        return Err(err.into());
    }
    match disk.delete(RUSTFS_META_BUCKET, path_str, Default::default()).await {
        Ok(()) | Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn replacement_completion_proof_path(task_id: &str) -> std::path::PathBuf {
    replacement_recovery_dir().join(format!("{task_id}_{REPLACEMENT_COMPLETION_PROOF_FILE}"))
}

fn legacy_replacement_completion_proof_path(task_id: &str) -> std::path::PathBuf {
    Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{REPLACEMENT_COMPLETION_PROOF_FILE}"))
}

fn replacement_recovery_dir() -> std::path::PathBuf {
    Path::new(BUCKET_META_PREFIX).join(REPLACEMENT_RECOVERY_DIR)
}

async fn ensure_replacement_recovery_dir(disk: &DiskStore) -> std::result::Result<(), DiskError> {
    let volume = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/{REPLACEMENT_RECOVERY_DIR}");
    match super::storage_api::owner::EcstoreDiskAPI::make_volume(disk.as_ref(), &volume).await {
        Ok(()) | Err(DiskError::VolumeExists) => Ok(()),
        Err(error) => Err(error),
    }
}

fn replacement_intent_seal_path(task_id: &str) -> std::path::PathBuf {
    replacement_recovery_dir().join(format!("{task_id}_{REPLACEMENT_INTENT_SEAL_FILE}"))
}

fn legacy_replacement_recovery_marker_path(task_id: &str) -> std::path::PathBuf {
    Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{LEGACY_REPLACEMENT_RECOVERY_MARKER_FILE}"))
}

/// resume state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeState {
    /// on-disk schema version; absent in legacy snapshots (defaults to 0)
    #[serde(default)]
    pub schema_version: u32,
    /// authoritative opaque `(marker, version_marker)` continuation token for
    /// the version listing. `None` means "start from the beginning".
    #[serde(default)]
    pub resume_cursor: Option<String>,
    /// task id
    pub task_id: String,
    /// task type
    pub task_type: String,
    /// set disk identifier (for erasure set tasks)
    #[serde(default)]
    pub set_disk_id: String,
    #[serde(default)]
    pub replacement_targets: Vec<String>,
    #[serde(default)]
    pub replacement_target_identities: Vec<ReplacementTargetIdentity>,
    /// Immutable bucket plan for a replacement generation. `pending_buckets`
    /// shrinks during a pass and must never be reused as a positional resume
    /// input after restart.
    #[serde(default)]
    pub replacement_buckets: Vec<String>,
    /// A task-scoped generation assigned before formatting an automatic
    /// replacement. A new unformatted replacement receives a new generation
    /// and therefore cannot reuse an older disk's cursor or checkpoint.
    #[serde(default)]
    pub replacement_generation: Option<String>,
    #[serde(default)]
    pub replacement_phase: ReplacementPhase,
    /// start time
    pub start_time: u64,
    /// last update time
    pub last_update: u64,
    /// completed
    pub completed: bool,
    /// total objects
    pub total_objects: u64,
    /// processed objects
    pub processed_objects: u64,
    /// successful objects
    pub successful_objects: u64,
    /// failed objects
    pub failed_objects: u64,
    /// skipped objects
    pub skipped_objects: u64,
    /// current bucket
    pub current_bucket: Option<String>,
    /// current object
    pub current_object: Option<String>,
    /// completed buckets
    pub completed_buckets: Vec<String>,
    /// pending buckets
    pub pending_buckets: Vec<String>,
    /// error message
    pub error_message: Option<String>,
    /// retry count
    pub retry_count: u32,
    /// max retries
    pub max_retries: u32,
}

impl ResumeState {
    pub fn new(task_id: String, task_type: String, set_disk_id: String, buckets: Vec<String>) -> Self {
        Self {
            schema_version: CURRENT_RESUME_SCHEMA,
            resume_cursor: None,
            task_id,
            task_type,
            set_disk_id,
            replacement_targets: Vec::new(),
            replacement_target_identities: Vec::new(),
            replacement_buckets: Vec::new(),
            replacement_generation: None,
            replacement_phase: ReplacementPhase::None,
            start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            completed: false,
            total_objects: 0,
            processed_objects: 0,
            successful_objects: 0,
            failed_objects: 0,
            skipped_objects: 0,
            current_bucket: None,
            current_object: None,
            completed_buckets: Vec::new(),
            pending_buckets: buckets,
            error_message: None,
            retry_count: 0,
            max_retries: 3,
        }
    }

    fn replacement_intent(
        task_id: String,
        task_type: String,
        set_disk_id: String,
        buckets: Vec<String>,
        replacement_targets: Vec<String>,
        replacement_target_identities: Vec<ReplacementTargetIdentity>,
    ) -> Self {
        let mut state = Self::new(task_id.clone(), task_type, set_disk_id, buckets);
        state.replacement_targets = replacement_targets;
        state.replacement_target_identities = replacement_target_identities;
        state.replacement_buckets = state.pending_buckets.clone();
        state.replacement_generation = Some(task_id);
        state.replacement_phase = ReplacementPhase::Intent;
        state
    }

    pub fn update_progress(&mut self, processed: u64, successful: u64, failed: u64, skipped: u64) {
        self.processed_objects = processed;
        self.successful_objects = successful;
        self.failed_objects = failed;
        self.skipped_objects = skipped;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_current_item(&mut self, bucket: Option<String>, object: Option<String>) {
        self.current_bucket = bucket;
        self.current_object = object;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    /// Read the authoritative version-listing continuation cursor.
    pub fn resume_cursor(&self) -> Option<String> {
        self.resume_cursor.clone()
    }

    /// Persist the authoritative version-listing continuation cursor. `None`
    /// resets the scan to the beginning of the current bucket.
    pub fn set_resume_cursor(&mut self, cursor: Option<String>) {
        self.resume_cursor = cursor;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn complete_bucket(&mut self, bucket: &str) {
        if !self.completed_buckets.contains(&bucket.to_string()) {
            self.completed_buckets.push(bucket.to_string());
        }
        if let Some(pos) = self.pending_buckets.iter().position(|b| b == bucket) {
            self.pending_buckets.remove(pos);
        }
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn mark_completed(&mut self) {
        self.completed = true;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    /// Reset per-pass progress so a retry re-scans the whole set from the
    /// start. `retry_count`/`max_retries` are intentionally preserved so
    /// retries stay bounded.
    pub fn reset_for_retry(&mut self) {
        self.completed_buckets.clear();
        self.processed_objects = 0;
        self.successful_objects = 0;
        self.failed_objects = 0;
        self.skipped_objects = 0;
        self.completed = false;
        // A retry re-scans every bucket from the beginning, so the version
        // cursor must be cleared too — otherwise the retry would resume mid-scan.
        self.resume_cursor = None;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_error(&mut self, error: String) {
        self.error_message = Some(error);
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    pub fn get_progress_percentage(&self) -> f64 {
        if self.total_objects == 0 {
            return 0.0;
        }
        (self.processed_objects as f64 / self.total_objects as f64) * 100.0
    }

    pub fn get_success_rate(&self) -> f64 {
        let total = self.successful_objects + self.failed_objects;
        if total == 0 {
            return 0.0;
        }
        (self.successful_objects as f64 / total as f64) * 100.0
    }
}

/// resume manager
pub struct ResumeManager {
    disk: DiskStore,
    state: Arc<RwLock<ResumeState>>,
    throttle: Mutex<PersistThrottle>,
    state_file: ResumeStateFile,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeStateFile {
    Ordinary,
    ReplacementIntent,
    LegacyReplacementIntent,
}

impl ResumeStateFile {
    fn path(self, task_id: &str) -> std::path::PathBuf {
        match self {
            Self::Ordinary => Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_STATE_FILE}")),
            Self::ReplacementIntent => replacement_recovery_dir().join(format!("{task_id}_{REPLACEMENT_INTENT_FILE}")),
            Self::LegacyReplacementIntent => Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{REPLACEMENT_INTENT_FILE}")),
        }
    }
}

fn is_replacement_intent(state: &ResumeState) -> bool {
    state.replacement_generation.as_deref() == Some(state.task_id.as_str())
        && !state.replacement_targets.is_empty()
        && replacement_targets_match_identities(&state.replacement_targets, &state.replacement_target_identities)
        && matches!(
            state.replacement_phase,
            ReplacementPhase::Intent
                | ReplacementPhase::Rebuilding
                | ReplacementPhase::Verified
                | ReplacementPhase::CleanupPending
                | ReplacementPhase::Abandoned
        )
}

impl ResumeManager {
    /// create new resume manager
    pub async fn new(
        disk: DiskStore,
        task_id: String,
        task_type: String,
        set_disk_id: String,
        buckets: Vec<String>,
    ) -> Result<Self> {
        validate_resume_task_id(&task_id)?;
        let state = ResumeState::new(task_id, task_type, set_disk_id, buckets);
        let manager = Self {
            disk,
            state: Arc::new(RwLock::new(state)),
            throttle: Mutex::new(PersistThrottle::new()),
            state_file: ResumeStateFile::Ordinary,
        };

        // save initial state
        manager.save_state().await?;
        Ok(manager)
    }

    /// Persist the automatic replacement intent before the target is formatted.
    /// Reusing the same task id is idempotent for scheduler retries, while a new
    /// admission obtains a new task id and cannot inherit stale progress.
    pub async fn new_replacement_intent(
        disk: DiskStore,
        task_id: String,
        set_disk_id: String,
        buckets: Vec<String>,
        mut replacement_targets: Vec<String>,
        mut replacement_target_identities: Vec<ReplacementTargetIdentity>,
    ) -> Result<Self> {
        validate_resume_task_id(&task_id)?;
        replacement_targets.sort_unstable();
        replacement_targets.dedup();
        replacement_target_identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
        replacement_target_identities.dedup_by(|left, right| left.endpoint == right.endpoint);
        if replacement_target_identities
            .iter()
            .map(|identity| &identity.endpoint)
            .collect::<Vec<_>>()
            != replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement identities do not match targets for task {task_id}"),
            });
        }

        let recovery_expected = if Self::has_replacement_intent(&disk, &task_id).await {
            match Self::load_replacement_intent(disk.clone(), &task_id).await {
                Ok(manager) => {
                    let state = manager.get_state().await;
                    if state.set_disk_id != set_disk_id
                        || state.replacement_targets != replacement_targets
                        || state.replacement_target_identities != replacement_target_identities
                        || state.replacement_generation.as_deref() != Some(task_id.as_str())
                        || !matches!(
                            state.replacement_phase,
                            ReplacementPhase::Intent
                                | ReplacementPhase::Rebuilding
                                | ReplacementPhase::Verified
                                | ReplacementPhase::CleanupPending
                        )
                    {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement intent does not match task {task_id}"),
                        });
                    }
                    manager.ensure_replacement_intent_seal().await?;
                    return Ok(manager);
                }
                Err(error) => match Self::torn_replacement_intent_bytes(&disk, &task_id).await? {
                    Some(expected) => {
                        warn!(
                            target: "rustfs::heal::resume",
                            event = EVENT_HEAL_RESUME_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_RESUME,
                            task_id,
                            error = %error,
                            state = "torn_replacement_intent_recovered",
                            "Replacing a torn replacement intent before formatting"
                        );
                        Some(expected)
                    }
                    None => return Err(error),
                },
            }
        } else {
            None
        };

        let state = ResumeState::replacement_intent(
            task_id,
            "erasure_set".to_string(),
            set_disk_id,
            buckets,
            replacement_targets,
            replacement_target_identities,
        );
        let manager = Self {
            disk,
            state: Arc::new(RwLock::new(state)),
            throttle: Mutex::new(PersistThrottle::new()),
            state_file: ResumeStateFile::ReplacementIntent,
        };
        manager.publish_new_replacement_intent(recovery_expected).await?;
        manager.ensure_replacement_intent_seal().await?;
        Ok(manager)
    }

    /// Seal a durably published intent before the caller may format a target.
    /// A torn intent without this seal is known to have failed before its
    /// creator returned and can be atomically recreated on retry.
    async fn ensure_replacement_intent_seal(&self) -> Result<()> {
        let task_id = self.state.read().await.task_id.clone();
        validate_resume_task_id(&task_id)?;
        let path = replacement_intent_seal_path(&task_id);
        let path = path_to_str(&path)?;
        match self.disk.read_all(RUSTFS_META_BUCKET, path).await {
            Ok(_) => return Ok(()),
            Err(DiskError::FileNotFound) => {}
            Err(error) => {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to read replacement intent seal: {error}"),
                });
            }
        }
        self.disk
            .write_all(RUSTFS_META_BUCKET, path, b"sealed".as_slice().into())
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to save replacement intent seal: {error}"),
            })
    }

    pub async fn mark_replacement_rebuilding(
        &self,
        mut replacement_target_identities: Vec<ReplacementTargetIdentity>,
    ) -> Result<()> {
        replacement_target_identities.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
        replacement_target_identities.dedup_by(|left, right| left.endpoint == right.endpoint);
        let mut state = self.state.write().await;
        if !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement intent is not active for task {}", state.task_id),
            });
        }
        if replacement_target_identities
            .iter()
            .map(|identity| &identity.endpoint)
            .collect::<Vec<_>>()
            != state.replacement_targets.iter().collect::<Vec<_>>()
        {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement identities do not match targets for task {}", state.task_id),
            });
        }
        if !replacement_target_identities_match(&state.replacement_target_identities, &replacement_target_identities) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement target changed after format for task {}", state.task_id),
            });
        }
        state.replacement_phase = ReplacementPhase::Rebuilding;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    /// Persist survivor-anchor completion proof before transitioning this
    /// resumable state to `Verified`. If proof persistence fails, this state
    /// stays rebuildable and the caller must retain the healing marker.
    pub async fn mark_replacement_completed_and_verified(&self) -> Result<()> {
        let state = self.state.read().await.clone();
        if !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement verification is not active for task {}", state.task_id),
            });
        }
        let proof = self.write_replacement_completion_proof(&state, None).await?;

        let mut state = self.state.write().await;
        if !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement verification changed for task {}", state.task_id),
            });
        }
        state.mark_completed();
        state.replacement_phase = ReplacementPhase::Verified;
        state.last_update = proof.verified_at;
        drop(state);
        self.save_state_strict().await
    }

    /// Verify or backfill the terminal proof before marker removal or resume
    /// cleanup. This supports restart recovery from a `Verified` state written
    /// by a prior binary that did not yet have a separate proof record.
    pub(crate) async fn ensure_replacement_completion_proof(&self) -> Result<ReplacementCompletionProof> {
        let state = self.state.read().await.clone();
        if !state.completed || !matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion is not verified for task {}", state.task_id),
            });
        }
        self.write_replacement_completion_proof(&state, Some(state.last_update)).await
    }

    /// Record that the healing markers have been removed, so a later retry can
    /// safely delete the remaining resume artifacts without touching markers.
    pub async fn mark_replacement_cleanup_pending(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.completed || !matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement cleanup is not ready for task {}", state.task_id),
            });
        }
        state.replacement_phase = ReplacementPhase::CleanupPending;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    /// Load the durable terminal proof from the healthy survivor anchor.
    pub(crate) async fn load_replacement_completion_proof(disk: DiskStore, task_id: &str) -> Result<ReplacementCompletionProof> {
        Self::replacement_completion_proof_if_present(disk, task_id)
            .await?
            .ok_or_else(|| Error::TaskExecutionFailed {
                message: format!("Failed to read replacement completion proof: proof is missing for task {task_id}"),
            })
    }

    async fn replacement_completion_proof_if_present(
        disk: DiskStore,
        task_id: &str,
    ) -> Result<Option<ReplacementCompletionProof>> {
        validate_resume_task_id(task_id)?;
        for path in [
            replacement_completion_proof_path(task_id),
            legacy_replacement_completion_proof_path(task_id),
        ] {
            let path_str = path_to_str(&path)?;
            let bytes = match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(bytes) => bytes,
                Err(DiskError::FileNotFound) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            };
            let proof: ReplacementCompletionProof =
                serde_json::from_slice(&bytes).map_err(|error| Error::TaskExecutionFailed {
                    message: format!("Failed to deserialize replacement completion proof: {error}"),
                })?;
            proof.validate(task_id)?;
            return Ok(Some(proof));
        }
        Ok(None)
    }

    /// Reconcile the proof-first publication order after a crash. A matching
    /// proof is durable evidence that rebuilding finished, so it must win over
    /// an older active state before a retry may format the target again.
    async fn reconcile_replacement_completion_proof(&self) -> Result<()> {
        let task_id = self.state.read().await.task_id.clone();
        let Some(proof) = Self::replacement_completion_proof_if_present(self.disk.clone(), &task_id).await? else {
            return Ok(());
        };

        let mut state = self.state.write().await;
        if !proof.matches_state(&state) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof does not match active intent for task {}", state.task_id),
            });
        }
        if state.completed && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending) {
            return Ok(());
        }
        if state.completed || !matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Replacement completion proof conflicts with state for task {}", state.task_id),
            });
        }

        state.mark_completed();
        state.replacement_phase = ReplacementPhase::Verified;
        state.last_update = proof.verified_at;
        drop(state);
        self.save_state_strict().await
    }

    async fn migrate_legacy_replacement_completion_proof(disk: &DiskStore, task_id: &str) -> Result<bool> {
        validate_resume_task_id(task_id)?;
        let legacy_path = legacy_replacement_completion_proof_path(task_id);
        let legacy_path_str = path_to_str(&legacy_path)?;
        let legacy_bytes = match disk.read_all(RUSTFS_META_BUCKET, legacy_path_str).await {
            Ok(bytes) => bytes,
            Err(DiskError::FileNotFound) => return Ok(false),
            Err(error) => {
                return Err(Error::TaskExecutionFailed {
                    message: format!("Failed to read legacy replacement completion proof: {error}"),
                });
            }
        };
        let legacy_proof: ReplacementCompletionProof =
            serde_json::from_slice(&legacy_bytes).map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to deserialize legacy replacement completion proof: {error}"),
            })?;
        legacy_proof.validate(task_id)?;

        ensure_replacement_recovery_dir(disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        let path = replacement_completion_proof_path(task_id);
        let path_str = path_to_str(&path)?;
        for _ in 0..2 {
            match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(bytes) => {
                    let proof: ReplacementCompletionProof =
                        serde_json::from_slice(&bytes).map_err(|error| Error::TaskExecutionFailed {
                            message: format!("Failed to deserialize replacement completion proof: {error}"),
                        })?;
                    proof.validate(task_id)?;
                    if proof != legacy_proof {
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement completion proof conflicts with legacy proof for task {task_id}"),
                        });
                    }
                    delete_resume_file(disk, &legacy_path).await?;
                    return Ok(true);
                }
                Err(DiskError::FileNotFound) => {}
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            }

            match super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_str,
                None,
                Some(legacy_bytes.clone()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => {
                    delete_resume_file(disk, &legacy_path).await?;
                    return Ok(true);
                }
                Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to migrate replacement completion proof: {error}"),
                    });
                }
            }
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement completion proof changed while migrating task {task_id}"),
        })
    }

    pub async fn abandon_replacement_intent(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if matches!(state.replacement_phase, ReplacementPhase::Abandoned) {
            return Ok(());
        }
        state.replacement_phase = ReplacementPhase::Abandoned;
        state.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        drop(state);
        self.save_state_strict().await
    }

    pub async fn set_replacement_targets(&self, replacement_targets: Vec<String>) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.replacement_targets = replacement_targets;
        }
        self.save_state().await
    }

    /// Load an ordinary resume state from disk. Replacement intents have a
    /// separate namespace so they cannot be mistaken for ordinary work by an
    /// older binary.
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        Self::load_from_disk_at(disk, task_id, ResumeStateFile::Ordinary).await
    }

    /// Load a replacement intent, migrating a legacy flat record only after
    /// the dedicated replacement record has been written.
    pub async fn load_replacement_intent(disk: DiskStore, task_id: &str) -> Result<Self> {
        if Self::has_state_file(&disk, task_id, ResumeStateFile::ReplacementIntent).await {
            let isolated = Self::load_from_disk_at(disk.clone(), task_id, ResumeStateFile::ReplacementIntent).await?;
            isolated.ensure_replacement_intent_seal().await?;
            for legacy_file in [ResumeStateFile::LegacyReplacementIntent, ResumeStateFile::Ordinary] {
                if !Self::has_state_file(&disk, task_id, legacy_file).await {
                    continue;
                }
                let legacy = Self::load_from_disk_at(disk.clone(), task_id, legacy_file).await?;
                let legacy_state = legacy.get_state().await;
                if !is_replacement_intent(&legacy_state) || legacy_state != isolated.get_state().await {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Replacement intent has conflicting legacy state for task {task_id}"),
                    });
                }
                legacy.cleanup().await?;
            }
            delete_resume_file(&disk, &legacy_replacement_recovery_marker_path(task_id)).await?;
            isolated.reconcile_replacement_completion_proof().await?;
            return Ok(isolated);
        }

        let legacy_file = if Self::has_state_file(&disk, task_id, ResumeStateFile::LegacyReplacementIntent).await {
            ResumeStateFile::LegacyReplacementIntent
        } else {
            ResumeStateFile::Ordinary
        };
        let legacy = Self::load_from_disk_at(disk.clone(), task_id, legacy_file).await?;
        if !is_replacement_intent(&legacy.get_state().await) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Resume state is not a replacement intent for task {task_id}"),
            });
        }

        let migrated = Self {
            disk,
            state: legacy.state.clone(),
            throttle: Mutex::new(PersistThrottle::new()),
            state_file: ResumeStateFile::ReplacementIntent,
        };
        migrated.save_state_strict().await?;
        migrated.ensure_replacement_intent_seal().await?;
        legacy.cleanup().await?;
        delete_resume_file(&migrated.disk, &legacy_replacement_recovery_marker_path(task_id)).await?;
        migrated.reconcile_replacement_completion_proof().await?;
        Ok(migrated)
    }

    async fn load_from_disk_at(disk: DiskStore, task_id: &str, state_file: ResumeStateFile) -> Result<Self> {
        validate_resume_task_id(task_id)?;
        let state_data = Self::read_state_file(&disk, task_id, state_file).await?;
        let mut state: ResumeState = serde_json::from_slice(&state_data).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to deserialize resume state: {e}"),
        })?;
        if state.task_id != task_id {
            return Err(Error::TaskExecutionFailed {
                message: "Resume state task id does not match filename".to_string(),
            });
        }

        // A snapshot written by an older schema tracked a latest-only positional
        // cursor that is meaningless under per-version resume. Discard the stale
        // progress so the scan restarts cleanly, then stamp the current schema.
        if state.schema_version > CURRENT_RESUME_SCHEMA {
            return Err(Error::TaskExecutionFailed {
                message: format!(
                    "Resume state schema {} is newer than supported schema {CURRENT_RESUME_SCHEMA}",
                    state.schema_version
                ),
            });
        }
        if state.schema_version < CURRENT_RESUME_SCHEMA {
            warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_RESUME_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                task_id,
                found_schema = state.schema_version,
                current_schema = CURRENT_RESUME_SCHEMA,
                state = "schema_discarded",
                "Heal resume state schema is stale; discarding cursor and progress"
            );
            state.resume_cursor = None;
            state.processed_objects = 0;
            state.successful_objects = 0;
            state.failed_objects = 0;
            state.skipped_objects = 0;
            state.completed = false;
            state.completed_buckets.clear();
            state.schema_version = CURRENT_RESUME_SCHEMA;
        }

        Ok(Self {
            disk,
            state: Arc::new(RwLock::new(state)),
            throttle: Mutex::new(PersistThrottle::new()),
            state_file,
        })
    }

    /// check if resume state exists
    pub async fn has_resume_state(disk: &DiskStore, task_id: &str) -> bool {
        Self::has_state_file(disk, task_id, ResumeStateFile::Ordinary).await
    }

    /// Check for a replacement intent in its dedicated namespace. Flat and
    /// ordinary legacy records are recognized only for migration compatibility.
    pub async fn has_replacement_intent(disk: &DiskStore, task_id: &str) -> bool {
        if Self::has_state_file(disk, task_id, ResumeStateFile::ReplacementIntent).await {
            return true;
        }
        if Self::has_state_file(disk, task_id, ResumeStateFile::LegacyReplacementIntent).await {
            return true;
        }
        match Self::load_from_disk_at(disk.clone(), task_id, ResumeStateFile::Ordinary).await {
            Ok(manager) => is_replacement_intent(&manager.get_state().await),
            Err(_) => false,
        }
    }

    async fn has_state_file(disk: &DiskStore, task_id: &str, state_file: ResumeStateFile) -> bool {
        if validate_resume_task_id(task_id).is_err() {
            return false;
        }
        let file_path = state_file.path(task_id);
        match path_to_str(&file_path) {
            Ok(path_str) => match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(data) => !data.is_empty(),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    async fn torn_replacement_intent_bytes(disk: &DiskStore, task_id: &str) -> Result<Option<EcstoreDiskBytes>> {
        validate_resume_task_id(task_id)?;
        let path = ResumeStateFile::ReplacementIntent.path(task_id);
        let path = path_to_str(&path)?;
        match disk.read_all(RUSTFS_META_BUCKET, path).await {
            Ok(bytes) if serde_json::from_slice::<ResumeState>(&bytes).is_err() => {
                let seal = replacement_intent_seal_path(task_id);
                let seal = path_to_str(&seal)?;
                match disk.read_all(RUSTFS_META_BUCKET, seal).await {
                    Err(DiskError::FileNotFound) => Ok(Some(bytes)),
                    Ok(_) => Ok(None),
                    Err(error) => Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement intent seal: {error}"),
                    }),
                }
            }
            Ok(_) => Ok(None),
            Err(DiskError::FileNotFound) => Ok(None),
            Err(error) => Err(Error::TaskExecutionFailed {
                message: format!("Failed to read replacement intent: {error}"),
            }),
        }
    }

    /// get current state
    pub async fn get_state(&self) -> ResumeState {
        self.state.read().await.clone()
    }

    /// update progress
    pub async fn update_progress(&self, processed: u64, successful: u64, failed: u64, skipped: u64) -> Result<()> {
        let mut state = self.state.write().await;
        state.update_progress(processed, successful, failed, skipped);
        drop(state);
        self.save_state_throttled().await
    }

    /// Set current item. Called once per healed object, so persistence is
    /// throttled: the in-memory state always updates, but the snapshot is only
    /// written every `PERSIST_EVERY_MUTATIONS` calls or `PERSIST_INTERVAL`.
    pub async fn set_current_item(&self, bucket: Option<String>, object: Option<String>) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_current_item(bucket, object);
        drop(state);
        let should_save = self.throttle.lock().map(|mut throttle| throttle.record()).unwrap_or(true);
        if !should_save {
            return Ok(());
        }
        self.save_state_throttled().await
    }

    async fn save_state_throttled(&self) -> Result<()> {
        let result = self.save_state().await;
        if result.is_ok()
            && let Ok(mut throttle) = self.throttle.lock()
        {
            throttle.mark_saved();
        }
        result
    }

    /// Read the authoritative version-listing continuation cursor.
    pub async fn resume_cursor(&self) -> Option<String> {
        self.state.read().await.resume_cursor()
    }

    /// Persist the authoritative version-listing continuation cursor. This is
    /// written unthrottled (once per completed page) so a crash always resumes
    /// from a real page boundary.
    pub async fn set_resume_cursor(&self, cursor: Option<String>) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_resume_cursor(cursor);
        drop(state);
        self.save_state().await
    }

    /// complete bucket
    pub async fn complete_bucket(&self, bucket: &str) -> Result<()> {
        let mut state = self.state.write().await;
        state.complete_bucket(bucket);
        drop(state);
        self.save_state_throttled().await
    }

    /// mark task completed
    pub async fn mark_completed(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.mark_completed();
        drop(state);
        self.save_state().await
    }

    /// set error message
    pub async fn set_error(&self, error: String) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_error(error);
        drop(state);
        self.save_state().await
    }

    /// increment retry count
    pub async fn increment_retry(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.increment_retry();
        drop(state);
        self.save_state().await
    }

    /// Arm a bounded retry: if the retry budget remains, bump the retry
    /// counter, reset per-pass progress (so the next resume re-scans the whole
    /// set), persist, and return `true`. Returns `false` when retries are
    /// exhausted, leaving the state untouched.
    pub async fn schedule_retry(&self) -> Result<bool> {
        let mut state = self.state.write().await;
        if !state.can_retry() {
            return Ok(false);
        }
        state.increment_retry();
        state.reset_for_retry();
        drop(state);
        self.save_state().await?;
        Ok(true)
    }

    /// cleanup resume state
    pub async fn cleanup(&self) -> Result<()> {
        let state = self.state.read().await;
        let task_id = state.task_id.clone();
        drop(state);
        validate_resume_task_id(&task_id)?;

        let state_file = self.state_file.path(&task_id);
        let progress_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_PROGRESS_FILE}"));

        delete_resume_file(&self.disk, &progress_file).await?;
        delete_resume_file(&self.disk, &state_file).await?;
        if matches!(self.state_file, ResumeStateFile::ReplacementIntent) {
            delete_resume_file(&self.disk, &replacement_intent_seal_path(&task_id)).await?;
        }

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id,
            state = "cleaned",
            "Heal resume state cleaned"
        );
        Ok(())
    }

    /// save state to disk
    async fn save_state(&self) -> Result<()> {
        self.save_state_with_unformatted_policy(true).await
    }

    async fn save_state_strict(&self) -> Result<()> {
        self.save_state_with_unformatted_policy(false).await
    }

    async fn publish_new_replacement_intent(&self, expected: Option<EcstoreDiskBytes>) -> Result<()> {
        let state = self.state.read().await.clone();
        validate_resume_task_id(&state.task_id)?;
        let state_data = EcstoreDiskBytes::from(serde_json::to_vec(&state).map_err(|error| Error::TaskExecutionFailed {
            message: format!("Failed to serialize resume state: {error}"),
        })?);
        let path = self.state_file.path(&state.task_id);
        let path = path_to_str(&path)?;

        ensure_replacement_recovery_dir(&self.disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        match super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
            self.disk.as_ref(),
            RUSTFS_META_BUCKET,
            path,
            expected,
            Some(state_data),
        )
        .await
        {
            Ok(EcstoreConditionalFileUpdate::Updated) => Ok(()),
            Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => {
                Err(Error::TaskExecutionFailed {
                    message: format!("Replacement intent changed before publication for task {}", state.task_id),
                })
            }
            Err(error) => Err(Error::TaskExecutionFailed {
                message: format!("Failed to save resume state: {error}"),
            }),
        }
    }

    async fn write_replacement_completion_proof(
        &self,
        state: &ResumeState,
        verified_at: Option<u64>,
    ) -> Result<ReplacementCompletionProof> {
        ensure_replacement_recovery_dir(&self.disk)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to create replacement recovery directory: {error}"),
            })?;
        let path = replacement_completion_proof_path(&state.task_id);
        let path_str = path_to_str(&path)?;
        let proof = ReplacementCompletionProof::from_state(
            state,
            verified_at.unwrap_or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()),
        )?;
        let proof_data = EcstoreDiskBytes::from(serde_json::to_vec(&proof).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize replacement completion proof: {e}"),
        })?);
        if let Some(error) = injected_replacement_proof_write_error(path_str) {
            return Err(Error::TaskExecutionFailed {
                message: format!("Failed to save replacement completion proof: {error}"),
            });
        }

        // Publish through the disk CAS primitive: `write_all` can expose a
        // partially written proof to a crash/restart reader. If a prior
        // version left torn bytes behind, replace exactly the observed bytes;
        // a concurrently published valid proof is never overwritten.
        for _ in 0..2 {
            let expected = match self.disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(existing) => match serde_json::from_slice::<ReplacementCompletionProof>(&existing) {
                    Ok(existing_proof) => {
                        existing_proof.validate(&state.task_id)?;
                        if existing_proof.matches_state(state) {
                            return Ok(existing_proof);
                        }
                        return Err(Error::TaskExecutionFailed {
                            message: format!("Replacement completion proof does not match task {}", state.task_id),
                        });
                    }
                    Err(_) => Some(existing),
                },
                Err(DiskError::FileNotFound) => None,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to read replacement completion proof: {error}"),
                    });
                }
            };

            match super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
                self.disk.as_ref(),
                RUSTFS_META_BUCKET,
                path_str,
                expected,
                Some(proof_data.clone()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => return Ok(proof),
                Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => continue,
                Err(error) => {
                    return Err(Error::TaskExecutionFailed {
                        message: format!("Failed to save replacement completion proof: {error}"),
                    });
                }
            }
        }

        Err(Error::TaskExecutionFailed {
            message: format!("Replacement completion proof changed while publishing task {}", state.task_id),
        })
    }

    async fn save_state_with_unformatted_policy(&self, allow_unformatted: bool) -> Result<()> {
        let state = self.state.read().await.clone();
        validate_resume_task_id(&state.task_id)?;
        let state_data = EcstoreDiskBytes::from(serde_json::to_vec(&state).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize resume state: {e}"),
        })?);

        let file_path = self.state_file.path(&state.task_id);

        let path_str = path_to_str(&file_path)?;
        let write_result = match self.state_file {
            ResumeStateFile::Ordinary | ResumeStateFile::LegacyReplacementIntent => {
                self.disk.write_all(RUSTFS_META_BUCKET, path_str, state_data).await
            }
            ResumeStateFile::ReplacementIntent => self.write_replacement_intent_state(path_str, state_data).await,
        };
        if let Err(e) = write_result {
            if allow_unformatted && matches!(e, DiskError::UnformattedDisk) {
                warn!(
                    target: "rustfs::heal::resume",
                    event = EVENT_HEAL_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_RESUME,
                    task_id = %state.task_id,
                    state = "skipped_unformatted_disk",
                    "Heal resume state persistence skipped on unformatted disk"
                );
                return Ok(());
            }
            return Err(Error::TaskExecutionFailed {
                message: format!("Failed to save resume state: {e}"),
            });
        }

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id = %state.task_id,
            state = "saved",
            "Heal resume state persisted"
        );
        Ok(())
    }

    async fn write_replacement_intent_state(
        &self,
        path: &str,
        state_data: EcstoreDiskBytes,
    ) -> std::result::Result<(), DiskError> {
        ensure_replacement_recovery_dir(&self.disk).await?;
        for _ in 0..2 {
            let expected = match self.disk.read_all(RUSTFS_META_BUCKET, path).await {
                Ok(existing) => Some(existing),
                Err(DiskError::FileNotFound) => None,
                Err(error) => return Err(error),
            };
            match super::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
                self.disk.as_ref(),
                RUSTFS_META_BUCKET,
                path,
                expected,
                Some(state_data.clone()),
            )
            .await
            {
                Ok(EcstoreConditionalFileUpdate::Updated) => return Ok(()),
                Ok(EcstoreConditionalFileUpdate::Missing | EcstoreConditionalFileUpdate::Mismatch) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(DiskError::other("replacement intent changed while publishing"))
    }

    /// read state file from disk
    async fn read_state_file(disk: &DiskStore, task_id: &str, state_file: ResumeStateFile) -> Result<Vec<u8>> {
        validate_resume_task_id(task_id)?;
        let file_path = state_file.path(task_id);

        let path_str = path_to_str(&file_path)?;
        disk.read_all(RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read resume state file: {e}"),
            })
    }
}

/// resume checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeCheckpoint {
    /// on-disk schema version; absent in legacy snapshots (defaults to 0)
    #[serde(default)]
    pub schema_version: u32,
    /// task id
    pub task_id: String,
    /// checkpoint time
    pub checkpoint_time: u64,
    /// current bucket index
    pub current_bucket_index: usize,
    /// current object index
    pub current_object_index: usize,
    /// Objects healed since the last completed page. HashSet: with the
    /// previous Vec the per-object `contains` was O(n) and made large-bucket
    /// heals O(N²). Only spans the in-flight page — completed pages are
    /// covered by `current_object_index`, so `complete_page` prunes the sets.
    pub processed_objects: HashSet<String>,
    /// failed objects
    pub failed_objects: HashSet<String>,
    /// skipped objects
    pub skipped_objects: HashSet<String>,
}

impl ResumeCheckpoint {
    pub fn new(task_id: String) -> Self {
        Self {
            schema_version: CURRENT_CHECKPOINT_SCHEMA,
            task_id,
            checkpoint_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            current_bucket_index: 0,
            current_object_index: 0,
            processed_objects: HashSet::new(),
            failed_objects: HashSet::new(),
            skipped_objects: HashSet::new(),
        }
    }

    pub fn update_position(&mut self, bucket_index: usize, object_index: usize) {
        self.current_bucket_index = bucket_index;
        self.current_object_index = object_index;
        self.checkpoint_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn add_processed_object(&mut self, object: String) {
        self.processed_objects.insert(object);
    }

    pub fn add_failed_object(&mut self, object: String) {
        self.failed_objects.insert(object);
    }

    pub fn add_skipped_object(&mut self, object: String) {
        self.skipped_objects.insert(object);
    }

    /// Advance past a fully-processed page: objects below `object_index` are
    /// skipped by position on resume, so the per-object sets no longer need
    /// their entries and would otherwise grow with the whole bucket.
    pub fn complete_page(&mut self, bucket_index: usize, object_index: usize) {
        self.update_position(bucket_index, object_index);
        self.processed_objects.clear();
        self.skipped_objects.clear();
        self.failed_objects.clear();
    }

    /// Reset the scan to the start and clear the per-object sets so a retry
    /// re-scans the whole set.
    pub fn reset_for_retry(&mut self) {
        self.update_position(0, 0);
        self.processed_objects.clear();
        self.skipped_objects.clear();
        self.failed_objects.clear();
    }
}

/// resume checkpoint manager
pub struct CheckpointManager {
    disk: DiskStore,
    checkpoint: Arc<RwLock<ResumeCheckpoint>>,
    throttle: Mutex<PersistThrottle>,
}

impl CheckpointManager {
    /// create new checkpoint manager
    pub async fn new(disk: DiskStore, task_id: String) -> Result<Self> {
        validate_resume_task_id(&task_id)?;
        let checkpoint = ResumeCheckpoint::new(task_id);
        let manager = Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
        };

        // save initial checkpoint
        if let Err(e) = manager.save_checkpoint().await {
            warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                state = "initial_save_failed",
                error = %e,
                "Heal checkpoint persistence failed"
            );
        }
        Ok(manager)
    }

    /// load checkpoint from disk
    pub async fn load_from_disk(disk: DiskStore, task_id: &str) -> Result<Self> {
        validate_resume_task_id(task_id)?;
        let checkpoint_data = Self::read_checkpoint_file(&disk, task_id).await?;
        let mut checkpoint: ResumeCheckpoint =
            serde_json::from_slice(&checkpoint_data).map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to deserialize checkpoint: {e}"),
            })?;
        if checkpoint.task_id != task_id {
            return Err(Error::TaskExecutionFailed {
                message: "Resume checkpoint task id does not match filename".to_string(),
            });
        }

        // A checkpoint from an older schema stored latest-only dedup identities
        // that are not comparable to the new per-version `compose_key`
        // identities. Discard the stale sets and position, then stamp the
        // current schema so the scan restarts cleanly.
        if checkpoint.schema_version > CURRENT_CHECKPOINT_SCHEMA {
            return Err(Error::TaskExecutionFailed {
                message: format!(
                    "Checkpoint schema {} is newer than supported schema {CURRENT_CHECKPOINT_SCHEMA}",
                    checkpoint.schema_version
                ),
            });
        }
        if checkpoint.schema_version < CURRENT_CHECKPOINT_SCHEMA {
            warn!(
                target: "rustfs::heal::resume",
                event = EVENT_HEAL_CHECKPOINT_STATE,
                component = LOG_COMPONENT_HEAL,
                subsystem = LOG_SUBSYSTEM_RESUME,
                task_id,
                found_schema = checkpoint.schema_version,
                current_schema = CURRENT_CHECKPOINT_SCHEMA,
                state = "schema_discarded",
                "Heal checkpoint schema is stale; discarding dedup sets and position"
            );
            checkpoint.processed_objects.clear();
            checkpoint.failed_objects.clear();
            checkpoint.skipped_objects.clear();
            checkpoint.current_bucket_index = 0;
            checkpoint.current_object_index = 0;
            checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA;
        }

        Ok(Self {
            disk,
            checkpoint: Arc::new(RwLock::new(checkpoint)),
            throttle: Mutex::new(PersistThrottle::new()),
        })
    }

    /// check if checkpoint exists
    pub async fn has_checkpoint(disk: &DiskStore, task_id: &str) -> bool {
        if validate_resume_task_id(task_id).is_err() {
            return false;
        }
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        match path_to_str(&file_path) {
            Ok(path_str) => match disk.read_all(RUSTFS_META_BUCKET, path_str).await {
                Ok(data) => !data.is_empty(),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    /// get current checkpoint
    pub async fn get_checkpoint(&self) -> ResumeCheckpoint {
        self.checkpoint.read().await.clone()
    }

    /// update position
    pub async fn update_position(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.update_position(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Advance past a completed page and prune the per-object sets, then persist.
    pub async fn complete_page(&self, bucket_index: usize, object_index: usize) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.complete_page(bucket_index, object_index);
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Reset the checkpoint to the start of the scan for a retry, then persist.
    pub async fn reset_for_retry(&self) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.reset_for_retry();
        drop(checkpoint);
        self.save_checkpoint_throttled().await
    }

    /// Add a processed object. Called once per healed object, so persistence
    /// is batched (`PERSIST_EVERY_MUTATIONS` / `PERSIST_INTERVAL`); positions
    /// and page boundaries still persist unconditionally.
    pub async fn add_processed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_processed_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    /// add failed object (batched, see `add_processed_object`)
    pub async fn add_failed_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_failed_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    /// add skipped object (batched, see `add_processed_object`)
    pub async fn add_skipped_object(&self, object: String) -> Result<()> {
        let mut checkpoint = self.checkpoint.write().await;
        checkpoint.add_skipped_object(object);
        drop(checkpoint);
        self.save_checkpoint_if_due().await
    }

    async fn save_checkpoint_if_due(&self) -> Result<()> {
        let should_save = self.throttle.lock().map(|mut throttle| throttle.record()).unwrap_or(true);
        if !should_save {
            return Ok(());
        }
        self.save_checkpoint_throttled().await
    }

    async fn save_checkpoint_throttled(&self) -> Result<()> {
        let result = self.save_checkpoint().await;
        if result.is_ok()
            && let Ok(mut throttle) = self.throttle.lock()
        {
            throttle.mark_saved();
        }
        result
    }

    /// cleanup checkpoint
    pub async fn cleanup(&self) -> Result<()> {
        let task_id = self.checkpoint.read().await.task_id.clone();
        validate_resume_task_id(&task_id)?;

        let checkpoint_file = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));
        delete_resume_file(&self.disk, &checkpoint_file).await?;

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_CHECKPOINT_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id,
            state = "cleaned",
            "Heal checkpoint cleaned"
        );
        Ok(())
    }

    /// save checkpoint to disk
    async fn save_checkpoint(&self) -> Result<()> {
        let checkpoint = self.checkpoint.read().await;
        validate_resume_task_id(&checkpoint.task_id)?;
        let checkpoint_data = serde_json::to_vec(&*checkpoint).map_err(|e| Error::TaskExecutionFailed {
            message: format!("Failed to serialize checkpoint: {e}"),
        })?;

        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{}_{}", checkpoint.task_id, RESUME_CHECKPOINT_FILE));

        let path_str = path_to_str(&file_path)?;
        self.disk
            .write_all(RUSTFS_META_BUCKET, path_str, checkpoint_data.into())
            .await
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to save checkpoint: {e}"),
            })?;

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_CHECKPOINT_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_id = %checkpoint.task_id,
            state = "saved",
            "Heal checkpoint persisted"
        );
        Ok(())
    }

    /// read checkpoint file from disk
    async fn read_checkpoint_file(disk: &DiskStore, task_id: &str) -> Result<Vec<u8>> {
        validate_resume_task_id(task_id)?;
        let file_path = Path::new(BUCKET_META_PREFIX).join(format!("{task_id}_{RESUME_CHECKPOINT_FILE}"));

        let path_str = path_to_str(&file_path)?;
        disk.read_all(RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|e| Error::TaskExecutionFailed {
                message: format!("Failed to read checkpoint file: {e}"),
            })
    }
}

/// resume utils
pub struct ResumeUtils;

impl ResumeUtils {
    /// generate unique task id
    pub fn generate_task_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// check if task can be resumed
    pub async fn can_resume_task(disk: &DiskStore, task_id: &str) -> bool {
        ResumeManager::has_resume_state(disk, task_id).await
    }

    /// get all resumable task ids
    pub async fn get_resumable_tasks(disk: &DiskStore) -> Result<Vec<String>> {
        // List all files in the buckets metadata directory
        let entries = match disk.list_dir("", RUSTFS_META_BUCKET, BUCKET_META_PREFIX, -1).await {
            Ok(entries) => entries,
            Err(e) => {
                debug!(
                    target: "rustfs::heal::resume",
                    event = EVENT_HEAL_RESUME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_RESUME,
                    state = "list_failed",
                    error = %e,
                    "Heal resume state listing failed"
                );
                return Ok(Vec::new());
            }
        };

        let mut task_ids = Vec::new();

        // Filter files that end with ahm_resume_state.json and extract task IDs
        for entry in entries {
            if entry.ends_with(&format!("_{RESUME_STATE_FILE}")) {
                // Extract task ID from filename: {task_id}_ahm_resume_state.json
                if let Some(task_id) = entry.strip_suffix(&format!("_{RESUME_STATE_FILE}"))
                    && validate_resume_task_id(task_id).is_ok()
                {
                    task_ids.push(task_id.to_string());
                }
            }
        }

        debug!(
            target: "rustfs::heal::resume",
            event = EVENT_HEAL_RESUME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RESUME,
            task_count = task_ids.len(),
            state = "listed",
            "Heal resume states listed"
        );
        Ok(task_ids)
    }

    /// Return replacement intent task IDs from the dedicated recovery
    /// directory. Periodic recovery must never enumerate the ordinary resume
    /// directory, whose cardinality is unrelated to replacement work.
    pub async fn get_replacement_intent_tasks(disk: &DiskStore) -> Result<Vec<String>> {
        let entries = Self::replacement_recovery_entries(disk).await?;
        let suffix = format!("_{REPLACEMENT_INTENT_FILE}");
        let mut task_ids = HashSet::new();

        for entry in entries {
            if let Some(task_id) = entry.strip_suffix(&suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                task_ids.insert(task_id.to_string());
                continue;
            }
        }

        let mut task_ids = task_ids.into_iter().collect::<Vec<_>>();
        task_ids.sort_unstable();
        Ok(task_ids)
    }

    async fn replacement_recovery_entries(disk: &DiskStore) -> Result<Vec<String>> {
        let recovery_dir = replacement_recovery_dir();
        let recovery_dir = path_to_str(&recovery_dir)?;
        match disk.list_dir("", RUSTFS_META_BUCKET, recovery_dir, -1).await {
            Ok(entries) => Ok(entries),
            Err(DiskError::FileNotFound) => Ok(Vec::new()),
            Err(error) => Err(Error::TaskExecutionFailed {
                message: format!("Failed to list replacement recovery records: {error}"),
            }),
        }
    }

    /// Migrate flat replacement artifacts from earlier builds exactly once at
    /// manager startup. The normal scanner only uses the dedicated directory;
    /// ordinary resume JSON is never read on its periodic path.
    pub async fn migrate_legacy_replacement_records(disk: &DiskStore) -> Result<()> {
        let entries = disk
            .list_dir("", RUSTFS_META_BUCKET, BUCKET_META_PREFIX, -1)
            .await
            .map_err(|error| Error::TaskExecutionFailed {
                message: format!("Failed to list legacy replacement records: {error}"),
            })?;
        let ordinary_suffix = format!("_{RESUME_STATE_FILE}");
        let intent_suffix = format!("_{REPLACEMENT_INTENT_FILE}");
        let proof_suffix = format!("_{REPLACEMENT_COMPLETION_PROOF_FILE}");
        let mut ordinary_task_ids = HashSet::new();
        let mut intent_task_ids = HashSet::new();
        let mut proof_task_ids = HashSet::new();

        for entry in entries {
            if let Some(task_id) = entry.strip_suffix(&intent_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                intent_task_ids.insert(task_id.to_string());
                continue;
            }
            if let Some(task_id) = entry.strip_suffix(&ordinary_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                ordinary_task_ids.insert(task_id.to_string());
                continue;
            }
            if let Some(task_id) = entry.strip_suffix(&proof_suffix)
                && validate_resume_task_id(task_id).is_ok()
            {
                proof_task_ids.insert(task_id.to_string());
            }
        }

        let mut state_task_ids = intent_task_ids.into_iter().collect::<Vec<_>>();
        state_task_ids.extend(ordinary_task_ids);
        state_task_ids.sort_unstable();
        state_task_ids.dedup();
        for task_id in state_task_ids {
            let has_flat_intent = ResumeManager::has_state_file(disk, &task_id, ResumeStateFile::LegacyReplacementIntent).await;
            if !has_flat_intent {
                let Ok(manager) = ResumeManager::load_from_disk(disk.clone(), &task_id).await else {
                    continue;
                };
                if !is_replacement_intent(&manager.get_state().await) {
                    continue;
                }
            }
            ResumeManager::load_replacement_intent(disk.clone(), &task_id).await?;
        }

        for task_id in proof_task_ids {
            ResumeManager::migrate_legacy_replacement_completion_proof(disk, &task_id).await?;
        }
        Ok(())
    }

    /// Return all durable replacement states and completion proofs stored on
    /// one survivor disk. Unlike the legacy resumable-task helper, listing
    /// failures are returned to the caller so an observability surface cannot
    /// silently turn an unreadable durable record into a green result.
    pub async fn get_replacement_recovery_records(disk: &DiskStore) -> Result<Vec<ReplacementRecoveryRecord>> {
        let entries = Self::replacement_recovery_entries(disk).await?;
        let proof_suffix = format!("_{REPLACEMENT_COMPLETION_PROOF_FILE}");
        let mut records = Vec::new();
        let mut intent_task_ids = HashSet::new();

        for task_id in Self::get_replacement_intent_tasks(disk).await? {
            let state = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
                .await?
                .get_state()
                .await;
            intent_task_ids.insert(task_id.clone());
            records.push(ReplacementRecoveryRecord::from_state(state).unwrap_or_else(|| {
                ReplacementRecoveryRecord::unknown(
                    task_id,
                    "isolated replacement intent violates its generation or target identity binding",
                )
            }));
        }

        for entry in entries {
            let Some(task_id) = entry.strip_suffix(&proof_suffix) else {
                continue;
            };
            if validate_resume_task_id(task_id).is_err() {
                continue;
            }
            if intent_task_ids.contains(task_id) {
                continue;
            }
            let proof = ResumeManager::load_replacement_completion_proof(disk.clone(), task_id).await?;
            records.push(ReplacementRecoveryRecord::from_completion_proof(&proof));
        }

        records.sort_by(|left, right| left.task_id.cmp(&right.task_id).then(left.state.cmp(&right.state)));
        Ok(records)
    }

    /// cleanup expired resume states
    pub async fn cleanup_expired_states(disk: &DiskStore, max_age_hours: u64) -> Result<()> {
        let task_ids = Self::get_resumable_tasks(disk).await?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for task_id in task_ids {
            if let Ok(resume_manager) = ResumeManager::load_from_disk(disk.clone(), &task_id).await {
                let state = resume_manager.get_state().await;
                let age_hours = current_time.saturating_sub(state.last_update) / 3600;

                if !state.completed && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding)
                {
                    continue;
                }
                if state.completed
                    && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)
                {
                    continue;
                }

                if age_hours > max_age_hours {
                    debug!(
                        target: "rustfs::heal::resume",
                        event = EVENT_HEAL_RESUME_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_RESUME,
                        task_id,
                        age_hours,
                        state = "expired_cleanup_started",
                        "Heal resume cleanup started"
                    );
                    if let Err(e) = resume_manager.cleanup().await {
                        warn!(
                            target: "rustfs::heal::resume",
                            event = EVENT_HEAL_RESUME_STATE,
                            component = LOG_COMPONENT_HEAL,
                            subsystem = LOG_SUBSYSTEM_RESUME,
                            task_id,
                            age_hours,
                            state = "expired_cleanup_failed",
                            error = %e,
                            "Heal resume state cleanup failed"
                        );
                    }
                }
            }
        }

        for task_id in Self::get_replacement_intent_tasks(disk).await? {
            if let Ok(resume_manager) = ResumeManager::load_replacement_intent(disk.clone(), &task_id).await {
                let state = resume_manager.get_state().await;
                let age_hours = current_time.saturating_sub(state.last_update) / 3600;

                if !state.completed && matches!(state.replacement_phase, ReplacementPhase::Intent | ReplacementPhase::Rebuilding)
                {
                    continue;
                }
                if state.completed
                    && matches!(state.replacement_phase, ReplacementPhase::Verified | ReplacementPhase::CleanupPending)
                {
                    continue;
                }

                if age_hours > max_age_hours
                    && let Err(e) = resume_manager.cleanup().await
                {
                    warn!(
                        target: "rustfs::heal::resume",
                        event = EVENT_HEAL_RESUME_STATE,
                        component = LOG_COMPONENT_HEAL,
                        subsystem = LOG_SUBSYSTEM_RESUME,
                        task_id,
                        age_hours,
                        state = "expired_cleanup_failed",
                        error = %e,
                        "Replacement intent cleanup failed"
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn schema_test_disk() -> (tempfile::TempDir, DiskStore) {
        use super::super::{DiskOption, Endpoint, new_disk};

        let temp_dir = tempfile::TempDir::new().expect("create schema test directory");
        let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create schema test disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create schema test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("create metadata volume: {error}"),
        }
        match disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("create resume metadata volume: {error}"),
        }

        (temp_dir, disk)
    }

    #[tokio::test]
    async fn test_resume_state_creation() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let state = ResumeState::new(task_id.clone(), "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        assert_eq!(state.task_id, task_id);
        assert_eq!(state.task_type, "erasure_set");
        assert!(!state.completed);
        assert_eq!(state.processed_objects, 0);
        assert_eq!(state.pending_buckets.len(), 2);
    }

    #[test]
    fn replacement_intent_binds_a_generation_before_format() {
        let state = ResumeState::replacement_intent(
            "generation-a".to_string(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket-a".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        );

        assert_eq!(state.replacement_generation.as_deref(), Some("generation-a"));
        assert_eq!(state.replacement_phase, ReplacementPhase::Intent);
        assert_eq!(state.replacement_targets, ["replacement-a"]);
        assert!(state.resume_cursor.is_none(), "a new replacement must start from the beginning");

        let mut state = state;
        state.complete_bucket("bucket-a");
        assert_eq!(
            state.replacement_buckets,
            ["bucket-a"],
            "recovery must retain the original positional bucket plan"
        );
    }

    #[tokio::test]
    async fn replacement_terminal_phases_are_durable() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("create replacement phase test directory");
        let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("create metadata volume for replacement phase test: {err}"),
        }

        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");
        manager
            .mark_replacement_completed_and_verified()
            .await
            .expect("completion and verified phase must persist together");

        let verified = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("verified phase must survive a restart")
            .get_state()
            .await;
        assert!(verified.completed);
        assert_eq!(verified.replacement_phase, ReplacementPhase::Verified);

        let resumed = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["ignored-after-restart".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("verified replacement must be reopenable for terminal cleanup");
        assert_eq!(
            resumed.get_state().await.replacement_buckets,
            ["bucket"],
            "terminal recovery must preserve the original generation bucket plan"
        );

        manager
            .mark_replacement_cleanup_pending()
            .await
            .expect("cleanup-pending phase must persist after marker removal");
        let cleanup_pending = ResumeManager::load_replacement_intent(disk, &task_id)
            .await
            .expect("cleanup-pending phase must survive a restart")
            .get_state()
            .await;
        assert!(cleanup_pending.completed);
        assert_eq!(cleanup_pending.replacement_phase, ReplacementPhase::CleanupPending);
    }

    #[tokio::test]
    async fn replacement_proof_before_verified_state_is_reconciled_after_restart() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");

        let proof = ReplacementCompletionProof::from_state(&manager.get_state().await, 42)
            .expect("active replacement state should build a completion proof");
        let proof_path = replacement_completion_proof_path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            proof_path.to_str().expect("completion proof path must be UTF-8"),
            serde_json::to_vec(&proof)
                .expect("completion proof fixture should serialize")
                .into(),
        )
        .await
        .expect("proof-first crash fixture should persist");

        let recovered = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("matching completion proof must prevent another rebuild")
            .get_state()
            .await;
        assert!(recovered.completed);
        assert_eq!(recovered.replacement_phase, ReplacementPhase::Verified);
        assert_eq!(recovered.last_update, proof.verified_at);

        let records = ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("reconciled replacement state should be observable");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, ReplacementRecoveryState::CleanupPending);
    }

    #[tokio::test]
    async fn replacement_proof_conflicting_with_active_state_fails_closed_after_restart() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");

        let mut proof = ReplacementCompletionProof::from_state(&manager.get_state().await, 42)
            .expect("active replacement state should build a completion proof");
        proof.set_disk_id = "pool_0_set_1".to_string();
        let proof_path = replacement_completion_proof_path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            proof_path.to_str().expect("completion proof path must be UTF-8"),
            serde_json::to_vec(&proof)
                .expect("completion proof fixture should serialize")
                .into(),
        )
        .await
        .expect("conflicting proof fixture should persist");

        let error = match ResumeManager::load_replacement_intent(disk, &task_id).await {
            Ok(_) => panic!("a mismatched proof must not permit another rebuild"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not match active intent"));
    }

    #[tokio::test]
    async fn replacement_intent_is_not_an_ordinary_resumable_task() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist in its isolated namespace");

        assert!(
            !ResumeManager::has_resume_state(&disk, &task_id).await,
            "an old ordinary-resume lookup must not discover a replacement intent"
        );
        assert!(ResumeManager::has_replacement_intent(&disk, &task_id).await);
        assert!(
            !ResumeUtils::get_resumable_tasks(&disk)
                .await
                .expect("ordinary resume listing should succeed")
                .contains(&task_id),
            "the old filename enumeration must not return replacement work"
        );
        assert_eq!(
            ResumeUtils::get_replacement_intent_tasks(&disk)
                .await
                .expect("replacement intent listing should succeed"),
            vec![task_id.clone()]
        );
        assert_eq!(
            ResumeManager::load_replacement_intent(disk, &task_id)
                .await
                .expect("new replacement reader should load the isolated state")
                .get_state()
                .await,
            manager.get_state().await
        );
    }

    #[tokio::test]
    async fn replacement_intent_recovers_from_torn_publication_before_formatting() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            intent_path.to_str().expect("replacement intent path must be UTF-8"),
            b"{torn replacement intent".as_slice().into(),
        )
        .await
        .expect("torn replacement intent fixture should persist");

        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("a retry must atomically replace only a torn pre-format intent");

        let recovered = ResumeManager::load_replacement_intent(disk, &task_id)
            .await
            .expect("recovered intent should be readable after restart")
            .get_state()
            .await;
        assert_eq!(recovered, manager.get_state().await);
        assert_eq!(recovered.replacement_phase, ReplacementPhase::Intent);
    }

    #[tokio::test]
    async fn torn_intent_recovery_cas_preserves_a_concurrent_valid_binding() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
        let intent_path = intent_path.to_str().expect("replacement intent path must be UTF-8");
        let torn = EcstoreDiskBytes::from_static(b"{torn replacement intent");
        disk.write_all(RUSTFS_META_BUCKET, intent_path, torn)
            .await
            .expect("torn intent fixture should persist");

        let expected = ResumeManager::torn_replacement_intent_bytes(&disk, &task_id)
            .await
            .expect("torn intent should be recoverable before a seal exists")
            .expect("torn intent bytes should be retained as the CAS precondition");
        let winner = ResumeState::replacement_intent(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_winner".to_string(),
            vec!["winner-bucket".to_string()],
            vec!["replacement-winner".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-winner".to_string(),
                canonical_path: "/mnt/replacement-winner".to_string(),
                physical_device_ids: vec!["device-winner".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        );
        let winner_bytes = EcstoreDiskBytes::from(serde_json::to_vec(&winner).expect("winner intent should serialize"));
        disk.write_all(RUSTFS_META_BUCKET, intent_path, winner_bytes.clone())
            .await
            .expect("concurrent valid intent fixture should persist");

        let loser = ResumeManager {
            disk: disk.clone(),
            state: Arc::new(RwLock::new(ResumeState::replacement_intent(
                task_id.clone(),
                "erasure_set".to_string(),
                "pool_0_set_loser".to_string(),
                vec!["loser-bucket".to_string()],
                vec!["replacement-loser".to_string()],
                vec![ReplacementTargetIdentity {
                    endpoint: "replacement-loser".to_string(),
                    canonical_path: "/mnt/replacement-loser".to_string(),
                    physical_device_ids: vec!["device-loser".to_string()],
                    filesystem_identity: "4:5:6".to_string(),
                }],
            ))),
            throttle: Mutex::new(PersistThrottle::new()),
            state_file: ResumeStateFile::ReplacementIntent,
        };
        let error = match loser.publish_new_replacement_intent(Some(expected)).await {
            Ok(()) => panic!("a stale torn-intent recovery must not overwrite a concurrent valid binding"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("changed before publication"));
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, intent_path)
                .await
                .expect("concurrent valid intent must remain durable"),
            winner_bytes
        );
    }

    #[tokio::test]
    async fn replacement_intent_does_not_recreate_torn_state_after_seal_publication() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let intent_path = ResumeStateFile::ReplacementIntent.path(&task_id);
        let intent_path = intent_path.to_str().expect("replacement intent path must be UTF-8");
        let torn = b"{torn replacement intent";
        disk.write_all(RUSTFS_META_BUCKET, intent_path, torn.as_slice().into())
            .await
            .expect("torn replacement intent fixture should persist");
        let marker_path = replacement_intent_seal_path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            marker_path.to_str().expect("replacement seal path must be UTF-8"),
            b"sealed".as_slice().into(),
        )
        .await
        .expect("replacement seal fixture should persist");

        let error = match ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        {
            Ok(_) => panic!("a seal means a torn state may have crossed the format boundary"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Failed to deserialize resume state"));
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, intent_path)
                .await
                .expect("torn intent must remain for operator recovery"),
            torn.as_slice()
        );
    }

    #[tokio::test]
    async fn replacement_intent_migrates_from_legacy_resume_filename() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let legacy = ResumeState::replacement_intent(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        );
        let legacy_path = ResumeStateFile::Ordinary.path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            legacy_path.to_str().expect("legacy resume path must be UTF-8"),
            serde_json::to_vec(&legacy)
                .expect("serialize legacy replacement state")
                .into(),
        )
        .await
        .expect("write legacy replacement state");

        let migrated = ResumeManager::load_replacement_intent(disk.clone(), &task_id)
            .await
            .expect("new binary should migrate a legacy replacement state");
        assert_eq!(migrated.get_state().await, legacy);
        assert!(
            !ResumeManager::has_resume_state(&disk, &task_id).await,
            "migration must remove the old-binary-visible state only after the new state is durable"
        );
        assert!(ResumeManager::has_replacement_intent(&disk, &task_id).await);
    }

    #[tokio::test]
    async fn ordinary_targeted_resume_is_not_migrated_as_a_replacement_intent() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new(
            disk.clone(),
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("ordinary targeted resume should persist");
        manager
            .set_replacement_targets(vec!["manual-target".to_string()])
            .await
            .expect("ordinary targeted resume should retain its target filter");

        assert!(!ResumeManager::has_replacement_intent(&disk, &task_id).await);
        assert!(ResumeManager::load_replacement_intent(disk.clone(), &task_id).await.is_err());
        assert!(ResumeManager::has_resume_state(&disk, &task_id).await);
    }

    #[tokio::test]
    async fn malformed_isolated_replacement_intent_is_reported_as_unknown() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let malformed = ResumeState::new(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        );
        let path = ResumeStateFile::ReplacementIntent.path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            path.to_str().expect("isolated replacement path must be UTF-8"),
            serde_json::to_vec(&malformed)
                .expect("malformed replacement fixture should serialize")
                .into(),
        )
        .await
        .expect("malformed isolated replacement state should persist");

        let records = ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("isolated replacement listing should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].task_id, task_id);
        assert_eq!(records[0].state, ReplacementRecoveryState::Unknown);
    }

    #[tokio::test]
    async fn startup_migration_moves_flat_replacement_artifacts_to_dedicated_directory() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let mut state = ResumeState::replacement_intent(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        );
        state.mark_completed();
        state.replacement_phase = ReplacementPhase::Verified;
        let legacy_intent = ResumeStateFile::LegacyReplacementIntent.path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            legacy_intent.to_str().expect("legacy intent path must be UTF-8"),
            serde_json::to_vec(&state)
                .expect("serialize legacy replacement intent")
                .into(),
        )
        .await
        .expect("write legacy replacement intent");
        let proof = ReplacementCompletionProof::from_state(&state, state.last_update).expect("build legacy completion proof");
        let legacy_proof = legacy_replacement_completion_proof_path(&task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            legacy_proof.to_str().expect("legacy proof path must be UTF-8"),
            serde_json::to_vec(&proof).expect("serialize legacy completion proof").into(),
        )
        .await
        .expect("write legacy completion proof");

        ResumeUtils::migrate_legacy_replacement_records(&disk)
            .await
            .expect("startup migration should move flat replacement artifacts");

        assert_eq!(
            ResumeUtils::get_replacement_intent_tasks(&disk)
                .await
                .expect("dedicated intent listing should succeed"),
            vec![task_id.clone()]
        );
        assert_eq!(
            ResumeManager::load_replacement_completion_proof(disk.clone(), &task_id)
                .await
                .expect("dedicated completion proof should be readable"),
            proof
        );
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, legacy_intent.to_str().expect("legacy intent path must be UTF-8"))
                .await,
            Err(DiskError::FileNotFound)
        ));
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, legacy_proof.to_str().expect("legacy proof path must be UTF-8"))
                .await,
            Err(DiskError::FileNotFound)
        ));
    }

    #[tokio::test]
    async fn replacement_discovery_does_not_read_ordinary_resume_directory() {
        let (_temp_dir, disk) = schema_test_disk().await;
        for _ in 0..3 {
            ResumeManager::new(
                disk.clone(),
                ResumeUtils::generate_task_id(),
                "erasure_set".to_string(),
                "pool_0_set_0".to_string(),
                vec!["bucket".to_string()],
            )
            .await
            .expect("ordinary resume state should persist");
        }
        let corrupt_task_id = ResumeUtils::generate_task_id();
        let corrupt_path = ResumeStateFile::Ordinary.path(&corrupt_task_id);
        disk.write_all(
            RUSTFS_META_BUCKET,
            corrupt_path.to_str().expect("ordinary resume path must be UTF-8"),
            b"not-json".to_vec().into(),
        )
        .await
        .expect("corrupt ordinary resume state should persist");

        let replacement_task_id = ResumeUtils::generate_task_id();
        ResumeManager::new_replacement_intent(
            disk.clone(),
            replacement_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist in the dedicated directory");

        assert_eq!(
            ResumeUtils::get_replacement_intent_tasks(&disk)
                .await
                .expect("dedicated replacement listing should not parse ordinary JSON"),
            vec![replacement_task_id]
        );
    }

    #[tokio::test]
    async fn empty_replacement_recovery_directory_is_not_an_error() {
        let (_temp_dir, disk) = schema_test_disk().await;
        assert!(
            ResumeUtils::get_replacement_intent_tasks(&disk)
                .await
                .expect("missing recovery directory should be empty")
                .is_empty()
        );
        assert!(
            ResumeUtils::get_replacement_recovery_records(&disk)
                .await
                .expect("missing recovery directory should have no records")
                .is_empty()
        );
        ResumeUtils::cleanup_expired_states(&disk, 0)
            .await
            .expect("missing recovery directory should not block expiry cleanup");
    }

    #[tokio::test]
    async fn replacement_completion_proof_survives_resume_cleanup() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let identity = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        };
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![identity.clone()],
        )
        .await
        .expect("replacement intent should persist");
        manager
            .mark_replacement_completed_and_verified()
            .await
            .expect("verified replacement must persist proof before completion");

        let proof = ResumeManager::load_replacement_completion_proof(disk.clone(), &task_id)
            .await
            .expect("completion proof must be readable from the survivor anchor");
        assert_eq!(proof.task_id, task_id);
        assert_eq!(proof.replacement_generation, proof.task_id);
        assert_eq!(proof.set_disk_id, "pool_0_set_0");
        assert_eq!(proof.replacement_targets, ["replacement-a"]);
        assert_eq!(proof.replacement_target_identities, vec![identity]);
        assert!(proof.verified_at > 0);

        manager.cleanup().await.expect("resume cleanup should succeed");
        assert!(
            !ResumeManager::has_replacement_intent(&disk, &proof.task_id).await,
            "completion cleanup must remove the resumable state"
        );
        assert_eq!(
            ResumeManager::load_replacement_completion_proof(disk, &proof.task_id)
                .await
                .expect("survivor proof must outlive resume cleanup"),
            proof
        );
    }

    #[tokio::test]
    async fn replacement_recovery_records_distinguish_active_and_proven_completion() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");

        let active = ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("active replacement record should be readable");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].task_id, task_id);
        assert_eq!(active[0].state, ReplacementRecoveryState::WaitingForReplacement);
        assert_eq!(active[0].generation.as_deref(), Some(task_id.as_str()));

        manager
            .mark_replacement_completed_and_verified()
            .await
            .expect("completion proof should persist");

        let cleanup_pending = ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("cleanup-pending replacement record should be readable");
        assert_eq!(cleanup_pending.len(), 1);
        assert_eq!(cleanup_pending[0].state, ReplacementRecoveryState::CleanupPending);

        manager.cleanup().await.expect("resume state cleanup should succeed");

        let completed = ResumeUtils::get_replacement_recovery_records(&disk)
            .await
            .expect("completion proof should remain readable");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].state, ReplacementRecoveryState::Completed);
        assert!(completed[0].verified_at.is_some());
    }

    #[tokio::test]
    async fn replacement_completion_write_failure_cannot_mark_completed() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk,
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");
        let proof_path = replacement_completion_proof_path(&task_id)
            .to_str()
            .expect("completion proof path must be UTF-8")
            .to_string();
        let _failure = ReplacementProofWriteFailure::install(proof_path, DiskError::DiskAccessDenied);

        let error = manager
            .mark_replacement_completed_and_verified()
            .await
            .expect_err("completion must fail closed when durable proof cannot be written");
        assert!(error.to_string().contains("Failed to save replacement completion proof"));
        let state = manager.get_state().await;
        assert!(!state.completed, "a failed proof write must not produce a completed state");
        assert_eq!(state.replacement_phase, ReplacementPhase::Intent);
    }

    #[tokio::test]
    async fn replacement_completion_repairs_torn_proof_without_wedging_rebuild() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let identity = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        };
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![identity.clone()],
        )
        .await
        .expect("replacement intent should persist");
        manager
            .mark_replacement_rebuilding(vec![identity])
            .await
            .expect("replacement fixture should enter rebuilding before proof publication");
        let proof_path = replacement_completion_proof_path(&task_id);
        let proof_path = proof_path.to_str().expect("completion proof path must be UTF-8");
        disk.write_all(RUSTFS_META_BUCKET, proof_path, b"{torn completion proof".as_slice().into())
            .await
            .expect("torn proof fixture should persist");

        manager
            .mark_replacement_completed_and_verified()
            .await
            .expect("a rebuilding generation must replace only its torn completion proof");

        let proof = ResumeManager::load_replacement_completion_proof(disk, &task_id)
            .await
            .expect("repaired completion proof should be durable and readable");
        assert_eq!(proof.task_id, task_id);
        assert_eq!(proof.replacement_generation, proof.task_id);
    }

    #[tokio::test]
    async fn replacement_completion_does_not_replace_a_valid_mismatched_proof() {
        let (_temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![ReplacementTargetIdentity {
                endpoint: "replacement-a".to_string(),
                canonical_path: "/mnt/replacement-a".to_string(),
                physical_device_ids: vec!["device-a".to_string()],
                filesystem_identity: "1:2:3".to_string(),
            }],
        )
        .await
        .expect("replacement intent should persist");
        let mut conflicting = ReplacementCompletionProof::from_state(&manager.get_state().await, 1)
            .expect("replacement state should build a proof fixture");
        conflicting.set_disk_id = "pool_0_set_1".to_string();
        let proof_path = replacement_completion_proof_path(&task_id);
        let proof_path = proof_path.to_str().expect("completion proof path must be UTF-8");
        disk.write_all(
            RUSTFS_META_BUCKET,
            proof_path,
            serde_json::to_vec(&conflicting)
                .expect("proof fixture should serialize")
                .into(),
        )
        .await
        .expect("conflicting proof fixture should persist");

        let error = manager
            .mark_replacement_completed_and_verified()
            .await
            .expect_err("a distinct durable generation binding must not be overwritten");
        assert!(error.to_string().contains("does not match task"));
        assert_eq!(
            ResumeManager::load_replacement_completion_proof(disk, &task_id)
                .await
                .expect("valid conflicting proof should remain intact"),
            conflicting
        );
    }

    #[tokio::test]
    async fn cleanup_expired_states_keeps_all_durable_replacement_phases() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("create replacement expiry test directory");
        let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("create metadata volume for replacement expiry test: {err}"),
        }

        let target = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        };
        let intent_task_id = ResumeUtils::generate_task_id();
        let rebuilding_task_id = ResumeUtils::generate_task_id();
        let verified_task_id = ResumeUtils::generate_task_id();
        let cleanup_pending_task_id = ResumeUtils::generate_task_id();
        let abandoned_task_id = ResumeUtils::generate_task_id();
        let ordinary_task_id = ResumeUtils::generate_task_id();
        let intent = ResumeManager::new_replacement_intent(
            disk.clone(),
            intent_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![target.clone()],
        )
        .await
        .expect("replacement intent should persist");
        let rebuilding = ResumeManager::new_replacement_intent(
            disk.clone(),
            rebuilding_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![target.clone()],
        )
        .await
        .expect("replacement rebuilding state should persist");
        rebuilding
            .mark_replacement_rebuilding(vec![target.clone()])
            .await
            .expect("replacement rebuilding phase should persist");
        let verified = ResumeManager::new_replacement_intent(
            disk.clone(),
            verified_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![target.clone()],
        )
        .await
        .expect("replacement verified state should persist");
        verified
            .mark_replacement_completed_and_verified()
            .await
            .expect("replacement verified phase should persist");
        let cleanup_pending = ResumeManager::new_replacement_intent(
            disk.clone(),
            cleanup_pending_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![target.clone()],
        )
        .await
        .expect("replacement cleanup-pending state should persist");
        cleanup_pending
            .mark_replacement_completed_and_verified()
            .await
            .expect("replacement completion should persist");
        cleanup_pending
            .mark_replacement_cleanup_pending()
            .await
            .expect("replacement cleanup-pending phase should persist");
        let abandoned = ResumeManager::new_replacement_intent(
            disk.clone(),
            abandoned_task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
            vec!["replacement-a".to_string()],
            vec![target],
        )
        .await
        .expect("replacement abandoned state should persist");
        abandoned
            .abandon_replacement_intent()
            .await
            .expect("replacement abandoned phase should persist");
        let ordinary = ResumeManager::new(
            disk.clone(),
            ordinary_task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("ordinary resume state should persist");

        for manager in [&intent, &rebuilding, &verified, &cleanup_pending, &abandoned, &ordinary] {
            manager.state.write().await.last_update = 0;
            manager.save_state_strict().await.expect("persist expired resume state");
        }

        ResumeUtils::cleanup_expired_states(&disk, 0)
            .await
            .expect("replacement expiry cleanup should complete");

        for (task_id, expected_phase) in [
            (intent_task_id.as_str(), ReplacementPhase::Intent),
            (rebuilding_task_id.as_str(), ReplacementPhase::Rebuilding),
            (verified_task_id.as_str(), ReplacementPhase::Verified),
            (cleanup_pending_task_id.as_str(), ReplacementPhase::CleanupPending),
        ] {
            let state = ResumeManager::load_replacement_intent(disk.clone(), task_id)
                .await
                .expect("durable replacement state must survive expiry cleanup")
                .get_state()
                .await;
            assert_eq!(state.replacement_phase, expected_phase);
        }
        assert!(
            !ResumeManager::has_replacement_intent(&disk, &abandoned_task_id).await,
            "an abandoned replacement must expire"
        );
        assert!(
            !ResumeManager::has_resume_state(&disk, &ordinary_task_id).await,
            "an ordinary expired resume must expire"
        );
    }

    #[tokio::test]
    async fn test_resume_state_progress() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string()];
        let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        state.update_progress(10, 8, 1, 1);
        assert_eq!(state.processed_objects, 10);
        assert_eq!(state.successful_objects, 8);
        assert_eq!(state.failed_objects, 1);
        assert_eq!(state.skipped_objects, 1);

        let progress = state.get_progress_percentage();
        assert_eq!(progress, 0.0); // total_objects is 0

        state.total_objects = 100;
        let progress = state.get_progress_percentage();
        assert_eq!(progress, 10.0);
    }

    #[tokio::test]
    async fn replacement_intent_rejects_a_new_mount_at_the_same_endpoint() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let disk_path = temp_dir.path().join("resume_disk");
        std::fs::create_dir_all(&disk_path).unwrap();
        let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .unwrap();
        let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
        let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;

        let task_id = ResumeUtils::generate_task_id();
        let targets = vec!["replacement-a".to_string()];
        let first = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: vec!["device-a".to_string()],
            filesystem_identity: "1:2:3".to_string(),
        };
        ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket-a".to_string()],
            targets.clone(),
            vec![first.clone()],
        )
        .await
        .unwrap();

        let reused = ResumeManager::new_replacement_intent(
            disk.clone(),
            task_id.clone(),
            "pool_0_set_0".to_string(),
            vec!["bucket-b".to_string()],
            targets.clone(),
            vec![first.clone()],
        )
        .await
        .unwrap();
        assert_eq!(
            reused.get_state().await.replacement_buckets,
            ["bucket-a"],
            "retries must keep the first generation's bucket plan"
        );

        let mut second = ReplacementTargetIdentity {
            endpoint: "replacement-a".to_string(),
            canonical_path: "/mnt/replacement-a".to_string(),
            physical_device_ids: first.physical_device_ids.clone(),
            filesystem_identity: first.filesystem_identity.clone(),
        };
        for changed_identity in [
            {
                second.physical_device_ids = vec!["device-b".to_string()];
                second.clone()
            },
            {
                second.physical_device_ids = first.physical_device_ids.clone();
                second.filesystem_identity = "4:5:6".to_string();
                second.clone()
            },
            {
                second.filesystem_identity = first.filesystem_identity.clone();
                second.canonical_path = "/mnt/replacement-b".to_string();
                second.clone()
            },
        ] {
            let result = ResumeManager::new_replacement_intent(
                disk.clone(),
                task_id.clone(),
                "pool_0_set_0".to_string(),
                vec!["bucket-a".to_string()],
                targets.clone(),
                vec![changed_identity],
            )
            .await;
            assert!(result.is_err(), "a new mounted instance must not reuse the old replacement cursor");
        }
        temp_dir.close().unwrap();
    }

    #[test]
    fn reset_for_retry_clears_progress_but_keeps_retry_budget() {
        // backlog#855 / #799 B6: a retry must re-scan from the start without
        // spending the retry budget's identity.
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let mut state = ResumeState::new("t".to_string(), "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);
        state.update_progress(10, 8, 2, 0);
        state.complete_bucket("bucket1");
        state.increment_retry();
        state.mark_completed();

        state.reset_for_retry();

        assert!(!state.completed, "retry must un-complete the task");
        assert_eq!(state.completed_buckets.len(), 0, "all buckets must be re-scanned");
        assert_eq!(state.processed_objects, 0);
        assert_eq!(state.successful_objects, 0);
        assert_eq!(state.failed_objects, 0);
        assert_eq!(state.skipped_objects, 0);
        assert_eq!(state.retry_count, 1, "retry budget must be preserved");
    }

    #[test]
    fn can_retry_is_bounded_by_max_retries() {
        let mut state = ResumeState::new("t".to_string(), "erasure_set".to_string(), "pool_0_set_0".to_string(), vec![]);
        assert!(state.can_retry());
        for _ in 0..state.max_retries {
            assert!(state.can_retry());
            state.increment_retry();
        }
        assert!(!state.can_retry(), "retries must stop after max_retries");
    }

    #[test]
    fn checkpoint_reset_for_retry_rewinds_position_and_clears_sets() {
        let mut checkpoint = ResumeCheckpoint::new("task".to_string());
        checkpoint.update_position(3, 42);
        checkpoint.add_processed_object("bucket/a".to_string());
        checkpoint.add_failed_object("bucket/b".to_string());
        checkpoint.add_skipped_object("bucket/c".to_string());

        checkpoint.reset_for_retry();

        assert_eq!(checkpoint.current_bucket_index, 0);
        assert_eq!(checkpoint.current_object_index, 0);
        assert!(checkpoint.processed_objects.is_empty());
        assert!(checkpoint.failed_objects.is_empty());
        assert!(checkpoint.skipped_objects.is_empty());
    }

    #[tokio::test]
    async fn test_resume_state_bucket_completion() {
        let task_id = ResumeUtils::generate_task_id();
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let mut state = ResumeState::new(task_id, "erasure_set".to_string(), "pool_0_set_0".to_string(), buckets);

        assert_eq!(state.pending_buckets.len(), 2);
        assert_eq!(state.completed_buckets.len(), 0);

        state.complete_bucket("bucket1");
        assert_eq!(state.pending_buckets.len(), 1);
        assert_eq!(state.completed_buckets.len(), 1);
        assert!(state.completed_buckets.contains(&"bucket1".to_string()));
    }

    #[test]
    fn test_checkpoint_object_sets_dedupe_and_prune() {
        let mut checkpoint = ResumeCheckpoint::new("task".to_string());
        checkpoint.add_processed_object("bucket/a".to_string());
        checkpoint.add_processed_object("bucket/a".to_string());
        checkpoint.add_skipped_object("bucket/b".to_string());
        checkpoint.add_failed_object("bucket/c".to_string());
        assert_eq!(checkpoint.processed_objects.len(), 1);
        assert!(checkpoint.processed_objects.contains("bucket/a"));

        checkpoint.complete_page(2, 2000);
        assert_eq!(checkpoint.current_bucket_index, 2);
        assert_eq!(checkpoint.current_object_index, 2000);
        assert!(checkpoint.processed_objects.is_empty());
        assert!(checkpoint.skipped_objects.is_empty());
        assert!(checkpoint.failed_objects.is_empty());
    }

    #[test]
    fn test_checkpoint_loads_legacy_vec_format() {
        // Checkpoints written before the HashSet migration stored the object
        // lists as JSON arrays (possibly with duplicates); they must still load.
        let legacy = r#"{
            "task_id": "t1",
            "checkpoint_time": 1700000000,
            "current_bucket_index": 1,
            "current_object_index": 42,
            "processed_objects": ["a", "b", "a"],
            "failed_objects": [],
            "skipped_objects": ["c"]
        }"#;
        let checkpoint: ResumeCheckpoint = serde_json::from_str(legacy).unwrap();
        assert_eq!(checkpoint.current_object_index, 42);
        assert_eq!(checkpoint.processed_objects.len(), 2);
        assert!(checkpoint.processed_objects.contains("a"));
        assert!(checkpoint.skipped_objects.contains("c"));
    }

    #[test]
    fn test_compose_key_injective_with_adversarial_keys() {
        // Length-prefixing must keep the encoding injective even when keys
        // contain the delimiter, embedded nulls, or look like a composed key.
        assert_ne!(compose_key("a\0b", None), compose_key("a", Some("b")));
        assert_ne!(compose_key("3:xy", None), compose_key("x", Some("y")));
        assert_ne!(compose_key("a:b", None), compose_key("a", Some("b")));
        assert_ne!(compose_key("", Some("x")), compose_key("x", None));
        // Identical inputs must produce identical keys (stable identity).
        assert_eq!(compose_key("obj", Some("v1")), compose_key("obj", Some("v1")));
    }

    #[test]
    fn test_composite_key_dedup_distinguishes_versions() {
        // Two versions of the same object must be distinct dedup identities, and
        // the delete-marker/nil (None) version must not collide with a real one.
        let mut checkpoint = ResumeCheckpoint::new("task".to_string());
        checkpoint.add_processed_object(compose_key("obj", Some("v1")));
        checkpoint.add_processed_object(compose_key("obj", Some("v2")));
        checkpoint.add_processed_object(compose_key("obj", None));
        assert_eq!(checkpoint.processed_objects.len(), 3);
        assert!(checkpoint.processed_objects.contains(&compose_key("obj", Some("v1"))));
        assert!(checkpoint.processed_objects.contains(&compose_key("obj", Some("v2"))));
        assert!(checkpoint.processed_objects.contains(&compose_key("obj", None)));
        // A different object with the same version id is still distinct.
        assert!(!checkpoint.processed_objects.contains(&compose_key("other", Some("v1"))));
    }

    #[tokio::test]
    async fn test_resumestate_schema_v0_discarded_on_load() {
        let (temp_dir, disk) = schema_test_disk().await;

        // Legacy snapshot: no schema_version, a stale positional cursor and progress.
        let legacy = r#"{
            "task_id": "old-task",
            "task_type": "erasure_set",
            "set_disk_id": "pool_0_set_0",
            "start_time": 1700000000,
            "last_update": 1700000000,
            "completed": true,
            "total_objects": 100,
            "processed_objects": 50,
            "successful_objects": 40,
            "failed_objects": 10,
            "skipped_objects": 0,
            "current_bucket": null,
            "current_object": null,
            "completed_buckets": ["b1"],
            "pending_buckets": [],
            "error_message": null,
            "retry_count": 1,
            "max_retries": 3,
            "resume_cursor": "v1:stale-token"
        }"#;
        let task_id = "00000000-0000-4000-8000-000000000001";
        let legacy = legacy.replace("old-task", task_id);
        let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &file_path, legacy.as_bytes().to_vec().into())
            .await
            .expect("write legacy resume state");

        let manager = ResumeManager::load_from_disk(disk.clone(), task_id).await.unwrap();
        let state = manager.get_state().await;
        assert_eq!(state.schema_version, CURRENT_RESUME_SCHEMA, "schema must be stamped current");
        assert_eq!(state.resume_cursor, None, "stale cursor must be cleared");
        assert_eq!(state.processed_objects, 0);
        assert_eq!(state.successful_objects, 0);
        assert_eq!(state.failed_objects, 0);
        assert!(!state.completed);
        temp_dir.close().expect("remove schema test directory");
    }

    #[tokio::test]
    async fn test_checkpoint_schema_v4_discarded_on_load() {
        let (temp_dir, disk) = schema_test_disk().await;

        // The previous checkpoint schema is unsafe once its paired resume
        // state is discarded: retaining either position would skip work.
        let task_id = "00000000-0000-4000-8000-000000000002";
        let legacy = r#"{
            "schema_version": 4,
            "task_id": "00000000-0000-4000-8000-000000000002",
            "checkpoint_time": 1700000000,
            "current_bucket_index": 2,
            "current_object_index": 500,
            "processed_objects": ["a", "b"],
            "failed_objects": ["c"],
            "skipped_objects": ["d"]
        }"#;
        let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &file_path, legacy.as_bytes().to_vec().into())
            .await
            .expect("write legacy checkpoint");

        let manager = CheckpointManager::load_from_disk(disk.clone(), task_id).await.unwrap();
        let checkpoint = manager.get_checkpoint().await;
        assert_eq!(checkpoint.schema_version, CURRENT_CHECKPOINT_SCHEMA, "schema must be stamped current");
        assert_eq!(checkpoint.current_bucket_index, 0, "stale bucket position must be reset");
        assert_eq!(checkpoint.current_object_index, 0, "stale position must be reset");
        assert!(checkpoint.processed_objects.is_empty());
        assert!(checkpoint.failed_objects.is_empty());
        assert!(checkpoint.skipped_objects.is_empty());
        temp_dir.close().expect("remove schema test directory");
    }

    #[tokio::test]
    async fn current_normal_resume_schema_preserves_progress() {
        let (temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let mut state = ResumeState::new(
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket-b".to_string()],
        );
        state.resume_cursor = Some("opaque-marker".to_string());
        state.processed_objects = 7;
        state.successful_objects = 6;
        state.failed_objects = 1;
        state.completed_buckets = vec!["bucket-a".to_string()];
        let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
        let state_data = serde_json::to_vec(&state).expect("serialize current normal resume state");
        disk.write_all(RUSTFS_META_BUCKET, &file_path, state_data.into())
            .await
            .expect("write current normal resume state");

        let restored = ResumeManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("load current normal resume state")
            .get_state()
            .await;

        assert_eq!(restored.schema_version, CURRENT_RESUME_SCHEMA);
        assert_eq!(restored.resume_cursor.as_deref(), Some("opaque-marker"));
        assert_eq!(restored.processed_objects, 7);
        assert_eq!(restored.successful_objects, 6);
        assert_eq!(restored.failed_objects, 1);
        assert_eq!(restored.completed_buckets, ["bucket-a"]);
        assert!(restored.replacement_targets.is_empty());
        assert_eq!(restored.replacement_generation, None);
        assert_eq!(restored.replacement_phase, ReplacementPhase::None);
        temp_dir.close().expect("remove schema test directory");
    }

    #[tokio::test]
    async fn future_resume_and_checkpoint_schemas_are_rejected() {
        let (temp_dir, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let mut state = ResumeState::new(task_id.clone(), "erasure_set".to_string(), "pool_0_set_0".to_string(), Vec::new());
        state.schema_version = CURRENT_RESUME_SCHEMA + 1;
        let state_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
        let state_data = serde_json::to_vec(&state).expect("serialize future resume state");
        disk.write_all(RUSTFS_META_BUCKET, &state_path, state_data.into())
            .await
            .expect("write future resume state");

        let resume_error = match ResumeManager::load_from_disk(disk.clone(), &task_id).await {
            Ok(_) => panic!("future resume schema must not load"),
            Err(error) => error,
        };
        assert!(matches!(resume_error, Error::TaskExecutionFailed { .. }));
        assert!(resume_error.to_string().contains("newer than supported schema"));

        let mut checkpoint = ResumeCheckpoint::new(task_id.clone());
        checkpoint.schema_version = CURRENT_CHECKPOINT_SCHEMA + 1;
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        let checkpoint_data = serde_json::to_vec(&checkpoint).expect("serialize future checkpoint");
        disk.write_all(RUSTFS_META_BUCKET, &checkpoint_path, checkpoint_data.into())
            .await
            .expect("write future checkpoint");

        let checkpoint_error = match CheckpointManager::load_from_disk(disk.clone(), &task_id).await {
            Ok(_) => panic!("future checkpoint schema must not load"),
            Err(error) => error,
        };
        assert!(matches!(checkpoint_error, Error::TaskExecutionFailed { .. }));
        assert!(checkpoint_error.to_string().contains("newer than supported schema"));
        temp_dir.close().expect("remove schema test directory");
    }

    #[test]
    fn test_persist_throttle_batches_until_threshold() {
        let mut throttle = PersistThrottle::new();
        for _ in 0..PERSIST_EVERY_MUTATIONS - 1 {
            assert!(!throttle.record(), "must not flush below the mutation threshold");
        }
        assert!(throttle.record(), "must flush at the mutation threshold");
        throttle.mark_saved();
        assert!(!throttle.record(), "counter must reset after a save");
    }

    #[tokio::test]
    async fn completion_persists_immediately_and_cleanup_propagates_delete_errors() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("create resume persistence test directory");
        let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create resume persistence test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("create metadata volume for resume persistence test: {err}"),
        }

        let task_id = ResumeUtils::generate_task_id();
        let manager = ResumeManager::new(
            disk.clone(),
            task_id.clone(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        )
        .await
        .expect("create resume manager");
        manager
            .update_progress(1, 1, 0, 0)
            .await
            .expect("buffer progress below the persistence threshold");
        manager.mark_completed().await.expect("persist completed resume state");

        let persisted = ResumeManager::load_from_disk(disk.clone(), &task_id)
            .await
            .expect("reload completed resume state")
            .get_state()
            .await;
        assert!(persisted.completed, "completion must be persisted without waiting for the throttle");
        assert_eq!(persisted.processed_objects, 1, "the completion write must include buffered progress");

        let state_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
        let failure = ResumeDeleteFailure::install(state_path, DiskError::DiskAccessDenied);
        let error = manager
            .cleanup()
            .await
            .expect_err("resume cleanup must propagate a real delete failure");
        assert!(matches!(error, Error::Disk(DiskError::DiskAccessDenied)));
        drop(failure);
        manager.cleanup().await.expect("resume cleanup must be retryable");
        manager
            .cleanup()
            .await
            .expect("missing resume files must be idempotent success");

        let checkpoint = CheckpointManager::new(disk.clone(), task_id.clone())
            .await
            .expect("create checkpoint manager");
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        let failure = ResumeDeleteFailure::install(checkpoint_path, DiskError::DiskAccessDenied);
        let error = checkpoint
            .cleanup()
            .await
            .expect_err("checkpoint cleanup must propagate a real delete failure");
        assert!(matches!(error, Error::Disk(DiskError::DiskAccessDenied)));
        drop(failure);
        checkpoint.cleanup().await.expect("checkpoint cleanup must be retryable");
        checkpoint
            .cleanup()
            .await
            .expect("missing checkpoint must be idempotent success");
    }

    #[tokio::test]
    async fn checkpoint_rejects_a_task_id_mismatched_to_its_file_name() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("create checkpoint binding test directory");
        let endpoint =
            Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create checkpoint binding test endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create checkpoint binding test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(err) => panic!("create checkpoint binding metadata volume: {err}"),
        }

        let requested_task_id = ResumeUtils::generate_task_id();
        let checkpoint = ResumeCheckpoint::new(ResumeUtils::generate_task_id());
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{requested_task_id}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(
            RUSTFS_META_BUCKET,
            &checkpoint_path,
            serde_json::to_vec(&checkpoint)
                .expect("serialize mismatched checkpoint")
                .into(),
        )
        .await
        .expect("persist mismatched checkpoint");

        let error = match CheckpointManager::load_from_disk(disk, &requested_task_id).await {
            Ok(_) => panic!("checkpoint task id must be bound to its file name"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("does not match"));
        temp_dir.close().expect("remove checkpoint binding test directory");
    }

    #[tokio::test]
    async fn test_resume_utils() {
        let task_id1 = ResumeUtils::generate_task_id();
        let task_id2 = ResumeUtils::generate_task_id();

        assert_ne!(task_id1, task_id2);
        assert_eq!(task_id1.len(), 36); // UUID length
        assert_eq!(task_id2.len(), 36);
        assert!(validate_resume_task_id(&task_id1).is_ok());
        assert!(validate_resume_task_id(&format!("pool_0_set_0_{task_id1}")).is_err());
        assert!(validate_resume_task_id(&task_id1.to_uppercase()).is_err());
    }

    #[tokio::test]
    async fn test_get_resumable_tasks_integration() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let disk_path = temp_dir.path().join("test_disk");
        std::fs::create_dir_all(&disk_path).unwrap();

        // Create a local disk for testing
        let endpoint = Endpoint::try_from(disk_path.to_string_lossy().as_ref()).unwrap();
        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };
        let disk = new_disk(&endpoint, &disk_option).await.unwrap();

        // Create necessary directories first (ignore if already exist)
        let _ = disk.make_volume(RUSTFS_META_BUCKET).await;
        let _ = disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await;

        // Create some test resume state files
        let task_ids = vec![
            ResumeUtils::generate_task_id(),
            ResumeUtils::generate_task_id(),
            ResumeUtils::generate_task_id(),
        ];

        // Save resume state files for each task
        for task_id in &task_ids {
            let state = ResumeState::new(
                task_id.clone(),
                "erasure_set".to_string(),
                "pool_0_set_0".to_string(),
                vec!["bucket1".to_string(), "bucket2".to_string()],
            );

            let state_data = serde_json::to_vec(&state).unwrap();
            let file_path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");

            disk.write_all(RUSTFS_META_BUCKET, &file_path, state_data.into())
                .await
                .unwrap();
        }

        // Also create some non-resume state files to test filtering
        let non_resume_files = vec![
            "other_file.txt",
            "task4_ahm_checkpoint.json",
            "task5_ahm_progress.json",
            "_ahm_resume_state.json", // Invalid: empty task ID
            "not-a-uuid_ahm_resume_state.json",
            "00000000-0000-4000-8000-000000000001_extra_ahm_resume_state.json",
        ];

        for file_name in non_resume_files {
            let file_path = format!("{BUCKET_META_PREFIX}/{file_name}");
            disk.write_all(RUSTFS_META_BUCKET, &file_path, b"test data".to_vec().into())
                .await
                .unwrap();
        }

        // Now call get_resumable_tasks to see if it finds the correct files
        let found_task_ids = ResumeUtils::get_resumable_tasks(&disk).await.unwrap();

        // Verify that only the valid resume state files are found
        assert_eq!(found_task_ids.len(), 3);
        for task_id in &task_ids {
            assert!(found_task_ids.contains(task_id), "Task ID {task_id} not found");
        }

        // Verify that invalid files are not included
        assert!(!found_task_ids.contains(&"".to_string()));
        assert!(!found_task_ids.contains(&"task4".to_string()));
        assert!(!found_task_ids.contains(&"task5".to_string()));
        assert!(!found_task_ids.contains(&"not-a-uuid".to_string()));

        let error = match ResumeManager::load_from_disk(disk.clone(), "../not-a-uuid").await {
            Ok(_) => panic!("a traversal-like task id must be rejected before reading metadata"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::TaskExecutionFailed { message } if message == "Invalid resume task id"
        ));

        // Clean up
        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn resume_state_rejects_filename_and_json_task_id_mismatch() {
        use super::super::{DiskOption, Endpoint, new_disk};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().expect("create resume mismatch test directory");
        let endpoint = Endpoint::try_from(temp_dir.path().to_string_lossy().as_ref()).expect("create test disk endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("create resume mismatch test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("create metadata volume for resume mismatch test: {error}"),
        }

        let filename_task_id = ResumeUtils::generate_task_id();
        let state = ResumeState::new(
            ResumeUtils::generate_task_id(),
            "erasure_set".to_string(),
            "pool_0_set_0".to_string(),
            vec!["bucket".to_string()],
        );
        let path = format!("{BUCKET_META_PREFIX}/{filename_task_id}_{RESUME_STATE_FILE}");
        disk.write_all(
            RUSTFS_META_BUCKET,
            &path,
            serde_json::to_vec(&state).expect("serialize mismatched resume state").into(),
        )
        .await
        .expect("write mismatched resume state");

        let error = match ResumeManager::load_from_disk(disk.clone(), &filename_task_id).await {
            Ok(_) => panic!("resume state task id must match its filename"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::TaskExecutionFailed { message } if message == "Resume state task id does not match filename"
        ));

        let checkpoint_filename_task_id = ResumeUtils::generate_task_id();
        let checkpoint = ResumeCheckpoint::new(ResumeUtils::generate_task_id());
        let checkpoint_path = format!("{BUCKET_META_PREFIX}/{checkpoint_filename_task_id}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(
            RUSTFS_META_BUCKET,
            &checkpoint_path,
            serde_json::to_vec(&checkpoint)
                .expect("serialize mismatched resume checkpoint")
                .into(),
        )
        .await
        .expect("write mismatched resume checkpoint");

        let error = match CheckpointManager::load_from_disk(disk, &checkpoint_filename_task_id).await {
            Ok(_) => panic!("resume checkpoint task id must match its filename"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::TaskExecutionFailed { message } if message == "Resume checkpoint task id does not match filename"
        ));
    }
}
