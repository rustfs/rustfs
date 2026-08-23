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
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    BUCKET_META_PREFIX, DiskError, DiskStore, HealDiskExt as _, RUSTFS_META_BUCKET, storage_api::owner::EcstoreDiskBytes,
};

mod checkpoint;
mod gc;
mod replacement;
mod utils;

pub use checkpoint::{CheckpointManager, CheckpointObjectOutcome, CheckpointObjectOutcomeRecord, ResumeCheckpoint};
pub(crate) use gc::ResumeGc;
pub(crate) use replacement::replacement_target_identities_match;
use replacement::replacement_targets_match_identities;
pub use replacement::{
    ReplacementPhase, ReplacementRecoveryRecord, ReplacementRecoveryState, ReplacementTargetIdentity, compose_key,
};
pub use utils::ResumeUtils;

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_RESUME: &str = "resume";
const EVENT_HEAL_RESUME_STATE: &str = "heal_resume_state";

/// resume state file constants
const RESUME_STATE_FILE: &str = "ahm_resume_state.json";
// Replacement intents must not use the ordinary resume-state suffix. Older
// binaries enumerate that suffix and can otherwise resume a replacement as a
// normal heal without its identity and format fences.
const REPLACEMENT_INTENT_FILE: &str = "ahm_replacement_intent.json";
const RESUME_PROGRESS_FILE: &str = "ahm_progress.json";
pub(super) const RESUME_CHECKPOINT_FILE: &str = "ahm_checkpoint.json";
pub(super) const RESUME_CHECKPOINT_BLOCKED_FILE: &str = "ahm_checkpoint.blocked";
const REPLACEMENT_COMPLETION_PROOF_FILE: &str = "ahm_replacement_completion_proof.json";
const REPLACEMENT_RECOVERY_DIR: &str = "ahm-replacement";
const REPLACEMENT_INTENT_SEAL_FILE: &str = "ahm_replacement_intent_seal";
const LEGACY_REPLACEMENT_RECOVERY_MARKER_FILE: &str = "ahm_replacement_recovery.json";
const REPLACEMENT_RECOVERY_CONFLICT_PREFIX: &str = "replacement recovery conflict:";
const REPLACEMENT_RECOVERY_CORRUPTION_PREFIX: &str = "replacement recovery corruption:";

/// Current on-disk schema version for `ResumeState`. Snapshots written by an
/// older schema (which tracked latest-only object names and a positional
/// cursor) are incompatible with the per-version resume cursor, so they are
/// discarded on load and the scan restarts from the beginning.
const CURRENT_RESUME_SCHEMA: u32 = 5;

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

fn replacement_recovery_conflict(message: impl std::fmt::Display) -> Error {
    Error::TaskExecutionFailed {
        message: format!("{REPLACEMENT_RECOVERY_CONFLICT_PREFIX} {message}"),
    }
}

fn replacement_recovery_corruption(message: impl std::fmt::Display) -> Error {
    Error::TaskExecutionFailed {
        message: format!("{REPLACEMENT_RECOVERY_CORRUPTION_PREFIX} {message}"),
    }
}

pub(crate) fn replacement_recovery_error_requires_block(error: &Error) -> bool {
    matches!(
        error,
        Error::TaskExecutionFailed { message }
            if message.starts_with(REPLACEMENT_RECOVERY_CONFLICT_PREFIX)
                || message.starts_with(REPLACEMENT_RECOVERY_CORRUPTION_PREFIX)
    )
}

fn replacement_recovery_corruption_for_state_load(message: impl std::fmt::Display, error: Error) -> Error {
    if replacement_recovery_error_requires_block(&error) {
        error
    } else if matches!(
        error,
        Error::TaskExecutionFailed { .. } | Error::Serialization(_) | Error::InvalidCheckpoint(_)
    ) {
        replacement_recovery_corruption(format!("{message}: {error}"))
    } else {
        error
    }
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
    /// Terminal versions skipped because they were newer than the heal start.
    #[serde(default)]
    pub skipped_new_versions: u64,
    /// Terminal versions handed to lifecycle expiry.
    #[serde(default)]
    pub skipped_ilm_expired: u64,
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
    /// Bytes accounted by the object ledger; additive for old snapshots.
    #[serde(default)]
    pub processed_bytes: u64,
    /// Total bytes from a complete usage snapshot, when available.
    #[serde(default)]
    pub total_bytes: u64,
    /// Generation of the usage snapshot used for the baseline.
    #[serde(default)]
    pub baseline_generation: Option<u64>,
    /// Whether the usage baseline is known. Missing in old snapshots means
    /// indeterminate rather than a measured zero baseline.
    #[serde(default)]
    pub baseline_known: bool,
    /// Persistent telemetry fence for counter/byte overflow or corruption.
    /// It must survive a restart so a saturated snapshot is never presented as
    /// a measured percentage on the next resume.
    #[serde(default)]
    pub counter_unknown: bool,
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
            skipped_new_versions: 0,
            skipped_ilm_expired: 0,
            current_bucket: None,
            current_object: None,
            completed_buckets: Vec::new(),
            pending_buckets: buckets,
            error_message: None,
            retry_count: 0,
            max_retries: 3,
            processed_bytes: 0,
            total_bytes: 0,
            baseline_generation: None,
            baseline_known: false,
            counter_unknown: false,
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

    pub fn update_progress_with_bytes(
        &mut self,
        processed: u64,
        successful: u64,
        failed: u64,
        skipped: u64,
        processed_bytes: u64,
    ) {
        self.update_progress(processed, successful, failed, skipped);
        self.processed_bytes = processed_bytes;
    }

    pub fn set_skipped_version_counts(&mut self, new_versions: u64, ilm_expired: u64) {
        self.skipped_new_versions = new_versions;
        self.skipped_ilm_expired = ilm_expired;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn set_progress_baseline(&mut self, total_objects: u64, total_bytes: u64, generation: Option<u64>) {
        self.total_objects = total_objects;
        self.total_bytes = total_bytes;
        self.baseline_generation = generation;
        // This method is called only after a complete usage snapshot has been
        // validated.  A complete but empty snapshot is still a known baseline.
        self.baseline_known = true;
        self.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    }

    pub fn mark_counter_unknown(&mut self) {
        self.counter_unknown = true;
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
        self.resume_cursor = None;
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
        self.skipped_new_versions = 0;
        self.skipped_ilm_expired = 0;
        self.processed_bytes = 0;
        self.counter_unknown = false;
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
        if self.completed {
            return 100.0;
        }
        if self.counter_unknown {
            return 0.0;
        }
        if !self.baseline_known {
            return 0.0;
        }
        if self.total_bytes > 0 {
            return ((self.processed_bytes as f64 / self.total_bytes as f64) * 100.0).min(99.999);
        }
        if self.total_objects == 0 {
            return 0.0;
        }
        ((self.processed_objects as f64 / self.total_objects as f64) * 100.0).min(99.999)
    }

    pub fn get_success_rate(&self) -> f64 {
        let Some(total) = self.successful_objects.checked_add(self.failed_objects) else {
            return 0.0;
        };
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
                let legacy = Self::load_from_disk_at(disk.clone(), task_id, legacy_file)
                    .await
                    .map_err(|error| {
                        replacement_recovery_corruption_for_state_load(
                            format!("Failed to load legacy replacement state {task_id}"),
                            error,
                        )
                    })?;
                let legacy_state = legacy.get_state().await;
                if !is_replacement_intent(&legacy_state) || legacy_state != isolated.get_state().await {
                    return Err(replacement_recovery_conflict(format!(
                        "Replacement intent has conflicting legacy state for task {task_id}"
                    )));
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
        let legacy = Self::load_from_disk_at(disk.clone(), task_id, legacy_file)
            .await
            .map_err(|error| {
                replacement_recovery_corruption_for_state_load(
                    format!("Failed to load legacy replacement state {task_id}"),
                    error,
                )
            })?;
        if !is_replacement_intent(&legacy.get_state().await) {
            return Err(replacement_recovery_corruption(format!(
                "Resume state is not a replacement intent for task {task_id}"
            )));
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
            state.skipped_new_versions = 0;
            state.skipped_ilm_expired = 0;
            state.processed_bytes = 0;
            state.total_objects = 0;
            state.total_bytes = 0;
            state.baseline_generation = None;
            state.baseline_known = false;
            state.counter_unknown = false;
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

    pub async fn update_progress_with_bytes(
        &self,
        processed: u64,
        successful: u64,
        failed: u64,
        skipped: u64,
        processed_bytes: u64,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        state.update_progress_with_bytes(processed, successful, failed, skipped, processed_bytes);
        drop(state);
        self.save_state_throttled().await
    }

    pub async fn set_progress_baseline(&self, total_objects: u64, total_bytes: u64, generation: Option<u64>) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_progress_baseline(total_objects, total_bytes, generation);
        drop(state);
        self.save_state_throttled().await
    }

    pub async fn mark_counter_unknown(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.mark_counter_unknown();
        drop(state);
        self.save_state().await
    }

    pub async fn set_skipped_version_counts(&self, new_versions: u64, ilm_expired: u64) -> Result<()> {
        let mut state = self.state.write().await;
        state.set_skipped_version_counts(new_versions, ilm_expired);
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
        self.save_state().await
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

    /// read state file from disk
    async fn read_state_file(disk: &DiskStore, task_id: &str, state_file: ResumeStateFile) -> Result<Vec<u8>> {
        validate_resume_task_id(task_id)?;
        let file_path = state_file.path(task_id);

        let path_str = path_to_str(&file_path)?;
        disk.read_all(RUSTFS_META_BUCKET, path_str)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(Error::Disk)
    }
}

#[cfg(test)]
mod tests;
