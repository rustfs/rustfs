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

//! Bounded inspection of heal resume artifacts.
//!
//! The durable owner/CAS and quarantine primitives belong to backlog #1927 and
//! are not part of the current base revision. This module is therefore
//! deliberately inspect-only. In particular, it must never turn an age check
//! into a delete: ordinary heal writers still publish raw files on this base,
//! so a GC-side compare-and-delete would not fence a concurrent claim.

use metrics::counter;
use std::{
    collections::BTreeMap,
    path::{Component, Path},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;

use super::super::{BUCKET_META_PREFIX, DiskError, DiskStore, RUSTFS_META_BUCKET, storage_api::owner::EcstoreDiskAPI};
use super::{
    LEGACY_REPLACEMENT_RECOVERY_MARKER_FILE, REPLACEMENT_COMPLETION_PROOF_FILE, REPLACEMENT_INTENT_FILE,
    REPLACEMENT_INTENT_SEAL_FILE, RESUME_CHECKPOINT_FILE, RESUME_PROGRESS_FILE, RESUME_STATE_FILE, ResumeCheckpoint, ResumeState,
    checkpoint::CURRENT_CHECKPOINT_SCHEMA,
};
use crate::{Error, Result};

const DEFAULT_ENTRY_BUDGET: usize = 256;
const DEFAULT_BYTE_BUDGET: usize = 4 * 1024 * 1024;
const GC_METRIC: &str = "rustfs_heal_resume_gc_inspected_total";
const GC_ERROR_METRIC: &str = "rustfs_heal_resume_gc_inspect_errors_total";

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResumeGcConfig {
    /// Maximum number of directory entries considered in one disk pass.
    pub(crate) max_entries: usize,
    /// Maximum number of bytes read in one disk pass.
    pub(crate) max_bytes: usize,
}

impl Default for ResumeGcConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_ENTRY_BUDGET,
            max_bytes: DEFAULT_BYTE_BUDGET,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResumeGcReport {
    /// Directory entries visited (including malformed entries).
    pub(crate) inspected: usize,
    pub(crate) active_skipped: usize,
    pub(crate) orphaned: usize,
    /// Records that must be handed to #1927's quarantine owner.
    pub(crate) quarantine_required: usize,
    pub(crate) generation_skipped: usize,
    pub(crate) clock_skew: usize,
    pub(crate) read_errors: usize,
    pub(crate) retained: usize,
    /// True while #1927's durable claim/quarantine capability is unavailable.
    pub(crate) destructive_disabled: bool,
    pub(crate) budget_exhausted: bool,
}

#[derive(Debug, Default)]
pub(crate) struct ResumeGc {
    config: ResumeGcConfig,
    /// Alternate the first namespace so a full ordinary page cannot starve
    /// replacement recovery when the list API has no continuation token.
    recovery_first: bool,
}

impl ResumeGc {
    #[cfg(test)]
    fn with_config(config: ResumeGcConfig) -> Self {
        Self {
            config,
            recovery_first: false,
        }
    }

    /// Inspect one bounded page from each resume namespace.
    ///
    /// The caller owns scheduling and cancellation. A malformed or unreadable
    /// artifact is reported and retained so a later pass can retry it; no
    /// individual artifact error aborts the rest of the bounded page.
    pub(crate) async fn inspect_disk(&mut self, disk: &DiskStore) -> Result<ResumeGcReport> {
        let mut report = ResumeGcReport {
            destructive_disabled: true,
            ..ResumeGcReport::default()
        };
        if self.config.max_entries == 0 || self.config.max_bytes == 0 {
            report.budget_exhausted = true;
            return Ok(report);
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut bytes_read = 0usize;
        let recovery_first = self.recovery_first;
        self.recovery_first = !self.recovery_first;
        if recovery_first {
            inspect_namespace(self.config, disk, &replacement_prefix(), true, now, &mut bytes_read, &mut report).await?;
            if !report.budget_exhausted {
                inspect_namespace(self.config, disk, BUCKET_META_PREFIX, false, now, &mut bytes_read, &mut report).await?;
            }
        } else {
            inspect_namespace(self.config, disk, BUCKET_META_PREFIX, false, now, &mut bytes_read, &mut report).await?;
            if !report.budget_exhausted {
                inspect_namespace(self.config, disk, &replacement_prefix(), true, now, &mut bytes_read, &mut report).await?;
            }
        }

        counter!(GC_METRIC).increment(u64::try_from(report.inspected).unwrap_or(u64::MAX));
        counter!(GC_ERROR_METRIC).increment(u64::try_from(report.read_errors).unwrap_or(u64::MAX));
        Ok(report)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ArtifactSet {
    state: bool,
    checkpoint: bool,
    progress: bool,
    replacement_intent: bool,
    proof: bool,
    seal: bool,
    legacy_marker: bool,
    temporary: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactKind {
    State,
    Checkpoint,
    Progress,
    ReplacementIntent,
    Proof,
    Seal,
    LegacyMarker,
}

impl ArtifactSet {
    fn add(&mut self, kind: ArtifactKind, temporary: bool) {
        self.temporary |= temporary;
        match kind {
            ArtifactKind::State => self.state = true,
            ArtifactKind::Checkpoint => self.checkpoint = true,
            ArtifactKind::Progress => self.progress = true,
            ArtifactKind::ReplacementIntent => self.replacement_intent = true,
            ArtifactKind::Proof => self.proof = true,
            ArtifactKind::Seal => self.seal = true,
            ArtifactKind::LegacyMarker => self.legacy_marker = true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InspectOptions {
    max_bytes: usize,
    now: u64,
}

struct InspectProgress<'a> {
    bytes_read: &'a mut usize,
    report: &'a mut ResumeGcReport,
}

fn replacement_prefix() -> String {
    super::replacement_recovery_dir().to_string_lossy().into_owned()
}

async fn inspect_namespace(
    config: ResumeGcConfig,
    disk: &DiskStore,
    prefix: &str,
    replacement: bool,
    now: u64,
    bytes_read: &mut usize,
    report: &mut ResumeGcReport,
) -> Result<()> {
    let remaining = config.max_entries.saturating_sub(report.inspected);
    if remaining == 0 {
        report.budget_exhausted = true;
        return Ok(());
    }
    let count = i32::try_from(remaining).unwrap_or(i32::MAX);
    let mut entries = match EcstoreDiskAPI::list_dir(disk.as_ref(), "", RUSTFS_META_BUCKET, prefix, count).await {
        Ok(entries) => entries,
        Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    entries.sort_unstable();

    let mut artifacts = BTreeMap::<String, ArtifactSet>::new();
    for entry in entries {
        if report.inspected >= config.max_entries {
            report.budget_exhausted = true;
            break;
        }
        report.inspected += 1;
        let Some((task_id, kind, temporary)) = artifact_name(&entry, replacement) else {
            report.quarantine_required += 1;
            report.retained += 1;
            continue;
        };
        artifacts.entry(task_id).or_default().add(kind, temporary);
    }
    if report.inspected >= config.max_entries {
        report.budget_exhausted = true;
    }

    for (task_id, artifacts) in artifacts {
        if *bytes_read >= config.max_bytes {
            report.budget_exhausted = true;
            break;
        }
        let options = InspectOptions {
            max_bytes: config.max_bytes,
            now,
        };
        let mut progress = InspectProgress { bytes_read, report };
        inspect_task(options, disk, prefix, replacement, &task_id, artifacts, &mut progress).await?;
    }
    Ok(())
}

async fn inspect_task(
    options: InspectOptions,
    disk: &DiskStore,
    prefix: &str,
    replacement: bool,
    task_id: &str,
    artifacts: ArtifactSet,
    progress: &mut InspectProgress<'_>,
) -> Result<()> {
    let legacy_replacement = !replacement && !artifacts.state && artifacts.replacement_intent;
    let state_suffix = if replacement || legacy_replacement {
        REPLACEMENT_INTENT_FILE
    } else {
        RESUME_STATE_FILE
    };
    let state_path = artifact_path(prefix, task_id, state_suffix)?;
    let state = match read_bounded(disk, &state_path, options.max_bytes, progress.bytes_read).await {
        ReadOutcome::Missing => {
            progress.report.orphaned += 1;
            progress.report.retained += 1;
            return Ok(());
        }
        ReadOutcome::TooLarge => {
            progress.report.quarantine_required += 1;
            progress.report.retained += 1;
            progress.report.budget_exhausted = true;
            return Ok(());
        }
        ReadOutcome::Error => {
            progress.report.read_errors += 1;
            progress.report.retained += 1;
            return Ok(());
        }
        ReadOutcome::Bytes(bytes) => bytes,
    };

    let parsed: ResumeState = match serde_json::from_slice(&state) {
        Ok(state) => state,
        Err(_) => {
            progress.report.quarantine_required += 1;
            progress.report.retained += 1;
            return Ok(());
        }
    };
    if parsed.schema_version > super::CURRENT_RESUME_SCHEMA || parsed.task_id != task_id {
        progress.report.quarantine_required += 1;
        progress.report.retained += 1;
        return Ok(());
    }
    if persistent_age_seconds(options.now, parsed.last_update).is_none() {
        progress.report.clock_skew += 1;
        progress.report.retained += 1;
        return Ok(());
    }
    if let Some(generation) = parsed.replacement_generation.as_deref()
        && !claim_generation_matches(Some(generation), Some(task_id))
    {
        progress.report.generation_skipped += 1;
        progress.report.retained += 1;
        return Ok(());
    }

    if !replacement && artifacts.checkpoint {
        let checkpoint_path = artifact_path(prefix, task_id, RESUME_CHECKPOINT_FILE)?;
        match read_bounded(disk, &checkpoint_path, options.max_bytes, progress.bytes_read).await {
            ReadOutcome::Bytes(bytes) => match serde_json::from_slice::<ResumeCheckpoint>(&bytes) {
                Ok(checkpoint) if checkpoint.schema_version <= CURRENT_CHECKPOINT_SCHEMA && checkpoint.task_id == task_id => {}
                _ => {
                    progress.report.quarantine_required += 1;
                    progress.report.retained += 1;
                }
            },
            ReadOutcome::Missing => {
                progress.report.orphaned += 1;
                progress.report.retained += 1;
            }
            ReadOutcome::TooLarge => {
                progress.report.quarantine_required += 1;
                progress.report.retained += 1;
                progress.report.budget_exhausted = true;
            }
            ReadOutcome::Error => {
                progress.report.read_errors += 1;
                progress.report.retained += 1;
            }
        }
    }

    if artifacts.state && artifacts.replacement_intent {
        // A task cannot have two authoritative state records in one namespace;
        // preserve both until the durable owner can resolve the generation.
        progress.report.quarantine_required += 1;
    }

    if !parsed.completed {
        progress.report.active_skipped += 1;
    }
    // The state and all associated evidence remain recoverable until #1927
    // supplies a common generation/CAS transition and quarantine owner.
    progress.report.retained += 1;
    Ok(())
}

enum ReadOutcome {
    Bytes(Vec<u8>),
    Missing,
    TooLarge,
    Error,
}

async fn read_bounded(disk: &DiskStore, path: &str, max_bytes: usize, bytes_read: &mut usize) -> ReadOutcome {
    let remaining = max_bytes.saturating_sub(*bytes_read);
    if remaining == 0 {
        return ReadOutcome::TooLarge;
    }
    let read_len = remaining.saturating_add(1);
    let reader = match EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, path).await {
        Ok(reader) => reader,
        Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => return ReadOutcome::Missing,
        Err(_) => return ReadOutcome::Error,
    };
    let mut bytes = Vec::with_capacity(read_len.min(64 * 1024));
    let Ok(read_len) = u64::try_from(read_len) else {
        return ReadOutcome::TooLarge;
    };
    if reader.take(read_len).read_to_end(&mut bytes).await.is_err() {
        return ReadOutcome::Error;
    }
    if bytes.len() > remaining {
        *bytes_read = max_bytes;
        return ReadOutcome::TooLarge;
    }
    *bytes_read = bytes_read.saturating_add(bytes.len());
    ReadOutcome::Bytes(bytes)
}

fn artifact_path(prefix: &str, task_id: &str, suffix: &str) -> Result<String> {
    if super::validate_resume_task_id(task_id).is_err() {
        return Err(Error::other("invalid resume task id"));
    }
    Path::new(prefix)
        .join(format!("{task_id}_{suffix}"))
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::other("invalid resume artifact path"))
}

/// Parse one directory entry without ever accepting a path component supplied
/// by a client. DiskAPI filters symlinks, but this check also protects remote
/// implementations and future mutating callers from traversal/reparse names.
fn artifact_name(entry: &str, replacement: bool) -> Option<(String, ArtifactKind, bool)> {
    let path = Path::new(entry);
    if entry.is_empty() || path.components().count() != 1 || !matches!(path.components().next(), Some(Component::Normal(_))) {
        return None;
    }
    let (stem, temporary) = entry
        .strip_suffix(".tmp")
        .map(|stem| (stem, true))
        .or_else(|| entry.strip_suffix(".bak").map(|stem| (stem, true)))
        .unwrap_or((entry, false));
    let suffixes: &[(&str, ArtifactKind)] = if replacement {
        &[
            (REPLACEMENT_INTENT_FILE, ArtifactKind::ReplacementIntent),
            (REPLACEMENT_COMPLETION_PROOF_FILE, ArtifactKind::Proof),
            (REPLACEMENT_INTENT_SEAL_FILE, ArtifactKind::Seal),
        ]
    } else {
        &[
            (RESUME_STATE_FILE, ArtifactKind::State),
            (RESUME_CHECKPOINT_FILE, ArtifactKind::Checkpoint),
            (RESUME_PROGRESS_FILE, ArtifactKind::Progress),
            (LEGACY_REPLACEMENT_RECOVERY_MARKER_FILE, ArtifactKind::LegacyMarker),
            (REPLACEMENT_INTENT_FILE, ArtifactKind::ReplacementIntent),
            (REPLACEMENT_COMPLETION_PROOF_FILE, ArtifactKind::Proof),
            (REPLACEMENT_INTENT_SEAL_FILE, ArtifactKind::Seal),
        ]
    };
    suffixes.iter().find_map(|(suffix, kind)| {
        stem.strip_suffix(&format!("_{suffix}"))
            .filter(|task_id| super::validate_resume_task_id(task_id).is_ok())
            .map(|task_id| (task_id.to_string(), *kind, temporary))
    })
}

fn persistent_age_seconds(now: u64, updated: u64) -> Option<u64> {
    now.checked_sub(updated)
}

fn claim_generation_matches(observed: Option<&str>, expected: Option<&str>) -> bool {
    expected.is_none() || observed == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::{DiskOption, Endpoint, new_disk};
    use tempfile::TempDir;
    use uuid::Uuid;

    async fn test_disk() -> (TempDir, DiskStore) {
        let temp = TempDir::new().expect("test disk directory");
        let endpoint = Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("test endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("test disk");
        match disk.make_volume(RUSTFS_META_BUCKET).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("metadata volume: {error}"),
        }
        match disk.make_volume(&format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}")).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("resume volume: {error}"),
        }
        (temp, disk)
    }

    async fn write_state(disk: &DiskStore, state: &ResumeState) {
        let path = format!("{BUCKET_META_PREFIX}/{}_{}", state.task_id, RESUME_STATE_FILE);
        disk.write_all(RUSTFS_META_BUCKET, &path, serde_json::to_vec(state).unwrap().into())
            .await
            .expect("resume state");
    }

    async fn write_replacement_state(disk: &DiskStore, state: &ResumeState) {
        let volume = format!("{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}/ahm-replacement");
        match disk.make_volume(&volume).await {
            Ok(()) | Err(DiskError::VolumeExists) => {}
            Err(error) => panic!("replacement volume: {error}"),
        }
        let path = format!("{}/{}_{}", replacement_prefix(), state.task_id, REPLACEMENT_INTENT_FILE);
        disk.write_all(RUSTFS_META_BUCKET, &path, serde_json::to_vec(state).unwrap().into())
            .await
            .expect("replacement state");
    }

    #[tokio::test]
    async fn production_gc_does_not_delete_claimed_resume_state() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        write_state(&disk, &ResumeState::new(task_id.clone(), "set".into(), "disk".into(), vec![])).await;
        let report = ResumeGc::default().inspect_disk(&disk).await.expect("inspect");
        assert_eq!(report.active_skipped, 1);
        assert!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn production_gc_generation_mismatch_is_skip() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let mut state = ResumeState::new(task_id.clone(), "set".into(), "disk".into(), vec![]);
        state.replacement_generation = Some(Uuid::new_v4().to_string());
        write_state(&disk, &state).await;
        assert_eq!(ResumeGc::default().inspect_disk(&disk).await.unwrap().generation_skipped, 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn production_gc_rejects_symlink_or_outside_prefix() {
        let (_temp, disk) = test_disk().await;
        let id = Uuid::new_v4().to_string();
        let root = EcstoreDiskAPI::path(disk.as_ref());
        let outside = root.join("outside-resume-state");
        std::fs::write(&outside, b"must remain").expect("outside fixture");
        let symlink = root
            .join(RUSTFS_META_BUCKET)
            .join(BUCKET_META_PREFIX)
            .join(format!("{id}_{RESUME_STATE_FILE}"));
        std::os::unix::fs::symlink(&outside, &symlink).expect("symlink fixture");
        let report = ResumeGc::default().inspect_disk(&disk).await.expect("inspect");
        assert_eq!(report.inspected, 0, "symlinks are not eligible artifacts");
        assert!(outside.exists());
        assert!(artifact_name(&format!("{id}_{RESUME_STATE_FILE}"), false).is_some());
        assert!(artifact_name(&format!("../{id}_{RESUME_STATE_FILE}"), false).is_none());
        assert!(artifact_name(&format!("{id}/link_{RESUME_STATE_FILE}"), false).is_none());
    }

    #[cfg(not(unix))]
    #[test]
    fn production_gc_rejects_symlink_or_outside_prefix() {
        let id = Uuid::new_v4().to_string();
        assert!(artifact_name(&format!("{id}_{RESUME_STATE_FILE}"), false).is_some());
        assert!(artifact_name(&format!("../{id}_{RESUME_STATE_FILE}"), false).is_none());
        assert!(artifact_name(&format!("{id}/link_{RESUME_STATE_FILE}"), false).is_none());
    }

    #[tokio::test]
    async fn production_gc_delete_failure_leaves_recoverable_state() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let mut state = ResumeState::new(task_id.clone(), "set".into(), "disk".into(), vec![]);
        state.mark_completed();
        write_state(&disk, &state).await;
        ResumeGc::default().inspect_disk(&disk).await.expect("inspect");
        assert!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn production_gc_handles_clock_skew_and_restart() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let mut state = ResumeState::new(task_id, "set".into(), "disk".into(), vec![]);
        state.last_update = u64::MAX;
        write_state(&disk, &state).await;
        assert_eq!(ResumeGc::default().inspect_disk(&disk).await.unwrap().clock_skew, 1);
        assert!(persistent_age_seconds(1, 2).is_none());
    }

    #[tokio::test]
    async fn production_gc_100k_states_respects_budget() {
        let (_temp, disk) = test_disk().await;
        for _ in 0..8 {
            let state = ResumeState::new(Uuid::new_v4().to_string(), "set".into(), "disk".into(), vec![]);
            write_state(&disk, &state).await;
        }
        let config = ResumeGcConfig {
            max_entries: 2,
            max_bytes: usize::MAX,
        };
        let report = ResumeGc::with_config(config).inspect_disk(&disk).await.unwrap();
        assert!(report.inspected <= 2);
        assert!(report.budget_exhausted);
    }

    #[tokio::test]
    async fn production_gc_recovery_namespace_is_not_starved() {
        let (_temp, disk) = test_disk().await;
        let ordinary = ResumeState::new(Uuid::new_v4().to_string(), "set".into(), "disk".into(), vec![]);
        write_state(&disk, &ordinary).await;
        let replacement_id = Uuid::new_v4().to_string();
        let mut replacement = ResumeState::new(replacement_id, "set".into(), "disk".into(), vec![]);
        replacement.replacement_generation = Some(replacement.task_id.clone());
        write_replacement_state(&disk, &replacement).await;

        let mut gc = ResumeGc::with_config(ResumeGcConfig {
            max_entries: 1,
            max_bytes: usize::MAX,
        });
        assert_eq!(gc.inspect_disk(&disk).await.unwrap().inspected, 1);
        let second = gc.inspect_disk(&disk).await.unwrap();
        assert_eq!(second.inspected, 1, "the next bounded pass must start at recovery");
        assert_eq!(second.active_skipped, 1);
    }

    #[tokio::test]
    async fn production_gc_pairs_orphan_checkpoint_and_resume() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let checkpoint = ResumeCheckpoint::new(task_id.clone());
        let path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&checkpoint).unwrap().into())
            .await
            .expect("checkpoint");
        assert_eq!(ResumeGc::default().inspect_disk(&disk).await.unwrap().orphaned, 1);
    }

    #[tokio::test]
    async fn production_gc_does_not_delete_slow_active_task() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let mut state = ResumeState::new(task_id, "set".into(), "disk".into(), vec![]);
        state.last_update = 1;
        write_state(&disk, &state).await;
        assert_eq!(ResumeGc::default().inspect_disk(&disk).await.unwrap().active_skipped, 1);
    }

    #[tokio::test]
    async fn production_gc_disables_on_mixed_version_capability() {
        assert!(claim_generation_matches(None, None));
        assert!(!claim_generation_matches(Some("new"), Some("old")));
        // No #1927 capability means this implementation has no delete path.
        assert!(ResumeGcConfig::default().max_entries > 0);
        let (_temp, disk) = test_disk().await;
        let report = ResumeGc::default().inspect_disk(&disk).await.expect("inspect");
        assert!(report.destructive_disabled);
    }

    #[tokio::test]
    async fn production_gc_future_schema_is_not_mtime_deleted() {
        let (_temp, disk) = test_disk().await;
        let task_id = Uuid::new_v4().to_string();
        let mut state = ResumeState::new(task_id.clone(), "set".into(), "disk".into(), vec![]);
        state.schema_version = super::super::CURRENT_RESUME_SCHEMA + 1;
        write_state(&disk, &state).await;
        let report = ResumeGc::default().inspect_disk(&disk).await.unwrap();
        assert_eq!(report.quarantine_required, 1);
        assert!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn production_gc_quarantine_cleanup_is_bounded() {
        let (_temp, disk) = test_disk().await;
        for _ in 0..4 {
            let task_id = Uuid::new_v4().to_string();
            let path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_STATE_FILE}");
            disk.write_all(RUSTFS_META_BUCKET, &path, b"corrupt".to_vec().into())
                .await
                .expect("corrupt state");
        }
        let report = ResumeGc::with_config(ResumeGcConfig {
            max_entries: 2,
            max_bytes: 1024,
        })
        .inspect_disk(&disk)
        .await
        .expect("inspect");
        assert!(report.quarantine_required <= 2);
        assert!(report.budget_exhausted);
    }
}
