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

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

pub(crate) fn stable_generation(parts: &[&[u8]]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in (part.len() as u64).to_be_bytes().into_iter().chain(part.iter().copied()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

#[cfg(test)]
mod stable_generation_tests {
    use super::stable_generation;

    #[test]
    fn stable_generation_has_a_fixed_vector() {
        assert_eq!(stable_generation(&[b"rustfs", b"heal", b"42"]), 11_007_672_338_488_385_056);
    }
}

pub(crate) fn increment_counter(counter: &mut u64) -> bool {
    match counter.checked_add(1) {
        Some(next) => {
            *counter = next;
            true
        }
        None => {
            *counter = u64::MAX;
            false
        }
    }
}

pub(crate) fn add_bytes(total: &mut u64, amount: u64) -> bool {
    match total.checked_add(amount) {
        Some(next) => {
            *total = next;
            true
        }
        None => {
            *total = u64::MAX;
            false
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HealProgressKind {
    #[default]
    Unknown,
    Stage,
    ObjectSweep,
}

/// Whether the object ledger can produce a meaningful percentage.
///
/// A zero-valued baseline is not a completed scan: it means that no complete
/// usage snapshot was available.  Keep this state explicit so callers do not
/// mistake the legacy `0.0` wire value for a measured zero-percent result.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HealProgressState {
    #[default]
    Unknown,
    Indeterminate,
    Running,
    Completed,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HealProgress {
    #[serde(default)]
    pub kind: HealProgressKind,
    /// Objects scanned
    pub objects_scanned: u64,
    /// Objects healed
    pub objects_healed: u64,
    /// Objects failed
    pub objects_failed: u64,
    /// Versions deferred for a later retry pass.
    #[serde(default)]
    pub skipped_objects: u64,
    /// Versions skipped because they were written after this heal started
    pub skipped_new_versions: u64,
    /// Versions skipped because lifecycle already selected them for expiry
    pub skipped_ilm_expired: u64,
    /// Baseline object count from the latest complete usage snapshot
    pub objects_total_count: u64,
    /// Baseline object bytes from the latest complete usage snapshot
    pub objects_total_size: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Current object
    pub current_object: Option<String>,
    /// Progress percentage
    pub progress_percentage: f64,
    /// Start time
    pub start_time: Option<SystemTime>,
    /// Last update time
    pub last_update_time: Option<SystemTime>,
    /// Estimated completion time
    pub estimated_completion_time: Option<SystemTime>,
    /// Current stage number. Stage updates are intentionally independent from
    /// the object ledger below.
    #[serde(default)]
    pub stage_current: u64,
    /// Number of stages in the current task.
    #[serde(default)]
    pub stage_total: u64,
    /// Explicitly distinguishes a missing usage baseline from measured 0%.
    #[serde(default)]
    pub progress_state: HealProgressState,
    /// True only after the task's durable completion ledger was committed.
    #[serde(default)]
    pub ledger_complete: bool,
    /// Generation of the usage snapshot used for the baseline, if available.
    #[serde(default)]
    pub baseline_generation: Option<u64>,
    /// Whether the baseline was explicitly observed.  This is separate from
    /// the counters so a known empty scope (0 objects, 0 bytes) is not
    /// confused with a legacy snapshot that omitted the baseline fields.
    #[serde(default)]
    pub baseline_known: bool,
    /// Internal telemetry fence set when an aggregate counter overflows or
    /// becomes inconsistent.  It prevents a later refresh from fabricating a
    /// percentage from the poisoned values.
    #[serde(default)]
    pub counter_unknown: bool,
}

impl HealProgress {
    pub fn new() -> Self {
        Self {
            kind: HealProgressKind::Unknown,
            start_time: Some(SystemTime::now()),
            last_update_time: Some(SystemTime::now()),
            ..Default::default()
        }
    }

    pub fn update_progress(&mut self, scanned: u64, healed: u64, failed: u64, bytes: u64) {
        self.update_object_sweep_progress(scanned, healed, failed, bytes);
    }

    pub fn update_object_sweep_progress(&mut self, scanned: u64, healed: u64, failed: u64, bytes: u64) {
        self.kind = HealProgressKind::ObjectSweep;
        self.objects_scanned = scanned;
        self.objects_healed = healed;
        self.objects_failed = failed;
        self.bytes_processed = bytes;
        self.last_update_time = Some(SystemTime::now());

        let explicit_skipped = match self.skipped_new_versions.checked_add(self.skipped_ilm_expired) {
            Some(value) => value,
            None => {
                self.mark_unknown();
                0
            }
        };
        let skipped = healed
            .checked_add(failed)
            .and_then(|value| value.checked_add(explicit_skipped))
            .and_then(|value| scanned.checked_sub(value))
            .unwrap_or(0);
        self.update_object_progress(scanned, healed, failed, skipped, bytes);
    }

    /// Update task stage progress without modifying object counters.
    pub fn update_stage(&mut self, current: u64, total: u64) {
        let object_sweep_active = matches!(self.kind, HealProgressKind::ObjectSweep);
        if !object_sweep_active {
            self.kind = HealProgressKind::Stage;
        }
        self.ledger_complete = false;
        self.stage_current = current.min(total);
        self.stage_total = total;
        if object_sweep_active {
            self.last_update_time = Some(SystemTime::now());
            self.refresh_progress_percentage();
            return;
        }
        self.progress_state = if total == 0 {
            HealProgressState::Indeterminate
        } else {
            HealProgressState::Running
        };
        self.progress_percentage = if total == 0 {
            0.0
        } else {
            (current as f64 / total as f64 * 100.0).min(100.0)
        };
        self.last_update_time = Some(SystemTime::now());
    }

    /// Update the disjoint object ledger. `scanned` is the number of terminal
    /// object outcomes and must equal healed + failed + deferred skipped plus
    /// the two terminal skip classes. Overflow is a corrupt/unknown counter
    /// state, not a reason to abort a completed heal.
    pub fn update_object_progress(&mut self, scanned: u64, healed: u64, failed: u64, skipped: u64, bytes: u64) {
        self.kind = HealProgressKind::ObjectSweep;
        // `skipped` is the transient/deferred class.  The two explicit skip
        // counters are terminal classifications too, so include them in the
        // same ledger without making callers maintain a second aggregate.
        let outcomes = healed
            .checked_add(failed)
            .and_then(|value| value.checked_add(skipped))
            .and_then(|value| value.checked_add(self.skipped_new_versions))
            .and_then(|value| value.checked_add(self.skipped_ilm_expired));
        self.objects_scanned = scanned;
        self.objects_healed = healed;
        self.objects_failed = failed;
        self.skipped_objects = skipped;
        self.bytes_processed = bytes;
        self.last_update_time = Some(SystemTime::now());
        self.ledger_complete = false;
        if outcomes != Some(scanned) {
            // Telemetry corruption must not abort a heal.  Preserve the
            // counters for diagnostics, but do not derive a percentage from a
            // double-counted or overflowing ledger.
            self.mark_unknown();
            return;
        }
        self.refresh_progress_percentage();
        self.refresh_estimated_completion_time();
    }

    pub fn set_total_baseline(&mut self, objects_total_count: u64, objects_total_size: u64) {
        self.objects_total_count = objects_total_count;
        self.objects_total_size = objects_total_size;
        self.baseline_known = true;
        self.last_update_time = Some(SystemTime::now());
        self.refresh_progress_percentage();
        self.refresh_estimated_completion_time();
    }

    pub fn set_total_baseline_with_generation(&mut self, objects_total_count: u64, objects_total_size: u64, generation: u64) {
        self.baseline_generation = Some(generation);
        self.set_total_baseline(objects_total_count, objects_total_size);
    }

    pub fn record_skipped_new_version(&mut self) {
        let Some(next) = self.skipped_new_versions.checked_add(1) else {
            self.mark_unknown();
            return;
        };
        self.skipped_new_versions = next;
        self.last_update_time = Some(SystemTime::now());
        self.refresh_progress_percentage();
        self.refresh_estimated_completion_time();
    }

    pub fn record_skipped_ilm_expired(&mut self) {
        let Some(next) = self.skipped_ilm_expired.checked_add(1) else {
            self.mark_unknown();
            return;
        };
        self.skipped_ilm_expired = next;
        self.last_update_time = Some(SystemTime::now());
        self.refresh_progress_percentage();
        self.refresh_estimated_completion_time();
    }

    fn completed_for_baseline(&self) -> Option<u64> {
        self.objects_healed
            .checked_add(self.objects_failed)?
            .checked_add(self.skipped_objects)?
            .checked_add(self.skipped_new_versions)?
            .checked_add(self.skipped_ilm_expired)
    }

    pub(crate) fn refresh_progress_percentage(&mut self) {
        if self.ledger_complete {
            self.progress_state = HealProgressState::Completed;
            self.progress_percentage = 100.0;
            return;
        }
        if self.counter_unknown {
            self.progress_state = HealProgressState::Unknown;
            self.progress_percentage = 0.0;
            return;
        }
        if !self.baseline_known {
            self.progress_state = HealProgressState::Indeterminate;
            self.progress_percentage = 0.0;
            self.estimated_completion_time = None;
            return;
        }
        if self.objects_total_size > 0 {
            self.progress_percentage = ((self.bytes_processed as f64 / self.objects_total_size as f64) * 100.0).min(100.0);
            self.progress_percentage = self.progress_percentage.min(99.999);
            self.progress_state = HealProgressState::Running;
            return;
        }
        if self.objects_total_count > 0 {
            let Some(completed) = self.completed_for_baseline() else {
                self.progress_state = HealProgressState::Unknown;
                self.progress_percentage = 0.0;
                return;
            };
            self.progress_percentage = ((completed as f64 / self.objects_total_count as f64) * 100.0).min(100.0);
            self.progress_percentage = self.progress_percentage.min(99.999);
            self.progress_state = HealProgressState::Running;
            return;
        }
        if self.baseline_known {
            self.progress_state = HealProgressState::Running;
            self.progress_percentage = 0.0;
            return;
        }
        self.progress_state = HealProgressState::Indeterminate;
        self.progress_percentage = 0.0;
    }

    pub fn set_current_object(&mut self, object: Option<String>) {
        self.current_object = object;
        self.last_update_time = Some(SystemTime::now());
    }

    pub fn refresh_estimated_completion_time(&mut self) {
        let Some(start_time) = self.start_time else {
            self.estimated_completion_time = None;
            return;
        };
        if self.is_completed()
            || self.progress_percentage <= 0.0
            || self.progress_percentage >= 100.0
            || self.bytes_processed == 0
        {
            self.estimated_completion_time = None;
            return;
        }

        let elapsed = match SystemTime::now().duration_since(start_time) {
            Ok(elapsed) if !elapsed.is_zero() => elapsed,
            _ => {
                self.estimated_completion_time = None;
                return;
            }
        };
        let estimated_total_secs = elapsed.as_secs_f64() * 100.0 / self.progress_percentage;
        self.estimated_completion_time = start_time.checked_add(Duration::from_secs_f64(estimated_total_secs));
    }

    pub fn is_completed(&self) -> bool {
        self.ledger_complete
    }

    /// Mark telemetry unknown while allowing the underlying heal operation to
    /// continue.  This is used for corrupt/overflowing counters at the
    /// observability boundary; it must never turn a successful heal into an
    /// execution error.
    pub fn mark_unknown(&mut self) {
        self.counter_unknown = true;
        self.progress_state = HealProgressState::Unknown;
        self.ledger_complete = false;
        self.progress_percentage = 0.0;
        self.estimated_completion_time = None;
        self.last_update_time = Some(SystemTime::now());
    }

    /// Mark the object ledger terminal only after the enclosing task has
    /// committed all durable resume state and cleanup fences.
    pub fn mark_completed(&mut self) {
        let telemetry_unknown = self.counter_unknown || self.progress_state == HealProgressState::Unknown;
        self.ledger_complete = true;
        if !telemetry_unknown {
            self.progress_state = HealProgressState::Completed;
        }
        self.progress_percentage = 100.0;
        self.last_update_time = Some(SystemTime::now());
        self.estimated_completion_time = None;
    }

    pub fn get_success_rate(&self) -> f64 {
        let Some(total) = self.objects_healed.checked_add(self.objects_failed) else {
            return 0.0;
        };
        if total > 0 {
            (self.objects_healed as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

pub fn aggregate_heal_progress(progresses: impl IntoIterator<Item = HealProgress>) -> Option<HealProgress> {
    let mut snapshot = HealProgress::default();
    let mut found = false;
    let mut has_object_sweep = false;
    let mut all_object_baselines_known = true;
    let mut baseline_generation = None;
    let mut baseline_generation_consistent = true;
    let mut all_ledgers_complete = true;
    let mut counter_overflow = false;

    for progress in progresses {
        found = true;
        let object_sweep = matches!(progress.kind, HealProgressKind::ObjectSweep);
        has_object_sweep |= object_sweep;
        all_ledgers_complete &= progress.ledger_complete;
        if object_sweep {
            all_object_baselines_known &= progress.baseline_known;
            match baseline_generation {
                None => baseline_generation = Some(progress.baseline_generation),
                Some(generation) => baseline_generation_consistent &= generation == progress.baseline_generation,
            }
        }
        counter_overflow |= progress.counter_unknown || matches!(progress.progress_state, HealProgressState::Unknown);
        for (target, value) in [
            (&mut snapshot.objects_scanned, progress.objects_scanned),
            (&mut snapshot.objects_healed, progress.objects_healed),
            (&mut snapshot.objects_failed, progress.objects_failed),
            (&mut snapshot.skipped_objects, progress.skipped_objects),
            (&mut snapshot.skipped_new_versions, progress.skipped_new_versions),
            (&mut snapshot.skipped_ilm_expired, progress.skipped_ilm_expired),
            (&mut snapshot.objects_total_count, progress.objects_total_count),
            (&mut snapshot.objects_total_size, progress.objects_total_size),
            (&mut snapshot.bytes_processed, progress.bytes_processed),
            (&mut snapshot.stage_current, progress.stage_current),
            (&mut snapshot.stage_total, progress.stage_total),
        ] {
            match target.checked_add(value) {
                Some(sum) => *target = sum,
                None => {
                    *target = u64::MAX;
                    counter_overflow = true;
                }
            }
        }
        snapshot.start_time = match (snapshot.start_time, progress.start_time) {
            (Some(current), Some(next)) => Some(current.min(next)),
            (None, next) => next,
            (current, None) => current,
        };
        snapshot.last_update_time = match (snapshot.last_update_time, progress.last_update_time) {
            (Some(current), Some(next)) => Some(current.max(next)),
            (None, next) => next,
            (current, None) => current,
        };
        if progress.current_object.is_some() {
            snapshot.current_object = progress.current_object;
        }
    }

    if !found {
        return None;
    }

    snapshot.kind = if has_object_sweep {
        HealProgressKind::ObjectSweep
    } else {
        HealProgressKind::Stage
    };
    snapshot.baseline_known = has_object_sweep && all_object_baselines_known && baseline_generation_consistent;
    snapshot.baseline_generation = if snapshot.baseline_known && baseline_generation_consistent {
        baseline_generation.flatten()
    } else {
        None
    };
    snapshot.ledger_complete = all_ledgers_complete;
    snapshot.counter_unknown = counter_overflow;
    if counter_overflow {
        snapshot.progress_state = HealProgressState::Unknown;
        snapshot.progress_percentage = if snapshot.ledger_complete { 100.0 } else { 0.0 };
    } else if snapshot.ledger_complete {
        snapshot.progress_state = HealProgressState::Completed;
        snapshot.progress_percentage = 100.0;
    } else if has_object_sweep {
        snapshot.refresh_progress_percentage();
    } else if snapshot.stage_total == 0 {
        snapshot.progress_state = HealProgressState::Indeterminate;
        snapshot.progress_percentage = 0.0;
    } else {
        snapshot.progress_state = HealProgressState::Running;
        snapshot.progress_percentage = ((snapshot.stage_current as f64 / snapshot.stage_total as f64) * 100.0).min(99.999);
    }
    snapshot.refresh_estimated_completion_time();
    Some(snapshot)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealStatistics {
    /// Total heal tasks
    pub total_tasks: u64,
    /// Successful tasks
    pub successful_tasks: u64,
    /// Failed tasks
    pub failed_tasks: u64,
    /// Running tasks
    pub running_tasks: u64,
    /// Total healed objects
    pub total_objects_healed: u64,
    /// Total healed bytes
    pub total_bytes_healed: u64,
    /// Last update time
    pub last_update_time: SystemTime,
}

impl Default for HealStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl HealStatistics {
    pub fn new() -> Self {
        Self {
            total_tasks: 0,
            successful_tasks: 0,
            failed_tasks: 0,
            running_tasks: 0,
            total_objects_healed: 0,
            total_bytes_healed: 0,
            last_update_time: SystemTime::now(),
        }
    }

    pub fn update_task_completion(&mut self, success: bool) {
        if success {
            self.successful_tasks += 1;
        } else {
            self.failed_tasks += 1;
        }
        self.last_update_time = SystemTime::now();
    }

    pub fn update_running_tasks(&mut self, count: u64) {
        self.running_tasks = count;
        self.last_update_time = SystemTime::now();
    }

    pub fn add_healed_objects(&mut self, count: u64, bytes: u64) {
        self.total_objects_healed += count;
        self.total_bytes_healed += bytes;
        self.last_update_time = SystemTime::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heal_progress_new() {
        let progress = HealProgress::new();
        assert_eq!(progress.objects_scanned, 0);
        assert_eq!(progress.objects_healed, 0);
        assert_eq!(progress.objects_failed, 0);
        assert_eq!(progress.skipped_objects, 0);
        assert_eq!(progress.skipped_new_versions, 0);
        assert_eq!(progress.skipped_ilm_expired, 0);
        assert_eq!(progress.objects_total_count, 0);
        assert_eq!(progress.objects_total_size, 0);
        assert_eq!(progress.bytes_processed, 0);
        assert_eq!(progress.progress_percentage, 0.0);
        assert!(progress.start_time.is_some());
        assert!(progress.last_update_time.is_some());
        assert!(progress.current_object.is_none());
    }

    #[test]
    fn test_heal_progress_update_progress() {
        let mut progress = HealProgress::new();
        progress.update_progress(10, 8, 2, 1024);

        assert_eq!(progress.objects_scanned, 10);
        assert_eq!(progress.objects_healed, 8);
        assert_eq!(progress.objects_failed, 2);
        assert_eq!(progress.bytes_processed, 1024);
        assert_eq!(progress.progress_state, HealProgressState::Indeterminate);
        assert_eq!(progress.progress_percentage, 0.0);
        assert!(progress.last_update_time.is_some());
    }

    #[test]
    fn test_heal_progress_estimates_completion_time_from_progress() {
        let mut progress = HealProgress::new();
        progress.start_time = Some(SystemTime::now() - Duration::from_secs(10));

        progress.set_total_baseline(100, 16384);
        progress.update_progress(25, 25, 0, 4096);

        let eta = progress
            .estimated_completion_time
            .expect("partial byte progress should estimate completion");
        assert!(eta > SystemTime::now());
    }

    #[test]
    fn test_heal_progress_uses_byte_baseline_for_percentage() {
        let mut progress = HealProgress::new();
        progress.set_total_baseline(10, 8192);

        progress.update_progress(25, 25, 0, 4096);

        assert!((progress.progress_percentage - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_uses_object_baseline_when_bytes_unknown() {
        let mut progress = HealProgress::new();
        progress.set_total_baseline(10, 0);

        progress.update_progress(5, 3, 2, 0);

        assert!((progress.progress_percentage - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_counts_skipped_versions_for_object_baseline() {
        let mut progress = HealProgress::new();
        progress.set_total_baseline(10, 0);

        progress.update_progress(5, 3, 2, 0);
        progress.record_skipped_new_version();

        assert_eq!(progress.skipped_new_versions, 1);
        assert!((progress.progress_percentage - 60.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_does_not_estimate_completion_without_bytes() {
        let mut progress = HealProgress::new();
        progress.start_time = Some(SystemTime::now() - Duration::from_secs(10));

        progress.update_progress(100, 25, 0, 0);

        assert!(progress.estimated_completion_time.is_none());
    }

    #[test]
    fn test_heal_progress_with_baseline_is_not_completed_by_processed_count() {
        let mut progress = HealProgress::new();
        progress.start_time = Some(SystemTime::now() - Duration::from_secs(10));
        progress.set_total_baseline(10, 8192);

        progress.update_progress(1, 1, 0, 1024);

        assert!(!progress.is_completed());
        assert!(progress.estimated_completion_time.is_some());
    }

    #[test]
    fn test_heal_progress_update_progress_zero_total() {
        let mut progress = HealProgress::new();
        progress.update_progress(0, 0, 0, 0);

        assert_eq!(progress.progress_percentage, 0.0);
    }

    #[test]
    fn test_heal_progress_update_progress_all_healed() {
        let mut progress = HealProgress::new();
        // When scanned=0, healed=10, failed=0: total=10, progress = 10/10 = 100%
        progress.update_progress(10, 10, 0, 2048);
        progress.mark_completed();

        // All healed, should be 100%
        assert!((progress.progress_percentage - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_successful_heal_reports_zero_failed() {
        // A successful single-object heal must record the object's size as bytes
        // processed WITHOUT inflating the failure count: the `failed` positional
        // arg is distinct from `bytes`. Regression guard for backlog#1033 where
        // the success paths passed object_size for both, corrupting
        // objects_failed / the admin-visible success rate.
        let object_size = 4096u64;
        let mut progress = HealProgress::new();
        progress.update_progress(3, 3, 0, object_size);

        assert_eq!(progress.objects_healed, 3);
        assert_eq!(progress.objects_failed, 0, "a successful heal must report zero failures");
        assert_eq!(progress.bytes_processed, object_size);
    }

    #[test]
    fn test_heal_progress_set_current_object() {
        let mut progress = HealProgress::new();
        let initial_time = progress.last_update_time;

        // Small delay to ensure time difference
        std::thread::sleep(std::time::Duration::from_millis(10));

        progress.set_current_object(Some("test-bucket/test-object".to_string()));

        assert_eq!(progress.current_object, Some("test-bucket/test-object".to_string()));
        assert!(progress.last_update_time.is_some());
        // last_update_time should be updated
        assert_ne!(progress.last_update_time, initial_time);
    }

    #[test]
    fn test_heal_progress_set_current_object_none() {
        let mut progress = HealProgress::new();
        progress.set_current_object(Some("test".to_string()));
        progress.set_current_object(None);

        assert!(progress.current_object.is_none());
    }

    #[test]
    fn test_heal_progress_serializes_camel_case_fields() {
        let mut progress = HealProgress::new();
        progress.update_progress(10, 8, 2, 1024);
        progress.set_current_object(Some("test-bucket/test-object".to_string()));

        let json = serde_json::to_value(&progress).expect("progress should serialize");

        assert_eq!(json["objectsScanned"], 10);
        assert_eq!(json["objectsHealed"], 8);
        assert_eq!(json["objectsFailed"], 2);
        assert_eq!(json["skippedObjects"], 0);
        assert_eq!(json["skippedNewVersions"], 0);
        assert_eq!(json["skippedIlmExpired"], 0);
        assert_eq!(json["bytesProcessed"], 1024);
        assert_eq!(json["currentObject"], "test-bucket/test-object");
        assert!(json["progressPercentage"].is_number());
    }

    #[test]
    fn test_heal_progress_is_completed_by_percentage() {
        let mut progress = HealProgress::new();
        progress.update_progress(10, 10, 0, 1024);
        progress.mark_completed();

        assert!(progress.is_completed());
    }

    #[test]
    fn test_heal_progress_is_completed_by_processed() {
        let mut progress = HealProgress::new();
        progress.objects_scanned = 10;
        progress.objects_healed = 8;
        progress.objects_failed = 2;
        progress.mark_completed();
        assert!(progress.is_completed());
    }

    #[test]
    fn test_heal_progress_is_not_completed() {
        let mut progress = HealProgress::new();
        progress.objects_scanned = 10;
        progress.objects_healed = 5;
        progress.objects_failed = 2;
        // healed + failed = 5 + 2 = 7 < scanned = 10
        assert!(!progress.is_completed());
    }

    #[test]
    fn test_heal_progress_get_success_rate() {
        let mut progress = HealProgress::new();
        progress.objects_healed = 8;
        progress.objects_failed = 2;

        // success_rate = 8 / (8 + 2) * 100 = 80%
        assert!((progress.get_success_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_get_success_rate_zero_total() {
        let progress = HealProgress::new();
        // No healed or failed objects
        assert_eq!(progress.get_success_rate(), 0.0);
    }

    #[test]
    fn test_heal_progress_get_success_rate_all_success() {
        let mut progress = HealProgress::new();
        progress.objects_healed = 10;
        progress.objects_failed = 0;

        assert!((progress.get_success_rate() - 100.0).abs() < 0.001);
    }

    #[test]
    fn single_object_progress_reaches_terminal_100() {
        let mut progress = HealProgress::new();
        progress.update_object_progress(1, 1, 0, 0, 128);
        assert!(!progress.is_completed());
        progress.mark_completed();
        assert!(progress.is_completed());
        assert_eq!(progress.progress_percentage, 100.0);
    }

    #[test]
    fn progress_without_baseline_is_indeterminate() {
        let mut progress = HealProgress::new();
        progress.update_object_progress(1, 1, 0, 0, 128);
        assert_eq!(progress.progress_state, HealProgressState::Indeterminate);
        assert_eq!(progress.progress_percentage, 0.0);
        assert!(progress.estimated_completion_time.is_none());
    }

    #[test]
    fn progress_retry_is_exactly_once() {
        let mut progress = HealProgress::new();
        progress.set_total_baseline(1, 128);
        progress.update_object_progress(1, 1, 0, 0, 128);
        progress.update_object_progress(1, 1, 0, 0, 128);
        assert_eq!(progress.objects_scanned, 1);
        assert_eq!(progress.objects_healed, 1);
        assert_eq!(progress.bytes_processed, 128);
    }

    #[test]
    fn progress_never_triggers_cleanup_before_terminal_ledger_empty() {
        let mut progress = HealProgress::new();
        progress.progress_percentage = 100.0;
        assert!(!progress.is_completed());
        progress.mark_completed();
        assert!(progress.is_completed());
    }

    #[test]
    fn progress_counter_overflow_is_marked_unknown_without_aborting_completed_heal() {
        let mut progress = HealProgress::new();
        progress.update_object_progress(u64::MAX, u64::MAX, 1, 0, 0);
        assert_eq!(progress.progress_state, HealProgressState::Unknown);
        progress.mark_completed();
        assert!(progress.is_completed());
        assert_eq!(progress.progress_state, HealProgressState::Unknown);

        let aggregate = aggregate_heal_progress([progress]).expect("progress should aggregate");
        assert!(aggregate.ledger_complete);
        assert!(aggregate.counter_unknown);
        assert_eq!(aggregate.progress_state, HealProgressState::Unknown);
        assert_eq!(aggregate.progress_percentage, 100.0);
    }

    #[test]
    fn aggregate_rejects_mixed_baseline_generations() {
        let progress = |generation| HealProgress {
            kind: HealProgressKind::ObjectSweep,
            objects_scanned: 5,
            objects_total_count: 10,
            progress_state: HealProgressState::Running,
            baseline_generation: Some(generation),
            baseline_known: true,
            ..Default::default()
        };

        let aggregate = aggregate_heal_progress([progress(1), progress(2)]).expect("progress should aggregate");
        assert!(!aggregate.baseline_known);
        assert_eq!(aggregate.baseline_generation, None);
        assert_eq!(aggregate.progress_state, HealProgressState::Indeterminate);
        assert_eq!(aggregate.progress_percentage, 0.0);
    }

    #[test]
    fn aggregate_accepts_multiple_sets_from_one_snapshot_generation() {
        let progress = |objects_scanned| HealProgress {
            kind: HealProgressKind::ObjectSweep,
            objects_scanned,
            objects_total_count: 10,
            progress_state: HealProgressState::Running,
            baseline_generation: Some(7),
            baseline_known: true,
            ..Default::default()
        };

        let aggregate = aggregate_heal_progress([progress(5), progress(3)]).expect("progress should aggregate");
        assert!(aggregate.baseline_known);
        assert_eq!(aggregate.baseline_generation, Some(7));
    }

    #[test]
    fn stage_updates_do_not_double_count_object_outcomes() {
        let mut progress = HealProgress::new();
        progress.update_object_progress(2, 1, 0, 1, 256);
        progress.update_stage(3, 4);
        assert_eq!(progress.kind, HealProgressKind::ObjectSweep);
        assert_eq!(progress.objects_scanned, 2);
        assert_eq!(progress.objects_healed, 1);
        assert_eq!(progress.skipped_objects, 1);
    }

    #[test]
    fn test_heal_statistics_new() {
        let stats = HealStatistics::new();
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 0);
        assert_eq!(stats.running_tasks, 0);
        assert_eq!(stats.total_objects_healed, 0);
        assert_eq!(stats.total_bytes_healed, 0);
    }

    #[test]
    fn test_heal_statistics_default() {
        let stats = HealStatistics::default();
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 0);
    }

    #[test]
    fn test_heal_statistics_update_task_completion_success() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.update_task_completion(true);

        assert_eq!(stats.successful_tasks, 1);
        assert_eq!(stats.failed_tasks, 0);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_update_task_completion_failure() {
        let mut stats = HealStatistics::new();
        stats.update_task_completion(false);

        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 1);
    }

    #[test]
    fn test_heal_statistics_update_running_tasks() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.update_running_tasks(5);

        assert_eq!(stats.running_tasks, 5);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_add_healed_objects() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.add_healed_objects(10, 10240);

        assert_eq!(stats.total_objects_healed, 10);
        assert_eq!(stats.total_bytes_healed, 10240);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_add_healed_objects_accumulative() {
        let mut stats = HealStatistics::new();
        stats.add_healed_objects(5, 5120);
        stats.add_healed_objects(3, 3072);

        assert_eq!(stats.total_objects_healed, 8);
        assert_eq!(stats.total_bytes_healed, 8192);
    }
}
