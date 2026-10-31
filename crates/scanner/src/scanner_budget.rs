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

use rustfs_scanner_metrics::metrics::RemoteScanProgress;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const BUDGET_REASON_NONE: u8 = 0;
const BUDGET_REASON_RUNTIME: u8 = 1;
const BUDGET_REASON_OBJECTS: u8 = 2;
const BUDGET_REASON_DIRECTORIES: u8 = 3;
const BUDGET_REASON_ENTRIES: u8 = 4;
const PROGRESS_CLOCK_SAMPLE_INTERVAL: u64 = 128;
/// One raw-enumeration page, so a directory stop always commits durable work.
const DIRECTORY_ENTRY_BUDGET_FLOOR: u64 = 128;
/// Directory entries one object or directory budget unit pays for. A forward
/// sweep classifies in sorted order and so needs a whole listing first; a
/// directory a few times wider than the budget must still reach objects in
/// its first round, while a far wider one builds its index across rounds.
const DIRECTORY_ENTRIES_PER_COUNT_UNIT: u64 = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScannerCycleBudgetConfig {
    pub max_duration: Option<Duration>,
    pub max_objects: Option<u64>,
    pub max_directories: Option<u64>,
    /// Raw directory entries first observed under this budget. Entries replayed
    /// from a validated page index are not charged.
    pub max_entries: Option<u64>,
    /// Cumulative name bytes of the charged raw entries.
    pub max_entry_name_bytes: Option<u64>,
    /// One bucket's share. `None` lets a bucket consume the whole budget.
    pub bucket_quantum: Option<ScannerBucketQuantum>,
}

/// Limits of one bucket admission, charged to the owning cycle budget as well.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScannerBucketQuantum {
    pub max_duration: Option<Duration>,
    pub max_objects: Option<u64>,
    pub max_directories: Option<u64>,
    pub max_entries: Option<u64>,
    pub max_entry_name_bytes: Option<u64>,
}

impl From<ScannerBucketQuantum> for ScannerCycleBudgetConfig {
    fn from(quantum: ScannerBucketQuantum) -> Self {
        Self {
            max_duration: quantum.max_duration,
            max_objects: quantum.max_objects,
            max_directories: quantum.max_directories,
            max_entries: quantum.max_entries,
            max_entry_name_bytes: quantum.max_entry_name_bytes,
            bucket_quantum: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleBudgetReason {
    Runtime,
    Objects,
    Directories,
    Entries,
}

impl ScannerCycleBudgetReason {
    fn code(self) -> u8 {
        match self {
            Self::Runtime => BUDGET_REASON_RUNTIME,
            Self::Objects => BUDGET_REASON_OBJECTS,
            Self::Directories => BUDGET_REASON_DIRECTORIES,
            Self::Entries => BUDGET_REASON_ENTRIES,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            BUDGET_REASON_RUNTIME => Some(Self::Runtime),
            BUDGET_REASON_OBJECTS => Some(Self::Objects),
            BUDGET_REASON_DIRECTORIES => Some(Self::Directories),
            BUDGET_REASON_ENTRIES => Some(Self::Entries),
            _ => None,
        }
    }
}

pub struct ScannerCycleBudget {
    /// The cycle budget a bucket quantum also charges.
    parent: Option<Arc<ScannerCycleBudget>>,
    token: CancellationToken,
    reason: Arc<AtomicU8>,
    started_at: Instant,
    deadline: Option<Instant>,
    max_duration: Option<Duration>,
    max_objects: Option<u64>,
    max_directories: Option<u64>,
    max_entries: Option<u64>,
    max_entry_name_bytes: Option<u64>,
    directory_entry_limit: Option<u64>,
    bucket_quantum: Option<ScannerBucketQuantum>,
    track_progress: bool,
    track_unbounded_counts: bool,
    objects_scanned: AtomicU64,
    directories_started: AtomicU64,
    entries_visited: AtomicU64,
    entries_charged: AtomicU64,
    entry_name_bytes_charged: AtomicU64,
    last_progress_millis: AtomicU64,
    cycle_state_persisted: AtomicBool,
    remote_scan_progress: Arc<RemoteScanProgress>,
}

impl ScannerCycleBudget {
    #[cfg(test)]
    pub(crate) fn new(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        Self::new_inner(parent, config, false, false, None)
    }

    pub(crate) fn new_with_progress_tracking(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        Self::new_inner(parent, config, true, true, None)
    }

    pub(crate) fn new_with_runtime_progress_tracking(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        let track_progress = config.max_duration.is_some();
        Self::new_inner(parent, config, track_progress, false, None)
    }

    fn new_inner(
        parent: &CancellationToken,
        config: ScannerCycleBudgetConfig,
        track_progress: bool,
        track_unbounded_counts: bool,
        cycle_budget: Option<Arc<ScannerCycleBudget>>,
    ) -> Arc<Self> {
        let token = parent.child_token();
        let reason = Arc::new(AtomicU8::new(BUDGET_REASON_NONE));
        let started_at = Instant::now();
        let deadline = config.max_duration.map(|duration| match started_at.checked_add(duration) {
            Some(deadline) => deadline,
            // Runtime config rejects this range, but keep programmatic callers
            // fail-closed instead of panicking or silently disabling the wall clock.
            None => started_at,
        });

        if let Some(deadline) = deadline {
            let parent = parent.clone();
            let token_wait = token.clone();
            let token_cancel = token.clone();
            let reason = reason.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = parent.cancelled() => {}
                    _ = token_wait.cancelled() => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        Self::cancel_for_reason(&reason, &token_cancel, ScannerCycleBudgetReason::Runtime);
                    }
                }
            });
        }

        let remote_scan_progress = cycle_budget
            .as_ref()
            .map_or_else(|| Arc::new(RemoteScanProgress::default()), |budget| budget.remote_scan_progress());
        Arc::new(Self {
            parent: cycle_budget,
            token,
            reason,
            started_at,
            deadline,
            max_duration: config.max_duration,
            max_objects: config.max_objects,
            max_directories: config.max_directories,
            max_entries: config.max_entries,
            max_entry_name_bytes: config.max_entry_name_bytes,
            directory_entry_limit: directory_entry_limit(&config),
            bucket_quantum: config.bucket_quantum,
            track_progress,
            track_unbounded_counts,
            objects_scanned: AtomicU64::new(0),
            directories_started: AtomicU64::new(0),
            entries_visited: AtomicU64::new(0),
            entries_charged: AtomicU64::new(0),
            entry_name_bytes_charged: AtomicU64::new(0),
            last_progress_millis: AtomicU64::new(0),
            cycle_state_persisted: AtomicBool::new(false),
            remote_scan_progress,
        })
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Budget and cancellation scope of one bucket scan. Without a quantum the
    /// bucket shares this budget; a quantum stops only its own scan token.
    pub(crate) fn for_bucket(self: &Arc<Self>, ctx: &CancellationToken) -> (Arc<Self>, CancellationToken) {
        let Some(quantum) = self.bucket_quantum else {
            return (self.clone(), ctx.child_token());
        };
        let budget = Self::new_inner(ctx, quantum.into(), self.track_progress, self.track_unbounded_counts, Some(self.clone()));
        let token = budget.token();
        (budget, token)
    }

    /// True when only this bucket's share ended and the cycle can continue.
    pub(crate) fn quantum_exhausted(&self) -> bool {
        self.parent.as_ref().is_some_and(|parent| !parent.budget_elapsed())
            && self.reason.load(Ordering::Relaxed) != BUDGET_REASON_NONE
    }

    pub(crate) fn budget_elapsed(&self) -> bool {
        self.reason.load(Ordering::Relaxed) != BUDGET_REASON_NONE
            || self.parent.as_ref().is_some_and(|parent| parent.budget_elapsed())
    }

    pub(crate) fn reason(&self) -> Option<ScannerCycleBudgetReason> {
        ScannerCycleBudgetReason::from_code(self.reason.load(Ordering::Relaxed))
            .or_else(|| self.parent.as_ref().and_then(|parent| parent.reason()))
    }

    pub(crate) fn max_duration(&self) -> Option<Duration> {
        self.max_duration
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn cancel_for_runtime(&self) {
        self.cancel_for(ScannerCycleBudgetReason::Runtime);
    }

    pub(crate) fn max_objects(&self) -> Option<u64> {
        self.max_objects
    }

    pub(crate) fn max_directories(&self) -> Option<u64> {
        self.max_directories
    }

    pub(crate) fn requires_serial_progress_accounting(&self) -> bool {
        self.max_objects.is_some()
            || self.max_directories.is_some()
            || self.max_entries.is_some()
            || self.max_entry_name_bytes.is_some()
    }

    pub(crate) fn remaining_config(&self) -> ScannerCycleBudgetConfig {
        let max_duration = self
            .max_duration
            .map(|duration| duration.saturating_sub(self.started_at.elapsed()));
        if max_duration.is_some_and(|duration| duration.is_zero()) {
            self.cancel_for(ScannerCycleBudgetReason::Runtime);
        }

        ScannerCycleBudgetConfig {
            max_duration,
            max_objects: self
                .max_objects
                .map(|max| max.saturating_sub(self.objects_scanned.load(Ordering::Relaxed))),
            max_directories: self
                .max_directories
                .map(|max| max.saturating_sub(self.directories_started.load(Ordering::Relaxed))),
            max_entries: self
                .max_entries
                .map(|max| max.saturating_sub(self.entries_charged.load(Ordering::Relaxed))),
            max_entry_name_bytes: self
                .max_entry_name_bytes
                .map(|max| max.saturating_sub(self.entry_name_bytes_charged.load(Ordering::Relaxed))),
            bucket_quantum: None,
        }
    }

    pub(crate) fn progress(&self) -> (u64, u64) {
        (
            self.objects_scanned.load(Ordering::Relaxed),
            self.directories_started.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn entries_visited(&self) -> u64 {
        self.entries_visited.load(Ordering::Relaxed)
    }

    pub(crate) fn mark_cycle_state_persisted(&self) {
        self.cycle_state_persisted.store(true, Ordering::Release);
    }

    pub(crate) fn cycle_state_persisted(&self) -> bool {
        self.cycle_state_persisted.load(Ordering::Acquire)
    }

    pub(crate) fn progress_age(&self) -> Duration {
        let elapsed_millis = u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let last_progress = self.last_progress_millis.load(Ordering::Relaxed);
        Duration::from_millis(elapsed_millis.saturating_sub(last_progress))
    }

    fn record_progress_sample(&self, event: u64) {
        // Clock reads are sampled at batch/count boundaries; the scanner's
        // per-object path does not add a second progress atomic.
        if event == 0 || (event != 1 && !event.is_multiple_of(PROGRESS_CLOCK_SAMPLE_INTERVAL)) {
            return;
        }
        let elapsed_millis = u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last_progress_millis.store(elapsed_millis, Ordering::Relaxed);
    }

    pub(crate) fn record_entries_visited(&self, entries_visited: u64) {
        if let Some(parent) = &self.parent {
            parent.record_entries_visited(entries_visited);
        }
        if self.track_progress {
            let entries = saturating_fetch_add(&self.entries_visited, entries_visited);
            self.record_progress_sample(entries);
        }
    }

    pub(crate) fn remote_scan_progress(&self) -> Arc<RemoteScanProgress> {
        Arc::clone(&self.remote_scan_progress)
    }

    pub(crate) fn record_remote_progress(&self, objects_scanned: u64, directories_started: u64) {
        // A quantum shares the cycle's progress record; its parent writes it.
        match &self.parent {
            Some(parent) => parent.record_remote_progress(objects_scanned, directories_started),
            None => self.remote_scan_progress.record(objects_scanned, directories_started),
        }
        if self.track_progress || self.max_objects.is_some() {
            let objects = saturating_fetch_add(&self.objects_scanned, objects_scanned);
            if self.track_progress {
                self.record_progress_sample(objects);
            }
            if self.max_objects.is_some_and(|max_objects| objects >= max_objects) {
                self.cancel_for(ScannerCycleBudgetReason::Objects);
            }
        }

        if self.track_progress || self.max_directories.is_some() {
            let directories = saturating_fetch_add(&self.directories_started, directories_started);
            if self.track_progress {
                self.record_progress_sample(directories);
            }
            if self
                .max_directories
                .is_some_and(|max_directories| directory_budget_exhausted(directories, max_directories))
            {
                self.cancel_for(ScannerCycleBudgetReason::Directories);
            }
        }
    }

    pub(crate) fn cancel_after_unreported_remote_progress(&self) {
        if let Some(parent) = &self.parent {
            parent.cancel_after_unreported_remote_progress();
        }
        if self.max_objects.is_some() {
            self.cancel_for(ScannerCycleBudgetReason::Objects);
        } else if self.max_directories.is_some() {
            self.cancel_for(ScannerCycleBudgetReason::Directories);
        }
    }

    pub(crate) fn try_start_directory(&self) -> bool {
        let parent_allows = self.parent.as_ref().is_none_or(|parent| parent.try_start_directory());
        self.try_start_own_directory() && parent_allows
    }

    fn try_start_own_directory(&self) -> bool {
        if self.max_directories.is_none() && !self.track_unbounded_counts {
            return true;
        }

        let directories = saturating_fetch_add(&self.directories_started, 1);
        if self.track_progress {
            self.record_progress_sample(directories);
        }
        if self
            .max_directories
            .is_some_and(|max_directories| directory_budget_exhausted(directories, max_directories))
        {
            self.cancel_for(ScannerCycleBudgetReason::Directories);
            return false;
        }

        true
    }

    /// Charges one raw entry first observed in this scan. `directory_entries`
    /// counts the charged entries of the current directory: a count budget is
    /// a total work limit, so one listing cannot read past it.
    ///
    /// Returns `false` after charging the entry that exhausts a limit; the
    /// caller stops with the entry already recorded in its durable index.
    pub(crate) fn try_charge_raw_entry(&self, name_bytes: u64, directory_entries: u64) -> bool {
        let parent_allows = self
            .parent
            .as_ref()
            .is_none_or(|parent| parent.try_charge_raw_entry(name_bytes, directory_entries));
        let entries = self
            .max_entries
            .is_some_and(|max| saturating_fetch_add(&self.entries_charged, 1) >= max);
        let name_bytes = self
            .max_entry_name_bytes
            .is_some_and(|max| saturating_fetch_add(&self.entry_name_bytes_charged, name_bytes) >= max);
        let directory = self.directory_entry_limit.is_some_and(|limit| directory_entries >= limit);
        if entries || name_bytes || directory {
            self.cancel_for(ScannerCycleBudgetReason::Entries);
            return false;
        }
        parent_allows
    }

    pub(crate) fn record_object_scanned(&self) {
        if let Some(parent) = &self.parent {
            parent.record_object_scanned();
        }
        if self.max_objects.is_none() && !self.track_unbounded_counts {
            return;
        }

        let objects = saturating_fetch_add(&self.objects_scanned, 1);
        if self.track_progress {
            self.record_progress_sample(objects);
        }
        if self.max_objects.is_some_and(|max_objects| objects >= max_objects) {
            self.cancel_for(ScannerCycleBudgetReason::Objects);
        }
    }

    fn cancel_for(&self, reason: ScannerCycleBudgetReason) {
        Self::cancel_for_reason(&self.reason, &self.token, reason);
    }

    fn cancel_for_reason(reason: &AtomicU8, token: &CancellationToken, budget_reason: ScannerCycleBudgetReason) {
        if reason
            .compare_exchange(BUDGET_REASON_NONE, budget_reason.code(), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            token.cancel();
        }
    }
}

fn saturating_fetch_add(value: &AtomicU64, delta: u64) -> u64 {
    let mut current = value.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(delta);
        match value.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(observed) => current = observed,
        }
    }
}

fn directory_entry_limit(config: &ScannerCycleBudgetConfig) -> Option<u64> {
    if config.max_objects.is_none() && config.max_directories.is_none() {
        return None;
    }
    let total = config
        .max_objects
        .unwrap_or(0)
        .saturating_add(config.max_directories.unwrap_or(0));
    Some(
        total
            .saturating_mul(DIRECTORY_ENTRIES_PER_COUNT_UNIT)
            .max(DIRECTORY_ENTRY_BUDGET_FLOOR),
    )
}

fn directory_budget_exhausted(directories: u64, max_directories: u64) -> bool {
    // Saturation hides a remote max+1 update when the configured limit is the
    // largest representable counter. Treat that boundary as exhausted rather
    // than allowing work to continue indefinitely.
    directories > max_directories || (directories == u64::MAX && max_directories == u64::MAX)
}

impl Drop for ScannerCycleBudget {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fair_prefix_quantum_budget_limits_entries_and_name_bytes_with_one_reason() {
        let parent = CancellationToken::new();
        let by_entries = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_entries: Some(2),
                ..Default::default()
            },
        );
        assert!(by_entries.try_charge_raw_entry(1, 1));
        assert!(!by_entries.try_charge_raw_entry(1, 2));
        assert_eq!(by_entries.reason(), Some(ScannerCycleBudgetReason::Entries));
        assert!(by_entries.token().is_cancelled());

        let by_bytes = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_entry_name_bytes: Some(10),
                ..Default::default()
            },
        );
        assert!(by_bytes.try_charge_raw_entry(9, 1));
        assert!(!by_bytes.try_charge_raw_entry(1, 2));
        assert_eq!(by_bytes.reason(), Some(ScannerCycleBudgetReason::Entries));
        assert!(!parent.is_cancelled(), "a budget stop must not cancel its parent token");
    }

    #[tokio::test]
    async fn fair_prefix_quantum_budget_derives_a_directory_limit_only_from_count_budgets() {
        let parent = CancellationToken::new();
        let runtime_only = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        assert!(runtime_only.try_charge_raw_entry(1, u64::MAX));

        let small = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        assert!(small.try_charge_raw_entry(1, DIRECTORY_ENTRY_BUDGET_FLOOR - 1));
        assert!(!small.try_charge_raw_entry(1, DIRECTORY_ENTRY_BUDGET_FLOOR));

        let large = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(1000),
                max_directories: Some(24),
                ..Default::default()
            },
        );
        assert!(large.try_charge_raw_entry(1, 1024 * DIRECTORY_ENTRIES_PER_COUNT_UNIT - 1));
        assert!(!large.try_charge_raw_entry(1, 1024 * DIRECTORY_ENTRIES_PER_COUNT_UNIT));
    }

    #[tokio::test]
    async fn fair_prefix_quantum_bucket_share_stops_only_its_own_scan() {
        let ctx = CancellationToken::new();
        let cycle = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(3),
                bucket_quantum: Some(ScannerBucketQuantum {
                    max_objects: Some(2),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let (first, first_token) = cycle.for_bucket(&cycle.token());
        first.record_object_scanned();
        first.record_object_scanned();
        assert!(first.quantum_exhausted());
        assert!(first_token.is_cancelled());
        assert_eq!(first.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(!cycle.budget_elapsed(), "a bucket share leaves the cycle running");
        assert!(!cycle.token().is_cancelled());
        assert_eq!(cycle.progress().0, 2, "bucket work is charged to the cycle");

        let (second, second_token) = cycle.for_bucket(&cycle.token());
        assert!(!second.budget_elapsed());
        second.record_object_scanned();
        assert!(cycle.budget_elapsed(), "the total budget still bounds every share");
        assert!(second_token.is_cancelled());
        assert!(second.budget_elapsed());
        assert!(!second.quantum_exhausted(), "a cycle stop is not a quantum handoff");
    }

    #[tokio::test]
    async fn fair_prefix_quantum_bucket_share_runtime_expires_independently() {
        let ctx = CancellationToken::new();
        let cycle = ScannerCycleBudget::new(
            &ctx,
            ScannerCycleBudgetConfig {
                bucket_quantum: Some(ScannerBucketQuantum {
                    max_duration: Some(Duration::ZERO),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let (bucket, token) = cycle.for_bucket(&cycle.token());
        tokio::time::timeout(Duration::from_secs(5), token.cancelled())
            .await
            .expect("quantum deadline should cancel the bucket scan");
        assert_eq!(bucket.reason(), Some(ScannerCycleBudgetReason::Runtime));
        assert!(!cycle.budget_elapsed());
    }

    #[tokio::test]
    async fn bucket_without_a_quantum_shares_the_cycle_budget() {
        let ctx = CancellationToken::new();
        let cycle = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (bucket, token) = cycle.for_bucket(&cycle.token());
        assert!(Arc::ptr_eq(&bucket, &cycle));
        cycle.cancel_for_runtime();
        assert!(token.is_cancelled());
        assert!(!bucket.quantum_exhausted());
    }

    #[tokio::test]
    async fn runtime_budget_cancels_child_token() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        );

        let token = budget.token();
        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("runtime budget did not cancel child token");

        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Runtime));
        assert!(token.is_cancelled());
    }

    #[test]
    fn object_budget_cancels_after_reaching_limit() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(2),
                ..Default::default()
            },
        );

        budget.record_object_scanned();
        assert!(!budget.budget_elapsed());

        budget.record_object_scanned();
        assert!(budget.budget_elapsed());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(budget.token().is_cancelled());
    }

    #[test]
    fn directory_budget_rejects_directory_after_limit() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(1),
                ..Default::default()
            },
        );

        assert!(budget.try_start_directory());
        assert!(!budget.budget_elapsed());
        assert!(!budget.try_start_directory());
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Directories));
        assert!(budget.token().is_cancelled());
    }

    #[test]
    fn first_budget_cancellation_reason_remains_observable() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                max_directories: Some(0),
                ..Default::default()
            },
        );

        budget.record_object_scanned();
        assert!(!budget.try_start_directory());

        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(budget.token().is_cancelled());
    }

    #[tokio::test]
    async fn remaining_config_accounts_for_local_and_remote_progress() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_secs(60)),
                max_objects: Some(10),
                max_directories: Some(5),
                ..Default::default()
            },
        );

        budget.record_object_scanned();
        assert!(budget.try_start_directory());
        budget.record_remote_progress(3, 2);

        let remaining = budget.remaining_config();
        assert!(
            remaining
                .max_duration
                .is_some_and(|duration| duration <= Duration::from_secs(60))
        );
        assert_eq!(remaining.max_objects, Some(6));
        assert_eq!(remaining.max_directories, Some(2));
        assert_eq!(budget.progress(), (4, 3));
        assert!(!budget.budget_elapsed());
    }

    #[test]
    fn remote_progress_preserves_object_and_directory_limit_semantics() {
        let parent = CancellationToken::new();
        let object_budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(2),
                ..Default::default()
            },
        );
        object_budget.record_remote_progress(2, 0);
        assert_eq!(object_budget.reason(), Some(ScannerCycleBudgetReason::Objects));

        let directory_budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(2),
                ..Default::default()
            },
        );
        directory_budget.record_remote_progress(0, 2);
        assert!(!directory_budget.budget_elapsed());
        directory_budget.record_remote_progress(0, 1);
        assert_eq!(directory_budget.reason(), Some(ScannerCycleBudgetReason::Directories));
    }

    #[test]
    fn directory_budget_fails_closed_when_progress_saturates() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(u64::MAX),
                ..Default::default()
            },
        );

        budget.record_remote_progress(0, u64::MAX);

        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Directories));
        assert!(budget.token().is_cancelled());

        let local_budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(u64::MAX),
                ..Default::default()
            },
        );
        local_budget.record_remote_progress(0, u64::MAX - 1);
        assert!(!local_budget.budget_elapsed());
        assert!(!local_budget.try_start_directory());
        assert_eq!(local_budget.reason(), Some(ScannerCycleBudgetReason::Directories));
    }

    #[test]
    fn explicit_progress_tracking_counts_unbounded_remote_work_without_cancelling() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());

        assert!(budget.try_start_directory());
        budget.record_object_scanned();
        budget.record_remote_progress(2, 3);

        assert_eq!(budget.progress(), (3, 4));
        assert!(!budget.budget_elapsed());
        assert!(!budget.token().is_cancelled());
    }

    #[test]
    fn unreported_remote_progress_cancels_count_budget() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(10),
                ..Default::default()
            },
        );

        budget.cancel_after_unreported_remote_progress();

        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(budget.token().is_cancelled());
    }

    #[tokio::test]
    async fn count_budgets_require_serial_progress_accounting() {
        let parent = CancellationToken::new();
        let runtime_only = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        let object_limited = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        let directory_limited = ScannerCycleBudget::new(
            &parent,
            ScannerCycleBudgetConfig {
                max_directories: Some(1),
                ..Default::default()
            },
        );

        assert!(!runtime_only.requires_serial_progress_accounting());
        assert!(object_limited.requires_serial_progress_accounting());
        assert!(directory_limited.requires_serial_progress_accounting());
    }

    #[tokio::test(start_paused = true)]
    async fn progress_age_uses_virtual_time_and_sampled_progress() {
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_runtime_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(budget.progress_age(), Duration::from_secs(5));
        budget.record_entries_visited(1);
        assert_eq!(budget.progress_age(), Duration::ZERO);

        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..126 {
            budget.record_entries_visited(1);
        }
        assert_eq!(budget.progress_age(), Duration::from_secs(2));
        budget.record_entries_visited(1);
        assert_eq!(budget.progress_age(), Duration::ZERO);
    }
}
