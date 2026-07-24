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

use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
use std::time::Instant;

use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const BUDGET_REASON_NONE: u8 = 0;
const BUDGET_REASON_RUNTIME: u8 = 1;
const BUDGET_REASON_OBJECTS: u8 = 2;
const BUDGET_REASON_DIRECTORIES: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScannerCycleBudgetConfig {
    pub max_duration: Option<Duration>,
    pub max_objects: Option<u64>,
    pub max_directories: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleBudgetReason {
    Runtime,
    Objects,
    Directories,
}

impl ScannerCycleBudgetReason {
    fn code(self) -> u8 {
        match self {
            Self::Runtime => BUDGET_REASON_RUNTIME,
            Self::Objects => BUDGET_REASON_OBJECTS,
            Self::Directories => BUDGET_REASON_DIRECTORIES,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            BUDGET_REASON_RUNTIME => Some(Self::Runtime),
            BUDGET_REASON_OBJECTS => Some(Self::Objects),
            BUDGET_REASON_DIRECTORIES => Some(Self::Directories),
            _ => None,
        }
    }
}

pub struct ScannerCycleBudget {
    token: CancellationToken,
    reason: Arc<AtomicU8>,
    started_at: Instant,
    max_duration: Option<Duration>,
    max_objects: Option<u64>,
    max_directories: Option<u64>,
    track_progress: bool,
    objects_scanned: AtomicU64,
    directories_started: AtomicU64,
    entries_visited: AtomicU64,
}

impl ScannerCycleBudget {
    pub(crate) fn new(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        Self::new_inner(parent, config, false)
    }

    pub(crate) fn new_with_progress_tracking(parent: &CancellationToken, config: ScannerCycleBudgetConfig) -> Arc<Self> {
        Self::new_inner(parent, config, true)
    }

    fn new_inner(parent: &CancellationToken, config: ScannerCycleBudgetConfig, track_progress: bool) -> Arc<Self> {
        let token = parent.child_token();
        let reason = Arc::new(AtomicU8::new(BUDGET_REASON_NONE));

        if let Some(duration) = config.max_duration {
            let parent = parent.clone();
            let token_wait = token.clone();
            let token_cancel = token.clone();
            let reason = reason.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = parent.cancelled() => {}
                    _ = token_wait.cancelled() => {}
                    _ = tokio::time::sleep(duration) => {
                        Self::cancel_for_reason(&reason, &token_cancel, ScannerCycleBudgetReason::Runtime);
                    }
                }
            });
        }

        Arc::new(Self {
            token,
            reason,
            started_at: Instant::now(),
            max_duration: config.max_duration,
            max_objects: config.max_objects,
            max_directories: config.max_directories,
            track_progress,
            objects_scanned: AtomicU64::new(0),
            directories_started: AtomicU64::new(0),
            entries_visited: AtomicU64::new(0),
        })
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub(crate) fn budget_elapsed(&self) -> bool {
        self.reason.load(Ordering::Relaxed) != BUDGET_REASON_NONE
    }

    pub(crate) fn reason(&self) -> Option<ScannerCycleBudgetReason> {
        ScannerCycleBudgetReason::from_code(self.reason.load(Ordering::Relaxed))
    }

    pub(crate) fn max_duration(&self) -> Option<Duration> {
        self.max_duration
    }

    pub(crate) fn max_objects(&self) -> Option<u64> {
        self.max_objects
    }

    pub(crate) fn max_directories(&self) -> Option<u64> {
        self.max_directories
    }

    pub(crate) fn requires_serial_progress_accounting(&self) -> bool {
        self.max_objects.is_some() || self.max_directories.is_some()
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

    pub(crate) fn record_entries_visited(&self, entries_visited: u64) {
        if self.track_progress {
            saturating_fetch_add(&self.entries_visited, entries_visited);
        }
    }

    pub(crate) fn record_remote_progress(&self, objects_scanned: u64, directories_started: u64) {
        if self.track_progress || self.max_objects.is_some() {
            let objects = saturating_fetch_add(&self.objects_scanned, objects_scanned);
            if self.max_objects.is_some_and(|max_objects| objects >= max_objects) {
                self.cancel_for(ScannerCycleBudgetReason::Objects);
            }
        }

        if self.track_progress || self.max_directories.is_some() {
            let directories = saturating_fetch_add(&self.directories_started, directories_started);
            if self
                .max_directories
                .is_some_and(|max_directories| directories > max_directories)
            {
                self.cancel_for(ScannerCycleBudgetReason::Directories);
            }
        }
    }

    pub(crate) fn cancel_after_unreported_remote_progress(&self) {
        if self.max_objects.is_some() {
            self.cancel_for(ScannerCycleBudgetReason::Objects);
        } else if self.max_directories.is_some() {
            self.cancel_for(ScannerCycleBudgetReason::Directories);
        }
    }

    pub(crate) fn try_start_directory(&self) -> bool {
        if !self.track_progress && self.max_directories.is_none() {
            return true;
        }

        let directories = saturating_fetch_add(&self.directories_started, 1);
        if self
            .max_directories
            .is_some_and(|max_directories| directories > max_directories)
        {
            self.cancel_for(ScannerCycleBudgetReason::Directories);
            return false;
        }

        true
    }

    pub(crate) fn record_object_scanned(&self) {
        if !self.track_progress && self.max_objects.is_none() {
            return;
        }

        let objects = saturating_fetch_add(&self.objects_scanned, 1);
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

impl Drop for ScannerCycleBudget {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
