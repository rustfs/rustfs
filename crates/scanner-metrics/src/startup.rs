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

//! Node-local startup observations; none of these fields certify scan coverage.

use crate::metrics::unix_now_secs;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerStartupPhase {
    Initializing,
    UsageInspection,
    Waiting,
    Attempted,
    Stopped,
    #[default]
    #[serde(other)]
    Unknown,
}

impl ScannerStartupPhase {
    /// Stable Prometheus codes; zero also covers reports from older binaries.
    pub fn code(self) -> u64 {
        match self {
            Self::Unknown => 0,
            Self::Initializing => 1,
            Self::UsageInspection => 2,
            Self::Waiting => 3,
            Self::Attempted => 4,
            Self::Stopped => 5,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerStartupStatus {
    /// The latest observed runtime still owns its guard; this is not a heartbeat.
    pub runtime_active: bool,
    pub phase: ScannerStartupPhase,
    pub started_at_unix_secs: u64,
    pub phase_started_at_unix_secs: u64,
    pub phase_age_seconds: u64,
    /// An estimate anchored to runtime start, never a scan-completion deadline.
    pub planned_attempt_at_unix_secs: Option<u64>,
    pub wait_remaining_seconds: u64,
    pub wait_overdue_seconds: u64,
    /// Entry to the normal scanner leadership/recovery loop after startup recovery.
    /// This is not an acquired lease or scan success.
    pub first_attempt_at_unix_secs: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannerStartupToken(u64);

#[derive(Debug)]
struct StartupState {
    generation: u64,
    status: ScannerStartupStatus,
    started: Instant,
    phase_started: Instant,
    deadline: Option<Instant>,
}

impl Default for StartupState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            generation: 0,
            status: ScannerStartupStatus::default(),
            started: now,
            phase_started: now,
            deadline: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ScannerStartupObservation {
    state: Mutex<StartupState>,
}

impl ScannerStartupObservation {
    /// Install a new owner atomically; older callbacks cannot overwrite it.
    pub fn begin(&self) -> ScannerStartupToken {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.generation = state.generation.wrapping_add(1);
        state.started = Instant::now();
        state.phase_started = state.started;
        let now = unix_now_secs();
        state.status = ScannerStartupStatus {
            runtime_active: true,
            phase: ScannerStartupPhase::Initializing,
            started_at_unix_secs: now,
            phase_started_at_unix_secs: now,
            ..Default::default()
        };
        state.deadline = None;
        ScannerStartupToken(state.generation)
    }

    fn update_startup(&self, token: ScannerStartupToken, update: impl FnOnce(&mut StartupState)) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.generation == token.0 && state.status.runtime_active && state.status.first_attempt_at_unix_secs.is_none() {
            update(&mut state);
        }
    }

    fn transition(state: &mut StartupState, phase: ScannerStartupPhase) {
        state.status.phase = phase;
        state.phase_started = Instant::now();
        state.status.phase_started_at_unix_secs = unix_now_secs();
        state.deadline = None;
        state.status.planned_attempt_at_unix_secs = None;
    }

    pub fn record_usage_inspection(&self, token: ScannerStartupToken) {
        self.update_startup(token, |state| {
            if state.status.phase != ScannerStartupPhase::UsageInspection {
                Self::transition(state, ScannerStartupPhase::UsageInspection);
            }
        });
    }

    /// Observe the scheduler's already sampled monotonic deadline without sampling again.
    pub fn record_wait(&self, token: ScannerStartupToken, deadline: Instant) {
        self.update_startup(token, |state| {
            if state.status.phase == ScannerStartupPhase::Waiting && state.deadline == Some(deadline) {
                return;
            }
            Self::transition(state, ScannerStartupPhase::Waiting);
            let since_start = deadline.saturating_duration_since(state.started);
            let rounded_seconds = since_start
                .as_secs()
                .saturating_add(u64::from(since_start.subsec_nanos() != 0));
            state.status.planned_attempt_at_unix_secs = Some(state.status.started_at_unix_secs.saturating_add(rounded_seconds));
            state.deadline = Some(deadline);
        });
    }

    pub fn record_attempt(&self, token: ScannerStartupToken) {
        self.update_startup(token, |state| {
            Self::transition(state, ScannerStartupPhase::Attempted);
            state.status.first_attempt_at_unix_secs = Some(unix_now_secs());
        });
    }

    pub fn finish(&self, token: ScannerStartupToken) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.generation == token.0 && state.status.runtime_active {
            Self::transition(&mut state, ScannerStartupPhase::Stopped);
            state.status.runtime_active = false;
        }
    }

    /// Derive timing on reads only; no background timer or status polling task is needed.
    pub fn snapshot(&self) -> ScannerStartupStatus {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut status = state.status;
        let now = Instant::now();
        if status.phase != ScannerStartupPhase::Unknown {
            status.phase_age_seconds = now.saturating_duration_since(state.phase_started).as_secs();
        }
        if let Some(deadline) = state.deadline {
            let remaining = deadline.saturating_duration_since(now);
            status.wait_remaining_seconds = remaining.as_secs().saturating_add(u64::from(remaining.subsec_nanos() != 0));
            status.wait_overdue_seconds = now.saturating_duration_since(deadline).as_secs();
        }
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Metrics, ScannerMetricsReport};
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn startup_snapshot_tracks_actual_deadline_and_only_real_attempts() {
        let metrics = Metrics::new();
        let observer = metrics.startup();
        assert_eq!(observer.snapshot(), ScannerStartupStatus::default());
        let token = observer.begin();
        let initial = observer.snapshot();
        assert!(initial.runtime_active);
        assert_eq!(initial.phase, ScannerStartupPhase::Initializing);
        assert!(initial.planned_attempt_at_unix_secs.is_none());
        assert!(initial.first_attempt_at_unix_secs.is_none());
        observer.record_usage_inspection(token);
        assert_eq!(observer.snapshot().phase, ScannerStartupPhase::UsageInspection);
        let deadline = Instant::now() + Duration::from_secs(120);
        observer.record_wait(token, deadline);
        let waiting = observer.snapshot();
        assert_eq!(waiting.phase, ScannerStartupPhase::Waiting);
        assert_eq!(waiting.wait_remaining_seconds, 120);
        assert_eq!(waiting.planned_attempt_at_unix_secs, Some(initial.started_at_unix_secs + 120));
        tokio::time::advance(Duration::from_secs(30)).await;
        observer.record_wait(token, deadline);
        let unchanged = observer.snapshot();
        assert_eq!(unchanged.wait_remaining_seconds, 90);
        assert_eq!(unchanged.phase_age_seconds, 30);
        assert_eq!(unchanged.planned_attempt_at_unix_secs, waiting.planned_attempt_at_unix_secs);
        tokio::time::advance(Duration::from_secs(95)).await;
        let overdue = observer.snapshot();
        assert_eq!(overdue.wait_remaining_seconds, 0);
        assert_eq!(overdue.wait_overdue_seconds, 5);
        assert!(overdue.first_attempt_at_unix_secs.is_none());
        observer.record_attempt(token);
        let attempted = observer.snapshot();
        assert_eq!(attempted.phase, ScannerStartupPhase::Attempted);
        assert!(attempted.first_attempt_at_unix_secs.is_some());
        assert!(attempted.planned_attempt_at_unix_secs.is_none());
        assert_eq!(attempted.wait_overdue_seconds, 0);
        observer.record_attempt(token);
        observer.record_wait(token, Instant::now() + Duration::from_secs(1000));
        assert_eq!(observer.snapshot().first_attempt_at_unix_secs, attempted.first_attempt_at_unix_secs);
        assert_eq!(observer.snapshot().phase, ScannerStartupPhase::Attempted);
        let report = metrics.report().await;
        assert!(!report.current_cycle_active, "attempt observation cannot invent an active cycle");
        assert_eq!(report.usage_freshness.last_durable_success_unix_secs, 0);
        observer.finish(token);
        let stopped = observer.snapshot();
        assert_eq!(stopped.phase, ScannerStartupPhase::Stopped);
        assert!(!stopped.runtime_active);
        assert_eq!(stopped.first_attempt_at_unix_secs, attempted.first_attempt_at_unix_secs);
    }

    #[tokio::test(start_paused = true)]
    async fn old_or_stopped_runtime_cannot_modify_the_current_snapshot() {
        let observer = ScannerStartupObservation::default();
        let old = observer.begin();
        observer.record_wait(old, Instant::now() + Duration::from_secs(10));
        let current = observer.begin();
        observer.record_wait(current, Instant::now() + Duration::from_secs(100));
        let expected = observer.snapshot();
        observer.record_attempt(old);
        observer.record_usage_inspection(old);
        observer.finish(old);
        assert_eq!(observer.snapshot(), expected);
        observer.finish(current);
        let stopped = observer.snapshot();
        assert!(!stopped.runtime_active);
        assert_eq!(stopped.phase, ScannerStartupPhase::Stopped);
        assert!(stopped.planned_attempt_at_unix_secs.is_none());
        observer.record_attempt(current);
        observer.record_wait(current, Instant::now() + Duration::from_secs(200));
        observer.finish(current);
        assert_eq!(observer.snapshot(), stopped);
    }

    #[tokio::test]
    async fn reports_default_missing_startup_fields_to_unknown() {
        let metrics = Metrics::new();
        let mut value = serde_json::to_value(metrics.report().await).expect("scanner report");
        value.as_object_mut().expect("report object").remove("startup");
        let legacy: ScannerMetricsReport = serde_json::from_value(value).expect("legacy report without startup");
        assert_eq!(legacy.startup, ScannerStartupStatus::default());
        let future: ScannerStartupStatus = serde_json::from_str(r#"{"phase":"future_phase"}"#).expect("unknown future phase");
        assert_eq!(future.phase, ScannerStartupPhase::Unknown);
        assert_eq!(future.phase.code(), 0);
    }
}
