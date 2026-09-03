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

//! Durable operational accounting for scanner pauses and bounded catch-up.
//!
//! This ledger is replicated outside the authoritative data-usage publication
//! path so it can advance while that path is fenced by data movement. It never
//! grants publication admission; scanner usage still passes the storage-owned
//! movement epoch and final publication fences.

use super::ScannerCycleOutcome;
use crate::data_usage_define::DataUsageCacheRevision;
use crate::storage_api::ScannerStorage;
use crate::storage_api::owner::ObjectIO as _;
use crate::{
    BUCKET_META_PREFIX, ECStore, EcstoreError, RUSTFS_META_BUCKET, ScannerObjectOptions, SetDisks, save_config_with_preconditions,
};
use futures::future::join_all;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

const SCANNER_PAUSE_BACKLOG_SCHEMA_VERSION: u16 = 1;
const SCANNER_PAUSE_BACKLOG_REPLICA_SCHEMA_VERSION: u16 = 1;
const SCANNER_PAUSE_BACKLOG_OBJECT: &str = ".scanner-pause-backlog.json";
const MAX_SCANNER_PAUSE_BACKLOG_BYTES: u64 = 64 * 1024;
const SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS: u64 = 5 * 60;
const SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS: u64 = 5 * 60;
const SCANNER_CATCH_UP_WINDOW_SECONDS: u64 = 60 * 60;
const SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW: u32 = 4;
const SCANNER_CATCH_UP_FAILURE_LIMIT: u32 = 5;
const SCANNER_CATCH_UP_EXHAUSTED_PROBE_SECONDS: u64 = 60 * 60;
const SCANNER_PAUSE_DURATION_ALERT_SECONDS: u64 = 24 * 60 * 60;
const SCANNER_PAUSE_DEFERRED_CYCLES_ALERT: u64 = 3;
const SCANNER_PAUSE_BACKLOG_ITEMS_ALERT: u64 = 10_000;

const METRIC_SCANNER_PAUSE_BACKLOG_PHASE: &str = "rustfs_scanner_pause_backlog_phase";
const METRIC_SCANNER_PAUSE_BACKLOG_PAUSE_DURATION_SECONDS: &str = "rustfs_scanner_pause_backlog_pause_duration_seconds";
const METRIC_SCANNER_PAUSE_BACKLOG_PENDING_WORK_ITEMS: &str = "rustfs_scanner_pause_backlog_pending_work_items";
const METRIC_SCANNER_PAUSE_BACKLOG_CONSECUTIVE_FAILURES: &str = "rustfs_scanner_pause_backlog_consecutive_failures";
const METRIC_SCANNER_PAUSE_BACKLOG_RATE_LIMITED: &str = "rustfs_scanner_pause_backlog_rate_limited";
const METRIC_SCANNER_PAUSE_BACKLOG_RETRY_EXHAUSTED: &str = "rustfs_scanner_pause_backlog_retry_exhausted";
const METRIC_SCANNER_PAUSE_BACKLOG_ALERTING: &str = "rustfs_scanner_pause_backlog_alerting";
const METRIC_SCANNER_PAUSE_BACKLOG_REPLICA_DEGRADED: &str = "rustfs_scanner_pause_backlog_replica_degraded";

static SCANNER_PAUSE_BACKLOG_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}/{SCANNER_PAUSE_BACKLOG_OBJECT}"));
static SCANNER_PAUSE_BACKLOG_RUNTIME_ERROR: LazyLock<RwLock<Option<String>>> = LazyLock::new(|| RwLock::new(None));

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerPauseBacklogPhase {
    #[default]
    Idle,
    Paused,
    CatchingUp,
    RetryExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerPauseBacklogAlertReason {
    PauseDurationThreshold,
    DeferredCyclesThreshold,
    BacklogItemsThreshold,
    RetryBudgetExhausted,
    CounterExhausted,
    ReplicaDegraded,
    PersistenceUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ScannerPauseBacklogThresholds {
    pub pause_duration_seconds: u64,
    pub deferred_cycles: u64,
    pub backlog_work_items: u64,
    pub catch_up_min_interval_seconds: u64,
    pub catch_up_window_seconds: u64,
    pub catch_up_max_attempts_per_window: u32,
    pub catch_up_failure_limit: u32,
    pub retry_exhausted_probe_seconds: u64,
}

impl Default for ScannerPauseBacklogThresholds {
    fn default() -> Self {
        Self {
            pause_duration_seconds: SCANNER_PAUSE_DURATION_ALERT_SECONDS,
            deferred_cycles: SCANNER_PAUSE_DEFERRED_CYCLES_ALERT,
            backlog_work_items: SCANNER_PAUSE_BACKLOG_ITEMS_ALERT,
            catch_up_min_interval_seconds: SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS,
            catch_up_window_seconds: SCANNER_CATCH_UP_WINDOW_SECONDS,
            catch_up_max_attempts_per_window: SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW,
            catch_up_failure_limit: SCANNER_CATCH_UP_FAILURE_LIMIT,
            retry_exhausted_probe_seconds: SCANNER_CATCH_UP_EXHAUSTED_PROBE_SECONDS,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScannerPauseBacklogStatus {
    pub path: String,
    pub persistence_state: String,
    pub durable: bool,
    pub schema_version: u16,
    pub generation: u64,
    pub writer_epoch: u64,
    pub phase: ScannerPauseBacklogPhase,
    pub movement_generation: u64,
    pub movement_work_items: u64,
    pub pause_started_at_unix_secs: u64,
    pub pause_ended_at_unix_secs: u64,
    pub pause_duration_seconds: u64,
    pub last_updated_at_unix_secs: u64,
    pub deferred_cycles: u64,
    pub pending_full_scan: bool,
    pub dirty_usage_buckets: u64,
    pub discovered_expiry_items: u64,
    pub discovered_transition_items: u64,
    pub pending_work_items: u64,
    pub catch_up_attempts: u64,
    pub consecutive_failures: u32,
    pub attempts_in_current_window: u32,
    pub current_window_started_at_unix_secs: u64,
    pub last_attempt_at_unix_secs: u64,
    pub next_attempt_at_unix_secs: u64,
    pub rate_limited: bool,
    pub retry_exhausted: bool,
    pub replica_count: usize,
    pub healthy_replicas: usize,
    pub stale_or_unavailable_replicas: usize,
    pub alerting: bool,
    pub alert_reasons: Vec<ScannerPauseBacklogAlertReason>,
    pub thresholds: ScannerPauseBacklogThresholds,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScannerPauseBacklogLedger {
    schema_version: u16,
    generation: u64,
    writer_epoch: u64,
    phase: ScannerPauseBacklogPhase,
    movement_generation: u64,
    movement_work_items: u64,
    pause_started_at_unix_secs: u64,
    pause_ended_at_unix_secs: u64,
    last_updated_at_unix_secs: u64,
    deferred_cycles: u64,
    pending_full_scan: bool,
    dirty_usage_buckets: u64,
    discovered_expiry_items: u64,
    discovered_transition_items: u64,
    catch_up_attempts: u64,
    consecutive_failures: u32,
    current_window_started_at_unix_secs: u64,
    attempts_in_current_window: u32,
    last_attempt_at_unix_secs: u64,
    next_attempt_at_unix_secs: u64,
    current_attempt_serial: u64,
    last_finished_attempt_serial: u64,
    counter_exhausted: bool,
}

impl Default for ScannerPauseBacklogLedger {
    fn default() -> Self {
        Self {
            schema_version: SCANNER_PAUSE_BACKLOG_SCHEMA_VERSION,
            generation: 0,
            writer_epoch: 0,
            phase: ScannerPauseBacklogPhase::Idle,
            movement_generation: 0,
            movement_work_items: 0,
            pause_started_at_unix_secs: 0,
            pause_ended_at_unix_secs: 0,
            last_updated_at_unix_secs: 0,
            deferred_cycles: 0,
            pending_full_scan: false,
            dirty_usage_buckets: 0,
            discovered_expiry_items: 0,
            discovered_transition_items: 0,
            catch_up_attempts: 0,
            consecutive_failures: 0,
            current_window_started_at_unix_secs: 0,
            attempts_in_current_window: 0,
            last_attempt_at_unix_secs: 0,
            next_attempt_at_unix_secs: 0,
            current_attempt_serial: 0,
            last_finished_attempt_serial: 0,
            counter_exhausted: false,
        }
    }
}

impl ScannerPauseBacklogLedger {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != SCANNER_PAUSE_BACKLOG_SCHEMA_VERSION {
            return Err(format!("unsupported scanner pause backlog schema {}", self.schema_version));
        }
        if self.generation == 0 || self.writer_epoch == 0 || self.last_updated_at_unix_secs == 0 {
            return Err("scanner pause backlog has an invalid durable fence".to_string());
        }
        if self.last_finished_attempt_serial > self.current_attempt_serial {
            return Err("scanner pause backlog finished attempt exceeds the current attempt".to_string());
        }
        if self.attempts_in_current_window > SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW {
            return Err("scanner pause backlog rate window exceeds its attempt limit".to_string());
        }
        if self.phase != ScannerPauseBacklogPhase::Idle && self.last_attempt_at_unix_secs > self.next_attempt_at_unix_secs {
            return Err("scanner pause backlog next attempt precedes its last attempt".to_string());
        }
        if self.phase == ScannerPauseBacklogPhase::Paused && !self.pending_full_scan {
            return Err("scanner pause backlog lost its required post-pause scan".to_string());
        }
        if self.phase == ScannerPauseBacklogPhase::Paused && self.pause_started_at_unix_secs == 0 {
            return Err("scanner pause backlog has no pause start time".to_string());
        }
        if matches!(
            self.phase,
            ScannerPauseBacklogPhase::CatchingUp | ScannerPauseBacklogPhase::RetryExhausted
        ) && (self.pause_started_at_unix_secs == 0 || self.pause_ended_at_unix_secs == 0)
        {
            return Err("scanner pause backlog has incomplete catch-up timestamps".to_string());
        }
        if self.pause_ended_at_unix_secs != 0 && self.pause_ended_at_unix_secs < self.pause_started_at_unix_secs {
            return Err("scanner pause backlog ends before its pause start".to_string());
        }
        if self.phase == ScannerPauseBacklogPhase::Idle && self.pending_full_scan {
            return Err("idle scanner pause backlog still requires a full scan".to_string());
        }
        Ok(())
    }

    fn pending_work_items(&self) -> u64 {
        (if self.pending_full_scan { 1_u64 } else { 0 })
            .saturating_add(self.dirty_usage_buckets)
            .saturating_add(self.discovered_expiry_items)
            .saturating_add(self.discovered_transition_items)
    }

    fn pause_duration_seconds(&self, now: u64) -> u64 {
        if self.pause_started_at_unix_secs == 0 {
            return 0;
        }
        let end = if self.phase == ScannerPauseBacklogPhase::Paused {
            now
        } else {
            self.pause_ended_at_unix_secs
        };
        end.saturating_sub(self.pause_started_at_unix_secs)
    }

    fn has_unfinished_attempt(&self) -> bool {
        self.current_attempt_serial > self.last_finished_attempt_serial
    }

    fn rate_limit_floor(&self) -> u64 {
        let mut floor = self.next_attempt_at_unix_secs;
        if self.last_attempt_at_unix_secs > 0 {
            floor = floor.max(
                self.last_attempt_at_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS),
            );
        }
        if self.current_window_started_at_unix_secs > 0
            && self.attempts_in_current_window >= SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW
        {
            floor = floor.max(
                self.current_window_started_at_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_WINDOW_SECONDS),
            );
        }
        floor
    }

    fn claim_writer(&mut self, now: u64) -> Result<(), String> {
        if self.has_unfinished_attempt() {
            self.last_finished_attempt_serial = self.current_attempt_serial;
            if matches!(
                self.phase,
                ScannerPauseBacklogPhase::CatchingUp | ScannerPauseBacklogPhase::RetryExhausted
            ) {
                increment_u32(&mut self.consecutive_failures, &mut self.counter_exhausted);
                self.exhaust_retry_budget(now);
            }
        }
        self.writer_epoch = self
            .writer_epoch
            .checked_add(1)
            .ok_or_else(|| "scanner pause backlog writer epoch is exhausted".to_string())?;
        Ok(())
    }

    fn apply_observation(&mut self, observation: ScannerPauseBacklogObservation) {
        if !observation.paused
            && observation.movement_generation != 0
            && observation.movement_generation != self.movement_generation
        {
            self.apply_observation(ScannerPauseBacklogObservation {
                paused: true,
                pause_started_at_unix_secs: observation.now_unix_secs,
                ..observation
            });
            self.apply_observation(observation);
            return;
        }
        if !observation.paused && self.phase == ScannerPauseBacklogPhase::Idle {
            self.movement_generation = observation.movement_generation;
            self.movement_work_items = 0;
            return;
        }
        self.movement_work_items = observation.movement_work_items;
        self.dirty_usage_buckets = observation.dirty_usage_buckets;
        self.discovered_expiry_items = observation.discovered_expiry_items;
        self.discovered_transition_items = observation.discovered_transition_items;

        if observation.paused {
            let new_pause =
                self.phase != ScannerPauseBacklogPhase::Paused || self.movement_generation != observation.movement_generation;
            if new_pause {
                increment_u64(&mut self.deferred_cycles, &mut self.counter_exhausted);
                self.pause_started_at_unix_secs = if observation.pause_started_at_unix_secs == 0 {
                    observation.now_unix_secs
                } else {
                    observation.pause_started_at_unix_secs
                };
                self.pause_ended_at_unix_secs = 0;
                self.consecutive_failures = 0;
            } else if observation.pause_started_at_unix_secs > 0 {
                self.pause_started_at_unix_secs = self.pause_started_at_unix_secs.min(observation.pause_started_at_unix_secs);
            }
            self.phase = ScannerPauseBacklogPhase::Paused;
            self.movement_generation = observation.movement_generation;
            self.pending_full_scan = true;
            self.next_attempt_at_unix_secs = self.rate_limit_floor();
            return;
        }

        self.movement_generation = observation.movement_generation;
        if self.phase == ScannerPauseBacklogPhase::Paused {
            self.phase = ScannerPauseBacklogPhase::CatchingUp;
            self.pause_ended_at_unix_secs = observation.now_unix_secs.max(self.pause_started_at_unix_secs);
            self.pending_full_scan = true;
            self.consecutive_failures = 0;
            self.next_attempt_at_unix_secs = self.next_attempt_at_unix_secs.max(observation.now_unix_secs);
        } else if matches!(
            self.phase,
            ScannerPauseBacklogPhase::CatchingUp | ScannerPauseBacklogPhase::RetryExhausted
        ) && !self.pending_full_scan
            && self.pending_work_items() == 0
        {
            self.phase = ScannerPauseBacklogPhase::Idle;
            self.deferred_cycles = 0;
            self.consecutive_failures = 0;
            self.next_attempt_at_unix_secs = 0;
        }
    }

    fn begin_attempt(&mut self, now: u64) -> ScannerPauseBacklogAttemptDecision {
        if !matches!(
            self.phase,
            ScannerPauseBacklogPhase::CatchingUp | ScannerPauseBacklogPhase::RetryExhausted
        ) {
            return ScannerPauseBacklogAttemptDecision::Untracked;
        }
        if !self.pending_full_scan && self.pending_work_items() == 0 {
            self.next_attempt_at_unix_secs = now.saturating_add(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS);
            return ScannerPauseBacklogAttemptDecision::RateLimited;
        }
        if self.has_unfinished_attempt() || now < self.next_attempt_at_unix_secs {
            return ScannerPauseBacklogAttemptDecision::RateLimited;
        }

        if self.phase == ScannerPauseBacklogPhase::CatchingUp {
            let window_end = self
                .current_window_started_at_unix_secs
                .saturating_add(SCANNER_CATCH_UP_WINDOW_SECONDS);
            if self.current_window_started_at_unix_secs == 0 || now >= window_end {
                self.current_window_started_at_unix_secs = now;
                self.attempts_in_current_window = 0;
            }
            if self.attempts_in_current_window >= SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW {
                self.next_attempt_at_unix_secs = self
                    .current_window_started_at_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_WINDOW_SECONDS);
                return ScannerPauseBacklogAttemptDecision::RateLimited;
            }
        }

        let Some(serial) = self.current_attempt_serial.checked_add(1) else {
            self.counter_exhausted = true;
            self.phase = ScannerPauseBacklogPhase::RetryExhausted;
            self.next_attempt_at_unix_secs = now.saturating_add(SCANNER_CATCH_UP_EXHAUSTED_PROBE_SECONDS);
            return ScannerPauseBacklogAttemptDecision::RateLimited;
        };
        self.current_attempt_serial = serial;
        increment_u64(&mut self.catch_up_attempts, &mut self.counter_exhausted);
        self.last_attempt_at_unix_secs = now;

        if self.phase == ScannerPauseBacklogPhase::CatchingUp {
            increment_u32(&mut self.attempts_in_current_window, &mut self.counter_exhausted);
            self.next_attempt_at_unix_secs = if self.attempts_in_current_window >= SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW {
                self.current_window_started_at_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_WINDOW_SECONDS)
            } else {
                now.saturating_add(SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS)
            };
        } else {
            self.next_attempt_at_unix_secs = now.saturating_add(SCANNER_CATCH_UP_EXHAUSTED_PROBE_SECONDS);
        }
        self.exhaust_retry_budget(now);

        ScannerPauseBacklogAttemptDecision::Tracked(serial)
    }

    fn finish_attempt(
        &mut self,
        serial: u64,
        outcome: ScannerPauseBacklogCycleOutcome,
        observation: ScannerPauseBacklogObservation,
    ) {
        if serial == 0 || serial != self.current_attempt_serial || serial <= self.last_finished_attempt_serial {
            return;
        }
        let movement_generation_advanced =
            observation.movement_generation != 0 && observation.movement_generation != self.movement_generation;
        let retry_exhausted_probe = self.phase == ScannerPauseBacklogPhase::RetryExhausted;
        let successful_probe = matches!(
            outcome,
            ScannerPauseBacklogCycleOutcome::Completed
                | ScannerPauseBacklogCycleOutcome::PendingMaintenance
                | ScannerPauseBacklogCycleOutcome::Progressed
        );
        let scheduled_retry_at = self.rate_limit_floor();
        let attempt_retry_at = if movement_generation_advanced || outcome == ScannerPauseBacklogCycleOutcome::DataMovementDeferred
        {
            scheduled_retry_at.max(
                observation
                    .now_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS),
            )
        } else {
            scheduled_retry_at
        };
        self.last_finished_attempt_serial = serial;
        self.observe_cycle_outcome(outcome, observation);
        let recovered_probe = retry_exhausted_probe && successful_probe && !self.counter_exhausted;
        if self.phase == ScannerPauseBacklogPhase::Paused {
            if recovered_probe {
                self.consecutive_failures = 0;
                self.current_window_started_at_unix_secs = observation.now_unix_secs;
                self.attempts_in_current_window = 0;
                self.next_attempt_at_unix_secs = observation
                    .now_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS);
            } else {
                self.next_attempt_at_unix_secs = self.next_attempt_at_unix_secs.max(attempt_retry_at);
            }
            return;
        }

        match outcome {
            ScannerPauseBacklogCycleOutcome::Completed => {
                self.consecutive_failures = 0;
                if movement_generation_advanced {
                    self.phase = ScannerPauseBacklogPhase::CatchingUp;
                    self.pending_full_scan = true;
                } else {
                    self.pending_full_scan = false;
                    if self.pending_work_items() == 0 {
                        self.phase = ScannerPauseBacklogPhase::Idle;
                        self.deferred_cycles = 0;
                        self.next_attempt_at_unix_secs = 0;
                    } else {
                        self.phase = ScannerPauseBacklogPhase::CatchingUp;
                        self.next_attempt_at_unix_secs = observation
                            .now_unix_secs
                            .saturating_add(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS);
                    }
                }
            }
            ScannerPauseBacklogCycleOutcome::Progressed => {
                self.consecutive_failures = 0;
                self.phase = ScannerPauseBacklogPhase::CatchingUp;
            }
            ScannerPauseBacklogCycleOutcome::PendingMaintenance => {
                self.consecutive_failures = 0;
                self.phase = ScannerPauseBacklogPhase::CatchingUp;
                self.pending_full_scan = true;
            }
            ScannerPauseBacklogCycleOutcome::DataMovementDeferred => {}
            ScannerPauseBacklogCycleOutcome::RetryableFailure => {
                increment_u32(&mut self.consecutive_failures, &mut self.counter_exhausted);
                self.exhaust_retry_budget(observation.now_unix_secs);
            }
        }
        if retry_exhausted_probe && !recovered_probe {
            self.phase = ScannerPauseBacklogPhase::RetryExhausted;
        }
        if self.phase != ScannerPauseBacklogPhase::Idle {
            if recovered_probe {
                self.current_window_started_at_unix_secs = observation.now_unix_secs;
                self.attempts_in_current_window = 0;
                self.next_attempt_at_unix_secs = observation
                    .now_unix_secs
                    .saturating_add(SCANNER_CATCH_UP_MIN_INTERVAL_SECONDS);
            } else {
                self.next_attempt_at_unix_secs = self.next_attempt_at_unix_secs.max(attempt_retry_at);
            }
        }
    }

    fn observe_cycle_outcome(&mut self, outcome: ScannerPauseBacklogCycleOutcome, observation: ScannerPauseBacklogObservation) {
        if outcome == ScannerPauseBacklogCycleOutcome::DataMovementDeferred && !observation.paused {
            self.apply_observation(ScannerPauseBacklogObservation {
                paused: true,
                pause_started_at_unix_secs: observation.now_unix_secs,
                ..observation
            });
        }
        self.apply_observation(observation);
    }

    fn exhaust_retry_budget(&mut self, now: u64) {
        if self.counter_exhausted || self.consecutive_failures >= SCANNER_CATCH_UP_FAILURE_LIMIT {
            self.phase = ScannerPauseBacklogPhase::RetryExhausted;
            self.next_attempt_at_unix_secs = now.saturating_add(SCANNER_CATCH_UP_EXHAUSTED_PROBE_SECONDS);
        }
    }

    fn alert_reasons(
        &self,
        now: u64,
        replica_degraded: bool,
        persistence_unavailable: bool,
    ) -> Vec<ScannerPauseBacklogAlertReason> {
        let mut reasons = Vec::new();
        if self.phase != ScannerPauseBacklogPhase::Idle
            && self.pause_duration_seconds(now) >= SCANNER_PAUSE_DURATION_ALERT_SECONDS
        {
            reasons.push(ScannerPauseBacklogAlertReason::PauseDurationThreshold);
        }
        if self.phase != ScannerPauseBacklogPhase::Idle && self.deferred_cycles >= SCANNER_PAUSE_DEFERRED_CYCLES_ALERT {
            reasons.push(ScannerPauseBacklogAlertReason::DeferredCyclesThreshold);
        }
        if self.pending_work_items() >= SCANNER_PAUSE_BACKLOG_ITEMS_ALERT {
            reasons.push(ScannerPauseBacklogAlertReason::BacklogItemsThreshold);
        }
        if self.phase == ScannerPauseBacklogPhase::RetryExhausted {
            reasons.push(ScannerPauseBacklogAlertReason::RetryBudgetExhausted);
        }
        if self.counter_exhausted {
            reasons.push(ScannerPauseBacklogAlertReason::CounterExhausted);
        }
        if replica_degraded {
            reasons.push(ScannerPauseBacklogAlertReason::ReplicaDegraded);
        }
        if persistence_unavailable {
            reasons.push(ScannerPauseBacklogAlertReason::PersistenceUnavailable);
        }
        reasons
    }
}

fn increment_u64(value: &mut u64, exhausted: &mut bool) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    } else {
        *exhausted = true;
    }
}

fn increment_u32(value: &mut u32, exhausted: &mut bool) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    } else {
        *exhausted = true;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScannerPauseBacklogObservation {
    pub(super) now_unix_secs: u64,
    pub(super) paused: bool,
    pub(super) movement_generation: u64,
    pub(super) movement_work_items: u64,
    pub(super) pause_started_at_unix_secs: u64,
    pub(super) dirty_usage_buckets: u64,
    pub(super) discovered_expiry_items: u64,
    pub(super) discovered_transition_items: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerPauseBacklogCycleOutcome {
    Completed,
    PendingMaintenance,
    Progressed,
    DataMovementDeferred,
    RetryableFailure,
}

impl From<ScannerCycleOutcome> for ScannerPauseBacklogCycleOutcome {
    fn from(outcome: ScannerCycleOutcome) -> Self {
        match outcome {
            ScannerCycleOutcome::Completed => Self::Completed,
            ScannerCycleOutcome::CompletedWithPendingMaintenance => Self::PendingMaintenance,
            ScannerCycleOutcome::Partial => Self::Progressed,
            ScannerCycleOutcome::Deferred(super::ScannerCycleDeferReason::DataMovement) => Self::DataMovementDeferred,
            ScannerCycleOutcome::Superseded | ScannerCycleOutcome::Deferred(_) | ScannerCycleOutcome::Failed => {
                Self::RetryableFailure
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScannerPauseBacklogAttemptDecision {
    Untracked,
    RateLimited,
    Tracked(u64),
    PersistenceUnavailable,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
struct ScannerPauseBacklogReplicaId {
    pool_index: usize,
    set_index: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScannerPauseBacklogCommitRecord {
    ledger: ScannerPauseBacklogLedger,
    replicas: Vec<ScannerPauseBacklogReplicaId>,
}

impl ScannerPauseBacklogCommitRecord {
    fn new(ledger: ScannerPauseBacklogLedger, replicas: Vec<ScannerPauseBacklogReplicaId>) -> Self {
        Self { ledger, replicas }
    }

    fn validate(&self) -> Result<(), String> {
        self.ledger.validate()?;
        if self.replicas.is_empty() || !self.replicas.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err("scanner pause backlog commit has invalid replica membership".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScannerPauseBacklogReplicaRecord {
    replica_schema_version: u16,
    /// Last generation known to be safe without consulting commit records.
    stable: Option<ScannerPauseBacklogLedger>,
    /// Candidate authority only when every surviving set stores it exactly.
    committed: Option<ScannerPauseBacklogCommitRecord>,
}

impl ScannerPauseBacklogReplicaRecord {
    fn new(stable: Option<ScannerPauseBacklogLedger>, committed: Option<ScannerPauseBacklogCommitRecord>) -> Self {
        Self {
            replica_schema_version: SCANNER_PAUSE_BACKLOG_REPLICA_SCHEMA_VERSION,
            stable,
            committed,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.replica_schema_version != SCANNER_PAUSE_BACKLOG_REPLICA_SCHEMA_VERSION {
            return Err(format!(
                "unsupported scanner pause backlog replica schema {}",
                self.replica_schema_version
            ));
        }
        if self.stable.is_none() && self.committed.is_none() {
            return Err("scanner pause backlog replica has no durable state".to_string());
        }
        if let Some(stable) = &self.stable {
            stable.validate()?;
        }
        if let Some(committed) = &self.committed {
            committed.validate()?;
            if let Some(stable) = &self.stable {
                let stable_key = (stable.writer_epoch, stable.generation);
                let committed_key = (committed.ledger.writer_epoch, committed.ledger.generation);
                if committed_key < stable_key || (committed_key == stable_key && &committed.ledger != stable) {
                    return Err("scanner pause backlog commit precedes or diverges from its stable generation".to_string());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum ScannerPauseBacklogReplicaState {
    Missing,
    Valid(Box<ScannerPauseBacklogReplicaRecord>),
    Invalid(String),
    FutureSchema(u64),
    Unavailable(String),
}

#[derive(Clone)]
struct ScannerPauseBacklogReplica {
    id: ScannerPauseBacklogReplicaId,
    revision: Option<DataUsageCacheRevision>,
    state: ScannerPauseBacklogReplicaState,
}

#[derive(Clone)]
struct LoadedScannerPauseBacklog {
    ledger: ScannerPauseBacklogLedger,
    durable: bool,
    persistence_state: String,
    replicas: Vec<ScannerPauseBacklogReplica>,
    replica_count: usize,
    healthy_replicas: usize,
    stale_or_unavailable_replicas: usize,
    stable_matches_ledger: bool,
    authoritative_commit: Option<ScannerPauseBacklogCommitRecord>,
    requires_reload: bool,
}

impl LoadedScannerPauseBacklog {
    fn status(&self, now: u64, error: Option<String>) -> ScannerPauseBacklogStatus {
        let persistence_unavailable = error.is_some();
        let replica_degraded = self.durable && self.stale_or_unavailable_replicas > 0;
        status_from_ledger(
            &self.ledger,
            now,
            self.persistence_state.clone(),
            self.durable,
            self.replica_count,
            self.healthy_replicas,
            self.stale_or_unavailable_replicas,
            replica_degraded,
            persistence_unavailable,
            error,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn status_from_ledger(
    ledger: &ScannerPauseBacklogLedger,
    now: u64,
    persistence_state: String,
    durable: bool,
    replica_count: usize,
    healthy_replicas: usize,
    stale_or_unavailable_replicas: usize,
    replica_degraded: bool,
    persistence_unavailable: bool,
    error: Option<String>,
) -> ScannerPauseBacklogStatus {
    let alert_reasons = ledger.alert_reasons(now, replica_degraded, persistence_unavailable);
    let status = ScannerPauseBacklogStatus {
        path: SCANNER_PAUSE_BACKLOG_PATH.clone(),
        persistence_state,
        durable,
        schema_version: ledger.schema_version,
        generation: ledger.generation,
        writer_epoch: ledger.writer_epoch,
        phase: ledger.phase,
        movement_generation: ledger.movement_generation,
        movement_work_items: ledger.movement_work_items,
        pause_started_at_unix_secs: ledger.pause_started_at_unix_secs,
        pause_ended_at_unix_secs: ledger.pause_ended_at_unix_secs,
        pause_duration_seconds: ledger.pause_duration_seconds(now),
        last_updated_at_unix_secs: ledger.last_updated_at_unix_secs,
        deferred_cycles: ledger.deferred_cycles,
        pending_full_scan: ledger.pending_full_scan,
        dirty_usage_buckets: ledger.dirty_usage_buckets,
        discovered_expiry_items: ledger.discovered_expiry_items,
        discovered_transition_items: ledger.discovered_transition_items,
        pending_work_items: ledger.pending_work_items(),
        catch_up_attempts: ledger.catch_up_attempts,
        consecutive_failures: ledger.consecutive_failures,
        attempts_in_current_window: ledger.attempts_in_current_window,
        current_window_started_at_unix_secs: ledger.current_window_started_at_unix_secs,
        last_attempt_at_unix_secs: ledger.last_attempt_at_unix_secs,
        next_attempt_at_unix_secs: ledger.next_attempt_at_unix_secs,
        rate_limited: ledger.has_unfinished_attempt()
            || matches!(
                ledger.phase,
                ScannerPauseBacklogPhase::CatchingUp | ScannerPauseBacklogPhase::RetryExhausted
            ) && now < ledger.next_attempt_at_unix_secs,
        retry_exhausted: ledger.phase == ScannerPauseBacklogPhase::RetryExhausted,
        replica_count,
        healthy_replicas,
        stale_or_unavailable_replicas,
        alerting: !alert_reasons.is_empty(),
        alert_reasons,
        thresholds: ScannerPauseBacklogThresholds::default(),
        error,
    };
    record_scanner_pause_backlog_status(&status);
    status
}

fn record_scanner_pause_backlog_status(status: &ScannerPauseBacklogStatus) {
    let phase: u32 = match status.phase {
        ScannerPauseBacklogPhase::Idle => 0,
        ScannerPauseBacklogPhase::Paused => 1,
        ScannerPauseBacklogPhase::CatchingUp => 2,
        ScannerPauseBacklogPhase::RetryExhausted => 3,
    };
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_PHASE).set(f64::from(phase));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_PAUSE_DURATION_SECONDS).set(metric_u64(status.pause_duration_seconds));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_PENDING_WORK_ITEMS).set(metric_u64(status.pending_work_items));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_CONSECUTIVE_FAILURES).set(f64::from(status.consecutive_failures));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_RATE_LIMITED).set(bool_metric(status.rate_limited));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_RETRY_EXHAUSTED).set(bool_metric(status.retry_exhausted));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_ALERTING).set(bool_metric(status.alerting));
    metrics::gauge!(METRIC_SCANNER_PAUSE_BACKLOG_REPLICA_DEGRADED)
        .set(bool_metric(status.durable && status.stale_or_unavailable_replicas > 0));
}

fn metric_u64(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn bool_metric(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

async fn read_scanner_pause_backlog_replica(store: Arc<SetDisks>) -> ScannerPauseBacklogReplica {
    let id = ScannerPauseBacklogReplicaId {
        pool_index: store.pool_index,
        set_index: store.set_index,
    };
    let reader = match store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            SCANNER_PAUSE_BACKLOG_PATH.as_str(),
            None,
            HeaderMap::new(),
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(
            EcstoreError::ConfigNotFound
            | EcstoreError::FileNotFound
            | EcstoreError::VolumeNotFound
            | EcstoreError::ObjectNotFound(_, _)
            | EcstoreError::BucketNotFound(_),
        ) => {
            return ScannerPauseBacklogReplica {
                id,
                revision: Some(DataUsageCacheRevision::Missing),
                state: ScannerPauseBacklogReplicaState::Missing,
            };
        }
        Err(err) => {
            return ScannerPauseBacklogReplica {
                id,
                revision: None,
                state: ScannerPauseBacklogReplicaState::Unavailable(err.to_string()),
            };
        }
    };

    let revision = reader
        .object_info
        .etag
        .as_ref()
        .filter(|etag| !etag.is_empty())
        .cloned()
        .map(DataUsageCacheRevision::Etag);
    let max_size = i64::try_from(MAX_SCANNER_PAUSE_BACKLOG_BYTES).unwrap_or(i64::MAX);
    if revision.is_none() || reader.object_info.is_dir || reader.object_info.size < 0 || reader.object_info.size > max_size {
        return ScannerPauseBacklogReplica {
            id,
            revision,
            state: ScannerPauseBacklogReplicaState::Invalid(
                "scanner pause backlog replica is oversized or has no revision".to_string(),
            ),
        };
    }

    let mut data = Vec::new();
    let max_len = usize::try_from(MAX_SCANNER_PAUSE_BACKLOG_BYTES).unwrap_or(usize::MAX);
    let read_result = reader
        .take(MAX_SCANNER_PAUSE_BACKLOG_BYTES.saturating_add(1))
        .read_to_end(&mut data)
        .await;
    let state = match read_result {
        Err(err) => ScannerPauseBacklogReplicaState::Unavailable(err.to_string()),
        Ok(_) if data.len() > max_len => {
            ScannerPauseBacklogReplicaState::Invalid("scanner pause backlog replica exceeds its size bound".to_string())
        }
        Ok(_) => decode_scanner_pause_backlog_ledger(&data),
    };
    ScannerPauseBacklogReplica { id, revision, state }
}

fn decode_scanner_pause_backlog_ledger(data: &[u8]) -> ScannerPauseBacklogReplicaState {
    let value = match serde_json::from_slice::<serde_json::Value>(data) {
        Ok(value) => value,
        Err(err) => return ScannerPauseBacklogReplicaState::Invalid(err.to_string()),
    };
    if let Some(version) = value.get("replica_schema_version").and_then(serde_json::Value::as_u64) {
        if version > u64::from(SCANNER_PAUSE_BACKLOG_REPLICA_SCHEMA_VERSION) {
            return ScannerPauseBacklogReplicaState::FutureSchema(version);
        }
        let record = match serde_json::from_value::<ScannerPauseBacklogReplicaRecord>(value) {
            Ok(record) => record,
            Err(err) => return ScannerPauseBacklogReplicaState::Invalid(err.to_string()),
        };
        return match record.validate() {
            Ok(()) => ScannerPauseBacklogReplicaState::Valid(Box::new(record)),
            Err(err) => ScannerPauseBacklogReplicaState::Invalid(err),
        };
    }

    let version = value.get("schema_version").and_then(serde_json::Value::as_u64).unwrap_or(0);
    if version > u64::from(SCANNER_PAUSE_BACKLOG_SCHEMA_VERSION) {
        return ScannerPauseBacklogReplicaState::FutureSchema(version);
    }
    let ledger = match serde_json::from_value::<ScannerPauseBacklogLedger>(value) {
        Ok(ledger) => ledger,
        Err(err) => return ScannerPauseBacklogReplicaState::Invalid(err.to_string()),
    };
    match ledger.validate() {
        Ok(()) => ScannerPauseBacklogReplicaState::Valid(Box::new(ScannerPauseBacklogReplicaRecord::new(Some(ledger), None))),
        Err(err) => ScannerPauseBacklogReplicaState::Invalid(err),
    }
}

fn scanner_pause_backlog_consensus<'a, T: Clone + PartialEq + 'a>(
    values: impl Iterator<Item = Option<&'a T>>,
) -> Result<Option<T>, ()> {
    let mut values = values;
    let first = values.next().ok_or(())?;
    if values.all(|candidate| candidate == first) {
        Ok(first.cloned())
    } else {
        Err(())
    }
}

fn scanner_pause_backlog_replica_ids(replicas: &[ScannerPauseBacklogReplica]) -> Vec<ScannerPauseBacklogReplicaId> {
    let mut ids = replicas.iter().map(|replica| replica.id).collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn select_scanner_pause_backlog_commit(
    replicas: &[ScannerPauseBacklogReplica],
    replica_ids: &[ScannerPauseBacklogReplicaId],
) -> Result<Option<ScannerPauseBacklogCommitRecord>, String> {
    let current_ids = replica_ids.iter().copied().collect::<BTreeSet<_>>();
    let replicas_by_id = replicas
        .iter()
        .map(|replica| (replica.id, replica))
        .collect::<HashMap<_, _>>();
    let mut valid = Vec::<ScannerPauseBacklogCommitRecord>::new();

    for committed in replicas.iter().filter_map(|replica| match &replica.state {
        ScannerPauseBacklogReplicaState::Valid(record) => record.committed.as_ref(),
        _ => None,
    }) {
        if valid.contains(committed)
            || !committed.replicas.iter().all(|id| {
                current_ids.contains(id)
                    && replicas_by_id.get(id).is_some_and(|replica| {
                        matches!(
                            &replica.state,
                            ScannerPauseBacklogReplicaState::Valid(record)
                                if record.committed.as_ref() == Some(committed)
                        )
                    })
            })
        {
            continue;
        }
        valid.push(committed.clone());
    }

    let Some(max_membership_len) = valid.iter().map(|committed| committed.replicas.len()).max() else {
        return Ok(None);
    };
    let mut largest = valid
        .into_iter()
        .filter(|committed| committed.replicas.len() == max_membership_len);
    let Some(mut selected) = largest.next() else {
        return Ok(None);
    };
    for committed in largest {
        if committed.ledger != selected.ledger {
            return Err("scanner pause backlog has conflicting maximum-membership commit proofs".to_string());
        }
        if committed.replicas < selected.replicas {
            selected = committed;
        }
    }
    Ok(Some(selected))
}

fn select_scanner_pause_backlog_replicas(replicas: Vec<ScannerPauseBacklogReplica>) -> Result<LoadedScannerPauseBacklog, String> {
    if replicas.is_empty() {
        return Err("scanner pause backlog has no storage replicas".to_string());
    }
    for replica in &replicas {
        if let ScannerPauseBacklogReplicaState::FutureSchema(version) = &replica.state {
            return Err(format!(
                "scanner pause backlog pool {} set {} uses future schema {version}",
                replica.id.pool_index, replica.id.set_index
            ));
        }
    }

    let replica_ids = scanner_pause_backlog_replica_ids(&replicas);
    let authoritative_commit = select_scanner_pause_backlog_commit(&replicas, &replica_ids)?;
    let stable_consensus = if replicas.iter().all(|replica| {
        matches!(
            &replica.state,
            ScannerPauseBacklogReplicaState::Valid(_) | ScannerPauseBacklogReplicaState::Missing
        )
    }) {
        scanner_pause_backlog_consensus(replicas.iter().map(|replica| match &replica.state {
            ScannerPauseBacklogReplicaState::Valid(record) => record.stable.as_ref(),
            ScannerPauseBacklogReplicaState::Missing => None,
            _ => unreachable!("replica states were checked above"),
        }))
    } else {
        Err(())
    };

    let selected = match &authoritative_commit {
        Some(committed) => Some(committed.ledger.clone()),
        None => {
            if let Some(replica) = replicas.iter().find(|replica| {
                matches!(
                    &replica.state,
                    ScannerPauseBacklogReplicaState::Invalid(_) | ScannerPauseBacklogReplicaState::Unavailable(_)
                )
            }) {
                let reason = match &replica.state {
                    ScannerPauseBacklogReplicaState::Invalid(reason) | ScannerPauseBacklogReplicaState::Unavailable(reason) => {
                        reason
                    }
                    _ => unreachable!("replica state was checked above"),
                };
                return Err(format!(
                    "scanner pause backlog pool {} set {} is unavailable: {reason}",
                    replica.id.pool_index, replica.id.set_index
                ));
            }
            match &stable_consensus {
                Ok(stable) => stable.clone(),
                Err(()) => {
                    return Err(
                        "scanner pause backlog has neither a surviving membership commit nor a stable rollback point".to_string(),
                    );
                }
            }
        }
    };
    let Some(selected) = selected else {
        let replica_count = replicas.len();
        return Ok(LoadedScannerPauseBacklog {
            ledger: ScannerPauseBacklogLedger::default(),
            durable: false,
            persistence_state: "missing".to_string(),
            replica_count,
            healthy_replicas: 0,
            stale_or_unavailable_replicas: replica_count,
            stable_matches_ledger: true,
            authoritative_commit: None,
            requires_reload: false,
            replicas,
        });
    };

    let stable_matches_ledger = matches!(&stable_consensus, Ok(Some(stable)) if stable == &selected);
    let healthy_replicas = replicas
        .iter()
        .filter(|replica| {
            matches!(
                &replica.state,
                ScannerPauseBacklogReplicaState::Valid(record)
                    if record.stable.as_ref() == Some(&selected)
                        && match &authoritative_commit {
                            Some(committed) => record.committed.as_ref() == Some(committed),
                            None => record.committed.is_none(),
                        }
            )
        })
        .count();
    let replica_count = replicas.len();
    let stale_or_unavailable_replicas = replica_count.saturating_sub(healthy_replicas);
    let membership_repair_pending = authoritative_commit
        .as_ref()
        .is_some_and(|committed| committed.replicas.as_slice() != replica_ids.as_slice());
    Ok(LoadedScannerPauseBacklog {
        ledger: selected,
        durable: true,
        persistence_state: if membership_repair_pending {
            "membership_repair_pending"
        } else if authoritative_commit.is_some() && !stable_matches_ledger {
            "committed_pending_stabilization"
        } else if stale_or_unavailable_replicas > 0 {
            "rolled_back_partial_commit"
        } else {
            "healthy"
        }
        .to_string(),
        replica_count,
        replicas,
        healthy_replicas,
        stale_or_unavailable_replicas,
        stable_matches_ledger,
        authoritative_commit,
        requires_reload: false,
    })
}

async fn load_scanner_pause_backlog<S>(storeapi: Arc<S>) -> Result<LoadedScannerPauseBacklog, String>
where
    S: ScannerStorage,
{
    let writable = storeapi.scanner_pause_backlog_writable_set_disks().await;
    if writable.is_empty() {
        return Err("scanner pause backlog has no surviving storage replicas".to_string());
    }
    let replicas = join_all(writable.into_iter().map(read_scanner_pause_backlog_replica)).await;
    select_scanner_pause_backlog_replicas(replicas)
}

async fn write_scanner_pause_backlog_record<S>(
    storeapi: Arc<S>,
    loaded: &LoadedScannerPauseBacklog,
    record: ScannerPauseBacklogReplicaRecord,
) -> Result<(), String>
where
    S: ScannerStorage,
{
    let data = serde_json::to_vec(&record).map_err(|err| format!("failed to encode scanner pause backlog: {err}"))?;
    if data.len() > usize::try_from(MAX_SCANNER_PAUSE_BACKLOG_BYTES).unwrap_or(usize::MAX) {
        return Err("scanner pause backlog exceeds its size bound".to_string());
    }

    let writable = storeapi.scanner_pause_backlog_writable_set_disks().await;
    if writable.is_empty() {
        return Err("scanner pause backlog has no surviving writable set".to_string());
    }
    let loaded_ids = loaded.replicas.iter().map(|replica| replica.id).collect::<BTreeSet<_>>();
    let writable_ids = writable
        .iter()
        .map(|set| ScannerPauseBacklogReplicaId {
            pool_index: set.pool_index,
            set_index: set.set_index,
        })
        .collect::<BTreeSet<_>>();
    if writable_ids != loaded_ids {
        return Err("scanner pause backlog replica topology changed during commit".to_string());
    }

    let revisions = loaded
        .replicas
        .iter()
        .filter_map(|replica| replica.revision.clone().map(|revision| (replica.id, revision)))
        .collect::<HashMap<_, _>>();
    let results = join_all(writable.into_iter().map(|set| {
        let id = ScannerPauseBacklogReplicaId {
            pool_index: set.pool_index,
            set_index: set.set_index,
        };
        let revision = revisions.get(&id).cloned();
        let data = data.clone();
        async move {
            let Some(revision) = revision else {
                return (id, Err("replica revision is unavailable".to_string()));
            };
            let result = save_config_with_preconditions(set, SCANNER_PAUSE_BACKLOG_PATH.as_str(), data, revision.preconditions())
                .await
                .map(|_| ())
                .map_err(|err| err.to_string());
            (id, result)
        }
    }))
    .await;

    let failures = results
        .iter()
        .filter_map(|(id, result)| {
            result
                .as_ref()
                .err()
                .map(|err| format!("pool {} set {}: {err}", id.pool_index, id.set_index))
        })
        .collect::<Vec<_>>();
    if !failures.is_empty() {
        return Err(format!(
            "scanner pause backlog commit did not reach every surviving set ({})",
            failures.join("; ")
        ));
    }
    Ok(())
}

async fn stabilize_scanner_pause_backlog<S>(
    storeapi: Arc<S>,
    loaded: &LoadedScannerPauseBacklog,
) -> Result<LoadedScannerPauseBacklog, String>
where
    S: ScannerStorage,
{
    let committed = loaded.authoritative_commit.clone().unwrap_or_else(|| {
        ScannerPauseBacklogCommitRecord::new(loaded.ledger.clone(), scanner_pause_backlog_replica_ids(&loaded.replicas))
    });
    let record = ScannerPauseBacklogReplicaRecord::new(Some(loaded.ledger.clone()), Some(committed));
    write_scanner_pause_backlog_record(storeapi.clone(), loaded, record).await?;
    let stabilized = load_scanner_pause_backlog(storeapi).await?;
    if stabilized.ledger != loaded.ledger || !stabilized.stable_matches_ledger {
        return Err("scanner pause backlog failed to stabilize its last committed generation".to_string());
    }
    Ok(stabilized)
}

fn committed_scanner_pause_backlog_pending_reload(
    ledger: ScannerPauseBacklogLedger,
    replica_count: usize,
) -> LoadedScannerPauseBacklog {
    LoadedScannerPauseBacklog {
        ledger,
        durable: true,
        persistence_state: "committed_reload_pending".to_string(),
        replicas: Vec::new(),
        replica_count,
        healthy_replicas: 0,
        stale_or_unavailable_replicas: replica_count,
        stable_matches_ledger: false,
        authoritative_commit: None,
        requires_reload: true,
    }
}

async fn persist_scanner_pause_backlog<S>(
    storeapi: Arc<S>,
    loaded: &LoadedScannerPauseBacklog,
    ledger: ScannerPauseBacklogLedger,
) -> Result<LoadedScannerPauseBacklog, String>
where
    S: ScannerStorage,
{
    let mut base = if loaded.requires_reload {
        let reloaded = load_scanner_pause_backlog(storeapi.clone()).await?;
        if reloaded.ledger != loaded.ledger {
            return Err(format!(
                "scanner pause backlog reload advanced to writer epoch {} generation {}",
                reloaded.ledger.writer_epoch, reloaded.ledger.generation
            ));
        }
        reloaded
    } else {
        loaded.clone()
    };
    if base.durable && !base.stable_matches_ledger {
        base = stabilize_scanner_pause_backlog(storeapi.clone(), &base).await?;
    }

    let replicas = scanner_pause_backlog_replica_ids(&base.replicas);
    let committed = ScannerPauseBacklogCommitRecord::new(ledger.clone(), replicas);
    let record = ScannerPauseBacklogReplicaRecord::new(base.durable.then_some(base.ledger.clone()), Some(committed));
    write_scanner_pause_backlog_record(storeapi.clone(), &base, record).await?;

    match load_scanner_pause_backlog(storeapi.clone()).await {
        Ok(committed) if committed.ledger == ledger => match stabilize_scanner_pause_backlog(storeapi, &committed).await {
            Ok(stabilized) => Ok(stabilized),
            Err(_) => Ok(committed_scanner_pause_backlog_pending_reload(ledger, base.replica_count)),
        },
        Ok(_) | Err(_) => Ok(committed_scanner_pause_backlog_pending_reload(ledger, base.replica_count)),
    }
}

pub(super) struct ScannerPauseBacklogController<S: ScannerStorage> {
    storeapi: Arc<S>,
    loaded: LoadedScannerPauseBacklog,
    persistence_disabled: bool,
    persistence_retry_at_unix_secs: u64,
}

impl<S> ScannerPauseBacklogController<S>
where
    S: ScannerStorage,
{
    pub(super) async fn claim(storeapi: Arc<S>, now: u64) -> Result<Self, String> {
        let loaded = load_scanner_pause_backlog(storeapi.clone()).await?;
        let mut ledger = loaded.ledger.clone();
        ledger.claim_writer(now)?;
        prepare_scanner_pause_backlog_persist(&mut ledger, now)?;
        let loaded = persist_scanner_pause_backlog(storeapi.clone(), &loaded, ledger).await?;
        set_runtime_error(None);
        let controller = Self {
            storeapi,
            loaded,
            persistence_disabled: false,
            persistence_retry_at_unix_secs: 0,
        };
        controller.record_status(now);
        Ok(controller)
    }

    pub(super) fn unavailable(storeapi: Arc<S>, error: String, now: u64) -> Self {
        set_runtime_error(Some(error));
        let loaded = LoadedScannerPauseBacklog {
            ledger: ScannerPauseBacklogLedger::default(),
            durable: false,
            persistence_state: "unavailable".to_string(),
            replicas: Vec::new(),
            replica_count: 0,
            healthy_replicas: 0,
            stale_or_unavailable_replicas: 0,
            stable_matches_ledger: true,
            authoritative_commit: None,
            requires_reload: false,
        };
        let controller = Self {
            storeapi,
            loaded,
            persistence_disabled: true,
            persistence_retry_at_unix_secs: now.saturating_add(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS),
        };
        controller.record_status(now);
        controller
    }

    pub(super) fn scheduling_delay(&self, now: u64) -> Option<Duration> {
        if self.persistence_disabled {
            return Some(Duration::from_secs(self.persistence_retry_at_unix_secs.saturating_sub(now)));
        }
        match self.loaded.ledger.phase {
            ScannerPauseBacklogPhase::Idle => None,
            ScannerPauseBacklogPhase::Paused => Some(Duration::from_secs(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS)),
            ScannerPauseBacklogPhase::CatchingUp => {
                Some(Duration::from_secs(self.loaded.ledger.next_attempt_at_unix_secs.saturating_sub(now)))
            }
            ScannerPauseBacklogPhase::RetryExhausted => {
                Some(Duration::from_secs(self.loaded.ledger.next_attempt_at_unix_secs.saturating_sub(now)))
            }
        }
    }

    pub(super) async fn observe(&mut self, observation: ScannerPauseBacklogObservation) {
        if !self.try_recover_persistence(observation.now_unix_secs).await {
            return;
        }
        self.persist_mutation(observation.now_unix_secs, |ledger| ledger.apply_observation(observation))
            .await;
    }

    pub(super) async fn begin_attempt(&mut self, now: u64) -> ScannerPauseBacklogAttemptDecision {
        if self.persistence_disabled {
            return ScannerPauseBacklogAttemptDecision::PersistenceUnavailable;
        }
        let mut candidate = self.loaded.ledger.clone();
        let decision = candidate.begin_attempt(now);
        if candidate == self.loaded.ledger {
            self.record_status(now);
            return decision;
        }
        if let Err(err) = prepare_scanner_pause_backlog_persist(&mut candidate, now) {
            self.disable_persistence(err, now);
            return ScannerPauseBacklogAttemptDecision::PersistenceUnavailable;
        }
        match persist_scanner_pause_backlog(self.storeapi.clone(), &self.loaded, candidate).await {
            Ok(loaded) => {
                self.loaded = loaded;
                set_runtime_error(None);
                self.record_status(now);
                decision
            }
            Err(err) => {
                self.disable_persistence(err, now);
                ScannerPauseBacklogAttemptDecision::PersistenceUnavailable
            }
        }
    }

    pub(super) async fn finish_attempt(
        &mut self,
        serial: u64,
        outcome: ScannerCycleOutcome,
        observation: ScannerPauseBacklogObservation,
    ) {
        self.persist_mutation(observation.now_unix_secs, |ledger| {
            ledger.finish_attempt(serial, outcome.into(), observation)
        })
        .await;
    }

    pub(super) async fn observe_cycle_outcome(
        &mut self,
        outcome: ScannerCycleOutcome,
        observation: ScannerPauseBacklogObservation,
    ) {
        self.persist_mutation(observation.now_unix_secs, |ledger| {
            ledger.observe_cycle_outcome(outcome.into(), observation)
        })
        .await;
    }

    async fn persist_mutation(&mut self, now: u64, mutate: impl FnOnce(&mut ScannerPauseBacklogLedger)) {
        if self.persistence_disabled {
            return;
        }
        let mut candidate = self.loaded.ledger.clone();
        mutate(&mut candidate);
        if candidate == self.loaded.ledger {
            self.record_status(now);
            return;
        }
        if let Err(err) = prepare_scanner_pause_backlog_persist(&mut candidate, now) {
            self.disable_persistence(err, now);
            return;
        }
        match persist_scanner_pause_backlog(self.storeapi.clone(), &self.loaded, candidate).await {
            Ok(loaded) => {
                self.loaded = loaded;
                set_runtime_error(None);
                self.record_status(now);
            }
            Err(err) => self.disable_persistence(err, now),
        }
    }

    fn disable_persistence(&mut self, error: String, now: u64) {
        self.persistence_disabled = true;
        self.persistence_retry_at_unix_secs = now.saturating_add(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS);
        set_runtime_error(Some(error));
        self.record_status(now);
    }

    async fn try_recover_persistence(&mut self, now: u64) -> bool {
        if !self.persistence_disabled {
            return true;
        }
        if now < self.persistence_retry_at_unix_secs {
            self.record_status(now);
            return false;
        }
        match Self::claim(self.storeapi.clone(), now).await {
            Ok(controller) => {
                *self = controller;
                true
            }
            Err(error) => {
                self.persistence_retry_at_unix_secs = now.saturating_add(SCANNER_PAUSE_REFRESH_INTERVAL_SECONDS);
                set_runtime_error(Some(error));
                self.record_status(now);
                false
            }
        }
    }

    fn record_status(&self, now: u64) {
        let _ = self.loaded.status(now, runtime_error());
    }
}

fn prepare_scanner_pause_backlog_persist(ledger: &mut ScannerPauseBacklogLedger, now: u64) -> Result<(), String> {
    ledger.generation = ledger
        .generation
        .checked_add(1)
        .ok_or_else(|| "scanner pause backlog generation is exhausted".to_string())?;
    ledger.last_updated_at_unix_secs = now;
    ledger.validate()
}

fn set_runtime_error(error: Option<String>) {
    *SCANNER_PAUSE_BACKLOG_RUNTIME_ERROR
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = error;
}

fn runtime_error() -> Option<String> {
    SCANNER_PAUSE_BACKLOG_RUNTIME_ERROR
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub async fn scanner_pause_backlog_status(storeapi: Arc<ECStore>) -> ScannerPauseBacklogStatus {
    let now = unix_now();
    match load_scanner_pause_backlog(storeapi).await {
        Ok(loaded) => loaded.status(now, runtime_error()),
        Err(error) => {
            let error = runtime_error().map_or(error.clone(), |runtime| format!("{error}; {runtime}"));
            status_from_ledger(
                &ScannerPauseBacklogLedger::default(),
                now,
                "unavailable".to_string(),
                false,
                0,
                0,
                0,
                false,
                true,
                Some(error),
            )
        }
    }
}

pub(super) fn scanner_pause_backlog_now() -> u64 {
    unix_now()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(now: u64, paused: bool, pending: u64) -> ScannerPauseBacklogObservation {
        ScannerPauseBacklogObservation {
            now_unix_secs: now,
            paused,
            movement_generation: 7,
            movement_work_items: if paused { 1 } else { 0 },
            pause_started_at_unix_secs: if paused { now } else { 0 },
            dirty_usage_buckets: pending,
            discovered_expiry_items: 0,
            discovered_transition_items: 0,
        }
    }

    fn durable_ledger(now: u64) -> ScannerPauseBacklogLedger {
        let mut ledger = ScannerPauseBacklogLedger::default();
        ledger.claim_writer(now).expect("writer epoch should be available");
        prepare_scanner_pause_backlog_persist(&mut ledger, now).expect("ledger should become durable");
        ledger
    }

    fn decode_valid_ledger(ledger: &ScannerPauseBacklogLedger) -> ScannerPauseBacklogLedger {
        let encoded = serde_json::to_vec(ledger).expect("ledger should encode");
        let ScannerPauseBacklogReplicaState::Valid(decoded) = decode_scanner_pause_backlog_ledger(&encoded) else {
            panic!("ledger should decode");
        };
        decoded.stable.expect("legacy ledger should become a stable replica")
    }

    fn retry_exhausted_ledger() -> ScannerPauseBacklogLedger {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        for now in [120, 420, 720, 1020, 3720] {
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(now) else {
                panic!("failure attempt at {now} should be admitted");
            };
            ledger.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::RetryableFailure, observation(now + 1, false, 0));
        }
        ledger
    }

    fn replica_id(pool_index: usize, set_index: usize) -> ScannerPauseBacklogReplicaId {
        ScannerPauseBacklogReplicaId { pool_index, set_index }
    }

    fn decoded_replica(
        id: ScannerPauseBacklogReplicaId,
        record: &ScannerPauseBacklogReplicaRecord,
    ) -> ScannerPauseBacklogReplica {
        let encoded = serde_json::to_vec(record).expect("replica should encode");
        ScannerPauseBacklogReplica {
            id,
            revision: Some(DataUsageCacheRevision::Etag(format!("revision-{}-{}", id.pool_index, id.set_index))),
            state: decode_scanner_pause_backlog_ledger(&encoded),
        }
    }

    fn crash_reload_replicas(replicas: Vec<ScannerPauseBacklogReplica>) -> LoadedScannerPauseBacklog {
        select_scanner_pause_backlog_replicas(replicas).expect("replicas should have an authoritative rollback point")
    }

    fn crash_reload(records: &[ScannerPauseBacklogReplicaRecord]) -> LoadedScannerPauseBacklog {
        let replicas = records
            .iter()
            .enumerate()
            .map(|(set_index, record)| decoded_replica(replica_id(0, set_index), record))
            .collect();
        crash_reload_replicas(replicas)
    }

    fn replica_record_for_members(
        stable: &ScannerPauseBacklogLedger,
        committed: &ScannerPauseBacklogLedger,
        replicas: &[ScannerPauseBacklogReplicaId],
    ) -> ScannerPauseBacklogReplicaRecord {
        ScannerPauseBacklogReplicaRecord::new(
            Some(stable.clone()),
            Some(ScannerPauseBacklogCommitRecord::new(committed.clone(), replicas.to_vec())),
        )
    }

    fn replica_record(
        stable: &ScannerPauseBacklogLedger,
        committed: &ScannerPauseBacklogLedger,
    ) -> ScannerPauseBacklogReplicaRecord {
        let replicas = (0..3)
            .map(|set_index| ScannerPauseBacklogReplicaId {
                pool_index: 0,
                set_index,
            })
            .collect::<Vec<_>>();
        replica_record_for_members(stable, committed, &replicas)
    }

    #[derive(Clone, Copy)]
    enum RejoinedSourceState {
        Missing,
        OlderCommit,
        NewerSmallCommit,
        NewerUnprovenCommit,
        StaleStable,
    }

    fn assert_rejoined_source_is_safely_seeded(source_state: RejoinedSourceState) {
        let source_id = replica_id(0, 0);
        let target_ids = [replica_id(1, 0), replica_id(1, 1)];
        let full_ids = [source_id, target_ids[0], target_ids[1]];
        let old = durable_ledger(50);
        let mut target = old.clone();
        target.claim_writer(100).expect("target membership epoch should advance");
        prepare_scanner_pause_backlog_persist(&mut target, 100).expect("target membership should persist");
        let target_record = replica_record_for_members(&target, &target, &target_ids);
        let source = match source_state {
            RejoinedSourceState::Missing => ScannerPauseBacklogReplica {
                id: source_id,
                revision: Some(DataUsageCacheRevision::Missing),
                state: ScannerPauseBacklogReplicaState::Missing,
            },
            RejoinedSourceState::OlderCommit => {
                let stale_record = replica_record_for_members(&old, &old, &[source_id]);
                decoded_replica(source_id, &stale_record)
            }
            RejoinedSourceState::NewerSmallCommit => {
                let mut stale = target.clone();
                stale.claim_writer(110).expect("stale source epoch should advance");
                prepare_scanner_pause_backlog_persist(&mut stale, 110).expect("stale source should persist");
                stale.claim_writer(120).expect("stale source epoch should advance again");
                prepare_scanner_pause_backlog_persist(&mut stale, 120).expect("stale source should persist again");
                assert!(stale.writer_epoch > target.writer_epoch);
                let stale_record = replica_record_for_members(&stale, &stale, &[source_id]);
                decoded_replica(source_id, &stale_record)
            }
            RejoinedSourceState::NewerUnprovenCommit => {
                let mut stale = target.clone();
                stale.claim_writer(110).expect("stale source epoch should advance");
                prepare_scanner_pause_backlog_persist(&mut stale, 110).expect("stale source should persist");
                stale.claim_writer(120).expect("stale source epoch should advance again");
                prepare_scanner_pause_backlog_persist(&mut stale, 120).expect("stale source should persist again");
                assert!(stale.writer_epoch > target.writer_epoch);
                let stale_record = replica_record_for_members(&stale, &stale, &full_ids);
                decoded_replica(source_id, &stale_record)
            }
            RejoinedSourceState::StaleStable => {
                let stale_record = ScannerPauseBacklogReplicaRecord::new(Some(old), None);
                decoded_replica(source_id, &stale_record)
            }
        };

        let active = crash_reload_replicas(target_ids.iter().map(|id| decoded_replica(*id, &target_record)).collect());
        assert_eq!(active.ledger, target);
        assert_eq!(active.persistence_state, "healthy");

        let rejoined = crash_reload_replicas(vec![
            source,
            decoded_replica(target_ids[0], &target_record),
            decoded_replica(target_ids[1], &target_record),
        ]);
        assert_eq!(rejoined.ledger, target);
        assert_eq!(rejoined.persistence_state, "membership_repair_pending");
        assert_eq!(rejoined.healthy_replicas, target_ids.len());
        assert_eq!(rejoined.stale_or_unavailable_replicas, 1);
        assert_eq!(
            rejoined
                .authoritative_commit
                .as_ref()
                .expect("surviving target proof should remain authoritative")
                .replicas
                .as_slice(),
            target_ids.as_slice()
        );

        let seeded_record = replica_record_for_members(&target, &target, &target_ids);
        let seeded = crash_reload_replicas(full_ids.iter().map(|id| decoded_replica(*id, &seeded_record)).collect());
        assert_eq!(seeded.ledger, target);
        assert!(seeded.stable_matches_ledger);
        assert_eq!(seeded.persistence_state, "membership_repair_pending");

        let mut replacement = seeded.ledger;
        replacement
            .claim_writer(200)
            .expect("replacement node should claim the surviving ledger");
        prepare_scanner_pause_backlog_persist(&mut replacement, 200).expect("replacement node should persist");
        assert_eq!(replacement.writer_epoch, target.writer_epoch + 1);
        let full_commit = replica_record_for_members(&target, &replacement, &full_ids);

        let source_first = crash_reload_replicas(vec![
            decoded_replica(source_id, &full_commit),
            decoded_replica(target_ids[0], &seeded_record),
            decoded_replica(target_ids[1], &seeded_record),
        ]);
        assert_eq!(source_first.ledger, target);
        assert_eq!(source_first.persistence_state, "membership_repair_pending");

        let target_first = crash_reload_replicas(vec![
            decoded_replica(source_id, &seeded_record),
            decoded_replica(target_ids[0], &full_commit),
            decoded_replica(target_ids[1], &seeded_record),
        ]);
        assert_eq!(target_first.ledger, target);
        assert_eq!(target_first.persistence_state, "rolled_back_partial_commit");

        let committed = crash_reload_replicas(full_ids.iter().map(|id| decoded_replica(*id, &full_commit)).collect());
        assert_eq!(committed.ledger, replacement);
        assert_eq!(committed.persistence_state, "committed_pending_stabilization");

        let stable_full_commit = replica_record_for_members(&replacement, &replacement, &full_ids);
        let stable = crash_reload_replicas(full_ids.iter().map(|id| decoded_replica(*id, &stable_full_commit)).collect());
        assert_eq!(stable.ledger, replacement);
        assert_eq!(stable.persistence_state, "healthy");
        assert_eq!(stable.healthy_replicas, full_ids.len());
    }

    #[test]
    fn restart_recovers_paused_backlog_and_requires_one_full_catch_up_scan() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 3));
        prepare_scanner_pause_backlog_persist(&mut ledger, 110).expect("paused ledger should persist");
        let mut restarted = decode_valid_ledger(&ledger);

        restarted.claim_writer(120).expect("new process should claim a writer epoch");
        restarted.apply_observation(observation(120, false, 3));
        assert_eq!(restarted.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(restarted.pending_full_scan);
        assert_eq!(restarted.next_attempt_at_unix_secs, 120);
    }

    #[test]
    fn resume_after_clock_rollback_keeps_pause_timestamps_monotonic() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(ScannerPauseBacklogObservation {
            now_unix_secs: 200,
            pause_started_at_unix_secs: 200,
            paused: true,
            ..observation(200, true, 0)
        });

        ledger.apply_observation(observation(150, false, 0));

        assert_eq!(ledger.pause_started_at_unix_secs, 200);
        assert_eq!(ledger.pause_ended_at_unix_secs, 200);
        prepare_scanner_pause_backlog_persist(&mut ledger, 150).expect("clock rollback should remain persistable");
    }

    #[test]
    fn movement_that_starts_and_ends_inside_a_cycle_still_creates_catch_up_work() {
        let mut ledger = durable_ledger(100);
        ledger.observe_cycle_outcome(
            ScannerPauseBacklogCycleOutcome::DataMovementDeferred,
            ScannerPauseBacklogObservation {
                movement_generation: 8,
                ..observation(120, false, 0)
            },
        );

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
        assert_eq!(ledger.pause_started_at_unix_secs, 120);
        assert_eq!(ledger.pause_ended_at_unix_secs, 120);
    }

    #[test]
    fn restart_recovers_movement_that_completed_before_pause_was_persisted() {
        let ledger = durable_ledger(100);
        let mut restarted = decode_valid_ledger(&ledger);

        restarted.apply_observation(ScannerPauseBacklogObservation {
            movement_generation: 8,
            ..observation(120, false, 0)
        });

        assert_eq!(restarted.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(restarted.pending_full_scan);
        assert_eq!(restarted.pause_started_at_unix_secs, 120);
        assert_eq!(restarted.pause_ended_at_unix_secs, 120);
    }

    #[test]
    fn generation_advance_reopens_full_scan_while_catch_up_has_only_known_work() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(120) else {
            panic!("catch-up attempt should begin");
        };
        ledger.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::Completed, observation(130, false, 1));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(!ledger.pending_full_scan);

        ledger.apply_observation(ScannerPauseBacklogObservation {
            movement_generation: 8,
            ..observation(140, false, 0)
        });

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
        assert_eq!(ledger.movement_generation, 8);
        assert_eq!(ledger.pause_started_at_unix_secs, 140);
        assert_eq!(ledger.pause_ended_at_unix_secs, 140);
        assert_eq!(ledger.next_attempt_at_unix_secs, 430);
    }

    #[test]
    fn completed_attempt_observing_generation_advance_keeps_full_scan_debt() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(120) else {
            panic!("catch-up attempt should begin");
        };

        ledger.finish_attempt(
            serial,
            ScannerPauseBacklogCycleOutcome::Completed,
            ScannerPauseBacklogObservation {
                movement_generation: 8,
                ..observation(130, false, 0)
            },
        );

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
        assert_eq!(ledger.movement_generation, 8);
        assert_eq!(ledger.next_attempt_at_unix_secs, 430);
        prepare_scanner_pause_backlog_persist(&mut ledger, 130).expect("generation advance should remain persistable");
        let restarted = decode_valid_ledger(&ledger);
        assert_eq!(restarted.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(restarted.pending_full_scan);
        assert_eq!(restarted.movement_generation, 8);
    }

    #[test]
    fn generation_advance_preserves_attempt_rate_fence_for_deferred_and_progressed_cycles() {
        for outcome in [
            ScannerPauseBacklogCycleOutcome::DataMovementDeferred,
            ScannerPauseBacklogCycleOutcome::Progressed,
        ] {
            let mut ledger = durable_ledger(100);
            ledger.movement_generation = 7;
            ledger.apply_observation(observation(110, true, 0));
            ledger.apply_observation(observation(120, false, 0));
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(120) else {
                panic!("catch-up attempt should begin");
            };
            assert_eq!(ledger.next_attempt_at_unix_secs, 420);

            ledger.finish_attempt(
                serial,
                outcome,
                ScannerPauseBacklogObservation {
                    movement_generation: 8,
                    ..observation(130, false, 0)
                },
            );

            assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
            assert!(ledger.pending_full_scan);
            assert_eq!(ledger.next_attempt_at_unix_secs, 430);
            assert_eq!(ledger.current_window_started_at_unix_secs, 120);
            assert_eq!(ledger.attempts_in_current_window, 1);
            assert_eq!(ledger.last_attempt_at_unix_secs, 120);
            prepare_scanner_pause_backlog_persist(&mut ledger, 130).expect("rate fence should remain persistable");
            let mut restarted = decode_valid_ledger(&ledger);
            assert_eq!(restarted.next_attempt_at_unix_secs, 430);
            assert_eq!(restarted.current_window_started_at_unix_secs, 120);
            assert_eq!(restarted.attempts_in_current_window, 1);
            assert_eq!(restarted.last_attempt_at_unix_secs, 120);
            assert_eq!(restarted.begin_attempt(429), ScannerPauseBacklogAttemptDecision::RateLimited);
            assert!(matches!(restarted.begin_attempt(430), ScannerPauseBacklogAttemptDecision::Tracked(_)));
        }
    }

    #[test]
    fn remote_deferred_cycle_without_generation_change_keeps_durable_rate_state() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(120) else {
            panic!("catch-up attempt should begin");
        };
        assert_eq!(ledger.next_attempt_at_unix_secs, 420);

        ledger.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::DataMovementDeferred, observation(130, false, 0));

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
        assert_eq!(ledger.movement_generation, 7);
        assert_eq!(ledger.current_window_started_at_unix_secs, 120);
        assert_eq!(ledger.attempts_in_current_window, 1);
        assert_eq!(ledger.last_attempt_at_unix_secs, 120);
        assert_eq!(ledger.next_attempt_at_unix_secs, 430);
        prepare_scanner_pause_backlog_persist(&mut ledger, 130).expect("remote defer fence should persist");
        let restarted = decode_valid_ledger(&ledger);
        assert_eq!(restarted.current_window_started_at_unix_secs, 120);
        assert_eq!(restarted.attempts_in_current_window, 1);
        assert_eq!(restarted.last_attempt_at_unix_secs, 120);
        assert_eq!(restarted.next_attempt_at_unix_secs, 430);
    }

    #[test]
    fn repeated_short_movements_cannot_reset_the_four_attempt_window() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(first) = ledger.begin_attempt(120) else {
            panic!("first catch-up attempt should begin");
        };

        for (serial, attempt_at, finished_at, movement_generation) in
            [(first, 120, 130, 8), (0, 430, 440, 9), (0, 740, 750, 10), (0, 1050, 1060, 11)]
        {
            let serial = if serial == 0 {
                let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(attempt_at) else {
                    panic!("short-movement attempt at {attempt_at} should be admitted");
                };
                serial
            } else {
                serial
            };
            ledger.finish_attempt(
                serial,
                ScannerPauseBacklogCycleOutcome::Progressed,
                ScannerPauseBacklogObservation {
                    movement_generation,
                    ..observation(finished_at, false, 0)
                },
            );
        }

        assert_eq!(ledger.current_window_started_at_unix_secs, 120);
        assert_eq!(ledger.attempts_in_current_window, SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW);
        assert_eq!(ledger.last_attempt_at_unix_secs, 1050);
        assert_eq!(ledger.next_attempt_at_unix_secs, 3720);
        assert_eq!(ledger.begin_attempt(1360), ScannerPauseBacklogAttemptDecision::RateLimited);
        prepare_scanner_pause_backlog_persist(&mut ledger, 1360).expect("movement rate window should persist");
        let restarted = decode_valid_ledger(&ledger);
        assert_eq!(restarted.current_window_started_at_unix_secs, 120);
        assert_eq!(restarted.attempts_in_current_window, SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW);
        assert_eq!(restarted.next_attempt_at_unix_secs, 3720);
    }

    #[test]
    fn node_offline_for_the_whole_movement_epoch_detects_generation_advance() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(ScannerPauseBacklogObservation {
            movement_generation: 9,
            ..observation(120, false, 0)
        });

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
    }

    #[test]
    fn node_switch_selects_only_an_all_replica_writer_epoch() {
        let old = durable_ledger(100);
        let mut current = old.clone();
        current.claim_writer(110).expect("new node should claim a higher epoch");
        prepare_scanner_pause_backlog_persist(&mut current, 110).expect("new node claim should persist");
        let committed = crash_reload(&[
            replica_record(&old, &current),
            replica_record(&old, &current),
            replica_record(&old, &current),
        ]);
        assert_eq!(committed.ledger, current);

        let mut divergent = current.clone();
        divergent.deferred_cycles = 9;
        let rolled_back = crash_reload(&[
            replica_record(&old, &current),
            replica_record(&old, &divergent),
            replica_record(&old, &old),
        ]);
        assert_eq!(rolled_back.ledger, old);
        assert_eq!(rolled_back.persistence_state, "rolled_back_partial_commit");
    }

    #[test]
    fn active_to_failed_rejoins_and_seeds_a_missing_source() {
        assert_rejoined_source_is_safely_seeded(RejoinedSourceState::Missing);
    }

    #[test]
    fn active_to_canceled_rejoins_from_an_older_source_commit() {
        assert_rejoined_source_is_safely_seeded(RejoinedSourceState::OlderCommit);
    }

    #[test]
    fn cleared_decommission_rejoins_and_repairs_a_stale_source_after_restart() {
        assert_rejoined_source_is_safely_seeded(RejoinedSourceState::StaleStable);
    }

    #[test]
    fn rejoined_source_cannot_reverse_overwrite_a_surviving_commit() {
        assert_rejoined_source_is_safely_seeded(RejoinedSourceState::NewerUnprovenCommit);
    }

    #[test]
    fn one_replica_high_epoch_commit_cannot_override_a_larger_surviving_commit() {
        assert_rejoined_source_is_safely_seeded(RejoinedSourceState::NewerSmallCommit);
    }

    #[test]
    fn partial_begin_attempt_commit_is_not_authoritative_after_crash() {
        let mut stable = durable_ledger(100);
        stable.apply_observation(observation(110, true, 0));
        stable.apply_observation(observation(120, false, 0));
        prepare_scanner_pause_backlog_persist(&mut stable, 120).expect("catch-up state should persist");

        let mut begun = stable.clone();
        assert!(matches!(begun.begin_attempt(120), ScannerPauseBacklogAttemptDecision::Tracked(_)));
        prepare_scanner_pause_backlog_persist(&mut begun, 120).expect("begun attempt should persist");
        let partial = crash_reload(&[
            replica_record(&stable, &begun),
            replica_record(&stable, &stable),
            replica_record(&stable, &stable),
        ]);

        assert_eq!(partial.ledger, stable);
        assert!(!partial.ledger.has_unfinished_attempt());
        let failed_member_absent = crash_reload(&[replica_record(&stable, &begun), replica_record(&stable, &begun)]);
        assert_eq!(failed_member_absent.ledger, stable);
        assert!(!failed_member_absent.ledger.has_unfinished_attempt());
        let committed = crash_reload(&[
            replica_record(&stable, &begun),
            replica_record(&stable, &begun),
            replica_record(&stable, &begun),
        ]);
        assert_eq!(committed.ledger, begun);
        assert!(committed.ledger.has_unfinished_attempt());
    }

    #[test]
    fn partial_idle_finish_is_rolled_back_and_interrupted_attempt_fails_closed() {
        let mut stable = durable_ledger(100);
        stable.apply_observation(observation(110, true, 0));
        stable.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(serial) = stable.begin_attempt(120) else {
            panic!("catch-up attempt should begin");
        };
        prepare_scanner_pause_backlog_persist(&mut stable, 120).expect("begun attempt should persist");

        let mut finished = stable.clone();
        finished.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::Completed, observation(130, false, 0));
        prepare_scanner_pause_backlog_persist(&mut finished, 130).expect("finished attempt should persist");
        assert_eq!(finished.phase, ScannerPauseBacklogPhase::Idle);

        let partial = crash_reload(&[
            replica_record(&stable, &finished),
            replica_record(&stable, &stable),
            replica_record(&stable, &stable),
        ]);
        assert_eq!(partial.ledger, stable);
        assert_ne!(partial.ledger.phase, ScannerPauseBacklogPhase::Idle);
        assert!(partial.ledger.has_unfinished_attempt());
        let mut replacement = partial.ledger;
        replacement
            .claim_writer(140)
            .expect("replacement node should claim the interrupted attempt");
        assert_eq!(replacement.consecutive_failures, 1);
        assert_ne!(replacement.phase, ScannerPauseBacklogPhase::Idle);

        let failed_member_absent = crash_reload(&[replica_record(&stable, &finished), replica_record(&stable, &finished)]);
        assert_ne!(failed_member_absent.ledger.phase, ScannerPauseBacklogPhase::Idle);
        assert!(failed_member_absent.ledger.has_unfinished_attempt());

        let committed = crash_reload(&[
            replica_record(&stable, &finished),
            replica_record(&stable, &finished),
            replica_record(&stable, &finished),
        ]);
        assert_eq!(committed.ledger.phase, ScannerPauseBacklogPhase::Idle);
    }

    #[test]
    fn backlog_converges_only_after_full_scan_and_known_ilm_work_clear() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(first) = ledger.begin_attempt(120) else {
            panic!("first catch-up attempt should be admitted");
        };
        let mut pending = observation(130, false, 0);
        pending.discovered_expiry_items = 2;
        ledger.finish_attempt(first, ScannerPauseBacklogCycleOutcome::Completed, pending);
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(!ledger.pending_full_scan);

        ledger.apply_observation(observation(420, false, 0));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::Idle);
        assert_eq!(ledger.pending_work_items(), 0);
        prepare_scanner_pause_backlog_persist(&mut ledger, 430).expect("converged ledger should persist");
        assert_eq!(decode_valid_ledger(&ledger).phase, ScannerPauseBacklogPhase::Idle);
    }

    #[test]
    fn fourth_completed_known_work_attempt_preserves_window_end_before_convergence() {
        let mut ledger = durable_ledger(100);
        ledger.movement_generation = 7;
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        let ScannerPauseBacklogAttemptDecision::Tracked(full_scan) = ledger.begin_attempt(120) else {
            panic!("required full scan should begin");
        };
        let known_work = ScannerPauseBacklogObservation {
            dirty_usage_buckets: 1,
            discovered_expiry_items: 2,
            discovered_transition_items: 3,
            ..observation(130, false, 0)
        };
        ledger.finish_attempt(full_scan, ScannerPauseBacklogCycleOutcome::Completed, known_work);
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(!ledger.pending_full_scan);
        assert_eq!(ledger.pending_work_items(), 6);
        assert_eq!(ledger.begin_attempt(429), ScannerPauseBacklogAttemptDecision::RateLimited);

        for (attempt_at, finished_at, outcome) in [
            (430, 431, ScannerPauseBacklogCycleOutcome::Progressed),
            (730, 731, ScannerPauseBacklogCycleOutcome::Progressed),
            (1030, 1031, ScannerPauseBacklogCycleOutcome::Completed),
        ] {
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(attempt_at) else {
                panic!("known-work attempt at {attempt_at} should be admitted");
            };
            ledger.finish_attempt(
                serial,
                outcome,
                ScannerPauseBacklogObservation {
                    now_unix_secs: finished_at,
                    ..known_work
                },
            );
        }
        assert_eq!(ledger.attempts_in_current_window, SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW);
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert_eq!(ledger.pending_work_items(), 6);
        assert_eq!(ledger.next_attempt_at_unix_secs, 3720);
        assert_eq!(ledger.begin_attempt(1320), ScannerPauseBacklogAttemptDecision::RateLimited);
        assert_eq!(ledger.next_attempt_at_unix_secs, 3720);

        let ScannerPauseBacklogAttemptDecision::Tracked(final_attempt) = ledger.begin_attempt(3720) else {
            panic!("next bounded window should admit known work");
        };
        ledger.finish_attempt(final_attempt, ScannerPauseBacklogCycleOutcome::Completed, observation(3721, false, 0));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::Idle);
        assert_eq!(ledger.pending_work_items(), 0);
        prepare_scanner_pause_backlog_persist(&mut ledger, 3721).expect("converged known work should persist");
        assert_eq!(decode_valid_ledger(&ledger).phase, ScannerPauseBacklogPhase::Idle);
    }

    #[test]
    fn catch_up_rate_window_survives_restart() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        for now in [120, 420, 720, 1020] {
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(now) else {
                panic!("attempt at {now} should be admitted");
            };
            ledger.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::RetryableFailure, observation(now + 1, false, 0));
        }
        prepare_scanner_pause_backlog_persist(&mut ledger, 1021).expect("rate window should persist");
        let mut restarted = decode_valid_ledger(&ledger);
        assert_eq!(restarted.begin_attempt(1320), ScannerPauseBacklogAttemptDecision::RateLimited);
        assert_eq!(restarted.next_attempt_at_unix_secs, 3720);
    }

    #[test]
    fn bounded_partial_catch_up_progress_does_not_exhaust_the_retry_budget() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        for now in [120, 420, 720, 1020, 3720] {
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(now) else {
                panic!("partial attempt at {now} should be admitted");
            };
            ledger.finish_attempt(serial, ScannerPauseBacklogCycleOutcome::Progressed, observation(now + 1, false, 0));
        }

        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(ledger.pending_full_scan);
        assert_eq!(ledger.consecutive_failures, 0);
        assert_eq!(ledger.next_attempt_at_unix_secs, 4020);
    }

    #[test]
    fn completed_usage_with_pending_maintenance_stays_durable_until_convergence() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));

        let ScannerPauseBacklogAttemptDecision::Tracked(full_scan) = ledger.begin_attempt(120) else {
            panic!("full catch-up scan should be admitted");
        };
        ledger.finish_attempt(full_scan, ScannerCycleOutcome::Completed.into(), observation(121, false, 1));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert!(!ledger.pending_full_scan);
        assert_eq!(ledger.pending_work_items(), 1);
        assert_eq!(ledger.next_attempt_at_unix_secs, 421);

        for (index, now) in [421, 721, 1021].into_iter().enumerate() {
            let ScannerPauseBacklogAttemptDecision::Tracked(serial) = ledger.begin_attempt(now) else {
                panic!("pending-maintenance attempt at {now} should be admitted");
            };
            ledger.finish_attempt(
                serial,
                ScannerCycleOutcome::CompletedWithPendingMaintenance.into(),
                observation(now + 1, false, 0),
            );
            assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
            assert!(ledger.pending_full_scan);
            assert_eq!(ledger.pending_work_items(), 1);
            if index == 0 {
                assert_eq!(ledger.next_attempt_at_unix_secs, 721);
                prepare_scanner_pause_backlog_persist(&mut ledger, 422).expect("pending maintenance should remain durable");
                ledger = decode_valid_ledger(&ledger);
                assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
                assert!(ledger.pending_full_scan);
                assert_eq!(ledger.next_attempt_at_unix_secs, 721);
                let status = status_from_ledger(&ledger, 422, "healthy".to_string(), true, 1, 1, 0, false, false, None);
                assert_eq!(status.phase, ScannerPauseBacklogPhase::CatchingUp);
                assert!(status.pending_full_scan);
                assert_eq!(status.pending_work_items, 1);
                assert!(status.rate_limited);
            }
        }

        assert_eq!(ledger.attempts_in_current_window, SCANNER_CATCH_UP_MAX_ATTEMPTS_PER_WINDOW);
        assert_eq!(ledger.next_attempt_at_unix_secs, 3720);
        assert_eq!(ledger.begin_attempt(1320), ScannerPauseBacklogAttemptDecision::RateLimited);

        let ScannerPauseBacklogAttemptDecision::Tracked(final_attempt) = ledger.begin_attempt(3720) else {
            panic!("converged maintenance attempt should be admitted in the next window");
        };
        ledger.finish_attempt(final_attempt, ScannerCycleOutcome::Completed.into(), observation(3721, false, 0));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::Idle);
        assert!(!ledger.pending_full_scan);
        assert_eq!(ledger.pending_work_items(), 0);
    }

    #[test]
    fn retry_budget_exhaustion_uses_sparse_probe_and_alerts() {
        let mut ledger = retry_exhausted_ledger();
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::RetryExhausted);
        assert_eq!(ledger.next_attempt_at_unix_secs, 7321);
        assert_eq!(ledger.begin_attempt(7000), ScannerPauseBacklogAttemptDecision::RateLimited);
        assert!(
            ledger
                .alert_reasons(7000, false, false)
                .contains(&ScannerPauseBacklogAlertReason::RetryBudgetExhausted)
        );

        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = ledger.begin_attempt(7321) else {
            panic!("hourly recovery probe should be admitted");
        };
        ledger.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::Progressed, observation(7322, false, 0));
        assert_eq!(ledger.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert_eq!(ledger.consecutive_failures, 0);
        assert_eq!(ledger.next_attempt_at_unix_secs, 7622);
        prepare_scanner_pause_backlog_persist(&mut ledger, 7322).expect("recovered retry cadence should persist");
        let restarted = decode_valid_ledger(&ledger);
        assert_eq!(restarted.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert_eq!(restarted.current_window_started_at_unix_secs, 7322);
        assert_eq!(restarted.attempts_in_current_window, 0);
        assert_eq!(restarted.next_attempt_at_unix_secs, 7622);
    }

    #[test]
    fn retry_exhausted_probe_releases_hourly_floor_only_after_success() {
        let exhausted = retry_exhausted_ledger();

        let mut completed_with_work = exhausted.clone();
        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = completed_with_work.begin_attempt(7321) else {
            panic!("completed recovery probe should be admitted");
        };
        completed_with_work.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::Completed, observation(7322, false, 1));
        assert_eq!(completed_with_work.phase, ScannerPauseBacklogPhase::CatchingUp);
        assert_eq!(completed_with_work.current_window_started_at_unix_secs, 7322);
        assert_eq!(completed_with_work.attempts_in_current_window, 0);
        assert_eq!(completed_with_work.next_attempt_at_unix_secs, 7622);

        let mut completed = exhausted.clone();
        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = completed.begin_attempt(7321) else {
            panic!("final recovery probe should be admitted");
        };
        completed.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::Completed, observation(7322, false, 0));
        assert_eq!(completed.phase, ScannerPauseBacklogPhase::Idle);
        assert_eq!(completed.next_attempt_at_unix_secs, 0);

        let mut deferred = exhausted.clone();
        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = deferred.begin_attempt(7321) else {
            panic!("deferred probe should be admitted");
        };
        deferred.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::DataMovementDeferred, observation(7322, false, 0));
        assert_eq!(deferred.phase, ScannerPauseBacklogPhase::RetryExhausted);
        assert_eq!(deferred.next_attempt_at_unix_secs, 10921);

        let mut failed = exhausted;
        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = failed.begin_attempt(7321) else {
            panic!("failed probe should be admitted");
        };
        failed.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::RetryableFailure, observation(7322, false, 0));
        assert_eq!(failed.phase, ScannerPauseBacklogPhase::RetryExhausted);
        assert_eq!(failed.next_attempt_at_unix_secs, 10922);

        let mut counter_exhausted = retry_exhausted_ledger();
        counter_exhausted.counter_exhausted = true;
        let ScannerPauseBacklogAttemptDecision::Tracked(probe) = counter_exhausted.begin_attempt(7321) else {
            panic!("counter-exhausted probe should be admitted");
        };
        counter_exhausted.finish_attempt(probe, ScannerPauseBacklogCycleOutcome::Progressed, observation(7322, false, 0));
        assert_eq!(counter_exhausted.phase, ScannerPauseBacklogPhase::RetryExhausted);
        assert_eq!(counter_exhausted.next_attempt_at_unix_secs, 10921);
    }

    #[test]
    fn node_switch_counts_an_interrupted_attempt_and_keeps_the_rate_fence() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(observation(110, true, 0));
        ledger.apply_observation(observation(120, false, 0));
        assert!(matches!(ledger.begin_attempt(120), ScannerPauseBacklogAttemptDecision::Tracked(_)));
        ledger.claim_writer(130).expect("replacement node should claim the ledger");

        assert_eq!(ledger.current_attempt_serial, ledger.last_finished_attempt_serial);
        assert_eq!(ledger.consecutive_failures, 1);
        assert_eq!(ledger.begin_attempt(130), ScannerPauseBacklogAttemptDecision::RateLimited);
    }

    #[test]
    fn pause_and_backlog_thresholds_are_visible() {
        let mut ledger = durable_ledger(100);
        ledger.apply_observation(ScannerPauseBacklogObservation {
            now_unix_secs: 110,
            paused: true,
            movement_generation: 7,
            movement_work_items: 1,
            pause_started_at_unix_secs: 100,
            dirty_usage_buckets: SCANNER_PAUSE_BACKLOG_ITEMS_ALERT,
            discovered_expiry_items: 0,
            discovered_transition_items: 0,
        });
        ledger.deferred_cycles = SCANNER_PAUSE_DEFERRED_CYCLES_ALERT;
        let alerts = ledger.alert_reasons(100 + SCANNER_PAUSE_DURATION_ALERT_SECONDS, true, false);
        assert!(alerts.contains(&ScannerPauseBacklogAlertReason::PauseDurationThreshold));
        assert!(alerts.contains(&ScannerPauseBacklogAlertReason::DeferredCyclesThreshold));
        assert!(alerts.contains(&ScannerPauseBacklogAlertReason::BacklogItemsThreshold));
        assert!(alerts.contains(&ScannerPauseBacklogAlertReason::ReplicaDegraded));
    }
}
