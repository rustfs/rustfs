// Copyright 2026 RustFS Team
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

use super::*;
use tokio::time::Instant;

const PACING_QUANTUM: Duration = Duration::from_millis(1);
const PACING_MAX_OBJECTS: u32 = 64;
const PACING_MAX_AGE: Duration = Duration::from_millis(10);

static PACING_OBJECTS: LazyLock<metrics::Counter> = LazyLock::new(|| {
    metrics::describe_counter!(
        "rustfs_scanner_pacing_objects_total",
        "Object operations charged to walker proportional pacing; disk I/O is accounted separately."
    );
    metrics::counter!("rustfs_scanner_pacing_objects_total")
});
static PACING_BATCHES: LazyLock<metrics::Counter> = LazyLock::new(|| {
    metrics::describe_counter!("rustfs_scanner_pacing_batches_total", "Walker proportional pacing batches executed.");
    metrics::counter!("rustfs_scanner_pacing_batches_total")
});

/// Owned by one walker, including its recursive folder visits. No disk credit
/// is stored here: cancelling this walker cannot refund already charged I/O.
#[derive(Default)]
pub(crate) struct PacingDebt {
    pending: Duration,
    objects: u32,
    started: Option<Instant>,
    reset_generation: u64,
    #[cfg(test)]
    pub(crate) charged_objects: u64,
    #[cfg(test)]
    pub(crate) batches: u64,
}

impl PacingDebt {
    fn reset(&mut self) {
        self.pending = Duration::ZERO;
        self.objects = 0;
        self.started = None;
    }

    fn prepare(&mut self, config: PacingConfig) {
        if self.reset_generation != config.reset_generation
            || !DynamicSleeper::should_sleep()
            || config.factor == 0.0
            || config.max_sleep.is_zero()
        {
            self.reset();
        }
        self.reset_generation = config.reset_generation;
    }

    fn charge(&mut self, delay: Duration, now: Instant) {
        self.pending = self.pending.saturating_add(delay);
        self.objects = self.objects.saturating_add(1);
        if !self.pending.is_zero() {
            self.started.get_or_insert(now);
        }
        PACING_OBJECTS.increment(1);
        #[cfg(test)]
        {
            self.charged_objects += 1;
        }
    }

    // Bounds are checked at scanner operation boundaries; an in-flight I/O
    // operation does not create a second timer or task just to collect debt.
    fn due(&self, config: PacingConfig, now: Instant) -> bool {
        !self.pending.is_zero()
            && (self.pending >= PACING_QUANTUM.min(config.max_sleep)
                || self.objects >= PACING_MAX_OBJECTS
                || self.started.is_some_and(|start| now.duration_since(start) >= PACING_MAX_AGE))
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> Duration {
        self.pending
    }
}

fn proportional_delay(elapsed: Duration, config: PacingConfig) -> Duration {
    if !config.factor.is_finite() || config.factor <= 0.0 {
        return Duration::ZERO;
    }
    Duration::try_from_secs_f64(elapsed.as_secs_f64() * config.factor)
        .unwrap_or(config.max_sleep)
        .min(config.max_sleep)
}

/// Dropping a cancelled wait still records the time actually spent waiting.
struct PacingWait(Instant);

impl Drop for PacingWait {
    fn drop(&mut self) {
        global_metrics().record_scanner_throttle_sleep(self.0.elapsed());
    }
}

impl DynamicSleeper {
    pub(crate) async fn pace_object(&self, debt: &mut PacingDebt, elapsed: Duration) {
        self.pace(debt, Some(elapsed), false, false).await;
    }

    pub(crate) async fn sleep_folder_paced(&self, debt: &mut PacingDebt) {
        self.pace(debt, None, true, false).await;
        self.admit_disk_io(1, 0).await;
    }

    pub(crate) async fn flush_pacing(&self, debt: &mut PacingDebt) {
        self.pace(debt, None, false, true).await;
    }

    async fn pace(&self, debt: &mut PacingDebt, elapsed: Option<Duration>, folder: bool, flush: bool) {
        let config = self.read_config();
        debt.prepare(config);
        if !Self::should_sleep() || (flush && debt.pending.is_zero()) {
            return;
        }
        if let Some(elapsed) = elapsed {
            debt.charge(proportional_delay(elapsed, config), Instant::now());
        }
        let foreground = foreground_workload_backoff_duration(crate::workload_admission::foreground_workload_activity());
        let now = Instant::now();
        let folder_reaches_deadline = folder
            && debt.started.is_some_and(|start| {
                now.duration_since(start)
                    .saturating_add(proportional_delay(MIN_SLEEP, config))
                    >= PACING_MAX_AGE
            });
        let repay = flush || !foreground.is_zero() || folder_reaches_deadline || debt.due(config, now);
        if repay && !debt.pending.is_zero() {
            PACING_BATCHES.increment(1);
            #[cfg(test)]
            {
                debt.batches += 1;
            }
        }
        if folder || repay || !foreground.is_zero() {
            let mut folder = folder;
            let mut foreground = foreground;
            loop {
                let requested = if repay { debt.pending } else { Duration::ZERO };
                let paid = self.wait_pacing(requested, folder, foreground, debt.reset_generation).await;
                if repay {
                    debt.pending = debt.pending.saturating_sub(paid);
                }
                debt.prepare(self.read_config());
                if !repay || debt.pending.is_zero() {
                    break;
                }
                // A positive max_wait reduction caps each wait, not the debt.
                // Actual timer overshoot also repays debt, so sub-ms caps do
                // not require one timer per nanosecond of outstanding work.
                folder = false;
                foreground = Duration::ZERO;
            }
        }
        if debt.pending.is_zero() {
            debt.reset();
        }
    }

    async fn wait_pacing(&self, debt: Duration, folder: bool, foreground: Duration, reset_generation: u64) -> Duration {
        let start = Instant::now();
        let mut measurement = None;
        loop {
            // Register before reading configuration so an update cannot be
            // lost between choosing a duration and polling the notification.
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let config = self.read_config();
            if !Self::should_sleep() {
                return Duration::ZERO;
            }
            let proportional = if config.reset_generation == reset_generation && config.factor > 0.0 {
                debt.min(config.max_sleep)
            } else {
                Duration::ZERO
            };
            let folder_gap = if folder {
                proportional_delay(MIN_SLEEP, config)
            } else {
                Duration::ZERO
            };
            let duration = proportional.saturating_add(folder_gap).max(foreground);
            let remaining = duration.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return start.elapsed().saturating_sub(folder_gap);
            }
            measurement.get_or_insert_with(|| PacingWait(Instant::now()));
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {
                    return start.elapsed().saturating_sub(folder_gap);
                }
                _ = &mut changed => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
