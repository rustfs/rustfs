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

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Instant;

use rustfs_common::metrics::global_metrics;
use rustfs_config::{
    DEFAULT_SCANNER_IDLE_MODE, DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS, ENV_SCANNER_IDLE_MODE, ENV_SCANNER_SPEED,
    ENV_SCANNER_YIELD_EVERY_N_OBJECTS, ScannerSpeed,
};
use tokio::time::Duration;

const MIN_SLEEP: Duration = Duration::from_millis(1);
const SCANNER_SPEED_FASTEST: u8 = 0;
const SCANNER_SPEED_FAST: u8 = 1;
const SCANNER_SPEED_DEFAULT: u8 = 2;
const SCANNER_SPEED_SLOW: u8 = 3;
const SCANNER_SPEED_SLOWEST: u8 = 4;
const FOREGROUND_READ_BACKOFF_PER_REQUEST_MS: u64 = 10;
const FOREGROUND_READ_BACKOFF_MAX_MS: u64 = 250;

static SCANNER_DEFAULT_SPEED_PRESET: AtomicU8 = AtomicU8::new(SCANNER_SPEED_DEFAULT);

const fn scanner_speed_code(speed: ScannerSpeed) -> u8 {
    match speed {
        ScannerSpeed::Fastest => SCANNER_SPEED_FASTEST,
        ScannerSpeed::Fast => SCANNER_SPEED_FAST,
        ScannerSpeed::Default => SCANNER_SPEED_DEFAULT,
        ScannerSpeed::Slow => SCANNER_SPEED_SLOW,
        ScannerSpeed::Slowest => SCANNER_SPEED_SLOWEST,
    }
}

fn scanner_speed_from_code(value: u8) -> ScannerSpeed {
    match value {
        SCANNER_SPEED_FASTEST => ScannerSpeed::Fastest,
        SCANNER_SPEED_FAST => ScannerSpeed::Fast,
        SCANNER_SPEED_SLOW => ScannerSpeed::Slow,
        SCANNER_SPEED_SLOWEST => ScannerSpeed::Slowest,
        _ => ScannerSpeed::Default,
    }
}

pub(crate) fn set_scanner_default_speed(speed: ScannerSpeed) {
    SCANNER_DEFAULT_SPEED_PRESET.store(scanner_speed_code(speed), Ordering::Relaxed);
}

pub(crate) fn scanner_default_speed() -> ScannerSpeed {
    scanner_speed_from_code(SCANNER_DEFAULT_SPEED_PRESET.load(Ordering::Relaxed))
}

pub(crate) fn scanner_speed_from_env_or_default() -> ScannerSpeed {
    rustfs_utils::get_env_opt_str(ENV_SCANNER_SPEED)
        .map(|speed| ScannerSpeed::from_env_str(&speed))
        .unwrap_or_else(scanner_default_speed)
}

fn scanner_env_config() -> (ScannerSpeed, bool) {
    let speed = scanner_speed_from_env_or_default();
    let idle_mode = rustfs_utils::get_env_bool(ENV_SCANNER_IDLE_MODE, DEFAULT_SCANNER_IDLE_MODE);
    (speed, idle_mode)
}

pub(crate) fn scanner_yield_every_n_objects() -> u64 {
    rustfs_utils::get_env_u64(ENV_SCANNER_YIELD_EVERY_N_OBJECTS, DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS)
}

fn foreground_read_backoff_duration(active_reads: u64) -> Duration {
    if active_reads == 0 {
        return Duration::ZERO;
    }

    Duration::from_millis(
        active_reads
            .saturating_mul(FOREGROUND_READ_BACKOFF_PER_REQUEST_MS)
            .min(FOREGROUND_READ_BACKOFF_MAX_MS),
    )
}

/// When `true` (default), the scanner throttles itself between operations.
/// When `false`, all sleeps are skipped and the scanner runs at full speed.
pub static SCANNER_IDLE_MODE: AtomicBool = AtomicBool::new(DEFAULT_SCANNER_IDLE_MODE);

/// Global scanner sleeper initialized from the `RUSTFS_SCANNER_SPEED` and
/// `RUSTFS_SCANNER_IDLE_MODE` environment variables.
pub static SCANNER_SLEEPER: LazyLock<DynamicSleeper> = LazyLock::new(|| {
    let (speed, idle_mode) = scanner_env_config();
    SCANNER_IDLE_MODE.store(idle_mode, Ordering::Relaxed);

    let sleeper = DynamicSleeper::new(speed);
    sleeper.record_throttle_config();
    sleeper
});

/// Proportional-backoff sleeper for the data scanner.
///
/// For operations timed via [`SleepTimer`], sleeps are computed as
/// `elapsed_work_time * factor`, clamped to `[MIN_SLEEP, max_sleep]`.
/// For folder-level gaps, [`sleep_folder`] uses `MIN_SLEEP * factor`,
/// clamped only by `max_sleep`. All sleeps are gated on [`SCANNER_IDLE_MODE`];
#[derive(Clone)]
pub struct DynamicSleeper {
    inner: Arc<SleeperParams>,
}

struct SleeperParams {
    factor: RwLock<f64>,
    max_sleep: RwLock<Duration>,
}

impl DynamicSleeper {
    pub fn new(speed: ScannerSpeed) -> Self {
        Self {
            inner: Arc::new(SleeperParams {
                factor: RwLock::new(speed.sleep_factor()),
                max_sleep: RwLock::new(speed.max_sleep()),
            }),
        }
    }

    fn should_sleep() -> bool {
        SCANNER_IDLE_MODE.load(Ordering::Relaxed)
    }

    fn read_params(&self) -> (f64, Duration) {
        let factor = *self.inner.factor.read().unwrap_or_else(|e| e.into_inner());
        let max_sleep = *self.inner.max_sleep.read().unwrap_or_else(|e| e.into_inner());
        (factor, max_sleep)
    }

    pub async fn sleep_folder(&self) {
        if !Self::should_sleep() {
            return;
        }
        let (factor, max_sleep) = self.read_params();
        if factor == 0.0 || max_sleep.is_zero() {
            let foreground_sleep = foreground_read_backoff_duration(crate::current_foreground_read_activity());
            if !foreground_sleep.is_zero() {
                tokio::time::sleep(foreground_sleep).await;
                global_metrics().record_scanner_throttle_sleep(foreground_sleep);
            }
            return;
        }
        let foreground_sleep = foreground_read_backoff_duration(crate::current_foreground_read_activity());
        let sleep_dur = Duration::from_secs_f64(MIN_SLEEP.as_secs_f64() * factor)
            .min(max_sleep)
            .max(foreground_sleep);
        if !sleep_dur.is_zero() {
            tokio::time::sleep(sleep_dur).await;
            global_metrics().record_scanner_throttle_sleep(sleep_dur);
        }
    }

    /// Begin timing an operation. Call [`sleep()`](SleepTimer::sleep) on
    /// the returned timer after the work is done; it will sleep for
    /// dynamic duration.
    pub fn timer(&self) -> SleepTimer {
        SleepTimer {
            start: Instant::now(),
            sleeper: self.clone(),
        }
    }

    fn update_params(&self, factor: f64, max_sleep: Duration) {
        let mut f = self.inner.factor.write().unwrap_or_else(|e| e.into_inner());
        *f = factor;
        let mut m = self.inner.max_sleep.write().unwrap_or_else(|e| e.into_inner());
        *m = max_sleep;
    }

    /// Swap in parameters from a new speed preset (for runtime reconfiguration).
    pub fn update(&self, speed: ScannerSpeed) {
        self.update_params(speed.sleep_factor(), speed.max_sleep());
    }

    /// Reload speed and idle-mode settings from the current environment.
    pub fn refresh_from_env(&self) {
        let (speed, idle_mode) = scanner_env_config();
        self.update_from_runtime_config(speed.sleep_factor(), speed.max_sleep(), idle_mode, scanner_yield_every_n_objects());
    }

    pub(crate) fn update_from_runtime_config(
        &self,
        sleep_factor: f64,
        max_sleep: Duration,
        idle_mode: bool,
        yield_every_n_objects: u64,
    ) {
        self.update_params(sleep_factor, max_sleep);
        SCANNER_IDLE_MODE.store(idle_mode, Ordering::Relaxed);
        self.record_throttle_config_with_yield(yield_every_n_objects);
    }

    fn record_throttle_config(&self) {
        self.record_throttle_config_with_yield(scanner_yield_every_n_objects());
    }

    fn record_throttle_config_with_yield(&self, yield_every_n_objects: u64) {
        let (factor, max_sleep) = self.read_params();
        global_metrics().record_scanner_throttle_config(
            SCANNER_IDLE_MODE.load(Ordering::Relaxed),
            factor,
            max_sleep,
            yield_every_n_objects,
        );
    }
}

/// A timer returned by [`DynamicSleeper::timer`].  Records the instant it
/// was created so that [`sleep`](Self::sleep) can compute a proportional
/// backoff from the elapsed work time.
pub struct SleepTimer {
    start: Instant,
    sleeper: DynamicSleeper,
}

impl SleepTimer {
    /// Sleep proportional to the elapsed time since this timer was created.
    pub async fn sleep(self) {
        if !DynamicSleeper::should_sleep() {
            return;
        }
        let (factor, max_sleep) = self.sleeper.read_params();
        if factor == 0.0 || max_sleep.is_zero() {
            let foreground_sleep = foreground_read_backoff_duration(crate::current_foreground_read_activity());
            if !foreground_sleep.is_zero() {
                tokio::time::sleep(foreground_sleep).await;
                global_metrics().record_scanner_throttle_sleep(foreground_sleep);
            }
            return;
        }
        let elapsed = self.start.elapsed();
        let foreground_sleep = foreground_read_backoff_duration(crate::current_foreground_read_activity());
        let sleep_dur = Duration::from_secs_f64(elapsed.as_secs_f64() * factor)
            .max(MIN_SLEEP)
            .min(max_sleep)
            .max(foreground_sleep);
        if !sleep_dur.is_zero() {
            tokio::time::sleep(sleep_dur).await;
            global_metrics().record_scanner_throttle_sleep(sleep_dur);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use temp_env::{with_var, with_var_unset};

    struct ScannerDefaultSpeedGuard;

    impl ScannerDefaultSpeedGuard {
        fn set(speed: ScannerSpeed) -> Self {
            set_scanner_default_speed(speed);
            Self
        }
    }

    impl Drop for ScannerDefaultSpeedGuard {
        fn drop(&mut self) {
            set_scanner_default_speed(ScannerSpeed::Default);
        }
    }

    #[test]
    fn test_scanner_speed_presets() {
        let s = DynamicSleeper::new(ScannerSpeed::Fastest);
        let (factor, max_sleep) = s.read_params();
        assert_eq!(factor, 0.0);
        assert_eq!(max_sleep, Duration::ZERO);

        let s = DynamicSleeper::new(ScannerSpeed::Default);
        let (factor, max_sleep) = s.read_params();
        assert_eq!(factor, 2.0);
        assert_eq!(max_sleep, Duration::from_secs(1));

        let s = DynamicSleeper::new(ScannerSpeed::Slowest);
        let (factor, max_sleep) = s.read_params();
        assert_eq!(factor, 100.0);
        assert_eq!(max_sleep, Duration::from_secs(15));
    }

    #[test]
    fn test_update_changes_params() {
        let s = DynamicSleeper::new(ScannerSpeed::Default);
        s.update(ScannerSpeed::Slow);
        let (factor, max_sleep) = s.read_params();
        assert_eq!(factor, 10.0);
        assert_eq!(max_sleep, Duration::from_secs(15));
    }

    #[test]
    fn foreground_read_backoff_is_capped() {
        assert_eq!(foreground_read_backoff_duration(0), Duration::ZERO);
        assert_eq!(foreground_read_backoff_duration(1), Duration::from_millis(10));
        assert_eq!(foreground_read_backoff_duration(80), Duration::from_millis(250));
    }

    #[test]
    fn test_update_from_runtime_config_applies_explicit_pacing_params() {
        let prev_mode = SCANNER_IDLE_MODE.load(Ordering::Relaxed);
        let s = DynamicSleeper::new(ScannerSpeed::Default);

        s.update_from_runtime_config(3.5, Duration::from_secs(7), true, 64);

        let (factor, max_sleep) = s.read_params();
        assert_eq!(factor, 3.5);
        assert_eq!(max_sleep, Duration::from_secs(7));
        assert!(SCANNER_IDLE_MODE.load(Ordering::Relaxed));

        SCANNER_IDLE_MODE.store(prev_mode, Ordering::Relaxed);
    }

    #[test]
    #[serial]
    fn test_refresh_from_env_applies_speed_and_idle_mode_for_next_cycle() {
        let prev_mode = SCANNER_IDLE_MODE.load(Ordering::Relaxed);
        SCANNER_IDLE_MODE.store(true, Ordering::Relaxed);

        let s = DynamicSleeper::new(ScannerSpeed::Fastest);
        with_var(ENV_SCANNER_SPEED, Some("slow"), || {
            with_var(ENV_SCANNER_IDLE_MODE, Some("false"), || {
                s.refresh_from_env();
                let (factor, max_sleep) = s.read_params();
                assert_eq!(factor, 10.0);
                assert_eq!(max_sleep, Duration::from_secs(15));
                assert!(!SCANNER_IDLE_MODE.load(Ordering::Relaxed));
            });
        });

        SCANNER_IDLE_MODE.store(prev_mode, Ordering::Relaxed);
    }

    #[test]
    #[serial]
    fn test_refresh_from_env_uses_default_speed_override_when_speed_unset() {
        let _guard = ScannerDefaultSpeedGuard::set(ScannerSpeed::Slowest);
        let s = DynamicSleeper::new(ScannerSpeed::Default);

        with_var_unset(ENV_SCANNER_SPEED, || {
            with_var_unset("MINIO_SCANNER_SPEED", || {
                s.refresh_from_env();
                let (factor, max_sleep) = s.read_params();
                assert_eq!(factor, 100.0);
                assert_eq!(max_sleep, Duration::from_secs(15));
            });
        });
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_fastest_never_sleeps() {
        let prev_mode = SCANNER_IDLE_MODE.load(Ordering::Relaxed);
        SCANNER_IDLE_MODE.store(true, Ordering::Relaxed);

        let s = DynamicSleeper::new(ScannerSpeed::Fastest);
        let start = tokio::time::Instant::now();
        s.sleep_folder().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        SCANNER_IDLE_MODE.store(prev_mode, Ordering::Relaxed);
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn test_idle_mode_off_skips_sleep() {
        let prev_mode = SCANNER_IDLE_MODE.load(Ordering::Relaxed);
        SCANNER_IDLE_MODE.store(false, Ordering::Relaxed);

        let s = DynamicSleeper::new(ScannerSpeed::Slowest);
        let start = tokio::time::Instant::now();
        s.sleep_folder().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        SCANNER_IDLE_MODE.store(prev_mode, Ordering::Relaxed);
    }
}
