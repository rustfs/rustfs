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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Instant;

use rustfs_config::{DEFAULT_SCANNER_IDLE_MODE, DEFAULT_SCANNER_SPEED, ENV_SCANNER_IDLE_MODE, ENV_SCANNER_SPEED, ScannerSpeed};
use tokio::time::Duration;

const MIN_SLEEP: Duration = Duration::from_millis(1);

fn scanner_env_config() -> (ScannerSpeed, bool) {
    let speed_str = rustfs_utils::get_env_str(ENV_SCANNER_SPEED, DEFAULT_SCANNER_SPEED);
    let speed = ScannerSpeed::from_env_str(&speed_str);
    let idle_mode = rustfs_utils::get_env_bool(ENV_SCANNER_IDLE_MODE, DEFAULT_SCANNER_IDLE_MODE);
    (speed, idle_mode)
}

/// When `true` (default), the scanner throttles itself between operations.
/// When `false`, all sleeps are skipped and the scanner runs at full speed.
pub static SCANNER_IDLE_MODE: AtomicBool = AtomicBool::new(DEFAULT_SCANNER_IDLE_MODE);

/// Global scanner sleeper initialized from the `RUSTFS_SCANNER_SPEED` and
/// `RUSTFS_SCANNER_IDLE_MODE` environment variables.
pub static SCANNER_SLEEPER: LazyLock<DynamicSleeper> = LazyLock::new(|| {
    let (speed, idle_mode) = scanner_env_config();
    SCANNER_IDLE_MODE.store(idle_mode, Ordering::Relaxed);

    DynamicSleeper::new(speed)
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
            return;
        }
        let sleep_dur = Duration::from_secs_f64(MIN_SLEEP.as_secs_f64() * factor).min(max_sleep);
        if !sleep_dur.is_zero() {
            tokio::time::sleep(sleep_dur).await;
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

    /// Swap in parameters from a new speed preset (for runtime reconfiguration).
    pub fn update(&self, speed: ScannerSpeed) {
        let mut f = self.inner.factor.write().unwrap_or_else(|e| e.into_inner());
        *f = speed.sleep_factor();
        let mut m = self.inner.max_sleep.write().unwrap_or_else(|e| e.into_inner());
        *m = speed.max_sleep();
    }

    /// Reload speed and idle-mode settings from the current environment.
    pub fn refresh_from_env(&self) {
        let (speed, idle_mode) = scanner_env_config();
        self.update(speed);
        SCANNER_IDLE_MODE.store(idle_mode, Ordering::Relaxed);
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
            return;
        }
        let elapsed = self.start.elapsed();
        let sleep_dur = Duration::from_secs_f64(elapsed.as_secs_f64() * factor)
            .max(MIN_SLEEP)
            .min(max_sleep);
        if !sleep_dur.is_zero() {
            tokio::time::sleep(sleep_dur).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use temp_env::with_var;

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
