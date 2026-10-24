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
use serial_test::serial;

struct IdleGuard(bool);

impl IdleGuard {
    fn enabled() -> Self {
        Self(SCANNER_IDLE_MODE.swap(true, Ordering::Relaxed))
    }
}

impl Drop for IdleGuard {
    fn drop(&mut self) {
        SCANNER_IDLE_MODE.store(self.0, Ordering::Relaxed);
    }
}

#[test]
fn debt_boundaries_and_overflow() {
    let config = DynamicSleeper::new(ScannerSpeed::Default).read_config();
    let now = Instant::now();
    let mut debt = PacingDebt::default();
    debt.charge(Duration::ZERO, now);
    assert!(!debt.due(config, now));
    debt.charge(PACING_QUANTUM, now);
    assert!(debt.due(config, now));
    debt.reset();
    debt.charge(PACING_QUANTUM + Duration::from_nanos(1), now);
    assert!(debt.due(config, now));
    debt.reset();
    for _ in 0..PACING_MAX_OBJECTS - 1 {
        debt.charge(Duration::from_nanos(1), now);
    }
    assert!(!debt.due(config, now));
    assert!(debt.due(config, now + PACING_MAX_AGE));
    debt.charge(Duration::from_nanos(1), now);
    assert!(debt.due(config, now));
    debt.charge(Duration::MAX, now);
    debt.charge(Duration::MAX, now);
    assert_eq!(debt.pending, Duration::MAX);
    assert_eq!(proportional_delay(Duration::MAX, config), config.max_sleep);
    assert_eq!(proportional_delay(Duration::ZERO, config), Duration::ZERO);
    for factor in [f64::NAN, f64::INFINITY, -1.0] {
        assert_eq!(
            proportional_delay(Duration::from_secs(1), PacingConfig { factor, ..config }),
            Duration::ZERO
        );
    }
}

#[tokio::test(start_paused = true)]
#[serial]
async fn tiny_objects_conserve_debt_in_four_batches() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    let mut debt = PacingDebt::default();
    let start = Instant::now();
    for _ in 0..100 {
        sleeper.pace_object(&mut debt, Duration::from_micros(20)).await;
    }
    sleeper.flush_pacing(&mut debt).await;
    assert_eq!(debt.charged_objects, 100);
    assert_eq!(debt.batches, 4);
    assert_eq!(start.elapsed(), Duration::from_millis(4));
    assert_eq!(debt.pending, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn short_bucket_tails_and_walkers_remain_independent() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    let mut first = PacingDebt::default();
    let mut second = PacingDebt::default();
    sleeper.pace_object(&mut first, Duration::from_micros(20)).await;
    sleeper.pace_object(&mut second, Duration::from_micros(20)).await;
    assert_eq!(first.pending, Duration::from_micros(40));
    assert_eq!(second.pending, Duration::from_micros(40));
    sleeper.flush_pacing(&mut first).await;
    assert_eq!(first.pending, Duration::ZERO);
    assert_eq!(second.pending, Duration::from_micros(40));
    sleeper.flush_pacing(&mut second).await;
    for _ in 0..10 {
        let mut debt = PacingDebt::default();
        sleeper.pace_object(&mut debt, Duration::from_micros(20)).await;
        sleeper.flush_pacing(&mut debt).await;
        assert_eq!(debt.batches, 1);
        assert_eq!(debt.pending, Duration::ZERO);
    }
}

#[tokio::test(start_paused = true)]
#[serial]
async fn aged_debt_is_repaid_at_the_next_folder_boundary() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    let mut debt = PacingDebt::default();
    sleeper.pace_object(&mut debt, Duration::from_micros(20)).await;
    tokio::time::advance(PACING_MAX_AGE).await;
    sleeper.sleep_folder_paced(&mut debt).await;
    assert_eq!(debt.pending, Duration::ZERO);
    assert_eq!(debt.batches, 1);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn positive_max_wait_reduction_preserves_pending_debt() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    let mut debt = PacingDebt::default();
    sleeper.pace_object(&mut debt, Duration::from_micros(400)).await;
    sleeper.update_params(3.0, Duration::from_micros(100));
    assert_eq!(debt.pending, Duration::from_micros(800));
    let start = Instant::now();
    sleeper.flush_pacing(&mut debt).await;
    // Tokio timers have millisecond resolution; overshoot repays the debt.
    assert!(start.elapsed() >= Duration::from_micros(800));
    assert_eq!(debt.pending, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn fastest_and_idle_off_clear_pending_debt_even_after_reenable() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    for idle_off in [false, true] {
        let mut debt = PacingDebt::default();
        sleeper.pace_object(&mut debt, Duration::from_micros(20)).await;
        if idle_off {
            sleeper.update_from_runtime_config(2.0, Duration::from_secs(1), false, 64);
        } else {
            sleeper.update(ScannerSpeed::Fastest);
        }
        sleeper.update_from_runtime_config(2.0, Duration::from_secs(1), true, 64);
        let start = Instant::now();
        sleeper.flush_pacing(&mut debt).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert_eq!(debt.pending, Duration::ZERO);
    }
}

#[tokio::test(start_paused = true)]
#[serial]
async fn active_wait_wakes_for_fastest_idle_off_and_smaller_max_wait() {
    let _idle = IdleGuard::enabled();
    for update in 0..3 {
        let sleeper = DynamicSleeper::new(ScannerSpeed::Slowest);
        sleeper.update_from_runtime_config(100.0, Duration::from_secs(15), true, 64);
        let mut debt = PacingDebt::default();
        let start = Instant::now();
        tokio::join!(sleeper.pace_object(&mut debt, Duration::from_secs(1)), async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            match update {
                0 => sleeper.update(ScannerSpeed::Fastest),
                1 => sleeper.update_from_runtime_config(100.0, Duration::from_secs(15), false, 64),
                _ => {
                    sleeper.update_params(2.0, Duration::from_millis(1));
                    // Positive updates preserve the old debt, so the wait
                    // is still active until explicitly disabled.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    sleeper.update(ScannerSpeed::Fastest);
                }
            }
        });
        assert!(start.elapsed() <= Duration::from_millis(12));
        assert_eq!(debt.pending, Duration::ZERO);
    }
}

#[tokio::test(start_paused = true)]
#[serial]
async fn foreground_backoff_flushes_tail_without_double_charging_or_max_wait_cap() {
    let _idle = IdleGuard::enabled();
    let sleeper = DynamicSleeper::new(ScannerSpeed::Default);
    sleeper.update_params(2.0, Duration::from_micros(500));
    let mut debt = PacingDebt::default();
    sleeper.pace_object(&mut debt, Duration::from_micros(20)).await;
    struct ForegroundGuard;
    impl Drop for ForegroundGuard {
        fn drop(&mut self) {
            crate::reset_foreground_read_activity_for_test();
        }
    }
    let _foreground = ForegroundGuard;
    crate::set_foreground_read_activity(25);
    let start = Instant::now();
    sleeper.sleep_folder_paced(&mut debt).await;
    assert_eq!(start.elapsed(), Duration::from_millis(250));
    assert_eq!(debt.pending, Duration::ZERO);
    assert_eq!(debt.batches, 1);
}

#[tokio::test(start_paused = true)]
#[serial]
async fn cancellation_interrupts_long_pacing_without_refunding_disk_io() {
    let _idle = IdleGuard::enabled();
    let disk = tempfile::tempdir().expect("temporary budget identity");
    let sleeper = DynamicSleeper::new(ScannerSpeed::Slowest).for_disk_with_budget_for_tests(
        disk.path(),
        ScannerDiskIoBudgetLimits {
            iops: Some(1),
            bytes_per_sec: Some(1000),
        },
    );
    sleeper.admit_disk_io(1, 100).await;
    let ctx = tokio_util::sync::CancellationToken::new();
    let mut debt = PacingDebt::default();
    let start = Instant::now();
    tokio::join!(
        async {
            tokio::select! {
                biased;
                _ = ctx.cancelled() => {}
                _ = sleeper.pace_object(&mut debt, Duration::from_secs(1)) => panic!("cancel must interrupt the wait"),
            }
        },
        async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            ctx.cancel();
        }
    );
    assert_eq!(start.elapsed(), Duration::from_millis(5));
    let status = scanner_disk_io_budget_status()
        .into_iter()
        .find(|s| s.disk == disk.path().to_string_lossy())
        .expect("disk status");
    assert_eq!(status.charged_ops, 1);
    assert_eq!(status.charged_bytes, 100);
}
