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

//! Startup bitrot algorithm self-test (rustfs/backlog#1873).
//!
//! A drifted hash implementation fails silently in production: every shard
//! reads back "corrupt", heal rewrites healthy data, and cross-platform
//! clusters disagree about which copy is good. [`run_startup_bitrot_self_test`]
//! pins the algorithms once at process start — the check itself runs in well
//! under a millisecond on 4 KiB, so it executes inline before background
//! services come up and the result is published before the server accepts
//! traffic.
//!
//! Outcome surface:
//! - one structured `bitrot_selftest` log event (`passed`/`failed`/`skipped`),
//! - the `rustfs_bitrot_selftest_status` gauge (1=passed, 0=failed, 2=skipped),
//! - [`bitrot_selftest_passed`] for admin/health surfaces,
//! - `RUSTFS_BITROT_SELFTEST_STRICT=on` turns a failure into a startup error
//!   (MinIO `bitrotSelfTest` Fatal parity); the default only degrades the
//!   status so a bad build cannot brick an existing fleet on upgrade.

use crate::storage_api::startup::background::{BitrotSelfTestError, bitrot_self_test};
use metrics::gauge;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;
use tracing::{debug, error, info};

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_BITROT_SELFTEST: &str = "bitrot_selftest";
const METRIC_BITROT_SELFTEST_STATUS: &str = "rustfs_bitrot_selftest_status";

/// Gauge values for [`METRIC_BITROT_SELFTEST_STATUS`].
const STATUS_PASSED: f64 = 1.0;
const STATUS_FAILED: f64 = 0.0;
const STATUS_SKIPPED: f64 = 2.0;

/// Internal cell values for [`BITROT_SELF_TEST_STATUS`].
const STATUS_CELL_UNSET: u8 = 0;
const STATUS_CELL_PASSED: u8 = 1;
const STATUS_CELL_FAILED: u8 = 2;

static BITROT_SELF_TEST_STATUS: AtomicU8 = AtomicU8::new(STATUS_CELL_UNSET);

/// Last recorded self-test outcome: `None` before the first run, then
/// `Some(true)` on a passing check and `Some(false)` on a failed one (a
/// skipped check never publishes, so it cannot read as a pass). The cell is
/// last-writer-wins rather than set-once: production runs the self-test once,
/// and last-writer-wins keeps tests that exercise both outcomes
/// order-independent.
pub fn bitrot_selftest_passed() -> Option<bool> {
    match BITROT_SELF_TEST_STATUS.load(Ordering::Acquire) {
        STATUS_CELL_UNSET => None,
        STATUS_CELL_PASSED => Some(true),
        STATUS_CELL_FAILED => Some(false),
        _ => None,
    }
}

/// Run the bitrot self-test and publish the outcome. In strict mode a failure
/// is returned as an error so the caller aborts startup.
pub(crate) async fn run_startup_bitrot_self_test(enabled: bool, strict: bool) -> io::Result<()> {
    run_startup_bitrot_self_test_with(enabled, strict, bitrot_self_test).await
}

async fn run_startup_bitrot_self_test_with<F, Fut>(enabled: bool, strict: bool, run_check: F) -> io::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), BitrotSelfTestError>>,
{
    if !enabled {
        gauge!(METRIC_BITROT_SELFTEST_STATUS).set(STATUS_SKIPPED);
        debug!(
            target: "rustfs::main::run",
            event = EVENT_BITROT_SELFTEST,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "skipped",
            reason = "disabled",
            "Bitrot self-test skipped"
        );
        return Ok(());
    }

    let started = Instant::now();
    match run_check().await {
        Ok(()) => {
            BITROT_SELF_TEST_STATUS.store(STATUS_CELL_PASSED, Ordering::Release);
            gauge!(METRIC_BITROT_SELFTEST_STATUS).set(STATUS_PASSED);
            info!(
                target: "rustfs::main::run",
                event = EVENT_BITROT_SELFTEST,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "passed",
                duration_us = started.elapsed().as_micros() as u64,
                "Bitrot self-test passed"
            );
        }
        Err(err) => {
            BITROT_SELF_TEST_STATUS.store(STATUS_CELL_FAILED, Ordering::Release);
            gauge!(METRIC_BITROT_SELFTEST_STATUS).set(STATUS_FAILED);
            error!(
                target: "rustfs::main::run",
                event = EVENT_BITROT_SELFTEST,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                state = "failed",
                duration_us = started.elapsed().as_micros() as u64,
                error = %err,
                "Bitrot self-test failed"
            );
            if strict {
                return Err(io::Error::other(format!("bitrot self-test failed: {err}")));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BITROT_SELF_TEST_STATUS, STATUS_CELL_UNSET, bitrot_selftest_passed, run_startup_bitrot_self_test_with};
    use crate::storage_api::startup::background::BitrotSelfTestError;
    use std::future::ready;
    use std::sync::atomic::Ordering;

    fn failing_check() -> impl Future<Output = Result<(), BitrotSelfTestError>> {
        ready(Err(BitrotSelfTestError::RoundtripReadback {
            algorithm: "HighwayHash256S",
        }))
    }

    /// All scenarios run sequentially inside one test: the status cell is
    /// process-global, so parallel per-scenario tests would race the reset and
    /// read each other's outcomes (the exact order-dependent flake class this
    /// module exists to avoid).
    #[tokio::test]
    async fn startup_self_test_publishes_outcome_and_strict_gates_abort() {
        BITROT_SELF_TEST_STATUS.store(STATUS_CELL_UNSET, Ordering::Release);

        // Skipped: publishes nothing, never fails, never aborts.
        run_startup_bitrot_self_test_with(false, true, || async { Ok(()) })
            .await
            .expect("a disabled self-test must not fail even in strict mode");
        assert_eq!(bitrot_selftest_passed(), None, "a skipped run must leave the status unset");

        // Passing: publishes Some(true), never fails.
        run_startup_bitrot_self_test_with(true, false, || async { Ok(()) })
            .await
            .expect("a passing check must never fail startup");
        assert_eq!(bitrot_selftest_passed(), Some(true), "a passing run must publish Some(true)");

        // Failing, non-strict: publishes Some(false) but startup continues.
        run_startup_bitrot_self_test_with(true, false, failing_check)
            .await
            .expect("a failed check must not abort startup in non-strict mode");
        assert_eq!(bitrot_selftest_passed(), Some(false), "a failing run must publish Some(false)");

        // Failing, strict: startup error carries the failure and the published
        // outcome stays a failure.
        let err = run_startup_bitrot_self_test_with(true, true, failing_check)
            .await
            .expect_err("strict mode must turn a failed check into a startup error");
        assert!(err.to_string().contains("bitrot self-test failed"));
        assert_eq!(bitrot_selftest_passed(), Some(false));
    }
}
