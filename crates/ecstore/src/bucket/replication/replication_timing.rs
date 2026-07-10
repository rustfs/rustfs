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

//! Env-overridable timing knobs for replication background loops
//! (backlog#1147 repl-4).
//!
//! Defaults are identical to the previously hardcoded values; the env vars
//! exist so tests and operators can shorten the loops (e.g. fast e2e runs).
//! Every value is read **once, when the owning background task starts** —
//! changing an env var mid-run does not affect already-running loops.
//! Invalid values fall back to the default with a `warn!`; values below
//! [`MIN_INTERVAL`] are clamped so an override of `0`/near-zero can never
//! busy-spin a loop (`tokio::time::interval` also panics on a zero period).

use std::time::Duration;
use tracing::warn;

use super::replication_logging::{LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION};

/// Overrides the remote-target health-check probe interval, in milliseconds.
/// Default: `5000` (5s). Test/ops tuning only.
pub(crate) const ENV_REPL_HEALTH_CHECK_INTERVAL_MS: &str = "RUSTFS_REPL_HEALTH_CHECK_INTERVAL_MS";

/// Overrides the MRF persistence flush interval, in milliseconds.
/// Default: `10000` (10s). Test/ops tuning only.
pub(crate) const ENV_REPL_MRF_FLUSH_INTERVAL_MS: &str = "RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS";

/// Overrides the upper bound of the random resync retry-poll sleep, in
/// milliseconds. Default: `60000` (60s). Test/ops tuning only.
pub(crate) const ENV_REPL_RESYNC_POLL_MAX_MS: &str = "RUSTFS_REPL_RESYNC_POLL_MAX_MS";

const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(5_000);
const DEFAULT_MRF_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_RESYNC_POLL_MAX: Duration = Duration::from_millis(60_000);

/// Hard floor for every replication timing override. Values below this are
/// clamped (with a `warn!`) so a pathological override cannot busy-spin a
/// background loop. The defaults are all far above this floor, so clamping
/// never alters default behavior.
pub(crate) const MIN_INTERVAL: Duration = Duration::from_millis(10);

/// Remote-target health-check probe interval
/// ([`ENV_REPL_HEALTH_CHECK_INTERVAL_MS`], default 5s).
pub(crate) fn health_check_interval() -> Duration {
    env_interval(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, DEFAULT_HEALTH_CHECK_INTERVAL)
}

/// MRF persistence flush interval
/// ([`ENV_REPL_MRF_FLUSH_INTERVAL_MS`], default 10s).
pub(crate) fn mrf_flush_interval() -> Duration {
    env_interval(ENV_REPL_MRF_FLUSH_INTERVAL_MS, DEFAULT_MRF_FLUSH_INTERVAL)
}

/// Upper bound of the random resync retry-poll sleep
/// ([`ENV_REPL_RESYNC_POLL_MAX_MS`], default 60s).
pub(crate) fn resync_poll_max_sleep() -> Duration {
    env_interval(ENV_REPL_RESYNC_POLL_MAX_MS, DEFAULT_RESYNC_POLL_MAX)
}

/// Resolve `name` as a millisecond interval: absent → `default` (silently);
/// unparsable → `default` with a `warn!`; parsed but below [`MIN_INTERVAL`] →
/// clamped to [`MIN_INTERVAL`] with a `warn!`.
fn env_interval(name: &str, default: Duration) -> Duration {
    let raw = match std::env::var(name) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                env = name,
                default_ms = default.as_millis() as u64,
                "replication timing override is not valid unicode — falling back to default"
            );
            return default;
        }
    };
    match raw.trim().parse::<u64>() {
        Ok(ms) => {
            let value = Duration::from_millis(ms);
            if value < MIN_INTERVAL {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    env = name,
                    value_ms = ms,
                    min_ms = MIN_INTERVAL.as_millis() as u64,
                    "replication timing override below minimum — clamping to avoid busy-spin"
                );
                MIN_INTERVAL
            } else {
                value
            }
        }
        Err(_) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                env = name,
                value = %raw,
                default_ms = default.as_millis() as u64,
                "invalid replication timing override — falling back to default"
            );
            default
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    // SAFETY: this helper is only used from `#[serial]` tests, so no
    // concurrent reader/writer can access the process environment while
    // `env::set_var`/`env::remove_var` is active.
    #[allow(unsafe_code)]
    fn with_env<F: FnOnce()>(name: &str, value: Option<&str>, test_fn: F) {
        let original = env::var_os(name);
        match value {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));
        match original {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }
        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    /// The env var names are a cross-process contract: `crates/e2e_test/src/common.rs`
    /// (`replication_fast_env`) passes them by literal name to spawned server
    /// processes. Renaming one must fail here first.
    #[test]
    fn env_var_names_are_a_cross_process_contract() {
        assert_eq!(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, "RUSTFS_REPL_HEALTH_CHECK_INTERVAL_MS");
        assert_eq!(ENV_REPL_MRF_FLUSH_INTERVAL_MS, "RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS");
        assert_eq!(ENV_REPL_RESYNC_POLL_MAX_MS, "RUSTFS_REPL_RESYNC_POLL_MAX_MS");
    }

    #[test]
    #[serial]
    fn unset_env_uses_default() {
        with_env(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, None, || {
            assert_eq!(health_check_interval(), Duration::from_secs(5));
        });
        with_env(ENV_REPL_MRF_FLUSH_INTERVAL_MS, None, || {
            assert_eq!(mrf_flush_interval(), Duration::from_secs(10));
        });
        with_env(ENV_REPL_RESYNC_POLL_MAX_MS, None, || {
            assert_eq!(resync_poll_max_sleep(), Duration::from_secs(60));
        });
    }

    #[test]
    #[serial]
    fn valid_override_is_applied() {
        with_env(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, Some("250"), || {
            assert_eq!(health_check_interval(), Duration::from_millis(250));
        });
        with_env(ENV_REPL_MRF_FLUSH_INTERVAL_MS, Some("100"), || {
            assert_eq!(mrf_flush_interval(), Duration::from_millis(100));
        });
        with_env(ENV_REPL_RESYNC_POLL_MAX_MS, Some("500"), || {
            assert_eq!(resync_poll_max_sleep(), Duration::from_millis(500));
        });
        // Surrounding whitespace is tolerated.
        with_env(ENV_REPL_MRF_FLUSH_INTERVAL_MS, Some(" 750 "), || {
            assert_eq!(mrf_flush_interval(), Duration::from_millis(750));
        });
    }

    #[test]
    #[serial]
    fn invalid_override_falls_back_to_default() {
        for bad in ["abc", "", "-5", "1.5", "10s", "99999999999999999999999999"] {
            with_env(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, Some(bad), || {
                assert_eq!(health_check_interval(), Duration::from_secs(5), "input {bad:?}");
            });
            with_env(ENV_REPL_MRF_FLUSH_INTERVAL_MS, Some(bad), || {
                assert_eq!(mrf_flush_interval(), Duration::from_secs(10), "input {bad:?}");
            });
            with_env(ENV_REPL_RESYNC_POLL_MAX_MS, Some(bad), || {
                assert_eq!(resync_poll_max_sleep(), Duration::from_secs(60), "input {bad:?}");
            });
        }
    }

    #[test]
    #[serial]
    fn zero_or_tiny_override_is_clamped_to_minimum() {
        for tiny in ["0", "1", "9"] {
            with_env(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, Some(tiny), || {
                assert_eq!(health_check_interval(), MIN_INTERVAL, "input {tiny:?}");
            });
            with_env(ENV_REPL_MRF_FLUSH_INTERVAL_MS, Some(tiny), || {
                assert_eq!(mrf_flush_interval(), MIN_INTERVAL, "input {tiny:?}");
            });
            with_env(ENV_REPL_RESYNC_POLL_MAX_MS, Some(tiny), || {
                assert_eq!(resync_poll_max_sleep(), MIN_INTERVAL, "input {tiny:?}");
            });
        }
        // Exactly the minimum is accepted as-is.
        with_env(ENV_REPL_HEALTH_CHECK_INTERVAL_MS, Some("10"), || {
            assert_eq!(health_check_interval(), MIN_INTERVAL);
        });
    }
}
