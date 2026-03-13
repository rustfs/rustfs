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

//! Log filtering utilities for tracing subscribers.
//!
//! This module provides helper functions for building `EnvFilter` instances
//! used across different logging backends (stdout, file, OpenTelemetry).

use std::env;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

/// Build an `EnvFilter` from the given log level string.
///
/// If `default_level` is provided, it is used directly. Otherwise, the
/// `RUST_LOG` environment variable takes precedence over `logger_level`.
/// For non-verbose levels (`info`, `warn`, `error`), noisy internal crates
/// (`hyper`, `tonic`, `h2`, `reqwest`, `tower`) are automatically silenced
/// based on the effective log configuration.
///
/// # Arguments
/// * `logger_level` - The desired log level string (e.g., `"info"`, `"debug"`).
/// * `default_level` - An optional override that replaces `logger_level` as the
///   base directive; useful when the caller wants to force a specific level
///   regardless of what is stored in config.
///
/// # Returns
/// A configured `EnvFilter` ready to be attached to a `tracing_subscriber` registry.
fn is_verbose_level(level: &str) -> bool {
    let s = level.trim().to_ascii_lowercase();
    s.contains("trace") || s.contains("debug")
}

fn rust_log_requests_verbose(rust_log: &str) -> bool {
    rust_log.split(',').any(|directive| {
        let directive = directive.trim();
        if directive.is_empty() {
            return false;
        }

        // If the directive is just a level (e.g. "debug"), check it.
        // If it's a module=level (e.g. "my_crate=debug"), we split by '='.
        // Note: rsplit keeps the last part as the first element of iterator if we take next().
        if let Some(level_part) = directive.rsplit('=').next() {
            is_verbose_level(level_part)
        } else {
            false
        }
    })
}

fn should_suppress_noisy_crates(logger_level: &str, default_level: Option<&str>, rust_log: Option<&str>) -> bool {
    if let Some(level) = default_level {
        return !is_verbose_level(level);
    }

    if let Some(rust_log) = rust_log {
        // If RUST_LOG is present, we check if ANY part of it requests verbose logging.
        // If the user explicitly asks for debug/trace anywhere, we assume they are debugging
        // and might want to see third-party logs unless they silence them.
        // HOWEVER, standard practice is: if RUST_LOG is set, we respect it fully and
        // adding extra suppressions might be confusing.
        // But the original logic was: "For non-verbose levels... noisy crates are silenced".
        // So if RUST_LOG="info", we suppress. If RUST_LOG="debug", we don't.
        return !rust_log_requests_verbose(rust_log);
    }

    !is_verbose_level(logger_level)
}

pub(super) fn build_env_filter(logger_level: &str, default_level: Option<&str>) -> EnvFilter {
    // 1. Determine the base filter source.
    // If `default_level` is set (e.g. forced override), we use it.
    // Otherwise, we look at `RUST_LOG`.
    // If `RUST_LOG` is missing, we fallback to `logger_level` (from config/env var `RUSTFS_OBS_LOGGER_LEVEL`).

    let rust_log_env = env::var("RUST_LOG").ok();

    // Logic:
    // - If default_level is Some, use EnvFilter::new(default_level).
    // - Else if RUST_LOG is set, use EnvFilter::new(rust_log).
    // - Else use EnvFilter::new(logger_level).

    let mut filter = if let Some(lvl) = default_level {
        EnvFilter::new(lvl)
    } else if let Some(ref rust_log) = rust_log_env {
        EnvFilter::new(rust_log)
    } else {
        EnvFilter::new(logger_level)
    };

    // 2. Apply noisy crate suppression if needed.
    // We only suppress if the effective configuration is NOT verbose (i.e. not debug/trace).
    if should_suppress_noisy_crates(logger_level, default_level, rust_log_env.as_deref()) {
        let directives = [
            ("hyper", LevelFilter::OFF),
            ("tonic", LevelFilter::OFF),
            ("h2", LevelFilter::OFF),
            ("reqwest", LevelFilter::OFF),
            ("tower", LevelFilter::OFF),
            // HTTP request logs are demoted to WARN to reduce volume in production.
            ("rustfs::server::http", LevelFilter::WARN),
        ];

        for (crate_name, level) in directives {
            // We use `add_directive` which effectively appends to the filter.
            // If RUST_LOG already specified `hyper=debug`, adding `hyper=off` later MIGHT override it
            // depending on specificity, but usually the last directive wins or the most specific one.
            // EnvFilter parsing order matters.

            // To be safe and respectful of RUST_LOG, we should arguably NOT override if RUST_LOG is set?
            // BUT, the requirement says: "For non-verbose levels... noisy internal crates... are silenced".
            // So if RUST_LOG="info", we DO want to silence hyper.
            // If RUST_LOG="hyper=debug", `should_suppress_noisy_crates` returns false, so we won't silence it.

            match format!("{crate_name}={level}").parse() {
                Ok(directive) => filter = filter.add_directive(directive),
                Err(e) => {
                    eprintln!("obs: invalid log filter directive '{crate_name}={level}': {e}");
                }
            }
        }
    }

    filter
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_env;

    #[test]
    fn test_is_verbose_level() {
        assert!(is_verbose_level("debug"));
        assert!(is_verbose_level("trace"));
        assert!(is_verbose_level("DEBUG"));
        assert!(is_verbose_level("info,rustfs=debug"));
        assert!(!is_verbose_level("info"));
        assert!(!is_verbose_level("warn"));
    }

    #[test]
    fn test_rust_log_requests_verbose() {
        assert!(rust_log_requests_verbose("debug"));
        assert!(rust_log_requests_verbose("rustfs=debug"));
        assert!(rust_log_requests_verbose("info,rustfs=trace"));
        assert!(!rust_log_requests_verbose("info"));
        assert!(!rust_log_requests_verbose("rustfs=info"));
    }

    #[test]
    fn test_should_suppress() {
        // Case 1: logger_level="info", no RUST_LOG -> suppress
        assert!(should_suppress_noisy_crates("info", None, None));

        // Case 2: logger_level="debug", no RUST_LOG -> no suppress
        assert!(!should_suppress_noisy_crates("debug", None, None));

        // Case 3: RUST_LOG="info" -> suppress
        assert!(should_suppress_noisy_crates("debug", None, Some("info")));

        // Case 4: RUST_LOG="debug" -> no suppress
        assert!(!should_suppress_noisy_crates("info", None, Some("debug")));

        // Case 5: RUST_LOG="rustfs=debug" -> no suppress
        assert!(!should_suppress_noisy_crates("info", None, Some("rustfs=debug")));
    }

    #[test]
    fn test_build_env_filter_injects_suppressions_without_rust_log() {
        // When RUST_LOG is not set and the base level is non-verbose ("info"),
        // build_env_filter should inject suppression directives for noisy crates.
        temp_env::with_var("RUST_LOG", None::<&str>, || {
            let filter = build_env_filter("info", None);
            let filter_str = filter.to_string();

            for noisy_crate in ["hyper", "tonic", "h2", "reqwest", "tower"] {
                assert!(
                    filter_str.contains(noisy_crate),
                    "expected EnvFilter to contain suppression directive for `{}`; got `{}`",
                    noisy_crate,
                    filter_str
                );
            }
        });
    }

    #[test]
    fn test_build_env_filter_respects_verbose_rust_log() {
        // When RUST_LOG requests a verbose level, automatic noisy-crate suppression
        // should not be applied, even if the logger_level is non-verbose.
        temp_env::with_var("RUST_LOG", Some("debug"), || {
            let filter = build_env_filter("info", None);
            let filter_str = filter.to_string();

            // We assume "off" is used to silence noisy crates; absence of these
            // directives indicates that suppression was not injected.
            for noisy_crate in ["hyper", "tonic", "h2", "reqwest", "tower"] {
                let directive = format!("{}=off", noisy_crate);
                assert!(
                    !filter_str.contains(&directive),
                    "did not expect EnvFilter to contain `{}` when RUST_LOG is verbose; got `{}`",
                    directive,
                    filter_str
                );
            }
        });
    }

    #[test]
    fn test_build_env_filter_precedence_of_rust_log_over_logger_level() {
        // When default_level is None, RUST_LOG should override logger_level.
        temp_env::with_var("RUST_LOG", Some("warn"), || {
            let filter = build_env_filter("debug", None);
            let filter_str = filter.to_string();

            assert!(
                filter_str.contains("warn"),
                "expected EnvFilter to reflect RUST_LOG=warn; got `{}`",
                filter_str
            );
        });
    }
}
