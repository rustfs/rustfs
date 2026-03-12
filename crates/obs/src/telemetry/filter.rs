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

use smallvec::SmallVec;
use tracing_subscriber::EnvFilter;

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
    matches!(level.to_ascii_lowercase().as_str(), "trace" | "debug")
}

fn rust_log_requests_verbose(rust_log: &str) -> bool {
    rust_log.split(',').any(|directive| {
        let directive = directive.trim();
        if directive.is_empty() {
            return false;
        }

        let level = directive.rsplit('=').next().unwrap_or("").trim();
        is_verbose_level(level)
    })
}

fn should_suppress_noisy_crates(logger_level: &str, default_level: Option<&str>, rust_log: Option<&str>) -> bool {
    if let Some(level) = default_level {
        return !is_verbose_level(level);
    }

    if let Some(rust_log) = rust_log {
        return !rust_log_requests_verbose(rust_log);
    }

    !is_verbose_level(logger_level)
}

pub(super) fn build_env_filter(logger_level: &str, default_level: Option<&str>) -> EnvFilter {
    let level = default_level.unwrap_or(logger_level);
    let rust_log = if default_level.is_none() {
        std::env::var("RUST_LOG").ok()
    } else {
        None
    };
    let mut filter = default_level
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level)));

    // Suppress chatty infrastructure crates unless the operator explicitly
    // requests trace/debug output.
    if should_suppress_noisy_crates(logger_level, default_level, rust_log.as_deref()) {
        let directives: SmallVec<[&str; 6]> =
            smallvec::smallvec!["hyper", "tonic", "h2", "reqwest", "tower", "rustfs::server::http"];
        for directive in directives {
            if directive == "rustfs::server::http" {
                // For HTTP request logs, default to WARN to reduce volume in benchmarks/production
                filter = filter.add_directive(format!("{directive}=warn").parse().unwrap());
            } else {
                filter = filter.add_directive(format!("{directive}=off").parse().unwrap());
            }
        }
    }

    filter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_env_filter_default_level_overrides() {
        // Ensure that providing a default_level uses it instead of logger_level.
        let filter = build_env_filter("debug", Some("error"));
        // The Debug output uses `LevelFilter::ERROR` for the error level directive.
        let dbg = format!("{filter:?}");
        assert!(
            dbg.contains("LevelFilter::ERROR"),
            "Expected 'LevelFilter::ERROR' in filter debug output: {dbg}"
        );
    }

    #[test]
    fn test_build_env_filter_suppresses_noisy_crates() {
        // For info level, hyper/tonic/etc. should be suppressed with OFF.
        let filter = build_env_filter("debug", Some("info"));
        let dbg = format!("{filter:?}");
        // The Debug output uses `LevelFilter::OFF` for suppressed crates.
        assert!(
            dbg.contains("LevelFilter::OFF"),
            "Expected 'LevelFilter::OFF' suppression directives in filter: {dbg}"
        );
    }

    #[test]
    fn test_build_env_filter_debug_no_suppression() {
        // For debug level, our code does NOT inject any OFF directives.
        let filter = build_env_filter("info", Some("debug"));
        let dbg = format!("{filter:?}");
        // Verify the filter builds without panicking and contains the debug level.
        assert!(!dbg.is_empty());
        assert!(
            dbg.contains("LevelFilter::DEBUG"),
            "Expected 'LevelFilter::DEBUG' in filter debug output: {dbg}"
        );
    }

    #[test]
    fn test_should_suppress_noisy_crates_respects_rust_log_debug() {
        assert!(!should_suppress_noisy_crates("info", None, Some("debug")));
        assert!(!should_suppress_noisy_crates("info", None, Some("s3=info,hyper=debug")));
        assert!(should_suppress_noisy_crates("info", None, Some("info")));
    }
}
