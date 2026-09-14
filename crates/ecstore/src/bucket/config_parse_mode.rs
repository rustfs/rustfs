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

//! Handling of stored bucket sub-configurations whose bytes cannot be parsed
//! (rustfs/backlog#1734): the rollout mode for paths that historically read
//! them as absent, and the metrics that make them visible.
//!
//! The mode only governs those historical degrade paths. Paths that already
//! refuse an unreadable config (the typed getters, the read-modify-write
//! guard, Object Lock and default-encryption decisions, delete-time
//! versioning) refuse in every mode.

use rustfs_config::{DEFAULT_BUCKET_CONFIG_PARSE_MODE, ENV_BUCKET_CONFIG_PARSE_MODE};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, OnceLock};
use tracing::error;

/// Counter of stored XML sub-configurations that failed to parse, labeled
/// `bucket`, `config` and `mode`. Increments on every metadata parse, so it
/// must read zero fleet-wide before the default mode flips to strict.
pub const METRIC_BUCKET_METADATA_PARSE_FAILED_TOTAL: &str = "rustfs_bucket_metadata_parse_failed_total";

/// Gauge of buckets whose most recently parsed metadata holds an unreadable
/// XML sub-configuration, labeled `config`. The counter only moves when a
/// parse runs; this reflects the current state between parses.
pub const METRIC_BUCKET_METADATA_UNPARSABLE_CURRENT: &str = "rustfs_bucket_metadata_unparsable_current";

const LOG_COMPONENT: &str = "ecstore";
const LOG_SUBSYSTEM: &str = "bucket_metadata";
const EVENT_CONFIG_UNREADABLE: &str = "bucket_metadata_config_unreadable";
const EVENT_PARSE_MODE_INVALID: &str = "bucket_config_parse_mode_invalid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketConfigParseMode {
    /// Historical degrade paths keep reading an unreadable config as absent;
    /// the metrics and an error log record every occurrence.
    Permissive,
    /// Historical degrade paths refuse instead.
    Strict,
}

impl BucketConfigParseMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permissive => "permissive",
            Self::Strict => "strict",
        }
    }

    /// Parse a configured value; unset or blank selects the default. An
    /// unknown value is an error rather than a silent fallback.
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        let value = value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_BUCKET_CONFIG_PARSE_MODE);
        match value.to_ascii_lowercase().as_str() {
            "permissive" => Ok(Self::Permissive),
            "strict" => Ok(Self::Strict),
            _ => Err(format!(
                "invalid {ENV_BUCKET_CONFIG_PARSE_MODE} value {value:?}; expected permissive or strict"
            )),
        }
    }
}

/// Validate the configured mode; startup calls this so an invalid value fails
/// the node instead of being guessed at.
pub fn validate_bucket_config_parse_mode_env() -> Result<BucketConfigParseMode, String> {
    BucketConfigParseMode::parse(rustfs_utils::get_env_opt_str(ENV_BUCKET_CONFIG_PARSE_MODE).as_deref())
}

/// The process-wide mode. Startup has already rejected an invalid value; if
/// this is reached without that validation, an invalid value selects strict,
/// never permissive.
pub fn bucket_config_parse_mode() -> BucketConfigParseMode {
    static MODE: OnceLock<BucketConfigParseMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        validate_bucket_config_parse_mode_env().unwrap_or_else(|err| {
            error!(
                event = EVENT_PARSE_MODE_INVALID,
                component = LOG_COMPONENT,
                subsystem = LOG_SUBSYSTEM,
                error = %err,
                "Invalid bucket config parse mode; using strict"
            );
            BucketConfigParseMode::Strict
        })
    })
}

/// Which buckets currently hold which unreadable XML configs, so the gauge
/// reports buckets rather than parse events.
#[derive(Debug, Default)]
struct UnreadableConfigTracker {
    by_bucket: HashMap<String, Vec<&'static str>>,
}

impl UnreadableConfigTracker {
    /// Replace `bucket`'s unreadable set and return the current bucket count
    /// of every config whose count may have changed.
    fn update(&mut self, bucket: &str, configs: Vec<&'static str>) -> Vec<(&'static str, usize)> {
        let previous = if configs.is_empty() {
            self.by_bucket.remove(bucket).unwrap_or_default()
        } else {
            self.by_bucket.insert(bucket.to_string(), configs.clone()).unwrap_or_default()
        };
        let mut touched = previous;
        touched.extend(configs);
        touched.sort_unstable();
        touched.dedup();
        touched
            .into_iter()
            .map(|config| (config, self.by_bucket.values().filter(|set| set.contains(&config)).count()))
            .collect()
    }
}

static TRACKER: LazyLock<Mutex<UnreadableConfigTracker>> = LazyLock::new(Default::default);

fn publish_gauges(bucket: &str, configs: Vec<&'static str>) {
    let changed = TRACKER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .update(bucket, configs);
    for (config, count) in changed {
        metrics::gauge!(METRIC_BUCKET_METADATA_UNPARSABLE_CURRENT, "config" => config).set(count as f64);
    }
}

/// Record the outcome of one metadata parse: every unreadable XML config
/// (config file, stored byte length) counts once, logs at error level, and
/// replaces the bucket's entry in the current-state gauge.
pub(crate) fn record_bucket_config_parse_state(bucket: &str, unreadable: &[(&'static str, usize)]) {
    if !unreadable.is_empty() {
        let mode = bucket_config_parse_mode();
        for (config, _) in unreadable {
            metrics::counter!(
                METRIC_BUCKET_METADATA_PARSE_FAILED_TOTAL,
                "bucket" => bucket.to_string(),
                "config" => *config,
                "mode" => mode.as_str()
            )
            .increment(1);
        }
        error!(
            event = EVENT_CONFIG_UNREADABLE,
            component = LOG_COMPONENT,
            subsystem = LOG_SUBSYSTEM,
            bucket = %bucket,
            configs = ?unreadable,
            mode = mode.as_str(),
            "Stored bucket configuration cannot be parsed"
        );
    }
    publish_gauges(bucket, unreadable.iter().map(|(config, _)| *config).collect());
}

/// Drop a deleted bucket from the current-state gauge.
pub(crate) fn forget_bucket_config_parse_state(bucket: &str) {
    publish_gauges(bucket, Vec::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata::{BUCKET_TAGGING_CONFIG, BUCKET_VERSIONING_CONFIG};

    #[test]
    fn parse_mode_defaults_to_permissive_and_rejects_unknown_values() {
        assert_eq!(BucketConfigParseMode::parse(None), Ok(BucketConfigParseMode::Permissive));
        assert_eq!(BucketConfigParseMode::parse(Some("  ")), Ok(BucketConfigParseMode::Permissive));
        assert_eq!(BucketConfigParseMode::parse(Some("permissive")), Ok(BucketConfigParseMode::Permissive));
        assert_eq!(BucketConfigParseMode::parse(Some(" Strict ")), Ok(BucketConfigParseMode::Strict));

        let err = BucketConfigParseMode::parse(Some("lenient")).expect_err("an unknown mode must not be guessed");
        assert!(err.contains(ENV_BUCKET_CONFIG_PARSE_MODE), "{err}");
        assert!(err.contains("permissive") && err.contains("strict"), "must list the valid values: {err}");
    }

    #[test]
    fn tracker_counts_buckets_not_parse_events() {
        let mut tracker = UnreadableConfigTracker::default();
        assert_eq!(tracker.update("a", vec![BUCKET_VERSIONING_CONFIG]), vec![(BUCKET_VERSIONING_CONFIG, 1)]);
        // Re-parsing the same state does not double count.
        assert_eq!(tracker.update("a", vec![BUCKET_VERSIONING_CONFIG]), vec![(BUCKET_VERSIONING_CONFIG, 1)]);
        assert_eq!(tracker.update("b", vec![BUCKET_VERSIONING_CONFIG]), vec![(BUCKET_VERSIONING_CONFIG, 2)]);

        // A repaired bucket releases its configs; the changed one is reported.
        let mut changed = tracker.update("a", vec![BUCKET_TAGGING_CONFIG]);
        changed.sort_unstable();
        let mut expected = vec![(BUCKET_TAGGING_CONFIG, 1), (BUCKET_VERSIONING_CONFIG, 1)];
        expected.sort_unstable();
        assert_eq!(changed, expected);

        assert_eq!(tracker.update("b", Vec::new()), vec![(BUCKET_VERSIONING_CONFIG, 0)]);
        assert_eq!(tracker.update("never-unreadable", Vec::new()), Vec::new());
    }

    #[test]
    fn every_unreadable_config_increments_the_labeled_counter() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_bucket_config_parse_state("metric-bucket", &[(BUCKET_VERSIONING_CONFIG, 12), (BUCKET_TAGGING_CONFIG, 3)]);
            record_bucket_config_parse_state("metric-bucket", &[(BUCKET_VERSIONING_CONFIG, 12)]);
            record_bucket_config_parse_state("metric-clean-bucket", &[]);
        });

        let mut versioning = 0;
        let mut tagging = 0;
        for (composite, _, _, value) in snapshotter.snapshot().into_vec() {
            if composite.key().name() != METRIC_BUCKET_METADATA_PARSE_FAILED_TOTAL {
                continue;
            }
            let labels: HashMap<_, _> = composite.key().labels().map(|l| (l.key(), l.value())).collect();
            assert_eq!(labels.get("bucket"), Some(&"metric-bucket"));
            assert_eq!(labels.get("mode"), Some(&bucket_config_parse_mode().as_str()));
            let metrics_util::debugging::DebugValue::Counter(count) = value else {
                panic!("parse failures must be a counter");
            };
            match labels.get("config") {
                Some(&config) if config == BUCKET_VERSIONING_CONFIG => versioning += count,
                Some(&config) if config == BUCKET_TAGGING_CONFIG => tagging += count,
                other => panic!("unexpected config label {other:?}"),
            }
        }
        assert_eq!((versioning, tagging), (2, 1));
    }
}
