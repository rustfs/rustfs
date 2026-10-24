// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{error, warn};

use super::{
    EVENT_SERVER_CONFIG_COMPATIBILITY_IGNORED, EVENT_SERVER_CONFIG_DECODE_FAILED, EVENT_SERVER_CONFIG_DECRYPT_FAILED,
    HEAL_SUB_SYS, LOG_COMPONENT_CONFIG, LOG_SUBSYSTEM_CONFIG,
};

const RECENT_REPORTS: usize = 64;
const FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Default)]
struct Reports(VecDeque<Report>);

struct Report {
    key: u64,
    reported: Instant,
    suppressed: u64,
}

impl Reports {
    fn observe(&mut self, key: u64, now: Instant, repeat: Option<Duration>) -> Option<u64> {
        if let Some(index) = self.0.iter().position(|report| report.key == key) {
            let mut report = self.0.remove(index)?;
            let emit = repeat.is_some_and(|interval| now.saturating_duration_since(report.reported) >= interval);
            let suppressed = report.suppressed;
            if emit {
                report.reported = now;
                report.suppressed = 0;
            } else {
                report.suppressed = report.suppressed.saturating_add(1);
            }
            self.0.push_back(report);
            return emit.then_some(suppressed);
        }
        if self.0.len() == RECENT_REPORTS {
            self.0.pop_front();
        }
        self.0.push_back(Report {
            key,
            reported: now,
            suppressed: 0,
        });
        Some(0)
    }
}

static COMPATIBILITY_REPORTS: LazyLock<Mutex<Reports>> = LazyLock::new(Mutex::default);
static FAILURE_REPORTS: LazyLock<Mutex<Reports>> = LazyLock::new(Mutex::default);

fn observe(reports: &Mutex<Reports>, key: u64, repeat: Option<Duration>) -> Option<u64> {
    // Never hold this process-local diagnostics lock across tracing or I/O.
    reports
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .observe(key, Instant::now(), repeat)
}

#[derive(Default)]
pub(super) struct CompatibilityDiagnostics {
    digest: DefaultHasher,
    unknown_keys: usize,
    legacy_shapes: usize,
    legacy_heal_keys: u8,
    shape_kinds: u8,
}

const LEGACY_HEAL_KEYS: [&str; 4] = ["bitrotscan", "drive_workers", "max_io", "max_sleep"];
const SHAPE_KINDS: [&str; 6] = ["null", "boolean", "number", "string", "array", "object"];

fn shape(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

impl CompatibilityDiagnostics {
    pub(super) fn unknown_key(&mut self, subsystem: &str, key: &str) {
        ("unknown_key", subsystem, key).hash(&mut self.digest);
        self.unknown_keys += 1;
        if subsystem == HEAL_SUB_SYS
            && let Some(index) = LEGACY_HEAL_KEYS.iter().position(|known| *known == key)
        {
            self.legacy_heal_keys |= 1 << index;
        }
    }

    pub(super) fn legacy_shape(&mut self, subsystem: &str, value: &Value) {
        let kind = shape(value);
        ("legacy_shape", subsystem, kind).hash(&mut self.digest);
        self.legacy_shapes += 1;
        self.shape_kinds |= 1 << kind;
    }

    pub(super) fn report(&self) {
        if self.unknown_keys == 0 && self.legacy_shapes == 0 {
            return;
        }
        if observe(&COMPATIBILITY_REPORTS, self.digest.finish(), None).is_none() {
            return;
        }
        let legacy_heal_keys = LEGACY_HEAL_KEYS
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (self.legacy_heal_keys & (1 << index) != 0).then_some(*key))
            .collect::<Vec<_>>();
        let legacy_shape_kinds = SHAPE_KINDS
            .iter()
            .enumerate()
            .filter_map(|(index, kind)| (self.shape_kinds & (1 << index) != 0).then_some(*kind))
            .collect::<Vec<_>>();
        warn!(
            event = EVENT_SERVER_CONFIG_COMPATIBILITY_IGNORED,
            component = LOG_COMPONENT_CONFIG,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "ignored",
            unknown_key_count = self.unknown_keys,
            legacy_shape_count = self.legacy_shapes,
            ?legacy_heal_keys,
            ?legacy_shape_kinds,
            "Persisted server config contains unsupported settings; unknown fields are retained but not applied"
        );
    }
}

#[derive(Hash)]
pub(super) enum DecodeFailure {
    Corrupt,
    Decrypt,
}

pub(super) fn report_decode_failure(data: &[u8], failure: DecodeFailure, encrypted_size: Option<usize>) {
    let mut digest = DefaultHasher::new();
    failure.hash(&mut digest);
    data.hash(&mut digest);
    let Some(suppressed_attempts) = observe(&FAILURE_REPORTS, digest.finish(), Some(FAILURE_REPORT_INTERVAL)) else {
        return;
    };
    let event = match failure {
        DecodeFailure::Corrupt => EVENT_SERVER_CONFIG_DECODE_FAILED,
        DecodeFailure::Decrypt => EVENT_SERVER_CONFIG_DECRYPT_FAILED,
    };
    error!(
        event,
        component = LOG_COMPONENT_CONFIG,
        subsystem = LOG_SUBSYSTEM_CONFIG,
        state = "failed",
        size = data.len(),
        encrypted_size,
        suppressed_attempts,
        "Persisted server config could not be decoded; the read remains failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_keep_history_without_confusing_keys_with_versions() {
        let mut reports = Reports::default();
        let now = Instant::now();
        assert_eq!(reports.observe(1, now, None), Some(0));
        assert_eq!(reports.observe(2, now, None), Some(0));
        assert_eq!(reports.observe(1, now, None), None);
        for key in 3..=65 {
            assert_eq!(reports.observe(key, now, None), Some(0));
        }
        assert_eq!(reports.0.len(), RECENT_REPORTS);
        assert_eq!(reports.observe(2, now, None), Some(0), "only the least recently used entry is evicted");
    }

    #[test]
    fn failures_report_at_the_interval_boundary_and_count_suppressed_reads() {
        let mut reports = Reports::default();
        let now = Instant::now();
        assert_eq!(reports.observe(1, now, Some(FAILURE_REPORT_INTERVAL)), Some(0));
        assert_eq!(reports.observe(1, now + Duration::from_secs(299), Some(FAILURE_REPORT_INTERVAL)), None);
        assert_eq!(reports.observe(1, now + FAILURE_REPORT_INTERVAL, Some(FAILURE_REPORT_INTERVAL)), Some(1));
        assert_eq!(reports.observe(2, now + FAILURE_REPORT_INTERVAL, Some(FAILURE_REPORT_INTERVAL)), Some(0));
    }
}
