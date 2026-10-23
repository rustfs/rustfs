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

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const LEGACY_AMBIGUITY_LOG_INTERVAL: Duration = Duration::from_secs(15 * 60);
const LEGACY_AMBIGUITY_LOG_CAPACITY: usize = 4096;
const EVENT_HEAL_LEGACY_AMBIGUITY_LOG_STATE: &str = "heal_legacy_ambiguity_log_state";
const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_FAILURE_LOGGING: &str = "failure_logging";

#[derive(Clone, PartialEq, Eq, Hash)]
struct ObjectFailureKey {
    bucket: String,
    object: String,
    version_id: Option<String>,
    pool: Option<usize>,
    set: Option<usize>,
}

#[derive(Default)]
struct LegacyAmbiguityLogs {
    entries: HashMap<ObjectFailureKey, Instant>,
    expiry: VecDeque<(ObjectFailureKey, Instant)>,
    last_saturation_notice: Option<Instant>,
}

impl LegacyAmbiguityLogs {
    fn take_saturation_notice(&mut self, now: Instant) -> bool {
        if self
            .last_saturation_notice
            .is_some_and(|last| now.saturating_duration_since(last) < LEGACY_AMBIGUITY_LOG_INTERVAL)
        {
            return false;
        }
        self.last_saturation_notice = Some(now);
        true
    }

    fn allow(&mut self, key: ObjectFailureKey, now: Instant, capacity: usize) -> bool {
        while self
            .expiry
            .front()
            .is_some_and(|(_, logged_at)| now.saturating_duration_since(*logged_at) >= LEGACY_AMBIGUITY_LOG_INTERVAL)
        {
            if let Some((expired, _)) = self.expiry.pop_front() {
                self.entries.remove(&expired);
            }
        }
        // Do not evict a live suppression to admit another identity: that
        // would make a busy node repeat the earlier object's failure log.
        if self.entries.contains_key(&key) || self.entries.len() >= capacity {
            return false;
        }
        self.entries.insert(key.clone(), now);
        self.expiry.push_back((key, now));
        true
    }
}

/// A shared gate across storage, task and scheduler logs. It never changes
/// task execution, disposition backoff or durable repair responsibility.
pub(crate) fn log_legacy_ambiguity(
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    pool: Option<usize>,
    set: Option<usize>,
) -> bool {
    static LOGS: OnceLock<Mutex<LegacyAmbiguityLogs>> = OnceLock::new();
    let key = ObjectFailureKey {
        bucket: bucket.to_owned(),
        object: object.to_owned(),
        version_id: version_id.filter(|version| !version.is_empty()).map(ToOwned::to_owned),
        pool,
        set,
    };
    let (allowed, saturated) = {
        let mut logs = LOGS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        let allowed = logs.allow(key.clone(), now, LEGACY_AMBIGUITY_LOG_CAPACITY);
        let saturated = !allowed && !logs.entries.contains_key(&key) && logs.take_saturation_notice(now);
        (allowed, saturated)
    };
    if saturated {
        tracing::warn!(
            event = EVENT_HEAL_LEGACY_AMBIGUITY_LOG_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_FAILURE_LOGGING,
            state = "capacity_reached",
            capacity = LEGACY_AMBIGUITY_LOG_CAPACITY,
            "Legacy delete ambiguity log capacity reached; further failures remain visible in task status"
        );
    }
    allowed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(object: &str) -> ObjectFailureKey {
        ObjectFailureKey {
            bucket: "bucket".to_owned(),
            object: object.to_owned(),
            version_id: None,
            pool: Some(0),
            set: Some(0),
        }
    }

    #[test]
    fn legacy_ambiguity_logs_once_within_fifteen_minutes_across_retries() {
        let now = Instant::now();
        let mut logs = LegacyAmbiguityLogs::default();
        assert!(logs.allow(key("object"), now, 2));
        for second in [0, 60, 120, 240, 480, 899] {
            assert!(
                !logs.allow(key("object"), now + Duration::from_secs(second), 2),
                "a repeated typed failure must stay quiet at second {second}"
            );
        }
        assert!(logs.allow(key("object"), now + LEGACY_AMBIGUITY_LOG_INTERVAL, 2));
    }

    #[test]
    fn legacy_ambiguity_log_capacity_never_evicts_a_live_suppression() {
        let now = Instant::now();
        let mut logs = LegacyAmbiguityLogs::default();
        assert!(logs.allow(key("first"), now, 2));
        assert!(logs.allow(key("second"), now, 2));
        assert!(!logs.allow(key("overflow"), now, 2));
        assert!(!logs.allow(key("first"), now, 2));
        assert_eq!(logs.entries.len(), 2);
        assert_eq!(logs.expiry.len(), 2);
        assert!(logs.allow(key("overflow"), now + LEGACY_AMBIGUITY_LOG_INTERVAL, 2));
        assert_eq!(logs.entries.len(), 1);
        assert_eq!(logs.expiry.len(), 1);
    }

    #[test]
    fn legacy_ambiguity_logs_distinguish_versions_and_storage_scopes() {
        let now = Instant::now();
        let mut logs = LegacyAmbiguityLogs::default();
        let first = key("object");
        assert!(logs.allow(first.clone(), now, 8));
        let mut version = first.clone();
        version.version_id = Some("version".to_owned());
        assert!(logs.allow(version, now, 8));
        let mut pool = first.clone();
        pool.pool = Some(1);
        assert!(logs.allow(pool, now, 8));
        let mut set = first;
        set.set = Some(1);
        assert!(logs.allow(set, now, 8));
    }

    #[test]
    fn legacy_ambiguity_log_saturation_notice_is_limited_to_one_per_window() {
        let now = Instant::now();
        let mut logs = LegacyAmbiguityLogs::default();
        assert!(logs.take_saturation_notice(now));
        assert!(!logs.take_saturation_notice(now));
        assert!(!logs.take_saturation_notice(now + Duration::from_secs(899)));
        assert!(logs.take_saturation_notice(now + LEGACY_AMBIGUITY_LOG_INTERVAL));
    }
}
