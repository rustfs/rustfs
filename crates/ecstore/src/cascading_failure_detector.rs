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

//! Cascading Failure Detection (P3)
//!
//! This module detects and monitors cascading failures in the cluster where
//! initial failures trigger subsequent failures in other components.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Failure event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureType {
    PeerUnreachable,
    LockTimeout,
    DiskStateCheckTimeout,
    IAMOperationTimeout,
    UploadFailure,
}

impl std::fmt::Display for FailureType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailureType::PeerUnreachable => write!(f, "peer_unreachable"),
            FailureType::LockTimeout => write!(f, "lock_timeout"),
            FailureType::DiskStateCheckTimeout => write!(f, "disk_check_timeout"),
            FailureType::IAMOperationTimeout => write!(f, "iam_timeout"),
            FailureType::UploadFailure => write!(f, "upload_failure"),
        }
    }
}

/// Failure event
#[derive(Debug, Clone)]
struct FailureEvent {
    failure_type: FailureType,
    component: String,
    timestamp: Instant,
}

/// Cascading failure pattern
#[derive(Debug)]
struct CascadingPattern {
    initial_failure: FailureType,
    subsequent_failures: Vec<FailureType>,
    time_window: Duration,
    detected_count: u32,
}

/// Cascading failure detector
pub struct CascadingFailureDetector {
    events: Arc<RwLock<Vec<FailureEvent>>>,
    patterns: Arc<RwLock<Vec<CascadingPattern>>>,
}

impl CascadingFailureDetector {
    pub fn new() -> Self {
        let patterns = vec![
            // Pattern 1: Peer unreachable → Lock timeouts → IAM failures
            CascadingPattern {
                initial_failure: FailureType::PeerUnreachable,
                subsequent_failures: vec![FailureType::LockTimeout, FailureType::IAMOperationTimeout],
                time_window: Duration::from_secs(30),
                detected_count: 0,
            },
            // Pattern 2: Lock timeout → Disk check timeouts → Upload failures
            CascadingPattern {
                initial_failure: FailureType::LockTimeout,
                subsequent_failures: vec![FailureType::DiskStateCheckTimeout, FailureType::UploadFailure],
                time_window: Duration::from_secs(60),
                detected_count: 0,
            },
            // Pattern 3: Peer unreachable → Disk check timeouts → False quorum loss
            CascadingPattern {
                initial_failure: FailureType::PeerUnreachable,
                subsequent_failures: vec![FailureType::DiskStateCheckTimeout],
                time_window: Duration::from_secs(45),
                detected_count: 0,
            },
        ];

        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            patterns: Arc::new(RwLock::new(patterns)),
        }
    }

    /// Record a failure event
    pub async fn record_failure(&self, failure_type: FailureType, component: String) {
        let event = FailureEvent {
            failure_type,
            component: component.clone(),
            timestamp: Instant::now(),
        };

        let mut events = self.events.write().await;
        events.push(event.clone());

        // Detect cascading patterns
        self.detect_cascading_patterns(&events, &event).await;

        // Keep only recent events (last 5 minutes)
        let cutoff = Instant::now() - Duration::from_secs(300);
        events.retain(|e| e.timestamp > cutoff);

        // Record metrics
        metrics::counter!("rustfs_cascading_failure_events_total",
            "type" => failure_type.to_string(),
            "component" => component
        )
        .increment(1);
    }

    /// Detect if recent events match cascading patterns
    async fn detect_cascading_patterns(&self, events: &[FailureEvent], latest_event: &FailureEvent) {
        let mut patterns = self.patterns.write().await;

        for pattern in patterns.iter_mut() {
            // Check if latest event is part of the pattern
            if pattern.initial_failure != latest_event.failure_type
                && !pattern.subsequent_failures.contains(&latest_event.failure_type)
            {
                continue;
            }

            // Look for initial failure within time window
            let window_start = latest_event.timestamp - pattern.time_window;
            let mut found_initial = false;
            let mut found_subsequent = HashMap::new();

            for event in events.iter().rev() {
                if event.timestamp < window_start {
                    break;
                }

                if event.failure_type == pattern.initial_failure {
                    found_initial = true;
                }

                if pattern.subsequent_failures.contains(&event.failure_type) {
                    *found_subsequent.entry(event.failure_type).or_insert(0) += 1;
                }
            }

            // Cascading detected if we have initial failure + at least 2 subsequent failures
            if found_initial && found_subsequent.len() >= 2 {
                pattern.detected_count += 1;

                warn!(
                    "CASCADING FAILURE DETECTED: {} → {:?} (occurrence #{})",
                    pattern.initial_failure, pattern.subsequent_failures, pattern.detected_count
                );

                metrics::counter!("rustfs_cascading_failure_patterns_total",
                    "initial" => pattern.initial_failure.to_string()
                )
                .increment(1);

                // Critical alert if detected multiple times
                if pattern.detected_count >= 3 {
                    error!(
                        "CRITICAL: Cascading failure pattern recurring {} times - cluster may need intervention",
                        pattern.detected_count
                    );
                    metrics::gauge!("rustfs_cascading_failure_critical", "pattern" => pattern.initial_failure.to_string())
                        .set(1.0);
                }
            }
        }
    }

    /// Get failure statistics for monitoring
    pub async fn get_statistics(&self) -> CascadingFailureStats {
        let events = self.events.read().await;
        let patterns = self.patterns.read().await;

        let mut type_counts: HashMap<FailureType, u32> = HashMap::new();
        for event in events.iter() {
            *type_counts.entry(event.failure_type).or_insert(0) += 1;
        }

        let total_pattern_detections: u32 = patterns.iter().map(|p| p.detected_count).sum();

        CascadingFailureStats {
            total_events: events.len(),
            events_by_type: type_counts,
            cascading_patterns_detected: total_pattern_detections,
            recent_window_secs: 300,
        }
    }

    /// Background task to clean old events and reset pattern counters
    pub async fn cleanup_task(&self) {
        const CLEANUP_INTERVAL_SECS: u64 = 300; // 5 minutes
        const PATTERN_RESET_INTERVAL_SECS: u64 = 3600; // 1 hour

        let mut last_pattern_reset = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_secs(CLEANUP_INTERVAL_SECS)).await;

            // Clean old events
            let mut events = self.events.write().await;
            let cutoff = Instant::now() - Duration::from_secs(300);
            let old_len = events.len();
            events.retain(|e| e.timestamp > cutoff);
            let removed = old_len - events.len();
            if removed > 0 {
                debug!("Cleaned up {} old cascading failure events", removed);
            }
            drop(events);

            // Reset pattern counters periodically (hourly)
            if last_pattern_reset.elapsed() > Duration::from_secs(PATTERN_RESET_INTERVAL_SECS) {
                let mut patterns = self.patterns.write().await;
                for pattern in patterns.iter_mut() {
                    if pattern.detected_count > 0 {
                        debug!(
                            "Resetting cascading pattern counter for {}: {} detections",
                            pattern.initial_failure, pattern.detected_count
                        );
                    }
                    pattern.detected_count = 0;
                }
                drop(patterns);
                last_pattern_reset = Instant::now();
            }
        }
    }
}

impl Default for CascadingFailureDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about cascading failures
#[derive(Debug)]
pub struct CascadingFailureStats {
    pub total_events: usize,
    pub events_by_type: HashMap<FailureType, u32>,
    pub cascading_patterns_detected: u32,
    pub recent_window_secs: u64,
}

// Global cascading failure detector instance
use std::sync::LazyLock;
pub static GLOBAL_CASCADING_DETECTOR: LazyLock<CascadingFailureDetector> = LazyLock::new(CascadingFailureDetector::new);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cascading_detection() {
        let detector = CascadingFailureDetector::new();

        // Simulate cascading failure: peer unreachable → lock timeout → IAM timeout
        detector
            .record_failure(FailureType::PeerUnreachable, "node1".to_string())
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        detector
            .record_failure(FailureType::LockTimeout, "iam_lock".to_string())
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        detector
            .record_failure(FailureType::IAMOperationTimeout, "identity.json".to_string())
            .await;

        let stats = detector.get_statistics().await;
        assert!(stats.cascading_patterns_detected > 0, "Should detect cascading pattern");
    }

    #[tokio::test]
    async fn test_no_false_positive() {
        let detector = CascadingFailureDetector::new();

        // Simulate independent failures (not cascading)
        detector
            .record_failure(FailureType::PeerUnreachable, "node1".to_string())
            .await;
        tokio::time::sleep(Duration::from_secs(40)).await; // Outside time window
        detector.record_failure(FailureType::LockTimeout, "lock1".to_string()).await;

        let stats = detector.get_statistics().await;
        assert_eq!(
            stats.cascading_patterns_detected, 0,
            "Should not detect cascading for independent failures"
        );
    }
}
