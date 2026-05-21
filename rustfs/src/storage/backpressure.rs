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

//! Backpressure Management for Object Data Transfer.
//!
//! This module provides backpressure-aware pipes for object data transfer,
//! preventing buffer overflow and memory exhaustion under high concurrency.

//! # Key Features
//!
//! - Configurable buffer size with high/low watermarks
//! - Backpressure state monitoring and events
//! - Backpressure metrics emitted through the shared metrics pipeline
//! - Graceful handling of slow consumers
//!
//! # Architecture
//!
//! ```text
//! [Disk Reader] --> [BackpressurePipe] --> [HTTP Response]
//!                      |
//!                      v
//!               [Buffer Monitor]
//!                      |
//!                      v
//!              [High Watermark?] --> Apply Backpressure
//! ```

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{DuplexStream, duplex};
use tracing::{debug, warn};

use metrics::counter;

/// Object-transfer duplex pipe backpressure policy.
#[derive(Debug, Clone, Copy)]
pub struct ObjectPipeBackpressurePolicy {
    /// Buffer size in bytes (default 4MB).
    pub buffer_size: usize,
    /// High watermark percentage (default 80%).
    /// When buffer usage exceeds this, backpressure is applied.
    pub high_watermark: u32,
    /// Low watermark percentage (default 50%).
    /// When buffer usage drops below this after high watermark, backpressure is released.
    pub low_watermark: u32,
}

impl Default for ObjectPipeBackpressurePolicy {
    fn default() -> Self {
        Self {
            buffer_size: rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
            high_watermark: rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_HIGH_WATERMARK,
            low_watermark: rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_LOW_WATERMARK,
        }
    }
}

impl ObjectPipeBackpressurePolicy {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let buffer_size = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_DUPLEX_BUFFER_SIZE,
            rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
        );
        let high_watermark = rustfs_utils::get_env_u32(
            rustfs_config::ENV_OBJECT_BACKPRESSURE_HIGH_WATERMARK,
            rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_HIGH_WATERMARK,
        );
        let low_watermark = rustfs_utils::get_env_u32(
            rustfs_config::ENV_OBJECT_BACKPRESSURE_LOW_WATERMARK,
            rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_LOW_WATERMARK,
        );

        Self {
            buffer_size,
            high_watermark,
            low_watermark,
        }
    }

    /// Calculate high watermark threshold in bytes.
    pub fn high_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.high_watermark as u64 / 100) as usize
    }

    /// Calculate low watermark threshold in bytes.
    pub fn low_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.low_watermark as u64 / 100) as usize
    }
}

/// Backpressure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureState {
    /// Normal operation, buffer usage is below high watermark.
    Normal,
    /// Buffer usage is above high watermark, backpressure should be applied.
    HighWatermark,
    /// Backpressure is actively being applied to the producer.
    BackpressureApplied,
}

impl std::fmt::Display for BackpressureState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackpressureState::Normal => write!(f, "normal"),
            BackpressureState::HighWatermark => write!(f, "high_watermark"),
            BackpressureState::BackpressureApplied => write!(f, "backpressure_applied"),
        }
    }
}

/// Compact metadata snapshot for object-transfer backpressure pipes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackpressurePipeMeta {
    /// Buffer capacity in bytes.
    pub buffer_capacity: usize,
    /// Current backpressure state.
    pub state: BackpressureState,
    /// Age of the pipe since creation.
    pub age: Duration,
}

/// Compact metadata snapshot for the lightweight backpressure monitor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackpressureMonitorMeta {
    /// Buffer capacity in bytes.
    pub buffer_capacity: usize,
    /// Current buffer usage percentage.
    pub usage_percent: f32,
    /// Current backpressure state.
    pub state: BackpressureState,
}

fn calculate_usage_percent(usage: usize, capacity: usize) -> f32 {
    if capacity > 0 {
        (usage as f32 / capacity as f32) * 100.0
    } else {
        0.0
    }
}

fn apply_watermark_transition(
    in_high_watermark: &AtomicBool,
    usage: usize,
    high: usize,
    low: usize,
) -> (BackpressureState, bool) {
    let current = in_high_watermark.load(Ordering::Acquire);
    let next_state = if usage >= high {
        BackpressureState::HighWatermark
    } else if usage <= low {
        BackpressureState::Normal
    } else if current {
        BackpressureState::HighWatermark
    } else {
        BackpressureState::Normal
    };
    let next_is_high = matches!(next_state, BackpressureState::HighWatermark);
    let changed = in_high_watermark.swap(next_is_high, Ordering::AcqRel) != next_is_high;
    (next_state, changed)
}

fn saturating_sub_atomic(value: &AtomicUsize, delta: usize) {
    value
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_sub(delta)))
        .ok();
}

/// A backpressure-aware pipe wrapping tokio's duplex.
///
/// This provides monitoring and events for backpressure conditions
/// while maintaining compatibility with the standard duplex interface.
pub struct BackpressurePipe {
    /// Reader end of the duplex pipe.
    reader: DuplexStream,
    /// Writer end of the duplex pipe.
    writer: DuplexStream,
    /// Configuration.
    config: ObjectPipeBackpressurePolicy,
    /// Current buffer usage (approximate, updated on write).
    buffer_usage: AtomicUsize,
    /// Current backpressure state.
    state: AtomicBool, // true = in high watermark state
    /// Total bytes written.
    total_written: AtomicUsize,
    /// Total bytes read.
    total_read: AtomicUsize,
    /// Cached high watermark threshold in bytes.
    high_watermark_bytes: usize,
    /// Cached low watermark threshold in bytes.
    low_watermark_bytes: usize,
    /// Pipe creation timestamp.
    created_at: Instant,
}

impl BackpressurePipe {
    /// Create a new backpressure-aware pipe with default configuration.
    pub fn new() -> Self {
        Self::with_config(ObjectPipeBackpressurePolicy::from_env())
    }

    /// Create a new backpressure-aware pipe with custom configuration.
    pub fn with_config(config: ObjectPipeBackpressurePolicy) -> Self {
        let (reader, writer) = duplex(config.buffer_size);
        let high_watermark_bytes = config.high_watermark_bytes();
        let low_watermark_bytes = config.low_watermark_bytes();

        debug!(
            buffer_size = config.buffer_size,
            high_watermark = config.high_watermark,
            low_watermark = config.low_watermark,
            high_watermark_bytes,
            low_watermark_bytes,
            "Created backpressure pipe"
        );

        Self {
            reader,
            writer,
            config,
            buffer_usage: AtomicUsize::new(0),
            state: AtomicBool::new(false),
            total_written: AtomicUsize::new(0),
            total_read: AtomicUsize::new(0),
            high_watermark_bytes,
            low_watermark_bytes,
            created_at: Instant::now(),
        }
    }

    /// Take the reader end of the pipe (consumes self).
    pub fn into_reader(self) -> DuplexStream {
        self.reader
    }

    /// Take the writer end of the pipe (consumes self).
    pub fn into_writer(self) -> DuplexStream {
        self.writer
    }

    /// Split into reader and writer (consumes self).
    pub fn split(self) -> (DuplexStream, DuplexStream) {
        (self.reader, self.writer)
    }

    /// Get current backpressure state.
    pub fn state(&self) -> BackpressureState {
        if self.state.load(Ordering::Acquire) {
            BackpressureState::BackpressureApplied
        } else {
            BackpressureState::Normal
        }
    }

    /// Get a compact metadata snapshot for the pipe.
    pub fn meta(&self) -> BackpressurePipeMeta {
        BackpressurePipeMeta {
            buffer_capacity: self.config.buffer_size,
            state: self.state(),
            age: self.age(),
        }
    }

    /// Get the age of this pipe.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get current buffer usage.
    pub fn usage(&self) -> usize {
        self.buffer_usage.load(Ordering::Acquire)
    }

    /// Record bytes written (call after successful write).
    pub fn record_write(&self, bytes: usize) {
        self.total_written.fetch_add(bytes, Ordering::Relaxed);
        self.buffer_usage.fetch_add(bytes, Ordering::Release);
        self.update_watermark_state();
    }

    /// Record bytes read (call after successful read).
    pub fn record_read(&self, bytes: usize) {
        self.total_read.fetch_add(bytes, Ordering::Relaxed);
        saturating_sub_atomic(&self.buffer_usage, bytes);
        self.update_watermark_state();
    }

    /// Update watermark state and emit transition signals.
    fn update_watermark_state(&self) {
        let usage = self.buffer_usage.load(Ordering::Acquire);
        let usage_percent = calculate_usage_percent(usage, self.config.buffer_size) as u32;
        let (next_state, changed) =
            apply_watermark_transition(&self.state, usage, self.high_watermark_bytes, self.low_watermark_bytes);

        if changed {
            match next_state {
                BackpressureState::HighWatermark => {
                    counter!("rustfs_backpressure_events_total", "state" => "high_watermark").increment(1);

                    warn!(
                        buffer_usage = usage,
                        buffer_capacity = self.config.buffer_size,
                        usage_percent,
                        high_watermark = self.config.high_watermark,
                        "Backpressure: high watermark reached"
                    );
                }
                BackpressureState::Normal => {
                    counter!("rustfs_backpressure_events_total", "state" => "normal").increment(1);

                    debug!(
                        buffer_usage = usage,
                        buffer_capacity = self.config.buffer_size,
                        usage_percent,
                        low_watermark = self.config.low_watermark,
                        "Backpressure: returned to normal"
                    );
                }
                BackpressureState::BackpressureApplied => {}
            }
        }
    }

    /// Get total bytes written.
    pub fn total_written(&self) -> usize {
        self.total_written.load(Ordering::Relaxed)
    }

    /// Get total bytes read.
    pub fn total_read(&self) -> usize {
        self.total_read.load(Ordering::Relaxed)
    }

    /// Get buffer capacity.
    pub fn capacity(&self) -> usize {
        self.config.buffer_size
    }
}

impl Default for BackpressurePipe {
    fn default() -> Self {
        Self::new()
    }
}

/// A simple wrapper that provides backpressure monitoring for duplex streams.
///
/// This is a lighter-weight alternative to `BackpressurePipe` that doesn't
/// wrap the streams but provides monitoring capabilities.
pub struct BackpressureMonitor {
    /// Configuration.
    config: ObjectPipeBackpressurePolicy,
    /// Current buffer usage.
    buffer_usage: AtomicUsize,
    /// In high watermark state.
    in_high_watermark: AtomicBool,
    /// Cached high watermark threshold in bytes.
    high_watermark_bytes: usize,
    /// Cached low watermark threshold in bytes.
    low_watermark_bytes: usize,
}

impl BackpressureMonitor {
    /// Create a new monitor with default configuration.
    pub fn new() -> Self {
        Self::with_config(ObjectPipeBackpressurePolicy::from_env())
    }

    /// Create a new monitor with custom configuration.
    pub fn with_config(config: ObjectPipeBackpressurePolicy) -> Self {
        let high_watermark_bytes = config.high_watermark_bytes();
        let low_watermark_bytes = config.low_watermark_bytes();
        Self {
            config,
            buffer_usage: AtomicUsize::new(0),
            in_high_watermark: AtomicBool::new(false),
            high_watermark_bytes,
            low_watermark_bytes,
        }
    }

    /// Record bytes added to buffer.
    pub fn on_write(&self, bytes: usize) -> BackpressureState {
        self.buffer_usage.fetch_add(bytes, Ordering::Release);
        self.update_state()
    }

    /// Record bytes removed from buffer.
    pub fn on_read(&self, bytes: usize) -> BackpressureState {
        saturating_sub_atomic(&self.buffer_usage, bytes);
        self.update_state()
    }

    /// Get current state.
    pub fn state(&self) -> BackpressureState {
        if self.in_high_watermark.load(Ordering::Acquire) {
            BackpressureState::HighWatermark
        } else {
            BackpressureState::Normal
        }
    }

    /// Get current buffer usage.
    pub fn usage(&self) -> usize {
        self.buffer_usage.load(Ordering::Acquire)
    }

    /// Get usage percentage.
    pub fn usage_percent(&self) -> f32 {
        let usage = self.buffer_usage.load(Ordering::Acquire);
        calculate_usage_percent(usage, self.config.buffer_size)
    }

    /// Get a compact metadata snapshot for the monitor.
    pub fn meta(&self) -> BackpressureMonitorMeta {
        let usage = self.buffer_usage.load(Ordering::Acquire);
        BackpressureMonitorMeta {
            buffer_capacity: self.config.buffer_size,
            usage_percent: calculate_usage_percent(usage, self.config.buffer_size),
            state: self.state(),
        }
    }

    /// Update state based on current usage.
    fn update_state(&self) -> BackpressureState {
        let usage = self.buffer_usage.load(Ordering::Acquire);
        let usage_percent = calculate_usage_percent(usage, self.config.buffer_size) as u32;
        let (next_state, changed) =
            apply_watermark_transition(&self.in_high_watermark, usage, self.high_watermark_bytes, self.low_watermark_bytes);

        if matches!(next_state, BackpressureState::HighWatermark) {
            if changed {
                counter!("rustfs_backpressure_events_total", "state" => "high_watermark").increment(1);

                debug!(usage_percent, "Backpressure: entered high watermark");
            }
            BackpressureState::HighWatermark
        } else {
            if changed {
                counter!("rustfs_backpressure_events_total", "state" => "normal").increment(1);

                debug!(usage_percent, "Backpressure: returned to normal");
            }
            BackpressureState::Normal
        }
    }
}

impl Default for BackpressureMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_config_default() {
        let config = ObjectPipeBackpressurePolicy::default();
        assert_eq!(config.buffer_size, 4 * 1024 * 1024);
        assert_eq!(config.high_watermark, 80);
        assert_eq!(config.low_watermark, 50);
    }

    #[test]
    fn test_backpressure_config_watermarks() {
        let config = ObjectPipeBackpressurePolicy {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        assert_eq!(config.high_watermark_bytes(), 800);
        assert_eq!(config.low_watermark_bytes(), 500);
    }

    #[test]
    fn test_backpressure_state_display() {
        assert_eq!(format!("{}", BackpressureState::Normal), "normal");
        assert_eq!(format!("{}", BackpressureState::HighWatermark), "high_watermark");
        assert_eq!(format!("{}", BackpressureState::BackpressureApplied), "backpressure_applied");
    }

    #[test]
    fn test_backpressure_monitor() {
        let config = ObjectPipeBackpressurePolicy {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        let monitor = BackpressureMonitor::with_config(config);

        // Initially normal
        assert_eq!(monitor.state(), BackpressureState::Normal);
        assert_eq!(monitor.meta().buffer_capacity, 1000);
        assert_eq!(monitor.meta().usage_percent, 0.0);

        // Write to reach high watermark
        let state = monitor.on_write(850);
        assert_eq!(state, BackpressureState::HighWatermark);
        assert_eq!(monitor.meta().usage_percent, 85.0);

        // Read to go below low watermark
        let state = monitor.on_read(400);
        assert_eq!(state, BackpressureState::Normal);
        assert_eq!(monitor.meta().usage_percent, 45.0);
    }

    #[tokio::test]
    async fn test_backpressure_pipe_creation() {
        let pipe = BackpressurePipe::new();
        assert_eq!(pipe.capacity(), 4 * 1024 * 1024);
        assert_eq!(pipe.state(), BackpressureState::Normal);
        assert_eq!(pipe.meta().buffer_capacity, 4 * 1024 * 1024);
        assert!(pipe.meta().age <= pipe.age());
    }

    #[test]
    fn test_backpressure_pipe_state_transitions() {
        let config = ObjectPipeBackpressurePolicy {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        let pipe = BackpressurePipe::with_config(config);

        assert_eq!(pipe.state(), BackpressureState::Normal);
        assert_eq!(pipe.meta().state, BackpressureState::Normal);

        pipe.record_write(850);
        assert_eq!(pipe.state(), BackpressureState::BackpressureApplied);
        assert_eq!(pipe.meta().state, BackpressureState::BackpressureApplied);

        pipe.record_read(400);
        assert_eq!(pipe.state(), BackpressureState::Normal);
        assert_eq!(pipe.meta().state, BackpressureState::Normal);
    }
}
