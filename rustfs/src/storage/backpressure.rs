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

// Allow dead_code for public API that may be used by external modules or future features
#![allow(dead_code)]
//!
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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{DuplexStream, duplex};
use tracing::{debug, warn};

use metrics::counter;

/// Backpressure pipe configuration.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Buffer size in bytes (default 4MB).
    pub buffer_size: usize,
    /// High watermark percentage (default 80%).
    /// When buffer usage exceeds this, backpressure is applied.
    pub high_watermark: u32,
    /// Low watermark percentage (default 50%).
    /// When buffer usage drops below this after high watermark, backpressure is released.
    pub low_watermark: u32,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            buffer_size: rustfs_config::DEFAULT_OBJECT_DUPLEX_BUFFER_SIZE,
            high_watermark: rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_HIGH_WATERMARK,
            low_watermark: rustfs_config::DEFAULT_OBJECT_BACKPRESSURE_LOW_WATERMARK,
        }
    }
}

impl BackpressureConfig {
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

/// Backpressure event for monitoring.
#[derive(Debug, Clone)]
pub struct BackpressureEvent {
    /// Event timestamp.
    pub timestamp: Instant,
    /// Event type.
    pub event_type: BackpressureEventType,
    /// Buffer usage at event time.
    pub buffer_usage: usize,
    /// Buffer capacity.
    pub buffer_capacity: usize,
    /// Usage percentage.
    pub usage_percent: f32,
}

/// Backpressure event type.
#[derive(Debug, Clone, Copy)]
pub enum BackpressureEventType {
    /// Entered high watermark state.
    HighWatermarkReached,
    /// Exited high watermark state (backpressure released).
    HighWatermarkExited,
    /// Backpressure applied to producer.
    BackpressureApplied,
    /// Backpressure released.
    BackpressureReleased,
}

/// Snapshot of backpressure state.
#[derive(Debug, Clone)]
pub struct BackpressureSnapshot {
    /// Buffer capacity in bytes.
    pub buffer_capacity: usize,
    /// Current buffer usage in bytes (approximate).
    pub buffer_used: usize,
    /// Usage percentage.
    pub usage_percent: f32,
    /// Current state.
    pub state: BackpressureState,
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
    config: BackpressureConfig,
    /// Current buffer usage (approximate, updated on write).
    buffer_usage: Arc<AtomicUsize>,
    /// Current backpressure state.
    state: Arc<AtomicBool>, // true = in high watermark state
    /// Total bytes written.
    total_written: Arc<AtomicUsize>,
    /// Total bytes read.
    total_read: Arc<AtomicUsize>,
}

impl BackpressurePipe {
    /// Create a new backpressure-aware pipe with default configuration.
    pub fn new() -> Self {
        Self::with_config(BackpressureConfig::from_env())
    }

    /// Create a new backpressure-aware pipe with custom configuration.
    pub fn with_config(config: BackpressureConfig) -> Self {
        let (reader, writer) = duplex(config.buffer_size);

        debug!(
            buffer_size = config.buffer_size,
            high_watermark = config.high_watermark,
            low_watermark = config.low_watermark,
            high_watermark_bytes = config.high_watermark_bytes(),
            low_watermark_bytes = config.low_watermark_bytes(),
            "Created backpressure pipe"
        );

        Self {
            reader,
            writer,
            config,
            buffer_usage: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(AtomicBool::new(false)),
            total_written: Arc::new(AtomicUsize::new(0)),
            total_read: Arc::new(AtomicUsize::new(0)),
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
        if self.state.load(Ordering::Relaxed) {
            BackpressureState::BackpressureApplied
        } else {
            BackpressureState::Normal
        }
    }

    /// Get current buffer usage snapshot.
    pub fn snapshot(&self) -> BackpressureSnapshot {
        let buffer_used = self.buffer_usage.load(Ordering::Relaxed);
        let usage_percent = if self.config.buffer_size > 0 {
            (buffer_used as f64 / self.config.buffer_size as f64) * 100.0
        } else {
            0.0
        };

        BackpressureSnapshot {
            buffer_capacity: self.config.buffer_size,
            buffer_used,
            usage_percent: usage_percent as f32,
            state: self.state(),
        }
    }

    /// Record bytes written (call after successful write).
    pub fn record_write(&self, bytes: usize) {
        self.total_written.fetch_add(bytes, Ordering::Relaxed);
        self.buffer_usage.fetch_add(bytes, Ordering::Relaxed);
        self.check_high_watermark();
    }

    /// Record bytes read (call after successful read).
    pub fn record_read(&self, bytes: usize) {
        self.total_read.fetch_add(bytes, Ordering::Relaxed);
        self.buffer_usage.fetch_sub(bytes, Ordering::Relaxed);
        self.check_low_watermark();
    }

    /// Check if high watermark is reached.
    fn check_high_watermark(&self) {
        let usage = self.buffer_usage.load(Ordering::Relaxed);
        let threshold = self.config.high_watermark_bytes();

        if usage >= threshold && !self.state.load(Ordering::Relaxed) {
            self.state.store(true, Ordering::Relaxed);

            counter!("rustfs.backpressure.events.total", "state" => "high_watermark").increment(1);

            warn!(
                buffer_usage = usage,
                buffer_capacity = self.config.buffer_size,
                usage_percent = (usage as f64 / self.config.buffer_size as f64 * 100.0) as u32,
                high_watermark = self.config.high_watermark,
                "Backpressure: high watermark reached"
            );
        }
    }

    /// Check if low watermark is reached (backpressure can be released).
    fn check_low_watermark(&self) {
        let usage = self.buffer_usage.load(Ordering::Relaxed);
        let threshold = self.config.low_watermark_bytes();

        if usage <= threshold && self.state.load(Ordering::Relaxed) {
            self.state.store(false, Ordering::Relaxed);

            counter!("rustfs.backpressure.events.total", "state" => "normal").increment(1);

            debug!(
                buffer_usage = usage,
                buffer_capacity = self.config.buffer_size,
                usage_percent = (usage as f64 / self.config.buffer_size as f64 * 100.0) as u32,
                low_watermark = self.config.low_watermark,
                "Backpressure: returned to normal"
            );
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
    config: BackpressureConfig,
    /// Current buffer usage.
    buffer_usage: Arc<AtomicUsize>,
    /// In high watermark state.
    in_high_watermark: Arc<AtomicBool>,
}

impl BackpressureMonitor {
    /// Create a new monitor with default configuration.
    pub fn new() -> Self {
        Self::with_config(BackpressureConfig::from_env())
    }

    /// Create a new monitor with custom configuration.
    pub fn with_config(config: BackpressureConfig) -> Self {
        Self {
            config,
            buffer_usage: Arc::new(AtomicUsize::new(0)),
            in_high_watermark: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Record bytes added to buffer.
    pub fn on_write(&self, bytes: usize) -> BackpressureState {
        self.buffer_usage.fetch_add(bytes, Ordering::Relaxed);
        self.update_state()
    }

    /// Record bytes removed from buffer.
    pub fn on_read(&self, bytes: usize) -> BackpressureState {
        self.buffer_usage.fetch_sub(bytes, Ordering::Relaxed);
        self.update_state()
    }

    /// Get current state.
    pub fn state(&self) -> BackpressureState {
        if self.in_high_watermark.load(Ordering::Relaxed) {
            BackpressureState::HighWatermark
        } else {
            BackpressureState::Normal
        }
    }

    /// Get current buffer usage.
    pub fn usage(&self) -> usize {
        self.buffer_usage.load(Ordering::Relaxed)
    }

    /// Get usage percentage.
    pub fn usage_percent(&self) -> f32 {
        let usage = self.buffer_usage.load(Ordering::Relaxed);
        if self.config.buffer_size > 0 {
            (usage as f32 / self.config.buffer_size as f32) * 100.0
        } else {
            0.0
        }
    }

    /// Update state based on current usage.
    fn update_state(&self) -> BackpressureState {
        let usage = self.buffer_usage.load(Ordering::Relaxed);
        let high = self.config.high_watermark_bytes();
        let low = self.config.low_watermark_bytes();

        if usage >= high {
            if !self.in_high_watermark.swap(true, Ordering::Relaxed) {
                counter!("rustfs.backpressure.events.total", "state" => "high_watermark").increment(1);

                debug!(usage_percent = self.usage_percent() as u32, "Backpressure: entered high watermark");
            }
            BackpressureState::HighWatermark
        } else if usage <= low {
            if self.in_high_watermark.swap(false, Ordering::Relaxed) {
                counter!("rustfs.backpressure.events.total", "state" => "normal").increment(1);

                debug!(usage_percent = self.usage_percent() as u32, "Backpressure: returned to normal");
            }
            BackpressureState::Normal
        } else {
            self.state()
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
        let config = BackpressureConfig::default();
        assert_eq!(config.buffer_size, 4 * 1024 * 1024);
        assert_eq!(config.high_watermark, 80);
        assert_eq!(config.low_watermark, 50);
    }

    #[test]
    fn test_backpressure_config_watermarks() {
        let config = BackpressureConfig {
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
        let config = BackpressureConfig {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        let monitor = BackpressureMonitor::with_config(config);

        // Initially normal
        assert_eq!(monitor.state(), BackpressureState::Normal);

        // Write to reach high watermark
        let state = monitor.on_write(850);
        assert_eq!(state, BackpressureState::HighWatermark);

        // Read to go below low watermark
        let state = monitor.on_read(400);
        assert_eq!(state, BackpressureState::Normal);
    }

    #[tokio::test]
    async fn test_backpressure_pipe_creation() {
        let pipe = BackpressurePipe::new();
        assert_eq!(pipe.capacity(), 4 * 1024 * 1024);
        assert_eq!(pipe.state(), BackpressureState::Normal);
    }
}
