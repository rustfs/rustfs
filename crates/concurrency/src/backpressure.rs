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

//! Backpressure management

use rustfs_io_core::{BackpressureMonitor as CoreBackpressureMonitor, BackpressureState};
use rustfs_io_metrics::backpressure_metrics;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{DuplexStream, duplex};

/// Backpressure configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Buffer size in bytes
    pub buffer_size: usize,
    /// High watermark percentage
    pub high_watermark: u32,
    /// Low watermark percentage
    pub low_watermark: u32,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            buffer_size: 4 * 1024 * 1024, // 4MB
            high_watermark: 80,
            low_watermark: 50,
        }
    }
}

impl BackpressureConfig {
    /// Calculate high watermark threshold in bytes
    pub fn high_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.high_watermark as u64 / 100) as usize
    }

    /// Calculate low watermark threshold in bytes
    pub fn low_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.low_watermark as u64 / 100) as usize
    }
}

/// Backpressure manager
pub struct BackpressureManager {
    config: BackpressureConfig,
    monitor: Arc<CoreBackpressureMonitor>,
}

impl BackpressureManager {
    /// Create a new backpressure manager
    pub fn new(buffer_size: usize, high_watermark: u32, low_watermark: u32) -> Self {
        let config = BackpressureConfig {
            buffer_size,
            high_watermark,
            low_watermark,
        };

        let core_config = rustfs_io_core::BackpressureConfig {
            max_concurrent: 32,
            high_water_mark: high_watermark as f64 / 100.0,
            low_water_mark: low_watermark as f64 / 100.0,
            cooldown: std::time::Duration::from_millis(100),
            enabled: true,
        };

        Self {
            config,
            monitor: Arc::new(CoreBackpressureMonitor::new(core_config)),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// Get the monitor
    pub fn monitor(&self) -> Arc<CoreBackpressureMonitor> {
        self.monitor.clone()
    }

    /// Create a backpressure pipe
    pub fn create_pipe(&self) -> BackpressurePipe {
        BackpressurePipe::new(self.config.clone(), self.monitor.clone())
    }

    /// Get current state
    pub fn state(&self) -> BackpressureState {
        self.monitor.state()
    }

    /// Check if backpressure is active
    pub fn is_active(&self) -> bool {
        self.monitor.is_active()
    }
}

/// Backpressure pipe wrapping tokio's duplex
pub struct BackpressurePipe {
    reader: DuplexStream,
    writer: DuplexStream,
    config: BackpressureConfig,
    monitor: Arc<CoreBackpressureMonitor>,
    created_at: Instant,
}

impl BackpressurePipe {
    fn new(config: BackpressureConfig, monitor: Arc<CoreBackpressureMonitor>) -> Self {
        let (reader, writer) = duplex(config.buffer_size);

        Self {
            reader,
            writer,
            config,
            monitor,
            created_at: Instant::now(),
        }
    }

    /// Get the reader end
    pub fn reader(&mut self) -> &mut DuplexStream {
        &mut self.reader
    }

    /// Get the writer end
    pub fn writer(&mut self) -> &mut DuplexStream {
        &mut self.writer
    }

    /// Split into reader and writer
    pub fn into_split(self) -> (DuplexStream, DuplexStream) {
        (self.reader, self.writer)
    }

    /// Get the configuration
    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// Get current state
    pub fn state(&self) -> BackpressureState {
        self.monitor.state()
    }

    /// Get the age of this pipe
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Check if should apply backpressure
    pub fn should_apply_backpressure(&self) -> bool {
        let should = self.monitor.should_apply_backpressure();
        if should {
            backpressure_metrics::record_backpressure_activation();
        }
        should
    }
}

/// Backpressure event
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BackpressureEvent {
    /// Event timestamp
    pub timestamp: Instant,
    /// Event type
    pub event_type: BackpressureEventType,
    /// Buffer usage
    pub buffer_usage: usize,
    /// Buffer capacity
    pub buffer_capacity: usize,
}

/// Backpressure event type
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum BackpressureEventType {
    /// High watermark reached
    HighWatermarkReached,
    /// High watermark exited
    HighWatermarkExited,
    /// Backpressure applied
    BackpressureApplied,
    /// Backpressure released
    BackpressureReleased,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_config() {
        let config = BackpressureConfig::default();
        assert_eq!(config.buffer_size, 4 * 1024 * 1024);
        assert!(config.high_watermark > config.low_watermark);
    }

    #[test]
    fn test_backpressure_manager() {
        let manager = BackpressureManager::new(1024, 80, 50);
        assert_eq!(manager.state(), BackpressureState::Normal);
    }

    #[test]
    fn test_backpressure_pipe() {
        let manager = BackpressureManager::new(1024, 80, 50);
        let pipe = manager.create_pipe();
        assert_eq!(pipe.state(), BackpressureState::Normal);
    }
}
