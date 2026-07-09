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

//! Shared backpressure policy type.
//!
//! The runtime backpressure implementation (byte-watermark pipes and
//! monitors) lives in `rustfs/src/storage/backpressure.rs`; this module only
//! carries the watermark policy type that implementation shares.

use rustfs_io_core::BackpressureConfig as CoreBackpressureConfig;

/// Watermark policy for duplex-pipe backpressure.
#[derive(Debug, Clone, Copy)]
pub struct PipeBackpressurePolicy {
    /// Buffer size in bytes
    pub buffer_size: usize,
    /// High watermark percentage
    pub high_watermark: u32,
    /// Low watermark percentage
    pub low_watermark: u32,
}

impl Default for PipeBackpressurePolicy {
    fn default() -> Self {
        Self {
            buffer_size: 4 * 1024 * 1024, // 4MB
            high_watermark: 80,
            low_watermark: 50,
        }
    }
}

impl PipeBackpressurePolicy {
    /// Calculate high watermark threshold in bytes
    pub fn high_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.high_watermark as u64 / 100) as usize
    }

    /// Calculate low watermark threshold in bytes
    pub fn low_watermark_bytes(&self) -> usize {
        (self.buffer_size as u64 * self.low_watermark as u64 / 100) as usize
    }

    /// Convert the policy into the reusable io-core admission-pressure config.
    ///
    /// The caller still owns duplex buffer sizing, but the shared
    /// overload/admission primitive lives in `io-core`.
    pub fn to_core_config(&self) -> CoreBackpressureConfig {
        CoreBackpressureConfig {
            max_concurrent: 32,
            high_water_mark: self.high_watermark as f64 / 100.0,
            low_water_mark: self.low_watermark as f64 / 100.0,
            cooldown: std::time::Duration::from_millis(100),
            enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_policy_defaults() {
        let config = PipeBackpressurePolicy::default();
        assert_eq!(config.buffer_size, 4 * 1024 * 1024);
        assert!(config.high_watermark > config.low_watermark);
    }

    #[test]
    fn test_backpressure_policy_watermark_bytes() {
        let config = PipeBackpressurePolicy {
            buffer_size: 1000,
            high_watermark: 80,
            low_watermark: 50,
        };
        assert_eq!(config.high_watermark_bytes(), 800);
        assert_eq!(config.low_watermark_bytes(), 500);
    }

    #[test]
    fn test_backpressure_policy_to_core_config() {
        let policy = PipeBackpressurePolicy::default();
        let core = policy.to_core_config();
        assert_eq!(core.high_water_mark, policy.high_watermark as f64 / 100.0);
        assert_eq!(core.low_water_mark, policy.low_watermark as f64 / 100.0);
        assert!(core.enabled);
    }
}
