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

//! Recent bandwidth observation for adaptive scheduling.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandwidthTier {
    Low,
    Medium,
    High,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandwidthSnapshot {
    pub bytes_per_second: u64,
    pub tier: BandwidthTier,
}

#[derive(Debug, Clone)]
pub struct BandwidthMonitor {
    ema_beta: f64,
    low_threshold_bps: u64,
    high_threshold_bps: u64,
    current_bps: Option<f64>,
}

impl BandwidthMonitor {
    pub fn new(ema_beta: f64, low_threshold_bps: u64, high_threshold_bps: u64) -> Self {
        Self {
            ema_beta: ema_beta.clamp(0.0, 1.0),
            low_threshold_bps,
            high_threshold_bps,
            current_bps: None,
        }
    }

    pub fn record_transfer(&mut self, bytes: u64, duration: Duration) {
        if bytes == 0 || duration.is_zero() {
            return;
        }

        let sample_bps = bytes as f64 / duration.as_secs_f64();
        self.current_bps = Some(match self.current_bps {
            Some(current) => (self.ema_beta * sample_bps) + ((1.0 - self.ema_beta) * current),
            None => sample_bps,
        });
    }

    pub fn current_bytes_per_second(&self) -> Option<u64> {
        self.current_bps.map(|value| value.max(0.0) as u64)
    }

    pub fn snapshot(&self) -> BandwidthSnapshot {
        let bytes_per_second = self.current_bytes_per_second().unwrap_or(0);
        BandwidthSnapshot {
            tier: self.tier_for(bytes_per_second),
            bytes_per_second,
        }
    }

    pub fn tier_for(&self, bytes_per_second: u64) -> BandwidthTier {
        if bytes_per_second == 0 {
            BandwidthTier::Unknown
        } else if bytes_per_second < self.low_threshold_bps {
            BandwidthTier::Low
        } else if bytes_per_second >= self.high_threshold_bps {
            BandwidthTier::High
        } else {
            BandwidthTier::Medium
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bandwidth_monitor_records_samples() {
        let mut monitor = BandwidthMonitor::new(0.5, 100, 1000);
        monitor.record_transfer(1000, Duration::from_secs(1));
        assert_eq!(monitor.current_bytes_per_second(), Some(1000));

        monitor.record_transfer(200, Duration::from_secs(1));
        assert_eq!(monitor.current_bytes_per_second(), Some(600));
        assert_eq!(monitor.snapshot().tier, BandwidthTier::Medium);
    }
}
