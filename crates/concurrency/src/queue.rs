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

//! Disk permit queue snapshot for GetObject orchestration.

/// Snapshot of disk permit queue usage for GetObject orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectQueueSnapshot {
    /// Total permits configured for disk reads.
    pub total_permits: usize,
    /// Permits currently in use.
    pub permits_in_use: usize,
}

impl GetObjectQueueSnapshot {
    /// Create a queue snapshot from total and available permits.
    pub fn from_available_permits(total_permits: usize, available_permits: usize) -> Self {
        Self {
            total_permits,
            permits_in_use: total_permits.saturating_sub(available_permits),
        }
    }

    /// Return currently available permits.
    pub fn permits_available(&self) -> usize {
        self.total_permits.saturating_sub(self.permits_in_use)
    }

    /// Return queue utilization percentage in the 0-100 range.
    pub fn utilization_percent(&self) -> f64 {
        if self.total_permits == 0 {
            0.0
        } else {
            (self.permits_in_use as f64 / self.total_permits as f64) * 100.0
        }
    }

    /// Return whether the queue is considered congested.
    pub fn is_congested(&self, threshold_percent: f64) -> bool {
        self.utilization_percent() > threshold_percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_snapshot() {
        let snapshot = GetObjectQueueSnapshot::from_available_permits(64, 16);
        assert_eq!(snapshot.permits_in_use, 48);
        assert_eq!(snapshot.permits_available(), 16);
        assert!(snapshot.is_congested(70.0));
    }

    #[test]
    fn test_queue_snapshot_clamps_over_available_permits() {
        let snapshot = GetObjectQueueSnapshot::from_available_permits(64, 96);
        assert_eq!(snapshot.permits_in_use, 0);
        assert_eq!(snapshot.permits_available(), 64);
        assert_eq!(snapshot.utilization_percent(), 0.0);
        assert!(!snapshot.is_congested(0.0));
    }

    #[test]
    fn test_queue_snapshot_handles_zero_total_permits() {
        let snapshot = GetObjectQueueSnapshot::from_available_permits(0, 0);
        assert_eq!(snapshot.permits_in_use, 0);
        assert_eq!(snapshot.permits_available(), 0);
        assert_eq!(snapshot.utilization_percent(), 0.0);
        assert!(!snapshot.is_congested(0.0));
    }
}
