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

//! Capacity calculation configuration constants

// ============================================================================
// Environment Variable Names
// ============================================================================

/// Environment variable for scheduled update interval
pub const ENV_CAPACITY_SCHEDULED_INTERVAL: &str = "RUSTFS_CAPACITY_SCHEDULED_INTERVAL";

/// Environment variable for write trigger delay
pub const ENV_CAPACITY_WRITE_TRIGGER_DELAY: &str = "RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY";

/// Environment variable for write frequency threshold
pub const ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD: &str = "RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD";

/// Environment variable for fast update threshold
pub const ENV_CAPACITY_FAST_UPDATE_THRESHOLD: &str = "RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD";

/// Environment variable for max files threshold
pub const ENV_CAPACITY_MAX_FILES_THRESHOLD: &str = "RUSTFS_CAPACITY_MAX_FILES_THRESHOLD";

/// Environment variable for statistics timeout
pub const ENV_CAPACITY_STAT_TIMEOUT: &str = "RUSTFS_CAPACITY_STAT_TIMEOUT";

/// Environment variable for sample rate
pub const ENV_CAPACITY_SAMPLE_RATE: &str = "RUSTFS_CAPACITY_SAMPLE_RATE";

/// Environment variable for following symbolic links during capacity calculation
pub const ENV_CAPACITY_FOLLOW_SYMLINKS: &str = "RUSTFS_CAPACITY_FOLLOW_SYMLINKS";

/// Environment variable for maximum symlink follow depth
pub const ENV_CAPACITY_MAX_SYMLINK_DEPTH: &str = "RUSTFS_CAPACITY_MAX_SYMLINK_DEPTH";

/// Environment variable for enabling dynamic timeout calculation
pub const ENV_CAPACITY_ENABLE_DYNAMIC_TIMEOUT: &str = "RUSTFS_CAPACITY_ENABLE_DYNAMIC_TIMEOUT";

/// Environment variable for minimum capacity calculation timeout
pub const ENV_CAPACITY_MIN_TIMEOUT: &str = "RUSTFS_CAPACITY_MIN_TIMEOUT";

/// Environment variable for maximum capacity calculation timeout
pub const ENV_CAPACITY_MAX_TIMEOUT: &str = "RUSTFS_CAPACITY_MAX_TIMEOUT";

/// Environment variable for progress stall detection timeout
pub const ENV_CAPACITY_STALL_TIMEOUT: &str = "RUSTFS_CAPACITY_STALL_TIMEOUT";

// ============================================================================
// Default Values
// ============================================================================

/// Scheduled update interval in seconds
/// Default: 300 seconds (5 minutes)
pub const DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS: u64 = 300;

/// Write trigger delay in seconds
/// Default: 10 seconds
pub const DEFAULT_WRITE_TRIGGER_DELAY_SECS: u64 = 10;

/// Write frequency threshold (writes per minute)
/// Default: 10 writes/minute
pub const DEFAULT_WRITE_FREQUENCY_THRESHOLD: usize = 10;

/// Fast update threshold in seconds
/// Default: 60 seconds
pub const DEFAULT_FAST_UPDATE_THRESHOLD_SECS: u64 = 60;

/// Maximum files threshold for sampling
/// Default: 1,000,000 files
pub const DEFAULT_MAX_FILES_THRESHOLD: usize = 1_000_000;

/// Statistics timeout in seconds
/// Default: 5 seconds
pub const DEFAULT_STAT_TIMEOUT_SECS: u64 = 5;

/// Sampling rate (1 in every N files)
/// Default: 100
pub const DEFAULT_SAMPLE_RATE: usize = 100;

/// Follow symbolic links during capacity calculation
/// Default: false (disabled for safety)
pub const DEFAULT_CAPACITY_FOLLOW_SYMLINKS: bool = false;

/// Maximum symlink follow depth
/// Default: 3 levels
pub const DEFAULT_CAPACITY_MAX_SYMLINK_DEPTH: u8 = 3;

/// Enable dynamic timeout calculation based on directory characteristics
/// Default: true (enabled)
pub const DEFAULT_CAPACITY_ENABLE_DYNAMIC_TIMEOUT: bool = true;

/// Minimum capacity calculation timeout in seconds
/// Default: 5 seconds
pub const DEFAULT_CAPACITY_MIN_TIMEOUT_SECS: u64 = 5;

/// Maximum capacity calculation timeout in seconds
/// Default: 60 seconds
pub const DEFAULT_CAPACITY_MAX_TIMEOUT_SECS: u64 = 60;

/// Progress stall detection timeout in seconds
/// Default: 1 second (no progress for 1 second = stall)
pub const DEFAULT_CAPACITY_STALL_TIMEOUT_SECS: u64 = 1;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_names() {
        assert_eq!(ENV_CAPACITY_SCHEDULED_INTERVAL, "RUSTFS_CAPACITY_SCHEDULED_INTERVAL");
        assert_eq!(ENV_CAPACITY_WRITE_TRIGGER_DELAY, "RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY");
        assert_eq!(ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, "RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD");
        assert_eq!(ENV_CAPACITY_FAST_UPDATE_THRESHOLD, "RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD");
        assert_eq!(ENV_CAPACITY_MAX_FILES_THRESHOLD, "RUSTFS_CAPACITY_MAX_FILES_THRESHOLD");
        assert_eq!(ENV_CAPACITY_STAT_TIMEOUT, "RUSTFS_CAPACITY_STAT_TIMEOUT");
        assert_eq!(ENV_CAPACITY_SAMPLE_RATE, "RUSTFS_CAPACITY_SAMPLE_RATE");
        assert_eq!(ENV_CAPACITY_FOLLOW_SYMLINKS, "RUSTFS_CAPACITY_FOLLOW_SYMLINKS");
        assert_eq!(ENV_CAPACITY_MAX_SYMLINK_DEPTH, "RUSTFS_CAPACITY_MAX_SYMLINK_DEPTH");
        assert_eq!(ENV_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, "RUSTFS_CAPACITY_ENABLE_DYNAMIC_TIMEOUT");
        assert_eq!(ENV_CAPACITY_MIN_TIMEOUT, "RUSTFS_CAPACITY_MIN_TIMEOUT");
        assert_eq!(ENV_CAPACITY_MAX_TIMEOUT, "RUSTFS_CAPACITY_MAX_TIMEOUT");
        assert_eq!(ENV_CAPACITY_STALL_TIMEOUT, "RUSTFS_CAPACITY_STALL_TIMEOUT");
    }

    #[test]
    fn test_default_values() {
        assert_eq!(DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS, 300);
        assert_eq!(DEFAULT_WRITE_TRIGGER_DELAY_SECS, 10);
        assert_eq!(DEFAULT_WRITE_FREQUENCY_THRESHOLD, 10);
        assert_eq!(DEFAULT_FAST_UPDATE_THRESHOLD_SECS, 60);
        assert_eq!(DEFAULT_MAX_FILES_THRESHOLD, 1_000_000);
        assert_eq!(DEFAULT_STAT_TIMEOUT_SECS, 5);
        assert_eq!(DEFAULT_SAMPLE_RATE, 100);
        assert_eq!(DEFAULT_CAPACITY_FOLLOW_SYMLINKS, false);
        assert_eq!(DEFAULT_CAPACITY_MAX_SYMLINK_DEPTH, 3);
        assert_eq!(DEFAULT_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, true);
        assert_eq!(DEFAULT_CAPACITY_MIN_TIMEOUT_SECS, 5);
        assert_eq!(DEFAULT_CAPACITY_MAX_TIMEOUT_SECS, 60);
        assert_eq!(DEFAULT_CAPACITY_STALL_TIMEOUT_SECS, 1);
    }
}
