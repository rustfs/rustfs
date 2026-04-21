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

use std::time::Duration;

/// Canonical environment variable name that specifies the scanner start delay in seconds.
/// If set, this overrides the cycle interval derived from `RUSTFS_SCANNER_SPEED`.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_START_DELAY_SECS=10`
pub const ENV_SCANNER_START_DELAY_SECS: &str = "RUSTFS_SCANNER_START_DELAY_SECS";

/// Deprecated compatibility alias for scanner start delay.
/// Prefer `RUSTFS_SCANNER_START_DELAY_SECS`.
#[deprecated(note = "Use RUSTFS_SCANNER_START_DELAY_SECS instead")]
pub const ENV_DATA_SCANNER_START_DELAY_SECS: &str = "RUSTFS_DATA_SCANNER_START_DELAY_SECS";

/// Environment variable that specifies the scanner cycle interval in seconds.
/// If set, this overrides the cycle interval derived from `RUSTFS_SCANNER_SPEED`.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_CYCLE=3600` (1 hour)
pub const ENV_SCANNER_CYCLE: &str = "RUSTFS_SCANNER_CYCLE";

/// Environment variable that selects the scanner speed preset.
/// Valid values: `fastest`, `fast`, `default`, `slow`, `slowest`.
/// Controls the sleep factor, maximum sleep duration, and cycle interval.
/// - Example: `export RUSTFS_SCANNER_SPEED=slow`
pub const ENV_SCANNER_SPEED: &str = "RUSTFS_SCANNER_SPEED";

/// Default scanner speed preset.
pub const DEFAULT_SCANNER_SPEED: &str = "default";

/// Environment variable that controls whether the scanner sleeps between operations.
/// When `true` (default), the scanner throttles itself. When `false`, it runs at full speed.
/// - Example: `export RUSTFS_SCANNER_IDLE_MODE=false`
pub const ENV_SCANNER_IDLE_MODE: &str = "RUSTFS_SCANNER_IDLE_MODE";

/// Environment variable that controls scanner cache save timeout in seconds.
/// The scanner enforces a minimum value of `1`.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS=30`
pub const ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS: &str = "RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS";

/// Default scanner idle mode.
pub const DEFAULT_SCANNER_IDLE_MODE: bool = true;

/// Compatibility flag kept for Patch 3 rollback windows.
///
/// Inline scanner heal execution has been removed in favor of heal-candidate enqueue.
/// When this flag is enabled, RustFS logs a warning and continues to use enqueue-based heal.
pub const ENV_SCANNER_INLINE_HEAL_ENABLE: &str = "RUSTFS_SCANNER_INLINE_HEAL_ENABLE";

/// Default inline scanner heal compatibility mode.
pub const DEFAULT_SCANNER_INLINE_HEAL_ENABLE: bool = false;

/// Scanner speed preset controlling throttling behavior.
///
/// Each preset defines three parameters:
/// - **sleep_factor**: Multiplier applied to elapsed work time to compute inter-object sleep.
/// - **max_sleep**: Upper bound on any single throttle sleep.
/// - **cycle_interval**: Base delay between scan cycles.
///
/// | Preset    | Factor | Max Sleep | Cycle Interval |
/// |-----------|--------|-----------|----------------|
/// | `fastest` | 0      | 0         | 1 second       |
/// | `fast`    | 1x     | 100ms     | 1 minute       |
/// | `default` | 2x     | 1 second  | 1 minute       |
/// | `slow`    | 10x    | 15 seconds| 1 minute       |
/// | `slowest` | 100x   | 15 seconds| 30 minutes     |
///
/// The cycle interval can be overridden by `RUSTFS_SCANNER_CYCLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScannerSpeed {
    Fastest,
    Fast,
    #[default]
    Default,
    Slow,
    Slowest,
}

impl ScannerSpeed {
    pub fn sleep_factor(self) -> f64 {
        match self {
            Self::Fastest => 0.0,
            Self::Fast => 1.0,
            Self::Default => 2.0,
            Self::Slow => 10.0,
            Self::Slowest => 100.0,
        }
    }

    pub fn max_sleep(self) -> Duration {
        match self {
            Self::Fastest => Duration::ZERO,
            Self::Fast => Duration::from_millis(100),
            Self::Default => Duration::from_secs(1),
            Self::Slow | Self::Slowest => Duration::from_secs(15),
        }
    }

    pub fn cycle_interval(self) -> Duration {
        match self {
            Self::Fastest => Duration::from_secs(1),
            Self::Fast | Self::Default | Self::Slow => Duration::from_secs(60),
            Self::Slowest => Duration::from_secs(30 * 60),
        }
    }

    pub fn from_env_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "fastest" => Self::Fastest,
            "fast" => Self::Fast,
            "slow" => Self::Slow,
            "slowest" => Self::Slowest,
            _ => Self::Default,
        }
    }
}

impl std::fmt::Display for ScannerSpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fastest => write!(f, "fastest"),
            Self::Fast => write!(f, "fast"),
            Self::Default => write!(f, "default"),
            Self::Slow => write!(f, "slow"),
            Self::Slowest => write!(f, "slowest"),
        }
    }
}
