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

/// Scanner admin config subsystem name.
pub const SCANNER_SUB_SYS: &str = "scanner";

/// Scanner config key selecting the speed preset.
pub const SCANNER_SPEED: &str = "speed";

/// Scanner config key overriding the scanner sleep multiplier.
pub const SCANNER_DELAY: &str = "delay";

/// Scanner config key overriding the maximum scanner sleep in seconds.
pub const SCANNER_MAX_WAIT: &str = "max_wait";

/// Scanner config key overriding the cycle interval in seconds.
pub const SCANNER_CYCLE: &str = "cycle";

/// Scanner config key setting the startup delay in seconds.
///
/// For compatibility, this also acts as the scanner cycle interval when
/// `cycle` is unset.
pub const SCANNER_START_DELAY: &str = "start_delay";

/// Scanner config key capping one cycle's runtime in seconds.
pub const SCANNER_CYCLE_MAX_DURATION: &str = "cycle_max_duration";

/// Scanner config key capping objects processed by one cycle.
pub const SCANNER_CYCLE_MAX_OBJECTS: &str = "cycle_max_objects";

/// Scanner config key capping directories entered by one cycle.
pub const SCANNER_CYCLE_MAX_DIRECTORIES: &str = "cycle_max_directories";

/// Scanner config key setting the periodic bitrot scan cycle in seconds.
pub const SCANNER_BITROT_CYCLE: &str = "bitrot_cycle";

/// Scanner config key controlling whether scanner throttling is enabled.
pub const SCANNER_IDLE_MODE: &str = "idle_mode";

/// Scanner config key controlling scanner cache save timeout in seconds.
pub const SCANNER_CACHE_SAVE_TIMEOUT: &str = "cache_save_timeout";

/// Scanner config key capping concurrent scanner set tasks.
pub const SCANNER_MAX_CONCURRENT_SET_SCANS: &str = "max_concurrent_set_scans";

/// Scanner config key capping concurrent scanner disk bucket walks per set.
pub const SCANNER_MAX_CONCURRENT_DISK_SCANS: &str = "max_concurrent_disk_scans";

/// Scanner config key controlling how often object loops yield.
pub const SCANNER_YIELD_EVERY_N_OBJECTS: &str = "yield_every_n_objects";

/// Scanner config key controlling object version count alerts.
pub const SCANNER_ALERT_EXCESS_VERSIONS: &str = "alert_excess_versions";

/// Scanner config key controlling retained version size alerts.
pub const SCANNER_ALERT_EXCESS_VERSION_SIZE: &str = "alert_excess_version_size";

/// Scanner config key controlling direct subfolder count alerts.
pub const SCANNER_ALERT_EXCESS_FOLDERS: &str = "alert_excess_folders";

/// Scanner config keys supported by the admin config subsystem.
pub const SCANNER_KEYS: &[&str] = &[
    SCANNER_SPEED,
    SCANNER_DELAY,
    SCANNER_MAX_WAIT,
    SCANNER_CYCLE,
    SCANNER_START_DELAY,
    SCANNER_CYCLE_MAX_DURATION,
    SCANNER_CYCLE_MAX_OBJECTS,
    SCANNER_CYCLE_MAX_DIRECTORIES,
    SCANNER_BITROT_CYCLE,
    SCANNER_IDLE_MODE,
    SCANNER_CACHE_SAVE_TIMEOUT,
    SCANNER_MAX_CONCURRENT_SET_SCANS,
    SCANNER_MAX_CONCURRENT_DISK_SCANS,
    SCANNER_YIELD_EVERY_N_OBJECTS,
    SCANNER_ALERT_EXCESS_VERSIONS,
    SCANNER_ALERT_EXCESS_VERSION_SIZE,
    SCANNER_ALERT_EXCESS_FOLDERS,
];

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

/// Environment variable that caps one scanner cycle's runtime in seconds.
/// A value of `0` disables the cycle runtime budget.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS=1800`
pub const ENV_SCANNER_CYCLE_MAX_DURATION_SECS: &str = "RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS";

/// Environment variable that caps objects processed by one scanner cycle.
/// A value of `0` disables the per-cycle object budget.
/// - Example: `export RUSTFS_SCANNER_CYCLE_MAX_OBJECTS=1000000`
pub const ENV_SCANNER_CYCLE_MAX_OBJECTS: &str = "RUSTFS_SCANNER_CYCLE_MAX_OBJECTS";

/// Environment variable that caps directories entered by one scanner cycle.
/// A value of `0` disables the per-cycle directory budget.
/// - Example: `export RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES=100000`
pub const ENV_SCANNER_CYCLE_MAX_DIRECTORIES: &str = "RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES";

/// Environment variable that selects the scanner speed preset.
/// Valid values: `fastest`, `fast`, `default`, `slow`, `slowest`.
/// Controls the sleep factor, maximum sleep duration, and cycle interval.
/// - Example: `export RUSTFS_SCANNER_SPEED=slow`
pub const ENV_SCANNER_SPEED: &str = "RUSTFS_SCANNER_SPEED";

/// Environment variable that overrides the scanner sleep multiplier.
/// - Example: `export RUSTFS_SCANNER_DELAY=30.0`
pub const ENV_SCANNER_DELAY: &str = "RUSTFS_SCANNER_DELAY";

/// Environment variable that overrides the maximum scanner sleep in seconds.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_MAX_WAIT_SECS=15`
pub const ENV_SCANNER_MAX_WAIT_SECS: &str = "RUSTFS_SCANNER_MAX_WAIT_SECS";

/// Default scanner speed preset.
pub const DEFAULT_SCANNER_SPEED: &str = "default";

/// Default scanner cycle runtime budget.
/// `0` keeps the existing unbounded per-cycle behavior.
pub const DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS: u64 = 0;

/// Default scanner per-cycle object budget.
/// `0` keeps the existing unbounded per-cycle behavior.
pub const DEFAULT_SCANNER_CYCLE_MAX_OBJECTS: u64 = 0;

/// Default scanner per-cycle directory budget.
/// `0` keeps the existing unbounded per-cycle behavior.
pub const DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES: u64 = 0;

/// Environment variable that specifies the periodic bitrot scan cycle in seconds.
/// When set to `0`, `true`, `on`, or `yes`, every scanner cycle runs in deep mode.
/// When set to `false`, `off`, `no`, or `disabled`, periodic deep scans are disabled.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_BITROT_CYCLE_SECS=2592000` (30 days)
pub const ENV_SCANNER_BITROT_CYCLE_SECS: &str = "RUSTFS_SCANNER_BITROT_CYCLE_SECS";

/// Default bitrot scan cycle used by the scanner.
pub const DEFAULT_SCANNER_BITROT_CYCLE_SECS: u64 = 30 * 24 * 60 * 60;

/// Environment variable that controls how many object versions trigger scanner alerts.
/// - Example: `export RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS=100`
pub const ENV_SCANNER_ALERT_EXCESS_VERSIONS: &str = "RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS";

/// Default object version count that triggers scanner alerts.
pub const DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS: u64 = 100;

/// Environment variable that controls how many cumulative bytes across object versions trigger scanner alerts.
/// - Example: `export RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE=1099511627776`
pub const ENV_SCANNER_ALERT_EXCESS_VERSION_SIZE: &str = "RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE";

/// Default cumulative object version bytes that trigger scanner alerts.
pub const DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

/// Environment variable that controls how many subfolders trigger scanner alerts.
/// - Example: `export RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS=65538`
pub const ENV_SCANNER_ALERT_EXCESS_FOLDERS: &str = "RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS";

/// Default subfolder count that triggers scanner alerts.
/// Allows Proxmox Backup Server's chunk namespace layout.
pub const DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS: u64 = 65_538;

/// Environment variable that controls whether the scanner sleeps between operations.
/// When `true` (default), the scanner throttles itself. When `false`, it runs at full speed.
/// - Example: `export RUSTFS_SCANNER_IDLE_MODE=false`
pub const ENV_SCANNER_IDLE_MODE: &str = "RUSTFS_SCANNER_IDLE_MODE";

/// Environment variable that controls scanner cache save timeout in seconds.
/// The scanner enforces a minimum value of `1`.
/// - Unit: seconds (u64).
/// - Example: `export RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS=30`
pub const ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS: &str = "RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS";

/// Default scanner cache save timeout in seconds.
pub const DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS: u64 = 30;

/// Environment variable that caps concurrent scanner set tasks.
/// A value of `0` keeps the existing topology-based concurrency.
/// - Example: `export RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS=2`
pub const ENV_SCANNER_MAX_CONCURRENT_SET_SCANS: &str = "RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS";

/// Environment variable that caps concurrent scanner disk bucket walks per set.
/// A value of `0` keeps the existing disk-count-based concurrency.
/// - Example: `export RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS=1`
pub const ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS: &str = "RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS";

/// Environment variable that controls how often scanner object loops yield to the async runtime.
/// A value of `0` disables this extra object-count yield.
/// - Example: `export RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS=32`
pub const ENV_SCANNER_YIELD_EVERY_N_OBJECTS: &str = "RUSTFS_SCANNER_YIELD_EVERY_N_OBJECTS";

/// Default scanner idle mode.
pub const DEFAULT_SCANNER_IDLE_MODE: bool = true;

/// Default set scan concurrency budget.
/// `0` means no additional limit beyond deployment topology.
pub const DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS: usize = 0;

/// Default disk scan concurrency budget.
/// `0` means no additional limit beyond available disks in the set.
pub const DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS: usize = 0;

/// Default object interval for cooperative scanner yields.
pub const DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS: u64 = 128;

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
/// The sleep factor, max sleep, and cycle interval can be overridden by
/// `RUSTFS_SCANNER_DELAY`, `RUSTFS_SCANNER_MAX_WAIT_SECS`, and
/// `RUSTFS_SCANNER_CYCLE`.
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

    pub fn parse_str(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fastest" => Some(Self::Fastest),
            "fast" => Some(Self::Fast),
            "default" => Some(Self::Default),
            "slow" => Some(Self::Slow),
            "slowest" => Some(Self::Slowest),
            _ => None,
        }
    }

    pub fn from_env_str(s: &str) -> Self {
        Self::parse_str(s).unwrap_or_default()
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
