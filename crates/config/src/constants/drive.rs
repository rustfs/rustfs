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

/// Legacy global drive timeout fallback.
/// Deprecated in favor of per-operation drive timeout knobs.
pub const ENV_DRIVE_MAX_TIMEOUT_DURATION: &str = "RUSTFS_DRIVE_MAX_TIMEOUT_DURATION";

/// Default timeout in seconds for the legacy global drive timeout fallback.
pub const DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS: u64 = 30;

/// Timeout for metadata-oriented drive operations such as `read_metadata`.
pub const ENV_DRIVE_METADATA_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_METADATA_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_METADATA_TIMEOUT_SECS: u64 = 5;

/// Timeout for `disk_info()` calls on local and remote drives.
pub const ENV_DRIVE_DISK_INFO_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_DISK_INFO_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_DISK_INFO_TIMEOUT_SECS: u64 = 5;

/// Timeout for `list_dir()` style metadata listing operations.
pub const ENV_DRIVE_LIST_DIR_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_LIST_DIR_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_LIST_DIR_TIMEOUT_SECS: u64 = 5;

/// Total timeout for `walk_dir()` operations.
pub const ENV_DRIVE_WALKDIR_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_WALKDIR_TIMEOUT_SECS: u64 = 5;

/// Maximum time without forward progress while consuming a `walk_dir()` stream.
pub const ENV_DRIVE_WALKDIR_STALL_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_WALKDIR_STALL_TIMEOUT_SECS: u64 = 5;

/// Interval in seconds between active health probes for local and remote drives.
pub const ENV_DRIVE_ACTIVE_CHECK_INTERVAL_SECS: &str = "RUSTFS_DRIVE_ACTIVE_CHECK_INTERVAL_SECS";
pub const DEFAULT_DRIVE_ACTIVE_CHECK_INTERVAL_SECS: u64 = 15;

/// Timeout in seconds for a single active health probe.
pub const ENV_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS: &str = "RUSTFS_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS";
pub const DEFAULT_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS: u64 = 5;

/// Number of consecutive failures before a suspect drive is classified as offline.
pub const ENV_DRIVE_SUSPECT_FAILURE_THRESHOLD: &str = "RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD";
pub const DEFAULT_DRIVE_SUSPECT_FAILURE_THRESHOLD: u64 = 2;

/// Number of consecutive successful recovery probes before a returning drive is considered online again.
pub const ENV_DRIVE_RETURNING_SUCCESS_THRESHOLD: &str = "RUSTFS_DRIVE_RETURNING_SUCCESS_THRESHOLD";
pub const DEFAULT_DRIVE_RETURNING_SUCCESS_THRESHOLD: u64 = 3;

/// Probe interval in seconds while a drive is in the recovery path.
pub const ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS: &str = "RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS";
pub const DEFAULT_DRIVE_RETURNING_PROBE_INTERVAL_SECS: u64 = 2;

/// Duration in seconds for classifying a recovered drive as a short offline event.
pub const ENV_DRIVE_OFFLINE_GRACE_PERIOD_SECS: &str = "RUSTFS_DRIVE_OFFLINE_GRACE_PERIOD_SECS";
pub const DEFAULT_DRIVE_OFFLINE_GRACE_PERIOD_SECS: u64 = 30;

/// Duration in seconds after which a recovered drive is classified as long offline.
pub const ENV_DRIVE_LONG_OFFLINE_THRESHOLD_SECS: &str = "RUSTFS_DRIVE_LONG_OFFLINE_THRESHOLD_SECS";
pub const DEFAULT_DRIVE_LONG_OFFLINE_THRESHOLD_SECS: u64 = 172_800;
