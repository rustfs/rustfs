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

/// Enable or disable public `/health` and `/health/ready` endpoints.
/// When disabled, the routes are not registered and return 404.
pub const ENV_HEALTH_ENDPOINT_ENABLE: &str = "RUSTFS_HEALTH_ENDPOINT_ENABLE";
pub const DEFAULT_HEALTH_ENDPOINT_ENABLE: bool = true;

/// Cache TTL for storage readiness runtime-state evaluation (milliseconds).
/// This reduces storage-layer pressure when probes are called at high frequency.
pub const ENV_HEALTH_READINESS_CACHE_TTL_MS: &str = "RUSTFS_HEALTH_READINESS_CACHE_TTL_MS";
pub const DEFAULT_HEALTH_READINESS_CACHE_TTL_MS: u64 = 1000;

/// Timeout for cluster health readiness collectors (milliseconds).
/// This bounds expensive storage and lock quorum checks used by cluster probes.
pub const ENV_HEALTH_CLUSTER_TIMEOUT_MS: &str = "RUSTFS_HEALTH_CLUSTER_TIMEOUT_MS";
pub const DEFAULT_HEALTH_CLUSTER_TIMEOUT_MS: u64 = 2000;

/// Maximum time to wait for local node runtime readiness (storage / IAM / lock
/// quorum) during startup before failing fast (seconds).
///
/// On slow or multi-node cold starts — e.g. Docker/Kubernetes/NAS deployments
/// where peer DNS records and erasure-format quorum take time to converge — this
/// budget must be large enough to outlast the internal startup DNS-retry window
/// and the format-load retry loop. Increase it for slow NAS/edge clusters; lower
/// it for fast single-node setups that should fail fast. A value of `0` is
/// treated as the default rather than an instant timeout.
pub const ENV_STARTUP_READINESS_MAX_WAIT_SECS: &str = "RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS";
pub const DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS: u64 = 120;

/// Enable minimal health payload mode for GET `/health*` responses.
/// When enabled, only `status` and `ready` fields are returned.
pub const ENV_HEALTH_MINIMAL_RESPONSE_ENABLE: &str = "RUSTFS_HEALTH_MINIMAL_RESPONSE_ENABLE";
pub const DEFAULT_HEALTH_MINIMAL_RESPONSE_ENABLE: bool = false;

/// Enable busy protection for health probes.
/// When enabled with a positive request threshold, alias health probes may
/// return 429 when active HTTP requests exceed the threshold.
pub const ENV_HEALTH_COMPAT_BUSY_CHECK_ENABLE: &str = "RUSTFS_HEALTH_COMPAT_BUSY_CHECK_ENABLE";
pub const DEFAULT_HEALTH_COMPAT_BUSY_CHECK_ENABLE: bool = false;

/// Max active HTTP requests; alias health probes report busy (429) when active requests reach or exceed this value.
/// Set to 0 to disable thresholding even when busy protection is enabled.
pub const ENV_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS: &str = "RUSTFS_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS";
pub const DEFAULT_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS: usize = 0;

/// Enable KMS readiness check for alias readiness probes.
/// When enabled, `/health/ready` additionally requires KMS service to be
/// in running state if a global KMS manager exists.
pub const ENV_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE: &str = "RUSTFS_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE";
pub const DEFAULT_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE: bool = false;

/// Enable peer-health readiness impact.
/// When disabled, peer-health state is reported but does not affect readiness.
pub const ENV_HEALTH_PEER_READY_CHECK_ENABLE: &str = "RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE";
pub const DEFAULT_HEALTH_PEER_READY_CHECK_ENABLE: bool = false;
