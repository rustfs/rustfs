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

/// Environment variable name that enables or disables auto-heal functionality.
/// - Purpose: Control whether the system automatically performs heal operations.
/// - Valid values: "true" or "false" (case insensitive).
/// - Semantics: When set to "true", auto-heal is enabled and the system will automatically attempt to heal detected issues; when set to "false", auto-heal is disabled and healing must be triggered manually.
/// - Example: `export RUSTFS_HEAL_AUTO_HEAL_ENABLE=true`
/// - Note: Enabling auto-heal can improve system resilience by automatically addressing issues, but may increase resource usage; evaluate based on your operational requirements.
pub const ENV_HEAL_AUTO_HEAL_ENABLE: &str = "RUSTFS_HEAL_AUTO_HEAL_ENABLE";

/// Environment variable name that specifies the heal queue size.
///
/// - Purpose: Set the maximum number of heal requests that can be queued.
/// - Unit: number of requests (usize).
/// - Valid values: any positive integer.
/// - Semantics: When the heal queue reaches this size, new heal requests may be rejected or blocked until space is available; tune according to expected heal workload and system capacity.
/// - Example: `export RUSTFS_HEAL_QUEUE_SIZE=10000`
/// - Note: A larger queue size can accommodate bursts of heal requests but may increase memory usage.
pub const ENV_HEAL_QUEUE_SIZE: &str = "RUSTFS_HEAL_QUEUE_SIZE";
/// Environment variable name that specifies the heal interval in seconds.
/// - Purpose: Define the time interval between successive heal operations.
/// - Unit: seconds (u64).
/// - Valid values: any positive integer.
/// - Semantics: This interval controls how frequently the heal manager checks for and processes heal requests; shorter intervals lead to more responsive healing but may increase system load.
/// - Example: `export RUSTFS_HEAL_INTERVAL_SECS=10`
/// - Note: Choose an interval that balances healing responsiveness with overall system performance.
pub const ENV_HEAL_INTERVAL_SECS: &str = "RUSTFS_HEAL_INTERVAL_SECS";

/// Environment variable name that specifies the heal task timeout in seconds.
/// - Purpose: Set the maximum duration allowed for a heal task to complete.
/// - Unit: seconds (u64).
/// - Valid values: any positive integer.
/// - Semantics: If a heal task exceeds this timeout, it may be aborted or retried; tune according to the expected duration of heal operations and system performance characteristics.
/// - Example: `export RUSTFS_HEAL_TASK_TIMEOUT_SECS=300`
/// - Note: Setting an appropriate timeout helps prevent long-running heal tasks from impacting system stability.
pub const ENV_HEAL_TASK_TIMEOUT_SECS: &str = "RUSTFS_HEAL_TASK_TIMEOUT_SECS";

/// Environment variable name that specifies the maximum number of concurrent heal operations.
/// - Purpose: Limit the number of heal operations that can run simultaneously.
/// - Unit: number of operations (usize).
/// - Valid values: any positive integer.
/// - Semantics: This limit helps control resource usage during healing; tune according to system capacity and expected heal workload.
/// - Example: `export RUSTFS_HEAL_MAX_CONCURRENT_HEALS=4`
/// - Note: A higher concurrency limit can speed up healing but may lead to resource contention.
pub const ENV_HEAL_MAX_CONCURRENT_HEALS: &str = "RUSTFS_HEAL_MAX_CONCURRENT_HEALS";

/// Default value for enabling authentication for heal operations if not specified in the environment variable.
/// - Value: true (authentication enabled).
/// - Rationale: Enabling authentication by default enhances security for heal operations.
/// - Adjustments: Users may disable this feature via the `RUSTFS_HEAL_AUTO_HEAL_ENABLE` environment variable based on their security requirements.
pub const DEFAULT_HEAL_AUTO_HEAL_ENABLE: bool = true;

/// Default heal queue size if not specified in the environment variable.
///
/// - Value: 10,000 requests.
/// - Rationale: This default size balances the need to handle typical heal workloads without excessive memory consumption.
/// - Adjustments: Users may modify this value via the `RUSTFS_HEAL_QUEUE_SIZE` environment variable based on their specific use cases and system capabilities.
pub const DEFAULT_HEAL_QUEUE_SIZE: usize = 10_000;

/// Default heal interval in seconds if not specified in the environment variable.
/// - Value: 10 seconds.
/// - Rationale: This default interval provides a reasonable balance between healing responsiveness and system load for most deployments.
/// - Adjustments: Users may modify this value via the `RUSTFS_HEAL_INTERVAL_SECS` environment variable based on their specific healing requirements and system performance.
pub const DEFAULT_HEAL_INTERVAL_SECS: u64 = 10;

/// Default heal task timeout in seconds if not specified in the environment variable.
/// - Value: 300 seconds (5 minutes).
/// - Rationale: This default timeout allows sufficient time for most heal operations to complete while preventing excessively long-running tasks.
/// - Adjustments: Users may modify this value via the `RUSTFS_HEAL_TASK_TIMEOUT_SECS` environment variable based on their specific heal operation characteristics and system performance.
pub const DEFAULT_HEAL_TASK_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Default maximum number of concurrent heal operations if not specified in the environment variable.
/// - Value: 4 concurrent heal operations.
/// - Rationale: This default concurrency limit helps balance healing speed with resource usage, preventing system overload.
/// - Adjustments: Users may modify this value via the `RUSTFS_HEAL_MAX_CONCURRENT_HEALS` environment variable based on their system capacity and expected heal workload.
pub const DEFAULT_HEAL_MAX_CONCURRENT_HEALS: usize = 4;
