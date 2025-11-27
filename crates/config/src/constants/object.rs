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

/// Environment variable name to toggle object-level in-memory caching.
///
/// - Purpose: Enable or disable the object-level in-memory cache (moka).
/// - Acceptable values: `"true"` / `"false"` (case-insensitive) or a boolean typed config.
/// - Semantics: When enabled, the system keeps fully-read objects in memory to reduce backend requests; when disabled, reads bypass the object cache.
/// - Example: `export RUSTFS_OBJECT_CACHE_ENABLE=true`
/// - Note: Evaluate together with `RUSTFS_OBJECT_CACHE_CAPACITY_MB`, TTL/TTI and concurrency thresholds to balance memory usage and throughput.
pub const ENV_OBJECT_CACHE_ENABLE: &str = "RUSTFS_OBJECT_CACHE_ENABLE";

/// Environment variable name that specifies the object cache capacity in megabytes.
///
/// - Purpose: Set the maximum total capacity of the object cache (in MB).
/// - Unit: MB (1 MB = 1_048_576 bytes).
/// - Valid values: any positive integer (0 may indicate disabled or alternative handling).
/// - Semantics: When the moka cache reaches this capacity, eviction policies will remove entries; tune according to available memory and object size distribution.
/// - Example: `export RUSTFS_OBJECT_CACHE_CAPACITY_MB=512`
/// - Note: Actual memory usage will be slightly higher due to object headers and indexing overhead.
pub const ENV_OBJECT_CACHE_CAPACITY_MB: &str = "RUSTFS_OBJECT_CACHE_CAPACITY_MB";

/// Environment variable name for maximum object size eligible for caching in megabytes.
///
/// - Purpose: Define the upper size limit for individual objects to be considered for caching.
/// - Unit: MB (1 MB = 1_048_576 bytes).
/// - Valid values: any positive integer; objects larger than this size will not be cached.
/// - Semantics: Prevents caching of excessively large objects that could monopolize cache capacity; tune based on typical object size distribution.
/// - Example: `export RUSTFS_OBJECT_CACHE_MAX_OBJECT_SIZE_MB=50`
/// - Note: Setting this too low may reduce cache effectiveness; setting it too high may lead to inefficient memory usage.
pub const ENV_OBJECT_CACHE_MAX_OBJECT_SIZE_MB: &str = "RUSTFS_OBJECT_CACHE_MAX_OBJECT_SIZE_MB";

/// Environment variable name for object cache TTL (time-to-live) in seconds.
///
/// - Purpose: Specify the maximum lifetime of a cached entry from the moment it is written.
/// - Unit: seconds (u64).
/// - Semantics: TTL acts as a hard upper bound; entries older than TTL are considered expired and removed by periodic cleanup.
/// - Example: `export RUSTFS_OBJECT_CACHE_TTL_SECS=300`
/// - Note: TTL and TTI both apply; either policy can cause eviction.
pub const ENV_OBJECT_CACHE_TTL_SECS: &str = "RUSTFS_OBJECT_CACHE_TTL_SECS";

/// Environment variable name for object cache TTI (time-to-idle) in seconds.
///
/// - Purpose: Specify how long an entry may remain in cache without being accessed before it is evicted.
/// - Unit: seconds (u64).
/// - Semantics: TTI helps remove one-time or infrequently used entries; frequent accesses reset idle timers but do not extend beyond TTL unless additional logic exists.
/// - Example: `export RUSTFS_OBJECT_CACHE_TTI_SECS=120`
/// - Note: Works together with TTL to keep the cache populated with actively used objects.
pub const ENV_OBJECT_CACHE_TTI_SECS: &str = "RUSTFS_OBJECT_CACHE_TTI_SECS";

/// Environment variable name for threshold of "hot" object hit count used to extend life.
///
/// - Purpose: Define a hit-count threshold to mark objects as "hot" so they may be treated preferentially near expiration.
/// - Valid values: positive integer (usize).
/// - Semantics: Objects reaching this hit count can be considered for relaxed eviction to avoid thrashing hot items.
/// - Example: `export RUSTFS_OBJECT_HOT_MIN_HITS_TO_EXTEND=5`
/// - Note: This is an optional enhancement and requires cache-layer statistics and extension logic to take effect.
pub const ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND: &str = "RUSTFS_OBJECT_HOT_MIN_HITS_TO_EXTEND";

/// Environment variable name for high concurrency threshold used in adaptive buffering.
///
/// - Purpose: When concurrent request count exceeds this threshold, the system enters a "high concurrency" optimization mode to reduce per-request buffer sizes.
/// - Unit: request count (usize).
/// - Semantics: High concurrency mode reduces per-request buffers (e.g., to a fraction of base size) to protect overall memory and fairness.
/// - Example: `export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=8`
/// - Note: This affects buffering and I/O behavior, not cache capacity directly.
pub const ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD: &str = "RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD";

/// Environment variable name for medium concurrency threshold used in adaptive buffering.
///
/// - Purpose: Define the boundary for "medium concurrency" where more moderate buffer adjustments apply.
/// - Unit: request count (usize).
/// - Semantics: In the medium range, buffers are reduced moderately to balance throughput and memory efficiency.
/// - Example: `export RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD=4`
/// - Note: Tune this value based on target workload and hardware.
pub const ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD: &str = "RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD";

/// Default: object caching is disabled.
///
/// - Semantics: Safe default to avoid unexpected memory usage or cache consistency concerns when not explicitly enabled.
pub const DEFAULT_OBJECT_CACHE_ENABLE: bool = false;

/// Default object cache capacity in MB.
///
/// - Default: 100 MB (can be overridden by `RUSTFS_OBJECT_CACHE_CAPACITY_MB`).
/// - Note: Choose a conservative default to reduce memory pressure in development/testing.
pub const DEFAULT_OBJECT_CACHE_CAPACITY_MB: u64 = 100;

/// Default maximum object size eligible for caching in MB.
///
/// - Default: 10 MB (can be overridden by `RUSTFS_OBJECT_CACHE_MAX_OBJECT_SIZE_MB`).
/// - Note: Balances caching effectiveness with memory usage.
pub const DEFAULT_OBJECT_CACHE_MAX_OBJECT_SIZE_MB: usize = 10;

/// Maximum concurrent requests before applying aggressive optimization.
///
/// When concurrent requests exceed this threshold (>8), the system switches to
/// aggressive memory optimization mode, reducing buffer sizes to 40% of base size
/// to prevent memory exhaustion and ensure fair resource allocation.
pub const DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD: usize = 8;

/// Medium concurrency threshold for buffer size adjustment.
///
/// At this level (3-4 requests), buffers are reduced to 75% of base size to
/// balance throughput and memory efficiency as load increases.
pub const DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD: usize = 4;

/// Time-to-live for cached objects (5 minutes = 300 seconds).
///
/// After this duration, cached objects are automatically expired by Moka's
/// background cleanup process, even if they haven't been accessed. This prevents
/// stale data from consuming cache capacity indefinitely.
pub const DEFAULT_OBJECT_CACHE_TTL_SECS: u64 = 300;

/// Time-to-idle for cached objects (2 minutes = 120 seconds).
///
/// Objects that haven't been accessed for this duration are automatically evicted,
/// even if their TTL hasn't expired. This ensures cache is populated with actively
/// used objects and clears out one-time reads efficiently.
pub const DEFAULT_OBJECT_CACHE_TTI_SECS: u64 = 120;

/// Minimum hit count to extend object lifetime beyond TTL.
///
/// "Hot" objects that have been accessed at least this many times are treated
/// specially - they can survive longer in cache even as they approach TTL expiration.
/// This prevents frequently accessed objects from being evicted prematurely.
pub const DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND: usize = 5;
