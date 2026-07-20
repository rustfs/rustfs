//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/// Enable or disable per-client rate limiting for the S3 API.
///
/// When enabled (and `RUSTFS_API_RATE_LIMIT_RPM` > 0), requests are throttled
/// per client IP using a token bucket; over-limit requests receive
/// `429 Too Many Requests` with a `Retry-After` header. Internode RPC/gRPC,
/// health probes, and the console (which has its own limiter) are exempt.
/// Environment variable: RUSTFS_API_RATE_LIMIT_ENABLE
/// Example: RUSTFS_API_RATE_LIMIT_ENABLE=true
pub const ENV_API_RATE_LIMIT_ENABLE: &str = "RUSTFS_API_RATE_LIMIT_ENABLE";

/// Default for `RUSTFS_API_RATE_LIMIT_ENABLE`.
///
/// Disabled by default: RustFS ships permissive and operators opt in to
/// abuse-protection hardening. When disabled the request path is unchanged.
pub const DEFAULT_API_RATE_LIMIT_ENABLE: bool = false;

/// Sustained S3 API request budget per client IP, in requests per minute.
///
/// `0` means unlimited (rate limiting stays inert even when enabled).
/// Environment variable: RUSTFS_API_RATE_LIMIT_RPM
/// Example: RUSTFS_API_RATE_LIMIT_RPM=6000
pub const ENV_API_RATE_LIMIT_RPM: &str = "RUSTFS_API_RATE_LIMIT_RPM";

/// Default for `RUSTFS_API_RATE_LIMIT_RPM`.
///
/// `0` (unlimited) so that setting only the enable switch cannot throttle
/// traffic by surprise; operators must choose an explicit budget.
pub const DEFAULT_API_RATE_LIMIT_RPM: u32 = 0;

/// Burst capacity per client IP (maximum tokens in the bucket).
///
/// Allows short spikes above the sustained rate. `0` means "same as RPM".
/// Environment variable: RUSTFS_API_RATE_LIMIT_BURST
/// Example: RUSTFS_API_RATE_LIMIT_BURST=200
pub const ENV_API_RATE_LIMIT_BURST: &str = "RUSTFS_API_RATE_LIMIT_BURST";

/// Default for `RUSTFS_API_RATE_LIMIT_BURST` (`0` = same as RPM).
pub const DEFAULT_API_RATE_LIMIT_BURST: u32 = 0;

/// Sustained S3 API request budget per addressed bucket, in requests per
/// minute — a collective ceiling shared by all clients of that bucket.
///
/// Complements the per-client-IP dimension: it protects the server from one
/// hot bucket regardless of how many client IPs the traffic comes from. `0`
/// disables the bucket dimension. Requires `RUSTFS_API_RATE_LIMIT_ENABLE`.
/// Environment variable: RUSTFS_API_RATE_LIMIT_BUCKET_RPM
/// Example: RUSTFS_API_RATE_LIMIT_BUCKET_RPM=60000
pub const ENV_API_RATE_LIMIT_BUCKET_RPM: &str = "RUSTFS_API_RATE_LIMIT_BUCKET_RPM";

/// Default for `RUSTFS_API_RATE_LIMIT_BUCKET_RPM` (`0` = dimension disabled).
pub const DEFAULT_API_RATE_LIMIT_BUCKET_RPM: u32 = 0;

/// Burst capacity per bucket (maximum tokens in the bucket-dimension bucket).
///
/// `0` means "same as `RUSTFS_API_RATE_LIMIT_BUCKET_RPM`".
/// Environment variable: RUSTFS_API_RATE_LIMIT_BUCKET_BURST
/// Example: RUSTFS_API_RATE_LIMIT_BUCKET_BURST=2000
pub const ENV_API_RATE_LIMIT_BUCKET_BURST: &str = "RUSTFS_API_RATE_LIMIT_BUCKET_BURST";

/// Default for `RUSTFS_API_RATE_LIMIT_BUCKET_BURST` (`0` = same as bucket RPM).
pub const DEFAULT_API_RATE_LIMIT_BUCKET_BURST: u32 = 0;

/// Maximum concurrently served connections on the main API listener.
///
/// `0` (the default) means unlimited. When set, the accept loop stops
/// accepting once the cap is reached and lets the kernel backlog absorb
/// bursts, releasing capacity as connections close. This bounds file
/// descriptor and memory usage under a connection flood.
///
/// The cap covers everything on the main listener — S3, admin, console,
/// and internode gRPC — so size it well above peer-node count plus the
/// expected client concurrency.
/// Environment variable: RUSTFS_API_MAX_CONNECTIONS
/// Example: RUSTFS_API_MAX_CONNECTIONS=10000
pub const ENV_API_MAX_CONNECTIONS: &str = "RUSTFS_API_MAX_CONNECTIONS";

/// Default for `RUSTFS_API_MAX_CONNECTIONS` (`0` = unlimited).
pub const DEFAULT_API_MAX_CONNECTIONS: usize = 0;
