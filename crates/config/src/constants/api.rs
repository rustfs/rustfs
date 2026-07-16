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
