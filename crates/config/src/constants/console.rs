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

/// CORS allowed origins for the endpoint service
/// Comma-separated list of origins or "*" for all origins
pub const ENV_CORS_ALLOWED_ORIGINS: &str = "RUSTFS_CORS_ALLOWED_ORIGINS";

/// Default CORS allowed origins for the endpoint service
/// Comes from the console service default
/// See DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS
pub const DEFAULT_CORS_ALLOWED_ORIGINS: &str = DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS;

/// CORS allowed origins for the console service
/// Comma-separated list of origins or "*" for all origins
pub const ENV_CONSOLE_CORS_ALLOWED_ORIGINS: &str = "RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS";

/// Default CORS allowed origins for the console service
pub const DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS: &str = "*";

/// Enable or disable the console service
pub const ENV_CONSOLE_ENABLE: &str = "RUSTFS_CONSOLE_ENABLE";

/// Address for the console service to bind to
pub const ENV_CONSOLE_ADDRESS: &str = "RUSTFS_CONSOLE_ADDRESS";

/// RUSTFS_CONSOLE_RATE_LIMIT_ENABLE
/// Enable or disable rate limiting for the console service
pub const ENV_CONSOLE_RATE_LIMIT_ENABLE: &str = "RUSTFS_CONSOLE_RATE_LIMIT_ENABLE";

/// Default console rate limit enable
/// This is the default value for enabling rate limiting on the console server.
/// Rate limiting helps protect against abuse and DoS attacks on the management interface.
/// Default value: false
/// Environment variable: RUSTFS_CONSOLE_RATE_LIMIT_ENABLE
/// Command line argument: --console-rate-limit-enable
/// Example: RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true
/// Example: --console-rate-limit-enable true
pub const DEFAULT_CONSOLE_RATE_LIMIT_ENABLE: bool = false;

/// Set the rate limit requests per minute for the console service
/// Limits the number of requests per minute per client IP when rate limiting is enabled
/// Default: 100 requests per minute
pub const ENV_CONSOLE_RATE_LIMIT_RPM: &str = "RUSTFS_CONSOLE_RATE_LIMIT_RPM";

/// Default console rate limit requests per minute
/// This is the default rate limit for console requests when rate limiting is enabled.
/// Limits the number of requests per minute per client IP to prevent abuse.
/// Default value: 100 requests per minute
/// Environment variable: RUSTFS_CONSOLE_RATE_LIMIT_RPM
/// Command line argument: --console-rate-limit-rpm
/// Example: RUSTFS_CONSOLE_RATE_LIMIT_RPM=100
/// Example: --console-rate-limit-rpm 100
pub const DEFAULT_CONSOLE_RATE_LIMIT_RPM: u32 = 100;

/// Set the console authentication timeout in seconds
/// Specifies how long a console authentication session remains valid
/// Default: 3600 seconds (1 hour)
/// Minimum: 300 seconds (5 minutes)
/// Maximum: 86400 seconds (24 hours)
pub const ENV_CONSOLE_AUTH_TIMEOUT: &str = "RUSTFS_CONSOLE_AUTH_TIMEOUT";

/// Default console authentication timeout in seconds
/// This is the default timeout for console authentication sessions.
/// After this timeout, users need to re-authenticate to access the console.
/// Default value: 3600 seconds (1 hour)
/// Environment variable: RUSTFS_CONSOLE_AUTH_TIMEOUT
/// Command line argument: --console-auth-timeout
/// Example: RUSTFS_CONSOLE_AUTH_TIMEOUT=3600
/// Example: --console-auth-timeout 3600
pub const DEFAULT_CONSOLE_AUTH_TIMEOUT: u64 = 3600;

/// Toggle update check
/// It controls whether to check for newer versions of rustfs
/// Default value: true
/// Environment variable: RUSTFS_CHECK_UPDATE
/// Example: RUSTFS_CHECK_UPDATE=false
pub const ENV_UPDATE_CHECK: &str = "RUSTFS_CHECK_UPDATE";

/// Default value for update toggle
pub const DEFAULT_UPDATE_CHECK: bool = true;
