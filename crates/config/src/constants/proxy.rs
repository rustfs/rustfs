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

/// RUSTFS_HTTP_TRUSTED_PROXIES
/// Environment variable name for trusted proxies configuration
/// Example: RUSTFS_HTTP_TRUSTED_PROXIES="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7"
/// If not set, defaults to local loopback and common private networks
/// Used in proxy configuration loading
/// Refer to `TrustedProxiesConfig` for details
pub const ENV_TRUSTED_PROXIES: &str = "RUSTFS_HTTP_TRUSTED_PROXIES";

/// Default trusted proxies: Local loopback and common private networks
/// Used when the environment variable is not set
/// Format: Comma-separated list of IPs and CIDR blocks
/// Example: RUSTFS_HTTP_TRUSTED_PROXIES="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7"
/// Refer to `TrustedProxiesConfig` for details
pub const DEFAULT_TRUSTED_PROXIES: &str = "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7";
