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

/// Timeout for establishing a new internode gRPC connection.
pub const ENV_INTERNODE_CONNECT_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS: u64 = 3;

/// TCP keepalive interval for internode gRPC channels.
pub const ENV_INTERNODE_TCP_KEEPALIVE_SECS: &str = "RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS";
pub const DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS: u64 = 10;

/// HTTP/2 keepalive interval for internode gRPC channels.
pub const ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS: &str = "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS";
pub const DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 5;

/// HTTP/2 keepalive timeout for internode gRPC channels.
pub const ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 3;

/// Overall timeout for a single internode gRPC request.
pub const ENV_INTERNODE_RPC_TIMEOUT_SECS: &str = "RUSTFS_INTERNODE_RPC_TIMEOUT_SECS";
pub const DEFAULT_INTERNODE_RPC_TIMEOUT_SECS: u64 = 10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internode_timeout_defaults_stay_in_expected_bounds() {
        assert_eq!(DEFAULT_INTERNODE_CONNECT_TIMEOUT_SECS, 3);
        assert_eq!(DEFAULT_INTERNODE_TCP_KEEPALIVE_SECS, 10);
        assert_eq!(DEFAULT_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS, 5);
        assert_eq!(DEFAULT_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS, 3);
        assert_eq!(DEFAULT_INTERNODE_RPC_TIMEOUT_SECS, 10);
    }

    #[test]
    fn internode_timeout_env_names_are_stable() {
        assert_eq!(ENV_INTERNODE_CONNECT_TIMEOUT_SECS, "RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS");
        assert_eq!(ENV_INTERNODE_TCP_KEEPALIVE_SECS, "RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS");
        assert_eq!(
            ENV_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS,
            "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS"
        );
        assert_eq!(
            ENV_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS,
            "RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS"
        );
        assert_eq!(ENV_INTERNODE_RPC_TIMEOUT_SECS, "RUSTFS_INTERNODE_RPC_TIMEOUT_SECS");
    }
}
