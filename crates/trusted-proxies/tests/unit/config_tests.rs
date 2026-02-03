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

use rustfs_config::{DEFAULT_TRUSTED_PROXY_PROXIES, ENV_TRUSTED_PROXY_PROXIES};
use rustfs_trusted_proxies::{ConfigLoader, TrustedProxy, TrustedProxyConfig, ValidationMode};
use std::net::IpAddr;

#[test]
#[allow(unsafe_code)]
fn test_config_loader_default() {
    unsafe {
        std::env::remove_var(ENV_TRUSTED_PROXY_PROXIES);
    }
    let config = ConfigLoader::from_env_or_default();
    assert_eq!(config.server_addr.port(), 9000);
    assert!(!config.proxy.proxies.is_empty());
    assert_eq!(config.proxy.validation_mode, ValidationMode::HopByHop);
    assert!(config.proxy.enable_rfc7239);
    assert_eq!(config.proxy.max_hops, 10);
}

#[test]
#[allow(unsafe_code)]
fn test_config_loader_env_vars() {
    unsafe {
        std::env::set_var(ENV_TRUSTED_PROXY_PROXIES, "192.168.1.0/24,10.0.0.0/8");
    }
    unsafe {
        std::env::set_var("RUSTFS_TRUSTED_PROXY_VALIDATION_MODE", "strict");
    }
    unsafe {
        std::env::set_var("RUSTFS_TRUSTED_PROXY_MAX_HOPS", "5");
    }

    let config = ConfigLoader::from_env();

    if let Ok(config) = config {
        assert_eq!(config.server_addr.port(), 9000);
        assert_eq!(config.proxy.validation_mode, ValidationMode::Strict);
        assert_eq!(config.proxy.max_hops, 5);

        unsafe {
            std::env::remove_var(ENV_TRUSTED_PROXY_PROXIES);
        }
        unsafe {
            std::env::remove_var("RUSTFS_TRUSTED_PROXY_VALIDATION_MODE");
        }
        unsafe {
            std::env::remove_var("RUSTFS_TRUSTED_PROXY_MAX_HOPS");
        }
        unsafe {
            std::env::remove_var("SERVER_PORT");
        }
    } else {
        panic!("Failed to load configuration from environment variables");
    }
}

#[test]
fn test_trusted_proxy_config() {
    let proxies = vec![
        TrustedProxy::Single("192.168.1.1".parse().unwrap()),
        TrustedProxy::Cidr("10.0.0.0/8".parse().unwrap()),
    ];

    let config = TrustedProxyConfig::new(proxies.clone(), ValidationMode::Strict, true, 10, true, vec![]);

    assert_eq!(config.proxies.len(), 2);
    assert_eq!(config.validation_mode, ValidationMode::Strict);
    assert!(config.enable_rfc7239);
    assert_eq!(config.max_hops, 10);
    assert!(config.enable_chain_continuity_check);

    let test_ip: IpAddr = "192.168.1.1".parse().unwrap();
    let test_socket_addr = std::net::SocketAddr::new(test_ip, 8080);
    assert!(config.is_trusted(&test_socket_addr));

    let test_ip2: IpAddr = "10.0.1.1".parse().unwrap();
    let test_socket_addr2 = std::net::SocketAddr::new(test_ip2, 8080);
    assert!(config.is_trusted(&test_socket_addr2));
}

#[test]
fn test_trusted_proxy_contains() {
    let single_proxy = TrustedProxy::Single("192.168.1.1".parse().unwrap());
    let test_ip: IpAddr = "192.168.1.1".parse().unwrap();
    let test_ip2: IpAddr = "192.168.1.2".parse().unwrap();

    assert!(single_proxy.contains(&test_ip));
    assert!(!single_proxy.contains(&test_ip2));

    let cidr_proxy = TrustedProxy::Cidr("192.168.1.0/24".parse().unwrap());
    assert!(cidr_proxy.contains(&test_ip));
    assert!(cidr_proxy.contains(&test_ip2));

    let test_ip3: IpAddr = "192.168.2.1".parse().unwrap();
    assert!(!cidr_proxy.contains(&test_ip3));
}

#[test]
fn test_private_network_check() {
    let config = TrustedProxyConfig::new(
        Vec::new(),
        ValidationMode::Lenient,
        true,
        10,
        true,
        vec!["10.0.0.0/8".parse().unwrap(), "192.168.0.0/16".parse().unwrap()],
    );

    let private_ip: IpAddr = "10.0.1.1".parse().unwrap();
    let private_ip2: IpAddr = "192.168.1.1".parse().unwrap();
    let public_ip: IpAddr = "8.8.8.8".parse().unwrap();

    assert!(config.is_private_network(&private_ip));
    assert!(config.is_private_network(&private_ip2));
    assert!(!config.is_private_network(&public_ip));
}

#[test]
fn test_default_values() {
    assert_eq!(
        DEFAULT_TRUSTED_PROXY_PROXIES,
        "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8"
    );
}
