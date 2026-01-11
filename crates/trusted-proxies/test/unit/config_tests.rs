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

//! Configuration module unit tests

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use crate::config::env::{DEFAULT_TRUSTED_PROXIES, ENV_TRUSTED_PROXIES};
    use crate::config::{ConfigLoader, TrustedProxy, TrustedProxyConfig, ValidationMode};

    #[test]
    fn test_config_loader_default() {
        // 清理环境变量
        std::env::remove_var(ENV_TRUSTED_PROXIES);

        let config = ConfigLoader::from_env_or_default();

        // 验证默认值
        assert_eq!(config.server_addr.port(), 3000);
        assert!(!config.proxy.proxies.is_empty());
        assert_eq!(config.proxy.validation_mode, ValidationMode::HopByHop);
        assert!(config.proxy.enable_rfc7239);
        assert_eq!(config.proxy.max_hops, 10);
    }

    #[test]
    fn test_config_loader_env_vars() {
        // 设置环境变量
        std::env::set_var(ENV_TRUSTED_PROXIES, "192.168.1.0/24,10.0.0.0/8");
        std::env::set_var("TRUSTED_PROXY_VALIDATION_MODE", "strict");
        std::env::set_var("TRUSTED_PROXY_MAX_HOPS", "5");
        std::env::set_var("SERVER_PORT", "8080");

        let config = ConfigLoader::from_env();

        if let Ok(config) = config {
            assert_eq!(config.server_addr.port(), 8080);
            assert_eq!(config.proxy.validation_mode, ValidationMode::Strict);
            assert_eq!(config.proxy.max_hops, 5);

            // 清理环境变量
            std::env::remove_var(ENV_TRUSTED_PROXIES);
            std::env::remove_var("TRUSTED_PROXY_VALIDATION_MODE");
            std::env::remove_var("TRUSTED_PROXY_MAX_HOPS");
            std::env::remove_var("SERVER_PORT");
        } else {
            panic!("Failed to load config from env");
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

        // 测试 IP 检查
        let test_ip: IpAddr = "192.168.1.1".parse().unwrap();
        let test_socket_addr = std::net::SocketAddr::new(test_ip, 8080);

        assert!(config.is_trusted(&test_socket_addr));

        let test_ip2: IpAddr = "10.0.1.1".parse().unwrap();
        let test_socket_addr2 = std::net::SocketAddr::new(test_ip2, 8080);

        assert!(config.is_trusted(&test_socket_addr2));
    }

    #[test]
    fn test_validation_mode_from_str() {
        assert_eq!(ValidationMode::from_str("lenient").unwrap(), ValidationMode::Lenient);
        assert_eq!(ValidationMode::from_str("strict").unwrap(), ValidationMode::Strict);
        assert_eq!(ValidationMode::from_str("hop_by_hop").unwrap(), ValidationMode::HopByHop);

        // 测试无效值
        assert!(ValidationMode::from_str("invalid").is_err());
    }

    #[test]
    fn test_trusted_proxy_contains() {
        // 测试单个 IP
        let single_proxy = TrustedProxy::Single("192.168.1.1".parse().unwrap());
        let test_ip: IpAddr = "192.168.1.1".parse().unwrap();
        let test_ip2: IpAddr = "192.168.1.2".parse().unwrap();

        assert!(single_proxy.contains(&test_ip));
        assert!(!single_proxy.contains(&test_ip2));

        // 测试 CIDR 范围
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
    fn test_parse_ip_list_from_env() {
        use crate::config::env::parse_ip_list_from_env;

        // 测试有效的 IP 列表
        std::env::set_var("TEST_IP_LIST", "10.0.0.0/8,192.168.1.0/24");

        let result = parse_ip_list_from_env("TEST_IP_LIST", "");
        assert!(result.is_ok());

        let networks = result.unwrap();
        assert_eq!(networks.len(), 2);

        // 测试空值
        std::env::set_var("TEST_IP_LIST_EMPTY", "");
        let result = parse_ip_list_from_env("TEST_IP_LIST_EMPTY", "");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        // 测试无效值
        std::env::set_var("TEST_IP_LIST_INVALID", "invalid,10.0.0.0/8");
        let result = parse_ip_list_from_env("TEST_IP_LIST_INVALID", "");
        assert!(result.is_ok()); // 无效项会被跳过

        // 清理环境变量
        std::env::remove_var("TEST_IP_LIST");
        std::env::remove_var("TEST_IP_LIST_EMPTY");
        std::env::remove_var("TEST_IP_LIST_INVALID");
    }

    #[test]
    fn test_default_values() {
        use crate::config::env::{
            DEFAULT_PROXY_ENABLE_RFC7239, DEFAULT_PROXY_MAX_HOPS, DEFAULT_PROXY_VALIDATION_MODE, DEFAULT_TRUSTED_PROXIES,
        };

        assert_eq!(DEFAULT_TRUSTED_PROXIES, "127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fd00::/8");
        assert_eq!(DEFAULT_PROXY_VALIDATION_MODE, "hop_by_hop");
        assert_eq!(DEFAULT_PROXY_MAX_HOPS, 10);
        assert!(DEFAULT_PROXY_ENABLE_RFC7239);
    }
}
