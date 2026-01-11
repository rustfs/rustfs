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

//! Proxy validator unit tests

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;

    use axum::http::HeaderMap;

    use crate::config::{TrustedProxy, TrustedProxyConfig, ValidationMode};
    use crate::proxy::chain::ProxyChainAnalyzer;
    use crate::proxy::validator::{ClientInfo, ProxyValidator};

    fn create_test_config() -> TrustedProxyConfig {
        let proxies = vec![
            TrustedProxy::Single("192.168.1.100".parse().unwrap()),
            TrustedProxy::Cidr("10.0.0.0/8".parse().unwrap()),
            TrustedProxy::Cidr("172.16.0.0/12".parse().unwrap()),
        ];

        TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 5, true, vec![])
    }

    #[test]
    fn test_client_info_direct() {
        let addr = SocketAddr::new(IpAddr::from([192, 168, 1, 1]), 8080);
        let client_info = ClientInfo::direct(addr);

        assert_eq!(client_info.real_ip, IpAddr::from([192, 168, 1, 1]));
        assert!(client_info.forwarded_host.is_none());
        assert!(client_info.forwarded_proto.is_none());
        assert!(!client_info.is_from_trusted_proxy);
        assert!(client_info.proxy_ip.is_none());
        assert_eq!(client_info.proxy_hops, 0);
        assert_eq!(client_info.validation_mode, ValidationMode::Lenient);
        assert!(client_info.warnings.is_empty());
    }

    #[test]
    fn test_parse_x_forwarded_for() {
        use crate::proxy::validator::ProxyValidator;

        // 测试有效的 X-Forwarded-For 头部
        let header_value = "203.0.113.195, 198.51.100.1, 10.0.1.100";
        let result = ProxyValidator::parse_x_forwarded_for(header_value);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], IpAddr::from_str("203.0.113.195").unwrap());
        assert_eq!(result[1], IpAddr::from_str("198.51.100.1").unwrap());
        assert_eq!(result[2], IpAddr::from_str("10.0.1.100").unwrap());

        // 测试带端口的 IP
        let header_value_with_ports = "203.0.113.195:8080, 198.51.100.1:443";
        let result = ProxyValidator::parse_x_forwarded_for(header_value_with_ports);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], IpAddr::from_str("203.0.113.195").unwrap());
        assert_eq!(result[1], IpAddr::from_str("198.51.100.1").unwrap());

        // 测试空值
        let empty_result = ProxyValidator::parse_x_forwarded_for("");
        assert!(empty_result.is_empty());

        // 测试无效 IP
        let invalid_result = ProxyValidator::parse_x_forwarded_for("invalid, 203.0.113.195");
        assert_eq!(invalid_result.len(), 1); // 无效项被跳过
    }

    #[test]
    fn test_proxy_chain_analyzer_lenient() {
        let config = create_test_config();
        let analyzer = ProxyChainAnalyzer::new(config.clone());

        // 测试链：客户端 -> 可信代理 1 -> 可信代理 2
        let chain = vec![
            IpAddr::from_str("203.0.113.195").unwrap(), // 客户端
            IpAddr::from_str("10.0.1.100").unwrap(),    // 可信代理 1
            IpAddr::from_str("192.168.1.100").unwrap(), // 可信代理 2
        ];

        let current_proxy = IpAddr::from_str("192.168.1.100").unwrap();
        let mut headers = HeaderMap::new();

        let result = analyzer.analyze_chain(&chain, current_proxy, &headers);
        assert!(result.is_ok());

        let analysis = result.unwrap();
        assert_eq!(analysis.client_ip, IpAddr::from_str("203.0.113.195").unwrap());
        assert_eq!(analysis.hops, 3);
        assert!(analysis.is_continuous);
        assert_eq!(analysis.validation_mode, ValidationMode::HopByHop);
    }

    #[test]
    fn test_proxy_chain_analyzer_strict() {
        let mut config = create_test_config();
        config.validation_mode = ValidationMode::Strict;

        let analyzer = ProxyChainAnalyzer::new(config);

        // 测试链：客户端 -> 可信代理 1 -> 可信代理 2 (全部可信)
        let chain = vec![
            IpAddr::from_str("203.0.113.195").unwrap(), // 客户端
            IpAddr::from_str("10.0.1.100").unwrap(),    // 可信代理 1
            IpAddr::from_str("192.168.1.100").unwrap(), // 可信代理 2
        ];

        let current_proxy = IpAddr::from_str("192.168.1.100").unwrap();
        let mut headers = HeaderMap::new();

        let result = analyzer.analyze_chain(&chain, current_proxy, &headers);
        assert!(result.is_ok());

        // 测试链包含不可信代理
        let chain_with_untrusted = vec![
            IpAddr::from_str("203.0.113.195").unwrap(), // 客户端
            IpAddr::from_str("8.8.8.8").unwrap(),       // 不可信代理
            IpAddr::from_str("192.168.1.100").unwrap(), // 可信代理 2
        ];

        let result = analyzer.analyze_chain(&chain_with_untrusted, current_proxy, &headers);
        assert!(result.is_err());
    }

    #[test]
    fn test_proxy_chain_analyzer_hop_by_hop() {
        let config = create_test_config();
        let analyzer = ProxyChainAnalyzer::new(config);

        // 测试链：客户端 -> 不可信代理 -> 可信代理 1 -> 可信代理 2
        let chain = vec![
            IpAddr::from_str("203.0.113.195").unwrap(), // 客户端
            IpAddr::from_str("8.8.8.8").unwrap(),       // 不可信代理
            IpAddr::from_str("10.0.1.100").unwrap(),    // 可信代理 1
            IpAddr::from_str("192.168.1.100").unwrap(), // 可信代理 2
        ];

        let current_proxy = IpAddr::from_str("192.168.1.100").unwrap();
        let mut headers = HeaderMap::new();

        let result = analyzer.analyze_chain(&chain, current_proxy, &headers);
        assert!(result.is_ok());

        let analysis = result.unwrap();
        // 应该找到客户端 IP (203.0.113.195)
        assert_eq!(analysis.client_ip, IpAddr::from_str("203.0.113.195").unwrap());
        // 应该验证 2 跳 (10.0.1.100 和 192.168.1.100)
        assert_eq!(analysis.hops, 2);
    }

    #[test]
    fn test_chain_continuity_check() {
        let config = create_test_config();
        let analyzer = ProxyChainAnalyzer::new(config);

        // 测试连续链
        let full_chain = vec![
            IpAddr::from_str("203.0.113.195").unwrap(),
            IpAddr::from_str("10.0.1.100").unwrap(),
            IpAddr::from_str("192.168.1.100").unwrap(),
        ];

        let trusted_chain = vec![
            IpAddr::from_str("10.0.1.100").unwrap(),
            IpAddr::from_str("192.168.1.100").unwrap(),
        ];

        assert!(analyzer.check_chain_continuity(&full_chain, &trusted_chain));

        // 测试不连续链
        let bad_trusted_chain = vec![IpAddr::from_str("192.168.1.100").unwrap()];

        assert!(!analyzer.check_chain_continuity(&full_chain, &bad_trusted_chain));
    }

    #[test]
    fn test_validate_ip_addresses() {
        let config = create_test_config();
        let analyzer = ProxyChainAnalyzer::new(config);

        // 测试有效 IP
        let valid_chain = vec![
            IpAddr::from_str("203.0.113.195").unwrap(),
            IpAddr::from_str("10.0.1.100").unwrap(),
        ];

        let result = analyzer.validate_ip_addresses(&valid_chain);
        assert!(result.is_ok());

        // 测试未指定地址
        let invalid_chain = vec![IpAddr::from_str("0.0.0.0").unwrap()];

        let result = analyzer.validate_ip_addresses(&invalid_chain);
        assert!(result.is_err());

        // 测试多播地址
        let multicast_chain = vec![IpAddr::from_str("224.0.0.1").unwrap()];

        let result = analyzer.validate_ip_addresses(&multicast_chain);
        assert!(result.is_err());
    }

    #[test]
    fn test_proxy_validator_creation() {
        let config = create_test_config();
        let validator = ProxyValidator::new(config, None);

        // 验证器应该成功创建
        assert!(true); // 如果没有 panic，测试通过
    }
}
