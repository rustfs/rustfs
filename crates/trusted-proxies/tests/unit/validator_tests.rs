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

use axum::http::HeaderMap;
use rustfs_trusted_proxies::{ClientInfo, ProxyChainAnalyzer, ProxyValidator, TrustedProxy, TrustedProxyConfig, ValidationMode};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

fn create_test_config() -> TrustedProxyConfig {
    let proxies = vec![
        TrustedProxy::Single("192.168.1.100".parse().unwrap()),
        TrustedProxy::Cidr("10.0.0.0/8".parse().unwrap()),
    ];
    TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 5, true, vec![])
}

#[test]
fn test_client_info_direct() {
    let addr = SocketAddr::new(IpAddr::from([192, 168, 1, 1]), 8080);
    let client_info = ClientInfo::direct(addr);
    assert_eq!(client_info.real_ip, IpAddr::from([192, 168, 1, 1]));
}

#[test]
fn test_parse_x_forwarded_for() {
    let header_value = "203.0.113.195, 198.51.100.1";
    let result = ProxyValidator::parse_x_forwarded_for(header_value);
    assert_eq!(result.len(), 2);
}

#[test]
fn test_proxy_chain_analyzer_hop_by_hop() {
    let config = create_test_config();
    let analyzer = ProxyChainAnalyzer::new(config);
    let chain = vec![
        IpAddr::from_str("203.0.113.195").unwrap(),
        IpAddr::from_str("10.0.1.100").unwrap(),
    ];
    let current_proxy = IpAddr::from_str("192.168.1.100").unwrap();
    let headers = HeaderMap::new();
    let result = analyzer.analyze_chain(&chain, current_proxy, &headers);
    assert!(result.is_ok());
}

#[test]
fn test_proxy_chain_too_long() {
    let config = create_test_config();
    let analyzer = ProxyChainAnalyzer::new(config);
    let chain = vec![
        IpAddr::from_str("203.0.113.195").unwrap(),
        IpAddr::from_str("10.0.1.100").unwrap(),
        IpAddr::from_str("10.0.1.101").unwrap(),
        IpAddr::from_str("10.0.1.102").unwrap(),
        IpAddr::from_str("10.0.1.103").unwrap(),
    ];
    let current_proxy = IpAddr::from_str("192.168.1.100").unwrap();
    let headers = HeaderMap::new();
    // Total chain length is 6 (5 in chain + 1 current proxy), max hops is 5
    let result = analyzer.analyze_chain(&chain, current_proxy, &headers);
    assert!(result.is_err());
    match result {
        Err(rustfs_trusted_proxies::ProxyError::ChainTooLong(len, max)) => {
            assert_eq!(len, 6);
            assert_eq!(max, 5);
        }
        _ => panic!("Expected ChainTooLong error"),
    }
}
