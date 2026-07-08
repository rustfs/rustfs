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
use rustfs_trusted_proxies::{
    CacheConfig, ClientInfo, ProxyChainAnalyzer, ProxyValidator, TrustedProxy, TrustedProxyConfig, ValidationMode,
};
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

// A client-supplied leftmost `Forwarded` element must not be treated as the
// real IP: the proxy-appended node further right must win via chain analysis.
#[test]
fn test_forwarded_resolves_proxy_appended_client_not_spoofed() {
    let validator = ProxyValidator::new(create_test_config(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("10.0.1.5").unwrap(), 1234));

    let mut headers = HeaderMap::new();
    headers.insert("forwarded", "for=1.2.3.4, for=198.51.100.7".parse().unwrap());

    let info = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(info.is_from_trusted_proxy);
    assert_eq!(info.real_ip, IpAddr::from_str("198.51.100.7").unwrap());
    assert_ne!(info.real_ip, IpAddr::from_str("1.2.3.4").unwrap());
}

// A multi-element `Forwarded` header whose rightmost node is trusted validates,
// resolving the client to the first untrusted (leftmost) element.
#[test]
fn test_forwarded_multi_element_trusted_rightmost_validates() {
    let validator = ProxyValidator::new(create_test_config(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("192.168.1.100").unwrap(), 1234));

    let mut headers = HeaderMap::new();
    headers.insert("forwarded", "for=203.0.113.9, for=10.0.0.8".parse().unwrap());

    let info = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(info.is_from_trusted_proxy);
    assert_eq!(info.real_ip, IpAddr::from_str("203.0.113.9").unwrap());
    assert_eq!(info.proxy_hops, 2);
}

// A `proto=https` injected by the client in an earlier element must not set the
// forwarded protocol; only the trusted proxy-appended node is authoritative.
#[test]
fn test_forwarded_client_proto_injection_ignored() {
    let validator = ProxyValidator::new(create_test_config(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("10.0.1.5").unwrap(), 1234));

    let mut headers = HeaderMap::new();
    headers.insert("forwarded", "for=1.2.3.4;proto=https, for=198.51.100.7".parse().unwrap());

    let info = validator.validate_request(peer_addr, &headers).unwrap();
    assert_eq!(info.real_ip, IpAddr::from_str("198.51.100.7").unwrap());
    assert_eq!(info.forwarded_proto, None);
}

// Bare IPv6, bracketed IPv6 with a port, and IPv4 with a port must all parse.
#[test]
fn test_parse_x_forwarded_for_ipv6_and_ports() {
    assert_eq!(
        ProxyValidator::parse_x_forwarded_for("2001:db8::1"),
        vec![IpAddr::from_str("2001:db8::1").unwrap()]
    );
    assert_eq!(
        ProxyValidator::parse_x_forwarded_for("[2001:db8::1]:443"),
        vec![IpAddr::from_str("2001:db8::1").unwrap()]
    );
    assert_eq!(
        ProxyValidator::parse_x_forwarded_for("203.0.113.1:8080"),
        vec![IpAddr::from_str("203.0.113.1").unwrap()]
    );
}

// An IPv6 client behind a trusted IPv6 proxy must resolve to the client, not
// collapse to the proxy because of bare-IPv6 truncation.
#[test]
fn test_ipv6_client_behind_trusted_proxy_resolves_client() {
    let config = TrustedProxyConfig::new(
        vec![TrustedProxy::Cidr("fd00::/8".parse().unwrap())],
        ValidationMode::HopByHop,
        true,
        5,
        true,
        vec![],
    );
    let validator = ProxyValidator::new(config, None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("fd00::5").unwrap(), 1234));

    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", "2001:db8::1234".parse().unwrap());

    let info = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(info.is_from_trusted_proxy);
    assert_eq!(info.real_ip, IpAddr::from_str("2001:db8::1234").unwrap());
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

#[test]
fn test_validator_caches_trusted_direct_peer_decision() {
    let validator = ProxyValidator::with_cache_config(create_test_config(), CacheConfig::default(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("192.168.1.100").unwrap(), 8080));
    let headers = HeaderMap::new();

    assert_eq!(validator.cache_stats().size, 0);

    let first = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(first.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 1);

    let second = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(second.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 1);
}

#[test]
fn test_validator_caches_untrusted_direct_peer_decision() {
    let validator = ProxyValidator::with_cache_config(create_test_config(), CacheConfig::default(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from_str("203.0.113.8").unwrap(), 8080));
    let headers = HeaderMap::new();

    let first = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(!first.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 0);

    let second = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(!second.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 0);
}

#[test]
fn test_validator_skips_cache_when_peer_addr_is_missing() {
    let validator = ProxyValidator::with_cache_config(create_test_config(), CacheConfig::default(), None);
    let headers = HeaderMap::new();

    let client_info = validator.validate_request(None, &headers).unwrap();
    assert!(!client_info.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 0);
}

#[test]
fn test_validator_skips_cache_for_unspecified_peer_addr() {
    let validator = ProxyValidator::with_cache_config(create_test_config(), CacheConfig::default(), None);
    let peer_addr = Some(SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 0));
    let headers = HeaderMap::new();

    let client_info = validator.validate_request(peer_addr, &headers).unwrap();
    assert!(!client_info.is_from_trusted_proxy);
    assert_eq!(validator.cache_stats().size, 0);
}
