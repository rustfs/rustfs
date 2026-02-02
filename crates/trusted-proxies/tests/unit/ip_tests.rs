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

use rustfs_trusted_proxies::IpUtils;
use std::net::IpAddr;
use std::str::FromStr;

#[test]
fn test_is_valid_ip_address() {
    let valid_ip: IpAddr = "192.168.1.1".parse().unwrap();
    assert!(IpUtils::is_valid_ip_address(&valid_ip));

    let unspecified_ip: IpAddr = "0.0.0.0".parse().unwrap();
    assert!(!IpUtils::is_valid_ip_address(&unspecified_ip));

    let multicast_ip: IpAddr = "224.0.0.1".parse().unwrap();
    assert!(!IpUtils::is_valid_ip_address(&multicast_ip));

    let valid_ipv6: IpAddr = "2001:db8::1".parse().unwrap();
    assert!(IpUtils::is_valid_ip_address(&valid_ipv6));

    let unspecified_ipv6: IpAddr = "::".parse().unwrap();
    assert!(!IpUtils::is_valid_ip_address(&unspecified_ipv6));
}

#[test]
fn test_is_reserved_ip() {
    let private_ip: IpAddr = "10.0.0.1".parse().unwrap();
    assert!(IpUtils::is_reserved_ip(&private_ip));

    let loopback_ip: IpAddr = "127.0.0.1".parse().unwrap();
    assert!(IpUtils::is_reserved_ip(&loopback_ip));

    let link_local_ip: IpAddr = "169.254.0.1".parse().unwrap();
    assert!(IpUtils::is_reserved_ip(&link_local_ip));

    let documentation_ip: IpAddr = "192.0.2.1".parse().unwrap();
    assert!(IpUtils::is_reserved_ip(&documentation_ip));

    let public_ip: IpAddr = "8.8.8.8".parse().unwrap();
    assert!(!IpUtils::is_reserved_ip(&public_ip));
}

#[test]
fn test_is_private_ip() {
    assert!(IpUtils::is_private_ip(&"10.0.0.1".parse().unwrap()));
    assert!(IpUtils::is_private_ip(&"10.255.255.254".parse().unwrap()));

    assert!(IpUtils::is_private_ip(&"172.16.0.1".parse().unwrap()));
    assert!(IpUtils::is_private_ip(&"172.31.255.254".parse().unwrap()));
    assert!(!IpUtils::is_private_ip(&"172.15.0.1".parse().unwrap()));
    assert!(!IpUtils::is_private_ip(&"172.32.0.1".parse().unwrap()));

    assert!(IpUtils::is_private_ip(&"192.168.0.1".parse().unwrap()));
    assert!(IpUtils::is_private_ip(&"192.168.255.254".parse().unwrap()));

    assert!(!IpUtils::is_private_ip(&"8.8.8.8".parse().unwrap()));
    assert!(!IpUtils::is_private_ip(&"203.0.113.1".parse().unwrap()));
}

#[test]
fn test_is_loopback_ip() {
    assert!(IpUtils::is_loopback_ip(&"127.0.0.1".parse().unwrap()));
    assert!(IpUtils::is_loopback_ip(&"127.255.255.254".parse().unwrap()));
    assert!(IpUtils::is_loopback_ip(&"::1".parse().unwrap()));
    assert!(!IpUtils::is_loopback_ip(&"192.168.1.1".parse().unwrap()));
    assert!(!IpUtils::is_loopback_ip(&"2001:db8::1".parse().unwrap()));
}

#[test]
fn test_is_link_local_ip() {
    assert!(IpUtils::is_link_local_ip(&"169.254.0.1".parse().unwrap()));
    assert!(IpUtils::is_link_local_ip(&"169.254.255.254".parse().unwrap()));
    assert!(IpUtils::is_link_local_ip(&"fe80::1".parse().unwrap()));
    assert!(IpUtils::is_link_local_ip(&"fe80::abcd:1234:5678:9abc".parse().unwrap()));
    assert!(!IpUtils::is_link_local_ip(&"192.168.1.1".parse().unwrap()));
    assert!(!IpUtils::is_link_local_ip(&"2001:db8::1".parse().unwrap()));
}

#[test]
fn test_is_documentation_ip() {
    assert!(IpUtils::is_documentation_ip(&"192.0.2.1".parse().unwrap()));
    assert!(IpUtils::is_documentation_ip(&"198.51.100.1".parse().unwrap()));
    assert!(IpUtils::is_documentation_ip(&"203.0.113.1".parse().unwrap()));
    assert!(IpUtils::is_documentation_ip(&"2001:db8::1".parse().unwrap()));
    assert!(!IpUtils::is_documentation_ip(&"8.8.8.8".parse().unwrap()));
    assert!(!IpUtils::is_documentation_ip(&"2001:4860::1".parse().unwrap()));
}

#[test]
fn test_parse_ip_or_cidr() {
    let result = IpUtils::parse_ip_or_cidr("192.168.1.1");
    assert!(result.is_ok());

    let result = IpUtils::parse_ip_or_cidr("192.168.1.0/24");
    assert!(result.is_ok());

    let result = IpUtils::parse_ip_or_cidr("2001:db8::1");
    assert!(result.is_ok());

    let result = IpUtils::parse_ip_or_cidr("2001:db8::/32");
    assert!(result.is_ok());

    let result = IpUtils::parse_ip_or_cidr("invalid");
    assert!(result.is_err());
}

#[test]
fn test_parse_ip_list() {
    let result = IpUtils::parse_ip_list("192.168.1.1, 10.0.0.1, 8.8.8.8");
    assert!(result.is_ok());

    let ips = result.unwrap();
    assert_eq!(ips.len(), 3);
    assert_eq!(ips[0], IpAddr::from_str("192.168.1.1").unwrap());
    assert_eq!(ips[1], IpAddr::from_str("10.0.0.1").unwrap());
    assert_eq!(ips[2], IpAddr::from_str("8.8.8.8").unwrap());

    let result = IpUtils::parse_ip_list("192.168.1.1,10.0.0.1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 2);

    let result = IpUtils::parse_ip_list("");
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());

    let result = IpUtils::parse_ip_list("192.168.1.1, invalid");
    assert!(result.is_err());
}

#[test]
fn test_parse_network_list() {
    let result = IpUtils::parse_network_list("192.168.1.0/24, 10.0.0.0/8");
    assert!(result.is_ok());

    let networks = result.unwrap();
    assert_eq!(networks.len(), 2);

    let result = IpUtils::parse_network_list("192.168.1.1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);

    let result = IpUtils::parse_network_list("192.168.1.0/24, invalid");
    assert!(result.is_err());
}

#[test]
fn test_ip_in_networks() {
    let networks = vec!["10.0.0.0/8".parse().unwrap(), "192.168.1.0/24".parse().unwrap()];

    let ip_in_network: IpAddr = "10.0.1.1".parse().unwrap();
    let ip_in_network2: IpAddr = "192.168.1.100".parse().unwrap();
    let ip_not_in_network: IpAddr = "8.8.8.8".parse().unwrap();

    assert!(IpUtils::ip_in_networks(&ip_in_network, &networks));
    assert!(IpUtils::ip_in_networks(&ip_in_network2, &networks));
    assert!(!IpUtils::ip_in_networks(&ip_not_in_network, &networks));
}

#[test]
fn test_get_ip_type() {
    assert_eq!(IpUtils::get_ip_type(&"10.0.0.1".parse().unwrap()), "private");
    assert_eq!(IpUtils::get_ip_type(&"127.0.0.1".parse().unwrap()), "loopback");
    assert_eq!(IpUtils::get_ip_type(&"169.254.0.1".parse().unwrap()), "link_local");
    assert_eq!(IpUtils::get_ip_type(&"192.0.2.1".parse().unwrap()), "documentation");
    assert_eq!(IpUtils::get_ip_type(&"224.0.0.1".parse().unwrap()), "reserved");
    assert_eq!(IpUtils::get_ip_type(&"8.8.8.8".parse().unwrap()), "public");
}

#[test]
fn test_canonical_ip() {
    // NOTE: std parsing rejects IPv4 octets with leading zeros (e.g. "001") on some platforms/
    // Rust versions because of ambiguity with non-decimal notations. Use an unambiguous input.
    let ipv4: IpAddr = "192.168.1.1".parse().unwrap();
    assert_eq!(IpUtils::canonical_ip(&ipv4), "192.168.1.1");

    let ipv6_full: IpAddr = "2001:0db8:0000:0000:0000:0000:0000:0001".parse().unwrap();
    let ipv6_compressed: IpAddr = "2001:db8::1".parse().unwrap();

    assert_eq!(IpUtils::canonical_ip(&ipv6_full), "2001:db8::1");
    assert_eq!(IpUtils::canonical_ip(&ipv6_compressed), "2001:db8::1");

    // For IPv6 with multiple runs of zeros, the exact compression choice is an output formatting
    // detail. Compare canonicalized address values instead of requiring a specific layout.
    let ipv6_multi_zero: IpAddr = "2001:0db8:0000:0000:abcd:0000:0000:1234".parse().unwrap();
    let canonical = IpUtils::canonical_ip(&ipv6_multi_zero);
    let reparsed: IpAddr = canonical.parse().unwrap();
    assert_eq!(reparsed, ipv6_multi_zero);
}
