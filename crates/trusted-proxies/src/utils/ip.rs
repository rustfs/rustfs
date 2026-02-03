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

//! IP address utility functions for validation and classification.

use ipnetwork::IpNetwork;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

/// Collection of IP-related utility functions.
pub struct IpUtils;

impl IpUtils {
    /// Checks if an IP address is valid for general use.
    ///
    /// "Valid" here means the address is syntactically valid and not an unspecified or multicast
    /// address. Classification (private/link-local/documentation/reserved) is handled separately.
    pub fn is_valid_ip_address(ip: &IpAddr) -> bool {
        !ip.is_unspecified() && !ip.is_multicast()
    }

    /// Checks if an IP address belongs to a reserved range.
    pub fn is_reserved_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => Self::is_reserved_ipv4(ipv4),
            IpAddr::V6(ipv6) => Self::is_reserved_ipv6(ipv6),
        }
    }

    /// Checks if an IPv4 address belongs to a reserved range.
    pub fn is_reserved_ipv4(ip: &Ipv4Addr) -> bool {
        let octets = ip.octets();

        // Check common reserved IPv4 ranges
        matches!(
            octets,
            [0, _, _, _] |           // 0.0.0.0/8
            [10, _, _, _] |          // 10.0.0.0/8
            [100, 64, _, _] |        // 100.64.0.0/10
            [127, _, _, _] |         // 127.0.0.0/8
            [169, 254, _, _] |       // 169.254.0.0/16
            [172, 16..=31, _, _] |   // 172.16.0.0/12
            [192, 0, 0, _] |         // 192.0.0.0/24
            [192, 0, 2, _] |         // 192.0.2.0/24
            [192, 88, 99, _] |       // 192.88.99.0/24
            [192, 168, _, _] |       // 192.168.0.0/16
            [198, 18..=19, _, _] |   // 198.18.0.0/15
            [198, 51, 100, _] |      // 198.51.100.0/24
            [203, 0, 113, _] |       // 203.0.113.0/24
            [224..=239, _, _, _] |   // 224.0.0.0/4
            [240..=255, _, _, _] // 240.0.0.0/4
        )
    }

    /// Checks if an IPv6 address belongs to a reserved range.
    pub fn is_reserved_ipv6(ip: &Ipv6Addr) -> bool {
        let segments = ip.segments();

        // Check common reserved IPv6 ranges
        matches!(
            segments,
            [0, 0, 0, 0, 0, 0, 0, 0] |                          // ::/128
            [0, 0, 0, 0, 0, 0, 0, 1] |                          // ::1/128
            [0x2001, 0xdb8, _, _, _, _, _, _] |                  // 2001:db8::/32
            [0xfc00..=0xfdff, _, _, _, _, _, _, _] |             // fc00::/7
            [0xfe80..=0xfebf, _, _, _, _, _, _, _] |             // fe80::/10
            [0xff00..=0xffff, _, _, _, _, _, _, _] // ff00::/8
        )
    }

    /// Checks if an IP address is a private address.
    pub fn is_private_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => Self::is_private_ipv4(ipv4),
            IpAddr::V6(ipv6) => Self::is_private_ipv6(ipv6),
        }
    }

    /// Checks if an IPv4 address is a private address.
    pub fn is_private_ipv4(ip: &Ipv4Addr) -> bool {
        let octets = ip.octets();

        matches!(
            octets,
            [10, _, _, _] |          // 10.0.0.0/8
            [172, 16..=31, _, _] |   // 172.16.0.0/12
            [192, 168, _, _] // 192.168.0.0/16
        )
    }

    /// Checks if an IPv6 address is a private address.
    pub fn is_private_ipv6(ip: &Ipv6Addr) -> bool {
        let segments = ip.segments();

        matches!(
            segments,
            [0xfc00..=0xfdff, _, _, _, _, _, _, _] // fc00::/7
        )
    }

    /// Checks if an IP address is a loopback address.
    pub fn is_loopback_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => ipv4.is_loopback(),
            IpAddr::V6(ipv6) => ipv6.is_loopback(),
        }
    }

    /// Checks if an IP address is a link-local address.
    pub fn is_link_local_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => ipv4.is_link_local(),
            IpAddr::V6(ipv6) => ipv6.is_unicast_link_local(),
        }
    }

    /// Checks if an IP address is a documentation address (TEST-NET).
    pub fn is_documentation_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => {
                let octets = ipv4.octets();
                matches!(
                    octets,
                    [192, 0, 2, _] |      // 192.0.2.0/24
                    [198, 51, 100, _] |   // 198.51.100.0/24
                    [203, 0, 113, _] // 203.0.113.0/24
                )
            }
            IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                matches!(segments, [0x2001, 0xdb8, _, _, _, _, _, _]) // 2001:db8::/32
            }
        }
    }

    /// Parses an IP address or CIDR range from a string.
    pub fn parse_ip_or_cidr(s: &str) -> Result<IpNetwork, String> {
        IpNetwork::from_str(s).map_err(|e| format!("Failed to parse IP/CIDR '{}': {}", s, e))
    }

    /// Parses a comma-separated list of IP addresses.
    pub fn parse_ip_list(s: &str) -> Result<Vec<IpAddr>, String> {
        let mut ips = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            match IpAddr::from_str(part) {
                Ok(ip) => ips.push(ip),
                Err(e) => return Err(format!("Failed to parse IP '{}': {}", part, e)),
            }
        }

        Ok(ips)
    }

    /// Parses a comma-separated list of IP networks (CIDR).
    pub fn parse_network_list(s: &str) -> Result<Vec<IpNetwork>, String> {
        let mut networks = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            match Self::parse_ip_or_cidr(part) {
                Ok(network) => networks.push(network),
                Err(e) => return Err(e),
            }
        }

        Ok(networks)
    }

    /// Checks if an IP address is contained within any of the given networks.
    pub fn ip_in_networks(ip: &IpAddr, networks: &[IpNetwork]) -> bool {
        networks.iter().any(|network| network.contains(*ip))
    }

    /// Returns a string description of the IP address type.
    pub fn get_ip_type(ip: &IpAddr) -> &'static str {
        if Self::is_private_ip(ip) {
            "private"
        } else if Self::is_loopback_ip(ip) {
            "loopback"
        } else if Self::is_link_local_ip(ip) {
            "link_local"
        } else if Self::is_documentation_ip(ip) {
            "documentation"
        } else if Self::is_reserved_ip(ip) {
            "reserved"
        } else {
            "public"
        }
    }

    /// Returns the canonical string representation of an IP address.
    pub fn canonical_ip(ip: &IpAddr) -> String {
        match ip {
            IpAddr::V4(ipv4) => ipv4.to_string(),
            IpAddr::V6(ipv6) => {
                // Use the standard library's Display implementation for canonical representation
                ipv6.to_string()
            }
        }
    }
}

/// Checks if an IP address is valid for general use.
///
/// "Valid" here means the address is syntactically valid and not an unspecified or multicast
/// address. Classification (private/link-local/documentation/reserved) is handled separately.
pub fn is_valid_ip_address(ip: &IpAddr) -> bool {
    !ip.is_unspecified() && !ip.is_multicast()
}
