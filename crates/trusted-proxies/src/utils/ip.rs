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

//! IP address utility functions

use ipnetwork::IpNetwork;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

/// IP 工具函数集合
pub struct IpUtils;

impl IpUtils {
    /// 检查 IP 地址是否有效
    pub fn is_valid_ip_address(ip: &IpAddr) -> bool {
        !ip.is_unspecified() && !ip.is_multicast() && !Self::is_reserved_ip(ip)
    }

    /// 检查 IP 是否为保留地址
    pub fn is_reserved_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => Self::is_reserved_ipv4(ipv4),
            IpAddr::V6(ipv6) => Self::is_reserved_ipv6(ipv6),
        }
    }

    /// 检查 IPv4 是否为保留地址
    pub fn is_reserved_ipv4(ip: &Ipv4Addr) -> bool {
        let octets = ip.octets();

        // 检查常见的保留地址范围
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

    /// 检查 IPv6 是否为保留地址
    pub fn is_reserved_ipv6(ip: &Ipv6Addr) -> bool {
        let segments = ip.segments();

        // 检查常见的保留地址范围
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

    /// 检查 IP 是否为私有地址
    pub fn is_private_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => Self::is_private_ipv4(ipv4),
            IpAddr::V6(ipv6) => Self::is_private_ipv6(ipv6),
        }
    }

    /// 检查 IPv4 是否为私有地址
    pub fn is_private_ipv4(ip: &Ipv4Addr) -> bool {
        let octets = ip.octets();

        matches!(
            octets,
            [10, _, _, _] |          // 10.0.0.0/8
            [172, 16..=31, _, _] |   // 172.16.0.0/12
            [192, 168, _, _] // 192.168.0.0/16
        )
    }

    /// 检查 IPv6 是否为私有地址
    pub fn is_private_ipv6(ip: &Ipv6Addr) -> bool {
        let segments = ip.segments();

        matches!(
            segments,
            [0xfc00..=0xfdff, _, _, _, _, _, _, _] // fc00::/7
        )
    }

    /// 检查 IP 是否为回环地址
    pub fn is_loopback_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => ipv4.is_loopback(),
            IpAddr::V6(ipv6) => ipv6.is_loopback(),
        }
    }

    /// 检查 IP 是否为链路本地地址
    pub fn is_link_local_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => ipv4.is_link_local(),
            IpAddr::V6(ipv6) => ipv6.is_unicast_link_local(),
        }
    }

    /// 检查 IP 是否为文档地址（TEST-NET）
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

    /// 从字符串解析 IP 地址，支持 CIDR 表示法
    pub fn parse_ip_or_cidr(s: &str) -> Result<IpNetwork, String> {
        IpNetwork::from_str(s).map_err(|e| format!("Failed to parse IP/CIDR '{}': {}", s, e))
    }

    /// 从逗号分隔的字符串解析 IP 列表
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

    /// 从逗号分隔的字符串解析网络列表
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

    /// 检查 IP 是否在给定的网络列表中
    pub fn ip_in_networks(ip: &IpAddr, networks: &[IpNetwork]) -> bool {
        networks.iter().any(|network| network.contains(*ip))
    }

    /// 获取 IP 地址的类型描述
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

    /// 将 IP 地址转换为规范形式
    pub fn canonical_ip(ip: &IpAddr) -> String {
        match ip {
            IpAddr::V4(ipv4) => ipv4.to_string(),
            IpAddr::V6(ipv6) => {
                // 压缩 IPv6 地址
                let mut result = String::new();
                let segments = ipv6.segments();

                // 查找最长的连续零段
                let mut longest_start = 0;
                let mut longest_len = 0;
                let mut current_start = 0;
                let mut current_len = 0;

                for (i, &segment) in segments.iter().enumerate() {
                    if segment == 0 {
                        if current_len == 0 {
                            current_start = i;
                        }
                        current_len += 1;
                    } else {
                        if current_len > longest_len {
                            longest_start = current_start;
                            longest_len = current_len;
                        }
                        current_len = 0;
                    }
                }

                if current_len > longest_len {
                    longest_start = current_start;
                    longest_len = current_len;
                }

                // 格式化为字符串
                for mut i in 0..8 {
                    if i == longest_start && longest_len > 1 {
                        result.push_str("::");
                        i += longest_len - 1;
                    } else if i == longest_start && longest_len == 1 {
                        result.push('0');
                    } else {
                        if i > 0 && i != longest_start {
                            result.push(':');
                        }
                        if segments[i] != 0 || (i == 7 && result.is_empty()) {
                            result.push_str(&format!("{:x}", segments[i]));
                        }
                    }
                }

                result
            }
        }
    }
}
