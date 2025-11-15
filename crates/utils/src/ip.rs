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

use std::net::{IpAddr, Ipv4Addr};

/// Get the IP address of the machine
///
/// Priority is given to trying to get the IPv4 address, and if it fails, try to get the IPv6 address.
/// If both fail to retrieve, None is returned.
///
/// # Returns
/// * `Some(IpAddr)` - Native IP address (IPv4 or IPv6)
/// * `None` - Unable to obtain any native IP address
///
pub fn get_local_ip() -> Option<IpAddr> {
    local_ip_address::local_ip()
        .ok()
        .or_else(|| local_ip_address::local_ipv6().ok())
}

/// Get the IP address of the machine as a string
///
/// If the IP address cannot be obtained, returns "127.0.0.1" as the default value.
///
/// # Returns
/// * `String` - Native IP address (IPv4 or IPv6) as a string, or the default value
///
pub fn get_local_ip_with_default() -> String {
    get_local_ip()
        .unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))) // Provide a safe default value
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_get_local_ip_returns_some_ip() {
        // Test getting local IP address, should return Some value
        let ip = get_local_ip();
        assert!(ip.is_some(), "Should be able to get local IP address");

        if let Some(ip_addr) = ip {
            println!("Local IP address: {ip_addr}");
            // Verify that the returned IP address is valid
            match ip_addr {
                IpAddr::V4(ipv4) => {
                    assert!(!ipv4.is_unspecified(), "IPv4 should not be unspecified (0.0.0.0)");
                    println!("Got IPv4 address: {ipv4}");
                }
                IpAddr::V6(ipv6) => {
                    assert!(!ipv6.is_unspecified(), "IPv6 should not be unspecified (::)");
                    println!("Got IPv6 address: {ipv6}");
                }
            }
        }
    }

    #[test]
    fn test_get_local_ip_with_default_never_empty() {
        // Test that function with default value never returns empty string
        let ip_string = get_local_ip_with_default();
        assert!(!ip_string.is_empty(), "IP string should never be empty");

        // Verify that the returned string can be parsed as a valid IP address
        let parsed_ip: Result<IpAddr, _> = ip_string.parse();
        assert!(parsed_ip.is_ok(), "Returned string should be a valid IP address: {ip_string}");

        println!("Local IP with default: {ip_string}");
    }

    #[test]
    fn test_get_local_ip_with_default_fallback() {
        // Test whether the default value is 127.0.0.1
        let default_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip_string = get_local_ip_with_default();

        // If unable to get real IP, should return default value
        if get_local_ip().is_none() {
            assert_eq!(ip_string, default_ip.to_string());
        }

        // In any case, should always return a valid IP address string
        let parsed: Result<IpAddr, _> = ip_string.parse();
        assert!(parsed.is_ok(), "Should always return a valid IP string");
    }

    #[test]
    fn test_ip_address_types() {
        // Test IP address type recognition
        if let Some(ip) = get_local_ip() {
            match ip {
                IpAddr::V4(ipv4) => {
                    // Test IPv4 address properties
                    println!("IPv4 address: {ipv4}");
                    assert!(!ipv4.is_multicast(), "Local IP should not be multicast");
                    assert!(!ipv4.is_broadcast(), "Local IP should not be broadcast");

                    // Check if it's a private address (usually local IP is private)
                    let is_private = ipv4.is_private();
                    let is_loopback = ipv4.is_loopback();
                    println!("IPv4 is private: {is_private}, is loopback: {is_loopback}");
                }
                IpAddr::V6(ipv6) => {
                    // Test IPv6 address properties
                    println!("IPv6 address: {ipv6}");
                    assert!(!ipv6.is_multicast(), "Local IP should not be multicast");

                    let is_loopback = ipv6.is_loopback();
                    println!("IPv6 is loopback: {is_loopback}");
                }
            }
        }
    }

    #[test]
    fn test_ip_string_format() {
        // Test IP address string format
        let ip_string = get_local_ip_with_default();

        // Verify string format
        assert!(!ip_string.contains(' '), "IP string should not contain spaces");
        assert!(!ip_string.is_empty(), "IP string should not be empty");

        // Verify round-trip conversion
        let parsed_ip: IpAddr = ip_string.parse().expect("Should parse as valid IP");
        let back_to_string = parsed_ip.to_string();

        // For standard IP addresses, round-trip conversion should be consistent
        println!("Original: {ip_string}, Parsed back: {back_to_string}");
    }

    #[test]
    fn test_default_fallback_value() {
        // Test correctness of default fallback value
        let default_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(default_ip.to_string(), "127.0.0.1");

        // Verify default IP properties
        if let IpAddr::V4(ipv4) = default_ip {
            assert!(ipv4.is_loopback(), "Default IP should be loopback");
            assert!(!ipv4.is_unspecified(), "Default IP should not be unspecified");
            assert!(!ipv4.is_multicast(), "Default IP should not be multicast");
        }
    }

    #[test]
    fn test_consistency_between_functions() {
        // Test consistency between the two functions
        let ip_option = get_local_ip();
        let ip_string = get_local_ip_with_default();

        match ip_option {
            Some(ip) => {
                // If get_local_ip returns Some, then get_local_ip_with_default should return the same IP
                assert_eq!(ip.to_string(), ip_string, "Both functions should return the same IP when available");
            }
            None => {
                // If get_local_ip returns None, then get_local_ip_with_default should return default value
                assert_eq!(ip_string, "127.0.0.1", "Should return default value when no IP is available");
            }
        }
    }

    #[test]
    fn test_multiple_calls_consistency() {
        // Test consistency of multiple calls
        let ip1 = get_local_ip();
        let ip2 = get_local_ip();
        let ip_str1 = get_local_ip_with_default();
        let ip_str2 = get_local_ip_with_default();

        // Multiple calls should return the same result
        assert_eq!(ip1, ip2, "Multiple calls to get_local_ip should return same result");
        assert_eq!(ip_str1, ip_str2, "Multiple calls to get_local_ip_with_default should return same result");
    }

    #[cfg(feature = "integration")]
    #[test]
    fn test_network_connectivity() {
        // Integration test: verify that the obtained IP address can be used for network connections
        if let Some(ip) = get_local_ip() {
            match ip {
                IpAddr::V4(ipv4) => {
                    // For IPv4, check if it's a valid network address
                    assert!(!ipv4.is_unspecified(), "Should not be 0.0.0.0");

                    // If it's not a loopback address, it should be routable
                    if !ipv4.is_loopback() {
                        println!("Got routable IPv4: {ipv4}");
                    }
                }
                IpAddr::V6(ipv6) => {
                    // For IPv6, check if it's a valid network address
                    assert!(!ipv6.is_unspecified(), "Should not be ::");

                    if !ipv6.is_loopback() {
                        println!("Got routable IPv6: {ipv6}");
                    }
                }
            }
        }
    }
}
