use std::net::{IpAddr, Ipv4Addr};

/// Get the IP address of the machine
///
/// Priority is given to trying to get the IPv4 address, and if it fails, try to get the IPv6 address.
/// If both fail to retrieve, None is returned.
///
/// # Returns
///
/// * `Some(IpAddr)` - Native IP address (IPv4 or IPv6)
/// * `None` - Unable to obtain any native IP address
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
///
/// * `String` - Native IP address (IPv4 or IPv6) as a string, or the default value
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
        // 测试获取本地IP地址，应该返回Some值
        let ip = get_local_ip();
        assert!(ip.is_some(), "Should be able to get local IP address");

        if let Some(ip_addr) = ip {
            println!("Local IP address: {}", ip_addr);
            // 验证返回的是有效的IP地址
            match ip_addr {
                IpAddr::V4(ipv4) => {
                    assert!(!ipv4.is_unspecified(), "IPv4 should not be unspecified (0.0.0.0)");
                    println!("Got IPv4 address: {}", ipv4);
                }
                IpAddr::V6(ipv6) => {
                    assert!(!ipv6.is_unspecified(), "IPv6 should not be unspecified (::)");
                    println!("Got IPv6 address: {}", ipv6);
                }
            }
        }
    }

    #[test]
    fn test_get_local_ip_with_default_never_empty() {
        // 测试带默认值的函数永远不会返回空字符串
        let ip_string = get_local_ip_with_default();
        assert!(!ip_string.is_empty(), "IP string should never be empty");

        // 验证返回的字符串可以解析为有效的IP地址
        let parsed_ip: Result<IpAddr, _> = ip_string.parse();
        assert!(parsed_ip.is_ok(), "Returned string should be a valid IP address: {}", ip_string);

        println!("Local IP with default: {}", ip_string);
    }

    #[test]
    fn test_get_local_ip_with_default_fallback() {
        // 测试默认值是否为127.0.0.1
        let default_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip_string = get_local_ip_with_default();

        // 如果无法获取真实IP，应该返回默认值
        if get_local_ip().is_none() {
            assert_eq!(ip_string, default_ip.to_string());
        }

        // 无论如何，返回的都应该是有效的IP地址字符串
        let parsed: Result<IpAddr, _> = ip_string.parse();
        assert!(parsed.is_ok(), "Should always return a valid IP string");
    }

    #[test]
    fn test_ip_address_types() {
        // 测试IP地址类型的识别
        if let Some(ip) = get_local_ip() {
            match ip {
                IpAddr::V4(ipv4) => {
                    // 测试IPv4地址的属性
                    println!("IPv4 address: {}", ipv4);
                    assert!(!ipv4.is_multicast(), "Local IP should not be multicast");
                    assert!(!ipv4.is_broadcast(), "Local IP should not be broadcast");

                    // 检查是否为私有地址（通常本地IP是私有的）
                    let is_private = ipv4.is_private();
                    let is_loopback = ipv4.is_loopback();
                    println!("IPv4 is private: {}, is loopback: {}", is_private, is_loopback);
                }
                IpAddr::V6(ipv6) => {
                    // 测试IPv6地址的属性
                    println!("IPv6 address: {}", ipv6);
                    assert!(!ipv6.is_multicast(), "Local IP should not be multicast");

                    let is_loopback = ipv6.is_loopback();
                    println!("IPv6 is loopback: {}", is_loopback);
                }
            }
        }
    }

    #[test]
    fn test_ip_string_format() {
        // 测试IP地址字符串格式
        let ip_string = get_local_ip_with_default();

        // 验证字符串格式
        assert!(!ip_string.contains(' '), "IP string should not contain spaces");
        assert!(!ip_string.is_empty(), "IP string should not be empty");

        // 验证可以往返转换
        let parsed_ip: IpAddr = ip_string.parse().expect("Should parse as valid IP");
        let back_to_string = parsed_ip.to_string();

        // 对于标准IP地址，往返转换应该保持一致
        println!("Original: {}, Parsed back: {}", ip_string, back_to_string);
    }

    #[test]
    fn test_default_fallback_value() {
        // 测试默认回退值的正确性
        let default_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(default_ip.to_string(), "127.0.0.1");

        // 验证默认IP的属性
        if let IpAddr::V4(ipv4) = default_ip {
            assert!(ipv4.is_loopback(), "Default IP should be loopback");
            assert!(!ipv4.is_unspecified(), "Default IP should not be unspecified");
            assert!(!ipv4.is_multicast(), "Default IP should not be multicast");
        }
    }

    #[test]
    fn test_consistency_between_functions() {
        // 测试两个函数之间的一致性
        let ip_option = get_local_ip();
        let ip_string = get_local_ip_with_default();

        match ip_option {
            Some(ip) => {
                // 如果get_local_ip返回Some，那么get_local_ip_with_default应该返回相同的IP
                assert_eq!(ip.to_string(), ip_string,
                    "Both functions should return the same IP when available");
            }
            None => {
                // 如果get_local_ip返回None，那么get_local_ip_with_default应该返回默认值
                assert_eq!(ip_string, "127.0.0.1",
                    "Should return default value when no IP is available");
            }
        }
    }

    #[test]
    fn test_multiple_calls_consistency() {
        // 测试多次调用的一致性
        let ip1 = get_local_ip();
        let ip2 = get_local_ip();
        let ip_str1 = get_local_ip_with_default();
        let ip_str2 = get_local_ip_with_default();

        // 多次调用应该返回相同的结果
        assert_eq!(ip1, ip2, "Multiple calls to get_local_ip should return same result");
        assert_eq!(ip_str1, ip_str2, "Multiple calls to get_local_ip_with_default should return same result");
    }

    #[cfg(feature = "integration")]
    #[test]
    fn test_network_connectivity() {
        // 集成测试：验证获取的IP地址是否可用于网络连接
        if let Some(ip) = get_local_ip() {
            match ip {
                IpAddr::V4(ipv4) => {
                    // 对于IPv4，检查是否为有效的网络地址
                    assert!(!ipv4.is_unspecified(), "Should not be 0.0.0.0");

                    // 如果不是回环地址，应该是可路由的
                    if !ipv4.is_loopback() {
                        println!("Got routable IPv4: {}", ipv4);
                    }
                }
                IpAddr::V6(ipv6) => {
                    // 对于IPv6，检查是否为有效的网络地址
                    assert!(!ipv6.is_unspecified(), "Should not be ::");

                    if !ipv6.is_loopback() {
                        println!("Got routable IPv6: {}", ipv6);
                    }
                }
            }
        }
    }
}
