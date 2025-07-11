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

use bytes::Bytes;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use hyper::client::conn::http2::Builder;
use hyper_util::rt::TokioExecutor;
use std::net::Ipv6Addr;
use std::sync::LazyLock;
use std::{
    collections::HashSet,
    fmt::Display,
    net::{IpAddr, SocketAddr, TcpListener, ToSocketAddrs},
};
use transform_stream::AsyncTryStream;
use url::{Host, Url};

static LOCAL_IPS: LazyLock<Vec<IpAddr>> = LazyLock::new(|| must_get_local_ips().unwrap());

/// helper for validating if the provided arg is an ip address.
pub fn is_socket_addr(addr: &str) -> bool {
    // TODO IPv6 zone information?

    addr.parse::<SocketAddr>().is_ok() || addr.parse::<IpAddr>().is_ok()
}

/// checks if server_addr is valid and local host.
pub fn check_local_server_addr(server_addr: &str) -> std::io::Result<SocketAddr> {
    let addr: Vec<SocketAddr> = match server_addr.to_socket_addrs() {
        Ok(addr) => addr.collect(),
        Err(err) => return Err(std::io::Error::other(err)),
    };

    // 0.0.0.0 is a wildcard address and refers to local network
    // addresses. I.e, 0.0.0.0:9000 like ":9000" refers to port
    // 9000 on localhost.
    for a in addr {
        if a.ip().is_unspecified() {
            return Ok(a);
        }

        let host = match a {
            SocketAddr::V4(a) => Host::<&str>::Ipv4(*a.ip()),
            SocketAddr::V6(a) => Host::Ipv6(*a.ip()),
        };

        if is_local_host(host, 0, 0)? {
            return Ok(a);
        }
    }

    Err(std::io::Error::other("host in server address should be this server"))
}

/// checks if the given parameter correspond to one of
/// the local IP of the current machine
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> std::io::Result<bool> {
    let local_set: HashSet<IpAddr> = LOCAL_IPS.iter().copied().collect();
    let is_local_host = match host {
        Host::Domain(domain) => {
            let ips = match (domain, 0).to_socket_addrs().map(|v| v.map(|v| v.ip()).collect::<Vec<_>>()) {
                Ok(ips) => ips,
                Err(err) => return Err(std::io::Error::other(err)),
            };

            ips.iter().any(|ip| local_set.contains(ip))
        }
        Host::Ipv4(ip) => local_set.contains(&IpAddr::V4(ip)),
        Host::Ipv6(ip) => local_set.contains(&IpAddr::V6(ip)),
    };

    if port > 0 {
        return Ok(is_local_host && port == local_port);
    }

    Ok(is_local_host)
}

/// returns IP address of given host.
pub fn get_host_ip(host: Host<&str>) -> std::io::Result<HashSet<IpAddr>> {
    match host {
        Host::Domain(domain) => match (domain, 0)
            .to_socket_addrs()
            .map(|v| v.map(|v| v.ip()).collect::<HashSet<_>>())
        {
            Ok(ips) => Ok(ips),
            Err(err) => Err(std::io::Error::other(err)),
        },
        Host::Ipv4(ip) => {
            let mut set = HashSet::with_capacity(1);
            set.insert(IpAddr::V4(ip));
            Ok(set)
        }
        Host::Ipv6(ip) => {
            let mut set = HashSet::with_capacity(1);
            set.insert(IpAddr::V6(ip));
            Ok(set)
        }
    }
}

pub fn get_available_port() -> u16 {
    TcpListener::bind("0.0.0.0:0").unwrap().local_addr().unwrap().port()
}

/// returns IPs of local interface
pub fn must_get_local_ips() -> std::io::Result<Vec<IpAddr>> {
    match netif::up() {
        Ok(up) => Ok(up.map(|x| x.address().to_owned()).collect()),
        Err(err) => Err(std::io::Error::other(format!("Unable to get IP addresses of this host: {err}"))),
    }
}

pub fn get_default_location(_u: Url, _region_override: &str) -> String {
    todo!();
}

pub fn get_endpoint_url(endpoint: &str, secure: bool) -> Result<Url, std::io::Error> {
    let mut scheme = "https";
    if !secure {
        scheme = "http";
    }

    let endpoint_url_str = format!("{scheme}://{endpoint}");
    let Ok(endpoint_url) = Url::parse(&endpoint_url_str) else {
        return Err(std::io::Error::other("url parse error."));
    };

    //is_valid_endpoint_url(endpoint_url)?;
    Ok(endpoint_url)
}

pub const DEFAULT_DIAL_TIMEOUT: i64 = 5;

pub fn new_remotetarget_http_transport(_insecure: bool) -> Builder<TokioExecutor> {
    todo!();
}

const ALLOWED_CUSTOM_QUERY_PREFIX: &str = "x-";

pub fn is_custom_query_value(qs_key: &str) -> bool {
    qs_key.starts_with(ALLOWED_CUSTOM_QUERY_PREFIX)
}

#[derive(Debug, Clone)]
pub struct XHost {
    pub name: String,
    pub port: u16,
    pub is_port_set: bool,
}

impl Display for XHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.is_port_set {
            write!(f, "{}", self.name)
        } else if self.name.contains(':') {
            write!(f, "[{}]:{}", self.name, self.port)
        } else {
            write!(f, "{}:{}", self.name, self.port)
        }
    }
}

impl TryFrom<String> for XHost {
    type Error = std::io::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        if let Some(addr) = value.to_socket_addrs()?.next() {
            Ok(Self {
                name: addr.ip().to_string(),
                port: addr.port(),
                is_port_set: addr.port() > 0,
            })
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "value invalid"))
        }
    }
}

pub fn parse_and_resolve_address(addr_str: &str) -> std::io::Result<SocketAddr> {
    let resolved_addr: SocketAddr = if let Some(port) = addr_str.strip_prefix(":") {
        let port_str = port;
        let port: u16 = port_str
            .parse()
            .map_err(|e| std::io::Error::other(format!("Invalid port format: {addr_str}, err:{e:?}")))?;
        let final_port = if port == 0 {
            get_available_port() // assume get_available_port is available here
        } else {
            port
        };
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), final_port)
    } else {
        let mut addr = check_local_server_addr(addr_str)?; // assume check_local_server_addr is available here
        if addr.port() == 0 {
            addr.set_port(get_available_port());
        }
        addr
    };
    Ok(resolved_addr)
}

#[allow(dead_code)]
pub fn bytes_stream<S, E>(stream: S, content_length: usize) -> impl Stream<Item = std::result::Result<Bytes, E>> + Send + 'static
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    AsyncTryStream::<Bytes, E, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while let Some(result) = stream.next().await {
            let mut bytes = result?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn test_is_socket_addr() {
        let test_cases = [
            // Valid IP addresses
            ("192.168.1.0", true),
            ("127.0.0.1", true),
            ("10.0.0.1", true),
            ("0.0.0.0", true),
            ("255.255.255.255", true),
            // Valid IPv6 addresses
            ("2001:db8::1", true),
            ("::1", true),
            ("::", true),
            ("fe80::1", true),
            // Valid socket addresses
            ("192.168.1.0:8080", true),
            ("127.0.0.1:9000", true),
            ("[2001:db8::1]:9000", true),
            ("[::1]:8080", true),
            ("0.0.0.0:0", true),
            // Invalid addresses
            ("localhost", false),
            ("localhost:9000", false),
            ("example.com", false),
            ("example.com:8080", false),
            ("http://192.168.1.0", false),
            ("http://192.168.1.0:9000", false),
            ("256.256.256.256", false),
            ("192.168.1", false),
            ("192.168.1.0.1", false),
            ("", false),
            (":", false),
            (":::", false),
            ("invalid_ip", false),
        ];

        for (addr, expected) in test_cases {
            let result = is_socket_addr(addr);
            assert_eq!(expected, result, "addr: '{addr}', expected: {expected}, got: {result}");
        }
    }

    #[test]
    fn test_check_local_server_addr() {
        // Test valid local addresses
        let valid_cases = ["localhost:54321", "127.0.0.1:9000", "0.0.0.0:9000", "[::1]:8080", "::1:8080"];

        for addr in valid_cases {
            let result = check_local_server_addr(addr);
            assert!(result.is_ok(), "Expected '{addr}' to be valid, but got error: {result:?}");
        }

        // Test invalid addresses
        let invalid_cases = [
            ("localhost", "invalid socket address"),
            ("", "invalid socket address"),
            ("example.org:54321", "host in server address should be this server"),
            ("8.8.8.8:53", "host in server address should be this server"),
            (":-10", "invalid port value"),
            ("invalid:port", "invalid port value"),
        ];

        for (addr, expected_error_pattern) in invalid_cases {
            let result = check_local_server_addr(addr);
            assert!(result.is_err(), "Expected '{addr}' to be invalid, but it was accepted: {result:?}");

            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains(expected_error_pattern) || error_msg.contains("invalid socket address"),
                "Error message '{error_msg}' doesn't contain expected pattern '{expected_error_pattern}' for address '{addr}'"
            );
        }
    }

    #[test]
    fn test_is_local_host() {
        // Test localhost domain
        let localhost_host = Host::Domain("localhost");
        assert!(is_local_host(localhost_host, 0, 0).unwrap());

        // Test loopback IP addresses
        let ipv4_loopback = Host::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
        assert!(is_local_host(ipv4_loopback, 0, 0).unwrap());

        let ipv6_loopback = Host::Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
        assert!(is_local_host(ipv6_loopback, 0, 0).unwrap());

        // Test port matching
        let localhost_with_port1 = Host::Domain("localhost");
        assert!(is_local_host(localhost_with_port1, 8080, 8080).unwrap());
        let localhost_with_port2 = Host::Domain("localhost");
        assert!(!is_local_host(localhost_with_port2, 8080, 9000).unwrap());

        // Test non-local host
        let external_host = Host::Ipv4(Ipv4Addr::new(8, 8, 8, 8));
        assert!(!is_local_host(external_host, 0, 0).unwrap());

        // Test invalid domain should return error
        let invalid_host = Host::Domain("invalid.nonexistent.domain.example");
        assert!(is_local_host(invalid_host, 0, 0).is_err());
    }

    #[test]
    fn test_get_host_ip() {
        // Test IPv4 address
        let ipv4_host = Host::Ipv4(Ipv4Addr::new(192, 168, 1, 1));
        let ipv4_result = get_host_ip(ipv4_host).unwrap();
        assert_eq!(ipv4_result.len(), 1);
        assert!(ipv4_result.contains(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));

        // Test IPv6 address
        let ipv6_host = Host::Ipv6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
        let ipv6_result = get_host_ip(ipv6_host).unwrap();
        assert_eq!(ipv6_result.len(), 1);
        assert!(ipv6_result.contains(&IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))));

        // Test localhost domain
        let localhost_host = Host::Domain("localhost");
        let localhost_result = get_host_ip(localhost_host).unwrap();
        assert!(!localhost_result.is_empty());
        // Should contain at least loopback address
        assert!(
            localhost_result.contains(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
                || localhost_result.contains(&IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)))
        );

        // Test invalid domain
        let invalid_host = Host::Domain("invalid.nonexistent.domain.example");
        assert!(get_host_ip(invalid_host).is_err());
    }

    #[test]
    fn test_get_available_port() {
        let port1 = get_available_port();
        let port2 = get_available_port();

        // Port should be in valid range (u16 max is always <= 65535)
        assert!(port1 > 0);
        assert!(port2 > 0);

        // Different calls should typically return different ports
        assert_ne!(port1, port2);
    }

    #[test]
    fn test_must_get_local_ips() {
        let local_ips = must_get_local_ips().unwrap();
        let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();

        // Should contain loopback addresses
        assert!(local_set.contains(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));

        // Should not be empty
        assert!(!local_set.is_empty());

        // All IPs should be valid
        for ip in &local_set {
            match ip {
                IpAddr::V4(_) | IpAddr::V6(_) => {} // Valid
            }
        }
    }

    #[test]
    fn test_xhost_display() {
        // Test without port
        let host_no_port = XHost {
            name: "example.com".to_string(),
            port: 0,
            is_port_set: false,
        };
        assert_eq!(host_no_port.to_string(), "example.com");

        // Test with port (IPv4-like name)
        let host_with_port = XHost {
            name: "192.168.1.1".to_string(),
            port: 8080,
            is_port_set: true,
        };
        assert_eq!(host_with_port.to_string(), "192.168.1.1:8080");

        // Test with port (IPv6-like name)
        let host_ipv6_with_port = XHost {
            name: "2001:db8::1".to_string(),
            port: 9000,
            is_port_set: true,
        };
        assert_eq!(host_ipv6_with_port.to_string(), "[2001:db8::1]:9000");

        // Test domain name with port
        let host_domain_with_port = XHost {
            name: "example.com".to_string(),
            port: 443,
            is_port_set: true,
        };
        assert_eq!(host_domain_with_port.to_string(), "example.com:443");
    }

    #[test]
    fn test_xhost_try_from() {
        // Test valid IPv4 address with port
        let result = XHost::try_from("192.168.1.1:8080".to_string()).unwrap();
        assert_eq!(result.name, "192.168.1.1");
        assert_eq!(result.port, 8080);
        assert!(result.is_port_set);

        // Test valid IPv4 address without port
        let result = XHost::try_from("192.168.1.1:0".to_string()).unwrap();
        assert_eq!(result.name, "192.168.1.1");
        assert_eq!(result.port, 0);
        assert!(!result.is_port_set);

        // Test valid IPv6 address with port
        let result = XHost::try_from("[2001:db8::1]:9000".to_string()).unwrap();
        assert_eq!(result.name, "2001:db8::1");
        assert_eq!(result.port, 9000);
        assert!(result.is_port_set);

        // Test localhost with port (localhost may resolve to either IPv4 or IPv6)
        let result = XHost::try_from("localhost:3000".to_string()).unwrap();
        // localhost can resolve to either 127.0.0.1 or ::1 depending on system configuration
        assert!(result.name == "127.0.0.1" || result.name == "::1");
        assert_eq!(result.port, 3000);
        assert!(result.is_port_set);

        // Test invalid format
        let result = XHost::try_from("invalid_format".to_string());
        assert!(result.is_err());

        // Test empty string
        let result = XHost::try_from("".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_and_resolve_address() {
        // Test port-only format
        let result = parse_and_resolve_address(":8080").unwrap();
        assert_eq!(result.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(result.port(), 8080);

        // Test port-only format with port 0 (should get available port)
        let result = parse_and_resolve_address(":0").unwrap();
        assert_eq!(result.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert!(result.port() > 0);

        // Test localhost with port
        let result = parse_and_resolve_address("localhost:9000").unwrap();
        assert_eq!(result.port(), 9000);

        // Test localhost with port 0 (should get available port)
        let result = parse_and_resolve_address("localhost:0").unwrap();
        assert!(result.port() > 0);

        // Test 0.0.0.0 with port
        let result = parse_and_resolve_address("0.0.0.0:7000").unwrap();
        assert_eq!(result.ip(), IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
        assert_eq!(result.port(), 7000);

        // Test invalid port format
        let result = parse_and_resolve_address(":invalid_port");
        assert!(result.is_err());

        // Test invalid address
        let result = parse_and_resolve_address("example.org:8080");
        assert!(result.is_err());
    }

    #[test]
    fn test_edge_cases() {
        // Test empty string for is_socket_addr
        assert!(!is_socket_addr(""));

        // Test single colon for is_socket_addr
        assert!(!is_socket_addr(":"));

        // Test malformed IPv6 for is_socket_addr
        assert!(!is_socket_addr("[::]"));
        assert!(!is_socket_addr("[::1"));

        // Test very long strings
        let long_string = "a".repeat(1000);
        assert!(!is_socket_addr(&long_string));

        // Test unicode characters
        assert!(!is_socket_addr("测试.example.com"));

        // Test special characters
        assert!(!is_socket_addr("test@example.com:8080"));
        assert!(!is_socket_addr("http://example.com:8080"));
    }

    #[test]
    fn test_boundary_values() {
        // Test port boundaries
        assert!(is_socket_addr("127.0.0.1:0"));
        assert!(is_socket_addr("127.0.0.1:65535"));
        assert!(!is_socket_addr("127.0.0.1:65536"));

        // Test IPv4 boundaries
        assert!(is_socket_addr("0.0.0.0"));
        assert!(is_socket_addr("255.255.255.255"));
        assert!(!is_socket_addr("256.0.0.0"));
        assert!(!is_socket_addr("0.0.0.256"));

        // Test XHost with boundary ports
        let host_max_port = XHost {
            name: "example.com".to_string(),
            port: 65535,
            is_port_set: true,
        };
        assert_eq!(host_max_port.to_string(), "example.com:65535");

        let host_zero_port = XHost {
            name: "example.com".to_string(),
            port: 0,
            is_port_set: true,
        };
        assert_eq!(host_zero_port.to_string(), "example.com:0");
    }
}
