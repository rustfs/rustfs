use lazy_static::lazy_static;
use std::io::{Error, Result};
use std::{
    collections::HashSet,
    fmt::Display,
    net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener, ToSocketAddrs},
};

use url::Host;

lazy_static! {
    static ref LOCAL_IPS: Vec<IpAddr> = must_get_local_ips().unwrap();
}

/// helper for validating if the provided arg is an ip address.
pub fn is_socket_addr(addr: &str) -> bool {
    // TODO IPv6 zone information?

    addr.parse::<SocketAddr>().is_ok() || addr.parse::<IpAddr>().is_ok()
}

/// checks if server_addr is valid and local host.
pub fn check_local_server_addr(server_addr: &str) -> Result<SocketAddr> {
    let addr: Vec<SocketAddr> = match server_addr.to_socket_addrs() {
        Ok(addr) => addr.collect(),
        Err(err) => return Err(err),
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

    Err(Error::other("host in server address should be this server"))
}

/// checks if the given parameter correspond to one of
/// the local IP of the current machine
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> Result<bool> {
    let local_set: HashSet<IpAddr> = LOCAL_IPS.iter().copied().collect();
    let is_local_host = match host {
        Host::Domain(domain) => {
            let ips = match (domain, 0).to_socket_addrs().map(|v| v.map(|v| v.ip()).collect::<Vec<_>>()) {
                Ok(ips) => ips,
                Err(err) => return Err(err),
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
pub fn get_host_ip(host: Host<&str>) -> Result<HashSet<IpAddr>> {
    match host {
        Host::Domain(domain) => match (domain, 0)
            .to_socket_addrs()
            .map(|v| v.map(|v| v.ip()).collect::<HashSet<_>>())
        {
            Ok(ips) => Ok(ips),
            Err(err) => Err(err),
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
pub(crate) fn must_get_local_ips() -> Result<Vec<IpAddr>> {
    match netif::up() {
        Ok(up) => Ok(up.map(|x| x.address().to_owned()).collect()),
        Err(err) => Err(Error::other(format!("Unable to get IP addresses of this host: {}", err))),
    }
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

/// parses the address string, process the ":port" format for double-stack binding,
/// and resolve the host name or IP address. If the port is 0, an available port is assigned.
pub fn parse_and_resolve_address(addr_str: &str) -> Result<SocketAddr> {
    let resolved_addr: SocketAddr = if let Some(port) = addr_str.strip_prefix(":") {
        // Process the ":port" format for double stack binding
        let port_str = port;
        let port: u16 = port_str
            .parse()
            .map_err(|e| Error::other(format!("Invalid port format: {}, err:{:?}", addr_str, e)))?;
        let final_port = if port == 0 {
            get_available_port() // assume get_available_port is available here
        } else {
            port
        };
        // Using IPv6 without address specified [::], it should handle both IPv4 and IPv6
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), final_port)
    } else {
        // Use existing logic to handle regular address formats
        let mut addr = check_local_server_addr(addr_str)?; // assume check_local_server_addr is available here
        if addr.port() == 0 {
            addr.set_port(get_available_port());
        }
        addr
    };
    Ok(resolved_addr)
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use super::*;

    #[test]
    fn test_is_socket_addr() {
        let test_cases = [
            ("localhost", false),
            ("localhost:9000", false),
            ("example.com", false),
            ("http://192.168.1.0", false),
            ("http://192.168.1.0:9000", false),
            ("192.168.1.0", true),
            ("[2001:db8::1]:9000", true),
        ];

        for (addr, expected) in test_cases {
            let ret = is_socket_addr(addr);
            assert_eq!(expected, ret, "addr: {}, expected: {}, got: {}", addr, expected, ret);
        }
    }

    #[test]
    fn test_check_local_server_addr() {
        let test_cases = [
            // (":54321", Ok(())),
            ("localhost:54321", Ok(())),
            ("0.0.0.0:9000", Ok(())),
            // (":0", Ok(())),
            ("localhost", Err(Error::other("invalid socket address"))),
            ("", Err(Error::other("invalid socket address"))),
            ("example.org:54321", Err(Error::other("host in server address should be this server"))),
            (":-10", Err(Error::other("invalid port value"))),
        ];

        for test_case in test_cases {
            let ret = check_local_server_addr(test_case.0);
            if test_case.1.is_ok() && ret.is_err() {
                panic!("{}: error: expected = <nil>, got = {:?}", test_case.0, ret);
            }
            if test_case.1.is_err() && ret.is_ok() {
                panic!("{}: error: expected = {:?}, got = <nil>", test_case.0, test_case.1);
            }
        }
    }

    #[test]
    fn test_must_get_local_ips() {
        let local_ips = must_get_local_ips().unwrap();
        let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();

        assert!(local_set.contains(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
    }
}
