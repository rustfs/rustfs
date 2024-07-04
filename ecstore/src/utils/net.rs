use crate::error::{Error, Result};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
};
use url::Host;

/// helper for validating if the provided arg is an ip address.
pub fn is_socket_addr(host: &str) -> bool {
    host.parse::<SocketAddr>().is_ok() || host.parse::<IpAddr>().is_ok()
}

/// checks if server_addr is valid and local host.
pub fn check_local_server_addr(server_addr: &str) -> Result<SocketAddr> {
    let addr: Vec<SocketAddr> = match server_addr.to_socket_addrs() {
        Ok(addr) => addr.collect(),
        Err(err) => return Err(Error::new(Box::new(err))),
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

        if is_local_host(host, 0, 0).is_ok() {
            return Ok(a);
        }
    }

    return Err(Error::from_string("host in server address should be this server"));
}

pub fn split_host_port(s: &str) -> Result<(String, u16)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 2 {
        if let Ok(port) = parts[1].parse::<u16>() {
            return Ok((parts[0].to_string(), port));
        }
    }
    Err(Error::from_string("Invalid address format or port number"))
}

/// checks if the given parameter correspond to one of
/// the local IP of the current machine
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> Result<bool> {
    let local_ips = must_get_local_ips();

    let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();
    let is_local_host = match host {
        Host::Domain(domain) => {
            let ips = match (domain, 0).to_socket_addrs().map(|v| v.map(|v| v.ip()).collect::<Vec<_>>()) {
                Ok(ips) => ips,
                Err(err) => return Err(Error::new(Box::new(err))),
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

/// returns IPs of local interface
fn must_get_local_ips() -> Vec<IpAddr> {
    match netif::up() {
        Ok(up) => up.map(|x| x.address().to_owned()).collect(),
        Err(_) => vec![],
    }
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
    fn test_must_get_local_ips() {
        let local_ips = must_get_local_ips();
        let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();

        assert!(local_set.contains(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
    }
}
