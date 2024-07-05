use crate::error::{Error, Result};
use lazy_static::lazy_static;
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
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

        if is_local_host(host, 0, 0)? {
            return Ok(a);
        }
    }

    Err(Error::from_string("host in server address should be this server"))
}

/// checks if the given parameter correspond to one of
/// the local IP of the current machine
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> Result<bool> {
    let local_set: HashSet<IpAddr> = LOCAL_IPS.iter().copied().collect();
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
fn must_get_local_ips() -> Result<Vec<IpAddr>> {
    match netif::up() {
        Ok(up) => Ok(up.map(|x| x.address().to_owned()).collect()),
        Err(err) => Err(Error::from_string(format!("Unable to get IP addresses of this host: {}", err))),
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
    fn test_check_local_server_addr() {
        let test_cases = [
            (":54321", Ok(())),
            ("localhost:54321", Ok(())),
            ("0.0.0.0:9000", Ok(())),
            (":0", Ok(())),
            ("localhost", Err(Error::from_string("invalid socket address"))),
            ("", Err(Error::from_string("invalid socket address"))),
            (
                "example.org:54321",
                Err(Error::from_string("host in server address should be this server")),
            ),
            (":-10", Err(Error::from_string("invalid port value"))),
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
