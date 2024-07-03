use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
};

use anyhow::{Error, Result};
use netif;
use url::Host;

// helper for validating if the provided arg is an ip address.
pub fn is_socket_addr(host: &str) -> bool {
    host.parse::<SocketAddr>().is_ok() || host.parse::<IpAddr>().is_ok()
}

pub fn split_host_port(s: &str) -> Result<(String, u16)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 2 {
        if let Ok(port) = parts[1].parse::<u16>() {
            return Ok((parts[0].to_string(), port));
        }
    }
    Err(Error::msg("Invalid address format or port number"))
}

/// checks if the given parameter correspond to one of
/// the local IP of the current machine
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> bool {
    let local_ips = must_get_local_ips();

    let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();
    let is_local_host = match host {
        Host::Domain(domain) => {
            let ips = match (domain, 0).to_socket_addrs().map(|v| v.map(|v| v.ip()).collect::<Vec<_>>()) {
                Ok(ips) => ips,
                Err(_) => return false,
            };

            ips.iter().any(|ip| local_set.contains(ip))
        }
        Host::Ipv4(ip) => local_set.contains(&IpAddr::V4(ip)),
        Host::Ipv6(ip) => local_set.contains(&IpAddr::V6(ip)),
    };

    if port > 0 {
        return is_local_host && port == local_port;
    }

    is_local_host
}

/// returns IPs of local interface
pub fn must_get_local_ips() -> Vec<IpAddr> {
    match netif::up() {
        Ok(up) => up.map(|x| x.address().to_owned()).collect(),
        Err(_) => vec![],
    }
}

#[cfg(test)]
mod test {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn test_must_get_local_ips() {
        let local_ips = must_get_local_ips();
        let local_set: HashSet<IpAddr> = local_ips.into_iter().collect();

        assert!(local_set.contains(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
    }

    #[test]
    fn test_is_local_host() {
        let host = Host::Domain("localhost");
        let is = is_local_host(host, 0, 9000);
        assert!(is);

        let host = Host::Ipv6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
        let is = is_local_host(host, 0, 9000);
        assert!(is);

        let host = Host::Ipv4(Ipv4Addr::new(8, 8, 8, 8));
        let is = is_local_host(host, 0, 9000);
        assert!(!is);

        let host = Host::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
        let is = is_local_host(host, 8000, 9000);
        assert!(!is);
    }
}
