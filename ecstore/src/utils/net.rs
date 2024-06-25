use std::{
    collections::HashMap,
    net::{IpAddr, ToSocketAddrs},
};

use anyhow::Error;
use netif;
use url::Host;

pub fn split_host_port(s: &str) -> Result<(String, u16), Error> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 2 {
        if let Ok(port) = parts[1].parse::<u16>() {
            return Ok((parts[0].to_string(), port));
        }
    }
    Err(Error::msg("Invalid address format or port number"))
}

// is_local_host 判断是否是本地ip
pub fn is_local_host(host: Host<&str>, port: u16, local_port: u16) -> bool {
    let local_ips = must_get_local_ips();

    let local_map =
        local_ips
            .iter()
            .map(|ip| ip.to_string())
            .fold(HashMap::new(), |mut acc, item| {
                *acc.entry(item).or_insert(true) = true;
                acc
            });

    let is_local_host = match host {
        Host::Domain(domain) => {
            let ips: Vec<String> = (domain, 0)
                .to_socket_addrs()
                .unwrap_or(Vec::new().into_iter())
                .map(|addr| addr.ip().to_string())
                .collect();

            let mut isok = false;
            for ip in ips.iter() {
                if local_map.contains_key(ip) {
                    isok = true;
                    break;
                }
            }
            isok
        }
        Host::Ipv4(ip) => local_map.contains_key(&ip.to_string()),
        Host::Ipv6(ip) => local_map.contains_key(&ip.to_string()),
    };

    if port > 0 {
        return is_local_host && port == local_port;
    }

    is_local_host
}

pub fn must_get_local_ips() -> Vec<IpAddr> {
    let mut v: Vec<IpAddr> = Vec::new();
    if let Some(up) = netif::up().ok() {
        v = up.map(|x| x.address().to_owned()).collect();
    }

    v
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use super::*;

    #[test]
    fn test_must_get_local_ips() {
        let ips = must_get_local_ips();
        for ip in ips.iter() {
            println!("{:?}", ip)
        }
    }

    #[test]
    fn test_is_local_host() {
        // let host = Host::Ipv4(Ipv4Addr::new(192, 168, 0, 233));
        let host = Host::Ipv4(Ipv4Addr::new(127, 0, 0, 1));
        // let host = Host::Domain("localhost");
        let port = 0;
        let local_port = 9000;
        let is = is_local_host(host, port, local_port);
        assert!(is)
    }
}
