use local_ip_address::{local_ip, local_ipv6};
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
    local_ip().ok().or_else(|| local_ipv6().ok())
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

    #[test]
    fn test_get_local_ip() {
        match get_local_ip() {
            Some(ip) => println!("the ip address of this machine:{}", ip),
            None => println!("Unable to obtain the IP address of the machine"),
        }
        assert!(get_local_ip().is_some());
    }
}
