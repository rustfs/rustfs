use std::net::IpAddr;

pub(crate) fn get_local_ip() -> Option<std::net::Ipv4Addr> {
    match local_ip_address::local_ip() {
        Ok(IpAddr::V4(ip)) => Some(ip),
        Err(_) => None,
        Ok(IpAddr::V6(_)) => todo!(),
    }
}
