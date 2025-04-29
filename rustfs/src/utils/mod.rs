mod certs;
use std::net::IpAddr;

pub(crate) use certs::create_multi_cert_resolver;
pub(crate) use certs::error;
pub(crate) use certs::load_all_certs_from_directory;
pub(crate) use certs::load_certs;
pub(crate) use certs::load_private_key;

/// Get the local IP address.
/// This function retrieves the local IP address of the machine.
pub(crate) fn get_local_ip() -> Option<std::net::Ipv4Addr> {
    match local_ip_address::local_ip() {
        Ok(IpAddr::V4(ip)) => Some(ip),
        Err(_) => None,
        Ok(IpAddr::V6(_)) => todo!(),
    }
}
