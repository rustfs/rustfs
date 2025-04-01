use rustls_pemfile::{certs, private_key};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::net::IpAddr;
use std::{fs, io};

pub(crate) fn get_local_ip() -> Option<std::net::Ipv4Addr> {
    match local_ip_address::local_ip() {
        Ok(IpAddr::V4(ip)) => Some(ip),
        Err(_) => None,
        Ok(IpAddr::V6(_)) => todo!(),
    }
}

/// Load public certificate from file.
pub(crate) fn load_certs(filename: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    // Open certificate file.
    let cert_file = fs::File::open(filename).map_err(|e| error(format!("failed to open {}: {}", filename, e)))?;
    let mut reader = io::BufReader::new(cert_file);

    // Load and return certificate.
    certs(&mut reader).collect()
}

/// Load private key from file.
pub(crate) fn load_private_key(filename: &str) -> io::Result<PrivateKeyDer<'static>> {
    // Open keyfile.
    let keyfile = fs::File::open(filename).map_err(|e| error(format!("failed to open {}: {}", filename, e)))?;
    let mut reader = io::BufReader::new(keyfile);

    // Load and return a single private key.
    private_key(&mut reader).map(|key| key.unwrap())
}

pub(crate) fn error(err: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}
