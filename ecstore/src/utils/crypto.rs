pub fn base64_encode(input: &[u8]) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(input)
}

pub fn base64_decode(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::URL_SAFE_NO_PAD.decode_to_vec(input)
}

pub fn hex(data: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower)
}

#[cfg(windows)]
pub fn sha256(data: &[u8]) -> impl AsRef<[u8; 32]> {
    use sha2::{Digest, Sha256};
    <Sha256 as Digest>::digest(data)
}

#[cfg(not(windows))]
pub fn sha256(data: &[u8]) -> impl AsRef<[u8]> {
    use openssl::hash::{Hasher, MessageDigest};
    let mut h = Hasher::new(MessageDigest::sha256()).unwrap();
    h.update(data).unwrap();
    h.finish().unwrap()
}
