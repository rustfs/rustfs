pub fn base64_encode(input: &[u8]) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(input)
}

pub fn base64_decode(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::URL_SAFE_NO_PAD.decode_to_vec(input)
}

pub fn hex(data: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower)
}

// #[cfg(windows)]
// pub fn sha256(data: &[u8]) -> impl AsRef<[u8; 32]> {
//     use sha2::{Digest, Sha256};
//     <Sha256 as Digest>::digest(data)
// }

// #[cfg(not(windows))]
// pub fn sha256(data: &[u8]) -> impl AsRef<[u8]> {
//     use openssl::hash::{Hasher, MessageDigest};
//     let mut h = Hasher::new(MessageDigest::sha256()).unwrap();
//     h.update(data).unwrap();
//     h.finish().unwrap()
// }

#[test]
fn test_base64() {
    let src = "c0194290-d911-45cb-8e12-79ec563f46a8x1735460504394878000";

    let s = base64_encode(src.as_bytes());

    println!("{}", &s);

    let de = base64_decode(s.clone().as_bytes()).unwrap();
    let decoded_str = String::from_utf8(de).unwrap();

    assert_eq!(decoded_str, src)
}
