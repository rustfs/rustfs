// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use hex_simd::{AsOut, AsciiCase};
use hmac::Hmac;
#[cfg(not(feature = "fips"))]
pub use hmac::{KeyInit, Mac};
use hyper::body::Bytes;
#[cfg(not(feature = "fips"))]
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::mem::MaybeUninit;

#[cfg(not(feature = "fips"))]
pub type HmacSha256 = Hmac<Sha256>;

#[cfg(feature = "fips")]
pub struct HmacSha256Output([u8; 32]);

#[cfg(feature = "fips")]
impl HmacSha256Output {
    #[must_use]
    pub fn into_bytes(self) -> hmac::digest::Output<Hmac<Sha256>> {
        self.0.into()
    }
}

#[cfg(feature = "fips")]
pub trait KeyInit: Sized {
    fn new_from_slice(key: &[u8]) -> Result<Self, hmac::digest::InvalidLength>;
}

#[cfg(feature = "fips")]
pub trait Mac: Sized {
    fn update(&mut self, data: &[u8]);
    fn finalize(self) -> HmacSha256Output;
    fn verify_slice(self, tag: &[u8]) -> Result<(), hmac::digest::MacError>;
}

#[cfg(feature = "fips")]
#[derive(Clone)]
pub struct HmacSha256(aws_lc_rs::hmac::Context);

#[cfg(feature = "fips")]
impl HmacSha256 {
    pub fn new_from_slice(key: &[u8]) -> Result<Self, hmac::digest::InvalidLength> {
        let signing_key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key);
        Ok(Self(aws_lc_rs::hmac::Context::with_key(&signing_key)))
    }
}

#[cfg(feature = "fips")]
impl KeyInit for HmacSha256 {
    fn new_from_slice(key: &[u8]) -> Result<Self, hmac::digest::InvalidLength> {
        Self::new_from_slice(key)
    }
}

#[cfg(feature = "fips")]
impl Mac for HmacSha256 {
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize(self) -> HmacSha256Output {
        let tag = self.0.sign();
        HmacSha256Output(tag.as_ref().try_into().expect("HMAC-SHA256 tag is 32 bytes"))
    }

    fn verify_slice(self, tag: &[u8]) -> Result<(), hmac::digest::MacError> {
        let computed = self.0.sign();
        aws_lc_rs::constant_time::verify_slices_are_equal(computed.as_ref(), tag).map_err(|_| hmac::digest::MacError)
    }
}

/// Base64 URL safe encoding without padding
/// `base64_encode_url_safe_no_pad(input)`
///
/// # Arguments
/// * `input` - A byte slice to be encoded
///
/// # Returns
/// A `String` containing the Base64 URL safe encoded representation of the input data
pub fn base64_encode_url_safe_no_pad(input: &[u8]) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(input)
}

/// Base64 URL safe decoding without padding
/// `base64_decode_url_safe_no_pad(input)`
///
/// # Arguments
/// * `input` - A byte slice containing the Base64 URL safe encoded data
///
/// # Returns
/// A `Result` containing a `Vec<u8>` with the decoded data or a `base64_simd::Error` if decoding fails
///
/// # Errors
/// This function will return an error if the input data is not valid Base64 URL safe encoded data
///
pub fn base64_decode_url_safe_no_pad(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::URL_SAFE_NO_PAD.decode_to_vec(input)
}

/// encode to hex string (lowercase)
/// `hex(data)`
///
/// # Arguments
/// * `data` - A byte slice to be encoded
///
/// # Returns
/// A `String` containing the hexadecimal representation of the input data in lowercase
///
pub fn hex(data: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower)
}

/// verify sha256 checksum string
///
/// # Arguments
/// * `s` - A string slice to be verified
///
/// # Returns
/// A `bool` indicating whether the input string is exactly 64 lowercase hexadecimal characters.
/// Uppercase hexadecimal characters are not accepted.
pub fn is_sha256_checksum(s: &str) -> bool {
    let is_lowercase_hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f');
    s.len() == 64 && s.as_bytes().iter().copied().all(is_lowercase_hex)
}

/// HMAC-SHA1 hashing
/// `hmac_sha1(key, data)`
///
/// # Arguments
/// * `key` - A byte slice representing the HMAC key
/// * `data` - A byte slice representing the data to be hashed
///
/// # Returns
/// A 20-byte array containing the HMAC-SHA1 hash of the input data using the provided key
///
pub fn hmac_sha1(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> [u8; 20] {
    #[cfg(feature = "fips")]
    {
        let signing_key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, key.as_ref());
        let tag = aws_lc_rs::hmac::sign(&signing_key, data.as_ref());
        tag.as_ref().try_into().expect("HMAC-SHA1 tag is 20 bytes")
    }
    #[cfg(not(feature = "fips"))]
    {
        let mut m = <Hmac<Sha1>>::new_from_slice(key.as_ref()).expect("operation should succeed");
        m.update(data.as_ref());
        m.finalize().into_bytes().into()
    }
}

/// HMAC-SHA256 hashing
/// `hmac_sha256(key, data)`
///
/// # Arguments
/// * `key` - A byte slice representing the HMAC key
/// * `data` - A byte slice representing the data to be hashed
///
/// # Returns
/// A 32-byte array containing the HMAC-SHA256 hash of the input data using the provided key
///
pub fn hmac_sha256(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key.as_ref()).expect("operation should succeed");
    m.update(data.as_ref());
    m.finalize().into_bytes().into()
}

/// `f(hex(src))`
fn hex_bytes32<R>(src: impl AsRef<[u8]>, f: impl FnOnce(&str) -> R) -> R {
    let buf: &mut [_] = &mut [MaybeUninit::uninit(); 64];
    let ans = hex_simd::encode_as_str(src.as_ref(), buf.as_out(), AsciiCase::Lower);
    f(ans)
}

fn sha256(data: &[u8]) -> impl AsRef<[u8; 32]> + use<> {
    <Sha256 as Digest>::digest(data)
}

fn sha256_chunk(chunk: &[Bytes]) -> impl AsRef<[u8; 32]> + use<> {
    let mut h = <Sha256 as Digest>::new();
    chunk.iter().for_each(|data| h.update(data));
    h.finalize()
}

/// hex of sha256 `f(hex(sha256(data)))`
///
/// # Arguments
/// * `data` - A byte slice representing the data to be hashed
/// * `f` - A closure that takes a string slice and returns a value of type `R`
///
/// # Returns
/// A value of type `R` returned by the closure `f` after processing the hexadecimal
/// representation of the SHA-256 hash of the input data
///
pub fn hex_sha256<R>(data: &[u8], f: impl FnOnce(&str) -> R) -> R {
    hex_bytes32(sha256(data).as_ref(), f)
}

/// `f(hex(sha256(chunk)))`
pub fn hex_sha256_chunk<R>(chunk: &[Bytes], f: impl FnOnce(&str) -> R) -> R {
    hex_bytes32(sha256_chunk(chunk).as_ref(), f)
}

#[test]
fn test_base64_encoding_decoding() {
    let original_uuid_timestamp = "c0194290-d911-45cb-8e12-79ec563f46a8x1735460504394878000";

    let encoded_string = base64_encode_url_safe_no_pad(original_uuid_timestamp.as_bytes());

    println!("Encoded: {}", encoded_string);

    let decoded_bytes = base64_decode_url_safe_no_pad(encoded_string.as_bytes()).expect("operation should succeed");
    let decoded_string = String::from_utf8(decoded_bytes).expect("operation should succeed");

    assert_eq!(decoded_string, original_uuid_timestamp)
}

#[test]
fn sha256_checksum_accepts_exact_lowercase_hex() {
    assert!(is_sha256_checksum("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"));
}

#[test]
fn sha256_checksum_rejects_wrong_length_or_non_lowercase_hex() {
    assert!(!is_sha256_checksum("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"));
    assert!(!is_sha256_checksum("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"));
    assert!(!is_sha256_checksum("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"));
    assert!(!is_sha256_checksum("0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"));
}

#[test]
fn test_hmac_sha256_rfc4231() {
    #[cfg(feature = "fips")]
    aws_lc_rs::try_fips_mode().expect("aws-lc-rs FIPS mode must be active when `rustfs-utils/fips` is enabled");

    let key = [0x0b_u8; 20];
    let data = b"Hi There";
    assert_eq!(hex(hmac_sha1(key, data)), "b617318655057264e28bc0b6fb378c8ef146be00");
    let mac = hmac_sha256(key, data);
    assert_eq!(hex(mac), "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");

    let mut verifier = <HmacSha256 as KeyInit>::new_from_slice(&key).expect("valid key");
    verifier.update(data);
    assert!(verifier.clone().verify_slice(&mac).is_ok());
    assert!(verifier.verify_slice(&[0_u8; 32]).is_err());
}
