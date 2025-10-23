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

use std::mem::MaybeUninit;

use hex_simd::{AsOut, AsciiCase};
use hyper::body::Bytes;

pub fn base64_encode_url_safe_no_pad(input: &[u8]) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(input)
}

pub fn base64_decode_url_safe_no_pad(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::URL_SAFE_NO_PAD.decode_to_vec(input)
}

pub fn hex(data: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower)
}

/// verify sha256 checksum string
pub fn is_sha256_checksum(s: &str) -> bool {
    // TODO: optimize
    let is_lowercase_hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f');
    s.len() == 64 && s.as_bytes().iter().copied().all(is_lowercase_hex)
}

/// `hmac_sha1(key, data)`
pub fn hmac_sha1(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> [u8; 20] {
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    let mut m = <Hmac<Sha1>>::new_from_slice(key.as_ref()).unwrap();
    m.update(data.as_ref());
    m.finalize().into_bytes().into()
}

/// `hmac_sha256(key, data)`
pub fn hmac_sha256(key: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut m = Hmac::<Sha256>::new_from_slice(key.as_ref()).unwrap();
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
    use sha2::{Digest, Sha256};
    <Sha256 as Digest>::digest(data)
}

fn sha256_chunk(chunk: &[Bytes]) -> impl AsRef<[u8; 32]> + use<> {
    use sha2::{Digest, Sha256};
    let mut h = <Sha256 as Digest>::new();
    chunk.iter().for_each(|data| h.update(data));
    h.finalize()
}

/// `f(hex(sha256(data)))`
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

    println!("Encoded: {}", &encoded_string);

    let decoded_bytes = base64_decode_url_safe_no_pad(encoded_string.clone().as_bytes()).unwrap();
    let decoded_string = String::from_utf8(decoded_bytes).unwrap();

    assert_eq!(decoded_string, original_uuid_timestamp)
}
