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

/// Standard base64 (with `+`/`/` and `=` padding). Every base64 value this
/// transition client emits or parses — `Content-MD5`, `x-amz-checksum-*`, and
/// checksum digests in request/response bodies — is S3 wire format, which is
/// standard base64. The URL-safe, unpadded alphabet used previously made remotes
/// reject `Content-MD5` with "Invalid content MD5: Base64Error" and could not
/// even decode a padded checksum coming back from the peer (rustfs/rustfs#4811).
pub fn base64_encode(input: &[u8]) -> String {
    base64_simd::STANDARD.encode_to_string(input)
}

pub fn base64_decode(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::STANDARD.decode_to_vec(input)
}

#[cfg(test)]
mod tests {
    use super::{base64_decode, base64_encode};

    #[test]
    fn base64_encode_is_standard_s3_wire_format() {
        // S3 reads Content-MD5 / checksum values with a standard base64 decoder,
        // so the encoder must emit '+'/'/' and '=' padding and round-trip through
        // one. Regression for rustfs/rustfs#4811 ("Invalid content MD5:
        // Base64Error"). 16-byte MD5-length input chosen to force '=' padding.
        let digest: [u8; 16] = [
            0xfb, 0xff, 0xff, 0xef, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0,
        ];
        let encoded = base64_encode(&digest);
        assert!(encoded.ends_with('='), "16-byte input must be padded: {encoded}");
        assert!(!encoded.contains(['-', '_']), "must use the standard alphabet: {encoded}");
        let via_standard = base64_simd::STANDARD
            .decode_to_vec(encoded.as_bytes())
            .expect("standard decode");
        assert_eq!(via_standard, digest);
        // Our own decoder must accept the same wire format it produces.
        assert_eq!(base64_decode(encoded.as_bytes()).expect("round-trip"), digest);
    }
}
