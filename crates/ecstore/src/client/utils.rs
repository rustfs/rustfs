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

use lazy_static::lazy_static;
use std::collections::HashMap;

use s3s::header::X_AMZ_STORAGE_CLASS;

lazy_static! {
    static ref SUPPORTED_QUERY_VALUES: HashMap<String, bool> = {
        let mut m = HashMap::new();
        m.insert("attributes".to_string(), true);
        m.insert("partNumber".to_string(), true);
        m.insert("versionId".to_string(), true);
        m.insert("response-cache-control".to_string(), true);
        m.insert("response-content-disposition".to_string(), true);
        m.insert("response-content-encoding".to_string(), true);
        m.insert("response-content-language".to_string(), true);
        m.insert("response-content-type".to_string(), true);
        m.insert("response-expires".to_string(), true);
        m
    };
    static ref SUPPORTED_HEADERS: HashMap<String, bool> = {
        let mut m = HashMap::new();
        m.insert("content-type".to_string(), true);
        m.insert("cache-control".to_string(), true);
        m.insert("content-encoding".to_string(), true);
        m.insert("content-disposition".to_string(), true);
        m.insert("content-language".to_string(), true);
        m.insert("x-amz-website-redirect-location".to_string(), true);
        m.insert("x-amz-object-lock-mode".to_string(), true);
        m.insert("x-amz-metadata-directive".to_string(), true);
        m.insert("x-amz-object-lock-retain-until-date".to_string(), true);
        m.insert("expires".to_string(), true);
        m.insert("x-amz-replication-status".to_string(), true);
        m
    };
    static ref SSE_HEADERS: HashMap<String, bool> = {
        let mut m = HashMap::new();
        m.insert("x-amz-server-side-encryption".to_string(), true);
        m.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), true);
        m.insert("x-amz-server-side-encryption-context".to_string(), true);
        m.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), true);
        m.insert("x-amz-server-side-encryption-customer-key".to_string(), true);
        m.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), true);
        m
    };
}

pub fn is_standard_query_value(qs_key: &str) -> bool {
    SUPPORTED_QUERY_VALUES[qs_key]
}

pub fn is_storageclass_header(header_key: &str) -> bool {
    header_key.to_lowercase() == X_AMZ_STORAGE_CLASS.as_str().to_lowercase()
}

pub fn is_standard_header(header_key: &str) -> bool {
    *SUPPORTED_HEADERS.get(&header_key.to_lowercase()).unwrap_or(&false)
}

pub fn is_sse_header(header_key: &str) -> bool {
    *SSE_HEADERS.get(&header_key.to_lowercase()).unwrap_or(&false)
}

pub fn is_amz_header(header_key: &str) -> bool {
    let key = header_key.to_lowercase();
    key.starts_with("x-amz-meta-")
        || key.starts_with("x-amz-grant-")
        || key == "x-amz-acl"
        || is_sse_header(header_key)
        || key.starts_with("x-amz-checksum-")
}

pub fn is_rustfs_header(header_key: &str) -> bool {
    header_key.to_lowercase().starts_with("x-rustfs-")
}

pub fn is_minio_header(header_key: &str) -> bool {
    header_key.to_lowercase().starts_with("x-minio-")
}

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
