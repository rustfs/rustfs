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

pub fn base64_encode(input: &[u8]) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(input)
}

pub fn base64_decode(input: &[u8]) -> Result<Vec<u8>, base64_simd::Error> {
    base64_simd::URL_SAFE_NO_PAD.decode_to_vec(input)
}
