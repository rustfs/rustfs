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

//! Shared Key signing for Azure Blob REST requests.

use http::HeaderMap;
use rustfs_utils::crypto::hmac_sha256;
use std::collections::BTreeMap;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum AzureSignError {
    #[error("Azure Shared Key signing requires a non-empty account key")]
    EmptyKey,
    #[error("Azure Shared Key header {0} is not valid ASCII")]
    InvalidHeader(String),
    #[error("Azure Shared Key header {0} has multiple values")]
    DuplicateHeader(&'static str),
}

/// Sign an Azure Blob request using a decoded storage-account key.
///
/// The caller supplies the final URL and headers, including x-ms-date and
/// x-ms-version. Content headers and conditions are signed for writes as well
/// as reads. This implements the Shared Key format for service versions
/// 2015-02-21 and newer, where a zero Content-Length is signed as an empty
/// field. The returned base64 signature does not include the account name.
///
/// # Errors
///
/// Rejects an empty key, non-ASCII signed headers and repeated standard
/// headers instead of signing a different request from the one on the wire.
pub fn azure_shared_key_signature(
    key: &[u8],
    account: &str,
    method: &str,
    url: &Url,
    headers: &HeaderMap,
) -> Result<String, AzureSignError> {
    if key.is_empty() {
        return Err(AzureSignError::EmptyKey);
    }
    let mut canonical = String::with_capacity(512);
    canonical.push_str(method);
    canonical.push('\n');
    for name in [
        "content-encoding",
        "content-language",
        "content-length",
        "content-md5",
        "content-type",
        "date",
        "if-modified-since",
        "if-match",
        "if-none-match",
        "if-unmodified-since",
        "range",
    ] {
        let mut values = headers.get_all(name).iter();
        let value = values.next();
        if values.next().is_some() {
            return Err(AzureSignError::DuplicateHeader(name));
        }
        let value = value
            .map(|value| {
                value
                    .to_str()
                    .map(str::trim)
                    .map_err(|_| AzureSignError::InvalidHeader(name.to_string()))
            })
            .transpose()?
            .unwrap_or_default();
        if !(name == "content-length" && value == "0" || name == "date" && headers.contains_key("x-ms-date")) {
            canonical.push_str(value);
        }
        canonical.push('\n');
    }

    let mut service_headers: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        if name.as_str().starts_with("x-ms-") {
            let value = value.to_str().map_err(|_| AzureSignError::InvalidHeader(name.to_string()))?;
            service_headers
                .entry(name.as_str())
                .or_default()
                .push(value.split_ascii_whitespace().collect::<Vec<_>>().join(" "));
        }
    }
    for (name, values) in service_headers {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
    }
    canonical.push('/');
    canonical.push_str(account);
    canonical.push_str(url.path());
    let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in url.query_pairs() {
        query.entry(name.to_ascii_lowercase()).or_default().push(value.into_owned());
    }
    for (name, mut values) in query {
        values.sort();
        canonical.push('\n');
        canonical.push_str(&name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
    }
    Ok(base64_simd::STANDARD.encode_to_string(hmac_sha256(key, canonical.as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn signs_write_content_and_conditions_with_independent_hmac_vectors() {
        let url = Url::parse("https://account.blob.core.windows.net/container/blob?comp=block&blockid=AAAA")
            .expect("fixture URL should parse");
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("content-length", "3"),
            ("content-md5", "YWJj"),
            ("content-type", "application/octet-stream"),
            ("if-none-match", "*"),
            ("x-ms-date", "Fri, 06 Nov 2026 01:00:00 GMT"),
            ("x-ms-version", "2020-10-02"),
        ] {
            headers.insert(name, HeaderValue::from_static(value));
        }
        // Fixed vectors generated with Python's hmac/hashlib from the Azure
        // documented canonical fields, independently of this implementation.
        for (length, expected) in [
            ("3", "TMvdha8PzCSf0SB3v9EQSv0Se1gpdu1HusHBamwkyws="),
            ("0", "kxBwUHkI/MK9beMd85Mij0MvaAlBtQQVYo9Qvl9rams="),
        ] {
            headers.insert("content-length", HeaderValue::from_static(length));
            let signature = azure_shared_key_signature(b"0123456789abcdef0123456789abcdef", "account", "PUT", &url, &headers)
                .expect("valid write should sign");
            assert_eq!(signature, expected);
        }
    }

    #[test]
    fn rejects_ambiguous_headers_without_exposing_their_values() {
        let url = Url::parse("https://account.blob.core.windows.net/container/blob").expect("fixture URL should parse");
        let mut headers = HeaderMap::new();
        headers.append("content-length", HeaderValue::from_static("1"));
        headers.append("content-length", HeaderValue::from_static("2"));
        assert!(matches!(
            azure_shared_key_signature(b"key", "account", "PUT", &url, &headers),
            Err(AzureSignError::DuplicateHeader("content-length"))
        ));
        headers.clear();
        headers.insert(
            "x-ms-meta-value",
            HeaderValue::from_bytes(b"\xff-private").expect("opaque header is legal HTTP"),
        );
        let err =
            azure_shared_key_signature(b"key", "account", "PUT", &url, &headers).expect_err("opaque signed header must fail");
        assert_eq!(err.to_string(), "Azure Shared Key header x-ms-meta-value is not valid ASCII");
    }
}
