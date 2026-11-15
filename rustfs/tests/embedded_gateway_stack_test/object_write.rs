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

//! Refusal-matrix rows of migration batch D (rustfs/backlog#2765). None is served yet.

pub(crate) const MATRIX: &[super::MatrixRow] = &[];

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct WireClient<'a> {
    endpoint: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

struct WireAnswer {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

fn hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

fn mac(key: &[u8], value: &str) -> Vec<u8> {
    let mut state = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    state.update(value.as_bytes());
    state.finalize().into_bytes().to_vec()
}

impl WireClient<'_> {
    fn signing_key(&self, day: &str) -> Vec<u8> {
        let mut key = format!("AWS4{}", self.secret_key).into_bytes();
        for value in [day, "us-east-1", "s3", "aws4_request"] {
            key = mac(&key, value);
        }
        key
    }

    async fn exchange(
        &self,
        method: &str,
        path: &str,
        mut headers: BTreeMap<String, String>,
        body: &[u8],
        declared: Option<usize>,
    ) -> WireAnswer {
        let host = self.endpoint.strip_prefix("http://").expect("local HTTP listener");
        headers.insert("host".to_owned(), host.to_owned());
        headers.insert("connection".to_owned(), "close".to_owned());
        let wire_body = if let Some(length) = declared {
            headers.insert("content-length".to_owned(), length.to_string());
            body.to_vec()
        } else {
            headers.insert("transfer-encoding".to_owned(), "chunked".to_owned());
            let mut framed = format!("{:x}\r\n", body.len()).into_bytes();
            framed.extend_from_slice(body);
            framed.extend_from_slice(b"\r\n0\r\n\r\n");
            framed
        };
        let mut head = format!("{method} {path} HTTP/1.1\r\n");
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        let raw = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut socket = tokio::net::TcpStream::connect(host).await.expect("connect test listener");
            socket.write_all(head.as_bytes()).await.expect("write request headers");
            socket.write_all(&wire_body).await.expect("write request body");
            if declared.is_some_and(|length| length > body.len()) {
                socket.shutdown().await.expect("half-close a short body");
            }
            let mut bytes = Vec::new();
            socket
                .take(128 * 1024)
                .read_to_end(&mut bytes)
                .await
                .expect("read wire response");
            bytes
        })
        .await
        .expect("wire request must finish");
        let raw = String::from_utf8(raw).expect("text fixture response");
        let (head, body) = raw.split_once("\r\n\r\n").expect("HTTP response head");
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .expect("status line")
            .split_whitespace()
            .nth(1)
            .expect("status")
            .parse()
            .expect("status number");
        let headers = lines
            .map(|line| {
                let (name, value) = line.split_once(':').expect("response header");
                (name.to_ascii_lowercase(), value.trim().to_owned())
            })
            .collect();
        WireAnswer {
            status,
            headers,
            body: body.to_owned(),
        }
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        extra: &[(&str, &str)],
        declared: Option<usize>,
    ) -> WireAnswer {
        let format = time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z")
            .expect("fixed date format");
        let stamp = time::OffsetDateTime::now_utc().format(&format).expect("request date");
        let day = &stamp[..8];
        let scope = format!("{day}/us-east-1/s3/aws4_request");
        let mut headers = BTreeMap::from([
            (
                "host".to_owned(),
                self.endpoint.strip_prefix("http://").expect("HTTP endpoint").to_owned(),
            ),
            ("x-amz-date".to_owned(), stamp.clone()),
            ("x-amz-content-sha256".to_owned(), hex(&Sha256::digest(body))),
        ]);
        headers.extend(extra.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())));
        if declared.is_none() {
            headers.insert("transfer-encoding".to_owned(), "chunked".to_owned());
        }
        let canonical_headers: String = headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect();
        let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical = format!(
            "{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{}",
            headers["x-amz-content-sha256"]
        );
        let message = format!("AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}", hex(&Sha256::digest(canonical.as_bytes())));
        let signature = hex(&mac(&self.signing_key(day), &message));
        headers.insert(
            "authorization".to_owned(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
                self.access_key
            ),
        );
        self.exchange(method, path, headers, body, declared).await
    }

    async fn post(&self, bucket: &str, status: Option<&str>, redirect: Option<&str>) -> WireAnswer {
        let format = time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z")
            .expect("fixed date format");
        let now = time::OffsetDateTime::now_utc();
        let stamp = now.format(&format).expect("form date");
        let scope = format!("{}/us-east-1/s3/aws4_request", &stamp[..8]);
        let mut fields = BTreeMap::from([
            ("bucket".to_owned(), bucket.to_owned()),
            ("key".to_owned(), "post-object".to_owned()),
            ("x-amz-algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            ("x-amz-credential".to_owned(), format!("{}/{scope}", self.access_key)),
            ("x-amz-date".to_owned(), stamp.clone()),
        ]);
        if let Some(status) = status {
            fields.insert("success_action_status".to_owned(), status.to_owned());
        }
        if let Some(redirect) = redirect {
            fields.insert("success_action_redirect".to_owned(), redirect.to_owned());
        }
        let conditions: Vec<_> = fields
            .iter()
            .map(|(name, value)| serde_json::json!({(name): value}))
            .collect();
        let policy = serde_json::json!({
            "expiration": (now + time::Duration::minutes(5)).format(&time::format_description::well_known::Rfc3339).expect("policy expiry"),
            "conditions": conditions,
        });
        let encoded = base64_simd::STANDARD.encode_to_string(policy.to_string().as_bytes());
        let signature = hex(&mac(&self.signing_key(&stamp[..8]), &encoded));
        fields.insert("policy".to_owned(), encoded);
        fields.insert("x-amz-signature".to_owned(), signature);
        let boundary = "native-put-form";
        let mut body = String::new();
        for (name, value) in fields {
            body.push_str(&format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            ));
        }
        body.push_str(&format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"file\"\r\nContent-Type: application/octet-stream\r\n\r\nhello\r\n--{boundary}--\r\n"));
        self.exchange(
            "POST",
            &format!("/{bucket}"),
            BTreeMap::from([("content-type".to_owned(), format!("multipart/form-data; boundary={boundary}"))]),
            body.as_bytes(),
            Some(body.len()),
        )
        .await
    }
}

#[test]
#[serial_test::serial]
fn legacy_owned_put_preserves_refusal_order_checksums_and_short_body() {
    super::with_stack(super::LEGACY, |server| async move {
        let endpoint = server.endpoint();
        let client = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        let long_key = "k".repeat(1025);
        let sha512 = base64_simd::STANDARD.encode_to_string(sha2::Sha512::digest(b"hello"));
        for bucket in ["dn-native-wire", "sthree-dn-native-wire", "dn-native-wire-s3alias"] {
            super::create_bucket_in_the_store(bucket).await;
            for (key, extra, expected) in [
                ("bad%00key", vec![], "InvalidArgument"),
                (long_key.as_str(), vec![], "KeyTooLongError"),
                ("bad%00key", vec![("x-amz-storage-class", "INVALID")], "InvalidStorageClass"),
                ("bad-checksum", vec![("x-amz-checksum-crc32", "AAAA")], "BadDigest"),
            ] {
                let answer = client
                    .request("PUT", &format!("/{bucket}/{key}"), b"hello", &extra, Some(5))
                    .await;
                assert_eq!(answer.status, 400, "{expected}");
                assert!(answer.body.contains(&format!("<Code>{expected}</Code>")), "expected {expected}");
            }
            for extra in [
                vec![],
                vec![("x-amz-checksum-crc32", "NhCmhg==")],
                vec![
                    ("x-amz-checksum-crc32", "NhCmhg=="),
                    ("x-amz-checksum-sha256", "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="),
                ],
                vec![("x-amz-checksum-sha512", sha512.as_str())],
            ] {
                let answer = client
                    .request("PUT", &format!("/{bucket}/object"), b"hello", &extra, Some(5))
                    .await;
                assert_eq!(answer.status, 200);
                assert_eq!(
                    answer.headers.get("etag").map(String::as_str),
                    Some("\"5d41402abc4b2a76b9719d911017c592\"")
                );
                for (name, value) in extra {
                    assert_eq!(answer.headers.get(name).map(String::as_str), Some(value));
                }
            }
            let answer = client
                .request(
                    "PUT",
                    &format!("/{bucket}/trailers"),
                    b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n",
                    &[
                        ("content-encoding", "aws-chunked"),
                        ("x-amz-decoded-content-length", "5"),
                        ("x-amz-trailer", "x-amz-checksum-crc32"),
                        ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                    ],
                    None,
                )
                .await;
            assert_eq!(answer.status, 200);
            assert_eq!(answer.headers.get("x-amz-checksum-crc32").map(String::as_str), Some("NhCmhg=="));
            let answer = client.request("PUT", &format!("/{bucket}/short"), b"abc", &[], Some(5)).await;
            assert_eq!(answer.status, 400);
            assert!(answer.body.contains("<Code>IncompleteBody</Code>"));
            assert_eq!(
                client
                    .request("HEAD", &format!("/{bucket}/short"), b"", &[], Some(0))
                    .await
                    .status,
                404
            );
        }
        server.shutdown().await;
    });
}

#[test]
#[serial_test::serial]
fn legacy_owned_post_keeps_implicit_status_and_redirect_selection() {
    super::with_stack(super::LEGACY, |server| async move {
        let endpoint = server.endpoint();
        let client = WireClient {
            endpoint: &endpoint,
            access_key: server.access_key(),
            secret_key: server.secret_key(),
        };
        for bucket in ["dn-native-post", "sthree-dn-native-post", "dn-native-post-s3alias"] {
            super::create_bucket_in_the_store(bucket).await;
            for (status, expected) in [("200", 200), ("201", 201), ("204", 204)] {
                assert_eq!(client.post(bucket, Some(status), None).await.status, expected);
            }
            let answer = client.post(bucket, None, Some("https://example.invalid/after")).await;
            assert_eq!(answer.status, 303);
            assert_eq!(
                answer.headers.get("location"),
                Some(&format!(
                    "https://example.invalid/after?bucket={bucket}&key=post-object&etag=5d41402abc4b2a76b9719d911017c592"
                ))
            );
        }
        server.shutdown().await;
    });
}
