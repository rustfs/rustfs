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

//! Black-box data-plane tests for `RUSTFS_API_PREFIX`.
//!
//! The server is expected to serve S3 under the configured prefix with the
//! request path passed through unchanged by the proxy (SigV4 signs the full
//! wire path), while the unprefixed endpoint keeps working (additive).
//!
//! The API prefix is a process-wide `OnceLock`, so this binary configures
//! exactly one prefix and runs every scenario against one server, serially.

#![recursion_limit = "256"]

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{Client, Config};
use reqwest::StatusCode;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use std::time::Duration;
use temp_env::async_with_vars;

mod common;

const API_PREFIX: &str = "/tools/rustfs/api";
const ENV_API_PREFIX: &str = "RUSTFS_API_PREFIX";
const ENV_CONSOLE_PREFIX: &str = "RUSTFS_CONSOLE_PREFIX";

fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    let config = Config::builder()
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

fn presign_config() -> PresigningConfig {
    PresigningConfig::expires_in(Duration::from_secs(300)).expect("valid presigning config")
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build reqwest client without proxy")
}

/// Sends a request to a presigned URL, replaying the headers the SDK signed.
async fn send_presigned<'a>(
    http: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    headers: impl Iterator<Item = (&'a str, &'a str)>,
    body: Option<Vec<u8>>,
) -> (StatusCode, String) {
    let mut req = http.request(method, url);
    for (name, value) in headers {
        req = req.header(name, value);
    }
    if let Some(body) = body {
        req = req.body(body);
    }
    let resp = req.send().await.expect("presigned request should reach the server");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    (status, text)
}

fn preview(body: &str) -> String {
    body.chars().take(512).collect()
}

/// Precondition for AC-1..AC-3: the SDK must keep the endpoint path when it
/// builds path-style URLs. Needs no server.
#[tokio::test]
async fn test_precondition_sdk_keeps_endpoint_path_in_path_style_urls() {
    // aws-sdk-s3 concatenates the endpoint path and the bucket without adding a
    // separator, so an endpoint path needs a trailing slash (a prefix without it
    // yields `/tools/rustfs/apibkt/...`). Clients must configure it that way.
    let endpoint = format!("http://127.0.0.1:9{API_PREFIX}/");
    let client = s3_client(&endpoint, "ak", "sk");
    let presigned = client
        .get_object()
        .bucket("bkt")
        .key("dir/obj.txt")
        .presigned(presign_config())
        .await
        .expect("presign get");
    let uri = presigned.uri().to_string();
    assert!(
        uri.starts_with(&format!("{endpoint}bkt/dir/obj.txt?")),
        "SDK dropped the endpoint path from the path-style URL: {uri}"
    );
}

#[cfg(debug_assertions)]
#[test]
fn test_api_prefix_data_plane() {
    common::run_embedded_test(test_api_prefix_data_plane_body);
}

#[cfg(debug_assertions)]
async fn test_api_prefix_data_plane_body() {
    async_with_vars([(ENV_API_PREFIX, Some(API_PREFIX)), (ENV_CONSOLE_PREFIX, None::<&str>)], async {
        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("find free port: {err}"),
        };
        let server = RustFSServerBuilder::new()
            .address(format!("127.0.0.1:{port}"))
            .access_key("testaccesskey")
            .secret_key("testsecretkey")
            .build()
            .await
            .expect("start embedded server with RUSTFS_API_PREFIX");

        let base = server.endpoint();
        // Trailing slash: aws-sdk-s3 joins endpoint path and bucket without a separator.
        let prefixed = format!("{base}{API_PREFIX}/");
        let prefixed_client = s3_client(&prefixed, server.access_key(), server.secret_key());
        let plain_client = s3_client(&base, server.access_key(), server.secret_key());
        let http = http_client();

        // Wait for readiness (unprefixed probe path, additive semantics).
        wait_ready(&http, &base).await;

        ac1_header_sigv4_operations_under_prefix(&prefixed_client).await;
        ac2_presigned_get_and_put_under_prefix(&prefixed_client, &http, &base).await;
        ac3_signature_binds_the_wire_path(&prefixed_client, &plain_client, &http, &base).await;
        ac4_unprefixed_endpoint_and_health_still_work(&plain_client, &http, &base).await;

        server.shutdown().await;
    })
    .await;
}

async fn wait_ready(http: &reqwest::Client, base: &str) {
    let url = format!("{base}/health/ready");
    let ok = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(resp) = http.get(&url).send().await
                && resp.status() == StatusCode::OK
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(ok.is_ok(), "server did not become ready at {url}");
}

/// AC-1: every data-plane call works with header SigV4 through the prefix.
async fn ac1_header_sigv4_operations_under_prefix(client: &Client) {
    let bucket = "ac1-bucket";
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("AC-1 CreateBucket under prefix");

    let body = b"hello through the api prefix".to_vec();
    client
        .put_object()
        .bucket(bucket)
        .key("dir/small.txt")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("AC-1 PutObject under prefix");

    // Multipart: two parts, the first at the 5 MiB minimum.
    let part1: Vec<u8> = (0..5 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let part2: Vec<u8> = b"tail of the multipart object".to_vec();
    let created = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("big/multipart.bin")
        .send()
        .await
        .expect("AC-1 CreateMultipartUpload under prefix");
    let upload_id = created.upload_id().expect("upload id").to_string();
    let mut completed = Vec::new();
    for (idx, data) in [part1.clone(), part2.clone()].into_iter().enumerate() {
        let number = (idx + 1) as i32;
        let out = client
            .upload_part()
            .bucket(bucket)
            .key("big/multipart.bin")
            .upload_id(&upload_id)
            .part_number(number)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap_or_else(|e| panic!("AC-1 UploadPart {number} under prefix: {e:?}"));
        completed.push(
            CompletedPart::builder()
                .part_number(number)
                .set_e_tag(out.e_tag().map(str::to_string))
                .build(),
        );
    }
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("big/multipart.bin")
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await
        .expect("AC-1 CompleteMultipartUpload under prefix");

    let listed = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("AC-1 ListObjectsV2 under prefix");
    let keys: Vec<&str> = listed.contents().iter().filter_map(|o| o.key()).collect();
    assert!(keys.contains(&"dir/small.txt"), "listing missing small object: {keys:?}");
    assert!(keys.contains(&"big/multipart.bin"), "listing missing multipart object: {keys:?}");

    let head = client
        .head_object()
        .bucket(bucket)
        .key("dir/small.txt")
        .send()
        .await
        .expect("AC-1 HeadObject under prefix");
    assert_eq!(head.content_length(), Some(body.len() as i64));

    let got = client
        .get_object()
        .bucket(bucket)
        .key("dir/small.txt")
        .send()
        .await
        .expect("AC-1 GetObject under prefix")
        .body
        .collect()
        .await
        .expect("read body")
        .into_bytes();
    assert_eq!(got.as_ref(), body.as_slice(), "AC-1 GetObject bytes must equal uploaded bytes");

    let got_mp = client
        .get_object()
        .bucket(bucket)
        .key("big/multipart.bin")
        .send()
        .await
        .expect("AC-1 GetObject multipart under prefix")
        .body
        .collect()
        .await
        .expect("read multipart body")
        .into_bytes();
    let mut expected_mp = part1;
    expected_mp.extend_from_slice(&part2);
    assert_eq!(got_mp.as_ref(), expected_mp.as_slice(), "AC-1 multipart bytes must round-trip");

    let buckets = client.list_buckets().send().await.expect("AC-1 ListBuckets under prefix");
    assert!(
        buckets.buckets().iter().any(|b| b.name() == Some(bucket)),
        "AC-1 ListBuckets must include {bucket}"
    );

    for key in ["dir/small.txt", "big/multipart.bin"] {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap_or_else(|e| panic!("AC-1 DeleteObject {key} under prefix: {e:?}"));
    }
    assert!(
        client.head_object().bucket(bucket).key("dir/small.txt").send().await.is_err(),
        "AC-1 object must be gone after DeleteObject"
    );
}

/// AC-2: presigned GET and PUT work through the prefix; removing the prefix
/// from the URL breaks the signature.
async fn ac2_presigned_get_and_put_under_prefix(client: &Client, http: &reqwest::Client, base: &str) {
    let bucket = "ac2-bucket";
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("AC-2 CreateBucket under prefix");

    let payload = b"presigned payload".to_vec();
    let put = client
        .put_object()
        .bucket(bucket)
        .key("presigned/put.txt")
        .presigned(presign_config())
        .await
        .expect("AC-2 presign PUT");
    assert!(
        put.uri().starts_with(&format!("{base}{API_PREFIX}/{bucket}/")),
        "presigned PUT URL must carry the prefix: {}",
        put.uri()
    );
    let (status, text) = send_presigned(http, reqwest::Method::PUT, put.uri(), put.headers(), Some(payload.clone())).await;
    assert!(status.is_success(), "AC-2 presigned PUT: {status} {}", preview(&text));

    let get = client
        .get_object()
        .bucket(bucket)
        .key("presigned/put.txt")
        .presigned(presign_config())
        .await
        .expect("AC-2 presign GET");
    let (status, text) = send_presigned(http, reqwest::Method::GET, get.uri(), get.headers(), None).await;
    assert_eq!(status, StatusCode::OK, "AC-2 presigned GET: {}", preview(&text));
    assert_eq!(text.as_bytes(), payload.as_slice(), "AC-2 presigned GET body must equal the PUT body");

    // Prefix removed from the signed URL => the wire path no longer matches the signature.
    let stripped = get.uri().replacen(&format!("{base}{API_PREFIX}"), base, 1);
    assert_ne!(stripped, get.uri(), "test must actually remove the prefix");
    let (status, text) = send_presigned(http, reqwest::Method::GET, &stripped, get.headers(), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "AC-2 prefix-less presigned GET: {}", preview(&text));
    assert!(
        text.contains("SignatureDoesNotMatch"),
        "AC-2 expected SignatureDoesNotMatch, got: {}",
        preview(&text)
    );
}

/// AC-3: the signature binds the exact wire path, in both directions.
async fn ac3_signature_binds_the_wire_path(prefixed: &Client, plain: &Client, http: &reqwest::Client, base: &str) {
    // Object created via the plain endpoint so a mismatch is the only reason to fail.
    let bucket = "ac3-bucket";
    plain
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("AC-3 CreateBucket on plain endpoint");
    plain
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from_static(b"v"))
        .send()
        .await
        .expect("AC-3 PutObject on plain endpoint");

    // Signed for the plain endpoint, sent with the prefix added.
    let signed_plain = plain
        .get_object()
        .bucket(bucket)
        .key("k")
        .presigned(presign_config())
        .await
        .expect("AC-3 presign plain GET");
    let with_prefix = signed_plain.uri().replacen(base, &format!("{base}{API_PREFIX}"), 1);
    assert_ne!(with_prefix, signed_plain.uri());
    let (status, text) = send_presigned(http, reqwest::Method::GET, &with_prefix, signed_plain.headers(), None).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "AC-3 plain-signed URL sent with prefix: {}",
        preview(&text)
    );
    assert!(
        text.contains("SignatureDoesNotMatch"),
        "AC-3 expected SignatureDoesNotMatch, got: {}",
        preview(&text)
    );

    // Sanity: the same plain-signed URL, unmodified, is accepted.
    let (status, text) = send_presigned(http, reqwest::Method::GET, signed_plain.uri(), signed_plain.headers(), None).await;
    assert_eq!(status, StatusCode::OK, "AC-3 plain-signed URL unmodified: {}", preview(&text));

    // Signed for the prefixed endpoint, sent without the prefix.
    let signed_prefixed = prefixed
        .get_object()
        .bucket(bucket)
        .key("k")
        .presigned(presign_config())
        .await
        .expect("AC-3 presign prefixed GET");
    let without_prefix = signed_prefixed.uri().replacen(&format!("{base}{API_PREFIX}"), base, 1);
    assert_ne!(without_prefix, signed_prefixed.uri());
    let (status, text) = send_presigned(http, reqwest::Method::GET, &without_prefix, signed_prefixed.headers(), None).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "AC-3 prefixed-signed URL sent without prefix: {}",
        preview(&text)
    );
    assert!(
        text.contains("SignatureDoesNotMatch"),
        "AC-3 expected SignatureDoesNotMatch, got: {}",
        preview(&text)
    );

    // Sanity: the same prefixed-signed URL, unmodified, is accepted.
    let (status, text) = send_presigned(http, reqwest::Method::GET, signed_prefixed.uri(), signed_prefixed.headers(), None).await;
    assert_eq!(status, StatusCode::OK, "AC-3 prefixed-signed URL unmodified: {}", preview(&text));
}

/// AC-4: additive semantics, unprefixed S3 and `/health` keep working.
async fn ac4_unprefixed_endpoint_and_health_still_work(plain: &Client, http: &reqwest::Client, base: &str) {
    let bucket = "ac4-bucket";
    plain
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("AC-4 CreateBucket without prefix");
    plain
        .put_object()
        .bucket(bucket)
        .key("plain.txt")
        .body(ByteStream::from_static(b"plain"))
        .send()
        .await
        .expect("AC-4 PutObject without prefix");
    let got = plain
        .get_object()
        .bucket(bucket)
        .key("plain.txt")
        .send()
        .await
        .expect("AC-4 GetObject without prefix")
        .body
        .collect()
        .await
        .expect("read body")
        .into_bytes();
    assert_eq!(got.as_ref(), b"plain");
    let buckets = plain.list_buckets().send().await.expect("AC-4 ListBuckets without prefix");
    assert!(buckets.buckets().iter().any(|b| b.name() == Some(bucket)));

    let resp = http.get(format!("{base}/health")).send().await.expect("AC-4 GET /health");
    assert_eq!(resp.status(), StatusCode::OK, "AC-4 GET /health");
}
