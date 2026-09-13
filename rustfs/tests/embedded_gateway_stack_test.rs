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

//! `RUSTFS_S3_STACK=gateway` end to end (rustfs/backlog#1752).
//!
//! Embedded servers run the legacy and the gateway stack; the tests prove that a signed PutObject
//! and GetBucketLocation succeed on the gateway stack, that the same requests answer equally on
//! both stacks, and that an unsigned, forged or unknown-key request is refused by the gateway.

#![recursion_limit = "256"]

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use hmac::{Hmac, KeyInit, Mac};
use rustfs::config::S3Stack;
use rustfs::embedded::{RustFSServer, RustFSServerBuilder, find_available_port};
use sha2::{Digest, Sha256};

mod common;

const ACCESS_KEY: &str = "gatewaystackaccess";
const SECRET_KEY: &str = "gatewaystacksecret";
const REGION: &str = "us-east-1";
const BUCKET: &str = "gateway-stack-bucket";
const MISSING_BUCKET: &str = "gateway-stack-missing";
const OBJECT_KEY: &str = "greeting.txt";
const OBJECT_BODY: &[u8] = b"hello from the gateway stack";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const XML_DECLARATION: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
/// The one message the gateway gives both credential rejections (rustfs-gateway `AuthError`).
const GATEWAY_CREDENTIAL_MESSAGE: &str = "the request was not authenticated";

/// A pinned difference between the stacks that has no `rd-` ruling yet.
///
/// The ruled divergences on record (rd-put-0001..0008, rd-ctx-0001..0006, rustfs/gateway#759) do
/// not cover GetBucketLocation; every difference below was observed by this test and must gain a
/// ruling in rustfs/gateway's request-divergence register before the default stack changes.
/// Matching is exact, so a change on either side fails the test instead of widening the pin.
fn unruled_divergence(legacy: &Answer, gateway: &Answer) -> Option<&'static str> {
    fn seen(answer: &Answer) -> (u16, Option<&str>, Option<&str>) {
        (answer.status, answer.element("Code"), answer.element("Message"))
    }
    match (seen(legacy), seen(gateway)) {
        // The gateway answers a signature mismatch as an unknown key (`render::from_auth`), so
        // the wire never confirms the access key exists.
        ((403, Some("SignatureDoesNotMatch"), Some(_)), (403, Some("InvalidAccessKeyId"), Some(GATEWAY_CREDENTIAL_MESSAGE))) => {
            Some("signature-mismatch-answered-as-invalid-access-key")
        }
        // Same code; the gateway's message is its one credential-rejection sentence.
        (
            (403, Some("InvalidAccessKeyId"), Some(message)),
            (403, Some("InvalidAccessKeyId"), Some(GATEWAY_CREDENTIAL_MESSAGE)),
        ) if message != GATEWAY_CREDENTIAL_MESSAGE => Some("credential-rejection-message"),
        _ => None,
    }
}

async fn start(stack: S3Stack) -> Option<RustFSServer> {
    let port = match find_available_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("find free port: {err}"),
    };
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key(ACCESS_KEY)
        .secret_key(SECRET_KEY)
        .s3_stack(stack)
        .build()
        .await
        .expect("start embedded server");
    Some(server)
}

fn sdk_client(server: &RustFSServer) -> Client {
    let config = Config::builder()
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .region(Region::new(REGION))
        .endpoint_url(server.endpoint())
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

/// Creates the bucket and stores one object with the SDK's signed PutObject.
async fn seed(client: &Client) {
    client.create_bucket().bucket(BUCKET).send().await.expect("create bucket");
    client
        .put_object()
        .bucket(BUCKET)
        .key(OBJECT_KEY)
        .body(ByteStream::from_static(OBJECT_BODY))
        .send()
        .await
        .expect("signed PutObject");
}

#[derive(Clone, Copy, Debug)]
enum Auth {
    Anonymous,
    Signed,
    /// A valid access key signed with the wrong secret.
    Forged,
    /// An access key IAM does not know.
    UnknownKey,
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key length is valid for HMAC");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Header SigV4 for a bodyless `GET {path}?{query_key}`, the shape GetBucketLocation uses.
fn sigv4_headers(host: &str, path: &str, query_key: &str, access_key: &str, secret_key: &str) -> Vec<(&'static str, String)> {
    let amz_date = jiff::Timestamp::now().strftime("%Y%m%dT%H%M%SZ").to_string();
    let date = amz_date.get(..8).expect("amz date has a day part").to_owned();
    let scope = format!("{date}/{REGION}/s3/aws4_request");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "GET\n{path}\n{query_key}=\nhost:{host}\nx-amz-content-sha256:{EMPTY_SHA256}\nx-amz-date:{amz_date}\n\n{signed_headers}\n{EMPTY_SHA256}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signing_key = hmac(
        &hmac(&hmac(&hmac(format!("AWS4{secret_key}").as_bytes(), &date), REGION), "s3"),
        "aws4_request",
    );
    let signature = hex(&hmac(&signing_key, &string_to_sign));
    vec![
        ("x-amz-date", amz_date),
        ("x-amz-content-sha256", EMPTY_SHA256.to_owned()),
        (
            "authorization",
            format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"),
        ),
    ]
}

/// One observed response.
#[derive(Debug)]
struct Answer {
    status: u16,
    content_type: Option<String>,
    body: String,
}

impl Answer {
    /// The text of the first `<name>` or `<name attr…>` element.
    fn element(&self, name: &str) -> Option<&str> {
        let open = format!("<{name}");
        let mut from = 0;
        let tag = loop {
            let at = self.body.get(from..)?.find(&open)? + from;
            let after = at + open.len();
            match self.body.as_bytes().get(after) {
                Some(b'>') | Some(b' ') => break after,
                _ => from = after,
            }
        };
        let start = self.body.get(tag..)?.find('>')? + tag + 1;
        let end = self.body.get(start..)?.find(&format!("</{name}>"))? + start;
        self.body.get(start..end)
    }

    /// The body with the one newline the gateway writes after the XML declaration removed.
    ///
    /// UNRULED `xml-declaration-newline`: the gateway XML writer ends its declaration with `\n`,
    /// s3s does not. Pinned (not ignored) by the differential test below; it needs an `rd-` entry
    /// before the default stack changes.
    fn document(&self) -> String {
        match self
            .body
            .strip_prefix(XML_DECLARATION)
            .and_then(|rest| rest.strip_prefix('\n'))
        {
            Some(rest) => format!("{XML_DECLARATION}{rest}"),
            None => self.body.clone(),
        }
    }

    /// The part of an answer a client acts on. Per-response identifiers (`RequestId`, `HostId`,
    /// `x-amz-request-id`, `Date`) differ between any two responses and are left out.
    fn client_view(&self) -> (u16, Option<&str>, Option<&str>, Option<&str>, Option<String>) {
        let success_body = (self.status == 200).then(|| self.document());
        (
            self.status,
            self.content_type.as_deref(),
            self.element("Code"),
            self.element("Message"),
            success_body,
        )
    }
}

async fn get_bucket_location(server: &RustFSServer, bucket: &str, auth: Auth) -> Answer {
    let endpoint = server.endpoint();
    let host = endpoint.trim_start_matches("http://").to_owned();
    let path = format!("/{bucket}");
    let headers = match auth {
        Auth::Anonymous => Vec::new(),
        Auth::Signed => sigv4_headers(&host, &path, "location", ACCESS_KEY, SECRET_KEY),
        Auth::Forged => sigv4_headers(&host, &path, "location", ACCESS_KEY, "not-the-secret-key"),
        Auth::UnknownKey => sigv4_headers(&host, &path, "location", "AKIDUNKNOWNGATEWAY", SECRET_KEY),
    };
    let client = reqwest::Client::builder().no_proxy().build().expect("http client");
    let mut request = client.get(format!("{endpoint}{path}?location"));
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send().await.expect("GetBucketLocation round trip");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.text().await.expect("response body");
    Answer {
        status,
        content_type,
        body,
    }
}

#[test]
fn gateway_stack_serves_signed_put_object_and_get_bucket_location() {
    common::run_embedded_test(gateway_stack_serves_signed_put_object_and_get_bucket_location_body);
}

async fn gateway_stack_serves_signed_put_object_and_get_bucket_location_body() {
    let Some(server) = start(S3Stack::Gateway).await else {
        return;
    };
    let client = sdk_client(&server);
    seed(&client).await;

    // The SDK's own signed GetBucketLocation goes through the gateway pipeline.
    let location = client
        .get_bucket_location()
        .bucket(BUCKET)
        .send()
        .await
        .expect("signed GetBucketLocation through the gateway stack");
    assert_eq!(location.location_constraint().map(|constraint| constraint.as_str()), Some(REGION));

    let answer = get_bucket_location(&server, BUCKET, Auth::Signed).await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.element("LocationConstraint"), Some(REGION), "{answer:?}");

    // The PutObject the stack switch left on the legacy service stored the object.
    let object = client
        .get_object()
        .bucket(BUCKET)
        .key(OBJECT_KEY)
        .send()
        .await
        .expect("GetObject");
    let stored = object.body.collect().await.expect("object body").into_bytes();
    assert_eq!(stored.as_ref(), OBJECT_BODY);

    server.shutdown().await;
}

#[test]
fn legacy_and_gateway_stacks_answer_get_bucket_location_equally() {
    common::run_embedded_test(legacy_and_gateway_stacks_answer_get_bucket_location_equally_body);
}

async fn legacy_and_gateway_stacks_answer_get_bucket_location_equally_body() {
    let Some(legacy) = start(S3Stack::Legacy).await else {
        return;
    };
    let Some(gateway) = start(S3Stack::Gateway).await else {
        legacy.shutdown().await;
        return;
    };
    seed(&sdk_client(&legacy)).await;
    seed(&sdk_client(&gateway)).await;

    // No ruled divergence applies: the rulings on record (rd-put-0001..0008, rd-ctx-0001..0006,
    // rustfs/gateway#759) concern PutObject decoding and request shapes this slice keeps on the
    // legacy stack (regional virtual hosts, absolute-form URIs, a bare `?`, non-UTF-8 headers,
    // repeated metadata). Every answer must be equal apart from the pinned unruled differences.
    let cases = [
        (BUCKET, Auth::Signed),
        (MISSING_BUCKET, Auth::Signed),
        (BUCKET, Auth::Anonymous),
        (BUCKET, Auth::Forged),
        (BUCKET, Auth::UnknownKey),
    ];
    let mut mismatches = Vec::new();
    let mut unruled = Vec::new();
    for (bucket, auth) in cases {
        let from_legacy = get_bucket_location(&legacy, bucket, auth).await;
        let from_gateway = get_bucket_location(&gateway, bucket, auth).await;
        if from_legacy.status == 200 {
            // Pin `xml-declaration-newline` on both sides so it cannot drift unseen.
            assert!(!from_legacy.body.starts_with(&format!("{XML_DECLARATION}\n")), "{from_legacy:?}");
            assert!(from_gateway.body.starts_with(&format!("{XML_DECLARATION}\n")), "{from_gateway:?}");
        }
        if from_legacy.client_view() == from_gateway.client_view() {
            continue;
        }
        match unruled_divergence(&from_legacy, &from_gateway) {
            Some(id) => unruled.push(format!("{bucket} {auth:?}: {id}")),
            None => mismatches.push(format!("{bucket} {auth:?}\n  legacy:  {from_legacy:?}\n  gateway: {from_gateway:?}")),
        }
    }
    assert!(mismatches.is_empty(), "stacks differ:\n{}", mismatches.join("\n"));
    assert_eq!(
        unruled,
        [
            format!("{BUCKET} Forged: signature-mismatch-answered-as-invalid-access-key"),
            format!("{BUCKET} UnknownKey: credential-rejection-message"),
        ],
        "the unruled differences changed; update the pin and the ruling request together"
    );

    gateway.shutdown().await;
    legacy.shutdown().await;
}

#[test]
fn gateway_stack_refuses_unsigned_forged_and_unknown_key_requests() {
    common::run_embedded_test(gateway_stack_refuses_unsigned_forged_and_unknown_key_requests_body);
}

async fn gateway_stack_refuses_unsigned_forged_and_unknown_key_requests_body() {
    let Some(server) = start(S3Stack::Gateway).await else {
        return;
    };
    seed(&sdk_client(&server)).await;

    for (auth, code) in [
        (Auth::Anonymous, "AccessDenied"),
        // The gateway answers a signature mismatch as InvalidAccessKeyId on purpose
        // (rustfs-gateway `render::from_auth`), so the wire never confirms a key exists.
        (Auth::Forged, "InvalidAccessKeyId"),
        (Auth::UnknownKey, "InvalidAccessKeyId"),
    ] {
        let answer = get_bucket_location(&server, BUCKET, auth).await;
        assert_eq!(answer.status, 403, "{auth:?}: {answer:?}");
        assert_eq!(answer.element("Code"), Some(code), "{auth:?}: {answer:?}");
        assert_eq!(answer.element("LocationConstraint"), None, "{auth:?} must not reveal the location");
    }

    server.shutdown().await;
}
