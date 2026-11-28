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

//! The fixtures the gateway stack's unit tests share: the facts RustFS's layers attach, and a
//! request signed the way a client signs it.

use crate::storage_api::server::http::request_context::RequestContext;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request};
use http_body_util::Empty;
use rustfs_gateway::sig::{AmzDate, PayloadMode, SigService, SigV4Signer, SigningCredentials, SigningRequest, SigningScope};
use rustfs_gateway::{Limits, WireRequest};
use std::time::Instant;

pub(crate) const PEER: ([u8; 4], u16) = ([10, 0, 0, 7], 41000);
pub(crate) const CLIENT: [u8; 4] = [203, 0, 113, 9];
pub(crate) const REQUEST_ID: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

/// The context RustFS's request layer attaches, naming the request `request_id`.
pub(crate) fn request_context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        x_amz_request_id: request_id.to_owned(),
        trace_id: None,
        span_id: None,
        start_time: Instant::now(),
    }
}

/// Where a signature travels.
#[derive(Clone, Copy)]
pub(crate) enum Signed {
    Header,
    Query,
}

/// `method path?query` to `localhost`, signed now for `access_key` and `secret_key`.
pub(crate) fn signed_request(
    form: Signed,
    method: Method,
    path: &str,
    query: &str,
    access_key: &str,
    secret_key: &str,
) -> Request<Empty<Bytes>> {
    signed_request_with_headers(form, method, path, query, access_key, secret_key, HeaderMap::new())
}

/// Signs the supplied headers along with the request target.
pub(crate) fn signed_request_with_headers(
    form: Signed,
    method: Method,
    path: &str,
    query: &str,
    access_key: &str,
    secret_key: &str,
    headers: HeaderMap,
) -> Request<Empty<Bytes>> {
    signed_session_request(form, method, path, query, access_key, secret_key, None, headers)
}

/// Signs an `aws-chunked` request using `STREAMING-UNSIGNED-PAYLOAD-TRAILER` and `trailer_name`.
pub(crate) fn signed_streaming_trailer_request(
    method: Method,
    path: &str,
    query: &str,
    access_key: &str,
    secret_key: &str,
    decoded_content_length: u64,
    trailer_name: &str,
) -> Request<Empty<Bytes>> {
    use hmac::{Hmac, KeyInit as _, Mac as _};
    use sha2::{Digest as _, Sha256};
    use std::collections::BTreeMap;

    const PAYLOAD_TOKEN: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    let mac = |key: &[u8], data: &str| -> Vec<u8> {
        let mut h = Hmac::<Sha256>::new_from_slice(key).expect("HMAC key");
        h.update(data.as_bytes());
        h.finalize().into_bytes().to_vec()
    };
    let hex = |bytes: &[u8]| bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>();

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let day = now.format("%Y%m%d").to_string();
    let scope = format!("{day}/us-east-1/s3/aws4_request");

    let mut headers = BTreeMap::new();
    headers.insert("content-encoding", "aws-chunked".to_owned());
    headers.insert("host", "localhost".to_owned());
    headers.insert("x-amz-content-sha256", PAYLOAD_TOKEN.to_owned());
    headers.insert("x-amz-date", amz_date.clone());
    headers.insert("x-amz-decoded-content-length", decoded_content_length.to_string());
    headers.insert("x-amz-trailer", trailer_name.to_owned());

    let canonical_headers = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect::<String>();
    let signed_headers = headers.keys().copied().collect::<Vec<_>>().join(";");
    let canonical_request = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{PAYLOAD_TOKEN}",
        method.as_str()
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let mut key = format!("AWS4{secret_key}").into_bytes();
    for part in [day.as_str(), "us-east-1", "s3", "aws4_request"] {
        key = mac(&key, part);
    }
    let signature = hex(&mac(&key, &string_to_sign));
    let authorization =
        format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}");

    let target = if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    };
    let mut builder = Request::builder().method(method).uri(target);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder
        .header(http::header::AUTHORIZATION, authorization)
        .body(Empty::new())
        .expect("signed streaming trailer request")
}

/// As [`signed_request_with_headers`], for a session whose token the signer places where the
/// signature travels: the `X-Amz-Security-Token` header or query parameter.
#[allow(clippy::too_many_arguments)] // A test fixture naming every signing input.
pub(crate) fn signed_session_request(
    form: Signed,
    method: Method,
    path: &str,
    query: &str,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    mut headers: HeaderMap,
) -> Request<Empty<Bytes>> {
    headers
        .entry(http::header::HOST)
        .or_insert(HeaderValue::from_static("localhost"));
    let probe = Request::builder()
        .method(method.clone())
        .uri(path)
        .header(http::header::HOST, headers.get(http::header::HOST).expect("a supplied or default host"))
        .body(Bytes::new())
        .expect("static request");
    let accepted = WireRequest::accept(probe, &Limits::default()).expect("an acceptable host");
    let stamp = AmzDate::parse(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()).expect("a stamp");
    let scope = SigningScope::new(stamp.day(), "us-east-1", SigService::S3).expect("a scope");
    let mut credentials = SigningCredentials::new(access_key, secret_key.as_bytes()).expect("credentials");
    if let Some(token) = session_token {
        credentials = credentials.with_session_token(rustfs_gateway::SessionToken::new(token).expect("a session token"));
    }
    let mut signer = SigV4Signer::new(credentials, scope);
    let signing = SigningRequest::new(
        &method,
        path,
        query,
        &headers,
        accepted.host().raw_for_signing(),
        PayloadMode::Unsigned,
        stamp,
    );
    let signed = match form {
        Signed::Header => signer.sign_headers(&signing),
        Signed::Query => signer.presign(&signing, 300),
    }
    .expect("a signable request");
    let mut builder = Request::builder().method(method).uri(signed.target());
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }
    builder.body(Empty::new()).expect("a request")
}

/// `method path?query` carrying `body`, header-signed now over the body's SHA-256.
pub(crate) fn signed_body_request(
    method: Method,
    path: &str,
    query: &str,
    access_key: &str,
    secret_key: &str,
    body: Bytes,
) -> Request<http_body_util::Full<Bytes>> {
    use sha2::{Digest as _, Sha256};
    let mut headers = HeaderMap::new();
    headers.insert(http::header::HOST, HeaderValue::from_static("localhost"));
    let probe = Request::builder()
        .method(method.clone())
        .uri(path)
        .header(http::header::HOST, "localhost")
        .body(Bytes::new())
        .expect("static request");
    let accepted = WireRequest::accept(probe, &Limits::default()).expect("an acceptable host");
    let stamp = AmzDate::parse(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()).expect("a stamp");
    let scope = SigningScope::new(stamp.day(), "us-east-1", SigService::S3).expect("a scope");
    let credentials = SigningCredentials::new(access_key, secret_key.as_bytes()).expect("credentials");
    let digest: [u8; 32] = Sha256::digest(&body).into();
    let hex = digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    headers.insert("x-amz-content-sha256", HeaderValue::from_str(&hex).expect("hex is a header value"));
    let signing = SigningRequest::new(
        &method,
        path,
        query,
        &headers,
        accepted.host().raw_for_signing(),
        PayloadMode::ExactSha256(digest),
        stamp,
    );
    let signed = SigV4Signer::new(credentials, scope)
        .sign_headers(&signing)
        .expect("a signable request");
    let mut builder = Request::builder()
        .method(method)
        .uri(signed.target())
        .header(http::header::CONTENT_LENGTH, body.len());
    for (name, value) in signed.headers() {
        builder = builder.header(name, value);
    }
    builder.body(http_body_util::Full::new(body)).expect("a request")
}

/// A body of `total` bytes in 64 KiB frames that declares no length, counting every byte a reader
/// has taken from it: what a `Transfer-Encoding: chunked` upload looks like to the stacks.
pub(crate) struct CountedBody {
    left: usize,
    taken: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl CountedBody {
    /// A body of `total` bytes, and the counter of the bytes read from it.
    pub(crate) fn new(total: usize) -> (Self, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let taken = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Self {
                left: total,
                taken: std::sync::Arc::clone(&taken),
            },
            taken,
        )
    }
}

impl http_body::Body for CountedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self.left == 0 {
            return std::task::Poll::Ready(None);
        }
        let frame = self.left.min(64 * 1024);
        self.left -= frame;
        self.taken.fetch_add(frame, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from(vec![b'a'; frame])))))
    }

    fn is_end_stream(&self) -> bool {
        self.left == 0
    }
}
