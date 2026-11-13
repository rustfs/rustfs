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
