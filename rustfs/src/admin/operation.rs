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

use crate::app::{RequestCredentials, RequestEnvelope};
use http::{HeaderMap, Method, StatusCode, Uri};
use matchit::Params;
use rustfs_s3_types::{Body, S3Result};

/// An admin request whose application-facing state is owned by RustFS.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the body and remaining request facts are consumed as later admin handlers move to the owned boundary"
    )
)]
pub(crate) struct AdminRequest {
    envelope: RequestEnvelope,
    body: Body,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the remaining accessors are used as later admin handlers move to the owned boundary"
    )
)]
impl AdminRequest {
    pub(crate) fn new(envelope: RequestEnvelope, body: Body) -> Self {
        Self { envelope, body }
    }

    pub(crate) fn envelope(&self) -> &RequestEnvelope {
        &self.envelope
    }

    pub(crate) fn method(&self) -> &Method {
        self.envelope.method()
    }

    pub(crate) fn uri(&self) -> &Uri {
        self.envelope.uri()
    }

    pub(crate) fn headers(&self) -> &HeaderMap {
        self.envelope.headers()
    }

    pub(crate) fn credentials(&self) -> Option<&RequestCredentials> {
        self.envelope.credentials()
    }

    pub(crate) fn region(&self) -> Option<&str> {
        self.envelope.region()
    }

    pub(crate) fn service(&self) -> Option<&str> {
        self.envelope.service()
    }

    pub(crate) fn into_body(self) -> Body {
        self.body
    }
}

/// The raw HTTP response produced by an owned admin operation.
pub(crate) struct AdminResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Body,
}

impl AdminResponse {
    pub(crate) fn new(status: StatusCode, body: Body) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body,
        }
    }

    pub(crate) fn with_headers(status: StatusCode, body: Body, headers: HeaderMap) -> Self {
        let mut response = Self::new(status, body);
        response.headers = headers;
        response
    }

    pub(crate) fn into_parts(self) -> (StatusCode, HeaderMap, Body) {
        (self.status, self.headers, self.body)
    }
}

#[async_trait::async_trait]
pub(crate) trait Operation: Send + Sync + 'static {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse>;
}
