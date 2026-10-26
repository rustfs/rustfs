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
use crate::storage::access::ReqInfo;
use crate::storage::request_context::RequestContext;
use http::{HeaderMap, Method, StatusCode, Uri};
use matchit::Params;
use rustfs_s3_types::{Body, S3Result};
use rustfs_trusted_proxies::ClientInfo;
use std::net::SocketAddr;

/// An admin request whose application-facing state is owned by RustFS.
pub(crate) struct AdminRequest {
    envelope: RequestEnvelope,
    body: Body,
}

impl AdminRequest {
    pub(crate) fn new(envelope: RequestEnvelope, body: Body) -> Self {
        Self { envelope, body }
    }

    pub(crate) fn envelope(&self) -> &RequestEnvelope {
        &self.envelope
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn envelope_mut(&mut self) -> &mut RequestEnvelope {
        &mut self.envelope
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

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn service(&self) -> Option<&str> {
        self.envelope.service()
    }

    /// The peer address the connection layer recorded, if any.
    pub(crate) fn remote_addr(&self) -> Option<SocketAddr> {
        self.envelope.remote_addr()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn client_info(&self) -> Option<&ClientInfo> {
        self.envelope.client_info()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn request_context(&self) -> Option<&RequestContext> {
        self.envelope.request_context()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn req_info(&self) -> Option<&ReqInfo> {
        self.envelope.req_info()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn req_info_mut(&mut self) -> Option<&mut ReqInfo> {
        self.envelope.req_info_mut()
    }

    /// Attaches the authorization-phase request info, returning any it replaces.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
    pub(crate) fn install_req_info(&mut self, req_info: ReqInfo) -> Option<ReqInfo> {
        self.envelope.install_req_info(req_info)
    }

    /// Moves the body out while the rest of the request stays readable; a
    /// second call yields an empty body.
    pub(crate) fn take_body(&mut self) -> Body {
        std::mem::take(&mut self.body)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "used as admin handlers move to the owned request boundary in T1.12 (rustfs/backlog#2788)"
        )
    )]
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

/// Builds owned admin requests for handler tests without any s3s type.
#[cfg(test)]
pub(crate) mod test_support {
    use super::AdminRequest;
    use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
    use crate::auth::get_session_token;
    use http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, Uri};
    use rustfs_s3_types::Body;
    use std::fmt::Debug;

    pub(crate) struct TestRequest {
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        extensions: Extensions,
        credentials: Option<RequestCredentials>,
        region: Option<String>,
        service: Option<String>,
        body: Body,
    }

    impl TestRequest {
        pub(crate) fn new<U>(method: Method, uri: U) -> Self
        where
            U: TryInto<Uri>,
            U::Error: Debug,
        {
            Self {
                method,
                uri: uri.try_into().expect("test request uri must parse"),
                headers: HeaderMap::new(),
                extensions: Extensions::new(),
                credentials: None,
                region: None,
                service: None,
                body: Body::empty(),
            }
        }

        pub(crate) fn headers(mut self, headers: HeaderMap) -> Self {
            self.headers = headers;
            self
        }

        pub(crate) fn header(mut self, name: &'static str, value: &str) -> Self {
            self.headers.append(
                HeaderName::from_static(name),
                HeaderValue::from_str(value).expect("test header value must be valid"),
            );
            self
        }

        /// The presented identity. Without an explicit `session_token` the
        /// token is read from the request like the s3s edge does, so a test
        /// that sets `x-amz-security-token` keeps working unchanged.
        pub(crate) fn credentials(mut self, access_key: &str, secret_key: &str, session_token: Option<&str>) -> Self {
            self.credentials = Some(RequestCredentials::new(
                access_key.to_owned(),
                secret_key.to_owned(),
                session_token.map(str::to_owned),
            ));
            self
        }

        pub(crate) fn extensions(mut self, extensions: Extensions) -> Self {
            self.extensions = extensions;
            self
        }

        pub(crate) fn extension<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
            self.extensions.insert(value);
            self
        }

        pub(crate) fn region(mut self, region: Option<String>) -> Self {
            self.region = region;
            self
        }

        pub(crate) fn service(mut self, service: Option<String>) -> Self {
            self.service = service;
            self
        }

        pub(crate) fn body(mut self, body: impl Into<Body>) -> Self {
            self.body = body.into();
            self
        }

        pub(crate) fn build(self) -> AdminRequest {
            let credentials = self.credentials.map(|credentials| match credentials.session_token() {
                Some(_) => credentials,
                None => RequestCredentials::new(
                    credentials.access_key().to_owned(),
                    credentials.secret_key().to_owned(),
                    get_session_token(&self.uri, &self.headers).map(str::to_owned),
                ),
            });
            let head = RequestHead {
                method: self.method,
                uri: self.uri,
                headers: self.headers,
                credentials,
                region: self.region,
                service: self.service,
            };
            AdminRequest::new(RequestEnvelope::from_parts(head, self.extensions), self.body)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AdminRequest;
    use super::test_support::TestRequest;
    use crate::shared_types::RemoteAddr;
    use crate::storage::access::ReqInfo;
    use http::{HeaderValue, Method};
    use http_body_util::BodyExt;
    use rustfs_s3_types::Body;
    use std::net::{IpAddr, SocketAddr};

    #[tokio::test]
    async fn take_body_leaves_the_request_readable_and_drains_once() {
        let mut req: AdminRequest = TestRequest::new(Method::POST, "/rustfs/admin/v3/probe?x=1")
            .body(Body::from("payload"))
            .build();

        let first = req.take_body().collect().await.expect("body reads").to_bytes();
        assert_eq!(first.as_ref(), b"payload");
        assert_eq!(req.uri().query(), Some("x=1"));
        assert_eq!(req.method(), &Method::POST);
        let second = req.into_body().collect().await.expect("empty body reads").to_bytes();
        assert!(second.is_empty());
    }

    #[test]
    fn test_request_reads_the_session_token_from_the_request_like_the_edge() {
        let req = TestRequest::new(Method::GET, "/rustfs/admin/v3/info")
            .header("x-amz-security-token", "from-header")
            .credentials("AKIATEST", "secret", None)
            .build();
        let credentials = req.credentials().expect("credentials were supplied");
        assert_eq!(credentials.access_key(), "AKIATEST");
        assert_eq!(credentials.secret_key(), "secret");
        assert_eq!(credentials.session_token(), Some("from-header"));

        let explicit = TestRequest::new(Method::GET, "/rustfs/admin/v3/info")
            .header("x-amz-security-token", "from-header")
            .credentials("AKIATEST", "secret", Some("explicit"))
            .build();
        assert_eq!(explicit.credentials().and_then(|c| c.session_token()), Some("explicit"));
    }

    #[test]
    fn req_info_can_be_installed_and_mutated_on_the_owned_request() {
        let peer = SocketAddr::new(IpAddr::from([10, 0, 0, 7]), 41000);
        let mut headers = http::HeaderMap::new();
        headers.insert("x-probe", HeaderValue::from_static("present"));
        let mut extensions = http::Extensions::new();
        extensions.insert(Some(RemoteAddr(peer)));
        let mut req = TestRequest::new(Method::GET, "/rustfs/admin/v3/info")
            .headers(headers)
            .extensions(extensions)
            .build();

        assert!(req.req_info().is_none());
        assert!(
            req.install_req_info(ReqInfo {
                bucket: Some("probe-bucket".to_owned()),
                ..ReqInfo::default()
            })
            .is_none()
        );
        req.req_info_mut().expect("req info was installed").is_owner = true;
        req.envelope_mut()
            .req_info_mut()
            .expect("the envelope exposes the same req info")
            .object = Some("probe-object".to_owned());

        let replaced = req
            .install_req_info(ReqInfo::default())
            .expect("the previous req info is returned");
        assert_eq!(replaced.bucket.as_deref(), Some("probe-bucket"));
        assert_eq!(replaced.object.as_deref(), Some("probe-object"));
        assert!(replaced.is_owner);
        assert!(!req.req_info().expect("the replacement is installed").is_owner);
        assert_eq!(req.headers().get("x-probe").and_then(|v| v.to_str().ok()), Some("present"));
        assert_eq!(req.remote_addr(), Some(peer));
    }

    #[test]
    fn test_request_moves_extension_payloads_into_named_facts() {
        let peer = SocketAddr::new(IpAddr::from([10, 0, 0, 7]), 41000);
        let req = TestRequest::new(Method::GET, "/rustfs/admin/v3/info")
            .extension(Some(RemoteAddr(peer)))
            .region(Some("us-east-2".to_owned()))
            .service(Some("s3".to_owned()))
            .build();
        assert_eq!(req.remote_addr(), Some(peer));
        assert_eq!(req.region(), Some("us-east-2"));
        assert_eq!(req.service(), Some("s3"));
        assert!(req.client_info().is_none());
        assert!(req.request_context().is_none());
        assert!(req.req_info().is_none());
        assert!(req.headers().get("x-missing").map(HeaderValue::to_str).is_none());
    }
}
