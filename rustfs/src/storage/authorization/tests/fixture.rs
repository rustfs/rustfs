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

//! Owned request fixtures for the authorization tests: a [`RequestEnvelope`] builder and the
//! IAM and KMS stand-ins a server context needs.
//!
//! Responsible for: building the request the gateway edge hands the authorization core, with
//! the same facts the legacy edge copied from its request (headers, URI, credentials and the
//! session token taken from them, extensions).
//! Not responsible for: any authorization decision, or fixtures for the legacy request type.
//! Upstream: none. Downstream: the tests of `storage::authorization` and `storage::access_owned`.

use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::auth::get_session_token;
use crate::storage::req_info::ReqInfo;
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, ECStore, IamInterface, KmsInterface, ServerContextSlot};
use http::header::{HeaderName, HeaderValue};
use http::{Extensions, HeaderMap, Method, Uri};
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_kms::KmsServiceManager;
use std::sync::Arc;

/// An IAM that is never ready: a request authorized against it resolves no identity policy.
pub(crate) struct UnreadyIam;

impl IamInterface for UnreadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        panic!("authorization fixtures built on UnreadyIam do not resolve IAM")
    }

    fn is_ready(&self) -> bool {
        false
    }
}

/// A KMS with no configured backend.
pub(crate) struct TestKms;

impl KmsInterface for TestKms {
    fn handle(&self) -> Arc<KmsServiceManager> {
        Arc::new(KmsServiceManager::new())
    }
}

/// A server context bound to `store`, with an unready IAM and an unconfigured KMS.
pub(crate) fn server_context(store: Arc<ECStore>) -> Arc<ServerContextSlot> {
    let slot = ServerContextSlot::new();
    assert!(slot.install(Arc::new(AppContext::new(store, Arc::new(UnreadyIam), Arc::new(TestKms)))));
    slot
}

/// Builds the owned request the authorization core reads.
pub(crate) struct RequestBuilder {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    credentials: Option<(String, String)>,
    extensions: Extensions,
}

/// A request with no headers, no credentials, no extensions, and the URI `/`.
pub(crate) fn request(method: Method) -> RequestBuilder {
    RequestBuilder {
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        credentials: None,
        extensions: Extensions::new(),
    }
}

impl RequestBuilder {
    pub(crate) fn uri(mut self, uri: &str) -> Self {
        self.uri = uri.parse().expect("test URI");
        self
    }

    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.append(
            HeaderName::from_bytes(name.as_bytes()).expect("test header name"),
            HeaderValue::from_str(value).expect("test header value"),
        );
        self
    }

    pub(crate) fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Signs the request as `access_key`. The session token is read from the request's
    /// headers and query when the envelope is built, as the edge reads it.
    pub(crate) fn credentials(mut self, access_key: &str, secret_key: &str) -> Self {
        self.credentials = Some((access_key.to_owned(), secret_key.to_owned()));
        self
    }

    /// Carries an empty [`ReqInfo`], as the access phase installs before any hook runs.
    pub(crate) fn req_info(self) -> Self {
        self.extension(ReqInfo::default())
    }

    pub(crate) fn server_context(self, slot: &Arc<ServerContextSlot>) -> Self {
        self.extension(slot.clone())
    }

    pub(crate) fn extension<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.extensions.insert(value);
        self
    }

    pub(crate) fn build(self) -> RequestEnvelope {
        let credentials = self.credentials.map(|(access_key, secret_key)| {
            let session_token = get_session_token(&self.uri, &self.headers).map(str::to_owned);
            RequestCredentials::new(access_key.clone(), secret_key, session_token.clone())
                .unwrap_or_else(|_| RequestCredentials::without_secret_key(access_key, session_token))
        });
        RequestEnvelope::from_parts(
            RequestHead {
                method: self.method,
                uri: self.uri,
                headers: self.headers,
                credentials,
                region: None,
                service: None,
            },
            self.extensions,
        )
    }
}
