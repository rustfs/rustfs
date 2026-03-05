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

//! Keystone authentication middleware
//!
//! This middleware intercepts HTTP requests and checks for OpenStack Keystone
//! authentication headers (X-Auth-Token). If found, it validates the token
//! with Keystone and stores the authenticated credentials in task-local storage
//! for use by downstream authentication handlers.
//!
//! ## Authentication Flow
//!
//! 1. Check if Keystone is enabled (via global provider)
//! 2. Extract X-Auth-Token header from request
//! 3. If token present:
//!    - Validate with Keystone service
//!    - On success: Store credentials in task-local, continue processing
//!    - On failure: Return 401 Unauthorized immediately
//! 4. If no token: Pass through to standard S3 authentication
//!
//! ## Task-Local Storage
//!
//! Uses tokio task-local storage to pass credentials from middleware to
//! auth handlers without modifying request/response types. This is async-safe
//! and properly scoped to the request lifetime.

use bytes::Bytes;
use futures::Future;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use rustfs_credentials::Credentials;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::{debug, info, warn};

use crate::KeystoneAuthProvider;

// Task-local storage for Keystone credentials
// This allows passing credentials from middleware to auth handlers
// without modifying the request/response types
tokio::task_local! {
    pub static KEYSTONE_CREDENTIALS: Option<Credentials>;
}

/// Tower Layer for Keystone authentication
///
/// This layer wraps services with Keystone authentication middleware.
/// It checks for X-Auth-Token headers and validates them with OpenStack Keystone.
#[derive(Clone)]
pub struct KeystoneAuthLayer {
    keystone_auth: Option<Arc<KeystoneAuthProvider>>,
}

impl KeystoneAuthLayer {
    /// Create a new Keystone authentication layer
    ///
    /// # Arguments
    ///
    /// * `keystone_auth` - Optional Keystone auth provider. If None, middleware is disabled.
    pub fn new(keystone_auth: Option<Arc<KeystoneAuthProvider>>) -> Self {
        if keystone_auth.is_some() {
            info!("Keystone authentication middleware enabled");
        } else {
            debug!("Keystone authentication middleware disabled (no provider)");
        }
        Self { keystone_auth }
    }
}

impl<S> Layer<S> for KeystoneAuthLayer {
    type Service = KeystoneAuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        KeystoneAuthMiddleware {
            inner,
            keystone_auth: self.keystone_auth.clone(),
        }
    }
}

/// Keystone authentication middleware service
///
/// This service intercepts requests, validates Keystone tokens if present,
/// and stores authenticated credentials in task-local storage.
#[derive(Clone)]
pub struct KeystoneAuthMiddleware<S> {
    inner: S,
    keystone_auth: Option<Arc<KeystoneAuthProvider>>,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;

impl<S, B> Service<Request<Incoming>> for KeystoneAuthMiddleware<S>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        let keystone_auth = self.keystone_auth.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Check if Keystone is enabled
            let keystone_auth = match keystone_auth {
                Some(auth) => auth,
                None => {
                    // No Keystone configured, pass through to normal authentication
                    debug!("Keystone middleware: No provider configured, passing through");
                    let resp = inner.call(req).await?;
                    let (parts, body) = resp.into_parts();
                    let body: BoxBody = body.map_err(Into::into).boxed_unsync();
                    return Ok(Response::from_parts(parts, body));
                }
            };

            // Extract X-Auth-Token header
            let token = extract_keystone_token(req.headers());

            if let Some(token) = token {
                debug!("Keystone middleware: Found X-Auth-Token header, validating");

                // Validate token with Keystone
                match keystone_auth.authenticate_with_token(token).await {
                    Ok(credentials) => {
                        // Authentication successful!
                        info!("Keystone middleware: Authentication successful for user: {}", credentials.parent_user);

                        // Store credentials in task-local storage and continue processing
                        // The auth handlers will retrieve these credentials when needed
                        let resp = KEYSTONE_CREDENTIALS.scope(Some(credentials), inner.call(req)).await?;
                        let (parts, body) = resp.into_parts();
                        let body: BoxBody = body.map_err(Into::into).boxed_unsync();
                        return Ok(Response::from_parts(parts, body));
                    }
                    Err(e) => {
                        // Authentication failed - return 401 Unauthorized immediately
                        // Per Q5.A: Return 401 immediately, no fallback to local auth
                        warn!("Keystone middleware: Authentication failed: {}", e);

                        let error_xml = format!(
                            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InvalidToken</Code>
    <Message>Invalid Keystone token</Message>
    <Details>{}</Details>
</Error>"#,
                            xml_escape(&e.to_string())
                        );

                        let body: BoxBody = Full::new(Bytes::from(error_xml))
                            .map_err(|e| -> BoxError { Box::new(e) })
                            .boxed_unsync();

                        let response = Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .header("Content-Type", "application/xml")
                            .header("WWW-Authenticate", "Keystone")
                            .body(body)
                            .unwrap();

                        return Ok(response);
                    }
                }
            }

            // No Keystone token header present, pass through to normal S3 authentication
            debug!("Keystone middleware: No X-Auth-Token header, passing through to S3 auth");
            let resp = inner.call(req).await?;
            let (parts, body) = resp.into_parts();
            let body: BoxBody = body.map_err(Into::into).boxed_unsync();
            Ok(Response::from_parts(parts, body))
        })
    }
}

/// Extract Keystone token from request headers
///
/// Checks for X-Auth-Token header (Keystone v3 standard).
/// Note: X-Storage-Token (Swift) support deferred to future PR per Q4.C
fn extract_keystone_token(headers: &HeaderMap) -> Option<&str> {
    headers.get("X-Auth-Token").and_then(|v| v.to_str().ok())
    // TODO: Add X-Storage-Token support in Phase 2 (Swift API)
    // .or_else(|| headers.get("X-Storage-Token").and_then(|v| v.to_str().ok()))
}

/// Escape XML special characters to prevent injection
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeystoneClient, KeystoneVersion};
    use std::time::Duration;

    #[test]
    fn test_layer_creation_no_keystone() {
        // Test that layer can be created without Keystone provider
        let layer = KeystoneAuthLayer::new(None);
        assert!(layer.keystone_auth.is_none());
    }

    #[test]
    fn test_layer_creation_with_keystone() {
        // Test that layer can be created with Keystone provider
        let client = KeystoneClient::new(
            "http://localhost:5000".to_string(),
            KeystoneVersion::V3,
            None,
            None,
            None,
            "Default".to_string(),
            true,
        );
        let provider = KeystoneAuthProvider::new(client, 100, Duration::from_secs(60), true);
        let layer = KeystoneAuthLayer::new(Some(Arc::new(provider)));
        assert!(layer.keystone_auth.is_some());
    }

    #[tokio::test]
    async fn test_extract_keystone_token() {
        let mut headers = HeaderMap::new();
        assert!(extract_keystone_token(&headers).is_none());

        headers.insert("X-Auth-Token", "test-token-123".parse().unwrap());
        assert_eq!(extract_keystone_token(&headers), Some("test-token-123"));
    }

    #[tokio::test]
    async fn test_xml_escape() {
        assert_eq!(xml_escape("normal text"), "normal text");
        assert_eq!(xml_escape("<tag>"), "&lt;tag&gt;");
        assert_eq!(xml_escape("a&b"), "a&amp;b");
        assert_eq!(xml_escape("it's \"quoted\""), "it&apos;s &quot;quoted&quot;");
    }

    #[tokio::test]
    async fn test_task_local_scope() {
        // Verify that task-local storage works correctly
        use rustfs_credentials::Credentials;

        let creds = Credentials {
            access_key: "test-key".to_string(),
            parent_user: "test-user".to_string(),
            ..Default::default()
        };

        // Should be None outside of scope
        assert!(KEYSTONE_CREDENTIALS.try_with(|c| c.clone()).is_err());

        // Should be Some inside scope
        KEYSTONE_CREDENTIALS
            .scope(Some(creds.clone()), async {
                let stored = KEYSTONE_CREDENTIALS.try_with(|c| c.clone()).unwrap();
                assert!(stored.is_some());
                assert_eq!(stored.unwrap().access_key, "test-key");
            })
            .await;

        // Should be None again after scope
        assert!(KEYSTONE_CREDENTIALS.try_with(|c| c.clone()).is_err());
    }

    // Note: test_valid_token and test_invalid_token require mock Keystone server
    // These will be added in Task 3.3 (Integration Testing)
}
