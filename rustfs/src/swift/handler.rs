// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Swift HTTP handler
//!
//! This module provides the HTTP request handler that routes Swift API
//! requests and delegates to appropriate Swift handlers or falls through
//! to S3 service for non-Swift requests.

use crate::swift::container;
use crate::swift::object;
use crate::swift::{SwiftError, SwiftRoute, SwiftRouter};
use axum::http::{Method, Request, Response, StatusCode};
use futures::Future;
use rustfs_credentials::Credentials;
use rustfs_keystone::KEYSTONE_CREDENTIALS;
use s3s::Body;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;
use tracing::{debug, instrument};

/// Swift-aware service that routes to Swift handlers or S3 service
#[derive(Clone)]
pub struct SwiftService<S> {
    /// Swift router for URL parsing
    router: SwiftRouter,
    /// Underlying S3 service for fallback
    s3_service: S,
}

impl<S> SwiftService<S> {
    /// Create a new Swift service wrapping an S3 service
    pub fn new(enabled: bool, url_prefix: Option<String>, s3_service: S) -> Self {
        let router = SwiftRouter::new(enabled, url_prefix);
        Self { router, s3_service }
    }
}

impl<S, B> Service<Request<B>> for SwiftService<S>
where
    S: Service<Request<B>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.s3_service.poll_ready(cx).map_err(Into::into)
    }

    #[instrument(skip(self, req), fields(method = %req.method(), uri = %req.uri()))]
    fn call(&mut self, req: Request<B>) -> Self::Future {
        let router = self.router.clone();
        let method = req.method().clone();
        let uri = req.uri().clone();

        // Try to parse as Swift request
        if let Some(route) = router.route(&uri, method.clone()) {
            debug!("Swift route matched: {:?}", route);

            // Extract credentials from Keystone task-local storage (if available)
            // This is consistent with how S3 auth handler retrieves Keystone credentials
            let credentials = KEYSTONE_CREDENTIALS.try_with(|creds| creds.clone()).ok().flatten();

            // Handle Swift operations based on route and method
            let response_future = handle_swift_request(route, credentials);
            return Box::pin(async move {
                match response_future.await {
                    Ok(response) => Ok(response),
                    Err(swift_error) => {
                        // Convert SwiftError to Response
                        Ok(swift_error_to_response(swift_error))
                    }
                }
            });
        }

        // Not a Swift request, delegate to S3 service
        debug!("No Swift route matched, delegating to S3 service");
        let mut s3_service = self.s3_service.clone();
        Box::pin(async move { s3_service.call(req).await.map_err(Into::into) })
    }
}

/// Handle Swift API requests
async fn handle_swift_request(route: SwiftRoute, credentials: Option<Credentials>) -> Result<Response<Body>, SwiftError> {
    // Credentials are required for all Swift operations
    let credentials = credentials.ok_or_else(|| SwiftError::Unauthorized("Authentication required".to_string()))?;

    match route {
        SwiftRoute::Account { account, method } => {
            match method {
                Method::GET => {
                    // List containers
                    let containers = container::list_containers(&account, &credentials).await?;

                    // Generate JSON response
                    let json = serde_json::to_string(&containers)
                        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json; charset=utf-8")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::from(json))
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::HEAD => {
                    // Account metadata operations not yet implemented
                    Err(SwiftError::InternalServerError(format!(
                        "Swift Account HEAD operation not yet implemented: HEAD {}",
                        account
                    )))
                }
                Method::POST => {
                    // Account metadata update not yet implemented
                    Err(SwiftError::InternalServerError(format!(
                        "Swift Account POST operation not yet implemented: POST {}",
                        account
                    )))
                }
                _ => Err(SwiftError::BadRequest(format!("Unsupported method for account: {}", method))),
            }
        }
        SwiftRoute::Container {
            account,
            container,
            method,
        } => {
            match method {
                Method::PUT => {
                    // Create container
                    let is_new = container::create_container(&account, &container, &credentials).await?;

                    let trans_id = generate_trans_id();
                    let status = if is_new {
                        StatusCode::CREATED // 201 - Container created
                    } else {
                        StatusCode::ACCEPTED // 202 - Container already exists
                    };

                    Response::builder()
                        .status(status)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::GET => {
                    // List objects in container
                    let objects = container::list_objects(&account, &container, &credentials, None, None, None, None).await?;

                    // Generate JSON response
                    let json = serde_json::to_string(&objects)
                        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json; charset=utf-8")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::from(json))
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::HEAD => {
                    // Container metadata
                    let metadata = container::get_container_metadata(&account, &container, &credentials).await?;

                    let trans_id = generate_trans_id();
                    let mut response = Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-container-object-count", metadata.object_count.to_string())
                        .header("x-container-bytes-used", metadata.bytes_used.to_string())
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id.clone());

                    // Add creation timestamp if available
                    if let Some(created) = metadata.created
                        && let Ok(timestamp_str) = created.format(&time::format_description::well_known::Rfc3339)
                    {
                        response = response.header("x-timestamp", timestamp_str);
                    }

                    // Add custom metadata headers (X-Container-Meta-*)
                    for (key, value) in metadata.custom_metadata {
                        let header_name = format!("x-container-meta-{}", key.to_lowercase());
                        response = response.header(header_name, value);
                    }

                    Ok(response
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
                }
                Method::POST => {
                    // Update container metadata
                    // Note: Currently sends empty metadata because handler doesn't have access to request headers
                    // TODO: handler needs access to request headers for X-Container-Meta-* extraction
                    let metadata = std::collections::HashMap::new();

                    container::update_container_metadata(&account, &container, &credentials, metadata).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::DELETE => {
                    // Delete container
                    container::delete_container(&account, &container, &credentials).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                _ => Err(SwiftError::BadRequest(format!("Unsupported method for container: {}", method))),
            }
        }
        SwiftRoute::Object {
            account,
            container,
            object,
            method,
        } => {
            match method {
                Method::PUT => {
                    // Upload object
                    // Note: Currently cannot handle request body in this handler signature
                    // TODO: handler needs access to request body for object upload
                    Err(SwiftError::InternalServerError(
                        "Object PUT not yet integrated with handler (requires request body access)".to_string(),
                    ))
                }
                Method::GET => {
                    // Download object
                    // Note: Handler architecture doesn't support streaming response bodies
                    // The get_object function returns a stream, but this handler can only return Body::from(String)
                    // TODO: handler needs Request<B> parameter to support streaming response
                    // Current signature: handle_swift_request(route, credentials)
                    // Required: handle_swift_request(req: Request<B>, route, credentials)
                    Err(SwiftError::NotImplemented(
                        "Object GET is not yet implemented. Use HEAD for metadata.".to_string(),
                    ))
                }
                Method::HEAD => {
                    // Get object metadata
                    let info = object::head_object(&account, &container, &object, &credentials).await?;

                    let trans_id = generate_trans_id();
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", info.content_type.as_deref().unwrap_or("application/octet-stream"))
                        .header("content-length", info.size.to_string())
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id);

                    // Add ETag if available
                    if let Some(etag) = info.etag {
                        response = response.header("etag", etag);
                    }

                    // Add custom metadata headers (X-Object-Meta-*)
                    for (key, value) in info.user_defined {
                        if key != "content-type" {
                            let header_name = format!("x-object-meta-{}", key);
                            response = response.header(header_name, value);
                        }
                    }

                    response
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::POST => {
                    // Update object metadata
                    // Note: Currently sends empty headers because handler doesn't have access to request headers
                    // TODO: handler needs access to request headers for X-Object-Meta-* extraction
                    Err(SwiftError::InternalServerError(
                        "Object POST not yet integrated with handler (requires request headers access)".to_string(),
                    ))
                }
                Method::DELETE => {
                    // Delete object
                    object::delete_object(&account, &container, &object, &credentials).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                // COPY method for server-side copy
                m if m == Method::from_bytes(b"COPY").unwrap_or(Method::GET) && m.as_str() == "COPY" => {
                    // Server-side object copy
                    // Note: Requires access to Destination header from request
                    // TODO: handler needs access to request headers for Destination header parsing
                    Err(SwiftError::InternalServerError(
                        "Object COPY not yet integrated with handler (requires request headers access)".to_string(),
                    ))
                }
                _ => Err(SwiftError::BadRequest(format!("Unsupported method for object: {}", method))),
            }
        }
    }
}

/// Generate a transaction ID for Swift responses
fn generate_trans_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_micros();
    format!("tx{:x}", timestamp)
}

/// Convert SwiftError to HTTP Response
fn swift_error_to_response(error: SwiftError) -> Response<Body> {
    let trans_id = generate_trans_id();
    let (status, message) = match &error {
        SwiftError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
        SwiftError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.as_str()),
        SwiftError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.as_str()),
        SwiftError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.as_str()),
        SwiftError::Conflict(msg) => (StatusCode::CONFLICT, msg.as_str()),
        SwiftError::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.as_str()),
        SwiftError::InternalServerError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.as_str()),
        SwiftError::NotImplemented(msg) => (StatusCode::NOT_IMPLEMENTED, msg.as_str()),
        SwiftError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.as_str()),
    };

    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(message.to_string()))
        .unwrap_or_else(|_| {
            // Fallback response if builder fails
            Response::new(Body::from("Internal Server Error".to_string()))
        })
}

// Tests will be added when handler architecture supports full request access
// #[cfg(test)]
// mod tests {
//     // Tests will be re-enabled after handler refactoring
// }
