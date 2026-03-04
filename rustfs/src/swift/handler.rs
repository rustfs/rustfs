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
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.s3_service.poll_ready(cx).map_err(Into::into)
    }

    #[instrument(skip(self, req), fields(method = %req.method(), uri = %req.uri()))]
    fn call(&mut self, req: Request<B>) -> Self::Future {
        // Try to parse as Swift request - only clone method if needed
        let method = req.method();
        let uri = req.uri();

        if let Some(route) = self.router.route(uri, method.clone()) {
            debug!("Swift route matched: {:?}", route);

            // Extract credentials from Keystone task-local storage (if available)
            // This is consistent with how S3 auth handler retrieves Keystone credentials
            let credentials = KEYSTONE_CREDENTIALS.try_with(|creds| creds.clone()).ok().flatten();

            // Handle Swift operations with full request
            let response_future = handle_swift_request(req, route, credentials);
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

/// Handle Swift API requests with full access to request data
async fn handle_swift_request<B>(
    req: Request<B>,
    route: SwiftRoute,
    credentials: Option<Credentials>,
) -> Result<Response<Body>, SwiftError>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Credentials are required for all Swift operations
    let credentials = credentials.ok_or_else(|| SwiftError::Unauthorized("Authentication required".to_string()))?;

    // Extract headers and body from request
    let (parts, body) = req.into_parts();
    let headers = parts.headers;

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
                    Err(SwiftError::NotImplemented("Swift Account HEAD operation not yet implemented".to_string()))
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
                    // Update container metadata - now we have access to request headers
                    let mut metadata = std::collections::HashMap::new();
                    for (name, value) in headers.iter() {
                        if let Some(meta_key) = name.as_str().strip_prefix("x-container-meta-")
                            && let Ok(value_str) = value.to_str()
                        {
                            metadata.insert(meta_key.to_string(), value_str.to_string());
                        }
                    }

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
                    // Upload object - collect body as bytes
                    // TODO(P1): Stream uploads directly instead of buffering entire body in memory.
                    // Current implementation buffers the entire request body before uploading,
                    // which can cause OOM on large uploads. This should be refactored to stream
                    // the body directly to put_object using a proper AsyncRead adapter.
                    // See: https://github.com/rustfs/rustfs/pull/2066#pullrequestreview-3890571917
                    use http_body_util::BodyExt;

                    // Collect the body into bytes
                    let collected = body
                        .collect()
                        .await
                        .map_err(|e| SwiftError::BadRequest(format!("Failed to read request body: {}", e)))?;
                    let body_bytes = collected.to_bytes();

                    // Create an AsyncRead reader from the bytes
                    // We use a simple adapter that wraps the bytes
                    struct BytesReader {
                        bytes: bytes::Bytes,
                        pos: usize,
                    }

                    impl tokio::io::AsyncRead for BytesReader {
                        fn poll_read(
                            mut self: std::pin::Pin<&mut Self>,
                            _cx: &mut std::task::Context<'_>,
                            buf: &mut tokio::io::ReadBuf<'_>,
                        ) -> std::task::Poll<std::io::Result<()>> {
                            let remaining = self.bytes.len() - self.pos;
                            if remaining == 0 {
                                return std::task::Poll::Ready(Ok(()));
                            }
                            let to_read = std::cmp::min(remaining, buf.remaining());
                            buf.put_slice(&self.bytes[self.pos..self.pos + to_read]);
                            self.pos += to_read;
                            std::task::Poll::Ready(Ok(()))
                        }
                    }

                    let reader = BytesReader {
                        bytes: body_bytes,
                        pos: 0,
                    };

                    let etag = object::put_object(&account, &container, &object, &credentials, reader, &headers).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::CREATED)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("etag", etag)
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::GET => {
                    // Download object - parse Range header if present
                    let range = headers
                        .get("range")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|r| object::parse_range_header(r).ok());

                    // Determine status code based on range presence before moving range
                    let status = if range.is_some() {
                        StatusCode::PARTIAL_CONTENT
                    } else {
                        StatusCode::OK
                    };

                    let reader = object::get_object(&account, &container, &object, &credentials, range).await?;

                    // Get object metadata for headers
                    let info = object::head_object(&account, &container, &object, &credentials).await?;

                    let trans_id = generate_trans_id();

                    let mut response = Response::builder()
                        .status(status)
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

                    // Convert GetObjectReader stream to Body
                    // Use ReaderStream to convert AsyncRead to Stream
                    let stream = tokio_util::io::ReaderStream::new(reader.stream);
                    let axum_body = axum::body::Body::from_stream(stream);
                    // Use http_body_unsync since axum Body doesn't implement Sync
                    let body = Body::http_body_unsync(axum_body);

                    response
                        .body(body)
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
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
                    // Update object metadata - pass headers directly since the function expects HeaderMap
                    object::update_object_metadata(&account, &container, &object, &credentials, &headers).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
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
                m if m.as_str() == "COPY" => {
                    // Server-side object copy - now we have access to request headers
                    let destination = headers
                        .get("destination")
                        .and_then(|v| v.to_str().ok())
                        .ok_or_else(|| SwiftError::BadRequest("Destination header required for COPY".to_string()))?;

                    // Validate destination header to prevent path traversal
                    if destination.contains("..") {
                        return Err(SwiftError::BadRequest("Path traversal not allowed in destination".to_string()));
                    }

                    // Parse destination: /{container}/{object}
                    // Object can have multiple path segments (e.g., /container/path/to/file.txt)
                    let destination_parts: Vec<&str> = destination.trim_start_matches('/').splitn(2, '/').collect();
                    if destination_parts.len() != 2 {
                        return Err(SwiftError::BadRequest("Destination must be /{container}/{object}".to_string()));
                    }
                    let dest_container = destination_parts[0];
                    let dest_object = destination_parts[1];

                    // Validate container and object names
                    if dest_container.is_empty() || dest_container.len() > 256 {
                        return Err(SwiftError::BadRequest("Invalid destination container name".to_string()));
                    }
                    if dest_object.is_empty() || dest_object.len() > 1024 {
                        return Err(SwiftError::BadRequest("Invalid destination object name".to_string()));
                    }

                    object::copy_object(
                        &account,
                        &container,
                        &object,
                        &account,
                        dest_container,
                        dest_object,
                        &credentials,
                        &headers,
                    )
                    .await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::CREATED)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
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
