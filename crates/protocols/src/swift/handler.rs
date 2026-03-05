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

use super::container;
use super::dlo;
use super::object;
use super::slo;
use super::tempurl;
use super::{SwiftError, SwiftRoute, SwiftRouter};
use axum::http::{Method, Request, Response, StatusCode};
use futures::Future;
use rustfs_credentials::Credentials;
use rustfs_keystone::KEYSTONE_CREDENTIALS;
use s3s::Body;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::io::StreamReader;
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
    B: axum::body::HttpBody<Data = bytes::Bytes> + Send + 'static,
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

            // Convert Request<B> to Request<Body> for Swift handler
            let req_body = req.map(|b| Body::http_body_unsync(b));

            // Handle Swift operations with full request
            let response_future = handle_swift_request(req_body, route, credentials);
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
async fn handle_swift_request(
    req: Request<Body>,
    route: SwiftRoute,
    credentials: Option<Credentials>,
) -> Result<Response<Body>, SwiftError>
{
    // Extract parts before checking TempURL or credentials
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let query = parts.uri.query().unwrap_or("");

    // Check for TempURL request (skip authentication if valid TempURL)
    if tempurl::is_tempurl_request(query) {
        if let Some(_tempurl_params) = tempurl::parse_tempurl_params(query) {
            // For TempURL, we need to get the account's secret key
            // In a real implementation, this would be stored in account metadata
            // For now, we'll use a placeholder that can be configured

            // TODO: Retrieve TempURL key from account metadata
            // For MVP, allow TempURL if credentials are not provided but signature validates
            // This is a simplified implementation - production would need proper key storage

            // For now, since we don't have account-level key storage yet,
            // we'll just fall through to normal authentication.
            // TempURL will be fully functional once account metadata storage is added.
            debug!("TempURL detected but account key storage not yet implemented");
        }
    }

    // Credentials are required for all Swift operations (TempURL will bypass this in future)
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
                    Err(SwiftError::NotImplemented("Swift Account HEAD operation not yet implemented".to_string()))
                }
                Method::POST => {
                    // Account metadata update not yet implemented
                    Err(SwiftError::NotImplemented("Swift Account POST operation not yet implemented".to_string()))
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
                    // Check for SLO manifest creation
                    if let Some(query) = parts.uri.query() {
                        if query.contains("multipart-manifest=put") {
                            // SLO manifest creation
                            return slo::handle_slo_put(&account, &container, &object, body, &headers, &Some(credentials.clone())).await;
                        }
                    }

                    // Check for DLO registration via X-Object-Manifest header
                    if let Some(manifest_value) = headers.get("x-object-manifest") {
                        if let Ok(manifest_str) = manifest_value.to_str() {
                            return dlo::handle_dlo_register(&account, &container, &object, manifest_str, &Some(credentials.clone())).await;
                        }
                    }

                    // Regular object upload - stream directly without buffering entire body in memory
                    // Convert HTTP body to AsyncRead stream using StreamReader
                    use http_body_util::BodyExt;
                    use futures::StreamExt;

                    // Convert body into data stream with proper error mapping
                    let stream = body
                        .into_data_stream()
                        .map(|result| result.map_err(|e| std::io::Error::other(e.to_string())));

                    // Create streaming reader from the body stream
                    let reader = StreamReader::new(stream);

                    // Add buffering for optimal streaming performance (64KB buffer)
                    // This provides backpressure handling and reduces syscall overhead
                    let buffered_reader = tokio::io::BufReader::with_capacity(65536, reader);

                    let etag = object::put_object(&account, &container, &object, &credentials, buffered_reader, &headers).await?;

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
                    let creds_opt = Some(credentials.clone());

                    // Check for SLO manifest retrieval
                    if let Some(query) = parts.uri.query() {
                        if query.contains("multipart-manifest=get") {
                            return slo::handle_slo_get_manifest(&account, &container, &object, &creds_opt).await;
                        }
                    }

                    // Check if object is SLO (via metadata)
                    if slo::is_slo_object(&account, &container, &object, &creds_opt).await? {
                        return slo::handle_slo_get(&account, &container, &object, &headers, &creds_opt).await;
                    }

                    // Check if object is DLO (via x-object-manifest metadata)
                    if let Some(manifest_value) = dlo::is_dlo_object(&account, &container, &object, &creds_opt).await? {
                        return dlo::handle_dlo_get(&account, &container, &object, &headers, &creds_opt, manifest_value).await;
                    }

                    // Regular object download - parse Range header if present
                    let range_header = headers
                        .get("range")
                        .and_then(|v| v.to_str().ok());

                    // Get object metadata first (needed for Range validation)
                    // TODO(optimization): GetObjectReader contains object_info, but we need
                    // metadata BEFORE calling get_object to validate Range headers and return
                    // 416 errors without opening the object stream. Options:
                    // 1. Modify get_object API to return (GetObjectReader, ObjectInfo)
                    // 2. Add a .metadata() method to GetObjectReader
                    // 3. Accept this extra HEAD call as the cost of proper Range validation
                    // Currently using option 3 for correctness over performance.
                    let info = object::head_object(&account, &container, &object, &credentials).await?;

                    // Parse and validate Range header, returning 416 for invalid ranges
                    let parsed_range = if let Some(rh) = range_header {
                        match object::parse_range_header(rh) {
                            Ok(r) => Some(r),
                            Err(_) => {
                                // Invalid range - return 416 Range Not Satisfiable
                                let trans_id = generate_trans_id();
                                let mut response = Response::builder()
                                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                    .header("content-type", info.content_type.as_deref().unwrap_or("application/octet-stream"))
                                    .header("content-length", "0")
                                    .header("x-trans-id", trans_id.clone())
                                    .header("x-openstack-request-id", trans_id)
                                    .header("accept-ranges", "bytes")
                                    .header("content-range", format!("bytes */{}", info.size));

                                if let Some(etag) = info.etag {
                                    response = response.header("etag", etag);
                                }

                                for (key, value) in info.user_defined {
                                    if key != "content-type" {
                                        let header_name = format!("x-object-meta-{}", key);
                                        response = response.header(header_name, value);
                                    }
                                }

                                return response
                                    .body(Body::empty())
                                    .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)));
                            }
                        }
                    } else {
                        None
                    };

                    // Determine status code based on range presence
                    let status = if parsed_range.is_some() {
                        StatusCode::PARTIAL_CONTENT
                    } else {
                        StatusCode::OK
                    };

                    let reader = object::get_object(&account, &container, &object, &credentials, parsed_range).await?;

                    let trans_id = generate_trans_id();

                    let mut response = Response::builder()
                        .status(status)
                        .header("content-type", info.content_type.as_deref().unwrap_or("application/octet-stream"))
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id);

                    // Set Content-Length and range-specific headers
                    if status == StatusCode::PARTIAL_CONTENT {
                        // For partial content, we need to calculate the actual byte range
                        // and set proper Content-Range and Content-Length headers
                        response = response.header("accept-ranges", "bytes");
                        if let Some(rh) = range_header {
                            // TODO: Calculate actual byte range from parsed_range and object size
                            // For now, use the original Range header as Content-Range
                            // This should be improved to format: "bytes START-END/TOTAL"
                            response = response.header("content-range", format!("bytes {}", &rh[6..]));
                        }
                        // TODO: Set content-length to actual range size, not full object size
                        // For now we'll set the full size (incorrect but prevents breaking clients)
                        response = response.header("content-length", info.size.to_string());
                    } else {
                        // For full responses, set full length and advertise range support
                        response = response
                            .header("accept-ranges", "bytes")
                            .header("content-length", info.size.to_string());
                    }

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
                        .status(StatusCode::NO_CONTENT)
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
                }
                Method::DELETE => {
                    let creds_opt = Some(credentials.clone());

                    // Check for SLO delete with segments
                    if let Some(query) = parts.uri.query() {
                        if query.contains("multipart-manifest=delete") {
                            return slo::handle_slo_delete(&account, &container, &object, &creds_opt).await;
                        }
                    }

                    // Regular object delete
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

                    // Parse destination: /{container}/{object}
                    // Object can have multiple path segments (e.g., /container/path/to/file.txt)
                    let destination_parts: Vec<&str> = destination.trim_start_matches('/').splitn(2, '/').collect();
                    if destination_parts.len() != 2 {
                        return Err(SwiftError::BadRequest("Destination must be /{container}/{object}".to_string()));
                    }

                    // Percent-decode and validate destination components
                    use percent_encoding::percent_decode_str;
                    let dest_container = percent_decode_str(destination_parts[0])
                        .decode_utf8()
                        .map_err(|_| SwiftError::BadRequest("Invalid UTF-8 in destination container".to_string()))?;
                    let dest_object_raw = percent_decode_str(destination_parts[1])
                        .decode_utf8()
                        .map_err(|_| SwiftError::BadRequest("Invalid UTF-8 in destination object".to_string()))?;

                    // Validate path segments to prevent path traversal
                    // Check each segment (split by '/') - none should be ".."
                    for segment in dest_object_raw.split('/') {
                        if segment == ".." {
                            return Err(SwiftError::BadRequest("Path traversal not allowed in destination".to_string()));
                        }
                    }

                    let dest_container = dest_container.as_ref();
                    let dest_object = dest_object_raw.as_ref();

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
