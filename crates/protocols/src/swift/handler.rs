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
    // Extract parts
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let uri = parts.uri.clone();
    let headers = parts.headers.clone();

    // Check for TempURL before requiring authentication
    // TempURL only applies to Object operations (GET, HEAD, PUT)
    if let SwiftRoute::Object { ref account, ref container, ref object, .. } = route {
        if let Some(query) = uri.query() {
            if let Some(tempurl_params) = tempurl::TempURLParams::from_query(query) {
                // TempURL detected - validate it
                debug!("TempURL detected for {}/{}/{}", account, container, object);

                // Get account TempURL key
                let tempurl_key = super::account::get_tempurl_key(account, &credentials).await?;

                if let Some(key) = tempurl_key {
                    // Validate TempURL signature
                    let tempurl = tempurl::TempURL::new(key);
                    let path = uri.path();

                    tempurl.validate_request(method.as_str(), path, &tempurl_params)?;

                    // TempURL is valid - proceed with request (no credentials needed)
                    debug!("TempURL validated successfully");

                    // Reconstruct request for object operation
                    let req = Request::from_parts(parts, body);
                    return handle_tempurl_object_request(req, route).await;
                } else {
                    // No TempURL key configured for this account
                    return Err(SwiftError::Unauthorized(
                        "TempURL key not configured for this account".to_string(),
                    ));
                }
            }
        }
    }

    // No TempURL or TempURL validation failed - require normal authentication
    let credentials = credentials.ok_or_else(|| SwiftError::Unauthorized("Authentication required".to_string()))?;

    // Reconstruct request
    let req = Request::from_parts(parts, body);
    handle_authenticated_request(req, route, credentials).await
}

/// Handle TempURL-authenticated object requests
async fn handle_tempurl_object_request(
    req: Request<Body>,
    route: SwiftRoute,
) -> Result<Response<Body>, SwiftError> {
    let SwiftRoute::Object { account, container, object, method } = route else {
        return Err(SwiftError::InternalServerError("Invalid route for TempURL".to_string()));
    };

    let (parts, body) = req.into_parts();
    let headers = parts.headers;

    match method {
        Method::GET => {
            // TempURL GET request
            handle_object_get(&account, &container, &object, &headers, &None).await
        }
        Method::HEAD => {
            // TempURL HEAD request
            handle_object_head(&account, &container, &object, &None).await
        }
        Method::PUT => {
            // TempURL PUT request (upload via TempURL)
            handle_object_put(&account, &container, &object, body, &headers, &None).await
        }
        _ => Err(SwiftError::BadRequest(format!(
            "Method {} not allowed via TempURL",
            method
        ))),
    }
}

/// Handle authenticated Swift API requests
async fn handle_authenticated_request(
    req: Request<Body>,
    route: SwiftRoute,
    credentials: Credentials,
) -> Result<Response<Body>, SwiftError>
{
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let credentials_opt = Some(credentials.clone());

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
                    // Account metadata update - extract headers
                    let mut metadata = std::collections::HashMap::new();

                    // Extract X-Account-Meta-* headers
                    for (key, value) in &headers {
                        let key_str = key.as_str();
                        if key_str.starts_with("x-account-meta-") {
                            let meta_key = &key_str[15..]; // Strip "x-account-meta-"
                            if let Ok(value_str) = value.to_str() {
                                metadata.insert(meta_key.to_string(), value_str.to_string());
                            }
                        }
                    }

                    // Special handling for TempURL key headers
                    // X-Account-Meta-Temp-URL-Key or X-Account-Meta-Temp-Url-Key
                    if let Some(tempurl_key) = headers.get("x-account-meta-temp-url-key")
                        .or_else(|| headers.get("x-account-meta-temp-Url-key")) {
                        if let Ok(key_str) = tempurl_key.to_str() {
                            metadata.insert("temp-url-key".to_string(), key_str.to_string());
                        }
                    }

                    // Update account metadata
                    super::account::update_account_metadata(&account, &metadata, &credentials_opt).await?;

                    let trans_id = generate_trans_id();
                    Response::builder()
                        .status(StatusCode::NO_CONTENT) // 204 - Success
                        .header("content-type", "text/html; charset=utf-8")
                        .header("content-length", "0")
                        .header("x-trans-id", trans_id.clone())
                        .header("x-openstack-request-id", trans_id)
                        .body(Body::empty())
                        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
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
                    // Check for versioning header
                    if let Some(versions_location) = headers.get("x-versions-location") {
                        if let Ok(archive_container) = versions_location.to_str() {
                            // Enable versioning on this container
                            container::enable_versioning(&account, &container, archive_container, &credentials).await?;
                        }
                    }

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
                    // Check for versioning headers first
                    if let Some(versions_location) = headers.get("x-versions-location") {
                        if let Ok(archive_container) = versions_location.to_str() {
                            // Enable versioning
                            container::enable_versioning(&account, &container, archive_container, &credentials).await?;
                        }
                    } else if headers.contains_key("x-remove-versions-location") {
                        // Disable versioning
                        container::disable_versioning(&account, &container, &credentials).await?;
                    }

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

                    // Check if versioning is enabled for this container
                    if let Some(archive_container) = container::get_versions_location(&account, &container, &credentials).await? {
                        // Check if object already exists (need to archive it)
                        if object::head_object(&account, &container, &object, &credentials).await.is_ok() {
                            // Archive current version before overwriting
                            super::versioning::archive_current_version(
                                &account,
                                &container,
                                &object,
                                &archive_container,
                                &credentials,
                            )
                            .await?;
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
                            // Parse Range header and calculate actual byte range
                            if let Some((start, end)) = parse_range_header(rh, info.size as u64) {
                                let length = end - start + 1;
                                response = response
                                    .header("content-range", format!("bytes {}-{}/{}", start, end, info.size))
                                    .header("content-length", length.to_string());
                            } else {
                                // Invalid range - return full object with 200 OK
                                response = response
                                    .status(StatusCode::OK)
                                    .header("content-length", info.size.to_string());
                            }
                        } else {
                            // No valid range - should not happen since we set is_range_request
                            // But be defensive
                            response = response.header("content-length", info.size.to_string());
                        }
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

                    // Check if versioning is enabled and restore previous version
                    if let Some(archive_container) = container::get_versions_location(&account, &container, &credentials).await? {
                        // Attempt to restore previous version from archive
                        // Ignore errors if no versions exist (object is fully deleted)
                        let _ = super::versioning::restore_previous_version(
                            &account,
                            &container,
                            &object,
                            &archive_container,
                            &credentials,
                        )
                        .await;
                    }

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

/// Helper function for object GET operations (used by both authenticated and TempURL requests)
async fn handle_object_get(
    account: &str,
    container: &str,
    object: &str,
    headers: &axum::http::HeaderMap,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    // For TempURL requests, credentials will be None
    // Operations that require credentials will fail appropriately

    // Check if object is SLO (via metadata)
    if slo::is_slo_object(account, container, object, credentials).await? {
        return slo::handle_slo_get(account, container, object, headers, credentials).await;
    }

    // Check if object is DLO (via x-object-manifest metadata)
    if let Some(manifest_value) = dlo::is_dlo_object(account, container, object, credentials).await? {
        return dlo::handle_dlo_get(account, container, object, headers, credentials, manifest_value).await;
    }

    // Regular object download - parse Range header if present
    let range_header = headers
        .get("range")
        .and_then(|v| v.to_str().ok());

    // Get object metadata first (needed for Range validation)
    let info = if let Some(creds) = credentials {
        object::head_object(account, container, object, creds).await?
    } else {
        // TempURL access - try without credentials
        // Note: This will fail if the object requires authentication
        // In production, we'd need a special path for TempURL access
        return Err(SwiftError::InternalServerError("TempURL object access not fully implemented".to_string()));
    };

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

    let reader = if let Some(creds) = credentials {
        object::get_object(account, container, object, creds, parsed_range).await?
    } else {
        return Err(SwiftError::InternalServerError("TempURL object access not fully implemented".to_string()));
    };

    let trans_id = generate_trans_id();

    let mut response = Response::builder()
        .status(status)
        .header("content-type", info.content_type.as_deref().unwrap_or("application/octet-stream"))
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id);

    // Set Content-Length and range-specific headers
    if status == StatusCode::PARTIAL_CONTENT {
        response = response.header("accept-ranges", "bytes");
        if let Some(rh) = range_header {
            if let Ok(range_spec) = object::parse_range_header(rh) {
                // range_spec is HTTPRangeSpec struct with start and end as i64
                let start = range_spec.start;
                let end = range_spec.end;
                let length = end - start + 1;
                response = response
                    .header("content-range", format!("bytes {}-{}/{}", start, end, info.size))
                    .header("content-length", length.to_string());
            }
        }
    } else {
        response = response
            .header("content-length", info.size.to_string())
            .header("accept-ranges", "bytes");
    }

    if let Some(etag) = info.etag {
        response = response.header("etag", etag);
    }

    for (key, value) in info.user_defined {
        if key != "content-type" {
            let header_name = format!("x-object-meta-{}", key);
            response = response.header(header_name, value);
        }
    }

    // Convert GetObjectReader AsyncRead stream to Body
    // Use ReaderStream to convert AsyncRead to Stream
    let stream = tokio_util::io::ReaderStream::new(reader.stream);
    let axum_body = axum::body::Body::from_stream(stream);
    let body = Body::http_body_unsync(axum_body);

    response
        .body(body)
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
}

/// Helper function for object HEAD operations
async fn handle_object_head(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    let info = if let Some(creds) = credentials {
        object::head_object(account, container, object, creds).await?
    } else {
        return Err(SwiftError::InternalServerError("TempURL object access not fully implemented".to_string()));
    };

    let trans_id = generate_trans_id();
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", info.content_type.as_deref().unwrap_or("application/octet-stream"))
        .header("content-length", info.size.to_string())
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .header("accept-ranges", "bytes");

    if let Some(etag) = info.etag {
        response = response.header("etag", etag);
    }

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

/// Helper function for object PUT operations
async fn handle_object_put(
    account: &str,
    container: &str,
    object: &str,
    body: Body,
    headers: &axum::http::HeaderMap,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::InternalServerError("TempURL object upload not fully implemented".to_string()))?;

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
    let buffered_reader = tokio::io::BufReader::with_capacity(65536, reader);

    let etag = object::put_object(account, container, object, creds, buffered_reader, headers).await?;

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

/// Generate a transaction ID for Swift responses
fn generate_trans_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_micros();
    format!("tx{:x}", timestamp)
}

/// Parse Range header and calculate actual byte range
///
/// Supports formats:
/// - "bytes=100-199" (range from 100 to 199 inclusive)
/// - "bytes=100-" (from 100 to end)
/// - "bytes=-500" (last 500 bytes)
///
/// Returns Some((start, end)) for valid range, None for invalid
fn parse_range_header(range_header: &str, total_size: u64) -> Option<(u64, u64)> {
    if total_size == 0 {
        return None;
    }

    // Expected format: "bytes=START-END" or "bytes=START-" or "bytes=-SUFFIX"
    let range_spec = range_header.strip_prefix("bytes=")?;

    // Only consider first range if multiple specified (some clients send multiple ranges)
    let first_range = range_spec.split(',').next()?.trim();

    // Split on hyphen
    let mut parts = first_range.splitn(2, '-');
    let start_str = parts.next()?.trim();
    let end_str = parts.next()?.trim();

    match (start_str.parse::<u64>().ok(), end_str.parse::<u64>().ok()) {
        // bytes=START-END
        (Some(start), Some(end)) if start < total_size && start <= end => {
            let end = end.min(total_size - 1);
            Some((start, end))
        }
        // bytes=START- (from start to end of file)
        (Some(start), None) if start < total_size => {
            Some((start, total_size - 1))
        }
        // bytes=-SUFFIX (last N bytes)
        (None, Some(suffix_len)) if suffix_len > 0 => {
            let len = suffix_len.min(total_size);
            let start = total_size - len;
            let end = total_size - 1;
            Some((start, end))
        }
        // Invalid or unsatisfiable range
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range_header_start_end() {
        // bytes=100-199
        let result = parse_range_header("bytes=100-199", 1000);
        assert_eq!(result, Some((100, 199)));
    }

    #[test]
    fn test_parse_range_header_start_to_eof() {
        // bytes=100- (from 100 to end)
        let result = parse_range_header("bytes=100-", 1000);
        assert_eq!(result, Some((100, 999)));
    }

    #[test]
    fn test_parse_range_header_suffix() {
        // bytes=-500 (last 500 bytes)
        let result = parse_range_header("bytes=-500", 1000);
        assert_eq!(result, Some((500, 999)));
    }

    #[test]
    fn test_parse_range_header_suffix_larger_than_file() {
        // bytes=-2000 when file is only 1000 bytes
        let result = parse_range_header("bytes=-2000", 1000);
        assert_eq!(result, Some((0, 999)));
    }

    #[test]
    fn test_parse_range_header_end_beyond_eof() {
        // bytes=100-2000 when file is only 1000 bytes
        let result = parse_range_header("bytes=100-2000", 1000);
        assert_eq!(result, Some((100, 999))); // Clamp to EOF
    }

    #[test]
    fn test_parse_range_header_start_beyond_eof() {
        // bytes=1500- when file is only 1000 bytes
        let result = parse_range_header("bytes=1500-", 1000);
        assert_eq!(result, None); // Invalid
    }

    #[test]
    fn test_parse_range_header_invalid_start_greater_than_end() {
        // bytes=500-100 (start > end)
        let result = parse_range_header("bytes=500-100", 1000);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_range_header_zero_size_file() {
        // Any range on 0-byte file is invalid
        let result = parse_range_header("bytes=0-100", 0);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_range_header_multiple_ranges_first_only() {
        // bytes=0-100,200-300 (only parse first range)
        let result = parse_range_header("bytes=0-100,200-300", 1000);
        assert_eq!(result, Some((0, 100)));
    }

    #[test]
    fn test_parse_range_header_no_bytes_prefix() {
        // Missing "bytes=" prefix
        let result = parse_range_header("0-100", 1000);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_range_header_invalid_format() {
        // Invalid format
        let result = parse_range_header("bytes=abc-def", 1000);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_range_header_single_byte() {
        // bytes=100-100 (single byte)
        let result = parse_range_header("bytes=100-100", 1000);
        assert_eq!(result, Some((100, 100)));
    }

    #[test]
    fn test_parse_range_header_full_file() {
        // bytes=0-999 (entire 1000-byte file)
        let result = parse_range_header("bytes=0-999", 1000);
        assert_eq!(result, Some((0, 999)));
    }
}

