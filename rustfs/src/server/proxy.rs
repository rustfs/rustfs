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

use axum::{
    body::Body,
    extract::{Request, State},
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use hyper::Request as HyperRequest;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, instrument};

/// Constant definition PROXY_TARGET
const PROXY_TARGET_HOST: &str = "http://127.0.0.1"; // Intranet forwarding target

/// Create an optimized Hyper client for proxying requests
///
/// Returns:
/// * `Client<HttpConnector, Body>` - An optimized Hyper client
///
pub(crate) fn create_optimized_client() -> Client<HttpConnector, Body> {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(Duration::from_secs(10)));
    connector.set_keepalive(Some(Duration::from_secs(60)));
    connector.enforce_http(false);

    Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        .build(connector)
}

/// The method does not allow processing: log and return 405
/// # Arguments
/// * `req` - The incoming request
///
/// Returns:
/// * `Response` - A 405 Method Not Allowed response
///
#[instrument]
pub(crate) async fn method_not_allowed(req: Request) -> Response {
    error!("The method does not allow:{} {}", req.method(), req.uri());
    (
        StatusCode::METHOD_NOT_ALLOWED,
        format!("The method does not allow:{} {}", req.method(), req.uri()),
    )
        .into_response()
}

/// Proxy processing: Forwards requests to 9000 using Hyper, supporting all methods and streaming bodies
/// # Arguments
/// * `State(client)` - Shared Hyper client state
/// * `req` - The incoming request
///
/// Returns:
/// * `Response` - The response from the upstream server or an error response
///
#[instrument(skip(req, client), fields(method = %req.method(), path = %req.uri().path()))]
pub(crate) async fn proxy_handler(State(client): State<Arc<Client<HttpConnector, Body>>>, req: Request) -> Response {
    let method = req.method().clone();
    let original_uri = req.uri().clone();

    // Build the target URI: PROXY_TARGET + original path + query parameters
    let path_and_query = original_uri.path_and_query().map(|p| p.as_str()).unwrap_or("");
    let port = rustfs_ecstore::global::global_rustfs_port();
    debug!("Global RustFS Port: {}", port);
    let target_host = if port == 80 || port == 443 {
        PROXY_TARGET_HOST.to_string()
    } else {
        format!("{}:{}", PROXY_TARGET_HOST, port)
    };
    let target_uri_str = format!("{}{}", target_host, path_and_query);

    debug!("Forward the request to:{}", target_uri_str);

    // 解析目标 URI
    let target_uri: Uri = match target_uri_str.parse() {
        Ok(u) => u,
        Err(e) => {
            error!("Invalid request path URI: {}", e);
            return (StatusCode::BAD_REQUEST, format!("Invalid request path URI:{}", e)).into_response();
        }
    };
    debug!("Proxy forwarding: {} {} -> {}", method, original_uri, target_uri);
    // Build a new Hyper request
    let mut builder = HyperRequest::builder()
        .method(method.clone())
        .uri(target_uri.clone())
        .version(req.version());

    // Smart Replication Headers（Filter hop-by-hop headers）
    let headers_to_skip = [header::CONNECTION, header::UPGRADE, header::TRANSFER_ENCODING];

    // Copy headers
    let mut header_count = 0;
    for (key, value) in req.headers() {
        if !headers_to_skip.contains(key) {
            builder = builder.header(key.clone(), value.clone());
            debug!("Copy headers: {}: {:?}", key, value);
            header_count += 1;
        }
    }
    debug!("Retweeted {} headers", header_count);

    // Remove body: Use the original body directly, support streaming forwarding (efficient, no buffering)
    debug!("The request body is ready for forwarding");
    let body = req.into_body();
    // Build a Hyper request
    let hyper_req = match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            error!("Proxy request build fails:{}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Proxy request build fails:{}", e)).into_response();
        }
    };

    // Send requests with timeout control
    // Total timeout: 60 seconds
    let response = tokio::time::timeout(Duration::from_secs(60), client.request(hyper_req)).await;
    match response {
        Ok(Ok(upstream_res)) => {
            let status = upstream_res.status();
            let headers = upstream_res.headers().clone();

            debug!("Received upstream response: {}", status);

            let mut resp = Response::builder().status(status);

            // Copy the response headers (Filter hop-by-hop headers)
            for (k, v) in headers.iter() {
                if !headers_to_skip.contains(k) {
                    resp = resp.header(k, v);
                }
            }

            // Streaming Forwarding Responder (Zero Copy)
            let body_stream = upstream_res.into_body();
            match resp.body(Body::new(body_stream)) {
                Ok(response) => response,
                Err(e) => {
                    error!("Build response failed:{}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Build response failed").into_response()
                }
            }
        }
        Ok(Err(e)) => {
            if e.is_connect() {
                error!("Unable to connect to upstream services:{}", e);
                (StatusCode::BAD_GATEWAY, "Upstream service connection failed").into_response()
            } else {
                error!("The proxy request failed:{}", e);
                (StatusCode::BAD_GATEWAY, format!("Proxy error:{}", e)).into_response()
            }
        }
        Err(_) => {
            error!("Agent request total timeout");
            (StatusCode::GATEWAY_TIMEOUT, "Request timeout").into_response()
        }
    }
}
