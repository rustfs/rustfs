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

use super::profile::{TriggerProfileCPU, TriggerProfileMemory};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::{HEALTH_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};

pub fn register_health_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Health check endpoint for monitoring and orchestration
    r.insert(Method::GET, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::HEAD, HEALTH_PREFIX, AdminOperation(&HealthCheckHandler {}))?;
    r.insert(Method::GET, PROFILE_CPU_PATH, AdminOperation(&TriggerProfileCPU {}))?;
    r.insert(Method::GET, PROFILE_MEMORY_PATH, AdminOperation(&TriggerProfileMemory {}))?;

    Ok(())
}

/// Health check handler for endpoint monitoring
pub struct HealthCheckHandler {}

#[async_trait::async_trait]
impl Operation for HealthCheckHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        use serde_json::json;

        // Extract the original HTTP Method (encapsulated by s3s into S3Request)
        let method = req.method;

        // Only GET and HEAD are allowed
        if method != http::Method::GET && method != http::Method::HEAD {
            // 405 Method Not Allowed
            let mut headers = HeaderMap::new();
            headers.insert(http::header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(S3Response::with_headers(
                (StatusCode::METHOD_NOT_ALLOWED, Body::from("Method Not Allowed".to_string())),
                headers,
            ));
        }

        let health_info = json!({
            "status": "ok",
            "service": "rustfs-endpoint",
            "timestamp": jiff::Zoned::now().to_string(),
            "version": env!("CARGO_PKG_VERSION")
        });

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if method == http::Method::HEAD {
            // HEAD: only returns the header and status code, not the body
            return Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), headers));
        }

        // GET: Return JSON body normally
        let body_str = serde_json::to_string(&health_info).unwrap_or_else(|_| "{}".to_string());
        let body = Body::from(body_str);

        Ok(S3Response::with_headers((StatusCode::OK, body), headers))
    }
}
