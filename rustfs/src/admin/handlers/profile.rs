//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::admin::router::Operation;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, StatusCode};
use matchit::Params;
use s3s::{Body, S3Request, S3Response, S3Result};
use tracing::info;

pub struct TriggerProfileCPU {}
#[async_trait::async_trait]
impl Operation for TriggerProfileCPU {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Triggering CPU profile dump via S3 request...");
        #[cfg(target_os = "windows")]
        {
            let mut header = HeaderMap::new();
            header.insert(CONTENT_TYPE, "text/plain".parse().unwrap());
            return Ok(S3Response::with_headers(
                (
                    StatusCode::NOT_IMPLEMENTED,
                    Body::from("CPU profiling is not supported on Windows".to_string()),
                ),
                header,
            ));
        }

        #[cfg(not(target_os = "windows"))]
        {
            let dur = std::time::Duration::from_secs(60);
            match crate::profiling::dump_cpu_pprof_for(dur).await {
                Ok(path) => {
                    let mut header = HeaderMap::new();
                    header.insert(CONTENT_TYPE, "text/html".parse().unwrap());
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(path.display().to_string())), header))
                }
                Err(e) => Err(s3s::s3_error!(InternalError, "{}", format!("Failed to dump CPU profile: {e}"))),
            }
        }
    }
}

pub struct TriggerProfileMemory {}
#[async_trait::async_trait]
impl Operation for TriggerProfileMemory {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("Triggering Memory profile dump via S3 request...");
        #[cfg(target_os = "windows")]
        {
            let mut header = HeaderMap::new();
            header.insert(CONTENT_TYPE, "text/plain".parse().unwrap());
            return Ok(S3Response::with_headers(
                (
                    StatusCode::NOT_IMPLEMENTED,
                    Body::from("Memory profiling is not supported on Windows".to_string()),
                ),
                header,
            ));
        }

        #[cfg(not(target_os = "windows"))]
        {
            match crate::profiling::dump_memory_pprof_now().await {
                Ok(path) => {
                    let mut header = HeaderMap::new();
                    header.insert(CONTENT_TYPE, "text/html".parse().unwrap());
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(path.display().to_string())), header))
                }
                Err(e) => Err(s3s::s3_error!(InternalError, "{}", format!("Failed to dump Memory profile: {e}"))),
            }
        }
    }
}
