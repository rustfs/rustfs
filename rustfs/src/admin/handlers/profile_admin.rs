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

use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};
use std::collections::HashMap;
use tracing::error;

#[allow(dead_code)]
fn extract_query_params(uri: &Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }

    params
}

pub fn register_profiling_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/profile").as_str(),
        AdminOperation(&ProfileHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/debug/pprof/status").as_str(),
        AdminOperation(&ProfileStatusHandler {}),
    )?;

    Ok(())
}

pub struct ProfileHandler {}

#[async_trait::async_trait]
impl Operation for ProfileHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        {
            let requested_url = req.uri.to_string();
            let target_os = std::env::consts::OS;
            let target_arch = std::env::consts::ARCH;
            let target_env = option_env!("CARGO_CFG_TARGET_ENV").unwrap_or("unknown");
            let msg = format!(
                "CPU profiling is not supported on this platform. target_os={target_os}, target_env={target_env}, target_arch={target_arch}, requested_url={requested_url}"
            );
            return Ok(S3Response::new((StatusCode::NOT_IMPLEMENTED, Body::from(msg))));
        }

        #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
        {
            use rustfs_config::{DEFAULT_CPU_FREQ, ENV_CPU_FREQ};
            use rustfs_utils::get_env_usize;

            let queries = extract_query_params(&req.uri);
            let seconds = queries.get("seconds").and_then(|s| s.parse::<u64>().ok()).unwrap_or(30);
            let format = queries.get("format").cloned().unwrap_or_else(|| "protobuf".to_string());

            if seconds > 300 {
                return Ok(S3Response::new((
                    StatusCode::BAD_REQUEST,
                    Body::from("Profile duration cannot exceed 300 seconds".to_string()),
                )));
            }

            match format.as_str() {
                "protobuf" | "pb" => match crate::profiling::dump_cpu_pprof_for(std::time::Duration::from_secs(seconds)).await {
                    Ok(path) => match tokio::fs::read(&path).await {
                        Ok(bytes) => {
                            let mut headers = HeaderMap::new();
                            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
                            Ok(S3Response::with_headers((StatusCode::OK, Body::from(bytes)), headers))
                        }
                        Err(e) => Ok(S3Response::new((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Body::from(format!("Failed to read profile file: {e}")),
                        ))),
                    },
                    Err(e) => Ok(S3Response::new((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Body::from(format!("Failed to collect CPU profile: {e}")),
                    ))),
                },
                "flamegraph" | "svg" => {
                    let freq = get_env_usize(ENV_CPU_FREQ, DEFAULT_CPU_FREQ) as i32;
                    let guard = match pprof::ProfilerGuard::new(freq) {
                        Ok(g) => g,
                        Err(e) => {
                            return Ok(S3Response::new((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Body::from(format!("Failed to create profiler: {e}")),
                            )));
                        }
                    };

                    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;

                    let report = match guard.report().build() {
                        Ok(r) => r,
                        Err(e) => {
                            return Ok(S3Response::new((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Body::from(format!("Failed to build profile report: {e}")),
                            )));
                        }
                    };

                    let mut flamegraph_buf = Vec::new();
                    if let Err(e) = report.flamegraph(&mut flamegraph_buf) {
                        return Ok(S3Response::new((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Body::from(format!("Failed to generate flamegraph: {e}")),
                        )));
                    }

                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));
                    Ok(S3Response::with_headers((StatusCode::OK, Body::from(flamegraph_buf)), headers))
                }
                _ => Ok(S3Response::new((
                    StatusCode::BAD_REQUEST,
                    Body::from("Unsupported format. Use 'protobuf' or 'flamegraph'".to_string()),
                ))),
            }
        }
    }
}

pub struct ProfileStatusHandler {}

#[async_trait::async_trait]
impl Operation for ProfileStatusHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        let message = format!("CPU profiling is not supported on {} platform", std::env::consts::OS);
        #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
        let status = HashMap::from([
            ("enabled", "false"),
            ("status", "not_supported"),
            ("platform", std::env::consts::OS),
            ("message", message.as_str()),
        ]);

        #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
        let status = {
            use rustfs_config::{DEFAULT_ENABLE_PROFILING, ENV_ENABLE_PROFILING};
            use rustfs_utils::get_env_bool;

            let enabled = get_env_bool(ENV_ENABLE_PROFILING, DEFAULT_ENABLE_PROFILING);
            if enabled {
                HashMap::from([
                    ("enabled", "true"),
                    ("status", "running"),
                    ("supported_formats", "protobuf, flamegraph"),
                    ("max_duration_seconds", "300"),
                    ("endpoint", "/rustfs/admin/debug/pprof/profile"),
                ])
            } else {
                HashMap::from([
                    ("enabled", "false"),
                    ("status", "disabled"),
                    ("message", "Set RUSTFS_ENABLE_PROFILING=true to enable profiling"),
                ])
            }
        };

        match serde_json::to_string(&status) {
            Ok(json) => {
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok(S3Response::with_headers((StatusCode::OK, Body::from(json)), headers))
            }
            Err(e) => {
                error!("Failed to serialize status: {}", e);
                Ok(S3Response::new((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Body::from("Failed to serialize status".to_string()),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::extract_query_params;
    use http::Uri;

    #[test]
    fn test_extract_query_params_decodes_percent_encoded_values() {
        let uri: Uri = "/rustfs/admin/debug/pprof/profile?format=flamegraph&note=a%2Bb+value"
            .parse()
            .expect("uri should parse");
        let params = extract_query_params(&uri);

        assert_eq!(params.get("format"), Some(&"flamegraph".to_string()));
        assert_eq!(params.get("note"), Some(&"a+b value".to_string()));
    }
}
