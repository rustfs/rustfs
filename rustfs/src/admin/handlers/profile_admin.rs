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

use super::profile::{authorize_profile_request, profile_not_implemented_response};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result};
use serde::Serialize;
use tracing::error;

#[derive(Serialize)]
struct ProfileStatus {
    enabled: &'static str,
    status: &'static str,
    platform: &'static str,
    message: &'static str,
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
        authorize_profile_request(&req).await?;

        let requested_url = req.uri.to_string();
        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(format!(
            "{}; requested_url={requested_url}",
            crate::profiling::local_cpu_pprof_unsupported_message()
        )))
    }
}

pub struct ProfileStatusHandler {}

#[async_trait::async_trait]
impl Operation for ProfileStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;

        let status = ProfileStatus {
            enabled: "false",
            status: "not_supported",
            platform: std::env::consts::OS,
            message: crate::profiling::LOCAL_CPU_PPROF_UNSUPPORTED_SUMMARY,
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
    use super::{ProfileHandler, ProfileStatusHandler};
    use crate::admin::router::Operation;
    use http::{Extensions, HeaderMap, Uri};
    use hyper::Method;
    use matchit::Params;
    use s3s::{Body, S3ErrorCode, S3Request};

    fn build_profile_request(uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn profile_handler_rejects_missing_credentials() {
        let result = ProfileHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/profile?format=protobuf"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn profile_status_handler_rejects_missing_credentials() {
        let result = ProfileStatusHandler {}
            .call(build_profile_request("/rustfs/admin/debug/pprof/status"), Params::new())
            .await;
        let err = match result {
            Ok(_) => panic!("profile status handler must reject unauthenticated requests"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }
}
