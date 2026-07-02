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

use crate::admin::{auth::validate_admin_request, router::Operation};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::RemoteAddr;
use http::StatusCode;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use tracing::info;

pub(super) async fn authorize_profile_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::ProfilingAdminAction)],
        remote_addr,
    )
    .await
}

pub struct TriggerProfileCPU {}
#[async_trait::async_trait]
impl Operation for TriggerProfileCPU {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;
        info!("Triggering CPU profile dump via S3 request...");

        let message = match crate::profiling::dump_cpu_pprof_for(std::time::Duration::from_secs(0)).await {
            Ok(path) => path.display().to_string(),
            Err(err) => err,
        };
        Ok(S3Response::new((StatusCode::NOT_IMPLEMENTED, Body::from(message))))
    }
}

pub struct TriggerProfileMemory {}
#[async_trait::async_trait]
impl Operation for TriggerProfileMemory {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;
        info!("Triggering Memory profile dump via S3 request...");

        let message = match crate::profiling::dump_memory_pprof_now().await {
            Ok(path) => path.display().to_string(),
            Err(err) => err,
        };
        Ok(S3Response::new((StatusCode::NOT_IMPLEMENTED, Body::from(message))))
    }
}

#[cfg(test)]
mod tests {
    use super::{TriggerProfileCPU, TriggerProfileMemory};
    use crate::admin::router::Operation;
    use crate::server::{PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
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
    async fn trigger_profile_cpu_rejects_missing_credentials() {
        let result = TriggerProfileCPU {}
            .call(build_profile_request(PROFILE_CPU_PATH), Params::new())
            .await;
        let err = result.expect_err("legacy CPU profile handler must reject anonymous requests");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }

    #[tokio::test]
    async fn trigger_profile_memory_rejects_missing_credentials() {
        let result = TriggerProfileMemory {}
            .call(build_profile_request(PROFILE_MEMORY_PATH), Params::new())
            .await;
        let err = result.expect_err("legacy memory profile handler must reject anonymous requests");

        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Signature is required"));
    }
}
