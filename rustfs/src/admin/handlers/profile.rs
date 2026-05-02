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
use http::header::CONTENT_TYPE;
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use tracing::info;

pub(super) async fn authorize_profile_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
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

pub struct TriggerProfileMemory {}
#[async_trait::async_trait]
impl Operation for TriggerProfileMemory {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_profile_request(&req).await?;
        info!("Triggering Memory profile dump via S3 request...");

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
