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

use crate::admin::auth::authorize_admin_envelope;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::app::RequestEnvelope;
use http::StatusCode;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{Body, S3Result, s3_error};
use tracing::info;

/// The pre-check keeps these endpoints' historical `AccessDenied` missing-credentials
/// response; the shared gate reports `InvalidRequest` "get cred failed".
pub(super) async fn authorize_profile_envelope(envelope: &RequestEnvelope) -> S3Result<()> {
    if envelope.credentials().is_none() {
        return Err(s3_error!(AccessDenied, "Signature is required"));
    }

    authorize_admin_envelope(envelope, vec![Action::AdminAction(AdminAction::ProfilingAdminAction)]).await?;
    Ok(())
}

pub(super) fn profile_not_implemented_response(message: String) -> AdminResponse {
    AdminResponse::new(StatusCode::NOT_IMPLEMENTED, Body::from(message))
}

pub struct TriggerProfileCPU {}
#[async_trait::async_trait]
impl Operation for TriggerProfileCPU {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_profile_envelope(req.envelope()).await?;
        info!("Triggering CPU profile dump via S3 request...");

        crate::profiling::log_cpu_pprof_dump_skipped();
        Ok(profile_not_implemented_response(crate::profiling::local_cpu_pprof_unsupported_message()))
    }
}

pub struct TriggerProfileMemory {}
#[async_trait::async_trait]
impl Operation for TriggerProfileMemory {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_profile_envelope(req.envelope()).await?;
        info!("Triggering Memory profile dump via S3 request...");

        crate::profiling::log_memory_pprof_dump_skipped();
        Ok(profile_not_implemented_response(crate::profiling::memory_pprof_unsupported_message()))
    }
}

#[cfg(test)]
mod tests {
    use super::{TriggerProfileCPU, TriggerProfileMemory, authorize_profile_envelope};
    use crate::admin::operation::test_support::TestRequest;
    use crate::admin::operation::{AdminRequest, Operation};
    use crate::server::{PROFILE_CPU_PATH, PROFILE_MEMORY_PATH};
    use hyper::Method;
    use matchit::Params;
    use rustfs_s3_types::S3ErrorCode;

    #[tokio::test]
    async fn authorize_profile_envelope_keeps_the_access_denied_precheck() {
        let owned_request = TestRequest::new(Method::GET, "/debug/pprof/cpu").build();
        let owned = authorize_profile_envelope(owned_request.envelope())
            .await
            .expect_err("no credentials");

        assert_eq!(owned.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(owned.message(), Some("Signature is required"));
    }

    fn build_profile_request(uri: &'static str) -> AdminRequest {
        TestRequest::new(Method::GET, uri).build()
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
