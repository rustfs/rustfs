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

use crate::admin::{
    auth::authorize_admin_request,
    router::{AdminOperation, Operation, S3Router},
};
use crate::server::{
    ADMIN_PREFIX, NativeTlsTransport,
    tls_publish::{self, PublishError, PublishRequest},
};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::{action::Action, action::AdminAction};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

pub fn register_tls_certificate_routes(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/tls/certificate-state"),
        AdminOperation(&CertificateStateHandler),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/tls/certificate"),
        AdminOperation(&PublishCertificateHandler),
    )?;
    Ok(())
}

pub struct CertificateStateHandler;
pub struct PublishCertificateHandler;

fn require_native_tls(req: &S3Request<Body>) -> S3Result<()> {
    if !req
        .extensions
        .get::<NativeTlsTransport>()
        .is_some_and(|transport| transport.0)
    {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "certificate management requires TLS on the RustFS listener",
        ));
    }
    Ok(())
}

async fn authorize_certificate_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    if req.credentials.is_none() {
        return Err(S3Error::with_message(
            S3ErrorCode::AccessDenied,
            "certificate management requires credentials",
        ));
    }
    authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;
    require_native_tls(req)
}

fn tls_path() -> S3Result<String> {
    crate::server::tls_material::active_server_tls_path()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "certificate publication is not configured"))
}

fn publication_error(error: PublishError) -> S3Error {
    let (code, status) = match error {
        PublishError::Conflict => (S3ErrorCode::InvalidRequest, StatusCode::CONFLICT),
        PublishError::Disabled => (S3ErrorCode::InvalidRequest, StatusCode::SERVICE_UNAVAILABLE),
        PublishError::Invalid => (S3ErrorCode::InvalidRequest, StatusCode::BAD_REQUEST),
        PublishError::Storage | PublishError::Busy => (S3ErrorCode::InternalError, StatusCode::SERVICE_UNAVAILABLE),
    };
    let mut result = S3Error::with_message(code, error.to_string());
    result.set_status_code(status);
    result
}

fn json_response(value: &impl serde::Serialize) -> S3Result<S3Response<(StatusCode, Body)>> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "certificate status serialization failed"))?;
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(bytes)), headers))
}

#[async_trait::async_trait]
impl Operation for CertificateStateHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_certificate_request(&req, AdminAction::TLSCertificateStatusAdminAction).await?;
        json_response(&tls_publish::certificate_state(tls_path()?).await.map_err(publication_error)?)
    }
}

#[async_trait::async_trait]
impl Operation for PublishCertificateHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_certificate_request(&req, AdminAction::TLSCertificatePublishAdminAction).await?;
        let body = req.input.store_all_limited(128 * 1024).await.map_err(|_| {
            S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "certificate publication body exceeds the limit or is unreadable",
            )
        })?;
        let request: PublishRequest = serde_json::from_slice(&body)
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "invalid certificate publication JSON"))?;
        json_response(&tls_publish::publish(tls_path()?, request).await.map_err(publication_error)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: Method) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: "/rustfs/admin/v3/tls/certificate".parse().expect("URI"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn certificate_handlers_reject_missing_credentials() {
        assert_eq!(
            CertificateStateHandler
                .call(request(Method::GET), Params::new())
                .await
                .expect_err("unauthenticated GET")
                .code(),
            &S3ErrorCode::AccessDenied
        );
        assert_eq!(
            PublishCertificateHandler
                .call(request(Method::POST), Params::new())
                .await
                .expect_err("unauthenticated POST")
                .code(),
            &S3ErrorCode::AccessDenied
        );
    }

    #[test]
    fn forwarded_protocol_never_authorizes_private_key_upload() {
        let mut request = request(Method::POST);
        request.headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert!(require_native_tls(&request).is_err());
        request.extensions.insert(NativeTlsTransport(false));
        assert!(require_native_tls(&request).is_err());
        request.extensions.insert(NativeTlsTransport(true));
        assert!(require_native_tls(&request).is_ok());
    }

    #[test]
    fn unknown_json_fields_are_rejected_without_echoing_material() {
        let error = serde_json::from_str::<PublishRequest>(
            r#"{"schema_version":"x","resource_id":"x","expected_target_revision":"x","issuance_id":"00000000-0000-0000-0000-000000000001","certificate_pem":"cert","private_key_pem":"secret","extra":true}"#,
        );
        assert!(error.is_err());
    }
}
