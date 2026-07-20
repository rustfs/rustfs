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

use super::is_admin::IsAdminHandler;
use crate::admin::service::federated_identity::DefaultFederatedSessionBinding;
use crate::admin::service::session_policy::populate_session_policy;
use crate::admin::storage_api::bucket::utils::serialize;
use crate::{
    admin::runtime_sources::{current_action_credentials, current_federated_identity_service, current_token_signing_key},
    admin::{
        handlers::site_replication::site_replication_iam_change_hook,
        router::{AdminOperation, Operation, S3Router},
    },
    auth::{check_key_valid, get_session_token},
    server::ADMIN_PREFIX,
    server::RemoteAddr,
};
use http::StatusCode;
use http::header::HeaderValue;
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_iam::federation::{FederatedSessionBindingError, FederationError};
use rustfs_madmin::{SITE_REPL_API_VERSION, SRIAMItem, SRSTSCredential};
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{
        Args,
        action::{Action, StsAction},
    },
};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{AssumeRoleOutput, Credentials, Timestamp},
    s3_error,
};
use serde::Deserialize;
use serde_json::Value;
use serde_urlencoded::from_bytes;
use time::{Duration, OffsetDateTime};
use tracing::{debug, error, info, warn};

const ASSUME_ROLE_ACTION: &str = "AssumeRole";
const ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION: &str = "AssumeRoleWithWebIdentity";
const ASSUME_ROLE_VERSION: &str = "2011-06-15";

/// Default STS temporary credential lifetime (seconds) when the client omits DurationSeconds.
const STS_DEFAULT_DURATION_SECS: usize = 3600;
/// Minimum STS temporary credential lifetime (seconds), matching AWS/MinIO (15 minutes).
const STS_MIN_DURATION_SECS: usize = 900;
/// Maximum STS temporary credential lifetime (seconds), matching AWS/MinIO AssumeRole (12 hours).
const STS_MAX_DURATION_SECS: usize = 43200;

/// Clamp the client-supplied DurationSeconds into the allowed STS window.
///
/// A value of 0 (unset) falls back to the default; any other value is clamped into
/// `[STS_MIN_DURATION_SECS, STS_MAX_DURATION_SECS]`. This prevents callers from minting
/// near-permanent temporary credentials and keeps the standard AssumeRole path consistent
/// with the AssumeRoleWithWebIdentity path.
fn clamp_assume_role_duration(duration_seconds: usize) -> usize {
    if duration_seconds == 0 {
        STS_DEFAULT_DURATION_SECS
    } else {
        duration_seconds.clamp(STS_MIN_DURATION_SECS, STS_MAX_DURATION_SECS)
    }
}

fn web_identity_federation_error(error: FederationError) -> S3Error {
    match error {
        FederationError::TokenVerification(message) => {
            S3Error::with_message(S3ErrorCode::AccessDenied, format!("token verification failed: {message}"))
        }
        FederationError::NoAuthorizationContext => {
            s3_error!(InvalidArgument, "no policies are available for this OIDC token")
        }
        FederationError::Binding(FederatedSessionBindingError::InvalidRequest(message)) => {
            S3Error::with_message(S3ErrorCode::InvalidRequest, message)
        }
        FederationError::Binding(FederatedSessionBindingError::Internal(message)) => {
            S3Error::with_message(S3ErrorCode::InternalError, message)
        }
        other => S3Error::with_message(S3ErrorCode::InternalError, other.to_string()),
    }
}

pub fn register_admin_auth_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(Method::POST, "/", AdminOperation(&AssumeRoleHandle {}))?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/is-admin").as_str(),
        AdminOperation(&IsAdminHandler {}),
    )?;

    Ok(())
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AssumeRoleRequest {
    pub action: String,
    pub duration_seconds: usize,
    pub version: String,
    pub role_arn: String,
    pub role_session_name: String,
    pub policy: String,
    pub external_id: String,
    pub web_identity_token: String,
}

pub struct AssumeRoleHandle {}
#[async_trait::async_trait]
impl Operation for AssumeRoleHandle {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AssumeRoleHandle");

        let mut input = req.input;

        let bytes = match input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "STS request body too large or failed to read"));
            }
        };

        let body: AssumeRoleRequest = from_bytes(&bytes).map_err(|_e| s3_error!(InvalidRequest, "invalid STS request format"))?;

        match body.action.as_str() {
            ASSUME_ROLE_ACTION => {
                let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
                handle_assume_role(req.credentials, req.uri, req.headers, remote_addr, body).await
            }
            ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION => handle_assume_role_with_web_identity(body).await,
            _ => Err(s3_error!(InvalidArgument, "unsupported Action")),
        }
    }
}

/// Handle the standard AssumeRole action (requires SigV4 credentials).
async fn handle_assume_role(
    credentials: Option<s3s::auth::Credentials>,
    uri: http::Uri,
    headers: http::HeaderMap,
    remote_addr: Option<std::net::SocketAddr>,
    body: AssumeRoleRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let Some(user) = credentials else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let session_token = get_session_token(&uri, &headers);
    if session_token.is_some() {
        return Err(s3_error!(InvalidRequest, "AccessDenied1"));
    }

    let (cred, owner) = check_key_valid(get_session_token(&uri, &headers).unwrap_or_default(), &user.access_key).await?;

    if cred.is_temp() || cred.is_service_account() {
        return Err(s3_error!(InvalidRequest, "AccessDenied"));
    }

    let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };
    let conditions = crate::auth::get_condition_values(&headers, &cred, None, None, remote_addr);
    if !iam_store
        .is_allowed(&Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action: Action::StsAction(StsAction::AssumeRoleAction),
            conditions: &conditions,
            is_owner: owner,
            claims: cred.claims_or_empty(),
            deny_only: false,
            bucket: "",
            object: "",
        })
        .await
    {
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }

    if body.version.as_str() != ASSUME_ROLE_VERSION {
        return Err(s3_error!(InvalidArgument, "not support version"));
    }

    let mut claims = cred.claims.unwrap_or_default();

    populate_session_policy(&mut claims, &body.policy)?;

    let exp = clamp_assume_role_duration(body.duration_seconds);

    claims.insert(
        "exp".to_string(),
        Value::Number(serde_json::Number::from(
            OffsetDateTime::now_utc().unix_timestamp().saturating_add(exp as i64),
        )),
    );

    claims.insert("parent".to_string(), Value::String(cred.access_key.clone()));

    if let Err(_err) = iam_store.policy_db_get(&cred.access_key, &cred.groups).await {
        error!(
            "AssumeRole get policy failed, err: {:?}, access_key: {:?}, groups: {:?}",
            _err, cred.access_key, cred.groups
        );
        return Err(s3_error!(InvalidArgument, "invalid policy arg"));
    }

    let Some(secret) = current_token_signing_key() else {
        return Err(s3_error!(InvalidArgument, "global active sk not init"));
    };

    info!("AssumeRole get claims {:?}", &claims);

    let mut new_cred = get_new_credentials_with_metadata(&claims, &secret)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get new cred failed {e}")))?;

    new_cred.parent_user = cred.access_key.clone();

    debug!(
        access_key = %new_cred.access_key,
        parent_user = %new_cred.parent_user,
        expiration = ?new_cred.expiration,
        "AssumeRole generated temporary credentials"
    );

    let updated_at = iam_store
        .set_temp_user(&new_cred.access_key, &new_cred, None)
        .await
        .map_err(|_| s3_error!(InternalError, "set_temp_user failed"))?;

    let root_access_key = current_action_credentials().map(|cred| cred.access_key);
    if root_access_key.as_deref() != Some(new_cred.parent_user.as_str())
        && let Err(err) = site_replication_iam_change_hook(SRIAMItem {
            r#type: "sts-credential".to_string(),
            sts_credential: Some(SRSTSCredential {
                access_key: new_cred.access_key.clone(),
                secret_key: new_cred.secret_key.clone(),
                session_token: new_cred.session_token.clone(),
                parent_user: new_cred.parent_user.clone(),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
    {
        warn!("site replication STS hook failed, err: {err}");
    }

    let resp = AssumeRoleOutput {
        credentials: Some(Credentials {
            access_key_id: new_cred.access_key,
            expiration: Timestamp::from(
                new_cred
                    .expiration
                    .unwrap_or_else(|| OffsetDateTime::now_utc().saturating_add(Duration::seconds(3600))),
            ),
            secret_access_key: new_cred.secret_key,
            session_token: new_cred.session_token,
        }),
        ..Default::default()
    };

    // getAssumeRoleCredentials
    let output = serialize::<AssumeRoleOutput>(&resp)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize assume role output failed: {e}")))?;

    Ok(S3Response::new((StatusCode::OK, Body::from(output))))
}

/// Handle the AssumeRoleWithWebIdentity action.
/// The JWT (id_token) in the request is the authentication — no SigV4 needed.
async fn handle_assume_role_with_web_identity(body: AssumeRoleRequest) -> S3Result<S3Response<(StatusCode, Body)>> {
    if body.web_identity_token.is_empty() {
        return Err(s3_error!(InvalidArgument, "WebIdentityToken is required"));
    }

    if body.version.as_str() != ASSUME_ROLE_VERSION {
        return Err(s3_error!(InvalidArgument, "not support version"));
    }

    let mut duration = if body.duration_seconds > 0 {
        body.duration_seconds
    } else {
        3600
    };

    // Enforce reasonable bounds for STS credentials duration (similar to AWS STS)
    duration = duration.clamp(900, 43200);
    let federation = current_federated_identity_service().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;
    let session = federation
        .assume_role_with_web_identity(
            &body.web_identity_token,
            duration,
            if body.policy.is_empty() { None } else { Some(body.policy) },
            &DefaultFederatedSessionBinding,
        )
        .await
        .map_err(|error| {
            if let FederationError::TokenVerification(message) = &error {
                warn!("AssumeRoleWithWebIdentity JWT verification failed: {}", message);
            }
            web_identity_federation_error(error)
        })?;
    let authorization = &session.authorization;
    let new_cred = &session.credentials;
    let subject = authorization.claims.session_identity();

    // Build XML response (AssumeRoleWithWebIdentityResponse)
    let expiration = new_cred
        .expiration
        .unwrap_or_else(|| OffsetDateTime::now_utc().saturating_add(Duration::seconds(3600)));
    let exp_str = expiration
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>{}</AccessKeyId>
      <SecretAccessKey>{}</SecretAccessKey>
      <SessionToken>{}</SessionToken>
      <Expiration>{}</Expiration>
    </Credentials>
    <SubjectFromWebIdentityToken>{}</SubjectFromWebIdentityToken>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#,
        xml_escape(&new_cred.access_key),
        xml_escape(&new_cred.secret_key),
        xml_escape(&new_cred.session_token),
        xml_escape(&exp_str),
        xml_escape(&subject),
    );

    let mut resp = S3Response::new((StatusCode::OK, Body::from(xml.into_bytes())));
    resp.headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(resp)
}

/// Escape special XML characters in a string.
fn xml_escape(s: &str) -> String {
    // Fast path: if there are no escapable characters, just clone the string.
    if !s.chars().any(|c| matches!(c, '&' | '<' | '>' | '"' | '\'')) {
        return s.to_owned();
    }

    // Slow path: build the escaped string in a single pass.
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("hello"), "hello");
        assert_eq!(xml_escape("<script>"), "&lt;script&gt;");
        assert_eq!(xml_escape("a&b"), "a&amp;b");
        assert_eq!(xml_escape("\"quoted\""), "&quot;quoted&quot;");
        assert_eq!(xml_escape("it's"), "it&apos;s");
    }

    #[test]
    fn test_duration_clamping() {
        // Simulates the clamping logic from handle_assume_role_with_web_identity
        let clamp = |d: usize| if d > 0 { d.clamp(900, 43200) } else { 3600 };

        assert_eq!(clamp(0), 3600); // default
        assert_eq!(clamp(100), 900); // clamped to min
        assert_eq!(clamp(900), 900); // exact min
        assert_eq!(clamp(3600), 3600); // normal
        assert_eq!(clamp(43200), 43200); // exact max
        assert_eq!(clamp(999999), 43200); // clamped to max
    }

    #[test]
    fn test_assume_role_duration_is_clamped_to_max() {
        // Regression: the standard AssumeRole path previously used the raw client-supplied
        // DurationSeconds with no upper bound, allowing near-permanent temporary credentials.
        let ten_years_secs: usize = 315_360_000;
        assert_eq!(clamp_assume_role_duration(ten_years_secs), STS_MAX_DURATION_SECS);
        assert_eq!(STS_MAX_DURATION_SECS, 43200);
        assert_eq!(clamp_assume_role_duration(0), STS_DEFAULT_DURATION_SECS);
        assert_eq!(clamp_assume_role_duration(60), STS_MIN_DURATION_SECS);
        assert_eq!(clamp_assume_role_duration(3600), 3600);
        assert_eq!(clamp_assume_role_duration(43200), 43200);

        // The exp timestamp derived from a huge duration must not exceed now + 12h.
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let exp = now.saturating_add(clamp_assume_role_duration(ten_years_secs) as i64);
        assert!(exp - now <= STS_MAX_DURATION_SECS as i64);
    }

    #[test]
    fn web_identity_errors_preserve_existing_s3_semantics() {
        let cases = [
            (
                FederationError::TokenVerification("invalid token".to_string()),
                S3ErrorCode::AccessDenied,
                "token verification failed: invalid token",
            ),
            (
                FederationError::NoAuthorizationContext,
                S3ErrorCode::InvalidArgument,
                "no policies are available for this OIDC token",
            ),
            (
                FederationError::Binding(FederatedSessionBindingError::InvalidRequest("invalid policy".to_string())),
                S3ErrorCode::InvalidRequest,
                "invalid policy",
            ),
            (
                FederationError::Binding(FederatedSessionBindingError::Internal("failed to store temp user".to_string())),
                S3ErrorCode::InternalError,
                "failed to store temp user",
            ),
        ];

        for (error, expected_code, expected_message) in cases {
            let error = web_identity_federation_error(error);
            assert_eq!(error.code(), &expected_code);
            assert_eq!(error.message(), Some(expected_message));
        }
    }
}
