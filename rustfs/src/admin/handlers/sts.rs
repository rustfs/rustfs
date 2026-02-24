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
use crate::{
    admin::router::{AdminOperation, Operation, S3Router},
    auth::{check_key_valid, get_session_token},
    server::ADMIN_PREFIX,
};
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::bucket::utils::serialize;
use rustfs_iam::{manager::get_token_signing_key, oidc::OidcClaims, sys::SESSION_POLICY_NAME};
use rustfs_policy::{auth::get_new_credentials_with_metadata, policy::Policy};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{AssumeRoleOutput, Credentials, Timestamp},
    s3_error,
};
use serde::Deserialize;
use serde_json::Value;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use tracing::{debug, error, info, warn};

const ASSUME_ROLE_ACTION: &str = "AssumeRole";
const ASSUME_ROLE_WITH_WEB_IDENTITY_ACTION: &str = "AssumeRoleWithWebIdentity";
const ASSUME_ROLE_VERSION: &str = "2011-06-15";

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
            ASSUME_ROLE_ACTION => handle_assume_role(req.credentials, req.uri, req.headers, body).await,
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
    body: AssumeRoleRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let Some(user) = credentials else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let session_token = get_session_token(&uri, &headers);
    if session_token.is_some() {
        return Err(s3_error!(InvalidRequest, "AccessDenied1"));
    }

    let (cred, _owner) = check_key_valid(get_session_token(&uri, &headers).unwrap_or_default(), &user.access_key).await?;

    // TODO: Check permissions, do not allow STS access
    if cred.is_temp() || cred.is_service_account() {
        return Err(s3_error!(InvalidRequest, "AccessDenied"));
    }

    if body.version.as_str() != ASSUME_ROLE_VERSION {
        return Err(s3_error!(InvalidArgument, "not support version"));
    }

    let mut claims = cred.claims.unwrap_or_default();

    populate_session_policy(&mut claims, &body.policy)?;

    let exp = {
        if body.duration_seconds > 0 {
            body.duration_seconds
        } else {
            3600
        }
    };

    claims.insert(
        "exp".to_string(),
        Value::Number(serde_json::Number::from(OffsetDateTime::now_utc().unix_timestamp() + exp as i64)),
    );

    claims.insert("parent".to_string(), Value::String(cred.access_key.clone()));

    let Ok(iam_store) = rustfs_iam::get() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    if let Err(_err) = iam_store.policy_db_get(&cred.access_key, &cred.groups).await {
        error!(
            "AssumeRole get policy failed, err: {:?}, access_key: {:?}, groups: {:?}",
            _err, cred.access_key, cred.groups
        );
        return Err(s3_error!(InvalidArgument, "invalid policy arg"));
    }

    let Some(secret) = get_token_signing_key() else {
        return Err(s3_error!(InvalidArgument, "global active sk not init"));
    };

    info!("AssumeRole get claims {:?}", &claims);

    let mut new_cred = get_new_credentials_with_metadata(&claims, &secret)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get new cred failed {e}")))?;

    new_cred.parent_user = cred.access_key.clone();

    debug!("AssumeRole get new_cred {:?}", &new_cred);

    if let Err(_err) = iam_store.set_temp_user(&new_cred.access_key, &new_cred, None).await {
        return Err(s3_error!(InternalError, "set_temp_user failed"));
    }

    // TODO: globalSiteReplicationSys

    let resp = AssumeRoleOutput {
        credentials: Some(Credentials {
            access_key_id: new_cred.access_key,
            expiration: Timestamp::from(
                new_cred
                    .expiration
                    .unwrap_or(OffsetDateTime::now_utc().saturating_add(Duration::seconds(3600))),
            ),
            secret_access_key: new_cred.secret_key,
            session_token: new_cred.session_token,
        }),
        ..Default::default()
    };

    // getAssumeRoleCredentials
    let output = serialize::<AssumeRoleOutput>(&resp).unwrap();

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

    // Verify the JWT and extract claims
    let oidc_sys = rustfs_iam::get_oidc().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

    let (claims, provider_id) = oidc_sys
        .verify_web_identity_token(&body.web_identity_token)
        .await
        .map_err(|e| {
            warn!("AssumeRoleWithWebIdentity JWT verification failed: {}", e);
            S3Error::with_message(S3ErrorCode::AccessDenied, format!("token verification failed: {e}"))
        })?;

    // Map claims to policies and groups
    let (policies, groups) = oidc_sys.map_claims_to_policies(&provider_id, &claims);

    info!(
        "AssumeRoleWithWebIdentity: user='{}', provider='{}', policies={:?}, groups={:?}",
        claims.username, provider_id, policies, groups
    );

    let mut duration = if body.duration_seconds > 0 {
        body.duration_seconds
    } else {
        3600
    };

    // Enforce reasonable bounds for STS credentials duration (similar to AWS STS)
    if duration < 900 {
        duration = 900;
    } else if duration > 43200 {
        duration = 43200;
    }
    // Generate STS credentials using the shared helper
    let new_cred = create_oidc_sts_credentials(
        &claims,
        &provider_id,
        &policies,
        &groups,
        duration,
        if body.policy.is_empty() { None } else { Some(&body.policy) },
    )
    .await?;

    let subject = if claims.sub.is_empty() {
        claims.email.clone()
    } else {
        claims.sub.clone()
    };

    // Build XML response (AssumeRoleWithWebIdentityResponse)
    let expiration = new_cred
        .expiration
        .unwrap_or(OffsetDateTime::now_utc().saturating_add(Duration::seconds(3600)));
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
        .insert(http::header::CONTENT_TYPE, "application/xml".parse().unwrap());
    Ok(resp)
}

/// Shared helper to generate STS credentials from OIDC claims.
/// Used by both the OIDC callback handler and AssumeRoleWithWebIdentity.
pub async fn create_oidc_sts_credentials(
    claims: &OidcClaims,
    provider_id: &str,
    policies: &[String],
    groups: &[String],
    duration_seconds: usize,
    session_policy: Option<&str>,
) -> S3Result<rustfs_credentials::Credentials> {
    let mut token_claims: HashMap<String, Value> = HashMap::new();
    token_claims.insert("sub".to_string(), Value::String(claims.sub.clone()));
    token_claims.insert("iss".to_string(), Value::String("rustfs-oidc".to_string()));
    token_claims.insert("oidc_provider".to_string(), Value::String(provider_id.to_string()));

    if !claims.email.is_empty() {
        token_claims.insert("email".to_string(), Value::String(claims.email.clone()));
    }
    if !claims.username.is_empty() {
        token_claims.insert("preferred_username".to_string(), Value::String(claims.username.clone()));
    }
    if !groups.is_empty() {
        token_claims.insert(
            "groups".to_string(),
            Value::Array(groups.iter().map(|g| Value::String(g.clone())).collect()),
        );
    }

    // Set expiration
    let exp = OffsetDateTime::now_utc().saturating_add(Duration::seconds(duration_seconds as i64));
    token_claims.insert("exp".to_string(), Value::Number(serde_json::Number::from(exp.unix_timestamp())));

    // Set the parent user: prefer email, then username, then sub
    let parent_user = if !claims.email.is_empty() {
        claims.email.clone()
    } else if !claims.username.is_empty() {
        claims.username.clone()
    } else if !claims.sub.is_empty() {
        claims.sub.clone()
    } else {
        "oidc-user-unknown".to_string()
    };
    info!(
        "OIDC STS credential: parent_user='{}' (email='{}', username='{}', sub='{}')",
        parent_user, claims.email, claims.username, claims.sub
    );
    token_claims.insert("parent".to_string(), Value::String(parent_user.clone()));

    // Set policies as a comma-separated string
    if !policies.is_empty() {
        token_claims.insert("policy".to_string(), Value::String(policies.join(",")));
    }

    // Optionally apply session policy
    if let Some(policy_str) = session_policy {
        populate_session_policy(&mut token_claims, policy_str)?;
    }

    // Generate STS temp credentials
    let secret = get_token_signing_key().ok_or_else(|| s3_error!(InternalError, "token signing key not initialized"))?;

    let mut new_cred = get_new_credentials_with_metadata(&token_claims, &secret)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("credential generation failed: {e}")))?;

    new_cred.parent_user = parent_user;
    new_cred.groups = Some(groups.to_vec());

    // Store temp user in IAM
    let iam_store = rustfs_iam::get().map_err(|_| s3_error!(InternalError, "IAM not initialized"))?;

    iam_store
        .set_temp_user(&new_cred.access_key, &new_cred, None)
        .await
        .map_err(|_| s3_error!(InternalError, "failed to store temp user"))?;

    Ok(new_cred)
}

pub fn populate_session_policy(claims: &mut HashMap<String, Value>, policy: &str) -> S3Result<()> {
    if !policy.is_empty() {
        let session_policy = Policy::parse_config(policy.as_bytes())
            .map_err(|e| {
                let error_msg = format!("Failed to parse session policy: {}. Please check that the policy is valid JSON format with standard brackets [] for arrays.", e);
                S3Error::with_message(S3ErrorCode::InvalidRequest, error_msg)
            })?;
        if session_policy.version.is_empty() {
            return Err(s3_error!(InvalidRequest, "invalid policy"));
        }

        let policy_buf = serde_json::to_vec(&session_policy)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal policy err {e}")))?;

        if policy_buf.len() > 2048 {
            return Err(s3_error!(InvalidRequest, "policy too large"));
        }

        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
        );
    }

    Ok(())
}

/// Escape special XML characters in a string.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
}
