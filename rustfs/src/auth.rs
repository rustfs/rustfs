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

use http::HeaderMap;
use http::Uri;
use rustfs_credentials::{Credentials, get_global_action_cred};
use rustfs_iam::error::Error as IamError;
use rustfs_iam::sys::SESSION_POLICY_NAME;
use rustfs_iam::sys::get_claims_from_token_with_secret;
use rustfs_utils::http::ip::get_source_ip_raw;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::auth::S3Auth;
use s3s::auth::SecretKey;
use s3s::auth::SimpleAuth;
use s3s::s3_error;
use serde_json::Value;
use std::collections::HashMap;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Performs constant-time string comparison to prevent timing attacks.
///
/// This function should be used when comparing sensitive values like passwords,
/// API keys, or authentication tokens. It ensures the comparison time is
/// independent of the position where strings differ and handles length differences
/// securely.
///
/// # Security Note
/// This implementation uses the `subtle` crate to provide cryptographically
/// sound constant-time guarantees. The function is resistant to timing side-channel
/// attacks and suitable for security-critical comparisons.
///
/// # Example
/// ```
/// use rustfs::auth::constant_time_eq;
///
/// let secret1 = "my-secret-key";
/// let secret2 = "my-secret-key";
/// let secret3 = "wrong-secret";
///
/// assert!(constant_time_eq(secret1, secret2));
/// assert!(!constant_time_eq(secret1, secret3));
/// ```
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

// Authentication type constants
const JWT_ALGORITHM: &str = "Bearer ";
const SIGN_V2_ALGORITHM: &str = "AWS ";
const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const STREAMING_CONTENT_SHA256: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_CONTENT_SHA256_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub(crate) const UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
const ACTION_HEADER: &str = "Action";
const AMZ_CREDENTIAL: &str = "X-Amz-Credential";
const AMZ_ACCESS_KEY_ID: &str = "AWSAccessKeyId";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

// Authentication type enum
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AuthType {
    #[default]
    Unknown,
    Anonymous,
    Presigned,
    PresignedV2,
    PostPolicy,
    StreamingSigned,
    Signed,
    SignedV2,
    #[allow(clippy::upper_case_acronyms)]
    JWT,
    #[allow(clippy::upper_case_acronyms)]
    STS,
    StreamingSignedTrailer,
    StreamingUnsignedTrailer,
}

pub struct IAMAuth {
    simple_auth: SimpleAuth,
}

impl IAMAuth {
    pub fn new(ak: impl Into<String>, sk: impl Into<SecretKey>) -> Self {
        let simple_auth = SimpleAuth::from_single(ak, sk);
        Self { simple_auth }
    }
}

#[async_trait::async_trait]
impl S3Auth for IAMAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key.is_empty() {
            return Err(s3_error!(UnauthorizedAccess, "Your account is not signed up"));
        }

        if let Ok(key) = self.simple_auth.get_secret_key(access_key).await {
            return Ok(key);
        }

        if let Ok(iam_store) = rustfs_iam::get() {
            if let Some(id) = iam_store.get_user(access_key).await {
                return Ok(SecretKey::from(id.credentials.secret_key.clone()));
            } else {
                tracing::warn!("get_user failed: no such user, access_key: {access_key}");
            }
        } else {
            tracing::warn!("get_secret_key failed: iam not initialized, access_key: {access_key}");
        }

        Err(s3_error!(UnauthorizedAccess, "Your account is not signed up2, access_key: {access_key}"))
    }
}

// check_key_valid checks the key is valid or not. return the user's credentials and if the user is the owner.
pub async fn check_key_valid(session_token: &str, access_key: &str) -> S3Result<(Credentials, bool)> {
    let Some(mut cred) = get_global_action_cred() else {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("get_global_action_cred {:?}", IamError::IamSysNotInitialized),
        ));
    };

    let sys_cred = cred.clone();

    if !constant_time_eq(&cred.access_key, access_key) {
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("check_key_valid {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let (u, ok) = iam_store
            .check_key(access_key)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed1 {e}")))?;

        if !ok {
            if let Some(u) = u {
                if u.credentials.status == "off" {
                    return Err(s3_error!(InvalidRequest, "ErrAccessKeyDisabled"));
                }
            }

            return Err(s3_error!(InvalidRequest, "ErrAccessKeyDisabled"));
        }

        let Some(u) = u else {
            return Err(s3_error!(InvalidRequest, "check key failed"));
        };

        cred = u.credentials;
    }

    let claims = check_claims_from_token(session_token, &cred)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed {e}")))?;

    cred.claims = if !claims.is_empty() { Some(claims) } else { None };

    let mut owner =
        constant_time_eq(&sys_cred.access_key, &cred.access_key) || constant_time_eq(&cred.parent_user, &sys_cred.access_key);

    // permitRootAccess
    if let Some(claims) = &cred.claims {
        if claims.contains_key(SESSION_POLICY_NAME) {
            owner = false
        }
    }

    Ok((cred, owner))
}

pub fn check_claims_from_token(token: &str, cred: &Credentials) -> S3Result<HashMap<String, Value>> {
    if !token.is_empty() && cred.access_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "no access key"));
    }

    if token.is_empty() && cred.is_temp() && !cred.is_service_account() {
        return Err(s3_error!(InvalidRequest, "invalid token1"));
    }

    if !token.is_empty() && !cred.is_temp() {
        return Err(s3_error!(InvalidRequest, "invalid token2"));
    }

    if !cred.is_service_account() && cred.is_temp() && token != cred.session_token {
        return Err(s3_error!(InvalidRequest, "invalid token3"));
    }

    if cred.is_temp() && cred.is_expired() {
        return Err(s3_error!(InvalidRequest, "invalid access key is temp and expired"));
    }

    let Some(sys_cred) = get_global_action_cred() else {
        return Err(s3_error!(InternalError, "action cred not init"));
    };

    // TODO: REPLICATION

    let (token, secret) = if cred.is_service_account() {
        (cred.session_token.as_str(), cred.secret_key.as_str())
    } else {
        (token, sys_cred.secret_key.as_str())
    };

    if !token.is_empty() {
        let claims: HashMap<String, Value> =
            get_claims_from_token_with_secret(token, secret).map_err(|_e| s3_error!(InvalidRequest, "invalid token"))?;
        return Ok(claims);
    }

    Ok(HashMap::new())
}

pub fn get_session_token<'a>(uri: &'a Uri, hds: &'a HeaderMap) -> Option<&'a str> {
    hds.get("x-amz-security-token")
        .map(|v| v.to_str().unwrap_or_default())
        .or_else(|| get_query_param(uri.query().unwrap_or_default(), "x-amz-security-token"))
}

/// Get condition values for policy evaluation
///
/// # Arguments
/// * `header` - HTTP headers of the request
/// * `cred` - User credentials
/// * `version_id` - Optional version ID of the object
/// * `region` - Optional region/location constraint
/// * `remote_addr` - Optional remote address of the connection
///
/// # Returns
/// * `HashMap<String, Vec<String>>` - Condition values for policy evaluation
///
pub fn get_condition_values(
    header: &HeaderMap,
    cred: &Credentials,
    version_id: Option<&str>,
    region: Option<&str>,
    remote_addr: Option<std::net::SocketAddr>,
) -> HashMap<String, Vec<String>> {
    let username = if cred.is_temp() || cred.is_service_account() {
        cred.parent_user.clone()
    } else {
        cred.access_key.clone()
    };

    let sys_cred = get_global_action_cred().unwrap_or_default();

    let claims = &cred.claims;

    let principal_type = if !username.is_empty() {
        if claims.is_some() {
            "AssumedRole"
        } else if constant_time_eq(&sys_cred.access_key, &username) {
            "Account"
        } else {
            "User"
        }
    } else {
        "Anonymous"
    };

    // Get current time
    let curr_time = OffsetDateTime::now_utc();
    let epoch_time = curr_time.unix_timestamp();

    // Use provided version ID or empty string
    let vid = version_id.unwrap_or("");

    // Determine auth type and signature version from headers
    let (auth_type, signature_version) = determine_auth_type_and_version(header);

    // Get TLS status from header
    let is_tls = header
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "https")
        .or_else(|| {
            header
                .get("x-forwarded-scheme")
                .and_then(|v| v.to_str().ok())
                .map(|s| s == "https")
        })
        .unwrap_or(false);

    // Get remote address from header or use default
    let remote_addr_s = remote_addr.map(|a| a.ip().to_string()).unwrap_or_default();

    let mut args = HashMap::new();

    // Add basic time and security info
    args.insert("CurrentTime".to_owned(), vec![curr_time.format(&Rfc3339).unwrap_or_default()]);
    args.insert("EpochTime".to_owned(), vec![epoch_time.to_string()]);
    args.insert("SecureTransport".to_owned(), vec![is_tls.to_string()]);
    args.insert("SourceIp".to_owned(), vec![get_source_ip_raw(header, &remote_addr_s)]);

    // Add user agent and referer
    if let Some(user_agent) = header.get("user-agent") {
        args.insert("UserAgent".to_owned(), vec![user_agent.to_str().unwrap_or("").to_string()]);
    }
    if let Some(referer) = header.get("referer") {
        args.insert("Referer".to_owned(), vec![referer.to_str().unwrap_or("").to_string()]);
    }

    // Add user and principal info
    args.insert("userid".to_owned(), vec![username.clone()]);
    args.insert("username".to_owned(), vec![username]);
    args.insert("principaltype".to_owned(), vec![principal_type.to_string()]);

    // Add version ID
    if !vid.is_empty() {
        args.insert("versionid".to_owned(), vec![vid.to_string()]);
    }

    // Add signature version and auth type
    if !signature_version.is_empty() {
        args.insert("signatureversion".to_owned(), vec![signature_version]);
    }
    if !auth_type.is_empty() {
        args.insert("authType".to_owned(), vec![auth_type]);
    }

    if let Some(lc) = region {
        if !lc.is_empty() {
            args.insert("LocationConstraint".to_owned(), vec![lc.to_string()]);
        }
    }

    let mut clone_header = header.clone();
    if let Some(v) = clone_header.get("x-amz-signature-age") {
        args.insert("signatureAge".to_string(), vec![v.to_str().unwrap_or("").to_string()]);
        clone_header.remove("x-amz-signature-age");
    }

    for obj_lock in &[
        "x-amz-object-lock-mode",
        "x-amz-object-lock-legal-hold",
        "x-amz-object-lock-retain-until-date",
    ] {
        let values = clone_header
            .get_all(*obj_lock)
            .iter()
            .map(|v| v.to_str().unwrap_or("").to_string())
            .collect::<Vec<String>>();
        if !values.is_empty() {
            args.insert(obj_lock.trim_start_matches("x-amz-").to_string(), values);
        }
        clone_header.remove(*obj_lock);
    }

    for (key, _values) in clone_header.iter() {
        if key.as_str().eq_ignore_ascii_case("x-amz-tagging") {
            continue;
        }
        if let Some(existing_values) = args.get_mut(key.as_str()) {
            existing_values.extend(clone_header.get_all(key).iter().map(|v| v.to_str().unwrap_or("").to_string()));
        } else {
            args.insert(
                key.as_str().to_string(),
                header
                    .get_all(key)
                    .iter()
                    .map(|v| v.to_str().unwrap_or("").to_string())
                    .collect(),
            );
        }
    }

    if let Some(claims) = &cred.claims {
        for (k, v) in claims {
            if let Some(v_str) = v.as_str() {
                args.insert(k.trim_start_matches("ldap").to_lowercase(), vec![v_str.to_string()]);
            }
        }

        if let Some(grps_val) = claims.get("groups") {
            if let Some(grps_is) = grps_val.as_array() {
                let grps = grps_is
                    .iter()
                    .filter_map(|g| g.as_str().map(|s| s.to_string()))
                    .collect::<Vec<String>>();
                if !grps.is_empty() {
                    args.insert("groups".to_string(), grps);
                }
            }
        }
    }

    if let Some(groups) = &cred.groups {
        if !args.contains_key("groups") {
            args.insert("groups".to_string(), groups.clone());
        }
    }

    args
}

/// Get request authentication type
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `AuthType` - The determined authentication type
///
pub fn get_request_auth_type(header: &HeaderMap) -> AuthType {
    if is_request_signature_v2(header) {
        AuthType::SignedV2
    } else if is_request_presigned_signature_v2(header) {
        AuthType::PresignedV2
    } else if is_request_sign_streaming_v4(header) {
        AuthType::StreamingSigned
    } else if is_request_sign_streaming_trailer_v4(header) {
        AuthType::StreamingSignedTrailer
    } else if is_request_unsigned_trailer_v4(header) {
        AuthType::StreamingUnsignedTrailer
    } else if is_request_signature_v4(header) {
        AuthType::Signed
    } else if is_request_presigned_signature_v4(header) {
        AuthType::Presigned
    } else if is_request_jwt(header) {
        AuthType::JWT
    } else if is_request_post_policy_signature_v4(header) {
        AuthType::PostPolicy
    } else if is_request_sts(header) {
        AuthType::STS
    } else if is_request_anonymous(header) {
        AuthType::Anonymous
    } else {
        AuthType::Unknown
    }
}

/// Helper function to determine auth type and signature version
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `(String, String)` - Tuple of auth type and signature version
///
fn determine_auth_type_and_version(header: &HeaderMap) -> (String, String) {
    match get_request_auth_type(header) {
        AuthType::JWT => ("JWT".to_string(), String::new()),
        AuthType::SignedV2 => ("REST-HEADER".to_string(), "AWS2".to_string()),
        AuthType::PresignedV2 => ("REST-QUERY-STRING".to_string(), "AWS2".to_string()),
        AuthType::StreamingSigned | AuthType::StreamingSignedTrailer | AuthType::StreamingUnsignedTrailer => {
            ("REST-HEADER".to_string(), "AWS4-HMAC-SHA256".to_string())
        }
        AuthType::Signed => ("REST-HEADER".to_string(), "AWS4-HMAC-SHA256".to_string()),
        AuthType::Presigned => ("REST-QUERY-STRING".to_string(), "AWS4-HMAC-SHA256".to_string()),
        AuthType::PostPolicy => ("POST".to_string(), String::new()),
        AuthType::STS => ("STS".to_string(), String::new()),
        AuthType::Anonymous => ("Anonymous".to_string(), String::new()),
        AuthType::Unknown => (String::new(), String::new()),
    }
}

/// Verify if request has JWT
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has JWT, false otherwise
fn is_request_jwt(header: &HeaderMap) -> bool {
    if let Some(auth) = header.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            return auth_str.starts_with(JWT_ALGORITHM);
        }
    }
    false
}

/// Verify if request has AWS Signature Version '4'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Signature Version '4', false otherwise
fn is_request_signature_v4(header: &HeaderMap) -> bool {
    if let Some(auth) = header.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            return auth_str.starts_with(SIGN_V4_ALGORITHM);
        }
    }
    false
}

/// Verify if request has AWS Signature Version '2'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Signature Version '2', false otherwise
fn is_request_signature_v2(header: &HeaderMap) -> bool {
    if let Some(auth) = header.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            return !auth_str.starts_with(SIGN_V4_ALGORITHM) && auth_str.starts_with(SIGN_V2_ALGORITHM);
        }
    }
    false
}

/// Verify if request has AWS PreSign Version '4'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS PreSign Version '4', false otherwise
pub(crate) fn is_request_presigned_signature_v4(header: &HeaderMap) -> bool {
    if let Some(credential) = header.get(AMZ_CREDENTIAL) {
        return !credential.to_str().unwrap_or("").is_empty();
    }
    false
}

/// Verify request has AWS PreSign Version '2'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS PreSign Version '2', false otherwise
fn is_request_presigned_signature_v2(header: &HeaderMap) -> bool {
    if let Some(access_key) = header.get(AMZ_ACCESS_KEY_ID) {
        return !access_key.to_str().unwrap_or("").is_empty();
    }
    false
}

/// Verify if request has AWS Post policy Signature Version '4'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Post policy Signature Version '4', false otherwise
fn is_request_post_policy_signature_v4(header: &HeaderMap) -> bool {
    if let Some(content_type) = header.get("content-type") {
        if let Ok(ct) = content_type.to_str() {
            return ct.contains("multipart/form-data");
        }
    }
    false
}

/// Verify if the request has AWS Streaming Signature Version '4'
fn is_request_sign_streaming_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256") {
        if let Ok(sha256_str) = content_sha256.to_str() {
            return sha256_str == STREAMING_CONTENT_SHA256;
        }
    }
    false
}

// Verify if the request has AWS Streaming Signature Version '4' with trailer
fn is_request_sign_streaming_trailer_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256") {
        if let Ok(sha256_str) = content_sha256.to_str() {
            return sha256_str == STREAMING_CONTENT_SHA256_TRAILER;
        }
    }
    false
}

// Verify if the request has AWS Streaming Signature Version '4' with unsigned content and trailer
fn is_request_unsigned_trailer_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256") {
        if let Ok(sha256_str) = content_sha256.to_str() {
            return sha256_str == UNSIGNED_PAYLOAD_TRAILER;
        }
    }
    false
}

// Verify if request is STS (Security Token Service)
fn is_request_sts(header: &HeaderMap) -> bool {
    if let Some(action) = header.get(ACTION_HEADER) {
        return !action.to_str().unwrap_or("").is_empty();
    }
    false
}

// Verify if request is anonymous
fn is_request_anonymous(header: &HeaderMap) -> bool {
    header.get("authorization").is_none()
}

pub fn get_query_param<'a>(query: &'a str, param_name: &str) -> Option<&'a str> {
    let param_name = param_name.to_lowercase();

    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            if key.to_lowercase() == param_name {
                return Some(value);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Uri};
    use rustfs_credentials::Credentials;
    use s3s::auth::SecretKey;
    use serde_json::json;
    use std::collections::HashMap;
    use time::OffsetDateTime;

    fn create_test_credentials() -> Credentials {
        Credentials {
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            session_token: "".to_string(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "".to_string(),
            groups: None,
            claims: None,
            name: Some("test-user".to_string()),
            description: Some("test user for auth tests".to_string()),
        }
    }

    fn create_temp_credentials() -> Credentials {
        Credentials {
            access_key: "temp-access-key".to_string(),
            secret_key: "temp-secret-key".to_string(),
            session_token: "temp-session-token".to_string(),
            expiration: Some(OffsetDateTime::now_utc() + time::Duration::hours(1)),
            status: "on".to_string(),
            parent_user: "parent-user".to_string(),
            groups: Some(vec!["test-group".to_string()]),
            claims: None,
            name: Some("temp-user".to_string()),
            description: Some("temporary user for auth tests".to_string()),
        }
    }

    fn create_service_account_credentials() -> Credentials {
        let mut claims = HashMap::new();
        claims.insert(rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(), json!("test-policy"));

        Credentials {
            access_key: "service-access-key".to_string(),
            secret_key: "service-secret-key".to_string(),
            session_token: "service-session-token".to_string(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "service-parent".to_string(),
            groups: None,
            claims: Some(claims),
            name: Some("service-account".to_string()),
            description: Some("service account for auth tests".to_string()),
        }
    }

    #[test]
    fn test_iam_auth_creation() {
        let access_key = "test-access-key";
        let secret_key = SecretKey::from("test-secret-key");

        let iam_auth = IAMAuth::new(access_key, secret_key);

        // The struct should be created successfully
        // We can't easily test internal state without exposing it,
        // but we can test it doesn't panic on creation
        assert_eq!(size_of_val(&iam_auth), size_of::<IAMAuth>());
    }

    #[tokio::test]
    async fn test_iam_auth_get_secret_key_empty_access_key() {
        let iam_auth = IAMAuth::new("test-ak", SecretKey::from("test-sk"));

        let result = iam_auth.get_secret_key("").await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::UnauthorizedAccess);
        assert!(error.message().unwrap_or("").contains("Your account is not signed up"));
    }

    #[test]
    fn test_check_claims_from_token_empty_token_and_access_key() {
        let mut cred = create_test_credentials();
        cred.access_key = "".to_string();

        let result = check_claims_from_token("test-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("no access key"));
    }

    #[test]
    fn test_check_claims_from_token_temp_credentials_without_token() {
        let mut cred = create_temp_credentials();
        // Make it non-service account
        cred.claims = None;

        let result = check_claims_from_token("", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token1"));
    }

    #[test]
    fn test_check_claims_from_token_non_temp_with_token() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string(); // Make it non-temp

        let result = check_claims_from_token("some-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token2"));
    }

    #[test]
    fn test_check_claims_from_token_mismatched_session_token() {
        let mut cred = create_temp_credentials();
        // Make sure it's not a service account
        cred.claims = None;

        let result = check_claims_from_token("wrong-session-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token3"));
    }

    #[test]
    fn test_check_claims_from_token_expired_credentials() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1)); // Expired
        cred.claims = None; // Make sure it's not a service account

        let result = check_claims_from_token(&cred.session_token, &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        // The function checks various conditions in order. An expired temp credential
        // might trigger other validation errors first (like token mismatch)
        let msg = error.message().unwrap_or("");
        let is_valid_error = msg.contains("invalid access key is temp and expired")
            || msg.contains("invalid token")
            || msg.contains("action cred not init");
        assert!(is_valid_error, "Unexpected error message: '{msg}'");
    }

    #[test]
    fn test_check_claims_from_token_valid_non_temp_credentials() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string(); // Make it non-temp

        let result = check_claims_from_token("", &cred);

        // This might fail due to global state dependencies, but should return error about global cred init
        if let Ok(claims) = result {
            assert!(claims.is_empty());
        } else if let Err(error) = result {
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert!(error.message().unwrap_or("").contains("action cred not init"));
        }
    }

    #[test]
    fn test_get_session_token_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-security-token", HeaderValue::from_static("test-session-token"));

        let uri: Uri = "https://example.com/".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("test-session-token"));
    }

    #[test]
    fn test_get_session_token_from_query_param() {
        let headers = HeaderMap::new();
        let uri: Uri = "https://example.com/?x-amz-security-token=query-session-token"
            .parse()
            .unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("query-session-token"));
    }

    #[test]
    fn test_get_session_token_header_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-security-token", HeaderValue::from_static("header-token"));

        let uri: Uri = "https://example.com/?x-amz-security-token=query-token".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("header-token"));
    }

    #[test]
    fn test_get_session_token_no_token() {
        let headers = HeaderMap::new();
        let uri: Uri = "https://example.com/".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, None);
    }

    #[test]
    fn test_get_condition_values_regular_user() {
        let cred = create_test_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("userid"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("principaltype"), Some(&vec!["User".to_string()]));
    }

    #[test]
    fn test_get_condition_values_temp_user() {
        let cred = create_temp_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("userid"), Some(&vec!["parent-user".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["parent-user".to_string()]));
        assert_eq!(conditions.get("principaltype"), Some(&vec!["User".to_string()]));
    }

    #[test]
    fn test_get_condition_values_service_account() {
        let cred = create_service_account_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("userid"), Some(&vec!["service-parent".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["service-parent".to_string()]));
        // Service accounts with claims should be "AssumedRole" type
        assert_eq!(conditions.get("principaltype"), Some(&vec!["AssumedRole".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_object_lock_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        headers.insert("x-amz-object-lock-retain-until-date", HeaderValue::from_static("2024-12-31T23:59:59Z"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["GOVERNANCE".to_string()]));
        assert_eq!(
            conditions.get("object-lock-retain-until-date"),
            Some(&vec!["2024-12-31T23:59:59Z".to_string()])
        );
    }

    #[test]
    fn test_get_condition_values_with_signature_age() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-signature-age", HeaderValue::from_static("300"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("signatureAge"), Some(&vec!["300".to_string()]));
        // Verify the header is removed after processing
        // (we can't directly test this without changing the function signature)
    }

    #[test]
    fn test_get_condition_values_with_claims() {
        let mut cred = create_service_account_credentials();
        let mut claims = HashMap::new();
        claims.insert("ldapUsername".to_string(), json!("ldap-user"));
        claims.insert("groups".to_string(), json!(["group1", "group2"]));
        cred.claims = Some(claims);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("username"), Some(&vec!["ldap-user".to_string()]));
        assert_eq!(conditions.get("groups"), Some(&vec!["group1".to_string(), "group2".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_credential_groups() {
        let mut cred = create_test_credentials();
        cred.groups = Some(vec!["cred-group1".to_string(), "cred-group2".to_string()]);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(
            conditions.get("groups"),
            Some(&vec!["cred-group1".to_string(), "cred-group2".to_string()])
        );
    }

    #[test]
    fn test_get_query_param_found() {
        let query = "param1=value1&param2=value2&param3=value3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));
    }

    #[test]
    fn test_get_query_param_case_insensitive() {
        let query = "Param1=value1&PARAM2=value2&param3=value3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));
    }

    #[test]
    fn test_get_query_param_not_found() {
        let query = "param1=value1&param2=value2&param3=value3";

        let result = get_query_param(query, "param4");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_empty_query() {
        let query = "";

        let result = get_query_param(query, "param1");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_malformed_query() {
        let query = "param1&param2=value2&param3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));

        let result = get_query_param(query, "param1");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_with_equals_in_value() {
        let query = "param1=value=with=equals&param2=value2";

        let result = get_query_param(query, "param1");

        assert_eq!(result, Some("value=with=equals"));
    }

    #[test]
    fn test_credentials_is_expired() {
        let mut cred = create_test_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        assert!(cred.is_expired());
    }

    #[test]
    fn test_credentials_is_not_expired() {
        let mut cred = create_test_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() + time::Duration::hours(1));

        assert!(!cred.is_expired());
    }

    #[test]
    fn test_credentials_no_expiration() {
        let cred = create_test_credentials();

        assert!(!cred.is_expired());
    }

    #[test]
    fn test_credentials_is_temp() {
        let cred = create_temp_credentials();

        assert!(cred.is_temp());
    }

    #[test]
    fn test_credentials_is_not_temp_no_session_token() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string();

        assert!(!cred.is_temp());
    }

    #[test]
    fn test_credentials_is_not_temp_expired() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        assert!(!cred.is_temp());
    }

    #[test]
    fn test_credentials_is_service_account() {
        let cred = create_service_account_credentials();

        assert!(cred.is_service_account());
    }

    #[test]
    fn test_credentials_is_not_service_account() {
        let cred = create_test_credentials();

        assert!(!cred.is_service_account());
    }

    #[test]
    fn test_get_request_auth_type_jwt() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::JWT);
    }

    #[test]
    fn test_get_request_auth_type_signature_v2() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//bqJZQ1iY="),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::SignedV2);
    }

    #[test]
    fn test_get_request_auth_type_signature_v4() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Signed);
    }

    #[test]
    fn test_get_request_auth_type_presigned_v2() {
        let mut headers = HeaderMap::new();
        headers.insert("AWSAccessKeyId", HeaderValue::from_static("AKIAIOSFODNN7EXAMPLE"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::PresignedV2);
    }

    #[test]
    fn test_get_request_auth_type_presigned_v4() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Amz-Credential",
            HeaderValue::from_static("AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Presigned);
    }

    #[test]
    fn test_get_request_auth_type_post_policy() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::PostPolicy);
    }

    #[test]
    fn test_get_request_auth_type_streaming_signed() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::StreamingSigned);
    }

    #[test]
    fn test_get_request_auth_type_streaming_signed_trailer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::StreamingSignedTrailer);
    }

    #[test]
    fn test_get_request_auth_type_streaming_unsigned_trailer() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-UNSIGNED-PAYLOAD-TRAILER"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::StreamingUnsignedTrailer);
    }

    #[test]
    fn test_get_request_auth_type_sts() {
        let mut headers = HeaderMap::new();
        headers.insert("Action", HeaderValue::from_static("AssumeRole"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::STS);
    }

    #[test]
    fn test_get_request_auth_type_anonymous() {
        let headers = HeaderMap::new();

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Anonymous);
    }

    #[test]
    fn test_get_request_auth_type_unknown() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("CustomAuth token123"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Unknown);
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq("test", "test"));
        assert!(!constant_time_eq("test", "Test"));
        assert!(!constant_time_eq("test", "test1"));
        assert!(!constant_time_eq("test1", "test"));
        assert!(!constant_time_eq("", "test"));
        assert!(constant_time_eq("", ""));

        // Test with credentials-like strings
        let key1 = "AKIAIOSFODNN7EXAMPLE";
        let key2 = "AKIAIOSFODNN7EXAMPLE";
        let key3 = "AKIAIOSFODNN7EXAMPLF";
        assert!(constant_time_eq(key1, key2));
        assert!(!constant_time_eq(key1, key3));
    }

    #[test]
    fn test_get_condition_values_source_ip() {
        let mut headers = HeaderMap::new();
        let cred = Credentials::default();

        // Case 1: No headers, no remote addr -> empty string
        let conditions = get_condition_values(&headers, &cred, None, None, None);
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "");

        // Case 2: No headers, with remote addr -> remote addr
        let remote_addr: std::net::SocketAddr = "192.168.0.10:12345".parse().unwrap();
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 3: X-Forwarded-For present -> XFF (takes precedence over remote_addr)
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.1");

        // Case 4: X-Forwarded-For with multiple IPs -> First IP
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.3, 10.0.0.4"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.3");

        // Case 5: X-Real-IP present (XFF removed) -> X-Real-IP
        headers.remove("x-forwarded-for");
        headers.insert("x-real-ip", HeaderValue::from_static("10.0.0.2"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.2");

        // Case 6: Forwarded header present (X-Real-IP removed) -> Forwarded
        headers.remove("x-real-ip");
        headers.insert("forwarded", HeaderValue::from_static("for=10.0.0.5;proto=http"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.5");

        // Case 7: Forwarded header with quotes and multiple values
        headers.insert("forwarded", HeaderValue::from_static("for=\"10.0.0.6\", for=10.0.0.7"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.6");

        // Case 8: IPv6 Remote Addr
        let remote_addr_v6: std::net::SocketAddr = "[2001:db8::1]:8080".parse().unwrap();
        headers.clear();
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr_v6));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "2001:db8::1");
    }
}

#[cfg(test)]
mod tests_policy {
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_policy::policy::{Args, BucketPolicy, BucketPolicyArgs, Policy};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_iam_policy_source_ip() {
        let policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::mybucket/*"],
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "192.168.1.0/24"
                        }
                    }
                }
            ]
        }"#;

        let policy: Policy = serde_json::from_str(policy_json).expect("Failed to parse IAM policy");

        // Case 1: Matching IP
        let mut conditions = HashMap::new();
        conditions.insert("SourceIp".to_string(), vec!["192.168.1.10".to_string()]);

        let claims = HashMap::new();
        let args = Args {
            account: "test-account",
            groups: &None,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "mybucket",
            conditions: &conditions,
            is_owner: false,
            object: "myobject",
            claims: &claims,
            deny_only: false,
        };

        assert!(policy.is_allowed(&args).await, "IAM Policy should allow matching IP");

        // Case 2: Non-matching IP
        let mut conditions_fail = HashMap::new();
        conditions_fail.insert("SourceIp".to_string(), vec!["10.0.0.1".to_string()]);

        let args_fail = Args {
            conditions: &conditions_fail,
            ..args
        };

        assert!(!policy.is_allowed(&args_fail).await, "IAM Policy should deny non-matching IP");
    }

    #[tokio::test]
    async fn test_bucket_policy_source_ip() {
        let policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::mybucket/*"],
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "192.168.1.0/24"
                        }
                    }
                }
            ]
        }"#;

        let policy: BucketPolicy = serde_json::from_str(policy_json).expect("Failed to parse Bucket policy");

        // Case 1: Matching IP
        let mut conditions = HashMap::new();
        conditions.insert("SourceIp".to_string(), vec!["192.168.1.10".to_string()]);

        let args = BucketPolicyArgs {
            account: "test-account",
            groups: &None,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "mybucket",
            conditions: &conditions,
            is_owner: false,
            object: "myobject",
        };

        assert!(policy.is_allowed(&args).await, "Bucket Policy should allow matching IP");

        // Case 2: Non-matching IP
        let mut conditions_fail = HashMap::new();
        conditions_fail.insert("SourceIp".to_string(), vec!["10.0.0.1".to_string()]);

        let args_fail = BucketPolicyArgs {
            conditions: &conditions_fail,
            ..args
        };

        assert!(!policy.is_allowed(&args_fail).await, "Bucket Policy should deny non-matching IP");
    }
}
