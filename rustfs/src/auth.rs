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

use crate::runtime_sources::{AppContext, current_action_credentials, current_ready_iam_handle};
use facts::{AuthzFacts, AuthzPrincipal, AuthzTarget};
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::Uri;
use http::header::HeaderName;
use mime_guess::mime::{self, Mime};
use rustfs_credentials::Credentials;
use rustfs_iam::error::Error as IamError;
use rustfs_iam::sys::{
    SESSION_POLICY_NAME, get_claims_from_token_with_secret, get_claims_from_token_with_secret_allow_missing_exp,
};
use rustfs_policy::policy::action::Action;
use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};
use rustfs_s3_types::{S3Result as OwnedResult, s3_error as owned_error};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::MaskedAccessKey;
use rustfs_utils::http::metadata_compat::starts_with_ignore_ascii_case;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use tracing::{debug, trace, warn};
use url::form_urlencoded;

pub(crate) mod facts;

pub(crate) const LOG_COMPONENT_AUTH: &str = "auth";
pub(crate) const LOG_SUBSYSTEM_CREDENTIALS: &str = "credentials";
pub(crate) const LOG_SUBSYSTEM_KEYSTONE: &str = "keystone";
const LOG_SUBSYSTEM_REQUEST: &str = "request";
pub(crate) const EVENT_SECRET_KEY_LOOKUP_FAILED: &str = "secret_key_lookup_failed";
const EVENT_ACCESS_KEY_VALIDATION_STARTED: &str = "access_key_validation_started";
pub(crate) const EVENT_KEYSTONE_CREDENTIALS_DETECTED: &str = "keystone_credentials_detected";
const EVENT_KEYSTONE_CREDENTIALS_VALIDATED: &str = "keystone_credentials_validated";
const EVENT_KEYSTONE_CONTEXT_MISSING: &str = "keystone_context_missing";
const EVENT_SESSION_TOKEN_EXTRACTION: &str = "session_token_extraction";
const EVENT_PRESIGNED_UNSIGNED_AMZ_HEADER: &str = "presigned_unsigned_amz_header";
const EVENT_SIGV4_UNSIGNED_AMZ_HEADER: &str = "sigv4_unsigned_amz_header";
const EVENT_SIGV2_EXTENSION_HEADER: &str = "sigv2_extension_header";
const EVENT_SIGV4_UNSUPPORTED_ALGORITHM: &str = "sigv4_unsupported_algorithm";

/// RustFS-specific query capability for a single presigned PutObject request.
pub(crate) const RUSTFS_MAX_CONTENT_LENGTH_QUERY: &str = "x-rustfs-max-content-length";
pub(crate) const RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY: &str = "x-rustfs-max-total-object-size";

/// Inserted by the S3 access boundary after the upstream verifier accepts a
/// request as SigV4 presigned. Downstream capability parsing must require this
/// marker instead of treating query syntax as proof of authentication.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifiedPresignedRequest;

#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifiedSigV4Request;

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

/// Owner status of a validated Keystone identity, shared by both request edges.
pub(crate) fn keystone_is_owner(credentials: &Credentials) -> bool {
    credentials
        .claims
        .as_ref()
        .and_then(|claims| claims.get("keystone_roles"))
        .and_then(|roles| roles.as_array())
        .is_some_and(|roles| {
            roles
                .iter()
                .any(|role| matches!(role.as_str(), Some("admin" | "reseller_admin")))
        })
}

// check_key_valid checks the key is valid or not. return the user's credentials and if the user is the owner.
pub async fn check_key_valid(session_token: &str, access_key: &str) -> OwnedResult<(Credentials, bool)> {
    check_key_valid_with_context(session_token, access_key, None).await
}

fn has_root_access(sys_cred: &Credentials, cred: &Credentials) -> bool {
    (constant_time_eq(&sys_cred.access_key, &cred.access_key) || constant_time_eq(&cred.parent_user, &sys_cred.access_key))
        && !cred
            .claims
            .as_ref()
            .is_some_and(|claims| claims.contains_key(SESSION_POLICY_NAME) || rustfs_iam::sys::is_rustfs_oidc_claims(claims))
}

/// Validate an access key, resolving the root credentials and IAM system from
/// an explicit application context when one is given (backlog#1052 S6).
///
/// A per-server request path passes its own context so a second embedded
/// server authenticates against its own root identity and IAM domain instead
/// of the process defaults; `None` falls back to the ambient globals — the
/// single-instance legacy behavior that all existing callers keep.
pub async fn check_key_valid_with_context(
    session_token: &str,
    access_key: &str,
    ctx: Option<&AppContext>,
) -> OwnedResult<(Credentials, bool)> {
    // KEYSTONE INTEGRATION: Check if Keystone credentials are present in task-local storage
    // This handles both:
    // 1. Pure X-Auth-Token requests (access_key may be empty)
    // 2. Keystone access keys formatted as "keystone:user_id"
    use crate::auth_keystone;
    use rustfs_keystone::KEYSTONE_CREDENTIALS;

    // Try to get Keystone credentials from task-local storage first
    // Add debug logging for UI authentication tracking
    debug!(
        event = EVENT_ACCESS_KEY_VALIDATION_STARTED,
        component = LOG_COMPONENT_AUTH,
        subsystem = LOG_SUBSYSTEM_CREDENTIALS,
        access_key = %MaskedAccessKey(access_key),
        has_session_token = !session_token.is_empty(),
        "Access key validation started"
    );
    if let Ok(Some(credentials)) = KEYSTONE_CREDENTIALS.try_with(|creds| creds.clone()) {
        debug!(
            event = EVENT_KEYSTONE_CREDENTIALS_DETECTED,
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_KEYSTONE,
            result = "task_local",
            "Keystone task-local credentials detected"
        );

        if !auth_keystone::is_keystone_enabled() {
            return Err(owned_error!(InvalidAccessKeyId, "Keystone authentication is not enabled"));
        }

        debug!(
            event = EVENT_KEYSTONE_CREDENTIALS_VALIDATED,
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_KEYSTONE,
            principal = %MaskedAccessKey(&credentials.parent_user),
            has_project_name = credentials
                .claims
                .as_ref()
                .and_then(|c| c.get("keystone_project_name"))
                .and_then(|v| v.as_str())
                .is_some(),
            "Keystone task-local credentials validated"
        );

        let is_owner = keystone_is_owner(&credentials);

        debug!(
            event = EVENT_KEYSTONE_CREDENTIALS_VALIDATED,
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_KEYSTONE,
            principal = %MaskedAccessKey(&credentials.parent_user),
            is_owner,
            "Keystone owner permissions evaluated"
        );

        return Ok((credentials, is_owner));
    }

    // Legacy check for explicit "keystone:" prefix (for backwards compatibility)
    if access_key.starts_with("keystone:") {
        warn!(
            event = EVENT_KEYSTONE_CONTEXT_MISSING,
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_KEYSTONE,
            access_key = %MaskedAccessKey(access_key),
            "Keystone context missing for access key"
        );

        if !auth_keystone::is_keystone_enabled() {
            return Err(owned_error!(InvalidAccessKeyId, "Keystone authentication is not enabled"));
        }

        return Err(owned_error!(InvalidAccessKeyId, "Keystone authentication requires X-Auth-Token header"));
    }

    // Prefer this server's context (backlog#1052 S6); fall back to the ambient
    // process credentials when no context was threaded in.
    let root_cred = match ctx {
        Some(context) => context.action_credentials().get(),
        None => current_action_credentials(),
    };
    let Some(mut cred) = root_cred else {
        return Err(owned_error!(InternalError, "get_global_action_cred {:?}", IamError::IamSysNotInitialized));
    };

    let sys_cred = cred.clone();

    if !constant_time_eq(&cred.access_key, access_key) {
        let iam_store = match ctx {
            Some(context) if context.iam().is_ready() => Ok(context.iam().handle()),
            Some(_) => Err(()),
            None => current_ready_iam_handle().map_err(|_| ()),
        };
        let Ok(iam_store) = iam_store else {
            return Err(owned_error!(InternalError, "check_key_valid {:?}", IamError::IamSysNotInitialized));
        };

        let (u, ok) = iam_store
            .check_key(access_key)
            .await
            .map_err(|e| owned_error!(InternalError, "check claims failed1 {e}"))?;

        if !ok {
            let Some(ref u) = u else {
                warn!(
                    event = EVENT_SECRET_KEY_LOOKUP_FAILED,
                    component = LOG_COMPONENT_AUTH,
                    subsystem = LOG_SUBSYSTEM_CREDENTIALS,
                    access_key = %MaskedAccessKey(access_key),
                    reason = "user_not_found",
                    "Access key validation rejected"
                );
                return Err(owned_error!(InvalidAccessKeyId, "check key failed"));
            };

            if u.credentials.status == "off" {
                warn!(
                    event = EVENT_SECRET_KEY_LOOKUP_FAILED,
                    component = LOG_COMPONENT_AUTH,
                    subsystem = LOG_SUBSYSTEM_CREDENTIALS,
                    access_key = %MaskedAccessKey(access_key),
                    reason = "account_disabled",
                    "Access key validation rejected"
                );
                return Err(owned_error!(InvalidAccessKeyId, "check key failed"));
            }

            check_session_token(session_token, &u.credentials)?;

            warn!(
                event = EVENT_SECRET_KEY_LOOKUP_FAILED,
                component = LOG_COMPONENT_AUTH,
                subsystem = LOG_SUBSYSTEM_CREDENTIALS,
                access_key = %MaskedAccessKey(access_key),
                reason = "validation_failed",
                "Access key validation rejected"
            );
            return Err(owned_error!(InvalidRequest, "check key failed"));
        }

        let Some(u) = u else {
            return Err(owned_error!(InvalidAccessKeyId, "check key failed"));
        };

        cred = u.credentials;
    }

    let claims = check_claims_from_token_with_context(session_token, &cred, ctx)?;

    cred.claims = if !claims.is_empty() { Some(claims) } else { None };

    let owner = has_root_access(&sys_cred, &cred);
    Ok((cred, owner))
}

pub fn check_claims_from_token(token: &str, cred: &Credentials) -> OwnedResult<HashMap<String, Value>> {
    check_claims_from_token_with_context(token, cred, None)
}

fn check_session_token(token: &str, cred: &Credentials) -> OwnedResult<()> {
    // Session identity survives expiration; is_temp() only describes active credentials.
    let has_session_token = !cred.session_token.is_empty();

    if !token.is_empty() && cred.access_key.is_empty() {
        return Err(owned_error!(InvalidRequest, "no access key"));
    }

    if token.is_empty() && has_session_token && !cred.is_service_account() {
        return Err(owned_error!(InvalidRequest, "invalid token1"));
    }

    if !token.is_empty() && !has_session_token {
        return Err(owned_error!(InvalidRequest, "invalid token2"));
    }

    if !cred.is_service_account() && has_session_token && !constant_time_eq(token, &cred.session_token) {
        return Err(owned_error!(InvalidRequest, "invalid token3"));
    }

    if has_session_token && !cred.is_service_account() && cred.is_expired() {
        return Err(owned_error!(ExpiredToken, "The provided token has expired."));
    }

    Ok(())
}

fn check_claims_from_token_with_context(
    token: &str,
    cred: &Credentials,
    ctx: Option<&AppContext>,
) -> OwnedResult<HashMap<String, Value>> {
    check_session_token(token, cred)?;

    let sys_cred = match ctx {
        Some(context) => context.action_credentials().get(),
        None => current_action_credentials(),
    };
    let Some(sys_cred) = sys_cred else {
        return Err(owned_error!(InternalError, "action cred not init"));
    };

    let (token, secret) = if cred.is_service_account() {
        (cred.session_token.as_str(), cred.secret_key.as_str())
    } else {
        (token, sys_cred.secret_key.as_str())
    };

    if !token.is_empty() {
        let claims: HashMap<String, Value> = if cred.is_service_account() {
            get_claims_from_token_with_secret_allow_missing_exp(token, secret)
                .map_err(|_e| owned_error!(InvalidRequest, "invalid token"))?
        } else {
            get_claims_from_token_with_secret(token, secret).map_err(|_e| owned_error!(InvalidRequest, "invalid token"))?
        };
        return Ok(claims);
    }

    Ok(HashMap::new())
}

pub fn get_session_token<'a>(uri: &'a Uri, hds: &'a HeaderMap) -> Option<&'a str> {
    let token = hds
        .get("x-amz-security-token")
        .map(|v| v.to_str().unwrap_or_default())
        .or_else(|| get_query_param(uri.query().unwrap_or_default(), "x-amz-security-token"));

    trace!(
        event = EVENT_SESSION_TOKEN_EXTRACTION,
        component = LOG_COMPONENT_AUTH,
        subsystem = LOG_SUBSYSTEM_REQUEST,
        has_session_token = token.is_some(),
        "Completed session token extraction"
    );

    token
}

pub(crate) fn extract_string_list_claim(claims: &HashMap<String, Value>, claim_name: &str) -> Vec<String> {
    match get_claim_case_insensitive(claims, claim_name) {
        ClaimLookup::Found(Value::Array(values)) => values.iter().filter_map(|v| v.as_str().map(ToOwned::to_owned)).collect(),
        ClaimLookup::Found(Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        ClaimLookup::Missing | ClaimLookup::Ambiguous | ClaimLookup::Found(_) => Vec::new(),
    }
}

/// Canonicalize an `s3:VersionId` condition value.
///
/// The object handlers accept several spellings of one version id (upper
/// case, unhyphenated, braced, `urn:uuid:`), and the storage layer resolves
/// all of them to the same version. Policy conditions must see the single
/// spelling the server itself emits, otherwise a `Deny` with `StringEquals`
/// can be bypassed by re-spelling the id. `null` in any case and the nil
/// UUID both name the null version. Strings that are not a UUID are passed
/// through unchanged; the handlers reject them later.
pub(crate) fn canonical_policy_version_id(version: &str) -> String {
    let trimmed = version.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return "null".to_string();
    }
    match uuid::Uuid::parse_str(trimmed) {
        Ok(id) if id.is_nil() => "null".to_string(),
        Ok(id) => id.to_string(),
        Err(_) => version.to_string(),
    }
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
    region: Option<rustfs_s3_types::Region>,
    remote_addr: Option<SocketAddr>,
) -> HashMap<String, Vec<String>> {
    get_condition_values_with_client_info(header, cred, version_id, region, remote_addr, None)
}

/// Get condition values for policy evaluation with verified client information.
pub fn get_condition_values_with_client_info(
    header: &HeaderMap,
    cred: &Credentials,
    version_id: Option<&str>,
    region: Option<rustfs_s3_types::Region>,
    remote_addr: Option<SocketAddr>,
    client_info: Option<&ClientInfo>,
) -> HashMap<String, Vec<String>> {
    condition_values(header, cred, version_id, region, remote_addr, None, client_info)
}

/// Get condition values for policy evaluation with optional query-string values.
///
/// # Arguments
/// * `header` - HTTP headers of the request
/// * `cred` - User credentials
/// * `version_id` - Optional version ID of the object
/// * `region` - Optional region/location constraint
/// * `remote_addr` - Optional remote address of the connection
/// * `query` - Optional request query string
///
/// # Returns
/// * `HashMap<String, Vec<String>>` - Condition values for policy evaluation
pub fn get_condition_values_with_query(
    header: &HeaderMap,
    cred: &Credentials,
    version_id: Option<&str>,
    region: Option<rustfs_s3_types::Region>,
    remote_addr: Option<SocketAddr>,
    query: Option<&str>,
) -> HashMap<String, Vec<String>> {
    condition_values(header, cred, version_id, region, remote_addr, query, None)
}

/// The condition values the legacy edge evaluates policies with: what
/// [`facts::build_conditions`] derives for a caller that names no action, plus
/// the secret headers [`facts::forward_secret_headers`] still copies in.
fn condition_values(
    header: &HeaderMap,
    cred: &Credentials,
    version_id: Option<&str>,
    region: Option<rustfs_s3_types::Region>,
    remote_addr: Option<SocketAddr>,
    query: Option<&str>,
    client_info: Option<&ClientInfo>,
) -> HashMap<String, Vec<String>> {
    let root = current_action_credentials().unwrap_or_default();
    let principal = AuthzPrincipal::new(cred, &root.access_key);
    let target = AuthzTarget {
        action: Action::None,
        bucket: "",
        object: "",
        version_id,
        location_constraint: region.as_ref().map(|region| region.as_str()),
    };
    let request_facts =
        AuthzFacts::from_request_parts(header, query, remote_addr, client_info, target, OffsetDateTime::now_utc());
    let mut conditions = facts::build_conditions(&principal, &request_facts);
    facts::forward_secret_headers(header, &mut conditions);
    conditions
}

/// The S3 operation, rather than identity claims or request input, determines this value.
pub(crate) const OBJECT_CREATION_OPERATION_CONDITION: &str = "ObjectCreationOperation";

/// Condition name for `s3:object-lock-remaining-retention-days`. Storage access
/// derives it from the requested retain-until date, so no header may set it.
pub(crate) const OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION: &str = "object-lock-remaining-retention-days";

/// Get request authentication type
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `AuthType` - The determined authentication type
///
pub fn get_request_auth_type(header: &HeaderMap) -> AuthType {
    get_request_auth_type_with_query(header, None)
}

pub(crate) fn get_request_auth_type_with_query(header: &HeaderMap, query: Option<&str>) -> AuthType {
    get_request_auth_type_with_method_and_query(None, header, query)
}

/// Classify a request according to the authentication mechanism verified by the gateway.
///
/// Unsigned headers (`AWSAccessKeyId`, `X-Amz-Credential`, standalone `STREAMING-*`
/// `x-amz-content-sha256`, or repeated `Authorization` / `Content-Type` headers) must not
/// relabel a presigned query, signed POST policy form, or anonymous request.
pub(crate) fn get_request_auth_type_with_method_and_query(
    method: Option<&Method>,
    header: &HeaderMap,
    query: Option<&str>,
) -> AuthType {
    if method == Some(&Method::POST) && is_request_post_policy_signature_v4(header) {
        AuthType::PostPolicy
    } else if is_request_presigned_signature_v2(query) {
        AuthType::PresignedV2
    } else if is_request_presigned_signature_v4_with_query(query) {
        AuthType::Presigned
    } else if is_request_signature_v2(header) {
        AuthType::SignedV2
    } else if is_request_signature_v4(header) {
        if is_request_sign_streaming_v4(header) {
            AuthType::StreamingSigned
        } else if is_request_sign_streaming_trailer_v4(header) {
            AuthType::StreamingSignedTrailer
        } else if is_request_unsigned_trailer_v4(header) {
            AuthType::StreamingUnsignedTrailer
        } else {
            AuthType::Signed
        }
    } else if is_request_jwt(header) {
        AuthType::JWT
    } else if method.is_none() && is_request_post_policy_signature_v4(header) {
        AuthType::PostPolicy
    } else if is_request_sts(header) {
        AuthType::STS
    } else if is_request_anonymous(header) {
        AuthType::Anonymous
    } else {
        AuthType::Unknown
    }
}

/// Returns the header value only when the header appears exactly once and is valid UTF-8.
fn get_unique_header_str<'a>(header: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = header.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    std::str::from_utf8(value.as_bytes()).ok()
}

/// Returns true when the percent-decoded query string contains `param`.
fn query_has_param(query: Option<&str>, param: &str) -> bool {
    query.is_some_and(|query| form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == param))
}

/// Verify if request has JWT
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has JWT, false otherwise
fn is_request_jwt(header: &HeaderMap) -> bool {
    get_unique_header_str(header, "authorization").is_some_and(|auth| auth.starts_with(JWT_ALGORITHM))
}

/// Verify if request has AWS Signature Version '4'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Signature Version '4', false otherwise
fn is_request_signature_v4(header: &HeaderMap) -> bool {
    get_unique_header_str(header, "authorization").is_some_and(|auth| auth.starts_with(SIGN_V4_ALGORITHM))
}

/// Verify if request has AWS Signature Version '2'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Signature Version '2', false otherwise
fn is_request_signature_v2(header: &HeaderMap) -> bool {
    get_unique_header_str(header, "authorization")
        .and_then(|auth| auth.strip_prefix(SIGN_V2_ALGORITHM))
        .is_some_and(|credential| credential.contains(':'))
}

/// True when `query` can name any `x-amz-*` query parameter at all. A single
/// ASCII scan, so a request whose query only carries things like `versionId`
/// or `uploads` never pays for a parse.
fn query_may_carry_hoistable_amz_param(query: &str) -> bool {
    const NEEDLE: &[u8] = b"x-amz-";
    query
        .as_bytes()
        .windows(NEEDLE.len())
        .any(|window| window.eq_ignore_ascii_case(NEEDLE))
}

/// Exact `x-amz-*` request properties that SigV4 presigners hoist into the
/// query string (`@smithy/signature-v4` `moveHeadersToQuery`) and that RustFS
/// safely restores as headers after signature verification (rustfs/backlog#3143).
///
/// Explicitly excluded (fail-closed):
/// - SigV4 authentication parameters (`x-amz-algorithm`, `x-amz-credential`,
///   `x-amz-date`, `x-amz-expires`, `x-amz-signedheaders`, `x-amz-signature`,
///   `x-amz-security-token`);
/// - payload digest / streaming framing headers (`x-amz-content-sha256`,
///   `x-amz-decoded-content-length`, `x-amz-trailer`) — hoisting
///   `x-amz-content-sha256` after body construction would disable the storage
///   layer's query-digest verification (`get_content_sha256_with_query`);
/// - SSE-C customer key headers (`x-amz-server-side-encryption-customer-*`,
///   `x-amz-copy-source-server-side-encryption-customer-*`) — customer keys
///   must arrive as signed headers over TLS, never hoisted from query strings;
/// - replication-only, governance-bypass, and vendor extension headers
///   (`x-amz-replication-status`, `x-amz-bypass-governance-retention`,
///   `x-rustfs-*`, `x-minio-*`).
const HOISTABLE_SIGNED_QUERY_PARAMS: &[&str] = &[
    "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date",
    "x-amz-object-lock-legal-hold",
    "x-amz-acl",
    "x-amz-grant-full-control",
    "x-amz-grant-read",
    "x-amz-grant-write",
    "x-amz-grant-read-acp",
    "x-amz-grant-write-acp",
    "x-amz-tagging",
    "x-amz-tagging-directive",
    "x-amz-storage-class",
    "x-amz-website-redirect-location",
    "x-amz-snowball-auto-extract",
    "x-amz-copy-source",
    "x-amz-copy-source-range",
    "x-amz-copy-source-if-match",
    "x-amz-copy-source-if-none-match",
    "x-amz-copy-source-if-modified-since",
    "x-amz-copy-source-if-unmodified-since",
    "x-amz-metadata-directive",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-checksum-algorithm",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-bucket-key-enabled",
];

pub(crate) fn is_hoistable_signed_query_param(name: &str) -> bool {
    starts_with_ignore_ascii_case(name, "x-amz-meta-")
        || HOISTABLE_SIGNED_QUERY_PARAMS
            .iter()
            .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

/// Restores signature-covered hoistable `x-amz-*` request properties from the
/// query string as request headers (rustfs/rustfs#9142, rustfs/backlog#3143).
///
/// AWS SDK presigners hoist signable `x-amz-*` headers into the query string
/// (`moveHeadersToQuery`), while RustFS's policy evaluation and object write
/// handlers read headers. This covers user metadata (`x-amz-meta-*`) plus the
/// explicit allowlist of S3 request properties (`x-amz-object-lock-*`,
/// `x-amz-acl`, `x-amz-grant-*`, `x-amz-tagging`, `x-amz-storage-class`,
/// `x-amz-website-redirect-location`, `x-amz-copy-source*`,
/// `x-amz-checksum-*`, `x-amz-snowball-auto-extract`, managed SSE headers).
///
/// This runs after signature verification and before policy evaluation and
/// operation handlers read request properties. It must not run earlier: the
/// SigV4 verifier rejects an `x-amz-*` header outside `X-Amz-SignedHeaders`,
/// which is exactly what a restored parameter becomes. Downstream authorization
/// hooks (`authorize_put_object`, `authorize_copy_object`,
/// `authorize_create_multipart_upload`) inspect the restored headers so Object
/// Lock, tagging, ACL/grant, and copy-source permissions and validations apply
/// identically to wire headers.
///
/// Only a query the SigV4 signature covers is honored. SigV4 signs the
/// canonical query string (every parameter except `X-Amz-Signature`), so its
/// query parameters are as trustworthy as its headers. SigV2's string-to-sign
/// covers headers plus a fixed subresource list rather than arbitrary query
/// parameters, so a SigV2 presigned URL keeps validating after
/// `X-Amz-Algorithm`/`X-Amz-Signature` markers are appended to it — those
/// markers must not be allowed to widen it into accepting query properties the
/// presigner never authorized. An existing header always wins, so this is purely
/// additive and the unsigned-header guard keeps rejecting anything the signer did
/// not sign. Keys and values that could not have arrived as a header are
/// rejected rather than stored: the read path rebuilds them as header fields,
/// where a control character would fail every `HeadObject` of that object.
pub fn hoist_signed_query_params(headers: &mut HeaderMap, query: Option<&str>) -> OwnedResult<()> {
    let Some(query) = query else {
        return Ok(());
    };
    if !query_may_carry_hoistable_amz_param(query) || is_request_signature_v2(headers) {
        return Ok(());
    }

    // One pass over the query: recognize the presigned-SigV4 marker set and
    // stage allowlisted `x-amz-*` request properties. Nothing is validated or
    // applied until the gate below passes, so a request whose query is not
    // signature-covered is never rejected here.
    let mut algorithm = false;
    let mut credential = false;
    let mut signature = false;
    let mut staged: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut duplicate = false;
    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        if name.eq_ignore_ascii_case("awsaccesskeyid") || name.eq_ignore_ascii_case("signature") {
            // A SigV2 presigned URL is recognized by its own credential or
            // signature parameter; the SigV4 markers below can be appended to
            // it without invalidating the SigV2 signature, so it must never be
            // widened.
            if !value.is_empty() {
                return Ok(());
            }
            continue;
        }
        if name.eq_ignore_ascii_case("x-amz-algorithm") {
            algorithm = value == SIGN_V4_ALGORITHM;
            continue;
        }
        if name.eq_ignore_ascii_case("x-amz-credential") {
            credential = !value.is_empty();
            continue;
        }
        if name.eq_ignore_ascii_case("x-amz-signature") {
            signature = !value.is_empty();
            continue;
        }
        // Parameter names keep whatever case the signer used (`X-Amz-Meta-User-Id`
        // or `X-Amz-Object-Lock-Mode` is what a hoist produces), so match
        // case-insensitively the way `HeaderName` would.
        if !is_hoistable_signed_query_param(&name) {
            continue;
        }
        let stored_name = name.to_ascii_lowercase();
        duplicate |= !seen.insert(stored_name.clone());
        staged.push((stored_name, value.into_owned()));
    }

    if !is_request_signature_v4(headers) && !(algorithm && credential && signature) {
        return Ok(());
    }
    if duplicate {
        return Err(owned_error!(
            InvalidArgument,
            "Duplicate x-amz request parameters in the request query string"
        ));
    }

    for (name, value) in staged {
        // The read path rebuilds `x-amz-meta-<key>` as a header name and the
        // value as a header value; anything that could not have arrived as a
        // header would fail downstream header validation or Head/Get responses.
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| owned_error!(InvalidArgument, "Invalid x-amz request parameter name"))?;
        let header_value = HeaderValue::from_bytes(value.as_bytes())
            .map_err(|_| owned_error!(InvalidArgument, "x-amz request parameter values must be valid HTTP header values"))?;
        if headers.contains_key(&header_name) {
            continue;
        }
        headers.insert(header_name, header_value);
    }
    Ok(())
}

/// Verify request has AWS PreSign Version '4' from the decoded query string.
pub(crate) fn is_request_presigned_signature_v4_with_query(query: Option<&str>) -> bool {
    query_has_param(query, "X-Amz-Signature")
}

/// Verify request has AWS PreSign Version '2' from the decoded query string.
fn is_request_presigned_signature_v2(query: Option<&str>) -> bool {
    query_has_param(query, "Signature")
}

/// Verify if request has AWS Post policy Signature Version '4'
///
/// # Arguments
/// * `header` - HTTP headers of the request
///
/// # Returns
/// * `bool` - True if request has AWS Post policy Signature Version '4', false otherwise
fn is_request_post_policy_signature_v4(header: &HeaderMap) -> bool {
    get_unique_header_str(header, "content-type")
        .and_then(|content_type| content_type.parse::<Mime>().ok())
        .is_some_and(|mime| mime.type_() == mime::MULTIPART && mime.subtype() == mime::FORM_DATA)
}

/// Verify if the request has AWS Streaming Signature Version '4'
fn is_request_sign_streaming_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256")
        && let Ok(sha256_str) = content_sha256.to_str()
    {
        return sha256_str == STREAMING_CONTENT_SHA256;
    }
    false
}

// Verify if the request has AWS Streaming Signature Version '4' with trailer
fn is_request_sign_streaming_trailer_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256")
        && let Ok(sha256_str) = content_sha256.to_str()
    {
        return sha256_str == STREAMING_CONTENT_SHA256_TRAILER;
    }
    false
}

// Verify if the request has AWS Streaming Signature Version '4' with unsigned content and trailer
fn is_request_unsigned_trailer_v4(header: &HeaderMap) -> bool {
    if let Some(content_sha256) = header.get("x-amz-content-sha256")
        && let Ok(sha256_str) = content_sha256.to_str()
    {
        return sha256_str == UNSIGNED_PAYLOAD_TRAILER;
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
    header.get_all("authorization").iter().count() != 1
}

pub fn get_query_param<'a>(query: &'a str, param_name: &str) -> Option<&'a str> {
    let param_name = param_name.to_lowercase();

    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next())
            && key.to_lowercase() == param_name
        {
            return Some(value);
        }
    }
    None
}

pub(crate) const UNSUPPORTED_SIGV4_ALGORITHM_MESSAGE: &str = "Unsupported SigV4 authorization algorithm";

/// Request-envelope `x-amz-*` headers a header-signed SigV4 request may carry
/// without listing them in `SignedHeaders`, matching the upstream verifier.
///
/// `x-amz-content-sha256` is bound through the canonical request's payload
/// hash. The aws-chunked framing headers are added around an already signed
/// request and are only read after verification to decode the body; none of
/// them selects a different operation.
const SIGV4_UNSIGNED_ENVELOPE_HEADERS: &[&str] = &[
    "x-amz-content-sha256",
    "x-amz-decoded-content-length",
    "x-amz-trailer",
    "x-amz-checksum-algorithm",
];

/// RustFS extension request headers and their MinIO aliases
/// (`rustfs_utils::http::get_header` reads both spellings).
///
/// Handlers act on these headers like `x-amz-*`: user metadata aliases,
/// replication-only write options (source version id, mtime, ETag, SSE-C
/// ciphertext passthrough, lock timestamps), and force-delete arrive under
/// these prefixes.
const EXTENSION_HEADER_PREFIXES: &[&str] = &["x-rustfs-", "x-minio-"];

/// `name` must already be lowercase, as `HeaderName` is.
fn is_extension_header(name: &str) -> bool {
    EXTENSION_HEADER_PREFIXES.iter().any(|prefix| name.starts_with(prefix))
}

/// True for a request header that can change how RustFS handles the request, so
/// the authenticating signature must cover it.
fn is_signature_bound_header(name: &str) -> bool {
    name.starts_with("x-amz-") || is_extension_header(name)
}

/// True when the request head carries SigV4 or SigV2 presigned query/header
/// markers. Used as defense in depth so replication-only write headers cannot
/// take effect on presigned URLs even if signed.
pub(crate) fn is_presigned_request_head(_header: &HeaderMap, query: Option<&str>) -> bool {
    if is_request_presigned_signature_v4_with_query(query) || is_request_presigned_signature_v2(query) {
        return true;
    }
    query.is_some_and(|query| {
        form_urlencoded::parse(query.as_bytes()).any(|(name, _)| {
            name.eq_ignore_ascii_case("x-amz-signature")
                || name.eq_ignore_ascii_case("x-amz-credential")
                || name.eq_ignore_ascii_case("signature")
                || name.eq_ignore_ascii_case("awsaccesskeyid")
        })
    })
}

/// GHSA-xm99-m3gq-83g8: reject `x-amz-*`, `x-rustfs-*`, and `x-minio-*` request
/// headers that the request's signature does not cover, across SigV4 (header
/// and presigned) and SigV2 authentication forms.
///
/// The upstream verifier only proves that the headers named in `SignedHeaders`
/// match; every other `x-amz-*`, `x-rustfs-*`, or `x-minio-*` header would still
/// reach handlers unsigned. Every signed-header list the request carries must
/// cover every signature-bound header, so an `Authorization` header can never
/// widen a presigned URL and a query can never widen a header signature. SigV2
/// canonicalizes `x-amz-*` headers itself but cannot sign extension headers, so
/// any SigV2 request carrying `x-rustfs-*` or `x-minio-*` is rejected. JWT and
/// anonymous requests carry no HTTP request signature.
pub(crate) fn reject_unsigned_amz_headers_on_sigv4_request(header: &HeaderMap, query: Option<&str>) -> OwnedResult<()> {
    reject_unsigned_headers_matching(header, query, is_signature_bound_header)
}

/// Enforce the signed-header rule for `x-rustfs-*` and `x-minio-*` extension
/// headers on SigV4 (header-signed and presigned) and SigV2 requests at the
/// gateway entry, complementing the upstream `x-amz-*` guard.
pub(crate) fn reject_unsigned_extension_headers_on_signed_request(header: &HeaderMap, query: Option<&str>) -> OwnedResult<()> {
    if !header.keys().any(|name| is_extension_header(name.as_str())) {
        return Ok(());
    }
    reject_unsigned_headers_matching(header, query, is_extension_header)
}

fn reject_unsigned_headers_matching(header: &HeaderMap, query: Option<&str>, check_header: fn(&str) -> bool) -> OwnedResult<()> {
    reject_unsigned_presigned_headers_matching(header, query, check_header)?;

    for value in header.get_all(http::header::AUTHORIZATION) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let authorization = match rustfs_gateway::sig::SigV4HeaderCoverage::parse(value) {
            Some(authorization) => authorization,
            None if value.starts_with(SIGN_V4_ALGORITHM) => {
                return Err(owned_error!(AccessDenied, "Invalid SigV4 authorization header"));
            }
            None if value.trim_start().starts_with(SIGN_V2_ALGORITHM) => {
                reject_extension_headers_on_sigv2_request(header)?;
                continue;
            }
            None => continue,
        };
        if authorization.algorithm() != SIGN_V4_ALGORITHM {
            warn!(
                event = EVENT_SIGV4_UNSUPPORTED_ALGORITHM,
                component = LOG_COMPONENT_AUTH,
                subsystem = LOG_SUBSYSTEM_REQUEST,
                reason = "unsupported_algorithm",
                "SigV4 request rejected"
            );
            return Err(owned_error!(AccessDenied, "{UNSUPPORTED_SIGV4_ALGORITHM_MESSAGE}"));
        }
        for name in header.keys() {
            // `HeaderName` is already lowercase; the verifier looks signed
            // names up case-insensitively, so compare them the same way.
            let name = name.as_str();
            if !check_header(name)
                || SIGV4_UNSIGNED_ENVELOPE_HEADERS.contains(&name)
                || PRESIGNED_UNSIGNED_AMZ_HEADER_ALLOWLIST.contains(&name)
                || authorization.covers_header(name)
            {
                continue;
            }
            warn!(
                event = EVENT_SIGV4_UNSIGNED_AMZ_HEADER,
                component = LOG_COMPONENT_AUTH,
                subsystem = LOG_SUBSYSTEM_REQUEST,
                reason = "unsigned_amz_header",
                header = name,
                "SigV4 request rejected"
            );
            return Err(owned_error!(AccessDenied, "{UNSIGNED_HEADERS_MESSAGE}"));
        }
    }
    Ok(())
}

/// `x-amz-*` request headers a SigV4 presigned request may carry without
/// listing them in `X-Amz-SignedHeaders`.
///
/// CloudFront stamps `x-amz-cf-id` on every origin request it forwards, so a
/// presigned URL served through a CDN could never be honoured if that header
/// had to be signed; nothing in RustFS reads it, so it cannot change what the
/// request does.
const PRESIGNED_UNSIGNED_AMZ_HEADER_ALLOWLIST: &[&str] = &["x-amz-cf-id"];

pub(crate) const UNSIGNED_HEADERS_MESSAGE: &str = "There were headers present in the request which were not signed";

/// Reject `x-rustfs-*` / `x-minio-*` request headers on a SigV2 request.
///
/// SigV2 canonicalizes `x-amz-*` headers only and has no signed-header list, so
/// neither a SigV2 presigned URL nor a SigV2 `Authorization` header can cover
/// an extension header.
fn reject_extension_headers_on_sigv2_request(header: &HeaderMap) -> OwnedResult<()> {
    let Some(name) = header.keys().map(HeaderName::as_str).find(|name| is_extension_header(name)) else {
        return Ok(());
    };
    warn!(
        event = EVENT_SIGV2_EXTENSION_HEADER,
        component = LOG_COMPONENT_AUTH,
        subsystem = LOG_SUBSYSTEM_REQUEST,
        reason = "unsigned_extension_header",
        header = name,
        "SigV2 request rejected"
    );
    Err(owned_error!(AccessDenied, "{UNSIGNED_HEADERS_MESSAGE}"))
}

/// GHSA-g8w9-qw9q-fghr: reject `x-amz-*`, `x-rustfs-*`, and `x-minio-*` request
/// headers that a presigned URL did not sign.
///
/// SigV4 presigned requests must list every signature-bound header in
/// `X-Amz-SignedHeaders` (or match a same-named signed query parameter). SigV2
/// canonicalizes `x-amz-*` headers into its string to sign, so any `x-rustfs-*`
/// or `x-minio-*` extension header is outside the SigV2 signature and is
/// refused outright.
#[cfg(test)]
pub(crate) fn reject_unsigned_amz_headers_on_presigned_request(header: &HeaderMap, query: Option<&str>) -> OwnedResult<()> {
    reject_unsigned_presigned_headers_matching(header, query, is_signature_bound_header)
}

fn reject_unsigned_presigned_headers_matching(
    header: &HeaderMap,
    query: Option<&str>,
    check_header: fn(&str) -> bool,
) -> OwnedResult<()> {
    let Some(query) = query else {
        return Ok(());
    };

    // Presence detection is case-insensitive so a query the upstream verifier
    // would not treat as presigned still fails closed here; the signed list is
    // read with the exact key the verifier uses (`X-Amz-SignedHeaders`, unique),
    // so both sides always see the same list. A duplicate or missing key
    // yields an empty list, which signs nothing.
    let mut is_presigned_v4 = false;
    let mut is_presigned_v2 = false;
    let mut signed_headers: Option<String> = None;
    let mut duplicate_signed_headers = false;
    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        if name.eq_ignore_ascii_case("x-amz-signature") {
            is_presigned_v4 = true;
        } else if name.eq_ignore_ascii_case("signature") || name.eq_ignore_ascii_case("awsaccesskeyid") {
            is_presigned_v2 = true;
        } else if name == "X-Amz-SignedHeaders" {
            if signed_headers.is_some() {
                duplicate_signed_headers = true;
            }
            signed_headers = Some(value.into_owned());
        }
    }
    // SigV2 leaves ordinary query parameters unsigned and takes precedence in
    // presigned verification, so appending SigV4 parameters to a SigV2 URL must
    // never cover an extension header.
    if is_presigned_v2 {
        reject_extension_headers_on_sigv2_request(header)?;
    }
    if !is_presigned_v4 {
        return Ok(());
    }
    if duplicate_signed_headers {
        signed_headers = None;
    }

    let signed: Vec<String> = signed_headers
        .as_deref()
        .unwrap_or_default()
        .split(';')
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect();

    for name in header.keys() {
        // `HeaderName` is already lowercase.
        let name = name.as_str();
        if !check_header(name) || PRESIGNED_UNSIGNED_AMZ_HEADER_ALLOWLIST.contains(&name) {
            continue;
        }
        if signed.iter().any(|signed_name| signed_name == name) {
            continue;
        }
        // The query-hoist layer restores signed, allowlisted `x-amz-*` query
        // parameters as headers before this rule runs a second time at the
        // access boundary. Extension headers (`x-rustfs-*`, `x-minio-*`) and
        // non-hoistable `x-amz-*` headers (e.g. `x-amz-content-sha256`,
        // `x-amz-replication-status`, SSE-C customer keys) are never hoisted
        // from the query string, so they must always appear in
        // `X-Amz-SignedHeaders`.
        if is_hoistable_signed_query_param(name) && header_as_signed_query_value(header, query, name) {
            continue;
        }
        warn!(
            event = EVENT_PRESIGNED_UNSIGNED_AMZ_HEADER,
            component = LOG_COMPONENT_AUTH,
            subsystem = LOG_SUBSYSTEM_REQUEST,
            reason = "unsigned_amz_header",
            header = name,
            "Presigned request rejected"
        );
        return Err(owned_error!(AccessDenied, "{UNSIGNED_HEADERS_MESSAGE}"));
    }

    Ok(())
}

/// True when `name` is a single-valued header whose value is exactly the value
/// of a unique same-named query parameter.
///
/// The SigV4 canonical query string covers every parameter except the signature
/// itself, so a unique match proves the value the header carries is authorized.
/// Only a single-valued header matching a single query occurrence can pass: the
/// query-hoist layer rejects duplicate query keys and never produces a
/// multi-valued header, so any extra value would be unsigned or conflicting.
fn header_as_signed_query_value(header: &HeaderMap, query: &str, name: &str) -> bool {
    let mut values = header.get_all(name).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let mut matched = false;
    for (param, param_value) in form_urlencoded::parse(query.as_bytes()) {
        if param.eq_ignore_ascii_case(name) {
            if matched || param_value.as_bytes() != value.as_bytes() {
                return false;
            }
            matched = true;
        }
    }
    matched
}

/// Parse the RustFS presigned PutObject size capability after authentication.
///
/// The query value is covered by SigV4 when it is present before presigning, but
/// the signature does not assign any semantics to the extension. Keep parsing
/// strict and only enable the capability for a verified SigV4 presigned request.
pub(crate) fn parse_presigned_put_max_content_length(
    header: &HeaderMap,
    query: Option<&str>,
    verified_presigned: bool,
) -> OwnedResult<Option<u64>> {
    let Some(query) = query else {
        return Ok(None);
    };

    let mut value = None;
    let mut decoded_query = Vec::new();
    for (name, candidate) in form_urlencoded::parse(query.as_bytes()) {
        decoded_query.push((name.to_string(), candidate.to_string()));
        if name == RUSTFS_MAX_CONTENT_LENGTH_QUERY {
            if value.is_some() {
                return Err(owned_error!(InvalidRequest, "{RUSTFS_MAX_CONTENT_LENGTH_QUERY} must appear exactly once"));
            }
            value = Some(candidate.into_owned());
        } else if name.eq_ignore_ascii_case(RUSTFS_MAX_CONTENT_LENGTH_QUERY) {
            return Err(owned_error!(
                InvalidRequest,
                "query parameter name must be exactly {RUSTFS_MAX_CONTENT_LENGTH_QUERY}"
            ));
        }
    }

    let Some(value) = value else {
        return Ok(None);
    };

    let query_value = |wanted: &str| {
        decoded_query
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
            .map(|(_, value)| value.as_str())
    };
    let is_complete_sigv4_query = [
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-date", ""),
        ("x-amz-expires", ""),
        ("x-amz-signedheaders", ""),
        ("x-amz-credential", ""),
        ("x-amz-signature", ""),
    ]
    .into_iter()
    .all(|(name, expected)| {
        query_value(name).is_some_and(|value| !value.is_empty() && (expected.is_empty() || value == expected))
    });
    if !verified_presigned
        || !is_complete_sigv4_query
        || !matches!(get_request_auth_type_with_query(header, Some(query)), AuthType::Presigned)
    {
        return Err(owned_error!(
            InvalidRequest,
            "{RUSTFS_MAX_CONTENT_LENGTH_QUERY} requires a SigV4 presigned request"
        ));
    }

    let limit = value
        .parse::<u64>()
        .map_err(|_| owned_error!(InvalidRequest, "{RUSTFS_MAX_CONTENT_LENGTH_QUERY} must be a non-negative 64-bit integer"))?;

    Ok(Some(limit))
}

/// Reject the PutObject-only size capability when it appears on another
/// operation. Callers must invoke this after request authentication has run.
pub(crate) fn reject_presigned_put_max_content_length_for_other_operation(
    header: &HeaderMap,
    query: Option<&str>,
    verified_presigned: bool,
) -> OwnedResult<()> {
    if parse_presigned_put_max_content_length(header, query, verified_presigned)?.is_some() {
        return Err(owned_error!(
            InvalidRequest,
            "{RUSTFS_MAX_CONTENT_LENGTH_QUERY} is only supported for presigned PutObject"
        ));
    }
    Ok(())
}

/// Parse the V2 multipart total-size capability after SigV4 authentication.
/// Header-authenticated CreateMultipartUpload requests are accepted because the
/// custom query is covered by the SigV4 canonical request; later multipart
/// operations read the immutable value from the upload session metadata.
pub(crate) fn parse_presigned_multipart_max_total_object_size(
    header: &HeaderMap,
    query: Option<&str>,
    verified_sigv4: bool,
) -> OwnedResult<Option<u64>> {
    let Some(query) = query else {
        return Ok(None);
    };

    let mut value = None;
    let mut decoded_query = Vec::new();
    for (name, candidate) in form_urlencoded::parse(query.as_bytes()) {
        decoded_query.push((name.to_string(), candidate.to_string()));
        if name == RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY {
            if value.is_some() {
                return Err(owned_error!(
                    InvalidRequest,
                    "{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} must appear exactly once"
                ));
            }
            value = Some(candidate.into_owned());
        } else if name.eq_ignore_ascii_case(RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY) {
            return Err(owned_error!(
                InvalidRequest,
                "query parameter name must be exactly {RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY}"
            ));
        }
    }

    let Some(value) = value else {
        return Ok(None);
    };

    let auth_type = get_request_auth_type_with_query(header, Some(query));
    let is_presigned = matches!(auth_type, AuthType::Presigned);
    let is_header_signed = matches!(auth_type, AuthType::Signed);
    let complete_presigned_query = [
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-date", ""),
        ("x-amz-expires", ""),
        ("x-amz-signedheaders", ""),
        ("x-amz-credential", ""),
        ("x-amz-signature", ""),
    ]
    .into_iter()
    .all(|(name, expected)| {
        decoded_query
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .is_some_and(|(_, candidate)| !candidate.is_empty() && (expected.is_empty() || candidate == expected))
    });

    let authenticated = verified_sigv4 && (is_header_signed || (is_presigned && complete_presigned_query));
    if !authenticated {
        return Err(owned_error!(
            InvalidRequest,
            "{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} requires a verified SigV4 request"
        ));
    }

    value.parse::<u64>().map(Some).map_err(|_| {
        owned_error!(
            InvalidRequest,
            "{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} must be a non-negative 64-bit integer"
        )
    })
}

pub(crate) fn reject_presigned_multipart_max_total_object_size_for_other_operation(
    header: &HeaderMap,
    query: Option<&str>,
    verified_sigv4: bool,
) -> OwnedResult<()> {
    if parse_presigned_multipart_max_total_object_size(header, query, verified_sigv4)?.is_some() {
        return Err(owned_error!(
            InvalidRequest,
            "{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} is only supported for CreateMultipartUpload"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_sources::{IamInterface, KmsInterface};
    use http::header::{IF_MATCH, IF_NONE_MATCH};
    use http::{HeaderMap, HeaderValue, Uri};
    use http_body_util::BodyExt;
    use rustfs_credentials::Credentials;
    use rustfs_iam::sys::OIDC_ISSUER_CLAIM;
    use rustfs_iam::{
        store::{
            Store as _,
            object::{IAM_CONFIG_PREFIX, ObjectStore},
        },
        sys::IamSys,
    };
    use rustfs_kms::KmsServiceManager;
    use rustfs_policy::auth::get_new_credentials_with_metadata;
    use rustfs_s3_types::S3ErrorCode;
    use rustfs_trusted_proxies::ValidationMode;
    use rustfs_utils::http::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use time::OffsetDateTime;

    #[test]
    fn signed_metadata_hoist_preserves_errors_and_headers() {
        for (query, message, applied) in [
            (
                "x-amz-meta-a=1&X-Amz-Meta-A=2",
                "Duplicate x-amz request parameters in the request query string",
                false,
            ),
            ("x-amz-meta-a=1&x-amz-meta-b%20c=2", "Invalid x-amz request parameter name", true),
            (
                "x-amz-meta-a=1&x-amz-meta-b=%0A",
                "x-amz request parameter values must be valid HTTP header values",
                true,
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static(SIGN_V4_ALGORITHM));
            let mut expected = headers.clone();
            if applied {
                expected.insert("x-amz-meta-a", HeaderValue::from_static("1"));
            }
            let error = hoist_signed_query_params(&mut headers, Some(query)).unwrap_err();
            assert_eq!(headers, expected);
            assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
            assert_eq!(error.message(), Some(message));
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn hoist_signed_query_params_widens_sigv4_presigned_properties() {
        let presigned_base = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20261126%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261126T000000Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=deadbeef&X-Amz-Security-Token=session-tok";
        let hoisted_pairs = [
            ("X-Amz-Meta-Owner", "x-amz-meta-owner", "alice"),
            ("X-Amz-Object-Lock-Mode", AMZ_OBJECT_LOCK_MODE_LOWER, "GOVERNANCE"),
            (
                "X-Amz-Object-Lock-Retain-Until-Date",
                AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
                "2030-01-01T00:00:00Z",
            ),
            ("X-Amz-Object-Lock-Legal-Hold", AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "ON"),
            ("X-Amz-Acl", "x-amz-acl", "private"),
            ("X-Amz-Grant-Full-Control", "x-amz-grant-full-control", "id=owner-1"),
            ("X-Amz-Grant-Read", "x-amz-grant-read", "id=reader-1"),
            ("X-Amz-Grant-Write", "x-amz-grant-write", "id=writer-1"),
            ("X-Amz-Grant-Read-Acp", "x-amz-grant-read-acp", "id=acp-reader"),
            ("X-Amz-Grant-Write-Acp", "x-amz-grant-write-acp", "id=acp-writer"),
            ("X-Amz-Tagging", "x-amz-tagging", "env=prod&team=core"),
            ("X-Amz-Tagging-Directive", "x-amz-tagging-directive", "REPLACE"),
            ("X-Amz-Storage-Class", "x-amz-storage-class", "STANDARD_IA"),
            ("X-Amz-Website-Redirect-Location", "x-amz-website-redirect-location", "/redirect"),
            ("X-Amz-Snowball-Auto-Extract", "x-amz-snowball-auto-extract", "true"),
            ("X-Amz-Copy-Source", "x-amz-copy-source", "/src-bucket/src-key"),
            ("X-Amz-Copy-Source-Range", "x-amz-copy-source-range", "bytes=0-10"),
            ("X-Amz-Metadata-Directive", "x-amz-metadata-directive", "REPLACE"),
            (
                "X-Amz-Checksum-Sha256",
                "x-amz-checksum-sha256",
                "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
            ),
            ("X-Amz-Sdk-Checksum-Algorithm", "x-amz-sdk-checksum-algorithm", "SHA256"),
            ("X-Amz-Server-Side-Encryption", "x-amz-server-side-encryption", "aws:kms"),
            (
                "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
                "x-amz-server-side-encryption-aws-kms-key-id",
                "kms-key-1",
            ),
            (
                "X-Amz-Server-Side-Encryption-Context",
                "x-amz-server-side-encryption-context",
                "eyJhIjoiYiJ9",
            ),
        ];
        let excluded_pairs = [
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("x-amz-replication-status", "REPLICA"),
            ("x-amz-bypass-governance-retention", "true"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key", "secret-key"),
            ("x-rustfs-meta-owner", "mallory"),
            ("x-rustfs-force-delete", "true"),
            ("x-minio-force-delete", "true"),
            ("x-amz-unknown-custom", "dropped"),
        ];

        let mut query = presigned_base.to_owned();
        for (raw_name, _, val) in hoisted_pairs {
            query.push('&');
            query.push_str(raw_name);
            query.push('=');
            query.push_str(&urlencoding::encode(val));
        }
        for (raw_name, val) in excluded_pairs {
            query.push('&');
            query.push_str(raw_name);
            query.push('=');
            query.push_str(&urlencoding::encode(val));
        }

        let mut headers = HeaderMap::new();
        // Pre-existing header must always win over a query parameter of the same name.
        headers.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
        hoist_signed_query_params(&mut headers, Some(&query)).expect("valid SigV4 presigned query must hoist");

        for (_, lower_name, expected_val) in hoisted_pairs {
            let expected = if lower_name == "x-amz-storage-class" {
                "STANDARD"
            } else {
                expected_val
            };
            assert_eq!(
                headers.get(lower_name).and_then(|v| v.to_str().ok()),
                Some(expected),
                "expected hoisted header {lower_name}"
            );
        }
        for auth_param in [
            "x-amz-algorithm",
            "x-amz-credential",
            "x-amz-date",
            "x-amz-expires",
            "x-amz-signedheaders",
            "x-amz-signature",
            "x-amz-security-token",
        ] {
            assert!(!headers.contains_key(auth_param), "SigV4 auth param {auth_param} must not be hoisted");
        }
        for (excluded_name, _) in excluded_pairs {
            assert!(!headers.contains_key(excluded_name), "excluded param {excluded_name} must not be hoisted");
        }

        // Duplicate hoisted property (case-insensitive) fails closed without mutating headers.
        let dup_query = format!("{presigned_base}&x-amz-tagging=a%3D1&X-Amz-Tagging=b%3D2&x-amz-acl=private");
        let mut dup_headers = HeaderMap::new();
        let err =
            hoist_signed_query_params(&mut dup_headers, Some(&dup_query)).expect_err("duplicate x-amz-tagging must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert!(dup_headers.is_empty(), "failed duplicate check must not mutate headers");
    }

    #[test]
    fn hoist_signed_query_params_does_not_widen_sigv2_or_unsigned_requests() {
        let payload_params = "x-amz-object-lock-legal-hold=ON&x-amz-object-lock-mode=GOVERNANCE&x-amz-acl=public-read&x-amz-grant-read=id%3D1&x-amz-tagging=k%3Dv&x-amz-storage-class=GLACIER&x-amz-copy-source=%2Fsrc%2Fkey&x-amz-meta-owner=mallory&x-amz-snowball-auto-extract=true";
        let sigv4_markers = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20261126%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=deadbeef";

        // 1. Anonymous / unsigned query.
        let mut headers = HeaderMap::new();
        hoist_signed_query_params(&mut headers, Some(payload_params)).expect("unsigned query is ignored");
        assert!(headers.is_empty());

        // 2. SigV2 Authorization header (even with SigV4 query markers appended).
        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static("AWS AKID:sigv2"));
        let query = format!("{sigv4_markers}&{payload_params}");
        hoist_signed_query_params(&mut headers, Some(&query)).expect("SigV2 Authorization header must not hoist");
        assert_eq!(headers.len(), 1);

        // 3. SigV2 presigned URL (AWSAccessKeyId + Signature) with appended SigV4 markers.
        let mut headers = HeaderMap::new();
        let sigv2_widened = format!("AWSAccessKeyId=AKID&Expires=1893456000&Signature=sigv2&{sigv4_markers}&{payload_params}");
        hoist_signed_query_params(&mut headers, Some(&sigv2_widened)).expect("SigV2 presigned URL must not hoist");
        assert!(headers.is_empty());

        // 4. SigV2 Signature marker alone with appended SigV4 markers.
        let mut headers = HeaderMap::new();
        let sigv2_sig_only = format!("Signature=sigv2&{sigv4_markers}&{payload_params}");
        hoist_signed_query_params(&mut headers, Some(&sigv2_sig_only)).expect("SigV2 Signature marker must not hoist");
        assert!(headers.is_empty());
    }

    #[test]
    fn presigned_unsigned_header_guard_accepts_hoisted_properties_and_rejects_smuggling() {
        let base = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20261126T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test%2F20261126%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=signature";
        let query = format!(
            "{base}&x-amz-object-lock-mode=GOVERNANCE&x-amz-object-lock-legal-hold=ON&x-amz-acl=private&x-amz-grant-read=id%3Dreader&x-amz-tagging=a%3Db&x-amz-storage-class=STANDARD_IA&x-amz-copy-source=%2Fsrc%2Fkey"
        );

        let mut headers = HeaderMap::new();
        hoist_signed_query_params(&mut headers, Some(&query)).expect("hoist signed properties");
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&query))
            .expect("hoisted signed query properties must pass presigned unsigned-header guard");

        // Tampered header value (does not match signed query) must be denied.
        let mut tampered = headers.clone();
        tampered.insert("x-amz-tagging", HeaderValue::from_static("a=evil"));
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&tampered, Some(&query)));

        // Extra unsigned header not in query or SignedHeaders must be denied.
        let mut extra = headers.clone();
        extra.insert("x-amz-website-redirect-location", HeaderValue::from_static("https://evil.example"));
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&extra, Some(&query)));

        // Multi-valued header (matching query + unsigned second value) must be denied.
        let mut multi = headers.clone();
        multi.append("x-amz-acl", HeaderValue::from_static("public-read"));
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&multi, Some(&query)));

        // Duplicate query parameter must never satisfy the signed-query exemption.
        let dup_query = format!("{base}&x-amz-tagging=a%3Db&X-Amz-Tagging=c%3Dd");
        let mut dup_headers = HeaderMap::new();
        dup_headers.insert("x-amz-tagging", HeaderValue::from_static("a=b"));
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&dup_headers, Some(&dup_query)));

        // Non-hoistable `x-amz-*` headers cannot bypass X-Amz-SignedHeaders via a matching query parameter.
        for (name, value) in [
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ("x-amz-replication-status", "REPLICA"),
            ("x-amz-bypass-governance-retention", "true"),
            ("x-amz-server-side-encryption-customer-key", "secret-key"),
        ] {
            let matching_query = format!("{base}&{name}={}", urlencoding::encode(value));
            let mut smuggled = HeaderMap::new();
            smuggled.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("header name"),
                HeaderValue::from_str(value).expect("header value"),
            );
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&smuggled, Some(&matching_query)));
        }
    }

    struct ContextIam {
        handle: Arc<IamSys<ObjectStore>>,
    }

    impl IamInterface for ContextIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            self.handle.clone()
        }

        fn is_ready(&self) -> bool {
            true
        }
    }

    struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<KmsServiceManager> {
            Arc::new(KmsServiceManager::new())
        }
    }

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
    fn oidc_session_cannot_inherit_root_access_from_display_name() {
        let sys_cred = Credentials {
            access_key: "root-access-key".to_string(),
            ..Default::default()
        };
        let oidc_cred = Credentials {
            access_key: "temporary-access-key".to_string(),
            parent_user: sys_cred.access_key.clone(),
            claims: Some(HashMap::from([
                ("iss".to_string(), json!("rustfs-oidc")),
                ("oidc_provider".to_string(), json!("default")),
                ("sub".to_string(), json!("subject-123")),
            ])),
            ..Default::default()
        };

        assert!(!has_root_access(&sys_cred, &oidc_cred));
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

    #[tokio::test]
    async fn test_check_claims_from_token_expired_credentials() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        let error = check_claims_from_token(&cred.session_token, &cred).unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::ExpiredToken);
        let response = error.to_http_response().expect("serialize the S3 error");
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.expect("error XML body").to_bytes();
        let xml = std::str::from_utf8(&body).expect("UTF-8 error XML");
        assert!(xml.contains("<Code>ExpiredToken</Code>"));
        assert!(!xml.contains(&cred.session_token));
        assert!(!xml.contains(&cred.secret_key));
    }

    #[test]
    fn expired_sts_credentials_require_the_matching_token() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));
        for token in ["", "wrong-session-token"] {
            let error = check_claims_from_token(token, &cred).unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn check_key_valid_preserves_sts_error_codes() {
        use rustfs_iam::{cache::Cache, manager::IamCache};
        use rustfs_policy::auth::UserIdentity;

        let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let (send_chan, _receiver) = tokio::sync::mpsc::channel(1);
        let iam_cache = Arc::new(IamCache {
            cache: Cache::default(),
            api: ObjectStore::new(store.clone()),
            state: Default::default(),
            loading: Default::default(),
            roles: HashMap::new(),
            send_chan,
            last_timestamp: Default::default(),
            sync_failures: Default::default(),
            sync_successes: Default::default(),
            last_sync_duration_millis: Default::default(),
        });
        let iam = Arc::new(IamSys::new(iam_cache.clone()));
        let context = AppContext::new(store, Arc::new(ContextIam { handle: iam }), Arc::new(TestKms));
        let root = Credentials {
            access_key: "sts-test-root".to_string(),
            secret_key: "sts-test-signing-secret".to_string(),
            status: "on".to_string(),
            ..Default::default()
        };
        assert!(context.publish_action_credentials(root.clone()));
        let now = OffsetDateTime::now_utc();
        let claims = HashMap::from([("exp".to_string(), json!((now + time::Duration::hours(1)).unix_timestamp()))]);
        let mut valid = get_new_credentials_with_metadata(&claims, &root.secret_key).expect("signed STS credentials");
        valid.parent_user = root.access_key.clone();
        iam_cache
            .cache
            .add_or_update_sts_account(&valid.access_key, &UserIdentity::new(valid.clone()), now);
        for (fixture, expected_claims) in [(&valid, Some(&claims)), (&root, None)] {
            let token = &fixture.session_token;
            let key = &fixture.access_key;
            let (verified, is_owner) = check_key_valid_with_context(token, key, Some(&context)).await.unwrap();
            assert!(is_owner);
            assert!(verified.access_key == fixture.access_key);
            assert!(verified.secret_key == fixture.secret_key);
            assert!(verified.session_token == fixture.session_token);
            assert!(verified.parent_user == fixture.parent_user);
            assert_eq!(verified.expiration, fixture.expiration);
            assert!(verified.claims.as_ref() == expected_claims);
        }

        // Seed the state of an issued identity after expiration without sleeping
        // or changing the clock used by signature verification.
        let expired_claims = HashMap::from([("exp".to_string(), json!((now - time::Duration::hours(1)).unix_timestamp()))]);
        let mut expired =
            get_new_credentials_with_metadata(&expired_claims, &root.secret_key).expect("expired signed STS credentials");
        expired.parent_user = root.access_key.clone();
        let mut disabled = expired.clone();
        disabled.status = "off".to_string();
        let mut service_account = create_service_account_credentials();
        service_account.expiration = expired.expiration;
        let mut invalid_key = create_test_credentials();
        invalid_key.secret_key = "short".to_string();
        for (name, credential, token, expected, message) in [
            (
                "expired",
                &expired,
                expired.session_token.as_str(),
                S3ErrorCode::ExpiredToken,
                "The provided token has expired.",
            ),
            ("expired without token", &expired, "", S3ErrorCode::InvalidRequest, "invalid token1"),
            (
                "expired with wrong token",
                &expired,
                "wrong-token",
                S3ErrorCode::InvalidRequest,
                "invalid token3",
            ),
            (
                "disabled expired",
                &disabled,
                disabled.session_token.as_str(),
                S3ErrorCode::InvalidAccessKeyId,
                "check key failed",
            ),
            (
                "expired service account",
                &service_account,
                "",
                S3ErrorCode::InvalidRequest,
                "check key failed",
            ),
            ("active without token", &valid, "", S3ErrorCode::InvalidRequest, "invalid token1"),
            (
                "active with wrong token",
                &valid,
                "wrong-token",
                S3ErrorCode::InvalidRequest,
                "invalid token3",
            ),
            ("invalid key", &invalid_key, "", S3ErrorCode::InvalidRequest, "check key failed"),
        ] {
            iam_cache.cache.add_or_update_sts_account(
                &credential.access_key,
                &UserIdentity::new(credential.clone()),
                OffsetDateTime::now_utc(),
            );
            let error = check_key_valid_with_context(token, &credential.access_key, Some(&context))
                .await
                .unwrap_err();
            assert_eq!(error.code(), &expected, "{name}");
            assert_eq!(error.message(), Some(message), "{name}");
            assert_eq!(error.status_code(), expected.status_code(), "{name}");
            assert!(error.source().is_none(), "{name}");
            if expected == S3ErrorCode::ExpiredToken {
                let response = error.to_http_response().expect("serialize the S3 error");
                assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
                let body = response.into_body().collect().await.expect("error XML body").to_bytes();
                assert!(std::str::from_utf8(&body).unwrap().contains("<Code>ExpiredToken</Code>"));
            }
        }
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

    #[tokio::test]
    async fn check_claims_uses_the_explicit_context_signing_secret() {
        let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("seed request IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone())
            .await
            .expect("request IAM should initialize");
        let matching = AppContext::new(store.clone(), Arc::new(ContextIam { handle: iam.clone() }), Arc::new(TestKms));
        let mismatching = AppContext::new(store, Arc::new(ContextIam { handle: iam.clone() }), Arc::new(TestKms));
        assert!(matching.publish_action_credentials(Credentials {
            access_key: "matching-root".to_string(),
            secret_key: "matching-signing-secret".to_string(),
            status: "on".to_string(),
            ..Default::default()
        }));
        assert!(mismatching.publish_action_credentials(Credentials {
            access_key: "mismatching-root".to_string(),
            secret_key: "mismatching-signing-secret".to_string(),
            status: "on".to_string(),
            ..Default::default()
        }));
        let claims = HashMap::from([
            (
                "exp".to_string(),
                json!((OffsetDateTime::now_utc() + time::Duration::minutes(5)).unix_timestamp()),
            ),
            ("context".to_string(), json!("matching")),
        ]);
        let mut credential = get_new_credentials_with_metadata(&claims, "matching-signing-secret")
            .expect("temporary credentials should be generated");
        credential.parent_user = "matching-root".to_string();
        iam.set_temp_user(&credential.access_key, &credential, None)
            .await
            .expect("temporary credentials should be stored in request IAM");

        let (verified, _) = check_key_valid_with_context(&credential.session_token, &credential.access_key, Some(&matching))
            .await
            .expect("the matching request context should verify the token");
        assert_eq!(
            verified.claims.as_ref().and_then(|claims| claims.get("context")),
            Some(&json!("matching"))
        );
        let error = check_key_valid_with_context(&credential.session_token, &credential.access_key, Some(&mismatching))
            .await
            .expect_err("a different server context must not validate the token");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
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
    fn test_get_condition_values_with_presigned_query() {
        let cred = create_test_credentials();
        let headers = HeaderMap::new();
        let uri: Uri = format!("https://example.com/?{SIGV4_PRESIGNED_QUERY}").parse().unwrap();

        let conditions = get_condition_values_with_query(&headers, &cred, None, None, None, uri.query());

        assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS4-HMAC-SHA256".to_string()]));
        assert_eq!(conditions.get("authType"), Some(&vec!["REST-QUERY-STRING".to_string()]));
    }

    #[test]
    fn test_get_condition_values_presigned_query_ignores_unsigned_headers() {
        let cred = create_test_credentials();

        for (name, value) in [
            ("awsaccesskeyid", "x"),
            ("awsaccesskeyid", ""),
            ("x-amz-credential", ""),
            ("authorization", "AWS x"),
            ("x-amz-content-sha256", STREAMING_CONTENT_SHA256),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_static(value));
            let conditions = get_condition_values_with_query(&headers, &cred, None, None, None, Some(SIGV4_PRESIGNED_QUERY));
            assert_eq!(
                conditions.get("authType"),
                Some(&vec!["REST-QUERY-STRING".to_string()]),
                "{name}: {value}"
            );
            assert_eq!(
                conditions.get("signatureversion"),
                Some(&vec!["AWS4-HMAC-SHA256".to_string()]),
                "{name}: {value}"
            );
        }

        for (name, value) in [
            ("awsaccesskeyid", ""),
            ("authorization", "AWS x"),
            ("authorization", DUMMY_SIGV4_AUTHORIZATION),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_static(value));
            let conditions = get_condition_values_with_query(&headers, &cred, None, None, None, Some(SIGV2_PRESIGNED_QUERY));
            assert_eq!(
                conditions.get("authType"),
                Some(&vec!["REST-QUERY-STRING".to_string()]),
                "{name}: {value}"
            );
            assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS2".to_string()]), "{name}: {value}");
        }
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
    fn ghsa_6r96_identity_condition_keys_ignore_spoofed_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        // A caller naming its headers after identity condition keys must not be able
        // to add or replace values the server derives from the credential.
        headers.insert("userid", "admin".parse().unwrap());
        headers.insert("username", "admin".parse().unwrap());
        headers.insert("principaltype", "Account".parse().unwrap());
        headers.insert("signatureversion", "AWS4-HMAC-SHA256".parse().unwrap());

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("userid"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("principaltype"), Some(&vec!["User".to_string()]));
        assert!(
            !conditions
                .get("signatureversion")
                .is_some_and(|v| v.iter().any(|s| s == "AWS4-HMAC-SHA256")),
            "an unsigned request must not gain a signatureversion from a header"
        );
    }

    #[test]
    fn version_id_condition_ignores_spoofed_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("versionid", "spoofed-version".parse().unwrap());

        let conditions = get_condition_values(&headers, &cred, None, None, None);
        assert_eq!(conditions.get("versionid"), None);

        let conditions = get_condition_values(&headers, &cred, Some("server-version"), None, None);
        assert_eq!(conditions.get("versionid"), Some(&vec!["server-version".to_string()]));
    }

    #[test]
    fn version_id_condition_uses_canonical_spelling() {
        let cred = create_test_credentials();
        let headers = HeaderMap::new();
        let canonical = "68f21f07-9bae-4b3a-a120-36f48fb9de1a";
        for spelling in [
            canonical,
            "68F21F07-9BAE-4B3A-A120-36F48FB9DE1A",
            "68f21f079bae4b3aa12036f48fb9de1a",
            "{68f21f07-9bae-4b3a-a120-36f48fb9de1a}",
            "urn:uuid:68f21f07-9bae-4b3a-a120-36f48fb9de1a",
            " 68f21f07-9bae-4b3a-a120-36f48fb9de1a ",
        ] {
            let conditions = get_condition_values(&headers, &cred, Some(spelling), None, None);
            assert_eq!(conditions.get("versionid"), Some(&vec![canonical.to_string()]), "{spelling}");
        }
        for spelling in [
            "null",
            "NULL",
            " null ",
            "00000000-0000-0000-0000-000000000000",
            "00000000000000000000000000000000",
        ] {
            let conditions = get_condition_values(&headers, &cred, Some(spelling), None, None);
            assert_eq!(conditions.get("versionid"), Some(&vec!["null".to_string()]), "{spelling}");
        }
        // Strings the handlers reject are passed through unchanged.
        let conditions = get_condition_values(&headers, &cred, Some("not-a-uuid"), None, None);
        assert_eq!(conditions.get("versionid"), Some(&vec!["not-a-uuid".to_string()]));
        for absent in [None, Some("")] {
            assert!(!get_condition_values(&headers, &cred, absent, None, None).contains_key("versionid"));
        }
    }

    #[test]
    fn ghsa_6r96_claim_condition_keys_ignore_spoofed_headers() {
        // The credential carries no groups/roles claims, so these keys are absent --
        // precisely the case a spoofed header would otherwise fill in.
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("groups", "admins".parse().unwrap());
        headers.insert("roles", "RustFS.ConsoleAdmin".parse().unwrap());
        headers.insert("sub", "someone-else".parse().unwrap());

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("groups"), None, "groups must come from the credential only");
        assert_eq!(conditions.get("roles"), None, "roles must come from claims only");
        assert_eq!(conditions.get("sub"), None, "jwt claim keys must come from claims only");
    }

    #[test]
    fn test_request_headers_still_reach_conditions() {
        // The reserved list must stay narrow: ordinary request headers, including the
        // `s3:x-amz-*` condition keys, are still expected to be available to policies.
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-content-sha256", "UNSIGNED-PAYLOAD".parse().unwrap());
        headers.insert("x-amz-server-side-encryption", "AES256".parse().unwrap());

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("x-amz-content-sha256"), Some(&vec!["UNSIGNED-PAYLOAD".to_string()]));
        assert_eq!(conditions.get("x-amz-server-side-encryption"), Some(&vec!["AES256".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_object_lock_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_OBJECT_LOCK_MODE_LOWER, HeaderValue::from_static("GOVERNANCE"));
        headers.insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, HeaderValue::from_static("OFF"));
        headers.insert(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, HeaderValue::from_static("2024-12-31T23:59:59Z"));
        headers.insert("object-lock-mode", HeaderValue::from_static("COMPLIANCE"));
        headers.insert("object-lock-legal-hold", HeaderValue::from_static("ON"));
        headers.insert("object-lock-retain-until-date", HeaderValue::from_static("2099-12-31T23:59:59Z"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["GOVERNANCE".to_string()]));
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
        assert_eq!(
            conditions.get("object-lock-retain-until-date"),
            Some(&vec!["2024-12-31T23:59:59Z".to_string()])
        );
    }

    #[test]
    fn object_lock_condition_aliases_cannot_spoof_canonical_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("object-lock-mode", HeaderValue::from_static("COMPLIANCE"));
        headers.insert("object-lock-legal-hold", HeaderValue::from_static("ON"));
        headers.insert("object-lock-retain-until-date", HeaderValue::from_static("2099-12-31T23:59:59Z"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("object-lock-mode"), None);
        assert_eq!(conditions.get("object-lock-legal-hold"), None);
        assert_eq!(conditions.get("object-lock-retain-until-date"), None);
    }

    #[test]
    fn object_lock_remaining_retention_days_header_is_ignored() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, HeaderValue::from_static("1"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION), None);
    }

    #[test]
    fn test_get_condition_values_with_grant_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-grant-full-control", HeaderValue::from_static("id=owner-123"));
        headers.insert(
            "x-amz-grant-read",
            HeaderValue::from_static("uri=http://acs.amazonaws.com/groups/global/AllUsers"),
        );

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("x-amz-grant-full-control"), Some(&vec!["id=owner-123".to_string()]));
        assert_eq!(
            conditions.get("x-amz-grant-read"),
            Some(&vec!["uri=http://acs.amazonaws.com/groups/global/AllUsers".to_string()])
        );
    }

    #[test]
    fn test_get_condition_values_with_conditional_write_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("If-None-Match", HeaderValue::from_static(" * "));
        headers.insert("If-Match", HeaderValue::from_static("\"9b2cf535f27731c974343645a3985328\""));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("if-none-match"), Some(&vec!["*".to_string()]));
        assert_eq!(
            conditions.get("if-match"),
            Some(&vec!["\"9b2cf535f27731c974343645a3985328\"".to_string()])
        );
    }

    /// Storage ignores a blank precondition, and s3s rejects a non-UTF-8 one, so
    /// neither may satisfy a `Null: false` check on `s3:if-none-match` / `s3:if-match`.
    #[test]
    fn test_get_condition_values_without_conditional_write_headers() {
        let cred = create_test_credentials();
        let mut blank = HeaderMap::new();
        blank.insert("If-None-Match", HeaderValue::from_static(""));
        blank.insert("If-Match", HeaderValue::from_static(""));
        let mut non_utf8 = HeaderMap::new();
        non_utf8.insert(
            "If-None-Match",
            HeaderValue::from_bytes(b"\xff").expect("obs-text is a valid header value"),
        );
        non_utf8.insert("If-Match", HeaderValue::from_bytes(b"\xff").expect("obs-text is a valid header value"));

        let mut existence_only = HeaderMap::new();
        existence_only.insert(IF_MATCH, HeaderValue::from_static(" * "));
        for headers in [HeaderMap::new(), blank, non_utf8, existence_only] {
            let conditions = get_condition_values(&headers, &cred, None, None, None);

            assert!(!conditions.contains_key("if-none-match"), "headers {headers:?}");
            assert!(!conditions.contains_key("if-match"), "headers {headers:?}");
        }
    }

    #[test]
    fn test_get_condition_values_conditional_write_headers_cannot_be_overridden_by_claims() {
        let mut cred = create_service_account_credentials();
        cred.claims = Some(HashMap::from([
            ("if-none-match".to_string(), json!("*")),
            ("if-match".to_string(), json!("\"claim-etag\"")),
            ("IF-NONE-MATCH".to_string(), json!("*")),
            ("ldapIF-MATCH".to_string(), json!("\"claim-etag\"")),
        ]));

        for value in [None, Some(""), Some(" ")] {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(IF_NONE_MATCH, HeaderValue::from_str(value).expect("valid header"));
                headers.insert(IF_MATCH, HeaderValue::from_str(value).expect("valid header"));
            }
            let conditions = get_condition_values(&headers, &cred, None, None, None);
            assert!(
                !conditions.contains_key("if-none-match"),
                "claim must not fabricate an absent precondition"
            );
            assert!(!conditions.contains_key("if-match"), "claim must not fabricate an absent precondition");
        }

        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        headers.insert(IF_MATCH, HeaderValue::from_static("\"request-etag\""));
        let conditions = get_condition_values(&headers, &cred, None, None, None);
        assert_eq!(conditions.get("if-none-match"), Some(&vec!["*".to_string()]));
        assert_eq!(conditions.get("if-match"), Some(&vec!["\"request-etag\"".to_string()]));
    }

    #[test]
    fn test_get_condition_values_preserves_consumed_headers_and_multivalues() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-signature-age", HeaderValue::from_static("300"));
        headers.insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        headers.insert("x-amz-object-lock-legal-hold", HeaderValue::from_static("ON"));
        headers.insert("x-amz-object-lock-retain-until-date", HeaderValue::from_static("2030-01-01T00:00:00Z"));
        headers.insert("x-amz-tagging", HeaderValue::from_static("project=blue"));
        for name in [
            "x-amz-grant-full-control",
            "x-amz-grant-read",
            "x-amz-grant-write",
            "x-amz-grant-read-acp",
            "x-amz-grant-write-acp",
            "x-custom-header",
        ] {
            headers.append(name, HeaderValue::from_static("first"));
            headers.append(name, HeaderValue::from_static("second"));
        }
        headers.append(IF_MATCH, HeaderValue::from_static(" \"first-etag\" "));
        headers.append(IF_MATCH, HeaderValue::from_static("\"second-etag\""));

        let conditions = get_condition_values(&headers, &cred, None, None, None);
        for (name, expected) in [
            ("object-lock-mode", "GOVERNANCE"),
            ("object-lock-legal-hold", "ON"),
            ("object-lock-retain-until-date", "2030-01-01T00:00:00Z"),
            ("if-match", "\"first-etag\""),
        ] {
            assert_eq!(conditions.get(name), Some(&vec![expected.to_string()]), "{name}");
        }
        assert!(
            !conditions.contains_key("signatureAge"),
            "client-supplied x-amz-signature-age must not populate signatureAge"
        );
        for name in [
            "x-amz-signature-age",
            "x-amz-object-lock-mode",
            "x-amz-object-lock-legal-hold",
            "x-amz-object-lock-retain-until-date",
            "x-amz-tagging",
        ] {
            assert!(!conditions.contains_key(name), "consumed header {name} must not be merged again");
        }
        for name in [
            "x-amz-grant-full-control",
            "x-amz-grant-read",
            "x-amz-grant-write",
            "x-amz-grant-read-acp",
            "x-amz-grant-write-acp",
            "x-custom-header",
        ] {
            assert_eq!(conditions.get(name), Some(&vec!["first".to_string(), "second".to_string()]), "{name}");
        }
    }

    #[test]
    fn test_get_condition_values_with_signature_age() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-signature-age", HeaderValue::from_static("300"));

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert!(
            !conditions.contains_key("signatureAge"),
            "client-supplied x-amz-signature-age must not populate signatureAge"
        );
        assert!(
            !conditions.contains_key("x-amz-signature-age"),
            "client-supplied x-amz-signature-age must not be forwarded as a condition"
        );
    }

    #[tokio::test]
    async fn oidc_issuer_condition_uses_verified_provider_not_session_marker_or_header() {
        use rustfs_policy::policy::{
            Args, Policy,
            action::{Action, S3Action},
        };

        let policy = Policy::parse_config(
            br#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},
                {"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*",
                 "Condition":{"StringEquals":{"jwt:iss":"https://partner.example"}}}
            ]}"#,
        )
        .expect("parse issuer condition");
        let mut credentials = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("iss", HeaderValue::from_static("https://spoofed.invalid"));
        headers.insert(OIDC_ISSUER_CLAIM, HeaderValue::from_static("https://spoofed.invalid"));
        for issuer in [Some("https://partner.example"), Some("https://corporate.example"), None] {
            let mut claims = HashMap::from([
                ("iss".to_string(), json!("rustfs-oidc")),
                ("oidc_provider".to_string(), json!("provider")),
                ("sub".to_string(), json!("subject")),
            ]);
            if let Some(issuer) = issuer {
                claims.insert(OIDC_ISSUER_CLAIM.to_string(), json!(issuer));
            }
            credentials.claims = Some(claims);
            let conditions = get_condition_values(&headers, &credentials, None, None, None);
            assert_eq!(conditions.get("iss"), issuer.map(|value| vec![value.to_string()]).as_ref());
            assert_eq!(
                policy
                    .is_allowed(&Args {
                        account: &credentials.access_key,
                        groups: &credentials.groups,
                        action: Action::S3Action(S3Action::GetObjectAction),
                        bucket: "bucket",
                        object: "object",
                        conditions: &conditions,
                        claims: credentials.claims_or_empty(),
                        is_owner: false,
                        deny_only: false,
                    })
                    .await,
                issuer != Some("https://partner.example"),
            );
        }
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
    fn test_get_condition_values_with_roles_claim_array() {
        let mut cred = create_service_account_credentials();
        let mut claims = HashMap::new();
        claims.insert("roles".to_string(), json!(["role1", "role2"]));
        cred.claims = Some(claims);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("roles"), Some(&vec!["role1".to_string(), "role2".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_roles_claim_csv_and_case_insensitive() {
        let mut cred = create_service_account_credentials();
        let mut claims = HashMap::new();
        claims.insert("Roles".to_string(), json!("role1, role2"));
        cred.claims = Some(claims);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("roles"), Some(&vec!["role1".to_string(), "role2".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_roles_claim_ambiguous_case_insensitive_match_returns_empty() {
        let mut cred = create_service_account_credentials();
        let mut claims = HashMap::new();
        claims.insert("Roles".to_string(), json!(["role1"]));
        claims.insert("ROLES".to_string(), json!(["role2"]));
        cred.claims = Some(claims);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("roles"), None);
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

    fn assert_invalid_capability(error: rustfs_s3_types::S3Error, message: &str) {
        assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidRequest);
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_eq!(error.message(), Some(message));
    }

    #[test]
    fn presigned_put_max_content_length_requires_exactly_one_signed_query_value() {
        let headers = HeaderMap::new();
        let signed_prefix = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";

        let query = format!("{signed_prefix}&x-rustfs-max-content-length=104857600");
        assert_eq!(
            parse_presigned_put_max_content_length(&headers, Some(&query), true).unwrap(),
            Some(104_857_600)
        );

        let encoded_credential = query.replacen("X-Amz-Credential", "X%2DAmz-Credential", 1);
        assert_eq!(
            parse_presigned_put_max_content_length(&headers, Some(&encoded_credential), true).unwrap(),
            Some(104_857_600)
        );

        let duplicate = format!("{query}&x-rustfs-max-content-length=1");
        assert_invalid_capability(
            parse_presigned_put_max_content_length(&headers, Some(&duplicate), true).unwrap_err(),
            "x-rustfs-max-content-length must appear exactly once",
        );

        let wrong_case = format!("{signed_prefix}&X-RustFS-Max-Content-Length=1");
        assert_invalid_capability(
            parse_presigned_put_max_content_length(&headers, Some(&wrong_case), true).unwrap_err(),
            "query parameter name must be exactly x-rustfs-max-content-length",
        );

        assert_invalid_capability(
            reject_presigned_put_max_content_length_for_other_operation(&headers, Some(&query), true).unwrap_err(),
            "x-rustfs-max-content-length is only supported for presigned PutObject",
        );
        let overflow = format!("{signed_prefix}&x-rustfs-max-content-length=18446744073709551616");
        assert_invalid_capability(
            parse_presigned_put_max_content_length(&headers, Some(&overflow), true).unwrap_err(),
            "x-rustfs-max-content-length must be a non-negative 64-bit integer",
        );
    }

    /// GHSA-g8w9-qw9q-fghr: `x-amz-*` request headers that are not listed in
    /// `X-Amz-SignedHeaders` must not survive the presigned access boundary.
    #[test]
    fn ghsa_g8w9_presigned_request_rejects_unsigned_x_amz_headers() {
        let presigned_host_only = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";

        for header in [
            "x-amz-tagging",
            "x-amz-website-redirect-location",
            "x-amz-storage-class",
            "x-amz-acl",
            "x-amz-meta-owner",
            "x-amz-object-lock-mode",
            "x-amz-server-side-encryption",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("content-type", HeaderValue::from_static("text/plain"));
            headers.insert(header, HeaderValue::from_static("attacker-controlled"));
            let error = reject_unsigned_amz_headers_on_presigned_request(&headers, Some(presigned_host_only)).unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{header} must be rejected when unsigned");
            assert_eq!(error.message(), Some(UNSIGNED_HEADERS_MESSAGE));
        }

        // Non-`x-amz-*` headers are outside the SigV4 rule and stay allowed.
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("cache-control", HeaderValue::from_static("no-store"));
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(presigned_host_only)).unwrap();

        // A missing SignedHeaders list signs nothing and still fails closed.
        let missing_signed_headers = presigned_host_only.replace("&X-Amz-SignedHeaders=host", "");
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-tagging", HeaderValue::from_static("a=b"));
        assert_eq!(
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&missing_signed_headers))
                .unwrap_err()
                .code(),
            &S3ErrorCode::AccessDenied
        );

        // Detection follows the upstream dispatch: any query carrying the
        // signature is a presigned request, whatever the key's case.
        let lowercase_query = presigned_host_only.to_ascii_lowercase();
        assert_eq!(
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&lowercase_query))
                .unwrap_err()
                .code(),
            &S3ErrorCode::AccessDenied
        );

        // Only the exact key the upstream verifier reads counts; a second
        // (or differently cased) list must not widen the signed set, and a
        // duplicate exact key signs nothing at all.
        let widened_by_case = format!("{presigned_host_only}&x-amz-signedheaders=host%3Bx-amz-tagging");
        assert_eq!(
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&widened_by_case))
                .unwrap_err()
                .code(),
            &S3ErrorCode::AccessDenied
        );
        let duplicated = format!("{presigned_host_only}&X-Amz-SignedHeaders=host%3Bx-amz-tagging");
        assert_eq!(
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&duplicated))
                .unwrap_err()
                .code(),
            &S3ErrorCode::AccessDenied
        );
    }

    #[test]
    fn ghsa_g8w9_presigned_request_accepts_signed_or_exempt_x_amz_headers() {
        let signed_tagging = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host%3Bx-amz-tagging%3Bx-amz-meta-owner&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-tagging", HeaderValue::from_static("owner=app"));
        headers.insert("X-Amz-Meta-Owner", HeaderValue::from_static("app"));
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(signed_tagging)).unwrap();

        // Case differences in the signed list do not matter; header names are
        // canonicalised to lowercase on both sides.
        let uppercase_list = signed_tagging.replace("x-amz-tagging", "X-Amz-Tagging");
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&uppercase_list)).unwrap();

        // The CDN request id is the only unsigned `x-amz-*` header tolerated.
        headers.insert("x-amz-cf-id", HeaderValue::from_static("cloudfront-request-id"));
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(signed_tagging)).unwrap();

        // Adding one more unsigned header on top of signed ones still fails.
        headers.insert("x-amz-storage-class", HeaderValue::from_static("REDUCED_REDUNDANCY"));
        assert_eq!(
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(signed_tagging))
                .unwrap_err()
                .code(),
            &S3ErrorCode::AccessDenied
        );
    }

    #[test]
    fn ghsa_g8w9_check_ignores_header_signed_sigv2_and_anonymous_requests() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-tagging", HeaderValue::from_static("owner=app"));
        headers.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));

        // No query at all: nothing to bind against.
        reject_unsigned_amz_headers_on_presigned_request(&headers, None).unwrap();

        // Header-signed SigV4 and SigV2 carry no `X-Amz-Signature` query.
        headers.insert(
            "authorization",
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=test/20260827/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc",
            ),
        );
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some("versioning=")).unwrap();

        // SigV2 presigned URLs sign every `x-amz-*` header in the string to sign.
        let sigv2_query = "AWSAccessKeyId=test&Expires=1893456000&Signature=abc";
        reject_unsigned_amz_headers_on_presigned_request(&headers, Some(sigv2_query)).unwrap();
    }

    fn header_sigv4_authorization(algorithm: &str, signed_headers: &str) -> HeaderValue {
        format!(
            "{algorithm} Credential=test/20260827/us-east-1/s3/aws4_request, SignedHeaders={signed_headers}, Signature={}",
            "0".repeat(64)
        )
        .parse()
        .expect("authorization header")
    }

    fn header_sigv4_headers(signed_headers: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", header_sigv4_authorization(SIGN_V4_ALGORITHM, signed_headers));
        headers.insert("x-amz-date", HeaderValue::from_static("20260827T000000Z"));
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("UNSIGNED-PAYLOAD"));
        headers
    }

    fn assert_unsigned_headers_denied(result: OwnedResult<()>) {
        let error = result.expect_err("unsigned x-amz header must be denied");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(error.message(), Some(UNSIGNED_HEADERS_MESSAGE));
    }

    /// GHSA-xm99-m3gq-83g8: a header-signed SigV4 request must not carry an
    /// `x-amz-*` header its `SignedHeaders` list leaves out. An unsigned
    /// `x-amz-copy-source` turned a replayed PutObject into a CopyObject.
    #[test]
    fn ghsa_xm99_header_sigv4_rejects_unsigned_x_amz_headers() {
        for name in [
            "x-amz-copy-source",
            "x-amz-copy-source-range",
            "x-amz-tagging",
            "x-amz-meta-owner",
            "x-amz-metadata-directive",
            "x-amz-security-token",
            "x-amz-server-side-encryption",
        ] {
            let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-date");
            headers.insert(name, HeaderValue::from_static("injected"));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, Some("tagging")));
        }

        // `x-amz-date` is read by the verifier, but it is not exempt here.
        let headers = header_sigv4_headers("host;x-amz-content-sha256");
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));
    }

    #[test]
    fn ghsa_xm99_header_sigv4_accepts_signed_or_exempt_x_amz_headers() {
        // The payload hash is bound through the canonical request's payload
        // field, the aws-chunked framing headers wrap an already signed
        // request, and CloudFront stamps `x-amz-cf-id` after the client signs.
        let mut headers = header_sigv4_headers("host;x-amz-date");
        headers.insert("x-amz-cf-id", HeaderValue::from_static("cdn-request"));
        headers.insert("x-amz-decoded-content-length", HeaderValue::from_static("1024"));
        headers.insert("x-amz-trailer", HeaderValue::from_static("x-amz-checksum-crc32"));
        headers.insert("x-amz-checksum-algorithm", HeaderValue::from_static("CRC32"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("envelope headers and CDN id are exempt");

        // The exemption is by exact name, not by prefix.
        headers.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("CRC32"));
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));

        let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-copy-source;x-amz-date");
        headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("signed copy source is allowed");

        // The verifier looks signed names up case-insensitively.
        let headers = header_sigv4_headers("Host;X-Amz-Content-Sha256;X-Amz-Date");
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("mixed-case signed names are allowed");
    }

    /// The verifier accepts any algorithm token in the Authorization header, so
    /// swapping it must not move the request out of this check.
    #[test]
    fn ghsa_xm99_header_sigv4_rejects_unsupported_algorithm_token() {
        for algorithm in ["OTHER", "aws4-hmac-sha256", "AWS4-ECDSA-P256-SHA256"] {
            let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-date");
            headers.insert(
                "authorization",
                header_sigv4_authorization(algorithm, "host;x-amz-content-sha256;x-amz-date"),
            );
            let error = reject_unsigned_amz_headers_on_sigv4_request(&headers, None)
                .expect_err("non-SigV4 algorithm token must be denied");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            assert_eq!(error.message(), Some(UNSUPPORTED_SIGV4_ALGORITHM_MESSAGE));

            headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
            reject_unsigned_amz_headers_on_sigv4_request(&headers, None)
                .expect_err("algorithm token cannot smuggle an unsigned copy source");
        }

        let mut headers = header_sigv4_headers("host");
        headers.insert("authorization", HeaderValue::from_static("AWS4-HMAC-SHA256 invalid"));
        let error =
            reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect_err("malformed SigV4 header must fail closed");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    }

    /// Every SigV4 signed-header list present must cover every `x-amz-*`
    /// header: neither auth form can widen the other.
    #[test]
    fn ghsa_xm99_header_and_query_signatures_cannot_widen_each_other() {
        let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-copy-source;x-amz-date");
        headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
        let presigned = "X-Amz-Signature=test&X-Amz-SignedHeaders=host";
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, Some(presigned)));

        let headers_signing_less = {
            let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-date");
            headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
            headers
        };
        let presigned_copy = "X-Amz-Signature=test&X-Amz-SignedHeaders=host%3Bx-amz-copy-source%3Bx-amz-date";
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers_signing_less, Some(presigned_copy)));

        // A duplicate Authorization header is checked entry by entry.
        let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-copy-source;x-amz-date");
        headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
        headers.append(
            "authorization",
            header_sigv4_authorization(SIGN_V4_ALGORITHM, "host;x-amz-content-sha256;x-amz-date"),
        );
        assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));
    }

    #[test]
    fn ghsa_xm99_check_ignores_sigv2_jwt_and_anonymous_requests() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-copy-source", HeaderValue::from_static("/source/secret"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("anonymous auth is handled downstream");

        for authorization in ["AWS key:signature", "Bearer token"] {
            headers.insert("authorization", HeaderValue::from_static(authorization));
            reject_unsigned_amz_headers_on_sigv4_request(&headers, None)
                .expect("non-SigV4 authorization carries no signed-header list");
        }
    }

    const EXTENSION_HEADERS: [&str; 10] = [
        "x-rustfs-source-replication-request",
        "x-rustfs-source-version-id",
        "x-rustfs-source-mtime",
        "x-rustfs-source-etag",
        "x-rustfs-replication-ssec-original-size",
        "x-rustfs-force-delete",
        "x-rustfs-meta-owner",
        "x-minio-force-delete",
        "x-minio-meta-owner",
        "x-minio-source-version-id",
    ];

    #[test]
    fn presigned_request_rejects_unsigned_extension_headers() {
        let presigned_host_only = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";

        for name in EXTENSION_HEADERS {
            let mut headers = HeaderMap::new();
            headers.insert("content-type", HeaderValue::from_static("text/plain"));
            headers.insert(name, HeaderValue::from_static("true"));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(presigned_host_only)));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, Some(presigned_host_only)));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(
                &headers,
                Some(presigned_host_only),
            ));

            let signed = presigned_host_only.replace("SignedHeaders=host", &format!("SignedHeaders=host%3B{name}"));
            reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&signed))
                .expect("signed extension header is allowed");
            reject_unsigned_extension_headers_on_signed_request(&headers, Some(&signed))
                .expect("signed extension header is allowed on gateway guard");

            // Extension headers are never hoisted from query parameters, so matching
            // a query parameter cannot substitute for listing them in X-Amz-SignedHeaders.
            let as_query = format!("{presigned_host_only}&{name}=true");
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(&as_query)));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(&headers, Some(&as_query)));
        }
    }

    #[test]
    fn header_sigv4_rejects_unsigned_extension_headers() {
        for name in EXTENSION_HEADERS {
            let mut headers = header_sigv4_headers("host;x-amz-content-sha256;x-amz-date");
            headers.insert(name, HeaderValue::from_static("true"));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(&headers, None));

            let mut headers = header_sigv4_headers(&format!("host;x-amz-content-sha256;x-amz-date;{name}"));
            headers.insert(name, HeaderValue::from_static("true"));
            reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("signed extension header is allowed");
            reject_unsigned_extension_headers_on_signed_request(&headers, None)
                .expect("signed extension header is allowed on gateway guard");
        }
    }

    #[test]
    fn sigv2_request_rejects_extension_headers() {
        let sigv2_query = "AWSAccessKeyId=test&Expires=1893456000&Signature=abc";

        for name in EXTENSION_HEADERS {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_static("true"));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_presigned_request(&headers, Some(sigv2_query)));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(&headers, Some(sigv2_query)));

            // Appending SigV4 query parameters to a SigV2 URL must not allow an extension header.
            let widened = format!("{sigv2_query}&X-Amz-Signature=abc&X-Amz-SignedHeaders=host%3B{name}");
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, Some(&widened)));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(&headers, Some(&widened)));

            headers.insert("authorization", HeaderValue::from_static("AWS key:signature"));
            assert_unsigned_headers_denied(reject_unsigned_amz_headers_on_sigv4_request(&headers, None));
            assert_unsigned_headers_denied(reject_unsigned_extension_headers_on_signed_request(&headers, None));
        }

        // Plain SigV2 requests without extension headers remain accepted.
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("x-amz-meta-owner", HeaderValue::from_static("app"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, Some(sigv2_query)).expect("SigV2 signs x-amz-* itself");
        reject_unsigned_extension_headers_on_signed_request(&headers, Some(sigv2_query))
            .expect("gateway guard ignores plain SigV2");
        headers.insert("authorization", HeaderValue::from_static("AWS key:signature"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("SigV2 signs x-amz-* itself");
        reject_unsigned_extension_headers_on_signed_request(&headers, None).expect("gateway guard ignores plain SigV2");
    }

    #[test]
    fn extension_header_check_ignores_jwt_and_anonymous_requests() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-force-delete", HeaderValue::from_static("true"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("anonymous auth is handled downstream");
        reject_unsigned_extension_headers_on_signed_request(&headers, None).expect("anonymous auth is handled downstream");
        reject_unsigned_amz_headers_on_sigv4_request(&headers, Some("versionId=null")).expect("no signature in the query");

        headers.insert("authorization", HeaderValue::from_static("Bearer token"));
        reject_unsigned_amz_headers_on_sigv4_request(&headers, None).expect("JWT carries no request signature");
        reject_unsigned_extension_headers_on_signed_request(&headers, None).expect("JWT carries no request signature");
    }

    #[test]
    fn is_presigned_request_head_detects_sigv4_and_sigv2_presigned_requests() {
        let headers = HeaderMap::new();
        assert!(!is_presigned_request_head(&headers, None));
        assert!(!is_presigned_request_head(&headers, Some("versionId=null")));
        assert!(is_presigned_request_head(&headers, Some("X-Amz-Signature=deadbeef")));
        assert!(is_presigned_request_head(&headers, Some("Signature=abc&AWSAccessKeyId=test")));
        assert!(is_presigned_request_head(
            &headers,
            Some("x-amz-credential=test/20260827/us-east-1/s3/aws4_request")
        ));

        // Mixed header + presigned query is still detected as presigned.
        let signed = header_sigv4_headers("host;x-amz-content-sha256;x-amz-date");
        assert!(!is_presigned_request_head(&signed, None));
        assert!(is_presigned_request_head(&signed, Some("X-Amz-Signature=deadbeef")));
    }

    #[test]
    fn presigned_put_max_content_length_rejects_unsigned_or_invalid_values() {
        let headers = HeaderMap::new();
        let forged = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/credential&X-Amz-Signature=fake&x-rustfs-max-content-length=1";
        assert_invalid_capability(
            parse_presigned_put_max_content_length(&headers, Some(forged), false).unwrap_err(),
            "x-rustfs-max-content-length requires a SigV4 presigned request",
        );
        for query in [
            "x-rustfs-max-content-length=1",
            "X-Amz-Credential=test/credential&x-rustfs-max-content-length=-1",
            "X-Amz-Credential=test/credential&x-rustfs-max-content-length=18446744073709551616",
        ] {
            let error = parse_presigned_put_max_content_length(&headers, Some(query), true).unwrap_err();
            assert_invalid_capability(error, "x-rustfs-max-content-length requires a SigV4 presigned request");
        }
    }

    #[test]
    fn multipart_max_total_object_size_requires_signed_create_request() {
        let headers = HeaderMap::new();
        let signed_prefix = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";
        let query = format!("{signed_prefix}&x-rustfs-max-total-object-size=104857600");

        assert_eq!(
            parse_presigned_multipart_max_total_object_size(&headers, Some(&query), true).unwrap(),
            Some(104_857_600)
        );
        assert_invalid_capability(
            reject_presigned_multipart_max_total_object_size_for_other_operation(&headers, Some(&query), true).unwrap_err(),
            "x-rustfs-max-total-object-size is only supported for CreateMultipartUpload",
        );
        let overflow = format!("{signed_prefix}&x-rustfs-max-total-object-size=18446744073709551616");
        assert_invalid_capability(
            parse_presigned_multipart_max_total_object_size(&headers, Some(&overflow), true).unwrap_err(),
            "x-rustfs-max-total-object-size must be a non-negative 64-bit integer",
        );
    }

    #[test]
    fn multipart_max_total_object_size_rejects_tampering_and_invalid_values() {
        let headers = HeaderMap::new();
        let signed_prefix = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260827T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Credential=test/20260827/us-east-1/s3/aws4_request&X-Amz-Signature=signature";
        let auth_message = "x-rustfs-max-total-object-size requires a verified SigV4 request";
        for (query, message) in [
            ("x-rustfs-max-total-object-size=1", auth_message),
            (
                "X-RustFS-Max-Total-Object-Size=1",
                "query parameter name must be exactly x-rustfs-max-total-object-size",
            ),
            (
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&x-rustfs-max-total-object-size=1&x-rustfs-max-total-object-size=2",
                "x-rustfs-max-total-object-size must appear exactly once",
            ),
            ("X-Amz-Algorithm=AWS4-HMAC-SHA256&x-rustfs-max-total-object-size=-1", auth_message),
            (
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&x-rustfs-max-total-object-size=18446744073709551616",
                auth_message,
            ),
        ] {
            let error = parse_presigned_multipart_max_total_object_size(&headers, Some(query), true).unwrap_err();
            assert_invalid_capability(error, message);
        }

        let forged = format!("{signed_prefix}&x-rustfs-max-total-object-size=1");
        assert_invalid_capability(
            parse_presigned_multipart_max_total_object_size(&headers, Some(&forged), false).unwrap_err(),
            "x-rustfs-max-total-object-size requires a verified SigV4 request",
        );
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

    const SIGV2_PRESIGNED_QUERY: &str = "AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Signature=example&Expires=1672531200";
    const SIGV4_PRESIGNED_QUERY: &str = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=example";
    const SIGV2_AUTHORIZATION: &str = "AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//bqJZQ1iY=";
    const DUMMY_SIGV4_AUTHORIZATION: &str =
        "AWS4-HMAC-SHA256 Credential=x/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00";
    const FORM_CONTENT_TYPE: &str = "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW";

    fn headers_from(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(*name, HeaderValue::from_static(value));
        }
        headers
    }

    #[test]
    fn test_get_request_auth_type_presigned_v2_header_is_not_read() {
        let mut headers = HeaderMap::new();
        headers.insert("AWSAccessKeyId", HeaderValue::from_static("AKIAIOSFODNN7EXAMPLE"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Anonymous);
    }

    #[test]
    fn test_get_request_auth_type_presigned_v2_from_query() {
        let headers = HeaderMap::new();
        let uri: Uri = format!("https://example.com/?{SIGV2_PRESIGNED_QUERY}").parse().unwrap();

        let auth_type = get_request_auth_type_with_query(&headers, uri.query());

        assert_eq!(auth_type, AuthType::PresignedV2);
    }

    #[test]
    fn test_get_request_auth_type_presigned_v4_header_is_not_read() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Amz-Credential",
            HeaderValue::from_static("AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
        );

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::Anonymous);
    }

    #[test]
    fn test_get_request_auth_type_presigned_v4_from_query() {
        let headers = HeaderMap::new();
        let uri: Uri = format!("https://example.com/?{SIGV4_PRESIGNED_QUERY}").parse().unwrap();

        let auth_type = get_request_auth_type_with_query(&headers, uri.query());

        assert_eq!(auth_type, AuthType::Presigned);
    }

    #[test]
    fn test_get_request_auth_type_presigned_query_ignores_unsigned_headers() {
        let unsigned: [&[(&str, &str)]; 10] = [
            &[("awsaccesskeyid", "")],
            &[("awsaccesskeyid", "x")],
            &[("x-amz-credential", "")],
            &[("authorization", "AWS x")],
            &[("authorization", "Basic dXNlcjpwYXNz")],
            &[("authorization", SIGV2_AUTHORIZATION), ("authorization", SIGV2_AUTHORIZATION)],
            &[
                ("authorization", DUMMY_SIGV4_AUTHORIZATION),
                ("authorization", DUMMY_SIGV4_AUTHORIZATION),
            ],
            &[("x-amz-content-sha256", STREAMING_CONTENT_SHA256)],
            &[("content-type", FORM_CONTENT_TYPE)],
            &[("action", "AssumeRole")],
        ];
        for pairs in unsigned {
            let headers = headers_from(pairs);
            for method in [None, Some(&Method::GET), Some(&Method::PUT)] {
                assert_eq!(
                    get_request_auth_type_with_method_and_query(method, &headers, Some(SIGV2_PRESIGNED_QUERY)),
                    AuthType::PresignedV2,
                    "{method:?} {pairs:?}"
                );
                assert_eq!(
                    get_request_auth_type_with_method_and_query(method, &headers, Some(SIGV4_PRESIGNED_QUERY)),
                    AuthType::Presigned,
                    "{method:?} {pairs:?}"
                );
            }
        }
    }

    #[test]
    fn test_get_request_auth_type_presigned_query_decodes_signature_parameter() {
        let headers = HeaderMap::new();
        for (query, expected) in [
            ("X%2DAmz-Signature=example", AuthType::Presigned),
            ("Signature=example", AuthType::PresignedV2),
            (
                "AWSAccessKey%49d=AKIAIOSFODNN7EXAMPLE&Expires=1672531200&Signature=example",
                AuthType::PresignedV2,
            ),
            (
                "AWSAccessKey%49d=AKIAIOSFODNN7EXAMPLE&Expires=1672531200&%53ignature=example",
                AuthType::PresignedV2,
            ),
            (
                "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
                AuthType::Anonymous,
            ),
            ("x-amz-signature=example", AuthType::Anonymous),
            ("AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE", AuthType::Anonymous),
            ("signature=example", AuthType::Anonymous),
        ] {
            assert_eq!(get_request_auth_type_with_query(&headers, Some(query)), expected, "{query}");
        }
    }

    #[test]
    fn test_get_request_auth_type_anonymous_ignores_unverified_streaming_and_duplicate_headers() {
        let unverified: [&[(&str, &str)]; 5] = [
            &[("x-amz-content-sha256", STREAMING_CONTENT_SHA256)],
            &[("x-amz-content-sha256", STREAMING_CONTENT_SHA256_TRAILER)],
            &[("x-amz-content-sha256", UNSIGNED_PAYLOAD_TRAILER)],
            &[
                ("authorization", DUMMY_SIGV4_AUTHORIZATION),
                ("authorization", DUMMY_SIGV4_AUTHORIZATION),
            ],
            &[("authorization", SIGV2_AUTHORIZATION), ("authorization", SIGV2_AUTHORIZATION)],
        ];
        for pairs in unverified {
            let headers = headers_from(pairs);
            for method in [None, Some(&Method::GET)] {
                assert_eq!(
                    get_request_auth_type_with_method_and_query(method, &headers, None),
                    AuthType::Anonymous,
                    "{method:?} {pairs:?}"
                );
            }
        }
    }

    #[test]
    fn test_get_request_auth_type_malformed_signature_v2() {
        let headers = headers_from(&[("authorization", "AWS x")]);
        assert_eq!(get_request_auth_type(&headers), AuthType::Unknown);
    }

    #[test]
    fn test_get_request_auth_type_post_policy() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static(FORM_CONTENT_TYPE));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::PostPolicy);
    }

    #[test]
    fn test_get_request_auth_type_post_policy_ignores_unsigned_headers_and_checks_method() {
        for pairs in [
            &[("content-type", FORM_CONTENT_TYPE), ("awsaccesskeyid", "x")][..],
            &[("content-type", FORM_CONTENT_TYPE), ("x-amz-credential", "x")][..],
            &[
                ("content-type", FORM_CONTENT_TYPE),
                ("authorization", DUMMY_SIGV4_AUTHORIZATION),
            ][..],
            &[
                ("content-type", FORM_CONTENT_TYPE),
                ("x-amz-content-sha256", STREAMING_CONTENT_SHA256),
            ][..],
        ] {
            let headers = headers_from(pairs);
            assert_eq!(
                get_request_auth_type_with_method_and_query(Some(&Method::POST), &headers, None),
                AuthType::PostPolicy,
                "{pairs:?}"
            );
        }

        // Without method, a POST form with unsigned AWSAccessKeyId still stays PostPolicy.
        let headers = headers_from(&[("content-type", FORM_CONTENT_TYPE), ("awsaccesskeyid", "x")]);
        assert_eq!(get_request_auth_type_with_query(&headers, None), AuthType::PostPolicy);

        // A multipart/form-data Content-Type on a non-POST method is not a POST policy form.
        let headers = headers_from(&[("content-type", FORM_CONTENT_TYPE)]);
        assert_eq!(
            get_request_auth_type_with_method_and_query(Some(&Method::GET), &headers, None),
            AuthType::Anonymous
        );
        assert_eq!(
            get_request_auth_type_with_method_and_query(Some(&Method::PUT), &headers, None),
            AuthType::Anonymous
        );

        // Non-multipart/form-data or repeated Content-Type values are not POST policy forms.
        for pairs in [
            &[("content-type", "text/plain; note=\"multipart/form-data\"")][..],
            &[("content-type", "multipart/form-data; boundary")][..],
            &[("content-type", FORM_CONTENT_TYPE), ("content-type", FORM_CONTENT_TYPE)][..],
        ] {
            let headers = headers_from(pairs);
            assert_eq!(
                get_request_auth_type_with_method_and_query(Some(&Method::POST), &headers, None),
                AuthType::Anonymous,
                "{pairs:?}"
            );
        }
    }

    #[test]
    fn test_get_request_auth_type_streaming_signed() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static(DUMMY_SIGV4_AUTHORIZATION));
        headers.insert("x-amz-content-sha256", HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));

        let auth_type = get_request_auth_type(&headers);

        assert_eq!(auth_type, AuthType::StreamingSigned);
    }

    #[test]
    fn test_get_request_auth_type_streaming_signed_trailer() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static(DUMMY_SIGV4_AUTHORIZATION));
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
        headers.insert("authorization", HeaderValue::from_static(DUMMY_SIGV4_AUTHORIZATION));
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
        assert!(!constant_time_eq("Test", "test"), "first-byte mismatch must fail");
        assert!(!constant_time_eq("tesu", "test"), "last-byte mismatch must fail");
        assert!(!constant_time_eq("test", "test1"), "longer candidate must fail");
        assert!(!constant_time_eq("test1", "test"), "shorter candidate must fail");
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
    fn session_token_comparison_uses_constant_time_helper() {
        let source = include_str!("auth.rs");
        let production = source
            .split_once("\n#[cfg(test)]\nmod tests")
            .map_or(source, |(production, _)| production);
        let ordinary_comparison = ["token ", "!=", " cred.session_token"].concat();
        assert!(
            !production.contains(&ordinary_comparison),
            "temporary session tokens must not use ordinary string comparison"
        );
        assert!(production.contains("!constant_time_eq(token, &cred.session_token)"));
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

        // Case 3: X-Forwarded-For is ignored without verified proxy context
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 4: X-Forwarded-For with multiple IPs is ignored without verified proxy context
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.3, 10.0.0.4"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 5: X-Real-IP is ignored without verified proxy context
        headers.remove("x-forwarded-for");
        headers.insert("x-real-ip", HeaderValue::from_static("10.0.0.2"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 6: Forwarded is ignored without verified proxy context
        headers.remove("x-real-ip");
        headers.insert("forwarded", HeaderValue::from_static("for=10.0.0.5;proto=http"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 7: Forwarded with quotes and multiple values is ignored without verified proxy context
        headers.insert("forwarded", HeaderValue::from_static("for=\"10.0.0.6\", for=10.0.0.7"));
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "192.168.0.10");

        // Case 8: IPv6 Remote Addr
        let remote_addr_v6: std::net::SocketAddr = "[2001:db8::1]:8080".parse().unwrap();
        headers.clear();
        let conditions = get_condition_values(&headers, &cred, None, None, Some(remote_addr_v6));
        assert_eq!(conditions.get("SourceIp").unwrap()[0], "2001:db8::1");
    }

    #[test]
    fn test_get_condition_values_uses_verified_client_info() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        let cred = Credentials::default();
        let remote_addr: std::net::SocketAddr = "192.168.0.10:12345".parse().unwrap();
        let client_info = ClientInfo::from_trusted_proxy(
            "10.0.0.1".parse().unwrap(),
            None,
            Some("https".to_string()),
            "192.168.0.10".parse().unwrap(),
            1,
            ValidationMode::Lenient,
            Vec::new(),
        );

        let conditions =
            get_condition_values_with_client_info(&headers, &cred, None, None, Some(remote_addr), Some(&client_info));

        assert_eq!(conditions.get("SourceIp").unwrap()[0], "10.0.0.1");
        assert_eq!(conditions.get("SecureTransport").unwrap()[0], "true");
    }

    #[test]
    fn test_get_condition_values_ignores_unverified_secure_transport_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        let cred = Credentials::default();

        let conditions = get_condition_values(&headers, &cred, None, None, None);

        assert_eq!(conditions.get("SecureTransport").unwrap()[0], "false");
    }

    // ========== KEYSTONE AUTHENTICATION TESTS ==========

    #[tokio::test]
    async fn test_check_key_valid_keystone_not_enabled() {
        // Test that keystone: access key fails when Keystone is not enabled
        let result = check_key_valid("dummy-token", "keystone:user123").await;

        // Should fail with InvalidAccessKeyId because Keystone is not enabled
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::InvalidAccessKeyId);
    }

    #[tokio::test]
    async fn test_check_key_valid_keystone_no_credentials() {
        use rustfs_keystone::KEYSTONE_CREDENTIALS;

        // Test behavior when Keystone would be enabled but no credentials in task-local
        // This simulates a request that bypassed middleware
        KEYSTONE_CREDENTIALS
            .scope(None, async {
                // Call function that checks for keystone: prefix
                // In real scenario, would check is_keystone_enabled() first
                let access_key = "keystone:user123";
                if access_key.starts_with("keystone:") {
                    // Without credentials in task-local, this should fail
                    let creds_result = KEYSTONE_CREDENTIALS.try_with(|c: &Option<Credentials>| c.clone());
                    assert!(creds_result.is_ok()); // try_with succeeds
                    assert!(creds_result.unwrap().is_none()); // but value is None
                }
            })
            .await;
    }

    #[test]
    fn test_keystone_role_detection_admin() {
        // Test role detection logic for admin role
        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("roles".to_string(), json!(["admin", "member"]));

        let is_owner = claims
            .get("roles")
            .and_then(|roles| roles.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .any(|role| role.as_str().map(|r| r == "admin" || r == "reseller_admin").unwrap_or(false))
            })
            .unwrap_or(false);

        assert!(is_owner);
    }

    #[test]
    fn test_keystone_role_detection_reseller_admin() {
        // Test role detection logic for reseller_admin role
        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("roles".to_string(), json!(["reseller_admin"]));

        let is_owner = claims
            .get("roles")
            .and_then(|roles| roles.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .any(|role| role.as_str().map(|r| r == "admin" || r == "reseller_admin").unwrap_or(false))
            })
            .unwrap_or(false);

        assert!(is_owner);
    }

    #[test]
    fn test_keystone_role_detection_non_admin() {
        // Test role detection logic for non-admin roles
        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("roles".to_string(), json!(["member", "reader"]));

        let is_owner = claims
            .get("roles")
            .and_then(|roles| roles.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .any(|role| role.as_str().map(|r| r == "admin" || r == "reseller_admin").unwrap_or(false))
            })
            .unwrap_or(false);

        assert!(!is_owner);
    }

    #[test]
    fn test_keystone_role_detection_empty() {
        // Test role detection logic for empty roles
        let mut claims: HashMap<String, serde_json::Value> = HashMap::new();
        claims.insert("roles".to_string(), json!([]));

        let is_owner = claims
            .get("roles")
            .and_then(|roles| roles.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .any(|role| role.as_str().map(|r| r == "admin" || r == "reseller_admin").unwrap_or(false))
            })
            .unwrap_or(false);

        assert!(!is_owner);
    }

    #[test]
    fn test_keystone_role_detection_no_claim() {
        // Test role detection logic when roles claim is missing
        let claims: HashMap<String, serde_json::Value> = HashMap::new();

        let is_owner = claims
            .get("roles")
            .and_then(|roles| roles.as_array())
            .map(|roles| {
                roles
                    .iter()
                    .any(|role| role.as_str().map(|r| r == "admin" || r == "reseller_admin").unwrap_or(false))
            })
            .unwrap_or(false);

        assert!(!is_owner);
    }

    #[tokio::test]
    async fn test_keystone_task_local_storage() {
        use rustfs_keystone::KEYSTONE_CREDENTIALS;

        // Test that task-local storage properly stores and retrieves credentials
        let mut claims = HashMap::new();
        claims.insert("project_id".to_string(), json!("project123"));
        claims.insert("roles".to_string(), json!(["member"]));

        let test_creds = Credentials {
            access_key: "keystone:testuser".to_string(),
            secret_key: String::new(),
            session_token: String::new(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "testuser".to_string(),
            groups: None,
            claims: Some(claims),
            name: Some("Test User".to_string()),
            description: None,
        };

        // Outside scope, should fail
        let result = KEYSTONE_CREDENTIALS.try_with(|c: &Option<Credentials>| c.clone());
        assert!(result.is_err());

        // Inside scope, should succeed
        KEYSTONE_CREDENTIALS
            .scope(Some(test_creds.clone()), async {
                let result = KEYSTONE_CREDENTIALS.try_with(|c: &Option<Credentials>| c.clone());
                assert!(result.is_ok());
                let creds = result.unwrap();
                assert!(creds.is_some());
                assert_eq!(creds.unwrap().access_key, "keystone:testuser");
            })
            .await;

        // After scope, should fail again
        let result = KEYSTONE_CREDENTIALS.try_with(|c: &Option<Credentials>| c.clone());
        assert!(result.is_err());
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

    /// The failure this issue is about: when `remote_addr` is dropped the
    /// `aws:SourceIp` key never reaches the condition map, and `AddrFunc::evaluate`
    /// returns `false` for an absent key. That flips two policy shapes in
    /// opposite directions, and only one of them looks like a failure
    /// (rustfs/backlog#1885).
    #[tokio::test]
    async fn source_ip_policies_break_in_both_directions_when_the_key_is_missing() {
        let allow_from_office = |effect: &str| {
            format!(
                r#"{{
                    "Version": "2012-10-17",
                    "Statement": [
                        {{"Effect": "Allow", "Action": ["admin:ConfigUpdate"], "Resource": ["arn:aws:s3:::*"]}},
                        {{
                            "Effect": "{effect}",
                            "Action": ["admin:ConfigUpdate"],
                            "Resource": ["arn:aws:s3:::*"],
                            "Condition": {{"IpAddress": {{"aws:SourceIp": "192.168.1.0/24"}}}}
                        }}
                    ]
                }}"#
            )
        };

        let claims = HashMap::new();
        let groups = None;
        let mut with_ip = HashMap::new();
        with_ip.insert("SourceIp".to_string(), vec!["192.168.1.10".to_string()]);
        let without_ip: HashMap<String, Vec<String>> = HashMap::new();

        let args_with_ip = Args {
            account: "test-account",
            groups: &groups,
            action: Action::AdminAction(rustfs_policy::policy::action::AdminAction::ConfigUpdateAdminAction),
            bucket: "",
            conditions: &with_ip,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        let args_without_ip = Args {
            conditions: &without_ip,
            ..args_with_ip
        };

        // Deny + blacklist: the bypass shape. With the key present the deny
        // matches and the request is refused; drop the key and the deny stops
        // matching, so a source that policy means to block gets through.
        let deny_policy: Policy = serde_json::from_str(&allow_from_office("Deny")).expect("deny policy parses");
        assert!(
            !deny_policy.is_allowed(&args_with_ip).await,
            "a blacklisted source must be refused while aws:SourceIp is present"
        );
        assert!(
            deny_policy.is_allowed(&args_without_ip).await,
            "dropping remote_addr makes the Deny statement unreachable — this is the bypass"
        );

        // Allow + whitelist: the availability shape, and the only one an
        // operator would notice, which is why the bypass above went unseen.
        let allow_policy: Policy = serde_json::from_str(
            r#"{
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["admin:ConfigUpdate"],
                    "Resource": ["arn:aws:s3:::*"],
                    "Condition": {"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}
                }]
            }"#,
        )
        .expect("allow policy parses");
        assert!(
            allow_policy.is_allowed(&args_with_ip).await,
            "a whitelisted source must be allowed while aws:SourceIp is present"
        );
        assert!(
            !allow_policy.is_allowed(&args_without_ip).await,
            "dropping remote_addr locks out a legitimate admin"
        );
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

/// The condition-value derivation exactly as it was before `auth::facts`
/// replaced it, kept only as the oracle `auth::facts` is tested against
/// (rustfs/backlog#2734 task T1.10). Delete it once the facts tests stand on
/// their committed golden values alone.
#[cfg(test)]
pub(crate) mod legacy_condition_oracle {
    use super::{
        AuthType, constant_time_eq, current_action_credentials, current_ready_iam_handle, extract_string_list_claim,
        get_request_auth_type_with_query,
    };
    use http::HeaderMap;
    use rustfs_credentials::Credentials;
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_policy::policy::is_server_derived_condition_key;
    use rustfs_trusted_proxies::ClientInfo;
    use rustfs_utils::http::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    use url::form_urlencoded;

    fn policy_source_ip(remote_addr: Option<SocketAddr>, client_info: Option<&ClientInfo>) -> String {
        client_info
            .map(|info| info.real_ip.to_string())
            .or_else(|| remote_addr.map(|addr| addr.ip().to_string()))
            .unwrap_or_default()
    }

    fn policy_secure_transport(client_info: Option<&ClientInfo>) -> bool {
        client_info
            .and_then(|info| info.forwarded_proto.as_deref())
            .map(|proto| proto.eq_ignore_ascii_case("https"))
            .unwrap_or(false)
    }

    /// Get condition values for policy evaluation with optional query-string values
    /// and verified client information from trusted proxy middleware.
    pub(crate) fn get_condition_values_with_query_and_client_info(
        header: &HeaderMap,
        cred: &Credentials,
        version_id: Option<&str>,
        region: Option<rustfs_s3_types::Region>,
        remote_addr: Option<SocketAddr>,
        query: Option<&str>,
        client_info: Option<&ClientInfo>,
    ) -> HashMap<String, Vec<String>> {
        let username = if cred.is_temp() || cred.is_service_account() {
            cred.parent_user.clone()
        } else {
            cred.access_key.clone()
        };

        let sys_cred = current_action_credentials().unwrap_or_default();

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

        // Determine auth type and signature version from headers and query
        let (auth_type, signature_version) = determine_auth_type_and_version_with_query(header, query);

        let is_tls = policy_secure_transport(client_info);
        let source_ip = policy_source_ip(remote_addr, client_info);

        let mut args = HashMap::new();

        // Add basic time and security info
        args.insert("CurrentTime".to_owned(), vec![curr_time.format(&Rfc3339).unwrap_or_default()]);
        args.insert("EpochTime".to_owned(), vec![epoch_time.to_string()]);
        args.insert("SecureTransport".to_owned(), vec![is_tls.to_string()]);
        args.insert("SourceIp".to_owned(), vec![source_ip]);

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
            args.insert("versionid".to_owned(), vec![super::canonical_policy_version_id(vid)]);
        }

        // Add signature version and auth type
        if !signature_version.is_empty() {
            args.insert("signatureversion".to_owned(), vec![signature_version]);
        }
        if !auth_type.is_empty() {
            args.insert("authType".to_owned(), vec![auth_type]);
        }

        if let Some(lc) = region
            && !lc.as_str().is_empty()
        {
            args.insert("LocationConstraint".to_owned(), vec![lc.to_string()]);
        }

        let mut clone_header = header.clone();
        if let Some(v) = clone_header.get("x-amz-signature-age") {
            args.insert("signatureAge".to_string(), vec![v.to_str().unwrap_or("").to_string()]);
            clone_header.remove("x-amz-signature-age");
        }

        for obj_lock in &[
            AMZ_OBJECT_LOCK_MODE_LOWER,
            AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
            AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
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

        // S3 policy condition keys use "x-amz-grant-*" (policy key s3:x-amz-grant-* -> name() returns x-amz-grant-*)
        for grant_header in &[
            "x-amz-grant-full-control",
            "x-amz-grant-read",
            "x-amz-grant-write",
            "x-amz-grant-read-acp",
            "x-amz-grant-write-acp",
        ] {
            let values = clone_header
                .get_all(*grant_header)
                .iter()
                .map(|v| v.to_str().unwrap_or("").to_string())
                .collect::<Vec<String>>();
            if !values.is_empty() {
                args.insert((*grant_header).to_string(), values);
            }
            clone_header.remove(*grant_header);
        }

        // Claims and group membership are part of the verified identity, so they are
        // resolved before request headers are merged in below.
        if let Some(claims) = &cred.claims {
            for (k, v) in claims {
                if let Some(v_str) = v.as_str() {
                    args.insert(k.trim_start_matches("ldap").to_lowercase(), vec![v_str.to_string()]);
                }
            }

            if rustfs_iam::sys::is_rustfs_oidc_claims(claims) {
                args.remove("iss");
                if let Some(issuer) = claims
                    .get(rustfs_iam::sys::OIDC_ISSUER_CLAIM)
                    .and_then(Value::as_str)
                    .filter(|issuer| !issuer.is_empty())
                {
                    args.insert("iss".to_string(), vec![issuer.to_string()]);
                }
            }

            let grps = extract_string_list_claim(claims, "groups");
            if !grps.is_empty() {
                args.insert("groups".to_string(), grps);
            }

            let roles = extract_string_list_claim(claims, "roles");
            if !roles.is_empty() {
                args.insert("roles".to_string(), roles);
            }
        }

        let effective_user = if (cred.is_service_account() || cred.is_temp()) && !cred.parent_user.is_empty() {
            &cred.parent_user
        } else {
            &cred.access_key
        };
        let live_groups = current_ready_iam_handle()
            .map(|iam| iam.user_group_memberships(effective_user))
            .unwrap_or_default();
        for group in cred.groups.iter().flatten().chain(live_groups.iter()) {
            let groups = args.entry("groups".to_string()).or_default();
            if !groups.contains(group) {
                groups.push(group.clone());
            }
        }

        // Every remaining header is attacker-controlled. A header must never contribute
        // to a condition key that describes the caller's own identity or the connection,
        // otherwise sending `userid: admin` (or any `jwt:`/`ldap:` claim name) would let a
        // request satisfy a policy condition about itself. Reject those names outright --
        // both the ones already populated above and the well-known identity keys that are
        // absent for this credential, since an absent key is exactly what a spoofed header
        // would fill in.
        for key in clone_header.keys() {
            if key.as_str().eq_ignore_ascii_case("x-amz-tagging") {
                continue;
            }
            if is_reserved_condition_key(key.as_str(), &args) {
                continue;
            }
            args.insert(
                key.as_str().to_string(),
                header
                    .get_all(key)
                    .iter()
                    .map(|v| v.to_str().unwrap_or("").to_string())
                    .collect(),
            );
        }

        args
    }

    /// Whether a request header is forbidden from contributing to policy condition key
    /// `key`, either because the server already derived that key from verified state or
    /// because it is a well-known identity/context key that only the server may populate.
    fn is_reserved_condition_key(key: &str, server_derived: &HashMap<String, Vec<String>>) -> bool {
        server_derived.contains_key(key)
            || [
                AMZ_OBJECT_LOCK_MODE_LOWER,
                AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
                AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
            ]
            .iter()
            .any(|header| key.eq_ignore_ascii_case(header.trim_start_matches("x-amz-")))
            || is_server_derived_condition_key(key)
    }

    fn determine_auth_type_and_version_with_query(header: &HeaderMap, query: Option<&str>) -> (String, String) {
        match get_request_auth_type_with_query(header, query) {
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

    /// `merge_list_bucket_query_conditions` as `storage/access.rs` had it.
    pub(crate) fn merge_list_bucket_query_conditions(
        action: Action,
        query: Option<&str>,
        conditions: &mut HashMap<String, Vec<String>>,
    ) -> Result<(), String> {
        if !matches!(
            action,
            Action::S3Action(
                S3Action::ListBucketAction | S3Action::ListBucketVersionsAction | S3Action::ListBucketMultipartUploadsAction
            )
        ) {
            return Ok(());
        }
        for key in ["prefix", "delimiter", "max-keys"] {
            conditions.remove(key);
        }
        let Some(query) = query else {
            return Ok(());
        };

        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            if matches!(key.as_ref(), "prefix" | "delimiter" | "max-keys")
                && conditions.insert(key.to_string(), vec![value.into_owned()]).is_some()
            {
                return Err(format!("duplicate query: {key}"));
            }
        }
        Ok(())
    }
}
