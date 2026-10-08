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

//! The request facts policy conditions are derived from, and the one function
//! that derives them (rustfs/backlog#2734 task T1.10).
//!
//! [`AuthzFacts`] holds every request fact a condition key reads, already
//! resolved: the client address, whether the transport was secure, how the
//! request was authenticated, its headers and its listing query. Each edge
//! builds one — the legacy s3s edge from a [`RequestEnvelope`], the gateway
//! authorizer from its own request context — and both hand it to
//! [`build_conditions`], so a policy cannot be judged differently depending on
//! which edge accepted the request.
//!
//! [`build_conditions`] and [`build_args`] are pure: no I/O, no clock and no
//! process-wide state. What the condition values need from the environment —
//! the time, the root access key, whether a temporary credential has expired —
//! is resolved by the constructors and passed in.
//!
//! The facts hold no secret: the request headers that carry a credential or key
//! material are dropped when the facts are built, no query parameter other than
//! the listing keys is copied, and `Debug` prints names and counts only.

use crate::app::RequestEnvelope;
use crate::auth::{AuthType, constant_time_eq, extract_string_list_claim, get_request_auth_type_with_query};
use http::{HeaderMap, HeaderName};
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, is_server_derived_condition_key};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::{AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::form_urlencoded;

#[cfg(test)]
mod tests;

/// Request headers whose values authenticate the caller or unlock object data.
///
/// [`AuthzFacts`] never holds them. The legacy edge still copies them into the
/// condition values through [`forward_secret_headers`].
const SECRET_HEADERS: [&str; 4] = [
    "authorization",
    "x-amz-security-token",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key",
];

/// The query parameters a condition key reads: `s3:prefix`, `s3:delimiter` and
/// `s3:max-keys`, matched case-sensitively.
const LIST_QUERY_KEYS: [&str; 3] = ["prefix", "delimiter", "max-keys"];

const SIGNATURE_AGE_HEADER: &str = "x-amz-signature-age";

const OBJECT_LOCK_HEADERS: [&str; 3] = [
    AMZ_OBJECT_LOCK_MODE_LOWER,
    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
];

/// The `s3:x-amz-grant-*` condition keys keep the header name.
const GRANT_HEADERS: [&str; 5] = [
    "x-amz-grant-full-control",
    "x-amz-grant-read",
    "x-amz-grant-write",
    "x-amz-grant-read-acp",
    "x-amz-grant-write-acp",
];

/// Request tags are matched through `s3:RequestObjectTag/<key>` instead.
const TAGGING_HEADER: &str = "x-amz-tagging";

/// What a request asks to do, as its caller resolved it.
pub(crate) struct AuthzTarget<'a> {
    /// `Action::None` for a caller that only derives condition values.
    pub(crate) action: Action,
    pub(crate) bucket: &'a str,
    pub(crate) object: &'a str,
    pub(crate) version_id: Option<&'a str>,
    /// The `s3:LocationConstraint` value, when the caller supplies one.
    pub(crate) location_constraint: Option<&'a str>,
}

/// The `s3:authType` value: where the request carried its authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthTypeCondition {
    Jwt,
    RestHeader,
    RestQueryString,
    Post,
    Sts,
    Anonymous,
}

impl AuthTypeCondition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Jwt => "JWT",
            Self::RestHeader => "REST-HEADER",
            Self::RestQueryString => "REST-QUERY-STRING",
            Self::Post => "POST",
            Self::Sts => "STS",
            Self::Anonymous => "Anonymous",
        }
    }
}

/// The `s3:signatureversion` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignatureVersionCondition {
    V2,
    V4,
}

impl SignatureVersionCondition {
    fn as_str(self) -> &'static str {
        match self {
            Self::V2 => "AWS2",
            Self::V4 => "AWS4-HMAC-SHA256",
        }
    }
}

/// The `s3:authType` and `s3:signatureversion` values of a request the legacy
/// edge classified; `None` is a key the condition values leave out.
fn scheme_conditions(auth_type: &AuthType) -> (Option<AuthTypeCondition>, Option<SignatureVersionCondition>) {
    use AuthTypeCondition::{Anonymous, Jwt, Post, RestHeader, RestQueryString, Sts};
    use SignatureVersionCondition::{V2, V4};
    match auth_type {
        AuthType::JWT => (Some(Jwt), None),
        AuthType::SignedV2 => (Some(RestHeader), Some(V2)),
        AuthType::PresignedV2 => (Some(RestQueryString), Some(V2)),
        AuthType::StreamingSigned | AuthType::StreamingSignedTrailer | AuthType::StreamingUnsignedTrailer | AuthType::Signed => {
            (Some(RestHeader), Some(V4))
        }
        AuthType::Presigned => (Some(RestQueryString), Some(V4)),
        AuthType::PostPolicy => (Some(Post), None),
        AuthType::STS => (Some(Sts), None),
        AuthType::Anonymous => (Some(Anonymous), None),
        AuthType::Unknown => (None, None),
    }
}

/// Every request fact a policy condition key is derived from.
///
/// No field holds a secret: see the module documentation. There is no
/// `PartialEq`, and `Debug` prints header and query names, never their values.
#[derive(Clone)]
pub(crate) struct AuthzFacts {
    action: Action,
    bucket: String,
    object: String,
    version_id: Option<String>,
    location_constraint: Option<String>,
    source_ip: Option<IpAddr>,
    transport_secure: bool,
    auth_type: Option<AuthTypeCondition>,
    signature_version: Option<SignatureVersionCondition>,
    /// The request headers without [`SECRET_HEADERS`].
    headers: HeaderMap,
    /// The decoded [`LIST_QUERY_KEYS`] pairs, in request order.
    list_query: Vec<(String, String)>,
    now: OffsetDateTime,
}

impl AuthzFacts {
    /// The facts of a request the legacy edge decoded into an envelope.
    pub(crate) fn from_envelope(envelope: &RequestEnvelope, target: AuthzTarget<'_>, now: OffsetDateTime) -> Self {
        Self::from_request_parts(
            envelope.headers(),
            envelope.uri().query(),
            envelope.remote_addr(),
            envelope.client_info(),
            target,
            now,
        )
    }

    /// The facts of a request the legacy edge received, resolved by its rules.
    ///
    /// The client address is the one a trusted proxy attested, else the socket
    /// peer's. The transport is secure only when a trusted proxy reported
    /// `https`. Neither reads a forwarding header itself. The authentication
    /// scheme is classified from the headers and the query.
    pub(crate) fn from_request_parts(
        headers: &HeaderMap,
        raw_query: Option<&str>,
        remote_addr: Option<SocketAddr>,
        client_info: Option<&ClientInfo>,
        target: AuthzTarget<'_>,
        now: OffsetDateTime,
    ) -> Self {
        let source_ip = client_info
            .map(|info| info.real_ip)
            .or_else(|| remote_addr.map(|addr| addr.ip()));
        let transport_secure = client_info
            .and_then(|info| info.forwarded_proto.as_deref())
            .is_some_and(|proto| proto.eq_ignore_ascii_case("https"));
        let (auth_type, signature_version) = scheme_conditions(&get_request_auth_type_with_query(headers, raw_query));
        let mut headers = headers.clone();
        for name in SECRET_HEADERS {
            headers.remove(name);
        }
        Self {
            action: target.action,
            bucket: target.bucket.to_owned(),
            object: target.object.to_owned(),
            version_id: target.version_id.map(str::to_owned),
            location_constraint: target.location_constraint.map(str::to_owned),
            source_ip,
            transport_secure,
            auth_type,
            signature_version,
            headers,
            list_query: list_query_pairs(raw_query),
            now,
        }
    }

    pub(crate) fn action(&self) -> Action {
        self.action
    }
}

impl fmt::Debug for AuthzFacts {
    /// Names and counts only: a header or query value can be a credential.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self.headers.keys().map(HeaderName::as_str).collect();
        let query_keys: Vec<&str> = self.list_query.iter().map(|(key, _)| key.as_str()).collect();
        f.debug_struct("AuthzFacts")
            .field("action", &self.action)
            .field("bucket", &self.bucket)
            .field("object", &self.object)
            .field("has_version_id", &self.version_id.is_some())
            .field("has_location_constraint", &self.location_constraint.is_some())
            .field("source_ip", &self.source_ip)
            .field("transport_secure", &self.transport_secure)
            .field("auth_type", &self.auth_type)
            .field("signature_version", &self.signature_version)
            .field("now", &self.now)
            .field("header_count", &self.headers.len())
            .field("header_names", &header_names)
            .field("query_pair_count", &self.list_query.len())
            .field("query_keys", &query_keys)
            .finish()
    }
}

/// The decoded listing-key pairs of a raw query string, in request order.
pub(crate) fn list_query_pairs(raw_query: Option<&str>) -> Vec<(String, String)> {
    let Some(raw_query) = raw_query else {
        return Vec::new();
    };
    form_urlencoded::parse(raw_query.as_bytes())
        .filter(|(key, _)| LIST_QUERY_KEYS.contains(&key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

/// Adds the `s3:prefix`, `s3:delimiter` and `s3:max-keys` values of a listing
/// request; any other action gets none of them.
pub(crate) fn merge_list_query_conditions(
    action: Action,
    list_query: &[(String, String)],
    conditions: &mut HashMap<String, Vec<String>>,
) {
    if !matches!(
        action,
        Action::S3Action(
            S3Action::ListBucketAction | S3Action::ListBucketVersionsAction | S3Action::ListBucketMultipartUploadsAction
        )
    ) {
        return;
    }
    for (key, value) in list_query {
        conditions.entry(key.clone()).or_default().push(value.clone());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrincipalType {
    Anonymous,
    Account,
    User,
    AssumedRole,
}

impl PrincipalType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "Anonymous",
            Self::Account => "Account",
            Self::User => "User",
            Self::AssumedRole => "AssumedRole",
        }
    }
}

/// Who a request runs as, as condition values and policy arguments see it.
///
/// Borrowed from the authenticated credential, without its secret key or its
/// session token. It has no `Debug`.
pub(crate) struct AuthzPrincipal<'a> {
    account: &'a str,
    username: &'a str,
    principal_type: PrincipalType,
    groups: &'a Option<Vec<String>>,
    claims: Option<&'a HashMap<String, Value>>,
    is_owner: bool,
}

impl<'a> AuthzPrincipal<'a> {
    /// The principal `credentials` names; `Credentials::default()` is the
    /// anonymous one.
    ///
    /// `root_access_key` is the deployment's root access key, empty when it is
    /// not known. A principal acting as that user is the `Account`.
    pub(crate) fn new(credentials: &'a Credentials, root_access_key: &str) -> Self {
        // Temporary and service-account credentials act for their parent user.
        // `is_temp` reads the clock, which is why this is resolved here.
        let username = if credentials.is_temp() || credentials.is_service_account() {
            credentials.parent_user.as_str()
        } else {
            credentials.access_key.as_str()
        };
        let principal_type = if username.is_empty() {
            PrincipalType::Anonymous
        } else if credentials.claims.is_some() {
            PrincipalType::AssumedRole
        } else if constant_time_eq(root_access_key, username) {
            PrincipalType::Account
        } else {
            PrincipalType::User
        };
        Self {
            account: &credentials.access_key,
            username,
            principal_type,
            groups: &credentials.groups,
            claims: credentials.claims.as_ref(),
            is_owner: false,
        }
    }

    /// Whether the principal owns the deployment (root, or acting for root).
    /// Only [`build_args`] reads it.
    pub(crate) fn with_owner(mut self, is_owner: bool) -> Self {
        self.is_owner = is_owner;
        self
    }

    pub(crate) fn claims(&self) -> Option<&'a HashMap<String, Value>> {
        self.claims
    }
}

static NO_CLAIMS: LazyLock<HashMap<String, Value>> = LazyLock::new(HashMap::new);

/// The condition values of `facts` for `principal`, without the listing keys.
///
/// The internal table-catalog authorization uses this directly; every other
/// caller wants [`build_conditions`].
pub(crate) fn base_conditions(principal: &AuthzPrincipal<'_>, facts: &AuthzFacts) -> HashMap<String, Vec<String>> {
    let headers = &facts.headers;
    let mut args = HashMap::new();

    args.insert("CurrentTime".to_owned(), vec![facts.now.format(&Rfc3339).unwrap_or_default()]);
    args.insert("EpochTime".to_owned(), vec![facts.now.unix_timestamp().to_string()]);
    args.insert("SecureTransport".to_owned(), vec![facts.transport_secure.to_string()]);
    // An unknown client address is the empty string, not an absent key.
    args.insert("SourceIp".to_owned(), vec![facts.source_ip.map(|ip| ip.to_string()).unwrap_or_default()]);

    if let Some(user_agent) = headers.get("user-agent") {
        args.insert("UserAgent".to_owned(), vec![user_agent.to_str().unwrap_or("").to_owned()]);
    }
    if let Some(referer) = headers.get("referer") {
        args.insert("Referer".to_owned(), vec![referer.to_str().unwrap_or("").to_owned()]);
    }

    args.insert("userid".to_owned(), vec![principal.username.to_owned()]);
    args.insert("username".to_owned(), vec![principal.username.to_owned()]);
    args.insert("principaltype".to_owned(), vec![principal.principal_type.as_str().to_owned()]);

    if let Some(version_id) = facts.version_id.as_deref()
        && !version_id.is_empty()
    {
        args.insert("versionid".to_owned(), vec![version_id.to_owned()]);
    }
    if let Some(signature_version) = facts.signature_version {
        args.insert("signatureversion".to_owned(), vec![signature_version.as_str().to_owned()]);
    }
    if let Some(auth_type) = facts.auth_type {
        args.insert("authType".to_owned(), vec![auth_type.as_str().to_owned()]);
    }
    if let Some(location_constraint) = facts.location_constraint.as_deref()
        && !location_constraint.is_empty()
    {
        args.insert("LocationConstraint".to_owned(), vec![location_constraint.to_owned()]);
    }

    if let Some(signature_age) = headers.get(SIGNATURE_AGE_HEADER) {
        args.insert("signatureAge".to_owned(), vec![signature_age.to_str().unwrap_or("").to_owned()]);
    }
    for name in OBJECT_LOCK_HEADERS {
        let values = header_values(headers, name);
        if !values.is_empty() {
            args.insert(name.trim_start_matches("x-amz-").to_owned(), values);
        }
    }
    for name in GRANT_HEADERS {
        let values = header_values(headers, name);
        if !values.is_empty() {
            args.insert(name.to_owned(), values);
        }
    }

    // Claims and group membership are part of the verified identity, so they are
    // resolved before request headers are merged in below.
    if let Some(claims) = principal.claims {
        for (name, value) in claims {
            if let Some(value) = value.as_str() {
                args.insert(name.trim_start_matches("ldap").to_lowercase(), vec![value.to_owned()]);
            }
        }
        let groups = extract_string_list_claim(claims, "groups");
        if !groups.is_empty() {
            args.insert("groups".to_owned(), groups);
        }
        let roles = extract_string_list_claim(claims, "roles");
        if !roles.is_empty() {
            args.insert("roles".to_owned(), roles);
        }
    }
    if let Some(groups) = principal.groups
        && !args.contains_key("groups")
    {
        args.insert("groups".to_owned(), groups.clone());
    }

    // Every remaining header is attacker-controlled. A header must never contribute
    // to a condition key that describes the caller's own identity or the connection,
    // otherwise sending `userid: admin` (or any `jwt:`/`ldap:` claim name) would let a
    // request satisfy a policy condition about itself.
    for name in headers.keys() {
        let name = name.as_str();
        if name == SIGNATURE_AGE_HEADER
            || OBJECT_LOCK_HEADERS.contains(&name)
            || GRANT_HEADERS.contains(&name)
            || name.eq_ignore_ascii_case(TAGGING_HEADER)
            || is_reserved_condition_key(name, &args)
        {
            continue;
        }
        args.insert(name.to_owned(), header_values(headers, name));
    }

    args
}

/// The condition values of `facts` for `principal`: [`base_conditions`] and,
/// for a listing request, its `s3:prefix`, `s3:delimiter` and `s3:max-keys`.
pub(crate) fn build_conditions(principal: &AuthzPrincipal<'_>, facts: &AuthzFacts) -> HashMap<String, Vec<String>> {
    let mut conditions = base_conditions(principal, facts);
    merge_list_query_conditions(facts.action, &facts.list_query, &mut conditions);
    conditions
}

/// The policy arguments for `principal` asking `facts`, judged against `conditions`.
pub(crate) fn build_args<'a>(
    principal: &AuthzPrincipal<'a>,
    facts: &'a AuthzFacts,
    conditions: &'a HashMap<String, Vec<String>>,
) -> Args<'a> {
    Args {
        account: principal.account,
        groups: principal.groups,
        action: facts.action,
        bucket: &facts.bucket,
        conditions,
        is_owner: principal.is_owner,
        object: &facts.object,
        claims: principal.claims.unwrap_or(&NO_CLAIMS),
        deny_only: false,
    }
}

/// Copies the [`SECRET_HEADERS`] of `headers` into `conditions` under their own
/// names, as the legacy condition values always did.
///
/// No policy condition key names these headers, but the whole condition map is
/// part of the OPA input, so dropping them is a behaviour change of its own.
/// Until that change is made, this keeps the legacy edge's condition values
/// byte-identical while [`AuthzFacts`] stays free of secrets.
pub(crate) fn forward_secret_headers(headers: &HeaderMap, conditions: &mut HashMap<String, Vec<String>>) {
    // RUSTFS_COMPAT_TODO(authz-secret-header-conditions): the legacy condition map, and so the OPA input, carries these header values. Remove after a reviewed change stops putting credentials and customer keys into policy input.
    for name in SECRET_HEADERS {
        let values = header_values(headers, name);
        if values.is_empty() || is_reserved_condition_key(name, conditions) {
            continue;
        }
        conditions.insert(name.to_owned(), values);
    }
}

/// Every value of header `name`, in request order; a value that is not visible
/// ASCII becomes the empty string.
fn header_values(headers: &HeaderMap, name: &str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .map(|value| value.to_str().unwrap_or("").to_owned())
        .collect()
}

/// Whether a request header is forbidden from contributing to policy condition key
/// `key`, either because the server already derived that key from verified state or
/// because it is a well-known identity/context key that only the server may populate.
fn is_reserved_condition_key(key: &str, server_derived: &HashMap<String, Vec<String>>) -> bool {
    server_derived.contains_key(key)
        || OBJECT_LOCK_HEADERS
            .iter()
            .any(|header| key.eq_ignore_ascii_case(header.trim_start_matches("x-amz-")))
        || is_server_derived_condition_key(key)
}
