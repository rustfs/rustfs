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

use crate::admin::operation::AdminRequest;
use crate::admin::runtime_sources::AppContext;
use crate::app::{RequestCredentials, RequestEnvelope};
use crate::auth::get_condition_values_with_client_info;
use http::HeaderMap;
use rustfs_credentials::Credentials;
use rustfs_iam::store::Store;
use rustfs_iam::sys::IamSys;
use rustfs_policy::policy::{
    Args,
    action::{Action, AdminAction},
};
use rustfs_s3_types::{S3Result, s3_error};
use rustfs_trusted_proxies::ClientInfo;
use std::sync::Arc;
use tracing::debug;

#[derive(Clone)]
struct AuthContext<'a> {
    headers: &'a HeaderMap,
    cred: &'a Credentials,
    is_owner: bool,
    deny_only: bool,
    remote_addr: Option<std::net::SocketAddr>,
    client_info: Option<&'a ClientInfo>,
}

/// Network and optional per-server context facts for an admin authorization check.
///
/// Converting `&RequestEnvelope` via `From` threads only the verified proxy
/// [`ClientInfo`] and policy source address (`aws:SourceIp` and
/// `aws:SecureTransport`) while keeping IAM resolution ambient so ambient-store
/// admin handlers never split-brain auth/authz against a different server
/// domain. Handlers whose authentication, authorization, and execution all
/// resolve from the request's `ServerContextSlot` opt in explicitly via
/// [`Self::with_server_context`]. Callers outside `admin` that hold only a
/// resolved `Option<SocketAddr>` can keep passing it directly.
#[derive(Clone, Copy)]
pub struct AdminRequestOrigin<'a> {
    remote_addr: Option<std::net::SocketAddr>,
    client_info: Option<&'a ClientInfo>,
    server_context_envelope: Option<&'a RequestEnvelope>,
}

impl<'a> AdminRequestOrigin<'a> {
    /// Thread both verified proxy [`ClientInfo`] and the request's
    /// `ServerContextSlot` for handlers that authenticate and execute against
    /// the same request-scoped server context.
    pub(crate) fn with_server_context(envelope: &'a RequestEnvelope) -> Self {
        Self {
            remote_addr: envelope.policy_source_addr(),
            client_info: envelope.client_info(),
            server_context_envelope: Some(envelope),
        }
    }
}

impl From<Option<std::net::SocketAddr>> for AdminRequestOrigin<'_> {
    fn from(remote_addr: Option<std::net::SocketAddr>) -> Self {
        Self {
            remote_addr,
            client_info: None,
            server_context_envelope: None,
        }
    }
}

impl<'a> From<&'a RequestEnvelope> for AdminRequestOrigin<'a> {
    fn from(envelope: &'a RequestEnvelope) -> Self {
        Self {
            remote_addr: envelope.policy_source_addr(),
            client_info: envelope.client_info(),
            server_context_envelope: None,
        }
    }
}

pub(crate) fn ready_iam_handle_from_envelope(
    envelope: Option<&RequestEnvelope>,
) -> S3Result<Arc<IamSys<rustfs_iam::store::object::ObjectStore>>> {
    if let Some(slot) = envelope.and_then(RequestEnvelope::server_context) {
        let Some(context) = slot.installed_app_context() else {
            return Err(s3_error!(ServiceUnavailable, "server context is not ready"));
        };
        if !context.iam().is_ready() {
            return Err(s3_error!(InternalError, "iam not init"));
        }
        return Ok(context.iam().handle());
    }

    crate::admin::runtime_sources::current_ready_iam_handle().map_err(|_| s3_error!(InternalError, "iam not init"))
}

pub(crate) fn action_credentials_from_envelope(envelope: &RequestEnvelope) -> Option<Credentials> {
    match envelope.server_context() {
        Some(slot) => slot.installed_app_context().and_then(|ctx| ctx.action_credentials().get()),
        None => crate::admin::runtime_sources::current_action_credentials(),
    }
}

fn scoped_app_context_for_auth(envelope: &RequestEnvelope) -> S3Result<Option<Arc<AppContext>>> {
    match envelope.server_context() {
        Some(slot) => slot
            .installed_app_context()
            .map(Some)
            .ok_or_else(|| s3_error!(ServiceUnavailable, "server context is not ready")),
        None => Ok(None),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AdminResourceScope<'a> {
    bucket: &'a str,
    object: &'a str,
}

impl<'a> AdminResourceScope<'a> {
    pub fn bucket(bucket: &'a str) -> Self {
        Self { bucket, object: "" }
    }

    pub fn bucket_object(bucket: &'a str, object: &'a str) -> Self {
        Self { bucket, object }
    }

    /// Scope an admin request to a single KMS key.
    ///
    /// The policy crate carries the requested key identifier in the object slot
    /// with an empty bucket (see `Statement::kms_key_scope_matches`); `Args` has
    /// no dedicated field for it. An empty `key_id` means the caller could not
    /// name a target key, which keeps the pre-resource-scoping behaviour where a
    /// KMS statement matches every key.
    pub fn kms_key(key_id: &'a str) -> Self {
        Self {
            bucket: "",
            object: key_id,
        }
    }
}

pub async fn validate_admin_request<'a>(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    origin: impl Into<AdminRequestOrigin<'a>>,
) -> S3Result<()> {
    let origin = origin.into();
    let iam_store = ready_iam_handle_from_envelope(origin.server_context_envelope)?;
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr: origin.remote_addr,
        client_info: origin.client_info,
    };

    evaluate_admin_actions(iam_store, &ctx, &actions, "", "").await
}

pub(crate) enum SelfServiceMutation {
    Password,
    Mfa,
}

impl SelfServiceMutation {
    pub(crate) async fn check(&self, req: &AdminRequest, cred: &Credentials, is_owner: bool) -> S3Result<()> {
        let iam = match self {
            // Password self-service handlers (`account.rs`, `user.rs::AddUser`)
            // authenticate and mutate against the process-ambient IAM domain.
            Self::Password => crate::admin::runtime_sources::current_ready_iam_handle()
                .map_err(|_| s3_error!(ServiceUnavailable, "iam unavailable"))?,
            // MFA self-service handlers (`mfa.rs`) resolve auth, IAM, and store
            // strictly from the request envelope's `ServerContextSlot`.
            Self::Mfa => ready_iam_handle_from_envelope(Some(req.envelope())).map_err(|err| {
                if err.code() == &rustfs_s3_types::S3ErrorCode::ServiceUnavailable {
                    err
                } else {
                    s3_error!(ServiceUnavailable, "iam unavailable")
                }
            })?,
        };
        self.check_for_iam(iam, req, cred, is_owner).await
    }

    async fn check_for_iam<S: Store>(
        &self,
        iam: Arc<IamSys<S>>,
        req: &AdminRequest,
        cred: &Credentials,
        is_owner: bool,
    ) -> S3Result<()> {
        let ctx = AuthContext {
            headers: req.headers(),
            cred,
            // Derived credentials must pass through session policy evaluation.
            is_owner: is_owner && cred.session_token.is_empty() && !cred.is_service_account(),
            deny_only: true,
            remote_addr: req.policy_source_addr(),
            client_info: req.client_info(),
        };
        // Password rotation also honors the existing add-user self-rotation deny.
        // The ordinary multi-action admin gate is any-of; here every check must pass.
        let actions: &[AdminAction] = match self {
            Self::Password => &[AdminAction::ChangeOwnPasswordAdminAction, AdminAction::CreateUserAdminAction],
            Self::Mfa => &[AdminAction::ManageOwnMFAAdminAction],
        };
        for action in actions {
            check_admin_request_auth(iam.clone(), &ctx, Action::AdminAction(*action), "", "").await?;
        }
        Ok(())
    }
}

/// Return `Ok(())` as soon as any of the candidate `actions` is allowed for the
/// request context, otherwise `AccessDenied`. This is the single decision core
/// shared by every admin-gate entry point; unit tests exercise it directly so
/// the allow/deny contract does not silently drift.
async fn evaluate_admin_actions<S: Store>(
    iam_store: Arc<IamSys<S>>,
    ctx: &AuthContext<'_>,
    actions: &[Action],
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    for action in actions {
        if check_admin_request_auth(iam_store.clone(), ctx, *action, bucket, object)
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
    Err(s3_error!(AccessDenied, "Access Denied"))
}

async fn check_admin_request_auth<S: Store>(
    iam_store: Arc<IamSys<S>>,
    ctx: &AuthContext<'_>,
    action: Action,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    let conditions = get_condition_values_with_client_info(ctx.headers, ctx.cred, None, None, ctx.remote_addr, ctx.client_info);

    let allowed = iam_store
        .is_allowed(&Args {
            account: &ctx.cred.access_key,
            groups: &ctx.cred.groups,
            action,
            conditions: &conditions,
            is_owner: ctx.is_owner,
            claims: ctx.cred.claims_or_empty(),
            deny_only: ctx.deny_only,
            bucket,
            object,
        })
        .await;

    if !allowed {
        debug!(
            target = "rustfs::admin::auth",
            ?action,
            deny_only = ctx.deny_only,
            is_owner = ctx.is_owner,
            signer_access_key = %ctx.cred.access_key,
            "IAM is_allowed returned false for admin request (enable DEBUG for rustfs::admin::auth)"
        );
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }

    Ok(())
}

pub async fn validate_admin_request_with_bucket<'a>(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    origin: impl Into<AdminRequestOrigin<'a>>,
    bucket: &str,
) -> S3Result<()> {
    validate_admin_request_with_bucket_object(
        headers,
        cred,
        is_owner,
        deny_only,
        actions,
        origin,
        AdminResourceScope::bucket(bucket),
    )
    .await
}

pub async fn validate_admin_request_with_bucket_object<'a>(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    origin: impl Into<AdminRequestOrigin<'a>>,
    resource: AdminResourceScope<'_>,
) -> S3Result<()> {
    let origin = origin.into();
    let iam_store = ready_iam_handle_from_envelope(origin.server_context_envelope)?;
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr: origin.remote_addr,
        client_info: origin.client_info,
    };

    evaluate_admin_actions(iam_store, &ctx, &actions, resource.bucket, resource.object).await
}

pub(crate) async fn validate_admin_action_with_bucket_object_for_iam<'a, S: Store>(
    iam_store: Arc<IamSys<S>>,
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    action: Action,
    origin: impl Into<AdminRequestOrigin<'a>>,
    resource: AdminResourceScope<'_>,
) -> S3Result<()> {
    let origin = origin.into();
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only: false,
        remote_addr: origin.remote_addr,
        client_info: origin.client_info,
    };
    evaluate_admin_actions(iam_store, &ctx, &[action], resource.bucket, resource.object).await
}

/// Admin gate for KMS endpoints that act on one key.
///
/// `key_id` is the identifier as requested (before any alias resolution), so a
/// policy scoped to `arn:aws:kms:::key/<id>` only authorizes that key. Endpoints
/// without a target key pass `""` and stay unscoped, which is also what a
/// malformed request resolves to: the request is rejected on its own parse error
/// right after the gate, so no key is ever touched under an unscoped decision.
pub async fn validate_admin_request_with_kms_key<'a>(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    origin: impl Into<AdminRequestOrigin<'a>>,
    key_id: &str,
) -> S3Result<()> {
    validate_admin_request_with_bucket_object(
        headers,
        cred,
        is_owner,
        deny_only,
        actions,
        origin,
        AdminResourceScope::kms_key(key_id),
    )
    .await
}

/// Authenticate the caller of an owned admin request from the facts its
/// envelope carries: the presented access key and the session token the edge
/// decoded.
pub(crate) async fn authenticate_envelope_credentials(credentials: &RequestCredentials) -> S3Result<(Credentials, bool)> {
    authenticate_request_facts_with_context(credentials.access_key(), credentials.session_token().unwrap_or_default(), None).await
}

/// Authenticate the caller of an owned admin request against the request's
/// server context slot when present, falling back to the ambient process
/// context only when no slot was attached.
pub(crate) async fn authenticate_envelope_credentials_with_context(
    credentials: &RequestCredentials,
    envelope: &RequestEnvelope,
) -> S3Result<(Credentials, bool)> {
    let context = scoped_app_context_for_auth(envelope)?;
    authenticate_request_facts_with_context(
        credentials.access_key(),
        credentials.session_token().unwrap_or_default(),
        context.as_deref(),
    )
    .await
}

/// Validate an access key against an explicit application context while also
/// scoping IAM root-credential lookup to that context's own root credentials
/// so another embedded server's process-ambient root key cannot leak through
/// `IamSys::check_key` (rustfs/backlog#1469).
pub(crate) async fn check_admin_key_valid_with_context(
    session_token: &str,
    access_key: &str,
    ctx: Option<&AppContext>,
) -> S3Result<(Credentials, bool)> {
    use crate::auth::check_key_valid_with_context;

    match ctx {
        Some(context) => {
            rustfs_iam::sys::with_scoped_root_credentials(
                context.action_credentials().get(),
                check_key_valid_with_context(session_token, access_key, Some(context)),
            )
            .await
        }
        None => check_key_valid_with_context(session_token, access_key, None).await,
    }
}

async fn authenticate_request_facts_with_context(
    access_key: &str,
    session_token: &str,
    ctx: Option<&AppContext>,
) -> S3Result<(Credentials, bool)> {
    // Log authentication attempt for debugging
    debug!(
        "authenticate_request: processing authentication - access_key={}, has_session_token={}",
        access_key,
        !session_token.is_empty()
    );

    let result = check_admin_key_valid_with_context(session_token, access_key, ctx).await;

    match &result {
        Ok((cred, is_owner)) => {
            debug!(
                "authenticate_request: authentication successful - access_key={}, is_owner={}",
                cred.access_key, is_owner
            );
        }
        Err(e) => {
            tracing::warn!("authenticate_request: authentication failed - access_key={}, error={}", access_key, e);
        }
    }

    result
}

/// Full admin gate over RustFS-owned request facts: authenticate the
/// presented credentials, then authorize the caller for `actions`
/// ([`validate_admin_request`], allowing on the first permitted action) against
/// the process-ambient IAM domain while propagating verified proxy
/// `ClientInfo`. Returns the authenticated credentials for handlers that need
/// the caller identity. `deny_only` stays `false`: every caller performs a full
/// allow check.
pub async fn authorize_admin_envelope(req: &RequestEnvelope, actions: Vec<Action>) -> S3Result<Credentials> {
    authorize_admin_envelope_with_scope(req, actions, AdminResourceScope::bucket_object("", "")).await
}

/// Full admin gate over RustFS-owned request facts scoped to a bucket/object
/// resource against the process-ambient IAM domain (matching ambient-store
/// admin handlers).
pub(crate) async fn authorize_admin_envelope_with_scope(
    req: &RequestEnvelope,
    actions: Vec<Action>,
    resource: AdminResourceScope<'_>,
) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) = authenticate_envelope_credentials(input_cred).await?;
    // Policy conditions see the verified proxy client, never the raw peer.
    validate_admin_request_with_bucket_object(req.headers(), &cred, owner, false, actions, req, resource).await?;

    Ok(cred)
}

/// Full admin gate over RustFS-owned request facts scoped to both the request's
/// `ServerContextSlot` (when present) and a bucket/object resource. Use this
/// ONLY for handlers whose downstream execution also resolves from
/// `req.envelope()`.
pub(crate) async fn authorize_admin_envelope_with_context_and_scope(
    req: &RequestEnvelope,
    actions: Vec<Action>,
    resource: AdminResourceScope<'_>,
) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) = authenticate_envelope_credentials_with_context(input_cred, req).await?;
    // Policy conditions see the verified proxy client, never the raw peer.
    validate_admin_request_with_bucket_object(
        req.headers(),
        &cred,
        owner,
        false,
        actions,
        AdminRequestOrigin::with_server_context(req),
        resource,
    )
    .await?;

    Ok(cred)
}

/// Resolve an [`SseKmsPrincipal`](crate::admin::storage_api::sse::SseKmsPrincipal)
/// for an authenticated admin request envelope that serves object bytes.
///
/// Records the authenticated caller as a [`ReqInfo`](crate::admin::storage_api::access::ReqInfo)
/// on `req` — matching the S3 read boundary — and extracts the KMS principal from
/// it, failing closed with `AccessDenied` if the principal cannot be resolved so
/// `None` (the SSE layer's internal-caller sentinel) is never passed through.
pub(crate) async fn resolve_admin_object_read_sse_kms_principal(
    req: &mut RequestEnvelope,
) -> S3Result<crate::admin::storage_api::sse::SseKmsPrincipal> {
    use crate::admin::runtime_sources::current_region;
    use crate::admin::storage_api::access::ReqInfo;
    use crate::admin::storage_api::sse::{SseKmsPrincipal, SseKmsPrincipalExt as _};

    let Some(input_cred) = req.credentials() else {
        return Err(s3_error!(AccessDenied, "Access Denied"));
    };
    let (cred, is_owner) = authenticate_envelope_credentials_with_context(input_cred, req).await?;
    let request_context = req.request_context().cloned();
    req.install_req_info(ReqInfo {
        cred: Some(cred),
        is_owner,
        region: current_region(),
        request_context,
        ..Default::default()
    });
    SseKmsPrincipal::from_request(req).ok_or_else(|| s3_error!(AccessDenied, "Access Denied"))
}

#[cfg(test)]
mod tests {
    //! Unit coverage for the central admin authorization gate (rustfs/backlog#1151 sec-4).
    //!
    //! These tests pin the allow/deny contract of the gate decision core without a
    //! running cluster. The gate delegates the actual policy evaluation to
    //! `IamSys::is_allowed`; the behavior owned *here* is: owner/root short-circuits
    //! to allow, any other principal with no matching policy is denied with
    //! `AccessDenied`, and `evaluate_admin_actions` grants access as soon as any one
    //! candidate action is permitted. We drive that with an `IamSys` backed by an
    //! empty in-memory cache so `is_owner=true` returns allow (owner short-circuit)
    //! and every non-owner principal resolves to deny.
    use super::*;
    use http::Uri;
    use rustfs_iam::cache::Cache;
    use rustfs_iam::manager::IamCache;
    use rustfs_iam::store::{GroupInfo, MappedPolicy, UserType};
    use rustfs_policy::auth::UserIdentity;
    use rustfs_policy::policy::action::{AdminAction, KmsAction};
    use rustfs_policy::policy::{Policy, PolicyDoc};
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64};
    use time::OffsetDateTime;

    /// An in-memory test store with the small authoritative revision surface
    /// needed by derived-session authorization. All other persistence methods
    /// remain unreachable in these gate tests.
    #[derive(Clone, Default)]
    struct EmptyStore {
        regular_users: Arc<std::sync::RwLock<HashMap<String, (UserIdentity, String)>>>,
    }

    #[async_trait::async_trait]
    impl Store for EmptyStore {
        fn has_watcher(&self) -> bool {
            false
        }
        async fn regular_user_revision(&self, name: &str) -> rustfs_iam::error::Result<Option<(UserIdentity, String)>> {
            Ok(self.regular_users.read().expect("test store lock").get(name).cloned())
        }
        async fn save_iam_config<Item: Serialize + Send>(
            &self,
            _item: Item,
            _path: impl AsRef<str> + Send,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_iam_config<Item: DeserializeOwned>(
            &self,
            _path: impl AsRef<str> + Send,
        ) -> rustfs_iam::error::Result<Item> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn delete_iam_config(&self, _path: impl AsRef<str> + Send) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn save_user_identity(
            &self,
            _name: &str,
            _user_type: UserType,
            _item: UserIdentity,
            _ttl: Option<usize>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn delete_user_identity(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_user_identity(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<UserIdentity> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_user(
            &self,
            _name: &str,
            _user_type: UserType,
            _m: &mut HashMap<String, UserIdentity>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_users(
            &self,
            _user_type: UserType,
            _m: &mut HashMap<String, UserIdentity>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_secret_key(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<String> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn save_group_info(&self, _name: &str, _item: GroupInfo) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn delete_group_info(&self, _name: &str) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_group(&self, _name: &str, _m: &mut HashMap<String, GroupInfo>) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_groups(&self, _m: &mut HashMap<String, GroupInfo>) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn save_policy_doc(&self, _name: &str, _item: PolicyDoc) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn delete_policy_doc(&self, _name: &str) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_policy(&self, _name: &str) -> rustfs_iam::error::Result<PolicyDoc> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_policy_doc(&self, _name: &str, _m: &mut HashMap<String, PolicyDoc>) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_policy_docs(&self, _m: &mut HashMap<String, PolicyDoc>) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn save_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
            _item: MappedPolicy,
            _ttl: Option<usize>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn delete_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
            _m: &mut HashMap<String, MappedPolicy>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_mapped_policies(
            &self,
            _user_type: UserType,
            _is_group: bool,
            _m: &mut HashMap<String, MappedPolicy>,
        ) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
        async fn load_all(&self, _cache: &Cache) -> rustfs_iam::error::Result<()> {
            unimplemented!("EmptyStore is not exercised by the admin-auth gate tests")
        }
    }

    /// An `IamCache` over an empty in-memory snapshot. It is constructed
    /// directly (all fields are public, mirroring the iam crate's own
    /// `build_test_iam_cache`) so no cluster-backed `ObjectStore` or disk load is
    /// required. Readiness state is irrelevant: the gate calls `is_allowed`
    /// directly, which does not gate on `is_ready`.
    fn iam_cache_with_empty_store() -> IamCache<EmptyStore> {
        let (send_chan, _rx) = tokio::sync::mpsc::channel::<i64>(1);
        IamCache {
            cache: Cache::default(),
            api: EmptyStore::default(),
            state: Arc::new(AtomicU8::new(0)),
            loading: Arc::new(AtomicBool::new(false)),
            roles: HashMap::new(),
            send_chan,
            last_timestamp: AtomicI64::new(0),
            sync_failures: AtomicU64::new(0),
            sync_successes: AtomicU64::new(0),
            last_sync_duration_millis: AtomicU64::new(0),
        }
    }

    fn iam_sys_with_empty_store() -> Arc<IamSys<EmptyStore>> {
        Arc::new(IamSys::new(Arc::new(iam_cache_with_empty_store())))
    }

    /// Same in-memory `IamSys`, with one regular (non-owner, non-temp,
    /// non-service) account carrying `document` as its only attached policy. All
    /// three cache entries the evaluation path consults are pre-seeded, so no
    /// `EmptyStore` method is ever reached.
    fn iam_sys_with_policy(account: &str, document: &str) -> Arc<IamSys<EmptyStore>> {
        let store = iam_cache_with_empty_store();
        let now = OffsetDateTime::now_utc();
        let cache = &store.cache;

        cache.add_or_update_user(
            account,
            &UserIdentity::new(Credentials {
                access_key: account.to_string(),
                secret_key: "kms-operator-secret".to_string(),
                status: "on".to_string(),
                ..Default::default()
            }),
            now,
        );
        cache.add_or_update_policy_doc(
            KMS_TEST_POLICY_NAME,
            &PolicyDoc {
                policy: Policy::parse_config(document.as_bytes()).expect("test policy should parse"),
                ..Default::default()
            },
            now,
        );
        cache.add_or_update_user_policy(account, &MappedPolicy::new(KMS_TEST_POLICY_NAME), now);

        Arc::new(IamSys::new(Arc::new(store)))
    }

    const KMS_TEST_POLICY_NAME: &str = "kms-key-a-only";

    fn ctx_for<'a>(headers: &'a HeaderMap, cred: &'a Credentials, is_owner: bool) -> AuthContext<'a> {
        AuthContext {
            headers,
            cred,
            is_owner,
            deny_only: false,
            remote_addr: None,
            client_info: None,
        }
    }

    fn admin_action() -> Action {
        Action::AdminAction(AdminAction::ServerInfoAdminAction)
    }

    fn assert_access_denied(res: S3Result<()>) {
        let err = res.expect_err("gate should reject the request");
        assert_eq!(
            err.code(),
            &rustfs_s3_types::S3ErrorCode::AccessDenied,
            "admin gate rejection must map to the AccessDenied S3 error code"
        );
    }

    #[test]
    #[serial_test::serial]
    fn admin_and_sts_handlers_use_verified_client_ip_and_bucket_scope() {
        crate::app::gating_test_env::run_large_stack_test("admin-client-conditions", || async {
            use crate::admin::operation::Operation;
            use crate::admin::operation::test_support::TestRequest;
            use crate::admin::runtime_sources::{AppContext, IamInterface, KmsInterface, publish_test_app_context};
            use crate::shared_types::RemoteAddr;
            use rustfs_iam::store::object::ObjectStore;
            use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
            use std::sync::Arc;
            struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
            impl IamInterface for ReadyIam {
                fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                    self.0.clone()
                }
                fn is_ready(&self) -> bool {
                    true
                }
            }
            struct TestKms;
            impl KmsInterface for TestKms {
                fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
                    Arc::new(rustfs_kms::KmsServiceManager::new())
                }
            }
            let previous = crate::runtime_sources::current_app_context();
            let (_directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
            ObjectStore::new(store.clone())
                .save_iam_config(
                    serde_json::json!({"version": 1}),
                    format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
                )
                .await
                .unwrap();
            let iam = rustfs_iam::build_iam_sys(store.clone()).await.unwrap();
            for (key, document) in [
                (
                    "proxy-admin",
                    serde_json::json!({"Version":"2012-10-17","Statement":[
                        {"Effect":"Allow","Action":"admin:*"},
                        {"Effect":"Allow","Action":"sts:AssumeRole"},
                        {"Effect":"Deny","Action":"admin:*","Condition":{"NotIpAddress":{"aws:SourceIp":"10.0.0.0/8"}}},
                        {"Effect":"Deny","Action":"sts:AssumeRole","Condition":{"NotIpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}
                    ]}),
                ),
                (
                    "scoped-usage",
                    serde_json::json!({"Version":"2012-10-17","Statement":[
                        {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::usage-allowed"},
                        {"Effect":"Deny","Action":"s3:ListBucket","NotResource":"arn:aws:s3:::usage-allowed"}
                    ]}),
                ),
            ] {
                iam.create_user(
                    key,
                    &AddOrUpdateUserReq {
                        secret_key: "test-user-secret".to_owned(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .unwrap();
                iam.set_policy(key, Policy::parse_config(&serde_json::to_vec(&document).unwrap()).unwrap())
                    .await
                    .unwrap();
                iam.policy_db_set(key, UserType::Reg, false, key).await.unwrap();
            }
            let context = Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms)));
            assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
                access_key: "proxy-test-root".to_owned(),
                secret_key: "proxy-root-secret".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }));
            publish_test_app_context(context);
            let (root, is_owner) = authenticate_request_facts_with_context("proxy-test-root", "", None)
                .await
                .expect("root credentials");
            assert_eq!(root.access_key, "proxy-test-root");
            assert!(is_owner);

            let error = authenticate_request_facts_with_context("proxy-test-root", "unexpected-token", None)
                .await
                .expect_err("the root has no session token");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidRequest);
            assert_eq!(error.message(), Some("invalid token2"));
            assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
            assert!(error.source().is_none());
            let request = |key: &str, client_ip: Option<&str>, method: http::Method, uri: &str, body: &str| {
                let mut builder = TestRequest::new(method, uri)
                    .extension(Some(RemoteAddr("10.1.2.3:4567".parse().unwrap())))
                    .header("x-forwarded-for", "10.1.2.3")
                    .header("x-real-ip", "10.1.2.3")
                    .credentials(key, "test-user-secret", None)
                    .body(body.to_owned());
                if let Some(ip) = client_ip {
                    builder =
                        builder.extension(rustfs_trusted_proxies::ClientInfo::direct(format!("{ip}:7890").parse().unwrap()));
                }
                builder.build()
            };
            for (client, allowed) in [(Some("203.0.113.17"), false), (Some("10.8.7.6"), true), (None, true)] {
                let req = request("proxy-admin", client, http::Method::GET, "/", "");
                assert_eq!(req.policy_source_addr().unwrap().port(), 4567);
                let result = authorize_admin_envelope(req.envelope(), vec![admin_action()]).await;
                if allowed {
                    result.expect("verified private client is allowed");
                } else {
                    assert_eq!(result.unwrap_err().code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
                }
                let req = request(
                    "proxy-admin",
                    client,
                    http::Method::POST,
                    "/",
                    "Action=AssumeRole&Version=invalid-version",
                );
                let error = crate::admin::handlers::sts::AssumeRoleHandle {}
                    .call(req, matchit::Params::new())
                    .await
                    .unwrap_err();
                if allowed {
                    assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
                    assert_eq!(
                        error.message(),
                        Some("not support version"),
                        "only an authorized caller reaches version validation"
                    );
                } else {
                    assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
                }
                let response = crate::admin::handlers::is_admin::IsAdminHandler {}
                    .call(request("proxy-admin", client, http::Method::GET, "/", ""), matchit::Params::new())
                    .await
                    .unwrap();
                let (_status, _headers, mut body) = response.into_parts();
                let json: serde_json::Value = serde_json::from_slice(&body.store_all_limited(4096).await.unwrap()).unwrap();
                assert_eq!(json["is_admin"], allowed);
            }
            let mut routes = matchit::Router::new();
            routes.insert("/usage/{bucket}", ()).unwrap();
            for (bucket, allowed) in [("usage-allowed", true), ("usage-secret", false)] {
                let path = format!("/usage/{bucket}");
                let matched = routes.at(&path).unwrap();
                let req = request("scoped-usage", None, http::Method::GET, &format!("{path}?unexpected=1"), "");
                let error = crate::admin::handlers::usage_prefix::BucketPrefixUsageHandler {}
                    .call(req, matched.params)
                    .await
                    .unwrap_err();
                if allowed {
                    assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
                    assert_eq!(error.message(), Some("unknown query parameter: unexpected"));
                } else {
                    assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
                }
            }
            if let Some(previous) = previous {
                publish_test_app_context(previous);
            }
        });
    }

    /// Owner (root/admin) credential is authorized for an admin action.
    #[tokio::test]
    async fn admin_owner_credential_is_allowed() {
        let iam = iam_sys_with_empty_store();
        let headers = HeaderMap::new();
        let cred = Credentials {
            access_key: "rustfsadmin".to_string(),
            ..Default::default()
        };
        let ctx = ctx_for(&headers, &cred, true);

        let res = check_admin_request_auth(iam, &ctx, admin_action(), "", "").await;
        assert!(res.is_ok(), "owner credential must pass the admin auth gate");
    }

    /// A known-shaped but non-owner principal with no matching policy is denied.
    #[tokio::test]
    async fn non_admin_credential_is_denied() {
        let iam = iam_sys_with_empty_store();
        let headers = HeaderMap::new();
        let cred = Credentials {
            access_key: "limiteduser".to_string(),
            secret_key: "limitedsecret".to_string(),
            ..Default::default()
        };
        let ctx = ctx_for(&headers, &cred, false);

        let res = check_admin_request_auth(iam, &ctx, admin_action(), "", "").await;
        assert_access_denied(res);
    }

    /// A request carrying an empty (missing) access key is denied.
    #[tokio::test]
    async fn missing_credential_is_denied() {
        let iam = iam_sys_with_empty_store();
        let headers = HeaderMap::new();
        let cred = Credentials::default();
        let ctx = ctx_for(&headers, &cred, false);

        let res = check_admin_request_auth(iam, &ctx, admin_action(), "", "").await;
        assert_access_denied(res);
    }

    /// The shared admin gate rejects a request carrying no credentials before
    /// authentication or IAM is consulted, with the exact error the folded
    /// per-handler wrappers produced (rustfs/backlog#1829).
    #[tokio::test]
    async fn authorize_admin_envelope_without_credentials_is_rejected() {
        let envelope = crate::app::RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: http::Method::GET,
                uri: Uri::from_static("/rustfs/admin/v3/usage/bucket"),
                headers: HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            http::Extensions::new(),
        );

        let err = authorize_admin_envelope(&envelope, vec![admin_action()])
            .await
            .expect_err("an envelope without credentials must be rejected");
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("get cred failed"));
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert!(err.source().is_none());
    }

    /// KMS scoping rides the object slot with an empty bucket, matching the
    /// contract the policy crate evaluates KMS statements against.
    #[test]
    fn kms_scope_carries_the_key_id_in_the_object_slot() {
        let scope = AdminResourceScope::kms_key("key-a");
        assert_eq!(scope.bucket, "");
        assert_eq!(scope.object, "key-a");

        let unscoped = AdminResourceScope::kms_key("");
        assert_eq!(unscoped.object, "", "an absent key id must stay unscoped");
    }

    const KMS_OPERATOR: &str = "kms-operator";

    #[tokio::test]
    async fn self_service_mutation_checks_each_applicable_deny() {
        let cred = Credentials {
            access_key: "self-service-user".into(),
            ..Default::default()
        };
        let req = crate::admin::operation::test_support::TestRequest::new(http::Method::POST, "/").build();
        for (action, password_allowed, mfa_allowed) in [
            ("admin:CreateUser", false, true),
            ("admin:ChangeOwnPassword", false, true),
            ("admin:ManageOwnMFA", true, false),
            ("admin:*", false, false),
        ] {
            let document = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"admin:*","Resource":"*"}},{{"Effect":"Deny","Action":"{action}","Resource":"*"}}]}}"#
            );
            let iam = iam_sys_with_policy(&cred.access_key, &document);
            for (mutation, allowed) in [
                (SelfServiceMutation::Password, password_allowed),
                (SelfServiceMutation::Mfa, mfa_allowed),
            ] {
                let result = mutation.check_for_iam(iam.clone(), &req, &cred, false).await;
                if allowed {
                    result.expect("unrelated deny should not block self-service");
                } else {
                    assert_access_denied(result);
                }
            }
        }
    }

    #[tokio::test]
    async fn self_service_mutation_uses_request_conditions() {
        use crate::admin::operation::test_support::TestRequest;
        let cred = Credentials {
            access_key: "conditional-user".into(),
            ..Default::default()
        };
        let iam = iam_sys_with_policy(
            &cred.access_key,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"admin:ManageOwnMFA","Condition":{"StringEquals":{"aws:UserAgent":"restricted-client"}}}]}"#,
        );
        let request = |agent| TestRequest::new(http::Method::POST, "/").header("user-agent", agent).build();
        assert_access_denied(
            SelfServiceMutation::Mfa
                .check_for_iam(iam.clone(), &request("restricted-client"), &cred, false)
                .await,
        );
        SelfServiceMutation::Mfa
            .check_for_iam(iam, &request("other-client"), &cred, false)
            .await
            .expect("condition is not satisfied");
    }

    #[tokio::test]
    async fn self_service_mutation_defaults_and_session_boundary() {
        let store = iam_cache_with_empty_store();
        let now = OffsetDateTime::now_utc();
        let user = Credentials {
            access_key: "self-service-user".into(),
            secret_key: "user-secret".into(),
            status: "on".into(),
            ..Default::default()
        };
        store
            .cache
            .add_or_update_user(&user.access_key, &UserIdentity::new(user.clone()), now);
        store
            .api
            .regular_users
            .write()
            .expect("test store lock")
            .insert(user.access_key.clone(), (UserIdentity::new(user.clone()), "self-service".into()));
        store
            .cache
            .add_or_update_user_policy(&user.access_key, &MappedPolicy::new(""), now);
        let session = Credentials {
            access_key: "session-user".into(),
            parent_user: user.access_key.clone(),
            session_token: "session".into(),
            claims: Some(HashMap::from([(
                rustfs_iam::sys::SESSION_POLICY_NAME_EXTRACTED.to_string(),
                serde_json::json!(
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"admin:ManageOwnMFA","Resource":"*"}]}"#
                ),
            )])),
            ..user.clone()
        };
        store
            .cache
            .add_or_update_sts_account(&session.access_key, &UserIdentity::new(session.clone()), now);
        let iam = Arc::new(IamSys::new(Arc::new(store)));
        let req = crate::admin::operation::test_support::TestRequest::new(http::Method::POST, "/").build();
        for mutation in [SelfServiceMutation::Password, SelfServiceMutation::Mfa] {
            mutation
                .check_for_iam(iam.clone(), &req, &user, false)
                .await
                .expect("active user with no policies may manage their own credentials");
        }
        let ordinary = ctx_for(req.headers(), &user, false);
        assert_access_denied(
            check_admin_request_auth(iam.clone(), &ordinary, Action::AdminAction(AdminAction::CreateUserAdminAction), "", "")
                .await,
        );
        assert_access_denied(
            SelfServiceMutation::Mfa
                .check_for_iam(iam.clone(), &req, &session, true)
                .await,
        );
        SelfServiceMutation::Password
            .check_for_iam(iam.clone(), &req, &session, false)
            .await
            .expect("unrelated session deny permits password rotation");
        for (claim, value) in [
            (rustfs_iam::sys::SESSION_POLICY_NAME, serde_json::json!("invalid encoding")),
            (rustfs_iam::sys::PLUGIN_ATTEMPT_CREDENTIAL_CLAIM, serde_json::json!(true)),
        ] {
            let restricted = Credentials {
                claims: Some(HashMap::from([(claim.to_owned(), value)])),
                ..session.clone()
            };
            assert_access_denied(
                SelfServiceMutation::Password
                    .check_for_iam(iam.clone(), &req, &restricted, false)
                    .await,
            );
        }
    }

    fn kms_disable_key_actions() -> Vec<Action> {
        vec![Action::KmsAction(KmsAction::DisableKeyAction)]
    }

    async fn gate_for_key(iam: Arc<IamSys<EmptyStore>>, key_id: &str, is_owner: bool) -> S3Result<()> {
        let headers = HeaderMap::new();
        let cred = Credentials {
            access_key: KMS_OPERATOR.to_string(),
            secret_key: "kms-operator-secret".to_string(),
            status: "on".to_string(),
            ..Default::default()
        };
        let ctx = ctx_for(&headers, &cred, is_owner);
        let scope = AdminResourceScope::kms_key(key_id);

        evaluate_admin_actions(iam, &ctx, &kms_disable_key_actions(), scope.bucket, scope.object).await
    }

    /// End-to-end through the gate: a `kms:DisableKey` grant limited to `key/A`
    /// authorizes key A and denies key B with `AccessDenied`. This is the seam the
    /// per-handler tests cannot reach — it pins that the scope actually travels
    /// into the policy verdict rather than being dropped on the way
    /// (rustfs/backlog#1582).
    #[tokio::test]
    async fn kms_scoped_gate_denies_a_key_outside_the_policy_scope() {
        let document = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["kms:DisableKey"],"Resource":["arn:aws:kms:::key/key-a"]}
        ]}"#;

        let allowed = gate_for_key(iam_sys_with_policy(KMS_OPERATOR, document), "key-a", false).await;
        assert!(allowed.is_ok(), "the key the policy names must pass the gate");

        assert_access_denied(gate_for_key(iam_sys_with_policy(KMS_OPERATOR, document), "key-b", false).await);
    }

    /// Compatibility pins for the same gate: a resource-less KMS statement keeps
    /// authorizing every key, and the owner short-circuit is unaffected by scoping.
    #[tokio::test]
    async fn kms_scoped_gate_keeps_unscoped_policies_and_owners_unrestricted() {
        let unscoped = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["kms:DisableKey"]}
        ]}"#;
        let scoped = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["kms:DisableKey"],"Resource":["arn:aws:kms:::key/key-a"]}
        ]}"#;

        for key in ["key-a", "key-b"] {
            let res = gate_for_key(iam_sys_with_policy(KMS_OPERATOR, unscoped), key, false).await;
            assert!(res.is_ok(), "a resource-less KMS statement must keep authorizing {key}");

            let res = gate_for_key(iam_sys_with_policy(KMS_OPERATOR, scoped), key, true).await;
            assert!(res.is_ok(), "the owner short-circuit must still authorize {key}");
        }
    }

    /// An endpoint with no target key passes an empty scope, which must behave
    /// exactly like the unscoped gate it replaced.
    #[tokio::test]
    async fn kms_gate_without_a_target_key_matches_every_key() {
        let scoped = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["kms:DisableKey"],"Resource":["arn:aws:kms:::key/key-a"]}
        ]}"#;

        let res = gate_for_key(iam_sys_with_policy(KMS_OPERATOR, scoped), "", false).await;
        assert!(res.is_ok(), "an absent key id must keep the pre-resource-scoping verdict");
    }

    /// The multi-action loop authorizes as soon as one candidate action passes
    /// (owner short-circuits every action), and denies when none pass.
    #[tokio::test]
    async fn evaluate_admin_actions_grants_when_any_action_allowed() {
        let headers = HeaderMap::new();
        let actions = vec![
            Action::AdminAction(AdminAction::ServerInfoAdminAction),
            Action::AdminAction(AdminAction::ConfigUpdateAdminAction),
        ];

        // Owner: allowed on the first candidate action.
        let owner_cred = Credentials {
            access_key: "rustfsadmin".to_string(),
            ..Default::default()
        };
        let owner_ctx = ctx_for(&headers, &owner_cred, true);
        let iam = iam_sys_with_empty_store();
        let res = evaluate_admin_actions(iam, &owner_ctx, &actions, "", "").await;
        assert!(res.is_ok(), "owner must be granted when at least one action is permitted");

        // Non-owner with no policy: none of the candidate actions pass.
        let user_cred = Credentials {
            access_key: "limiteduser".to_string(),
            ..Default::default()
        };
        let user_ctx = ctx_for(&headers, &user_cred, false);
        let iam = iam_sys_with_empty_store();
        let res = evaluate_admin_actions(iam, &user_ctx, &actions, "", "").await;
        assert_access_denied(res);
    }

    #[tokio::test]
    async fn resolve_admin_object_read_sse_kms_principal_fails_closed_without_credentials() {
        let mut envelope = crate::app::RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: http::Method::GET,
                uri: Uri::from_static("/rustfs/admin/v3/inspect-data?volume=data&file=a.txt"),
                headers: HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            http::Extensions::new(),
        );

        let err = resolve_admin_object_read_sse_kms_principal(&mut envelope)
            .await
            .expect_err("unauthenticated envelope must fail closed");
        assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
    }

    #[test]
    #[serial_test::serial]
    fn admin_auth_evaluates_aws_secure_transport_from_verified_client_info() {
        crate::app::gating_test_env::run_large_stack_test("admin-secure-transport", || async {
            use crate::admin::operation::Operation;
            use crate::admin::operation::test_support::TestRequest;
            use crate::admin::runtime_sources::{AppContext, IamInterface, KmsInterface, publish_test_app_context};
            use crate::shared_types::RemoteAddr;
            use rustfs_iam::store::object::ObjectStore;
            use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
            use rustfs_trusted_proxies::{ClientInfo, ValidationMode};
            use std::sync::Arc;

            struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
            impl IamInterface for ReadyIam {
                fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                    Arc::clone(&self.0)
                }
                fn is_ready(&self) -> bool {
                    true
                }
            }
            struct TestKms;
            impl KmsInterface for TestKms {
                fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
                    Arc::new(rustfs_kms::KmsServiceManager::new())
                }
            }

            let previous = crate::runtime_sources::current_app_context();
            let (_dir, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
            ObjectStore::new(Arc::clone(&store))
                .save_iam_config(
                    serde_json::json!({"version": 1}),
                    format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
                )
                .await
                .expect("seed IAM format");
            let iam = rustfs_iam::build_iam_sys(Arc::clone(&store)).await.expect("init IAM");
            let user = "tls-only-admin";
            let secret = "tls-admin-secret-1234";
            iam.create_user(
                user,
                &AddOrUpdateUserReq {
                    secret_key: secret.to_owned(),
                    policy: None,
                    status: AccountStatus::Enabled,
                },
            )
            .await
            .expect("create user");
            let doc = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["admin:*"],
                        "Condition": {"Bool": {"aws:SecureTransport": "true"}}
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["sts:AssumeRole"]
                    },
                    {
                        "Effect": "Deny",
                        "Action": ["sts:AssumeRole"],
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}}
                    }
                ]
            });
            iam.set_policy(
                "tls-only",
                Policy::parse_config(&serde_json::to_vec(&doc).expect("encode policy")).expect("parse policy"),
            )
            .await
            .expect("store policy");
            iam.policy_db_set(user, UserType::Reg, false, "tls-only")
                .await
                .expect("attach policy");

            let context = Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms)));
            assert!(context.publish_action_credentials(Credentials {
                access_key: "tls-test-root".to_owned(),
                secret_key: "tls-root-secret".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }));
            publish_test_app_context(context);

            let proxy_peer: std::net::SocketAddr = "10.0.0.2:4430".parse().expect("proxy socket");
            let client_ip: std::net::IpAddr = "198.51.100.42".parse().expect("client ip");
            let build_req = |proto: Option<&str>, trusted: bool, method: http::Method, body: &str| {
                let mut builder = TestRequest::new(method, "/")
                    .extension(Some(RemoteAddr(proxy_peer)))
                    // Spoofed header must never satisfy aws:SecureTransport without verified ClientInfo.
                    .header("x-forwarded-proto", "https")
                    .credentials(user, secret, None)
                    .body(body.to_owned());
                if trusted {
                    builder = builder.extension(ClientInfo::from_trusted_proxy(
                        client_ip,
                        None,
                        proto.map(str::to_owned),
                        proxy_peer.ip(),
                        1,
                        ValidationMode::Strict,
                        Vec::new(),
                    ));
                } else if proto.is_some() {
                    builder = builder.extension(ClientInfo::direct(proxy_peer));
                }
                builder.build()
            };

            for (label, proto, trusted, allowed) in [
                ("trusted-https", Some("https"), true, true),
                ("trusted-http", Some("http"), true, false),
                ("trusted-missing-proto", None, true, false),
                ("spoofed-direct-header", Some("https"), false, false),
                ("spoofed-no-client-info", None, false, false),
            ] {
                let req = build_req(proto, trusted, http::Method::GET, "");
                let gate = authorize_admin_envelope(req.envelope(), vec![admin_action()]).await;
                if allowed {
                    gate.unwrap_or_else(|err| panic!("{label}: trusted https must satisfy aws:SecureTransport=true: {err}"));
                } else {
                    assert_access_denied(gate.map(|_| ()));
                }

                let is_admin_resp = crate::admin::handlers::is_admin::IsAdminHandler {}
                    .call(build_req(proto, trusted, http::Method::GET, ""), matchit::Params::new())
                    .await
                    .expect("is_admin responds 200");
                let (_status, _headers, mut body) = is_admin_resp.into_parts();
                let json: serde_json::Value =
                    serde_json::from_slice(&body.store_all_limited(4096).await.expect("read body")).expect("parse json");
                assert_eq!(json["is_admin"], allowed, "{label}");

                let sts_err = crate::admin::handlers::sts::AssumeRoleHandle {}
                    .call(
                        build_req(proto, trusted, http::Method::POST, "Action=AssumeRole&Version=invalid"),
                        matchit::Params::new(),
                    )
                    .await
                    .expect_err("invalid STS version or denied");
                if allowed {
                    assert_eq!(sts_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument, "{label}");
                } else {
                    assert_eq!(sts_err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied, "{label}");
                }
            }

            if let Some(previous) = previous {
                publish_test_app_context(previous);
            }
        });
    }

    #[test]
    #[serial_test::serial]
    fn admin_auth_uses_request_server_context_slot_for_accountinfo_and_heal() {
        crate::app::gating_test_env::run_large_stack_test("admin-multi-instance-context", || async {
            use crate::admin::operation::Operation;
            use crate::admin::operation::test_support::TestRequest;
            use crate::admin::runtime_sources::{
                AppContext, IamInterface, KmsInterface, ServerContextSlot, publish_test_app_context,
            };
            use rustfs_iam::store::object::ObjectStore;
            use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
            use std::sync::Arc;

            struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);
            impl IamInterface for ReadyIam {
                fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
                    Arc::clone(&self.0)
                }
                fn is_ready(&self) -> bool {
                    true
                }
            }
            struct TestKms;
            impl KmsInterface for TestKms {
                fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
                    Arc::new(rustfs_kms::KmsServiceManager::new())
                }
            }

            let previous = crate::runtime_sources::current_app_context();
            let (_dir_a, _paths_a, store_a) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
            let (_dir_b, _paths_b, store_b) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
            for store in [&store_a, &store_b] {
                ObjectStore::new(Arc::clone(store))
                    .save_iam_config(
                        serde_json::json!({"version": 1}),
                        format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
                    )
                    .await
                    .expect("seed IAM format");
            }
            let iam_a = rustfs_iam::build_iam_sys(Arc::clone(&store_a)).await.expect("init IAM A");
            let iam_b = rustfs_iam::build_iam_sys(Arc::clone(&store_b)).await.expect("init IAM B");

            iam_a
                .create_user(
                    "alice-server-a",
                    &AddOrUpdateUserReq {
                        secret_key: "alice-secret-1234".to_owned(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create alice on server A");
            iam_a
                .set_policy(
                    "a-all",
                    Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:*"]}]}"#)
                        .expect("parse policy A"),
                )
                .await
                .expect("save policy A");
            iam_a
                .policy_db_set("alice-server-a", UserType::Reg, false, "a-all")
                .await
                .expect("attach policy A");

            iam_a
                .create_user(
                    "split-brain-user",
                    &AddOrUpdateUserReq {
                        secret_key: "split-secret-a-1234".to_owned(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create unprivileged split-brain-user on server A");

            iam_b
                .create_user(
                    "bob-server-b",
                    &AddOrUpdateUserReq {
                        secret_key: "bob-secret-1234".to_owned(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create bob on server B");
            iam_b
                .create_user(
                    "split-brain-user",
                    &AddOrUpdateUserReq {
                        secret_key: "split-secret-b-1234".to_owned(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create privileged split-brain-user on server B");
            iam_b
                .set_policy(
                    "b-heal",
                    Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:Heal"]}]}"#)
                        .expect("parse policy B"),
                )
                .await
                .expect("save policy B");
            iam_b
                .policy_db_set("bob-server-b", UserType::Reg, false, "b-heal")
                .await
                .expect("attach policy B");
            iam_b
                .set_policy(
                    "b-create-user",
                    Policy::parse_config(
                        br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:CreateUser"]}]}"#,
                    )
                    .expect("parse create-user policy B"),
                )
                .await
                .expect("save create-user policy B");
            iam_b
                .policy_db_set("split-brain-user", UserType::Reg, false, "b-create-user")
                .await
                .expect("attach create-user policy B");

            // Server A is the process-ambient global context.
            let context_a = Arc::new(AppContext::new(store_a, Arc::new(ReadyIam(iam_a)), Arc::new(TestKms)));
            assert!(context_a.publish_action_credentials(Credentials {
                access_key: "root-server-a".to_owned(),
                secret_key: "root-a-secret-1234".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }));
            publish_test_app_context(context_a);

            // Server B runs in the same process with its own ServerContextSlot and root credentials.
            let context_b = Arc::new(AppContext::new(store_b, Arc::new(ReadyIam(iam_b)), Arc::new(TestKms)));
            assert!(context_b.publish_action_credentials(Credentials {
                access_key: "root-server-b".to_owned(),
                secret_key: "root-b-secret-1234".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }));
            let slot_b = ServerContextSlot::new();
            assert!(slot_b.install(Arc::clone(&context_b)));

            // 1. Server A's root and IAM user must NEVER authenticate against Server B's slot
            //    or direct `check_admin_key_valid_with_context` (used by table_catalog/plugin/listen).
            for (access_key, secret_key) in [
                ("root-server-a", "root-a-secret-1234"),
                ("alice-server-a", "alice-secret-1234"),
            ] {
                let direct_err = check_admin_key_valid_with_context("", access_key, Some(context_b.as_ref()))
                    .await
                    .expect_err("direct context-scoped key check must reject server A identity on server B");
                assert_eq!(direct_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId, "{access_key}");

                let req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/accountinfo")
                    .extension(slot_b.clone())
                    .credentials(access_key, secret_key, None)
                    .build();
                let auth_err =
                    authenticate_envelope_credentials_with_context(req.credentials().expect("test cred"), req.envelope())
                        .await
                        .expect_err("server A identity must not authenticate on server B");
                assert_eq!(auth_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId, "{access_key}");

                let acct_err = crate::admin::handlers::account_info::AccountInfoHandler {}
                    .call(req, matchit::Params::new())
                    .await
                    .expect_err("AccountInfoHandler must reject server A identity on server B");
                assert_eq!(acct_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId, "{access_key}");

                let heal_req = TestRequest::new(http::Method::POST, "/rustfs/admin/v3/heal")
                    .extension(slot_b.clone())
                    .credentials(access_key, secret_key, None)
                    .build();
                let heal_err = crate::admin::handlers::heal::HealHandler {}
                    .call(heal_req, matchit::Params::new())
                    .await
                    .expect_err("HealHandler must reject server A identity on server B");
                assert_eq!(heal_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId, "{access_key}");
            }

            // 2. An empty ServerContextSlot fails closed on slot-scoped paths rather than falling back to Server A.
            let empty_slot = ServerContextSlot::new();
            let empty_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/accountinfo")
                .extension(empty_slot)
                .credentials("root-server-a", "root-a-secret-1234", None)
                .build();
            let empty_err = authorize_admin_envelope_with_context_and_scope(
                empty_req.envelope(),
                vec![admin_action()],
                AdminResourceScope::bucket_object("", ""),
            )
            .await
            .expect_err("uninstalled server slot must fail closed");
            assert_eq!(empty_err.code(), &rustfs_s3_types::S3ErrorCode::ServiceUnavailable);

            // 3. Server B's own root and IAM user authenticate and authorize against Server B's slot.
            let (direct_root_b, direct_root_b_owner) =
                check_admin_key_valid_with_context("", "root-server-b", Some(context_b.as_ref()))
                    .await
                    .expect("direct context-scoped key check must authenticate server B root");
            assert_eq!(direct_root_b.access_key, "root-server-b");
            assert!(direct_root_b_owner);

            let root_b_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/accountinfo")
                .extension(slot_b.clone())
                .credentials("root-server-b", "root-b-secret-1234", None)
                .build();
            let (cred_b, is_owner_b) = authenticate_envelope_credentials_with_context(
                root_b_req.credentials().expect("test cred"),
                root_b_req.envelope(),
            )
            .await
            .expect("server B root must authenticate on server B");
            assert_eq!(cred_b.access_key, "root-server-b");
            assert!(is_owner_b);

            let acct_resp = crate::admin::handlers::account_info::AccountInfoHandler {}
                .call(root_b_req, matchit::Params::new())
                .await
                .expect("server B root must succeed on AccountInfoHandler");
            let (status, _headers, mut body) = acct_resp.into_parts();
            assert_eq!(status, http::StatusCode::OK);
            let payload: serde_json::Value =
                serde_json::from_slice(&body.store_all_limited(65_536).await.expect("read body")).expect("parse json");
            assert_eq!(payload["account_name"], "root-server-b");

            // Server B's user with admin:Heal clears HealHandler authentication & authorization.
            let bob_heal_req = TestRequest::new(http::Method::POST, "/rustfs/admin/v3/heal")
                .extension(slot_b.clone())
                .credentials("bob-server-b", "bob-secret-1234", None)
                .body("{invalid-json")
                .build();
            let bob_heal_err = crate::admin::handlers::heal::HealHandler {}
                .call(bob_heal_req, matchit::Params::new())
                .await
                .expect_err("malformed heal body should fail after authz");
            assert_ne!(bob_heal_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId);
            assert_ne!(bob_heal_err.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);

            // Server B's user without admin:ServerInfo is denied by Server B's IAM on slot-scoped gate.
            let bob_info_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/info")
                .extension(slot_b.clone())
                .credentials("bob-server-b", "bob-secret-1234", None)
                .build();
            assert_access_denied(
                authorize_admin_envelope_with_context_and_scope(
                    bob_info_req.envelope(),
                    vec![admin_action()],
                    AdminResourceScope::bucket_object("", ""),
                )
                .await
                .map(|_| ()),
            );

            // 4. Zero split-brain on ambient-store admin gates:
            //    (a) `validate_admin_request(..., req.envelope())` keeps IAM ambient (Server A), so
            //        an unprivileged Server A user whose name holds `admin:CreateUser` on Server B
            //        must NOT pass authz against Server A's ambient IAM.
            let split_req = TestRequest::new(http::Method::PUT, "/rustfs/admin/v3/add-user?accessKey=victim")
                .extension(slot_b.clone())
                .credentials("split-brain-user", "split-secret-a-1234", None)
                .build();
            let split_cred = Credentials {
                access_key: "split-brain-user".to_owned(),
                secret_key: "split-secret-a-1234".to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            };
            assert_access_denied(
                validate_admin_request(
                    split_req.headers(),
                    &split_cred,
                    false,
                    false,
                    vec![Action::AdminAction(AdminAction::CreateUserAdminAction)],
                    split_req.envelope(),
                )
                .await,
            );

            //    (b) `authorize_admin_envelope(req.envelope(), ...)` stays ambient across both auth
            //        and authz, so a Server B-only admin cannot authenticate on Server B and mutate
            //        Server A's ambient IAM/stores.
            let bob_ambient_req = TestRequest::new(http::Method::GET, "/rustfs/admin/v3/list-groups")
                .extension(slot_b.clone())
                .credentials("bob-server-b", "bob-secret-1234", None)
                .build();
            let bob_ambient_err =
                authorize_admin_envelope(bob_ambient_req.envelope(), vec![Action::AdminAction(AdminAction::HealAdminAction)])
                    .await
                    .expect_err("Server B identity must not pass ambient authorize_admin_envelope");
            assert_eq!(bob_ambient_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId);

            //    (c) Ambient `CallerIdentity::resolve_envelope` rejects Server B identities while
            //        slot-scoped `CallerIdentity::resolve_envelope_with_context` resolves them.
            let caller_ambient_err =
                crate::admin::service::caller_identity::CallerIdentity::resolve_envelope(bob_ambient_req.envelope())
                    .await
                    .expect_err("ambient CallerIdentity must stay in Server A");
            assert_eq!(caller_ambient_err.code(), &rustfs_s3_types::S3ErrorCode::InvalidAccessKeyId);
            let caller_scoped =
                crate::admin::service::caller_identity::CallerIdentity::resolve_envelope_with_context(bob_ambient_req.envelope())
                    .await
                    .expect("slot-scoped CallerIdentity must resolve Server B identity");
            assert_eq!(caller_scoped.access_key, "bob-server-b");

            if let Some(previous) = previous {
                publish_test_app_context(previous);
            }
        });
    }
}
