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

use crate::auth::get_condition_values;
use crate::server::RemoteAddr;
use http::HeaderMap;
use http::Uri;
use rustfs_credentials::Credentials;
use rustfs_iam::store::Store;
use rustfs_iam::sys::IamSys;
use rustfs_policy::policy::{Args, action::Action};
use s3s::{Body, S3Request, S3Result, s3_error};
use std::sync::Arc;
use tracing::debug;

#[derive(Clone)]
struct AuthContext<'a> {
    headers: &'a HeaderMap,
    cred: &'a Credentials,
    is_owner: bool,
    deny_only: bool,
    remote_addr: Option<std::net::SocketAddr>,
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

pub async fn validate_admin_request(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
) -> S3Result<()> {
    let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr,
    };

    evaluate_admin_actions(iam_store, &ctx, &actions, "", "").await
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
    let conditions = get_condition_values(ctx.headers, ctx.cred, None, None, ctx.remote_addr);

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

pub async fn validate_admin_request_with_bucket(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
    bucket: &str,
) -> S3Result<()> {
    validate_admin_request_with_bucket_object(
        headers,
        cred,
        is_owner,
        deny_only,
        actions,
        remote_addr,
        AdminResourceScope::bucket(bucket),
    )
    .await
}

pub async fn validate_admin_request_with_bucket_object(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
    resource: AdminResourceScope<'_>,
) -> S3Result<()> {
    let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr,
    };

    evaluate_admin_actions(iam_store, &ctx, &actions, resource.bucket, resource.object).await
}

pub(crate) async fn validate_admin_action_with_bucket_object_for_iam<S: Store>(
    iam_store: Arc<IamSys<S>>,
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    action: Action,
    remote_addr: Option<std::net::SocketAddr>,
    resource: AdminResourceScope<'_>,
) -> S3Result<()> {
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only: false,
        remote_addr,
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
pub async fn validate_admin_request_with_kms_key(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
    key_id: &str,
) -> S3Result<()> {
    validate_admin_request_with_bucket_object(
        headers,
        cred,
        is_owner,
        deny_only,
        actions,
        remote_addr,
        AdminResourceScope::kms_key(key_id),
    )
    .await
}

/// Unified authentication request handler for both UI and CLI
///
/// This function provides a single entry point for authentication,
/// ensuring consistent behavior between UI and CLI authentication flows.
///
/// # Arguments
/// * `headers` - HTTP request headers
/// * `uri` - Request URI
/// * `credentials` - User credentials from request (Credentials)
///
/// # Returns
/// * `Ok((Credentials, bool))` - Authentication successful, returns user credentials and is_owner flag
/// * `Err(S3Error)` - Authentication failed with error details
///
/// # Example
/// ```ignore
/// let (cred, is_owner) = authenticate_request(&req.headers, &req.uri, &input_cred).await?;
/// ```
pub async fn authenticate_request(
    headers: &HeaderMap,
    uri: &Uri,
    credentials: &s3s::auth::Credentials,
) -> S3Result<(Credentials, bool)> {
    use crate::auth::{check_key_valid, get_session_token};

    // Extract session token from request
    let session_token = get_session_token(uri, headers).unwrap_or_default();

    // Log authentication attempt for debugging
    debug!(
        "authenticate_request: processing authentication - access_key={}, has_session_token={}",
        credentials.access_key,
        !session_token.is_empty()
    );

    // Validate credentials using the core authentication function
    let result = check_key_valid(session_token, &credentials.access_key).await;

    match &result {
        Ok((cred, is_owner)) => {
            debug!(
                "authenticate_request: authentication successful - access_key={}, is_owner={}",
                cred.access_key, is_owner
            );
        }
        Err(e) => {
            tracing::warn!(
                "authenticate_request: authentication failed - access_key={}, error={}",
                credentials.access_key,
                e
            );
        }
    }

    result
}

/// Full admin gate over an `S3Request`: extract the request credentials,
/// authenticate them ([`authenticate_request`]), then authorize the caller for
/// `actions` ([`validate_admin_request`], allowing on the first permitted
/// action). Returns the authenticated credentials for handlers that need the
/// caller identity. `deny_only` stays `false`: every caller performs a full
/// allow check.
pub async fn authorize_admin_request(req: &S3Request<Body>, actions: Vec<Action>) -> S3Result<Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) = authenticate_request(&req.headers, &req.uri, input_cred).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, actions, remote_addr).await?;

    Ok(cred)
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

    /// A `Store` whose methods are never invoked by these tests: the gate only
    /// reaches the persistence layer for non-owner principals, and the
    /// `IamCache` we build starts with an empty in-memory snapshot, so
    /// `get_user` resolves to `None` (unknown user) before any `api` call.
    #[derive(Clone)]
    struct EmptyStore;

    #[async_trait::async_trait]
    impl Store for EmptyStore {
        fn has_watcher(&self) -> bool {
            false
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
            api: EmptyStore,
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
        }
    }

    fn admin_action() -> Action {
        Action::AdminAction(AdminAction::ServerInfoAdminAction)
    }

    fn assert_access_denied(res: S3Result<()>) {
        let err = res.expect_err("gate should reject the request");
        assert_eq!(
            err.code(),
            &s3s::S3ErrorCode::AccessDenied,
            "admin gate rejection must map to the AccessDenied S3 error code"
        );
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
    async fn authorize_admin_request_without_credentials_is_rejected() {
        let req = S3Request {
            input: Body::from(String::new()),
            method: http::Method::GET,
            uri: Uri::from_static("/rustfs/admin/v3/list-jobs"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let err = authorize_admin_request(&req, vec![admin_action()])
            .await
            .expect_err("a request without credentials must be rejected");
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("get cred failed"));
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
}
