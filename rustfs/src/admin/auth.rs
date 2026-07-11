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
use http::HeaderMap;
use http::Uri;
use rustfs_credentials::Credentials;
use rustfs_iam::store::Store;
use rustfs_iam::sys::IamSys;
use rustfs_policy::policy::{Args, action::Action};
use s3s::{S3Result, s3_error};
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
    use rustfs_policy::policy::PolicyDoc;
    use rustfs_policy::policy::action::AdminAction;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64};

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

    /// Build an `IamSys` backed by an empty in-memory cache. The cache is
    /// constructed directly (all fields are public, mirroring the iam crate's
    /// own `build_test_iam_cache`) so no cluster-backed `ObjectStore` or disk
    /// load is required. Readiness state is irrelevant: the gate calls
    /// `is_allowed` directly, which does not gate on `is_ready`.
    fn iam_sys_with_empty_store() -> Arc<IamSys<EmptyStore>> {
        let (send_chan, _rx) = tokio::sync::mpsc::channel::<i64>(1);
        let cache = IamCache {
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
        };
        Arc::new(IamSys::new(Arc::new(cache)))
    }

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
