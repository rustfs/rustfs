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

//! Discovery decisions: which buckets a caller may see, and whether an action is possible at all.
//!
//! Responsible for: the IAM-only ListBuckets visibility decision with its per-bucket fallback
//! ([`ListBucketsIamAuthorization`], [`query_list_buckets_permission`]) and the
//! discoverability hint for an action whose exact target is unknown ([`s3_action_potential`]).
//! Not responsible for: authorizing an operation on a known target (`decision`) — a
//! discovery answer never authorizes an object.
//! Upstream: the ListBuckets use case and the admin account-permissions handler. Downstream:
//! IAM and the bucket policy system.

use super::conditions::{authorization_conditions, request_authz_facts, request_principal, root_access_key};
use super::decision::{DenialContext, bucket_policy_deny_accounts, is_reserved_meta_bucket, owner_can_bypass_policy_deny};
use super::{AuthorizationRequest, missing_req_info_error, request_iam_store, request_object_store};
use crate::auth::facts::{AuthzTarget, build_args};
use crate::error::ApiError;
use crate::storage::denial_audit::DenialAuditor;
use crate::storage::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use crate::storage::{StorageError, is_err_bucket_not_found};
use rustfs_iam::{
    store::object::ObjectStore,
    sys::{IamSys, PreparedIamAuth},
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicyArgs};
use rustfs_s3_types::S3Result;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct ListBucketsIamAuthorization {
    iam_store: Arc<IamSys<ObjectStore>>,
    prepared: PreparedIamAuth,
    account: String,
    groups: Option<Vec<String>>,
    claims: HashMap<String, serde_json::Value>,
    is_owner: bool,
    base_conditions: HashMap<String, Vec<String>>,
    bucket_conditions: HashMap<String, Vec<String>>,
}

impl ListBucketsIamAuthorization {
    async fn probe_conditions_known(&self, bucket: &str, action: S3Action) -> bool {
        let conditions = if bucket.is_empty() {
            &self.base_conditions
        } else {
            &self.bucket_conditions
        };
        !self
            .prepared
            .has_unknown_probe_conditions(&Args {
                account: &self.account,
                groups: &self.groups,
                claims: &self.claims,
                is_owner: self.is_owner,
                action: Action::S3Action(action),
                bucket,
                object: "",
                conditions,
                deny_only: false,
            })
            .await
    }
    pub(crate) async fn is_allowed(&self, bucket: &str, action: S3Action) -> bool {
        let conditions = if bucket.is_empty() {
            &self.base_conditions
        } else {
            &self.bucket_conditions
        };
        self.iam_store
            .eval_prepared(
                &self.prepared,
                &Args {
                    account: &self.account,
                    groups: &self.groups,
                    action: Action::S3Action(action),
                    bucket,
                    conditions,
                    is_owner: self.is_owner,
                    object: "",
                    claims: &self.claims,
                    deny_only: false,
                },
            )
            .await
    }
}

/// The same IAM-only discovery contract as ListBuckets, including its per-bucket fallback.
/// Bucket policies may authorize a known bucket but do not make its name discoverable.
pub(crate) async fn query_list_buckets_permission<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<()> {
    let authorization = prepare_list_buckets_iam_authorization(req).await?;
    let all_known = authorization
        .probe_conditions_known("", S3Action::ListAllMyBucketsAction)
        .await;
    if all_known && authorization.is_allowed("", S3Action::ListAllMyBucketsAction).await {
        return Ok(());
    }
    let mut unknown = !all_known;
    let store = request_object_store(req)?;
    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
    for bucket in buckets {
        for action in [S3Action::ListBucketAction, S3Action::GetBucketLocationAction] {
            if authorization.probe_conditions_known(&bucket.name, action).await {
                if authorization.is_allowed(&bucket.name, action).await {
                    return Ok(());
                }
            } else {
                unknown = true;
            }
        }
    }
    if unknown {
        Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query lacks operation facts"))
    } else {
        Err(rustfs_s3_types::s3_error!(AccessDenied, "Access Denied"))
    }
}

pub(crate) fn request_root_access_key(server_context: Option<&Arc<ServerContextSlot>>) -> String {
    match server_context {
        Some(slot) => slot
            .installed_app_context()
            .and_then(|context| context.action_credentials().get())
            .map(|root| root.access_key)
            .unwrap_or_default(),
        None => root_access_key(),
    }
}

/// Prepare the IAM-only authorization used to decide which buckets are visible in ListBuckets.
/// Bucket policies are intentionally excluded from this discovery decision, matching MinIO.
pub(crate) async fn prepare_list_buckets_iam_authorization<R: AuthorizationRequest + ?Sized>(
    req: &R,
) -> S3Result<ListBucketsIamAuthorization> {
    let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
    let Some(cred) = req_info.cred.as_ref() else {
        return Err(ApiError::access_denied().into());
    };
    let iam_store = request_iam_store(req)?;
    let account = cred.access_key.clone();
    let groups = cred.groups.clone();
    let claims = cred.claims.clone().unwrap_or_default();
    let action = Action::S3Action(S3Action::ListAllMyBucketsAction);
    let root_access_key = request_root_access_key(req.server_context());
    let principal = request_principal(req, cred, &root_access_key);
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action,
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    let base_conditions = authorization_conditions(req, &principal, &facts)?;
    let mut bucket_conditions = base_conditions.clone();
    bucket_conditions.insert("prefix".to_string(), vec![String::new()]);
    bucket_conditions.insert("delimiter".to_string(), vec!["/".to_string()]);
    let prepared = iam_store
        .prepare_auth(&Args {
            account: &account,
            groups: &groups,
            action,
            bucket: "",
            conditions: &base_conditions,
            is_owner: req_info.is_owner,
            object: "",
            claims: &claims,
            deny_only: false,
        })
        .await;

    Ok(ListBucketsIamAuthorization {
        iam_store,
        prepared,
        account,
        groups,
        claims,
        is_owner: req_info.is_owner,
        base_conditions,
        bucket_conditions,
    })
}

/// Preserve the top-level IAM denial audit emitted before ListBuckets falls back
/// to bucket-level visibility checks.
pub(crate) fn log_list_buckets_iam_implicit_deny<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<()> {
    let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
    let denial = DenialContext {
        quiet: true,
        suppress_audit: true,
        auditor: DenialAuditor::global(),
        bucket: req_info.bucket.as_deref().unwrap_or_default(),
        object: req_info.object.as_deref().unwrap_or_default(),
        version_id: req_info.version_id.as_deref(),
        account: req_info.cred.as_ref().map(|cred| cred.access_key.as_str()),
        is_owner: req_info.is_owner,
    };
    denial.log("iam_implicit_deny", Action::S3Action(S3Action::ListAllMyBucketsAction));
    Ok(())
}

/// Discoverability hint for an action with an unknown exact target. This does not authorize
/// an object, and the normal exact-object gate must still run before the operation.
pub(crate) async fn s3_action_potential<R: AuthorizationRequest + ?Sized>(req: &R, action: Action) -> S3Result<bool> {
    use rustfs_policy::policy::probe::{bucket_policy_action_potential, bucket_policy_blocks_action_potential};
    let info = req.req_info().ok_or_else(missing_req_info_error)?;
    let cred = info
        .cred
        .as_ref()
        .ok_or_else(|| rustfs_s3_types::s3_error!(AccessDenied, "authentication required"))?;
    let bucket = info.bucket.as_deref().unwrap_or_default();
    if is_reserved_meta_bucket(bucket) {
        return Ok(false);
    }
    let root = request_root_access_key(req.server_context());
    let principal = request_principal(req, cred, &root).with_owner(info.is_owner);
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action,
            bucket,
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    let conditions = authorization_conditions(req, &principal, &facts)?;
    let args = build_args(&principal, &facts, &conditions);
    let iam = request_iam_store(req)?;
    let prepared = iam.prepare_auth(&args).await;
    let iam_potential = prepared.action_potential(&args).await;
    if bucket.is_empty() {
        return Ok(iam_potential);
    }
    let store = request_object_store(req)?;
    let policy = match store.get_bucket_policy(bucket).await {
        Ok((policy, _)) => policy,
        Err(error) if error == StorageError::ConfigNotFound || is_err_bucket_not_found(&error) => return Ok(iam_potential),
        Err(_) => return Err(rustfs_s3_types::s3_error!(NotImplemented, "permission query is unavailable")),
    };
    let bucket_args = BucketPolicyArgs {
        bucket,
        action,
        is_owner: info.is_owner,
        account: &cred.access_key,
        groups: &cred.groups,
        conditions: &conditions,
        object: "",
    };
    if !owner_can_bypass_policy_deny(info.is_owner, &action) {
        for account in bucket_policy_deny_accounts(cred) {
            if bucket_policy_blocks_action_potential(
                &policy,
                &BucketPolicyArgs {
                    account,
                    ..bucket_args.clone()
                },
            )
            .await
            {
                return Ok(false);
            }
        }
    }
    Ok(iam_potential
        || (prepared.permits_bucket_policy_probe_fallback() && bucket_policy_action_potential(&policy, &bucket_args).await))
}
