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

use super::ecfs::FS;
use super::ecfs::{
    ACL_GROUP_ALL_USERS, ACL_GROUP_AUTHENTICATED_USERS, INTERNAL_ACL_METADATA_KEY, StoredAcl, default_owner,
    parse_acl_json_or_canned_bucket, parse_acl_json_or_canned_object, stored_acl_from_canned_bucket,
    stored_acl_from_canned_object,
};
use super::options::get_opts;
use crate::auth::{check_key_valid, get_condition_values, get_session_token};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::RemoteAddr;
use metrics::counter;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::policy_sys::PolicySys;
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOperations, ObjectOperations};
use rustfs_iam::error::Error as IamError;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicyArgs};
use rustfs_utils::http::AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE;
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, dto::*, s3_error};
use std::collections::HashMap;

#[derive(Default, Clone, Debug)]
pub(crate) struct ReqInfo {
    pub cred: Option<rustfs_credentials::Credentials>,
    pub is_owner: bool,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub version_id: Option<String>,
    #[allow(dead_code)]
    pub region: Option<s3s::region::Region>,
}

#[derive(Clone, Copy)]
enum AclTarget {
    Bucket,
    Object,
}

#[derive(Clone, Copy)]
enum AclPermission {
    Read,
    Write,
    ReadAcp,
    WriteAcp,
}

fn acl_permission_for_action(action: &Action) -> Option<(AclTarget, AclPermission)> {
    match action {
        Action::S3Action(S3Action::ListBucketAction) => Some((AclTarget::Bucket, AclPermission::Read)),
        Action::S3Action(S3Action::PutObjectAction) => Some((AclTarget::Bucket, AclPermission::Write)),
        Action::S3Action(S3Action::GetBucketAclAction) => Some((AclTarget::Bucket, AclPermission::ReadAcp)),
        Action::S3Action(S3Action::PutBucketAclAction) => Some((AclTarget::Bucket, AclPermission::WriteAcp)),
        Action::S3Action(S3Action::GetObjectAction) => Some((AclTarget::Object, AclPermission::Read)),
        Action::S3Action(S3Action::GetObjectAclAction) => Some((AclTarget::Object, AclPermission::ReadAcp)),
        Action::S3Action(S3Action::PutObjectAclAction) => Some((AclTarget::Object, AclPermission::WriteAcp)),
        _ => None,
    }
}

fn permission_matches(grant_perm: &str, required: AclPermission) -> bool {
    if grant_perm == Permission::FULL_CONTROL {
        return true;
    }

    match required {
        AclPermission::Read => grant_perm == Permission::READ,
        AclPermission::Write => grant_perm == Permission::WRITE,
        AclPermission::ReadAcp => grant_perm == Permission::READ_ACP,
        AclPermission::WriteAcp => grant_perm == Permission::WRITE_ACP,
    }
}

fn acl_allows(
    acl: &StoredAcl,
    user_id: Option<&str>,
    is_authenticated: bool,
    required: AclPermission,
    ignore_public_acls: bool,
) -> bool {
    for grant in &acl.grants {
        if !permission_matches(grant.permission.as_str(), required) {
            continue;
        }

        match grant.grantee.grantee_type.as_str() {
            "CanonicalUser" => {
                if user_id.is_some_and(|id| grant.grantee.id.as_deref() == Some(id)) {
                    return true;
                }
            }
            "Group" => {
                if ignore_public_acls {
                    continue;
                }
                if let Some(uri) = grant.grantee.uri.as_deref() {
                    if uri == ACL_GROUP_ALL_USERS {
                        return true;
                    }
                    if uri == ACL_GROUP_AUTHENTICATED_USERS && is_authenticated {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }

    false
}

async fn load_bucket_acl(bucket: &str) -> S3Result<StoredAcl> {
    let owner = default_owner();
    match metadata_sys::get_bucket_acl_config(bucket).await {
        Ok((acl, _)) => Ok(parse_acl_json_or_canned_bucket(&acl, &owner)),
        Err(err) => {
            if err == StorageError::ConfigNotFound {
                Ok(stored_acl_from_canned_bucket(BucketCannedACL::PRIVATE, &owner))
            } else {
                Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()))
            }
        }
    }
}

async fn load_object_acl(bucket: &str, object: &str, version_id: Option<&str>, headers: &http::HeaderMap) -> S3Result<StoredAcl> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let opts = get_opts(bucket, object, version_id.map(|v| v.to_string()), None, headers)
        .await
        .map_err(ApiError::from)?;
    let info = store.get_object_info(bucket, object, &opts).await.map_err(ApiError::from)?;

    let bucket_owner = default_owner();
    let object_owner = info
        .user_defined
        .get(INTERNAL_ACL_METADATA_KEY)
        .and_then(|acl| serde_json::from_str::<StoredAcl>(acl).ok())
        .map(|acl| acl.owner)
        .unwrap_or_else(default_owner);

    Ok(info
        .user_defined
        .get(INTERNAL_ACL_METADATA_KEY)
        .map(|acl| parse_acl_json_or_canned_object(acl, &bucket_owner, &object_owner))
        .unwrap_or_else(|| stored_acl_from_canned_object(ObjectCannedACL::PRIVATE, &bucket_owner, &object_owner)))
}

async fn check_acl_access<T>(req: &S3Request<T>, req_info: &ReqInfo, action: &Action, policy_allowed: bool) -> S3Result<bool> {
    let Some((target, permission)) = acl_permission_for_action(action) else {
        return Ok(true);
    };

    // For object-level operations, do not bypass on account-level is_owner.
    // The bucket/account owner must have explicit ACL grant to access objects owned by others.
    let bypass_owner = match target {
        AclTarget::Bucket => req_info.is_owner,
        AclTarget::Object => false,
    };
    if bypass_owner || policy_allowed {
        return Ok(true);
    }

    let bucket = req_info.bucket.as_deref().unwrap_or("");
    if bucket.is_empty() {
        return Ok(true);
    }

    let ignore_public_acls = match metadata_sys::get_public_access_block_config(bucket).await {
        Ok((config, _)) => config.ignore_public_acls.unwrap_or(false),
        Err(_) => false,
    };

    let user_id = req_info.cred.as_ref().map(|cred| cred.access_key.as_str());
    let is_authenticated = user_id.is_some();

    let acl = match target {
        AclTarget::Bucket => load_bucket_acl(bucket).await?,
        AclTarget::Object => {
            let object = req_info.object.as_deref().unwrap_or("");
            if object.is_empty() {
                return Ok(true);
            }
            load_object_acl(bucket, object, req_info.version_id.as_deref(), &req.headers).await?
        }
    };

    Ok(acl_allows(&acl, user_id, is_authenticated, permission, ignore_public_acls))
}

pub(crate) fn req_info_ref<T>(req: &S3Request<T>) -> S3Result<&ReqInfo> {
    req.extensions
        .get::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

pub(crate) fn req_info_mut<T>(req: &mut S3Request<T>) -> S3Result<&mut ReqInfo> {
    req.extensions
        .get_mut::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

fn ext_req_info_mut(ext: &mut http::Extensions) -> S3Result<&mut ReqInfo> {
    ext.get_mut::<ReqInfo>()
        .ok_or_else(|| s3_error!(InternalError, "ReqInfo not found in request extensions"))
}

#[derive(Clone, Debug)]
pub(crate) struct ObjectTagConditions(pub HashMap<String, Vec<String>>);

/// Returns true if the bucket has a policy that uses `s3:ExistingObjectTag` (or
/// `ExistingObjectTag/...`) conditions. Used to skip fetching object tags when
/// no tag-based policy is in effect.
async fn bucket_policy_uses_existing_object_tag(bucket: &str) -> bool {
    let Ok((policy_str, _)) = metadata_sys::get_bucket_policy_raw(bucket).await else {
        return false;
    };
    policy_str.contains("ExistingObjectTag")
}

impl FS {
    /// Fetches object tags (when the bucket policy requires them) and wraps them
    /// as `ObjectTagConditions`. Returns `AccessDenied` on transient storage
    /// errors to avoid fail-open on Deny policies.
    async fn fetch_tag_conditions(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        op: &'static str,
    ) -> S3Result<ObjectTagConditions> {
        let tag_conditions = if bucket_policy_uses_existing_object_tag(bucket).await {
            counter!("rustfs.object_tag_conditions.fetched", "op" => op).increment(1);
            self.get_object_tag_conditions_for_policy(bucket, key, version_id).await?
        } else {
            counter!("rustfs.object_tag_conditions.skipped", "op" => op).increment(1);
            HashMap::new()
        };
        Ok(ObjectTagConditions(tag_conditions))
    }
}

/// Authorizes the request based on the action and credentials.
pub async fn authorize_request<T>(req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    let object_tag_conditions = req.extensions.get::<ObjectTagConditions>().cloned();

    let req_info = req_info_ref(req)?;

    if let Some(cred) = &req_info.cred {
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("authorize_request {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);
        let mut conditions = get_condition_values(&req.headers, cred, req_info.version_id.as_deref(), None, remote_addr);
        // Merge object tag conditions; extend existing values if the same key exists (e.g. from get_condition_values).
        if let Some(ref tags) = object_tag_conditions {
            for (k, v) in &tags.0 {
                conditions
                    .entry(k.clone())
                    .and_modify(|existing| existing.extend(v.iter().cloned()))
                    .or_insert_with(|| v.clone());
            }
        }
        let bucket_name = req_info.bucket.as_deref().unwrap_or("");

        if !bucket_name.is_empty()
            && !PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket_name,
                action,
                // Run this early check in deny-only mode so ACL/IAM fallbacks can still grant access.
                is_owner: true,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: req_info.object.as_deref().unwrap_or(""),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        if action == Action::S3Action(S3Action::DeleteObjectAction)
            && req_info.version_id.is_some()
            && !iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                    bucket: req_info.bucket.as_deref().unwrap_or(""),
                    conditions: &conditions,
                    is_owner: req_info.is_owner,
                    object: req_info.object.as_deref().unwrap_or(""),
                    claims,
                    deny_only: false,
                })
                .await
            && !PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: req_info.bucket.as_deref().unwrap_or(""),
                action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                is_owner: req_info.is_owner,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: req_info.object.as_deref().unwrap_or(""),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        let iam_allowed = iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action,
                bucket: req_info.bucket.as_deref().unwrap_or(""),
                conditions: &conditions,
                is_owner: req_info.is_owner,
                object: req_info.object.as_deref().unwrap_or(""),
                claims,
                deny_only: false,
            })
            .await;

        if iam_allowed {
            let policy_allowed = if !bucket_name.is_empty() {
                PolicySys::is_allowed(&BucketPolicyArgs {
                    bucket: bucket_name,
                    action,
                    is_owner: false,
                    account: &cred.access_key,
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: req_info.object.as_deref().unwrap_or(""),
                })
                .await
            } else {
                false
            };

            if check_acl_access(req, req_info, &action, policy_allowed).await? {
                return Ok(());
            }

            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        let policy_allowed_fallback = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: req_info.bucket.as_deref().unwrap_or(""),
            action,
            is_owner: req_info.is_owner,
            account: &cred.access_key,
            groups: &cred.groups,
            conditions: &conditions,
            object: req_info.object.as_deref().unwrap_or(""),
        })
        .await;

        if policy_allowed_fallback {
            // For object-level operations, bucket owner (is_owner) must still pass ACL check.
            // Policy may have allowed due to is_owner, but object owner is authoritative.
            if let Some((AclTarget::Object, _)) = acl_permission_for_action(&action)
                && !check_acl_access(req, req_info, &action, false).await?
            {
                return Err(s3_error!(AccessDenied, "Access Denied"));
            }
            return Ok(());
        }

        if action == Action::S3Action(S3Action::ListBucketVersionsAction) {
            if iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::S3Action(S3Action::ListBucketAction),
                    bucket: req_info.bucket.as_deref().unwrap_or(""),
                    conditions: &conditions,
                    is_owner: req_info.is_owner,
                    object: req_info.object.as_deref().unwrap_or(""),
                    claims,
                    deny_only: false,
                })
                .await
            {
                return Ok(());
            }

            if PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: req_info.bucket.as_deref().unwrap_or(""),
                action: Action::S3Action(S3Action::ListBucketAction),
                is_owner: req_info.is_owner,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: req_info.object.as_deref().unwrap_or(""),
            })
            .await
            {
                return Ok(());
            }
        }
    } else {
        let mut conditions = get_condition_values(
            &req.headers,
            &rustfs_credentials::Credentials::default(),
            req_info.version_id.as_deref(),
            req.region.clone(),
            remote_addr,
        );
        // Merge object tag conditions; extend existing values if the same key exists.
        if let Some(ref tags) = object_tag_conditions {
            for (k, v) in &tags.0 {
                conditions
                    .entry(k.clone())
                    .and_modify(|existing| existing.extend(v.iter().cloned()))
                    .or_insert_with(|| v.clone());
            }
        }
        let bucket_name = req_info.bucket.as_deref().unwrap_or("");

        if !bucket_name.is_empty()
            && !PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket_name,
                action,
                // Run this early check in deny-only mode so ACL checks are not bypassed.
                is_owner: true,
                account: "",
                groups: &None,
                conditions: &conditions,
                object: req_info.object.as_deref().unwrap_or(""),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        if action != Action::S3Action(S3Action::ListAllMyBucketsAction) {
            let policy_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: req_info.bucket.as_deref().unwrap_or(""),
                action,
                is_owner: false,
                account: "",
                groups: &None,
                conditions: &conditions,
                object: req_info.object.as_deref().unwrap_or(""),
            })
            .await;

            if policy_allowed {
                // RestrictPublicBuckets: when true, deny public access even if bucket policy allows it.
                // Fail closed: if we cannot read the config, do not allow public access.
                match metadata_sys::get_public_access_block_config(bucket_name).await {
                    Ok((config, _)) => {
                        if config.restrict_public_buckets.unwrap_or(false) {
                            return Err(s3_error!(AccessDenied, "Access Denied"));
                        }
                    }
                    Err(_) => {
                        return Err(s3_error!(AccessDenied, "Access Denied"));
                    }
                }
                return Ok(());
            }

            if action == Action::S3Action(S3Action::ListBucketVersionsAction)
                && PolicySys::is_allowed(&BucketPolicyArgs {
                    bucket: req_info.bucket.as_deref().unwrap_or(""),
                    action: Action::S3Action(S3Action::ListBucketAction),
                    is_owner: false,
                    account: "",
                    groups: &None,
                    conditions: &conditions,
                    object: "",
                })
                .await
            {
                return Ok(());
            }

            if acl_permission_for_action(&action).is_some() && check_acl_access(req, req_info, &action, false).await? {
                return Ok(());
            }
        }
    }

    Err(s3_error!(AccessDenied, "Access Denied"))
}

/// Check if the request has the x-amz-bypass-governance-retention header set to true
pub fn has_bypass_governance_header(headers: &http::HeaderMap) -> bool {
    headers
        .get(AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn get_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::GetBucketPolicyAction)
}

fn get_object_acl_authorize_action() -> Action {
    Action::S3Action(S3Action::GetObjectAclAction)
}

fn put_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::PutBucketPolicyAction)
}

fn put_object_acl_authorize_action() -> Action {
    Action::S3Action(S3Action::PutObjectAclAction)
}

#[async_trait::async_trait]
impl S3Access for FS {
    // /// Checks whether the current request has accesses to the resources.
    // ///
    // /// This method is called before deserializing the operation input.
    // ///
    // /// By default, this method rejects all anonymous requests
    // /// and returns [`AccessDenied`](crate::S3ErrorCode::AccessDenied) error.
    // ///
    // /// An access control provider can override this method to implement custom logic.
    // ///
    // /// Common fields in the context:
    // /// + [`cx.credentials()`](S3AccessContext::credentials)
    // /// + [`cx.s3_path()`](S3AccessContext::s3_path)
    // /// + [`cx.s3_op().name()`](crate::S3Operation::name)
    // /// + [`cx.extensions_mut()`](S3AccessContext::extensions_mut)
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Upper layer has verified ak/sk
        // info!(
        //     "s3 check uri: {:?}, method: {:?} path: {:?}, s3_op: {:?}, cred: {:?}, headers:{:?}",
        //     cx.uri(),
        //     cx.method(),
        //     cx.s3_path(),
        //     cx.s3_op().name(),
        //     cx.credentials(),
        //     cx.headers(),
        //     // cx.extensions_mut(),
        // );

        let (cred, is_owner) = if let Some(input_cred) = cx.credentials() {
            let (cred, is_owner) =
                check_key_valid(get_session_token(cx.uri(), cx.headers()).unwrap_or_default(), &input_cred.access_key).await?;
            (Some(cred), is_owner)
        } else {
            (None, false)
        };

        let req_info = ReqInfo {
            cred,
            is_owner,
            region: rustfs_ecstore::global::get_global_region(),
            ..Default::default()
        };

        let ext = cx.extensions_mut();
        ext.insert(req_info);

        // Verify uniformly here? Or verify separately below?

        Ok(())
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
        license_check().map_err(|er| s3_error!(AccessDenied, "{:?}", er.to_string()))?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::CreateBucketAction)).await?;

        if req.input.object_lock_enabled_for_bucket.is_some_and(|v| v) {
            authorize_request(req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::PutBucketVersioningAction)).await?;
        }

        Ok(())
    }
    /// Checks whether the AbortMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn abort_multipart_upload(&self, _req: &mut S3Request<AbortMultipartUploadInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the CompleteMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn complete_multipart_upload(&self, _req: &mut S3Request<CompleteMultipartUploadInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the CopyObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        {
            let (src_bucket, src_key, version_id) = match &req.input.copy_source {
                CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Bucket { bucket, key, version_id } => {
                    (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
                }
            };

            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(src_bucket.clone());
            req_info.object = Some(src_key.clone());
            req_info.version_id = version_id.clone();

            let tag_conds = self
                .fetch_tag_conditions(&src_bucket, &src_key, version_id.as_deref(), "copy_object_src")
                .await?;
            req.extensions.insert(tag_conds);

            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        let req_info = ext_req_info_mut(&mut req.extensions)?;

        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    async fn create_multipart_upload(&self, req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        license_check().map_err(|er| s3_error!(AccessDenied, "{:?}", er.to_string()))?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketAction)).await?;

        if req.input.force_delete.is_some_and(|v| v) {
            authorize_request(req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }
        Ok(())
    }

    /// Checks whether the DeleteBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_cors(&self, req: &mut S3Request<DeleteBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketCorsAction)).await
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketEncryptionAction)).await
    }

    /// Checks whether the DeleteBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketLifecycle request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_lifecycle(&self, req: &mut S3Request<DeleteBucketLifecycleInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketLifecycleAction)).await
    }

    /// Checks whether the DeleteBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_metrics_configuration(
        &self,
        _req: &mut S3Request<DeleteBucketMetricsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_ownership_controls(&self, _req: &mut S3Request<DeleteBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_policy(&self, req: &mut S3Request<DeleteBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketPolicyAction)).await
    }

    /// Checks whether the DeleteBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_replication(&self, req: &mut S3Request<DeleteBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutReplicationConfigurationAction)).await
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketTaggingAction)).await
    }

    /// Checks whether the DeleteBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_website(&self, _req: &mut S3Request<DeleteBucketWebsiteInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object(&self, req: &mut S3Request<DeleteObjectInput>) -> S3Result<()> {
        if let Some(store) = new_object_layer_fn()
            && let Err(err) = store.get_bucket_info(&req.input.bucket, &Default::default()).await
            && is_err_bucket_not_found(&err)
        {
            return Err(s3_error!(NoSuchBucket, "The specified bucket does not exist"));
        }

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "delete_object")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectAction)).await?;

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the DeleteObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object_tagging(&self, req: &mut S3Request<DeleteObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
                "delete_object_tagging",
            )
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectTaggingAction)).await
    }

    /// Checks whether the DeleteObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_objects(&self, req: &mut S3Request<DeleteObjectsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = None;
        req_info.version_id = None;

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectAction)).await?;

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the DeletePublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_public_access_block(&self, req: &mut S3Request<DeletePublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the GetBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_accelerate_configuration(
        &self,
        _req: &mut S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_acl(&self, req: &mut S3Request<GetBucketAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketAclAction)).await
    }

    /// Checks whether the GetBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<GetBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_cors(&self, req: &mut S3Request<GetBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketCorsAction)).await
    }

    /// Checks whether the GetBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_encryption(&self, req: &mut S3Request<GetBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketEncryptionAction)).await
    }

    /// Checks whether the GetBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<GetBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<GetBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketLifecycleConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: &mut S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLifecycleAction)).await
    }

    /// Checks whether the GetBucketLocation request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_location(&self, req: &mut S3Request<GetBucketLocationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLocationAction)).await
    }

    /// Checks whether the GetBucketLogging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_logging(&self, req: &mut S3Request<GetBucketLoggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLoggingAction)).await
    }

    /// Checks whether the GetBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_metrics_configuration(&self, _req: &mut S3Request<GetBucketMetricsConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketNotificationConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_notification_configuration(
        &self,
        req: &mut S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketNotificationAction)).await
    }

    /// Checks whether the GetBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_ownership_controls(&self, _req: &mut S3Request<GetBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy(&self, req: &mut S3Request<GetBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, get_bucket_policy_authorize_action()).await
    }

    /// Checks whether the GetBucketPolicyStatus request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy_status(&self, req: &mut S3Request<GetBucketPolicyStatusInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyStatusAction)).await
    }

    /// Checks whether the GetBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_replication(&self, req: &mut S3Request<GetBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetReplicationConfigurationAction)).await
    }

    /// Checks whether the GetBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_request_payment(&self, _req: &mut S3Request<GetBucketRequestPaymentInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_tagging(&self, req: &mut S3Request<GetBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketTaggingAction)).await
    }

    /// Checks whether the GetBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_versioning(&self, req: &mut S3Request<GetBucketVersioningInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketVersioningAction)).await
    }

    /// Checks whether the GetBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_website(&self, _req: &mut S3Request<GetBucketWebsiteInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object(&self, req: &mut S3Request<GetObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "get_object")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "get_object_acl")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, get_object_acl_authorize_action()).await
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
                "get_object_attributes",
            )
            .await?;
        req.extensions.insert(tag_conds);

        if req.input.version_id.is_some() {
            authorize_request(req, Action::S3Action(S3Action::GetObjectVersionAttributesAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::GetObjectVersionAction)).await?;
        } else {
            authorize_request(req, Action::S3Action(S3Action::GetObjectAttributesAction)).await?;
            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the GetObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_legal_hold(&self, req: &mut S3Request<GetObjectLegalHoldInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectLegalHoldAction)).await
    }

    /// Checks whether the GetObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_lock_configuration(&self, req: &mut S3Request<GetObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the GetObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_retention(&self, req: &mut S3Request<GetObjectRetentionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectRetentionAction)).await
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "get_object_tagging")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::GetObjectTaggingAction)).await
    }

    /// Checks whether the GetObjectTorrent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_torrent(&self, _req: &mut S3Request<GetObjectTorrentInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_public_access_block(&self, req: &mut S3Request<GetPublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the HeadBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_bucket(&self, req: &mut S3Request<HeadBucketInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "head_object")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the ListBucketAnalyticsConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_analytics_configurations(
        &self,
        _req: &mut S3Request<ListBucketAnalyticsConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketIntelligentTieringConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_intelligent_tiering_configurations(
        &self,
        _req: &mut S3Request<ListBucketIntelligentTieringConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketInventoryConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_inventory_configurations(
        &self,
        _req: &mut S3Request<ListBucketInventoryConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBucketMetricsConfigurations request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_bucket_metrics_configurations(
        &self,
        _req: &mut S3Request<ListBucketMetricsConfigurationsInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListBuckets request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_buckets(&self, _req: &mut S3Request<ListBucketsInput>) -> S3Result<()> {
        // check inside
        Ok(())
    }

    /// Checks whether the ListMultipartUploads request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_multipart_uploads(&self, req: &mut S3Request<ListMultipartUploadsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketMultipartUploadsAction)).await
    }

    /// Checks whether the `ListObjectVersions` request is authorized for the requested bucket.
    ///
    /// Returns `Ok(())` if the request is allowed, or an error if access is denied or another
    /// authorization-related issue occurs.
    async fn list_object_versions(&self, req: &mut S3Request<ListObjectVersionsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        authorize_request(req, Action::S3Action(S3Action::ListBucketVersionsAction)).await
    }

    /// Checks whether the ListObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects(&self, req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the ListParts request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_parts(&self, _req: &mut S3Request<ListPartsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_accelerate_configuration(
        &self,
        _req: &mut S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_acl(&self, req: &mut S3Request<PutBucketAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketAclAction)).await
    }

    /// Checks whether the PutBucketAnalyticsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_analytics_configuration(
        &self,
        _req: &mut S3Request<PutBucketAnalyticsConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketCors request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_cors(&self, req: &mut S3Request<PutBucketCorsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketCorsAction)).await
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketEncryptionAction)).await
    }

    /// Checks whether the PutBucketIntelligentTieringConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_intelligent_tiering_configuration(
        &self,
        _req: &mut S3Request<PutBucketIntelligentTieringConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketInventoryConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_inventory_configuration(
        &self,
        _req: &mut S3Request<PutBucketInventoryConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketLifecycleConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: &mut S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketLifecycleAction)).await
    }

    async fn put_bucket_logging(&self, req: &mut S3Request<PutBucketLoggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketLoggingAction)).await
    }

    /// Checks whether the PutBucketMetricsConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_metrics_configuration(&self, _req: &mut S3Request<PutBucketMetricsConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketNotificationConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_notification_configuration(
        &self,
        req: &mut S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketNotificationAction)).await
    }

    /// Checks whether the PutBucketOwnershipControls request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_ownership_controls(&self, _req: &mut S3Request<PutBucketOwnershipControlsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketPolicy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_policy(&self, req: &mut S3Request<PutBucketPolicyInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, put_bucket_policy_authorize_action()).await
    }

    /// Checks whether the PutBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_replication(&self, req: &mut S3Request<PutBucketReplicationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutReplicationConfigurationAction)).await
    }

    /// Checks whether the PutBucketRequestPayment request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_request_payment(&self, _req: &mut S3Request<PutBucketRequestPaymentInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_tagging(&self, req: &mut S3Request<PutBucketTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketTaggingAction)).await
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketVersioningAction)).await
    }

    /// Checks whether the PutBucketWebsite request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_website(&self, _req: &mut S3Request<PutBucketWebsiteInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object(&self, req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        license_check().map_err(|er| s3_error!(AccessDenied, "{:?}", er.to_string()))?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "put_object_acl")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, put_object_acl_authorize_action()).await
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await
    }

    /// Checks whether the PutObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_lock_configuration(&self, req: &mut S3Request<PutObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    async fn put_object_retention(&self, req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;

        // S3 Standard: When bypass_governance header is set, must have s3:BypassGovernanceRetention permission
        if has_bypass_governance_header(&req.headers) {
            authorize_request(req, Action::S3Action(S3Action::BypassGovernanceRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        let tag_conds = self
            .fetch_tag_conditions(&req.input.bucket, &req.input.key, req.input.version_id.as_deref(), "put_object_tagging")
            .await?;
        req.extensions.insert(tag_conds);

        authorize_request(req, Action::S3Action(S3Action::PutObjectTaggingAction)).await
    }

    /// Checks whether the PutPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_public_access_block(&self, req: &mut S3Request<PutPublicAccessBlockInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketPublicAccessBlockAction)).await
    }

    /// Checks whether the RestoreObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn restore_object(&self, req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::RestoreObjectAction)).await
    }

    /// Checks whether the SelectObjectContent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn select_object_content(&self, req: &mut S3Request<SelectObjectContentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the UploadPart request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part(&self, req: &mut S3Request<UploadPartInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the UploadPartCopy request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part_copy(&self, _req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the WriteGetObjectResponse request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn write_get_object_response(&self, _req: &mut S3Request<WriteGetObjectResponseInput>) -> S3Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn get_bucket_policy_uses_get_bucket_policy_action() {
        assert_eq!(get_bucket_policy_authorize_action(), Action::S3Action(S3Action::GetBucketPolicyAction));
    }

    #[test]
    fn get_object_acl_uses_get_object_acl_action() {
        assert_eq!(get_object_acl_authorize_action(), Action::S3Action(S3Action::GetObjectAclAction));
    }

    #[test]
    fn put_bucket_policy_uses_put_bucket_policy_action() {
        assert_eq!(put_bucket_policy_authorize_action(), Action::S3Action(S3Action::PutBucketPolicyAction));
    }

    #[test]
    fn put_object_acl_uses_put_object_acl_action() {
        assert_eq!(put_object_acl_authorize_action(), Action::S3Action(S3Action::PutObjectAclAction));
    }

    /// Object tag conditions must use keys like ExistingObjectTag/<tag-key> so that
    /// bucket policy conditions (e.g. s3:ExistingObjectTag/security) are evaluated correctly.
    #[test]
    fn test_object_tag_conditions_key_format() {
        let mut tags = HashMap::new();
        tags.insert("ExistingObjectTag/security".to_string(), vec!["public".to_string()]);
        tags.insert("ExistingObjectTag/project".to_string(), vec!["webapp".to_string()]);
        let object_tag_conditions = ObjectTagConditions(tags);
        let mut conditions = HashMap::new();
        conditions.insert("delimiter".to_string(), vec!["/".to_string()]);
        for (k, v) in &object_tag_conditions.0 {
            conditions.insert(k.clone(), v.clone());
        }
        assert_eq!(conditions.get("ExistingObjectTag/security"), Some(&vec!["public".to_string()]));
        assert_eq!(conditions.get("ExistingObjectTag/project"), Some(&vec!["webapp".to_string()]));
        assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
    }

    /// When bucket has no policy or policy fetch fails, tag-based check is skipped (returns false).
    #[tokio::test]
    async fn test_bucket_policy_uses_existing_object_tag_no_policy() {
        let result = bucket_policy_uses_existing_object_tag("test-bucket-no-policy-xyz-absent").await;
        assert!(!result, "bucket with no policy should not use ExistingObjectTag");
    }
}
