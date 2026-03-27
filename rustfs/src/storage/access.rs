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
use crate::auth::{check_key_valid, get_condition_values_with_query, get_session_token};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::RemoteAddr;
use metrics::counter;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::policy_sys::PolicySys;
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::BucketOperations;
use rustfs_iam::error::Error as IamError;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicy, BucketPolicyArgs, bucket_policy_uses_existing_object_tag_conditions};
use rustfs_utils::http::AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE;
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, dto::*, s3_error};
use std::collections::HashMap;
use std::sync::OnceLock;
use url::Url;

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

#[derive(Clone, Debug)]
pub(crate) struct PostObjectRequestMarker;

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
pub(crate) struct ObjectTagConditions {
    bucket: String,
    object: String,
    version_id: Option<String>,
    values: HashMap<String, Vec<String>>,
}

impl ObjectTagConditions {
    fn new(bucket: &str, object: &str, version_id: Option<&str>, values: HashMap<String, Vec<String>>) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.map(str::to_string),
            values,
        }
    }

    fn matches(&self, bucket: &str, object: &str, version_id: Option<&str>) -> bool {
        self.bucket == bucket && self.object == object && self.version_id.as_deref() == version_id
    }
}

const AMZ_WRITE_OFFSET_BYTES_HEADER: &str = "x-amz-write-offset-bytes";

fn has_write_offset_bytes_header(headers: &http::HeaderMap) -> bool {
    headers.contains_key(AMZ_WRITE_OFFSET_BYTES_HEADER)
}

/// Returns true if the bucket has a policy that uses `s3:ExistingObjectTag` (or
/// `ExistingObjectTag/...`) conditions. Used to skip fetching object tags when
/// no tag-based policy is in effect.
async fn bucket_policy_uses_existing_object_tag(bucket: &str) -> bool {
    let Ok((policy_str, _)) = metadata_sys::get_bucket_policy_raw(bucket).await else {
        return false;
    };
    bucket_policy_doc_uses_existing_object_tag(&policy_str)
}

fn bucket_policy_doc_uses_existing_object_tag(policy_str: &str) -> bool {
    // Fast path: avoid JSON parsing when the marker does not exist at all.
    if !policy_str.contains("ExistingObjectTag") {
        return false;
    }

    let Ok(policy) = serde_json::from_str::<BucketPolicy>(policy_str) else {
        // Keep previous fail-safe behavior for malformed policy JSON:
        // marker present means we conservatively fetch tags.
        return true;
    };

    bucket_policy_uses_existing_object_tag_conditions(&policy)
}

fn merge_object_tag_conditions(conditions: &mut HashMap<String, Vec<String>>, tags: &HashMap<String, Vec<String>>) {
    for (k, v) in tags {
        conditions
            .entry(k.clone())
            .and_modify(|existing| existing.extend(v.iter().cloned()))
            .or_insert_with(|| v.clone());
    }
}

fn action_tag_metric_label(action: &Action) -> &'static str {
    match action {
        Action::S3Action(S3Action::GetObjectAction) => "get_object",
        Action::S3Action(S3Action::GetObjectAttributesAction) => "get_object_attributes",
        Action::S3Action(S3Action::GetObjectVersionAction) => "get_object_version",
        Action::S3Action(S3Action::GetObjectVersionAttributesAction) => "get_object_version_attributes",
        Action::S3Action(S3Action::GetObjectTaggingAction) => "get_object_tagging",
        Action::S3Action(S3Action::DeleteObjectAction) => "delete_object",
        Action::S3Action(S3Action::DeleteObjectVersionAction) => "delete_object_version",
        Action::S3Action(S3Action::DeleteObjectTaggingAction) => "delete_object_tagging",
        Action::S3Action(S3Action::PutObjectTaggingAction) => "put_object_tagging",
        _ => "authorize",
    }
}

fn auth_fs() -> &'static FS {
    static AUTH_FS: OnceLock<FS> = OnceLock::new();
    AUTH_FS.get_or_init(FS::new)
}

/// Returns true when the owner (root or parent=root credentials) may bypass bucket policy
/// explicit Deny for this action. Per AWS S3, only GetBucketPolicy, PutBucketPolicy, and
/// DeleteBucketPolicy have this bypass so the admin can recover from a misconfigured policy.
pub(crate) fn owner_can_bypass_policy_deny(is_owner: bool, action: &Action) -> bool {
    is_owner
        && matches!(
            action,
            Action::S3Action(S3Action::GetBucketPolicyAction)
                | Action::S3Action(S3Action::PutBucketPolicyAction)
                | Action::S3Action(S3Action::DeleteBucketPolicyAction)
        )
}

/// Authorizes the request based on the action and credentials.
pub async fn authorize_request<T>(req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    let req_info = req_info_ref(req)?;
    let cred = req_info.cred.clone();
    let is_owner = req_info.is_owner;
    let bucket = req_info.bucket.clone().unwrap_or_default();
    let object = req_info.object.clone().unwrap_or_default();
    let version_id = req_info.version_id.clone();

    if let Some(cred) = &cred {
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("authorize_request {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);
        let mut conditions =
            get_condition_values_with_query(&req.headers, cred, version_id.as_deref(), None, remote_addr, req.uri.query());

        let action_args = Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action,
            bucket: bucket.as_str(),
            conditions: &conditions,
            is_owner,
            object: object.as_str(),
            claims,
            deny_only: false,
        };
        let prepared = iam_store.prepare_auth(&action_args).await;
        let needs_tag_from_iam = prepared.needs_existing_object_tag;

        let needs_tag_from_bucket =
            !bucket.is_empty() && !object.is_empty() && bucket_policy_uses_existing_object_tag(&bucket).await;
        let needs_tag = needs_tag_from_iam || needs_tag_from_bucket;
        if needs_tag && !bucket.is_empty() && !object.is_empty() {
            let mut tag_conditions = None;
            if let Some(cached) = req.extensions.get::<ObjectTagConditions>()
                && cached.matches(&bucket, &object, version_id.as_deref())
            {
                tag_conditions = Some(cached.values.clone());
            }

            if tag_conditions.is_none() {
                counter!("rustfs.object_tag_conditions.fetched", "op" => action_tag_metric_label(&action)).increment(1);
                let fetched = auth_fs()
                    .get_object_tag_conditions_for_policy(&bucket, &object, version_id.as_deref())
                    .await?;
                req.extensions
                    .insert(ObjectTagConditions::new(&bucket, &object, version_id.as_deref(), fetched.clone()));
                tag_conditions = Some(fetched);
            }

            if let Some(tags) = tag_conditions {
                merge_object_tag_conditions(&mut conditions, &tags);
            }
        } else {
            counter!("rustfs.object_tag_conditions.skipped", "op" => action_tag_metric_label(&action)).increment(1);
        }
        let bucket_name = bucket.as_str();

        // Per AWS S3: root can always perform GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy
        // even if bucket policy explicitly denies. Other actions (ListBucket, GetObject, etc.) are
        // subject to bucket policy Deny for root as well. See: repost.aws/knowledge-center/s3-accidentally-denied-access
        // Here "owner" means root or credentials whose parent_user is root (e.g. Console admin via STS).
        let owner_can_bypass_deny = owner_can_bypass_policy_deny(is_owner, &action);
        if !bucket_name.is_empty()
            && !owner_can_bypass_deny
            && !PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket_name,
                action,
                // Early explicit-deny gate for bucket policy: use owner short-circuit path so
                // deny statements are enforced before IAM/bucket allow fallback evaluation.
                is_owner: true,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: object.as_str(),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        if action == Action::S3Action(S3Action::DeleteObjectAction) && version_id.is_some() {
            let delete_version_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            let delete_version_allowed = iam_store.eval_prepared(&prepared, &delete_version_args).await;
            if !delete_version_allowed
                && !PolicySys::is_allowed(&BucketPolicyArgs {
                    bucket: bucket.as_str(),
                    action: Action::S3Action(S3Action::DeleteObjectVersionAction),
                    is_owner,
                    account: &cred.access_key,
                    groups: &cred.groups,
                    conditions: &conditions,
                    object: object.as_str(),
                })
                .await
            {
                return Err(s3_error!(AccessDenied, "Access Denied"));
            }
        }

        let iam_allowed = {
            let final_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action,
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            iam_store.eval_prepared(&prepared, &final_args).await
        };

        if iam_allowed {
            return Ok(());
        }

        let policy_allowed_fallback = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: bucket.as_str(),
            action,
            is_owner,
            account: &cred.access_key,
            groups: &cred.groups,
            conditions: &conditions,
            object: object.as_str(),
        })
        .await;

        if policy_allowed_fallback {
            return Ok(());
        }

        if action == Action::S3Action(S3Action::ListBucketVersionsAction) {
            let list_bucket_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::S3Action(S3Action::ListBucketAction),
                bucket: bucket.as_str(),
                conditions: &conditions,
                is_owner,
                object: object.as_str(),
                claims,
                deny_only: false,
            };
            let list_bucket_allowed = iam_store.eval_prepared(&prepared, &list_bucket_args).await;
            if list_bucket_allowed {
                return Ok(());
            }

            if PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket.as_str(),
                action: Action::S3Action(S3Action::ListBucketAction),
                is_owner,
                account: &cred.access_key,
                groups: &cred.groups,
                conditions: &conditions,
                object: object.as_str(),
            })
            .await
            {
                return Ok(());
            }
        }
    } else {
        let default_cred = rustfs_credentials::Credentials::default();
        let mut conditions = get_condition_values_with_query(
            &req.headers,
            &default_cred,
            version_id.as_deref(),
            req.region.clone(),
            remote_addr,
            req.uri.query(),
        );

        let needs_tag_from_bucket =
            !bucket.is_empty() && !object.is_empty() && bucket_policy_uses_existing_object_tag(&bucket).await;
        if needs_tag_from_bucket {
            let mut tag_conditions = None;
            if let Some(cached) = req.extensions.get::<ObjectTagConditions>()
                && cached.matches(&bucket, &object, version_id.as_deref())
            {
                tag_conditions = Some(cached.values.clone());
            }
            if tag_conditions.is_none() {
                counter!("rustfs.object_tag_conditions.fetched", "op" => action_tag_metric_label(&action)).increment(1);
                let fetched = auth_fs()
                    .get_object_tag_conditions_for_policy(&bucket, &object, version_id.as_deref())
                    .await?;
                req.extensions
                    .insert(ObjectTagConditions::new(&bucket, &object, version_id.as_deref(), fetched.clone()));
                tag_conditions = Some(fetched);
            }
            if let Some(tags) = tag_conditions {
                merge_object_tag_conditions(&mut conditions, &tags);
            }
        } else {
            counter!("rustfs.object_tag_conditions.skipped", "op" => action_tag_metric_label(&action)).increment(1);
        }
        let bucket_name = bucket.as_str();

        if !bucket_name.is_empty()
            && !PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket_name,
                action,
                // Early explicit-deny gate for bucket policy in anonymous path.
                is_owner: true,
                account: "",
                groups: &None,
                conditions: &conditions,
                object: object.as_str(),
            })
            .await
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        if action != Action::S3Action(S3Action::ListAllMyBucketsAction) {
            let policy_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
                bucket: bucket.as_str(),
                action,
                is_owner: false,
                account: "",
                groups: &None,
                conditions: &conditions,
                object: object.as_str(),
            })
            .await;

            if policy_allowed {
                // RestrictPublicBuckets: when true, deny public access even if bucket policy allows it.
                match metadata_sys::get_public_access_block_config(bucket_name).await {
                    Ok((config, _)) => {
                        if config.restrict_public_buckets.unwrap_or(false) {
                            return Err(s3_error!(AccessDenied, "Access Denied"));
                        }
                    }
                    Err(StorageError::ConfigNotFound) => {}
                    Err(_) => {
                        return Err(s3_error!(AccessDenied, "Access Denied"));
                    }
                }
                return Ok(());
            }

            if action == Action::S3Action(S3Action::ListBucketVersionsAction)
                && PolicySys::is_allowed(&BucketPolicyArgs {
                    bucket: bucket.as_str(),
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

fn legal_hold_write_requested(object_lock_legal_hold_status: Option<&ObjectLockLegalHoldStatus>) -> bool {
    object_lock_legal_hold_status.is_some()
}

fn retention_write_requested(
    object_lock_mode: Option<&ObjectLockMode>,
    object_lock_retain_until_date: Option<&Timestamp>,
) -> bool {
    object_lock_mode.is_some() || object_lock_retain_until_date.is_some()
}

fn get_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::GetBucketPolicyAction)
}

fn put_bucket_policy_authorize_action() -> Action {
    Action::S3Action(S3Action::PutBucketPolicyAction)
}

fn post_object_authorize_action() -> Action {
    Action::S3Action(S3Action::PutObjectAction)
}

fn complete_multipart_upload_authorize_action() -> Action {
    Action::S3Action(S3Action::PutObjectAction)
}

fn list_parts_authorize_action() -> Action {
    Action::S3Action(S3Action::ListMultipartUploadPartsAction)
}

fn validate_post_object_success_controls(input: &PostObjectInput) -> S3Result<()> {
    if let Some(status) = input.success_action_status
        && !matches!(status, 200 | 201 | 204)
    {
        return Err(s3_error!(MalformedPOSTRequest, "success_action_status must be one of 200, 201, or 204"));
    }

    if let Some(redirect) = input.success_action_redirect.as_deref().map(str::trim)
        && !redirect.is_empty()
        && Url::parse(redirect).is_err()
    {
        return Err(s3_error!(MalformedPOSTRequest, "success_action_redirect must be a valid absolute URL"));
    }

    Ok(())
}

#[async_trait::async_trait]
impl S3Access for FS {
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
        license_check().map_err(|er| match er.kind() {
            std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
            _ => {
                tracing::error!("license check failed due to unexpected error: {er}");
                s3_error!(InternalError, "License validation failed")
            }
        })?;

        // Verify uniformly here? Or verify separately below?

        Ok(())
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
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
    async fn abort_multipart_upload(&self, req: &mut S3Request<AbortMultipartUploadInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::AbortMultipartUploadAction)).await
    }

    /// Checks whether the CompleteMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn complete_multipart_upload(&self, req: &mut S3Request<CompleteMultipartUploadInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, complete_multipart_upload_authorize_action()).await
    }

    /// Checks whether the CopyObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        {
            let (src_bucket, src_key, version_id) = match &req.input.copy_source {
                CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Bucket { bucket, key, version_id } => {
                    (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
                }
            };

            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(src_bucket.clone());
            req_info.object = Some(src_key.clone());
            req_info.version_id = version_id.clone();

            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        let req_info = ext_req_info_mut(&mut req.extensions)?;

        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    async fn create_multipart_upload(&self, req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        Ok(())
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
    async fn delete_bucket_website(&self, req: &mut S3Request<DeleteBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
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
        req: &mut S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
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
    async fn get_bucket_request_payment(&self, req: &mut S3Request<GetBucketRequestPaymentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
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
    async fn get_bucket_website(&self, req: &mut S3Request<GetBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
    }

    /// Checks whether the GetObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object(&self, req: &mut S3Request<GetObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

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

        authorize_request(req, Action::S3Action(S3Action::GetObjectAclAction)).await
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

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
    async fn list_parts(&self, req: &mut S3Request<ListPartsInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, list_parts_authorize_action()).await
    }

    /// Checks whether the PostObject request has accesses to the resources.
    async fn post_object(&self, req: &mut S3Request<PostObjectInput>) -> S3Result<()> {
        validate_post_object_success_controls(&req.input)?;

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();
        req.extensions.insert(PostObjectRequestMarker);

        authorize_request(req, post_object_authorize_action()).await
    }

    /// Checks whether the PutBucketAccelerateConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_accelerate_configuration(
        &self,
        req: &mut S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
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
    async fn put_bucket_request_payment(&self, req: &mut S3Request<PutBucketRequestPaymentInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
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
    async fn put_bucket_website(&self, req: &mut S3Request<PutBucketWebsiteInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
    }

    /// Checks whether the PutObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object(&self, req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        if has_write_offset_bytes_header(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::NotImplemented,
                ApiError::error_code_to_message(&S3ErrorCode::NotImplemented),
            ));
        }

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;

        if legal_hold_write_requested(req.input.object_lock_legal_hold_status.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
        }

        if retention_write_requested(req.input.object_lock_mode.as_ref(), req.input.object_lock_retain_until_date.as_ref()) {
            authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
        }

        Ok(())
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAclAction)).await
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
    async fn upload_part_copy(&self, req: &mut S3Request<UploadPartCopyInput>) -> S3Result<()> {
        {
            let (src_bucket, src_key, version_id) = match &req.input.copy_source {
                CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Bucket { bucket, key, version_id } => {
                    (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
                }
            };

            let req_info = ext_req_info_mut(&mut req.extensions)?;
            req_info.bucket = Some(src_bucket.clone());
            req_info.object = Some(src_key.clone());
            req_info.version_id = version_id.clone();

            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        let req_info = ext_req_info_mut(&mut req.extensions)?;
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = None;

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
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
    use http::{HeaderMap, Method, Uri};
    use std::collections::HashMap;
    use time::OffsetDateTime;

    #[test]
    fn get_bucket_policy_uses_get_bucket_policy_action() {
        assert_eq!(get_bucket_policy_authorize_action(), Action::S3Action(S3Action::GetBucketPolicyAction));
    }

    #[test]
    fn put_bucket_policy_uses_put_bucket_policy_action() {
        assert_eq!(put_bucket_policy_authorize_action(), Action::S3Action(S3Action::PutBucketPolicyAction));
    }

    #[test]
    fn post_object_uses_put_object_action() {
        assert_eq!(post_object_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
    }

    #[test]
    fn complete_multipart_upload_uses_put_object_action() {
        assert_eq!(complete_multipart_upload_authorize_action(), Action::S3Action(S3Action::PutObjectAction));
    }

    #[test]
    fn list_parts_uses_list_multipart_upload_parts_action() {
        assert_eq!(list_parts_authorize_action(), Action::S3Action(S3Action::ListMultipartUploadPartsAction));
    }

    #[test]
    fn legal_hold_write_requested_is_true_when_status_present() {
        assert!(legal_hold_write_requested(Some(&ObjectLockLegalHoldStatus::from_static(
            ObjectLockLegalHoldStatus::ON
        ))));
        assert!(!legal_hold_write_requested(None));
    }

    #[test]
    fn retention_write_requested_is_true_when_mode_or_date_present() {
        let retain_until = OffsetDateTime::now_utc().into();

        assert!(retention_write_requested(
            Some(&ObjectLockMode::from_static(ObjectLockMode::GOVERNANCE)),
            None
        ));
        assert!(retention_write_requested(None, Some(&retain_until)));
        assert!(!retention_write_requested(None, None));
    }

    #[test]
    fn validate_post_object_success_controls_accepts_supported_status_codes() {
        for status in [200, 201, 204] {
            let input = PostObjectInput::builder()
                .bucket("test-bucket".to_string())
                .key("test-key".to_string())
                .success_action_status(Some(status))
                .build()
                .expect("post object input should build");
            assert!(
                validate_post_object_success_controls(&input).is_ok(),
                "status {status} should be accepted"
            );
        }
    }

    #[test]
    fn validate_post_object_success_controls_rejects_invalid_status_code() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_status(Some(202))
            .build()
            .expect("post object input should build");

        let err = validate_post_object_success_controls(&input).expect_err("status 202 should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::MalformedPOSTRequest);
    }

    #[test]
    fn validate_post_object_success_controls_accepts_empty_redirect() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_redirect(Some("".to_string()))
            .build()
            .expect("post object input should build");
        assert!(validate_post_object_success_controls(&input).is_ok());
    }

    #[test]
    fn validate_post_object_success_controls_rejects_invalid_redirect() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .success_action_redirect(Some("://invalid-url".to_string()))
            .build()
            .expect("post object input should build");

        let err = validate_post_object_success_controls(&input).expect_err("invalid redirect should be rejected");
        assert_eq!(err.code(), &S3ErrorCode::MalformedPOSTRequest);
    }

    /// Object tag conditions must use keys like ExistingObjectTag/<tag-key> so that
    /// bucket policy conditions (e.g. s3:ExistingObjectTag/security) are evaluated correctly.
    #[test]
    fn test_object_tag_conditions_key_format() {
        let mut tags = HashMap::new();
        tags.insert("ExistingObjectTag/security".to_string(), vec!["public".to_string()]);
        tags.insert("ExistingObjectTag/project".to_string(), vec!["webapp".to_string()]);
        let object_tag_conditions = ObjectTagConditions::new("bucket", "object", None, tags);
        let mut conditions = HashMap::new();
        conditions.insert("delimiter".to_string(), vec!["/".to_string()]);
        for (k, v) in &object_tag_conditions.values {
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

    #[test]
    fn test_bucket_policy_doc_uses_existing_object_tag_matches_condition_keys_only() {
        let condition_key_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#;
        assert!(
            bucket_policy_doc_uses_existing_object_tag(condition_key_policy),
            "ExistingObjectTag in condition key must be detected"
        );

        let value_only_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:prefix":"ExistingObjectTag/security"}}
  }]
}"#;
        assert!(
            !bucket_policy_doc_uses_existing_object_tag(value_only_policy),
            "ExistingObjectTag text in values should not trigger tag dependency"
        );
    }

    #[test]
    fn test_bucket_policy_doc_uses_existing_object_tag_malformed_fallback() {
        let malformed_with_marker = r#"{"Version":"2012-10-17","Statement":[INVALID,"ExistingObjectTag"]}"#;
        assert!(
            bucket_policy_doc_uses_existing_object_tag(malformed_with_marker),
            "malformed JSON with marker should conservatively require tag fetch"
        );
    }

    /// Owner can bypass bucket policy Deny only for the three policy management APIs (per AWS S3).
    #[test]
    fn test_owner_can_bypass_policy_deny_only_for_policy_apis() {
        // Owner + policy management actions -> bypass allowed
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetBucketPolicyAction)));
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::PutBucketPolicyAction)));
        assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::DeleteBucketPolicyAction)));

        // Owner + other actions -> no bypass (still subject to bucket policy Deny)
        assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::ListBucketAction)));
        assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetObjectAction)));

        // Non-owner -> no bypass for any action
        assert!(!owner_can_bypass_policy_deny(false, &Action::S3Action(S3Action::GetBucketPolicyAction)));
        assert!(!owner_can_bypass_policy_deny(
            false,
            &Action::S3Action(S3Action::DeleteBucketPolicyAction)
        ));
    }

    #[tokio::test]
    async fn post_object_marks_request_extensions() {
        let input = PostObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .expect("post object input should build");

        let mut req = S3Request {
            input,
            method: Method::POST,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.extensions.insert(ReqInfo::default());

        let fs = FS::new();
        let _ = fs.post_object(&mut req).await;

        assert!(
            req.extensions.get::<PostObjectRequestMarker>().is_some(),
            "post object request should carry the marker for downstream handling"
        );
    }

    #[test]
    fn write_offset_bytes_header_detection_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Amz-Write-Offset-Bytes", http::HeaderValue::from_static("0"));

        assert!(has_write_offset_bytes_header(&headers));
    }
}
