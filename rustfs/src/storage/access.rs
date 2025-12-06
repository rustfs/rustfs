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
use crate::auth::{check_key_valid, get_condition_values, get_session_token};
use crate::license::license_check;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::policy_sys::PolicySys;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::ObjectOptions;
use rustfs_iam::error::Error as IamError;
use rustfs_policy::auth;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicyArgs};
use s3s::access::{S3Access, S3AccessContext};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, dto::*, s3_error};
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Default, Clone)]
pub(crate) struct ReqInfo {
    pub cred: Option<auth::Credentials>,
    pub is_owner: bool,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub version_id: Option<String>,
    pub region: Option<String>,
}

/// Checks if a bucket allows access based on its ACL for list operations
async fn is_bucket_list_accessible(bucket: &str, is_authenticated: bool) -> bool {
    // Get the bucket ACL
    let acl_result = metadata_sys::get_bucket_acl_config(bucket).await;
    let acl = match acl_result {
        Ok((acl_str, _timestamp)) => acl_str,
        Err(_) => {
            "private".to_string() // Default to private
        }
    };

    // Check ACL type for list access:
    // - Private: Only owner can list (handled elsewhere)
    // - PublicRead: Anyone can list
    // - PublicReadWrite: Anyone can list
    // - AuthenticatedRead: Authenticated users can list
    match acl.as_str() {
        "public-read" | "public-read-write" => true, // Public ACLs allow anyone to list
        "authenticated-read" => is_authenticated,    // AuthenticatedRead allows authenticated users to list
        "private" => false,                          // Private only allows owner (handled elsewhere)
        _ => false,
    }
}

/// Checks if an object allows access based on its ACL (unified for both authenticated and anonymous users)
async fn check_object_acl_unified(
    bucket: &str,
    object: &str,
    version_id: Option<&str>,
    is_write: bool,
    is_authenticated: bool,
) -> bool {
    let Some(store) = new_object_layer_fn() else {
        return false;
    };
    let mut opts = ObjectOptions::default();
    if let Some(vid) = version_id {
        opts.version_id = Some(vid.to_string());
    }

    let object_info = match store.get_object_info(bucket, object, &opts).await {
        Ok(info) => info,
        Err(_) => {
            return false; // Object does not exist, cannot check object ACL
        }
    };

    if let Some(acl) = object_info.user_defined.get("x-amz-acl") {
        match (acl.as_str(), is_write, is_authenticated) {
            ("private", _, _) => false,
            ("public-read-write", true, _) => true,
            ("public-read-write", false, _) => true,
            ("public-read", false, _) => true,
            ("authenticated-read", false, true) => true,
            _ => false,
        }
    } else {
        false // Default private
    }
}

/// Checks if a bucket allows to write access based on its ACL
async fn check_bucket_acl_allow_write(bucket: &str) -> bool {
    let acl_result = metadata_sys::get_bucket_acl_config(bucket).await;
    let acl = match acl_result {
        Ok((s, _)) => s,
        Err(_) => "private".to_string(),
    };
    acl == "public-read-write"
}

/// Authorizes the request based on the action and credentials.
pub async fn authorize_request<T>(req: &mut S3Request<T>, action: Action) -> S3Result<()> {
    let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");

    // Owner channel
    if req_info.is_owner {
        return Ok(());
    }

    let bucket = req_info.bucket.as_deref().unwrap_or("");
    let object = req_info.object.as_deref().unwrap_or("");

    // Prepare Bucket Policy parameters (shared by IAM and Policy)
    let anon_cred = auth::Credentials::default();
    let cred_ref = req_info.cred.as_ref().unwrap_or(&anon_cred);
    let conditions = get_condition_values(&req.headers, cred_ref, req_info.version_id.as_deref(), req.region.as_deref());

    // IAM check (only for authenticated users)
    if let Some(cred) = &req_info.cred {
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("authorize_request {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);

        // Check DeleteObjectVersion
        if action == Action::S3Action(S3Action::DeleteObjectAction) && req_info.version_id.is_some() {
            let delete_version_allowed = iam_store
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
                .await;

            if !delete_version_allowed {
                return Err(s3_error!(AccessDenied, "Access Denied"));
            }
        }

        // Check regular operations
        let iam_allowed = iam_store
            .is_allowed(&Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action,
                bucket,
                object,
                conditions: &conditions,
                is_owner: false,
                claims,
                deny_only: false,
            })
            .await;

        if iam_allowed {
            return Ok(());
        }
    }

    // Bucket Policy check
    if action != Action::S3Action(S3Action::ListAllMyBucketsAction) {
        let policy_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket,
            action,
            is_owner: false,
            account: req_info.cred.as_ref().map(|c| c.access_key.as_str()).unwrap_or(""),
            groups: &None,
            conditions: &conditions,
            object,
        })
        .await;

        if policy_allowed {
            return Ok(());
        }
    }

    // 4. ACL fallback (fallback mechanism)
    if !bucket.is_empty() {
        // ListBucket check Bucket ACL
        if action == Action::S3Action(S3Action::ListBucketAction) {
            let acl_result = is_bucket_list_accessible(bucket, req_info.cred.is_some()).await;
            if acl_result {
                return Ok(());
            }
        }

        // Read/Write objects check Object ACL / Bucket ACL
        if !object.is_empty()
            && (action == Action::S3Action(S3Action::GetObjectAction) || action == Action::S3Action(S3Action::PutObjectAction))
        {
            let is_write = action == Action::S3Action(S3Action::PutObjectAction);

            // Check object ACL (for existing objects)
            let object_acl_result =
                check_object_acl_unified(bucket, object, req_info.version_id.as_deref(), is_write, req_info.cred.is_some()).await;
            if object_acl_result {
                return Ok(());
            }

            // Check Bucket ACL (for new objects / PublicReadWrite Bucket)
            if is_write {
                let bucket_acl_result = check_bucket_acl_allow_write(bucket).await;
                if bucket_acl_result {
                    return Ok(());
                }
            }
        }
    }

    Err(s3_error!(AccessDenied, "Access Denied"))
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

        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
            let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
            let (src_bucket, src_key, version_id) = match &req.input.copy_source {
                CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
                CopySource::Bucket { bucket, key, version_id } => {
                    (bucket.to_string(), key.to_string(), version_id.as_ref().map(|v| v.to_string()))
                }
            };

            req_info.bucket = Some(src_bucket);
            req_info.object = Some(src_key);
            req_info.version_id = version_id;

            authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await?;
        }

        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");

        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_multipart_upload(&self, _req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        license_check().map_err(|er| s3_error!(AccessDenied, "{:?}", er.to_string()))?;
        Ok(())
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketCorsAction)).await
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::DeleteBucketPolicyAction)).await
    }

    /// Checks whether the DeleteBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_replication(&self, req: &mut S3Request<DeleteBucketReplicationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutReplicationConfigurationAction)).await
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectAction)).await
    }

    /// Checks whether the DeleteObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object_tagging(&self, req: &mut S3Request<DeleteObjectTaggingInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::DeleteObjectTaggingAction)).await
    }

    /// Checks whether the DeleteObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_objects(&self, _req: &mut S3Request<DeleteObjectsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeletePublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_public_access_block(&self, _req: &mut S3Request<DeletePublicAccessBlockInput>) -> S3Result<()> {
        Ok(())
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketCorsAction)).await
    }

    /// Checks whether the GetBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_encryption(&self, req: &mut S3Request<GetBucketEncryptionInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLifecycleAction)).await
    }

    /// Checks whether the GetBucketLocation request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_location(&self, req: &mut S3Request<GetBucketLocationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketLocationAction)).await
    }

    /// Checks whether the GetBucketLogging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_logging(&self, _req: &mut S3Request<GetBucketLoggingInput>) -> S3Result<()> {
        Ok(())
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyAction)).await
    }

    /// Checks whether the GetBucketPolicyStatus request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy_status(&self, req: &mut S3Request<GetBucketPolicyStatusInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketPolicyStatusAction)).await
    }

    /// Checks whether the GetBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_replication(&self, req: &mut S3Request<GetBucketReplicationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketTaggingAction)).await
    }

    /// Checks whether the GetBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_versioning(&self, req: &mut S3Request<GetBucketVersioningInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectAclAction)).await
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectLegalHoldAction)).await
    }

    /// Checks whether the GetObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_lock_configuration(&self, req: &mut S3Request<GetObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::GetBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the GetObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_retention(&self, req: &mut S3Request<GetObjectRetentionInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::GetObjectRetentionAction)).await
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
    async fn get_public_access_block(&self, _req: &mut S3Request<GetPublicAccessBlockInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the HeadBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_bucket(&self, req: &mut S3Request<HeadBucketInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketMultipartUploadsAction)).await
    }

    /// Checks whether the ListObjectVersions request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_object_versions(&self, _req: &mut S3Request<ListObjectVersionsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListObjects request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects(&self, req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::ListBucketAction)).await
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketCorsAction)).await
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketLifecycleAction)).await
    }

    /// Checks whether the PutBucketLogging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_logging(&self, _req: &mut S3Request<PutBucketLoggingInput>) -> S3Result<()> {
        Ok(())
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketPolicyAction)).await
    }

    /// Checks whether the PutBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_replication(&self, req: &mut S3Request<PutBucketReplicationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketTaggingAction)).await
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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

        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectAclAction)).await
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await
    }

    /// Checks whether the PutObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_lock_configuration(&self, req: &mut S3Request<PutObjectLockConfigurationInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());

        authorize_request(req, Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction)).await
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_retention(&self, req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::PutObjectTaggingAction)).await
    }

    /// Checks whether the PutPublicAccessBlock request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_public_access_block(&self, _req: &mut S3Request<PutPublicAccessBlockInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the RestoreObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn restore_object(&self, req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());
        req_info.version_id = req.input.version_id.clone();

        authorize_request(req, Action::S3Action(S3Action::RestoreObjectAction)).await
    }

    /// Checks whether the SelectObjectContent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn select_object_content(&self, req: &mut S3Request<SelectObjectContentInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
        req_info.bucket = Some(req.input.bucket.clone());
        req_info.object = Some(req.input.key.clone());

        authorize_request(req, Action::S3Action(S3Action::GetObjectAction)).await
    }

    /// Checks whether the UploadPart request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part(&self, req: &mut S3Request<UploadPartInput>) -> S3Result<()> {
        let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
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
