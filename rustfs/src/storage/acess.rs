use std::str::FromStr;

use super::ecfs::FS;

use ecstore::bucket::policy::action::Action;
use http::HeaderMap;
use s3s::access::{S3Access, S3AccessContext};

use s3s::auth::Credentials;
use s3s::{dto::*, s3_error, S3Request, S3Result};
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
struct ReqInfo {
    pub card: Option<Credentials>,
    pub action: Option<Action>,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub version_id: Option<Uuid>,
}

async fn authorize_request(_req: &ReqInfo, _hs: &HeaderMap, _action: Action) -> S3Result<()> {
    // TODO: globalIAMSys.IsAllowed
    Ok(())
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
        // 上层验证了 ak/sk
        // warn!("check s3_op {:?} cred {:?}", cx.s3_op().name(), cx.credentials());

        let action = match Action::from_str(format!("s3:{}", cx.s3_op().name()).as_str()) {
            Ok(res) => Some(res),
            Err(_) => None,
        };

        let req_info = ReqInfo {
            card: cx.credentials().cloned(),
            action,
            ..Default::default()
        };

        // warn!("req_info {:?}", req_info);

        let ext = cx.extensions_mut();
        ext.insert(req_info);

        // 统一在这验证？还是在下面各自验证？

        Ok(())
    }

    /// Checks whether the CreateBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_bucket(&self, req: &mut S3Request<CreateBucketInput>) -> S3Result<()> {
        if let Some(req_info) = req.extensions.get_mut::<ReqInfo>() {
            let CreateBucketInput { bucket, .. } = &req.input;
            req_info.bucket = Some(bucket.to_owned());

            authorize_request(req_info, &req.headers, Action::CreateBucket).await
        } else {
            Err(s3_error!(AccessDenied, "AccessDenied"))
        }
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
    async fn copy_object(&self, _req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the CreateMultipartUpload request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn create_multipart_upload(&self, _req: &mut S3Request<CreateMultipartUploadInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucket request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket(&self, _req: &mut S3Request<DeleteBucketInput>) -> S3Result<()> {
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
    async fn delete_bucket_cors(&self, _req: &mut S3Request<DeleteBucketCorsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_encryption(&self, _req: &mut S3Request<DeleteBucketEncryptionInput>) -> S3Result<()> {
        Ok(())
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
    async fn delete_bucket_lifecycle(&self, _req: &mut S3Request<DeleteBucketLifecycleInput>) -> S3Result<()> {
        Ok(())
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
    async fn delete_bucket_policy(&self, _req: &mut S3Request<DeleteBucketPolicyInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_replication(&self, _req: &mut S3Request<DeleteBucketReplicationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteBucketTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_bucket_tagging(&self, _req: &mut S3Request<DeleteBucketTaggingInput>) -> S3Result<()> {
        Ok(())
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
    async fn delete_object(&self, _req: &mut S3Request<DeleteObjectInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the DeleteObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn delete_object_tagging(&self, _req: &mut S3Request<DeleteObjectTaggingInput>) -> S3Result<()> {
        Ok(())
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
    async fn get_bucket_acl(&self, _req: &mut S3Request<GetBucketAclInput>) -> S3Result<()> {
        Ok(())
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
    async fn get_bucket_cors(&self, _req: &mut S3Request<GetBucketCorsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_encryption(&self, _req: &mut S3Request<GetBucketEncryptionInput>) -> S3Result<()> {
        Ok(())
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
        _req: &mut S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketLocation request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_location(&self, _req: &mut S3Request<GetBucketLocationInput>) -> S3Result<()> {
        Ok(())
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
        _req: &mut S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
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
    async fn get_bucket_policy(&self, _req: &mut S3Request<GetBucketPolicyInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketPolicyStatus request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_policy_status(&self, _req: &mut S3Request<GetBucketPolicyStatusInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_replication(&self, _req: &mut S3Request<GetBucketReplicationInput>) -> S3Result<()> {
        Ok(())
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
    async fn get_bucket_tagging(&self, _req: &mut S3Request<GetBucketTaggingInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_bucket_versioning(&self, _req: &mut S3Request<GetBucketVersioningInput>) -> S3Result<()> {
        Ok(())
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
    async fn get_object(&self, _req: &mut S3Request<GetObjectInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_acl(&self, _req: &mut S3Request<GetObjectAclInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectAttributes request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_attributes(&self, _req: &mut S3Request<GetObjectAttributesInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_legal_hold(&self, _req: &mut S3Request<GetObjectLegalHoldInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_lock_configuration(&self, _req: &mut S3Request<GetObjectLockConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_retention(&self, _req: &mut S3Request<GetObjectRetentionInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the GetObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn get_object_tagging(&self, _req: &mut S3Request<GetObjectTaggingInput>) -> S3Result<()> {
        Ok(())
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
    async fn head_bucket(&self, _req: &mut S3Request<HeadBucketInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the HeadObject request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn head_object(&self, _req: &mut S3Request<HeadObjectInput>) -> S3Result<()> {
        Ok(())
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
        Ok(())
    }

    /// Checks whether the ListMultipartUploads request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_multipart_uploads(&self, _req: &mut S3Request<ListMultipartUploadsInput>) -> S3Result<()> {
        Ok(())
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
    async fn list_objects(&self, _req: &mut S3Request<ListObjectsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the ListObjectsV2 request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn list_objects_v2(&self, _req: &mut S3Request<ListObjectsV2Input>) -> S3Result<()> {
        Ok(())
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
    async fn put_bucket_acl(&self, _req: &mut S3Request<PutBucketAclInput>) -> S3Result<()> {
        Ok(())
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
    async fn put_bucket_cors(&self, _req: &mut S3Request<PutBucketCorsInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketEncryption request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_encryption(&self, _req: &mut S3Request<PutBucketEncryptionInput>) -> S3Result<()> {
        Ok(())
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
        _req: &mut S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
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
        _req: &mut S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<()> {
        Ok(())
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
    async fn put_bucket_policy(&self, _req: &mut S3Request<PutBucketPolicyInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketReplication request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_replication(&self, _req: &mut S3Request<PutBucketReplicationInput>) -> S3Result<()> {
        Ok(())
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
    async fn put_bucket_tagging(&self, _req: &mut S3Request<PutBucketTaggingInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutBucketVersioning request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_bucket_versioning(&self, _req: &mut S3Request<PutBucketVersioningInput>) -> S3Result<()> {
        Ok(())
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
    async fn put_object(&self, _req: &mut S3Request<PutObjectInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObjectAcl request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_acl(&self, _req: &mut S3Request<PutObjectAclInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObjectLegalHold request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_legal_hold(&self, _req: &mut S3Request<PutObjectLegalHoldInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObjectLockConfiguration request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_lock_configuration(&self, _req: &mut S3Request<PutObjectLockConfigurationInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObjectRetention request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_retention(&self, _req: &mut S3Request<PutObjectRetentionInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the PutObjectTagging request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn put_object_tagging(&self, _req: &mut S3Request<PutObjectTaggingInput>) -> S3Result<()> {
        Ok(())
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
    async fn restore_object(&self, _req: &mut S3Request<RestoreObjectInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the SelectObjectContent request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn select_object_content(&self, _req: &mut S3Request<SelectObjectContentInput>) -> S3Result<()> {
        Ok(())
    }

    /// Checks whether the UploadPart request has accesses to the resources.
    ///
    /// This method returns `Ok(())` by default.
    async fn upload_part(&self, _req: &mut S3Request<UploadPartInput>) -> S3Result<()> {
        Ok(())
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
