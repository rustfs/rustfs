// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded in-process object access with explicit resource and request identity.

use super::put::{PutObjectLockInput, PutObjectOrigin, PutObjectSseInput, PutObjectWriteRequest};
use super::*;
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::BucketResolveError;
use crate::app::storage_api::managed::{
    self, BucketIdentityError, BucketIncarnationFenceGuard, BucketKind, ManagedObjectAccess, ManagedObjectOperation,
};
use crate::app::storage_api::multipart_usecase::contract::multipart::CompletePart;
use crate::app::storage_api::object_usecase::sse::authorize_sse_kms_multipart_write;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManagedEncryption {
    Unencrypted,
    Aes256,
    AwsKms { key_id: String },
}

#[derive(Clone)]
pub(crate) struct ManagedObjectContext {
    pub(super) access: ManagedObjectAccess,
    pub(super) identity: ManagedObjectIdentity,
    encryption: ManagedEncryption,
}

#[derive(Clone)]
pub(super) enum ManagedObjectIdentity {
    Request {
        request: Arc<RequestEnvelope>,
        principal: Box<SseKmsPrincipal>,
    },
    Maintenance {
        context: Arc<super::managed_maintenance::ManagedMaintenanceContext>,
    },
}

pub(crate) struct ManagedMultipartUpload {
    context: ManagedObjectContext,
    key: String,
    upload_id: String,
    lifecycle_guard: Arc<BucketIncarnationFenceGuard>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ManagedObjectError {
    #[error(transparent)]
    Scope(#[from] BucketIdentityError),
    #[error(transparent)]
    Bucket(#[from] BucketResolveError),
    #[error("managed operation requires an authenticated request principal")]
    MissingPrincipal,
    #[error("invalid managed object range")]
    InvalidRange,
    #[error("managed object exceeds its read budget")]
    SizeLimitExceeded,
    #[error("managed object has no body revision")]
    MissingBodyRevision,
    #[error("managed object body revision changed")]
    BodyRevisionMismatch,
    #[error("managed object encryption differs from its resource configuration")]
    EncryptionMismatch,
    #[error(transparent)]
    Api(#[from] ApiError),
}

impl From<rustfs_s3_types::S3Error> for ManagedObjectError {
    fn from(error: rustfs_s3_types::S3Error) -> Self {
        Self::Api(ApiError {
            code: error.code().clone(),
            message: error.message().unwrap_or_default().to_owned(),
            source: Some(Box::new(error)),
        })
    }
}

impl ManagedObjectContext {
    /// Called only after the resource service authorizes its operation and resolves the current bucket.
    pub(crate) fn from_authorized_request(
        access: ManagedObjectAccess,
        request: Arc<RequestEnvelope>,
        encryption: ManagedEncryption,
    ) -> Result<Self, ManagedObjectError> {
        let principal = SseKmsPrincipal::from_request(request.as_ref()).ok_or(ManagedObjectError::MissingPrincipal)?;
        if request.request_context().is_none() || request.req_info().and_then(|info| info.cred.as_ref()).is_none() {
            return Err(ManagedObjectError::MissingPrincipal);
        }
        Self::validate_scope(&access, &encryption)?;
        if access
            .check(&format!("{}scope-check", access.allowed_prefix()), ManagedObjectOperation::Delete)
            .is_ok()
        {
            return Err(BucketIdentityError::OperationNotAllowed.into());
        }
        Ok(Self {
            access,
            identity: ManagedObjectIdentity::Request {
                request,
                principal: Box::new(principal),
            },
            encryption,
        })
    }

    fn validate_scope(access: &ManagedObjectAccess, encryption: &ManagedEncryption) -> Result<(), ManagedObjectError> {
        let table_id = Uuid::parse_str(access.table_id().as_str()).map_err(|_| BucketIdentityError::InvalidTableId)?;
        let dataset_prefix = format!(".rustfs-table/{table_id}/dataset/");
        let result_prefix = format!(".rustfs-table/{table_id}/query-results/");
        if table_id.is_nil()
            || access.bucket().kind() != BucketKind::Vector
            || (access.allowed_prefix() != dataset_prefix && access.allowed_prefix() != result_prefix)
        {
            return Err(BucketIdentityError::InvalidPrefix.into());
        }
        if access.allowed_prefix() == result_prefix {
            let scope_key = format!("{result_prefix}scope-check");
            for operation in [
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ] {
                if access.check(&scope_key, operation).is_ok() {
                    return Err(BucketIdentityError::OperationNotAllowed.into());
                }
            }
        }
        if matches!(&encryption, ManagedEncryption::AwsKms { key_id } if key_id.is_empty()) {
            return Err(ManagedObjectError::EncryptionMismatch);
        }
        Ok(())
    }

    pub(crate) fn from_maintenance(
        access: ManagedObjectAccess,
        encryption: ManagedEncryption,
        context: Arc<super::managed_maintenance::ManagedMaintenanceContext>,
    ) -> Result<Self, ManagedObjectError> {
        Self::validate_scope(&access, &encryption)?;
        Ok(Self {
            access,
            encryption,
            identity: ManagedObjectIdentity::Maintenance { context },
        })
    }

    #[cfg(test)]
    fn request_for_test(&self) -> &Arc<RequestEnvelope> {
        match &self.identity {
            ManagedObjectIdentity::Request { request, .. } => request,
            ManagedObjectIdentity::Maintenance { .. } => panic!("foreground fixture required"),
        }
    }

    pub(crate) fn access(&self) -> &ManagedObjectAccess {
        &self.access
    }

    pub(in crate::app) fn encryption(&self) -> &ManagedEncryption {
        &self.encryption
    }

    pub(super) async fn validate_object(
        &self,
        info: &ObjectInfo,
        key: &str,
        expected: Option<Uuid>,
        principal: &SseKmsPrincipal,
    ) -> Result<(), ManagedObjectError> {
        if info.delete_marker {
            return Err(ApiError::from(StorageError::ObjectNotFound(
                self.access.bucket().storage_bucket_id().as_str().to_owned(),
                key.to_owned(),
            ))
            .into());
        }
        let revision = info
            .data_dir
            .filter(|id| !id.is_nil())
            .ok_or(ManagedObjectError::MissingBodyRevision)?;
        if expected.is_some_and(|expected| expected != revision) {
            return Err(ManagedObjectError::BodyRevisionMismatch);
        }
        self.validate_encryption_metadata(key, &info.user_defined, principal).await
    }

    pub(in crate::app) async fn validate_encryption_metadata(
        &self,
        key: &str,
        metadata: &HashMap<String, String>,
        principal: &SseKmsPrincipal,
    ) -> Result<(), ManagedObjectError> {
        if self.encryption == ManagedEncryption::Unencrypted {
            return if metadata
                .keys()
                .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
            {
                Err(ManagedObjectError::EncryptionMismatch)
            } else {
                Ok(())
            };
        }
        let response = classify_sse_read_response(DecryptionRequest {
            bucket: self.access.bucket().storage_bucket_id().as_str(),
            key,
            metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(principal),
        })
        .await?
        .ok_or(ManagedObjectError::EncryptionMismatch)?;
        let matches = match &self.encryption {
            ManagedEncryption::Unencrypted => false,
            ManagedEncryption::Aes256 => response.server_side_encryption == "AES256",
            ManagedEncryption::AwsKms { key_id } => {
                response.server_side_encryption == "aws:kms" && response.ssekms_key_id.as_deref() == Some(key_id.as_str())
            }
        };
        if !matches {
            return Err(ManagedObjectError::EncryptionMismatch);
        }
        Ok(())
    }
}

impl ManagedMultipartUpload {
    fn origin(
        &self,
        principal: SseKmsPrincipal,
        completion: Option<OperationHelper>,
    ) -> crate::app::multipart_usecase::MultipartOrigin {
        crate::app::multipart_usecase::MultipartOrigin::Managed {
            context: Box::new(self.context.clone()),
            principal: Box::new(principal),
            completion,
            lifecycle_guard: self.lifecycle_guard.clone(),
        }
    }
}

fn managed_encryption_input(encryption: &ManagedEncryption) -> (Option<String>, Option<String>) {
    match encryption {
        ManagedEncryption::Unencrypted => (None, None),
        ManagedEncryption::Aes256 => (Some("AES256".to_owned()), None),
        ManagedEncryption::AwsKms { key_id } => (Some("aws:kms".to_owned()), Some(key_id.clone())),
    }
}

struct ManagedGetInput<'a> {
    context: &'a ManagedObjectContext,
    key: &'a str,
    expected_body_revision: Option<Uuid>,
    range: Option<Range<u64>>,
    max_bytes: usize,
    principal: &'a SseKmsPrincipal,
    request_id: &'a str,
}

impl DefaultObjectUsecase {
    pub(crate) async fn managed_create_object_with_guard<B>(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        size: u64,
        body: B,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> Result<ObjectInfo, ManagedObjectError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        self.managed_create_object_inner(context, key, size, body, Some(guard)).await
    }

    pub(crate) async fn managed_head_object_with_guard(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation { principal, helper, .. } = context
            .admit(key, ManagedObjectOperation::Head, S3Operation::HeadObject)
            .await?;
        let result = self
            .managed_head_object_under_guard(context, key, expected_body_revision, &principal, guard)
            .await;
        finish_managed_read(helper, &result);
        result
    }

    pub(crate) async fn managed_create_multipart_upload_with_guard(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> Result<Arc<ManagedMultipartUpload>, ManagedObjectError> {
        self.managed_start_multipart_upload(context, key, guard).await
    }

    #[cfg(test)]
    pub(crate) async fn managed_create_multipart_upload(
        &self,
        context: &ManagedObjectContext,
        key: &str,
    ) -> Result<Arc<ManagedMultipartUpload>, ManagedObjectError> {
        context.access.check(key, ManagedObjectOperation::CreateOnly)?;
        let (_, guard) = self
            .managed_bucket_guard(context, key, ManagedObjectOperation::CreateMultipart)
            .await?;
        self.managed_start_multipart_upload(context, key, Arc::new(guard)).await
    }

    async fn managed_start_multipart_upload(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> Result<Arc<ManagedMultipartUpload>, ManagedObjectError> {
        context.access.check(key, ManagedObjectOperation::CreateOnly)?;
        context.access.check(key, ManagedObjectOperation::CreateMultipart)?;
        self.managed_store_under_guard(context, &guard).await?;
        let (algorithm, key_id) = managed_encryption_input(&context.encryption);
        let input = crate::app::multipart_usecase::CreateMultipartUploadFields {
            bucket: context.access.bucket().storage_bucket_id().as_str().to_owned(),
            key: key.to_owned(),
            metadata: None,
            tagging: None,
            storage_class: None,
            server_side_encryption: algorithm,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ssekms_key_id: key_id,
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_date: None,
        };
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = context
            .admit(key, ManagedObjectOperation::CreateMultipart, S3Operation::CreateMultipartUpload)
            .await?;
        let request = request.fork_for_authorization();
        let upload = Arc::new(ManagedMultipartUpload {
            context: context.clone(),
            key: key.to_owned(),
            upload_id: String::new(),
            lifecycle_guard: guard,
        });
        let owner = crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(self.context.clone());
        run_owned_mutation(async move {
            let response = owner
                .create_multipart_with_origin(request, input, None, upload.origin(principal, Some(helper)))
                .await?;
            let upload_id = response.upload_id;
            Ok::<_, rustfs_s3_types::S3Error>(Arc::new(ManagedMultipartUpload {
                context: upload.context.clone(),
                key: upload.key.clone(),
                upload_id,
                lifecycle_guard: upload.lifecycle_guard.clone(),
            }))
        })
        .await
        .map_err(ManagedObjectError::from)
    }

    pub(crate) async fn managed_upload_part<B>(
        &self,
        upload: &Arc<ManagedMultipartUpload>,
        part_number: u16,
        size: u64,
        body: B,
    ) -> Result<CompletePart, ManagedObjectError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + Unpin + 'static,
    {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = upload
            .context
            .admit(&upload.key, ManagedObjectOperation::UploadPart, S3Operation::UploadPart)
            .await?;
        async {
            self.managed_validate_upload(upload, ManagedObjectOperation::UploadPart)
                .await?;
            let input = crate::app::multipart_usecase::UploadPartWriteInput {
                target: crate::app::multipart_usecase::UploadPartTarget::Legacy {
                    bucket: upload.context.access.bucket().storage_bucket_id().as_str().to_owned(),
                    key: upload.key.clone(),
                    upload_id: upload.upload_id.clone(),
                },
                part_number: i32::from(part_number),
                content_length: Some(i64::try_from(size).map_err(|_| ManagedObjectError::SizeLimitExceeded)?),
                body: Some(super::native_body::from_io_stream(body)),
                frame_trailers: None,
                trailing_headers: None,
                content_md5: None,
                checksum_algorithm: None,
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_crc64nvme: None,
                checksum_sha1: None,
                checksum_sha256: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            };
            let owner = crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(self.context.clone());
            let (_, part) = owner
                .execute_upload_part_with_origin(request.fork_for_authorization(), input, upload.origin(principal, Some(helper)))
                .await?;
            Ok(part)
        }
        .await
    }

    pub(crate) async fn managed_complete_multipart_upload(
        &self,
        upload: &Arc<ManagedMultipartUpload>,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = upload
            .context
            .admit(
                &upload.key,
                ManagedObjectOperation::CompleteMultipart,
                S3Operation::CompleteMultipartUpload,
            )
            .await?;
        self.managed_validate_upload(upload, ManagedObjectOperation::CompleteMultipart)
            .await?;
        upload.context.access.check(&upload.key, ManagedObjectOperation::CreateOnly)?;
        if parts.is_empty() || parts.windows(2).any(|pair| pair[0].part_num >= pair[1].part_num) {
            return Err(ApiError::invalid_request("managed multipart parts must be nonempty and strictly ordered").into());
        }
        let parts = parts
            .into_iter()
            .map(|part| {
                Ok(rustfs_gateway_types::dto::CompletedPart {
                    part_number: i32::try_from(part.part_num).map_err(|_| ManagedObjectError::SizeLimitExceeded)?,
                    e_tag: part.etag.map(rustfs_gateway_types::ETag::from_stored),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, ManagedObjectError>>()?;
        let input = rustfs_gateway_types::dto::CompleteMultipartUploadInput {
            bucket: rustfs_gateway_types::BucketName::new(upload.context.access.bucket().storage_bucket_id().as_str())
                .map_err(ApiError::other)?,
            key: rustfs_gateway_types::ObjectKey::new(upload.key.clone()).map_err(ApiError::other)?,
            upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(upload.upload_id.clone()),
            if_none_match: Some("*".to_owned()),
            ..Default::default()
        };
        let owner = crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(self.context.clone());
        let (_, info) = owner
            .complete_multipart_with_origin(
                request.fork_for_authorization(),
                input,
                Some(rustfs_gateway_types::dto::CompletedMultipartUpload { parts }),
                upload.origin(principal, Some(helper)),
            )
            .await?;
        info.data_dir
            .filter(|value| !value.is_nil())
            .ok_or(ManagedObjectError::MissingBodyRevision)?;
        info.get_actual_size().map_err(ApiError::from)?;
        Ok(info)
    }

    /// Reauthorizes an object whose successful completion was already observed by this session.
    pub(crate) async fn managed_confirm_completed_multipart_upload(
        &self,
        upload: &Arc<ManagedMultipartUpload>,
        expected_body_revision: Uuid,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation { principal, helper, .. } = upload
            .context
            .admit(
                &upload.key,
                ManagedObjectOperation::CompleteMultipart,
                S3Operation::CompleteMultipartUpload,
            )
            .await?;
        let result = async {
            if expected_body_revision.is_nil() {
                return Err(ManagedObjectError::MissingBodyRevision);
            }
            upload
                .context
                .access
                .check(&upload.key, ManagedObjectOperation::CompleteMultipart)?;
            upload.context.access.check(&upload.key, ManagedObjectOperation::CreateOnly)?;
            let info = self
                .managed_head_object_under_guard(
                    &upload.context,
                    &upload.key,
                    Some(expected_body_revision),
                    &principal,
                    &upload.lifecycle_guard,
                )
                .await?;
            authorize_sse_kms_multipart_write(Some(&principal), &info.user_defined).await?;
            if rustfs_utils::http::get_consistent_str(&info.user_defined, rustfs_utils::http::SUFFIX_MULTIPART_UPLOAD_ID)
                != Some(upload.upload_id.as_str())
                || upload.lifecycle_guard.is_lock_lost()
            {
                return Err(ApiError::from(StorageError::PreconditionFailed).into());
            }
            Ok(info)
        }
        .await;
        finish_managed_read(helper, &result);
        result
    }

    pub(crate) async fn managed_abort_multipart_upload(
        &self,
        upload: &Arc<ManagedMultipartUpload>,
    ) -> Result<(), ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = upload
            .context
            .admit(&upload.key, ManagedObjectOperation::AbortMultipart, S3Operation::AbortMultipartUpload)
            .await?;
        let result = async {
            self.managed_validate_upload(upload, ManagedObjectOperation::AbortMultipart)
                .await?;
            let target = crate::app::multipart_usecase::AbortMultipartUploadTarget::Gateway {
                bucket: rustfs_gateway_types::BucketName::new(upload.context.access.bucket().storage_bucket_id().as_str())
                    .map_err(ApiError::other)?,
                key: rustfs_gateway_types::ObjectKey::new(upload.key.clone()).map_err(ApiError::other)?,
                upload_id: rustfs_gateway_types::UploadIdClaim::from_wire(upload.upload_id.clone()),
            };
            let owner = crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(self.context.clone());
            let request = request.fork_for_authorization();
            let origin = upload.origin(principal, None);
            run_owned_mutation(async move {
                owner.abort_multipart_with_origin(request, target, origin).await?;
                Ok::<_, rustfs_s3_types::S3Error>(())
            })
            .await?;
            Ok(())
        }
        .await;
        finish_managed_read(helper, &result);
        result
    }

    async fn managed_validate_upload(
        &self,
        upload: &ManagedMultipartUpload,
        operation: ManagedObjectOperation,
    ) -> Result<(), ManagedObjectError> {
        upload.context.access.check(&upload.key, operation)?;
        self.managed_store_under_guard(&upload.context, &upload.lifecycle_guard)
            .await?;
        Ok(())
    }

    async fn managed_bucket_guard(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        operation: ManagedObjectOperation,
    ) -> Result<(Arc<ECStore>, BucketIncarnationFenceGuard), ManagedObjectError> {
        context.access.check(key, operation)?;
        let identity = context.access.bucket();
        let guard = crate::app::bucket_usecase::DefaultBucketUsecase::with_context(self.context.clone())
            .acquire_bucket_handle_guard(identity)
            .await?;
        let store = self.managed_store_under_guard(context, &guard).await?;
        Ok((store, guard))
    }

    pub(super) async fn managed_store_under_guard(
        &self,
        context: &ManagedObjectContext,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<Arc<ECStore>, ManagedObjectError> {
        let identity = context.access.bucket();
        let expected_storage = crate::app::bucket_usecase::DefaultBucketUsecase::with_context(self.context.clone())
            .checked_storage_bucket_id(
                identity.kind(),
                identity.account_namespace_id(),
                identity.region(),
                identity.public_name(),
            )?;
        if &expected_storage != identity.storage_bucket_id() {
            return Err(BucketResolveError::IdentityMismatch.into());
        }
        let store = self.object_store().ok_or(BucketResolveError::CapabilityUnavailable)?;
        managed::validate_bucket_incarnation_fence(&store, guard, identity.storage_bucket_id().as_str())
            .map_err(ApiError::from)?;
        let metadata = managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str())
            .await
            .map_err(ApiError::from)?;
        metadata.validate_bucket_handle(identity).map_err(ApiError::from)?;
        if metadata.versioning() || !metadata.versioning_config_xml.is_empty() {
            return Err(ApiError::invalid_request("managed object storage requires an unversioned bucket").into());
        }
        // The authorized context carries the effective Index configuration, including explicit overrides.
        if context.encryption == ManagedEncryption::Unencrypted && !metadata.encryption_config_xml.is_empty() {
            return Err(ManagedObjectError::EncryptionMismatch);
        }
        if guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        Ok(store)
    }

    pub(crate) async fn managed_create_object<B>(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        size: u64,
        body: B,
    ) -> Result<ObjectInfo, ManagedObjectError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        self.managed_create_object_inner(context, key, size, body, None).await
    }

    async fn managed_create_object_inner<B>(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        size: u64,
        body: B,
        guard: Option<Arc<BucketIncarnationFenceGuard>>,
    ) -> Result<ObjectInfo, ManagedObjectError>
    where
        B: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper: completion,
        } = context
            .admit(key, ManagedObjectOperation::CreateOnly, S3Operation::PutObject)
            .await?;
        let guard = match guard {
            Some(guard) => {
                context.access.check(key, ManagedObjectOperation::CreateOnly)?;
                self.managed_store_under_guard(context, &guard).await?;
                guard
            }
            None => Arc::new(
                self.managed_bucket_guard(context, key, ManagedObjectOperation::CreateOnly)
                    .await?
                    .1,
            ),
        };
        let size = i64::try_from(size).map_err(|_| ManagedObjectError::SizeLimitExceeded)?;
        validate_new_object_key(key, "PUT").map_err(|error| {
            ManagedObjectError::Api(ApiError {
                code: error.code().clone(),
                message: error.message().unwrap_or_default().to_owned(),
                source: Some(Box::new(error)),
            })
        })?;
        let mut headers = internal_put::internal_put_headers(&HashMap::new())?;
        headers.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let (algorithm, key_id) = match &context.encryption {
            ManagedEncryption::Unencrypted => (None, None),
            ManagedEncryption::Aes256 => (Some("AES256".to_owned()), None),
            ManagedEncryption::AwsKms { key_id } => (Some("aws:kms".to_owned()), Some(key_id.clone())),
        };
        let write = PutObjectWriteRequest {
            bucket: context.access.bucket().storage_bucket_id().as_str().to_owned(),
            key: key.to_owned(),
            size,
            quota_operation: QuotaOperation::PutObject,
            ciphertext_passthrough: false,
            inbound_replication_put: false,
            headers: &headers,
            metadata_headers: &headers,
            checksum_headers: &headers,
            query: None,
            trailing_headers: None,
            max_content_length: None,
            version_id: None,
            sse: PutObjectSseInput {
                server_side_encryption: algorithm,
                ssekms_key_id: key_id,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            },
            user_metadata: HashMap::new(),
            ec_block_size_hint: None,
            internal_metadata: HashMap::new(),
            content: internal_put::internal_put_content_input(&headers, None),
            object_lock: PutObjectLockInput {
                legal_hold_status: None,
                mode: None,
                retain_until_date: None,
            },
            content_md5: None,
            preserve_etag: None,
            origin: PutObjectOrigin::Managed {
                context,
                principal: Box::new(principal),
                request_context: request.request_context().cloned(),
                lifecycle_guard: Some(guard),
                completion: Some(completion),
            },
        };
        let committed = self
            .put_object_core(write, super::native_body::from_io_stream(body), None, Instant::now())
            .await?;
        let info = committed.obj_info.clone();
        committed.finish(&Ok(()), StatusCode::OK, &HeaderMap::new());
        info.data_dir
            .filter(|id| !id.is_nil())
            .ok_or(ManagedObjectError::MissingBodyRevision)?;
        Ok(info)
    }

    pub(crate) async fn managed_head_object(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation { principal, helper, .. } = context
            .admit(key, ManagedObjectOperation::Head, S3Operation::HeadObject)
            .await?;
        let result = self
            .managed_head_object_inner(context, key, expected_body_revision, &principal)
            .await;
        finish_managed_read(helper, &result);
        result
    }

    async fn managed_head_object_inner(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
        principal: &SseKmsPrincipal,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        let (_, guard) = self.managed_bucket_guard(context, key, ManagedObjectOperation::Head).await?;
        self.managed_head_object_under_guard(context, key, expected_body_revision, principal, &guard)
            .await
    }

    async fn managed_head_object_under_guard(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
        principal: &SseKmsPrincipal,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<ObjectInfo, ManagedObjectError> {
        context.access.check(key, ManagedObjectOperation::Head)?;
        let store = self.managed_store_under_guard(context, guard).await?;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let mut opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(context.access.bucket().incarnation().as_uuid()),
            ..Default::default()
        };
        guard.attach_to_object_options(&mut opts);
        let info = store.get_object_info(bucket, key, &opts).await.map_err(ApiError::from)?;
        context.validate_object(&info, key, expected_body_revision, principal).await?;
        info.get_actual_size().map_err(ApiError::from)?;
        if guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        Ok(info)
    }

    pub(crate) async fn managed_get_object(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
        range: Option<Range<u64>>,
        max_bytes: usize,
    ) -> Result<(ObjectInfo, Bytes), ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = context
            .admit(
                key,
                if range.is_some() {
                    ManagedObjectOperation::Range
                } else {
                    ManagedObjectOperation::Get
                },
                S3Operation::GetObject,
            )
            .await?;
        let request_id = request
            .request_context()
            .ok_or(ManagedObjectError::MissingPrincipal)?
            .request_id
            .as_str();
        let result = async {
            context.access.check(
                key,
                if range.is_some() {
                    ManagedObjectOperation::Range
                } else {
                    ManagedObjectOperation::Get
                },
            )?;
            let guard = crate::app::bucket_usecase::DefaultBucketUsecase::with_context(self.context.clone())
                .acquire_bucket_handle_guard(context.access.bucket())
                .await?;
            self.managed_get_object_inner(
                ManagedGetInput {
                    context,
                    key,
                    expected_body_revision,
                    range,
                    max_bytes,
                    principal: &principal,
                    request_id,
                },
                &guard,
            )
            .await
        }
        .await;
        finish_managed_read(helper, &result);
        result
    }

    pub(crate) async fn managed_get_object_with_guard(
        &self,
        context: &ManagedObjectContext,
        key: &str,
        expected_body_revision: Option<Uuid>,
        range: Option<Range<u64>>,
        max_bytes: usize,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<(ObjectInfo, Bytes), ManagedObjectError> {
        let super::managed_maintenance::ManagedOperation {
            request,
            principal,
            helper,
        } = context
            .admit(
                key,
                if range.is_some() {
                    ManagedObjectOperation::Range
                } else {
                    ManagedObjectOperation::Get
                },
                S3Operation::GetObject,
            )
            .await?;
        let request_id = request
            .request_context()
            .ok_or(ManagedObjectError::MissingPrincipal)?
            .request_id
            .as_str();
        let result = self
            .managed_get_object_inner(
                ManagedGetInput {
                    context,
                    key,
                    expected_body_revision,
                    range,
                    max_bytes,
                    principal: &principal,
                    request_id,
                },
                guard,
            )
            .await;
        finish_managed_read(helper, &result);
        result
    }

    async fn managed_get_object_inner(
        &self,
        input: ManagedGetInput<'_>,
        guard: &BucketIncarnationFenceGuard,
    ) -> Result<(ObjectInfo, Bytes), ManagedObjectError> {
        let ManagedGetInput {
            context,
            key,
            expected_body_revision,
            range,
            max_bytes,
            principal,
            request_id,
        } = input;
        let operation = if range.is_some() {
            ManagedObjectOperation::Range
        } else {
            ManagedObjectOperation::Get
        };
        context.access.check(key, operation)?;
        let store = self.managed_store_under_guard(context, guard).await?;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let mut opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(context.access.bucket().incarnation().as_uuid()),
            ..Default::default()
        };
        guard.attach_to_object_options(&mut opts);
        // Authorize the stored key before the reader can unwrap any encryption material.
        let info = store.get_object_info(bucket, key, &opts).await.map_err(ApiError::from)?;
        context.validate_object(&info, key, expected_body_revision, principal).await?;
        let size =
            u64::try_from(info.get_actual_size().map_err(ApiError::from)?).map_err(|_| ManagedObjectError::SizeLimitExceeded)?;
        let (rs, selected_size) = managed_range(range, size)?;
        let selected_size = usize::try_from(selected_size).map_err(|_| ManagedObjectError::SizeLimitExceeded)?;
        if selected_size > max_bytes {
            return Err(ManagedObjectError::SizeLimitExceeded);
        }
        let result = self
            .managed_read_bytes_core(super::get::ManagedReadInput {
                context,
                key,
                opts: &opts,
                range: rs,
                expected: &info,
                max_bytes,
                principal,
                request_id,
            })
            .await?;
        context.validate_object(&result.0, key, info.data_dir, principal).await?;
        if guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::PreconditionFailed).into());
        }
        Ok(result)
    }
}

fn managed_range(range: Option<Range<u64>>, size: u64) -> Result<(Option<HTTPRangeSpec>, u64), ManagedObjectError> {
    let Some(range) = range else {
        return Ok((None, size));
    };
    if range.start >= range.end || range.end > size {
        return Err(ManagedObjectError::InvalidRange);
    }
    let start = i64::try_from(range.start).map_err(|_| ManagedObjectError::InvalidRange)?;
    let end = i64::try_from(range.end - 1).map_err(|_| ManagedObjectError::InvalidRange)?;
    Ok((
        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        }),
        range.end - range.start,
    ))
}

pub(super) fn finish_managed_read<T>(helper: OperationHelper, result: &Result<T, ManagedObjectError>) {
    let outcome: rustfs_s3_types::S3Result<()> = match result {
        Ok(_) => Ok(()),
        Err(error) => {
            let code = match error {
                ManagedObjectError::Api(error) => error.code.clone(),
                ManagedObjectError::Scope(_) | ManagedObjectError::MissingPrincipal => rustfs_s3_types::S3ErrorCode::AccessDenied,
                ManagedObjectError::BodyRevisionMismatch => rustfs_s3_types::S3ErrorCode::PreconditionFailed,
                ManagedObjectError::InvalidRange => rustfs_s3_types::S3ErrorCode::InvalidRange,
                _ => rustfs_s3_types::S3ErrorCode::InternalError,
            };
            Err(rustfs_s3_types::S3Error::with_message(code, error.to_string()))
        }
    };
    let _ = helper.complete_owned(&outcome, StatusCode::OK, &HeaderMap::new());
}

#[cfg(test)]
pub(super) mod tests {
    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial]
    async fn managed_multipart_part_keeps_lifecycle_guard_after_caller_cancel() {
        use crate::app::storage_api::test::contract::multipart::MultipartOperations as _;
        use crate::app::storage_api::test::{
            LocalPublicationPause, LocalPublicationStage, RUSTFS_META_MULTIPART_BUCKET, ReadOptions, StorageDiskRpcExt as _,
            all_local_disk_path, find_local_disk_by_ref,
        };
        use sha2::{Digest as _, Sha256};

        let (usecase, context, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let locks_before = managed_lock_counts();
        let context = multipart_context(&context);
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let sentinel = format!("{}sentinel.bin", context.access.allowed_prefix());
        usecase
            .managed_create_object(&context, &sentinel, 1, futures::stream::iter([Ok(Bytes::from_static(b"s"))]))
            .await
            .expect("keep the bucket present after the queued deletion resumes");
        let key = format!("{}cancelled-part.lance", context.access.allowed_prefix());
        let upload = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("multipart session");
        let upload_id = upload.upload_id.clone();

        let endpoints = store.instance_endpoints().expect("fixture store endpoint topology");
        let endpoint = endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref())
            .find(|endpoint| endpoint.is_local)
            .expect("fixture store has a local disk");
        let disk_ref = endpoint.to_string();
        assert!(all_local_disk_path().await.contains(&disk_ref));
        let disk = find_local_disk_by_ref(&disk_ref)
            .await
            .expect("resolve the fixture store's exact local endpoint");
        let decoded_upload = String::from_utf8(
            base64_simd::URL_SAFE_NO_PAD
                .decode_to_vec(upload_id.as_bytes())
                .expect("fixture upload id is base64url"),
        )
        .expect("fixture upload id is UTF-8");
        let (_, upload_suffix) = decoded_upload.split_once('.').expect("deployment-bound upload id");
        let object_hash = hex_simd::encode_to_string(Sha256::digest(format!("{bucket}/{key}")), hex_simd::AsciiCase::Lower);
        let upload_path = format!("{object_hash}/{upload_suffix}");
        let staged = disk
            .read_version(&bucket, RUSTFS_META_MULTIPART_BUCKET, &upload_path, "", &ReadOptions::default())
            .await
            .expect("read this session's actual multipart xl.meta from the selected disk");
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&staged.metadata, "multipart-bucket"),
            Some(bucket.as_str()),
        );
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&staged.metadata, "multipart-object"),
            Some(key.as_str()),
        );
        let incarnation =
            rustfs_utils::http::get_consistent_str(&staged.metadata, rustfs_utils::http::SUFFIX_BUCKET_INCARNATION_ID)
                .and_then(|value| Uuid::parse_str(value).ok());
        assert_eq!(incarnation, Some(context.access.bucket().incarnation().as_uuid()));
        let data_dir = staged
            .data_dir
            .filter(|id| !id.is_nil())
            .expect("persisted multipart data directory");
        let part_path = format!("{upload_path}/{data_dir}/part.1");
        let native_part_path = disk
            .get_object_path_if_local(RUSTFS_META_MULTIPART_BUCKET, &part_path)
            .expect("selected disk is local")
            .expect("exact native part destination");
        let raw_endpoint_path = endpoint.get_file_path();
        let canonical_endpoint_path = tokio::fs::canonicalize(&raw_endpoint_path)
            .await
            .unwrap_or_else(|error| panic!("canonicalize fixture endpoint path {raw_endpoint_path:?}: {error}"));
        let expected_part_path = canonical_endpoint_path.join(RUSTFS_META_MULTIPART_BUCKET).join(&part_path);
        assert_eq!(
            native_part_path, expected_part_path,
            "native part destination must match the selected fixture endpoint: raw_endpoint={raw_endpoint_path:?}, canonical_endpoint={canonical_endpoint_path:?}",
        );
        assert!(
            !tokio::fs::try_exists(&native_part_path)
                .await
                .expect("part destination is readable")
        );
        let mut pause =
            LocalPublicationPause::install(&disk, RUSTFS_META_MULTIPART_BUCKET, &part_path, LocalPublicationStage::Rename)
                .expect("pause the actual native part rename");
        let mut part = tokio::spawn(async move {
            usecase
                .managed_upload_part(&upload, 1, 7, futures::stream::iter([Ok(Bytes::from_static(b"durable"))]))
                .await
        });
        tokio::select! {
            biased;
            result = &mut part => panic!("UploadPart returned before the native rename pause: {result:?}"),
            entered = tokio::time::timeout(Duration::from_secs(10), pause.entered()) => {
                entered.expect("UploadPart reaches the native rename pause").expect("native rename entry signal");
            }
        }
        assert!(!part.is_finished(), "UploadPart caller must remain pending inside native rename");
        assert!(
            !tokio::fs::try_exists(&native_part_path)
                .await
                .expect("paused part destination is readable")
        );
        eprintln!("managed multipart cancellation: native rename entered; caller pending; destination={native_part_path:?}");
        let mut delete = queued_bucket_delete(Arc::clone(&store), bucket.clone()).await;
        eprintln!("managed multipart cancellation: bucket writer entered the lock waiter queue before caller cancellation");
        part.abort();
        assert!(part.await.expect_err("part caller cancelled").is_cancelled());
        assert!(
            !tokio::fs::try_exists(&native_part_path)
                .await
                .expect("cancelled caller leaves native rename paused")
        );
        eprintln!("managed multipart cancellation: caller cancelled; native pause remains held; destination remains absent");
        let while_paused = tokio::time::timeout(Duration::from_millis(100), &mut delete).await;
        assert!(
            while_paused.is_err(),
            "bucket deletion must remain queued while the cancelled caller's native rename is paused: {while_paused:?}",
        );
        eprintln!("managed multipart cancellation: caller cancelled; bucket writer remains queued; native pause remains held");
        drop(pause);
        let result = tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("deletion resumes after the native part rename finishes")
            .expect("delete task");
        assert!(matches!(result, Err(StorageError::BucketNotEmpty(_))));
        assert!(
            tokio::fs::try_exists(&native_part_path)
                .await
                .expect("native part destination is readable after release")
        );
        let parts = store
            .list_object_parts(&bucket, &key, &upload_id, None, 10, &ObjectOptions::default())
            .await
            .expect("committed part remains readable");
        assert_eq!(parts.parts.len(), 1);
        assert_eq!(parts.parts[0].part_num, 1);
        assert_eq!(parts.parts[0].actual_size, 7);
        assert!(parts.parts[0].etag.as_ref().is_some_and(|etag| !etag.is_empty()));
        wait_for_managed_lock_counts(locks_before).await;
    }

    use super::*;
    use rustfs_s3_types::S3ErrorCode;

    const AES256_CONFIG: &[u8] = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";

    pub(crate) struct SseProviderReset;
    impl Drop for SseProviderReset {
        fn drop(&mut self) {
            crate::app::storage_api::test::sse::reset_sse_dek_provider();
        }
    }

    fn managed_envelope(extensions: http::Extensions) -> RequestEnvelope {
        RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: http::Method::POST,
                uri: http::Uri::from_static("/"),
                headers: http::HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            extensions,
        )
    }

    async fn managed_fixture() -> (DefaultObjectUsecase, ManagedObjectContext, Arc<ECStore>, SseProviderReset) {
        managed_fixture_with_encryption(ManagedEncryption::Aes256).await
    }

    async fn managed_fixture_with_encryption(
        encryption: ManagedEncryption,
    ) -> (DefaultObjectUsecase, ManagedObjectContext, Arc<ECStore>, SseProviderReset) {
        use crate::app::storage_api::managed::{
            AccountNamespaceId, BucketHandle, BucketIncarnation, BucketPublicName, ManagedTableId,
        };
        use crate::app::storage_api::test::contract::bucket::MakeBucketOptions;
        let (store, app_context) = super::super::test_support::real_store_test_context().await;
        let reset = SseProviderReset;
        crate::app::storage_api::test::sse::reset_sse_dek_provider();
        if encryption != ManagedEncryption::Unencrypted {
            crate::app::storage_api::test::sse::set_sse_dek_provider_for_test(Arc::new(
                crate::app::storage_api::test::sse::LocalSseDekProvider::new_with_key([37; 32]),
            ));
        }
        let account = AccountNamespaceId::empty();
        let name = BucketPublicName::try_new(format!("managed-{}", Uuid::new_v4().simple())).expect("name");
        let id = crate::app::bucket_usecase::vector_storage_bucket_id(&account, "us-east-1", &name).expect("derived ID");
        let identity = BucketHandle::try_from_parts(
            BucketKind::Vector,
            account,
            "us-east-1".into(),
            name,
            id,
            BucketIncarnation::try_new(Uuid::new_v4()).expect("incarnation"),
        )
        .expect("identity");
        let encryption_xml = match &encryption {
            ManagedEncryption::Unencrypted => Vec::new(),
            ManagedEncryption::Aes256 => AES256_CONFIG.to_vec(),
            ManagedEncryption::AwsKms { key_id } => format!("<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>{key_id}</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>").into_bytes(),
        };
        managed::make_managed_bucket(&store, &identity, &encryption_xml, &[], &MakeBucketOptions::default())
            .await
            .expect("create managed bucket");
        let metadata = managed::get_bucket_metadata(&store, identity.storage_bucket_id().as_str())
            .await
            .expect("persisted metadata");
        metadata
            .validate_bucket_handle(&identity)
            .expect("committed binding and sidecar");
        let table_id = ManagedTableId::try_new(Uuid::new_v4().to_string()).expect("table identity");
        let prefix = format!(".rustfs-table/{}/dataset/", table_id.as_str());
        let scope = ManagedObjectAccess::try_from_parts(
            identity.clone(),
            table_id,
            prefix,
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
            ]
            .into_iter()
            .collect(),
        )
        .expect("scope");
        let mut extensions = http::Extensions::new();
        let request_context = request_context::RequestContext::fallback();
        extensions.insert(request_context.clone());
        let server = crate::app::runtime_sources::ServerContextSlot::new();
        assert!(server.install(Arc::clone(&app_context)));
        extensions.insert(server);
        extensions.insert(crate::app::storage_api::test::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: "managed-test-user".into(),
                ..Default::default()
            }),
            bucket: Some(identity.storage_bucket_id().as_str().into()),
            request_context: Some(request_context),
            ..Default::default()
        });
        let context = ManagedObjectContext::from_authorized_request(scope, Arc::new(managed_envelope(extensions)), encryption)
            .expect("authorized context");
        (DefaultObjectUsecase::with_context(Some(app_context)), context, store, reset)
    }

    pub(crate) async fn managed_request_fixture() -> (Arc<RequestEnvelope>, ManagedObjectAccess, Arc<ECStore>, SseProviderReset) {
        managed_request_fixture_with_encryption(ManagedEncryption::Aes256).await
    }

    pub(crate) async fn managed_request_fixture_with_encryption(
        encryption: ManagedEncryption,
    ) -> (Arc<RequestEnvelope>, ManagedObjectAccess, Arc<ECStore>, SseProviderReset) {
        let (_, context, store, reset) = managed_fixture_with_encryption(encryption).await;
        (context.request_for_test().clone(), context.access, store, reset)
    }

    fn multipart_context(context: &ManagedObjectContext) -> ManagedObjectContext {
        let access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            context.access.table_id().clone(),
            context.access.allowed_prefix().to_owned(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ]
            .into_iter()
            .collect(),
        )
        .expect("complete multipart scope");
        ManagedObjectContext::from_authorized_request(access, context.request_for_test().clone(), context.encryption.clone())
            .expect("authorized multipart context")
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_multipart_unencrypted_streams_and_ranges_without_kms() {
        let (usecase, original, _, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let context = multipart_context(&original);
        let key = format!("{}data/large.lance", context.access.allowed_prefix());
        let upload = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("create unencrypted multipart session without KMS");
        let part_size = 6 * 1024 * 1024;
        let mut parts = Vec::new();
        for part_number in 1..=3_u16 {
            let value = u8::try_from(part_number).expect("three test parts");
            let chunk = Bytes::from(vec![value; 1024 * 1024]);
            let chunks = std::iter::repeat_n(chunk, 6).map(Ok::<_, io::Error>);
            parts.push(
                usecase
                    .managed_upload_part(
                        &upload,
                        part_number,
                        u64::try_from(part_size).expect("part size"),
                        futures::stream::iter(chunks),
                    )
                    .await
                    .expect("stream a managed part"),
            );
        }
        let completed = usecase
            .managed_complete_multipart_upload(&upload, parts)
            .await
            .expect("complete a Dataset file larger than the manifest budget");
        assert!(!completed.is_encrypted());
        let revision = completed.data_dir.filter(|value| !value.is_nil()).expect("body revision");
        assert_eq!(completed.get_actual_size().expect("plaintext size"), 18 * 1024 * 1024);
        let head = usecase
            .managed_head_object(&context, &key, Some(revision))
            .await
            .expect("HEAD");
        assert_eq!(head.get_actual_size().expect("HEAD plaintext size"), 18 * 1024 * 1024);
        let boundary = u64::try_from(part_size).expect("part size");
        let (_, data) = usecase
            .managed_get_object(&context, &key, Some(revision), Some(boundary - 8..boundary + 8), 16)
            .await
            .expect("bounded Range across two stored parts");
        assert_eq!(&data[..8], &[1; 8]);
        assert_eq!(&data[8..], &[2; 8]);
        let (_, tail) = usecase
            .managed_get_object(&context, &key, Some(revision), Some(boundary * 3 - 8..boundary * 3), 8)
            .await
            .expect("final part Range");
        assert_eq!(&tail[..], &[3; 8]);
    }

    async fn assert_managed_shared_guard_progress(operation: &str) {
        let (usecase, initial, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let context = multipart_context(&initial);
        let key = format!("{}data/guarded.lance", context.access.allowed_prefix());
        let seeded = if operation == "head" {
            Some(
                usecase
                    .managed_create_object(&context, &key, 7, futures::stream::iter([Ok(Bytes::from_static(b"guarded"))]))
                    .await
                    .expect("seed object before the writer queues"),
            )
        } else {
            None
        };
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let guard = Arc::new(
            crate::app::bucket_usecase::DefaultBucketUsecase::with_context(usecase.context.clone())
                .acquire_bucket_handle_guard(context.access.bucket())
                .await
                .expect("operation lifecycle guard"),
        );
        let writer = queued_bucket_delete(store.clone(), bucket.clone()).await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            match operation {
                "head" => {
                    usecase
                        .managed_head_object_with_guard(&context, &key, seeded.as_ref().and_then(|info| info.data_dir), &guard)
                        .await
                }
                "create" => {
                    usecase
                        .managed_create_object_with_guard(
                            &context,
                            &key,
                            7,
                            futures::stream::iter([Ok(Bytes::from_static(b"guarded"))]),
                            guard.clone(),
                        )
                        .await
                }
                "multipart" => {
                    let upload = usecase
                        .managed_create_multipart_upload_with_guard(&context, &key, guard.clone())
                        .await?;
                    let part = usecase
                        .managed_upload_part(&upload, 1, 7, futures::stream::iter([Ok(Bytes::from_static(b"guarded"))]))
                        .await?;
                    usecase.managed_complete_multipart_upload(&upload, vec![part]).await
                }
                _ => unreachable!("test operation is fixed by each test"),
            }
        })
        .await
        .expect("shared lifecycle read must progress while an exclusive writer is queued")
        .expect("managed operation reuses the supplied lifecycle guard");
        assert_eq!(result.get_actual_size().expect("plaintext length"), 7);
        let (_, data) = usecase
            .managed_get_object_with_guard(&context, &key, result.data_dir, None, 7, &guard)
            .await
            .expect("read the exact result under the same guard");
        assert_eq!(data, Bytes::from_static(b"guarded"));
        assert!(!writer.is_finished(), "bucket writer remains blocked by the active operation");
        drop(guard);
        let deletion = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
            .await
            .expect("writer completes after guard release")
            .expect("writer task");
        assert!(matches!(deletion, Err(StorageError::BucketNotEmpty(_))));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_shared_guard_head_with_waiting_writer() {
        assert_managed_shared_guard_progress("head").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_shared_guard_create_with_waiting_writer() {
        assert_managed_shared_guard_progress("create").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_shared_guard_multipart_with_waiting_writer() {
        assert_managed_shared_guard_progress("multipart").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_multipart_aes256_create_only_and_abort_preserve_object() {
        let (usecase, original, store, _reset) = managed_fixture().await;
        let context = multipart_context(&original);
        let key = format!("{}data/encrypted.lance", context.access.allowed_prefix());
        let first = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("first upload");
        let second = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("competing upload");
        let mut completed_parts = Vec::new();
        for (upload, payload) in [(&first, b"original".as_slice()), (&second, b"replaced".as_slice())] {
            completed_parts.push(
                usecase
                    .managed_upload_part(upload, 1, 8, futures::stream::iter([Ok(Bytes::copy_from_slice(payload))]))
                    .await
                    .expect("encrypted upload part"),
            );
        }
        let competing_part = completed_parts.pop().expect("competing part");
        let original_part = completed_parts.pop().expect("original part");
        let completed = usecase
            .managed_complete_multipart_upload(&first, vec![original_part])
            .await
            .expect("first conditional completion");
        assert!(completed.is_encrypted());
        let revision = completed.data_dir.expect("completion revision");
        let conflict = usecase
            .managed_complete_multipart_upload(&second, vec![competing_part])
            .await
            .expect_err("create-only completion preserves the first object");
        assert!(matches!(conflict, ManagedObjectError::Api(ref error) if error.code == S3ErrorCode::PreconditionFailed));
        usecase
            .managed_abort_multipart_upload(&second)
            .await
            .expect("abort staged conflicting upload");
        let after_complete = usecase.managed_abort_multipart_upload(&first).await;
        assert!(
            after_complete.is_ok()
                || matches!(after_complete, Err(ManagedObjectError::Api(ref error)) if error.code == S3ErrorCode::NoSuchUpload)
        );
        let (_, data) = usecase
            .managed_get_object(&context, &key, Some(revision), None, 8)
            .await
            .expect("retained object");
        assert_eq!(data, Bytes::from_static(b"original"));
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let info = store
            .get_object_info(bucket, &key, &ObjectOptions::default())
            .await
            .expect("persisted object");
        assert_eq!(info.data_dir, Some(revision));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_multipart_quota_rechecks_at_completion() {
        let (usecase, original, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let context = multipart_context(&original);
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let metadata = usecase.bucket_metadata_sys().expect("instance metadata system");
        let mut checker = QuotaChecker::new(metadata);
        checker
            .set_quota_config(bucket, BucketQuota::new(Some(64)))
            .await
            .expect("initial quota");
        crate::app::storage_api::test::set_disk::initialize_quota_ledger_for_test(store.clone(), bucket)
            .await
            .expect("initial ledger");
        let key = format!("{}data/quota.lance", context.access.allowed_prefix());
        let upload = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("quota upload session");
        let part = usecase
            .managed_upload_part(&upload, 1, 8, futures::stream::iter([Ok(Bytes::from_static(b"count-me"))]))
            .await
            .expect("stage part");
        checker
            .set_quota_config(bucket, BucketQuota::new(Some(4)))
            .await
            .expect("lower current quota");
        crate::app::storage_api::test::set_disk::initialize_quota_ledger_for_test(store.clone(), bucket)
            .await
            .expect("ledger for the new quota revision");
        let error = usecase
            .managed_complete_multipart_upload(&upload, vec![part])
            .await
            .expect_err("completion must use the current quota");
        assert!(
            matches!(error, ManagedObjectError::Api(ref error) if error.code == S3ErrorCode::InvalidRequest && error.message.contains("quota"))
        );
        let missing = store
            .get_object_info(bucket, &key, &ObjectOptions::default())
            .await
            .expect_err("failed completion publishes no object");
        assert_eq!(ApiError::from(missing).code, S3ErrorCode::NoSuchKey);
        usecase
            .managed_abort_multipart_upload(&upload)
            .await
            .expect("remove staged quota-refused upload");
        let fitting_key = format!("{}data/fits.lance", context.access.allowed_prefix());
        usecase
            .managed_create_object(&context, &fitting_key, 4, futures::stream::iter([Ok(Bytes::from_static(b"fits"))]))
            .await
            .expect("failed completion has not consumed object quota");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_query_result_scope_is_independent() {
        let (usecase, dataset, _, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let prefix = format!(".rustfs-table/{}/query-results/", dataset.access.table_id().as_str());
        let access = ManagedObjectAccess::try_from_parts(
            dataset.access.bucket().clone(),
            dataset.access.table_id().clone(),
            prefix.clone(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
            ]
            .into_iter()
            .collect(),
        )
        .expect("query result scope shape");
        let results = ManagedObjectContext::from_authorized_request(
            access,
            dataset.request_for_test().clone(),
            ManagedEncryption::Unencrypted,
        )
        .expect("independent query result context");
        let key = format!("{prefix}result.json");
        let created = usecase
            .managed_create_object(&results, &key, 2, futures::stream::iter([Ok(Bytes::from_static(b"[]"))]))
            .await
            .expect("persist query result");
        let (_, data) = usecase
            .managed_get_object(&results, &key, created.data_dir, None, 2)
            .await
            .expect("read query result");
        assert_eq!(data, Bytes::from_static(b"[]"));
        assert!(matches!(
            usecase.managed_get_object(&dataset, &key, created.data_dir, None, 2).await,
            Err(ManagedObjectError::Scope(BucketIdentityError::ObjectOutsideScope))
        ));
        let dataset_key = format!("{}data/fragment.lance", dataset.access.allowed_prefix());
        assert!(matches!(
            usecase
                .managed_create_object(&results, &dataset_key, 2, futures::stream::iter([Ok(Bytes::from_static(b"[]"))]))
                .await,
            Err(ManagedObjectError::Scope(BucketIdentityError::ObjectOutsideScope))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_context_requires_exact_dataset_scope() {
        let (_, context, _, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let table_id = context.access.table_id();
        let dataset_prefix = format!(".rustfs-table/{}/dataset/", table_id.as_str());
        assert_eq!(context.access.allowed_prefix(), dataset_prefix);
        for prefix in [
            format!(".rustfs-table/{}/", table_id.as_str()),
            format!(".rustfs-table/{}/dataset/", Uuid::new_v4()),
            format!("{dataset_prefix}_versions/"),
            format!("indexes/{}/", table_id.as_str()),
        ] {
            let access = ManagedObjectAccess::try_from_parts(
                context.access.bucket().clone(),
                table_id.clone(),
                prefix,
                [ManagedObjectOperation::Get].into_iter().collect(),
            )
            .expect("well-formed but non-Dataset scope");
            assert!(matches!(
                ManagedObjectContext::from_authorized_request(
                    access,
                    context.request_for_test().clone(),
                    ManagedEncryption::Unencrypted
                ),
                Err(ManagedObjectError::Scope(BucketIdentityError::InvalidPrefix))
            ));
        }
        for key in [
            format!(".rustfs-table/{}/query-results/result", table_id.as_str()),
            format!(".rustfs-table/{}/dataset/data/fragment", Uuid::new_v4()),
            format!(".rustfs-table/{}/dataset-neighbor/object", table_id.as_str()),
        ] {
            assert_eq!(
                context.access.check(&key, ManagedObjectOperation::Get),
                Err(BucketIdentityError::ObjectOutsideScope)
            );
        }
        context
            .access
            .check(&format!("{dataset_prefix}_versions/candidate.manifest"), ManagedObjectOperation::Get)
            .expect("exact Dataset descendant is allowed");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_unencrypted_create_head_get_range_ignores_default_sse() {
        use crate::app::storage_api::test::sse::{RecordingKmsKeyAuthorizer, TestAuthorizationHooks, reset_sse_dek_provider};
        let (_, default_context, _, _default_reset) = managed_fixture_with_encryption(ManagedEncryption::AwsKms {
            key_id: "cached-default-key".into(),
        })
        .await;
        let (usecase, original, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        reset_sse_dek_provider();
        let bucket = original.access.bucket().storage_bucket_id().as_str();
        let metadata = managed::get_bucket_metadata(&store, bucket)
            .await
            .expect("unencrypted persisted metadata");
        assert!(metadata.encryption_config_xml.is_empty());
        let default_metadata = managed::get_bucket_metadata(&store, default_context.access.bucket().storage_bucket_id().as_str())
            .await
            .expect("configured KMS default");
        let mut cached = metadata.as_ref().clone();
        cached.encryption_config_xml = default_metadata.encryption_config_xml.clone();
        cached.sse_config = default_metadata.sse_config.clone();
        crate::app::storage_api::test::set_bucket_metadata(bucket.to_owned(), cached)
            .await
            .expect("inject unrelated cached default");
        let authorizer = RecordingKmsKeyAuthorizer::new(false);
        let mut extensions = http::Extensions::new();
        let request_context = request_context::RequestContext::fallback();
        extensions.insert(request_context.clone());
        extensions.insert(original.request_for_test().server_context().expect("server context").clone());
        extensions.insert(crate::app::storage_api::test::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: "unencrypted-user".into(),
                ..Default::default()
            }),
            bucket: Some(bucket.into()),
            request_context: Some(request_context),
            ..Default::default()
        });
        extensions.insert(TestAuthorizationHooks {
            enforced: true,
            authorizer: authorizer.clone(),
        });
        let context = ManagedObjectContext::from_authorized_request(
            original.access.clone(),
            Arc::new(managed_envelope(extensions)),
            ManagedEncryption::Unencrypted,
        )
        .expect("unencrypted context");
        let key = format!("{}plain.bin", context.access.allowed_prefix());
        let payload = Bytes::from_static(b"unencrypted managed bytes");
        let info = usecase
            .managed_create_object(
                &context,
                &key,
                u64::try_from(payload.len()).expect("size"),
                futures::stream::iter([Ok(payload.clone())]),
            )
            .await
            .expect("unencrypted create without a KMS provider");
        assert!(!info.is_encrypted());
        let stored = store
            .get_object_info(bucket, &key, &ObjectOptions::default())
            .await
            .expect("physical object metadata");
        assert!(!stored.is_encrypted());
        assert!(
            stored
                .user_defined
                .keys()
                .all(|key| !rustfs_utils::http::is_object_encryption_marker(key))
        );
        let head = usecase
            .managed_head_object(&context, &key, info.data_dir)
            .await
            .expect("unencrypted HEAD");
        assert!(!head.is_encrypted());
        let (_, bytes) = usecase
            .managed_get_object(&context, &key, info.data_dir, None, payload.len())
            .await
            .expect("unencrypted GET");
        assert_eq!(bytes, payload);
        let (_, bytes) = usecase
            .managed_get_object(&context, &key, info.data_dir, Some(2..12), 10)
            .await
            .expect("unencrypted Range");
        assert_eq!(bytes, payload.slice(2..12));
        assert!(authorizer.calls().is_empty(), "unencrypted storage must not request KMS authorization");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_index_encryption_override_and_object_mismatch_are_checked() {
        let (usecase, plain, _, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let plaintext_key = format!("{}plaintext.bin", plain.access.allowed_prefix());
        let plaintext = usecase
            .managed_create_object(&plain, &plaintext_key, 5, futures::stream::iter([Ok(Bytes::from_static(b"plain"))]))
            .await
            .expect("plain candidate");
        let encrypted = ManagedObjectContext::from_authorized_request(
            plain.access.clone(),
            plain.request_for_test().clone(),
            ManagedEncryption::Aes256,
        )
        .expect("explicit Index AES256 override");
        assert!(matches!(
            usecase
                .managed_head_object(&encrypted, &plaintext_key, plaintext.data_dir)
                .await,
            Err(ManagedObjectError::EncryptionMismatch)
        ));
        crate::app::storage_api::test::sse::set_sse_dek_provider_for_test(Arc::new(
            crate::app::storage_api::test::sse::LocalSseDekProvider::new_with_key([37; 32]),
        ));
        let encrypted_key = format!("{}aes.bin", encrypted.access.allowed_prefix());
        let info = usecase
            .managed_create_object(&encrypted, &encrypted_key, 3, futures::stream::iter([Ok(Bytes::from_static(b"aes"))]))
            .await
            .expect("explicit AES256 on an unencrypted bucket");
        assert!(info.is_encrypted());
        assert!(matches!(
            usecase
                .managed_get_object(&plain, &encrypted_key, info.data_dir, None, 3)
                .await,
            Err(ManagedObjectError::EncryptionMismatch)
        ));
        let (_, bytes) = usecase
            .managed_get_object(&encrypted, &encrypted_key, info.data_dir, None, 3)
            .await
            .expect("AES256 candidate");
        assert_eq!(bytes, Bytes::from_static(b"aes"));
        let (encrypted_usecase, encrypted_bucket, _, _encrypted_reset) = managed_fixture().await;
        let invalid = ManagedObjectContext::from_authorized_request(
            encrypted_bucket.access.clone(),
            encrypted_bucket.request_for_test().clone(),
            ManagedEncryption::Unencrypted,
        )
        .expect("context syntax remains separate from persisted defaults");
        let key = format!("{}invalid.bin", invalid.access.allowed_prefix());
        assert!(matches!(
            encrypted_usecase
                .managed_create_object(&invalid, &key, 0, futures::stream::empty())
                .await,
            Err(ManagedObjectError::EncryptionMismatch)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_create_read_head_range() {
        let (usecase, context, _, _reset) = managed_fixture().await;
        let versioning_lookups =
            crate::app::storage_api::test::VERSIONING_CONFIG_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst);
        let key = format!("{}candidate.bin", context.access.allowed_prefix());
        let payload = Bytes::from_static(b"\0managed binary object\xff");
        let write = usecase
            .managed_create_object(
                &context,
                &key,
                u64::try_from(payload.len()).expect("length"),
                futures::stream::iter([Ok(payload.clone())]),
            )
            .await
            .expect("managed encrypted create");
        let revision = write.data_dir.filter(|value| !value.is_nil()).expect("body revision");
        assert!(write.is_encrypted(), "managed create must encrypt the object");
        let head = usecase
            .managed_head_object(&context, &key, Some(revision))
            .await
            .expect("managed HEAD");
        assert_eq!(
            head.get_actual_size().expect("plaintext length"),
            i64::try_from(payload.len()).expect("length")
        );
        let (info, read) = usecase
            .managed_get_object(&context, &key, Some(revision), None, 1024)
            .await
            .expect("managed GET");
        assert_eq!(info.data_dir, Some(revision));
        assert_eq!(read, payload);
        let (_, ranged) = usecase
            .managed_get_object(&context, &key, Some(revision), Some(1..8), 7)
            .await
            .expect("managed Range");
        assert_eq!(ranged, payload.slice(1..8));
        assert!(matches!(
            usecase.managed_get_object(&context, &key, Some(revision), None, 1).await,
            Err(ManagedObjectError::SizeLimitExceeded)
        ));
        assert!(matches!(
            usecase.managed_head_object(&context, &key, Some(Uuid::new_v4())).await,
            Err(ManagedObjectError::BodyRevisionMismatch)
        ));
        assert_eq!(
            crate::app::storage_api::test::VERSIONING_CONFIG_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst),
            versioning_lookups,
            "managed reads derive unversioned options from the validated instance metadata"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_create_only_preserves_existing_candidate() {
        let (usecase, context, _, _reset) = managed_fixture().await;
        let key = format!("{}immutable.bin", context.access.allowed_prefix());
        let original = Bytes::from_static(b"original");
        let created = usecase
            .managed_create_object(&context, &key, 8, futures::stream::iter([Ok(original.clone())]))
            .await
            .expect("first create");
        let error = usecase
            .managed_create_object(&context, &key, 8, futures::stream::iter([Ok(Bytes::from_static(b"replaced"))]))
            .await
            .expect_err("create-only rejects an existing candidate");
        assert!(matches!(error, ManagedObjectError::Api(ref error) if error.code == S3ErrorCode::PreconditionFailed));
        let (current, bytes) = usecase
            .managed_get_object(&context, &key, created.data_dir, None, 32)
            .await
            .expect("original remains readable");
        assert_eq!(bytes, original);
        assert_eq!(current.data_dir, created.data_dir);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_stale_incarnation_cannot_access_recreated_bucket() {
        use crate::app::storage_api::managed::{BucketHandle, BucketIncarnation};
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions, MakeBucketOptions};
        let (usecase, context, store, _reset) = managed_fixture().await;
        let original = context.access.bucket();
        let bucket = original.storage_bucket_id().as_str();
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete empty managed bucket");
        let replacement = BucketHandle::try_from_parts(
            original.kind(),
            original.account_namespace_id().clone(),
            original.region().into(),
            original.public_name().clone(),
            original.storage_bucket_id().clone(),
            BucketIncarnation::try_new(Uuid::new_v4()).expect("new incarnation"),
        )
        .expect("replacement identity");
        managed::make_managed_bucket(&store, &replacement, AES256_CONFIG, &[], &MakeBucketOptions::default())
            .await
            .expect("recreate same name");
        let key = format!("{}stale.bin", context.access.allowed_prefix());
        assert!(usecase.managed_head_object(&context, &key, None).await.is_err());
        assert!(usecase.managed_get_object(&context, &key, None, None, 32).await.is_err());
        assert!(
            usecase
                .managed_create_object(&context, &key, 5, futures::stream::iter([Ok(Bytes::from_static(b"stale"))]))
                .await
                .is_err()
        );
        let access = ManagedObjectAccess::try_from_parts(
            replacement,
            context.access.table_id().clone(),
            context.access.allowed_prefix().into(),
            [ManagedObjectOperation::CreateOnly].into_iter().collect(),
        )
        .expect("current scope");
        let current =
            ManagedObjectContext::from_authorized_request(access, context.request_for_test().clone(), ManagedEncryption::Aes256)
                .expect("current context");
        usecase
            .managed_create_object(&current, &key, 7, futures::stream::iter([Ok(Bytes::from_static(b"current"))]))
            .await
            .expect("current incarnation accepts create");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_missing_object_returns_storage_error_and_scope_is_required() {
        let (usecase, context, _, _reset) = managed_fixture().await;
        let missing = format!("{}missing.bin", context.access.allowed_prefix());
        for range in [None, Some(0..1)] {
            let error = usecase
                .managed_get_object(&context, &missing, None, range, 32)
                .await
                .expect_err("missing local object");
            assert!(matches!(error, ManagedObjectError::Api(ref error) if error.code == S3ErrorCode::NoSuchKey));
        }
        assert!(matches!(usecase.managed_head_object(&context, &missing, None).await,
            Err(ManagedObjectError::Api(ref error)) if error.code == S3ErrorCode::NoSuchKey));
        assert!(matches!(
            usecase
                .managed_get_object(&context, ".rustfs-table/other/dataset/object", None, None, 32)
                .await,
            Err(ManagedObjectError::Scope(_))
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_write_retains_request_audit() {
        let (usecase, context, _, _reset) = managed_fixture().await;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(bucket);
        let key = format!("{}audit.bin", context.access.allowed_prefix());
        usecase
            .managed_create_object(&context, &key, 5, futures::stream::iter([Ok(Bytes::from_static(b"audit"))]))
            .await
            .expect("audited write");
        probe.wait_for(1, 0).await;
        {
            let audits = probe.audits.lock().expect("audit entries");
            assert_eq!(
                audits[0].request_id.as_deref(),
                context
                    .request_for_test()
                    .request_context()
                    .map(|value| value.request_id.as_str())
            );
            assert_eq!(audits[0].access_key.as_deref(), Some("managed-test-user"));
            assert_eq!(audits[0].api.status.as_deref(), Some("success"));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_write_rejects_kms_denial_with_request_audit() {
        use crate::app::storage_api::test::sse::{RecordingKmsKeyAuthorizer, TestAuthorizationHooks};
        let encryption = ManagedEncryption::AwsKms {
            key_id: "managed-test-key".into(),
        };
        let (usecase, context, store, _reset) = managed_fixture_with_encryption(encryption).await;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(bucket);
        let authorizer = RecordingKmsKeyAuthorizer::new(false);
        let mut extensions = http::Extensions::new();
        let request_context = request_context::RequestContext::fallback();
        extensions.insert(request_context.clone());
        extensions.insert(crate::app::storage_api::test::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: "denied-kms-user".into(),
                ..Default::default()
            }),
            bucket: Some(bucket.into()),
            request_context: Some(request_context),
            ..Default::default()
        });
        extensions.insert(TestAuthorizationHooks {
            enforced: true,
            authorizer: authorizer.clone(),
        });
        let denied = ManagedObjectContext::from_authorized_request(
            context.access.clone(),
            Arc::new(managed_envelope(extensions)),
            ManagedEncryption::AwsKms {
                key_id: "managed-test-key".into(),
            },
        )
        .expect("denied principal remains a real principal");
        let denied_key = format!("{}denied.bin", context.access.allowed_prefix());
        let error = usecase
            .managed_create_object(&denied, &denied_key, 5, futures::stream::iter([Ok(Bytes::from_static(b"audit"))]))
            .await
            .expect_err("KMS denied");
        assert!(matches!(error, ManagedObjectError::Api(ref error) if error.code == S3ErrorCode::AccessDenied));
        assert!(authorizer.calls().iter().any(|(_, key)| key == "managed-test-key"));
        assert!(
            store
                .get_object_info(bucket, &denied_key, &ObjectOptions::default())
                .await
                .is_err()
        );
        probe.wait_for(1, 0).await;
        let audits = probe.audits.lock().expect("audit entries");
        assert_eq!(audits[0].access_key.as_deref(), Some("denied-kms-user"));
        assert_eq!(audits[0].api.status.as_deref(), Some("failure"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_create_obeys_bucket_quota() {
        let (usecase, context, store, _reset) = managed_fixture().await;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let metadata = crate::app::storage_api::test::get_global_bucket_metadata_sys().expect("metadata system");
        QuotaChecker::new(metadata)
            .set_quota_config(bucket, BucketQuota::new(Some(4)))
            .await
            .expect("quota configuration");
        crate::app::storage_api::test::set_disk::initialize_quota_ledger_for_test(store.clone(), bucket)
            .await
            .expect("quota ledger");
        let key = format!("{}quota.bin", context.access.allowed_prefix());
        let error = usecase
            .managed_create_object(&context, &key, 5, futures::stream::iter([Ok(Bytes::from_static(b"large"))]))
            .await
            .expect_err("quota rejects the candidate");
        assert!(matches!(error, ManagedObjectError::Api(ref error)
            if error.code == S3ErrorCode::InvalidRequest && error.message.contains("quota")));
        assert!(store.get_object_info(bucket, &key, &ObjectOptions::default()).await.is_err());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_kms_create_head_get_range_use_real_provider() {
        use crate::app::storage_api::test::sse::{
            RecordingKmsKeyAuthorizer, TestAuthorizationHooks, kms_sse_dek_provider_for_test, set_sse_dek_provider_for_test,
        };
        use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
        let encryption = ManagedEncryption::AwsKms {
            key_id: "managed-kms-key".into(),
        };
        let (usecase, initial, store, _reset) = managed_fixture_with_encryption(encryption.clone()).await;
        let key_dir = tempfile::TempDir::new().expect("KMS key directory");
        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
        manager
            .reconfigure(rustfs_kms::KmsConfig::local(key_dir.path().to_path_buf()).with_insecure_development_defaults())
            .await
            .expect("instance KMS configuration");
        manager
            .get_encryption_service()
            .await
            .expect("KMS service")
            .create_key(CreateKeyRequest {
                key_name: Some("managed-kms-key".into()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("managed KMS key");
        set_sse_dek_provider_for_test(kms_sse_dek_provider_for_test(manager).await.expect("real KMS provider"));
        let authorizer = RecordingKmsKeyAuthorizer::new(true);
        let mut extensions = http::Extensions::new();
        let request_context = request_context::RequestContext::fallback();
        extensions.insert(request_context.clone());
        extensions.insert(crate::app::storage_api::test::ReqInfo {
            cred: Some(rustfs_credentials::Credentials {
                access_key: "managed-kms-user".into(),
                ..Default::default()
            }),
            bucket: Some(initial.access.bucket().storage_bucket_id().as_str().into()),
            request_context: Some(request_context),
            ..Default::default()
        });
        extensions.insert(TestAuthorizationHooks {
            enforced: true,
            authorizer: authorizer.clone(),
        });
        let context =
            ManagedObjectContext::from_authorized_request(initial.access, Arc::new(managed_envelope(extensions)), encryption)
                .expect("KMS operation context");
        let key = format!("{}kms.bin", context.access.allowed_prefix());
        let payload = Bytes::from_static(b"encrypted managed manifest");
        let created = usecase
            .managed_create_object(
                &context,
                &key,
                u64::try_from(payload.len()).expect("payload size"),
                futures::stream::iter([Ok(payload.clone())]),
            )
            .await
            .expect("KMS create");
        let revision = created.data_dir.expect("body revision");
        assert!(created.is_encrypted());
        let head = usecase
            .managed_head_object(&context, &key, Some(revision))
            .await
            .expect("KMS HEAD");
        assert_eq!(
            head.get_actual_size().expect("plaintext length"),
            i64::try_from(payload.len()).expect("size")
        );
        let (_, bytes) = usecase
            .managed_get_object(&context, &key, Some(revision), None, payload.len())
            .await
            .expect("KMS GET");
        assert_eq!(bytes, payload);
        let (_, bytes) = usecase
            .managed_get_object(&context, &key, Some(revision), Some(3..13), 10)
            .await
            .expect("KMS Range");
        assert_eq!(bytes, payload.slice(3..13));
        assert!(authorizer.calls().len() >= 4, "each managed operation must authorize the KMS key");
        let multipart = multipart_context(&context);
        let multipart_key = format!("{}data/kms.lance", context.access.allowed_prefix());
        let upload = usecase
            .managed_create_multipart_upload(&multipart, &multipart_key)
            .await
            .expect("KMS multipart session");
        let part = usecase
            .managed_upload_part(&upload, 1, 7, futures::stream::iter([Ok(Bytes::from_static(b"kmsdata"))]))
            .await
            .expect("KMS encrypted part");
        let completed = usecase
            .managed_complete_multipart_upload(&upload, vec![part])
            .await
            .expect("KMS complete");
        assert!(completed.is_encrypted());
        let (_, multipart_bytes) = usecase
            .managed_get_object(&multipart, &multipart_key, completed.data_dir, Some(1..6), 5)
            .await
            .expect("KMS multipart Range");
        assert_eq!(multipart_bytes, Bytes::from_static(b"msdat"));
        let unavailable = ManagedObjectContext::from_authorized_request(
            context.access.clone(),
            context.request_for_test().clone(),
            ManagedEncryption::AwsKms {
                key_id: "missing-managed-key".into(),
            },
        )
        .expect("explicit unavailable KMS key");
        let unavailable_key = format!("{}unavailable-kms.bin", context.access.allowed_prefix());
        let error = usecase
            .managed_create_object(
                &unavailable,
                &unavailable_key,
                7,
                futures::stream::iter([Ok(Bytes::from_static(b"payload"))]),
            )
            .await
            .expect_err("missing KMS key must reject the write");
        assert!(matches!(error, ManagedObjectError::Api(ref error) if error.code.as_str() == "KMS.NotFoundException"));
        let error = store
            .get_object_info(
                context.access.bucket().storage_bucket_id().as_str(),
                &unavailable_key,
                &ObjectOptions::default(),
            )
            .await
            .expect_err("failed encryption cannot publish a plaintext object");
        assert_eq!(ApiError::from(error).code.as_str(), "NoSuchKey");
    }

    fn managed_lock_counts() -> (u64, u64) {
        crate::app::storage_api::test::bootstrap_instance_ctx()
            .lock_manager()
            .as_fast_lock_manager()
            .expect("managed fixture uses the enabled FastLockManager")
            .current_lock_counts()
    }

    async fn wait_for_managed_lock_counts(expected: (u64, u64)) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while managed_lock_counts() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("managed operations must release their holders and waiters");
    }

    async fn queued_bucket_delete(store: Arc<ECStore>, bucket: String) -> tokio::task::JoinHandle<Result<(), StorageError>> {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, DeleteBucketOptions};
        let waiting_before = managed_lock_counts().1;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let delete = tokio::spawn(async move {
            started_tx.send(()).expect("delete request start signal");
            store.delete_bucket(&bucket, &DeleteBucketOptions::default()).await
        });
        tokio::time::timeout(Duration::from_secs(10), started_rx)
            .await
            .expect("delete request starts")
            .expect("delete start signal");
        tokio::time::timeout(Duration::from_secs(10), async {
            while managed_lock_counts().1 <= waiting_before {
                assert!(!delete.is_finished(), "delete must wait for the held lifecycle guard");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete request enters the actual lock waiter queue");
        assert!(!delete.is_finished(), "observed deletion remains queued");
        delete
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_write_keeps_lifecycle_guard_after_caller_cancel() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};
        let (usecase, context, store, _reset) = managed_fixture().await;
        let locks_before = managed_lock_counts();
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let key = format!("{}cancelled.bin", context.access.allowed_prefix());
        let barrier = PutObjectCommitBarrier::install(&bucket, &key, PutObjectCommitPause::AfterRenameQuorum);
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(&bucket);
        let write_key = key.clone();
        let write = tokio::spawn(async move {
            usecase
                .managed_create_object(&context, &write_key, 7, futures::stream::iter([Ok(Bytes::from_static(b"durable"))]))
                .await
        });
        barrier.wait_until_paused().await;
        write.abort();
        assert!(write.await.expect_err("caller cancelled").is_cancelled());
        let delete = queued_bucket_delete(Arc::clone(&store), bucket.clone()).await;
        barrier.release();
        probe.wait_for(1, 0).await;
        let result = tokio::time::timeout(Duration::from_secs(10), delete)
            .await
            .expect("deletion resumes after the write completes")
            .expect("delete task");
        assert!(matches!(result, Err(StorageError::BucketNotEmpty(_))));
        assert_eq!(
            store
                .get_object_info(&bucket, &key, &ObjectOptions::default())
                .await
                .expect("committed object")
                .get_actual_size()
                .expect("plaintext size"),
            7
        );
        wait_for_managed_lock_counts(locks_before).await;
        let audits = probe.audits.lock().expect("audit entries");
        assert_eq!(audits[0].api.status.as_deref(), Some("success"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_borrowed_guard_reads_while_bucket_writer_waits() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        let (usecase, context, store, _reset) = managed_fixture().await;
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let key = format!("{}borrowed.bin", context.access.allowed_prefix());
        let created = usecase
            .managed_create_object(&context, &key, 7, futures::stream::iter([Ok(Bytes::from_static(b"current"))]))
            .await
            .expect("candidate");
        let locks_before = managed_lock_counts();
        let guard = managed::acquire_bucket_incarnation_fence(&store, &bucket, context.access.bucket().incarnation().as_uuid())
            .await
            .expect("publication lifecycle guard");
        let delete = queued_bucket_delete(Arc::clone(&store), bucket.clone()).await;
        let (_, bytes) = tokio::time::timeout(
            Duration::from_secs(5),
            usecase.managed_get_object_with_guard(&context, &key, created.data_dir, None, 7, &guard),
        )
        .await
        .expect("borrowed guard does not reacquire behind the writer")
        .expect("read candidate under the publication guard");
        assert_eq!(bytes, Bytes::from_static(b"current"));
        drop(guard);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(10), delete)
                .await
                .expect("writer resumes")
                .expect("delete task"),
            Err(StorageError::BucketNotEmpty(_))
        ));
        wait_for_managed_lock_counts(locks_before).await;
        let other_bucket = format!("other-fence-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&other_bucket, &MakeBucketOptions::default())
            .await
            .expect("different bucket");
        let other_incarnation = store
            .bucket_incarnation_id_from_disk(&other_bucket)
            .await
            .expect("different incarnation");
        let other_guard = managed::acquire_bucket_incarnation_fence(&store, &other_bucket, other_incarnation)
            .await
            .expect("different bucket guard");
        assert!(matches!(
            usecase.managed_get_object_with_guard(&context, &key, created.data_dir, None, 7, &other_guard).await,
            Err(ManagedObjectError::Api(ref error)) if error.code == S3ErrorCode::PreconditionFailed
        ));
        drop(other_guard);
        wait_for_managed_lock_counts(locks_before).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_write_reuses_lifecycle_guard_after_body_staging() {
        let (usecase, context, store, _reset) = managed_fixture().await;
        let locks_before = managed_lock_counts();
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let key = format!("{}staged.bin", context.access.allowed_prefix());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let body = futures::stream::once(async move {
            entered_tx.send(()).expect("body entered");
            release_rx.await.map_err(io::Error::other)?;
            Ok(Bytes::from_static(b"staged"))
        });
        let write_key = key.clone();
        let write = tokio::spawn(async move { usecase.managed_create_object(&context, &write_key, 6, body).await });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("body staging starts")
            .expect("body signal");
        let delete = queued_bucket_delete(Arc::clone(&store), bucket.clone()).await;
        release_tx.send(()).expect("release body");
        let created = tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("PUT reuses its existing lifecycle coverage")
            .expect("write task")
            .expect("staged write commits");
        assert_eq!(created.get_actual_size().expect("plaintext size"), 6);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(10), delete)
                .await
                .expect("writer resumes")
                .expect("delete task"),
            Err(StorageError::BucketNotEmpty(_))
        ));
        wait_for_managed_lock_counts(locks_before).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn managed_write_rechecks_lowered_quota_after_body_staging() {
        use crate::app::storage_api::managed::ManagedTableId;
        use crate::app::storage_api::test::set_disk::initialize_quota_ledger_for_test;
        let (usecase, context, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        crate::app::storage_api::test::sse::set_sse_dek_provider_for_test(Arc::new(
            crate::app::storage_api::test::sse::LocalSseDekProvider::new_with_key([37; 32]),
        ));
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let encrypted_table_id = ManagedTableId::try_new(Uuid::new_v4().to_string()).expect("encrypted Index identity");
        let encrypted_prefix = format!(".rustfs-table/{}/dataset/", encrypted_table_id.as_str());
        let encrypted_access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            encrypted_table_id,
            encrypted_prefix,
            [ManagedObjectOperation::CreateOnly].into_iter().collect(),
        )
        .expect("separate encrypted Index scope");
        let key = format!("{}lowered-quota.bin", encrypted_access.allowed_prefix());
        let mut checker = QuotaChecker::new(usecase.bucket_metadata_sys().expect("fixture metadata system"));
        checker
            .set_quota_config(&bucket, BucketQuota::new(Some(64)))
            .await
            .expect("initial quota allows the candidate");
        initialize_quota_ledger_for_test(Arc::clone(&store), &bucket)
            .await
            .expect("initial quota ledger");
        let locks_before = managed_lock_counts();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let body = futures::stream::once(async move {
            entered_tx.send(()).expect("body entered streaming staging");
            release_rx.await.map_err(io::Error::other)?;
            Ok(Bytes::from_static(b"staged"))
        });
        let write_context = ManagedObjectContext::from_authorized_request(
            encrypted_access,
            context.request_for_test().clone(),
            ManagedEncryption::Aes256,
        )
        .expect("fixed AES256 configuration for the streaming Index");
        let write_usecase = DefaultObjectUsecase::with_context(usecase.context.clone());
        let write_key = key.clone();
        let write = tokio::spawn(async move { write_usecase.managed_create_object(&write_context, &write_key, 6, body).await });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("AES256 streaming reaches body staging")
            .expect("body staging signal");
        tokio::time::timeout(Duration::from_secs(10), checker.set_quota_config(&bucket, BucketQuota::new(Some(4))))
            .await
            .expect("quota metadata can change during body staging")
            .expect("lower quota before commit");
        let metadata = managed::get_bucket_metadata(&store, &bucket)
            .await
            .expect("persisted quota metadata");
        let persisted: BucketQuota = serde_json::from_slice(&metadata.quota_config_json).expect("persisted quota config");
        assert_eq!(persisted.quota, Some(4));
        tokio::time::timeout(Duration::from_secs(10), initialize_quota_ledger_for_test(Arc::clone(&store), &bucket))
            .await
            .expect("new quota revision ledger preparation completes")
            .expect("new quota revision ledger");
        assert!(!write.is_finished(), "candidate remains paused before commit-time quota validation");
        release_tx.send(()).expect("release staged body after quota update");
        let error = tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("staged PUT completes its quota check")
            .expect("staged PUT task")
            .expect_err("commit must use the lowered quota");
        assert!(matches!(error, ManagedObjectError::Api(ref error)
            if error.code == S3ErrorCode::InvalidRequest && error.message.contains("quota")));
        let missing = store
            .get_object_info(&bucket, &key, &ObjectOptions::default())
            .await
            .expect_err("quota rejection must not publish the candidate");
        assert_eq!(ApiError::from(missing).code, S3ErrorCode::NoSuchKey);
        wait_for_managed_lock_counts(locks_before).await;

        // A separate unencrypted Index measures the same bucket's remaining capacity without SSE framing.
        let fitting_key = format!("{}fits-quota.bin", context.access.allowed_prefix());
        let fitted = tokio::time::timeout(
            Duration::from_secs(10),
            usecase.managed_create_object(&context, &fitting_key, 4, futures::stream::iter([Ok(Bytes::from_static(b"fits"))])),
        )
        .await
        .expect("next write can acquire the released guards")
        .expect("failed candidate must not retain quota capacity");
        assert_eq!(fitted.get_actual_size().expect("fitting plaintext size"), 4);
        assert_eq!(quota_object_size(&fitted).expect("actual quota charge"), 4);
        let (_, bytes) = usecase
            .managed_get_object(&context, &fitting_key, fitted.data_dir, None, 4)
            .await
            .expect("fitting candidate contents");
        assert_eq!(bytes, Bytes::from_static(b"fits"));
        let excess_key = format!("{}exceeds-quota.bin", context.access.allowed_prefix());
        let error = usecase
            .managed_create_object(&context, &excess_key, 1, futures::stream::iter([Ok(Bytes::from_static(b"x"))]))
            .await
            .expect_err("successful candidate consumes all four quota bytes");
        assert!(matches!(error, ManagedObjectError::Api(ref error)
            if error.code == S3ErrorCode::InvalidRequest && error.message.contains("quota")));
        let missing = store
            .get_object_info(&bucket, &excess_key, &ObjectOptions::default())
            .await
            .expect_err("over-quota object must remain absent");
        assert_eq!(ApiError::from(missing).code, S3ErrorCode::NoSuchKey);
        wait_for_managed_lock_counts(locks_before).await;
    }

    struct MaintenanceTestAuthorizer {
        runtime: Arc<crate::app::storage_api::object_usecase::sse::ManagedSseRuntime>,
        enabled: std::sync::atomic::AtomicBool,
        generation: std::sync::Mutex<[u8; 16]>,
        operations: std::sync::Mutex<Vec<ManagedObjectOperation>>,
    }

    struct MaintenanceTestKeyAuthorizer;
    #[async_trait::async_trait]
    impl crate::app::storage_api::object_usecase::sse::KmsKeyAuthorizer for MaintenanceTestKeyAuthorizer {
        async fn is_allowed(
            &self,
            _: &SseKmsPrincipal,
            _: rustfs_policy::policy::action::KmsAction,
            _: &str,
        ) -> Result<bool, rustfs_sse::SseError> {
            Ok(true)
        }
    }

    #[async_trait::async_trait]
    impl super::super::managed_maintenance::ManagedMaintenanceAuthorizer for MaintenanceTestAuthorizer {
        async fn authorize(
            &self,
            _: &ManagedObjectAccess,
            operation: ManagedObjectOperation,
            _: &str,
            _: &ManagedEncryption,
            audit: &super::super::managed_maintenance::ManagedMaintenanceAuditContext,
        ) -> Result<super::super::managed_maintenance::ManagedMaintenanceAdmission, ManagedObjectError> {
            if !self.enabled.load(std::sync::atomic::Ordering::Acquire)
                || self.generation.lock().expect("generation").as_slice() != audit.execution_identity().user_generation()
            {
                return Err(ApiError::access_denied().into());
            }
            self.operations.lock().expect("operations").push(operation);
            let mut context = rustfs_kms::types::OperationContext::new(audit.execution_identity().account().to_owned());
            context.operation_id = audit.operation_id();
            let principal = SseKmsPrincipal::for_managed_maintenance(
                &rustfs_credentials::Credentials {
                    access_key: audit.execution_identity().account().to_owned(),
                    ..Default::default()
                },
                HashMap::new(),
                &audit.operation_id().to_string(),
                Arc::new(MaintenanceTestKeyAuthorizer),
                Arc::new(self.runtime.begin_operation(context).await),
            );
            super::super::managed_maintenance::ManagedMaintenanceAdmission::new(principal, audit.clone())
        }
    }

    fn maintenance_context(
        foreground: &ManagedObjectContext,
        runtime: Arc<crate::app::storage_api::object_usecase::sse::ManagedSseRuntime>,
    ) -> (ManagedObjectContext, Arc<MaintenanceTestAuthorizer>) {
        use super::super::managed_maintenance::*;
        let authorizer = Arc::new(MaintenanceTestAuthorizer {
            runtime,
            enabled: std::sync::atomic::AtomicBool::new(true),
            generation: std::sync::Mutex::new([3; 16]),
            operations: std::sync::Mutex::new(Vec::new()),
        });
        let audit = ManagedMaintenanceAuditContext::new(
            ManagedMaintenanceIdentityRef::new("maintenance-account".to_owned(), [3; 16]),
            Uuid::new_v4(),
            1,
            "worker".to_owned(),
            Uuid::new_v4(),
            None,
        );
        let context = ManagedObjectContext::from_maintenance(
            foreground.access.clone(),
            foreground.encryption.clone(),
            Arc::new(ManagedMaintenanceContext::new(audit, authorizer.clone())),
        )
        .expect("maintenance context");
        (context, authorizer)
    }

    async fn maintenance_kms_runtime() -> (
        tempfile::TempDir,
        Arc<rustfs_kms::KmsServiceManager>,
        Arc<crate::app::storage_api::object_usecase::sse::ManagedSseRuntime>,
    ) {
        let directory = tempfile::tempdir().expect("instance key directory");
        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
        manager
            .reconfigure(rustfs_kms::KmsConfig::local(directory.path().to_path_buf()).with_insecure_development_defaults())
            .await
            .expect("configure instance KMS");
        manager
            .get_encryption_service()
            .await
            .expect("instance encryption service")
            .create_key(rustfs_kms::types::CreateKeyRequest {
                key_name: Some("maintenance-key".to_owned()),
                key_usage: rustfs_kms::types::KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("create instance encryption key");
        let runtime = Arc::new(crate::app::storage_api::object_usecase::sse::ManagedSseRuntime::new(Some(
            manager.clone(),
        )));
        (directory, manager, runtime)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn maintenance_revocation_and_generation_change_stop_the_next_real_io() {
        let (usecase, foreground, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let (context, authorizer) = maintenance_context(
            &foreground,
            Arc::new(crate::app::storage_api::object_usecase::sse::ManagedSseRuntime::new(None)),
        );
        let key = format!("{}maintenance.bin", context.access.allowed_prefix());
        let info = usecase
            .managed_create_object(&context, &key, 4, futures::stream::iter([Ok(Bytes::from_static(b"data"))]))
            .await
            .expect("unencrypted operation needs no KMS");
        let (_, data) = usecase
            .managed_get_object(&context, &key, info.data_dir, None, 4)
            .await
            .expect("current read admission");
        assert_eq!(data, Bytes::from_static(b"data"));
        authorizer.enabled.store(false, std::sync::atomic::Ordering::Release);
        assert!(
            usecase
                .managed_get_object(&context, &key, info.data_dir, None, 4)
                .await
                .is_err(),
            "revocation refuses next GET"
        );
        authorizer.enabled.store(true, std::sync::atomic::Ordering::Release);
        *authorizer.generation.lock().expect("change generation") = [4; 16];
        let refused = format!("{}refused.bin", context.access.allowed_prefix());
        assert!(
            usecase
                .managed_create_object(&context, &refused, 4, futures::stream::iter([Ok(Bytes::from_static(b"data"))]))
                .await
                .is_err()
        );
        assert!(
            store
                .get_object_info(context.access.bucket().storage_bucket_id().as_str(), &refused, &ObjectOptions::default())
                .await
                .is_err(),
            "changed generation must not dispatch a write"
        );
        assert_eq!(
            *authorizer.operations.lock().expect("admissions"),
            [ManagedObjectOperation::CreateOnly, ManagedObjectOperation::Get]
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn preparation_reads_and_denials_audit_real_preparation_identity() {
        use super::super::managed_maintenance::*;
        let (usecase, foreground, _store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let key = format!("{}preparation.bin", foreground.access.allowed_prefix());
        let body = Bytes::from_static(b"preparation-data");
        let info = usecase
            .managed_create_object(
                &foreground,
                &key,
                u64::try_from(body.len()).expect("body size"),
                futures::stream::iter([Ok(body.clone())]),
            )
            .await
            .expect("source object");
        let bucket = foreground.access.bucket().storage_bucket_id().as_str();
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(bucket);
        let mut expected = HashMap::new();
        for purpose in [
            ManagedMaintenancePreparationPurpose::Enroll,
            ManagedMaintenancePreparationPurpose::GarbageCollectionPlanning,
            ManagedMaintenancePreparationPurpose::RecoveryPlanning,
        ] {
            let preparation_id = Uuid::new_v4();
            let (_, authorizer) = maintenance_context(
                &foreground,
                Arc::new(crate::app::storage_api::object_usecase::sse::ManagedSseRuntime::new(None)),
            );
            let audit = ManagedMaintenanceAuditContext::new_preparation(
                ManagedMaintenanceIdentityRef::new("maintenance-account".to_owned(), [3; 16]),
                preparation_id,
                purpose,
                "preparation-worker".to_owned(),
                Uuid::new_v4(),
                None,
            );
            assert_eq!((audit.job_id(), audit.attempt()), (None, None));
            assert_eq!(audit.preparation_id(), Some(preparation_id));
            assert_eq!(audit.preparation_purpose(), Some(purpose));
            expected.insert(preparation_id.to_string(), (purpose.as_str(), audit.worker_boot_id().to_string()));
            let context = ManagedObjectContext::from_maintenance(
                foreground.access.clone(),
                foreground.encryption.clone(),
                Arc::new(ManagedMaintenanceContext::new(audit, authorizer.clone())),
            )
            .expect("preparation context");
            let (_, actual) = usecase
                .managed_get_object(&context, &key, info.data_dir, None, body.len())
                .await
                .expect("preparation GET");
            assert_eq!(actual, body);
            let (_, ranged) = usecase
                .managed_get_object(&context, &key, info.data_dir, Some(2..7), 5)
                .await
                .expect("preparation Range");
            assert_eq!(ranged.as_ref(), &body[2..7]);
            assert!(
                matches!(
                    usecase
                        .managed_get_object(&context, ".rustfs-table/other/dataset/denied", None, None, body.len())
                        .await,
                    Err(ManagedObjectError::Scope(_))
                ),
                "out-of-scope access must remain denied and audited"
            );
            authorizer.enabled.store(false, std::sync::atomic::Ordering::Release);
            assert!(
                matches!(usecase.managed_get_object(&context, &key, info.data_dir, None, body.len()).await,
                Err(ManagedObjectError::Api(error)) if error.code == S3ErrorCode::AccessDenied)
            );
        }
        probe.wait_for(12, 0).await;
        let audits = probe.audits.lock().expect("preparation audit entries");
        assert_eq!(audits.len(), 12);
        let mut operation_ids = std::collections::HashSet::new();
        let mut denied = 0;
        for entry in audits.iter() {
            assert_eq!(entry.access_key.as_deref(), Some("maintenance-account"));
            let tags = entry.tags.as_ref().expect("preparation audit tags");
            assert!(!tags.contains_key("maintenanceJobId"));
            assert!(!tags.contains_key("maintenanceAttempt"));
            let preparation_id = tags["maintenancePreparationId"].as_str().expect("preparation id");
            let (purpose, boot) = expected.get(preparation_id).expect("actual admitted preparation");
            assert_eq!(tags["maintenancePreparationPurpose"], *purpose);
            assert_eq!(tags["maintenanceWorkerId"], "preparation-worker");
            assert_eq!(tags["maintenanceWorkerBootId"].as_str(), Some(boot.as_str()));
            let operation_id = entry.request_id.as_deref().expect("fresh operation id");
            assert_eq!(tags["maintenanceOperationId"], operation_id);
            assert!(operation_ids.insert(operation_id.to_owned()), "each admission has its own operation id");
            if entry.api.status.as_deref() == Some("failure") {
                denied += 1;
            }
        }
        assert_eq!(denied, 6, "each denied admission preserves its identity and failure outcome");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn maintenance_actual_get_and_range_use_the_bound_kms_instance() {
        let (usecase, foreground, _store, _reset) = managed_fixture_with_encryption(ManagedEncryption::AwsKms {
            key_id: "maintenance-key".to_owned(),
        })
        .await;
        let (_b_directory, _b_manager, b) = maintenance_kms_runtime().await;
        let (_a_directory, _a_manager, a) = maintenance_kms_runtime().await;
        let (context, _) = maintenance_context(&foreground, b);
        let (wrong_context, _) = maintenance_context(&foreground, a);
        for size in [32_usize, 1024 * 1024] {
            let key = format!("{}kms-{size}.bin", context.access.allowed_prefix());
            let plaintext = Bytes::from(vec![0x54; size]);
            let info = usecase
                .managed_create_object(
                    &context,
                    &key,
                    u64::try_from(size).expect("size"),
                    futures::stream::iter([Ok(plaintext.clone())]),
                )
                .await
                .expect("bound instance encrypted PUT");
            let (_, actual) = usecase
                .managed_get_object(&context, &key, info.data_dir, None, size)
                .await
                .expect("real bound GET");
            assert_eq!(actual, plaintext);
            let (_, ranged) = usecase
                .managed_get_object(&context, &key, info.data_dir, Some(7..23), 16)
                .await
                .expect("real bound Range GET");
            assert_eq!(ranged.as_ref(), &plaintext[7..23]);
            assert!(
                usecase
                    .managed_get_object(&wrong_context, &key, info.data_dir, None, size)
                    .await
                    .is_err(),
                "other instance cannot decrypt this body"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn maintenance_multipart_steps_reauthorize_and_read_bound_ciphertext() {
        let (usecase, foreground, _store, _reset) = managed_fixture_with_encryption(ManagedEncryption::AwsKms {
            key_id: "maintenance-key".to_owned(),
        })
        .await;
        let (_directory, _manager, runtime) = maintenance_kms_runtime().await;
        let (mut context, authorizer) = maintenance_context(&foreground, runtime);
        context.access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            context.access.table_id().clone(),
            context.access.allowed_prefix().to_owned(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Range,
                ManagedObjectOperation::Head,
            ]
            .into_iter()
            .collect(),
        )
        .expect("multipart capability");
        let key = format!("{}multipart.lance", context.access.allowed_prefix());
        let upload = usecase
            .managed_create_multipart_upload(&context, &key)
            .await
            .expect("bound multipart session");
        let plaintext = Bytes::from(vec![0x53; 128 * 1024]);
        let part = usecase
            .managed_upload_part(
                &upload,
                1,
                u64::try_from(plaintext.len()).expect("part size"),
                futures::stream::iter([Ok(plaintext.clone())]),
            )
            .await
            .expect("bound encrypted part");
        authorizer.enabled.store(false, std::sync::atomic::Ordering::Release);
        assert!(
            usecase
                .managed_complete_multipart_upload(&upload, vec![part.clone()])
                .await
                .is_err(),
            "permission removal blocks completion"
        );
        authorizer.enabled.store(true, std::sync::atomic::Ordering::Release);
        let info = usecase
            .managed_complete_multipart_upload(&upload, vec![part])
            .await
            .expect("currently authorized completion");
        let (_, actual) = usecase
            .managed_get_object(&context, &key, info.data_dir, None, plaintext.len())
            .await
            .expect("actual multipart GET");
        assert_eq!(actual, plaintext);
        let (_, actual) = usecase
            .managed_get_object(&context, &key, info.data_dir, Some(1024..2048), 1024)
            .await
            .expect("actual multipart Range");
        assert_eq!(actual.as_ref(), &plaintext[1024..2048]);
        assert_eq!(
            *authorizer.operations.lock().expect("admissions"),
            [
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Range
            ]
        );
    }

    async fn exact_delete_permit(
        context: &ManagedObjectContext,
        store: &Arc<ECStore>,
        key: &str,
        info: &ObjectInfo,
    ) -> super::super::managed_maintenance::ManagedRetirementDeletePermit {
        use super::super::managed_maintenance::*;
        let ManagedObjectIdentity::Maintenance { context: maintenance } = &context.identity else {
            panic!("maintenance context")
        };
        let audit = &maintenance.audit;
        let bucket = context.access.bucket().storage_bucket_id().as_str();
        let lifecycle = Arc::new(
            store
                .acquire_bucket_incarnation_fence(bucket, context.access.bucket().incarnation().as_uuid())
                .await
                .expect("exact lifecycle owner"),
        );
        let lock = store
            .new_ns_lock(bucket, &format!("dispatch/{}", audit.job_id().expect("persisted job identity")))
            .await
            .expect("dispatch namespace");
        let dispatch = Arc::new(
            lock.get_read_lock(Duration::from_secs(10))
                .await
                .expect("actual dispatch read owner"),
        );
        let retained: Arc<dyn Send + Sync> = Arc::new((lifecycle.clone(), dispatch.clone()));
        let signals = dispatch.lock_lost_signal().into_iter().collect();
        ManagedRetirementDeletePermit::from_verified_dispatch(ManagedRetirementDeletePermitParts {
            access: context.access.clone(),
            target: ManagedRetiredObject {
                key: key.to_owned(),
                body_revision: info.data_dir.expect("body revision"),
                mod_time: info.mod_time,
                content_length: u64::try_from(info.size).expect("stored size"),
                version_id: None,
                delete_marker: false,
            },
            retirement_id: Uuid::new_v4(),
            retirement_revision: Uuid::new_v4(),
            job_id: audit.job_id().expect("persisted job identity"),
            job_revision: Uuid::new_v4(),
            attempt: audit.attempt().expect("persisted job attempt"),
            lease_id: Uuid::new_v4(),
            worker_id: audit.worker_id().to_owned(),
            worker_boot_id: audit.worker_boot_id(),
            dispatch_id: Uuid::new_v4(),
            ordinal: 0,
            execution_identity: audit.execution_identity().clone(),
            lifecycle_owner: lifecycle,
            dispatch_owner: dispatch,
            retained_guard_owner: retained,
            retained_guard_loss_signals: signals,
        })
        .expect("verified exact permit")
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn preparation_context_cannot_consume_a_job_delete_permit() {
        use super::super::managed_maintenance::*;
        let (usecase, foreground, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let key = format!("{}retirement.bin", foreground.access.allowed_prefix());
        let info = usecase
            .managed_create_object(&foreground, &key, 4, futures::stream::iter([Ok(Bytes::from_static(b"data"))]))
            .await
            .expect("existing body");
        let (mut job, authorizer) = maintenance_context(
            &foreground,
            Arc::new(crate::app::storage_api::object_usecase::sse::ManagedSseRuntime::new(None)),
        );
        job.access = ManagedObjectAccess::try_from_parts(
            job.access.bucket().clone(),
            job.access.table_id().clone(),
            job.access.allowed_prefix().to_owned(),
            [ManagedObjectOperation::Delete].into_iter().collect(),
        )
        .expect("job deletion capability");
        let permit = exact_delete_permit(&job, &store, &key, &info).await;
        let ManagedObjectIdentity::Maintenance { context: maintenance } = &job.identity else {
            panic!("job identity")
        };
        let preparation = ManagedMaintenanceAuditContext::new_preparation(
            maintenance.audit.execution_identity().clone(),
            Uuid::new_v4(),
            ManagedMaintenancePreparationPurpose::RecoveryPlanning,
            maintenance.audit.worker_id().to_owned(),
            maintenance.audit.worker_boot_id(),
            None,
        );
        let context = ManagedObjectContext::from_maintenance(
            job.access.clone(),
            ManagedEncryption::Unencrypted,
            Arc::new(ManagedMaintenanceContext::new(preparation, authorizer.clone())),
        )
        .expect("preparation context");
        assert!(matches!(usecase.delete_retired_object(context, permit).await,
            Err(ManagedObjectError::Api(error)) if error.code == S3ErrorCode::PreconditionFailed));
        assert!(
            authorizer.operations.lock().expect("admissions").is_empty(),
            "a preparation cannot admit job deletion"
        );
        let current = store
            .get_object_info_for_delete(job.access.bucket().storage_bucket_id().as_str(), &key, &ObjectOptions::default())
            .await
            .expect("body survives invalid origin");
        assert_eq!(current.data_dir, info.data_dir);
    }

    #[cfg(not(windows))]
    #[tokio::test]
    #[serial_test::serial]
    async fn maintenance_delete_audit_retains_real_job_through_cancelled_native_work() {
        use crate::app::storage_api::test::{
            LocalPublicationPause, LocalPublicationStage, ReadOptions, StorageDiskRpcExt as _, find_local_disk_by_ref,
        };
        let (usecase, foreground, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let (mut context, _) = maintenance_context(
            &foreground,
            Arc::new(crate::app::storage_api::object_usecase::sse::ManagedSseRuntime::new(None)),
        );
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        let key = format!("{}native-retirement.bin", context.access.allowed_prefix());
        let mut reader = PutObjReader::from_vec(vec![0x52; 1024 * 1024]);
        let info = store
            .put_object(
                &bucket,
                &key,
                &mut reader,
                &ObjectOptions {
                    write_completion: crate::app::storage_api::object_usecase::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("external body for native cleanup");
        context.access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            context.access.table_id().clone(),
            context.access.allowed_prefix().to_owned(),
            [ManagedObjectOperation::Delete].into_iter().collect(),
        )
        .expect("deletion scope");
        let ManagedObjectIdentity::Maintenance { context: maintenance } = &context.identity else {
            panic!("maintenance identity")
        };
        let expected_job_id = maintenance.audit.job_id().expect("real job").to_string();
        let expected_attempt = maintenance.audit.attempt().expect("real attempt");
        let expected_boot = maintenance.audit.worker_boot_id().to_string();
        let permit = exact_delete_permit(&context, &store, &key, &info).await;
        let endpoints = store.instance_endpoints().expect("store endpoints");
        let endpoint = endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref())
            .find(|endpoint| endpoint.is_local)
            .expect("local disk");
        let disk = find_local_disk_by_ref(&endpoint.to_string()).await.expect("selected disk");
        let physical = disk
            .read_version("", &bucket, &key, "", &ReadOptions::default())
            .await
            .expect("physical body metadata");
        assert!(!physical.inline_data(), "native pause must cover an external payload");
        assert_eq!(physical.data_dir, info.data_dir);
        let part_path = format!("{key}/{}/part.1", physical.data_dir.expect("physical data directory"));
        let mut pause = LocalPublicationPause::install(&disk, &bucket, &part_path, LocalPublicationStage::Remove)
            .expect("pause native removal");
        let probe = crate::app::storage_api::object_usecase::helper::MutationCompletionProbe::install(&bucket);
        let mut deletion = tokio::spawn(async move { usecase.delete_retired_object(context, permit).await });
        tokio::select! {
            biased;
            result = &mut deletion => panic!("delete returned before native removal pause: {}", result.is_ok()),
            entered = tokio::time::timeout(Duration::from_secs(10), pause.entered()) => {
                entered.expect("physical cleanup reached").expect("native cleanup entry signal");
            }
        }
        deletion.abort();
        assert!(
            matches!(deletion.await, Err(error) if error.is_cancelled()),
            "outer waiter must be cancelled"
        );
        assert!(
            probe.audits.lock().expect("audit owner").is_empty(),
            "native holder retains the audit until completion"
        );
        drop(pause);
        probe.wait_for(1, 0).await;
        let audits = probe.audits.lock().expect("completed native audit");
        assert_eq!(audits.len(), 1);
        let audit = &audits[0];
        assert_eq!(audit.access_key.as_deref(), Some("maintenance-account"));
        assert_eq!(audit.api.status.as_deref(), Some("success"));
        let tags = audit.tags.as_ref().expect("job audit tags");
        assert_eq!(tags["maintenanceJobId"].as_str(), Some(expected_job_id.as_str()));
        assert_eq!(tags["maintenanceAttempt"], expected_attempt);
        assert_eq!(tags["maintenanceWorkerBootId"].as_str(), Some(expected_boot.as_str()));
        assert!(!tags.contains_key("maintenancePreparationId"));
        assert!(!tags.contains_key("maintenancePreparationPurpose"));
        assert_eq!(tags["maintenanceOperationId"].as_str(), audit.request_id.as_deref());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn maintenance_exact_delete_preserves_replacement_and_needs_no_kms() {
        use super::super::managed_maintenance::ManagedDeleteOutcome;
        use crate::app::storage_api::test::data_usage::{
            apply_bucket_usage_memory_overlay, get_bucket_usage_memory, seed_bucket_usage_memory_for_test,
        };

        async fn observed_usage(bucket: &str) -> Option<u64> {
            let mut usage = rustfs_data_usage::DataUsageInfo::default();
            apply_bucket_usage_memory_overlay(&mut usage).await;
            usage.buckets_usage.get(bucket).map(|entry| entry.size)
        }
        let (usecase, foreground, store, _reset) = managed_fixture_with_encryption(ManagedEncryption::AwsKms {
            key_id: "maintenance-key".to_owned(),
        })
        .await;
        let (_directory, manager, runtime) = maintenance_kms_runtime().await;
        let (mut context, authorizer) = maintenance_context(&foreground, runtime);
        let key = format!("{}retired.bin", context.access.allowed_prefix());
        let info = usecase
            .managed_create_object(&context, &key, 32, futures::stream::iter([Ok(Bytes::from(vec![0x41; 32]))]))
            .await
            .expect("real encrypted retired body");
        context.access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            context.access.table_id().clone(),
            context.access.allowed_prefix().to_owned(),
            [ManagedObjectOperation::Delete, ManagedObjectOperation::Get]
                .into_iter()
                .collect(),
        )
        .expect("retirement access");
        let bucket = context.access.bucket().storage_bucket_id().as_str().to_owned();
        seed_bucket_usage_memory_for_test(&bucket, 2_000).await;
        manager.stop().await.expect("stop the exact KMS instance");
        assert!(
            usecase
                .managed_get_object(&context, &key, info.data_dir, None, 32)
                .await
                .is_err(),
            "encrypted content still needs KMS"
        );
        let mut replacement_claim = info.clone();
        replacement_claim.data_dir = Some(Uuid::new_v4());
        let mismatch = exact_delete_permit(&context, &store, &key, &replacement_claim).await;
        assert!(matches!(
            usecase
                .delete_retired_object(context.clone(), mismatch)
                .await
                .expect("delete outcome"),
            ManagedDeleteOutcome::NeedsRecovery { .. }
        ));
        assert_eq!(get_bucket_usage_memory(&bucket).await, Some(2_000));
        assert_eq!(observed_usage(&bucket).await, Some(2_000));
        let current = store
            .get_object_info_for_delete(context.access.bucket().storage_bucket_id().as_str(), &key, &ObjectOptions::default())
            .await
            .expect("replacement survived");
        assert_eq!(current.data_dir, info.data_dir);
        let permit = exact_delete_permit(&context, &store, &key, &info).await;
        let ManagedDeleteOutcome::DeleteCommitted { accounting } = usecase
            .delete_retired_object(context.clone(), permit)
            .await
            .expect("metadata-only exact delete")
        else {
            panic!("unavailable wrapping key cannot prevent a proven exact deletion")
        };
        assert!(accounting.removed_current_object);
        assert!(accounting.version_id.is_none());
        let expected_usage = 2_000 - accounting.size.expect("actual encrypted body accounting size");
        assert_eq!(observed_usage(&bucket).await, Some(expected_usage));
        assert_eq!(get_bucket_usage_memory(&bucket).await, Some(2_000));
        let replay = exact_delete_permit(&context, &store, &key, &info).await;
        assert!(
            matches!(
                usecase.delete_retired_object(context, replay).await.expect("replay outcome"),
                ManagedDeleteOutcome::NeedsRecovery { error: None }
            ),
            "unproved absence must supply no second accounting receipt"
        );
        assert_eq!(
            observed_usage(&bucket).await,
            Some(expected_usage),
            "an absent replay cannot decrement usage again"
        );
        assert_eq!(
            get_bucket_usage_memory(&bucket).await,
            Some(2_000),
            "quota retains its scanner reconciliation floor"
        );
        assert_eq!(
            authorizer
                .operations
                .lock()
                .expect("admissions")
                .iter()
                .filter(|operation| **operation == ManagedObjectOperation::Delete)
                .count(),
            3,
            "every dispatch has a fresh deletion admission"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn foreground_managed_context_rejects_delete_capability() {
        let (_usecase, context, _store, _reset) = managed_fixture_with_encryption(ManagedEncryption::Unencrypted).await;
        let access = ManagedObjectAccess::try_from_parts(
            context.access.bucket().clone(),
            context.access.table_id().clone(),
            context.access.allowed_prefix().to_owned(),
            [ManagedObjectOperation::Delete].into_iter().collect(),
        )
        .expect("delete scope data");
        assert!(matches!(
            ManagedObjectContext::from_authorized_request(
                access,
                context.request_for_test().clone(),
                ManagedEncryption::Unencrypted
            ),
            Err(ManagedObjectError::Scope(BucketIdentityError::OperationNotAllowed))
        ));
    }

    #[test]
    fn managed_range_preserves_half_open_bounds() {
        let (range, count) = managed_range(Some(2..5), 7).expect("valid range");
        let range = range.expect("range present");
        assert_eq!((range.start, range.end, count), (2, 4, 3));
        for range in [0..0, Range { start: 3, end: 2 }, 1..8] {
            assert!(managed_range(Some(range), 7).is_err());
        }
        assert_eq!(managed_range(None, 0).expect("empty object").1, 0);
    }
}
