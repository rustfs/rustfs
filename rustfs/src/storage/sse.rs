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

//! The server's side of server-side encryption.
//!
//! Key resolution, metadata and validation live in the `rustfs-sse` crate, which
//! knows neither the storage engine nor IAM. This module is the seam between
//! the two: it gives that crate the server it runs in ([`RustfsSseHost`]),
//! binds its host-taking entry points to that server, and carries its results
//! across the storage-engine boundary (the read-side resolver, the write plan,
//! the multipart completion gate).

use super::StorageError;
use super::storage_api::ecstore_object::{
    EncryptionResolutionError, EncryptionResolutionErrorKind, ObjectEncryptionResolver, ReadEncryptionMaterial,
    ReadEncryptionMode, ReadEncryptionRequest,
};
use super::{Error, WriteEncryption, get_bucket_sse_config};
use crate::error::ApiError;
use crate::runtime_sources::current_kms_runtime_service_manager;
use crate::storage::authorization::resource_free_condition_values;
use crate::storage::authorization::{AuthorizationRequest, request_context_from_req};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use async_trait::async_trait;
use http::HeaderMap;
use rustfs_kms::KmsServiceManager;
use rustfs_kms::service::ObjectEncryptionService;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::{Action, KmsAction};
use rustfs_s3_types::S3ErrorCode;
use rustfs_sse::{
    BucketDefaultEncryption, BucketEncryption, DEFAULT_SSE_ALGORITHM, KMS_KEY_NOT_FOUND_ERROR_CODE, SseError, SseHost,
    normalize_encryption_metadata_case, ssec_invalid_request,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

pub(crate) use rustfs_sse::startup::init as init_sse_startup;
#[cfg(test)]
pub(crate) use rustfs_sse::testing::{RecordingKmsKeyAuthorizer, sse_kms_object_metadata};
pub(crate) use rustfs_sse::{
    DecryptionMaterial, DecryptionRequest, EncryptionKeyKind, EncryptionMaterial, EncryptionRequest, KmsRequestAuditScope,
    ObjectDekRewrapOutcome, PrepareEncryptionRequest, SseKmsPrincipal, SseReadResponseHeaders, SsecParamsFromHeaders,
    build_ssec_read_headers, contains_managed_encryption_metadata, mark_encrypted_multipart_metadata, stored_ssec_metadata,
    strip_managed_encryption_metadata,
};
pub(crate) use rustfs_sse::{KmsKeyAuthorizer, LocalSseDekProvider, SseDekProvider};
#[cfg(test)]
pub(crate) use rustfs_sse::{TestAuthorizationHooks, TestSseDekProvider, reset_sse_dek_provider, set_sse_dek_provider_for_test};

// ============================================================================
// The server `rustfs-sse` runs in
// ============================================================================

/// This server, as `rustfs-sse` sees it.
///
/// Stateless: every capability is resolved from the running server on each
/// call, so a KMS reconfiguration or a bucket-encryption change is observed by
/// the next operation.
pub(crate) struct RustfsSseHost;

static HOST: RustfsSseHost = RustfsSseHost;

#[async_trait]
impl SseHost for RustfsSseHost {
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        runtime_sources::current_encryption_service().await
    }

    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        current_kms_runtime_service_manager()
    }

    async fn bucket_encryption(&self, bucket: &str) -> Result<Option<BucketEncryption>, SseError> {
        match get_bucket_sse_config(bucket).await {
            Ok((config, _updated_at)) => Ok(Some(BucketEncryption {
                default_rule: config
                    .rules
                    .first()
                    .and_then(|rule| rule.apply_server_side_encryption_by_default.as_ref())
                    .map(|sse| BucketDefaultEncryption {
                        sse_algorithm: sse.sse_algorithm.as_str().to_string(),
                        kms_master_key_id: sse.kms_master_key_id.clone(),
                    }),
            })),
            Err(Error::ConfigNotFound) => Ok(None),
            Err(err) => Err(sse_error(ApiError::from(err))),
        }
    }

    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        &IamKmsKeyAuthorizer
    }
}

/// Hand a storage failure this server already classified to `rustfs-sse` unchanged.
pub(crate) fn sse_error(error: ApiError) -> SseError {
    SseError {
        code: error.code,
        message: error.message,
        source: error.source,
    }
}

struct IamKmsKeyAuthorizer;

#[async_trait]
impl KmsKeyAuthorizer for IamKmsKeyAuthorizer {
    async fn is_allowed(&self, principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, SseError> {
        let iam_store = runtime_sources::current_ready_iam_handle()
            .map_err(|err| SseError::internal(format!("KMS key authorization requires IAM: {err:?}")))?;

        Ok(iam_store
            .is_allowed(&Args {
                account: principal.account(),
                groups: principal.groups(),
                action: Action::KmsAction(action),
                // Call-site contract of `Statement::kms_key_scope_matches`: the requested key
                // identifier travels in `object` with `bucket` left empty.
                bucket: "",
                object: key_id,
                conditions: principal.conditions(),
                is_owner: principal.is_owner(),
                claims: principal.claims(),
                deny_only: false,
            })
            .await)
    }
}

/// Instance-local encryption services retained by maintenance operations.
pub(crate) struct ManagedSseRuntime {
    service_manager: Option<Arc<rustfs_kms::KmsServiceManager>>,
    local_provider: OnceLock<Arc<dyn SseDekProvider>>,
}

pub(crate) struct ManagedSseOperation {
    runtime: Arc<ManagedSseRuntime>,
    service: Option<Arc<rustfs_kms::service::ObjectEncryptionService>>,
    configured_but_unavailable: bool,
    operation_context: rustfs_kms::types::OperationContext,
}

impl ManagedSseRuntime {
    pub(crate) fn new(service_manager: Option<Arc<rustfs_kms::KmsServiceManager>>) -> Self {
        Self {
            service_manager,
            local_provider: OnceLock::new(),
        }
    }

    pub(crate) async fn begin_operation(self: &Arc<Self>, context: rustfs_kms::types::OperationContext) -> ManagedSseOperation {
        let service = match &self.service_manager {
            Some(manager) => manager.get_encryption_service().await,
            None => None,
        };
        let configured_but_unavailable = if service.is_none() {
            match &self.service_manager {
                Some(manager) => !matches!(manager.get_status().await, rustfs_kms::KmsServiceStatus::NotConfigured),
                None => false,
            }
        } else {
            false
        };
        ManagedSseOperation {
            runtime: self.clone(),
            service,
            configured_but_unavailable,
            operation_context: context,
        }
    }
}

struct ManagedSseHost {
    operation: Arc<ManagedSseOperation>,
    authorizer: Arc<dyn KmsKeyAuthorizer>,
}

#[async_trait]
impl SseHost for ManagedSseHost {
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        self.operation.service.clone()
    }

    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        self.operation.runtime.service_manager.clone()
    }

    async fn kms_configured_but_unavailable(&self) -> bool {
        self.operation.configured_but_unavailable
    }

    fn local_sse_dek_provider(&self) -> Result<Arc<dyn SseDekProvider>, SseError> {
        let cache = &self.operation.runtime.local_provider;
        if let Some(provider) = cache.get() {
            return Ok(provider.clone());
        }
        let provider: Arc<dyn SseDekProvider> = Arc::new(LocalSseDekProvider::new_from_env()?);
        let _ = cache.set(provider.clone());
        Ok(cache.get().cloned().unwrap_or(provider))
    }

    async fn bucket_encryption(&self, bucket: &str) -> Result<Option<BucketEncryption>, SseError> {
        HOST.bucket_encryption(bucket).await
    }

    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        self.authorizer.as_ref()
    }
}

// ============================================================================
// `rustfs-sse` entry points, bound to this server
// ============================================================================

/// Building an [`SseKmsPrincipal`] from the request that crossed the S3 boundary.
pub(crate) trait SseKmsPrincipalExt: Sized {
    /// Build a principal from an S3 request.
    ///
    /// Anonymous requests use an empty account, matching the bucket-policy authorization
    /// path. This keeps every request that crossed the S3 boundary distinct from trusted
    /// internal callers, which are represented by `None`.
    fn from_request<R: AuthorizationRequest + ?Sized>(req: &R) -> Option<Self>;

    fn for_managed_maintenance(
        credential: &rustfs_credentials::Credentials,
        conditions: HashMap<String, Vec<String>>,
        request_id: &str,
        authorizer: Arc<dyn KmsKeyAuthorizer>,
        operation: Arc<ManagedSseOperation>,
    ) -> Self;

    fn kms_policy_args<'a>(&'a self, action: KmsAction, key_id: &'a str) -> Args<'a>;
    fn matches_managed_operation(&self, account: &str, operation_id: uuid::Uuid) -> bool;
    fn is_managed_maintenance(&self) -> bool;
}

impl SseKmsPrincipalExt for SseKmsPrincipal {
    fn for_managed_maintenance(
        credential: &rustfs_credentials::Credentials,
        conditions: HashMap<String, Vec<String>>,
        request_id: &str,
        authorizer: Arc<dyn KmsKeyAuthorizer>,
        operation: Arc<ManagedSseOperation>,
    ) -> Self {
        SseKmsPrincipal::new(
            credential.access_key.clone(),
            credential.groups.clone(),
            false,
            credential.claims_or_empty().clone(),
            conditions,
        )
        .with_managed_operation(
            request_id,
            operation.operation_context.clone(),
            Arc::new(ManagedSseHost { operation, authorizer }),
        )
    }

    fn kms_policy_args<'a>(&'a self, action: KmsAction, key_id: &'a str) -> Args<'a> {
        Args {
            account: self.account(),
            groups: self.groups(),
            action: Action::KmsAction(action),
            bucket: "",
            object: key_id,
            conditions: self.conditions(),
            is_owner: false,
            claims: self.claims(),
            deny_only: false,
        }
    }

    fn matches_managed_operation(&self, account: &str, operation_id: uuid::Uuid) -> bool {
        self.account() == account
            && self.managed_operation_context().is_some_and(|(request_id, context)| {
                context.principal == account && context.operation_id == operation_id && request_id == operation_id.to_string()
            })
    }

    fn is_managed_maintenance(&self) -> bool {
        self.managed_operation_context().is_some()
    }

    fn from_request<R: AuthorizationRequest + ?Sized>(req: &R) -> Option<Self> {
        let req_info = req.req_info()?;
        let cred = req_info.cred.as_ref();

        let principal = SseKmsPrincipal::new(
            cred.map(|cred| cred.access_key.clone()).unwrap_or_default(),
            cred.and_then(|cred| cred.groups.clone()),
            req_info.is_owner,
            cred.map(|cred| cred.claims_or_empty().clone()).unwrap_or_default(),
            cred.map(|cred| resource_free_condition_values(req, cred)).unwrap_or_default(),
        );
        let principal = match request_context_from_req(req) {
            Some(context) => principal.with_request_id(&context.request_id),
            None => principal,
        };
        #[cfg(test)]
        let principal = principal.with_test_hooks(req.test_extensions().get::<TestAuthorizationHooks>().cloned());
        Some(principal)
    }
}

/// Managed SSE resolved from a bucket default encryption rule on a write path.
///
/// See [`rustfs_sse::bucket_default_write_sse`] for the mapping and why an
/// unknown algorithm resolves to AES256 rather than to nothing.
pub(crate) fn bucket_default_write_sse(sse: &rustfs_gateway_types::persistence::PersistedEncryptionByDefault) -> String {
    rustfs_sse::bucket_default_write_sse(sse.sse_algorithm.as_str())
}

pub(crate) fn extract_ssec_params_from_headers(headers: &HeaderMap) -> Result<SsecParamsFromHeaders, ApiError> {
    Ok(rustfs_sse::extract_ssec_params_from_headers(headers)?)
}

pub(crate) fn extract_server_side_encryption_from_headers(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    Ok(rustfs_sse::extract_server_side_encryption_from_headers(headers)?)
}

pub(crate) fn extract_ssekms_context_from_headers(headers: &HeaderMap) -> Result<Option<HashMap<String, String>>, ApiError> {
    Ok(rustfs_sse::extract_ssekms_context_from_headers(headers)?)
}

pub(crate) fn validate_sse_headers_for_write(
    server_side_encryption: Option<&str>,
    ssekms_key_id: Option<&String>,
    ssekms_context: Option<&HashMap<String, String>>,
    sse_customer_algorithm: Option<&String>,
    sse_customer_key: Option<&String>,
    sse_customer_key_md5: Option<&String>,
    require_sse_customer_key: bool,
) -> Result<(), ApiError> {
    Ok(rustfs_sse::validate_sse_headers_for_write(
        server_side_encryption,
        ssekms_key_id,
        ssekms_context,
        sse_customer_algorithm,
        sse_customer_key,
        sse_customer_key_md5,
        require_sse_customer_key,
    )?)
}

pub(crate) fn validate_sse_headers_for_read(metadata: &HashMap<String, String>, headers: &HeaderMap) -> Result<(), ApiError> {
    Ok(rustfs_sse::validate_sse_headers_for_read(metadata, headers)?)
}

pub(crate) fn validate_ssec_for_read(
    metadata: &HashMap<String, String>,
    sse_customer_key: Option<&String>,
    sse_customer_key_md5: Option<&String>,
) -> Result<(), ApiError> {
    Ok(rustfs_sse::validate_ssec_for_read(metadata, sse_customer_key, sse_customer_key_md5)?)
}

pub fn encryption_material_to_metadata(material: &EncryptionMaterial) -> Result<HashMap<String, String>, ApiError> {
    Ok(rustfs_sse::encryption_material_to_metadata(material)?)
}

pub(crate) fn project_sse_read_response_headers(
    metadata: &HashMap<String, String>,
    sse_customer_algorithm: Option<&String>,
    sse_customer_key_md5: Option<&String>,
) -> Result<HeaderMap, ApiError> {
    Ok(rustfs_sse::project_sse_read_response_headers(
        metadata,
        sse_customer_algorithm,
        sse_customer_key_md5,
    )?)
}

pub async fn authorize_sse_kms_object_read(
    principal: Option<&SseKmsPrincipal>,
    metadata: &HashMap<String, String>,
) -> Result<(), ApiError> {
    Ok(rustfs_sse::authorize_sse_kms_object_read(&HOST, principal, metadata).await?)
}

pub async fn authorize_sse_kms_multipart_write(
    principal: Option<&SseKmsPrincipal>,
    metadata: &HashMap<String, String>,
) -> Result<(), ApiError> {
    Ok(rustfs_sse::authorize_sse_kms_multipart_write(&HOST, principal, metadata).await?)
}

pub async fn sse_encryption(request: EncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    Ok(rustfs_sse::sse_encryption(&HOST, request).await?)
}

pub async fn sse_prepare_encryption(request: PrepareEncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    Ok(rustfs_sse::sse_prepare_encryption(&HOST, request).await?)
}

pub async fn sse_decryption(request: DecryptionRequest<'_>) -> Result<Option<DecryptionMaterial>, ApiError> {
    Ok(rustfs_sse::sse_decryption(&HOST, request).await?)
}

pub async fn classify_sse_read_response(request: DecryptionRequest<'_>) -> Result<Option<SseReadResponseHeaders>, ApiError> {
    Ok(rustfs_sse::classify_sse_read_response(&HOST, request).await?)
}

pub(crate) async fn rewrap_object_encryption_metadata(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
) -> Result<ObjectDekRewrapOutcome, ApiError> {
    Ok(rustfs_sse::rewrap_object_encryption_metadata(&HOST, bucket, key, metadata).await?)
}

// ============================================================================
// Storage-engine boundary: write plan
// ============================================================================

/// Turning resolved encryption material into the write plan the object layer applies.
pub(crate) trait EncryptionMaterialExt {
    fn write_encryption(&self, multipart_part_number: Option<usize>) -> WriteEncryption;
}

impl EncryptionMaterialExt for EncryptionMaterial {
    fn write_encryption(&self, multipart_part_number: Option<usize>) -> WriteEncryption {
        match (self.key_kind, multipart_part_number) {
            (EncryptionKeyKind::Object, Some(part_number)) => {
                WriteEncryption::multipart_object_key(self.key_bytes, part_number as u32)
            }
            (EncryptionKeyKind::Object, None) => WriteEncryption::singlepart_object_key(self.key_bytes),
            (EncryptionKeyKind::Direct, Some(part_number)) => {
                WriteEncryption::multipart(self.key_bytes, self.base_nonce, part_number)
            }
            (EncryptionKeyKind::Direct, None) => WriteEncryption::singlepart(self.key_bytes, self.base_nonce),
        }
    }
}

// ============================================================================
// Storage-engine boundary: read errors
// ============================================================================

pub(crate) fn map_get_object_reader_error(err: StorageError) -> ApiError {
    // GET/HEAD object-read contract: these failures mean "this object is not
    // readable right now" (S3 `SlowDownRead`), not a write admission reject.
    if crate::error::is_object_read_unavailable(&err) {
        return crate::error::slow_down_read_api_error(err);
    }

    if let StorageError::Io(io_error) = &err
        && let Some(resolution_error) = io_error
            .get_ref()
            .and_then(|source| source.downcast_ref::<EncryptionResolutionError>())
    {
        let code = match resolution_error.kind() {
            EncryptionResolutionErrorKind::InvalidRequest => S3ErrorCode::InvalidRequest,
            EncryptionResolutionErrorKind::ServiceUnavailable => S3ErrorCode::ServiceUnavailable,
            // Same code the write path returns for this key; `From<ApiError>`
            // attaches the 400 that s3s cannot derive for a custom code.
            EncryptionResolutionErrorKind::KeyNotFound => S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()),
            EncryptionResolutionErrorKind::AccessDenied => S3ErrorCode::AccessDenied,
            EncryptionResolutionErrorKind::NotImplemented => S3ErrorCode::NotImplemented,
            // A permanent property of the stored object, not a transient server
            // fault: 5xx would invite client retry storms against an object
            // this server can never decrypt.
            EncryptionResolutionErrorKind::InvalidMetadata => S3ErrorCode::InvalidObjectState,
            _ => S3ErrorCode::InternalError,
        };
        return ApiError {
            code,
            message: resolution_error.to_string(),
            source: Some(Box::new(err)),
        };
    }

    if let Some(message) = map_ssec_get_object_reader_error_message(&err) {
        return ApiError {
            code: S3ErrorCode::InvalidRequest,
            message,
            source: Some(Box::new(err)),
        };
    }

    ApiError::from(err)
}

fn map_ssec_get_object_reader_error_message(err: &StorageError) -> Option<String> {
    let StorageError::Io(io_err) = err else {
        return None;
    };

    let detail = io_err.to_string();
    match detail.as_str() {
        "missing SSE-C algorithm header"
        | "invalid SSE-C algorithm header"
        | "missing SSE-C key header"
        | "invalid SSE-C key header"
        | "missing SSE-C key md5 header"
        | "invalid SSE-C key md5 header" => Some(
            "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
                .to_string(),
        ),
        "failed to decode SSE-C key" => Some("Invalid SSE-C key: not valid Base64.".to_string()),
        "SSE-C key must be 32 bytes" => Some("SSE-C key must be exactly 32 bytes.".to_string()),
        "SSE-C key MD5 mismatch" => {
            Some("The calculated MD5 hash of the key did not match the hash that was provided.".to_string())
        }
        "missing stored SSE-C key md5" => Some("Object has no stored SSE-C key metadata.".to_string()),
        "SSE-C key does not match object metadata" => Some(
            "The provided encryption parameters did not match the ones used originally to encrypt the object.".to_string(),
        ),
        _ => detail
            .strip_prefix("unsupported SSE-C algorithm ")
            .map(|_| format!("Unsupported SSE-C algorithm. Only {DEFAULT_SSE_ALGORITHM} is supported.")),
    }
}

/// Carry the S3-level classification of a decryption failure through the
/// ecstore boundary. Every code produced by `rustfs_sse::data_plane_kms_error`
/// needs a kind here, otherwise the read path reports it as an internal fault
/// even though the write path already reports the same KMS error to the client.
fn map_encryption_resolution_error(error: impl Into<ApiError>) -> EncryptionResolutionError {
    let error = error.into();
    let kind = match &error.code {
        S3ErrorCode::InvalidArgument | S3ErrorCode::InvalidRequest => EncryptionResolutionErrorKind::InvalidRequest,
        S3ErrorCode::ServiceUnavailable => EncryptionResolutionErrorKind::ServiceUnavailable,
        S3ErrorCode::Custom(code) if &**code == KMS_KEY_NOT_FOUND_ERROR_CODE => EncryptionResolutionErrorKind::KeyNotFound,
        S3ErrorCode::AccessDenied => EncryptionResolutionErrorKind::AccessDenied,
        S3ErrorCode::NotImplemented => EncryptionResolutionErrorKind::NotImplemented,
        _ => EncryptionResolutionErrorKind::DecryptionFailed,
    };
    EncryptionResolutionError::new(kind, error.message)
}

/// Canonical-case encryption metadata, or the resolution error for conflicting spellings.
fn normalize_metadata_for_resolution(
    metadata: &HashMap<String, String>,
) -> Result<Cow<'_, HashMap<String, String>>, EncryptionResolutionError> {
    normalize_encryption_metadata_case(metadata)
        .map_err(|conflict| EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidMetadata, conflict.to_string()))
}

// ============================================================================
// Storage-engine boundary: read-side resolver
// ============================================================================

pub(crate) struct SseObjectEncryptionResolver;

/// Checks metadata and key access before a conditional read returns without a body.
pub(crate) async fn validate_conditional_read(
    bucket: &str,
    object: &str,
    metadata: &HashMap<String, String>,
    headers: &HeaderMap,
    principal: Option<&SseKmsPrincipal>,
) -> Result<(), ApiError> {
    let metadata = normalize_metadata_for_resolution(metadata)
        .map_err(|error| map_get_object_reader_error(StorageError::Io(std::io::Error::other(error))))?;
    validate_sse_headers_for_read(&metadata, headers)?;
    let (_, customer_key, customer_key_md5) = extract_ssec_params_from_headers(headers)?;
    classify_sse_read_response(DecryptionRequest {
        bucket,
        key: object,
        metadata: &metadata,
        sse_customer_key: customer_key.as_ref(),
        sse_customer_key_md5: customer_key_md5.as_ref(),
        principal,
    })
    .await?;
    SseObjectEncryptionResolver
        .resolve_read_material(ReadEncryptionRequest {
            bucket,
            object,
            metadata: &metadata,
            headers,
        })
        .await
        .map_err(|error| map_get_object_reader_error(StorageError::Io(std::io::Error::other(error))))?;
    Ok(())
}

#[async_trait]
impl ObjectEncryptionResolver for SseObjectEncryptionResolver {
    async fn resolve_read_material(
        &self,
        request: ReadEncryptionRequest<'_>,
    ) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
        // Internal readers have no request identity; S3 callers authorize before
        // entering this resolver. Maintenance uses its operation-bound resolver.
        resolve_sse_read_material(request, None).await
    }
}

struct ManagedReadEncryptionResolver {
    bucket: String,
    key: String,
    principal: SseKmsPrincipal,
}

pub(crate) fn managed_read_encryption_resolver(
    bucket: String,
    key: String,
    principal: SseKmsPrincipal,
) -> Arc<dyn ObjectEncryptionResolver> {
    Arc::new(ManagedReadEncryptionResolver { bucket, key, principal })
}

#[async_trait]
impl ObjectEncryptionResolver for ManagedReadEncryptionResolver {
    async fn resolve_read_material(
        &self,
        request: ReadEncryptionRequest<'_>,
    ) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
        if request.bucket != self.bucket || request.object != self.key {
            return Err(EncryptionResolutionError::new(
                EncryptionResolutionErrorKind::InvalidMetadata,
                "read resolver resource mismatch",
            ));
        }
        resolve_sse_read_material(request, Some(&self.principal)).await
    }
}

async fn resolve_sse_read_material(
    request: ReadEncryptionRequest<'_>,
    principal: Option<&SseKmsPrincipal>,
) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
    let metadata = normalize_metadata_for_resolution(request.metadata)?;
    let (customer_algorithm, customer_key, customer_key_md5) =
        extract_ssec_params_from_headers(request.headers).map_err(map_encryption_resolution_error)?;
    if let Some(stored_algorithm) = metadata.get("x-amz-server-side-encryption-customer-algorithm") {
        let request_algorithm = customer_algorithm.as_ref().ok_or_else(|| {
            map_encryption_resolution_error(ssec_invalid_request(
                "The object was stored using a form of Server Side Encryption. \
                 The correct parameters must be provided to retrieve the object.",
            ))
        })?;
        if stored_algorithm != request_algorithm.as_str() {
            return Err(map_encryption_resolution_error(ssec_invalid_request(
                "The provided encryption parameters did not match the ones used originally to encrypt the object.",
            )));
        }
    }
    let material = sse_decryption(DecryptionRequest {
        bucket: request.bucket,
        key: request.object,
        metadata: &metadata,
        sse_customer_key: customer_key.as_ref(),
        sse_customer_key_md5: customer_key_md5.as_ref(),
        principal,
    })
    .await
    .map_err(map_encryption_resolution_error)?;
    let Some(material) = material else {
        // Fail closed with a diagnosis instead of a bare `None`: the read
        // plan classifies the object as encrypted from these same markers
        // and would refuse to serve it anyway, but its generic error names
        // neither the object's format nor what the operator can do about it.
        if metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
        {
            #[cfg(not(feature = "rio-v2"))]
            let message = "object is stored encrypted, but its decryption material could not be resolved: the \
                           encryption metadata is incomplete, or it is in the MinIO-compatible sealed format, \
                           which this server build has no read path for (reading it requires a build with the \
                           `rio-v2` feature)";
            #[cfg(feature = "rio-v2")]
            let message = "object is stored encrypted, but its decryption material could not be resolved: the \
                           encryption metadata is incomplete or in an unrecognized sealed format";
            return Err(EncryptionResolutionError::new(EncryptionResolutionErrorKind::InvalidMetadata, message));
        }
        return Ok(None);
    };

    Ok(Some(ReadEncryptionMaterial {
        key_bytes: material.key_bytes,
        mode: match material.key_kind {
            EncryptionKeyKind::Direct => ReadEncryptionMode::Direct {
                base_nonce: material.base_nonce,
            },
            EncryptionKeyKind::Object => ReadEncryptionMode::Object,
        },
    }))
}

// ============================================================================
// Storage-engine boundary: multipart completion gate
// ============================================================================

/// Capture the request's encryption gate without exposing customer keys in
/// ObjectOptions Debug output. Storage applies it to the source selected under
/// completion locks, including recovered uploads and historical replay versions.
pub(crate) fn multipart_completion_authorization(
    request: EncryptionRequest<'_>,
    replication_authorized: bool,
) -> Arc<dyn super::storage_api::contract::multipart::MultipartCompletionAuthorization<Error = StorageError>> {
    Arc::new(SseMultipartCompletionAuthorization {
        bucket: request.bucket.to_string(),
        key: request.key.to_string(),
        principal: request.principal.cloned(),
        algorithm: request.sse_customer_algorithm,
        customer_key: request.sse_customer_key,
        customer_md5: request.sse_customer_key_md5,
        replication_authorized,
    })
}

struct SseMultipartCompletionAuthorization {
    bucket: String,
    key: String,
    principal: Option<SseKmsPrincipal>,
    algorithm: Option<String>,
    customer_key: Option<String>,
    customer_md5: Option<String>,
    replication_authorized: bool,
}

#[async_trait::async_trait]
impl super::storage_api::contract::multipart::MultipartCompletionAuthorization for SseMultipartCompletionAuthorization {
    type Error = StorageError;

    async fn authorize(&self, metadata: &HashMap<String, String>) -> Result<(), StorageError> {
        authorize_sse_kms_multipart_write(self.principal.as_ref(), metadata)
            .await
            .map_err(|error| StorageError::other(map_encryption_resolution_error(error)))?;
        if self.customer_key.is_some()
            || !self.replication_authorized
            || !rustfs_utils::http::contains_key_str(metadata, rustfs_utils::http::SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT)
        {
            EncryptionRequest {
                bucket: &self.bucket,
                key: &self.key,
                server_side_encryption: None,
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm: self.algorithm.clone(),
                sse_customer_key: self.customer_key.clone(),
                sse_customer_key_md5: self.customer_md5.clone(),
                content_size: 0,
                principal: self.principal.as_ref(),
            }
            .validate_complete_multipart_ssec(metadata)
            .map_err(|error| StorageError::other(map_encryption_resolution_error(error)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) async fn kms_sse_dek_provider_for_test(
    manager: Arc<rustfs_kms::KmsServiceManager>,
) -> Result<Arc<dyn SseDekProvider>, ApiError> {
    Ok(rustfs_sse::testing::kms_sse_dek_provider_for_test(manager).await?)
}

#[cfg(test)]
mod tests {
    use super::{
        ApiError, Arc, EncryptionRequest, EncryptionResolutionError, EncryptionResolutionErrorKind, HashMap, HeaderMap,
        KMS_KEY_NOT_FOUND_ERROR_CODE, KmsRequestAuditScope, ObjectEncryptionResolver, ReadEncryptionMode, ReadEncryptionRequest,
        RecordingKmsKeyAuthorizer, S3ErrorCode, SseError, SseKmsPrincipal, SseKmsPrincipalExt, SseObjectEncryptionResolver,
        StorageError, TestAuthorizationHooks, map_encryption_resolution_error, map_get_object_reader_error,
        multipart_completion_authorization, sse_kms_object_metadata, validate_conditional_read,
    };
    use base64_simd::STANDARD as BASE64_STANDARD;
    use http::HeaderValue;
    use md5::{Digest, Md5};
    use rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER;

    fn md5_base64(input: impl AsRef<[u8]>) -> String {
        let mut hasher = Md5::new();
        hasher.update(input.as_ref());
        BASE64_STANDARD.encode_to_string(hasher.finalize())
    }

    fn principal_with_authorizer(enforced: bool, authorizer: Arc<RecordingKmsKeyAuthorizer>) -> SseKmsPrincipal {
        SseKmsPrincipal::new("analyst".to_string(), None, false, HashMap::new(), HashMap::new())
            .with_test_hooks(Some(TestAuthorizationHooks { enforced, authorizer }))
    }

    fn enforcing_principal(allowed: bool) -> (SseKmsPrincipal, Arc<RecordingKmsKeyAuthorizer>) {
        let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
        (principal_with_authorizer(true, authorizer.clone()), authorizer)
    }

    fn audit_tag(tags: &[(&'static str, serde_json::Value)], key: &str) -> Option<String> {
        tags.iter()
            .find(|(candidate, _)| *candidate == key)
            .and_then(|(_, value)| value.as_str().map(str::to_string))
    }

    fn ssec_read_headers(key_b64: &str, key_md5: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
        headers.insert(
            "x-amz-server-side-encryption-customer-key",
            HeaderValue::from_str(key_b64).expect("base64 key is a valid header"),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            HeaderValue::from_str(key_md5).expect("base64 MD5 is a valid header"),
        );
        headers
    }

    fn multipart_ssec_request(key_byte: u8) -> EncryptionRequest<'static> {
        let key_bytes = [key_byte; 32];
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(BASE64_STANDARD.encode_to_string(key_bytes)),
            sse_customer_key_md5: Some(md5_base64(key_bytes)),
            content_size: 1,
            principal: None,
        }
    }

    fn multipart_ssec_metadata(key_byte: u8) -> HashMap<String, String> {
        let key_bytes = [key_byte; 32];
        HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), md5_base64(key_bytes)),
        ])
    }

    /// The messages `rustfs-sse` attaches to the codes it classifies must be the
    /// ones this server answers those codes with everywhere else.
    #[test]
    fn sse_errors_carry_this_servers_messages() {
        let internal = ApiError::from(SseError::internal("plain failure"));
        assert_eq!(internal.code, S3ErrorCode::InternalError);
        assert_eq!(internal.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));

        let unavailable = ApiError::from(SseError::internal(rustfs_kms::KmsUnavailableError));
        assert_eq!(unavailable.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(unavailable.message, ApiError::error_code_to_message(&S3ErrorCode::ServiceUnavailable));

        let denied = ApiError::from(SseError::internal(rustfs_kms::KmsError::access_denied("policy")));
        assert_eq!(denied.code, S3ErrorCode::AccessDenied);
        assert_eq!(denied.message, ApiError::error_code_to_message(&S3ErrorCode::AccessDenied));
    }

    #[test]
    fn anonymous_s3_request_builds_kms_principal() {
        let mut extensions = http::Extensions::new();
        extensions.insert(crate::storage::req_info::ReqInfo::default());
        let envelope = crate::app::RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: http::Method::GET,
                uri: http::Uri::from_static("/bucket/object"),
                headers: http::HeaderMap::new(),
                credentials: None,
                region: None,
                service: None,
            },
            extensions,
        );
        let principal = SseKmsPrincipal::from_request(&envelope).expect("S3-boundary request should build a principal");

        assert!(principal.account().is_empty());
        assert!(principal.groups().is_none());
        assert!(principal.claims().is_empty());
    }

    /// The read path squeezes the S3 classification through ecstore's
    /// resolution-error kinds; every KMS class the write path reports to the
    /// client must survive that hop instead of collapsing onto `DecryptionFailed`
    /// (which the S3 layer reports as `500`).
    #[test]
    fn encryption_resolution_kinds_preserve_kms_read_classification() {
        let cases = [
            (
                rustfs_kms::KmsError::key_not_found("no-such-key"),
                EncryptionResolutionErrorKind::KeyNotFound,
            ),
            (rustfs_kms::KmsError::access_denied("policy"), EncryptionResolutionErrorKind::AccessDenied),
            (
                rustfs_kms::KmsError::unsupported_capability("local", "decrypt_legacy"),
                EncryptionResolutionErrorKind::NotImplemented,
            ),
            (
                rustfs_kms::KmsError::backend_error("connection refused"),
                EncryptionResolutionErrorKind::ServiceUnavailable,
            ),
            (
                rustfs_kms::KmsError::invalid_operation("key is disabled"),
                EncryptionResolutionErrorKind::InvalidRequest,
            ),
            (
                rustfs_kms::KmsError::cryptographic_error("decrypt", "authentication failed"),
                EncryptionResolutionErrorKind::DecryptionFailed,
            ),
        ];
        for (error, expected) in cases {
            let description = error.to_string();
            let resolution = map_encryption_resolution_error(SseError::internal(error));
            assert_eq!(resolution.kind(), expected, "{description}");
        }
    }

    #[tokio::test]
    async fn object_encryption_resolver_returns_ssec_read_material() {
        let key = [0x31; 32];
        let key_b64 = BASE64_STANDARD.encode_to_string(key);
        let key_md5 = md5_base64(key);
        let nonce = [0x42; 12];
        let metadata = HashMap::from([
            ("X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(), "AES256".to_string()),
            ("X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(), key_md5.clone()),
            ("X-Rustfs-Encryption-Iv".to_string(), BASE64_STANDARD.encode_to_string(nonce)),
        ]);
        let headers = ssec_read_headers(&key_b64, &key_md5);

        let material = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &headers,
            })
            .await
            .expect("SSE-C material should resolve")
            .expect("SSE-C metadata should produce material");

        assert_eq!(material.key_bytes, key);
        assert_eq!(material.mode, ReadEncryptionMode::Direct { base_nonce: nonce });
    }

    #[tokio::test]
    async fn object_encryption_resolver_rejects_missing_or_invalid_ssec_algorithm() {
        let key = [0x31; 32];
        let key_b64 = BASE64_STANDARD.encode_to_string(key);
        let key_md5 = md5_base64(key);
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.clone()),
        ]);

        for algorithm in [None, Some("AES128")] {
            let mut headers = ssec_read_headers(&key_b64, &key_md5);
            match algorithm {
                Some(algorithm) => {
                    headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static(algorithm));
                }
                None => {
                    headers.remove("x-amz-server-side-encryption-customer-algorithm");
                }
            }

            let result = SseObjectEncryptionResolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: "bucket",
                    object: "object",
                    metadata: &metadata,
                    headers: &headers,
                })
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("missing or invalid SSE-C algorithm must fail closed"),
            };

            assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn object_encryption_resolver_classifies_missing_ssec_key_as_invalid_request() {
        let metadata = HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string())]);
        let result = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("missing SSE-C key must fail closed"),
        };

        assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidRequest);
    }

    #[tokio::test]
    async fn object_encryption_resolver_rejects_conflicting_metadata_case_variants() {
        let metadata = HashMap::from([
            ("x-rustfs-encryption-key".to_string(), "first".to_string()),
            ("X-Rustfs-Encryption-Key".to_string(), "second".to_string()),
        ]);
        let result = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("conflicting metadata aliases must fail closed"),
        };

        assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidMetadata);
    }

    #[tokio::test]
    async fn object_encryption_resolver_diagnoses_unresolvable_encrypted_metadata() {
        // A MinIO-written SSE-C object carries only internal sealed-key slots:
        // no public scheme header, no customer-algorithm metadata. The read
        // must fail closed either way, but the failure differs by build: a
        // default build has no read path for the shape and diagnoses that; a
        // rio-v2 build recognizes it as SSE-C and asks for the customer key.
        let metadata = HashMap::from([(
            MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
            "c2VhbGVkLWtleQ==".to_string(),
        )]);
        let result = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("encryption markers without resolvable material must fail closed"),
        };

        #[cfg(not(feature = "rio-v2"))]
        {
            assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidMetadata);
            assert!(
                error.to_string().contains("could not be resolved"),
                "error must diagnose the unresolvable metadata, got: {error}"
            );
        }
        #[cfg(feature = "rio-v2")]
        {
            assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidRequest);
            assert!(
                error.to_string().contains("must be provided"),
                "error must ask for the SSE-C parameters, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn object_encryption_resolver_keeps_plaintext_objects_unresolved() {
        let metadata = HashMap::from([("content-type".to_string(), "text/plain".to_string())]);
        let material = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await
            .expect("plaintext metadata resolves without error");
        assert!(material.is_none(), "plaintext objects carry no read material");
    }

    #[test]
    fn map_get_object_reader_error_maps_unresolvable_metadata_to_invalid_object_state() {
        let error = StorageError::Io(std::io::Error::other(EncryptionResolutionError::new(
            EncryptionResolutionErrorKind::InvalidMetadata,
            "object is marked encrypted, but no decryption material could be resolved from its metadata",
        )));
        let api_error = map_get_object_reader_error(error);
        assert_eq!(api_error.code, S3ErrorCode::InvalidObjectState);
        assert!(api_error.message.contains("no decryption material could be resolved"));
    }

    #[test]
    fn test_map_get_object_reader_error_converts_missing_ssec_headers_to_invalid_request() {
        let err = map_get_object_reader_error(StorageError::other("missing SSE-C algorithm header"));
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message,
            "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
        );
    }

    #[test]
    fn test_map_get_object_reader_error_converts_ssec_md5_mismatch_to_invalid_request() {
        let err = map_get_object_reader_error(StorageError::other("SSE-C key MD5 mismatch"));
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message,
            "The calculated MD5 hash of the key did not match the hash that was provided."
        );
    }

    #[test]
    fn test_map_get_object_reader_error_preserves_typed_service_unavailable() {
        let resolution_error =
            EncryptionResolutionError::new(EncryptionResolutionErrorKind::ServiceUnavailable, "KMS unavailable");
        let err = map_get_object_reader_error(StorageError::other(resolution_error));
        assert_eq!(err.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(err.message, "KMS unavailable");
    }

    /// The reader wraps the resolution error in an io error exactly like
    /// `readers.rs` does; a missing key must come out as the same `400
    /// KMS.NotFoundException` the write path returns, not `500`.
    #[test]
    fn test_map_get_object_reader_error_reports_missing_kms_key_as_client_error() {
        let resolution_error =
            EncryptionResolutionError::new(EncryptionResolutionErrorKind::KeyNotFound, "KMS key not found: finance-key");
        let err = map_get_object_reader_error(StorageError::other(resolution_error));
        assert_eq!(err.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
        assert_eq!(err.message, "KMS key not found: finance-key");
        let s3_error = rustfs_s3_types::S3Error::from(err);
        assert_eq!(s3_error.status_code(), Some(http::StatusCode::BAD_REQUEST));

        let denied = map_get_object_reader_error(StorageError::other(EncryptionResolutionError::new(
            EncryptionResolutionErrorKind::AccessDenied,
            "Access Denied",
        )));
        assert_eq!(denied.code, S3ErrorCode::AccessDenied);

        let unsupported = map_get_object_reader_error(StorageError::other(EncryptionResolutionError::new(
            EncryptionResolutionErrorKind::NotImplemented,
            "backend cannot unwrap legacy envelopes",
        )));
        assert_eq!(unsupported.code, S3ErrorCode::NotImplemented);
    }

    #[test]
    fn test_map_get_object_reader_error_maps_part_missing_to_slow_down_read() {
        let err = map_get_object_reader_error(StorageError::PartMissingOrCorrupt);

        assert_eq!(err.code, S3ErrorCode::Custom("SlowDownRead".into()));
        assert_eq!(err.message, "The object could not be read from storage.");
    }

    #[test]
    fn test_map_get_object_reader_error_redacts_non_ssec_internal_errors() {
        let err = map_get_object_reader_error(StorageError::other("plain io failure"));
        assert_eq!(err.code, S3ErrorCode::InternalError);
        assert_eq!(err.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        let source = err
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::Io(io_error) if io_error.to_string().contains("plain io failure")));
    }

    #[tokio::test]
    async fn completion_authorization_preserves_typed_denials_and_redacts_customer_keys() {
        let key = multipart_ssec_request(42).sse_customer_key.expect("test key");
        let mut metadata = multipart_ssec_metadata(42);
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
        let no_key = multipart_completion_authorization(
            EncryptionRequest {
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                ..multipart_ssec_request(42)
            },
            false,
        );
        let error = no_key.authorize(&metadata).await.expect_err("customer key required");
        assert_eq!(map_get_object_reader_error(error).code, S3ErrorCode::InvalidRequest);
        let (denied, _) = enforcing_principal(false);
        let denied = multipart_completion_authorization(
            EncryptionRequest {
                principal: Some(&denied),
                ..multipart_ssec_request(42)
            },
            false,
        );
        let error = denied.authorize(&sse_kms_object_metadata()).await.expect_err("KMS denied");
        assert_eq!(map_get_object_reader_error(error).code, S3ErrorCode::AccessDenied);
        let valid = multipart_completion_authorization(multipart_ssec_request(42), false);
        valid.authorize(&metadata).await.expect("correct customer key");
        let opts = crate::storage::ObjectOptions {
            multipart_completion_authorization: Some(valid),
            ..Default::default()
        };
        assert!(
            !format!("{opts:?}").contains(&key),
            "ObjectOptions Debug must never expose the captured customer key"
        );
    }

    #[tokio::test]
    async fn conditional_read_denies_kms_before_unwrap_with_normalized_metadata() {
        let request_id = "conditional-read-denies-kms-before-unwrap";
        let audit = KmsRequestAuditScope::register(request_id);
        let principal = principal_with_authorizer(true, RecordingKmsKeyAuthorizer::new(false)).with_request_id(request_id);
        let metadata = sse_kms_object_metadata()
            .into_iter()
            .map(|(name, value)| (name.to_ascii_uppercase(), value))
            .collect();
        let error = validate_conditional_read("finance", "ledger.csv", &metadata, &HeaderMap::new(), Some(&principal))
            .await
            .expect_err("conditional read must authorize the real key before unwrap");
        assert_eq!(error.code, S3ErrorCode::AccessDenied);
        assert_eq!(audit_tag(&audit.audit_tags(), "kmsKeyId").as_deref(), Some("finance-key"));
    }

    #[cfg(feature = "rio-v2")]
    mod rio_v2 {
        use super::{BASE64_STANDARD, enforcing_principal, md5_base64, ssec_read_headers};
        use crate::storage::sse::{
            EncryptionKeyKind, EncryptionRequest, ObjectEncryptionResolver, ReadEncryptionMode, ReadEncryptionRequest,
            SseObjectEncryptionResolver, encryption_material_to_metadata, reset_sse_dek_provider, sse_encryption,
            validate_conditional_read,
        };
        use http::HeaderMap;
        use rustfs_policy::policy::action::KmsAction;
        use rustfs_s3_types::S3ErrorCode;
        use rustfs_utils::http::headers::AMZ_ENCRYPTION_AES;
        use rustfs_utils::http::object_encryption_keys::{
            MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        };
        use std::collections::HashMap;
        use std::sync::OnceLock;
        use temp_env::async_with_vars;
        use tokio::sync::Mutex;

        static SSE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        async fn lock_sse_test_state() -> tokio::sync::MutexGuard<'static, ()> {
            SSE_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
        }

        fn ssec_write(customer_key: &str, customer_key_md5: &str) -> EncryptionRequest<'static> {
            EncryptionRequest {
                bucket: "bucket",
                key: "object",
                server_side_encryption: None,
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm: Some("AES256".to_string()),
                sse_customer_key: Some(customer_key.to_string()),
                sse_customer_key_md5: Some(customer_key_md5.to_string()),
                content_size: 4096,
                principal: None,
            }
        }

        #[tokio::test]
        async fn conditional_read_denies_minio_only_kms_before_unwrap() {
            let (principal, authorizer) = enforcing_principal(false);
            let metadata = HashMap::from([
                (MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_owned(), "sealed-key".to_owned()),
                (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_owned(), "finance-key".to_owned()),
            ]);
            let error = validate_conditional_read("finance", "ledger.csv", &metadata, &HeaderMap::new(), Some(&principal))
                .await
                .expect_err("MinIO metadata must not bypass KMS authorization");
            assert_eq!(error.code, S3ErrorCode::AccessDenied);
            assert_eq!(authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_owned())]);
        }

        #[tokio::test]
        async fn object_encryption_resolver_reads_managed_object_keys_in_object_mode() {
            let _guard = lock_sse_test_state().await;
            reset_sse_dek_provider();
            if let Some(manager) = rustfs_kms::get_global_kms_service_manager() {
                let _ = manager.stop().await;
            }
            let local_sse_master_key = BASE64_STANDARD.encode_to_string([0x24u8; 32]);

            async_with_vars(
                [
                    ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
                    ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
                ],
                async {
                    let material = sse_encryption(EncryptionRequest {
                        bucket: "bucket",
                        key: "object",
                        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                        ssekms_key_id: None,
                        ssekms_context: None,
                        sse_customer_algorithm: None,
                        sse_customer_key: None,
                        sse_customer_key_md5: None,
                        content_size: 4096,
                        principal: None,
                    })
                    .await
                    .expect("managed sse encryption")
                    .expect("managed sse material");
                    assert_eq!(material.key_kind, EncryptionKeyKind::Object);
                    let metadata = encryption_material_to_metadata(&material).expect("managed SSE metadata should serialize");

                    let resolved = SseObjectEncryptionResolver
                        .resolve_read_material(ReadEncryptionRequest {
                            bucket: "bucket",
                            object: "object",
                            metadata: &metadata,
                            headers: &HeaderMap::new(),
                        })
                        .await
                        .expect("managed resolver")
                        .expect("managed material");
                    assert_eq!(resolved.mode, ReadEncryptionMode::Object);
                    assert_eq!(resolved.key_bytes, material.key_bytes);
                },
            )
            .await;

            reset_sse_dek_provider();
        }

        #[tokio::test]
        async fn object_encryption_resolver_reads_sealed_ssec_object_keys_in_object_mode() {
            let customer_key_bytes = [0x42u8; 32];
            let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
            let customer_key_md5 = md5_base64(customer_key_bytes);

            let material = sse_encryption(ssec_write(&customer_key, &customer_key_md5))
                .await
                .expect("sse-c encryption")
                .expect("sse-c material");
            assert_eq!(material.key_kind, EncryptionKeyKind::Object);
            let metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");

            let resolved = SseObjectEncryptionResolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: "bucket",
                    object: "object",
                    metadata: &metadata,
                    headers: &ssec_read_headers(&customer_key, &customer_key_md5),
                })
                .await
                .expect("SSE-C resolver")
                .expect("SSE-C material");
            assert_eq!(resolved.mode, ReadEncryptionMode::Object);
            assert_eq!(resolved.key_bytes, material.key_bytes);
        }

        /// A MinIO-written SSE-C object keeps only the internal sealed-key slots.
        #[tokio::test]
        async fn object_encryption_resolver_reads_minio_shaped_ssec_metadata() {
            let customer_key_bytes = [0x51u8; 32];
            let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
            let customer_key_md5 = md5_base64(customer_key_bytes);

            let material = sse_encryption(ssec_write(&customer_key, &customer_key_md5))
                .await
                .expect("sse-c encryption")
                .expect("sse-c material");
            let mut metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
            metadata.remove("x-amz-server-side-encryption");
            metadata.remove("x-amz-server-side-encryption-customer-algorithm");
            metadata.remove("x-amz-server-side-encryption-customer-key-md5");

            let resolved = SseObjectEncryptionResolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: "bucket",
                    object: "object",
                    metadata: &metadata,
                    headers: &ssec_read_headers(&customer_key, &customer_key_md5),
                })
                .await
                .expect("minio-shaped SSE-C resolver")
                .expect("minio-shaped SSE-C material");
            assert_eq!(resolved.mode, ReadEncryptionMode::Object);
            assert_eq!(resolved.key_bytes, material.key_bytes);
        }
    }
}

#[cfg(test)]
mod managed_instance_tests {
    use super::{
        ApiError, DecryptionMaterial, DecryptionRequest, EncryptionMaterial, EncryptionRequest, KmsKeyAuthorizer, ManagedSseHost,
        ManagedSseOperation, ManagedSseRuntime, ReadEncryptionRequest, SseError, SseHost, SseKmsPrincipal, SseKmsPrincipalExt,
        TestAuthorizationHooks, kms_sse_dek_provider_for_test, managed_read_encryption_resolver, sse_decryption, sse_encryption,
    };
    use async_trait::async_trait;
    use http::HeaderMap;
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage, OperationContext};
    use rustfs_policy::policy::action::KmsAction;
    use rustfs_s3_types::S3ErrorCode;
    use rustfs_sse::encryption_material_to_metadata;
    use std::{collections::HashMap, sync::Arc};

    struct AllowKey;
    #[async_trait]
    impl KmsKeyAuthorizer for AllowKey {
        async fn is_allowed(&self, _: &SseKmsPrincipal, _: KmsAction, _: &str) -> Result<bool, SseError> {
            Ok(true)
        }
    }

    async fn runtime() -> (tempfile::TempDir, Arc<ManagedSseRuntime>) {
        let directory = tempfile::tempdir().expect("instance key directory");
        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
        manager
            .reconfigure(rustfs_kms::KmsConfig::local(directory.path().to_path_buf()).with_insecure_development_defaults())
            .await
            .expect("configure instance KMS");
        manager
            .get_encryption_service()
            .await
            .expect("instance service")
            .create_key(CreateKeyRequest {
                key_name: Some("maintenance-key".to_owned()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("create instance key");
        (directory, Arc::new(ManagedSseRuntime::new(Some(manager))))
    }

    fn principal_for_operation(operation: &Arc<ManagedSseOperation>, authorizer: Arc<dyn KmsKeyAuthorizer>) -> SseKmsPrincipal {
        SseKmsPrincipal::for_managed_maintenance(
            &rustfs_credentials::Credentials {
                access_key: "maintenance-account".to_owned(),
                ..Default::default()
            },
            HashMap::new(),
            &operation.operation_context.operation_id.to_string(),
            authorizer,
            operation.clone(),
        )
    }

    async fn admit(runtime: &Arc<ManagedSseRuntime>) -> (Arc<ManagedSseOperation>, SseKmsPrincipal) {
        let operation = Arc::new(
            runtime
                .begin_operation(OperationContext::new("maintenance-account".to_owned()))
                .await,
        );
        let principal = principal_for_operation(&operation, Arc::new(AllowKey));
        (operation, principal)
    }

    async fn principal(runtime: &Arc<ManagedSseRuntime>) -> SseKmsPrincipal {
        admit(runtime).await.1
    }

    async fn encrypt(
        bucket: &str,
        key: &str,
        algorithm: String,
        kms_key: Option<String>,
        kms_context: Option<HashMap<String, String>>,
        content_size: i64,
        principal: Option<&SseKmsPrincipal>,
    ) -> Result<EncryptionMaterial, ApiError> {
        sse_encryption(EncryptionRequest {
            bucket,
            key,
            server_side_encryption: Some(algorithm),
            ssekms_key_id: kms_key,
            ssekms_context: kms_context,
            content_size,
            principal,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        })
        .await?
        .ok_or_else(|| ApiError::invalid_request("encrypted material is required"))
    }

    async fn decrypt(
        bucket: &str,
        key: &str,
        metadata: &HashMap<String, String>,
        principal: Option<&SseKmsPrincipal>,
    ) -> Result<Option<DecryptionMaterial>, ApiError> {
        sse_decryption(DecryptionRequest {
            bucket,
            key,
            metadata,
            principal,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        })
        .await
    }

    #[tokio::test]
    async fn managed_service_binding_decrypts_only_its_own_ciphertext() {
        let (_a_directory, a) = runtime().await;
        let (_b_directory, b) = runtime().await;
        let a_principal = principal(&a).await;
        let b_principal = principal(&b).await;
        let material = encrypt(
            "bucket",
            "object",
            "aws:kms".to_owned(),
            Some("maintenance-key".to_owned()),
            None,
            32,
            Some(&b_principal),
        )
        .await
        .expect("B generates actual DEK");
        let metadata = encryption_material_to_metadata(&material).expect("persist B encryption metadata");
        let resolved = decrypt("bucket", "object", &metadata, Some(&b_principal))
            .await
            .expect("B decrypts B ciphertext")
            .expect("encrypted material");
        assert_eq!(resolved.key_bytes, material.key_bytes);
        assert!(
            decrypt("bucket", "object", &metadata, Some(&a_principal)).await.is_err(),
            "A must reject B ciphertext even when both name the same key"
        );
        let resolver = managed_read_encryption_resolver("bucket".to_owned(), "object".to_owned(), b_principal);
        let headers = HeaderMap::new();
        assert!(
            resolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: "bucket",
                    object: "other",
                    metadata: &metadata,
                    headers: &headers
                })
                .await
                .is_err(),
            "bound resolver must reject another object before resolving material"
        );
        let read = resolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &headers,
            })
            .await
            .expect("actual bound resolver")
            .expect("read material");
        assert_eq!(read.key_bytes, material.key_bytes);
    }

    struct DenyKey;
    #[async_trait]
    impl KmsKeyAuthorizer for DenyKey {
        async fn is_allowed(&self, _: &SseKmsPrincipal, _: KmsAction, _: &str) -> Result<bool, SseError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn managed_key_permissions_remain_mandatory_when_ordinary_enforcement_is_disabled() {
        let (_directory, runtime) = runtime().await;
        let (operation, allowed) = admit(&runtime).await;
        let material = encrypt(
            "bucket",
            "object",
            "aws:kms".to_owned(),
            Some("maintenance-key".to_owned()),
            None,
            32,
            Some(&allowed),
        )
        .await
        .expect("authorized encrypted body");
        let metadata = encryption_material_to_metadata(&material).expect("stored metadata");
        let denied = principal_for_operation(&operation, Arc::new(DenyKey)).with_test_hooks(Some(TestAuthorizationHooks {
            enforced: false,
            authorizer: Arc::new(AllowKey),
        }));
        let write = encrypt(
            "bucket",
            "object",
            "aws:kms".to_owned(),
            Some("maintenance-key".to_owned()),
            None,
            32,
            Some(&denied),
        )
        .await
        .expect_err("GenerateDataKey remains mandatory");
        assert_eq!(write.code, S3ErrorCode::AccessDenied);
        let read = decrypt("bucket", "object", &metadata, Some(&denied))
            .await
            .expect_err("Decrypt remains mandatory");
        assert_eq!(read.code, S3ErrorCode::AccessDenied);
        let service = operation.service.as_ref().expect("service");
        let host = ManagedSseHost {
            operation: operation.clone(),
            authorizer: Arc::new(AllowKey),
        };
        assert!(Arc::ptr_eq(&host.encryption_service().await.expect("retained service"), service));
        let manager = runtime.service_manager.clone().expect("instance manager");
        assert!(Arc::ptr_eq(&manager.get_encryption_service().await.expect("provider service"), service));
        let provider = kms_sse_dek_provider_for_test(manager).await.expect("actual KMS provider");
        assert!(!provider.reads_bare_data_key_ciphertext().await, "Local capability remains unchanged");
        let context = rustfs_kms::types::ObjectEncryptionContext::new("bucket".to_owned(), "object".to_owned())
            .with_encryption_context("bucket".to_owned(), "bucket/object".to_owned());
        let error = provider
            .rewrap_sse_dek(material.encrypted_data_key.as_ref().expect("encrypted DEK"), &context)
            .await
            .expect_err("Local backend keeps its explicit rewrap capability refusal");
        assert!(error.to_string().contains("rewrap") || format!("{:?}", error.source).contains("rewrap_data_key"));
    }

    #[tokio::test]
    async fn managed_admitted_service_survives_instance_reconfiguration() {
        let config = |fill: u8| {
            rustfs_kms::KmsConfig::static_kms("maintenance-key".to_owned(), base64_simd::STANDARD.encode_to_string([fill; 32]))
        };
        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());
        manager.reconfigure(config(0x11)).await.expect("configure instance KMS");
        let runtime = Arc::new(ManagedSseRuntime::new(Some(manager.clone())));
        let (admitted_operation, admitted) = admit(&runtime).await;
        let admitted_service = admitted_operation.service.as_ref().expect("admitted service");
        let material = encrypt(
            "bucket",
            "object",
            "aws:kms".to_owned(),
            Some("maintenance-key".to_owned()),
            None,
            32,
            Some(&admitted),
        )
        .await
        .expect("admitted encryption");
        let metadata = encryption_material_to_metadata(&material).expect("old service metadata");
        manager
            .reconfigure(config(0x22))
            .await
            .expect("replace this instance service");
        let replacement = manager.get_encryption_service().await.expect("replacement service");
        assert!(!Arc::ptr_eq(admitted_service, &replacement));
        let result = decrypt("bucket", "object", &metadata, Some(&admitted))
            .await
            .expect("admitted operation retains actual service Arc")
            .expect("old material");
        assert_eq!(result.key_bytes, material.key_bytes);
        let (next_operation, next) = admit(&runtime).await;
        let next_service = next_operation.service.as_ref().expect("next service");
        assert!(Arc::ptr_eq(next_service, &replacement));
        assert!(
            decrypt("bucket", "object", &metadata, Some(&next)).await.is_err(),
            "next operation must sample this instance's replacement service"
        );
    }

    #[tokio::test]
    async fn managed_unavailable_service_never_uses_another_instance() {
        let (_directory, configured) = runtime().await;
        let configured_principal = principal(&configured).await;
        let material = encrypt(
            "bucket",
            "object",
            "aws:kms".to_owned(),
            Some("maintenance-key".to_owned()),
            None,
            32,
            Some(&configured_principal),
        )
        .await
        .expect("configured encryption");
        let metadata = encryption_material_to_metadata(&material).expect("encryption metadata");
        let missing = Arc::new(ManagedSseRuntime::new(None));
        let missing_principal = principal(&missing).await;
        assert!(
            decrypt("bucket", "object", &metadata, Some(&missing_principal))
                .await
                .is_err()
        );
        assert!(
            encrypt(
                "bucket",
                "object",
                "aws:kms".to_owned(),
                Some("maintenance-key".to_owned()),
                None,
                32,
                Some(&missing_principal)
            )
            .await
            .is_err()
        );
    }
}
