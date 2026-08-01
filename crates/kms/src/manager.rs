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

//! KMS manager for handling key operations and backend coordination

use crate::audit::{KmsAuditOperation, KmsAuditRecord, KmsAuditSink};
use crate::backends::KmsBackend;
use crate::cache::KmsCache;
use crate::config::KmsConfig;
use crate::error::Result;
use crate::types::{
    CancelKeyDeletionRequest, CancelKeyDeletionResponse, CreateKeyRequest, CreateKeyResponse, DecryptRequest, DecryptResponse,
    DeleteKeyRequest, DeleteKeyResponse, DescribeKeyRequest, DescribeKeyResponse, EncryptRequest, EncryptResponse,
    GenerateDataKeyRequest, GenerateDataKeyResponse, ListKeysRequest, ListKeysResponse, OperationContext,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// KMS Manager coordinates operations between backends and caching
#[derive(Clone)]
pub struct KmsManager {
    backend: Arc<dyn KmsBackend>,
    cache: Arc<RwLock<KmsCache>>,
    default_key_id: Option<String>,
    enable_cache: bool,
    backend_kind: &'static str,
    audit_sink: Option<Arc<dyn KmsAuditSink>>,
}

impl KmsManager {
    /// Create a new KMS manager with the given backend and config
    pub fn new(backend: Arc<dyn KmsBackend>, config: KmsConfig) -> Self {
        let cache = Arc::new(RwLock::new(KmsCache::new(config.cache_config.max_keys as u64)));
        Self {
            backend,
            cache,
            default_key_id: config.default_key_id,
            enable_cache: config.enable_cache,
            backend_kind: config.backend.as_str(),
            audit_sink: None,
        }
    }

    /// Send an audit record for every management operation to `sink`.
    ///
    /// Without a sink the manager builds no records at all, so a deployment
    /// that does not consume KMS audit records is unaffected.
    pub fn with_audit_sink(mut self, sink: Arc<dyn KmsAuditSink>) -> Self {
        self.audit_sink = Some(sink);
        self
    }

    /// Get the default key ID if configured
    pub fn get_default_key_id(&self) -> Option<&String> {
        self.default_key_id.as_ref()
    }

    /// Emit an audit record for a completed management operation.
    ///
    /// Called after the operation resolved, so nothing here can change its
    /// result; the record is built only when a sink is installed.
    fn audit<T>(
        &self,
        operation: KmsAuditOperation,
        context: &OperationContext,
        key_id: Option<&str>,
        started: Instant,
        result: &Result<T>,
    ) {
        let Some(sink) = self.audit_sink.as_ref() else {
            return;
        };

        sink.emit(
            KmsAuditRecord::new(operation, context, self.backend_kind)
                .with_key_id(key_id)
                .with_latency(started.elapsed())
                .with_result(result),
        );
    }

    /// Create a new master key
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::create_key_with_context`].
    pub async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        self.create_key_with_context(request, &OperationContext::internal()).await
    }

    /// Create a new master key on behalf of `context`'s principal
    pub async fn create_key_with_context(
        &self,
        request: CreateKeyRequest,
        context: &OperationContext,
    ) -> Result<CreateKeyResponse> {
        let started = Instant::now();
        let key_name = request.key_name.clone();
        let result = self.create_key_inner(request).await;
        let key_id = result.as_ref().ok().map(|r| r.key_id.as_str()).or(key_name.as_deref());
        self.audit(KmsAuditOperation::CreateKey, context, key_id, started, &result);
        result
    }

    async fn create_key_inner(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let response = self.backend.create_key(request).await?;

        // Cache the key metadata if enabled
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.put_key_metadata(&response.key_id, &response.key_metadata).await;
        }

        Ok(response)
    }

    /// Encrypt data with a master key
    #[hotpath::measure]
    pub async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        self.backend.encrypt(request).await
    }

    /// Decrypt data with a master key
    #[hotpath::measure]
    pub async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        self.backend.decrypt(request).await
    }

    /// Generate a data encryption key
    #[hotpath::measure]
    pub async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        self.backend.generate_data_key(request).await
    }

    /// Describe a key
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::describe_key_with_context`].
    pub async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        self.describe_key_with_context(request, &OperationContext::internal()).await
    }

    /// Describe a key on behalf of `context`'s principal
    pub async fn describe_key_with_context(
        &self,
        request: DescribeKeyRequest,
        context: &OperationContext,
    ) -> Result<DescribeKeyResponse> {
        let started = Instant::now();
        let key_id = request.key_id.clone();
        let result = self.describe_key_inner(request).await;
        self.audit(KmsAuditOperation::DescribeKey, context, Some(&key_id), started, &result);
        result
    }

    async fn describe_key_inner(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        // Check cache first if enabled
        if self.enable_cache {
            let cache = self.cache.read().await;
            if let Some(cached_metadata) = cache.get_key_metadata(&request.key_id).await {
                return Ok(DescribeKeyResponse {
                    key_metadata: cached_metadata,
                });
            }
        }

        // Get from backend and cache
        let response = self.backend.describe_key(request).await?;

        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache
                .put_key_metadata(&response.key_metadata.key_id, &response.key_metadata)
                .await;
        }

        Ok(response)
    }

    /// List keys
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::list_keys_with_context`].
    pub async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        self.list_keys_with_context(request, &OperationContext::internal()).await
    }

    /// List keys on behalf of `context`'s principal
    pub async fn list_keys_with_context(&self, request: ListKeysRequest, context: &OperationContext) -> Result<ListKeysResponse> {
        let started = Instant::now();
        let result = self.backend.list_keys(request).await;
        // Listing spans keys, so the record carries no key id.
        self.audit(KmsAuditOperation::ListKeys, context, None, started, &result);
        result
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> Option<(u64, u64)> {
        if self.enable_cache {
            let cache = self.cache.read().await;
            Some(cache.stats())
        } else {
            None
        }
    }

    /// Clear the cache
    pub async fn clear_cache(&self) -> Result<()> {
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.clear().await;
        }
        Ok(())
    }

    /// Delete a key
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::delete_key_with_context`].
    pub async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        self.delete_key_with_context(request, &OperationContext::internal()).await
    }

    /// Delete a key on behalf of `context`'s principal
    pub async fn delete_key_with_context(
        &self,
        request: DeleteKeyRequest,
        context: &OperationContext,
    ) -> Result<DeleteKeyResponse> {
        let started = Instant::now();
        let key_id = request.key_id.clone();
        let result = self.delete_key_inner(request).await;
        self.audit(KmsAuditOperation::ScheduleKeyDeletion, context, Some(&key_id), started, &result);
        result
    }

    async fn delete_key_inner(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        let response = self.backend.delete_key(request).await?;

        // Remove from cache if enabled and key is being deleted
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.remove_key_metadata(&response.key_id).await;
        }

        Ok(response)
    }

    /// Cancel key deletion
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::cancel_key_deletion_with_context`].
    pub async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        self.cancel_key_deletion_with_context(request, &OperationContext::internal())
            .await
    }

    /// Cancel key deletion on behalf of `context`'s principal
    pub async fn cancel_key_deletion_with_context(
        &self,
        request: CancelKeyDeletionRequest,
        context: &OperationContext,
    ) -> Result<CancelKeyDeletionResponse> {
        let started = Instant::now();
        let key_id = request.key_id.clone();
        let result = self.cancel_key_deletion_inner(request).await;
        self.audit(KmsAuditOperation::CancelKeyDeletion, context, Some(&key_id), started, &result);
        result
    }

    async fn cancel_key_deletion_inner(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let response = self.backend.cancel_key_deletion(request).await?;

        // Update cache if enabled
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.put_key_metadata(&response.key_id, &response.key_metadata).await;
        }

        Ok(response)
    }

    /// Enable a disabled key
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::enable_key_with_context`].
    pub async fn enable_key(&self, key_id: &str) -> Result<()> {
        self.enable_key_with_context(key_id, &OperationContext::internal()).await
    }

    /// Enable a disabled key on behalf of `context`'s principal
    pub async fn enable_key_with_context(&self, key_id: &str, context: &OperationContext) -> Result<()> {
        let started = Instant::now();
        let result = self.backend.enable_key(key_id).await;
        if result.is_ok() {
            self.invalidate_cached_metadata(key_id).await;
        }
        self.audit(KmsAuditOperation::EnableKey, context, Some(key_id), started, &result);
        result
    }

    /// Disable a key; existing data remains decryptable
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::disable_key_with_context`].
    pub async fn disable_key(&self, key_id: &str) -> Result<()> {
        self.disable_key_with_context(key_id, &OperationContext::internal()).await
    }

    /// Disable a key on behalf of `context`'s principal
    pub async fn disable_key_with_context(&self, key_id: &str, context: &OperationContext) -> Result<()> {
        let started = Instant::now();
        let result = self.backend.disable_key(key_id).await;
        if result.is_ok() {
            self.invalidate_cached_metadata(key_id).await;
        }
        self.audit(KmsAuditOperation::DisableKey, context, Some(key_id), started, &result);
        result
    }

    /// Rotate a key to a new version
    ///
    /// Audited as an internal operation; callers serving an authenticated
    /// request should use [`Self::rotate_key_with_context`].
    pub async fn rotate_key(&self, key_id: &str) -> Result<()> {
        self.rotate_key_with_context(key_id, &OperationContext::internal()).await
    }

    /// Rotate a key on behalf of `context`'s principal
    pub async fn rotate_key_with_context(&self, key_id: &str, context: &OperationContext) -> Result<()> {
        let started = Instant::now();
        let result = self.backend.rotate_key(key_id).await;
        if result.is_ok() {
            self.invalidate_cached_metadata(key_id).await;
        }
        self.audit(KmsAuditOperation::RotateKey, context, Some(key_id), started, &result);
        result
    }

    /// Drop cached metadata after a state mutation so the next describe
    /// observes backend truth instead of the pre-mutation snapshot.
    async fn invalidate_cached_metadata(&self, key_id: &str) {
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.remove_key_metadata(key_id).await;
        }
    }

    /// Perform health check on the KMS backend
    pub async fn health_check(&self) -> Result<bool> {
        self.backend.health_check().await
    }

    /// Report the capabilities of the configured backend
    pub fn backend_capabilities(&self) -> crate::backends::BackendCapabilities {
        self.backend.capabilities()
    }

    /// Direct handle to the configured backend, bypassing the metadata cache.
    /// Used by background maintenance that must observe fresh state.
    pub(crate) fn backend(&self) -> Arc<dyn KmsBackend> {
        self.backend.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::KmsAuditOutcome;
    use crate::backends::local::LocalKmsBackend;
    use crate::error::KmsError;
    use crate::types::{KeyMetadata, KeySpec, KeyState, KeyUsage};
    use async_trait::async_trait;
    use base64::Engine as _;
    use jiff::Zoned;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::tempdir;

    /// Sink that keeps every record so tests can assert on the audit trail.
    #[derive(Default)]
    struct CapturingSink {
        records: Mutex<Vec<KmsAuditRecord>>,
    }

    impl KmsAuditSink for CapturingSink {
        fn emit(&self, record: KmsAuditRecord) {
            self.records
                .lock()
                .expect("audit records lock should not be poisoned")
                .push(record);
        }
    }

    impl CapturingSink {
        fn records(&self) -> Vec<KmsAuditRecord> {
            self.records
                .lock()
                .expect("audit records lock should not be poisoned")
                .clone()
        }

        fn take_one(&self) -> KmsAuditRecord {
            let mut records = self.records.lock().expect("audit records lock should not be poisoned");
            assert_eq!(records.len(), 1, "expected exactly one audit record, got {records:?}");
            records.remove(0)
        }
    }

    /// Backend whose management operations all succeed or all fail, so a
    /// single test can drive every audited operation down both paths —
    /// including operations no real backend supports on both.
    struct ScriptedBackend {
        failure: Option<KmsError>,
    }

    impl ScriptedBackend {
        fn succeeding() -> Self {
            Self { failure: None }
        }

        fn failing(failure: KmsError) -> Self {
            Self { failure: Some(failure) }
        }

        fn check(&self) -> Result<()> {
            match &self.failure {
                Some(failure) => Err(failure.clone()),
                None => Ok(()),
            }
        }

        fn metadata(key_id: &str) -> KeyMetadata {
            KeyMetadata {
                key_id: key_id.to_string(),
                key_state: KeyState::Enabled,
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                creation_date: Zoned::now(),
                deletion_date: None,
                origin: "RUSTFS_KMS".to_string(),
                key_manager: "RUSTFS".to_string(),
                tags: HashMap::new(),
            }
        }
    }

    #[async_trait]
    impl KmsBackend for ScriptedBackend {
        async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
            self.check()?;
            let key_id = request.key_name.unwrap_or_else(|| "scripted-key".to_string());
            Ok(CreateKeyResponse {
                key_metadata: Self::metadata(&key_id),
                key_id,
            })
        }

        async fn encrypt(&self, _request: EncryptRequest) -> Result<EncryptResponse> {
            unimplemented!("data plane is not audited by the manager")
        }

        async fn decrypt(&self, _request: DecryptRequest) -> Result<DecryptResponse> {
            unimplemented!("data plane is not audited by the manager")
        }

        async fn generate_data_key(&self, _request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
            unimplemented!("data plane is not audited by the manager")
        }

        async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
            self.check()?;
            Ok(DescribeKeyResponse {
                key_metadata: Self::metadata(&request.key_id),
            })
        }

        async fn list_keys(&self, _request: ListKeysRequest) -> Result<ListKeysResponse> {
            self.check()?;
            Ok(ListKeysResponse {
                keys: Vec::new(),
                next_marker: None,
                truncated: false,
            })
        }

        async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
            self.check()?;
            Ok(DeleteKeyResponse {
                key_metadata: Self::metadata(&request.key_id),
                key_id: request.key_id,
                deletion_date: None,
            })
        }

        async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
            self.check()?;
            Ok(CancelKeyDeletionResponse {
                key_metadata: Self::metadata(&request.key_id),
                key_id: request.key_id,
            })
        }

        async fn enable_key(&self, _key_id: &str) -> Result<()> {
            self.check()
        }

        async fn disable_key(&self, _key_id: &str) -> Result<()> {
            self.check()
        }

        async fn rotate_key(&self, _key_id: &str) -> Result<()> {
            self.check()
        }

        async fn health_check(&self) -> Result<bool> {
            Ok(true)
        }
    }

    const AUDITED_KEY_ID: &str = "audited-key";

    /// Prefix length used when checking that a record embedded no *part* of a
    /// secret. Long enough that a collision with unrelated text is not a real
    /// concern.
    const FRAGMENT_LEN: usize = 24;

    fn scripted_manager(backend: ScriptedBackend) -> (KmsManager, Arc<CapturingSink>) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let sink = Arc::new(CapturingSink::default());
        let manager = KmsManager::new(Arc::new(backend), config).with_audit_sink(sink.clone());
        (manager, sink)
    }

    fn request_context() -> OperationContext {
        OperationContext::new("arn:aws:iam::user/alice".to_string())
            .with_source_ip("192.0.2.10".to_string())
            .with_user_agent("rustfs-admin/1".to_string())
            .with_context("requestID".to_string(), "req-42".to_string())
    }

    /// Drive one management operation and return the single record it emitted.
    async fn run_operation(manager: &KmsManager, operation: KmsAuditOperation, context: &OperationContext) -> Result<()> {
        match operation {
            KmsAuditOperation::CreateKey => manager
                .create_key_with_context(
                    CreateKeyRequest {
                        key_name: Some(AUDITED_KEY_ID.to_string()),
                        ..Default::default()
                    },
                    context,
                )
                .await
                .map(|_| ()),
            KmsAuditOperation::DescribeKey => manager
                .describe_key_with_context(
                    DescribeKeyRequest {
                        key_id: AUDITED_KEY_ID.to_string(),
                    },
                    context,
                )
                .await
                .map(|_| ()),
            KmsAuditOperation::ListKeys => manager
                .list_keys_with_context(ListKeysRequest::default(), context)
                .await
                .map(|_| ()),
            KmsAuditOperation::ScheduleKeyDeletion => manager
                .delete_key_with_context(
                    DeleteKeyRequest {
                        key_id: AUDITED_KEY_ID.to_string(),
                        pending_window_in_days: None,
                        force_immediate: None,
                    },
                    context,
                )
                .await
                .map(|_| ()),
            KmsAuditOperation::CancelKeyDeletion => manager
                .cancel_key_deletion_with_context(
                    CancelKeyDeletionRequest {
                        key_id: AUDITED_KEY_ID.to_string(),
                    },
                    context,
                )
                .await
                .map(|_| ()),
            KmsAuditOperation::EnableKey => manager.enable_key_with_context(AUDITED_KEY_ID, context).await,
            KmsAuditOperation::DisableKey => manager.disable_key_with_context(AUDITED_KEY_ID, context).await,
            KmsAuditOperation::RotateKey => manager.rotate_key_with_context(AUDITED_KEY_ID, context).await,
            // Physical removal happens on the background sweep, not here.
            KmsAuditOperation::DeleteKey => unreachable!("removal is audited by the deletion worker"),
        }
    }

    /// Every management operation the manager serves, in audit terms.
    const AUDITED_OPERATIONS: [KmsAuditOperation; 8] = [
        KmsAuditOperation::CreateKey,
        KmsAuditOperation::DescribeKey,
        KmsAuditOperation::ListKeys,
        KmsAuditOperation::ScheduleKeyDeletion,
        KmsAuditOperation::CancelKeyDeletion,
        KmsAuditOperation::EnableKey,
        KmsAuditOperation::DisableKey,
        KmsAuditOperation::RotateKey,
    ];

    #[tokio::test]
    async fn every_management_operation_emits_a_complete_success_record() {
        for operation in AUDITED_OPERATIONS {
            let (manager, sink) = scripted_manager(ScriptedBackend::succeeding());
            let context = request_context();

            let outer = Instant::now();
            run_operation(&manager, operation, &context)
                .await
                .unwrap_or_else(|error| panic!("{} should succeed: {error}", operation.as_str()));
            let outer_elapsed = outer.elapsed();

            let record = sink.take_one();
            assert_eq!(record.operation, operation);
            assert_eq!(record.event, operation.event_name());
            assert_eq!(record.outcome, KmsAuditOutcome::Success);
            assert_eq!(record.error_class, None);
            assert_eq!(record.operation_id, context.operation_id);
            assert_eq!(record.principal, "arn:aws:iam::user/alice");
            assert_eq!(record.source_ip.as_deref(), Some("192.0.2.10"));
            assert_eq!(record.user_agent.as_deref(), Some("rustfs-admin/1"));
            assert_eq!(record.backend, "local");
            assert_eq!(record.context.get("requestID").map(String::as_str), Some("req-42"));
            assert!(
                record.latency <= outer_elapsed,
                "{} reported a latency larger than the call it measured",
                operation.as_str()
            );

            // Listing spans keys; every other operation names the key it touched.
            if operation == KmsAuditOperation::ListKeys {
                assert_eq!(record.key_id, None);
            } else {
                assert_eq!(record.key_id.as_deref(), Some(AUDITED_KEY_ID));
            }
        }
    }

    #[tokio::test]
    async fn every_management_operation_emits_a_failure_record() {
        for operation in AUDITED_OPERATIONS {
            let (manager, sink) = scripted_manager(ScriptedBackend::failing(KmsError::access_denied("denied by policy")));
            let context = request_context();

            let error = run_operation(&manager, operation, &context)
                .await
                .expect_err("scripted backend should reject the operation");
            assert!(matches!(error, KmsError::AccessDenied { .. }));

            let record = sink.take_one();
            assert_eq!(record.operation, operation);
            assert_eq!(record.event, operation.event_name());
            assert_eq!(record.outcome, KmsAuditOutcome::Failure);
            assert_eq!(record.error_class, Some("access_denied"));
            assert_eq!(record.operation_id, context.operation_id);
            assert_eq!(record.principal, "arn:aws:iam::user/alice");
            assert_eq!(record.source_ip.as_deref(), Some("192.0.2.10"));

            // A denied create still has to name the key the caller asked for,
            // otherwise the record cannot answer "what were they after".
            if operation != KmsAuditOperation::ListKeys {
                assert_eq!(record.key_id.as_deref(), Some(AUDITED_KEY_ID));
            }
        }
    }

    #[tokio::test]
    async fn operations_are_unaffected_when_no_sink_is_installed() {
        // The audit trail is optional; without a sink the manager must behave
        // exactly as it did before records existed.
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let manager = KmsManager::new(Arc::new(ScriptedBackend::succeeding()), config);

        for operation in AUDITED_OPERATIONS {
            run_operation(&manager, operation, &request_context())
                .await
                .unwrap_or_else(|error| panic!("{} should succeed without a sink: {error}", operation.as_str()));
        }
    }

    #[tokio::test]
    async fn context_free_calls_are_attributed_to_the_internal_principal() {
        // Callers that have no authenticated identity must still be
        // distinguishable from an identity we failed to record.
        let (manager, sink) = scripted_manager(ScriptedBackend::succeeding());

        manager
            .describe_key(DescribeKeyRequest {
                key_id: AUDITED_KEY_ID.to_string(),
            })
            .await
            .expect("describe should succeed");

        let record = sink.take_one();
        assert_eq!(record.principal, OperationContext::INTERNAL_PRINCIPAL);
        assert_eq!(record.source_ip, None);
        assert_eq!(record.user_agent, None);
    }

    #[tokio::test]
    async fn unsupported_lifecycle_operations_are_audited_with_their_own_class() {
        // The local backend has no version history, so rotation is a capability
        // gap rather than a policy denial; the audit trail must say so.
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let sink = Arc::new(CapturingSink::default());
        let manager = KmsManager::new(backend, config).with_audit_sink(sink.clone());

        let key_id = manager
            .create_key_with_context(
                CreateKeyRequest {
                    key_name: Some("rotate-me".to_string()),
                    ..Default::default()
                },
                &request_context(),
            )
            .await
            .expect("create should succeed")
            .key_id;

        manager
            .rotate_key_with_context(&key_id, &request_context())
            .await
            .expect_err("local rotation must be rejected");

        let records = sink.records();
        let rotate = records.last().expect("rotation should be audited");
        assert_eq!(rotate.operation, KmsAuditOperation::RotateKey);
        assert_eq!(rotate.outcome, KmsAuditOutcome::Failure);
        assert_eq!(rotate.error_class, Some("unsupported_capability"));
        assert_eq!(rotate.key_id.as_deref(), Some(key_id.as_str()));
    }

    /// Negative assertion: no audit record may reproduce key material. Driven
    /// against the real local backend so the assertion covers whatever the
    /// backend actually hands back, not a hand-written stand-in.
    #[tokio::test]
    async fn audit_records_never_reproduce_key_material() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let sink = Arc::new(CapturingSink::default());
        let manager = KmsManager::new(backend, config).with_audit_sink(sink.clone());

        let grant_token = "grant-token-cec4d4b5a1";
        let context = request_context();

        let key_id = manager
            .create_key_with_context(
                CreateKeyRequest {
                    key_name: Some("material-key".to_string()),
                    ..Default::default()
                },
                &context,
            )
            .await
            .expect("create should succeed")
            .key_id;

        // Produce real key material, then keep driving the management plane so
        // any record built afterwards is covered by the assertions below.
        let data_key = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: HashMap::from([("bucket".to_string(), "secrets".to_string())]),
            })
            .await
            .expect("data key generation should succeed");
        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: data_key.ciphertext_blob.clone(),
                encryption_context: HashMap::from([("bucket".to_string(), "secrets".to_string())]),
                grant_tokens: vec![grant_token.to_string()],
            })
            .await
            .expect("decrypt should succeed");

        manager
            .describe_key_with_context(DescribeKeyRequest { key_id: key_id.clone() }, &context)
            .await
            .expect("describe should succeed");
        manager
            .list_keys_with_context(ListKeysRequest::default(), &context)
            .await
            .expect("list should succeed");
        manager
            .disable_key_with_context(&key_id, &context)
            .await
            .expect("disable should succeed");
        manager
            .enable_key_with_context(&key_id, &context)
            .await
            .expect("enable should succeed");

        let base64 = base64::engine::general_purpose::STANDARD;
        let encodings = |bytes: &[u8]| vec![hex::encode(bytes), base64.encode(bytes)];
        let mut forbidden = vec![grant_token.to_string()];
        forbidden.extend(encodings(&data_key.plaintext_key));
        forbidden.extend(encodings(&decrypted.plaintext));
        forbidden.extend(encodings(&data_key.ciphertext_blob));
        // Fragments catch a record that embedded only part of a blob.
        let fragments: Vec<String> = forbidden
            .iter()
            .filter(|secret| secret.len() > FRAGMENT_LEN)
            .map(|secret| secret[..FRAGMENT_LEN].to_string())
            .collect();
        forbidden.extend(fragments);

        let records = sink.records();
        assert!(!records.is_empty(), "management operations should have been audited");
        for record in &records {
            let rendered = format!("{record:?}");
            for secret in &forbidden {
                assert!(
                    !rendered.contains(secret.as_str()),
                    "audit record leaked key material or a grant token: {rendered}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_manager_operations() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let manager = KmsManager::new(backend, config);

        // Test key creation
        let create_request = CreateKeyRequest {
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Test key".to_string()),
            ..Default::default()
        };

        let create_response = manager.create_key(create_request).await.expect("Failed to create key");
        assert!(!create_response.key_id.is_empty());
        assert_eq!(create_response.key_metadata.key_state, KeyState::Enabled);

        // Test data key generation
        let data_key_request = GenerateDataKeyRequest {
            key_id: create_response.key_id.clone(),
            key_spec: KeySpec::Aes256,
            encryption_context: Default::default(),
        };

        let data_key_response = manager
            .generate_data_key(data_key_request)
            .await
            .expect("Failed to generate data key");
        assert_eq!(data_key_response.plaintext_key.len(), 32); // 256 bits
        assert!(!data_key_response.ciphertext_blob.is_empty());

        // Test describe key
        let describe_request = DescribeKeyRequest {
            key_id: create_response.key_id.clone(),
        };

        let describe_response = manager.describe_key(describe_request).await.expect("Failed to describe key");
        assert_eq!(describe_response.key_metadata.key_id, create_response.key_id);

        // Test cache stats
        let stats = manager.cache_stats().await;
        assert!(stats.is_some());

        // Test health check
        let health = manager.health_check().await.expect("Health check failed");
        assert!(health);
    }

    #[tokio::test]
    async fn lifecycle_round_trip_invalidates_cached_metadata() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let manager = KmsManager::new(backend, config);

        let key_id = manager
            .create_key(CreateKeyRequest {
                key_name: Some("lifecycle-round-trip".to_string()),
                ..Default::default()
            })
            .await
            .expect("Failed to create key")
            .key_id;

        let describe = |key_id: String| {
            let manager = manager.clone();
            async move {
                manager
                    .describe_key(DescribeKeyRequest { key_id })
                    .await
                    .expect("describe should succeed")
                    .key_metadata
                    .key_state
            }
        };

        // Warm the metadata cache, then flip states; each describe must see
        // the post-mutation state, proving the cache entry was dropped.
        assert_eq!(describe(key_id.clone()).await, KeyState::Enabled);
        manager.disable_key(&key_id).await.expect("disable should succeed");
        assert_eq!(describe(key_id.clone()).await, KeyState::Disabled);
        manager.enable_key(&key_id).await.expect("enable should succeed");
        assert_eq!(describe(key_id.clone()).await, KeyState::Enabled);

        // The local backend does not retain version history, so rotation is
        // reported as a capability gap rather than a missing key.
        let error = manager.rotate_key(&key_id).await.expect_err("local rotate must be rejected");
        assert!(
            matches!(error, crate::error::KmsError::UnsupportedCapability { .. }),
            "expected UnsupportedCapability, got {error:?}"
        );
    }

    #[tokio::test]
    async fn generate_data_key_does_not_reuse_context_bound_ciphertext() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let manager = KmsManager::new(backend, config);

        let create_response = manager
            .create_key(CreateKeyRequest {
                key_usage: KeyUsage::EncryptDecrypt,
                description: Some("Context-bound data key test".to_string()),
                ..Default::default()
            })
            .await
            .expect("Failed to create key");

        let first_context = HashMap::from([
            ("bucket".to_string(), "sse-smoke".to_string()),
            ("object".to_string(), "first.bin".to_string()),
        ]);
        let second_context = HashMap::from([
            ("bucket".to_string(), "sse-smoke".to_string()),
            ("object".to_string(), "second.bin".to_string()),
        ]);

        let first = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: create_response.key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: first_context.clone(),
            })
            .await
            .expect("Failed to generate first data key");
        let second = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: create_response.key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: second_context.clone(),
            })
            .await
            .expect("Failed to generate second data key");

        assert_ne!(
            first.ciphertext_blob, second.ciphertext_blob,
            "data keys must not be cached only by KMS key id because ciphertext is bound to object context"
        );

        manager
            .decrypt(DecryptRequest {
                ciphertext: second.ciphertext_blob,
                encryption_context: second_context,
                grant_tokens: Vec::new(),
            })
            .await
            .expect("second data key should decrypt with its own context");
    }
}
