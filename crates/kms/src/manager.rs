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
use crate::cache::{KmsCache, KmsCacheStats};
use crate::config::{ENV_KMS_ALLOW_IMMEDIATE_DELETION, ENV_KMS_ROTATION_MAX_AGE_SECS, ENV_KMS_ROTATION_MAX_WRAPS, KmsConfig};
use crate::deletion_worker::DeletionReferenceChecker;
use crate::error::{KmsError, Result};
use crate::types::{
    CancelKeyDeletionRequest, CancelKeyDeletionResponse, CreateKeyRequest, CreateKeyResponse,
    DEFAULT_PENDING_DELETION_WINDOW_DAYS, DecryptRequest, DecryptResponse, DeleteKeyRequest, DeleteKeyResponse,
    DescribeDataKeyWrappingRequest, DescribeDataKeyWrappingResponse, DescribeKeyRequest, DescribeKeyResponse, EncryptRequest,
    EncryptResponse, GenerateDataKeyRequest, GenerateDataKeyResponse, KeyInfo, ListKeysRequest, ListKeysResponse,
    MAX_PENDING_DELETION_WINDOW_DAYS, MIN_PENDING_DELETION_WINDOW_DAYS, OperationContext, RewrapDataKeyRequest,
    RewrapDataKeyResponse, RotationDueReason,
};
use jiff::Zoned;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

/// KMS Manager coordinates operations between backends and caching
/// Smallest rotation age that can be configured.
///
/// A threshold shorter than this would report every key as overdue moments
/// after it was rotated, which trains operators to ignore the signal.
const MIN_ROTATION_MAX_AGE: Duration = Duration::from_secs(3600);

/// Smallest wrap budget that can be configured.
///
/// Wraps are accounted in reserved blocks, so any threshold below one block
/// would be crossed by a single reservation and report a key that has barely
/// wrapped anything as overdue.
const MIN_ROTATION_MAX_WRAPS: u64 = 1_000_000;

/// Rotation age from the environment, or `None` when the signal is off.
///
/// Unset leaves it off rather than guessing a policy: how often a deployment
/// must rotate is a compliance decision, and inventing a default would report
/// keys as overdue against a rule nobody chose. An unparsable value is refused
/// the same way, loudly, instead of falling back to a number the operator did
/// not write.
fn configured_rotation_max_age() -> Option<Duration> {
    parse_rotation_max_age(std::env::var(ENV_KMS_ROTATION_MAX_AGE_SECS).ok().as_deref())
}

fn parse_rotation_max_age(value: Option<&str>) -> Option<Duration> {
    let value = value?;
    let Ok(seconds) = value.trim().parse::<u64>() else {
        warn!(
            variable = ENV_KMS_ROTATION_MAX_AGE_SECS,
            "ignoring unparsable KMS rotation age; rotation readiness stays unreported"
        );
        return None;
    };
    if seconds == 0 {
        return None;
    }
    Some(Duration::from_secs(seconds).max(MIN_ROTATION_MAX_AGE))
}

/// Wrap budget from the environment, or `None` when the signal is off.
///
/// Same discipline as the age threshold: unset means unreported rather than a
/// guessed policy, and an unparsable value is refused loudly instead of
/// falling back to a number the operator did not write. Clamped to
/// [`MIN_ROTATION_MAX_WRAPS`] because the backend accounts for wraps in
/// reserved blocks, so a threshold below one block would trip on the first
/// reservation regardless of how many wraps actually happened.
fn configured_rotation_max_wraps() -> Option<u64> {
    parse_rotation_max_wraps(std::env::var(ENV_KMS_ROTATION_MAX_WRAPS).ok().as_deref())
}

fn parse_rotation_max_wraps(value: Option<&str>) -> Option<u64> {
    let value = value?;
    let Ok(wraps) = value.trim().parse::<u64>() else {
        warn!(
            variable = ENV_KMS_ROTATION_MAX_WRAPS,
            "ignoring unparsable KMS rotation wrap budget; rotation readiness stays unreported"
        );
        return None;
    };
    if wraps == 0 {
        return None;
    }
    Some(wraps.max(MIN_ROTATION_MAX_WRAPS))
}

#[derive(Clone)]
pub struct KmsManager {
    backend: Arc<dyn KmsBackend>,
    cache: Arc<RwLock<KmsCache>>,
    default_key_id: Option<String>,
    enable_cache: bool,
    backend_kind: &'static str,
    audit_sink: Option<Arc<dyn KmsAuditSink>>,
    allow_immediate_deletion: bool,
    reference_checker: Option<Arc<dyn DeletionReferenceChecker>>,
    /// Age beyond which a key is reported as due for rotation; `None` leaves
    /// the verdict unreported. Read once at construction so a listing cannot
    /// change its answer halfway through.
    rotation_max_age: Option<Duration>,
    rotation_max_wraps: Option<u64>,
}

impl KmsManager {
    /// Create a new KMS manager with the given backend and config
    pub fn new(backend: Arc<dyn KmsBackend>, config: KmsConfig) -> Self {
        let cache = Arc::new(RwLock::new(KmsCache::new(&config.cache_config)));
        if config.allow_immediate_deletion {
            warn!(
                "KMS immediate key deletion is enabled: a DeleteKey request may destroy key material without any waiting window, and every object encrypted under that key becomes permanently unreadable"
            );
        }
        Self {
            backend,
            cache,
            default_key_id: config.default_key_id,
            enable_cache: config.enable_cache,
            backend_kind: config.backend.as_str(),
            audit_sink: None,
            allow_immediate_deletion: config.allow_immediate_deletion,
            reference_checker: None,
            rotation_max_age: configured_rotation_max_age(),
            rotation_max_wraps: configured_rotation_max_wraps(),
        }
    }

    /// Consult `checker` before immediate deletion destroys key material.
    ///
    /// This is the same checker the deletion worker consults before it removes
    /// an expired key; installing it here extends that gate to the one
    /// deletion path that never reaches the worker. It can only add a refusal:
    /// without a checker the manager behaves exactly as before.
    pub fn with_deletion_reference_checker(mut self, checker: Option<Arc<dyn DeletionReferenceChecker>>) -> Self {
        self.reference_checker = checker;
        self
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

    /// Re-wrap an existing data key envelope onto the master key's current
    /// version, leaving the data key — and therefore every object body it
    /// protects — untouched.
    ///
    /// Backends without retained version history reject this with
    /// [`KmsError::UnsupportedCapability`]; check
    /// [`Self::backend_capabilities`] before offering it.
    pub async fn rewrap_data_key(&self, request: RewrapDataKeyRequest) -> Result<RewrapDataKeyResponse> {
        self.backend.rewrap_data_key(request).await
    }

    /// Report which master key version wraps an existing data key envelope.
    ///
    /// The read-only side of [`Self::rewrap_data_key`], and the supported way to
    /// ask that question: where the version is recorded differs per backend, so
    /// callers must not inspect envelopes themselves.
    pub async fn describe_data_key_wrapping(
        &self,
        request: DescribeDataKeyWrappingRequest,
    ) -> Result<DescribeDataKeyWrappingResponse> {
        self.backend.describe_data_key_wrapping(request).await
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
        let mut result = self.backend.list_keys(request).await;
        if let Ok(response) = result.as_mut() {
            let rotates = self.backend.capabilities().rotate;
            let now = Zoned::now();
            for key in &mut response.keys {
                self.apply_rotation_readiness(key, rotates, &now);
            }
        }
        // Listing spans keys, so the record carries no key id.
        self.audit(KmsAuditOperation::ListKeys, context, None, started, &result);
        result
    }

    /// Fill in the advisory rotation verdict for one listed key.
    ///
    /// Decided here rather than in each backend so no two backends can disagree
    /// about what "overdue" means, and so a backend that cannot rotate is never
    /// the one deciding whether it should. Nothing consults the verdict before
    /// encrypting or decrypting: a key reported as due keeps serving traffic.
    fn apply_rotation_readiness(&self, key: &mut KeyInfo, backend_rotates: bool, now: &Zoned) {
        if !backend_rotates {
            // Reported, not silently omitted: an operator chasing an overdue key
            // needs to know the answer is "this backend cannot rotate at all",
            // which is a backend choice to revisit rather than a key to fix.
            key.rotation_due = false;
            key.rotation_due_reason = Some(RotationDueReason::Unsupported);
            return;
        }
        key.rotation_due = false;
        key.rotation_due_reason = None;

        // The wrap budget is checked first: it is the cryptographic bound (the
        // AES-GCM random-nonce ceiling), whereas the age threshold is a policy
        // choice, so when both are crossed the reason an operator most needs to
        // see is the one they cannot negotiate.
        if let (Some(max_wraps), Some(wraps)) = (self.rotation_max_wraps, key.wrap_budget_reserved)
            && wraps >= max_wraps
        {
            key.rotation_due = true;
            key.rotation_due_reason = Some(RotationDueReason::Wraps);
            return;
        }

        let Some(max_age) = self.rotation_max_age else {
            return;
        };

        // A key that was never rotated is measured from when it was created:
        // that is how long its material has been in use, which is the quantity
        // the threshold is about.
        let (since, reason) = match key.rotated_at.as_ref() {
            Some(rotated_at) => (rotated_at, RotationDueReason::Age),
            None => (&key.created_at, RotationDueReason::NeverRotated),
        };
        // Saturating: a timestamp from a node running ahead must not read as an
        // enormous age and report a fresh key as overdue.
        let age = Duration::from_secs((now.timestamp().as_second() - since.timestamp().as_second()).max(0) as u64);
        if age >= max_age {
            key.rotation_due = true;
            key.rotation_due_reason = Some(reason);
        }
    }

    /// Get cache statistics, or `None` when caching is disabled
    pub async fn cache_stats(&self) -> Option<KmsCacheStats> {
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

    /// Delete a key, either scheduled behind the waiting window or — when the
    /// server allows it — immediately.
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

    /// This is the single enforcement point for the waiting window: every
    /// admin-facing deletion goes through here, so the checks below run before
    /// any backend sees the request. The backends repeat the window bound as a
    /// defensive assertion for callers that hold a backend handle directly.
    async fn delete_key_inner(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        self.check_deletion_request(&request)?;
        if request.force_immediate.unwrap_or(false) {
            self.refuse_referenced_immediate_deletion(&request.key_id).await?;
        }

        let response = self.backend.delete_key(request).await?;

        // Remove from cache if enabled and key is being deleted
        if self.enable_cache {
            let mut cache = self.cache.write().await;
            cache.remove_key_metadata(&response.key_id).await;
        }

        Ok(response)
    }

    /// Gate a deletion request before it reaches the backend.
    ///
    /// Immediate deletion is unrecoverable, so it needs both a server-side
    /// opt-in and a per-request confirmation that echoes the key id; without
    /// either, the request is refused rather than downgraded to a scheduled
    /// deletion, so a caller never believes a key is gone when it is not.
    fn check_deletion_request(&self, request: &DeleteKeyRequest) -> Result<()> {
        if !request.force_immediate.unwrap_or(false) {
            let days = request.pending_window_in_days.unwrap_or(DEFAULT_PENDING_DELETION_WINDOW_DAYS);
            if !(MIN_PENDING_DELETION_WINDOW_DAYS..=MAX_PENDING_DELETION_WINDOW_DAYS).contains(&days) {
                return Err(KmsError::invalid_parameter(format!(
                    "pending_window_in_days must be between {MIN_PENDING_DELETION_WINDOW_DAYS} and {MAX_PENDING_DELETION_WINDOW_DAYS}"
                )));
            }
            return Ok(());
        }

        if !self.allow_immediate_deletion {
            return Err(KmsError::invalid_operation(format!(
                "immediate deletion of key {} is not allowed; schedule the deletion and wait out the pending window, or set {ENV_KMS_ALLOW_IMMEDIATE_DELETION}=true on the server",
                request.key_id
            )));
        }

        if request.confirm_key_id.as_deref() != Some(request.key_id.as_str()) {
            return Err(KmsError::invalid_operation(format!(
                "immediate deletion of key {} requires confirm_key_id to repeat the key id exactly",
                request.key_id
            )));
        }

        warn!(
            key_id = %request.key_id,
            "immediate KMS key deletion accepted; key material is destroyed without a waiting window and cannot be recovered"
        );
        Ok(())
    }

    /// Refuse an immediate deletion while configuration still points at the
    /// key.
    ///
    /// A scheduled deletion is re-checked against these same references by the
    /// deletion worker before it destroys anything, and stays cancellable
    /// until then. Immediate deletion has neither property: it destroys
    /// material on the spot and never reaches the worker, so the check has to
    /// happen here or not at all.
    ///
    /// Only ever a refusal. An empty reference set is not a clearance — it
    /// means the sources consulted here raised no objection, while the caller
    /// still had to pass the server-side opt-in and the key-id confirmation to
    /// get this far. With no checker installed the manager has no
    /// configuration source to consult and behaves as it did before, matching
    /// the deletion worker, which also skips a checker it was not given.
    async fn refuse_referenced_immediate_deletion(&self, key_id: &str) -> Result<()> {
        let mut references = Vec::new();
        if self.default_key_id.as_deref() == Some(key_id) {
            references.push("kms-service-default-key".to_string());
        }
        if let Some(checker) = &self.reference_checker {
            references.extend(checker.references(key_id).await);
        }
        if references.is_empty() {
            return Ok(());
        }

        warn!(
            key_id,
            ?references,
            "immediate KMS key deletion refused; configuration still references the key"
        );
        Err(KmsError::key_still_referenced(key_id, references))
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

    /// Replace a key's description; `None` clears it
    pub async fn update_key_description(&self, key_id: &str, description: Option<&str>) -> Result<()> {
        self.backend.update_key_description(key_id, description).await?;
        self.invalidate_cached_metadata(key_id).await;
        Ok(())
    }

    /// Add or overwrite key tags, leaving every other tag untouched
    pub async fn tag_key(&self, key_id: &str, tags: &HashMap<String, String>) -> Result<()> {
        self.backend.tag_key(key_id, tags).await?;
        self.invalidate_cached_metadata(key_id).await;
        Ok(())
    }

    /// Remove key tags; tags that are not set are ignored
    pub async fn untag_key(&self, key_id: &str, tag_keys: &[String]) -> Result<()> {
        self.backend.untag_key(key_id, tag_keys).await?;
        self.invalidate_cached_metadata(key_id).await;
        Ok(())
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

    /// The running client a full-material backup can be exported from, or
    /// `None` for backends whose cryptographic root lives outside RustFS.
    ///
    /// See [`KmsBackend::local_backup_client`] for why the export must use the
    /// running client rather than a freshly opened one.
    pub fn local_backup_client(&self) -> Option<&crate::backends::local::LocalKmsClient> {
        self.backend.local_backup_client()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::KmsAuditOutcome;
    use crate::backends::local::LocalKmsBackend;
    use crate::error::KmsError;
    use crate::types::{KeyMetadata, KeySpec, KeyState, KeyStatus, KeyUsage};
    use async_trait::async_trait;
    use base64::Engine as _;
    use jiff::Zoned;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;
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
                unreadable_key_ids: Vec::new(),
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
                        confirm_key_id: None,
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

        // Creating the key populated the cache, so the describe above was
        // served from it rather than from the backend.
        let stats = manager.cache_stats().await.expect("cache is enabled");
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);

        // Test health check
        let health = manager.health_check().await.expect("Health check failed");
        assert!(health);
    }

    #[tokio::test]
    async fn configured_cache_ttl_bounds_how_long_metadata_is_reused() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        config.cache_config.ttl = Duration::from_millis(100);

        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let manager = KmsManager::new(backend, config);

        let key_id = manager
            .create_key(CreateKeyRequest {
                key_name: Some("cache-ttl-wiring".to_string()),
                ..Default::default()
            })
            .await
            .expect("Failed to create key")
            .key_id;

        // Creating the key populated the cache, so this describe is served from it.
        manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe should succeed");
        assert_eq!(manager.cache_stats().await.expect("cache is enabled").hits, 1);

        // Past the configured lifetime the entry is gone and the describe falls
        // through to the backend. A cache built with a hardcoded lifetime would
        // still be serving the entry here.
        tokio::time::sleep(Duration::from_millis(150)).await;
        manager
            .describe_key(DescribeKeyRequest { key_id })
            .await
            .expect("describe should succeed");

        let stats = manager.cache_stats().await.expect("cache is enabled");
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
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

    /// Manager over a local backend, with the immediate-deletion gate set as
    /// the server operator would set it.
    async fn deletion_manager(temp_dir: &tempfile::TempDir, allow_immediate_deletion: bool) -> KmsManager {
        let mut config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        config.allow_immediate_deletion = allow_immediate_deletion;
        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        KmsManager::new(backend, config)
    }

    async fn create_named_key(manager: &KmsManager, key_name: &str) -> String {
        manager
            .create_key(CreateKeyRequest {
                key_name: Some(key_name.to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("Failed to create key")
            .key_id
    }

    /// Data key generated up front, decrypted again afterwards: a refused
    /// deletion must leave the master key material byte-for-byte usable, not
    /// merely leave a metadata record behind.
    async fn data_key_probe(manager: &KmsManager, key_id: &str) -> (Vec<u8>, Vec<u8>) {
        let generated = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.to_string(),
                key_spec: KeySpec::Aes256,
                encryption_context: HashMap::new(),
            })
            .await
            .expect("Failed to generate data key");
        (generated.plaintext_key, generated.ciphertext_blob)
    }

    async fn assert_key_material_intact(manager: &KmsManager, key_id: &str, probe: &(Vec<u8>, Vec<u8>)) {
        let state = manager
            .describe_key(DescribeKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .expect("a key that was not deleted must still be describable")
            .key_metadata
            .key_state;
        assert_eq!(state, KeyState::Enabled, "a refused deletion must not change the key state");

        let decrypted = manager
            .decrypt(DecryptRequest {
                ciphertext: probe.1.clone(),
                encryption_context: HashMap::new(),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("key material must still decrypt data keys issued before the refused deletion");
        assert_eq!(decrypted.plaintext, probe.0, "decrypted data key must match the original plaintext");
    }

    #[tokio::test]
    async fn immediate_deletion_is_refused_under_default_config() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, false).await;
        let key_id = create_named_key(&manager, "default-config-force-delete").await;
        let probe = data_key_probe(&manager, &key_id).await;

        // Confirmation present and correct: the server-side gate alone must
        // refuse this, no matter how well-formed the request is.
        let error = manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect_err("immediate deletion must be refused unless the server allows it");
        assert!(
            matches!(error, KmsError::InvalidOperation { .. }),
            "expected InvalidOperation, got {error:?}"
        );

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    #[tokio::test]
    async fn immediate_deletion_requires_a_matching_confirmation() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, true).await;
        let key_id = create_named_key(&manager, "confirmation-required").await;
        let probe = data_key_probe(&manager, &key_id).await;

        for confirmation in [None, Some(String::new()), Some(format!("{key_id}-typo"))] {
            let result = manager
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    force_immediate: Some(true),
                    confirm_key_id: confirmation.clone(),
                    ..Default::default()
                })
                .await;
            assert!(
                matches!(result, Err(KmsError::InvalidOperation { .. })),
                "confirmation {confirmation:?} must be refused, got {result:?}"
            );
        }

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    #[tokio::test]
    async fn immediate_deletion_succeeds_with_a_matching_confirmation() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, true).await;
        let key_id = create_named_key(&manager, "confirmed-force-delete").await;

        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect("a confirmed immediate deletion must be allowed once the server enables it");

        let error = manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect_err("an immediately deleted key must be gone");
        assert!(matches!(error, KmsError::KeyNotFound { .. }), "expected KeyNotFound, got {error:?}");
    }

    /// Reference checker whose answer is fixed, standing in for the server's
    /// bucket-configuration gate.
    struct StaticReferences(Vec<String>);

    #[async_trait]
    impl DeletionReferenceChecker for StaticReferences {
        async fn references(&self, _key_id: &str) -> Vec<String> {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn immediate_deletion_is_refused_while_configuration_references_the_key() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, true)
            .await
            .with_deletion_reference_checker(Some(Arc::new(StaticReferences(vec!["bucket:sse-bucket".to_string()]))));
        let key_id = create_named_key(&manager, "referenced-force-delete").await;
        let probe = data_key_probe(&manager, &key_id).await;

        // Server opt-in granted and the confirmation exact: the reference is
        // the only thing left to refuse this.
        let error = manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect_err("immediate deletion must be refused while configuration references the key");

        match error {
            KmsError::KeyStillReferenced {
                key_id: refused,
                references,
            } => {
                assert_eq!(refused, key_id);
                assert_eq!(references, vec!["bucket:sse-bucket".to_string()], "the caller must learn what refused it");
            }
            other => panic!("expected KeyStillReferenced, got {other:?}"),
        }

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    #[tokio::test]
    async fn immediate_deletion_of_the_service_default_key_is_refused() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let mut config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        config.allow_immediate_deletion = true;
        let backend = Arc::new(LocalKmsBackend::new(config.clone()).await.expect("Failed to create backend"));
        let key_id = KmsManager::new(backend.clone(), config.clone())
            .create_key(CreateKeyRequest {
                key_name: Some("default-key-force-delete".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("Failed to create key")
            .key_id;

        config.default_key_id = Some(key_id.clone());
        let manager = KmsManager::new(backend, config);
        let probe = data_key_probe(&manager, &key_id).await;

        let error = manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect_err("the service default key must not be destroyed out from under the deployment");
        assert!(
            matches!(error, KmsError::KeyStillReferenced { .. }),
            "expected KeyStillReferenced, got {error:?}"
        );

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    /// A checker that reports nothing must not become a shortcut around the
    /// gates that were already there: it is not a clearance, only the absence
    /// of one more objection.
    #[tokio::test]
    async fn an_empty_reference_set_grants_no_deletion_on_its_own() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, false)
            .await
            .with_deletion_reference_checker(Some(Arc::new(StaticReferences(Vec::new()))));
        let key_id = create_named_key(&manager, "unreferenced-force-delete").await;
        let probe = data_key_probe(&manager, &key_id).await;

        // Server opt-in withheld, then confirmation missing: both still refuse.
        let error = manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect_err("an unreferenced key still needs the server-side opt-in");
        assert!(
            matches!(error, KmsError::InvalidOperation { .. }),
            "expected InvalidOperation, got {error:?}"
        );

        let allowed = deletion_manager(&temp_dir, true)
            .await
            .with_deletion_reference_checker(Some(Arc::new(StaticReferences(Vec::new()))));
        let error = allowed
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                ..Default::default()
            })
            .await
            .expect_err("an unreferenced key still needs the key-id confirmation");
        assert!(
            matches!(error, KmsError::InvalidOperation { .. }),
            "expected InvalidOperation, got {error:?}"
        );

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    /// The refusal is the only thing the checker adds: a fully authorized
    /// immediate deletion of a key nothing points at still goes through.
    #[tokio::test]
    async fn immediate_deletion_still_succeeds_when_nothing_references_the_key() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, true)
            .await
            .with_deletion_reference_checker(Some(Arc::new(StaticReferences(Vec::new()))));
        let key_id = create_named_key(&manager, "unreferenced-confirmed-force-delete").await;

        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                force_immediate: Some(true),
                confirm_key_id: Some(key_id.clone()),
                ..Default::default()
            })
            .await
            .expect("a confirmed immediate deletion must still be allowed when nothing references the key");

        let error = manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect_err("an immediately deleted key must be gone");
        assert!(matches!(error, KmsError::KeyNotFound { .. }), "expected KeyNotFound, got {error:?}");
    }

    /// Scheduling stays a schedule: it destroys nothing, stays cancellable,
    /// and is re-checked against the same references by the deletion worker
    /// before any material goes away. Turning references into an up-front
    /// refusal here would let one unreadable bucket block routine operations.
    #[tokio::test]
    async fn scheduled_deletion_is_unaffected_by_references() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, false)
            .await
            .with_deletion_reference_checker(Some(Arc::new(StaticReferences(vec!["bucket:sse-bucket".to_string()]))));
        let key_id = create_named_key(&manager, "referenced-schedule").await;

        manager
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                ..Default::default()
            })
            .await
            .expect("a scheduled deletion must still be accepted while configuration references the key");

        let state = manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("a scheduled key must still be describable")
            .key_metadata
            .key_state;
        assert_eq!(state, KeyState::PendingDeletion);
    }

    #[tokio::test]
    async fn pending_window_outside_the_supported_range_is_refused() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, false).await;
        let key_id = create_named_key(&manager, "window-bounds").await;
        let probe = data_key_probe(&manager, &key_id).await;

        for days in [0, MIN_PENDING_DELETION_WINDOW_DAYS - 1, MAX_PENDING_DELETION_WINDOW_DAYS + 1] {
            let result = manager
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    pending_window_in_days: Some(days),
                    ..Default::default()
                })
                .await;
            assert!(
                matches!(result, Err(KmsError::InvalidOperation { .. })),
                "a {days}-day window must be refused, got {result:?}"
            );
        }

        assert_key_material_intact(&manager, &key_id, &probe).await;
    }

    #[tokio::test]
    async fn scheduled_deletion_keeps_its_existing_behaviour() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let manager = deletion_manager(&temp_dir, false).await;

        for (name, days) in [
            ("schedule-default-window", None),
            ("schedule-min-window", Some(MIN_PENDING_DELETION_WINDOW_DAYS)),
            ("schedule-max-window", Some(MAX_PENDING_DELETION_WINDOW_DAYS)),
        ] {
            let key_id = create_named_key(&manager, name).await;
            let response = manager
                .delete_key(DeleteKeyRequest {
                    key_id: key_id.clone(),
                    pending_window_in_days: days,
                    ..Default::default()
                })
                .await
                .expect("scheduling a deletion inside the window must still succeed");
            assert!(response.deletion_date.is_some(), "a scheduled deletion must report its deadline");
            assert_eq!(response.key_metadata.key_state, KeyState::PendingDeletion);

            manager
                .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
                .await
                .expect("a scheduled deletion must still be cancellable");
            let state = manager
                .describe_key(DescribeKeyRequest { key_id })
                .await
                .expect("describe should succeed")
                .key_metadata
                .key_state;
            assert_eq!(state, KeyState::Enabled, "cancelling must restore the key");
        }
    }
    /// The threshold is a compliance decision, so nothing is inferred: unset
    /// and unparsable both leave the signal off rather than reporting keys
    /// overdue against a rule nobody wrote. A value below the floor is raised,
    /// because a threshold of a few seconds reports every key as overdue moments
    /// after it was rotated and trains operators to ignore the signal.
    #[test]
    fn rotation_age_is_configured_or_absent_never_guessed() {
        assert_eq!(parse_rotation_max_age(None), None);
        assert_eq!(parse_rotation_max_age(Some("not-a-number")), None);
        assert_eq!(parse_rotation_max_age(Some("")), None);
        assert_eq!(parse_rotation_max_age(Some("-1")), None);
        assert_eq!(parse_rotation_max_age(Some("0")), None);

        assert_eq!(parse_rotation_max_age(Some("1")), Some(MIN_ROTATION_MAX_AGE));
        assert_eq!(parse_rotation_max_age(Some(" 86400 ")), Some(Duration::from_secs(86_400)));
        assert_eq!(
            parse_rotation_max_age(Some(&MIN_ROTATION_MAX_AGE.as_secs().to_string())),
            Some(MIN_ROTATION_MAX_AGE)
        );
    }

    fn readiness_manager(rotation_max_age: Option<Duration>) -> KmsManager {
        readiness_manager_with(rotation_max_age, None)
    }

    fn readiness_manager_with(rotation_max_age: Option<Duration>, rotation_max_wraps: Option<u64>) -> KmsManager {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let mut manager = KmsManager::new(Arc::new(ScriptedBackend::succeeding()), config);
        manager.rotation_max_age = rotation_max_age;
        manager.rotation_max_wraps = rotation_max_wraps;
        manager
    }

    fn aged_key(rotated_at: Option<Zoned>, created_at: Zoned) -> KeyInfo {
        KeyInfo {
            key_id: "key-a".to_string(),
            description: None,
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            created_at,
            rotated_at,
            created_by: None,
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: None,
        }
    }

    /// The whole decision matrix, including the one case that must never fire:
    /// a backend that cannot rotate is never told to rotate.
    #[test]
    fn rotation_readiness_reports_but_never_asks_the_impossible() {
        let now = Zoned::now();
        let long_ago = &now - jiff::Span::new().days(400);
        let recently = &now - jiff::Span::new().hours(1);
        let day = Duration::from_secs(86_400);

        // A backend without rotation is reported as such and never as overdue,
        // however ancient the key and however short the threshold.
        let manager = readiness_manager(Some(Duration::from_secs(1)));
        let mut key = aged_key(None, long_ago.clone());
        manager.apply_rotation_readiness(&mut key, false, &now);
        assert!(!key.rotation_due, "a backend that cannot rotate must never be told to");
        assert_eq!(key.rotation_due_reason, Some(RotationDueReason::Unsupported));

        // No threshold configured: no verdict, on any key.
        let manager = readiness_manager(None);
        let mut key = aged_key(None, long_ago.clone());
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert!(!key.rotation_due);
        assert_eq!(key.rotation_due_reason, None);

        let manager = readiness_manager(Some(day));

        // Rotated, but longer ago than the threshold.
        let mut key = aged_key(Some(long_ago.clone()), long_ago.clone());
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert!(key.rotation_due);
        assert_eq!(key.rotation_due_reason, Some(RotationDueReason::Age));

        // Never rotated, and in use longer than the threshold. Measured from
        // creation, and distinguished from a stale rotation so an operator can
        // tell "overdue again" from "never once".
        let mut key = aged_key(None, long_ago.clone());
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert!(key.rotation_due);
        assert_eq!(key.rotation_due_reason, Some(RotationDueReason::NeverRotated));

        // Rotated within the threshold, and created long ago: recency wins.
        let mut key = aged_key(Some(recently), long_ago);
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert!(!key.rotation_due);
        assert_eq!(key.rotation_due_reason, None);

        // A timestamp from a node running ahead must not read as an enormous
        // age and report a fresh key as overdue.
        let ahead = &now + jiff::Span::new().days(2);
        let mut key = aged_key(Some(ahead.clone()), ahead);
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert!(!key.rotation_due, "clock skew must not manufacture an overdue key");
    }

    /// The wrap-budget half of the verdict: the cryptographic bound, checked
    /// independently of the age policy and reported under its own reason.
    #[test]
    fn rotation_readiness_reports_an_exhausted_wrap_budget() {
        let now = Zoned::now();
        let recently = &now - jiff::Span::new().hours(1);
        let long_ago = &now - jiff::Span::new().days(400);
        let day = Duration::from_secs(86_400);
        let budget = 2_000_000;

        let with_wraps = |manager: &KmsManager, wraps: Option<u64>, rotated_at: Option<Zoned>| {
            let mut key = aged_key(rotated_at, recently.clone());
            key.wrap_budget_reserved = wraps;
            manager.apply_rotation_readiness(&mut key, true, &now);
            (key.rotation_due, key.rotation_due_reason)
        };

        // Budget configured and exceeded on a freshly rotated key: due, and the
        // reason names the wrap budget rather than an age nobody crossed.
        let manager = readiness_manager_with(Some(day), Some(budget));
        assert_eq!(
            with_wraps(&manager, Some(budget), Some(recently.clone())),
            (true, Some(RotationDueReason::Wraps))
        );
        // At the threshold exactly, not only past it: the bound is a ceiling.
        assert_eq!(
            with_wraps(&manager, Some(budget + 1), Some(recently.clone())),
            (true, Some(RotationDueReason::Wraps))
        );
        // Under the threshold: no verdict from the wrap half.
        assert_eq!(with_wraps(&manager, Some(budget - 1), Some(recently.clone())), (false, None));

        // The cryptographic bound outranks the policy one when both are crossed.
        let mut key = aged_key(Some(long_ago.clone()), long_ago);
        key.wrap_budget_reserved = Some(budget);
        manager.apply_rotation_readiness(&mut key, true, &now);
        assert_eq!(key.rotation_due_reason, Some(RotationDueReason::Wraps));

        // No wrap threshold configured: an enormous count reports nothing, the
        // same way an unset age threshold does.
        let age_only = readiness_manager_with(Some(day), None);
        assert_eq!(with_wraps(&age_only, Some(u64::MAX), Some(recently.clone())), (false, None));

        // Backend reports no count (Transit, AWS, or a pre-accounting record):
        // the wrap half stays silent instead of guessing, and the age half
        // still decides.
        let wraps_only = readiness_manager_with(None, Some(budget));
        assert_eq!(with_wraps(&wraps_only, None, Some(recently.clone())), (false, None));
        assert_eq!(
            with_wraps(&wraps_only, Some(budget), Some(recently.clone())),
            (true, Some(RotationDueReason::Wraps))
        );

        // A backend that cannot rotate is never told to, whatever it wrapped.
        let mut key = aged_key(None, recently);
        key.wrap_budget_reserved = Some(u64::MAX);
        wraps_only.apply_rotation_readiness(&mut key, false, &now);
        assert!(!key.rotation_due);
        assert_eq!(key.rotation_due_reason, Some(RotationDueReason::Unsupported));
    }

    /// Threshold parsing matches the age threshold's discipline: unset and
    /// unparsable both disable the signal rather than inventing a policy.
    #[test]
    fn rotation_wrap_threshold_parsing_refuses_to_guess() {
        assert_eq!(parse_rotation_max_wraps(None), None);
        assert_eq!(parse_rotation_max_wraps(Some("not-a-number")), None);
        assert_eq!(parse_rotation_max_wraps(Some("")), None);
        assert_eq!(parse_rotation_max_wraps(Some("-1")), None);
        assert_eq!(parse_rotation_max_wraps(Some("0")), None);
        // Clamped: below one reservation block the first reservation would trip it.
        assert_eq!(parse_rotation_max_wraps(Some("1")), Some(MIN_ROTATION_MAX_WRAPS));
        assert_eq!(
            parse_rotation_max_wraps(Some(" 5000000 ")),
            Some(5_000_000),
            "a configured budget above the floor is honored verbatim"
        );
    }

    /// The two fields are additive on the wire: a payload written before they
    /// existed still deserializes, and a key with no verdict serializes exactly
    /// as it did before.
    #[test]
    fn rotation_readiness_fields_are_additive_on_the_wire() {
        let legacy = serde_json::json!({
            "key_id": "key-a",
            "description": null,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "version": 1,
            "metadata": {},
            "tags": {},
            "created_at": "2026-01-01T00:00:00Z[UTC]",
            "rotated_at": null,
            "created_by": null,
        });
        let decoded: KeyInfo = serde_json::from_value(legacy).expect("a payload without the fields must still decode");
        assert!(!decoded.rotation_due);
        assert_eq!(decoded.rotation_due_reason, None);

        let encoded = serde_json::to_value(&decoded).expect("encode");
        assert_eq!(encoded.get("rotation_due"), Some(&serde_json::Value::Bool(false)));
        assert!(
            encoded.get("rotation_due_reason").is_none(),
            "an absent verdict must not add a field for old consumers to trip over"
        );

        let mut due = decoded;
        due.rotation_due = true;
        due.rotation_due_reason = Some(RotationDueReason::NeverRotated);
        let encoded = serde_json::to_value(&due).expect("encode");
        assert_eq!(encoded.get("rotation_due_reason").and_then(|value| value.as_str()), Some("never_rotated"));
    }
}
