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

//! The data-key provider interface and the per-request choice between the KMS and the local master key.

use crate::error::SseError;
use crate::host::SseHost;
use crate::kms::kms_backed::KmsSseDekProvider;
use crate::kms::local::LocalSseDekProvider;
use async_trait::async_trait;
use rustfs_kms::DataKey;
use rustfs_kms::types::ObjectEncryptionContext;
use std::sync::{Arc, LazyLock, RwLock};
use tracing::debug;

/// Trait for SSE data encryption key management
/// Abstracts the source of encryption keys (KMS, test provider, etc.)
#[async_trait]
pub trait SseDekProvider: Send + Sync {
    /// Generate an SSE data encryption key
    async fn generate_sse_dek(&self, context: &ObjectEncryptionContext, kms_key_id: &str)
    -> Result<(DataKey, Vec<u8>), SseError>;

    /// Decrypt an SSE data encryption key (returns only plaintext key, nonce should be read from metadata)
    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError>;

    /// Whether this provider may own a stored data key that is not a RustFS
    /// envelope — see `KmsBackend::reads_bare_data_key_ciphertext`.
    async fn reads_bare_data_key_ciphertext(&self) -> bool {
        false
    }

    /// Re-wrap a KMS-wrapped DEK envelope onto its master key's current
    /// version without exposing the plaintext DEK to the caller.
    ///
    /// Defaults to refusing: only the KMS-backed provider can rewrap, and a
    /// provider that cannot must say so rather than hand back the input as if
    /// it had been re-protected.
    async fn rewrap_sse_dek(
        &self,
        _encrypted_dek: &[u8],
        _context: &ObjectEncryptionContext,
    ) -> Result<rustfs_kms::types::RewrapDataKeyResponse, SseError> {
        Err(SseError::internal("This DEK provider cannot rewrap KMS-wrapped data keys"))
    }

    /// Whether this provider wraps data keys with a node-local master key
    /// instead of a KMS service.
    ///
    /// SSE-KMS must never be served by such a provider: the stored object would
    /// claim `aws:kms` and name a KMS key id that never wrapped anything.
    /// Defaults to false so only the local fallback has to declare itself.
    fn wraps_dek_with_local_master_key(&self) -> bool {
        false
    }

    /// Decrypt a DEK from positively identified legacy managed metadata.
    #[cfg(feature = "rio-v2")]
    async fn decrypt_legacy_sse_dek(
        &self,
        encrypted_dek: &[u8],
        kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        self.decrypt_sse_dek(encrypted_dek, kms_key_id, context).await
    }

    /// Unwrap a data key that MinIO's builtin KMS sealed.
    ///
    /// A separate entry point rather than a shape sniff inside
    /// [`Self::decrypt_sse_dek`]: the caller already knows the object carries a
    /// MinIO sealed key, and MinIO's raw ciphertext is unstructured bytes that
    /// no parser can reliably tell apart from anything else. Routing on the
    /// caller's knowledge keeps a RustFS envelope from ever reaching MinIO's
    /// decoder, and vice versa.
    ///
    /// Defaults to refusing: only a provider holding the MinIO master secret
    /// can serve these, and a provider that cannot must fail rather than fall
    /// back to a decoder that would misread the bytes.
    #[cfg(feature = "rio-v2")]
    async fn decrypt_minio_sse_dek(
        &self,
        _encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        Err(SseError::internal(
            "This KMS provider cannot unwrap a data key sealed by MinIO's builtin KMS",
        ))
    }
}

/// Unwrap the data key of an object that carries a sealed object key but no
/// RustFS envelope.
///
/// Two writers produce that shape, and nothing in the bytes tells them apart:
/// MinIO, whose data key is sealed under the static master key, and a RustFS
/// `rio-v2` build on a backend that persisted its native ciphertext bare.
/// MinIO's sealing is authenticated, so opening it under the local master key
/// is a proof rather than a guess: the bare blob is offered to the KMS only
/// once that has failed, or when no local master key exists to try. Without a
/// backend that reads bare ciphertext the MinIO route is the only one, exactly
/// as before.
#[cfg(feature = "rio-v2")]
pub(crate) async fn decrypt_unenveloped_sealed_dek(
    bare_kms: Option<Arc<dyn SseDekProvider + '_>>,
    local: Result<Arc<dyn SseDekProvider>, SseError>,
    encrypted_dek: &[u8],
    kms_key_id: &str,
    context: &ObjectEncryptionContext,
) -> Result<[u8; 32], SseError> {
    let Some(kms) = bare_kms else {
        return local?.decrypt_minio_sse_dek(encrypted_dek, kms_key_id, context).await;
    };
    if let Ok(local) = local
        && let Ok(data_key) = local.decrypt_minio_sse_dek(encrypted_dek, kms_key_id, context).await
    {
        return Ok(data_key);
    }
    kms.decrypt_sse_dek(encrypted_dek, kms_key_id, context).await
}

/// The provider that unwraps KMS-wrapped data keys.
///
/// A provider registered through `set_sse_dek_provider_for_test` stands in for
/// the fresh [`KmsSseDekProvider`], which cannot resolve a KMS service from a
/// host that has none. Only `GLOBAL_KMS_DEK_PROVIDER` is consulted, never
/// `GLOBAL_SSE_DEK_PROVIDER`, so a local provider cached by an earlier
/// `get_local_sse_dek_provider` call cannot be selected to unwrap a KMS envelope.
pub(crate) async fn kms_sse_dek_provider<'a>(host: &'a dyn SseHost) -> Result<Arc<dyn SseDekProvider + 'a>, SseError> {
    if let Some(cached) = GLOBAL_KMS_DEK_PROVIDER.read().ok().and_then(|guard| guard.as_ref().cloned()) {
        return Ok(cached);
    }
    Ok(Arc::new(KmsSseDekProvider::new(host).await?))
}

/// The KMS provider, when its backend may own a stored data key that is not a
/// RustFS envelope. `None` without a running KMS, which leaves such a key to
/// the local provider exactly as before.
pub(crate) async fn bare_data_key_kms_provider<'a>(host: &'a dyn SseHost) -> Option<Arc<dyn SseDekProvider + 'a>> {
    let provider = kms_sse_dek_provider(host).await.ok()?;
    provider.reads_bare_data_key_ciphertext().await.then_some(provider)
}

/// Global SSE DEK provider cache for local / test providers.
///
/// Populated by `get_local_sse_dek_provider` and (for backward-compat in tests)
/// `set_sse_dek_provider_for_test`.  Read by `get_local_sse_dek_provider` only —
/// the KMS-envelope decrypt path uses `GLOBAL_KMS_DEK_PROVIDER` so that a cached
/// local provider can never be selected for a KMS-wrapped data-key.
pub(crate) static GLOBAL_SSE_DEK_PROVIDER: LazyLock<RwLock<Option<Arc<dyn SseDekProvider>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Global KMS DEK provider cache for test-injected KMS providers.
///
/// Populated **only** by `set_sse_dek_provider_for_test` (test-only).
/// Read **only** by the KMS-envelope branch of `apply_managed_decryption_material`.
/// Separation from `GLOBAL_SSE_DEK_PROVIDER` prevents a previously-cached local
/// provider from being selected to unwrap a KMS data-key envelope.
static GLOBAL_KMS_DEK_PROVIDER: LazyLock<RwLock<Option<Arc<dyn SseDekProvider>>>> = LazyLock::new(|| RwLock::new(None));

/// Get or initialize the global SSE DEK provider
///
/// Factory function that automatically selects the appropriate provider:
/// - If `__RUSTFS_SSE_SIMPLE_CMK` environment variable exists: use SimpleSseDekProvider (test mode)
/// - Otherwise: use KmsSseDekProvider (production mode with real KMS)
///
/// # Returns
/// Arc to the global SSE DEK provider instance
///
/// # Example
/// ```rust,ignore
/// let provider = get_sse_dek_provider(host).await?;
/// let (data_key, encrypted_dek) = provider
///     .generate_sse_dek("bucket", "key", "kms-key-id")
///     .await?;
/// ```
pub(crate) async fn get_sse_dek_provider<'a>(host: &'a dyn SseHost) -> Result<Arc<dyn SseDekProvider + 'a>, SseError> {
    if host.encryption_service().await.is_some() {
        debug!("Using KmsSseDekProvider (KMS configured)");
        return Ok(Arc::new(KmsSseDekProvider::new(host).await?));
    }

    get_local_sse_dek_provider()
}

/// Get the local data-key provider, loading and caching it on first use.
///
/// Startup calls this to check the local master key ahead of traffic; a request
/// that arrives without that step loads it the same way and gets the same error.
pub(crate) fn get_local_sse_dek_provider() -> Result<Arc<dyn SseDekProvider>, SseError> {
    // An explicitly injected test provider overrides both the cache and the
    // environment-based selection below.
    #[cfg(any(test, feature = "test-util"))]
    if let Some(injected) = test_injected_sse_dek_provider() {
        return Ok(injected);
    }

    // Check if already initialized
    if let Some(provider) = GLOBAL_SSE_DEK_PROVIDER
        .read()
        .map_err(|_| SseError::internal("Failed to read global SSE DEK provider cache"))?
        .as_ref()
        .cloned()
    {
        return Ok(provider);
    }

    // In test mode, prefer the simple CMK provider when the env var is set.
    #[cfg(any(test, feature = "test-util"))]
    {
        if std::env::var("__RUSTFS_SSE_SIMPLE_CMK").is_ok() {
            debug!("Using LocalSseDekProvider (test mode) based on __RUSTFS_SSE_SIMPLE_CMK");
            let provider: Arc<dyn SseDekProvider> = Arc::new(LocalSseDekProvider::new()?);
            let mut slot = GLOBAL_SSE_DEK_PROVIDER
                .write()
                .map_err(|_| SseError::internal("Failed to update global SSE DEK provider cache"))?;
            if let Some(existing) = slot.as_ref() {
                return Ok(existing.clone());
            }
            *slot = Some(provider.clone());
            return Ok(provider);
        }
    }

    // Production fallback: local SSE-S3 provider (no KMS configured).
    debug!("Using local SSE-S3 provider (KMS not configured)");
    let provider: Arc<dyn SseDekProvider> = Arc::new(LocalSseDekProvider::new_from_env()?);

    let mut slot = GLOBAL_SSE_DEK_PROVIDER
        .write()
        .map_err(|_| SseError::internal("Failed to update global SSE DEK provider cache"))?;
    if let Some(existing) = slot.as_ref() {
        return Ok(existing.clone());
    }
    *slot = Some(provider.clone());

    Ok(provider)
}

/// Reset both global SSE DEK provider caches (for testing only)
///
/// Clears GLOBAL_SSE_DEK_PROVIDER (local/test providers) and
/// GLOBAL_KMS_DEK_PROVIDER (test-injected KMS providers).
#[cfg(any(test, feature = "test-util"))]
pub fn reset_sse_dek_provider() {
    if let Ok(mut slot) = GLOBAL_SSE_DEK_PROVIDER.write() {
        *slot = None;
    }
    if let Ok(mut slot) = GLOBAL_KMS_DEK_PROVIDER.write() {
        *slot = None;
    }
}

#[cfg(any(test, feature = "test-util"))]
pub fn set_sse_dek_provider_for_test(provider: Arc<dyn SseDekProvider>) {
    if let Ok(mut slot) = GLOBAL_KMS_DEK_PROVIDER.write() {
        *slot = Some(provider.clone());
    }
    if let Ok(mut slot) = GLOBAL_SSE_DEK_PROVIDER.write() {
        *slot = Some(provider);
    }
}

/// Provider explicitly injected via `set_sse_dek_provider_for_test`, if any.
///
/// Reads `GLOBAL_KMS_DEK_PROVIDER` because that slot is populated *only* by the
/// test setter, so a hit here always means an explicit test injection.
#[cfg(any(test, feature = "test-util"))]
fn test_injected_sse_dek_provider() -> Option<Arc<dyn SseDekProvider>> {
    GLOBAL_KMS_DEK_PROVIDER.read().ok().and_then(|guard| guard.as_ref().cloned())
}
