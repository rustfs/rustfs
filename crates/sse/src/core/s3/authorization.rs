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

//! Per-key KMS authorization on the SSE-KMS data path.

use crate::core::audit::{record_managed_kms_outcome, stored_envelope_master_key_version};
use crate::core::request::SseKmsPrincipal;
use crate::error::SseError;
use crate::host::SseHost;
use crate::{LOG_COMPONENT_STORAGE, LOG_SUBSYSTEM_SSE};
use rustfs_config::ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY;
use rustfs_policy::policy::action::KmsAction;
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::http::object_encryption_keys::{SSEType, stored_managed_encryption_key};
use std::collections::HashMap;
use tracing::debug;

/// Ask the authorizer that decides for `principal`: the host's, unless a test overrode it.
async fn is_kms_key_allowed(
    host: &dyn SseHost,
    principal: &SseKmsPrincipal,
    action: KmsAction,
    key_id: &str,
) -> Result<bool, SseError> {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hooks) = principal.test_hooks.as_ref() {
        return hooks.authorizer.is_allowed(principal, action, key_id).await;
    }

    host.kms_key_authorizer().is_allowed(principal, action, key_id).await
}

/// Whether per-key KMS authorization is enforced on the SSE-KMS data path.
fn sse_kms_key_policy_enforced(principal: Option<&SseKmsPrincipal>) -> bool {
    #[cfg(any(test, feature = "test-util"))]
    if let Some(hooks) = principal.and_then(|candidate| candidate.test_hooks.as_ref()) {
        return hooks.enforced;
    }
    #[cfg(not(any(test, feature = "test-util")))]
    let _ = principal;

    crate::startup::key_policy_enforced()
}

/// The principal whose KMS key permissions this operation must satisfy, if any.
///
/// Scoping is limited to SSE-KMS, matching AWS: SSE-S3 wraps its data key with a
/// server-owned key the caller never names, and SSE-C never reaches KMS at all.
pub(crate) fn kms_authorization_subject(
    enforced: bool,
    principal: Option<&SseKmsPrincipal>,
    sse_type: SSEType,
) -> Option<&SseKmsPrincipal> {
    if !enforced || !matches!(sse_type, SSEType::SseKms) {
        return None;
    }

    principal
}

/// Authorize `action` against `key_id` for the caller behind this SSE operation.
///
/// Runs before the key is used, so a denied request cannot distinguish an unauthorized
/// key from a disabled or pending-deletion one.
pub(crate) async fn authorize_sse_kms_key(
    host: &dyn SseHost,
    principal: Option<&SseKmsPrincipal>,
    sse_type: SSEType,
    action: KmsAction,
    key_id: &str,
) -> Result<(), SseError> {
    let Some(principal) = kms_authorization_subject(sse_kms_key_policy_enforced(principal), principal, sse_type) else {
        return Ok(());
    };

    if is_kms_key_allowed(host, principal, action, key_id).await? {
        return Ok(());
    }

    debug!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "sse_kms_key_authorization_denied",
        account = %principal.account,
        action = ?action,
        "Principal is not authorized for the KMS key resolved for this request"
    );

    // One warn per process, not per request: anonymous denials are driven by
    // unauthenticated traffic, so a per-request warn would let anyone flood the
    // log. Per-request detail stays on the audit entry and the debug event above.
    if principal.account.is_empty() {
        static ANONYMOUS_DENIAL_WARNED: std::sync::Once = std::sync::Once::new();
        ANONYMOUS_DENIAL_WARNED.call_once(|| {
            tracing::warn!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SSE,
                event = "sse_kms_anonymous_key_authorization_denied",
                action = ?action,
                "Anonymous requests are being denied by SSE-KMS per-key authorization: anonymous \
                 callers hold no kms grants, so a public bucket serving SSE-KMS objects is \
                 incompatible with {ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY}=true. Reported once per \
                 process; per-request denials are on audit entries and at debug level."
            );
        });
    }

    Err(SseError {
        code: S3ErrorCode::AccessDenied,
        message: "Access Denied".to_string(),
        source: None,
    })
}

/// Authorize `kms:Decrypt` for an object whose stored encryption material is unwrapped
/// outside [`sse_decryption`].
///
/// The copy-source read resolves its material inside the object layer, which has no
/// request identity, so the S3 layer has to run the check itself.
pub async fn authorize_sse_kms_object_read(
    host: &dyn SseHost,
    principal: Option<&SseKmsPrincipal>,
    metadata: &HashMap<String, String>,
) -> Result<(), SseError> {
    let Some((sse_type, key_id)) = stored_managed_encryption_key(metadata) else {
        return Ok(());
    };

    let result = authorize_sse_kms_key(host, principal, sse_type, KmsAction::DecryptAction, &key_id).await;
    // Only a denial is recorded here: an allowed read goes on to unwrap the key,
    // and that operation reports its own outcome.
    if let Err(error) = &result {
        record_managed_kms_outcome(
            principal,
            sse_type,
            Some(&key_id),
            || stored_envelope_master_key_version(metadata),
            Err(error),
        );
    }

    result
}

/// Authorize the current request to write under the upload's stored KMS key.
/// Upload IDs do not confer the creator's KMS permissions on another caller.
pub async fn authorize_sse_kms_multipart_write(
    host: &dyn SseHost,
    principal: Option<&SseKmsPrincipal>,
    metadata: &HashMap<String, String>,
) -> Result<(), SseError> {
    let Some((sse_type, key_id)) = stored_managed_encryption_key(metadata) else {
        return Ok(());
    };
    let result = authorize_sse_kms_key(host, principal, sse_type, KmsAction::GenerateDataKeyAction, &key_id).await;
    if let Err(error) = &result {
        record_managed_kms_outcome(
            principal,
            sse_type,
            Some(&key_id),
            || stored_envelope_master_key_version(metadata),
            Err(error),
        );
    }
    result
}
