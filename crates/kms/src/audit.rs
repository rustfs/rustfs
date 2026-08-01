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

//! Audit contract for KMS management operations.
//!
//! [`KmsManager`](crate::manager::KmsManager) builds a [`KmsAuditRecord`] for
//! every management operation it serves and hands it to the installed
//! [`KmsAuditSink`]. No sink is installed by default, so a deployment that
//! does not consume KMS audit records pays nothing beyond the `Option` check.
//!
//! The record is deliberately a KMS-side type rather than an audit-crate
//! entry: keeping the dependency edge out of this crate lets the server map
//! records onto its existing audit pipeline (and its delivery semantics)
//! without KMS having to know that pipeline exists.
//!
//! # Redaction
//!
//! A record has no field that can hold key material, and every value that
//! originates from a caller passes through [`redact_encryption_context`]
//! before it is stored. Adding a field here means re-checking that invariant.

use crate::error::KmsError;
use crate::types::OperationContext;
use rustfs_s3_types::EventName;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use uuid::Uuid;

/// Encryption-context keys that describe *where* an object lives rather than
/// anything secret about it. Their values are recorded verbatim; every other
/// key is reduced to a digest.
const ENCRYPTION_CONTEXT_ALLOWLIST: [&str; 5] = ["bucket", "object", "object_key", "algorithm", "sse_type"];

/// Prefix marking a value that was replaced by a digest of itself.
const DIGEST_PREFIX: &str = "sha256:";

/// Hex characters of the digest that are kept. Enough to correlate repeated
/// values across records without being reversible in practice.
const DIGEST_LEN: usize = 16;

/// A KMS management operation that produces an audit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KmsAuditOperation {
    /// Creation of a new master key.
    CreateKey,
    /// Metadata lookup for a single key.
    DescribeKey,
    /// Enumeration of keys.
    ListKeys,
    /// Scheduling a key for deletion after the pending window.
    ScheduleKeyDeletion,
    /// Cancelling a previously scheduled deletion.
    CancelKeyDeletion,
    /// Returning a disabled key to service.
    EnableKey,
    /// Taking a key out of service without destroying it.
    DisableKey,
    /// Rotating a key to a new version.
    RotateKey,
    /// Irreversible removal of key material once the pending window expired.
    DeleteKey,
}

impl KmsAuditOperation {
    /// Stable operation name for audit consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            KmsAuditOperation::CreateKey => "CreateKey",
            KmsAuditOperation::DescribeKey => "DescribeKey",
            KmsAuditOperation::ListKeys => "ListKeys",
            KmsAuditOperation::ScheduleKeyDeletion => "ScheduleKeyDeletion",
            KmsAuditOperation::CancelKeyDeletion => "CancelKeyDeletion",
            KmsAuditOperation::EnableKey => "EnableKey",
            KmsAuditOperation::DisableKey => "DisableKey",
            KmsAuditOperation::RotateKey => "RotateKey",
            KmsAuditOperation::DeleteKey => "DeleteKey",
        }
    }

    /// Notification/audit event name carried by records for this operation.
    pub fn event_name(&self) -> EventName {
        match self {
            KmsAuditOperation::CreateKey => EventName::KmsKeyCreated,
            KmsAuditOperation::DescribeKey | KmsAuditOperation::ListKeys => EventName::KmsKeyAccessed,
            KmsAuditOperation::ScheduleKeyDeletion => EventName::KmsKeyDeletionScheduled,
            KmsAuditOperation::CancelKeyDeletion => EventName::KmsKeyDeletionCancelled,
            KmsAuditOperation::EnableKey => EventName::KmsKeyEnabled,
            KmsAuditOperation::DisableKey => EventName::KmsKeyDisabled,
            KmsAuditOperation::RotateKey => EventName::KmsKeyRotated,
            KmsAuditOperation::DeleteKey => EventName::KmsKeyDeleted,
        }
    }
}

/// Whether the audited operation completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KmsAuditOutcome {
    /// The operation completed and its effect is durable.
    Success,
    /// The operation failed; `error_class` carries the reason category.
    Failure,
}

impl KmsAuditOutcome {
    /// Stable outcome name for audit consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            KmsAuditOutcome::Success => "success",
            KmsAuditOutcome::Failure => "failure",
        }
    }
}

/// Coarse, stable classification of a failed KMS operation.
///
/// Audit consumers alert on these, so the strings are a wire contract: add
/// new classes rather than renaming existing ones.
pub fn error_class(error: &KmsError) -> &'static str {
    match error {
        KmsError::ConfigurationError { .. } => "configuration",
        KmsError::KeyNotFound { .. } => "key_not_found",
        KmsError::InvalidKey { .. } => "invalid_key",
        KmsError::CryptographicError { .. } => "cryptographic",
        KmsError::BackendError { .. } => "backend",
        KmsError::AccessDenied { .. } => "access_denied",
        KmsError::KeyAlreadyExists { .. } => "key_already_exists",
        KmsError::InvalidOperation { .. } => "invalid_operation",
        KmsError::InternalError { .. } => "internal",
        KmsError::SerializationError { .. } => "serialization",
        KmsError::IoError { .. } => "io",
        KmsError::CacheError { .. } => "cache",
        KmsError::ValidationError { .. } => "validation",
        KmsError::UnsupportedAlgorithm { .. } => "unsupported_algorithm",
        KmsError::InvalidKeySize { .. } => "invalid_key_size",
        KmsError::ContextMismatch { .. } => "context_mismatch",
        KmsError::OperationTimedOut { .. } => "timeout",
        KmsError::OperationCancelled { .. } => "cancelled",
        KmsError::MaterialMissing { .. } => "material_missing",
        KmsError::MaterialCorrupt { .. } => "material_corrupt",
        KmsError::MaterialAuthenticationFailed { .. } => "material_authentication_failed",
        KmsError::UnsupportedFormatVersion { .. } => "unsupported_format_version",
        KmsError::KeyVersionNotFound { .. } => "key_version_not_found",
        KmsError::Backup(_) => "backup",
        KmsError::UnsupportedCapability { .. } => "unsupported_capability",
        KmsError::CredentialsUnavailable { .. } => "credentials_unavailable",
        KmsError::BaselineVersionLost { .. } => "baseline_version_lost",
    }
}

/// One audited KMS management operation.
///
/// Everything an audit consumer needs to answer "who did what to which key,
/// against which backend, and did it work" — and nothing that could carry key
/// material. See the module docs for the redaction rules.
///
/// Tenant attribution is intentionally absent: multi-tenancy is not modelled
/// yet, and an invented tenant value is worse than a missing one. It will
/// arrive as an entry in [`Self::context`] once tenancy lands.
#[derive(Debug, Clone)]
pub struct KmsAuditRecord {
    /// Correlates every record emitted for one logical request.
    pub operation_id: Uuid,
    /// The audited operation.
    pub operation: KmsAuditOperation,
    /// Event name published to audit consumers.
    pub event: EventName,
    /// Authenticated identity that requested the operation, or
    /// [`OperationContext::INTERNAL_PRINCIPAL`] for server-initiated work.
    pub principal: String,
    /// Client address, when the caller supplied one.
    pub source_ip: Option<String>,
    /// Client user agent, when the caller supplied one.
    pub user_agent: Option<String>,
    /// Key the operation acted on. `None` for operations that span keys, such
    /// as listing.
    pub key_id: Option<String>,
    /// Key version the operation resolved to, when the result identifies one.
    /// Operations on a key as a whole leave this unset.
    pub key_version: Option<u32>,
    /// Whether the operation succeeded.
    pub outcome: KmsAuditOutcome,
    /// Failure category; `None` on success. See [`error_class`].
    pub error_class: Option<&'static str>,
    /// Backend that served the operation, e.g. `local` or `vault-transit`.
    pub backend: &'static str,
    /// Wall-clock time spent in the audited operation.
    pub latency: Duration,
    /// Retries performed above the audit point. `None` means the number is
    /// not observable here: backend-internal retries are accounted for by the
    /// `rustfs_kms_backend_operation_attempts` metric instead of being
    /// duplicated (and possibly contradicted) in the audit trail.
    pub retry_count: Option<u32>,
    /// Server-supplied correlation values carried by the operation context.
    /// Also the reserved slot for tenant attribution.
    pub context: BTreeMap<String, String>,
    /// Caller-supplied encryption context, after [`redact_encryption_context`].
    pub encryption_context: BTreeMap<String, String>,
}

impl KmsAuditRecord {
    /// Start a record for `operation` performed under `context`.
    ///
    /// The outcome defaults to failure so that a record which somehow escapes
    /// without [`Self::with_result`] under-reports success rather than
    /// inventing it.
    pub fn new(operation: KmsAuditOperation, context: &OperationContext, backend: &'static str) -> Self {
        Self {
            operation_id: context.operation_id,
            operation,
            event: operation.event_name(),
            principal: context.principal.clone(),
            source_ip: context.source_ip.clone(),
            user_agent: context.user_agent.clone(),
            key_id: None,
            key_version: None,
            outcome: KmsAuditOutcome::Failure,
            error_class: None,
            backend,
            latency: Duration::ZERO,
            retry_count: None,
            context: context
                .additional_context
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            encryption_context: BTreeMap::new(),
        }
    }

    /// Attach the key the operation acted on.
    pub fn with_key_id(mut self, key_id: Option<impl Into<String>>) -> Self {
        self.key_id = key_id.map(Into::into);
        self
    }

    /// Attach the key version the operation resolved to.
    pub fn with_key_version(mut self, key_version: Option<u32>) -> Self {
        self.key_version = key_version;
        self
    }

    /// Attach the measured duration of the operation.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// Derive outcome and error class from the operation result.
    pub fn with_result<T>(mut self, result: &crate::error::Result<T>) -> Self {
        match result {
            Ok(_) => {
                self.outcome = KmsAuditOutcome::Success;
                self.error_class = None;
            }
            Err(error) => {
                self.outcome = KmsAuditOutcome::Failure;
                self.error_class = Some(error_class(error));
            }
        }
        self
    }

    /// Attach the caller-supplied encryption context, redacted.
    ///
    /// Redaction happens here rather than at the call site so no caller can
    /// place a raw context value into a record by accident.
    pub fn with_encryption_context(mut self, encryption_context: &HashMap<String, String>) -> Self {
        self.encryption_context = redact_encryption_context(encryption_context);
        self
    }
}

/// Reduce a caller-supplied encryption context to something safe to persist.
///
/// Keys naming the object's location are kept verbatim because they are the
/// reason the context is audited at all. Every other value is replaced by a
/// truncated digest, which still correlates repeated values across records
/// but does not reproduce whatever the caller put there.
pub fn redact_encryption_context(encryption_context: &HashMap<String, String>) -> BTreeMap<String, String> {
    encryption_context
        .iter()
        .map(|(key, value)| {
            let recorded = if ENCRYPTION_CONTEXT_ALLOWLIST.contains(&key.as_str()) {
                value.clone()
            } else {
                digest_value(value)
            };
            (key.clone(), recorded)
        })
        .collect()
}

fn digest_value(value: &str) -> String {
    let digest = hex::encode(Sha256::digest(value.as_bytes()));
    format!("{DIGEST_PREFIX}{}", &digest[..DIGEST_LEN])
}

/// Receives KMS audit records.
///
/// Implementations must not block: they are called on the task that served
/// the KMS operation. Delivery failures are the sink's problem — the KMS
/// operation has already completed by the time a record is emitted, and its
/// result is never changed by what the sink does.
pub trait KmsAuditSink: Send + Sync {
    /// Handle one audit record.
    fn emit(&self, record: KmsAuditRecord);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_operation_maps_to_a_kms_event() {
        let operations = [
            KmsAuditOperation::CreateKey,
            KmsAuditOperation::DescribeKey,
            KmsAuditOperation::ListKeys,
            KmsAuditOperation::ScheduleKeyDeletion,
            KmsAuditOperation::CancelKeyDeletion,
            KmsAuditOperation::EnableKey,
            KmsAuditOperation::DisableKey,
            KmsAuditOperation::RotateKey,
            KmsAuditOperation::DeleteKey,
        ];

        for operation in operations {
            let event = operation.event_name();
            assert!(event.is_kms(), "{} must map to a KMS event, got {event}", operation.as_str());
        }
    }

    #[test]
    fn record_defaults_to_failure_until_a_result_is_attached() {
        let context = OperationContext::new("tester".to_string());
        let record = KmsAuditRecord::new(KmsAuditOperation::CreateKey, &context, "local");

        assert_eq!(record.outcome, KmsAuditOutcome::Failure);
        assert!(record.error_class.is_none());

        let ok: crate::error::Result<()> = Ok(());
        assert_eq!(record.clone().with_result(&ok).outcome, KmsAuditOutcome::Success);

        let denied: crate::error::Result<()> = Err(KmsError::access_denied("nope"));
        let failed = record.with_result(&denied);
        assert_eq!(failed.outcome, KmsAuditOutcome::Failure);
        assert_eq!(failed.error_class, Some("access_denied"));
    }

    #[test]
    fn record_carries_the_full_operation_context() {
        let context = OperationContext::new("arn:aws:iam::user/alice".to_string())
            .with_source_ip("10.0.0.7".to_string())
            .with_user_agent("aws-cli/2".to_string())
            .with_context("requestID".to_string(), "req-1".to_string());

        let record = KmsAuditRecord::new(KmsAuditOperation::RotateKey, &context, "vault-transit")
            .with_key_id(Some("key-1"))
            .with_key_version(Some(3))
            .with_latency(Duration::from_millis(12));

        assert_eq!(record.operation_id, context.operation_id);
        assert_eq!(record.principal, "arn:aws:iam::user/alice");
        assert_eq!(record.source_ip.as_deref(), Some("10.0.0.7"));
        assert_eq!(record.user_agent.as_deref(), Some("aws-cli/2"));
        assert_eq!(record.key_id.as_deref(), Some("key-1"));
        assert_eq!(record.key_version, Some(3));
        assert_eq!(record.backend, "vault-transit");
        assert_eq!(record.latency, Duration::from_millis(12));
        assert_eq!(record.event, EventName::KmsKeyRotated);
        assert_eq!(record.context.get("requestID").map(String::as_str), Some("req-1"));
    }

    #[test]
    fn encryption_context_keeps_only_allowlisted_values_verbatim() {
        let secret = "eyJhbGciOiJIUzI1NiJ9.super-secret-grant-token";
        let context = HashMap::from([
            ("bucket".to_string(), "photos".to_string()),
            ("object_key".to_string(), "2026/cat.png".to_string()),
            ("algorithm".to_string(), "AES256".to_string()),
            ("grant_token".to_string(), secret.to_string()),
            ("customer_reference".to_string(), "acct-4711".to_string()),
        ]);

        let redacted = redact_encryption_context(&context);

        assert_eq!(redacted.get("bucket").map(String::as_str), Some("photos"));
        assert_eq!(redacted.get("object_key").map(String::as_str), Some("2026/cat.png"));
        assert_eq!(redacted.get("algorithm").map(String::as_str), Some("AES256"));

        for key in ["grant_token", "customer_reference"] {
            let value = redacted.get(key).expect("non-allowlisted key should be kept as a digest");
            assert!(value.starts_with(DIGEST_PREFIX), "{key} should be digested, got {value}");
        }

        let rendered = format!("{redacted:?}");
        assert!(!rendered.contains(secret), "redacted context must not reproduce the raw value");
        assert!(!rendered.contains("acct-4711"), "redacted context must not reproduce the raw value");
    }

    #[test]
    fn identical_values_digest_identically_across_records() {
        // Correlation is the reason a digest is preferable to a fixed
        // placeholder; assert it actually holds.
        let first = redact_encryption_context(&HashMap::from([("tenant".to_string(), "alpha".to_string())]));
        let second = redact_encryption_context(&HashMap::from([("tenant".to_string(), "alpha".to_string())]));
        let other = redact_encryption_context(&HashMap::from([("tenant".to_string(), "beta".to_string())]));

        assert_eq!(first.get("tenant"), second.get("tenant"));
        assert_ne!(first.get("tenant"), other.get("tenant"));
    }
}
