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

//! Per-request summary of data-plane KMS use, attached to the request's audit entry.

use crate::core::request::SseKmsPrincipal;
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_kms::is_data_key_envelope;
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::http::object_encryption_keys::{INTERNAL_ENCRYPTION_KEY_HEADER, SSEType, normalize_managed_metadata};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, Mutex, Weak};

/// Tag keys carrying the KMS summary on an S3 audit entry.
///
/// Audit consumers key off these strings, so they are a wire contract: append
/// new keys rather than renaming existing ones.
const KMS_AUDIT_TAG_SSE_TYPE: &str = "sseType";

const KMS_AUDIT_TAG_KEY_ID: &str = "kmsKeyId";

const KMS_AUDIT_TAG_KEY_VERSION: &str = "kmsKeyVersion";

const KMS_AUDIT_TAG_OUTCOME: &str = "kmsOutcome";

const KMS_AUDIT_TAG_ERROR_CLASS: &str = "kmsErrorClass";

/// Separator for the rare request that touched more than one key or scheme.
const KMS_AUDIT_VALUE_SEPARATOR: &str = ",";

/// What the data path did with KMS while serving one S3 request.
///
/// This rides on the request's existing S3 audit entry instead of becoming its
/// own event: the entry already carries the principal, request ID and API, so
/// attaching the KMS outcome costs no extra event volume and needs no second
/// correlation key.
///
/// # Redaction
///
/// Only the fields below may ever be recorded. The S3 audit entry fans out to
/// every configured target, several of which sit outside the KMS trust
/// boundary, so nothing derived from a data key — plaintext, ciphertext,
/// envelope, nonce — nor any caller-supplied encryption-context value belongs
/// here. Key identifiers and scheme names are configuration, not secrets.
#[derive(Debug, Default)]
struct KmsRequestAuditState {
    sse_types: BTreeSet<&'static str>,
    key_ids: BTreeSet<String>,
    key_versions: BTreeSet<u32>,
    /// Set by the first failure. A request that failed any KMS interaction is
    /// reported as failed even if others succeeded: a partially completed
    /// envelope operation is not a success for an audit reader.
    error_class: Option<&'static str>,
    recorded: bool,
}

/// Shared accumulator for one request's KMS summary.
///
/// Written by the managed-SSE code paths through the request's principal, read
/// once through [`KmsRequestAuditScope::audit_tags`] by whoever owns the
/// request's audit entry.
#[derive(Debug, Default)]
pub(crate) struct KmsRequestAudit(Mutex<KmsRequestAuditState>);

impl KmsRequestAudit {
    pub(crate) fn record(
        &self,
        sse_type: SSEType,
        key_id: Option<&str>,
        key_version: Option<u32>,
        error_class: Option<&'static str>,
    ) {
        // A poisoned lock costs the audit entry its KMS tags; it must never cost
        // the request its result.
        let Ok(mut state) = self.0.lock() else {
            return;
        };

        state.recorded = true;
        state.sse_types.insert(sse_type.audit_label());
        if let Some(key_id) = key_id.filter(|value| !value.is_empty()) {
            state.key_ids.insert(key_id.to_string());
        }
        if let Some(key_version) = key_version {
            state.key_versions.insert(key_version);
        }
        if state.error_class.is_none() {
            state.error_class = error_class;
        }
    }

    /// Render the summary as audit tags, or nothing when the request performed
    /// no KMS work at all (SSE-C and unencrypted objects never reach KMS).
    pub fn audit_tags(&self) -> Vec<(&'static str, Value)> {
        let Ok(state) = self.0.lock() else {
            return Vec::new();
        };
        if !state.recorded {
            return Vec::new();
        }

        let mut tags = Vec::with_capacity(5);
        tags.push((KMS_AUDIT_TAG_SSE_TYPE, join_audit_values(state.sse_types.iter().copied())));
        if !state.key_ids.is_empty() {
            tags.push((KMS_AUDIT_TAG_KEY_ID, join_audit_values(state.key_ids.iter().map(String::as_str))));
        }
        if !state.key_versions.is_empty() {
            tags.push((
                KMS_AUDIT_TAG_KEY_VERSION,
                join_audit_values(state.key_versions.iter().map(u32::to_string)),
            ));
        }
        let outcome = if state.error_class.is_some() { "failure" } else { "success" };
        tags.push((KMS_AUDIT_TAG_OUTCOME, Value::String(outcome.to_string())));
        if let Some(error_class) = state.error_class {
            tags.push((KMS_AUDIT_TAG_ERROR_CLASS, Value::String(error_class.to_string())));
        }

        tags
    }
}

/// Render a set of values as one tag value.
///
/// Always a string, including for the single-value case: a consumer that would
/// have to branch on the JSON type of a tag is a consumer that will get it
/// wrong for the request that happens to touch two keys.
fn join_audit_values(values: impl Iterator<Item = impl AsRef<str>>) -> Value {
    let joined = values.map(|value| value.as_ref().to_string()).collect::<Vec<_>>();
    Value::String(joined.join(KMS_AUDIT_VALUE_SEPARATOR))
}

/// Slots of the requests currently being audited, keyed by canonical request ID.
///
/// Only a weak reference is held: the owning [`KmsRequestAuditScope`] decides the
/// lifetime, so a scope that is somehow not dropped cannot keep a slot alive.
static KMS_REQUEST_AUDITS: LazyLock<Mutex<HashMap<String, Weak<KmsRequestAudit>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registration of one request's KMS audit slot, dropped with the request's
/// audit entry.
pub struct KmsRequestAuditScope {
    request_id: String,
    audit: Arc<KmsRequestAudit>,
}

impl KmsRequestAuditScope {
    /// Open a slot for `request_id`, so managed-SSE operations served under it can
    /// be summarised onto its audit entry.
    pub fn register(request_id: &str) -> Self {
        let audit = Arc::new(KmsRequestAudit::default());
        if let Ok(mut registry) = KMS_REQUEST_AUDITS.lock() {
            registry.insert(request_id.to_string(), Arc::downgrade(&audit));
        }

        Self {
            request_id: request_id.to_string(),
            audit,
        }
    }

    /// Tags describing the KMS work recorded under this request.
    pub fn audit_tags(&self) -> Vec<(&'static str, Value)> {
        self.audit.audit_tags()
    }
}

impl Drop for KmsRequestAuditScope {
    fn drop(&mut self) {
        if let Ok(mut registry) = KMS_REQUEST_AUDITS.lock() {
            registry.remove(&self.request_id);
        }
    }
}

/// Resolve the audit slot opened for `request_id`, if the request is being audited.
pub(crate) fn kms_request_audit(request_id: &str) -> Option<Arc<KmsRequestAudit>> {
    KMS_REQUEST_AUDITS.lock().ok()?.get(request_id)?.upgrade()
}

/// Failure classes for data-path failures that never reached a KMS backend.
///
/// Backend failures carry the classification defined by the KMS audit contract;
/// everything else is classified at the S3 boundary it surfaced through.
pub(crate) fn kms_data_plane_error_class(error: &SseError) -> &'static str {
    if let Some(failure) = error
        .source
        .as_ref()
        .and_then(|source| source.downcast_ref::<KmsDataPlaneFailure>())
    {
        return failure.class;
    }

    match error.code {
        S3ErrorCode::AccessDenied => "access_denied",
        S3ErrorCode::InvalidArgument | S3ErrorCode::InvalidRequest => "invalid_argument",
        _ => "sse_internal",
    }
}

/// Carries a KMS failure class across the conversion to an S3-facing error.
///
/// The class is decided while the `KmsError` is still typed; re-deriving it from
/// a rendered message downstream would be guesswork.
#[derive(Debug)]
pub(crate) struct KmsDataPlaneFailure {
    pub(crate) class: &'static str,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for KmsDataPlaneFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            Some(source) => source.fmt(formatter),
            None => formatter.write_str(self.class),
        }
    }
}

impl std::error::Error for KmsDataPlaneFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Record the outcome of one managed-SSE operation on the request's audit entry.
///
/// A `None` principal marks an internal caller — replication, lifecycle, heal —
/// which has no S3 audit entry to attach to. `key_version` is a closure for
/// exactly that caller: extracting the version means base64-decoding and
/// parsing the stored envelope, work that must not run on the internal hot
/// paths that discard it.
pub(crate) fn record_managed_kms_outcome(
    principal: Option<&SseKmsPrincipal>,
    sse_type: SSEType,
    key_id: Option<&str>,
    key_version: impl FnOnce() -> Option<u32>,
    result: Result<(), &SseError>,
) {
    let Some(audit) = principal.and_then(|principal| principal.request_audit.as_ref()) else {
        return;
    };

    audit.record(sse_type, key_id, key_version(), result.err().map(kms_data_plane_error_class));
}

/// Master-key version recorded in a managed-SSE data-key envelope, if the
/// wrapping backend recorded one.
///
/// `None` is the honest answer for every other shape: Transit and AWS keep
/// the version inside their own opaque ciphertext, Local records no version,
/// and pre-versioning envelopes never carried the field. The single field is
/// read through `serde_json::Value` rather than a full `DataKeyEnvelope`
/// parse, so the audit path cannot double-count the envelope's unknown-field
/// observability and touches nothing else in the envelope.
pub(crate) fn envelope_master_key_version(envelope_bytes: &[u8]) -> Option<u32> {
    if !is_data_key_envelope(envelope_bytes) {
        return None;
    }
    u32::try_from(
        serde_json::from_slice::<Value>(envelope_bytes)
            .ok()?
            .get("master_key_version")?
            .as_u64()?,
    )
    .ok()
}

/// Master-key version of the envelope stored on an object, for the audit
/// summary of a read against that object.
pub(crate) fn stored_envelope_master_key_version(metadata: &HashMap<String, String>) -> Option<u32> {
    // No context recoder: the recode only ever inserts the context key, which
    // this lookup never reads, so the normalized result is identical without it.
    let encoded = normalize_managed_metadata(metadata, None);
    let encoded = encoded.get(INTERNAL_ENCRYPTION_KEY_HEADER)?;
    let envelope = BASE64_STANDARD.decode_to_vec(encoded).ok()?;
    envelope_master_key_version(&envelope)
}
