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

//! Audit adapter for the KMS admin API.
//!
//! Maps the KMS-side audit contract onto the server's existing audit pipeline:
//! a [`KmsAuditRecord`] becomes an [`AuditEntry`] and travels to the same
//! targets as S3 audit entries, so KMS management activity lands in whatever
//! SIEM a deployment already operates instead of a KMS-private channel.
//!
//! Two emission paths meet here:
//!
//! * [`KmsAdminAuditSink`] converts the records [`rustfs_kms::KmsManager`]
//!   builds for the operations it serves, covering their success and failure
//!   outcomes.
//! * The handlers emit directly for what the KMS layer never sees: a request
//!   rejected by the authorization gate, and the endpoints that have no
//!   context-aware KMS entry point (data-key derivation and service control).
//!
//! # Redaction
//!
//! No field copied into an entry can hold key material. The recorded failure
//! reason is the [`error_class`] vocabulary, never the error message, so a
//! backend that echoes request data into an error cannot leak it here; the
//! caller-supplied encryption context arrives already reduced by
//! [`rustfs_kms::redact_encryption_context`].

use crate::server::RemoteAddr;
use crate::storage::access::request_context_from_extensions;
use crate::storage::helper::spawn_background_with_context;
use hashbrown::HashMap;
use rustfs_audit::entity::{ApiDetailsBuilder, AuditEntry, AuditEntryBuilder};
use rustfs_audit::global::AuditLogger;
use rustfs_credentials::Credentials;
use rustfs_kms::audit::error_class;
use rustfs_kms::types::OperationContext;
use rustfs_kms::{KmsAuditOperation, KmsAuditOutcome, KmsAuditRecord, KmsAuditSink, KmsError};
use rustfs_s3_types::EventName;
use rustfs_targets::get_request_user_agent;
use s3s::S3Result;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Audit entry schema version, shared with the S3 request path so a consumer
/// parses KMS entries with the parser it already has.
const AUDIT_ENTRY_VERSION: &str = "1.0";

/// `trigger` value marking an entry as produced by the KMS admin API.
const AUDIT_TRIGGER: &str = "kms-admin";

/// `type` value letting a consumer separate KMS entries from S3 ones without
/// enumerating event names.
const AUDIT_ENTRY_TYPE: &str = "kms";

/// Shared empty context for the emission paths that carry none.
static EMPTY_ENCRYPTION_CONTEXT: BTreeMap<String, String> = BTreeMap::new();

/// [`OperationContext::additional_context`] key carrying the canonical request
/// id, promoted to [`AuditEntry::request_id`] by the conversion below.
const CONTEXT_REQUEST_ID: &str = "requestID";

/// [`OperationContext::additional_context`] key carrying the account a
/// temporary or service credential belongs to.
const CONTEXT_PARENT_USER: &str = "parentUser";

/// Failure class recorded when the authorization gate rejects a request. The
/// gate answers before any KMS error exists, so the class is named here rather
/// than derived from one.
const ERROR_CLASS_ACCESS_DENIED: &str = "access_denied";

/// KMS admin operations audited by the handler itself.
///
/// The KMS manager audits everything it serves, but two groups never reach it
/// with a caller context: data-key derivation goes through
/// `ObjectEncryptionService`, which has no context-aware entry point, and the
/// service-control endpoints act on the service rather than on a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KmsAdminOperation {
    /// Derivation of a data key from a master key.
    GenerateDataKey,
    /// Installation of a KMS configuration.
    Configure,
    /// Replacement of the running configuration.
    Reconfigure,
    /// Start or restart of the KMS service.
    Start,
    /// Stop of the KMS service.
    Stop,
}

impl KmsAdminOperation {
    /// Stable operation name for audit consumers.
    fn as_str(self) -> &'static str {
        match self {
            Self::GenerateDataKey => "GenerateDataKey",
            Self::Configure => "Configure",
            Self::Reconfigure => "Reconfigure",
            Self::Start => "Start",
            Self::Stop => "Stop",
        }
    }

    /// Event name published to audit consumers.
    fn event(self) -> EventName {
        match self {
            // Deriving a data key uses the master key without changing it,
            // which is what the access event already denotes.
            Self::GenerateDataKey => EventName::KmsKeyAccessed,
            Self::Configure | Self::Reconfigure => EventName::KmsServiceConfigured,
            Self::Start => EventName::KmsServiceStarted,
            Self::Stop => EventName::KmsServiceStopped,
        }
    }
}

/// Per-request audit state for a KMS admin endpoint.
///
/// Built once the caller is authenticated and before the authorization gate
/// runs, so a denial is attributable to the identity that was rejected.
pub(super) struct KmsAdminAudit {
    context: OperationContext,
    started: Instant,
}

impl KmsAdminAudit {
    /// Build the audit state for an authenticated admin request.
    ///
    /// Takes the request in pieces because every KMS handler moves
    /// `credentials` out of it before reaching this point.
    pub(super) fn from_request(extensions: &http::Extensions, headers: &http::HeaderMap, cred: &Credentials) -> Self {
        // The peer address the authorization gate was given, so the audit trail
        // and the policy decision agree on where the request came from.
        let source_ip = extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|opt| opt.map(|addr| addr.0.ip().to_string()));
        let request_id = request_context_from_extensions(extensions).map(|context| context.request_id);

        Self::new(
            &cred.access_key,
            &cred.parent_user,
            source_ip,
            Some(get_request_user_agent(headers)),
            request_id,
        )
    }

    /// Assemble the operation context from already-extracted request values.
    ///
    /// Separate from [`Self::from_request`] so the mapping onto an audit entry
    /// is testable without an HTTP request.
    fn new(
        access_key: &str,
        parent_user: &str,
        source_ip: Option<String>,
        user_agent: Option<String>,
        request_id: Option<String>,
    ) -> Self {
        let mut context = OperationContext::new(access_key.to_string());
        context.source_ip = source_ip;
        context.user_agent = user_agent.filter(|agent| !agent.is_empty());
        if let Some(request_id) = request_id.filter(|id| !id.is_empty()) {
            context = context.with_context(CONTEXT_REQUEST_ID.to_string(), request_id);
        }
        if !parent_user.is_empty() && parent_user != access_key {
            context = context.with_context(CONTEXT_PARENT_USER.to_string(), parent_user.to_string());
        }

        Self {
            context,
            started: Instant::now(),
        }
    }

    /// The context handed to the KMS manager's `*_with_context` methods, so the
    /// record it builds carries the same identity as a handler-side entry.
    pub(super) fn context(&self) -> &OperationContext {
        &self.context
    }

    /// Audit an authorization denial and propagate it unchanged.
    ///
    /// A denied request never reaches the KMS layer, so this is the only place
    /// that can record it. Wrapping the gate's result keeps the record and the
    /// rejection on the same path: there is no way to return the error without
    /// producing the entry.
    pub(super) fn gate<T>(&self, result: S3Result<T>, operation: KmsAuditOperation, key_id: Option<&str>) -> S3Result<T> {
        if result.is_err() {
            self.emit(operation.as_str(), operation.event_name(), key_id, Some(ERROR_CLASS_ACCESS_DENIED));
        }
        result
    }

    /// Audit an authorization denial for an operation the handler owns.
    pub(super) fn gate_admin<T>(&self, result: S3Result<T>, operation: KmsAdminOperation, key_id: Option<&str>) -> S3Result<T> {
        if result.is_err() {
            self.emit(operation.as_str(), operation.event(), key_id, Some(ERROR_CLASS_ACCESS_DENIED));
        }
        result
    }

    /// Audit the outcome of an operation the handler served itself.
    pub(super) fn finish(&self, operation: KmsAdminOperation, key_id: Option<&str>, error: Option<&KmsError>) {
        self.emit(operation.as_str(), operation.event(), key_id, error.map(error_class));
    }

    /// Audit the outcome of a handler-served operation whose failure is not a
    /// [`KmsError`], such as a service-control call rejected by the manager's
    /// own state machine.
    pub(super) fn finish_with_class(&self, operation: KmsAdminOperation, error_class: Option<&'static str>) {
        self.emit(operation.as_str(), operation.event(), None, error_class);
    }

    fn emit(&self, operation: &str, event: EventName, key_id: Option<&str>, error_class: Option<&str>) {
        dispatch(build_entry(KmsAuditFields {
            operation,
            event,
            context: &self.context,
            key_id,
            key_version: None,
            outcome: match error_class {
                Some(_) => KmsAuditOutcome::Failure,
                None => KmsAuditOutcome::Success,
            },
            error_class,
            backend: None,
            retry_count: None,
            latency: self.started.elapsed(),
            encryption_context: &EMPTY_ENCRYPTION_CONTEXT,
        }));
    }
}

/// Sends the KMS manager's audit records to the server's audit pipeline.
///
/// Installed once at KMS service assembly. Delivery follows the pipeline's
/// established best-effort semantics: the KMS operation has already completed
/// when a record arrives, and nothing here can change its result.
pub struct KmsAdminAuditSink;

impl KmsAuditSink for KmsAdminAuditSink {
    fn emit(&self, record: KmsAuditRecord) {
        dispatch(entry_from_record(&record));
    }
}

/// Convert a KMS audit record into an audit entry.
fn entry_from_record(record: &KmsAuditRecord) -> AuditEntry {
    build_entry(KmsAuditFields {
        operation: record.operation.as_str(),
        event: record.event,
        context: &operation_context_of(record),
        key_id: record.key_id.as_deref(),
        key_version: record.key_version,
        outcome: record.outcome,
        error_class: record.error_class,
        backend: Some(record.backend),
        retry_count: record.retry_count,
        latency: record.latency,
        encryption_context: &record.encryption_context,
    })
}

/// Rebuild the caller context a record was created from. The record flattens
/// it, so the shared entry builder gets it back in one shape.
fn operation_context_of(record: &KmsAuditRecord) -> OperationContext {
    let mut context = OperationContext::new(record.principal.clone());
    context.operation_id = record.operation_id;
    context.source_ip = record.source_ip.clone();
    context.user_agent = record.user_agent.clone();
    context.additional_context = record
        .context
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    context
}

/// Everything one audited KMS operation contributes to an entry.
///
/// A struct rather than an argument list because both emission paths fill the
/// same shape, and a positional mix-up between the operation name, the error
/// class and the backend would be invisible at the call site.
struct KmsAuditFields<'a> {
    operation: &'a str,
    event: EventName,
    context: &'a OperationContext,
    key_id: Option<&'a str>,
    key_version: Option<u32>,
    outcome: KmsAuditOutcome,
    error_class: Option<&'a str>,
    backend: Option<&'a str>,
    retry_count: Option<u32>,
    latency: Duration,
    encryption_context: &'a BTreeMap<String, String>,
}

fn build_entry(fields: KmsAuditFields<'_>) -> AuditEntry {
    let context = fields.context;
    let api = ApiDetailsBuilder::new()
        .name(fields.operation)
        .status(fields.outcome.as_str())
        .time_to_response(format!("{:.2?}", fields.latency))
        .time_to_response_in_ns(fields.latency.as_nanos().to_string())
        .build();

    let mut builder = AuditEntryBuilder::new(AUDIT_ENTRY_VERSION, fields.event, AUDIT_TRIGGER, api)
        .entry_type(AUDIT_ENTRY_TYPE)
        .access_key(&context.principal)
        .tags(entry_tags(&fields));

    if let Some(request_id) = context.additional_context.get(CONTEXT_REQUEST_ID) {
        builder = builder.request_id(request_id);
    }
    if let Some(parent_user) = context.additional_context.get(CONTEXT_PARENT_USER) {
        builder = builder.parent_user(parent_user);
    }
    if let Some(source_ip) = context.source_ip.as_deref() {
        builder = builder.remote_host(source_ip);
    }
    if let Some(user_agent) = context.user_agent.as_deref() {
        builder = builder.user_agent(user_agent);
    }
    // The class, not the message: an error rendered by a backend can quote the
    // request that produced it, and an audit entry is a poor place to find out.
    if let Some(error_class) = fields.error_class {
        builder = builder.error(error_class);
    }

    builder.build()
}

fn entry_tags(fields: &KmsAuditFields<'_>) -> HashMap<String, Value> {
    let context = fields.context;
    let mut tags = HashMap::new();
    tags.insert("kmsOperation".to_string(), Value::String(fields.operation.to_string()));
    tags.insert("kmsOutcome".to_string(), Value::String(fields.outcome.as_str().to_string()));
    tags.insert("operationId".to_string(), Value::String(context.operation_id.to_string()));

    if let Some(key_id) = fields.key_id.filter(|value| !value.is_empty()) {
        tags.insert("keyId".to_string(), Value::String(key_id.to_string()));
    }
    if let Some(key_version) = fields.key_version {
        tags.insert("keyVersion".to_string(), Value::Number(key_version.into()));
    }
    if let Some(backend) = fields.backend {
        tags.insert("kmsBackend".to_string(), Value::String(backend.to_string()));
    }
    if let Some(retry_count) = fields.retry_count {
        tags.insert("retryCount".to_string(), Value::Number(retry_count.into()));
    }
    if !fields.encryption_context.is_empty() {
        let redacted = fields
            .encryption_context
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<serde_json::Map<_, _>>();
        tags.insert("encryptionContext".to_string(), Value::Object(redacted));
    }

    // Correlation values that are not promoted to a dedicated field stay
    // nested, so a future context key can never shadow a reserved tag.
    let extra = context
        .additional_context
        .iter()
        .filter(|(key, _)| key.as_str() != CONTEXT_REQUEST_ID && key.as_str() != CONTEXT_PARENT_USER)
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    if !extra.is_empty() {
        tags.insert("kmsContext".to_string(), Value::Object(extra));
    }

    tags
}

/// Hand a built entry to the audit pipeline.
///
/// Off the caller's task for the same reason the S3 path defers its entry: the
/// operation is finished, and audit delivery must not extend its latency. With
/// no audit system configured the dispatch is a no-op.
fn dispatch(entry: AuditEntry) {
    spawn_background_with_context(None, async move {
        AuditLogger::log(entry).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use rustfs_kms::backends::local::LocalKmsBackend;
    use rustfs_kms::config::KmsConfig;
    use rustfs_kms::types::{CreateKeyRequest, DescribeKeyRequest, GenerateDataKeyRequest, KeySpec};
    use rustfs_kms::{KmsAuditOperation, KmsManager};
    use std::sync::{Arc, Mutex};

    /// Captures the entries a real `KmsManager` produces, exercising the same
    /// sink the server installs.
    #[derive(Default)]
    struct CapturingSink {
        entries: Mutex<Vec<AuditEntry>>,
    }

    impl KmsAuditSink for CapturingSink {
        fn emit(&self, record: KmsAuditRecord) {
            self.entries
                .lock()
                .expect("capturing sink should not be poisoned")
                .push(entry_from_record(&record));
        }
    }

    impl CapturingSink {
        fn drain(&self) -> Vec<AuditEntry> {
            std::mem::take(&mut *self.entries.lock().expect("capturing sink should not be poisoned"))
        }
    }

    fn request_audit() -> KmsAdminAudit {
        KmsAdminAudit::new(
            "AKIAOPERATOR",
            "root-account",
            Some("10.1.2.3".to_string()),
            Some("rustfs-admin/1".to_string()),
            Some("req-kms-1".to_string()),
        )
    }

    fn tag<'a>(entry: &'a AuditEntry, name: &str) -> Option<&'a Value> {
        entry.tags.as_ref().and_then(|tags| tags.get(name))
    }

    async fn local_manager(temp_dir: &tempfile::TempDir, sink: Arc<CapturingSink>) -> KmsManager {
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = Arc::new(
            LocalKmsBackend::new(config.clone())
                .await
                .expect("local backend should build"),
        );
        KmsManager::new(backend, config).with_audit_sink(sink)
    }

    /// The identity, correlation id and outcome the KMS manager records must
    /// all survive the mapping onto an audit entry.
    #[tokio::test]
    async fn manager_records_become_attributable_audit_entries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let sink = Arc::new(CapturingSink::default());
        let manager = local_manager(&temp_dir, sink.clone()).await;
        let audit = request_audit();

        let key_id = manager
            .create_key_with_context(
                CreateKeyRequest {
                    key_name: Some("audited-key".to_string()),
                    ..Default::default()
                },
                audit.context(),
            )
            .await
            .expect("key should be created")
            .key_id;

        let entries = sink.drain();
        let entry = entries.first().expect("create should produce one entry");

        assert_eq!(entry.event, EventName::KmsKeyCreated);
        assert_eq!(entry.request_id.as_deref(), Some("req-kms-1"));
        assert_eq!(entry.access_key.as_deref(), Some("AKIAOPERATOR"));
        assert_eq!(entry.parent_user.as_deref(), Some("root-account"));
        assert_eq!(entry.remote_host.as_deref(), Some("10.1.2.3"));
        assert_eq!(entry.user_agent.as_deref(), Some("rustfs-admin/1"));
        assert_eq!(entry.api.status.as_deref(), Some("success"));
        assert_eq!(entry.error, None);
        assert_eq!(tag(entry, "keyId"), Some(&Value::String(key_id.clone())));
        assert_eq!(tag(entry, "kmsBackend"), Some(&Value::String("local".to_string())));

        // A failure must carry the class, and only the class.
        manager
            .describe_key_with_context(
                DescribeKeyRequest {
                    key_id: "no-such-key".to_string(),
                },
                audit.context(),
            )
            .await
            .expect_err("describe of a missing key should fail");

        let entries = sink.drain();
        let entry = entries.first().expect("describe should produce one entry");
        assert_eq!(entry.api.status.as_deref(), Some("failure"));
        assert_eq!(entry.error.as_deref(), Some("key_not_found"));
        assert_eq!(entry.request_id.as_deref(), Some("req-kms-1"));
    }

    /// A denied request never reaches the KMS layer, so the handler-side entry
    /// is the only record of it.
    #[test]
    fn a_denied_request_is_audited_with_the_identity_that_was_rejected() {
        let audit = request_audit();
        // The delete endpoint schedules deletion, which is the operation the
        // KMS manager audits for a served request; a denial must be recorded
        // under the same name so both outcomes of one endpoint correlate.
        let entry = build_entry(KmsAuditFields {
            operation: KmsAuditOperation::ScheduleKeyDeletion.as_str(),
            event: KmsAuditOperation::ScheduleKeyDeletion.event_name(),
            context: audit.context(),
            key_id: Some("key-denied"),
            key_version: None,
            outcome: KmsAuditOutcome::Failure,
            error_class: Some(ERROR_CLASS_ACCESS_DENIED),
            backend: None,
            retry_count: None,
            latency: Duration::from_millis(1),
            encryption_context: &EMPTY_ENCRYPTION_CONTEXT,
        });

        assert_eq!(entry.event, EventName::KmsKeyDeletionScheduled);
        assert_eq!(entry.error.as_deref(), Some("access_denied"));
        assert_eq!(entry.api.status.as_deref(), Some("failure"));
        assert_eq!(entry.access_key.as_deref(), Some("AKIAOPERATOR"));
        assert_eq!(entry.request_id.as_deref(), Some("req-kms-1"));
        assert_eq!(tag(&entry, "keyId"), Some(&Value::String("key-denied".to_string())));
    }

    /// Every handler-owned operation must map to a KMS event, or its entries
    /// would be indistinguishable from S3 traffic to a consumer filtering on
    /// the event name.
    #[test]
    fn handler_owned_operations_map_to_kms_events() {
        for operation in [
            KmsAdminOperation::GenerateDataKey,
            KmsAdminOperation::Configure,
            KmsAdminOperation::Reconfigure,
            KmsAdminOperation::Start,
            KmsAdminOperation::Stop,
        ] {
            assert!(
                operation.event().is_kms(),
                "{} must map to a KMS event, got {}",
                operation.as_str(),
                operation.event()
            );
        }
    }

    /// The data-key endpoint returns plaintext key material to its caller. The
    /// audit entry for the same operation must not reproduce any of it, in raw
    /// or base64 form.
    #[tokio::test]
    async fn a_data_key_audit_entry_carries_no_key_material() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let sink = Arc::new(CapturingSink::default());
        let manager = local_manager(&temp_dir, sink.clone()).await;
        let audit = request_audit();

        let key_id = manager
            .create_key_with_context(
                CreateKeyRequest {
                    key_name: Some("data-key-source".to_string()),
                    ..Default::default()
                },
                audit.context(),
            )
            .await
            .expect("key should be created")
            .key_id;

        let secret = "super-secret-grant-token";
        let response = manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: std::collections::HashMap::from([
                    ("bucket".to_string(), "photos".to_string()),
                    ("grant_token".to_string(), secret.to_string()),
                ]),
            })
            .await
            .expect("data key should be generated");

        // What the endpoint hands back, and therefore what must not reappear.
        let plaintext_b64 = base64::prelude::BASE64_STANDARD.encode(&response.plaintext_key);
        let ciphertext_b64 = base64::prelude::BASE64_STANDARD.encode(&response.ciphertext_blob);
        assert!(!response.plaintext_key.is_empty(), "the test must drive real key material");

        let redacted = rustfs_kms::redact_encryption_context(&std::collections::HashMap::from([
            ("bucket".to_string(), "photos".to_string()),
            ("grant_token".to_string(), secret.to_string()),
        ]));
        let entry = build_entry(KmsAuditFields {
            operation: KmsAdminOperation::GenerateDataKey.as_str(),
            event: KmsAdminOperation::GenerateDataKey.event(),
            context: audit.context(),
            key_id: Some(&key_id),
            key_version: None,
            outcome: KmsAuditOutcome::Success,
            error_class: None,
            backend: Some("local"),
            retry_count: None,
            latency: Duration::from_millis(1),
            encryption_context: &redacted,
        });

        let rendered = serde_json::to_string(&entry).expect("audit entry should serialize");
        for material in [plaintext_b64.as_str(), ciphertext_b64.as_str(), secret] {
            assert!(
                !rendered.contains(material),
                "audit entry must not reproduce key material or secret context: {rendered}"
            );
        }
        // Raw bytes cannot appear either, whatever encoding a future field uses.
        assert!(
            !rendered
                .as_bytes()
                .windows(response.plaintext_key.len())
                .any(|window| window == response.plaintext_key.as_slice()),
            "audit entry must not reproduce raw key material"
        );

        // The location keys stay readable, which is why the context is audited.
        let encryption_context = tag(&entry, "encryptionContext").expect("encryption context should be recorded");
        assert_eq!(encryption_context.get("bucket"), Some(&Value::String("photos".to_string())));
        assert!(
            encryption_context
                .get("grant_token")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("sha256:")),
            "a non-allowlisted context value must be digested"
        );
    }
}
