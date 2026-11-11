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

//! The gateway's [`AuthzAuditSink`], writing RustFS audit records.
//!
//! Responsible for: one [`AuthzAuditRecord`] per authorization decision — `Allow`, `Deny` and
//! `Indeterminate` alike, the decision spelled out in the `decision` tag — carrying the legacy
//! authorization-denial entry's field set as far as the gateway's [`AuthzAuditEvent`] can supply
//! it: `version`, `event`, `trigger`, `api.name`, `api.bucket`, `api.object`, `api.status`,
//! `api.status_code`, `requestID`, `accessKey`, `error` and `tags.denialReason`
//! ([`OWNED_CONTRACT_FIELDS`]). `accessKey` is the caller the gateway verified, and is absent for an
//! anonymous caller.
//! NOT responsible for: facts the event does not carry. The entry's `time` and its rate limiting
//! (`tags.suppressedDenials`) are the binary sink's, as are `parentUser` and `requestClaims`, which
//! are IAM facts the event names only by snapshot id. The peer and request-line fields
//! (`remotehost`, `userAgent`, `requestHost`, `requestPath`, `requestQuery`) are the outer layer's:
//! no hook carries a header, by design. Durations and bytes are the outer layer's too. The per-key
//! `tags.deniedKeys` of a bulk delete stays with handler code. The whole assignment is
//! `tests/fixtures/observability-contract.json`.
//! Upstream: the gateway service, once per decision. Downstream: the binary's
//! [`AuthzAuditRecordSink`], which builds a `rustfs_audit::AuditEntry` and hands it to the audit
//! logger.
//!
//! # What may not reach a record
//!
//! The record is built field by field from the event: no secret key, session token, signature or
//! SSE-C key exists in the event, and none of the event's other caller-chosen values — a version
//! id, a copy-source identity, a subject — is copied, because the legacy entry has no field for
//! them. Nothing is formatted from the event's `Debug`.

use rustfs_gateway::{AuthzAuditEvent, AuthzAuditSink, AuthzStage, Decision};
use rustfs_s3_types::EventName;

/// The `version` every legacy audit entry carries.
pub const AUDIT_VERSION: &str = "1.0";
/// The `error` of a refused decision, as the legacy denial entry writes it.
pub const ACCESS_DENIED_ERROR: &str = "Access Denied";
/// `tags.denialReason` of a `Deny`: the gateway's event carries the decision, not RustFS's
/// internal reason vocabulary.
pub const DENY_REASON: &str = "authorization_denied";
/// `tags.denialReason` of an `Indeterminate`: no policy reading could answer.
pub const INDETERMINATE_REASON: &str = "authorization_indeterminate";

/// Tag carrying the decision: `allow`, `deny` or `indeterminate`.
pub const DECISION_TAG: &str = "decision";
/// Tag carrying the stage that decided: `route` or `input`.
pub const STAGE_TAG: &str = "authorizationStage";
/// Tag carrying the gateway's operation name.
pub const OPERATION_TAG: &str = "operation";
/// Tag carrying the policy reading's snapshot id; absent when none could be read.
pub const POLICY_SNAPSHOT_TAG: &str = "policySnapshot";
/// The legacy denial-reason tag.
pub const DENIAL_REASON_TAG: &str = "denialReason";

/// The legacy authorization-denial entry fields (contract ids without their `authorization_denial.`
/// prefix) a refused decision's record supplies.
pub const OWNED_CONTRACT_FIELDS: [&str; 12] = [
    "version",
    "event",
    "trigger",
    "api.name",
    "api.status",
    "api.status_code",
    "api.bucket",
    "api.object",
    "requestID",
    "accessKey",
    "error",
    "tags.denialReason",
];

/// One authorization decision, in the shape of a RustFS audit entry.
///
/// Field names follow `rustfs_audit::AuditEntry` and its `ApiDetails`. Owned, so a sink can move it
/// onto a channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthzAuditRecord {
    /// `version`.
    pub version: &'static str,
    /// `event`: always [`EventName::AuditS3Operation`], as for legacy denial entries.
    pub event: EventName,
    /// `trigger`: the IAM action decided on, `s3:GetObject`.
    pub trigger: String,
    /// `api.name`: the same action.
    pub api_name: String,
    /// `api.bucket`, when the decision was about one.
    pub api_bucket: Option<String>,
    /// `api.object`, when the decision was about one.
    pub api_object: Option<String>,
    /// `api.status`: `failure` for a refusal; absent for an `Allow`, whose request has no outcome yet.
    pub api_status: Option<&'static str>,
    /// `api.status_code`: `403` for a refusal; absent for an `Allow`.
    pub api_status_code: Option<i32>,
    /// `requestID`: the identifier the caller received.
    pub request_id: String,
    /// `accessKey`: the verified caller; `None` for an anonymous one.
    pub access_key: Option<String>,
    /// `error`: [`ACCESS_DENIED_ERROR`] for a refusal.
    pub error: Option<&'static str>,
    /// `tags.decision`.
    pub decision: &'static str,
    /// `tags.authorizationStage`.
    pub stage: &'static str,
    /// `tags.operation`.
    pub operation: String,
    /// `tags.policySnapshot`.
    pub policy_snapshot: Option<String>,
    /// `tags.denialReason`, for a refusal.
    pub denial_reason: Option<&'static str>,
}

impl AuthzAuditRecord {
    /// The record's `tags`, as key and value.
    #[must_use]
    pub fn tags(&self) -> Vec<(&'static str, String)> {
        let mut tags = vec![
            (DECISION_TAG, self.decision.to_owned()),
            (STAGE_TAG, self.stage.to_owned()),
            (OPERATION_TAG, self.operation.clone()),
        ];
        if let Some(snapshot) = &self.policy_snapshot {
            tags.push((POLICY_SNAPSHOT_TAG, snapshot.clone()));
        }
        if let Some(reason) = self.denial_reason {
            tags.push((DENIAL_REASON_TAG, reason.to_owned()));
        }
        tags
    }

    /// The legacy contract fields this record populates, in [`OWNED_CONTRACT_FIELDS`] spelling.
    #[must_use]
    pub fn contract_fields(&self) -> Vec<&'static str> {
        let present = [
            ("version", true),
            ("event", true),
            ("trigger", true),
            ("api.name", true),
            ("api.status", self.api_status.is_some()),
            ("api.status_code", self.api_status_code.is_some()),
            ("api.bucket", self.api_bucket.is_some()),
            ("api.object", self.api_object.is_some()),
            ("requestID", true),
            ("accessKey", self.access_key.is_some()),
            ("error", self.error.is_some()),
            ("tags.denialReason", self.denial_reason.is_some()),
        ];
        present.into_iter().filter_map(|(field, set)| set.then_some(field)).collect()
    }
}

/// Where decision records go. Implemented by the binary over `rustfs-audit`.
pub trait AuthzAuditRecordSink: Send + Sync + 'static {
    /// Records one decision. Called inline on the request path: must not block or panic.
    fn record_decision(&self, record: AuthzAuditRecord);
}

/// An [`AuthzAuditSink`] writing one record per decision.
pub struct RustfsAuthzAudit<S> {
    sink: S,
}

impl<S: AuthzAuditRecordSink> RustfsAuthzAudit<S> {
    /// An audit sink reporting into `sink`.
    #[must_use]
    pub fn new(sink: S) -> Self {
        Self { sink }
    }
}

impl<S: AuthzAuditRecordSink> AuthzAuditSink for RustfsAuthzAudit<S> {
    fn on_decision(&self, event: &AuthzAuditEvent<'_>) {
        self.sink.record_decision(audit_record(event));
    }
}

/// The record for one decision; [`RustfsAuthzAudit`] calls it for every event.
#[must_use]
pub fn audit_record(event: &AuthzAuditEvent<'_>) -> AuthzAuditRecord {
    let denial_reason = match event.decision {
        Decision::Allow => None,
        Decision::Deny => Some(DENY_REASON),
        Decision::Indeterminate => Some(INDETERMINATE_REASON),
    };
    let refused = denial_reason.is_some();
    AuthzAuditRecord {
        version: AUDIT_VERSION,
        event: EventName::AuditS3Operation,
        trigger: event.action.to_owned(),
        api_name: event.action.to_owned(),
        api_bucket: event.bucket.map(|bucket| bucket.as_str().to_owned()),
        api_object: event.key.map(|key| key.as_str().to_owned()),
        api_status: refused.then_some("failure"),
        api_status_code: refused.then_some(403),
        request_id: event.request_id.as_str().to_owned(),
        access_key: event.identity.map(|identity| identity.access_key_id().to_owned()),
        error: refused.then_some(ACCESS_DENIED_ERROR),
        decision: event.decision.as_str(),
        stage: match event.stage {
            AuthzStage::Route => "route",
            AuthzStage::Input => "input",
        },
        operation: event.operation.to_owned(),
        policy_snapshot: event.policy_snapshot.map(|snapshot| snapshot.to_string()),
        denial_reason,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use core::time::Duration;
    use rustfs_gateway::{
        AuthSchemeRef, AuthzRequest, BucketName, Identity, ObjectKey, PolicySnapshot, RequestId, ResourceShape, SessionBinding,
        TargetOrigin,
    };
    use std::sync::Mutex;

    const ACCESS_KEY: &str = "AKIDCALLER0001";
    const BUCKET: &str = "audit-bucket";
    const KEY: &str = "reports/q3.csv";

    #[derive(Default)]
    struct Recorded(Mutex<Vec<AuthzAuditRecord>>);

    impl AuthzAuditRecordSink for Recorded {
        fn record_decision(&self, record: AuthzAuditRecord) {
            self.0.lock().expect("not poisoned").push(record);
        }
    }

    struct Fixture {
        request_id: RequestId,
        bucket: BucketName,
        key: ObjectKey,
        identity: Identity,
        snapshot: PolicySnapshot,
    }

    fn fixture() -> Fixture {
        Fixture {
            request_id: RequestId::from_bits(0x5EED),
            bucket: BucketName::new(BUCKET).expect("a valid bucket"),
            key: ObjectKey::new(KEY).expect("a valid key"),
            identity: Identity::new(ACCESS_KEY).expect("a valid access key"),
            snapshot: PolicySnapshot::empty(),
        }
    }

    fn event<'a>(f: &'a Fixture, decision: Decision, identity: Option<&'a Identity>) -> AuthzAuditEvent<'a> {
        AuthzAuditEvent {
            request_id: &f.request_id,
            stage: AuthzStage::Route,
            operation: "GetObject",
            action: "s3:GetObject",
            resource: ResourceShape::Object,
            bucket: Some(&f.bucket),
            key: Some(&f.key),
            resources: &[],
            auth_scheme: if identity.is_some() {
                AuthSchemeRef::SigV4Header
            } else {
                AuthSchemeRef::Anonymous
            },
            identity,
            target_origin: TargetOrigin::Path,
            policy_snapshot: Some(f.snapshot.id()),
            decision,
            elapsed: Duration::ZERO,
        }
    }

    fn recorded(decisions: &[Decision]) -> Vec<AuthzAuditRecord> {
        let f = fixture();
        let sink = RustfsAuthzAudit::new(Recorded::default());
        for decision in decisions {
            sink.on_decision(&event(&f, *decision, Some(&f.identity)));
        }
        sink.sink.0.into_inner().expect("not poisoned")
    }

    /// Positive — every decision is one record, and an `Allow` is recorded too.
    #[test]
    fn audit_every_decision_is_one_record() {
        let records = recorded(&[Decision::Allow, Decision::Deny, Decision::Indeterminate]);
        let decisions: Vec<&str> = records.iter().map(|record| record.decision).collect();
        assert_eq!(decisions, ["allow", "deny", "indeterminate"]);
        let allow = &records[0];
        assert_eq!(allow.api_status, None);
        assert_eq!(allow.error, None);
        assert_eq!(allow.denial_reason, None);
        assert_eq!(allow.access_key.as_deref(), Some(ACCESS_KEY));
        assert_eq!(allow.request_id, RequestId::from_bits(0x5EED).as_str());
    }

    /// Negative — `Indeterminate` is recorded as itself, distinguishable from `Deny` in both the
    /// decision tag and the denial reason, though both are the same refusal on the wire.
    #[test]
    fn audit_indeterminate_is_not_deny() {
        let records = recorded(&[Decision::Deny, Decision::Indeterminate]);
        let (deny, indeterminate) = (&records[0], &records[1]);
        assert_eq!(indeterminate.decision, "indeterminate");
        assert!(indeterminate.tags().contains(&(DECISION_TAG, "indeterminate".to_owned())));
        assert_eq!(indeterminate.denial_reason, Some(INDETERMINATE_REASON));
        assert_eq!(deny.decision, "deny");
        assert_eq!(deny.denial_reason, Some(DENY_REASON));
        assert_ne!(deny.tags(), indeterminate.tags());
        for refusal in [deny, indeterminate] {
            assert_eq!(refusal.api_status, Some("failure"));
            assert_eq!(refusal.api_status_code, Some(403));
            assert_eq!(refusal.error, Some(ACCESS_DENIED_ERROR));
        }
    }

    /// Negative — an anonymous caller has no `accessKey`; none is invented, not even an empty one.
    #[test]
    fn audit_anonymous_caller_has_no_access_key() {
        let f = fixture();
        for decision in [Decision::Allow, Decision::Deny, Decision::Indeterminate] {
            let record = audit_record(&event(&f, decision, None));
            assert_eq!(record.access_key, None, "{decision:?}");
            assert!(!record.contract_fields().contains(&"accessKey"), "{decision:?}");
        }
    }

    /// Negative — a policy source that could not answer leaves no snapshot tag rather than a
    /// placeholder.
    #[test]
    fn audit_missing_policy_reading_has_no_snapshot_tag() {
        let f = fixture();
        let mut unread = event(&f, Decision::Indeterminate, Some(&f.identity));
        unread.policy_snapshot = None;
        let record = audit_record(&unread);
        assert_eq!(record.policy_snapshot, None);
        assert!(record.tags().iter().all(|(key, _)| *key != POLICY_SNAPSHOT_TAG));
        let read = audit_record(&event(&f, Decision::Indeterminate, Some(&f.identity)));
        assert_eq!(read.policy_snapshot, Some(f.snapshot.id().to_string()));
    }

    /// Negative — a service-level decision has no bucket or object, and the input stage is named.
    #[test]
    fn audit_service_decision_has_no_target() {
        let f = fixture();
        let mut service = event(&f, Decision::Deny, Some(&f.identity));
        service.bucket = None;
        service.key = None;
        service.stage = AuthzStage::Input;
        let record = audit_record(&service);
        assert_eq!((record.api_bucket.as_deref(), record.api_object.as_deref()), (None, None));
        assert_eq!(record.stage, "input");
    }

    /// Negative — the access key is the caller's verified identity, never a default credential,
    /// including for a session credential.
    #[test]
    fn audit_access_key_is_the_callers_identity() {
        let f = fixture();
        let binding = SessionBinding::new(crate::STS_SESSION_ISSUER, 4_000_000_000).expect("a binding");
        let session = Identity::new("ASIATEMPCALLER01")
            .expect("a key")
            .with_session_binding(binding);
        let record = audit_record(&event(&f, Decision::Deny, Some(&session)));
        assert_eq!(record.access_key.as_deref(), Some("ASIATEMPCALLER01"));
    }

    /// Negative — secret material planted in every caller-chosen field the legacy entry has no
    /// field for (a version id, a derived resource, a session binding) reaches nothing the sink
    /// receives, and the record has no field that could hold a secret.
    #[test]
    fn audit_no_secret_reaches_the_sink() {
        const SECRETS: [&str; 4] = [
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYSECRETKEY",
            "AQoDYXdzEJrSESSIONTOKEN",
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024",
            "SSECKEYMDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=",
        ];
        let f = fixture();
        let binding = SessionBinding::new(SECRETS[1], 4_000_000_000).expect("a binding");
        let session = Identity::new(ACCESS_KEY).expect("a key").with_session_binding(binding);
        let derived = AuthzRequest {
            operation: "GetObject",
            action: "s3:GetObjectVersion",
            resource: ResourceShape::Object,
            bucket: Some(&f.bucket),
            key: Some(&f.key),
            copy_source_identity: None,
            version_id: Some(SECRETS[0]),
            route_action: SECRETS[2],
            route_bucket: Some(&f.bucket),
            route_key: Some(&f.key),
            identity: Some(&session),
            target_origin: TargetOrigin::Host,
            subject: None,
        };
        let resources = [derived];
        let mut planted = event(&f, Decision::Deny, Some(&session));
        planted.resources = &resources;
        planted.stage = AuthzStage::Input;
        let sink = RustfsAuthzAudit::new(Recorded::default());
        sink.on_decision(&planted);
        let records = sink.sink.0.into_inner().expect("not poisoned");
        assert_eq!(records.len(), 1);
        let everything = format!("{:?} {:?}", records[0], records[0].tags());
        for secret in SECRETS {
            assert!(!everything.contains(secret), "{secret} reached the sink: {everything}");
        }
        assert!(everything.contains(ACCESS_KEY), "the access key id is the one identity fact recorded");
    }

    /// Negative — a refusal populates exactly the fields this module claims, so the fixture's
    /// `gateway-authz-audit` owner is backed by what a record really carries.
    #[test]
    fn audit_refusal_populates_the_owned_contract_fields() {
        let records = recorded(&[Decision::Deny, Decision::Indeterminate]);
        for record in &records {
            assert_eq!(record.contract_fields(), OWNED_CONTRACT_FIELDS, "{}", record.decision);
        }
    }
}
