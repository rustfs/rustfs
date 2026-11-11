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

//! The gateway's [`AuthzAuditSink`]: the legacy denial record for a refusal the gateway enforced.
//!
//! Responsible for: one [`GatewayDenialRecord`] for each authorization decision the gateway made
//! at an enforcing stage that ended the request with a refusal (`Deny` or `Indeterminate`), after
//! the legacy audit switch and the legacy denial rate limiter have both admitted it.
//! NOT responsible for: the authoritative authorization decision. On the gateway stack that is the
//! legacy access check in owned form, run by the handler before the use case, and it writes its own
//! denial record (`DenialContext::record`, `rustfs/src/storage/access.rs`). The gateway
//! `Authorizer` is only an earlier gate that may refuse what the legacy check certainly refuses, so
//! a request it lets through is not decided yet, and a request it refuses never reaches the check.
//! Nor is this module responsible for building the `rustfs_audit::AuditEntry`, filling its
//! request-line fields, or dispatching it: that is the binary's [`DenialAuditSink`].
//! Upstream: the gateway service, once per decision (`AuthzAuditSink::on_decision`). Downstream:
//! the binary's [`DenialAuditSink`].
//!
//! # What is recorded, and what is not
//!
//! - An `Allow` is never recorded: at the early gate it is not the final decision, and a handler
//!   denial after it would contradict the record.
//! - The missing-object visibility probe is never recorded. For `GetObject` the gateway also asks
//!   `s3:ListBucket`, only to choose between `404` and `403` for an absent object; that answer
//!   refuses nothing, yet it reaches the sink as an `Input` event of its own. Legacy keeps the same
//!   probe out of the audit (`suppress_denial_audit`, `rustfs/src/app/object/head.rs`).
//! - Every other `Deny` or `Indeterminate` ended its request: a route-stage refusal is answered
//!   before the body is read, and an input-stage refusal either fails the stage or prevents the
//!   gateway core from constructing the authorized request. One refused request therefore yields
//!   exactly one record.
//!
//! # Telling the visibility probe apart
//!
//! At the pinned gateway rev every event's `resources` starts with the request the decision was
//! about. For the route and input events that is the route target, whose `action` equals its
//! `route_action` (`crates/gateway/src/service.rs`, the route request and every asked question,
//! and the input stage's route request). The visibility event alone is built from the route
//! target with only `action` replaced by `s3:ListBucket`, keeping the target's `route_action`
//! (`crates/gateway/src/ext/authz_audit.rs`, `emit_input_safely`; `crates/gateway/src/ext/authorizer.rs`,
//! `InputAuthzRequest::new`). An `Input` event whose first resource asks an action other than its
//! route action is the visibility event.
//!
//! # Known limit: the denied action and resource
//!
//! `trigger` (and `api.name`), `api.bucket` and `api.object` are the event's `action`, `bucket` and
//! `key`: the operation's declared action and primary target. The event carries no per-resource
//! decision, so the sink cannot name a derived resource that was refused. This is exact only while
//! every early-gate refusal concerns the declared action and target. An operation whose denial can
//! concern a second resource or a version-specific action (a copy source, a versioned read) must
//! therefore be decided by the handler, never refused by the early gate: legacy records those with
//! the source bucket and object, and with `s3:GetObjectVersion`.
//!
//! # What may not reach a record
//!
//! The record is built field by field from the event's `action`, `bucket`, `key`, `request_id`,
//! `decision` and the verified identity's access key id. Nothing is copied from `resources`,
//! `subject`, `auth_scheme`, a version id or a copy-source identity, and nothing is formatted from a
//! `Debug` implementation. The event carries no secret key, session token or signature.

use rustfs_gateway::{AuthzAuditEvent, AuthzAuditSink, AuthzStage, Decision};

/// `tags.denialReason` of a `Deny` the gateway enforced. The event carries the decision, not one of
/// the legacy reasons (`iam_explicit_deny`, `no_policy_allows_action`, ...).
pub const DENY_REASON: &str = "authorization_denied";
/// `tags.denialReason` of an `Indeterminate` the gateway enforced: no policy reading could answer.
pub const INDETERMINATE_REASON: &str = "authorization_indeterminate";

/// What the legacy denial rate limiter answered for one refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenialAdmission {
    /// Record it; `suppressed` refusals of the same caller class were dropped since the previous
    /// record, and are reported as `tags.suppressedDenials` when non-zero.
    Emit {
        /// Refusals dropped since the previous record of this caller class.
        suppressed: u64,
    },
    /// Drop it.
    Suppress,
}

/// One refusal the gateway enforced, in the fields of the legacy authorization-denial entry the
/// event can supply.
///
/// The binary builds the entry as legacy `authorization_denial_entry` (`rustfs/src/storage/helper.rs`)
/// does: `version` `1.0`, `event` `AuditS3Operation`, `trigger` and `api.name` [`Self::action`],
/// `api.status` `failure`, `api.status_code` `403`, `error` `Access Denied`, `tags.denialReason`
/// [`Self::denial_reason`], and `tags.suppressedDenials` only when [`Self::suppressed_denials`] is
/// non-zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewayDenialRecord {
    /// `trigger` and `api.name`: the operation's declared IAM action, `s3:GetObject`.
    pub action: String,
    /// `api.bucket`, when the request addressed one.
    pub bucket: Option<String>,
    /// `api.object`, when the request addressed one.
    pub object: Option<String>,
    /// `requestID`: the identifier the caller received.
    pub request_id: String,
    /// `accessKey`: the verified caller; `None` for an anonymous caller, as legacy omits it.
    pub access_key: Option<String>,
    /// `tags.denialReason`: [`DENY_REASON`] or [`INDETERMINATE_REASON`].
    pub denial_reason: &'static str,
    /// `tags.suppressedDenials`, from [`DenialAdmission::Emit`].
    pub suppressed_denials: u64,
}

/// Where admitted refusals go. Implemented by the binary over the legacy denial auditor.
///
/// The three methods are called in order, at most once each per refusal, on the request's task,
/// and must neither block nor panic.
///
/// # What the implementation owes the legacy entry
///
/// - [`Self::enabled`] is the legacy audit switch (`is_audit_module_enabled`), and [`Self::admit`]
///   the process-wide legacy `DenialRateLimiter` itself, not a second instance: gateway and handler
///   refusals share one token bucket per caller class.
/// - `time`, `parentUser` and `requestClaims` are filled as legacy fills them.
/// - `remotehost`, `userAgent`, `requestHost`, `requestPath` and `requestQuery` come from the
///   request the outer layer saw. **`requestQuery` is not the URI query.** Legacy fills it with
///   `rustfs_targets::net::extract_params_header` over the request *headers*, which replaces the
///   value of every sensitive header (`authorization`, `x-amz-security-token`, the SSE-C key
///   headers, ...) with the redaction marker. The raw URI query must never be put there: on a
///   presigned request it carries `X-Amz-Signature`, `X-Amz-Credential` and
///   `X-Amz-Security-Token`.
pub trait DenialAuditSink: Send + Sync + 'static {
    /// Whether denial auditing is switched on. Asked first; `false` drops the refusal before the
    /// rate limiter is consulted.
    fn enabled(&self) -> bool;
    /// Asks the rate limiter for one refusal of this caller class. Asked before any record exists.
    fn admit(&self, anonymous: bool) -> DenialAdmission;
    /// Receives one admitted record.
    fn record(&self, record: GatewayDenialRecord);
}

/// An [`AuthzAuditSink`] recording only the refusals the gateway enforced.
pub struct RustfsAuthzAudit<S> {
    sink: S,
}

impl<S: DenialAuditSink> RustfsAuthzAudit<S> {
    /// An audit sink reporting into `sink`.
    #[must_use]
    pub fn new(sink: S) -> Self {
        Self { sink }
    }
}

impl<S: DenialAuditSink> AuthzAuditSink for RustfsAuthzAudit<S> {
    fn on_decision(&self, event: &AuthzAuditEvent<'_>) {
        let denial_reason = match event.decision {
            Decision::Allow => return,
            Decision::Deny => DENY_REASON,
            Decision::Indeterminate => INDETERMINATE_REASON,
        };
        if is_visibility_probe(event) || !self.sink.enabled() {
            return;
        }
        let anonymous = event.identity.is_none();
        let DenialAdmission::Emit { suppressed } = self.sink.admit(anonymous) else {
            return;
        };
        self.sink.record(GatewayDenialRecord {
            action: event.action.to_owned(),
            bucket: event.bucket.map(|bucket| bucket.as_str().to_owned()),
            object: event.key.map(|key| key.as_str().to_owned()),
            request_id: event.request_id.as_str().to_owned(),
            access_key: event.identity.map(|identity| identity.access_key_id().to_owned()),
            denial_reason,
            suppressed_denials: suppressed,
        });
    }
}

/// Whether `event` is the missing-object visibility probe, which refuses nothing. See the module
/// documentation for why the first resource identifies it.
fn is_visibility_probe(event: &AuthzAuditEvent<'_>) -> bool {
    event.stage == AuthzStage::Input
        && event
            .resources
            .first()
            .is_some_and(|first| first.action != first.route_action)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use core::time::Duration;
    use rustfs_gateway::{
        AuthSchemeRef, AuthzRequest, BucketName, Identity, ObjectKey, PolicySnapshot, RequestId, ResourceShape, SessionBinding,
        Subject, TargetOrigin,
    };
    use std::sync::Mutex;

    const ACCESS_KEY: &str = "AKIDCALLER0001";
    const BUCKET: &str = "audit-bucket";
    const KEY: &str = "reports/q3.csv";

    /// A sink with a scripted switch and limiter that records every call it receives.
    struct Probe {
        enabled: bool,
        admission: DenialAdmission,
        switch_asked: Mutex<usize>,
        limiter_asked: Mutex<Vec<bool>>,
        records: Mutex<Vec<GatewayDenialRecord>>,
    }

    impl Probe {
        fn new(enabled: bool, admission: DenialAdmission) -> Self {
            Self {
                enabled,
                admission,
                switch_asked: Mutex::new(0),
                limiter_asked: Mutex::new(Vec::new()),
                records: Mutex::new(Vec::new()),
            }
        }

        fn open() -> Self {
            Self::new(true, DenialAdmission::Emit { suppressed: 0 })
        }

        fn records(&self) -> Vec<GatewayDenialRecord> {
            self.records.lock().expect("not poisoned").clone()
        }

        fn switch_asked(&self) -> usize {
            *self.switch_asked.lock().expect("not poisoned")
        }

        fn limiter_asked(&self) -> Vec<bool> {
            self.limiter_asked.lock().expect("not poisoned").clone()
        }
    }

    impl DenialAuditSink for Probe {
        fn enabled(&self) -> bool {
            *self.switch_asked.lock().expect("not poisoned") += 1;
            self.enabled
        }

        fn admit(&self, anonymous: bool) -> DenialAdmission {
            self.limiter_asked.lock().expect("not poisoned").push(anonymous);
            self.admission
        }

        fn record(&self, record: GatewayDenialRecord) {
            self.records.lock().expect("not poisoned").push(record);
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

    /// The route target of a GetObject, as the gateway builds it: its action is its route action.
    fn target<'a>(f: &'a Fixture, identity: Option<&'a Identity>) -> AuthzRequest<'a> {
        AuthzRequest {
            operation: "GetObject",
            action: "s3:GetObject",
            resource: ResourceShape::Object,
            bucket: Some(&f.bucket),
            key: Some(&f.key),
            copy_source_identity: None,
            version_id: None,
            route_action: "s3:GetObject",
            route_bucket: Some(&f.bucket),
            route_key: Some(&f.key),
            identity,
            target_origin: TargetOrigin::Path,
            subject: None,
        }
    }

    /// The visibility question, built from the route target as `InputAuthzRequest::new` builds it.
    fn visibility<'a>(route: &AuthzRequest<'a>) -> AuthzRequest<'a> {
        AuthzRequest {
            action: "s3:ListBucket",
            resource: ResourceShape::Bucket,
            copy_source_identity: None,
            version_id: None,
            ..*route
        }
    }

    /// A route or input event of a GetObject about `resources`.
    fn event<'a>(
        f: &'a Fixture,
        stage: AuthzStage,
        decision: Decision,
        resources: &'a [AuthzRequest<'a>],
        identity: Option<&'a Identity>,
    ) -> AuthzAuditEvent<'a> {
        AuthzAuditEvent {
            request_id: &f.request_id,
            stage,
            operation: "GetObject",
            action: "s3:GetObject",
            resource: ResourceShape::Object,
            bucket: Some(&f.bucket),
            key: Some(&f.key),
            resources,
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

    /// The visibility event, as `emit_input_safely` builds it from the input event.
    fn visibility_event<'a>(
        input: &AuthzAuditEvent<'a>,
        request: &'a AuthzRequest<'a>,
        decision: Decision,
    ) -> AuthzAuditEvent<'a> {
        AuthzAuditEvent {
            stage: AuthzStage::Input,
            action: request.action,
            resource: request.resource,
            bucket: request.bucket,
            key: request.key,
            resources: std::slice::from_ref(request),
            decision,
            ..*input
        }
    }

    /// Drives the events of one GetObject through `audit` in the gateway's order: the route stage,
    /// then (when the route allowed) the input stage followed by its visibility question.
    fn get_object(
        audit: &RustfsAuthzAudit<Probe>,
        f: &Fixture,
        identity: Option<&Identity>,
        route: Decision,
        input: Decision,
        probe: Decision,
    ) {
        let route_target = target(f, identity);
        let asked = [route_target];
        audit.on_decision(&event(f, AuthzStage::Route, route, &asked, identity));
        if route != Decision::Allow {
            return;
        }
        let input_event = event(f, AuthzStage::Input, input, &asked, identity);
        audit.on_decision(&input_event);
        let question = visibility(&route_target);
        audit.on_decision(&visibility_event(&input_event, &question, probe));
    }

    fn expected(denial_reason: &'static str, access_key: Option<&str>, suppressed_denials: u64) -> GatewayDenialRecord {
        GatewayDenialRecord {
            action: "s3:GetObject".to_owned(),
            bucket: Some(BUCKET.to_owned()),
            object: Some(KEY.to_owned()),
            request_id: RequestId::from_bits(0x5EED).as_str().to_owned(),
            access_key: access_key.map(str::to_owned),
            denial_reason,
            suppressed_denials,
        }
    }

    /// Positive — a route-stage refusal is one record carrying the legacy denial entry's values,
    /// and `Indeterminate` keeps a reason of its own.
    #[test]
    fn p_route_refusal_is_one_record_with_the_legacy_values() {
        for (decision, reason) in [(Decision::Deny, DENY_REASON), (Decision::Indeterminate, INDETERMINATE_REASON)] {
            let f = fixture();
            let audit = RustfsAuthzAudit::new(Probe::open());
            get_object(&audit, &f, Some(&f.identity), decision, Decision::Allow, Decision::Allow);
            assert_eq!(audit.sink.records(), [expected(reason, Some(ACCESS_KEY), 0)], "{decision:?}");
            assert_eq!(audit.sink.limiter_asked(), [false], "{decision:?}");
        }
    }

    /// Positive — an input-stage refusal of a GetObject is exactly one record, though its
    /// visibility question was refused as well.
    #[test]
    fn p_enforcing_input_refusal_is_recorded_once() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::open());
        get_object(&audit, &f, Some(&f.identity), Decision::Allow, Decision::Deny, Decision::Deny);
        assert_eq!(audit.sink.records(), [expected(DENY_REASON, Some(ACCESS_KEY), 0)]);
        assert_eq!(audit.sink.limiter_asked(), [false]);
    }

    /// Positive — the refusals the limiter dropped before this one are carried on the record.
    #[test]
    fn p_suppressed_count_is_carried() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::new(true, DenialAdmission::Emit { suppressed: 7 }));
        get_object(&audit, &f, Some(&f.identity), Decision::Deny, Decision::Allow, Decision::Allow);
        assert_eq!(audit.sink.records(), [expected(DENY_REASON, Some(ACCESS_KEY), 7)]);
    }

    /// Negative — an `Allow` at either stage records nothing and asks neither the switch nor the
    /// limiter, so it cannot spend a token a real refusal needs.
    #[test]
    fn n_allow_is_never_recorded() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::open());
        let asked = [target(&f, Some(&f.identity))];
        for stage in [AuthzStage::Route, AuthzStage::Input] {
            audit.on_decision(&event(&f, stage, Decision::Allow, &asked, Some(&f.identity)));
        }
        assert!(audit.sink.records().is_empty());
        assert_eq!((audit.sink.switch_asked(), audit.sink.limiter_asked()), (0, Vec::new()));
    }

    /// Negative — a GetObject that succeeds for a caller refused `s3:ListBucket` records nothing:
    /// the visibility question refuses no request, whatever it answered.
    #[test]
    fn n_visibility_probe_is_never_recorded() {
        for probe in [Decision::Deny, Decision::Indeterminate] {
            let f = fixture();
            let audit = RustfsAuthzAudit::new(Probe::open());
            get_object(&audit, &f, Some(&f.identity), Decision::Allow, Decision::Allow, probe);
            assert!(audit.sink.records().is_empty(), "{probe:?}");
            assert_eq!((audit.sink.switch_asked(), audit.sink.limiter_asked()), (0, Vec::new()), "{probe:?}");
        }
    }

    /// Negative — with the audit switch off a refusal records nothing and the limiter is never
    /// asked, as legacy checks the switch first.
    #[test]
    fn n_audit_switch_off_records_nothing() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::new(false, DenialAdmission::Emit { suppressed: 0 }));
        get_object(&audit, &f, Some(&f.identity), Decision::Deny, Decision::Allow, Decision::Allow);
        assert!(audit.sink.records().is_empty());
        assert_eq!((audit.sink.switch_asked(), audit.sink.limiter_asked()), (1, Vec::new()));
    }

    /// Negative — a refusal the limiter suppresses records nothing.
    #[test]
    fn n_rate_limiter_suppression_records_nothing() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::new(true, DenialAdmission::Suppress));
        get_object(&audit, &f, Some(&f.identity), Decision::Deny, Decision::Allow, Decision::Allow);
        assert!(audit.sink.records().is_empty());
        assert_eq!(audit.sink.limiter_asked(), [false]);
    }

    /// Negative — an anonymous caller has no access key, not even an empty one, and is limited as
    /// the anonymous class; the authenticated refusal beside it is limited as the other class.
    #[test]
    fn n_anonymous_caller_has_no_access_key() {
        let f = fixture();
        let audit = RustfsAuthzAudit::new(Probe::open());
        get_object(&audit, &f, None, Decision::Deny, Decision::Allow, Decision::Allow);
        get_object(&audit, &f, Some(&f.identity), Decision::Deny, Decision::Allow, Decision::Allow);
        assert_eq!(
            audit.sink.records(),
            [expected(DENY_REASON, None, 0), expected(DENY_REASON, Some(ACCESS_KEY), 0)]
        );
        assert_eq!(audit.sink.limiter_asked(), [true, false]);
    }

    /// Negative — values planted in the event's `resources`, version ids, subject, auth scheme,
    /// target origin and session binding leave the record exactly as it is without them, and none
    /// of their text reaches its `Debug` output.
    #[test]
    fn n_event_fields_outside_the_record_do_not_reach_it() {
        const SECRETS: [&str; 3] = [
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYSECRETKEY",
            "AQoDYXdzEJrSESSIONTOKEN",
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024",
        ];
        const PLANTED_BUCKET: &str = "planted-source-bucket";
        const PLANTED_KEY: &str = "planted/source-key";
        let f = fixture();
        let source_bucket = BucketName::new(PLANTED_BUCKET).expect("a valid bucket");
        let source_key = ObjectKey::new(PLANTED_KEY).expect("a valid key");
        let binding = SessionBinding::new(SECRETS[1], 4_000_000_000).expect("a binding");
        let session = Identity::new(ACCESS_KEY).expect("a key").with_session_binding(binding);
        let subject = Subject::Caller;
        let planted_target = AuthzRequest {
            version_id: Some(SECRETS[0]),
            target_origin: TargetOrigin::Host,
            subject: Some(&subject),
            ..target(&f, Some(&session))
        };
        let derived = AuthzRequest {
            action: "s3:GetObjectVersion",
            bucket: Some(&source_bucket),
            key: Some(&source_key),
            version_id: Some(SECRETS[2]),
            ..planted_target
        };
        let planted_resources = [planted_target, derived];
        let plain_resources = [target(&f, Some(&f.identity))];
        for stage in [AuthzStage::Route, AuthzStage::Input] {
            let plain = RustfsAuthzAudit::new(Probe::open());
            plain.on_decision(&event(&f, stage, Decision::Deny, &plain_resources, Some(&f.identity)));
            let planted = RustfsAuthzAudit::new(Probe::open());
            planted.on_decision(&AuthzAuditEvent {
                auth_scheme: AuthSchemeRef::SigV4Presigned,
                target_origin: TargetOrigin::Host,
                ..event(&f, stage, Decision::Deny, &planted_resources, Some(&session))
            });
            let records = planted.sink.records();
            assert_eq!(records, plain.sink.records(), "{stage:?}");
            assert_eq!(records.len(), 1, "{stage:?}");
            let rendered = format!("{records:?}");
            for planted_text in SECRETS.into_iter().chain([
                PLANTED_BUCKET,
                PLANTED_KEY,
                "GetObjectVersion",
                "Presigned",
                "Host",
                "Caller",
                "Authz",
            ]) {
                assert!(!rendered.contains(planted_text), "{planted_text} reached the record: {rendered}");
            }
        }
    }
}
