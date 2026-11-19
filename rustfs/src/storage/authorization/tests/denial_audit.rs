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

//! Authorization denial auditing on the owned request: the client-facing AccessDenied stays
//! generic, a denial yields one bounded audit entry built from the envelope's audit view, probe
//! and aggregate flags suppress it, and every request-level denial exit without a
//! `DenialContext` still calls `audit_request_denial` in its owning production file.
//!
//! Not covered here: the limiter's own token arithmetic (`storage/denial_audit.rs` tests), the
//! policy decisions that produce a denial, or any legacy request type.
//! Upstream: `DenialContext`, `audit_request_denial`, `DenialAuditor::audit_owned`, `BulkDenialAudit`
//! and the envelope's `AuthorizationRequest::audit_denial`; downstream: none.

use crate::app::RequestEnvelope;
use crate::storage::authorization::tests::fixture::request;
use crate::storage::authorization::{DenialContext, audit_request_denial};
use crate::storage::denial_audit::{BulkDenialAudit, DenialAuditor, DenialRateLimiter};
use crate::storage::helper::DenialScope;
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use http::Method;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type CapturedAudit = Arc<Mutex<Vec<rustfs_audit::AuditEntry>>>;

/// An auditor wired like the global one except that entries land in the returned vector.
fn test_auditor(enabled: bool, authenticated_rate: u64, anonymous_rate: u64) -> (DenialAuditor, CapturedAudit) {
    let captured: CapturedAudit = Arc::default();
    let sink = Arc::clone(&captured);
    // A frozen clock: no wall-clock refill can change what the limiter admits.
    let t0 = Instant::now();
    let limiter = DenialRateLimiter::new(authenticated_rate, anonymous_rate, t0);
    let auditor =
        DenialAuditor::new(limiter, move || enabled, move |_, entry| sink.lock().unwrap().push(entry)).with_clock(move || t0);
    (auditor, captured)
}

fn denial_request(request_id: &str, authenticated: bool) -> RequestEnvelope {
    request(Method::GET)
        .uri("/audit-bucket/audit-object")
        .extension(RequestContext {
            request_id: request_id.to_string(),
            x_amz_request_id: request_id.to_string(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        })
        .extension(ReqInfo {
            cred: authenticated.then(|| rustfs_credentials::Credentials {
                access_key: "denied-user".to_string(),
                ..Default::default()
            }),
            bucket: Some("audit-bucket".to_string()),
            object: Some("audit-object".to_string()),
            ..Default::default()
        })
        .build()
}

fn denial_context(auditor: &DenialAuditor, quiet: bool) -> DenialContext<'_> {
    DenialContext {
        quiet,
        suppress_audit: false,
        auditor,
        bucket: "audit-bucket",
        object: "audit-object",
        version_id: Some("version"),
        account: Some("denied-user"),
        is_owner: false,
    }
}

const GET_OBJECT: Action = Action::S3Action(S3Action::GetObjectAction);

/// Issue #5740: the denial helper must keep the client-facing error identical
/// to the historical bare response — the added diagnostics are log-only.
#[test]
fn test_access_denied_helper_keeps_generic_wire_response() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let req = denial_request("denial-wire-response", true);
    let err = denial_context(&auditor, false).deny(&req, "bucket_policy_explicit_deny", GET_OBJECT);
    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    assert_eq!(err.message(), Some("Access Denied"));
    assert_eq!(captured.lock().unwrap().len(), 1);
}

/// A real denial yields exactly one audit entry naming the account, bucket, object,
/// denied action and internal reason.
#[test]
fn denial_is_audited_once_with_account_resource_action_and_reason() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let req = denial_request("denial-audited-once", true);
    let _ = denial_context(&auditor, false).deny(&req, "iam_explicit_deny", Action::S3Action(S3Action::PutObjectAction));

    let entries = captured.lock().unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.request_id.as_deref(), Some("denial-audited-once"));
    assert_eq!(entry.access_key.as_deref(), Some("denied-user"));
    assert_eq!(entry.api.name.as_deref(), Some("s3:PutObject"));
    assert_eq!(entry.api.bucket.as_deref(), Some("audit-bucket"));
    assert_eq!(entry.api.object.as_deref(), Some("audit-object"));
    assert_eq!(entry.api.status.as_deref(), Some("failure"));
    assert_eq!(entry.api.status_code, Some(403));
    assert_eq!(entry.error.as_deref(), Some("Access Denied"));
    let tags = entry.tags.as_ref().expect("denial entry carries its reason");
    assert_eq!(tags.get("denialReason").and_then(|value| value.as_str()), Some("iam_explicit_deny"));
    assert!(!tags.contains_key("suppressedDenials"));
}

/// Probe denials (per-bucket ListBuckets fallback, per-object metadata collection) are
/// expected filter outcomes and must not flood the audit stream.
#[test]
fn quiet_probe_denial_is_not_audited() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let req = denial_request("denial-quiet-probe", true);
    let err = denial_context(&auditor, true).deny(&req, "no_policy_allows_action", Action::S3Action(S3Action::ListBucketAction));
    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    assert!(captured.lock().unwrap().is_empty());
}

#[test]
fn denial_is_not_audited_when_the_audit_module_is_off() {
    let (auditor, captured) = test_auditor(false, 10, 10);
    let req = denial_request("denial-audit-off", true);
    let _ = denial_context(&auditor, false).deny(&req, "no_policy_allows_action", GET_OBJECT);
    assert!(captured.lock().unwrap().is_empty());
}

#[test]
fn denial_audited_in_aggregate_by_the_caller_is_not_audited_per_key() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let req = denial_request("denial-per-key", true);
    let mut denial = denial_context(&auditor, false);
    denial.suppress_audit = true;
    let _ = denial.deny(&req, "iam_explicit_deny", GET_OBJECT);
    assert!(captured.lock().unwrap().is_empty());
}

/// A flood of denials emits a bounded number of entries; the rest are only counted.
#[test]
fn denial_flood_emits_bounded_entries() {
    let (auditor, captured) = test_auditor(true, 3, 3);
    let req = denial_request("denial-flood", true);
    for _ in 0..20 {
        let _ = denial_context(&auditor, false).deny(&req, "no_policy_allows_action", GET_OBJECT);
    }
    assert_eq!(captured.lock().unwrap().len(), 3);
}

/// The entry that follows a suppressed run reports how many denials were dropped.
///
/// The owned auditor reads time only from its clock, so this test drives a settable clock
/// instead of passing an instant per call.
#[test]
fn entry_after_a_flood_reports_the_suppressed_count() {
    let captured: CapturedAudit = Arc::default();
    let sink = Arc::clone(&captured);
    let t0 = Instant::now();
    let now = Arc::new(Mutex::new(t0));
    let clock = Arc::clone(&now);
    let auditor = DenialAuditor::new(
        DenialRateLimiter::new(1, 1, t0),
        || true,
        move |_, entry| sink.lock().unwrap().push(entry),
    )
    .with_clock(move || *clock.lock().unwrap());
    let req = denial_request("denial-suppressed-count", true);
    for _ in 0..5 {
        auditor.audit_owned(&req.audit_view(), &GET_OBJECT, "no_policy_allows_action", DenialScope::Request);
    }
    *now.lock().unwrap() = t0 + Duration::from_secs(2);
    auditor.audit_owned(&req.audit_view(), &GET_OBJECT, "no_policy_allows_action", DenialScope::Request);
    let entries = captured.lock().unwrap();
    assert_eq!(entries.len(), 2);
    let tag = |index: usize| {
        entries[index]
            .tags
            .as_ref()
            .unwrap()
            .get("suppressedDenials")
            .and_then(|v| v.as_u64())
    };
    assert_eq!(tag(0), None);
    assert_eq!(tag(1), Some(4));
}

/// Anonymous callers have their own, lower limit, so an unauthenticated scanner cannot
/// use up the budget for authenticated callers.
#[test]
fn anonymous_denials_are_limited_separately_from_authenticated_ones() {
    let (auditor, captured) = test_auditor(true, 10, 1);
    let anonymous = denial_request("denial-anonymous", false);
    let authenticated = denial_request("denial-authenticated", true);
    for _ in 0..5 {
        let _ = denial_context(&auditor, false).deny(&anonymous, "no_policy_allows_action", GET_OBJECT);
    }
    for _ in 0..5 {
        let _ = denial_context(&auditor, false).deny(&authenticated, "no_policy_allows_action", GET_OBJECT);
    }
    let entries = captured.lock().unwrap();
    let count = |id: &str| entries.iter().filter(|entry| entry.request_id.as_deref() == Some(id)).count();
    assert_eq!(count("denial-anonymous"), 1);
    assert_eq!(count("denial-authenticated"), 5);
}

/// Exits that have no `DenialContext` of their own audit through the request's own state
/// and honor the same probe flags.
#[test]
fn request_denial_exit_is_audited_and_honors_the_probe_flags() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let mut req = denial_request("denial-exit", true);
    audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
    assert_eq!(captured.lock().unwrap().len(), 1);

    req.req_info_mut().unwrap().suppress_denial_log = true;
    audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
    assert_eq!(captured.lock().unwrap().len(), 1);

    req.req_info_mut().unwrap().suppress_denial_log = false;
    req.req_info_mut().unwrap().suppress_denial_audit = true;
    audit_request_denial(&req, &auditor, "table_resource_policy_denied", GET_OBJECT);
    assert_eq!(captured.lock().unwrap().len(), 1);
}

fn production_sources_audit_reason(sources: &[&str], reason: &str) -> bool {
    sources.iter().any(|source| {
        let production = source.split("\nmod tests {").next().expect("source has a production section");
        production
            .match_indices("audit_request_denial(")
            .any(|(at, _)| production[at..].chars().take(260).collect::<String>().contains(reason))
    })
}

/// Request-level denial exits span the owned authorization modules.
/// Check each exit in its owning file alongside the audit behavior tests.
#[test]
fn denial_exits_without_a_context_call_the_audit_function() {
    let access = include_str!("../table_data_plane.rs");
    let object_misc = include_str!("../../access_owned/object_misc.rs")
        .split("\nmod tests;")
        .next()
        .expect("owned source has a production section");
    for (source, reason) in [
        (access, "\"table_resource_policy_denied\""),
        (access, "\"anonymous_table_data_plane_access\""),
        (object_misc, "\"recursive_force_delete_requires_authenticated_caller\""),
    ] {
        assert!(
            production_sources_audit_reason(&[source], reason),
            "{reason} exit does not call audit_request_denial"
        );
    }
}

#[test]
fn denial_source_guard_tracks_moved_calls_and_rejects_removed_calls() {
    let reason = "\"recursive_force_delete_requires_authenticated_caller\"";
    let original = "fn original_exit() {}";
    let moved = format!("fn moved_exit() {{ audit_request_denial(req, auditor, {reason}, action); }}");

    assert!(!production_sources_audit_reason(&[original], reason));
    assert!(production_sources_audit_reason(&[original, &moved], reason));

    let removed = moved.replace("audit_request_denial(", "unrecorded_denial(");
    assert!(!production_sources_audit_reason(&[original, &removed], reason));

    let test_only = format!("{original}\nmod tests {{\n{moved}\n}}");
    assert!(!production_sources_audit_reason(&[original, &test_only], reason));
}

/// DeleteObjects audits its refused keys as one entry per denied action, carrying the count and
/// no single object, and the per-key mark is cleared afterwards.
#[test]
fn bulk_denial_is_one_entry_per_denied_action_with_the_key_count() {
    let (auditor, captured) = test_auditor(true, 10, 10);
    let mut envelope = denial_request("denial-bulk", true);

    let nothing_denied = BulkDenialAudit::start_owned(&mut envelope).unwrap();
    assert!(envelope.req_info().unwrap().suppress_denial_audit);
    nothing_denied
        .finish_owned_with(&mut envelope, RequestEnvelope::audit_view, &auditor)
        .unwrap();
    assert!(!envelope.req_info().unwrap().suppress_denial_audit);
    assert!(captured.lock().unwrap().is_empty());

    let mut bulk = BulkDenialAudit::start_owned(&mut envelope).unwrap();
    let delete = Action::S3Action(S3Action::DeleteObjectAction);
    let bypass = Action::S3Action(S3Action::BypassGovernanceRetentionAction);
    for action in [delete, delete, bypass, delete] {
        bulk.denied(action);
    }
    bulk.finish_owned_with(&mut envelope, RequestEnvelope::audit_view, &auditor)
        .unwrap();

    let entries = captured.lock().unwrap();
    assert_eq!(entries.len(), 2);
    let summary: Vec<_> = entries
        .iter()
        .map(|entry| {
            assert_eq!(entry.api.object, None);
            (
                entry.api.name.clone().unwrap(),
                entry
                    .tags
                    .as_ref()
                    .unwrap()
                    .get("deniedKeys")
                    .and_then(|value| value.as_u64())
                    .unwrap(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            ("s3:DeleteObject".to_string(), 3),
            ("s3:BypassGovernanceRetention".to_string(), 1)
        ]
    );
}
