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

//! Audit entries for authorization denials.
//!
//! A denial is caused by the caller, not by an operation the server chose to run, so the
//! rate of entries is bounded: a process-wide token bucket per caller class (authenticated
//! and anonymous), with a lower default for anonymous callers. Suppressed denials are
//! counted and the count is carried on the next entry that is let through.

use crate::module_switches::is_audit_module_enabled;
use crate::storage::authorization::{AuthorizationRequest, req_info_mut};
use crate::storage::helper::{AuditRequestView, DenialScope, authorization_denial_entry_for_view, spawn_background_with_context};
use crate::storage::request_context::RequestContext;
use rustfs_audit::{AuditEntry, global::AuditLogger};
use rustfs_policy::policy::action::Action;
use rustfs_s3_types::S3Result;
use serde_json::Value;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Tag on an entry that reports how many denials were suppressed since the previous entry.
pub(crate) const SUPPRESSED_DENIALS_TAG: &str = "suppressedDenials";

struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    /// Denials dropped since this class last emitted an entry.
    suppressed: u64,
}

impl Bucket {
    fn new(rate_per_sec: u64) -> Self {
        let rate = rate_per_sec as f64;
        let burst = rate.max(1.0);
        Self {
            rate,
            burst,
            tokens: burst,
            suppressed: 0,
        }
    }

    fn refill(&mut self, elapsed_secs: f64) {
        self.tokens = (self.tokens + elapsed_secs * self.rate).min(self.burst);
    }

    /// Takes one token. `Suppress` drops the denial; a bucket with rate 0 is switched
    /// off and does not count what it drops.
    fn admit(&mut self) -> Admission {
        if self.rate > 0.0 && self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Admission::Emit {
                suppressed: std::mem::take(&mut self.suppressed),
            }
        } else {
            if self.rate > 0.0 {
                self.suppressed = self.suppressed.saturating_add(1);
            }
            Admission::Suppress
        }
    }
}

struct LimiterState {
    authenticated: Bucket,
    anonymous: Bucket,
    last: Instant,
}

/// Outcome of asking the limiter for one denial entry.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Emit the entry; `suppressed` denials were dropped since the previous one.
    Emit {
        suppressed: u64,
    },
    Suppress,
}

pub(crate) struct DenialRateLimiter {
    state: Mutex<LimiterState>,
}

impl DenialRateLimiter {
    pub(crate) fn new(authenticated_rate: u64, anonymous_rate: u64, now: Instant) -> Self {
        Self {
            state: Mutex::new(LimiterState {
                authenticated: Bucket::new(authenticated_rate),
                anonymous: Bucket::new(anonymous_rate),
                last: now,
            }),
        }
    }

    pub(crate) fn admit(&self, anonymous: bool, now: Instant) -> Admission {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
        state.last = now.max(state.last);
        state.authenticated.refill(elapsed);
        state.anonymous.refill(elapsed);

        if anonymous {
            state.anonymous.admit()
        } else {
            state.authenticated.admit()
        }
    }
}

type Emit = Box<dyn Fn(Option<RequestContext>, AuditEntry) + Send + Sync>;

/// Builds, bounds and dispatches denial audit entries.
pub(crate) struct DenialAuditor {
    limiter: DenialRateLimiter,
    enabled: Box<dyn Fn() -> bool + Send + Sync>,
    emit: Emit,
    clock: Box<dyn Fn() -> Instant + Send + Sync>,
}

impl DenialAuditor {
    pub(crate) fn new(
        limiter: DenialRateLimiter,
        enabled: impl Fn() -> bool + Send + Sync + 'static,
        emit: impl Fn(Option<RequestContext>, AuditEntry) + Send + Sync + 'static,
    ) -> Self {
        Self {
            limiter,
            enabled: Box::new(enabled),
            emit: Box::new(emit),
            clock: Box::new(Instant::now),
        }
    }

    #[cfg(test)]
    pub(crate) fn collecting(
        rate: u64,
        clock: impl Fn() -> Instant + Send + Sync + 'static,
        emit: impl Fn(Option<RequestContext>, AuditEntry) + Send + Sync + 'static,
    ) -> Self {
        Self::new(DenialRateLimiter::new(rate, rate, clock()), || true, emit).with_clock(clock)
    }

    /// Replaces the clock the limiter is driven by; tests use a frozen one so no wall-clock
    /// refill can change an outcome.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: impl Fn() -> Instant + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    /// The process-wide auditor: audit switch from the module state, rates from the environment,
    /// entries dispatched to the audit system in the background.
    pub(crate) fn global() -> &'static Self {
        static AUDITOR: OnceLock<DenialAuditor> = OnceLock::new();
        AUDITOR.get_or_init(|| {
            let limiter = DenialRateLimiter::new(
                rustfs_utils::get_env_u64(rustfs_config::ENV_AUDIT_DENIAL_RATE, rustfs_config::DEFAULT_AUDIT_DENIAL_RATE),
                rustfs_utils::get_env_u64(
                    rustfs_config::ENV_AUDIT_DENIAL_ANONYMOUS_RATE,
                    rustfs_config::DEFAULT_AUDIT_DENIAL_ANONYMOUS_RATE,
                ),
                Instant::now(),
            );
            DenialAuditor::new(limiter, is_audit_module_enabled, |context, entry| {
                spawn_background_with_context(context, async move {
                    AuditLogger::log(entry).await;
                });
            })
        })
    }

    /// Audits a denial for a request an owned edge decoded into `view`.
    pub(crate) fn audit_owned(&self, view: &AuditRequestView<'_>, action: &Action, reason: &'static str, scope: DenialScope) {
        self.audit_if_admitted(view.req_info.is_none_or(|info| info.cred.is_none()), || {
            (view.request_context(), authorization_denial_entry_for_view(view, action, reason, scope))
        });
    }

    pub(crate) fn enabled(&self) -> bool {
        (self.enabled)()
    }

    /// Build and publish one confirmed refusal under the shared suppression budget.
    pub(crate) fn audit_if_admitted(&self, anonymous: bool, build: impl FnOnce() -> (Option<RequestContext>, AuditEntry)) {
        if !self.enabled() {
            return;
        }
        let Admission::Emit { suppressed } = self.limiter.admit(anonymous, (self.clock)()) else {
            return;
        };
        let (context, mut entry) = build();
        if suppressed > 0 {
            entry
                .tags
                .get_or_insert_with(Default::default)
                .insert(SUPPRESSED_DENIALS_TAG.to_string(), Value::from(suppressed));
        }
        (self.emit)(context, entry);
    }
}

/// Audits the refused keys of a bulk request (DeleteObjects) in aggregate instead of once per key.
///
/// `start_owned` marks the request so per-key denials are logged but not audited; `finish_owned` clears the
/// mark and emits one entry per denied action with the number of keys refused for it.
pub(crate) struct BulkDenialAudit {
    denied: Vec<(Action, u64)>,
}

impl BulkDenialAudit {
    pub(crate) fn start_owned<R: AuthorizationRequest + ?Sized>(request: &mut R) -> S3Result<Self> {
        req_info_mut(request)?.suppress_denial_audit = true;
        Ok(Self { denied: Vec::new() })
    }

    /// Counts one refused key for `action`.
    pub(crate) fn denied(&mut self, action: Action) {
        match self.denied.iter_mut().find(|(denied_action, _)| *denied_action == action) {
            Some((_, count)) => *count += 1,
            None => self.denied.push((action, 1)),
        }
    }

    pub(crate) fn finish_owned<R: AuthorizationRequest + ?Sized>(
        self,
        request: &mut R,
        audit_view: impl FnOnce(&R) -> AuditRequestView<'_>,
    ) -> S3Result<()> {
        self.finish_owned_with(request, audit_view, DenialAuditor::global())
    }

    pub(crate) fn finish_owned_with<R: AuthorizationRequest + ?Sized>(
        self,
        request: &mut R,
        audit_view: impl FnOnce(&R) -> AuditRequestView<'_>,
        auditor: &DenialAuditor,
    ) -> S3Result<()> {
        req_info_mut(request)?.suppress_denial_audit = false;
        let view = audit_view(request);
        for (action, keys) in self.denied {
            auditor.audit_owned(&view, &action, "bulk_request_keys_denied", DenialScope::Keys(keys));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, DenialRateLimiter};
    use std::time::{Duration, Instant};

    #[test]
    fn burst_is_bounded_and_suppressed_denials_are_counted_on_the_next_entry() {
        let t0 = Instant::now();
        let limiter = DenialRateLimiter::new(2, 1, t0);
        assert_eq!(limiter.admit(false, t0), Admission::Emit { suppressed: 0 });
        assert_eq!(limiter.admit(false, t0), Admission::Emit { suppressed: 0 });
        for _ in 0..5 {
            assert_eq!(limiter.admit(false, t0), Admission::Suppress);
        }
        // One second later the bucket has refilled: the next entry reports what was dropped.
        assert_eq!(limiter.admit(false, t0 + Duration::from_secs(1)), Admission::Emit { suppressed: 5 });
        assert_eq!(limiter.admit(false, t0 + Duration::from_secs(1)), Admission::Emit { suppressed: 0 });
    }

    #[test]
    fn suppressed_counts_are_kept_per_caller_class() {
        let t0 = Instant::now();
        let limiter = DenialRateLimiter::new(1, 1, t0);
        assert_eq!(limiter.admit(true, t0), Admission::Emit { suppressed: 0 });
        for _ in 0..4 {
            assert_eq!(limiter.admit(true, t0), Admission::Suppress);
        }
        // The authenticated class dropped nothing, so it must not inherit the anonymous drops.
        assert_eq!(limiter.admit(false, t0), Admission::Emit { suppressed: 0 });
        assert_eq!(limiter.admit(true, t0 + Duration::from_secs(1)), Admission::Emit { suppressed: 4 });
    }

    #[test]
    fn a_zero_rate_disables_that_class_without_counting() {
        let t0 = Instant::now();
        let limiter = DenialRateLimiter::new(2, 0, t0);
        assert_eq!(limiter.admit(true, t0), Admission::Suppress);
        assert_eq!(limiter.admit(false, t0), Admission::Emit { suppressed: 0 });
    }

    #[test]
    fn tokens_never_exceed_the_burst() {
        let t0 = Instant::now();
        let limiter = DenialRateLimiter::new(2, 2, t0);
        let later = t0 + Duration::from_secs(3600);
        assert_eq!(limiter.admit(false, later), Admission::Emit { suppressed: 0 });
        assert_eq!(limiter.admit(false, later), Admission::Emit { suppressed: 0 });
        assert_eq!(limiter.admit(false, later), Admission::Suppress);
    }
}
