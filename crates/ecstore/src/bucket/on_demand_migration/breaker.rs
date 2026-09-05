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

//! Per-bucket three-state circuit breaker protecting an on-demand migration
//! source (rustfs/backlog#2152).
//!
//! `Closed` lets every request through and counts consecutive failures
//! inside a sliding window; reaching the threshold opens the breaker. `Open`
//! rejects everything until the open duration elapses, then moves to
//! `HalfOpen`, which admits a single probe: success closes the breaker,
//! failure re-opens it. Timing uses `tokio::time::Instant` so tests can drive
//! it with `tokio::time::pause`.
//!
//! Only transport-level failures count (`Throttled`, `Timeout`, `Connect`,
//! `ServerError`). `NotFound` is a healthy answer and resets the failure
//! streak; `AccessDenied`, `Unsupported` and `Other` are configuration or
//! object problems that neither open nor close the breaker.

use super::source_client::SourceError;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::Instant;

/// Consecutive counted failures that open the breaker.
pub const BREAKER_FAILURE_THRESHOLD: u32 = 5;
/// Failures further apart than this do not accumulate.
pub const BREAKER_FAILURE_WINDOW: Duration = Duration::from_secs(30);
/// How long an open breaker rejects before admitting a probe.
pub const BREAKER_OPEN_DURATION: Duration = Duration::from_secs(30);
/// Probes admitted while half-open.
pub const BREAKER_HALF_OPEN_MAX_PROBES: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    pub fn as_str(self) -> &'static str {
        match self {
            BreakerState::Closed => "closed",
            BreakerState::Open => "open",
            BreakerState::HalfOpen => "half_open",
        }
    }
}

/// A state change the caller may want to log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BreakerTransition {
    pub from: BreakerState,
    pub to: BreakerState,
}

/// How a source result is scored by the breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerVerdict {
    /// Resets the failure streak; closes a half-open breaker.
    Success,
    /// Counts toward the threshold; re-opens a half-open breaker.
    Failure,
    /// Leaves the breaker untouched.
    Neutral,
}

impl BreakerVerdict {
    /// `None` is a successful source call.
    pub fn for_result(error: Option<&SourceError>) -> Self {
        match error {
            None | Some(SourceError::NotFound) => BreakerVerdict::Success,
            Some(SourceError::Throttled | SourceError::Timeout | SourceError::Connect(_) | SourceError::ServerError(_)) => {
                BreakerVerdict::Failure
            }
            Some(
                SourceError::AccessDenied
                | SourceError::Unsupported(_)
                | SourceError::InvalidPagination(_)
                | SourceError::Other(_),
            ) => BreakerVerdict::Neutral,
        }
    }
}

#[derive(Debug)]
struct Inner {
    state: BreakerState,
    consecutive_failures: u32,
    last_failure_at: Option<Instant>,
    opened_at: Option<Instant>,
    half_open_probes: u32,
}

#[derive(Debug)]
pub struct Breaker {
    inner: Mutex<Inner>,
}

impl Default for Breaker {
    fn default() -> Self {
        Self::new()
    }
}

impl Breaker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                last_failure_at: None,
                opened_at: None,
                half_open_probes: 0,
            }),
        }
    }

    /// Current state after applying the open-duration timeout.
    pub fn state(&self) -> BreakerState {
        let mut inner = self.inner.lock();
        Self::advance(&mut inner, Instant::now());
        inner.state
    }

    /// Whether a request may reach the source right now. Consumes the
    /// half-open probe budget when it grants one.
    pub fn allow_request(&self) -> bool {
        let mut inner = self.inner.lock();
        Self::advance(&mut inner, Instant::now());
        match inner.state {
            BreakerState::Closed => true,
            BreakerState::Open => false,
            BreakerState::HalfOpen => {
                if inner.half_open_probes < BREAKER_HALF_OPEN_MAX_PROBES {
                    inner.half_open_probes += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Scores a source result; returns the transition it caused, if any.
    pub fn record(&self, verdict: BreakerVerdict) -> Option<BreakerTransition> {
        match verdict {
            BreakerVerdict::Success => self.record_success(),
            BreakerVerdict::Failure => self.record_failure(),
            BreakerVerdict::Neutral => None,
        }
    }

    pub fn record_success(&self) -> Option<BreakerTransition> {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        Self::advance(&mut inner, now);
        inner.consecutive_failures = 0;
        inner.last_failure_at = None;
        match inner.state {
            BreakerState::Closed => None,
            // A success while open can only come from a request admitted
            // before the breaker opened; it says nothing about recovery.
            BreakerState::Open => None,
            BreakerState::HalfOpen => Some(Self::transition(&mut inner, BreakerState::Closed, now)),
        }
    }

    pub fn record_failure(&self) -> Option<BreakerTransition> {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        Self::advance(&mut inner, now);
        match inner.state {
            BreakerState::Closed => {
                let within_window = inner
                    .last_failure_at
                    .is_some_and(|last| now.saturating_duration_since(last) <= BREAKER_FAILURE_WINDOW);
                inner.consecutive_failures = if within_window { inner.consecutive_failures + 1 } else { 1 };
                inner.last_failure_at = Some(now);
                if inner.consecutive_failures >= BREAKER_FAILURE_THRESHOLD {
                    Some(Self::transition(&mut inner, BreakerState::Open, now))
                } else {
                    None
                }
            }
            BreakerState::Open => None,
            BreakerState::HalfOpen => Some(Self::transition(&mut inner, BreakerState::Open, now)),
        }
    }

    fn advance(inner: &mut Inner, now: Instant) {
        if inner.state == BreakerState::Open
            && inner
                .opened_at
                .is_some_and(|opened| now.saturating_duration_since(opened) >= BREAKER_OPEN_DURATION)
        {
            Self::transition(inner, BreakerState::HalfOpen, now);
        }
    }

    fn transition(inner: &mut Inner, to: BreakerState, now: Instant) -> BreakerTransition {
        let from = inner.state;
        inner.state = to;
        match to {
            BreakerState::Open => {
                inner.opened_at = Some(now);
                inner.half_open_probes = 0;
            }
            BreakerState::HalfOpen => {
                inner.half_open_probes = 0;
            }
            BreakerState::Closed => {
                inner.opened_at = None;
                inner.half_open_probes = 0;
                inner.consecutive_failures = 0;
                inner.last_failure_at = None;
            }
        }
        BreakerTransition { from, to }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_error() -> SourceError {
        SourceError::ServerError(503)
    }

    #[tokio::test(start_paused = true)]
    async fn five_failures_open_then_half_open_after_timeout() {
        let breaker = Breaker::new();
        for i in 0..BREAKER_FAILURE_THRESHOLD - 1 {
            assert_eq!(breaker.record(BreakerVerdict::for_result(Some(&server_error()))), None, "failure {i}");
            assert_eq!(breaker.state(), BreakerState::Closed);
        }
        assert_eq!(
            breaker.record(BreakerVerdict::for_result(Some(&server_error()))),
            Some(BreakerTransition {
                from: BreakerState::Closed,
                to: BreakerState::Open
            })
        );
        assert_eq!(breaker.state(), BreakerState::Open);
        assert!(!breaker.allow_request());

        tokio::time::advance(BREAKER_OPEN_DURATION - Duration::from_secs(1)).await;
        assert!(!breaker.allow_request());
        assert_eq!(breaker.state(), BreakerState::Open);

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(breaker.state(), BreakerState::HalfOpen);
        assert!(breaker.allow_request(), "one probe is admitted");
        assert!(!breaker.allow_request(), "second probe is rejected");
    }

    #[tokio::test(start_paused = true)]
    async fn half_open_probe_success_closes_and_failure_reopens() {
        let breaker = Breaker::new();
        for _ in 0..BREAKER_FAILURE_THRESHOLD {
            breaker.record_failure();
        }
        tokio::time::advance(BREAKER_OPEN_DURATION).await;
        assert!(breaker.allow_request());
        assert_eq!(
            breaker.record_failure(),
            Some(BreakerTransition {
                from: BreakerState::HalfOpen,
                to: BreakerState::Open
            })
        );
        assert!(!breaker.allow_request());

        tokio::time::advance(BREAKER_OPEN_DURATION).await;
        assert!(breaker.allow_request());
        assert_eq!(
            breaker.record_success(),
            Some(BreakerTransition {
                from: BreakerState::HalfOpen,
                to: BreakerState::Closed
            })
        );
        assert_eq!(breaker.state(), BreakerState::Closed);
        assert!(breaker.allow_request());
        // The streak restarts from zero after closing.
        for _ in 0..BREAKER_FAILURE_THRESHOLD - 1 {
            assert_eq!(breaker.record_failure(), None);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_outside_window_do_not_accumulate() {
        let breaker = Breaker::new();
        for _ in 0..BREAKER_FAILURE_THRESHOLD - 1 {
            breaker.record_failure();
        }
        tokio::time::advance(BREAKER_FAILURE_WINDOW + Duration::from_secs(1)).await;
        assert_eq!(breaker.record_failure(), None, "stale streak restarts at one");
        assert_eq!(breaker.state(), BreakerState::Closed);
    }

    #[test]
    fn not_found_and_access_denied_do_not_count() {
        let breaker = Breaker::new();
        for _ in 0..BREAKER_FAILURE_THRESHOLD - 1 {
            breaker.record(BreakerVerdict::for_result(Some(&server_error())));
        }
        assert_eq!(breaker.record(BreakerVerdict::for_result(Some(&SourceError::AccessDenied))), None);
        assert_eq!(breaker.state(), BreakerState::Closed);
        // AccessDenied is neutral: the streak is still one short of opening.
        assert_eq!(
            breaker.record(BreakerVerdict::for_result(Some(&SourceError::Unsupported("sse-c".into())))),
            None
        );
        assert_eq!(breaker.record(BreakerVerdict::for_result(Some(&SourceError::Other("x".into())))), None);
        // NotFound is a healthy answer and resets the streak entirely.
        assert_eq!(breaker.record(BreakerVerdict::for_result(Some(&SourceError::NotFound))), None);
        for _ in 0..BREAKER_FAILURE_THRESHOLD - 1 {
            assert_eq!(breaker.record(BreakerVerdict::for_result(Some(&SourceError::Timeout))), None);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
    }

    #[test]
    fn verdicts_cover_every_source_error_class() {
        assert_eq!(BreakerVerdict::for_result(None), BreakerVerdict::Success);
        assert_eq!(BreakerVerdict::for_result(Some(&SourceError::NotFound)), BreakerVerdict::Success);
        for failure in [
            SourceError::Throttled,
            SourceError::Timeout,
            SourceError::Connect("refused".into()),
            SourceError::ServerError(500),
        ] {
            assert_eq!(BreakerVerdict::for_result(Some(&failure)), BreakerVerdict::Failure, "{failure:?}");
        }
        for neutral in [
            SourceError::AccessDenied,
            SourceError::Unsupported("sse-c".into()),
            SourceError::Other("x".into()),
        ] {
            assert_eq!(BreakerVerdict::for_result(Some(&neutral)), BreakerVerdict::Neutral, "{neutral:?}");
        }
    }

    #[test]
    fn state_labels_are_stable() {
        assert_eq!(BreakerState::Closed.as_str(), "closed");
        assert_eq!(BreakerState::Open.as_str(), "open");
        assert_eq!(BreakerState::HalfOpen.as_str(), "half_open");
        assert_eq!(serde_json::to_string(&BreakerState::HalfOpen).unwrap(), "\"half_open\"");
    }
}
