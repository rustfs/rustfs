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

//! Operation execution policy for external KMS backend calls.
//!
//! Vault-backed operations leave the process boundary, so every call needs a
//! per-attempt timeout, a total operation deadline, and classification-driven
//! bounded retries. The Vault backends and the credential provider wire every
//! outbound `vaultrs` call through [`execute`].
//!
//! Retry safety is driven by two orthogonal classifications:
//! - [`OpClass`] states whether replaying the operation is safe at all.
//! - [`ErrorClass`] states whether the observed failure is worth replaying.
//!
//! Mutating operations without an idempotency key or CAS precondition are never
//! retried automatically: a response lost after the server applied the write
//! would otherwise be replayed into duplicate side effects (extra key versions,
//! repeated deletes).
//!
//! Every execution also records operation metrics (attempt failures by retry
//! class, terminal outcome, attempts used, wall-clock duration) through the
//! process-global `metrics` recorder. Metric labels carry only static enum
//! values — operation names, classes, outcomes — never key identifiers, key
//! material, ciphertext, or tokens.

use std::future::Future;
use std::time::Duration;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::KmsConfig;
use crate::error::{KmsError, Result};

/// Default backoff cap before the first retry; doubles per retry.
const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Default upper bound for a single backoff sleep.
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Replay safety of a backend operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpClass {
    /// No persistent side effects (encrypt/decrypt/generate/describe/list/health);
    /// safe to retry on any retryable failure.
    ReadIdempotent,
    /// External write without an idempotency key or CAS precondition
    /// (create/rotate/delete/configure); executed at most once.
    MutatingNonIdempotent,
    /// Authentication exchange (login, token renewal); safe to resend.
    Auth,
}

impl OpClass {
    fn retryable(self) -> bool {
        !matches!(self, OpClass::MutatingNonIdempotent)
    }
}

/// Retry-relevant classification of a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    /// Connection-level failure (connect/send/response read); the server may or
    /// may not have observed the request.
    RetryableConn,
    /// Retryable HTTP status: 429 throttling or a recoverable 5xx.
    RetryableStatus,
    /// Deterministic failure (auth, validation, not-found, malformed data);
    /// retrying cannot help and may mask the real problem.
    Fatal,
}

/// Classify a `vaultrs` client error for retry purposes.
///
/// Status codes are inspected both on `ClientError::APIError` (JSON error body)
/// and on a wrapped rustify `ServerResponseError` (non-JSON body, e.g. an HTML
/// page from an intermediate load balancer). Everything that is not throttling,
/// a recoverable 5xx, or a connection-level failure is fatal; in particular
/// 400/401/403/404 must never be retried.
pub(crate) fn classify_vaultrs(error: &vaultrs::error::ClientError) -> ErrorClass {
    use rustify::errors::ClientError as RestError;
    use vaultrs::error::ClientError;

    match error {
        ClientError::APIError { code, .. } => classify_status(*code),
        ClientError::RestClientError { source } => match source {
            RestError::ServerResponseError { code, .. } => classify_status(*code),
            RestError::RequestError { .. } | RestError::ResponseError { .. } => ErrorClass::RetryableConn,
            _ => ErrorClass::Fatal,
        },
        _ => ErrorClass::Fatal,
    }
}

fn classify_status(code: u16) -> ErrorClass {
    match code {
        429 | 500 | 502 | 503 | 504 => ErrorClass::RetryableStatus,
        _ => ErrorClass::Fatal,
    }
}

/// Failure of a single attempt, carrying its retry classification.
#[derive(Debug)]
pub(crate) struct AttemptError {
    pub(crate) class: ErrorClass,
    pub(crate) error: KmsError,
}

impl AttemptError {
    /// A failure that must never be retried, regardless of operation class.
    pub(crate) fn fatal(error: KmsError) -> Self {
        Self {
            class: ErrorClass::Fatal,
            error,
        }
    }

    /// Classify a `vaultrs` failure and map it onto a domain error.
    ///
    /// Classification reads the raw error before `map` consumes it, so call
    /// sites keep their site-specific error mapping (404 to key-not-found and
    /// so on) without losing the status code the retry decision needs.
    pub(crate) fn from_vaultrs(
        error: vaultrs::error::ClientError,
        map: impl FnOnce(vaultrs::error::ClientError) -> KmsError,
    ) -> Self {
        let class = classify_vaultrs(&error);
        Self {
            class,
            error: map(error),
        }
    }
}

/// Budgets applied by [`execute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    /// Upper bound for one backend attempt.
    pub(crate) attempt_timeout: Duration,
    /// Upper bound for the whole operation, including retries and backoff.
    pub(crate) op_deadline: Duration,
    /// Maximum attempts for retryable operation classes; non-idempotent
    /// mutations always run exactly once regardless of this value.
    pub(crate) max_attempts: u32,
    /// Backoff cap before the first retry; doubles per retry.
    pub(crate) base_backoff: Duration,
    /// Upper bound for a single backoff sleep.
    pub(crate) max_backoff: Duration,
}

impl RetryPolicy {
    /// Derive the policy from the KMS configuration.
    ///
    /// `timeout` and `retry_attempts` are taken with the config-level clamps
    /// applied. The operation deadline covers the worst-case budget of all
    /// attempts plus backoff, so it bounds runaway loops without cutting any
    /// attempt short; an independently configurable deadline is left to the
    /// admin-API follow-up.
    pub(crate) fn from_config(config: &KmsConfig) -> Self {
        let mut policy = Self {
            attempt_timeout: config.effective_timeout(),
            op_deadline: Duration::ZERO,
            max_attempts: config.effective_retry_attempts(),
            base_backoff: DEFAULT_BASE_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        };
        policy.op_deadline = policy.worst_case_budget();
        policy
    }

    /// Total worst-case duration: every attempt hits `attempt_timeout` and
    /// every backoff sleeps its full cap.
    fn worst_case_budget(&self) -> Duration {
        let attempts = self.max_attempts.max(1);
        let mut budget = self.attempt_timeout.saturating_mul(attempts);
        for completed in 1..attempts {
            budget = budget.saturating_add(backoff_cap(self, completed));
        }
        budget
    }
}

/// Exponential backoff cap after `completed_attempts` failed attempts.
fn backoff_cap(policy: &RetryPolicy, completed_attempts: u32) -> Duration {
    let doublings = completed_attempts.saturating_sub(1).min(31);
    policy.base_backoff.saturating_mul(1u32 << doublings).min(policy.max_backoff)
}

/// Equal jitter: sleep within `[cap / 2, cap]`, keeping at least half the cap
/// so backoff still backs off while decorrelating retry bursts.
fn equal_jitter(rng: &mut impl RngExt, cap: Duration) -> Duration {
    let half = cap / 2;
    let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
    half + Duration::from_nanos(rng.random_range(0..=spread))
}

// ---------------------------------------------------------------------------
// Metrics
//
// Every execution is recorded here, at the single choke point all backend
// calls flow through, so instrumenting a new call site costs nothing beyond
// naming its operation. Label values are exclusively static enum strings
// (operation names, classes, outcomes) — key identifiers, key material,
// ciphertext, and tokens must never reach a metric label.
// ---------------------------------------------------------------------------

/// Counter: operations executed, by `operation`, `op_class`, and `outcome`.
const METRIC_OPERATIONS_TOTAL: &str = "rustfs_kms_backend_operations_total";
/// Counter: failed attempts, by `operation` and `error_class` (including
/// `attempt_timeout` for attempts cut off by the per-attempt timeout).
const METRIC_ATTEMPT_FAILURES_TOTAL: &str = "rustfs_kms_backend_attempt_failures_total";
/// Histogram: wall-clock duration of a whole operation (attempts plus
/// backoff), in seconds, by `operation` and `outcome`.
const METRIC_OPERATION_DURATION_SECONDS: &str = "rustfs_kms_backend_operation_duration_seconds";
/// Histogram: attempts one operation used before completing, by `operation`
/// and `outcome`.
const METRIC_OPERATION_ATTEMPTS: &str = "rustfs_kms_backend_operation_attempts";

impl OpClass {
    fn as_label(self) -> &'static str {
        match self {
            OpClass::ReadIdempotent => "read_idempotent",
            OpClass::MutatingNonIdempotent => "mutating_non_idempotent",
            OpClass::Auth => "auth",
        }
    }
}

impl ErrorClass {
    fn as_label(self) -> &'static str {
        match self {
            ErrorClass::RetryableConn => "retryable_conn",
            ErrorClass::RetryableStatus => "retryable_status",
            ErrorClass::Fatal => "fatal",
        }
    }
}

/// How one policy execution terminated, for the `outcome` metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Success,
    /// A fatal-classified failure ended the operation on its first observation.
    Fatal,
    /// The attempt budget ran out; the last failure was retryable (including a
    /// timed-out final attempt).
    BudgetExhausted,
    /// The operation deadline ran out before another attempt could complete.
    DeadlineExceeded,
    Cancelled,
}

impl Outcome {
    fn as_label(self) -> &'static str {
        match self {
            Outcome::Success => "success",
            Outcome::Fatal => "fatal",
            Outcome::BudgetExhausted => "budget_exhausted",
            Outcome::DeadlineExceeded => "deadline_exceeded",
            Outcome::Cancelled => "cancelled",
        }
    }
}

/// Register metric descriptions once per process.
fn describe_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_counter!(
            METRIC_OPERATIONS_TOTAL,
            "Total KMS backend operations executed under the operation policy, by operation, operation class, and outcome"
        );
        metrics::describe_counter!(
            METRIC_ATTEMPT_FAILURES_TOTAL,
            "Total failed KMS backend attempts, by operation and retry classification"
        );
        metrics::describe_histogram!(
            METRIC_OPERATION_DURATION_SECONDS,
            "Wall-clock duration of KMS backend operations including retries and backoff, in seconds"
        );
        metrics::describe_histogram!(
            METRIC_OPERATION_ATTEMPTS,
            "Number of attempts a KMS backend operation used before completing"
        );
    });
}

/// Record one failed attempt with its retry classification.
fn record_attempt_failure(operation: &'static str, error_class: &'static str) {
    metrics::counter!(
        METRIC_ATTEMPT_FAILURES_TOTAL,
        "operation" => operation,
        "error_class" => error_class
    )
    .increment(1);
}

/// Record the terminal outcome of one policy execution.
fn record_operation(operation: &'static str, class: OpClass, outcome: Outcome, attempts: u32, elapsed: Duration) {
    metrics::counter!(
        METRIC_OPERATIONS_TOTAL,
        "operation" => operation,
        "op_class" => class.as_label(),
        "outcome" => outcome.as_label()
    )
    .increment(1);
    metrics::histogram!(
        METRIC_OPERATION_DURATION_SECONDS,
        "operation" => operation,
        "outcome" => outcome.as_label()
    )
    .record(elapsed.as_secs_f64());
    metrics::histogram!(
        METRIC_OPERATION_ATTEMPTS,
        "operation" => operation,
        "outcome" => outcome.as_label()
    )
    .record(f64::from(attempts));
}

/// Run `attempt` under the policy.
///
/// Each attempt is bounded by `attempt_timeout` (further capped by whatever is
/// left of `op_deadline`), and retryable failures are replayed with exponential
/// backoff and jitter when the operation class allows it. Cancellation aborts
/// both in-flight attempts and backoff sleeps.
///
/// A timed-out attempt counts as a connection-class failure: the server may
/// have processed the request, which is exactly why non-idempotent mutations
/// are never replayed.
pub(crate) async fn execute<T, F, Fut>(
    operation: &'static str,
    class: OpClass,
    policy: &RetryPolicy,
    cancel: &CancellationToken,
    attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, AttemptError>>,
{
    // Seed an owned RNG up front: the thread-local RNG is not Send and must
    // not be held across await points.
    let mut rng = StdRng::from_rng(&mut rand::rng());
    execute_with_jitter(operation, class, policy, cancel, move |cap| equal_jitter(&mut rng, cap), attempt).await
}

/// [`execute`] with an injectable jitter source so tests can pin deterministic
/// backoff durations instead of asserting around random sleeps.
pub(crate) async fn execute_with_jitter<T, F, Fut, J>(
    operation: &'static str,
    class: OpClass,
    policy: &RetryPolicy,
    cancel: &CancellationToken,
    jitter: J,
    attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, AttemptError>>,
    J: FnMut(Duration) -> Duration,
{
    describe_metrics();
    let started = Instant::now();
    let mut attempts_made = 0u32;
    let (outcome, result) = drive_attempts(operation, class, policy, cancel, jitter, attempt, &mut attempts_made).await;
    record_operation(operation, class, outcome, attempts_made, started.elapsed());
    result
}

/// The attempt loop behind [`execute_with_jitter`], returning the terminal
/// outcome alongside the result so the caller can record it exactly once.
async fn drive_attempts<T, F, Fut, J>(
    operation: &'static str,
    class: OpClass,
    policy: &RetryPolicy,
    cancel: &CancellationToken,
    mut jitter: J,
    mut attempt: F,
    attempts_made: &mut u32,
) -> (Outcome, Result<T>)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, AttemptError>>,
    J: FnMut(Duration) -> Duration,
{
    let deadline = Instant::now() + policy.op_deadline;
    let max_attempts = if class.retryable() { policy.max_attempts.max(1) } else { 1 };

    let mut attempt_no = 0u32;
    loop {
        if cancel.is_cancelled() {
            return (
                Outcome::Cancelled,
                Err(KmsError::operation_cancelled(format!(
                    "{operation} cancelled before attempt {}",
                    attempt_no + 1
                ))),
            );
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return (
                Outcome::DeadlineExceeded,
                Err(KmsError::operation_timed_out(format!(
                    "{operation} exceeded operation deadline of {:?}",
                    policy.op_deadline
                ))),
            );
        }

        attempt_no += 1;
        *attempts_made = attempt_no;
        let attempt_budget = policy.attempt_timeout.min(remaining);
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return (
                    Outcome::Cancelled,
                    Err(KmsError::operation_cancelled(format!("{operation} cancelled during attempt {attempt_no}"))),
                );
            }
            outcome = tokio::time::timeout(attempt_budget, attempt()) => outcome,
        };

        let failure = match outcome {
            Ok(Ok(value)) => return (Outcome::Success, Ok(value)),
            Ok(Err(failure)) => {
                record_attempt_failure(operation, failure.class.as_label());
                failure
            }
            Err(_) => {
                record_attempt_failure(operation, "attempt_timeout");
                AttemptError {
                    class: ErrorClass::RetryableConn,
                    error: KmsError::operation_timed_out(format!(
                        "{operation} attempt {attempt_no} timed out after {attempt_budget:?}"
                    )),
                }
            }
        };

        if failure.class == ErrorClass::Fatal {
            return (Outcome::Fatal, Err(failure.error));
        }
        if attempt_no >= max_attempts {
            return (Outcome::BudgetExhausted, Err(failure.error));
        }

        let backoff = jitter(backoff_cap(policy, attempt_no));
        if backoff >= deadline.saturating_duration_since(Instant::now()) {
            // Not enough deadline budget left for another attempt.
            return (Outcome::DeadlineExceeded, Err(failure.error));
        }
        tracing::warn!(
            operation,
            attempt = attempt_no,
            error_class = ?failure.class,
            backoff = ?backoff,
            "KMS backend attempt failed with a retryable error; backing off before retry"
        );
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return (
                    Outcome::Cancelled,
                    Err(KmsError::operation_cancelled(format!("{operation} cancelled during retry backoff"))),
                );
            }
            _ = tokio::time::sleep(backoff) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    type AttemptResult<T> = std::result::Result<T, AttemptError>;

    fn policy_of(attempt_timeout_ms: u64, op_deadline_ms: u64, max_attempts: u32, base_ms: u64, max_ms: u64) -> RetryPolicy {
        RetryPolicy {
            attempt_timeout: Duration::from_millis(attempt_timeout_ms),
            op_deadline: Duration::from_millis(op_deadline_ms),
            max_attempts,
            base_backoff: Duration::from_millis(base_ms),
            max_backoff: Duration::from_millis(max_ms),
        }
    }

    /// Deterministic jitter: always sleep the full backoff cap.
    fn full_jitter(cap: Duration) -> Duration {
        cap
    }

    fn retryable_conn_error() -> AttemptError {
        AttemptError {
            class: ErrorClass::RetryableConn,
            error: KmsError::backend_error("connection reset by peer"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hung_attempt_fails_within_attempt_timeout() {
        let policy = policy_of(5_000, 60_000, 1, 100, 2_000);
        let cancel = CancellationToken::new();
        let started = Instant::now();

        let result: Result<()> = execute_with_jitter("encrypt", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, || {
            std::future::pending::<AttemptResult<()>>()
        })
        .await;

        assert!(matches!(result, Err(KmsError::OperationTimedOut { .. })), "got {result:?}");
        assert_eq!(started.elapsed(), Duration::from_millis(5_000));
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_status_retries_until_success() {
        let policy = policy_of(1_000, 60_000, 3, 100, 2_000);
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();
        let started = Instant::now();

        let result =
            execute_with_jitter("generate_data_key", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                let calls = calls_in_attempt.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(AttemptError {
                            class: ErrorClass::RetryableStatus,
                            error: KmsError::backend_error("throttled (429)"),
                        })
                    } else {
                        Ok(7u32)
                    }
                }
            })
            .await;

        assert_eq!(result.expect("retries within budget must succeed"), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // Full-cap backoff: 100ms after attempt 1, 200ms after attempt 2.
        assert_eq!(started.elapsed(), Duration::from_millis(300));
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_error_is_not_retried() {
        let policy = policy_of(1_000, 60_000, 5, 100, 2_000);
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();
        let started = Instant::now();

        let result: Result<()> =
            execute_with_jitter("decrypt", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                let calls = calls_in_attempt.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(AttemptError {
                        class: ErrorClass::Fatal,
                        error: KmsError::access_denied("permission denied (403)"),
                    })
                }
            })
            .await;

        assert!(matches!(result, Err(KmsError::AccessDenied { .. })), "got {result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn mutating_non_idempotent_runs_exactly_once() {
        let policy = policy_of(1_000, 60_000, 5, 100, 2_000);
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();

        let result: Result<()> =
            execute_with_jitter("rotate_key", OpClass::MutatingNonIdempotent, &policy, &cancel, full_jitter, move || {
                let calls = calls_in_attempt.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(retryable_conn_error())
                }
            })
            .await;

        // Even a retryable failure must not replay a non-idempotent mutation.
        assert!(matches!(result, Err(KmsError::BackendError { .. })), "got {result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_aborts_backoff_immediately() {
        // Long backoff (10s cap) so a prompt return can only come from cancellation.
        let policy = policy_of(1_000, 600_000, 5, 10_000, 10_000);
        let cancel = CancellationToken::new();
        let canceller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            canceller.cancel();
        });
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();
        let started = Instant::now();

        let result: Result<()> =
            execute_with_jitter("decrypt", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                let calls = calls_in_attempt.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(retryable_conn_error())
                }
            })
            .await;

        assert!(matches!(result, Err(KmsError::OperationCancelled { .. })), "got {result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(started.elapsed(), Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_token_short_circuits_before_first_attempt() {
        let policy = policy_of(1_000, 60_000, 5, 100, 2_000);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();

        let result: Result<()> =
            execute_with_jitter("list_keys", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                let calls = calls_in_attempt.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;

        assert!(matches!(result, Err(KmsError::OperationCancelled { .. })), "got {result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn total_duration_never_exceeds_deadline() {
        // Worst case without a deadline would be 5 * 10s + backoff; the 25s
        // deadline must cut both the attempt count and the final attempt short.
        let policy = policy_of(10_000, 25_000, 5, 100, 2_000);
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();
        let started = Instant::now();

        let result: Result<()> =
            execute_with_jitter("describe_key", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                calls_in_attempt.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<AttemptResult<()>>()
            })
            .await;

        assert!(matches!(result, Err(KmsError::OperationTimedOut { .. })), "got {result:?}");
        // attempt 1: 10s + 100ms backoff; attempt 2: 10s + 200ms backoff;
        // attempt 3 runs with the residual 4.7s budget, then the deadline is
        // exhausted and no further backoff or attempt happens.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(started.elapsed(), Duration::from_millis(25_000));
    }

    #[tokio::test(start_paused = true)]
    async fn default_jitter_stays_within_equal_jitter_bounds() {
        let policy = policy_of(1_000, 60_000, 3, 100, 2_000);
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in_attempt = calls.clone();
        let started = Instant::now();

        let result = execute("health_check", OpClass::ReadIdempotent, &policy, &cancel, move || {
            let calls = calls_in_attempt.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(retryable_conn_error())
                } else {
                    Ok(())
                }
            }
        })
        .await;

        result.expect("retries within budget must succeed");
        let elapsed = started.elapsed();
        // Equal jitter sleeps within [cap / 2, cap]; caps are 100ms then 200ms.
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(300),
            "elapsed {elapsed:?} outside equal-jitter bounds"
        );
    }

    #[test]
    fn equal_jitter_is_seed_deterministic_and_bounded() {
        let caps = [Duration::from_millis(100), Duration::from_millis(250), Duration::from_secs(2)];
        let mut first = StdRng::seed_from_u64(1569);
        let mut second = StdRng::seed_from_u64(1569);
        for cap in caps {
            let a = equal_jitter(&mut first, cap);
            let b = equal_jitter(&mut second, cap);
            assert_eq!(a, b, "same seed must yield the same jitter sequence");
            assert!(a >= cap / 2 && a <= cap, "jitter {a:?} outside [{:?}, {cap:?}]", cap / 2);
        }
    }

    #[test]
    fn backoff_caps_double_up_to_max() {
        let policy = policy_of(1_000, 60_000, 6, 100, 700);
        let caps: Vec<Duration> = (1..=5).map(|completed| backoff_cap(&policy, completed)).collect();
        assert_eq!(
            caps,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(700),
                Duration::from_millis(700),
            ]
        );
    }

    #[test]
    fn retry_policy_from_config_applies_clamps() {
        let config = KmsConfig {
            timeout: Duration::from_secs(3_600),
            retry_attempts: 50,
            ..KmsConfig::default()
        };
        let policy = RetryPolicy::from_config(&config);
        assert_eq!(policy.attempt_timeout, Duration::from_secs(300));
        assert_eq!(policy.max_attempts, 10);
        // The deadline must cover the full worst case so it never cuts a
        // policy-conformant operation short.
        assert!(policy.op_deadline >= policy.attempt_timeout.saturating_mul(policy.max_attempts));

        let in_range = KmsConfig::default();
        let policy = RetryPolicy::from_config(&in_range);
        assert_eq!(policy.attempt_timeout, in_range.timeout);
        assert_eq!(policy.max_attempts, in_range.retry_attempts);
    }

    #[test]
    fn classify_vaultrs_matrix() {
        use rustify::errors::ClientError as RestError;
        use vaultrs::error::ClientError;

        let api = |code: u16| ClientError::APIError { code, errors: vec![] };
        for code in [429u16, 500, 502, 503, 504] {
            assert_eq!(classify_vaultrs(&api(code)), ErrorClass::RetryableStatus, "status {code}");
        }
        for code in [400u16, 401, 403, 404, 405, 412, 501] {
            assert_eq!(classify_vaultrs(&api(code)), ErrorClass::Fatal, "status {code}");
        }

        // Status errors whose body could not be parsed stay wrapped in the
        // rustify error; classification must still see the code.
        let raw_status = |code: u16| ClientError::RestClientError {
            source: RestError::ServerResponseError { code, content: None },
        };
        assert_eq!(classify_vaultrs(&raw_status(503)), ErrorClass::RetryableStatus);
        assert_eq!(classify_vaultrs(&raw_status(403)), ErrorClass::Fatal);

        let send_failure = ClientError::RestClientError {
            source: RestError::RequestError {
                source: anyhow::anyhow!("connection refused"),
                url: "http://127.0.0.1:8200/v1/sys/health".to_string(),
                method: "GET".to_string(),
            },
        };
        assert_eq!(classify_vaultrs(&send_failure), ErrorClass::RetryableConn);

        let read_failure = ClientError::RestClientError {
            source: RestError::ResponseError {
                source: anyhow::anyhow!("connection reset by peer"),
            },
        };
        assert_eq!(classify_vaultrs(&read_failure), ErrorClass::RetryableConn);

        let malformed = ClientError::JsonParseError {
            source: serde_json::from_str::<serde_json::Value>("{").expect_err("truncated JSON must not parse"),
        };
        assert_eq!(classify_vaultrs(&malformed), ErrorClass::Fatal);
        assert_eq!(classify_vaultrs(&ClientError::ResponseEmptyError), ErrorClass::Fatal);
        assert_eq!(classify_vaultrs(&ClientError::InvalidLoginMethodError), ErrorClass::Fatal);
    }

    // -- Metric emission ----------------------------------------------------
    //
    // Each test installs a thread-local debugging recorder and drives a
    // paused-clock current-thread runtime inside it, so the emitted metrics
    // (including virtual-clock durations) are fully deterministic.

    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    type MetricEntry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Run `test` on a paused current-thread runtime under a debugging
    /// recorder and return one snapshot of everything it emitted.
    ///
    /// A single snapshot per test on purpose: `Snapshotter::snapshot` drains
    /// the recorded state, so taking it per assertion would only show the
    /// first assertion any data.
    fn record_metrics<Out>(test: impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Out>>>) -> (Vec<MetricEntry>, Out) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let out = metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .expect("current-thread runtime must build");
            runtime.block_on(test())
        });
        (snapshotter.snapshot().into_vec(), out)
    }

    fn labels_match(key: &metrics::Key, labels: &[(&str, &str)]) -> bool {
        labels
            .iter()
            .all(|(label, expected)| key.labels().any(|l| l.key() == *label && l.value() == *expected))
    }

    fn counter_value(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> u64 {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Counter
                    && composite.key().name() == name
                    && labels_match(composite.key(), labels);
                match (matches, value) {
                    (true, DebugValue::Counter(count)) => Some(*count),
                    _ => None,
                }
            })
            .sum()
    }

    fn histogram_values(snapshot: &[MetricEntry], name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
        snapshot
            .iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Histogram
                    && composite.key().name() == name
                    && labels_match(composite.key(), labels);
                match (matches, value) {
                    (true, DebugValue::Histogram(values)) => Some(values),
                    _ => None,
                }
            })
            .flatten()
            .map(|value| value.into_inner())
            .collect()
    }

    #[test]
    fn metrics_record_retried_success_with_attempts_and_duration() {
        let calls_in_test = Arc::new(AtomicU32::new(0));
        let (snapshot, ()) = record_metrics(move || {
            Box::pin(async move {
                let policy = policy_of(1_000, 60_000, 3, 100, 2_000);
                let cancel = CancellationToken::new();
                let calls_in_attempt = calls_in_test.clone();
                execute_with_jitter("metrics_read", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                    let calls = calls_in_attempt.clone();
                    async move {
                        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                            Err(AttemptError {
                                class: ErrorClass::RetryableStatus,
                                error: KmsError::backend_error("throttled (429)"),
                            })
                        } else {
                            Ok(())
                        }
                    }
                })
                .await
                .expect("retries within budget must succeed");
            })
        });

        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_OPERATIONS_TOTAL,
                &[
                    ("operation", "metrics_read"),
                    ("op_class", "read_idempotent"),
                    ("outcome", "success")
                ]
            ),
            1
        );
        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_ATTEMPT_FAILURES_TOTAL,
                &[("operation", "metrics_read"), ("error_class", "retryable_status")]
            ),
            2
        );
        assert_eq!(
            histogram_values(
                &snapshot,
                METRIC_OPERATION_ATTEMPTS,
                &[("operation", "metrics_read"), ("outcome", "success")]
            ),
            vec![3.0]
        );
        // Full-cap backoffs of 100ms and 200ms on the paused clock.
        let durations = histogram_values(
            &snapshot,
            METRIC_OPERATION_DURATION_SECONDS,
            &[("operation", "metrics_read"), ("outcome", "success")],
        );
        assert_eq!(durations.len(), 1);
        assert!((durations[0] - 0.3).abs() < 1e-9, "expected 0.3s of virtual backoff, got {durations:?}");
    }

    #[test]
    fn metrics_record_fatal_outcome_with_single_attempt() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async {
                let policy = policy_of(1_000, 60_000, 5, 100, 2_000);
                let cancel = CancellationToken::new();
                let result: Result<()> =
                    execute_with_jitter("metrics_fatal", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, || async {
                        Err(AttemptError {
                            class: ErrorClass::Fatal,
                            error: KmsError::access_denied("permission denied (403)"),
                        })
                    })
                    .await;
                result.expect_err("a fatal failure must end the operation");
            })
        });

        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_OPERATIONS_TOTAL,
                &[("operation", "metrics_fatal"), ("outcome", "fatal")]
            ),
            1
        );
        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_ATTEMPT_FAILURES_TOTAL,
                &[("operation", "metrics_fatal"), ("error_class", "fatal")]
            ),
            1
        );
        assert_eq!(
            histogram_values(
                &snapshot,
                METRIC_OPERATION_ATTEMPTS,
                &[("operation", "metrics_fatal"), ("outcome", "fatal")]
            ),
            vec![1.0]
        );
    }

    #[test]
    fn metrics_record_mutating_budget_exhausted_after_one_attempt() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async {
                let policy = policy_of(1_000, 60_000, 5, 100, 2_000);
                let cancel = CancellationToken::new();
                let result: Result<()> = execute_with_jitter(
                    "metrics_rotate",
                    OpClass::MutatingNonIdempotent,
                    &policy,
                    &cancel,
                    full_jitter,
                    || async { Err(retryable_conn_error()) },
                )
                .await;
                result.expect_err("a mutating operation must not retry a retryable failure");
            })
        });

        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_OPERATIONS_TOTAL,
                &[
                    ("operation", "metrics_rotate"),
                    ("op_class", "mutating_non_idempotent"),
                    ("outcome", "budget_exhausted")
                ]
            ),
            1
        );
        assert_eq!(
            histogram_values(
                &snapshot,
                METRIC_OPERATION_ATTEMPTS,
                &[("operation", "metrics_rotate"), ("outcome", "budget_exhausted")]
            ),
            vec![1.0]
        );
    }

    #[test]
    fn metrics_record_timeouts_and_deadline_outcome() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async {
                // Hung attempts: 10s each against a 25s deadline (see
                // total_duration_never_exceeds_deadline for the timeline).
                let policy = policy_of(10_000, 25_000, 5, 100, 2_000);
                let cancel = CancellationToken::new();
                let result: Result<()> =
                    execute_with_jitter("metrics_hung", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, || {
                        std::future::pending::<AttemptResult<()>>()
                    })
                    .await;
                result.expect_err("hung attempts must exhaust the deadline");
            })
        });

        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_OPERATIONS_TOTAL,
                &[("operation", "metrics_hung"), ("outcome", "deadline_exceeded")]
            ),
            1
        );
        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_ATTEMPT_FAILURES_TOTAL,
                &[("operation", "metrics_hung"), ("error_class", "attempt_timeout")]
            ),
            3
        );
        let durations = histogram_values(
            &snapshot,
            METRIC_OPERATION_DURATION_SECONDS,
            &[("operation", "metrics_hung"), ("outcome", "deadline_exceeded")],
        );
        assert_eq!(durations.len(), 1);
        assert!((durations[0] - 25.0).abs() < 1e-9, "expected the full 25s deadline, got {durations:?}");
    }

    #[test]
    fn metrics_record_cancelled_outcome() {
        let calls_in_test = Arc::new(AtomicU32::new(0));
        let (snapshot, ()) = record_metrics(move || {
            Box::pin(async move {
                let policy = policy_of(1_000, 600_000, 5, 10_000, 10_000);
                let cancel = CancellationToken::new();
                let canceller = cancel.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    canceller.cancel();
                });
                let calls_in_attempt = calls_in_test.clone();
                let result: Result<()> =
                    execute_with_jitter("metrics_cancel", OpClass::ReadIdempotent, &policy, &cancel, full_jitter, move || {
                        let calls = calls_in_attempt.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Err(retryable_conn_error())
                        }
                    })
                    .await;
                result.expect_err("cancellation must abort the backoff");
            })
        });

        assert_eq!(
            counter_value(
                &snapshot,
                METRIC_OPERATIONS_TOTAL,
                &[("operation", "metrics_cancel"), ("outcome", "cancelled")]
            ),
            1
        );
        assert_eq!(
            histogram_values(
                &snapshot,
                METRIC_OPERATION_ATTEMPTS,
                &[("operation", "metrics_cancel"), ("outcome", "cancelled")]
            ),
            vec![1.0]
        );
    }
}
