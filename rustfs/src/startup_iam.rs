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

use crate::app::context::{AppContext, get_global_app_context, init_global_app_context};
use crate::server::{ServiceStateManager, publish_ready_when_runtime_ready};
use rustfs_common::{GlobalReadiness, SystemStage};
use rustfs_ecstore::store::ECStore;
use rustfs_iam::init_iam_sys;
use rustfs_kms::KmsServiceManager;
use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const IAM_RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(5);
const IAM_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(30);
/// After this many retries (~5 min at initial interval), escalate log level to ERROR.
const IAM_RETRY_ESCALATION_THRESHOLD: u64 = 12;
const TEST_ENV_IAM_FAIL_INIT_ATTEMPTS: &str = "RUSTFS_TEST_IAM_FAIL_INIT_ATTEMPTS";
const TEST_ENV_IAM_RETRY_INTERVAL_MS: &str = "RUSTFS_TEST_IAM_RETRY_INTERVAL_MS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamBootstrapDisposition {
    ReadyInline,
    Deferred,
}

fn init_app_context_if_needed(store: Arc<ECStore>, kms_interface: Arc<KmsServiceManager>) -> bool {
    if get_global_app_context().is_some() {
        return false;
    }

    let Ok(iam_interface) = rustfs_iam::get() else {
        return false;
    };

    init_global_app_context(AppContext::with_default_interfaces(store, iam_interface, kms_interface));
    true
}

async fn finalize_iam_recovery(
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
) -> Result<()> {
    readiness.mark_stage(SystemStage::IamReady);
    let _ = init_app_context_if_needed(store, kms_interface);
    publish_ready_when_runtime_ready(readiness.as_ref(), state_manager.as_deref()).await
}

type RecoveryFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type RecoveryInitFn = Box<dyn FnMut() -> RecoveryFuture + Send>;
type RecoveryFinalizeFn = Box<dyn FnMut() -> RecoveryFuture + Send>;

fn compute_backoff_interval(attempt: u64, initial: Duration, max: Duration) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = 2u32.saturating_pow(exponent);
    let backoff = initial.saturating_mul(multiplier);
    if backoff > max { max } else { backoff }
}

fn spawn_iam_recovery_task(
    initial_interval: Duration,
    max_interval: Duration,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    mut init_fn: RecoveryInitFn,
    mut finalize_fn: RecoveryFinalizeFn,
) {
    tokio::spawn(async move {
        let mut attempts: u64 = 0;
        let degraded_since = std::time::Instant::now();

        // Phase 1: retry init until it succeeds
        loop {
            attempts += 1;
            let sleep_duration = compute_backoff_interval(attempts, initial_interval, max_interval);
            if let Some(token) = shutdown_token.as_ref() {
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(sleep_duration) => {}
                }
            } else {
                tokio::time::sleep(sleep_duration).await;
            }

            match init_fn().await {
                Ok(()) => {
                    let degraded_secs = degraded_since.elapsed().as_secs();
                    info!(
                        attempts,
                        degraded_duration_secs = degraded_secs,
                        "IAM bootstrap recovered after startup; publishing IAM readiness"
                    );
                    break;
                }
                Err(err) => {
                    let next_interval = compute_backoff_interval(attempts + 1, initial_interval, max_interval);
                    if attempts >= IAM_RETRY_ESCALATION_THRESHOLD {
                        error!(
                            attempts,
                            next_retry_secs = next_interval.as_secs(),
                            degraded_duration_secs = degraded_since.elapsed().as_secs(),
                            error = %err,
                            "IAM bootstrap retry failed after {} attempts; service remains degraded",
                            attempts
                        );
                    } else {
                        warn!(
                            attempts,
                            next_retry_secs = next_interval.as_secs(),
                            error = %err,
                            "IAM bootstrap retry failed; service remains degraded"
                        );
                    }
                }
            }
        }

        // Phase 2: retry finalize until readiness is published
        let mut finalize_attempts: u64 = 0;
        loop {
            finalize_attempts += 1;
            match finalize_fn().await {
                Ok(()) => {
                    info!(
                        init_attempts = attempts,
                        finalize_attempts,
                        degraded_duration_secs = degraded_since.elapsed().as_secs(),
                        "IAM readiness published successfully"
                    );
                    break;
                }
                Err(err) => {
                    let retry_interval = compute_backoff_interval(finalize_attempts, initial_interval, max_interval);
                    warn!(
                        finalize_attempts,
                        retry_secs = retry_interval.as_secs(),
                        error = %err,
                        "IAM recovered, but readiness publication failed; retrying"
                    );
                    if let Some(token) = shutdown_token.as_ref() {
                        tokio::select! {
                            _ = token.cancelled() => return,
                            _ = tokio::time::sleep(retry_interval) => {}
                        }
                    } else {
                        tokio::time::sleep(retry_interval).await;
                    }
                }
            }
        }
    });
}

fn initial_retry_interval() -> Duration {
    // Only honor the test override in debug builds to prevent accidental
    // production use via environment configuration.
    #[cfg(debug_assertions)]
    if let Some(ms) = rustfs_utils::get_env_opt_u64(TEST_ENV_IAM_RETRY_INTERVAL_MS) {
        return Duration::from_millis(ms);
    }
    IAM_RETRY_INITIAL_INTERVAL
}

// Sentinel marks "not yet initialized"; env var is read on first call.
static TEST_REMAINING_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);

fn should_fail_test_init_attempt() -> bool {
    // Only honor the test hook in debug builds to prevent accidental
    // production use via environment configuration.
    #[cfg(not(debug_assertions))]
    return false;

    #[cfg(debug_assertions)]
    {
        use std::sync::atomic::Ordering;

        let mut current = TEST_REMAINING_FAILURES.load(Ordering::SeqCst);
        if current == u64::MAX {
            let configured = rustfs_utils::get_env_opt_u64(TEST_ENV_IAM_FAIL_INIT_ATTEMPTS).unwrap_or(0);
            match TEST_REMAINING_FAILURES.compare_exchange(u64::MAX, configured, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => current = configured,
                Err(actual) => current = actual,
            }
        }

        while current > 0 {
            match TEST_REMAINING_FAILURES.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }

        false
    }
}

/// Reset the test failure counter so the next `should_fail_test_init_attempt`
/// call re-reads the environment variable by restoring the sentinel value.
/// Intended for use in integration tests that share a process.
#[doc(hidden)]
pub fn reset_test_failure_counter() {
    use std::sync::atomic::Ordering;
    TEST_REMAINING_FAILURES.store(u64::MAX, Ordering::SeqCst);
}

async fn attempt_init_iam_sys(store: Arc<ECStore>) -> std::result::Result<(), std::io::Error> {
    if should_fail_test_init_attempt() {
        return Err(std::io::Error::other("forced test IAM bootstrap failure"));
    }

    init_iam_sys(store)
        .await
        .map(|_| ())
        .map_err(|err| std::io::Error::other(err.to_string()))
}

/// Attempt IAM bootstrap at startup. If it fails, enter degraded mode and
/// spawn a background recovery task with exponential backoff.
///
/// This function always returns `Ok`. A failure to initialize IAM is not
/// fatal — the process continues in degraded mode where:
/// - `/health/ready` returns 503
/// - IAM-dependent operations return `IamSysNotInitialized`
/// - A background task retries IAM initialization until it succeeds
///
/// Returns `ReadyInline` if IAM initialized immediately, `Deferred` if
/// recovery is happening in the background.
pub async fn bootstrap_or_defer_iam_init(
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
) -> Result<IamBootstrapDisposition> {
    match attempt_init_iam_sys(store.clone()).await {
        Ok(()) => {
            readiness.mark_stage(SystemStage::IamReady);
            if !init_app_context_if_needed(store, kms_interface) {
                warn!("IAM bootstrap succeeded but app context initialization deferred");
            }
            return Ok(IamBootstrapDisposition::ReadyInline);
        }
        Err(err) => {
            let interval = initial_retry_interval();
            warn!(
                error = %err,
                initial_retry_secs = interval.as_secs(),
                max_retry_secs = IAM_RETRY_MAX_INTERVAL.as_secs(),
                escalation_threshold = IAM_RETRY_ESCALATION_THRESHOLD,
                "Initial IAM bootstrap failed; continuing startup in degraded mode until IAM recovers. \
                 Health endpoint will report 503 until IAM is ready."
            );

            let retry_store = store.clone();
            let retry_kms_interface = kms_interface.clone();
            let retry_readiness = readiness.clone();
            let retry_state_manager = state_manager.clone();

            spawn_iam_recovery_task(
                interval,
                IAM_RETRY_MAX_INTERVAL,
                shutdown_token,
                Box::new(move || {
                    let retry_store = retry_store.clone();
                    Box::pin(async move { attempt_init_iam_sys(retry_store).await })
                }),
                Box::new(move || {
                    let retry_store = store.clone();
                    let retry_kms_interface = retry_kms_interface.clone();
                    let retry_readiness = retry_readiness.clone();
                    let retry_state_manager = retry_state_manager.clone();
                    Box::pin(async move {
                        finalize_iam_recovery(retry_store, retry_kms_interface, retry_readiness, retry_state_manager).await
                    })
                }),
            );
        }
    }

    Ok(IamBootstrapDisposition::Deferred)
}

#[cfg(test)]
mod tests {
    use super::{
        IAM_RETRY_ESCALATION_THRESHOLD, IAM_RETRY_INITIAL_INTERVAL, IAM_RETRY_MAX_INTERVAL, compute_backoff_interval,
        spawn_iam_recovery_task,
    };
    use std::io::Error;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[test]
    fn iam_bootstrap_retry_intervals_are_non_zero_and_reasonable() {
        assert!(IAM_RETRY_INITIAL_INTERVAL > Duration::ZERO);
        assert_eq!(IAM_RETRY_INITIAL_INTERVAL.as_secs(), 5);
        assert!(IAM_RETRY_MAX_INTERVAL > IAM_RETRY_INITIAL_INTERVAL);
        assert_eq!(IAM_RETRY_MAX_INTERVAL.as_secs(), 30);
        const {
            assert!(IAM_RETRY_ESCALATION_THRESHOLD > 0);
            assert!(IAM_RETRY_ESCALATION_THRESHOLD == 12);
        }
    }

    #[test]
    fn compute_backoff_doubles_until_max() {
        let initial = Duration::from_secs(5);
        let max = Duration::from_secs(30);

        assert_eq!(compute_backoff_interval(1, initial, max), Duration::from_secs(5));
        assert_eq!(compute_backoff_interval(2, initial, max), Duration::from_secs(10));
        assert_eq!(compute_backoff_interval(3, initial, max), Duration::from_secs(20));
        assert_eq!(compute_backoff_interval(4, initial, max), Duration::from_secs(30));
        assert_eq!(compute_backoff_interval(5, initial, max), Duration::from_secs(30));
        assert_eq!(compute_backoff_interval(100, initial, max), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_task_retries_until_success_and_finalizes_once() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let finalize_calls = Arc::new(AtomicUsize::new(0));

        let attempts_for_init = attempts.clone();
        let finalize_for_assert = finalize_calls.clone();

        spawn_iam_recovery_task(
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            Box::new(move || {
                let attempts = attempts_for_init.clone();
                Box::pin(async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt < 2 {
                        Err(Error::other(format!("attempt {attempt} failed")))
                    } else {
                        Ok(())
                    }
                })
            }),
            Box::new(move || {
                let finalize_calls = finalize_calls.clone();
                Box::pin(async move {
                    finalize_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
        );

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 0);

        // First attempt after 5s (backoff: 5s)
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 0);

        // Second attempt after 10s (backoff: 5 * 2^1 = 10s)
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 1);

        // No more attempts after success
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_task_retries_finalize_when_finalize_fails_after_init() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let finalize_calls = Arc::new(AtomicUsize::new(0));

        let attempts_for_init = attempts.clone();
        let finalize_for_assert = finalize_calls.clone();

        spawn_iam_recovery_task(
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            Box::new(move || {
                let attempts = attempts_for_init.clone();
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
            Box::new(move || {
                let finalize_calls = finalize_calls.clone();
                Box::pin(async move {
                    let call = finalize_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if call < 3 {
                        Err(Error::other("finalize failed"))
                    } else {
                        Ok(())
                    }
                })
            }),
        );

        tokio::task::yield_now().await;

        // Init succeeds after first 5s sleep
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 1);

        // Finalize retried after 5s backoff (attempt 1 failed)
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 2);

        // Finalize retried after 10s backoff (attempt 2 failed)
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 3);

        // No more retries after finalize succeeds
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert_eq!(finalize_for_assert.load(Ordering::SeqCst), 3);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_task_uses_exponential_backoff_on_repeated_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_init = attempts.clone();

        spawn_iam_recovery_task(
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            Box::new(move || {
                let attempts = attempts_for_init.clone();
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(Error::other("persistent failure"))
                })
            }),
            Box::new(|| Box::pin(async { Ok(()) })),
        );

        tokio::task::yield_now().await;

        // Attempt 1: after 5s (backoff for attempt 1)
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // Attempt 2: after 10s (backoff for attempt 2)
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        // Attempt 3: after 20s (backoff for attempt 3)
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // Attempt 4: after 30s (capped at max)
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 4);

        // Attempt 5: still 30s (capped)
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
    }
}
