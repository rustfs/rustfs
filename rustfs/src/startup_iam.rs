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

use crate::app::context::AppContext;
use crate::server::{ServiceStateManager, publish_ready_when_runtime_ready};
use crate::storage_compat::ECStore;
use rustfs_common::{GlobalReadiness, SystemStage};
use rustfs_iam::init_iam_sys;
use rustfs_kms::KmsServiceManager;
use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const LOG_COMPONENT_STARTUP_IAM: &str = "startup_iam";
const LOG_SUBSYSTEM_BOOTSTRAP: &str = "bootstrap";
const EVENT_IAM_BOOTSTRAP_RECOVERED: &str = "iam_bootstrap_recovered";
const EVENT_IAM_BOOTSTRAP_RETRY_FAILED: &str = "iam_bootstrap_retry_failed";
const EVENT_IAM_READINESS_PUBLISHED: &str = "iam_readiness_published";
const EVENT_IAM_READINESS_PUBLICATION_FAILED: &str = "iam_readiness_publication_failed";
const EVENT_IAM_BOOTSTRAP_DEFERRED: &str = "iam_bootstrap_deferred";

const IAM_RETRY_INITIAL_INTERVAL: Duration = Duration::from_secs(5);
const IAM_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(30);
/// After this many retries (~5 min at initial interval), escalate log level to ERROR.
const IAM_RETRY_ESCALATION_THRESHOLD: u64 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamBootstrapDisposition {
    ReadyInline,
    Deferred,
}

pub async fn publish_ready_for_iam_bootstrap(
    disposition: IamBootstrapDisposition,
    readiness: &GlobalReadiness,
    state_manager: Option<&ServiceStateManager>,
) -> Result<bool> {
    publish_ready_for_iam_bootstrap_with(disposition, || async move {
        publish_ready_when_runtime_ready(readiness, state_manager).await
    })
    .await
}

async fn publish_ready_for_iam_bootstrap_with<PublishFn, PublishFuture>(
    disposition: IamBootstrapDisposition,
    publish_ready: PublishFn,
) -> Result<bool>
where
    PublishFn: FnOnce() -> PublishFuture,
    PublishFuture: Future<Output = Result<()>>,
{
    if disposition == IamBootstrapDisposition::ReadyInline {
        publish_ready().await?;
        return Ok(true);
    }

    Ok(false)
}

async fn finalize_iam_recovery(
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
) -> Result<()> {
    AppContext::ensure_startup_after_iam(store, kms_interface)?;

    readiness.mark_stage(SystemStage::IamReady);
    publish_ready_when_runtime_ready(readiness.as_ref(), state_manager.as_deref()).await
}

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
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
) {
    let init_store = store.clone();
    let finalize_store = store;
    let finalize_kms_interface = kms_interface;
    let finalize_readiness = readiness;
    let finalize_state_manager = state_manager;
    tokio::spawn(async move {
        run_iam_recovery_loop(
            initial_interval,
            max_interval,
            shutdown_token,
            move || {
                let store = init_store.clone();
                Box::pin(async move { attempt_init_iam_sys(store).await })
            },
            move || {
                let store = finalize_store.clone();
                let kms_interface = finalize_kms_interface.clone();
                let readiness = finalize_readiness.clone();
                let state_manager = finalize_state_manager.clone();
                Box::pin(async move { finalize_iam_recovery(store, kms_interface, readiness, state_manager).await })
            },
        )
        .await;
    });
}

type RecoveryFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

async fn run_iam_recovery_loop<InitFn, FinalizeFn>(
    initial_interval: Duration,
    max_interval: Duration,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    mut init_fn: InitFn,
    mut finalize_fn: FinalizeFn,
) where
    InitFn: FnMut() -> RecoveryFuture,
    FinalizeFn: FnMut() -> RecoveryFuture,
{
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
                    event = EVENT_IAM_BOOTSTRAP_RECOVERED,
                    component = LOG_COMPONENT_STARTUP_IAM,
                    subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
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
                        event = EVENT_IAM_BOOTSTRAP_RETRY_FAILED,
                        component = LOG_COMPONENT_STARTUP_IAM,
                        subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
                        attempts,
                        next_retry_secs = next_interval.as_secs(),
                        degraded_duration_secs = degraded_since.elapsed().as_secs(),
                        error = %err,
                        "IAM bootstrap retry failed; service remains degraded"
                    );
                } else {
                    warn!(
                        event = EVENT_IAM_BOOTSTRAP_RETRY_FAILED,
                        component = LOG_COMPONENT_STARTUP_IAM,
                        subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
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
                    event = EVENT_IAM_READINESS_PUBLISHED,
                    component = LOG_COMPONENT_STARTUP_IAM,
                    subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
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
                    event = EVENT_IAM_READINESS_PUBLICATION_FAILED,
                    component = LOG_COMPONENT_STARTUP_IAM,
                    subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
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
}

fn initial_retry_interval() -> Duration {
    // Only honor the test override in debug builds to prevent accidental
    // production use via environment configuration.
    #[cfg(debug_assertions)]
    if let Some(ms) = rustfs_utils::get_env_opt_u64(rustfs_config::ENV_TEST_IAM_RETRY_INTERVAL_MS) {
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
            let configured = rustfs_utils::get_env_opt_u64(rustfs_config::ENV_TEST_IAM_FAIL_INIT_ATTEMPTS).unwrap_or(0);
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

    init_iam_sys(store).await.map_err(std::io::Error::other)
}

/// Attempt IAM bootstrap at startup. If it fails, enter degraded mode and
/// spawn a background recovery task with exponential backoff.
///
/// A failure to initialize IAM is not fatal — the process continues in degraded mode where:
/// - `/health/ready` returns 503
/// - IAM-dependent operations return `IamSysNotInitialized`
/// - A background task retries IAM initialization until it succeeds
///
/// Returns `Ok(ReadyInline)` if IAM initialized immediately, `Ok(Deferred)` if
/// recovery is happening in the background, or `Err` if IAM succeeded but
/// app context initialization failed (unexpected, indicates a bug).
pub async fn bootstrap_or_defer_iam_init(
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
) -> Result<IamBootstrapDisposition> {
    match attempt_init_iam_sys(store.clone()).await {
        Ok(()) => {
            AppContext::ensure_startup_after_iam(store, kms_interface)?;
            readiness.mark_stage(SystemStage::IamReady);
            return Ok(IamBootstrapDisposition::ReadyInline);
        }
        Err(err) => {
            let interval = initial_retry_interval();
            warn!(
                event = EVENT_IAM_BOOTSTRAP_DEFERRED,
                component = LOG_COMPONENT_STARTUP_IAM,
                subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
                error = %err,
                initial_retry_secs = interval.as_secs(),
                max_retry_secs = IAM_RETRY_MAX_INTERVAL.as_secs(),
                escalation_threshold = IAM_RETRY_ESCALATION_THRESHOLD,
                "Initial IAM bootstrap failed; continuing startup in degraded mode until IAM recovers. \
                 Health endpoint will report 503 until IAM is ready."
            );

            spawn_iam_recovery_task(
                interval,
                IAM_RETRY_MAX_INTERVAL,
                shutdown_token,
                store,
                kms_interface,
                readiness,
                state_manager,
            );
        }
    }

    Ok(IamBootstrapDisposition::Deferred)
}

pub async fn bootstrap_or_defer_iam_init_with_startup_kms(
    store: Arc<ECStore>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
) -> Result<IamBootstrapDisposition> {
    let kms_interface = AppContext::ensure_startup_kms_interface();
    bootstrap_or_defer_iam_init(store, kms_interface, readiness, state_manager, shutdown_token).await
}

pub(crate) async fn init_embedded_iam_runtime(
    store: Arc<ECStore>,
    ctx: tokio_util::sync::CancellationToken,
    readiness: Arc<GlobalReadiness>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, None, Some(ctx)).await
}

pub(crate) async fn init_iam_runtime(
    store: Arc<ECStore>,
    ctx: tokio_util::sync::CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, Some(state_manager), Some(ctx)).await
}

#[cfg(test)]
mod tests {
    use super::{
        IAM_RETRY_ESCALATION_THRESHOLD, IAM_RETRY_INITIAL_INTERVAL, IAM_RETRY_MAX_INTERVAL, IamBootstrapDisposition,
        compute_backoff_interval, publish_ready_for_iam_bootstrap_with, run_iam_recovery_loop,
    };
    use rustfs_common::{GlobalReadiness, SystemStage};
    use std::io::Error;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

    #[tokio::test]
    async fn ready_inline_bootstrap_publishes_runtime_readiness() {
        let publish_calls = Arc::new(AtomicUsize::new(0));
        let publish_calls_for_assert = publish_calls.clone();

        let published = publish_ready_for_iam_bootstrap_with(IamBootstrapDisposition::ReadyInline, move || async move {
            publish_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert!(published.is_ok(), "ready inline publication should succeed");
        let published = published.unwrap_or(false);
        assert!(published);
        assert_eq!(publish_calls_for_assert.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deferred_bootstrap_skips_runtime_readiness_publication() {
        let publish_calls = Arc::new(AtomicUsize::new(0));
        let publish_calls_for_assert = publish_calls.clone();

        let published = publish_ready_for_iam_bootstrap_with(IamBootstrapDisposition::Deferred, move || async move {
            publish_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert!(published.is_ok(), "deferred publication should be a no-op");
        let published = published.unwrap_or(true);
        assert!(!published);
        assert_eq!(publish_calls_for_assert.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ready_inline_bootstrap_propagates_runtime_readiness_failure() {
        let err = publish_ready_for_iam_bootstrap_with(IamBootstrapDisposition::ReadyInline, || async {
            Err(Error::other("runtime readiness failed"))
        })
        .await
        .expect_err("ready inline publication failure should be returned");

        assert_eq!(err.to_string(), "runtime readiness failed");
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_loop_retries_finalize_until_success() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let finalize_calls = Arc::new(AtomicUsize::new(0));

        let init_calls_for_assert = init_calls.clone();
        let finalize_calls_for_assert = finalize_calls.clone();

        run_iam_recovery_loop(
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            move || {
                let init_calls = init_calls.clone();
                Box::pin(async move {
                    init_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            },
            move || {
                let finalize_calls = finalize_calls.clone();
                Box::pin(async move {
                    let call = finalize_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if call < 3 {
                        Err(Error::other("finalize failed"))
                    } else {
                        Ok(())
                    }
                })
            },
        )
        .await;

        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 1);
        assert_eq!(finalize_calls_for_assert.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_loop_can_publish_iam_and_full_ready_after_degraded_init() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let readiness = Arc::new(GlobalReadiness::new());
        let observed_iam_ready = Arc::new(AtomicBool::new(false));

        let init_calls_for_assert = init_calls.clone();
        let readiness_for_finalize = readiness.clone();
        let observed_iam_ready_for_finalize = observed_iam_ready.clone();

        run_iam_recovery_loop(
            Duration::from_secs(5),
            Duration::from_secs(30),
            None,
            move || {
                let init_calls = init_calls.clone();
                Box::pin(async move {
                    let call = init_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if call == 1 {
                        Err(Error::other("degraded init"))
                    } else {
                        Ok(())
                    }
                })
            },
            move || {
                let readiness = readiness_for_finalize.clone();
                let observed_iam_ready = observed_iam_ready_for_finalize.clone();
                Box::pin(async move {
                    readiness.mark_stage(SystemStage::IamReady);
                    if matches!(readiness.current_stage(), SystemStage::IamReady) {
                        observed_iam_ready.store(true, Ordering::SeqCst);
                    }
                    readiness.mark_stage(SystemStage::FullReady);
                    Ok(())
                })
            },
        )
        .await;

        assert_eq!(init_calls_for_assert.load(Ordering::SeqCst), 2);
        assert!(observed_iam_ready.load(Ordering::SeqCst));
        assert!(readiness.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_loop_stops_after_shutdown_cancellation() {
        let init_calls = Arc::new(AtomicUsize::new(0));
        let cancel = tokio_util::sync::CancellationToken::new();

        let task = tokio::spawn({
            let init_calls = init_calls.clone();
            let cancel = cancel.clone();
            async move {
                run_iam_recovery_loop(
                    Duration::from_secs(5),
                    Duration::from_secs(30),
                    Some(cancel),
                    move || {
                        let init_calls = init_calls.clone();
                        Box::pin(async move {
                            init_calls.fetch_add(1, Ordering::SeqCst);
                            Err(Error::other("keep retrying"))
                        })
                    },
                    move || Box::pin(async { Ok(()) }),
                )
                .await;
            }
        });

        tokio::task::yield_now().await;
        cancel.cancel();
        tokio::task::yield_now().await;
        task.await.expect("recovery task should exit after cancellation");

        assert_eq!(init_calls.load(Ordering::SeqCst), 0);
    }
}
