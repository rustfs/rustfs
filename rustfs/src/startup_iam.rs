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

use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::server::{ServiceStateManager, publish_ready_when_runtime_ready};
use crate::storage_api::startup::iam::ECStore;
use rustfs_common::{GlobalReadiness, SystemStage};
use rustfs_iam::init_iam_sys_for_context;
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
pub(crate) struct HealTopologyReady(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IamBootstrapDisposition {
    ReadyInline(HealTopologyReady),
    Deferred,
}

pub(crate) async fn publish_ready_for_iam_bootstrap(
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
    if let IamBootstrapDisposition::ReadyInline(topology_ready) = disposition {
        publish_runtime_ready_after_heal_topology(topology_ready, publish_ready).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

async fn publish_runtime_ready_after_heal_topology<PublishFn, PublishFuture>(
    _topology_ready: HealTopologyReady,
    publish_ready: PublishFn,
) -> Result<()>
where
    PublishFn: FnOnce() -> PublishFuture,
    PublishFuture: Future<Output = Result<()>>,
{
    publish_ready().await
}

async fn prepare_iam_startup(
    iam: Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<HealTopologyReady> {
    let endpoint_pools = heal_control_endpoint_pools(store.as_ref())?;
    publish_iam_ready_after_heal_topology(readiness.as_ref(), server_ctx.heal_topology_fingerprint(), endpoint_pools, || {
        AppContext::ensure_startup_after_iam(store, kms_interface, &server_ctx, iam)
    })
    .await
}

async fn publish_iam_ready_after_heal_topology<InstallFn>(
    readiness: &GlobalReadiness,
    cache: Arc<tokio::sync::OnceCell<String>>,
    endpoint_pools: crate::storage_api::cluster::EndpointServerPools,
    install_context: InstallFn,
) -> Result<HealTopologyReady>
where
    InstallFn: FnOnce() -> Result<()>,
{
    crate::storage_api::startup::heal_control::initialize_heal_topology_fingerprint(cache, endpoint_pools)
        .await
        .map_err(std::io::Error::other)?;
    // Finish fallible preparation before claiming the one-time context slot.
    // No fallible/async step may follow a successful claim before this token.
    install_context()?;
    readiness.mark_stage(SystemStage::IamReady);
    Ok(HealTopologyReady(()))
}

fn heal_control_endpoint_pools(store: &ECStore) -> Result<crate::storage_api::cluster::EndpointServerPools> {
    store
        .instance_endpoints()
        .ok_or_else(|| std::io::Error::other("heal control topology is unavailable"))
}

fn compute_backoff_interval(attempt: u64, initial: Duration, max: Duration) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = 2u32.saturating_pow(exponent);
    let backoff = initial.saturating_mul(multiplier);
    if backoff > max { max } else { backoff }
}

#[allow(clippy::too_many_arguments)]
fn spawn_iam_recovery_task(
    initial_interval: Duration,
    max_interval: Duration,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    server_ctx: Arc<ServerContextSlot>,
) {
    let init_store = store.clone();
    let finalize_store = store;
    let finalize_kms_interface = kms_interface;
    let finalize_readiness = readiness;
    let finalize_state_manager = state_manager;
    let finalize_server_ctx = server_ctx;
    let prepare_readiness = finalize_readiness.clone();
    tokio::spawn(async move {
        run_iam_recovery_loop(
            initial_interval,
            max_interval,
            shutdown_token,
            move || {
                let store = init_store.clone();
                Box::pin(async move { attempt_init_iam_sys(store).await })
            },
            move |iam| {
                let store = finalize_store.clone();
                let kms_interface = finalize_kms_interface.clone();
                let readiness = prepare_readiness.clone();
                let server_ctx = finalize_server_ctx.clone();
                Box::pin(prepare_iam_startup(iam, store, kms_interface, readiness, server_ctx))
            },
            move |topology_ready| {
                let readiness = finalize_readiness.clone();
                let state_manager = finalize_state_manager.clone();
                Box::pin(async move {
                    publish_runtime_ready_after_heal_topology(topology_ready, || async {
                        publish_ready_when_runtime_ready(readiness.as_ref(), state_manager.as_deref()).await
                    })
                    .await
                })
            },
        )
        .await;
    });
}

type RecoveryFuture<T = ()> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

async fn run_iam_recovery_loop<T, Prepared, InitFn, PrepareFn, FinalizeFn>(
    initial_interval: Duration,
    max_interval: Duration,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    mut init_fn: InitFn,
    mut prepare_fn: PrepareFn,
    mut finalize_fn: FinalizeFn,
) where
    T: Clone,
    Prepared: Clone,
    InitFn: FnMut() -> RecoveryFuture<T>,
    PrepareFn: FnMut(T) -> RecoveryFuture<Prepared>,
    FinalizeFn: FnMut(Prepared) -> RecoveryFuture,
{
    let mut attempts: u64 = 0;
    let degraded_since = std::time::Instant::now();

    // Keep the initialized IAM owner across preparation/publication retries.
    let recovered = loop {
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
            Ok(recovered) => {
                let degraded_secs = degraded_since.elapsed().as_secs();
                info!(
                    event = EVENT_IAM_BOOTSTRAP_RECOVERED,
                    component = LOG_COMPONENT_STARTUP_IAM,
                    subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
                    attempts,
                    degraded_duration_secs = degraded_secs,
                    "IAM bootstrap recovered after startup; publishing IAM readiness"
                );
                break recovered;
            }
            Err(err) => {
                let next_interval = compute_backoff_interval(attempts + 1, initial_interval, max_interval);
                let hint = iam_bootstrap_failure_hint(&err.to_string());
                if attempts >= IAM_RETRY_ESCALATION_THRESHOLD {
                    error!(
                        event = EVENT_IAM_BOOTSTRAP_RETRY_FAILED,
                        component = LOG_COMPONENT_STARTUP_IAM,
                        subsystem = LOG_SUBSYSTEM_BOOTSTRAP,
                        attempts,
                        next_retry_secs = next_interval.as_secs(),
                        degraded_duration_secs = degraded_since.elapsed().as_secs(),
                        error = %err,
                        hint,
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
                        hint,
                        "IAM bootstrap retry failed; service remains degraded"
                    );
                }
            }
        }
    };

    // Preparation claims the context once. Only publication retries after it.
    let mut prepared: Option<Prepared> = None;
    let mut finalize_attempts: u64 = 0;
    loop {
        let result = match prepared.as_ref() {
            Some(prepared) => finalize_fn(prepared.clone()).await,
            None => match prepare_fn(recovered.clone()).await {
                Ok(value) => {
                    prepared = Some(value);
                    continue;
                }
                Err(error) => Err(error),
            },
        };
        finalize_attempts += 1;
        match result {
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

/// Classifies an IAM bootstrap failure into an actionable operator hint, so
/// the degraded-retry logs explain *what to do* instead of only echoing the
/// storage error (rustfs#4304: sequential cold starts left operators
/// guessing why the node stayed degraded).
fn iam_bootstrap_failure_hint(err_text: &str) -> &'static str {
    let lower = err_text.to_lowercase();
    if lower.contains("quorum") && lower.contains("lock") {
        "distributed lock quorum unavailable; waiting for peer nodes' lock RPC endpoints to come online"
    } else if lower.contains("read quorum") || lower.contains("erasure") || lower.contains("quorum") {
        "storage read quorum not met yet; waiting for enough cluster nodes/disks to come online"
    } else if lower.contains("not ready") || lower.contains("not found") {
        "storage metadata not initialized yet; retrying automatically"
    } else {
        "retrying automatically; check storage and peer connectivity if this persists"
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
#[cfg(debug_assertions)]
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
async fn attempt_init_iam_sys(
    store: Arc<ECStore>,
) -> std::result::Result<Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>, std::io::Error> {
    if should_fail_test_init_attempt() {
        return Err(std::io::Error::other("forced test IAM bootstrap failure"));
    }

    init_iam_sys_for_context(store).await.map_err(std::io::Error::other)
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
/// the instance context or heal topology could not be initialized.
pub(crate) async fn bootstrap_or_defer_iam_init(
    store: Arc<ECStore>,
    kms_interface: Arc<KmsServiceManager>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<IamBootstrapDisposition> {
    match attempt_init_iam_sys(store.clone()).await {
        Ok(iam) => {
            let topology_ready = prepare_iam_startup(iam, store, kms_interface, readiness, server_ctx).await?;
            return Ok(IamBootstrapDisposition::ReadyInline(topology_ready));
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
                server_ctx,
            );
        }
    }

    Ok(IamBootstrapDisposition::Deferred)
}

pub(crate) async fn bootstrap_or_defer_iam_init_with_startup_kms(
    store: Arc<ECStore>,
    readiness: Arc<GlobalReadiness>,
    state_manager: Option<Arc<ServiceStateManager>>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<IamBootstrapDisposition> {
    let kms_interface = AppContext::ensure_startup_kms_interface();
    bootstrap_or_defer_iam_init(store, kms_interface, readiness, state_manager, shutdown_token, server_ctx).await
}

pub(crate) async fn init_embedded_iam_runtime(
    store: Arc<ECStore>,
    ctx: tokio_util::sync::CancellationToken,
    readiness: Arc<GlobalReadiness>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, None, Some(ctx), server_ctx).await
}

pub(crate) async fn init_iam_runtime(
    store: Arc<ECStore>,
    ctx: tokio_util::sync::CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
    server_ctx: Arc<ServerContextSlot>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, Some(state_manager), Some(ctx), server_ctx).await
}

#[cfg(test)]
mod hint_tests {
    use super::iam_bootstrap_failure_hint;

    /// rustfs#4304: the degraded-retry log must tell the operator what the
    /// node is waiting on, for each of the observed failure shapes.
    #[test]
    fn classifies_observed_bootstrap_failures() {
        assert!(
            iam_bootstrap_failure_hint(
                "load group failed: io error: Failed to acquire read lock: Quorum not reached: required 2, achieved 0"
            )
            .contains("lock quorum"),
            "lock-quorum failures must point at peer lock RPC endpoints"
        );
        assert!(
            iam_bootstrap_failure_hint("load group failed: erasure read quorum").contains("storage read quorum"),
            "storage-quorum failures must point at missing nodes/disks"
        );
        assert!(
            iam_bootstrap_failure_hint("Storage metadata not ready: probe object not found").contains("not initialized"),
            "missing-metadata failures must say the store is still initializing"
        );
        assert!(
            iam_bootstrap_failure_hint("some opaque failure").contains("retrying automatically"),
            "unknown failures must still promise the automatic retry"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{IAM_RETRY_ESCALATION_THRESHOLD, IAM_RETRY_INITIAL_INTERVAL, IAM_RETRY_MAX_INTERVAL, compute_backoff_interval};
    use crate::storage_api::cluster::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
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

        let published = publish_ready_for_iam_bootstrap_with(
            IamBootstrapDisposition::ReadyInline(HealTopologyReady(())),
            move || async move {
                publish_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
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
        let err = publish_ready_for_iam_bootstrap_with(IamBootstrapDisposition::ReadyInline(HealTopologyReady(())), || async {
            Err(Error::other("runtime readiness failed"))
        })
        .await
        .expect_err("ready inline publication failure should be returned");

        assert_eq!(err.to_string(), "runtime readiness failed");
    }

    #[tokio::test]
    async fn iam_readiness_is_published_only_after_heal_topology_initialization() {
        let readiness = GlobalReadiness::new();
        let cache = Arc::new(tokio::sync::OnceCell::new());
        let mut endpoint = Endpoint::try_from("http://node-a:9000/disk").expect("valid test endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let endpoint_pools = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![endpoint]),
            cmd_line: "test".to_string(),
            platform: String::new(),
        }]);
        let expected = crate::storage_api::startup::heal_control::heal_topology_fingerprint(&endpoint_pools)
            .expect("valid topology should hash");

        publish_iam_ready_after_heal_topology(&readiness, Arc::clone(&cache), endpoint_pools, || Ok(()))
            .await
            .expect("topology initialization should publish IAM readiness");

        assert_eq!(cache.get(), Some(&expected));
        assert!(matches!(readiness.current_stage(), SystemStage::IamReady));

        let failed_readiness = GlobalReadiness::new();
        let invalid_cache = Arc::new(tokio::sync::OnceCell::new());
        publish_iam_ready_after_heal_topology(
            &failed_readiness,
            Arc::clone(&invalid_cache),
            EndpointServerPools::from(Vec::<PoolEndpoints>::new()),
            || panic!("invalid topology must not claim the context"),
        )
        .await
        .expect_err("invalid topology must block IAM readiness");
        assert!(invalid_cache.get().is_none());
        assert!(!matches!(
            failed_readiness.current_stage(),
            SystemStage::IamReady | SystemStage::FullReady
        ));
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
            |()| Box::pin(async { Ok(()) }),
            move |_| {
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
            |()| Box::pin(async { Ok(()) }),
            move |_| {
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

    #[tokio::test]
    #[serial_test::serial]
    async fn recovery_publication_timeout_reuses_the_installed_context() {
        let context = crate::app::gating_test_env::shared_gating_ambient().await;
        let server_ctx = ServerContextSlot::new();
        // The legacy fixture exposes its real disks without instance endpoints.
        let (paths, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
        assert!(Arc::ptr_eq(&store, &context.object_store()));
        let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: paths.len(),
            endpoints: Endpoints::from(
                paths
                    .iter()
                    .enumerate()
                    .map(|(index, path)| {
                        let mut endpoint =
                            Endpoint::try_from(path.to_str().expect("fixture disk path")).expect("fixture endpoint");
                        endpoint.set_pool_index(0);
                        endpoint.set_set_index(0);
                        endpoint.set_disk_index(index);
                        endpoint
                    })
                    .collect::<Vec<_>>(),
            ),
            cmd_line: "recovery-publication-test".to_string(),
            platform: String::new(),
        }]);
        crate::storage_api::startup::heal_control::initialize_heal_topology_fingerprint(
            server_ctx.heal_topology_fingerprint(),
            endpoints.clone(),
        )
        .await
        .expect("prepare fixture fingerprint before pausing time");
        // A fresh paused runtime keeps real fixture I/O and its timer-wheel
        // rounding out of the exact retry/deadline observations below.
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .expect("paused recovery runtime");
            runtime.block_on(async move {
                let readiness = Arc::new(GlobalReadiness::new());
                let init_calls = Arc::new(AtomicUsize::new(0));
                let prepare_calls = Arc::new(AtomicUsize::new(0));
                let publish_calls = Arc::new(AtomicUsize::new(0));
                let started = tokio::time::Instant::now();
                let recovery = run_iam_recovery_loop(
                    Duration::from_secs(5),
                    Duration::from_secs(30),
                    None,
                    {
                        let context = Arc::clone(&context);
                        let calls = Arc::clone(&init_calls);
                        move || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let context = Arc::clone(&context);
                            Box::pin(async move { Ok(context) })
                        }
                    },
                    {
                        let server_ctx = Arc::clone(&server_ctx);
                        let readiness = Arc::clone(&readiness);
                        let calls = Arc::clone(&prepare_calls);
                        move |context| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let server_ctx = Arc::clone(&server_ctx);
                            let readiness = Arc::clone(&readiness);
                            let endpoints = endpoints.clone();
                            Box::pin(async move {
                                publish_iam_ready_after_heal_topology(
                                    readiness.as_ref(),
                                    server_ctx.heal_topology_fingerprint(),
                                    endpoints,
                                    || server_ctx.try_install(context),
                                )
                                .await
                            })
                        }
                    },
                    {
                        let readiness = Arc::clone(&readiness);
                        let calls = Arc::clone(&publish_calls);
                        move |prepared| {
                            let attempt = calls.fetch_add(1, Ordering::SeqCst);
                            let readiness = Arc::clone(&readiness);
                            Box::pin(async move {
                                let result = publish_runtime_ready_after_heal_topology(prepared, || async {
                                    crate::server::wait_for_runtime_readiness_with(
                                        Duration::from_secs(2),
                                        Duration::from_secs(1),
                                        || async move {
                                            if attempt == 0 {
                                                std::future::pending::<crate::shared_types::DependencyReadiness>().await
                                            } else {
                                                crate::shared_types::DependencyReadiness {
                                                    storage_ready: true,
                                                    iam_ready: true,
                                                    lock_quorum_ready: true,
                                                    peer_health_ready: true,
                                                }
                                            }
                                        },
                                        |_| readiness.mark_stage(SystemStage::FullReady),
                                    )
                                    .await
                                })
                                .await;
                                if attempt == 0 {
                                    assert!(result.is_err(), "the first publication must time out");
                                    assert!(!readiness.is_ready(), "timeout cannot publish FullReady");
                                }
                                result
                            })
                        }
                    },
                );
                tokio::time::timeout(Duration::from_secs(20), recovery)
                    .await
                    .expect("publication retry must succeed without attempting another context installation");
                assert_eq!(init_calls.load(Ordering::SeqCst), 1);
                assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
                assert_eq!(publish_calls.load(Ordering::SeqCst), 2);
                assert_eq!(started.elapsed(), Duration::from_secs(12));
                assert!(readiness.is_ready());
                assert!(Arc::ptr_eq(&server_ctx.installed_app_context().expect("installed context"), &context));
                assert_eq!(
                    server_ctx
                        .try_install(context)
                        .expect_err("duplicate installs remain conflicts")
                        .kind(),
                    std::io::ErrorKind::AlreadyExists
                );
            });
        })
        .await
        .expect("recovery model completes without panic");
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_preparation_retries_reuse_the_initialized_owner() {
        let owner = Arc::new(AtomicUsize::new(0));
        let init_calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::new(AtomicUsize::new(0));
        run_iam_recovery_loop(
            Duration::from_secs(1),
            Duration::from_secs(2),
            None,
            {
                let owner = Arc::clone(&owner);
                let calls = Arc::clone(&init_calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let owner = Arc::clone(&owner);
                    Box::pin(async move { Ok(owner) })
                }
            },
            {
                let expected = Arc::clone(&owner);
                move |owner| {
                    assert!(Arc::ptr_eq(&expected, &owner), "preparation must keep the same initialized IAM owner");
                    Box::pin(async move {
                        if owner.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Error::other("topology not ready"))
                        } else {
                            Ok(owner)
                        }
                    })
                }
            },
            {
                let calls = Arc::clone(&publish_calls);
                move |owner| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        assert_eq!(owner.load(Ordering::SeqCst), 2, "publication follows successful preparation");
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            },
        )
        .await;
        assert_eq!(init_calls.load(Ordering::SeqCst), 1);
        assert_eq!(owner.load(Ordering::SeqCst), 2);
        assert_eq!(publish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&owner), 1, "loop releases its initialized and prepared owners");
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
                    |()| Box::pin(async { Ok(()) }),
                    |()| Box::pin(async { Ok(()) }),
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
