// Copyright 2026 RustFS Team
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

//! Admission and execution isolation for PUT, local/RPC disk cleanup and GC.
//! PUT admission precedes staging; receiver and GC disk work share a separate
//! budget so coordinators waiting for peers cannot exhaust receiver capacity.

use crate::disk::{LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_DISK};
use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const ENABLE: &str = "RUSTFS_CLEANUP_ISOLATE_ENABLE";
const BUDGET: &str = "RUSTFS_PUT_RENAME_TAIL_CLEANUP_MAX_PENDING";
const BLOCKING_THREADS: &str = "RUSTFS_CLEANUP_BLOCKING_THREADS";
const ASYNC_THREADS: &str = "RUSTFS_CLEANUP_ASYNC_THREADS";
const CPUS: &str = "RUSTFS_CLEANUP_CPUS";
const WORKERS: &str = "RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_WORKERS";
const DISK_PENDING: &str = "RUSTFS_CLEANUP_DISK_MAX_PENDING";
const DISK_WORKERS: &str = "RUSTFS_CLEANUP_DISK_WORKERS";
const GC_WORKERS: &str = "RUSTFS_CLEANUP_GC_WORKERS";
const EVENT_CLEANUP_RUNTIME: &str = "disk_cleanup_runtime";
pub(crate) const OLD_DATA_CLEANUP_RECEIPT_FILE: &str = ".rustfs-old-data-cleanup-receipt.json";

#[derive(Clone, Copy)]
enum DiskAdmission {
    Request,
    Guarded,
}

#[derive(Clone)]
pub(crate) struct Reservation {
    _admission: Arc<Admission>,
    execution: Arc<Semaphore>,
    handle: Handle,
}

struct Admission {
    _permit: OwnedSemaphorePermit,
    metrics_enabled: bool,
    metric: &'static str,
}

impl Admission {
    fn new(permit: OwnedSemaphorePermit, metric: &'static str) -> Self {
        let metrics_enabled = rustfs_io_metrics::put_stage_metrics_enabled();
        if metrics_enabled {
            metrics::gauge!(metric).increment(1.0);
        }
        Self {
            _permit: permit,
            metrics_enabled,
            metric,
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if self.metrics_enabled {
            metrics::gauge!(self.metric).decrement(1.0);
        }
    }
}

/// A lease release cannot queue behind work that might need the releasing
/// reader's locks. It relinquishes the lease first, then tries this disk budget.
pub(crate) struct DiskExecution {
    _active: Admission,
    _pending: Admission,
}

/// Created only inside an owned scan. Steps can borrow the scan's directory
/// iterator and paths: cancelling its waiter cannot cancel a step's syscalls.
#[cfg_attr(test, derive(Default))]
pub(crate) struct GcBudget {
    gates: Option<(Arc<Semaphore>, Arc<Semaphore>)>,
}

impl GcBudget {
    pub(crate) async fn step<F, T>(&self, future: F) -> super::error::Result<T>
    where
        F: Future<Output = super::error::Result<T>>,
    {
        let Some((pending, execution)) = &self.gates else {
            return future.await;
        };
        // Scan producers are bounded and hold no object locks here. Unlike
        // requests, they can wait for admission without an unbounded backlog.
        let started = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);
        let pending = pending.clone().acquire_owned().await.map_err(io::Error::other)?;
        execute_disk(
            Admission::new(pending, "rustfs_cleanup_disk_pending"),
            None,
            execution.clone(),
            started,
            future,
        )
        .await
    }
}

fn try_disk_permit(semaphore: &Arc<Semaphore>) -> io::Result<OwnedSemaphorePermit> {
    semaphore.clone().try_acquire_owned().map_err(|err| {
        if rustfs_io_metrics::put_stage_metrics_enabled() {
            metrics::counter!("rustfs_cleanup_disk_admission_rejected_total").increment(1);
        }
        io::Error::new(io::ErrorKind::WouldBlock, err)
    })
}

async fn execute_disk<F, T>(
    pending: Admission,
    active: Option<OwnedSemaphorePermit>,
    execution: Arc<Semaphore>,
    started: Option<Instant>,
    future: F,
) -> super::error::Result<T>
where
    F: Future<Output = super::error::Result<T>>,
{
    let _pending = pending;
    let active = match active {
        Some(active) => active,
        None => execution.acquire_owned().await.map_err(super::error::DiskError::other)?,
    };
    let _active = Admission::new(active, "rustfs_cleanup_disk_active");
    if let Some(started) = started {
        metrics::histogram!("rustfs_cleanup_disk_execution_wait_seconds").record(started.elapsed().as_secs_f64());
    }
    future.await
}

struct Config {
    pending: usize,
    workers: usize,
    async_threads: usize,
    blocking_threads: usize,
    cpus: Vec<usize>,
    disk_pending: usize,
    disk_workers: usize,
    gc_workers: usize,
}

fn positive_env(name: &str, default: usize) -> io::Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0 && *v <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} must be a positive supported count"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidInput, err)),
    }
}

fn parse_cpus(value: &str) -> io::Result<Vec<usize>> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "cleanup CPUs must be comma-separated CPU IDs or ascending ranges",
        )
    };
    if value.trim().is_empty() {
        return Err(invalid());
    }
    let mut cpus = BTreeSet::new();
    for part in value.split(',') {
        let mut ends = part.trim().split('-');
        let first = ends.next().and_then(|s| s.parse::<usize>().ok()).ok_or_else(invalid)?;
        let last = match ends.next() {
            Some(s) => s.parse::<usize>().map_err(|_| invalid())?,
            None => first,
        };
        if ends.next().is_some() || first > last || last >= 1024 {
            return Err(invalid());
        }
        cpus.extend(first..=last);
    }
    Ok(cpus.into_iter().collect())
}

impl Config {
    fn from_env() -> io::Result<Self> {
        for name in [
            "RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP",
            "RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER",
            "RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP",
        ] {
            if rustfs_utils::get_env_bool(name, false) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} cannot be combined with cleanup isolation"),
                ));
            }
        }
        let workers = positive_env(WORKERS, 2)?;
        let pending = positive_env(BUDGET, 1024)?;
        let blocking_threads = positive_env(BLOCKING_THREADS, 4)?;
        let cpus = match std::env::var(CPUS) {
            Ok(value) => parse_cpus(&value)?,
            Err(std::env::VarError::NotPresent) => Vec::new(),
            Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidInput, err)),
        };
        let available = std::thread::available_parallelism().map_or(1, usize::from);
        let async_threads = positive_env(ASYNC_THREADS, available.min(if cpus.is_empty() { 2 } else { cpus.len().min(2) }))?;
        let disk_workers = positive_env(DISK_WORKERS, blocking_threads)?;
        Ok(Self {
            pending,
            workers,
            async_threads,
            blocking_threads,
            cpus,
            // Bound queueing to sixteen service waves by default. Increasing
            // async coordinator concurrency does not create more OS threads.
            disk_pending: positive_env(DISK_PENDING, disk_workers.saturating_mul(16).min(Semaphore::MAX_PERMITS))?,
            disk_workers,
            gc_workers: positive_env(GC_WORKERS, 1)?,
        })
    }

    fn validate(&self) -> io::Result<()> {
        if self.pending == 0
            || self.pending > Semaphore::MAX_PERMITS
            || self.workers == 0
            || self.workers > self.pending
            || self.async_threads == 0
            || self.async_threads > 1024
            || self.blocking_threads == 0
            || self.blocking_threads > 1024
            || self.disk_pending == 0
            || self.disk_pending > Semaphore::MAX_PERMITS
            || self.disk_workers == 0
            || self.disk_workers > self.disk_pending
            || self.gc_workers == 0
            || self.gc_workers > self.disk_workers
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cleanup counts must be positive: PUT workers <= PUT pending, GC workers <= disk workers <= disk pending, async/blocking threads <= 1024",
            ));
        }
        validate_cpus(&self.cpus)
    }
}

#[cfg(target_os = "linux")]
fn validate_cpus(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    let allowed = rustix::thread::sched_getaffinity(None)?;
    if cpus
        .iter()
        .any(|cpu| *cpu >= rustix::thread::CpuSet::MAX_CPU || !allowed.is_set(*cpu))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cleanup CPUs must be a subset of the caller's allowed CPU set",
        ));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_cpus(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Unsupported, "cleanup CPU affinity requires Linux"))
    }
}

fn bind_thread(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        let mut set = rustix::thread::CpuSet::new();
        for cpu in cpus {
            set.set(*cpu);
        }
        rustix::thread::sched_setaffinity(None, &set)?;
        if rustix::thread::sched_getaffinity(None)? != set {
            return Err(io::Error::other("cleanup CPU affinity changed during thread startup"));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        validate_cpus(cpus)
    }
}

struct CleanupRuntime {
    runtime: Option<Runtime>,
    admission: Arc<Semaphore>,
    execution: Arc<Semaphore>,
    affinity_failed: Arc<AtomicBool>,
    disk_admission: Arc<Semaphore>,
    disk_execution: Arc<Semaphore>,
    gc_execution: Arc<Semaphore>,
}

impl CleanupRuntime {
    fn new(config: Config) -> io::Result<Self> {
        config.validate()?;
        let affinity_failed = Arc::new(AtomicBool::new(false));
        let failed = affinity_failed.clone();
        let cpus = config.cpus;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.async_threads)
            .max_blocking_threads(config.blocking_threads)
            .thread_name("rustfs-cleanup")
            .enable_all()
            .on_thread_start(move || {
                // Explicit fsync dispatch must not escape into the shared
                // fsync runtime. This callback also runs on blocking threads.
                crate::disk::os::use_current_runtime_for_fsync();
                if let Err(err) = bind_thread(&cpus)
                    && !failed.swap(true, Ordering::AcqRel)
                {
                    tracing::error!(event = EVENT_CLEANUP_RUNTIME, component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DISK, state = "affinity_failed", error = %err,
                        "Cleanup thread affinity failed");
                }
            })
            .build()?;
        Ok(Self {
            runtime: Some(runtime),
            admission: Arc::new(Semaphore::new(config.pending)),
            execution: Arc::new(Semaphore::new(config.workers)),
            affinity_failed,
            disk_admission: Arc::new(Semaphore::new(config.disk_pending)),
            disk_execution: Arc::new(Semaphore::new(config.disk_workers)),
            gc_execution: Arc::new(Semaphore::new(config.gc_workers)),
        })
    }

    fn reserve(&self) -> io::Result<Reservation> {
        if self.affinity_failed.load(Ordering::Acquire) {
            return Err(io::Error::other("cleanup thread affinity failed; refusing new cleanup admission"));
        }
        let metrics_enabled = rustfs_io_metrics::put_stage_metrics_enabled();
        // Callers may already own source-reader or data-movement locks. Do
        // not wait here behind a PUT whose completion requires those locks.
        let permit = self.admission.clone().try_acquire_owned().map_err(|err| {
            if metrics_enabled {
                metrics::counter!("rustfs_cleanup_admission_rejected_total").increment(1);
            }
            io::Error::new(io::ErrorKind::WouldBlock, err)
        })?;
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("cleanup runtime stopped"))?;
        Ok(Reservation {
            _admission: Arc::new(Admission::new(permit, "rustfs_cleanup_admitted_puts")),
            execution: self.execution.clone(),
            handle: runtime.handle().clone(),
        })
    }

    async fn disk_operation<F, T>(&self, future: F, mode: DiskAdmission) -> super::error::Result<T>
    where
        F: Future<Output = super::error::Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if self.affinity_failed.load(Ordering::Acquire) && !super::os::is_isolated_io_thread() {
            return Err(io::Error::other("cleanup thread affinity failed; refusing new disk cleanup admission").into());
        }
        let started = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);
        let pending = try_disk_permit(&self.disk_admission)?;
        let pending = Admission::new(pending, "rustfs_cleanup_disk_pending");
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("cleanup runtime stopped"))?;
        let execution = self.disk_execution.clone();
        // An externally guarded mutation may already hold a namespace lock
        // needed by an active disk job. Never queue it behind that job.
        let active = if matches!(mode, DiskAdmission::Guarded) {
            try_disk_permit(&execution)?
        } else {
            // Keep queued work in the caller's existing future. A waiter
            // timeout must discard an unstarted delete, not leave a task that
            // can later delete a reused path after its caller released locks.
            execution.clone().acquire_owned().await.map_err(io::Error::other)?
        };
        // A permit does not mean the worker has polled this operation. Use
        // the result receiver's lifetime to discard work cancelled during
        // runtime scheduling, before its first poll can start filesystem I/O.
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = runtime.spawn(async move {
            if result_tx.is_closed() {
                return;
            }
            // After the first poll, the owned job must retain both permits
            // through I/O even if the waiter cancels and closes result_rx.
            let result = execute_disk(pending, Some(active), execution, started, future).await;
            let _ = result_tx.send(result);
        });
        match result_rx.await {
            Ok(result) => result,
            Err(err) => {
                // Preserve a panic's JoinError rather than erasing its cause
                // into a closed-channel error.
                task.await.map_err(super::error::DiskError::other)?;
                Err(super::error::DiskError::other(err))
            }
        }
    }

    fn try_disk_execution(&self) -> io::Result<DiskExecution> {
        let pending = try_disk_permit(&self.disk_admission)?;
        let active = try_disk_permit(&self.disk_execution)?;
        Ok(DiskExecution {
            _pending: Admission::new(pending, "rustfs_cleanup_disk_pending"),
            _active: Admission::new(active, "rustfs_cleanup_disk_active"),
        })
    }

    async fn gc_operation<F, Fut>(&self, scan: F) -> super::error::Result<()>
    where
        F: FnOnce(GcBudget) -> Fut + Send + 'static,
        Fut: Future<Output = super::error::Result<()>> + Send + 'static,
    {
        if self.affinity_failed.load(Ordering::Acquire) {
            return Err(io::Error::other("cleanup thread affinity failed; refusing new GC scan").into());
        }
        let handle = self
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("cleanup runtime stopped"))?
            .handle();
        let execution = self.gc_execution.clone();
        let budget = GcBudget {
            gates: Some((self.disk_admission.clone(), self.disk_execution.clone())),
        };
        handle
            .spawn(async move {
                let active = execution.acquire_owned().await.map_err(super::error::DiskError::other)?;
                let _active = Admission::new(active, "rustfs_cleanup_gc_active");
                scan(budget).await
            })
            .await
            .map_err(super::error::DiskError::other)?
    }
}

impl Drop for CleanupRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn instance() -> io::Result<Option<&'static CleanupRuntime>> {
    static INSTANCE: OnceLock<Result<Option<CleanupRuntime>, String>> = OnceLock::new();
    match INSTANCE.get_or_init(|| {
        if !rustfs_utils::get_env_bool(ENABLE, false) {
            return Ok(None);
        }
        Config::from_env()
            .and_then(CleanupRuntime::new)
            .map(Some)
            .map_err(|err| err.to_string())
    }) {
        Ok(runtime) => Ok(runtime.as_ref()),
        Err(err) => Err(io::Error::other(err.clone())),
    }
}

pub(crate) fn reserve() -> io::Result<Option<Reservation>> {
    match instance()? {
        Some(runtime) => runtime.reserve().map(Some),
        None => Ok(None),
    }
}

pub(crate) fn enabled() -> io::Result<bool> {
    Ok(instance()?.is_some())
}

/// Local cleanup entry point, also used by remote RPC receivers. This budget
/// is distinct from coordinator execution: coordinators can wait for peers
/// without exhausting the peers' capacity to service disk cleanup.
pub(crate) async fn disk_operation<F, T>(future: F) -> super::error::Result<T>
where
    F: Future<Output = super::error::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match instance()? {
        Some(runtime) => runtime.disk_operation(future, DiskAdmission::Request).await,
        None => future.await,
    }
}

pub(crate) async fn guarded_disk_operation<F, T>(future: F) -> super::error::Result<T>
where
    F: Future<Output = super::error::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match instance()? {
        Some(runtime) => runtime.disk_operation(future, DiskAdmission::Guarded).await,
        None => future.await,
    }
}

/// Lease release must run even under cleanup saturation, or reader tokens
/// would leak. Only its optional deferred deletion consumes disk permits.
pub(crate) async fn release_lease<F>(future: F) -> super::error::Result<()>
where
    F: Future<Output = super::error::Result<()>> + Send + 'static,
{
    match instance()? {
        Some(runtime) => runtime
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("cleanup runtime stopped"))?
            .spawn(future)
            .await
            .map_err(super::error::DiskError::other)?,
        None => future.await,
    }
}

pub(crate) fn try_disk_execution() -> io::Result<Option<DiskExecution>> {
    instance()?.map(CleanupRuntime::try_disk_execution).transpose()
}

/// At most gc_workers scans can produce work. Each scan yields the shared
/// disk execution budget between entries without spawning a task per entry.
pub(crate) async fn run_gc<F, Fut>(scan: F) -> super::error::Result<()>
where
    F: FnOnce(GcBudget) -> Fut + Send + 'static,
    Fut: Future<Output = super::error::Result<()>> + Send + 'static,
{
    match instance()? {
        Some(runtime) => runtime.gc_operation(scan).await,
        None => scan(GcBudget { gates: None }).await,
    }
}

/// Dispatch only already-admitted work. Keeping the runtime handle in the
/// reservation prevents configuration errors from dropping committed cleanup.
pub(crate) fn spawn<F>(reservation: Option<Reservation>, future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let started = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);
    match reservation {
        Some(reservation) => reservation.handle.clone().spawn(async move {
            // This semaphore is never closed. Both queued work and overflow
            // use this gate, and cancellation of the waiter cannot release it.
            let _permit = reservation.execution.clone().acquire_owned().await;
            if let Some(started) = started {
                metrics::histogram!("rustfs_cleanup_execution_wait_seconds").record(started.elapsed().as_secs_f64());
            }
            let result = future.await;
            drop(reservation);
            result
        }),
        None => tokio::spawn(future),
    }
}

/// Queue workers, queue overflow and synchronous failure cleanup share a gate.
/// With isolation disabled, retain the original inline polling behavior.
pub(crate) async fn run<F>(reservation: Option<Reservation>, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if reservation.is_none() {
        future.await;
    } else if let Err(err) = spawn(reservation, future).await {
        tracing::error!(event = EVENT_CLEANUP_RUNTIME, component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_DISK, state = "task_failed", error = %err,
            "Cleanup task failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn config(pending: usize, workers: usize, blocking_threads: usize) -> Config {
        Config {
            pending,
            workers,
            async_threads: workers.min(2),
            blocking_threads,
            cpus: Vec::new(),
            disk_pending: 4,
            disk_workers: 2,
            gc_workers: 1,
        }
    }

    async fn deadline<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(20), future)
            .await
            .expect("cleanup test timed out")
    }

    #[test]
    fn cpu_list_rejects_invalid_or_unbounded_ranges() {
        assert_eq!(parse_cpus("3, 1-3,6").unwrap(), vec![1, 2, 3, 6]);
        for value in ["", " ", "1,", "-1", "2-1", "1-2-3", "no", "1024", "0-18446744073709551615"] {
            assert_eq!(parse_cpus(value).unwrap_err().kind(), io::ErrorKind::InvalidInput, "{value}");
        }
    }

    #[test]
    fn invalid_environment_is_rejected_before_admission() {
        const CHILD: &str = "RUSTFS_CLEANUP_CONFIG_TEST_CHILD";
        if std::env::var_os(CHILD).is_some() {
            assert!(reserve().is_err());
            return;
        }
        // Each configuration needs a fresh process: production intentionally
        // caches configuration, and mutating process env races parallel tests.
        for (name, value) in [
            (WORKERS, "0"),
            (WORKERS, "5"),
            (BUDGET, "invalid"),
            (BLOCKING_THREADS, "0"),
            (BLOCKING_THREADS, "1025"),
            (ASYNC_THREADS, "0"),
            (ASYNC_THREADS, "1025"),
            (DISK_PENDING, "0"),
            (DISK_WORKERS, "3"),
            (GC_WORKERS, "3"),
            (CPUS, "1-0"),
            ("RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP", "true"),
            ("RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER", "true"),
            ("RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP", "true"),
        ] {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "disk::cleanup_runtime::tests::invalid_environment_is_rejected_before_admission",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env(ENABLE, "true")
                .env(WORKERS, "1")
                .env(BUDGET, "4")
                .env(BLOCKING_THREADS, "1")
                .env(ASYNC_THREADS, "1")
                .env(DISK_PENDING, "2")
                .env(DISK_WORKERS, "2")
                .env(GC_WORKERS, "1")
                .env_remove(CPUS)
                .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP")
                .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER")
                .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP")
                .env(name, value)
                .output()
                .unwrap();
            assert!(result.status.success(), "{name}={value}: {}", String::from_utf8_lossy(&result.stderr));
            assert!(
                String::from_utf8_lossy(&result.stdout).contains("1 passed"),
                "child must execute the admission assertion"
            );
        }
    }

    #[test]
    fn invalid_budgets_fail_before_building_threads() {
        for config in [config(0, 1, 1), config(1, 0, 1), config(1, 2, 1), config(1, 1, 0)] {
            assert!(matches!(CleanupRuntime::new(config), Err(err) if err.kind() == io::ErrorKind::InvalidInput));
        }
    }

    #[tokio::test]
    async fn disk_work_keeps_budget_after_rpc_waiter_cancellation() {
        let mut settings = config(1, 1, 1);
        settings.disk_pending = 1;
        settings.disk_workers = 1;
        let runtime = Arc::new(CleanupRuntime::new(settings).unwrap());
        let receiver = runtime.clone();
        let (entered, entry) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let rpc = tokio::spawn(async move {
            receiver
                .disk_operation(
                    async move {
                        tokio::task::spawn_blocking(move || {
                            assert_eq!(std::thread::current().name(), Some("rustfs-cleanup"));
                            entered.send(()).unwrap();
                            released.recv_timeout(Duration::from_secs(20)).unwrap();
                        })
                        .await
                        .unwrap();
                        Ok(())
                    },
                    DiskAdmission::Request,
                )
                .await
        });
        deadline(entry).await.unwrap();
        rpc.abort();
        assert!(rpc.await.unwrap_err().is_cancelled());
        assert_eq!(runtime.disk_admission.available_permits(), 0);
        assert_eq!(runtime.disk_execution.available_permits(), 0);
        let ran = Arc::new(AtomicBool::new(false));
        let invoked = ran.clone();
        let rejected = runtime
            .disk_operation(
                async move {
                    invoked.store(true, Ordering::SeqCst);
                    Ok(())
                },
                DiskAdmission::Request,
            )
            .await;
        assert!(matches!(rejected, Err(super::super::error::DiskError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock));
        assert!(!ran.load(Ordering::SeqCst), "rejected work must not execute");
        release.send(()).unwrap();
        deadline(async {
            while runtime.disk_admission.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(runtime.disk_execution.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelling_queued_disk_work_discards_it_before_execution() {
        let runtime = Arc::new(CleanupRuntime::new(config(1, 1, 1)).expect("cleanup runtime"));
        let occupied = runtime
            .disk_execution
            .clone()
            .acquire_many_owned(2)
            .await
            .expect("occupy all execution slots");
        let receiver = runtime.clone();
        let ran = Arc::new(AtomicBool::new(false));
        let invoked = ran.clone();
        let waiter = tokio::spawn(async move {
            receiver
                .disk_operation(
                    async move {
                        invoked.store(true, Ordering::SeqCst);
                        Ok(())
                    },
                    DiskAdmission::Request,
                )
                .await
        });
        deadline(async {
            while runtime.disk_admission.available_permits() == 4 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        waiter.abort();
        assert!(waiter.await.expect_err("cancel queued waiter").is_cancelled());
        assert_eq!(
            runtime.disk_admission.available_permits(),
            4,
            "unstarted work must release pending capacity immediately"
        );
        drop(occupied);
        deadline(runtime.disk_operation(async { Ok(()) }, DiskAdmission::Request))
            .await
            .expect("later work must progress");
        assert!(!ran.load(Ordering::SeqCst), "cancelled queued deletion must never execute later");
    }

    #[tokio::test]
    async fn cancelling_disk_work_before_worker_poll_preserves_reused_path() {
        let runtime = Arc::new(CleanupRuntime::new(config(1, 1, 1)).expect("cleanup runtime"));
        let (entered, entry) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let stalled = runtime.runtime.as_ref().expect("live runtime").spawn(async move {
            entered.send(()).expect("worker entered");
            released
                .recv_timeout(Duration::from_secs(30))
                .expect("release stalled worker");
        });
        deadline(entry).await.expect("worker must stall before dispatch");
        let dir = tempfile::tempdir().expect("temporary disk");
        let backup = dir.path().join("xl.meta.bkp");
        std::fs::write(&backup, b"previous transaction").expect("old backup");
        {
            let path = backup.clone();
            let deletion = runtime.disk_operation(
                async move {
                    tokio::fs::remove_file(path).await?;
                    Ok(())
                },
                DiskAdmission::Request,
            );
            tokio::pin!(deletion);
            assert!(futures::poll!(&mut deletion).is_pending());
            assert_eq!(runtime.disk_execution.available_permits(), 1, "execution was admitted but not polled");
        }
        std::fs::write(&backup, b"next transaction").expect("reuse path after cancelled waiter");
        release.send(()).expect("resume worker");
        deadline(stalled).await.expect("worker resumed");
        deadline(async {
            while runtime.disk_admission.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            std::fs::read(backup).expect("unstarted deletion must not execute after cancellation"),
            b"next transaction"
        );
        assert_eq!(runtime.disk_execution.available_permits(), 2);
    }

    #[tokio::test]
    async fn disk_task_panic_preserves_cause_and_releases_budget() {
        async fn panicking_job() -> super::super::error::Result<()> {
            panic!("injected cleanup panic");
        }
        let runtime = CleanupRuntime::new(config(1, 1, 1)).expect("cleanup runtime");
        let error = deadline(runtime.disk_operation(panicking_job(), DiskAdmission::Request))
            .await
            .expect_err("worker panic must reach its caller");
        let super::super::error::DiskError::Io(error) = error else {
            panic!("worker panic must retain its I/O error source");
        };
        let cause = error
            .get_ref()
            .expect("panic source")
            .downcast_ref::<tokio::task::JoinError>()
            .expect("preserved JoinError");
        assert!(cause.is_panic());
        assert_eq!(runtime.disk_admission.available_permits(), 4);
        assert_eq!(runtime.disk_execution.available_permits(), 2);
    }

    #[tokio::test]
    async fn coordinator_waiting_for_disk_does_not_consume_receiver_capacity() {
        let mut settings = config(1, 1, 1);
        settings.disk_pending = 1;
        settings.disk_workers = 1;
        let runtime = Arc::new(CleanupRuntime::new(settings).unwrap());
        let reservation = runtime.reserve().unwrap();
        let receiver = runtime.clone();
        let result = deadline(spawn(Some(reservation), async move {
            receiver
                .disk_operation(async { Ok(42) }, DiskAdmission::Request)
                .await
                .unwrap()
        }))
        .await
        .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn gc_shares_disk_budget_and_guarded_requests_do_not_wait_under_locks() {
        let mut settings = config(1, 1, 1);
        settings.disk_pending = 1;
        settings.disk_workers = 1;
        let runtime = Arc::new(CleanupRuntime::new(settings).unwrap());
        let receiver = runtime.clone();
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let rpc = tokio::spawn(async move {
            receiver
                .disk_operation(
                    async move {
                        entered.send(()).unwrap();
                        released.await.unwrap();
                        Ok(())
                    },
                    DiskAdmission::Request,
                )
                .await
        });
        deadline(entry).await.unwrap();
        let guarded = runtime.disk_operation(async { Ok(()) }, DiskAdmission::Guarded).await;
        assert!(matches!(guarded, Err(super::super::error::DiskError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock));
        let budget = GcBudget {
            gates: Some((runtime.disk_admission.clone(), runtime.disk_execution.clone())),
        };
        let gc = budget.step(async { Ok(7) });
        tokio::pin!(gc);
        assert!(
            futures::poll!(&mut gc).is_pending(),
            "GC must join the shared budget rather than bypass it"
        );
        release.send(()).unwrap();
        deadline(rpc).await.unwrap().unwrap();
        assert_eq!(deadline(gc).await.unwrap(), 7);

        // Also reject a guarded mutation when pending capacity is free but
        // execution is occupied. This prevents namespace -> budget inversion.
        let execution = runtime.disk_execution.clone().acquire_owned().await.unwrap();
        let guarded = runtime.disk_operation(async { Ok(()) }, DiskAdmission::Guarded).await;
        assert!(matches!(guarded, Err(super::super::error::DiskError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(runtime.disk_admission.available_permits(), 1);
        assert!(matches!(runtime.try_disk_execution(), Err(err) if err.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(runtime.disk_admission.available_permits(), 1);
        drop(execution);
        let lease_cleanup = runtime.try_disk_execution().unwrap();
        assert_eq!(runtime.disk_admission.available_permits(), 0);
        assert_eq!(runtime.disk_execution.available_permits(), 0);
        drop(lease_cleanup);
        assert_eq!(runtime.disk_execution.available_permits(), 1);
    }

    #[tokio::test]
    async fn gc_scan_limit_leaves_disk_slots_available_between_entries() {
        let runtime = Arc::new(CleanupRuntime::new(config(1, 1, 1)).unwrap());
        let first_runtime = runtime.clone();
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let first = tokio::spawn(async move {
            first_runtime
                .gc_operation(move |_| async move {
                    entered.send(()).unwrap();
                    released.await.unwrap();
                    Ok(())
                })
                .await
        });
        deadline(entry).await.unwrap();
        assert_eq!(runtime.gc_execution.available_permits(), 0);
        assert_eq!(
            deadline(runtime.disk_operation(async { Ok(42) }, DiskAdmission::Request))
                .await
                .unwrap(),
            42
        );
        let second = runtime.gc_operation(|_| async { Ok(()) });
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());
        release.send(()).unwrap();
        deadline(first).await.unwrap().unwrap();
        deadline(second).await.unwrap();
    }

    #[tokio::test]
    async fn gc_steps_borrow_scan_state_and_keep_budget_after_waiter_cancellation() {
        let runtime = Arc::new(CleanupRuntime::new(config(1, 1, 1)).expect("cleanup runtime"));
        let receiver = runtime.clone();
        let (entered, entry) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let waiter = tokio::spawn(async move {
            receiver
                .gc_operation(move |budget| async move {
                    let scan_task = tokio::task::id();
                    let mut count = 0;
                    for _ in 0..32 {
                        budget
                            .step(async {
                                assert_eq!(tokio::task::id(), scan_task, "each entry must stay on the owned scan task");
                                count += 1;
                                Ok(())
                            })
                            .await?;
                    }
                    assert_eq!(count, 32);
                    budget
                        .step(async move {
                            tokio::task::spawn_blocking(move || {
                                entered.send(()).expect("notify started syscall");
                                released.recv_timeout(Duration::from_secs(20)).expect("release syscall");
                            })
                            .await
                            .expect("blocking cleanup");
                            Ok(())
                        })
                        .await
                })
                .await
        });
        deadline(entry).await.expect("GC must start");
        waiter.abort();
        assert!(waiter.await.expect_err("cancelled waiter").is_cancelled());
        assert_eq!(runtime.gc_execution.available_permits(), 0);
        assert_eq!(runtime.disk_admission.available_permits(), 3);
        assert_eq!(runtime.disk_execution.available_permits(), 1);
        release.send(()).expect("release blocking cleanup");
        deadline(async {
            while runtime.gc_execution.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(runtime.disk_admission.available_permits(), 4);
        assert_eq!(runtime.disk_execution.available_permits(), 2);
    }

    #[test]
    fn defaults_bound_queue_waves_and_separate_threads_from_jobs() {
        const CHILD: &str = "RUSTFS_CLEANUP_DEFAULTS_TEST_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let config = Config::from_env().expect("default configuration");
            assert_eq!(config.workers, 64);
            assert!(config.async_threads <= 2);
            assert_eq!(config.blocking_threads, 4);
            assert_eq!(config.disk_workers, 4);
            assert_eq!(config.disk_pending, 64);
            config.validate().expect("defaults must be internally consistent");
            let threads = config.async_threads;
            let runtime = CleanupRuntime::new(config).expect("runtime with many logical jobs");
            assert_eq!(runtime.runtime.as_ref().expect("live runtime").metrics().num_workers(), threads);
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "disk::cleanup_runtime::tests::defaults_bound_queue_waves_and_separate_threads_from_jobs",
            ])
            .env(CHILD, "1")
            .env(WORKERS, "64")
            .env(BUDGET, "1024")
            .env(GC_WORKERS, "1")
            .env_remove(ASYNC_THREADS)
            .env_remove(BLOCKING_THREADS)
            .env_remove(DISK_WORKERS)
            .env_remove(DISK_PENDING)
            .env_remove(CPUS)
            .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_COUNTERFACTUAL_SKIP")
            .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_DEFER_HOLD_WORKER")
            .env_remove("RUSTFS_PUT_RENAME_TAIL_CLEANUP_ZERO_TARGET_TMP_DELETE_SKIP")
            .output()
            .expect("run defaults test in a fresh process");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }

    #[tokio::test]
    async fn affinity_failure_rejects_new_admission_without_dropping_admitted_work() {
        let runtime = CleanupRuntime::new(config(2, 1, 1)).unwrap();
        let reservation = runtime.reserve().unwrap();
        runtime.affinity_failed.store(true, Ordering::Release);
        assert!(runtime.reserve().is_err());
        assert_eq!(deadline(spawn(Some(reservation), async { 42 })).await.unwrap(), 42);
        assert_eq!(runtime.admission.available_permits(), 2);
    }

    #[tokio::test]
    async fn admission_is_retained_by_every_commit_and_cleanup_owner() {
        let runtime = CleanupRuntime::new(config(1, 1, 1)).unwrap();
        let request = runtime.reserve().unwrap();
        let commit = request.clone();
        let (release, released) = oneshot::channel();
        let cleanup = spawn(Some(commit.clone()), async move {
            released.await.unwrap();
        });
        drop(request);
        drop(commit);
        assert!(
            matches!(runtime.reserve(), Err(err) if err.kind() == io::ErrorKind::WouldBlock),
            "queued and running cleanup must retain admission without waiting on external locks"
        );
        release.send(()).unwrap();
        deadline(cleanup).await.unwrap();
        let next = runtime.reserve().unwrap();
        assert_eq!(runtime.admission.available_permits(), 0);
        drop(next);
        assert_eq!(runtime.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelling_cleanup_waiter_does_not_cancel_work_or_release_budget() {
        let runtime = CleanupRuntime::new(config(1, 1, 1)).unwrap();
        let reservation = runtime.reserve().unwrap();
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let waiter = tokio::spawn(run(Some(reservation), async move {
            entered.send(()).unwrap();
            released.await.unwrap();
        }));
        deadline(entry).await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(runtime.admission.available_permits(), 0);
        assert_eq!(runtime.execution.available_permits(), 0);
        release.send(()).unwrap();
        deadline(async {
            loop {
                if let Ok(reservation) = runtime.reserve() {
                    drop(reservation);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(runtime.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn worker_and_overflow_dispatch_share_one_execution_budget() {
        let runtime = CleanupRuntime::new(config(8, 2, 1)).unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
        let mut jobs = Vec::new();
        for index in 0..8 {
            let reservation = runtime.reserve().unwrap();
            let active = active.clone();
            let peak = peak.clone();
            let gate = gate.clone();
            let entered = entered.clone();
            let work = async move {
                let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(running, Ordering::SeqCst);
                entered.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
                active.fetch_sub(1, Ordering::SeqCst);
            };
            // Both a dequeued job and a queue-full continuation enter `run`;
            // no-tail temporary cleanup uses direct owned dispatch.
            jobs.push(if index % 2 == 0 {
                tokio::spawn(run(Some(reservation), work))
            } else {
                spawn(Some(reservation), work)
            });
        }
        deadline(entries.recv()).await.unwrap();
        deadline(entries.recv()).await.unwrap();
        assert_eq!(runtime.execution.available_permits(), 0);
        assert!(entries.try_recv().is_err());
        assert_eq!(runtime.admission.available_permits(), 0);
        gate.add_permits(8);
        for job in jobs {
            deadline(job).await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.admission.available_permits(), 8);
    }

    #[tokio::test]
    async fn cleanup_children_and_blocking_work_use_dedicated_threads() {
        let runtime = CleanupRuntime::new(config(1, 1, 1)).unwrap();
        let task = spawn(Some(runtime.reserve().unwrap()), async move {
            assert_eq!(std::thread::current().name(), Some("rustfs-cleanup"));
            tokio::spawn(async {
                assert_eq!(std::thread::current().name(), Some("rustfs-cleanup"));
                tokio::task::spawn_blocking(|| {
                    assert_eq!(std::thread::current().name(), Some("rustfs-cleanup"));
                })
                .await
                .unwrap();
            })
            .await
            .unwrap();
        });
        deadline(task).await.unwrap();
        assert_ne!(std::thread::current().name(), Some("rustfs-cleanup"));
    }

    #[tokio::test]
    async fn blocking_pool_saturation_does_not_block_foreground_pool() {
        let runtime = CleanupRuntime::new(config(2, 2, 1)).unwrap();
        let (entered, entry) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let first = spawn(Some(runtime.reserve().unwrap()), async move {
            tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(20)).unwrap();
            })
            .await
            .unwrap();
        });
        deadline(entry).await.unwrap();
        let (submitted, submission) = oneshot::channel();
        let (second_entered, mut second_entry) = oneshot::channel();
        let second = spawn(Some(runtime.reserve().unwrap()), async move {
            let task = tokio::task::spawn_blocking(move || {
                second_entered.send(()).unwrap();
            });
            submitted.send(()).unwrap();
            task.await.unwrap();
        });
        deadline(submission).await.unwrap();
        assert!(matches!(second_entry.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        assert_eq!(deadline(tokio::task::spawn_blocking(|| 42)).await.unwrap(), 42);
        release.send(()).unwrap();
        deadline(first).await.unwrap();
        deadline(second).await.unwrap();
        deadline(second_entry).await.unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn affinity_covers_async_and_blocking_threads_without_changing_caller() {
        let caller = rustix::thread::sched_getaffinity(None).unwrap();
        let cpu = (0..rustix::thread::CpuSet::MAX_CPU).find(|cpu| caller.is_set(*cpu)).unwrap();
        let mut config = config(1, 1, 1);
        config.cpus = vec![cpu];
        let runtime = CleanupRuntime::new(config).unwrap();
        deadline(spawn(Some(runtime.reserve().unwrap()), async move {
            let expected = {
                let mut set = rustix::thread::CpuSet::new();
                set.set(cpu);
                set
            };
            assert_eq!(rustix::thread::sched_getaffinity(None).unwrap(), expected);
            let actual = tokio::task::spawn_blocking(|| rustix::thread::sched_getaffinity(None).unwrap())
                .await
                .unwrap();
            assert_eq!(actual, expected);
        }))
        .await
        .unwrap();
        assert_eq!(rustix::thread::sched_getaffinity(None).unwrap(), caller);
        assert!(validate_cpus(&[rustix::thread::CpuSet::MAX_CPU]).is_err());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn explicit_cpu_affinity_is_rejected_on_unsupported_platforms() {
        assert_eq!(validate_cpus(&[0]).unwrap_err().kind(), io::ErrorKind::Unsupported);
        validate_cpus(&[]).unwrap();
    }
}
