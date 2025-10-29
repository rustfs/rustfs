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

use std::time::Duration;
use sysinfo::{RefreshKind, System};

#[inline]
fn compute_default_thread_stack_size() -> usize {
    // Baseline: Release 1 MiB，Debug 2 MiB；macOS at least 2 MiB
    // macOS is more conservative: many system libraries and backtracking are more "stack-eating"
    if cfg!(debug_assertions) || cfg!(target_os = "macos") {
        2 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
    } else {
        rustfs_config::DEFAULT_THREAD_STACK_SIZE
    }
}

#[inline]
fn detect_cores() -> usize {
    // Priority physical cores, fallback logic cores, minimum 1
    let mut sys = System::new_with_specifics(RefreshKind::everything().without_memory().without_processes());
    sys.refresh_cpu_all();
    sys.cpus().len().max(1)
}

#[inline]
fn compute_default_worker_threads() -> usize {
    // Physical cores are used by default (closer to CPU compute resources and cache topology)
    detect_cores()
}

/// Default max_blocking_threads calculations based on sysinfo:
/// 16 cores -> 1024; more than 16 cores are doubled by multiples:
/// 1..=16 -> 1024, 17..=32 -> 2048, 33..=64 -> 4096, and so on.
fn compute_default_max_blocking_threads() -> usize {
    const BASE_CORES: usize = rustfs_config::DEFAULT_WORKER_THREADS;
    const BASE_THREADS: usize = rustfs_config::DEFAULT_MAX_BLOCKING_THREADS;

    let cores = detect_cores();

    let mut threads = BASE_THREADS;
    let mut threshold = BASE_CORES;

    // When the number of cores exceeds the threshold, the number of threads is doubled for each doubling threshold
    while cores > threshold {
        threads = threads.saturating_mul(2);
        threshold = threshold.saturating_mul(2);
    }

    threads
}

/// Customize the Tokio runtime configuration
/// These configurations can be adjusted by environment variables
/// to optimize performance based on the deployment environment
/// Custom Tokio runtime builder (can be fully overridden by ENV)
/// - RUSTFS_RUNTIME_WORKER_THREADS
/// - RUSTFS_RUNTIME_MAX_BLOCKING_THREADS
/// - RUSTFS_RUNTIME_THREAD_STACK_SIZE
/// - RUSTFS_RUNTIME_THREAD_KEEP_ALIVE
/// - RUSTFS_RUNTIME_GLOBAL_QUEUE_INTERVAL
/// - RUSTFS_RUNTIME_EVENT_INTERVAL
/// - RUSTFS_RUNTIME_THREAD_NAME
/// - RUSTFS_RUNTIME_MAX_IO_EVENTS_PER_TICK
/// - RUSTFS_RUNTIME_THREAD_PRINT_ENABLED
///
/// Returns: Configured Tokio runtime builder
/// # Panics
/// Panics if environment variable values are invalid
/// # Examples
/// ```no_run
/// use rustfs_server::get_tokio_runtime_builder;
/// let builder = get_tokio_runtime_builder();
/// let runtime = builder.build().unwrap();
/// ```
pub(crate) fn get_tokio_runtime_builder() -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    // Worker threads(Default physical cores)
    let default_worker_threads = compute_default_worker_threads();
    let worker_threads = rustfs_utils::get_env_usize(rustfs_config::ENV_WORKER_THREADS, default_worker_threads);
    builder.worker_threads(worker_threads);

    // Max blocking threads: Prioritize environment variables, otherwise the default value is dynamically calculated based on sysinfo
    let default_max_blocking_threads = compute_default_max_blocking_threads();
    let max_blocking_threads = rustfs_utils::get_env_usize(rustfs_config::ENV_MAX_BLOCKING_THREADS, default_max_blocking_threads);
    builder.max_blocking_threads(max_blocking_threads);

    // Thread stack size (environment variables first, followed by dynamic default by platform/build type)
    let default_stack = compute_default_thread_stack_size();
    let thread_stack_size = rustfs_utils::get_env_usize(rustfs_config::ENV_THREAD_STACK_SIZE, default_stack);
    builder.thread_stack_size(thread_stack_size);

    // Thread keep alive(Blocking the thread pool is kept alive)
    let thread_keep_alive =
        rustfs_utils::get_env_u64(rustfs_config::ENV_THREAD_KEEP_ALIVE, rustfs_config::DEFAULT_THREAD_KEEP_ALIVE);
    builder.thread_keep_alive(Duration::from_secs(thread_keep_alive));

    // Global queue interval(Task Fairness/Throughput Tradeoff)
    let global_queue_interval =
        rustfs_utils::get_env_u32(rustfs_config::ENV_GLOBAL_QUEUE_INTERVAL, rustfs_config::DEFAULT_GLOBAL_QUEUE_INTERVAL);
    builder.global_queue_interval(global_queue_interval);

    // Event interval(View the interval of I/O/timer events)
    let event_interval = rustfs_utils::get_env_u32(rustfs_config::ENV_EVENT_INTERVAL, rustfs_config::DEFAULT_EVENT_INTERVAL);
    builder.event_interval(event_interval);

    // Thread name
    let thread_name = rustfs_utils::get_env_str(rustfs_config::ENV_THREAD_NAME, rustfs_config::DEFAULT_THREAD_NAME);
    builder.thread_name(thread_name.clone());

    // Enable I/O driver and set the maximum number of I/O events per tick (nevents)
    let max_io_events_per_tick =
        rustfs_utils::get_env_usize(rustfs_config::ENV_MAX_IO_EVENTS_PER_TICK, rustfs_config::DEFAULT_MAX_IO_EVENTS_PER_TICK);
    builder.enable_all().max_io_events_per_tick(max_io_events_per_tick);

    // Optional: Simple log of thread start/stop
    if print_tokio_thread_enable() {
        builder
            .on_thread_start(|| {
                let id = std::thread::current().id();
                println!(
                    "RustFS Worker Thread running - initializing resources time: {:?}, thread id: {:?}",
                    chrono::Utc::now().to_rfc3339(),
                    id
                );
            })
            .on_thread_stop(|| {
                let id = std::thread::current().id();
                println!(
                    "RustFS Worker Thread stopping - cleaning up resources time: {:?}, thread id: {:?}",
                    chrono::Utc::now().to_rfc3339(),
                    id
                )
            });
    }

    println!(
        "Starting Tokio runtime with configured parameters:\n\
         worker_threads: {worker_threads}, max_blocking_threads: {max_blocking_threads}, \
         thread_stack_size: {thread_stack_size}, thread_keep_alive: {thread_keep_alive}, \
         global_queue_interval: {global_queue_interval}, event_interval: {event_interval}, \
         max_io_events_per_tick: {max_io_events_per_tick}, thread_name: {thread_name}"
    );

    builder
}

/// Whether to print tokio threads
/// This can be useful for debugging purposes
fn print_tokio_thread_enable() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_THREAD_PRINT_ENABLED, rustfs_config::DEFAULT_THREAD_PRINT_ENABLED)
}
