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

//! Lifecycle control for long-lived peer/disk background monitor tasks.
//!
//! Peer health and recovery monitors (see [`super::peer_s3_client`],
//! [`super::peer_rest_client`], [`super::remote_disk`]) are detached
//! `tokio::spawn` tasks that run for the whole life of the process and each hold
//! a `tracing::Span` via `.instrument(..)`. If such a task is still alive when
//! the Tokio runtime is dropped, its future — and the `Span` it holds — is
//! dropped during worker-thread thread-local-storage (TLS) destruction. At that
//! point the `tracing-subscriber` fmt layer's `on_close` handler can touch an
//! already-destroyed TLS slot and panic with
//! `cannot access a Thread Local Storage value during or after destruction`,
//! which escalates to a panic-during-panic abort (see issue #4264).
//!
//! To avoid this, all such monitors are spawned through
//! [`spawn_background_monitor`], which races the monitor future against a
//! process-global shutdown token. Calling [`shutdown_background_monitors`]
//! during graceful shutdown — before the runtime is torn down — resolves every
//! monitor promptly so its `Span` is dropped while the runtime and the tracing
//! subscriber are still alive.

use std::future::Future;
use std::sync::OnceLock;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

/// Process-global shutdown signal for background monitor tasks.
fn global_monitor_shutdown_token() -> &'static CancellationToken {
    static TOKEN: OnceLock<CancellationToken> = OnceLock::new();
    TOKEN.get_or_init(CancellationToken::new)
}

/// Request shutdown of every background monitor task.
///
/// Intended to be called once during graceful shutdown, *before* the Tokio
/// runtime is dropped. It is idempotent and safe to call even if no monitors
/// were ever spawned.
pub fn shutdown_background_monitors() {
    global_monitor_shutdown_token().cancel();
}

/// Spawn a long-lived background monitor task that stops on graceful shutdown.
///
/// `task` is raced against the global monitor shutdown token; when shutdown is
/// requested the future is dropped promptly (while the runtime is alive),
/// taking `span` with it. This is the only sanctioned way to spawn a monitor
/// that holds an instrumentation `Span` for its whole lifetime.
pub(crate) fn spawn_background_monitor<F>(span: Span, task: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    spawn_background_monitor_on(global_monitor_shutdown_token().clone(), span, task)
}

/// Core implementation parameterised over the shutdown token, so tests can drive
/// it with a local token without touching the process-global one.
fn spawn_background_monitor_on<F>(shutdown: CancellationToken, span: Span, task: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(
        async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        }
        .instrument(span),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn spawn_background_monitor_on_stops_when_token_cancelled() {
        let shutdown = CancellationToken::new();
        let ran_to_completion = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran_to_completion);

        // A monitor future that would otherwise never return.
        let handle = spawn_background_monitor_on(shutdown.clone(), Span::none(), async move {
            std::future::pending::<()>().await;
            flag.store(true, Ordering::SeqCst);
        });

        shutdown.cancel();

        // The wrapper resolves via the shutdown branch and the task completes,
        // dropping the never-ending future (and its span) while the runtime is
        // alive.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("monitor task should stop after shutdown")
            .expect("monitor task should not panic");
        assert!(!ran_to_completion.load(Ordering::SeqCst), "inner future must be cancelled, not completed");
    }

    #[tokio::test]
    async fn spawn_background_monitor_on_completes_when_task_finishes() {
        let shutdown = CancellationToken::new();
        let handle = spawn_background_monitor_on(shutdown, Span::none(), async {});
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("finished monitor task should join")
            .expect("monitor task should not panic");
    }

    // NOTE: intentionally no test cancels `global_monitor_shutdown_token()` — it
    // is a process-global shared by every test in this binary that spawns a real
    // monitor via `spawn_background_monitor`, so cancelling it here would race
    // those tests. The cancellation mechanism is covered above via the
    // token-parameterised `spawn_background_monitor_on`.
}
