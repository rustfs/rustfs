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

//! Startup and periodic reconciliation of site-replication wiring.
//!
//! The reconcilers themselves live in `admin::handlers::site_replication`, next to the
//! site-replication state they repair. Startup runs in this (infra) layer and must not
//! reach up into the admin (interface) layer, so the direction is inverted here: this
//! module owns the schedule and the contract, and the admin layer registers its
//! implementations into it from `register_site_replication_route`. That registration
//! happens while the admin router is built, which `init_startup_http_servers` does before
//! `init_startup_runtime_services` calls in — so the hooks are always installed by the
//! time startup reconciles.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(600);

/// A reconciler reports its own failures; the outcome carries no value because neither
/// caller can act on one — a site that cannot repair its replication wiring still serves S3.
type ReconcileHook = fn() -> Pin<Box<dyn Future<Output = ()> + Send>>;

static RECONCILER: OnceLock<ReconcileHook> = OnceLock::new();

/// Install the admin layer's reconciler. Idempotent: a second call is ignored, which keeps
/// repeated router construction (tests, the embedded server) from panicking.
pub(crate) fn register_site_replication_reconciler(reconcile: ReconcileHook) {
    let _ = RECONCILER.set(reconcile);
}

/// Repair drifted site-replication wiring, immediately and then on a timer.
///
/// The first pass runs inside the spawned task rather than on the caller's path: it walks
/// every bucket, so awaiting it would delay the rest of startup in proportion to bucket
/// count. It is also spawned unconditionally, including when IAM bootstrap was deferred —
/// a node that recovers IAM in the background would otherwise keep self-pointing rules
/// until someone restarted it. The reconciler itself returns early while IAM or the object
/// store are still unavailable, so an early tick is silent rather than noisy.
///
/// Later passes are no-ops once the wiring matches — the bucket pass compares the serialized
/// targets and rule set before writing — so a healthy deployment reads and writes nothing.
pub(crate) fn spawn_site_replication_reconcile_task(ctx: CancellationToken) {
    if RECONCILER.get().is_none() {
        warn!("site replication reconciler is not registered; periodic reconcile disabled");
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ctx.cancelled() => break,
                // The first tick fires immediately, which is the startup repair pass.
                _ = ticker.tick() => {
                    if let Some(reconcile) = RECONCILER.get() {
                        reconcile().await;
                    }
                }
            }
        }
    });
}
