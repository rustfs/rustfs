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

use std::sync::Arc;

use once_cell::sync::Lazy;
use std::thread;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

use crate::{client::LockClient, types::LockId};

#[derive(Debug, Clone)]
struct UnlockJob {
    lock_id: LockId,
    clients: Vec<Arc<dyn LockClient>>, // cloned Arcs; cheap and shares state
}

// Global unlock runtime with background worker running on a dedicated thread-bound Tokio runtime.
// This avoids depending on the application's Tokio runtime lifetimes/cancellation scopes.
static UNLOCK_TX: Lazy<mpsc::Sender<UnlockJob>> = Lazy::new(|| {
    // Larger buffer to reduce contention during bursts
    let (tx, mut rx) = mpsc::channel::<UnlockJob>(8192);

    // Spawn a dedicated OS thread that owns its own Tokio runtime to process unlock jobs.
    thread::Builder::new()
        .name("rustfs-lock-unlocker".to_string())
        .spawn(move || {
            // A lightweight current-thread runtime is sufficient here.
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build Tokio runtime for background unlock jobs (possible causes: resource exhaustion, thread limit, Tokio misconfiguration)");

            rt.block_on(async move {
                while let Some(job) = rx.recv().await {
                    // Best-effort release across clients; try all, success if any succeeds
                    let mut any_ok = false;
                    let lock_id = job.lock_id.clone();
                    for client in job.clients.into_iter() {
                        if client.release(&lock_id).await.unwrap_or(false) {
                            any_ok = true;
                        }
                    }
                    if !any_ok {
                        tracing::warn!("LockGuard background release failed for {}", lock_id);
                    } else {
                        tracing::debug!("LockGuard background released {}", lock_id);
                    }
                }
            });
        })
        .expect("failed to spawn unlock worker thread");

    tx
});

/// A RAII guard that releases the lock asynchronously when dropped.
#[derive(Debug)]
pub struct LockGuard {
    lock_id: LockId,
    clients: Vec<Arc<dyn LockClient>>,
    /// If true, Drop will not try to release (used if user manually released).
    disarmed: bool,
}

impl LockGuard {
    pub(crate) fn new(lock_id: LockId, clients: Vec<Arc<dyn LockClient>>) -> Self {
        Self {
            lock_id,
            clients,
            disarmed: false,
        }
    }

    /// Get the lock id associated with this guard
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }

    /// Manually disarm the guard so dropping it won't release the lock.
    /// Call this if you explicitly released the lock elsewhere.
    pub fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }

        let job = UnlockJob {
            lock_id: self.lock_id.clone(),
            clients: self.clients.clone(),
        };

        // Try a non-blocking send to avoid panics in Drop
        if let Err(err) = UNLOCK_TX.try_send(job) {
            // Channel full or closed; best-effort fallback using a dedicated thread runtime
            let lock_id = self.lock_id.clone();
            let clients = self.clients.clone();
            tracing::warn!(
                "LockGuard channel send failed ({}), spawning fallback unlock thread for {}",
                err,
                lock_id.clone()
            );

            // Use a short-lived background thread to execute the async releases on its own runtime.
            let _ = thread::Builder::new()
                .name("rustfs-lock-unlock-fallback".to_string())
                .spawn(move || {
                    let rt = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to build fallback unlock runtime in LockGuard::drop fallback thread. This indicates resource exhaustion or misconfiguration (e.g., thread limits, Tokio runtime issues). Remediation: check system resource limits, ensure sufficient threads are available, and verify Tokio runtime configuration.");
                    rt.block_on(async move {
                        let futures_iter = clients.into_iter().map(|client| {
                            let id = lock_id.clone();
                            async move { client.release(&id).await.unwrap_or(false) }
                        });
                        let _ = futures::future::join_all(futures_iter).await;
                    });
                });
        }
    }
}
