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

use crate::{LockClient, LockId};
use std::sync::{Arc, LazyLock};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct UnlockJob {
    /// Entries to release: each (LockId, client) pair will be released independently.
    entries: Vec<(LockId, Arc<dyn LockClient>)>,
}

#[derive(Debug)]
struct UnlockRuntime {
    tx: mpsc::Sender<UnlockJob>,
}

// Global unlock runtime with background worker
static UNLOCK_RUNTIME: LazyLock<UnlockRuntime> = LazyLock::new(|| {
    // Larger buffer to reduce contention during bursts
    let (tx, mut rx) = mpsc::channel::<UnlockJob>(8192);

    // Spawn background worker when first used; assumes a Tokio runtime is available
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            // Best-effort release across all (LockId, client) entries.
            let mut any_ok = false;
            for (lock_id, client) in job.entries.into_iter() {
                if client.release(&lock_id).await.unwrap_or(false) {
                    any_ok = true;
                }
            }

            if !any_ok {
                tracing::warn!("LockGuard background release failed for one or more entries");
            } else {
                tracing::debug!("LockGuard background released one or more entries");
            }
        }
    });

    UnlockRuntime { tx }
});

/// A RAII guard that releases the lock asynchronously when dropped.
#[derive(Debug)]
pub struct LockGuard {
    /// The public-facing lock id. For multi-client scenarios this is typically
    /// an aggregate id; for single-client it is the only id.
    lock_id: LockId,
    /// All underlying (LockId, client) entries that should be released when the
    /// guard is dropped.
    entries: Vec<(LockId, Arc<dyn LockClient>)>,
    /// If true, Drop will not try to release (used if user manually released).
    disarmed: bool,
}

impl LockGuard {
    /// Create a new guard.
    ///
    /// - `lock_id` is the id returned to the caller (`lock_id()`).
    /// - `entries` is the full list of underlying (LockId, client) pairs
    ///   that should be released when this guard is dropped.
    pub(crate) fn new(lock_id: LockId, entries: Vec<(LockId, Arc<dyn LockClient>)>) -> Self {
        Self {
            lock_id,
            entries,
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
            entries: self.entries.clone(),
        };

        // Try a non-blocking send to avoid panics in Drop
        if let Err(err) = UNLOCK_RUNTIME.tx.try_send(job) {
            // Channel full or closed; best-effort fallback: spawn a detached task
            let entries = self.entries.clone();
            tracing::warn!(
                "LockGuard channel send failed ({}), spawning fallback unlock task for {} entries",
                err,
                entries.len()
            );

            // If runtime is not available, this will panic; but in RustFS we are inside Tokio contexts.
            let handle = tokio::spawn(async move {
                let futures_iter = entries.into_iter().map(|(lock_id, client)| {
                    async move { client.release(&lock_id).await.unwrap_or(false) }
                });
                let _ = futures::future::join_all(futures_iter).await;
            });
            // Explicitly drop the JoinHandle to acknowledge detaching the task.
            drop(handle);
        }
    }
}
