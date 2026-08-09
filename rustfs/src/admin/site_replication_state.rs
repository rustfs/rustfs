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

//! Locking primitive for the site-replication state object (P1-15,
//! rustfs/backlog#1675 B2).
//!
//! `config/site-replication/state.json` is mutated by read-modify-write
//! sequences spread over many call sites: admin handlers, the retry-event
//! writers on every hook broadcast path, and the service-side reload driven
//! over node RPC. Historically only some of them held the process-local
//! mutex and none held a distributed lock across the whole RMW, so
//! concurrent writers overwrote each other (single-process for the unlocked
//! writers, cross-node for everyone).
//!
//! `with_site_replication_state_lock` is the single transaction boundary:
//! it holds the process-local mutex AND the distributed config-object write
//! lock (the pattern proven by the repair state,
//! `update_site_replication_repair_state`) for the duration of the caller's
//! closure. All IO inside the closure must use the `*_no_lock` config
//! helpers — the locked variants would self-deadlock on the same object
//! lock. Do not perform peer network calls or take other config locks
//! inside the closure.
//!
//! The process-local mutex is transitional: call sites still outside this
//! primitive serialize against migrated ones through it. Once every RMW
//! call site goes through here (P1-15 PR2) it will be removed, leaving the
//! object lock as the only mechanism.
//!
//! Lock order (unchanged from the historical comment next to the mutex):
//! lifecycle -> bucket operation -> repair admission -> state (process
//! mutex, then state object lock) -> per-bucket metadata.

use crate::admin::storage_api::runtime::ECStore;
use crate::storage::storage_api::with_config_object_write_lock;
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::sync::Arc;

use super::runtime_sources::current_object_store_handle;

/// Config object holding the whole site-replication state, including the
/// retry-event queue. Shared by the typed handler-side accessors and the
/// byte-level tolerant reload on the service side.
pub(crate) const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";

/// Transitional process-local mutex — see the module docs. Stays private to
/// this module (owner-local static, enforced by
/// `scripts/check_architecture_migration_rules.sh`); callers go through
/// [`site_replication_state_process_guard`].
static SITE_REPLICATION_STATE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Owner helper for the transitional process mutex: the RMW call sites in
/// `handlers::site_replication` that PR2 has not migrated to
/// [`with_site_replication_state_lock`] yet hold this guard so they stay
/// mutually exclusive with the migrated ones. Removed together with the
/// mutex once every call site runs inside the transaction boundary.
pub(crate) async fn site_replication_state_process_guard() -> tokio::sync::MutexGuard<'static, ()> {
    SITE_REPLICATION_STATE_LOCK.lock().await
}

/// Run `operation` under the site-replication state transaction boundary:
/// process mutex first, then the distributed state-object write lock.
pub(crate) async fn with_site_replication_state_lock<T, F, Fut>(operation: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
{
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    with_site_replication_state_lock_on(store, operation).await
}

/// Context-store variant for callers that resolve their store from an
/// explicit [`AppContext`] (the service-side reload driven over node RPC).
pub(crate) async fn with_site_replication_state_lock_on<T, F, Fut>(store: Arc<ECStore>, operation: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
{
    let _process_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    with_config_object_write_lock(store, SITE_REPLICATION_STATE_PATH.to_string(), operation)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock site replication state failed: {e}")))?
}
