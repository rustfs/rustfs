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
//! over node RPC.
//!
//! `with_site_replication_state_lock` is the single transaction boundary:
//! it holds the distributed config-object write lock (the pattern proven by
//! the repair state, `update_site_replication_repair_state`) for the
//! duration of the caller's closure. The object lock is the sole mechanism —
//! it is the only thing that can serialize two nodes of the same site, so a
//! process-local lock must never be reintroduced in front of it as if it
//! added protection. All IO inside the closure must use the `*_no_lock`
//! config helpers — the locked variants would self-deadlock on the same
//! object lock. Do not perform peer network calls or take other config locks
//! inside the closure.
//!
//! Lock order: lifecycle -> bucket operation -> repair admission
//! -> state object lock -> per-bucket metadata.

use crate::admin::storage_api::runtime::ECStore;
use crate::admin::storage_api::s3::{S3Error, S3ErrorCode, S3Result};
use crate::storage::storage_api::with_config_object_write_lock;
use std::sync::Arc;

use super::runtime_sources::current_object_store_handle;

/// Config object holding the whole site-replication state, including the
/// retry-event queue. Shared by the typed handler-side accessors and the
/// byte-level tolerant reload on the service side.
pub(crate) const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";

/// Run `operation` under the site-replication state transaction boundary:
/// the distributed state-object write lock.
pub(crate) async fn with_site_replication_state_lock<T, F, Fut>(operation: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
{
    let store = current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;
    with_site_replication_state_lock_on(store, operation).await
}

/// Context-store variant for callers that resolve their store from an
/// explicit [`AppContext`] (the service-side reload driven over node RPC).
///
/// This is the whole boundary: a state-object write lock serializes writers
/// in *different* processes, which is what two nodes of one site are and
/// what a process mutex could never cover.
pub(crate) async fn with_site_replication_state_lock_on<T, F, Fut>(store: Arc<ECStore>, operation: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = S3Result<T>> + Send + 'static,
{
    with_config_object_write_lock(store, SITE_REPLICATION_STATE_PATH.to_string(), operation)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock site replication state failed: {e}")))?
}
