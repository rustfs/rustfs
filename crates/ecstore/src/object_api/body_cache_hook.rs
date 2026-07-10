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

//! Hook that lets the application layer serve a GET body from its object data
//! cache after metadata resolution but before the erasure data read.
//!
//! The cache itself lives above ecstore (it needs app-level config, metrics
//! and invalidation), but the lookup must happen inside `get_object_reader`:
//! probing earlier would require a second metadata fan-out, and probing later
//! (after the reader is built) means a hit no longer saves any disk I/O.

use crate::object_api::ObjectInfo;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

/// Serves full-object GET bodies from a cache keyed by object identity.
///
/// Implementations must validate identity (etag/version/size) against the
/// provided `ObjectInfo`, which reflects the just-resolved metadata quorum,
/// and must return `None` for anything they cannot serve byte-identically
/// (encrypted objects, remote/transitioned objects, size mismatches, ...).
#[async_trait::async_trait]
pub trait GetObjectBodyCacheHook: Send + Sync + 'static {
    async fn lookup(&self, bucket: &str, object: &str, info: &ObjectInfo) -> Option<Bytes>;
}

// `RwLock<Option<Arc<dyn ...>>>` rather than `ArcSwapOption`: arc-swap's
// `RefCnt` is implemented only for the sized `Arc<T>` (it stores a thin
// `*mut T`), so it cannot hold an `Arc<dyn GetObjectBodyCacheHook>` without a
// sized newtype wrapper. The probe reads this slot once per full-object GET,
// but the read guard only clones an `Arc`, which is negligible next to the
// metadata quorum fan-out already completed before the probe. Registration is
// a startup / config-reload event, so writer contention is a non-issue.
static GET_OBJECT_BODY_CACHE_HOOK: RwLock<Option<Arc<dyn GetObjectBodyCacheHook>>> = RwLock::new(None);

/// Register (or re-register) the process-wide GET body cache hook.
///
/// Re-registration atomically swaps to `hook`, so a rebuilt `AppContext`
/// (config reload, or the test re-init pattern) leaves ecstore's GET probe
/// pointed at the newest adapter. A first-wins slot would instead pin the probe
/// to the original adapter while every usecase-layer fill and invalidation
/// targeted the replacement, silently degrading the feature to a 0% hit rate
/// and stranding entries in the unreachable cache until their TTL (backlog#1126).
///
/// Replacing a *different* hook instance is logged at WARN: in production the
/// hook is installed exactly once per process, so a swap to a distinct instance
/// signals an unexpected re-init and orphans the previous adapter's cache.
pub fn register_get_object_body_cache_hook(hook: Arc<dyn GetObjectBodyCacheHook>) {
    let mut slot = GET_OBJECT_BODY_CACHE_HOOK.write().unwrap_or_else(|e| e.into_inner());
    if let Some(previous) = slot.as_ref()
        && !Arc::ptr_eq(previous, &hook)
    {
        tracing::warn!(
            "GET object body cache hook re-registered with a different instance; \
             the previous adapter's cache is now unreachable by ecstore's GET probe"
        );
    }
    *slot = Some(hook);
}

/// The registered hook, if any.
pub(crate) fn get_object_body_cache_hook() -> Option<Arc<dyn GetObjectBodyCacheHook>> {
    GET_OBJECT_BODY_CACHE_HOOK.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Test-only: unregister the hook so tests can register and clear the slot
/// deterministically without leaking a hook into unrelated tests.
#[cfg(test)]
pub(crate) fn clear_get_object_body_cache_hook() {
    *GET_OBJECT_BODY_CACHE_HOOK.write().unwrap_or_else(|e| e.into_inner()) = None;
}
