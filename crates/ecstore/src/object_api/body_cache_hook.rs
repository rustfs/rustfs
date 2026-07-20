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

use crate::object_api::hook_slot::HookSlot;
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::storage_api_contracts::range::HTTPRangeSpec;
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    static SKIP_GET_OBJECT_BODY_CACHE_HOOK: bool;
}

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

// A `HookSlot` (RwLock<Option<Arc<dyn ...>>>) rather than `ArcSwapOption`:
// arc-swap's `RefCnt` is implemented only for the sized `Arc<T>` (it stores a
// thin `*mut T`), so it cannot hold an `Arc<dyn GetObjectBodyCacheHook>`
// without a sized newtype wrapper. The probe reads this slot once per
// full-object GET, but the read guard only clones an `Arc`, which is negligible
// next to the metadata quorum fan-out already completed before the probe.
// Registration is a startup / config-reload event, so writer contention is a
// non-issue.
static GET_OBJECT_BODY_CACHE_HOOK: HookSlot<dyn GetObjectBodyCacheHook> = HookSlot::new();

/// Register (or re-register) the process-wide GET body cache hook.
///
/// Re-registration atomically swaps to `hook`, so a rebuilt `AppContext`
/// (config reload, or the test re-init pattern) leaves ecstore's GET probe
/// pointed at the newest adapter. A first-wins slot would instead pin the probe
/// to the original adapter while every usecase-layer fill and invalidation
/// targeted the replacement, silently degrading the feature to a 0% hit rate
/// and stranding entries in the unreachable cache until their TTL (backlog#1126).
pub fn register_get_object_body_cache_hook(hook: Arc<dyn GetObjectBodyCacheHook>) {
    GET_OBJECT_BODY_CACHE_HOOK.register(
        hook,
        "GET object body cache hook re-registered with a different instance; \
         the previous adapter's cache is now unreachable by ecstore's GET probe",
    );
}

/// Unregister the process-wide GET body cache hook.
///
/// Config reloads use this when body caching becomes disabled so an adapter
/// retained by the previous configuration cannot continue serving stale hits.
pub fn unregister_get_object_body_cache_hook() {
    GET_OBJECT_BODY_CACHE_HOOK.clear();
}

/// Probes the registered hook against an already resolved metadata snapshot.
/// Staged GET callers use this once before suppressing the nested reader probe,
/// preserving the legacy hook contract.
#[non_exhaustive]
pub enum GetObjectBodyCacheHookLookup {
    Ineligible,
    Absent,
    Miss,
    Hit(Bytes),
}

/// Returns the complete plaintext length only when the request can safely use
/// the body-cache hook. Callers may use this before conditional decisions so
/// ineligible reads retain the established reader-path error precedence.
pub fn get_object_body_cache_plaintext_len(
    range: &Option<HTTPRangeSpec>,
    opts: &ObjectOptions,
    info: &ObjectInfo,
) -> Option<i64> {
    crate::set_disk::body_cache_plaintext_len(range, opts, info)
}

pub async fn lookup_get_object_body_cache_hook(
    bucket: &str,
    object: &str,
    range: &Option<HTTPRangeSpec>,
    opts: &ObjectOptions,
    info: &ObjectInfo,
) -> GetObjectBodyCacheHookLookup {
    let Some(plaintext_len) = get_object_body_cache_plaintext_len(range, opts, info) else {
        return GetObjectBodyCacheHookLookup::Ineligible;
    };
    let Some(hook) = get_object_body_cache_hook() else {
        return GetObjectBodyCacheHookLookup::Absent;
    };
    match hook.lookup(bucket, object, info).await {
        Some(body) if i64::try_from(body.len()).is_ok_and(|body_len| body_len == plaintext_len) => {
            GetObjectBodyCacheHookLookup::Hit(body)
        }
        Some(_) | None => GetObjectBodyCacheHookLookup::Miss,
    }
}

/// The registered hook, if any.
pub(crate) fn get_object_body_cache_hook() -> Option<Arc<dyn GetObjectBodyCacheHook>> {
    GET_OBJECT_BODY_CACHE_HOOK.get()
}

pub(crate) fn get_object_body_cache_hook_suppressed() -> bool {
    SKIP_GET_OBJECT_BODY_CACHE_HOOK.try_with(|skip| *skip).unwrap_or(false)
}

pub(crate) async fn without_get_object_body_cache_hook<F>(future: F) -> F::Output
where
    F: Future,
{
    SKIP_GET_OBJECT_BODY_CACHE_HOOK.scope(true, future).await
}

/// Test-only: unregister the hook so tests can register and clear the slot
/// deterministically without leaking a hook into unrelated tests.
#[cfg(test)]
pub(crate) fn clear_get_object_body_cache_hook() {
    unregister_get_object_body_cache_hook();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LegacyHook {
        calls: AtomicUsize,
        body: Option<Bytes>,
    }

    #[async_trait::async_trait]
    impl GetObjectBodyCacheHook for LegacyHook {
        async fn lookup(&self, _bucket: &str, _object: &str, _info: &ObjectInfo) -> Option<Bytes> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.body.clone()
        }
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn staged_probe_preserves_legacy_hook_hit_once() {
        clear_get_object_body_cache_hook();
        let hook = Arc::new(LegacyHook {
            calls: AtomicUsize::new(0),
            body: Some(Bytes::from_static(b"legacy")),
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);

        let info = ObjectInfo {
            size: 6,
            actual_size: 6,
            ..Default::default()
        };
        let GetObjectBodyCacheHookLookup::Hit(body) =
            lookup_get_object_body_cache_hook("bucket", "object", &None, &ObjectOptions::default(), &info).await
        else {
            panic!("legacy hook hit must be returned");
        };
        assert_eq!(body, Bytes::from_static(b"legacy"));
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1);
        clear_get_object_body_cache_hook();
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn staged_probe_treats_wrong_length_legacy_body_as_authoritative_miss() {
        clear_get_object_body_cache_hook();
        let hook = Arc::new(LegacyHook {
            calls: AtomicUsize::new(0),
            body: Some(Bytes::from_static(b"short")),
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);
        let info = ObjectInfo {
            size: 6,
            actual_size: 6,
            ..Default::default()
        };

        assert!(matches!(
            lookup_get_object_body_cache_hook("bucket", "object", &None, &ObjectOptions::default(), &info).await,
            GetObjectBodyCacheHookLookup::Miss
        ));
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1);
        clear_get_object_body_cache_hook();
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn staged_probe_preserves_legacy_hook_miss_once() {
        clear_get_object_body_cache_hook();
        let hook = Arc::new(LegacyHook {
            calls: AtomicUsize::new(0),
            body: None,
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);

        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        assert!(matches!(
            lookup_get_object_body_cache_hook("bucket", "object", &None, &ObjectOptions::default(), &info).await,
            GetObjectBodyCacheHookLookup::Miss
        ));
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1);
        clear_get_object_body_cache_hook();
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn staged_probe_bypasses_raw_movement_and_restore_reads() {
        clear_get_object_body_cache_hook();
        let hook = Arc::new(LegacyHook {
            calls: AtomicUsize::new(0),
            body: Some(Bytes::from_static(b"body")),
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);
        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let mut restore = ObjectOptions::default();
        restore.transition.restore_request.days = Some(1);
        let cases = [
            ObjectOptions {
                raw_data_movement_read: true,
                ..Default::default()
            },
            ObjectOptions {
                data_movement: true,
                ..Default::default()
            },
            restore,
        ];

        for opts in &cases {
            assert!(matches!(
                lookup_get_object_body_cache_hook("bucket", "object", &None, opts, &info).await,
                GetObjectBodyCacheHookLookup::Ineligible
            ));
        }
        assert_eq!(hook.calls.load(Ordering::Relaxed), 0);
        clear_get_object_body_cache_hook();
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn staged_probe_bypasses_pre_hook_early_return_objects() {
        clear_get_object_body_cache_hook();
        let hook = Arc::new(LegacyHook {
            calls: AtomicUsize::new(0),
            body: Some(Bytes::from_static(b"body")),
        });
        register_get_object_body_cache_hook(Arc::clone(&hook) as Arc<dyn GetObjectBodyCacheHook>);
        let delete_marker = ObjectInfo {
            delete_marker: true,
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let zero = ObjectInfo::default();
        let inline = ObjectInfo {
            inlined: true,
            size: 4,
            actual_size: 4,
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };
        let version_only = ObjectInfo {
            version_only: true,
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let metadata_only = ObjectInfo {
            metadata_only: true,
            size: 4,
            actual_size: 4,
            ..Default::default()
        };

        for info in [&delete_marker, &zero, &inline, &version_only, &metadata_only] {
            assert!(matches!(
                lookup_get_object_body_cache_hook("bucket", "object", &None, &ObjectOptions::default(), info).await,
                GetObjectBodyCacheHookLookup::Ineligible
            ));
        }
        assert_eq!(hook.calls.load(Ordering::Relaxed), 0);
        clear_get_object_body_cache_hook();
    }

    #[tokio::test]
    async fn staged_reader_scope_suppresses_only_the_nested_probe() {
        assert!(!get_object_body_cache_hook_suppressed());
        without_get_object_body_cache_hook(async {
            assert!(get_object_body_cache_hook_suppressed());
        })
        .await;
        assert!(!get_object_body_cache_hook_suppressed());
    }
}
