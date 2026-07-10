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

//! Write-side counterpart to [`super::body_cache_hook`].
//!
//! ecstore terminates object bodies from paths that never pass through the
//! app-layer usecases: lifecycle/scanner expiry, non-current version cleanup,
//! and restored-copy expiry all delete objects directly on the store. The GET
//! body cache is keyed on `(bucket, object, versionId, etag, size, variant)`
//! and every lookup follows a fresh metadata quorum, so a stale entry can never
//! be *served* after its object is gone (the GET fails at metadata resolution
//! first). What lingers is dead body bytes resident until TTL, which evict live
//! hot entries — a hygiene and capacity problem, not a correctness one.
//!
//! This hook lets the app-layer cache adapter drop those bodies from its index
//! the moment ecstore removes the object (ODC-26, backlog#1131).

use crate::object_api::hook_slot::HookSlot;
use std::sync::Arc;

/// Invalidates cached object bodies after an ecstore-internal mutation removed
/// them. Keyed on `(bucket, object)`; the implementation invalidates every
/// cached version/etag under that identity.
#[async_trait::async_trait]
pub trait ObjectMutationHook: Send + Sync + 'static {
    async fn after_object_mutation(&self, bucket: &str, object: &str);
}

// A `HookSlot`, for the same reason as the GET hook slot: arc-swap cannot hold
// an unsized `Arc<dyn ObjectMutationHook>`. Registration is a startup event and
// each delete-path invocation only clones an `Arc` under the read guard,
// negligible next to the delete it accompanies.
static OBJECT_MUTATION_HOOK: HookSlot<dyn ObjectMutationHook> = HookSlot::new();

/// Register (or re-register) the process-wide object mutation hook.
///
/// Re-registration atomically swaps to `hook`, mirroring the GET body hook so a
/// rebuilt `AppContext` leaves ecstore's delete paths pointed at the newest
/// adapter.
pub fn register_object_mutation_hook(hook: Arc<dyn ObjectMutationHook>) {
    OBJECT_MUTATION_HOOK.register(
        hook,
        "object mutation cache hook re-registered with a different instance; \
         the previous adapter's cache is now unreachable by ecstore's delete paths",
    );
}

/// The registered hook, if any.
fn object_mutation_hook() -> Option<Arc<dyn ObjectMutationHook>> {
    OBJECT_MUTATION_HOOK.get()
}

/// Invoke the registered hook for `(bucket, object)`, if one is installed.
///
/// A single `None` branch when the cache feature is off, so the ecstore delete
/// paths pay nothing beyond one relaxed lock read when unconfigured.
pub(crate) async fn notify_object_mutation(bucket: &str, object: &str) {
    if let Some(hook) = object_mutation_hook() {
        hook.after_object_mutation(bucket, object).await;
    }
}

/// Test-only: unregister the hook so tests can register and clear the slot
/// deterministically without leaking a hook into unrelated tests.
#[cfg(test)]
pub(crate) fn clear_object_mutation_hook() {
    OBJECT_MUTATION_HOOK.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingHook {
        calls: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait::async_trait]
    impl ObjectMutationHook for RecordingHook {
        async fn after_object_mutation(&self, bucket: &str, object: &str) {
            self.calls.lock().unwrap().push((bucket.to_string(), object.to_string()));
        }
    }

    #[tokio::test]
    #[serial_test::serial(object_mutation_hook)]
    async fn notify_invokes_registered_hook_with_identity() {
        clear_object_mutation_hook();
        let calls = Arc::new(Mutex::new(Vec::new()));
        register_object_mutation_hook(Arc::new(RecordingHook {
            calls: Arc::clone(&calls),
        }));

        notify_object_mutation("bucket", "photos/a.jpg").await;

        assert_eq!(&*calls.lock().unwrap(), &[("bucket".to_string(), "photos/a.jpg".to_string())]);
        clear_object_mutation_hook();
    }

    #[tokio::test]
    #[serial_test::serial(object_mutation_hook)]
    async fn notify_without_registered_hook_is_noop() {
        clear_object_mutation_hook();
        // Must not panic when no hook is installed (the cache feature is off).
        notify_object_mutation("bucket", "object").await;
    }
}
