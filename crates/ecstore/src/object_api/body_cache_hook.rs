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
use std::sync::{Arc, OnceLock};

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

static GET_OBJECT_BODY_CACHE_HOOK: OnceLock<Arc<dyn GetObjectBodyCacheHook>> = OnceLock::new();

/// Register the process-wide GET body cache hook. First registration wins;
/// later calls are ignored so tests and re-inits cannot swap the hook midway.
pub fn register_get_object_body_cache_hook(hook: Arc<dyn GetObjectBodyCacheHook>) {
    let _ = GET_OBJECT_BODY_CACHE_HOOK.set(hook);
}

/// The registered hook, if any.
pub(crate) fn get_object_body_cache_hook() -> Option<&'static Arc<dyn GetObjectBodyCacheHook>> {
    GET_OBJECT_BODY_CACHE_HOOK.get()
}
