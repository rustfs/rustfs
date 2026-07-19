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

//! Engine-only object body cache for RustFS.
//!
//! App-layer semantics and storage-specific read paths stay in the `rustfs`
//! crate; this crate owns the key, the eviction backend and the metrics.
//!
//! # Correctness boundary
//!
//! An entry is keyed by object identity — bucket, object, version, etag, size
//! and body variant — and every lookup runs after the caller has resolved fresh
//! metadata from a read quorum. Serving a hit is therefore sound because the
//! key matched metadata that was just read, not because the entry was recently
//! invalidated. Invalidation is process-local and is **hygiene**: it frees
//! capacity promptly, but the key is what keeps a stale body from being served.
//!
//! # Timing side channel
//!
//! A cache hit skips the erasure read, bitrot verification and decode, so it is
//! reliably faster than a miss. Any principal authorized to read an object can
//! therefore infer, by timing a single GET, whether *someone* read that object
//! within the entry's lifetime (`ttl` / `time_to_idle`).
//!
//! This never crosses an authorization boundary: the probe requires read access
//! to the exact bucket and object, which is checked before the cache is
//! consulted. It does leak co-tenants' recent access patterns on objects the
//! observer may already read. Deployments where access-pattern confidentiality
//! matters — a bucket shared read-only between competing tenants — should leave
//! the cache disabled for those buckets. Adding timing noise is not a viable
//! mitigation: it would cost exactly the latency the cache exists to save.

pub mod backend;
pub mod cache;
pub mod config;
pub mod entry;
pub mod error;
pub mod index;
pub mod key;
pub mod memory;
pub mod metrics;
pub mod moka_backend;
pub mod noop;
pub mod singleflight;
pub mod starshard_index;
pub mod stats;

pub use cache::{
    ObjectDataCache, ObjectDataCacheBodyReservation, ObjectDataCacheFillResult, ObjectDataCacheGetPlan,
    ObjectDataCacheGetRequest, ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult, ObjectDataCacheLookup,
    ObjectDataCacheReservedBody,
};
pub use config::{ObjectDataCacheConfig, ObjectDataCacheMode};
pub use error::ObjectDataCacheConfigError;
pub use key::{NULL_VERSION_ID, ObjectDataCacheBodyVariant, ObjectDataCacheIdentity, ObjectDataCacheKey};
pub use stats::{ObjectDataCacheStats, ObjectDataCacheStatsSnapshot};
