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

//! Mission Repair Feed (MRF) intent channel.
//!
//! Producers on error paths (read decode failure, scanner metadata
//! corruption, partial-write recovery) hand a lightweight [`MrfIntent`] to the
//! heal crate through a global bounded channel. Delivery is strictly
//! non-blocking: `try_send_mrf_intent` never awaits and drops the intent
//! (counting it) when the channel is full or uninitialized — losing one heal
//! hint is always preferred over stalling an IO path. Durable replay of
//! unconsumed intents is the consumer's job (see `rustfs-heal`
//! `heal::mrf_queue`), mirroring MinIO's `.heal/mrf/list.bin`.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Bounded capacity of the global MRF channel. Backpressure is resolved by
/// dropping (and counting) intents, never by blocking the producer.
const MRF_CHANNEL_CAPACITY: usize = 8192;
const MRF_COALESCER_SHARDS: usize = 16;
const MRF_COALESCER_MAX_KEYS: usize = 8192;
const MRF_COALESCER_MAX_BYTES: usize = 16 * 1024 * 1024;
const MRF_COALESCER_TTL: Duration = Duration::from_secs(60);
const MRF_MAX_IDENTITY_COMPONENT: usize = 1024;

/// Why an intent was produced. Drives the heal priority mapping on the
/// consumer side (DecodeFailure -> Urgent, MetadataCorruption -> High,
/// PartialWrite -> Normal).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MrfKind {
    /// Erasure decode failed while serving a read (read path).
    DecodeFailure,
    /// Scanner classified object metadata as corrupt.
    MetadataCorruption,
    /// A write left the object with fewer committed shards than the set size.
    PartialWrite,
}

impl MrfKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            MrfKind::DecodeFailure => "decode-failure",
            MrfKind::MetadataCorruption => "metadata-corruption",
            MrfKind::PartialWrite => "partial-write",
        }
    }
}

/// One repair intent. Kept deliberately small so the in-memory queue and the
/// journal stay bounded; `bucket`/`object` are `Arc<str>` so re-arming an
/// intent never re-allocates the strings.
#[derive(Clone, Debug)]
pub struct MrfIntent {
    pub bucket: Arc<str>,
    pub object: Arc<str>,
    /// Version the intent targets, as raw UUID bytes.
    pub version_id: Option<[u8; 16]>,
    pub kind: MrfKind,
    /// Stable erasure-set scope when the producer has it. Kept optional so
    /// metadata corruption and legacy producers do not invent a scope.
    pub scope: Option<MrfScope>,
    /// Generation of the node-local ingress lease. It is not persisted in
    /// the journal; replayed records acquire a fresh lease when re-enqueued.
    pub lease: Option<MrfIngressLease>,
    pub enqueued_at_ms: u64,
    /// Times this intent has already been offered to the heal manager.
    /// Dropped by the consumer once it reaches `MRF_MAX_ATTEMPTS`.
    pub attempts: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MrfScope {
    pub pool_index: u32,
    pub set_index: u32,
}

/// Opaque generation used to release exactly the admission that created an
/// ingress entry. A generation prevents a late terminal callback from
/// deleting a newer retry for the same identity (ABA).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MrfIngressLease(u64);

impl MrfIngressLease {
    const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MrfDropReason {
    Disabled,
    Uninitialized,
    Full,
    OversizedIdentity,
    CoalescerFull,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MrfIngressResult {
    Enqueued,
    Coalesced,
    Dropped(MrfDropReason),
}

/// Consumer-side retry ceiling before an intent is given up on.
pub const MRF_MAX_ATTEMPTS: u8 = 3;

impl MrfIntent {
    /// Rough in-memory footprint used by the queue's byte budget.
    pub fn estimated_bytes(&self) -> usize {
        // Struct + strings + version bytes; buckets and objects are usually
        // far below this bound, so rounding up keeps the budget conservative.
        64 + self.bucket.len() + self.object.len()
    }
}

static GLOBAL_MRF_SENDER: OnceLock<mpsc::Sender<MrfIntent>> = OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct MrfIdentityKey {
    kind: MrfKind,
    bucket: Arc<str>,
    object: Arc<str>,
    version_id: Option<[u8; 16]>,
    scope: Option<MrfScope>,
}

#[derive(Debug)]
struct IngressEntry {
    lease: MrfIngressLease,
    expires_at: Instant,
    bytes: usize,
}

type MrfCoalescerShard = Mutex<HashMap<MrfIdentityKey, IngressEntry>>;
type MrfCoalescer = Box<[MrfCoalescerShard]>;

static MRF_COALESCER: OnceLock<MrfCoalescer> = OnceLock::new();
static NEXT_MRF_LEASE: AtomicU64 = AtomicU64::new(1);
static MRF_COALESCER_COUNT: AtomicUsize = AtomicUsize::new(0);
static MRF_COALESCER_BYTES: AtomicUsize = AtomicUsize::new(0);
static MRF_HASH_STATE: OnceLock<RandomState> = OnceLock::new();

fn coalescer() -> &'static [MrfCoalescerShard] {
    MRF_COALESCER.get_or_init(|| {
        (0..MRF_COALESCER_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

fn key_shard(key: &MrfIdentityKey) -> usize {
    let hash = MRF_HASH_STATE.get_or_init(RandomState::new).hash_one(key);
    usize::try_from(hash).unwrap_or(0) % MRF_COALESCER_SHARDS
}

fn canonical_version(version_id: Option<Uuid>) -> Option<[u8; 16]> {
    version_id
        .filter(|version| !version.is_nil())
        .map(|version| *version.as_bytes())
}

fn canonical_identity(
    kind: MrfKind,
    version_id: Option<[u8; 16]>,
    scope: Option<MrfScope>,
) -> (Option<[u8; 16]>, Option<MrfScope>) {
    let version_id = version_id.filter(|bytes| *bytes != [0; 16]);
    match kind {
        MrfKind::MetadataCorruption => (None, None),
        MrfKind::DecodeFailure | MrfKind::PartialWrite => (version_id, scope),
    }
}

fn identity_estimated_bytes(key: &MrfIdentityKey) -> usize {
    64usize
        .saturating_add(key.bucket.len())
        .saturating_add(key.object.len())
        .saturating_add(key.version_id.map_or(0, |_| 16))
        .saturating_add(key.scope.map_or(0, |_| 8))
}

fn reserve(counter: &AtomicUsize, limit: usize, amount: usize) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let Some(next) = current.checked_add(amount) else {
            return false;
        };
        if next > limit {
            return false;
        }
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn coalescer_admit(key: MrfIdentityKey) -> Result<MrfIngressLease, MrfIngressResult> {
    let shard = key_shard(&key);
    let mut entries = coalescer()[shard]
        .lock()
        .map_err(|_| MrfIngressResult::Dropped(MrfDropReason::CoalescerFull))?;
    let now = Instant::now();
    let before = entries.len();
    let mut expired_bytes = 0usize;
    entries.retain(|_, entry| {
        if entry.expires_at > now {
            true
        } else {
            expired_bytes = expired_bytes.saturating_add(entry.bytes);
            false
        }
    });
    let evicted = before.saturating_sub(entries.len());
    if evicted > 0 {
        MRF_COALESCER_COUNT.fetch_sub(evicted, Ordering::Relaxed);
        MRF_COALESCER_BYTES.fetch_sub(expired_bytes, Ordering::Relaxed);
        let evicted = u64::try_from(evicted).unwrap_or(u64::MAX);
        metrics::counter!("rustfs_heal_mrf_coalescer_expired_total").increment(evicted);
        metrics::counter!("rustfs_heal_mrf_coalescer_evictions_total").increment(evicted);
    }
    if entries.contains_key(&key) {
        metrics::counter!("rustfs_heal_mrf_coalesced_total").increment(1);
        return Err(MrfIngressResult::Coalesced);
    }
    let bytes = identity_estimated_bytes(&key);
    let count_reserved = reserve(&MRF_COALESCER_COUNT, MRF_COALESCER_MAX_KEYS, 1);
    let bytes_reserved = count_reserved && reserve(&MRF_COALESCER_BYTES, MRF_COALESCER_MAX_BYTES, bytes);
    if !count_reserved || !bytes_reserved {
        if count_reserved {
            MRF_COALESCER_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
        metrics::counter!("rustfs_heal_mrf_dropped_total", "reason" => "coalescer_full").increment(1);
        return Err(MrfIngressResult::Dropped(MrfDropReason::CoalescerFull));
    }
    let lease = MrfIngressLease::new(NEXT_MRF_LEASE.fetch_add(1, Ordering::Relaxed));
    if entries
        .insert(
            key,
            IngressEntry {
                lease,
                expires_at: now + MRF_COALESCER_TTL,
                bytes,
            },
        )
        .is_some()
    {
        MRF_COALESCER_COUNT.fetch_sub(1, Ordering::Relaxed);
        MRF_COALESCER_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        metrics::counter!("rustfs_heal_mrf_coalesced_total").increment(1);
        return Err(MrfIngressResult::Coalesced);
    }
    Ok(lease)
}

fn coalescer_release(key: &MrfIdentityKey, lease: Option<MrfIngressLease>) {
    let Some(lease) = lease else {
        return;
    };
    if let Ok(mut entries) = coalescer()[key_shard(key)].lock() {
        let should_remove = entries.get(key).is_some_and(|entry| entry.lease == lease);
        if should_remove {
            let bytes = entries.remove(key).map(|entry| entry.bytes).unwrap_or(0);
            MRF_COALESCER_COUNT.fetch_sub(1, Ordering::Relaxed);
            MRF_COALESCER_BYTES.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
}

/// Delivery kill-switch, set from `RUSTFS_HEAL_MRF_ENABLE`. Producers check
/// this before touching the channel so the disabled path stays allocation- and
/// sync-free.
static MRF_DELIVERY_ENABLED: AtomicBool = AtomicBool::new(true);

/// Override delivery (used at heal-runtime startup from configuration).
pub fn set_mrf_delivery_enabled(enabled: bool) {
    MRF_DELIVERY_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Whether producers currently deliver intents.
pub fn mrf_delivery_enabled() -> bool {
    MRF_DELIVERY_ENABLED.load(Ordering::Relaxed)
}

/// Create the global MRF channel and return the consumer half. Fails if the
/// channel is already initialized (the heal runtime is a singleton).
pub fn init_mrf_channel() -> Result<mpsc::Receiver<MrfIntent>, &'static str> {
    let (sender, receiver) = mpsc::channel(MRF_CHANNEL_CAPACITY);
    GLOBAL_MRF_SENDER
        .set(sender)
        .map_err(|_| "MRF channel sender already initialized")?;
    Ok(receiver)
}

/// Best-effort, non-blocking intent delivery from an error path.
///
/// Returns `true` when the intent was accepted into the channel. `false`
/// means the intent was dropped (feature disabled, channel not yet
/// initialized, or channel full) — callers must not retry or await; the
/// existing read-repair / scanner heal paths remain the safety net.
///
/// This runs on IO error paths, so it stays synchronous and cheap: one
/// bounded allocation for the two `Arc<str>` handles plus the channel slot.
pub fn try_send_mrf_intent(kind: MrfKind, bucket: &str, object: &str, version_id: Option<Uuid>) -> bool {
    matches!(
        try_send_mrf_intent_typed(kind, bucket, object, version_id, None),
        MrfIngressResult::Enqueued
    )
}

/// Typed ingress result. `Coalesced` means an equivalent in-flight channel
/// intent already exists; it is not a second executable or durable admission.
pub fn try_send_mrf_intent_typed(
    kind: MrfKind,
    bucket: &str,
    object: &str,
    version_id: Option<Uuid>,
    scope: Option<MrfScope>,
) -> MrfIngressResult {
    if !mrf_delivery_enabled() {
        return MrfIngressResult::Dropped(MrfDropReason::Disabled);
    }
    let Some(sender) = GLOBAL_MRF_SENDER.get() else {
        return MrfIngressResult::Dropped(MrfDropReason::Uninitialized);
    };
    if bucket.len() > MRF_MAX_IDENTITY_COMPONENT || object.len() > MRF_MAX_IDENTITY_COMPONENT {
        return MrfIngressResult::Dropped(MrfDropReason::OversizedIdentity);
    }
    let (version_id, scope) = canonical_identity(kind, canonical_version(version_id), scope);
    let key = MrfIdentityKey {
        kind,
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version_id,
        scope,
    };
    let lease = match coalescer_admit(key.clone()) {
        Ok(lease) => lease,
        Err(result) => return result,
    };
    let intent = MrfIntent {
        bucket: key.bucket.clone(),
        object: key.object.clone(),
        version_id: key.version_id,
        kind,
        scope,
        lease: Some(lease),
        enqueued_at_ms: unix_now_ms(),
        attempts: 0,
    };
    match sender.try_send(intent) {
        Ok(()) => MrfIngressResult::Enqueued,
        Err(mpsc::error::TrySendError::Full(_)) => {
            coalescer_release(&key, Some(lease));
            metrics::counter!("rustfs_heal_mrf_dropped_total", "reason" => "channel_full").increment(1);
            MrfIngressResult::Dropped(MrfDropReason::Full)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            coalescer_release(&key, Some(lease));
            MrfIngressResult::Dropped(MrfDropReason::Uninitialized)
        }
    }
}

/// Release the ingress key once the consumer owns the intent.
pub fn release_mrf_intent(intent: &MrfIntent) {
    release_mrf_identity(intent.kind, &intent.bucket, &intent.object, intent.version_id, intent.scope, intent.lease);
}

pub fn release_mrf_identity(
    kind: MrfKind,
    bucket: &str,
    object: &str,
    version_id: Option<[u8; 16]>,
    scope: Option<MrfScope>,
    lease: Option<MrfIngressLease>,
) {
    let (version_id, scope) = canonical_identity(kind, version_id, scope);
    coalescer_release(
        &MrfIdentityKey {
            kind,
            bucket: Arc::from(bucket),
            object: Arc::from(object),
            version_id,
            scope,
        },
        lease,
    );
}

fn unix_now_ms() -> u64 {
    // Kept trivial: the timestamp is diagnostic metadata only; wall-clock
    // failure would be a bug rather than something to handle here.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// A repair the MRF consumer landed, fanned out so retry ledgers can drop
/// entries the journal no longer tracks (backlog#1894 axis B). The payload
/// mirrors the intent identity so consumers match without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MrfRepairedEvent {
    pub bucket: Arc<str>,
    pub object: Arc<str>,
    pub version_id: Option<[u8; 16]>,
}

/// Bound on the repaired-event backlog. Notices are best-effort hints; when
/// the ring is full the oldest are dropped and the affected ledger entries
/// simply expire through their own attempts/age limits.
const MRF_REPAIRED_EVENT_CAP: usize = 4096;

static MRF_REPAIRED_EVENTS: OnceLock<std::sync::Mutex<std::collections::VecDeque<MrfRepairedEvent>>> = OnceLock::new();

/// Record that the MRF consumer landed a repair. Never blocks: the critical
/// section is a deque push under a std mutex.
pub fn note_mrf_repaired(bucket: &str, object: &str, version_id: Option<[u8; 16]>) {
    let registry = MRF_REPAIRED_EVENTS.get_or_init(|| std::sync::Mutex::new(std::collections::VecDeque::new()));
    let Ok(mut events) = registry.lock() else {
        return;
    };
    if events.len() >= MRF_REPAIRED_EVENT_CAP {
        events.pop_front();
    }
    events.push_back(MrfRepairedEvent {
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version_id,
    });
}

/// Take the repair notices recorded for `bucket`, leaving other buckets'
/// notices in place for their own scanners.
pub fn take_mrf_repaired_events_for(bucket: &str) -> Vec<MrfRepairedEvent> {
    let Some(registry) = MRF_REPAIRED_EVENTS.get() else {
        return Vec::new();
    };
    let Ok(mut events) = registry.lock() else {
        return Vec::new();
    };
    let mut taken = Vec::new();
    let mut retained = std::collections::VecDeque::with_capacity(events.len());
    while let Some(event) = events.pop_front() {
        if event.bucket.as_ref() == bucket {
            taken.push(event);
        } else {
            retained.push_back(event);
        }
    }
    *events = retained;
    taken
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intents_estimate_is_conservative() {
        let intent = MrfIntent {
            bucket: Arc::from("bucket"),
            object: Arc::from("object"),
            version_id: Some([0u8; 16]),
            kind: MrfKind::DecodeFailure,
            scope: None,
            lease: None,
            enqueued_at_ms: 0,
            attempts: 0,
        };
        assert!(intent.estimated_bytes() >= intent.bucket.len() + intent.object.len());
    }

    #[test]
    fn ingress_duplicate_identity_coalesces_and_releases_for_retry() {
        let key = MrfIdentityKey {
            kind: MrfKind::DecodeFailure,
            bucket: Arc::from("ingress-test-bucket"),
            object: Arc::from("ingress-test-object"),
            version_id: Some([9; 16]),
            scope: Some(MrfScope {
                pool_index: 3,
                set_index: 4,
            }),
        };
        let lease = coalescer_admit(key.clone()).expect("first identity should be admitted");
        for _ in 0..999 {
            assert_eq!(coalescer_admit(key.clone()), Err(MrfIngressResult::Coalesced));
        }
        coalescer_release(&key, Some(lease));
        let retry_lease = coalescer_admit(key.clone()).expect("released identity must admit a retry");
        coalescer_release(&key, Some(retry_lease));
    }

    #[test]
    fn ingress_identity_preserves_kind_scope_and_version_boundaries() {
        let (nil_version, nil_scope) = canonical_identity(
            MrfKind::DecodeFailure,
            Some([0; 16]),
            Some(MrfScope {
                pool_index: 1,
                set_index: 2,
            }),
        );
        assert_eq!(nil_version, None, "nil UUID is the unversioned identity");
        assert!(nil_scope.is_some());

        let (metadata_version, metadata_scope) = canonical_identity(
            MrfKind::MetadataCorruption,
            Some([7; 16]),
            Some(MrfScope {
                pool_index: 1,
                set_index: 2,
            }),
        );
        assert_eq!(metadata_version, None);
        assert_eq!(metadata_scope, None);
    }

    #[tokio::test]
    async fn try_send_delivers_and_respects_capacity() {
        let mut receiver = init_mrf_channel().expect("first initialization should succeed");
        assert!(init_mrf_channel().is_err(), "double initialization must fail");

        assert!(try_send_mrf_intent(MrfKind::DecodeFailure, "b", "o", Some(Uuid::nil())));
        let intent = receiver.recv().await.expect("intent should arrive");
        assert_eq!(intent.kind, MrfKind::DecodeFailure);
        assert_eq!(intent.bucket.as_ref(), "b");
        release_mrf_intent(&intent);

        // Disable delivery: producers become no-ops.
        set_mrf_delivery_enabled(false);
        assert!(!try_send_mrf_intent(MrfKind::PartialWrite, "b", "o", None));
        set_mrf_delivery_enabled(true);

        // Fill the bounded channel past capacity: excess intents are dropped,
        // never blocking.
        let mut accepted = 0;
        for index in 0..(MRF_CHANNEL_CAPACITY + 64) {
            if try_send_mrf_intent(MrfKind::PartialWrite, "b", &format!("o-{index}"), None) {
                accepted += 1;
            }
        }
        assert_eq!(accepted, MRF_CHANNEL_CAPACITY);
    }

    #[test]
    fn try_send_without_channel_is_false() {
        // This test may run after the tokio test above in the same process;
        // the singleton semantics make a clean "uninitialized" case hard, so
        // assert the flag-off behavior only.
        set_mrf_delivery_enabled(false);
        assert!(!try_send_mrf_intent(MrfKind::MetadataCorruption, "b", "o", None));
        set_mrf_delivery_enabled(true);
    }

    #[test]
    fn repaired_events_take_is_bucket_scoped_and_cap_bounded() {
        // Distinct buckets keep their notices until their own scanner takes
        // them; a take for one bucket leaves the others' notices in place.
        note_mrf_repaired("bucket-a", "object-1", None);
        note_mrf_repaired("bucket-b", "object-2", None);
        note_mrf_repaired("bucket-a", "object-3", None);

        let taken_a = take_mrf_repaired_events_for("bucket-a");
        assert_eq!(taken_a.len(), 2);
        assert_eq!(taken_a[0].object.as_ref(), "object-1");
        assert_eq!(taken_a[1].object.as_ref(), "object-3");
        assert!(take_mrf_repaired_events_for("bucket-a").is_empty(), "take is destructive per bucket");

        let taken_b = take_mrf_repaired_events_for("bucket-b");
        assert_eq!(taken_b.len(), 1);
        assert_eq!(taken_b[0].object.as_ref(), "object-2");

        // Cap bound: flooding the ring drops the oldest notices rather than
        // growing unbounded.
        for i in 0..=(MRF_REPAIRED_EVENT_CAP + 8) {
            note_mrf_repaired("flood-bucket", &format!("object-{i}"), None);
        }
        let flooded = take_mrf_repaired_events_for("flood-bucket");
        assert_eq!(flooded.len(), MRF_REPAIRED_EVENT_CAP);
        assert_eq!(flooded[0].object.as_ref(), "object-9", "the oldest notices past the cap are dropped");
    }
}
