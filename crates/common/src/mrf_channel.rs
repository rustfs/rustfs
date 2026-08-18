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

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Bounded capacity of the global MRF channel. Backpressure is resolved by
/// dropping (and counting) intents, never by blocking the producer.
const MRF_CHANNEL_CAPACITY: usize = 8192;

/// Why an intent was produced. Drives the heal priority mapping on the
/// consumer side (DecodeFailure -> Urgent, MetadataCorruption -> High,
/// PartialWrite -> Normal).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub enqueued_at_ms: u64,
    /// Times this intent has already been offered to the heal manager.
    /// Dropped by the consumer once it reaches `MRF_MAX_ATTEMPTS`.
    pub attempts: u8,
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
    if !mrf_delivery_enabled() {
        return false;
    }
    let Some(sender) = GLOBAL_MRF_SENDER.get() else {
        return false;
    };
    let intent = MrfIntent {
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version_id: version_id.map(|vid| *vid.as_bytes()),
        kind,
        enqueued_at_ms: unix_now_ms(),
        attempts: 0,
    };
    sender.try_send(intent).is_ok()
}

fn unix_now_ms() -> u64 {
    // Kept trivial: the timestamp is diagnostic metadata only; wall-clock
    // failure would be a bug rather than something to handle here.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
            enqueued_at_ms: 0,
            attempts: 0,
        };
        assert!(intent.estimated_bytes() >= intent.bucket.len() + intent.object.len());
    }

    #[tokio::test]
    async fn try_send_delivers_and_respects_capacity() {
        let mut receiver = init_mrf_channel().expect("first initialization should succeed");
        assert!(init_mrf_channel().is_err(), "double initialization must fail");

        assert!(try_send_mrf_intent(MrfKind::DecodeFailure, "b", "o", Some(Uuid::nil())));
        let intent = receiver.recv().await.expect("intent should arrive");
        assert_eq!(intent.kind, MrfKind::DecodeFailure);
        assert_eq!(intent.bucket.as_ref(), "b");

        // Disable delivery: producers become no-ops.
        set_mrf_delivery_enabled(false);
        assert!(!try_send_mrf_intent(MrfKind::PartialWrite, "b", "o", None));
        set_mrf_delivery_enabled(true);

        // Fill the bounded channel past capacity: excess intents are dropped,
        // never blocking.
        let mut accepted = 0;
        for _ in 0..(MRF_CHANNEL_CAPACITY + 64) {
            if try_send_mrf_intent(MrfKind::PartialWrite, "b", "o", None) {
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
}
