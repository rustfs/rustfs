// Copyright 2026 RustFS Team
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

use super::*;
use std::collections::VecDeque;

fn proof_fixture() -> (MrfDurableRepairAnchor, MrfVerifiedRepairEvent) {
    let anchor = MrfDurableRepairAnchor {
        kind: MrfKind::PartialWrite,
        bucket: Arc::from("proof-queue"),
        object: Arc::from("matched"),
        version_id: Some([7; 16]),
        scope: Some(MrfScope {
            pool_index: 1,
            set_index: 2,
        }),
        delete_marker_purge: None,
        lease: MrfIngressLease::new(21),
        bucket_incarnation_id: Uuid::new_v4(),
    };
    let event = MrfVerifiedRepairEvent {
        kind: anchor.kind,
        bucket: anchor.bucket.clone(),
        object: anchor.object.clone(),
        version_id: anchor.version_id,
        scope: anchor.scope,
        delete_marker_purge: anchor.delete_marker_purge,
        lease: Some(anchor.lease),
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        disposition: MrfVerifiedRepairDisposition::Repaired,
    };
    (anchor, event)
}

#[test]
fn proof_queue_reuses_unmatched_storage_in_contiguous_and_wrapped_layouts() {
    for wrapped in [false, true] {
        for foreign in [false, true] {
            let (anchor, mut event) = proof_fixture();
            if foreign {
                event.bucket = Arc::from("another-bucket");
            } else {
                event.lease = None;
            }
            let mut events = VecDeque::with_capacity(8);
            events.extend(std::iter::repeat_n(event, events.capacity()));
            if wrapped {
                let first = events.pop_front().expect("full local proof queue");
                events.push_back(first);
                assert!(!events.as_slices().1.is_empty(), "exercise a wrapped producer queue");
            }
            let capacity = events.capacity();
            let storage = events
                .iter()
                .map(std::ptr::from_ref)
                .min()
                .expect("nonempty local proof storage");
            let expected = events.clone();
            let mut anchors = vec![anchor.clone()];
            for _ in 0..4 {
                assert_eq!(
                    consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
                    0
                );
                assert_eq!(events, expected);
                assert_eq!(anchors, vec![anchor.clone()]);
                assert_eq!(events.capacity(), capacity);
                assert_eq!(
                    events.iter().map(std::ptr::from_ref).min(),
                    Some(storage),
                    "a full unmatched queue must reuse its buffer instead of rebuilding it for every bucket"
                );
            }
        }
    }
}

#[test]
fn proof_queue_consumes_all_exact_anchor_copies_and_preserves_duplicate_proof_order() {
    let (anchor, event) = proof_fixture();
    let waiting = MrfDurableRepairAnchor {
        object: Arc::from("waiting"),
        ..anchor.clone()
    };
    let foreign = MrfVerifiedRepairEvent {
        bucket: Arc::from("foreign"),
        ..event.clone()
    };
    let unleased = MrfVerifiedRepairEvent {
        lease: None,
        ..event.clone()
    };
    let future = MrfVerifiedRepairEvent {
        object: Arc::from("future"),
        ..event.clone()
    };
    let mut events = VecDeque::from(vec![
        foreign.clone(),
        event.clone(),
        unleased.clone(),
        event.clone(),
        future.clone(),
    ]);
    let first = events.pop_front().expect("foreign proof starts the queue");
    events.push_back(first);
    assert!(!events.as_slices().1.is_empty(), "match and retain proofs across the wrap boundary");
    let mut anchors = vec![anchor.clone(), waiting.clone(), anchor.clone()];
    assert_eq!(
        consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
        2
    );
    assert_eq!(anchors, vec![waiting]);
    assert_eq!(
        events,
        VecDeque::from(vec![unleased, event, future, foreign]),
        "the later duplicate proof remains available after the first proof consumes every exact anchor"
    );
    let retained = events.clone();
    assert_eq!(
        consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
        0
    );
    assert_eq!(events, retained, "a later consumer must still be able to match the retained proofs");
}

#[test]
fn proof_queue_keeps_every_incomplete_or_mismatched_identity() {
    let (anchor, event) = proof_fixture();
    let different_purge = MrfDeleteMarkerPurgeIdentity {
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        marker_incarnation_id: anchor.bucket_incarnation_id,
        marker_identity: [3; 32],
    };
    for rejected in [
        MrfVerifiedRepairEvent {
            kind: MrfKind::DecodeFailure,
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            bucket: Arc::from("foreign"),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            object: Arc::from("other"),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            version_id: None,
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            version_id: Some([8; 16]),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            scope: None,
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            scope: Some(MrfScope {
                pool_index: 2,
                set_index: 2,
            }),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            scope: Some(MrfScope {
                pool_index: 1,
                set_index: 3,
            }),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            lease: None,
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            lease: Some(MrfIngressLease::new(22)),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            bucket_incarnation_id: Uuid::new_v4(),
            ..event.clone()
        },
        MrfVerifiedRepairEvent {
            delete_marker_purge: Some(different_purge),
            ..event
        },
    ] {
        let mut anchors = vec![anchor.clone()];
        let mut events = VecDeque::from([rejected.clone()]);
        assert_eq!(
            consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
            0
        );
        assert_eq!(anchors, vec![anchor.clone()]);
        assert_eq!(events, VecDeque::from([rejected]));
    }
}

#[test]
fn proof_queue_requires_the_complete_delete_marker_purge_identity() {
    let (mut anchor, mut event) = proof_fixture();
    let purge = MrfDeleteMarkerPurgeIdentity {
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        marker_incarnation_id: anchor.bucket_incarnation_id,
        marker_identity: [3; 32],
    };
    anchor.kind = MrfKind::DeleteMarkerPurge;
    anchor.delete_marker_purge = Some(purge);
    event.kind = anchor.kind;
    event.delete_marker_purge = anchor.delete_marker_purge;
    for rejected in [
        None,
        Some(MrfDeleteMarkerPurgeIdentity {
            bucket_incarnation_id: Uuid::new_v4(),
            ..purge
        }),
        Some(MrfDeleteMarkerPurgeIdentity {
            marker_incarnation_id: Uuid::new_v4(),
            ..purge
        }),
        Some(MrfDeleteMarkerPurgeIdentity {
            marker_identity: [4; 32],
            ..purge
        }),
    ] {
        let rejected = MrfVerifiedRepairEvent {
            delete_marker_purge: rejected,
            ..event.clone()
        };
        let mut anchors = vec![anchor.clone()];
        let mut events = VecDeque::from([rejected.clone(), event.clone()]);
        assert_eq!(
            consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
            1
        );
        assert!(anchors.is_empty());
        assert_eq!(events, VecDeque::from([rejected]));
    }
}

#[test]
fn proof_queue_capacity_follows_the_preconsumption_length_and_releases_on_the_next_empty_call() {
    let (anchor, event) = proof_fixture();
    let future_events = (0..3)
        .map(|index| MrfVerifiedRepairEvent {
            object: Arc::from(format!("future-{index}")),
            ..event.clone()
        })
        .collect::<Vec<_>>();
    let mut events = VecDeque::with_capacity(64);
    events.push_back(event);
    events.extend(future_events.iter().cloned());
    let previous_event_count = events.len();
    let mut anchors = vec![anchor.clone()];
    assert_eq!(
        consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
        1
    );
    assert_eq!(events.iter().cloned().collect::<Vec<_>>(), future_events);
    assert_eq!(
        events.capacity(),
        VecDeque::<MrfVerifiedRepairEvent>::with_capacity(previous_event_count).capacity(),
        "preserve the former queue rebuild's capacity lifetime instead of retaining the producer high water"
    );
    let previous_event_count = events.len();
    let mut anchors = future_events
        .iter()
        .map(|event| MrfDurableRepairAnchor {
            object: event.object.clone(),
            ..anchor.clone()
        })
        .collect();
    assert_eq!(
        consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
        3
    );
    assert!(events.is_empty());
    assert_eq!(
        events.capacity(),
        VecDeque::<MrfVerifiedRepairEvent>::with_capacity(previous_event_count).capacity()
    );
    assert_eq!(
        consume_verified_mrf_repair_events_from_queue(&anchor.bucket, &mut anchors, &mut events),
        0
    );
    assert_eq!(
        events.capacity(),
        0,
        "the next empty consumption releases backing storage just as a rebuild did"
    );
}
