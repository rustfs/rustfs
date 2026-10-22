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

// Compatibility reference from a47649fb60c0492cc0fb02bff28439c5e357f8e4.
// Keep this independent of the shared resolver and its canonical helpers.
fn original_anchor(intent: &MrfIntent, bucket_incarnation_id: Uuid) -> Option<MrfDurableRepairAnchor> {
    let lease = intent.lease?;
    let version_id = intent.version_id.filter(|bytes| *bytes != [0; 16]);
    let (version_id, scope) = match intent.kind {
        MrfKind::MetadataCorruption => (None, None),
        MrfKind::DecodeFailure | MrfKind::PartialWrite | MrfKind::DeleteMarkerPurge => (version_id, intent.scope),
    };
    let delete_marker_purge = match intent.kind {
        MrfKind::DeleteMarkerPurge => intent
            .delete_marker_purge
            .as_ref()
            .filter(|payload| {
                !payload.bucket_incarnation_id.is_nil()
                    && payload.marker_incarnation_id == payload.bucket_incarnation_id
                    && !payload.marker.is_empty()
                    && payload.marker.len() <= MRF_MAX_DELETE_MARKER_BYTES
            })
            .map(|payload| {
                Some(MrfDeleteMarkerPurgeIdentity {
                    bucket_incarnation_id: payload.bucket_incarnation_id,
                    marker_incarnation_id: payload.marker_incarnation_id,
                    marker_identity: payload.marker_identity,
                })
            })?,
        _ if intent.delete_marker_purge.is_none() => None,
        _ => return None,
    };
    let bucket_incarnation_id = delete_marker_purge
        .map(|identity| identity.bucket_incarnation_id)
        .unwrap_or(bucket_incarnation_id);
    if bucket_incarnation_id.is_nil() {
        return None;
    }
    Some(MrfDurableRepairAnchor {
        kind: intent.kind,
        bucket: intent.bucket.clone(),
        object: intent.object.clone(),
        version_id,
        scope,
        delete_marker_purge,
        lease,
        bucket_incarnation_id,
    })
}

#[test]
fn shared_repair_identity_matches_the_original_constructor_and_source_filter() {
    let source = Uuid::from_u128(1);
    let foreign = Uuid::from_u128(2);
    let purge = MrfDeleteMarkerPurge {
        bucket_incarnation_id: source,
        marker_incarnation_id: source,
        marker_identity: [9; 32],
        marker: Arc::from([7_u8]),
    };
    let payloads = [
        None,
        Some(purge.clone()),
        Some(MrfDeleteMarkerPurge {
            bucket_incarnation_id: foreign,
            marker_incarnation_id: foreign,
            ..purge.clone()
        }),
        Some(MrfDeleteMarkerPurge {
            marker: Arc::from([]),
            ..purge.clone()
        }),
        Some(MrfDeleteMarkerPurge {
            marker: Arc::from(vec![7; MRF_MAX_DELETE_MARKER_BYTES]),
            ..purge.clone()
        }),
        Some(MrfDeleteMarkerPurge {
            marker: Arc::from(vec![7; MRF_MAX_DELETE_MARKER_BYTES + 1]),
            ..purge.clone()
        }),
        Some(MrfDeleteMarkerPurge {
            marker_incarnation_id: foreign,
            ..purge.clone()
        }),
        Some(MrfDeleteMarkerPurge {
            bucket_incarnation_id: Uuid::nil(),
            marker_incarnation_id: Uuid::nil(),
            ..purge
        }),
    ];
    let mut cases = 0;
    for kind in [
        MrfKind::DecodeFailure,
        MrfKind::MetadataCorruption,
        MrfKind::PartialWrite,
        MrfKind::DeleteMarkerPurge,
    ] {
        for lease in [None, Some(MrfIngressLease::new(0)), Some(MrfIngressLease::new(7))] {
            for version_id in [None, Some([0; 16]), Some([7; 16])] {
                for scope in [
                    None,
                    Some(MrfScope {
                        pool_index: u32::MAX,
                        set_index: 3,
                    }),
                ] {
                    for input_source in [Uuid::nil(), source, foreign] {
                        for (payload_index, payload) in payloads.iter().enumerate() {
                            let intent = MrfIntent {
                                bucket: Arc::from("source-shape"),
                                object: Arc::from("object"),
                                version_id,
                                kind,
                                delete_marker_purge: payload.clone(),
                                scope,
                                lease,
                                enqueued_at_ms: 0,
                                attempts: 0,
                            };
                            let expected = original_anchor(&intent, input_source);
                            assert_eq!(
                                MrfDurableRepairAnchor::from_intent(&intent, input_source),
                                expected,
                                "kind={kind:?}, lease={lease:?}, version={version_id:?}, scope={scope:?}, source={input_source}, payload={payload_index}"
                            );
                            assert_eq!(
                                MrfDurableRepairAnchor::is_eligible_for_source(&intent, input_source),
                                expected
                                    .as_ref()
                                    .is_some_and(|anchor| anchor.bucket_incarnation_id == input_source),
                                "source eligibility must preserve the exact source: kind={kind:?}, lease={lease:?}, version={version_id:?}, scope={scope:?}, source={input_source}, payload={payload_index}"
                            );
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 1728, "exercise the complete explicit identity-shape matrix");
}

#[test]
fn repair_identity_preserves_literal_purge_source_lease_and_metadata_boundaries() {
    let source = Uuid::from_u128(1);
    let purge_source = Uuid::from_u128(2);
    let mut intent = MrfIntent {
        bucket: Arc::from("literal-bucket"),
        object: Arc::from("marker"),
        version_id: None,
        kind: MrfKind::DeleteMarkerPurge,
        delete_marker_purge: Some(MrfDeleteMarkerPurge {
            bucket_incarnation_id: purge_source,
            marker_incarnation_id: purge_source,
            marker_identity: [9; 32],
            marker: Arc::from([7_u8]),
        }),
        scope: None,
        lease: Some(MrfIngressLease::new(17)),
        enqueued_at_ms: 0,
        attempts: 0,
    };
    assert!(
        !valid_intent_payload(&intent),
        "constructor compatibility is broader than durable admission"
    );
    assert_eq!(
        MrfDurableRepairAnchor::from_intent(&intent, Uuid::nil()),
        Some(MrfDurableRepairAnchor {
            kind: MrfKind::DeleteMarkerPurge,
            bucket: Arc::from("literal-bucket"),
            object: Arc::from("marker"),
            version_id: None,
            scope: None,
            delete_marker_purge: Some(MrfDeleteMarkerPurgeIdentity {
                bucket_incarnation_id: purge_source,
                marker_incarnation_id: purge_source,
                marker_identity: [9; 32],
            }),
            lease: MrfIngressLease::new(17),
            bucket_incarnation_id: purge_source,
        })
    );
    assert!(
        !MrfDurableRepairAnchor::is_eligible_for_source(&intent, source),
        "a purge cannot borrow a conflicting entry source"
    );
    assert!(!MrfDurableRepairAnchor::is_eligible_for_source(&intent, Uuid::nil()));
    assert!(MrfDurableRepairAnchor::is_eligible_for_source(&intent, purge_source));
    intent.lease = None;
    assert!(MrfDurableRepairAnchor::from_intent(&intent, purge_source).is_none());
    assert!(!MrfDurableRepairAnchor::is_eligible_for_source(&intent, purge_source));
    intent.lease = Some(MrfIngressLease::new(17));
    intent
        .delete_marker_purge
        .as_mut()
        .expect("purge fixture")
        .marker_incarnation_id = Uuid::nil();
    assert!(MrfDurableRepairAnchor::from_intent(&intent, purge_source).is_none());
    intent
        .delete_marker_purge
        .as_mut()
        .expect("purge fixture")
        .bucket_incarnation_id = Uuid::nil();
    assert!(
        !MrfDurableRepairAnchor::is_eligible_for_source(&intent, source),
        "nil marker identity cannot borrow a non-nil source"
    );
    intent.kind = MrfKind::PartialWrite;
    assert!(
        MrfDurableRepairAnchor::from_intent(&intent, source).is_none(),
        "ordinary intents cannot carry a purge payload"
    );
    intent.delete_marker_purge = None;
    intent.kind = MrfKind::MetadataCorruption;
    intent.version_id = Some([7; 16]);
    intent.scope = Some(MrfScope {
        pool_index: 8,
        set_index: 9,
    });
    assert_eq!(
        MrfDurableRepairAnchor::from_intent(&intent, source),
        Some(MrfDurableRepairAnchor {
            kind: MrfKind::MetadataCorruption,
            bucket: Arc::from("literal-bucket"),
            object: Arc::from("marker"),
            version_id: None,
            scope: None,
            delete_marker_purge: None,
            lease: MrfIngressLease::new(17),
            bucket_incarnation_id: source,
        })
    );
    assert!(MrfDurableRepairAnchor::is_eligible_for_source(&intent, source));
    assert!(MrfDurableRepairAnchor::from_intent(&intent, Uuid::nil()).is_none());
    intent.lease = Some(MrfIngressLease::new(0));
    assert_eq!(
        MrfDurableRepairAnchor::from_intent(&intent, source)
            .expect("preserve the existing opaque lease shape")
            .lease,
        MrfIngressLease::new(0)
    );
}
