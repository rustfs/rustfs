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

fn isolated(name: &str) -> bool {
    const CHILD: &str = "RUSTFS_MRF_EVENT_PRESENCE_TEST";
    let test = format!("mrf_channel::event_presence_tests::{name}");
    if let Ok(current) = std::env::var(CHILD) {
        assert_eq!(current, test, "only the selected registry test may run in the child");
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([&test, "--exact", "--test-threads=1"])
        .env(CHILD, &test)
        .output()
        .expect("run isolated global registry test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "registry child must execute exactly once: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[test]
fn event_presence_covers_both_queues_and_contention_without_consuming() {
    if isolated("event_presence_covers_both_queues_and_contention_without_consuming") {
        return;
    }
    assert!(!mrf_durable_repair_events_may_be_pending());
    let anchor = MrfDurableRepairAnchor {
        kind: MrfKind::PartialWrite,
        bucket: Arc::from("presence-test"),
        object: Arc::from("object"),
        version_id: Some([7; 16]),
        scope: Some(MrfScope {
            pool_index: 0,
            set_index: 0,
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
        delete_marker_purge: None,
        lease: Some(anchor.lease),
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        disposition: MrfVerifiedRepairDisposition::VerifiedHealthy,
    };
    note_mrf_verified_repair(event.clone());
    assert!(mrf_durable_repair_events_may_be_pending());
    assert!(mrf_durable_repair_events_may_be_pending(), "the hint must not consume a proof");
    assert_eq!(take_mrf_verified_repair_events_for(&anchor.bucket), vec![event]);
    assert!(
        mrf_durable_repair_events_may_be_pending(),
        "the empty allocated verified buffer still needs its original maintenance pass"
    );
    let mut retained = vec![anchor.clone()];
    assert_eq!(consume_recorded_verified_mrf_repair_events_for(&anchor.bucket, &mut retained), 0);
    assert_eq!(retained, vec![anchor.clone()], "buffer maintenance cannot discharge an anchor");
    assert_eq!(
        MRF_VERIFIED_REPAIR_EVENTS
            .get()
            .expect("verified registry")
            .lock()
            .expect("verified storage")
            .capacity(),
        0,
        "the actual consumer releases empty proof storage before subsequent idle passes can skip it"
    );
    assert!(!mrf_durable_repair_events_may_be_pending());
    let legacy = MrfUnverifiedLegacyEvent { anchor };
    note_mrf_unverified_legacy(legacy.clone());
    assert!(mrf_durable_repair_events_may_be_pending(), "legacy-only work must not be skipped");
    assert_eq!(take_mrf_unverified_legacy_events_for(&legacy.anchor.bucket), vec![legacy]);
    assert!(!mrf_durable_repair_events_may_be_pending());
    let verified = MRF_VERIFIED_REPAIR_EVENTS.get().expect("verified registry initialized");
    let guard = verified.lock().expect("hold verified registry");
    assert!(mrf_durable_repair_events_may_be_pending(), "contended verified is unknown");
    drop(guard);
    let legacy = MRF_UNVERIFIED_LEGACY_EVENTS.get().expect("legacy registry initialized");
    let guard = legacy.lock().expect("hold legacy registry");
    assert!(mrf_durable_repair_events_may_be_pending(), "contended legacy is unknown");
    drop(guard);
    assert!(!mrf_durable_repair_events_may_be_pending());
}

#[test]
fn event_presence_keeps_poisoned_verified_unknown() {
    if isolated("event_presence_keeps_poisoned_verified_unknown") {
        return;
    }
    assert!(!mrf_durable_repair_events_may_be_pending());
    let registry = MRF_VERIFIED_REPAIR_EVENTS.get_or_init(|| Mutex::new(Default::default()));
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = registry.lock().expect("unpoisoned registry");
            panic!("poison the isolated verified registry");
        })
        .is_err()
    );
    assert!(mrf_durable_repair_events_may_be_pending());
    assert!(registry.is_poisoned(), "the hint must not clear or recover poisoned state");
}

#[test]
fn event_presence_keeps_poisoned_legacy_unknown() {
    if isolated("event_presence_keeps_poisoned_legacy_unknown") {
        return;
    }
    assert!(!mrf_durable_repair_events_may_be_pending());
    let registry = MRF_UNVERIFIED_LEGACY_EVENTS.get_or_init(|| Mutex::new(Default::default()));
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = registry.lock().expect("unpoisoned registry");
            panic!("poison the isolated legacy registry");
        })
        .is_err()
    );
    assert!(mrf_durable_repair_events_may_be_pending());
    assert!(registry.is_poisoned(), "the hint must not clear or recover poisoned state");
}
