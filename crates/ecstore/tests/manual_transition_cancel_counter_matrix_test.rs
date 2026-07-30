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

mod storage_api;

use storage_api::manual_transition::{
    ManualTransitionJobRecord, ManualTransitionJobState, ManualTransitionQueueSnapshot, ManualTransitionRunOptions,
    ManualTransitionRunReport,
};
use uuid::Uuid;

#[test]
fn manual_transition_cancelled_record_preserves_worker_terminal_counters() {
    let options = ManualTransitionRunOptions {
        prefix: "logs/".to_string(),
        tier: Some("WARM".to_string()),
        ..Default::default()
    };
    let job_id = Uuid::new_v4();
    let mut record = ManualTransitionJobRecord::new(job_id, "manual-cancel-counter-bucket", &options, "owner-a");

    record.report.enqueued = 2;
    record.report.transition_completed = 1;
    record.report.transition_failed = 1;
    record.report.tier_failure = 1;
    record.mark_cancel_requested();
    record.complete(
        ManualTransitionRunReport {
            bucket: "manual-cancel-counter-bucket".to_string(),
            prefix: "logs/".to_string(),
            tier: Some("WARM".to_string()),
            scanned: 2,
            eligible: 2,
            enqueued: 2,
            ..Default::default()
        },
        ManualTransitionQueueSnapshot {
            queue_capacity: 8,
            workers: 2,
            ..Default::default()
        },
    );

    assert_eq!(record.state, ManualTransitionJobState::Cancelled);
    assert!(record.cancel_requested);
    assert!(record.report.cancelled);
    assert_eq!(record.report.transition_completed, 1);
    assert_eq!(record.report.transition_failed, 1);
    assert_eq!(record.report.tier_failure, 1);
    assert_eq!(record.queue_snapshot.queue_capacity, 8);
    assert_eq!(record.queue_snapshot.workers, 2);
    assert!(record.completed_at_unix_nanos.is_some());

    let encoded = record.encode().expect("cancelled terminal report should encode");
    let decoded = ManualTransitionJobRecord::decode(job_id, &encoded).expect("cancelled terminal report should decode");

    assert_eq!(decoded.state, ManualTransitionJobState::Cancelled);
    assert!(decoded.cancel_requested);
    assert!(decoded.report.cancelled);
    assert_eq!(decoded.report.transition_completed, 1);
    assert_eq!(decoded.report.transition_failed, 1);
    assert_eq!(decoded.report.tier_failure, 1);
    assert_eq!(decoded.queue_snapshot.queue_capacity, 8);
    assert_eq!(decoded.queue_snapshot.workers, 2);
    assert!(decoded.completed_at_unix_nanos.is_some());
}
