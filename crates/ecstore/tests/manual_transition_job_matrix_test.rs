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

use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport,
};
use rustfs_ecstore::api::bucket::lifecycle::manual_transition_job::{ManualTransitionJobRecord, ManualTransitionJobState};
use uuid::Uuid;

#[test]
fn manual_transition_record_persists_combined_tier_and_queue_failures_as_partial() {
    let options = ManualTransitionRunOptions {
        prefix: "logs/".to_string(),
        tier: Some("WARM".to_string()),
        ..Default::default()
    };
    let job_id = Uuid::new_v4();
    let mut record = ManualTransitionJobRecord::new(job_id, "manual-combined-failures-bucket", &options, "owner-a");
    let queue_snapshot = ManualTransitionQueueSnapshot {
        queue_capacity: 2,
        workers: 2,
        queue_full: 1,
        queue_send_timeout: 1,
        ..Default::default()
    };

    record.complete(
        ManualTransitionRunReport {
            bucket: "manual-combined-failures-bucket".to_string(),
            prefix: options.prefix.clone(),
            tier: options.tier.clone(),
            scanned: 4,
            eligible: 3,
            skipped_queue_full: 1,
            skipped_queue_closed: 1,
            skipped_queue_timeout: 1,
            tier_failure: 1,
            ..Default::default()
        },
        queue_snapshot,
    );

    assert_eq!(record.state, ManualTransitionJobState::Partial);
    assert!(record.report.has_partial_enqueue());
    assert_eq!(record.report.tier_failure, 1);
    assert_eq!(record.report.skipped_queue_full, 1);
    assert_eq!(record.report.skipped_queue_closed, 1);
    assert_eq!(record.report.skipped_queue_timeout, 1);
    assert_eq!(record.queue_snapshot.queue_full, 1);
    assert_eq!(record.queue_snapshot.queue_send_timeout, 1);
    assert!(record.completed_at_unix_nanos.is_some());
    assert!(record.error.is_none());

    let encoded = record.encode().expect("combined terminal report should encode");
    let decoded = ManualTransitionJobRecord::decode(job_id, &encoded).expect("combined terminal report should decode");

    assert_eq!(decoded.state, ManualTransitionJobState::Partial);
    assert!(decoded.report.has_partial_enqueue());
    assert_eq!(decoded.report.tier_failure, 1);
    assert_eq!(decoded.report.skipped_queue_full, 1);
    assert_eq!(decoded.report.skipped_queue_closed, 1);
    assert_eq!(decoded.report.skipped_queue_timeout, 1);
    assert_eq!(decoded.queue_snapshot.queue_full, 1);
    assert_eq!(decoded.queue_snapshot.queue_send_timeout, 1);
    assert!(decoded.completed_at_unix_nanos.is_some());
    assert!(decoded.error.is_none());
}
