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
            tier: options.tier,
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

#[test]
fn manual_transition_record_marks_unknown_when_cursor_would_skip_pending_page() {
    let options = ManualTransitionRunOptions {
        prefix: "logs/".to_string(),
        tier: Some("WARM".to_string()),
        ..Default::default()
    };
    let job_id = Uuid::new_v4();
    let mut record = ManualTransitionJobRecord::new(job_id, "manual-pending-page-bucket", &options, "owner-a");
    record.report = ManualTransitionRunReport {
        bucket: "manual-pending-page-bucket".to_string(),
        prefix: options.prefix.clone(),
        tier: options.tier,
        scanned: 1000,
        eligible: 2,
        enqueued: 2,
        transition_completed: 1,
        continuation_token: Some("opaque-page-cursor".to_string()),
        ..Default::default()
    };

    let marked = record.mark_unknown_if_recovery_would_skip_pending_page(ManualTransitionQueueSnapshot::default());

    assert!(marked);
    assert_eq!(record.state, ManualTransitionJobState::Unknown);
    assert!(record.is_terminal());
    assert_eq!(record.queue_snapshot, ManualTransitionQueueSnapshot::default());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("page/task journal is missing"))
    );

    let encoded = record.encode().expect("pending-page unknown state should encode");
    let decoded = ManualTransitionJobRecord::decode(job_id, &encoded).expect("pending-page unknown state should decode");
    assert_eq!(decoded.state, ManualTransitionJobState::Unknown);
    assert_eq!(decoded.report.continuation_token.as_deref(), Some("opaque-page-cursor"));
    assert_eq!(decoded.report.enqueued, 2);
    assert_eq!(decoded.report.transition_completed, 1);
    assert!(decoded.completed_at_unix_nanos.is_some());
}

#[test]
fn manual_transition_record_keeps_running_when_pending_task_journal_remains() {
    let options = ManualTransitionRunOptions {
        prefix: "logs/".to_string(),
        tier: Some("WARM".to_string()),
        ..Default::default()
    };
    let job_id = Uuid::new_v4();
    let mut record = ManualTransitionJobRecord::new(job_id, "manual-pending-task-bucket", &options, "owner-a");
    record.report = ManualTransitionRunReport {
        bucket: "manual-pending-task-bucket".to_string(),
        prefix: options.prefix.clone(),
        tier: options.tier,
        scanned: 1000,
        eligible: 2,
        enqueued: 2,
        transition_completed: 1,
        continuation_token: Some("opaque-page-cursor".to_string()),
        ..Default::default()
    };
    let queue_snapshot = ManualTransitionQueueSnapshot {
        queued: 1,
        active: 1,
        workers: 2,
        ..Default::default()
    };

    let marked = record.mark_unknown_if_recovery_would_skip_pending_page(queue_snapshot);

    assert!(!marked);
    assert_eq!(record.state, ManualTransitionJobState::Running);
    assert!(!record.is_terminal());
    assert!(record.completed_at_unix_nanos.is_none());
    assert!(record.error.is_none());
}

#[test]
fn manual_transition_record_marks_unknown_when_worker_result_is_lost_after_drain() {
    let options = ManualTransitionRunOptions {
        prefix: "logs/".to_string(),
        tier: Some("WARM".to_string()),
        ..Default::default()
    };
    let job_id = Uuid::new_v4();
    let mut record = ManualTransitionJobRecord::new(job_id, "manual-lost-worker-result-bucket", &options, "owner-a");
    record.complete(
        ManualTransitionRunReport {
            bucket: "manual-lost-worker-result-bucket".to_string(),
            prefix: options.prefix.clone(),
            tier: options.tier,
            scanned: 2,
            eligible: 2,
            enqueued: 2,
            ..Default::default()
        },
        ManualTransitionQueueSnapshot {
            queued: 1,
            workers: 1,
            ..Default::default()
        },
    );
    assert_eq!(record.state, ManualTransitionJobState::Running);
    assert!(record.scan_completed);

    let marked = record.mark_unknown_if_worker_results_lost(ManualTransitionQueueSnapshot::default());

    assert!(marked);
    assert_eq!(record.state, ManualTransitionJobState::Unknown);
    assert!(record.is_terminal());
    assert_eq!(record.report.enqueued, 2);
    assert_eq!(record.report.transition_completed, 0);
    assert_eq!(record.queue_snapshot, ManualTransitionQueueSnapshot::default());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("worker result was not persisted"))
    );

    let encoded = record.encode().expect("lost-worker-result unknown state should encode");
    let decoded = ManualTransitionJobRecord::decode(job_id, &encoded).expect("lost-worker-result unknown state should decode");
    assert_eq!(decoded.state, ManualTransitionJobState::Unknown);
    assert_eq!(decoded.report.enqueued, 2);
    assert_eq!(decoded.report.transition_completed, 0);
    assert!(decoded.completed_at_unix_nanos.is_some());
}
