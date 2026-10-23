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
use crate::heal::failure_logging::test_support::FailureLogCapture;

#[tokio::test(flavor = "current_thread")]
async fn legacy_ambiguity_scheduler_does_not_repeat_leaf_errors_for_new_task_ids() {
    let capture = FailureLogCapture::default();
    // The single-thread runtime also polls the scheduler's spawned tasks
    // under this subscriber, including their terminal error emission.
    let _subscriber = tracing::subscriber::set_default(capture.clone());
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let bucket = format!("legacy-ambiguity-scheduler-{}", Uuid::new_v4());
    let mut task_ids = HashSet::new();
    for attempt in 1..=3 {
        let mut request = HealRequest::object(bucket.clone(), "object".to_owned(), None);
        request.source = HealRequestSource::Scanner;
        // High priority already bypasses disposition admission backoff. The
        // test exercises repeated executions without changing that policy.
        request.priority = HealPriority::High;
        let task_id = request.id.clone();
        assert!(task_ids.insert(task_id.clone()));
        assert_eq!(
            manager.submit_heal_request(request).await.expect("admit explicit attempt"),
            HealAdmissionResult::Accepted
        );
        process_manager_queue_once(&manager).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while manager.get_statistics().await.failed_tasks < attempt {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduler must publish a terminal failure for every attempt");
        let report = manager.get_task_report(&task_id).await.expect("retained scheduler report");
        assert!(matches!(report.status, HealTaskStatus::Failed { .. }));
        assert_eq!(report.outcome.expect("retained outcome").counters.failed, 1);
        assert!(
            manager.retrying_heals.lock().await.is_empty(),
            "permanent ambiguity cannot enter recoverable retry state"
        );
    }
    assert_eq!(capture.count(tracing::Level::ERROR, Some("rustfs::heal::task")), 1);
    assert_eq!(
        capture.count(tracing::Level::ERROR, Some("rustfs::heal::manager")),
        0,
        "the actual scheduler must not repeat the leaf's ambiguity failure"
    );
    assert_eq!(manager.get_statistics().await.failed_tasks, 3);
    assert!(manager.active_heals.lock().await.is_empty());
}
