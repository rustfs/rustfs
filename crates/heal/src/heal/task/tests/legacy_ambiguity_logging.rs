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
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{Subscriber, instrument::WithSubscriber};

#[derive(Clone, Default)]
struct ErrorCapture(Arc<AtomicUsize>);

impl Subscriber for ErrorCapture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::ERROR {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn legacy_ambiguity_execution_logs_once_but_each_new_task_remains_failed() {
    for kind in ["object", "metadata", "ec_decode"] {
        for outer in [false, true] {
            let object = format!("legacy-ambiguity-{kind}-{outer}-{}", Uuid::new_v4());
            let capture = ErrorCapture::default();
            let storage = Arc::new(MockStorage::default());
            let mut scheduler_would_repeat = false;
            for _ in 0..3 {
                *storage.heal_object_outcome.lock().expect("outcome fixture") =
                    Some(MockHealObjectOutcome::LegacyAmbiguous { outer });
                let heal_type = match kind {
                    "metadata" => HealType::Metadata {
                        bucket: "bucket".to_owned(),
                        object: object.clone(),
                    },
                    "ec_decode" => HealType::ECDecode {
                        bucket: "bucket".to_owned(),
                        object: object.clone(),
                        version_id: None,
                    },
                    _ => HealType::Object {
                        bucket: "bucket".to_owned(),
                        object: object.clone(),
                        version_id: None,
                    },
                };
                let task = HealTask::from_request(
                    HealRequest::new(heal_type, HealOptions::default(), HealPriority::Normal),
                    storage.clone(),
                );
                let error = task
                    .execute()
                    .with_subscriber(capture.clone())
                    .await
                    .expect_err("ambiguity stays a failure");
                assert!(matches!(error, Error::Storage(EcstoreError::DeleteRecoveryLegacyAmbiguous)));
                assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
                assert!(!error.is_recoverable_heal(), "logging cannot alter retry classification");
                scheduler_would_repeat |= task.should_log_failure(&error);
                assert_eq!(task.get_outcome().await.counters.failed, 1);
            }
            assert_eq!(
                capture.0.load(Ordering::SeqCst),
                1,
                "only the first failure is ERROR for {kind}, outer={outer}"
            );
            assert!(!scheduler_would_repeat, "scheduler shares the leaf's suppression across task IDs");
            assert_eq!(
                storage.heal_object_calls.lock().expect("execution calls").len(),
                3,
                "all attempts still execute"
            );
        }
    }
}
