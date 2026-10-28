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

use super::*;
use crate::storage_api::owner::EcstoreExpiryState;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

// Return Some(result) if the operation ended too early, None only if it was
// actually polled pending after the prerequisite read had finished.
async fn observe_queued<F: Future>(
    mut operation: Pin<&mut F>,
    read_complete: impl Fn() -> bool,
    observation: Duration,
) -> std::result::Result<Option<F::Output>, tokio::time::error::Elapsed> {
    tokio::time::timeout(
        observation,
        std::future::poll_fn(|cx| match operation.as_mut().poll(cx) {
            Poll::Ready(value) => Poll::Ready(Some(value)),
            Poll::Pending if read_complete() => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }),
    )
    .await
}

#[tokio::test]
#[serial]
async fn free_version_cancelled_while_waiting_for_expiry_write_lock_never_queues() {
    let state = EcstoreExpiryState::new_with_unconsumed_worker_channel(4);
    let observation = Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS);
    let context = crate::ExpiryLockTestContext {
        state: state.clone(),
        store: None,
        completed_incarnation_reads: Arc::new(AtomicUsize::new(0)),
    };
    crate::EXPIRY_LOCK_TEST_CONTEXT
        .scope(context, async {
            let object = crate::ScannerObjectInfo {
                bucket: "expiry-lock-bucket".to_string(),
                name: "free-object".to_string(),
                ..Default::default()
            };
            let cancellation = CancellationToken::new();
            let guard = state.write().await;
            let mut pending = Box::pin(crate::enqueue_runtime_free_version(object.clone(), Some(&cancellation)));
            let first = observe_queued(pending.as_mut(), || true, observation).await;
            let observed_wait = matches!(&first, Ok(None));
            let already_finished = matches!(&first, Ok(Some(())));
            cancellation.cancel();
            let cancelled_while_locked = if already_finished {
                false
            } else {
                tokio::time::timeout(observation, pending.as_mut()).await.is_ok()
            };
            let pending_while_locked = guard.pending_tasks();
            drop(guard);
            if !already_finished && !cancelled_while_locked {
                pending.as_mut().await;
            }
            drop(pending);
            let pending_after_cancel = state.read().await.pending_tasks();

            // Positive control queues a real free-version task while using the same
            // implementation and isolated, unconsumed worker channel.
            let mut positive = Box::pin(crate::enqueue_runtime_free_version(object, None));
            let positive_in_time = tokio::time::timeout(observation, positive.as_mut()).await.is_ok();
            if !positive_in_time {
                positive.as_mut().await;
            }
            drop(positive);
            let pending_after_control = state.read().await.pending_tasks();
            assert!(observed_wait, "must poll the real enqueue while the write lock is held");
            assert!(cancelled_while_locked, "cancellation must finish before the lock is released");
            assert_eq!(pending_while_locked, 0);
            assert_eq!(pending_after_cancel, 0);
            assert!(positive_in_time, "positive control must finish within the observation limit");
            assert_eq!(pending_after_control, 1, "positive control must queue a task");
        })
        .await;
}

#[tokio::test]
#[serial]
async fn newer_noncurrent_cancelled_after_real_read_while_waiting_for_expiry_write_lock_never_queues() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 4).await;
    let bucket = "expiry-lock-bucket";
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let expected_incarnation = store.bucket_incarnation_id(bucket).await.expect("real incarnation metadata");
    assert!(!expected_incarnation.is_nil());
    let state = EcstoreExpiryState::new_with_unconsumed_worker_channel(4);
    let reads = Arc::new(AtomicUsize::new(0));
    let context = crate::ExpiryLockTestContext {
        state: state.clone(),
        store: Some(store.clone()),
        completed_incarnation_reads: reads.clone(),
    };
    let observation = Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS);
    crate::EXPIRY_LOCK_TEST_CONTEXT
        .scope(context, async {
            let versions = vec![crate::ScannerObjectToDelete {
                object_name: "noncurrent-object".to_string(),
                version_id: Some(Uuid::new_v4()),
                ..Default::default()
            }];
            let event = crate::Event::default();
            let src = crate::LcEventSrc::Scanner;
            let cancellation = CancellationToken::new();
            let guard = state.write().await;
            let mut pending = Box::pin(crate::enqueue_runtime_newer_noncurrent(
                bucket,
                versions.clone(),
                event.clone(),
                &src,
                Some(&cancellation),
            ));
            let first = observe_queued(pending.as_mut(), || reads.load(Ordering::SeqCst) == 1, observation).await;
            let observed_wait_after_read = matches!(&first, Ok(None));
            let mut result = match first {
                Ok(Some(value)) => Some(value),
                _ => None,
            };
            cancellation.cancel();
            let cancelled_while_locked = if result.is_some() {
                false
            } else {
                match tokio::time::timeout(observation, pending.as_mut()).await {
                    Ok(value) => {
                        result = Some(value);
                        true
                    }
                    Err(_) => false,
                }
            };
            let pending_while_locked = guard.pending_tasks();
            drop(guard);
            let result = match result {
                Some(value) => value,
                None => pending.as_mut().await,
            };
            drop(pending);
            let pending_after_cancel = state.read().await.pending_tasks();
            let mut positive = Box::pin(crate::enqueue_runtime_newer_noncurrent(bucket, versions, event, &src, None));
            let bounded_positive = tokio::time::timeout(observation, positive.as_mut()).await;
            let positive_in_time = bounded_positive.is_ok();
            let positive_result = match bounded_positive {
                Ok(value) => value,
                Err(_) => positive.as_mut().await,
            };
            drop(positive);
            let pending_after_control = state.read().await.pending_tasks();
            assert!(
                observed_wait_after_read,
                "real incarnation read must finish before waiting on the held lock"
            );
            assert!(cancelled_while_locked, "cancellation must finish before the lock is released");
            assert!(!result);
            assert_eq!(pending_while_locked, 0);
            assert_eq!(pending_after_cancel, 0);
            assert!(positive_in_time, "positive control must finish within the observation limit");
            assert!(positive_result, "positive control must enqueue the nonempty version batch");
            assert_eq!(pending_after_control, 1);
            assert_eq!(reads.load(Ordering::SeqCst), 2);
        })
        .await;
}
