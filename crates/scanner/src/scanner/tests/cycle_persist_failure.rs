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

struct Failure {
    epoch: u64,
    successful_saves: usize,
    after_commit: bool,
}

static FAILURE: StdMutex<Option<Failure>> = StdMutex::new(None);

struct FailureGuard;

impl Drop for FailureGuard {
    fn drop(&mut self) {
        *FAILURE.lock().expect("failure injection lock") = None;
    }
}

pub(in crate::scanner) fn take_failure(epoch: u64) -> Option<bool> {
    let mut slot = FAILURE.lock().expect("failure injection lock");
    let failure = slot.as_mut().filter(|failure| failure.epoch == epoch)?;
    if failure.successful_saves > 0 {
        failure.successful_saves -= 1;
        return None;
    }
    slot.take().map(|failure| failure.after_commit)
}

async fn assert_leader_reloads_after_failed_save(successful_saves: usize, after_commit: bool) {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let (_temp_dir, store) = setup_scanner_cycle_store().await;
        let ctx = CancellationToken::new();
        let mut cycle = CurrentCycle {
            next: 12,
            ..Default::default()
        };
        let mut revision = DataUsageCacheRevision::Missing;
        assert!(persist_scanner_cycle_state(&ctx, store.clone(), &mut cycle, &mut revision, 42).await);
        *FAILURE.lock().expect("failure injection lock") = Some(Failure {
            epoch: 43,
            successful_saves,
            after_commit,
        });
        let failure_guard = FailureGuard;

        let result = tokio::time::timeout(Duration::from_secs(20), run_data_scanner(ctx.clone(), store.clone()))
            .await
            .expect("a failed cycle-state save must stop this leader iteration");
        assert!(
            result
                .expect_err("state persistence failure must be retryable")
                .to_string()
                .contains("retrying from durable state")
        );
        assert!(
            FAILURE.lock().expect("failure injection lock").is_none(),
            "the injected failure must fire"
        );
        drop(failure_guard);
        assert!(!ctx.is_cancelled(), "recovery must not stop the scanner supervisor");
        assert!(!global_metrics().report().await.current_cycle_active);

        let (durable, epoch) = decode_scanner_cycle_state(
            &read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
                .await
                .expect("durable cycle state"),
        )
        .expect("valid durable cycle state");
        assert_eq!(epoch, 43);
        let expected_next = 12 + u64::try_from(successful_saves).expect("test count fits") + u64::from(after_commit);
        assert_eq!(durable.next, expected_next);

        let resumed_ctx = ctx.child_token();
        let _cancel_on_drop = resumed_ctx.clone().drop_guard();
        let mut resumed = tokio::spawn({
            let ctx = resumed_ctx.clone();
            let store = store.clone();
            async move { run_data_scanner(ctx, store).await }
        });
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                tokio::select! {
                    result = &mut resumed => panic!("resumed scanner stopped before making progress: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
                    .await
                    .expect("resumed state");
                let (cycle, epoch) = decode_scanner_cycle_state(&state).expect("resumed cycle state");
                if epoch == 44 && cycle.next > expected_next && !global_metrics().report().await.current_cycle_active {
                    crate::remote_scanner::validate_remote_scanner_request_fence_with_store(cycle.next, epoch, store.clone())
                        .await
                        .expect("the recovered cycle must pass the peer fence");
                    break;
                }
            }
        })
        .await
        .expect("the same process must resume durable scanner progress");
        resumed_ctx.cancel();
        resumed
            .await
            .expect("resumed scanner task should not panic")
            .expect("resumed scanner should stop cleanly");
        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
#[serial]
async fn initial_cycle_save_failure_reloads_durable_state() {
    assert_leader_reloads_after_failed_save(0, false).await;
}

#[tokio::test]
#[serial]
async fn later_cycle_save_failure_reloads_durable_state() {
    assert_leader_reloads_after_failed_save(1, false).await;
}

#[tokio::test]
#[serial]
async fn initial_cycle_post_commit_failure_reloads_durable_state() {
    assert_leader_reloads_after_failed_save(0, true).await;
}

#[tokio::test]
#[serial]
async fn later_cycle_post_commit_failure_reloads_durable_state() {
    assert_leader_reloads_after_failed_save(1, true).await;
}
