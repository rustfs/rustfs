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
    /// Consecutive write attempts that fail before the fault clears.
    remaining: usize,
}

static FAILURE: StdMutex<Option<Failure>> = StdMutex::new(None);

pub(super) struct FailureGuard;

pub(super) fn fail_next_save(epoch: u64, after_commit: bool) -> FailureGuard {
    fail_saves(epoch, after_commit, 1)
}

/// Fail `remaining` consecutive write attempts under `epoch`; a count above
/// the bounded in-save retries makes the whole save fail.
pub(super) fn fail_saves(epoch: u64, after_commit: bool, remaining: usize) -> FailureGuard {
    let mut slot = FAILURE.lock().expect("failure injection lock");
    assert!(slot.is_none(), "state failure injection must not replace another test's fault");
    *slot = Some(Failure {
        epoch,
        successful_saves: 0,
        after_commit,
        remaining,
    });
    FailureGuard
}

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
    if failure.remaining > 1 {
        failure.remaining -= 1;
        return Some(failure.after_commit);
    }
    slot.take().map(|failure| failure.after_commit)
}

/// rustfs/rustfs#8141: a cycle-state save that fails without a newer owner
/// must keep the current epoch. A transient fault is retried inside the save;
/// an exhausted save reloads durable state under the same epoch. Either way
/// the leader keeps dispatching with the fence peers already validate.
async fn assert_leader_keeps_epoch_after_failed_save(successful_saves: usize, after_commit: bool, remaining: usize) {
    temp_env::async_with_vars([(ENV_SCANNER_CYCLE, Some("1")), (ENV_SCANNER_START_DELAY_SECS, Some("0"))], async {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
        let (_temp_dir, store) = setup_scanner_cycle_store().await;
        let ctx = CancellationToken::new();
        let _cancel_on_drop = ctx.clone().drop_guard();
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
            remaining,
        });
        let failure_guard = FailureGuard;

        let mut leader = tokio::spawn({
            let ctx = ctx.clone();
            let store = store.clone();
            async move { run_data_scanner(ctx, store).await }
        });
        // Progress persisted before the fault plus the faulted cycle plus one
        // more cycle proves the leader kept going under the same epoch.
        let expected_next = 12 + u64::try_from(successful_saves).expect("test count fits") + 2;
        let (cycle, epoch) = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                tokio::select! {
                    result = &mut leader => panic!("a failed save must not end the leader iteration: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                let fired = FAILURE.lock().expect("failure injection lock").is_none();
                let state = read_config(store.clone(), &DATA_USAGE_BLOOM_NAME_PATH)
                    .await
                    .expect("durable cycle state");
                let (cycle, epoch) = decode_scanner_cycle_state(&state).expect("durable cycle state should decode");
                if fired && cycle.next >= expected_next {
                    break (cycle, epoch);
                }
            }
        })
        .await
        .expect("the leader must keep persisting progress after the injected save failure");
        drop(failure_guard);
        assert_eq!(epoch, 43, "a non-fence save failure must not change the leadership epoch");
        crate::remote_scanner::validate_remote_scanner_request_fence_with_store(cycle.next, epoch, store.clone())
            .await
            .expect("the durable cycle must keep passing the peer fence");
        assert!(!global_metrics().report().await.current_cycle_active || cycle.next >= expected_next);

        ctx.cancel();
        leader
            .await
            .expect("leader task should not panic")
            .expect("leader should stop cleanly");
        global_metrics().set_cycle(None).await;
        crate::scanner_io::clear_dirty_usage_buckets_for_tests();
    })
    .await;
    crate::runtime_config::refresh_scanner_runtime_config_for_tests();
}

#[tokio::test]
#[serial]
async fn initial_cycle_save_failure_keeps_epoch() {
    assert_leader_keeps_epoch_after_failed_save(0, false, 1).await;
}

#[tokio::test]
#[serial]
async fn later_cycle_save_failure_keeps_epoch() {
    assert_leader_keeps_epoch_after_failed_save(1, false, 1).await;
}

#[tokio::test]
#[serial]
async fn initial_cycle_post_commit_failure_keeps_epoch() {
    assert_leader_keeps_epoch_after_failed_save(0, true, 1).await;
}

#[tokio::test]
#[serial]
async fn later_cycle_post_commit_failure_keeps_epoch() {
    assert_leader_keeps_epoch_after_failed_save(1, true, 1).await;
}

#[tokio::test]
#[serial]
async fn exhausted_save_retries_reload_durable_state_under_the_same_epoch() {
    assert_leader_keeps_epoch_after_failed_save(0, false, 3).await;
}
