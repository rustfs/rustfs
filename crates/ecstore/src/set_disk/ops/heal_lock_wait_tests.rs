// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_isolated;
use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage_api_contracts::heal::HealOperations as _;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

const METRIC: &str = "rustfs_heal_object_lock_wait_seconds";

async fn captured<F: Future>(recorder: &DebuggingRecorder, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| metrics::with_local_recorder(recorder, || future.as_mut().poll(cx))).await
}

fn poll_once<F: Future>(recorder: &DebuggingRecorder, future: Pin<&mut F>) -> Poll<F::Output> {
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    metrics::with_local_recorder(recorder, || future.poll(&mut cx))
}

fn samples(recorder: &DebuggingRecorder) -> Vec<(BTreeMap<String, String>, f64)> {
    recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == METRIC)
        .flat_map(|(key, _, _, value)| {
            let labels: BTreeMap<_, _> = key
                .key()
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect();
            let DebugValue::Histogram(values) = value else { panic!("wait must be a histogram") };
            values.into_iter().map(move |value| (labels.clone(), value.0))
        })
        .collect()
}

fn one_sample(recorder: &DebuggingRecorder, op: &str, mode: &str, result: &str) -> f64 {
    let rows = samples(recorder);
    assert_eq!(rows.len(), 1, "one observation per actual acquisition attempt");
    assert_eq!(
        rows[0].0,
        BTreeMap::from([
            ("op".to_owned(), op.to_owned()),
            ("mode".to_owned(), mode.to_owned()),
            ("result".to_owned(), result.to_owned()),
        ]),
        "no object, bucket, version, or owner labels"
    );
    assert!(rows[0].1.is_finite() && rows[0].1 >= 0.0);
    rows[0].1
}

#[tokio::test]
async fn heal_object_lock_metrics_cover_paths_and_skip_borrowed_locks() {
    let (_dirs, _disks, set) = hermetic_set_disks_isolated(4).await;
    let bucket = "heal-lock-metric-paths";
    set.make_bucket(bucket, &MakeBucketOptions::default()).await.expect("bucket");
    let recorder = DebuggingRecorder::new();
    for (op, kind) in [
        ("read_repair_data", HealObjectLockKind::Read),
        ("read_repair_commit", HealObjectLockKind::Write),
        ("heal_object", HealObjectLockKind::Write),
    ] {
        let guard = captured(&recorder, set.acquire_heal_object_lock(bucket, "missing", "", kind, op))
            .await
            .expect("namespace lock");
        one_sample(&recorder, op, kind.as_str(), "acquired");
        metrics::with_local_recorder(&recorder, || drop(guard));
        assert!(samples(&recorder).is_empty(), "hold time must not become another wait sample");
    }
    let opts = HealOpts {
        dry_run: true,
        ..Default::default()
    };
    captured(&recorder, set.heal_object(bucket, "missing", "", &opts))
        .await
        .expect("missing object report");
    one_sample(&recorder, "heal_object", "write", "acquired");
    captured(&recorder, set.check_abandoned_parts(bucket, "missing", &opts))
        .await
        .expect("abandoned parts check");
    one_sample(&recorder, "check_abandoned_parts", "write", "acquired");
    captured(&recorder, set.heal_object_dir(bucket, "missing-dir/", true, false))
        .await
        .expect("directory report");
    one_sample(&recorder, "heal_object_dir", "write", "acquired");

    let no_lock = HealOpts { no_lock: true, ..opts };
    captured(&recorder, set.heal_object(bucket, "missing", "", &no_lock))
        .await
        .expect("borrowed-lock report");
    captured(&recorder, set.check_abandoned_parts(bucket, "missing", &no_lock))
        .await
        .expect("borrowed-lock check");
    assert!(samples(&recorder).is_empty(), "no_lock must not invent an acquisition");
}

#[tokio::test]
async fn heal_object_lock_metrics_preserve_contention_and_cancellation() {
    let (_dirs, _disks, set) = hermetic_set_disks_isolated(4).await;
    let namespace = set.new_ns_lock("metric-contention", "object").await.expect("namespace");
    let recorder = DebuggingRecorder::new();
    for cancel in [false, true] {
        let blocker = namespace.get_write_lock(Duration::from_secs(30)).await.expect("blocker");
        let mut waiting = Box::pin(set.acquire_heal_object_lock(
            "metric-contention",
            "object",
            "",
            HealObjectLockKind::Read,
            "read_repair_data",
        ));
        assert!(poll_once(&recorder, waiting.as_mut()).is_pending(), "real holder must block heal");
        let pending_at = Instant::now();
        assert!(samples(&recorder).is_empty(), "unfinished attempt has no completed sample");
        if cancel {
            let blocked_for = pending_at.elapsed();
            metrics::with_local_recorder(&recorder, || drop(waiting));
            assert!(one_sample(&recorder, "read_repair_data", "read", "cancelled") >= blocked_for.as_secs_f64());
            let mut successor = Box::pin(namespace.get_read_lock(Duration::from_secs(30)));
            assert!(
                poll_once(&recorder, successor.as_mut()).is_pending(),
                "cancelling heal must not release another holder"
            );
            drop(blocker);
            drop(successor.await.expect("cancelled waiter must not leak ownership"));
        } else {
            let blocked_for = pending_at.elapsed();
            drop(blocker);
            let guard = captured(&recorder, waiting).await.expect("acquire after blocker releases");
            assert!(one_sample(&recorder, "read_repair_data", "read", "acquired") >= blocked_for.as_secs_f64());
            let mut successor = Box::pin(namespace.get_write_lock(Duration::from_secs(30)));
            assert!(
                poll_once(&recorder, successor.as_mut()).is_pending(),
                "observation must preserve the returned guard"
            );
            metrics::with_local_recorder(&recorder, || drop(guard));
            drop(successor.await.expect("release at the original guard drop"));
        }
        assert!(samples(&recorder).is_empty(), "one outcome per attempt");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn heal_object_lock_metrics_retain_failed_acquisition() {
    temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
        let (_dirs, _disks, set) = hermetic_set_disks_isolated(4).await;
        let namespace = set.new_ns_lock("metric-timeout", "object").await.expect("namespace");
        let blocker = namespace.get_write_lock(Duration::from_secs(30)).await.expect("blocker");
        let recorder = DebuggingRecorder::new();
        let result = captured(
            &recorder,
            set.acquire_heal_object_lock("metric-timeout", "object", "", HealObjectLockKind::Write, "heal_object"),
        )
        .await;
        assert!(result.is_err(), "measurement must not turn a timeout into success");
        one_sample(&recorder, "heal_object", "write", "failed");
        drop(blocker);
    })
    .await;
}

#[tokio::test]
async fn heal_object_lock_metrics_separate_disabled_and_unpolled_acquisitions() {
    use rustfs_lock::{GlobalLockManager, NamespaceLock, NamespaceLockWrapper, ObjectKey};
    let manager = Arc::new(GlobalLockManager::Disabled(rustfs_lock::fast_lock::DisabledLockManager::new()));
    let namespace = NamespaceLockWrapper::new(
        NamespaceLock::with_local_manager("disabled-test".to_owned(), manager),
        ObjectKey::new("bucket", "object"),
        "owner".to_owned(),
    );
    let recorder = DebuggingRecorder::new();
    let unpolled =
        measure_heal_object_lock("heal_object", HealObjectLockKind::Write, namespace.get_write_lock(Duration::from_secs(1)));
    metrics::with_local_recorder(&recorder, || drop(unpolled));
    assert!(samples(&recorder).is_empty(), "a never-polled future is not an attempted acquisition");
    let guard = captured(
        &recorder,
        measure_heal_object_lock("heal_object", HealObjectLockKind::Write, namespace.get_write_lock(Duration::from_secs(1))),
    )
    .await
    .expect("disabled guard");
    assert!(matches!(&guard, rustfs_lock::NamespaceLockGuard::Fast(guard) if guard.is_disabled()));
    one_sample(&recorder, "heal_object", "write", "disabled");
}
