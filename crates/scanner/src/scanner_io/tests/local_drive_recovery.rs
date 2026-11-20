// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::owner::EcstoreDiskAPI as _;

struct Probe {
    set: Arc<SetDisks>,
    before: bool,
    replace: bool,
    faulty: bool,
    eof: bool,
    attempts: AtomicUsize,
    reads: AtomicUsize,
}

static PROBES: std::sync::LazyLock<StdMutex<HashMap<String, Arc<Probe>>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

fn probe(bucket: &str) -> Option<Arc<Probe>> {
    PROBES.lock().expect("probe registry").get(bucket).cloned()
}

async fn retire(probe: &Probe, disk: &Arc<Disk>) {
    let replacement = if probe.replace {
        let next = new_disk(
            &disk.endpoint(),
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("replacement wrapper");
        assert_eq!(
            next.get_disk_id().await.expect("replacement identity"),
            disk.get_disk_id().await.expect("old identity")
        );
        Some(next)
    } else {
        None
    };
    disk.close().await.expect("retire old wrapper");
    if let Some(next) = replacement {
        let mut disks = probe.set.disks.write().await;
        let slot = disks
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|current| Arc::ptr_eq(current, disk)))
            .expect("old wrapper in authoritative inventory");
        *slot = Some(next);
    }
}

pub(crate) async fn before_attempt(bucket: &str, disk: &Arc<Disk>) {
    let Some(probe) = probe(bucket) else { return };
    let attempt = probe.attempts.fetch_add(1, Ordering::SeqCst);
    if probe.before && !probe.faulty && attempt == 0 {
        retire(&probe, disk).await;
    }
}

pub(crate) async fn after_metadata_read(bucket: &str) -> bool {
    let Some(probe) = probe(bucket) else { return false };
    if probe.faulty {
        return probe.reads.fetch_add(1, Ordering::SeqCst) == 0;
    }
    if !probe.before && !probe.eof && probe.reads.fetch_add(1, Ordering::SeqCst) == 0 {
        let disk = scanner_set_disk_inventory(&probe.set)
            .await
            .into_iter()
            .next()
            .expect("local wrapper");
        retire(&probe, &disk).await;
    }
    false
}

pub(crate) async fn after_walk(bucket: &str, disk: &Arc<Disk>) {
    let Some(probe) = probe(bucket) else { return };
    if probe.eof && probe.reads.fetch_add(1, Ordering::SeqCst) == 0 {
        retire(&probe, disk).await;
    }
}

struct RegisteredProbe(String);
impl Drop for RegisteredProbe {
    fn drop(&mut self) {
        PROBES.lock().expect("probe registry").remove(&self.0);
    }
}

fn register_probe(
    bucket: &str,
    set: Arc<SetDisks>,
    before: bool,
    replace: bool,
    faulty: bool,
    eof: bool,
) -> (Arc<Probe>, RegisteredProbe) {
    let probe = Arc::new(Probe {
        set,
        before,
        replace,
        faulty,
        eof,
        attempts: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    });
    assert!(
        PROBES
            .lock()
            .expect("probe registry")
            .insert(bucket.to_owned(), probe.clone())
            .is_none()
    );
    (probe, RegisteredProbe(bucket.to_owned()))
}

pub(crate) fn retire_source_at_eof(bucket: &str, set: Arc<SetDisks>) -> impl Drop {
    register_probe(bucket, set, false, false, false, true).1
}

async fn recovery_fixture(bucket: &str, before: bool, replace: bool, faulty: bool, eof: bool) {
    let (_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for name in [bucket, "unaffected-bucket"] {
        store
            .make_bucket(name, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for path in ["a/deep/first", "a/deep/second", "z/last"] {
            let mut reader = ScannerPutObjReader::from_vec(b"payload".to_vec());
            store
                .put_object(name, path, &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("write object");
        }
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let set = store.pools[0].disk_set[0].clone();
    let (probe, _registered) = register_probe(bucket, set.clone(), before, replace, faulty, eof);
    let status = run_full_cycle(&store, 1).await;
    if replace || faulty {
        assert_eq!(status, ScannerCycleStatus::Complete);
        assert_eq!(
            probe.attempts.load(Ordering::SeqCst),
            2,
            "one recovery attempt including zero-progress interruption"
        );
        let mut cache = DataUsageCache::default();
        cache
            .load(set, &path_join_buf(&[bucket, DATA_USAGE_CACHE_NAME]))
            .await
            .expect("reload durable cache");
        assert!(cache.info.snapshot_complete, "recovery must finish a full scan");
        let total = cache.checked_flatten(bucket).expect("complete recovered statistics");
        assert_eq!(total.objects, 3);
        assert_eq!(total.size, 21);
    } else {
        assert_ne!(status, ScannerCycleStatus::Complete, "retired drive cannot certify completion");
        assert_eq!(probe.attempts.load(Ordering::SeqCst), 1, "no repeated scans on a retired wrapper");
    }
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn issue_9469_recovers_retired_wrapper_before_first_object() {
    recovery_fixture("recovery-first", true, true, false, false).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_recovers_retired_wrapper_in_deep_subtree() {
    recovery_fixture("recovery-deep", false, true, false, false).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_persistent_retirement_is_bounded() {
    recovery_fixture("recovery-offline", true, false, false, false).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_faulty_metadata_stops_and_retries_without_losing_object() {
    recovery_fixture("recovery-faulty", false, false, true, false).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_retirement_at_eof_cannot_certify_old_handle() {
    recovery_fixture("recovery-eof", false, true, false, true).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_binding_rejects_changed_format_identity_and_cancelled_recovery() {
    let (_dir, store) = setup_local_scanner_store(1, 1).await;
    let set = store.pools[0].disk_set[0].clone();
    let disk = scanner_set_disk_inventory(&set)
        .await
        .into_iter()
        .next()
        .expect("local drive");
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let current = current_local_scanner_disk(&set, &disk, &ctx, &budget)
        .await
        .expect("matching drive binding");
    assert!(Arc::ptr_eq(&current, &disk));
    let mut different_format = set.as_ref().clone();
    different_format.format.erasure.sets[0][0] = Uuid::new_v4();
    assert!(
        current_local_scanner_disk(&different_format, &disk, &ctx, &budget)
            .await
            .is_none(),
        "different physical identity cannot resume this slot"
    );
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    ctx.cancel();
    assert!(
        wait_for_local_scanner_recovery(&set, &disk, &ctx, &budget).await.is_none(),
        "cancelled source must not admit recovery"
    );
}

tokio::task_local! {
    static BLOCK_BINDING_PROBE: Arc<tokio::sync::Notify>;
}

pub(crate) async fn before_binding_probe() {
    if let Ok(entered) = BLOCK_BINDING_PROBE.try_with(Arc::clone) {
        entered.notify_one();
        std::future::pending::<()>().await;
    }
}

#[derive(Clone, Copy)]
enum BindingStop {
    Timeout,
    Cancellation,
    Budget,
}

async fn blocked_binding_fixture(stop: BindingStop) {
    let (_dir, store) = setup_local_scanner_store(1, 1).await;
    let set = store.pools[0].disk_set[0].clone();
    let disk = scanner_set_disk_inventory(&set)
        .await
        .into_iter()
        .next()
        .expect("local drive");
    // Real filesystem setup completes before using controlled time.
    tokio::time::pause();
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &ctx,
        ScannerCycleBudgetConfig {
            max_duration: matches!(stop, BindingStop::Budget).then_some(Duration::from_secs(1)),
            ..Default::default()
        },
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let started = tokio::time::Instant::now();
    BLOCK_BINDING_PROBE
        .scope(entered.clone(), async {
            let lookup = current_local_scanner_disk(&set, &disk, &ctx, &budget);
            tokio::pin!(lookup);
            tokio::select! {
                biased;
                _ = &mut lookup => panic!("the controlled probe must block before binding finishes"),
                _ = entered.notified() => {},
            }
            if matches!(stop, BindingStop::Cancellation) {
                ctx.cancel();
            }
            assert!(
                tokio::time::timeout(Duration::from_secs(6), &mut lookup)
                    .await
                    .expect("initial binding must stop even when metadata I/O never returns")
                    .is_none()
            );
        })
        .await;
    let expected = match stop {
        BindingStop::Timeout => Duration::from_secs(5),
        BindingStop::Cancellation => Duration::ZERO,
        BindingStop::Budget => Duration::from_secs(1),
    };
    let elapsed = started.elapsed();
    // Tokio timers round to millisecond ticks; no wall-clock sleeps are used.
    assert!(
        (expected..=expected + Duration::from_millis(1)).contains(&elapsed),
        "the binding must respect the earliest bound: expected {expected:?}, elapsed {elapsed:?}"
    );
}

#[tokio::test]
#[serial]
async fn issue_9469_initial_binding_metadata_probe_has_a_timeout() {
    blocked_binding_fixture(BindingStop::Timeout).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_initial_binding_metadata_probe_observes_cancellation() {
    blocked_binding_fixture(BindingStop::Cancellation).await;
}

#[tokio::test]
#[serial]
async fn issue_9469_initial_binding_metadata_probe_observes_budget() {
    blocked_binding_fixture(BindingStop::Budget).await;
}
