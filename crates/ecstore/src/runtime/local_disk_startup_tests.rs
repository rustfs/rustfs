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

#![cfg(not(windows))]

use super::initialize_local_disk_maps;
use crate::disk::format::FormatV3;
use crate::disk::os::prepared_publication_test_hooks as hooks;
use crate::disk::{DiskAPI, DiskOption, DiskStore, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET};
use crate::layout::endpoint::Endpoint;
use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
use crate::runtime::global::TypeLocalDiskSetDrives;
use crate::runtime::instance::InstanceContext;
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn startup_pools(directory: &Path, label: &str, count: usize, width: usize) -> EndpointServerPools {
    assert!(count > 0 && count.is_multiple_of(width));
    let format = FormatV3::new(count / width, width);
    let mut endpoints = Vec::with_capacity(count);
    for index in 0..count {
        let path = directory.join(format!("{label}-disk-{index}"));
        let metadata = path.join(RUSTFS_META_BUCKET);
        std::fs::create_dir_all(&metadata).expect("startup metadata directory");
        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[index / width][index % width];
        std::fs::write(metadata.join(FORMAT_CONFIG_FILE), disk_format.to_json().expect("startup format bytes"))
            .expect("persist startup format");
        let mut endpoint = Endpoint::try_from(path.to_str().expect("UTF-8 startup path")).expect("startup endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(index / width);
        endpoint.set_disk_index(index % width);
        endpoints.push(endpoint);
    }
    EndpointServerPools(vec![PoolEndpoints {
        legacy: false,
        set_count: count / width,
        drives_per_set: width,
        endpoints: Endpoints::from(endpoints),
        cmd_line: "startup-concurrency-test".to_string(),
        platform: "test".to_string(),
    }])
}

fn roots(pools: &EndpointServerPools) -> Vec<PathBuf> {
    pools.0[0]
        .endpoints
        .as_ref()
        .iter()
        .map(|endpoint| std::fs::canonicalize(endpoint.get_file_path()).expect("canonical startup root"))
        .collect()
}

fn open_options(cleanup: bool) -> DiskOption {
    DiskOption {
        cleanup,
        health_check: false,
    }
}

#[derive(Default)]
struct ReadGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl ReadGate {
    fn wait(&self) {
        let released = self.released.lock().expect("native read gate");
        drop(
            self.changed
                .wait_while(released, |released| !*released)
                .expect("native read gate wait"),
        );
    }

    fn release(&self) {
        *self.released.lock().expect("release native read gate") = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct ReadStats {
    entered: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct ActiveRead(Arc<ReadStats>);

impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct NativeReadBarrier {
    gate: Arc<ReadGate>,
    stats: Arc<ReadStats>,
    entries: UnboundedReceiver<usize>,
    _hooks: Vec<hooks::Guard>,
}

impl NativeReadBarrier {
    fn install(paths: &[PathBuf]) -> Self {
        Self::install_at(hooks::Stage::FormatReadOpen, paths)
    }

    fn install_at(stage: hooks::Stage, paths: &[PathBuf]) -> Self {
        let gate = Arc::new(ReadGate::default());
        let stats = Arc::new(ReadStats::default());
        let (entered, entries) = unbounded_channel();
        let mut installed = Vec::with_capacity(paths.len());
        for (index, path) in paths.iter().enumerate() {
            let gate = gate.clone();
            let stats = stats.clone();
            let entered = entered.clone();
            installed.push(hooks::install_at(stage, path, move || {
                let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
                let _active = ActiveRead(stats.clone());
                stats.peak.fetch_max(active, Ordering::SeqCst);
                stats.entered.fetch_add(1, Ordering::SeqCst);
                let _ = entered.send(index);
                gate.wait();
            }));
        }
        Self {
            gate,
            stats,
            entries,
            _hooks: installed,
        }
    }

    async fn wait_for_entries(&mut self, count: usize) {
        for _ in 0..count {
            tokio::time::timeout(TEST_TIMEOUT, self.entries.recv())
                .await
                .expect("expected independent native reads to start")
                .expect("native read entry channel");
        }
    }

    fn release(&self) {
        self.gate.release();
    }
}

impl Drop for NativeReadBarrier {
    fn drop(&mut self) {
        self.release();
    }
}

struct RegistrySnapshot {
    by_path: HashMap<String, Option<DiskStore>>,
    by_slot: TypeLocalDiskSetDrives,
}

fn snapshot_without_waiting(context: &InstanceContext) -> RegistrySnapshot {
    // Observe both inventories in the publisher's set-to-map lock order.
    let slots_handle = context.local_disk_set_drives();
    let slots = slots_handle
        .try_read()
        .expect("native disk preparation must not hold the slot inventory lock");
    let map_handle = context.local_disk_map();
    let map = map_handle
        .try_read()
        .expect("native disk preparation must not hold the RPC disk map lock");
    RegistrySnapshot {
        by_path: map.clone(),
        by_slot: slots.clone(),
    }
}

fn assert_same_disk(expected: &Option<DiskStore>, actual: &Option<DiskStore>) {
    match (expected, actual) {
        (Some(expected), Some(actual)) => assert!(Arc::ptr_eq(expected, actual), "registered handle must be preserved"),
        (None, None) => {}
        _ => panic!("registered slot availability changed"),
    }
}

fn assert_same_registry(expected: &RegistrySnapshot, actual: &RegistrySnapshot) {
    assert_eq!(actual.by_path.len(), expected.by_path.len());
    for (path, disk) in &expected.by_path {
        assert_same_disk(disk, actual.by_path.get(path).expect("original endpoint remains registered"));
    }
    assert_eq!(actual.by_slot.len(), expected.by_slot.len());
    for (expected_pool, actual_pool) in expected.by_slot.iter().zip(&actual.by_slot) {
        assert_eq!(actual_pool.len(), expected_pool.len());
        for (expected_set, actual_set) in expected_pool.iter().zip(actual_pool) {
            assert_eq!(actual_set.len(), expected_set.len());
            for (expected_disk, actual_disk) in expected_set.iter().zip(actual_set) {
                assert_same_disk(expected_disk, actual_disk);
            }
        }
    }
}

fn assert_complete_registry(context: &InstanceContext, pools: &EndpointServerPools) {
    let snapshot = snapshot_without_waiting(context);
    assert_eq!(snapshot.by_path.len(), pools.0[0].endpoints.as_ref().len());
    assert_eq!(snapshot.by_slot.len(), 1);
    assert_eq!(snapshot.by_slot[0].len(), pools.0[0].set_count);
    for endpoint in pools.0[0].endpoints.as_ref() {
        let disk = snapshot.by_path[&endpoint.to_string()]
            .as_ref()
            .expect("healthy registered endpoint");
        assert_eq!(disk.endpoint(), *endpoint);
        let set = usize::try_from(endpoint.set_idx).expect("valid set index");
        let index = usize::try_from(endpoint.disk_idx).expect("valid disk index");
        let by_slot = snapshot.by_slot[0][set][index].as_ref().expect("healthy original slot");
        assert!(Arc::ptr_eq(disk, by_slot), "path and slot inventories must publish the same handle");
    }
}

#[tokio::test]
async fn startup_registration_keeps_inventory_readers_responsive_during_native_io() {
    let directory = tempfile::tempdir().expect("responsive inventory fixture");
    let context = Arc::new(InstanceContext::new());
    let previous = startup_pools(directory.path(), "previous", 4, 4);
    initialize_local_disk_maps(&context, previous, &open_options(false))
        .await
        .expect("initial registry");
    let before = snapshot_without_waiting(&context);
    let next = startup_pools(directory.path(), "next", 4, 4);
    let mut barrier = NativeReadBarrier::install(&roots(&next)[..1]);
    let task_context = context.clone();
    let task_pools = next.clone();
    let task = tokio::spawn(async move { initialize_local_disk_maps(&task_context, task_pools, &open_options(false)).await });
    barrier.wait_for_entries(1).await;

    assert_same_registry(&before, &snapshot_without_waiting(&context));

    barrier.release();
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("registration completes after native I/O resumes")
        .expect("registration task")
        .expect("new registry publishes");
    assert_complete_registry(&context, &next);
}

#[tokio::test]
async fn startup_registration_bounds_parallel_native_opens_across_sets() {
    let directory = tempfile::tempdir().expect("bounded startup fixture");
    let context = Arc::new(InstanceContext::new());
    let pools = startup_pools(directory.path(), "parallel", 32, 16);
    let mut barrier = NativeReadBarrier::install(&roots(&pools));
    let task_context = context.clone();
    let task_pools = pools.clone();
    let task = tokio::spawn(async move { initialize_local_disk_maps(&task_context, task_pools, &open_options(false)).await });
    barrier.wait_for_entries(super::LOCAL_DISK_OPEN_CONCURRENCY).await;

    assert_eq!(barrier.stats.active.load(Ordering::SeqCst), super::LOCAL_DISK_OPEN_CONCURRENCY);
    assert!(snapshot_without_waiting(&context).by_path.is_empty());
    barrier.release();
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("all startup waves complete")
        .expect("bounded registration task")
        .expect("bounded registration succeeds");
    assert_eq!(barrier.stats.entered.load(Ordering::SeqCst), 32);
    assert_eq!(barrier.stats.peak.load(Ordering::SeqCst), super::LOCAL_DISK_OPEN_CONCURRENCY);
    assert_complete_registry(&context, &pools);
}

#[tokio::test]
async fn startup_registration_parallelizes_native_disk_info_queries() {
    let directory = tempfile::tempdir().expect("native disk-info fixture");
    let context = Arc::new(InstanceContext::new());
    let pools = startup_pools(directory.path(), "disk-info", 4, 4);
    let mut barrier = NativeReadBarrier::install_at(hooks::Stage::DiskInfoRead, &roots(&pools));
    let task_context = context.clone();
    let task_pools = pools.clone();
    let task = tokio::spawn(async move { initialize_local_disk_maps(&task_context, task_pools, &open_options(false)).await });
    barrier.wait_for_entries(4).await;

    assert_eq!(barrier.stats.active.load(Ordering::SeqCst), 4);
    assert!(snapshot_without_waiting(&context).by_path.is_empty());
    barrier.release();
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("disk-info preparation completes")
        .expect("disk-info registration task")
        .expect("disk-info registration succeeds");
    assert_complete_registry(&context, &pools);
}

#[tokio::test]
async fn failed_startup_preparation_preserves_the_previous_registry() {
    let directory = tempfile::tempdir().expect("failed startup fixture");
    let context = Arc::new(InstanceContext::new());
    initialize_local_disk_maps(&context, startup_pools(directory.path(), "previous", 4, 4), &open_options(false))
        .await
        .expect("initial registry");
    let before = snapshot_without_waiting(&context);
    let next = startup_pools(directory.path(), "invalid", 4, 4);
    let corrupt = PathBuf::from(next.0[0].endpoints.as_ref()[1].get_file_path())
        .join(RUSTFS_META_BUCKET)
        .join(FORMAT_CONFIG_FILE);
    std::fs::write(corrupt, b"invalid-format").expect("corrupt new format fixture");

    initialize_local_disk_maps(&context, next, &open_options(false))
        .await
        .expect_err("fatal disk preparation must reject the new registry");

    assert_same_registry(&before, &snapshot_without_waiting(&context));
}

#[tokio::test]
async fn cancelled_startup_preparation_preserves_the_previous_registry() {
    let directory = tempfile::tempdir().expect("cancelled startup fixture");
    let context = Arc::new(InstanceContext::new());
    initialize_local_disk_maps(&context, startup_pools(directory.path(), "previous", 4, 4), &open_options(false))
        .await
        .expect("initial registry");
    let before = snapshot_without_waiting(&context);
    let next = startup_pools(directory.path(), "cancelled", 4, 4);
    let mut barrier = NativeReadBarrier::install(&roots(&next));
    let task_context = context.clone();
    let task = tokio::spawn(async move { initialize_local_disk_maps(&task_context, next, &open_options(false)).await });
    barrier.wait_for_entries(1).await;

    task.abort();
    assert!(task.await.expect_err("cancelled registration task").is_cancelled());
    assert_same_registry(&before, &snapshot_without_waiting(&context));
    barrier.release();
}

#[tokio::test]
async fn repeated_registration_reuses_live_handles_and_staging() {
    let directory = tempfile::tempdir().expect("reused startup fixture");
    let context = Arc::new(InstanceContext::new());
    let pools = startup_pools(directory.path(), "reused", 4, 4);
    initialize_local_disk_maps(&context, pools.clone(), &open_options(false))
        .await
        .expect("initial registry");
    let before = snapshot_without_waiting(&context);
    let staging = roots(&pools)[0].join(RUSTFS_META_BUCKET).join("tmp/live-startup-writer");
    std::fs::write(&staging, b"live staging").expect("live startup staging");
    let barrier = NativeReadBarrier::install(&roots(&pools));

    tokio::time::timeout(TEST_TIMEOUT, initialize_local_disk_maps(&context, pools.clone(), &open_options(true)))
        .await
        .expect("healthy registration reuses handles without native format opens")
        .expect("repeated registration");

    assert_eq!(barrier.stats.entered.load(Ordering::SeqCst), 0);
    assert_same_registry(&before, &snapshot_without_waiting(&context));
    assert_eq!(std::fs::read(staging).expect("preserved live staging"), b"live staging");
}

#[tokio::test]
async fn concurrent_registration_writers_reuse_the_first_published_batch() {
    let directory = tempfile::tempdir().expect("serialized registration fixture");
    let context = Arc::new(InstanceContext::new());
    initialize_local_disk_maps(&context, startup_pools(directory.path(), "previous", 4, 4), &open_options(false))
        .await
        .expect("initial registry");
    let next = startup_pools(directory.path(), "next", 4, 4);
    let root = roots(&next)[0].clone();
    let mut first_barrier = NativeReadBarrier::install(std::slice::from_ref(&root));
    let task_context = context.clone();
    let task_pools = next.clone();
    let first = tokio::spawn(async move { initialize_local_disk_maps(&task_context, task_pools, &open_options(false)).await });
    first_barrier.wait_for_entries(1).await;
    let second_barrier = NativeReadBarrier::install(std::slice::from_ref(&root));
    let options = open_options(true);
    let mut second = Box::pin(initialize_local_disk_maps(&context, next.clone(), &options));
    poll_fn(|cx| {
        assert!(second.as_mut().poll(cx).is_pending(), "a second writer waits for the preparing writer");
        Poll::Ready(())
    })
    .await;

    first_barrier.release();
    tokio::time::timeout(TEST_TIMEOUT, first)
        .await
        .expect("first registration completes")
        .expect("first registration task")
        .expect("first registry publishes");
    let first_snapshot = snapshot_without_waiting(&context);
    second_barrier.release();
    tokio::time::timeout(TEST_TIMEOUT, second)
        .await
        .expect("second registration completes")
        .expect("second registry publishes");

    assert_same_registry(&first_snapshot, &snapshot_without_waiting(&context));
    assert_eq!(second_barrier.stats.entered.load(Ordering::SeqCst), 0);
    assert_complete_registry(&context, &next);
}
